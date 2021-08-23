// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::sync::Arc;

use crossbeam::channel::{unbounded, Receiver, Sender};
use log::debug;
use parking_lot::Mutex;

use crate::file_system::Readable;
use crate::log_batch::{LogBatch, LogItemBatch, LOG_BATCH_HEADER_LEN};
use crate::log_file::{LogFileHeader, LOG_FILE_MAX_HEADER_LEN};
use crate::{Error, FileId, Result};

type File = Box<dyn Readable>;

pub struct FileLogItemBatchIterator<'a> {
    reader: &'a mut LogItemBatchFileReader,
}

impl<'a> Iterator for FileLogItemBatchIterator<'a> {
    type Item = Result<LogItemBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.next().transpose()
    }
}

pub struct LogItemBatchFileReader {
    file: Option<File>,
    size: usize,

    buffer: Vec<u8>,
    buffer_offset: usize,
    valid_offset: usize,

    read_block_size: usize,
}

impl LogItemBatchFileReader {
    pub fn new(read_block_size: usize) -> Self {
        Self {
            file: None,
            size: 0,

            buffer: Vec::new(),
            buffer_offset: 0,
            valid_offset: 0,

            read_block_size,
        }
    }

    pub fn open(&mut self, file: File, size: usize) -> Result<FileLogItemBatchIterator> {
        self.file = Some(file);
        self.size = size;
        self.buffer.clear();
        self.buffer_offset = 0;
        self.valid_offset = 0;
        let peek_size = std::cmp::min(LOG_FILE_MAX_HEADER_LEN, size);
        let mut header = self.peek(0, peek_size, LOG_BATCH_HEADER_LEN)?;
        LogFileHeader::decode(&mut header)?;
        self.valid_offset = peek_size - header.len();
        Ok(FileLogItemBatchIterator { reader: self })
    }

    pub fn valid_offset(&self) -> usize {
        self.valid_offset
    }

    fn next(&mut self) -> Result<Option<LogItemBatch>> {
        if self.valid_offset < LOG_BATCH_HEADER_LEN {
            return Err(Error::Corruption(
                "attempt to read file with broken header".to_owned(),
            ));
        }
        if self.valid_offset < self.size {
            let (footer_offset, compression_type, len) = LogBatch::decode_header(&mut self.peek(
                self.valid_offset,
                LOG_BATCH_HEADER_LEN,
                0,
            )?)?;
            let entries_offset = self.valid_offset + LOG_BATCH_HEADER_LEN;
            let entries_len = footer_offset - LOG_BATCH_HEADER_LEN;

            let item_batch = LogItemBatch::decode(
                &mut self.peek(
                    self.valid_offset + footer_offset,
                    len - footer_offset,
                    LOG_BATCH_HEADER_LEN,
                )?,
                entries_offset,
                entries_len,
                compression_type,
            )?;
            self.valid_offset += len;
            return Ok(Some(item_batch));
        }
        Ok(None)
    }

    fn peek(&mut self, offset: usize, size: usize, prefetch: usize) -> Result<&[u8]> {
        debug_assert!(offset >= self.buffer_offset);
        let f = self.file.as_mut().unwrap();
        let end = self.buffer_offset + self.buffer.len();
        if offset > end {
            self.buffer_offset = offset;
            self.buffer
                .resize(std::cmp::max(size + prefetch, self.read_block_size), 0);
            f.seek(std::io::SeekFrom::Start(self.buffer_offset as u64))?;
            let read = f.read(&mut self.buffer)?;
            if read < size {
                return Err(Error::Corruption(format!(
                    "unexpected eof at {}",
                    self.buffer_offset + read
                )));
            }
            self.buffer.resize(read, 0);
            Ok(&self.buffer[..size])
        } else {
            let should_read = (offset + size + prefetch).saturating_sub(end);
            if should_read > 0 {
                let read_offset = self.buffer_offset + self.buffer.len();
                let prev_len = self.buffer.len();
                self.buffer.resize(
                    prev_len + std::cmp::max(should_read, self.read_block_size),
                    0,
                );
                let read = f.read(&mut self.buffer[prev_len..])?;
                if read + prefetch < should_read {
                    return Err(Error::Corruption(format!(
                        "unexpected eof at {}",
                        read_offset + read,
                    )));
                }
                self.buffer.truncate(prev_len + read);
            }
            Ok(&self.buffer[offset - self.buffer_offset..offset - self.buffer_offset + size])
        }
    }
}

type LogItemBatchReceiver = Receiver<Option<(FileId, Result<LogItemBatch>)>>;

pub struct LogItemBatchConcurrentFilesReader {
    mem_limits: usize,
    read_block_size: usize,

    files: Arc<Mutex<VecDeque<(FileId, File, usize)>>>,
    sender: Sender<Option<LogItemBatchReceiver>>,
    receiver: Receiver<Option<LogItemBatchReceiver>>,
    current: Option<LogItemBatchReceiver>,
}

impl LogItemBatchConcurrentFilesReader {
    pub fn open(
        files: VecDeque<(FileId, File, usize)>,
        concurrency: usize,
        mem_limits: usize,
        read_block_size: usize,
    ) -> Result<Self> {
        let (tx, rs) = unbounded();
        let mut reader = Self {
            mem_limits,
            read_block_size,

            files: Arc::new(Mutex::new(files)),
            sender: tx,
            receiver: rs,
            current: None,
        };
        for _ in 0..concurrency {
            reader.spawn();
        }
        Ok(reader)
    }

    pub fn next(&mut self) -> Option<(FileId, Result<LogItemBatch>)> {
        if let Some(ref mut current) = self.current {
            match current.recv().unwrap() {
                Some(r) => return Some(r),
                None => self.current = None,
            }
        }
        assert!(self.current.is_none());
        self.current = self.receiver.recv().unwrap();
        if self.current.is_none() {
            return None;
        }
        self.next()
    }

    fn spawn(&mut self) {
        let files = Arc::clone(&self.files);
        let read_block_size = self.read_block_size;
        let sender = self.sender.clone();
        std::thread::spawn(move || loop {
            // NOTE: `guard` must NOT be drop before the new `LogItemBatchReceiver` is sent.
            let mut guard = files.lock();
            if let Some(file) = guard.pop_front() {
                let mut reader = LogItemBatchFileReader::new(read_block_size);
                let (tx, rx) = unbounded();
                sender.send(Some(rx)).unwrap();
                drop(guard);
                debug!("read file: {:?}", file.0);
                let iter = match reader.open(file.1, file.2) {
                    Ok(iter) => iter,
                    Err(e) => {
                        tx.send(Some((file.0, Err(e)))).unwrap();
                        break;
                    }
                };
                for r in iter {
                    tx.send(Some((file.0, r))).unwrap();
                }
                tx.send(None).unwrap();
            } else {
                if let Err(_) = sender.send(None) {
                    debug!("Tail None.");
                }
                break;
            }
        });
    }
}
