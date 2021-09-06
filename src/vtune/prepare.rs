// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use clap::{App, Arg};
use raft::eraftpb::Entry;
use raft_engine::ReadableSize;
use raft_engine::{Config as EngineConfig, LogBatch, MessageExt, RaftLogEngine, Result};
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;

extern crate libc;

type Engine = RaftLogEngine<MessageExtTyped>;

#[derive(Clone)]
struct MessageExtTyped;
impl MessageExt for MessageExtTyped {
    type Entry = Entry;

    fn index(entry: &Entry) -> u64 {
        entry.index
    }
}

struct Config {
    total_size: ReadableSize,
    region_count: u64,
    batch_size: ReadableSize,
    item_size: ReadableSize,
    entry_size: ReadableSize,
    batch_compression_threshold: ReadableSize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            total_size: ReadableSize::gb(1),
            region_count: 100,
            batch_size: ReadableSize::mb(1),
            item_size: ReadableSize::kb(1),
            entry_size: ReadableSize(256),
            batch_compression_threshold: ReadableSize(0),
        }
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} [region-count: {}][batch-size: {}][item-size: {}][entry-size: {}][batch-compression-threshold: {}]",
            self.total_size,
            self.region_count,
            self.batch_size,
            self.item_size,
            self.entry_size,
            self.batch_compression_threshold
        )
    }
}

fn generate(path: String, cfg: &Config) -> Result<()> {
    let mut rng = rand::rngs::StdRng::seed_from_u64(0);

    let mut ecfg = EngineConfig::new();
    ecfg.dir = path.clone();
    ecfg.batch_compression_threshold = cfg.batch_compression_threshold;

    let engine = Engine::open(ecfg.clone(), None).unwrap();

    let mut indexes: HashMap<u64, u64> = (1..cfg.region_count + 1).map(|rid| (rid, 0)).collect();
    while dir_size(&path).0 < cfg.total_size.0 {
        let mut batch = LogBatch::default();
        while batch.size() < cfg.batch_size.0 as usize {
            let region_id = rng.gen_range(1, cfg.region_count + 1);
            let mut item_size = 0;
            let mut entries = vec![];
            while item_size < cfg.item_size.0 {
                entries.push(Entry {
                    data: (&mut rng)
                        .sample_iter(rand::distributions::Standard)
                        .take(cfg.entry_size.0 as usize)
                        .collect::<Vec<u8>>()
                        .into(),
                    ..Default::default()
                });
                item_size += cfg.entry_size.0;
            }
            let mut index = *indexes.get(&region_id).unwrap();
            index = entries.iter_mut().fold(index, |index, e| {
                e.index = index + 1;
                index + 1
            });
            *indexes.get_mut(&region_id).unwrap() = index;
            batch
                .add_entries::<MessageExtTyped>(region_id, entries)
                .unwrap();
        }
        engine.write(&mut batch, false).unwrap();
    }
    engine.sync().unwrap();
    drop(engine);
    Ok(())
}

fn dir_size(path: &str) -> ReadableSize {
    ReadableSize(
        std::fs::read_dir(PathBuf::from(path))
            .unwrap()
            .map(|entry| std::fs::metadata(entry.unwrap().path()).unwrap().len() as u64)
            .fold(0, |size, x| size + x),
    )
}

fn main() {
    let matches = App::new("Engine Stress (stress)")
        .about("A stress test tool for Raft Engine")
        .arg(
            Arg::with_name("path")
                .long("path")
                .required(true)
                .help("Set the data path for Raft Engine")
                .takes_value(true),
        )
        .get_matches();
    let path = matches
        .value_of("path")
        .expect("Arg `path` doesn't match any input args.");
    let cfg = Config {
        total_size: ReadableSize::gb(10),
        region_count: 1000,
        ..Default::default()
    };
    if let Err(e) = generate(path.to_owned(), &cfg) {
        panic!("Prepare data error: {}", e);
    }
}
