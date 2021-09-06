use clap::{App, Arg};
use raft::eraftpb::Entry;
use raft_engine::{Config as EngineConfig, MessageExt, RaftLogEngine};

type Engine = RaftLogEngine<MessageExtTyped>;

#[derive(Clone)]
struct MessageExtTyped;
impl MessageExt for MessageExtTyped {
    type Entry = Entry;

    fn index(entry: &Entry) -> u64 {
        entry.index
    }
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
    let ecfg = EngineConfig {
        dir: path.to_owned(),
        recovery_threads: 16,
        ..Default::default()
    };
    let engine = Engine::open(ecfg, None).unwrap();
    drop(engine);
}
