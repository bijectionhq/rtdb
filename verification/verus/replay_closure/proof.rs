#![feature(proc_macro_hygiene)]

#[macro_use]
#[path = "../../../rust/crates/format/src/catalog/checkpoint_files.rs"]
mod checkpoint_files;
#[macro_use]
#[path = "../../../rust/crates/replication/src/replay_closure_algorithm.rs"]
mod replay_closure_algorithm;
#[path = "../../../rust/crates/format/src/envelope/log_headers.rs"]
mod log_headers;
#[path = "../foreign_contracts/prost_replay.rs"]
mod prost_replay;

mod inputs;
mod production;
mod laws;

fn main() {}
