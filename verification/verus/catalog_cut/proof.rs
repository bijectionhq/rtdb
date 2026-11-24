#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/replication/src/catalog_cut.rs"]
mod production;

mod laws;

fn main() {}

#[cfg(not(verus_keep_ghost))]
const CORRESPONDENCE: &[&str] = &[
    include_str!("../../../rust/crates/replication/src/snapshot_replay.rs"),
    include_str!("../../../rust/crates/node/src/sql_snapshots.rs"),
    include_str!("../../../rust/crates/node/src/node.rs"),
    include_str!("../../../rust/crates/query/src/context.rs"),
];
