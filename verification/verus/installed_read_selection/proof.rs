#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/multi/src/journal/read_selection.rs"]
mod production;
#[path = "../../../rust/crates/index/src/partition/read_selection.rs"]
mod catalog;
#[path = "../../../rust/crates/node/src/read_selection.rs"]
mod query;
mod laws;

#[derive(Clone, Copy)]
pub struct RequestId {
    nonce: u128,
    expires_at: u64,
}

#[derive(Clone, Copy)]
pub struct StorageVersion(i32);
#[derive(Clone, Copy)]
pub struct StorageEpoch(i32);
#[derive(Clone, Copy)]
pub struct ReplicaPosition(i64);

#[derive(Clone, Copy)]
pub struct CatalogCut {
    storage_version: StorageVersion,
    storage_epoch: StorageEpoch,
    replica_epoch: StorageEpoch,
    through: ReplicaPosition,
}

#[derive(Clone, Copy)]
pub struct Cut {
    next_lsn: u64,
    history: [u8; 32],
}

#[derive(Clone, Copy)]
pub struct Hold {
    start: Cut,
    through: Cut,
}

pub type Instant = u64;

fn main() {}
