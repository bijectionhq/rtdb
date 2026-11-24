#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/replication/src/catalog_cut.rs"]
mod production;

mod laws;

extern crate self as rtdb_format;
#[path = "../../../rust/crates/format/src/publication_identity.rs"]
mod publication_identity;
mod catalog {
    pub use crate::publication_identity::same_publication_digest;
}

fn main() {}

#[cfg(not(verus_keep_ghost))]
const CORRESPONDENCE: &[&str] = &[
    include_str!("../../../rust/crates/replication/src/snapshot_replay.rs"),
    include_str!("../../../rust/crates/node/src/sql_snapshots.rs"),
    include_str!("../../../rust/crates/node/src/node.rs"),
    include_str!("../../../rust/crates/query/src/context.rs"),
    include_str!("../../../rust/crates/query/src/settings.rs"),
    include_str!("../../../rust/crates/multi/src/journal/log.rs"),
    include_str!("../../../rust/crates/multi/src/journal/hosted/storage/snapshot.rs"),
];
