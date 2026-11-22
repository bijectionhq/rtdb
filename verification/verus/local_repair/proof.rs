#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/storage/src/repair_anchor_decision.rs"]
mod anchor;
#[path = "../../../rust/crates/replication/src/repair_decision.rs"]
mod replay;

#[path = "../../../rust/crates/format/src/publication_identity.rs"]
mod digest;

mod laws;

fn main() {}
