#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/multi/src/journal/replay_cursor.rs"]
mod replay_cursor;

#[path = "../../../rust/crates/replication/src/image_digest.rs"]
mod image_digest;

#[path = "../../../rust/crates/replication/src/recovery_promotion.rs"]
mod recovery_promotion;

mod laws;

fn main() {}
