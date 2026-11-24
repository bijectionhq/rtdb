#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/multi/src/journal/hosted/erasure/decision.rs"]
mod production;
#[path = "../../../rust/crates/multi/src/journal/prefix_retirement.rs"]
mod prefix_retirement;
mod laws;

fn main() {}
