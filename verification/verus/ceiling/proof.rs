#![feature(proc_macro_hygiene)]

mod laws;

#[path = "../../../rust/crates/types/src/timestamp.rs"]
mod timestamp;
pub use timestamp::Timestamp;

#[path = "../../../rust/crates/temporal/src/ceiling_step.rs"]
mod production;

fn main() {}
