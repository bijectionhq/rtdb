#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/tx/src/idempotency_window.rs"]
mod production;

#[path = "../../../rust/crates/tx/src/guard_attribution.rs"]
mod guard_attribution;

mod laws;

fn main() {}
