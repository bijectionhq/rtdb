#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/tx/src/resolver/control_order.rs"]
mod control_order;
#[path = "../../../rust/crates/tx/src/system_time.rs"]
mod system_time;
#[path = "../../../rust/crates/index/src/event_revision.rs"]
mod event_revision;

mod laws;

fn main() {}
