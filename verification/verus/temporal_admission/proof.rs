#![feature(proc_macro_hygiene)]

mod laws;
#[path = "../../../rust/crates/temporal/src/event_step.rs"]
mod production;

fn main() {}

#[cfg(not(verus_keep_ghost))]
const CORRESPONDENCE: &[&str] = &[
    include_str!("../../../rust/crates/query/src/scan.rs"),
    include_str!("../../../rust/crates/query/src/settings.rs"),
    include_str!("../../../rust/crates/multi/src/archive/writable_world/held.rs"),
];
