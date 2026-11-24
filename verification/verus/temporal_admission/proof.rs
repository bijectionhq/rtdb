#![feature(proc_macro_hygiene)]

mod laws;
#[path = "../../../rust/crates/temporal/src/event_step.rs"]
mod production;
#[path = "../../../rust/crates/query/src/owned_range_order.rs"]
mod owned_range_order;

fn main() {}

#[cfg(not(verus_keep_ghost))]
const CORRESPONDENCE: &[&str] = &[
    include_str!("../../../rust/crates/query/src/scan.rs"),
    include_str!("../../../rust/crates/query/src/settings.rs"),
    include_str!("../../../rust/crates/query/src/owned_range.rs"),
    include_str!("../../../rust/crates/query/src/owned_journal.rs"),
    include_str!("../../../rust/crates/node/src/node/owned_changes.rs"),
    include_str!("../../../rust/crates/node/src/node/hosted_observation.rs"),
    include_str!("../../../rust/crates/tx/src/resolver.rs"),
    include_str!("../../../rust/crates/multi/src/archive/writable_world/held.rs"),
];
