#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/directory/src/receiver_clock_step.rs"]
mod production;
mod laws;

fn main() {}

#[cfg(not(verus_keep_ghost))]
const CORRESPONDENCE: &[&str] = &[
    include_str!("../../../rust/crates/directory/src/receiver_clock.rs"),
    include_str!("../../../rust/crates/directory/src/dynamo.rs"),
    include_str!("../../../rust/crates/multi/src/receiver_clock.rs"),
    include_str!("../../../rust/crates/tx/src/resolver.rs"),
    include_str!("../../../rust/crates/node/src/node.rs"),
];
