#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/tx/src/system_time.rs"]
mod production;

mod laws;

fn main() {}

#[cfg(not(verus_keep_ghost))]
const CORRESPONDENCE: &[&str] = &[
    include_str!("../../../rust/crates/node/src/node.rs"),
    include_str!("../../../rust/crates/flight-sql/src/owned_actions.rs"),
    include_str!("../../../rust/crates/tx/src/decision_clock.rs"),
    include_str!("../../../rust/crates/tx/src/source.rs"),
    include_str!("../../../rust/crates/tx/src/resolver.rs"),
    include_str!("../../../rust/crates/node/src/node/funded_mutation_clock.rs"),
    include_str!("../../../rust/crates/tx/src/resolver/hosted_control.rs"),
    include_str!("../../../rust/crates/tx/src/resolver/prepared_landing.rs"),
];
