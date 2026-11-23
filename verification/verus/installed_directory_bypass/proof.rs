#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/directory/src/installed_reservation.rs"]
mod production;
#[path = "../../../rust/crates/multi/src/directory_bypass.rs"]
mod node;
mod laws;

fn main() {}
