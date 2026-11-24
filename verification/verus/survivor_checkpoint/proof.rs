#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/replication/src/sql_survivor/selection.rs"]
mod production;
#[path = "../../../rust/crates/multi/src/journal/custody/member_declaration.rs"]
mod member_production;

mod laws;

fn main() {}
