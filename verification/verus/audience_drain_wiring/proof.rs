#![feature(proc_macro_hygiene)]

#[macro_use]
#[path = "../../../rust/crates/storage/src/audience_work/drain.rs"]
mod drain;
#[path = "../../../rust/crates/storage/src/audience_work/decision.rs"]
mod decision;
#[macro_use]
#[path = "../../../rust/crates/storage/src/audience_work/wiring.rs"]
mod wiring;
#[macro_use]
#[path = "../../../rust/crates/storage/src/io_region/provider_task.rs"]
mod provider_task;
#[macro_use]
#[path = "../../../rust/crates/flight-sql/src/service/audience_owner.rs"]
mod audience_owner;
#[path = "../foreign_contracts/async_io.rs"]
mod foreign;
mod laws;

fn main() {}
