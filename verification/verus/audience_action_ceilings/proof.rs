#![feature(proc_macro_hygiene)]

extern crate self as rtdb_storage;

#[path = "../../../rust/crates/storage/src/audience_work/decision.rs"]
mod decision;
#[path = "../../../rust/crates/storage/src/audience_work/drain.rs"]
mod drain;
#[path = "../../../rust/crates/flight-sql/src/config/audience_limits.rs"]
mod audience_limits;
mod laws;

mod audience_work {
    pub use crate::decision::Limits;
}

fn main() {}
