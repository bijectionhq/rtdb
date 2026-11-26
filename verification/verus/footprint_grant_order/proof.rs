//! Verus harness for RTDB's shipped SS8 durable footprint grant, strict
//! work-key age rotation, lease-loss transfer, work-keyed OCC debit, and
//! all-writer (Flight + SQL) fence.
#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/tx/src/resolver/footprint_grant.rs"]
pub mod footprint_grant;

pub mod laws;

fn main() {}
