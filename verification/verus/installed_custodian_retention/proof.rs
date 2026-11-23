#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/multi/src/journal/retention_decision.rs"]
mod production;
mod laws;

#[derive(Clone, Copy)]
pub struct Cut {
    pub next_lsn: u64,
    pub history: [u8; 32],
}

fn main() {}
