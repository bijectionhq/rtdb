#![feature(proc_macro_hygiene)]

#[path = "../../../rust/crates/multi/src/journal/owned_read.rs"]
mod production;
mod laws;

#[derive(Clone, Copy)]
pub struct Hold {
    pub owner: u128,
    pub nonce: u128,
    pub expires_at: u64,
    pub from: u64,
    pub through: u64,
}

pub enum Command {
    Barrier,
    ProtectCheckpoint { root: [u8; 32], from: u64, through: u64 },
}

pub enum Operation {
    Route,
    Release { hold: Hold },
}

fn main() {}
