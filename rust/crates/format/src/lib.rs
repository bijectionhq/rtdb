pub mod basis;
pub mod bloom;
pub mod catalog;
pub mod columns;
pub mod data;
pub mod envelope;
pub mod hll;
pub mod ipc;
pub mod keys;
pub mod memory_trie;
pub mod metadata;
mod statistics;
pub mod transit;

pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/xtdb.rs"));
}

pub use generated::xtdb::{block, cols, database, log, postgres, trie};

pub const PROTOBUF_LOG_HEADER: u8 = 3;
pub const LEGACY_TX_HEADER: u8 = 0xff;
pub const LEGACY_FLUSH_HEADER: u8 = 2;
