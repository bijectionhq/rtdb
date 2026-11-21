pub mod basis;
pub mod bloom;
pub mod catalog;
pub mod columns;
pub mod config;
pub mod data;
pub mod diagnostics;
pub mod envelope;
pub mod hll;
pub mod ipc;
mod ipc_decimal;
pub mod keys;
pub mod memory_trie;
pub mod metadata;
mod statistics;
pub mod temporal_text;
pub mod transit;
mod transit_cell;

pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/xtdb.rs"));
}

pub use generated::xtdb::{block, cols, database, log, postgres, trie};
impl From<rtdb_types::PhysicalObjectVersion> for log::proto::PublishedObjectVersion {
    fn from(object: rtdb_types::PhysicalObjectVersion) -> Self {
        let rtdb_types::PhysicalObjectVersion { namespace, key, version } = object;
        Self { bucket: Some(namespace.bucket), endpoint: namespace.endpoint, region: namespace.region,
            key: Some(key), version: Some(version) }
    }
}


pub const PROTOBUF_LOG_HEADER: u8 = 3;
pub const LEGACY_TX_HEADER: u8 = 0xff;
pub const LEGACY_FLUSH_HEADER: u8 = 2;
