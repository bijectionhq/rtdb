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
mod publication_identity;
pub mod temporal_text;
pub mod transit;
pub mod trie_retirement;
mod transit_cell;

pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/xtdb.rs"));
}

pub use generated::xtdb::{block, cols, database, log, postgres, trie};
impl From<rtdb_types::PhysicalObjectVersion> for log::proto::PublishedObjectVersion {
    fn from(object: rtdb_types::PhysicalObjectVersion) -> Self {
        let rtdb_types::PhysicalObjectVersion { namespace, key, version } = object;
        Self { bucket: Some(namespace.bucket), endpoint: namespace.endpoint, region: namespace.region,
            key: Some(key), version: Some(version), terminal: None }
    }
}

impl From<rtdb_types::PhysicalObjectPublication> for log::proto::PublishedObjectVersion {
    fn from(publication: rtdb_types::PhysicalObjectPublication) -> Self {
        match publication {
            rtdb_types::PhysicalObjectPublication::Version(version) => version.into(),
            rtdb_types::PhysicalObjectPublication::Terminal(publication) => {
                let rtdb_types::TerminalObjectPublication { namespace, key, allocation, etag } = publication;
                Self { bucket: Some(namespace.bucket), endpoint: namespace.endpoint, region: namespace.region,
                    key: Some(key), version: None, terminal: Some(log::proto::TerminalPublication {
                        allocation: Some(allocation.as_bytes().to_vec()), etag: Some(etag),
                    }) }
            }
        }
    }
}

impl log::proto::PublishedObjectVersion {
    pub fn terminal_publication(&self) -> Option<rtdb_types::TerminalObjectPublication> {
        if self.version.is_some() { return None; }
        let terminal = self.terminal.as_ref()?;
        let allocation = uuid::Uuid::from_slice(terminal.allocation.as_deref()?).ok()?;
        if allocation.is_nil() { return None; }
        Some(rtdb_types::TerminalObjectPublication {
            namespace: rtdb_types::S3ObjectNamespace {
                bucket: self.bucket.clone()?, endpoint: self.endpoint.clone(), region: self.region.clone(),
            },
            key: self.key.clone()?, allocation, etag: terminal.etag.clone()?,
        })
    }
}


pub const PROTOBUF_LOG_HEADER: u8 = 3;
pub const LEGACY_TX_HEADER: u8 = 0xff;
pub const LEGACY_FLUSH_HEADER: u8 = 2;
