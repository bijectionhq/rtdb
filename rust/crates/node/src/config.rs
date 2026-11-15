use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use prost_types::Any;
use rtdb_maintenance::{GarbageConfig, MergeConfig};
use rtdb_query::QueryOptions;
use rtdb_replication::{DriverStatus, ReplicationConfig, TermHandle};
use rtdb_trie::TrieConfig;
use rtdb_types::{Result, Timestamp};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::Membership;

#[derive(Clone, Debug)]
pub struct NodeOptions {
    pub membership: Membership,
    pub read_only: bool,
    pub default_timezone: String,
    pub execution_memory_bytes: usize,
    pub cache_bytes: usize,
    pub command_capacity: usize,
    pub file_size_target: i64,
    pub trie: TrieConfig,
    pub query: QueryOptions,
    pub replication: ReplicationConfig,
    pub compaction: MergeConfig,
    pub compaction_threads: usize,
    pub garbage: GarbageConfig,
    pub automatic_compaction: bool,
    pub automatic_gc: bool,
}

impl Default for NodeOptions {
    fn default() -> Self {
        Self { membership: Membership::Durable { primary: Default::default(), skipped: Default::default() }, read_only: false,
            default_timezone: "UTC".into(), execution_memory_bytes: 512 * 1024 * 1024, cache_bytes: 256 * 1024 * 1024,
            command_capacity: 64, file_size_target: 100 * 1024 * 1024, trie: TrieConfig::default(), query: QueryOptions::default(),
            replication: ReplicationConfig::default(), compaction: MergeConfig::default(), compaction_threads: 2,
            garbage: GarbageConfig::default(), automatic_compaction: true, automatic_gc: true }
    }
}

pub trait Clock: Send + Sync { fn now(&self) -> Timestamp; }
pub struct SystemClock;
impl Clock for SystemClock { fn now(&self) -> Timestamp { Timestamp(chrono::Utc::now().timestamp_micros()) } }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatabaseAccess { Read, Write }

#[async_trait]
pub trait SessionIdentity: Send + Sync {
    fn user(&self) -> Option<&str>;
    async fn revalidate(&self, now: Timestamp) -> Result<()>;
    fn authorize_database(&self, database: &str, access: DatabaseAccess) -> Result<()>;
}

#[async_trait]
pub trait ExternalSourceDriver: Send {
    /// Resume uses admitted applied progress; upstream confirmation uses only adopted block progress.
    /// The caller cancels and joins this run before assigning another leader term.
    async fn run(self: Box<Self>, term: TermHandle, after: Option<Arc<[u8]>>, blocks: watch::Receiver<DriverStatus>, cancellation: CancellationToken) -> Result<()>;
}

#[async_trait]
pub trait ExternalSourceFactory: Send + Sync {
    fn validate(&self, configuration: &Any) -> Result<()>;
    /// Returns an unstarted source driver whose resources are owned by its next `run` call.
    async fn open(&self, configuration: &Any, database: &str) -> Result<Box<dyn ExternalSourceDriver>>;
}

pub type ExternalSources = BTreeMap<String, Arc<dyn ExternalSourceFactory>>;
