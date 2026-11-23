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
    /// Enables native owned relations for installations running a compatible engine on every member.
    pub owned_relations: bool,
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
    /// Concurrent artefact builds per database; 0 turns the artefact lane off.
    pub artefact_threads: usize,
    pub automatic_artefacts: bool,
    /// Whether scans consult the node's accelerators; results are the same either way.
    pub scan_acceleration: bool,
    pub ordered_index_arrow_references: bool,
    pub ordered_index_arrow_references_on_private_storage: bool,
}

impl NodeOptions {
    /// The fixed UTC offset used when no node time zone is configured.
    pub const DEFAULT_TIMEZONE: &str = "Z";
}

impl Default for NodeOptions {
    fn default() -> Self {
        Self { membership: Membership::Durable { primary: Default::default(), skipped: Default::default() }, read_only: false, owned_relations: false,
            default_timezone: Self::DEFAULT_TIMEZONE.into(), execution_memory_bytes: 512 * 1024 * 1024, cache_bytes: 256 * 1024 * 1024,
            command_capacity: 64, file_size_target: 100 * 1024 * 1024, trie: TrieConfig::default(), query: QueryOptions::default(),
            replication: ReplicationConfig::default(), compaction: MergeConfig::default(), compaction_threads: 2,
            garbage: GarbageConfig::default(), automatic_compaction: true, automatic_gc: true,
            artefact_threads: 1, automatic_artefacts: true, scan_acceleration: true, ordered_index_arrow_references: false,
            ordered_index_arrow_references_on_private_storage: true }
    }
}

pub trait Clock: Send + Sync {
    fn now(&self) -> Timestamp;
    /// Supplies an exact query-clock reading; the default lifts the durable microsecond clock.
    fn query_instant(&self) -> rtdb_types::Instant { self.now().into() }
    /// Microseconds on a clock that never steps back, counted from an arbitrary origin in this process.
    /// Read leases and the collector's wait measure durations on it, so they assume a bounded rate drift between nodes rather than synchronized clocks.
    fn monotonic_micros(&self) -> i64 { process_monotonic_micros() }
}

pub fn process_monotonic_micros() -> i64 {
    static ORIGIN: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    i64::try_from(ORIGIN.get_or_init(std::time::Instant::now).elapsed().as_micros()).unwrap_or(i64::MAX)
}
pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> Timestamp { Timestamp(chrono::Utc::now().timestamp_micros()) }
    fn query_instant(&self) -> rtdb_types::Instant {
        let now = chrono::Utc::now();
        rtdb_types::Instant::new(now.timestamp(), now.timestamp_subsec_nanos()).expect("system clock has normalized nanoseconds")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DatabaseAccess { Read, Write }

#[async_trait]
pub trait SessionIdentity: Send + Sync {
    fn user(&self) -> Option<&str>;
    async fn revalidate(&self, now: Timestamp) -> Result<()>;
    fn authorize_database(&self, database: &str, access: DatabaseAccess) -> Result<()>;
    /// Whether another party may re-establish this identity from its name alone: true of a password or trusted principal, false of one whose tokens must keep being revalidated.
    fn delegable(&self) -> bool { false }
}

#[async_trait]
pub trait ExternalSourceDriver: Send {
    /// Resume uses admitted applied progress. Upstream confirmation requires a recoverable token.
    /// The caller cancels and joins this run before assigning another leader term.
    async fn run(self: Box<Self>, term: TermHandle, after: Option<Arc<[u8]>>, blocks: watch::Receiver<DriverStatus>, cancellation: CancellationToken) -> Result<()>;
}

#[async_trait]
pub trait ExternalSource: Send {
    /// The previous assignment must be cancelled and joined before requesting the next driver.
    async fn assign(&mut self) -> Result<Box<dyn ExternalSourceDriver>>;
}

#[async_trait]
pub trait ExternalSourceFactory: Send + Sync {
    fn validate(&self, configuration: &Any) -> Result<()>;
    /// Opens one database-scoped source; its assignment-independent state survives leader changes.
    /// Retained I/O must use `io`; database teardown drains this region after source workers join, including cancelled callers' work.
    async fn open(&self, configuration: &Any, database: &str, io: Arc<rtdb_storage::IoRegion>) -> Result<Box<dyn ExternalSource>>;
}

pub type ExternalSources = BTreeMap<String, Arc<dyn ExternalSourceFactory>>;
