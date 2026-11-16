use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use prost_types::Any;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_pg_source::{PgIndexer, Remote, SourceConfig, SourceDriver, SourceMetrics, SourceMetricsFactory, DIRECT_MIRROR_TYPE};
use rtdb_replication::{DriverStatus, TermHandle};
use rtdb_types::{Anomaly, Result};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::{ExternalSource, ExternalSourceDriver, ExternalSourceFactory};

pub trait PostgresIndexerFactory: Send + Sync {
    fn validate(&self, configuration: &Any) -> Result<()>;
    fn open(&self, configuration: &Any) -> Result<Arc<dyn PgIndexer>>;
}

pub struct PostgresSourceFactory {
    remotes: BTreeMap<String, Remote>,
    indexers: BTreeMap<String, Arc<dyn PostgresIndexerFactory>>,
    memory: Arc<MemoryBudget>,
    metrics: Option<Arc<dyn SourceMetricsFactory>>,
}
impl PostgresSourceFactory {
    /// Remote credentials remain in this node-local registry; durable source messages contain aliases only.
    pub fn new(remotes: BTreeMap<String, Remote>, memory: Arc<MemoryBudget>) -> Result<Self> {
        for remote in remotes.values() { remote.validate()?; }
        Ok(Self { remotes, indexers: BTreeMap::new(), memory, metrics: None })
    }
    pub fn with_metrics(mut self, metrics: Arc<dyn SourceMetricsFactory>) -> Self { self.metrics = Some(metrics); self }
    pub fn with_indexer(mut self, protobuf_type_url: String, factory: Arc<dyn PostgresIndexerFactory>) -> Result<Self> {
        if protobuf_type_url == DIRECT_MIRROR_TYPE || self.indexers.contains_key(&protobuf_type_url) { return Err(invalid("PostgreSQL indexer configuration type is already registered")); }
        self.indexers.insert(protobuf_type_url, factory);
        Ok(self)
    }
    fn configuration(&self, configuration: &Any) -> Result<SourceConfig> {
        let source = SourceConfig::decode(configuration)?;
        if !self.remotes.contains_key(&source.remote) { return Err(invalid(format!("No PostgreSQL remote configured with alias '{}'", source.remote))); }
        if let Some(indexer) = &source.indexer {
            if indexer.type_url != DIRECT_MIRROR_TYPE {
                self.indexers.get(&indexer.type_url).ok_or_else(|| invalid(format!("No native PostgreSQL indexer registered for '{}'", indexer.type_url)))?.validate(indexer)?;
            }
        }
        Ok(source)
    }
}
#[async_trait]
impl ExternalSourceFactory for PostgresSourceFactory {
    fn validate(&self, configuration: &Any) -> Result<()> { self.configuration(configuration).map(|_| ()) }
    async fn open(&self, configuration: &Any, database: &str) -> Result<Box<dyn ExternalSource>> {
        let configuration_charge = self.memory.reserve(configuration.value.len().checked_mul(512).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(allocation_overflow)?)?;
        let source = self.configuration(configuration)?;
        let remote = self.remotes.get(&source.remote).ok_or_else(|| invalid("PostgreSQL remote is absent"))?;
        let remote_bound = [&remote.hostname, &remote.database, &remote.username, &remote.password].into_iter().try_fold(4096usize, |bytes, value| bytes.checked_add(value.len()).ok_or_else(allocation_overflow))?;
        let remote_charge = self.memory.reserve(remote_bound)?;
        let remote = remote.clone();
        let indexer = source.indexer.as_ref().filter(|indexer| indexer.type_url != DIRECT_MIRROR_TYPE).map(|indexer| {
            self.indexers.get(&indexer.type_url).ok_or_else(|| invalid("PostgreSQL indexer is absent"))?.open(indexer)
        }).transpose()?;
        let metrics = self.metrics.as_ref().map(|metrics| metrics.register_source(database, &source.slot_name)).transpose()?;
        Ok(Box::new(PostgresSource { remote, source, memory: Arc::clone(&self.memory), indexer, metrics, reservations: [configuration_charge, remote_charge] }))
    }
}
struct PostgresSource { remote: Remote, source: SourceConfig, memory: Arc<MemoryBudget>, indexer: Option<Arc<dyn PgIndexer>>, metrics: Option<Arc<dyn SourceMetrics>>, reservations: [Arc<MemoryReservation>; 2] }
#[async_trait]
impl ExternalSource for PostgresSource {
    async fn assign(&mut self) -> Result<Box<dyn ExternalSourceDriver>> {
        let bound = self.reservations.iter().try_fold(0usize, |bytes, reservation| bytes.checked_add(reservation.bytes()).ok_or_else(allocation_overflow))?;
        let reservation = self.memory.reserve(bound)?;
        let driver = SourceDriver::new(self.remote.clone(), self.source.clone(), Arc::clone(&self.memory), self.indexer.clone())?;
        let driver = match &self.metrics { Some(metrics) => driver.with_metrics(Arc::clone(metrics)), None => driver };
        Ok(Box::new(PostgresDriver { driver, _reservation: reservation }))
    }
}
struct PostgresDriver { driver: SourceDriver, _reservation: Arc<MemoryReservation> }
#[async_trait]
impl ExternalSourceDriver for PostgresDriver {
    async fn run(self: Box<Self>, term: TermHandle, after: Option<Arc<[u8]>>, blocks: watch::Receiver<DriverStatus>, cancellation: CancellationToken) -> Result<()> {
        let Self { driver, _reservation } = *self;
        driver.run(term, after, blocks, cancellation).await
    }
}
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("xtdb.postgres/invalid-config", message) }
fn allocation_overflow() -> Anomaly { invalid("Source allocation exceeds addressable memory") }
