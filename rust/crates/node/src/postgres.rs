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

/// A node-local remote as seen by the PostgreSQL source factory.
pub enum PostgresSourceRemote {
    Postgres(Remote),
    /// The remote type name reported to clients. Its provider owns the connection settings.
    Other(&'static str),
}

pub struct PostgresSourceFactory {
    remotes: BTreeMap<String, PostgresSourceRemote>,
    indexers: BTreeMap<String, Arc<dyn PostgresIndexerFactory>>,
    memory: Arc<MemoryBudget>,
    metrics: Option<Arc<dyn SourceMetricsFactory>>,
}
impl PostgresSourceFactory {
    /// Remote credentials remain in this node-local registry; durable source messages contain aliases only.
    pub fn new(remotes: BTreeMap<String, Remote>, memory: Arc<MemoryBudget>) -> Result<Self> {
        let remotes = remotes.into_iter().map(|(alias, remote)| (alias, PostgresSourceRemote::Postgres(remote))).collect();
        Self::from_remote_registry(remotes, memory)
    }

    /// Accepts all configured aliases and validates PostgreSQL connection settings without opening connections.
    pub fn from_remote_registry(remotes: BTreeMap<String, PostgresSourceRemote>, memory: Arc<MemoryBudget>) -> Result<Self> {
        for remote in remotes.values() {
            if let PostgresSourceRemote::Postgres(remote) = remote {
                remote.validate()?;
            }
        }
        Ok(Self { remotes, indexers: BTreeMap::new(), memory, metrics: None })
    }
    pub fn with_metrics(mut self, metrics: Arc<dyn SourceMetricsFactory>) -> Self { self.metrics = Some(metrics); self }
    pub fn with_indexer(mut self, protobuf_type_url: String, factory: Arc<dyn PostgresIndexerFactory>) -> Result<Self> {
        if protobuf_type_url == DIRECT_MIRROR_TYPE || self.indexers.contains_key(&protobuf_type_url) { return Err(invalid("PostgreSQL indexer configuration type is already registered")); }
        self.indexers.insert(protobuf_type_url, factory);
        Ok(self)
    }
    fn configuration(&self, configuration: &Any) -> Result<(SourceConfig, &Remote)> {
        let source = SourceConfig::decode(configuration)?;
        let alias = source.remote.as_str();
        let remote = match self.remotes.get(alias) {
            Some(PostgresSourceRemote::Postgres(remote)) => remote,
            Some(&PostgresSourceRemote::Other(actual_type)) => return Err(Anomaly::incorrect(
                "xtdb.postgres/wrong-remote-type",
                format!("remote '{alias}' is a {actual_type}, expected a !Postgres remote"),
            ).with_context("alias", alias.into()).with_context("actualType", actual_type.into())),
            None => return Err(Anomaly::incorrect(
                "xtdb.postgres/missing-remote",
                format!("no remote configured with alias '{alias}' — add a '!Postgres' entry under 'remotes:' in node config"),
            ).with_context("alias", alias.into())),
        };
        if let Some(indexer) = &source.indexer {
            if indexer.type_url != DIRECT_MIRROR_TYPE {
                self.indexers.get(&indexer.type_url).ok_or_else(|| invalid(format!("No native PostgreSQL indexer registered for '{}'", indexer.type_url)))?.validate(indexer)?;
            }
        }
        Ok((source, remote))
    }
}
#[async_trait]
impl ExternalSourceFactory for PostgresSourceFactory {
    fn validate(&self, configuration: &Any) -> Result<()> { self.configuration(configuration).map(|_| ()) }
    async fn open(&self, configuration: &Any, database: &str, _: Arc<rtdb_storage::IoRegion>) -> Result<Box<dyn ExternalSource>> {
        let configuration_charge = self.memory.reserve(configuration.value.len().checked_mul(512).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(allocation_overflow)?)?;
        let (source, remote) = self.configuration(configuration)?;
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
