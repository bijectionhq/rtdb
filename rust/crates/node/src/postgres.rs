use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use prost_types::Any;
use rtdb_columnar::MemoryBudget;
use rtdb_pg_source::{PgIndexer, Remote, SourceConfig, SourceDriver, DIRECT_MIRROR_TYPE};
use rtdb_replication::{DriverStatus, TermHandle};
use rtdb_types::{Anomaly, Result};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::{ExternalSourceDriver, ExternalSourceFactory};

pub trait PostgresIndexerFactory: Send + Sync {
    fn validate(&self, configuration: &Any) -> Result<()>;
    fn open(&self, configuration: &Any) -> Result<Arc<dyn PgIndexer>>;
}

pub struct PostgresSourceFactory {
    remotes: BTreeMap<String, Remote>,
    indexers: BTreeMap<String, Arc<dyn PostgresIndexerFactory>>,
    memory: Arc<MemoryBudget>,
}
impl PostgresSourceFactory {
    /// Remote credentials remain in this node-local registry; durable source messages contain aliases only.
    pub fn new(remotes: BTreeMap<String, Remote>, memory: Arc<MemoryBudget>) -> Result<Self> {
        for remote in remotes.values() { remote.validate()?; }
        Ok(Self { remotes, indexers: BTreeMap::new(), memory })
    }
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
    async fn open(&self, configuration: &Any, _database: &str) -> Result<Box<dyn ExternalSourceDriver>> {
        let source = self.configuration(configuration)?;
        let remote = self.remotes.get(&source.remote).ok_or_else(|| invalid("PostgreSQL remote is absent"))?.clone();
        let indexer = source.indexer.as_ref().filter(|indexer| indexer.type_url != DIRECT_MIRROR_TYPE).map(|indexer| {
            self.indexers.get(&indexer.type_url).ok_or_else(|| invalid("PostgreSQL indexer is absent"))?.open(indexer)
        }).transpose()?;
        Ok(Box::new(PostgresDriver(SourceDriver::new(remote, source, Arc::clone(&self.memory), indexer)?)))
    }
}
struct PostgresDriver(SourceDriver);
#[async_trait]
impl ExternalSourceDriver for PostgresDriver {
    async fn run(self: Box<Self>, term: TermHandle, after: Option<Arc<[u8]>>, blocks: watch::Receiver<DriverStatus>, cancellation: CancellationToken) -> Result<()> {
        self.0.run(term, after, blocks, cancellation).await
    }
}
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("xtdb.postgres/invalid-config", message) }
