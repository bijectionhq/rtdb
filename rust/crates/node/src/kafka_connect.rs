use std::{collections::BTreeMap, sync::Arc};
use async_trait::async_trait;
use prost::Message;
use prost_types::Any;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_kafka::{KafkaClusterConfig, connect::{DocsIndexer, PluginRegistry, RecordIndexer, SinkRecord, SourceConfig, SourceDriver, SourceObserver, SourceOptions, SourceTransactions, DOCS_TYPE, proto}};
use rtdb_replication::{DriverStatus, TermHandle};
use rtdb_types::{Anomaly, Result, TableRef};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use crate::{ExternalSource, ExternalSourceDriver, ExternalSourceFactory};

pub trait KafkaIndexerFactory: Send + Sync {
    fn validate(&self, configuration: &Any) -> Result<()>;
    fn open(&self, configuration: &Any) -> Result<Box<dyn RecordIndexer>>;
}
pub struct KafkaConnectSourceFactory {
    remotes: BTreeMap<String, KafkaClusterConfig>,
    indexers: BTreeMap<String, Arc<dyn KafkaIndexerFactory>>,
    plugins: PluginRegistry,
    options: SourceOptions,
    memory: Arc<MemoryBudget>,
    observer: Option<Arc<dyn SourceObserver>>,
}
impl KafkaConnectSourceFactory {
    pub fn new(remotes: BTreeMap<String, KafkaClusterConfig>, memory: Arc<MemoryBudget>) -> Self {
        Self { remotes, indexers: BTreeMap::new(), plugins: PluginRegistry::default(), options: SourceOptions::default(), memory, observer: None }
    }
    pub fn with_observer(mut self, observer: Arc<dyn SourceObserver>) -> Self { self.observer = Some(observer); self }
    pub fn with_plugins(mut self, plugins: PluginRegistry) -> Self { self.plugins = plugins; self }
    pub fn with_options(mut self, options: SourceOptions) -> Result<Self> { options.validate()?; self.options = options; Ok(self) }
    pub fn with_indexer(mut self, type_url: String, indexer: Arc<dyn KafkaIndexerFactory>) -> Result<Self> {
        if type_url == DOCS_TYPE || self.indexers.contains_key(&type_url) { return Err(invalid("Indexer protobuf type is already registered")); }
        self.indexers.insert(type_url, indexer); Ok(self)
    }
    fn configuration(&self, configuration: &Any) -> Result<SourceConfig> {
        let source = SourceConfig::decode(configuration)?;
        source.validate()?;
        if !self.remotes.contains_key(&source.remote) { return Err(Anomaly::incorrect("xtdb.kafka-connect-source/missing-remote", format!("No Kafka remote configured with alias '{}'", source.remote))); }
        if source.indexer.type_url == DOCS_TYPE { docs_table(&source.indexer)?; }
        else { self.indexers.get(&source.indexer.type_url).ok_or_else(|| invalid("Kafka Connect indexer type is not registered"))?.validate(&source.indexer)?; }
        Ok(source)
    }
}
#[async_trait]
impl ExternalSourceFactory for KafkaConnectSourceFactory {
    fn validate(&self, configuration: &Any) -> Result<()> { self.configuration(configuration).map(|_| ()) }
    async fn open(&self, configuration: &Any, database: &str) -> Result<Box<dyn ExternalSource>> {
        let bound = configuration.value.len().checked_mul(512).and_then(|bytes| bytes.checked_add(database.len())).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(allocation_overflow)?;
        let configuration_charge = self.memory.reserve(bound)?;
        let source = self.configuration(configuration)?;
        let indexer = if source.indexer.type_url == DOCS_TYPE { Box::new(DocsIndexer::new(docs_table(&source.indexer)?)) as Box<dyn RecordIndexer> }
        else { self.indexers.get(&source.indexer.type_url).ok_or_else(|| invalid("Kafka Connect indexer type is not registered"))?.open(&source.indexer)? };
        let remote = self.remotes.get(&source.remote).ok_or_else(|| invalid("Kafka remote is absent"))?;
        let remote_bound = remote.properties_map.iter().try_fold(remote.bootstrap_servers.len().checked_add(4096).ok_or_else(allocation_overflow)?, |bytes, (key, value)|
            bytes.checked_add(key.len()).and_then(|bytes| bytes.checked_add(value.len())).and_then(|bytes| bytes.checked_add(512)).ok_or_else(allocation_overflow))?
            .checked_add(remote.properties_file.as_ref().map_or(0, |path| path.as_os_str().len())).and_then(|bytes| bytes.checked_add(remote.schema_registry_url.as_ref().map_or(0, String::len)))
            .and_then(|bytes| bytes.checked_add(self.plugins.clone_allocation_bound().ok()?)).ok_or_else(allocation_overflow)?;
        let remote_charge = self.memory.reserve(remote_bound)?;
        let remote = remote.clone();
        Ok(Box::new(KafkaConnectSource { remote, source, options: self.options.clone(), plugins: self.plugins.clone(),
            indexer: Arc::new(tokio::sync::Mutex::new(indexer)), memory: Arc::clone(&self.memory), database: database.into(), observer: self.observer.clone(), reservations: [configuration_charge, remote_charge] }))
    }
}
struct KafkaConnectSource {
    remote: KafkaClusterConfig, source: SourceConfig, options: SourceOptions, plugins: PluginRegistry,
    indexer: Arc<tokio::sync::Mutex<Box<dyn RecordIndexer>>>, memory: Arc<MemoryBudget>, database: String, observer: Option<Arc<dyn SourceObserver>>,
    reservations: [Arc<MemoryReservation>; 2],
}
#[async_trait]
impl ExternalSource for KafkaConnectSource {
    async fn assign(&mut self) -> Result<Box<dyn ExternalSourceDriver>> {
        let bound = self.reservations.iter().try_fold(0usize, |bytes, reservation| bytes.checked_add(reservation.bytes()).ok_or_else(allocation_overflow))?;
        let reservation = self.memory.reserve(bound)?;
        let indexer = Box::new(SharedRecordIndexer(Arc::clone(&self.indexer)));
        let driver = SourceDriver::new(self.remote.clone(), self.source.clone(), self.options.clone(), self.plugins.clone(), indexer, Arc::clone(&self.memory))?;
        let driver = match &self.observer { Some(observer) => driver.with_observer(self.database.clone(), Arc::clone(observer)), None => driver };
        Ok(Box::new(KafkaConnectDriver { driver, _reservation: reservation }))
    }
}
struct SharedRecordIndexer(Arc<tokio::sync::Mutex<Box<dyn RecordIndexer>>>);
#[async_trait]
impl RecordIndexer for SharedRecordIndexer {
    async fn index_records(&mut self, records: Vec<SinkRecord>, transactions: &mut SourceTransactions<'_>) -> Result<()> {
        self.0.lock().await.index_records(records, transactions).await
    }
}
struct KafkaConnectDriver { driver: SourceDriver, _reservation: Arc<MemoryReservation> }
#[async_trait]
impl ExternalSourceDriver for KafkaConnectDriver {
    async fn run(self: Box<Self>, term: TermHandle, after: Option<Arc<[u8]>>, blocks: watch::Receiver<DriverStatus>, cancellation: CancellationToken) -> Result<()> {
        let Self { driver, _reservation } = *self;
        driver.run(term, after, blocks, cancellation).await
    }
}
fn docs_table(configuration: &Any) -> Result<TableRef> {
    let config = proto::DocsIndexerConfig::decode(configuration.value.as_slice()).map_err(|cause| invalid(cause.to_string()))?;
    TableRef::parse(&config.table.unwrap_or_default())
}
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("xtdb.kafka-connect-source/invalid-config", message) }
fn allocation_overflow() -> Anomaly { invalid("Source allocation exceeds addressable memory") }

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Indexer { drops: Arc<AtomicUsize> }
    impl Drop for Indexer { fn drop(&mut self) { self.drops.fetch_add(1, Ordering::SeqCst); } }
    #[async_trait]
    impl RecordIndexer for Indexer {
        async fn index_records(&mut self, _records: Vec<SinkRecord>, _transactions: &mut SourceTransactions<'_>) -> Result<()> { Ok(()) }
    }
    struct Factory { opens: Arc<AtomicUsize>, drops: Arc<AtomicUsize> }
    impl KafkaIndexerFactory for Factory {
        fn validate(&self, _configuration: &Any) -> Result<()> { Ok(()) }
        fn open(&self, _configuration: &Any) -> Result<Box<dyn RecordIndexer>> {
            self.opens.fetch_add(1, Ordering::SeqCst); Ok(Box::new(Indexer { drops: Arc::clone(&self.drops) }))
        }
    }
    #[tokio::test]
    async fn source_opens_custom_indexer_once_and_retains_it_across_assignment_drivers() {
        let memory = MemoryBudget::new(1 << 22);
        let opens = Arc::new(AtomicUsize::new(0)); let drops = Arc::new(AtomicUsize::new(0));
        let factory = KafkaConnectSourceFactory::new(BTreeMap::from([("remote".into(), KafkaClusterConfig::new("unused.invalid"))]), Arc::clone(&memory))
            .with_indexer("fixture".into(), Arc::new(Factory { opens: Arc::clone(&opens), drops: Arc::clone(&drops) })).unwrap();
        let config = SourceConfig { remote: "remote".into(), topic: "topic".into(), indexer: Any { type_url: "fixture".into(), value: Vec::new() },
            connect_config: BTreeMap::from([("key.converter".into(), "org.apache.kafka.connect.storage.StringConverter".into()), ("value.converter".into(), "org.apache.kafka.connect.storage.StringConverter".into())]) }.encode();
        let mut source = factory.open(&config, "db").await.unwrap();
        let retained = memory.used_bytes();
        let first = source.assign().await.unwrap(); assert!(memory.used_bytes() > retained); drop(first);
        assert_eq!(memory.used_bytes(), retained);
        let second = source.assign().await.unwrap();
        assert_eq!(opens.load(Ordering::SeqCst), 1); assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(source); assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(second); assert_eq!(drops.load(Ordering::SeqCst), 1); assert_eq!(memory.used_bytes(), 0);
    }
}
