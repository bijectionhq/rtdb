use std::{collections::BTreeMap, sync::Arc};
use async_trait::async_trait;
use prost::Message;
use prost_types::Any;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_kafka::{KafkaClusterConfig, connect::{DateEnvironment, DocsIndexer, PluginRegistry, RecordIndexer, SinkRecord, SourceConfig, SourceDriver, SourceObserver, SourceOptions, SourceTransactions, DOCS_TYPE, proto}};
use rtdb_replication::{DriverStatus, TermHandle};
use rtdb_storage::IoRegion;
use rtdb_types::{Anomaly, Result, TableRef};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use crate::{Clock, ExternalSource, ExternalSourceDriver, ExternalSourceFactory};

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
    clock: Arc<dyn Clock>,
    initialized_at: i64,
}
impl KafkaConnectSourceFactory {
    /// `clock` supplies provider initialization and the retained formatter clock capability, independently of SQL sessions.
    pub fn new(remotes: BTreeMap<String, KafkaClusterConfig>, memory: Arc<MemoryBudget>, clock: Arc<dyn Clock>) -> Self {
        let initialized_at = clock.now().0.div_euclid(1000);
        Self { remotes, indexers: BTreeMap::new(), plugins: PluginRegistry::default(), options: SourceOptions::default(), memory, observer: None, clock, initialized_at }
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
    async fn open(&self, configuration: &Any, database: &str, io: Arc<IoRegion>) -> Result<Box<dyn ExternalSource>> {
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
            indexer: Arc::new(tokio::sync::Mutex::new(indexer)), memory: Arc::clone(&self.memory), database: database.into(), observer: self.observer.clone(),
            io, clock: Arc::clone(&self.clock), initialized_at: self.initialized_at, reservations: [configuration_charge, remote_charge] }))
    }
}
struct KafkaConnectSource {
    remote: KafkaClusterConfig, source: SourceConfig, options: SourceOptions, plugins: PluginRegistry,
    indexer: Arc<tokio::sync::Mutex<Box<dyn RecordIndexer>>>, memory: Arc<MemoryBudget>, database: String, observer: Option<Arc<dyn SourceObserver>>,
    io: Arc<IoRegion>, clock: Arc<dyn Clock>, initialized_at: i64,
    reservations: [Arc<MemoryReservation>; 2],
}
#[async_trait]
impl ExternalSource for KafkaConnectSource {
    async fn assign(&mut self) -> Result<Box<dyn ExternalSourceDriver>> {
        let bound = self.reservations.iter().try_fold(0usize, |bytes, reservation| bytes.checked_add(reservation.bytes()).ok_or_else(allocation_overflow))?;
        let reservation = self.memory.reserve(bound)?;
        let clock = Arc::new(ConnectDateClock(Arc::clone(&self.clock)));
        let environment = DateEnvironment::capture(&self.io, &self.memory, self.initialized_at, clock).await?;
        let indexer = Box::new(SharedRecordIndexer(Arc::clone(&self.indexer)));
        let driver = SourceDriver::new(self.remote.clone(), self.source.clone(), self.options.clone(), self.plugins.clone(), indexer, Arc::clone(&self.memory), environment)?;
        let driver = match &self.observer { Some(observer) => driver.with_observer(self.database.clone(), Arc::clone(observer)), None => driver };
        Ok(Box::new(KafkaConnectDriver { driver, _reservation: reservation }))
    }
}
struct SharedRecordIndexer(Arc<tokio::sync::Mutex<Box<dyn RecordIndexer>>>);
struct ConnectDateClock(Arc<dyn Clock>);
impl rtdb_kafka::connect::DateClock for ConnectDateClock { fn now_millis(&self) -> i64 { self.0.now().0.div_euclid(1000) } }
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
    struct EnvironmentClock(AtomicUsize);
    impl Clock for EnvironmentClock {
        fn now(&self) -> rtdb_types::Timestamp {
            rtdb_types::Timestamp(1_600_000_000_000_000 + self.0.fetch_add(1, Ordering::SeqCst) as i64 * 1_000_000)
        }
    }
    impl KafkaIndexerFactory for Factory {
        fn validate(&self, _configuration: &Any) -> Result<()> { Ok(()) }
        fn open(&self, _configuration: &Any) -> Result<Box<dyn RecordIndexer>> {
            self.opens.fetch_add(1, Ordering::SeqCst); Ok(Box::new(Indexer { drops: Arc::clone(&self.drops) }))
        }
    }
    #[tokio::test]
    async fn source_opens_custom_indexer_once_and_retains_it_across_assignment_drivers() {
        let memory = MemoryBudget::new(64 << 20);
        let io = IoRegion::new();
        let clock = Arc::new(EnvironmentClock(AtomicUsize::new(0)));
        let opens = Arc::new(AtomicUsize::new(0)); let drops = Arc::new(AtomicUsize::new(0));
        let factory = KafkaConnectSourceFactory::new(BTreeMap::from([("remote".into(), KafkaClusterConfig::new("unused.invalid"))]), Arc::clone(&memory), clock.clone())
            .with_indexer("fixture".into(), Arc::new(Factory { opens: Arc::clone(&opens), drops: Arc::clone(&drops) })).unwrap();
        let config = SourceConfig { remote: "remote".into(), topic: "topic".into(), indexer: Any { type_url: "fixture".into(), value: Vec::new() },
            connect_config: BTreeMap::from([("key.converter".into(), "org.apache.kafka.connect.storage.StringConverter".into()), ("value.converter".into(), "org.apache.kafka.connect.storage.StringConverter".into())]) }.encode();
        let mut source = factory.open(&config, "db", Arc::clone(&io)).await.unwrap();
        let retained = memory.used_bytes();
        let first = source.assign().await.unwrap(); assert!(memory.used_bytes() > retained); drop(first);
        assert_eq!(memory.used_bytes(), retained);
        let second = source.assign().await.unwrap();
        assert_eq!(opens.load(Ordering::SeqCst), 1); assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(clock.0.load(Ordering::SeqCst), 3);
        drop(source); assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(second); assert_eq!(drops.load(Ordering::SeqCst), 1); assert_eq!(memory.used_bytes(), 0);
        io.close_and_wait().await;
    }
    #[tokio::test]
    async fn assignment_environment_admission_failure_releases_its_partial_owners() {
        for closed in [false, true] {
            let memory = MemoryBudget::new(if closed { 64 << 20 } else { 1 << 20 });
            let io = IoRegion::new();
            let opens = Arc::new(AtomicUsize::new(0)); let drops = Arc::new(AtomicUsize::new(0));
            let factory = KafkaConnectSourceFactory::new(BTreeMap::from([("remote".into(), KafkaClusterConfig::new("unused.invalid"))]), Arc::clone(&memory), Arc::new(crate::SystemClock))
                .with_indexer("fixture".into(), Arc::new(Factory { opens: Arc::clone(&opens), drops: Arc::clone(&drops) })).unwrap();
            let config = SourceConfig { remote: "remote".into(), topic: "topic".into(), indexer: Any { type_url: "fixture".into(), value: Vec::new() },
                connect_config: BTreeMap::from([("key.converter".into(), "org.apache.kafka.connect.storage.StringConverter".into()), ("value.converter".into(), "org.apache.kafka.connect.storage.StringConverter".into())]) }.encode();
            let mut source = factory.open(&config, "db", Arc::clone(&io)).await.unwrap();
            let retained = memory.used_bytes();
            if closed { io.close_and_wait().await; }
            assert!(source.assign().await.is_err());
            assert_eq!(memory.used_bytes(), retained);
            assert_eq!(opens.load(Ordering::SeqCst), 1);
            assert_eq!(drops.load(Ordering::SeqCst), 0);
            drop(source);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(memory.used_bytes(), 0);
            io.close_and_wait().await;
        }
    }
    #[tokio::test]
    async fn cancelled_environment_capture_retains_database_admission_until_its_result_is_released() {
        use std::{future::{Future, poll_fn}, task::Poll};
        struct CaptureClock {
            calls: AtomicUsize,
            gate: std::sync::Mutex<Option<(tokio::sync::oneshot::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
        }
        impl Clock for CaptureClock {
            fn now(&self) -> rtdb_types::Timestamp {
                if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
                    let (entered, release) = self.gate.lock().unwrap().take().unwrap();
                    entered.send(()).unwrap();
                    release.recv().unwrap();
                }
                rtdb_types::Timestamp(1_600_000_000_000_000)
            }
        }
        let memory = MemoryBudget::new(64 << 20);
        let node_io = IoRegion::new();
        let io = node_io.child();
        let sibling = node_io.child();
        let (entered, started) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        let clock = Arc::new(CaptureClock { calls: AtomicUsize::new(0), gate: std::sync::Mutex::new(Some((entered, held))) });
        let factory = KafkaConnectSourceFactory::new(BTreeMap::from([("remote".into(), KafkaClusterConfig::new("unused.invalid"))]), Arc::clone(&memory), clock);
        let config = SourceConfig { remote: "remote".into(), topic: "topic".into(), indexer: Any { type_url: DOCS_TYPE.into(), value: proto::DocsIndexerConfig { table: Some("docs".into()) }.encode_to_vec() },
            connect_config: BTreeMap::from([("key.converter".into(), "org.apache.kafka.connect.storage.StringConverter".into()), ("value.converter".into(), "org.apache.kafka.connect.storage.StringConverter".into())]) }.encode();
        let mut source = factory.open(&config, "db", Arc::clone(&io)).await.unwrap();
        let mut assignment = Box::pin(source.assign());
        poll_fn(|context| { assert!(assignment.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        started.await.unwrap();
        drop(assignment);
        drop(source);
        assert!(memory.used_bytes() >= 4 << 20);
        let mut close = Box::pin(io.close_and_wait());
        poll_fn(|context| { assert!(close.as_mut().poll(context).is_pending()); Poll::Ready(()) }).await;
        assert!(matches!(io.run_blocking(|| Ok(())).await, Err(rtdb_storage::Error::Closed(_))));
        sibling.run_blocking(|| Ok(())).await.unwrap();
        release.send(()).unwrap();
        close.await;
        assert_eq!(memory.used_bytes(), 0);
        node_io.close_and_wait().await;
    }
}
