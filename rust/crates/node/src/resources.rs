use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{FutureExt, future::BoxFuture};
use prost_types::Any;
use rtdb_format::{database::proto::{DatabaseConfig, database_config}, keys::{CURRENT_STORAGE_VERSION, StorageRoot}};
use rtdb_storage::{IoRegion, LocalStore, MemoryStore, ObjectKey, ObjectStore, log::{AppendTimestampSource, LocalLog, LocalLogOptions, LocalLogSyncs, Log, MemoryLog, ReadOnlyLocalLog, ReadOnlyLocalLogOptions}};
use rtdb_storage::disk_cache::DiskCache;
use rtdb_types::{Anomaly, Result, SourcePosition, StorageEpoch, StorageVersion, Timestamp};
use tokio_util::sync::CancellationToken;

pub type ResourceDriver = BoxFuture<'static, Result<()>>;
pub type TransactionClock = dyn Fn() -> Result<Timestamp> + Send + Sync;

#[async_trait]
pub trait ReplicaReadIndex: Send + Sync {
    fn deployment(&self) -> Option<uuid::Uuid> { None }
    fn requires_anchored_catalog(&self) -> bool { false }

    /// Bounds writes completed before the call on the replica log opened with this capability.
    async fn confirm(&self) -> Result<rtdb_storage::log::LogOffset>;

    /// Keeps the protected source basis alive until the database applies it.
    async fn protect(&self, _applied: rtdb_storage::log::LogOffset) -> Result<ProtectedReadIndex> {
        Ok(ProtectedReadIndex {
            offset: self.confirm().await?,
            wait: std::time::Duration::from_millis(20),
            retention: None,
            certified_read: None,
        })
    }
}

pub struct ProtectedReadIndex {
    pub offset: rtdb_storage::log::LogOffset,
    pub wait: std::time::Duration,
    pub retention: Option<Arc<dyn Send + Sync>>,
    pub certified_read: Option<Arc<rtdb_types::residence_trace::CertifiedRead>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogAccess { ReadOnly, SourceWriter, ReadWrite }
impl LogAccess {
    pub fn source_read_only(self) -> bool { self == Self::ReadOnly }
    pub fn replica_read_only(self) -> bool { self != Self::ReadWrite }
}

pub struct OpenedLogs {
    pub source: Arc<dyn Log>,
    pub replica: Arc<dyn Log>,
    pub source_epoch: StorageEpoch,
    pub replica_epoch: StorageEpoch,
    pub placed_transfer: Option<rtdb_types::PlacedTransfer>,
    pub replica_read_index: Option<Box<dyn ReplicaReadIndex>>,
    pub await_authority: crate::AwaitAuthority,
    pub drivers: Vec<ResourceDriver>,
}

pub struct DatabaseResources {
    pub logs: OpenedLogs,
    pub store: Arc<dyn ObjectStore>,
    pub storage_epoch: StorageEpoch,
    pub storage_version: StorageVersion,
    /// The database closes admission and joins this region after its workers stop during teardown.
    pub io: Arc<IoRegion>,
}

pub struct AudienceReadStorage {
    pub source: uuid::Uuid,
    pub store: Arc<dyn ObjectStore>,
}

pub struct OpenedServingStores {
    pub ordinary: Arc<dyn ObjectStore>,
    pub audience: Option<AudienceReadStorage>,
}

pub struct DatabaseStorage {
    pub store: Arc<dyn ObjectStore>,
    pub audience_storage: Option<AudienceReadStorage>,
    /// How recovery finds the latest block: listing, unless the provider knows where to start.
    pub blocks: rtdb_replication::BlockDiscovery,
    pub checkpoint_metadata: Option<Arc<dyn Send + Sync>>,
    pub fragment_storage: Option<Arc<str>>,
    pub storage_epoch: StorageEpoch,
    pub storage_version: StorageVersion,
    /// The caller drains this region if later database assembly fails.
    pub io: Arc<IoRegion>,
}

#[async_trait]
pub trait LogFactory: Send + Sync {
    /// Opens distinct source and replica logs without spawning their drivers.
    /// Returned drivers finish all admitted appends when `cancellation` is cancelled.
    async fn open(&self, configuration: &Any, access: LogAccess, cancellation: CancellationToken) -> Result<OpenedLogs>;

    async fn open_for_default_erasure(&self, _configuration: &Any, _access: LogAccess, _cancellation: CancellationToken,
        _owner: uuid::Uuid, _topics: Option<[uuid::Uuid; 2]>,
    ) -> Result<OpenedLogs> {
        Err(Anomaly::unsupported("rtdb/default-erasure-history", "The log provider cannot reopen the original Default erasure history"))
    }
}

pub struct OpenedRecoveryStore {
    pub store: Arc<dyn ObjectStore>,
    /// The same admitted storage, when all object reads can use existing custody and audience transport.
    pub audience: Option<Arc<dyn ObjectStore>>,
    pub blocks: rtdb_replication::BlockDiscovery,
    /// Held through disk-cache and provider I/O for checkpoint discovery and load.
    pub checkpoint_metadata: Option<Arc<dyn Send + Sync>>,
    /// Identity of the actual opened storage for delegated scans.
    pub fragment_storage: Option<Arc<str>>,
    pub publication_sealing: Option<rtdb_storage::sealing::PublicationSealing>,
}

#[async_trait]
pub trait RemoteStorageFactory: Send + Sync {
    fn table_history(&self, _configuration: &Any) -> Result<Option<Arc<dyn rtdb_maintenance::TableHistorySource>>> { Ok(None) }

    /// The returned store is scoped to `storage_root`, under the provider's configured prefix.
    /// Retained provider operations belong to the supplied database region, including after caller cancellation.
    async fn open(&self, configuration: &Any, storage_root: &ObjectKey, io: Arc<IoRegion>) -> Result<Arc<dyn ObjectStore>>;
    /// Opens the store and chooses checkpoint discovery under the provider's admission rules.
    async fn open_for_recovery(&self, configuration: &Any, storage_root: &ObjectKey, io: Arc<IoRegion>) -> Result<OpenedRecoveryStore> {
        let store = self.open(configuration, storage_root, io).await?;
        let blocks = self.block_discovery(configuration).await?;
        Ok(OpenedRecoveryStore { store, audience: None, blocks, checkpoint_metadata: None, fragment_storage: None, publication_sealing: None })
    }
    /// These provider configurations use the Classic installation's key authority.
    fn uses_classic_custody(&self) -> bool { false }

    /// A remote store normally uses a disk cache. An isolated read-only
    /// factory may opt out to avoid retaining an extra physical copy.
    fn use_disk_cache(&self) -> bool { true }
    /// Wraps the ordinary disk view and any audience view admitted by `open_for_recovery`.
    async fn wrap_stores(&self, _configuration: &Any, cached: Arc<dyn ObjectStore>, _audience: Option<Arc<dyn ObjectStore>>, _publication_sealing: Option<rtdb_storage::sealing::PublicationSealing>) -> Result<OpenedServingStores> {
        Ok(OpenedServingStores { ordinary: cached, audience: None })
    }
    /// Identifies the raw objects returned by `open` before `wrap_stores`.
    /// Two configurations may share an identity when a matching immutable key has the same bytes in both.
    fn cache_identity<'a>(&self, configuration: &'a Any) -> Result<Cow<'a, [u8]>> {
        Ok(Cow::Borrowed(&configuration.value))
    }
    /// How recovery should find the store's latest block; a provider that tracks it spares recovery a listing.
    async fn block_discovery(&self, _configuration: &Any) -> Result<rtdb_replication::BlockDiscovery> { Ok(rtdb_replication::BlockDiscovery::List) }
}

#[cfg(feature = "multi")]
pub enum DeploymentTransfer {
    Restore {
        ticket: Vec<u8>,
    },
    Fork {
        pin: uuid::Uuid,
        basis: rtdb_types::rtdb_v1::Basis,
        retain_until: Option<rtdb_types::Timestamp>,
    },
    /// Private host operation for a source with an installed custody authority.
    ManagedFork {
        pin: uuid::Uuid,
        basis: rtdb_types::rtdb_v1::Basis,
    },
}

#[async_trait]
pub trait ResourceFactory: Send + Sync {
    fn supports_default_local_restore(&self) -> bool { false }
    /// Provisions or recovers an ordered registration before the node attaches it.
    async fn provision_managed_database(&self, _name: &str, _expected: Option<uuid::Uuid>) -> Result<(DatabaseConfig, uuid::Uuid)> {
        Err(Anomaly::unsupported("rtdb/deployment-catalog", "The runtime has no ordered deployment catalog"))
    }

    /// Routes an exact export to the current deployment writer.
    #[cfg(feature = "multi")]
    async fn export_managed_database(
        &self, _name: &str, _pin: uuid::Uuid, _basis: rtdb_types::rtdb_v1::Basis,
        _until: Option<rtdb_types::Timestamp>, _cancellation: &CancellationToken,
    ) -> Result<Vec<u8>> {
        Err(Anomaly::unsupported("rtdb/deployment-export", "The runtime has no managed checkpoint export authority"))
    }

    /// Installs an exact checkpoint before making its destination available for attachment.
    #[cfg(feature = "multi")]
    async fn transfer_managed_database(
        &self, _source: &str, _destination: &str, _transfer: DeploymentTransfer,
        _cancellation: &CancellationToken,
    ) -> Result<(DatabaseConfig, rtdb_types::rtdb_v1::Basis)> {
        Err(Anomaly::unsupported("rtdb/deployment-restore", "The runtime has no managed checkpoint transfer authority"))
    }

    #[cfg(feature = "multi")]
    async fn check_managed_checkpoint_read(
        &self, _name: &str, _incarnation: [u8; 16], _read: crate::ManagedCheckpointRead<'_>,
        _cancellation: &CancellationToken,
    ) -> Result<()> {
        Err(Anomaly::unsupported("rtdb/managed-checkpoint-read", "The runtime has no managed checkpoint read authority"))
    }

    /// Returns the authenticated erasure evidence and the latest time of confirmed unreadability.
    async fn erase_managed_database(&self, _name: &str) -> Result<(Vec<u8>, rtdb_types::Timestamp)> {
        Err(Anomaly::unsupported("rtdb/deployment-erasure", "The runtime has no deployment erasure authority"))
    }

    fn serving_driver(&self, _name: &str, _configuration: &DatabaseConfig, _cancellation: &CancellationToken)
        -> Result<Option<Arc<dyn crate::serving::ServingDriver>>> { Ok(None) }
    fn native_serving_authority(&self, _name: &str, _configuration: &DatabaseConfig)
        -> Result<Option<crate::serving::NativeServingAuthority>> { Ok(None) }
    fn native_checkpoint_kms(&self) -> Option<Arc<dyn rtdb_crypt::Kms>> { None }
    fn journal_native_sql(&self) -> bool { false }
    fn table_history(&self, _configuration: &DatabaseConfig) -> Result<Option<Arc<dyn rtdb_maintenance::TableHistorySource>>> { Ok(None) }
    fn has_private_storage(&self, _configuration: &DatabaseConfig) -> bool { false }
    fn token_kms(&self) -> Option<Arc<dyn rtdb_crypt::Kms>> { None }

    fn validate(&self, name: &str, configuration: &DatabaseConfig) -> Result<()>;
    /// Opens a fresh database storage region without opening logs; partial failures drain that region.
    /// The region's operations also belong to this factory's node region, which retains them after caller cancellation.
    async fn open_storage(&self, name: &str, configuration: &DatabaseConfig) -> Result<DatabaseStorage>;
    /// Opens unstarted log owners within the caller's opened storage region.
    /// Storage must come from this factory's `open_storage` for the same name and configuration.
    /// An error drops partial log owners; the caller must then drain the storage region.
    async fn open_logs(&self, name: &str, configuration: &DatabaseConfig, access: LogAccess, log_cancellation: CancellationToken, storage: &DatabaseStorage) -> Result<OpenedLogs>;
    /// Opens logs for native logical history, binding the replica journal's identity where supported.
    /// A read-only legacy journal can remain unbound. Storage and driver obligations follow [`Self::open_logs`].
    async fn open_logs_with_replica_lineage(&self, name: &str, configuration: &DatabaseConfig, access: LogAccess, log_cancellation: CancellationToken,
        storage: &DatabaseStorage,
    ) -> Result<OpenedLogs> {
        self.open_logs(name, configuration, access, log_cancellation, storage).await
    }
    /// No driver may run before this returns; an error releases partial owners and drains their admitted I/O.
    /// This convenience operation opens storage and logs without recovering persisted database state between them.
    async fn open(&self, name: &str, configuration: &DatabaseConfig, access: LogAccess, log_cancellation: CancellationToken) -> Result<DatabaseResources> {
        let storage = self.open_storage(name, configuration).await?;
        let logs = match self.open_logs(name, configuration, access, log_cancellation, &storage).await {
            Ok(logs) => logs,
            Err(error) => { storage.io.close_and_wait().await; return Err(error); }
        };
        Ok(DatabaseResources { logs, store: storage.store, storage_epoch: storage.storage_epoch, storage_version: storage.storage_version, io: storage.io })
    }
    /// The node calls this after its database workers join, or after an ordinary open failure.
    /// Closes this node's resource region and joins work retained beyond a cancelled I/O caller.
    /// Wrappers around another resource factory must forward this call.
    async fn close(&self) -> Result<()> { Ok(()) }
}

type NativeServingAuthorities = BTreeMap<String, crate::serving::NativeServingAuthority>;

pub struct NativeResources {
    pub maximum_log_message_bytes: usize,
    pub log_queue_capacity: usize,
    pub read_only_log: ReadOnlyLocalLogOptions,
    log_factories: BTreeMap<String, Arc<dyn LogFactory>>,
    storage_factories: BTreeMap<String, Arc<dyn RemoteStorageFactory>>,
    disk_cache: Option<Arc<DiskCache>>,
    pub(crate) classic_vault: Option<rtdb_storage::sealing::ClassicKeyVault>,
    token_kms: Option<Arc<dyn rtdb_crypt::Kms>>,
    native_checkpoint_kms: Option<Arc<dyn rtdb_crypt::Kms>>,
    local_checkpoint_vault: Option<PathBuf>,
    kafka_checkpoint_vault: Option<PathBuf>,
    io: Arc<IoRegion>,
    /// Shared by every local log this node opens, which it takes to sit on one drive.
    log_syncs: Arc<LocalLogSyncs>,
    memory_transaction_clock: Option<Arc<TransactionClock>>,
    local_log_clock: Arc<TransactionClock>,
    local_log_uses_clock_for_non_transactions: bool,
    serving_authorities: NativeServingAuthorities,
}

impl Default for NativeResources {
    fn default() -> Self {
        Self { maximum_log_message_bytes: 64 * 1024 * 1024, log_queue_capacity: 64, read_only_log: ReadOnlyLocalLogOptions::default(),
            log_factories: BTreeMap::new(), storage_factories: BTreeMap::new(), disk_cache: None, classic_vault: None, token_kms: None, native_checkpoint_kms: None, local_checkpoint_vault: None, kafka_checkpoint_vault: None, io: IoRegion::new(), log_syncs: LocalLogSyncs::new(),
            memory_transaction_clock: None, local_log_clock: Arc::new(|| Ok(crate::Clock::now(&crate::SystemClock))), local_log_uses_clock_for_non_transactions: false,
            serving_authorities: BTreeMap::new() }
    }
}

impl NativeResources {
    pub fn with_disk_cache(mut self, cache: Arc<DiskCache>) -> Self { self.disk_cache = Some(cache); self }
    pub fn with_token_kms(mut self, kms: Arc<dyn rtdb_crypt::Kms>) -> Self {
        self.token_kms = Some(kms);
        self
    }
    pub fn with_native_checkpoint_kms(mut self, kms: Arc<dyn rtdb_crypt::Kms>) -> Self {
        self.native_checkpoint_kms = Some(kms);
        self.local_checkpoint_vault = None;
        self.kafka_checkpoint_vault = None;
        self
    }
    /// Uses the original filesystem generation authority for a freshly sealed Local residence.
    pub fn with_local_checkpoint_kms(self, kms: rtdb_crypt::FileKms) -> Self {
        self.with_default_erasure_checkpoint_kms(kms, true, false)
    }

    pub fn with_default_erasure_checkpoint_kms(mut self, kms: rtdb_crypt::FileKms, local: bool, kafka: bool) -> Self {
        self.local_checkpoint_vault = local.then(|| kms.vault_path().to_owned());
        self.kafka_checkpoint_vault = kafka.then(|| kms.vault_path().to_owned());
        self.native_checkpoint_kms = Some(Arc::new(kms));
        self
    }
    /// Installs the same durable database incarnation and claim authority on each replica.
    pub fn with_native_serving_authority(mut self, database: impl Into<String>, authority: crate::serving::NativeServingAuthority) -> Self {
        self.serving_authorities.insert(database.into(), authority);
        self
    }
    pub fn with_classic_key_vault(mut self, vault: rtdb_storage::sealing::ClassicKeyVault) -> Self {
        self.classic_vault = Some(vault);
        self
    }

    /// Samples this clock for each transaction appended to an in-memory or local source log.
    /// Calls into a shared clock can originate from different databases concurrently.
    pub fn with_log_clock(mut self, clock: Arc<TransactionClock>) -> Self {
        self.memory_transaction_clock = Some(Arc::clone(&clock));
        self.local_log_clock = clock;
        self
    }
    pub fn with_local_log_clock(mut self, clock: Arc<dyn crate::Clock>, use_for_non_transactions: bool) -> Self {
        self.local_log_clock = Arc::new(move || Ok(clock.now()));
        self.local_log_uses_clock_for_non_transactions = use_for_non_transactions;
        self
    }
    pub fn register_log(mut self, protobuf_type: impl Into<String>, factory: Arc<dyn LogFactory>) -> Result<Self> {
        let name = protobuf_type.into();
        if self.log_factories.insert(name, factory).is_some() { return Err(invalid("A log factory is already registered for this protobuf type")); }
        Ok(self)
    }
    pub fn register_storage(mut self, protobuf_type: impl Into<String>, factory: Arc<dyn RemoteStorageFactory>) -> Result<Self> {
        let name = protobuf_type.into();
        if self.storage_factories.insert(name, factory).is_some() { return Err(invalid("A storage factory is already registered for this protobuf type")); }
        Ok(self)
    }
    fn local_log(&self, path: PathBuf, source: bool, read_only: bool, cancellation: CancellationToken, io: Arc<IoRegion>, bind_lineage: bool)
        -> Result<(Arc<dyn Log>, Option<ResourceDriver>)> {
        let keys = self.classic_vault.as_ref().map(|vault| vault.log_keys(&path, !read_only, Arc::clone(&io))).transpose()?;
        if read_only {
            let log = match keys {
                Some(keys) => ReadOnlyLocalLog::open_with_key_slots(path, self.read_only_log.clone(), io, keys)?,
                None => ReadOnlyLocalLog::open_in(path, self.read_only_log.clone(), io)?,
            };
            return Ok((log, None));
        }
        let timestamps = Arc::new(NativeLogTimestamps { clock: Arc::clone(&self.local_log_clock), source, use_for_non_transactions: self.local_log_uses_clock_for_non_transactions });
        let options = LocalLogOptions { maximum_message_bytes: self.maximum_log_message_bytes, queue_capacity: self.log_queue_capacity };
        let (log, driver) = if bind_lineage {
            LocalLog::open_replica(path, options, timestamps, io, keys, Arc::clone(&self.log_syncs))?
        } else {
            LocalLog::open_with_key_slots(path, options, timestamps, io, keys, Arc::clone(&self.log_syncs))?
        };
        Ok((log, Some(async move { driver.run(cancellation).await.map_err(Into::into) }.boxed())))
    }
}

struct NativeLogTimestamps { clock: Arc<TransactionClock>, source: bool, use_for_non_transactions: bool }
impl AppendTimestampSource for NativeLogTimestamps {
    fn timestamp(&self, payload: &[u8], _: rtdb_types::Timestamp) -> rtdb_storage::Result<rtdb_types::Timestamp> {
        use crate::Clock;
        let transaction = self.source && is_source_transaction(payload)?;
        if transaction || self.use_for_non_transactions {
            (self.clock)().map_err(|error| rtdb_storage::Error::Task(error.to_string()))
        } else {
            Ok(crate::SystemClock.now())
        }
    }
}

struct MemoryLogTransactionTimestamps(Arc<TransactionClock>);
impl AppendTimestampSource for MemoryLogTransactionTimestamps {
    fn timestamp(&self, payload: &[u8], supplied: Timestamp) -> rtdb_storage::Result<Timestamp> {
        if is_source_transaction(payload)? {
            (self.0)().map_err(|error| rtdb_storage::Error::Task(error.to_string()))
        } else {
            Ok(supplied)
        }
    }
}

fn is_source_transaction(payload: &[u8]) -> rtdb_storage::Result<bool> {
    rtdb_format::envelope::SourceMessage::is_transaction_encoded(payload)
        .map_err(|error| rtdb_storage::Error::Task(error.to_string()))
}

fn type_name(configuration: &Any) -> Result<&str> {
    configuration.type_url.rsplit('/').next().filter(|name| !name.is_empty()).ok_or_else(|| invalid("Backend configuration has no protobuf type"))
}

pub(crate) fn validate_database_shape(name: &str, configuration: &DatabaseConfig) -> Result<()> {
    let partitions = configuration.partitions.unwrap_or(0).max(1);
    if partitions > 1 {
        return Err(Anomaly::incorrect("xtdb/multi-partition-not-yet-enabled", format!("multi-partition external-source databases are not yet enabled (config declared partitions={partitions})"))
            .with_context("db-name", name.into()).with_context("partitions", partitions.into()));
    }
    Ok(())
}

#[async_trait]
impl ResourceFactory for NativeResources {
    fn supports_default_local_restore(&self) -> bool { self.classic_vault.is_none() && self.native_checkpoint_kms.is_none() }
    fn token_kms(&self) -> Option<Arc<dyn rtdb_crypt::Kms>> { self.token_kms.clone() }

    fn native_checkpoint_kms(&self) -> Option<Arc<dyn rtdb_crypt::Kms>> { self.native_checkpoint_kms.clone() }

    fn native_serving_authority(&self, name: &str, _configuration: &DatabaseConfig)
        -> Result<Option<crate::serving::NativeServingAuthority>> {
        Ok(self.serving_authorities.get(name).cloned())
    }

    fn table_history(&self, configuration: &DatabaseConfig) -> Result<Option<Arc<dyn rtdb_maintenance::TableHistorySource>>> {
        let Some(database_config::Storage::RemoteStorage(storage)) = &configuration.storage else { return Ok(None); };
        let configuration = storage.object_store.as_ref().ok_or_else(|| invalid("Remote storage has no provider configuration"))?;
        let factory = self.storage_factories.get(type_name(configuration)?).ok_or_else(|| invalid("Native storage factory is unavailable"))?;
        factory.table_history(configuration)
    }

    fn has_private_storage(&self, configuration: &DatabaseConfig) -> bool {
        matches!(configuration.storage, None | Some(database_config::Storage::InMemoryStorage(_)))
    }

    async fn close(&self) -> Result<()> { if let Some(cache) = &self.disk_cache { cache.close_and_wait().await; } self.io.close_and_wait().await; Ok(()) }
    fn validate(&self, name: &str, configuration: &DatabaseConfig) -> Result<()> {
        validate_database_shape(name, configuration)?;
        if self.maximum_log_message_bytes == 0 || self.maximum_log_message_bytes > i32::MAX as usize || self.log_queue_capacity == 0 { return Err(invalid("Invalid local log message or queue capacity")); }
        match &configuration.log {
            Some(database_config::Log::InMemoryLog(log)) => { SourcePosition::from_epoch_offset(StorageEpoch(log.epoch.unwrap_or(0)), -1)?; }
            Some(database_config::Log::LocalLog(log)) => {
                SourcePosition::from_epoch_offset(StorageEpoch(log.epoch.unwrap_or(0)), -1)?;
                if log.path.as_deref().is_none_or(str::is_empty) { return Err(invalid("Local log has no path")); }
            }
            Some(database_config::Log::OtherLog(log)) => {
                if !self.log_factories.contains_key(type_name(log)?) { return Err(invalid(format!("No native log factory is registered for {}", log.type_url))); }
            }
            None => (),
        }
        let epoch = match &configuration.storage {
            Some(database_config::Storage::InMemoryStorage(storage)) => storage.epoch.unwrap_or(0),
            Some(database_config::Storage::LocalStorage(storage)) => {
                if storage.path.as_deref().is_none_or(str::is_empty) { return Err(invalid("Local storage has no path")); }
                storage.epoch.unwrap_or(0)
            }
            Some(database_config::Storage::RemoteStorage(storage)) => {
                if self.disk_cache.is_none() { return Err(invalid("Remote storage requires a disk cache")); }
                let store = storage.object_store.as_ref().ok_or_else(|| invalid("Remote storage has no provider configuration"))?;
                if !self.storage_factories.contains_key(type_name(store)?) { return Err(invalid(format!("No native storage factory is registered for {}", store.type_url))); }
                storage.epoch.unwrap_or(0)
            }
            None => 0,
        };
        StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(epoch))?;
        Ok(())
    }

    async fn open_storage(&self, name: &str, configuration: &DatabaseConfig) -> Result<DatabaseStorage> {
        self.validate(name, configuration)?;
        let local_erasure_vault = self.local_checkpoint_vault.as_deref().filter(|_| {
            name == "xtdb" && self.classic_vault.is_some() && configuration.external_source.is_none()
        });
        let kafka = match &configuration.log {
            Some(database_config::Log::OtherLog(log)) if type_name(log)? == "xtdb.kafka.proto.KafkaLogConfig" => Some(log),
            _ => None,
        };
        let kafka_erasure_vault = self.kafka_checkpoint_vault.as_deref().filter(|_| {
            name == "xtdb" && kafka.is_some() && self.classic_vault.is_some() && configuration.external_source.is_none()
        });
        let residence = match (&configuration.storage, &configuration.log) {
            (Some(database_config::Storage::LocalStorage(storage)), log)
                if configuration.mode.unwrap_or(0) != 1 || kafka.is_some()
                    || matches!(log, Some(database_config::Log::LocalLog(_))) && local_erasure_vault.is_some() =>
            {
                let path = PathBuf::from(storage.path.as_deref().ok_or_else(|| invalid("Local storage has no path"))?);
                let exclusive = kafka_erasure_vault.is_some()
                    || local_erasure_vault.is_some() && matches!(log, Some(database_config::Log::LocalLog(_)));
                Some(Arc::new(self.io.run_blocking(move || {
                    std::fs::create_dir_all(&path).map_err(|error| rtdb_storage::Error::Task(error.to_string()))?;
                    rtdb_storage::sealing::open_local_residence(&path, exclusive)
                }).await?) as Arc<dyn Send + Sync>)
            }
            _ => None,
        };
        let io = self.io.child_holding(residence);
        let opened = async {
            let epoch = match &configuration.storage {
                Some(database_config::Storage::InMemoryStorage(storage)) => storage.epoch,
                Some(database_config::Storage::LocalStorage(storage)) => storage.epoch,
                Some(database_config::Storage::RemoteStorage(storage)) => storage.epoch,
                None => None,
            }.unwrap_or(0);
            let storage_epoch = StorageEpoch(epoch);
            let root = ObjectKey::new(StorageRoot::new(CURRENT_STORAGE_VERSION, storage_epoch)?.key())?;
            let allow_enrollment = configuration.mode.unwrap_or(0) != 1;
            let mut blocks = rtdb_replication::BlockDiscovery::List;
            let mut checkpoint_metadata = None;
            let mut fragment_storage = None;
            let mut audience_storage = None;
            let store: Arc<dyn ObjectStore> = match &configuration.storage {
                None | Some(database_config::Storage::InMemoryStorage(_)) => Arc::new(MemoryStore::default()),
                Some(database_config::Storage::LocalStorage(storage)) => {
                    let payload = PathBuf::from(storage.path.as_deref().ok_or_else(|| invalid("Local storage has no path"))?);
                    let store = LocalStore::open_in(payload.join(root.as_str()), Arc::clone(&io))?;
                    let kafka_erasure_epoch = if let (Some(_), Some(log)) = (kafka_erasure_vault, kafka) {
                        Some(kafka_log_epoch(log)?)
                    } else { None };
                    let store = if let Some(database_config::Log::LocalLog(log)) = &configuration.log {
                        store.with_catalog_ledger(StorageEpoch(log.epoch.unwrap_or_default()))
                    } else { store };
                    if let Some(vault) = &self.classic_vault {
                        vault.require_separate_path(&payload)?;
                        let residence = match (local_erasure_vault, kafka_erasure_vault, &configuration.log) {
                            (Some(checkpoint), _, Some(database_config::Log::LocalLog(log))) if epoch == 0 => {
                                let journal = PathBuf::from(log.path.as_deref().ok_or_else(|| invalid("Local log has no path"))?);
                                std::fs::create_dir_all(&journal).map_err(|error| rtdb_storage::Error::Task(error.to_string()))?;
                                vault.require_separate_path(&journal)?;
                                Some(rtdb_storage::sealing::LocalErasureResidence::open(
                                    store.root(), &journal, checkpoint, log.epoch.unwrap_or_default(),
                                )?)
                            }
                            (_, Some(checkpoint), _) if epoch == 0 => {
                                Some(rtdb_storage::sealing::LocalErasureResidence::open_kafka(
                                    store.root(), checkpoint, kafka_erasure_epoch.ok_or_else(|| invalid("Kafka erasure has no topic epoch"))?,
                                )?)
                            }
                            _ => None,
                        };
                        let keys = vault.local_units(&store, Arc::clone(&io), allow_enrollment, residence).await?;
                        let store = match kafka_erasure_epoch {
                            Some(epoch) if keys.has_erasure_custody() => store.with_catalog_ledger(StorageEpoch(epoch)),
                            _ => store,
                        };
                        Arc::new(rtdb_storage::sealing::ClassicKeyVault::wrap(Arc::new(store), keys)?)
                    } else {
                        rtdb_storage::sealing::ClassicKeyVault::require_unenrolled(&store).await?;
                        Arc::new(store)
                    }
                }
                Some(database_config::Storage::RemoteStorage(storage)) => {
                    let configuration = storage.object_store.as_ref().ok_or_else(|| invalid("Remote storage has no provider configuration"))?;
                    let factory = self.storage_factories.get(type_name(configuration)?).ok_or_else(|| invalid("Native storage factory is unavailable"))?;
                    let opened = factory.open_for_recovery(configuration, &root, Arc::clone(&io)).await?;
                    let remote = opened.store;
                    let classic_keys = match &self.classic_vault {
                        Some(vault) if factory.uses_classic_custody() => {
                            Some(vault.units(remote.as_ref(), Arc::clone(&io), allow_enrollment).await?)
                        }
                        None if factory.uses_classic_custody() => {
                            rtdb_storage::sealing::ClassicKeyVault::require_unenrolled(remote.as_ref()).await?;
                            None
                        }
                        _ => None,
                    };
                    blocks = opened.blocks;
                    checkpoint_metadata = opened.checkpoint_metadata;
                    fragment_storage = opened.fragment_storage;
                    let storage = if factory.use_disk_cache() && remote.permits_shared_read_cache() {
                        let cache = self.disk_cache.as_ref().ok_or_else(|| invalid("Remote storage requires a disk cache"))?;
                        let identity = factory.cache_identity(configuration)?;
                        let namespace = cache_namespace(&configuration.type_url, &identity, storage_epoch)?;
                        cache.scope_in(namespace, remote, Arc::clone(&io))?
                    } else {
                        remote
                    };
                    let opened = factory.wrap_stores(configuration, storage, opened.audience, opened.publication_sealing).await?;
                    audience_storage = opened.audience;
                    match classic_keys {
                        Some(keys) => Arc::new(rtdb_storage::sealing::ClassicKeyVault::wrap(opened.ordinary, keys)?),
                        None => opened.ordinary,
                    }
                }
            };
            Ok(DatabaseStorage { store, audience_storage, blocks, checkpoint_metadata, fragment_storage, storage_epoch, storage_version: CURRENT_STORAGE_VERSION, io: Arc::clone(&io) })
        }.await;
        if opened.is_err() { io.close_and_wait().await; }
        opened
    }

    async fn open_logs(&self, _: &str, configuration: &DatabaseConfig, access: LogAccess, log_cancellation: CancellationToken, storage: &DatabaseStorage) -> Result<OpenedLogs> {
        self.open_selected_logs(configuration, access, log_cancellation, storage, false).await
    }

    async fn open_logs_with_replica_lineage(&self, _: &str, configuration: &DatabaseConfig, access: LogAccess, log_cancellation: CancellationToken,
        storage: &DatabaseStorage,
    ) -> Result<OpenedLogs> {
        self.open_selected_logs(configuration, access, log_cancellation, storage, true).await
    }
}

impl NativeResources {
    async fn open_selected_logs(&self, configuration: &DatabaseConfig, access: LogAccess, log_cancellation: CancellationToken,
        storage: &DatabaseStorage, bind_lineage: bool,
    ) -> Result<OpenedLogs> {
        let io = &storage.io;
        match &configuration.log {
            None | Some(database_config::Log::InMemoryLog(_)) => {
                let epoch = match &configuration.log { Some(database_config::Log::InMemoryLog(log)) => log.epoch.unwrap_or(0), _ => 0 };
                let source = match &self.memory_transaction_clock {
                    Some(clock) => MemoryLog::with_timestamp_source(self.maximum_log_message_bytes, Arc::new(MemoryLogTransactionTimestamps(Arc::clone(clock)))),
                    None => MemoryLog::new(self.maximum_log_message_bytes),
                };
                Ok(OpenedLogs { source: Arc::new(source), replica: Arc::new(MemoryLog::new(self.maximum_log_message_bytes)),
                    source_epoch: StorageEpoch(epoch), replica_epoch: StorageEpoch(epoch), replica_read_index: None,
                    placed_transfer: None, await_authority: crate::AwaitAuthority::SourcePosition, drivers: Vec::new() })
            }
            Some(database_config::Log::LocalLog(log)) => {
                let path = PathBuf::from(log.path.as_deref().ok_or_else(|| invalid("Local log has no path"))?);
                let (source, source_driver) = self.local_log(path.join("LOG"), true, access.source_read_only(), log_cancellation.clone(), Arc::clone(io), storage.store.local_erasure_custody().is_some())?;
                let (replica, replica_driver) = self.local_log(path.join("REPLICA_LOG"), false, access.replica_read_only(), log_cancellation.clone(), Arc::clone(io), bind_lineage)?;
                let epoch = StorageEpoch(log.epoch.unwrap_or(0));
                Ok(OpenedLogs { source, replica, source_epoch: epoch, replica_epoch: epoch, replica_read_index: None, placed_transfer: None, await_authority: crate::AwaitAuthority::SourcePosition,
                    drivers: source_driver.into_iter().chain(replica_driver).collect() })
            }
            Some(database_config::Log::OtherLog(log)) => {
                let factory = self.log_factories.get(type_name(log)?).ok_or_else(|| invalid("Native log factory is unavailable"))?;
                if let Some(custody) = storage.store.local_erasure_custody()
                    && let Some(owner) = custody.kafka_owner()
                {
                    let custody = Arc::clone(custody);
                    let topics = io.run_blocking(move || custody.bound_kafka_topics()).await?;
                    factory.open_for_default_erasure(log, access, log_cancellation, owner, topics).await
                } else {
                    factory.open(log, access, log_cancellation).await
                }
            }
        }
    }
}

#[cfg(feature = "kafka")]
fn kafka_log_epoch(configuration: &Any) -> Result<i32> {
    use prost::Message;
    let wire = rtdb_format::generated::xtdb::kafka::proto::KafkaLogConfig::decode(configuration.value.as_slice()).map_err(|error| invalid(error.to_string()))?;
    Ok(rtdb_kafka::KafkaLogConfig::from_proto(&wire).epoch)
}

#[cfg(not(feature = "kafka"))]
fn kafka_log_epoch(_: &Any) -> Result<i32> {
    Err(invalid("Default Kafka erasure requires the native Kafka provider"))
}

/// Cached objects belong to the store they came from, so the namespace is the store's identity, never the database's name.
/// A name re-attached to a different bucket, prefix or storage epoch must not be served the previous store's objects.
fn cache_namespace(provider_type: &str, store_identity: &[u8], epoch: StorageEpoch) -> Result<ObjectKey> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for part in [provider_type.as_bytes(), store_identity, &epoch.0.to_be_bytes()] {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    let digest: String = hasher.finalize().iter().map(|byte| format!("{byte:02x}")).collect();
    ObjectKey::new(format!("~rtdb-store-{digest}/0")).map_err(Into::into)
}

#[cfg(test)]
mod cache_namespace_tests {
    use super::*;

    #[test]
    fn the_cache_namespace_follows_the_store_not_the_database_name() {
        let key = |identity: &[u8], epoch| cache_namespace("type.googleapis.com/xtdb.aws.proto.S3ObjectStoreConfig", identity, StorageEpoch(epoch)).unwrap();
        assert_eq!(key(b"bucket-a", 0), key(b"bucket-a", 0));
        assert_ne!(key(b"bucket-a", 0), key(b"bucket-b", 0), "another bucket or prefix");
        assert_ne!(key(b"bucket-a", 0), key(b"bucket-a", 1), "another storage epoch");
    }

}

#[cfg(test)]
#[tokio::test]
async fn native_search_lineage_follows_the_journal_after_its_directory_moves() -> Result<()> {
    let files = tempfile::tempdir().unwrap();
    let original = files.path().join("original");
    let moved = files.path().join("moved");
    let native = NativeResources::default();
    let config = |path: &std::path::Path| DatabaseConfig {
        log: Some(database_config::Log::LocalLog(rtdb_format::database::proto::LocalLog {
            path: Some(path.to_str().unwrap().into()), epoch: None,
        })),
        ..Default::default()
    };
    let original_config = config(&original);
    let storage = native.open_storage("xtdb", &original_config).await?;
    let open = || native.open_logs_with_replica_lineage("xtdb", &original_config, LogAccess::ReadWrite, CancellationToken::new(), &storage);
    let journal = open().await?;
    let lineage = journal.replica.journal_lineage().unwrap();
    assert_eq!(journal.source.journal_lineage(), None);
    drop(journal);
    std::fs::rename(&original, &moved).unwrap();

    let moved_config = config(&moved);
    let moved_storage = native.open_storage("xtdb", &moved_config).await?;
    let moved_journal = native.open_logs("xtdb", &moved_config, LogAccess::ReadOnly, CancellationToken::new(), &moved_storage).await?;
    let new_journal = open().await?;
    assert_eq!(moved_journal.replica.journal_lineage(), Some(lineage));
    assert_ne!(new_journal.replica.journal_lineage().unwrap(), lineage);
    drop((moved_journal, new_journal));
    moved_storage.io.close_and_wait().await;
    storage.io.close_and_wait().await;
    native.close().await
}

fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("xtdb.db-catalog/invalid-db-config", message) }
