use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{FutureExt, future::BoxFuture};
use prost_types::Any;
use rtdb_format::{database::proto::{DatabaseConfig, database_config}, keys::{CURRENT_STORAGE_VERSION, storage_root}};
use rtdb_storage::{LocalStore, MemoryStore, ObjectKey, ObjectStore, log::{LocalLog, LocalLogOptions, Log, MemoryLog, ReadOnlyLocalLog, ReadOnlyLocalLogOptions}};
use rtdb_types::{Anomaly, Result, SourcePosition, StorageEpoch, StorageVersion};
use tokio_util::sync::CancellationToken;

pub type ResourceDriver = BoxFuture<'static, Result<()>>;

pub struct OpenedLogs {
    pub source: Arc<dyn Log>,
    pub replica: Arc<dyn Log>,
    pub source_epoch: StorageEpoch,
    pub replica_epoch: StorageEpoch,
    pub drivers: Vec<ResourceDriver>,
}

pub struct DatabaseResources {
    pub logs: OpenedLogs,
    pub store: Arc<dyn ObjectStore>,
    pub storage_epoch: StorageEpoch,
    pub storage_version: StorageVersion,
}

#[async_trait]
pub trait LogFactory: Send + Sync {
    /// Opens distinct source and replica logs without spawning their drivers.
    /// Returned drivers finish all admitted appends when `cancellation` is cancelled.
    async fn open(&self, configuration: &Any, read_only: bool, cancellation: CancellationToken) -> Result<OpenedLogs>;
}

#[async_trait]
pub trait RemoteStorageFactory: Send + Sync {
    /// The returned store is scoped to `storage_root`, under the provider's configured prefix.
    async fn open(&self, configuration: &Any, storage_root: &ObjectKey) -> Result<Arc<dyn ObjectStore>>;
}

#[async_trait]
pub trait ResourceFactory: Send + Sync {
    fn validate(&self, name: &str, configuration: &DatabaseConfig) -> Result<()>;
    /// No driver may run before this returns; dropping a failed open must release its local resources.
    async fn open(&self, name: &str, configuration: &DatabaseConfig, read_only: bool, log_cancellation: CancellationToken) -> Result<DatabaseResources>;
}

pub struct NativeResources {
    pub maximum_log_message_bytes: usize,
    pub log_queue_capacity: usize,
    pub read_only_log: ReadOnlyLocalLogOptions,
    log_factories: BTreeMap<String, Arc<dyn LogFactory>>,
    storage_factories: BTreeMap<String, Arc<dyn RemoteStorageFactory>>,
}

impl Default for NativeResources {
    fn default() -> Self {
        Self { maximum_log_message_bytes: 64 * 1024 * 1024, log_queue_capacity: 64, read_only_log: ReadOnlyLocalLogOptions::default(),
            log_factories: BTreeMap::new(), storage_factories: BTreeMap::new() }
    }
}

impl NativeResources {
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
    fn local_log(&self, path: PathBuf, read_only: bool, cancellation: CancellationToken) -> Result<(Arc<dyn Log>, Option<ResourceDriver>)> {
        if read_only { return Ok((ReadOnlyLocalLog::open(path, self.read_only_log.clone())?, None)); }
        let (log, driver) = LocalLog::open(path, LocalLogOptions { maximum_message_bytes: self.maximum_log_message_bytes, queue_capacity: self.log_queue_capacity })?;
        Ok((log, Some(async move { driver.run(cancellation).await.map_err(Into::into) }.boxed())))
    }
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
                let store = storage.object_store.as_ref().ok_or_else(|| invalid("Remote storage has no provider configuration"))?;
                if !self.storage_factories.contains_key(type_name(store)?) { return Err(invalid(format!("No native storage factory is registered for {}", store.type_url))); }
                storage.epoch.unwrap_or(0)
            }
            None => 0,
        };
        storage_root(CURRENT_STORAGE_VERSION, StorageEpoch(epoch), 0, 1)?;
        Ok(())
    }

    async fn open(&self, name: &str, configuration: &DatabaseConfig, read_only: bool, log_cancellation: CancellationToken) -> Result<DatabaseResources> {
        self.validate(name, configuration)?;
        let logs = match &configuration.log {
            None | Some(database_config::Log::InMemoryLog(_)) => {
                let epoch = match &configuration.log { Some(database_config::Log::InMemoryLog(log)) => log.epoch.unwrap_or(0), _ => 0 };
                OpenedLogs { source: Arc::new(MemoryLog::new(self.maximum_log_message_bytes)), replica: Arc::new(MemoryLog::new(self.maximum_log_message_bytes)),
                    source_epoch: StorageEpoch(epoch), replica_epoch: StorageEpoch(epoch), drivers: Vec::new() }
            }
            Some(database_config::Log::LocalLog(log)) => {
                let path = PathBuf::from(log.path.as_deref().ok_or_else(|| invalid("Local log has no path"))?);
                let (source, source_driver) = self.local_log(path.join("LOG"), read_only, log_cancellation.clone())?;
                let (replica, replica_driver) = self.local_log(path.join("REPLICA_LOG"), read_only, log_cancellation.clone())?;
                let epoch = StorageEpoch(log.epoch.unwrap_or(0));
                OpenedLogs { source, replica, source_epoch: epoch, replica_epoch: epoch, drivers: source_driver.into_iter().chain(replica_driver).collect() }
            }
            Some(database_config::Log::OtherLog(log)) => self.log_factories.get(type_name(log)?).ok_or_else(|| invalid("Native log factory is unavailable"))?.open(log, read_only, log_cancellation).await?,
        };
        let epoch = match &configuration.storage {
            Some(database_config::Storage::InMemoryStorage(storage)) => storage.epoch,
            Some(database_config::Storage::LocalStorage(storage)) => storage.epoch,
            Some(database_config::Storage::RemoteStorage(storage)) => storage.epoch,
            None => None,
        }.unwrap_or(0);
        let storage_epoch = StorageEpoch(epoch);
        let root = ObjectKey::new(storage_root(CURRENT_STORAGE_VERSION, storage_epoch, 0, 1)?)?;
        let store: Arc<dyn ObjectStore> = match &configuration.storage {
            None | Some(database_config::Storage::InMemoryStorage(_)) => Arc::new(MemoryStore::default()),
            Some(database_config::Storage::LocalStorage(storage)) => Arc::new(LocalStore::open(PathBuf::from(storage.path.as_deref().ok_or_else(|| invalid("Local storage has no path"))?).join(root.as_str()))?),
            Some(database_config::Storage::RemoteStorage(storage)) => {
                let configuration = storage.object_store.as_ref().ok_or_else(|| invalid("Remote storage has no provider configuration"))?;
                self.storage_factories.get(type_name(configuration)?).ok_or_else(|| invalid("Native storage factory is unavailable"))?.open(configuration, &root).await?
            }
        };
        Ok(DatabaseResources { logs, store, storage_epoch, storage_version: CURRENT_STORAGE_VERSION })
    }
}

fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("xtdb.db-catalog/invalid-db-config", message) }
