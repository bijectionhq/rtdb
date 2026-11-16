use std::sync::Arc;
use async_trait::async_trait;
use prost_types::Any;
use rtdb_columnar::MemoryBudget;
use rtdb_format::database::proto::{DatabaseConfig, RemoteStorage, database_config};
use rtdb_node::{NativeResources, RemoteStorageFactory, ResourceFactory};
use rtdb_storage::{MemoryStore, ObjectKey, ObjectStore, disk_cache::DiskCache};
use rtdb_types::Result;
use tokio_util::sync::CancellationToken;

struct Remote;
#[async_trait]
impl RemoteStorageFactory for Remote {
    async fn open(&self, configuration: &Any, root: &ObjectKey) -> Result<Arc<dyn ObjectStore>> {
        assert!(!root.as_str().is_empty());
        let store = Arc::new(MemoryStore::default());
        store.put(&ObjectKey::new("same")?, configuration.value.clone().into()).await?;
        Ok(store)
    }
}
fn configuration(value: &[u8]) -> DatabaseConfig {
    DatabaseConfig { storage: Some(database_config::Storage::RemoteStorage(RemoteStorage {
        object_store: Some(Any { type_url: "type.googleapis.com/test.Remote".into(), value: value.to_vec() }), epoch: None,
    })), ..Default::default() }
}

#[tokio::test]
async fn remote_resources_require_cache_and_scope_equal_keys_by_database() {
    let native = NativeResources::default().register_storage("test.Remote", Arc::new(Remote)).unwrap();
    assert!(native.validate("first", &configuration(b"one")).unwrap_err().message.contains("disk cache"));
    let directory = tempfile::tempdir().unwrap();
    let cache = DiskCache::open(directory.path(), 1024, MemoryBudget::new(4 * 1024 * 1024)).unwrap();
    let native = native.with_disk_cache(cache);
    let first = native.open("first", &configuration(b"one"), rtdb_node::LogAccess::ReadWrite, CancellationToken::new()).await.unwrap();
    let second = native.open("second", &configuration(b"two"), rtdb_node::LogAccess::ReadWrite, CancellationToken::new()).await.unwrap();
    let key = ObjectKey::new("same").unwrap();
    assert_eq!(first.store.get(&key).await.unwrap().as_ref(), b"one");
    assert_eq!(second.store.get(&key).await.unwrap().as_ref(), b"two");
    assert!(directory.path().join("first/0/same").exists());
    assert!(directory.path().join("second/0/same").exists());
}

#[tokio::test]
async fn unusual_database_names_cannot_escape_or_alias_cache_directories() {
    let directory = tempfile::tempdir().unwrap();
    let cache = DiskCache::open(directory.path(), 1024, MemoryBudget::new(4 * 1024 * 1024)).unwrap();
    let native = NativeResources::default().with_disk_cache(cache).register_storage("test.Remote", Arc::new(Remote)).unwrap();
    for (index, name) in ["../outside", "/absolute", "a/b", "~rtdb-612f62", "", ".tmp"].iter().enumerate() {
        let database = native.open(name, &configuration(&[index as u8]), rtdb_node::LogAccess::ReadWrite, CancellationToken::new()).await.unwrap();
        assert_eq!(database.store.get(&ObjectKey::new("same").unwrap()).await.unwrap().as_ref(), &[index as u8]);
    }
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 7);
}

#[tokio::test]
async fn local_log_clock_applies_to_transactions_and_optionally_to_other_messages() {
    use rtdb_types::Timestamp;
    use rtdb_format::envelope::{SourceBody, SourceMessage};
    struct FixedClock;
    impl rtdb_node::Clock for FixedClock { fn now(&self) -> Timestamp { Timestamp(123) } }
    for use_for_non_transactions in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let native = NativeResources::default().with_local_log_clock(Arc::new(FixedClock), use_for_non_transactions);
        let config = DatabaseConfig { log: Some(database_config::Log::LocalLog(rtdb_format::database::proto::LocalLog {
            path: Some(directory.path().to_string_lossy().into_owned()), epoch: None,
        })), ..Default::default() };
        let shutdown = CancellationToken::new();
        let opened = native.open("xtdb", &config, rtdb_node::LogAccess::ReadWrite, shutdown.clone()).await.unwrap();
        let tasks = opened.logs.drivers.into_iter().map(tokio::spawn).collect::<Vec<_>>();
        let transaction = SourceMessage::Protobuf(SourceBody::Tx(Default::default())).encode().unwrap();
        let other = SourceMessage::Protobuf(SourceBody::DetachDatabase(Default::default())).encode().unwrap();
        assert_eq!(opened.logs.source.enqueue(transaction.into(), Timestamp(10)).await.unwrap().durable().await.unwrap().timestamp, Timestamp(123));
        let source = opened.logs.source.enqueue(other.into(), Timestamp(10)).await.unwrap().durable().await.unwrap().timestamp;
        let replica = opened.logs.replica.enqueue(vec![3, 0x2a, 0].into(), Timestamp(10)).await.unwrap().durable().await.unwrap().timestamp;
        assert_eq!(source == Timestamp(123), use_for_non_transactions);
        assert_eq!(replica == Timestamp(123), use_for_non_transactions);
        shutdown.cancel();
        for task in tasks { task.await.unwrap().unwrap(); }
    }
}

#[tokio::test]
async fn source_writer_log_access_leaves_replica_writer_ownership_available() {
    use rtdb_storage::log::{LocalLog, LocalLogOptions};
    let directory = tempfile::tempdir().unwrap();
    let native = NativeResources::default();
    let config = DatabaseConfig { log: Some(database_config::Log::LocalLog(rtdb_format::database::proto::LocalLog {
        path: Some(directory.path().to_string_lossy().into_owned()), epoch: None,
    })), ..Default::default() };
    let opened = native.open("xtdb", &config, rtdb_node::LogAccess::SourceWriter, CancellationToken::new()).await.unwrap();
    assert_eq!(opened.logs.drivers.len(), 1);
    assert!(matches!(opened.logs.replica.enqueue(vec![1].into(), rtdb_types::Timestamp(0)).await, Err(rtdb_storage::Error::ReadOnlyLog(_))));
    let (_replica, replica_driver) = LocalLog::open(directory.path().join("REPLICA_LOG"), LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 8 }).unwrap();
    assert!(LocalLog::open(directory.path().join("LOG"), LocalLogOptions { maximum_message_bytes: 1024, queue_capacity: 8 }).is_err());
    drop(replica_driver);
    drop(opened);
    native.close().await.unwrap();
}
