use std::sync::Arc;
use async_trait::async_trait;
use prost_types::Any;
use rtdb_columnar::MemoryBudget;
use rtdb_format::database::proto::{DatabaseConfig, RemoteStorage, database_config};
use rtdb_node::{NativeResources, RemoteStorageFactory, ResourceFactory};
use rtdb_storage::{MemoryStore, ObjectKey, ObjectStore, disk_cache::DiskCache};
use rtdb_types::Result;
use tokio_util::sync::CancellationToken;

#[path = "support/io.rs"]
mod io;

struct Remote;
#[async_trait]
impl RemoteStorageFactory for Remote {
    async fn open(&self, configuration: &Any, root: &ObjectKey, _: Arc<rtdb_storage::IoRegion>) -> Result<Arc<dyn ObjectStore>> {
        assert!(!root.as_str().is_empty());
        let store = Arc::new(MemoryStore::default());
        store.put(&ObjectKey::new("same")?, configuration.value.clone().into()).await?;
        Ok(store)
    }
}

struct ScopedLocalRemote {
    root: std::path::PathBuf,
    opened: tokio::sync::mpsc::UnboundedSender<Arc<rtdb_storage::LocalStore>>,
}

#[async_trait]
impl RemoteStorageFactory for ScopedLocalRemote {
    async fn open(&self, configuration: &Any, root: &ObjectKey, io: Arc<rtdb_storage::IoRegion>) -> Result<Arc<dyn ObjectStore>> {
        let name = ObjectKey::new(String::from_utf8(configuration.value.clone()).unwrap())?;
        let store = Arc::new(rtdb_storage::LocalStore::open_in(self.root.join(name.as_str()).join(root.as_str()), io)?);
        self.opened.send(Arc::clone(&store)).unwrap();
        Ok(store)
    }
}

#[tokio::test]
async fn reused_provider_factory_receives_a_distinct_region_for_each_node() {
    let provider_directory = tempfile::tempdir().unwrap();
    let first_cache = tempfile::tempdir().unwrap();
    let second_cache = tempfile::tempdir().unwrap();
    let (opened, mut stores) = tokio::sync::mpsc::unbounded_channel();
    let provider = Arc::new(ScopedLocalRemote { root: provider_directory.path().to_owned(), opened });
    let first = NativeResources::default().with_disk_cache(DiskCache::open(first_cache.path(), 1024, MemoryBudget::new(4 * 1024 * 1024)).unwrap())
        .register_storage("test.Remote", provider.clone()).unwrap();
    let second = NativeResources::default().with_disk_cache(DiskCache::open(second_cache.path(), 1024, MemoryBudget::new(4 * 1024 * 1024)).unwrap())
        .register_storage("test.Remote", provider).unwrap();
    let first_database = first.open("xtdb", &configuration(b"first"), rtdb_node::LogAccess::ReadOnly, CancellationToken::new()).await.unwrap();
    let first_store = stores.recv().await.unwrap();
    let second_database = second.open("xtdb", &configuration(b"second"), rtdb_node::LogAccess::ReadOnly, CancellationToken::new()).await.unwrap();
    let second_store = stores.recv().await.unwrap();
    let key = ObjectKey::new("object").unwrap();
    first_store.put(&key, vec![1].into()).await.unwrap();
    second_store.put(&key, vec![2].into()).await.unwrap();
    drop(first_database);
    first.close().await.unwrap();
    assert!(matches!(first_store.get(&key).await, Err(rtdb_storage::Error::Closed(_))));
    assert_eq!(second_store.get(&key).await.unwrap().as_ref(), &[2]);
    drop(second_database);
    second.close().await.unwrap();
    assert!(matches!(second_store.get(&key).await, Err(rtdb_storage::Error::Closed(_))));
}

#[tokio::test]
async fn database_region_closure_rejects_cached_and_remote_reads_without_closing_siblings() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let provider_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let (opened, mut stores) = tokio::sync::mpsc::unbounded_channel();
        let native = NativeResources::default()
            .with_disk_cache(DiskCache::open(cache_directory.path(), 1024, MemoryBudget::new(4 * 1024 * 1024)).unwrap())
            .register_storage("test.Remote", Arc::new(ScopedLocalRemote { root: provider_directory.path().to_owned(), opened })).unwrap();
        let first = native.open("first", &configuration(b"first"), rtdb_node::LogAccess::ReadOnly, CancellationToken::new()).await.unwrap();
        let first_store = stores.recv().await.unwrap();
        let second = native.open("second", &configuration(b"second"), rtdb_node::LogAccess::ReadOnly, CancellationToken::new()).await.unwrap();
        let second_store = stores.recv().await.unwrap();
        assert!(!Arc::ptr_eq(&first.io, &second.io));
        let key = ObjectKey::new("object").unwrap();
        first_store.put(&key, vec![1].into()).await.unwrap();
        second_store.put(&key, vec![2].into()).await.unwrap();
        assert_eq!(first.store.get(&key).await.unwrap().as_ref(), &[1]);
        first.io.close_and_wait().await;
        assert!(matches!(first.store.get(&key).await, Err(rtdb_storage::Error::Closed(_))));
        assert!(matches!(first_store.get(&key).await, Err(rtdb_storage::Error::Closed(_))));
        assert_eq!(second.store.get(&key).await.unwrap().as_ref(), &[2]);
        native.close().await.unwrap();
        assert!(matches!(second_store.get(&key).await, Err(rtdb_storage::Error::Closed(_))));
    }).await.unwrap();
}

struct FailingRemote {
    root: std::path::PathBuf,
    opened: tokio::sync::mpsc::UnboundedSender<(Arc<dyn ObjectStore>, std::sync::mpsc::Sender<()>)>,
}

struct UnopenedLog;
#[async_trait]
impl rtdb_node::LogFactory for UnopenedLog {
    async fn open(&self, _: &Any, _: rtdb_node::LogAccess, _: CancellationToken) -> Result<rtdb_node::OpenedLogs> { panic!("log opened before storage failure"); }
}

struct FailingLog;
#[async_trait]
impl rtdb_node::LogFactory for FailingLog {
    async fn open(&self, _: &Any, _: rtdb_node::LogAccess, _: CancellationToken) -> Result<rtdb_node::OpenedLogs> {
        Err(rtdb_types::Anomaly::fault("test/log-open", "Injected log opening failure"))
    }
}
#[async_trait]
impl RemoteStorageFactory for FailingRemote {
    async fn open(&self, configuration: &Any, root: &ObjectKey, io: Arc<rtdb_storage::IoRegion>) -> Result<Arc<dyn ObjectStore>> {
        let name = ObjectKey::new(String::from_utf8(configuration.value.clone()).unwrap())?;
        let store: Arc<dyn ObjectStore> = Arc::new(rtdb_storage::LocalStore::open_in(self.root.join(name.as_str()).join(root.as_str()), io)?);
        if configuration.value.as_slice() == b"fail" {
            let release = io::hold_cancelled_local_read(&store).await;
            self.opened.send((store, release)).unwrap();
            return Err(rtdb_types::Anomaly::fault("test/provider-open", "Injected provider opening failure"));
        }
        Ok(store)
    }
}

#[tokio::test]
async fn provider_open_failure_skips_logs_and_drains_only_the_partial_database_region() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let provider_directory = tempfile::tempdir().unwrap();
        let cache_directory = tempfile::tempdir().unwrap();
        let (opened, mut stores) = tokio::sync::mpsc::unbounded_channel();
        let native = Arc::new(NativeResources::default()
            .with_disk_cache(DiskCache::open(cache_directory.path(), 1024, MemoryBudget::new(4 * 1024 * 1024)).unwrap())
            .register_storage("test.Remote", Arc::new(FailingRemote { root: provider_directory.path().to_owned(), opened })).unwrap()
            .register_log("test.UnopenedLog", Arc::new(UnopenedLog)).unwrap());
        let sibling = native.open("sibling", &configuration(b"sibling"), rtdb_node::LogAccess::ReadOnly, CancellationToken::new()).await.unwrap();
        let factory = Arc::clone(&native);
        let opening = tokio::spawn(async move {
            let mut configuration = configuration(b"fail");
            configuration.log = Some(database_config::Log::OtherLog(Any { type_url: "type.googleapis.com/test.UnopenedLog".into(), value: Vec::new() }));
            factory.open("failed", &configuration, rtdb_node::LogAccess::ReadOnly, CancellationToken::new()).await
        });
        let (store, release) = stores.recv().await.unwrap();
        let key = ObjectKey::new("probe").unwrap();
        while !matches!(store.head(&key).await, Err(rtdb_storage::Error::Closed(_))) { tokio::task::yield_now().await; }
        assert!(!opening.is_finished());
        sibling.store.put(&key, vec![3].into()).await.unwrap();
        assert_eq!(sibling.store.get(&key).await.unwrap().as_ref(), &[3]);
        release.send(()).unwrap();
        let error = match opening.await.unwrap() { Err(error) => error, Ok(_) => panic!("provider opening succeeded") };
        assert_eq!(error.code, "test/provider-open");
        native.close().await.unwrap();
    }).await.unwrap();
}

#[tokio::test]
async fn log_open_failure_closes_the_already_opened_storage_region_without_closing_siblings() {
    let provider_directory = tempfile::tempdir().unwrap();
    let cache_directory = tempfile::tempdir().unwrap();
    let (opened, mut stores) = tokio::sync::mpsc::unbounded_channel();
    let native = NativeResources::default()
        .with_disk_cache(DiskCache::open(cache_directory.path(), 1024, MemoryBudget::new(4 * 1024 * 1024)).unwrap())
        .register_storage("test.Remote", Arc::new(ScopedLocalRemote { root: provider_directory.path().to_owned(), opened })).unwrap()
        .register_log("test.FailingLog", Arc::new(FailingLog)).unwrap();
    let sibling = native.open("sibling", &configuration(b"sibling"), rtdb_node::LogAccess::ReadOnly, CancellationToken::new()).await.unwrap();
    let _sibling_store = stores.recv().await.unwrap();
    let mut configuration = configuration(b"failed");
    configuration.log = Some(database_config::Log::OtherLog(Any { type_url: "type.googleapis.com/test.FailingLog".into(), value: Vec::new() }));
    let error = match native.open("failed", &configuration, rtdb_node::LogAccess::ReadOnly, CancellationToken::new()).await {
        Err(error) => error,
        Ok(_) => panic!("log opening succeeded"),
    };
    assert_eq!(error.code, "test/log-open");
    let failed_store = stores.recv().await.unwrap();
    let key = ObjectKey::new("probe").unwrap();
    assert!(matches!(failed_store.head(&key).await, Err(rtdb_storage::Error::Closed(_))));
    sibling.store.put(&key, vec![3].into()).await.unwrap();
    assert_eq!(sibling.store.get(&key).await.unwrap().as_ref(), &[3]);
    native.close().await.unwrap();
}
fn configuration(value: &[u8]) -> DatabaseConfig {
    DatabaseConfig { storage: Some(database_config::Storage::RemoteStorage(RemoteStorage {
        object_store: Some(Any { type_url: "type.googleapis.com/test.Remote".into(), value: value.to_vec() }), epoch: None,
    })), ..Default::default() }
}

#[tokio::test]
async fn remote_resources_require_a_cache_and_cache_each_store_apart_whatever_the_database_name() {
    let native = NativeResources::default().register_storage("test.Remote", Arc::new(Remote)).unwrap();
    assert!(native.validate("first", &configuration(b"one")).unwrap_err().message.as_deref().is_some_and(|message| message.contains("disk cache")));
    let directory = tempfile::tempdir().unwrap();
    let cache = DiskCache::open(directory.path(), 1024, MemoryBudget::new(4 * 1024 * 1024)).unwrap();
    let native = native.with_disk_cache(cache);
    let key = ObjectKey::new("same").unwrap();
    let first = native.open("first", &configuration(b"one"), rtdb_node::LogAccess::ReadWrite, CancellationToken::new()).await.unwrap();
    let second = native.open("second", &configuration(b"two"), rtdb_node::LogAccess::ReadWrite, CancellationToken::new()).await.unwrap();
    assert_eq!(first.store.get(&key).await.unwrap().as_ref(), b"one");
    assert_eq!(second.store.get(&key).await.unwrap().as_ref(), b"two");
    let reattached = native.open("first", &configuration(b"three"), rtdb_node::LogAccess::ReadWrite, CancellationToken::new()).await.unwrap();
    assert_eq!(reattached.store.get(&key).await.unwrap().as_ref(), b"three", "a name re-attached to another store is not served the previous store's cached object");
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
async fn host_log_clock_stamps_local_source_transactions_and_keeps_the_non_transaction_policy() {
    use rtdb_types::Timestamp;
    use rtdb_format::envelope::{SourceBody, SourceMessage};
    struct FixedClock;
    impl rtdb_node::Clock for FixedClock { fn now(&self) -> Timestamp { Timestamp(123) } }
    for use_for_non_transactions in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let native = NativeResources::default().with_local_log_clock(Arc::new(FixedClock), use_for_non_transactions).with_log_clock(Arc::new(|| Ok(Timestamp(456))));
        let config = DatabaseConfig { log: Some(database_config::Log::LocalLog(rtdb_format::database::proto::LocalLog {
            path: Some(directory.path().to_string_lossy().into_owned()), epoch: None,
        })), ..Default::default() };
        let shutdown = CancellationToken::new();
        let opened = native.open("xtdb", &config, rtdb_node::LogAccess::ReadWrite, shutdown.clone()).await.unwrap();
        let tasks = opened.logs.drivers.into_iter().map(tokio::spawn).collect::<Vec<_>>();
        let transaction = SourceMessage::Protobuf(SourceBody::Tx(Default::default())).encode().unwrap();
        let other = SourceMessage::Protobuf(SourceBody::DetachDatabase(Default::default())).encode().unwrap();
        assert_eq!(opened.logs.source.enqueue(transaction.into(), Timestamp(10)).await.unwrap().durable().await.unwrap().timestamp, Timestamp(456));
        let source = opened.logs.source.enqueue(other.into(), Timestamp(10)).await.unwrap().durable().await.unwrap().timestamp;
        assert_eq!(source == Timestamp(456), use_for_non_transactions);
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

#[tokio::test]
async fn plain_remote_stores_with_a_location_share_xtdbs_c1_disk_cache_layout() {
    struct LocatedRemote(Arc<MemoryStore>);
    #[async_trait]
    impl RemoteStorageFactory for LocatedRemote {
        fn storage_location(&self, _: &Any, root: &ObjectKey) -> Result<Option<String>> {
            Ok(Some(format!("s3://bucket/{}", root.as_str())))
        }
        async fn open(&self, _: &Any, _: &ObjectKey, _: Arc<rtdb_storage::IoRegion>) -> Result<Arc<dyn ObjectStore>> {
            Ok(Arc::clone(&self.0) as Arc<dyn ObjectStore>)
        }
    }

    let cache_dir = tempfile::tempdir().unwrap();
    let expected_prefix = cache_dir.path().join("c1/e5a7a9f09a7a6beba24351e69e57218b");
    let prewarmed = expected_prefix.join("blocks/b00.binpb");
    std::fs::create_dir_all(prewarmed.parent().unwrap()).unwrap();
    std::fs::write(&prewarmed, b"from-xtdb-cache").unwrap();

    let remote = Arc::new(MemoryStore::default());
    remote.put(&ObjectKey::new("blocks/b01.binpb").unwrap(), b"from-remote".as_slice().into()).await.unwrap();
    let cache = DiskCache::open(cache_dir.path(), 1024 * 1024, MemoryBudget::new(4 * 1024 * 1024)).unwrap();
    let native = NativeResources::default()
        .with_disk_cache(cache)
        .register_storage("test.Located", Arc::new(LocatedRemote(remote)))
        .unwrap();
    let config = DatabaseConfig {
        storage: Some(database_config::Storage::RemoteStorage(RemoteStorage {
            object_store: Some(Any { type_url: "type.googleapis.com/test.Located".into(), value: Vec::new() }),
            epoch: Some(0),
        })),
        ..Default::default()
    };
    let opened = native.open("xtdb", &config, rtdb_node::LogAccess::ReadOnly, CancellationToken::new()).await.unwrap();
    assert_eq!(opened.store.get(&ObjectKey::new("blocks/b00.binpb").unwrap()).await.unwrap().as_ref(), b"from-xtdb-cache");
    assert_eq!(opened.store.get(&ObjectKey::new("blocks/b01.binpb").unwrap()).await.unwrap().as_ref(), b"from-remote");
    assert_eq!(std::fs::read(expected_prefix.join("blocks/b01.binpb")).unwrap(), b"from-remote");
    drop(opened);
    native.close().await.unwrap();
}

#[test]
fn multi_partition_database_config_fails_with_xtdbs_error_code() {
    let config = DatabaseConfig { partitions: Some(2), ..Default::default() };
    let error = NativeResources::default().validate("xtdb", &config).unwrap_err();
    assert_eq!(error.code, "xtdb/multi-partition-not-yet-enabled");
    assert_eq!(error.context.get("partitions"), Some(&serde_json::json!(2)));
}
