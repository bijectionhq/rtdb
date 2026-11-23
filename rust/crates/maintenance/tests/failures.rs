use rtdb_types::StructFields;
use std::{ops::Range, path::Path, sync::{Arc, Mutex, atomic::{AtomicUsize, Ordering}}};
use async_trait::async_trait;
use bytes::Bytes;
use rtdb_columnar::MemoryBudget;
use rtdb_format::{data::{StoredEvent, StoredOperation, build_event_batch}, ipc::write_file, keys::{TableSlug, TrieKey}, log::proto::TriesAdded, metadata::MetadataBuilder};
use rtdb_index::{CatalogTrie, PartitionState, PartitionValue, TableCatalog, TrieCatalog, TrieState};
use rtdb_maintenance::{CompactionDriver, CompactionExecutor, CompactionFailure, CompactionJob, CompactionObserver, CompactionPublisher, MergeConfig, SegmentCompactor};
use rtdb_storage::{Error as StorageError, MemoryStore, ObjectKey, ObjectStore, StoredObject};
use rtdb_trie::TrieConfig;
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, Iid, Interval, LogicalType, Result, ScalarType, SourcePosition, StorageEpoch, StorageVersion, TableRef, Timestamp, Value};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Recorder(Mutex<Vec<CompactionFailure>>);
impl CompactionObserver for Recorder {
    fn failed(&self, failure: &CompactionFailure) { self.0.lock().unwrap().push(failure.clone()); }
}
impl Recorder { fn events(&self) -> Vec<CompactionFailure> { self.0.lock().unwrap().clone() } }

fn denied() -> Anomaly {
    Anomaly::new(AnomalyCategory::Forbidden, "rtdb.storage/permission-denied", "https://user:secret@provider.invalid/object?token=secret")
        .with_context("key", "tables/foo/meta/l01-rc-b00.arrow".into()).with_context("operation", "put_file".into())
}

fn state() -> Arc<PartitionState> {
    let table = TableRef::new("public", "foo");
    let catalog = TrieCatalog::default().with_tries(table, [CatalogTrie { key: "l00-rc-b00".parse().unwrap(), data_file_size: 1, metadata: None, state: TrieState::Live, artefacts: Vec::new() }], Timestamp(0)).unwrap();
    Arc::new(PartitionState::new(PartitionValue::loaded(catalog, TableCatalog::default(), TrieConfig::default()).unwrap()))
}

struct FailedJob { error: Anomaly, calls: AtomicUsize }
#[async_trait]
impl CompactionExecutor for FailedJob {
    async fn execute(&self, _: &CompactionJob, _: &CancellationToken) -> Result<TriesAdded> {
        self.calls.fetch_add(1, Ordering::SeqCst); Err(self.error.clone())
    }
}

struct RejectPublication;
#[async_trait]
impl CompactionPublisher for RejectPublication {
    async fn publish(&self, _: TriesAdded) -> Result<SourcePosition> { panic!("A failed executor cannot publish") }
}

#[test]
fn failure_json_has_typed_identity_and_sha256_without_raw_messages_or_arbitrary_context() {
    let table = TableRef::new("public", "foo"); let output: TrieKey = "l01-rc-b00".parse().unwrap();
    let error = denied().with_context("endpoint", "https://user:secret@provider.invalid".into());
    let event = CompactionFailure::new(&table, &output, &error);
    let mut bytes = Vec::new(); event.write_json_line(&mut bytes).unwrap();
    assert_eq!(bytes.last(), Some(&b'\n'));
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["event"], "compaction_failed"); assert_eq!(json["table"], "public/foo");
    assert_eq!(json["output_trie_key"], "l01-rc-b00");
    assert_eq!(json["anomaly_code"], "rtdb.storage/permission-denied"); assert_eq!(json["anomaly_category"], "forbidden");
    assert_eq!(json["operation"], "put_file"); assert_eq!(json["object_key"], "tables/foo/meta/l01-rc-b00.arrow");
    assert_eq!(json["permission_denied"], true);
    assert_eq!(json["message_sha256"].as_str().unwrap().len(), 64);
    assert_eq!(json.as_object().unwrap().len(), 9);
    let text = String::from_utf8(bytes).unwrap();
    assert!(!text.contains("secret")); assert!(!text.contains("provider.invalid")); assert!(!text.contains("endpoint"));
    let known = CompactionFailure::new(&table, &output, &Anomaly::fault("test/hash", "abc"));
    assert_eq!(known.message_sha256.as_deref(), Some("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"));
    let misleading = Anomaly::new(AnomalyCategory::Forbidden, "test/other", "PermissionDenied");
    assert!(!CompactionFailure::new(&table, &output, &misleading).permission_denied);
    let invalid = misleading.with_context("operation", "https://secret.invalid".into()).with_context("key", "https://secret.invalid/object".into());
    let invalid = CompactionFailure::new(&table, &output, &invalid);
    assert!(invalid.operation.is_none()); assert!(invalid.object_key.is_none());
}

#[test]
fn absent_messages_have_no_digest_and_empty_messages_hash_empty_bytes() {
    let table = TableRef::new("public", "foo"); let output: TrieKey = "l01-rc-b00".parse().unwrap();
    let absent = CompactionFailure::new(&table, &output, &Anomaly::new_nullable(AnomalyCategory::Fault, "test/error", None));
    assert!(absent.message_sha256.is_none());
    let json = serde_json::to_value(&absent).unwrap();
    assert_eq!(json.get("message_sha256"), Some(&serde_json::Value::Null));
    assert!(json.get("message").is_none());
    let empty = CompactionFailure::new(&table, &output, &Anomaly::fault("test/error", ""));
    assert_eq!(empty.message_sha256.as_deref(), Some("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));
}

#[test]
fn json_sink_reports_write_errors_to_its_caller() {
    struct Broken;
    impl std::io::Write for Broken {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> { Err(std::io::Error::other("sink closed")) }
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
    }
    let event = CompactionFailure::new(&TableRef::new("public", "foo"), &"l01-rc-b00".parse().unwrap(), &denied());
    assert!(event.write_json_line(Broken).is_err());
}

#[tokio::test]
async fn suppressed_job_failure_emits_once_across_later_signals_and_compact_all_requests() {
    let executor = Arc::new(FailedJob { error: denied(), calls: AtomicUsize::new(0) });
    let observer = Arc::new(Recorder::default());
    let (driver, handle) = CompactionDriver::new(state(), executor.clone(), Arc::new(RejectPublication), 1, 2, false).unwrap();
    let cancel = CancellationToken::new();
    let task = tokio::spawn(driver.with_observer(observer.clone()).run(cancel.clone()));
    for _ in 0..2 {
        handle.signal_block();
        assert_eq!(handle.compact_all().await.unwrap_err(), denied());
    }
    assert_eq!(executor.calls.load(Ordering::SeqCst), 1);
    assert_eq!(observer.events().len(), 1);
    cancel.cancel(); task.await.unwrap().unwrap();
    assert_eq!(observer.events().len(), 1);
}

struct CancelThenFail { started: Semaphore, cancelling: Semaphore, release: Semaphore, error: Anomaly }
#[async_trait]
impl CompactionExecutor for CancelThenFail {
    async fn execute(&self, _: &CompactionJob, cancellation: &CancellationToken) -> Result<TriesAdded> {
        self.started.add_permits(1);
        cancellation.cancelled().await;
        self.cancelling.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        Err(self.error.clone())
    }
}

#[tokio::test]
async fn two_databases_share_a_slot_until_cancelled_uploads_finish() {
    let slots = Arc::new(Semaphore::new(1));
    let first = Arc::new(CancelThenFail {
        started: Semaphore::new(0), cancelling: Semaphore::new(0), release: Semaphore::new(0),
        error: Anomaly::new(AnomalyCategory::Interrupted, "test/cancelled", "The job was cancelled"),
    });
    let second = Arc::new(FailedJob { error: denied(), calls: AtomicUsize::new(0) });
    let (first_driver, _) = CompactionDriver::new(state(), first.clone(), Arc::new(RejectPublication), 2, 1, false).unwrap();
    let (second_driver, second_handle) = CompactionDriver::new(state(), second.clone(), Arc::new(RejectPublication), 2, 1, false).unwrap();
    let first_cancel = CancellationToken::new();
    let second_cancel = CancellationToken::new();
    let first_run = tokio::spawn(first_driver.with_job_slots(slots.clone()).run(first_cancel.clone()));
    let mut second_run = Box::pin(second_driver.with_job_slots(slots.clone()).run(second_cancel.clone()));

    first.started.acquire().await.unwrap().forget();
    let second_pending_before_cancel = futures::poll!(second_run.as_mut()).is_pending();
    let calls_before_cancel = second.calls.load(Ordering::SeqCst);
    first_cancel.cancel();
    first.cancelling.acquire().await.unwrap().forget();
    let second_pending_during_cancel = second_pending_before_cancel && futures::poll!(second_run.as_mut()).is_pending();
    let calls_during_cancel = second.calls.load(Ordering::SeqCst);

    first.release.add_permits(1);
    first_run.await.unwrap().unwrap();
    assert!(second_pending_before_cancel);
    assert_eq!(calls_before_cancel, 0);
    assert!(second_pending_during_cancel);
    assert_eq!(calls_during_cancel, 0);
    let completion = tokio::select! {
        result = second_handle.compact_all() => result,
        result = &mut second_run => panic!("Second driver exited before replying: {result:?}"),
    };
    assert_eq!(completion.unwrap_err(), denied());
    assert_eq!(second.calls.load(Ordering::SeqCst), 1);
    second_cancel.cancel();
    second_run.await.unwrap();
    assert_eq!(slots.available_permits(), 1);
}

#[tokio::test]
async fn shutdown_reports_actual_worker_failure_after_joining_and_does_not_report_it_twice() {
    let executor = Arc::new(CancelThenFail { started: Semaphore::new(0), cancelling: Semaphore::new(0), release: Semaphore::new(0), error: denied() });
    let observer = Arc::new(Recorder::default());
    let (driver, handle) = CompactionDriver::new(state(), executor.clone(), Arc::new(RejectPublication), 1, 1, false).unwrap();
    let cancel = CancellationToken::new();
    let mut task = tokio::spawn(driver.with_observer(observer.clone()).run(cancel.clone()));
    let mut request = Box::pin(handle.compact_all()); assert!(futures::poll!(request.as_mut()).is_pending());
    executor.started.acquire().await.unwrap().forget();
    cancel.cancel(); executor.cancelling.acquire().await.unwrap().forget();
    assert!(futures::poll!(&mut task).is_pending()); assert!(observer.events().is_empty());
    executor.release.add_permits(1); task.await.unwrap().unwrap();
    assert_eq!(request.await.unwrap_err().code, "rtdb.maintenance/closed");
    let events = observer.events(); assert_eq!(events.len(), 1);
    assert_eq!(events[0].anomaly_code, "rtdb.storage/permission-denied"); assert!(events[0].permission_denied);
}

#[tokio::test]
async fn cooperative_executor_cancellation_is_joined_without_a_failure_event() {
    for error in [Anomaly::fault("rtdb.maintenance/closed", "Maintenance scope ended"),
        Anomaly::new(AnomalyCategory::Interrupted, "test/interrupted", "Interrupted compaction")] {
        let executor = Arc::new(CancelThenFail { started: Semaphore::new(0), cancelling: Semaphore::new(0), release: Semaphore::new(0), error });
        let observer = Arc::new(Recorder::default());
        let (driver, handle) = CompactionDriver::new(state(), executor.clone(), Arc::new(RejectPublication), 1, 1, false).unwrap();
        let cancel = CancellationToken::new();
        let mut task = tokio::spawn(driver.with_observer(observer.clone()).run(cancel.clone()));
        let mut request = Box::pin(handle.compact_all()); assert!(futures::poll!(request.as_mut()).is_pending());
        executor.started.acquire().await.unwrap().forget();
        cancel.cancel(); executor.cancelling.acquire().await.unwrap().forget();
        assert!(futures::poll!(&mut task).is_pending()); assert!(observer.events().is_empty());
        executor.release.add_permits(1);
        tokio::time::timeout(std::time::Duration::from_secs(5), task).await.unwrap().unwrap().unwrap();
        assert_eq!(request.await.unwrap_err().code, "rtdb.maintenance/closed");
        assert!(observer.events().is_empty());
    }
}

struct SuccessfulJob;
#[async_trait]
impl CompactionExecutor for SuccessfulJob {
    async fn execute(&self, _: &CompactionJob, _: &CancellationToken) -> Result<TriesAdded> { Ok(TriesAdded::default()) }
}

struct PublicationScope(Arc<AtomicUsize>);
impl Drop for PublicationScope { fn drop(&mut self) { self.0.fetch_add(1, Ordering::SeqCst); } }
struct PendingPublisher { started: Semaphore, dropped: Arc<AtomicUsize> }
#[async_trait]
impl CompactionPublisher for PendingPublisher {
    async fn publish(&self, _: TriesAdded) -> Result<SourcePosition> {
        let _scope = PublicationScope(Arc::clone(&self.dropped));
        self.started.add_permits(1);
        futures::future::pending().await
    }
}

#[tokio::test]
async fn cancelling_pending_publication_releases_its_future_without_a_failure_event() {
    let publisher = Arc::new(PendingPublisher { started: Semaphore::new(0), dropped: Arc::new(AtomicUsize::new(0)) });
    let observer = Arc::new(Recorder::default());
    let (driver, handle) = CompactionDriver::new(state(), Arc::new(SuccessfulJob), publisher.clone(), 1, 1, false).unwrap();
    let cancel = CancellationToken::new();
    let task = tokio::spawn(driver.with_observer(observer.clone()).run(cancel.clone()));
    let mut request = Box::pin(handle.compact_all()); assert!(futures::poll!(request.as_mut()).is_pending());
    publisher.started.acquire().await.unwrap().forget();
    assert_eq!(publisher.dropped.load(Ordering::SeqCst), 0);
    cancel.cancel();
    tokio::time::timeout(std::time::Duration::from_secs(5), task).await.unwrap().unwrap().unwrap();
    assert_eq!(request.await.unwrap_err().code, "rtdb.maintenance/closed");
    assert_eq!(publisher.dropped.load(Ordering::SeqCst), 1); assert!(observer.events().is_empty());
}
struct FailedPublisher(AtomicUsize);
#[async_trait]
impl CompactionPublisher for FailedPublisher {
    async fn publish(&self, _: TriesAdded) -> Result<SourcePosition> {
        self.0.fetch_add(1, Ordering::SeqCst); Err(Anomaly::fault("test/publication-failed", "append failed"))
    }
}

#[tokio::test]
async fn publication_failure_is_observed_as_its_own_actual_anomaly() {
    let observer = Arc::new(Recorder::default()); let publisher = Arc::new(FailedPublisher(AtomicUsize::new(0)));
    let (driver, handle) = CompactionDriver::new(state(), Arc::new(SuccessfulJob), publisher.clone(), 1, 1, false).unwrap();
    let cancel = CancellationToken::new(); let task = tokio::spawn(driver.with_observer(observer.clone()).run(cancel.clone()));
    assert_eq!(handle.compact_all().await.unwrap_err().code, "test/publication-failed");
    cancel.cancel(); task.await.unwrap().unwrap();
    assert_eq!(publisher.0.load(Ordering::SeqCst), 1);
    let events = observer.events(); assert_eq!(events.len(), 1);
    assert_eq!(events[0].anomaly_code, "test/publication-failed"); assert!(!events[0].permission_denied);
    assert!(events[0].operation.is_none()); assert!(events[0].object_key.is_none());
}

struct DenyUpload { inner: MemoryStore, denied: ObjectKey, local: bool, uploads: Mutex<Vec<ObjectKey>> }
#[async_trait]
impl ObjectStore for DenyUpload {
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head(key).await }
    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.inner.get(key).await }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> { self.inner.get_range(key, range).await }
    async fn put(&self, key: &ObjectKey, value: Bytes) -> rtdb_storage::Result<()> { self.inner.put(key, value).await }
    async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> {
        self.uploads.lock().unwrap().push(key.clone());
        if *key == self.denied {
            return Err(if self.local { StorageError::Io { path: source.into(), source: std::io::Error::new(std::io::ErrorKind::PermissionDenied, "secret endpoint token") } }
                else { StorageError::PermissionDenied { key: key.to_string(), message: "secret endpoint token".into() } });
        }
        self.inner.put_file(key, source).await
    }
    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> { self.inner.list(directory).await }
    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.inner.delete(key).await }
}

#[tokio::test]
async fn actual_metadata_upload_failure_preserves_storage_anomaly_and_emits_the_requested_relative_key() {
    for local in [false, true] {
        let temporary = tempfile::tempdir().unwrap();
        let table = TableRef::new("public", "foo"); let slug = TableSlug::for_unrecorded_table(&table).unwrap();
        let input = TrieKey::l0(BlockIndex(0)).unwrap(); let output: TrieKey = "l01-rc-b00".parse().unwrap();
        let data_key = ObjectKey::new(slug.data_key(&output)).unwrap(); let metadata_key = ObjectKey::new(slug.metadata_key(&output)).unwrap();
        let store = Arc::new(DenyUpload { inner: MemoryStore::default(), denied: metadata_key.clone(), local, uploads: Mutex::new(Vec::new()) });
        let rows = [StoredEvent { iid: Iid([0; 16]), system_from: Timestamp(1), valid_time: Interval::ALL,
            operation: StoredOperation::Put(StructFields::from([("value".into(), Value::I64(1))])), recency: None }];
        let data = build_event_batch(&rows, Some(StructFields::from([("value".into(), LogicalType::Scalar(ScalarType::I64))])), None).unwrap();
        let mut metadata = MetadataBuilder::new(false, true); metadata.write_leaf(&data).unwrap();
        let (metadata, _) = metadata.finish().unwrap();
        store.put(&ObjectKey::new(slug.data_key(&input)).unwrap(), Bytes::from(write_file(&data.schema(), &[data]).unwrap())).await.unwrap();
        store.put(&ObjectKey::new(slug.metadata_key(&input)).unwrap(), Bytes::from(metadata.write().unwrap())).await.unwrap();
        let tables = TableCatalog::default().with_seeded_table(table.clone(), ["value".into()]).unwrap();
        let tries = TrieCatalog::default().with_tries(table, [CatalogTrie { key: input, data_file_size: 1, metadata: None, state: TrieState::Live, artefacts: Vec::new() }], Timestamp(0)).unwrap();
        let state = Arc::new(PartitionState::new(PartitionValue::loaded(tries, tables, TrieConfig::default()).unwrap()));
        let memory = MemoryBudget::new(64 << 20);
        let executor = Arc::new(SegmentCompactor::new(store.clone(), state.clone(), memory.clone(), StorageVersion(6), StorageEpoch(0),
            MergeConfig { temporary_directory: temporary.path().into(), ..Default::default() }).unwrap());
        let observer = Arc::new(Recorder::default());
        let (driver, handle) = CompactionDriver::new(state, executor, Arc::new(RejectPublication), 1, 1, false).unwrap();
        let cancel = CancellationToken::new(); let task = tokio::spawn(driver.with_observer(observer.clone()).run(cancel.clone()));
        let error = handle.compact_all().await.unwrap_err(); cancel.cancel(); task.await.unwrap().unwrap();
        assert_eq!(error.category, AnomalyCategory::Forbidden); assert_eq!(error.code, "rtdb.storage/permission-denied");
        assert_eq!(error.context["key"], metadata_key.to_string()); assert_eq!(error.context["operation"], "put_file");
        assert_eq!(error.context.contains_key("path"), local);
        assert!(error.message.as_deref().is_some_and(|message| message.contains("secret endpoint token")));
        assert_eq!(*store.uploads.lock().unwrap(), [data_key.clone(), metadata_key.clone()]);
        assert!(store.head(&data_key).await.is_ok()); assert!(matches!(store.head(&metadata_key).await, Err(StorageError::MissingObject(_))));
        let events = observer.events(); assert_eq!(events.len(), 1);
        assert_eq!(events[0].table, "public/foo"); assert_eq!(events[0].output_trie_key, output.to_string());
        assert_eq!(events[0].object_key.as_deref(), Some(metadata_key.as_str())); assert_eq!(events[0].operation.as_deref(), Some("put_file"));
        assert!(events[0].permission_denied); assert_eq!(events[0].anomaly_category, AnomalyCategory::Forbidden);
        let mut encoded = Vec::new(); events[0].write_json_line(&mut encoded).unwrap();
        assert!(!String::from_utf8(encoded).unwrap().contains("secret"));
        assert_eq!(memory.used_bytes(), 0); assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 0);
    }
}
