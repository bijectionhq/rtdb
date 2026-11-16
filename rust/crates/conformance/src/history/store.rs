use async_trait::async_trait;
use bytes::Bytes;
use rtdb_format::database::proto::DatabaseConfig;
use rtdb_node::{DatabaseResources, LogAccess, ResourceFactory};
use rtdb_storage::{ObjectKey, ObjectStore, ReadReservation, RetainedBytes, StoredObject};
use std::{ops::Range, path::Path, sync::{Arc, atomic::{AtomicU64, Ordering}}};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub(super) struct Reads { bytes: AtomicU64, calls: AtomicU64 }
impl Reads {
    pub fn snapshot(&self) -> (u64, u64) { (self.bytes.load(Ordering::SeqCst), self.calls.load(Ordering::SeqCst)) }
    fn record(&self, bytes: usize) { self.bytes.fetch_add(bytes as u64, Ordering::SeqCst); self.calls.fetch_add(1, Ordering::SeqCst); }
}
pub(super) struct CountingResources { pub inner: Arc<dyn ResourceFactory>, pub reads: Arc<Reads> }
#[async_trait]
impl ResourceFactory for CountingResources {
    fn validate(&self, name: &str, configuration: &DatabaseConfig) -> rtdb_types::Result<()> { self.inner.validate(name, configuration) }
    async fn open(&self, name: &str, configuration: &DatabaseConfig, access: LogAccess, cancellation: CancellationToken) -> rtdb_types::Result<DatabaseResources> {
        let mut resources = self.inner.open(name, configuration, access, cancellation).await?;
        resources.store = Arc::new(CountingStore { inner: resources.store, reads: self.reads.clone() });
        Ok(resources)
    }
    async fn close(&self) -> rtdb_types::Result<()> { self.inner.close().await }
}
struct CountingStore { inner: Arc<dyn ObjectStore>, reads: Arc<Reads> }
#[async_trait]
impl ObjectStore for CountingStore {
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head(key).await }
    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        let bytes = self.inner.get(key).await?; self.reads.record(bytes.len()); Ok(bytes)
    }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
        let bytes = self.inner.get_range(key, range).await?; self.reads.record(bytes.len()); Ok(bytes)
    }
    async fn get_reserved_range(&self, key: &ObjectKey, range: Range<u64>, reservation: ReadReservation) -> rtdb_storage::Result<RetainedBytes> {
        let bytes = self.inner.get_reserved_range(key, range, reservation).await?; self.reads.record(bytes.len()); Ok(bytes)
    }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> { self.inner.put(key, bytes).await }
    async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> { self.inner.put_file(key, source).await }
    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> { self.inner.list(directory).await }
    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.inner.delete(key).await }
}
