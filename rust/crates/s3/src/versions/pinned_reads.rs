use std::{collections::BTreeMap, ops::Range, path::Path, sync::Arc};

use bytes::Bytes;
use rtdb_storage::{Error, ObjectKey, ObjectObservation, ObjectStore, StoredObject, remote::valid_object_version};
use rtdb_types::{PhysicalObjectVersion, Result};

use super::{VersionedInventory, unavailable, version_error};

struct PinnedReads {
    inventory: VersionedInventory,
    versions: BTreeMap<String, PhysicalObjectVersion>,
}

impl VersionedInventory {
    /// Opens a read-only view with one explicit S3 version per physical key.
    /// The caller supplies the selection and checks any required legal holds.
    /// Unselected keys are missing, listing returns an error, and ranges are bounded to 16 MiB.
    pub fn pinned_read_store(&self, versions: &[PhysicalObjectVersion]) -> Result<Arc<dyn ObjectStore>> {
        let root = self.key_layout.provider_path(self.root.as_str())?;
        let prefix = format!("{root}/");
        let mut selected = BTreeMap::new();
        for version in versions {
            if version.namespace != self.namespace
                || !version.key.strip_prefix(&prefix).is_some_and(|relative| !relative.is_empty())
                || !valid_object_version(version.version.as_bytes())
            {
                return Err(unavailable("The pinned read does not name an immutable version in this allocation"));
            }
            if selected.insert(version.key.clone(), version.clone()).is_some() {
                return Err(unavailable("The pinned read names more than one version of an object"));
            }
        }
        Ok(Arc::new(PinnedReads { inventory: self.clone(), versions: selected }))
    }
}

impl PinnedReads {
    fn selected(&self, key: &ObjectKey) -> rtdb_storage::Result<&PhysicalObjectVersion> {
        let path = self.inventory.object_path(key)?;
        self.versions.get(path.as_ref()).ok_or_else(|| Error::MissingObject(key.to_string()))
    }

    async fn read(&self, key: &ObjectKey, expected: Option<usize>) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        let selected = self.selected(key)?.clone();
        let value = self.inventory.inspect((selected.key.clone(), selected.version.clone()), move |owner, (path, version)| async move {
            let mut result = owner.client.get_object().bucket(&owner.namespace.bucket).key(path).version_id(&version)
                .set_request_payer(owner.request_payer.clone()).send().await
                .map_err(|error| unavailable(format!("Reading the pinned S3 object failed: {error}")))?;
            let length = result.content_length().and_then(|length| usize::try_from(length).ok())
                .filter(|length| expected.is_none_or(|expected| *length == expected))
                .ok_or_else(|| unavailable("S3 returned a different pinned object length"))?;
            if result.version_id() != Some(version.as_str()) || result.delete_marker().unwrap_or(false) {
                return Err(unavailable("S3 returned a different pinned object version"));
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = result.body.next().await {
                let chunk = chunk.map_err(|error| unavailable(format!("Reading the pinned S3 object body failed: {error}")))?;
                if chunk.len() > length - bytes.len() {
                    return Err(unavailable("S3 returned a longer pinned object"));
                }
                bytes.extend_from_slice(&chunk);
            }
            if bytes.len() != length {
                return Err(unavailable("S3 returned an incomplete pinned object"));
            }
            Ok(Bytes::from(bytes))
        }).await.map_err(|error| version_error(key, error))?;
        Ok(ObjectObservation { value, version: Some(selected) })
    }
}

#[async_trait::async_trait]
impl ObjectStore for PinnedReads {
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        Ok(self.observe_head(key).await?.value)
    }

    async fn observe_head(&self, key: &ObjectKey) -> rtdb_storage::Result<ObjectObservation<StoredObject>> {
        let selected = self.selected(key)?.clone();
        let size = self.inventory.inspect((selected.key.clone(), selected.version.clone()), |owner, (path, version)| async move {
            let result = owner.client.head_object().bucket(&owner.namespace.bucket).key(path).version_id(&version)
                .set_request_payer(owner.request_payer.clone()).send().await
                .map_err(|error| unavailable(format!("Reading pinned S3 object metadata failed: {error}")))?;
            if result.version_id() != Some(version.as_str()) || result.delete_marker().unwrap_or(false) {
                return Err(unavailable("S3 returned metadata for a different pinned object version"));
            }
            result.content_length().and_then(|length| u64::try_from(length).ok())
                .ok_or_else(|| unavailable("S3 returned no length for the pinned object"))
        }).await.map_err(|error| version_error(key, error))?;
        Ok(ObjectObservation { value: StoredObject { key: key.clone(), size }, version: Some(selected) })
    }

    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        Ok(self.read(key, None).await?.value)
    }

    async fn get_fresh_exact(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<Bytes> {
        Ok(self.observe_fresh_exact(key, length).await?.value)
    }

    async fn observe_fresh_exact(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        self.read(key, Some(length)).await
    }

    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
        Ok(self.observe_range(key, range).await?.value)
    }

    async fn observe_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        if range.is_empty() {
            let observed = self.observe_head(key).await?;
            if range.start == range.end && range.end <= observed.value.size {
                return Ok(ObjectObservation { value: Bytes::new(), version: observed.version });
            }
            return Err(Error::InvalidRange { key: key.to_string(), start: range.start, end: range.end, length: observed.value.size });
        }
        let version = self.selected(key)?;
        let value = self.inventory.read_version_range(key, version, range).await
            .map_err(|error| version_error(key, error))?;
        Ok(ObjectObservation { value, version: Some(version.clone()) })
    }

    async fn put(&self, _: &ObjectKey, _: Bytes) -> rtdb_storage::Result<()> { Err(read_only()) }
    async fn put_file(&self, _: &ObjectKey, _: &Path) -> rtdb_storage::Result<()> { Err(read_only()) }
    async fn delete(&self, _: &ObjectKey) -> rtdb_storage::Result<()> { Err(read_only()) }
    async fn list(&self, _: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
        Err(Error::Closed("The pinned S3 view only reads explicitly selected objects".into()))
    }
}

fn read_only() -> Error {
    Error::Closed("The pinned S3 view is read-only".into())
}
