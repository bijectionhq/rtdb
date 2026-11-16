use crate::{Error, ObjectKey, ObjectStore, Result, StoredObject};
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{ObjectStore as ProviderStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use std::{ops::Range, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

pub struct RemoteStore {
    client: Arc<dyn ProviderStore>,
    prefix: Option<ObjectKey>,
}

impl RemoteStore {
    /// The prefix scopes all keys to one configured database partition and storage generation.
    pub fn new(client: Arc<dyn ProviderStore>, prefix: Option<ObjectKey>) -> Self { Self { client, prefix } }

    /// Copies between explicit roots backed by the same provider client.
    /// Distinct client instances are rejected before I/O, even when they name the same bucket.
    /// The provider must support conditional copy; existing destinations are accepted only when their bytes match.
    /// Missing sources return MissingObject and different destination bytes return ConflictingObject.
    /// Incompatible clients, unsupported copy operations and provider failures return Remote; a failed request may have published the destination.
    pub async fn copy_to(&self, source: &ObjectKey, destination: &RemoteStore, key: &ObjectKey) -> Result<()> {
        if !Arc::ptr_eq(&self.client, &destination.client) {
            return Err(Error::Remote { key: key.to_string(), message: "object copy requires a shared provider client".into() });
        }
        match self.client.copy_if_not_exists(&self.path(source), &destination.path(key)).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }) => {
                let length = self.head(source).await?.size;
                if destination.head(key).await?.size != length { return Err(Error::ConflictingObject(key.to_string())); }
                let mut offset = 0;
                while offset < length {
                    let end = offset.saturating_add(1024 * 1024).min(length);
                    let source_bytes = self.get_range(source, offset..end).await?;
                    let destination_bytes = destination.get_range(key, offset..end).await?;
                    if source_bytes != destination_bytes { return Err(Error::ConflictingObject(key.to_string())); }
                    offset = end;
                }
                Ok(())
            }
            Err(error) => Err(provider_error(source, error)),
        }
    }

    fn path(&self, key: &ObjectKey) -> Path {
        Path::from(match &self.prefix { Some(prefix) => format!("{prefix}/{key}"), None => key.to_string() })
    }

    fn key(&self, path: &Path) -> Result<ObjectKey> {
        let value = path.as_ref();
        let relative = match &self.prefix {
            Some(prefix) => value.strip_prefix(&format!("{prefix}/")).ok_or_else(|| Error::InvalidKey(value.into()))?,
            None => value,
        };
        ObjectKey::new(relative)
    }

    async fn compare_file(&self, key: &ObjectKey, file: &mut tokio::fs::File, source: &std::path::Path, length: u64) -> Result<()> {
        if self.head(key).await?.size != length { return Err(Error::ConflictingObject(key.to_string())); }
        file.rewind().await.map_err(|error| Error::io(source, error))?;
        let mut offset = 0;
        let mut buffer = vec![0u8; 1024 * 1024];
        while offset < length {
            let count = (length - offset).min(buffer.len() as u64) as usize;
            file.read_exact(&mut buffer[..count]).await.map_err(|error| Error::io(source, error))?;
            if self.get_range(key, offset..offset + count as u64).await?.as_ref() != &buffer[..count] {
                return Err(Error::ConflictingObject(key.to_string()));
            }
            offset += count as u64;
        }
        Ok(())
    }
}

fn provider_error(key: &ObjectKey, error: object_store::Error) -> Error {
    match error {
        object_store::Error::NotFound { .. } => Error::MissingObject(key.to_string()),
        error => Error::Remote { key: key.to_string(), message: error.to_string() },
    }
}

#[async_trait]
impl ObjectStore for RemoteStore {
    async fn head(&self, key: &ObjectKey) -> Result<StoredObject> {
        let metadata = self.client.head(&self.path(key)).await.map_err(|error| provider_error(key, error))?;
        Ok(StoredObject { key: key.clone(), size: metadata.size })
    }

    async fn get(&self, key: &ObjectKey) -> Result<Bytes> {
        self.client.get(&self.path(key)).await.map_err(|error| provider_error(key, error))?
            .bytes().await.map_err(|error| provider_error(key, error))
    }

    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<Bytes> {
        let length = self.head(key).await?.size;
        if range.start > range.end || range.end > length {
            return Err(Error::InvalidRange { key: key.to_string(), start: range.start, end: range.end, length });
        }
        if range.is_empty() { return Ok(Bytes::new()); }
        let bytes = self.client.get_range(&self.path(key), range.clone()).await.map_err(|error| provider_error(key, error))?;
        if bytes.len() as u64 != range.end - range.start {
            return Err(Error::Remote { key: key.to_string(), message: "provider returned an incomplete byte range".into() });
        }
        Ok(bytes)
    }

    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> Result<()> {
        let options = PutOptions { mode: PutMode::Create, ..Default::default() };
        match self.client.put_opts(&self.path(key), bytes.clone().into(), options).await {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }) => {
                if self.get(key).await? == bytes { Ok(()) } else { Err(Error::ConflictingObject(key.to_string())) }
            }
            Err(error) => Err(provider_error(key, error)),
        }
    }

    async fn put_file(&self, key: &ObjectKey, source: &std::path::Path) -> Result<()> {
        let mut input = tokio::fs::File::open(source).await.map_err(|error| Error::io(source, error))?;
        let length = input.metadata().await.map_err(|error| Error::io(source, error))?.len();
        match self.head(key).await {
            Ok(_) => return self.compare_file(key, &mut input, source, length).await,
            Err(Error::MissingObject(_)) => (),
            Err(error) => return Err(error),
        }
        if length == 0 { return self.put(key, Bytes::new()).await; }
        let temporary = format!(".tmp/upload-{}", uuid::Uuid::new_v4());
        let temporary = Path::from(match &self.prefix { Some(prefix) => format!("{prefix}/{temporary}"), None => temporary });
        let mut upload = self.client.put_multipart(&temporary).await.map_err(|error| provider_error(key, error))?;
        let uploaded: Result<()> = async {
            let mut remaining = length;
            while remaining != 0 {
                let count = remaining.min(5 * 1024 * 1024) as usize;
                let mut bytes = vec![0u8; count];
                input.read_exact(&mut bytes).await.map_err(|error| Error::io(source, error))?;
                upload.put_part(Bytes::from(bytes).into()).await.map_err(|error| provider_error(key, error))?;
                remaining -= count as u64;
            }
            upload.complete().await.map_err(|error| provider_error(key, error))?;
            Ok(())
        }.await;
        if let Err(error) = uploaded {
            if let Err(cleanup) = upload.abort().await {
                return Err(Error::Remote { key: key.to_string(), message: format!("{error}; multipart abort also failed: {cleanup}") });
            }
            return Err(error);
        }
        let published = match self.client.copy_if_not_exists(&temporary, &self.path(key)).await {
            Ok(()) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }) => self.compare_file(key, &mut input, source, length).await,
            Err(error) => Err(provider_error(key, error)),
        };
        let cleanup = self.client.delete(&temporary).await.map_err(|error| provider_error(key, error));
        published.and(cleanup)
    }

    async fn copy(&self, source: &ObjectKey, destination: &ObjectKey) -> Result<()> {
        self.copy_to(source, self, destination).await
    }

    async fn list(&self, directory: Option<&ObjectKey>) -> Result<Vec<StoredObject>> {
        let path = directory.map(|key| self.path(key)).or_else(|| self.prefix.as_ref().map(|key| Path::from(key.as_str())));
        let mut stream = self.client.list(path.as_ref());
        let mut result = Vec::new();
        while let Some(metadata) = stream.try_next().await.map_err(|error| Error::Remote {
            key: path.as_ref().map(ToString::to_string).unwrap_or_default(), message: error.to_string(),
        })? {
            if self.prefix.as_ref().is_some_and(|prefix| !metadata.location.as_ref().starts_with(&format!("{prefix}/"))) {
                continue;
            }
            let relative = match &self.prefix {
                Some(prefix) => metadata.location.as_ref().strip_prefix(&format!("{prefix}/")).unwrap_or_default(),
                None => metadata.location.as_ref(),
            };
            if relative.split('/').any(|part| part == ".tmp") { continue; }
            let key = self.key(&metadata.location)?;
            if directory.is_none_or(|directory| key.is_under(directory)) {
                result.push(StoredObject { key, size: metadata.size });
            }
        }
        result.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(result)
    }

    async fn delete(&self, key: &ObjectKey) -> Result<()> {
        match self.client.delete(&self.path(key)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(error) => Err(provider_error(key, error)),
        }
    }
}
