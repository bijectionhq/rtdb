use crate::{Error, ObjectKey, ObjectStore, Result, StoredObject};
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{ObjectStore as ProviderStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use std::{ops::Range, sync::Arc};

pub struct RemoteStore {
    client: Arc<dyn ProviderStore>,
    prefix: Option<ObjectKey>,
}

impl RemoteStore {
    /// The prefix scopes all keys to one configured database partition and storage generation.
    pub fn new(client: Arc<dyn ProviderStore>, prefix: Option<ObjectKey>) -> Self { Self { client, prefix } }

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
