use crate::{Error, IoRegion, ObjectKey, ObjectStore, ReadReservation, Result, RetainedBytes, StoredObject};
use async_trait::async_trait;
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{Attribute, GetOptions, ObjectStore as ProviderStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use std::{ops::Range, sync::Arc};
use tokio::io::{AsyncReadExt, AsyncSeekExt};

#[derive(Clone)]
pub struct RemoteStore {
    objects: Arc<RemoteObjects>,
    io: Arc<IoRegion>,
    single_put: bool,
}

impl RemoteStore {
    /// Opens an independent I/O region; the owner calls `close_and_wait` before runtime shutdown.
    /// The prefix scopes keys to one configured database partition and storage generation.
    pub fn new(client: Arc<dyn ProviderStore>, prefix: Option<ObjectKey>) -> Self {
        Self::new_in(client, prefix, IoRegion::new())
    }

    /// Uses the owner's I/O region, which must be closed and joined after all store consumers finish.
    /// Every admitted operation remains owned through caller cancellation, including publication cleanup.
    pub fn new_in(client: Arc<dyn ProviderStore>, prefix: Option<ObjectKey>, io: Arc<IoRegion>) -> Self {
        Self { objects: Arc::new(RemoteObjects { client, prefix }), io, single_put: false }
    }

    /// Cell storage publishes a final object with one conditional provider
    /// write. The underlying client must not retry that request automatically.
    /// A lost reply can be recovered only by observing this write's unique
    /// metadata on the resulting object.
    pub fn new_single_put(client: Arc<dyn ProviderStore>, prefix: ObjectKey, io: Arc<IoRegion>) -> Self {
        Self { objects: Arc::new(RemoteObjects { client, prefix: Some(prefix) }), io, single_put: true }
    }

    /// Rejects new requests and waits for admitted operations, including requests whose callers were cancelled.
    /// This closes the complete shared region when the store was opened with `new_in`.
    pub async fn close_and_wait(&self) { self.io.close_and_wait().await; }

    /// Checks whether a fresh provider observation is the result of this
    /// exact conditional publication. An absent object is still uncertain.
    pub async fn publication_confirmed(&self, key: &ObjectKey, attempt: uuid::Uuid) -> Result<bool> {
        let (objects, key) = (Arc::clone(&self.objects), key.clone());
        self.io.run_async(async move { objects.publication_confirmed(&key, attempt).await }).await
    }

    /// Copies between explicit roots backed by the same provider client.
    /// Distinct client instances are rejected before I/O, even when they name the same bucket.
    /// The provider must support conditional copy; existing destinations are accepted only when their bytes match.
    /// Missing sources return MissingObject, different destination bytes return ConflictingObject, and a closed region returns Closed.
    /// Provider authorization failures return PermissionDenied; a panicking provider task returns Task.
    /// Incompatible clients, unsupported copy operations and provider failures return Remote; a failed request may have published the destination.
    /// Both regions retain admitted work until provider completion, even if the caller cancels.
    pub async fn copy_to(&self, source: &ObjectKey, destination: &RemoteStore, key: &ObjectKey) -> Result<()> {
        if !Arc::ptr_eq(&self.objects.client, &destination.objects.client) {
            return Err(Error::Remote { key: key.to_string(), message: "object copy requires a shared provider client".into() });
        }
        let admission = self.io.admit()?.and(destination.io.admit()?);
        let (objects, destination_objects, source, key) = (Arc::clone(&self.objects), Arc::clone(&destination.objects), source.clone(), key.clone());
        if destination.single_put {
            let io = destination.io.clone();
            return IoRegion::run_async_admitted(admission, async move {
                let bytes = objects.get(&source).await?;
                destination_objects.put(&key, bytes, Some(&io)).await
            }).await;
        }
        IoRegion::run_async_admitted(admission, async move { objects.copy_to(&source, &destination_objects, &key).await }).await
    }
}

#[async_trait]
impl ObjectStore for RemoteStore {
    async fn head(&self, key: &ObjectKey) -> Result<StoredObject> {
        let (objects, key) = (Arc::clone(&self.objects), key.clone());
        self.io.run_async(async move { objects.head(&key).await }).await
    }
    async fn get(&self, key: &ObjectKey) -> Result<Bytes> {
        let (objects, key) = (Arc::clone(&self.objects), key.clone());
        self.io.run_async(async move { objects.get(&key).await }).await
    }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> Result<Bytes> {
        let (objects, key) = (Arc::clone(&self.objects), key.clone());
        self.io.run_async(async move { objects.get_range(&key, range).await }).await
    }
    async fn get_reserved_range(&self, key: &ObjectKey, range: Range<u64>, reservation: ReadReservation) -> Result<RetainedBytes> {
        reservation.check_range(&range)?;
        let (objects, key) = (Arc::clone(&self.objects), key.clone());
        self.io.run_async(async move { reservation.finish(objects.get_range(&key, range).await?) }).await
    }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> Result<()> {
        let (objects, key) = (Arc::clone(&self.objects), key.clone());
        let io = self.io.clone();
        let single_put = self.single_put;
        self.io.run_async(async move { objects.put(&key, bytes, single_put.then_some(io.as_ref())).await }).await
    }
    async fn put_file(&self, key: &ObjectKey, source: &std::path::Path) -> Result<()> {
        if self.single_put {
            return Err(Error::Remote { key: key.to_string(), message: "Cell writes must supply the final sealed bytes".into() });
        }
        let admission = self.io.admit()?;
        let input = std::fs::File::open(source).map_err(|error| Error::io(source, error))?;
        let (objects, key, source) = (Arc::clone(&self.objects), key.clone(), source.to_owned());
        IoRegion::run_async_admitted(admission, async move { objects.put_opened_file(&key, tokio::fs::File::from_std(input), &source).await }).await
    }
    async fn copy(&self, source: &ObjectKey, destination: &ObjectKey) -> Result<()> { self.copy_to(source, self, destination).await }
    async fn list(&self, directory: Option<&ObjectKey>) -> Result<Vec<StoredObject>> {
        let (objects, directory) = (Arc::clone(&self.objects), directory.cloned());
        self.io.run_async(async move { objects.list(directory.as_ref()).await }).await
    }
    async fn delete(&self, key: &ObjectKey) -> Result<()> {
        let (objects, key) = (Arc::clone(&self.objects), key.clone());
        self.io.run_async(async move { objects.delete(&key).await }).await
    }
}

struct RemoteObjects {
    client: Arc<dyn ProviderStore>,
    prefix: Option<ObjectKey>,
}

impl RemoteObjects {
    async fn copy_to(&self, source: &ObjectKey, destination: &RemoteObjects, key: &ObjectKey) -> Result<()> {
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
            Err(error @ object_store::Error::NotFound { .. }) => Err(provider_error(source, error)),
            Err(error) => Err(provider_error(key, error)),
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
        // A provider path is percent-encoded segment by segment, so a key it lists is the key that was put only once decoded.
        let decoded = relative.split('/').map(|part| percent_encoding::percent_decode_str(part).decode_utf8().map(|part| part.into_owned()))
            .collect::<std::result::Result<Vec<_>, _>>().map_err(|_| Error::InvalidKey(relative.into()))?.join("/");
        ObjectKey::new(decoded)
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

fn publication_attribute() -> Attribute { Attribute::Metadata("rtdb-publication".into()) }

fn provider_error(key: &ObjectKey, error: object_store::Error) -> Error {
    match error {
        object_store::Error::NotFound { .. } => Error::MissingObject(key.to_string()),
        error @ object_store::Error::PermissionDenied { .. } => Error::PermissionDenied { key: key.to_string(), message: error.to_string() },
        error => Error::Remote { key: key.to_string(), message: error.to_string() },
    }
}

impl RemoteObjects {
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

    async fn put(&self, key: &ObjectKey, bytes: Bytes, authority: Option<&IoRegion>) -> Result<()> {
        let mut options = PutOptions { mode: PutMode::Create, ..Default::default() };
        let publication = authority.map(|io| {
            let attempt = uuid::Uuid::new_v4();
            options.attributes.insert(publication_attribute(), attempt.to_string().into());
            (attempt, io.publication(attempt, key.clone()))
        });
        let response = self.client.put_opts(&self.path(key), bytes.clone().into(), options).await;
        if let Some((attempt, publication)) = publication {
            if matches!(&response, Ok(_) | Err(object_store::Error::AlreadyExists { .. }
                | object_store::Error::Precondition { .. } | object_store::Error::PermissionDenied { .. }))
            {
                publication.confirm();
            } else if matches!(self.publication_confirmed(key, attempt).await, Ok(true)) {
                publication.confirm();
                return Ok(());
            }
        }
        match response {
            Ok(_) => Ok(()),
            Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }) => {
                if self.get(key).await? == bytes { Ok(()) } else { Err(Error::ConflictingObject(key.to_string())) }
            }
            Err(error) => Err(provider_error(key, error)),
        }
    }

    async fn publication_confirmed(&self, key: &ObjectKey, attempt: uuid::Uuid) -> Result<bool> {
        match self.client.get_opts(&self.path(key), GetOptions::new().with_head(true)).await {
            Ok(object) => {
                let expected = attempt.to_string();
                Ok(object.attributes.get(&publication_attribute())
                    .is_some_and(|receipt| receipt.as_ref() == expected.as_str()))
            }
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(provider_error(key, error)),
        }
    }

    async fn put_opened_file(&self, key: &ObjectKey, mut input: tokio::fs::File, source: &std::path::Path) -> Result<()> {
        let length = input.metadata().await.map_err(|error| Error::io(source, error))?.len();
        match self.head(key).await {
            Ok(_) => return self.compare_file(key, &mut input, source, length).await,
            Err(Error::MissingObject(_)) => (),
            Err(error) => return Err(error),
        }
        if length == 0 { return self.put(key, Bytes::new(), None).await; }
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
                return Err(Error::MultipartAbort { upload: Box::new(error), abort: Box::new(provider_error(key, cleanup)) });
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

    async fn list(&self, directory: Option<&ObjectKey>) -> Result<Vec<StoredObject>> {
        let path = directory.map(|key| self.path(key)).or_else(|| self.prefix.as_ref().map(|key| Path::from(key.as_str())));
        let mut stream = self.client.list(path.as_ref());
        let mut result = Vec::new();
        while let Some(metadata) = stream.try_next().await.map_err(|error| {
            let key = directory.map(ToString::to_string).unwrap_or_default();
            match error {
                error @ object_store::Error::PermissionDenied { .. } => Error::PermissionDenied { key, message: error.to_string() },
                error => Error::Remote { key, message: error.to_string() },
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_permission_denial_retains_the_requested_object_identity() {
        let key = ObjectKey::new("tables/foo/meta/l01-rc-b00.arrow").unwrap();
        let error = object_store::Error::PermissionDenied {
            path: "configured-prefix/tables/foo/meta/l01-rc-b00.arrow".into(),
            source: Box::new(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "injected provider denial")),
        };
        let error = provider_error(&key, error);
        assert!(matches!(&error, Error::PermissionDenied { key: actual, .. } if actual == key.as_str()));
        let anomaly = rtdb_types::Anomaly::from(error);
        assert_eq!(anomaly.category, rtdb_types::AnomalyCategory::Forbidden);
        assert_eq!(anomaly.code, "rtdb.storage/permission-denied");
        assert_eq!(anomaly.context["key"], key.as_str());
        assert!(anomaly.message.as_deref().is_some_and(|message| message.contains("injected provider denial")));
    }

    #[test]
    fn permission_words_in_generic_provider_errors_are_not_denials() {
        let key = ObjectKey::new("object").unwrap();
        let error = object_store::Error::Generic {
            store: "test", source: Box::new(std::io::Error::other("403 AccessDenied PermissionDenied")),
        };
        assert!(matches!(provider_error(&key, error), Error::Remote { .. }));
    }

    #[tokio::test]
    async fn a_listed_key_is_the_key_that_was_put_whatever_the_provider_encodes() {
        let store = RemoteStore::new_in(Arc::new(object_store::memory::InMemory::new()), Some(ObjectKey::new("cell").unwrap()), crate::IoRegion::new());
        for name in ["~packs/b00.pack", "tables/public$orders/data/l00-rc-b00.arrow", "notes/50%/a b"] {
            store.put(&ObjectKey::new(name).unwrap(), bytes::Bytes::from_static(b"x")).await.unwrap();
        }
        let listed: Vec<String> = store.list(None).await.unwrap().into_iter().map(|object| object.key.as_str().to_owned()).collect();
        assert_eq!(listed, ["notes/50%/a b", "tables/public$orders/data/l00-rc-b00.arrow", "~packs/b00.pack"]);
        for name in &listed { store.get(&ObjectKey::new(name.clone()).unwrap()).await.unwrap(); }
        assert_eq!(store.list(Some(&ObjectKey::new("~packs").unwrap())).await.unwrap().len(), 1, "and a listing under one finds it");
    }
}
