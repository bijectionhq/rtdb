use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{Error as ProviderError, ObjectStore, ObjectStoreExt, PutMode, PutOptions, path::Path};
use ring::digest::{SHA256, digest};
use uuid::Uuid;

use crate::{Error, IoRegion, ObjectKey, Result};

#[path = "read_drain_decision.rs"]
mod decision;

pub(super) struct ReadDrain<'a> {
    client: &'a dyn ObjectStore,
    key: &'a ObjectKey,
    census: Path,
}

impl<'a> ReadDrain<'a> {
    pub(super) fn new(client: &'a dyn ObjectStore, root: Path, key: &'a ObjectKey) -> Self {
        let identity = URL_SAFE_NO_PAD.encode(digest(&SHA256, key.as_str().as_bytes()));
        Self { client, key, census: root.join(identity) }
    }

    fn fence(&self) -> Path { self.census.clone().join("closed") }

    fn readers(&self) -> Path { self.census.clone().join("reads") }

    async fn create(&self, path: &Path) -> std::result::Result<(), ProviderError> {
        self.client.put_opts(path, Bytes::from_static(b"1").into(), PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        }).await.map(drop)
    }

    async fn forget(&self, reader: &Path) {
        if let Err(error) = self.client.delete(reader).await {
            tracing::warn!("Provider read census entry for {} is awaiting release: {error}", self.key);
        }
    }

    pub(super) async fn read<T>(&self, work: impl Future<Output = Result<T>>) -> Result<T> {
        let reader = self.readers().join(Uuid::new_v4().simple().to_string());
        self.create(&reader).await.map_err(|error| super::provider_error(self.key, error))?;

        // Publish the entry before checking the fence, which precedes the custodian's census.
        let open = match self.client.head(&self.fence()).await {
            Ok(_) => Err(Error::Closed(format!("Provider reads are fenced for {}", self.key))),
            Err(ProviderError::NotFound { .. }) => Ok(()),
            Err(error) => Err(super::provider_error(self.key, error)),
        };
        if let Err(error) = open.and_then(|()| IoRegion::current_read_lease().map_or(Ok(()), |lease| lease.check())) {
            self.forget(&reader).await;
            return Err(error);
        }

        let result = work.await;
        // An interrupted body or ambiguous transport failure leaves the entry protected.
        if matches!(&result, Ok(_) | Err(Error::MissingObject(_))) {
            self.forget(&reader).await;
        }
        result
    }

    pub(super) async fn close_and_observe(&self) -> Result<bool> {
        let fence = self.fence();
        match self.create(&fence).await {
            Ok(()) => (),
            Err(ProviderError::AlreadyExists { .. } | ProviderError::Precondition { .. }) => {
                self.client.head(&fence).await.map_err(|error| super::provider_error(self.key, error))?;
            }
            Err(error) => return Err(super::provider_error(self.key, error)),
        }
        let readers = self.readers();
        let mut registrations = self.client.list(Some(&readers));
        let mut live_reads_seen = false;
        while let Some(registration) = registrations.try_next().await
            .map_err(|error| super::provider_error(self.key, error))?
        {
            live_reads_seen = true;
            tracing::warn!(object_key = %self.key, registration = %registration.location, "Provider read registration is blocking installed reclamation");
        }
        Ok(decision::provider_drain_completed(true, live_reads_seen))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::{AtomicI64, Ordering}};

    use object_store::memory::InMemory;
    use tokio::sync::oneshot;

    use super::*;
    use crate::{ObjectStore as _, ReadLease, remote::RemoteStore};

    fn store(provider: Arc<InMemory>) -> RemoteStore {
        RemoteStore::new_single_put(provider, ObjectKey::new("cell").unwrap(), IoRegion::new())
            .with_read_drain(|_| Some("grant"))
    }

    #[tokio::test]
    async fn r7_expired_cancelled_read_delays_a_different_stores_effect_until_completion() {
        let provider = Arc::new(InMemory::new());
        let worker = store(provider.clone());
        let custodian = store(provider);
        let target = ObjectKey::new("grant/target").unwrap();
        let other = ObjectKey::new("grant/other").unwrap();
        worker.put(&target, Bytes::from_static(b"target")).await.unwrap();
        worker.put(&other, Bytes::from_static(b"other")).await.unwrap();

        let clock = Arc::new(AtomicI64::new(9));
        let lease = Arc::new(ReadLease::new(10, Arc::new({
            let clock = clock.clone();
            move || clock.load(Ordering::SeqCst)
        })));
        let (started, running) = oneshot::channel();
        let (complete, finish) = oneshot::channel();
        let read = tokio::spawn(IoRegion::leased(Some(lease.clone()), {
            let worker = worker.clone();
            let target = target.clone();
            async move {
                worker.io.read_async(worker.objects.clone(), target, |objects, key| async move {
                    objects.with_read(&key, async move {
                        started.send(()).unwrap();
                        finish.await.unwrap();
                        Ok(())
                    }).await
                }).await
            }
        }));
        running.await.unwrap();
        clock.store(11, Ordering::SeqCst);
        assert!(lease.check().is_err());
        read.abort();
        assert!(read.await.unwrap_err().is_cancelled());
        assert!(!custodian.close_read_admission(&target).await.unwrap());
        assert!(matches!(worker.get(&target).await, Err(Error::Closed(_))));
        assert_eq!(worker.get(&other).await.unwrap(), Bytes::from_static(b"other"));

        complete.send(()).unwrap();
        worker.close_and_wait().await;
        assert!(custodian.close_read_admission(&target).await.unwrap());
    }

    #[tokio::test]
    async fn object_reclamation_waits_for_the_original_provider_read() {
        let provider = Arc::new(InMemory::new());
        let worker = Arc::new(RemoteStore::new(provider.clone(), None).with_read_drain(|_| Some("grant")));
        let collector = RemoteStore::new(provider.clone(), None).with_read_drain(|_| Some("grant"));
        let target = ObjectKey::new("grant/file").unwrap();
        collector.put(&target, Bytes::from_static(b"image")).await.unwrap();

        let (started, running) = oneshot::channel();
        let (complete, wait) = oneshot::channel();
        let read = tokio::spawn({
            let target = target.clone();
            async move {
                worker.objects.with_read(&target, async {
                    started.send(()).unwrap();
                    wait.await.unwrap();
                    Ok(())
                }).await
            }
        });
        running.await.unwrap();
        assert!(!collector.reclaim_unreachable_object(&target).await.unwrap());
        assert!(provider.head(&Path::from(target.as_str())).await.is_ok());
        complete.send(()).unwrap();
        read.await.unwrap().unwrap();
        assert!(collector.reclaim_unreachable_object(&target).await.unwrap());
        assert!(provider.head(&Path::from(target.as_str())).await.is_err());
    }

    #[tokio::test]
    async fn r7_ambiguous_provider_completion_remains_in_the_census() {
        let provider = Arc::new(InMemory::new());
        let worker = store(provider.clone());
        let custodian = store(provider.clone());
        let target = ObjectKey::new("grant/target").unwrap();
        let _: Result<()> = worker.objects.with_read(&target, async {
            Err(Error::Remote { key: target.to_string(), message: "Lost provider outcome".into() })
        }).await;
        let drain = worker.objects.read_drain(&target).unwrap();
        let registrations = provider.list(Some(&drain.readers())).try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(registrations.len(), 1);
        let registration = &registrations[0].location;
        for _ in 0..2 {
            assert!(!custodian.close_read_admission(&target).await.unwrap());
            assert_eq!(provider.head(registration).await.unwrap().location, *registration);
        }
    }
}
