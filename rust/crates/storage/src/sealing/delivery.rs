//! Exact unit grants for plaintext retained by an execution between reads.

use std::{
    collections::BTreeMap,
    future::{Future, poll_fn},
    sync::Arc,
    task::Poll,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use rtdb_columnar::{MemoryBudget, MemoryReservation};

use super::{ObjectCipher, UnitOperation};
use crate::{Error, HeldView, IoRegion, ObjectKey, ReadLease, Result};

/// Sample before consulting the key owner. An issuer cannot extend this window
/// by delaying its reply or by reusing a successful earlier read.
const GRANT_WINDOW: Duration = Duration::from_secs(30);

/// A key owner waits this long after confirming its admission fence before
/// reporting settlement. The margin covers rate differences between monotonic
/// clocks at the reader and the owner. The wait starts again on an uncertain retry.
pub async fn drain_unit_delivery() {
    tokio::time::sleep(GRANT_WINDOW * 2).await;
}

tokio::task_local! {
    static PLAINTEXT_VIEWS: Arc<[Arc<PlaintextView>]>;
}

struct Unit {
    cipher: Arc<dyn ObjectCipher>,
    name: ObjectKey,
    expires: Mutex<Option<Instant>>,
    read_lease: Mutex<Option<Arc<ReadLease>>>,
    _allocation: Arc<MemoryReservation>,
}

/// A conservative inventory of the exact keys that could contribute to this
/// execution's answer. Buffered operator state can outlive any single pull.
pub struct PlaintextView {
    memory: Arc<MemoryBudget>,
    units: Mutex<BTreeMap<(usize, ObjectKey), Arc<Unit>>>,
}

impl PlaintextView {
    pub fn new(memory: Arc<MemoryBudget>) -> Arc<Self> {
        Arc::new(Self {
            memory,
            units: Mutex::new(BTreeMap::new()),
        })
    }

    pub async fn reading<F: Future>(self: &Arc<Self>, work: F) -> F::Output {
        let mut views = PLAINTEXT_VIEWS
            .try_with(|views| views.to_vec())
            .unwrap_or_default();
        if views.iter().any(|view| Arc::ptr_eq(self, view)) {
            return work.await;
        }
        views.push(Arc::clone(self));
        PLAINTEXT_VIEWS.scope(views.into(), work).await
    }

    pub(crate) fn inherit<F: Future>(work: F) -> impl Future<Output = F::Output> {
        let views = PLAINTEXT_VIEWS.try_with(Arc::clone).ok();
        async move {
            match views {
                Some(views) => PLAINTEXT_VIEWS.scope(views, work).await,
                None => work.await,
            }
        }
    }

    pub fn track(cipher: &Arc<dyn ObjectCipher>, name: &ObjectKey) -> Result<()> {
        Self::track_at(cipher, name, IoRegion::current_read_lease())
    }

    fn track_at(cipher: &Arc<dyn ObjectCipher>, name: &ObjectKey, lease: Option<Arc<ReadLease>>) -> Result<()> {
        let tracked = PLAINTEXT_VIEWS.try_with(|views| {
            for view in views.iter() {
                let key = (Arc::as_ptr(cipher) as *const () as usize, name.clone());
                let mut units = view.units.lock();
                if let Some(unit) = units.get(&key) {
                    if let Some(lease) = &lease {
                        let mut original = unit.read_lease.lock();
                        *original = Some(original.take().map_or_else(|| lease.clone(), |prior| prior.earlier(lease.clone())));
                    }
                } else {
                    let bytes = name.as_str().len().saturating_mul(2).saturating_add(512);
                    let allocation =
                        view.memory.reserve(bytes).map_err(|_| Error::MemoryLimit {
                            requested: bytes,
                            limit: view.memory.limit_bytes(),
                            retained: view.memory.used_bytes(),
                        })?;
                    units.insert(key, Arc::new(Unit {
                        cipher: Arc::clone(cipher),
                        name: name.clone(),
                        expires: Mutex::new(None),
                        read_lease: Mutex::new(lease.clone()),
                        _allocation: allocation,
                    }));
                }
            }
            Ok(())
        });
        tracked.unwrap_or(Ok(()))
    }

    pub async fn grant(&self) -> rtdb_types::Result<PlaintextDelivery> {
        let (allocation, units) = {
            let units = self.units.lock();
            let element = std::mem::size_of::<Arc<Unit>>() + std::mem::size_of::<Arc<dyn Send + Sync>>();
            let bytes = units.len().saturating_mul(element);
            (
                self.memory.reserve(bytes)?,
                units.values().cloned().collect::<Vec<_>>(),
            )
        };
        let mut deadline = None;
        let mut custody = Vec::new();
        for unit in units {
            let now = Instant::now();
            let mut cutoff = now + GRANT_WINDOW;
            let read_lease = unit.read_lease.lock().clone();
            if let Some(lease) = &read_lease {
                lease.check()?;
                if let Some(read_cutoff) = now.checked_add(lease.remaining()) {
                    cutoff = cutoff.min(read_cutoff);
                }
            }
            if let Some(provider) = unit.cipher.delivery_deadline() {
                cutoff = cutoff.min(provider);
            }
            if cutoff <= now {
                return Err(expired());
            }
            let cached = *unit.expires.lock();
            let expires = match cached.filter(|expires| *expires > now + Duration::from_secs(1)) {
                Some(expires) => expires.min(cutoff),
                None => {
                    let check = IoRegion::leased(
                        read_lease,
                        unit.cipher.operate(&unit.name, UnitOperation::Grant),
                    );
                    let checked = tokio::time::timeout_at(cutoff.into(), check)
                        .await
                        .map_err(|_| expired())??;
                    let (until, held) = checked.granted()?;
                    let cutoff = until.map_or(cutoff, |until| until.min(cutoff));
                    if let Some(held) = held {
                        custody.push(held);
                    } else {
                        *unit.expires.lock() = Some(cutoff);
                    }
                    cutoff
                }
            };
            deadline = Some(deadline.map_or(expires, |prior: Instant| prior.min(expires)));
        }
        let delivery = PlaintextDelivery {
            deadline,
            view: None,
            _custody: custody,
            _allocation: allocation,
        };
        delivery.check()?;
        Ok(delivery)
    }
}

/// The caller's delivery scopes travel with an asynchronous read request.
pub struct PlaintextScopes(Arc<[Arc<PlaintextView>]>);

impl PlaintextScopes {
    pub fn capture() -> Self {
        Self(PLAINTEXT_VIEWS.try_with(Arc::clone).unwrap_or_default())
    }

    pub async fn reading<F: Future>(self, work: F) -> F::Output {
        PLAINTEXT_VIEWS.scope(self.0, work).await
    }
}

/// The original object keys needed by a replayed in-memory snapshot.
pub struct PlaintextDependencies {
    units: Vec<Arc<Unit>>,
    _allocation: Arc<MemoryReservation>,
}

impl PlaintextDependencies {
    pub fn capture(memory: &Arc<MemoryBudget>) -> rtdb_types::Result<Self> {
        let view = PLAINTEXT_VIEWS.try_with(|views| views.last().cloned()).ok().flatten();
        let units = view.as_ref().map(|view| view.units.lock());
        let bytes = units.as_ref().map_or(0, |units| units.len()).saturating_mul(std::mem::size_of::<Arc<Unit>>());
        let allocation = memory.reserve(bytes)?;
        let units = units.map_or_else(Vec::new, |units| units.values().cloned().collect());
        Ok(Self { units, _allocation: allocation })
    }

    pub fn admit(&self) -> Result<()> {
        for unit in &self.units {
            let lease = unit.read_lease.lock().clone();
            PlaintextView::track_at(&unit.cipher, &unit.name, lease)?;
        }
        Ok(())
    }
}

fn expired() -> rtdb_types::Anomaly {
    rtdb_types::Anomaly::new(
        rtdb_types::AnomalyCategory::Unavailable,
        "rtdb.storage/plaintext-delivery-expired",
        "The live unit-key grant expired during answer delivery",
    )
}

pub struct PlaintextDelivery {
    deadline: Option<Instant>,
    view: Option<(Arc<HeldView>, Option<Arc<ReadLease>>)>,
    _custody: Vec<Arc<dyn Send + Sync>>,
    _allocation: Arc<MemoryReservation>,
}

impl PlaintextDelivery {
    pub fn holding(mut self, view: Arc<HeldView>, lease: Option<Arc<ReadLease>>) -> Self {
        self.view = Some((view, lease));
        self
    }

    pub fn is_bounded(&self) -> bool {
        self.deadline.is_some() || self.view.as_ref().is_some_and(|(_, lease)| lease.is_some())
    }

    pub fn check(&self) -> rtdb_types::Result<()> {
        if let Some((_, Some(lease))) = &self.view {
            lease.check()?;
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(expired());
        }
        Ok(())
    }

    /// Keeps a protocol message inside the earliest grant and wakes a
    /// backpressured write at expiry. Callers close a partially written frame.
    pub async fn deliver<T>(
        &self,
        work: impl Future<Output = rtdb_types::Result<T>>,
    ) -> rtdb_types::Result<T> {
        self.check()?;
        let lease = self
            .view
            .as_ref()
            .and_then(|(_, lease)| lease.as_ref())
            .and_then(|lease| Instant::now().checked_add(lease.remaining()));
        let deadline = match (self.deadline, lease) {
            (Some(grant), Some(lease)) => Some(grant.min(lease)),
            (grant, lease) => grant.or(lease),
        };
        let Some(deadline) = deadline else {
            return work.await;
        };
        let mut work = std::pin::pin!(work);
        let result = tokio::time::timeout_at(
            deadline.into(),
            poll_fn(|cx| {
                if let Err(error) = self.check() {
                    return Poll::Ready(Err(error));
                }
                work.as_mut().poll(cx)
            }),
        )
        .await
        .map_err(|_| expired())??;
        self.check()?;
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicI64, Ordering};

    use super::*;
    use crate::sealing::UnitOutput;

    struct AvailableKey;

    #[async_trait::async_trait]
    impl ObjectCipher for AvailableKey {
        async fn operate(&self, _: &ObjectKey, _: UnitOperation) -> Result<UnitOutput> {
            Ok(UnitOutput::Granted)
        }
    }

    #[tokio::test]
    async fn detached_reply_spends_dependencies_of_the_replayed_snapshot() {
        let memory = MemoryBudget::new(64 << 10);
        let original = PlaintextView::new(memory.clone());
        let key: Arc<dyn ObjectCipher> = Arc::new(AvailableKey);
        let name = ObjectKey::new("tables/data").unwrap();
        let dependencies = original.reading(async {
            PlaintextView::track(&key, &name).unwrap();
            PlaintextDependencies::capture(&memory).unwrap()
        }).await;

        let reply = PlaintextView::new(memory.clone());
        reply.reading(async {
            let caller = PlaintextScopes::capture();
            tokio::spawn(caller.reading(async move { dependencies.admit().unwrap() })).await.unwrap();
        }).await;
        assert!(reply.grant().await.unwrap().is_bounded());
    }

    #[tokio::test]
    async fn materialized_deployment_plaintext_requires_its_original_key() {
        use std::sync::atomic::AtomicBool;
        use bytes::Bytes;
        use crate::{MemoryStore, ObjectStore, sealing::EncryptingStore};

        struct DeploymentKey(AtomicBool);

        #[async_trait::async_trait]
        impl ObjectCipher for DeploymentKey {
            async fn operate(&self, _: &ObjectKey, _: UnitOperation) -> Result<UnitOutput> {
                if self.0.load(Ordering::Relaxed) { Ok(UnitOutput::Granted) }
                else { Err(Error::ErasedUnderView) }
            }
        }

        let key = Arc::new(DeploymentKey(AtomicBool::new(true)));
        let store = EncryptingStore::with_deployment_delivery(
            Arc::new(MemoryStore::default()), &[8; 32], key.clone(),
        ).unwrap();
        let name = ObjectKey::new("tables/data").unwrap();
        let original = Bytes::from_static(b"cached plaintext");
        store.put(&name, original.clone()).await.unwrap();
        let view = PlaintextView::new(MemoryBudget::new(64 << 10));
        let plaintext = view.reading(store.get(&name)).await.unwrap();
        assert_eq!(plaintext, original);

        key.0.store(false, Ordering::Relaxed);
        assert_eq!(view.grant().await.err().unwrap().code, "rtdb/erased-under-view");
    }

    #[tokio::test]
    async fn buffered_plaintext_cannot_renew_past_its_read_lease() {
        let memory = MemoryBudget::new(64 << 10);
        let view = PlaintextView::new(memory);
        let key: Arc<dyn ObjectCipher> = Arc::new(AvailableKey);
        let name = ObjectKey::new("table/data").unwrap();
        let now = Arc::new(AtomicI64::new(0));
        let clock = {
            let now = Arc::clone(&now);
            Arc::new(move || now.load(Ordering::Relaxed))
        };
        let lease = Arc::new(ReadLease::new(Some(5_000_000), clock));
        IoRegion::leased(
            Some(lease),
            view.reading(async {
                PlaintextView::track(&key, &name).unwrap();
            }),
        )
        .await;
        assert!(view.grant().await.unwrap().is_bounded());

        now.store(5_000_000, Ordering::Relaxed);
        assert!(view.grant().await.is_err());
    }
}
