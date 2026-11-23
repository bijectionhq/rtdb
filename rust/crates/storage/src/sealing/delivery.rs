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
    read_lease: Option<Arc<ReadLease>>,
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

    pub(super) fn track(cipher: &Arc<dyn ObjectCipher>, name: &ObjectKey) -> Result<()> {
        let tracked = PLAINTEXT_VIEWS.try_with(|views| {
            for view in views.iter() {
                let key = (Arc::as_ptr(cipher) as *const () as usize, name.clone());
                let mut units = view.units.lock();
                if let std::collections::btree_map::Entry::Vacant(entry) = units.entry(key) {
                    let bytes = name.as_str().len().saturating_mul(2).saturating_add(512);
                    let allocation =
                        view.memory.reserve(bytes).map_err(|_| Error::MemoryLimit {
                            requested: bytes,
                            limit: view.memory.limit_bytes(),
                            retained: view.memory.used_bytes(),
                        })?;
                    entry.insert(Arc::new(Unit {
                        cipher: Arc::clone(cipher),
                        name: name.clone(),
                        expires: Mutex::new(None),
                        read_lease: IoRegion::current_read_lease(),
                        _allocation: allocation,
                    }));
                }
            }
            Ok(())
        });
        tracked.unwrap_or(Ok(()))
    }

    pub async fn grant(&self) -> rtdb_types::Result<PlaintextDelivery> {
        let (_allocation, units) = {
            let units = self.units.lock();
            let bytes = units.len().saturating_mul(std::mem::size_of::<Arc<Unit>>());
            (
                self.memory.reserve(bytes)?,
                units.values().cloned().collect::<Vec<_>>(),
            )
        };
        let mut deadline = None;
        for unit in units {
            let now = Instant::now();
            let mut cutoff = now + GRANT_WINDOW;
            if let Some(lease) = &unit.read_lease {
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
                        unit.read_lease.clone(),
                        unit.cipher.operate(&unit.name, UnitOperation::Grant),
                    );
                    let checked = tokio::time::timeout_at(cutoff.into(), check)
                        .await
                        .map_err(|_| expired())??;
                    checked.granted()?;
                    *unit.expires.lock() = Some(cutoff);
                    cutoff
                }
            };
            deadline = Some(deadline.map_or(expires, |prior: Instant| prior.min(expires)));
        }
        let delivery = PlaintextDelivery {
            deadline,
            view: None,
        };
        delivery.check()?;
        Ok(delivery)
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
