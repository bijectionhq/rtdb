use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{Error, Result};

pub use crate::lease_decision::{collection_waited, lease_admits, lease_deadline};

/// A reader's entitlement to begin provider requests for files its catalog references.
/// Checked when each request is admitted; a request already admitted runs to completion.
pub struct ReadLease {
    catalog: Option<CatalogLease>,
    provider: Option<Instant>,
}

#[derive(Clone)]
struct CatalogLease {
    deadline: Option<i64>,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl ReadLease {
    /// A lease without a deadline admits nothing: its catalog was never confirmed current.
    pub fn new(deadline: Option<i64>, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        Self {
            catalog: Some(CatalogLease { deadline, clock }),
            provider: None,
        }
    }

    /// A provider's monotonic cutoff can shorten a serving view across clock origins.
    pub fn until(deadline: Instant) -> Self {
        Self {
            catalog: None,
            provider: Some(deadline),
        }
    }

    /// The provider's original monotonic cutoff, if this lease carries one.
    pub fn provider_cutoff(&self) -> Option<Instant> { self.provider }

    /// The catalog's cutoff in the node's clock coordinates.
    pub fn deadline(&self) -> Option<i64> {
        self.catalog.as_ref().and_then(|catalog| catalog.deadline)
    }

    /// Converts both cutoffs to a local wakeup for work holding plaintext.
    pub fn local_cutoff(&self) -> Result<Instant> {
        self.check()?;
        let sampled = Instant::now();
        sampled.checked_add(self.remaining()).ok_or_else(|| Error::Closed("The read lease cutoff is unrepresentable".into()))
    }

    pub(crate) fn remaining(&self) -> Duration {
        let catalog = self.catalog.as_ref().map_or(Duration::MAX, |lease| {
            let remaining = lease
                .deadline
                .map_or(0, |deadline| deadline.saturating_sub((lease.clock)()));
            Duration::from_micros(u64::try_from(remaining).unwrap_or_default())
        });
        self.provider.map_or(catalog, |deadline| {
            catalog.min(deadline.saturating_duration_since(Instant::now()))
        })
    }
    /// The earlier of two leases; a query over several databases holds every one of them.
    pub fn earlier(self: Arc<Self>, other: Arc<Self>) -> Arc<Self> {
        if Arc::ptr_eq(&self, &other) {
            return self;
        }
        if self.provider.is_none() && other.provider.is_none() {
            return if self.deadline() <= other.deadline() {
                self
            } else {
                other
            };
        }
        let catalog = match (&self.catalog, &other.catalog) {
            (Some(mine), Some(theirs)) if mine.deadline <= theirs.deadline => Some(mine.clone()),
            (_, Some(theirs)) => Some(theirs.clone()),
            (mine, None) => mine.as_ref().cloned(),
        };
        let provider = match (self.provider, other.provider) {
            (Some(mine), Some(theirs)) => Some(mine.min(theirs)),
            (mine, theirs) => mine.or(theirs),
        };
        Arc::new(Self { catalog, provider })
    }

    fn check_catalog(&self) -> Result<()> {
        let Some(lease) = &self.catalog else {
            return Ok(());
        };
        let now = (lease.clock)();
        match lease.deadline {
            Some(deadline) if lease_admits(now, deadline) => Ok(()),
            deadline => Err(Error::ReadLeaseExpired { deadline, now }),
        }
    }
    /// A request admitted in time may still find its object gone if it ran past the deadline.
    /// Objects are immutable, so an overrun can only fail; it then reports the expired lease.
    pub fn classify<T>(&self, result: Result<T>) -> Result<T> {
        match result {
            Err(Error::MissingObject(key)) => match self.check() {
                Ok(()) => Err(Error::MissingObject(key)),
                Err(expired) => Err(expired),
            },
            result => result,
        }
    }
    pub fn check(&self) -> Result<()> {
        self.check_catalog()?;
        if self
            .provider
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(Error::Closed(
                "The bounded provider read lease expired".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};
    use crate::IoRegion;

    fn lease(deadline: Option<i64>, now: &Arc<AtomicI64>) -> Arc<ReadLease> {
        let now = Arc::clone(now);
        Arc::new(ReadLease::new(deadline, Arc::new(move || now.load(Ordering::SeqCst))))
    }

    #[tokio::test]
    async fn a_request_begins_only_strictly_before_the_deadline() {
        let now = Arc::new(AtomicI64::new(9));
        let region = IoRegion::new();
        let admitted = IoRegion::leased(Some(lease(Some(10), &now)), region.run_blocking(|| Ok(1))).await;
        assert_eq!(admitted.unwrap(), 1);
        now.store(10, Ordering::SeqCst);
        let refused = IoRegion::leased(Some(lease(Some(10), &now)), region.run_blocking(|| Ok(1))).await;
        assert!(matches!(refused, Err(Error::ReadLeaseExpired { deadline: Some(10), now: 10 })));
        let unconfirmed = IoRegion::leased(Some(lease(None, &now)), region.run_blocking(|| Ok(1))).await;
        assert!(matches!(unconfirmed, Err(Error::ReadLeaseExpired { deadline: None, .. })));
    }

    #[tokio::test]
    async fn a_request_that_overruns_its_lease_reports_the_expiry_rather_than_the_missing_object() {
        let now = Arc::new(AtomicI64::new(0));
        let region = IoRegion::new();
        let overrun = { let now = Arc::clone(&now); move || { now.store(20, Ordering::SeqCst); Err::<(), _>(Error::MissingObject("trie".into())) } };
        let result = IoRegion::leased(Some(lease(Some(10), &now)), region.run_blocking(overrun)).await;
        assert!(matches!(result, Err(Error::ReadLeaseExpired { deadline: Some(10), now: 20 })));
        now.store(0, Ordering::SeqCst);
        let within = IoRegion::leased(Some(lease(Some(10), &now)), region.run_blocking(|| Err::<(), _>(Error::MissingObject("trie".into())))).await;
        assert!(matches!(within, Err(Error::MissingObject(_))), "a missing object within the lease is a violation, not an expiry");
    }

    #[tokio::test]
    async fn a_nested_lease_narrows_the_enclosing_one() {
        let now = Arc::new(AtomicI64::new(15));
        let region = IoRegion::new();
        let inner = lease(Some(100), &now);
        let result = IoRegion::leased(Some(lease(Some(10), &now)), async { IoRegion::leased(Some(inner), region.run_blocking(|| Ok(()))).await }).await;
        assert!(matches!(result, Err(Error::ReadLeaseExpired { deadline: Some(10), .. })));
    }

    #[test]
    fn catalog_and_provider_cutoffs_both_survive_nested_admission() {
        let now = Arc::new(AtomicI64::new(8_000_000_000));
        let catalog = lease(Some(8_000_000_010), &now);
        let provider = Arc::new(ReadLease::until(Instant::now() + Duration::from_secs(60)));
        let combined = provider.earlier(catalog.clone());
        assert_eq!(combined.deadline(), Some(8_000_000_010));
        assert!(combined.remaining() <= Duration::from_micros(10));
        now.store(8_000_000_011, Ordering::SeqCst);
        assert!(matches!(
            combined.check(),
            Err(Error::ReadLeaseExpired { .. })
        ));
        now.store(8_000_000_000, Ordering::SeqCst);
        let expired_provider = Arc::new(ReadLease::until(Instant::now()));
        let combined = catalog.earlier(expired_provider);
        assert!(combined.remaining().is_zero());
        assert!(combined.check().is_err());
    }
}
