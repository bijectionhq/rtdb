use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use crate::{Error, LeaseCutoff, Result};

pub use crate::lease_decision::lease_admits;

/// A reader's entitlement to begin provider requests for files its catalog references, until a cutoff.
/// Checked when each request is admitted; a request already admitted runs to completion.
/// Collection never waits for a lease: it bounds work that holds plaintext or a provider's grant, not file retention.
pub struct ReadLease {
    clocked: Option<ClockedCutoff>,
    provider: Option<Instant>,
}

/// A cutoff on a monotonic clock the caller supplies, such as the node's, so simulated time can drive it.
#[derive(Clone)]
struct ClockedCutoff {
    deadline: i64,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl ReadLease {
    /// A lease that admits requests strictly before `deadline` on `clock`'s monotonic microseconds.
    pub fn new(deadline: i64, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        Self { clocked: Some(ClockedCutoff { deadline, clock }), provider: None }
    }

    /// A provider's monotonic cutoff can shorten a serving view across clock origins.
    pub fn until(deadline: Instant) -> Self {
        Self { clocked: None, provider: Some(deadline) }
    }

    /// The provider's original monotonic cutoff, if this lease carries one.
    pub fn provider_cutoff(&self) -> Option<Instant> { self.provider }

    /// Converts both cutoffs to a local wakeup for work holding plaintext.
    pub fn local_cutoff(&self) -> Result<Instant> {
        self.check()?;
        let sampled = Instant::now();
        sampled.checked_add(self.remaining()).ok_or_else(|| Error::Closed("The read lease cutoff is unrepresentable".into()))
    }

    pub(crate) fn remaining(&self) -> Duration {
        let clocked = self.clocked.as_ref().map_or(Duration::MAX, |cutoff| {
            Duration::from_micros(u64::try_from(cutoff.deadline.saturating_sub((cutoff.clock)())).unwrap_or_default())
        });
        self.provider.map_or(clocked, |deadline| clocked.min(deadline.saturating_duration_since(Instant::now())))
    }

    /// The earlier of two leases; a query over several databases holds every one of them.
    pub fn earlier(self: Arc<Self>, other: Arc<Self>) -> Arc<Self> {
        if Arc::ptr_eq(&self, &other) {
            return self;
        }
        let clocked = match (&self.clocked, &other.clocked) {
            (Some(mine), Some(theirs)) if mine.deadline <= theirs.deadline => Some(mine.clone()),
            (_, Some(theirs)) => Some(theirs.clone()),
            (mine, None) => mine.clone(),
        };
        let provider = match (self.provider, other.provider) {
            (Some(mine), Some(theirs)) => Some(mine.min(theirs)),
            (mine, theirs) => mine.or(theirs),
        };
        Arc::new(Self { clocked, provider })
    }

    /// A request admitted in time may still find its object gone if it ran past the cutoff.
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

    /// - Errors: [`Error::ReadLeaseExpired`] at or after either cutoff, naming the one that passed.
    pub fn check(&self) -> Result<()> {
        if !self.clocked.as_ref().is_none_or(|cutoff| lease_admits((cutoff.clock)(), cutoff.deadline)) {
            return Err(Error::ReadLeaseExpired { cutoff: LeaseCutoff::Clocked });
        }
        if !self.provider.is_none_or(|deadline| Instant::now() < deadline) {
            return Err(Error::ReadLeaseExpired { cutoff: LeaseCutoff::Provider });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicI64, Ordering};
    use crate::IoRegion;

    fn lease(deadline: i64, now: &Arc<AtomicI64>) -> Arc<ReadLease> {
        let now = Arc::clone(now);
        Arc::new(ReadLease::new(deadline, Arc::new(move || now.load(Ordering::SeqCst))))
    }

    #[tokio::test]
    async fn a_request_begins_only_strictly_before_the_deadline() {
        let now = Arc::new(AtomicI64::new(9));
        let region = IoRegion::new();
        let admitted = IoRegion::leased(Some(lease(10, &now)), region.run_blocking(|| Ok(1))).await;
        assert_eq!(admitted.unwrap(), 1);
        now.store(10, Ordering::SeqCst);
        let refused = IoRegion::leased(Some(lease(10, &now)), region.run_blocking(|| Ok(1))).await;
        assert!(matches!(refused, Err(Error::ReadLeaseExpired { cutoff: LeaseCutoff::Clocked })));
    }

    #[tokio::test]
    async fn a_request_that_overruns_its_lease_reports_the_expiry_rather_than_the_missing_object() {
        let now = Arc::new(AtomicI64::new(0));
        let region = IoRegion::new();
        let overrun = { let now = Arc::clone(&now); move || { now.store(20, Ordering::SeqCst); Err::<(), _>(Error::MissingObject("trie".into())) } };
        let result = IoRegion::leased(Some(lease(10, &now)), region.run_blocking(overrun)).await;
        assert!(matches!(result, Err(Error::ReadLeaseExpired { cutoff: LeaseCutoff::Clocked })));
        now.store(0, Ordering::SeqCst);
        let within = IoRegion::leased(Some(lease(10, &now)), region.run_blocking(|| Err::<(), _>(Error::MissingObject("trie".into())))).await;
        assert!(matches!(within, Err(Error::MissingObject(_))), "a missing object within the lease is a violation, not an expiry");
    }

    #[tokio::test]
    async fn a_nested_lease_narrows_the_enclosing_one() {
        let now = Arc::new(AtomicI64::new(15));
        let region = IoRegion::new();
        let inner = lease(100, &now);
        let result = IoRegion::leased(Some(lease(10, &now)), async { IoRegion::leased(Some(inner), region.run_blocking(|| Ok(()))).await }).await;
        assert!(matches!(result, Err(Error::ReadLeaseExpired { cutoff: LeaseCutoff::Clocked })));
    }

    #[test]
    fn clocked_and_provider_cutoffs_both_survive_nested_admission() {
        let now = Arc::new(AtomicI64::new(8_000_000_000));
        let clocked = lease(8_000_000_010, &now);
        let provider = Arc::new(ReadLease::until(Instant::now() + Duration::from_secs(60)));
        let combined = provider.earlier(clocked.clone());
        assert!(combined.provider_cutoff().is_some());
        assert!(combined.remaining() <= Duration::from_micros(10));
        now.store(8_000_000_011, Ordering::SeqCst);
        assert!(matches!(combined.check(), Err(Error::ReadLeaseExpired { cutoff: LeaseCutoff::Clocked })));
        now.store(8_000_000_000, Ordering::SeqCst);
        let expired_provider = Arc::new(ReadLease::until(Instant::now()));
        let combined = clocked.earlier(expired_provider);
        assert!(combined.remaining().is_zero());
        assert!(matches!(combined.check(), Err(Error::ReadLeaseExpired { cutoff: LeaseCutoff::Provider })), "an expired provider lease raises the typed expiry");
    }

    #[test]
    fn an_expired_clocked_lease_reports_the_read_expiry_kind() {
        let anomaly = rtdb_types::Anomaly::from(Error::ReadLeaseExpired { cutoff: LeaseCutoff::Clocked });
        assert_eq!(anomaly.kind, Some(rtdb_types::AnomalyKind::ReadExpired));
        assert_eq!(anomaly.code, "rtdb.storage/read-lease-expired");
        // A provider withdrawing its serving view closes the read, which a placed session replays elsewhere.
        let anomaly = rtdb_types::Anomaly::from(Error::ReadLeaseExpired { cutoff: LeaseCutoff::Provider });
        assert_eq!(anomaly.code, "rtdb.storage/closed");
    }
}
