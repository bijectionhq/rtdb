use std::sync::Arc;

use crate::{Error, Result};

pub use crate::lease_decision::{collection_waited, lease_admits, lease_deadline};

/// A reader's entitlement to begin provider requests for files its catalog references.
/// Checked when each request is admitted; a request already admitted runs to completion.
pub struct ReadLease {
    deadline: Option<i64>,
    clock: Arc<dyn Fn() -> i64 + Send + Sync>,
}

impl ReadLease {
    /// A lease without a deadline admits nothing: its catalog was never confirmed current.
    pub fn new(deadline: Option<i64>, clock: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self { Self { deadline, clock } }
    pub fn deadline(&self) -> Option<i64> { self.deadline }
    pub(crate) fn remaining(&self) -> std::time::Duration {
        let remaining = self.deadline.map_or(0, |deadline| deadline.saturating_sub((self.clock)()));
        std::time::Duration::from_micros(u64::try_from(remaining).unwrap_or_default())
    }
    /// The earlier of two leases; a query over several databases holds every one of them.
    pub fn earlier(self: Arc<Self>, other: Arc<Self>) -> Arc<Self> {
        match (self.deadline, other.deadline) {
            (Some(mine), Some(theirs)) if mine <= theirs => self,
            (Some(_), Some(_)) | (Some(_), None) => other,
            (None, _) => self,
        }
    }
    /// A request admitted in time may still find its object gone if it ran past the deadline.
    /// Objects are immutable, so an overrun can only fail; it then reports the expired lease.
    pub fn classify<T>(&self, result: Result<T>) -> Result<T> {
        match result {
            Err(Error::MissingObject(key)) => match self.check() { Ok(()) => Err(Error::MissingObject(key)), Err(expired) => Err(expired) },
            result => result,
        }
    }
    pub fn check(&self) -> Result<()> {
        let now = (self.clock)();
        match self.deadline {
            Some(deadline) if lease_admits(now, deadline) => Ok(()),
            deadline => Err(Error::ReadLeaseExpired { deadline, now }),
        }
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
}
