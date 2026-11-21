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
    /// The earlier of two leases; a query over several databases holds every one of them.
    pub fn earlier(self: Arc<Self>, other: Arc<Self>) -> Arc<Self> {
        match (self.deadline, other.deadline) {
            (Some(mine), Some(theirs)) if mine <= theirs => self,
            (Some(_), Some(_)) | (Some(_), None) => other,
            (None, _) => self,
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
