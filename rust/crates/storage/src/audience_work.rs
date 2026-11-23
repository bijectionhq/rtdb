//! Nonrefundable work allowances for one authenticated audience action.

use std::future::Future;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::{Error, Result};

#[derive(Clone, Copy)]
pub struct Limits {
    pub provider_requests: usize,
    pub provider_bytes: usize,
    pub scan_items: usize,
    pub decode_bytes: usize,
}

pub(crate) type Budget = Arc<Mutex<Option<Limits>>>;

tokio::task_local! {
    static CURRENT: Budget;
}

pub async fn within<T>(limits: Option<Limits>, work: impl Future<Output = T>) -> T {
    let budget = current().or_else(|| limits.map(|limits| Arc::new(Mutex::new(Some(limits)))));
    inherit(budget, work).await
}

pub fn is_bounded() -> bool { CURRENT.try_with(|_| ()).is_ok() }

pub(crate) fn current() -> Option<Budget> { CURRENT.try_with(Arc::clone).ok() }

pub(crate) async fn inherit<T>(budget: Option<Budget>, work: impl Future<Output = T>) -> T {
    match budget {
        Some(budget) => CURRENT.scope(budget, work).await,
        None => work.await,
    }
}

fn charge(update: impl FnOnce(&mut Limits) -> Option<()>) -> Result<()> {
    CURRENT.try_with(|budget| {
        let mut available = budget.lock();
        if available.as_mut().and_then(update).is_none() {
            *available = None;
            return Err(Error::AudienceWorkLimit);
        }
        Ok(())
    }).unwrap_or(Ok(()))
}

pub(crate) fn provider_read(bytes: usize) -> Result<()> {
    charge(|remaining| {
        remaining.provider_requests = remaining.provider_requests.checked_sub(1)?;
        remaining.provider_bytes = remaining.provider_bytes.checked_sub(bytes)?;
        Some(())
    })
}

/// Charges catalog, trie, and row traversal before doing the counted work.
pub fn scan_items(count: usize) -> Result<()> {
    charge(|remaining| {
        remaining.scan_items = remaining.scan_items.checked_sub(count)?;
        Some(())
    })
}

/// Charges the validated maximum size of decoding and inspecting input buffers.
pub fn decode_bytes(bytes: usize) -> Result<()> {
    charge(|remaining| {
        remaining.decode_bytes = remaining.decode_bytes.checked_sub(bytes)?;
        Some(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IoRegion;

    #[tokio::test]
    async fn retained_provider_work_shares_the_call_allowance_and_cannot_replenish_it() {
        let limits = Limits { provider_requests: 2, provider_bytes: 8, scan_items: 4, decode_bytes: 16 };
        let region = IoRegion::new();
        within(Some(limits), async {
            region.run_async(async { provider_read(5) }).await.unwrap();
            assert!(matches!(region.run_async(async { provider_read(4) }).await, Err(Error::AudienceWorkLimit)));
            within(Some(limits), async {
                assert!(matches!(scan_items(1), Err(Error::AudienceWorkLimit)));
            }).await;
        }).await;
        assert!(provider_read(usize::MAX).is_ok());
    }
}
