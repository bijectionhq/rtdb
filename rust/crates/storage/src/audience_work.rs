//! Nonrefundable work allowances for authenticated audience reads.

use std::future::Future;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::{Error, Result};

pub const PROVIDER_METADATA_BYTES: usize = 16 << 10;

#[derive(Clone, Copy)]
pub struct Limits {
    pub provider_requests: usize,
    pub provider_bytes: usize,
    pub provider_metadata_bytes: usize,
    pub scan_items: usize,
    pub decode_bytes: usize,
}

type Allowance = Arc<Mutex<Option<Limits>>>;

#[derive(Clone)]
pub struct TurnAllowance {
    remaining: Allowance,
    actions: Arc<Mutex<usize>>,
}

impl TurnAllowance {
    pub fn new(limits: Limits, actions: usize) -> Self {
        Self { remaining: Arc::new(Mutex::new(Some(limits))), actions: Arc::new(Mutex::new(actions)) }
    }

    pub fn admit_action(&self) -> Result<()> {
        let mut actions = self.actions.lock();
        *actions = actions.checked_sub(1).ok_or(Error::AudienceWorkLimit)?;
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct Budget {
    action: Allowance,
    turn: Option<TurnAllowance>,
}

tokio::task_local! {
    static CURRENT: Budget;
}

pub async fn within<T>(limits: Option<Limits>, work: impl Future<Output = T>) -> T {
    within_turn(limits, None, work).await
}

pub async fn within_turn<T>(limits: Option<Limits>, turn: Option<TurnAllowance>, work: impl Future<Output = T>) -> T {
    let budget = current().or_else(|| limits.map(|limits| Budget {
        action: Arc::new(Mutex::new(Some(limits))), turn,
    }));
    inherit(budget, work).await
}

pub fn current_turn() -> Option<TurnAllowance> {
    CURRENT.try_with(|budget| budget.turn.clone()).ok().flatten()
}

pub fn is_bounded() -> bool { CURRENT.try_with(|_| ()).is_ok() }

pub(crate) fn current() -> Option<Budget> { CURRENT.try_with(Arc::clone).ok() }

pub(crate) async fn inherit<T>(budget: Option<Budget>, work: impl Future<Output = T>) -> T {
    match budget {
        Some(budget) => CURRENT.scope(budget, work).await,
        None => work.await,
    }
}

fn charge(update: impl Fn(&mut Limits) -> Option<()>) -> Result<()> {
    CURRENT.try_with(|budget| {
        let debit = |allowance: &Allowance| {
            let mut available = allowance.lock();
            if available.as_mut().and_then(&update).is_none() {
                *available = None;
                return Err(Error::AudienceWorkLimit);
            }
            Ok(())
        };
        debit(&budget.action)?;
        if let Some(turn) = &budget.turn {
            debit(&turn.remaining)?;
        }
        Ok(())
    }).unwrap_or(Ok(()))
}

pub(crate) fn provider_read(bytes: usize) -> Result<()> {
    charge(|remaining| {
        remaining.provider_requests = remaining.provider_requests.checked_sub(1)?;
        remaining.provider_bytes = remaining.provider_bytes.checked_sub(bytes)?;
        remaining.provider_metadata_bytes = remaining.provider_metadata_bytes.checked_sub(PROVIDER_METADATA_BYTES)?;
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
        let limits = Limits { provider_requests: 2, provider_bytes: 8, provider_metadata_bytes: 2 * PROVIDER_METADATA_BYTES, scan_items: 4, decode_bytes: 16 };
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

    #[tokio::test]
    async fn distinct_actions_spend_one_turn_allowance() {
        let limits = Limits { provider_requests: 2, provider_bytes: 8, provider_metadata_bytes: 2 * PROVIDER_METADATA_BYTES, scan_items: 4, decode_bytes: 16 };
        let turn = TurnAllowance::new(limits, 3);
        let region = IoRegion::new();
        for bytes in [5, 3] {
            turn.admit_action().unwrap();
            within_turn(Some(limits), Some(turn.clone()), async {
                region.run_async(async move { provider_read(bytes) }).await.unwrap();
            }).await;
        }
        turn.admit_action().unwrap();
        within_turn(Some(limits), Some(turn.clone()), async {
            assert!(matches!(provider_read(1), Err(Error::AudienceWorkLimit)));
        }).await;
        assert!(matches!(turn.admit_action(), Err(Error::AudienceWorkLimit)));
    }
}
