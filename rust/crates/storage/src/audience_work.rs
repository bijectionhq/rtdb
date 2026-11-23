//! Nonrefundable work allowances for authenticated audience reads.

use std::future::Future;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::{Error, Result};

mod decision;
#[cfg(any(feature = "remote", test))]
mod drain;

use decision::Work;
pub use decision::{Limits, PROVIDER_METADATA_BYTES};
#[cfg(any(feature = "remote", test))]
pub(crate) use drain::hold_provider_custody;

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
        *actions = decision::admit_action(*actions).ok_or(Error::AudienceWorkLimit)?;
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
    let existing = current();
    let budget = match decision::new_action(existing.is_some(), limits) {
        Some(limits) => Some(Budget { action: Arc::new(Mutex::new(Some(limits))), turn }),
        None => existing,
    };
    inherit(budget, work).await
}

pub fn current_turn() -> Option<TurnAllowance> {
    CURRENT.try_with(|budget| budget.turn.clone()).ok().flatten()
}

pub fn is_bounded() -> bool { CURRENT.try_with(|_| ()).is_ok() }

pub(crate) fn current() -> Option<Budget> { CURRENT.try_with(Budget::clone).ok() }

pub(crate) async fn inherit<T>(budget: Option<Budget>, work: impl Future<Output = T>) -> T {
    match budget {
        Some(budget) => CURRENT.scope(budget, work).await,
        None => work.await,
    }
}

fn charge(work: Work) -> Result<()> {
    CURRENT.try_with(|budget| {
        let cost = decision::cost(work);
        let debit = |allowance: &Allowance| {
            let mut available = allowance.lock();
            *available = decision::debit(*available, cost);
            available.as_ref().map(|_| ()).ok_or(Error::AudienceWorkLimit)
        };
        debit(&budget.action)?;
        if let Some(turn) = &budget.turn {
            debit(&turn.remaining)?;
        }
        Ok(())
    }).unwrap_or(Ok(()))
}

pub(crate) fn provider_read(bytes: usize) -> Result<()> {
    charge(Work::ProviderRead(bytes))
}

/// Charges catalog, trie, and row traversal before doing the counted work.
pub fn scan_items(count: usize) -> Result<()> {
    charge(Work::Scan(count))
}

/// Charges the validated maximum size of decoding and inspecting input buffers.
pub fn decode_bytes(bytes: usize) -> Result<()> {
    charge(Work::Decode(bytes))
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
