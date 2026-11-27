//! Settling the release of a durable hold whose owner has let it go.

use std::{future::Future, time::Duration};

use rtdb_types::{Anomaly, AnomalyCategory};
use tokio_util::sync::CancellationToken;

/// The delays between attempts: doubling from `first`, never above `max`, and no attempt begun after `give_up_after`.
#[derive(Clone, Copy, Debug)]
pub struct Backoff {
    pub first: Duration,
    pub max: Duration,
    /// `None` retries until the release applies or its owner stops. Bound only a release whose hold something else
    /// reconciles once it is abandoned, such as a witnessed hold that reconciliation retires when its owner is gone:
    /// a bound on any other hold leaves it registered, pinning what it holds, until the owner stops.
    pub give_up_after: Option<Duration>,
}

impl Backoff {
    pub const RELEASE: Self = Self { first: Duration::from_millis(100), max: Duration::from_secs(30), give_up_after: None };
}

/// How a settled release ended.
#[derive(Debug)]
pub enum Settled {
    /// The release took effect, or found nothing left to release.
    Released,
    /// The release failed with an error that another attempt cannot clear.
    Refused(Anomaly),
    /// The owner stopped before the release took effect; the hold stays registered for reconciliation.
    Stopped,
    /// The release kept failing transiently until its bound; the hold stays registered for reconciliation.
    GaveUp(Anomaly),
}

impl Settled {
    pub fn is_released(&self) -> bool {
        matches!(self, Self::Released)
    }
}

/// Whether another attempt can succeed where this one failed.
pub fn transient(error: &Anomaly) -> bool {
    matches!(error.category,
        AnomalyCategory::Conflict | AnomalyCategory::Busy | AnomalyCategory::Unavailable | AnomalyCategory::Interrupted)
}

/// Repeats `release` until it succeeds, fails with an error that is not [`transient`], `stop` is cancelled, or the
/// backoff's bound passes. Cancelling `stop` abandons an attempt in flight.
///
/// `release` must be idempotent: an attempt whose reply was lost, or that was abandoned, may already have taken
/// effect. `retrying` sees each transient failure, its attempt number and the delay before the next attempt.
pub async fn settle<F, Fut>(backoff: Backoff, stop: &CancellationToken, mut release: F,
    mut retrying: impl FnMut(u32, &Anomaly, Duration)) -> Settled
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), Anomaly>>,
{
    let started = tokio::time::Instant::now();
    let mut delay = backoff.first.min(backoff.max);
    let mut attempt = 1u32;
    loop {
        let outcome = tokio::select! {
            biased;
            _ = stop.cancelled() => return Settled::Stopped,
            outcome = release() => outcome,
        };
        match outcome {
            Ok(()) => return Settled::Released,
            Err(error) if !transient(&error) => return Settled::Refused(error),
            Err(error) => {
                if backoff.give_up_after.is_some_and(|bound| started.elapsed().saturating_add(delay) > bound) {
                    return Settled::GaveUp(error);
                }
                retrying(attempt, &error, delay);
                tokio::select! {
                    biased;
                    _ = stop.cancelled() => return Settled::Stopped,
                    _ = tokio::time::sleep(delay) => {}
                }
                delay = delay.saturating_mul(2).min(backoff.max);
                attempt = attempt.saturating_add(1);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    const IMMEDIATE: Backoff = Backoff { first: Duration::ZERO, max: Duration::ZERO, give_up_after: None };

    fn failure(category: AnomalyCategory) -> Anomaly {
        Anomaly::new(category, "rtdb.test/release", "release failed")
    }

    #[tokio::test]
    async fn transient_failures_are_retried_until_the_release_applies() {
        let attempts = &AtomicU32::new(0);
        let mut seen = Vec::new();
        let settled = settle(IMMEDIATE, &CancellationToken::new(), || async move {
            match attempts.fetch_add(1, Ordering::SeqCst) {
                0 => Err(failure(AnomalyCategory::Unavailable)),
                1 => Err(failure(AnomalyCategory::Conflict)),
                _ => Ok(()),
            }
        }, |attempt, _, _| seen.push(attempt)).await;
        assert!(matches!(settled, Settled::Released));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
        assert_eq!(seen, [1, 2]);
    }

    #[tokio::test]
    async fn a_permanent_failure_ends_the_release_without_retrying() {
        let attempts = &AtomicU32::new(0);
        let settled = settle(IMMEDIATE, &CancellationToken::new(), || async move {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(failure(AnomalyCategory::Incorrect))
        }, |_, _, _| panic!("a permanent failure is not retried")).await;
        assert!(matches!(settled, Settled::Refused(error) if error.category == AnomalyCategory::Incorrect));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn the_delay_doubles_up_to_its_ceiling() {
        let attempts = &AtomicU32::new(0);
        let mut delays = Vec::new();
        let backoff = Backoff { first: Duration::from_millis(1), max: Duration::from_millis(3), give_up_after: None };
        settle(backoff, &CancellationToken::new(), || async move {
            if attempts.fetch_add(1, Ordering::SeqCst) < 3 { Err(failure(AnomalyCategory::Busy)) } else { Ok(()) }
        }, |_, _, delay| delays.push(delay)).await;
        assert_eq!(delays, [Duration::from_millis(1), Duration::from_millis(2), Duration::from_millis(3)]);
    }

    #[tokio::test]
    async fn stopping_the_owner_ends_a_release_that_keeps_failing_and_abandons_one_in_flight() {
        let stop = CancellationToken::new();
        let attempts = &AtomicU32::new(0);
        let settled = settle(Backoff { first: Duration::from_secs(3600), ..Backoff::RELEASE }, &stop, || async move {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(failure(AnomalyCategory::Unavailable))
        }, |_, _, _| stop.cancel()).await;
        assert!(matches!(settled, Settled::Stopped));
        assert_eq!(attempts.load(Ordering::SeqCst), 1, "a stopped owner begins no further attempt");

        let in_flight = settle(IMMEDIATE, &stop, || std::future::pending(), |_, _, _| unreachable!()).await;
        assert!(matches!(in_flight, Settled::Stopped));
    }

    #[tokio::test]
    async fn a_bounded_release_gives_up_with_its_last_transient_failure() {
        let attempts = &AtomicU32::new(0);
        let backoff = Backoff { first: Duration::from_millis(1), max: Duration::from_millis(1), give_up_after: Some(Duration::ZERO) };
        let settled = settle(backoff, &CancellationToken::new(), || async move {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(failure(AnomalyCategory::Busy))
        }, |_, _, _| panic!("no attempt may begin after the bound")).await;
        assert!(matches!(settled, Settled::GaveUp(error) if error.category == AnomalyCategory::Busy));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
