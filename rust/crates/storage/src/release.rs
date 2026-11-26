//! Settling the release of a durable hold whose owner has let it go.

use std::{future::Future, time::Duration};

use rtdb_types::{Anomaly, AnomalyCategory};

/// The delays between attempts: doubling from `first`, never above `max`.
#[derive(Clone, Copy, Debug)]
pub struct Backoff {
    pub first: Duration,
    pub max: Duration,
}

impl Backoff {
    pub const RELEASE: Self = Self { first: Duration::from_millis(100), max: Duration::from_secs(30) };
}

/// How a settled release ended.
#[derive(Debug)]
pub enum Settled {
    /// The release took effect, or found nothing left to release.
    Released,
    /// The release failed with an error that another attempt cannot clear.
    Refused(Anomaly),
}

/// Whether another attempt can succeed where this one failed.
pub fn transient(error: &Anomaly) -> bool {
    matches!(error.category,
        AnomalyCategory::Conflict | AnomalyCategory::Busy | AnomalyCategory::Unavailable | AnomalyCategory::Interrupted)
}

/// Repeats `release` until it succeeds or fails with an error that is not [`transient`].
///
/// `release` must be idempotent: an attempt whose reply was lost may already
/// have taken effect. `retrying` sees each transient failure, its attempt
/// number and the delay before the next attempt.
pub async fn settle<F, Fut>(backoff: Backoff, mut release: F, mut retrying: impl FnMut(u32, &Anomaly, Duration)) -> Settled
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), Anomaly>>,
{
    let mut delay = backoff.first.min(backoff.max);
    let mut attempt = 1u32;
    loop {
        match release().await {
            Ok(()) => return Settled::Released,
            Err(error) if !transient(&error) => return Settled::Refused(error),
            Err(error) => {
                retrying(attempt, &error, delay);
                tokio::time::sleep(delay).await;
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

    const IMMEDIATE: Backoff = Backoff { first: Duration::ZERO, max: Duration::ZERO };

    fn failure(category: AnomalyCategory) -> Anomaly {
        Anomaly::new(category, "rtdb.test/release", "release failed")
    }

    #[tokio::test]
    async fn transient_failures_are_retried_until_the_release_applies() {
        let attempts = &AtomicU32::new(0);
        let mut seen = Vec::new();
        let settled = settle(IMMEDIATE, || async move {
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
        let settled = settle(IMMEDIATE, || async move {
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
        let backoff = Backoff { first: Duration::from_millis(1), max: Duration::from_millis(3) };
        settle(backoff, || async move {
            if attempts.fetch_add(1, Ordering::SeqCst) < 3 { Err(failure(AnomalyCategory::Busy)) } else { Ok(()) }
        }, |_, _, delay| delays.push(delay)).await;
        assert_eq!(delays, [Duration::from_millis(1), Duration::from_millis(2), Duration::from_millis(3)]);
    }
}
