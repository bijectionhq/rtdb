use std::{sync::Arc, time::{Duration, Instant}};

use rtdb_directory::Directory;
use rtdb_node::{Clock, DecisionClock, DecisionObservation, SystemClock};
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// A successor publisher can install a sample only once its predecessor's has expired, so a reader waits out one
/// directory lease (3 s), a publish interval and a publish write before reporting the clock unavailable.
const PUBLISHER_TAKEOVER: Duration = Duration::from_millis(4_500);

/// The publisher's elapsed clock is anchored once to the cell's system time scale.
/// Each live read confirms the persisted authority and expiry with a strong point read.
pub(crate) struct ReceiverClock {
    directory: Arc<dyn Directory>,
    domain: Uuid,
    publisher: Uuid,
    anchor: Timestamp,
    start: Instant,
}

impl ReceiverClock {
    pub(crate) fn new(directory: Arc<dyn Directory>, domain: Uuid) -> Arc<Self> {
        Arc::new(Self { directory, domain, publisher: Uuid::new_v4(), anchor: SystemClock.now(), start: Instant::now() })
    }

    fn elapsed(&self) -> Result<Timestamp> {
        i64::try_from(self.start.elapsed().as_micros()).ok()
            .and_then(|micros| self.anchor.0.checked_add(micros))
            .map(Timestamp).ok_or_else(unavailable)
    }

    pub(crate) async fn publish(self: Arc<Self>, cancellation: CancellationToken) {
        let mut ticks = tokio::time::interval(Duration::from_millis(250));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancellation.cancelled() => break,
                _ = ticks.tick() => {}
            }
            let Ok(observed) = self.elapsed() else { break; };
            let write = self.directory.publish_receiver_clock(self.domain, self.publisher, observed);
            tokio::select! {
                _ = cancellation.cancelled() => break,
                _ = tokio::time::timeout(Duration::from_secs(1), write) => {}
            }
        }
    }
}

#[async_trait::async_trait]
impl DecisionClock for ReceiverClock {
    async fn observe(&self) -> Result<DecisionObservation> {
        let deadline = tokio::time::Instant::now() + PUBLISHER_TAKEOVER;
        loop {
            let sample = tokio::time::timeout(Duration::from_secs(1), self.directory.receiver_clock(self.domain))
                .await.map_err(|_| unavailable())??;
            if let Some(sample) = sample && sample.admits(self.domain, self.elapsed()?) {
                return Ok(DecisionObservation { domain: *sample.domain.as_bytes(), epoch: sample.epoch, sequence: sample.sequence, instant: sample.instant });
            }
            if tokio::time::Instant::now() >= deadline { return Err(unavailable()); }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

fn unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.clock/unavailable", "No fresh receiver clock sample is installed")
}
