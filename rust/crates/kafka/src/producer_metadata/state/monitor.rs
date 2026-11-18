use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use rdkafka::ordered_metadata::RawMetadataView;
use rtdb_types::{Anomaly, Result};
use tokio::{sync::Notify, time::timeout};

use super::{
    Budget, Failure, FatalKind, IdentityWitness, JavaBackoffObservations, JavaShuffleObservations,
    MetadataError, MetadataNetwork, MetadataUpdater, MetadataWait, MetadataWaitObservation, Outcome,
    ProducerMetadata, RequestScope, ResponseAction, Shared, Snapshot, UpdateFailure, WaitStep,
};

mod topic;
pub use topic::{MetadataClock, TopicMetadata, TopicWaitFailure, TopicWaitRequest};

/// The terminal source result of waiting for metadata.
pub enum WaitCompletion {
    /// A newer signed update version is available.
    Updated,
    /// Metadata has closed.
    Closed,
    /// The deadline elapsed, with the original timeout in milliseconds.
    TimedOut(i64),
    /// The source fatal error, retaining its original payload admission.
    Fatal(MetadataError),
}

/// Serializes producer metadata state and publishes source version, fatal and close notifications.
#[derive(Clone)]
pub struct MetadataMonitor {
    state: Shared<MonitorState>,
}

struct MonitorState {
    metadata: Mutex<ProducerMetadata>,
    changed: Notify,
}

impl MetadataMonitor {
    /// Moves the producer into a shared monitor admitted against the producer's existing budget.
    pub fn new(metadata: ProducerMetadata) -> Outcome<Self> {
        let mut budget = Budget(Arc::clone(&metadata.budget.0));
        let state = Shared::new(MonitorState {
            metadata: Mutex::new(metadata),
            changed: Notify::new(),
        }, &mut budget)?;
        Ok(Self { state })
    }

    fn lock(&self) -> Outcome<MutexGuard<'_, ProducerMetadata>> {
        self.state.metadata.lock().map_err(|_| Anomaly::fault(
            "rtdb/kafka-metadata-monitor-poisoned", "Metadata monitor mutation did not complete",
        ).into())
    }

    /// Retains the current metadata generation independently of subsequent publications.
    pub fn snapshot(&self) -> Outcome<Snapshot> {
        Ok(self.lock()?.snapshot())
    }

    fn apply_sender_change<T>(
        &self, change: impl FnOnce(&mut ProducerMetadata) -> std::result::Result<T, UpdateFailure>,
    ) -> std::result::Result<T, UpdateFailure> {
        let (result, wake) = {
            let mut metadata = self.lock()?;
            let previous = metadata.fatal.clone();
            let was_usable = !metadata.failed;
            let result = change(&mut metadata);
            let fatal_published = metadata.fatal.as_ref().is_some_and(|error| {
                previous.as_ref().is_none_or(|old| !std::ptr::eq(old.cause(), error.cause()))
            });
            (result, fatal_published || (was_usable && metadata.failed))
        };
        if wake { self.state.changed.notify_waiters(); }
        result
    }

    /// Drives the caller's serialized updater and broadcasts its request fatal publications.
    /// Timing and observation order follow [`MetadataUpdater::maybe_update`].
    /// Network and source-observation callbacks run under this monitor's lock and must not reenter it.
    pub fn drive_update(
        &self,
        updater: &mut MetadataUpdater,
        now_ms: i64,
        backoff: &mut impl JavaBackoffObservations,
        identity: &mut impl IdentityWitness,
        shuffle: &mut impl JavaShuffleObservations,
        network: &mut impl MetadataNetwork,
    ) -> std::result::Result<i64, UpdateFailure> {
        self.apply_sender_change(|metadata| updater.maybe_update(metadata, now_ms, backoff, identity, shuffle, network))
    }

    /// Records the caller's observed disconnect and publishes any request fatal outcome.
    /// Ordering follows [`MetadataUpdater::disconnect`].
    /// Network and source-observation callbacks run under this monitor's lock and must not reenter it.
    pub fn drive_disconnect(
        &self,
        updater: &mut MetadataUpdater,
        now_ms: i64,
        fatal: Option<Anomaly>,
        backoff: &mut impl JavaBackoffObservations,
        network: &mut impl MetadataNetwork,
    ) -> std::result::Result<(), UpdateFailure> {
        self.apply_sender_change(|metadata| updater.disconnect(metadata, now_ms, fatal, backoff, network))
    }

    /// Applies the matching response after connection throttling and broadcasts successful metadata publication.
    /// Response, snapshot and failure ordering follow [`ProducerMetadata::apply_with_cluster_nodes`], including native failure after publication.
    /// Identity and shuffle observations run inside the metadata lock and must not reenter the monitor.
    /// Terminal native admission failure wakes waiters to observe the ended metadata capability.
    pub fn apply_response(
        &self,
        view: RawMetadataView<'_>,
        scope: RequestScope,
        now_ms: i64,
        rebootstrap_enabled: bool,
        identity: &mut impl IdentityWitness,
        shuffle: &mut dyn JavaShuffleObservations,
    ) -> Outcome<ResponseAction> {
        let result = self.lock()?.apply_with_cluster_nodes(
            view, scope, now_ms, rebootstrap_enabled, identity, shuffle,
        );
        if matches!(&result, Ok(ResponseAction::Published) | Err(Failure::Native(_))) {
            self.state.changed.notify_waiters();
        }
        result
    }

    /// Admits and publishes a fatal error before notifying every waiter.
    /// The synchronous payload builder follows [`ProducerMetadata::prepare_fatal_error`] and must not reenter the monitor.
    /// Terminal native admission failure wakes waiters to observe the ended metadata capability.
    pub fn publish_fatal(
        &self, kind: FatalKind, payload_bytes: usize, build: impl FnOnce() -> Result<Anomaly>,
    ) -> Outcome<()> {
        let result = {
            let mut metadata = self.lock()?;
            metadata.prepare_fatal_error(payload_bytes, build)
                .and_then(|error| metadata.record_fatal_error(kind, error))
        };
        if matches!(&result, Ok(()) | Err(Failure::Native(_))) {
            self.state.changed.notify_waiters();
        }
        result
    }

    /// Closes metadata response application and wakes every waiting caller.
    pub fn close(&self) -> Outcome<()> {
        self.lock()?.close();
        self.state.changed.notify_waiters();
        Ok(())
    }

    /// Waits on a Tokio runtime with time enabled using one caller-owned source timer.
    /// Predicate precedence and fatal consumption follow [`ProducerMetadata::poll_wait`].
    /// Tokio schedules reevaluation, and the same source clock history used to construct the timer decides expiry.
    /// The clock runs under the metadata lock and must not reenter the monitor.
    /// Cancellation removes the caller's notification future.
    pub async fn await_update(
        &self, wait: &mut MetadataWait, mut now_ms: impl FnMut() -> Result<i64>,
    ) -> Outcome<WaitCompletion> {
        loop {
            // Capture broadcasts before checking the condition to cover the gap before parking.
            let notified = self.state.changed.notified();
            let observation = {
                let mut metadata = self.lock()?;
                metadata.poll_wait(wait, &mut now_ms)?
            };
            match observation {
                MetadataWaitObservation::Fatal(error) => return Ok(WaitCompletion::Fatal(error)),
                MetadataWaitObservation::Step(WaitStep::Updated) => return Ok(WaitCompletion::Updated),
                MetadataWaitObservation::Step(WaitStep::Closed) => return Ok(WaitCompletion::Closed),
                MetadataWaitObservation::Step(WaitStep::TimedOut(timeout_ms)) => return Ok(WaitCompletion::TimedOut(timeout_ms)),
                MetadataWaitObservation::Step(WaitStep::Wait(remaining_ms)) => {
                    let _ = timeout(Duration::from_millis(remaining_ms as u64), notified).await;
                }
            }
        }
    }
}
