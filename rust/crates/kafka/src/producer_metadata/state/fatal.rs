use std::sync::Arc;

use rtdb_types::{Anomaly, MemoryBudget, Result, java_hash_map::Admission};

use super::{Lease, MetadataWait, Outcome, ProducerMetadata, Shared, WaitStep};

/// One admitted source fatal error whose clones retain the same payload and reservations.
#[derive(Clone)]
pub struct MetadataError {
    payload: Shared<ErrorPayload>,
    domain: Arc<MemoryBudget>,
}

struct ErrorPayload {
    cause: Anomaly,
    _lease: Lease,
}

impl MetadataError {
    /// Borrows the source error payload for this retained handle.
    pub fn cause(&self) -> &Anomaly { &self.payload.cause }
}

/// Which source fatal error slot is replaced.
pub enum FatalKind {
    /// Ordinary request errors are consumed when first observed.
    Request,
    /// Bootstrap DNS failures remain available on every observation.
    Bootstrap,
}

/// A fatal or recoverable error observed while resolving a topic.
pub enum MetadataTopicError {
    /// The source fatal error takes precedence over the topic's recoverable result.
    Fatal(MetadataError),
    /// The retained Kafka authorization or invalid-topic error code.
    Recoverable(i16),
}

/// Returns the retained fatal or timer outcome for one metadata wait evaluation.
pub enum MetadataWaitObservation {
    /// A source fatal with its existing payload reservation.
    Fatal(MetadataError),
    /// A clock or metadata state decision from the remaining wait predicate.
    Step(WaitStep),
}

impl ProducerMetadata {
    /// Admits the caller's bound for all payload allocation before constructing one source exception.
    /// The builder runs synchronously once and must stay within that byte bound.
    /// Native admission or builder failure ends this metadata capability's mutation.
    pub fn prepare_fatal_error(
        &mut self, payload_bytes: usize, build: impl FnOnce() -> Result<Anomaly>,
    ) -> Outcome<MetadataError> {
        self.usable()?;
        let prepared: Outcome<_> = (|| {
            let lease = self.budget.reserve(payload_bytes)?;
            let payload = Shared::new(ErrorPayload { cause: build()?, _lease: lease }, &mut self.budget)?;
            Ok(MetadataError { payload, domain: Arc::clone(&self.budget.0) })
        })();
        self.completed(prepared)
    }

    /// Replaces the selected fatal slot with an error admitted against the same `MemoryBudget` owner.
    /// An error from another budget ends metadata mutation before replacing the slot.
    /// The enclosing monitor publishes any required notification after this mutation.
    pub fn record_fatal_error(&mut self, kind: FatalKind, error: MetadataError) -> Outcome<()> {
        self.usable()?;
        if !Arc::ptr_eq(&error.domain, &self.budget.0) {
            return self.completed(Err(Anomaly::unsupported(
                "rtdb/kafka-fatal-error-budget-domain", "Metadata error belongs to another admission domain",
            ).into()));
        }
        match kind {
            FatalKind::Request => self.fatal = Some(error),
            FatalKind::Bootstrap => self.bootstrap_fatal = Some(error),
        }
        Ok(())
    }

    /// Observes a bootstrap failure without consuming it, otherwise takes an ordinary fatal error once.
    /// Recoverable topic state is untouched.
    pub fn take_fatal_error(&mut self) -> Outcome<Option<MetadataError>> {
        self.usable()?;
        Ok(self.bootstrap_fatal.clone().or_else(|| self.fatal.take()))
    }

    /// Observes bootstrap or consumes ordinary fatal state under [`Self::take_fatal_error`]'s precedence.
    /// The remaining closed, signed version and clock checks follow [`MetadataWait::step`].
    /// The caller performs this check and any wait registration in the notification owner's synchronized region.
    /// A missing clock observation is returned as a native failure for this waiting call.
    pub fn poll_wait(
        &mut self, wait: &mut MetadataWait, now_ms: impl FnOnce() -> Result<i64>,
    ) -> Outcome<MetadataWaitObservation> {
        if let Some(error) = self.take_fatal_error()? {
            return Ok(MetadataWaitObservation::Fatal(error));
        }
        Ok(MetadataWaitObservation::Step(wait.advance(&self.refresh, now_ms)?))
    }

    /// Observes source fatal errors before checking the requested topic's recoverable errors.
    /// A bootstrap error leaves all slots intact.
    /// An ordinary fatal error is consumed and clears the entire recoverable sets.
    /// Without a fatal error, the recoverable result and clearing follow [`Self::take_topic_error`].
    pub fn take_error_for_topic(&mut self, topic: &str) -> Outcome<Option<MetadataTopicError>> {
        self.usable()?;
        if let Some(bootstrap) = self.bootstrap_fatal.clone() {
            return Ok(Some(MetadataTopicError::Fatal(bootstrap)));
        }
        if let Some(fatal) = self.fatal.take() {
            let cleared = self.clear_recoverable_errors();
            return self.completed(cleared).map(|()| Some(MetadataTopicError::Fatal(fatal)));
        }
        self.take_topic_error(topic).map(|error| error.map(MetadataTopicError::Recoverable))
    }
}
