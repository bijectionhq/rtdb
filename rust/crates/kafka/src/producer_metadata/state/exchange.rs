use std::{sync::Arc, time::Duration};

use crate::refresh::ConnectionStatus;
use rdkafka::{
    client::{Client, ClientContext},
    ordered_metadata::{
        MetadataTopic, NativeConnectionObservation, OrderedMetadataError, OrderedMetadataRequest,
        RawMetadataView,
    },
};

use super::{
    Budget, Failure, IdentityWitness, InFlightQueue, JavaShuffleObservations, MetadataMonitor,
    MetadataRequest, MetadataUpdater, Outcome, PendingRequest, ProducerMetadata, RequestScope,
    ResponseAction, Vector,
};

/// The native destination established by the producer's connection selection.
pub enum MetadataTarget<'a> {
    /// The exact configured host bytes and port understood by the native broker inventory.
    Bootstrap { host: &'a str, port: u16 },
    /// An existing learned broker identified by its nonnegative Kafka node ID.
    Learned(i32),
}

/// Failure before acquiring a native Metadata completion reference.
#[derive(Debug)]
pub enum SubmissionFailure {
    /// Source request preparation or temporary native input admission failed.
    Preparation(Failure),
    /// The native client could not accept the request.
    Transport(OrderedMetadataError),
}

/// Failure while handling the oldest observed Metadata exchange.
#[derive(Debug)]
pub enum ReceiveFailure {
    /// Native completion failed. The caller still owns the queued request for disconnect handling.
    Transport(OrderedMetadataError),
    /// The response body was rejected after removing the received request.
    Wire(OrderedMetadataError),
    /// The captured request does not match the updater's current scope.
    ScopeMismatch,
    /// The matching response failed during source state application.
    State(Failure),
}

/// Owns a native ordered request paired with its captured producer Metadata scope.
/// Dropping it abandons polling and follows [`OrderedMetadataRequest`]'s native lifetime and account contract.
pub struct MetadataExchange {
    scope: RequestScope,
    request: OrderedMetadataRequest,
}

impl MetadataExchange {
    /// Borrows the validated raw response and original scope once native completion is available.
    /// Completion remains available across polls.
    /// The caller processes connection throttling before applying the response once.
    pub fn poll(&self) -> Option<std::result::Result<(RequestScope, RawMetadataView<'_>), OrderedMetadataError>> {
        self.request.poll_raw().map(|response| response.map(|body| (self.scope, body)))
    }
}

impl InFlightQueue<MetadataExchange> {
    /// Processes the oldest request when the native socket publishes a response body.
    /// A received request leaves the logical queue before field validation, then valid throttle
    /// applies to the remaining requests before metadata publication and updater completion.
    /// Native transport failures leave retirement to the caller's disconnect stage.
    pub fn poll_metadata<E>(
        &mut self,
        connection: &mut ConnectionStatus<E>,
        monitor: &MetadataMonitor,
        updater: &mut MetadataUpdater,
        now_ms: i64,
        identity: &mut impl IdentityWitness,
        shuffle: &mut dyn JavaShuffleObservations,
    ) -> std::result::Result<Option<ResponseAction>, ReceiveFailure> {
        let Some(exchange) = self.oldest().map(|flight| flight.payload()) else { return Ok(None); };
        let Some(body) = exchange.request.poll_body() else { return Ok(None); };
        body.map_err(ReceiveFailure::Transport)?;
        if updater.in_progress() != Some(exchange.scope) {
            return Err(ReceiveFailure::ScopeMismatch);
        }
        let exchange = self.complete_next().expect("observed oldest request").into_payload();
        let (scope, view) = exchange.poll().expect("native terminal result remains published").map_err(ReceiveFailure::Wire)?;
        connection.metadata_received(view.version(), view.summary().throttle_ms, now_ms, |delay| self.increment_throttle(delay));
        let action = monitor.apply_response(view, scope, now_ms, updater.rebootstrap_enabled(), identity, shuffle)
            .map_err(ReceiveFailure::State)?;
        updater.response_completed(action);
        Ok(Some(action))
    }
}

impl MetadataRequest {
    fn descriptors(&self) -> Outcome<Vector<MetadataTopic<'_>>> {
        let mut budget = Budget(Arc::clone(&self.domain));
        let mut topics = Vector::with_capacity(self.topics().len(), &mut budget)?;
        for topic in self.topics() {
            topics.push(MetadataTopic::new(topic), &mut budget)?;
        }
        Ok(topics)
    }

    /// Consumes the captured request and submits it through an observed exact native connection.
    /// The observation must come from this producer's native client with auto topic creation enabled.
    /// The caller qualifies source send eligibility separately before supplying the native connection.
    /// The broker receives the retained encounter-order topics, or the broker-only request for an empty list.
    /// The originating metadata budget admits temporary topic descriptors before native timeout or observation validation.
    /// Pending or stale native observations and remaining capacity/timeout rules follow [`NativeConnectionObservation::into_metadata`].
    /// A preparation failure reaches neither the native connection nor the wire.
    /// The caller applies the producer's terminal admission policy on such a failure.
    pub fn submit_observed(
        self, observation: NativeConnectionObservation<'_>, response_capacity: usize, timeout: Duration,
    ) -> std::result::Result<MetadataExchange, SubmissionFailure> {
        let descriptors = self.descriptors().map_err(SubmissionFailure::Preparation)?;
        let native = observation.into_metadata(descriptors.as_slice(), response_capacity, timeout)
            .map_err(SubmissionFailure::Transport)?;
        Ok(MetadataExchange { scope: self.scope(), request: native })
    }

    /// Admits the logical flight before sending the original request on the observed connection.
    /// Run after the selector has qualified this connection for send.
    /// The observation, configured client and `response_capacity` follow [`Self::submit_observed`].
    /// `sent_ms` is the signed source millisecond send time and `timeout_ms` is the positive request timeout in milliseconds.
    /// Queue and descriptor admission failures precede native submission errors.
    /// Rejection preserves the queue and drops replacement capacity proposed for this send.
    pub fn submit_in_flight(
        self,
        flights: &mut InFlightQueue<MetadataExchange>,
        observation: NativeConnectionObservation<'_>,
        response_capacity: usize,
        sent_ms: i64,
        timeout_ms: i32,
    ) -> std::result::Result<(), SubmissionFailure> {
        let millis = u64::try_from(timeout_ms).map_err(|_| SubmissionFailure::Transport(OrderedMetadataError::InvalidTimeout))?;
        flights.submit(|| {
            self.submit_observed(observation, response_capacity, Duration::from_millis(millis))
                .map(|exchange| PendingRequest::new(exchange, sent_ms, timeout_ms, true))
        }).map_err(|failure| SubmissionFailure::Preparation(failure.into()))?
    }
}

impl ProducerMetadata {
    /// Prepares the source request at signed milliseconds `now_ms` and submits it to the selected target.
    /// The client must be the accounted producer for this metadata state with auto topic creation enabled.
    /// Topics are passed in builder encounter order, and an empty list uses the broker-only request.
    /// Bootstrap uses [`Client::start_ordered_metadata_endpoint`] and learned brokers use [`Client::start_ordered_metadata_at`].
    /// The producer configuration must enable `enable.ordered.metadata.bootstrap` before submitting to a bootstrap target.
    /// Their response capacity, timeout, readiness and retry contracts apply.
    /// Temporary descriptor admission failure ends mutation under the producer's existing failure policy.
    /// Native submission rejection releases the temporary inputs and leaves the metadata capability usable.
    pub fn start_exchange<C: ClientContext>(
        &mut self,
        client: &Client<C>,
        target: MetadataTarget<'_>,
        now_ms: i64,
        response_capacity: usize,
        timeout: Duration,
    ) -> std::result::Result<MetadataExchange, SubmissionFailure> {
        let request = self.request(now_ms).map_err(SubmissionFailure::Preparation)?;
        let descriptors = self.completed(request.descriptors()).map_err(SubmissionFailure::Preparation)?;
        let topics = descriptors.as_slice();
        let native = match target {
            MetadataTarget::Bootstrap { host, port } => {
                client.start_ordered_metadata_endpoint(host, port, topics, response_capacity, timeout)
            }
            MetadataTarget::Learned(id) => {
                client.start_ordered_metadata_at(id, topics, response_capacity, timeout)
            }
        }.map_err(SubmissionFailure::Transport)?;
        Ok(MetadataExchange { scope: request.scope(), request: native })
    }
}
