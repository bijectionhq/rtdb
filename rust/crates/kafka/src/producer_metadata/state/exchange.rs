use std::{sync::Arc, time::Duration};

use rdkafka::{
    client::{Client, ClientContext},
    ordered_metadata::{
        MetadataTopic, NativeConnectionObservation, OrderedMetadataError, OrderedMetadataRequest,
        RawMetadataView,
    },
};

use super::{Budget, Failure, MetadataRequest, Outcome, ProducerMetadata, RequestScope, Vector};

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
