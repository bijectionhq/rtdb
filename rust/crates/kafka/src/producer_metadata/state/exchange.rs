use std::time::Duration;

use rdkafka::{
    client::{Client, ClientContext},
    ordered_metadata::{MetadataTopic, OrderedMetadataError, OrderedMetadataRequest, RawMetadataView},
};

use super::{Failure, Outcome, ProducerMetadata, RequestScope, Vector};

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

impl ProducerMetadata {
    /// Prepares the source request at signed milliseconds `now_ms` and submits it to the selected target.
    /// The client must be the accounted producer for this metadata state with auto topic creation enabled.
    /// Topics are passed in builder encounter order, and an empty list uses the broker-only request.
    /// Bootstrap uses [`Client::start_ordered_metadata_endpoint`] and learned brokers use [`Client::start_ordered_metadata_at`].
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
        let prepared: Outcome<_> = (|| {
            let mut topics = Vector::with_capacity(request.topics().len(), &mut self.budget)?;
            for topic in request.topics() {
                topics.push(MetadataTopic::new(topic), &mut self.budget)?;
            }
            Ok(topics)
        })();
        let descriptors = self.completed(prepared).map_err(SubmissionFailure::Preparation)?;
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
