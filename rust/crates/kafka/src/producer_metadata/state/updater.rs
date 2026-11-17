use rtdb_types::{Anomaly, Result};

use super::{
    Failure, IdentityWitness, JavaBackoffObservations, JavaShuffleObservations, MetadataRequest, Node,
    ProducerMetadata, RequestScope, ResponseAction,
};
use super::super::node_selection::{Bootstrap, ConnectionObservations, SelectionFailure, can_send_request};

/// NetworkClient timing and recovery settings after the caller has validated the producer configuration.
pub struct UpdateSettings {
    /// Default metadata request timeout in nonnegative signed int milliseconds.
    pub request_timeout_ms: i32,
    /// Delay before reconsidering a connection, in nonnegative signed long milliseconds.
    pub reconnect_backoff_ms: i64,
    /// Whether the source client is configured to bootstrap from DNS.
    pub bootstrap_enabled: bool,
    /// Whether the recovery strategy allows rebootstrap from retained addresses.
    pub rebootstrap_enabled: bool,
}

/// Outcome of the source internal request submission after version selection.
pub enum SendMetadata {
    /// The network owner has accepted the original request.
    Accepted,
    /// The source internal send encountered its synchronous UnsupportedVersionException.
    UnsupportedVersion(Anomaly),
}

/// Synchronous operations on the same serialized NetworkClient inventory used by node selection.
pub trait MetadataNetwork: ConnectionObservations<Node> {
    /// Observes CONNECTING for the node's connection ID, excluding other preparing states.
    fn is_connecting(&mut self, node: &Node) -> Result<bool>;
    /// Begins connecting the original node at signed milliseconds `now_ms`.
    fn initiate_connect(&mut self, node: &Node, now_ms: i64) -> Result<()>;
    /// Consumes the admitted request builder and retains any accepted wire operation.
    /// Retains the original request scope with each accepted operation for matching completion.
    /// UnsupportedVersion and native errors accept no wire operation.
    /// A native error leaves the updater's in-progress scope unchanged.
    fn send_metadata(&mut self, node: &Node, request: MetadataRequest, now_ms: i64) -> Result<SendMetadata>;
    /// Publishes the source fatal error and its waiter notification before returning.
    fn fatal_error(&mut self, cause: Anomaly) -> Result<()>;
}

/// Native capability and source-state failures at their observed demand point.
#[derive(Debug)]
pub enum UpdateFailure {
    /// The producer metadata state or request builder failed.
    Metadata(Failure),
    /// The retained node scan failed.
    Selection(SelectionFailure),
    /// A network observation or synchronous network operation failed.
    Network(Anomaly),
}

impl From<Failure> for UpdateFailure {
    fn from(error: Failure) -> Self { Self::Metadata(error) }
}

impl From<SelectionFailure> for UpdateFailure {
    fn from(error: SelectionFailure) -> Self { Self::Selection(error) }
}

impl From<Anomaly> for UpdateFailure {
    fn from(error: Anomaly) -> Self { Self::Network(error) }
}

/// Owns the request scope and rebootstrap attempt timer for one serialized source Metadata updater.
/// The network caller retains native operations and supplies their matching response or failure stages.
pub struct MetadataUpdater {
    settings: UpdateSettings,
    in_progress: Option<RequestScope>,
    attempt_started_ms: Option<i64>,
}

type UpdateResult<T> = std::result::Result<T, UpdateFailure>;

impl MetadataUpdater {
    /// Creates an updater before its first refresh attempt.
    pub fn new(settings: UpdateSettings) -> Self {
        Self { settings, in_progress: None, attempt_started_ms: None }
    }

    /// Copies the source in-progress scope for the current internal request.
    /// A synchronous unsupported-version send can publish a scope without accepting a wire request.
    pub fn in_progress(&self) -> Option<RequestScope> { self.in_progress }

    /// Checks whether an attempt exceeded the signed millisecond threshold, strictly.
    /// Elapsed time uses Java long wrapping arithmetic.
    pub fn needs_rebootstrap(&self, now_ms: i64, trigger_ms: i64) -> bool {
        self.attempt_started_ms.is_some_and(|start| now_ms.wrapping_sub(start) > trigger_ms)
    }

    /// Returns the due flag after short-circuiting active requests before any backoff observation.
    pub fn is_update_due(
        &self, metadata: &mut ProducerMetadata, now_ms: i64, backoff: &mut impl JavaBackoffObservations,
    ) -> UpdateResult<bool> {
        Ok(self.in_progress.is_none() && metadata.time_to_next_update(now_ms, backoff)? == 0)
    }

    /// Advances a source refresh decision and returns the next poll delay in signed milliseconds.
    /// The supplied network and random authorities must represent the same serialized producer.
    /// Native or unavailable observations preserve source mutations completed before the failure.
    /// `i64::MAX` denotes waiting for an external network event after an unavailable send or connect.
    pub fn maybe_update(
        &mut self,
        metadata: &mut ProducerMetadata,
        now_ms: i64,
        backoff: &mut impl JavaBackoffObservations,
        identity: &mut impl IdentityWitness,
        shuffle: &mut impl JavaShuffleObservations,
        network: &mut impl MetadataNetwork,
    ) -> UpdateResult<i64> {
        let refresh_delay = metadata.time_to_next_update(now_ms, backoff)?;
        let request_timeout = i64::from(self.settings.request_timeout_ms);
        let in_progress_delay = if self.in_progress.is_some() { request_timeout } else { 0 };
        let delay = refresh_delay.max(in_progress_delay);
        if delay > 0 {
            return Ok(delay);
        }

        self.attempt_started_ms.get_or_insert(now_ms);
        let bootstrap = if self.settings.bootstrap_enabled { Bootstrap::Pending } else { Bootstrap::Disabled };
        let initial = metadata.snapshot();
        let selected = initial.select_node(bootstrap, now_ms, network)?;
        let recovered;
        let (snapshot, selected) = if self.settings.rebootstrap_enabled
            && initial.cluster_nodes().is_some_and(|nodes| nodes.len() != 0)
            && !selected.has_node_available_or_connection_ready()
        {
            self.rebootstrap(metadata, now_ms, identity, shuffle)?;
            recovered = metadata.snapshot();
            (&recovered, recovered.select_node(bootstrap, now_ms, network)?)
        } else {
            (&initial, selected)
        };
        let Some(node) = selected.node else { return Ok(self.settings.reconnect_backoff_ms); };

        if can_send_request(network, node, now_ms)? {
            let request = metadata.request(now_ms)?;
            let scope = request.scope();
            if let SendMetadata::UnsupportedVersion(cause) = network.send_metadata(node, request, now_ms)? {
                self.failed_request(metadata, now_ms, Some(cause), network)?;
            }
            self.in_progress = Some(scope);
            return Ok(request_timeout);
        }

        for peer in snapshot.cluster_nodes().expect("selected Cluster has qualified node order") {
            if network.is_connecting(peer)? {
                return Ok(self.settings.reconnect_backoff_ms);
            }
        }
        if network.can_connect(node, now_ms)? {
            network.initiate_connect(node, now_ms)?;
            return Ok(self.settings.reconnect_backoff_ms);
        }
        Ok(i64::MAX)
    }

    /// Rebuilds bootstrap metadata from the retained addresses, then records the new attempt start.
    pub fn rebootstrap(
        &mut self, metadata: &mut ProducerMetadata, now_ms: i64, identity: &mut impl IdentityWitness,
        shuffle: &mut impl JavaShuffleObservations,
    ) -> UpdateResult<()> {
        metadata.rebootstrap(shuffle, identity)?;
        self.attempt_started_ms = Some(now_ms);
        Ok(())
    }

    /// Finishes an internal request after the caller has applied the matching raw response.
    /// A failed response application must leave this step uncalled.
    /// Publication clears the attempt start, a server rebootstrap sets it to zero, and an empty-broker retry preserves it.
    /// Each outcome clears the in-progress scope.
    pub fn response_completed(&mut self, action: ResponseAction) {
        match action {
            ResponseAction::Published => self.attempt_started_ms = None,
            ResponseAction::Rebootstrap => self.attempt_started_ms = Some(0),
            ResponseAction::RetryEmptyBrokers => (),
        }
        self.in_progress = None;
    }

    /// Publishes an optional fatal cause, records the failed refresh and clears its request scope.
    pub fn failed_request(
        &mut self, metadata: &mut ProducerMetadata, now_ms: i64, fatal: Option<Anomaly>,
        network: &mut impl MetadataNetwork,
    ) -> UpdateResult<()> {
        if let Some(cause) = fatal {
            network.fatal_error(cause)?;
        }
        metadata.failed_response(now_ms)?;
        self.in_progress = None;
        Ok(())
    }

    /// Updates refresh state after the caller has handled source disconnect diagnostics.
    /// A due request records failure before any fatal authentication cause and full-refresh demand.
    pub fn disconnect(
        &mut self, metadata: &mut ProducerMetadata, now_ms: i64, fatal: Option<Anomaly>,
        backoff: &mut impl JavaBackoffObservations, network: &mut impl MetadataNetwork,
    ) -> UpdateResult<()> {
        if self.is_update_due(metadata, now_ms, backoff)? {
            self.failed_request(metadata, now_ms, None, network)?;
        }
        if let Some(cause) = fatal {
            network.fatal_error(cause)?;
        }
        metadata.request_full(false)?;
        Ok(())
    }
}
