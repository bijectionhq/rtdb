use rtdb_types::{Anomaly, Result};

/// Synchronous observations of one serialized NetworkClient and its connection inventory.
/// Methods read the same connection ID for each node and preserve the source's short-circuit demand.
pub trait ConnectionObservations<N> {
    /// Observes the client's persistent Random.nextInt, returning a value in `0..bound` for a positive `bound`.
    fn next_offset(&mut self, bound: i32) -> Result<i32>;
    /// Observes READY and the connection's throttle deadline at signed milliseconds `now_ms`.
    fn connection_ready(&mut self, node: &N, now_ms: i64) -> Result<bool>;
    /// Observes readiness of the selector's active channel.
    fn channel_ready(&mut self, node: &N) -> Result<bool>;
    /// Observes whether the last send has completed and the in-flight queue has room, or the queue is empty.
    fn can_send_more(&mut self, node: &N) -> Result<bool>;
    /// Observes the request queue size, in 0..=i32::MAX.
    fn in_flight_count(&mut self, node: &N) -> Result<i32>;
    /// Observes CONNECTING or CHECKING_API_VERSIONS.
    fn preparing_connection(&mut self, node: &N) -> Result<bool>;
    /// Observes whether no connection exists or the disconnect backoff has elapsed at `now_ms`.
    fn can_connect(&mut self, node: &N, now_ms: i64) -> Result<bool>;
    /// Observes the previous connection attempt in signed milliseconds, using zero for unknown nodes.
    fn last_connect_attempt_ms(&mut self, node: &N) -> Result<i64>;
}

/// Initial bootstrap disposition consulted when the fetched cluster list is empty.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Bootstrap {
    /// Bootstrap enabled and the updater currently reports no usable generation.
    Pending,
    /// Bootstrap enabled and the updater currently reports a bootstrapped generation.
    Complete,
    /// Bootstrap disabled at client construction.
    Disabled,
}

/// A terminal selection outcome from source semantics or the native observation boundary.
#[derive(Debug)]
pub enum SelectionFailure {
    /// The source throws IllegalStateException for an empty cluster after bootstrap or with bootstrap disabled.
    NoClusterNodes,
    /// The wrapped Java scan computed a negative List.get index.
    NodeIndex(i32),
    /// The list or a required observation is outside the supported native profile.
    Native(Anomaly),
}

impl From<Anomaly> for SelectionFailure {
    fn from(error: Anomaly) -> Self { Self::Native(error) }
}

/// Borrows the selected cluster node and preserves readiness independently of selection.
#[derive(Debug)]
pub struct LeastLoadedNode<'a, N> {
    /// The original node in the caller's cluster list, if any candidate won.
    pub node: Option<&'a N>,
    /// Whether the scan observed a ready, unthrottled state and a ready channel.
    pub connection_ready: bool,
}

impl<N> LeastLoadedNode<'_, N> {
    /// Reports whether the network owner should keep the current bootstrap generation.
    pub fn has_node_available_or_connection_ready(&self) -> bool {
        self.node.is_some() || self.connection_ready
    }
}

/// Ranks the source Cluster.nodes list in its existing shuffled order starting at one random offset.
/// Returns the first zero-in-flight sendable node immediately.
/// Otherwise it prefers the first sendable minimum, the last preparing node, then the first connectable node with the oldest attempt.
/// The client serializes the scan with connection updates and supplies the original node objects.
/// The caller owns in-flight slot acquisition and any connection attempt after selection.
/// A required observation's error or invalid value ends the scan immediately with `SelectionFailure::Native`.
pub fn least_loaded_node<'a, N>(
    nodes: &'a [N],
    bootstrap: Bootstrap,
    now_ms: i64,
    observations: &mut impl ConnectionObservations<N>,
) -> std::result::Result<LeastLoadedNode<'a, N>, SelectionFailure> {
    if nodes.is_empty() {
        return match bootstrap {
            Bootstrap::Pending => Ok(LeastLoadedNode { node: None, connection_ready: false }),
            Bootstrap::Complete | Bootstrap::Disabled => Err(SelectionFailure::NoClusterNodes),
        };
    }

    let count = i32::try_from(nodes.len()).map_err(|_| Anomaly::unsupported(
        "rtdb/unsupported-producer-node-list", "Producer node count exceeds Java List size",
    ))?;
    let offset = observations.next_offset(count)?;
    if !(0..count).contains(&offset) {
        return Err(Anomaly::incorrect(
            "rtdb/invalid-producer-random-offset", "Producer random offset is outside its bound",
        ).into());
    }

    let mut connection_ready = false;
    let mut sendable = None;
    let mut min_in_flight = i32::MAX;
    let mut preparing = None;
    let mut connectable = None;
    for step in 0..count {
        let index = offset.wrapping_add(step) % count;
        let index = usize::try_from(index).map_err(|_| SelectionFailure::NodeIndex(index))?;
        let node = &nodes[index];

        if !connection_ready
            && observations.connection_ready(node, now_ms)?
            && observations.channel_ready(node)?
        {
            connection_ready = true;
        }

        if observations.connection_ready(node, now_ms)?
            && observations.channel_ready(node)?
            && observations.can_send_more(node)?
        {
            let in_flight = observations.in_flight_count(node)?;
            if in_flight < 0 {
                return Err(Anomaly::incorrect(
                    "rtdb/invalid-producer-in-flight-count", "Producer in-flight count is negative",
                ).into());
            }
            if in_flight == 0 {
                return Ok(LeastLoadedNode { node: Some(node), connection_ready: true });
            }
            if in_flight < min_in_flight {
                min_in_flight = in_flight;
                sendable = Some(node);
            }
        } else if observations.preparing_connection(node)? {
            preparing = Some(node);
        } else if observations.can_connect(node, now_ms)? {
            match connectable {
                None => connectable = Some(node),
                Some(previous) => {
                    if observations.last_connect_attempt_ms(previous)?
                        > observations.last_connect_attempt_ms(node)?
                    {
                        connectable = Some(node);
                    }
                }
            }
        }
    }

    Ok(LeastLoadedNode { node: sendable.or(preparing).or(connectable), connection_ready })
}

#[cfg(test)]
#[path = "node_selection/tests.rs"]
mod tests;
