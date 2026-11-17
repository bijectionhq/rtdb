use super::*;

#[derive(Debug)]
struct Node {
    id: i32,
    connection_ready: bool,
    channel_ready: bool,
    can_send_more: bool,
    in_flight: i32,
    preparing: bool,
    connectable: bool,
    last_attempt: i64,
}

impl Node {
    fn unavailable(id: i32) -> Self {
        Self { id, connection_ready: false, channel_ready: false, can_send_more: false,
            in_flight: 0, preparing: false, connectable: false, last_attempt: 0 }
    }

    fn ready(id: i32, in_flight: i32) -> Self {
        Self { connection_ready: true, channel_ready: true, can_send_more: true,
            in_flight, ..Self::unavailable(id) }
    }

    fn preparing(id: i32) -> Self {
        Self { preparing: true, ..Self::unavailable(id) }
    }

    fn connectable(id: i32, last_attempt: i64) -> Self {
        Self { connectable: true, last_attempt, ..Self::unavailable(id) }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Read {
    Offset(i32),
    StateReady(i32, i64),
    ChannelReady(i32),
    SendMore(i32),
    InFlight(i32),
    Preparing(i32),
    CanConnect(i32, i64),
    LastAttempt(i32),
}

struct Observations {
    offset: i32,
    reads: Vec<Read>,
}

impl Observations {
    fn new(offset: i32) -> Self { Self { offset, reads: Vec::new() } }
}

impl ConnectionObservations<Node> for Observations {
    fn next_offset(&mut self, bound: i32) -> Result<i32> {
        self.reads.push(Read::Offset(bound));
        Ok(self.offset)
    }
    fn connection_ready(&mut self, node: &Node, now_ms: i64) -> Result<bool> {
        self.reads.push(Read::StateReady(node.id, now_ms));
        Ok(node.connection_ready)
    }
    fn channel_ready(&mut self, node: &Node) -> Result<bool> {
        self.reads.push(Read::ChannelReady(node.id));
        Ok(node.channel_ready)
    }
    fn can_send_more(&mut self, node: &Node) -> Result<bool> {
        self.reads.push(Read::SendMore(node.id));
        Ok(node.can_send_more)
    }
    fn in_flight_count(&mut self, node: &Node) -> Result<i32> {
        self.reads.push(Read::InFlight(node.id));
        Ok(node.in_flight)
    }
    fn preparing_connection(&mut self, node: &Node) -> Result<bool> {
        self.reads.push(Read::Preparing(node.id));
        Ok(node.preparing)
    }
    fn can_connect(&mut self, node: &Node, now_ms: i64) -> Result<bool> {
        self.reads.push(Read::CanConnect(node.id, now_ms));
        Ok(node.connectable)
    }
    fn last_connect_attempt_ms(&mut self, node: &Node) -> Result<i64> {
        self.reads.push(Read::LastAttempt(node.id));
        Ok(node.last_attempt)
    }
}

#[test]
fn zero_in_flight_ends_the_rotated_scan_after_the_source_readiness_demands() {
    use Read::*;
    let nodes = [Node::ready(1, 0), Node::preparing(2), Node::ready(3, 0)];
    let mut observations = Observations::new(1);
    let selected = least_loaded_node(&nodes, Bootstrap::Complete, 22, &mut observations).unwrap();
    assert!(std::ptr::eq(selected.node.unwrap(), &nodes[2]));
    assert!(selected.connection_ready);
    assert_eq!(observations.reads, [Offset(3), StateReady(2, 22), StateReady(2, 22), Preparing(2),
        StateReady(3, 22), ChannelReady(3), StateReady(3, 22), ChannelReady(3), SendMore(3), InFlight(3)]);
}

#[test]
fn first_in_flight_minimum_beats_later_equal_preparing_and_connectable_nodes() {
    use Read::*;
    let nodes = [Node::ready(1, 1), Node::ready(2, 3), Node::preparing(3), Node::ready(4, 1),
        Node::connectable(5, 0)];
    let mut observations = Observations::new(1);
    let selected = least_loaded_node(&nodes, Bootstrap::Complete, -7, &mut observations).unwrap();
    assert!(std::ptr::eq(selected.node.unwrap(), &nodes[3]));
    assert!(selected.connection_ready);
    assert_eq!(observations.reads, [Offset(5), StateReady(2, -7), ChannelReady(2), StateReady(2, -7),
        ChannelReady(2), SendMore(2), InFlight(2), StateReady(3, -7), Preparing(3),
        StateReady(4, -7), ChannelReady(4), SendMore(4), InFlight(4), StateReady(5, -7),
        Preparing(5), CanConnect(5, -7), StateReady(1, -7), ChannelReady(1), SendMore(1), InFlight(1)]);
}

#[test]
fn last_preparing_node_wins_before_a_connectable_destination() {
    let nodes = [Node::preparing(-1), Node::preparing(-2), Node::connectable(-3, -10)];
    let mut observations = Observations::new(1);
    let selected = least_loaded_node(&nodes, Bootstrap::Pending, 15, &mut observations).unwrap();
    assert_eq!(selected.node.unwrap().id, -1);
    assert!(!selected.connection_ready);
    assert!(selected.has_node_available_or_connection_ready());
    assert!(!observations.reads.contains(&Read::CanConnect(-2, 15)));
}

#[test]
fn connectable_comparison_reads_the_current_winner_first_and_keeps_the_first_tie() {
    use Read::LastAttempt;
    let nodes = [Node::connectable(1, 7), Node::connectable(2, 0), Node::connectable(3, 7)];
    let mut observations = Observations::new(2);
    let selected = least_loaded_node(&nodes, Bootstrap::Complete, 100, &mut observations).unwrap();
    assert_eq!(selected.node.unwrap().id, 2);
    let attempts: Vec<_> = observations.reads.into_iter().filter(|read| matches!(read, LastAttempt(_))).collect();
    assert_eq!(attempts, [LastAttempt(3), LastAttempt(1), LastAttempt(3), LastAttempt(2)]);
}

#[test]
fn an_unsendable_ready_connection_still_prevents_rebootstrap() {
    let nodes = [Node { can_send_more: false, ..Node::ready(7, 5) },
        Node { channel_ready: false, ..Node::ready(8, 0) }];
    let mut observations = Observations::new(0);
    let selected = least_loaded_node(&nodes, Bootstrap::Complete, 10, &mut observations).unwrap();
    assert!(selected.node.is_none());
    assert!(selected.connection_ready);
    assert!(selected.has_node_available_or_connection_ready());
    assert!(!observations.reads.iter().any(|read| matches!(read, Read::InFlight(_) | Read::SendMore(8))));
}

#[test]
fn empty_cluster_reads_no_random_offset_and_distinguishes_pending_bootstrap() {
    let mut observations = Observations::new(-1);
    let selection = least_loaded_node(&[], Bootstrap::Pending, 0, &mut observations).unwrap();
    assert!(!selection.has_node_available_or_connection_ready());
    for bootstrap in [Bootstrap::Disabled, Bootstrap::Complete] {
        assert!(matches!(least_loaded_node(&[], bootstrap, 0, &mut observations), Err(SelectionFailure::NoClusterNodes)));
    }
    assert!(observations.reads.is_empty());
}

#[test]
fn invalid_observations_fail_before_an_unchecked_index_or_fallback_selection() {
    let nodes = [Node::ready(1, -1)];
    let mut observations = Observations::new(1);
    let result = least_loaded_node(&nodes, Bootstrap::Complete, 0, &mut observations);
    assert!(matches!(result, Err(SelectionFailure::Native(error)) if error.code == "rtdb/invalid-producer-random-offset"));
    assert_eq!(observations.reads, [Read::Offset(1)]);
    let mut observations = Observations::new(0);
    let result = least_loaded_node(&nodes, Bootstrap::Complete, 0, &mut observations);
    assert!(matches!(result, Err(SelectionFailure::Native(error)) if error.code == "rtdb/invalid-producer-in-flight-count"));
    assert_eq!(observations.reads.last(), Some(&Read::InFlight(1)));
}
