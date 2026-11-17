use super::*;
use crate::node_selection::{Bootstrap, ConnectionObservations, SelectionFailure};

#[derive(Default)]
struct Connections {
    draws: usize,
    selected_node: i32,
}

impl ConnectionObservations<Node> for Connections {
    fn next_offset(&mut self, bound: i32) -> rtdb_types::Result<i32> {
        self.draws += 1;
        assert_eq!(bound, 3);
        Ok(0)
    }

    fn connection_ready(&mut self, node: &Node, now_ms: i64) -> rtdb_types::Result<bool> {
        assert_eq!(now_ms, 15);
        self.selected_node = node.id;
        Ok(true)
    }

    fn channel_ready(&mut self, _: &Node) -> rtdb_types::Result<bool> { Ok(true) }
    fn can_send_more(&mut self, _: &Node) -> rtdb_types::Result<bool> { Ok(true) }
    fn in_flight_count(&mut self, _: &Node) -> rtdb_types::Result<i32> { Ok(0) }
    fn preparing_connection(&mut self, _: &Node) -> rtdb_types::Result<bool> { panic!("ready node") }
    fn can_connect(&mut self, _: &Node, _: i64) -> rtdb_types::Result<bool> { panic!("ready node") }
    fn last_connect_attempt_ms(&mut self, _: &Node) -> rtdb_types::Result<i64> { panic!("ready node") }
}

#[test]
fn selection_borrows_the_shuffled_bootstrap_node_from_the_old_generation() {
    let memory = MemoryBudget::new(1 << 20);
    let seeds = JavaRandomSeeds::new();
    let mut slot = Some(JavaRandom::from_seed(0));
    let mut shuffle = JavaShuffleRandom::new(&mut slot, &seeds, || panic!("generator already acquired"));
    let mut producer = ProducerMetadata::new_with_cluster_nodes(
        Settings { refresh_backoff_ms: 10, refresh_backoff_max_ms: 10, metadata_expire_ms: 1000, metadata_idle_ms: 100 },
        memory.clone(), &mut Observations, &mut NoIdentityWitness, &mut shuffle,
    ).unwrap();
    let addresses = producer.prepare_bootstrap_addresses(&[
        ResolvedEndpoint { host: "a", port: 1 },
        ResolvedEndpoint { host: "b", port: 2 },
        ResolvedEndpoint { host: "c", port: 3 },
    ]).unwrap();
    producer.bootstrap(addresses, &mut shuffle, &mut NoIdentityWitness).unwrap();
    let retained = producer.snapshot();
    producer.rebootstrap(&mut shuffle, &mut NoIdentityWitness).unwrap();
    let mut connections = Connections::default();
    let _full_budget = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
    let selection = retained.select_node(Bootstrap::Complete, 15, &mut connections).unwrap();
    let node = selection.node.unwrap();
    let original_cluster_node = retained.cluster_nodes().unwrap().next().unwrap();
    let broker_map_node = retained.brokers().find(|broker| broker.id == node.id).unwrap();
    assert_eq!(node.id, -3);
    assert!(std::ptr::eq(node, original_cluster_node));
    assert!(!std::ptr::eq(node, broker_map_node));
    assert!(selection.connection_ready);
    assert_eq!((connections.draws, connections.selected_node), (1, node.id));
    assert_eq!(memory.used_bytes(), memory.limit_bytes());
}

#[test]
fn unqualified_generation_fails_before_any_connection_observation() {
    let producer = ProducerMetadata::new(
        Settings { refresh_backoff_ms: 10, refresh_backoff_max_ms: 10, metadata_expire_ms: 1000, metadata_idle_ms: 100 },
        MemoryBudget::new(1 << 20), &mut Observations, &mut NoIdentityWitness,
    ).unwrap();
    let mut connections = Connections::default();
    assert!(matches!(
        producer.snapshot().select_node(Bootstrap::Pending, 0, &mut connections),
        Err(SelectionFailure::Native(error)) if error.code == "rtdb/kafka-cluster-node-order-required",
    ));
    assert_eq!(connections.draws, 0);
}
