use super::*;
use std::cell::RefCell;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Observation { Acquire, Offset(i32), Log }

struct Shuffle<'a> {
    observed: &'a RefCell<Vec<Observation>>,
    draws: std::collections::VecDeque<i32>,
}

impl JavaShuffleObservations for Shuffle<'_> {
    fn begin(&mut self, _: &mut ShuffleAdmission<'_>) -> rtdb_types::Result<()> {
        self.observed.borrow_mut().push(Observation::Acquire);
        Ok(())
    }

    fn next_int(&mut self, bound: i32, _: &mut ShuffleAdmission<'_>) -> rtdb_types::Result<i32> {
        self.observed.borrow_mut().push(Observation::Offset(bound));
        self.draws.pop_front().ok_or_else(|| Anomaly::unsupported("fixture/cluster-shuffle-exhausted", "Missing cluster shuffle observation"))
    }
}

struct Math<'a>(&'a RefCell<Vec<Observation>>);

impl JavaBackoffObservations for Math<'_> {
    fn log(&mut self, value: f64) -> f64 {
        assert_eq!(value, 2.0);
        self.0.borrow_mut().push(Observation::Log);
        std::f64::consts::LN_2
    }

    fn pow(&mut self, _: f64, _: f64) -> f64 { panic!("no delay calculation") }
    fn next_double(&mut self, _: f64, _: f64) -> f64 { panic!("no delay calculation") }
}

fn qualified(memory: Arc<MemoryBudget>, shuffle: &mut Shuffle<'_>) -> ProducerMetadata {
    ProducerMetadata::new_with_cluster_nodes(
        Settings { refresh_backoff_ms: 10, refresh_backoff_max_ms: 10, metadata_expire_ms: 1000, metadata_idle_ms: 100 },
        memory,
        &mut Observations,
        &mut NoIdentityWitness,
        shuffle,
    ).unwrap()
}

fn response(p: &mut ProducerMetadata, body: &[u8], version: i16, now_ms: i64, allow_rebootstrap: bool, shuffle: &mut Shuffle<'_>) -> Outcome<ResponseAction> {
    let scope = p.request(now_ms)?.scope();
    p.apply_with_cluster_nodes(RawMetadataView::parse(version, body).unwrap(), scope, now_ms, allow_rebootstrap, &mut NoIdentityWitness, shuffle)
}

#[test]
fn initial_empty_cluster_acquires_default_random_before_backoff_math() {
    let observed = RefCell::new(Vec::new());
    let mut shuffle = Shuffle { observed: &observed, draws: Default::default() };
    let p = ProducerMetadata::new_with_cluster_nodes(
        Settings { refresh_backoff_ms: 10, refresh_backoff_max_ms: 20, metadata_expire_ms: 1000, metadata_idle_ms: 100 },
        MemoryBudget::new(1 << 20),
        &mut Math(&observed),
        &mut NoIdentityWitness,
        &mut shuffle,
    ).unwrap();
    assert_eq!(&*observed.borrow(), &[Observation::Acquire, Observation::Log, Observation::Log]);
    assert_eq!(p.snapshot().cluster_nodes().unwrap().len(), 0);
    assert!(p.snapshot().controller().is_none());
}

#[test]
fn ordinary_cluster_shuffles_the_broker_map_values_and_shares_their_node_owners() {
    let memory = MemoryBudget::new(1 << 20);
    let observed = RefCell::new(Vec::new());
    let mut shuffle = Shuffle { observed: &observed, draws: [0, 0].into_iter().collect() };
    let mut p = qualified(memory.clone(), &mut shuffle);
    p.request_full(false).unwrap();
    let body = Wire::body(12, &[(3, b"three", 3), (1, b"one", 1), (2, b"two", 2)], &[], 0);
    assert_eq!(response(&mut p, &body, 12, 7, false, &mut shuffle).unwrap(), ResponseAction::Published);
    let snapshot = p.snapshot();
    assert_eq!(snapshot.brokers().map(|node| node.id).collect::<Vec<_>>(), [1, 2, 3]);
    assert_eq!(snapshot.cluster_nodes().unwrap().map(|node| node.id).collect::<Vec<_>>(), [2, 3, 1]);
    assert_eq!(snapshot.controller().unwrap().id, 1);
    assert!(snapshot.bootstrap_cluster_nodes().is_none());
    for node in snapshot.cluster_nodes().unwrap() {
        assert!(std::ptr::eq(node, snapshot.brokers().find(|broker| broker.id == node.id).unwrap()));
    }
    assert_eq!(&*observed.borrow(), &[Observation::Acquire, Observation::Acquire, Observation::Offset(3), Observation::Offset(2)]);
    drop(p);
    assert!(memory.used_bytes() > 0);
    assert_eq!(snapshot.cluster_nodes().unwrap().next().unwrap().host.as_str(), "two");
    drop(snapshot);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn partial_refresh_publishes_a_new_qualified_list_without_rewriting_the_old_generation() {
    let observed = RefCell::new(Vec::new());
    let mut shuffle = Shuffle { observed: &observed, draws: [1, 0].into_iter().collect() };
    let mut p = qualified(MemoryBudget::new(1 << 20), &mut shuffle);
    p.request_full(false).unwrap();
    let first = Wire::body(12, &[(1, b"one", 1), (2, b"two", 2)], &[], 0);
    response(&mut p, &first, 12, 7, false, &mut shuffle).unwrap();
    let retained = p.snapshot();
    p.add_topic("new", 8, &mut NoIdentityWitness).unwrap();
    assert_eq!(p.request(8).unwrap().scope().kind(), RequestKind::NewTopics);
    let second = Wire::body(12, &[(4, b"four", 4), (3, b"three", 3)], &[], 0);
    response(&mut p, &second, 12, 8, false, &mut shuffle).unwrap();
    assert_eq!(p.refresh().last_successful_ms(), 7);
    assert_eq!(retained.cluster_nodes().unwrap().map(|node| node.id).collect::<Vec<_>>(), [1, 2]);
    assert_eq!(p.snapshot().cluster_nodes().unwrap().map(|node| node.id).collect::<Vec<_>>(), [4, 3]);
    assert_eq!(&*observed.borrow(), &[Observation::Acquire, Observation::Acquire, Observation::Offset(2), Observation::Acquire, Observation::Offset(2)]);
}

#[test]
fn response_failures_and_short_circuits_before_cluster_construction_consume_no_shuffle() {
    let observed = RefCell::new(Vec::new());
    let mut shuffle = Shuffle { observed: &observed, draws: Default::default() };
    let mut p = qualified(MemoryBudget::new(1 << 20), &mut shuffle);
    let duplicate = Wire::body(13, &[(1, b"one", 1), (1, b"two", 2)], &[], 0);
    assert!(matches!(response(&mut p, &duplicate, 13, 1, true, &mut shuffle), Err(Failure::DuplicateBroker(1))));
    p.add_topics(["a", "b"], 1, &mut NoIdentityWitness).unwrap();
    p.request_full(false).unwrap();
    let duplicate_id = Wire::body(13, &[(1, b"one", 1)], &[topic(b"a", 7, &[]), topic(b"b", 7, &[])], 0);
    assert!(matches!(response(&mut p, &duplicate_id, 13, 1, true, &mut shuffle), Err(Failure::DuplicateTopicId(id)) if id == TopicId([7; 16])));
    let rebootstrap = Wire::body(13, &[], &[], 129);
    assert_eq!(response(&mut p, &rebootstrap, 13, 2, true, &mut shuffle).unwrap(), ResponseAction::Rebootstrap);
    let empty_brokers = Wire::body(13, &[], &[], 0);
    assert_eq!(response(&mut p, &empty_brokers, 13, 3, true, &mut shuffle).unwrap(), ResponseAction::RetryEmptyBrokers);
    p.close();
    let ordinary = Wire::body(13, &[(1, b"one", 1)], &[], 0);
    assert!(matches!(response(&mut p, &ordinary, 13, 4, true, &mut shuffle), Err(Failure::Closed)));
    assert_eq!(&*observed.borrow(), &[Observation::Acquire]);
}

#[test]
fn response_shuffle_failure_preserves_the_old_generation_after_advancing_refresh() {
    let memory = MemoryBudget::new(1 << 20);
    let observed = RefCell::new(Vec::new());
    let mut shuffle = Shuffle { observed: &observed, draws: Default::default() };
    let mut p = qualified(memory.clone(), &mut shuffle);
    p.request_full(false).unwrap();
    let old = Wire::body(12, &[(1, b"old", 1)], &[], 0);
    response(&mut p, &old, 12, 7, false, &mut shuffle).unwrap();
    let retained = p.snapshot();
    p.request_full(false).unwrap();
    let before = memory.used_bytes();
    let new = Wire::body(12, &[(3, b"three", 3), (2, b"two", 2)], &[], 0);
    let failure = response(&mut p, &new, 12, 8, false, &mut shuffle).unwrap_err();
    assert!(matches!(failure, Failure::Native(cause) if cause.code == "fixture/cluster-shuffle-exhausted"));
    assert_eq!(p.refresh().update_version(), 2);
    assert_eq!(p.refresh().last_successful_ms(), 8);
    assert!(p.snapshot().0.same_object(&retained.0));
    assert_eq!(memory.used_bytes(), before);
    assert!(matches!(p.request(8), Err(Failure::CapabilityFailed)));
    assert_eq!(&*observed.borrow(), &[Observation::Acquire, Observation::Acquire, Observation::Acquire, Observation::Offset(2)]);
}
