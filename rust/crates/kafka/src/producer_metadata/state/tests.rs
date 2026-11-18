use super::*;

struct Observations;
impl JavaBackoffObservations for Observations {
    fn log(&mut self, _: f64) -> f64 { panic!("equal backoff avoids Java log") }
    fn pow(&mut self, _: f64, _: f64) -> f64 { panic!("equal backoff avoids Java pow") }
    fn next_double(&mut self, _: f64, _: f64) -> f64 { panic!("equal backoff avoids Java random") }
}
struct Topic<'a> { name: Option<&'a [u8]>, id: u8, error: i16, internal: bool, partitions: &'a [(i32, i32, i32, i16)] }
fn topic<'a>(name: &'a [u8], id: u8, partitions: &'a [(i32, i32, i32, i16)]) -> Topic<'a> {
    Topic { name: Some(name), id, error: 0, internal: false, partitions }
}
struct Wire { version: i16, bytes: Vec<u8> }
impl Wire {
    fn i16(&mut self, value: i16) { self.bytes.extend(value.to_be_bytes()); }
    fn i32(&mut self, value: i32) { self.bytes.extend(value.to_be_bytes()); }
    fn varint(&mut self, mut value: u32) {
        while value >= 128 { self.bytes.push(value as u8 | 128); value >>= 7; }
        self.bytes.push(value as u8);
    }
    fn count(&mut self, value: usize) { if self.version >= 9 { self.varint(value as u32 + 1); } else { self.i32(value as i32); } }
    fn string(&mut self, value: Option<&[u8]>) {
        if self.version >= 9 { self.varint(value.map_or(0, |v| v.len() as u32 + 1)); }
        else { self.i16(value.map_or(-1, |v| v.len() as i16)); }
        if let Some(value) = value { self.bytes.extend(value); }
    }
    fn tags(&mut self) { if self.version >= 9 { self.bytes.push(0); } }
    fn body(version: i16, brokers: &[(i32, &[u8], i32)], topics: &[Topic<'_>], top_error: i16) -> Vec<u8> {
        let mut w = Self { version, bytes: Vec::new() };
        w.i32(0); w.count(brokers.len());
        for &(id, host, port) in brokers { w.i32(id); w.string(Some(host)); w.i32(port); w.string(None); w.tags(); }
        w.string(Some(b"cluster")); w.i32(1); w.count(topics.len());
        for topic in topics {
            w.i16(topic.error); w.string(topic.name);
            if version >= 10 { w.bytes.extend([topic.id; 16]); }
            w.bytes.push(u8::from(topic.internal)); w.count(topic.partitions.len());
            for &(id, leader, epoch, error) in topic.partitions {
                w.i16(error); w.i32(id); w.i32(leader); w.i32(epoch);
                w.count(2); w.i32(leader); w.i32(-3);
                w.count(1); w.i32(leader); w.count(1); w.i32(-3); w.tags();
            }
            w.i32(-1); w.tags();
        }
        if version <= 10 { w.i32(-1); }
        if version >= 13 { w.i16(top_error); }
        w.tags(); w.bytes
    }
}
fn producer(budget: Arc<MemoryBudget>) -> ProducerMetadata {
    let mut producer = ProducerMetadata::new(Settings { refresh_backoff_ms: 10, refresh_backoff_max_ms: 10, metadata_expire_ms: 1000, metadata_idle_ms: 100 }, budget, &mut Observations, &mut NoIdentityWitness).unwrap();
    producer.request_full(false).unwrap();
    producer
}
fn apply(p: &mut ProducerMetadata, version: i16, topics: &[Topic<'_>], now: i64) -> Outcome<ResponseAction> {
    let bytes = Wire::body(version, &[(1, b"host", 9092)], topics, 0);
    let scope = p.request(now)?.scope();
    p.apply(RawMetadataView::parse(version, &bytes).unwrap(), scope, now, false, &mut NoIdentityWitness)
}

#[test]
fn duplicate_broker_fails_before_null_topic_and_refresh_mutation() {
    let mut p = producer(MemoryBudget::new(1 << 20));
    let version = p.refresh().update_version();
    let bytes = Wire::body(12, &[(7, b"first", 1), (7, b"second", 2)], &[Topic { name: None, id: 1, error: 0, internal: false, partitions: &[] }], 0);
    let scope = p.request(0).unwrap().scope();
    assert!(matches!(p.apply(RawMetadataView::parse(12, &bytes).unwrap(), scope, 0, false, &mut NoIdentityWitness), Err(Failure::DuplicateBroker(7))));
    assert_eq!(p.refresh().update_version(), version);
    assert_eq!(p.snapshot().brokers().len(), 0);
    assert!(p.request(0).is_ok());
}

#[test]
fn null_error_topic_precedes_rebootstrap_even_with_no_topic_error_or_brokers() {
    let mut p = producer(MemoryBudget::new(1 << 20));
    let bytes = Wire::body(13, &[], &[Topic { name: None, id: 1, error: 0, internal: false, partitions: &[] }], 129);
    let scope = p.request(0).unwrap().scope();
    assert!(matches!(p.apply(RawMetadataView::parse(13, &bytes).unwrap(), scope, 0, true, &mut NoIdentityWitness), Err(Failure::NullErrorTopic)));
    assert_eq!(p.refresh().update_version(), 0);
    assert!(p.request(0).is_ok());
}

#[test]
fn reverse_uuid_duplicate_fails_after_refresh_and_preserves_generation() {
    let mut p = producer(MemoryBudget::new(1 << 20));
    p.add_topics(["a", "b"], 0, &mut NoIdentityWitness).unwrap();
    let version = p.refresh().update_version();
    assert!(matches!(apply(&mut p, 12, &[topic(b"a", 4, &[(0, 1, 3, 0)]), topic(b"b", 4, &[(0, 1, 4, 0)])], 0), Err(Failure::DuplicateTopicId(id)) if id == TopicId([4; 16])));
    assert_eq!(p.refresh().update_version(), version + 1);
    assert_eq!(p.snapshot().brokers().len(), 0);
    assert_eq!(p.epochs.len(), 2);
    assert!(p.request(0).is_ok());
}

#[test]
fn malformed_names_use_java_replacement_before_retention_and_key_equality() {
    let mut p = producer(MemoryBudget::new(1 << 20));
    p.add_topic("\u{fffd}", 0, &mut NoIdentityWitness).unwrap();
    apply(&mut p, 12, &[topic(&[0xff], 1, &[(8, 1, 3, 0)]), topic(&[0xfe], 1, &[(8, 1, 4, 0)])], 0).unwrap();
    let snapshot = p.snapshot();
    let partition = snapshot.partition("\u{fffd}", 8).unwrap();
    assert_eq!(snapshot.partition_metadata().len(), 1);
    assert_eq!(partition.epoch(), Some(4));
    assert_eq!(partition.replicas(), [1, -3]);
    assert_eq!(partition.isr(), [1]);
    assert_eq!(partition.offline(), [-3]);
}

#[test]
fn stale_epoch_retains_previous_value_until_a_nonzero_topic_id_changes() {
    let mut p = producer(MemoryBudget::new(1 << 20));
    p.add_topic("t", 0, &mut NoIdentityWitness).unwrap();
    apply(&mut p, 12, &[topic(b"t", 1, &[(3, 1, 10, 0)])], 0).unwrap();
    apply(&mut p, 12, &[topic(b"t", 1, &[(3, -1, 2, 6)])], 1).unwrap();
    assert_eq!(p.snapshot().partition("t", 3).unwrap().epoch(), Some(10));
    assert_eq!(p.snapshot().partition("t", 3).unwrap().leader(), Some(1));
    assert!(p.refresh().update_requested());
    apply(&mut p, 12, &[topic(b"t", 2, &[(3, -1, -2, 0)])], 2).unwrap();
    assert_eq!(p.snapshot().partition("t", 3).unwrap().epoch(), Some(-2));
    apply(&mut p, 12, &[topic(b"t", 0, &[(3, 1, -3, 0)])], 3).unwrap();
    assert_eq!(p.snapshot().partition("t", 3).unwrap().epoch(), Some(-2));
    assert_eq!(p.snapshot().topic_id("t"), None);
}

#[test]
fn unreliable_or_absent_epochs_remove_retained_epoch_state() {
    let mut p = producer(MemoryBudget::new(1 << 20));
    p.add_topic("t", 0, &mut NoIdentityWitness).unwrap();
    apply(&mut p, 9, &[topic(b"t", 0, &[(0, 1, 9, 0)])], 0).unwrap();
    apply(&mut p, 8, &[topic(b"t", 0, &[(0, 1, 1, 0)])], 1).unwrap();
    assert_eq!(p.snapshot().partition("t", 0).unwrap().epoch(), None);
    assert_eq!(p.epochs.len(), 0);
    apply(&mut p, 9, &[topic(b"t", 0, &[(0, 1, -2, 0)])], 2).unwrap();
    assert_eq!(p.snapshot().partition("t", 0).unwrap().epoch(), Some(-2));
    apply(&mut p, 9, &[topic(b"t", 0, &[(0, 1, -1, 0)])], 3).unwrap();
    assert_eq!(p.snapshot().partition("t", 0).unwrap().epoch(), None);
    assert_eq!(p.epochs.len(), 0);
}

#[test]
fn partial_update_replays_map_values_and_replaces_nodes_for_retained_partitions() {
    let mut p = producer(MemoryBudget::new(1 << 20));
    p.add_topics(["a", "b"], 0, &mut NoIdentityWitness).unwrap();
    apply(&mut p, 12, &[topic(b"a", 1, &[(0, 1, 1, 0), (1, 1, 1, 0)]), topic(b"b", 2, &[(0, 1, 1, 0), (1, 1, 1, 0)])], 0).unwrap();
    let old = p.snapshot();
    assert_eq!(old.partition_metadata().map(|p| (p.key().topic().unwrap(), p.key().partition())).collect::<Vec<_>>(), [("a", 1), ("a", 0), ("b", 1), ("b", 0)]);
    let scope = p.request(1).unwrap().scope();
    assert_eq!(scope.kind(), RequestKind::NewTopics);
    let bytes = Wire::body(12, &[(2, b"other", 0)], &[topic(b"b", 3, &[(2, 2, 2, 0)])], 0);
    p.apply(RawMetadataView::parse(12, &bytes).unwrap(), scope, 1, false, &mut NoIdentityWitness).unwrap();
    let current = p.snapshot();
    assert_eq!(current.partition_metadata().map(|p| (p.key().topic().unwrap(), p.key().partition())).collect::<Vec<_>>(), [("b", 2), ("a", 1), ("a", 0)]);
    assert_eq!(current.partitions("a").iter().map(|p| (p.id, p.has_leader)).collect::<Vec<_>>(), [(1, false), (0, false)]);
    assert_eq!(current.topic_id("a"), Some(TopicId([1; 16])));
    assert_eq!(current.topic_id("b"), Some(TopicId([3; 16])));
    assert_eq!(old.partition_metadata().len(), 4);
    assert!(old.partitions("a").iter().all(|p| p.has_leader));
}

#[test]
fn new_topic_survives_expiry_once_and_full_snapshot_retains_its_uuid_after_eviction() {
    let mut p = producer(MemoryBudget::new(1 << 20));
    p.add_topic("t", 0, &mut NoIdentityWitness).unwrap();
    apply(&mut p, 12, &[topic(b"t", 8, &[(0, 1, 1, 0)])], 100).unwrap();
    assert_eq!(p.snapshot().partitions("t").len(), 1);
    p.request_for_topic("t", &mut NoIdentityWitness).unwrap();
    apply(&mut p, 12, &[topic(b"t", 8, &[(0, 1, 2, 0)])], 100).unwrap();
    assert_eq!(p.snapshot().partitions("t").len(), 0);
    assert_eq!(p.snapshot().topic_id("t"), Some(TopicId([8; 16])));
    assert_eq!(p.epochs.len(), 0);
    assert_eq!(p.active.iter().len(), 0);
}

#[test]
fn request_version_retains_new_topics_added_while_response_is_pending() {
    let mut p = producer(MemoryBudget::new(1 << 20));
    p.add_topic("a", 0, &mut NoIdentityWitness).unwrap();
    let pending = p.request(0).unwrap();
    assert!(pending.allow_auto_topic_creation());
    p.add_topic("b", 0, &mut NoIdentityWitness).unwrap();
    let bytes = Wire::body(12, &[(1, b"host", 1)], &[topic(b"a", 1, &[])], 0);
    p.apply(RawMetadataView::parse(12, &bytes).unwrap(), pending.scope(), 0, false, &mut NoIdentityWitness).unwrap();
    assert!(p.refresh().update_requested());
    assert_eq!(p.request(1).unwrap().topics().collect::<Vec<_>>(), ["b"]);
    assert_eq!(pending.topics().collect::<Vec<_>>(), ["a"]);
}

#[test]
fn topic_errors_replace_duplicates_and_clear_recoverable_sets_together() {
    let mut p = producer(MemoryBudget::new(1 << 20));
    p.add_topics(["a", "b"], 0, &mut NoIdentityWitness).unwrap();
    let mut a = topic(b"a", 0, &[]); a.error = 17;
    let mut a2 = topic(b"a", 0, &[]); a2.error = 29;
    let mut b = topic(b"b", 0, &[]); b.error = 32767;
    apply(&mut p, 12, &[a, a2, b], 0).unwrap();
    assert_eq!(p.topic_error("a"), Some(29));
    assert_eq!(p.topic_error("b"), Some(-1));
    assert_eq!(p.take_topic_error("a").unwrap(), Some(29));
    assert_eq!(p.take_topic_error("a").unwrap(), None);
    assert_eq!(p.snapshot().invalid_topics().collect::<Vec<_>>(), [Some("a")]);
    assert_eq!(p.snapshot().unauthorized_topics().collect::<Vec<_>>(), [Some("a")]);
}

#[test]
fn rebootstrap_precedes_empty_brokers_without_refresh_publication() {
    let mut p = producer(MemoryBudget::new(1 << 20));
    let scope = p.request(0).unwrap().scope();
    let bytes = Wire::body(13, &[], &[], 129);
    let view = RawMetadataView::parse(13, &bytes).unwrap();
    assert_eq!(p.apply(view, scope, 5, true, &mut NoIdentityWitness).unwrap(), ResponseAction::Rebootstrap);
    assert_eq!(p.refresh().update_version(), 0);
    assert_eq!(p.apply(view, scope, 5, false, &mut NoIdentityWitness).unwrap(), ResponseAction::RetryEmptyBrokers);
    assert_eq!(p.refresh().update_version(), 0);
    assert_eq!(p.time_to_next_update(5, &mut Observations).unwrap(), 10);
}

#[test]
fn one_selection_survives_retries_refresh_and_producer_drop() {
    let budget = MemoryBudget::new(1 << 20);
    let mut p = producer(budget.clone());
    p.add_topic("t", 0, &mut NoIdentityWitness).unwrap();
    apply(&mut p, 12, &[topic(b"t", 1, &[(5, 1, 1, 0), (1, 1, 1, 0), (3, -1, 1, 0)])], 0).unwrap();
    let send = p.select_send("t", "keyed record").unwrap();
    assert_eq!(send.partition_id(), 5);
    assert_eq!(send.partition_id(), 5);
    assert_eq!(p.select_send("t", ()).unwrap().partition_id(), 1);
    apply(&mut p, 12, &[topic(b"t", 1, &[(99, 1, 2, 0)])], 1).unwrap();
    drop(p);
    assert_eq!(send.partition_id(), 5);
    assert_eq!(*send.record(), "keyed record");
    assert_eq!(send.snapshot().partitions("t").len(), 3);
    assert!(budget.used_bytes() > 0);
    drop(send);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn no_leader_fallback_uses_list_index_and_empty_attempt_consumes_counter() {
    let mut p = producer(MemoryBudget::new(1 << 20));
    assert!(matches!(p.select_send("t", ()), Err(Failure::EmptyTopic)));
    p.add_topic("t", 0, &mut NoIdentityWitness).unwrap();
    apply(&mut p, 12, &[topic(b"t", 1, &[(7, -1, 1, 0), (9, -1, 1, 0)])], 0).unwrap();
    assert_eq!(p.select_send("t", ()).unwrap().partition_id(), 1);
    assert_eq!(p.select_send("t", ()).unwrap().partition_id(), 0);
}

#[test]
fn counter_survives_topic_expiry_and_readdition() {
    let mut p = producer(MemoryBudget::new(1 << 20));
    p.add_topic("t", 0, &mut NoIdentityWitness).unwrap();
    apply(&mut p, 12, &[topic(b"t", 1, &[(7, 1, 1, 0), (9, 1, 1, 0)])], 0).unwrap();
    assert_eq!(p.select_send("t", ()).unwrap().partition_id(), 9);
    p.request_full(false).unwrap();
    apply(&mut p, 12, &[topic(b"t", 1, &[])], 100).unwrap();
    assert!(p.snapshot().partitions("t").is_empty());
    p.add_topic("t", 101, &mut NoIdentityWitness).unwrap();
    apply(&mut p, 12, &[topic(b"t", 1, &[(5, 1, 2, 0), (1, 1, 2, 0)])], 101).unwrap();
    assert_eq!(p.select_send("t", ()).unwrap().partition_id(), 1);
}

#[test]
fn resolved_empty_node_is_available() {
    let mut p = producer(MemoryBudget::new(1 << 20));
    p.add_topic("t", 0, &mut NoIdentityWitness).unwrap();
    let bytes = Wire::body(12, &[(1, b"", -1)], &[topic(b"t", 1, &[(17, 1, 1, 0)])], 0);
    let scope = p.request(0).unwrap().scope();
    p.apply(RawMetadataView::parse(12, &bytes).unwrap(), scope, 0, false, &mut NoIdentityWitness).unwrap();
    assert_eq!(p.select_send("t", ()).unwrap().partition_id(), 17);
}

#[test]
fn native_admission_failure_keeps_published_generation_and_final_lease() {
    let budget = MemoryBudget::new(1 << 20);
    let mut p = producer(budget.clone());
    p.add_topic("t", 0, &mut NoIdentityWitness).unwrap();
    apply(&mut p, 12, &[topic(b"t", 1, &[(0, 1, 1, 0)])], 0).unwrap();
    let snapshot = p.snapshot();
    let scope = p.request(1).unwrap().scope();
    let bytes = Wire::body(12, &[(1, b"host", 1)], &[topic(b"t", 1, &[(0, 1, 2, 0)])], 0);
    let blocker = budget.reserve(budget.limit_bytes() - budget.used_bytes()).unwrap();
    assert!(matches!(p.apply(RawMetadataView::parse(12, &bytes).unwrap(), scope, 1, false, &mut NoIdentityWitness), Err(Failure::Native(_))));
    assert!(matches!(p.request(1), Err(Failure::CapabilityFailed)));
    assert_eq!(snapshot.partition("t", 0).unwrap().epoch(), Some(1));
    assert_eq!(p.snapshot().partition("t", 0).unwrap().epoch(), Some(1));
    drop(blocker); drop(p);
    assert!(budget.used_bytes() > 0);
    assert_eq!(snapshot.partition("t", 0).unwrap().leader(), Some(1));
    drop(snapshot);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn active_topic_resize_reverses_prefix_before_the_last_run() {
    let mut p = producer(MemoryBudget::new(1 << 20));
    p.add_topics(["a", "q", "A", "Q", "b", "c", "d", "e", "f", "g", "h", "i"], 0, &mut NoIdentityWitness).unwrap();
    assert_eq!(p.request(0).unwrap().topics().collect::<Vec<_>>(), ["A", "a", "b", "c", "d", "e", "f", "g", "h", "i", "q", "Q"]);
}

#[test]
fn active_topic_resource_failure_rejects_mutation_without_invalidating_retained_snapshot() {
    let memory = MemoryBudget::new(1 << 20);
    let mut p = producer(Arc::clone(&memory));
    p.add_topic("t", 0, &mut NoIdentityWitness).unwrap();
    apply(&mut p, 12, &[topic(b"t", 1, &[(0, 1, 1, 0)])], 0).unwrap();
    let snapshot = p.snapshot();
    let _remaining = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
    assert!(matches!(p.add_topic("new", 1, &mut NoIdentityWitness), Err(Failure::Native(_))));
    assert!(matches!(p.request(1), Err(Failure::CapabilityFailed)));
    assert_eq!(snapshot.partition("t", 0).unwrap().epoch(), Some(1));
}

#[test]
fn topic_partition_identity_witness_is_observed_only_at_tree_tie_break() {
    struct Witness { calls: usize }
    impl IdentityWitness for Witness {
        fn topic_partition_hash(&mut self, _: &TopicPartitionKey) -> rtdb_types::Result<i32> { self.calls += 1; Err(Anomaly::unsupported("fixture/no-witness", "No identity observation supplied")) }
    }
    let mut budget = Budget(MemoryBudget::new(1 << 20));
    let mut witness = Witness { calls: 0 };
    let names = ["AaAaAaAa", "AaAaAaBB", "AaAaBBAa", "AaAaBBBB", "AaBBAaAa", "AaBBAaBB", "AaBBBBAa", "AaBBBBBB", "BBAaAaAa"];
    let mut map = Map::new(Some(64)).unwrap();
    let mut keys = Vec::new();
    for name in names { keys.push(TopicPartitionKey::new(Some(Text::from_raw(name.as_bytes(), &mut budget).unwrap()), 0, &mut budget).unwrap()); }
    assert_eq!(witness.calls, 0);
    for key in &keys[..8] { map.put(key.clone(), (), &mut Ops(&mut witness), &mut budget).unwrap(); }
    assert_eq!(witness.calls, 0);
    assert!(map.put(keys[8].clone(), (), &mut Ops(&mut witness), &mut budget).is_err());
    assert_eq!(witness.calls, 1);
}

#[test]
fn retained_payload_admits_every_ancestor_reservation_control() {
    use rtdb_types::java_hash_map::Admission;
    let root = MemoryBudget::new(1 << 20);
    let middle = root.scope();
    let leaf = middle.scope();
    let mut budget = Budget(leaf.clone());
    let lease = budget.reserve(7).unwrap();
    let expected = 7 + 3 * (std::mem::size_of::<rtdb_types::MemoryReservation>() + 4 * std::mem::size_of::<usize>());
    assert_eq!(root.used_bytes(), expected);
    assert_eq!(middle.used_bytes(), expected);
    assert_eq!(leaf.used_bytes(), expected);
    drop(lease);
    assert_eq!(root.used_bytes(), 0);
    assert_eq!(middle.used_bytes(), 0);
    assert_eq!(leaf.used_bytes(), 0);
}

#[test]
fn active_topic_trees_publish_full_request_order_and_retain_request_owners() {
    let memory = MemoryBudget::new(1 << 24);
    let mut p = producer(Arc::clone(&memory));
    let empty = p.snapshot();
    let names = ["AaAaAaAa", "AaAaAaBB", "AaAaBBAa", "AaAaBBBB", "AaBBAaAa", "AaBBAaBB", "AaBBBBAa", "AaBBBBBB", "BBAaAaAa", "BBAaAaBB", "BBAaBBAa"];
    p.add_topics(names, 0, &mut NoIdentityWitness).unwrap();
    let request = p.request(0).unwrap();
    assert_eq!(request.scope().kind(), RequestKind::Full);
    assert_eq!(request.topics().collect::<Vec<_>>(), [names[10]].into_iter().chain(names[..10].iter().copied()).collect::<Vec<_>>());
    p.add_topics([names[0]], 1, &mut NoIdentityWitness).unwrap();
    assert_eq!(p.request(1).unwrap().topics().collect::<Vec<_>>(), request.topics().collect::<Vec<_>>());
    drop(p);
    assert!(memory.used_bytes() > 0);
    assert_eq!(request.topics().count(), names.len());
    drop(request);
    assert!(memory.used_bytes() > 0);
    assert_eq!(empty.brokers().len(), 0);
    drop(empty);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn active_topic_tree_entries_expire_through_successful_response_application() {
    let mut p = producer(MemoryBudget::new(1 << 24));
    let names = ["AaAaAaAa", "AaAaAaBB", "AaAaBBAa", "AaAaBBBB", "AaBBAaAa", "AaBBAaBB", "AaBBBBAa", "AaBBBBBB", "BBAaAaAa", "BBAaAaBB", "BBAaBBAa", "BBAaBBBB", "BBBBAaAa", "BBBBAaBB", "BBBBBBAa", "BBBBBBBB"];
    p.add_topics(names, 0, &mut NoIdentityWitness).unwrap();
    let topics: Vec<_> = names.iter().enumerate().map(|(index, name)| topic(name.as_bytes(), index as u8 + 1, &[])).collect();
    assert!(matches!(apply(&mut p, 12, &topics, 0), Ok(ResponseAction::Published)));
    p.request_full(false).unwrap();
    assert_eq!(p.request(99).unwrap().topics().count(), names.len());
    assert!(matches!(apply(&mut p, 12, &topics, 100), Ok(ResponseAction::Published)));
    p.request_full(false).unwrap();
    assert_eq!(p.request(100).unwrap().topics().count(), 0);
    assert_eq!(p.snapshot().brokers().len(), 1);
}

mod bootstrap;
mod cluster_nodes;
mod random;
mod selection;
mod exchange;
mod updater;
mod wait;
mod fatal;
mod monitor;
mod topic_wait;
mod prepared_send;
