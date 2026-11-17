use super::*;

#[derive(Debug, PartialEq, Eq)]
enum ShuffleCall { Begin, Next(i32) }
struct Shuffle { draws: std::collections::VecDeque<i32>, calls: Vec<ShuffleCall>, begin_bytes: Option<usize> }
impl Shuffle { fn new(draws: impl IntoIterator<Item = i32>) -> Self { Self { draws: draws.into_iter().collect(), calls: Vec::new(), begin_bytes: None } } }
impl JavaShuffleObservations for Shuffle {
    fn begin(&mut self, admit: &mut ShuffleAdmission<'_>) -> rtdb_types::Result<()> {
        self.calls.push(ShuffleCall::Begin);
        if let Some(bytes) = self.begin_bytes { let _lease = admit(bytes)?; }
        Ok(())
    }
    fn next_int(&mut self, bound: i32, _: &mut ShuffleAdmission<'_>) -> rtdb_types::Result<i32> {
        self.calls.push(ShuffleCall::Next(bound));
        self.draws.pop_front().ok_or_else(|| Anomaly::unsupported("fixture/shuffle-exhausted", "Source shuffle observations are exhausted"))
    }
}
fn fresh(memory: Arc<MemoryBudget>) -> ProducerMetadata {
    ProducerMetadata::new(Settings { refresh_backoff_ms: 10, refresh_backoff_max_ms: 10, metadata_expire_ms: 1000, metadata_idle_ms: 100 }, memory, &mut Observations, &mut NoIdentityWitness).unwrap()
}
fn addresses(p: &mut ProducerMetadata) -> BootstrapAddresses {
    p.prepare_bootstrap_addresses(&[
        ResolvedEndpoint { host: "first.example", port: 0 },
        ResolvedEndpoint { host: "127.0.0.1", port: 9092 },
        ResolvedEndpoint { host: "::1", port: u16::MAX },
    ]).unwrap()
}

#[test]
fn bootstrap_preserves_negative_map_ids_and_independent_source_shuffled_cluster_order() {
    let memory = MemoryBudget::new(1 << 20);
    let mut p = fresh(memory.clone());
    let addresses = addresses(&mut p);
    let mut shuffle = Shuffle::new([0, 0]);
    p.bootstrap(addresses.clone(), &mut shuffle, &mut NoIdentityWitness).unwrap();
    assert_eq!(p.refresh().update_version(), 1);
    assert_eq!(p.request(0).unwrap().scope().kind(), RequestKind::Full);
    let snapshot = p.snapshot();
    assert_eq!(snapshot.brokers().map(|node| (node.id, node.host.as_str(), node.port)).collect::<Vec<_>>(), [(-1, "first.example", 0), (-2, "127.0.0.1", 9092), (-3, "::1", 65535)]);
    assert_eq!(snapshot.bootstrap_cluster_nodes().unwrap().map(|node| node.id).collect::<Vec<_>>(), [-2, -3, -1]);
    assert!(snapshot.is_bootstrap_configured());
    assert!(snapshot.controller().is_none());
    assert!(snapshot.cluster_id().is_none());
    assert_eq!(snapshot.partition_metadata().count(), 0);
    assert_eq!(snapshot.internal_topics().count(), 0);
    assert_eq!(snapshot.unauthorized_topics().count(), 0);
    assert_eq!(snapshot.invalid_topics().count(), 0);
    assert_eq!(shuffle.calls, [ShuffleCall::Begin, ShuffleCall::Next(3), ShuffleCall::Next(2)]);
    assert!(p.bootstrap_addresses().unwrap().same_object(&addresses));
    let map_first = snapshot.brokers().find(|node| node.id == -1).unwrap();
    let cluster_first = snapshot.bootstrap_cluster_nodes().unwrap().find(|node| node.id == -1).unwrap();
    assert!(!std::ptr::eq(map_first, cluster_first));
    drop(p); drop(addresses); assert!(memory.used_bytes() > 0);
    assert_eq!(snapshot.brokers().count(), 3);
    drop(snapshot); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn bootstrap_demands_random_acquisition_for_empty_and_singleton_lists_without_next_int() {
    for source in [Vec::new(), vec![ResolvedEndpoint { host: "one.example", port: 9092 }]] {
        let mut p = fresh(MemoryBudget::new(1 << 20));
        let addresses = p.prepare_bootstrap_addresses(&source).unwrap();
        let mut shuffle = Shuffle::new([]);
        p.bootstrap(addresses, &mut shuffle, &mut NoIdentityWitness).unwrap();
        assert_eq!(shuffle.calls, [ShuffleCall::Begin]);
        assert_eq!(p.snapshot().bootstrap_cluster_nodes().unwrap().len(), source.len());
        assert!(p.snapshot().controller().is_none());
    }
}

#[test]
fn unavailable_shuffle_profile_keeps_old_snapshot_after_the_source_refresh_prefix() {
    let mut p = fresh(MemoryBudget::new(1 << 20));
    let before = p.snapshot();
    let empty = p.prepare_bootstrap_addresses(&[]).unwrap();
    let cause = p.bootstrap(empty, &mut MissingShuffleObservations, &mut NoIdentityWitness).unwrap_err();
    assert!(matches!(cause, Failure::Native(cause) if cause.code == "rtdb/kafka-java-shuffle-required"));
    assert_eq!(p.refresh().update_version(), 1);
    assert!(p.snapshot().0.same_object(&before.0));
    assert!(p.bootstrap_addresses().is_none());
    assert!(matches!(p.request(0), Err(Failure::CapabilityFailed)));
}

#[test]
fn exhausted_shuffle_observations_preserve_the_prior_generation_addresses_and_original_error() {
    let memory = MemoryBudget::new(1 << 20);
    let mut p = fresh(memory.clone());
    let old = p.prepare_bootstrap_addresses(&[ResolvedEndpoint { host: "old.example", port: 1 }]).unwrap();
    p.bootstrap(old.clone(), &mut Shuffle::new([]), &mut NoIdentityWitness).unwrap();
    let snapshot = p.snapshot();
    let replacement = addresses(&mut p);
    let before = memory.used_bytes();
    let mut shuffle = Shuffle::new([0]);
    let cause = p.bootstrap(replacement.clone(), &mut shuffle, &mut NoIdentityWitness).unwrap_err();
    assert!(matches!(cause, Failure::Native(cause) if cause.code == "fixture/shuffle-exhausted"));
    assert_eq!(p.refresh().update_version(), 2);
    assert!(p.snapshot().0.same_object(&snapshot.0));
    assert!(p.bootstrap_addresses().unwrap().same_object(&old));
    assert_eq!(shuffle.calls, [ShuffleCall::Begin, ShuffleCall::Next(3), ShuffleCall::Next(2)]);
    assert_eq!(memory.used_bytes(), before);
    assert!(matches!(p.request(0), Err(Failure::CapabilityFailed)));
    drop(p); drop(old); drop(snapshot); drop(replacement); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn out_of_range_shuffle_observations_fail_instead_of_selecting_a_native_fallback() {
    for selected in [-1, 3] {
        let mut p = fresh(MemoryBudget::new(1 << 20));
        let addresses = addresses(&mut p);
        let mut shuffle = Shuffle::new([selected]);
        let cause = p.bootstrap(addresses, &mut shuffle, &mut NoIdentityWitness).unwrap_err();
        assert!(matches!(cause, Failure::Native(cause) if cause.code == "rtdb/kafka-java-shuffle-range"));
        assert_eq!(shuffle.calls, [ShuffleCall::Begin, ShuffleCall::Next(3)]);
        assert_eq!(p.refresh().update_version(), 1);
        assert!(!p.snapshot().is_bootstrap_configured());
    }
}

#[test]
fn snapshot_admission_failure_keeps_the_refresh_prefix_and_old_addresses_before_random_acquisition() {
    let memory = MemoryBudget::new(1 << 20);
    let mut p = fresh(memory.clone());
    let old = p.prepare_bootstrap_addresses(&[ResolvedEndpoint { host: "old.example", port: 1 }]).unwrap();
    p.bootstrap(old.clone(), &mut Shuffle::new([]), &mut NoIdentityWitness).unwrap();
    let snapshot = p.snapshot();
    let replacement = addresses(&mut p);
    let before = memory.used_bytes();
    let blocker = memory.reserve(memory.limit_bytes() - before).unwrap();
    let mut shuffle = Shuffle::new([0, 0]);
    assert!(matches!(p.bootstrap(replacement.clone(), &mut shuffle, &mut NoIdentityWitness), Err(Failure::Native(_))));
    assert_eq!(p.refresh().update_version(), 2);
    assert!(shuffle.calls.is_empty());
    assert!(p.snapshot().0.same_object(&snapshot.0));
    assert!(p.bootstrap_addresses().unwrap().same_object(&old));
    drop(blocker); assert_eq!(memory.used_bytes(), before);
    drop(p); drop(old); drop(snapshot); drop(replacement); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn shuffle_authority_scratch_uses_the_producer_budget_and_preserves_its_original_denial() {
    let memory = MemoryBudget::new(1 << 20);
    let mut p = fresh(memory.clone());
    let addresses = p.prepare_bootstrap_addresses(&[]).unwrap();
    let mut shuffle = Shuffle::new([]);
    shuffle.begin_bytes = Some(memory.limit_bytes());
    let cause = p.bootstrap(addresses, &mut shuffle, &mut NoIdentityWitness).unwrap_err();
    assert!(matches!(cause, Failure::Native(cause) if cause.code == "rtdb/memory-budget-exceeded"));
    assert_eq!(shuffle.calls, [ShuffleCall::Begin]);
    assert_eq!(p.refresh().update_version(), 1);
    assert!(p.bootstrap_addresses().is_none());
}

#[test]
fn rebootstrap_before_addresses_has_its_source_prefix_and_allows_later_bootstrap() {
    let mut p = fresh(MemoryBudget::new(1 << 20));
    let mut shuffle = Shuffle::new([]);
    assert!(matches!(p.rebootstrap(&mut shuffle, &mut NoIdentityWitness), Err(Failure::MissingBootstrapAddresses)));
    assert_eq!(p.refresh().update_version(), 1);
    assert!(shuffle.calls.is_empty());
    let addresses = p.prepare_bootstrap_addresses(&[]).unwrap();
    p.bootstrap(addresses, &mut shuffle, &mut NoIdentityWitness).unwrap();
    assert_eq!(p.refresh().update_version(), 2);
    assert!(p.snapshot().is_bootstrap_configured());
}

#[test]
fn bootstrap_rejects_an_unrelated_budget_domain_after_the_source_refresh_prefix() {
    let mut source = fresh(MemoryBudget::new(1 << 20));
    let addresses = addresses(&mut source);
    let mut destination = fresh(MemoryBudget::new(1 << 20));
    let mut shuffle = Shuffle::new([0, 0]);
    let cause = destination.bootstrap(addresses, &mut shuffle, &mut NoIdentityWitness).unwrap_err();
    assert!(matches!(cause, Failure::Native(cause) if cause.code == "rtdb/kafka-bootstrap-budget-domain"));
    assert_eq!(destination.refresh().update_version(), 1);
    assert!(shuffle.calls.is_empty());
    assert!(destination.bootstrap_addresses().is_none());
}

#[test]
fn ordinary_response_keeps_negative_controller_resolution_separate_from_bootstrap() {
    let mut p = fresh(MemoryBudget::new(1 << 20));
    let mut wire = Wire { version: 12, bytes: Vec::new() };
    wire.i32(0); wire.count(1); wire.i32(-1); wire.string(Some(b"negative.example")); wire.i32(9092); wire.string(None); wire.tags();
    wire.string(Some(b"cluster")); wire.i32(-1); wire.count(0); wire.tags();
    let scope = p.request(0).unwrap().scope();
    p.apply(RawMetadataView::parse(12, &wire.bytes).unwrap(), scope, 0, false, &mut NoIdentityWitness).unwrap();
    assert_eq!(p.snapshot().controller().unwrap().id, -1);
    assert!(!p.snapshot().is_bootstrap_configured());
    assert!(p.snapshot().bootstrap_cluster_nodes().is_none());
}

#[test]
fn rebootstrap_reuses_resolved_addresses_and_preserves_topics_epochs_errors_backoff_and_counters() {
    let memory = MemoryBudget::new(1 << 24);
    let mut p = fresh(memory);
    let addresses = p.prepare_bootstrap_addresses(&[ResolvedEndpoint { host: "old.example", port: 9092 }]).unwrap();
    p.bootstrap(addresses.clone(), &mut Shuffle::new([]), &mut NoIdentityWitness).unwrap();
    p.add_topics(["t", "a"], 0, &mut NoIdentityWitness).unwrap();
    let denied = Topic { name: Some(b"a"), id: 2, error: 29, internal: false, partitions: &[] };
    let partitions = [(5, 1, 5, 0), (1, 1, 5, 0)];
    apply(&mut p, 12, &[topic(b"t", 1, &partitions), denied], 7).unwrap();
    assert_eq!(p.select_send("t", ()).unwrap().partition_id(), 5);
    p.add_topic("new", 8, &mut NoIdentityWitness).unwrap();
    p.failed_response(9).unwrap();
    let before = (p.epochs.len(), p.active.iter().len(), p.new_topics.len(), p.counters.len(), p.invalid.len(), p.unauthorized.len(), p.refresh().last_successful_ms());
    let delay = p.time_to_next_update(10, &mut Observations).unwrap();
    let version = p.refresh().update_version();
    let old = p.snapshot();
    let mut shuffle = Shuffle::new([]);
    p.rebootstrap(&mut shuffle, &mut NoIdentityWitness).unwrap();
    assert_eq!(p.refresh().update_version(), version.wrapping_add(1));
    assert_eq!((p.epochs.len(), p.active.iter().len(), p.new_topics.len(), p.counters.len(), p.invalid.len(), p.unauthorized.len(), p.refresh().last_successful_ms()), before);
    assert_eq!(p.topic_error("a"), Some(29));
    assert_eq!(p.time_to_next_update(10, &mut Observations).unwrap(), delay);
    assert!(p.bootstrap_addresses().unwrap().same_object(&addresses));
    assert!(p.snapshot().is_bootstrap_configured());
    assert_eq!(old.partition("t", 5).unwrap().epoch(), Some(5));
    assert!(p.snapshot().partitions("t").is_empty());
    assert_eq!(shuffle.calls, [ShuffleCall::Begin]);
    apply(&mut p, 12, &[topic(b"t", 1, &partitions)], 11).unwrap();
    assert_eq!(p.select_send("t", ()).unwrap().partition_id(), 1);
    assert!(!p.snapshot().is_bootstrap_configured());
}

#[test]
fn bootstrap_keeps_closed_metadata_closed_without_adding_a_source_close_guard() {
    let mut p = fresh(MemoryBudget::new(1 << 20));
    p.close();
    let addresses = p.prepare_bootstrap_addresses(&[]).unwrap();
    p.bootstrap(addresses, &mut Shuffle::new([]), &mut NoIdentityWitness).unwrap();
    assert!(p.refresh().is_closed());
    assert_eq!(p.refresh().update_version(), 1);
    assert!(p.snapshot().is_bootstrap_configured());
}

#[test]
fn resolved_address_preparation_denial_precedes_bootstrap_prefix_and_endpoint_construction() {
    for source in [Vec::new(), vec![ResolvedEndpoint { host: "not-copied.example", port: 9092 }]] {
        let memory = MemoryBudget::new(1 << 20);
        let mut p = fresh(memory.clone());
        let old = p.snapshot();
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        assert!(matches!(p.prepare_bootstrap_addresses(&source), Err(Failure::Native(_))));
        assert_eq!(p.refresh().update_version(), 0);
        assert!(p.snapshot().0.same_object(&old.0));
        assert!(p.bootstrap_addresses().is_none());
        assert!(matches!(p.request(0), Err(Failure::CapabilityFailed)));
        drop(blocker); drop(p); drop(old); assert_eq!(memory.used_bytes(), 0);
    }
}
