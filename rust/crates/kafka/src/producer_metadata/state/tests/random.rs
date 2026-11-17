use std::cell::Cell;

use super::*;

#[test]
fn bounded_draws_consume_rejected_candidates_and_unit_bounds() {
    let random = JavaRandom::from_seed(0);
    assert!(random.next_int(0).is_err());
    assert!(random.next_int(i32::MIN).is_err());
    let bounds = [1_073_741_825, 16, 1, i32::MAX, 2, 3, 8];
    assert_eq!(
        bounds.map(|bound| random.next_int(bound).unwrap()),
        [516_548_029, 9, 0, 663_681_053, 1, 0, 4],
    );
    let random = JavaRandom::from_seed(-42);
    assert_eq!(
        bounds.map(|bound| random.next_int(bound).unwrap()),
        [585_437_265, 6, 0, 1_510_017_756, 0, 0, 2],
    );
}

#[test]
fn default_constructor_advances_before_the_clock_and_retains_the_failed_advance() {
    let seeds = JavaRandomSeeds::new();
    let failed = seeds.create(|| {
        let intervening = seeds.create(|| Ok(125)).unwrap();
        assert_eq!([4, 3, 2, 4].map(|bound| intervening.next_int(bound).unwrap()), [3, 0, 1, 1]);
        Err(Anomaly::unsupported("fixture/clock", "Clock unavailable"))
    });
    assert!(failed.is_err());
    let random = seeds.create(|| Ok(-99)).unwrap();
    assert_eq!([4, 3, 2, 4].map(|bound| random.next_int(bound).unwrap()), [0, 0, 1, 2]);
}

#[test]
fn empty_cluster_acquires_the_random_used_by_later_bootstrap_generations() {
    let history = JavaRandomSeeds::new();
    let mut slot = None;
    let clock_calls = Cell::new(0);
    let memory = MemoryBudget::new(1 << 20);
    let mut shuffle = JavaShuffleRandom::new(&mut slot, &history, || {
        clock_calls.set(clock_calls.get() + 1);
        Ok(0)
    });
    let mut producer = ProducerMetadata::new_with_cluster_nodes(
        Settings { refresh_backoff_ms: 10, refresh_backoff_max_ms: 10, metadata_expire_ms: 1000, metadata_idle_ms: 100 },
        memory.clone(),
        &mut Observations,
        &mut NoIdentityWitness,
        &mut shuffle,
    ).unwrap();
    assert_eq!(clock_calls.get(), 1);
    assert_eq!(producer.snapshot().cluster_nodes().unwrap().len(), 0);
    let addresses = producer.prepare_bootstrap_addresses(&[
        ResolvedEndpoint { host: "a", port: 9092 },
        ResolvedEndpoint { host: "b", port: 9092 },
        ResolvedEndpoint { host: "c", port: 9092 },
        ResolvedEndpoint { host: "d", port: 9092 },
    ]).unwrap();
    producer.bootstrap(addresses, &mut shuffle, &mut NoIdentityWitness).unwrap();
    let first = producer.snapshot();
    assert_eq!(first.cluster_nodes().unwrap().map(|node| node.id).collect::<Vec<_>>(), [-3, -2, -1, -4]);
    producer.rebootstrap(&mut shuffle, &mut NoIdentityWitness).unwrap();
    assert_eq!(producer.snapshot().cluster_nodes().unwrap().map(|node| node.id).collect::<Vec<_>>(), [-3, -4, -2, -1]);
    assert_eq!(clock_calls.get(), 1);
    assert_eq!(first.cluster_nodes().unwrap().map(|node| node.id).collect::<Vec<_>>(), [-3, -2, -1, -4]);
    drop(first);
    drop(producer);
    assert_eq!(memory.used_bytes(), 0);
}
