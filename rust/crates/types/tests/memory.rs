use std::sync::Arc;
use rtdb_types::{AnomalyKind, MemoryBudget};

#[test]
fn shrinking_a_lease_releases_every_ancestor_without_changing_peaks() {
    let root = MemoryBudget::new(100);
    let child = root.scope();
    let leaf = child.scope();
    let mut reservation = leaf.reserve(80).unwrap();
    assert_eq!(Arc::get_mut(&mut reservation).unwrap().shrink_to(81).unwrap_err().code, "rtdb/reservation-growth");
    assert_eq!(reservation.bytes(), 80);
    for budget in [&root, &child, &leaf] {
        assert_eq!(budget.used_bytes(), 80);
        assert_eq!(budget.peak_bytes(), 80);
    }
    Arc::get_mut(&mut reservation).unwrap().shrink_to(50).unwrap();
    assert_eq!(reservation.bytes(), 50);
    for budget in [&root, &child, &leaf] {
        assert_eq!(budget.used_bytes(), 50);
        assert_eq!(budget.peak_bytes(), 80);
    }
    let retained = Arc::clone(&reservation);
    assert!(Arc::get_mut(&mut reservation).is_none());
    drop(reservation);
    drop(leaf);
    assert_eq!(root.used_bytes(), 50);
    drop(retained);
    assert_eq!(root.used_bytes(), 0);
    assert_eq!(child.used_bytes(), 0);
}

#[test]
fn shrinking_to_zero_retains_the_budget_identity_until_the_lease_drops() {
    let root = MemoryBudget::new(20);
    let child = root.scope();
    let mut reservation = child.reserve(20).unwrap();
    Arc::get_mut(&mut reservation).unwrap().shrink_to(0).unwrap();
    Arc::get_mut(&mut reservation).unwrap().shrink_to(0).unwrap();
    assert!(Arc::ptr_eq(reservation.budget(), &child));
    assert_eq!(root.used_bytes(), 0);
    assert_eq!(child.used_bytes(), 0);
    assert_eq!(root.peak_bytes(), 20);
    let replacement = root.reserve(20).unwrap();
    drop(reservation);
    assert_eq!(root.used_bytes(), 20);
    drop(replacement);
    assert_eq!(root.used_bytes(), 0);
}

#[test]
fn growing_a_lease_charges_only_its_delta_through_every_ancestor() {
    let root = MemoryBudget::new(100);
    let child = root.scope();
    let leaf = child.scope();
    let mut reservation = leaf.reserve(80).unwrap();
    Arc::get_mut(&mut reservation).unwrap().grow_to(100).unwrap();
    Arc::get_mut(&mut reservation).unwrap().grow_to(100).unwrap();
    assert_eq!(reservation.bytes(), 100);
    assert!(Arc::ptr_eq(reservation.budget(), &leaf));
    for budget in [&root, &child, &leaf] {
        assert_eq!(budget.used_bytes(), 100);
        assert_eq!(budget.peak_bytes(), 100);
    }
    let alias = Arc::clone(&reservation);
    assert!(Arc::get_mut(&mut reservation).is_none());
    drop(alias);
    Arc::get_mut(&mut reservation).unwrap().shrink_to(0).unwrap();
    Arc::get_mut(&mut reservation).unwrap().grow_to(25).unwrap();
    for budget in [&root, &child, &leaf] {
        assert_eq!(budget.used_bytes(), 25);
        assert_eq!(budget.peak_bytes(), 100);
    }
    drop(reservation);
    for budget in [&root, &child, &leaf] { assert_eq!(budget.used_bytes(), 0); }
}

#[test]
fn denied_growth_preserves_lease_and_ancestor_counts_until_capacity_returns() {
    let root = MemoryBudget::new(100);
    let child = root.scope();
    let leaf = child.scope();
    let sibling = root.scope();
    let competing = sibling.reserve(60).unwrap();
    let mut reservation = leaf.reserve(30).unwrap();
    assert_eq!(Arc::get_mut(&mut reservation).unwrap().grow_to(41).unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(Arc::get_mut(&mut reservation).unwrap().grow_to(29).unwrap_err().code, "rtdb/reservation-shrink");
    assert_eq!(reservation.bytes(), 30);
    assert_eq!(root.used_bytes(), 90);
    assert_eq!(sibling.used_bytes(), 60);
    for budget in [&child, &leaf] { assert_eq!(budget.used_bytes(), 30); }
    drop(competing);
    Arc::get_mut(&mut reservation).unwrap().grow_to(100).unwrap();
    drop(reservation);
    for budget in [&root, &child, &leaf, &sibling] { assert_eq!(budget.used_bytes(), 0); }
}

#[test]
fn growing_a_lease_rejects_aggregate_overflow_without_releasing_its_original_bytes() {
    let root = MemoryBudget::new(usize::MAX);
    let child = root.scope();
    let mut reservation = child.reserve(usize::MAX - 8).unwrap();
    let competing = root.reserve(6).unwrap();
    assert_eq!(Arc::get_mut(&mut reservation).unwrap().grow_to(usize::MAX).unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(reservation.bytes(), usize::MAX - 8);
    assert_eq!(root.used_bytes(), usize::MAX - 2);
    assert_eq!(child.used_bytes(), usize::MAX - 8);
    drop(competing);
    Arc::get_mut(&mut reservation).unwrap().grow_to(usize::MAX).unwrap();
    drop(reservation);
    assert_eq!(root.used_bytes(), 0);
    assert_eq!(child.used_bytes(), 0);
}

#[test]
fn competing_exclusive_lease_growth_cannot_overdraw_a_shared_ancestor() {
    let root = MemoryBudget::new(160);
    let start = Arc::new(std::sync::Barrier::new(8));
    let leases = (0..8).map(|_| root.scope().reserve(10).unwrap()).collect::<Vec<_>>();
    let workers = leases.into_iter().map(|mut lease| {
        let start = Arc::clone(&start);
        std::thread::spawn(move || {
            start.wait();
            let result = Arc::get_mut(&mut lease).unwrap().grow_to(30);
            (lease, result)
        })
    }).collect::<Vec<_>>();
    let results = workers.into_iter().map(|worker| worker.join().unwrap()).collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|(_, result)| result.is_ok()).count(), 4);
    assert_eq!(root.used_bytes(), 160);
    assert_eq!(root.peak_bytes(), 160);
    for (lease, result) in &results {
        assert_eq!(lease.bytes(), if result.is_ok() { 30 } else { 10 });
        assert_eq!(lease.budget().used_bytes(), lease.bytes());
        if let Err(error) = result { assert_eq!(error.code, "rtdb/memory-budget-exceeded"); }
    }
    drop(results);
    assert_eq!(root.used_bytes(), 0);
}

#[test]
fn reservation_control_count_follows_nested_and_sibling_owners_without_changing_them() {
    let root = MemoryBudget::new(1000);
    let child = root.scope();
    let leaf = child.scope();
    let sibling = root.scope();
    let reservation = leaf.reserve(13).unwrap();
    let retained = Arc::clone(&reservation);
    let references = [Arc::strong_count(&root), Arc::strong_count(&child), Arc::strong_count(&leaf), Arc::strong_count(&sibling)];
    assert_eq!(root.reservation_lease_count().unwrap(), 1);
    assert_eq!(child.reservation_lease_count().unwrap(), 2);
    assert_eq!(leaf.reservation_lease_count().unwrap(), 3);
    assert_eq!(sibling.reservation_lease_count().unwrap(), 2);
    assert_eq!(references, [Arc::strong_count(&root), Arc::strong_count(&child), Arc::strong_count(&leaf), Arc::strong_count(&sibling)]);
    for budget in [&root, &child, &leaf] { assert_eq!((budget.used_bytes(), budget.peak_bytes()), (13, 13)); }
    assert_eq!((sibling.used_bytes(), sibling.peak_bytes()), (0, 0));
    drop(reservation);
    assert_eq!(root.used_bytes(), 13);
    assert_eq!(retained.budget().reservation_lease_count().unwrap(), 3);
    drop(retained);
    assert_eq!(root.used_bytes(), 0);
    let empty = leaf.reserve(0).unwrap();
    assert_eq!(empty.budget().reservation_lease_count().unwrap(), 3);
    drop(empty);
    assert_eq!(root.used_bytes(), 0);
}

#[test]
fn a_caller_control_allowance_is_admitted_with_payload_before_reservation_construction() {
    for limit in [199, 200] {
        let root = MemoryBudget::new(limit);
        let child = root.scope();
        let leaf = child.scope();
        let controls = leaf.reservation_lease_count().unwrap();
        let required = 8 + controls * 64;
        assert_eq!(required, 200);
        let reservation = leaf.reserve(required);
        if limit == 199 {
            assert_eq!(reservation.unwrap_err().code, "rtdb/memory-budget-exceeded");
            for budget in [&root, &child, &leaf] { assert_eq!(budget.used_bytes(), 0); }
        } else {
            let reservation = reservation.unwrap();
            for budget in [&root, &child, &leaf] { assert_eq!(budget.used_bytes(), required); }
            let retained = Arc::clone(&reservation);
            drop(reservation);
            assert_eq!(root.used_bytes(), required);
            drop(retained);
            for budget in [&root, &child, &leaf] { assert_eq!(budget.used_bytes(), 0); }
        }
    }
}


#[test]
fn moving_a_charge_does_not_require_headroom() {
    let memory = rtdb_types::MemoryBudget::new(128);
    let mut destination = memory.reserve(80).unwrap();
    let mut source = memory.reserve(48).unwrap();
    std::sync::Arc::get_mut(&mut destination).unwrap().absorb(std::sync::Arc::get_mut(&mut source).unwrap()).unwrap();
    assert_eq!((destination.bytes(), source.bytes(), memory.used_bytes()), (128, 0, 128));
    drop(source);
    assert_eq!(memory.used_bytes(), 128);
    drop(destination);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn every_refusal_by_a_budget_is_resource_exhaustion() {
    let root = MemoryBudget::new(100);
    let leaf = root.scope_with_limit(50).scope();
    let mut reservation = leaf.reserve(10).unwrap();
    let refused_growth = Arc::get_mut(&mut reservation).unwrap().grow_to(51).unwrap_err();
    drop(reservation);
    for error in [leaf.reserve(51).unwrap_err(), root.reserve(usize::MAX).unwrap_err(), refused_growth] {
        assert!(error.is(AnomalyKind::ResourceExhausted), "{error}");
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    }
    assert_eq!(root.used_bytes(), 0);
    assert!(!Arc::get_mut(&mut root.reserve(10).unwrap()).unwrap().shrink_to(11).unwrap_err().is(AnomalyKind::ResourceExhausted));
}
