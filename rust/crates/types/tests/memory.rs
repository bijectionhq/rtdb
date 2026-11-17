use std::sync::Arc;
use rtdb_types::MemoryBudget;

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
