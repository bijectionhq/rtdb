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
