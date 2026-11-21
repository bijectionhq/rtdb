use rtdb_types::MemoryBudget;

#[test]
fn a_quantized_child_admits_against_its_parent_in_whole_quanta_and_returns_them_as_it_frees() {
    let parent = MemoryBudget::new(1 << 20);
    let child = parent.quantized(1024);
    let small = child.reserve(10).unwrap();
    assert_eq!((child.used_bytes(), parent.used_bytes()), (10, 1024));
    let more = (0..100).map(|_| child.reserve(10).unwrap()).collect::<Vec<_>>();
    assert_eq!((child.used_bytes(), parent.used_bytes()), (1010, 1024), "reservations within a quantum leave the parent alone");
    let large = child.reserve(5000).unwrap();
    assert_eq!(parent.used_bytes(), 6144);
    drop(large);
    assert!(parent.used_bytes() <= 3 * 1024, "freed quanta beyond the slack go back to the parent: {}", parent.used_bytes());
    drop((small, more));
    assert_eq!(child.used_bytes(), 0);
    drop(child);
    assert_eq!(parent.used_bytes(), 0);
}

#[test]
fn a_quantized_child_refuses_what_its_parent_cannot_grant_and_keeps_its_count() {
    let parent = MemoryBudget::new(2048);
    let child = parent.quantized(1024);
    let held = child.reserve(1500).unwrap();
    assert_eq!(child.reserve(1000).unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!((child.used_bytes(), parent.used_bytes()), (1500, 2048));
    drop(held);
    drop(child);
    assert_eq!(parent.used_bytes(), 0);
}

#[test]
fn a_quantized_child_rolls_back_a_grant_overflow_and_can_reserve_again() {
    let parent = MemoryBudget::new(usize::MAX);
    let quantum = usize::MAX - 1;
    let child = parent.quantized(quantum);

    assert_eq!(child.reserve(usize::MAX).unwrap_err().code, "rtdb/allocation-overflow");
    assert_eq!((child.used_bytes(), parent.used_bytes()), (0, 0));

    let held = child.reserve(quantum).unwrap();
    assert_eq!(child.reserve(1).unwrap_err().code, "rtdb/allocation-overflow");
    assert_eq!((child.used_bytes(), parent.used_bytes()), (quantum, quantum));

    drop(held);
    assert_eq!(child.used_bytes(), 0);
    drop(child);
    assert_eq!(parent.used_bytes(), 0);
}
