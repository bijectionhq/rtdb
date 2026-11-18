use std::sync::Arc;

use rtdb_types::{AllocationScope, MemoryBudget};

#[test]
fn scope_retains_ancestor_charges_and_rejects_growth_without_leaking() {
    let parent = MemoryBudget::new(8192);
    let child = parent.scope();
    {
        let scope = AllocationScope::new(&child).unwrap();
        scope.reserve(512).unwrap();
        let retained = child.used_bytes();
        assert_eq!(parent.used_bytes(), retained);
        assert_eq!(scope.reserve(8192).unwrap_err().code, "rtdb/memory-budget-exceeded");
        assert_eq!(scope.reserve(usize::MAX).unwrap_err().code, "rtdb.expr/allocation-size");
        assert_eq!(child.used_bytes(), retained);
        assert_eq!(parent.used_bytes(), retained);
    }
    assert_eq!(child.used_bytes(), 0);
    assert_eq!(parent.used_bytes(), 0);
}

#[test]
fn shared_scope_releases_only_after_its_last_caller_drops() {
    let memory = MemoryBudget::new(65536);
    let scope = Arc::new(AllocationScope::new(&memory).unwrap());
    let held = Arc::clone(&scope);
    std::thread::scope(|threads| {
        for _ in 0..8 {
            let scope = Arc::clone(&scope);
            threads.spawn(move || scope.reserve(32).unwrap());
        }
    });
    let retained = memory.used_bytes();
    assert!(retained > 4096);
    assert!(retained <= memory.limit_bytes());
    drop(scope);
    assert_eq!(memory.used_bytes(), retained);
    drop(held);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn rejected_scope_construction_releases_parent_admission() {
    let parent = MemoryBudget::new(4095);
    let child = parent.scope();
    assert!(matches!(AllocationScope::new(&child), Err(error) if error.code == "rtdb/memory-budget-exceeded"));
    assert_eq!(parent.used_bytes(), 0);
    assert_eq!(child.used_bytes(), 0);
}

#[test]
fn scratch_scope_charges_on_demand_and_releases_before_its_parent() {
    let memory = MemoryBudget::new(8192);
    let parent = AllocationScope::new(&memory).unwrap();
    parent.reserve(123).unwrap();
    let parent_bytes = memory.used_bytes();
    let block = memory.reserve(memory.limit_bytes() - parent_bytes).unwrap();
    let scratch = parent.scratch();
    assert_eq!(memory.used_bytes(), memory.limit_bytes());
    assert!(matches!(scratch.reserve(1), Err(error) if error.code == "rtdb/memory-budget-exceeded"));
    drop(block);
    scratch.reserve(1).unwrap();
    assert_eq!(memory.used_bytes(), parent_bytes + 257);
    drop(scratch);
    assert_eq!(memory.used_bytes(), parent_bytes);
    drop(parent);
    assert_eq!(memory.used_bytes(), 0);
}
