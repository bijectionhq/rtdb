use rtdb_expr::EvaluationScope;
use rtdb_types::{AllocationScope, MemoryBudget};

#[test]
fn evaluation_scope_is_the_shared_allocation_owner() {
    let memory = MemoryBudget::new(8192);
    let expression: EvaluationScope = AllocationScope::new(&memory).unwrap();
    let shared: &AllocationScope = &expression;
    shared.reserve(512).unwrap();
    assert!(memory.used_bytes() > 512);
    drop(expression);
    assert_eq!(memory.used_bytes(), 0);
}
