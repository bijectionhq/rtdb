use std::{panic::{AssertUnwindSafe, catch_unwind}, sync::Arc};
use rtdb_types::MemoryBudget;
use super::*;
use super::super::with_prediction_contexts;

fn fixture<R>(limit: usize, run: impl for<'id, 'g, 'scope> FnOnce(&mut PredictionContexts<'id, 'g, 'scope>, &Arc<MemoryBudget>) -> Result<R>) -> R {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar_work = AllocationScope::new(&grammar_memory).unwrap();
    let grammar = Grammar::load(&grammar_work).unwrap();
    let memory = MemoryBudget::new(limit);
    let work = AllocationScope::new(&memory).unwrap();
    let result = with_prediction_contexts(&grammar, &work, |owner| run(owner, &memory)).unwrap();
    drop(work);
    assert_eq!(memory.used_bytes(), 0);
    result
}
fn array<'id>(owner: &mut PredictionContexts<'id, '_, '_>, parents: &[Option<ContextKey<'id>>]) -> Result<ContextKey<'id>> {
    let mut candidate = owner.candidate(parents.len())?;
    for (index, parent) in parents.iter().enumerate() { candidate.edges.push(Edge { parent: *parent, address: ReturnAddress::Follow(index as u32 + 1) }); }
    owner.array(&candidate)
}
fn nested<'id>(owner: &mut PredictionContexts<'id, '_, '_>, mut parent: ContextKey<'id>, depth: usize) -> Result<ContextKey<'id>> {
    let empty = owner.empty();
    for _ in 0..depth { parent = array(owner, &[Some(parent), Some(empty)])?; }
    Ok(parent)
}
fn clear_links(owner: &PredictionContexts<'_, '_, '_>) -> bool {
    owner.store.nodes.values().all(|node| match &node.shape { Shape::Array { continuation, .. } => continuation.is_none(), _ => true })
}
fn linked<'id>(owner: &mut PredictionContexts<'id, '_, '_>, left: ContextKey<'id>, right: ContextKey<'id>) -> Result<bool> {
    owner.equivalent_using(left, right, EqualityPolicy::LinkedFrames)
}

#[test]
fn linked_and_scoped_policies_preserve_identity_class_null_and_parent_order() {
    fixture(4 << 20, |owner, _| {
        let empty = owner.empty();
        let null_a = owner.singleton_edge(Edge { parent: None, address: ReturnAddress::Follow(1) })?;
        let null_b = owner.singleton_edge(Edge { parent: None, address: ReturnAddress::Follow(1) })?;
        let single = owner.singleton(empty, owner.grammar().state(1)?)?;
        let a = array(owner, &[Some(empty)])?;
        let b = array(owner, &[Some(empty)])?;
        let nullable_a = array(owner, &[None, Some(empty)])?;
        let nullable_b = array(owner, &[None, Some(empty)])?;
        let reordered = array(owner, &[Some(empty), None])?;
        for (left, right, expected) in [(empty, empty, true), (null_a, null_a, true), (null_a, null_b, false), (null_a, empty, false), (single, a, false), (a, b, true), (nullable_a, nullable_b, true), (nullable_a, reordered, false)] {
            assert_eq!(owner.equivalent(left, right)?, expected);
            assert_eq!(linked(owner, left, right)?, expected);
            assert_eq!(linked(owner, right, left)?, expected);
            assert!(clear_links(owner));
        }
        Ok(())
    });
}

#[test]
fn shared_left_nodes_can_be_revisited_against_different_right_nodes() {
    fixture(16 << 20, |owner, _| {
        let empty = owner.empty();
        let left_leaf = owner.singleton_edge(Edge { parent: None, address: ReturnAddress::Follow(3) })?;
        let other_leaf = owner.singleton_edge(Edge { parent: None, address: ReturnAddress::Follow(3) })?;
        let left = nested(owner, left_leaf, 96)?;
        let right_a = nested(owner, left_leaf, 96)?;
        let right_b = nested(owner, other_leaf, 96)?;
        let repeated = array(owner, &[Some(left), Some(left), Some(empty)])?;
        let equal = array(owner, &[Some(right_a), Some(right_a), Some(empty)])?;
        let unequal = array(owner, &[Some(right_a), Some(right_b), Some(empty)])?;
        assert_eq!(owner.hash(repeated), owner.hash(unequal));
        assert!(linked(owner, repeated, equal)?);
        assert!(!linked(owner, repeated, unequal)?);
        assert!(clear_links(owner));
        assert!(linked(owner, repeated, equal)?);
        Ok(())
    });
}

#[test]
fn linked_false_does_not_clear_or_grow_the_scoped_frame_residue() {
    fixture(8 << 20, |owner, _| {
        let a = owner.singleton_edge(Edge { parent: None, address: ReturnAddress::Follow(3) })?;
        let b = owner.singleton_edge(Edge { parent: None, address: ReturnAddress::Follow(3) })?;
        let a = nested(owner, a, 64)?;
        let b = nested(owner, b, 64)?;
        assert!(!owner.equivalent(a, b)?);
        let residue = (owner.equality.len(), owner.equality.capacity());
        assert_eq!(residue.0, 64);
        assert!(!linked(owner, a, b)?);
        assert_eq!((owner.equality.len(), owner.equality.capacity()), residue);
        assert!(clear_links(owner));
        assert!(owner.equivalent(owner.empty(), owner.empty())?);
        assert_eq!(owner.equality.len(), 0);
        Ok(())
    });
}

#[test]
fn linked_true_and_deep_false_need_no_admission_at_full_budget() {
    fixture(16 << 20, |owner, memory| {
        let a = owner.singleton_edge(Edge { parent: None, address: ReturnAddress::Follow(3) })?;
        let b = owner.singleton_edge(Edge { parent: None, address: ReturnAddress::Follow(3) })?;
        let left = nested(owner, a, 256)?;
        let right = nested(owner, a, 256)?;
        let false_right = nested(owner, b, 256)?;
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        let before = memory.used_bytes();
        assert!(linked(owner, left, right)?);
        assert!(!linked(owner, left, false_right)?);
        assert_eq!(memory.used_bytes(), before);
        assert_eq!(owner.equality.capacity(), 0);
        assert!(clear_links(owner));
        drop(blocker);
        Ok(())
    });
}

#[test]
fn linked_session_clears_its_chain_on_error_and_unwind() {
    fixture(4 << 20, |owner, _| {
        let empty = owner.empty();
        let child = array(owner, &[Some(empty), Some(empty)])?;
        let root = array(owner, &[Some(child), Some(empty)])?;
        let result: Result<()> = (|| {
            let mut session = EqualitySession::new(owner, EqualityPolicy::LinkedFrames);
            session.save(root, root, 1)?;
            session.save(child, child, 1)?;
            Err(PredictionError::Artifact(Problem::Context))
        })();
        assert!(result.is_err());
        assert!(clear_links(owner));
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let mut session = EqualitySession::new(owner, EqualityPolicy::LinkedFrames);
            session.save(root, root, 1).unwrap();
            session.save(child, child, 1).unwrap();
            panic!("probe session unwind");
        })).is_err());
        assert!(clear_links(owner));
        assert!(linked(owner, root, root)?);
        assert_eq!(owner.equality.capacity(), 0);
        Ok(())
    });
}

#[test]
fn linked_policy_checks_abort_before_identity_and_empty_shortcuts() {
    fixture(4096, |owner, memory| {
        let empty = owner.empty();
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        assert!(matches!(owner.singleton(empty, owner.grammar().state(1)?), Err(PredictionError::Admission(_))));
        drop(blocker);
        assert!(matches!(linked(owner, empty, empty), Err(PredictionError::Aborted)));
        assert!(clear_links(owner));
        Ok(())
    });
}

#[test]
fn twenty_thousand_array_levels_compare_and_destroy_without_recursion() {
    fixture(256 << 20, |owner, _| {
        let empty = owner.empty();
        let left = nested(owner, empty, 20_000)?;
        let right = nested(owner, empty, 20_000)?;
        assert!(linked(owner, left, right)?);
        assert!(clear_links(owner));
        assert_eq!(owner.equality.capacity(), 0);
        Ok(())
    });
}
