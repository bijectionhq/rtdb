use std::cell::Cell;
use std::mem::size_of;
use std::sync::Arc;
use rtdb_types::{MemoryBudget, murmur3_32};
use super::*;
use super::super::with_prediction_contexts;

fn fixture<R>(limit: usize, use_contexts: impl for<'id, 'g, 'scope> FnOnce(&mut PredictionContexts<'id, 'g, 'scope>, &Arc<MemoryBudget>) -> Result<R>) -> R {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar_allocation = AllocationScope::new(&grammar_memory).unwrap();
    let grammar = Grammar::load(&grammar_allocation).unwrap();
    let memory = MemoryBudget::new(limit);
    let allocation = AllocationScope::new(&memory).unwrap();
    let result = with_prediction_contexts(&grammar, &allocation, |contexts| use_contexts(contexts, &memory)).unwrap();
    drop(allocation);
    assert_eq!(memory.used_bytes(), 0);
    result
}

fn single<'id>(contexts: &mut PredictionContexts<'id, '_, '_>, parent: ContextKey<'id>, state: u32) -> Result<ContextKey<'id>> {
    contexts.singleton(parent, contexts.grammar.state(state)?)
}
fn array<'id>(contexts: &mut PredictionContexts<'id, '_, '_>, values: &[(Option<ContextKey<'id>>, Option<u32>)]) -> Result<ContextKey<'id>> {
    let mut candidate = contexts.candidate(values.len())?;
    for (parent, state) in values {
        let address = match state { Some(state) => { contexts.grammar.state(*state)?; ReturnAddress::Follow(*state) }, None => ReturnAddress::Empty };
        candidate.edges.push(Edge { parent: *parent, address });
    }
    contexts.array(&candidate)
}
fn admission<T>(result: Result<T>) {
    match result {
        Err(PredictionError::Admission(error)) => assert_eq!(error.code, "rtdb/memory-budget-exceeded"),
        _ => panic!("admission failure"),
    }
}
fn addresses<'id>(contexts: &PredictionContexts<'id, '_, '_>, key: ContextKey<'id>) -> Vec<i32> {
    (0..contexts.len(key)).map(|index| contexts.edge(key, index).address.number()).collect()
}

#[test]
fn singleton_null_parents_preserve_alias_identity_and_empty_is_distinct() {
    fixture(1 << 20, |contexts, _| {
        let a = contexts.singleton_edge(Edge { parent: None, address: ReturnAddress::Follow(1) })?;
        let b = contexts.singleton_edge(Edge { parent: None, address: ReturnAddress::Follow(1) })?;
        let empty = contexts.empty();
        assert!(contexts.equivalent(a, a)?);
        assert!(!contexts.equivalent(a, b)?);
        assert!(!contexts.equivalent(a, empty)?);
        assert_eq!(contexts.hash(a), contexts.hash(empty));
        let left = single(contexts, a, 2)?;
        let alias_parent = single(contexts, a, 2)?;
        let distinct_parent = single(contexts, b, 2)?;
        assert!(contexts.equivalent(left, alias_parent)?);
        assert!(!contexts.equivalent(left, distinct_parent)?);
        assert_eq!(contexts.singleton_edge(Edge { parent: None, address: ReturnAddress::Empty })?, empty);
        Ok(())
    });
}

#[test]
fn one_entry_arrays_keep_their_class_and_nullable_parent_equality() {
    fixture(1 << 20, |contexts, _| {
        let empty = contexts.empty();
        let one = single(contexts, empty, 2)?;
        let a = array(contexts, &[(Some(empty), Some(2))])?;
        let b = array(contexts, &[(Some(empty), Some(2))])?;
        assert!(!contexts.equivalent(a, one)?);
        assert!(contexts.equivalent(a, b)?);
        let nullable_a = array(contexts, &[(Some(empty), Some(1)), (None, None)])?;
        let nullable_b = array(contexts, &[(Some(empty), Some(1)), (None, None)])?;
        assert!(contexts.equivalent(nullable_a, nullable_b)?);
        let empty_array = array(contexts, &[(None, None)])?;
        assert!(contexts.view(empty_array).is_empty());
        assert_eq!(contexts.view(empty_array).shape(), ContextShape::Array);
        assert!(!contexts.equivalent(empty_array, empty)?);
        assert_eq!(contexts.canonicalize(empty_array)?, empty_array);
        Ok(())
    });
}

#[test]
fn context_hashes_use_parent_words_before_return_words() {
    fixture(1 << 20, |contexts, _| {
        let empty = contexts.empty();
        assert_eq!(contexts.hash(empty) as u32, murmur3_32(&[], 1));
        let a = single(contexts, empty, 12)?;
        let mut single_bytes = [0; 8];
        single_bytes[..4].copy_from_slice(&contexts.hash(empty).to_le_bytes());
        single_bytes[4..].copy_from_slice(&12i32.to_le_bytes());
        assert_eq!(contexts.hash(a) as u32, murmur3_32(&single_bytes, 1));
        let merged = array(contexts, &[(Some(a), Some(1)), (Some(empty), Some(3))])?;
        let mut bytes = [0; 16];
        for (index, word) in [contexts.hash(a), contexts.hash(empty), 1, 3].into_iter().enumerate() {
            bytes[index * 4..index * 4 + 4].copy_from_slice(&word.to_le_bytes());
        }
        assert_eq!(contexts.hash(merged) as u32, murmur3_32(&bytes, 1));
        let changed = array(contexts, &[(Some(empty), Some(1)), (Some(a), Some(3))])?;
        assert!(!contexts.equivalent(merged, changed)?);
        Ok(())
    });
}

#[test]
fn local_and_full_root_merges_and_same_round_mode_reuse_follow_source() {
    fixture(1 << 20, |contexts, _| {
        let empty = contexts.empty();
        let one = single(contexts, empty, 2)?;
        contexts.with_merge_round(|round| {
            assert_eq!(round.merge(empty, one, MergeMode::LocalWildcard)?, empty);
            assert_eq!(round.owner.memo.len, 1);
            assert_eq!(round.merge(empty, one, MergeMode::FullExact)?, empty);
            Ok(())
        })?;
        assert_eq!(contexts.memo.len, 0);
        let exact = contexts.with_merge_round(|round| round.merge(empty, one, MergeMode::FullExact))?;
        assert_eq!(addresses(contexts, exact), [2, i32::MAX]);
        assert_eq!(contexts.view(exact).parent(1), Some(None));
        assert!(contexts.view(exact).follow(1)?.is_none());
        Ok(())
    });
}

#[test]
fn memo_checks_both_orientations_and_equal_keys_without_interning_inputs() {
    fixture(1 << 20, |contexts, _| {
        let empty = contexts.empty();
        let a = single(contexts, empty, 3)?;
        let a_equal = single(contexts, empty, 3)?;
        let b = single(contexts, empty, 1)?;
        contexts.with_merge_round(|round| {
            assert_eq!(round.merge(a, a_equal, MergeMode::FullExact)?, a);
            assert_eq!(round.owner.memo.len, 0);
            let merged = round.merge(a, b, MergeMode::FullExact)?;
            assert_eq!(addresses(round.owner, merged), [1, 3]);
            assert_eq!(round.merge(b, a, MergeMode::FullExact)?, merged);
            assert_eq!(round.merge(a_equal, b, MergeMode::FullExact)?, merged);
            assert_eq!(round.owner.memo.len, 1);
            Ok(())
        })?;
        assert!(contexts.canonical.slots.is_empty());
        Ok(())
    });
}

#[test]
fn equal_return_states_reuse_the_identical_left_or_right_parent_without_memo_insert() {
    fixture(1 << 20, |contexts, _| {
        let empty = contexts.empty();
        let parent = single(contexts, empty, 1)?;
        let root_head = single(contexts, empty, 3)?;
        let nested_head = single(contexts, parent, 3)?;
        contexts.with_merge_round(|round| {
            assert_eq!(round.merge(root_head, nested_head, MergeMode::LocalWildcard)?, root_head);
            assert!(round.owner.memo_slot(root_head, nested_head)?.is_none());
            assert_eq!(round.merge(nested_head, root_head, MergeMode::LocalWildcard)?, root_head);
            assert!(round.owner.memo_slot(nested_head, root_head)?.is_none());
            Ok(())
        })
    });
}

#[test]
fn distinct_parent_merges_build_one_shared_parent_beneath_a_new_singleton() {
    fixture(1 << 20, |contexts, _| {
        let empty = contexts.empty();
        let a = single(contexts, empty, 1)?;
        let b = single(contexts, empty, 2)?;
        let left = single(contexts, a, 4)?;
        let right = single(contexts, b, 4)?;
        let merged = contexts.with_merge_round(|round| round.merge(left, right, MergeMode::FullExact))?;
        assert_eq!(contexts.view(merged).shape(), ContextShape::Singleton);
        let parent = contexts.view(merged).parent(0).flatten().unwrap();
        assert_eq!(addresses(contexts, parent), [1, 2]);
        assert_ne!(merged, left);
        assert_ne!(merged, right);
        Ok(())
    });
}

#[test]
fn array_operand_reuse_precedes_common_parent_normalization() {
    fixture(1 << 20, |contexts, _| {
        let empty = contexts.empty();
        let p1 = single(contexts, empty, 5)?;
        let p2 = single(contexts, empty, 5)?;
        let left = array(contexts, &[(Some(p1), Some(1)), (Some(p2), Some(3))])?;
        let right = array(contexts, &[(Some(p1), Some(1))])?;
        let merged = contexts.with_merge_round(|round| round.merge(left, right, MergeMode::FullExact))?;
        assert_eq!(merged, left);
        assert_eq!(contexts.view(merged).parent(1), Some(Some(p2)));
        assert!(contexts.common_parents.slots.is_empty());
        Ok(())
    });
}

#[test]
fn new_array_candidates_retain_the_first_equal_parent_in_sorted_edge_order() {
    fixture(1 << 20, |contexts, _| {
        let empty = contexts.empty();
        let p1 = single(contexts, empty, 5)?;
        let p2 = single(contexts, empty, 5)?;
        let p3 = single(contexts, empty, 5)?;
        let left = array(contexts, &[(Some(p1), Some(1)), (Some(p2), Some(3))])?;
        let right = array(contexts, &[(Some(p2), Some(2)), (Some(p3), Some(4))])?;
        let merged = contexts.with_merge_round(|round| round.merge(left, right, MergeMode::FullExact))?;
        assert_eq!(addresses(contexts, merged), [1, 2, 3, 4]);
        for index in 0..4 { assert_eq!(contexts.view(merged).parent(index), Some(Some(p1))); }
        assert_eq!(contexts.view(left).parent(1), Some(Some(p2)));
        assert_eq!(contexts.view(right).parent(1), Some(Some(p3)));
        Ok(())
    });
}

#[test]
fn one_result_array_merge_constructs_a_singleton_before_operand_reuse() {
    fixture(1 << 20, |contexts, _| {
        let empty = contexts.empty();
        let one = single(contexts, empty, 2)?;
        let array = array(contexts, &[(Some(empty), Some(2))])?;
        let merged = contexts.with_merge_round(|round| round.merge(array, one, MergeMode::FullExact))?;
        assert_eq!(contexts.view(merged).shape(), ContextShape::Singleton);
        assert_ne!(merged, one);
        assert!(contexts.equivalent(merged, one)?);
        Ok(())
    });
}

#[test]
fn canonicalization_reuses_cached_parents_and_preserves_unchanged_originals() {
    fixture(1 << 20, |contexts, _| {
        let empty = contexts.empty();
        let p1 = single(contexts, empty, 1)?;
        let p2 = single(contexts, empty, 1)?;
        assert_eq!(contexts.canonicalize(p1)?, p1);
        assert_eq!(contexts.canonicalize(p2)?, p1);
        let array = array(contexts, &[(Some(p1), Some(2))])?;
        assert_eq!(contexts.canonicalize(array)?, array);
        assert_eq!(contexts.view(array).shape(), ContextShape::Array);
        let root = single(contexts, p2, 3)?;
        let canonical = contexts.canonicalize(root)?;
        assert_ne!(canonical, root);
        assert_eq!(contexts.view(canonical).parent(0), Some(Some(p1)));
        assert_eq!(contexts.canonicalize(root)?, canonical);
        contexts.with_merge_round(|round| { assert_eq!(round.owner.canonicalize(p2)?, p1); Ok(()) })?;
        assert_eq!(contexts.canonicalize(p2)?, p1);
        Ok(())
    });
}

#[test]
fn changed_one_entry_array_returns_the_new_singleton_even_when_cache_add_finds_an_equal_head() {
    fixture(1 << 20, |contexts, _| {
        let empty = contexts.empty();
        let p1 = single(contexts, empty, 1)?;
        let p2 = single(contexts, empty, 1)?;
        let head = single(contexts, p1, 2)?;
        assert_eq!(contexts.canonicalize(head)?, head);
        let array = array(contexts, &[(Some(p2), Some(2))])?;
        let updated = contexts.canonicalize(array)?;
        assert_eq!(contexts.view(updated).shape(), ContextShape::Singleton);
        assert_ne!(updated, head);
        assert!(contexts.equivalent(updated, head)?);
        assert_eq!(contexts.visited_get(array), Some(updated));
        assert_eq!(contexts.visited_get(updated), Some(updated));
        assert_eq!(contexts.canonicalize(updated)?, head);
        Ok(())
    });
}

#[test]
fn full_context_null_return_paths_cannot_enter_sll_canonical_publication() {
    fixture(1 << 20, |contexts, memory| {
        let empty = contexts.empty();
        let one = single(contexts, empty, 2)?;
        let full = contexts.with_merge_round(|round| round.merge(empty, one, MergeMode::FullExact))?;
        let before = memory.used_bytes();
        assert!(matches!(contexts.canonicalize(full), Err(PredictionError::Artifact(Problem::CanonicalDomain))));
        assert_eq!(memory.used_bytes(), before);
        assert!(matches!(contexts.canonicalize(empty), Err(PredictionError::Aborted)));
        assert_eq!(contexts.view(full).parent(1), Some(None));
        Ok(())
    });
}

#[test]
fn invocation_conversion_keeps_source_outer_to_inner_follow_order() {
    fixture(1 << 20, |contexts, _| {
        let invoked = contexts.grammar.state(352)?;
        let inner = contexts.grammar.state(518)?;
        let context = contexts.from_invocations([inner, invoked].into_iter())?;
        assert_eq!(contexts.view(context).follow(0)?.unwrap().number(), 575);
        let parent = contexts.view(context).parent(0).flatten().unwrap();
        assert_eq!(contexts.view(parent).follow(0)?.unwrap().number(), 354);
        assert_eq!(contexts.view(parent).parent(0), Some(Some(contexts.empty())));
        Ok(())
    });
}

#[test]
fn state_owner_checks_precede_transition_access_and_parent_lookup_is_outer_first() {
    let memory = MemoryBudget::new(16 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    let grammar = Grammar::load(&allocation).unwrap();
    let other = Grammar::load(&allocation).unwrap();
    let foreign = other.state(3000).unwrap();
    assert!(grammar.state(352).unwrap().belongs_to(&grammar));
    assert!(!foreign.belongs_to(&grammar));
    with_prediction_contexts(&grammar, &allocation, |contexts| {
        let empty = contexts.empty();
        assert!(matches!(contexts.singleton(empty, foreign), Err(PredictionError::ForeignOwner)));
        assert!(contexts.nodes.is_empty());
        Ok(())
    }).unwrap();
    with_prediction_contexts(&grammar, &allocation, |contexts| {
        let demanded = Cell::new(0);
        let invocations = [foreign, grammar.state(356)?].into_iter().inspect(|_| demanded.set(demanded.get() + 1));
        assert!(matches!(contexts.from_invocations(invocations), Err(PredictionError::Artifact(Problem::Invocation(356)))));
        assert_eq!(demanded.get(), 2);
        Ok(())
    }).unwrap();
    with_prediction_contexts(&grammar, &allocation, |contexts| {
        assert!(matches!(contexts.from_invocations([foreign].into_iter()), Err(PredictionError::ForeignOwner)));
        Ok(())
    }).unwrap();
}

#[test]
fn deep_conversion_equality_merge_canonicalization_and_destruction_use_flat_storage() {
    fixture(256 << 20, |contexts, _| {
        let empty = contexts.empty();
        let mut left = single(contexts, empty, 1)?;
        let mut equal = single(contexts, empty, 1)?;
        let mut right = single(contexts, empty, 2)?;
        for _ in 0..20_000 {
            left = single(contexts, left, 3)?;
            equal = single(contexts, equal, 3)?;
            right = single(contexts, right, 3)?;
        }
        assert!(contexts.equivalent(left, equal)?);
        let merged = contexts.with_merge_round(|round| round.merge(left, right, MergeMode::FullExact))?;
        let canonical = contexts.canonicalize(merged)?;
        let mut cursor = canonical;
        for _ in 0..20_000 {
            assert_eq!(contexts.view(cursor).follow(0)?.unwrap().number(), 3);
            cursor = contexts.view(cursor).parent(0).flatten().unwrap();
        }
        assert_eq!(addresses(contexts, cursor), [1, 2]);
        let invocation = contexts.grammar.state(352)?;
        let converted = contexts.from_invocations(std::iter::repeat_n(invocation, 20_000))?;
        let mut cursor = converted;
        for _ in 0..20_000 {
            assert_eq!(contexts.view(cursor).follow(0)?.unwrap().number(), 354);
            cursor = contexts.view(cursor).parent(0).flatten().unwrap();
        }
        assert_eq!(cursor, empty);
        Ok(())
    });
}

#[test]
fn first_node_admission_seals_the_owner_and_releases_no_unpublished_key() {
    fixture(4096 + 256 + 16 * size_of::<Node<'_>>() - 1, |contexts, memory| {
        let empty = contexts.empty();
        admission(single(contexts, empty, 1));
        assert!(contexts.nodes.is_empty());
        assert_eq!(memory.used_bytes(), 4096);
        assert!(matches!(single(contexts, empty, 1), Err(PredictionError::Aborted)));
        assert_eq!(memory.used_bytes(), 4096);
        Ok(())
    });
}

#[test]
fn node_growth_admits_the_entire_replacement_while_the_old_capacity_remains_charged() {
    let first = 256 + 16 * size_of::<Node<'_>>();
    let replacement = 256 + 32 * size_of::<Node<'_>>();
    fixture(4096 + first + replacement - 1, |contexts, memory| {
        let empty = contexts.empty();
        let mut last = empty;
        for _ in 0..16 { last = single(contexts, empty, 1)?; }
        assert_eq!(memory.used_bytes(), 4096 + first);
        admission(single(contexts, empty, 1));
        assert_eq!(contexts.nodes.len(), 16);
        assert_eq!(contexts.view(last).follow(0)?.unwrap().number(), 1);
        assert_eq!(memory.used_bytes(), 4096 + first);
        Ok(())
    });
}

#[test]
fn failed_array_edge_admission_does_not_publish_the_candidate() {
    let nodes = 256 + 16 * size_of::<Node<'_>>();
    let frames = 256 + 16 * size_of::<MergeFrame<'_>>();
    let candidate = 256 + 2 * size_of::<Edge<'_>>();
    let edges = 256 + 16 * size_of::<Edge<'_>>();
    fixture(4096 + nodes + frames + candidate + edges - 1, |contexts, memory| {
        let empty = contexts.empty();
        let a = single(contexts, empty, 1)?;
        let b = single(contexts, empty, 2)?;
        admission(contexts.with_merge_round(|round| round.merge(a, b, MergeMode::FullExact)));
        assert_eq!(contexts.nodes.len(), 2);
        assert!(contexts.edges.is_empty());
        assert_eq!(memory.used_bytes(), 4096 + nodes + frames + candidate);
        assert!(matches!(contexts.equivalent(a, b), Err(PredictionError::Aborted)));
        Ok(())
    });
}

#[test]
fn failed_memo_admission_retains_only_private_completed_storage_under_an_aborted_owner() {
    let nodes = 256 + 16 * size_of::<Node<'_>>();
    let frames = 256 + 16 * size_of::<MergeFrame<'_>>();
    let candidate = 256 + 2 * size_of::<Edge<'_>>();
    let edges = 256 + 16 * size_of::<Edge<'_>>();
    let memo = 256 + 32 * size_of::<Option<Record<(ContextKey<'_>, ContextKey<'_>), ContextKey<'_>>>>();
    fixture(4096 + nodes + frames + candidate + edges + memo - 1, |contexts, memory| {
        let empty = contexts.empty();
        let a = single(contexts, empty, 1)?;
        let b = single(contexts, empty, 2)?;
        admission(contexts.with_merge_round(|round| round.merge(a, b, MergeMode::FullExact)));
        assert_eq!(contexts.nodes.len(), 3);
        assert_eq!(contexts.edges.len(), 2);
        assert_eq!(contexts.memo.len, 0);
        let before = memory.used_bytes();
        assert!(matches!(contexts.with_merge_round(|round| round.merge(a, b, MergeMode::FullExact)), Err(PredictionError::Aborted)));
        assert_eq!(memory.used_bytes(), before);
        Ok(())
    });
}

#[test]
fn released_external_memory_cannot_restart_canonicalization_after_admission_failure() {
    fixture(1 << 20, |contexts, memory| {
        let empty = contexts.empty();
        let node = single(contexts, empty, 1)?;
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        admission(contexts.canonicalize(node));
        drop(blocker);
        let before = memory.used_bytes();
        assert!(matches!(contexts.canonicalize(node), Err(PredictionError::Aborted)));
        assert_eq!(memory.used_bytes(), before);
        assert_eq!(contexts.view(node).parent(0), Some(Some(empty)));
        Ok(())
    });
}

#[test]
fn array_parent_descent_and_full_empty_paths_preserve_return_order() {
    fixture(1 << 20, |contexts, _| {
        let empty = contexts.empty();
        let p1 = single(contexts, empty, 1)?;
        let p2 = single(contexts, empty, 2)?;
        let left = array(contexts, &[(Some(p1), Some(5)), (None, None)])?;
        let right = array(contexts, &[(Some(p2), Some(5)), (None, None)])?;
        let merged = contexts.with_merge_round(|round| round.merge(left, right, MergeMode::FullExact))?;
        assert_eq!(addresses(contexts, merged), [5, i32::MAX]);
        let parent = contexts.view(merged).parent(0).flatten().unwrap();
        assert_eq!(addresses(contexts, parent), [1, 2]);
        assert_eq!(contexts.view(merged).parent(1), Some(None));
        Ok(())
    });
}

#[test]
fn canonical_membership_and_identity_visits_grow_without_changing_representatives() {
    fixture(2 << 20, |contexts, _| {
        let mut root = contexts.empty();
        for _ in 0..64 { root = single(contexts, root, 1)?; }
        assert_eq!(contexts.canonicalize(root)?, root);
        assert_eq!(contexts.canonical.len, 64);
        assert_eq!(contexts.canonical.slots.len(), 128);
        assert_eq!(contexts.visited.len, 64);
        assert_eq!(contexts.visited.slots.len(), 128);
        assert_eq!(contexts.canonicalize(root)?, root);
        assert_eq!(contexts.canonical.len, 64);
        assert_eq!(contexts.visited.len, 1);
        Ok(())
    });
}

#[test]
fn nested_array_equality_admits_continuations_before_descending() {
    fixture(2 << 20, |contexts, memory| {
        let empty = contexts.empty();
        let mut left = empty;
        let mut right = empty;
        for _ in 0..64 {
            left = array(contexts, &[(Some(left), Some(1)), (Some(empty), Some(2))])?;
            right = array(contexts, &[(Some(right), Some(1)), (Some(empty), Some(2))])?;
        }
        let first = 256 + 16 * size_of::<EqualityFrame<'_>>();
        let next = 256 + 32 * size_of::<EqualityFrame<'_>>();
        let remaining = memory.limit_bytes() - memory.used_bytes();
        let blocker = memory.reserve(remaining - first - next + 1).unwrap();
        let before = memory.used_bytes();
        admission(contexts.equivalent(left, right));
        assert_eq!(memory.used_bytes(), before + first);
        assert_eq!(contexts.equality.capacity(), 16);
        drop(blocker);
        assert!(matches!(contexts.equivalent(left, right), Err(PredictionError::Aborted)));
        Ok(())
    });
}

#[test]
fn empty_owner_uses_no_storage_and_only_inline_inspection_survives_the_region() {
    let observed = fixture(4096, |contexts, memory| {
        let view = contexts.view(contexts.empty());
        assert_eq!(memory.used_bytes(), 4096);
        Ok((view.shape(), view.is_empty(), view.len(), view.hash()))
    });
    assert_eq!(observed, (ContextShape::Empty, true, 1, murmur3_32(&[], 1) as i32));
}
