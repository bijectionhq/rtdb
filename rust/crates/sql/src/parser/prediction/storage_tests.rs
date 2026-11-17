use std::{mem::size_of, panic::{AssertUnwindSafe, catch_unwind}};
use super::*;
use super::super::{ContextShape, MergeMode, PredictionError, Problem, with_prediction_contexts};
use super::super::context::{ContextId, ContextStore};

fn storage(memory: &Arc<MemoryBudget>) -> (PredictionStorage, Arc<MemoryBudget>) {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
    (PredictionStorage::new(grammar, memory), grammar_memory)
}

fn admission<T>(result: Result<T>) {
    assert!(matches!(result, Err(PredictionError::Admission(error)) if error.code == "rtdb/memory-budget-exceeded"));
}

#[test]
fn empty_region_transfers_one_graph_without_allocating_prediction_storage() {
    let memory = MemoryBudget::new(0);
    let (storage, grammar_memory) = storage(&memory);
    assert!(grammar_memory.used_bytes() > 0);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(storage.contexts.live(), 0);
    assert_eq!(storage.contexts.canonical_len(), 0);
    drop(storage);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(grammar_memory.used_bytes(), 0);
}

#[test]
fn canonical_representatives_survive_separate_work_scopes() {
    let memory = MemoryBudget::new(1 << 20);
    let (mut storage, grammar_memory) = storage(&memory);
    let first = {
        let work_memory = MemoryBudget::new(1 << 20);
        let work = AllocationScope::new(&work_memory).unwrap();
        let id = storage.with_contexts(&work, |contexts| {
            let node = contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
            assert_eq!(contexts.canonicalize(node)?, node);
            Ok(node.identity())
        }).unwrap();
        drop(work);
        assert_eq!(work_memory.used_bytes(), 0);
        id
    };
    let retained = memory.used_bytes();
    assert!(retained > 0);
    let work_memory = MemoryBudget::new(1 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    storage.with_contexts(&work, |contexts| {
        let equal = contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
        assert_ne!(equal.identity(), first);
        assert_eq!(contexts.canonicalize(equal)?.identity(), first);
        Ok(())
    }).unwrap();
    assert_eq!(storage.contexts.live(), 1);
    assert_eq!(storage.contexts.canonical_len(), 1);
    assert_eq!(memory.used_bytes(), retained);
    drop(work);
    assert_eq!(work_memory.used_bytes(), 0);
    drop(storage);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(grammar_memory.used_bytes(), 0);
}

#[test]
fn unrooted_array_payloads_release_while_pool_capacity_remains_reusable() {
    let memory = MemoryBudget::new(1 << 20);
    let (mut storage, _) = storage(&memory);
    let work_memory = MemoryBudget::new(1 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    let during = storage.with_contexts(&work, |contexts| {
        let a = contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
        let b = contexts.singleton(contexts.empty(), contexts.grammar().state(2)?)?;
        let array = contexts.with_merge_round(|round| round.merge(a, b, MergeMode::FullExact))?;
        assert_eq!(contexts.view(array).shape(), ContextShape::Array);
        Ok(memory.used_bytes())
    }).unwrap();
    assert_eq!(storage.contexts.live(), 0);
    assert_eq!(storage.contexts.array_edges(), 0);
    assert_eq!(memory.used_bytes(), during - ContextStore::<Retained>::array_bytes(2));
    let retained = memory.used_bytes();
    let blocker = memory.reserve(memory.limit_bytes() - retained).unwrap();
    storage.with_contexts(&work, |contexts| {
        let node = contexts.singleton(contexts.empty(), contexts.grammar().state(3)?)?;
        assert_eq!(contexts.view(node).follow(0)?.unwrap().number(), 3);
        Ok(())
    }).unwrap();
    assert_eq!(storage.contexts.live(), 0);
    drop(blocker);
    assert_eq!(memory.used_bytes(), retained);
}

#[test]
fn canonical_array_roots_trace_shared_deep_parents_without_recursive_cleanup() {
    let memory = MemoryBudget::new(64 << 20);
    let (mut storage, grammar_memory) = storage(&memory);
    let work_memory = MemoryBudget::new(128 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    let root = storage.with_contexts(&work, |contexts| {
        let mut parent = contexts.empty();
        for _ in 0..20_000 { parent = contexts.singleton(parent, contexts.grammar().state(1)?)?; }
        let a = contexts.singleton(parent, contexts.grammar().state(2)?)?;
        let b = contexts.singleton(parent, contexts.grammar().state(3)?)?;
        let array = contexts.with_merge_round(|round| round.merge(a, b, MergeMode::FullExact))?;
        let canonical = contexts.canonicalize(array)?;
        assert_eq!(contexts.view(canonical).len(), 2);
        assert_eq!(contexts.view(canonical).parent(0), contexts.view(canonical).parent(1));
        Ok(canonical.identity())
    }).unwrap();
    assert_eq!(storage.contexts.live(), 20_001);
    assert_eq!(storage.contexts.canonical_len(), 20_001);
    assert_eq!(storage.contexts.array_edges(), 2);
    drop(work);
    assert_eq!(work_memory.used_bytes(), 0);
    let work = AllocationScope::new(&work_memory).unwrap();
    storage.with_contexts(&work, |contexts| {
        let root = contexts.restore(root)?;
        let mut parent = contexts.view(root).parent(0).flatten().unwrap();
        for _ in 0..20_000 {
            assert_eq!(contexts.view(parent).follow(0)?.unwrap().number(), 1);
            parent = contexts.view(parent).parent(0).flatten().unwrap();
        }
        assert_eq!(parent, contexts.empty());
        Ok(())
    }).unwrap();
    drop(storage);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(grammar_memory.used_bytes(), 0);
}

#[test]
fn a_failed_access_seals_only_its_facade_and_the_next_access_can_publish() {
    let memory = MemoryBudget::new(1 << 20);
    let (mut storage, _) = storage(&memory);
    let work_memory = MemoryBudget::new(1 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    storage.with_contexts(&work, |contexts| {
        let blocker = memory.reserve(memory.limit_bytes()).unwrap();
        admission(contexts.singleton(contexts.empty(), contexts.grammar().state(1)?));
        drop(blocker);
        assert!(matches!(contexts.singleton(contexts.empty(), contexts.grammar().state(1)?), Err(PredictionError::Aborted)));
        Ok(())
    }).unwrap();
    assert_eq!(memory.used_bytes(), 0);
    storage.with_contexts(&work, |contexts| {
        let node = contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
        assert_eq!(contexts.canonicalize(node)?, node);
        Ok(())
    }).unwrap();
    assert_eq!(storage.contexts.live(), 1);
}

#[test]
fn canonical_publication_survives_a_later_visited_admission_failure() {
    let memory = MemoryBudget::new(1 << 20);
    let (mut storage, _) = storage(&memory);
    let work_memory = MemoryBudget::new(4096 + ContextStore::<Retained>::first_canonical_work_bytes());
    let work = AllocationScope::new(&work_memory).unwrap();
    let id = storage.with_contexts(&work, |contexts| {
        let node = contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
        admission(contexts.canonicalize(node));
        assert!(matches!(contexts.canonicalize(node), Err(PredictionError::Aborted)));
        Ok(node.identity())
    }).unwrap();
    assert_eq!(storage.contexts.live(), 1);
    assert_eq!(storage.contexts.canonical_len(), 1);
    drop(work);
    assert_eq!(work_memory.used_bytes(), 0);
    let work_memory = MemoryBudget::new(1 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    storage.with_contexts(&work, |contexts| {
        let equal = contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
        assert_eq!(contexts.canonicalize(equal)?.identity(), id);
        Ok(())
    }).unwrap();
    assert_eq!(storage.contexts.live(), 1);
}

#[test]
fn teardown_collects_under_a_full_retained_budget() {
    let memory = MemoryBudget::new(1 << 20);
    let (mut storage, _) = storage(&memory);
    let work_memory = MemoryBudget::new(1 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    let blocker = storage.with_contexts(&work, |contexts| {
        let a = contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
        let b = contexts.singleton(contexts.empty(), contexts.grammar().state(2)?)?;
        contexts.with_merge_round(|round| round.merge(a, b, MergeMode::FullExact))?;
        Ok(memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap())
    }).unwrap();
    assert_eq!(storage.contexts.live(), 0);
    assert_eq!(storage.contexts.array_edges(), 0);
    assert_eq!(memory.used_bytes(), memory.limit_bytes() - ContextStore::<Retained>::array_bytes(2));
    drop(blocker);
    assert!(memory.used_bytes() > 0);
    drop(storage);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn returned_errors_and_unwinding_both_release_unrooted_contexts() {
    let memory = MemoryBudget::new(1 << 20);
    let (mut storage, _) = storage(&memory);
    let work_memory = MemoryBudget::new(1 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    let result: Result<()> = storage.with_contexts(&work, |contexts| {
        contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
        Err(PredictionError::Artifact(Problem::Context))
    });
    assert!(matches!(result, Err(PredictionError::Artifact(Problem::Context))));
    assert_eq!(storage.contexts.live(), 0);
    let result = catch_unwind(AssertUnwindSafe(|| {
        storage.with_contexts(&work, |contexts| -> Result<()> {
            contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
            panic!("caller unwinds");
        })
    }));
    assert!(result.is_err());
    assert_eq!(storage.contexts.live(), 0);
    storage.with_contexts(&work, |contexts| {
        let node = contexts.singleton(contexts.empty(), contexts.grammar().state(2)?)?;
        assert_eq!(contexts.view(node).follow(0)?.unwrap().number(), 2);
        Ok(())
    }).unwrap();
}

#[test]
fn retained_access_rejects_foreign_graph_states_before_storage_admission() {
    let memory = MemoryBudget::new(0);
    let (mut storage, _) = storage(&memory);
    let other_memory = MemoryBudget::new(8 << 20);
    let other = OwnedGrammar::load(&other_memory).unwrap();
    let foreign = other.view();
    let work_memory = MemoryBudget::new(1 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    {
        let grammar = storage.grammar.view();
        let mut contexts = PredictionContexts::borrowed(&grammar, &work, &mut storage.contexts);
        let empty = contexts.empty();
        let same = contexts.grammar().state(1).unwrap();
        assert!(same.belongs_to(contexts.grammar()));
        assert_eq!(contexts.view(empty).shape(), ContextShape::Empty);
        assert!(matches!(contexts.singleton(empty, foreign.state(3000).unwrap()), Err(PredictionError::ForeignOwner)));
        assert_eq!(memory.used_bytes(), 0);
        assert!(matches!(contexts.singleton(empty, same), Err(PredictionError::Aborted)));
    }
    storage.contexts.collect();
    assert_eq!(storage.contexts.live(), 0);
}

#[test]
fn separate_facades_of_the_owned_graph_share_retained_state_admission() {
    let memory = MemoryBudget::new(1 << 20);
    let (mut storage, _) = storage(&memory);
    let work_memory = MemoryBudget::new(1 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    {
        let first = storage.grammar.view();
        let second = storage.grammar.view();
        let mut contexts = PredictionContexts::borrowed(&first, &work, &mut storage.contexts);
        let node = contexts.singleton(contexts.empty(), second.state(1).unwrap()).unwrap();
        assert_eq!(contexts.view(node).follow(0).unwrap().unwrap().number(), 1);
        assert_eq!(contexts.canonicalize(node).unwrap(), node);
    }
    storage.contexts.collect();
    assert_eq!(storage.contexts.live(), 1);
    assert_eq!(storage.contexts.canonical_len(), 1);
}

#[test]
fn stale_context_ids_cannot_rebind_after_a_slot_is_reused() {
    let memory = MemoryBudget::new(1 << 20);
    let (mut storage, _) = storage(&memory);
    let work_memory = MemoryBudget::new(1 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    let old = storage.with_contexts(&work, |contexts| {
        Ok(contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?.identity())
    }).unwrap();
    assert_eq!(storage.contexts.live(), 0);
    storage.with_contexts(&work, |contexts| {
        let new = contexts.singleton(contexts.empty(), contexts.grammar().state(2)?)?;
        let (ContextId::Node(old), ContextId::Node(current)) = (old, new.identity()) else { panic!() };
        assert_eq!(old.index, current.index);
        assert_eq!(old.generation + 1, current.generation);
        assert!(matches!(contexts.restore(ContextId::Node(old)), Err(PredictionError::Artifact(Problem::Context))));
        assert_eq!(contexts.view(new).follow(0)?.unwrap().number(), 2);
        assert!(matches!(contexts.restore(new.identity()), Err(PredictionError::Aborted)));
        Ok(())
    }).unwrap();
}

#[test]
fn payload_admission_then_failed_node_growth_releases_the_unpublished_payload() {
    let memory = MemoryBudget::new(1 << 20);
    let (mut storage, _) = storage(&memory);
    let work_memory = MemoryBudget::new(1 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    let blocker = storage.with_contexts(&work, |contexts| {
        let mut a = contexts.empty();
        let mut b = contexts.empty();
        for state in 1..=16 {
            let node = contexts.singleton(contexts.empty(), contexts.grammar().state(state)?)?;
            if state == 1 { a = node; }
            if state == 2 { b = node; }
        }
        let payload = ContextStore::<Retained>::array_bytes(2);
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes() - payload).unwrap();
        let before = memory.used_bytes();
        admission(contexts.with_merge_round(|round| round.merge(a, b, MergeMode::FullExact)));
        assert_eq!(memory.used_bytes(), before);
        assert_eq!(memory.peak_bytes(), memory.limit_bytes());
        Ok(blocker)
    }).unwrap();
    assert_eq!(storage.contexts.live(), 0);
    assert_eq!(storage.contexts.array_edges(), 0);
    drop(blocker);
    let retained = memory.used_bytes();
    storage.with_contexts(&work, |contexts| {
        contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
        Ok(())
    }).unwrap();
    assert_eq!(memory.used_bytes(), retained);
}

#[test]
fn retained_block_growth_admits_old_and_new_capacities_before_replacement() {
    let first = 256 + 16 * size_of::<u64>();
    let replacement = 256 + 32 * size_of::<u64>();
    let memory = MemoryBudget::new(first + replacement - 1);
    let policy = Retained::new(&memory);
    let mut block = Block::with_capacity(&policy, 16).unwrap();
    block.push(11u64);
    admission(block.grow(&policy, 32));
    assert_eq!(&*block, &[11]);
    assert_eq!(block.capacity(), 16);
    assert_eq!(memory.used_bytes(), first);
    drop(block);
    assert_eq!(memory.used_bytes(), 0);
    let memory = MemoryBudget::new(first + replacement);
    let policy = Retained::new(&memory);
    let mut block = Block::with_capacity(&policy, 16).unwrap();
    block.push(11u64);
    block.grow(&policy, 32).unwrap();
    assert_eq!(&*block, &[11]);
    assert_eq!(memory.used_bytes(), replacement);
    assert_eq!(memory.peak_bytes(), first + replacement);
    drop(block);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn pool_reclamation_advances_generation_and_traces_roots_without_admission() {
    let memory = MemoryBudget::new(1 << 20);
    let policy = Retained::new(&memory);
    let mut pool = Pool::new();
    let first = pool.insert(10u64, &policy).unwrap();
    let retained = memory.used_bytes();
    let blocker = memory.reserve(memory.limit_bytes() - retained).unwrap();
    pool.mark(first);
    assert_eq!(pool.next_marked(), Some(first));
    assert!(pool.next_marked().is_none());
    pool.sweep();
    assert_eq!(pool.get(first), Some(&10));
    pool.sweep();
    assert!(pool.get(first).is_none());
    let second = pool.insert(20u64, &policy).unwrap();
    assert_eq!(second.index, first.index);
    assert_eq!(second.generation, first.generation + 1);
    assert!(pool.get(first).is_none());
    assert_eq!(pool.get(second), Some(&20));
    drop(blocker);
    assert_eq!(memory.used_bytes(), retained);
    drop(pool);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn exhausted_generations_retire_slots_without_wrapping_or_fallible_cleanup() {
    let memory = MemoryBudget::new(1 << 20);
    let policy = Retained::new(&memory);
    let mut pool = Pool::new();
    let first = pool.insert(10u64, &policy).unwrap();
    let Slot::Occupied { generation, .. } = &mut pool.slots[first.index] else { panic!() };
    *generation = u64::MAX;
    let retired = SlotId { index: first.index, generation: u64::MAX };
    assert_eq!(pool.get(retired), Some(&10));
    pool.sweep();
    assert!(matches!(pool.slots[first.index], Slot::Exhausted));
    assert!(pool.get(retired).is_none());
    let second = pool.insert(20u64, &policy).unwrap();
    assert_ne!(second.index, first.index);
    assert_eq!(second.generation, 0);
}

#[test]
fn scoped_blocks_and_factories_keep_reservations_until_their_scope_drops() {
    let memory = MemoryBudget::new(1 << 20);
    let scope = AllocationScope::new(&memory).unwrap();
    let block = Block::<u64, _>::with_capacity(&Scoped(&scope), 4).unwrap();
    let held = memory.used_bytes();
    drop(block);
    assert_eq!(memory.used_bytes(), held);
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
    let view = grammar.view();
    with_prediction_contexts(&view, &scope, |contexts| {
        contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
        Ok(())
    }).unwrap();
    assert!(memory.used_bytes() > held);
    drop(view);
    drop(scope);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn retained_access_returns_owner_independent_facts_and_keeps_drop_witnesses() {
    let memory = MemoryBudget::new(0);
    let (mut storage, _) = storage(&memory);
    let work_memory = MemoryBudget::new(4096);
    let work = AllocationScope::new(&work_memory).unwrap();
    let observed = storage.with_contexts(&work, |contexts| {
        let view = contexts.view(contexts.empty());
        Ok((view.shape(), view.is_empty(), view.len(), view.hash()))
    }).unwrap();
    drop(work);
    assert_eq!(work_memory.used_bytes(), 0);
    assert_eq!(observed.0, ContextShape::Empty);
    assert!(observed.1);
    assert_eq!(observed.2, 1);
    assert!(std::mem::needs_drop::<PredictionStorage>());
    assert!(std::mem::needs_drop::<PredictionContexts<'_, '_, '_, Retained>>());
    assert!(std::mem::needs_drop::<BorrowSession<'_>>());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn the_retained_region_moves_and_outlives_its_constructor_budget_handles() {
    let memory = MemoryBudget::new(16 << 20);
    let (mut storage, original) = {
        let grammar_memory = memory.scope();
        let retained_memory = memory.scope();
        let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
        let mut storage = PredictionStorage::new(grammar, &retained_memory);
        let work_memory = MemoryBudget::new(1 << 20);
        let work = AllocationScope::new(&work_memory).unwrap();
        let id = storage.with_contexts(&work, |contexts| {
            let node = contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
            Ok(contexts.canonicalize(node)?.identity())
        }).unwrap();
        (storage, id)
    };
    assert!(memory.used_bytes() > 0);
    let retained = memory.used_bytes();
    let work_memory = MemoryBudget::new(1 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    storage.with_contexts(&work, |contexts| {
        let equal = contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
        assert_eq!(contexts.canonicalize(equal)?.identity(), original);
        Ok(())
    }).unwrap();
    assert_eq!(memory.used_bytes(), retained);
    drop(storage);
    assert_eq!(memory.used_bytes(), 0);
}
