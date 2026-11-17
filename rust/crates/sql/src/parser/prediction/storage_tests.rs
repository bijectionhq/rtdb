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

use super::super::{ConfigChange, ConfigKey, ContextKey, Semantic, SetMode};

fn joint_config<'cfg, 'id, 'g, 'scope>(configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>, contexts: &PredictionContexts<'id, 'g, 'scope, Retained>, context: ContextKey<'id>, alternative: usize) -> Result<ConfigKey<'cfg>> {
    let alternative = configs.alternative(contexts.grammar().decision(65)?, alternative)?;
    configs.create(contexts.grammar().state(352)?, alternative, context, Semantic::Unpredicated)
}

#[test]
fn retained_sets_keep_shared_config_identities_and_current_contexts_across_accesses() {
    let memory = MemoryBudget::new(4 << 20);
    let (mut storage, grammar_memory) = storage(&memory);
    let work_memory = MemoryBudget::new(4 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    let (a, b, detached_set, first, detached, original_context) = with_prediction_storage(&mut storage, &work, |access| {
        let (a, b, c, first, detached, one) = {
            let (configs, contexts) = access.parts();
            let one = contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
            let first = joint_config(configs, contexts, one, 1)?;
            let detached = configs.copy(first, ConfigChange::Same)?;
            let a = configs.new_set(SetMode::Full)?;
            configs.add_uncached(a, first, contexts)?;
            let b = configs.copy_set(a, contexts)?;
            let c = configs.new_set(SetMode::Full)?;
            configs.add_uncached(c, detached, contexts)?;
            (a, b, c, first.identity(), detached.identity(), one.identity())
        };
        access.fixture_state(0, Some((a, b)))?;
        access.fixture_state(1, Some((c, c)))?;
        Ok((a.identity(), b.identity(), c.identity(), first, detached, one))
    }).unwrap();
    drop(work);
    assert_eq!(work_memory.used_bytes(), 0);
    let work = AllocationScope::new(&work_memory).unwrap();
    let merged = with_prediction_storage(&mut storage, &work, |access| {
        let (configs, contexts) = access.parts();
        let a = configs.restore_set(a)?;
        let b = configs.restore_set(b)?;
        let c = configs.restore_set(detached_set)?;
        let first = configs.restore_config(first)?;
        let detached = configs.restore_config(detached)?;
        assert_eq!(configs.view_set(a).entry(0), Some(first));
        assert_eq!(configs.view_set(b).entry(0), Some(first));
        assert_eq!(configs.view_set(c).entry(0), Some(detached));
        let two = contexts.singleton(contexts.empty(), contexts.grammar().state(2)?)?;
        let incoming = joint_config(configs, contexts, two, 1)?;
        configs.increment_outer(incoming)?;
        configs.set_suppressed(incoming, true)?;
        configs.add_uncached(b, incoming, contexts)?;
        let current = configs.view(first).context();
        assert_eq!(contexts.view(current).len(), 2);
        assert_eq!(configs.view(first).outer_depth(), 1);
        assert!(configs.view(first).suppressed());
        assert_eq!(configs.view(detached).context().identity(), original_context);
        assert_eq!(configs.view(detached).packed_outer(), 0);
        Ok(current.identity())
    }).unwrap();
    assert_eq!(storage.configs.live(), (2, 3, 0));
    assert_eq!(storage.contexts.live(), 2);
    drop(work);
    let work = AllocationScope::new(&work_memory).unwrap();
    with_prediction_storage(&mut storage, &work, |access| {
        let (configs, contexts) = access.parts();
        let first = configs.restore_config(first)?;
        assert_eq!(configs.view(first).context().identity(), merged);
        assert_eq!(contexts.view(configs.view(first).context()).len(), 2);
        Ok(())
    }).unwrap();
    drop(storage);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(grammar_memory.used_bytes(), 0);
}

#[test]
fn readonly_hash_and_conflict_identity_survive_later_shared_alias_updates() {
    let memory = MemoryBudget::new(4 << 20);
    let (mut storage, _) = storage(&memory);
    let work_memory = MemoryBudget::new(4 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    let (a, b, old_hash, first_conflict, second_conflict) = with_prediction_storage(&mut storage, &work, |access| {
        let (a, b, hash, first_conflict, second_conflict) = {
            let (configs, contexts) = access.parts();
            let one = contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
            let key = joint_config(configs, contexts, one, 1)?;
            let a = configs.new_set(SetMode::Full)?;
            configs.add_uncached(a, key, contexts)?;
            let mut bits = super::super::AlternativeSet::default();
            bits.insert(configs.view(key).alternative());
            let first_conflict = configs.publish_conflict(bits)?;
            configs.set_conflicting(a, Some(first_conflict))?;
            configs.mark_readonly(a)?;
            let hash = configs.set_hash(a, contexts)?;
            assert_ne!(hash, -1);
            let b = configs.copy_set(a, contexts)?;
            let second_conflict = configs.publish_conflict(bits)?;
            configs.set_conflicting(b, Some(second_conflict))?;
            (a, b, hash, first_conflict.identity(), second_conflict.identity())
        };
        access.fixture_state(0, Some((a, b)))?;
        Ok((a.identity(), b.identity(), hash, first_conflict, second_conflict))
    }).unwrap();
    with_prediction_storage(&mut storage, &work, |access| {
        let (configs, contexts) = access.parts();
        let a = configs.restore_set(a)?;
        let b = configs.restore_set(b)?;
        let two = contexts.singleton(contexts.empty(), contexts.grammar().state(2)?)?;
        let incoming = joint_config(configs, contexts, two, 1)?;
        configs.add_uncached(b, incoming, contexts)?;
        assert_eq!(configs.set_hash(a, contexts)?, old_hash);
        assert_ne!(configs.set_hash(b, contexts)?, old_hash);
        let first = configs.restore_conflict(first_conflict)?;
        let second = configs.restore_conflict(second_conflict)?;
        assert_ne!(first, second);
        assert_eq!(configs.conflict(first), configs.conflict(second));
        assert_eq!(configs.view_set(a).conflicting(), Some(first));
        configs.clear(b)?;
        assert_eq!(configs.view_set(b).len(), 0);
        assert_eq!(configs.view_set(b).conflicting(), Some(second));
        Ok(())
    }).unwrap();
    assert_eq!(storage.configs.live(), (1, 2, 2));
    assert_eq!(storage.contexts.live(), 1);
    with_prediction_storage(&mut storage, &work, |access| {
        let (configs, contexts) = access.parts();
        let a = configs.restore_set(a)?;
        assert_eq!(configs.set_hash(a, contexts)?, old_hash);
        Ok(())
    }).unwrap();
}

#[test]
fn rooted_building_membership_retains_a_config_published_before_failed_ordered_append() {
    let memory = MemoryBudget::new(4 << 20);
    let (mut storage, _) = storage(&memory);
    let work_memory = MemoryBudget::new(4 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    let (set, member, context) = with_prediction_storage(&mut storage, &work, |access| {
        let (set, member, context) = {
            let (configs, contexts) = access.parts();
            let first = joint_config(configs, contexts, contexts.empty(), 1)?;
            let set = configs.new_set(SetMode::Full)?;
            for _ in 0..7 { configs.add_uncached(set, first, contexts)?; }
            let mut context = contexts.empty();
            for _ in 0..64 { context = contexts.singleton(context, contexts.grammar().state(2)?)?; }
            let member = joint_config(configs, contexts, context, 2)?;
            configs.increment_outer(member)?;
            (set, member, context)
        };
        access.fixture_state(0, Some((set, set)))?;
        {
            let (configs, contexts) = access.parts();
            let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
            admission(configs.add_uncached(set, member, contexts));
            assert_eq!(configs.view_set(set).len(), 7);
            assert!(configs.view_set(set).dips_outer());
            drop(blocker);
            assert!(matches!(configs.clear(set), Err(PredictionError::Aborted)));
        }
        Ok((set.identity(), member.identity(), context.identity()))
    }).unwrap();
    assert_eq!(storage.configs.live(), (2, 1, 0));
    assert_eq!(storage.contexts.live(), 64);
    with_prediction_storage(&mut storage, &work, |access| {
        let (configs, contexts) = access.parts();
        let set = configs.restore_set(set)?;
        let member = configs.restore_config(member)?;
        assert_eq!(configs.view(member).context().identity(), context);
        let equal = configs.copy(member, ConfigChange::Same)?;
        configs.add_uncached(set, equal, contexts)?;
        assert_eq!(configs.view_set(set).len(), 7);
        configs.add_uncached(set, member, contexts)?;
        assert_eq!(configs.view_set(set).len(), 8);
        assert_eq!(configs.view_set(set).entry(7), Some(member));
        Ok(())
    }).unwrap();
    assert_eq!(storage.configs.live(), (2, 1, 0));
}

#[test]
fn context_only_access_and_full_budget_cleanup_follow_actual_config_roots() {
    let memory = MemoryBudget::new(64 << 20);
    let (mut storage, grammar_memory) = storage(&memory);
    let work_memory = MemoryBudget::new(1 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    let blocker = with_prediction_storage(&mut storage, &work, |access| {
        let set = {
            let (configs, contexts) = access.parts();
            let mut context = contexts.empty();
            for _ in 0..20_000 { context = contexts.singleton(context, contexts.grammar().state(1)?)?; }
            let config = joint_config(configs, contexts, context, 1)?;
            let set = configs.new_set(SetMode::Full)?;
            configs.add_uncached(set, config, contexts)?;
            set
        };
        access.fixture_state(0, Some((set, set)))?;
        Ok(memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap())
    }).unwrap();
    assert_eq!(memory.used_bytes(), memory.limit_bytes());
    assert_eq!(storage.configs.live(), (1, 1, 0));
    assert_eq!(storage.contexts.live(), 20_000);
    drop(work);
    assert_eq!(work_memory.used_bytes(), 0);
    let context_work_memory = MemoryBudget::new(4096);
    let context_work = AllocationScope::new(&context_work_memory).unwrap();
    storage.with_contexts(&context_work, |contexts| {
        assert!(contexts.view(contexts.empty()).is_empty());
        Ok(())
    }).unwrap();
    assert_eq!(memory.used_bytes(), memory.limit_bytes());
    assert_eq!(storage.configs.live(), (1, 1, 0));
    assert_eq!(storage.contexts.live(), 20_000);
    drop(blocker);
    with_prediction_storage(&mut storage, &context_work, |access| access.fixture_state(0, None)).unwrap();
    assert_eq!(storage.configs.live(), (0, 0, 0));
    assert_eq!(storage.contexts.live(), 0);
    drop(storage);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(grammar_memory.used_bytes(), 0);
}

#[test]
fn fixture_map_keys_and_values_retain_independent_sets_until_actual_replacement() {
    let memory = MemoryBudget::new(4 << 20);
    let (mut storage, _) = storage(&memory);
    let work_memory = MemoryBudget::new(4 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    let (a, b, c) = with_prediction_storage(&mut storage, &work, |access| {
        let (a, b, c) = {
            let (configs, contexts) = access.parts();
            let mut sets = [None; 3];
            for state in 1..=3 {
                let context = contexts.singleton(contexts.empty(), contexts.grammar().state(state)?)?;
                let config = joint_config(configs, contexts, context, 1)?;
                let set = configs.new_set(SetMode::Full)?;
                configs.add_uncached(set, config, contexts)?;
                sets[(state - 1) as usize] = Some(set);
            }
            (sets[0].unwrap(), sets[1].unwrap(), sets[2].unwrap())
        };
        access.fixture_state(0, Some((a, b)))?;
        access.fixture_state(1, Some((c, a)))?;
        Ok((a.identity(), b.identity(), c.identity()))
    }).unwrap();
    assert_eq!(storage.configs.live(), (3, 3, 0));
    with_prediction_storage(&mut storage, &work, |access| {
        let b = { let (configs, _) = access.parts(); configs.restore_set(b)? };
        access.fixture_state(0, Some((b, b)))?;
        access.fixture_state(1, None)
    }).unwrap();
    assert_eq!(storage.configs.live(), (1, 1, 0));
    assert_eq!(storage.contexts.live(), 1);
    for stale in [a, c] {
        with_prediction_storage(&mut storage, &work, |access| {
            let (configs, _) = access.parts();
            let kept = configs.restore_set(b)?;
            assert_eq!(configs.view_set(kept).len(), 1);
            assert!(matches!(configs.restore_set(stale), Err(PredictionError::Artifact(Problem::Context))));
            Ok(())
        }).unwrap();
    }
}

#[test]
fn joint_errors_and_unwinding_collect_unrooted_state_and_keep_completed_roots() {
    let memory = MemoryBudget::new(4 << 20);
    let (mut storage, _) = storage(&memory);
    let work_memory = MemoryBudget::new(4 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    let root = with_prediction_storage(&mut storage, &work, |access| {
        let set = {
            let (configs, contexts) = access.parts();
            let config = joint_config(configs, contexts, contexts.empty(), 1)?;
            let set = configs.new_set(SetMode::Full)?;
            configs.add_uncached(set, config, contexts)?;
            set
        };
        access.fixture_state(0, Some((set, set)))?;
        Ok(set.identity())
    }).unwrap();
    let result: Result<()> = with_prediction_storage(&mut storage, &work, |access| {
        let (configs, contexts) = access.parts();
        let context = contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
        let config = joint_config(configs, contexts, context, 2)?;
        let set = configs.new_set(SetMode::Full)?;
        configs.add_uncached(set, config, contexts)?;
        Err(PredictionError::Artifact(Problem::Context))
    });
    assert!(matches!(result, Err(PredictionError::Artifact(Problem::Context))));
    assert_eq!(storage.configs.live(), (1, 1, 0));
    assert_eq!(storage.contexts.live(), 0);
    let result = catch_unwind(AssertUnwindSafe(|| {
        with_prediction_storage(&mut storage, &work, |access| -> Result<()> {
            let (configs, contexts) = access.parts();
            let context = contexts.singleton(contexts.empty(), contexts.grammar().state(2)?)?;
            let config = joint_config(configs, contexts, context, 3)?;
            let set = configs.new_set(SetMode::Full)?;
            configs.add_uncached(set, config, contexts)?;
            panic!("caller unwinds joint access");
        })
    }));
    assert!(result.is_err());
    assert_eq!(storage.configs.live(), (1, 1, 0));
    assert_eq!(storage.contexts.live(), 0);
    with_prediction_storage(&mut storage, &work, |access| {
        let (configs, _) = access.parts();
        let root = configs.restore_set(root)?;
        assert_eq!(configs.view_set(root).len(), 1);
        Ok(())
    }).unwrap();
}

#[test]
fn completed_optimization_updates_survive_a_later_failed_access() {
    let memory = MemoryBudget::new(4 << 20);
    let (mut storage, _) = storage(&memory);
    let work_memory = MemoryBudget::new(4 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    let (set, first, second, canonical, old_equal, deep) = with_prediction_storage(&mut storage, &work, |access| {
        let (set, first, second, canonical, equal, deep) = {
            let (configs, contexts) = access.parts();
            let canonical = contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
            contexts.canonicalize(canonical)?;
            let equal = contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
            let mut deep = contexts.empty();
            for _ in 0..64 { deep = contexts.singleton(deep, contexts.grammar().state(2)?)?; }
            let first = joint_config(configs, contexts, equal, 1)?;
            let second = joint_config(configs, contexts, deep, 2)?;
            let set = configs.new_set(SetMode::Full)?;
            configs.add_uncached(set, first, contexts)?;
            configs.add_uncached(set, second, contexts)?;
            (set, first.identity(), second.identity(), canonical.identity(), equal.identity(), deep.identity())
        };
        access.fixture_state(0, Some((set, set)))?;
        Ok((set.identity(), first, second, canonical, equal, deep))
    }).unwrap();
    drop(work);
    let work = AllocationScope::new(&work_memory).unwrap();
    with_prediction_storage(&mut storage, &work, |access| {
        let (configs, contexts) = access.parts();
        let canonical_key = contexts.restore(canonical)?;
        contexts.canonicalize(canonical_key)?;
        let set = configs.restore_set(set)?;
        let blocker = work_memory.reserve(work_memory.limit_bytes() - work_memory.used_bytes()).unwrap();
        let first = configs.restore_config(first)?;
        let second = configs.restore_config(second)?;
        admission(configs.optimize(set, contexts));
        assert_eq!(configs.view(first).context().identity(), canonical);
        assert_eq!(configs.view(second).context().identity(), deep);
        drop(blocker);
        Ok(())
    }).unwrap();
    with_prediction_storage(&mut storage, &work, |access| {
        let (configs, contexts) = access.parts();
        let first = configs.restore_config(first)?;
        let second = configs.restore_config(second)?;
        assert_eq!(configs.view(first).context().identity(), canonical);
        assert_eq!(configs.view(second).context().identity(), deep);
        assert!(matches!(contexts.restore(old_equal), Err(PredictionError::Artifact(Problem::Context))));
        Ok(())
    }).unwrap();
}

#[test]
fn empty_joint_access_returns_only_independent_facts_without_allocating_stores() {
    let memory = MemoryBudget::new(0);
    let (mut storage, grammar_memory) = storage(&memory);
    let work_memory = MemoryBudget::new(4096);
    let work = AllocationScope::new(&work_memory).unwrap();
    let facts = with_prediction_storage(&mut storage, &work, |access| {
        let (configs, contexts) = access.parts();
        let alternative = configs.alternative(contexts.grammar().decision(65)?, 1)?;
        Ok((alternative.number(), contexts.view(contexts.empty()).shape()))
    }).unwrap();
    assert_eq!(facts, (1, ContextShape::Empty));
    assert_eq!(storage.configs.live(), (0, 0, 0));
    assert_eq!(storage.contexts.live(), 0);
    assert_eq!(memory.used_bytes(), 0);
    assert!(std::mem::needs_drop::<Configurations<'_, '_, '_, '_, Retained>>());
    assert!(std::mem::needs_drop::<PredictionAccess<'_, '_, '_, '_>>());
    drop(work);
    assert_eq!(work_memory.used_bytes(), 0);
    drop(storage);
    assert_eq!(grammar_memory.used_bytes(), 0);
}

#[test]
fn joint_region_moves_and_keeps_config_leases_after_creator_budget_handles_drop() {
    let memory = MemoryBudget::new(16 << 20);
    let (mut storage, root) = {
        let grammar_memory = memory.scope();
        let retained_memory = memory.scope();
        let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
        let mut storage = PredictionStorage::new(grammar, &retained_memory);
        let work_memory = MemoryBudget::new(1 << 20);
        let work = AllocationScope::new(&work_memory).unwrap();
        let root = with_prediction_storage(&mut storage, &work, |access| {
            let set = {
                let (configs, contexts) = access.parts();
                let context = contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
                let config = joint_config(configs, contexts, context, 1)?;
                let set = configs.new_set(SetMode::Full)?;
                configs.add_uncached(set, config, contexts)?;
                set
            };
            access.fixture_state(0, Some((set, set)))?;
            Ok(set.identity())
        }).unwrap();
        (storage, root)
    };
    let retained = memory.used_bytes();
    assert!(retained > 0);
    let work_memory = MemoryBudget::new(1 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    with_prediction_storage(&mut storage, &work, |access| {
        let (configs, contexts) = access.parts();
        let root = configs.restore_set(root)?;
        let config = configs.view_set(root).entry(0).unwrap();
        assert_eq!(contexts.view(configs.view(config).context()).follow(0)?.unwrap().number(), 1);
        configs.increment_outer(config)?;
        assert_eq!(configs.view(config).outer_depth(), 1);
        Ok(())
    }).unwrap();
    assert_eq!(memory.used_bytes(), retained);
    drop(work);
    assert_eq!(work_memory.used_bytes(), 0);
    drop(storage);
    assert_eq!(memory.used_bytes(), 0);
}
