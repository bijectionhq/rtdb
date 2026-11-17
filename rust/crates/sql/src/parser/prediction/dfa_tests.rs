use std::{mem::size_of, panic::{AssertUnwindSafe, catch_unwind}, sync::Arc};
use rtdb_types::MemoryBudget;
use crate::parser::grammar::OwnedGrammar;
use super::*;
use super::super::{AlternativeSet, ConfigKey, ContextKey, MergeMode, PredictionAccess, PredictionStorage, with_prediction_storage};

fn region(memory: &Arc<MemoryBudget>) -> (PredictionStorage, Arc<MemoryBudget>) {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
    (PredictionStorage::new(grammar, memory), grammar_memory)
}
fn open<R>(storage: &mut PredictionStorage, run: impl for<'cfg, 'id, 'access> FnOnce(&mut PredictionAccess<'cfg, 'id, 'access, 'access>, &Arc<MemoryBudget>) -> Result<R>) -> Result<R> {
    let memory = MemoryBudget::new(64 << 20);
    let work = AllocationScope::new(&memory).unwrap();
    let result = with_prediction_storage(storage, &work, |access| run(access, &memory));
    drop(work);
    assert_eq!(memory.used_bytes(), 0);
    result
}
fn fixture<R>(limit: usize, run: impl for<'cfg, 'id, 'access> FnOnce(&mut PredictionAccess<'cfg, 'id, 'access, 'access>, &Arc<MemoryBudget>, &Arc<MemoryBudget>) -> Result<R>) -> R {
    let memory = MemoryBudget::new(limit);
    let (mut storage, grammar_memory) = region(&memory);
    let result = open(&mut storage, |access, work| run(access, &memory, work)).unwrap();
    drop(storage);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(grammar_memory.used_bytes(), 0);
    result
}
fn one<'cfg, 'id, 'g, 'scope>(configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>, contexts: &mut PredictionContexts<'id, 'g, 'scope, Retained>, decision: u16, state: u32, context: ContextKey<'id>) -> Result<(ConfigSetKey<'cfg>, ConfigKey<'cfg>)> {
    let alt = configs.alternative(contexts.grammar().decision(decision)?, 1)?;
    let config = configs.create(contexts.grammar().state(state)?, alt, context, Semantic::Unpredicated)?;
    let set = configs.new_set(SetMode::Local)?;
    configs.add_uncached(set, config, contexts)?;
    Ok((set, config))
}
fn interned<'cfg>(target: DfaTarget<'cfg>) -> InternedStateKey<'cfg> { match target { DfaTarget::Interned(key) => key, DfaTarget::Error => panic!("real state") } }
fn nested<'id, 'g, 'scope>(contexts: &mut PredictionContexts<'id, 'g, 'scope, Retained>, depth: usize) -> Result<ContextKey<'id>> {
    let mut parent = contexts.empty();
    for _ in 0..depth {
        let a = contexts.singleton(parent, contexts.grammar().state(1)?)?;
        let b = contexts.singleton(contexts.empty(), contexts.grammar().state(2)?)?;
        parent = contexts.merge_uncached(a, b, MergeMode::LocalWildcard)?;
    }
    Ok(parent)
}
fn admission<T>(result: Result<T>) { assert!(matches!(result, Err(PredictionError::Admission(error)) if error.code == "rtdb/memory-budget-exceeded")); }
fn phase_error<T>(result: Result<T>) { assert!(matches!(result, Err(PredictionError::Artifact(Problem::DfaPhase)))); }
fn decision_error<T>(result: Result<T>) { assert!(matches!(result, Err(PredictionError::Artifact(Problem::DfaDecision)))); }

#[test]
fn initialization_owns_all_decisions_and_only_five_separate_synthetic_roots() {
    fixture(16 << 20, |access, _, _| {
        let (dfas, configs, _) = access.dfa_parts();
        assert!(dfas.store.decisions.is_empty());
        dfas.initialize(configs)?;
        let mut precedence = Vec::new();
        for number in 0..372 {
            let view = dfas.view_decision(number)?;
            assert_eq!(view.number(), number);
            if view.precedence() { precedence.push((number, view.atn_start())); }
            assert_eq!(dfas.state_count(number)?, 0);
            assert!(dfas.start(number, 0)?.is_none());
        }
        assert_eq!(precedence, [(83,957),(99,1131),(153,1661),(220,2084),(286,2512)]);
        assert_eq!(dfas.store.objects.len(), 5);
        for state in dfas.store.objects.values() {
            let Body::Synthetic { prefilter, edges } = &state.body else { panic!("synthetic initialization") };
            assert!(edges.is_empty());
            let set = configs.bind_set(*prefilter)?;
            assert_eq!(configs.view_set(set).mode(), SetMode::Full);
            assert_eq!(configs.view_set(set).len(), 0);
        }
        Ok(())
    });
}

#[test]
fn context_only_access_does_not_initialize_or_charge_a_dfa_cache() {
    let memory = MemoryBudget::new(0);
    let (mut storage, grammar_memory) = region(&memory);
    let work_memory = MemoryBudget::new(4096);
    let work = AllocationScope::new(&work_memory).unwrap();
    assert!(storage.with_contexts(&work, |contexts| Ok(contexts.view(contexts.empty()).is_empty())).unwrap());
    assert_eq!(memory.used_bytes(), 0);
    drop(storage);
    assert_eq!(grammar_memory.used_bytes(), 0);
}

#[test]
fn equal_get_returns_the_stored_value_before_numbering_or_optimization() {
    fixture(16 << 20, |access, memory, work| {
        let (dfas, configs, contexts) = access.dfa_parts();
        dfas.initialize(configs)?;
        let parent = contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
        let (set, _) = one(configs, contexts, 65, 352, parent)?;
        let first = dfas.new_draft(65, set, configs)?;
        let stored = interned(dfas.add_state(65, Candidate::Draft(first), configs, contexts)?);
        assert_eq!(dfas.view(65, stored)?.number(), 0);
        let duplicate_parent = contexts.singleton(contexts.empty(), contexts.grammar().state(1)?)?;
        let (set, config) = one(configs, contexts, 65, 352, duplicate_parent)?;
        let duplicate = dfas.new_draft(65, set, configs)?;
        let retained_blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        let work_blocker = work.reserve(work.limit_bytes() - work.used_bytes()).unwrap();
        let before = (memory.used_bytes(), work.used_bytes());
        assert_eq!(dfas.add_state(65, Candidate::Draft(duplicate), configs, contexts)?, DfaTarget::Interned(stored));
        assert_eq!(dfas.view_draft(65, duplicate)?.phase(), Phase::UnnumberedDraft);
        assert!(!configs.view_set(set).readonly());
        assert_eq!(configs.view(config).context(), duplicate_parent);
        assert_eq!((memory.used_bytes(), work.used_bytes()), before);
        drop((retained_blocker, work_blocker));
        Ok(())
    });
}

#[test]
fn duplicate_order_conflict_identity_and_insertion_numbering_remain_part_of_keys() {
    fixture(32 << 20, |access, _, _| {
        let (dfas, configs, contexts) = access.dfa_parts();
        dfas.initialize(configs)?;
        let empty = contexts.empty();
        let (base, object) = one(configs, contexts, 65, 352, empty)?;
        let a = dfas.new_draft(65, base, configs)?;
        let a = interned(dfas.add_state(65, Candidate::Draft(a), configs, contexts)?);
        let duplicate = configs.copy_set(base, contexts)?;
        configs.add_uncached(duplicate, object, contexts)?;
        let b = dfas.new_draft(65, duplicate, configs)?;
        let b = interned(dfas.add_state(65, Candidate::Draft(b), configs, contexts)?);
        let mut bits = AlternativeSet::default(); bits.insert(configs.view(object).alternative());
        let conflict_a = configs.publish_conflict(bits)?;
        let conflict_b = configs.publish_conflict(bits)?;
        assert_ne!(conflict_a, conflict_b);
        let mut keys = [a, b, a, a];
        for (index, conflict) in [conflict_a, conflict_b].into_iter().enumerate() {
            let set = configs.copy_set(base, contexts)?;
            configs.set_conflicting(set, Some(conflict))?;
            let draft = dfas.new_draft(65, set, configs)?;
            keys[index + 2] = interned(dfas.add_state(65, Candidate::Draft(draft), configs, contexts)?);
        }
        for (number, key) in keys.into_iter().enumerate() {
            assert_eq!(dfas.view(65, key)?.number(), number as i32);
            assert_eq!(dfas.state_at(65, number as i32)?, Some(key));
        }
        assert_eq!(dfas.state_count(65)?, 4);
        Ok(())
    });
}

#[test]
fn copied_draft_alias_cannot_edit_acceptance_after_intern() {
    fixture(16 << 20, |access, memory, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty(); let (set, config) = one(configs, contexts, 65, 352, empty)?;
        let alt = configs.view(config).alternative(); configs.set_unique(set, Some(alt))?;
        let draft = dfas.new_draft(65, set, configs)?; let alias = draft;
        dfas.complete_acceptance(65, draft, Acceptance::Plain { reason: AcceptReason::Unique, prediction: alt }, configs)?;
        let key = interned(dfas.add_state(65, Candidate::Draft(draft), configs, contexts)?);
        let before = memory.used_bytes();
        phase_error(dfas.complete_acceptance(65, alias, Acceptance::NonAccept, configs));
        assert!(matches!(dfas.view(65, key)?.acceptance(), Acceptance::Plain { reason: AcceptReason::Unique, prediction } if prediction == alt));
        assert_eq!(memory.used_bytes(), before);
        assert!(matches!(dfas.add_state(65, Candidate::Error, configs, contexts), Err(PredictionError::Aborted)));
        Ok(())
    });
}

#[test]
fn completed_predicate_acceptance_retains_order_and_rejects_an_empty_block() {
    fixture(16 << 20, |access, _, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty(); let (set, object) = one(configs, contexts, 65, 352, empty)?;
        let first = configs.view(object).alternative();
        let second = configs.alternative(contexts.grammar().decision(65)?, 2)?;
        let pairs = [PredPrediction { semantic: Semantic::Unpredicated, alternative: second }, PredPrediction { semantic: Semantic::Unpredicated, alternative: first }];
        let draft = dfas.new_draft(65, set, configs)?;
        dfas.complete_acceptance(65, draft, Acceptance::Predicated { reason: AcceptReason::Conflict, pairs: &pairs }, configs)?;
        let key = interned(dfas.add_state(65, Candidate::Draft(draft), configs, contexts)?);
        assert!(matches!(dfas.view(65, key)?.acceptance(), Acceptance::Predicated { reason: AcceptReason::Conflict, pairs: stored } if stored == pairs.as_slice()));
        let other = dfas.new_draft(65, set, configs)?;
        phase_error(dfas.complete_acceptance(65, other, Acceptance::Predicated { reason: AcceptReason::Unique, pairs: &[] }, configs));
        assert!(matches!(dfas.view_draft(65, other)?.acceptance(), Acceptance::NonAccept));
        Ok(())
    });
}

#[test]
fn synthetic_backing_cannot_enter_ordinary_membership_even_through_a_forged_alias() {
    fixture(16 << 20, |access, _, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let Start::Precedence(synthetic) = dfas.decision(83)?.start else { panic!("precedence") };
        phase_error(dfas.add_state(83, Candidate::Draft(DraftKey::at(synthetic.0)), configs, contexts));
        assert_eq!(dfas.view_draft(83, DraftKey::at(synthetic.0))?.phase(), Phase::Synthetic);
        assert_eq!(dfas.decision(83)?.states.len, 0);
        Ok(())
    });
}

#[test]
fn draft_aliases_cannot_become_starts_or_ordinary_edge_sources() {
    for edge in [false, true] {
        fixture(16 << 20, |access, _, _| {
            let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
            let empty = contexts.empty(); let (set, _) = one(configs, contexts, 65, 352, empty)?;
            let draft = dfas.new_draft(65, set, configs)?;
            let forged = InternedStateKey::at(InternedId(draft.id));
            if edge { phase_error(dfas.existing_edge(65, forged, -1)); }
            else { phase_error(dfas.set_start(65, 0, forged)); }
            assert_eq!(dfas.view_draft(65, draft)?.phase(), Phase::UnnumberedDraft);
            Ok(())
        });
    }
}

#[test]
fn every_stored_start_and_edge_target_checks_its_decision() {
    for precedence in [false, true] {
        fixture(16 << 20, |access, _, _| {
            let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
            let empty = contexts.empty(); let (set, _) = one(configs, contexts, 65, 352, empty)?;
            let draft = dfas.new_draft(65, set, configs)?;
            let key = interned(dfas.add_state(65, Candidate::Draft(draft), configs, contexts)?);
            decision_error(dfas.set_start(if precedence { 83 } else { 0 }, 0, key));
            assert!(matches!(dfas.view(0, key), Err(PredictionError::Artifact(Problem::DfaDecision))));
            Ok(())
        });
    }
    fixture(16 << 20, |access, _, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty(); let (set, _) = one(configs, contexts, 65, 352, empty)?;
        let draft = dfas.new_draft(65, set, configs)?;
        decision_error(dfas.add_state(0, Candidate::Draft(draft), configs, contexts));
        assert_eq!(dfas.view_draft(65, draft)?.phase(), Phase::UnnumberedDraft);
        Ok(())
    });
}

#[test]
fn missing_error_eof_and_real_edges_remain_distinct() {
    fixture(16 << 20, |access, _, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty(); let (set, _) = one(configs, contexts, 65, 352, empty)?;
        let draft = dfas.new_draft(65, set, configs)?;
        let from = interned(dfas.add_state(65, Candidate::Draft(draft), configs, contexts)?);
        assert_eq!(dfas.existing_edge(65, from, -1)?, None);
        assert_eq!(dfas.add_edge(65, Some(from), -1, Some(Candidate::Error), configs, contexts)?, Some(DfaTarget::Error));
        assert_eq!(dfas.existing_edge(65, from, -1)?, Some(DfaTarget::Error));
        assert_eq!(dfas.existing_edge(65, from, 1)?, None);
        let (set, _) = one(configs, contexts, 65, 353, empty)?;
        let draft = dfas.new_draft(65, set, configs)?;
        let to = dfas.add_edge(65, Some(from), 368, Some(Candidate::Draft(draft)), configs, contexts)?.unwrap();
        assert_eq!(dfas.existing_edge(65, from, 368)?, Some(to));
        assert_eq!(dfas.existing_edge(65, from, i32::MAX)?, None);
        assert_eq!(dfas.existing_edge(65, from, i32::MIN)?, None);
        assert_eq!(dfas.state_count(65)?, 2);
        Ok(())
    });
}

#[test]
fn target_publication_precedes_null_source_and_token_range_checks() {
    fixture(16 << 20, |access, _, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty();
        for (number, token) in [-2,369,i32::MAX].into_iter().enumerate() {
            let (set, _) = one(configs, contexts, 65, 352 + number as u32, empty)?;
            let draft = dfas.new_draft(65, set, configs)?;
            assert!(matches!(dfas.add_edge(65, None, token, Some(Candidate::Draft(draft)), configs, contexts)?, Some(DfaTarget::Interned(_))));
            assert_eq!(dfas.view_draft(65, draft)?.phase(), Phase::Interned(number as i32));
        }
        assert_eq!(dfas.add_edge(65, None, 0, None, configs, contexts)?, None);
        assert_eq!(dfas.state_count(65)?, 3);
        Ok(())
    });
}

#[test]
fn precedence_edges_grow_to_the_reached_length_with_full_replacement_overlap() {
    fixture(16 << 20, |access, memory, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty(); let (set, _) = one(configs, contexts, 83, 352, empty)?;
        let draft = dfas.new_draft(83, set, configs)?;
        let key = interned(dfas.add_state(83, Candidate::Draft(draft), configs, contexts)?);
        let before = memory.used_bytes();
        dfas.set_start(83, -1, key)?;
        assert_eq!(memory.used_bytes(), before);
        dfas.set_start(83, 2, key)?;
        let old = 256 + 3 * size_of::<Option<InternedId>>();
        assert_eq!(memory.used_bytes(), before + old);
        let growth = 256 + 10 * size_of::<Option<InternedId>>();
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes() - growth + 1).unwrap();
        let retained = memory.used_bytes();
        admission(dfas.set_start(83, 9, key));
        assert_eq!(memory.used_bytes(), retained);
        let Start::Precedence(id) = dfas.decision(83)?.start else { unreachable!() };
        let Body::Synthetic { edges, .. } = &dfas.state(83, id.0)?.body else { unreachable!() };
        assert_eq!(edges.len(), 3);
        assert_eq!(edges[2], Some(key.id));
        drop(blocker);
        Ok(())
    });
}

#[test]
fn numbered_unpublished_phase_keeps_completed_optimization_prefix_on_later_failure() {
    fixture(16 << 20, |access, _, work| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty();
        let canonical = contexts.singleton(empty, contexts.grammar().state(1)?)?;
        assert_eq!(contexts.canonicalize(canonical)?, canonical);
        let duplicate = contexts.singleton(empty, contexts.grammar().state(1)?)?;
        let (set, first) = one(configs, contexts, 65, 352, duplicate)?;
        let second_context = contexts.singleton(empty, contexts.grammar().state(2)?)?;
        let alt = configs.view(first).alternative();
        let second = configs.create(contexts.grammar().state(353)?, alt, second_context, Semantic::Unpredicated)?;
        configs.add_uncached(set, second, contexts)?;
        let draft = dfas.new_draft(65, set, configs)?;
        let blocker = work.reserve(work.limit_bytes() - work.used_bytes()).unwrap();
        admission(dfas.add_state(65, Candidate::Draft(draft), configs, contexts));
        assert_eq!(dfas.view_draft(65, draft)?.phase(), Phase::NumberedUnpublished(0));
        assert_eq!(configs.view(first).context(), canonical);
        assert_eq!(configs.view(second).context(), second_context);
        assert!(!configs.view_set(set).readonly());
        assert_eq!(dfas.decision(65)?.states.len, 0);
        drop(blocker);
        Ok(())
    });
}

#[test]
fn failed_map_admission_retains_number_and_readonly_before_the_put() {
    fixture(16 << 20, |access, memory, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty(); let (set, _) = one(configs, contexts, 65, 352, empty)?;
        let draft = dfas.new_draft(65, set, configs)?;
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        admission(dfas.add_state(65, Candidate::Draft(draft), configs, contexts));
        assert_eq!(dfas.view_draft(65, draft)?.phase(), Phase::NumberedUnpublished(0));
        assert!(configs.view_set(set).readonly());
        assert_eq!(dfas.decision(65)?.states.len, 0);
        assert!(dfas.decision(65)?.states.slots.is_empty());
        drop(blocker);
        Ok(())
    });
}

#[test]
fn failed_edge_storage_does_not_undo_the_interned_target() {
    fixture(16 << 20, |access, memory, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty(); let (set, _) = one(configs, contexts, 65, 352, empty)?;
        let from = dfas.new_draft(65, set, configs)?;
        let from = interned(dfas.add_state(65, Candidate::Draft(from), configs, contexts)?);
        let (set, _) = one(configs, contexts, 65, 353, empty)?;
        let target = dfas.new_draft(65, set, configs)?;
        configs.mark_readonly(set)?;
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        admission(dfas.add_edge(65, Some(from), -1, Some(Candidate::Draft(target)), configs, contexts));
        assert_eq!(dfas.view_draft(65, target)?.phase(), Phase::Interned(1));
        assert_eq!(dfas.decision(65)?.states.len, 2);
        assert!(matches!(&dfas.view(65, from)?.state.body, Body::Ordinary { phase: OrdinaryPhase::Interned { edges, .. }, .. } if edges.is_empty()));
        drop(blocker);
        Ok(())
    });
}

#[test]
fn full_budget_collision_walks_find_the_unique_value_without_work_admission() {
    fixture(64 << 20, |access, memory, work| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let canonical_input = nested(contexts, 96)?;
        let mut last = None;
        let mut conflict = None;
        let mut stored_hash = None;
        for number in 0..40 {
            let (set, object) = one(configs, contexts, 65, 352, canonical_input)?;
            let mut bits = AlternativeSet::default(); bits.insert(configs.view(object).alternative());
            let identity = configs.publish_conflict(bits)?;
            configs.set_conflicting(set, Some(identity))?;
            let draft = dfas.new_draft(65, set, configs)?;
            let key = interned(dfas.add_state(65, Candidate::Draft(draft), configs, contexts)?);
            let hash = dfas.hash(set, configs, contexts)?;
            assert_eq!(*stored_hash.get_or_insert(hash), hash);
            assert_eq!(dfas.view(65, key)?.number(), number);
            last = Some(key); conflict = Some(identity);
        }
        let hashes: Vec<_> = dfas.decision(65)?.states.slots.iter().flatten().map(|record| record.hash).collect();
        assert_eq!(hashes.len(), 40);
        assert!(hashes.iter().all(|hash| Some(*hash) == stored_hash));
        assert_eq!(dfas.decision(65)?.states.slots.len(), 128);
        let query_parent = nested(contexts, 96)?;
        let (query, _) = one(configs, contexts, 65, 352, query_parent)?;
        configs.set_conflicting(query, conflict)?;
        let query = dfas.new_draft(65, query, configs)?;
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        let work_blocker = work.reserve(work.limit_bytes() - work.used_bytes()).unwrap();
        let before = (memory.used_bytes(), work.used_bytes());
        assert_eq!(dfas.add_state(65, Candidate::Draft(query), configs, contexts)?, DfaTarget::Interned(last.unwrap()));
        assert_eq!(dfas.view_draft(65, query)?.phase(), Phase::UnnumberedDraft);
        assert_eq!((memory.used_bytes(), work.used_bytes()), before);
        drop((blocker, work_blocker));
        Ok(())
    });
}

#[test]
fn actual_map_keys_and_values_root_independently_across_context_only_access() {
    let memory = MemoryBudget::new(32 << 20); let (mut storage, grammar_memory) = region(&memory);
    let (key_id, value_id, key_set, value_set) = open(&mut storage, |access, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty();
        let (key_set, _) = one(configs, contexts, 65, 352, empty)?;
        let key = dfas.new_draft(65, key_set, configs)?; let key = interned(dfas.add_state(65, Candidate::Draft(key), configs, contexts)?);
        let (value_set, _) = one(configs, contexts, 65, 353, empty)?;
        let value = dfas.new_draft(65, value_set, configs)?; let value = interned(dfas.add_state(65, Candidate::Draft(value), configs, contexts)?);
        dfas.interned(65, key.id)?; dfas.interned(65, value.id)?;
        let map = &mut dfas.store.decisions[65].states;
        for slot in map.slots.iter_mut() {
            if let Some(record) = slot {
                if record.key == value.id { *slot = None; }
                else if record.key == key.id { record.value = value.id; }
            }
        }
        map.len = 1;
        Ok((key.id, value.id, key_set.stored(), value_set.stored()))
    }).unwrap();
    let work_memory = MemoryBudget::new(4096); let work = AllocationScope::new(&work_memory).unwrap();
    storage.with_contexts(&work, |contexts| Ok(contexts.view(contexts.empty()).is_empty())).unwrap(); drop(work);
    open(&mut storage, |access, _| {
        let (dfas, configs, contexts) = access.dfa_parts();
        assert_eq!(dfas.store.objects.len(), 7);
        assert!(configs.bind_set(key_set).is_ok()); assert!(configs.bind_set(value_set).is_ok());
        let empty = contexts.empty(); let (query_set, _) = one(configs, contexts, 65, 352, empty)?;
        let query = dfas.new_draft(65, query_set, configs)?;
        assert_eq!(dfas.add_state(65, Candidate::Draft(query), configs, contexts)?, DfaTarget::Interned(InternedStateKey::at(value_id)));
        assert_eq!(dfas.state_at(65, 0)?, Some(InternedStateKey::at(key_id)));
        dfas.store.decisions[65].states.slots.fill(None); dfas.store.decisions[65].states.len = 0;
        Ok(())
    }).unwrap();
    open(&mut storage, |access, _| {
        let (dfas, configs, _) = access.dfa_parts();
        assert_eq!(dfas.store.objects.len(), 5);
        phase_error(dfas.view(65, InternedStateKey::at(key_id)));
        phase_error(dfas.view(65, InternedStateKey::at(value_id)));
        assert!(configs.bind_set(key_set).is_err()); assert!(configs.bind_set(value_set).is_err());
        Ok(())
    }).unwrap();
    drop(storage); assert_eq!(memory.used_bytes(), 0); assert_eq!(grammar_memory.used_bytes(), 0);
}

#[test]
fn starts_and_cycles_keep_edge_only_states_alive_without_recursive_collection() {
    let memory = MemoryBudget::new(32 << 20); let (mut storage, _) = region(&memory);
    let (a, b) = open(&mut storage, |access, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty(); let (a, _) = one(configs, contexts, 65, 352, empty)?;
        let a = dfas.new_draft(65, a, configs)?; let a = interned(dfas.add_state(65, Candidate::Draft(a), configs, contexts)?);
        let (b, _) = one(configs, contexts, 65, 353, empty)?;
        let b = dfas.new_draft(65, b, configs)?; let b = interned(dfas.add_state(65, Candidate::Draft(b), configs, contexts)?);
        dfas.add_edge(65, Some(a), 1, Some(Candidate::Existing(b)), configs, contexts)?;
        dfas.add_edge(65, Some(b), 2, Some(Candidate::Existing(a)), configs, contexts)?;
        dfas.set_start(65, 0, a)?;
        dfas.store.decisions[65].states.slots.fill(None); dfas.store.decisions[65].states.len = 0;
        Ok((a.id, b.id))
    }).unwrap();
    open(&mut storage, |access, _| {
        let (dfas, _, _) = access.dfa_parts();
        assert_eq!(dfas.start(65, 0)?, Some(InternedStateKey::at(a)));
        assert_eq!(dfas.existing_edge(65, InternedStateKey::at(a), 1)?, Some(DfaTarget::Interned(InternedStateKey::at(b))));
        assert_eq!(dfas.existing_edge(65, InternedStateKey::at(b), 2)?, Some(DfaTarget::Interned(InternedStateKey::at(a))));
        dfas.store.decisions[65].start = Start::Ordinary(None);
        Ok(())
    }).unwrap();
    open(&mut storage, |access, _| {
        let (dfas, _, _) = access.dfa_parts(); assert_eq!(dfas.store.objects.len(), 5);
        phase_error(dfas.view(65, InternedStateKey::at(a))); phase_error(dfas.view(65, InternedStateKey::at(b))); Ok(())
    }).unwrap();
    drop(storage); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn synthetic_prefilter_traces_current_merged_context_and_releases_the_old_root() {
    let memory = MemoryBudget::new(32 << 20); let (mut storage, _) = region(&memory);
    let (set_id, old_context, current_context) = open(&mut storage, |access, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty(); let a = contexts.singleton(empty, contexts.grammar().state(1)?)?;
        let (set, first) = one(configs, contexts, 83, 352, a)?;
        dfas.replace_synthetic_prefilter(83, set, configs)?;
        let b = contexts.singleton(empty, contexts.grammar().state(2)?)?;
        let incoming = configs.create(contexts.grammar().state(352)?, configs.view(first).alternative(), b, Semantic::Unpredicated)?;
        configs.add_uncached(set, incoming, contexts)?;
        let current = configs.view(first).context();
        assert_eq!(contexts.view(current).shape(), super::super::ContextShape::Array);
        Ok((set.stored(), a.stored(), current.stored()))
    }).unwrap();
    open(&mut storage, |access, _| {
        let (dfas, configs, contexts) = access.dfa_parts();
        let set = configs.bind_set(set_id)?;
        let object = configs.view_set(set).entry(0).unwrap();
        assert_eq!(configs.view(object).context().stored(), current_context);
        let replacement = configs.new_set(SetMode::Local)?;
        dfas.replace_synthetic_prefilter(83, replacement, configs)?;
        assert!(matches!(contexts.restore(old_context), Err(PredictionError::Artifact(Problem::Context))));
        Ok(())
    }).unwrap();
    open(&mut storage, |access, _| {
        let (_, configs, contexts) = access.dfa_parts();
        assert!(configs.bind_set(set_id).is_err());
        assert!(matches!(contexts.restore(current_context), Err(PredictionError::Artifact(Problem::Context)))); Ok(())
    }).unwrap();
    drop(storage); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn synthetic_building_membership_roots_a_config_before_a_failed_ordered_append() {
    let memory = MemoryBudget::new(32 << 20); let (mut storage, _) = region(&memory);
    let (set_id, incoming_id) = open(&mut storage, |access, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty(); let (set, first) = one(configs, contexts, 83, 352, empty)?;
        for _ in 1..7 { configs.add_uncached(set, first, contexts)?; }
        let parent = contexts.singleton(empty, contexts.grammar().state(1)?)?;
        let incoming = configs.create(contexts.grammar().state(353)?, configs.view(first).alternative(), parent, Semantic::Unpredicated)?;
        dfas.replace_synthetic_prefilter(83, set, configs)?;
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        admission(configs.add_uncached(set, incoming, contexts));
        assert_eq!(configs.view_set(set).len(), 7);
        drop(blocker);
        Ok((set.stored(), incoming.identity()))
    }).unwrap();
    open(&mut storage, |access, _| {
        let (_, configs, contexts) = access.dfa_parts();
        let set = configs.bind_set(set_id)?; let incoming = configs.restore_config(incoming_id)?;
        assert_eq!(contexts.view(configs.view(incoming).context()).shape(), super::super::ContextShape::Singleton);
        configs.add_uncached(set, incoming, contexts)?;
        assert_eq!(configs.view_set(set).len(), 8);
        assert_eq!(configs.view_set(set).entry(7), Some(incoming));
        Ok(())
    }).unwrap();
    drop(storage); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn stale_state_generations_and_exhausted_slots_never_bind_to_replacements() {
    let memory = MemoryBudget::new(16 << 20); let (mut storage, _) = region(&memory);
    let old = open(&mut storage, |access, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty(); let (set, _) = one(configs, contexts, 65, 352, empty)?;
        let draft = dfas.new_draft(65, set, configs)?; Ok(draft.id)
    }).unwrap();
    let exhausted = open(&mut storage, |access, _| {
        let (dfas, configs, contexts) = access.dfa_parts();
        let empty = contexts.empty(); let (set, _) = one(configs, contexts, 65, 352, empty)?;
        let draft = dfas.new_draft(65, set, configs)?;
        assert_eq!(draft.id.0.index, old.0.index); assert_ne!(draft.id.0.generation, old.0.generation);
        phase_error(dfas.view_draft(65, DraftKey::at(old)));
        let id = dfas.store.objects.fixture_generation(draft.id.0, u64::MAX);
        Ok(StateId(id))
    }).unwrap();
    open(&mut storage, |access, _| {
        let (dfas, configs, contexts) = access.dfa_parts();
        let empty = contexts.empty(); let (set, _) = one(configs, contexts, 65, 352, empty)?;
        let draft = dfas.new_draft(65, set, configs)?;
        assert_ne!(draft.id.0.index, exhausted.0.index);
        phase_error(dfas.view_draft(65, DraftKey::at(exhausted)));
        Ok(())
    }).unwrap();
    drop(storage); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn predicate_admission_and_state_pool_growth_fail_before_publication() {
    fixture(16 << 20, |access, memory, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty(); let (set, object) = one(configs, contexts, 65, 352, empty)?;
        let draft = dfas.new_draft(65, set, configs)?;
        let pair = PredPrediction { semantic: Semantic::Unpredicated, alternative: configs.view(object).alternative() };
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        admission(dfas.complete_acceptance(65, draft, Acceptance::Predicated { reason: AcceptReason::Unique, pairs: &[pair] }, configs));
        assert!(matches!(dfas.view_draft(65, draft)?.acceptance(), Acceptance::NonAccept));
        assert_eq!(dfas.view_draft(65, draft)?.phase(), Phase::UnnumberedDraft);
        drop(blocker); Ok(())
    });
    fixture(16 << 20, |access, memory, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty(); let (set, _) = one(configs, contexts, 83, 352, empty)?;
        dfas.replace_synthetic_prefilter(83, set, configs)?;
        for _ in 0..11 { dfas.new_draft(83, set, configs)?; }
        assert_eq!(dfas.store.objects.len(), 16);
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        let before = memory.used_bytes(); admission(dfas.new_draft(83, set, configs));
        assert_eq!(dfas.store.objects.len(), 16); assert_eq!(memory.used_bytes(), before);
        drop(blocker); Ok(())
    });
}

#[test]
fn failed_initialization_releases_the_unpublished_descriptor_and_fresh_access_can_retry() {
    let memory = MemoryBudget::new(16 << 20); let (mut storage, _) = region(&memory);
    let descriptor = 256 + DECISIONS * size_of::<DecisionDfa<Retained>>();
    let blocker = memory.reserve(memory.limit_bytes() - descriptor + 1).unwrap();
    open(&mut storage, |access, _| {
        let (dfas, configs, _) = access.dfa_parts(); admission(dfas.initialize(configs));
        assert!(dfas.store.decisions.is_empty()); assert_eq!(dfas.store.objects.len(), 0);
        assert!(matches!(dfas.initialize(configs), Err(PredictionError::Aborted)));
        Ok(())
    }).unwrap();
    assert_eq!(memory.used_bytes(), blocker.bytes()); drop(blocker);
    open(&mut storage, |access, _| {
        let (dfas, configs, _) = access.dfa_parts(); dfas.initialize(configs)?;
        let before = memory.used_bytes(); dfas.initialize(configs)?;
        assert_eq!(memory.used_bytes(), before); assert_eq!(dfas.store.decisions.len(), 372);
        Ok(())
    }).unwrap();
    drop(storage); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn error_and_unwind_keep_completed_map_publications_with_fresh_access_flags() {
    for unwind in [false, true] {
        let memory = MemoryBudget::new(16 << 20); let (mut storage, _) = region(&memory);
        if unwind {
            assert!(catch_unwind(AssertUnwindSafe(|| {
                let _: Result<()> = open(&mut storage, |access, _| {
                    let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
                    let empty = contexts.empty(); let (set, _) = one(configs, contexts, 65, 352, empty)?;
                    let draft = dfas.new_draft(65, set, configs)?; dfas.add_state(65, Candidate::Draft(draft), configs, contexts)?;
                    panic!("after cache publication");
                });
            })).is_err());
        } else {
            let result: Result<()> = open(&mut storage, |access, _| {
                let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
                let empty = contexts.empty(); let (set, _) = one(configs, contexts, 65, 352, empty)?;
                let draft = dfas.new_draft(65, set, configs)?; dfas.add_state(65, Candidate::Draft(draft), configs, contexts)?;
                Err(PredictionError::Artifact(Problem::DfaPhase))
            }); phase_error(result);
        }
        open(&mut storage, |access, _| {
            let (dfas, configs, contexts) = access.dfa_parts(); assert_eq!(dfas.state_count(65)?, 1);
            let key = dfas.state_at(65, 0)?.unwrap();
            assert_eq!(dfas.add_state(65, Candidate::Existing(key), configs, contexts)?, DfaTarget::Interned(key));
            Ok(())
        }).unwrap();
        drop(storage); assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn aborted_participants_are_checked_before_error_missing_and_cached_shortcuts() {
    for shortcut in 0..3 {
        fixture(16 << 20, |access, memory, _| {
            let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
            let empty = contexts.empty(); let (set, _) = one(configs, contexts, 65, 352, empty)?;
            let draft = dfas.new_draft(65, set, configs)?; let key = interned(dfas.add_state(65, Candidate::Draft(draft), configs, contexts)?);
            let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
            admission(contexts.singleton(empty, contexts.grammar().state(1)?)); drop(blocker);
            let before = memory.used_bytes();
            let result = match shortcut {
                0 => dfas.add_state(65, Candidate::Error, configs, contexts).map(|_| ()),
                1 => dfas.add_edge(65, None, -1, None, configs, contexts).map(|_| ()),
                _ => dfas.add_state(65, Candidate::Existing(key), configs, contexts).map(|_| ()),
            };
            assert!(matches!(result, Err(PredictionError::Aborted)));
            assert!(matches!(configs.new_set(SetMode::Local), Err(PredictionError::Aborted)));
            assert!(matches!(dfas.start(65, 0), Err(PredictionError::Aborted)));
            assert_eq!(memory.used_bytes(), before); Ok(())
        });
    }
}

#[test]
fn teardown_under_full_budget_releases_cached_arrays_predicates_and_owned_grammar() {
    let memory = MemoryBudget::new(64 << 20); let (mut storage, grammar_memory) = region(&memory);
    open(&mut storage, |access, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let parent = nested(contexts, 128)?; let (set, object) = one(configs, contexts, 83, 352, parent)?;
        let draft = dfas.new_draft(83, set, configs)?;
        let pair = PredPrediction { semantic: Semantic::Unpredicated, alternative: configs.view(object).alternative() };
        dfas.complete_acceptance(83, draft, Acceptance::Predicated { reason: AcceptReason::Unique, pairs: &[pair] }, configs)?;
        let key = interned(dfas.add_state(83, Candidate::Draft(draft), configs, contexts)?);
        dfas.set_start(83, 68, key)?;
        dfas.add_edge(83, Some(key), -1, Some(Candidate::Error), configs, contexts)?; Ok(())
    }).unwrap();
    let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
    let work_memory = MemoryBudget::new(4096); let work = AllocationScope::new(&work_memory).unwrap();
    let result: Result<()> = storage.with_contexts(&work, |_| Err(PredictionError::Artifact(Problem::Context)));
    assert!(result.is_err()); assert_eq!(memory.used_bytes(), memory.limit_bytes());
    drop(storage); assert_eq!(memory.used_bytes(), blocker.bytes()); assert_eq!(grammar_memory.used_bytes(), 0);
    drop(blocker); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn retained_dfa_owner_moves_and_outlives_constructor_budget_handles() {
    let memory = MemoryBudget::new(16 << 20); let weak = Arc::downgrade(&memory);
    let (mut original, grammar_memory) = region(&memory); let grammar_weak = Arc::downgrade(&grammar_memory);
    open(&mut original, |access, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty(); let (set, _) = one(configs, contexts, 65, 352, empty)?;
        let draft = dfas.new_draft(65, set, configs)?; dfas.add_state(65, Candidate::Draft(draft), configs, contexts)?; Ok(())
    }).unwrap();
    let mut moved = original; drop(memory); drop(grammar_memory);
    open(&mut moved, |access, _| {
        let (dfas, _, _) = access.dfa_parts(); assert_eq!(dfas.state_count(65)?, 1);
        let key = dfas.state_at(65, 0)?.unwrap(); assert_eq!(dfas.view(65, key)?.number(), 0); Ok(())
    }).unwrap();
    assert!(weak.upgrade().is_some()); assert!(grammar_weak.upgrade().is_some());
    drop(moved); assert!(weak.upgrade().is_none()); assert!(grammar_weak.upgrade().is_none());
    assert!(std::mem::needs_drop::<PredictionDfas<'_, '_, '_>>());
}

#[test]
fn map_replacement_admits_the_full_new_block_before_changing_old_records() {
    fixture(32 << 20, |access, memory, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty();
        for number in 0..16 {
            let (set, _) = one(configs, contexts, 65, 352 + number, empty)?;
            let draft = dfas.new_draft(65, set, configs)?; dfas.add_state(65, Candidate::Draft(draft), configs, contexts)?;
        }
        let records: Vec<_> = dfas.decision(65)?.states.slots.iter().flatten().map(|r| (r.hash,r.key,r.value)).collect();
        assert_eq!(records.len(), 16); assert_eq!(dfas.decision(65)?.states.slots.len(), 32);
        let (set, _) = one(configs, contexts, 65, 400, empty)?; configs.mark_readonly(set)?;
        let draft = dfas.new_draft(65, set, configs)?;
        let replacement = 256 + 64 * size_of::<Option<Record>>();
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes() - replacement + 1).unwrap();
        let before = memory.used_bytes(); admission(dfas.add_state(65, Candidate::Draft(draft), configs, contexts));
        assert_eq!(memory.used_bytes(), before);
        assert_eq!(dfas.view_draft(65, draft)?.phase(), Phase::NumberedUnpublished(16));
        assert_eq!(dfas.decision(65)?.states.slots.len(), 32);
        let after: Vec<_> = dfas.decision(65)?.states.slots.iter().flatten().map(|r| (r.hash,r.key,r.value)).collect();
        assert_eq!(after, records); drop(blocker); Ok(())
    });
}

#[test]
fn successful_precedence_replacement_covers_both_blocks_and_preserves_entries() {
    fixture(16 << 20, |access, memory, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let empty = contexts.empty(); let (set, _) = one(configs, contexts, 83, 352, empty)?;
        let draft = dfas.new_draft(83, set, configs)?; let key = interned(dfas.add_state(83, Candidate::Draft(draft), configs, contexts)?);
        let before = memory.used_bytes();
        dfas.set_start(83, 2, key)?;
        let old = 256 + 3 * size_of::<Option<InternedId>>();
        let new = 256 + 70 * size_of::<Option<InternedId>>();
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes() - new).unwrap();
        dfas.set_start(83, 69, key)?;
        assert_eq!(memory.used_bytes(), before + new + blocker.bytes());
        assert!(memory.peak_bytes() >= before + old + new + blocker.bytes());
        assert_eq!(dfas.start(83, 2)?, Some(key)); assert_eq!(dfas.start(83, 69)?, Some(key));
        assert_eq!(dfas.start(83, 68)?, None); drop(blocker); Ok(())
    });
}

#[test]
fn twenty_thousand_context_parents_survive_real_cache_collection_and_flat_drop() {
    let memory = MemoryBudget::new(128 << 20); let (mut storage, grammar_memory) = region(&memory);
    open(&mut storage, |access, _| {
        let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
        let mut parent = contexts.empty();
        for _ in 0..20_000 { parent = contexts.singleton(parent, contexts.grammar().state(1)?)?; }
        let (set, _) = one(configs, contexts, 65, 352, parent)?;
        let draft = dfas.new_draft(65, set, configs)?; let key = interned(dfas.add_state(65, Candidate::Draft(draft), configs, contexts)?);
        dfas.set_start(65, 0, key)?; Ok(())
    }).unwrap();
    open(&mut storage, |access, _| {
        let (dfas, configs, contexts) = access.dfa_parts();
        let key = dfas.start(65, 0)?.unwrap(); let set = dfas.configs(65, key, configs)?;
        let object = configs.view_set(set).entry(0).unwrap(); let mut context = configs.view(object).context();
        for _ in 0..20_000 { context = contexts.view(context).parent(0).unwrap().unwrap(); }
        assert_eq!(context, contexts.empty()); Ok(())
    }).unwrap();
    drop(storage); assert_eq!(memory.used_bytes(), 0); assert_eq!(grammar_memory.used_bytes(), 0);
}

#[test]
fn cross_decision_edge_guards_preserve_the_reached_target_publication_prefix() {
    for foreign_target in [false, true] {
        fixture(16 << 20, |access, _, _| {
            let (dfas, configs, contexts) = access.dfa_parts(); dfas.initialize(configs)?;
            let empty = contexts.empty();
            let (set, _) = one(configs, contexts, 0, 352, empty)?;
            let foreign = dfas.new_draft(0, set, configs)?; let foreign = interned(dfas.add_state(0, Candidate::Draft(foreign), configs, contexts)?);
            let (set, _) = one(configs, contexts, 65, 352, empty)?;
            let local = dfas.new_draft(65, set, configs)?; let local = interned(dfas.add_state(65, Candidate::Draft(local), configs, contexts)?);
            if foreign_target {
                decision_error(dfas.add_edge(65, Some(local), 1, Some(Candidate::Existing(foreign)), configs, contexts));
                assert_eq!(dfas.decision(65)?.states.len, 1);
            } else {
                let (set, _) = one(configs, contexts, 65, 353, empty)?;
                let target = dfas.new_draft(65, set, configs)?;
                decision_error(dfas.add_edge(65, Some(foreign), 1, Some(Candidate::Draft(target)), configs, contexts));
                assert_eq!(dfas.view_draft(65, target)?.phase(), Phase::Interned(1));
                assert_eq!(dfas.decision(65)?.states.len, 2);
            }
            assert!(matches!(&dfas.view(65, local)?.state.body, Body::Ordinary { phase: OrdinaryPhase::Interned { edges, .. }, .. } if edges.is_empty()));
            Ok(())
        });
    }
}
