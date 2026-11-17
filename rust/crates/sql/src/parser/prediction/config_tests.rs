use std::{mem::size_of, sync::Arc};
use rtdb_types::{MemoryBudget, murmur3_32};
use super::*;
use super::super::{with_configurations, with_prediction_contexts};

fn fixture<R>(limit: usize, run: impl for<'cfg, 'id, 'g, 'scope> FnOnce(&mut Configurations<'cfg, 'id, 'g, 'scope>, &mut PredictionContexts<'id, 'g, 'scope>, &Arc<MemoryBudget>) -> Result<R>) -> R {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar_allocation = AllocationScope::new(&grammar_memory).unwrap();
    let grammar = Grammar::load(&grammar_allocation).unwrap();
    let memory = MemoryBudget::new(limit);
    let allocation = AllocationScope::new(&memory).unwrap();
    let result = with_prediction_contexts(&grammar, &allocation, |contexts| {
        with_configurations(contexts, |configs, contexts| run(configs, contexts, &memory))
    }).unwrap();
    drop(allocation);
    assert_eq!(memory.used_bytes(), 0);
    result
}
fn alt(configs: &mut Configurations<'_, '_, '_, '_>, number: usize) -> Result<Alternative> { configs.alternative(configs.grammar.decision(65)?, number) }
fn node<'id>(contexts: &mut PredictionContexts<'id, '_, '_>, parent: ContextKey<'id>, state: u32) -> Result<ContextKey<'id>> { contexts.singleton(parent, contexts.grammar().state(state)?) }
fn config<'cfg, 'id>(configs: &mut Configurations<'cfg, 'id, '_, '_>, context: ContextKey<'id>, state: u32, number: usize) -> Result<ConfigKey<'cfg>> {
    let alt = alt(configs, number)?;
    configs.create(configs.grammar.state(state)?, alt, context, Semantic::Unpredicated)
}
fn admission<T>(result: Result<T>) {
    match result { Err(PredictionError::Admission(error)) => assert_eq!(error.code, "rtdb/memory-budget-exceeded"), _ => panic!("admission failure") }
}

#[test]
fn config_copy_has_fresh_identity_and_captures_current_packed_and_context_values() {
    fixture(1 << 20, |configs, contexts, _| {
        let empty = contexts.empty();
        let one = node(contexts, empty, 1)?;
        let original = config(configs, one, 352, 1)?;
        configs.increment_outer(original)?;
        configs.set_suppressed(original, true)?;
        let copy = configs.copy(original, ConfigChange::Same)?;
        assert_ne!(copy, original);
        assert!(configs.equivalent_config(copy, original, contexts)?);
        configs.increment_outer(original)?;
        configs.set_suppressed(original, false)?;
        assert_eq!(configs.view(copy).context(), one);
        assert_eq!(configs.view(copy).packed_outer(), SUPPRESSED | 1);
        assert_eq!(configs.view(original).packed_outer(), 2);
        assert!(!configs.equivalent_config(copy, original, contexts)?);
        Ok(())
    });
}

#[test]
fn sets_share_live_config_objects_but_copy_constructors_keep_their_own_value() {
    fixture(1 << 20, |configs, contexts, _| {
        let empty = contexts.empty();
        let one = node(contexts, empty, 1)?;
        let two = node(contexts, empty, 2)?;
        let first = config(configs, one, 352, 1)?;
        let detached = configs.copy(first, ConfigChange::Same)?;
        let incoming = config(configs, two, 352, 1)?;
        configs.increment_outer(incoming)?;
        let a = configs.new_set(SetMode::Full)?;
        let b = configs.new_set(SetMode::Full)?;
        configs.add_uncached(a, first, contexts)?;
        configs.add_uncached(b, first, contexts)?;
        configs.add_uncached(b, incoming, contexts)?;
        assert_eq!(configs.view_set(a).entry(0), Some(first));
        assert_eq!(configs.view_set(b).entry(0), Some(first));
        assert_eq!(configs.view_set(b).len(), 1);
        assert_ne!(configs.view(first).context(), one);
        assert_eq!(contexts.view(configs.view(first).context()).len(), 2);
        assert_eq!(configs.view(first).outer_depth(), 1);
        assert_eq!(configs.view(detached).context(), one);
        assert_eq!(configs.view(detached).outer_depth(), 0);
        assert!(!configs.view_set(a).dips_outer());
        assert!(configs.view_set(b).dips_outer());
        Ok(())
    });
}

#[test]
fn readding_the_same_identity_appends_while_distinct_equal_membership_merges() {
    fixture(1 << 20, |configs, contexts, _| {
        let first = config(configs, contexts.empty(), 352, 1)?;
        let distinct = configs.copy(first, ConfigChange::Same)?;
        let set = configs.new_set(SetMode::Local)?;
        configs.add_uncached(set, first, contexts)?;
        configs.add_uncached(set, first, contexts)?;
        configs.add_uncached(set, distinct, contexts)?;
        assert_eq!(configs.view_set(set).len(), 2);
        assert_eq!(configs.view_set(set).entry(0), Some(first));
        assert_eq!(configs.view_set(set).entry(1), Some(first));
        let MembershipState::Building(index) = &configs.sets[set.index].membership else { panic!() };
        assert_eq!(index.count, 1);
        Ok(())
    });
}

#[test]
fn shallow_set_copy_preserves_aliases_conflict_identity_and_metadata_after_clear() {
    fixture(1 << 20, |configs, contexts, _| {
        let first = config(configs, contexts.empty(), 352, 1)?;
        let set = configs.new_set(SetMode::Full)?;
        configs.add_uncached(set, first, contexts)?;
        configs.add_uncached(set, first, contexts)?;
        let one = alt(configs, 1)?;
        let mut bits = AlternativeSet::default(); bits.insert(one);
        let conflict = configs.publish_conflict(bits)?;
        configs.set_conflicting(set, Some(conflict))?;
        configs.set_unique(set, Some(one))?;
        configs.mark_outer_context(set)?;
        configs.mark_readonly(set)?;
        let copy = configs.copy_set(set, contexts)?;
        assert!(!configs.view_set(copy).readonly());
        assert!(configs.equivalent_set(set, copy, contexts)?);
        assert_eq!(configs.view_set(copy).entry(1), Some(first));
        configs.increment_outer(first)?;
        assert_eq!(configs.view(configs.view_set(copy).entry(0).unwrap()).outer_depth(), 1);
        let distinct = configs.publish_conflict(bits)?;
        assert_ne!(conflict, distinct);
        assert_eq!(configs.conflict(conflict), configs.conflict(distinct));
        configs.set_conflicting(copy, Some(distinct))?;
        assert!(!configs.equivalent_set(set, copy, contexts)?);
        configs.clear(copy)?;
        assert_eq!(configs.view_set(copy).len(), 0);
        assert_eq!(configs.view_set(copy).unique(), Some(one));
        assert_eq!(configs.view_set(copy).conflicting(), Some(distinct));
        assert!(configs.view_set(copy).dips_outer());
        Ok(())
    });
}

#[test]
fn readonly_hash_retains_the_cached_value_across_shared_context_changes() {
    fixture(1 << 20, |configs, contexts, _| {
        let empty = contexts.empty();
        let one = node(contexts, empty, 1)?;
        let two = node(contexts, empty, 2)?;
        let first = config(configs, one, 352, 1)?;
        let readonly = configs.new_set(SetMode::Full)?;
        configs.add_uncached(readonly, first, contexts)?;
        configs.mark_readonly(readonly)?;
        let before = configs.set_hash(readonly, contexts)?;
        assert_ne!(before, -1);
        let writable = configs.copy_set(readonly, contexts)?;
        let next = config(configs, two, 352, 1)?;
        configs.add_uncached(writable, next, contexts)?;
        assert!(configs.equivalent_set(readonly, writable, contexts)?);
        let current = configs.set_hash(writable, contexts)?;
        assert_ne!(current, before);
        assert_eq!(configs.set_hash(readonly, contexts)?, before);
        configs.sets[readonly.index].membership = MembershipState::Readonly { cached_hash: -1 };
        assert_eq!(configs.set_hash(readonly, contexts)?, current);
        assert!(matches!(configs.add_uncached(readonly, next, contexts), Err(PredictionError::Artifact(Problem::Readonly))));
        assert!(matches!(configs.increment_outer(first), Err(PredictionError::Aborted)));
        Ok(())
    });
}

#[test]
fn full_config_equality_ignores_depth_but_observes_suppression() {
    fixture(1 << 20, |configs, contexts, _| {
        let a = config(configs, contexts.empty(), 352, 1)?;
        let b = configs.copy(a, ConfigChange::Same)?;
        configs.increment_outer(b)?;
        assert!(configs.equivalent_config(a, b, contexts)?);
        assert_eq!(configs.config_hash(a, contexts)?, configs.config_hash(b, contexts)?);
        configs.set_suppressed(b, true)?;
        assert!(!configs.equivalent_config(a, b, contexts)?);
        assert_eq!(configs.config_hash(a, contexts)?, configs.config_hash(b, contexts)?);
        let set = configs.new_set(SetMode::Full)?;
        configs.add_uncached(set, a, contexts)?;
        configs.add_uncached(set, b, contexts)?;
        assert_eq!(configs.view_set(set).len(), 1);
        assert!(configs.view(a).suppressed());
        Ok(())
    });
}

#[test]
fn ordered_list_hash_uses_full_config_murmur_words_and_set_order() {
    fixture(1 << 20, |configs, contexts, _| {
        let a = config(configs, contexts.empty(), 352, 1)?;
        let b = config(configs, contexts.empty(), 352, 2)?;
        let mut bytes = [0; 16];
        for (index, word) in [352i32, 1, contexts.view(contexts.empty()).hash(), 0].into_iter().enumerate() {
            bytes[index * 4..index * 4 + 4].copy_from_slice(&word.to_le_bytes());
        }
        assert_eq!(configs.config_hash(a, contexts)? as u32, murmur3_32(&bytes, 7));
        let ab = configs.new_set(SetMode::Local)?;
        let ba = configs.new_set(SetMode::Local)?;
        for key in [a, b] { configs.add_uncached(ab, key, contexts)?; }
        for key in [b, a] { configs.add_uncached(ba, key, contexts)?; }
        assert!(!configs.equivalent_set(ab, ba, contexts)?);
        let expected = 31i32.wrapping_add(configs.config_hash(a, contexts)?).wrapping_mul(31).wrapping_add(configs.config_hash(b, contexts)?);
        assert_eq!(configs.set_hash(ab, contexts)?, expected);
        assert_ne!(configs.set_hash(ab, contexts)?, configs.set_hash(ba, contexts)?);
        Ok(())
    });
}

#[test]
fn packed_outer_merge_uses_signed_max_then_only_incoming_suppression() {
    fixture(1 << 20, |configs, contexts, _| {
        for (left, right, expected) in [(SUPPRESSED | 1, 7, SUPPRESSED | 1), (i32::MIN | SUPPRESSED | 1, 7, 7), (0, i32::MIN | SUPPRESSED, SUPPRESSED)] {
            let a = config(configs, contexts.empty(), 352, 1)?;
            let b = configs.copy(a, ConfigChange::Same)?;
            configs.objects[a.index].outer = left;
            configs.objects[b.index].outer = right;
            let set = configs.new_set(SetMode::Local)?;
            configs.add_uncached(set, a, contexts)?;
            configs.add_uncached(set, b, contexts)?;
            assert_eq!(configs.view(a).packed_outer(), expected);
        }
        let key = config(configs, contexts.empty(), 352, 1)?;
        configs.objects[key.index].outer = i32::MAX;
        configs.increment_outer(key)?;
        assert_eq!(configs.view(key).packed_outer(), i32::MIN);
        let set = configs.new_set(SetMode::Local)?;
        configs.add_uncached(set, key, contexts)?;
        assert!(!configs.view_set(set).dips_outer());
        Ok(())
    });
}

#[test]
fn uncached_merges_ignore_and_preserve_round_memo_including_array_parents() {
    fixture(2 << 20, |_, contexts, _| {
        let empty = contexts.empty();
        let one = node(contexts, empty, 1)?;
        let two = node(contexts, empty, 2)?;
        let left = node(contexts, one, 5)?;
        let right = node(contexts, two, 5)?;
        contexts.with_merge_round(|round| {
            let cached = round.merge(left, right, MergeMode::FullExact)?;
            let uncached = round.contexts().merge_uncached(left, right, MergeMode::FullExact)?;
            let again = round.contexts().merge_uncached(left, right, MergeMode::FullExact)?;
            assert_ne!(cached, uncached);
            assert_ne!(uncached, again);
            assert!(round.contexts().equivalent(cached, uncached)?);
            assert_eq!(round.merge(left, right, MergeMode::FullExact)?, cached);
            let a = round.contexts().merge_uncached(empty, left, MergeMode::FullExact)?;
            let b = round.contexts().merge_uncached(empty, right, MergeMode::FullExact)?;
            let x = round.contexts().merge_uncached(a, b, MergeMode::FullExact)?;
            let y = round.contexts().merge_uncached(a, b, MergeMode::FullExact)?;
            assert_ne!(x, y);
            assert!(round.contexts().equivalent(x, y)?);
            Ok(())
        })
    });
}

#[test]
fn config_additions_share_the_existing_round_memo_across_set_modes() {
    fixture(2 << 20, |configs, contexts, _| {
        let empty = contexts.empty();
        let one = node(contexts, empty, 1)?;
        let local = configs.new_set(SetMode::Local)?;
        let full = configs.new_set(SetMode::Full)?;
        let a = config(configs, empty, 352, 1)?;
        let b = config(configs, one, 352, 1)?;
        let c = configs.copy(a, ConfigChange::Same)?;
        let d = configs.copy(b, ConfigChange::Same)?;
        contexts.with_merge_round(|round| {
            configs.add(local, a, round)?;
            configs.add(local, b, round)?;
            configs.add(full, c, round)?;
            configs.add(full, d, round)?;
            assert_eq!(configs.view(c).context(), empty);
            Ok(())
        })?;
        let uncached = configs.new_set(SetMode::Full)?;
        let e = config(configs, empty, 352, 1)?;
        configs.add_uncached(uncached, e, contexts)?;
        configs.add_uncached(uncached, d, contexts)?;
        assert_ne!(configs.view(e).context(), empty);
        assert_eq!(contexts.view(configs.view(e).context()).len(), 2);
        Ok(())
    });
}

#[test]
fn optimize_mutates_shared_objects_in_list_order_without_freezing_aliases() {
    fixture(1 << 20, |configs, contexts, _| {
        let empty = contexts.empty();
        let canonical = node(contexts, empty, 1)?;
        let equal = node(contexts, empty, 1)?;
        contexts.canonicalize(canonical)?;
        let key = config(configs, equal, 352, 1)?;
        let a = configs.new_set(SetMode::Local)?;
        configs.add_uncached(a, key, contexts)?;
        let b = configs.copy_set(a, contexts)?;
        configs.mark_readonly(a)?;
        configs.optimize(b, contexts)?;
        assert_eq!(configs.view(configs.view_set(a).entry(0).unwrap()).context(), canonical);
        assert_eq!(configs.view_set(b).entry(0), Some(key));
        Ok(())
    });
}

#[test]
fn later_optimization_failure_retains_completed_alias_updates_and_seals_further_work() {
    fixture(2 << 20, |configs, contexts, memory| {
        let empty = contexts.empty();
        let canonical = node(contexts, empty, 1)?;
        let equal = node(contexts, empty, 1)?;
        contexts.canonicalize(canonical)?;
        let mut deep = empty;
        for _ in 0..64 { deep = node(contexts, deep, 2)?; }
        let first = config(configs, equal, 352, 1)?;
        let second = config(configs, deep, 352, 2)?;
        let set = configs.new_set(SetMode::Local)?;
        configs.add_uncached(set, first, contexts)?;
        configs.add_uncached(set, second, contexts)?;
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        admission(configs.optimize(set, contexts));
        assert_eq!(configs.view(first).context(), canonical);
        assert_eq!(configs.view(second).context(), deep);
        drop(blocker);
        assert!(matches!(configs.optimize(set, contexts), Err(PredictionError::Aborted)));
        Ok(())
    });
}

#[test]
fn configuration_region_drop_keeps_its_scope_charge_until_the_scope_drops() {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar_allocation = AllocationScope::new(&grammar_memory).unwrap();
    let grammar = Grammar::load(&grammar_allocation).unwrap();
    let memory = MemoryBudget::new(1 << 20);
    let allocation = AllocationScope::new(&memory).unwrap();
    with_prediction_contexts(&grammar, &allocation, |contexts| {
        with_configurations(contexts, |configs, contexts| {
            let key = config(configs, contexts.empty(), 352, 1)?;
            let set = configs.new_set(SetMode::Local)?;
            configs.add_uncached(set, key, contexts)?;
            Ok(())
        })?;
        assert!(memory.used_bytes() > 4096);
        Ok(())
    }).unwrap();
    assert!(memory.used_bytes() > 4096);
    drop(allocation);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn failed_merge_never_half_updates_the_shared_config_value() {
    fixture(1 << 20, |configs, contexts, memory| {
        let empty = contexts.empty();
        let one = node(contexts, empty, 1)?;
        let two = node(contexts, empty, 2)?;
        let a = config(configs, one, 352, 1)?;
        let b = config(configs, two, 352, 1)?;
        configs.increment_outer(b)?;
        configs.set_suppressed(b, true)?;
        let set = configs.new_set(SetMode::Full)?;
        configs.add_uncached(set, a, contexts)?;
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        admission(configs.add_uncached(set, b, contexts));
        assert_eq!(configs.view(a).context(), one);
        assert_eq!(configs.view(a).packed_outer(), 0);
        assert!(configs.view_set(set).dips_outer());
        drop(blocker);
        let before = memory.used_bytes();
        assert!(matches!(configs.add_uncached(set, b, contexts), Err(PredictionError::Aborted)));
        assert_eq!(memory.used_bytes(), before);
        Ok(())
    });
}

#[test]
fn aborted_context_rejects_distinct_membership_identity_empty_and_cached_shortcuts() {
    for operation in 0..7 {
        fixture(1 << 20, |configs, contexts, memory| {
            let empty = contexts.empty();
            let context = node(contexts, empty, 1)?;
            let key = config(configs, empty, 352, 1)?;
            configs.increment_outer(key)?;
            let set = configs.new_set(SetMode::Full)?;
            let cached = configs.new_set(SetMode::Full)?;
            configs.mark_readonly(cached)?;
            assert_eq!(configs.set_hash(cached, contexts)?, 1);
            let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
            admission(contexts.canonicalize(context));
            drop(blocker);
            let before = memory.used_bytes();
            let result = match operation {
                0 => configs.add_uncached(set, key, contexts),
                1 => configs.copy_set(set, contexts).map(|_| ()),
                2 => configs.equivalent_config(key, key, contexts).map(|_| ()),
                3 => configs.equivalent_set(set, set, contexts).map(|_| ()),
                4 => configs.config_hash(key, contexts).map(|_| ()),
                5 => configs.set_hash(cached, contexts).map(|_| ()),
                _ => configs.optimize(set, contexts),
            };
            assert!(matches!(result, Err(PredictionError::Aborted)));
            assert_eq!(configs.view_set(set).len(), 0);
            assert!(!configs.view_set(set).dips_outer());
            assert_eq!(configs.view(key).context(), empty);
            assert!(matches!(configs.increment_outer(key), Err(PredictionError::Aborted)));
            assert_eq!(memory.used_bytes(), before);
            Ok(())
        });
    }
}

#[test]
fn aborted_round_rejects_distinct_membership_before_flags_or_index_changes() {
    fixture(1 << 20, |configs, contexts, memory| {
        let empty = contexts.empty();
        let one = node(contexts, empty, 1)?;
        let two = node(contexts, empty, 2)?;
        let key = config(configs, empty, 352, 1)?;
        configs.increment_outer(key)?;
        let set = configs.new_set(SetMode::Local)?;
        contexts.with_merge_round(|round| {
            let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
            admission(round.merge(one, two, MergeMode::FullExact));
            drop(blocker);
            let before = memory.used_bytes();
            assert!(matches!(configs.add(set, key, round), Err(PredictionError::Aborted)));
            assert_eq!(configs.view_set(set).len(), 0);
            assert!(!configs.view_set(set).dips_outer());
            assert!(matches!(configs.mark_outer_context(set), Err(PredictionError::Aborted)));
            assert_eq!(memory.used_bytes(), before);
            Ok(())
        })
    });
}

#[test]
fn first_object_admission_and_replacement_storage_are_reserved_before_publication() {
    fixture(1 << 20, |configs, contexts, memory| {
        let first = config(configs, contexts.empty(), 352, 1)?;
        for _ in 1..16 { configs.copy(first, ConfigChange::Same)?; }
        let replacement = 256 + 32 * size_of::<ConfigData<'_>>();
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes() - replacement + 1).unwrap();
        let before = memory.used_bytes();
        admission(configs.copy(first, ConfigChange::Same));
        assert_eq!(configs.objects.len(), 16);
        assert_eq!(memory.used_bytes(), before);
        drop(blocker);
        assert!(matches!(configs.copy(first, ConfigChange::Same), Err(PredictionError::Aborted)));
        Ok(())
    });
    fixture(4096, |configs, contexts, _| {
        admission(config(configs, contexts.empty(), 352, 1));
        assert!(configs.objects.is_empty());
        Ok(())
    });
}

#[test]
fn new_set_and_duplicate_append_failures_do_not_publish_a_successful_result() {
    fixture(4096, |configs, _, _| {
        admission(configs.new_set(SetMode::Full));
        assert!(configs.sets.is_empty());
        Ok(())
    });
    fixture(1 << 20, |configs, contexts, memory| {
        let key = config(configs, contexts.empty(), 352, 1)?;
        let set = configs.new_set(SetMode::Local)?;
        for _ in 0..7 { configs.add_uncached(set, key, contexts)?; }
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        admission(configs.add_uncached(set, key, contexts));
        assert_eq!(configs.view_set(set).len(), 7);
        drop(blocker);
        assert!(matches!(configs.clear(set), Err(PredictionError::Aborted)));
        Ok(())
    });
}

#[test]
fn membership_replacement_failure_preserves_the_admitted_ordered_prefix() {
    fixture(2 << 20, |configs, contexts, memory| {
        let set = configs.new_set(SetMode::Local)?;
        for number in 1..=16 {
            let key = config(configs, contexts.empty(), 352, number)?;
            configs.add_uncached(set, key, contexts)?;
        }
        let next = config(configs, contexts.empty(), 352, 17)?;
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        admission(configs.add_uncached(set, next, contexts));
        assert_eq!(configs.view_set(set).len(), 16);
        let MembershipState::Building(index) = &configs.sets[set.index].membership else { panic!() };
        assert_eq!(index.count, 16);
        assert_eq!(index.slots.len(), 32);
        drop(blocker);
        assert!(matches!(configs.add_uncached(set, next, contexts), Err(PredictionError::Aborted)));
        Ok(())
    });
}

#[test]
fn sll_strip_admits_each_new_object_in_order_after_its_new_set() {
    fixture(2 << 20, |configs, contexts, memory| {
        let mut predicate = None;
        for number in 0..3017 {
            let state = contexts.grammar().state(number)?;
            if let Some(index) = state.transitions().position(|transition| matches!(transition, crate::parser::grammar::TransitionView::Precedence { .. })) {
                predicate = Some(configs.precedence(state, index)?); break;
            }
        }
        let semantic = predicate.unwrap();
        let one = alt(configs, 1)?;
        let first = configs.create(contexts.grammar().state(352)?, one, contexts.empty(), semantic)?;
        let second = config(configs, contexts.empty(), 352, 2)?;
        for _ in 2..15 { configs.copy(first, ConfigChange::Same)?; }
        let original = configs.new_set(SetMode::Local)?;
        configs.add_uncached(original, first, contexts)?;
        configs.add_uncached(original, second, contexts)?;
        let next_set = 256 + 7 * size_of::<ConfigKey<'_>>() + 256 + 32 * size_of::<Option<ConfigKey<'_>>>();
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes() - next_set).unwrap();
        let mut mode = configs.mode_workspace();
        admission(mode.has_sll_conflict(super::super::PredictionMode::Sll, configs, original, contexts));
        assert_eq!(configs.objects.len(), 16);
        assert_eq!(configs.sets.len(), 2);
        assert_eq!(configs.sets[1].ordered.len(), 1);
        assert_eq!(configs.view(first).semantic(), semantic);
        assert_eq!(configs.view_set(original).len(), 2);
        drop(blocker);
        assert!(matches!(configs.copy(first, ConfigChange::Same), Err(PredictionError::Aborted)));
        Ok(())
    });
}

#[test]
fn membership_growth_retains_first_representatives_and_order() {
    fixture(2 << 20, |configs, contexts, _| {
        let set = configs.new_set(SetMode::Local)?;
        for number in 1..=87 {
            let first = config(configs, contexts.empty(), 352, number)?;
            configs.add_uncached(set, first, contexts)?;
            let duplicate = configs.copy(first, ConfigChange::Same)?;
            configs.add_uncached(set, duplicate, contexts)?;
            assert_eq!(configs.view_set(set).entry(number - 1), Some(first));
        }
        assert_eq!(configs.view_set(set).len(), 87);
        assert_eq!(configs.view_set(set).alternatives().count(), 87);
        let MembershipState::Building(index) = &configs.sets[set.index].membership else { panic!() };
        assert_eq!(index.slots.len(), 256);
        Ok(())
    });
}
