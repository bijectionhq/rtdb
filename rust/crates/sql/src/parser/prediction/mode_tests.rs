use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};
use rtdb_types::MemoryBudget;
use crate::parser::grammar::{Grammar, TransitionView};
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
fn alternative<'id, 'g, 'scope>(configs: &mut Configurations<'_, 'id, 'g, 'scope>, contexts: &PredictionContexts<'id, 'g, 'scope>, number: usize) -> Result<Alternative> {
    configs.alternative(contexts.grammar().decision(65)?, number)
}
fn add<'cfg, 'id, 'g, 'scope>(configs: &mut Configurations<'cfg, 'id, 'g, 'scope>, contexts: &mut PredictionContexts<'id, 'g, 'scope>, set: ConfigSetKey<'cfg>, state: u32, number: usize, context: ContextKey<'id>, semantic: Semantic) -> Result<ConfigKey<'cfg>> {
    let alt = alternative(configs, contexts, number)?;
    let key = configs.create(contexts.grammar().state(state)?, alt, context, semantic)?;
    configs.add_uncached(set, key, contexts)?;
    Ok(key)
}
fn predicate<'id, 'g, 'scope>(configs: &mut Configurations<'_, 'id, 'g, 'scope>, contexts: &PredictionContexts<'id, 'g, 'scope>, expected: i32) -> Result<Semantic> {
    for number in 0..3017 {
        let state = contexts.grammar().state(number)?;
        for (index, transition) in state.transitions().enumerate() {
            if matches!(transition, TransitionView::Precedence { precedence, .. } if precedence == expected) { return configs.precedence(state, index); }
        }
    }
    Err(PredictionError::Artifact(Problem::Precedence))
}
fn single<'id>(contexts: &mut PredictionContexts<'id, '_, '_>, parent: ContextKey<'id>, number: u32) -> Result<ContextKey<'id>> {
    contexts.singleton(parent, contexts.grammar().state(number)?)
}
fn admission<T>(result: Result<T>) {
    match result { Err(PredictionError::Admission(error)) => assert_eq!(error.code, "rtdb/memory-budget-exceeded"), _ => panic!("admission failure") }
}

#[test]
fn pinned_graph_closes_alternative_and_semantic_domains() {
    fixture(1 << 20, |configs, contexts, _| {
        let mut histogram = BTreeMap::new();
        for number in 0..372 {
            let decision = contexts.grammar().decision(number)?;
            let degree = decision.state().transitions().len();
            *histogram.entry(degree).or_insert(0) += 1;
            let mut bits = AlternativeSet::default();
            for ordinal in 1..=degree { bits.insert(configs.alternative(decision, ordinal)?); }
            assert_eq!(bits.count() as usize, degree);
            assert_eq!(bits.first().unwrap().number(), 1);
        }
        assert_eq!(histogram, BTreeMap::from([(2,337),(3,15),(4,6),(5,2),(6,1),(7,3),(9,1),(13,1),(15,1),(22,1),(28,1),(30,1),(63,1),(87,1)]));
        let max = contexts.grammar().decision(65)?;
        assert_eq!(max.state().number(), 848);
        assert_eq!(max.state().rule().number(), 23);
        assert_eq!(configs.alternative(max, 87)?.number(), 87);
        let mut values = BTreeSet::new();
        let mut count = 0;
        for number in 0..3017 {
            let state = contexts.grammar().state(number)?;
            for (index, transition) in state.transitions().enumerate() {
                if let TransitionView::Precedence { precedence, .. } = transition {
                    count += 1; values.insert(precedence);
                    let semantic = configs.precedence(state, index)?;
                    assert!(semantic.evaluate(precedence));
                    assert!(!semantic.evaluate(precedence + 1));
                }
            }
        }
        assert_eq!(count, 42);
        assert_eq!(values, BTreeSet::from([1,2,3,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,58,60,61,64,65,66,67]));
        Ok(())
    });
}

#[test]
fn alternatives_reject_zero_and_the_first_unrepresentable_bit() {
    for ordinal in [0, 88] {
        fixture(4096, |configs, contexts, memory| {
            assert!(matches!(configs.alternative(contexts.grammar().decision(65)?, ordinal), Err(PredictionError::Artifact(Problem::Alternative))));
            assert_eq!(memory.used_bytes(), 4096);
            assert!(matches!(configs.new_set(SetMode::Local), Err(PredictionError::Aborted)));
            Ok(())
        });
    }
}

#[test]
fn semantic_precedence_comes_from_a_real_precedence_edge() {
    fixture(4096, |configs, contexts, memory| {
        assert!(matches!(configs.precedence(contexts.grammar().state(352)?, 0), Err(PredictionError::Artifact(Problem::Precedence))));
        assert_eq!(memory.used_bytes(), 4096);
        Ok(())
    });
}

#[test]
fn foreign_grammar_views_fail_before_transition_lookup_or_storage_admission() {
    let grammar_memory = MemoryBudget::new(16 << 20);
    let grammar_allocation = AllocationScope::new(&grammar_memory).unwrap();
    let grammar = Grammar::load(&grammar_allocation).unwrap();
    let foreign = Grammar::load(&grammar_allocation).unwrap();
    for operation in 0..3 {
        let memory = MemoryBudget::new(4096);
        let allocation = AllocationScope::new(&memory).unwrap();
        with_prediction_contexts(&grammar, &allocation, |contexts| with_configurations(contexts, |configs, contexts| {
            let result = match operation {
                0 => configs.alternative(foreign.decision(65)?, 1).map(|_| ()),
                1 => configs.precedence(foreign.state(352)?, usize::MAX).map(|_| ()),
                _ => {
                    let alt = configs.alternative(grammar.decision(65)?, 1)?;
                    configs.create(foreign.state(352)?, alt, contexts.empty(), Semantic::Unpredicated).map(|_| ())
                },
            };
            assert!(matches!(result, Err(PredictionError::ForeignOwner)));
            assert_eq!(memory.used_bytes(), 4096);
            Ok(())
        })).unwrap();
        drop(allocation);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn precedence_algebra_preserves_null_empty_and_bounded_minimum_maximum() {
    fixture(4096, |configs, contexts, memory| {
        let low = predicate(configs, contexts, 1)?;
        let high = predicate(configs, contexts, 67)?;
        let empty = Semantic::Unpredicated;
        assert_eq!(Semantic::and(None, None), None);
        assert_eq!(Semantic::and(Some(empty), None), None);
        assert_eq!(Semantic::and(None, Some(empty)), Some(empty));
        assert_eq!(Semantic::and(Some(empty), Some(low)), Some(low));
        assert_eq!(Semantic::and(Some(high), Some(empty)), Some(high));
        assert_eq!(Semantic::and(Some(high), Some(low)), Some(low));
        assert_eq!(Semantic::or(None, Some(high)), Some(high));
        assert_eq!(Semantic::or(Some(low), None), Some(low));
        assert_eq!(Semantic::or(Some(low), Some(high)), Some(high));
        assert_eq!(Semantic::or(Some(empty), Some(high)), Some(empty));
        assert!(!empty.evaluate(i32::MIN));
        assert_eq!(empty.evaluate_precedence(i32::MAX), Some(empty));
        assert_eq!(low.evaluate_precedence(1), Some(empty));
        assert_eq!(low.evaluate_precedence(2), None);
        assert_eq!(high.hash(), 98);
        assert_eq!(memory.used_bytes(), 4096);
        Ok(())
    });
}

#[test]
fn empty_and_all_stop_sets_terminate_before_semantic_copy_or_group_allocation() {
    fixture(1 << 20, |configs, contexts, memory| {
        let empty_set = configs.new_set(SetMode::Local)?;
        let stopped = configs.new_set(SetMode::Local)?;
        let semantic = predicate(configs, contexts, 1)?;
        let stop = contexts.grammar().rule(0)?.stop().number();
        let empty = contexts.empty();
        add(configs, contexts, stopped, stop, 1, empty, semantic)?;
        let mut mode = configs.mode_workspace();
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        assert!(mode.has_sll_conflict(PredictionMode::Sll, configs, empty_set, contexts)?);
        assert!(mode.has_sll_conflict(PredictionMode::Sll, configs, stopped, contexts)?);
        assert!(mode.groups.is_empty());
        assert!(mode.states.is_empty());
        drop(blocker);
        Ok(())
    });
}

#[test]
fn sll_strips_into_fresh_full_context_configs_while_ll_preserves_the_originals() {
    fixture(2 << 20, |configs, contexts, _| {
        let empty = contexts.empty();
        let one = single(contexts, empty, 1)?;
        let full = contexts.merge_uncached(empty, one, super::super::MergeMode::FullExact)?;
        let semantic = predicate(configs, contexts, 1)?;
        let set = configs.new_set(SetMode::Local)?;
        let a = add(configs, contexts, set, 352, 1, empty, semantic)?;
        let b = add(configs, contexts, set, 352, 1, one, Semantic::Unpredicated)?;
        let c = add(configs, contexts, set, 352, 2, full, Semantic::Unpredicated)?;
        let mut mode = configs.mode_workspace();
        assert!(!mode.has_sll_conflict(PredictionMode::Ll, configs, set, contexts)?);
        assert!(mode.has_sll_conflict(PredictionMode::Sll, configs, set, contexts)?);
        assert_eq!(mode.groups.len(), 1);
        assert!(![a, b, c].contains(&mode.groups[0].representative));
        assert!(contexts.equivalent(mode.groups[0].context, full)?);
        assert_eq!(configs.view(a).context(), empty);
        assert_eq!(configs.view(a).semantic(), semantic);
        assert_eq!(configs.view(b).context(), one);
        assert_eq!(configs.view(c).context(), full);
        assert_eq!(configs.view_set(set).len(), 3);
        Ok(())
    });
}

#[test]
fn grouping_uses_state_and_structural_context_without_alt_semantic_or_suppression() {
    fixture(2 << 20, |configs, contexts, _| {
        let empty = contexts.empty();
        let a = single(contexts, empty, 1)?;
        let b = single(contexts, empty, 1)?;
        assert_ne!(a, b);
        let semantic = predicate(configs, contexts, 1)?;
        let set = configs.new_set(SetMode::Full)?;
        let first = add(configs, contexts, set, 352, 1, a, semantic)?;
        let second = add(configs, contexts, set, 352, 2, b, Semantic::Unpredicated)?;
        configs.set_suppressed(second, true)?;
        let mut mode = configs.mode_workspace();
        let subsets = mode.conflicting_subsets(configs, set, contexts)?;
        assert_eq!(subsets.len(), 1);
        assert_eq!(subsets.get(0).unwrap().count(), 2);
        assert!(subsets.all_conflict());
        assert!(subsets.all_equal()?);
        assert_eq!(subsets.single_viable(), AlternativeChoice::One(configs.view(first).alternative()));
        Ok(())
    });
}

#[test]
fn source_viable_minima_and_state_singleton_veto_remain_distinct() {
    fixture(2 << 20, |configs, contexts, _| {
        let empty = contexts.empty();
        let one = alternative(configs, contexts, 1)?;
        let set = configs.new_set(SetMode::Local)?;
        for (state, alt) in [(352,1),(352,2),(353,1),(353,2),(354,1)] {
            add(configs, contexts, set, state, alt, empty, Semantic::Unpredicated)?;
        }
        let mut mode = configs.mode_workspace();
        let subsets = mode.conflicting_subsets(configs, set, contexts)?;
        assert_eq!(subsets.single_viable(), AlternativeChoice::One(one));
        assert!(subsets.has_conflict());
        assert!(subsets.has_singleton());
        assert!(!subsets.all_conflict());
        assert_eq!(subsets.unique(), None);
        assert!(!mode.has_sll_conflict(PredictionMode::Ll, configs, set, contexts)?);
        let competing = configs.new_set(SetMode::Full)?;
        for (state, alt) in [(352,1),(352,2),(353,2),(353,3)] {
            add(configs, contexts, competing, state, alt, empty, Semantic::Unpredicated)?;
        }
        let subsets = mode.conflicting_subsets(configs, competing, contexts)?;
        assert_eq!(subsets.single_viable(), AlternativeChoice::Conflict);
        assert!(!subsets.all_equal()?);
        assert_eq!(subsets.union().count(), 3);
        assert!(mode.has_sll_conflict(PredictionMode::Ll, configs, competing, contexts)?);
        Ok(())
    });
}

#[test]
fn empty_subsets_distinguish_negative_one_viable_from_zero_conflict_and_no_first_equal_set() {
    fixture(1 << 20, |configs, contexts, _| {
        let set = configs.new_set(SetMode::Local)?;
        let mut mode = configs.mode_workspace();
        let subsets = mode.conflicting_subsets(configs, set, contexts)?;
        assert_eq!(subsets.single_viable(), AlternativeChoice::Empty);
        assert_eq!(subsets.unique(), None);
        assert_eq!(subsets.union().count(), 0);
        assert!(subsets.all_conflict());
        assert!(!subsets.has_conflict());
        assert!(matches!(subsets.all_equal(), Err(PredictionError::Artifact(Problem::EmptySubsets))));
        Ok(())
    });
}

#[test]
fn deep_equal_hash_groups_retain_identity_order_and_reuse_equality_capacity() {
    fixture(16 << 20, |configs, contexts, memory| {
        let state = contexts.grammar().state(1)?;
        let mut left = contexts.null_parent_fixture(state)?;
        let mut right = contexts.null_parent_fixture(state)?;
        let empty = contexts.empty();
        for _ in 0..64 {
            let other = single(contexts, empty, 2)?;
            let a = single(contexts, left, 1)?;
            let b = single(contexts, right, 1)?;
            left = contexts.merge_uncached(a, other, super::super::MergeMode::FullExact)?;
            right = contexts.merge_uncached(b, other, super::super::MergeMode::FullExact)?;
        }
        assert_eq!(contexts.view(left).hash(), contexts.view(right).hash());
        let set = configs.new_set(SetMode::Full)?;
        let first = add(configs, contexts, set, 352, 1, left, Semantic::Unpredicated)?;
        let second = add(configs, contexts, set, 352, 2, right, Semantic::Unpredicated)?;
        let mut mode = configs.mode_workspace();
        assert_eq!(mode.conflicting_subsets(configs, set, contexts)?.len(), 2);
        assert_eq!(mode.groups[0].representative, first);
        assert_eq!(mode.groups[0].next, Some(1));
        assert_eq!(mode.groups[1].representative, second);
        for state in 400..470 { add(configs, contexts, set, state, 3, empty, Semantic::Unpredicated)?; }
        assert_eq!(mode.conflicting_subsets(configs, set, contexts)?.len(), 72);
        assert!(mode.groups_by_hash.slots.len() >= 128);
        assert_eq!(mode.groups[0].representative, first);
        assert_eq!(mode.groups[1].representative, second);
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        assert!(!contexts.equivalent(left, right)?);
        assert!(!contexts.equivalent(left, right)?);
        assert_eq!(mode.conflicting_subsets(configs, set, contexts)?.len(), 72);
        drop(blocker);
        Ok(())
    });
}

#[test]
fn final_group_permutation_does_not_change_pure_reductions() {
    fixture(1 << 20, |configs, contexts, _| {
        let set = configs.new_set(SetMode::Full)?;
        let empty = contexts.empty();
        for (state, alt) in [(352,1),(352,3),(353,1),(353,2),(354,1)] { add(configs, contexts, set, state, alt, empty, Semantic::Unpredicated)?; }
        let mut mode = configs.mode_workspace();
        let before = mode.conflicting_subsets(configs, set, contexts)?;
        let facts = (before.has_conflict(), before.has_singleton(), before.all_conflict(), before.all_equal()?, before.union(), before.unique(), before.single_viable());
        mode.groups.reverse();
        let after = SubsetsView { groups: &mode.groups };
        assert_eq!((after.has_conflict(), after.has_singleton(), after.all_conflict(), after.all_equal()?, after.union(), after.unique(), after.single_viable()), facts);
        Ok(())
    });
}

#[test]
fn no_conflict_does_not_admit_state_map_and_repeated_groups_need_no_new_charge() {
    fixture(1 << 20, |configs, contexts, memory| {
        let set = configs.new_set(SetMode::Local)?;
        let empty = contexts.empty();
        add(configs, contexts, set, 352, 1, empty, Semantic::Unpredicated)?;
        let mut mode = configs.mode_workspace();
        mode.conflicting_subsets(configs, set, contexts)?;
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        assert!(!mode.has_sll_conflict(PredictionMode::Sll, configs, set, contexts)?);
        assert!(mode.states.is_empty());
        assert!(mode.states_by_number.slots.is_empty());
        drop(blocker);
        Ok(())
    });
}

#[test]
fn group_and_state_admission_failures_seal_all_reached_work() {
    for fail_state in [false, true] {
        fixture(1 << 20, |configs, contexts, memory| {
            let set = configs.new_set(SetMode::Local)?;
            let empty = contexts.empty();
            add(configs, contexts, set, 352, 1, empty, Semantic::Unpredicated)?;
            add(configs, contexts, set, 352, 2, empty, Semantic::Unpredicated)?;
            let mut mode = configs.mode_workspace();
            if fail_state { mode.conflicting_subsets(configs, set, contexts)?; }
            let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
            admission(mode.has_sll_conflict(PredictionMode::Ll, configs, set, contexts));
            drop(blocker);
            let before = memory.used_bytes();
            assert!(matches!(mode.has_sll_conflict(PredictionMode::Ll, configs, set, contexts), Err(PredictionError::Aborted)));
            assert!(matches!(configs.new_set(SetMode::Full), Err(PredictionError::Aborted)));
            assert!(matches!(contexts.equivalent(contexts.empty(), contexts.empty()), Err(PredictionError::Aborted)));
            assert_eq!(memory.used_bytes(), before);
            Ok(())
        });
    }
}

#[test]
fn failed_sll_strip_preserves_original_configs_and_seals_resume() {
    fixture(1 << 20, |configs, contexts, memory| {
        let set = configs.new_set(SetMode::Local)?;
        let semantic = predicate(configs, contexts, 1)?;
        let empty = contexts.empty();
        let key = add(configs, contexts, set, 352, 1, empty, semantic)?;
        let mut mode = configs.mode_workspace();
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        admission(mode.has_sll_conflict(PredictionMode::Sll, configs, set, contexts));
        assert_eq!(configs.view_set(set).entry(0), Some(key));
        assert_eq!(configs.view(key).semantic(), semantic);
        assert_eq!(configs.view(key).context(), contexts.empty());
        drop(blocker);
        assert!(matches!(mode.has_sll_conflict(PredictionMode::Sll, configs, set, contexts), Err(PredictionError::Aborted)));
        Ok(())
    });
}

#[test]
fn context_abort_at_mode_entry_seals_workspace_and_configs_before_empty_shortcuts() {
    for grouped in [false, true] {
        fixture(1 << 20, |configs, contexts, memory| {
            let empty = contexts.empty();
            let context = single(contexts, empty, 1)?;
            let set = configs.new_set(SetMode::Full)?;
            let mut mode = configs.mode_workspace();
            let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
            admission(contexts.canonicalize(context));
            drop(blocker);
            let before = memory.used_bytes();
            let result = if grouped { mode.conflicting_subsets(configs, set, contexts).map(|_| ()) }
            else { mode.has_sll_conflict(PredictionMode::Sll, configs, set, contexts).map(|_| ()) };
            assert!(matches!(result, Err(PredictionError::Aborted)));
            assert!(mode.aborted);
            assert!(mode.groups.is_empty());
            assert!(matches!(configs.clear(set), Err(PredictionError::Aborted)));
            assert!(matches!(mode.has_state_with_one_alt(configs, set), Err(PredictionError::Aborted)));
            assert_eq!(memory.used_bytes(), before);
            Ok(())
        });
    }
}

#[test]
fn config_abort_at_state_map_entry_seals_workspace_without_allocating() {
    fixture(1 << 20, |configs, contexts, memory| {
        let set = configs.new_set(SetMode::Full)?;
        let mut mode = configs.mode_workspace();
        assert!(matches!(configs.alternative(contexts.grammar().decision(65)?, 0), Err(PredictionError::Artifact(Problem::Alternative))));
        let before = memory.used_bytes();
        assert!(matches!(mode.has_state_with_one_alt(configs, set), Err(PredictionError::Aborted)));
        assert!(mode.aborted);
        assert!(mode.states.is_empty());
        assert_eq!(memory.used_bytes(), before);
        Ok(())
    });
}
