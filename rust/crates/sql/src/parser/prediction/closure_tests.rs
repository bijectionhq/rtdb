use std::{mem::size_of, panic::{AssertUnwindSafe, catch_unwind}, sync::Arc};
use rtdb_types::MemoryBudget;
use crate::parser::grammar::{Grammar, OwnedGrammar};
use super::*;
use super::super::{PredictionAccess, PredictionStorage, ContextShape, MergeMode, with_configurations, with_prediction_contexts, with_prediction_storage};
use super::super::dfa::{Candidate, DfaTarget};

fn options() -> ClosureOptions { ClosureOptions { pruning: LoopEntryOption::Enabled, assertions: SourceAssertions::Enabled } }
fn retained_fixture<R>(retained_limit: usize, work_limit: usize, run: impl for<'cfg, 'id, 'access> FnOnce(&mut PredictionAccess<'cfg, 'id, 'access, 'access>, &Arc<MemoryBudget>, &Arc<MemoryBudget>) -> Result<R>) -> R {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
    let retained = MemoryBudget::new(retained_limit);
    let mut storage = PredictionStorage::new(grammar, &retained);
    let memory = MemoryBudget::new(work_limit);
    let work = AllocationScope::new(&memory).unwrap();
    let result = with_prediction_storage(&mut storage, &work, |access| run(access, &retained, &memory)).unwrap();
    drop(work);
    assert_eq!(memory.used_bytes(), 0);
    drop(storage);
    assert_eq!(retained.used_bytes(), 0);
    assert_eq!(grammar_memory.used_bytes(), 0);
    result
}
fn fixture<R>(decision: u16, options: ClosureOptions, run: impl for<'call, 'round, 'cfg, 'id, 'access> FnOnce(&mut ClosureRound<'call, 'round, 'cfg, 'id, 'access, 'access, Retained>, &Arc<MemoryBudget>, &Arc<MemoryBudget>) -> Result<R>) -> R {
    retained_fixture(64 << 20, 64 << 20, |access, retained, work| {
        let (_, configs, contexts) = access.dfa_parts();
        let decision = contexts.grammar().decision(decision)?;
        with_closure_round(configs, contexts, decision, options, |round| run(round, retained, work))
    })
}
fn config<'cfg, 'id, 'g, 'scope, M: StoreMemory>(configs: &mut Configurations<'cfg, 'id, 'g, 'scope, M>, contexts: &PredictionContexts<'id, 'g, 'scope, M>, state: u32, alt: usize, context: ContextKey<'id>, semantic: Semantic) -> Result<ConfigKey<'cfg>> {
    let alternative = configs.alternative(contexts.grammar().decision(65)?, alt)?;
    configs.create(contexts.grammar().state(state)?, alternative, context, semantic)
}
fn set<'cfg, 'id, 'g, 'scope, M: StoreMemory>(configs: &mut Configurations<'cfg, 'id, 'g, 'scope, M>, contexts: &mut PredictionContexts<'id, 'g, 'scope, M>, keys: &[ConfigKey<'cfg>]) -> Result<ConfigSetKey<'cfg>> {
    let set = configs.new_set(SetMode::Local)?;
    for key in keys { configs.add_uncached(set, *key, contexts)?; }
    Ok(set)
}
fn find<'g, 'scope>(grammar: &'g Grammar<'scope>, predicate: impl Fn(TransitionView<'g, 'scope>) -> bool) -> (State<'g, 'scope>, usize, TransitionView<'g, 'scope>) {
    for number in 0..3017 {
        let state = grammar.state(number).unwrap();
        for (index, transition) in state.transitions().enumerate() { if predicate(transition) { return (state, index, transition); } }
    }
    panic!("pinned transition")
}
fn nested<'id, 'g, 'scope, M: StoreMemory>(contexts: &mut PredictionContexts<'id, 'g, 'scope, M>, depth: usize) -> Result<ContextKey<'id>> {
    let mut parent = contexts.empty();
    for _ in 0..depth {
        let a = contexts.singleton(parent, contexts.grammar().state(1)?)?;
        let b = contexts.singleton(contexts.empty(), contexts.grammar().state(2)?)?;
        parent = contexts.merge_uncached(a, b, MergeMode::LocalWildcard)?;
    }
    Ok(parent)
}
fn merge_pair<'round, 'cfg, 'id, 'g, 'scope>(round: &mut ClosureRound<'_, 'round, 'cfg, 'id, 'g, 'scope, Retained>, a: ContextKey<'id>, b: ContextKey<'id>) -> Result<ContextKey<'id>> {
    let x=config(round.configs,round.contexts.contexts(),352,1,a,Semantic::Unpredicated)?;
    let y=config(round.configs,round.contexts.contexts(),352,1,b,Semantic::Unpredicated)?;
    let output=round.configs.new_set(SetMode::Local)?;
    round.add(output,x)?;
    round.add(output,y)?;
    Ok(round.configs.view(x).context())
}
fn admission<T>(result: Result<T>) { assert!(matches!(result, Err(PredictionError::Admission(error)) if error.code == "rtdb/memory-budget-exceeded")); }
fn interned<'cfg>(target: DfaTarget<'cfg>) -> InternedStateKey<'cfg> { match target { DfaTarget::Interned(key) => key, DfaTarget::Error => panic!("ordinary state") } }

#[test]
fn all_372_local_starts_preserve_local_contexts_stops_and_unprepared_metadata_with_both_pruning_options() {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar_scope = AllocationScope::new(&grammar_memory).unwrap();
    let grammar = Grammar::load(&grammar_scope).unwrap();
    for pruning in [LoopEntryOption::Enabled, LoopEntryOption::Disabled] {
        for number in 0..372 {
            let memory = MemoryBudget::new(128 << 20);
            let work = AllocationScope::new(&memory).unwrap();
            with_prediction_contexts(&grammar, &work, |contexts| with_configurations(contexts, |configs, contexts| {
                let decision = contexts.grammar().decision(number)?;
                with_closure_round(configs, contexts, decision, ClosureOptions { pruning, ..options() }, |round| {
                    let start = round.local_start()?;
                    let (key, precedence) = match start { StartSet::Ordinary(set) => (set.key(), false), StartSet::Precedence(set) => (set.prefilter(), true) };
                    assert_eq!(precedence, [83,99,153,220,286].contains(&number));
                    let view = round.configs.view_set(key);
                    assert_eq!(view.mode(), SetMode::Local);
                    assert!(!view.readonly());
                    assert!(view.unique().is_none());
                    assert!(view.conflicting().is_none());
                    assert!(view.len() > 0);
                    for index in 0..view.len() {
                        let config = round.configs.view(view.entry(index).unwrap());
                        let context = round.contexts.contexts().view(config.context());
                        if matches!(config.state()?.kind(), StateKindView::RuleStop) { assert!(context.is_empty()); }
                        if context.shape() == ContextShape::Array { assert!(context.len() >= 2); }
                        for edge in 0..context.len() {
                            if !context.is_empty() { assert!(context.follow(edge)?.is_some()); assert!(context.parent(edge).flatten().is_some()); }
                        }
                    }
                    assert_eq!(round.memo, MergePresence::Absent);
                    assert!(round.work.frames.is_empty());
                    Ok(())
                })
            })).unwrap();
            drop(work);
            assert_eq!(memory.used_bytes(), 0);
        }
    }
}

#[test]
fn all_six_transition_kinds_copy_fresh_objects_and_rule_pushes_the_actual_follow() {
    fixture(65, options(), |round, _, _| {
        let grammar = round.contexts.contexts().grammar();
        let mut kinds = [0; 6];
        for number in 0..3017 {
            let state = grammar.state(number)?;
            for (index, transition) in state.transitions().enumerate() {
                let kind = match transition { TransitionView::Epsilon { .. } => 0, TransitionView::Rule { .. } => 1, TransitionView::Atom { .. } => 2, TransitionView::Action { .. } => 3, TransitionView::Set { .. } => 4, TransitionView::Precedence { .. } => 5 };
                if kinds[kind] == 0 {
                    let empty = round.contexts.contexts().empty();
                    let original = config(round.configs, round.contexts.contexts(), number, 1, empty, Semantic::Unpredicated)?;
                    round.configs.increment_outer(original)?;
                    round.configs.set_suppressed(original, true)?;
                    let copy = round.epsilon_target(original, state, index, transition, true, true, false)?;
                    if matches!(transition, TransitionView::Atom { .. } | TransitionView::Set { .. }) { assert!(copy.is_none()); }
                    else {
                        let copy = copy.unwrap();
                        assert_ne!(copy, original);
                        assert_eq!(round.configs.view(copy).state()?.number(), target(transition).number());
                        assert_eq!(round.configs.view(copy).packed_outer(), round.configs.view(original).packed_outer());
                        if let TransitionView::Rule { follow, .. } = transition {
                            let context = round.contexts.contexts().view(round.configs.view(copy).context());
                            assert_eq!(context.follow(0)?.unwrap().number(), follow.number());
                            assert_eq!(context.parent(0).flatten(), Some(empty));
                        }
                    }
                }
                kinds[kind] += 1;
            }
        }
        assert_eq!(kinds, [2475,602,891,5,56,42]);
        Ok(())
    });
}

#[test]
fn precedence_epsilon_accumulates_only_when_collecting_at_depth_zero() {
    fixture(83, options(), |round, _, _| {
        let grammar = round.contexts.contexts().grammar();
        let (state, index, transition) = find(grammar, |t| matches!(t, TransitionView::Precedence { .. }));
        let empty = round.contexts.contexts().empty();
        let original = config(round.configs, round.contexts.contexts(), state.number(), 1, empty, Semantic::Unpredicated)?;
        let predicate = round.configs.precedence(state, index)?;
        for (collect, in_context) in [(true,true),(true,false),(false,true),(false,false)] {
            let copy = round.epsilon_target(original, state, index, transition, collect, in_context, false)?.unwrap();
            assert_eq!(round.configs.view(copy).semantic(), if collect && in_context { predicate } else { Semantic::Unpredicated });
        }
        Ok(())
    });
}

#[test]
fn action_latches_collection_off_for_the_whole_descendant_closure() {
    fixture(83, options(), |round, _, _| {
        let grammar = round.contexts.contexts().grammar();
        let (action, _, _) = find(grammar, |t| matches!(t, TransitionView::Action { .. }));
        let empty = round.contexts.contexts().empty();
        let original = config(round.configs, round.contexts.contexts(), action.number(), 1, empty, Semantic::Unpredicated)?;
        let output = round.configs.new_set(SetMode::Local)?;
        round.closure(original, output, true, 0, false)?;
        for index in 0..round.configs.view_set(output).len() {
            let key = round.configs.view_set(output).entry(index).unwrap();
            assert_eq!(round.configs.view(key).semantic(), Semantic::Unpredicated);
        }
        assert!(round.configs.view_set(output).len() > 0);
        Ok(())
    });
}

#[test]
fn eof_labels_are_the_only_consuming_epsilon_targets() {
    fixture(65, options(), |round, _, _| {
        let grammar = round.contexts.contexts().grammar();
        let mut eof_pairs = Vec::new();
        for number in 0..3017 {
            let state = grammar.state(number)?;
            for (index, transition) in state.transitions().enumerate() {
                if !epsilon(transition) {
                    let empty = round.contexts.contexts().empty();
                    let key = config(round.configs, round.contexts.contexts(), number, 1, empty, Semantic::Unpredicated)?;
                    assert!(round.epsilon_target(key, state, index, transition, false, false, false)?.is_none());
                    let copied = round.epsilon_target(key, state, index, transition, false, false, true)?;
                    assert_eq!(copied.is_some(), reachable(transition, TokenType::EOF).is_some());
                    if let Some(copy) = copied { eof_pairs.push((number, round.configs.view(copy).state()?.number())); }
                }
            }
        }
        assert_eq!(eof_pairs, [(356,357),(369,370)]);
        Ok(())
    });
}

#[test]
fn parent_pop_visits_sorted_return_states_and_propagates_packed_bits() {
    fixture(65, options(), |round, _, _| {
        let grammar = round.contexts.contexts().grammar();
        let empty = round.contexts.contexts().empty();
        let (first, _, _) = find(grammar, |t| matches!(t, TransitionView::Atom { token, .. } if token != TokenType::EOF));
        let second = (first.number()+1..3017).map(|n| grammar.state(n).unwrap()).find(|s| !s.epsilon_only() && s.transitions().len() > 0).unwrap();
        let a = round.contexts.contexts().singleton(empty, second)?;
        let b = round.contexts.contexts().singleton(empty, first)?;
        let context = round.contexts.contexts().merge_uncached(a, b, MergeMode::LocalWildcard)?;
        let original = config(round.configs, round.contexts.contexts(), grammar.rule(0)?.stop().number(), 1, context, Semantic::Unpredicated)?;
        round.configs.increment_outer(original)?;
        round.configs.set_suppressed(original, true)?;
        let output = round.configs.new_set(SetMode::Local)?;
        round.closure(original, output, true, 0, false)?;
        assert_eq!(round.configs.view_set(output).len(), 2);
        for (index, number) in [first.number(),second.number()].into_iter().enumerate() {
            let key = round.configs.view_set(output).entry(index).unwrap();
            assert_ne!(key, original);
            assert_eq!(round.configs.view(key).state()?.number(), number);
            assert_eq!(round.configs.view(key).context(), empty);
            assert_eq!(round.configs.view(key).packed_outer(), round.configs.view(original).packed_outer());
        }
        Ok(())
    });
}

#[test]
fn empty_stop_falloff_uses_busy_and_marks_outer_context_after_increment() {
    fixture(83, options(), |round, _, _| {
        let grammar = round.contexts.contexts().grammar();
        let stop = grammar.rule(round.decision.state().rule().number())?.stop();
        let empty = round.contexts.contexts().empty();
        let original = config(round.configs, round.contexts.contexts(), stop.number(), 1, empty, Semantic::Unpredicated)?;
        let output = round.configs.new_set(SetMode::Local)?;
        round.closure(original, output, false, 0, false)?;
        assert!(round.configs.view_set(output).dips_outer());
        assert!(round.work.busy.len > 0);
        assert!(round.work.busy.slots.iter().flatten().all(|entry| round.configs.view(entry.config).outer_depth() > 0));
        assert!(round.work.busy.slots.iter().flatten().any(|entry| round.configs.view(entry.config).suppressed()));
        assert!(round.work.frames.is_empty());
        Ok(())
    });
}

#[test]
fn busy_full_equality_observes_state_alt_context_semantic_and_suppression_but_not_depth() {
    fixture(65, options(), |round, _, _| {
        let contexts = round.contexts.contexts();
        let empty = contexts.empty();
        let parent = contexts.singleton(empty, contexts.grammar().state(1)?)?;
        let (p, pi, _) = find(contexts.grammar(), |t| matches!(t, TransitionView::Precedence { .. }));
        let predicate = round.configs.precedence(p, pi)?;
        let a = config(round.configs, contexts, 352, 1, empty, Semantic::Unpredicated)?;
        let equal = round.configs.copy(a, ConfigChange::Same)?;
        round.configs.increment_outer(equal)?;
        assert!(round.work.busy.add(a, round.configs, contexts, round.work.allocation)?);
        assert!(!round.work.busy.add(equal, round.configs, contexts, round.work.allocation)?);
        let suppressed = round.configs.copy(a, ConfigChange::Same)?;
        round.configs.set_suppressed(suppressed, true)?;
        for key in [config(round.configs, contexts, 353, 1, empty, Semantic::Unpredicated)?,config(round.configs, contexts, 352, 2, empty, Semantic::Unpredicated)?,config(round.configs, contexts, 352, 1, parent, Semantic::Unpredicated)?,config(round.configs, contexts, 352, 1, empty, predicate)?,suppressed] {
            assert!(round.work.busy.add(key, round.configs, contexts, round.work.allocation)?);
        }
        assert_eq!(round.work.busy.len, 6);
        assert!(!round.work.busy.add(a, round.configs, contexts, round.work.allocation)?);
        Ok(())
    });
}

#[test]
fn busy_suppression_convergence_keeps_stored_hashes_and_boolean_membership() {
    fixture(65, options(), |round, _, _| {
        let contexts = round.contexts.contexts();
        let a = config(round.configs, contexts, 352, 1, contexts.empty(), Semantic::Unpredicated)?;
        let b = round.configs.copy(a, ConfigChange::Same)?;
        round.configs.set_suppressed(b, true)?;
        assert!(round.work.busy.add(a, round.configs, contexts, round.work.allocation)?);
        assert!(round.work.busy.add(b, round.configs, contexts, round.work.allocation)?);
        let hash = spread(round.configs.config_hash(a, contexts)?);
        round.configs.set_suppressed(a, true)?;
        assert!(!round.work.busy.add(a, round.configs, contexts, round.work.allocation)?);
        assert!(!round.work.busy.add(b, round.configs, contexts, round.work.allocation)?);
        for alt in 2..=17 {
            let key = config(round.configs, contexts, 352, alt, contexts.empty(), Semantic::Unpredicated)?;
            assert!(round.work.busy.add(key, round.configs, contexts, round.work.allocation)?);
        }
        assert_eq!(round.work.busy.slots.len(), 64);
        assert!(round.work.busy.slots.iter().flatten().filter(|e| e.config == a || e.config == b).all(|e| e.hash == hash));
        assert!(!round.work.busy.add(b, round.configs, contexts, round.work.allocation)?);
        Ok(())
    });
}

#[test]
fn deep_busy_comparison_is_allocation_free_at_full_work_and_retained_budgets() {
    fixture(65, options(), |round, retained, work| {
        let contexts = round.contexts.contexts();
        let a = nested(contexts, 256)?;
        let b = nested(contexts, 256)?;
        let left = config(round.configs, contexts, 352, 1, a, Semantic::Unpredicated)?;
        let right = config(round.configs, contexts, 352, 1, b, Semantic::Unpredicated)?;
        assert_ne!(a,b);
        assert!(round.work.busy.add(left, round.configs, contexts, round.work.allocation)?);
        let keep_retained = retained.reserve(retained.limit_bytes()-retained.used_bytes()).unwrap();
        let keep_work = work.reserve(work.limit_bytes()-work.used_bytes()).unwrap();
        let before = (retained.used_bytes(),work.used_bytes());
        assert!(!round.work.busy.add(right, round.configs, contexts, round.work.allocation)?);
        assert_eq!((retained.used_bytes(),work.used_bytes()),before);
        drop((keep_retained,keep_work));
        assert!(contexts.equivalent(a,b)?);
        Ok(())
    });
}

#[test]
fn busy_duplicate_precedes_growth_and_failed_overlap_retains_the_old_table() {
    fixture(65, options(), |round, _, work| {
        let contexts = round.contexts.contexts();
        let mut first = None;
        for alt in 1..=16 {
            let key = config(round.configs, contexts, 352, alt, contexts.empty(), Semantic::Unpredicated)?;
            first.get_or_insert(key);
            assert!(round.work.busy.add(key, round.configs, contexts, round.work.allocation)?);
        }
        let next = config(round.configs, contexts, 352, 17, contexts.empty(), Semantic::Unpredicated)?;
        let keep = work.reserve(work.limit_bytes()-work.used_bytes()).unwrap();
        assert!(!round.work.busy.add(first.unwrap(), round.configs, contexts, round.work.allocation)?);
        admission(round.work.busy.add(next, round.configs, contexts, round.work.allocation));
        assert_eq!((round.work.busy.len,round.work.busy.slots.len()),(16,32));
        drop(keep);
        round.work.busy.clear();
        assert_eq!(round.work.busy.len,0);
        assert_eq!(round.work.busy.slots.len(),32);
        assert!(round.work.busy.add(next, round.configs, contexts, round.work.allocation)?);
        Ok(())
    });
}

#[test]
fn consuming_reach_single_and_unique_paths_do_not_admit_unused_closure_work() {
    fixture(65, options(), |round, _, work| {
        let (state, _, transition) = find(round.contexts.contexts().grammar(), |t| matches!(t, TransitionView::Atom { token, .. } if token != TokenType::EOF));
        let token = match transition { TransitionView::Atom { token, .. } => token, _ => unreachable!() };
        let empty = round.contexts.contexts().empty();
        let key = config(round.configs, round.contexts.contexts(), state.number(), 1, empty, Semantic::Unpredicated)?;
        let input = set(round.configs, round.contexts.contexts(), &[key])?;
        let before = work.used_bytes();
        let output = round.local_reach(LocalSet::new(input),token)?.unwrap();
        assert_eq!(work.used_bytes(),before);
        assert_eq!(round.configs.view_set(output.key()).len(),1);
        assert_eq!(round.configs.view(round.configs.view_set(output.key()).entry(0).unwrap()).state()?.number(),target(transition).number());
        assert_eq!(round.work.frames.capacity(),0);
        assert_eq!(round.work.busy.slots.len(),0);
        assert_eq!(round.memo,MergePresence::Present);
        Ok(())
    });
}

#[test]
fn unique_alternative_with_multiple_intermediates_also_bypasses_closure() {
    fixture(65, options(), |round, _, work| {
        let grammar = round.contexts.contexts().grammar();
        let mut pair = None;
        'states: for a in 0..3017 {
            for ta in grammar.state(a)?.transitions() {
                let TransitionView::Atom { token, target: first } = ta else { continue; };
                if token == TokenType::EOF { continue; }
                for b in a+1..3017 {
                    if grammar.state(b)?.transitions().any(|tb| reachable(tb,token).is_some_and(|s| s.number()!=first.number())) { pair=Some((a,b,token)); break 'states; }
                }
            }
        }
        let (a,b,token)=pair.unwrap();
        let empty=round.contexts.contexts().empty();
        let a=config(round.configs,round.contexts.contexts(),a,1,empty,Semantic::Unpredicated)?;
        let b=config(round.configs,round.contexts.contexts(),b,1,empty,Semantic::Unpredicated)?;
        let input=set(round.configs,round.contexts.contexts(),&[a,b])?;
        let before=work.used_bytes();
        let output=round.local_reach(LocalSet::new(input),token)?.unwrap();
        assert!(round.configs.view_set(output.key()).len()>1);
        assert!(round.configs.view_set(output.key()).unique_alternative().is_some());
        assert_eq!(work.used_bytes(),before);
        assert_eq!(round.work.frames.capacity(),0);
        Ok(())
    });
}

#[test]
fn unreachable_symbol_returns_none_only_after_final_empty_reach() {
    fixture(65, options(), |round, _, _| {
        let input=round.configs.new_set(SetMode::Local)?;
        assert!(round.local_reach(LocalSet::new(input),TokenType::EOF)?.is_none());
        assert_eq!(round.memo,MergePresence::Present);
        assert_eq!(round.work.busy.len,0);
        assert!(round.work.frames.is_empty());
        Ok(())
    });
}

#[test]
fn both_real_eof_targets_close_to_their_uncalled_stop_and_filter_nonstops() {
    for (state,stop) in [(356,1),(369,3)] {
        fixture(65,options(),|round,_,_| {
            let empty=round.contexts.contexts().empty();
            let key=config(round.configs,round.contexts.contexts(),state,1,empty,Semantic::Unpredicated)?;
            let input=set(round.configs,round.contexts.contexts(),&[key])?;
            let output=round.local_reach(LocalSet::new(input),TokenType::EOF)?.unwrap();
            assert_eq!(round.configs.view_set(output.key()).len(),1);
            let result=round.configs.view_set(output.key()).entry(0).unwrap();
            assert_eq!(round.configs.view(result).state()?.number(),stop);
            assert_ne!(key,result);
            assert_eq!(round.configs.view(result).context(),empty);
            assert!(round.configs.view_set(output.key()).all_rule_stop()?);
            Ok(())
        });
    }
}

#[test]
fn eof_skipped_stops_reuse_identity_order_and_same_identity_duplicates() {
    fixture(65,options(),|round,_,_| {
        let empty=round.contexts.contexts().empty();
        let a=config(round.configs,round.contexts.contexts(),1,1,empty,Semantic::Unpredicated)?;
        let b=config(round.configs,round.contexts.contexts(),3,2,empty,Semantic::Unpredicated)?;
        let input=set(round.configs,round.contexts.contexts(),&[b,a,b])?;
        let output=round.local_reach(LocalSet::new(input),TokenType::EOF)?.unwrap();
        assert_eq!(round.configs.view_set(output.key()).len(),3);
        for (i,key) in [b,a,b].into_iter().enumerate() { assert_eq!(round.configs.view_set(output.key()).entry(i),Some(key)); }
        assert_eq!(round.work.skipped,[b,a,b]);
        assert!(!round.configs.view_set(output.key()).readonly());
        Ok(())
    });
}

#[test]
fn non_eof_stops_are_skipped_without_being_returned() {
    fixture(65,options(),|round,_,_| {
        let (_,_,transition)=find(round.contexts.contexts().grammar(),|t|matches!(t,TransitionView::Atom{token,..} if token!=TokenType::EOF));
        let token=match transition {TransitionView::Atom{token,..}=>token,_=>unreachable!()};
        let empty = round.contexts.contexts().empty();
        let key=config(round.configs,round.contexts.contexts(),1,1,empty,Semantic::Unpredicated)?;
        let input=set(round.configs,round.contexts.contexts(),&[key])?;
        assert!(round.local_reach(LocalSet::new(input),token)?.is_none());
        assert!(round.work.skipped.is_empty());
        Ok(())
    });
}

#[test]
fn cold_precedence_changes_copy_but_unpredicated_configurations_keep_identity() {
    fixture(83,options(),|round,_,_| {
        let (p,index,_)=find(round.contexts.contexts().grammar(),|t|matches!(t,TransitionView::Precedence{..}));
        let predicate=round.configs.precedence(p,index)?;
        let empty=round.contexts.contexts().empty();
        let changed=config(round.configs,round.contexts.contexts(),352,1,empty,predicate)?;
        let unchanged=config(round.configs,round.contexts.contexts(),353,1,empty,Semantic::Unpredicated)?;
        let input=set(round.configs,round.contexts.contexts(),&[changed,unchanged])?;
        let output=round.precedence_filter(PrecedenceStart{set:LocalSet::new(input)},0)?;
        let first=round.configs.view_set(output.key()).entry(0).unwrap();
        assert_ne!(first,changed);
        assert_eq!(round.configs.view(first).semantic(),Semantic::Unpredicated);
        assert_eq!(round.configs.view(changed).semantic(),predicate);
        assert_eq!(round.configs.view_set(output.key()).entry(1),Some(unchanged));
        assert_eq!(round.memo,MergePresence::Absent);
        Ok(())
    });
}

#[test]
fn cold_precedence_captures_last_context_before_alias_merge_and_processes_alt1_first() {
    fixture(83,options(),|round,_,_| {
        let contexts=round.contexts.contexts();
        let a=contexts.singleton(contexts.empty(),contexts.grammar().state(1)?)?;
        let b=contexts.singleton(contexts.empty(),contexts.grammar().state(2)?)?;
        let (p,index,_)=find(contexts.grammar(),|t|matches!(t,TransitionView::Precedence{..}));
        let predicate=round.configs.precedence(p,index)?;
        let first=config(round.configs,contexts,352,1,a,Semantic::Unpredicated)?;
        let last=config(round.configs,contexts,352,1,b,predicate)?;
        let eliminated=config(round.configs,contexts,352,2,b,Semantic::Unpredicated)?;
        let surviving=config(round.configs,contexts,352,3,a,Semantic::Unpredicated)?;
        let input=set(round.configs,contexts,&[eliminated,first,surviving,last])?;
        let output=round.precedence_filter(PrecedenceStart{set:LocalSet::new(input)},0)?;
        assert_eq!(round.work.alt1.get(352),Some(b));
        assert_ne!(round.configs.view(first).context(),a);
        assert_eq!(round.configs.view_set(output.key()).len(),2);
        assert_eq!(round.configs.view_set(output.key()).entry(0),Some(first));
        assert_eq!(round.configs.view_set(output.key()).entry(1),Some(surviving));
        assert_eq!(round.configs.view(last).context(),b);
        Ok(())
    });
}

#[test]
fn suppressed_alternatives_bypass_context_equality_and_alt1_none_precedes_map_demand() {
    retained_fixture(16<<20,4096,|access,_,work| {
        let (_,configs,contexts)=access.dfa_parts();
        let decision=contexts.grammar().decision(83)?;
        let (p,index,_)=find(contexts.grammar(),|t|matches!(t,TransitionView::Precedence{..}));
        let predicate=configs.precedence(p,index)?;
        let eliminated=config(configs,contexts,352,1,contexts.empty(),predicate)?;
        let suppressed=config(configs,contexts,352,2,contexts.empty(),Semantic::Unpredicated)?;
        configs.set_suppressed(suppressed,true)?;
        let input=set(configs,contexts,&[eliminated,suppressed])?;
        with_closure_round(configs,contexts,decision,options(),|round| {
            let before=work.used_bytes();
            let output=round.precedence_filter(PrecedenceStart{set:LocalSet::new(input)},i32::MAX)?;
            assert_eq!(round.configs.view_set(output.key()).len(),1);
            assert_eq!(round.configs.view_set(output.key()).entry(0),Some(suppressed));
            assert_eq!(round.work.alt1.slots.len(),0);
            assert_eq!(work.used_bytes(),before);
            Ok(())
        })
    });
    fixture(83,options(),|round,_,work| {
        let a=nested(round.contexts.contexts(),80)?;
        let b=nested(round.contexts.contexts(),80)?;
        let first=config(round.configs,round.contexts.contexts(),352,1,a,Semantic::Unpredicated)?;
        let suppressed=config(round.configs,round.contexts.contexts(),352,2,b,Semantic::Unpredicated)?;
        round.configs.set_suppressed(suppressed,true)?;
        let input=set(round.configs,round.contexts.contexts(),&[first,suppressed])?;
        round.work.alt1.put(352,a,round.work.allocation)?;
        let keep=work.reserve(work.limit_bytes()-work.used_bytes()).unwrap();
        let before=work.used_bytes();
        let output=round.precedence_filter(PrecedenceStart{set:LocalSet::new(input)},0)?;
        assert_eq!(round.configs.view_set(output.key()).len(),2);
        assert_eq!(round.configs.view_set(output.key()).entry(1),Some(suppressed));
        assert_eq!(work.used_bytes(),before);
        drop(keep);
        Ok(())
    });
}

#[test]
fn precedence_context_demand_keeps_the_fallible_scoped_equality_policy() {
    fixture(83,options(),|round,_,work| {
        let a=nested(round.contexts.contexts(),80)?;
        let b=nested(round.contexts.contexts(),80)?;
        let first=config(round.configs,round.contexts.contexts(),352,1,a,Semantic::Unpredicated)?;
        let other=config(round.configs,round.contexts.contexts(),352,2,b,Semantic::Unpredicated)?;
        let input=set(round.configs,round.contexts.contexts(),&[first,other])?;
        round.work.alt1.put(352,a,round.work.allocation)?;
        let keep=work.reserve(work.limit_bytes()-work.used_bytes()).unwrap();
        admission(round.precedence_filter(PrecedenceStart{set:LocalSet::new(input)},0));
        assert!(round.aborted);
        assert!(matches!(round.local_start(),Err(PredictionError::Aborted)));
        drop(keep);
        Ok(())
    });
}

#[test]
fn alt1_map_overwrites_without_admission_and_rehash_preserves_captured_values() {
    fixture(83,options(),|round,_,work| {
        let empty=round.contexts.contexts().empty();
        let follow=round.contexts.contexts().grammar().state(1)?;
        let other=round.contexts.contexts().singleton(empty,follow)?;
        for state in 0..16 { round.work.alt1.put(state,empty,round.work.allocation)?; }
        let keep=work.reserve(work.limit_bytes()-work.used_bytes()).unwrap();
        round.work.alt1.put(5,other,round.work.allocation)?;
        admission(round.work.alt1.put(16,empty,round.work.allocation));
        assert_eq!((round.work.alt1.len,round.work.alt1.slots.len()),(16,32));
        assert_eq!(round.work.alt1.get(5),Some(other));
        drop(keep);
        round.work.alt1.put(16,empty,round.work.allocation)?;
        assert_eq!(round.work.alt1.slots.len(),64);
        assert_eq!(round.work.alt1.get(5),Some(other));
        assert_eq!(round.work.alt1.get(16),Some(empty));
        Ok(())
    });
}

#[test]
fn loop_entry_pruning_accepts_each_of_the_four_real_follow_shapes() {
    fixture(83,options(),|round,_,_| {
        let grammar=round.contexts.contexts().grammar();
        for follow in [940,956,948,867] {
            let empty=round.contexts.contexts().empty();
            let context=round.contexts.contexts().singleton(empty,grammar.state(follow)?)?;
            let key=config(round.configs,round.contexts.contexts(),957,1,context,Semantic::Unpredicated)?;
            assert!(round.can_drop_loop_entry(key)?);
        }
        Ok(())
    });
}

#[test]
fn loop_entry_pruning_rejects_disabled_empty_wrong_shape_foreign_rule_and_nearby_return() {
    fixture(83,options(),|round,_,_| {
        let grammar=round.contexts.contexts().grammar();
        let empty=round.contexts.contexts().empty();
        let accepted=round.contexts.contexts().singleton(empty,grammar.state(940)?)?;
        let rejected=round.contexts.contexts().singleton(empty,grammar.state(856)?)?;
        let foreign=round.contexts.contexts().singleton(empty,grammar.state(1)?)?;
        let mixed=round.contexts.contexts().merge_uncached(accepted,foreign,MergeMode::LocalWildcard)?;
        let empty_path=round.contexts.contexts().merge_uncached(accepted,empty,MergeMode::FullExact)?;
        for (state,context) in [(957,empty),(940,accepted),(957,rejected),(957,mixed),(957,empty_path)] {
            let key=config(round.configs,round.contexts.contexts(),state,1,context,Semantic::Unpredicated)?;
            assert!(!round.can_drop_loop_entry(key)?);
        }
        let key=config(round.configs,round.contexts.contexts(),957,1,accepted,Semantic::Unpredicated)?;
        round.options.pruning=LoopEntryOption::Disabled;
        assert!(!round.can_drop_loop_entry(key)?);
        Ok(())
    });
}

#[test]
fn loop_entry_pruning_requires_every_return_to_pass_before_dropping_the_first_edge() {
    fixture(83,options(),|round,_,_| {
        let grammar=round.contexts.contexts().grammar();
        let empty=round.contexts.contexts().empty();
        let a=round.contexts.contexts().singleton(empty,grammar.state(940)?)?;
        let b=round.contexts.contexts().singleton(empty,grammar.state(948)?)?;
        let c=round.contexts.contexts().singleton(empty,grammar.state(856)?)?;
        let good=round.contexts.contexts().merge_uncached(a,b,MergeMode::LocalWildcard)?;
        let bad=round.contexts.contexts().merge_uncached(good,c,MergeMode::LocalWildcard)?;
        let good=config(round.configs,round.contexts.contexts(),957,1,good,Semantic::Unpredicated)?;
        let bad=config(round.configs,round.contexts.contexts(),957,1,bad,Semantic::Unpredicated)?;
        assert!(round.can_drop_loop_entry(good)?);
        assert!(!round.can_drop_loop_entry(bad)?);
        Ok(())
    });
}

#[test]
fn absent_merges_are_fresh_then_first_reach_preserves_one_memo_across_later_operations() {
    fixture(65,options(),|round,_,_| {
        let grammar=round.contexts.contexts().grammar();
        let empty=round.contexts.contexts().empty();
        let a=round.contexts.contexts().singleton(empty,grammar.state(1)?)?;
        let b=round.contexts.contexts().singleton(empty,grammar.state(2)?)?;
        let uncached_a=merge_pair(round,a,b)?;
        let uncached_b=merge_pair(round,a,b)?;
        assert_ne!(uncached_a,uncached_b);
        assert!(round.contexts.contexts().equivalent(uncached_a,uncached_b)?);
        let empty_set=round.configs.new_set(SetMode::Local)?;
        assert!(round.local_reach(LocalSet::new(empty_set),TokenType::EOF)?.is_none());
        let cached_a=merge_pair(round,a,b)?;
        {
            let (configs,_)=round.parts();
            let mut workspace=configs.mode_workspace();
            assert!(!workspace.has_state_with_one_alt(configs,empty_set)?);
        }
        let cached_b=merge_pair(round,a,b)?;
        assert_eq!(cached_a,cached_b);
        assert_eq!(round.memo,MergePresence::Present);
        Ok(())
    });
}

#[test]
fn first_reach_activates_memo_before_intermediate_admission_failure_and_seals_every_owner() {
    fixture(65,options(),|round,retained,_| {
        let input=round.configs.new_set(SetMode::Local)?;
        let keep=retained.reserve(retained.limit_bytes()-retained.used_bytes()).unwrap();
        admission(round.local_reach(LocalSet::new(input),TokenType::EOF));
        assert_eq!(round.memo,MergePresence::Present);
        assert!(round.aborted);
        assert!(matches!(round.local_start(),Err(PredictionError::Aborted)));
        assert!(matches!(round.configs.clear(input),Err(PredictionError::Aborted)));
        let contexts=round.contexts.contexts();
        assert!(matches!(contexts.equivalent(contexts.empty(),contexts.empty()),Err(PredictionError::Aborted)));
        drop(keep);
        Ok(())
    });
}

#[test]
fn parent_frame_admission_follows_completed_rule_context_and_copy_prefix() {
    fixture(65,options(),|round,retained,work| {
        let grammar=round.contexts.contexts().grammar();
        let (state,_,_)=find(grammar,|t|matches!(t,TransitionView::Rule{..}));
        let empty = round.contexts.contexts().empty();
        let key=config(round.configs,round.contexts.contexts(),state.number(),1,empty,Semantic::Unpredicated)?;
        let output=round.configs.new_set(SetMode::Local)?;
        let before=retained.used_bytes();
        let keep=work.reserve(work.limit_bytes()-work.used_bytes()).unwrap();
        let result=round.closure(key,output,true,0,false);
        admission(round.finish(result));
        assert!(retained.used_bytes()>before);
        assert_eq!(round.work.frames.capacity(),0);
        assert_eq!(round.configs.view_set(output).len(),0);
        assert!(matches!(round.configs.copy(key,ConfigChange::Same),Err(PredictionError::Aborted)));
        drop(keep);
        Ok(())
    });
}

#[test]
fn frame_failure_after_falloff_keeps_busy_suppression_increment_and_dips_prefix() {
    fixture(83,options(),|round,_,work| {
        let grammar=round.contexts.contexts().grammar();
        let stop=grammar.rule(26)?.stop();
        let empty = round.contexts.contexts().empty();
        let key=config(round.configs,round.contexts.contexts(),stop.number(),1,empty,Semantic::Unpredicated)?;
        let output=round.configs.new_set(SetMode::Local)?;
        round.work.busy.grow(round.work.allocation)?;
        let keep=work.reserve(work.limit_bytes()-work.used_bytes()).unwrap();
        let result=round.closure(key,output,false,0,false);
        admission(round.finish(result));
        assert_eq!(round.work.busy.len,1);
        let child=round.work.busy.slots.iter().flatten().next().unwrap().config;
        assert_eq!(round.configs.view(child).outer_depth(),1);
        assert!(round.configs.view(child).suppressed());
        assert!(round.configs.view_set(output).dips_outer());
        assert_eq!(round.work.frames.capacity(),0);
        drop(keep);
        Ok(())
    });
}

#[test]
fn busy_admission_failure_precedes_dips_and_frame_admission() {
    fixture(83,options(),|round,_,work| {
        let grammar=round.contexts.contexts().grammar();
        let stop=grammar.rule(26)?.stop();
        let empty = round.contexts.contexts().empty();
        let key=config(round.configs,round.contexts.contexts(),stop.number(),1,empty,Semantic::Unpredicated)?;
        let output=round.configs.new_set(SetMode::Local)?;
        let keep=work.reserve(work.limit_bytes()-work.used_bytes()).unwrap();
        let result=round.closure(key,output,false,0,false);
        admission(round.finish(result));
        assert_eq!(round.work.busy.len,0);
        assert!(!round.configs.view_set(output).dips_outer());
        assert_eq!(round.work.frames.capacity(),0);
        drop(keep);
        Ok(())
    });
}

#[test]
fn source_assertion_profile_reports_reach_stop_site_before_skipped_append() {
    fixture(65,options(),|round,_,_| {
        let contexts=round.contexts.contexts();
        let parent=contexts.singleton(contexts.empty(),contexts.grammar().state(353)?)?;
        let key=config(round.configs,contexts,1,1,parent,Semantic::Unpredicated)?;
        let input=set(round.configs,contexts,&[key])?;
        assert!(matches!(round.local_reach(LocalSet::new(input),TokenType::EOF),Err(PredictionError::SourceAssertion(SourceAssertionSite::ReachStopEmpty))));
        assert!(round.work.skipped.is_empty());
        assert_eq!(round.memo,MergePresence::Present);
        assert!(round.aborted);
        Ok(())
    });
}

#[test]
fn disabled_source_assertions_preserve_the_reached_stop_identity_without_rust_profile_inference() {
    fixture(65,ClosureOptions{assertions:SourceAssertions::Disabled,..options()},|round,_,_| {
        let contexts=round.contexts.contexts();
        let parent=contexts.singleton(contexts.empty(),contexts.grammar().state(353)?)?;
        let key=config(round.configs,contexts,1,1,parent,Semantic::Unpredicated)?;
        let input=set(round.configs,contexts,&[key])?;
        let output=round.local_reach(LocalSet::new(input),TokenType::EOF)?.unwrap();
        assert_eq!(round.configs.view_set(output.key()).entry(0),Some(key));
        assert_eq!(round.configs.view(key).context(),parent);
        Ok(())
    });
}

#[test]
fn pop_depth_assertion_is_after_child_copy_and_disabled_subtraction_wraps() {
    for assertions in [SourceAssertions::Enabled,SourceAssertions::Disabled] {
        fixture(65,ClosureOptions{assertions,..options()},|round,_,_| {
            let contexts=round.contexts.contexts();
            let parent=contexts.singleton(contexts.empty(),contexts.grammar().state(353)?)?;
            let key=config(round.configs,contexts,1,1,parent,Semantic::Unpredicated)?;
            let output=round.configs.new_set(SetMode::Local)?;
            let result=round.closure(key,output,true,i32::MIN,false);
            if assertions==SourceAssertions::Enabled {
                assert!(matches!(round.finish(result),Err(PredictionError::SourceAssertion(SourceAssertionSite::PopDepth))));
                assert_eq!(round.configs.view_set(output).len(),0);
                assert_eq!(round.work.frames.capacity(),0);
            } else {
                result?;
                assert_eq!(round.configs.view_set(output).len(),1);
                assert_eq!(round.configs.view(round.configs.view_set(output).entry(0).unwrap()).state()?.number(),353);
            }
            Ok(())
        });
    }
}

#[test]
fn falloff_depth_assertion_is_after_busy_and_dips_with_disabled_java_wrapping() {
    for assertions in [SourceAssertions::Enabled,SourceAssertions::Disabled] {
        fixture(83,ClosureOptions{assertions,..options()},|round,_,_| {
            let contexts=round.contexts.contexts();
            let key=config(round.configs,contexts,53,1,contexts.empty(),Semantic::Unpredicated)?;
            let output=round.configs.new_set(SetMode::Local)?;
            let result=round.closure(key,output,false,i32::MIN,false);
            if assertions==SourceAssertions::Enabled {
                assert!(matches!(round.finish(result),Err(PredictionError::SourceAssertion(SourceAssertionSite::FalloffDepth))));
                assert_eq!(round.work.busy.len,1);
                assert!(round.configs.view_set(output).dips_outer());
                assert_eq!(round.work.frames.capacity(),0);
            } else {
                result?;
                assert!(round.configs.view_set(output).dips_outer());
                assert!(round.work.frames.is_empty());
            }
            Ok(())
        });
    }
}

#[test]
fn unexpected_null_or_empty_return_context_is_an_explicit_local_domain_failure() {
    for empty_return in [false,true] {
        fixture(65,options(),|round,_,_| {
            let contexts=round.contexts.contexts();
            let follow=contexts.grammar().state(353)?;
            let bad=if empty_return {
                let normal=contexts.singleton(contexts.empty(),follow)?;
                contexts.merge_uncached(normal,contexts.empty(),MergeMode::FullExact)?
            } else { contexts.null_parent_fixture(follow)? };
            let key=config(round.configs,contexts,1,1,bad,Semantic::Unpredicated)?;
            let output=round.configs.new_set(SetMode::Local)?;
            let result=round.closure(key,output,true,0,false);
            assert!(matches!(round.finish(result),Err(PredictionError::Artifact(Problem::LocalClosure))));
            assert_eq!(round.configs.view_set(output).len(),if empty_return { 1 } else { 0 });
            Ok(())
        });
    }
}

#[test]
fn local_round_rejects_full_set_before_any_shortcut_or_memo_transition() {
    fixture(65,options(),|round,_,_| {
        let full=round.configs.new_set(SetMode::Full)?;
        assert!(matches!(round.local_reach(LocalSet::new(full),TokenType::EOF),Err(PredictionError::Artifact(Problem::LocalClosure))));
        assert_eq!(round.memo,MergePresence::Absent);
        assert!(round.aborted);
        Ok(())
    });
}

#[test]
fn cached_binding_uses_actual_c4_output_and_rejects_wrong_decision_without_losing_original_error() {
    retained_fixture(32<<20,32<<20,|access,_,_| {
        let (dfas,configs,contexts)=access.dfa_parts();
        dfas.initialize(configs)?;
        let decision=contexts.grammar().decision(65)?;
        let key=with_closure_round(configs,contexts,decision,options(),|round| {
            let StartSet::Ordinary(start)=round.local_start()? else {panic!("ordinary")};
            let (configs,contexts)=round.parts();
            let draft=dfas.new_draft(65,start.key(),configs)?;
            Ok(interned(dfas.add_state(65,Candidate::Draft(draft),configs,contexts)?))
        })?;
        let wrong=contexts.grammar().decision(64)?;
        with_closure_round(configs,contexts,wrong,options(),|round| {
            assert!(matches!(round.from_cached(dfas,key),Err(PredictionError::Artifact(Problem::DfaDecision))));
            assert!(matches!(round.from_cached(dfas,key),Err(PredictionError::Aborted)));
            assert!(round.aborted);
            Ok(())
        })
    });
}

#[test]
fn cached_binding_checks_aborted_contexts_before_identity_and_seals_the_dfa() {
    retained_fixture(32<<20,32<<20,|access,_,_| {
        let (dfas,configs,contexts)=access.dfa_parts();
        dfas.initialize(configs)?;
        let decision=contexts.grammar().decision(65)?;
        with_closure_round(configs,contexts,decision,options(),|round| {
            let StartSet::Ordinary(start)=round.local_start()? else {panic!("ordinary")};
            let key={let(configs,contexts)=round.parts();let draft=dfas.new_draft(65,start.key(),configs)?;interned(dfas.add_state(65,Candidate::Draft(draft),configs,contexts)?)};
            let _=round.contexts.contexts().finish::<()>(Err(PredictionError::Artifact(Problem::Context)));
            assert!(matches!(round.from_cached(dfas,key),Err(PredictionError::Aborted)));
            assert!(matches!(dfas.configs(65,key,round.configs),Err(PredictionError::Aborted)));
            assert!(round.configs.view_set(start.key()).len()>0);
            Ok(())
        })
    });
}

#[test]
fn actual_cold_precedence_prefilter_is_rooted_before_filter_and_interning_on_the_same_round() {
    retained_fixture(64<<20,64<<20,|access,_,_| {
        let(dfas,configs,contexts)=access.dfa_parts();
        dfas.initialize(configs)?;
        let decision=contexts.grammar().decision(83)?;
        with_closure_round(configs,contexts,decision,options(),|round| {
            let StartSet::Precedence(start)=round.local_start()? else {panic!("precedence")};
            dfas.replace_synthetic_prefilter(83,start.prefilter(),round.configs)?;
            let output=round.precedence_filter(start,0)?;
            let key={let(configs,contexts)=round.parts();let draft=dfas.new_draft(83,output.key(),configs)?;interned(dfas.add_state(83,Candidate::Draft(draft),configs,contexts)?)};
            dfas.set_start(83,0,key)?;
            let bound=round.from_cached(dfas,key)?;
            assert_eq!(bound.key(),output.key());
            assert!(round.configs.view_set(bound.key()).readonly());
            assert_eq!(dfas.start(83,0)?,Some(key));
            assert_eq!(round.memo,MergePresence::Absent);
            Ok(())
        })
    });
}

#[test]
fn deep_parent_pops_are_iterative_and_preserve_the_negative_depth_latch() {
    fixture(65,options(),|round,_,_| {
        let contexts=round.contexts.contexts();
        let grammar=contexts.grammar();
        let mut parent=contexts.singleton(contexts.empty(),grammar.state(353)?)?;
        for _ in 0..2048 { parent=contexts.singleton(parent,grammar.state(1)?)?; }
        let key=config(round.configs,contexts,1,1,parent,Semantic::Unpredicated)?;
        let output=round.configs.new_set(SetMode::Local)?;
        round.closure(key,output,true,-1,false)?;
        assert_eq!(round.configs.view_set(output).len(),1);
        assert_eq!(round.configs.view(round.configs.view_set(output).entry(0).unwrap()).state()?.number(),353);
        assert!(round.work.frames.is_empty());
        assert!(round.work.frames.capacity()>2048);
        Ok(())
    });
}

#[test]
fn factory_rejects_a_short_lived_foreign_decision_before_work_or_set_admission() {
    fixture(65,options(),|round,retained,work| {
        let before=(retained.used_bytes(),work.used_bytes());
        {
            let other_memory=MemoryBudget::new(8<<20);
            let other_scope=AllocationScope::new(&other_memory).unwrap();
            let foreign=Grammar::load(&other_scope).unwrap();
            let(configs,contexts)=round.parts();
            assert!(matches!(with_closure_round(configs,contexts,foreign.decision(65)?,options(),|_|Ok(())),Err(PredictionError::ForeignOwner)));
        }
        assert_eq!((retained.used_bytes(),work.used_bytes()),before);
        assert!(matches!(round.local_start(),Err(PredictionError::Aborted)));
        Ok(())
    });
}

#[test]
fn unwind_drops_round_work_and_leaves_actual_dfa_roots_available_to_fresh_access() {
    let grammar_memory=MemoryBudget::new(8<<20);
    let retained=MemoryBudget::new(64<<20);
    let grammar=OwnedGrammar::load(&grammar_memory).unwrap();
    let mut storage=PredictionStorage::new(grammar,&retained);
    let memory=MemoryBudget::new(64<<20);
    let work=AllocationScope::new(&memory).unwrap();
    let failed=catch_unwind(AssertUnwindSafe(|| {
        with_prediction_storage(&mut storage,&work,|access| {
            let(dfas,configs,contexts)=access.dfa_parts();
            dfas.initialize(configs)?;
            let decision=contexts.grammar().decision(65)?;
            with_closure_round(configs,contexts,decision,options(),|round| -> Result<()> {
                let StartSet::Ordinary(start)=round.local_start()? else {panic!("ordinary")};
                let(configs,contexts)=round.parts();
                let draft=dfas.new_draft(65,start.key(),configs)?;
                let key=interned(dfas.add_state(65,Candidate::Draft(draft),configs,contexts)?);
                dfas.set_start(65,0,key)?;
                panic!("after actual cache publication")
            })
        })
    }));
    assert!(failed.is_err());
    with_prediction_storage(&mut storage,&work,|access| {
        let(dfas,configs,contexts)=access.dfa_parts();
        let key=dfas.start(65,0)?.unwrap();
        let decision=contexts.grammar().decision(65)?;
        with_closure_round(configs,contexts,decision,options(),|round| {
            let set=round.from_cached(dfas,key)?;
            assert!(round.configs.view_set(set.key()).len()>0);
            assert!(round.configs.view_set(set.key()).readonly());
            assert_eq!(round.memo,MergePresence::Absent);
            Ok(())
        })
    }).unwrap();
    drop(work);
    assert_eq!(memory.used_bytes(),0);
    drop(storage);
    assert_eq!(retained.used_bytes(),0);
    assert_eq!(grammar_memory.used_bytes(),0);
}

#[test]
fn parent_continuation_reads_live_context_after_child_merges_the_original_alias() {
    fixture(65,options(),|round,_,_| {
        let contexts=round.contexts.contexts();
        let empty=contexts.empty();
        let stop=contexts.singleton(empty,contexts.grammar().state(1)?)?;
        let later=contexts.singleton(empty,contexts.grammar().state(353)?)?;
        let merged=contexts.merge_uncached(stop,later,MergeMode::LocalWildcard)?;
        let original=config(round.configs,contexts,1,1,merged,Semantic::Unpredicated)?;
        let output=set(round.configs,contexts,&[original])?;
        round.closure(original,output,true,0,false)?;
        assert_eq!(round.configs.view(original).context(),empty);
        assert_eq!(round.configs.view_set(output).len(),1);
        assert_eq!(round.configs.view_set(output).entry(0),Some(original));
        assert!(round.work.frames.is_empty());
        Ok(())
    });
}

#[test]
fn skipped_stop_append_admission_failure_preserves_the_input_and_seals_before_closure() {
    fixture(65,options(),|round,_,work| {
        let contexts=round.contexts.contexts();
        let original=config(round.configs,contexts,1,1,contexts.empty(),Semantic::Unpredicated)?;
        let input=set(round.configs,contexts,&[original])?;
        let keep=work.reserve(work.limit_bytes()-work.used_bytes()).unwrap();
        admission(round.local_reach(LocalSet::new(input),TokenType::EOF));
        assert_eq!(round.configs.view_set(input).entry(0),Some(original));
        assert!(round.work.skipped.is_empty());
        assert_eq!(round.work.frames.capacity(),0);
        assert_eq!(round.work.busy.slots.len(),0);
        assert!(round.aborted);
        drop(keep);
        Ok(())
    });
}

#[test]
fn vector_growth_admits_full_replacement_overlap_and_preserves_order_after_failure() {
    fixture(65,options(),|round,_,work| {
        let contexts=round.contexts.contexts();
        let original=config(round.configs,contexts,1,1,contexts.empty(),Semantic::Unpredicated)?;
        for _ in 0..16 { push(&mut round.work.skipped,original,round.work.allocation)?; }
        assert_eq!(round.work.skipped.capacity(),16);
        let needed=32*size_of::<ConfigKey<'_>>()+256;
        let leave=needed-1;
        let keep=work.reserve(work.limit_bytes()-work.used_bytes()-leave).unwrap();
        admission(push(&mut round.work.skipped,original,round.work.allocation));
        assert_eq!(round.work.skipped.len(),16);
        assert!(round.work.skipped.iter().all(|key|*key==original));
        drop(keep);
        push(&mut round.work.skipped,original,round.work.allocation)?;
        assert_eq!(round.work.skipped.capacity(),32);
        assert_eq!(round.work.skipped.len(),17);
        Ok(())
    });
}

#[test]
fn native_work_charges_survive_round_drop_and_are_released_with_the_work_scope() {
    let grammar_memory=MemoryBudget::new(8<<20);
    let grammar_scope=AllocationScope::new(&grammar_memory).unwrap();
    let grammar=Grammar::load(&grammar_scope).unwrap();
    let memory=MemoryBudget::new(32<<20);
    let work=AllocationScope::new(&memory).unwrap();
    let before=memory.used_bytes();
    with_prediction_contexts(&grammar,&work,|contexts| with_configurations(contexts,|configs,contexts| {
        let decision=contexts.grammar().decision(83)?;
        with_closure_round(configs,contexts,decision,options(),|round| {
            let StartSet::Precedence(start)=round.local_start()? else {panic!("precedence")};
            round.precedence_filter(start,0)?;
            assert!(round.work.frames.capacity()>0);
            Ok(())
        })
    })).unwrap();
    assert!(memory.used_bytes()>before);
    drop(work);
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn fresh_access_collects_unpublished_producer_work_and_keeps_only_actual_cache_roots() {
    let grammar_memory=MemoryBudget::new(8<<20);
    let retained=MemoryBudget::new(64<<20);
    let grammar=OwnedGrammar::load(&grammar_memory).unwrap();
    let mut storage=PredictionStorage::new(grammar,&retained);
    let memory=MemoryBudget::new(64<<20);
    let work=AllocationScope::new(&memory).unwrap();
    let mut peak=0;
    with_prediction_storage(&mut storage,&work,|access| {
        let(dfas,configs,contexts)=access.dfa_parts();
        dfas.initialize(configs)?;
        let decision=contexts.grammar().decision(83)?;
        with_closure_round(configs,contexts,decision,options(),|round| {
            let StartSet::Precedence(start)=round.local_start()? else {panic!("precedence")};
            dfas.replace_synthetic_prefilter(83,start.prefilter(),round.configs)?;
            let output=round.precedence_filter(start,0)?;
            let(configs,contexts)=round.parts();
            let draft=dfas.new_draft(83,output.key(),configs)?;
            let key=interned(dfas.add_state(83,Candidate::Draft(draft),configs,contexts)?);
            dfas.set_start(83,0,key)?;
            let empty=contexts.empty();
            for number in 0..96 { let _=contexts.singleton(empty,contexts.grammar().state(number)?)?; let _=configs.new_set(SetMode::Local)?; }
            peak=retained.used_bytes();
            Ok(())
        })
    }).unwrap();
    assert!(retained.used_bytes()<peak);
    with_prediction_storage(&mut storage,&work,|access| {
        let(dfas,configs,contexts)=access.dfa_parts();
        let key=dfas.start(83,0)?.unwrap();
        let decision=contexts.grammar().decision(83)?;
        with_closure_round(configs,contexts,decision,options(),|round| {
            let set=round.from_cached(dfas,key)?;
            assert!(round.configs.view_set(set.key()).len()>0);
            assert_eq!(dfas.state_count(83)?,1);
            Ok(())
        })
    }).unwrap();
    drop(work);
    assert_eq!(memory.used_bytes(),0);
    drop(storage);
    assert_eq!(retained.used_bytes(),0);
}

#[test]
fn deep_unequal_busy_collision_keeps_linked_cleanup_and_needs_no_admission() {
    fixture(65,options(),|round,retained,work| {
        let contexts=round.contexts.contexts();
        let follow=contexts.grammar().state(3)?;
        let mut a=contexts.null_parent_fixture(follow)?;
        let mut b=contexts.null_parent_fixture(follow)?;
        for _ in 0..128 {
            let tail=contexts.singleton(contexts.empty(),contexts.grammar().state(2)?)?;
            let left=contexts.singleton(a,contexts.grammar().state(1)?)?;
            let right=contexts.singleton(b,contexts.grammar().state(1)?)?;
            a=contexts.merge_uncached(left,tail,MergeMode::LocalWildcard)?;
            b=contexts.merge_uncached(right,tail,MergeMode::LocalWildcard)?;
        }
        let left=config(round.configs,contexts,352,1,a,Semantic::Unpredicated)?;
        let right=config(round.configs,contexts,352,1,b,Semantic::Unpredicated)?;
        assert_eq!(round.configs.config_hash(left,contexts)?,round.configs.config_hash(right,contexts)?);
        assert!(round.work.busy.add(left,round.configs,contexts,round.work.allocation)?);
        let keep_retained=retained.reserve(retained.limit_bytes()-retained.used_bytes()).unwrap();
        let keep_work=work.reserve(work.limit_bytes()-work.used_bytes()).unwrap();
        let before=(retained.used_bytes(),work.used_bytes());
        assert!(round.work.busy.add(right,round.configs,contexts,round.work.allocation)?);
        assert!(!round.work.busy.add(right,round.configs,contexts,round.work.allocation)?);
        assert_eq!((retained.used_bytes(),work.used_bytes()),before);
        drop((keep_retained,keep_work));
        assert!(!contexts.equivalent(a,b)?);
        Ok(())
    });
}

#[test]
fn context_and_config_admission_fail_at_their_reached_rule_prefix_before_any_frame() {
    for warm_context in [false,true] {
        fixture(65,options(),|round,retained,_| {
            let grammar=round.contexts.contexts().grammar();
            let (state,_,_)=find(grammar,|t|matches!(t,TransitionView::Rule{..}));
            let empty=round.contexts.contexts().empty();
            if warm_context { let _=round.contexts.contexts().singleton(empty,grammar.state(1)?)?; }
            let original=config(round.configs,round.contexts.contexts(),state.number(),1,empty,Semantic::Unpredicated)?;
            for _ in 1..16 { let _=round.configs.copy(original,ConfigChange::Same)?; }
            let output=round.configs.new_set(SetMode::Local)?;
            let keep=retained.reserve(retained.limit_bytes()-retained.used_bytes()).unwrap();
            let result=round.closure(original,output,true,0,false);
            admission(round.finish(result));
            assert!(round.aborted);
            assert_eq!(round.work.frames.capacity(),0);
            assert_eq!(round.configs.view_set(output).len(),0);
            assert_eq!(round.configs.view(original).context(),empty);
            drop(keep);
            Ok(())
        });
    }
}

#[test]
fn actual_c3_interning_between_reaches_keeps_the_same_present_merge_memo() {
    retained_fixture(64<<20,64<<20,|access,_,_| {
        let(dfas,configs,contexts)=access.dfa_parts();
        dfas.initialize(configs)?;
        let decision=contexts.grammar().decision(65)?;
        with_closure_round(configs,contexts,decision,options(),|round| {
            let StartSet::Ordinary(start)=round.local_start()? else {panic!("ordinary")};
            let mut token=None;
            for index in 0..round.configs.view_set(start.key()).len() {
                let key=round.configs.view_set(start.key()).entry(index).unwrap();
                for transition in round.configs.view(key).state()?.transitions() {
                    match transition {
                        TransitionView::Atom{token:value,..}=>{token=Some(value);break;},
                        TransitionView::Set{tokens,..}=>{token=tokens.iter().find_map(|symbol|match symbol{ExpectedSymbol::Token(value)=>Some(value),ExpectedSymbol::Epsilon=>None});},
                        _=>{},
                    }
                }
                if token.is_some(){break;}
            }
            let _=round.local_reach(start,token.unwrap())?;
            let contexts=round.contexts.contexts();
            let a=contexts.singleton(contexts.empty(),contexts.grammar().state(1)?)?;
            let b=contexts.singleton(contexts.empty(),contexts.grammar().state(2)?)?;
            let before=merge_pair(round,a,b)?;
            let key={let(configs,contexts)=round.parts();let draft=dfas.new_draft(65,start.key(),configs)?;interned(dfas.add_state(65,Candidate::Draft(draft),configs,contexts)?)};
            let after=merge_pair(round,a,b)?;
            assert_eq!(before,after);
            let cached=round.from_cached(dfas,key)?;
            assert_eq!(cached.key(),start.key());
            assert_eq!(round.memo,MergePresence::Present);
            let _=round.local_reach(cached,token.unwrap())?;
            assert_eq!(merge_pair(round,a,b)?,before);
            Ok(())
        })
    });
}

#[test]
fn busy_membership_is_shared_between_closures_but_cleared_for_each_start_alternative() {
    fixture(65,options(),|round,_,_| {
        let contexts=round.contexts.contexts();
        let original=config(round.configs,contexts,53,1,contexts.empty(),Semantic::Unpredicated)?;
        let first=round.configs.new_set(SetMode::Local)?;
        round.closure(original,first,false,0,false)?;
        assert!(round.work.busy.len>0);
        let second=round.configs.new_set(SetMode::Local)?;
        round.closure(original,second,false,0,false)?;
        assert_eq!(round.configs.view_set(second).len(),0);
        assert!(!round.configs.view_set(second).dips_outer());
        round.work.busy.clear();
        let third=round.configs.new_set(SetMode::Local)?;
        round.closure(original,third,false,0,false)?;
        assert!(round.configs.view_set(third).dips_outer());
        assert!(round.configs.equivalent_set(first,third,round.contexts.contexts())?);
        let _=round.local_start()?;
        let last_alt=round.decision.state().transitions().len();
        assert!(round.work.busy.slots.iter().flatten().all(|entry|round.configs.view(entry.config).alternative().number()==last_alt));
        Ok(())
    });
}

#[test]
fn nontrivial_reach_shares_one_busy_set_across_both_intermediate_alternatives() {
    fixture(65,options(),|round,_,_| {
        let contexts=round.contexts.contexts();
        let state=contexts.grammar().state(470)?;
        let TransitionView::Atom{token,..}=state.transitions().next().unwrap() else {panic!("pinned atom")};
        let a=config(round.configs,contexts,470,1,contexts.empty(),Semantic::Unpredicated)?;
        let b=config(round.configs,contexts,470,2,contexts.empty(),Semantic::Unpredicated)?;
        let input=set(round.configs,contexts,&[a,b])?;
        let output=round.local_reach(LocalSet::new(input),token)?.unwrap();
        assert!(round.configs.view_set(output.key()).dips_outer());
        for alt in [1,2] {
            assert!(round.work.busy.slots.iter().flatten().any(|entry|round.configs.view(entry.config).alternative().number()==alt));
        }
        Ok(())
    });
}

#[test]
fn cold_filter_admission_failure_preserves_the_actual_synthetic_prefilter_for_fresh_access() {
    let grammar_memory=MemoryBudget::new(8<<20);
    let retained=MemoryBudget::new(64<<20);
    let grammar=OwnedGrammar::load(&grammar_memory).unwrap();
    let mut storage=PredictionStorage::new(grammar,&retained);
    let memory=MemoryBudget::new(64<<20);
    let work=AllocationScope::new(&memory).unwrap();
    let (saved,count)=with_prediction_storage(&mut storage,&work,|access| {
        let(dfas,configs,contexts)=access.dfa_parts();
        dfas.initialize(configs)?;
        let decision=contexts.grammar().decision(83)?;
        with_closure_round(configs,contexts,decision,options(),|round| {
            let StartSet::Precedence(start)=round.local_start()? else {panic!("precedence")};
            let prefilter=start.prefilter();
            dfas.replace_synthetic_prefilter(83,prefilter,round.configs)?;
            let count=round.configs.view_set(prefilter).len();
            let keep=memory.reserve(memory.limit_bytes()-memory.used_bytes()).unwrap();
            admission(round.precedence_filter(start,0));
            assert!(round.aborted);
            assert_eq!(round.configs.view_set(prefilter).len(),count);
            assert_eq!(dfas.state_count(83)?,0);
            drop(keep);
            Ok((prefilter.identity(),count))
        })
    }).unwrap();
    with_prediction_storage(&mut storage,&work,|access| {
        let(dfas,configs,_)=access.dfa_parts();
        let prefilter=configs.restore_set(saved)?;
        assert_eq!(configs.view_set(prefilter).len(),count);
        assert!(count>0);
        assert_eq!(dfas.state_count(83)?,0);
        assert!(dfas.start(83,0)?.is_none());
        Ok(())
    }).unwrap();
    drop(work);
    assert_eq!(memory.used_bytes(),0);
    drop(storage);
    assert_eq!(retained.used_bytes(),0);
}
