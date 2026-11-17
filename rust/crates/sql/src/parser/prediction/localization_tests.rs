use super::*;
use crate::parser::grammar::OwnedGrammar;
use crate::parser::prediction::dfa::{Candidate, DfaTarget};
use crate::parser::prediction::runtime::{CapturedClassPolicy, ParserEnvironment, with_parser_session};
use crate::parser::prediction::{MergeMode, PredictionStorage};
use rtdb_types::MemoryBudget;
use std::sync::Arc;

fn fixture<R>(
    current_precedence: i32,
    run: impl for<'call, 'round, 'cfg, 'id, 'access, 'input, 'origin, 'parse, 'work> FnOnce(
        &mut ClosureRound<'call, 'round, 'cfg, 'id, 'access, 'access, Retained>,
        &mut PredictionDfas<'cfg, 'access, 'access>,
        &mut PredictionInput<'input, 'origin, 'round, 'parse, 'static, 'work>,
        &Arc<MemoryBudget>,
    ) -> RuntimeResult<'static, R>,
) -> RuntimeResult<'static, R> {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
    let retained = MemoryBudget::new(64 << 20);
    let work_memory = MemoryBudget::new(64 << 20);
    let work = AllocationScope::new(&work_memory).unwrap();
    let policy = CapturedClassPolicy::from_captured(ClosureOptions {
        pruning: LoopEntryOption::Enabled,
        assertions: SourceAssertions::Enabled,
    });
    let mut environment = ParserEnvironment::new(PredictionStorage::new(grammar, &retained), policy, &work).unwrap();

    let result = with_parser_session(&mut environment, "1 2", &work, |session| {
        let pending = session.new_recursion_entry(26)?;
        let mut entry = session.enter_recursion(pending, current_precedence)?;
        let result = session.with_prediction(83, |round, dfas, input| run(round, dfas, input, &retained));
        assert_eq!(session.input_index(), Some(0));
        session.unroll_recursion(&mut entry)?;
        result
    });

    drop(environment);
    assert_eq!(retained.used_bytes(), 0);
    assert_eq!(grammar_memory.used_bytes(), 0);
    drop(work);
    assert_eq!(work_memory.used_bytes(), 0);
    result
}

fn predicate(round: &mut ClosureRound<'_, '_, '_, '_, '_, '_, Retained>) -> Result<Semantic> {
    let grammar = round.contexts.contexts().grammar();
    for number in 0..3017 {
        let state = grammar.state(number)?;
        for (index, transition) in state.transitions().enumerate() {
            if matches!(transition, TransitionView::Precedence { .. }) {
                return round.configs.precedence(state, index);
            }
        }
    }
    panic!("pinned grammar has precedence predicates");
}

fn add<'cfg, 'id>(
    round: &mut ClosureRound<'_, '_, 'cfg, 'id, '_, '_, Retained>,
    set: ConfigSetKey<'cfg>,
    state: u32,
    ordinal: usize,
    context: ContextKey<'id>,
    semantic: Semantic,
    outer: bool,
) -> Result<ConfigKey<'cfg>> {
    let state = round.contexts.contexts().grammar().state(state)?;
    let alternative = round.configs.alternative(round.decision, ordinal)?;
    let config = round.configs.create(state, alternative, context, semantic)?;
    if outer {
        round.configs.increment_outer(config)?;
    }
    round.configs.add_uncached(set, config, round.contexts.contexts())?;
    Ok(config)
}

#[test]
fn the_signed_precedence_selects_which_finishing_alternative_is_valid() {
    for (precedence, expected) in [(-1, 1), (0, 1), (100, 2)] {
        fixture(precedence, |round, _, input, _| {
            let predicate = predicate(round)?;
            let empty = round.contexts.contexts().empty();
            let stop = round.decision.state().rule().stop().number();
            let set = round.configs.new_set(SetMode::Local)?;
            add(round, set, stop, 1, empty, predicate, false)?;
            add(round, set, stop, 2, empty, Semantic::Unpredicated, false)?;

            let chosen = round.local_finishing_alternative(LocalSet::new(set), input)?;
            assert_eq!(chosen.map(Alternative::number), Some(expected));
            assert_eq!(round.configs.view_set(set).len(), 2);
            assert!(matches!(round.memo, MergePresence::Absent));
            Ok(())
        }).unwrap();
    }
}

#[test]
fn false_predicate_can_finish_when_valid_members_do_not() {
    fixture(100, |round, _, input, _| {
        let predicate = predicate(round)?;
        let empty = round.contexts.contexts().empty();
        let stop = round.decision.state().rule().stop().number();
        let state = round.decision.state().number();
        let set = round.configs.new_set(SetMode::Local)?;
        add(round, set, stop, 1, empty, predicate, false)?;
        let ordinary = add(round, set, state, 2, empty, Semantic::Unpredicated, false)?;
        assert!(!matches!(round.configs.view(ordinary).state()?.kind(), StateKindView::RuleStop));

        let chosen = round.local_finishing_alternative(LocalSet::new(set), input)?;
        assert_eq!(chosen.map(Alternative::number), Some(1));
        Ok(())
    }).unwrap();
}

#[test]
fn cached_local_split_preserves_readonly_values_and_repeated_identity() {
    fixture(100, |round, dfas, input, _| {
        let empty = round.contexts.contexts().empty();
        let state = round.decision.state().number();
        let set = round.configs.new_set(SetMode::Local)?;
        let head = add(round, set, state, 1, empty, Semantic::Unpredicated, true)?;
        round.configs.add_uncached(set, head, round.contexts.contexts())?;
        let draft = dfas.new_draft(83, set, round.configs)?;
        let DfaTarget::Interned(state) = dfas.add_state(83, Candidate::Draft(draft), round.configs, round.contexts.contexts())? else {
            panic!("expected interned fixture");
        };
        let previous = round.from_cached(dfas, state)?;
        let contexts = round.contexts.contexts();
        let left = contexts.singleton(empty, contexts.grammar().state(356)?)?;
        let right = contexts.singleton(empty, contexts.grammar().state(370)?)?;
        let representative = round.contexts.merge(left, right, MergeMode::LocalWildcard)?;
        round.memo = MergePresence::Present;

        let chosen = round.local_finishing_alternative(previous, input)?;
        assert_eq!(chosen.map(Alternative::number), Some(1));
        let view = round.configs.view_set(set);
        assert!(view.readonly());
        assert_eq!((view.len(), view.entry(0), view.entry(1)), (2, Some(head), Some(head)));
        assert_eq!(round.configs.view(head).context(), empty);
        assert_eq!(round.configs.view(head).outer_depth(), 1);
        assert!(matches!(round.memo, MergePresence::Present));
        assert_eq!(round.contexts.merge(right, left, MergeMode::LocalWildcard)?, representative);
        Ok(())
    }).unwrap();
}

#[test]
fn full_split_recognizes_the_terminal_empty_return_of_a_nonempty_context() {
    fixture(100, |round, _, input, _| {
        let mut full = round.enter_full(input)?;
        full.full_start()?;
        let contexts = full.round.contexts.contexts();
        let empty = contexts.empty();
        let branch = contexts.singleton(empty, contexts.grammar().state(356)?)?;
        let mixed = contexts.merge_uncached(branch, empty, MergeMode::FullExact)?;
        assert!(!contexts.view(mixed).is_empty());
        assert!(!contexts.view(branch).has_empty_path());
        assert!(contexts.view(mixed).has_empty_path());
        let stop = full.round.decision.state().rule().stop().number();
        let set = full.round.configs.new_set(SetMode::Full)?;
        add(full.round, set, stop, 1, branch, Semantic::Unpredicated, false)?;
        add(full.round, set, stop, 2, mixed, Semantic::Unpredicated, false)?;

        let chosen = full.full_finishing_alternative(FullSet::new(set))?;
        assert_eq!(chosen.map(Alternative::number), Some(2));
        assert!(matches!(full.round.memo, MergePresence::Absent));
        Ok(())
    }).unwrap();
}

#[test]
fn split_admission_error_survives_the_prediction_round() {
    let result = fixture(100, |round, _, input, retained| {
        let empty = round.contexts.contexts().empty();
        let stop = round.decision.state().rule().stop().number();
        let set = round.configs.new_set(SetMode::Local)?;
        add(round, set, stop, 1, empty, Semantic::Unpredicated, false)?;
        let _fill = retained.reserve(retained.limit_bytes() - retained.used_bytes()).unwrap();
        round.local_finishing_alternative(LocalSet::new(set), input)
    });
    assert!(matches!(result, Err(RuntimeError::Prediction(PredictionError::Admission(error)))
        if error.code == "rtdb/memory-budget-exceeded"));
}

#[test]
fn finishing_alternative_rejects_an_unrewound_input_and_restores_the_origin() {
    let result = fixture(100, |round, _, input, _| {
        let empty = round.contexts.contexts().empty();
        let stop = round.decision.state().rule().stop().number();
        let set = round.configs.new_set(SetMode::Local)?;
        add(round, set, stop, 1, empty, Semantic::Unpredicated, false)?;

        input.advance()?;
        assert_eq!(input.index(), Some(1));
        round.local_finishing_alternative(LocalSet::new(set), input)
    });
    assert!(matches!(result, Err(RuntimeError::Control(RuntimeProblem::PredictionSite))));
}
