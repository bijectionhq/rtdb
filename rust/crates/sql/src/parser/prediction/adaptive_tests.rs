use std::sync::Arc;
use rtdb_types::{AllocationScope, MemoryBudget, MemoryReservation};
use crate::lexer::{Kind, ScanError, StreamError};
use crate::parser::grammar::{Grammar, OwnedGrammar, TransitionView};
use super::*;
use super::super::{ContextKey, MergeMode, PredictionStorage, SetMode};
use super::super::closure::{ClosureOptions, LoopEntryOption, SourceAssertions};
use super::super::runtime::{CapturedClassPolicy, ParserEnvironment, ParserSession, ParserState, RuntimeError, RuntimeProblem, with_parser_session};

const ISOLATION_RULE: u16 = 175;
const ISOLATION_DECISION: u16 = 371;
const DATA_TYPE_RULE: u16 = 26;
const DATA_TYPE_LOOP: u16 = 83;

fn environment(run: impl FnOnce(&mut ParserEnvironment, &AllocationScope, &Arc<MemoryBudget>, &Arc<MemoryBudget>)) {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
    let retained = MemoryBudget::new(64 << 20);
    let memory = MemoryBudget::new(64 << 20);
    let work = AllocationScope::new(&memory).unwrap();
    let policy = CapturedClassPolicy::from_captured(ClosureOptions {
        pruning: LoopEntryOption::Enabled,
        assertions: SourceAssertions::Enabled,
    });
    let mut environment = ParserEnvironment::new(PredictionStorage::new(grammar, &retained), policy, &work).unwrap();
    run(&mut environment, &work, &retained, &memory);
    drop(environment);
    drop(work);
    assert_eq!(grammar_memory.used_bytes(), 0);
    assert_eq!(retained.used_bytes(), 0);
    assert_eq!(memory.used_bytes(), 0);
}

fn isolation<'sql>(session: &mut ParserSession<'_, 'sql, '_, '_>) -> RuntimeResult<'sql, ()> {
    let invocation = session.new_invocation(ISOLATION_RULE)?;
    session.enter_rule(invocation)?;
    session.set_state(3014)
}

fn interned<'cfg>(target: DfaTarget<'cfg>) -> InternedStateKey<'cfg> {
    let DfaTarget::Interned(state) = target else { panic!("ordinary DFA target") };
    state
}

fn exhaust(memory: &Arc<MemoryBudget>) -> Arc<MemoryReservation> {
    memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap()
}

#[test]
fn adaptive_isolation_uses_two_real_tokens_and_leaves_the_following_token_unreached() {
    for (cold, warm) in [(PredictionMode::Sll, PredictionMode::Ll), (PredictionMode::Ll, PredictionMode::Sll)] {
        environment(|env, work, retained, memory| {
            with_parser_session(env, "READ COMMITTED @", work, |session| {
                isolation(session)?;
                for (mode, cached) in [(cold, false), (warm, true)] {
                    let blockers = cached.then(|| (exhaust(retained), exhaust(memory)));
                    assert_eq!(session.adaptive_predict(ISOLATION_DECISION, mode)?.number(), 2);
                    assert_eq!(session.input_index(), Some(0));
                    assert_eq!(session.buffered_token(1).unwrap().lexeme().raw(), "COMMITTED");
                    assert!(session.buffered_token(2).is_none());
                    drop(blockers);
                }
                session.exit_rule()
            }).unwrap();
        });
    }
}

#[test]
fn adaptive_isolation_reports_the_original_offending_occurrence_after_rewind() {
    for (sql, eof, expected_raw) in [
        ("before READ /* ignored */ read", false, "read"),
        ("before READ", true, ""),
    ] {
        environment(|env, work, _, _| {
            for mode in [PredictionMode::Sll, PredictionMode::Ll] {
                let result = with_parser_session(env, sql, work, |session| {
                    isolation(session)?;
                    session.advance_input()?;
                    let invocation = session.current();
                    let result = session.adaptive_predict(ISOLATION_DECISION, mode);
                    assert_eq!(session.input_index(), Some(1));
                    assert_eq!(session.state(), ParserState::State(3014));
                    assert_eq!(session.current(), invocation);
                    assert!(matches!(session.lookahead(1), Err(RuntimeError::Control(RuntimeProblem::Aborted))));
                    session.exit_rule()?;
                    result
                });
                let Err(RuntimeError::NoViableAlternative(error)) = result else {
                    panic!("adaptive no viable alternative: {result:?}");
                };
                assert_eq!((error.start.index(), error.start.lexeme().raw()), (1, "READ"));
                let expected_kind = if eof { Kind::Eof } else { error.start.lexeme().kind() };
                assert_eq!((error.offending.index(), error.offending.lexeme().kind(), error.offending.lexeme().raw()),
                    (2, expected_kind, expected_raw));
                assert_eq!(error.offending.lexeme().span().start.byte, sql.len() - expected_raw.len());
                assert_eq!(error.state, ParserState::State(3014));
            }
        });
    }
}

#[test]
fn adaptive_isolation_propagates_the_following_lexer_fault_before_localization() {
    environment(|env, work, _, _| {
        for mode in [PredictionMode::Sll, PredictionMode::Ll] {
            let result = with_parser_session(env, "READ @", work, |session| {
                isolation(session)?;
                let result = session.adaptive_predict(ISOLATION_DECISION, mode);
                assert_eq!(session.input_index(), Some(0));
                assert!(session.buffered_token(1).is_none());
                session.exit_rule()?;
                result
            });
            assert!(matches!(result, Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_))))));
        }
    });
}

#[test]
fn adaptive_isolation_retains_the_actual_token_buffer_admission_failure() {
    environment(|env, work, _, memory| {
        with_parser_session(env, "READ COMMITTED", work, |session| {
            isolation(session)?;
            assert_eq!(session.adaptive_predict(ISOLATION_DECISION, PredictionMode::Sll)?.number(), 2);
            session.exit_rule()
        }).unwrap();

        let sql = format!("{}READ COMMITTED", "before ".repeat(99));
        let result = with_parser_session(env, &sql, work, |session| {
            isolation(session)?;
            for _ in 0..99 {
                session.advance_input()?;
            }
            let blocker = exhaust(memory);
            let result = session.adaptive_predict(ISOLATION_DECISION, PredictionMode::Ll);
            assert_eq!(session.input_index(), Some(99));
            assert!(session.buffered_token(100).is_none());
            drop(blocker);
            session.exit_rule()?;
            result
        });
        assert!(matches!(result, Err(RuntimeError::Input(StreamError::Scan(ScanError::Admission(error))))
            if error.code == "rtdb/memory-budget-exceeded"));
    });
}

#[test]
fn overlapping_insert_values_and_query_alternatives_reach_full_prediction_only_in_ll() {
    const RULE: u16 = 157;
    const DECISION: u16 = 335;
    environment(|env, work, retained, _| {
        for (mode, block_full) in [
            (PredictionMode::Sll, false),
            (PredictionMode::Sll, true),
            (PredictionMode::Ll, true),
            (PredictionMode::Ll, false),
        ] {
            let result = with_parser_session(env, "VALUES (1)", work, |session| {
                let invocation = session.new_invocation(RULE)?;
                session.enter_rule(invocation)?;
                session.set_state(2804)?;
                let blocker = block_full.then(|| exhaust(retained));
                let result = session.adaptive_predict(DECISION, mode);
                assert_eq!(session.input_index(), Some(0));
                drop(blocker);
                session.exit_rule()?;
                result
            });
            if block_full && mode == PredictionMode::Ll {
                assert!(matches!(result, Err(RuntimeError::Prediction(PredictionError::Admission(error)))
                    if error.code == "rtdb/memory-budget-exceeded"));
            } else {
                assert_eq!(result.unwrap().number(), 1);
            }
        }
    });
}

#[test]
fn data_type_branch_evaluates_the_stored_array_predicate_at_the_prediction_origin() {
    const DECISION: u16 = 82;
    environment(|env, work, _, _| {
        for mode in [PredictionMode::Sll, PredictionMode::Ll] {
            for precedence in [2, 3] {
                let result = with_parser_session(env, "INT ARRAY @", work, |session| {
                    let pending = session.new_recursion_entry(DATA_TYPE_RULE)?;
                    let mut entry = session.enter_recursion(pending, precedence)?;
                    session.advance_input()?;
                    session.set_state(955)?;
                    let result = session.adaptive_predict(DECISION, mode);
                    assert_eq!(session.input_index(), Some(1));
                    assert!(session.buffered_token(2).is_none());
                    session.unroll_recursion(&mut entry)?;
                    result
                });
                if precedence == 2 {
                    assert_eq!(result.unwrap().number(), 1);
                } else {
                    let Err(RuntimeError::NoViableAlternative(error)) = result else {
                        panic!("array precedence rejection: {result:?}");
                    };
                    assert_eq!((error.start.index(), error.offending.index()), (1, 1));
                    assert_eq!(error.offending.lexeme().raw(), "ARRAY");
                    assert_eq!(error.state, ParserState::State(955));
                }
            }
        }
    });
}

#[test]
fn real_regular_start_and_successive_edges_warm_across_modes_without_admission() {
    for (sql, accepts) in [("READ COMMITTED", true), ("READ READ", false)] {
        environment(|env, work, retained, memory| {
            with_parser_session(env, sql, work, |session| {
                isolation(session)?;
                for mode in [PredictionMode::Sll, PredictionMode::Ll] {
                    session.with_prediction(ISOLATION_DECISION, |round, dfas, input| {
                        let decision = input.decision();
                        let blockers = (mode == PredictionMode::Ll).then(|| (exhaust(retained), exhaust(memory)));
                        let start = local_dfa_start(round, dfas, input)?;
                        assert_eq!(dfas.start(decision, i32::MIN)?, Some(start));
                        assert_eq!(dfas.state_at(decision, 0)?, Some(start));
                        assert!(matches!(dfas.view(decision, start)?.acceptance(), Acceptance::NonAccept));

                        let grammar = round.parts().1.grammar();
                        let read = lookahead_type(input, grammar)?;
                        let previous = interned(local_dfa_target(round, dfas, input, start, read, mode)?);
                        assert_eq!(dfas.state_at(decision, 1)?, Some(previous));
                        assert_eq!(dfas.existing_edge(decision, start, read.number())?, Some(DfaTarget::Interned(previous)));
                        assert!(matches!(dfas.view(decision, previous)?.acceptance(), Acceptance::NonAccept));
                        input.advance()?;
                        let token = lookahead_type(input, grammar)?;
                        let target = local_dfa_target(round, dfas, input, previous, token, mode)?;
                        assert_eq!(dfas.existing_edge(decision, previous, token.number())?, Some(target));
                        if accepts {
                            let target = interned(target);
                            assert_eq!(dfas.state_at(decision, 2)?, Some(target));
                            let configs = round.parts().0;
                            let set = dfas.configs(decision, target, configs)?;
                            let prediction = configs.view_set(set).unique().unwrap();
                            assert_eq!(prediction.number(), 2);
                            assert!(matches!(dfas.view(decision, target)?.acceptance(), Acceptance::Plain { reason: AcceptReason::Unique, prediction: alt } if alt == prediction));
                        } else {
                            assert_eq!(target, DfaTarget::Error);
                        }
                        assert_eq!(dfas.state_count(decision)?, if accepts { 3 } else { 2 });
                        assert_eq!(input.index(), Some(1));
                        drop(blockers);
                        Ok(())
                    })?;
                    assert_eq!(session.input_index(), Some(0));
                }
                session.exit_rule()
            }).unwrap();
        });
    }
}

#[test]
fn real_precedence_start_uses_signed_stack_top_and_the_interned_representative() {
    environment(|env, work, retained, memory| {
        for (precedence, warm) in [(0, false), (3, false), (0, true), (3, true), (-7, false), (-7, false)] {
            with_parser_session(env, "INT ARRAY", work, |session| {
                let pending = session.new_recursion_entry(DATA_TYPE_RULE)?;
                let mut entry = session.enter_recursion(pending, precedence)?;
                session.advance_input()?;
                session.set_state(957)?;
                session.with_prediction(DATA_TYPE_LOOP, |round, dfas, input| {
                    let blockers = warm.then(|| (exhaust(retained), exhaust(memory)));
                    assert_eq!(input.precedence(), precedence);
                    assert_eq!(dfas.start(DATA_TYPE_LOOP, precedence)?.is_some(), warm);
                    let start = local_dfa_start(round, dfas, input)?;
                    if precedence < 0 {
                        assert_eq!(dfas.start(DATA_TYPE_LOOP, precedence)?, None);
                        assert_eq!(dfas.start(DATA_TYPE_LOOP, 0)?, Some(start));
                    } else {
                        assert_eq!(dfas.start(DATA_TYPE_LOOP, precedence)?, Some(start));
                    }
                    if let (Some(low), Some(high)) = (dfas.start(DATA_TYPE_LOOP, 0)?, dfas.start(DATA_TYPE_LOOP, 3)?) {
                        assert_ne!(low, high);
                        assert_eq!(dfas.state_count(DATA_TYPE_LOOP)?, 2);
                    }
                    assert!(matches!(dfas.view(DATA_TYPE_LOOP, start)?.acceptance(), Acceptance::NonAccept));
                    assert_eq!(input.index(), Some(1));
                    drop(blockers);
                    Ok(())
                })?;
                assert_eq!(session.input_index(), Some(1));
                session.unroll_recursion(&mut entry)?;
                Ok(())
            }).unwrap();
        }
    });
}

#[test]
fn completed_precedence_interning_survives_failed_start_edge_publication() {
    environment(|env, work, _, _| {
        let precedence = i32::MAX;
        let result = with_parser_session(env, "INT", work, |session| {
            let pending = session.new_recursion_entry(DATA_TYPE_RULE)?;
            let mut entry = session.enter_recursion(pending, precedence)?;
            session.advance_input()?;
            session.set_state(957)?;
            let result = session.with_prediction(DATA_TYPE_LOOP, |round, dfas, input| {
                local_dfa_start(round, dfas, input).map(|_| ())
            });
            assert_eq!(session.input_index(), Some(1));
            session.unroll_recursion(&mut entry)?;
            result
        });
        assert!(matches!(result, Err(RuntimeError::Prediction(PredictionError::Artifact(Problem::Capacity)))));

        with_parser_session(env, "INT", work, |session| {
            let pending = session.new_recursion_entry(DATA_TYPE_RULE)?;
            let mut entry = session.enter_recursion(pending, 3)?;
            session.advance_input()?;
            session.set_state(957)?;
            session.with_prediction(DATA_TYPE_LOOP, |round, dfas, input| {
                assert_eq!(dfas.start(DATA_TYPE_LOOP, precedence)?, None);
                assert_eq!(dfas.state_count(DATA_TYPE_LOOP)?, 1);
                let prior = dfas.state_at(DATA_TYPE_LOOP, 0)?.unwrap();
                let start = local_dfa_start(round, dfas, input)?;
                assert_eq!(start, prior);
                assert_eq!(dfas.start(DATA_TYPE_LOOP, 3)?, Some(prior));
                Ok(())
            })?;
            session.unroll_recursion(&mut entry)?;
            Ok(())
        }).unwrap();
    });
}

#[test]
fn warm_local_start_and_target_reject_terminal_full_phase() {
    for start_lookup in [true, false] {
        environment(|env, work, retained, memory| {
            let result = with_parser_session(env, "READ COMMITTED", work, |session| {
                isolation(session)?;
                let result = session.with_prediction(ISOLATION_DECISION, |round, dfas, input| {
                    let start = local_dfa_start(round, dfas, input)?;
                    let token = lookahead_type(input, round.parts().1.grammar())?;
                    local_dfa_target(round, dfas, input, start, token, PredictionMode::Sll)?;
                    drop(round.enter_full(input)?);
                    let _blockers = (exhaust(retained), exhaust(memory));
                    if start_lookup {
                        local_dfa_start(round, dfas, input).map(|_| ())
                    } else {
                        local_dfa_target(round, dfas, input, start, token, PredictionMode::Ll).map(|_| ())
                    }
                });
                assert_eq!(session.input_index(), Some(0));
                session.exit_rule()?;
                result
            });
            assert!(matches!(result, Err(RuntimeError::Prediction(PredictionError::Artifact(Problem::ClosurePhase)))));
        });
    }
}

#[test]
fn real_miss_admission_failure_leaves_the_prior_start_and_allows_a_fresh_retry() {
    environment(|env, work, retained, _| {
        for fail in [true, false] {
            let result = with_parser_session(env, "READ COMMITTED", work, |session| {
                isolation(session)?;
                let result = session.with_prediction(ISOLATION_DECISION, |round, dfas, input| {
                    let start = local_dfa_start(round, dfas, input)?;
                    assert_eq!(dfas.state_count(ISOLATION_DECISION)?, 1);
                    let token = lookahead_type(input, round.parts().1.grammar())?;
                    assert_eq!(dfas.existing_edge(ISOLATION_DECISION, start, token.number())?, None);
                    let _blocker = fail.then(|| exhaust(retained));
                    let target = interned(local_dfa_target(round, dfas, input, start, token, PredictionMode::Sll)?);
                    assert_eq!(dfas.existing_edge(ISOLATION_DECISION, start, token.number())?, Some(DfaTarget::Interned(target)));
                    Ok(())
                });
                assert_eq!(session.input_index(), Some(0));
                session.exit_rule()?;
                result
            });
            if fail {
                assert!(matches!(result, Err(RuntimeError::Prediction(PredictionError::Admission(error))) if error.code == "rtdb/memory-budget-exceeded"));
            } else {
                result.unwrap();
            }
        }
    });
}

#[test]
fn completed_local_edge_survives_the_real_following_token_error() {
    environment(|env, work, retained, memory| {
        let result = with_parser_session(env, "READ @", work, |session| {
            isolation(session)?;
            let result = session.with_prediction(ISOLATION_DECISION, |round, dfas, input| {
                let start = local_dfa_start(round, dfas, input)?;
                let token = lookahead_type(input, round.parts().1.grammar())?;
                let target = interned(local_dfa_target(round, dfas, input, start, token, PredictionMode::Sll)?);
                assert!(matches!(dfas.view(ISOLATION_DECISION, target)?.acceptance(), Acceptance::NonAccept));
                input.advance()
            });
            assert_eq!(session.input_index(), Some(0));
            session.exit_rule()?;
            result
        });
        assert!(matches!(result, Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_))))));

        with_parser_session(env, "READ COMMITTED", work, |session| {
            isolation(session)?;
            session.with_prediction(ISOLATION_DECISION, |round, dfas, input| {
                let _blockers = (exhaust(retained), exhaust(memory));
                let start = local_dfa_start(round, dfas, input)?;
                let token = lookahead_type(input, round.parts().1.grammar())?;
                let target = interned(local_dfa_target(round, dfas, input, start, token, PredictionMode::Ll)?);
                assert_eq!(dfas.state_at(ISOLATION_DECISION, 1)?, Some(target));
                Ok(())
            })?;
            session.exit_rule()
        }).unwrap();
    });
}

fn precedence<'g, 'scope>(configs: &mut Configurations<'_, '_, 'g, 'scope, Retained>, grammar: &'g Grammar<'scope>, value: i32) -> Result<Semantic> {
    for number in 0..3017 {
        let state = grammar.state(number)?;
        for (index, transition) in state.transitions().enumerate() {
            if matches!(transition, TransitionView::Precedence { precedence, .. } if precedence == value) {
                return configs.precedence(state, index);
            }
        }
    }
    Err(PredictionError::Artifact(Problem::Precedence))
}

fn add<'cfg, 'id, 'g, 'scope>(
    configs: &mut Configurations<'cfg, 'id, 'g, 'scope, Retained>,
    contexts: &mut PredictionContexts<'id, 'g, 'scope, Retained>,
    set: ConfigSetKey<'cfg>,
    state: u32,
    ordinal: usize,
    context: ContextKey<'id>,
    semantic: Semantic,
) -> Result<()> {
    let grammar = contexts.grammar();
    let alternative = configs.alternative(grammar.decision(ISOLATION_DECISION)?, ordinal)?;
    let config = configs.create(grammar.state(state)?, alternative, context, semantic)?;
    configs.add_uncached(set, config, contexts)
}

#[test]
fn late_predicated_conflict_restarts_real_ll_and_uses_its_unique_choice_or_offending_token() {
    for (sql, viable) in [("READ COMMITTED", true), ("READ read", false)] {
        environment(|env, work, _, _| {
            let result = with_parser_session(env, sql, work, |session| {
                isolation(session)?;
                session.with_prediction(ISOLATION_DECISION, |round, dfas, input| {
                    let start = local_dfa_start(round, dfas, input)?;
                    let grammar = round.parts().1.grammar();
                    let first = lookahead_type(input, grammar)?;
                    let previous = interned(local_dfa_target(round, dfas, input, start, first, PredictionMode::Sll)?);
                    assert!(matches!(dfas.view(ISOLATION_DECISION, previous)?.acceptance(), Acceptance::NonAccept));
                    input.advance()?;
                    let second = lookahead_type(input, grammar)?;

                    let (configs, contexts) = round.parts();
                    let decision = grammar.decision(ISOLATION_DECISION)?;
                    let stop = decision.state().rule().stop().number();
                    let predicate = precedence(configs, grammar, 1)?;
                    let set = configs.new_set(SetMode::Local)?;
                    let empty = contexts.empty();
                    add(configs, contexts, set, stop, 1, empty, predicate)?;
                    add(configs, contexts, set, stop, 2, empty, Semantic::Unpredicated)?;
                    let draft = prepare_target(dfas, configs, contexts, decision, set, PredictionMode::Sll)?;
                    assert!(matches!(dfas.view_draft(ISOLATION_DECISION, draft)?.acceptance(),
                        Acceptance::Predicated { reason: AcceptReason::Conflict, .. }));
                    dfas.add_edge(ISOLATION_DECISION, Some(previous), second.number(), Some(Candidate::Draft(draft)), configs, contexts)?;
                    Ok(())
                })?;

                assert_eq!(session.adaptive_predict(ISOLATION_DECISION, PredictionMode::Sll)?.number(), 1);
                let result = session.adaptive_predict(ISOLATION_DECISION, PredictionMode::Ll);
                assert_eq!(session.input_index(), Some(0));
                session.exit_rule()?;
                result
            });
            if viable {
                assert_eq!(result.unwrap().number(), 2);
            } else {
                let Err(RuntimeError::NoViableAlternative(error)) = result else {
                    panic!("full no viable alternative: {result:?}");
                };
                assert_eq!((error.start.index(), error.start.lexeme().raw()), (0, "READ"));
                assert_eq!((error.offending.index(), error.offending.lexeme().raw()), (1, "read"));
                assert_eq!(error.state, ParserState::State(3014));
            }
        });
    }
}

#[test]
fn ll_conflict_with_one_passing_predicate_resolves_from_the_warm_local_state() {
    const DECISION: u16 = 82;
    environment(|env, work, retained, memory| {
        with_parser_session(env, "INT ARRAY @", work, |session| {
            let pending = session.new_recursion_entry(DATA_TYPE_RULE)?;
            let mut entry = session.enter_recursion(pending, 2)?;
            session.advance_input()?;
            session.set_state(955)?;
            session.with_prediction(DECISION, |round, dfas, input| {
                let start = local_dfa_start(round, dfas, input)?;
                let (configs, contexts) = round.parts();
                let grammar = contexts.grammar();
                let token = lookahead_type(input, grammar)?;
                let decision = grammar.decision(DECISION)?;
                let stop = decision.state().rule().stop();
                let set = configs.new_set(SetMode::Local)?;
                for (ordinal, threshold) in [(1, 1), (2, 2)] {
                    let alternative = configs.alternative(decision, ordinal)?;
                    let semantic = precedence(configs, grammar, threshold)?;
                    let config = configs.create(stop, alternative, contexts.empty(), semantic)?;
                    configs.add_uncached(set, config, contexts)?;
                }
                let draft = prepare_target(dfas, configs, contexts, decision, set, PredictionMode::Sll)?;
                assert!(matches!(dfas.view_draft(DECISION, draft)?.acceptance(),
                    Acceptance::Predicated { reason: AcceptReason::Conflict, .. }));
                dfas.add_edge(DECISION, Some(start), token.number(), Some(Candidate::Draft(draft)), configs, contexts)?;
                Ok(())
            })?;

            let blockers = (exhaust(retained), exhaust(memory));
            assert_eq!(session.adaptive_predict(DECISION, PredictionMode::Ll)?.number(), 2);
            assert_eq!(session.input_index(), Some(1));
            assert!(session.buffered_token(2).is_none());
            drop(blockers);
            session.unroll_recursion(&mut entry)?;
            Ok(())
        }).unwrap();
    });
}

#[test]
fn all_stop_conflict_prepares_predicates_in_alternative_order_and_accepts_the_unpredicated_default() {
    environment(|env, work, _, _| {
        with_parser_session(env, "READ COMMITTED", work, |session| {
            isolation(session)?;
            session.with_prediction(ISOLATION_DECISION, |round, dfas, _| {
                let (configs, contexts) = round.parts();
                let grammar = contexts.grammar();
                let decision = grammar.decision(ISOLATION_DECISION)?;
                let stop = decision.state().rule().stop().number();
                let low = precedence(configs, grammar, 1)?;
                let high = precedence(configs, grammar, 2)?;
                let unpredicated = Semantic::Unpredicated;
                let empty = contexts.empty();
                for dominates in [false, true] {
                    let set = configs.new_set(SetMode::Local)?;
                    for (alt, semantic) in [(3, low), (2, low), (1, unpredicated), (2, high), (3, unpredicated)] {
                        add(configs, contexts, set, stop, alt, empty, semantic)?;
                    }
                    if dominates { add(configs, contexts, set, stop, 2, empty, unpredicated)?; }
                    let draft = prepare_target(dfas, configs, contexts, decision, set, PredictionMode::Sll)?;
                    let view = configs.view_set(set);
                    assert!(view.has_semantic());
                    assert!(view.unique().is_none());
                    let conflicting = configs.conflict(view.conflicting().unwrap());
                    assert_eq!(conflicting, view.alternatives());
                    assert_eq!(conflicting.count(), 3);
                    match dfas.view_draft(ISOLATION_DECISION, draft)?.acceptance() {
                        Acceptance::Plain { reason: AcceptReason::Conflict, prediction } if dominates => assert_eq!(prediction.number(), 1),
                        Acceptance::Predicated { reason: AcceptReason::Conflict, pairs } if !dominates => {
                            assert!(pairs.iter().map(|pair| (pair.alternative.number(), pair.semantic)).eq([(1, unpredicated), (2, high), (3, unpredicated)]));
                            assert_eq!(evaluate_predicate_pairs(pairs, 2), conflicting);
                            let passing = evaluate_predicate_pairs(pairs, 3);
                            assert_eq!(passing.count(), 2);
                            assert_eq!(passing.first().unwrap().number(), 1);
                            assert!(!passing.contains(pairs[1].alternative));
                        }
                        _ => panic!("prepared conflict"),
                    }
                }
                assert_eq!(dfas.state_count(ISOLATION_DECISION)?, 0);
                Ok(())
            })?;
            session.exit_rule()
        }).unwrap();
    });
}

#[test]
fn predicated_unique_stop_precedes_the_all_stop_conflict_heuristic() {
    environment(|env, work, _, _| {
        with_parser_session(env, "READ", work, |session| {
            isolation(session)?;
            session.with_prediction(ISOLATION_DECISION, |round, dfas, _| {
                let (configs, contexts) = round.parts();
                let grammar = contexts.grammar();
                let decision = grammar.decision(ISOLATION_DECISION)?;
                let stop = decision.state().rule().stop().number();
                let semantic = precedence(configs, grammar, 1)?;
                let set = configs.new_set(SetMode::Local)?;
                let empty = contexts.empty();
                add(configs, contexts, set, stop, 2, empty, semantic)?;
                let draft = prepare_target(dfas, configs, contexts, decision, set, PredictionMode::Sll)?;
                let view = configs.view_set(set);
                let alternative = view.unique().unwrap();
                assert_eq!(alternative.number(), 2);
                assert!(view.conflicting().is_none());
                assert!(matches!(dfas.view_draft(ISOLATION_DECISION, draft)?.acceptance(), Acceptance::Predicated { reason: AcceptReason::Unique, pairs } if pairs == [PredPrediction { semantic, alternative }]));
                Ok(())
            })?;
            session.exit_rule()
        }).unwrap();
    });
}

#[test]
fn sll_target_heuristic_merges_copies_and_prepares_original_semantics_while_ll_keeps_looking() {
    environment(|env, work, _, _| {
        with_parser_session(env, "READ", work, |session| {
            isolation(session)?;
            session.with_prediction(ISOLATION_DECISION, |round, dfas, _| {
                let (configs, contexts) = round.parts();
                let grammar = contexts.grammar();
                let decision = grammar.decision(ISOLATION_DECISION)?;
                let low = precedence(configs, grammar, 1)?;
                let high = precedence(configs, grammar, 2)?;
                let empty = contexts.empty();
                let left = contexts.singleton(empty, grammar.state(356)?)?;
                let right = contexts.singleton(empty, grammar.state(369)?)?;
                let merged = contexts.merge_uncached(left, right, MergeMode::LocalWildcard)?;

                for mode in [PredictionMode::Ll, PredictionMode::Sll] {
                    let set = configs.new_set(SetMode::Local)?;
                    add(configs, contexts, set, 3008, 1, left, low)?;
                    add(configs, contexts, set, 3008, 1, right, high)?;
                    add(configs, contexts, set, 3008, 2, merged, Semantic::Unpredicated)?;
                    let draft = prepare_target(dfas, configs, contexts, decision, set, mode)?;
                    let reach = configs.view_set(set);
                    assert!(reach.unique().is_none());
                    assert_eq!(configs.view(reach.entry(0).unwrap()).context(), left);
                    assert_eq!(configs.view(reach.entry(1).unwrap()).context(), right);
                    let state = dfas.view_draft(ISOLATION_DECISION, draft)?;
                    if mode == PredictionMode::Ll {
                        assert!(matches!(state.acceptance(), Acceptance::NonAccept));
                        assert!(reach.conflicting().is_none());
                    } else {
                        let conflicting = configs.conflict(reach.conflicting().unwrap());
                        assert_eq!(conflicting, reach.alternatives());
                        let Acceptance::Predicated { reason: AcceptReason::Conflict, pairs } = state.acceptance() else { panic!("predicated conflict") };
                        assert!(pairs.iter().map(|pair| (pair.alternative.number(), pair.semantic)).eq([(1, high), (2, Semantic::Unpredicated)]));
                    }
                }
                Ok(())
            })?;
            session.exit_rule()
        }).unwrap();
    });
}

#[test]
fn predication_admission_failure_preserves_unique_metadata_without_publishing_a_dfa_state() {
    environment(|env, work, _, memory| {
        let result = with_parser_session(env, "READ", work, |session| {
            isolation(session)?;
            let result = session.with_prediction(ISOLATION_DECISION, |round, dfas, _| {
                let (configs, contexts) = round.parts();
                let grammar = contexts.grammar();
                let decision = grammar.decision(ISOLATION_DECISION)?;
                let semantic = precedence(configs, grammar, 1)?;
                let set = configs.new_set(SetMode::Local)?;
                let empty = contexts.empty();
                add(configs, contexts, set, 3008, 2, empty, semantic)?;
                let _blocker = exhaust(memory);
                let result = prepare_target(dfas, configs, contexts, decision, set, PredictionMode::Sll);
                assert_eq!(configs.view_set(set).unique().unwrap().number(), 2);
                assert_eq!(dfas.state_count(ISOLATION_DECISION)?, 0);
                result.map(|_| ()).map_err(RuntimeError::Prediction)
            });
            assert_eq!(session.input_index(), Some(0));
            session.exit_rule()?;
            result
        });
        assert!(matches!(result, Err(RuntimeError::Prediction(PredictionError::Admission(error))) if error.code == "rtdb/memory-budget-exceeded"));
    });
}
