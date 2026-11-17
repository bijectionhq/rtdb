use std::sync::Arc;
use rtdb_types::{MemoryBudget, MemoryReservation};
use crate::lexer::{ScanError, StreamError};
use crate::parser::grammar::{OwnedGrammar, TransitionView};
use super::*;
use super::super::{CapturedClassPolicy, ParserState};
use super::super::super::PredictionStorage;
use super::super::super::closure::{ClosureOptions, LoopEntryOption, SourceAssertions};

const MALFORMED_LEVEL: &str = "ISOLATION LEVEL READ read @";
const DECISION: u16 = 371;

fn fixture<'sql, T>(
    sql: &'sql str,
    mut run: impl for<'parse, 'env, 'work> FnMut(
        &mut ParserSession<'parse, 'sql, 'env, 'work>, PredictionMode, &Arc<MemoryBudget>, &Arc<MemoryBudget>,
    ) -> PassResult<'parse, 'sql, T>,
) -> (RuntimeResult<'sql, T>, Arc<MemoryBudget>) {
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
    let result = with_bail_fallback(&mut environment, sql, &work, |session, mode| run(session, mode, &memory, &retained));
    drop(environment);
    drop(work);
    assert_eq!(grammar_memory.used_bytes(), 0);
    assert_eq!(retained.used_bytes(), 0);
    (result, memory)
}

fn enter_isolation<'parse, 'sql>(
    session: &mut ParserSession<'parse, 'sql, '_, '_>,
) -> RuntimeResult<'sql, [InvocationKey<'parse>; 3]> {
    let characteristics = session.new_invocation(163)?;
    session.enter_rule(characteristics)?;
    session.set_state(2872)?;
    let mode = session.new_invocation(164)?;
    session.enter_rule(mode)?;
    session.set_state(2882)?;
    assert_eq!(session.lookahead(1)?.unwrap().lexeme().raw(), "ISOLATION");
    session.advance_input()?;
    session.set_state(2883)?;
    assert_eq!(session.lookahead(1)?.unwrap().lexeme().raw(), "LEVEL");
    session.advance_input()?;
    session.set_state(2884)?;
    let level = session.new_invocation(175)?;
    session.enter_rule(level)?;
    session.set_state(3014)?;
    Ok([characteristics, mode, level])
}

fn exit_isolation<'sql>(session: &mut ParserSession<'_, 'sql, '_, '_>) -> RuntimeResult<'sql, ()> {
    session.exit_rule()?;
    session.exit_rule()?;
    session.exit_rule()
}

fn catch_failed_prediction<'parse, 'sql>(
    session: &mut ParserSession<'parse, 'sql, '_, '_>,
    local: InvocationKey<'parse>,
    mode: PredictionMode,
) -> PassResult<'parse, 'sql, ()> {
    match session.adaptive_predict(DECISION, mode) {
        Err(RuntimeError::NoViableAlternative(_)) => session.catch_no_viable_alternative(local),
        Err(error) => Err(error.into()),
        Ok(_) => panic!("expected the level-of-isolation decision to fail"),
    }
}

fn exhaust(memory: &Arc<MemoryBudget>) -> Arc<MemoryReservation> {
    memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap()
}

fn assert_shared_exception<'parse>(
    session: &ParserSession<'parse, '_, '_, '_>,
    frames: [InvocationKey<'parse>; 3],
) {
    let [root, parent, local] = frames;
    let view = session.invocation_view();
    let original = view.exception(local).unwrap();
    let InvocationCause::NoViableAlternative(cause) = original else { panic!("expected no-viable cause"); };
    assert_eq!(cause.state, ParserState::State(3014));
    assert_eq!((cause.start.index(), cause.offending.index()), (2, 3));
    assert_eq!((cause.start.lexeme().raw(), cause.offending.lexeme().raw()), ("READ", "read"));
    assert!(std::ptr::eq(original, view.exception(parent).unwrap()));
    assert!(std::ptr::eq(original, view.exception(root).unwrap()));
}

#[test]
fn bail_stamps_one_exception_across_real_parents_and_retries_the_same_action_on_the_same_owners() {
    let mut calls = 0;
    let mut owner_addresses = None;
    let mut blocker = None;
    let mut retained_after_sll = None;
    let (result, memory) = fixture(MALFORMED_LEVEL, |session, mode, memory, retained| {
        calls += 1;
        let addresses = (
            std::ptr::from_ref(&*session).cast::<()>() as usize,
            std::ptr::from_ref(&session.input).cast::<()>() as usize,
            std::ptr::from_ref(&*session.environment).cast::<()>() as usize,
        );
        if mode == PredictionMode::Sll {
            assert_eq!(calls, 1);
            assert_eq!(session.input_index(), None);
            session.set_state(2872)?;
            owner_addresses = Some(addresses);
        } else {
            assert_eq!(calls, 2);
            assert_eq!(owner_addresses, Some(addresses));
            assert_eq!(retained_after_sll, Some(retained.used_bytes()));
            assert_eq!(session.input_index(), Some(0));
            assert_eq!(session.buffered_token(0).unwrap().lexeme().raw(), "ISOLATION");
            assert_eq!(session.buffered_token(3).unwrap().lexeme().raw(), "read");
            assert!(session.buffered_token(4).is_none());
            assert!(session.control.is_ready());
            assert!(session.profile == ParserProfile::LlDefault);
            assert_eq!(session.syntax_errors, 0);
            assert!(!session.matched_eof);
            assert_eq!(session.precedence(), 0);
            assert!(session.current().is_none());
            assert_eq!(session.state(), ParserState::State(2872));
            assert_eq!(session.records.len(), 3);
            assert_shared_exception(session, [0, 1, 2].map(InvocationKey::at));
            assert_eq!(memory.used_bytes(), memory.limit_bytes());
            drop(blocker.take().unwrap());
        }

        let frames = enter_isolation(session)?;
        let [root, parent, local] = frames;
        let pass = catch_failed_prediction(session, local, mode);
        assert_eq!(session.current(), Some(local));
        assert_eq!(session.input_index(), Some(2));
        assert!(session.buffered_token(4).is_none());
        assert_eq!(session.syntax_errors, 1);
        if mode == PredictionMode::Sll {
            let Err(PassError::Cancelled(cancellation)) = &pass else { panic!("expected Bail cancellation"); };
            assert_eq!(cancellation.exception, local);
            assert_shared_exception(session, frames);
        } else {
            assert!(matches!(&pass, Err(PassError::Runtime(RuntimeError::ReportedNoViableAlternative(_)))));
            let view = session.invocation_view();
            let Some(InvocationCause::NoViableAlternative(caught)) = view.exception(local) else { panic!("expected no-viable cause"); };
            assert_eq!(caught.offending.index(), 3);
            assert!(view.exception(parent).is_none());
            assert!(view.exception(root).is_none());
        }
        exit_isolation(session)?;
        if mode == PredictionMode::Sll {
            assert_eq!(session.state(), ParserState::State(2872));
            retained_after_sll = Some(retained.used_bytes());
            blocker = Some(exhaust(memory));
        }
        pass
    });

    assert_eq!(calls, 2);
    let Err(RuntimeError::ReportedNoViableAlternative(error)) = result else { panic!("expected LL diagnostic"); };
    assert_eq!(error.anomaly().message.as_deref(), Some(
        "Errors parsing SQL statement:\n  - line 1:21 no viable alternative at input 'READread'",
    ));
    drop(error);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn report_admission_failure_preserves_the_catch_assignment_and_prevents_bail() {
    let mut calls = 0;
    let (result, memory) = fixture(MALFORMED_LEVEL, |session, mode, memory, _| {
        calls += 1;
        let [root, parent, local] = enter_isolation(session)?;
        assert!(matches!(session.adaptive_predict(DECISION, mode), Err(RuntimeError::NoViableAlternative(_))));
        let _blocker = exhaust(memory);
        let pass: PassResult<'_, '_, ()> = session.catch_no_viable_alternative(local);
        assert!(matches!(&pass, Err(PassError::Runtime(RuntimeError::Input(
            StreamError::Scan(ScanError::Admission(error)),
        ))) if error.code == "rtdb/memory-budget-exceeded"));
        assert!(matches!(session.control, ParserControl::InErrorRecovery));
        assert_eq!(session.syntax_errors, 0);
        let view = session.invocation_view();
        let Some(InvocationCause::NoViableAlternative(cause)) = view.exception(local) else { panic!("expected no-viable cause"); };
        assert_eq!(cause.offending.index(), 3);
        assert!(view.exception(parent).is_none());
        assert!(view.exception(root).is_none());
        exit_isolation(session)?;
        pass
    });
    assert_eq!(calls, 1);
    assert!(matches!(result, Err(RuntimeError::Input(StreamError::Scan(ScanError::Admission(_))))));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn lexer_failure_crosses_real_finally_without_a_catch_assignment_or_retry() {
    let mut calls = 0;
    let (result, memory) = fixture("ISOLATION LEVEL READ @", |session, mode, _, _| {
        calls += 1;
        let frames = enter_isolation(session)?;
        let pass = catch_failed_prediction(session, frames[2], mode);
        assert!(matches!(&pass, Err(PassError::Runtime(RuntimeError::Input(
            StreamError::Scan(ScanError::Recognition(_)),
        )))));
        assert_eq!(session.syntax_errors, 0);
        assert!(frames.into_iter().all(|frame| session.invocation_view().exception(frame).is_none()));
        exit_isolation(session)?;
        pass
    });
    assert_eq!(calls, 1);
    assert!(matches!(result, Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_))))));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn the_catch_rejects_a_different_local_invocation_before_writing_or_reporting() {
    let mut calls = 0;
    let (result, memory) = fixture(MALFORMED_LEVEL, |session, mode, memory, _| {
        calls += 1;
        let frames = enter_isolation(session)?;
        assert!(matches!(session.adaptive_predict(DECISION, mode), Err(RuntimeError::NoViableAlternative(_))));
        let used = memory.used_bytes();
        let pass: PassResult<'_, '_, ()> = session.catch_no_viable_alternative(frames[1]);
        assert!(matches!(&pass, Err(PassError::Runtime(RuntimeError::Control(RuntimeProblem::Aborted)))));
        assert!(frames.into_iter().all(|frame| session.invocation_view().exception(frame).is_none()));
        assert_eq!(session.syntax_errors, 0);
        assert_eq!(memory.used_bytes(), used);
        exit_isolation(session)?;
        pass
    });
    assert_eq!(calls, 1);
    assert!(matches!(result, Err(RuntimeError::Control(RuntimeProblem::Aborted))));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn cancellation_cannot_reset_before_generated_finally_or_through_generic_cleanup() {
    for attempt_generic_cleanup in [false, true] {
        let mut calls = 0;
        let (result, memory) = fixture(MALFORMED_LEVEL, |session, mode, memory, _| {
            calls += 1;
            let frames = enter_isolation(session)?;
            let pass = catch_failed_prediction(session, frames[2], mode);
            assert!(matches!(&pass, Err(PassError::Cancelled(_))));
            assert_eq!(session.current(), Some(frames[2]));
            if attempt_generic_cleanup {
                let used = memory.used_bytes();
                assert!(matches!(session.clear_rule_control(), Err(RuntimeError::Control(RuntimeProblem::Aborted))));
                assert_eq!(session.current(), Some(frames[2]));
                assert_eq!(session.input_index(), Some(2));
                assert_eq!(memory.used_bytes(), used);
                exit_isolation(session)?;
            }
            pass
        });
        assert_eq!(calls, 1);
        assert!(matches!(result, Err(RuntimeError::Control(RuntimeProblem::Aborted))));
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn isolated_prediction_kernel_cannot_mint_a_bail_cancellation() {
    let memory = MemoryBudget::new(64 << 20);
    let work = AllocationScope::new(&memory).unwrap();
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
    let policy = CapturedClassPolicy::from_captured(ClosureOptions {
        pruning: LoopEntryOption::Enabled,
        assertions: SourceAssertions::Enabled,
    });
    let mut environment = ParserEnvironment::new(PredictionStorage::new(grammar, &memory), policy, &work).unwrap();
    with_parser_session(&mut environment, MALFORMED_LEVEL, &work, |session| {
        let frames = enter_isolation(session)?;
        assert!(matches!(session.adaptive_predict(DECISION, PredictionMode::Sll), Err(RuntimeError::NoViableAlternative(_))));
        let used = memory.used_bytes();
        let pass: PassResult<'_, '_, ()> = session.catch_no_viable_alternative(frames[2]);
        assert!(matches!(pass, Err(PassError::Runtime(RuntimeError::Control(RuntimeProblem::Aborted)))));
        assert!(session.invocation_view().exception(frames[2]).is_none());
        assert_eq!(session.syntax_errors, 0);
        assert_eq!(memory.used_bytes(), used);
        exit_isolation(session)
    }).unwrap();
    drop(environment);
    drop(work);
    assert_eq!(grammar_memory.used_bytes(), 0);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn wrapper_profile_rejects_the_wrong_mode_before_prediction_or_a_retry() {
    let mut calls = 0;
    let (result, memory) = fixture(MALFORMED_LEVEL, |session, mode, memory, _| {
        calls += 1;
        assert_eq!(mode, PredictionMode::Sll);
        let frames = enter_isolation(session)?;
        let used = memory.used_bytes();
        let result = session.adaptive_predict(DECISION, PredictionMode::Ll);
        assert!(matches!(result, Err(RuntimeError::Control(RuntimeProblem::PredictionSite))));
        assert!(session.buffered_token(3).is_none());
        assert!(session.invocation_view().exception(frames[2]).is_none());
        assert_eq!(memory.used_bytes(), used);
        exit_isolation(session)?;
        Ok(result?)
    });
    assert_eq!(calls, 1);
    assert!(matches!(result, Err(RuntimeError::Control(RuntimeProblem::PredictionSite))));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn a_successful_sll_action_returns_without_reset_or_ll() {
    let mut calls = 0;
    let (result, memory) = fixture("ISOLATION LEVEL READ COMMITTED", |session, mode, _, _| {
        calls += 1;
        assert_eq!(mode, PredictionMode::Sll);
        enter_isolation(session)?;
        let alternative = session.adaptive_predict(DECISION, mode)?;
        assert_eq!(alternative.number(), 2);
        session.set_state(3009)?;
        session.advance_input()?;
        session.set_state(3010)?;
        session.advance_input()?;
        exit_isolation(session)?;
        Ok(alternative)
    });
    assert_eq!(result.unwrap().number(), 2);
    assert_eq!(calls, 1);
    assert_eq!(memory.used_bytes(), 0);
}

fn match_at_state<'parse, 'sql>(
    session: &mut ParserSession<'parse, 'sql, '_, '_>,
    state: u32,
) -> PassResult<'parse, 'sql, IndexedLexeme<'sql>> {
    session.set_state(state)?;
    let token = {
        let grammar = session.environment.storage.grammar_view();
        let state = grammar.state(state).map_err(RuntimeError::from)?;
        let Some(TransitionView::Atom { token, .. }) = state.transitions().next() else { panic!("generated token match"); };
        token
    };
    session.match_bail(token)
}

#[test]
fn repeatable_predicts_before_demanding_the_token_that_bails_inline() {
    for (source, spelling) in [
        ("ISOLATION LEVEL RePeAtAbLe wrong @", Some("wrong")),
        ("ISOLATION LEVEL RePeAtAbLe", None),
    ] {
        let mut calls = 0;
        let (result, memory) = fixture(source, |session, mode, _, _| {
            calls += 1;
            if mode == PredictionMode::Ll {
                assert_eq!(session.input_index(), Some(0));
                assert!(session.current().is_none());
                assert!(session.control.is_ready());
                assert!(session.profile == ParserProfile::LlDefault);
                assert_eq!(session.syntax_errors, 0);
                assert!(session.buffered_token(3).is_some());
                assert!(session.buffered_token(4).is_none());
                return Ok(());
            }

            let [root, parent, local] = enter_isolation(session)?;
            assert_eq!(session.adaptive_predict(DECISION, mode)?.number(), 3);
            assert_eq!(session.input_index(), Some(2));
            assert!(session.buffered_token(3).is_none());
            let matched = match_at_state(session, 3011)?;
            assert_eq!((matched.index(), matched.lexeme().raw()), (2, "RePeAtAbLe"));
            let pass = match_at_state(session, 3012);
            let Err(PassError::Cancelled(cancellation)) = &pass else { panic!("expected inline Bail cancellation"); };
            assert_eq!(cancellation.exception, local);
            assert!(matches!(&session.control, ParserControl::BailCancelled { exception, recovery: BailRecovery::Clear } if *exception == local));
            assert_eq!(session.syntax_errors, 0);
            assert!(!session.matched_eof);
            assert!(session.pending_no_viable_report().is_none());
            assert_eq!(session.input_index(), Some(3));
            assert_eq!(session.current(), Some(local));
            assert!(session.buffered_token(4).is_none());

            assert!(matches!(&session.records[local.index].exception, Some(InvocationException::Owned(InvocationCause::InputMismatch { .. }))));
            let view = session.invocation_view();
            let cause = view.exception(local).unwrap();
            let InvocationCause::InputMismatch { state, offending } = cause else { panic!("expected inline mismatch cause"); };
            assert_eq!(*state, ParserState::State(3012));
            assert_eq!(offending.index(), 3);
            match spelling {
                Some(spelling) => assert_eq!(offending.lexeme().raw(), spelling),
                None => assert_eq!(offending.lexeme().kind(), Kind::Eof),
            }
            assert!(std::ptr::eq(cause, view.exception(parent).unwrap()));
            assert!(std::ptr::eq(cause, view.exception(root).unwrap()));
            exit_isolation(session)?;
            assert_eq!(session.records[local.index].stop.unwrap().index(), 2);
            pass.map(|_| ())
        });
        result.unwrap();
        assert_eq!(calls, 2);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn successful_bail_matches_return_actual_occurrences_without_retry() {
    let mut calls = 0;
    let (result, memory) = fixture("ISOLATION LEVEL RePeAtAbLe ReAd", |session, mode, _, _| {
        calls += 1;
        let [_, _, local] = enter_isolation(session)?;
        assert_eq!(session.adaptive_predict(DECISION, mode)?.number(), 3);
        let first = match_at_state(session, 3011)?;
        let second = match_at_state(session, 3012)?;
        assert_eq!((first.index(), first.lexeme().raw()), (2, "RePeAtAbLe"));
        assert_eq!((second.index(), second.lexeme().raw()), (3, "ReAd"));
        assert_eq!(session.input_index(), Some(4));
        assert_eq!(session.buffered_token(4).unwrap().lexeme().kind(), Kind::Eof);
        assert!(!session.matched_eof);
        assert!(session.invocation_view().exception(local).is_none());
        exit_isolation(session)?;
        assert_eq!(session.records[local.index].stop.unwrap().index(), 3);
        Ok(())
    });
    result.unwrap();
    assert_eq!(calls, 1);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn generated_eof_match_keeps_the_buffered_occurrence_for_finally() {
    let mut calls = 0;
    let (result, memory) = fixture("SELECT 1", |session, _, _, _| {
        calls += 1;
        let local = session.new_invocation(0)?;
        session.enter_rule(local)?;
        session.advance_input()?;
        session.advance_input()?;
        let matched = match_at_state(session, 356)?;
        assert_eq!((matched.index(), matched.lexeme().kind()), (2, Kind::Eof));
        assert!(session.matched_eof);
        assert_eq!(session.input_index(), Some(2));
        assert!(session.buffered_token(3).is_none());
        session.exit_rule()?;
        assert_eq!(session.records[local.index].stop.unwrap().index(), 2);
        Ok(())
    });
    result.unwrap();
    assert_eq!(calls, 1);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn lexer_failure_during_successful_match_consume_cannot_bail_inline() {
    let mut calls = 0;
    let (result, memory) = fixture("ISOLATION LEVEL REPEATABLE @", |session, mode, _, _| {
        calls += 1;
        let frames = enter_isolation(session)?;
        assert_eq!(session.adaptive_predict(DECISION, mode)?.number(), 3);
        assert!(session.buffered_token(3).is_none());
        let pass = match_at_state(session, 3011);
        assert!(matches!(pass, Err(PassError::Runtime(RuntimeError::Input(
            StreamError::Scan(ScanError::Recognition(_)),
        )))));
        assert_eq!(session.state(), ParserState::State(3011));
        assert_eq!(session.input_index(), Some(2));
        assert!(session.buffered_token(3).is_none());
        assert!(matches!(session.control, ParserControl::Failed));
        assert_eq!(session.syntax_errors, 0);
        assert!(frames.into_iter().all(|frame| session.invocation_view().exception(frame).is_none()));
        exit_isolation(session)?;
        pass.map(|_| ())
    });
    assert!(matches!(result, Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_))))));
    assert_eq!(calls, 1);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn token_buffer_admission_failure_during_match_consume_cannot_bail_inline() {
    let source = format!("{}REPEATABLE READ", "before ".repeat(99));
    let mut calls = 0;
    let (result, memory) = fixture(&source, |session, mode, memory, _| {
        calls += 1;
        let local = session.new_invocation(175)?;
        session.enter_rule(local)?;
        for _ in 0..99 {
            session.advance_input()?;
        }
        session.set_state(3014)?;
        assert_eq!(session.adaptive_predict(DECISION, mode)?.number(), 3);
        assert_eq!(session.adaptive_predict(DECISION, mode)?.number(), 3);
        assert_eq!(session.input_index(), Some(99));
        assert!(session.buffered_token(100).is_none());
        let _blocker = exhaust(memory);
        let pass = match_at_state(session, 3011);
        assert!(matches!(&pass, Err(PassError::Runtime(RuntimeError::Input(
            StreamError::Scan(ScanError::Admission(error)),
        ))) if error.code == "rtdb/memory-budget-exceeded"));
        assert_eq!(session.state(), ParserState::State(3011));
        assert_eq!(session.input_index(), Some(99));
        assert!(session.buffered_token(100).is_none());
        assert!(matches!(session.control, ParserControl::Failed));
        assert_eq!(session.syntax_errors, 0);
        assert!(session.invocation_view().exception(local).is_none());
        session.exit_rule()?;
        pass.map(|_| ())
    });
    assert!(matches!(result, Err(RuntimeError::Input(StreamError::Scan(ScanError::Admission(_))))));
    assert_eq!(calls, 1);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn denied_normal_use_preserves_the_recovery_state_of_each_cancelled_path() {
    for (source, inline) in [
        ("ISOLATION LEVEL REPEATABLE wrong", true),
        (MALFORMED_LEVEL, false),
    ] {
        let mut calls = 0;
        let (result, memory) = fixture(source, |session, mode, memory, _| {
            calls += 1;
            let [_, _, local] = enter_isolation(session)?;
            let pass = if inline {
                assert_eq!(session.adaptive_predict(DECISION, mode)?.number(), 3);
                match_at_state(session, 3011)?;
                match_at_state(session, 3012).map(|_| ())
            } else {
                catch_failed_prediction(session, local, mode)
            };
            assert!(matches!(pass, Err(PassError::Cancelled(_))));
            let used = memory.used_bytes();
            assert!(matches!(session.lookahead(1), Err(RuntimeError::Control(RuntimeProblem::Aborted))));
            assert!(if inline { matches!(session.control, ParserControl::Failed) }
                else { matches!(session.control, ParserControl::InErrorRecovery) });
            assert_eq!(session.syntax_errors, if inline { 0 } else { 1 });
            assert_eq!(memory.used_bytes(), used);
            exit_isolation(session)?;
            pass
        });
        assert!(matches!(result, Err(RuntimeError::Control(RuntimeProblem::Aborted))));
        assert_eq!(calls, 1);
        assert_eq!(memory.used_bytes(), 0);
    }
}
