use super::*;
use crate::parser::prediction::{PredictionError, runtime::TokenDiagnostic};

fn enter_common_isolation<'parse, 'sql>(
    session: &mut ParserSession<'parse, 'sql, '_, '_>,
) -> RuntimeResult<'sql, [InvocationKey<'parse>; 2]> {
    let parent = session.new_invocation(164)?;
    session.enter_rule(parent)?;
    session.set_state(2886)?;
    session.sync_required_block()?;
    assert_eq!(session.lookahead(1)?.unwrap().lexeme().raw(), "ISOLATION");
    session.set_state(2882)?;
    session.advance_input()?;
    session.set_state(2883)?;
    assert_eq!(session.lookahead(1)?.unwrap().lexeme().raw(), "LEVEL");
    session.advance_input()?;
    session.set_state(2884)?;
    let local = session.new_invocation(175)?;
    session.enter_rule(local)?;
    session.set_state(3014)?;
    Ok([parent, local])
}

fn predict_isolation<'parse, 'sql>(
    session: &mut ParserSession<'parse, 'sql, '_, '_>,
    local: InvocationKey<'parse>,
    mode: PredictionMode,
) -> PassResult<'parse, 'sql, ()> {
    let result = session.sync_required_block()
        .and_then(|()| session.adaptive_predict(DECISION, mode));
    match result {
        Err(RuntimeError::NoViableAlternative(_) | RuntimeError::InputMismatch(_)) => session.catch_recognition(local),
        result => result.map(|_| ()).map_err(PassError::from),
    }
}

fn finish_common_isolation<'sql>(session: &mut ParserSession<'_, 'sql, '_, '_>) -> RuntimeResult<'sql, ()> {
    session.exit_rule()?;
    session.exit_rule()
}

fn diagnostic<'sql, T>(result: RuntimeResult<'sql, T>) -> TokenDiagnostic<'sql> {
    let Err(RuntimeError::ReportedTokenSyntax(error)) = result else { panic!("expected LL token diagnostic"); };
    error
}

fn leave_free(memory: &Arc<MemoryBudget>, bytes: usize) -> Arc<MemoryReservation> {
    memory.reserve(memory.limit_bytes() - memory.used_bytes() - bytes).unwrap()
}

#[test]
fn common_mode_and_isolation_sync_accept_the_first_tokens_before_the_real_ll_no_viable() {
    let mut calls = 0;
    let (result, memory) = fixture(MALFORMED_LEVEL, |session, mode, _, _| {
        calls += 1;
        let [parent, local] = enter_common_isolation(session)?;
        let result = predict_isolation(session, local, mode);
        assert_eq!(session.syntax_errors, 1);
        assert_eq!(session.input_index(), Some(2));
        assert!(session.buffered_token(4).is_none());
        let view = session.invocation_view();
        let Some(InvocationCause::NoViableAlternative(cause)) = view.exception(local) else { panic!("expected adaptive error"); };
        assert_eq!(cause.state, ParserState::State(3014));
        assert_eq!((cause.start.index(), cause.offending.index()), (2, 3));
        assert_eq!(cause.offending.lexeme().raw(), "read");
        match mode {
            PredictionMode::Sll => {
                assert!(matches!(&result, Err(PassError::Cancelled(_))));
                assert!(std::ptr::eq(view.exception(local).unwrap(), view.exception(parent).unwrap()));
            }
            PredictionMode::Ll => {
                assert!(matches!(&result, Err(PassError::Runtime(RuntimeError::ReportedNoViableAlternative(_)))));
                assert!(view.exception(parent).is_none());
            }
        }
        finish_common_isolation(session)?;
        result
    });
    assert_eq!(calls, 2);
    let Err(RuntimeError::ReportedNoViableAlternative(error)) = result else { panic!("expected LL adaptive report"); };
    assert_eq!(error.anomaly().message.as_deref(), Some(
        "Errors parsing SQL statement:\n  - line 1:21 no viable alternative at input 'READread'",
    ));
    drop(error);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn unwanted_token_report_has_no_cause_and_stops_before_deletion() {
    let mut calls = 0;
    let (result, memory) = fixture("ISOLATION LEVEL wrong READ @", |session, mode, _, _| {
        calls += 1;
        let [parent, local] = enter_common_isolation(session)?;
        let result = predict_isolation(session, local, mode);
        assert_eq!(session.syntax_errors, 1);
        assert_eq!(session.input_index(), Some(2));
        assert!(session.buffered_token(4).is_none());
        if mode == PredictionMode::Ll {
            assert_eq!(session.buffered_token(3).unwrap().lexeme().raw(), "READ");
            assert!(matches!(session.control, ParserControl::InErrorRecovery));
            assert!(matches!(&result, Err(PassError::Runtime(RuntimeError::ReportedTokenSyntax(_)))));
            let view = session.invocation_view();
            assert!(view.exception(local).is_none());
            assert!(view.exception(parent).is_none());
        } else {
            assert!(session.buffered_token(3).is_none());
            assert!(matches!(&result, Err(PassError::Cancelled(_))));
        }
        finish_common_isolation(session)?;
        result
    });
    assert_eq!(calls, 2);
    let error = diagnostic(result);
    assert!(error.cause().is_none());
    assert_eq!((error.offending().index(), error.offending().lexeme().raw()), (2, "wrong"));
    let message = "Errors parsing SQL statement:\n  - line 1:16 extraneous input 'wrong' expecting {'READ', 'REPEATABLE', 'SERIALIZABLE'}";
    assert_eq!(error.anomaly().message.as_deref(), Some(message));
    assert_eq!(error.anomaly().code, "xtdb/sql-error");
    assert_eq!(memory.used_bytes(), message.len() + "xtdb/sql-error".len() + 256);
    drop(error);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sync_mismatch_is_caught_in_the_current_rule_and_reports_its_original_cause() {
    for (source, spelling, eof, offending_index) in [
        ("ISOLATION LEVEL wrong", "wrong", false, 2),
        ("ISOLATION LEVEL ", "<EOF>", true, 2),
    ] {
        let mut calls = 0;
        let (result, memory) = fixture(source, |session, mode, _, _| {
            calls += 1;
            let [parent, local] = enter_common_isolation(session)?;
            if mode == PredictionMode::Sll {
                let result = predict_isolation(session, local, mode);
                finish_common_isolation(session)?;
                return result;
            }
            let Err(RuntimeError::InputMismatch(cause)) = session.sync_required_block() else { panic!("expected ordinary sync mismatch"); };
            assert_eq!(cause.state, ParserState::State(3014));
            assert_eq!(cause.offending.index(), offending_index);
            assert_eq!(cause.offending.lexeme().kind() == Kind::Eof, eof);
            assert_eq!(session.input_index(), Some(offending_index));
            assert_eq!(session.buffered_token(if eof { 2 } else { 3 }).unwrap().lexeme().kind(), Kind::Eof);
            assert_eq!(session.syntax_errors, 0);
            assert!(matches!(session.control, ParserControl::PendingInputMismatch(_)));
            assert!(session.invocation_view().exception(local).is_none());

            let result = session.catch_recognition(local);
            assert_eq!(session.syntax_errors, 1);
            assert!(matches!(session.control, ParserControl::InErrorRecovery));
            let view = session.invocation_view();
            let Some(InvocationCause::InputMismatch(stored)) = view.exception(local) else { panic!("expected catch assignment"); };
            assert_eq!(stored.state, cause.state);
            assert_eq!(stored.offending.index(), cause.offending.index());
            assert!(view.exception(parent).is_none());
            finish_common_isolation(session)?;
            result
        });
        assert_eq!(calls, 2);
        let error = diagnostic(result);
        let cause = error.cause().unwrap();
        assert_eq!(cause.state, ParserState::State(3014));
        assert_eq!(cause.offending.index(), offending_index);
        let message = format!("Errors parsing SQL statement:\n  - line 1:16 mismatched input '{spelling}' expecting {{'READ', 'REPEATABLE', 'SERIALIZABLE'}}");
        assert_eq!(error.anomaly().message.as_deref(), Some(message.as_str()));
        drop(error);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn real_second_lookahead_lexer_failure_preempts_expected_token_reporting() {
    let mut calls = 0;
    let (result, memory) = fixture("ISOLATION LEVEL wrong @", |session, mode, _, _| {
        calls += 1;
        let frames = enter_common_isolation(session)?;
        let result = predict_isolation(session, frames[1], mode);
        if mode == PredictionMode::Ll {
            assert!(matches!(&result, Err(PassError::Runtime(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_)))))));
            assert_eq!(session.syntax_errors, 0);
            assert!(matches!(session.control, ParserControl::Failed));
            assert!(frames.into_iter().all(|frame| session.invocation_view().exception(frame).is_none()));
        } else {
            assert!(matches!(&result, Err(PassError::Cancelled(_))));
        }
        assert_eq!(session.input_index(), Some(2));
        assert!(session.buffered_token(3).is_none());
        finish_common_isolation(session)?;
        result
    });
    assert_eq!(calls, 2);
    let Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(error)))) = result else { panic!("expected actual lexer error"); };
    assert_eq!(error.span().start.column, 22);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn real_second_lookahead_token_admission_preempts_mismatch_and_report() {
    let source = format!("{}wrong READ", "before ".repeat(99));
    let mut calls = 0;
    let (result, memory) = fixture(&source, |session, mode, memory, _| {
        calls += 1;
        let local = session.new_invocation(175)?;
        session.enter_rule(local)?;
        for _ in 0..99 {
            session.advance_input()?;
        }
        session.set_state(3014)?;
        if mode == PredictionMode::Sll {
            let result = predict_isolation(session, local, mode);
            assert!(matches!(&result, Err(PassError::Cancelled(_))));
            assert!(session.buffered_token(100).is_none());
            session.exit_rule()?;
            return result;
        }

        let probe = AllocationScope::new(memory).unwrap();
        let before = memory.used_bytes();
        {
            let grammar = session.environment.storage.grammar_view();
            grammar.state(3014).unwrap().next_tokens(&probe).unwrap();
        }
        let look_bytes = memory.used_bytes() - before;
        drop(probe);

        let _blocker = leave_free(memory, look_bytes);
        let result = session.sync_required_block();
        assert_eq!(memory.used_bytes(), memory.limit_bytes());
        assert!(matches!(&result, Err(RuntimeError::Input(StreamError::Scan(ScanError::Admission(error))))
            if error.code == "rtdb/memory-budget-exceeded"));
        assert_eq!(session.input_index(), Some(99));
        assert!(session.buffered_token(100).is_none());
        assert!(matches!(session.control, ParserControl::Failed));
        assert_eq!(session.syntax_errors, 0);
        assert!(session.invocation_view().exception(local).is_none());
        session.exit_rule()?;
        result.map_err(PassError::from)
    });
    assert_eq!(calls, 2);
    assert!(matches!(result, Err(RuntimeError::Input(StreamError::Scan(ScanError::Admission(_))))));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn mismatch_report_admissions_bracket_notification_count_and_keep_local_assignment() {
    let message = "mismatched input 'wrong' expecting {'READ', 'REPEATABLE', 'SERIALIZABLE'}";
    for (available, count) in [(0, 0), (message.len() + 256, 1)] {
        let mut calls = 0;
        let (result, memory) = fixture("ISOLATION LEVEL wrong", |session, mode, memory, _| {
            calls += 1;
            let [parent, local] = enter_common_isolation(session)?;
            if mode == PredictionMode::Sll {
                let result = predict_isolation(session, local, mode);
                finish_common_isolation(session)?;
                return result;
            }
            assert!(matches!(session.sync_required_block(), Err(RuntimeError::InputMismatch(_))));
            let _blocker = leave_free(memory, available);
            let result = session.catch_recognition(local);
            assert_eq!(session.syntax_errors, count);
            assert!(matches!(session.control, ParserControl::InErrorRecovery));
            assert!(matches!(&result, Err(PassError::Runtime(RuntimeError::Prediction(PredictionError::Admission(error))))
                if error.code == "rtdb/memory-budget-exceeded"));
            let view = session.invocation_view();
            assert!(matches!(view.exception(local), Some(InvocationCause::InputMismatch(_))));
            assert!(view.exception(parent).is_none());
            finish_common_isolation(session)?;
            result
        });
        assert_eq!(calls, 2);
        assert!(matches!(result, Err(RuntimeError::Prediction(PredictionError::Admission(_)))));
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn ll_sync_rejects_a_different_current_atn_state_without_new_demand() {
    let mut calls = 0;
    let (result, memory) = fixture("ISOLATION LEVEL wrong @", |session, mode, memory, _| {
        calls += 1;
        let [_, local] = enter_common_isolation(session)?;
        if mode == PredictionMode::Sll {
            let result = predict_isolation(session, local, mode);
            finish_common_isolation(session)?;
            return result;
        }
        session.set_state(3009)?;
        let used = memory.used_bytes();
        let result = session.sync_required_block();
        assert!(matches!(result, Err(RuntimeError::Control(RuntimeProblem::PredictionSite))));
        assert_eq!(session.syntax_errors, 0);
        assert!(session.buffered_token(3).is_none());
        assert_eq!(memory.used_bytes(), used);
        finish_common_isolation(session)?;
        result.map_err(PassError::from)
    });
    assert!(matches!(result, Err(RuntimeError::Control(RuntimeProblem::PredictionSite))));
    assert_eq!(calls, 2);
    assert_eq!(memory.used_bytes(), 0);
}
