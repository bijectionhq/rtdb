use std::sync::Arc;
use rtdb_types::{AnomalyCategory, MemoryBudget};
use crate::lexer::{ScanError, StreamError};
use crate::parser::grammar::OwnedGrammar;
use super::*;
use super::super::{CapturedClassPolicy, ParserEnvironment, ParserState, with_parser_session};
use super::super::super::PredictionStorage;
use super::super::super::closure::{ClosureOptions, LoopEntryOption, SourceAssertions};

fn fixture<'sql, R>(
    sql: &'sql str,
    run: impl for<'parse, 'env, 'work> FnOnce(
        &mut ParserSession<'parse, 'sql, 'env, 'work>,
        &Arc<MemoryBudget>,
    ) -> RuntimeResult<'sql, R>,
) -> (RuntimeResult<'sql, R>, Arc<MemoryBudget>) {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
    let cache = MemoryBudget::new(64 << 20);
    let memory = MemoryBudget::new(64 << 20);
    let work = AllocationScope::new(&memory).unwrap();
    let policy = CapturedClassPolicy::from_captured(ClosureOptions {
        pruning: LoopEntryOption::Enabled,
        assertions: SourceAssertions::Enabled,
    });
    let mut environment = ParserEnvironment::new(PredictionStorage::new(grammar, &cache), policy, &work).unwrap();
    let result = with_parser_session(&mut environment, sql, &work, |session| run(session, &memory));
    drop(environment);
    drop(work);
    assert_eq!(grammar_memory.used_bytes(), 0);
    assert_eq!(cache.used_bytes(), 0);
    (result, memory)
}

fn isolation<'sql>(session: &mut ParserSession<'_, 'sql, '_, '_>) -> RuntimeResult<'sql, ()> {
    let invocation = session.new_invocation(175)?;
    session.enter_rule(invocation)?;
    session.set_state(3014)
}

fn fail_isolation<'sql>(
    session: &mut ParserSession<'_, 'sql, '_, '_>,
    mode: PredictionMode,
) -> NoViableAlternative<'sql> {
    let Err(RuntimeError::NoViableAlternative(cause)) = session.adaptive_predict(371, mode) else {
        panic!("level of isolation must fail with a no-viable exception");
    };
    cause
}

fn leave_free(memory: &Arc<MemoryBudget>, bytes: usize) -> Arc<MemoryReservation> {
    memory.reserve(memory.limit_bytes() - memory.used_bytes() - bytes).unwrap()
}

fn report_has_begun(session: &ParserSession<'_, '_, '_, '_>, syntax_errors: i32) {
    assert!(matches!(session.control, ParserControl::InErrorRecovery));
    assert_eq!(session.syntax_errors, syntax_errors);
}

fn aborted<T>(result: RuntimeResult<'_, T>) {
    assert!(matches!(result, Err(RuntimeError::Control(RuntimeProblem::Aborted))));
}

fn reported<'sql, T>(result: RuntimeResult<'sql, T>) -> NoViableDiagnostic<'sql> {
    let Err(RuntimeError::ReportedNoViableAlternative(error)) = result else {
        panic!("expected the throwing listener's diagnostic");
    };
    error
}

#[test]
fn ll_report_uses_captured_token_interval_and_offending_code_point_column_after_rewind() {
    let (result, memory) = fixture("\"🙂\" READ /* ignored */ \"a\r\n\t🙂'\" @", |session, memory| {
        isolation(session)?;
        session.advance_input()?;
        let cause = fail_isolation(session, PredictionMode::Ll);
        assert_eq!(session.input_index(), Some(1));
        assert_eq!((cause.start.index(), cause.offending.index()), (1, 2));

        let result = session.report_no_viable_alternative();
        report_has_begun(session, 1);
        assert_eq!(session.input_index(), Some(1));
        assert!(session.buffered_token(3).is_none());
        let retained = memory.used_bytes();
        aborted(session.lookahead(1));
        aborted(session.report_no_viable_alternative());
        assert_eq!(memory.used_bytes(), retained);
        session.exit_rule()?;
        result
    });

    let error = reported(result);
    let cause = error.cause();
    assert_eq!(cause.start.lexeme().raw(), "READ");
    assert_eq!(cause.offending.lexeme().raw(), "\"a\r\n\t🙂'\"");
    assert_eq!(cause.state, ParserState::State(3014));
    assert_eq!(cause.offending.lexeme().span().start.byte, 26);
    assert_eq!(cause.offending.lexeme().span().start.code_point, 23);
    assert_eq!(error.anomaly().category, AnomalyCategory::Incorrect);
    assert_eq!(error.anomaly().code, "xtdb/sql-error");
    let message = "Errors parsing SQL statement:\n  - line 1:23 no viable alternative at input 'READ\"a\\r\\n\\t🙂'\"'";
    assert_eq!(error.anomaly().message.as_deref(), Some(message));
    assert_eq!(memory.used_bytes(), message.len() + "xtdb/sql-error".len() + 256);
    drop(error);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn ll_report_uses_eof_position_but_omits_eof_from_nonempty_token_text() {
    let (result, memory) = fixture("before READ -- skipped\n  ", |session, _| {
        isolation(session)?;
        session.advance_input()?;
        fail_isolation(session, PredictionMode::Ll);
        let result = session.report_no_viable_alternative();
        report_has_begun(session, 1);
        session.exit_rule()?;
        result
    });
    let error = reported(result);
    let cause = error.cause();
    assert_eq!((cause.start.index(), cause.offending.index()), (1, 2));
    assert_eq!(cause.offending.lexeme().kind(), Kind::Eof);
    assert_eq!(error.anomaly().message.as_deref(), Some(
        "Errors parsing SQL statement:\n  - line 2:2 no viable alternative at input 'READ'",
    ));
    drop(error);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn eof_start_reports_the_eof_literal_without_an_interval_allocation() {
    let parser_capacity = "no viable alternative at input '<EOF>'".len() + 256;
    let (result, memory) = fixture("", |session, memory| {
        isolation(session)?;
        let cause = fail_isolation(session, PredictionMode::Sll);
        assert_eq!(cause.start.lexeme().kind(), Kind::Eof);
        assert_eq!((cause.start.index(), cause.offending.index()), (0, 0));
        let blocker = leave_free(memory, parser_capacity);
        session.report_no_viable_alternative()?;
        assert_eq!(memory.used_bytes(), memory.limit_bytes());
        report_has_begun(session, 1);
        drop(blocker);
        session.exit_rule()?;
        Err::<(), _>(RuntimeError::NoViableAlternative(cause))
    });
    assert!(matches!(result, Err(RuntimeError::NoViableAlternative(_))));
    assert_eq!(memory.used_bytes(), 0);

    let (result, _) = fixture("", |session, _| {
        isolation(session)?;
        fail_isolation(session, PredictionMode::Ll);
        session.report_no_viable_alternative()
    });
    assert_eq!(reported(result).anomaly().message.as_deref(), Some(
        "Errors parsing SQL statement:\n  - line 1:0 no viable alternative at input '<EOF>'",
    ));
}

#[test]
fn sll_empty_dispatch_counts_the_report_and_keeps_the_session_failed() {
    let (result, memory) = fixture("before READ /* skipped */ read @", |session, memory| {
        isolation(session)?;
        session.advance_input()?;
        let cause = fail_isolation(session, PredictionMode::Sll);
        let used = memory.used_bytes();
        session.report_no_viable_alternative()?;
        report_has_begun(session, 1);
        assert_eq!(memory.used_bytes() - used, "READread".len() + "no viable alternative at input 'READread'".len() + 2 * 256);
        assert_eq!(session.input_index(), Some(1));
        assert!(session.buffered_token(3).is_none());

        let used = memory.used_bytes();
        aborted(session.advance_input());
        aborted(session.report_no_viable_alternative());
        assert_eq!(memory.used_bytes(), used);
        session.exit_rule()?;
        Err::<(), _>(RuntimeError::NoViableAlternative(cause))
    });
    assert!(matches!(result, Err(RuntimeError::NoViableAlternative(_))));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn token_text_admission_fails_after_entering_recovery_but_before_counting() {
    let (result, memory) = fixture("before READ read @", |session, memory| {
        isolation(session)?;
        session.advance_input()?;
        fail_isolation(session, PredictionMode::Sll);
        let blocker = leave_free(memory, 0);
        let result = session.report_no_viable_alternative();
        assert!(matches!(&result, Err(RuntimeError::Input(StreamError::Scan(ScanError::Admission(error))))
            if error.code == "rtdb/memory-budget-exceeded"));
        report_has_begun(session, 0);
        assert_eq!(session.input_index(), Some(1));
        assert!(session.buffered_token(3).is_none());

        drop(blocker);
        aborted(session.report_no_viable_alternative());
        session.exit_rule()?;
        result
    });
    assert!(matches!(result, Err(RuntimeError::Input(_))));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn empty_sll_dispatch_still_requires_the_parser_message_before_counting() {
    let (result, memory) = fixture("READ read", |session, memory| {
        isolation(session)?;
        fail_isolation(session, PredictionMode::Sll);
        let _blocker = leave_free(memory, "READread".len() + 256);
        let result = session.report_no_viable_alternative();
        assert_eq!(memory.used_bytes(), memory.limit_bytes());
        assert!(matches!(&result, Err(RuntimeError::Prediction(PredictionError::Admission(error)))
            if error.code == "rtdb/memory-budget-exceeded"));
        report_has_begun(session, 0);
        session.exit_rule()?;
        result
    });
    assert!(matches!(result, Err(RuntimeError::Prediction(_))));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn throwing_listener_admission_fails_after_the_parser_counts_the_report() {
    let (result, memory) = fixture("READ read", |session, memory| {
        isolation(session)?;
        fail_isolation(session, PredictionMode::Ll);
        let _blocker = leave_free(memory, "READread".len() + "no viable alternative at input 'READread'".len() + 2 * 256);
        let result = session.report_no_viable_alternative();
        assert_eq!(memory.used_bytes(), memory.limit_bytes());
        assert!(matches!(&result, Err(RuntimeError::Prediction(PredictionError::Admission(error)))
            if error.code == "rtdb/memory-budget-exceeded"));
        report_has_begun(session, 1);
        session.exit_rule()?;
        result
    });
    assert!(matches!(result, Err(RuntimeError::Prediction(_))));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn denied_ordinary_execution_consumes_the_pending_report_without_input_work() {
    let (result, memory) = fixture("READ read @", |session, memory| {
        isolation(session)?;
        fail_isolation(session, PredictionMode::Ll);
        assert!(matches!(session.control, ParserControl::PendingNoViable(_)));
        let used = memory.used_bytes();
        aborted(session.lookahead(2));
        aborted(session.report_no_viable_alternative());
        assert!(matches!(session.control, ParserControl::Failed));
        assert_eq!(session.syntax_errors, 0);
        assert_eq!(memory.used_bytes(), used);
        assert_eq!(session.input_index(), Some(0));
        assert!(session.buffered_token(2).is_none());
        session.exit_rule()
    });
    result.unwrap();
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn pending_report_cannot_run_after_rewind_or_rule_finally() {
    for exit in [false, true] {
        let (result, memory) = fixture("before READ read", |session, memory| {
            isolation(session)?;
            session.advance_input()?;
            fail_isolation(session, PredictionMode::Ll);
            if exit {
                session.exit_rule()?;
                assert!(session.current().is_none());
            } else {
                session.rewind_input()?;
                assert_eq!(session.input_index(), Some(0));
            }
            let used = memory.used_bytes();
            aborted(session.report_no_viable_alternative());
            assert_eq!(memory.used_bytes(), used);
            assert_eq!(session.syntax_errors, 0);
            assert!(matches!(session.control, ParserControl::Failed));
            if !exit {
                session.exit_rule()?;
            }
            Ok(())
        });
        result.unwrap();
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn lexer_failure_has_no_pending_parser_report() {
    let (result, memory) = fixture("READ @", |session, memory| {
        isolation(session)?;
        let result = session.adaptive_predict(371, PredictionMode::Sll);
        assert!(matches!(&result, Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_))))));
        let used = memory.used_bytes();
        aborted(session.report_no_viable_alternative());
        assert!(matches!(session.control, ParserControl::Failed));
        assert_eq!(session.syntax_errors, 0);
        assert_eq!(memory.used_bytes(), used);
        assert_eq!(session.input_index(), Some(0));
        session.exit_rule()?;
        result
    });
    assert!(matches!(result, Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(_))))));
    assert_eq!(memory.used_bytes(), 0);
}
