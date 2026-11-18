use std::sync::Arc;
use rtdb_types::{AllocationScope, MemoryBudget};
use crate::lexer::{Kind, ScanError, StreamError};
use crate::parser::grammar::{GrammarError, OwnedGrammar};
use crate::parser::prediction::{PredictionError, PredictionStorage};
use crate::parser::prediction::closure::{ClosureOptions, LoopEntryOption, SourceAssertions};
use super::*;
use super::super::{BailRecovery, CapturedClassPolicy, InvocationCause, InvocationException, InvocationView, ParserControl, ParserEnvironment, TokenDiagnostic, with_bail_fallback, with_parser_session};
use super::super::super::PredictionMode;

const CAPTURED_EXPECTED: &str = concat!(
    "{')', 'AT', 'ATTACH', 'AVG', 'BETWEEN', 'BIGINT', 'BOOL', 'BOOLEAN', 'BOOL_AND', 'BOOL_OR', 'BPCHAR', ",
    "'BY', 'BYTEA', 'CHAR', 'COMMITTED', 'COPY', 'COUNT', 'CUME_DIST', 'CURRENT', 'DATABASE', 'DATE', 'DAY', ",
    "'DEC', 'DECIMAL', 'DELETE', 'DENSE_RANK', 'DETACH', 'DISCARD', 'DOUBLE', 'DOW', 'DOY', 'DURATION', 'END', ",
    "'EPOCH', 'ERASE', 'EVERY', 'FILTER', 'FIRST', 'FLOAT', 'FLOAT4', 'FLOAT8', 'FORMAT', 'GROUPS', 'HOUR', ",
    "'INSERT', 'INT', 'INT2', 'INT4', 'INT8', 'INTEGER', 'INTERVAL', 'ISODOW', 'KEYWORD', 'LAST', 'LEVEL', ",
    "'MAX', 'METADATA', 'MIN', 'MINUTE', 'MONTH', 'NAME', 'NEXT', 'NUMERIC', 'OBJECT', 'OF', 'OID', 'ORDER', ",
    "'PARTITION', 'PASSWORD', 'PERCENT_RANK', 'PERIOD', 'PLANS', 'POSITION', 'PRECISION', 'QUARTER', 'RANGE', ",
    "'RANK', 'REAL', 'RECORD', 'REGCLASS', 'REGPROC', 'ROLE', 'ROW', 'ROWS', 'SECOND', 'SELECT', 'SEQUENCES', ",
    "'SETTING', 'SMALLINT', 'START', 'STDDEV_POP', 'STDDEV_SAMP', 'SUM', 'SYNC', 'SYSTEM_TIME', 'TABLE', ",
    "'TEMP', 'TEMPORARY', 'TEXT', 'TIME', 'TIMESTAMP', 'TIMESTAMPTZ', 'TIMEZONE', 'TSTZRANGE', 'UNCOMMITTED', ",
    "'UPDATE', 'URI', 'USER', 'UUID', 'VALID_TIME', 'VARCHAR', 'VAR_POP', 'VAR_SAMP', 'VARBINARY', 'VERSION', ",
    "'WEEK', 'YEAR', REGULAR_IDENTIFIER, DELIMITED_IDENTIFIER}",
);

fn environment_fixture<T>(
    run: impl FnOnce(&mut ParserEnvironment, &AllocationScope, &Arc<MemoryBudget>) -> T,
) -> (T, Arc<MemoryBudget>) {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
    let memory = MemoryBudget::new(64 << 20);
    let retained = MemoryBudget::new(64 << 20);
    let work = AllocationScope::new(&memory).unwrap();
    let policy = CapturedClassPolicy::from_captured(ClosureOptions {
        pruning: LoopEntryOption::Enabled, assertions: SourceAssertions::Enabled,
    });
    let mut environment = ParserEnvironment::new(PredictionStorage::new(grammar, &retained), policy, &work).unwrap();
    let result = run(&mut environment, &work, &memory);
    drop(environment);
    drop(work);
    assert_eq!(retained.used_bytes(), 0);
    assert_eq!(grammar_memory.used_bytes(), 0);
    (result, memory)
}

fn direct_ll<'sql>(
    source: &'sql str,
    run: impl for<'parse, 'env, 'work> FnOnce(&mut ParserSession<'parse, 'sql, 'env, 'work>, &Arc<MemoryBudget>),
) {
    let (result, memory) = environment_fixture(|environment, work, memory| {
        with_parser_session(environment, source, work, |session| {
            session.profile = ParserProfile::LlDefault;
            run(session, memory);
            Ok(())
        })
    });
    result.unwrap();
    assert_eq!(memory.used_bytes(), 0);
}

fn diagnostic<T>(result: RuntimeResult<'_, T>) -> TokenDiagnostic<'_> {
    let Err(RuntimeError::ReportedTokenSyntax(report)) = result else { panic!("expected token report"); };
    report
}

fn finished_window<'parse>(
    session: &ParserSession<'parse, '_, '_, '_>, index: usize,
) -> (InvocationKey<'parse>, InvocationKey<'parse>) {
    let window = InvocationKey::at(index);
    let details = InvocationKey::at(index + 1);
    let view = session.invocation_view();
    let root = view.context(window).unwrap();
    let child = view.context(details).unwrap();
    assert_eq!((root.rule(), root.parent(), root.invoking_state()), (96, None, ParserState::RootSentinel));
    assert_eq!((child.rule(), child.parent(), child.invoking_state()), (97, Some(window), ParserState::State(2219)));
    assert_eq!((root.start().unwrap().index(), root.stop().unwrap().index()), (0, 0));
    assert_eq!((child.start().unwrap().index(), child.stop().unwrap().index()), (1, 0));
    assert!(matches!(root.children(), [ContextChild::Terminal(token), ContextChild::Context(key)] if token.index() == 0 && *key == details));
    assert!(child.children().is_empty());
    assert!(view.exception(details).is_none());
    assert_eq!(session.current(), None);
    assert_eq!(session.state(), ParserState::RootSentinel);
    assert_eq!(session.input_index(), Some(1));
    assert!(!session.matched_eof);
    (window, details)
}

fn assert_first_pass<'parse>(session: &ParserSession<'parse, '_, '_, '_>, owner: InvocationKey<'parse>) {
    let view = session.invocation_view();
    let Some(InvocationCause::InputMismatch(cause)) = view.exception(owner) else { panic!("expected Bail mismatch cause"); };
    assert_eq!(cause.state, ParserState::State(2220));
    assert_eq!(cause.offending.index(), 1);
    assert!(matches!(&session.control, ParserControl::BailCancelled { exception, recovery: BailRecovery::Clear } if *exception == owner));
    assert!(session.next_tokens_capture.is_none());
    assert!(session.buffered_token(2).is_none());
    assert_eq!(session.syntax_errors, 0);
}

#[test]
fn wrapped_inline_mismatch_reports_from_the_exited_details_but_belongs_to_the_live_window() {
    let mut attempts = Vec::new();
    let (result, memory) = environment_fixture(|environment, work, _| {
        with_bail_fallback(environment, "(: :)", work, |session, mode| {
            attempts.push(mode);
            let index = session.records.len();
            let result = session.window_specification();
            let (window, details) = finished_window(session, index);
            if mode == PredictionMode::Sll {
                assert!(matches!(&result, Err(PassError::Cancelled(_))));
                assert_first_pass(session, window);
            } else {
                assert_eq!(index, 2);
                let Some(InvocationException::CapturedMismatch { cause: InvocationCause::InputMismatch(stored), context }) =
                    session.records[window.index].exception.as_ref() else { panic!("expected the window's primary exception"); };
                assert_eq!(*context, details);
                assert_eq!((stored.state, stored.offending.index()), (ParserState::State(2223), 1));
                let view = session.invocation_view();
                let Some(InvocationCause::InputMismatch(old)) = view.exception(InvocationKey::at(0)) else { panic!("retained SLL cause"); };
                assert_eq!(old.state, ParserState::State(2220));
                assert!(!std::ptr::eq(old, stored));
                assert_eq!(session.next_tokens_capture, Some(NextTokensCapture { context: details, state: 2223 }));
                assert_eq!(session.buffered_token(2).unwrap().lexeme().raw(), ":");
                assert!(session.buffered_token(3).is_none());
                assert_eq!(session.syntax_errors, 1);
                assert!(matches!(session.control, ParserControl::InErrorRecovery));
            }
            result.map(|_| ())
        })
    });
    assert_eq!(attempts, [PredictionMode::Sll, PredictionMode::Ll]);
    let report = diagnostic(result);
    let cause = report.cause().unwrap();
    assert_eq!((cause.state, cause.offending.index()), (ParserState::State(2223), 1));
    assert_eq!(cause.offending.lexeme().span().start.column, 1);
    assert_eq!(report.offending().index(), 1);
    assert_eq!(report.anomaly().code, "xtdb/sql-error");
    let message = format!("Errors parsing SQL statement:\n  - line 1:1 mismatched input ':' expecting {CAPTURED_EXPECTED}");
    assert_eq!(report.anomaly().message.as_deref(), Some(message.as_str()));
    assert_eq!(memory.used_bytes(), message.len() + "xtdb/sql-error".len() + 256);
    drop(report);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn deletion_and_insertion_notify_with_null_cause_before_consumption_or_synthesis() {
    for (source, message, eof) in [
        ("(:)", "extraneous input ':' expecting ')'", false),
        ("(", "missing ')' at '<EOF>'", true),
    ] {
        let mut calls = 0;
        let (result, memory) = environment_fixture(|environment, work, _| {
            with_bail_fallback(environment, source, work, |session, mode| {
                calls += 1;
                let index = session.records.len();
                let result = session.window_specification();
                let (window, details) = finished_window(session, index);
                if mode == PredictionMode::Sll {
                    assert_first_pass(session, window);
                } else {
                    assert!(session.invocation_view().exception(window).is_none());
                    assert_eq!(session.next_tokens_capture, Some(NextTokensCapture { context: details, state: 2223 }));
                    assert_eq!(session.syntax_errors, 1);
                    assert!(matches!(session.control, ParserControl::InErrorRecovery));
                    assert_eq!(session.buffered_token(2).map(|token| token.lexeme().raw()), (!eof).then_some(")"));
                    assert!(session.buffered_token(3).is_none());
                }
                result.map(|_| ())
            })
        });
        assert_eq!(calls, 2);
        let report = diagnostic(result);
        assert!(report.cause().is_none());
        assert_eq!(report.offending().index(), 1);
        assert_eq!(report.offending().lexeme().kind() == Kind::Eof, eof);
        let message = format!("Errors parsing SQL statement:\n  - line 1:1 {message}");
        assert_eq!(report.anomaly().message.as_deref(), Some(message.as_str()));
        drop(report);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn ll_second_token_lexer_error_preempts_mismatch_construction_after_capture() {
    let mut calls = 0;
    let (result, memory) = environment_fixture(|environment, work, _| {
        with_bail_fallback(environment, "(: @", work, |session, mode| {
            calls += 1;
            let index = session.records.len();
            let result = session.window_specification();
            let (window, details) = finished_window(session, index);
            if mode == PredictionMode::Sll {
                assert_first_pass(session, window);
            } else {
                assert!(session.invocation_view().exception(window).is_none());
                assert_eq!(session.next_tokens_capture, Some(NextTokensCapture { context: details, state: 2223 }));
                assert_eq!(session.syntax_errors, 0);
                assert!(matches!(session.control, ParserControl::Failed));
                assert!(session.buffered_token(2).is_none());
            }
            result.map(|_| ())
        })
    });
    assert_eq!(calls, 2);
    let Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(error)))) = result else { panic!("expected LA(2) lexer error"); };
    assert_eq!(error.span().start.column, 3);
    assert_eq!(memory.used_bytes(), 0);
}

fn reached_window_mismatch<'parse, 'sql>(
    session: &mut ParserSession<'parse, 'sql, '_, '_>,
) -> (InvocationKey<'parse>, InvocationKey<'parse>, PassResult<'parse, 'sql, ()>) {
    let window = session.new_invocation(WINDOW_SPECIFICATION).unwrap();
    session.enter_tree_rule(window).unwrap();
    session.match_window_terminal(window, 2218).unwrap();
    session.set_state(2219).unwrap();
    let details = session.window_specification_details().unwrap();
    let result = session.match_window_terminal(window, 2220);
    assert!(matches!(&result, Err(PassError::Runtime(RuntimeError::InputMismatch(cause)))
        if cause.state == ParserState::State(2223) && cause.offending.index() == 1));
    assert!(matches!(&session.control, ParserControl::PendingInputMismatch(pending)
        if pending.invocation == window && pending.match_state == ParserState::State(2220)));
    assert_eq!((session.current(), session.state(), session.input_index()), (Some(window), ParserState::State(2220), Some(1)));
    assert!(session.invocation_view().exception(window).is_none());
    (window, details, result)
}

#[test]
fn captured_expected_lookup_admission_follows_catch_assignment_and_precedes_notification() {
    direct_ll("(: :)", |session, memory| {
        let (window, details, body) = reached_window_mismatch(session);
        let held = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        let denied_lookup = {
            let grammar = session.environment.storage.grammar_view();
            let child = InvocationView { records: &session.records, current: Some(details) };
            let Err(GrammarError::Admission(error)) = grammar.state(2223).unwrap().expected_tokens(child.invocations(&grammar), session.work)
                else { panic!("expected captured LOOK budget denial"); };
            error
        };
        let result = session.finish_window_rule(window, body);
        assert!(matches!(result, Err(PassError::Runtime(RuntimeError::Prediction(PredictionError::Admission(error))))
            if error == denied_lookup && error.code == "rtdb/memory-budget-exceeded"));
        assert!(matches!(session.records[window.index].exception.as_ref(), Some(InvocationException::CapturedMismatch { context, .. })
            if *context == details));
        assert_eq!(session.syntax_errors, 0);
        assert!(matches!(session.control, ParserControl::InErrorRecovery));
        finished_window(session, 0);
        assert!(session.buffered_token(3).is_none());
        drop(held);
    });
}

#[test]
fn pending_catch_uses_actual_owner_state_and_cursor_instead_of_the_saved_source_context() {
    for failure in 0..3 {
        direct_ll("(: :)", |session, _| {
            let (owner, child, _) = reached_window_mismatch(session);
            let local = match failure {
                0 => child,
                1 => { session.exit_rule().unwrap(); owner }
                _ => { session.rewind_input().unwrap(); owner }
            };
            let result = session.catch_recognition::<()>(local);
            assert!(matches!(result, Err(PassError::Runtime(RuntimeError::Control(RuntimeProblem::Aborted)))));
            assert!(session.invocation_view().exception(owner).is_none());
            assert!(session.invocation_view().exception(child).is_none());
            assert_eq!(session.next_tokens_capture, Some(NextTokensCapture { context: child, state: 2223 }));
            assert_eq!(session.syntax_errors, 0);
            assert!(matches!(session.control, ParserControl::Failed));
        });
    }
}

#[test]
fn successful_rparen_match_keeps_capture_until_the_next_accepted_sync() {
    direct_ll("()(existingWindow)", |session, _| {
        let window = session.window_specification().unwrap();
        let detail = InvocationKey::at(window.index + 1);
        let view = session.invocation_view();
        let root = view.context(window).unwrap();
        assert!(matches!(root.children(), [ContextChild::Terminal(open), ContextChild::Context(key), ContextChild::Terminal(close)]
            if open.index() == 0 && *key == detail && close.index() == 1));
        assert_eq!(root.stop().unwrap().index(), 1);
        assert_eq!(session.next_tokens_capture, Some(NextTokensCapture { context: detail, state: 2223 }));
        let result = session.window_specification();
        assert!(matches!(result, Err(PassError::Runtime(RuntimeError::Control(RuntimeProblem::PredictionSite)))));
        assert!(session.next_tokens_capture.is_none());
        assert_eq!(session.input_index(), Some(3));
        assert!(session.buffered_token(4).is_none());
        assert_eq!(session.current(), None);
        assert_eq!(session.state(), ParserState::RootSentinel);
    });
}

#[test]
fn each_generated_optional_guard_fails_closed_before_demanding_its_unimplemented_rule() {
    for source in ["(existingWindow @", "(PARTITION @", "(ORDER @", "(GROUPS @", "(RANGE @", "(ROWS @"] {
        direct_ll(source, |session, _| {
            let result = session.window_specification();
            assert!(matches!(result, Err(PassError::Runtime(RuntimeError::Control(RuntimeProblem::PredictionSite)))));
            let (owner, _) = finished_window(session, 0);
            assert!(session.invocation_view().exception(owner).is_none());
            assert_eq!(session.records.len(), 2);
            assert!(session.next_tokens_capture.is_none());
            assert!(session.buffered_token(2).is_none());
            assert_eq!(session.syntax_errors, 0);
        });
    }
}
