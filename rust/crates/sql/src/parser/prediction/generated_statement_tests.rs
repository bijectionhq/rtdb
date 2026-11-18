use std::{alloc::Layout, sync::Arc};
use rtdb_types::{AllocationScope, MemoryBudget};
use crate::lexer::{Kind, ScanError, StreamError};
use crate::parser::grammar::OwnedGrammar;
use crate::parser::prediction::{PredictionError, PredictionStorage};
use crate::parser::prediction::closure::{ClosureOptions, LoopEntryOption, SourceAssertions};
use super::*;
use super::super::{BailRecovery, CapturedClassPolicy, InvocationCause, InvocationException, InvocationRecord, InvocationRole, ParserControl, ParserEnvironment, ParserProfile, RuntimeResult, with_bail_fallback, with_parser_session};

const FIRST_STATEMENT: &str = concat!(
    "{'(', 'ASSERT', 'ATTACH', 'BEGIN', 'COMMIT', 'COPY', 'CREATE', 'DELETE', 'DETACH', 'DISCARD', ",
    "'ERASE', 'EXECUTE', 'EXPLAIN', 'FROM', 'GRANT', 'INSERT', 'PATCH', 'PREPARE', 'RECORDS', 'REVOKE', ",
    "'ROLLBACK', 'SELECT', 'SET', 'SETTING', 'SHOW', 'START', 'UPDATE', 'VALUES', 'WITH', 'XTQL'}",
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

fn wrapped<'sql>(
    source: &'sql str,
    mut observe: impl for<'parse, 'env, 'work> FnMut(
        &ParserSession<'parse, 'sql, 'env, 'work>, PredictionMode, usize,
        &PassResult<'parse, 'sql, InvocationKey<'parse>>,
    ),
) -> (RuntimeResult<'sql, ()>, Arc<MemoryBudget>) {
    environment_fixture(|environment, work, _| {
        with_bail_fallback(environment, source, work, |session, mode| {
            let first = session.records.len();
            let result = session.direct_sql_statement();
            observe(session, mode, first, &result);
            result.map(|_| ())
        })
    })
}

fn frames<'parse>(
    session: &ParserSession<'parse, '_, '_, '_>, first: usize,
) -> (InvocationKey<'parse>, InvocationKey<'parse>) {
    let root = InvocationKey::at(first);
    let generic = InvocationKey::at(first + 1);
    let view = session.invocation_view();
    let a = view.context(root).unwrap();
    let b = view.context(generic).unwrap();
    assert_eq!((a.rule(), a.class(), a.parent(), a.invoking_state()),
        (DIRECT_SQL_STATEMENT, None, None, ParserState::RootSentinel));
    assert_eq!((b.rule(), b.class(), b.parent(), b.invoking_state()),
        (DIRECTLY_EXECUTABLE_STATEMENT, None, Some(root), ParserState::State(352)));
    assert_eq!(a.start().unwrap().index(), 0);
    assert_eq!(b.start().unwrap().index(), 0);
    assert!(b.children().is_empty());
    (root, generic)
}

fn labelled_child<'parse>(
    session: &ParserSession<'parse, '_, '_, '_>, root: InvocationKey<'parse>, generic: InvocationKey<'parse>,
) -> InvocationKey<'parse> {
    let view = session.invocation_view();
    let children = view.context(root).unwrap().children();
    let Some(ContextChild::Context(child)) = children.first() else { panic!("expected rollback context"); };
    assert_eq!(child.index, generic.index + 1);
    assert_eq!(session.records.len(), child.index + 1);
    let label = view.context(*child).unwrap();
    assert_eq!((label.rule(), label.class(), label.parent(), label.invoking_state()),
        (DIRECTLY_EXECUTABLE_STATEMENT, Some(ContextClass::RollbackStatement), Some(root), ParserState::State(352)));
    assert_eq!(label.start().unwrap().index(), 0);
    assert!(view.context(generic).unwrap().stop().is_none());
    assert!(view.exception(generic).is_none());
    assert!(view.exception(*child).is_none());
    *child
}

fn assert_finished(session: &ParserSession<'_, '_, '_, '_>, cursor: usize, matched_eof: bool) {
    assert_eq!(session.input_index(), Some(cursor));
    assert_eq!(session.current(), None);
    assert_eq!(session.state(), ParserState::RootSentinel);
    assert_eq!(session.matched_eof, matched_eof);
    assert_eq!(session.precedence(), 0);
}

fn assert_rollback_success<'parse>(
    session: &ParserSession<'parse, '_, '_, '_>, root_key: InvocationKey<'parse>, semi: bool,
) {
    let (root, generic) = frames(session, 0);
    assert_eq!(root, root_key);
    let child = labelled_child(session, root, generic);
    let view = session.invocation_view();
    let a = view.context(root).unwrap();
    let b = view.context(child).unwrap();
    assert!(matches!(b.children(), [ContextChild::Terminal(token)] if token.index() == 0 && token.lexeme().raw() == "ROLLBACK"));
    assert_eq!(b.stop().unwrap().index(), 0);
    let eof_index = if semi { 2 } else { 1 };
    match a.children() {
        [ContextChild::Context(key), ContextChild::Terminal(eof)] if !semi => {
            assert_eq!(*key, child);
            assert_eq!((eof.index(), eof.lexeme().kind()), (eof_index, Kind::Eof));
        }
        [ContextChild::Context(key), ContextChild::Terminal(delimiter), ContextChild::Terminal(eof)] if semi => {
            assert_eq!(*key, child);
            assert_eq!((delimiter.index(), delimiter.lexeme().raw()), (1, ";"));
            assert_eq!((eof.index(), eof.lexeme().kind()), (eof_index, Kind::Eof));
        }
        children => panic!("unexpected root tree: {children:?}"),
    }
    let stop = a.stop().unwrap();
    assert_eq!((stop.index(), stop.lexeme().kind()), (eof_index, Kind::Eof));
    assert!(view.exception(root).is_none());
    assert_finished(session, eof_index, true);
    assert!(session.buffered_token(eof_index + 1).is_none());
    assert!(session.next_tokens_capture.is_none());
    assert_eq!(session.syntax_errors, 0);
}

#[test]
fn rollback_root_matches_optional_semicolon_and_attaches_actual_eof_in_both_profiles() {
    for (source, semi) in [("ROLLBACK", false), ("ROLLBACK;", true)] {
        let mut calls = 0;
        let (result, memory) = wrapped(source, |session, mode, first, result| {
            calls += 1;
            assert_eq!(mode, PredictionMode::Sll);
            assert_eq!(first, 0);
            assert_rollback_success(session, *result.as_ref().unwrap(), semi);
        });
        result.unwrap();
        assert_eq!(calls, 1);
        assert_eq!(memory.used_bytes(), 0);
        let (result, memory) = environment_fixture(|environment, work, _| {
            with_parser_session(environment, source, work, |session| {
                session.profile = ParserProfile::LlDefault;
                let root = session.direct_sql_statement().unwrap();
                assert_rollback_success(session, root, semi);
                Ok(())
            })
        });
        result.unwrap();
        assert_eq!(memory.used_bytes(), 0);
    }
}

fn assert_initial_failure_tree<'parse>(
    session: &ParserSession<'parse, '_, '_, '_>, first: usize,
) -> (InvocationKey<'parse>, InvocationKey<'parse>) {
    let (root, child) = frames(session, first);
    let view = session.invocation_view();
    assert_eq!(session.records.len(), first + 2);
    assert!(matches!(view.context(root).unwrap().children(), [ContextChild::Context(key)] if *key == child));
    assert!(view.context(root).unwrap().stop().is_none());
    assert!(view.context(child).unwrap().stop().is_none());
    assert_finished(session, 0, false);
    assert!(session.next_tokens_capture.is_none());
    (root, child)
}

fn assert_first_no_viable<'parse>(
    session: &ParserSession<'parse, '_, '_, '_>, root: InvocationKey<'parse>, child: InvocationKey<'parse>, spelling: &str,
) {
    let view = session.invocation_view();
    let original = view.exception(child).unwrap();
    let Some(InvocationCause::NoViableAlternative(cause)) = view.exception(root) else { panic!("expected SLL decision failure"); };
    assert!(std::ptr::eq(original, view.exception(root).unwrap()));
    assert!(matches!(&session.records[child.index].exception, Some(InvocationException::Owned(InvocationCause::NoViableAlternative(_)))));
    assert!(matches!(&session.records[root.index].exception, Some(InvocationException::Shared(owner)) if *owner == child));
    assert_eq!((cause.state, cause.start.index(), cause.offending.index()), (ParserState::State(574), 0, 0));
    assert_eq!(cause.start.lexeme().raw(), spelling);
    assert_eq!(cause.offending.lexeme().raw(), spelling);
    assert_eq!(session.syntax_errors, 1);
    assert!(session.buffered_token(1).is_none());
    assert!(matches!(&session.control, ParserControl::BailCancelled { exception, recovery: BailRecovery::Entered } if *exception == child));
}

fn assert_report(result: RuntimeResult<'_, ()>, memory: &Arc<MemoryBudget>, state: Option<u32>, index: usize, message: &str) {
    let Err(RuntimeError::ReportedTokenSyntax(report)) = result else { panic!("expected LL syntax report"); };
    assert_eq!(report.cause().map(|cause| cause.state), state.map(ParserState::State));
    assert_eq!(report.cause().map(|cause| cause.offending.index()), state.map(|_| index));
    assert_eq!(report.offending().index(), index);
    assert_eq!(report.anomaly().code, "xtdb/sql-error");
    let message = format!("Errors parsing SQL statement:\n  - line {message}");
    assert_eq!(report.anomaly().message.as_deref(), Some(message.as_str()));
    assert_eq!(memory.used_bytes(), message.len() + "xtdb/sql-error".len() + 256);
    drop(report);
    assert_eq!(memory.used_bytes(), 0);
}

fn check_initial_rule_report(source: &str, first_token: &str, second_token: &str, cause: bool) {
    let mut modes = Vec::new();
    let (result, memory) = wrapped(source, |session, mode, first, result| {
        modes.push(mode);
        let (root, child) = assert_initial_failure_tree(session, first);
        match mode {
            PredictionMode::Sll => {
                assert_eq!(first, 0);
                assert!(matches!(result, Err(PassError::Cancelled(_))));
                assert_first_no_viable(session, root, child, first_token);
            }
            PredictionMode::Ll => {
                assert_eq!(first, 2);
                let view = session.invocation_view();
                assert!(matches!(view.exception(InvocationKey::at(1)), Some(InvocationCause::NoViableAlternative(_))));
                assert!(view.exception(root).is_none());
                match (cause, view.exception(child)) {
                    (true, Some(InvocationCause::InputMismatch(stored))) => {
                        assert_eq!((stored.state, stored.offending.index()), (ParserState::State(574), 0));
                        assert_eq!(stored.offending.lexeme().raw(), first_token);
                    }
                    (false, None) => (),
                    _ => panic!("unexpected LL exception assignment"),
                }
                assert_eq!(session.buffered_token(1).unwrap().lexeme().raw(), second_token);
                assert!(session.buffered_token(2).is_none());
                assert_eq!(session.syntax_errors, 1);
                assert!(matches!(session.control, ParserControl::InErrorRecovery));
                let Err(PassError::Runtime(RuntimeError::ReportedTokenSyntax(diagnostic))) = result else { panic!("expected live LL report"); };
                assert_eq!(diagnostic.cause().is_some(), cause);
            }
        }
    });
    assert_eq!(modes, [PredictionMode::Sll, PredictionMode::Ll]);
    let prefix = if cause { "mismatched input" } else { "extraneous input" };
    let message = format!("1:0 {prefix} '{first_token}' expecting {FIRST_STATEMENT}");
    assert_report(result, &memory, cause.then_some(574), 0, &message);
}

#[test]
fn selec_source_mismatch_belongs_to_the_generic_ll_child_at_first574() {
    check_initial_rule_report("SELEC 1", "SELEC", "1", true);
}

#[test]
fn extra_select_source_deletion_reports_null_cause_without_demanding_the_literal() {
    check_initial_rule_report("extra SELECT 1", "extra", "SELECT", false);
}

#[test]
fn statement_suffix_sync_and_eof_match_select_distinct_expected_sets_and_cause_owners() {
    for (source, offending, next, semi, state, message) in [
        ("ROLLBACK junk", "junk", None, false, None, "extraneous input 'junk' expecting {<EOF>, ';'}"),
        ("ROLLBACK junk bad", "junk", Some("bad"), false, Some(354), "mismatched input 'junk' expecting {<EOF>, ';'}"),
        ("ROLLBACK;;", ";", None, true, None, "extraneous input ';' expecting <EOF>"),
        ("ROLLBACK;;;", ";", Some(";"), true, Some(356), "mismatched input ';' expecting <EOF>"),
    ] {
        let cursor = if semi { 2 } else { 1 };
        let mut modes = Vec::new();
        let (result, memory) = wrapped(source, |session, mode, first, result| {
            modes.push(mode);
            let (root, generic) = frames(session, first);
            let label = labelled_child(session, root, generic);
            let view = session.invocation_view();
            let a = view.context(root).unwrap();
            let b = view.context(label).unwrap();
            assert!(matches!(b.children(), [ContextChild::Terminal(token)] if token.index() == 0));
            assert_eq!(b.stop().unwrap().index(), 0);
            assert_eq!(a.stop().unwrap().index(), cursor - 1);
            assert_eq!(a.children().len(), cursor);
            if semi {
                assert!(matches!(a.children()[1], ContextChild::Terminal(token) if token.index() == 1 && token.lexeme().raw() == ";"));
            }
            assert_finished(session, cursor, false);
            assert!(session.next_tokens_capture.is_none());
            if mode == PredictionMode::Sll {
                assert!(matches!(result, Err(PassError::Cancelled(_))));
                assert!(matches!(&session.control, ParserControl::BailCancelled { exception, recovery: BailRecovery::Clear } if *exception == root));
                assert_eq!(session.syntax_errors, 0);
                assert!(session.buffered_token(cursor + 1).is_none());
            } else {
                let token = session.buffered_token(cursor + 1).unwrap();
                if let Some(spelling) = next {
                    assert_eq!(token.lexeme().raw(), spelling);
                } else {
                    assert_eq!(token.lexeme().kind(), Kind::Eof);
                }
                assert!(session.buffered_token(cursor + 2).is_none());
                assert_eq!(session.syntax_errors, 1);
                assert!(matches!(session.control, ParserControl::InErrorRecovery));
            }
            let state = if mode == PredictionMode::Sll { Some(356) } else { state };
            match (state, view.exception(root)) {
                (Some(state), Some(InvocationCause::InputMismatch(stored))) => {
                    assert_eq!((stored.state, stored.offending.index()), (ParserState::State(state), cursor));
                    assert_eq!(stored.offending.lexeme().raw(), offending);
                }
                (None, None) => (),
                _ => panic!("unexpected root exception assignment"),
            }
        });
        assert_eq!(modes, [PredictionMode::Sll, PredictionMode::Ll]);
        assert_report(result, &memory, state, cursor, &format!("1:9 {message}"));
    }
}

#[test]
fn second_token_lexer_failure_preempts_required_block_report_on_ll_retry() {
    let mut modes = Vec::new();
    let (result, memory) = wrapped("SELEC @", |session, mode, first, result| {
        modes.push(mode);
        let (root, child) = assert_initial_failure_tree(session, first);
        if mode == PredictionMode::Sll {
            assert!(matches!(result, Err(PassError::Cancelled(_))));
            assert_first_no_viable(session, root, child, "SELEC");
        } else {
            let view = session.invocation_view();
            assert!(view.exception(root).is_none());
            assert!(view.exception(child).is_none());
            assert!(session.buffered_token(1).is_none());
            assert_eq!(session.syntax_errors, 0);
            assert!(matches!(session.control, ParserControl::Failed));
        }
    });
    assert_eq!(modes, [PredictionMode::Sll, PredictionMode::Ll]);
    let Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(error)))) = result else { panic!("expected lexer error"); };
    assert_eq!(error.span().start.column, 6);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn rollback_consume_lexer_failure_keeps_the_label_without_a_terminal_or_ll_retry() {
    let mut calls = 0;
    let (result, memory) = wrapped("ROLLBACK @", |session, mode, first, _| {
        calls += 1;
        assert_eq!(mode, PredictionMode::Sll);
        let (root, generic) = frames(session, first);
        let child = labelled_child(session, root, generic);
        let view = session.invocation_view();
        assert!(view.context(root).unwrap().stop().is_none());
        assert!(view.exception(root).is_none());
        let b = view.context(child).unwrap();
        assert!(b.children().is_empty());
        assert!(b.stop().is_none());
        assert_finished(session, 0, false);
        assert!(session.buffered_token(1).is_none());
        assert_eq!(session.syntax_errors, 0);
        assert!(matches!(session.control, ParserControl::Failed));
    });
    assert_eq!(calls, 1);
    let Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(error)))) = result else { panic!("expected lexer error"); };
    assert_eq!(error.span().start.column, 9);
    assert_eq!(memory.used_bytes(), 0);
}

fn child_charge(capacity: usize) -> usize {
    Layout::array::<ContextChild<'_, '_>>(capacity).unwrap().size() + 256
}

#[test]
fn warm_rollback_budget_preserves_actual_current_cleanup_and_eof_stop() {
    let (result, memory) = environment_fixture(|environment, work, memory| {
        with_bail_fallback(environment, "ROLLBACK", work, |session, mode| {
            assert_eq!(mode, PredictionMode::Sll);
            session.direct_sql_statement().map(|_| ())
        })?;
        let records = Layout::array::<InvocationRecord<'_, '_>>(16).unwrap().size() + 256;
        for (extra, denied) in [(0, child_charge(1)), (2 * child_charge(1), child_charge(2))] {
            with_parser_session(environment, "ROLLBACK", work, |session| {
                session.profile = ParserProfile::SllBail;
                let available = records + extra;
                let held = memory.reserve(memory.limit_bytes() - memory.used_bytes() - available).unwrap();
                let result = session.direct_sql_statement();
                let Err(PassError::Runtime(RuntimeError::Prediction(PredictionError::Admission(error)))) = result else { panic!("expected a child-vector admission failure"); };
                assert_eq!(error.code, "rtdb/memory-budget-exceeded");
                let requested = format!("Cannot reserve {denied} bytes from budget ");
                assert!(error.message.as_deref().unwrap().starts_with(requested.as_str()));
                let (root, generic) = frames(session, 0);
                let view = session.invocation_view();
                let a = view.context(root).unwrap();
                if extra == 0 {
                    assert_eq!(session.records.len(), 2);
                    assert!(matches!(session.records[generic.index].role, InvocationRole::OrdinaryTreeAttachmentFailed));
                    assert!(a.children().is_empty());
                    assert!(a.stop().is_none());
                    assert!(view.context(generic).unwrap().stop().is_none());
                    assert_eq!((session.current(), session.state()), (Some(root), ParserState::State(352)));
                    assert_eq!(session.input_index(), Some(0));
                    assert!(session.buffered_token(1).is_none());
                    assert!(!session.matched_eof);
                } else {
                    let child = labelled_child(session, root, generic);
                    assert!(matches!(a.children(), [ContextChild::Context(key)] if *key == child));
                    assert_eq!(view.context(child).unwrap().stop().unwrap().index(), 0);
                    let stop = a.stop().unwrap();
                    assert_eq!((stop.index(), stop.lexeme().kind()), (1, Kind::Eof));
                    assert_finished(session, 1, true);
                }
                assert!(view.exception(root).is_none());
                assert!(view.exception(generic).is_none());
                assert_eq!(session.syntax_errors, 0);
                assert!(matches!(session.control, ParserControl::Failed));
                drop(held);
                Ok(())
            })?;
        }
        Ok::<(), RuntimeError<'_>>(())
    });
    result.unwrap();
    assert_eq!(memory.used_bytes(), 0);
}
