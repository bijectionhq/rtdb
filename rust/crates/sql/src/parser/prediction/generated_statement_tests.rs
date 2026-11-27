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

#[test]
fn insert_values_uses_the_original_returning_rule_and_select_list_call() {
    let source = "INSERT INTO \"select\".\"table\" (\"_id\") VALUES (1), (2) RETURNING 3 AS \"order\";";
    let (result, memory) = environment_fixture(|environment, work, _| {
        for profile in [ParserProfile::SllBail, ParserProfile::LlDefault] {
            with_parser_session(environment, source, work, |session| {
                session.profile = profile;
                let root = session.direct_sql_statement().unwrap();
                let view = session.invocation_view();
                let [ContextChild::Context(insert), ContextChild::Terminal(_), ContextChild::Terminal(_)] =
                    view.context(root).unwrap().children() else { panic!("expected INSERT and statement delimiters"); };
                let insert = view.context(*insert).unwrap();
                assert_eq!(insert.class(), Some(ContextClass::InsertStatement));
                let Some(ContextChild::Context(returning)) = insert.children().last() else { panic!("expected RETURNING child"); };
                let returning = view.context(*returning).unwrap();
                assert_eq!((returning.class(), returning.invoking_state()),
                    (Some(ContextClass::DmlReturningStatement), ParserState::State(385)));
                let [ContextChild::Terminal(keyword), ContextChild::Context(list)] = returning.children() else {
                    panic!("expected RETURNING and its select list");
                };
                assert_eq!(keyword.lexeme().raw(), "RETURNING");
                let list = view.context(*list).unwrap();
                assert_eq!(list.invoking_state(), ParserState::State(2746));
                assert_eq!(list.start().unwrap().lexeme().raw(), "3");
                assert_eq!(list.stop().unwrap().lexeme().raw(), "\"order\"");
                assert_eq!(session.current(), None);
                assert_eq!(session.precedence(), 0);
                assert!(session.matched_eof);
                Ok(())
            })?;
        }
        Ok(())
    });
    result.unwrap();
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn returning_after_a_multibyte_update_prefix_keeps_both_coordinates() {
    let source = "UPDATE \"🙂\".\"select\" SET \"returning\" = $1 RETURNING CAST(a AS BLOB)";
    let (result, memory) = wrapped(source, |_, _, _, _| {});
    let RuntimeError::ReportedNoViableAlternative(report) = result.unwrap_err() else {
        panic!("expected the listener's no-viable-alternative error");
    };
    let offending = report.cause().offending.lexeme();
    let position = offending.span().start;
    assert_eq!(offending.raw(), "BLOB");
    assert_eq!(position.byte, source.find("BLOB").unwrap());
    assert_eq!((position.line, position.column), (1, 61));
    assert_eq!(position.byte, position.column + 3);
    drop(report);
    assert_eq!(memory.used_bytes(), 0);
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

fn both_statement_profiles<'sql>(
    environment: &mut ParserEnvironment, source: &'sql str, work: &AllocationScope, multi: bool,
    mut check: impl for<'parse, 'env, 'work> FnMut(&ParserSession<'parse, 'sql, 'env, 'work>, InvocationKey<'parse>),
) {
    with_bail_fallback(environment, source, work, |session, mode| {
        assert_eq!(mode, PredictionMode::Sll);
        let root = if multi { session.multi_sql_statement()? } else { session.direct_sql_statement()? };
        check(session, root);
        Ok(())
    }).unwrap();
    with_parser_session(environment, source, work, |session| {
        session.profile = ParserProfile::LlDefault;
        let root = if multi { session.multi_sql_statement().unwrap() } else { session.direct_sql_statement().unwrap() };
        check(session, root);
        Ok(())
    }).unwrap();
}

fn context_child<'parse>(child: ContextChild<'parse, '_>) -> InvocationKey<'parse> {
    let ContextChild::Context(key) = child else { panic!("expected child rule"); };
    key
}

#[test]
fn direct_commit_and_discard_use_the_complete_optional_and_target_schedules() {
    let ((), memory) = environment_fixture(|environment, work, _| {
        for (source, suffix, target) in [
            ("COMMIT", None, None),
            ("COMMIT ASYNC", Some("ASYNC"), None),
            ("commit sYnC;", Some("sYnC"), None),
            ("DISCARD ALL", Some("ALL"), Some(ContextClass::DiscardAll)),
            ("DISCARD PLANS", Some("PLANS"), Some(ContextClass::DiscardPlans)),
            ("DISCARD TEMP", Some("TEMP"), Some(ContextClass::DiscardTemp)),
            ("DISCARD TEMPORARY", Some("TEMPORARY"), Some(ContextClass::DiscardTemp)),
            ("DISCARD SEQUENCES;", Some("SEQUENCES"), Some(ContextClass::DiscardSequences)),
        ] {
            both_statement_profiles(environment, source, work, false, |session, root| {
                let tree = session.invocation_view();
                let root = tree.context(root).unwrap();
                let child = context_child(root.children()[0]);
                let statement = tree.context(child).unwrap();
                assert_eq!(statement.invoking_state(), ParserState::State(352));
                let terminals = if let Some(target_class) = target {
                    assert_eq!(statement.class(), Some(ContextClass::DiscardStatement));
                    assert!(matches!(statement.children(), [ContextChild::Terminal(head), ContextChild::Context(_)]
                        if head.lexeme().raw() == "DISCARD"));
                    let target = tree.context(context_child(statement.children()[1])).unwrap();
                    assert_eq!((target.class(), target.parent(), target.invoking_state()),
                        (Some(target_class), Some(child), ParserState::State(502)));
                    target.children()
                } else {
                    assert_eq!(statement.class(), Some(ContextClass::CommitStatement));
                    assert!(matches!(statement.children()[0], ContextChild::Terminal(head)
                        if head.lexeme().raw().eq_ignore_ascii_case("commit")));
                    &statement.children()[1..]
                };
                if let Some(raw) = suffix {
                    assert!(matches!(terminals, [ContextChild::Terminal(token)]
                        if token.index() == 1 && token.lexeme().raw() == raw));
                } else {
                    assert!(terminals.is_empty());
                }
                assert_eq!(statement.stop().unwrap().index(), if suffix.is_some() { 1 } else { 0 });
                assert!(session.next_tokens_capture.is_none());
                assert_eq!(session.syntax_errors, 0);
                assert_finished(session, root.stop().unwrap().index(), true);
            });
        }
    });
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn set_role_reaches_the_complete_identifier_domain_through_actual_decision34() {
    use crate::parser::grammar::ExpectedSymbol;

    let ((), memory) = environment_fixture(|environment, work, _| {
        let keywords: Vec<_> = {
            let grammar = environment.storage.grammar_view();
            let first = grammar.state(848).unwrap().next_tokens(work).unwrap();
            first.iter().map(|symbol| {
                let ExpectedSymbol::Token(token) = symbol else { panic!("keyword token"); };
                let literal = grammar.display_name(token);
                (token.number(), literal.strip_prefix('\'').unwrap().strip_suffix('\'').unwrap())
            }).collect()
        };
        assert_eq!(keywords.len(), 111);
        for (number, raw) in keywords.into_iter().chain([(365, "my_role"), (366, "\"My Role\""), (215, "NONE")]) {
            let source = format!("SET ROLE {raw}");
            both_statement_profiles(environment, &source, work, false, |session, root| {
                let tree = session.invocation_view();
                let root = tree.context(root).unwrap();
                let statement_key = context_child(root.children()[0]);
                let statement = tree.context(statement_key).unwrap();
                assert_eq!(statement.class(), Some(ContextClass::SetRoleStatement));
                let [ContextChild::Terminal(set), ContextChild::Terminal(role), operand] = statement.children() else {
                    panic!("SET ROLE children");
                };
                assert_eq!((set.index(), set.lexeme().raw(), role.index(), role.lexeme().raw()), (0, "SET", 1, "ROLE"));
                let terminal = if number == 215 {
                    *operand
                } else {
                    let name_key = context_child(*operand);
                    let name = tree.context(name_key).unwrap();
                    let class = if number == 366 { ContextClass::DelimitedIdentifier } else { ContextClass::RegularIdentifier };
                    assert_eq!((name.class(), name.parent(), name.invoking_state()),
                        (Some(class), Some(statement_key), ParserState::State(505)));
                    if matches!(number, 365 | 366) { name.children()[0] } else {
                        let keyword = tree.context(context_child(name.children()[0])).unwrap();
                        assert_eq!((keyword.rule(), keyword.parent(), keyword.invoking_state()),
                            (23, Some(name_key), ParserState::State(755)));
                        let child = keyword.children()[0];
                        if let ContextChild::Context(group) = child {
                            let group = tree.context(group).unwrap();
                            assert!(matches!((group.rule(), group.invoking_state()),
                                (147, ParserState::State(797)) | (63, ParserState::State(798))
                                | (65, ParserState::State(799)) | (44, ParserState::State(800))));
                            if group.rule() == 63 && number != 286 {
                                let field = tree.context(context_child(group.children()[0])).unwrap();
                                assert_eq!((field.rule(), field.invoking_state()), (19, ParserState::State(1894)));
                                field.children()[0]
                            } else {
                                group.children()[0]
                            }
                        } else {
                            child
                        }
                    }
                };
                assert!(matches!(terminal, ContextChild::Terminal(token) if token.index() == 2 && token.lexeme().raw() == raw));
                assert_eq!(statement.stop().unwrap().index(), 2);
                assert_finished(session, 3, true);
            });
        }
    });
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn source_session_variable_alternative_is_not_labelled_as_set_role() {
    let (result, memory) = wrapped("SET ROLE = 'v'", |session, mode, first, result| {
        assert_eq!(mode, PredictionMode::Sll);
        assert_eq!(first, 0);
        assert!(matches!(result, Err(PassError::Runtime(RuntimeError::Control(RuntimeProblem::PredictionSite)))));
        let (root, child) = frames(session, first);
        let tree = session.invocation_view();
        assert!(matches!(tree.context(root).unwrap().children(), [ContextChild::Context(key)] if *key == child));
        assert!(tree.context(child).unwrap().class().is_none());
        assert!(session.next_tokens_capture.is_none());
        assert_finished(session, 0, false);
    });
    assert!(matches!(result, Err(RuntimeError::Control(RuntimeProblem::PredictionSite))));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn multi_root_keeps_ordered_calls_and_the_distinct_trailing_semicolon() {
    let ((), memory) = environment_fixture(|environment, work, _| {
        for source in [
            "ROLLBACK; COMMIT ASYNC; DISCARD TEMPORARY; SET ROLE COUNT",
            "ROLLBACK; COMMIT ASYNC; DISCARD TEMPORARY; SET ROLE COUNT;",
            "ROLLBACK; COMMIT ASYNC; DISCARD TEMPORARY; SET ROLE COUNT;",
        ] {
            both_statement_profiles(environment, source, work, true, |session, root| {
                let tree = session.invocation_view();
                let root_view = tree.context(root).unwrap();
                assert_eq!((root_view.rule(), root_view.parent(), root_view.invoking_state()),
                    (MULTI_SQL_STATEMENT, None, ParserState::RootSentinel));
                let children = root_view.children();
                let classes = [ContextClass::RollbackStatement, ContextClass::CommitStatement,
                    ContextClass::DiscardStatement, ContextClass::SetRoleStatement];
                for (index, (class, stop)) in classes.into_iter().zip([0, 3, 6, 10]).enumerate() {
                    let statement = tree.context(context_child(children[index * 2])).unwrap();
                    assert_eq!((statement.class(), statement.parent(), statement.invoking_state(), statement.stop().unwrap().index()),
                        (Some(class), Some(root), ParserState::State(if index == 0 { 358 } else { 360 }), stop));
                }
                let trailing = source.ends_with(';');
                let delimiters = if trailing { &[(1, 1), (3, 4), (5, 7), (7, 11)][..] } else { &[(1, 1), (3, 4), (5, 7)][..] };
                for &(child, token_index) in delimiters {
                    assert!(matches!(children[child], ContextChild::Terminal(token)
                        if token.index() == token_index && token.lexeme().raw() == ";"));
                }
                let eof = if trailing { 12 } else { 11 };
                assert_eq!(children.len(), if trailing { 9 } else { 8 });
                assert!(matches!(children.last().unwrap(), ContextChild::Terminal(token)
                    if token.index() == eof && token.lexeme().kind() == Kind::Eof));
                assert_eq!(root_view.stop().unwrap().index(), eof);
                assert_eq!(session.syntax_errors, 0);
                assert!(session.next_tokens_capture.is_none());
                assert_finished(session, eof, true);
            });
        }
    });
    assert_eq!(memory.used_bytes(), 0);

    let (result, memory) = wrapped("ROLLBACK; COMMIT ASYNC", |session, mode, _, _| {
        if mode == PredictionMode::Ll {
            assert_eq!(session.input_index(), Some(2));
            assert!(!session.matched_eof);
        }
    });
    assert_report(result, &memory, Some(356), 2, "1:10 mismatched input 'COMMIT' expecting <EOF>");
}

#[test]
fn discard_switch_bail_reports_one_live_cause_before_ll_sync_handles_the_same_prefix() {
    for source in ["DISCARD junk ALL", "DISCARD junk @"] {
        let mut modes = Vec::new();
        let (result, memory) = wrapped(source, |session, mode, first, result| {
            modes.push(mode);
            let root = InvocationKey::at(first);
            let tree = session.invocation_view();
            let statement = context_child(tree.context(root).unwrap().children()[0]);
            let target = context_child(tree.context(statement).unwrap().children()[1]);
            let discard = tree.context(target).unwrap();
            assert_eq!((discard.class(), discard.invoking_state(), discard.stop().unwrap().index()),
                (None, ParserState::State(502), 0));
            assert!(discard.children().is_empty());
            assert_eq!(tree.context(root).unwrap().stop().unwrap().index(), 0);
            assert_finished(session, 1, false);
            if mode == PredictionMode::Sll {
                assert!(matches!(result, Err(PassError::Cancelled(_))));
                let cause = tree.exception(target).unwrap();
                assert!(std::ptr::eq(cause, tree.exception(statement).unwrap()));
                assert!(std::ptr::eq(cause, tree.exception(root).unwrap()));
                let InvocationCause::NoViableAlternative(cause) = cause else { panic!("direct generated switch error"); };
                assert_eq!((cause.state, cause.start.index(), cause.offending.index()), (ParserState::State(580), 1, 1));
                assert_eq!(cause.offending.lexeme().raw(), "junk");
                assert!(matches!(&session.control, ParserControl::BailCancelled { exception, recovery: BailRecovery::Entered } if *exception == target));
                assert_eq!(session.syntax_errors, 1);
                assert!(session.buffered_token(2).is_none());
            } else {
                assert!(tree.exception(root).is_none());
                assert!(tree.exception(statement).is_none());
                assert!(tree.exception(target).is_none());
                assert!(session.buffered_token(3).is_none());
                if source.ends_with('@') {
                    assert!(session.buffered_token(2).is_none());
                    assert_eq!(session.syntax_errors, 0);
                } else {
                    assert_eq!(session.buffered_token(2).unwrap().lexeme().raw(), "ALL");
                    assert_eq!(session.syntax_errors, 1);
                }
            }
        });
        assert_eq!(modes, [PredictionMode::Sll, PredictionMode::Ll]);
        if source.ends_with('@') {
            let Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(error)))) = result else { panic!("LL LA(2) lexer error"); };
            assert_eq!(error.span().start.column, 13);
            assert_eq!(memory.used_bytes(), 0);
        } else {
            assert_report(result, &memory, None, 1, "1:8 extraneous input 'junk' expecting {'ALL', 'PLANS', 'SEQUENCES', 'TEMP', 'TEMPORARY'}");
        }
    }
}

#[test]
fn multi_loop_entry_demands_lexer_la2_and_loopback_reports_before_that_demand() {
    for (source, cursor, last) in [("ROLLBACK junk @", 1, 0), ("ROLLBACK; ROLLBACK junk @", 3, 2)] {
        let (result, memory) = environment_fixture(|environment, work, _| {
            with_parser_session(environment, source, work, |session| {
                session.profile = ParserProfile::LlDefault;
                let result = session.multi_sql_statement();
                let root = InvocationKey::at(0);
                let tree = session.invocation_view();
                let root_view = tree.context(root).unwrap();
                assert_eq!(root_view.stop().unwrap().index(), last);
                assert!(tree.exception(root).is_none());
                assert_finished(session, cursor, false);
                assert!(session.buffered_token(cursor + 1).is_none());
                assert_eq!(session.syntax_errors, if cursor == 3 { 1 } else { 0 });
                match result {
                    Err(PassError::Runtime(error)) => Err(error),
                    _ => panic!("expected LL sync error"),
                }
            })
        });
        if cursor == 3 {
            assert_report(result, &memory, None, cursor, "1:19 extraneous input 'junk' expecting {<EOF>, ';'}");
        } else {
            let Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(error)))) = result else { panic!("loop entry LA(2) lexer error"); };
            assert_eq!(error.span().start.column, 14);
            assert_eq!(memory.used_bytes(), 0);
        }
    }
}

#[test]
fn commit_optional_capture_survives_only_until_the_next_accepted_required_sync() {
    let (result, memory) = environment_fixture(|environment, work, _| {
        with_parser_session(environment, "COMMIT junk bad", work, |session| {
            session.profile = ParserProfile::LlDefault;
            let Err(PassError::Runtime(error)) = session.direct_sql_statement() else { panic!("root required mismatch"); };
            let root = InvocationKey::at(0);
            let tree = session.invocation_view();
            let commit = context_child(tree.context(root).unwrap().children()[0]);
            let capture = session.next_tokens_capture.unwrap();
            assert_eq!((capture.context, capture.state), (commit, 486));
            assert!(tree.exception(commit).is_none());
            assert!(matches!(tree.exception(root), Some(InvocationCause::InputMismatch(cause))
                if cause.state == ParserState::State(354)));
            assert_finished(session, 1, false);
            Err(error)
        })
    });
    assert_report(result, &memory, Some(354), 1, "1:7 mismatched input 'junk' expecting {<EOF>, ';'}");
}

#[test]
fn warm_multi_budget_preserves_later_child_entry_and_eof_finally_prefixes() {
    const SOURCE: &str = "ROLLBACK; ROLLBACK;";
    let (result, memory) = environment_fixture(|environment, work, memory| {
        with_bail_fallback(environment, SOURCE, work, |session, mode| {
            assert_eq!(mode, PredictionMode::Sll);
            session.multi_sql_statement().map(|_| ())
        })?;
        let records = Layout::array::<InvocationRecord<'_, '_>>(16).unwrap().size() + 256;
        for (extra, capacity) in [
            (2 * child_charge(1) + child_charge(2), 4),
            (3 * child_charge(1) + child_charge(2) + child_charge(4), 8),
        ] {
            with_parser_session(environment, SOURCE, work, |session| {
                session.profile = ParserProfile::SllBail;
                let held = memory.reserve(memory.limit_bytes() - memory.used_bytes() - records - extra).unwrap();
                let result = session.multi_sql_statement();
                let Err(PassError::Runtime(RuntimeError::Prediction(PredictionError::Admission(error)))) = result else {
                    panic!("expected multi child vector admittance failure");
                };
                assert_eq!(error.code, "rtdb/memory-budget-exceeded");
                let requested = format!("Cannot reserve {} bytes from budget ", child_charge(capacity));
                assert!(error.message.as_deref().unwrap().starts_with(requested.as_str()));
                let root = InvocationKey::at(0);
                let tree = session.invocation_view();
                let root_view = tree.context(root).unwrap();
                assert!(tree.exception(root).is_none());
                assert_eq!(session.syntax_errors, 0);
                assert!(matches!(session.control, ParserControl::Failed));
                if capacity == 4 {
                    let child = InvocationKey::at(3);
                    assert_eq!((session.current(), session.state()), (Some(root), ParserState::State(360)));
                    assert!(matches!(session.records[child.index].role, InvocationRole::OrdinaryTreeAttachmentFailed));
                    assert_eq!(tree.context(child).unwrap().stop().unwrap().index(), 1);
                    assert!(root_view.stop().is_none());
                    assert_eq!(root_view.children().len(), 2);
                    assert_eq!(session.input_index(), Some(2));
                    assert!(session.buffered_token(3).is_none());
                    assert!(!session.matched_eof);
                } else {
                    assert_eq!(root_view.children().len(), 4);
                    assert_eq!(root_view.stop().unwrap().index(), 4);
                    assert!(matches!(root_view.children()[3], ContextChild::Terminal(token)
                        if token.index() == 3 && token.lexeme().raw() == ";"));
                    assert_finished(session, 4, true);
                }
                drop(held);
                Ok(())
            })?;
        }
        Ok::<(), RuntimeError<'_>>(())
    });
    result.unwrap();
    assert_eq!(memory.used_bytes(), 0);
}
