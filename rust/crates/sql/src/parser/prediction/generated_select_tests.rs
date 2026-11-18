use std::{alloc::Layout, sync::Arc};
use rtdb_types::{AllocationScope, MemoryBudget};
use crate::lexer::{Kind, ScanError, StreamError};
use crate::parser::grammar::{ExpectedSymbol, OwnedGrammar};
use crate::parser::prediction::{PredictionError, PredictionStorage};
use crate::parser::prediction::closure::{ClosureOptions, LoopEntryOption, SourceAssertions};
use super::*;
use super::super::{
    BailRecovery, CapturedClassPolicy, InvocationCause, InvocationRecord, ParserEnvironment, ParserProfile,
    RuleContextView, with_bail_fallback, with_parser_session,
};

fn environment<T>(run: impl FnOnce(&mut ParserEnvironment, &AllocationScope, &Arc<MemoryBudget>) -> T) -> (T, Arc<MemoryBudget>) {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
    let retained = MemoryBudget::new(64 << 20);
    let memory = MemoryBudget::new(64 << 20);
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

fn either_root<'parse, 'sql>(
    session: &mut ParserSession<'parse, 'sql, '_, '_>, multi: bool,
) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
    if multi { session.multi_sql_statement() } else { session.direct_sql_statement() }
}

fn both_profiles<'sql>(
    environment: &mut ParserEnvironment, source: &'sql str, work: &AllocationScope, multi: bool,
    mut check: impl for<'parse, 'env, 'work> FnMut(&ParserSession<'parse, 'sql, 'env, 'work>, InvocationKey<'parse>),
) {
    with_bail_fallback(environment, source, work, |session, mode| {
        assert_eq!(mode, PredictionMode::Sll);
        let root = either_root(session, multi)?;
        check(session, root);
        Ok(())
    }).unwrap();
    with_parser_session(environment, source, work, |session| {
        session.profile = ParserProfile::LlDefault;
        let root = either_root(session, multi).unwrap();
        check(session, root);
        Ok(())
    }).unwrap();
}

fn child<'a, 'parse, 'sql>(
    session: &'a ParserSession<'parse, 'sql, '_, '_>, parent: InvocationKey<'parse>, index: usize,
    rule: u16, invoking: u32,
) -> RuleContextView<'a, 'parse, 'sql> {
    let tree = session.invocation_view();
    let view = tree.context(parent).unwrap();
    let ContextChild::Context(key) = view.children()[index] else { panic!("child rule"); };
    let view = tree.context(key).unwrap();
    assert_eq!((view.parent(), view.rule(), view.invoking_state()),
        (Some(parent), rule, ParserState::State(invoking)));
    view
}

fn terminal(value: ContextChild<'_, '_>, index: usize, spelling: &str) {
    assert!(matches!(value, ContextChild::Terminal(token) if token.index() == index && token.lexeme().raw() == spelling));
}

fn bounds(view: RuleContextView<'_, '_, '_>, start: usize, stop: usize) {
    assert_eq!((view.start().unwrap().index(), view.stop().unwrap().index()), (start, stop));
}

fn complete_root<'parse>(session: &ParserSession<'parse, '_, '_, '_>, root: InvocationKey<'parse>, rule: u16, eof: usize) {
    let tree = session.invocation_view();
    let root = tree.context(root).unwrap();
    assert_eq!((root.parent(), root.rule(), root.invoking_state()), (None, rule, ParserState::RootSentinel));
    bounds(root, 0, eof);
    assert!(matches!(root.children().last(), Some(ContextChild::Terminal(token))
        if token.index() == eof && token.lexeme().kind() == Kind::Eof));
    assert_eq!((session.current(), session.state(), session.precedence()), (None, ParserState::RootSentinel, 0));
    assert_eq!(session.input_index(), Some(eof));
    assert!(session.matched_eof);
    assert!(session.next_tokens_capture.is_none());
    assert_eq!(session.syntax_errors, 0);
}

fn projection<'a, 'parse, 'sql>(
    session: &'a ParserSession<'parse, 'sql, '_, '_>, root: InvocationKey<'parse>, child_index: usize,
    invocation: u32, explain_count: usize, quantifier: bool,
) -> (RuleContextView<'a, 'parse, 'sql>, RuleContextView<'a, 'parse, 'sql>, RuleContextView<'a, 'parse, 'sql>) {
    let statement = child(session, root, child_index, STATEMENT, invocation);
    assert_eq!(statement.class(), Some(ContextClass::QueryExpr));
    let query = child(session, statement.key(), explain_count, QUERY_EXPRESSION, 380);
    let no_with = child(session, query.key(), 0, QUERY_NO_WITH, 2420);
    let body = child(session, no_with.key(), 0, QUERY_BODY, 2422);
    assert_eq!(body.class(), Some(ContextClass::QuerySpecification));
    let clause = child(session, body.key(), 0, SELECT_CLAUSE, 2455);
    let list = child(session, clause.key(), if quantifier { 2 } else { 1 }, SELECT_LIST, 2301);
    (statement, clause, list)
}

fn derived<'a, 'parse, 'sql>(
    session: &'a ParserSession<'parse, 'sql, '_, '_>, list: InvocationKey<'parse>, index: usize, first: bool,
) -> RuleContextView<'a, 'parse, 'sql> {
    let sublist = child(session, list, index, SELECT_SUBLIST, if first { 2313 } else { 2317 });
    child(session, sublist.key(), 0, DERIVED_COLUMN, 2338)
}

fn integer<'parse>(session: &ParserSession<'parse, '_, '_, '_>, column: InvocationKey<'parse>, index: usize, raw: &str) {
    let expr = child(session, column, 0, 33, 2405);
    assert_eq!(expr.class(), Some(ContextClass::CommonValueExpr0));
    bounds(expr, index, index);
    let value = child(session, expr.key(), 0, 34, 1000);
    assert_eq!(value.class(), Some(ContextClass::LiteralExpr));
    let literal = child(session, value.key(), 0, 14, 1140);
    assert_eq!(literal.class(), Some(ContextClass::IntegerLiteral0));
    let integer_rule = child(session, literal.key(), 0, 13, 692);
    assert_eq!(integer_rule.children().len(), 1);
    terminal(integer_rule.children()[0], index, raw);
}

#[test]
fn typed_select_sites_check_calls_kind_token_sets_and_linked_repeat_decision() {
    let ((), memory) = environment(|environment, work, _| {
        let grammar = environment.storage.grammar_view();
        for action in [
            QUERY_LOOP, QUERY_COMBINATION, MAY_QUANTIFIER, LIST_BASE, FIRST_ITEM, LIST_LOOP, LIST_BACKEDGE,
            NEXT_ITEM, TRAILING_STAR, SUBLIST_BASE, MAY_ALIAS,
        ].into_iter().chain(QUERY_TAILS.map(|(action, _)| action)) {
            assert!(action.agrees(&grammar));
            let first = grammar.state(action.sync.state).unwrap().next_tokens(work).unwrap();
            assert_eq!(first.contains(ExpectedSymbol::Epsilon), action.sync.shape.nullable());
        }
        for action in [
            SETTING_CALL, QUERY_CALL, WITH_CALL, NO_WITH_CALL, QUERY_BODY_CALL, ORDER_CALL, OFFSET_CALL,
            QUERY_SELECT, QUANTIFIER_CALL, SELECT_LIST_CALL, FIRST_STAR, FIRST_ITEM_CALL, NEXT_ITEM_CALL,
            TRAILING_STAR_CALL, DERIVED_CALL, QUALIFIED_STAR_CALL, AS_CALL, EXPLICIT_LABEL_CALL,
            IMPLICIT_LABEL_CALL, LABEL_IDENTIFIER,
        ].into_iter().chain(QUERY_TAILS.map(|(_, action)| action)) {
            assert!(action.agrees(&grammar));
        }
        for action in [EXPLAIN, ANALYZE, SELECT, QUANTIFIER_SET, LIST_COMMA, TRAILING_COMMA, AS, DEFAULT_ALIAS, CURRENT_USER_LABEL] {
            assert!(action.read(&grammar).is_some());
        }
        for (action, width) in [(QUERY_BASE, 6), (ALIAS_BASE, 115), (EXPLICIT_ALIAS, 115), (LABEL_BASE, 114)] {
            let state = grammar.state(action.state).unwrap();
            assert_eq!(state.rule().number(), action.rule);
            assert!(action.shape.agrees(state.kind()));
            let tokens = state.next_tokens(work).unwrap();
            assert_eq!(tokens.iter().count(), width);
            assert!(!tokens.contains(ExpectedSymbol::Epsilon));
        }
        assert!(!(PredictionAction { decision_state: 2324, ..LIST_BACKEDGE }).agrees(&grammar));
    });
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn direct_select_uses_the_same_numeric_expression_and_generated_identifier_as_its_children() {
    let ((), memory) = environment(|environment, work, _| {
        for (sql, last, alias) in [
            ("SELECT 42", 1, None),
            ("SELECT 42 AS answer", 3, Some(("answer", true))),
            ("SELECT 42 answer", 2, Some(("answer", false))),
            ("SELECT 42 AS DEFAULT", 3, Some(("DEFAULT", true))),
            ("SELECT 42 AS CURRENT_USER", 3, Some(("CURRENT_USER", true))),
            ("SELECT 42 AS answer", 3, Some(("answer", true))),
        ] {
            both_profiles(environment, sql, work, false, |session, root| {
                let (statement, clause, list) = projection(session, root, 0, 352, 0, false);
                bounds(statement, 0, last);
                terminal(clause.children()[0], 0, "SELECT");
                let column = derived(session, list.key(), 0, true);
                integer(session, column.key(), 1, "42");
                if let Some((raw, explicit)) = alias {
                    let as_clause = child(session, column.key(), 1, AS_CLAUSE, 2406);
                    if explicit { terminal(as_clause.children()[0], 2, "AS"); }
                    let offset = if explicit { 1 } else { 0 };
                    if raw == "DEFAULT" {
                        terminal(as_clause.children()[offset], last, raw);
                    } else {
                        let label = child(session, as_clause.key(), offset, COLUMN_LABEL, if explicit { 2410 } else { 2414 });
                        if raw == "CURRENT_USER" {
                            terminal(label.children()[0], last, raw);
                        } else {
                            let identifier = child(session, label.key(), 0, 22, 852);
                            assert_eq!(identifier.class(), Some(ContextClass::RegularIdentifier));
                            terminal(identifier.children()[0], last, raw);
                        }
                    }
                } else {
                    assert_eq!(column.children().len(), 1);
                }
                complete_root(session, root, 0, last + 1);
            });
        }
    });
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn explain_quantifier_and_arithmetic_keep_actual_indexed_tree_order() {
    let ((), memory) = environment(|environment, work, _| {
        let sql = "EXPLAIN ANALYZE SELECT DISTINCT 1+2*3 AS \"sum\"";
        both_profiles(environment, sql, work, false, |session, root| {
            let (statement, clause, list) = projection(session, root, 0, 352, 2, true);
            terminal(statement.children()[0], 0, "EXPLAIN");
            terminal(statement.children()[1], 1, "ANALYZE");
            terminal(clause.children()[0], 2, "SELECT");
            let quantifier = child(session, clause.key(), 1, QUANTIFIER, 2298);
            terminal(quantifier.children()[0], 3, "DISTINCT");
            let column = derived(session, list.key(), 0, true);
            let expr = child(session, column.key(), 0, 33, 2405);
            bounds(expr, 4, 8);
            let term = child(session, expr.key(), 0, 34, 1000);
            assert_eq!(term.class(), Some(ContextClass::NumericTermExpr));
            terminal(term.children()[1], 5, "+");
            let factor = child(session, term.key(), 2, 34, 1644);
            assert_eq!(factor.class(), Some(ContextClass::NumericFactorExpr));
            terminal(factor.children()[1], 7, "*");
            let as_clause = child(session, column.key(), 1, AS_CLAUSE, 2406);
            terminal(as_clause.children()[0], 9, "AS");
            let label = child(session, as_clause.key(), 1, COLUMN_LABEL, 2410);
            let name = child(session, label.key(), 0, 22, 852);
            assert_eq!(name.class(), Some(ContextClass::DelimitedIdentifier));
            terminal(name.children()[0], 10, "\"sum\"");
            complete_root(session, root, 0, 11);
        });
    });
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn select_list_runs_first_repeat_and_omitted_sublists_through_real_decisions() {
    let ((), memory) = environment(|environment, work, _| {
        both_profiles(environment, "SELECT 42, 7 AS COUNT", work, false, |session, root| {
            let (_, _, list) = projection(session, root, 0, 352, 0, false);
            assert_eq!(list.children().len(), 3);
            let first = derived(session, list.key(), 0, true);
            integer(session, first.key(), 1, "42");
            terminal(list.children()[1], 2, ",");
            let next = derived(session, list.key(), 2, false);
            integer(session, next.key(), 3, "7");
            let as_clause = child(session, next.key(), 1, AS_CLAUSE, 2406);
            let label = child(session, as_clause.key(), 1, COLUMN_LABEL, 2410);
            let id = child(session, label.key(), 0, 22, 852);
            let keyword = child(session, id.key(), 0, 23, 755);
            let function = child(session, keyword.key(), 0, 147, 797);
            terminal(function.children()[0], 5, "COUNT");
            complete_root(session, root, 0, 6);
        });
        both_profiles(environment, "SELECT ,42,,7,", work, false, |session, root| {
            let (_, _, list) = projection(session, root, 0, 352, 0, false);
            assert_eq!(list.children().len(), 6);
            terminal(list.children()[0], 1, ",");
            terminal(list.children()[2], 3, ",");
            terminal(list.children()[3], 4, ",");
            terminal(list.children()[5], 6, ",");
            integer(session, derived(session, list.key(), 1, false).key(), 2, "42");
            integer(session, derived(session, list.key(), 4, false).key(), 5, "7");
            complete_root(session, root, 0, 7);
        });
        both_profiles(environment, "SELECT", work, false, |session, root| {
            let (_, _, list) = projection(session, root, 0, 352, 0, false);
            assert!(list.children().is_empty());
            complete_root(session, root, 0, 1);
        });
    });
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn select_and_control_share_both_statement_root_modes_and_final_eof() {
    let ((), memory) = environment(|environment, work, _| {
        for _ in 0..2 {
            both_profiles(environment, "SELECT 42 AS answer; ROLLBACK; SELECT ALL 7;", work, true, |session, root| {
                let (first, _, _) = projection(session, root, 0, 358, 0, false);
                bounds(first, 0, 3);
                let root_view = session.invocation_view().context(root).unwrap();
                terminal(root_view.children()[1], 4, ";");
                let rollback = child(session, root, 2, STATEMENT, 360);
                assert_eq!(rollback.class(), Some(ContextClass::RollbackStatement));
                terminal(root_view.children()[3], 6, ";");
                let (last, clause, last_list) = projection(session, root, 4, 360, 0, true);
                bounds(last, 7, 9);
                terminal(child(session, clause.key(), 1, QUANTIFIER, 2298).children()[0], 8, "ALL");
                integer(session, derived(session, last_list.key(), 0, true).key(), 9, "7");
                terminal(root_view.children()[5], 10, ";");
                complete_root(session, root, 1, 11);
            });
        }
    });
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn declined_trailing_asterisk_keeps_its_separate_generated_comma_and_actual_current_token() {
    let ((), memory) = environment(|environment, work, _| {
        let source = "SELECT 42, *";
        for profile in [ParserProfile::SllBail, ParserProfile::LlDefault] {
            with_parser_session(environment, source, work, |session| {
                session.profile = profile;
                let action = session.direct_sql_statement();
                assert!(matches!(action, Err(PassError::Runtime(RuntimeError::Control(RuntimeProblem::PredictionSite)))));
                let root = InvocationKey::at(0);
                let (_, _, list) = projection(session, root, 0, 352, 0, false);
                assert_eq!(list.children().len(), 2);
                terminal(list.children()[1], 2, ",");
                assert_eq!(session.buffered_token(3).unwrap().lexeme().raw(), "*");
                assert_eq!((session.current(), session.input_index(), session.syntax_errors), (None, Some(3), 0));
                assert_eq!(session.invocation_view().context(root).unwrap().stop().unwrap().index(), 2);
                assert!(!session.matched_eof);
                Ok(())
            }).unwrap();
        }
    });
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn select_match_consume_lexer_error_keeps_query_labels_without_a_select_terminal() {
    let (result, memory) = environment(|environment, work, _| {
        let mut calls = 0;
        let result = with_bail_fallback(environment, "SELECT @", work, |session, mode| {
            calls += 1;
            assert_eq!(mode, PredictionMode::Sll);
            let action = session.direct_sql_statement();
            let statement = child(session, InvocationKey::at(0), 0, STATEMENT, 352);
            let query = child(session, statement.key(), 0, QUERY_EXPRESSION, 380);
            let no_with = child(session, query.key(), 0, QUERY_NO_WITH, 2420);
            let body = child(session, no_with.key(), 0, QUERY_BODY, 2422);
            assert_eq!(body.class(), Some(ContextClass::QuerySpecification));
            let clause = child(session, body.key(), 0, SELECT_CLAUSE, 2455);
            assert!(clause.children().is_empty());
            assert!(clause.stop().is_none());
            assert_eq!((session.current(), session.input_index(), session.syntax_errors), (None, Some(0), 0));
            assert!(session.buffered_token(1).is_none());
            action.map(|_| ())
        });
        assert_eq!(calls, 1);
        result
    });
    let Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(error)))) = result else { panic!("lexer error"); };
    assert_eq!(error.span().start.column, 7);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn warmed_query_unroll_uses_direct_and_nested_actual_select_clause_residue() {
    let (result, memory) = environment(|environment, work, memory| {
        with_bail_fallback(environment, "SELECT 42", work, |session, mode| {
            assert_eq!(mode, PredictionMode::Sll);
            session.direct_sql_statement().map(|_| ())
        })?;
        let arena = Layout::array::<InvocationRecord<'_, '_>>(16).unwrap().size() + 256;
        let one = Layout::array::<ContextChild<'_, '_>>(1).unwrap().size() + 256;
        let two = Layout::array::<ContextChild<'_, '_>>(2).unwrap().size() + 256;
        for (extra, failed_child, cursor) in [(3 * one, 7, 0), (5 * one, 8, 1), (6 * one + two, 10, 1)] {
            with_parser_session(environment, "SELECT 42", work, |session| {
                session.profile = ParserProfile::SllBail;
                let held = memory.reserve(memory.limit_bytes() - memory.used_bytes() - arena - extra).unwrap();
                let action = session.direct_sql_statement();
                let Err(PassError::Runtime(RuntimeError::Prediction(PredictionError::Admission(error)))) = action
                    else { panic!("actual query child vector denial"); };
                assert_eq!(error.code, "rtdb/memory-budget-exceeded");
                let prefix = format!("Cannot reserve {one} bytes from budget ");
                assert!(error.message.as_deref().unwrap().starts_with(prefix.as_str()));
                assert_eq!((session.current(), session.state(), session.precedence()), (None, ParserState::RootSentinel, 0));
                assert_eq!(session.input_index(), Some(cursor));
                assert_eq!(session.syntax_errors, 0);
                assert!(!session.matched_eof);
                assert!(matches!(session.records[5].role, InvocationRole::RecursiveEntry { phase: RecursionPhase::Closed, .. }));
                let clause = &session.records[7];
                assert_eq!(clause.parent, Some(InvocationKey::at(4)));
                assert_eq!(clause.invoking, ParserState::State(2455));
                assert_eq!(clause.stop.map(crate::lexer::IndexedLexeme::index), if cursor == 0 { None } else { Some(0) });
                assert!(matches!(session.records[failed_child].role, InvocationRole::OrdinaryTreeAttachmentFailed));
                assert!(session.records[4].children.as_ref().unwrap().is_empty());
                drop(held);
                Ok(())
            })?;
        }
        Ok::<(), RuntimeError<'_>>(())
    });
    result.unwrap();
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn explicit_alias_retry_preserves_the_generated_switch_cause_and_ll_demand_order() {
    for source in ["SELECT 42 AS ) answer", "SELECT 42 AS ) ;", "SELECT 42 AS ) @"] {
        let (result, memory) = environment(|environment, work, _| {
            let mut passes = Vec::new();
            let result = with_bail_fallback(environment, source, work, |session, mode| {
                let root = InvocationKey::at(session.records.len());
                let action = session.direct_sql_statement();
                passes.push(mode);
                let (_, _, list) = projection(session, root, 0, 352, 0, false);
                let derived = derived(session, list.key(), 0, true);
                let alias = child(session, derived.key(), 1, AS_CLAUSE, 2406);
                assert_eq!(alias.children().len(), 1);
                terminal(alias.children()[0], 2, "AS");
                bounds(alias, 2, 2);
                assert_eq!((session.current(), session.precedence(), session.input_index()), (None, 0, Some(3)));
                assert!(!session.matched_eof);
                let tree = session.invocation_view();
                if mode == PredictionMode::Sll {
                    assert!(matches!(action, Err(PassError::Cancelled(_))));
                    let cause = tree.exception(alias.key()).unwrap();
                    assert!(std::ptr::eq(cause, tree.exception(root).unwrap()));
                    let InvocationCause::NoViableAlternative(cause) = cause else { panic!("actual alias switch error"); };
                    assert_eq!((cause.state, cause.start.index(), cause.offending.index()), (ParserState::State(2412), 3, 3));
                    assert!(matches!(&session.control, ParserControl::BailCancelled { exception, recovery: BailRecovery::Entered }
                        if *exception == alias.key()));
                    assert_eq!(session.syntax_errors, 1);
                    assert!(session.buffered_token(4).is_none());
                } else {
                    assert!(tree.exception(root).is_none());
                    assert!(session.buffered_token(5).is_none());
                    if source.ends_with(';') {
                        assert!(matches!(tree.exception(alias.key()), Some(InvocationCause::InputMismatch(cause))
                            if cause.state == ParserState::State(2412)));
                    } else {
                        assert!(tree.exception(alias.key()).is_none());
                    }
                    assert_eq!(session.buffered_token(4).is_some(), !source.ends_with('@'));
                    assert_eq!(session.syntax_errors, if source.ends_with('@') { 0 } else { 1 });
                }
                action.map(|_| ())
            });
            assert_eq!(passes, [PredictionMode::Sll, PredictionMode::Ll]);
            result
        });
        if source.ends_with('@') {
            let Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(error)))) = result else {
                panic!("LL alias LA(2) lexer error");
            };
            assert_eq!(error.span().start.column, 15);
        } else {
            let Err(RuntimeError::ReportedTokenSyntax(report)) = result else { panic!("LL alias report"); };
            assert_eq!(report.offending().index(), 3);
            assert_eq!(report.offending().lexeme().raw(), ")");
            assert_eq!(report.cause().map(|cause| cause.state), source.ends_with(';').then_some(ParserState::State(2412)));
            assert_eq!(report.anomaly().code, "xtdb/sql-error");
            drop(report);
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn ll_list_capture_keeps_the_live_entry_or_backedge_context_through_outer_reporting() {
    for (source, cursor, state) in [
        ("SELECT 42 AS answer)", 4, 2322),
        ("SELECT 42, 7 AS answer)", 6, 2324),
    ] {
        let (result, memory) = environment(|environment, work, _| {
            with_parser_session(environment, source, work, |session| {
                session.profile = ParserProfile::LlDefault;
                let Err(PassError::Runtime(error)) = session.direct_sql_statement() else {
                    panic!("outer required sync error");
                };
                let root = InvocationKey::at(0);
                let (_, _, list) = projection(session, root, 0, 352, 0, false);
                let capture = session.next_tokens_capture.unwrap();
                assert_eq!((capture.context, capture.state), (list.key(), state));
                let root_view = session.invocation_view().context(root).unwrap();
                assert_eq!(root_view.stop().unwrap().index(), cursor - 1);
                assert_eq!(session.input_index(), Some(cursor));
                assert_eq!(session.buffered_token(cursor + 1).unwrap().lexeme().kind(), Kind::Eof);
                assert!(!session.matched_eof);
                Err::<(), _>(error)
            })
        });
        let Err(RuntimeError::ReportedTokenSyntax(report)) = result else { panic!("root deletion report"); };
        assert!(report.cause().is_none());
        assert_eq!((report.offending().index(), report.offending().lexeme().raw()), (cursor, ")"));
        drop(report);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn fixed_scalar_projections_share_the_generated_direct_and_multiple_statement_roots() {
    let ((), memory) = environment(|environment, work, _| {
        both_profiles(environment, "SELECT NULL AS x;", work, false, |session, root| {
            let (_, _, list) = projection(session, root, 0, 352, 0, false);
            let column = derived(session, list.key(), 0, true);
            let expr = child(session, column.key(), 0, 33, 2405);
            let value = child(session, expr.key(), 0, 34, 1000);
            let literal = child(session, value.key(), 0, 14, 1140);
            assert_eq!(literal.class(), Some(ContextClass::NullLiteral));
            terminal(literal.children()[0], 1, "NULL");
            let alias = child(session, column.key(), 1, AS_CLAUSE, 2406);
            terminal(alias.children()[0], 2, "AS");
            let root_view = session.invocation_view().context(root).unwrap();
            terminal(root_view.children()[1], 4, ";");
            complete_root(session, root, 0, 5);
        });
        let source = r"SELECT NuLl AS absent_value, TrUe, fAlSe, oN, oFf, 'a''b' plain_value, E'\uD800' AS escaped_value; SELECT FALSE;";
        both_profiles(environment, source, work, true, |session, root| {
            let (statement, _, list) = projection(session, root, 0, 358, 0, false);
            bounds(statement, 0, 18);
            assert_eq!(list.children().len(), 13);
            for (column_index, (token, raw, literal_class, string_class)) in [
                (1, "NuLl", ContextClass::NullLiteral, None),
                (5, "TrUe", ContextClass::BooleanLiteral, None),
                (7, "fAlSe", ContextClass::BooleanLiteral, None),
                (9, "oN", ContextClass::BooleanLiteral, None),
                (11, "oFf", ContextClass::BooleanLiteral, None),
                (13, "'a''b'", ContextClass::CharacterStringLiteral, Some(ContextClass::SqlStandardString)),
                (16, r"E'\uD800'", ContextClass::CharacterStringLiteral, Some(ContextClass::CEscapesString)),
            ].into_iter().enumerate() {
                let column = derived(session, list.key(), column_index * 2, column_index == 0);
                let expr = child(session, column.key(), 0, 33, 2405);
                bounds(expr, token, token);
                let value = child(session, expr.key(), 0, 34, 1000);
                let literal = child(session, value.key(), 0, 14, 1140);
                assert_eq!(literal.class(), Some(literal_class));
                let leaf = if let Some(string_class) = string_class {
                    let string = child(session, literal.key(), 0, 16, 693);
                    assert_eq!(string.class(), Some(string_class));
                    string
                } else {
                    literal
                };
                assert_eq!(leaf.children().len(), 1);
                terminal(leaf.children()[0], token, raw);
                let (name, name_index, as_index) = match column_index {
                    0 => ("absent_value", 3, Some(2)),
                    5 => ("plain_value", 14, None),
                    6 => ("escaped_value", 18, Some(17)),
                    _ => {
                        assert_eq!(column.children().len(), 1);
                        continue;
                    }
                };
                let alias = child(session, column.key(), 1, AS_CLAUSE, 2406);
                let label = if let Some(as_index) = as_index {
                    terminal(alias.children()[0], as_index, "AS");
                    child(session, alias.key(), 1, COLUMN_LABEL, 2410)
                } else {
                    child(session, alias.key(), 0, COLUMN_LABEL, 2414)
                };
                let identifier = child(session, label.key(), 0, 22, 852);
                terminal(identifier.children()[0], name_index, name);
            }
            let (_, _, second_list) = projection(session, root, 2, 360, 0, false);
            let second = derived(session, second_list.key(), 0, true);
            let expr = child(session, second.key(), 0, 33, 2405);
            let value = child(session, expr.key(), 0, 34, 1000);
            let literal = child(session, value.key(), 0, 14, 1140);
            assert_eq!(literal.class(), Some(ContextClass::BooleanLiteral));
            terminal(literal.children()[0], 21, "FALSE");
            let root_view = session.invocation_view().context(root).unwrap();
            terminal(root_view.children()[1], 19, ";");
            terminal(root_view.children()[3], 22, ";");
            complete_root(session, root, 1, 23);
        });
    });
    assert_eq!(memory.used_bytes(), 0);
}
