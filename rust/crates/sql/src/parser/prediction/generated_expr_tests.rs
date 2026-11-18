use std::{alloc::Layout, sync::Arc};
use rtdb_types::{AllocationScope, MemoryBudget};
use crate::lexer::{Kind, ScanError, StreamError};
use crate::parser::grammar::{ExpectedSymbol, OwnedGrammar};
use crate::parser::prediction::{PredictionError, PredictionStorage};
use crate::parser::prediction::closure::{ClosureOptions, LoopEntryOption, SourceAssertions};
use super::*;
use super::super::{
    BailRecovery, CapturedClassPolicy, InvocationCause, InvocationRecord, ParserEnvironment, ParserProfile,
    with_bail_fallback, with_parser_session,
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

fn both_profiles<'sql>(
    environment: &mut ParserEnvironment, source: &'sql str, work: &AllocationScope,
    mut check: impl for<'parse, 'env, 'work> FnMut(&ParserSession<'parse, 'sql, 'env, 'work>, InvocationKey<'parse>),
) {
    with_bail_fallback(environment, source, work, |session, mode| {
        assert_eq!(mode, PredictionMode::Sll);
        let root = session.numeric_expression()?;
        check(session, root);
        Ok(())
    }).unwrap();
    with_parser_session(environment, source, work, |session| {
        session.profile = ParserProfile::LlDefault;
        let root = session.numeric_expression().unwrap();
        check(session, root);
        Ok(())
    }).unwrap();
}

fn child_rule<'parse>(children: &[ContextChild<'parse, '_>]) -> InvocationKey<'parse> {
    let [ContextChild::Context(child)] = children else { panic!("one child rule"); };
    *child
}

fn only_terminal(children: &[ContextChild<'_, '_>], index: usize, raw: &str) {
    assert!(matches!(children, [ContextChild::Terminal(token)] if token.index() == index && token.lexeme().raw() == raw));
}

fn number<'parse>(
    session: &ParserSession<'parse, '_, '_, '_>, value: InvocationKey<'parse>, index: usize, raw: &str, float: bool,
) {
    let tree = session.invocation_view();
    let value_view = tree.context(value).unwrap();
    assert_eq!(value_view.class(), Some(ContextClass::LiteralExpr));
    let literal = child_rule(value_view.children());
    let literal_view = tree.context(literal).unwrap();
    assert_eq!((literal_view.parent(), literal_view.invoking_state()), (Some(value), ParserState::State(1140)));
    assert_eq!((value_view.start().unwrap().index(), value_view.stop().unwrap().index()), (index, index));
    assert_eq!((literal_view.start().unwrap().index(), literal_view.stop().unwrap().index()), (index, index));
    if float {
        assert_eq!(literal_view.class(), Some(ContextClass::FloatLiteral));
        only_terminal(literal_view.children(), index, raw);
    } else {
        assert_eq!(literal_view.class(), Some(ContextClass::IntegerLiteral0));
        let integer = tree.context(child_rule(literal_view.children())).unwrap();
        assert_eq!((integer.rule(), integer.class(), integer.parent(), integer.invoking_state()),
            (INTEGER_LITERAL, None, Some(literal), ParserState::State(692)));
        only_terminal(integer.children(), index, raw);
    }
}

fn expr_value<'parse>(session: &ParserSession<'parse, '_, '_, '_>, root: InvocationKey<'parse>, last: usize) -> InvocationKey<'parse> {
    let tree = session.invocation_view();
    let expr = tree.context(root).unwrap();
    assert_eq!((expr.rule(), expr.class(), expr.parent(), expr.invoking_state()),
        (EXPR, Some(ContextClass::CommonValueExpr0), None, ParserState::RootSentinel));
    assert_eq!((expr.start().unwrap().index(), expr.stop().unwrap().index()), (0, last));
    let key = child_rule(expr.children());
    let value = tree.context(key).unwrap();
    assert_eq!((value.rule(), value.parent(), value.invoking_state()),
        (COMMON_VALUE, Some(root), ParserState::State(1000)));
    assert_eq!(session.current(), None);
    assert_eq!(session.state(), ParserState::State(1131));
    assert_eq!(session.precedence(), 0);
    assert_eq!(session.input_index(), Some(last + 1));
    assert!(!session.matched_eof);
    assert_eq!(session.syntax_errors, 0);
    key
}

#[test]
fn numeric_action_descriptors_agree_with_the_pinned_runtime_geometry() {
    let ((), memory) = environment(|environment, work, _| {
        let grammar = environment.storage.grammar_view();
        for action in [EXPR_BASE, EXPR_LOOP, EXPR_TAIL, VALUE_BASE, VALUE_LOOP, VALUE_BACKEDGE, VALUE_TAIL, LITERAL_BASE] {
            assert!(action.agrees(&grammar));
        }
        for action in [EXPR_VALUE, VALUE_LITERAL, LITERAL_INTEGER, FACTOR_RHS, TERM_RHS] {
            assert!(action.agrees(&grammar));
        }
        for action in [MATCH_FLOAT_SIGN, MATCH_FLOAT, MATCH_INTEGER_SIGN, MATCH_INTEGER, MATCH_FACTOR, MATCH_TERM] {
            assert!(action.read(&grammar).is_some());
        }
        for action in [FACTOR_PREDICATE, TERM_PREDICATE] {
            assert!(action.agrees(&grammar));
        }
        for (sync, width) in [
            (EXPR_BASE.sync, 197), (EXPR_LOOP.sync, 29), (EXPR_TAIL.sync, 28), (VALUE_BASE.sync, 196),
            (VALUE_LOOP.sync, 19), (VALUE_BACKEDGE.sync, 19), (VALUE_TAIL.sync, 18), (LITERAL_BASE.sync, 21),
            (FLOAT_SIGN, 3), (INTEGER_SIGN, 3),
        ] {
            let state = grammar.state(sync.state).unwrap();
            assert_eq!(state.rule().number(), sync.rule);
            assert!(sync.shape.agrees(state.kind()));
            let first = state.next_tokens(work).unwrap();
            assert_eq!(first.iter().count(), width);
            assert_eq!(first.contains(ExpectedSymbol::Epsilon), sync.shape.nullable());
        }
        assert!(!(PredictionAction { alternatives: 62, ..VALUE_BASE }).agrees(&grammar));
        assert!(!(CallAction { precedence: 61, ..FACTOR_RHS }).agrees(&grammar));
        assert!(!(PredicateAction { minimum: 61, ..TERM_PREDICATE }).agrees(&grammar));
        assert!(TerminalAction { target: 685, ..MATCH_FLOAT_SIGN }.read(&grammar).is_none());
    });
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn generated_expr_root_retains_raw_integer_and_float_leaves_without_eof_match() {
    let ((), memory) = environment(|environment, work, _| {
        for (sql, raw, float, suffix) in [
            ("42", "42", false, false),
            ("42;", "42", false, true),
            ("18446744073709551616", "18446744073709551616", false, false),
            ("1.25", "1.25", true, false),
            ("42;", "42", false, true),
        ] {
            both_profiles(environment, sql, work, |session, root| {
                let value = expr_value(session, root, 0);
                number(session, value, 0, raw, float);
                if session.profile == ParserProfile::LlDefault {
                    let capture = session.next_tokens_capture.unwrap();
                    assert_eq!((capture.context, capture.state), (value, 1661));
                } else {
                    assert!(session.next_tokens_capture.is_none());
                }
                let next = session.buffered_token(1).unwrap();
                if suffix { assert_eq!(next.lexeme().raw(), ";"); }
                else { assert_eq!(next.lexeme().kind(), Kind::Eof); }
            });
        }
    });
    assert_eq!(memory.used_bytes(), 0);
}

fn binary<'parse>(
    session: &ParserSession<'parse, '_, '_, '_>, value: InvocationKey<'parse>, class: ContextClass,
    parent: Option<InvocationKey<'parse>>, invoking: u32, operator: (usize, &str), bounds: (usize, usize),
) -> (InvocationKey<'parse>, InvocationKey<'parse>) {
    let tree = session.invocation_view();
    let value_view = tree.context(value).unwrap();
    assert_eq!((value_view.class(), value_view.parent(), value_view.invoking_state()),
        (Some(class), parent, ParserState::State(invoking)));
    assert_eq!((value_view.start().unwrap().index(), value_view.stop().unwrap().index()), bounds);
    let [ContextChild::Context(left), ContextChild::Terminal(token), ContextChild::Context(right)] = value_view.children()
        else { panic!("ordered arithmetic children"); };
    assert_eq!((token.index(), token.lexeme().raw()), operator);
    assert_eq!(tree.context(*left).unwrap().parent(), Some(value));
    assert_eq!(tree.context(*left).unwrap().invoking_state(), ParserState::State(68));
    assert_eq!(tree.context(*right).unwrap().parent(), Some(value));
    (*left, *right)
}

#[test]
fn generated_numeric_terms_and_factors_reuse_saved_links_at_bounded_rhs_precedence() {
    let ((), memory) = environment(|environment, work, _| {
        for _ in 0..2 {
            both_profiles(environment, "1+2*3", work, |session, root| {
                let value = expr_value(session, root, 4);
                let (one, factor) = binary(session, value, ContextClass::NumericTermExpr, Some(root), 1000, (1, "+"), (0, 4));
                let (two, three) = binary(session, factor, ContextClass::NumericFactorExpr, Some(value), 1644, (3, "*"), (2, 4));
                assert_eq!(session.invocation_view().context(three).unwrap().invoking_state(), ParserState::State(1641));
                number(session, one, 0, "1", false);
                number(session, two, 2, "2", false);
                number(session, three, 4, "3", false);
                if session.profile == ParserProfile::LlDefault {
                    let capture = session.next_tokens_capture.unwrap();
                    assert_eq!((capture.context, capture.state), (three, 1661));
                }
            });
        }
        both_profiles(environment, "8-3-1", work, |session, root| {
            let value = expr_value(session, root, 4);
            let (term, one) = binary(session, value, ContextClass::NumericTermExpr, Some(root), 1000, (3, "-"), (0, 4));
            let (eight, three) = binary(session, term, ContextClass::NumericTermExpr, Some(value), 68, (1, "-"), (0, 2));
            number(session, eight, 0, "8", false);
            number(session, three, 2, "3", false);
            number(session, one, 4, "1", false);
        });
        both_profiles(environment, "8/3%2", work, |session, root| {
            let value = expr_value(session, root, 4);
            let (factor, two) = binary(session, value, ContextClass::NumericFactorExpr, Some(root), 1000, (3, "%"), (0, 4));
            let (eight, three) = binary(session, factor, ContextClass::NumericFactorExpr, Some(value), 68, (1, "/"), (0, 2));
            number(session, eight, 0, "8", false);
            number(session, three, 2, "3", false);
            number(session, two, 4, "2", false);
        });
    });
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn generated_expr_error_retry_distinguishes_ll_deletion_mismatch_and_lexer_demand() {
    for (sql, mismatch) in [(") 42", false), (") ;", true), (") @", false)] {
        let (result, memory) = environment(|environment, work, _| {
            let mut passes = Vec::new();
            let result = with_bail_fallback(environment, sql, work, |session, mode| {
                let root = InvocationKey::at(session.records.len());
                let action = session.numeric_expression();
                passes.push(mode);
                let tree = session.invocation_view();
                let context = tree.context(root).unwrap();
                assert!(context.class().is_none());
                assert_eq!(context.start().unwrap().index(), 0);
                assert!(context.stop().is_none());
                assert!(context.children().is_empty());
                assert_eq!((session.current(), session.precedence(), session.input_index()), (None, 0, Some(0)));
                if mode == PredictionMode::Sll {
                    assert!(matches!(action, Err(PassError::Cancelled(_))));
                    let Some(InvocationCause::NoViableAlternative(cause)) = tree.exception(root) else {
                        panic!("actual d89 failure");
                    };
                    assert_eq!((cause.state, cause.start.index(), cause.offending.index()), (ParserState::State(1001), 0, 0));
                    assert!(matches!(&session.control, ParserControl::BailCancelled { exception, recovery: BailRecovery::Entered } if *exception == root));
                    assert_eq!(session.syntax_errors, 1);
                    assert!(session.buffered_token(1).is_none());
                } else {
                    assert_eq!(session.syntax_errors, if sql.ends_with('@') { 0 } else { 1 });
                    if mismatch {
                        assert!(matches!(tree.exception(root), Some(InvocationCause::InputMismatch(cause))
                            if cause.state == ParserState::State(1001) && cause.offending.index() == 0));
                    } else {
                        assert!(tree.exception(root).is_none());
                    }
                    assert_eq!(session.buffered_token(1).is_some(), !sql.ends_with('@'));
                    assert!(session.buffered_token(2).is_none());
                    assert!(session.next_tokens_capture.is_none());
                }
                action.map(|_| ())
            });
            assert_eq!(passes, [PredictionMode::Sll, PredictionMode::Ll]);
            result
        });
        if sql.ends_with('@') {
            let Err(RuntimeError::Input(StreamError::Scan(ScanError::Recognition(error)))) = result else {
                panic!("LL LA(2) lexer error");
            };
            assert_eq!(error.span().start.column, 2);
        } else {
            let Err(RuntimeError::ReportedTokenSyntax(report)) = result else { panic!("LL token report"); };
            assert_eq!(report.cause().map(|cause| cause.state), mismatch.then_some(ParserState::State(1001)));
            assert_eq!(report.offending().index(), 0);
            assert_eq!(report.offending().lexeme().raw(), ")");
            assert_eq!(report.anomaly().code, "xtdb/sql-error");
            let prefix = if mismatch { "mismatched" } else { "extraneous" };
            let prefix = format!("Errors parsing SQL statement:\n  - line 1:0 {prefix} input ')' expecting ");
            assert!(report.anomaly().message.as_deref().unwrap().starts_with(prefix.as_str()));
            drop(report);
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn unadopted_recursive_base_choices_and_select_leave_their_private_scope() {
    let ((), memory) = environment(|environment, work, _| {
        for (sql, state) in [("NOT 42", 1001), ("((42))", 1634), ("- - 42", 1634)] {
            let mut calls = 0;
            let result = with_bail_fallback(environment, sql, work, |session, mode| {
                calls += 1;
                assert_eq!(mode, PredictionMode::Sll);
                let action = session.numeric_expression();
                assert!(matches!(action, Err(PassError::Runtime(RuntimeError::Control(RuntimeProblem::PredictionSite)))));
                assert_eq!((session.current(), session.state(), session.precedence(), session.input_index()),
                    (None, ParserState::State(state), 0, Some(0)));
                assert_eq!(session.syntax_errors, 0);
                action.map(|_| ())
            });
            assert!(matches!(result, Err(RuntimeError::Control(RuntimeProblem::PredictionSite))));
            assert_eq!(calls, 1);
        }
        let result = with_bail_fallback(environment, "SELECT 42", work, |session, mode| {
            assert_eq!(mode, PredictionMode::Sll);
            let action = session.direct_sql_statement();
            assert!(matches!(action, Err(PassError::Runtime(RuntimeError::Control(RuntimeProblem::PredictionSite)))));
            assert_eq!(session.records.len(), 2);
            assert!(session.records.iter().all(|record| record.class.is_none()));
            action.map(|_| ())
        });
        assert!(matches!(result, Err(RuntimeError::Control(RuntimeProblem::PredictionSite))));
    });
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn warm_numeric_unroll_preserves_actual_literal_residue_and_restored_caller_on_denial() {
    let (result, memory) = environment(|environment, work, memory| {
        with_bail_fallback(environment, "42", work, |session, mode| {
            assert_eq!(mode, PredictionMode::Sll);
            session.numeric_expression().map(|_| ())
        })?;
        let arena = Layout::array::<InvocationRecord<'_, '_>>(16).unwrap().size() + 256;
        let child = Layout::new::<ContextChild<'_, '_>>().size() + 256;
        with_parser_session(environment, "42", work, |session| {
            session.profile = ParserProfile::SllBail;
            let before = memory.used_bytes();
            let root = session.numeric_expression().unwrap();
            expr_value(session, root, 0);
            assert_eq!(memory.used_bytes() - before, arena + 4 * child);
            Ok(())
        })?;
        for slots in [0, 1, 3] {
            with_parser_session(environment, "42", work, |session| {
                session.profile = ParserProfile::SllBail;
                let held = memory.reserve(memory.limit_bytes() - memory.used_bytes() - arena - slots * child).unwrap();
                let result = session.numeric_expression();
                let Err(PassError::Runtime(RuntimeError::Prediction(PredictionError::Admission(error)))) = result else {
                    panic!("expected child vector denial");
                };
                assert_eq!(error.code, "rtdb/memory-budget-exceeded");
                let prefix = format!("Cannot reserve {child} bytes from budget ");
                assert!(error.message.as_deref().unwrap().starts_with(prefix.as_str()));
                assert!(matches!(session.control, ParserControl::Failed));
                assert_eq!((session.current(), session.precedence(), session.syntax_errors), (None, 0, 0));
                assert!(!session.matched_eof);
                let tree = session.invocation_view();
                let expr = InvocationKey::at(1);
                let expr_view = tree.context(expr).unwrap();
                assert!(expr_view.children().is_empty());
                assert!(matches!(session.records[0].role, InvocationRole::RecursiveEntry { phase: RecursionPhase::Closed, .. }));
                assert!(matches!(session.records[2].role, InvocationRole::RecursiveEntry { phase: RecursionPhase::Closed, .. }));
                match slots {
                    0 => {
                        let literal = InvocationKey::at(4);
                        let residue = tree.context(literal).unwrap();
                        assert_eq!(residue.parent(), Some(expr));
                        assert_eq!(residue.class(), None);
                        assert!(residue.stop().is_none());
                        assert!(matches!(session.records[literal.index].role, InvocationRole::OrdinaryTreeAttachmentFailed));
                        assert_eq!((session.state(), session.input_index()), (ParserState::State(28), Some(0)));
                        assert!(expr_view.stop().is_none());
                    }
                    1 => {
                        let literal = InvocationKey::at(5);
                        let residue = tree.context(literal).unwrap();
                        assert_eq!((residue.parent(), residue.class()), (Some(expr), Some(ContextClass::IntegerLiteral0)));
                        let integer = &session.records[6];
                        assert_eq!(integer.parent, Some(literal));
                        assert!(integer.stop.is_none());
                        assert!(matches!(integer.role, InvocationRole::OrdinaryTreeAttachmentFailed));
                        assert_eq!((session.state(), session.input_index()), (ParserState::State(692), Some(0)));
                        assert!(expr_view.stop().is_none());
                    }
                    3 => {
                        let value = InvocationKey::at(3);
                        assert_eq!(tree.context(value).unwrap().parent(), Some(expr));
                        assert_eq!(expr_view.stop().unwrap().index(), 0);
                        number(session, value, 0, "42", false);
                        assert_eq!((session.state(), session.input_index()), (ParserState::State(1661), Some(1)));
                    }
                    _ => unreachable!("chosen admission prefix"),
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
