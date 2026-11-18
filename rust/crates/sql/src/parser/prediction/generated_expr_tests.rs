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
    let memory = work.memory_budget();
    let retained = memory.used_bytes();
    {
        let parse_work = AllocationScope::new(memory).unwrap();
        with_bail_fallback(environment, source, &parse_work, |session, mode| {
            assert_eq!(mode, PredictionMode::Sll);
            let root = session.numeric_expression()?;
            check(session, root);
            Ok(())
        }).unwrap();
    }
    assert_eq!(memory.used_bytes(), retained);
    {
        let parse_work = AllocationScope::new(memory).unwrap();
        with_parser_session(environment, source, &parse_work, |session| {
            session.profile = ParserProfile::LlDefault;
            let root = session.numeric_expression().unwrap();
            check(session, root);
            Ok(())
        }).unwrap();
    }
    assert_eq!(memory.used_bytes(), retained);
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
                let invoking = match mode {
                    PredictionMode::Sll => ParserState::RootSentinel,
                    PredictionMode::Ll => ParserState::State(1001),
                };
                assert_eq!(context.invoking_state(), invoking);
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
fn unadopted_recursive_base_choices_and_delete_leave_their_private_scope() {
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
        let result = with_bail_fallback(environment, "DELETE FROM foo", work, |session, mode| {
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

#[test]
fn fixed_scalar_literals_preserve_source_labels_and_raw_occurrences_in_both_profiles() {
    let ((), memory) = environment(|environment, work, _| {
        for (raw, class, string_class, token_number) in [
            ("NuLl", ContextClass::NullLiteral, None, 220),
            ("TrUe", ContextClass::BooleanLiteral, None, 326),
            ("fAlSe", ContextClass::BooleanLiteral, None, 139),
            ("oN", ContextClass::BooleanLiteral, None, 230),
            ("oFf", ContextClass::BooleanLiteral, None, 227),
            ("'a''b'", ContextClass::CharacterStringLiteral, Some(ContextClass::SqlStandardString), 6),
            ("'Ω🙂'", ContextClass::CharacterStringLiteral, Some(ContextClass::SqlStandardString), 6),
            ("'a\rb'", ContextClass::CharacterStringLiteral, Some(ContextClass::SqlStandardString), 6),
            (r"e'\uD800'", ContextClass::CharacterStringLiteral, Some(ContextClass::CEscapesString), 7),
            (r"E'\U00110000'", ContextClass::CharacterStringLiteral, Some(ContextClass::CEscapesString), 7),
        ] {
            let source = format!("{raw}; @");
            both_profiles(environment, &source, work, |session, root| {
                let value = expr_value(session, root, 0);
                let tree = session.invocation_view();
                let literal = tree.context(child_rule(tree.context(value).unwrap().children())).unwrap();
                assert_eq!((literal.class(), literal.parent(), literal.invoking_state()),
                    (Some(class), Some(value), ParserState::State(1140)));
                assert_eq!((literal.start().unwrap().index(), literal.stop().unwrap().index()), (0, 0));
                let terminal_parent = if let Some(string_class) = string_class {
                    let string = tree.context(child_rule(literal.children())).unwrap();
                    assert_eq!((string.rule(), string.class(), string.parent(), string.invoking_state()),
                        (CHARACTER_STRING, Some(string_class), Some(literal.key()), ParserState::State(693)));
                    assert_eq!((string.start().unwrap().index(), string.stop().unwrap().index()), (0, 0));
                    string
                } else {
                    literal
                };
                let [ContextChild::Terminal(occurrence)] = terminal_parent.children() else {
                    panic!("one fixed literal terminal");
                };
                let lexeme = occurrence.lexeme();
                let grammar = session.environment.storage.grammar_view();
                assert_eq!(grammar.token_type(lexeme.kind()).unwrap().number(), token_number);
                assert_eq!((occurrence.index(), lexeme.raw()), (0, raw));
                let span = lexeme.span();
                assert_eq!((span.start.byte, span.end.byte), (0, raw.len()));
                assert_eq!((span.start.code_point, span.end.code_point), (0, raw.chars().count()));
                assert_eq!(session.buffered_token(1).unwrap().lexeme().raw(), ";");
                assert!(session.buffered_token(2).is_none());
            });
        }
    });
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn scalar_literal_first_tokens_reach_ll_deletion_and_keep_unknown_distinct() {
    for (source, raw, cause) in [
        (") NULL", ")", None),
        (") FALSE", ")", None),
        ("UNKNOWN", "UNKNOWN", Some(ParserState::State(1001))),
    ] {
        let (result, memory) = environment(|environment, work, _| {
            let mut passes = Vec::new();
            let result = with_bail_fallback(environment, source, work, |session, mode| {
                let root = InvocationKey::at(session.records.len());
                let action = session.numeric_expression();
                passes.push(mode);
                let tree = session.invocation_view();
                assert!(tree.context(root).unwrap().class().is_none());
                assert_eq!((session.current(), session.precedence(), session.input_index()), (None, 0, Some(0)));
                if mode == PredictionMode::Sll {
                    assert!(matches!(action, Err(PassError::Cancelled(_))));
                    assert!(session.buffered_token(1).is_none());
                } else {
                    assert!(session.buffered_token(1).is_some());
                    assert!(session.buffered_token(2).is_none());
                    assert_eq!(tree.exception(root).is_some(), cause.is_some());
                }
                action.map(|_| ())
            });
            assert_eq!(passes, [PredictionMode::Sll, PredictionMode::Ll]);
            result
        });
        let Err(RuntimeError::ReportedTokenSyntax(report)) = result else { panic!("reached LL token report"); };
        assert_eq!((report.offending().index(), report.offending().lexeme().raw()), (0, raw));
        assert_eq!(report.cause().map(|cause| cause.state), cause);
        assert_eq!(report.anomaly().code, "xtdb/sql-error");
        drop(report);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn generated_dollar_string_preserves_empty_and_multichunk_text_in_both_profiles() {
    let ((), memory) = environment(|environment, work, _| {
        for (source, tag, parts) in [
            ("$$$$; @", "$$", &[][..]),
            ("$Q$a;$q$b$Q$; @", "$Q$", &["a;", "$", "q", "$", "b"][..]),
        ] {
            both_profiles(environment, source, work, |session, root| {
                let close = parts.len() + 1;
                let value = expr_value(session, root, close);
                let tree = session.invocation_view();
                let literal = tree.context(child_rule(tree.context(value).unwrap().children())).unwrap();
                let string = tree.context(child_rule(literal.children())).unwrap();
                assert_eq!(literal.class(), Some(ContextClass::CharacterStringLiteral));
                assert_eq!((string.rule(), string.class(), string.parent(), string.invoking_state()),
                    (CHARACTER_STRING, Some(ContextClass::DollarString), Some(literal.key()), ParserState::State(693)));
                let children = string.children();
                assert_eq!(children.len(), if parts.is_empty() { 2 } else { 3 });
                only_terminal(&children[..1], 0, tag);
                only_terminal(&children[children.len() - 1..], close, tag);
                assert_eq!((string.start().unwrap().index(), string.stop().unwrap().index()), (0, close));
                if !parts.is_empty() {
                    let text = tree.context(child_rule(&children[1..2])).unwrap();
                    assert_eq!((text.rule(), text.class(), text.parent(), text.invoking_state()),
                        (DOLLAR_STRING_TEXT, None, Some(string.key()), ParserState::State(719)));
                    assert_eq!((text.start().unwrap().index(), text.stop().unwrap().index()), (1, parts.len()));
                    assert_eq!(text.children().len(), parts.len());
                    for (index, raw) in parts.iter().enumerate() {
                        only_terminal(&text.children()[index..=index], index + 1, raw);
                    }
                }
                assert_eq!(session.input_index(), Some(close + 1));
                assert_eq!(session.buffered_token(close + 1).unwrap().lexeme().raw(), ";");
                assert!(session.buffered_token(close + 2).is_none());
                assert!(!session.matched_eof);
            });
        }
    });
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn dollar_string_rule_distinguishes_the_empty_body_sync_and_missing_closer_after_text() {
    for (source, eof, ll_state, message) in [
        ("$q$", 1, Some(ParserState::State(720)), "mismatched input '<EOF>' expecting {DM_END_TAG, DM_TEXT}"),
        ("$q$text", 2, None, "missing DM_END_TAG at '<EOF>'"),
    ] {
        let (result, memory) = environment(|environment, work, _| {
            let mut passes = Vec::new();
            let result = with_bail_fallback(environment, source, work, |session, mode| {
                let labelled = InvocationKey::at(session.records.len() + 1);
                let action = session.character_string();
                passes.push(mode);
                let tree = session.invocation_view();
                let string = tree.context(labelled).unwrap();
                assert_eq!(string.class(), Some(ContextClass::DollarString));
                assert_eq!((string.start().unwrap().index(), string.stop().unwrap().index()), (0, eof - 1));
                assert_eq!(string.children().len(), eof);
                only_terminal(&string.children()[..1], 0, "$q$");
                if eof == 2 {
                    let text = tree.context(child_rule(&string.children()[1..])).unwrap();
                    assert_eq!(text.invoking_state(), ParserState::State(719));
                    only_terminal(text.children(), 1, "text");
                }
                if mode == PredictionMode::Sll {
                    assert!(matches!(action, Err(PassError::Cancelled(_))));
                    assert!(matches!(tree.exception(labelled), Some(InvocationCause::InputMismatch(cause))
                        if cause.state == ParserState::State(722)));
                } else {
                    assert_eq!(tree.exception(labelled).is_some(), ll_state.is_some());
                    if eof == 2 {
                        let capture = session.next_tokens_capture.unwrap();
                        assert_eq!((capture.context, capture.state), (child_rule(&string.children()[1..]), 714));
                    }
                }
                assert_eq!((session.current(), session.input_index(), session.matched_eof), (None, Some(eof), false));
                action.map(|_| ())
            });
            assert_eq!(passes, [PredictionMode::Sll, PredictionMode::Ll]);
            result
        });
        let Err(RuntimeError::ReportedTokenSyntax(report)) = result else { panic!("generated dollar string report"); };
        assert_eq!(report.cause().map(|cause| cause.state), ll_state);
        assert_eq!((report.offending().index(), report.offending().lexeme().kind()), (eof, Kind::Eof));
        assert!(report.anomaly().message.as_deref().unwrap().ends_with(message));
        drop(report);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn warmed_string_child_attachment_denial_unrolls_the_actual_literal_residue() {
    let (result, memory) = environment(|environment, work, memory| {
        let source = "'text'";
        let retained = work.memory_budget().used_bytes();
        {
            let parse_work = AllocationScope::new(memory).unwrap();
            with_bail_fallback(environment, source, &parse_work, |session, mode| {
                assert_eq!(mode, PredictionMode::Sll);
                session.numeric_expression().map(|_| ())
            })?;
        }
        let arena = Layout::array::<InvocationRecord<'_, '_>>(16).unwrap().size() + 256;
        let slot = Layout::new::<ContextChild<'_, '_>>().size() + 256;
        assert_eq!(memory.used_bytes(), retained);
        {
            let parse_work = AllocationScope::new(memory).unwrap();
            with_parser_session(environment, source, &parse_work, |session| {
                session.profile = ParserProfile::SllBail;
                let before = memory.used_bytes();
                let root = session.numeric_expression().unwrap();
                expr_value(session, root, 0);
                assert_eq!(memory.used_bytes() - before, arena + 4 * slot);
                Ok(())
            })?;
        }
        assert_eq!(memory.used_bytes(), retained);
        {
            let parse_work = AllocationScope::new(memory).unwrap();
            with_parser_session(environment, source, &parse_work, |session| {
                session.profile = ParserProfile::SllBail;
                let held = memory.reserve(memory.limit_bytes() - memory.used_bytes() - arena - slot).unwrap();
                let action = session.numeric_expression();
                let Err(PassError::Runtime(RuntimeError::Prediction(PredictionError::Admission(error)))) = action
                    else { panic!("actual string child attachment denial"); };
                assert_eq!(error.code, "rtdb/memory-budget-exceeded");
                let prefix = format!("Cannot reserve {slot} bytes from budget ");
                assert!(error.message.as_deref().unwrap().starts_with(prefix.as_str()));
                let tree = session.invocation_view();
                let expr = tree.context(InvocationKey::at(1)).unwrap();
                let literal = tree.context(InvocationKey::at(5)).unwrap();
                let string = tree.context(InvocationKey::at(6)).unwrap();
                assert_eq!((literal.parent(), literal.class()), (Some(expr.key()), Some(ContextClass::CharacterStringLiteral)));
                assert_eq!((string.rule(), string.parent(), string.invoking_state()),
                    (CHARACTER_STRING, Some(literal.key()), ParserState::State(693)));
                assert!(matches!(session.records[string.key().index].role, InvocationRole::OrdinaryTreeAttachmentFailed));
                assert!(literal.children().is_empty() && string.children().is_empty() && expr.children().is_empty());
                assert!(literal.stop().is_none() && string.stop().is_none() && expr.stop().is_none());
                assert!(matches!(session.records[0].role, InvocationRole::RecursiveEntry { phase: RecursionPhase::Closed, .. }));
                assert!(matches!(session.records[2].role, InvocationRole::RecursiveEntry { phase: RecursionPhase::Closed, .. }));
                assert_eq!((session.current(), session.state(), session.precedence(), session.input_index()),
                    (None, ParserState::State(693), 0, Some(0)));
                assert_eq!(session.syntax_errors, 0);
                assert!(!session.matched_eof);
                drop(held);
                Ok(())
            })?;
        }
        assert_eq!(memory.used_bytes(), retained);
        Ok::<(), RuntimeError<'_>>(())
    });
    result.unwrap();
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn warmed_dollar_text_attachment_denial_restores_through_both_entered_string_ancestors() {
    let (result, memory) = environment(|environment, work, memory| {
        let source = "$q$text$q$";
        let baseline = work.memory_budget().used_bytes();
        {
            let parse_work = AllocationScope::new(memory).unwrap();
            with_bail_fallback(environment, source, &parse_work, |session, mode| {
                assert_eq!(mode, PredictionMode::Sll);
                session.numeric_expression().map(|_| ())
            })?;
        }
        assert_eq!(memory.used_bytes(), baseline);
        let arena = Layout::array::<InvocationRecord<'_, '_>>(16).unwrap().size() + 256;
        let slot = Layout::new::<ContextChild<'_, '_>>().size() + 256;
        {
            let parse_work = AllocationScope::new(memory).unwrap();
            with_parser_session(environment, source, &parse_work, |session| {
                session.profile = ParserProfile::SllBail;
                let held = memory.reserve(memory.limit_bytes() - memory.used_bytes() - arena - 3 * slot).unwrap();
                let action = session.numeric_expression();
                let Err(PassError::Runtime(RuntimeError::Prediction(PredictionError::Admission(error)))) = action
                    else { panic!("dollar-text child allocation denial"); };
                assert_eq!(error.code, "rtdb/memory-budget-exceeded");
                let prefix = format!("Cannot reserve {slot} bytes from budget ");
                assert!(error.message.as_deref().unwrap().starts_with(prefix.as_str()));

                let tree = session.invocation_view();
                let expr = tree.context(InvocationKey::at(1)).unwrap();
                let literal = tree.context(InvocationKey::at(5)).unwrap();
                let string = tree.context(InvocationKey::at(7)).unwrap();
                let text = tree.context(InvocationKey::at(8)).unwrap();
                assert_eq!((literal.parent(), literal.class()), (Some(expr.key()), Some(ContextClass::CharacterStringLiteral)));
                assert_eq!((string.parent(), string.class(), string.invoking_state()),
                    (Some(literal.key()), Some(ContextClass::DollarString), ParserState::State(693)));
                assert_eq!((text.parent(), text.rule(), text.invoking_state()),
                    (Some(string.key()), DOLLAR_STRING_TEXT, ParserState::State(719)));
                assert!(matches!(session.records[text.key().index].role, InvocationRole::OrdinaryTreeAttachmentFailed));
                assert_eq!((text.start().unwrap().index(), text.stop().unwrap().index()), (1, 0));
                assert_eq!(string.stop().unwrap().index(), 0);
                assert_eq!(literal.stop().unwrap().index(), 0);
                only_terminal(string.children(), 0, "$q$");
                assert!(expr.children().is_empty() && text.children().is_empty());
                assert_eq!((session.current(), session.input_index(), session.precedence()), (None, Some(1), 0));
                assert!(!session.matched_eof);
                drop(held);
                Ok(())
            })?;
        }
        assert_eq!(memory.used_bytes(), baseline);
        Ok::<(), RuntimeError<'_>>(())
    });
    result.unwrap();
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn prefixed_string_literals_preserve_raw_tokens_and_rule16_calls_in_both_profiles() {
    let ((), memory) = environment(|environment, work, _| {
        for (prefix, raw, class, call, string_class, kind, stop) in [
            ("tImE", "'23:59:59'", ContextClass::TimeLiteral, 697, ContextClass::SqlStandardString, Kind::CharacterString, 1),
            ("duration", "E'P2D'", ContextClass::DurationLiteral, 700, ContextClass::CEscapesString, Kind::EscapedString, 1),
            ("UUID", "'00000000-0000-0000-0000-000000000000'", ContextClass::UuidLiteral, 702, ContextClass::SqlStandardString, Kind::CharacterString, 1),
            ("uri", "$Q$https://example.test/a;b$Q$", ContextClass::UriLiteral, 704, ContextClass::DollarString, Kind::DollarStart, 3),
            ("Keyword", "'a''b'", ContextClass::KeywordLiteral, 706, ContextClass::SqlStandardString, Kind::CharacterString, 1),
        ] {
            let source = format!("{prefix} {raw}; @");
            both_profiles(environment, &source, work, |session, root| {
                let value = expr_value(session, root, stop);
                let tree = session.invocation_view();
                let literal = tree.context(child_rule(tree.context(value).unwrap().children())).unwrap();
                assert_eq!((literal.rule(), literal.class(), literal.parent(), literal.invoking_state()),
                    (LITERAL, Some(class), Some(value), ParserState::State(1140)));
                assert_eq!(literal.children().len(), 2);
                only_terminal(&literal.children()[..1], 0, prefix);
                let start = literal.start().unwrap().lexeme().span();
                assert_eq!((start.start.byte, start.end.byte), (0, prefix.len()));

                let string = tree.context(child_rule(&literal.children()[1..])).unwrap();
                assert_eq!((string.rule(), string.class(), string.parent(), string.invoking_state()),
                    (CHARACTER_STRING, Some(string_class), Some(literal.key()), ParserState::State(call)));
                let first = string.start().unwrap();
                let last = string.stop().unwrap();
                assert_eq!((first.index(), last.index(), literal.stop().unwrap().index()), (1, stop, stop));
                assert_eq!(first.lexeme().kind(), kind);
                assert_eq!((first.lexeme().span().start.byte, last.lexeme().span().end.byte), (prefix.len() + 1, prefix.len() + 1 + raw.len()));
                if string_class == ContextClass::DollarString {
                    assert_eq!(string.children().len(), 3);
                    only_terminal(&string.children()[..1], 1, "$Q$");
                    let body = tree.context(child_rule(&string.children()[1..2])).unwrap();
                    assert_eq!((body.rule(), body.invoking_state()), (DOLLAR_STRING_TEXT, ParserState::State(719)));
                    only_terminal(body.children(), 2, "https://example.test/a;b");
                    only_terminal(&string.children()[2..], 3, "$Q$");
                } else {
                    only_terminal(string.children(), 1, raw);
                }
                assert_eq!(session.buffered_token(stop + 1).unwrap().lexeme().raw(), ";");
                assert!(session.buffered_token(stop + 2).is_none());
            });
        }
    });
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn prefixed_rule14_reports_string_mismatch_or_deletion_from_rule16() {
    for (source, prefix, class, call, cause, second, message) in [
        ("TIME )", "TIME", ContextClass::TimeLiteral, 697, Some(ParserState::State(723)), Kind::Eof,
            "mismatched input ')' expecting {CHARACTER_STRING, C_ESCAPES_STRING, DOLLAR_TAG}"),
        ("URI ) 'ok'", "URI", ContextClass::UriLiteral, 704, None, Kind::CharacterString,
            "extraneous input ')' expecting {CHARACTER_STRING, C_ESCAPES_STRING, DOLLAR_TAG}"),
    ] {
        let (result, memory) = environment(|environment, work, _| {
            let mut passes = Vec::new();
            let result = with_bail_fallback(environment, source, work, |session, mode| {
                let labelled = InvocationKey::at(session.records.len() + 1);
                let action = session.scalar_literal(mode);
                passes.push(mode);
                let tree = session.invocation_view();
                let literal = tree.context(labelled).unwrap();
                assert_eq!((literal.class(), literal.children().len()), (Some(class), 2));
                only_terminal(&literal.children()[..1], 0, prefix);
                let string = tree.context(child_rule(&literal.children()[1..])).unwrap();
                assert_eq!((string.rule(), string.class(), string.parent(), string.invoking_state()),
                    (CHARACTER_STRING, None, Some(labelled), ParserState::State(call)));
                assert_eq!((string.start().unwrap().index(), string.stop().unwrap().index(), literal.stop().unwrap().index()), (1, 0, 0));
                assert!(string.children().is_empty());

                if mode == PredictionMode::Sll {
                    assert!(matches!(action, Err(PassError::Cancelled(_))));
                    assert!(matches!(tree.exception(string.key()), Some(InvocationCause::NoViableAlternative(cause))
                        if cause.state == ParserState::State(723) && cause.start.index() == 1 && cause.offending.index() == 1));
                    assert!(matches!(tree.exception(labelled), Some(InvocationCause::NoViableAlternative(_))));
                    assert!(session.buffered_token(2).is_none());
                } else {
                    assert_eq!(tree.exception(string.key()).is_some(), cause.is_some());
                    assert!(tree.exception(labelled).is_none());
                    assert_eq!(session.buffered_token(2).unwrap().lexeme().kind(), second);
                    assert!(session.buffered_token(3).is_none());
                }
                assert_eq!((session.current(), session.input_index(), session.matched_eof), (None, Some(1), false));
                action.map(|_| ())
            });
            assert_eq!(passes, [PredictionMode::Sll, PredictionMode::Ll]);
            result
        });
        let Err(RuntimeError::ReportedTokenSyntax(report)) = result else { panic!("generated prefixed literal report"); };
        assert_eq!(report.cause().map(|cause| cause.state), cause);
        assert_eq!((report.offending().index(), report.offending().lexeme().raw()), (1, ")"));
        assert_eq!(report.anomaly().code, "xtdb/sql-error");
        assert!(report.anomaly().message.as_deref().unwrap().ends_with(message));
        drop(report);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn warmed_prefixed_literals_restore_their_exact_direct_and_dollar_child_residues() {
    for (source, prefix, class, call, dollar) in [
        ("TIME '12:00:00'", "TIME", ContextClass::TimeLiteral, 697, false),
        ("URI $q$text$q$", "URI", ContextClass::UriLiteral, 704, true),
    ] {
        let (result, memory) = environment(|environment, work, memory| {
            let baseline = work.memory_budget().used_bytes();
            {
                let parse_work = AllocationScope::new(memory).unwrap();
                with_bail_fallback(environment, source, &parse_work, |session, mode| {
                    assert_eq!(mode, PredictionMode::Sll);
                    session.numeric_expression().map(|_| ())
                })?;
            }
            assert_eq!(memory.used_bytes(), baseline);
            let arena = Layout::array::<InvocationRecord<'_, '_>>(16).unwrap().size() + 256;
            let single = Layout::array::<ContextChild<'_, '_>>(1).unwrap().size() + 256;
            let pair = Layout::array::<ContextChild<'_, '_>>(2).unwrap().size() + 256;
            let capacity = arena + if dollar { 3 * single + pair } else { 2 * single };
            {
                let parse_work = AllocationScope::new(memory).unwrap();
                with_parser_session(environment, source, &parse_work, |session| {
                    session.profile = ParserProfile::SllBail;
                    let held = memory.reserve(memory.limit_bytes() - memory.used_bytes() - capacity).unwrap();
                    let action = session.numeric_expression();
                    let Err(PassError::Runtime(RuntimeError::Prediction(PredictionError::Admission(error)))) = action
                        else { panic!("prefixed literal child attachment denial"); };
                    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
                    let denied_request = format!("Cannot reserve {single} bytes from budget ");
                    assert!(error.message.as_deref().unwrap().starts_with(denied_request.as_str()));

                    let tree = session.invocation_view();
                    let expr = tree.context(InvocationKey::at(1)).unwrap();
                    let literal = tree.context(InvocationKey::at(5)).unwrap();
                    assert_eq!((literal.parent(), literal.class()), (Some(expr.key()), Some(class)));
                    only_terminal(&literal.children()[..1], 0, prefix);
                    let (failed, parent, rule, invoking, start, stop) = if dollar {
                        assert_eq!(session.records.len(), 9);
                        let string = tree.context(child_rule(&literal.children()[1..])).unwrap();
                        assert_eq!((string.class(), string.parent(), string.invoking_state(), string.stop().unwrap().index()),
                            (Some(ContextClass::DollarString), Some(literal.key()), ParserState::State(call), 1));
                        only_terminal(string.children(), 1, "$q$");
                        (InvocationKey::at(8), string.key(), DOLLAR_STRING_TEXT, 719, 2, 1)
                    } else {
                        assert_eq!(session.records.len(), 7);
                        assert_eq!(literal.children().len(), 1);
                        (InvocationKey::at(6), literal.key(), CHARACTER_STRING, call, 1, 0)
                    };
                    assert!(matches!(session.records[failed.index].role, InvocationRole::OrdinaryTreeAttachmentFailed));
                    let child = tree.context(failed).unwrap();
                    assert_eq!((child.parent(), child.rule(), child.invoking_state()), (Some(parent), rule, ParserState::State(invoking)));
                    assert_eq!((child.start().unwrap().index(), child.stop().unwrap().index(), literal.stop().unwrap().index()), (start, stop, stop));
                    assert!(expr.children().is_empty() && child.children().is_empty());
                    assert_eq!((session.current(), session.state(), session.input_index(), session.precedence()),
                        (None, ParserState::State(call), Some(start), 0));
                    assert!(matches!(session.records[0].role, InvocationRole::RecursiveEntry { phase: RecursionPhase::Closed, .. }));
                    assert!(matches!(session.records[2].role, InvocationRole::RecursiveEntry { phase: RecursionPhase::Closed, .. }));
                    assert!(!session.matched_eof);
                    drop(held);
                    Ok(())
                })?;
            }
            assert_eq!(memory.used_bytes(), baseline);
            Ok::<(), RuntimeError<'_>>(())
        });
        result.unwrap();
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn date_and_timestamp_literals_preserve_raw_string_and_all_zone_trees_in_both_profiles() {
    let ((), memory) = environment(|environment, work, _| {
        for (prefix, class, zone, zone_class, raw, string_class) in [
            ("dAtE", ContextClass::DateLiteral, &[][..], None, "'1901-01-01'", ContextClass::SqlStandardString),
            ("TiMeStAmP", ContextClass::TimestampLiteral, &[][..], None, "E'2001-02-03T04:05:06'", ContextClass::CEscapesString),
            ("TIMESTAMP", ContextClass::TimestampLiteral, &["WiTh", "TiMe", "ZoNe"][..], Some(ContextClass::WithTimeZone),
                "$q$2001-02-03T04:05:06+00$q$", ContextClass::DollarString),
            ("TIMESTAMP", ContextClass::TimestampLiteral, &["WITH", "TIMEZONE"][..], Some(ContextClass::WithTimeZone),
                "'2001-02-03T04:05:06+00'", ContextClass::SqlStandardString),
            ("TIMESTAMP", ContextClass::TimestampLiteral, &["WITHOUT", "TIME", "ZONE"][..], Some(ContextClass::WithoutTimeZone),
                "'2001-02-03T04:05:06'", ContextClass::SqlStandardString),
            ("TIMESTAMP", ContextClass::TimestampLiteral, &["WITHOUT", "TIMEZONE"][..], Some(ContextClass::WithoutTimeZone),
                "'2001-02-03T04:05:06'", ContextClass::SqlStandardString),
        ] {
            let source = if zone.is_empty() {
                format!("{prefix} {raw}; @")
            } else {
                format!("{prefix} {} {raw}; @", zone.join(" "))
            };
            let start = zone.len() + 1;
            let stop = start + if string_class == ContextClass::DollarString { 2 } else { 0 };
            both_profiles(environment, &source, work, |session, root| {
                let value = expr_value(session, root, stop);
                let tree = session.invocation_view();
                let literal = tree.context(child_rule(tree.context(value).unwrap().children())).unwrap();
                assert_eq!((literal.rule(), literal.class(), literal.parent(), literal.invoking_state()),
                    (LITERAL, Some(ContextClass::DateTimeLiteral0), Some(value), ParserState::State(1140)));
                let wrapper = tree.context(child_rule(literal.children())).unwrap();
                assert_eq!((wrapper.rule(), wrapper.class(), wrapper.parent(), wrapper.invoking_state()),
                    (DATE_TIME_LITERAL, Some(class), Some(literal.key()), ParserState::State(695)));
                only_terminal(&wrapper.children()[..1], 0, prefix);
                let string_index = if let Some(zone_class) = zone_class {
                    assert_eq!(wrapper.children().len(), 3);
                    let qualifier = tree.context(child_rule(&wrapper.children()[1..2])).unwrap();
                    assert_eq!((qualifier.rule(), qualifier.class(), qualifier.parent(), qualifier.invoking_state()),
                        (TIME_ZONE, Some(zone_class), Some(wrapper.key()), ParserState::State(677)));
                    assert_eq!((qualifier.start().unwrap().index(), qualifier.stop().unwrap().index()), (1, zone.len()));
                    assert_eq!(qualifier.children().len(), zone.len());
                    for (index, raw) in zone.iter().enumerate() {
                        only_terminal(&qualifier.children()[index..=index], index + 1, raw);
                    }
                    2
                } else {
                    assert_eq!(wrapper.children().len(), 2);
                    1
                };
                let string = tree.context(child_rule(&wrapper.children()[string_index..])).unwrap();
                let invoking = if class == ContextClass::DateLiteral { 675 } else { 680 };
                assert_eq!((string.rule(), string.class(), string.parent(), string.invoking_state()),
                    (CHARACTER_STRING, Some(string_class), Some(wrapper.key()), ParserState::State(invoking)));
                assert_eq!((string.start().unwrap().index(), string.stop().unwrap().index()), (start, stop));
                let first = string.start().unwrap().lexeme().span().start.byte;
                let end = string.stop().unwrap().lexeme().span().end.byte;
                assert_eq!(&source[first..end], raw);
                assert_eq!((literal.start().unwrap().lexeme().span().start.byte, literal.stop().unwrap().lexeme().span().end.byte), (0, end));
                assert_eq!((wrapper.start().unwrap().index(), wrapper.stop().unwrap().index()), (0, stop));
                if string_class == ContextClass::DollarString {
                    assert_eq!(string.children().len(), 3);
                    only_terminal(&string.children()[..1], start, "$q$");
                    let body = tree.context(child_rule(&string.children()[1..2])).unwrap();
                    assert_eq!((body.rule(), body.invoking_state()), (DOLLAR_STRING_TEXT, ParserState::State(719)));
                    only_terminal(body.children(), start + 1, "2001-02-03T04:05:06+00");
                    only_terminal(&string.children()[2..], stop, "$q$");
                } else {
                    only_terminal(string.children(), start, raw);
                }
                assert_eq!(session.buffered_token(stop + 1).unwrap().lexeme().raw(), ";");
                assert!(session.buffered_token(stop + 2).is_none());
            });
        }
    });
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn date_time_rule14_preserves_the_rule16_rule12_and_rule30_recovery_boundaries() {
    for (source, offending, index, bail_state, ll_state, message) in [
        ("DATE )", ")", 1, 723, Some(ParserState::State(723)),
            "mismatched input ')' expecting {CHARACTER_STRING, C_ESCAPES_STRING, DOLLAR_TAG}"),
        ("TIMESTAMP ) 'x'", ")", 1, 723, None,
            "extraneous input ')' expecting {CHARACTER_STRING, C_ESCAPES_STRING, 'WITH', 'WITHOUT', DOLLAR_TAG}"),
        ("TIMESTAMP WITH TIME 'x'", "'x'", 3, 968, None, "missing 'ZONE' at ''x''"),
    ] {
        let (result, memory) = environment(|environment, work, _| {
            let mut passes = Vec::new();
            let result = with_bail_fallback(environment, source, work, |session, mode| {
                let root = InvocationKey::at(session.records.len() + 1);
                let action = session.scalar_literal(mode);
                passes.push(mode);
                let tree = session.invocation_view();
                let literal = tree.context(root).unwrap();
                assert_eq!(literal.class(), Some(ContextClass::DateTimeLiteral0));
                let wrapper = tree.context(child_rule(literal.children())).unwrap();
                let date = source.starts_with("DATE");
                let (prefix, class) = if date { ("DATE", ContextClass::DateLiteral) } else { ("TIMESTAMP", ContextClass::TimestampLiteral) };
                assert_eq!((wrapper.rule(), wrapper.class(), wrapper.parent(), wrapper.invoking_state()),
                    (DATE_TIME_LITERAL, Some(class), Some(root), ParserState::State(695)));
                only_terminal(&wrapper.children()[..1], 0, prefix);
                let owner = if bail_state == 968 {
                    let zone = tree.context(child_rule(&wrapper.children()[1..])).unwrap();
                    assert_eq!((zone.rule(), zone.class(), zone.invoking_state()),
                        (TIME_ZONE, Some(ContextClass::WithTimeZone), ParserState::State(677)));
                    assert_eq!(zone.children().len(), 2);
                    only_terminal(&zone.children()[..1], 1, "WITH");
                    only_terminal(&zone.children()[1..], 2, "TIME");
                    zone
                } else if date || mode == PredictionMode::Sll {
                    let string = tree.context(child_rule(&wrapper.children()[1..])).unwrap();
                    assert_eq!((string.rule(), string.class(), string.invoking_state()),
                        (CHARACTER_STRING, None, ParserState::State(if date { 675 } else { 680 })));
                    assert!(string.children().is_empty());
                    string
                } else {
                    assert_eq!(wrapper.children().len(), 1);
                    wrapper
                };
                assert_eq!((owner.stop().unwrap().index(), wrapper.stop().unwrap().index(), literal.stop().unwrap().index()),
                    (index - 1, index - 1, index - 1));

                if mode == PredictionMode::Sll {
                    assert!(matches!(action, Err(PassError::Cancelled(_))));
                    let cause = tree.exception(owner.key()).unwrap();
                    match cause {
                        InvocationCause::NoViableAlternative(cause) => {
                            assert_eq!(bail_state, 723);
                            assert_eq!((cause.state, cause.start.index(), cause.offending.index()), (ParserState::State(723), index, index));
                        }
                        InvocationCause::InputMismatch(cause) => {
                            assert_eq!(bail_state, 968);
                            assert_eq!((cause.state, cause.offending.index()), (ParserState::State(968), index));
                        }
                    }
                    assert!(std::ptr::eq(cause, tree.exception(wrapper.key()).unwrap()));
                    assert!(std::ptr::eq(cause, tree.exception(root).unwrap()));
                    assert!(session.buffered_token(index + 1).is_none());
                } else {
                    assert_eq!(tree.exception(owner.key()).is_some(), ll_state.is_some());
                    assert!(tree.exception(root).is_none());
                    let second = if !date && bail_state == 723 { Kind::CharacterString } else { Kind::Eof };
                    assert_eq!(session.buffered_token(index + 1).unwrap().lexeme().kind(), second);
                    assert!(session.buffered_token(index + 2).is_none());
                }
                assert_eq!((session.current(), session.input_index(), session.matched_eof), (None, Some(index), false));
                action.map(|_| ())
            });
            assert_eq!(passes, [PredictionMode::Sll, PredictionMode::Ll]);
            result
        });
        let Err(RuntimeError::ReportedTokenSyntax(report)) = result else { panic!("generated date-time literal report"); };
        assert_eq!(report.cause().map(|cause| cause.state), ll_state);
        assert_eq!((report.offending().index(), report.offending().lexeme().raw()), (index, offending));
        assert_eq!(report.anomaly().code, "xtdb/sql-error");
        assert!(report.anomaly().message.as_deref().unwrap().ends_with(message));
        drop(report);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn warmed_date_time_literals_restore_direct_and_nested_failed_attachments() {
    for (source, ordinal, parent, rule, invoking, start, stop) in [
        ("DATE 'x'", 6, 5, DATE_TIME_LITERAL, 695, 0, None),
        ("DATE 'x'", 8, 7, CHARACTER_STRING, 675, 1, Some(0)),
        ("TIMESTAMP WITH TIME ZONE 'x'", 8, 7, TIME_ZONE, 677, 1, Some(0)),
        ("DATE $q$x$q$", 10, 9, DOLLAR_STRING_TEXT, 719, 2, Some(1)),
    ] {
        let (result, memory) = environment(|environment, work, memory| {
            let baseline = work.memory_budget().used_bytes();
            {
                let parse_work = AllocationScope::new(memory).unwrap();
                with_bail_fallback(environment, source, &parse_work, |session, mode| {
                    assert_eq!(mode, PredictionMode::Sll);
                    session.numeric_expression().map(|_| ())
                })?;
            }
            assert_eq!(memory.used_bytes(), baseline);
            let arena = Layout::array::<InvocationRecord<'_, '_>>(16).unwrap().size() + 256;
            let single = Layout::array::<ContextChild<'_, '_>>(1).unwrap().size() + 256;
            let pair = Layout::array::<ContextChild<'_, '_>>(2).unwrap().size() + 256;
            let capacity = arena + match rule {
                DATE_TIME_LITERAL => single,
                CHARACTER_STRING | TIME_ZONE => 3 * single,
                DOLLAR_STRING_TEXT => 4 * single + pair,
                _ => unreachable!("fixture attachment rule"),
            };
            {
                let parse_work = AllocationScope::new(memory).unwrap();
                with_parser_session(environment, source, &parse_work, |session| {
                    session.profile = ParserProfile::SllBail;
                    let held = memory.reserve(memory.limit_bytes() - memory.used_bytes() - capacity).unwrap();
                    let action = session.numeric_expression();
                    let Err(PassError::Runtime(RuntimeError::Prediction(PredictionError::Admission(error)))) = action
                        else { panic!("date-time child attachment denial"); };
                    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
                    let denied_request = format!("Cannot reserve {single} bytes from budget ");
                    assert!(error.message.as_deref().unwrap().starts_with(denied_request.as_str()));

                    assert_eq!(session.records.len(), ordinal + 1);
                    assert!(matches!(session.records[ordinal].role, InvocationRole::OrdinaryTreeAttachmentFailed));
                    let tree = session.invocation_view();
                    let expr = tree.context(InvocationKey::at(1)).unwrap();
                    let literal = tree.context(InvocationKey::at(5)).unwrap();
                    assert_eq!((literal.rule(), literal.class(), literal.parent()),
                        (LITERAL, Some(ContextClass::DateTimeLiteral0), Some(expr.key())));
                    assert_eq!(literal.stop().map(|token| token.index()), stop);
                    let failed = tree.context(InvocationKey::at(ordinal)).unwrap();
                    assert_eq!((failed.rule(), failed.class(), failed.parent(), failed.invoking_state()),
                        (rule, None, Some(InvocationKey::at(parent)), ParserState::State(invoking)));
                    assert_eq!((failed.start().unwrap().index(), failed.stop().map(|token| token.index())), (start, stop));
                    assert!(expr.children().is_empty() && failed.children().is_empty());
                    if rule == DATE_TIME_LITERAL {
                        assert!(literal.children().is_empty());
                    } else {
                        let wrapper = tree.context(child_rule(literal.children())).unwrap();
                        let (prefix, class) = if rule == TIME_ZONE {
                            ("TIMESTAMP", ContextClass::TimestampLiteral)
                        } else {
                            ("DATE", ContextClass::DateLiteral)
                        };
                        assert_eq!((wrapper.key(), wrapper.class(), wrapper.invoking_state(), wrapper.stop().map(|token| token.index())),
                            (InvocationKey::at(7), Some(class), ParserState::State(695), stop));
                        only_terminal(&wrapper.children()[..1], 0, prefix);
                        if rule == DOLLAR_STRING_TEXT {
                            let string = tree.context(child_rule(&wrapper.children()[1..])).unwrap();
                            assert_eq!((string.key(), string.class(), string.invoking_state(), string.stop().unwrap().index()),
                                (InvocationKey::at(9), Some(ContextClass::DollarString), ParserState::State(675), 1));
                            only_terminal(string.children(), 1, "$q$");
                        } else {
                            assert_eq!(wrapper.children().len(), 1);
                        }
                    }
                    assert_eq!((session.current(), session.state(), session.input_index(), session.precedence()),
                        (None, ParserState::State(695), Some(start), 0));
                    assert!(matches!(session.records[0].role, InvocationRole::RecursiveEntry { phase: RecursionPhase::Closed, .. }));
                    assert!(matches!(session.records[2].role, InvocationRole::RecursiveEntry { phase: RecursionPhase::Closed, .. }));
                    assert!(!session.matched_eof);
                    drop(held);
                    Ok(())
                })?;
            }
            assert_eq!(memory.used_bytes(), baseline);
            Ok::<(), RuntimeError<'_>>(())
        });
        result.unwrap();
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn binary_literals_keep_one_raw_terminal_with_its_original_span_in_both_profiles() {
    let ((), memory) = environment(|environment, work, _| {
        for (raw, body) in [("X('')", ""), ("x('aF01')", "aF01"), ("X('0')", "0")] {
            let source = format!("/* é */\r\n {raw}; @");
            both_profiles(environment, &source, work, |session, root| {
                let value = expr_value(session, root, 0);
                let tree = session.invocation_view();
                let value_view = tree.context(value).unwrap();
                assert_eq!(value_view.class(), Some(ContextClass::LiteralExpr));
                let literal = tree.context(child_rule(value_view.children())).unwrap();
                assert_eq!((literal.rule(), literal.class(), literal.parent(), literal.invoking_state()),
                    (LITERAL, Some(ContextClass::BinaryStringLiteral), Some(value), ParserState::State(1140)));
                only_terminal(literal.children(), 0, raw);
                let [ContextChild::Terminal(token)] = literal.children() else { panic!("one binary terminal"); };
                let lexeme = token.lexeme();
                let span = lexeme.span();
                assert_eq!((lexeme.kind(), lexeme.raw(), lexeme.body()), (Kind::BinaryString, raw, Some(body)));
                assert_eq!((span.start.byte, span.start.code_point, span.start.line, span.start.column), (11, 10, 2, 1));
                assert_eq!((span.end.byte, span.end.code_point, span.end.line, span.end.column),
                    (11 + raw.len(), 10 + raw.len(), 2, 1 + raw.len()));
                for view in [value_view, literal] {
                    assert_eq!((view.start().unwrap().index(), view.stop().unwrap().index()), (0, 0));
                    assert_eq!((view.start().unwrap().lexeme().span(), view.stop().unwrap().lexeme().span()), (span, span));
                }
                assert_eq!(session.buffered_token(1).unwrap().lexeme().raw(), ";");
                assert!(session.buffered_token(2).is_none());
            });
        }
    });
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn malformed_binary_spellings_reach_rule14_recovery_before_a_literal_label() {
    let expected = "{UNSIGNED_FLOAT, UNSIGNED_INTEGER, CHARACTER_STRING, C_ESCAPES_STRING, BINARY_STRING, '+', '-', 'DATE', 'DURATION', 'FALSE', 'INTERVAL', 'KEYWORD', 'NULL', 'OFF', 'ON', 'TIME', 'TIMESTAMP', 'TRUE', 'URI', 'UUID', DOLLAR_TAG}";
    for (source, second, ll_state, message) in [
        ("X('GG')", "(", Some(ParserState::State(709)), "mismatched input 'X' expecting"),
        ("X'00'", "'00'", None, "extraneous input 'X' expecting"),
    ] {
        let (result, memory) = environment(|environment, work, _| {
            let mut modes = Vec::new();
            let result = with_bail_fallback(environment, source, work, |session, mode| {
                modes.push(mode);
                let count = session.records.len();
                let key = InvocationKey::at(count);
                let action = session.scalar_literal(mode);
                assert_eq!(session.records.len(), count + 1);
                let tree = session.invocation_view();
                let literal = tree.context(key).unwrap();
                assert_eq!((literal.rule(), literal.class(), literal.parent(), literal.invoking_state()),
                    (LITERAL, None, None, ParserState::RootSentinel));
                assert_eq!(literal.start().unwrap().index(), 0);
                assert_eq!(literal.start().unwrap().lexeme().kind(), Kind::Identifier);
                assert_eq!(literal.start().unwrap().lexeme().raw(), "X");
                assert!(literal.stop().is_none() && literal.children().is_empty());
                if mode == PredictionMode::Sll {
                    assert!(matches!(action, Err(PassError::Cancelled(_))));
                    assert!(matches!(tree.exception(key), Some(InvocationCause::NoViableAlternative(cause))
                        if cause.state == ParserState::State(709) && cause.start.index() == 0 && cause.offending.index() == 0));
                    assert!(session.buffered_token(1).is_none());
                } else {
                    if let Some(state) = ll_state {
                        assert!(matches!(tree.exception(key), Some(InvocationCause::InputMismatch(cause))
                            if cause.state == state && cause.offending.index() == 0));
                    } else {
                        assert!(tree.exception(key).is_none());
                    }
                    assert_eq!(session.buffered_token(1).unwrap().lexeme().raw(), second);
                    assert!(session.buffered_token(2).is_none());
                }
                assert_eq!((session.current(), session.state(), session.input_index(), session.matched_eof),
                    (None, ParserState::RootSentinel, Some(0), false));
                action.map(|_| ())
            });
            assert_eq!(modes, [PredictionMode::Sll, PredictionMode::Ll]);
            result
        });
        let Err(RuntimeError::ReportedTokenSyntax(report)) = result else { panic!("generated literal diagnostic"); };
        assert_eq!(report.cause().map(|cause| cause.state), ll_state);
        let offending = report.offending();
        assert_eq!((offending.index(), offending.lexeme().raw()), (0, "X"));
        let span = offending.lexeme().span();
        assert_eq!((span.start.byte, span.end.byte), (0, 1));
        assert_eq!(report.anomaly().code, "xtdb/sql-error");
        let suffix = format!("{message} {expected}");
        assert!(report.anomaly().message.as_deref().unwrap().ends_with(suffix.as_str()));
        drop(report);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn warmed_binary_terminal_denial_keeps_the_ordinary_literal_exit_and_caller_restoration() {
    let (result, memory) = environment(|environment, work, memory| {
        let source = "x('aF01'); @";
        let baseline = work.memory_budget().used_bytes();
        {
            let parse_work = AllocationScope::new(memory).unwrap();
            with_bail_fallback(environment, source, &parse_work, |session, mode| {
                assert_eq!(mode, PredictionMode::Sll);
                session.numeric_expression().map(|_| ())
            })?;
        }
        assert_eq!(memory.used_bytes(), baseline);
        let arena = Layout::array::<InvocationRecord<'_, '_>>(16).unwrap().size() + 256;
        let single = Layout::array::<ContextChild<'_, '_>>(1).unwrap().size() + 256;
        let capacity = arena + single;
        {
            let parse_work = AllocationScope::new(memory).unwrap();
            with_parser_session(environment, source, &parse_work, |session| {
                session.profile = ParserProfile::SllBail;
                let held = memory.reserve(memory.limit_bytes() - memory.used_bytes() - capacity).unwrap();
                let action = session.numeric_expression();
                let Err(PassError::Runtime(RuntimeError::Prediction(PredictionError::Admission(error)))) = action
                    else { panic!("binary terminal attachment denial"); };
                assert_eq!(error.code, "rtdb/memory-budget-exceeded");
                let request = format!("Cannot reserve {single} bytes from budget ");
                assert!(error.message.as_deref().unwrap().starts_with(request.as_str()));
                assert_eq!(session.records.len(), 6);
                for ordinal in [0, 2] {
                    assert!(matches!(session.records[ordinal].role, InvocationRole::RecursiveEntry { phase: RecursionPhase::Closed, .. }));
                }
                assert!(matches!(session.records[5].role, InvocationRole::Ordinary));

                let tree = session.invocation_view();
                let expr = tree.context(InvocationKey::at(1)).unwrap();
                let value = tree.context(InvocationKey::at(3)).unwrap();
                let literal = tree.context(child_rule(value.children())).unwrap();
                assert_eq!((value.class(), value.parent()), (Some(ContextClass::LiteralExpr), Some(expr.key())));
                assert_eq!((literal.key(), literal.rule(), literal.class(), literal.parent(), literal.invoking_state()),
                    (InvocationKey::at(5), LITERAL, Some(ContextClass::BinaryStringLiteral), Some(value.key()), ParserState::State(1140)));
                for view in [expr, value, literal] {
                    assert_eq!((view.start().unwrap().index(), view.stop().unwrap().index()), (0, 0));
                }
                assert_eq!(literal.stop().unwrap().lexeme().raw(), "x('aF01')");
                assert!(expr.children().is_empty() && literal.children().is_empty());
                assert_eq!((session.current(), session.state(), session.input_index(), session.precedence()),
                    (None, ParserState::State(1140), Some(1), 0));
                assert_eq!(session.buffered_token(1).unwrap().lexeme().raw(), ";");
                assert!(session.buffered_token(2).is_none());
                assert!(!session.matched_eof);
                drop(held);
                Ok(())
            })?;
        }
        assert_eq!(memory.used_bytes(), baseline);
        Ok::<(), RuntimeError<'_>>(())
    });
    result.unwrap();
    assert_eq!(memory.used_bytes(), 0);
}

fn interval_child<'a, 'parse, 'sql>(
    session: &'a ParserSession<'parse, 'sql, '_, '_>, parent: InvocationKey<'parse>, slot: usize, rule: u16, invoking: u32,
) -> super::super::RuleContextView<'a, 'parse, 'sql> {
    let tree = session.invocation_view();
    let parent_view = tree.context(parent).unwrap();
    let key = child_rule(&parent_view.children()[slot..=slot]);
    let child = tree.context(key).unwrap();
    assert_eq!((child.parent(), child.rule(), child.invoking_state()), (Some(parent), rule, ParserState::State(invoking)));
    child
}

#[test]
fn interval_literals_preserve_raw_strings_fields_signs_and_precision_in_both_profiles() {
    let ((), memory) = environment(|environment, work, _| {
        for (source, sign, raw, string_class, qualifier) in [
            ("INTERVAL 'P1D';", None, "'P1D'", ContextClass::SqlStandardString, None),
            ("iNtErVaL + e'1' HoUr;", Some("+"), "e'1'", ContextClass::CEscapesString, Some(("HoUr", None, None))),
            ("INTERVAL - $q$1$q$ MINUTE;", Some("-"), "$q$1$q$", ContextClass::DollarString, Some(("MINUTE", None, None))),
            ("INTERVAL '1' YEAR;", None, "'1'", ContextClass::SqlStandardString, Some(("YEAR", None, None))),
            ("INTERVAL '1' MONTH;", None, "'1'", ContextClass::SqlStandardString, Some(("MONTH", None, None))),
            ("INTERVAL '1' DAY;", None, "'1'", ContextClass::SqlStandardString, Some(("DAY", None, None))),
            ("INTERVAL '1' YEAR TO MONTH;", None, "'1'", ContextClass::SqlStandardString, Some(("YEAR", Some("MONTH"), None))),
            ("INTERVAL '1' DAY TO SECOND(3);", None, "'1'", ContextClass::SqlStandardString, Some(("DAY", Some("SECOND"), Some("3")))),
            ("INTERVAL '1' SECOND;", None, "'1'", ContextClass::SqlStandardString, Some(("SECOND", None, None))),
            ("INTERVAL '1' SECOND(03);", None, "'1'", ContextClass::SqlStandardString, Some(("SECOND", None, Some("03")))),
        ] {
            let string_start = 1 + usize::from(sign.is_some());
            let field_start = string_start + if string_class == ContextClass::DollarString { 3 } else { 1 };
            let last = field_start - 1 + qualifier.map_or(0, |(_, end, precision)| {
                1 + 2 * usize::from(end.is_some()) + 3 * usize::from(precision.is_some())
            });
            both_profiles(environment, source, work, |session, root| {
                let value = expr_value(session, root, last);
                let literal = interval_child(session, value, 0, LITERAL, 1140);
                assert_eq!(literal.class(), Some(ContextClass::IntervalLiteral0));
                assert_eq!(literal.children().len(), 1);
                let interval = interval_child(session, literal.key(), 0, INTERVAL_LITERAL, 698);
                assert_eq!(interval.class(), None);
                for view in [literal, interval] {
                    let start = view.start().unwrap();
                    let stop = view.stop().unwrap();
                    assert_eq!((start.index(), stop.index()), (0, last));
                    assert_eq!((start.lexeme().span().start.byte, stop.lexeme().span().end.byte), (0, source.len() - 1));
                }
                only_terminal(&interval.children()[..1], 0, source.split_whitespace().next().unwrap());
                if let Some(sign) = sign {
                    only_terminal(&interval.children()[1..2], 1, sign);
                }
                assert_eq!(interval.children().len(), string_start + 1 + usize::from(qualifier.is_some()));
                let string = interval_child(session, interval.key(), string_start, CHARACTER_STRING, 670);
                let start = string.start().unwrap();
                let stop = string.stop().unwrap();
                assert_eq!((string.class(), start.index(), stop.index()), (Some(string_class), string_start, field_start - 1));
                assert_eq!(&source[start.lexeme().span().start.byte..stop.lexeme().span().end.byte], raw);
                if string_class == ContextClass::DollarString {
                    assert_eq!(string.children().len(), 3);
                    only_terminal(&string.children()[..1], string_start, "$q$");
                    let text = interval_child(session, string.key(), 1, DOLLAR_STRING_TEXT, 719);
                    only_terminal(text.children(), string_start + 1, "1");
                    only_terminal(&string.children()[2..], string_start + 2, "$q$");
                } else {
                    only_terminal(string.children(), string_start, raw);
                }
                if let Some((first, end, precision)) = qualifier {
                    let q = interval_child(session, interval.key(), string_start + 1, INTERVAL_QUALIFIER, 671);
                    assert_eq!((q.start().unwrap().index(), q.stop().unwrap().index()), (field_start, last));
                    let (field, word, index) = if let Some(end) = end {
                        assert_eq!((q.class(), q.children().len()), (Some(ContextClass::MultiFieldInterval), 3));
                        let start = interval_child(session, q.key(), 0, NON_SECOND_DATETIME_FIELD, 725);
                        assert_eq!(start.class(), None);
                        only_terminal(start.children(), field_start, first);
                        only_terminal(&q.children()[1..2], field_start + 1, "TO");
                        (interval_child(session, q.key(), 2, SINGLE_DATETIME_FIELD, 727), end, field_start + 2)
                    } else {
                        assert_eq!((q.class(), q.children().len()), (Some(ContextClass::SingleFieldInterval), 1));
                        (interval_child(session, q.key(), 0, SINGLE_DATETIME_FIELD, 729), first, field_start)
                    };
                    assert_eq!((field.class(), field.start().unwrap().index(), field.stop().unwrap().index()), (None, index, last));
                    if let Some(digits) = precision {
                        assert_eq!(field.children().len(), 4);
                        only_terminal(&field.children()[..1], index, word);
                        only_terminal(&field.children()[1..2], index + 1, "(");
                        let precision = interval_child(session, field.key(), 2, INTERVAL_PRECISION, 739);
                        assert_eq!(precision.class(), None);
                        assert_eq!((precision.start().unwrap().index(), precision.stop().unwrap().index()), (index + 2, index + 2));
                        only_terminal(precision.children(), index + 2, digits);
                        only_terminal(&field.children()[3..], index + 3, ")");
                    } else if word == "SECOND" {
                        only_terminal(field.children(), index, word);
                    } else {
                        assert_eq!(field.children().len(), 1);
                        let unit = interval_child(session, field.key(), 0, NON_SECOND_DATETIME_FIELD, 736);
                        assert_eq!(unit.class(), None);
                        only_terminal(unit.children(), index, word);
                    }
                }
                assert_eq!(session.buffered_token(last + 1).unwrap().lexeme().raw(), ";");
            });
        }
    });
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn interval_sign_and_string_recovery_use_the_reached_rule_and_required_probe() {
    for (source, index, ll_state, expected) in [
        ("INTERVAL ) 'P1D'", 1, None, "extraneous input ')' expecting {CHARACTER_STRING, C_ESCAPES_STRING, '+', '-', DOLLAR_TAG}"),
        ("INTERVAL + )", 2, Some(ParserState::State(723)), "mismatched input ')' expecting {CHARACTER_STRING, C_ESCAPES_STRING, DOLLAR_TAG}"),
    ] {
        let (result, memory) = environment(|environment, work, _| {
            let mut modes = Vec::new();
            let result = with_bail_fallback(environment, source, work, |session, mode| {
                modes.push(mode);
                let root = InvocationKey::at(session.records.len() + 1);
                let action = session.scalar_literal(mode);
                let tree = session.invocation_view();
                let literal = tree.context(root).unwrap();
                assert_eq!(literal.class(), Some(ContextClass::IntervalLiteral0));
                let interval = interval_child(session, root, 0, INTERVAL_LITERAL, 698);
                assert_eq!(interval.class(), None);
                only_terminal(&interval.children()[..1], 0, "INTERVAL");
                if index == 2 {
                    only_terminal(&interval.children()[1..2], 1, "+");
                }
                assert_eq!((literal.stop().unwrap().index(), interval.stop().unwrap().index()), (index - 1, index - 1));
                if mode == PredictionMode::Sll || ll_state.is_some() {
                    assert_eq!(interval.children().len(), index + 1);
                    let string = interval_child(session, interval.key(), index, CHARACTER_STRING, 670);
                    assert_eq!((string.class(), string.start().unwrap().index(), string.stop().unwrap().index()), (None, index, index - 1));
                    assert!(string.children().is_empty());
                    if mode == PredictionMode::Sll {
                        assert!(matches!(action, Err(PassError::Cancelled(_))));
                        let cause = tree.exception(string.key()).unwrap();
                        assert!(matches!(cause, InvocationCause::NoViableAlternative(cause)
                            if cause.state == ParserState::State(723) && cause.start.index() == index && cause.offending.index() == index));
                        assert!(std::ptr::eq(cause, tree.exception(interval.key()).unwrap()));
                        assert!(std::ptr::eq(cause, tree.exception(root).unwrap()));
                    } else {
                        assert!(matches!(tree.exception(string.key()), Some(InvocationCause::InputMismatch(cause))
                            if cause.state == ParserState::State(723) && cause.offending.index() == index));
                    }
                } else {
                    assert_eq!(interval.children().len(), 1);
                    assert!(tree.exception(interval.key()).is_none());
                }
                if mode == PredictionMode::Sll {
                    assert!(session.buffered_token(index + 1).is_none());
                } else {
                    let next = if index == 1 { Kind::CharacterString } else { Kind::Eof };
                    assert_eq!(session.buffered_token(index + 1).unwrap().lexeme().kind(), next);
                    assert!(session.buffered_token(index + 2).is_none());
                    assert!(tree.exception(root).is_none());
                }
                assert_eq!((session.current(), session.state(), session.input_index(), session.matched_eof),
                    (None, ParserState::RootSentinel, Some(index), false));
                action.map(|_| ())
            });
            assert_eq!(modes, [PredictionMode::Sll, PredictionMode::Ll]);
            result
        });
        let Err(RuntimeError::ReportedTokenSyntax(report)) = result else { panic!("generated interval lexical diagnostic"); };
        assert_eq!(report.cause().map(|cause| cause.state), ll_state);
        assert_eq!((report.offending().index(), report.offending().lexeme().raw()), (index, ")"));
        assert_eq!(report.anomaly().code, "xtdb/sql-error");
        assert!(report.anomaly().message.as_deref().unwrap().ends_with(expected));
        drop(report);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn actual_rule18_precision_atom_keeps_inline_mismatch_and_deletion_boundaries() {
    for (source, raw, next, ll_state, expected) in [
        (")", ")", Kind::Eof, Some(ParserState::State(732)), "mismatched input ')' expecting UNSIGNED_INTEGER"),
        ("-1", "-", Kind::Integer, None, "extraneous input '-' expecting UNSIGNED_INTEGER"),
    ] {
        let (result, memory) = environment(|environment, work, _| {
            let mut modes = Vec::new();
            let result = with_bail_fallback(environment, source, work, |session, mode| {
                modes.push(mode);
                let root = InvocationKey::at(session.records.len());
                let action = session.interval_fractional_seconds_precision();
                let tree = session.invocation_view();
                let field = tree.context(root).unwrap();
                assert_eq!((field.rule(), field.class(), field.parent(), field.invoking_state()),
                    (INTERVAL_PRECISION, None, None, ParserState::RootSentinel));
                assert_eq!((field.start().unwrap().index(), field.start().unwrap().lexeme().raw()), (0, raw));
                assert!(field.children().is_empty() && field.stop().is_none());
                if mode == PredictionMode::Sll || ll_state.is_some() {
                    assert!(matches!(tree.exception(root), Some(InvocationCause::InputMismatch(cause))
                        if cause.state == ParserState::State(732) && cause.offending.index() == 0));
                } else {
                    assert!(tree.exception(root).is_none());
                }
                if mode == PredictionMode::Sll {
                    assert!(matches!(action, Err(PassError::Cancelled(_))));
                    assert!(session.buffered_token(1).is_none());
                } else {
                    assert_eq!(session.buffered_token(1).unwrap().lexeme().kind(), next);
                    assert!(session.buffered_token(2).is_none());
                }
                assert_eq!((session.current(), session.state(), session.input_index(), session.matched_eof),
                    (None, ParserState::RootSentinel, Some(0), false));
                action.map(|_| ())
            });
            assert_eq!(modes, [PredictionMode::Sll, PredictionMode::Ll]);
            result
        });
        let Err(RuntimeError::ReportedTokenSyntax(report)) = result else { panic!("generated precision diagnostic"); };
        assert_eq!(report.cause().map(|cause| cause.state), ll_state);
        assert_eq!((report.offending().index(), report.offending().lexeme().raw()), (0, raw));
        assert_eq!(report.anomaly().code, "xtdb/sql-error");
        assert!(report.anomaly().message.as_deref().unwrap().ends_with(expected));
        drop(report);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn warmed_interval_attachments_restore_only_the_reached_ordinary_caller_paths() {
    type Failed = (usize, u16, u32, usize, Option<usize>);
    type Exited = (usize, u16, u32, Option<ContextClass>);
    let cases: [(&str, [usize; 3], Failed, &[Exited]); 11] = [
        ("INTERVAL 'P1D';", [1, 0, 0], (6, INTERVAL_LITERAL, 698, 0, None),
            &[]),
        ("INTERVAL 'P1D';", [3, 0, 0], (7, CHARACTER_STRING, 670, 1, Some(0)),
            &[(6, INTERVAL_LITERAL, 698, None)]),
        ("INTERVAL $q$1$q$;", [4, 1, 0], (9, DOLLAR_STRING_TEXT, 719, 2, Some(1)),
            &[(8, CHARACTER_STRING, 670, Some(ContextClass::DollarString)), (6, INTERVAL_LITERAL, 698, None)]),
        ("INTERVAL '1' HOUR;", [4, 1, 0], (9, INTERVAL_QUALIFIER, 671, 2, Some(1)),
            &[(6, INTERVAL_LITERAL, 698, None)]),
        ("INTERVAL '1' DAY TO SECOND(3);", [4, 1, 1], (11, NON_SECOND_DATETIME_FIELD, 725, 2, Some(1)),
            &[(10, INTERVAL_QUALIFIER, 671, Some(ContextClass::MultiFieldInterval)), (6, INTERVAL_LITERAL, 698, None)]),
        ("INTERVAL '1' DAY TO SECOND(3);", [6, 2, 1], (12, SINGLE_DATETIME_FIELD, 727, 4, Some(3)),
            &[(10, INTERVAL_QUALIFIER, 671, Some(ContextClass::MultiFieldInterval)), (6, INTERVAL_LITERAL, 698, None)]),
        ("INTERVAL '1' HOUR;", [4, 1, 1], (11, SINGLE_DATETIME_FIELD, 729, 2, Some(1)),
            &[(10, INTERVAL_QUALIFIER, 671, Some(ContextClass::SingleFieldInterval)), (6, INTERVAL_LITERAL, 698, None)]),
        ("INTERVAL '1' HOUR;", [5, 1, 1], (12, NON_SECOND_DATETIME_FIELD, 736, 2, Some(1)),
            &[(11, SINGLE_DATETIME_FIELD, 729, None), (10, INTERVAL_QUALIFIER, 671, Some(ContextClass::SingleFieldInterval)), (6, INTERVAL_LITERAL, 698, None)]),
        ("INTERVAL '1' YEAR TO MONTH;", [6, 2, 2], (13, NON_SECOND_DATETIME_FIELD, 736, 4, Some(3)),
            &[(12, SINGLE_DATETIME_FIELD, 727, None), (10, INTERVAL_QUALIFIER, 671, Some(ContextClass::MultiFieldInterval)), (6, INTERVAL_LITERAL, 698, None)]),
        ("INTERVAL '1' SECOND(3);", [6, 2, 1], (12, INTERVAL_PRECISION, 739, 4, Some(3)),
            &[(11, SINGLE_DATETIME_FIELD, 729, None), (10, INTERVAL_QUALIFIER, 671, Some(ContextClass::SingleFieldInterval)), (6, INTERVAL_LITERAL, 698, None)]),
        ("INTERVAL '1' DAY TO SECOND(3);", [7, 3, 2], (13, INTERVAL_PRECISION, 739, 6, Some(5)),
            &[(12, SINGLE_DATETIME_FIELD, 727, None), (10, INTERVAL_QUALIFIER, 671, Some(ContextClass::MultiFieldInterval)), (6, INTERVAL_LITERAL, 698, None)]),
    ];
    for (source, slots, (ordinal, rule, invoking, start, stop), ancestors) in cases {
        let (result, memory) = environment(|environment, work, memory| {
            let baseline = work.memory_budget().used_bytes();
            {
                let parse_work = AllocationScope::new(memory).unwrap();
                with_bail_fallback(environment, source, &parse_work, |session, mode| {
                    assert_eq!(mode, PredictionMode::Sll);
                    session.numeric_expression().map(|_| ())
                })?;
            }
            assert_eq!(memory.used_bytes(), baseline);
            let arena = Layout::array::<InvocationRecord<'_, '_>>(16).unwrap().size() + 256;
            let single = Layout::array::<ContextChild<'_, '_>>(1).unwrap().size() + 256;
            let pair = Layout::array::<ContextChild<'_, '_>>(2).unwrap().size() + 256;
            let four = Layout::array::<ContextChild<'_, '_>>(4).unwrap().size() + 256;
            let capacity = arena + slots[0] * single + slots[1] * pair + slots[2] * four;
            {
                let parse_work = AllocationScope::new(memory).unwrap();
                with_parser_session(environment, source, &parse_work, |session| {
                    session.profile = ParserProfile::SllBail;
                    let held = memory.reserve(memory.limit_bytes() - memory.used_bytes() - capacity).unwrap();
                    let action = session.numeric_expression();
                    let Err(PassError::Runtime(RuntimeError::Prediction(PredictionError::Admission(error)))) = action
                        else { panic!("interval child attachment denial"); };
                    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
                    let request = format!("Cannot reserve {single} bytes from budget ");
                    assert!(error.message.as_deref().unwrap().starts_with(request.as_str()));
                    assert_eq!(session.records.len(), ordinal + 1);
                    assert!(matches!(session.records[ordinal].role, InvocationRole::OrdinaryTreeAttachmentFailed));
                    for entry in [0, 2] {
                        assert!(matches!(session.records[entry].role, InvocationRole::RecursiveEntry { phase: RecursionPhase::Closed, .. }));
                    }

                    let tree = session.invocation_view();
                    let expr = tree.context(InvocationKey::at(1)).unwrap();
                    let literal = tree.context(InvocationKey::at(5)).unwrap();
                    assert_eq!((literal.class(), literal.parent(), literal.invoking_state()),
                        (Some(ContextClass::IntervalLiteral0), Some(expr.key()), ParserState::State(1140)));
                    assert_eq!((literal.start().unwrap().index(), literal.stop().map(|token| token.index())), (0, stop));
                    let failed = tree.context(InvocationKey::at(ordinal)).unwrap();
                    assert_eq!((failed.rule(), failed.class(), failed.invoking_state()), (rule, None, ParserState::State(invoking)));
                    assert_eq!((failed.start().unwrap().index(), failed.stop().map(|token| token.index())), (start, stop));
                    assert!(expr.children().is_empty() && failed.children().is_empty());
                    let mut parent = failed.parent();
                    for &(key, rule, state, class) in ancestors {
                        assert_eq!(parent, Some(InvocationKey::at(key)));
                        assert!(matches!(session.records[key].role, InvocationRole::Ordinary));
                        let record = tree.context(InvocationKey::at(key)).unwrap();
                        assert_eq!((record.rule(), record.class(), record.invoking_state()), (rule, class, ParserState::State(state)));
                        assert!(record.start().is_some());
                        assert_eq!(record.stop().map(|token| token.index()), stop);
                        parent = record.parent();
                    }
                    assert_eq!(parent, Some(literal.key()));
                    assert_eq!((session.current(), session.state(), session.input_index(), session.precedence()),
                        (None, ParserState::State(698), Some(start), 0));
                    assert!(!session.matched_eof);
                    drop(held);
                    Ok(())
                })?;
            }
            assert_eq!(memory.used_bytes(), baseline);
            Ok::<(), RuntimeError<'_>>(())
        });
        result.unwrap();
        assert_eq!(memory.used_bytes(), 0);
    }
}
