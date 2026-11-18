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
