use std::sync::Arc;
use rtdb_types::{AllocationScope, MemoryBudget};
use crate::parser::grammar::{ExpectedSymbol, OwnedGrammar};
use crate::parser::prediction::PredictionStorage;
use crate::parser::prediction::closure::{ClosureOptions, LoopEntryOption, SourceAssertions};
use super::*;
use super::super::{
    CapturedClassPolicy, ContextChild, ParserEnvironment, ParserProfile, RuntimeError, with_bail_fallback, with_parser_session,
};
use super::super::super::PredictionMode;

fn environment<T>(run: impl FnOnce(&mut ParserEnvironment, &AllocationScope) -> T) -> (T, Arc<MemoryBudget>) {
    let grammar_memory = MemoryBudget::new(8 << 20);
    let grammar = OwnedGrammar::load(&grammar_memory).unwrap();
    let retained = MemoryBudget::new(64 << 20);
    let memory = MemoryBudget::new(64 << 20);
    let work = AllocationScope::new(&memory).unwrap();
    let policy = CapturedClassPolicy::from_captured(ClosureOptions {
        pruning: LoopEntryOption::Enabled, assertions: SourceAssertions::Enabled,
    });
    let mut environment = ParserEnvironment::new(PredictionStorage::new(grammar, &retained), policy, &work).unwrap();
    let result = run(&mut environment, &work);
    drop(environment);
    drop(work);
    assert_eq!(retained.used_bytes(), 0);
    assert_eq!(grammar_memory.used_bytes(), 0);
    (result, memory)
}

fn root<'parse, 'sql>(session: &mut ParserSession<'parse, 'sql, '_, '_>, multi: bool) -> PassResult<'parse, 'sql, InvocationKey<'parse>> {
    if multi { session.multi_sql_statement() } else { session.direct_sql_statement() }
}

/// Rules and terminal spellings in pre-order, so a test reads the whole offset/limit subtree.
fn shape<'parse>(session: &ParserSession<'parse, '_, '_, '_>, key: InvocationKey<'parse>, out: &mut Vec<String>) {
    let tree = session.invocation_view();
    let view = tree.context(key).unwrap();
    out.push(format!("({}", view.rule()));
    for child in view.children() {
        match *child {
            ContextChild::Context(child) => shape(session, child, out),
            ContextChild::Terminal(token) => out.push(token.lexeme().raw().to_owned()),
        }
    }
    out.push(")".into());
}

fn offset_subtree(rendered: &[String]) -> String {
    let start = rendered.iter().position(|item| item == "(130").expect("offsetAndLimit context");
    let mut depth = 0;
    let mut end = start;
    for (index, item) in rendered.iter().enumerate().skip(start) {
        if item.starts_with('(') { depth += 1 } else if item == ")" { depth -= 1 }
        if depth == 0 { end = index; break; }
    }
    rendered[start..=end].join(" ")
}

#[test]
fn offset_and_limit_sites_agree_with_the_parser_atn() {
    let ((), memory) = environment(|environment, work| {
        let grammar = environment.storage.grammar_view();
        for call in [
            LEADING_OFFSET_CALL, TRAILING_FETCH_CALL, LEADING_FETCH_CALL, TRAILING_OFFSET_CALL, OFFSET_COUNT_CALL,
            FETCH_COUNT_CALL, LIMIT_COUNT_CALL, OFFSET_PARAMETER_CALL, FETCH_PARAMETER_CALL,
        ] {
            assert!(call.agrees(&grammar), "call at {}", call.state);
        }
        for terminal in [
            OFFSET_KEYWORD, OFFSET_ROWS, FETCH_KEYWORD, FETCH_DIRECTION, FETCH_ROWS, FETCH_ONLY, LIMIT_KEYWORD,
            OFFSET_INTEGER, FETCH_INTEGER, DYNAMIC_PARAMETER, POSTGRES_PARAMETER_TERMINAL,
        ] {
            assert!(terminal.read(&grammar).is_some(), "terminal at {}", terminal.state);
        }
        for site in [
            LIMIT_BASE, MAY_TRAILING_FETCH, MAY_TRAILING_OFFSET, MAY_OFFSET_ROWS, FETCH_BASE, MAY_FETCH_COUNT,
            OFFSET_COUNT_BASE, FETCH_COUNT_BASE, PARAMETER_BASE,
        ] {
            let state = grammar.state(site.state).unwrap();
            assert_eq!(state.rule().number(), site.rule);
            assert!(site.shape.agrees(state.kind()), "sync at {}", site.state);
            let first = state.next_tokens(work).unwrap();
            assert_eq!(first.contains(ExpectedSymbol::Epsilon), site.shape.nullable(), "sync at {}", site.state);
        }
    });
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn offset_and_limit_trees_match_in_both_statement_roots_and_profiles() {
    let cases = [
        ("SELECT 1 LIMIT 2", "(130 (132 LIMIT (134 2 ) ) )"),
        ("SELECT 1 LIMIT ?", "(130 (132 LIMIT (134 (39 ? ) ) ) )"),
        ("SELECT 1 OFFSET $1 ROWS", "(130 (131 OFFSET (133 (39 $1 ) ) ROWS ) )"),
        ("SELECT 1 OFFSET 3 LIMIT 2", "(130 (131 OFFSET (133 3 ) ) (132 LIMIT (134 2 ) ) )"),
        ("SELECT 1 LIMIT 2 OFFSET 3 ROW", "(130 (132 LIMIT (134 2 ) ) (131 OFFSET (133 3 ) ROW ) )"),
        ("SELECT 1 FETCH FIRST 2 ROWS ONLY", "(130 (132 FETCH FIRST (134 2 ) ROWS ONLY ) )"),
        ("SELECT 1 OFFSET 1 FETCH NEXT ROW ONLY", "(130 (131 OFFSET (133 1 ) ) (132 FETCH NEXT ROW ONLY ) )"),
    ];
    for (sql, expected) in cases {
        let ((), memory) = environment(|environment, work| {
            for multi in [false, true] {
                with_bail_fallback(environment, sql, work, |session, mode| {
                    assert_eq!(mode, PredictionMode::Sll, "{sql}");
                    let root = root(session, multi)?;
                    let mut rendered = Vec::new();
                    shape(session, root, &mut rendered);
                    assert_eq!(offset_subtree(&rendered), expected, "{sql}");
                    Ok(())
                }).unwrap();
                with_parser_session(environment, sql, work, |session| {
                    session.profile = ParserProfile::LlDefault;
                    let root = root(session, multi).unwrap();
                    let mut rendered = Vec::new();
                    shape(session, root, &mut rendered);
                    assert_eq!(offset_subtree(&rendered), expected, "{sql}");
                    Ok(())
                }).unwrap();
            }
        });
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn limit_all_reports_the_row_count_mismatch_at_all() {
    let ((), memory) = environment(|environment, work| {
        for (sql, multi) in [("SELECT 1 LIMIT ALL", false), ("SELECT 1 LIMIT ALL; SELEC 7", true), ("SELECT 1 OFFSET ALL", true)] {
            let result = with_bail_fallback(environment, sql, work, |session, _| {
                root(session, multi)?;
                Ok(())
            });
            let Err(RuntimeError::ReportedTokenSyntax(report)) = result else { panic!("expected a token report for {sql}") };
            assert_eq!(report.offending().lexeme().raw(), "ALL");
            assert_eq!(report.anomaly().code, "xtdb/sql-error");
            let column = sql.find("ALL").unwrap();
            assert_eq!(report.anomaly().message.as_deref(), Some(format!(
                "Errors parsing SQL statement:\n  - line 1:{column} mismatched input 'ALL' expecting {{UNSIGNED_INTEGER, POSTGRES_PARAMETER_SPECIFICATION, '?'}}"
            ).as_str()), "{sql}");
            drop(report);
        }
    });
    assert_eq!(memory.used_bytes(), 0);
}
