use std::{alloc::Layout, sync::Arc};
use rtdb_plan::SourceSpan;
use rtdb_types::{Anomaly, MemoryBudget, MemoryReservation, Result};
use sqlparser::tokenizer::{Location, Span, Token, TokenWithSpan};

use crate::{
    DeferredStatementSyntax, ParsedControl,
    bind::FormAllocation,
    control::{parse_control_tokens, prepare_name},
    parse::{StatementSource, parse_statement_syntax, sql_error, validate_common_syntax},
    split::SplitTokens,
};

/// The syntax of a complete SQL batch and the reservation for its retained trees.
#[derive(Debug)]
pub struct StatementBatch<'input> {
    statements: std::vec::IntoIter<BatchStatement<'input>>,
    _storage: Arc<MemoryReservation>,
}

impl<'input> Iterator for StatementBatch<'input> {
    type Item = BatchStatement<'input>;

    fn next(&mut self) -> Option<Self::Item> { self.statements.next() }
}

#[derive(Debug)]
pub struct BatchStatement<'input> {
    /// SQL PREPARE wrappers in source order, outermost first.
    pub preparations: Vec<PrepareSyntax<'input>>,
    pub command: ExecutableSyntax<'input>,
    /// Retained until this command's syntax has been consumed.
    pub allocation: Arc<MemoryReservation>,
}

#[derive(Debug)]
pub struct PrepareSyntax<'input> {
    pub name: String,
    pub source: &'input str,
}

#[derive(Debug)]
pub enum ExecutableSyntax<'input> {
    Sql(DeferredStatementSyntax<'input>),
    Control(ParsedControl<'input>),
}

impl<'input> ExecutableSyntax<'input> {
    pub fn source(&self) -> &'input str {
        match self {
            Self::Sql(syntax) => syntax.source(),
            Self::Control(syntax) => syntax.source(),
        }
    }
}

/// Parses every statement before returning. Inline XTQL text and option values remain unresolved.
pub fn parse_statement_batch<'input>(source: &'input str, memory: &'input Arc<MemoryBudget>) -> Result<StatementBatch<'input>> {
    let mut tokens = SplitTokens::tokenize_in(source, memory)?;
    tokens.values.retain(|token| !matches!(token.token, Token::Whitespace(_) | Token::EOF));
    let mut end = tokens.values.len();
    if tokens.values.last().is_some_and(|token| token.token == Token::SemiColon) { end -= 1; }
    if end == 0 {
        return Err(sql_error("Expected an SQL statement", source, tokens.values.last().map_or_else(Span::empty, |token| token.span)));
    }
    let tokens = &tokens.values[..end];
    let ranges = tokens.split(|token| token.token == Token::SemiColon);
    let count = ranges.clone().count();
    let bytes = Layout::array::<BatchStatement<'_>>(count).map_err(|_| batch_size())?.size();
    let storage = memory.reserve(bytes.checked_add(256).ok_or_else(batch_size)?)?;
    let mut statements = Vec::new();
    statements.try_reserve_exact(count).map_err(|error| Anomaly::incorrect("rtdb/allocation-failed", error.to_string()))?;
    let (mut starts, mut ends) = (SourceOffsets::new(source), SourceOffsets::new(source));
    let forms = FormAllocation::Tracked(memory);
    let mut next = 0;
    for range in ranges {
        let (Some(first), Some(last)) = (range.first(), range.last()) else {
            let span = tokens.get(next).or_else(|| tokens.last()).map_or_else(Span::empty, |token| token.span);
            return Err(sql_error("Expected an SQL statement between semicolons", source, span));
        };
        validate_common_syntax(range, source)?;
        let start = starts.offset(first.span.start);
        let end = ends.offset(last.span.end);
        let bytes = (end - start).checked_mul(128).and_then(|bytes| bytes.checked_add(8192)).ok_or_else(batch_size)?;
        let allocation = memory.reserve(bytes)?;
        let (preparations, command) = parse_batch_statement(source, range, end, &mut starts, forms)?;
        statements.push(BatchStatement { preparations, command, allocation });
        next += range.len() + 1;
    }
    Ok(StatementBatch { statements: statements.into_iter(), _storage: storage })
}

fn batch_size() -> Anomaly { Anomaly::incorrect("rtdb.sql/batch-size", "SQL batch exceeds addressable memory") }

fn parse_batch_statement<'input>(
    input: &'input str, mut tokens: &[TokenWithSpan], end: usize, starts: &mut SourceOffsets<'input>, forms: FormAllocation<'input>,
) -> Result<(Vec<PrepareSyntax<'input>>, ExecutableSyntax<'input>)> {
    let mut preparations = Vec::new();
    while let Some(name) = prepare_name(input, tokens)? {
        let start = starts.offset(tokens[0].span.start);
        preparations.push(PrepareSyntax { name, source: &input[start..end] });
        tokens = &tokens[3..];
    }
    let start = starts.offset(tokens[0].span.start);
    let span = SourceSpan { start, end };
    let source = StatementSource::bounded(input, span);
    let command = match parse_control_tokens(source, tokens, forms)? {
        Some(control) => ExecutableSyntax::Control(control),
        None => ExecutableSyntax::Sql(parse_statement_syntax(source, span, tokens)?),
    };
    Ok((preparations, command))
}

struct SourceOffsets<'input> {
    remaining: std::str::Chars<'input>,
    location: Location,
    offset: usize,
}

impl<'input> SourceOffsets<'input> {
    fn new(input: &'input str) -> Self { Self { remaining: input.chars(), location: Location::new(1, 1), offset: 0 } }

    fn offset(&mut self, target: Location) -> usize {
        while self.location < target {
            let Some(ch) = self.remaining.next() else { break; };
            self.offset += ch.len_utf8();
            if ch == '\n' {
                self.location = Location::new(self.location.line + 1, 1);
            } else {
                self.location.column += 1;
            }
        }
        self.offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_rejects_later_grammar_before_reading_an_earlier_xtql_literal() {
        let memory = MemoryBudget::new(1 << 20);
        for source in [
            "XTQL $$($$; SELEC 7",
            "SET TIME ZONE 'UTC'; PREPARE p AS PREPARE q AS SELEC 7",
            "SET TIME ZONE 'UTC'; BEGIN READ ONLY, CLOCK_TIME = )",
            "SELECT 1;; SELECT 2",
            "SELECT 1;; /* trailing */",
            "/* no statement */",
        ] {
            assert_eq!(parse_statement_batch(source, &memory).unwrap_err().code, "xtdb/sql-error", "{source}");
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn batch_preparations_keep_original_unicode_source_ranges() {
        let memory = MemoryBudget::new(1 << 20);
        let source = "SELECT 'é';\n/* ☕ */ PREPARE outer_q AS PREPARE inner_q AS\n  XTQL $$($$;";
        let mut batch = parse_statement_batch(source, &memory).unwrap();
        let first = batch.next().unwrap();
        assert_eq!(first.command.source(), "SELECT 'é'");
        let retained = memory.used_bytes();
        drop(first);
        assert!(memory.used_bytes() < retained);

        let second = batch.next().unwrap();
        assert_eq!(second.preparations[0].name, "outer_q");
        assert_eq!(second.preparations[0].source, "PREPARE outer_q AS PREPARE inner_q AS\n  XTQL $$($$");
        assert_eq!(second.preparations[1].name, "inner_q");
        assert_eq!(second.preparations[1].source, "PREPARE inner_q AS\n  XTQL $$($$");
        let ExecutableSyntax::Sql(syntax) = second.command else { panic!("SQL"); };
        let error = syntax.resolve_in(&MemoryBudget::new(0)).unwrap_err();
        assert_eq!(error.code, "xtql/edn-reader");
        assert_eq!(error.context["position"], serde_json::json!(source.find("$$").unwrap() + 1));
        assert!(memory.used_bytes() > 0);
        drop(second.preparations);
        drop(second.allocation);
        drop(batch);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn batch_operand_admission_uses_the_reached_statement_budget() {
        let grammar_budget = MemoryBudget::new(1 << 20);
        let operand_budget = MemoryBudget::new(0);
        let source = r#"XTQL $$(-> (rel [{}] []) (return {:v (regexp-replace "abc" "a" "x")}))$$"#;
        let mut batch = parse_statement_batch(source, &grammar_budget).unwrap();
        let BatchStatement { command, allocation, .. } = batch.next().unwrap();
        let grammar_bytes = grammar_budget.used_bytes();
        let ExecutableSyntax::Sql(syntax) = command else { panic!("SQL"); };
        let error = syntax.resolve_in(&operand_budget).unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(grammar_budget.used_bytes(), grammar_bytes);
        assert_eq!(operand_budget.used_bytes(), 0);
        drop(allocation);
        drop(batch);
        assert_eq!(grammar_budget.used_bytes(), 0);
    }
}
