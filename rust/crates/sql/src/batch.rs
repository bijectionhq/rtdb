use std::{alloc::Layout, sync::Arc};
use rtdb_plan::SourceSpan;
use rtdb_types::{Anomaly, MemoryBudget, MemoryReservation, Result};
use sqlparser::tokenizer::{Location, Span, Token, TokenWithSpan};

use crate::{
    DeferredStatementSyntax, ParsedControl,
    bind::FormAllocation,
    control::{RetainedControlSyntax, parse_control_tokens, prepare_name},
    parse::{RetainedStatementSyntax, StatementSource, parse_statement_syntax, sql_error, validate_common_syntax},
    split::SplitTokens,
};

/// The commands in a complete SQL batch and the reservation for their array.
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
    /// Keep this reservation live until preparation consumes or discards the command.
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
    /// The unparsed statement source selected for a frontend response.
    FrontendResponse(&'input str),
}

impl<'input> ExecutableSyntax<'input> {
    pub fn source(&self) -> &'input str {
        match self {
            Self::Sql(syntax) => syntax.source(),
            Self::Control(syntax) => syntax.source(),
            Self::FrontendResponse(source) => source,
        }
    }
}

/// Extended Parse classification with its grammar allocation live for the caller.
#[derive(Debug)]
pub struct ExtendedParse<'input> {
    pub statement: ExtendedStatement<'input>,
    _allocation: Arc<MemoryReservation>,
}

#[derive(Debug)]
pub enum ExtendedStatement<'input> {
    Empty,
    Multiple,
    Ordinary(&'input str),
    Prepare(SqlPrepareSyntax),
}

/// One reusable PREPARE wrapper with its original input and admitted raw child syntax.
#[derive(Clone, Debug)]
pub struct SqlPrepareSyntax {
    input: Arc<SqlPrepareInput>,
    layer: usize,
}

#[derive(Debug)]
struct SqlPrepareInput {
    text: String,
    layers: Vec<SqlPrepareLayer>,
    leaf: SqlPrepareLeaf,
    _source_allocation: Arc<MemoryReservation>,
    _syntax_allocation: Arc<MemoryReservation>,
}

#[derive(Debug)]
struct SqlPrepareLayer {
    name: String,
    span: SourceSpan,
}

#[derive(Debug)]
enum SqlPrepareLeaf {
    Sql(RetainedStatementSyntax),
    Control(RetainedControlSyntax),
}

#[derive(Debug)]
pub enum SqlPrepareChild<'input> {
    Prepare(SqlPrepareSyntax),
    Command { syntax: ExecutableSyntax<'input>, allocation: Arc<MemoryReservation> },
}

impl SqlPrepareSyntax {
    /// Returns the name registered on execution of this wrapper.
    pub fn name(&self) -> &str { &self.input.layers[self.layer].name }

    /// Returns the original text of this wrapper, excluding surrounding statement separators.
    pub fn source(&self) -> &str {
        let span = self.input.layers[self.layer].span;
        &self.input.text[span.start..span.end]
    }

    /// Returns the next wrapper or admits a fresh copy of the reached leaf using the supplied budget.
    /// Keep the command's allocation live until consuming its syntax.
    pub fn child<'input>(&'input self, memory: &'input Arc<MemoryBudget>) -> Result<SqlPrepareChild<'input>> {
        let layer = self.layer + 1;
        if layer < self.input.layers.len() {
            return Ok(SqlPrepareChild::Prepare(Self { input: Arc::clone(&self.input), layer }));
        }
        let span = match &self.input.leaf {
            SqlPrepareLeaf::Sql(syntax) => syntax.span(),
            SqlPrepareLeaf::Control(syntax) => syntax.span(),
        };
        let allocation = memory.reserve(command_size(span.end - span.start)?)?;
        let syntax = match &self.input.leaf {
            SqlPrepareLeaf::Sql(syntax) => ExecutableSyntax::Sql(syntax.with_input(&self.input.text)),
            SqlPrepareLeaf::Control(syntax) => ExecutableSyntax::Control(syntax.with_input(&self.input.text, memory)),
        };
        Ok(SqlPrepareChild::Command { syntax, allocation })
    }
}

/// Lexes the complete input and classifies its nonempty ranges.
/// A lone PREPARE validates every wrapper and its leaf and retains a copy of the input under the caller's budget.
/// An ordinary range borrows the input for the caller's own parsing policy.
pub fn parse_extended_statement<'input>(source: &'input str, memory: &Arc<MemoryBudget>) -> Result<ExtendedParse<'input>> {
    let allocation = memory.reserve(command_size(source.len())?)?;
    let mut tokens = crate::parse::tokenize(source)?;
    tokens.retain(|token| !matches!(token.token, Token::Whitespace(_) | Token::EOF));
    let mut ranges = tokens.split(|token| token.token == Token::SemiColon).filter(|range| !range.is_empty());
    let statement = match (ranges.next(), ranges.next()) {
        (None, _) => ExtendedStatement::Empty,
        (_, Some(_)) => ExtendedStatement::Multiple,
        (Some(range), None) => {
            let bounded = StatementSource::tokens(source, &range[0], &range[range.len() - 1]);
            if !crate::parse::word(range.first(), "PREPARE") {
                ExtendedStatement::Ordinary(bounded.text())
            } else {
                validate_common_syntax(range, source)?;
                let end = bounded.span().end;
                let (layers, command) = parse_batch_statement(
                    source, range, end, &mut SourceOffsets::new(source), FormAllocation::Tracked(memory),
                )?;
                let layers = layers.into_iter().map(|layer| SqlPrepareLayer {
                    name: layer.name,
                    span: SourceSpan { start: end - layer.source.len(), end },
                }).collect();
                let leaf = match command {
                    ExecutableSyntax::Sql(syntax) => SqlPrepareLeaf::Sql(syntax.into_retained()),
                    ExecutableSyntax::Control(syntax) => SqlPrepareLeaf::Control(syntax.into_retained()),
                    ExecutableSyntax::FrontendResponse(_) => unreachable!("PREPARE body grammar"),
                };
                let source_allocation = memory.reserve(source.len().checked_add(256).ok_or_else(batch_size)?)?;
                ExtendedStatement::Prepare(SqlPrepareSyntax { layer: 0, input: Arc::new(SqlPrepareInput {
                    text: source.to_owned(), layers, leaf,
                    _source_allocation: source_allocation,
                    _syntax_allocation: Arc::clone(&allocation),
                }) })
            }
        }
    };
    Ok(ExtendedParse { statement, _allocation: allocation })
}

/// Parses every statement before returning. Inline XTQL text and option values remain unresolved.
pub fn parse_statement_batch<'input>(source: &'input str, memory: &'input Arc<MemoryBudget>) -> Result<StatementBatch<'input>> {
    parse_statement_batch_with_frontend_response(source, memory, |_| false)
}

/// Validates the full batch lexically before classifying statement ranges.
/// Calls the selector in source order on each nonempty semicolon-delimited range it reaches.
/// The selector receives the original text from the first through the last significant token.
/// A matching range bypasses grammar and becomes `ExecutableSyntax::FrontendResponse` with an empty PREPARE list.
/// Parses the grammar of each remaining range before returning.
pub fn parse_statement_batch_with_frontend_response<'input>(
    source: &'input str, memory: &'input Arc<MemoryBudget>, is_frontend_response: impl Fn(&str) -> bool,
) -> Result<StatementBatch<'input>> {
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
        let start = starts.offset(first.span.start);
        let end = ends.offset(last.span.end);
        let range_source = &source[start..end];
        let frontend_response = is_frontend_response(range_source);
        if !frontend_response { validate_common_syntax(range, source)?; }
        let allocation = memory.reserve(command_size(end - start)?)?;
        let (preparations, command) = if frontend_response {
            (Vec::new(), ExecutableSyntax::FrontendResponse(range_source))
        } else {
            parse_batch_statement(source, range, end, &mut starts, forms)?
        };
        statements.push(BatchStatement { preparations, command, allocation });
        next += range.len() + 1;
    }
    Ok(StatementBatch { statements: statements.into_iter(), _storage: storage })
}

fn batch_size() -> Anomaly { Anomaly::incorrect("rtdb.sql/batch-size", "SQL batch exceeds addressable memory") }

fn command_size(len: usize) -> Result<usize> {
    len.checked_mul(128).and_then(|bytes| bytes.checked_add(8192)).ok_or_else(batch_size)
}

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

    #[test]
    fn extended_prepare_shares_its_input_and_admits_each_reached_leaf_copy() {
        let memory = MemoryBudget::new(1 << 20);
        let (root, input_len) = {
            let input = String::from("-- début\r\nPREPARE outer_q AS PREPARE inner_q AS SELECT $1;");
            let parsed = parse_extended_statement(&input, &memory).unwrap();
            let ExtendedStatement::Prepare(root) = parsed.statement else { panic!("PREPARE"); };
            (root, input.len())
        };
        let retained = command_size(input_len).unwrap() + input_len + 256;
        assert_eq!(memory.used_bytes(), retained);
        assert_eq!(root.name(), "outer_q");
        assert_eq!(root.source(), "PREPARE outer_q AS PREPARE inner_q AS SELECT $1");
        let SqlPrepareChild::Prepare(inner) = root.child(&memory).unwrap() else { panic!("nested PREPARE"); };
        assert!(Arc::ptr_eq(&root.input, &inner.input));
        assert_eq!(inner.name(), "inner_q");
        assert_eq!(inner.source(), "PREPARE inner_q AS SELECT $1");
        drop(root);
        assert_eq!(memory.used_bytes(), retained);

        let clone_bytes = command_size("SELECT $1".len()).unwrap();
        let held = memory.reserve(memory.limit_bytes() - retained - clone_bytes + 1).unwrap();
        assert_eq!(inner.child(&memory).unwrap_err().code, "rtdb/memory-budget-exceeded");
        drop(held);
        assert_eq!(memory.used_bytes(), retained);

        for _ in 0..2 {
            let SqlPrepareChild::Command { syntax, allocation } = inner.child(&memory).unwrap()
                else { panic!("prepared command"); };
            assert_eq!(memory.used_bytes(), retained + clone_bytes);
            let ExecutableSyntax::Sql(syntax) = syntax else { panic!("SQL"); };
            let statement = syntax.resolve_in(&memory).unwrap();
            assert_eq!(statement.source(), "SELECT $1");
            assert_eq!(statement.parameter_count(), 1);
            drop(statement);
            drop(allocation);
            assert_eq!(memory.used_bytes(), retained);
        }
        drop(inner);
        assert_eq!(memory.used_bytes(), 0);
    }
}
