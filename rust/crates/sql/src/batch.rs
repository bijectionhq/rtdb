use std::{alloc::Layout, sync::Arc};
use rtdb_plan::SourceSpan;
use rtdb_types::{Anomaly, MemoryBudget, MemoryReservation, Result};
use sqlparser::tokenizer::{Location, Span, Token, TokenWithSpan};

use crate::{
    DeferredStatementSyntax, ParsedControl,
    bind::FormAllocation,
    control::{RetainedControlSyntax, parse_control_tokens, prepare_name},
    parse::{RetainedStatementSyntax, StatementSource, parse_statement_syntax, sql_error, validate_common_syntax},
    split::{SplitTokens, boundary_lexical_bytes},
};

/// The commands in a complete SQL batch and the reservation for their array.
#[derive(Debug)]
pub struct StatementBatch<'input> {
    statements: std::vec::IntoIter<BatchStatement<'input>>,
    prepare_source: BatchPrepareSource<'input>,
    _storage: Arc<MemoryReservation>,
}

impl<'input> StatementBatch<'input> {
    /// Takes the next command in source order.
    /// A pending PREPARE borrows this batch's source until its immediate child is selected.
    pub fn next_for_execution(&mut self) -> Option<BatchExecution<'_, 'input>> {
        let BatchStatement { preparations, command: syntax, allocation } = self.next()?;
        let mut layers = preparations.into_iter();
        Some(match layers.next() {
            Some(outer) => BatchExecution::Prepare(BatchPrepare {
                name: outer.name,
                statement: outer.source,
                layers,
                syntax,
                allocation,
                source: &mut self.prepare_source,
            }),
            None => BatchExecution::Command { syntax, allocation },
        })
    }
}

impl<'input> Iterator for StatementBatch<'input> {
    type Item = BatchStatement<'input>;

    fn next(&mut self) -> Option<Self::Item> { self.statements.next() }
    fn size_hint(&self) -> (usize, Option<usize>) { self.statements.size_hint() }
}

impl ExactSizeIterator for StatementBatch<'_> {}

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

/// The next command or outer SQL PREPARE in a validated batch.
pub enum BatchExecution<'batch, 'input> {
    /// Keep the reservation live until preparation consumes or discards the syntax.
    Command { syntax: ExecutableSyntax<'input>, allocation: Arc<MemoryReservation> },
    Prepare(BatchPrepare<'batch, 'input>),
}

/// An outer PREPARE with access to the source of its immediate child.
pub struct BatchPrepare<'batch, 'input> {
    name: String,
    statement: &'input str,
    layers: std::vec::IntoIter<PrepareSyntax<'input>>,
    syntax: ExecutableSyntax<'input>,
    allocation: Arc<MemoryReservation>,
    source: &'batch mut BatchPrepareSource<'input>,
}

impl<'input> BatchPrepare<'_, 'input> {
    /// The original outer PREPARE without surrounding statement separators.
    pub fn source(&self) -> &'input str { self.statement }

    /// Returns the registration name and its immediate child.
    /// Both child forms retain the grammar reservation.
    /// A terminal child borrows the input.
    /// The first nested child admits a shared copy of the input against the batch's memory budget and can fail if the reservation is denied.
    pub fn into_child(self) -> Result<(String, SqlPrepareChild<'input>)> {
        let Self { name, layers, syntax, allocation, source, .. } = self;
        let child = if layers.as_slice().is_empty() {
            SqlPrepareChild::Command { syntax, allocation }
        } else {
            let source = source.retain_in(allocation.budget())?;
            SqlPrepareChild::Prepare(SqlPrepareSyntax::from_parsed(source, layers, syntax, allocation))
        };
        Ok((name, child))
    }
}

#[derive(Debug)]
struct BatchPrepareSource<'input> {
    input: &'input str,
    retained: Option<Arc<SqlPrepareSource>>,
}

impl BatchPrepareSource<'_> {
    fn retain_in(&mut self, memory: &Arc<MemoryBudget>) -> Result<Arc<SqlPrepareSource>> {
        if let Some(source) = &self.retained {
            return Ok(Arc::clone(source));
        }
        let source = SqlPrepareSource::copy_in(self.input, memory)?;
        self.retained = Some(Arc::clone(&source));
        Ok(source)
    }
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

/// Extended Parse classification retaining the per-command allowance.
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
    source: Arc<SqlPrepareSource>,
    layers: Vec<SqlPrepareLayer>,
    leaf: SqlPrepareLeaf,
    _syntax_allocation: Arc<MemoryReservation>,
}

#[derive(Debug)]
struct SqlPrepareSource {
    text: String,
    _allocation: Arc<MemoryReservation>,
}

impl SqlPrepareSource {
    fn copy_in(input: &str, memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let allocation = memory.reserve(input.len().checked_add(256).ok_or_else(batch_size)?)?;
        Ok(Arc::new(Self { text: input.to_owned(), _allocation: allocation }))
    }
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

impl SqlPrepareLeaf {
    fn span(&self) -> SourceSpan {
        match self {
            Self::Sql(syntax) => syntax.span(),
            Self::Control(syntax) => syntax.span(),
        }
    }
}

#[derive(Debug)]
pub enum SqlPrepareChild<'input> {
    Prepare(SqlPrepareSyntax),
    Command { syntax: ExecutableSyntax<'input>, allocation: Arc<MemoryReservation> },
}

impl SqlPrepareSyntax {
    fn from_parsed<'input>(
        source: Arc<SqlPrepareSource>,
        layers: impl IntoIterator<Item = PrepareSyntax<'input>>,
        syntax: ExecutableSyntax<'input>,
        allocation: Arc<MemoryReservation>,
    ) -> Self {
        let leaf = match syntax {
            ExecutableSyntax::Sql(syntax) => SqlPrepareLeaf::Sql(syntax.into_retained()),
            ExecutableSyntax::Control(syntax) => SqlPrepareLeaf::Control(syntax.into_retained()),
            ExecutableSyntax::FrontendResponse(_) => unreachable!("PREPARE body grammar"),
        };
        let end = leaf.span().end;
        let layers = layers.into_iter().map(|layer| SqlPrepareLayer {
            name: layer.name,
            span: SourceSpan { start: end - layer.source.len(), end },
        }).collect();
        Self { input: Arc::new(SqlPrepareInput { source, layers, leaf, _syntax_allocation: allocation }), layer: 0 }
    }

    /// Returns the name registered on execution of this wrapper.
    pub fn name(&self) -> &str { &self.input.layers[self.layer].name }

    /// Returns the original text of this wrapper, excluding surrounding statement separators.
    pub fn source(&self) -> &str {
        let span = self.input.layers[self.layer].span;
        &self.input.source.text[span.start..span.end]
    }

    /// Returns the next wrapper or admits a fresh copy of the reached leaf using the supplied budget.
    /// Keep the command's allocation live until consuming its syntax.
    pub fn child<'input>(&'input self, memory: &'input Arc<MemoryBudget>) -> Result<SqlPrepareChild<'input>> {
        let layer = self.layer + 1;
        if layer < self.input.layers.len() {
            return Ok(SqlPrepareChild::Prepare(Self { input: Arc::clone(&self.input), layer }));
        }
        let span = self.input.leaf.span();
        let allocation = memory.reserve(command_size(span.end - span.start)?)?;
        let input = &self.input.source.text;
        let syntax = match &self.input.leaf {
            SqlPrepareLeaf::Sql(syntax) => ExecutableSyntax::Sql(syntax.with_input(input)),
            SqlPrepareLeaf::Control(syntax) => ExecutableSyntax::Control(syntax.with_input(input, memory)),
        };
        Ok(SqlPrepareChild::Command { syntax, allocation })
    }
}

/// Lexes the complete input and classifies its nonempty ranges.
/// A lone PREPARE validates every wrapper and its leaf and retains a copy of the input under the caller's budget.
/// An ordinary range borrows the input for the caller's own parsing policy.
/// The located token array holds its own reservation through classification and is released before return.
/// A refused reservation can precede later lexical or grammar errors.
pub fn parse_extended_statement<'input>(source: &'input str, memory: &Arc<MemoryBudget>) -> Result<ExtendedParse<'input>> {
    let allocation = memory.reserve(command_size(source.len())?)?;
    let mut tokens = SplitTokens::tokenize_in(source, memory)?;
    tokens.values.retain(|token| !matches!(token.token, Token::Whitespace(_) | Token::EOF));
    let mut ranges = tokens.values.split(|token| token.token == Token::SemiColon).filter(|range| !range.is_empty());
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
                let source = SqlPrepareSource::copy_in(source, memory)?;
                ExtendedStatement::Prepare(SqlPrepareSyntax::from_parsed(source, layers, command, Arc::clone(&allocation)))
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
    let _lexical = memory.reserve(boundary_lexical_bytes(source).ok_or_else(batch_size)?)?;
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
    statements.try_reserve_exact(count).map_err(|error| Anomaly::incorrect("rtdb/allocation-failed", error.to_string()).with_kind(rtdb_types::AnomalyKind::ResourceExhausted))?;
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
    Ok(StatementBatch {
        statements: statements.into_iter(),
        prepare_source: BatchPrepareSource { input: source, retained: None },
        _storage: storage,
    })
}

fn batch_size() -> Anomaly { Anomaly::incorrect("rtdb.sql/batch-size", "SQL batch exceeds addressable memory") }

fn command_size(len: usize) -> Result<usize> {
    crate::statement_syntax_estimate(len).ok_or_else(batch_size)
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
    fn batch_admits_lexer_work_before_token_capacity_and_validates_all_input_before_grammar() {
        let source = format!("SELEC '{}'; SELECT @", "é".repeat(4096));
        let lexical_bytes = boundary_lexical_bytes(&source).unwrap();
        let too_small = MemoryBudget::new(lexical_bytes - 1);
        let error = parse_statement_batch(&source, &too_small).unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(too_small.peak_bytes(), 0);
        assert_eq!(too_small.used_bytes(), 0);

        let lexer_only = MemoryBudget::new(lexical_bytes);
        let error = parse_statement_batch(&source, &lexer_only).unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(lexer_only.peak_bytes(), lexical_bytes);
        assert_eq!(lexer_only.used_bytes(), 0);

        let enough = MemoryBudget::new(1 << 20);
        let error = parse_statement_batch(&source, &enough).unwrap_err();
        assert_eq!(error.code, "xtdb/sql-error");
        assert_eq!(error.message.as_deref(), Some("Invalid character in SQL source"));
        let invalid = source.find('@').unwrap();
        assert_eq!(error.context["span"], serde_json::json!({"start": invalid, "end": invalid + 1}));
        assert_eq!(enough.used_bytes(), 0);
    }

    #[test]
    fn batch_retains_statement_storage_after_releasing_lexical_admission() {
        let source = "SELECT 'café'; SHOW TIME ZONE";
        let memory = MemoryBudget::new(1 << 20);
        let mut batch = parse_statement_batch(source, &memory).unwrap();
        let first = batch.next().unwrap();
        let second = batch.next().unwrap();
        assert_eq!(first.command.source(), "SELECT 'café'");
        assert_eq!(second.command.source(), "SHOW TIME ZONE");
        assert_eq!(memory.used_bytes(), batch._storage.bytes() + first.allocation.bytes() + second.allocation.bytes());
        assert!(memory.peak_bytes() > memory.used_bytes());
        drop(first);
        drop(second);
        drop(batch);
        assert_eq!(memory.used_bytes(), 0);
    }

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
    fn extended_parse_admits_replacement_token_array_before_releasing_previous_storage() {
        let source = "SELECT/* café; */ 1 ";
        let command = command_size(source.len()).unwrap();
        let first = Layout::array::<TokenWithSpan>(4).unwrap().size() + 256;
        let replacement = Layout::array::<TokenWithSpan>(8).unwrap().size() + 256;
        let peak = command + first + replacement;

        let short = MemoryBudget::new(peak - 1);
        let error = parse_extended_statement(source, &short).unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(short.peak_bytes(), command + first);
        assert_eq!(short.used_bytes(), 0);

        let exact = MemoryBudget::new(peak);
        let parsed = parse_extended_statement(source, &exact).unwrap();
        assert!(matches!(parsed.statement, ExtendedStatement::Ordinary("SELECT/* café; */ 1")));
        assert_eq!(exact.peak_bytes(), peak);
        assert_eq!(exact.used_bytes(), command);
        drop(parsed);
        assert_eq!(exact.used_bytes(), 0);
    }

    #[test]
    fn extended_parse_preserves_malformed_ordinary_and_control_text_for_preparation() {
        let memory = MemoryBudget::new(1 << 20);
        let sql = "SELEC/* café; */ 1";
        let control = "SET TIME /* café; */ ZONE (";
        for statement in [sql, control] {
            let source = format!(";; -- leading β\r\n{statement}; /* trailing */ ;");
            let parsed = parse_extended_statement(&source, &memory).unwrap();
            assert!(matches!(parsed.statement, ExtendedStatement::Ordinary(text) if text == statement));
            drop(parsed);
            assert_eq!(memory.used_bytes(), 0);
        }

        let error = crate::parse_syntax_in(sql, &memory).err().expect("malformed ordinary SQL");
        assert_eq!(error.code, "xtdb/sql-error");
        let error = crate::parse_admitted_control_in(control, &Default::default(), &memory).unwrap_err();
        assert_eq!(error.code, "xtdb/sql-error");
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn extended_parse_checks_full_lexical_input_before_nested_prepare_grammar() {
        let memory = MemoryBudget::new(1 << 20);
        let malformed = "/* café */ PREPARE outer_q AS PREPARE inner_q AS SELECT 1 LIMIT ALL";
        let error = parse_extended_statement(malformed, &memory).unwrap_err();
        assert_eq!(error.message.as_deref(), Some("LIMIT requires an unsigned integer or parameter"));
        assert_eq!(memory.used_bytes(), 0);

        let source = format!("{malformed}; -- trailing β\r\n@ 'unterminated");
        let error = parse_extended_statement(&source, &memory).unwrap_err();
        assert_eq!(error, crate::parse::tokenize(&source, Some(&memory)).unwrap_err());
        assert_eq!(error.message.as_deref(), Some("Invalid character in SQL source"));
        let invalid = source.find('@').unwrap();
        assert_eq!(error.context["span"], serde_json::json!({"start": invalid, "end": invalid + 1}));
        assert_eq!(memory.used_bytes(), 0);
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

    #[test]
    fn batch_prepare_copies_the_full_source_once_after_the_borrowed_child() {
        const SOURCE: &str = "SELECT 'café';\r\nPREPARE once_q AS SELECT 1;\n/* ☕ */ PREPARE alpha AS PREPARE alpha_q AS SELECT 7;\nPREPARE beta AS PREPARE beta_q AS SELECT 9";
        let input = SOURCE.to_owned();
        let memory = MemoryBudget::new(1 << 20);
        let mut batch = parse_statement_batch(&input, &memory).unwrap();
        let BatchExecution::Command { syntax, allocation } = batch.next_for_execution().unwrap()
            else { panic!("ordinary command"); };
        assert_eq!(syntax.source(), "SELECT 'café'");
        drop(syntax);
        drop(allocation);

        let before_prepare = memory.used_bytes();
        let held = memory.reserve(memory.limit_bytes() - before_prepare).unwrap();
        let BatchExecution::Prepare(once) = batch.next_for_execution().unwrap()
            else { panic!("one-layer PREPARE"); };
        let (name, SqlPrepareChild::Command { syntax, allocation }) = once.into_child().unwrap()
            else { panic!("borrowed child"); };
        assert_eq!(name, "once_q");
        assert_eq!(syntax.source(), "SELECT 1");
        assert_eq!(memory.used_bytes(), memory.limit_bytes());
        assert!(batch.prepare_source.retained.is_none());
        drop(held);
        let once_bytes = allocation.bytes();
        drop(syntax);
        drop(allocation);
        let grammar_bytes = memory.used_bytes();
        assert_eq!(grammar_bytes, before_prepare - once_bytes);

        let BatchExecution::Prepare(alpha) = batch.next_for_execution().unwrap()
            else { panic!("nested PREPARE"); };
        let (name, SqlPrepareChild::Prepare(alpha)) = alpha.into_child().unwrap()
            else { panic!("retained child"); };
        assert_eq!(name, "alpha");
        assert_eq!(alpha.source(), "PREPARE alpha_q AS SELECT 7");
        let source_bytes = SOURCE.len() + 256;
        assert_eq!(memory.used_bytes(), grammar_bytes + source_bytes);

        let BatchExecution::Prepare(beta) = batch.next_for_execution().unwrap()
            else { panic!("nested PREPARE"); };
        let (name, SqlPrepareChild::Prepare(beta)) = beta.into_child().unwrap()
            else { panic!("retained child"); };
        assert_eq!(name, "beta");
        assert_eq!(beta.source(), "PREPARE beta_q AS SELECT 9");
        assert_eq!(memory.used_bytes(), grammar_bytes + source_bytes);
        assert!(Arc::ptr_eq(&alpha.input.source, &beta.input.source));
        assert!(!Arc::ptr_eq(&alpha.input._syntax_allocation, &beta.input._syntax_allocation));

        let alpha_bytes = alpha.input._syntax_allocation.bytes();
        let beta_bytes = beta.input._syntax_allocation.bytes();
        drop(batch);
        drop(input);
        assert_eq!(memory.used_bytes(), source_bytes + alpha_bytes + beta_bytes);
        assert_eq!(beta.input.source.text, SOURCE);
        drop(alpha);
        assert_eq!(memory.used_bytes(), source_bytes + beta_bytes);
        {
            let SqlPrepareChild::Command { syntax, allocation } = beta.child(&memory).unwrap()
                else { panic!("terminal child"); };
            assert_eq!(syntax.source(), "SELECT 9");
            drop(syntax);
            drop(allocation);
        }
        drop(beta);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn denied_batch_prepare_source_releases_its_command_and_leaves_later_commands_reachable() {
        let source = "PREPARE alpha AS PREPARE alpha_q AS SELECT 7; PREPARE beta AS PREPARE beta_q AS SELECT 9";
        let memory = MemoryBudget::new(1 << 20);
        let mut batch = parse_statement_batch(source, &memory).unwrap();
        let grammar_bytes = memory.used_bytes();
        let alpha_bytes = batch.statements.as_slice()[0].allocation.bytes();
        let source_bytes = source.len() + 256;
        let held = memory.reserve(memory.limit_bytes() - grammar_bytes - source_bytes + 1).unwrap();
        let BatchExecution::Prepare(alpha) = batch.next_for_execution().unwrap()
            else { panic!("nested PREPARE"); };
        let error = alpha.into_child().unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert!(batch.prepare_source.retained.is_none());
        drop(held);
        assert_eq!(memory.used_bytes(), grammar_bytes - alpha_bytes);

        let BatchExecution::Prepare(beta) = batch.next_for_execution().unwrap()
            else { panic!("nested PREPARE"); };
        let (name, SqlPrepareChild::Prepare(beta)) = beta.into_child().unwrap()
            else { panic!("retained child"); };
        assert_eq!(name, "beta");
        assert_eq!(beta.input.source.text, source);
        drop(batch);
        drop(beta);
        assert_eq!(memory.used_bytes(), 0);
    }
}
