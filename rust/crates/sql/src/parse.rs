use rtdb_plan::{Identifier, SourceSpan};
use rtdb_types::{Anomaly, MemoryBudget, Result};
use std::{ops::ControlFlow, sync::Arc};
use sqlparser::ast::VisitMut;
use sqlparser::{ast as ast, parser::{Parser, ParserError}, tokenizer::{Location, Span, Token, TokenWithSpan, TokenizeBufferError, Tokenizer}};
use crate::dialect::SqlDialect;

#[derive(Clone, Debug)]
pub struct ParsedStatement<'input> {
    pub span: SourceSpan,
    pub(crate) tree: StatementTree<StatementSource<'input>>,
    pub(crate) forms: crate::bind::FormAllocation<'input>,
    parameter_count: usize,
    pub(crate) inline_xtql: Vec<crate::inline_xtql::InlineXtql>,
}

/// A complete SQL syntax tree with pending argument assignments and embedded XTQL text.
#[derive(Clone, Debug)]
pub struct StatementSyntax<'input> {
    syntax: DeferredStatementSyntax<'input>,
    forms: crate::bind::FormAllocation<'input>,
}

/// Statement syntax whose operands will be resolved in a later preparation context.
#[derive(Clone, Debug)]
pub struct DeferredStatementSyntax<'input> {
    span: SourceSpan,
    tree: StatementTree<StatementSource<'input>>,
    parameters: crate::inline_xtql::SourceArguments,
}

#[derive(Debug)]
pub(super) struct RetainedStatementSyntax {
    span: SourceSpan,
    tree: StatementTree<SourceSpan>,
    parameters: crate::inline_xtql::SourceArguments,
}

/// A sequence of syntax trees from one complete SQL input.
#[derive(Debug)]
pub struct StatementSyntaxBatch<'input> {
    source: &'input str,
    forms: crate::bind::FormAllocation<'input>,
    statements: Vec<StatementSyntax<'input>>,
}

impl<'input> StatementSyntaxBatch<'input> {
    /// Returns the complete source supplied to the parser.
    pub fn source(&self) -> &'input str { self.source }

    /// Resolves statements in source order using the original budget.
    /// A different budget fails before resolution, including for empty input.
    /// A clone of the original `Arc` shares the same budget.
    pub fn resolve_in(self, memory: &Arc<MemoryBudget>) -> Result<Vec<ParsedStatement<'input>>> {
        require_syntax_budget(self.forms, memory)?;
        self.resolve()
    }

    fn resolve(self) -> Result<Vec<ParsedStatement<'input>>> {
        self.into_iter().map(StatementSyntax::resolve).collect()
    }
}

impl<'input> IntoIterator for StatementSyntaxBatch<'input> {
    type Item = StatementSyntax<'input>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter { self.statements.into_iter() }
}

impl<'input> StatementSyntax<'input> {
    /// Assigns source-order argument slots and reads inline XTQL using the originating metadata budget.
    pub fn resolve(self) -> Result<ParsedStatement<'input>> {
        self.syntax.resolve(self.forms)
    }
}

impl<'input> DeferredStatementSyntax<'input> {
    /// Returns the exact statement text from the batch.
    pub fn source(&self) -> &'input str { self.tree.source.text() }

    pub(super) fn into_retained(self) -> RetainedStatementSyntax {
        let source = self.tree.source.span();
        RetainedStatementSyntax { span: self.span, tree: self.tree.with_source(source), parameters: self.parameters }
    }

    /// Resolves the operands using the budget of the reached preparation context.
    pub fn resolve_in<'bind>(self, memory: &'bind Arc<MemoryBudget>) -> Result<ParsedStatement<'bind>> where 'input: 'bind {
        self.resolve(crate::bind::FormAllocation::Tracked(memory))
    }

    fn resolve<'bind>(mut self, forms: crate::bind::FormAllocation<'bind>) -> Result<ParsedStatement<'bind>> where 'input: 'bind {
        let crate::inline_xtql::ParameterAssignments { count, dynamic, inline } = self.parameters.assign(self.tree.source.input, forms)?;
        if !dynamic.is_empty() && let ControlFlow::Break(span) = self.tree.visit(&mut AssignParameters(&dynamic)) {
            return Err(sql_error("Invalid anonymous parameter occurrence", self.tree.source.input, span));
        }
        Ok(ParsedStatement { span: self.span, tree: self.tree, forms, parameter_count: count, inline_xtql: inline })
    }
}

impl RetainedStatementSyntax {
    pub(super) fn span(&self) -> SourceSpan { self.span }

    pub(super) fn with_input<'input>(&self, input: &'input str) -> DeferredStatementSyntax<'input> {
        let source = StatementSource::bounded(input, self.tree.source);
        DeferredStatementSyntax {
            span: self.span,
            tree: self.tree.clone().with_source(source),
            parameters: self.parameters.clone(),
        }
    }
}

fn require_syntax_budget(forms: crate::bind::FormAllocation<'_>, memory: &Arc<MemoryBudget>) -> Result<()> {
    if !matches!(forms, crate::bind::FormAllocation::Tracked(origin) if Arc::ptr_eq(origin, memory)) {
        return Err(Anomaly::incorrect("rtdb.sql/syntax-budget", "SQL syntax belongs to a different raw metadata budget"));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct StatementSource<'input> {
    pub(crate) input: &'input str,
    span: SourceSpan,
}

impl<'input> StatementSource<'input> {
    pub(super) fn full(input: &'input str) -> Self { Self { input, span: SourceSpan { start: 0, end: input.len() } } }

    pub(super) fn tokens(input: &'input str, first: &TokenWithSpan, last: &TokenWithSpan) -> Self {
        Self { input, span: source_span(input, Span::new(first.span.start, last.span.end)) }
    }

    pub(super) fn bounded(input: &'input str, span: SourceSpan) -> Self { Self { input, span } }
    pub(super) fn span(self) -> SourceSpan { self.span }
    pub(super) fn text(self) -> &'input str { &self.input[self.span.start..self.span.end] }
}

#[derive(Clone, Debug)]
pub(crate) struct StatementTree<S> {
    pub(crate) source: S,
    pub(crate) statement: Syntax,
    pub(crate) temporal: Vec<TemporalClause>,
    pub(crate) period_columns: std::collections::BTreeSet<String>,
    pub(crate) settings: crate::settings::ParsedSettings,
    pub(crate) wildcard_renames: Vec<crate::wildcard::WildcardRename>,
}

impl<S> StatementTree<S> {
    fn with_source<T>(self, source: T) -> StatementTree<T> {
        let Self { statement, temporal, period_columns, settings, wildcard_renames, .. } = self;
        StatementTree { source, statement, temporal, period_columns, settings, wildcard_renames }
    }
}

impl<S> VisitMut for StatementTree<S> {
    fn visit<V: ast::VisitorMut>(&mut self, visitor: &mut V) -> ControlFlow<V::Break> {
        match &mut self.statement {
            Syntax::Standard(statement) => statement.visit(visitor)?,
            Syntax::Erase(delete) => delete.visit(visitor)?,
            Syntax::Assert(value, _) | Syntax::Set { value, .. } => value.visit(visitor)?,
            Syntax::Records { records, .. } => records.visit(visitor)?,
            Syntax::CreateTable { .. } | Syntax::CreateSearchIndex { .. } | Syntax::DropSearchIndex { .. } | Syntax::CreateOrderedIndex { .. } | Syntax::DropOrderedIndex { .. } | Syntax::GrantRole { .. } | Syntax::Show(_) => (),
        }
        for clause in &mut self.temporal { clause.extent.visit(visitor)?; }
        self.settings.visit(visitor)
    }
}

struct AssignParameters<'a>(&'a [usize]);

impl ast::VisitorMut for AssignParameters<'_> {
    type Break = Span;

    fn pre_visit_value(&mut self, value: &mut ast::ValueWithSpan) -> ControlFlow<Span> {
        if let ast::Value::Placeholder(parameter) = &mut value.value
            && let Some(ordinal) = parameter.strip_prefix('?') {
            let Some(index) = ordinal.parse::<usize>().ok().and_then(|ordinal| self.0.get(ordinal)) else {
                return ControlFlow::Break(value.span);
            };
            *parameter = format!("${}", index + 1);
        }
        ControlFlow::Continue(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementKind { Query, Dml, Session }

impl<'input> ParsedStatement<'input> {
    /// Binds this syntax against the caller's catalog using its original source and raw metadata budget.
    pub fn bind(self, catalog: &dyn rtdb_plan::Catalog, options: &crate::PrepareOptions) -> Result<rtdb_plan::PreparedStatement> {
        crate::bind::bind(self, catalog, options)
    }

    /// Returns the original text for preparation and execution.
    /// The span refers to the complete input supplied to the parser.
    pub fn source(&self) -> &'input str { self.tree.source.text() }

    pub fn parameter_count(&self) -> usize { self.parameter_count }
    pub fn ordered_index_command_tag(&self) -> Option<&'static str> {
        match &self.tree.statement {
            Syntax::CreateOrderedIndex { .. } => Some("CREATE INDEX"),
            Syntax::DropOrderedIndex { .. } => Some("DROP INDEX"),
            _ => None,
        }
    }
    /// Identifies parsed reads and table mutations that cannot change database/storage configuration or execute a named prepared statement.
    /// This is a syntax classification, not authorization, binding, or proof that referenced tables exist.
    /// Session controls require their separate typed control parser.
    pub fn is_local_workload_statement(&self) -> bool {
        fn standard(statement: &ast::Statement) -> bool {
            match statement {
                ast::Statement::Query(_) | ast::Statement::Insert(_) | ast::Statement::Update(_) | ast::Statement::Delete(_) => true,
                ast::Statement::Explain { statement, .. } => standard(statement),
                _ => false,
            }
        }
        match &self.tree.statement {
            Syntax::Standard(statement) => standard(statement),
            Syntax::Erase(_) | Syntax::Assert(_, _) | Syntax::Records { .. } | Syntax::CreateTable { .. }
            | Syntax::CreateSearchIndex { .. } | Syntax::DropSearchIndex { .. } | Syntax::CreateOrderedIndex { .. } | Syntax::DropOrderedIndex { .. } => true,
            _ => false,
        }
    }
    pub fn kind(&self) -> StatementKind {
        match &self.tree.statement {
            Syntax::Standard(ast::Statement::Query(_) | ast::Statement::Explain { .. }) => StatementKind::Query,
            Syntax::Records { .. } | Syntax::CreateTable { .. } | Syntax::GrantRole { .. }
            | Syntax::CreateSearchIndex { .. } | Syntax::DropSearchIndex { .. } | Syntax::CreateOrderedIndex { .. } | Syntax::DropOrderedIndex { .. }
            | Syntax::Erase(_) | Syntax::Assert(_, _)
            | Syntax::Standard(ast::Statement::Insert(_) | ast::Statement::Update(_) | ast::Statement::Delete(_)) => StatementKind::Dml,
            _ => StatementKind::Session,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum Syntax {
    Standard(ast::Statement),
    Erase(ast::Delete),
    Assert(ast::Expr, Option<String>),
    Records { patch: bool, table: ast::ObjectName, records: Vec<ast::Expr> },
    CreateTable { table: ast::ObjectName, columns: Vec<(Identifier, Option<ast::DataType>)> },
    GrantRole { user: Identifier, role: Identifier, revoke: bool },
    CreateSearchIndex { name: Identifier, table: ast::ObjectName, columns: Vec<Identifier>, history: bool, if_not_exists: bool },
    DropSearchIndex { name: Identifier },
    CreateOrderedIndex { name: Identifier, table: ast::ObjectName, columns: Vec<Identifier>, if_not_exists: bool },
    DropOrderedIndex { name: Identifier },
    Set { name: Vec<Identifier>, value: ast::Expr, local: bool },
    Show(Vec<Identifier>),
}

#[derive(Clone, Debug)]
pub(crate) struct TemporalClause {
    pub owner_end: Location,
    pub system: bool,
    pub clamp: bool,
    pub extent: TemporalExtent,
}

#[derive(Clone, Debug)]
pub(crate) enum TemporalExtent {
    All,
    At(ast::Expr),
    FromTo(ast::Expr, Option<ast::Expr>),
    Between(ast::Expr, ast::Expr),
}

impl VisitMut for TemporalExtent {
    fn visit<V: ast::VisitorMut>(&mut self, visitor: &mut V) -> ControlFlow<V::Break> {
        match self {
            Self::All => (),
            Self::At(value) => value.visit(visitor)?,
            Self::FromTo(from, to) => { from.visit(visitor)?; to.visit(visitor)?; }
            Self::Between(from, to) => { from.visit(visitor)?; to.visit(visitor)?; }
        }
        ControlFlow::Continue(())
    }
}

pub(crate) fn tokenize(source: &str, memory: Option<&Arc<MemoryBudget>>) -> Result<Vec<TokenWithSpan>> {
    let dialect = SqlDialect::default();
    let mut tokens = Vec::new();
    tokenize_into(source, memory, Tokenizer::new(&dialect, source), &mut tokens, |tokens, token| {
        tokens.push(token);
        Ok(())
    })?;
    Ok(tokens)
}

pub(super) fn tokenize_boundaries_into(
    source: &str,
    memory: Option<&Arc<MemoryBudget>>,
    tokens: &mut Vec<TokenWithSpan>,
    append: impl FnMut(&mut Vec<TokenWithSpan>, TokenWithSpan) -> Result<()>,
) -> Result<()> {
    let dialect = SqlDialect::default();
    let tokenizer = Tokenizer::new(&dialect, source).without_comment_text();
    tokenize_into(source, memory, tokenizer, tokens, append)
}

fn tokenize_into(
    source: &str,
    memory: Option<&Arc<MemoryBudget>>,
    mut tokenizer: Tokenizer<'_>,
    tokens: &mut Vec<TokenWithSpan>,
    mut append: impl FnMut(&mut Vec<TokenWithSpan>, TokenWithSpan) -> Result<()>,
) -> Result<()> {
    let quoted_error = |span: Span| {
        let bytes = source_span(source, span);
        crate::lexer::RecognitionError::for_delimited_identifier(source, bytes.start, bytes.end)
            .map(|error| error.compatibility_anomaly(memory))
    };
    let mut invalid_token = None;
    let result = tokenizer.try_tokenize_with_location_into_buf(tokens, |tokens, token| {
        if invalid_token.is_none() {
            invalid_token = match &token.token {
                Token::Word(word) if word.quote_style.is_some() && word.value.contains('/') => quoted_error(token.span),
                Token::ArrowAt => Some(sql_error("Invalid character in SQL source", source,
                    Span::new(Location::new(token.span.start.line, token.span.start.column + 1), token.span.end))),
                Token::Char(_) | Token::AtSign | Token::AtAt | Token::AtArrow | Token::AtQuestion | Token::CaretAt =>
                    Some(sql_error("Invalid character in SQL source", source, token.span)),
                _ => None,
            };
        }
        append(tokens, token)
    });
    if let Some(error) = invalid_token { return Err(error); }
    result.map_err(|error| match error {
        TokenizeBufferError::Lexer(error) => {
            let at = Span::new(error.location, error.location);
            let start = source_span(source, at).start;
            crate::lexer::RecognitionError::for_delimited_identifier(source, start, source.len())
                .map(|error| error.compatibility_anomaly(memory))
                .unwrap_or_else(|| sql_error(error.to_string(), source, at))
        }
        TokenizeBufferError::Append(error) => error,
    })
}

/// Nesting the reference grammar parses: its generated parser has no depth limit, and the SQLite logic corpus nests boolean predicates
/// well beyond the vendor parser's default of 50. The vendor parser grows its stack as it recurses.
const STATEMENT_RECURSION_LIMIT: usize = 256;

pub fn parse(sql: &str) -> Result<Vec<ParsedStatement<'_>>> {
    parse_with(sql, crate::bind::FormAllocation::Untracked)
}

/// Parses statements while charging reached inline XTQL raw metadata to the existing caller budget.
/// Allocation/admission errors propagate when the parsed statements resolve their parameters.
pub fn parse_in<'input>(sql: &'input str, memory: &'input std::sync::Arc<rtdb_types::MemoryBudget>) -> Result<Vec<ParsedStatement<'input>>> {
    parse_with(sql, crate::bind::FormAllocation::Tracked(memory))
}

pub(crate) fn parse_with<'input>(sql: &'input str, forms: crate::bind::FormAllocation<'input>) -> Result<Vec<ParsedStatement<'input>>> {
    parse_syntax(sql, forms)?.resolve()
}

/// Parses the surrounding SQL and carries the caller's budget into `StatementSyntaxBatch::resolve_in`.
/// Resolution interprets embedded XTQL text.
pub fn parse_syntax_in<'input>(sql: &'input str, memory: &'input Arc<MemoryBudget>) -> Result<StatementSyntaxBatch<'input>> {
    parse_syntax(sql, crate::bind::FormAllocation::Tracked(memory))
}

fn parse_syntax<'input>(sql: &'input str, forms: crate::bind::FormAllocation<'input>) -> Result<StatementSyntaxBatch<'input>> {
    let tokens = tokenize(sql, forms.memory())?;
    let tokens: Vec<_> = tokens.into_iter().filter(|token| !matches!(token.token, Token::Whitespace(_))).collect();
    validate_common_syntax(&tokens, sql)?;
    let source = StatementSource::full(sql);
    let statements = tokens.split(|token| token.token == Token::SemiColon).filter(|slice| !slice.is_empty())
        .map(|slice| {
            let span = StatementSource::tokens(sql, &slice[0], &slice[slice.len() - 1]).span();
            Ok(StatementSyntax { syntax: parse_statement_syntax(source, span, slice)?, forms })
        }).collect::<Result<_>>()?;
    Ok(StatementSyntaxBatch { source: sql, forms, statements })
}

pub(super) fn validate_common_syntax(tokens: &[TokenWithSpan], sql: &str) -> Result<()> {
    for pair in tokens.windows(2) {
        if word(pair.first(),"LIMIT") && word(pair.get(1),"ALL") { return Err(sql_error("LIMIT requires an unsigned integer or parameter",sql,pair[1].span)); }
    }
    Ok(())
}

pub(super) fn parse_statement_syntax<'input>(
    source: StatementSource<'input>, span: SourceSpan, slice: &[TokenWithSpan],
) -> Result<DeferredStatementSyntax<'input>> {
    let sql = source.input;
    let dialect = SqlDialect::default();
    let (prefix, returning) = crate::returning::split(slice, sql)?;
    if let Some(returning) = returning { returning.validate(sql)?; }
    let period_columns = prefix.iter().filter_map(|token| match &token.token {
        Token::Word(word) => {
            let name = if word.quote_style.is_some() { word.value.clone() } else { word.value.to_lowercase() };
            matches!(name.as_str(), "_valid_time" | "_system_time").then_some(name)
        }
        _ => None,
    }).collect();
    let (tokens, parameters) = crate::inline_xtql::extract(prefix, sql)?;
    let (tokens, settings) = crate::settings::extract(tokens, sql)?;
    let (tokens, wildcard_renames) = crate::wildcard::extract(tokens, sql)?;
    let (tokens, materialized) = crate::materialized::cte_tokens(tokens, sql)?;
    let (mut tokens, mut temporal) = extract_temporal(tokens, sql)?;
    let mut statement = if word(tokens.first(), "PATCH") || (word(tokens.first(), "INSERT") && tokens.iter().any(|token| word(Some(token), "RECORDS"))) {
        let patch = word(tokens.first(), "PATCH");
        let start = if word(tokens.get(1), "INTO") { 2 } else { 1 };
        let mut parser = Parser::new(&dialect).with_recursion_limit(STATEMENT_RECURSION_LIMIT).with_tokens_with_locations(tokens[start..].to_vec());
        let table = parser.parse_object_name(false).map_err(|error| parser_error(error, sql, slice[0].span))?;
        if patch {
            let end = table.0.last().and_then(ast::ObjectNamePart::as_ident).map(|name| name.span.end);
            if temporal.iter().any(|clause| Some(clause.owner_end) == end && matches!(clause.extent, TemporalExtent::All)) {
                return Err(sql_error("PATCH valid time requires FROM with an optional TO", sql, slice[0].span));
            }
        }
        if !word(Some(&parser.next_token()), "RECORDS") { return Err(sql_error("Expected RECORDS after mutation target", sql, slice[0].span)); }
        let records = dialect.checked(parser.parse_comma_separated(|parser| parser.parse_expr())).map_err(|error| parser_error(error, sql, slice[0].span))?;
        if parser.peek_token().token != Token::EOF { return Err(sql_error("Unexpected input after RECORDS", sql, parser.peek_token().span)); }
        Syntax::Records { patch, table, records }
    } else if word(tokens.first(), "CREATE") && (word(tokens.get(1), "TABLE") || (word(tokens.get(1), "OR") && word(tokens.get(2), "ALTER") && word(tokens.get(3), "TABLE"))) {
        let start = if word(tokens.get(1), "TABLE") { 2 } else { 4 };
        let mut parser = Parser::new(&dialect).with_recursion_limit(STATEMENT_RECURSION_LIMIT).with_tokens_with_locations(tokens[start..].to_vec());
        let table = parser.parse_object_name(false).map_err(|error| parser_error(error, sql, slice[0].span))?;
        let mut columns = Vec::new();
        if parser.consume_token(&Token::LParen) && !parser.consume_token(&Token::RParen) {
            loop {
                let name = identifier_token(&parser.next_token())?;
                let ty = match parser.peek_token().token {
                    Token::Comma | Token::RParen => None,
                    _ => Some(dialect.checked(parser.parse_data_type()).map_err(|error| parser_error(error, sql, parser.peek_token().span))?),
                };
                columns.push((name, ty));
                if parser.consume_token(&Token::RParen) { break; }
                parser.expect_token(&Token::Comma).map_err(|error| parser_error(error, sql, parser.peek_token().span))?;
            }
        }
        if parser.peek_token().token != Token::EOF { return Err(sql_error("Unexpected input after CREATE TABLE", sql, parser.peek_token().span)); }
        Syntax::CreateTable { table, columns }
    } else if word(tokens.first(), "CREATE")
        && (word(tokens.get(1), "INDEX") || (word(tokens.get(1), "SEARCH") && word(tokens.get(2), "INDEX"))) {
        let search = word(tokens.get(1), "SEARCH");
        let first = if search { 3 } else { 2 };
        let mut parser = Parser::new(&dialect).with_tokens_with_locations(tokens[first..].to_vec());
        let if_not_exists = word(tokens.get(first), "IF")
            && word(tokens.get(first + 1), "NOT") && word(tokens.get(first + 2), "EXISTS");
        if if_not_exists {
            for _ in 0..3 { parser.next_token(); }
        }
        let name = identifier_token(&parser.next_token())?;
        if !word(Some(&parser.next_token()), "ON") { return Err(sql_error("Expected ON after the index name", sql, slice[0].span)); }
        let table = parser.parse_object_name(false).map_err(|error| parser_error(error, sql, slice[0].span))?;
        parser.expect_token(&Token::LParen).map_err(|error| parser_error(error, sql, parser.peek_token().span))?;
        let mut columns = Vec::new();
        loop {
            columns.push(identifier_token(&parser.next_token())?);
            if parser.consume_token(&Token::RParen) { break; }
            parser.expect_token(&Token::Comma).map_err(|error| parser_error(error, sql, parser.peek_token().span))?;
        }
        let mut history = true;
        if search && word(Some(&parser.peek_token()), "WITH") {
            parser.next_token();
            parser.expect_token(&Token::LParen).map_err(|error| parser_error(error, sql, parser.peek_token().span))?;
            let option = parser.next_token();
            if !word(Some(&option), "HISTORY") { return Err(sql_error("The only search index option is history", sql, option.span)); }
            parser.expect_token(&Token::Eq).map_err(|error| parser_error(error, sql, parser.peek_token().span))?;
            let value = parser.next_token();
            history = if word(Some(&value), "TRUE") { true } else if word(Some(&value), "FALSE") { false } else { return Err(sql_error("Expected TRUE or FALSE for history", sql, value.span)) };
            parser.expect_token(&Token::RParen).map_err(|error| parser_error(error, sql, parser.peek_token().span))?;
        }
        if parser.peek_token().token != Token::EOF { return Err(sql_error("Unexpected input after index definition", sql, parser.peek_token().span)); }
        if search { Syntax::CreateSearchIndex { name, table, columns, history, if_not_exists } }
        else { Syntax::CreateOrderedIndex { name, table, columns, if_not_exists } }
    } else if word(tokens.first(), "DROP")
        && (word(tokens.get(1), "INDEX") || (word(tokens.get(1), "SEARCH") && word(tokens.get(2), "INDEX"))) {
        let search = word(tokens.get(1), "SEARCH");
        let first = if search { 3 } else { 2 };
        if tokens.len() != first + 1 { return Err(sql_error("Expected one index name", sql, slice[0].span)); }
        let name = identifier_token(&tokens[first])?;
        if search { Syntax::DropSearchIndex { name } }
        else { Syntax::DropOrderedIndex { name } }
    } else if word(tokens.first(), "GRANT") || word(tokens.first(), "REVOKE") {
        let revoke = word(tokens.first(), "REVOKE");
        if tokens.len() != 4 || !word(tokens.get(2), if revoke { "FROM" } else { "TO" }) { return Err(sql_error("Expected one role and one user", sql, slice[0].span)); }
        Syntax::GrantRole { role: identifier_token(&tokens[1])?, user: identifier_token(&tokens[3])?, revoke }
    } else if word(tokens.first(), "ERASE") {
        let replacement = Tokenizer::new(&dialect, "DELETE").tokenize().map_err(|error| Anomaly::fault("rtdb.sql/lexer", error.to_string()))?;
        tokens[0].token = replacement.into_iter().next().ok_or_else(|| Anomaly::fault("rtdb.sql/lexer", "Missing DELETE token"))?;
        let statement = standard(tokens, sql)?;
        let ast::Statement::Delete(delete) = statement else { return Err(sql_error("ERASE must target a table", sql, slice[0].span)); };
        Syntax::Erase(delete)
    } else if word(tokens.first(), "ASSERT") {
        let mut parser = Parser::new(&dialect).with_recursion_limit(STATEMENT_RECURSION_LIMIT).with_tokens_with_locations(tokens[1..].to_vec());
        let expression = dialect.checked(parser.parse_expr()).map_err(|error| parser_error(error, sql, slice[0].span))?;
        let message = if parser.consume_token(&Token::Comma) {
            match parser.next_token().token { Token::SingleQuotedString(message) => Some(message), _ => return Err(sql_error("ASSERT message must be a string", sql, slice[0].span)) }
        } else { None };
        if parser.peek_token().token != Token::EOF { return Err(sql_error("Unexpected input after ASSERT", sql, parser.peek_token().span)); }
        Syntax::Assert(expression, message)
    } else if word(tokens.first(), "SHOW") {
        Syntax::Show(tokens[1..].iter().map(identifier_token).collect::<Result<_>>()?)
    } else if word(tokens.first(), "SET") && !word(tokens.get(1), "TRANSACTION") && !word(tokens.get(1), "ROLE") {
        let mut index = 1;
        let local = word(tokens.get(index), "LOCAL");
        if local || word(tokens.get(index), "SESSION") { index += 1; }
        let mut name = vec![identifier_token(tokens.get(index).ok_or_else(|| sql_error("SET requires a variable", sql, slice[0].span))?)?];
        index += 1;
        if name[0].normalized() == "time" && word(tokens.get(index), "ZONE") { name.push(identifier_token(&tokens[index])?); index += 1; }
        if tokens.get(index).is_some_and(|token| token.token == Token::Eq) || word(tokens.get(index), "TO") { index += 1; }
        let (value, used) = expression(&tokens[index..], sql, 0)?;
        if index + used != tokens.len() { return Err(sql_error("Unexpected input after SET", sql, tokens[index + used].span)); }
        Syntax::Set { name, value, local }
    } else { Syntax::Standard(standard(tokens, sql)?) };
    if !settings.is_empty() && !matches!(&statement, Syntax::Standard(ast::Statement::Query(_) | ast::Statement::Explain { .. })) {
        return Err(sql_error("SETTING requires a query expression", sql, slice[0].span));
    }
    crate::materialized::restore(&mut statement, &mut temporal, materialized, sql)?;
    let tree = StatementTree { source, statement, temporal, period_columns, settings, wildcard_renames };
    Ok(DeferredStatementSyntax { span, tree, parameters })
}

fn standard(tokens: Vec<TokenWithSpan>, source: &str) -> Result<ast::Statement> {
    let at = tokens.first().map(|token| token.span).unwrap_or_else(Span::empty);
    let dialect = SqlDialect::default();
    let mut statements = dialect.checked(Parser::new(&dialect).with_recursion_limit(STATEMENT_RECURSION_LIMIT).with_tokens_with_locations(tokens).parse_statements()).map_err(|error| parser_error(error, source, at))?;
    if statements.len() != 1 { return Err(sql_error("Expected one SQL statement", source, at)); }
    Ok(statements.remove(0))
}

pub(crate) fn extract_temporal(tokens: Vec<TokenWithSpan>, source: &str) -> Result<(Vec<TokenWithSpan>, Vec<TemporalClause>)> {
    let mut output: Vec<TokenWithSpan> = Vec::new();
    let mut temporal = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        if !word(tokens.get(index), "FOR") { output.push(tokens[index].clone()); index += 1; continue; }
        let start = index;
        let mut at = index + 1;
        let leading_all = word(tokens.get(at), "ALL");
        if leading_all { at += 1; }
        if word(tokens.get(at), "PORTION") {
            at += 1;
            if !word(tokens.get(at), "OF") { return Err(sql_error("Expected OF after PORTION", source, tokens[start].span)); }
            at += 1;
        }
        let system = word(tokens.get(at), "SYSTEM_TIME");
        if !system && !word(tokens.get(at), "VALID_TIME") { output.push(tokens[index].clone()); index += 1; continue; }
        at += 1;
        let owner_end = output.last().filter(|token| matches!(token.token, Token::Word(_))).map(|token| token.span.end).ok_or_else(|| sql_error("Temporal clause must follow a table name", source, tokens[start].span))?;
        let clamp = word(tokens.get(at), "ONLY");
        if clamp { at += 1; }
        let extent = if leading_all { TemporalExtent::All }
        else if word(tokens.get(at), "ALL") { at += 1; TemporalExtent::All }
        else if word(tokens.get(at), "AS") && word(tokens.get(at + 1), "OF") {
            at += 2;
            let (expr, used) = expression(&tokens[at..], source, 0)?; at += used;
            TemporalExtent::At(expr)
        } else if word(tokens.get(at), "FROM") {
            at += 1;
            let (from, used) = expression(&tokens[at..], source, 0)?; at += used;
            let to = if word(tokens.get(at), "TO") { at += 1; let (to, used) = expression(&tokens[at..], source, 0)?; at += used; Some(to) } else { None };
            TemporalExtent::FromTo(from, to)
        } else if word(tokens.get(at), "BETWEEN") {
            at += 1;
            let (from, used) = expression(&tokens[at..], source, 20)?; at += used;
            if !word(tokens.get(at), "AND") { return Err(sql_error("BETWEEN requires AND", source, tokens[start].span)); }
            at += 1;
            let (to, used) = expression(&tokens[at..], source, 20)?; at += used;
            TemporalExtent::Between(from, to)
        } else { return Err(sql_error("Invalid temporal extent", source, tokens[start].span)); };
        if temporal.iter().any(|previous: &TemporalClause| previous.owner_end == owner_end && previous.system == system) { return Err(sql_error("Duplicate temporal dimension", source, tokens[start].span)); }
        temporal.push(TemporalClause { owner_end, system, clamp, extent });
        index = at;
    }
    Ok((output, temporal))
}

pub(crate) fn expression(tokens: &[TokenWithSpan], source: &str, precedence: u8) -> Result<(ast::Expr, usize)> {
    let dialect = SqlDialect::default();
    let mut parser = Parser::new(&dialect).with_recursion_limit(STATEMENT_RECURSION_LIMIT).with_tokens_with_locations(tokens.to_vec());
    let expression = dialect.checked(parser.parse_subexpr(precedence)).map_err(|error| parser_error(error, source, parser.peek_token().span))?;
    Ok((expression, parser.index()))
}

pub(crate) fn word(token: Option<&TokenWithSpan>, expected: &str) -> bool {
    token.is_some_and(|token| matches!(&token.token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected)))
}

fn identifier_token(token: &TokenWithSpan) -> Result<Identifier> {
    match &token.token {
        Token::Word(word) => Ok(Identifier { text: word.value.clone(), quoted: word.quote_style.is_some() }),
        _ => Err(Anomaly::incorrect("xtdb/sql-error", "Expected an identifier")),
    }
}

pub(crate) fn source_span(source: &str, span: Span) -> SourceSpan {
    let offset = |location: Location| {
        if location.line == 0 { return 0; }
        let start: usize = source.split_inclusive('\n').take(location.line.saturating_sub(1) as usize).map(str::len).sum();
        start + source[start.min(source.len())..].chars().take(location.column.saturating_sub(1) as usize).map(char::len_utf8).sum::<usize>()
    };
    SourceSpan { start: offset(span.start), end: offset(span.end) }
}

pub(crate) fn parser_error(error: ParserError, source: &str, at: Span) -> Anomaly {
    match error {
        ParserError::TypeModifierNotAllowed(span) =>
            sql_error(format!("Errors parsing SQL statement:\n  - line {}:{} mismatched input '('",
                span.start.line, span.start.column.saturating_sub(1)), source, span),
        error => sql_error(error.to_string(), source, at),
    }
}

pub(crate) fn sql_error(message: impl Into<String>, source: &str, span: Span) -> Anomaly {
    let span = source_span(source, span);
    Anomaly::incorrect("xtdb/sql-error", message).with_context("position", serde_json::json!(span.start + 1)).with_context("span", serde_json::json!({"start":span.start,"end":span.end}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::GenericDialect;

    #[test]
    fn length_units_keep_lexical_parameter_occurrences_and_spans() {
        use sqlparser::ast::Spanned;

        let source = "SELECT 'é', CHAR_LENGTH(? USING CHARACTERS), CHARACTER_LENGTH($2 USING OCTETS), ?";
        let parsed = parse(source).unwrap();
        let [statement] = parsed.as_slice() else { panic!("one statement") };
        assert_eq!(statement.parameter_count(), 2);
        let Syntax::Standard(ast::Statement::Query(query)) = &statement.tree.statement else { panic!("query") };
        let ast::SetExpr::Select(select) = query.body.as_ref() else { panic!("select") };
        for (index, spelling, parameter, parameter_text, units) in [
            (1, "CHAR_LENGTH", "$1", "?", "CHARACTERS"),
            (2, "CHARACTER_LENGTH", "$2", "$2", "OCTETS"),
        ] {
            let ast::SelectItem::UnnamedExpr(ast::Expr::Function(function)) = &select.projection[index] else { panic!("function") };
            let ast::FunctionArguments::List(arguments) = &function.args else { panic!("argument list") };
            let [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(ast::Expr::Value(value)))] = arguments.args.as_slice() else { panic!("parameter argument") };
            let [ast::FunctionArgumentClause::UsingCharacterLengthUnits(unit)] = arguments.clauses.as_slice() else { panic!("unit clause") };
            assert_eq!(value.value, ast::Value::Placeholder(parameter.into()));
            for (span, expected) in [(function.name.span(), spelling), (value.span, parameter_text), (unit.span(), units)] {
                let span = source_span(source, span);
                assert_eq!(&source[span.start..span.end], expected);
            }
        }
        let ast::SelectItem::UnnamedExpr(ast::Expr::Value(last)) = &select.projection[3] else { panic!("last parameter") };
        assert_eq!(last.value, ast::Value::Placeholder("$2".into()));
        assert_eq!(source_span(source, last.span).start, source.rfind('?').unwrap());
        assert!(Parser::parse_sql(&GenericDialect {}, source).is_err());
    }

    #[test]
    fn substring_bare_operands_retain_parameter_slots_and_byte_spans() {
        for source in [
            "SELECT 'é', SUBSTRING(? FROM $2 ?), ?",
            "SELECT 'é', SUBSTRING(? $2 ?), ?",
        ] {
            assert_substring_parameter_slots_and_byte_spans(source);
        }
    }

    fn assert_substring_parameter_slots_and_byte_spans(source: &str) {
        let mut ordinary = parse(source).unwrap();
        assert_eq!(ordinary.len(), 1);

        let memory = MemoryBudget::new(1 << 20);
        let mut batch = crate::parse_statement_batch(source, &memory).unwrap();
        let crate::BatchStatement { command, allocation: _allocation, .. } = batch.next().unwrap();
        assert!(batch.next().is_none());
        let crate::ExecutableSyntax::Sql(deferred) = command else { panic!("SQL statement") };

        let mut positions = source.match_indices('?').map(|(position, _)| position);
        let source_argument = positions.next().unwrap();
        let start_argument = source.find("$2").unwrap();
        let length_argument = positions.next().unwrap();
        let last_argument = positions.next().unwrap();
        for statement in [ordinary.remove(0), deferred.resolve_in(&memory).unwrap()] {
            assert_eq!(statement.parameter_count(), 3);
            let Syntax::Standard(ast::Statement::Query(query)) = &statement.tree.statement else { panic!("query") };
            let ast::SetExpr::Select(select) = query.body.as_ref() else { panic!("select") };
            let [
                _,
                ast::SelectItem::UnnamedExpr(ast::Expr::Substring {
                    expr,
                    substring_from: Some(start),
                    substring_for: Some(length),
                    special: false,
                    shorthand: false,
                }),
                ast::SelectItem::UnnamedExpr(last),
            ] = select.projection.as_slice() else { panic!("substring and trailing parameter") };
            for (operand, assigned, original, at) in [
                (expr.as_ref(), "$1", "?", source_argument),
                (start.as_ref(), "$2", "$2", start_argument),
                (length.as_ref(), "$2", "?", length_argument),
                (last, "$3", "?", last_argument),
            ] {
                let ast::Expr::Value(value) = operand else { panic!("parameter expression") };
                assert_eq!(value.value, ast::Value::Placeholder(assigned.into()));
                let span = source_span(source, value.span);
                assert_eq!(span.start, at);
                assert_eq!(&source[span.start..span.end], original);
            }
        }
    }

    #[test]
    fn values_row_ast_retains_scalar_and_tuple_syntax() {
        use sqlparser::ast::Spanned;
        let source = "VALUES 1, (2, 3), ROW(), ()";
        let parsed = Parser::parse_sql(&SqlDialect::default(), source).unwrap();
        let [ast::Statement::Query(query)] = parsed.as_slice() else { panic!("VALUES query") };
        let ast::SetExpr::Values(values) = query.body.as_ref() else { panic!("VALUES constructor") };
        assert_eq!(values.to_string(), source);
        assert!(matches!(values.rows[0], ast::ValuesRow::Scalar(_)));
        for (row, expected) in values.rows.iter().zip(["1", "(2, 3)", "ROW()", "()"]) {
            let span = source_span(source, row.span());
            assert_eq!(&source[span.start..span.end], expected);
        }
        assert!(Parser::parse_sql(&GenericDialect {}, "VALUES 1").is_err());
    }

    #[test]
    fn both_tokenizer_paths_keep_quoted_identifier_spelling_and_literal_unescaping() {
        let source = r#""é""😀", `a````b`, 'x''y'"#;
        let full = tokenize(source, None).unwrap();
        let mut boundary = Vec::new();
        tokenize_boundaries_into(source, None, &mut boundary, |tokens, token| {
            tokens.push(token);
            Ok(())
        }).unwrap();
        assert_eq!(full, boundary);
        let identifier = |index: usize, text: &str, delimiter: char| {
            let TokenWithSpan { token: Token::Word(word), span } = &full[index] else { panic!("quoted identifier") };
            assert_eq!(word.value, text);
            assert_eq!(word.quote_style, Some(delimiter));
            let span = source_span(source, *span);
            assert_eq!(&source[span.start + 1..span.end - 1], text);
        };
        identifier(0, "é\"\"😀", '"');
        identifier(3, "a````b", '`');
        assert!(matches!(&full[6].token, Token::SingleQuotedString(text) if text == "x'y"));

        let dialect = GenericDialect {};
        let vendor_default = Tokenizer::new(&dialect, source).tokenize_with_location().unwrap();
        assert_eq!(vendor_default[0].span, full[0].span);
        assert!(matches!(&vendor_default[0].token, Token::Word(word) if word.value == "é\"😀"));
        assert!(matches!(&vendor_default[3].token, Token::Word(word) if word.value == "a``b"));
        assert_eq!(vendor_default[6], full[6]);
    }
}
