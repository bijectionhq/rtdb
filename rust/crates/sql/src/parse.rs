use rtdb_plan::{Identifier, SourceSpan};
use rtdb_types::{Anomaly, Result};
use sqlparser::{ast as ast, dialect::GenericDialect, parser::Parser, tokenizer::{Location, Span, Token, TokenWithSpan, Tokenizer}};

#[derive(Clone, Debug)]
pub struct ParsedStatement {
    pub span: SourceSpan,
    parameter_count: usize,
    pub(crate) statement: Syntax,
    pub(crate) temporal: Vec<TemporalClause>,
    pub(crate) inline_xtql: Vec<crate::inline_xtql::InlineXtql>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementKind { Query, Dml, Session }

impl ParsedStatement {
    pub fn parameter_count(&self) -> usize { self.parameter_count }
    pub fn kind(&self) -> StatementKind {
        match &self.statement {
            Syntax::Standard(ast::Statement::Query(_)) => StatementKind::Query,
            Syntax::Records { .. } | Syntax::CreateTable { .. } | Syntax::GrantRole { .. }
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
    CreateTable { table: ast::ObjectName, columns: Vec<Identifier> },
    GrantRole { user: Identifier, role: Identifier, revoke: bool },
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

pub fn parse(sql: &str) -> Result<Vec<ParsedStatement>> {
    let dialect = GenericDialect {};
    let tokens = Tokenizer::new(&dialect, sql).tokenize_with_location().map_err(|error| sql_error(error.to_string(), sql, Span::new(error.location, error.location)))?;
    let tokens: Vec<_> = tokens.into_iter().filter(|token| !matches!(token.token, Token::Whitespace(_))).collect();
    let mut statements = Vec::new();
    for slice in tokens.split(|token| token.token == Token::SemiColon) {
        if slice.is_empty() { continue; }
        let span = source_span(sql, Span::new(slice[0].span.start, slice[slice.len() - 1].span.end));
        let (tokens, inline_xtql, parameter_count) = crate::inline_xtql::extract(slice, sql)?;
        let tokens = quantified_values(tokens, sql)?;
        let (mut tokens, temporal) = extract_temporal(tokens, sql)?;
        let statement = if word(tokens.first(), "PATCH") || (word(tokens.first(), "INSERT") && tokens.iter().any(|token| word(Some(token), "RECORDS"))) {
            let patch = word(tokens.first(), "PATCH");
            let start = if word(tokens.get(1), "INTO") { 2 } else { 1 };
            let mut parser = Parser::new(&dialect).with_tokens_with_locations(tokens[start..].to_vec());
            let table = parser.parse_object_name(false).map_err(|error| sql_error(error.to_string(), sql, slice[0].span))?;
            if !word(Some(&parser.next_token()), "RECORDS") { return Err(sql_error("Expected RECORDS after mutation target", sql, slice[0].span)); }
            let records = parser.parse_comma_separated(|parser| parser.parse_expr()).map_err(|error| sql_error(error.to_string(), sql, slice[0].span))?;
            if parser.peek_token().token != Token::EOF { return Err(sql_error("Unexpected input after RECORDS", sql, parser.peek_token().span)); }
            Syntax::Records { patch, table, records }
        } else if word(tokens.first(), "CREATE") && (word(tokens.get(1), "TABLE") || (word(tokens.get(1), "OR") && word(tokens.get(2), "ALTER") && word(tokens.get(3), "TABLE"))) {
            let start = if word(tokens.get(1), "TABLE") { 2 } else { 4 };
            let mut parser = Parser::new(&dialect).with_tokens_with_locations(tokens[start..].to_vec());
            let table = parser.parse_object_name(false).map_err(|error| sql_error(error.to_string(), sql, slice[0].span))?;
            let mut columns = Vec::new();
            if parser.consume_token(&Token::LParen) && !parser.consume_token(&Token::RParen) {
                loop {
                    columns.push(identifier_token(&parser.next_token())?);
                    if parser.consume_token(&Token::RParen) { break; }
                    parser.expect_token(&Token::Comma).map_err(|error| sql_error(error.to_string(), sql, parser.peek_token().span))?;
                }
            }
            if parser.peek_token().token != Token::EOF { return Err(sql_error("Unexpected input after CREATE TABLE", sql, parser.peek_token().span)); }
            Syntax::CreateTable { table, columns }
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
            let mut parser = Parser::new(&dialect).with_tokens_with_locations(tokens[1..].to_vec());
            let expression = parser.parse_expr().map_err(|error| sql_error(error.to_string(), sql, slice[0].span))?;
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
        statements.push(ParsedStatement { span, parameter_count, statement, temporal, inline_xtql });
    }
    Ok(statements)
}

fn quantified_values(tokens: Vec<TokenWithSpan>, source: &str) -> Result<Vec<TokenWithSpan>> {
    let mut insertions = std::collections::BTreeMap::new();
    for index in 0..tokens.len() {
        if !["ANY", "ALL", "SOME"].into_iter().any(|name| word(tokens.get(index), name))
            || tokens.get(index + 1).is_none_or(|token| token.token != Token::LParen)
            || !word(tokens.get(index + 2), "VALUES") { continue; }
        let mut depth = 1usize;
        let mut end = index + 2;
        while end < tokens.len() {
            match tokens[end].token {
                Token::LParen => depth += 1,
                Token::RParen => { depth -= 1; if depth == 0 { break; } },
                _ => {},
            }
            end += 1;
        }
        if end == tokens.len() { return Err(sql_error("Unclosed quantified subquery", source, tokens[index].span)); }
        for (at, text) in [(index + 2, "SELECT * FROM ("), (end, ") AS _rtdb_quantified_values")] {
            let injected = Tokenizer::new(&GenericDialect {}, text).tokenize().map_err(|error| Anomaly::fault("rtdb.sql/lexer", error.to_string()))?;
            insertions.insert(at, injected.into_iter().filter(|token| !matches!(token, Token::Whitespace(_))).map(|token| TokenWithSpan { token, span: tokens[at].span }).collect::<Vec<_>>());
        }
    }
    if insertions.is_empty() { return Ok(tokens); }
    let mut result = Vec::new();
    for (index, token) in tokens.into_iter().enumerate() {
        if let Some(inserted) = insertions.remove(&index) { result.extend(inserted); }
        result.push(token);
    }
    Ok(result)
}

fn standard(tokens: Vec<TokenWithSpan>, source: &str) -> Result<ast::Statement> {
    let at = tokens.first().map(|token| token.span).unwrap_or_else(Span::empty);
    let mut statements = Parser::new(&GenericDialect {}).with_tokens_with_locations(tokens).parse_statements().map_err(|error| sql_error(error.to_string(), source, at))?;
    if statements.len() != 1 { return Err(sql_error("Expected one SQL statement", source, at)); }
    Ok(statements.remove(0))
}

fn extract_temporal(tokens: Vec<TokenWithSpan>, source: &str) -> Result<(Vec<TokenWithSpan>, Vec<TemporalClause>)> {
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

fn expression(tokens: &[TokenWithSpan], source: &str, precedence: u8) -> Result<(ast::Expr, usize)> {
    let dialect = GenericDialect {};
    let mut parser = Parser::new(&dialect).with_tokens_with_locations(tokens.to_vec());
    let expression = parser.parse_subexpr(precedence).map_err(|error| sql_error(error.to_string(), source, parser.peek_token().span))?;
    Ok((expression, parser.index()))
}

fn word(token: Option<&TokenWithSpan>, expected: &str) -> bool {
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

pub(crate) fn sql_error(message: impl Into<String>, source: &str, span: Span) -> Anomaly {
    let span = source_span(source, span);
    Anomaly::incorrect("xtdb/sql-error", message).with_context("position", serde_json::json!(span.start + 1)).with_context("span", serde_json::json!({"start":span.start,"end":span.end}))
}
