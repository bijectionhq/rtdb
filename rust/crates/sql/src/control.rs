use std::{collections::BTreeMap, mem::size_of, ops::ControlFlow, sync::Arc};

use rtdb_plan::{AccessMode, ControlAccessOptions, ControlCommitMode, ControlStatement, ControlValue, CopyFormat, CopyOutFormat, DiscardTarget, Identifier, ReadOnlyOptions, ReadWriteOptions, TransactionOptions};
use rtdb_types::{Anomaly, MemoryBudget, MemoryReservation, Result};
use sqlparser::{ast, parser::Parser, tokenizer::{Span, Token, TokenWithSpan}};
use sqlparser::ast::VisitMut;

use crate::{PrepareOptions, parse::{StatementSource, source_span, sql_error}, split::{SplitTokens, boundary_lexical_bytes}};

const CONTROL_EXPRESSION_RECURSION_LIMIT: usize = 50;

pub fn parse_control(source: &str, options: &PrepareOptions) -> Result<Option<ControlStatement>> {
    parse_control_with(source, options, crate::bind::FormAllocation::Untracked)
}

/// Binds control expressions in the caller's existing budget after parsing the command.
/// A non-control probe creates no raw owner; native allocation/admission errors propagate.
pub fn parse_control_in(source: &str, options: &PrepareOptions, memory: &std::sync::Arc<rtdb_types::MemoryBudget>) -> Result<Option<ControlStatement>> {
    parse_control_with(source, options, crate::bind::FormAllocation::Tracked(memory))
}

/// A bound control command that retains its parsing and binding allowance until dropped.
#[derive(Debug)]
pub struct AdmittedControl {
    statement: ControlStatement,
    _reservation: Arc<MemoryReservation>,
}

impl AdmittedControl {
    /// Borrows the command while its allowance is held.
    pub fn statement(&self) -> &ControlStatement { &self.statement }
}

/// Validates the full SQL input lexically, then parses and binds recognized controls.
/// With sufficient budget, a lexically valid input without a control head returns `None` and releases the classification allowance.
/// A refused reservation can return before syntax or binding errors.
pub fn parse_admitted_control_in(source: &str, options: &PrepareOptions, memory: &Arc<MemoryBudget>) -> Result<Option<AdmittedControl>> {
    let reservation;
    let syntax = {
        let lexical_bytes = boundary_lexical_bytes(source).ok_or_else(control_size)?;
        let _lexical = memory.reserve(lexical_bytes)?;
        let mut tokens = SplitTokens::tokenize_in(source, memory)?;
        if !probe_control(source, &mut tokens.values)? { return Ok(None); }

        reservation = memory.reserve(control_parser_bytes(source.len(), tokens.values.len())?)?;
        let position_bytes = position_parser_bytes(source.len(), &tokens.values)?;
        let _position = (position_bytes != 0).then(|| memory.reserve(position_bytes)).transpose()?;
        parse_control_tokens(StatementSource::full(source), &tokens.values, crate::bind::FormAllocation::Tracked(memory))?
    };
    syntax.map(|syntax| syntax.bind(options).map(|statement| AdmittedControl { statement, _reservation: reservation })).transpose()
}

fn control_size() -> Anomaly { Anomaly::incorrect("rtdb.sql/control-size", "SQL control statement exceeds addressable memory").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded) }

fn control_parser_bytes(source_len: usize, tokens: usize) -> Result<usize> {
    let per_token = 4 * (size_of::<TokenWithSpan>() + size_of::<ast::Expr>() + size_of::<rtdb_plan::Expr>()) + 64;
    source_len.checked_mul(8)
        .and_then(|payload| tokens.checked_mul(per_token)?.checked_add(payload)?.checked_add(8192))
        .ok_or_else(control_size)
}

fn position_parser_bytes(source_len: usize, tokens: &[TokenWithSpan]) -> Result<usize> {
    if word(tokens.first(), "PREPARE") { return Ok(0); }
    let sites = tokens.windows(2).filter(|pair| word(pair.first(), "POSITION") && pair[1].token == Token::LParen)
        .take(CONTROL_EXPRESSION_RECURSION_LIMIT).count();
    if sites == 0 { return Ok(0); }

    // Each nested POSITION needle parse keeps its token snapshots under the same parser recursion counter.
    let placeholder_bytes = 2 + usize::MAX.ilog10() as usize;
    let per_token = 3 * (size_of::<TokenWithSpan>() + placeholder_bytes);
    source_len.checked_mul(4)
        .and_then(|payload| tokens.len().checked_add(1)?.checked_mul(per_token)?.checked_add(payload)?.checked_mul(sites))
        .ok_or_else(control_size)
}

fn parse_control_with(source: &str, options: &PrepareOptions, forms: crate::bind::FormAllocation<'_>) -> Result<Option<ControlStatement>> {
    parse_control_syntax(source, forms)?.map(|syntax| syntax.bind(options)).transpose()
}

/// A parsed session command whose expressions bind against the later statement context.
#[derive(Debug)]
pub struct ParsedControl<'input> {
    source: StatementSource<'input>,
    forms: crate::bind::FormAllocation<'input>,
    syntax: ControlStatement<ast::Expr>,
}

#[derive(Debug)]
pub(super) struct RetainedControlSyntax {
    span: rtdb_plan::SourceSpan,
    syntax: ControlStatement<ast::Expr>,
}

impl<'input> ParsedControl<'input> {
    /// Returns the original text of this control command.
    pub fn source(&self) -> &'input str { self.source.text() }

    pub(super) fn into_retained(self) -> RetainedControlSyntax {
        RetainedControlSyntax { span: self.source.span(), syntax: self.syntax }
    }

    /// Determines whether Simple Query starts an implicit transaction around this command.
    pub fn starts_implicit_transaction(&self) -> bool {
        !self.syntax.is_show()
            && !matches!(self.syntax, ControlStatement::Begin(_) | ControlStatement::Discard(_)
                | ControlStatement::SetTransaction(_) | ControlStatement::CopyIn { .. })
            && !matches!(&self.syntax, ControlStatement::SetParameter { name, .. } if name == "bijection.at")
    }

    /// Compiles the control expressions using their original source and raw metadata budget.
    pub fn bind(self, options: &PrepareOptions) -> Result<ControlStatement> {
        bind_control_expressions(self.syntax, self.source.input, options, self.forms)
    }
}

pub(super) fn bind_control_expressions(
    syntax: ControlStatement<ast::Expr>, source: &str, options: &PrepareOptions, forms: crate::bind::FormAllocation<'_>,
) -> Result<ControlStatement> {
    let bind = |expression| crate::bind::control_expression(source, &expression, options, forms);
    syntax.try_map_expressions(bind, |expression| {
        match crate::settings::clock_timestamp_literal(&expression)? {
            Some(text) => Ok(ControlValue::TimestampLiteral(text)),
            None => bind(expression).map(ControlValue::Expression),
        }
    })
}

pub(super) fn visit_control_expressions<V: ast::VisitorMut>(
    control: &mut ControlStatement<ast::Expr>, visitor: &mut V,
) -> ControlFlow<V::Break> {
    match control {
        ControlStatement::Begin(options) | ControlStatement::SetTransaction(options) => {
            let mut visit = |value: &mut Option<ControlValue<ast::Expr>>| -> ControlFlow<V::Break> {
                if let Some(ControlValue::Expression(expression)) = value { expression.visit(visitor)?; }
                ControlFlow::Continue(())
            };
            visit(&mut options.timezone)?;
            match &mut options.access {
                Some(ControlAccessOptions::ReadOnly(options)) => {
                    for value in [&mut options.snapshot_token, &mut options.snapshot_time, &mut options.clock_time, &mut options.await_token] { visit(value)?; }
                }
                Some(ControlAccessOptions::ReadWrite(options)) => {
                    for value in [&mut options.system_time, &mut options.metadata, &mut options.asynchronous] { visit(value)?; }
                }
                None => (),
            }
        }
        ControlStatement::SetTimeZone(value) | ControlStatement::SetAwaitToken(value) | ControlStatement::SetParameter { value, .. } => value.visit(visitor)?,
        ControlStatement::Execute { arguments, .. } => arguments.visit(visitor)?,
        _ => (),
    }
    ControlFlow::Continue(())
}

impl RetainedControlSyntax {
    pub(super) fn span(&self) -> rtdb_plan::SourceSpan { self.span }

    pub(super) fn with_input<'input>(
        &self, input: &'input str, memory: &'input std::sync::Arc<rtdb_types::MemoryBudget>,
    ) -> ParsedControl<'input> {
        ParsedControl {
            source: StatementSource::bounded(input, self.span),
            forms: crate::bind::FormAllocation::Tracked(memory),
            syntax: self.syntax.clone(),
        }
    }
}

/// Parses the outer control command and retains its expression syntax for binding.
pub fn parse_control_syntax_in<'input>(source: &'input str, memory: &'input std::sync::Arc<rtdb_types::MemoryBudget>) -> Result<Option<ParsedControl<'input>>> {
    parse_control_syntax(source, crate::bind::FormAllocation::Tracked(memory))
}

fn parse_control_syntax<'input>(source: &'input str, forms: crate::bind::FormAllocation<'input>) -> Result<Option<ParsedControl<'input>>> {
    let mut tokens = crate::parse::tokenize(source, forms.memory())?;
    if !probe_control(source, &mut tokens)? { return Ok(None); }
    parse_control_tokens(StatementSource::full(source), &tokens, forms)
}

fn probe_control(source: &str, tokens: &mut Vec<TokenWithSpan>) -> Result<bool> {
    tokens.retain(|token| !matches!(token.token, Token::Whitespace(_)));
    if tokens.last().is_some_and(|token| token.token == Token::SemiColon) { tokens.pop(); }
    let Some(first) = tokens.first().filter(|first| is_control_head(first)) else { return Ok(false); };
    if tokens.iter().any(|token| token.token == Token::SemiColon) {
        return Err(sql_error("A control statement must contain exactly one statement", source, first.span));
    }
    Ok(true)
}

fn is_control_head(first: &TokenWithSpan) -> bool {
    ["BEGIN", "START", "SET", "COMMIT", "ROLLBACK", "DISCARD", "SHOW", "PREPARE", "EXECUTE", "ATTACH", "DETACH", "COPY"]
        .into_iter().any(|head| word(Some(first), head))
}

pub(super) fn prepare_name(input: &str, tokens: &[TokenWithSpan]) -> Result<Option<String>> {
    if !word(tokens.first(), "PREPARE") { return Ok(None); }
    let mut parser = ControlParser { source: input, tokens: tokens[..tokens.len().min(3)].to_vec(), index: 1 };
    let name = parser.statement_name()?;
    parser.expect("AS")?;
    if tokens.get(parser.index).is_none() { return Err(parser.error("PREPARE requires a statement")); }
    Ok(Some(name))
}

pub(super) fn parse_control_tokens<'input>(
    source: StatementSource<'input>, tokens: &[TokenWithSpan], forms: crate::bind::FormAllocation<'input>,
) -> Result<Option<ParsedControl<'input>>> {
    Ok(parse_control_tree(source, tokens)?.map(|syntax| ParsedControl { source, forms, syntax }))
}

pub(super) fn parse_control_tree(source: StatementSource<'_>, tokens: &[TokenWithSpan]) -> Result<Option<ControlStatement<ast::Expr>>> {
    let Some(first) = tokens.first() else { return Ok(None); };
    if !is_control_head(first) { return Ok(None); }
    if let Some(name) = prepare_name(source.input, tokens)? {
        let inner = StatementSource::tokens(source.input, &tokens[3], tokens.last().expect("PREPARE body was checked"));
        return Ok(Some(ControlStatement::Prepare { name, source: inner.text().into() }));
    }
    let mut next_parameter = 1usize;
    let mut tokens = tokens.to_vec();
    for token in &mut tokens {
        if let Token::Placeholder(parameter) = &mut token.token {
            if parameter == "?" { *parameter = format!("${next_parameter}"); next_parameter += 1; }
        }
    }
    let mut parser = ControlParser { source: source.input, tokens, index: 0 };
    let syntax = parser.statement()?;
    if parser.index != parser.tokens.len() { return Err(parser.error("Unexpected input after control statement")); }
    Ok(Some(syntax))
}

struct ControlParser<'a> { source: &'a str, tokens: Vec<TokenWithSpan>, index: usize }

impl ControlParser<'_> {
    fn current_span(&self) -> Span {
        self.tokens.get(self.index).or_else(|| self.tokens.last()).map_or_else(Span::empty, |token| token.span)
    }
    fn error(&self, message: impl Into<String>) -> rtdb_types::Anomaly {
        sql_error(message, self.source, self.current_span())
    }
    fn eat(&mut self, expected: &str) -> bool {
        if word(self.tokens.get(self.index), expected) { self.index += 1; true } else { false }
    }
    fn expect(&mut self, expected: &str) -> Result<()> { if self.eat(expected) { Ok(()) } else { Err(self.error(format!("Expected {expected}"))) } }
    fn token(&mut self, expected: Token) -> bool {
        if self.tokens.get(self.index).is_some_and(|token| token.token == expected) { self.index += 1; true } else { false }
    }
    fn identifier(&mut self) -> Result<Identifier> {
        let Some(TokenWithSpan { token: Token::Word(word), .. }) = self.tokens.get(self.index) else { return Err(self.error("Expected an identifier")); };
        let identifier = Identifier { text: word.value.clone(), quoted: word.quote_style.is_some() };
        self.index += 1;
        Ok(identifier)
    }
    /// `Sql.g4`'s `identifier`: an unquoted grammar keyword names something only when it is non-reserved.
    fn grammar_identifier(&mut self) -> Result<Identifier> {
        let token = self.index;
        let identifier = self.identifier()?;
        if !identifier.quoted && !crate::parser::is_unquoted_identifier(&identifier.text) {
            return Err(sql_error(format!("Expected an identifier, found the reserved word {}", identifier.text), self.source, self.tokens[token].span));
        }
        Ok(identifier)
    }
    fn statement_name(&mut self) -> Result<String> {
        let token = self.index;
        let identifier = self.identifier()?;
        if !identifier.quoted && !crate::parser::is_unquoted_identifier(&identifier.text) {
            return Err(sql_error("Expected a prepared statement name", self.source, self.tokens[token].span));
        }
        Ok(identifier.normalized())
    }
    fn string(&mut self) -> Result<String> {
        let Some(token) = self.tokens.get(self.index) else { return Err(self.error("Expected a string literal")); };
        let value = match &token.token {
            Token::SingleQuotedString(value) => value.clone(),
            Token::DollarQuotedString(value) => value.value.clone(),
            Token::EscapedStringLiteral(_) => {
                let span = source_span(self.source, token.span);
                let source = self.source.get(span.start..span.end).ok_or_else(|| self.error("Invalid string literal span"))?;
                source.get(2..source.len().saturating_sub(1)).ok_or_else(|| self.error("Invalid escaped string literal"))?.to_owned()
            }
            _ => return Err(self.error("Expected a string literal")),
        };
        self.index += 1; Ok(value)
    }
    fn expression(&mut self) -> Result<ast::Expr> {
        if self.eat("ON") { return Ok(ast::Expr::Value(ast::Value::Boolean(true).into())); }
        if self.eat("OFF") { return Ok(ast::Expr::Value(ast::Value::Boolean(false).into())); }
        let dialect = crate::dialect::SqlDialect::default();
        let mut parser = Parser::new(&dialect).with_recursion_limit(CONTROL_EXPRESSION_RECURSION_LIMIT)
            .with_tokens_with_locations(self.tokens[self.index..].to_vec());
        let expression = dialect.checked(parser.parse_expr())
            .map_err(|error| crate::parse::parser_error(error, self.source, self.current_span()))?;
        self.index += parser.index();
        Ok(expression)
    }
    fn option_value(&mut self, literal_only: bool) -> Result<ControlValue<ast::Expr>> {
        self.token(Token::Eq);
        if self.eat("DEFAULT") { return Ok(ControlValue::Default); }
        let value = if literal_only { self.literal("ASYNC requires a literal or DEFAULT")? } else { self.expression()? };
        Ok(ControlValue::Expression(value))
    }
    fn literal(&mut self, expected: &str) -> Result<ast::Expr> {
        let start = self.index;
        let value = self.expression()?;
        if is_literal(&value) || binary_literal_tokens(&self.tokens[start..self.index]) { Ok(value) }
        else { Err(self.error(expected)) }
    }
    fn isolation(&mut self) -> Result<()> {
        self.expect("LEVEL")?;
        if self.eat("SERIALIZABLE") { return Ok(()); }
        if self.eat("READ") { if self.eat("COMMITTED") || self.eat("UNCOMMITTED") { return Ok(()); } }
        else if self.eat("REPEATABLE") { self.expect("READ")?; return Ok(()); }
        Err(self.error("Invalid transaction isolation level"))
    }
    fn access_mode(&mut self) -> Result<AccessMode> {
        self.expect("READ")?;
        if self.eat("ONLY") { return Ok(AccessMode::ReadOnly); }
        self.expect("WRITE")?;
        Ok(AccessMode::ReadWrite)
    }
    fn transaction_options(&mut self, begin: bool) -> Result<TransactionOptions<ast::Expr>> {
        if self.index == self.tokens.len() {
            return if begin { Ok(TransactionOptions::default()) }
                else { Err(self.error("SET TRANSACTION requires transaction characteristics")) };
        }
        while self.eat("ISOLATION") {
            self.isolation()?;
            if !self.token(Token::Comma) { return Ok(TransactionOptions::default()); }
        }
        if begin && word(self.tokens.get(self.index), "READ") && word(self.tokens.get(self.index + 2), "WITH") {
            return self.begin_with_options();
        }
        let mut access = None;
        let mut values = BTreeMap::new();
        loop {
            if word(self.tokens.get(self.index), "READ") {
                if access.is_some() { return Err(self.error("Transaction access mode was specified more than once")); }
                access = Some(self.access_mode()?);
            } else if self.eat("ISOLATION") {
                self.isolation()?;
            } else {
                let (name, value) = self.transaction_option()?;
                values.insert(name, value);
            }
            if !self.token(Token::Comma) { break; }
        }
        self.finish_transaction_options(access, values)
    }
    fn begin_with_options(&mut self) -> Result<TransactionOptions<ast::Expr>> {
        let mut options = TransactionOptions::default();
        loop {
            if self.eat("ISOLATION") {
                self.isolation()?;
            } else {
                let mode = self.access_mode()?;
                self.expect("WITH")?;
                if !self.token(Token::LParen) { return Err(self.error("Expected opening WITH parenthesis")); }
                let mut values = BTreeMap::new();
                while !self.token(Token::RParen) {
                    if self.token(Token::Comma) { continue; }
                    let (name, value) = self.transaction_option()?;
                    values.insert(name, value);
                    if self.token(Token::RParen) { break; }
                    if !self.token(Token::Comma) { return Err(self.error("Expected comma between WITH options")); }
                }
                let group = self.finish_transaction_options(Some(mode), values)?;
                options.access = group.access;
                if group.timezone.is_some() { options.timezone = group.timezone; }
            }
            if !self.token(Token::Comma) { break; }
        }
        Ok(options)
    }
    fn transaction_option(&mut self) -> Result<(&'static str, ControlValue<ast::Expr>)> {
        let name = if self.eat("TIME") {
            self.expect("ZONE")?;
            "timezone"
        } else {
            let Some((_, name)) = [
                ("TIMEZONE", "timezone"), ("SNAPSHOT_TOKEN", "snapshot_token"), ("SNAPSHOT_TIME", "snapshot_time"),
                ("CLOCK_TIME", "clock_time"), ("AWAIT_TOKEN", "await_token"), ("SYSTEM_TIME", "system_time"),
                ("METADATA", "metadata"), ("ASYNC", "async"),
            ].into_iter().find(|(keyword, _)| self.eat(keyword)) else { return Err(self.error("Expected a transaction option")); };
            name
        };
        Ok((name, self.option_value(name == "async")?))
    }
    fn finish_transaction_options(
        &self, access: Option<AccessMode>, mut values: BTreeMap<&str, ControlValue<ast::Expr>>,
    ) -> Result<TransactionOptions<ast::Expr>> {
        let timezone = values.remove("timezone");
        let access = match access {
            Some(AccessMode::ReadOnly) => Some(ControlAccessOptions::ReadOnly(ReadOnlyOptions {
                snapshot_token: values.remove("snapshot_token"), snapshot_time: values.remove("snapshot_time"), clock_time: values.remove("clock_time"), await_token: values.remove("await_token"),
            })),
            Some(AccessMode::ReadWrite) => Some(ControlAccessOptions::ReadWrite(ReadWriteOptions {
                system_time: values.remove("system_time"), metadata: values.remove("metadata"), asynchronous: values.remove("async"),
            })),
            None => None,
        };
        if !values.is_empty() { return Err(self.error("Transaction option does not belong to the declared access mode")); }
        Ok(TransactionOptions { access, timezone })
    }
    fn statement(&mut self) -> Result<ControlStatement<ast::Expr>> {
        if self.eat("BEGIN") { self.eat("TRANSACTION"); return Ok(ControlStatement::Begin(self.transaction_options(true)?)); }
        if self.eat("START") { self.expect("TRANSACTION")?; return Ok(ControlStatement::Begin(self.transaction_options(true)?)); }
        if self.eat("COMMIT") {
            return Ok(ControlStatement::Commit(if self.eat("SYNC") { Some(ControlCommitMode::Sync) } else if self.eat("ASYNC") { Some(ControlCommitMode::Async) } else { None }));
        }
        if self.eat("ROLLBACK") { return Ok(ControlStatement::Rollback); }
        if self.eat("SET") {
            if self.eat("TRANSACTION") { return Ok(ControlStatement::SetTransaction(self.transaction_options(false)?)); }
            if !self.set_separator_at(self.index + 1) && self.eat("ROLE") {
                if !self.eat("NONE") { self.grammar_identifier()?; }
                return Ok(ControlStatement::SetRole);
            }
            let session = self.eat("SESSION");
            if session && self.eat("CHARACTERISTICS") {
                self.expect("AS")?; self.expect("TRANSACTION")?;
                let mut access = None;
                loop {
                    if self.eat("ISOLATION") { self.isolation()?; }
                    else { self.expect("READ")?; access = Some(if self.eat("ONLY") { AccessMode::ReadOnly } else { self.expect("WRITE")?; AccessMode::ReadWrite }); }
                    if !self.token(Token::Comma) { break; }
                    self.eat("TRANSACTION");
                }
                return Ok(ControlStatement::SetSessionCharacteristics(access));
            }
            if self.eat("TIME") {
                self.expect("ZONE")?;
                return Ok(ControlStatement::SetTimeZone(self.expression()?));
            }
            if !self.set_separator_at(self.index + 1) && self.eat("TIMEZONE") {
                return Ok(ControlStatement::SetTimeZone(self.expression()?));
            }
            if !session && self.eat("AWAIT_TOKEN") {
                self.set_separator()?;
                return Ok(ControlStatement::SetAwaitToken(self.expression()?));
            }
            let mut name = self.grammar_identifier()?.text.to_lowercase();
            if name == "bijection" && self.token(Token::Period) {
                self.expect("AT")?;
                name = "bijection.at".into();
            }
            self.set_separator()?;
            let value = self.literal("Session parameter requires a literal")?;
            return Ok(ControlStatement::SetParameter { name, value });
        }
        if self.eat("DISCARD") {
            return Ok(ControlStatement::Discard(if self.eat("ALL") { DiscardTarget::All }
                else if self.eat("PLANS") { DiscardTarget::Plans }
                else if self.eat("TEMP") || self.eat("TEMPORARY") { DiscardTarget::Temporary }
                else { self.expect("SEQUENCES")?; DiscardTarget::Sequences }));
        }
        if self.eat("SHOW") {
            if self.eat("TRANSACTION") {
                self.expect("ISOLATION")?; self.expect("LEVEL")?;
                return Ok(ControlStatement::ShowTransactionIsolationLevel);
            }
            if let Some(keyword) = ["AWAIT_TOKEN", "SNAPSHOT_TOKEN", "CLOCK_TIME"].into_iter().find(|keyword| self.eat(keyword)) {
                return Ok(ControlStatement::Show { name: keyword.to_ascii_lowercase() });
            }
            if self.eat("TIME") {
                self.expect("ZONE")?;
                return Ok(ControlStatement::Show { name: "timezone".into() });
            }
            let identifier = self.grammar_identifier()?;
            let name = identifier.text.to_lowercase();
            if identifier.quoted && matches!(name.as_str(), "clock_time" | "snapshot_token" | "timezone") {
                return Ok(ControlStatement::ShowSessionParameter { name });
            }
            return Ok(ControlStatement::Show { name });
        }
        if self.eat("EXECUTE") {
            let name = self.statement_name()?; let mut arguments = Vec::new();
            if self.token(Token::LParen) {
                if self.token(Token::RParen) { return Err(self.error("EXECUTE parentheses require an argument")); }
                loop { arguments.push(self.expression()?); if self.token(Token::RParen) { break; } if !self.token(Token::Comma) { return Err(self.error("Expected comma between EXECUTE arguments")); } }
            }
            return Ok(ControlStatement::Execute { name, arguments });
        }
        if self.eat("ATTACH") {
            self.expect("DATABASE")?; let name = self.grammar_identifier()?.normalized();
            let configuration_yaml = if self.eat("WITH") { Some(self.string()?) } else { None };
            return Ok(ControlStatement::AttachDatabase { name, configuration_yaml, source: self.source.trim().to_owned() });
        }
        if self.eat("DETACH") { self.expect("DATABASE")?; return Ok(ControlStatement::DetachDatabase { name: self.grammar_identifier()?.normalized() }); }
        self.expect("COPY")?;
        if self.token(Token::LParen) {
            let start = self.index;
            let mut depth = 1;
            loop {
                let Some(token) = self.tokens.get(self.index) else { return Err(self.error("Unclosed COPY query")); };
                match token.token {
                    Token::LParen => depth += 1,
                    Token::RParen => { depth -= 1; if depth == 0 { break; } },
                    _ => (),
                }
                self.index += 1;
            }
            let end = self.index;
            self.index += 1;
            if start == end { return Err(self.error("COPY requires a query")); }
            self.expect("TO")?; self.expect("STDOUT")?; self.expect("WITH")?;
            if !self.token(Token::LParen) { return Err(self.error("COPY requires WITH parentheses")); }
            self.expect("FORMAT")?; self.token(Token::Eq);
            if self.string()? != "arrow-stream" { return Err(self.error("COPY TO requires the arrow-stream format")); }
            if !self.token(Token::RParen) { return Err(self.error("Expected closing COPY options parenthesis")); }
            let source = StatementSource::tokens(self.source, &self.tokens[start], &self.tokens[end - 1]);
            crate::parse::parse_statement_syntax(source, source.span(), &self.tokens[start..end])?;
            let open = source_span(self.source, self.tokens[start - 1].span).end;
            let offer = self.source[open..source.span().start].trim();
            let format = if offer == "/* rtdb-history-label-dictionary-v1 */" {
                CopyOutFormat::HistoryLabelDictionaryV1
            } else {
                CopyOutFormat::ArrowStream
            };
            return Ok(ControlStatement::CopyOut { query: source.text().into(), format });
        }
        let mut table = vec![self.grammar_identifier()?];
        if self.token(Token::Period) { table.push(self.grammar_identifier()?); }
        self.expect("FROM")?; self.expect("STDIN")?; self.expect("WITH")?;
        if !self.token(Token::LParen) { return Err(self.error("COPY requires WITH parentheses")); }
        let mut format = None;
        while !self.token(Token::RParen) {
            if self.token(Token::Comma) { continue; }
            self.expect("FORMAT")?; self.token(Token::Eq);
            if format.is_none() {
                format = Some(match self.string()?.as_str() {
                    "transit-json" => Some(CopyFormat::TransitJson), "transit-msgpack" => Some(CopyFormat::TransitMsgpack),
                    "arrow-file" => Some(CopyFormat::ArrowFile), "arrow-stream" => Some(CopyFormat::ArrowStream),
                    _ => None,
                });
            } else {
                if !self.tokens.get(self.index).is_some_and(|token| matches!(&token.token, Token::SingleQuotedString(_) | Token::DollarQuotedString(_) | Token::EscapedStringLiteral(_))) { return Err(self.error("Expected a string literal")); }
                self.index += 1;
            }
            if self.token(Token::RParen) { break; }
            if !self.token(Token::Comma) { return Err(self.error("Expected comma between COPY options")); }
        }
        Ok(ControlStatement::CopyIn { table, format: format.flatten() })
    }

    fn set_separator_at(&self, index: usize) -> bool {
        let token = self.tokens.get(index);
        token.is_some_and(|token| token.token == Token::Eq) || word(token, "TO")
    }

    fn set_separator(&mut self) -> Result<()> {
        if self.token(Token::Eq) || self.eat("TO") { Ok(()) } else { Err(self.error("SET requires TO or =")) }
    }
}

fn word(token: Option<&TokenWithSpan>, expected: &str) -> bool {
    token.is_some_and(|token| matches!(&token.token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected)))
}

fn character_string(value: &ast::Value) -> bool {
    matches!(value, ast::Value::SingleQuotedString(_) | ast::Value::DollarQuotedString(_) | ast::Value::EscapedStringLiteral(_))
}

fn binary_literal_tokens(tokens: &[TokenWithSpan]) -> bool {
    let [name, open, body, close] = tokens else { return false; };
    word(Some(name), "X") && open.token == Token::LParen && close.token == Token::RParen
        && matches!(&body.token, Token::SingleQuotedString(body) if body.bytes().all(|byte| byte.is_ascii_hexdigit()))
        && tokens.windows(2).all(|pair| pair[0].span.end == pair[1].span.start)
}

fn typed_literal(value: &ast::TypedString) -> bool {
    use ast::{DataType as D, TimezoneInfo as Z};
    if value.uses_odbc_syntax || !character_string(&value.value.value) {
        return false;
    }
    match &value.data_type {
        D::Date | D::Uuid | D::Time(None, Z::None)
        | D::Timestamp(None, Z::None | Z::WithTimeZone | Z::WithoutTimeZone) => true,
        other => crate::dialect::CustomStringLiteral::from_type(other).is_some(),
    }
}

fn is_literal(expression: &ast::Expr) -> bool {
    let signed_number = |expression: &ast::Expr| matches!(expression, ast::Expr::Value(value) if matches!(value.value, ast::Value::Number(..)));
    match expression {
        ast::Expr::Value(value) => character_string(&value.value)
            || matches!(value.value, ast::Value::Null | ast::Value::Boolean(_) | ast::Value::Number(..)),
        ast::Expr::UnaryOp { op: ast::UnaryOperator::Plus | ast::UnaryOperator::Minus, expr } => signed_number(expr),
        ast::Expr::TypedString(value) => typed_literal(value),
        ast::Expr::Interval(interval) => {
            let value = match interval.value.as_ref() {
                ast::Expr::UnaryOp { op: ast::UnaryOperator::Plus | ast::UnaryOperator::Minus, expr } => expr.as_ref(),
                value => value,
            };
            matches!(value, ast::Expr::Value(value) if character_string(&value.value))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admitted_control_preserves_legacy_values_and_diagnostics() {
        let options = PrepareOptions::default();
        let legacy_memory = MemoryBudget::new(1 << 20);
        let memory = MemoryBudget::new(1 << 20);
        for source in [
            "/* outer /* nested */ comment */ sHoW TIME -- ;\n ZONE; -- end",
            "SET application_name = E'café\\nexample'",
            "ATTACH DATABASE example WITH $body$clé: valeur$body$",
            "PREPARE p AS SELECT /* retained café */ 1;",
            "SET TIME ZONE POSITION(POSITION('a' IN ('ba'))::VARCHAR IN '1')::VARCHAR",
            "BEGIN READ ONLY WITH (CLOCK_TIME TIMESTAMP 'bad', CLOCK_TIME TIMESTAMP '2020-01-01T00:00:00Z')",
            "EXECUTE p(?, $3, ?)",
        ] {
            let legacy = parse_control_in(source, &options, &legacy_memory).unwrap().unwrap();
            let admitted = parse_admitted_control_in(source, &options, &memory).unwrap().unwrap();
            assert_eq!(admitted.statement(), &legacy, "{source}");
        }
        for source in ["", "-- nothing\n;", "\"SHOW\" TIME ZONE;", "SELECT ';' /* ; */; SELECT 2"] {
            assert!(parse_control_in(source, &options, &legacy_memory).unwrap().is_none(), "{source}");
            assert!(parse_admitted_control_in(source, &options, &memory).unwrap().is_none(), "{source}");
        }
        for source in [
            "SHOW TIME ZONE;;",
            "SHOW TIME ZONE; SELECT 1",
            "SHOW TIME ZONE 'unterminated",
            "SELECT 'unterminated",
            "/* café */\n SELECT $$unterminated",
            "SET TIME ZONE unknown() TRAILING",
            "BEGIN READ ONLY WITH (CLOCK_TIME TIMESTAMP 'bad')",
        ] {
            let legacy = parse_control_in(source, &options, &legacy_memory).unwrap_err();
            let admitted = parse_admitted_control_in(source, &options, &memory).unwrap_err();
            assert_eq!(admitted, legacy, "{source}");
        }
        assert_eq!(legacy_memory.used_bytes(), 0);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn classification_releases_lexical_charges_and_retains_the_bound_command() {
        let options = PrepareOptions::default();
        let zero = MemoryBudget::new(0);
        for source in ["SELECT 1", "SHOW abc"] {
            let error = parse_admitted_control_in(source, &options, &zero).unwrap_err();
            assert_eq!(error.code, "rtdb/memory-budget-exceeded");
            assert_eq!(zero.used_bytes(), 0);
        }

        let enough = MemoryBudget::new(1 << 20);
        assert!(parse_admitted_control_in("SELECT 1", &options, &enough).unwrap().is_none());
        let lexical_peak = enough.peak_bytes();
        assert!(lexical_peak > 0);
        assert_eq!(enough.used_bytes(), 0);

        let lexical_only = MemoryBudget::new(lexical_peak);
        let error = parse_admitted_control_in("SHOW abc", &options, &lexical_only).unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(lexical_only.peak_bytes(), lexical_peak);
        assert_eq!(lexical_only.used_bytes(), 0);

        let control = parse_admitted_control_in("SHOW abc", &options, &enough).unwrap().unwrap();
        assert_eq!(control.statement(), &ControlStatement::Show { name: "abc".into() });
        assert!(enough.used_bytes() > 0);
        assert!(enough.peak_bytes() > enough.used_bytes());
        drop(control);
        assert_eq!(enough.used_bytes(), 0);
    }
}
