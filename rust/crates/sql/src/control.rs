use std::collections::BTreeMap;

use rtdb_plan::{AccessMode, ControlAccessOptions, ControlCommitMode, ControlStatement, ControlValue, CopyFormat, DiscardTarget, Expr, Identifier, ReadOnlyOptions, ReadWriteOptions, TransactionOptions};
use rtdb_types::{Result, Value};
use sqlparser::{dialect::GenericDialect, parser::Parser, tokenizer::{Span, Token, TokenWithSpan, Tokenizer}};

use crate::{PrepareOptions, parse::{source_span, sql_error}};

pub fn parse_control(source: &str, options: &PrepareOptions) -> Result<Option<ControlStatement>> {
    let dialect = GenericDialect {};
    let mut tokens: Vec<_> = Tokenizer::new(&dialect, source).tokenize_with_location()
        .map_err(|error| sql_error(error.to_string(), source, Span::new(error.location, error.location)))?
        .into_iter().filter(|token| !matches!(token.token, Token::Whitespace(_))).collect();
    if tokens.last().is_some_and(|token| token.token == Token::SemiColon) { tokens.pop(); }
    let Some(first) = tokens.first() else { return Ok(None); };
    if !["BEGIN", "START", "SET", "COMMIT", "ROLLBACK", "DISCARD", "SHOW", "PREPARE", "EXECUTE", "ATTACH", "DETACH", "COPY"].iter().any(|head| word(Some(first), head)) { return Ok(None); }
    if tokens.iter().any(|token| token.token == Token::SemiColon) { return Err(sql_error("A control statement must contain exactly one statement", source, first.span)); }
    let mut next_parameter = 1usize;
    for token in &mut tokens {
        if let Token::Placeholder(parameter) = &mut token.token {
            if parameter == "?" { *parameter = format!("${next_parameter}"); next_parameter += 1; }
            if parameter.strip_prefix('$').and_then(|index| index.parse::<usize>().ok()).is_none_or(|index| index == 0) {
                return Err(sql_error("SQL parameters use ? or a positive $n index", source, token.span));
            }
        }
    }
    let mut parser = ControlParser { source, options, tokens, index: 0 };
    let statement = parser.statement()?;
    if parser.index != parser.tokens.len() { return Err(parser.error("Unexpected input after control statement")); }
    Ok(Some(statement))
}

struct ControlParser<'a> { source: &'a str, options: &'a PrepareOptions, tokens: Vec<TokenWithSpan>, index: usize }

impl ControlParser<'_> {
    fn error(&self, message: impl Into<String>) -> rtdb_types::Anomaly {
        sql_error(message, self.source, self.tokens.get(self.index).or_else(|| self.tokens.last()).map_or_else(Span::empty, |token| token.span))
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
    fn string(&mut self) -> Result<String> {
        let Some(TokenWithSpan { token: Token::SingleQuotedString(value), .. }) = self.tokens.get(self.index) else { return Err(self.error("Expected a string literal")); };
        let value = value.clone(); self.index += 1; Ok(value)
    }
    fn expression(&mut self) -> Result<Expr> {
        if self.eat("ON") { return Ok(Expr::Literal(Value::Bool(true))); }
        if self.eat("OFF") { return Ok(Expr::Literal(Value::Bool(false))); }
        let dialect = GenericDialect {};
        let mut parser = Parser::new(&dialect).with_tokens_with_locations(self.tokens[self.index..].to_vec());
        let expression = parser.parse_expr().map_err(|error| self.error(error.to_string()))?;
        self.index += parser.index();
        crate::bind::control_expression(self.source, &expression, self.options)
    }
    fn option_value(&mut self) -> Result<ControlValue> {
        self.token(Token::Eq);
        if self.eat("DEFAULT") { Ok(ControlValue::Default) } else { self.expression().map(ControlValue::Expression) }
    }
    fn isolation(&mut self) -> Result<()> {
        self.expect("LEVEL")?;
        if self.eat("SERIALIZABLE") { return Ok(()); }
        if self.eat("READ") { if self.eat("COMMITTED") || self.eat("UNCOMMITTED") { return Ok(()); } }
        else if self.eat("REPEATABLE") { self.expect("READ")?; return Ok(()); }
        Err(self.error("Invalid transaction isolation level"))
    }
    fn transaction_options(&mut self, begin: bool) -> Result<TransactionOptions> {
        let mut access = None;
        let mut values = BTreeMap::new();
        let mut in_with = false;
        let mut saw_mode = false;
        while self.index < self.tokens.len() {
            if in_with && self.token(Token::RParen) { in_with = false; if !self.token(Token::Comma) { break; } continue; }
            if in_with && self.token(Token::Comma) { continue; }
            if self.eat("READ") {
                if in_with { return Err(self.error("READ mode cannot appear inside WITH options")); }
                let mode = if self.eat("ONLY") { AccessMode::ReadOnly } else { self.expect("WRITE")?; AccessMode::ReadWrite };
                if access.is_some() { return Err(self.error("Transaction access mode was specified more than once")); }
                access = Some(mode); saw_mode = true;
                if self.eat("WITH") {
                    if !begin || !self.token(Token::LParen) { return Err(self.error("WITH options require BEGIN and parentheses")); }
                    in_with = true; continue;
                }
            } else if self.eat("ISOLATION") {
                if in_with { return Err(self.error("Isolation level cannot appear inside WITH options")); }
                self.isolation()?; saw_mode = true;
            } else {
                let mut name = self.identifier()?.normalized();
                if name == "time" { self.expect("ZONE")?; name = "timezone".into(); }
                if !["timezone", "snapshot_token", "snapshot_time", "clock_time", "await_token", "system_time", "metadata", "async"].contains(&name.as_str()) {
                    return Err(self.error(format!("Unknown transaction option: {name}")));
                }
                let value = self.option_value()?;
                if name == "async" && !matches!(&value, ControlValue::Default | ControlValue::Expression(Expr::Literal(_))) { return Err(self.error("ASYNC requires a literal or DEFAULT")); }
                values.insert(name, value); saw_mode = true;
            }
            if !self.token(Token::Comma) {
                if in_with {
                    if !self.token(Token::RParen) { return Err(self.error("Expected closing WITH parenthesis")); }
                    in_with = false;
                    if self.token(Token::Comma) { continue; }
                }
                break;
            }
        }
        if in_with { return Err(self.error("Unclosed WITH options")); }
        if !begin && !saw_mode { return Err(self.error("SET TRANSACTION requires transaction characteristics")); }
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
    fn statement(&mut self) -> Result<ControlStatement> {
        if self.eat("BEGIN") { self.eat("TRANSACTION"); return Ok(ControlStatement::Begin(self.transaction_options(true)?)); }
        if self.eat("START") { self.expect("TRANSACTION")?; return Ok(ControlStatement::Begin(self.transaction_options(true)?)); }
        if self.eat("COMMIT") {
            return Ok(ControlStatement::Commit(if self.eat("SYNC") { Some(ControlCommitMode::Sync) } else if self.eat("ASYNC") { Some(ControlCommitMode::Async) } else { None }));
        }
        if self.eat("ROLLBACK") { return Ok(ControlStatement::Rollback); }
        if self.eat("SET") {
            if self.eat("TRANSACTION") { return Ok(ControlStatement::SetTransaction(self.transaction_options(false)?)); }
            if self.eat("ROLE") { self.identifier()?; return Ok(ControlStatement::SetRole); }
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
            let mut name = self.identifier()?.normalized();
            if name == "time" { self.expect("ZONE")?; name = "timezone".into(); }
            let separator = self.token(Token::Eq) || self.eat("TO");
            if name != "timezone" && !separator { return Err(self.error("SET requires TO or =")); }
            let value = self.expression()?;
            return Ok(match name.as_str() {
                "timezone" => ControlStatement::SetTimeZone(value),
                "await_token" if !session => ControlStatement::SetAwaitToken(value),
                _ => {
                    if !literal_expression(&value) { return Err(self.error("Session parameter requires a literal")); }
                    ControlStatement::SetParameter { name, value }
                }
            });
        }
        if self.eat("DISCARD") {
            return Ok(ControlStatement::Discard(if self.eat("ALL") { DiscardTarget::All }
                else if self.eat("PLANS") { DiscardTarget::Plans }
                else if self.eat("TEMP") || self.eat("TEMPORARY") { DiscardTarget::Temporary }
                else { self.expect("SEQUENCES")?; DiscardTarget::Sequences }));
        }
        if self.eat("SHOW") {
            let mut name = self.identifier()?.normalized();
            if name == "time" { self.expect("ZONE")?; name = "timezone".into(); }
            if name == "transaction" { self.expect("ISOLATION")?; self.expect("LEVEL")?; name = "transaction_isolation".into(); }
            return Ok(ControlStatement::Show { name });
        }
        if self.eat("PREPARE") {
            let name = self.identifier()?.normalized(); self.expect("AS")?;
            let first = self.tokens.get(self.index).ok_or_else(|| self.error("PREPARE requires a statement"))?;
            let last = self.tokens.last().ok_or_else(|| self.error("PREPARE requires a statement"))?;
            let span = source_span(self.source, Span::new(first.span.start, last.span.end));
            let source = self.source[span.start..span.end].to_owned(); self.index = self.tokens.len();
            return Ok(ControlStatement::Prepare { name, source });
        }
        if self.eat("EXECUTE") {
            let name = self.identifier()?.normalized(); let mut arguments = Vec::new();
            if self.token(Token::LParen) {
                if self.token(Token::RParen) { return Err(self.error("EXECUTE parentheses require an argument")); }
                loop { arguments.push(self.expression()?); if self.token(Token::RParen) { break; } if !self.token(Token::Comma) { return Err(self.error("Expected comma between EXECUTE arguments")); } }
            }
            return Ok(ControlStatement::Execute { name, arguments });
        }
        if self.eat("ATTACH") {
            self.expect("DATABASE")?; let name = self.identifier()?.normalized();
            let configuration_yaml = if self.eat("WITH") { Some(self.string()?) } else { None };
            return Ok(ControlStatement::AttachDatabase { name, configuration_yaml });
        }
        if self.eat("DETACH") { self.expect("DATABASE")?; return Ok(ControlStatement::DetachDatabase { name: self.identifier()?.normalized() }); }
        self.expect("COPY")?;
        let mut table = vec![self.identifier()?];
        if self.token(Token::Period) { table.push(self.identifier()?); }
        self.expect("FROM")?; self.expect("STDIN")?; self.expect("WITH")?;
        if !self.token(Token::LParen) { return Err(self.error("COPY requires WITH parentheses")); }
        let mut format = None;
        while !self.token(Token::RParen) {
            if self.token(Token::Comma) { continue; }
            self.expect("FORMAT")?; self.token(Token::Eq);
            format = Some(match self.string()?.to_ascii_lowercase().as_str() {
                "transit-json" => CopyFormat::TransitJson, "transit-msgpack" => CopyFormat::TransitMsgpack,
                "arrow-file" => CopyFormat::ArrowFile, "arrow-stream" => CopyFormat::ArrowStream,
                other => return Err(self.error(format!("Unsupported COPY format: {other}"))),
            });
            if self.token(Token::RParen) { break; }
            if !self.token(Token::Comma) { return Err(self.error("Expected comma between COPY options")); }
        }
        Ok(ControlStatement::CopyIn { table, format })
    }
}

fn word(token: Option<&TokenWithSpan>, expected: &str) -> bool {
    token.is_some_and(|token| matches!(&token.token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected)))
}

fn literal_expression(expression: &Expr) -> bool {
    match expression {
        Expr::Literal(_) => true,
        Expr::Cast { expr, .. } | Expr::Unary { expr, .. } => literal_expression(expr),
        _ => false,
    }
}
