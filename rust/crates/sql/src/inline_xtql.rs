use rtdb_types::{Anomaly, Result};
use rtdb_xtql::Query;
use sqlparser::{ast, dialect::GenericDialect, tokenizer::{Span, Token, TokenWithSpan, Tokenizer}};
use crate::parse::{source_span, sql_error};

#[derive(Clone, Debug)]
pub(crate) struct InlineXtql {
    pub name: String,
    pub span: Span,
    pub query: Query,
    pub parameters: Vec<usize>,
}

impl InlineXtql {
    pub fn matches(&self, name: &ast::ObjectName) -> bool {
        name.0.len() == 1 && name.0[0].as_ident().is_some_and(|name| name.value == self.name && name.span == self.span)
    }
    pub fn error(&self, error: Anomaly, source: &str) -> Anomaly { located(error, source, self.span) }
}

pub(crate) fn extract(tokens: &[TokenWithSpan], source: &str) -> Result<(Vec<TokenWithSpan>, Vec<InlineXtql>, usize)> {
    let mut output = Vec::new();
    let mut inline = Vec::new();
    let mut position = 0;
    let mut next_parameter = 0usize;
    let mut parameter_count = 0usize;
    while position < tokens.len() {
        let token = &tokens[position];
        let keyword = matches!(&token.token,Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("XTQL"));
        let explicit = tokens.get(position+1).is_some_and(|token| token.token == Token::LParen);
        let literal_position = position+if explicit { 2 } else { 1 };
        let literal = tokens.get(literal_position).and_then(|token| match &token.token {
            Token::SingleQuotedString(value) | Token::EscapedStringLiteral(value) => Some(value.as_str()),
            Token::DollarQuotedString(value) => Some(value.value.as_str()),
            _ => None,
        });
        if !keyword || literal.is_none() {
            let mut token = token.clone();
            if matches!(token.token,Token::Placeholder(_)) {
                let index = claim(&mut token,&mut next_parameter,source)?;
                parameter_count = parameter_count.max(index+1);
            }
            output.push(token);
            position += 1;
            continue;
        }
        let literal = literal.ok_or_else(|| sql_error("XTQL requires a query string",source,token.span))?;
        let query = rtdb_xtql::read(literal).and_then(|form| rtdb_xtql::parse_query(&form)).map_err(|error| located(error,source,tokens[literal_position].span))?;
        let arity = match &query { Query::Function { parameters,.. } => Some(parameters.len()), _ => None };
        let mut end = literal_position+1;
        let mut parameters = Vec::new();
        if explicit {
            while tokens.get(end).is_some_and(|token| token.token == Token::Comma) {
                let mut parameter = tokens.get(end+1).cloned().ok_or_else(|| sql_error("XTQL argument must be a parameter",source,token.span))?;
                parameters.push(claim(&mut parameter,&mut next_parameter,source)?);
                end += 2;
            }
            if tokens.get(end).is_none_or(|token| token.token != Token::RParen) { return Err(sql_error("Expected closing parenthesis after XTQL parameters",source,token.span)); }
            end += 1;
        } else if let Some(arity) = arity {
            let next = next_parameter.checked_add(arity).ok_or_else(|| sql_error("SQL parameter count overflow",source,token.span))?;
            parameters.extend(next_parameter..next);
            next_parameter = next;
        }
        let span = Span::new(token.span.start,tokens[end-1].span.end);
        if let Some(arity) = arity {
            if arity != parameters.len() { return Err(located(Anomaly::incorrect("xtql/fn-arity-error",format!("XTQL fn expects {arity} argument(s), got {}",parameters.len())),source,span)); }
            if let Some(index) = parameters.iter().max() { parameter_count = parameter_count.max(index+1); }
        } else { parameters.clear(); }
        let name = format!("_rtdb_inline_xtql_{}",inline.len());
        let injected = Tokenizer::new(&GenericDialect {}, &format!("SELECT * FROM \"{name}\"")).tokenize().map_err(|error| sql_error(error.to_string(),source,span))?;
        output.extend(injected.into_iter().filter(|token| !matches!(token,Token::Whitespace(_))).map(|token| TokenWithSpan { token,span }));
        inline.push(InlineXtql { name,span,query,parameters });
        position = end;
    }
    Ok((output,inline,parameter_count))
}

fn claim(token: &mut TokenWithSpan, next: &mut usize, source: &str) -> Result<usize> {
    let Token::Placeholder(value) = &mut token.token else { return Err(sql_error("XTQL arguments must be SQL parameters",source,token.span)); };
    if value == "?" {
        let index = *next;
        *next = next.checked_add(1).ok_or_else(|| sql_error("SQL parameter count overflow",source,token.span))?;
        *value = format!("${}",*next);
        Ok(index)
    } else {
        value.strip_prefix('$').and_then(|value| value.parse::<usize>().ok()).and_then(|index| index.checked_sub(1)).filter(|index| *index < usize::MAX)
            .ok_or_else(|| sql_error("SQL parameters use ? or a positive $n index",source,token.span))
    }
}

fn located(error: Anomaly, source: &str, span: Span) -> Anomaly {
    let span = source_span(source,span);
    error.with_context("span",serde_json::json!({"start":span.start,"end":span.end})).with_context("position",serde_json::json!(span.start+1))
}
