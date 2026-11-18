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

#[derive(Clone, Debug)]
enum ParameterSyntax { Dynamic, Numbered(String) }

#[derive(Clone, Debug)]
struct InlineSyntax {
    name: String,
    span: Span,
    literal_span: Span,
    literal: String,
    arguments: Option<Vec<(ParameterSyntax, Span)>>,
}

#[derive(Clone, Debug)]
enum SourceArgument {
    Sql(ParameterSyntax, Span),
    Inline(InlineSyntax),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SourceArguments { ordered: Vec<SourceArgument> }

#[derive(Clone, Debug, Default)]
pub(crate) struct ParameterAssignments {
    pub(crate) count: usize,
    pub(crate) dynamic: Vec<usize>,
    pub(crate) inline: Vec<InlineXtql>,
}

impl SourceArguments {
    pub(crate) fn assign(self, source: &str, forms: crate::bind::FormAllocation<'_>) -> Result<ParameterAssignments> {
        let mut result = ParameterAssignments::default();
        let mut next = 0usize;
        for argument in self.ordered {
            match argument {
                SourceArgument::Sql(parameter, span) => {
                    let index = parameter.claim(&mut next, source, span)?;
                    result.count = result.count.max(index + 1);
                    if matches!(parameter, ParameterSyntax::Dynamic) { result.dynamic.push(index); }
                }
                SourceArgument::Inline(syntax) => {
                    let query = rtdb_xtql::read(&syntax.literal).and_then(|form| forms.query(&form))
                        .map_err(|error| located(error, source, syntax.literal_span))?;
                    let arity = match &query { Query::Function { parameters, .. } => Some(parameters.len()), _ => None };
                    let mut parameters: Vec<_> = match syntax.arguments {
                        Some(arguments) => arguments.into_iter()
                            .map(|(parameter, span)| parameter.claim(&mut next, source, span)).collect::<Result<_>>()?,
                        None => {
                            let end = next.checked_add(arity.unwrap_or(0))
                                .ok_or_else(|| sql_error("SQL parameter count overflow", source, syntax.span))?;
                            let parameters = (next..end).collect();
                            next = end;
                            parameters
                        }
                    };
                    if let Some(arity) = arity {
                        if arity != parameters.len() {
                            return Err(located(Anomaly::incorrect("xtql/fn-arity-error",
                                format!("XTQL fn expects {arity} argument(s), got {}", parameters.len())), source, syntax.span));
                        }
                        if let Some(index) = parameters.iter().max() { result.count = result.count.max(index + 1); }
                    } else {
                        parameters.clear();
                    }
                    result.inline.push(InlineXtql { name: syntax.name, span: syntax.span, query, parameters });
                }
            }
        }
        Ok(result)
    }
}

impl ParameterSyntax {
    fn read(token: &TokenWithSpan, source: &str) -> Result<Self> {
        let Token::Placeholder(value) = &token.token else { return Err(sql_error("XTQL arguments must be SQL parameters", source, token.span)); };
        if value == "?" { return Ok(Self::Dynamic); }
        value.strip_prefix('$').filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
            .map(|digits| Self::Numbered(digits.to_owned())).ok_or_else(|| sql_error("Expected ? or a numbered SQL parameter", source, token.span))
    }

    fn claim(&self, next: &mut usize, source: &str, span: Span) -> Result<usize> {
        match self {
            Self::Dynamic => {
                let index = *next;
                *next = next.checked_add(1).ok_or_else(|| sql_error("SQL parameter count overflow", source, span))?;
                Ok(index)
            }
            Self::Numbered(digits) => digits.parse::<usize>().ok().and_then(|index| index.checked_sub(1))
                .ok_or_else(|| sql_error("SQL parameters use ? or a positive $n index", source, span)),
        }
    }
}

pub(crate) fn extract(tokens: &[TokenWithSpan], source: &str) -> Result<(Vec<TokenWithSpan>, SourceArguments)> {
    let mut output = Vec::new();
    let mut parameters = SourceArguments::default();
    let mut position = 0;
    let mut inline_count = 0usize;
    let mut dynamic_count = 0usize;
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
                let parameter = ParameterSyntax::read(&token, source)?;
                if matches!(parameter, ParameterSyntax::Dynamic) {
                    // AST equality ignores spans, so anonymous occurrences need distinct placeholder text before their slots are assigned.
                    token.token = Token::Placeholder(format!("?{dynamic_count}"));
                    dynamic_count += 1;
                }
                parameters.ordered.push(SourceArgument::Sql(parameter, token.span));
            }
            output.push(token);
            position += 1;
            continue;
        }
        let literal = literal.ok_or_else(|| sql_error("XTQL requires a query string",source,token.span))?;
        let mut end = literal_position+1;
        let arguments = if explicit {
            let mut arguments = Vec::new();
            while tokens.get(end).is_some_and(|token| token.token == Token::Comma) {
                let parameter = tokens.get(end+1).ok_or_else(|| sql_error("XTQL argument must be a parameter",source,token.span))?;
                arguments.push((ParameterSyntax::read(parameter, source)?, parameter.span));
                end += 2;
            }
            if tokens.get(end).is_none_or(|token| token.token != Token::RParen) { return Err(sql_error("Expected closing parenthesis after XTQL parameters",source,token.span)); }
            end += 1;
            Some(arguments)
        } else { None };
        let span = Span::new(token.span.start,tokens[end-1].span.end);
        let name = format!("_rtdb_inline_xtql_{inline_count}");
        inline_count += 1;
        let injected = Tokenizer::new(&GenericDialect {}, &format!("SELECT * FROM \"{name}\"")).tokenize().map_err(|error| sql_error(error.to_string(),source,span))?;
        output.extend(injected.into_iter().filter(|token| !matches!(token,Token::Whitespace(_))).map(|token| TokenWithSpan { token,span }));
        parameters.ordered.push(SourceArgument::Inline(InlineSyntax {
            name, span, literal_span: tokens[literal_position].span, literal: literal.to_owned(), arguments,
        }));
        position = end;
    }
    Ok((output, parameters))
}

fn located(error: Anomaly, source: &str, span: Span) -> Anomaly {
    let span = source_span(source,span);
    error.with_context("span",serde_json::json!({"start":span.start,"end":span.end})).with_context("position",serde_json::json!(span.start+1))
}
