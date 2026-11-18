use rtdb_types::Result;
use sqlparser::{ast, parser::Parser, tokenizer::{Span, Token, TokenWithSpan}};
use crate::{dialect::SqlDialect, parse::{sql_error, word}};

pub(crate) struct ReturningClause<'a> { keyword: Span, items: &'a [TokenWithSpan] }

pub(crate) fn split<'a>(tokens: &'a [TokenWithSpan], source: &str) -> Result<(&'a [TokenWithSpan], Option<ReturningClause<'a>>)> {
    let accepts = ["INSERT", "UPDATE", "DELETE"].iter().any(|verb| word(tokens.first(), verb));
    let rejects = ["ERASE", "PATCH"].iter().any(|verb| word(tokens.first(), verb));
    if !accepts && !rejects { return Ok((tokens, None)); }
    let mut depth = 0usize;
    for (index, token) in tokens.iter().enumerate() {
        if depth == 0 && word(Some(token), "RETURNING") {
            if rejects { return Err(sql_error("RETURNING is only allowed on INSERT, UPDATE and DELETE", source, token.span)); }
            return Ok((&tokens[..index], Some(ReturningClause { keyword: token.span, items: &tokens[index + 1..] })));
        }
        match token.token {
            Token::LParen | Token::LBracket | Token::LBrace => depth += 1,
            Token::RParen | Token::RBracket | Token::RBrace => depth = depth.saturating_sub(1),
            _ => {}
        }
    }
    Ok((tokens, None))
}

impl ReturningClause<'_> {
    pub(crate) fn validate(self, source: &str) -> Result<()> {
        let mut tokens = vec![TokenWithSpan { token: Token::make_keyword("RETURNING"), span: self.keyword }];
        let mut depth = 0usize;
        for token in self.items {
            if depth == 0 && word(Some(token), "FOR") { return Err(sql_error("Temporal clauses require a table inside a RETURNING subquery", source, token.span)); }
            match token.token {
                Token::LParen | Token::LBracket | Token::LBrace => depth += 1,
                Token::RParen | Token::RBracket | Token::RBrace => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
        tokens.extend(ignored_xtql(self.items, source)?);
        let (tokens, _) = crate::wildcard::extract(tokens, source)?;
        let (tokens, materialized) = crate::materialized::cte_tokens(tokens, source)?;
        let (tokens, mut temporal) = crate::parse::extract_temporal(tokens, source)?;
        let mut materialized = crate::materialized::MarkerVisitor::new(materialized, source)?;
        materialized.apply_temporal(&mut temporal)?;
        let dialect = SqlDialect::default();
        let mut parser = Parser::new(&dialect).with_tokens_with_locations(tokens.into_iter().skip(1).collect());
        let mut saw_star = false;
        let mut at_start = true;
        loop {
            let token = parser.peek_token();
            if token.token == Token::EOF { break; }
            if parser.consume_token(&Token::Comma) { at_start = false; continue; }
            if ["ALL", "DISTINCT"].iter().any(|keyword| word(Some(&token), keyword)) { return Err(sql_error("RETURNING accepts a select list without a set quantifier", source, token.span)); }
            let mut item = dialect.checked(parser.parse_select_item()).map_err(|error| sql_error(error.to_string(), source, token.span))?;
            materialized.apply(&mut item)?;
            let wildcard = match &item {
                ast::SelectItem::Wildcard(options) => {
                    if saw_star { return Err(sql_error("RETURNING permits one unqualified wildcard", source, token.span)); }
                    saw_star = true;
                    Some(options)
                }
                ast::SelectItem::QualifiedWildcard(ast::SelectItemQualifiedWildcardKind::ObjectName(_), options) => Some(options),
                ast::SelectItem::UnnamedExpr(_) => None,
                ast::SelectItem::ExprWithAlias { alias, .. } => {
                    let explicit_as = self.items.iter().position(|token| token.span == alias.span).and_then(|index| index.checked_sub(1)).is_some_and(|index| word(self.items.get(index), "AS"));
                    if alias.quote_style == Some('\'') || (alias.quote_style.is_none() && alias.value.eq_ignore_ascii_case("DEFAULT") && !explicit_as) { return Err(sql_error("Invalid RETURNING column alias", source, alias.span)); }
                    None
                }
                _ => return Err(sql_error("Unsupported RETURNING projection", source, token.span)),
            };
            if let Some(options) = wildcard {
                if options.opt_ilike.is_some() || options.opt_except.is_some() || options.opt_replace.is_some() || options.opt_alias.is_some() { return Err(sql_error("Unsupported RETURNING wildcard modifier", source, token.span)); }
            }
            if !at_start && matches!(item, ast::SelectItem::Wildcard(_)) {
                if parser.peek_token().token != Token::EOF { return Err(sql_error("A trailing RETURNING wildcard must end the select list", source, parser.peek_token().span)); }
                break;
            }
            at_start = false;
            match parser.peek_token().token {
                Token::EOF => break,
                Token::Comma => { parser.next_token(); }
                _ => return Err(sql_error("Expected comma or end of RETURNING select list", source, parser.peek_token().span)),
            }
        }
        materialized.finish()
    }
}

fn ignored_xtql(tokens: &[TokenWithSpan], source: &str) -> Result<Vec<TokenWithSpan>> {
    let mut output = Vec::with_capacity(tokens.len());
    let mut index = 0;
    while index < tokens.len() {
        let explicit = tokens.get(index + 1).is_some_and(|token| token.token == Token::LParen);
        let literal = index + if explicit { 2 } else { 1 };
        if !word(tokens.get(index), "XTQL") || !tokens.get(literal).is_some_and(|token| matches!(token.token, Token::SingleQuotedString(_) | Token::EscapedStringLiteral(_) | Token::DollarQuotedString(_))) {
            output.push(tokens[index].clone()); index += 1; continue;
        }
        let mut end = literal + 1;
        if explicit {
            while tokens.get(end).is_some_and(|token| token.token == Token::Comma) {
                if !tokens.get(end + 1).is_some_and(|token| matches!(token.token, Token::Placeholder(_))) { return Err(sql_error("XTQL argument must be a parameter", source, tokens[end].span)); }
                end += 2;
            }
            if tokens.get(end).is_none_or(|token| token.token != Token::RParen) { return Err(sql_error("Expected closing parenthesis after XTQL parameters", source, tokens.get(end).map_or(tokens[index].span, |token| token.span))); }
            end += 1;
        }
        let span = Span::new(tokens[index].span.start, tokens[end - 1].span.end);
        output.push(TokenWithSpan { token: Token::make_keyword("SELECT"), span });
        output.push(TokenWithSpan { token: Token::Number("1".into(), false), span });
        index = end;
    }
    Ok(output)
}
