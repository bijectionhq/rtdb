use std::{collections::BTreeSet, ops::ControlFlow};

use rtdb_types::{Anomaly, AnomalyKind, Result};
use sqlparser::{ast, parser::Parser, tokenizer::{Location, Span, Token, TokenWithSpan}};
use sqlparser::ast::Visit;
use crate::{bind::grammar_rejected, dialect::{CustomStringLiteral, SqlDialect}, parse::{TemporalExtent, sql_error, word}};

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
        let mut syntax = ReturningSyntax::default();
        for clause in &temporal {
            if clause.portion {
                return Err(grammar_rejected("A DML-only temporal period specification in a RETURNING subquery"));
            }
            match &clause.extent {
                TemporalExtent::All => (),
                TemporalExtent::At(at) => syntax.validate(at)?,
                TemporalExtent::FromTo(from, Some(to)) | TemporalExtent::Between(from, to) => {
                    syntax.validate(from)?;
                    syntax.validate(to)?;
                }
                TemporalExtent::FromTo(_, None) => return Err(grammar_rejected("A RETURNING subquery period FROM without TO")),
            }
        }
        let dialect = SqlDialect::default();
        let mut parser = Parser::new(&dialect).with_tokens_with_locations(tokens.into_iter().skip(1).collect());
        let mut saw_star = false;
        let mut at_start = true;
        loop {
            let token = parser.peek_token();
            if token.token == Token::EOF { break; }
            if parser.consume_token(&Token::Comma) { at_start = false; continue; }
            if ["ALL", "DISTINCT"].iter().any(|keyword| word(Some(&token), keyword)) { return Err(sql_error("RETURNING accepts a select list without a set quantifier", source, token.span)); }
            let mut item = dialect.checked(parser.parse_select_item()).map_err(|error| crate::parse::parser_error(error, source, token.span))?;
            materialized.apply(&mut item)?;
            let wildcard = match &item {
                ast::SelectItem::Wildcard(options) => {
                    if saw_star { return Err(sql_error("RETURNING permits one unqualified wildcard", source, token.span)); }
                    saw_star = true;
                    Some(options)
                }
                ast::SelectItem::QualifiedWildcard(ast::SelectItemQualifiedWildcardKind::ObjectName(name), options) => {
                    for part in &name.0 {
                        let part = part.as_ident().ok_or_else(|| grammar_rejected("A computed wildcard qualifier"))?;
                        crate::bind::grammar_identifier(part, "wildcard qualifier")?;
                    }
                    Some(options)
                }
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
            syntax.validate(&item)?;
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
        if temporal.iter().any(|clause| !syntax.table_ends.contains(&clause.owner_end)) {
            return Err(grammar_rejected("A misplaced temporal period specification in a RETURNING subquery"));
        }
        materialized.finish()
    }
}

#[derive(Default)]
struct ReturningSyntax {
    table_ends: BTreeSet<Location>,
}

impl ReturningSyntax {
    fn validate(&mut self, item: &impl Visit) -> Result<()> {
        match item.visit(self) {
            ControlFlow::Continue(()) => Ok(()),
            ControlFlow::Break(error) => Err(error),
        }
    }
}

impl ast::Visitor for ReturningSyntax {
    type Break = Anomaly;

    fn pre_visit_table_factor(&mut self, table: &ast::TableFactor) -> ControlFlow<Anomaly> {
        if let ast::TableFactor::Table { name, args: None, .. } = table
            && let Some(name) = name.0.last().and_then(ast::ObjectNamePart::as_ident)
        {
            self.table_ends.insert(name.span.end);
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &ast::Expr) -> ControlFlow<Anomaly> {
        use ast::{BinaryOperator as B, DataType as D, Expr as E, TimezoneInfo as T, UnaryOperator as U};
        let invalid = match expr {
            E::Cast { kind, data_type, format, .. } => {
                if format.is_some() || !matches!(kind, ast::CastKind::Cast | ast::CastKind::DoubleColon) {
                    return ControlFlow::Break(grammar_rejected("TRY_CAST or a cast format"));
                }
                if let Err(error) = crate::types::cast_target(data_type, None)
                    && error.is(AnomalyKind::GrammarRejected)
                {
                    return ControlFlow::Break(error);
                }
                false
            }
            E::TypedString(value) => value.uses_odbc_syntax
                || !(matches!(value.data_type, D::Date | D::Time(None, T::None) | D::Uuid
                    | D::Timestamp(None, T::None | T::WithTimeZone | T::WithoutTimeZone))
                    || CustomStringLiteral::from_type(&value.data_type).is_some()),
            E::BinaryOp { op, .. } => matches!(op, B::Spaceship | B::Xor | B::DuckIntegerDivide | B::MyIntegerDivide
                | B::Match | B::Regexp | B::Glob | B::PGOverlap | B::PGLikeMatch | B::PGILikeMatch
                | B::PGNotLikeMatch | B::PGNotILikeMatch | B::BitwiseXor | B::PGExp),
            E::UnaryOp { op, .. } => !matches!(op, U::Plus | U::Minus | U::Not | U::BitwiseNot),
            E::Like { any, .. } | E::ILike { any, .. } => *any,
            E::Substring { substring_from, .. } => substring_from.is_none(),
            E::Trim { trim_characters, .. } => trim_characters.is_some(),
            E::Ceil { field, .. } | E::Floor { field, .. } => {
                !matches!(field, ast::CeilFloorKind::DateTimeField(ast::DateTimeField::NoDateTime))
            }
            E::Function(function) => invalid_function_syntax(function),
            E::Collate { .. } | E::Convert { .. } | E::SimilarTo { .. } | E::RLike { .. } | E::InUnnest { .. } | E::AtTimeZone { .. }
            | E::IsJson { .. } | E::IsNormalized { .. } => true,
            _ => false,
        };
        if invalid { ControlFlow::Break(grammar_rejected(format!("RETURNING expression {expr}"))) }
        else { ControlFlow::Continue(()) }
    }

    fn pre_visit_value(&mut self, value: &ast::ValueWithSpan) -> ControlFlow<Anomaly> {
        let invalid = match &value.value {
            ast::Value::Number(text, long) => *long || text.starts_with('.') || text.ends_with('.') || text.contains(".e") || text.contains(".E"),
            ast::Value::Null | ast::Value::Boolean(_) | ast::Value::Placeholder(_) | ast::Value::SingleQuotedString(_)
            | ast::Value::EscapedStringLiteral(_) | ast::Value::DollarQuotedString(_) => false,
            _ => true,
        };
        if invalid { ControlFlow::Break(grammar_rejected(format!("RETURNING literal {}", value.value))) }
        else { ControlFlow::Continue(()) }
    }
}

fn invalid_function_syntax(function: &ast::Function) -> bool {
    if function.uses_odbc_syntax || !matches!(function.parameters, ast::FunctionArguments::None) {
        return true;
    }
    if function.over.is_some() && crate::window::grammar_window_name(function).is_err() { return true; }
    if matches!(function.args, ast::FunctionArguments::Subquery(_)) { return true; }
    let ast::FunctionArguments::List(arguments) = &function.args else { return false; };
    let name = match function.name.0.as_slice() {
        [part] => part.as_ident().filter(|name| name.quote_style.is_none()).map(|name| name.value.to_ascii_lowercase()),
        _ => None,
    };
    let name = name.as_deref();
    if function.over.is_some() {
        if matches!(name, Some("row_number" | "rank" | "dense_rank" | "percent_rank" | "cume_dist"))
            && (!arguments.args.is_empty() || function.null_treatment.is_some())
        {
            return true;
        }
        if matches!(name, Some("lead" | "lag")) {
            if !(1..=3).contains(&arguments.args.len()) { return true; }
            if let Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(offset))) = arguments.args.get(1)
                && crate::window::offset_literal(offset).is_err()
            {
                return true;
            }
        }
    }
    let count_star = name == Some("count")
        && matches!(arguments.args.as_slice(), [ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard)]);
    let aggregate = count_star || matches!(name, Some("count" | "avg" | "min" | "max" | "sum" | "every" | "bool_and"
        | "bool_or" | "var_pop" | "var_samp" | "stddev_pop" | "stddev_samp"));
    let ordered_set = matches!(name, Some("percentile_cont" | "percentile_disc"));
    let array_agg = name == Some("array_agg");
    (name == Some("coalesce") && arguments.args.is_empty())
        || (arguments.duplicate_treatment.is_some() && (!aggregate || count_star))
        || (function.filter.is_some() && !(aggregate || ordered_set || array_agg))
        || (!function.within_group.is_empty() && !ordered_set)
        || arguments.args.iter().any(|arg| !count_star && !matches!(arg, ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(_))))
        || arguments.clauses.iter().any(|clause| match clause {
            ast::FunctionArgumentClause::OrderBy(_) => !array_agg,
            ast::FunctionArgumentClause::UsingCharacterLengthUnits(_) => !matches!(name, Some("char_length" | "character_length")),
            _ => true,
        })
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
