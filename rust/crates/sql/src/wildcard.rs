use std::collections::{BTreeMap, BTreeSet};
use rtdb_plan::{ColumnId, Identifier, NamedExpr, Schema};
use rtdb_types::Result;
use sqlparser::{ast, tokenizer::{Span, Token, TokenWithSpan}};
use crate::{bind::{Context, error, grammar_rejected, identifier, identifiers, unsupported}, parse::{sql_error, word}};

#[derive(Clone, Debug)]
pub(crate) struct WildcardRename { source: Span, columns: Vec<Identifier> }

#[derive(Default)]
struct ProjectionScope { item_start: Option<usize>, closed: bool }

pub(crate) fn extract(tokens: Vec<TokenWithSpan>, source: &str) -> Result<(Vec<TokenWithSpan>, Vec<WildcardRename>)> {
    let mut scopes = vec![ProjectionScope::default()];
    let mut removals = BTreeSet::new();
    let mut insertions = BTreeMap::new();
    let mut renames = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let scope = scopes.last_mut().ok_or_else(|| error("Wildcard token scope is absent"))?;
        if (word(tokens.get(index), "SELECT") && scope.item_start.is_none() && !scope.closed) || word(tokens.get(index), "RETURNING") {
            scope.item_start = Some(index + 1);
        } else if ["UNION", "EXCEPT", "INTERSECT"].iter().any(|name| word(tokens.get(index), name)) {
            scope.item_start = None; scope.closed = false;
        } else if ["FROM", "WHERE", "GROUP", "HAVING", "WINDOW", "QUALIFY", "ORDER", "LIMIT", "OFFSET", "FETCH"].iter().any(|name| word(tokens.get(index), name)) {
            scope.item_start = None; scope.closed = true;
        } else if scope.item_start == Some(index) && ["ALL", "DISTINCT"].iter().any(|name| word(tokens.get(index), name)) {
            scope.item_start = Some(index + 1);
        }
        match &tokens[index].token {
            Token::LParen | Token::LBracket | Token::LBrace => scopes.push(ProjectionScope::default()),
            Token::RParen | Token::RBracket | Token::RBrace => { if scopes.len() > 1 { scopes.pop(); } },
            Token::Comma => if scope.item_start.is_some() { scope.item_start = Some(index + 1); },
            Token::Mul if scope.item_start.is_some_and(|start| wildcard_prefix(&tokens[start..index])) => {
                let qualified = scope.item_start != Some(index);
                let mut next = index + 1;
                if word(tokens.get(next), "EXCLUDE") { next = exclude_end(&tokens, next + 1, source)?; }
                if word(tokens.get(next), "RENAME") {
                    let parenthesized = tokens.get(next + 1).is_some_and(|token| token.token == Token::LParen);
                    next += if parenthesized { 2 } else { 1 };
                    loop {
                        let first = next;
                        let mut columns = vec![token_identifier(&tokens, next, source)?]; next += 1;
                        while tokens.get(next).is_some_and(|token| token.token == Token::Period) {
                            columns.push(token_identifier(&tokens, next + 1, source)?); next += 2;
                        }
                        if qualified && columns.len() != 1 { return Err(sql_error("A qualified wildcard renames unqualified columns", source, tokens[first].span)); }
                        for removed in first + 1..next { removals.insert(removed); }
                        let has_as = word(tokens.get(next), "AS");
                        if has_as { next += 1; }
                        token_identifier(&tokens, next, source)?;
                        if !has_as && word(tokens.get(next), "DEFAULT") { return Err(sql_error("DEFAULT requires AS", source, tokens[next].span)); }
                        if !has_as { insertions.insert(next, TokenWithSpan { token: Token::make_keyword("AS"), span: tokens[next].span }); }
                        renames.push(WildcardRename { source: tokens[first].span, columns });
                        next += 1;
                        if !parenthesized { break; }
                        match tokens.get(next).map(|token| &token.token) {
                            Some(Token::Comma) => next += 1,
                            Some(Token::RParen) => { next += 1; break; }
                            _ => return Err(sql_error("Expected comma or closing parenthesis in wildcard RENAME", source, tokens.get(next).map_or(tokens[first].span, |token| token.span))),
                        }
                    }
                }
                if ["EXCLUDE", "RENAME"].iter().any(|name| word(tokens.get(next), name)) { return Err(sql_error("Wildcard modifiers require one EXCLUDE followed by one RENAME", source, tokens[next].span)); }
                index = next; continue;
            }
            _ => {}
        }
        index += 1;
    }
    let mut result = Vec::with_capacity(tokens.len().checked_add(insertions.len()).ok_or_else(|| error("Wildcard token count overflow"))?);
    for (index, token) in tokens.into_iter().enumerate() {
        if let Some(insertion) = insertions.remove(&index) { result.push(insertion); }
        if !removals.contains(&index) { result.push(token); }
    }
    Ok((result, renames))
}

fn wildcard_prefix(tokens: &[TokenWithSpan]) -> bool {
    tokens.is_empty() || tokens.len().is_multiple_of(2) && tokens.chunks_exact(2).all(|pair| matches!(&pair[0].token, Token::Word(_)) && pair[1].token == Token::Period)
}

fn token_identifier(tokens: &[TokenWithSpan], index: usize, source: &str) -> Result<Identifier> {
    let token = tokens.get(index).ok_or_else(|| sql_error("Expected wildcard identifier", source, tokens.last().map_or(Span::empty(), |token| token.span)))?;
    if ["FROM", "WHERE", "GROUP", "HAVING", "ORDER", "LIMIT", "OFFSET", "FETCH", "EXCLUDE", "RENAME", "AS"].iter().any(|name| word(Some(token), name)) {
        return Err(sql_error("Expected wildcard identifier", source, token.span));
    }
    match &token.token {
        Token::Word(word) => Ok(Identifier { text: word.value.clone(), quoted: word.quote_style.is_some() }),
        _ => Err(sql_error("Expected wildcard identifier", source, token.span)),
    }
}

fn exclude_end(tokens: &[TokenWithSpan], mut index: usize, source: &str) -> Result<usize> {
    let parenthesized = tokens.get(index).is_some_and(|token| token.token == Token::LParen);
    if parenthesized { index += 1; }
    loop {
        token_identifier(tokens, index, source)?; index += 1;
        if tokens.get(index).is_some_and(|token| token.token == Token::Period) { return Err(sql_error("Wildcard EXCLUDE requires unqualified identifiers", source, tokens[index].span)); }
        if !parenthesized { return Ok(index); }
        match tokens.get(index).map(|token| &token.token) {
            Some(Token::Comma) => index += 1,
            Some(Token::RParen) => return Ok(index + 1),
            _ => return Err(sql_error("Expected comma or closing parenthesis in wildcard EXCLUDE", source, tokens.get(index).map_or(tokens[index - 1].span, |token| token.span))),
        }
    }
}

impl Context<'_> {
    pub(crate) fn wildcard(&mut self, schema: &Schema, qualifier: Option<&ast::ObjectName>, options: &ast::WildcardAdditionalOptions, explicit_names: &BTreeSet<String>) -> Result<Vec<NamedExpr>> {
        if options.opt_ilike.is_some() || options.opt_except.is_some() || options.opt_replace.is_some() || options.opt_alias.is_some() { return Err(grammar_rejected("A wildcard ILIKE, EXCEPT, REPLACE or alias")); }
        let qualifier = qualifier.map(|name| {
            let names = identifiers(name)?;
            if names.len() != 1 { return Err(unsupported("Schema-qualified wildcard")); }
            Ok(names[0].clone())
        }).transpose()?;
        let mut excludes = explicit_names.clone();
        let excluded: &[ast::ObjectName] = match &options.opt_exclude { None => &[], Some(ast::ExcludeSelectItem::Single(name)) => std::slice::from_ref(name), Some(ast::ExcludeSelectItem::Multiple(names)) => names };
        for name in excluded {
            let names = identifiers(name)?;
            if names.len() != 1 { return Err(error("Wildcard EXCLUDE requires unqualified identifiers")); }
            excludes.insert(names[0].normalized());
        }
        let (selected, renames) = if let Some(qualifier) = &qualifier {
            let selected = self.wildcard_columns(schema, Some(&qualifier.normalized()), &excludes)?;
            if selected.is_empty() { return Err(error(format!("Table not found: {}", qualifier.normalized()))); }
            let renames = self.wildcard_rename_map(schema, Some(qualifier), options)?;
            (selected, renames)
        } else {
            let renames = self.wildcard_rename_map(schema, None, options)?;
            (self.wildcard_columns(schema, None, &excludes)?, renames)
        };
        selected.into_iter().enumerate().map(|(index, (field, expr))| {
            let name = renames.get(&field.id).cloned().unwrap_or_else(|| if field.unnamed { format!("_column_{}", index + 1) } else { field.name });
            Ok(NamedExpr { field: self.binder.field(name, None, field.data_type)?, expr })
        }).collect()
    }

    fn wildcard_rename_map(&mut self, schema: &Schema, qualifier: Option<&Identifier>, options: &ast::WildcardAdditionalOptions) -> Result<BTreeMap<ColumnId, String>> {
        let pairs: &[ast::IdentWithAlias] = match &options.opt_rename { None => &[], Some(ast::RenameSelectItem::Single(pair)) => std::slice::from_ref(pair), Some(ast::RenameSelectItem::Multiple(pairs)) => pairs };
        let mut renames = BTreeMap::new();
        for pair in pairs {
            let mut columns = self.wildcard_renames.iter().find(|rename| rename.source == pair.ident.span).map(|rename| rename.columns.clone()).unwrap_or_else(|| vec![identifier(&pair.ident)]);
            if let Some(qualifier) = qualifier {
                if columns.len() != 1 { return Err(error("A qualified wildcard renames unqualified columns")); }
                columns.insert(0, qualifier.clone());
            }
            if let Some(id) = self.wildcard_source_id(&columns, schema)? { renames.insert(id, identifier(&pair.alias).normalized()); }
        }
        Ok(renames)
    }
}
