use std::{collections::BTreeMap, ops::ControlFlow};
use sqlparser::{ast::{self, VisitMut, VisitorMut}, tokenizer::{Span, Token, TokenWithSpan}};
use rtdb_types::Result;

pub(crate) fn cte_tokens(mut tokens: Vec<TokenWithSpan>, source: &str) -> Result<(Vec<TokenWithSpan>, Vec<Span>)> {
    let mut markers = BTreeMap::new();
    let mut index = 0;
    while index < tokens.len() {
        if !word(tokens.get(index), "WITH") { index += 1; continue; }
        let mut head = index + 1;
        if word(tokens.get(head), "RECURSIVE") { head += 1; }
        loop {
            let materialized = word(tokens.get(head), "MATERIALIZED");
            let name = head + usize::from(materialized);
            if !matches!(tokens.get(name).map(|token| &token.token), Some(Token::Word(_))) { break; }
            let mut after_name = name + 1;
            if tokens.get(after_name).is_some_and(|token| token.token == Token::LParen) {
                let Some(end) = closing(&tokens, after_name) else { break; }; after_name = end + 1;
            }
            if !word(tokens.get(after_name), "AS") { break; }
            if word(tokens.get(after_name + 1), "MATERIALIZED")
                || (word(tokens.get(after_name + 1), "NOT") && word(tokens.get(after_name + 2), "MATERIALIZED")) {
                return Err(crate::parse::sql_error("MATERIALIZED precedes the common-table name", source, tokens[after_name + 1].span));
            }
            if tokens.get(after_name + 1).is_none_or(|token| token.token != Token::LParen) { break; }
            let Some(end) = closing(&tokens, after_name + 1) else { break; };
            if materialized {
                if markers.insert(head, tokens[name].span).is_some() { return Err(crate::parse::sql_error("Duplicate materialized CTE marker", source, tokens[head].span)); }
            }
            if tokens.get(end + 1).is_none_or(|token| token.token != Token::Comma) { break; }
            head = end + 2;
        }
        index += 1;
    }
    let spans = markers.values().copied().collect();
    for index in markers.keys().rev() { tokens.remove(*index); }
    Ok((tokens, spans))
}
fn word(token: Option<&TokenWithSpan>, expected: &str) -> bool {
    matches!(token.map(|token| &token.token), Some(Token::Word(word)) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected))
}
fn closing(tokens: &[TokenWithSpan], start: usize) -> Option<usize> {
    let mut depth = 0usize;
    for (index, token) in tokens.iter().enumerate().skip(start) {
        match token.token {
            Token::LParen => depth += 1,
            Token::RParen => { depth = depth.checked_sub(1)?; if depth == 0 { return Some(index); } }
            _ => {}
        }
    }
    None
}


pub(crate) struct MarkerVisitor<'a> { markers: BTreeMap<Span, bool>, source: &'a str }
impl VisitorMut for MarkerVisitor<'_> {
    type Break = rtdb_types::Anomaly;
    fn pre_visit_query(&mut self, query: &mut ast::Query) -> ControlFlow<Self::Break> {
        if let Some(with) = &mut query.with {
            for cte in &mut with.cte_tables {
                if let Some(applied) = self.markers.get_mut(&cte.alias.name.span) {
                    if *applied { return ControlFlow::Break(crate::parse::sql_error("Repeated materialized CTE marker", self.source, cte.alias.name.span)); }
                    cte.materialized = Some(ast::CteAsMaterialized::Materialized);
                    *applied = true;
                }
            }
        }
        ControlFlow::Continue(())
    }
}
impl<'a> MarkerVisitor<'a> {
    pub(crate) fn new(spans: Vec<Span>, source: &'a str) -> Result<Self> {
        let mut markers = BTreeMap::new();
        for span in spans {
            if markers.insert(span, false).is_some() { return Err(crate::parse::sql_error("Duplicate materialized CTE span", source, span)); }
        }
        Ok(Self { markers, source })
    }
    pub(crate) fn apply(&mut self, value: &mut impl VisitMut) -> Result<()> {
        if self.markers.is_empty() { return Ok(()); }
        match value.visit(self) { ControlFlow::Continue(()) => Ok(()), ControlFlow::Break(error) => Err(error) }
    }
    pub(crate) fn apply_temporal(&mut self, clauses: &mut [crate::parse::TemporalClause]) -> Result<()> {
        use crate::parse::TemporalExtent;
        for clause in clauses {
            match &mut clause.extent {
                TemporalExtent::At(value) => self.apply(value)?,
                TemporalExtent::FromTo(from, to) => { self.apply(from)?; self.apply(to)?; },
                TemporalExtent::Between(from, to) => { self.apply(from)?; self.apply(to)?; },
                TemporalExtent::All => {},
            }
        }
        Ok(())
    }
    pub(crate) fn finish(self) -> Result<()> {
        if let Some((span, _)) = self.markers.iter().find(|(_, applied)| !**applied) { return Err(crate::parse::sql_error("Unmatched materialized CTE marker", self.source, *span)); }
        Ok(())
    }
}

pub(crate) fn restore(syntax: &mut crate::parse::Syntax, temporal: &mut [crate::parse::TemporalClause], spans: Vec<Span>, source: &str) -> Result<()> {
    if spans.is_empty() { return Ok(()); }
    let mut visitor = MarkerVisitor::new(spans, source)?;
    use crate::parse::Syntax;
    match syntax {
        Syntax::Standard(statement) => visitor.apply(statement)?,
        Syntax::Erase(delete) => visitor.apply(delete)?,
        Syntax::Assert(expression, _) => visitor.apply(expression)?,
        Syntax::Control(control) => {
            if let ControlFlow::Break(error) = crate::control::visit_control_expressions(control, &mut visitor) { return Err(error); }
        }
        Syntax::Records { records, .. } => visitor.apply(records)?,
        Syntax::CreateTable { .. } | Syntax::CreateSearchIndex { .. } | Syntax::DropSearchIndex { .. } | Syntax::CreateOrderedIndex { .. } | Syntax::DropOrderedIndex { .. } | Syntax::CreateVectorIndex { .. } | Syntax::DropVectorIndex { .. } | Syntax::GrantRole { .. } => {},
    }
    visitor.apply_temporal(temporal)?;
    visitor.finish()
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::{Syntax, TemporalExtent};

    fn marked(expression: &ast::Expr) {
        let query = match expression {
            ast::Expr::Subquery(query) | ast::Expr::Exists { subquery: query, .. } => query,
            ast::Expr::Nested(expression) => return marked(expression),
            _ => panic!("Expected subquery"),
        };
        assert_eq!(query.with.as_ref().unwrap().cte_tables[0].materialized, Some(ast::CteAsMaterialized::Materialized));
    }

    #[test]
    fn materialized_markers_reach_custom_assert_set_and_record_expressions() {
        let mut statements = crate::parse("ASSERT EXISTS (WITH MATERIALIZED c AS (SELECT 1 AS n) SELECT n FROM c)").unwrap();
        let Syntax::Assert(expression, _) = &statements.remove(0).tree.statement else { panic!("ASSERT") };
        marked(expression);
        let mut statements = crate::parse("SET TIME ZONE (WITH MATERIALIZED c AS (SELECT 'UTC' AS n) SELECT n FROM c)").unwrap();
        let Syntax::Control(rtdb_plan::ControlStatement::SetTimeZone(value)) = &statements.remove(0).tree.statement else { panic!("SET TIME ZONE") };
        marked(value);
        let mut statements = crate::parse("INSERT INTO docs RECORDS {_id: 1, v: (WITH MATERIALIZED c AS (SELECT 1 AS n) SELECT n FROM c)}").unwrap();
        let Syntax::Records { records, .. } = &statements.remove(0).tree.statement else { panic!("RECORDS") };
        let ast::Expr::Dictionary(fields) = &records[0] else { panic!("record") };
        marked(&fields[1].value);
    }

    #[test]
    fn materialized_markers_reach_temporal_and_ignored_returning_subqueries() {
        let mut statements = crate::parse("SELECT * FROM docs FOR SYSTEM_TIME AS OF (WITH MATERIALIZED c AS (SELECT TIMESTAMP '2000-01-01Z' AS t) SELECT t FROM c)").unwrap();
        let TemporalExtent::At(expression) = &statements.remove(0).tree.temporal[0].extent else { panic!("AS OF") };
        marked(expression);
        crate::parse("INSERT INTO docs (_id) VALUES (1) RETURNING (WITH MATERIALIZED c AS (SELECT 1 AS n) SELECT n FROM c)").unwrap();
        assert!(crate::parse("INSERT INTO docs (_id) VALUES (1) RETURNING (WITH c AS MATERIALIZED (SELECT 1 AS n) SELECT n FROM c)").is_err());
    }
}
