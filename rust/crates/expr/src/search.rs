use crate::EvalContext;
use crate::compare::nullish;
use rtdb_types::{Result, Value};

/// Only the UTF-8 leg of a column is indexed, so any other value matches nothing and scores nothing.
pub(crate) fn call(name: &str, args: &[Value], context: &EvalContext<'_>) -> Option<Result<Value>> {
    if !matches!(name, "match" | "search_score") { return None; }
    Some((|| {
        if args.iter().any(nullish) { return Ok(Value::Null); }
        let query = match &args[1] { Value::Utf8(query) => rtdb_text::Query::parse(query), _ => return Ok(Value::Null) };
        let tokens = match &args[0] { Value::Utf8(text) => rtdb_text::tokens(text), _ => Vec::new() };
        context.reserve(tokens.iter().map(|token| token.len() + 32).sum::<usize>().saturating_add(256))?;
        Ok(match name {
            "match" => Value::Bool(query.matches(&tokens)),
            _ => Value::F64(rtdb_text::bm25(&query, &tokens, None)),
        })
    })())
}
