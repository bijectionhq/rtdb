use crate::EvalContext;
use crate::compare::nullish;
use rtdb_types::{Result, Value};

/// Only the UTF-8 leg of a column is indexed, so any other value matches nothing and scores nothing.
pub(crate) fn call(name: &str, args: &[Value], context: &EvalContext<'_>) -> Option<Result<Value>> {
    if !matches!(name, "match" | "search_score") { return None; }
    Some((|| {
        if args.iter().any(nullish) { return Ok(Value::Null); }
        let Value::Utf8(query) = &args[1] else { return Ok(Value::Null) };
        let tokens = match &args[0] { Value::Utf8(text) => rtdb_text::tokens(text), _ => Vec::new() };
        context.reserve(tokens.iter().map(|token| token.len() + 32).sum::<usize>().saturating_add(256))?;
        Ok(match name {
            "match" => Value::Bool(rtdb_text::Query::parse(query).matches(&tokens)),
            _ => Value::F64(match context.corpora {
                Some(corpora) => corpora.scorer(query).bm25(&tokens),
                None => rtdb_text::Scorer::new(rtdb_text::Query::parse(query), None).bm25(&tokens),
            }),
        })
    })())
}

/// MATCH of a value against a parsed query: null where the value is null or absent, and false for a value that is not text.
pub fn match_value(value: &Value, query: &rtdb_text::Query) -> Value {
    match value {
        value if nullish(value) => Value::Null,
        Value::Utf8(text) => Value::Bool(query.matches(&rtdb_text::tokens(text))),
        _ => Value::Bool(false),
    }
}
