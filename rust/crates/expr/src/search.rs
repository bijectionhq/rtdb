use crate::EvalContext;
use crate::compare::nullish;
use crate::numeric::integer;
use rtdb_text::{Corpus, Query, Scorer, stored_length, tokens};
use rtdb_types::{Anomaly, Result, Value};

/// Only the UTF-8 leg of a column is indexed, so any other value matches nothing and scores nothing.
pub(crate) fn call(name: &str, args: &[Value], context: &EvalContext<'_>) -> Option<Result<Value>> {
    if !matches!(
        name,
        "match" | "search_score" | "search_length" | "search_terms"
    ) {
        return None;
    }
    Some((|| {
        if args.iter().any(nullish) {
            return Ok(Value::Null);
        }
        match (name, args) {
            ("search_terms", [Value::Utf8(query)]) => {
                let query = parse_query(query, context)?;
                Ok(Value::List(
                    query.terms().iter().cloned().map(Value::Utf8).collect(),
                ))
            }
            ("search_length", [text]) => {
                let tokens = tokens_of(text, context)?;
                Ok(Value::I64(i64::from(stored_length(tokens.len()))))
            }
            ("match", [text, Value::Utf8(query)]) => {
                let tokens = tokens_of(text, context)?;
                Ok(Value::Bool(Query::parse(query).matches(&tokens)))
            }
            ("search_score", [text, Value::Utf8(query)]) => {
                let tokens = tokens_of(text, context)?;
                Ok(Value::F64(Scorer::new(Query::parse(query), None).bm25(&tokens)))
            }
            ("search_score", [text, Value::Utf8(query), documents, total, frequencies]) => {
                let query = parse_query(query, context)?;
                let tokens = tokens_of(text, context)?;
                let corpus = explicit_corpus(&query, documents, total, frequencies)?;
                Ok(Value::F64(Scorer::new(query, Some(&corpus)).bm25(&tokens)))
            }
            _ => Ok(Value::Null),
        }
    })())
}

fn tokens_of(value: &Value, context: &EvalContext<'_>) -> Result<Vec<String>> {
    match value {
        Value::Utf8(text) => document_tokens(text, context),
        _ => Ok(Vec::new()),
    }
}

fn document_tokens(text: &str, context: &EvalContext<'_>) -> Result<Vec<String>> {
    let tokens = tokens(text);
    context.reserve(
        tokens
            .iter()
            .map(|token| token.len() + 32)
            .sum::<usize>()
            .saturating_add(256),
    )?;
    Ok(tokens)
}

fn parse_query(text: &str, context: &EvalContext<'_>) -> Result<Query> {
    context.reserve(text.len().saturating_mul(128).saturating_add(256))?;
    Ok(Query::parse(text))
}

fn explicit_corpus(
    query: &Query,
    documents: &Value,
    total: &Value,
    list: &Value,
) -> Result<Corpus> {
    let invalid = || {
        Anomaly::incorrect(
            "rtdb.expr/invalid-search-corpus",
            "Search statistics require nonnegative counts and one document frequency per search term",
        )
    };
    let count = |value: &Value| {
        integer(value)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(invalid)
    };
    let Value::List(list) = list else {
        return Err(invalid());
    };
    let documents = count(documents)?;
    let tokens = count(total)?;
    if list.len() != query.terms().len() {
        return Err(invalid());
    }
    let frequencies = query
        .terms()
        .iter()
        .zip(list)
        .map(|(term, value)| {
            let frequency = count(value)?;
            if frequency > documents || frequency > tokens {
                return Err(invalid());
            }
            Ok((term.clone(), frequency))
        })
        .collect::<Result<_>>()?;
    Ok(Corpus {
        documents,
        tokens,
        frequencies,
    })
}

/// MATCH of a value against a parsed query: null where the value is null or absent, and false for a value that is not text.
pub fn match_value(value: &Value, query: &Query) -> Value {
    match value {
        value if nullish(value) => Value::Null,
        Value::Utf8(text) => Value::Bool(query.matches(&tokens(text))),
        _ => Value::Bool(false),
    }
}
