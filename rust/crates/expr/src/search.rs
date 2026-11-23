use crate::EvalContext;
use crate::compare::nullish;
use crate::numeric::integer;
use arrow_array::{Array, StringArray};
use rtdb_text::{Corpus, Query, Scorer, analyze, stored_length};
use rtdb_types::{Anomaly, Result, Value, text_pair_fingerprint};

/// Only the UTF-8 leg of a column is indexed, so any other value matches nothing and scores nothing.
pub(crate) fn call(name: &str, args: &[Value], context: &EvalContext<'_>) -> Option<Result<Value>> {
    if !matches!(
        name,
        "match" | "search_score" | "search_length" | "search_terms" | "search_corpus_fingerprint"
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
                Ok(Value::list(
                    query.terms().iter().cloned().map(Value::Utf8).collect(),
                ))
            }
            ("search_length", [Value::Utf8(text)]) => {
                let tokens = document_tokens(text, context)?;
                Ok(Value::I64(i64::from(stored_length(tokens.len()))))
            }
            ("search_corpus_fingerprint", [Value::List(ids), Value::List(bodies)]) => {
                if ids.is_empty() || ids.len() != bodies.len() {
                    return Err(invalid_documents());
                }
                let (ids, bodies) = (ids.view(), bodies.view());
                let pairs = (0..ids.len()).map(|i| ids.utf8(i).zip(bodies.utf8(i)).ok_or_else(invalid_documents));
                Ok(Value::Binary(text_pair_fingerprint(pairs)?.into()))
            }
            ("match", [text, Value::Utf8(query)]) => {
                let tokens = tokens_of(text, context)?;
                Ok(Value::Bool(Query::parse(query).matches_arrow(&tokens)))
            }
            ("search_score", [text, Value::Utf8(query)]) => {
                let tokens = tokens_of(text, context)?;
                Ok(Value::F64(Scorer::new(Query::parse(query), None).bm25_arrow(&tokens)))
            }
            ("search_score", [text, Value::Utf8(query), documents, total, frequencies]) => {
                let query = parse_query(query, context)?;
                let tokens = tokens_of(text, context)?;
                let corpus = explicit_corpus(&query, documents, total, frequencies)?;
                Ok(Value::F64(Scorer::new(query, Some(&corpus)).bm25_arrow(&tokens)))
            }
            _ => Ok(Value::Null),
        }
    })())
}

fn invalid_documents() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.expr/invalid-search-documents",
        "A search fingerprint requires equally sized arrays of text ids and bodies",
    )
}

fn tokens_of(value: &Value, context: &EvalContext<'_>) -> Result<StringArray> {
    match value {
        Value::Utf8(text) => document_tokens(text, context),
        _ => Ok(StringArray::from(Vec::<&str>::new())),
    }
}

fn document_tokens(text: &str, context: &EvalContext<'_>) -> Result<StringArray> {
    let tokens = analyze(text);
    context.reserve(tokens.get_array_memory_size().saturating_add(256))?;
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
        .zip(list.iter())
        .map(|(term, value)| {
            let frequency = count(value.as_ref())?;
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

#[cfg(test)]
mod tests {
    #[test]
    fn corpus_fingerprint_reads_packed_text() {
        use std::sync::Arc;
        use arrow_array::StringArray;
        use arrow_schema::{DataType, Field};
        use rtdb_types::{ListValue, Value};

        let arguments = [Value::list(vec![Value::Utf8("id".into())]), Value::list(vec![Value::Utf8("document".into())])];
        let context = crate::EvalContext::default();
        let expected = crate::scalar_call("search_corpus_fingerprint", &arguments, &context).unwrap();
        let packed = ["id", "document"].map(|value| Value::List(ListValue::from_scalar_arrow(
            Arc::new(Field::new("value", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec![value])),
        ).unwrap()));
        assert_eq!(crate::scalar_call("search_corpus_fingerprint", &packed, &context).unwrap(), expected);
    }
}
