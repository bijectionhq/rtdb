use crate::EvalContext;
use crate::compare::nullish;
use crate::numeric::integer;
use rtdb_text::{Corpus, Query, Scorer, count_tokens, stored_length};
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
                let count = count_tokens(text);
                context.reserve(count.arrow_bytes().saturating_add(256))?;
                Ok(Value::I64(i64::from(stored_length(count.len()))))
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
                let (_, _, matched) = document_terms(&Query::parse(query), text, true, context)?;
                Ok(Value::Bool(matched))
            }
            ("search_score", [text, Value::Utf8(query)]) => {
                let query = Query::parse(query);
                let (length, frequencies, _) = document_terms(&query, text, false, context)?;
                Ok(Value::F64(Scorer::new(query, None).score(&frequencies, length)))
            }
            ("search_score", [text, Value::Utf8(query), documents, total, frequencies]) => {
                let query = parse_query(query, context)?;
                let (length, counts, _) = document_terms(&query, text, false, context)?;
                let corpus = explicit_corpus(&query, documents, total, frequencies)?;
                Ok(Value::F64(Scorer::new(query, Some(&corpus)).score(&counts, length)))
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

fn document_terms(query: &Query, value: &Value, phrases: bool, context: &EvalContext<'_>) -> Result<(u32, Vec<u32>, bool)> {
    let Value::Utf8(text) = value else { return Ok((0, vec![0; query.terms().len()], false)); };
    let (count, frequencies, matched) = query.text_terms(text, phrases);
    context.reserve(count.arrow_bytes().saturating_add(256))?;
    Ok((stored_length(count.len()), frequencies, matched))
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
    use super::*;
    use arrow_array::Array;

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

    #[test]
    fn arrow_search_scalar_uses_the_token_vector_charge_and_exact_score() {
        let document = "ΟΤΕ a b a b ".repeat(1030);
        let tokens = rtdb_text::analyze(&document);
        let charge = 4096 + 512 + tokens.get_array_memory_size();
        let query = r#""a b" οτε"#;
        let args = [Value::Utf8(document), Value::Utf8(query.into())];
        for (name, expected) in [
            ("match", Value::Bool(true)),
            ("search_length", Value::I64(i64::from(stored_length(tokens.len())))),
            ("search_score", Value::F64(Scorer::new(Query::parse(query), None).bm25_arrow(&tokens))),
        ] {
            let args = if name == "search_length" { &args[..1] } else { &args[..] };
            let memory = rtdb_columnar::MemoryBudget::new(charge);
            let allocation = crate::EvaluationScope::new(&memory).unwrap();
            let context = EvalContext { allocation: Some(&allocation), ..EvalContext::default() };
            assert_eq!(call(name, args, &context).unwrap().unwrap(), expected);
            assert_eq!(memory.used_bytes(), charge);
        }
    }
}
