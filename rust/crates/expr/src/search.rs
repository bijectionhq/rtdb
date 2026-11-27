use crate::EvalContext;
use crate::compare::nullish;
use crate::numeric::integer;
use rtdb_text::{Analyzer, Corpus, Query, Scorer, stored_length};
use rtdb_types::{Anomaly, Result, Value, text_pair_fingerprint};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SearchFunction {
    Match,
    SearchScore,
    SearchLength,
    SearchTerms,
    SearchCorpusFingerprint,
    SearchTokenCount,
    SearchTermMatches,
}

impl SearchFunction {
    pub(crate) fn from_canonical(name: &str) -> Option<Self> {
        Some(match name {
            "match" => Self::Match,
            "search_score" => Self::SearchScore,
            "search_length" => Self::SearchLength,
            "search_terms" => Self::SearchTerms,
            "search_corpus_fingerprint" => Self::SearchCorpusFingerprint,
            "search_token_count" => Self::SearchTokenCount,
            "search_term_matches" => Self::SearchTermMatches,
            _ => return None,
        })
    }

    pub(crate) fn canonical_name(self) -> &'static str {
        match self {
            Self::Match => "match",
            Self::SearchScore => "search_score",
            Self::SearchLength => "search_length",
            Self::SearchTerms => "search_terms",
            Self::SearchCorpusFingerprint => "search_corpus_fingerprint",
            Self::SearchTokenCount => "search_token_count",
            Self::SearchTermMatches => "search_term_matches",
        }
    }

    fn analyzer_index(self, arity: usize) -> Option<usize> {
        match (self, arity) {
            (Self::Match | Self::SearchScore | Self::SearchTermMatches, 3)
            | (Self::SearchTerms | Self::SearchLength | Self::SearchTokenCount, 2) => Some(arity - 1),
            (Self::SearchScore, 6) => Some(5),
            _ => None,
        }
    }

    /// Only the UTF-8 leg of a column is indexed, so any other value matches nothing and scores nothing.
    pub(crate) fn eval(self, args: &[Value], context: &EvalContext<'_>) -> Result<Value> {
        if args.iter().any(nullish) {
            return Ok(Value::Null);
        }
        let (analyzer, args) = match self.analyzer_index(args.len()) {
            Some(index) => match &args[index] {
                Value::Utf8(analyzer) => (rtdb_plan::parse_analyzer(analyzer)?, &args[..index]),
                _ => return Err(Anomaly::incorrect("xtdb.expression/type-error", format!("The analyzer argument to `{}` must be a string", self.canonical_name()))),
            },
            None => (Analyzer::Simple, args),
        };
        match (self, args) {
            (Self::SearchTerms, [Value::Utf8(query)]) => {
                let query = parse_query(query, analyzer, context)?;
                Ok(Value::list(
                    query.terms().iter().cloned().map(Value::Utf8).collect(),
                ))
            }
            (Self::SearchLength | Self::SearchTokenCount, [Value::Utf8(text)]) => {
                let count = analyzer.count_tokens(text);
                context.reserve(count.arrow_bytes().saturating_add(256))?;
                Ok(Value::I64(if self == Self::SearchLength { i64::from(stored_length(count.len())) } else { count.len() as i64 }))
            }
            (Self::SearchTermMatches, [Value::Utf8(text), Value::Utf8(query)]) => {
                let query = parse_query(query, analyzer, context)?;
                let (count, frequencies, _) = query.text_terms(text, false);
                context.reserve(count.arrow_bytes().saturating_add(256))?;
                Ok(Value::list(frequencies.into_iter().map(|count| Value::Bool(count != 0)).collect()))
            }
            (Self::SearchCorpusFingerprint, [Value::List(ids), Value::List(bodies)]) => {
                if ids.is_empty() || ids.len() != bodies.len() {
                    return Err(invalid_documents());
                }
                let (ids, bodies) = (ids.view(), bodies.view());
                let pairs = (0..ids.len()).map(|i| ids.utf8(i).zip(bodies.utf8(i)).ok_or_else(invalid_documents));
                Ok(Value::Binary(text_pair_fingerprint(pairs)?.into()))
            }
            (Self::Match, [text, Value::Utf8(query)]) => {
                let (_, _, matched) = document_terms(&Query::analyzed(analyzer, query), text, true, context)?;
                Ok(Value::Bool(matched))
            }
            (Self::SearchScore, [text, Value::Utf8(query)]) => {
                let query = Query::analyzed(analyzer, query);
                let (length, frequencies, _) = document_terms(&query, text, false, context)?;
                Ok(Value::F64(Scorer::new(query, None).score(&frequencies, length)))
            }
            (Self::SearchScore, [text, Value::Utf8(query), documents, total, frequencies]) => {
                let query = parse_query(query, analyzer, context)?;
                let (length, counts, _) = document_terms(&query, text, false, context)?;
                Ok(Value::F64(search_corpus_scorer(query, documents, total, frequencies)?.score(&counts, length)))
            }
            _ => Ok(Value::Null),
        }
    }
}

#[cfg(test)]
fn call(name: &str, args: &[Value], context: &EvalContext<'_>) -> Option<Result<Value>> {
    SearchFunction::from_canonical(name).map(|function| function.eval(args, context))
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

fn parse_query(text: &str, analyzer: Analyzer, context: &EvalContext<'_>) -> Result<Query> {
    context.reserve(text.len().saturating_mul(128).saturating_add(256))?;
    Ok(Query::analyzed(analyzer, text))
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

pub fn search_corpus_scorer(query: Query, documents: &Value, tokens: &Value, frequencies: &Value) -> Result<Scorer> {
    let corpus = explicit_corpus(&query, documents, tokens, frequencies)?;
    Ok(Scorer::new(query, Some(&corpus)))
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
    fn a_named_analyzer_tokenises_both_the_query_and_the_document() {
        let context = EvalContext::default();
        let text = |value: &str| Value::Utf8(value.into());
        let call = |name: &str, args: &[Value]| call(name, args, &context).unwrap();
        assert_eq!(call("match", &[text("The runners ran"), text("runner"), text("english")]).unwrap(), Value::Bool(true));
        assert_eq!(call("match", &[text("The runners ran"), text("runner")]).unwrap(), Value::Bool(false));
        assert_eq!(call("search_terms", &[text("The runners"), text("english_stop")]).unwrap(), Value::list(vec![text("runner")]));
        assert_eq!(call("search_length", &[text("the a an runners"), text("english_stop")]).unwrap(), Value::I64(1));
        assert!(call("match", &[text("text"), text("text"), text("klingon")]).is_err());
    }

    #[test]
    fn arrow_search_scalar_uses_the_token_vector_charge_and_exact_score() {
        let document = "ΟΤΕ a b a b ".repeat(1030);
        let tokens = rtdb_text::Analyzer::Simple.analyze(&document);
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
