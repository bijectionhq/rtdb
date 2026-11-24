use rtdb_text::Analyzer;
use rtdb_types::{Anomaly, LogicalType as L, Result, ScalarType as S, Value};

use crate::Expr;

/// Where a search function call names its analyzer: a string literal after its other arguments.
pub fn analyzer_index(name: &str, arity: usize) -> Option<usize> {
    match (name, arity) {
        ("match" | "search_score", 3) | ("search_terms" | "search_length", 2) => Some(arity - 1),
        ("search_score", 6) => Some(5),
        _ => None,
    }
}

/// The analyzer a search function call names, the simple one where it names none.
pub fn call_analyzer(name: &str, args: &[Expr]) -> Result<Analyzer> {
    match analyzer_index(&name.to_ascii_lowercase(), args.len()).map(|index| &args[index]) {
        None => Ok(Analyzer::Simple),
        Some(Expr::Literal(Value::Utf8(analyzer))) => parse_analyzer(analyzer),
        Some(_) => Err(Anomaly::incorrect("xtdb.expression/type-error",
            format!("The analyzer argument to `{name}` must be a string literal"))),
    }
}

pub fn parse_analyzer(name: &str) -> Result<Analyzer> {
    Analyzer::parse(name).ok_or_else(|| Anomaly::incorrect("rtdb.search/unknown-analyzer",
        format!("Unknown search analyzer '{name}'; expected one of {}", Analyzer::names().collect::<Vec<_>>().join(", "))))
}

pub(crate) fn signature(name: &str, arguments: &[L]) -> Option<Result<L>> {
    let result = match name {
        "match" => L::Scalar(S::Bool),
        "search_score" => L::Scalar(S::F64),
        "search_length" => L::Scalar(S::I64).maybe(),
        "search_corpus_fingerprint" => L::Scalar(S::Binary),
        "search_terms" => L::list(L::Scalar(S::Utf8)),
        _ => return None,
    };
    Some((|| {
        let arity = analyzer_index(name, arguments.len()).unwrap_or(arguments.len());
        if let Some(analyzer) = arguments.get(arity)
            && *analyzer != L::Null && !analyzer.legs().contains(&L::Scalar(S::Utf8))
        {
            return Err(Anomaly::incorrect("xtdb.expression/type-error", format!("The analyzer argument to `{name}` must be a string")));
        }
        let query = match (name, &arguments[..arity]) {
            ("search_length", [_]) | ("search_corpus_fingerprint", [_, _]) => None,
            ("search_terms", [query]) => Some(query),
            ("match" | "search_score", [_, query]) => Some(query),
            ("search_score", [_, query, _, _, _]) => Some(query),
            _ => {
                return Err(Anomaly::incorrect(
                    "xtdb/invalid-function-arity",
                    format!("Wrong argument count for {name}: {}", arguments.len()),
                ));
            }
        };
        if let Some(query) = query
            && *query != L::Null
            && !query.legs().contains(&L::Scalar(S::Utf8))
        {
            return Err(Anomaly::incorrect(
                "xtdb.expression/type-error",
                format!("The query argument to `{name}` must be a string"),
            ));
        }
        Ok(
            if arguments
                .iter()
                .any(|argument| argument.legs().contains(&L::Null))
            {
                result.maybe()
            } else {
                result
            },
        )
    })())
}
