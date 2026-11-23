use rtdb_types::{Anomaly, LogicalType as L, Result, ScalarType as S};

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
        let query = match (name, arguments) {
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
