use rtdb_types::{Anomaly, LogicalType as L, Result, ScalarType as S};

/// `match(text, query)` holds when the text has every term of the query; `search_score(text, query)` ranks it (fts.allium).
pub(crate) fn signature(name: &str, arguments: &[L]) -> Option<Result<L>> {
    let result = match name { "match" => L::Scalar(S::Bool), "search_score" => L::Scalar(S::F64), _ => return None };
    Some((|| {
        if arguments.len() != 2 { return Err(Anomaly::incorrect("xtdb/invalid-function-arity", format!("Wrong argument count for {name}: {}", arguments.len()))); }
        if arguments[1] != L::Null && !arguments[1].legs().contains(&L::Scalar(S::Utf8)) {
            return Err(Anomaly::incorrect("xtdb.expression/type-error", format!("The query argument to `{name}` must be a string")));
        }
        Ok(if arguments.iter().any(|argument| argument.legs().contains(&L::Null)) { result.maybe() } else { result })
    })())
}
