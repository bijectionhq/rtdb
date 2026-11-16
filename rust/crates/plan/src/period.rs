use rtdb_types::{Anomaly, ListKind, LogicalType as T, Result, ScalarType as S};

pub(crate) fn is_range(ty: &T) -> bool { matches!(ty, T::List { kind: ListKind::TimestampTzRange, .. }) }

pub(crate) fn predicate_type(name: &str, arguments: &[T]) -> Option<Result<T>> {
    let base = name.strip_suffix('?').unwrap_or(name);
    let unary = matches!(name, "lower_inf" | "upper_inf");
    if !unary && !matches!(base, "contains" | "strictly_contains" | "overlaps" | "strictly_overlaps" | "equals" | "precedes" | "strictly_precedes" | "immediately_precedes" | "succeeds" | "strictly_succeeds" | "immediately_succeeds" | "leads" | "strictly_leads" | "immediately_leads" | "lags" | "strictly_lags" | "immediately_lags") { return None; }
    Some((|| {
        let arity = if unary { 1 } else { 2 };
        if arguments.len() != arity { return Err(Anomaly::incorrect("xtdb/invalid-function-arity", format!("Wrong argument count for {name}: {}", arguments.len()))); }
        let right = if unary { vec![T::Scalar(S::Bool)] } else { arguments[1].legs().into_iter().collect() };
        let mut result = Vec::new();
        for left in arguments[0].legs() {
            for right in &right {
                if left == T::Null || *right == T::Null { result.push(T::Null); continue; }
                if left == T::Nothing || *right == T::Nothing { continue; }
                let point = name == "contains?" && matches!(right, T::Scalar(S::Timestamp { .. } | S::DateDay | S::DateMilli));
                if !is_range(&left) || (!unary && !is_range(right) && !point) {
                    return Err(Anomaly::incorrect("xtdb.expression/function-type-mismatch", format!("{name} not applicable to {left:?} and {right:?}")));
                }
                result.push(T::Scalar(S::Bool));
            }
        }
        Ok(T::join_all(&result))
    })())
}
