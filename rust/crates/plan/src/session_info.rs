use rtdb_types::{Anomaly, ListKind, LogicalType as L, Result, ScalarType as S};

pub(crate) fn signature(name: &str, arguments: &[L]) -> Option<Result<L>> {
    let (argument, output) = match name {
        "current_user" | "current_database" | "current_schema" | "version" | "xt.version" => (None, L::Scalar(S::Utf8)),
        "current_schemas" => (Some(S::Bool), L::List { kind: ListKind::List, element: Box::new(L::Scalar(S::Utf8)) }),
        "current_setting" => (Some(S::Utf8), L::Scalar(S::Utf8)),
        _ => return None,
    };
    Some((|| {
        if arguments.len() != usize::from(argument.is_some()) {
            return Err(Anomaly::incorrect("xtdb/invalid-function-arity", format!("Wrong argument count for {name}: {}", arguments.len())));
        }
        let Some(expected) = argument else { return Ok(output); };
        let mut result = L::Nothing;
        for leg in arguments[0].legs() {
            let value = match leg {
                L::Null | L::Nothing => L::Null,
                L::Scalar(actual) if actual == expected => output.clone(),
                _ => return Err(Anomaly::incorrect("xtdb.expression/function-type-mismatch", format!("{name} not applicable to argument type"))),
            };
            result = result.join(&value);
        }
        Ok(result)
    })())
}
