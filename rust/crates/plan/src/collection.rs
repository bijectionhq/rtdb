use rtdb_types::{Anomaly, ListKind, LogicalType as L, Result, ScalarType as S};

pub(crate) fn signature(name: &str, arguments: &[L]) -> Option<Result<L>> {
    if name == "_id_eq_any" {
        return Some((|| {
            let [id, array] = arguments else {
                return Err(Anomaly::incorrect("xtdb/invalid-function-arity", "_id_eq_any requires two arguments"));
            };
            for leg in array.legs() {
                match leg {
                    L::List { kind: ListKind::List | ListKind::FixedSizeList(_), element } =>
                        crate::validate_equality_types(id, &element)?,
                    L::Null | L::Nothing => (),
                    _ => return Err(Anomaly::incorrect("xtdb/sql-error", "Quantified comparison requires an array or subquery")),
                }
            }
            Ok(L::Scalar(S::Bool).maybe())
        })());
    }
    if !matches!(name, "array_upper" | "array_lower" | "array_length" | "trim_array" | "nth") { return None; }
    Some((|| {
        if arguments.len() != 2 {
            return Err(Anomaly::incorrect("xtdb/invalid-function-arity", format!("Wrong argument count for {name}: {}", arguments.len())));
        }
        let mut output = L::Nothing;
        for list in arguments[0].legs() {
            for count in arguments[1].legs() {
                let ty = if list == L::Null || count == L::Null { L::Null }
                    else if name == "nth" && matches!(count, L::Scalar(S::I8 | S::I16 | S::I32 | S::I64)) {
                        match &list {
                            L::List { kind: ListKind::List, element } => (**element).clone().maybe(),
                            L::List { kind: ListKind::FixedSizeList(_), .. } | L::Scalar(S::F16) => return Err(Anomaly::incorrect("xtdb.expression/function-type-mismatch", "nth not applicable to argument types")),
                            _ => L::Null,
                        }
                    }
                    else if matches!(&list, L::List { kind: ListKind::List, .. })
                        && (count == L::Scalar(S::I64) || name == "trim_array" && matches!(&count, L::Scalar(S::I8 | S::I16 | S::I32))) {
                        if name == "trim_array" { list.clone() } else { L::Scalar(S::I32) }
                    } else {
                        return Err(Anomaly::incorrect("xtdb.expression/function-type-mismatch", format!("{name} not applicable to argument types")));
                    };
                output = output.join(&ty);
            }
        }
        Ok(output)
    })())
}
