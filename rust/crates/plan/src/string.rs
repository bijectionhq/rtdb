use rtdb_types::{Anomaly, ListKind, LogicalType as L, Result, ScalarType as S};

pub(crate) fn signature(name: &str, arguments: &[L]) -> Option<Result<L>> {
    let arity = match name {
        "str" => 0..=usize::MAX,
        "concat" => 2..=usize::MAX,
        "format" => 1..=usize::MAX,
        "trim" | "trim_leading" | "trim_trailing" => 1..=2,
        "substring" => 2..=3,
        "overlay" => 4..=4,
        "replace" => 3..=3,
        "regexp_replace" => 3..=4,
        "position" | "octet_position" | "string_to_array" | "like" | "ilike" => 2..=2,
        "reverse" | "quote_ident" | "parse_ident" | "character_length" | "octet_length" | "default_overlay_length" => 1..=1,
        _ => return None,
    };
    Some((|| {
        if !arity.contains(&arguments.len()) {
            let message = match name {
                "concat" => "Arity error, concat requires at least two arguments".into(),
                "format" => "Arity error, format requires at least one argument (the format string)".into(),
                _ => format!("Wrong argument count for {name}: {}", arguments.len()),
            };
            return Err(Anomaly::incorrect(if matches!(name, "concat" | "format") { "xtdb.expression/arity-error" } else { "xtdb/invalid-function-arity" }, message));
        }
        if name == "str" { return Ok(L::Scalar(S::Utf8)); }
        if name == "format" {
            if arguments[0] != L::Null && !arguments[0].legs().contains(&L::Scalar(S::Utf8)) {
                return Err(Anomaly::incorrect("xtdb.expression/type-error", "First argument to `format` must be a string"));
            }
            return Ok(L::Scalar(S::Utf8));
        }
        if name == "concat" {
            let ty = L::join_all(arguments);
            let legs = ty.legs();
            let values = legs.iter().filter(|leg| !matches!(leg, L::Null | L::Nothing)).collect::<Vec<_>>();
            if values.len() > 1 { return Err(Anomaly::incorrect("xtdb.expression/type-error", "All arguments to `concat` must be of the same type.")); }
            if values.iter().any(|leg| !matches!(leg, L::Scalar(S::Utf8 | S::Binary))) { return Err(mismatch(name)); }
            return Ok(ty);
        }
        let mut outputs = Vec::new();
        combinations(arguments, 0, &mut Vec::new(), &mut |types| {
            if name == "string_to_array" {
                if !types.iter().all(|ty| matches!(ty, L::Null | L::Nothing | L::Scalar(S::Utf8))) { return Err(mismatch(name)); }
                outputs.push(if null(&types[0]) { L::Null } else { list() });
                return Ok(());
            }
            if types.iter().any(null) { outputs.push(L::Null); return Ok(()); }
            let bytes = matches!(types[0], L::Scalar(S::Utf8 | S::Binary));
            let utf8 = types[0] == L::Scalar(S::Utf8);
            let valid = match name {
                "like" => types.iter().all(|ty| *ty == L::Scalar(S::Utf8)) || types.iter().all(|ty| matches!(ty,L::Scalar(S::Binary | S::FixedSizeBinary(_)))),
                "ilike" => types.iter().all(|ty| *ty == L::Scalar(S::Utf8)),
                "trim" | "trim_leading" | "trim_trailing" => bytes && (types.len() == 1 || if utf8 { types[1] == L::Scalar(S::Utf8) } else { types[1] == L::Scalar(S::Binary) || numeric(&types[1]) }),
                "substring" => bytes && types[1..].iter().all(integer),
                "overlay" => bytes && types[0] == types[1] && types[2..].iter().all(integer),
                "octet_position" => bytes && types[0] == types[1],
                "position" | "replace" | "regexp_replace" => types.iter().all(|ty| *ty == L::Scalar(S::Utf8)),
                "octet_length" | "default_overlay_length" => bytes,
                _ => utf8,
            };
            if !valid { return Err(mismatch(name)); }
            outputs.push(match name {
                "like" | "ilike" => L::Scalar(S::Bool),
                "character_length" | "octet_length" | "position" | "octet_position" | "default_overlay_length" => L::Scalar(S::I32),
                "parse_ident" => list(),
                _ => types[0].clone(),
            });
            Ok(())
        })?;
        Ok(L::join_all(&outputs))
    })())
}

fn combinations(arguments: &[L], index: usize, chosen: &mut Vec<L>, visit: &mut impl FnMut(&[L]) -> Result<()>) -> Result<()> {
    let Some(argument) = arguments.get(index) else { return visit(chosen); };
    for leg in argument.legs() {
        chosen.push(leg);
        combinations(arguments, index + 1, chosen, visit)?;
        chosen.pop();
    }
    Ok(())
}
fn null(ty: &L) -> bool { matches!(ty, L::Null | L::Nothing) }
fn integer(ty: &L) -> bool { matches!(ty, L::Scalar(S::I8 | S::I16 | S::I32 | S::I64)) }
fn numeric(ty: &L) -> bool { integer(ty) || matches!(ty, L::Scalar(S::F16 | S::F32 | S::F64 | S::Decimal { .. })) }
fn list() -> L { L::List { kind: ListKind::List, element: Box::new(L::Scalar(S::Utf8)) } }
fn mismatch(name: &str) -> Anomaly { Anomaly::incorrect("xtdb.expression/function-type-mismatch", format!("{name} not applicable to argument types")) }
