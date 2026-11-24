use rtdb_types::StructFields;
use crate::compare::{null_eq_scoped,sql_eq_scoped};
use crate::{EvalContext, cast, compare_in_timezone};
use crate::compare::nullish;
use crate::numeric::{decimal, decimal_value, floating_value, inapplicable, integer, integer_value, number, overflow};
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, TimeUnit, Value};
use std::cmp::Ordering;

pub use rtdb_plan::function_type;

fn text(value: &Value) -> Result<&str> { match value { Value::Utf8(text) => Ok(text), _ => Err(inapplicable("text", &[value])) } }
fn count(value: usize) -> Result<Value> { Ok(Value::I32(i32::try_from(value).map_err(|_| overflow())?)) }

/// Runs a scalar call on the caller's future; SLEEP borrows the Tokio time driver without spawning a task.
/// Dropping this future cancels its timer; callers retain the evaluation allocation scope and own cancellation.
/// Temporal GENERATE_SERIES and RANGE_BINS yield while constructing results; cancellation drops that construction.
/// Remaining scalar calls use the synchronous implementation and return the same errors.
pub async fn scalar_call_async(name: &str, args: &[Value], context: &EvalContext<'_>) -> Result<Value> {
    if name.eq_ignore_ascii_case("generate_series") || name.eq_ignore_ascii_case("generate-series") {
        return crate::series::prepare(args,context)?.materialize_async(context).await;
    }
    if name.eq_ignore_ascii_case("range_bins") || name.eq_ignore_ascii_case("range-bins") {
        return match crate::temporal_functions::RangeBins::prepare(args,context)? { Some(bins) => bins.materialize_async(context).await, None => Ok(Value::Null) };
    }
    if name.eq_ignore_ascii_case("sleep") {
        let bytes = args.iter().try_fold(4096usize, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(overflow))?;
        context.reserve(bytes.checked_mul(4).ok_or_else(overflow)?)?;
        function_type(name, &args.iter().map(Value::logical_type).collect::<Vec<_>>())?;
        let mut remaining = crate::volatile::sleep_millis(&args[0])?;
        while remaining != 0 {
            let millis = remaining.min(86_400_000);
            tokio::time::sleep(std::time::Duration::from_millis(millis)).await;
            remaining -= millis;
        }
        return Ok(Value::Null);
    }
    scalar_call(name, args, context)
}

pub fn scalar_call(name: &str, args: &[Value], context: &EvalContext<'_>) -> Result<Value> {
    let bytes = args.iter().try_fold(256usize, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(overflow))?;
    context.reserve(bytes.checked_mul(16).ok_or_else(overflow)?)?;
    let name = name.to_lowercase().replace('-', "_");
    if name == "cosine_distance" {
        let [left, right] = args else {
            return Err(Anomaly::incorrect("xtdb/invalid-function-arity", "cosine_distance requires two arguments"));
        };
        return crate::vector::cosine_distance(left, right);
    }
    if name == "approximate_nearest" {
        let [column, _, _] = args else {
            return Err(Anomaly::incorrect("xtdb/invalid-function-arity", "approximate_nearest requires a column, a probe vector and a limit"));
        };
        return Ok(Value::Bool(!nullish(column)));
    }
    function_type(&name, &args.iter().map(Value::logical_type).collect::<Vec<_>>())?;
    match name.as_str() {
        "cosine_distance_f32" => return crate::vector::cosine_distance_f32(&args[0], &args[1]),
        "entity_sort_key" => return if nullish(&args[0]) { Ok(Value::Null) } else {
            Ok(Value::Binary(args[0].entity_id()?.handle_bytes().into()))
        },
        "rank_admitted_count" => {
            let nonnegative = |value| integer(value).and_then(|count| i64::try_from(count).ok()).filter(|count| *count >= 0);
            let (Some(count), Some(limit)) = (nonnegative(&args[0]), nonnegative(&args[1])) else {
                return Err(Anomaly::incorrect("rtdb/rank-bound", "Rank admission requires nonnegative integer counts"));
            };
            if count > limit {
                return Err(Anomaly::unsupported("rtdb/rank-bound", "The selected candidate set exceeds the ranking bound"));
            }
            return Ok(Value::I64(count));
        }
        _ => {}
    }
    if let Some(result) = crate::period::predicate(&name, args, context) { return result; }
    if let Some(result) = crate::search::call(&name, args, context) { return result; }
    if let Some(result) = crate::spatial::call(&name, args, context) { return result; }
    if let Some(result) = crate::string::call(&name, args, context) { return result; }
    if let Some(result) = crate::session_info::call(&name, args, context) { return result; }
    if let Some(result) = crate::collection::call(&name, args, context) { return result; }
    if let Some(result) = crate::bitwise::call(&name, args) { return result; }
    if let Some(result) = crate::temporal_functions::call(&name, args, context) { return result; }
    if let Some(result) = crate::interval_constructor::call(&name, args) { return result; }
    if name == "get_field" { return crate::field::get(&args[0],None,context); }
    if name == "generate_series" { return crate::series::prepare(args,context)?.materialize(context); }
    if matches!(name.as_str(), "compare" | "compare_nulls_first" | "compare_nulls_last") {
        if name == "compare" && args.iter().any(nullish) { return Ok(Value::Null); }
        return crate::ordered::scalar_compare(&args[0],&args[1],name == "compare_nulls_first",context.timezone).map(Value::I32);
    }
    if matches!(name.as_str(), "uri_scheme" | "uri_user_info" | "uri_host" | "uri_port" | "uri_path" | "uri_query" | "uri_fragment") {
        if nullish(&args[0]) { return Ok(Value::Null); }
        let Value::Uri(text) = &args[0] else { return Err(inapplicable(&name, &[&args[0]])); };
        let uri = rtdb_types::UriParts::parse(text)?;
        if name == "uri_port" { return Ok(uri.port().map(Value::I32).unwrap_or(Value::Null)); }
        let component = match name.as_str() {
            "uri_scheme" => uri.scheme().map(std::borrow::Cow::Borrowed),
            "uri_host" => uri.host().map(std::borrow::Cow::Borrowed),
            "uri_user_info" => uri.user_info(),
            "uri_path" => uri.path(),
            "uri_query" => uri.query(),
            _ => uri.fragment(),
        };
        return Ok(component.map(|text| Value::Utf8(text.into_owned())).unwrap_or(Value::Null));
    }
    match name.as_str() {
        "col_description" | "pg_indexam_has_property" => return Ok(Value::Null),
        "random" => return crate::volatile::random().map(Value::F64),
        "sleep" => return if crate::volatile::sleep_millis(&args[0])? == 0 { Ok(Value::Null) }
            else { Err(Anomaly::unsupported("rtdb.expr/async-required", "SLEEP requires asynchronous expression evaluation")) },
        "format_type" => {
            if nullish(&args[0]) { return Ok(Value::Null); }
            let oid = match &args[0] { Value::Oid(oid) => *oid, value => i32::try_from(integer(value).ok_or_else(|| inapplicable("format_type", &[value]))?).map_err(|_| overflow())? };
            let modifier = if nullish(&args[1]) { -1 } else { i32::try_from(integer(&args[1]).ok_or_else(|| inapplicable("format_type", &[&args[1]]))?).map_err(|_| overflow())? };
            return Ok(Value::Utf8(rtdb_types::PgType::format_oid(oid, modifier)));
        }
        "_patch" => {
            let Value::Struct(right) = &args[1] else { return Err(inapplicable("_patch", &[&args[0], &args[1]])); };
            let mut fields = match &args[0] {
                Value::Struct(left) => left.clone(),
                Value::Null | Value::Undefined => StructFields::new(),
                _ => return Err(inapplicable("_patch", &[&args[0], &args[1]])),
            };
            for (name, value) in right {
                if !nullish(value) || !fields.contains_key(name) { fields.insert(name.clone(), value.clone()); }
            }
            return Ok(Value::Struct(fields));
        }
        "coalesce" => return Ok(args.iter().find(|value| !nullish(value)).cloned().unwrap_or(Value::Null)),
        "nullif" => return Ok(if sql_eq_scoped(&args[0], &args[1], context.timezone, context.allocation)? == Some(true) { Value::Null } else { args[0].clone() }),
        "nil?" | "is_null" => return Ok(Value::Bool(nullish(&args[0]))),
        "true?" | "is_true" => return Ok(Value::Bool(args[0] == Value::Bool(true))),
        "false?" | "is_false" => return Ok(Value::Bool(args[0] == Value::Bool(false))),
        "distinct_from" => return if args.iter().any(nullish) { Ok(Value::Bool(!args.iter().all(nullish))) }
            else { Ok(crate::compare::sql_not_eq_scoped(&args[0], &args[1], context.timezone, context.allocation)?.map(Value::Bool).unwrap_or(Value::Null)) },
        "_meta_double" => return Ok(if let Some(value) = number(&args[0]) { Value::F64(value) }
            else if let Some(days) = crate::temporal::date_days(&args[0]) { Value::F64(days as f64 * 86_400.0) }
            else if let Value::Timestamp { value, unit, .. } | Value::Time { value, unit, .. } | Value::Duration { value, unit } = &args[0] { Value::F64(*value as f64 / unit.units_per_second() as f64) }
            else { Value::Null }),
        "boolean" => return Ok(Value::Bool(!nullish(&args[0]) && args[0] != Value::Bool(false))),
        "null_eq" => return Ok(Value::Bool(null_eq_scoped(&args[0], &args[1], context.timezone, context.allocation)?)),
        "===" => return Ok(Value::Bool(crate::compare::strict_eq_scoped(&args[0], &args[1], context.allocation)?)),
        "period" => {
            let timestamp = LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) });
            let endpoint = |value: &Value, unbounded| -> Result<i64> {
                if nullish(value) { return Ok(unbounded); }
                let Value::Timestamp { value, .. } = cast(value, &timestamp, context.timezone)? else { return Err(inapplicable("period", &[value])); };
                Ok(value)
            };
            let lower = endpoint(&args[0], i64::MIN)?;
            let upper = endpoint(&args[1], i64::MAX)?;
            if lower >= upper {
                return Err(Anomaly::incorrect("xtdb/invalid-period", "'from' must be earlier than 'to' when constructing a period")
                    .with_context("from", lower.into()).with_context("to", upper.into()));
            }
            return Ok(Value::TimestampTzRange { lower, upper });
        }
        "set" => {
            let mut items = Vec::new();
            for value in args {
                let mut found = false;
                for old in &items {
                    let equal = if nullish(old) || nullish(value) { nullish(old) && nullish(value) } else {
                        crate::compare::sql_eq_value(old,value,context.timezone, context.allocation)? == Some(true)
                    };
                    if equal { found = true; break; }
                }
                if !found { items.push(value.clone()); }
            }
            return Ok(Value::Set(items));
        }
        "snapshot_token" => {
            if let Some(token) = context.snapshot_token {
                if let Some(allocation) = context.allocation { allocation.reserve(token.len())?; }
                return Ok(Value::Utf8(token.to_owned()));
            }
            return Ok(Value::Null);
        }
        _ => (),
    }
    if args.iter().any(nullish) { return Ok(Value::Null); }
    if name == "abs" { if let Some(result) = crate::temporal::absolute(&args[0]) { return result; } }
    if let Some(Value::TimestampTzRange { lower, upper }) = args.first() {
        match name.as_str() {
            "lower" => return Ok(Value::Timestamp { value: *lower, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }),
            "upper" => return Ok(if *upper == i64::MAX { Value::Null } else { Value::Timestamp { value: *upper, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) } }),
            "lower_inf" => return Ok(Value::Bool(*lower == i64::MIN)),
            "upper_inf" => return Ok(Value::Bool(*upper == i64::MAX)),
            _ => (),
        }
    }
    match name.as_str() {
        "lower" => Ok(Value::Utf8(text(&args[0])?.to_lowercase())),
        "upper" => Ok(Value::Utf8(text(&args[0])?.to_uppercase())),
        "length" => match &args[0] { Value::Utf8(text) => count(text.chars().count()), Value::Binary(bytes) | Value::FixedSizeBinary(bytes) => count(bytes.len()), Value::List(items) => count(items.len()), value => Err(inapplicable(&name, &[value])) },
        "epoch_nanos_be" => {
            let Value::Timestamp { value, unit, timezone: Some(_) } = &args[0] else {
                return Err(inapplicable("epoch_nanos_be", &[&args[0]]));
            };
            let nanos = i128::from(*value) * (1_000_000_000 / i128::from(unit.units_per_second()));
            let nanos = u64::try_from(nanos).map_err(|_| overflow())?;
            Ok(Value::Binary(nanos.to_be_bytes().to_vec()))
        }
        "_iid" => Ok(Value::FixedSizeBinary(args[0].entity_id()?.iid().0.to_vec())),
        "keyword" => match &args[0] {
            Value::Utf8(text) => Ok(Value::Keyword(rtdb_types::Keyword::from_name(text))),
            value => Err(inapplicable(&name, &[value])),
        },
        "namespace" | "local_name" => {
            let Value::Keyword(keyword) = &args[0] else { return Err(inapplicable(&name, &[&args[0]])); };
            let parts = keyword.as_str().split_once('/');
            Ok(if name == "namespace" { parts.map(|(namespace, _)| Value::Utf8(namespace.into())).unwrap_or(Value::Null) }
                else { Value::Utf8(parts.map_or(keyword.as_str(), |(_, local)| local).into()) })
        }
        "cardinality" => match &args[0] { Value::List(_) | Value::Set(_) | Value::FixedSizeList(_) => count(args[0].list_view().unwrap().len()), _ => Err(inapplicable(&name, &[&args[0]])) },
        "nth" => {
            let index = integer(&args[1]).ok_or_else(|| inapplicable(&name, &[&args[1]]))?;
            let items = match &args[0] { Value::List(_) | Value::FixedSizeList(_) => args[0].list_view().unwrap(), _ => return Ok(Value::Null) };
            let mut value = usize::try_from(index).ok().and_then(|index| items.get(index)).map(std::borrow::Cow::into_owned).unwrap_or(Value::Null);
            value.admit_box_date()?;
            Ok(value)
        }
        "greatest" | "least" => {
            let Some(first) = args.first() else { return Ok(Value::Null); };
            let mut best = first.clone();
            for value in &args[1..] {
                let unordered = number(&best).is_some_and(f64::is_nan) || number(value).is_some_and(f64::is_nan);
                let ordering = if unordered { Ordering::Equal } else { compare_in_timezone(&best, value, false, context.timezone)? };
                if ordering != if name == "greatest" { Ordering::Greater } else { Ordering::Less } { best = value.clone(); }
            }
            Ok(best)
        }
        "like_regex" => crate::string::pattern::matches(args, context),
        "regexp_replace" => crate::string::pattern::replace(args, context),
        "abs" | "floor" | "ceil" | "ceiling" | "round" => {
            let scale = args.get(1).map(|value| integer(value).ok_or_else(|| inapplicable(&name, &[value]))).transpose()?.unwrap_or(0);
            if matches!(name.as_str(), "floor" | "ceil" | "ceiling") {
                let value = number(&args[0]).ok_or_else(|| inapplicable(&name, &[&args[0]]))?;
                return Ok(Value::F64(if name == "floor" { value.floor() } else { value.ceil() }));
            }
            if let Value::Decimal(value) = &args[0] {
                if name == "abs" { return decimal_value(decimal(value).abs(), value.precision, value.scale, value.bit_width); }
                return crate::decimal::rounded(value, i32::try_from(scale).map_err(|_| overflow())?, bigdecimal::RoundingMode::HalfUp).map(Value::Decimal);
            }
            let LogicalType::Scalar(ty) = args[0].logical_type() else { return Err(inapplicable(&name, &[&args[0]])); };
            if let Some(integer) = integer(&args[0]) {
                if name == "round" && args.len() == 2 {
                    let precision = match ty { ScalarType::I32 => 10, ScalarType::I64 => 19, _ => return Err(inapplicable(&name, &[&args[0]])) };
                    let value = crate::decimal::from_coefficient(integer.into(), precision, 0, 128)?;
                    return crate::decimal::rounded(&value, i32::try_from(scale).map_err(|_| overflow())?, bigdecimal::RoundingMode::HalfUp).map(Value::Decimal);
                }
                return integer_value(if name == "abs" { integer.checked_abs().ok_or_else(overflow)? } else { integer }, &ty);
            }
            let x = number(&args[0]).ok_or_else(|| inapplicable(&name, &[&args[0]]))?;
            if name == "round" {
                if !x.is_finite() { return Ok(Value::F64(x)); }
                let scale = i64::try_from(scale).map_err(|_| overflow())?;
                let factor = 10.0_f64.powf((scale as f64).abs());
                let value = if scale >= 0 { x * factor } else { x / factor };
                if !value.is_finite() { return Err(overflow()); }
                return Ok(Value::F64(if scale >= 0 { value.round() / factor } else { value.round() * factor }));
            }
            Ok(floating_value(x.abs(), &ty))
        }
        "mod" => crate::arithmetic(rtdb_plan::BinaryOp::Modulo, &args[0], &args[1]),
        name if is_float_math(name) => {
            let x = number(&args[0]).ok_or_else(|| inapplicable(name, &[&args[0]]))?;
            let y = args.get(1).map(|value| number(value).ok_or_else(|| inapplicable(name, &[value]))).transpose()?;
            float_math(name, x, y).map(Value::F64)
        }
        _ => Err(Anomaly::unsupported("xtdb/unknown-function", format!("Unknown scalar function: {name}"))),
    }
}

pub(crate) fn is_float_math(name: &str) -> bool {
    matches!(name, "sqrt" | "exp" | "ln" | "log" | "log10" | "power" | "sin" | "cos" | "tan"
        | "sinh" | "cosh" | "tanh" | "double" | "asin" | "acos" | "atan")
}

pub(crate) fn float_math(name: &str, x: f64, y: Option<f64>) -> Result<f64> {
    Ok(match name {
        "sqrt" => x.sqrt(), "exp" => x.exp(), "ln" => x.ln(),
        "log10" => x.log10(), "sinh" => x.sinh(), "cosh" => x.cosh(), "tanh" => x.tanh(), "double" => x,
        "log" => y.map_or_else(|| x.log10(), |y| y.log(x)), "power" => x.powf(y.ok_or_else(|| inapplicable(name, &[]))?),
        "sin" => x.sin(), "cos" => x.cos(), "tan" => x.tan(), "asin" => x.asin(), "acos" => x.acos(),
        "atan" => x.atan(), _ => return Err(inapplicable(name, &[])),
    })
}
