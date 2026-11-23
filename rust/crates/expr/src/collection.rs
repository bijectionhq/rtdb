use rtdb_types::{Anomaly, Result, Value};
use crate::{EvalContext, numeric::{integer, overflow}};

pub(crate) fn dimension(name: &str, dimension: i64) -> Result<()> {
    if dimension == 1 { return Ok(()); }
    Err(Anomaly::unsupported("xtdb.expression/array-dimension-error", format!("Unsupported: {} for dimension != 1", name.to_uppercase()))
        .with_context("dim", dimension.into()))
}

pub(crate) fn trim_size(size: usize, count: i64) -> Result<i64> {
    let size = i64::try_from(size).map_err(|_| overflow())?.wrapping_sub(count);
    if size < 0 {
        return Err(Anomaly::incorrect("xtdb.expression/array-element-error", "Data exception - array element error.").with_context("nlen", size.into()));
    }
    Ok(size)
}

pub(crate) fn view_size(size: i64) -> Result<usize> {
    i32::try_from(size).map(|size| size as usize)
        .map_err(|_| Anomaly::incorrect("xtdb.expression/overflow-error", "data exception - overflow error"))
}

pub(crate) fn call(name: &str, args: &[Value], context: &EvalContext<'_>) -> Option<Result<Value>> {
    if !matches!(name, "array_upper" | "array_lower" | "array_length" | "trim_array") { return None; }
    Some((|| {
        if args.iter().any(crate::compare::nullish) { return Ok(Value::Null); }
        let [Value::List(items), count] = args else { return Err(Anomaly::incorrect("xtdb/invalid-argument-types", "Array function requires a list and an integer")); };
        let count = integer(count).and_then(|count| i64::try_from(count).ok()).ok_or_else(|| Anomaly::incorrect("xtdb/invalid-argument-types", "Array function requires an integer"))?;
        if name != "trim_array" {
            dimension(name, count)?;
            return Ok(Value::I32(if name == "array_lower" { 1 } else { i32::try_from(items.len()).map_err(|_| overflow())? }));
        }
        let size = view_size(trim_size(items.len(), count)?)?;
        if size > items.len() { return Err(Anomaly::incorrect("xtdb/invalid-argument-types", "nth is not applicable to these collection values")); }
        if let Some(list) = items.slice_packed(0, size) { return Ok(Value::List(list)); }
        let items = items.iter().take(size);
        context.reserve(items.clone().try_fold(size.checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?, |bytes, item| bytes.checked_add(item.retained_bytes()?).ok_or_else(overflow))?)?;
        let mut result = Vec::with_capacity(size);
        for item in items { let mut item = item.into_owned();item.admit_box_date()?;result.push(item); }
        Ok(Value::list(result))
    })())
}
