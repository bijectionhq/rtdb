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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CollectionFunction {
    ArrayUpper,
    ArrayLower,
    ArrayLength,
    TrimArray,
}

impl CollectionFunction {
    pub(crate) fn from_canonical(name: &str) -> Option<Self> {
        Some(match name {
            "array_upper" => Self::ArrayUpper,
            "array_lower" => Self::ArrayLower,
            "array_length" => Self::ArrayLength,
            "trim_array" => Self::TrimArray,
            _ => return None,
        })
    }

    pub(crate) fn canonical_name(self) -> &'static str {
        match self {
            Self::ArrayUpper => "array_upper",
            Self::ArrayLower => "array_lower",
            Self::ArrayLength => "array_length",
            Self::TrimArray => "trim_array",
        }
    }

    pub(crate) fn eval(self, args: &[Value], context: &EvalContext<'_>) -> Result<Value> {
        if args.iter().any(crate::compare::nullish) { return Ok(Value::Null); }
        let [Value::List(items), count] = args else { return Err(Anomaly::incorrect("xtdb/invalid-argument-types", "Array function requires a list and an integer")); };
        let count = integer(count).and_then(|count| i64::try_from(count).ok()).ok_or_else(|| Anomaly::incorrect("xtdb/invalid-argument-types", "Array function requires an integer"))?;
        if self != Self::TrimArray {
            dimension(self.canonical_name(), count)?;
            return Ok(Value::I32(if self == Self::ArrayLower { 1 } else { i32::try_from(items.len()).map_err(|_| overflow())? }));
        }
        let size = view_size(trim_size(items.len(), count)?)?;
        if size > items.len() { return Err(Anomaly::incorrect("xtdb/invalid-argument-types", "nth is not applicable to these collection values")); }
        if let Some(list) = items.slice_packed(0, size) { return Ok(Value::List(list)); }
        let items = items.iter().take(size);
        context.reserve(items.clone().try_fold(size.checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?, |bytes, item| bytes.checked_add(item.retained_bytes()?).ok_or_else(overflow))?)?;
        let mut result = Vec::with_capacity(size);
        for item in items { let mut item = item.into_owned();item.admit_box_date()?;result.push(item); }
        Ok(Value::list(result))
    }
}
