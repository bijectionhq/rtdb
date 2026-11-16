use crate::numeric::{exact_decimal, inapplicable, integer, number};
use rtdb_types::{Result, Value};
use std::cmp::Ordering;

pub(crate) fn nullish(value: &Value) -> bool { matches!(value, Value::Null | Value::Undefined) }

pub fn sql_eq(left: &Value, right: &Value) -> Result<Option<bool>> {
    if nullish(left) || nullish(right) { return Ok(None); }
    if let (Some(left), Some(right)) = (integer(left), integer(right)) { return Ok(Some(left == right)); }
    if let (Some(left), Some(right)) = (exact_decimal(left), exact_decimal(right)) {
        let (left, ls) = left.into_bigint_and_scale(); let (right, rs) = right.into_bigint_and_scale();
        return Ok(Some(crate::decimal::compare_parts(&left, ls, &right, rs) == Ordering::Equal));
    }
    if let (Some(left), Some(right)) = (number(left), number(right)) { return Ok(Some(left == right)); }
    match (left, right) {
        (Value::List(left), Value::List(right)) | (Value::FixedSizeList(left), Value::FixedSizeList(right)) => {
            if left.len() != right.len() { return Ok(Some(false)); }
            let mut unknown = false;
            for (a, b) in left.iter().zip(right) { match sql_eq(a, b)? { Some(false) => return Ok(Some(false)), None => unknown = true, _ => () } }
            Ok(if unknown { None } else { Some(true) })
        }
        (Value::Struct(left), Value::Struct(right)) => {
            if !left.keys().eq(right.keys()) { return Ok(Some(false)); }
            let mut unknown = false;
            for name in left.keys() {
                match sql_eq(left.get(name).unwrap_or(&Value::Null), right.get(name).unwrap_or(&Value::Null))? {
                    Some(false) => return Ok(Some(false)), None => unknown = true, _ => (),
                }
            }
            Ok(if unknown { None } else { Some(true) })
        }
        (Value::Timestamp { value: a, unit: au, timezone: az }, Value::Timestamp { value: b, unit: bu, timezone: bz }) if az.is_some() == bz.is_some() => {
            Ok(Some(i128::from(*a) * i128::from(bu.units_per_second()) == i128::from(*b) * i128::from(au.units_per_second())))
        }
        _ => Ok(Some(left == right)),
    }
}

pub fn null_eq(left: &Value, right: &Value) -> Result<bool> {
    if nullish(left) || nullish(right) { return Ok(nullish(left) && nullish(right)); }
    Ok(sql_eq(left, right)? == Some(true))
}

/// Null placement is explicit; the ordering of non-null values follows their runtime types.
pub fn compare(left: &Value, right: &Value, nulls_first: bool) -> Result<Ordering> {
    match (nullish(left), nullish(right)) {
        (true, true) => return Ok(Ordering::Equal),
        (true, false) => return Ok(if nulls_first { Ordering::Less } else { Ordering::Greater }),
        (false, true) => return Ok(if nulls_first { Ordering::Greater } else { Ordering::Less }),
        _ => (),
    }
    if let (Some(left), Some(right)) = (integer(left), integer(right)) { return Ok(left.cmp(&right)); }
    if let (Some(left), Some(right)) = (exact_decimal(left), exact_decimal(right)) {
        let (left, ls) = left.into_bigint_and_scale(); let (right, rs) = right.into_bigint_and_scale();
        return Ok(crate::decimal::compare_parts(&left, ls, &right, rs));
    }
    if let (Some(left), Some(right)) = (number(left), number(right)) {
        return Ok(if left.is_nan() { if right.is_nan() { Ordering::Equal } else { Ordering::Greater } }
            else if right.is_nan() { Ordering::Less } else { left.total_cmp(&right) });
    }
    match (left, right) {
        (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),
        (Value::Utf8(a), Value::Utf8(b)) | (Value::Uri(a), Value::Uri(b)) => Ok(a.as_bytes().cmp(b.as_bytes())),
        (Value::Keyword(a), Value::Keyword(b)) => Ok(a.as_str().as_bytes().cmp(b.as_str().as_bytes())),
        (Value::Binary(a), Value::Binary(b)) | (Value::FixedSizeBinary(a), Value::FixedSizeBinary(b)) => Ok(a.cmp(b)),
        (Value::Uuid(a), Value::Uuid(b)) => Ok(a.cmp(b)),
        (Value::DateDay(a), Value::DateDay(b)) => Ok(a.cmp(b)),
        (Value::DateMilli(a), Value::DateMilli(b)) => Ok(a.cmp(b)),
        (Value::Timestamp { value: a, unit: au, timezone: az }, Value::Timestamp { value: b, unit: bu, timezone: bz }) if az.is_some() == bz.is_some() => {
            Ok((i128::from(*a) * i128::from(bu.units_per_second())).cmp(&(i128::from(*b) * i128::from(au.units_per_second()))))
        }
        (Value::Duration { value: a, unit: au }, Value::Duration { value: b, unit: bu })
        | (Value::Time { value: a, unit: au, .. }, Value::Time { value: b, unit: bu, .. }) => {
            Ok((i128::from(*a) * i128::from(bu.units_per_second())).cmp(&(i128::from(*b) * i128::from(au.units_per_second()))))
        }
        (Value::List(a), Value::List(b)) | (Value::FixedSizeList(a), Value::FixedSizeList(b)) => {
            for (a, b) in a.iter().zip(b) { let order = compare(a, b, nulls_first)?; if order != Ordering::Equal { return Ok(order); } }
            Ok(a.len().cmp(&b.len()))
        }
        _ => Err(inapplicable("compare", &[left, right])),
    }
}
