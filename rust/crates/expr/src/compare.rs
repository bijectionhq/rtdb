use crate::numeric::{exact_decimal, inapplicable, integer, number};
use crate::temporal::{date_days, timestamp_units, IntervalParts};
use rtdb_types::{Anomaly, Result, TimeUnit, Value};
use std::cmp::Ordering;

pub(crate) fn nullish(value: &Value) -> bool { matches!(value, Value::Null | Value::Undefined) }

fn contains_set(value: &Value) -> bool {
    match value {
        Value::Set(_) => true,
        Value::List(values) => values.iter().any(contains_set),
        Value::Struct(fields) => fields.values().any(contains_set),
        _ => false,
    }
}
/// Bounds temporary type metadata and decoded Transit objects while calling an equality API, including strict `===`.
/// This charge is separate from numeric comparison scratch.
pub fn equality_allocation_bound(left: &Value, right: &Value) -> Result<usize> {
    let metadata = if contains_set(left) && contains_set(right) {
        left.retained_bytes()?.checked_add(right.retained_bytes()?).and_then(|bytes| bytes.checked_mul(8)).ok_or_else(crate::numeric::overflow)?
    } else { 0 };
    let left = transit_allocation_bound(left)?;
    let right = transit_allocation_bound(right)?;
    metadata.checked_add(left).and_then(|bytes| bytes.checked_add(right)).ok_or_else(crate::numeric::overflow)
}
fn transit_allocation_bound(value: &Value) -> Result<usize> {
    let add = |total: usize, value: &Value| total.checked_add(transit_allocation_bound(value)?).ok_or_else(crate::numeric::overflow);
    match value {
        Value::Transit(_) => crate::text_allocation_bound(value),
        Value::List(values) | Value::Set(values) | Value::FixedSizeList(values) => values.iter().try_fold(0, add),
        Value::Struct(fields) => fields.values().try_fold(0, add),
        Value::Map { entries, .. } => entries.iter().try_fold(0, |bytes, (key, value)| add(add(bytes, key)?, value)),
        _ => Ok(0),
    }
}
fn validate_equality_values(left: &Value, right: &Value) -> Result<()> {
    fn shape(value: &Value) -> rtdb_types::LogicalType {
        use rtdb_types::{ListKind, LogicalType as T};
        match value {
            Value::Set(_) => T::List { kind: ListKind::Set, element: Box::new(T::Nothing) },
            Value::List(values) => T::list(match values.as_slice() { [] => T::Nothing,[value] => shape(value),_ => T::join_all(&values.iter().map(shape).collect::<Vec<_>>()) }),
            Value::Struct(fields) => T::Struct(fields.iter().map(|(name, value)| (name.clone(), shape(value))).collect()),
            _ => T::Null,
        }
    }
    if contains_set(left) && contains_set(right) { rtdb_plan::validate_equality_types(&shape(left), &shape(right))?; }
    Ok(())
}

pub fn sql_eq(left: &Value, right: &Value) -> Result<Option<bool>> {
    sql_eq_in_timezone(left, right, "UTC")
}

/// Uses the session timezone when comparing a date or local timestamp with a zoned timestamp.
/// Temporal units are converted to the finer unit with checked signed 64-bit arithmetic.
/// Declared collection shapes reject nested Set/Set equality before length or value comparisons.
/// Callers own the scratch charge returned by `equality_allocation_bound`.
pub fn sql_eq_in_timezone(left: &Value, right: &Value, timezone: &str) -> Result<Option<bool>> {
    validate_equality_values(left, right)?;
    sql_eq_value(left, right, timezone)
}

pub(crate) fn sql_eq_value(left: &Value, right: &Value, timezone: &str) -> Result<Option<bool>> {
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
            for (a, b) in left.iter().zip(right) { match sql_eq_value(a, b, timezone)? { Some(false) => return Ok(Some(false)), None => unknown = true, _ => () } }
            Ok(if unknown { None } else { Some(true) })
        }
        (Value::Struct(left), Value::Struct(right)) => {
            if !left.keys().eq(right.keys()) { return Ok(Some(false)); }
            let mut unknown = false;
            for name in left.keys() {
                match sql_eq_value(left.get(name).unwrap_or(&Value::Null), right.get(name).unwrap_or(&Value::Null), timezone)? {
                    Some(false) => return Ok(Some(false)), None => unknown = true, _ => (),
                }
            }
            Ok(if unknown { None } else { Some(true) })
        }
        _ if date_time(left) && date_time(right) => Ok(Some(compare_in_timezone(left, right, false, timezone)? == Ordering::Equal)),
        (Value::Time { .. }, Value::Time { .. }) | (Value::Duration { .. }, Value::Duration { .. }) => Ok(Some(compare_in_timezone(left, right, false, timezone)? == Ordering::Equal)),
        _ => Ok(Some(emitted_eq(left, right)?)),
    }
}

pub fn null_eq(left: &Value, right: &Value) -> Result<bool> {
    null_eq_in_timezone(left, right, "UTC")
}

/// Treats two outer NULLs as equal; nested unknown equality counts as unequal.
/// Callers own the scratch charge returned by `equality_allocation_bound`.
pub fn null_eq_in_timezone(left: &Value, right: &Value, timezone: &str) -> Result<bool> {
    if nullish(left) || nullish(right) { return Ok(nullish(left) && nullish(right)); }
    Ok(sql_eq_in_timezone(left, right, timezone)? == Some(true))
}

/// Numeric and composite inequality negate SQL equality; temporal scalars compare their read values without unit coercion.
/// NULL produces unknown, including nested NULLs in equal-shaped lists or structs.
/// Callers own the scratch charge returned by `equality_allocation_bound`.
pub fn sql_not_eq_in_timezone(left: &Value, right: &Value, timezone: &str) -> Result<Option<bool>> {
    validate_equality_values(left, right)?;
    if nullish(left) || nullish(right) { return Ok(None); }
    if (number(left).is_some() && number(right).is_some()) || matches!((left, right),
        (Value::List(_), Value::List(_)) | (Value::FixedSizeList(_), Value::FixedSizeList(_)) | (Value::Struct(_), Value::Struct(_))) {
        return Ok(sql_eq_value(left, right, timezone)?.map(|equal| !equal));
    }
    Ok(Some(!emitted_eq(left, right)?))
}

fn emitted_integer(value: &Value) -> Option<i128> {
    integer(value).or_else(|| date_days(value).map(i128::from)).or_else(|| match value {
        Value::Timestamp { value, .. } | Value::Time { value, .. } | Value::Duration { value, .. } => Some(i128::from(*value)),
        _ => None,
    })
}

fn emitted_eq(left: &Value, right: &Value) -> Result<bool> {
    if let (Value::Transit(left),Value::Transit(right)) = (left,right) {
        let left = crate::transit_object::Object::decode(left)?;
        let right = crate::transit_object::Object::decode(right)?;
        return left.equivalent(&right);
    }
    if let (Some(left), Some(right)) = (emitted_integer(left), emitted_integer(right)) { return Ok(left == right); }
    if let (Some(left), Some(right)) = (IntervalParts::of(left), IntervalParts::of(right)) {
        return Ok(left.months == right.months && left.days == right.days && left.nanos == right.nanos);
    }
    if let (Value::Uri(left),Value::Uri(right)) = (left,right) {
        return Ok(rtdb_types::UriParts::parse(left)?.equivalent(&rtdb_types::UriParts::parse(right)?));
    }
    Ok(left == right)
}

/// Preserves numeric categories, the distinction between dates and timestamps, timestamp zone labels and interval units.
/// Nested NULLs compare equal; same-category temporal units still undergo checked conversion.
/// Callers own the scratch charge returned by `equality_allocation_bound`.
pub fn strict_eq(left: &Value, right: &Value) -> Result<bool> {
    if nullish(left) || nullish(right) { return Ok(nullish(left) && nullish(right)); }
    if date_time(left) && date_time(right) {
        return match (left, right) {
            (Value::Timestamp { timezone: a, .. }, Value::Timestamp { timezone: b, .. }) if a == b => Ok(compare(left, right, false)? == Ordering::Equal),
            _ if date_days(left).is_some() && date_days(right).is_some() => Ok(compare(left, right, false)? == Ordering::Equal),
            _ => Ok(false),
        };
    }
    if IntervalParts::of(left).is_some() && IntervalParts::of(right).is_some() {
        return Ok(std::mem::discriminant(left) == std::mem::discriminant(right) && emitted_eq(left, right)?);
    }
    let category = |value: &Value| match value { Value::Decimal(_) => 2, Value::F16(_) | Value::F32(_) | Value::F64(_) => 1, _ => 0 };
    if number(left).is_some() && number(right).is_some() {
        return Ok(category(left) == category(right) && sql_eq(left, right)? == Some(true));
    }
    match (left, right) {
        (Value::List(a), Value::List(b)) | (Value::FixedSizeList(a), Value::FixedSizeList(b)) => {
            if a.len() != b.len() { return Ok(false); }
            for (a, b) in a.iter().zip(b) { if !strict_eq(a, b)? { return Ok(false); } }
            Ok(true)
        }
        (Value::Struct(a), Value::Struct(b)) => {
            if !a.keys().eq(b.keys()) { return Ok(false); }
            for (a, b) in a.values().zip(b.values()) { if !strict_eq(a, b)? { return Ok(false); } }
            Ok(true)
        }
        (Value::Time { .. }, Value::Time { .. }) | (Value::Duration { .. }, Value::Duration { .. }) => Ok(compare(left, right, false)? == Ordering::Equal),
        _ => emitted_eq(left, right),
    }
}

fn date_time(value: &Value) -> bool { value.date_days().is_some() || matches!(value, Value::Timestamp { .. }) }
fn unit(value: &Value) -> TimeUnit {
    match value { Value::Timestamp { unit, .. } | Value::Time { unit, .. } | Value::Duration { unit, .. } => *unit, _ => TimeUnit::Microsecond }
}
fn temporal_order(left: &Value, right: &Value, timezone: &str) -> Result<Ordering> {
    if let (Some(left), Some(right)) = (date_days(left), date_days(right)) { return Ok(left.cmp(&right)); }
    let target = if unit(left).units_per_second() >= unit(right).units_per_second() { unit(left) } else { unit(right) };
    let zoned = |value: &Value| matches!(value, Value::Timestamp { timezone: Some(_), .. });
    let localize = zoned(left) != zoned(right);
    Ok(timestamp_units(left, target, localize, timezone)?.cmp(&timestamp_units(right, target, localize, timezone)?))
}

fn interval_order(left: &Value, right: &Value, a: IntervalParts, b: IntervalParts) -> Result<Ordering> {
    let name = |value: &Value| match value {
        Value::IntervalYearMonth(_) => "year-month", Value::IntervalDayTime { .. } => "day-time",
        Value::IntervalMonthDayMicro { .. } => "month-day-micro", _ => "month-day-nano",
    };
    if name(left) != name(right) {
        return Err(Anomaly::incorrect("xtdb.expression/cannot-compare-different-unit-intervals", "Cannot compare intervals with different units")
            .with_context("x-unit", name(left).into()).with_context("y-unit", name(right).into()));
    }
    if matches!(left, Value::IntervalYearMonth(_)) { return Ok(a.months.cmp(&b.months)); }
    if matches!(left, Value::IntervalDayTime { .. }) { return Err(inapplicable("compare", &[left, right])); }
    if a.months > 0 || b.months > 0 {
        return Err(Anomaly::incorrect("xtdb.expression/cannot-compare-mdn-interval-with-months", "Cannot compare month-day-micro/nano intervals when month component is non-zero."));
    }
    let nanos = |value: IntervalParts| i64::from(value.days).wrapping_mul(86_400_000_000_000).wrapping_add(value.nanos);
    Ok(nanos(a).cmp(&nanos(b)))
}

/// Null placement is explicit; the ordering of non-null values follows their runtime types.
pub fn compare(left: &Value, right: &Value, nulls_first: bool) -> Result<Ordering> {
    compare_in_timezone(left, right, nulls_first, "UTC")
}

/// Orders non-null temporal values using the session timezone and checked conversion to the finer unit.
/// Interval ordering requires matching units and rejects positive month components outside year-month intervals.
/// Month-day interval day/time totals wrap as signed 64-bit nanoseconds.
/// Two nonnull URI objects fail at the pinned byte-buffer comparator boundary.
pub fn compare_in_timezone(left: &Value, right: &Value, nulls_first: bool, timezone: &str) -> Result<Ordering> {
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
        (Value::Utf8(a), Value::Utf8(b)) => Ok(a.as_bytes().cmp(b.as_bytes())),
        (Value::Uri(_), Value::Uri(_)) => Err(Anomaly::incorrect("xtdb.expression/uri-comparator", "URI values cannot be passed to the reference byte-buffer comparator")),
        (Value::Keyword(a), Value::Keyword(b)) => Ok(a.as_str().as_bytes().cmp(b.as_str().as_bytes())),
        (Value::Binary(a), Value::Binary(b)) | (Value::FixedSizeBinary(a), Value::FixedSizeBinary(b)) => Ok(a.cmp(b)),
        (Value::Uuid(a), Value::Uuid(b)) => Ok(a.cmp(b)),
        _ if date_time(left) && date_time(right) => temporal_order(left, right, timezone),
        (Value::Duration { .. }, Value::Duration { .. }) | (Value::Time { .. }, Value::Time { .. }) => temporal_order(left, right, timezone),
        (Value::List(a), Value::List(b)) | (Value::FixedSizeList(a), Value::FixedSizeList(b)) => {
            for (a, b) in a.iter().zip(b) { let order = compare_in_timezone(a, b, nulls_first, timezone)?; if order != Ordering::Equal { return Ok(order); } }
            Ok(a.len().cmp(&b.len()))
        }
        _ => match (IntervalParts::of(left), IntervalParts::of(right)) {
            (Some(a), Some(b)) => interval_order(left, right, a, b),
            _ => Err(inapplicable("compare", &[left, right])),
        },
    }
}
