use crate::numeric::{exact_decimal, inapplicable, integer, number};
use crate::temporal::{date_days, timestamp_units, IntervalParts};
use rtdb_types::{Anomaly, ListView, NumericCell, NumericSpan, Result, TimeUnit, Value};
use std::cmp::Ordering;
use crate::EvaluationScope;

pub(crate) fn nullish(value: &Value) -> bool { matches!(value, Value::Null | Value::Undefined) }

fn contains_set(value: &Value) -> bool {
    match value {
        Value::Set(_) => true,
        Value::List(values) => matches!(values.view(), rtdb_types::ListView::Values(values) if values.iter().any(contains_set)),
        Value::Struct(fields) => fields.values().any(contains_set),
        _ => false,
    }
}
/// Bounds temporary equality type metadata.
/// Equality APIs acquire this charge from their scope; numeric comparison scratch remains caller-owned.
pub fn equality_allocation_bound(left: &Value, right: &Value) -> Result<usize> {
    if contains_set(left) && contains_set(right) {
        left.retained_bytes()?.checked_add(right.retained_bytes()?).and_then(|bytes| bytes.checked_mul(8)).ok_or_else(crate::numeric::overflow)
    } else { Ok(0) }
}
fn admit_equality(left: &Value, right: &Value, allocation: Option<&EvaluationScope>) -> Result<()> {
    if let Some(scope) = allocation {
        let bytes = equality_allocation_bound(left, right)?;
        if bytes != 0 { scope.reserve(bytes)?; }
    }
    validate_equality_values(left, right)
}
fn validate_equality_values(left: &Value, right: &Value) -> Result<()> {
    fn shape(value: &Value) -> rtdb_types::LogicalType {
        use rtdb_types::{ListKind, LogicalType as T};
        match value {
            Value::Set(_) => T::List { kind: ListKind::Set, element: Box::new(T::Nothing) },
            Value::List(values) => T::list(match values.view() {
                rtdb_types::ListView::Values([]) => T::Nothing,
                rtdb_types::ListView::Values([value]) => shape(value),
                rtdb_types::ListView::Values(values) => T::join_all(&values.iter().map(shape).collect::<Vec<_>>()),
                view if view.is_empty() => T::Nothing,
                _ => T::Null,
            }),
            Value::Struct(fields) => T::Struct(fields.iter().map(|(name, value)| (name.clone(), shape(value))).collect()),
            _ => T::Null,
        }
    }
    if contains_set(left) && contains_set(right) { rtdb_plan::validate_equality_types(&shape(left), &shape(right))?; }
    Ok(())
}

pub(crate) enum NumericListInput<'a> {
    Stored(ListView<'a>),
    Opened(NumericSpan<'a>),
}

impl<'a> NumericListInput<'a> {
    pub(crate) fn stored(view: ListView<'a>) -> Option<Self> {
        view.is_packed_numeric().then_some(Self::Stored(view))
    }

    fn len(&self) -> usize {
        match self { Self::Stored(view) => view.len(), Self::Opened(span) => span.len() }
    }

    fn get(&self, index: usize) -> NumericCell {
        match self {
            Self::Stored(view) => view.numeric_component(index),
            Self::Opened(span) => span.get(index).expect("index in list").signed(),
        }
    }

    pub(crate) fn equal(&self, other: &Self, strict: bool) -> Option<bool> {
        if self.len() != other.len() { return Some(false); }
        let mut unknown = false;
        for index in 0..self.len() {
            let (a, b) = (self.get(index), other.get(index));
            if a == NumericCell::Null || b == NumericCell::Null {
                if strict && a != b { return Some(false); }
                unknown |= !strict;
            } else if numeric_category(a) != numeric_category(b) {
                if strict || sql_eq_value(&a.into_value(), &b.into_value(), "UTC", None).expect("numeric equality") != Some(true) {
                    return Some(false);
                }
            } else if !numeric_equal(a, b) {
                return Some(false);
            }
        }
        if unknown { None } else { Some(true) }
    }

    pub(crate) fn order(&self, other: &Self, nulls_first: bool) -> Ordering {
        let null_order = if nulls_first { Ordering::Less } else { Ordering::Greater };
        for index in 0..self.len().min(other.len()) {
            let order = match (self.get(index), other.get(index)) {
                (NumericCell::Null, NumericCell::Null) => Ordering::Equal,
                (NumericCell::Null, _) => null_order,
                (_, NumericCell::Null) => null_order.reverse(),
                (a, b) if numeric_category(a) != numeric_category(b) => {
                    compare(&a.into_value(), &b.into_value(), nulls_first).expect("numeric ordering")
                }
                (a, b) => exact_number_order(a, b).unwrap_or_else(|| float_order(a.as_f64().unwrap(), b.as_f64().unwrap())),
            };
            if order != Ordering::Equal { return order; }
        }
        self.len().cmp(&other.len())
    }
}

fn numeric_category(value: NumericCell) -> u8 {
    match value {
        NumericCell::Decimal(..) => 2,
        NumericCell::F16(_) | NumericCell::F32(_) | NumericCell::F64(_) => 1,
        _ => 0,
    }
}

fn exact_number_order(left: NumericCell, right: NumericCell) -> Option<Ordering> {
    use num_bigint::BigInt;
    if let (Some(a), Some(b)) = (left.integer(), right.integer()) { return Some(a.cmp(&b)); }
    if let (NumericCell::Decimal(a, ap), NumericCell::Decimal(b, bp)) = (left, right)
        && ap.scale == bp.scale
    {
        return Some((a[0] as i8).cmp(&(b[0] as i8)).then_with(|| a[1..].cmp(&b[1..])));
    }
    let parts = |value: NumericCell| match value {
        NumericCell::Decimal(bytes, params) => Some((BigInt::from_signed_bytes_be(&bytes), i64::from(params.scale))),
        value => value.integer().map(|integer| (BigInt::from(integer), 0)),
    };
    let ((a, ls), (b, rs)) = (parts(left)?, parts(right)?);
    Some(crate::decimal::compare_parts(&a, ls, &b, rs))
}

fn numeric_equal(left: NumericCell, right: NumericCell) -> bool {
    exact_number_order(left, right).map_or_else(|| left.as_f64() == right.as_f64(), |order| order == Ordering::Equal)
}

/// SQL equality in UTC; the scope retains admitted metadata and reached Transit allocations.
/// NULL yields unknown; callers separately own numeric comparison scratch.
pub fn sql_eq(left: &Value, right: &Value, allocation: &EvaluationScope) -> Result<Option<bool>> {
    sql_eq_in_timezone(left, right, "UTC", allocation)
}

/// Uses the session timezone when comparing a date or local timestamp with a zoned timestamp.
/// Temporal units are converted to the finer unit with checked signed 64-bit arithmetic.
/// Declared collection shapes reject nested Set/Set equality before length or value comparisons.
/// The scope retains type metadata and reached Transit intermediates; callers separately own numeric scratch.
pub fn sql_eq_in_timezone(left: &Value, right: &Value, timezone: &str, allocation: &EvaluationScope) -> Result<Option<bool>> {
    sql_eq_scoped(left, right, timezone, Some(allocation))
}

pub(crate) fn sql_eq_scoped(left: &Value, right: &Value, timezone: &str, allocation: Option<&EvaluationScope>) -> Result<Option<bool>> {
    admit_equality(left, right, allocation)?;
    sql_eq_value(left, right, timezone, allocation)
}

pub(crate) fn sql_eq_value(left: &Value, right: &Value, timezone: &str, allocation: Option<&EvaluationScope>) -> Result<Option<bool>> {
    if nullish(left) || nullish(right) { return Ok(None); }
    if matches!(left, Value::Transit(_)) || matches!(right, Value::Transit(_)) { return emitted_eq(left, right, allocation).map(Some); }
    if let (Some(left), Some(right)) = (integer(left), integer(right)) { return Ok(Some(left == right)); }
    if let (Some(left), Some(right)) = (exact_decimal(left), exact_decimal(right)) {
        let (left, ls) = left.into_bigint_and_scale(); let (right, rs) = right.into_bigint_and_scale();
        return Ok(Some(crate::decimal::compare_parts(&left, ls, &right, rs) == Ordering::Equal));
    }
    if let (Some(left), Some(right)) = (number(left), number(right)) { return Ok(Some(left == right)); }
    match (left, right) {
        (Value::List(_), Value::List(_)) | (Value::FixedSizeList(_), Value::FixedSizeList(_)) => {
            let (left, right) = (left.list_view().unwrap(), right.list_view().unwrap());
            if left.len() != right.len() { return Ok(Some(false)); }
            let mut unknown = false;
            if left.is_float() && right.is_float() {
                for index in 0..left.len() {
                    match (left.float_component(index), right.float_component(index)) {
                        (Some(a), Some(b)) if a == b => {}
                        (Some(_), Some(_)) => return Ok(Some(false)),
                        _ => unknown = true,
                    }
                }
                return Ok(if unknown { None } else { Some(true) });
            }
            if let (Some(left), Some(right)) = (NumericListInput::stored(left), NumericListInput::stored(right)) {
                return Ok(left.equal(&right, false));
            }
            for (a, b) in left.iter().zip(right.iter()) { match sql_eq_value(a.as_ref(), b.as_ref(), timezone, allocation)? { Some(false) => return Ok(Some(false)), None => unknown = true, _ => () } }
            Ok(if unknown { None } else { Some(true) })
        }
        (Value::Struct(left), Value::Struct(right)) => {
            if !left.keys().eq(right.keys()) { return Ok(Some(false)); }
            let mut unknown = false;
            for name in left.keys() {
                match sql_eq_value(left.get(name).unwrap_or(&Value::Null), right.get(name).unwrap_or(&Value::Null), timezone, allocation)? {
                    Some(false) => return Ok(Some(false)), None => unknown = true, _ => (),
                }
            }
            Ok(if unknown { None } else { Some(true) })
        }
        _ if date_time(left) && date_time(right) => Ok(Some(compare_in_timezone(left, right, false, timezone)? == Ordering::Equal)),
        (Value::Time { .. }, Value::Time { .. }) | (Value::Duration { .. }, Value::Duration { .. }) => Ok(Some(compare_in_timezone(left, right, false, timezone)? == Ordering::Equal)),
        _ => Ok(Some(emitted_eq(left, right, allocation)?)),
    }
}

/// NULL-aware equality in UTC with caller-owned allocation admission and numeric scratch.
pub fn null_eq(left: &Value, right: &Value, allocation: &EvaluationScope) -> Result<bool> {
    null_eq_in_timezone(left, right, "UTC", allocation)
}

/// Treats two outer NULLs as equal; nested unknown equality counts as unequal.
/// The scope retains type metadata and reached Transit intermediates; callers separately own numeric scratch.
pub fn null_eq_in_timezone(left: &Value, right: &Value, timezone: &str, allocation: &EvaluationScope) -> Result<bool> {
    null_eq_scoped(left, right, timezone, Some(allocation))
}

pub(crate) fn null_eq_scoped(left: &Value, right: &Value, timezone: &str, allocation: Option<&EvaluationScope>) -> Result<bool> {
    if nullish(left) || nullish(right) { return Ok(nullish(left) && nullish(right)); }
    Ok(sql_eq_scoped(left, right, timezone, allocation)? == Some(true))
}

/// Negates SQL equality in the session timezone and preserves unknown results.
pub fn sql_not_eq_in_timezone(left: &Value, right: &Value, timezone: &str, allocation: &EvaluationScope) -> Result<Option<bool>> {
    sql_not_eq_scoped(left, right, timezone, Some(allocation))
}

pub(crate) fn sql_not_eq_scoped(left: &Value, right: &Value, timezone: &str, allocation: Option<&EvaluationScope>) -> Result<Option<bool>> {
    Ok(sql_eq_scoped(left, right, timezone, allocation)?.map(|equal| !equal))
}

fn postgres_reference(value: &Value) -> Option<i32> {
    match value {
        Value::Oid(oid) | Value::RegClass(oid) | Value::RegProc(oid) => Some(*oid),
        _ => None,
    }
}

fn emitted_integer(value: &Value) -> Option<i128> {
    integer(value).or_else(|| postgres_reference(value).map(i128::from)).or_else(|| date_days(value).map(i128::from)).or_else(|| match value {
        Value::Timestamp { value, .. } | Value::Time { value, .. } | Value::Duration { value, .. } => Some(i128::from(*value)),
        _ => None,
    })
}

pub(crate) fn emitted_byte_buffer(value: &Value) -> Option<&[u8]> {
    match value {
        Value::Utf8(text) => Some(text.as_bytes()),
        Value::Keyword(keyword) => Some(keyword.as_str().as_bytes()),
        Value::Binary(bytes) | Value::FixedSizeBinary(bytes) => Some(bytes),
        Value::Uuid(bytes) => Some(bytes),
        _ => None,
    }
}

fn emitted_eq(left: &Value, right: &Value, allocation: Option<&EvaluationScope>) -> Result<bool> {
    if let Value::Transit(left) = left {
        let left = crate::transit_object::Decoded::read(left, crate::transit_object::scope(allocation)?)?;
        return if let Value::Transit(right) = right { left.equivalent(&crate::transit_object::Decoded::read(right, crate::transit_object::scope(allocation)?)?) }
            else { left.equivalent_emitted(right) };
    }
    if let Value::Transit(right) = right { return crate::transit_object::Decoded::read(right, crate::transit_object::scope(allocation)?)?.equivalent_emitted(left); }
    if let (Some(left), Some(right)) = (emitted_byte_buffer(left), emitted_byte_buffer(right)) { return Ok(left == right); }
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
/// The scope retains type metadata and reached Transit intermediates; callers separately own numeric scratch.
pub fn strict_eq(left: &Value, right: &Value, allocation: &EvaluationScope) -> Result<bool> {
    strict_eq_scoped(left, right, Some(allocation))
}

pub(crate) fn strict_eq_scoped(left: &Value, right: &Value, allocation: Option<&EvaluationScope>) -> Result<bool> {
    if matches!(left, Value::Transit(_)) || matches!(right, Value::Transit(_)) { return emitted_eq(left, right, allocation); }
    if nullish(left) || nullish(right) { return Ok(nullish(left) && nullish(right)); }
    if date_time(left) && date_time(right) {
        return match (left, right) {
            (Value::Timestamp { timezone: a, .. }, Value::Timestamp { timezone: b, .. }) if a == b => Ok(compare(left, right, false)? == Ordering::Equal),
            _ if date_days(left).is_some() && date_days(right).is_some() => Ok(compare(left, right, false)? == Ordering::Equal),
            _ => Ok(false),
        };
    }
    if IntervalParts::of(left).is_some() && IntervalParts::of(right).is_some() {
        return Ok(std::mem::discriminant(left) == std::mem::discriminant(right) && emitted_eq(left, right, allocation)?);
    }
    let category = |value: &Value| match value { Value::Decimal(_) => 2, Value::F16(_) | Value::F32(_) | Value::F64(_) => 1, _ => 0 };
    if number(left).is_some() && number(right).is_some() {
        return Ok(category(left) == category(right) && sql_eq_scoped(left, right, "UTC", allocation)? == Some(true));
    }
    match (left, right) {
        (Value::List(_), Value::List(_)) | (Value::FixedSizeList(_), Value::FixedSizeList(_)) => {
            let (a, b) = (left.list_view().unwrap(), right.list_view().unwrap());
            if a.len() != b.len() { return Ok(false); }
            if a.is_float() && b.is_float() {
                return Ok((0..a.len()).all(|index| a.float_component(index) == b.float_component(index)));
            }
            if let (Some(a), Some(b)) = (NumericListInput::stored(a), NumericListInput::stored(b)) {
                return Ok(a.equal(&b, true) == Some(true));
            }
            for (a, b) in a.iter().zip(b.iter()) { if !strict_eq_scoped(a.as_ref(), b.as_ref(), allocation)? { return Ok(false); } }
            Ok(true)
        }
        (Value::Struct(a), Value::Struct(b)) => {
            if !a.keys().eq(b.keys()) { return Ok(false); }
            for (a, b) in a.values().zip(b.values()) { if !strict_eq_scoped(a, b, allocation)? { return Ok(false); } }
            Ok(true)
        }
        (Value::Time { .. }, Value::Time { .. }) | (Value::Duration { .. }, Value::Duration { .. }) => Ok(compare(left, right, false)? == Ordering::Equal),
        _ => emitted_eq(left, right, allocation),
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

pub(crate) fn float_order(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.total_cmp(&right),
    }
}

pub(crate) fn predicate_order(left: &Value, right: &Value, timezone: &str) -> Result<Ordering> {
    let left_reference = postgres_reference(left).map(Value::I32);
    let right_reference = postgres_reference(right).map(Value::I32);
    if left_reference.is_some() || right_reference.is_some() {
        if let (Some(left), Some(right)) = (emitted_integer(left), emitted_integer(right)) {
            return Ok(left.cmp(&right));
        }
        return compare_in_timezone(left_reference.as_ref().unwrap_or(left), right_reference.as_ref().unwrap_or(right), false, timezone);
    }
    if std::mem::discriminant(left) != std::mem::discriminant(right) {
        if let (Some(left), Some(right)) = (emitted_byte_buffer(left), emitted_byte_buffer(right)) {
            return Ok(left.iter().map(|byte| *byte as i8).cmp(right.iter().map(|byte| *byte as i8)));
        }
    }
    compare_in_timezone(left, right, false, timezone)
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
        return Ok(float_order(left, right));
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
        (Value::List(_), Value::List(_)) | (Value::FixedSizeList(_), Value::FixedSizeList(_)) => {
            let (a, b) = (left.list_view().unwrap(), right.list_view().unwrap());
            if a.is_float() && b.is_float() {
                let null_first = if nulls_first { Ordering::Less } else { Ordering::Greater };
                for index in 0..a.len().min(b.len()) {
                    let order = match (a.float_component(index), b.float_component(index)) {
                        (Some(left), Some(right)) => float_order(left, right),
                        (None, Some(_)) => null_first,
                        (Some(_), None) => null_first.reverse(),
                        (None, None) => Ordering::Equal,
                    };
                    if order != Ordering::Equal { return Ok(order); }
                }
                return Ok(a.len().cmp(&b.len()));
            }
            if let (Some(a), Some(b)) = (NumericListInput::stored(a), NumericListInput::stored(b)) {
                return Ok(a.order(&b, nulls_first));
            }
            for (a, b) in a.iter().zip(b.iter()) { let order = compare_in_timezone(a.as_ref(), b.as_ref(), nulls_first, timezone)?; if order != Ordering::Equal { return Ok(order); } }
            Ok(a.len().cmp(&b.len()))
        }
        _ => match (IntervalParts::of(left), IntervalParts::of(right)) {
            (Some(a), Some(b)) => interval_order(left, right, a, b),
            _ => Err(inapplicable("compare", &[left, right])),
        },
    }
}

#[cfg(test)]
mod numeric_lists {
    use super::*;
    use rtdb_types::{DecimalValue, ListValue};

    #[test]
    fn typed_comparison_preserves_exact_decimals_and_nulls() {
        let decimal = |n: i128, scale| Value::Decimal(DecimalValue::new(n.to_be_bytes().to_vec(), 32, scale, 128).unwrap());
        let left = ListValue::from(vec![Value::I64(9_007_199_254_740_993), Value::Null, decimal(100, 2)]);
        let right = ListValue::from(vec![Value::U64(9_007_199_254_740_993), Value::Null, decimal(10, 1)]);
        let a = NumericListInput::stored(left.view()).unwrap();
        let b = NumericListInput::stored(right.view()).unwrap();
        assert_eq!(a.equal(&b, false), None);
        assert_eq!(a.equal(&b, true), Some(true));
        assert_eq!(a.order(&b, false), Ordering::Equal);
        assert_ne!(left, right);
        let right = ListValue::from(vec![Value::U64(9_007_199_254_740_993), Value::Null, decimal(11, 1)]);
        let b = NumericListInput::stored(right.view()).unwrap();
        assert_eq!(a.equal(&b, false), Some(false));
        assert_eq!(a.order(&b, false), Ordering::Less);
    }
}
