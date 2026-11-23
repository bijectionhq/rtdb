use std::cmp::Ordering;
use rtdb_types::{Anomaly, LogicalType, Result, Value};
use crate::compare::emitted_byte_buffer;

/// Checks every vector-leg pair, including legs with no populated rows.
/// Null legs compare with every type; unsupported nonnull pairs fail with function-type-mismatch.
/// Mixed interval units fail with cannot-compare-different-unit-intervals.
pub fn validate_order_type(data_type: &LogicalType) -> Result<()> { rtdb_plan::validate_comparison_types(data_type, data_type) }

pub(crate) fn scalar_compare(left: &Value,right: &Value,nulls_first: bool,timezone: &str) -> Result<i32> {
    if let (Some(left),Some(right)) = (emitted_byte_buffer(left),emitted_byte_buffer(right)) {
        let left_len = i32::try_from(left.len()).map_err(|_| crate::numeric::overflow())?;
        let right_len = i32::try_from(right.len()).map_err(|_| crate::numeric::overflow())?;
        let common = left.len().min(right.len());
        for index in (0..common - common % 2).step_by(2) {
            let difference = i32::from(u16::from_be_bytes([left[index],left[index + 1]])) - i32::from(u16::from_be_bytes([right[index],right[index + 1]]));
            if difference != 0 { return Ok(difference); }
        }
        if common % 2 != 0 {
            let difference = i32::from(left[common - 1]) - i32::from(right[common - 1]);
            if difference != 0 { return Ok(difference); }
        }
        return Ok(left_len - right_len);
    }
    Ok(match ordered_compare_in_timezone(left,right,nulls_first,timezone)? { Ordering::Less => -1, Ordering::Equal => 0, Ordering::Greater => 1 })
}

/// Implements the ordered-vector comparator used by sorting and ordered-set aggregates.
/// Integer vector pairs compare exact signed reads, including unsigned physical buffers.
/// Other numeric pairs use Java Double.compare semantics.
/// Temporal values borrow the session timezone; null placement is independent of sort direction.
/// Call `validate_order_type` for the complete vector type before comparing rows, including empty legs.
/// URI types pass admission, but a reached pair of nonnull URI objects fails in the pinned comparator.
pub fn ordered_compare_in_timezone(left: &Value, right: &Value, nulls_first: bool, timezone: &str) -> Result<Ordering> {
    if matches!(left, Value::Null | Value::Undefined) || matches!(right, Value::Null | Value::Undefined) {
        return crate::compare_in_timezone(left, right, nulls_first, timezone);
    }
    let integer = |value: &Value| match value {
        Value::I8(value) => Some(i64::from(*value)), Value::I16(value) => Some(i64::from(*value)),
        Value::I32(value) => Some(i64::from(*value)), Value::I64(value) => Some(*value),
        Value::U8(value) => Some(i64::from(*value as i8)), Value::U16(value) => Some(i64::from(*value as i16)),
        Value::U32(value) => Some(i64::from(*value as i32)), Value::U64(value) => Some(*value as i64), _ => None,
    };
    if let (Some(left), Some(right)) = (integer(left), integer(right)) { return Ok(left.cmp(&right)); }
    if matches!(left, Value::F16(_)) || matches!(right, Value::F16(_)) { return Err(mismatch()); }
    let number = |value: &Value| integer(value).map(|value| value as f64).or_else(|| crate::number(value));
    if let (Some(left), Some(right)) = (number(left), number(right)) {
        return Ok(compare_ordered_floats(left, right));
    }
    match (left, right) {
        (Value::Binary(left) | Value::FixedSizeBinary(left), Value::Binary(right) | Value::FixedSizeBinary(right)) => Ok(left.cmp(right)),
        (Value::Bool(_), Value::Bool(_)) | (Value::Utf8(_), Value::Utf8(_)) | (Value::Uri(_), Value::Uri(_)) | (Value::Keyword(_), Value::Keyword(_)) | (Value::Uuid(_), Value::Uuid(_))
        | (Value::Date(_) | Value::DateDay(_) | Value::DateMilli(_) | Value::Timestamp { .. }, Value::Date(_) | Value::DateDay(_) | Value::DateMilli(_) | Value::Timestamp { .. })
        | (Value::Time { .. }, Value::Time { .. }) | (Value::Duration { .. }, Value::Duration { .. })
        | (Value::IntervalYearMonth(_) | Value::IntervalDayTime { .. } | Value::IntervalMonthDayNano { .. } | Value::IntervalMonthDayMicro { .. },
           Value::IntervalYearMonth(_) | Value::IntervalDayTime { .. } | Value::IntervalMonthDayNano { .. } | Value::IntervalMonthDayMicro { .. }) => crate::compare_in_timezone(left, right, nulls_first, timezone),
        _ => Err(mismatch()),
    }
}

fn mismatch() -> Anomaly { Anomaly::incorrect("xtdb.expression/function-type-mismatch", "compare is not applicable to these vector types") }

pub fn compare_ordered_floats(left: f64, right: f64) -> Ordering {
    match (left.is_nan(), right.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => left.total_cmp(&right),
    }
}
