use crate::BinaryOp;
use rtdb_types::{IntervalUnit, ScalarType, TimeUnit};

fn date(value: &ScalarType) -> bool { matches!(value, ScalarType::DateDay | ScalarType::DateMilli) }
fn integer(value: &ScalarType) -> bool { matches!(value, ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64) }
fn numeric(value: &ScalarType) -> bool { integer(value) || matches!(value, ScalarType::F16 | ScalarType::F32 | ScalarType::F64 | ScalarType::Decimal { .. }) }
pub(crate) fn interval(value: &ScalarType) -> bool { matches!(value, ScalarType::Interval(_) | ScalarType::IntervalMonthDayMicro) }

/// Returns the scalar result of an admitted temporal arithmetic pair, before null propagation.
pub fn temporal_arithmetic_type(op: BinaryOp, left: &ScalarType, right: &ScalarType) -> Option<ScalarType> {
    use BinaryOp as B;
    use ScalarType as S;
    match (op, left, right) {
        (B::Add | B::Subtract, S::Duration(a), S::Duration(b)) => Some(S::Duration((*a).max(*b))),
        (B::Divide, S::Duration(_), S::Duration(_)) => Some(S::I64),
        (B::Multiply | B::Divide, S::Duration(_), right) if numeric(right) => Some(left.clone()),
        (B::Multiply, left, S::Duration(_)) if numeric(left) => Some(right.clone()),
        (B::Divide, S::Duration(_), right) if interval(right) => Some(S::I64),
        (B::Add | B::Subtract, left, right) if interval(left) && interval(right) => Some(if left == right { left.clone() }
            else if [left, right].iter().any(|ty| **ty == S::Interval(IntervalUnit::MonthDayNano)) { S::Interval(IntervalUnit::MonthDayNano) }
            else { S::IntervalMonthDayMicro }),
        (B::Multiply | B::Divide, left, right) if interval(left) && integer(right) => Some(left.clone()),
        (B::Multiply, left, right) if integer(left) && interval(right) => Some(right.clone()),
        (B::Subtract, left, right) if date(left) && date(right) => Some(S::I32),
        (B::Subtract, S::Time { unit: a, .. }, S::Time { unit: b, .. }) => Some(S::Duration((*a).max(*b))),
        (B::Subtract, S::Timestamp { unit: a, .. }, S::Timestamp { unit: b, .. }) => Some(S::Duration((*a).max(*b))),
        (B::Subtract, left, S::Timestamp { unit, .. }) if date(left) => Some(S::Duration((*unit).max(TimeUnit::Microsecond))),
        (B::Subtract, S::Timestamp { unit, .. }, right) if date(right) => Some(S::Duration((*unit).max(TimeUnit::Microsecond))),
        (B::Add | B::Subtract, left, S::Duration(unit) | S::Time { unit, .. }) if date(left) => Some(S::Timestamp { unit: *unit, timezone: None }),
        (B::Add | B::Subtract, S::Timestamp { unit: a, timezone }, S::Duration(b) | S::Time { unit: b, .. }) => Some(S::Timestamp { unit: (*a).max(*b), timezone: timezone.clone() }),
        (B::Add | B::Subtract, left, S::Interval(IntervalUnit::YearMonth)) if date(left) => Some(left.clone()),
        (B::Add | B::Subtract, left, S::IntervalMonthDayMicro) if date(left) => Some(S::Timestamp { unit: TimeUnit::Microsecond, timezone: None }),
        (B::Add | B::Subtract, left, S::Interval(IntervalUnit::MonthDayNano)) if date(left) => Some(S::Timestamp { unit: TimeUnit::Nanosecond, timezone: None }),
        (B::Add | B::Subtract, S::Timestamp { .. }, S::Interval(IntervalUnit::YearMonth)) => Some(left.clone()),
        (B::Add | B::Subtract, S::Timestamp { unit, timezone }, S::IntervalMonthDayMicro) => Some(S::Timestamp { unit: (*unit).max(TimeUnit::Microsecond), timezone: timezone.clone() }),
        (B::Add | B::Subtract, S::Timestamp { timezone, .. }, S::Interval(IntervalUnit::MonthDayNano)) => Some(S::Timestamp { unit: TimeUnit::Nanosecond, timezone: timezone.clone() }),
        (B::Add, left, right) if matches!(left, S::Duration(_) | S::Time { .. }) || interval(left) => {
            if date(right) || matches!(right, S::Timestamp { .. }) { temporal_arithmetic_type(op, right, left) } else { None }
        }
        _ => None,
    }
}
