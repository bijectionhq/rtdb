use rtdb_types::{Anomaly, IntervalUnit, LogicalType, Result, ScalarType};

/// Checks every pair of vector legs, including unpopulated legs.
/// Null and absent legs admit every type; incompatible nonnull pairs return function-type-mismatch.
/// Interval pairs require matching units and exclude day-time intervals.
pub fn validate_comparison_types(left: &LogicalType, right: &LogicalType) -> Result<()> {
    use LogicalType as T;
    match (left, right) {
        (T::Nothing | T::Null, _) | (_, T::Nothing | T::Null) => Ok(()),
        (T::Maybe(inner) | T::RunEndEncoded { value: inner, .. }, _) => validate_comparison_types(inner, right),
        (_, T::Maybe(inner) | T::RunEndEncoded { value: inner, .. }) => validate_comparison_types(left, inner),
        (T::Union(legs), _) => legs.iter().try_for_each(|leg| validate_comparison_types(leg, right)),
        (_, T::Union(legs)) => legs.iter().try_for_each(|leg| validate_comparison_types(left, leg)),
        (T::Scalar(left), T::Scalar(right)) if scalar_pair(left, right)? => Ok(()),
        _ => Err(mismatch()),
    }
}

/// Admits ordered predicates through their broad generic fallback and specialized interval methods.
/// Every nonnull declared leg must have a registered type head; mixed interval units fail before payload evaluation.
pub fn validate_ordered_predicate_types(left: &LogicalType,right: &LogicalType) -> Result<()> {
    for left in left.legs() { for right in right.legs() {
        if left == LogicalType::Null || right == LogicalType::Null { continue; }
        if [&left,&right].iter().any(|ty|matches!(ty,LogicalType::Scalar(ScalarType::F16) | LogicalType::List { kind: rtdb_types::ListKind::FixedSizeList(_),.. })) { return Err(mismatch()); }
        if let (LogicalType::Scalar(left),LogicalType::Scalar(right)) = (&left,&right) {
            if interval(left).is_some() && interval(right).is_some() && !scalar_pair(left,right)? { return Err(mismatch()); }
        }
    } }
    Ok(())
}

fn numeric(data_type: &ScalarType) -> bool {
    matches!(data_type, ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64 | ScalarType::U8 | ScalarType::U16 | ScalarType::U32 | ScalarType::U64 | ScalarType::F32 | ScalarType::F64 | ScalarType::Decimal { .. })
}
fn date_time(data_type: &ScalarType) -> bool { matches!(data_type, ScalarType::DateDay | ScalarType::DateMilli | ScalarType::Timestamp { .. }) }
fn interval(data_type: &ScalarType) -> Option<&'static str> {
    match data_type {
        ScalarType::Interval(IntervalUnit::YearMonth) => Some("year-month"), ScalarType::Interval(IntervalUnit::DayTime) => Some("day-time"),
        ScalarType::IntervalMonthDayMicro => Some("month-day-micro"), ScalarType::Interval(IntervalUnit::MonthDayNano) => Some("month-day-nano"), _ => None,
    }
}
fn scalar_pair(left: &ScalarType, right: &ScalarType) -> Result<bool> {
    use ScalarType as S;
    if numeric(left) && numeric(right) || date_time(left) && date_time(right) { return Ok(true); }
    if let (Some(left), Some(right)) = (interval(left), interval(right)) {
        if left != right {
            return Err(Anomaly::incorrect("xtdb.expression/cannot-compare-different-unit-intervals", "Cannot compare intervals with different units")
                .with_context("x-unit", left.into()).with_context("y-unit", right.into()));
        }
        return Ok(left != "day-time");
    }
    Ok(matches!((left, right),
        (S::Bool, S::Bool) | (S::Binary | S::FixedSizeBinary(_), S::Binary | S::FixedSizeBinary(_))
        | (S::Utf8, S::Utf8) | (S::Uri, S::Uri) | (S::Keyword, S::Keyword) | (S::Uuid, S::Uuid)
        | (S::Time { .. }, S::Time { .. }) | (S::Duration(_), S::Duration(_))))
}

pub(crate) fn signature(name: &str, arguments: &[LogicalType]) -> Option<Result<LogicalType>> {
    if !matches!(name, "compare" | "compare_nulls_first" | "compare_nulls_last") { return None; }
    Some((|| {
        if arguments.len() != 2 { return Err(Anomaly::incorrect("xtdb/invalid-function-arity", format!("{name} requires two arguments"))); }
        validate_comparison_types(&arguments[0], &arguments[1])?;
        let mut output = LogicalType::Nothing;
        for left in arguments[0].legs() { for right in arguments[1].legs() {
            output = output.join(&if name == "compare" && (left == LogicalType::Null || right == LogicalType::Null) { LogicalType::Null } else { LogicalType::Scalar(ScalarType::I32) });
        } }
        Ok(output)
    })())
}

fn mismatch() -> Anomaly { Anomaly::incorrect("xtdb.expression/function-type-mismatch", "compare is not applicable to these vector types") }
