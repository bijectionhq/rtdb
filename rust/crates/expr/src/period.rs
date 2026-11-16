use crate::{EvalContext, cast};
use crate::{compare::nullish, numeric::inapplicable};
use rtdb_types::{LogicalType, Result, ScalarType, TimeUnit, Value};

pub(crate) fn intersection(left: &Value, right: &Value) -> Option<Value> {
    let (Value::TimestampTzRange { lower: lf, upper: lt }, Value::TimestampTzRange { lower: rf, upper: rt }) = (left, right) else { return None; };
    let (lower, upper) = ((*lf).max(*rf), (*lt).min(*rt));
    Some(if lower < upper { Value::TimestampTzRange { lower, upper } } else { Value::Null })
}

pub(crate) fn predicate(name: &str, args: &[Value], context: &EvalContext<'_>) -> Option<Result<Value>> {
    let base = name.strip_suffix('?').unwrap_or(name);
    if !matches!(base, "contains" | "strictly_contains" | "overlaps" | "strictly_overlaps" | "equals" | "precedes" | "strictly_precedes" | "immediately_precedes" | "succeeds" | "strictly_succeeds" | "immediately_succeeds" | "leads" | "strictly_leads" | "immediately_leads" | "lags" | "strictly_lags" | "immediately_lags") { return None; }
    Some((|| {
        if args.iter().any(nullish) { return Ok(Value::Null); }
        let Value::TimestampTzRange { lower: lf, upper: lt } = &args[0] else { return Err(inapplicable(name, &[&args[0], &args[1]])); };
        let (rf, rt) = match &args[1] {
            Value::TimestampTzRange { lower, upper } => (*lower, *upper),
            point if name == "contains?" => {
                let point = match point {
                    Value::Timestamp { value, timezone: Some(_), .. } => *value,
                    Value::Timestamp { unit, .. } => {
                        let Value::Timestamp { value, .. } = cast(point, &LogicalType::Scalar(ScalarType::Timestamp { unit: *unit, timezone: Some(context.timezone.into()) }), context.timezone)? else { return Err(inapplicable(name, &[point])); };
                        value
                    }
                    Value::Date(_) | Value::DateDay(_) | Value::DateMilli(_) => {
                        let Value::Timestamp { value, .. } = cast(point, &LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some(context.timezone.into()) }), context.timezone)? else { return Err(inapplicable(name, &[point])); };
                        value
                    }
                    _ => return Err(inapplicable(name, &[point])),
                };
                return Ok(Value::Bool(*lf <= point && point < *lt));
            }
            point => return Err(inapplicable(name, &[&args[0], point])),
        };
        let (lf, lt) = (*lf, *lt);
        Ok(Value::Bool(match base {
            "contains" => lf <= rf && lt >= rt,
            "strictly_contains" => lf < rf && lt > rt,
            "overlaps" => lf < rt && lt > rf,
            "strictly_overlaps" => lf > rf && lt < rt,
            "equals" => lf == rf && lt == rt,
            "precedes" => lt <= rf,
            "strictly_precedes" => lt < rf,
            "immediately_precedes" => lt == rf,
            "succeeds" => lf >= rt,
            "strictly_succeeds" => lf > rt,
            "immediately_succeeds" => lf == rt,
            "leads" => lf < rf && rf < lt && lt <= rt,
            "strictly_leads" => lf < rf && rf < lt && lt < rt,
            "immediately_leads" => lf < rf && lt == rt,
            "lags" => lf >= rf && rf < lt && lt > rt,
            "strictly_lags" => lf > rf && rf < lt && lt > rt,
            "immediately_lags" => lf == rf && lt > rt,
            _ => return Err(inapplicable(name, &[&args[0], &args[1]])),
        }))
    })())
}
