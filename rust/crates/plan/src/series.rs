use rtdb_types::{Anomaly, IntervalUnit, LogicalType as L, Result, ScalarType as S, TimeUnit};

pub(crate) fn signature(name: &str, arguments: &[L]) -> Option<Result<L>> {
    if name != "generate_series" { return None; }
    Some((|| {
        if !(3..=4).contains(&arguments.len()) {
            return Err(Anomaly::incorrect("xtdb/invalid-function-arity", "generate_series requires three or four arguments"));
        }
        let flags = arguments.get(3).cloned().unwrap_or(L::Scalar(S::Bool));
        let mut output = L::Nothing;
        for start in arguments[0].legs() { for end in arguments[1].legs() { for step in arguments[2].legs() { for inclusive in flags.legs() {
            let ty = if [&start,&end,&step,&inclusive].into_iter().any(|ty| *ty == L::Null) { L::Null }
            else {
                if inclusive != L::Scalar(S::Bool) { return Err(mismatch()); }
                let (L::Scalar(start),L::Scalar(end),L::Scalar(step)) = (&start,&end,&step) else { return Err(mismatch()); };
                L::list(L::Scalar(element(start,end,step)?))
            };
            output = output.join(&ty);
        } } } }
        Ok(output)
    })())
}

fn element(start: &S, end: &S, step: &S) -> Result<S> {
    let signed = |ty: &S| matches!(ty,S::I8 | S::I16 | S::I32 | S::I64);
    if signed(start) && signed(end) && signed(step) { return Ok(S::I64); }
    let stride_unit = match step {
        S::Interval(IntervalUnit::YearMonth) => TimeUnit::Second,
        S::IntervalMonthDayMicro => TimeUnit::Microsecond,
        S::Interval(IntervalUnit::MonthDayNano) => TimeUnit::Nanosecond,
        _ => return Err(mismatch()),
    };
    if matches!(start,S::DateDay | S::DateMilli) && matches!(end,S::DateDay | S::DateMilli) {
        if *start == S::DateMilli { return Err(Anomaly::unsupported("xtdb.expression/unsupported", "generate_series with date-from of type date-milli")); }
        if *end == S::DateMilli { return Err(Anomaly::unsupported("xtdb.expression/unsupported", "generate_series with date-to of type date-milli")); }
        return Ok(if *step == S::Interval(IntervalUnit::YearMonth) { S::DateDay } else { S::Timestamp { unit: stride_unit,timezone: None } });
    }
    let (S::Timestamp { unit: from,timezone: from_zone },S::Timestamp { unit: to,timezone: to_zone }) = (start,end) else { return Err(mismatch()); };
    if from_zone.is_some() != to_zone.is_some() { return Err(mismatch()); }
    let unit = [*from,*to,stride_unit].into_iter().max_by_key(|unit|unit.units_per_second()).expect("three temporal units");
    let timezone = if from_zone == to_zone { from_zone.clone() } else { Some("UTC".into()) };
    Ok(S::Timestamp { unit,timezone })
}
fn mismatch() -> Anomaly { Anomaly::incorrect("xtdb.expression/function-type-mismatch", "generate_series not applicable to argument types") }
