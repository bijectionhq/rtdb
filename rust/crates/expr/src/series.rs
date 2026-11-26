use rtdb_types::{Anomaly, CivilDate, FixedOffset, LogicalType, Result, ScalarType, TimeUnit, Value, ZoneId};
use crate::{EvalContext, numeric::{division_by_zero, inapplicable, integer}, temporal::IntervalParts};

fn overflow() -> Anomaly { Anomaly::incorrect("xtdb.expression/overflow-error","data exception - overflow error") }

#[derive(Clone, Copy)]
pub(crate) struct IntegerSeries { start: i64, step: i64, length: usize }
impl IntegerSeries {
    fn new(start: i64, end: i64, step: i64, inclusive: bool) -> Result<Self> {
        let end = if inclusive { end.wrapping_add(step.signum()) } else { end };
        let distance = end.checked_sub(start).ok_or_else(overflow)?;
        if step == 0 { return Err(division_by_zero()); }
        let quotient = distance.wrapping_div(step);
        let ceiling = if (distance ^ step) >= 0 && distance.wrapping_rem(step) != 0 { quotient.wrapping_add(1) } else { quotient };
        let length = usize::try_from(i32::try_from(ceiling.max(0)).map_err(|_| overflow())?).map_err(|_| overflow())?;
        Ok(Self { start, step, length })
    }
    pub(crate) fn len(self) -> usize { self.length }
    pub(crate) fn at(self, index: usize) -> i64 { self.start.wrapping_add(self.step.wrapping_mul(index as i64)) }
    pub(crate) fn nth(self, index: usize) -> Value { Value::I64(self.at(index)) }
}

pub(crate) enum GeneratedSeries { Null, Integer(IntegerSeries), Temporal(TemporalSeries) }
pub(crate) struct TemporalSeries { position: Position, inclusive: bool }
enum Position {
    Date { current: i64, end: i64, months: i64 },
    Timestamp { second: i64, nano: i64, end_second: i64, end_nano: i64, stride: IntervalParts, unit: TimeUnit, zone: Option<ZoneId>, timezone: Option<String> },
}

pub(crate) fn prepare(args: &[Value], context: &EvalContext<'_>) -> Result<GeneratedSeries> {
    context.reserve(args.iter().try_fold(4096usize, |size,value|size.checked_add(value.retained_bytes()?).ok_or_else(overflow))?.checked_mul(8).ok_or_else(overflow)?)?;
    let output = rtdb_plan::function_type("generate_series",&args.iter().map(Value::logical_type).collect::<Vec<_>>())?;
    if args.iter().any(crate::compare::nullish) { return Ok(GeneratedSeries::Null); }
    let inclusive = match args.get(3) { Some(Value::Bool(value)) => *value,None => false,_ => return Err(inapplicable("generate_series",&[])) };
    if let (Some(start),Some(end),Some(step)) = (integer(&args[0]),integer(&args[1]),integer(&args[2])) {
        return Ok(GeneratedSeries::Integer(IntegerSeries::new(start as i64,end as i64,step as i64,inclusive)?));
    }
    let LogicalType::List { element,.. } = output else { return Err(inapplicable("generate_series",&[])); };
    let LogicalType::Scalar(element) = *element else { return Err(inapplicable("generate_series",&[])); };
    let stride = IntervalParts::of(&args[2]).ok_or_else(||inapplicable("generate_series",&[]))?;
    let position = match element {
        ScalarType::DateDay => Position::Date { current: args[0].date_days().ok_or_else(overflow)?,end: args[1].date_days().ok_or_else(overflow)?,months: i64::from(stride.months) },
        ScalarType::Timestamp { unit,timezone } => {
            let (second,nano) = timestamp(&args[0],unit)?;
            let (end_second,end_nano) = timestamp(&args[1],unit)?;
            let zone = timezone.as_deref().map(ZoneId::parse).transpose()?;
            Position::Timestamp { second,nano,end_second,end_nano,stride,unit,zone,timezone }
        }
        _ => return Err(inapplicable("generate_series",&[])),
    };
    Ok(GeneratedSeries::Temporal(TemporalSeries { position,inclusive }))
}

fn timestamp(value: &Value, date_unit: TimeUnit) -> Result<(i64,i64)> {
    let (value,unit) = match value {
        Value::Timestamp { value,unit,.. } => (*value,*unit),
        date => (date.date_days().ok_or_else(overflow)?.checked_mul(86_400).and_then(|value|value.checked_mul(date_unit.units_per_second())).ok_or_else(overflow)?,date_unit),
    };
    Ok((value.div_euclid(unit.units_per_second()),value.rem_euclid(unit.units_per_second()) * (1_000_000_000 / unit.units_per_second())))
}

impl TemporalSeries {
    fn next(&mut self, context: &EvalContext<'_>) -> Result<Option<Value>> {
        Ok(Some(match &mut self.position {
            Position::Date { current,end,months } => {
                if *current > *end || !self.inclusive && *current == *end { return Ok(None); }
                let date = CivilDate::from_epoch_day(*current)?;
                let value = Value::DateDay(*current as i32);
                *current = date.plus_months(*months)?.epoch_day();
                value
            }
            Position::Timestamp { second,nano,end_second,end_nano,stride,unit,zone,timezone } => {
                let order = (*second,*nano).cmp(&(*end_second,*end_nano));
                if order.is_gt() || !self.inclusive && order.is_eq() { return Ok(None); }
                let value = second.checked_mul(unit.units_per_second()).and_then(|value|value.checked_add(*nano / (1_000_000_000 / unit.units_per_second()))).ok_or_else(overflow)?;
                context.reserve(timezone.as_ref().map_or(0,String::len))?;
                let value = Value::Timestamp { value,unit: *unit,timezone: timezone.clone() };
                if stride.months != 0 || stride.days != 0 { *second = calendar_step(*second,*stride,*zone)?; }
                *second = second.checked_add(stride.nanos.div_euclid(1_000_000_000)).ok_or_else(overflow)?;
                *nano += stride.nanos.rem_euclid(1_000_000_000);
                if *nano >= 1_000_000_000 { *nano -= 1_000_000_000;*second = second.checked_add(1).ok_or_else(overflow)?; }
                value
            }
        }))
    }
}

fn calendar_step(second: i64, stride: IntervalParts, zone: Option<ZoneId>) -> Result<i64> {
    let offset = zone.map(|zone|zone.offset_at(second)).transpose()?.unwrap_or(FixedOffset::UTC);
    let local = second.checked_add(i64::from(offset.seconds())).ok_or_else(overflow)?;
    let date = CivilDate::from_epoch_day(local.div_euclid(86_400))?.plus_months(i64::from(stride.months))?.plus_days(i64::from(stride.days))?;
    let local = date.epoch_day().checked_mul(86_400).and_then(|day|day.checked_add(local.rem_euclid(86_400))).ok_or_else(overflow)?;
    match zone {
        Some(zone) => local.checked_sub(i64::from(zone.offset_for_local(local,Some(offset))?.seconds())).ok_or_else(overflow),
        None => Ok(local),
    }
}

fn push(out: &mut Vec<Value>, value: Value, context: &EvalContext<'_>) -> Result<()> {
    if out.len() == i32::MAX as usize { return Err(overflow()); }
    if out.len() == out.capacity() {
        let capacity = out.capacity().saturating_mul(2).max(16).min(i32::MAX as usize);
        let additional = capacity - out.capacity();
        context.reserve(additional.checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?)?;
        out.try_reserve_exact(additional).map_err(|_|Anomaly::incorrect("rtdb.expr/allocation-failed","Unable to allocate temporal series").with_kind(rtdb_types::AnomalyKind::ResourceExhausted))?;
    }
    out.push(value);
    Ok(())
}

impl GeneratedSeries {
    pub(crate) fn materialize(self, context: &EvalContext<'_>) -> Result<Value> {
        match self {
            Self::Null => Ok(Value::Null),
            Self::Integer(series) => {
                context.reserve(series.len().checked_mul(std::mem::size_of::<Value>()).ok_or_else(overflow)?)?;
                let mut out = Vec::new();
                out.try_reserve_exact(series.len()).map_err(|_|Anomaly::incorrect("rtdb.expr/allocation-failed","Unable to allocate integer series").with_kind(rtdb_types::AnomalyKind::ResourceExhausted))?;
                out.extend((0..series.len()).map(|index|series.nth(index)));
                Ok(Value::list(out))
            }
            Self::Temporal(mut series) => {
                let mut out = Vec::new();
                while let Some(value) = series.next(context)? { push(&mut out,value,context)?; }
                Ok(Value::list(out))
            }
        }
    }
    pub(crate) async fn materialize_async(self, context: &EvalContext<'_>) -> Result<Value> {
        let Self::Temporal(mut series) = self else { return self.materialize(context); };
        let mut out = Vec::new();
        while let Some(value) = series.next(context)? {
            push(&mut out,value,context)?;
            if out.len() % 1024 == 0 { tokio::task::yield_now().await; }
        }
        Ok(Value::list(out))
    }
}
