use std::num::NonZeroI64;
use rtdb_types::StructFields;
use crate::{EvalContext, cast, numeric::{division_by_zero, inapplicable, integer}, temporal::{IntervalParts, timestamp_units}};
use rtdb_plan::{BinaryOp, TemporalField as F};
use rtdb_types::{CivilDate, DateUnit, LogicalType as L, Result, ScalarType as S, TimeUnit as U, Value, ZoneId};

fn overflow() -> rtdb_types::Anomaly { rtdb_types::Anomaly::incorrect("xtdb.expression/overflow-error", "data exception - overflow error") }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TemporalFunction {
    Extract,
    DateTrunc,
    Age,
    DateBin,
    RangeBins,
    CurrentTimestamp,
    CurrentDate,
    CurrentTime,
    CurrentTimezone,
    LocalDate,
    LocalTime,
    LocalTimestamp,
}

impl TemporalFunction {
    pub(crate) fn from_canonical(name: &str) -> Option<Self> {
        Some(match name {
            "extract" => Self::Extract,
            "date_trunc" => Self::DateTrunc,
            "age" => Self::Age,
            "date_bin" => Self::DateBin,
            "range_bins" => Self::RangeBins,
            "current_timestamp" | "now" => Self::CurrentTimestamp,
            "current_date" => Self::CurrentDate,
            "current_time" => Self::CurrentTime,
            "current_timezone" => Self::CurrentTimezone,
            "local_date" | "localdate" => Self::LocalDate,
            "local_time" | "localtime" => Self::LocalTime,
            "local_timestamp" | "localtimestamp" => Self::LocalTimestamp,
            _ => return None,
        })
    }

    pub(crate) fn canonical_name(self) -> &'static str {
        match self {
            Self::Extract => "extract",
            Self::DateTrunc => "date_trunc",
            Self::Age => "age",
            Self::DateBin => "date_bin",
            Self::RangeBins => "range_bins",
            Self::CurrentTimestamp => "current_timestamp",
            Self::CurrentDate => "current_date",
            Self::CurrentTime => "current_time",
            Self::CurrentTimezone => "current_timezone",
            Self::LocalDate => "local_date",
            Self::LocalTime => "local_time",
            Self::LocalTimestamp => "local_timestamp",
        }
    }

    pub(crate) fn is_strict(self) -> bool {
        !matches!(self, Self::DateBin | Self::RangeBins)
    }

    pub(crate) fn eval(self, args: &[Value], context: &EvalContext<'_>) -> Result<Value> {
        if args.iter().any(crate::compare::nullish) { return Ok(Value::Null); }
        match self {
            Self::Extract | Self::DateTrunc => {
                let name = self.canonical_name();
                let Value::Utf8(field) = &args[0] else { return Err(inapplicable(name, &[])); };
                let field = F::parse(field)?;
                let L::Scalar(source) = args[1].logical_type() else { return Err(inapplicable(name, &[])); };
                field.validate_for(name, &source)?;
                if self == Self::Extract { extract(field, &args[1]) }
                else {
                    let zone = args.get(2).map(|value| match value { Value::Utf8(zone) => rtdb_plan::truncation_zone(zone), _ => Err(inapplicable(name, &[])) }).transpose()?;
                    truncate(field, &args[1], zone)
                }
            }
            Self::Age => age(&args[0], &args[1], context.timezone),
            Self::DateBin => {
                let default_origin = origin();
                let origin = args.get(2).unwrap_or(&default_origin);
                let difference = crate::arithmetic_in_timezone(BinaryOp::Subtract, &args[1], origin, context.timezone)?;
                let quotient = crate::arithmetic_in_timezone(BinaryOp::Divide, &difference, &args[0], context.timezone)?;
                let offset = crate::arithmetic_in_timezone(BinaryOp::Multiply, &args[0], &quotient, context.timezone)?;
                crate::arithmetic_in_timezone(BinaryOp::Add, origin, &offset, context.timezone)
            }
            Self::RangeBins => match RangeBins::prepare(args, context)? { Some(bins) => bins.materialize(context), None => Ok(Value::Null) },
            _ => clock(self, args, context),
        }
    }
}

fn timestamp(value: i64, unit: U, timezone: Option<String>) -> Value { Value::Timestamp { value, unit, timezone } }
fn instant_type() -> L { L::Scalar(S::Timestamp { unit: U::Microsecond, timezone: Some("UTC".into()) }) }
fn origin() -> Value { timestamp(0,U::Microsecond,Some("UTC".into())) }

fn clock(function: TemporalFunction, args: &[Value], context: &EvalContext<'_>) -> Result<Value> {
    if function == TemporalFunction::CurrentTimezone { return Ok(Value::Utf8(ZoneId::parse(context.timezone)?.to_string())); }
    let name = function.canonical_name();
    let precision = args.first().map(|value| integer(value).map(|n| n.clamp(0, 9) as u8).ok_or_else(|| inapplicable(name, &[value]))).transpose()?.unwrap_or(6);
    let unit = match precision { 0 => U::Second, 1..=3 => U::Millisecond, 4..=6 => U::Microsecond, _ => U::Nanosecond };
    let hz = unit.units_per_second();
    let instant = context.current_time;
    let is_local = matches!(function, TemporalFunction::LocalDate | TemporalFunction::LocalTime | TemporalFunction::LocalTimestamp);
    let seconds = if is_local { instant.seconds().checked_add(i64::from(ZoneId::parse(context.timezone)?.offset_at(instant.seconds())?.seconds())).ok_or_else(overflow)? } else { instant.seconds() };
    if matches!(function, TemporalFunction::CurrentDate | TemporalFunction::LocalDate) {
        return Ok(Value::calendar_date(CivilDate::from_epoch_day(seconds.div_euclid(86_400))?, DateUnit::Day));
    }
    let modulus = [1, 100, 10, 1, 100, 10, 1, 100, 10, 1][usize::from(precision)];
    let is_timestamp = matches!(function, TemporalFunction::CurrentTimestamp | TemporalFunction::LocalTimestamp);
    let seconds = if is_timestamp { seconds } else { seconds.rem_euclid(86_400) };
    let value = seconds.checked_mul(hz).and_then(|value| value.checked_add(i64::from(instant.nanos()) / (1_000_000_000 / hz))).ok_or_else(overflow)?;
    let value = (value / modulus) * modulus;
    Ok(if is_timestamp { timestamp(value, unit, if function == TemporalFunction::CurrentTimestamp { Some("UTC".into()) } else { None }) }
        else { Value::Time { value, unit, bit_width: if hz <= 1000 { 32 } else { 64 } } })
}

fn date_field(field: F,date: CivilDate) -> Result<i64> {
    let (year,month,day) = date.ymd();
    Ok(match field {
        F::Year => i64::from(year), F::Month => i64::from(month), F::Day => i64::from(day),
        F::Dow => i64::from(date.iso_day_of_week() % 7), F::IsoDow => i64::from(date.iso_day_of_week()), F::Doy => i64::from(date.day_of_year()),
        F::Quarter => i64::from((month - 1) / 3 + 1),
        F::Week => {
            let thursday = date.plus_days(4 - i64::from(date.iso_day_of_week()))?;
            i64::from((thursday.day_of_year() - 1) / 7 + 1)
        }
        _ => return Err(inapplicable("extract",&[])),
    })
}
fn clock_field(field: F,seconds: i64) -> Result<i64> {
    match field { F::Hour => Ok(seconds / 3600),F::Minute => Ok((seconds / 60) % 60),F::Second => Ok(seconds % 60),_ => Err(inapplicable("extract",&[])) }
}
fn extract(field: F,value: &Value) -> Result<Value> {
    let result = if let Some(day) = value.date_days() { date_field(field,CivilDate::from_epoch_day(day)?)? }
    else if let Some(interval) = IntervalParts::of(value) {
        match field { F::Year => i64::from(interval.months / 12), F::Month => i64::from(interval.months % 12), F::Day => i64::from(interval.days), _ => clock_field(field,interval.nanos.div_euclid(1_000_000_000))? }
    } else {
        match value {
            Value::Time { value,unit,.. } => {
                let nanos = value.checked_mul(1_000_000_000 / unit.units_per_second()).ok_or_else(overflow)?;
                if !(0..86_400_000_000_000).contains(&nanos) { return Err(inapplicable("extract",&[])); }
                clock_field(field,nanos / 1_000_000_000)?
            }
            Value::Timestamp { value,unit,timezone } => timestamp_field(field,*value,*unit,timezone.as_deref().map(ZoneId::parse).transpose()?)?,
            _ => return Err(inapplicable("extract",&[value])),
        }
    };
    Ok(Value::I32(extracted(result)?))
}
fn extracted(result: i64) -> Result<i32> { i32::try_from(result).map_err(|_|overflow()) }
/// A timestamp's field under its zone label's rules, `zone` being the parsed label of a zoned timestamp.
fn timestamp_field(field: F,value: i64,unit: U,zone: Option<ZoneId>) -> Result<i64> {
    let seconds = value.div_euclid(unit.units_per_second());
    let offset = zone.map(|zone|zone.offset_at(seconds)).transpose()?.map_or(0,|offset|offset.seconds());
    Ok(match field {
        F::TimezoneHour if zone.is_some() => i64::from(offset / 3600), F::TimezoneMinute if zone.is_some() => i64::from((offset / 60) % 60), F::Epoch if zone.is_some() => seconds,
        _ => {
            let local = seconds.checked_add(i64::from(offset)).ok_or_else(overflow)?;
            if matches!(field,F::Hour | F::Minute | F::Second) { clock_field(field,local.rem_euclid(86_400))? }
            else { date_field(field,CivilDate::from_epoch_day(local.div_euclid(86_400))?)? }
        }
    })
}

/// EXTRACT over microsecond timestamps sharing one zone label, cell for cell as the scalar call computes it.
/// `Ok(None)` where the field doesn't apply or the label isn't a zone, which the scalar call decides per non-null cell;
/// a failing cell raises the scalar call's error for it. Unselected rows give null and are never read.
pub(crate) fn extract_timestamps(field: &str,values: &arrow_array::TimestampMicrosecondArray,timezone: Option<&str>,selection: crate::kernels::Selection<'_>) -> Result<Option<arrow_array::Int32Array>> {
    use arrow_array::Array;
    let Ok(field) = F::parse(field) else { return Ok(None) };
    if field.validate_for("extract",&S::Timestamp { unit: U::Microsecond,timezone: timezone.map(str::to_owned) }).is_err() { return Ok(None); }
    let Ok(zone) = timezone.map(ZoneId::parse).transpose() else { return Ok(None) };
    let mut cells = Vec::with_capacity(values.len());
    for row in 0..values.len() {
        let read = !values.is_null(row) && selection.is_none_or(|rows| rows[row]);
        cells.push(if read { Some(timestamp_field(field,values.value(row),U::Microsecond,zone).and_then(extracted)?) } else { None });
    }
    Ok(Some(arrow_array::Int32Array::from(cells)))
}

fn truncated_date(field: F,date: CivilDate) -> Result<CivilDate> {
    let (year,month,_) = date.ymd();
    match field {
        F::Millennium | F::Century | F::Decade | F::Year => {
            let multiple = match field { F::Millennium => 1000,F::Century => 100,F::Decade => 10,_ => 1 };
            CivilDate::from_ymd(year / multiple * multiple,1,1)
        }
        F::Quarter => CivilDate::from_ymd(year,(month - 1) / 3 * 3 + 1,1), F::Month => CivilDate::from_ymd(year,month,1),
        F::Week => date.plus_days(1 - i64::from(date.iso_day_of_week())), _ => Ok(date),
    }
}
fn trunc_local(field: F,value: i64,hz: i64) -> Result<i64> {
    let calendar = matches!(field,F::Millennium | F::Century | F::Decade | F::Year | F::Quarter | F::Month | F::Week | F::Day);
    if calendar { return truncated_date(field,CivilDate::from_epoch_day(value.div_euclid(hz * 86_400))?)?.epoch_day().checked_mul(86_400).and_then(|value|value.checked_mul(hz)).ok_or_else(overflow); }
    let quantum = match field { F::Hour => hz * 3600,F::Minute => hz * 60,F::Second => hz,F::Millisecond => hz / 1000,F::Microsecond => hz / 1_000_000,_ => return Err(inapplicable("date_trunc",&[])) };
    if quantum <= 1 { Ok(value) } else { value.div_euclid(quantum).checked_mul(quantum).ok_or_else(overflow) }
}

/// Truncates an Arrow Date32 cell, reporting overflow if the resulting day cannot be stored.
pub fn truncate_date32(field: F, days: i64) -> Result<i32> {
    let date = truncated_date(field, CivilDate::from_epoch_day(days)?)?;
    i32::try_from(date.epoch_day()).map_err(|_| overflow())
}

/// Truncates the calendar day containing an Arrow Date64 cell and returns its midnight in milliseconds.
pub fn truncate_date64(field: F, millis: i64) -> Result<i64> {
    use rtdb_types::java_date::MILLIS_PER_DAY;

    let day = CivilDate::from_epoch_day(millis.div_euclid(MILLIS_PER_DAY))?;
    truncated_date(field, day)?.epoch_day().checked_mul(MILLIS_PER_DAY).ok_or_else(overflow)
}

/// Selects the calendar zone for an applicable DATE_TRUNC field and timestamp.
pub fn timestamp_truncation_zone(field: F, label: Option<&str>, override_zone: Option<ZoneId>) -> Result<Option<ZoneId>> {
    match label {
        Some(label) if !matches!(field, F::Millisecond | F::Microsecond) => override_zone.map(Ok).unwrap_or_else(|| ZoneId::parse(label)).map(Some),
        _ => Ok(None),
    }
}

/// Truncates a timestamp in its original unit using the zone selected by `timestamp_truncation_zone`.
pub fn truncate_timestamp(field: F, value: i64, unit: U, zone: Option<&ZoneId>) -> Result<i64> {
    let hz = unit.units_per_second();
    let Some(zone) = zone else { return trunc_local(field, value, hz); };
    let seconds = value.div_euclid(hz);
    let offset = zone.offset_at(seconds)?;
    let local = seconds.checked_add(i64::from(offset.seconds())).ok_or_else(overflow)?;
    let truncated = trunc_local(field, local, 1)?;
    truncated.checked_sub(i64::from(zone.offset_for_local(truncated, Some(offset))?.seconds()))
        .and_then(|value| value.checked_mul(hz)).ok_or_else(overflow)
}

fn truncate(field: F,value: &Value,override_zone: Option<ZoneId>) -> Result<Value> {
    if let Some(day) = value.date_days() {
        let unit = if value.logical_type() == L::Scalar(S::DateDay) { DateUnit::Day } else { DateUnit::Millisecond };
        return Ok(Value::calendar_date(truncated_date(field,CivilDate::from_epoch_day(day)?)?,unit));
    }
    if let Some(mut interval) = IntervalParts::of(value) {
        match field {
            F::Millennium | F::Century | F::Decade | F::Year => {
                let multiple = match field { F::Millennium => 12000,F::Century => 1200,F::Decade => 120,_ => 12 };
                interval.months = interval.months / multiple * multiple;interval.days = 0;interval.nanos = 0;
            }
            F::Quarter => { interval.months = interval.months / 3 * 3;interval.days = 0;interval.nanos = 0; }
            F::Month => { interval.days = 0;interval.nanos = 0; }
            F::Week => { interval.days = interval.days / 7 * 7;interval.nanos = 0; }
            F::Day => interval.nanos = 0,
            _ => { let quantum = match field { F::Hour => 3_600_000_000_000,F::Minute => 60_000_000_000,F::Second => 1_000_000_000,F::Millisecond => 1_000_000,F::Microsecond => 1000,_ => return Err(inapplicable("date_trunc",&[])) };interval.nanos = interval.nanos / quantum * quantum; }
        }
        let L::Scalar(ty) = value.logical_type() else { return Err(inapplicable("date_trunc",&[])); };
        return interval.into_value(&ty);
    }
    let Value::Timestamp { value,unit,timezone } = value else { return Err(inapplicable("date_trunc",&[value])); };
    let zone = timestamp_truncation_zone(field, timezone.as_deref(), override_zone)?;
    let result = truncate_timestamp(field, *value, *unit, zone.as_ref())?;
    Ok(timestamp(result,*unit,timezone.clone()))
}

fn age(end: &Value,start: &Value,timezone: &str) -> Result<Value> {
    let local = |value: &Value| -> Result<(CivilDate,i64)> {
        let unit = match value { Value::Timestamp { unit,.. } => *unit,_ => U::Microsecond };
        age_local(timestamp_units(value,unit,true,timezone)?, unit)
    };
    let end_local = local(end)?;
    let start_local = local(start)?;
    let nano = [start,end].iter().any(|value|matches!(value,Value::Timestamp { unit: U::Nanosecond,.. }));
    age_parts(end_local, start_local, nano)?.into_value(&if nano { S::Interval(rtdb_types::IntervalUnit::MonthDayNano) } else { S::IntervalMonthDayMicro })
}

fn age_local(value: i64, unit: U) -> Result<(CivilDate, i64)> {
    let hz = unit.units_per_second();
    Ok((CivilDate::from_epoch_day(value.div_euclid(hz * 86_400))?, value.rem_euclid(hz * 86_400) * (1_000_000_000 / hz)))
}

/// An epoch day or timestamp tick with its session zone for AGE.
pub enum AgeOperand<'a> {
    Date(i64),
    Timestamp(i64, U, Option<&'a ZoneId>),
}

pub(crate) fn calendar_age(end: AgeOperand<'_>, start: AgeOperand<'_>) -> Result<IntervalParts> {
    use crate::temporal::{date_timestamp, local_timestamp};

    let local = |operand| match operand {
        AgeOperand::Date(days) => age_local(date_timestamp(days, U::Microsecond)?, U::Microsecond),
        AgeOperand::Timestamp(value, unit, zone) => age_local(local_timestamp(value, unit, zone)?, unit),
    };
    let nano = [&end, &start].iter().any(|operand| matches!(operand, AgeOperand::Timestamp(_, U::Nanosecond, _)));
    age_parts(local(end)?, local(start)?, nano)
}

fn age_parts((end_date, end_nano): (CivilDate, i64), (start_date, start_nano): (CivilDate, i64), nano: bool) -> Result<IntervalParts> {
    let (ey,em,ed) = end_date.ymd();let (sy,sm,sd) = start_date.ymd();
    let mut months = (i64::from(ey) - i64::from(sy)) * 12 + i64::from(em) - i64::from(sm);
    let mut days = i64::from(ed) - i64::from(sd);
    if months > 0 && days < 0 { months -= 1;days = end_date.epoch_day() - start_date.plus_months(months)?.epoch_day(); }
    else if months < 0 && days > 0 { months += 1;days -= i64::from(CivilDate::month_length(ey,em)); }
    let negative = months < 0 || days < 0;
    if negative && start_nano < end_nano { days += 1; }
    else if !negative && days != 0 && start_nano > end_nano { days -= 1; }
    let total_nanos = i128::from(end_date.epoch_day() - start_date.epoch_day()) * 86_400_000_000_000 + i128::from(end_nano) - i128::from(start_nano);
    let duration_days = total_nanos.div_euclid(1_000_000_000) / 86_400;
    let mut nanos = (total_nanos - duration_days * 86_400_000_000_000) as i64;
    if !nano { nanos = nanos.div_euclid(1000) * 1000; }
    Ok(IntervalParts { months: i32::try_from(months).map_err(|_|overflow())?,days: i32::try_from(days).map_err(|_|overflow())?,nanos })
}

/// Fixed-time binning for a source and origin that are both local or both zoned.
pub struct FixedDateBin {
    source_unit: U,
    origin: i64,
    origin_unit: U,
    stride_nanos: i64,
    output_unit: U,
}

impl FixedDateBin {
    /// Computes the output unit for fixed-time interval strides of the given precision.
    pub fn output_unit(stride_unit: U, origin_unit: U) -> U { stride_unit.max(origin_unit) }

    /// Binds the time component of an interval with zero months and days.
    pub fn new(stride_nanos: i64, stride_unit: U, source_unit: U, origin: i64, origin_unit: U) -> Self {
        let output_unit = Self::output_unit(stride_unit, origin_unit);
        Self { source_unit, origin, origin_unit, stride_nanos, output_unit }
    }

    pub fn cell(&self, source: i64) -> Result<i64> {
        use crate::{numeric::signed_quotient, temporal::{interval_ticks, rescale}};

        let difference_unit = self.source_unit.max(self.origin_unit);
        let source = rescale(source, self.source_unit, difference_unit)?;
        let origin = rescale(self.origin, self.origin_unit, difference_unit)?;
        let difference = source.checked_sub(origin).ok_or_else(overflow)?;
        let stride = rescale(self.stride_nanos, U::Nanosecond, difference_unit)?;
        let quotient = signed_quotient(difference, stride)?;
        let offset = self.stride_nanos.checked_mul(quotient).ok_or_else(overflow)?;
        let origin = rescale(self.origin, self.origin_unit, self.output_unit)?;
        origin.checked_add(interval_ticks(offset, self.output_unit, 1)?).ok_or_else(overflow)
    }
}

#[derive(Clone,Copy)]
pub struct RangeBins { stride: i64, from: i64, to: i64, base: i64, size: i64 }
impl RangeBins {
    pub(crate) fn prepare(args: &[Value],context: &EvalContext<'_>) -> Result<Option<Self>> {
        context.reserve(args.iter().try_fold(4096usize,|bytes,value|bytes.checked_add(value.retained_bytes()?).ok_or_else(overflow))?.checked_mul(8).ok_or_else(overflow)?)?;
        rtdb_plan::function_type("range_bins",&args.iter().map(Value::logical_type).collect::<Vec<_>>())?;
        if args.iter().any(crate::compare::nullish) { return Ok(None); }
        let stride = Self::stride_micros(&args[0], context.timezone)?;
        let convert = |value: &Value| -> Result<i64> { let Value::Timestamp { value,.. } = cast(value,&instant_type(),context.timezone)? else { return Err(inapplicable("range_bins",&[])); };Ok(value) };
        let from = convert(&args[1])?;let to = convert(&args[2])?;let origin = args.get(3).map(convert).transpose()?.unwrap_or(0);
        Ok(Some(Self::from_micros(stride, from, to, origin)))
    }

    pub fn stride_micros(value: &Value, timezone: &str) -> Result<NonZeroI64> {
        let Value::Duration { value, .. } = cast(value, &L::Scalar(S::Duration(U::Microsecond)), timezone)? else {
            return Err(inapplicable("range_bins", &[]));
        };
        NonZeroI64::new(value).ok_or_else(division_by_zero)
    }

    /// Casts month-day-micro or month-day-nano stride cells with the scalar duration checks.
    pub fn stride_parts_micros(months: i32, days: i32, nanos: i64) -> Result<NonZeroI64> {
        let micros = crate::cast::month_day_duration(months, days, nanos, U::Microsecond)?;
        NonZeroI64::new(micros).ok_or_else(division_by_zero)
    }

    pub fn from_micros(stride: NonZeroI64, from: i64, to: i64, origin: i64) -> Self {
        let stride = stride.get();
        let base = from.wrapping_sub(origin).wrapping_div(stride).wrapping_mul(stride).wrapping_add(origin);
        let top = to.wrapping_sub(origin).wrapping_sub(1).wrapping_div(stride).wrapping_add(1).wrapping_mul(stride).wrapping_add(origin);
        let size = top.wrapping_sub(base).wrapping_div(stride);
        Self { stride, from, to, base, size }
    }
    pub(crate) fn size(self) -> Result<i32> { i32::try_from(self.size).map_err(|_|overflow()) }
    pub fn len(self) -> Result<usize> { Ok(self.size()?.max(0) as usize) }
    pub fn cell(self, index: usize) -> (i64, i64, f64) {
        let from = self.base.wrapping_add((index as i64).wrapping_mul(self.stride));
        let to = self.base.wrapping_add((index as i64).wrapping_add(1).wrapping_mul(self.stride));
        let weight = (to.min(self.to).wrapping_sub(from.max(self.from))) as f64 / self.to.wrapping_sub(self.from) as f64;
        (from, to, weight)
    }

    pub(crate) fn nth(self, index: usize) -> Value {
        let (from, to, weight) = self.cell(index);
        Value::Struct(StructFields::from([("_from".into(),timestamp(from,U::Microsecond,Some("UTC".into()))),("_to".into(),timestamp(to,U::Microsecond,Some("UTC".into()))),("_weight".into(),Value::F64(weight))]))
    }

    pub(crate) fn arrow_column(self, length: usize, scope: &crate::EvaluationScope)
        -> Result<(arrow_schema::FieldRef, arrow_array::ArrayRef)>
    {
        use std::sync::Arc;
        use arrow_array::{Float64Array, StructArray, TimestampMicrosecondArray};
        use arrow_schema::DataType;

        scope.reserve(length.checked_mul(24).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?)?;
        let field = Arc::new(rtdb_columnar::to_field("value", &rtdb_plan::range_bin_element_type())?);
        let DataType::Struct(children) = field.data_type() else { unreachable!("range bin is a struct") };
        let from = TimestampMicrosecondArray::from_iter_values((0..length).map(|index| self.cell(index).0)).with_timezone("UTC");
        let to = TimestampMicrosecondArray::from_iter_values((0..length).map(|index| self.cell(index).1)).with_timezone("UTC");
        let weight = Float64Array::from_iter_values((0..length).map(|index| self.cell(index).2));
        let array = StructArray::try_new(children.clone(), vec![Arc::new(from), Arc::new(to), Arc::new(weight)], None)
            .map_err(|error| rtdb_types::Anomaly::fault("rtdb.expr/range-bin", error.to_string()))?;
        Ok((field, Arc::new(array)))
    }

    pub(crate) fn reserve(self,context: &EvalContext<'_>) -> Result<Vec<Value>> {
        let length = self.len()?;
        context.reserve(length.checked_mul(4096).ok_or_else(overflow)?)?;
        let mut out = Vec::new();out.try_reserve_exact(length).map_err(|_|overflow())?;Ok(out)
    }
    pub(crate) fn materialize(self,context: &EvalContext<'_>) -> Result<Value> {
        let mut out = self.reserve(context)?;out.extend((0..self.len()?).map(|index|self.nth(index)));Ok(Value::list(out))
    }
    pub(crate) async fn materialize_async(self,context: &EvalContext<'_>) -> Result<Value> {
        let mut out = self.reserve(context)?;
        for index in 0..self.len()? { out.push(self.nth(index));if index % 1024 == 1023 { tokio::task::yield_now().await; } }
        Ok(Value::list(out))
    }
}
