use rtdb_types::{Anomaly, IntervalUnit, LogicalType, Result, ScalarType, TimeUnit};
use serde::{Deserialize, Serialize};

/// Zone metadata for a cast result.
/// `Session` is reserved for retained control expressions; query planners bind it to an explicit zone before advertising a schema.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CastTimezone { Explicit(String), Session }

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TemporalKind { Time, TimestampLocal, TimestampZone(CastTimezone), Duration }

/// A temporal cast request with optional fractional-second precision.
/// The request retains out-of-range precision until validation knows whether a source leg uses it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TemporalCast {
    pub kind: TemporalKind,
    pub unit: TimeUnit,
    pub precision: Option<i64>,
}

impl TemporalCast {
    /// Returns result metadata, resolving `Session` from the supplied execution zone.
    /// The zone is metadata; local-to-instant conversion uses the evaluator's session zone separately.
    pub fn data_type(&self, session_timezone: &str) -> LogicalType {
        LogicalType::Scalar(match &self.kind {
            TemporalKind::Time => ScalarType::Time { unit: self.unit, bit_width: if self.unit < TimeUnit::Microsecond { 32 } else { 64 } },
            TemporalKind::TimestampLocal => ScalarType::Timestamp { unit: self.unit, timezone: None },
            TemporalKind::TimestampZone(zone) => ScalarType::Timestamp { unit: self.unit, timezone: Some(match zone { CastTimezone::Explicit(zone) => zone, CastTimezone::Session => session_timezone }.to_owned()) },
            TemporalKind::Duration => ScalarType::Duration(self.unit),
        })
    }

    /// Validates precision only for text and interval-to-duration casts.
    /// Null and unknown source legs defer validation; unsupported interval duration families return `invalid-cast`.
    pub fn validate_source(&self, source: &LogicalType) -> Result<()> {
        for leg in source.legs() {
            if matches!(self.kind, TemporalKind::Time) && matches!(leg, LogicalType::Scalar(ScalarType::DateDay | ScalarType::DateMilli)) {
                return Err(Anomaly::incorrect("xtdb.expression/cast-error", format!("Unsupported cast: {leg:?} -> {:?}", self.data_type("UTC"))));
            }
            if matches!(self.kind, TemporalKind::Duration) {
                if let LogicalType::Scalar(ScalarType::Interval(unit @ (IntervalUnit::YearMonth | IntervalUnit::DayTime))) = leg {
                    let unit = if unit == IntervalUnit::YearMonth { "year-month" } else { "day-time" };
                    return Err(Anomaly::incorrect("xtdb.expression/invalid-cast", format!("Cannot cast a {unit} interval to a duration")).with_context("interval-unit", unit.into()));
                }
            }
            let uses_precision = matches!(leg, LogicalType::Scalar(ScalarType::Utf8))
                || matches!(self.kind, TemporalKind::Duration) && matches!(leg, LogicalType::Scalar(ScalarType::Interval(_) | ScalarType::IntervalMonthDayMicro));
            if uses_precision {
                if let Some(precision) = self.precision { fractional_precision(precision, false)?; }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntervalField { Year, Month, Day, Hour, Minute, Second }

impl IntervalField {
    pub fn name(self) -> &'static str { match self { Self::Year => "YEAR", Self::Month => "MONTH", Self::Day => "DAY", Self::Hour => "HOUR", Self::Minute => "MINUTE", Self::Second => "SECOND" } }
    /// Parses the uppercase qualifier field names used by SQL's expression lowering.
    /// Unknown names return `xtdb.expression/invalid-interval-units`.
    pub fn parse(name: &str) -> Result<Self> {
        match name { "YEAR" => Ok(Self::Year), "MONTH" => Ok(Self::Month), "DAY" => Ok(Self::Day), "HOUR" => Ok(Self::Hour), "MINUTE" => Ok(Self::Minute), "SECOND" => Ok(Self::Second), _ => Err(Anomaly::incorrect("xtdb.expression/invalid-interval-units", format!("Unknown interval field: {name}"))) }
    }
}

/// A requested qualifier; validation depends on the source cast's dispatch method.
/// Integer casts ignore precision options, whereas textual and interval inputs validate the options they use.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IntervalQualifier {
    pub start: IntervalField,
    pub end: Option<IntervalField>,
    pub leading_precision: Option<i64>,
    pub fractional_precision: Option<i64>,
}

impl IntervalQualifier {
    pub fn year_month(&self) -> bool { matches!(self.start, IntervalField::Year | IntervalField::Month) }
    /// Computes the output representation for one nonnull source leg.
    /// Call `validate_source` before execution; integer and single-field text casts retain micro-interval metadata.
    pub fn result_type(&self, source: &LogicalType) -> LogicalType {
        let scalar = if self.year_month() { ScalarType::Interval(IntervalUnit::YearMonth) }
            else if (self.end.is_none() && matches!(source, LogicalType::Scalar(ScalarType::Utf8))) || is_integer(source) || self.fractional_precision.unwrap_or(0) <= 6 { ScalarType::IntervalMonthDayMicro }
            else { ScalarType::Interval(IntervalUnit::MonthDayNano) };
        LogicalType::Scalar(scalar)
    }
    /// Checks required precisions, permitted field ranges, and year-month source compatibility.
    /// Integer casts use only the field; null and unknown legs defer the checks.
    pub fn validate_source(&self, source: &LogicalType) -> Result<()> {
        self.validate_before_metadata(source)?;
        for leg in source.legs().into_iter().filter(|leg| !matches!(leg, LogicalType::Null | LogicalType::Nothing)) {
            let needs_fraction = !self.year_month() && !is_integer(&leg)
                && (self.end.is_some() || !matches!(leg, LogicalType::Scalar(ScalarType::Utf8)) || self.start == IntervalField::Second);
            if needs_fraction && self.fractional_precision.is_none() {
                return Err(Anomaly::incorrect("xtdb.expression/invalid-interval-fractional-precision", "Interval cast requires fractional-precision"));
            }
        }
        Ok(())
    }
    pub(crate) fn validate_before_fraction(&self, source: &LogicalType) -> Result<()> {
        for leg in source.legs().into_iter().filter(|leg| !matches!(leg, LogicalType::Null | LogicalType::Nothing)) {
            if is_integer(&leg) {
                if let Some(end) = self.end { return Err(Anomaly::unsupported("xtdb.expression/attempting-to-cast-int-to-multi-field-interval", "Cannot cast integer to a multi field interval").with_context("start-field", self.start.name().into()).with_context("end-field", end.name().into())); }
                continue;
            }
            if matches!(leg, LogicalType::Scalar(ScalarType::Duration(_))) && self.year_month() { return Err(Anomaly::incorrect("xtdb.expression/unsupported-cast", "Cannot cast a duration to a year-month interval")); }
            if matches!(leg, LogicalType::Scalar(ScalarType::Utf8)) { self.validate_units()?; }
            let leading = self.leading_precision.ok_or_else(|| Anomaly::incorrect("xtdb.expression/invalid-interval-precision", "Interval cast requires leading-precision"))?;
            if !(1..=8).contains(&leading) { return Err(Anomaly::incorrect("xtdb.expression/invalid-interval-precision", if leading < 1 { "The minimum leading field precision is 1." } else { "The maximum leading field precision is 8." }).with_context("precision", leading.into())); }
            if !matches!(leg, LogicalType::Scalar(ScalarType::Utf8)) { self.validate_units()?; }
        }
        Ok(())
    }
    pub(crate) fn validate_before_metadata(&self, source: &LogicalType) -> Result<()> {
        use IntervalField as F;
        self.validate_before_fraction(source)?;
        for leg in source.legs().into_iter().filter(|leg| !matches!(leg, LogicalType::Null | LogicalType::Nothing) && !is_integer(leg)) {
            if self.end == Some(F::Second) || self.start == F::Second && matches!(leg, LogicalType::Scalar(ScalarType::Utf8)) {
                let fraction = self.fractional_precision.ok_or_else(|| Anomaly::incorrect("xtdb.expression/invalid-interval-fractional-precision", "Interval cast requires fractional-precision"))?;
                fractional_precision(fraction, true)?;
            }
            if matches!(&leg, LogicalType::Scalar(ScalarType::Interval(_) | ScalarType::IntervalMonthDayMicro)) {
                let source_ym = matches!(leg, LogicalType::Scalar(ScalarType::Interval(IntervalUnit::YearMonth)));
                if self.year_month() != source_ym { return Err(Anomaly::unsupported("xtdb.expression/unsupported-cast", if source_ym { "Cannot cast a Year-Month interval with a non Year-Month interval qualifier" } else { "Cannot cast a non Year-Month interval with a Year-Month interval qualifier" })); }
            }
        }
        Ok(())
    }
    pub(crate) fn validate_units(&self) -> Result<()> {
        use IntervalField as F;
        let Some(end) = self.end else { return Ok(()); };
        let valid = matches!((self.start, end), (F::Year,F::Month) | (F::Day,F::Hour | F::Minute | F::Second) | (F::Hour,F::Minute | F::Second) | (F::Minute,F::Second));
        if valid { return Ok(()); }
        let message = match self.start { F::Year => "If YEAR specified as the interval start field, MONTH must be the end field.", F::Month => "MONTH is not permitted as the interval start field.", _ => "Interval end field must have less significance than the start field." };
        Err(Anomaly::incorrect("xtdb.expression/invalid-interval-units",message).with_context("start-unit",self.start.name().into()).with_context("end-unit",end.name().into()))
    }
}

fn is_integer(source: &LogicalType) -> bool { matches!(source, LogicalType::Scalar(ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64 | ScalarType::U8 | ScalarType::U16 | ScalarType::U32 | ScalarType::U64)) }

/// Checks a fractional-second precision in 0 through 9.
/// `interval` selects the interval-specific error code; the offending value is retained in error context.
pub fn fractional_precision(precision: i64, interval: bool) -> Result<u32> {
    if !(0..=9).contains(&precision) { return Err(Anomaly::incorrect(if interval { "xtdb.expression/invalid-interval-fractional-precision" } else { "xtdb.expression/invalid-fractional-precision" }, if precision < 0 { "The minimum fractional seconds precision is 0." } else { "The maximum fractional seconds precision is 9." }).with_context("fractional-precision", precision.into())); }
    Ok(precision as u32)
}
