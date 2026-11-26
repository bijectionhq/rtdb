use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, TimeUnit};
use serde::{Deserialize, Serialize};
use crate::{CastTarget, CastTimezone, IntervalField, IntervalQualifier, TemporalCast, TemporalKind};

/// An unevaluated cast-option atom.
/// Numeric reader adapters retain primitive `Number.longValue` and checked `long` conversions separately.
/// Conversion errors remain deferred until a source leg consumes the option.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CastOption {
    Null,
    Boolean(bool),
    Integer(i64),
    Numeric { primitive: i64, checked: Option<i64>, form: String },
    Character { value: u32, form: String },
    String(String),
    Other(String),
}

impl CastOption {
    /// Uses Clojure truthiness: only nil and false are false.
    pub fn truthy(&self) -> bool { !matches!(self, Self::Null | Self::Boolean(false)) }
    /// Borrows a string-valued field name without coercing other reader forms.
    pub fn string(&self) -> Option<&str> { if let Self::String(value) = self { Some(value) } else { None } }
    /// Returns the reader's `Number.longValue` conversion or an option-type error.
    /// Call only for options consumed by the selected source dispatch.
    pub fn long(&self) -> Result<i64> {
        match self {
            Self::Integer(value) | Self::Numeric { primitive: value, .. } => Ok(*value),
            _ => Err(Anomaly::incorrect("xtql/malformed-cast", "Cast numeric option requires a number")),
        }
    }
    /// Performs the checked coercion used by explicit alias metadata conversion and generated temporal calls.
    pub fn checked_long(&self) -> Result<i64> {
        match self {
            Self::Numeric { checked: Some(value), .. } => Ok(*value),
            Self::Numeric { checked: None, .. } => Err(Anomaly::incorrect("xtdb.expression/overflow-error", "Cast option exceeds signed 64 bits")),
            Self::Character { value, .. } if *value <= u16::MAX as u32 => Ok(i64::from(*value)),
            other => other.long(),
        }
    }
    /// Counts the atom and its owned string capacity, in bytes.
    pub fn retained_bytes(&self) -> usize {
        std::mem::size_of::<Self>() + match self { Self::Numeric { form, .. } | Self::Character { form, .. } | Self::String(form) | Self::Other(form) => form.capacity(), _ => 0 }
    }
    /// Returns a readable EDN atom for plan rendering, preserving ignored option values.
    pub fn to_edn(&self) -> String {
        match self {
            Self::Null => "nil".into(), Self::Boolean(value) => value.to_string(), Self::Integer(value) => value.to_string(),
            Self::Numeric { form, .. } | Self::Character { form, .. } | Self::Other(form) => form.clone(),
            Self::String(value) => serde_json::to_string(value).expect("strings serialize"),
        }
    }
}

/// XTQL cast options retained before a parameter or union source leg is known.
/// `resolve` consumes only the options used by one nonnull, non-unknown leg and returns a typed execution descriptor.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum DeferredCast {
    Temporal { kind: TemporalKind, unit: TimeUnit, precision: CastOption },
    Interval { start: CastOption, end: CastOption, leading: CastOption, fractional: CastOption },
}

impl DeferredCast {
    /// Captures a session zone for query result metadata; explicit zones and interval requests remain unchanged.
    pub fn bind_timezone(&self, timezone: &str) -> Self {
        match self {
            Self::Temporal { kind: TemporalKind::TimestampZone(CastTimezone::Session), unit, precision } => Self::Temporal { kind: TemporalKind::TimestampZone(CastTimezone::Explicit(timezone.into())), unit: *unit, precision: precision.clone() },
            other => other.clone(),
        }
    }
    /// Reports time-to-timestamp requests whose execution needs the query clock's calendar date.
    pub fn requires_current_time(&self, source: &LogicalType) -> bool {
        matches!(self, Self::Temporal { kind: TemporalKind::TimestampLocal | TemporalKind::TimestampZone(_), .. })
            && source.legs().iter().any(|source| matches!(source, LogicalType::Scalar(ScalarType::Time { .. })))
    }
    /// Counts owned request storage in bytes, returning an allocation-overflow fault if the sum cannot fit.
    pub fn retained_bytes(&self) -> Result<usize> {
        let values: &[&CastOption] = match self { Self::Temporal { precision, .. } => &[precision], Self::Interval { start, end, leading, fractional } => &[start, end, leading, fractional] };
        let zone = match self { Self::Temporal { kind: TemporalKind::TimestampZone(CastTimezone::Explicit(zone)), .. } => zone.capacity(), _ => 0 };
        values.iter().try_fold(zone + std::mem::size_of::<Self>(), |bytes, value| bytes.checked_add(value.retained_bytes()).ok_or_else(|| Anomaly::fault("rtdb.plan/allocation-overflow", "Cast options exceed addressable memory")))
    }
    /// Resolves one source leg; null/unknown legs return their identity target without reading options.
    /// Invalid fields or consumed numeric options return an error before execution.
    pub fn resolve(&self, source: &LogicalType) -> Result<CastTarget> {
        if matches!(source, LogicalType::Null | LogicalType::Nothing) { return Ok(CastTarget::Fixed(source.clone())); }
        let target = match self {
            Self::Temporal { kind, unit, precision } => {
                TemporalCast { kind: kind.clone(), unit: *unit, precision: None }.validate_source(source)?;
                let uses_precision = matches!(source, LogicalType::Scalar(ScalarType::Utf8))
                    || matches!(kind, TemporalKind::Duration) && matches!(source, LogicalType::Scalar(ScalarType::IntervalMonthDayMicro | ScalarType::Interval(_)));
                let precision = if uses_precision && precision.truthy() { Some(precision.long()?) } else { None };
                CastTarget::Temporal(TemporalCast { kind: kind.clone(), unit: *unit, precision })
            }
            Self::Interval { start, end, leading, fractional } => {
                let integer = matches!(source, LogicalType::Scalar(ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64 | ScalarType::U8 | ScalarType::U16 | ScalarType::U32 | ScalarType::U64));
                if integer && end.truthy() { return Err(Anomaly::unsupported("xtdb.expression/attempting-to-cast-int-to-multi-field-interval", "Cannot cast integer to a multi field interval")); }
                let field = |value: &CastOption| value.string().ok_or_else(|| Anomaly::incorrect("xtql/malformed-cast", "Interval qualifier fields require strings")).and_then(IntervalField::parse);
                let start = field(start)?;
                if matches!(source, LogicalType::Scalar(ScalarType::Duration(_))) && matches!(start, IntervalField::Year | IntervalField::Month) {
                    IntervalQualifier { start, end: None, leading_precision: None, fractional_precision: None }.validate_source(source)?;
                }
                let end = if matches!(end, CastOption::Null) || integer && !end.truthy() { None } else { Some(field(end)?) };
                let text = matches!(source, LogicalType::Scalar(ScalarType::Utf8));
                let mut qualifier = IntervalQualifier { start, end, leading_precision: None, fractional_precision: None };
                if text { qualifier.validate_units()?; }
                qualifier.leading_precision = if integer { None } else { Some(leading.long()?) };
                qualifier.validate_before_fraction(source)?;
                let validates_fraction = !integer && (end == Some(IntervalField::Second) || text && start == IntervalField::Second);
                qualifier.fractional_precision = if validates_fraction { Some(fractional.long()?) } else { None };
                qualifier.validate_before_metadata(source)?;
                if !integer && !qualifier.year_month() && (!text || end.is_some() || start == IntervalField::Second) && !validates_fraction {
                    qualifier.fractional_precision = Some(fractional.long()?);
                }
                CastTarget::Interval(qualifier)
            }
        };
        target.validate_source(source)?;
        Ok(target)
    }
    /// Resolves the descriptor for a value after source validation.
    /// Generated temporal precision calls use checked coercion; interval normalization consumes the primitive conversion.
    pub fn resolve_value(&self, source: &LogicalType) -> Result<CastTarget> {
        let mut target = self.resolve(source)?;
        if let (Self::Temporal { precision, .. }, CastTarget::Temporal(typed)) = (self, &mut target) {
            if typed.precision.is_some() { typed.precision = Some(precision.checked_long()?); }
        }
        Ok(target)
    }
}
