use rtdb_types::{Anomaly, ListKind, LogicalType, Result, ScalarType};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CastTarget {
    Fixed(LogicalType),
    DecimalDefault,
    IntervalDefault,
    IntervalEmptyOptions,
    Temporal(crate::TemporalCast),
    Interval(crate::IntervalQualifier),
    Deferred(crate::DeferredCast),
    List(Box<CastTarget>),
    Checked { requested: LogicalType, target: Box<CastTarget> },
}

impl CastTarget {
    /// Binds session-zone result metadata before a query's schema is published.
    /// Retained control expressions keep the unbound target and read their evaluation context instead.
    pub fn bind_timezone(&self, timezone: &str) -> Self {
        match self {
            Self::Deferred(target) => Self::Deferred(target.bind_timezone(timezone)),
            Self::Temporal(crate::TemporalCast { kind: crate::TemporalKind::TimestampZone(crate::CastTimezone::Session), unit, precision }) => Self::Temporal(crate::TemporalCast { kind: crate::TemporalKind::TimestampZone(crate::CastTimezone::Explicit(timezone.into())), unit: *unit, precision: *precision }),
            Self::Checked { requested, target } => Self::Checked { requested: requested.clone(), target: Box::new(target.bind_timezone(timezone)) },
            Self::List(element) => Self::List(Box::new(element.bind_timezone(timezone))),
            other => other.clone(),
        }
    }
    /// Reports casts whose value depends on the query clock's calendar date.
    /// Submission-time constant expansion must leave these expressions for transaction resolution.
    pub fn requires_current_time(&self, source: &LogicalType) -> bool {
        match self {
            Self::Deferred(target) => target.requires_current_time(source),
            Self::Temporal(crate::TemporalCast { kind: crate::TemporalKind::TimestampLocal | crate::TemporalKind::TimestampZone(_), .. }) => source.legs().iter().any(|source| matches!(source, LogicalType::Scalar(ScalarType::Time { .. }))),
            Self::Checked { target, .. } => target.requires_current_time(source),
            Self::List(element) => source.legs().iter().any(|source| match source { LogicalType::List { element: source, .. } => element.requires_current_time(source), _ => false }),
            _ => false,
        }
    }
    /// Checks the expression dispatch contract against each nonnull source leg.
    /// Unknown and null-only sources defer validation until a nonnull value is supplied.
    /// Unsupported fallback identity casts return `xtdb.expression/cast-error`.
    pub fn validate_source(&self, source: &LogicalType) -> Result<()> {
        match self {
            Self::Deferred(target) => { for leg in source.legs() { target.resolve(&leg)?; } Ok(()) },
            Self::Temporal(target) => target.validate_source(source),
            Self::Interval(target) => target.validate_source(source),
            Self::IntervalEmptyOptions => if source.legs().iter().any(|leg| matches!(leg, LogicalType::Scalar(ScalarType::Duration(_)))) { Err(Anomaly::incorrect("xtdb.expression/invalid-interval-precision", "Duration interval cast options require a qualifier")) } else { Ok(()) },
            Self::Checked { requested, target } => {
                let dispatch = requested.unencoded();
                for leg in source.legs().into_iter().filter(|leg| *leg != LogicalType::Null) {
                    if leg != *requested && !specialized_cast(&leg, &dispatch) {
                        return Err(Anomaly::incorrect("xtdb.expression/cast-error", format!("Unsupported cast: {leg:?} -> {requested:?}")));
                    }
                }
                target.validate_source(source)
            }
            Self::List(element) => {
                for leg in source.legs() {
                    match leg {
                        LogicalType::List { kind: ListKind::List, element: source } if element.union_target().is_none() => element.validate_source(&source)?,
                        LogicalType::Scalar(ScalarType::Utf8) => element.validate_source(&LogicalType::Scalar(ScalarType::Utf8).maybe())?,
                        _ => (),
                    }
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Returns a list-element union target whose values are remapped without scalar conversion.
    /// Physical encodings are retained in the borrowed target; `legs` supplies its plain value kinds.
    pub fn union_target(&self) -> Option<&LogicalType> {
        match self {
            Self::Fixed(target @ (LogicalType::Maybe(_) | LogicalType::Union(_))) => Some(target),
            Self::Checked { requested, .. } => {
                let value = match requested { LogicalType::RunEndEncoded { value, .. } => value.as_ref(), value => value };
                matches!(value, LogicalType::Maybe(_) | LogicalType::Union(_)).then_some(requested)
            }
            _ => None,
        }
    }

    /// Normalizes expression targets recursively through lists.
    /// An interval without a qualifier preserves interval inputs and derives duration/text result metadata from the source.
    pub fn fixed(data_type: LogicalType) -> Self {
        match data_type {
            LogicalType::Scalar(ScalarType::Interval(_) | ScalarType::IntervalMonthDayMicro) => Self::IntervalDefault,
            LogicalType::List { kind: ListKind::List, element } => Self::List(Box::new(Self::fixed(*element))),
            other => Self::Fixed(other),
        }
    }

    /// An absent precision preserves existing decimal legs and uses decimal(64,9) for other values.
    /// Scale without precision has no effect, matching the expression cast options.
    /// Explicit precision above 64 and parameters outside signed 32 bits return an error.
    /// Nonpositive precision is valid metadata but cannot accept a nonnull value.
    pub fn decimal(precision: Option<i64>, scale: Option<i64>) -> Result<Self> {
        let Some(precision) = precision else { return Ok(Self::DecimalDefault); };
        if precision > 64 { return Err(Anomaly::incorrect("xtdb.expression/unsupported-precision", format!("Unsupported precision: {precision}"))); }
        let precision = i32::try_from(precision).map_err(|_| Anomaly::incorrect("xtdb.expression/overflow-error", "Decimal precision exceeds signed 32 bits"))?;
        let scale = i32::try_from(scale.unwrap_or(0)).map_err(|_| Anomaly::incorrect("xtdb.expression/overflow-error", "Decimal scale exceeds signed 32 bits"))?;
        Ok(Self::Fixed(LogicalType::Scalar(ScalarType::Decimal { precision, scale, bit_width: if precision <= 32 { 128 } else { 256 } })))
    }

    /// Computes each source leg's result, retaining nested nullability and decimal scale distinctions.
    /// Query planners must call `bind_timezone` before publishing a schema; unbound control targets use UTC only for scalar type inference.
    pub fn result_type(&self, source: &LogicalType) -> LogicalType {
        if *source == LogicalType::Nothing { return LogicalType::Nothing; }
        let results = source.legs().into_iter().map(|source| {
            if source == LogicalType::Null { return source; }
            match self {
                Self::Deferred(target) => target.resolve(&source).map(|target| target.result_type(&source)).unwrap_or(LogicalType::Nothing),
                Self::Checked { target, .. } => target.result_type(&source),
                Self::Fixed(target) => target.clone(),
                Self::DecimalDefault => if matches!(source, LogicalType::Scalar(ScalarType::Decimal { .. })) { source } else { default_decimal_type() },
                Self::Temporal(target) => target.data_type("UTC"),
                Self::Interval(target) => target.result_type(&source),
                Self::IntervalDefault | Self::IntervalEmptyOptions => match source {
                    LogicalType::Scalar(ScalarType::Interval(_) | ScalarType::IntervalMonthDayMicro) => source,
                    LogicalType::Scalar(ScalarType::Duration(rtdb_types::TimeUnit::Nanosecond)) => LogicalType::Scalar(ScalarType::Interval(rtdb_types::IntervalUnit::MonthDayNano)),
                    _ => LogicalType::Scalar(ScalarType::IntervalMonthDayMicro),
                },
                Self::List(element) => {
                    if let Some(target) = element.union_target() {
                        return LogicalType::list(target.unencoded());
                    }
                    let source = match source {
                        LogicalType::List { element, .. } => *element,
                        LogicalType::Scalar(ScalarType::Utf8) => LogicalType::Scalar(ScalarType::Utf8).maybe(),
                        _ => LogicalType::Nothing,
                    };
                    LogicalType::list(element.result_type(&source))
                }
            }
        }).collect::<Vec<_>>();
        LogicalType::join_all(&results)
    }
}

// Dispatch families from xtdb.expression/codegen-cast and expression/{temporal,pg,uri}.clj.
fn specialized_cast(source: &LogicalType, target: &LogicalType) -> bool {
    use ScalarType as S;
    fn integer(value: &S) -> bool { matches!(value, S::I8 | S::I16 | S::I32 | S::I64 | S::U8 | S::U16 | S::U32 | S::U64) }
    fn numeric(value: &S) -> bool { integer(value) || matches!(value, S::F32 | S::F64 | S::Decimal { .. }) }
    match (source, target) {
        (_, LogicalType::Scalar(S::Utf8)) => true,
        (LogicalType::List { kind: ListKind::List, .. } | LogicalType::Scalar(S::Utf8), LogicalType::List { kind: ListKind::List, .. }) => true,
        (LogicalType::Scalar(from), LogicalType::Scalar(to)) => {
            if numeric(from) && numeric(to) { return true; }
            if integer(from) && matches!(to, S::Bool | S::Oid | S::RegClass | S::RegProc | S::Interval(_) | S::IntervalMonthDayMicro) { return true; }
            if matches!(from, S::Oid | S::RegClass | S::RegProc) && integer(to) { return true; }
            match (from, to) {
                (S::Utf8, S::I8 | S::I16 | S::I32 | S::I64 | S::F32 | S::F64 | S::Decimal { .. } | S::Uuid | S::Binary | S::Keyword | S::Uri | S::RegClass | S::RegProc | S::DateDay | S::DateMilli | S::Time { .. } | S::Timestamp { .. } | S::Duration(_) | S::Interval(_) | S::IntervalMonthDayMicro) => true,
                (S::RegClass | S::RegProc, S::Oid) => true,
                (S::DateDay | S::DateMilli, S::DateDay | S::DateMilli | S::Timestamp { .. }) => true,
                (S::Time { .. }, S::Time { .. } | S::Timestamp { .. } | S::Duration(_)) => true,
                (S::Timestamp { .. }, S::DateDay | S::DateMilli | S::Time { .. } | S::Timestamp { .. }) => true,
                (S::Duration(_), S::Duration(_) | S::Interval(_) | S::IntervalMonthDayMicro) => true,
                (S::Interval(_) | S::IntervalMonthDayMicro, S::Duration(_) | S::Interval(_) | S::IntervalMonthDayMicro) => true,
                (S::F64, S::Duration(_)) => true,
                _ => false,
            }
        }
        _ => false,
    }
}

/// The numeric and text decimal-cast result when no precision option is supplied.
pub fn default_decimal_type() -> LogicalType { LogicalType::Scalar(ScalarType::Decimal { precision: 64, scale: 9, bit_width: 256 }) }
