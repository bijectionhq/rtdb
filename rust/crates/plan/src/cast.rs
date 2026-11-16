use rtdb_types::{Anomaly, ListKind, LogicalType, Result, ScalarType};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CastTarget {
    Fixed(LogicalType),
    DecimalDefault,
    IntervalDefault,
    List(Box<CastTarget>),
}

impl CastTarget {
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
    /// Explicit precision above 64 and scales outside the native signed-byte metadata return an error.
    pub fn decimal(precision: Option<u64>, scale: Option<i64>) -> Result<Self> {
        let Some(precision) = precision else { return Ok(Self::DecimalDefault); };
        if precision > 64 { return Err(Anomaly::incorrect("xtdb.expression/unsupported-precision", format!("Unsupported precision: {precision}"))); }
        let scale = i8::try_from(scale.unwrap_or(0)).map_err(|_| Anomaly::unsupported("xtdb.expression/unsupported-scale", "Decimal scale exceeds the native Arrow representation"))?;
        Ok(Self::Fixed(LogicalType::Scalar(ScalarType::Decimal { precision: precision as u8, scale, bit_width: if precision <= 32 { 128 } else { 256 } })))
    }

    /// Computes each source leg's result, retaining nested nullability and decimal scale distinctions.
    pub fn result_type(&self, source: &LogicalType) -> LogicalType {
        if *source == LogicalType::Nothing { return LogicalType::Nothing; }
        let results = source.legs().into_iter().map(|source| {
            if source == LogicalType::Null { return source; }
            match self {
                Self::Fixed(target) => target.clone(),
                Self::DecimalDefault => if matches!(source, LogicalType::Scalar(ScalarType::Decimal { .. })) { source } else { default_decimal_type() },
                Self::IntervalDefault => match source {
                    LogicalType::Scalar(ScalarType::Interval(_) | ScalarType::IntervalMonthDayMicro) => source,
                    LogicalType::Scalar(ScalarType::Duration(rtdb_types::TimeUnit::Nanosecond)) => LogicalType::Scalar(ScalarType::Interval(rtdb_types::IntervalUnit::MonthDayNano)),
                    _ => LogicalType::Scalar(ScalarType::IntervalMonthDayMicro),
                },
                Self::List(element) => {
                    if let Self::Fixed(target @ (LogicalType::Union(_) | LogicalType::Maybe(_))) = element.as_ref() {
                        return LogicalType::list(target.clone());
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

/// The numeric and text decimal-cast result when no precision option is supplied.
pub fn default_decimal_type() -> LogicalType { LogicalType::Scalar(ScalarType::Decimal { precision: 64, scale: 9, bit_width: 256 }) }
