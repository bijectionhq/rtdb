use rtdb_types::{LogicalType, Result, ScalarType, TimeUnit, Value};
use sqlparser::ast as ast;

use crate::bind::{error, grammar_rejected, identifiers, unsupported};

pub(crate) fn cast_target(data_type: &ast::DataType, timezone: Option<&str>) -> Result<rtdb_plan::CastTarget> {
    use ast::{DataType as D, ExactNumberInfo as N};
    match data_type {
        D::Numeric(info) | D::Decimal(info) | D::Dec(info) => {
            let (precision, scale) = match info { N::None => (None, None), N::Precision(p) => (Some(*p), None), N::PrecisionAndScale(p, s) => (Some(*p), Some(*s)) };
            if scale.is_some_and(|scale| scale < 0) { return Err(error("SQL decimal scale must be unsigned")); }
            let precision = precision.map(|precision| {
                if precision > 64 { return Err(rtdb_types::Anomaly::incorrect("xtdb.expression/unsupported-precision", format!("Unsupported precision: {precision}"))); }
                Ok(precision as i64)
            }).transpose()?;
            rtdb_plan::CastTarget::decimal(precision, scale)
        }
        D::Array(element) => {
            let element = match element {
                ast::ArrayElemTypeDef::SquareBracket(element, _) | ast::ArrayElemTypeDef::Qualified(element, _) => element,
                _ => return Err(grammar_rejected(format!("Cast type {data_type}"))),
            };
            Ok(rtdb_plan::CastTarget::List(Box::new(cast_target(element, timezone)?)))
        }
        D::Time(precision, zone) | D::Timestamp(precision, zone) => {
            use rtdb_plan::{CastTimezone, TemporalCast, TemporalKind};
            let kind = if matches!(zone, ast::TimezoneInfo::WithTimeZone | ast::TimezoneInfo::Tz) { TemporalKind::TimestampZone(timezone.map_or(CastTimezone::Session, |zone| CastTimezone::Explicit(zone.into()))) }
                else if matches!(data_type, D::Time(..)) { TemporalKind::Time } else { TemporalKind::TimestampLocal };
            Ok(rtdb_plan::CastTarget::Temporal(TemporalCast { kind, unit: precision_unit(*precision), precision: precision.map(|precision| i64::try_from(precision).map_err(error)).transpose()? }))
        }
        D::Interval { fields, precision } => {
            use ast::IntervalFields as A;
            use rtdb_plan::{IntervalField as F, IntervalQualifier};
            let Some(fields) = fields else { return Ok(rtdb_plan::CastTarget::IntervalDefault); };
            let bound = |bound| match bound {
                ast::IntervalBound::Year => F::Year,
                ast::IntervalBound::Month => F::Month,
                ast::IntervalBound::Day => F::Day,
                ast::IntervalBound::Hour => F::Hour,
                ast::IntervalBound::Minute => F::Minute,
                ast::IntervalBound::Second => F::Second,
            };
            let (start, end) = match fields { A::Year => (F::Year,None), A::Month => (F::Month,None), A::Day => (F::Day,None), A::Hour => (F::Hour,None), A::Minute => (F::Minute,None), A::Second => (F::Second,None), A::YearToMonth => (F::Year,Some(F::Month)), A::DayToHour => (F::Day,Some(F::Hour)), A::DayToMinute => (F::Day,Some(F::Minute)), A::DayToSecond => (F::Day,Some(F::Second)), A::HourToMinute => (F::Hour,Some(F::Minute)), A::HourToSecond => (F::Hour,Some(F::Second)), A::MinuteToSecond => (F::Minute,Some(F::Second)), A::Range { start, end } => (bound(*start), Some(bound(*end))) };
            if precision.is_some() && start != F::Second && end != Some(F::Second) { return Err(error("Interval precision requires a SECOND field")); }
            Ok(rtdb_plan::CastTarget::Interval(IntervalQualifier { start, end, leading_precision: Some(2), fractional_precision: Some(precision.map(|precision| i64::try_from(precision).map_err(error)).transpose()?.unwrap_or(6)) }))
        }
        D::Custom(name, arguments) if identifiers(name)?.len() == 1 && matches!(identifiers(name)?[0].normalized().as_str(), "duration" | "timestamptz") => {
            use rtdb_plan::{CastTimezone, TemporalCast, TemporalKind};
            let normalized = identifiers(name)?[0].normalized();
            if normalized == "timestamptz" && !arguments.is_empty() { return Err(grammar_rejected(format!("Cast type {data_type}"))); }
            if arguments.len() > 1 { return Err(error("Temporal cast accepts one precision")); }
            let precision = arguments.first().map(|value| value.parse::<u64>().map_err(error)).transpose()?;
            let kind = if normalized == "duration" { TemporalKind::Duration } else { TemporalKind::TimestampZone(timezone.map_or(CastTimezone::Session, |zone| CastTimezone::Explicit(zone.into()))) };
            Ok(rtdb_plan::CastTarget::Temporal(TemporalCast { kind, unit: precision_unit(precision), precision: precision.map(|precision| i64::try_from(precision).map_err(error)).transpose()? }))
        }
        other => Ok(rtdb_plan::CastTarget::fixed(self::data_type(other)?)),
    }
}

fn precision_unit(precision: Option<u64>) -> TimeUnit { if precision.is_some_and(|precision| precision > 6) { TimeUnit::Nanosecond } else { TimeUnit::Microsecond } }

pub(crate) fn data_type(data_type: &ast::DataType) -> Result<LogicalType> {
    use ast::DataType as D;
    let scalar = match data_type {
        D::Boolean | D::Bool => ScalarType::Bool,
        D::SmallInt(None) | D::Int2(None) => ScalarType::I16,
        D::Int(None) | D::Integer(None) | D::Int4(None) => ScalarType::I32,
        D::BigInt(None) | D::Int8(None) => ScalarType::I64,
        D::Float(ast::ExactNumberInfo::None | ast::ExactNumberInfo::Precision(_)) | D::Real | D::Float4 => ScalarType::F32,
        D::Double(ast::ExactNumberInfo::None) | D::DoublePrecision | D::Float8 => ScalarType::F64,
        D::Numeric(info) | D::Decimal(info) | D::Dec(info) => decimal_type(info)?,
        D::Char(None) | D::Varchar(None) | D::Text => ScalarType::Utf8,
        D::Varbinary(None) | D::Bytea => ScalarType::Binary,
        D::Uuid => ScalarType::Uuid,
        D::Regclass => ScalarType::RegClass,
        D::Date => ScalarType::DateDay,
        D::Time(precision, timezone) => {
            if matches!(timezone, ast::TimezoneInfo::WithTimeZone | ast::TimezoneInfo::Tz) { return Err(unsupported("TIME WITH TIME ZONE cast")); }
            ScalarType::Time { unit: time_unit(*precision)?, bit_width: 64 }
        }
        D::Timestamp(precision, timezone) => ScalarType::Timestamp { unit: time_unit(*precision)?, timezone: matches!(timezone, ast::TimezoneInfo::WithTimeZone | ast::TimezoneInfo::Tz).then(|| "UTC".to_owned()) },
        D::Array(element) => {
            let element = match element {
                ast::ArrayElemTypeDef::SquareBracket(element, _) | ast::ArrayElemTypeDef::Qualified(element, _) => element,
                _ => return Err(grammar_rejected(format!("Cast type {data_type}"))),
            };
            return Ok(LogicalType::list(self::data_type(element)?));
        }
        D::Custom(name, arguments) => {
            let names = identifiers(name)?;
            if names.len() != 1 { return Err(grammar_rejected("A qualified cast type")); }
            let normalized = names[0].normalized();
            if normalized != "duration" && normalized != "row" && !arguments.is_empty() {
                return Err(grammar_rejected(format!("Cast type {data_type}")));
            }
            match normalized.as_str() {
                "keyword" => ScalarType::Keyword, "uri" => ScalarType::Uri, "oid" => ScalarType::Oid,
                "regclass" => ScalarType::RegClass, "regproc" => ScalarType::RegProc,
                "duration" => ScalarType::Duration(time_unit(arguments.first().map(|value| value.parse::<u64>().map_err(error)).transpose()?)?),
                "timestamptz" => ScalarType::Timestamp { unit: time_unit(arguments.first().map(|value| value.parse::<u64>().map_err(error)).transpose()?)?, timezone: Some("UTC".into()) },
                "bpchar" | "name" => ScalarType::Utf8,
                "row" => return Err(unsupported(format!("Cast type: {data_type}"))),
                _ => return Err(grammar_rejected(format!("Cast type {data_type}"))),
            }
        }
        D::Interval { .. } | D::Struct(..) | D::Tuple(_) => return Err(unsupported(format!("Cast type: {data_type}"))),
        _ => return Err(grammar_rejected(format!("Cast type {data_type}"))),
    };
    scalar.validate()?;
    Ok(LogicalType::Scalar(scalar))
}

fn decimal_type(info: &ast::ExactNumberInfo) -> Result<ScalarType> {
    let (precision, scale) = match info {
        ast::ExactNumberInfo::None => (64, 9),
        ast::ExactNumberInfo::Precision(precision) => (*precision, 0),
        ast::ExactNumberInfo::PrecisionAndScale(precision, scale) => (*precision, *scale),
    };
    if precision > 64 { return Err(rtdb_types::Anomaly::incorrect("xtdb.expression/unsupported-precision", format!("Unsupported precision: {precision}"))); }
    if scale < 0 { return Err(error("SQL decimal scale must be unsigned")); }
    Ok(ScalarType::Decimal { precision: precision as i32, scale: i32::try_from(scale).map_err(error)?, bit_width: if precision <= 32 { 128 } else { 256 } })
}

fn time_unit(precision: Option<u64>) -> Result<TimeUnit> {
    match precision { Some(10..) => Err(error("Temporal precision cannot exceed nine digits")), Some(7..=9) => Ok(TimeUnit::Nanosecond), _ => Ok(TimeUnit::Microsecond) }
}

pub(crate) fn literal(value: &ast::Value) -> Result<Value> {
    Ok(match value {
        ast::Value::Null => Value::Null,
        ast::Value::Boolean(value) => Value::Bool(*value),
        ast::Value::Number(value, true) => return Err(grammar_rejected(format!("Numeric literal {value}L"))),
        ast::Value::Number(value, false) if value.starts_with('.') || value.ends_with('.') || value.contains(".e") || value.contains(".E") =>
            return Err(grammar_rejected(format!("Numeric literal {value}"))),
        ast::Value::Number(value, false) if value.contains(['.', 'e', 'E']) => Value::F64(value.parse().map_err(error)?),
        ast::Value::Number(value, false) => Value::I64(value.parse().map_err(|_| error(format!("Cannot parse integer: {value}")))?),
        ast::Value::SingleQuotedString(value) | ast::Value::EscapedStringLiteral(value) => Value::Utf8(value.clone()),
        ast::Value::DollarQuotedString(value) => Value::Utf8(value.value.clone()),
        _ => return Err(grammar_rejected(format!("Literal {value}"))),
    })
}
