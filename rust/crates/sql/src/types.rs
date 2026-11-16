use rtdb_types::{LogicalType, Result, ScalarType, TimeUnit, Value};
use sqlparser::ast as ast;

use crate::bind::{error, identifiers, unsupported};

pub(crate) fn cast_target(data_type: &ast::DataType) -> Result<rtdb_plan::CastTarget> {
    use ast::{DataType as D, ExactNumberInfo as N};
    match data_type {
        D::Numeric(info) | D::Decimal(info) | D::Dec(info) => {
            let (precision, scale) = match info { N::None => (None, None), N::Precision(p) => (Some(*p), None), N::PrecisionAndScale(p, s) => (Some(*p), Some(*s)) };
            if scale.is_some_and(|scale| scale < 0) { return Err(error("SQL decimal scale must be unsigned")); }
            rtdb_plan::CastTarget::decimal(precision, scale)
        }
        D::Array(element) => {
            let element = match element { ast::ArrayElemTypeDef::AngleBracket(element) | ast::ArrayElemTypeDef::SquareBracket(element, _) | ast::ArrayElemTypeDef::Parenthesis(element) | ast::ArrayElemTypeDef::Qualified(element, _) => element, _ => return Err(error("Array cast requires an element type")) };
            Ok(rtdb_plan::CastTarget::List(Box::new(cast_target(element)?)))
        }
        other => Ok(rtdb_plan::CastTarget::fixed(self::data_type(other)?)),
    }
}

pub(crate) fn data_type(data_type: &ast::DataType) -> Result<LogicalType> {
    use ast::DataType as D;
    let scalar = match data_type {
        D::Boolean | D::Bool => ScalarType::Bool,
        D::SmallInt(_) | D::Int2(_) => ScalarType::I16,
        D::Int(_) | D::Integer(_) | D::Int4(_) => ScalarType::I32,
        D::BigInt(_) | D::Int8(_) => ScalarType::I64,
        D::Float(_) | D::Real | D::Float4 => ScalarType::F32,
        D::Double(_) | D::DoublePrecision | D::Float8 => ScalarType::F64,
        D::Numeric(info) | D::Decimal(info) | D::Dec(info) => decimal_type(info)?,
        D::Character(_) | D::Char(_) | D::CharacterVarying(_) | D::CharVarying(_) | D::Varchar(_) | D::Text => ScalarType::Utf8,
        D::Binary(_) | D::Varbinary(_) | D::Bytea => ScalarType::Binary,
        D::Uuid => ScalarType::Uuid,
        D::Regclass => ScalarType::RegClass,
        D::Date => ScalarType::DateDay,
        D::Time(precision, timezone) => {
            if matches!(timezone, ast::TimezoneInfo::WithTimeZone | ast::TimezoneInfo::Tz) { return Err(unsupported("TIME WITH TIME ZONE cast")); }
            ScalarType::Time { unit: time_unit(*precision)?, bit_width: 64 }
        }
        D::Timestamp(precision, timezone) => ScalarType::Timestamp { unit: time_unit(*precision)?, timezone: matches!(timezone, ast::TimezoneInfo::WithTimeZone | ast::TimezoneInfo::Tz).then(|| "UTC".to_owned()) },
        D::Array(element) => {
            let element = match element { ast::ArrayElemTypeDef::AngleBracket(element) | ast::ArrayElemTypeDef::SquareBracket(element, _) | ast::ArrayElemTypeDef::Parenthesis(element) | ast::ArrayElemTypeDef::Qualified(element, _) => element, _ => return Err(error("Array cast requires an element type")) };
            return Ok(LogicalType::list(self::data_type(element)?));
        }
        D::Custom(name, arguments) => {
            let names = identifiers(name)?;
            if names.len() != 1 { return Err(unsupported("Qualified cast type")); }
            match names[0].normalized().as_str() {
                "keyword" => ScalarType::Keyword, "uri" => ScalarType::Uri, "oid" => ScalarType::Oid,
                "regclass" => ScalarType::RegClass, "regproc" => ScalarType::RegProc,
                "duration" => ScalarType::Duration(time_unit(arguments.first().map(|value| value.parse::<u64>().map_err(error)).transpose()?)?),
                "timestamptz" => ScalarType::Timestamp { unit: time_unit(arguments.first().map(|value| value.parse::<u64>().map_err(error)).transpose()?)?, timezone: Some("UTC".into()) },
                "bpchar" | "name" => ScalarType::Utf8,
                _ => return Err(unsupported(format!("Cast type: {data_type}"))),
            }
        }
        _ => return Err(unsupported(format!("Cast type: {data_type}"))),
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
    Ok(ScalarType::Decimal { precision: precision as u8, scale: i32::try_from(scale).map_err(error)?, bit_width: if precision <= 32 { 128 } else { 256 } })
}

fn time_unit(precision: Option<u64>) -> Result<TimeUnit> {
    match precision { Some(10..) => Err(error("Temporal precision cannot exceed nine digits")), Some(7..=9) => Ok(TimeUnit::Nanosecond), _ => Ok(TimeUnit::Microsecond) }
}

pub(crate) fn literal(value: &ast::Value) -> Result<Value> {
    Ok(match value {
        ast::Value::Null => Value::Null,
        ast::Value::Boolean(value) => Value::Bool(*value),
        ast::Value::Number(value, _) if value.contains(['.', 'e', 'E']) => Value::F64(value.parse().map_err(error)?),
        ast::Value::Number(value, _) => Value::I64(value.parse().map_err(error)?),
        ast::Value::SingleQuotedString(value) | ast::Value::EscapedStringLiteral(value) | ast::Value::UnicodeStringLiteral(value) | ast::Value::NationalStringLiteral(value) => Value::Utf8(value.clone()),
        ast::Value::DollarQuotedString(value) => Value::Utf8(value.value.clone()),
        ast::Value::HexStringLiteral(value) => {
            if value.len() % 2 != 0 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) { return Err(error("Binary literals require complete hexadecimal bytes")); }
            Value::Binary((0..value.len()).step_by(2).map(|index| u8::from_str_radix(&value[index..index + 2], 16).map_err(error)).collect::<Result<_>>()?)
        }
        _ => return Err(unsupported(format!("Literal: {value}"))),
    })
}
