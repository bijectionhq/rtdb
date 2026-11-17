use std::collections::BTreeMap;

use rtdb_types::{Anomaly, IntervalUnit, ListKind, LogicalType, Result, ScalarType, TimeUnit};

use crate::generated::xtdb::cols::proto as pb;
use pb::vec_type::Type;

pub fn logical_type(column: &pb::ColumnMeta) -> Result<LogicalType> {
    type_from_proto(column.r#type.as_deref().ok_or_else(|| invalid("No column type recorded"))?)
}

pub fn column_from_type(ty: &LogicalType, hll: Option<Vec<u8>>) -> Result<pb::ColumnMeta> {
    Ok(pb::ColumnMeta { r#type: Some(Box::new(type_to_proto(ty)?)), hll })
}

pub fn type_from_proto(ty: &pb::VecType) -> Result<LogicalType> {
    let result = match ty.r#type.as_ref().ok_or_else(|| invalid("No vector type recorded"))? {
        Type::Simple(simple) => match *simple {
            0 => LogicalType::Nothing,
            1 => LogicalType::Null,
            2 => scalar(ScalarType::Bool),
            3 => scalar(ScalarType::I8),
            4 => scalar(ScalarType::I16),
            5 => scalar(ScalarType::I32),
            6 => scalar(ScalarType::I64),
            7 => scalar(ScalarType::F32),
            8 => scalar(ScalarType::F64),
            9 => scalar(ScalarType::Utf8),
            10 => scalar(ScalarType::Binary),
            11 => scalar(ScalarType::Keyword),
            12 => scalar(ScalarType::Transit),
            13 => scalar(ScalarType::Uri),
            14 => scalar(ScalarType::Uuid),
            15 => scalar(ScalarType::Oid),
            16 => scalar(ScalarType::RegClass),
            17 => scalar(ScalarType::RegProc),
            unknown => return Err(invalid(format!("Unknown scalar type: {unknown}"))),
        },
        Type::FixedSizeBinary(width) => scalar(ScalarType::FixedSizeBinary(*width)),
        Type::Decimal(decimal) => scalar(ScalarType::Decimal {
            precision: decimal.precision.unwrap_or_default(),
            scale: decimal.scale.unwrap_or_default(),
            bit_width: u16::try_from(decimal.bit_width.unwrap_or_default()).map_err(invalid)?,
        }),
        Type::Timestamp(time) => scalar(ScalarType::Timestamp {
            unit: decode_unit(time.unit.unwrap_or_default())?, timezone: time.timezone.clone(),
        }),
        Type::Date(date) => scalar(match date.unit.unwrap_or_default() {
            0 => ScalarType::DateDay,
            1 => ScalarType::DateMilli,
            unknown => return Err(invalid(format!("Unknown date unit: {unknown}"))),
        }),
        Type::Time(time) => scalar(ScalarType::Time {
            unit: decode_unit(time.unit.unwrap_or_default())?,
            bit_width: u8::try_from(time.bit_width.unwrap_or_default()).map_err(invalid)?,
        }),
        Type::Duration(unit) => scalar(ScalarType::Duration(decode_unit(*unit)?)),
        Type::Interval(interval) => scalar(match interval.unit.unwrap_or_default() {
            0 => ScalarType::Interval(IntervalUnit::YearMonth),
            1 => ScalarType::Interval(IntervalUnit::DayTime),
            2 => ScalarType::Interval(IntervalUnit::MonthDayNano),
            3 => ScalarType::IntervalMonthDayMicro,
            unknown => return Err(invalid(format!("Unknown interval unit: {unknown}"))),
        }),
        Type::List(list) => list_type(ListKind::List, list.el.as_deref())?,
        Type::Set(list) => list_type(ListKind::Set, list.el.as_deref())?,
        Type::TstzRange(list) => list_type(ListKind::TimestampTzRange, list.el.as_deref())?,
        Type::FixedSizeList(list) => list_type(ListKind::FixedSizeList(list.size.unwrap_or_default()), list.el.as_deref())?,
        Type::Map(map) => list_type(ListKind::Map { sorted: map.keys_sorted.unwrap_or_default() }, map.el.as_deref())?,
        Type::Struct(record) => LogicalType::Struct(record.children.iter().map(|(name, column)| Ok((name.clone(), logical_type(column)?))).collect::<Result<_>>()?),
        Type::Maybe(maybe) => {
            let mono = type_from_proto(maybe.mono.as_deref().ok_or_else(|| invalid("Nullable type has no monomorphic value"))?)?;
            if !matches!(mono, LogicalType::Scalar(_) | LogicalType::List { .. } | LogicalType::Struct(_)) {
                return Err(invalid("Nullable type requires a monomorphic value"));
            }
            LogicalType::Maybe(Box::new(mono))
        }
        Type::Poly(poly) => {
            let legs = poly.legs.iter().map(type_from_proto).collect::<Result<Vec<_>>>()?;
            if legs.iter().any(|leg| !matches!(leg, LogicalType::Null | LogicalType::Scalar(_) | LogicalType::List { .. } | LogicalType::Struct(_))) {
                return Err(invalid("Union contains a non-monomorphic leg"));
            }
            LogicalType::from_legs(legs)
        }
    };
    result.validate()?;
    Ok(result)
}

fn scalar(value: ScalarType) -> LogicalType { LogicalType::Scalar(value) }

fn list_type(kind: ListKind, child: Option<&pb::ColumnMeta>) -> Result<LogicalType> {
    Ok(LogicalType::List { kind, element: Box::new(logical_type(child.ok_or_else(|| invalid("List has no element metadata"))?)?) })
}

pub fn type_to_proto(ty: &LogicalType) -> Result<pb::VecType> {
    ty.validate()?;
    let unencoded = ty.unencoded();
    let encoded = match &unencoded {
        LogicalType::Nothing => Type::Simple(0),
        LogicalType::Null => Type::Simple(1),
        LogicalType::Scalar(value) => scalar_to_proto(value)?,
        LogicalType::List { kind, element } => {
            let child = Some(Box::new(column_from_type(element, None)?));
            match kind {
                ListKind::List => Type::List(Box::new(pb::Listy { el: child })),
                ListKind::Set => Type::Set(Box::new(pb::Listy { el: child })),
                ListKind::TimestampTzRange => Type::TstzRange(Box::new(pb::Listy { el: child })),
                ListKind::FixedSizeList(size) => Type::FixedSizeList(Box::new(pb::FixedSizeList { size: Some(*size), el: child })),
                ListKind::Map { sorted } => Type::Map(Box::new(pb::MapType { keys_sorted: Some(*sorted), el: child })),
            }
        }
        LogicalType::Struct(fields) => Type::Struct(Box::new(pb::Struct {
            children: fields.iter().map(|(name, ty)| Ok((name.clone(), column_from_type(ty, None)?))).collect::<Result<BTreeMap<_, _>>>()?,
        })),
        LogicalType::Maybe(mono) => Type::Maybe(Box::new(pb::Maybe { mono: Some(Box::new(type_to_proto(mono)?)) })),
        LogicalType::Union(legs) => {
            let mut legs = legs.iter().map(|leg| Ok((rtdb_columnar::leg_name(leg)?, type_to_proto(leg)?))).collect::<Result<Vec<_>>>()?;
            legs.sort_by(|left, right| left.0.cmp(&right.0));
            Type::Poly(Box::new(pb::Poly { legs: legs.into_iter().map(|(_, ty)| ty).collect() }))
        }
        LogicalType::RunEndEncoded { .. } => return Err(invalid("Column type retains a physical encoding after normalization")),
    };
    Ok(pb::VecType { r#type: Some(encoded) })
}

fn scalar_to_proto(value: &ScalarType) -> Result<Type> {
    Ok(match value {
        ScalarType::Bool => Type::Simple(2),
        ScalarType::I8 => Type::Simple(3),
        ScalarType::I16 => Type::Simple(4),
        ScalarType::I32 => Type::Simple(5),
        ScalarType::I64 => Type::Simple(6),
        ScalarType::F32 => Type::Simple(7),
        ScalarType::F64 => Type::Simple(8),
        ScalarType::Utf8 => Type::Simple(9),
        ScalarType::Binary => Type::Simple(10),
        ScalarType::Keyword => Type::Simple(11),
        ScalarType::Transit => Type::Simple(12),
        ScalarType::Uri => Type::Simple(13),
        ScalarType::Uuid => Type::Simple(14),
        ScalarType::Oid => Type::Simple(15),
        ScalarType::RegClass => Type::Simple(16),
        ScalarType::RegProc => Type::Simple(17),
        ScalarType::FixedSizeBinary(width) => Type::FixedSizeBinary(*width),
        ScalarType::Decimal { precision, scale, bit_width } => Type::Decimal(Box::new(pb::Decimal {
            precision: Some(*precision), scale: Some(*scale), bit_width: Some(i32::from(*bit_width)),
        })),
        ScalarType::Timestamp { unit, timezone } => Type::Timestamp(Box::new(pb::Timestamp { unit: Some(encode_unit(*unit)), timezone: timezone.clone() })),
        ScalarType::DateDay => Type::Date(Box::new(pb::Date { unit: Some(0) })),
        ScalarType::DateMilli => Type::Date(Box::new(pb::Date { unit: Some(1) })),
        ScalarType::Time { unit, bit_width } => Type::Time(Box::new(pb::Time { unit: Some(encode_unit(*unit)), bit_width: Some(i32::from(*bit_width)) })),
        ScalarType::Duration(unit) => Type::Duration(encode_unit(*unit)),
        ScalarType::Interval(unit) => Type::Interval(Box::new(pb::Interval { unit: Some(match unit { IntervalUnit::YearMonth => 0, IntervalUnit::DayTime => 1, IntervalUnit::MonthDayNano => 2 }) })),
        ScalarType::IntervalMonthDayMicro => Type::Interval(Box::new(pb::Interval { unit: Some(3) })),
        ScalarType::U8 | ScalarType::U16 | ScalarType::U32 | ScalarType::U64 => return Err(Anomaly::unsupported("xtdb/col-meta-unsigned-int", "unsigned ints")),
        ScalarType::F16 => return Err(Anomaly::unsupported("xtdb/col-meta-float-precision", "float precision HALF")),
    })
}

fn decode_unit(unit: i32) -> Result<TimeUnit> {
    match unit {
        0 => Ok(TimeUnit::Second), 1 => Ok(TimeUnit::Millisecond),
        2 => Ok(TimeUnit::Microsecond), 3 => Ok(TimeUnit::Nanosecond),
        unknown => Err(invalid(format!("Unknown time unit: {unknown}"))),
    }
}

fn encode_unit(unit: TimeUnit) -> i32 {
    match unit { TimeUnit::Second => 0, TimeUnit::Millisecond => 1, TimeUnit::Microsecond => 2, TimeUnit::Nanosecond => 3 }
}

fn invalid(message: impl std::fmt::Display) -> Anomaly {
    Anomaly::unsupported("xtdb/col-meta-type", message.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsupported_scalar_metadata_preserves_the_reference_error_branch() {
        for scalar in [ScalarType::U8, ScalarType::U16, ScalarType::U32, ScalarType::U64, ScalarType::F16] {
            let (code, message) = if scalar == ScalarType::F16 { ("xtdb/col-meta-float-precision", "float precision HALF") } else { ("xtdb/col-meta-unsigned-int", "unsigned ints") };
            for ty in [LogicalType::Scalar(scalar.clone()), LogicalType::list(LogicalType::Scalar(scalar.clone()))] {
                let error = type_to_proto(&ty).unwrap_err();
                assert_eq!(error.category, rtdb_types::AnomalyCategory::Unsupported);
                assert_eq!(error.code, code);
                assert_eq!(error.message.as_deref(), Some(message));
            }
        }
    }
}
