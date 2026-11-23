use rtdb_types::StructFields;
use num_bigint::BigInt;
use rtdb_types::transit_value::{MAX_DEPTH, TransitValue as T};
use rtdb_types::{Anomaly, Result, TimeUnit, Value, format_iso_duration};
use crate::temporal_text::{TimestampText, date_text, interval_parts, interval_text, nanos, time_text, timestamp_text};

/// Converts a logical cell into the typed Transit representation.
/// The caller accounts for the output and temporary allocations.
pub fn value_to_transit(value: &Value) -> Result<T> { cell(value, 0) }

fn cell(value: &Value, depth: usize) -> Result<T> {
    if depth > MAX_DEPTH {
        return Err(Anomaly::incorrect("rtdb.format/transit-value-depth", "Transit cell exceeds the nesting limit"));
    }
    let next = |value| cell(value, depth + 1);
    Ok(match value {
        Value::Undefined | Value::Null => T::Null,
        Value::Bool(value) => T::Bool(*value),
        Value::I8(value) => T::Integer(i64::from(*value)),
        Value::I16(value) => T::Integer(i64::from(*value)),
        Value::I32(value) => T::Integer(i64::from(*value)),
        Value::Oid(value) => tagged("xt/oid", T::Integer(i64::from(*value))),
        Value::RegClass(value) => tagged("xt/regclass", T::Integer(i64::from(*value))),
        Value::RegProc(value) => tagged("xt/regproc", T::Integer(i64::from(*value))),
        Value::I64(value) => T::Integer(*value),
        Value::U8(value) => T::Integer(i64::from(*value)),
        Value::U16(value) => T::Integer(i64::from(*value)),
        Value::U32(value) => T::Integer(i64::from(*value)),
        Value::U64(value) => i64::try_from(*value).map_or_else(|_| tagged_text("n", value.to_string()), T::Integer),
        Value::F16(value) => T::Float(f64::from(half::f16::from_bits(*value).to_f32())),
        Value::F32(value) => T::Float(f64::from(*value)),
        Value::F64(value) => T::Float(*value),
        Value::Utf8(value) => T::String(value.clone()),
        Value::Keyword(value) => T::Keyword(value.as_str().into()),
        Value::Binary(value) | Value::FixedSizeBinary(value) => T::Bytes(value.clone()),
        Value::Transit(value) => crate::transit::decode_msgpack(value)?,
        Value::Decimal(value) => tagged_text("f", rtdb_types::jvm_number::decimal_parts_text(
            &BigInt::from_signed_bytes_be(&value.unscaled), value.scale,
        )),
        Value::Uuid(value) => tagged_text("u", uuid::Uuid::from_bytes(*value).to_string()),
        Value::Uri(value) => tagged_text("r", value.clone()),
        Value::Date(value) => tagged_text("time/date", value.date().to_string()),
        Value::DateDay(value) => tagged_text("time/date", date_text(i64::from(*value))?),
        Value::DateMilli(value) => tagged_text("time/date", date_text(value.div_euclid(rtdb_types::java_date::MILLIS_PER_DAY))?),
        Value::Time { value, unit, .. } => tagged_text("time/time", time_text(nanos(*value, *unit), false)?),
        Value::Timestamp { value, unit, timezone } => tagged_text(
            if timezone.is_some() { "time/zoned-date-time" } else { "time/date-time" },
            timestamp_text(*value, *unit, timezone.as_deref(), TimestampText::Iso)?,
        ),
        Value::Duration { value, unit } => tagged_text("time/duration", format_iso_duration(nanos(*value, *unit))),
        value @ (Value::IntervalYearMonth(_) | Value::IntervalDayTime { .. } | Value::IntervalMonthDayMicro { .. } | Value::IntervalMonthDayNano { .. }) => {
            let (months, days, nanos) = interval_parts(value).expect("matched interval cell");
            tagged_text("xtdb/interval", interval_text(months, days, nanos))
        }
        Value::List(values) | Value::FixedSizeList(values) => T::Array(values.iter().map(|value| cell(value.as_ref(), depth + 1)).collect::<Result<_>>()?),
        Value::Set(values) => tagged("set", T::Array(values.iter().map(next).collect::<Result<_>>()?)),
        Value::Struct(values) => document(values, depth)?,
        Value::Map { entries, .. } => T::Map(entries.iter().map(|(key, value)| Ok((next(key)?, next(value)?))).collect::<Result<_>>()?),
        Value::TimestampTzRange { lower, upper } => {
            let bound = |value, absent| {
                if value == absent { return Ok(T::Null); }
                Ok(tagged_text("time/zoned-date-time", timestamp_text(value, TimeUnit::Microsecond, Some("UTC"), TimestampText::Iso)?))
            };
            tagged("xtdb/tstz-range", T::Array(vec![bound(*lower, i64::MIN)?, bound(*upper, i64::MAX)?]))
        }
    })
}

/// Converts document fields without copying the logical row.
pub fn document_to_transit(fields: &StructFields<Value>) -> Result<T> { document(fields, 0) }

fn document(fields: &StructFields<Value>, depth: usize) -> Result<T> {
    Ok(T::Map(fields.iter().map(|(key, value)| Ok((T::String(key.clone()), cell(value, depth + 1)?))).collect::<Result<_>>()?))
}

fn tagged_text(tag: &str, text: String) -> T { tagged(tag, T::String(text)) }
fn tagged(tag: &str, representation: T) -> T { T::Tagged { tag: tag.into(), representation: Box::new(representation) } }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cell_conversion_preserves_typed_struct_members_and_numeric_ranges() {
        let cell = Value::Struct([
            ("i".into(), Value::U64(u64::MAX)),
            ("n".into(), Value::Null),
            ("t".into(), Value::Timestamp { value: 0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }),
        ].into());
        assert_eq!(value_to_transit(&cell).unwrap(), T::Map(vec![
            (T::String("i".into()), tagged_text("n", u64::MAX.to_string())),
            (T::String("n".into()), T::Null),
            (T::String("t".into()), tagged_text("time/zoned-date-time", "1970-01-01T00:00Z[UTC]".into())),
        ]));
    }
}
