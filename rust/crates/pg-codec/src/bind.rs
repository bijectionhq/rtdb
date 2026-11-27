use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, Date32Array, DurationMicrosecondArray, FixedSizeListArray,
    IntervalMonthDayNanoArray, IntervalYearMonthArray, NullArray, Time64NanosecondArray,
    TimestampMicrosecondArray, builder::FixedSizeBinaryBuilder,
};
use arrow_buffer::IntervalMonthDayNano;
use arrow_schema::{DataType, Field};
use rtdb_types::{Anomaly, CivilDate, LogicalType, PgType, Result};

use crate::{
    Format,
    binary::{Input, read_fixed},
    invalid, text,
};

pub(super) fn read_scalar(
    name: &str,
    ty: PgType,
    format: Format,
    bytes: &[u8],
) -> Result<Option<(Field, ArrayRef)>> {
    let utf8 = || std::str::from_utf8(bytes).map_err(invalid);
    let array: ArrayRef = match ty {
        PgType::Bytes if format == Format::Text => {
            let bytes = text::bytea(utf8()?)?;
            Arc::new(BinaryArray::from_vec(vec![bytes.as_ref()]))
        }
        PgType::Uuid => {
            let value = match format {
                Format::Text => text::parse_uuid(utf8()?)?,
                Format::Binary => read_fixed(bytes, Input::uuid)?,
            };
            let mut builder = FixedSizeBinaryBuilder::with_capacity(1, 16);
            builder.append_value(value).map_err(invalid)?;
            let field = rtdb_columnar::to_field(name, &ty.logical_type().expect("UUID type"))?;
            return Ok(Some((field, Arc::new(builder.finish()))));
        }
        PgType::Date => {
            let days = match format {
                Format::Text => {
                    let date = CivilDate::parse_iso(utf8()?)?;
                    i32::try_from(date.epoch_day()).map_err(|_| {
                        Anomaly::incorrect(
                            "xtdb/date-out-of-range",
                            format!("Date {date} is outside the range of its Arrow date type"),
                        )
                    })?
                }
                Format::Binary => read_fixed(bytes, Input::date_days)?,
            };
            Arc::new(Date32Array::from(vec![days]))
        }
        PgType::Time => {
            let nanos = match format {
                Format::Text => rtdb_types::parse_iso_local_time(utf8()?)? as i64,
                Format::Binary => read_fixed(bytes, Input::time_nanos)?,
            };
            Arc::new(Time64NanosecondArray::from(vec![nanos]))
        }
        PgType::Timestamp | PgType::TimestampTz => {
            let (value, zone) = match (format, ty) {
                (Format::Binary, _) => (
                    read_fixed(bytes, Input::timestamp_micros)?,
                    (ty == PgType::TimestampTz).then(|| "Z".into()),
                ),
                (Format::Text, PgType::TimestampTz) => {
                    let (value, zone) = text::zoned_timestamp_micros(utf8()?)?;
                    (value, Some(zone))
                }
                (Format::Text, _) => (text::local_timestamp_micros(utf8()?)?, None),
            };
            Arc::new(TimestampMicrosecondArray::from(vec![value]).with_timezone_opt(zone))
        }
        PgType::Interval => {
            let (months, days, nanos) = match format {
                Format::Text => crate::interval::parse(utf8()?)?,
                Format::Binary => read_fixed(bytes, Input::interval_parts)?,
            };
            let scalar = text::interval_type(days, nanos);
            let field = rtdb_columnar::to_field(name, &LogicalType::Scalar(scalar))?;
            let array: ArrayRef = match field.data_type() {
                DataType::Interval(arrow_schema::IntervalUnit::YearMonth) => {
                    Arc::new(IntervalYearMonthArray::from(vec![months]))
                }
                _ => Arc::new(IntervalMonthDayNanoArray::from(vec![
                    IntervalMonthDayNano::new(months, days, nanos),
                ])),
            };
            return Ok(Some((field, array)));
        }
        PgType::Duration if format == Format::Text => {
            Arc::new(DurationMicrosecondArray::from(vec![
                crate::interval::duration_micros(utf8()?)?,
            ]))
        }
        PgType::TsTzRange => {
            let Some((lower, upper)) = text::read_tstzrange(utf8()?)? else {
                return Ok(Some((
                    rtdb_columnar::to_field(name, &LogicalType::Null)?,
                    Arc::new(NullArray::new(1)),
                )));
            };
            let field = text::tstzrange_field(name, false)?;
            let DataType::FixedSizeList(item, width) = field.data_type() else {
                unreachable!()
            };
            let values = TimestampMicrosecondArray::from(vec![lower, upper]).with_timezone("UTC");
            let array =
                FixedSizeListArray::try_new(Arc::clone(item), *width, Arc::new(values), None)
                    .map_err(invalid)?;
            return Ok(Some((field, Arc::new(array))));
        }
        _ => return Ok(None),
    };
    Ok(Some((
        Field::new(name, array.data_type().clone(), false),
        array,
    )))
}
