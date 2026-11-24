use arrow_array::{
    cast::AsArray,
    types::{
        Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType,
        DurationNanosecondType, DurationSecondType, Int8Type, Int16Type, Int32Type, Int64Type,
        IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType,
        Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_schema::{
    DataType, IntervalUnit,
    TimeUnit::{Microsecond, Millisecond, Nanosecond, Second},
};

use super::{
    ArrowCell, Format, PgType, SessionEnvironment, TimestampText, Write, date_binary, date_text,
    duration_text, interval_text, nanos, time_binary, time_text, timestamp_text, unsupported,
    write_bytes, write_range, write_timestamp,
};

use rtdb_types::{Anomaly, Result, TimeUnit};

enum Temporal<'a> {
    Date(i64),
    Time(i128),
    Timestamp(i64, TimeUnit, Option<&'a str>),
    Duration(i128),
    Interval(i32, i32, i64),
}

impl<'a> Temporal<'a> {
    fn read(ArrowCell { field, array, row }: ArrowCell<'a>) -> Option<Self> {
        macro_rules! get {
            ($ty:ty) => {
                array.as_primitive::<$ty>().value(row)
            };
        }
        Some(match field.data_type() {
            DataType::Date32 => Self::Date(i64::from(get!(Date32Type))),
            DataType::Date64 => {
                Self::Date(get!(Date64Type).div_euclid(rtdb_types::java_date::MILLIS_PER_DAY))
            }
            DataType::Time32(Second) => {
                Self::Time(nanos(i64::from(get!(Time32SecondType)), TimeUnit::Second))
            }
            DataType::Time32(Millisecond) => Self::Time(nanos(
                i64::from(get!(Time32MillisecondType)),
                TimeUnit::Millisecond,
            )),
            DataType::Time64(Microsecond) => {
                Self::Time(nanos(get!(Time64MicrosecondType), TimeUnit::Microsecond))
            }
            DataType::Time64(Nanosecond) => {
                Self::Time(nanos(get!(Time64NanosecondType), TimeUnit::Nanosecond))
            }
            DataType::Timestamp(unit, zone) => {
                let (value, unit) = match unit {
                    Second => (get!(TimestampSecondType), TimeUnit::Second),
                    Millisecond => (get!(TimestampMillisecondType), TimeUnit::Millisecond),
                    Microsecond => (get!(TimestampMicrosecondType), TimeUnit::Microsecond),
                    Nanosecond => (get!(TimestampNanosecondType), TimeUnit::Nanosecond),
                };
                Self::Timestamp(value, unit, zone.as_deref())
            }
            DataType::Duration(unit) => {
                let (value, unit) = match unit {
                    Second => (get!(DurationSecondType), TimeUnit::Second),
                    Millisecond => (get!(DurationMillisecondType), TimeUnit::Millisecond),
                    Microsecond => (get!(DurationMicrosecondType), TimeUnit::Microsecond),
                    Nanosecond => (get!(DurationNanosecondType), TimeUnit::Nanosecond),
                };
                Self::Duration(nanos(value, unit))
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                Self::Interval(get!(IntervalYearMonthType), 0, 0)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                let value = get!(IntervalDayTimeType);
                Self::Interval(0, value.days, i64::from(value.milliseconds) * 1_000_000)
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                let value = get!(IntervalMonthDayNanoType);
                Self::Interval(value.months, value.days, value.nanoseconds)
            }
            _ => return None,
        })
    }

    fn text(self) -> Result<(&'static str, String)> {
        Ok(match self {
            Self::Date(days) => ("xt:date", date_text(days)?),
            Self::Time(value) => ("xt:time", time_text(value, false)?),
            Self::Timestamp(value, unit, zone) => (
                if zone.is_some() {
                    "xt:timestamptz"
                } else {
                    "xt:timestamp"
                },
                timestamp_text(value, unit, zone, TimestampText::Iso)?,
            ),
            Self::Duration(value) => ("xt:duration", duration_text(value)),
            Self::Interval(months, days, nanos) => {
                ("xt:interval", interval_text(months, days, nanos))
            }
        })
    }
}

pub(super) fn temporal_text(cell: ArrowCell<'_>) -> Result<Option<(&'static str, String)>> {
    Temporal::read(cell).map(Temporal::text).transpose()
}

pub(super) fn write_scalar(
    ty: PgType,
    format: Format,
    cell: ArrowCell<'_>,
    environment: &SessionEnvironment,
    output: &mut impl Write,
) -> Result<bool> {
    let binary = format == Format::Binary;
    match (ty, Temporal::read(cell)) {
        (PgType::Timestamp | PgType::TimestampTz, Some(Temporal::Timestamp(value, unit, zone))) => {
            write_timestamp(
                format,
                value,
                unit,
                zone,
                ty == PgType::TimestampTz,
                environment,
                output,
            )?
        }
        (PgType::Date, Some(Temporal::Date(days))) if binary => {
            write_bytes(output, &date_binary(days)?)?
        }
        (PgType::Date, Some(Temporal::Date(days))) => {
            write_bytes(output, date_text(days)?.as_bytes())?
        }
        (PgType::Time, Some(Temporal::Time(value))) if binary => {
            write_bytes(output, &time_binary(value)?)?
        }
        (PgType::Time, Some(Temporal::Time(value))) => {
            write_bytes(output, time_text(value, true)?.as_bytes())?
        }
        (PgType::Interval, Some(Temporal::Interval(months, days, nanos))) => write_bytes(
            output,
            interval_text(months, days, nanos / 1000 * 1000).as_bytes(),
        )?,
        (PgType::Duration, Some(Temporal::Duration(_))) if binary => {
            return Err(unsupported(ty, "binary writer"));
        }
        (PgType::Duration, Some(Temporal::Duration(value))) => {
            write_bytes(output, duration_text(value).as_bytes())?
        }
        (PgType::TsTzRange, _) if cell.field.extension_type_name() == Some("xt/tstz-range") => {
            let Some((lower, upper)) = range_bounds(cell)? else {
                return Ok(false);
            };
            write_range(lower, upper, output)?;
        }
        (PgType::Oid | PgType::RegClass | PgType::RegProc, _)
            if cell.field.data_type() == &DataType::Int32 =>
        {
            let number = cell.array.as_primitive::<Int32Type>().value(cell.row) as u32;
            write!(output, "{number}").map_err(super::write_error)?;
        }
        _ => return Ok(false),
    }
    Ok(true)
}

pub(super) fn range_bounds(cell: ArrowCell<'_>) -> Result<Option<(i64, i64)>> {
    let invalid = || {
        Anomaly::incorrect(
            "rtdb/timestamp-range",
            "Timestamp range requires two non-null timestamps",
        )
    };
    let Some(mut bounds) = cell.sequence() else {
        return Ok(None);
    };
    if bounds.len() != 2 {
        return Ok(None);
    }
    let timestamp = |bound: ArrowCell<'_>| -> Result<_> {
        let bound = bound.physical()?;
        Ok(if bound.is_null()? {
            Some(None)
        } else if let Some(Temporal::Timestamp(value, _, _)) = Temporal::read(bound) {
            Some(Some(value))
        } else {
            None
        })
    };
    let lower = timestamp(bounds.next().expect("A range has a lower bound"))?;
    let upper = timestamp(bounds.next().expect("A range has an upper bound"))?;
    match (lower, upper) {
        (Some(Some(lower)), Some(Some(upper))) => Ok(Some((lower, upper))),
        (Some(_), Some(_)) => Err(invalid()),
        _ => Ok(None),
    }
}

pub(super) fn json_integer(cell: ArrowCell<'_>) -> Option<i128> {
    let ArrowCell { field, array, row } = cell;
    Some(match field.data_type() {
        DataType::Int8 => array.as_primitive::<Int8Type>().value(row).into(),
        DataType::Int16 => array.as_primitive::<Int16Type>().value(row).into(),
        DataType::Int32 if field.extension_type_name().is_none() => {
            array.as_primitive::<Int32Type>().value(row).into()
        }
        DataType::Int64 => array.as_primitive::<Int64Type>().value(row).into(),
        DataType::UInt8 => array.as_primitive::<UInt8Type>().value(row).into(),
        DataType::UInt16 => array.as_primitive::<UInt16Type>().value(row).into(),
        DataType::UInt32 => array.as_primitive::<UInt32Type>().value(row).into(),
        DataType::UInt64 => array.as_primitive::<UInt64Type>().value(row).into(),
        _ => return None,
    })
}
