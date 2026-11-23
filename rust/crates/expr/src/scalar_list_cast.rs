use std::{ops::Range, sync::Arc};

use arrow_array::{
    Array, ArrayRef, PrimitiveArray, StringArray,
    builder::StringBuilder,
    types::{
        Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
        DurationSecondType, Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    },
};
use rtdb_columnar::ArrowCell;
use rtdb_plan::{CastTarget, TemporalCast};
use rtdb_types::{
    CivilDate, ListBuilder, ListValue, ListView, LogicalType, NumericCell, Result, ScalarList, ScalarType,
    TimeUnit, Value,
};

use crate::{cast, list_cast, numeric::overflow};

pub(crate) enum Source<'a> {
    Stored(ListView<'a>),
    Arrow(ArrowCell<'a>, Range<usize>),
}

pub(crate) fn cast_list(
    source: Source<'_>,
    target: &CastTarget,
    timezone: &str,
    projection: bool,
    mut reserve: impl FnMut(usize) -> Result<()>,
) -> Result<Option<ListValue>> {
    if target.union_target().is_some() {
        return Ok(None);
    }
    let Some(cells) = Cells::new(source) else {
        return Ok(None);
    };
    let Some((ty, temporal)) = target_type(target, timezone) else {
        return Ok(None);
    };
    let count = cells.len();
    if count == 0 {
        return Ok(None);
    }
    if ty == ScalarType::Utf8 {
        return render(&cells, target, reserve);
    }
    if let Some(ty) = list_cast::scalar_target(target)
        && cells.is_text()
    {
        let source_type = LogicalType::Scalar(ScalarType::Utf8);
        reserve(count.checked_mul(192).and_then(|n| n.checked_add(4096)).ok_or_else(overflow)?)?;
        let mut builder = ListBuilder::with_capacity(count);
        for i in 0..count {
            let cell = match cells.get(i) {
                Cell::Null => NumericCell::Null,
                Cell::Text(text) => {
                    reserve(text.len().checked_mul(64).ok_or_else(overflow)?)?;
                    target.validate_source(&source_type)?;
                    list_cast::cast_text_number(text, ty)?
                }
                _ => unreachable!("text list"),
            };
            if projection {
                list_cast::validate_output(cell)?;
            }
            if !builder.try_push_numeric(cell) {
                return Ok(None);
            }
        }
        return Ok(Some(builder.finish()));
    }
    if !is_temporal(&ty) || !cells.supports_temporal(&ty) {
        return Ok(None);
    }
    let logical = LogicalType::Scalar(ty.clone());
    if rtdb_columnar::to_field("value", &logical).is_err() {
        return Ok(None);
    }
    reserve(count.checked_mul(32).and_then(|n| n.checked_add(4096)).ok_or_else(overflow)?)?;
    let mut raw = Vec::with_capacity(count);
    for i in 0..count {
        let source = cells.get(i);
        let (source_type, value) = match source {
            Cell::Null => {
                raw.push(None);
                continue;
            }
            Cell::Text(text) => {
                reserve(text.len().checked_mul(64).ok_or_else(overflow)?)?;
                (ScalarType::Utf8, TemporalValue::Text(text))
            }
            Cell::Temporal(value, ty) => (ty.clone(), TemporalValue::Raw(value)),
            Cell::Number(NumericCell::F64(value)) => (ScalarType::F64, TemporalValue::Seconds(value)),
            _ => return Ok(None),
        };
        target.validate_source(&LogicalType::Scalar(source_type.clone()))?;
        let Some(value) = cast_temporal(value, &source_type, &ty, temporal, timezone)? else {
            return Ok(None);
        };
        raw.push(Some(value));
    }
    if raw.iter().all(Option::is_none) {
        return Ok(None);
    }
    let array = temporal_array(&ty, raw)?;
    let Some(array) = array else {
        return Ok(None);
    };
    let field =
        rtdb_columnar::to_field("value", &if array.null_count() == 0 { logical } else { logical.maybe() })?;
    Ok(ListValue::from_scalar_arrow(Arc::new(field), array))
}

fn render(
    cells: &Cells<'_>,
    target: &CastTarget,
    mut reserve: impl FnMut(usize) -> Result<()>,
) -> Result<Option<ListValue>> {
    if !cells.is_text() && !matches!(cells, Cells::Numeric(_)) {
        return Ok(None);
    }
    reserve(cells.len().checked_mul(160).and_then(|n| n.checked_add(4096)).ok_or_else(overflow)?)?;
    let mut builder = StringBuilder::with_capacity(cells.len(), 0);
    let text_type = LogicalType::Scalar(ScalarType::Utf8);
    let mut bytes = 0usize;
    let mut present = false;
    for row in 0..cells.len() {
        let formatted;
        let text = match cells.get(row) {
            Cell::Null | Cell::Number(NumericCell::Null) => {
                builder.append_null();
                continue;
            }
            Cell::Text(text) => {
                target.validate_source(&text_type)?;
                reserve(text.len())?;
                text
            }
            Cell::Number(value) => {
                target.validate_source(&LogicalType::Scalar(value.scalar_type().unwrap()))?;
                formatted = number_text(value);
                &formatted
            }
            _ => unreachable!("text or numeric list"),
        };
        present = true;
        bytes = bytes.checked_add(text.len()).ok_or_else(overflow)?;
        if bytes > i32::MAX as usize {
            return Ok(None);
        }
        builder.append_value(text);
    }
    if !present {
        return Ok(None);
    }
    let array = builder.finish();
    let field = arrow_schema::Field::new("value", arrow_schema::DataType::Utf8, array.null_count() != 0);
    Ok(ListValue::from_scalar_arrow(Arc::new(field), Arc::new(array)))
}

fn number_text(value: NumericCell) -> String {
    match value {
        NumericCell::F16(value) => {
            let value = half::f16::from_bits(value).to_f32();
            crate::string::float_text(f64::from(value), Some(value))
        }
        NumericCell::F32(value) => crate::string::float_text(f64::from(value), Some(value)),
        NumericCell::F64(value) => crate::string::float_text(value, None),
        NumericCell::Decimal(bytes, params) => {
            crate::decimal::decimal_parts_text(&num_bigint::BigInt::from_signed_bytes_be(&bytes), params.scale)
        }
        value => value.integer().unwrap().to_string(),
    }
}

enum Cells<'a> {
    Numeric(list_cast::Input<'a>),
    Text(&'a StringArray, Range<usize>),
    Temporal(&'a dyn Array, Range<usize>, ScalarType),
    Values(&'a [Value]),
}

enum Cell<'a> {
    Null,
    Text(&'a str),
    Number(NumericCell),
    Temporal(i64, &'a ScalarType),
}

impl<'a> Cells<'a> {
    fn new(source: Source<'a>) -> Option<Self> {
        match source {
            Source::Stored(ListView::Values(values)) => Some(Self::Values(values)),
            Source::Stored(ListView::Scalars(values)) => {
                Self::arrow(values.field(), values.array().as_ref(), 0..values.array().len())
            }
            Source::Stored(view) => Some(Self::Numeric(list_cast::Input::Stored(view))),
            Source::Arrow(cell, selection) => {
                if let Some(span) = cell.numeric_list_span() {
                    return Some(Self::Numeric(list_cast::Input::Opened(
                        span.slice(selection.start, selection.len())?,
                    )));
                }
                let (field, array, range) = cell.sequence_range()?;
                if selection.end > range.len() {
                    return None;
                }
                Self::arrow(field, array, (range.start + selection.start)..(range.start + selection.end))
            }
        }
    }

    fn arrow(field: &arrow_schema::Field, array: &'a dyn Array, range: Range<usize>) -> Option<Self> {
        let ty = rtdb_columnar::from_field(field).ok()?;
        let ty = match ty {
            LogicalType::Maybe(inner) => *inner,
            ty => ty,
        };
        match ty {
            LogicalType::Scalar(ScalarType::Utf8) => Some(Self::Text(array.as_any().downcast_ref()?, range)),
            LogicalType::Scalar(ty) if is_temporal(&ty) => Some(Self::Temporal(array, range, ty)),
            _ => None,
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Numeric(values) => values.len(),
            Self::Text(_, range) | Self::Temporal(_, range, _) => range.len(),
            Self::Values(values) => values.len(),
        }
    }

    fn is_text(&self) -> bool {
        match self {
            Self::Text(..) => true,
            Self::Values(values) => values.iter().all(|value| matches!(value, Value::Null | Value::Utf8(_))),
            _ => false,
        }
    }

    fn supports_temporal(&self, target: &ScalarType) -> bool {
        match self {
            Self::Numeric(values) => {
                matches!(target, ScalarType::Duration(_))
                    && (0..values.len())
                        .all(|row| matches!(values.get(row), NumericCell::Null | NumericCell::F64(_)))
            }
            Self::Temporal(_, _, source) => matches!(
                (source, target),
                (ScalarType::Timestamp { .. }, ScalarType::Timestamp { .. })
                    | (ScalarType::Time { .. }, ScalarType::Time { .. } | ScalarType::Duration(_))
                    | (ScalarType::Duration(_), ScalarType::Duration(_))
            ),
            _ => self.is_text(),
        }
    }

    fn get(&self, row: usize) -> Cell<'_> {
        match self {
            Self::Numeric(values) => match values.get(row) {
                NumericCell::Null => Cell::Null,
                value => Cell::Number(value),
            },
            Self::Text(array, range) => {
                let row = range.start + row;
                if array.is_null(row) { Cell::Null } else { Cell::Text(array.value(row)) }
            }
            Self::Temporal(array, range, ty) => {
                let row = range.start + row;
                if array.is_null(row) {
                    Cell::Null
                } else {
                    Cell::Temporal(ScalarList::temporal_raw(*array, row).expect("temporal array"), ty)
                }
            }
            Self::Values(values) => match &values[row] {
                Value::Null => Cell::Null,
                Value::Utf8(text) => Cell::Text(text),
                _ => unreachable!("text list"),
            },
        }
    }
}

fn is_temporal(ty: &ScalarType) -> bool {
    matches!(
        ty,
        ScalarType::DateDay
            | ScalarType::DateMilli
            | ScalarType::Timestamp { .. }
            | ScalarType::Time { .. }
            | ScalarType::Duration(_)
    )
}

fn target_type<'a>(target: &'a CastTarget, timezone: &str) -> Option<(ScalarType, Option<&'a TemporalCast>)> {
    if target.union_target().is_some() {
        return None;
    }
    match target {
        CastTarget::Checked { target, .. } => target_type(target, timezone),
        CastTarget::Fixed(LogicalType::Scalar(ty)) => Some((ty.clone(), None)),
        CastTarget::DecimalDefault => {
            Some((ScalarType::Decimal { precision: 64, scale: 9, bit_width: 256 }, None))
        }
        CastTarget::Temporal(target) => match target.data_type(timezone) {
            LogicalType::Scalar(ty) => Some((ty, Some(target))),
            _ => None,
        },
        _ => None,
    }
}

enum TemporalValue<'a> {
    Text(&'a str),
    Raw(i64),
    Seconds(f64),
}

fn cast_temporal(
    value: TemporalValue<'_>,
    source: &ScalarType,
    target: &ScalarType,
    temporal: Option<&TemporalCast>,
    timezone: &str,
) -> Result<Option<i64>> {
    use ScalarType as S;
    use TimeUnit::Nanosecond;
    let raw = match (value, source, target) {
        (TemporalValue::Text(text), _, _) if temporal.is_some() => {
            crate::temporal_cast::text_raw(text, temporal.unwrap())?
        }
        (TemporalValue::Text(text), _, S::Timestamp { unit, timezone }) => {
            cast::timestamp_text_raw(text, *unit, timezone.is_some())?
        }
        (TemporalValue::Text(text), _, S::Time { unit, .. }) => {
            (rtdb_types::parse_iso_local_time(text)? as i64) / (1_000_000_000 / unit.units_per_second())
        }
        (TemporalValue::Text(text), _, S::Duration(unit)) => {
            cast::rescale_units(crate::temporal_text::duration(text)?, Nanosecond, *unit)?
        }
        (TemporalValue::Text(text), _, S::DateDay | S::DateMilli) => {
            let days = CivilDate::parse_iso(text)
                .map_err(|_| crate::temporal_cast::invalid_temporal_text(text, "date"))?
                .epoch_day();
            if *target == S::DateDay {
                return Ok(i32::try_from(days).ok().map(i64::from));
            }
            return Ok(days.checked_mul(rtdb_types::java_date::MILLIS_PER_DAY));
        }
        (TemporalValue::Seconds(value), _, S::Duration(unit)) => cast::duration_seconds(value, *unit)?,
        (
            TemporalValue::Raw(value),
            S::Timestamp { unit: source, timezone: source_zone },
            S::Timestamp { unit, timezone: target_zone },
        ) => cast::timestamp_raw(value, *source, source_zone.is_some(), *unit, target_zone.is_some(), timezone)?,
        (
            TemporalValue::Raw(value),
            S::Time { unit: source, .. } | S::Duration(source),
            S::Time { unit, .. } | S::Duration(unit),
        ) => cast::rescale_units(value, *source, *unit)?,
        _ => return Ok(None),
    };
    Ok(Some(raw))
}

fn temporal_array(target: &ScalarType, raw: Vec<Option<i64>>) -> Result<Option<ArrayRef>> {
    use ScalarType as S;
    use TimeUnit::{Microsecond, Millisecond, Nanosecond, Second};
    macro_rules! primitive {
        ($ty:ty) => {
            Arc::new(raw.into_iter().collect::<PrimitiveArray<$ty>>())
        };
    }
    macro_rules! narrow {
        ($ty:ty) => {{
            let Some(raw) = raw
                .into_iter()
                .map(|value| match value {
                    Some(value) => i32::try_from(value).ok().map(Some),
                    None => Some(None),
                })
                .collect::<Option<PrimitiveArray<$ty>>>()
            else {
                return Ok(None);
            };
            Arc::new(raw)
        }};
    }
    macro_rules! timestamp {
        ($ty:ty, $zone:expr) => {
            Arc::new(raw.into_iter().collect::<PrimitiveArray<$ty>>().with_timezone_opt($zone.as_deref()))
        };
    }
    Ok(Some(match target {
        S::DateDay => narrow!(Date32Type),
        S::DateMilli => primitive!(Date64Type),
        S::Time { unit: Second, bit_width: 32 } => narrow!(Time32SecondType),
        S::Time { unit: Millisecond, bit_width: 32 } => narrow!(Time32MillisecondType),
        S::Time { unit: Microsecond, bit_width: 64 } => primitive!(Time64MicrosecondType),
        S::Time { unit: Nanosecond, bit_width: 64 } => primitive!(Time64NanosecondType),
        S::Timestamp { unit: Second, timezone } => timestamp!(TimestampSecondType, timezone),
        S::Timestamp { unit: Millisecond, timezone } => timestamp!(TimestampMillisecondType, timezone),
        S::Timestamp { unit: Microsecond, timezone } => timestamp!(TimestampMicrosecondType, timezone),
        S::Timestamp { unit: Nanosecond, timezone } => timestamp!(TimestampNanosecondType, timezone),
        S::Duration(Second) => primitive!(DurationSecondType),
        S::Duration(Millisecond) => primitive!(DurationMillisecondType),
        S::Duration(Microsecond) => primitive!(DurationMicrosecondType),
        S::Duration(Nanosecond) => primitive!(DurationNanosecondType),
        _ => return Ok(None),
    }))
}
