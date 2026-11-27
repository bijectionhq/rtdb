use std::{ops::Range, sync::Arc};

use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, StringArray,
    builder::{BooleanBuilder, PrimitiveBuilder, StringBuilder},
    cast::AsArray,
    types::{
        Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
        DurationSecondType, Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
    },
};
use rtdb_columnar::ArrowCell;
use rtdb_plan::{CastTarget, TemporalCast};
use rtdb_types::{
    CivilDate, Instant, ListBuilder, ListValue, ListView, LogicalType, NumericCell, Result, ScalarList, ScalarType,
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
    current_time: Option<Instant>,
    projection: bool,
    mut reserve: impl FnMut(usize) -> Result<()>,
) -> Result<Option<ListValue>> {
    if target.union_target().is_some() {
        return Ok(None);
    }
    let Some(cells) = Cells::new(source) else {
        return Ok(None);
    };
    let Some(ty) = target_type(target, timezone) else {
        return Ok(None);
    };
    let count = cells.len();
    if count == 0 {
        return Ok(None);
    }
    if ty == ScalarType::Utf8 {
        return render(&cells, target, reserve);
    }
    if ty == ScalarType::Bool {
        return boolean(&cells, target, reserve);
    }
    if let Some(ty) = list_cast::scalar_target(target)
        && cells.all(|cell| matches!(cell, Cell::Null | Cell::Number(_) | Cell::Text(_)))
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
                Cell::Number(number) => {
                    let source_type = number.scalar_type().expect("present number");
                    target.validate_source(&LogicalType::Scalar(source_type.clone()))?;
                    list_cast::cast_number(number, &source_type, ty)?
                }
                _ => unreachable!("text or numeric list"),
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
    let temporal = temporal_target(target);
    if !is_temporal(&ty) || !cells.all(|cell| supports_temporal(cell, &ty, temporal, current_time)) {
        return Ok(None);
    }
    let Ok(field) = rtdb_columnar::to_field("value", &LogicalType::Scalar(ty.clone())) else {
        return Ok(None);
    };
    let width = field.data_type().primitive_width().expect("temporal primitive");
    reserve(count.checked_mul(width)
        .and_then(|bytes| bytes.checked_add(count.div_ceil(8)))
        .and_then(|bytes| bytes.checked_add(field.size()))
        .and_then(|bytes| bytes.checked_add(4096))
        .ok_or_else(overflow)?)?;
    let array = temporal_array(&field, &cells, |source| {
        let (source_type, value) = match source {
            Cell::Text(text) => {
                reserve(text.len().checked_mul(64).ok_or_else(overflow)?)?;
                (ScalarType::Utf8, TemporalValue::Text(text))
            }
            Cell::Temporal(value, ty) => (ty, TemporalValue::Raw(value)),
            Cell::Number(NumericCell::F64(value)) => (ScalarType::F64, TemporalValue::Seconds(value)),
            _ => return Ok(None),
        };
        let source = LogicalType::Scalar(source_type.clone());
        target.validate_source(&source)?;
        let temporal = temporal_options(target, &source)?;
        cast_temporal(value, &source_type, &ty, temporal.as_deref(), timezone, current_time)
    })?;
    let Some(array) = array else {
        return Ok(None);
    };
    let field = field.with_nullable(array.null_count() != 0);
    Ok(ListValue::from_scalar_arrow(Arc::new(field), array))
}

fn boolean(
    cells: &Cells<'_>, target: &CastTarget, mut reserve: impl FnMut(usize) -> Result<()>,
) -> Result<Option<ListValue>> {
    if matches!(cells, Cells::Values(values) if values.iter().any(|value| matches!(value, Value::Undefined))) {
        return Ok(None);
    }
    if !cells.all(|cell| match cell {
        Cell::Null | Cell::Bool(_) | Cell::Text(_) => true,
        Cell::Number(number) => !matches!(number, NumericCell::Decimal(..)),
        _ => false,
    }) || cells.all(|cell| matches!(cell, Cell::Null)) {
        return Ok(None);
    }
    let bytes = cells.len().div_ceil(8).checked_mul(2).and_then(|bytes| bytes.checked_add(1024)).ok_or_else(overflow)?;
    reserve(bytes)?;
    let mut builder = BooleanBuilder::with_capacity(cells.len());
    for row in 0..cells.len() {
        let (source, value) = match cells.get(row) {
            Cell::Null => { builder.append_null(); continue; }
            Cell::Bool(value) => (ScalarType::Bool, value),
            Cell::Text(text) => {
                target.validate_source(&LogicalType::Scalar(ScalarType::Utf8))?;
                reserve(text.len().checked_mul(4).ok_or_else(overflow)?)?;
                builder.append_value(cast::boolean_text(text)?);
                continue;
            }
            Cell::Number(number) => (number.scalar_type().expect("Present number"), number.as_f64() != Some(0.0)),
            _ => unreachable!("Boolean cast inputs were admitted"),
        };
        target.validate_source(&LogicalType::Scalar(source))?;
        builder.append_value(value);
    }
    let array = builder.finish();
    let field = arrow_schema::Field::new("value", arrow_schema::DataType::Boolean, array.null_count() != 0);
    Ok(ListValue::from_scalar_arrow(Arc::new(field), Arc::new(array)))
}

fn render(
    cells: &Cells<'_>,
    target: &CastTarget,
    mut reserve: impl FnMut(usize) -> Result<()>,
) -> Result<Option<ListValue>> {
    if !cells.all(|cell| matches!(cell, Cell::Null | Cell::Text(_) | Cell::Number(_))) {
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
    Selected(&'a arrow_schema::Field, &'a dyn Array, Range<usize>),
}

enum Cell<'a> {
    Null,
    Bool(bool),
    Text(&'a str),
    Number(NumericCell),
    Temporal(i64, ScalarType),
    Unsupported,
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
                if let Some(span) = cell.numeric_list_span_range(selection.clone()) {
                    return Some(Self::Numeric(list_cast::Input::Opened(span)));
                }
                let (field, array, range) = cell.sequence_range()?;
                if selection.end > range.len() {
                    return None;
                }
                Self::arrow(field, array, (range.start + selection.start)..(range.start + selection.end))
            }
        }
    }

    fn arrow(field: &'a arrow_schema::Field, array: &'a dyn Array, range: Range<usize>) -> Option<Self> {
        let ty = rtdb_columnar::from_field(field).ok()?;
        let ty = match ty {
            LogicalType::Maybe(inner) => *inner,
            ty => ty,
        };
        match ty {
            LogicalType::Scalar(ScalarType::Utf8) => Some(Self::Text(array.as_any().downcast_ref()?, range)),
            LogicalType::Scalar(ty) if is_temporal(&ty) => Some(Self::Temporal(array, range, ty)),
            _ => Some(Self::Selected(field, array, range)),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Numeric(values) => values.len(),
            Self::Text(_, range) | Self::Temporal(_, range, _) | Self::Selected(_, _, range) => range.len(),
            Self::Values(values) => values.len(),
        }
    }

    fn all(&self, mut accepts: impl FnMut(Cell<'_>) -> bool) -> bool {
        (0..self.len()).all(|i| accepts(self.get(i)))
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
                    Cell::Temporal(ScalarList::temporal_raw(*array, row).expect("temporal array"), ty.clone())
                }
            }
            Self::Values(values) => {
                use ScalarType as S;
                match &values[row] {
                    Value::Null | Value::Undefined => Cell::Null,
                    Value::Bool(value) => Cell::Bool(*value),
                    Value::Utf8(text) => Cell::Text(text),
                    Value::DateDay(value) => Cell::Temporal(i64::from(*value), S::DateDay),
                    Value::DateMilli(value) => Cell::Temporal(*value, S::DateMilli),
                    Value::Time { value, unit, bit_width } => {
                        Cell::Temporal(*value, S::Time { unit: *unit, bit_width: *bit_width })
                    }
                    Value::Timestamp { value, unit, timezone } => {
                        Cell::Temporal(*value, S::Timestamp { unit: *unit, timezone: timezone.clone() })
                    }
                    Value::Duration { value, unit } => Cell::Temporal(*value, S::Duration(*unit)),
                    value => NumericCell::from_value(value).map_or(Cell::Unsupported, Cell::Number),
                }
            }
            Self::Selected(field, array, range) => {
                let row = range.start + row;
                let Ok(cell) = (ArrowCell { field, array: *array, row }).physical() else { return Cell::Unsupported; };
                if !matches!(cell.is_undefined(), Ok(false)) { return Cell::Unsupported; }
                if matches!(cell.is_null(), Ok(true)) { return Cell::Null; }
                if let Some(span) = rtdb_types::NumericSpan::new(cell.field, cell.array, cell.row..cell.row + 1) {
                    return Cell::Number(span.get(0).expect("selected cell").signed());
                }
                match cell.logical_type() {
                    Ok(LogicalType::Scalar(ScalarType::Bool)) => Cell::Bool(cell.array.as_boolean().value(cell.row)),
                    Ok(LogicalType::Scalar(ScalarType::Utf8)) => Cell::Text(
                        cell.array.as_any().downcast_ref::<StringArray>().expect("string array").value(cell.row),
                    ),
                    Ok(LogicalType::Scalar(ty)) if is_temporal(&ty) => {
                        ScalarList::temporal_raw(cell.array, cell.row)
                            .map_or(Cell::Unsupported, |value| Cell::Temporal(value, ty))
                    }
                    _ => Cell::Unsupported,
                }
            }
        }
    }
}

fn supports_temporal(cell: Cell<'_>, target: &ScalarType, temporal: bool, clock: Option<Instant>) -> bool {
    use ScalarType as S;
    match cell {
        Cell::Null | Cell::Text(_) => true,
        Cell::Number(NumericCell::F64(_)) => matches!(target, S::Duration(_)),
        Cell::Temporal(_, source) => match (&source, target) {
            (S::Time { .. }, S::Timestamp { .. }) => temporal && clock.is_some(),
            (S::DateDay | S::DateMilli, S::DateDay | S::DateMilli | S::Timestamp { .. })
            | (S::Timestamp { .. }, S::Timestamp { .. } | S::DateDay | S::DateMilli | S::Time { .. })
            | (S::Time { .. }, S::Time { .. } | S::Duration(_))
            | (S::Duration(_), S::Duration(_)) => true,
            _ => false,
        },
        _ => false,
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

fn target_type(target: &CastTarget, timezone: &str) -> Option<ScalarType> {
    if target.union_target().is_some() {
        return None;
    }
    match target {
        CastTarget::Checked { target, .. } => target_type(target, timezone),
        CastTarget::Fixed(LogicalType::Scalar(ty)) => Some(ty.clone()),
        CastTarget::DecimalDefault => Some(ScalarType::Decimal { precision: 64, scale: 9, bit_width: 256 }),
        CastTarget::Temporal(target) => match target.data_type(timezone) {
            LogicalType::Scalar(ty) => Some(ty),
            _ => None,
        },
        CastTarget::Deferred(rtdb_plan::DeferredCast::Temporal { kind, unit, .. }) => {
            let target = TemporalCast { kind: kind.clone(), unit: *unit, precision: None };
            let LogicalType::Scalar(ty) = target.data_type(timezone) else { unreachable!() };
            Some(ty)
        }
        _ => None,
    }
}

fn temporal_target(target: &CastTarget) -> bool {
    match target {
        CastTarget::Checked { target, .. } => temporal_target(target),
        CastTarget::Temporal(_) | CastTarget::Deferred(rtdb_plan::DeferredCast::Temporal { .. }) => true,
        _ => false,
    }
}

fn temporal_options<'a>(target: &'a CastTarget, source: &LogicalType) -> Result<Option<std::borrow::Cow<'a, TemporalCast>>> {
    use std::borrow::Cow;
    Ok(match target {
        CastTarget::Checked { target, .. } => return temporal_options(target, source),
        CastTarget::Temporal(target) => Some(Cow::Borrowed(target)),
        CastTarget::Deferred(target) => match target.resolve_value(source)? {
            CastTarget::Temporal(target) => Some(Cow::Owned(target)),
            _ => None,
        },
        _ => None,
    })
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
    current_time: Option<Instant>,
) -> Result<Option<i64>> {
    use ScalarType as S;
    use TimeUnit::Nanosecond;
    let date_raw = |days| -> Result<Option<i64>> {
        let days = CivilDate::from_epoch_day(days)?.epoch_day();
        Ok(if *target == S::DateDay { i32::try_from(days).ok().map(i64::from) }
            else { days.checked_mul(rtdb_types::java_date::MILLIS_PER_DAY) })
    };
    let raw = match (value, source, target) {
        (TemporalValue::Text(text), _, _) if temporal.is_some() => {
            crate::temporal_cast::text_raw(text, temporal.unwrap())?
        }
        (TemporalValue::Text(text), _, S::Timestamp { unit, timezone }) => {
            cast::timestamp_text_raw(text, *unit, timezone.is_some())?
        }
        (TemporalValue::Text(text), _, S::Time { unit, .. }) => {
            (crate::temporal_cast::time_nanos(text)? as i64) / (1_000_000_000 / unit.units_per_second())
        }
        (TemporalValue::Text(text), _, S::Duration(unit)) => {
            cast::rescale_units(crate::temporal_text::duration(text)?, Nanosecond, *unit)?
        }
        (TemporalValue::Text(text), _, S::DateDay | S::DateMilli) => {
            let days = CivilDate::parse_iso(text)
                .map_err(|_| crate::temporal_cast::invalid_temporal_text(text, "date"))?
                .epoch_day();
            return date_raw(days);
        }
        (TemporalValue::Seconds(value), _, S::Duration(unit)) => cast::duration_seconds(value, *unit)?,
        (TemporalValue::Raw(value), _, _) if source == target => value,
        (TemporalValue::Raw(value), S::DateDay | S::DateMilli, _) => {
            let days = if *source == S::DateDay { value } else { value.div_euclid(rtdb_types::java_date::MILLIS_PER_DAY) };
            match target {
                S::Timestamp { unit, timezone: target_zone } => cast::date_to_timestamp(days, *unit, target_zone.is_some(), timezone)?,
                _ => return date_raw(days),
            }
        }
        (TemporalValue::Raw(value), S::Time { unit: source, .. }, S::Timestamp { unit, timezone: target_zone }) => {
            crate::temporal_cast::time_to_timestamp(value, *source, *unit, target_zone.is_some(), timezone, current_time.expect("clock-aware cast"))?
        }
        (TemporalValue::Raw(value), S::Timestamp { unit, timezone: source_zone }, S::DateDay | S::DateMilli) => {
            let days = cast::local_seconds(value, *unit, source_zone.as_ref().map(|_| timezone))?.div_euclid(86_400);
            return date_raw(days);
        }
        (TemporalValue::Raw(value), S::Timestamp { unit: source, timezone: source_zone }, S::Time { unit, .. }) => {
            cast::timestamp_to_time(value, *source, *unit, source_zone.as_ref().map(|_| timezone))?
        }

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

fn temporal_array<'a>(
    field: &arrow_schema::Field, cells: &'a Cells<'_>, cast: impl FnMut(Cell<'a>) -> Result<Option<i64>>,
) -> Result<Option<ArrayRef>> {
    use arrow_schema::{DataType as D, TimeUnit::{Microsecond, Millisecond, Nanosecond, Second}};
    macro_rules! primitive {
        ($ty:ty) => {
            build_temporal::<$ty>(field.data_type(), cells, cast)
        };
    }
    match field.data_type() {
        D::Date32 => primitive!(Date32Type),
        D::Date64 => primitive!(Date64Type),
        D::Time32(Second) => primitive!(Time32SecondType),
        D::Time32(Millisecond) => primitive!(Time32MillisecondType),
        D::Time64(Microsecond) => primitive!(Time64MicrosecondType),
        D::Time64(Nanosecond) => primitive!(Time64NanosecondType),
        D::Timestamp(Second, _) => primitive!(TimestampSecondType),
        D::Timestamp(Millisecond, _) => primitive!(TimestampMillisecondType),
        D::Timestamp(Microsecond, _) => primitive!(TimestampMicrosecondType),
        D::Timestamp(Nanosecond, _) => primitive!(TimestampNanosecondType),
        D::Duration(Second) => primitive!(DurationSecondType),
        D::Duration(Millisecond) => primitive!(DurationMillisecondType),
        D::Duration(Microsecond) => primitive!(DurationMicrosecondType),
        D::Duration(Nanosecond) => primitive!(DurationNanosecondType),
        _ => Ok(None),
    }
}

fn build_temporal<'a, T: ArrowPrimitiveType>(
    data_type: &arrow_schema::DataType, cells: &'a Cells<'_>, mut cast: impl FnMut(Cell<'a>) -> Result<Option<i64>>,
) -> Result<Option<ArrayRef>>
where
    T::Native: TryFrom<i64>,
{
    let mut builder = PrimitiveBuilder::<T>::with_capacity(cells.len()).with_data_type(data_type.clone());
    let mut present = false;
    let mut narrow_overflow = false;
    for row in 0..cells.len() {
        let cell = cells.get(row);
        if matches!(cell, Cell::Null) {
            builder.append_null();
            continue;
        }
        let Some(value) = cast(cell)? else { return Ok(None); };
        match T::Native::try_from(value) {
            Ok(value) => {
                builder.append_value(value);
                present = true;
            }
            Err(_) if matches!(data_type, arrow_schema::DataType::Time32(_)) => return Ok(None),
            // A later conversion error takes precedence over Date32 storage overflow.
            Err(_) => narrow_overflow = true,
        }
    }
    Ok((present && !narrow_overflow).then(|| Arc::new(builder.finish()) as ArrayRef))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, TimeUnit as ArrowUnit};
    use rtdb_types::Anomaly;

    #[test]
    fn boolean_list_cast_keeps_nulls_and_matches_the_first_scalar_error() {
        let target = CastTarget::fixed(LogicalType::Scalar(ScalarType::Bool));
        let inputs = [Value::I64(0), Value::Null, Value::I64(-7), Value::Utf8("YeS".into()), Value::Bool(false)];
        let source = Source::Stored(ListView::Values(&inputs));
        let list = cast_list(source, &target, "UTC", None, true, |_| Ok(())).unwrap().unwrap();
        let (field, values) = list.packed_column().unwrap();
        assert_eq!(field.data_type(), &DataType::Boolean);
        assert_eq!(values.as_boolean().iter().collect::<Vec<_>>(), [Some(false), None, Some(true), Some(true), Some(false)]);

        let child = Arc::new(arrow_array::StringArray::from(vec![Some("unused"), Some("YeS"), None, Some("bogus")])) as ArrayRef;
        let item = Arc::new(Field::new("item", DataType::Utf8, true));
        let lists = rtdb_columnar::build_list_array(item, vec![0, 1, 4].into(), &child, None).unwrap();
        let field = Field::new("items", lists.data_type().clone(), false);
        let cell = ArrowCell { field: &field, array: lists.as_ref(), row: 1 };
        let list = cast_list(Source::Arrow(cell, 0..2), &target, "UTC", None, true, |_| Ok(())).unwrap().unwrap();
        assert_eq!(list.packed_column().unwrap().1.as_boolean().iter().collect::<Vec<_>>(), [Some(true), None]);
        let expected = cast::cast_target(&Value::Utf8("bogus".into()), &target, "UTC").unwrap_err();
        let error = cast_list(Source::Arrow(cell, 0..3), &target, "UTC", None, true, |_| Ok(())).unwrap_err();
        assert_eq!((error.code, error.message, error.context), (expected.code, expected.message, expected.context));

        let checked = CastTarget::Checked { requested: LogicalType::Scalar(ScalarType::Bool), target: Box::new(target) };
        let bad = [Value::I64(1), Value::F64(0.25), Value::Utf8("bogus".into())];
        let source = Source::Stored(ListView::Values(&bad));
        let expected = cast::cast_target(&bad[1], &checked, "UTC").unwrap_err();
        let error = cast_list(source, &checked, "UTC", None, true, |_| Ok(())).unwrap_err();
        assert_eq!((error.code, error.message, error.context), (expected.code, expected.message, expected.context));

        let nulls = [Value::Null, Value::Null];
        let source = Source::Stored(ListView::Values(&nulls));
        assert!(cast_list(source, &checked, "UTC", None, true, |_| Ok(())).unwrap().is_none());
        let bottom = [Value::Bool(true), Value::Undefined];
        let source = Source::Stored(ListView::Values(&bottom));
        assert!(cast_list(source, &checked, "UTC", None, true, |_| Ok(())).unwrap().is_none());
    }

    #[test]
    fn temporal_narrowing_preserves_the_time32_and_date32_error_order() {
        let inputs = [Value::I64(i64::MAX), Value::I64(0)];
        let cells = Cells::Values(&inputs);
        let convert = |cell| {
            match cell {
                Cell::Number(NumericCell::I64(i64::MAX)) => Ok(Some(i64::MAX)),
                _ => Err(Anomaly::incorrect("example/bad-component", "Invalid temporal component")),
            }
        };
        let result = build_temporal::<Time32SecondType>(&DataType::Time32(ArrowUnit::Second), &cells, convert).unwrap();
        assert!(result.is_none());
        let error = build_temporal::<Date32Type>(&DataType::Date32, &cells, convert).unwrap_err();
        assert_eq!(error.code, "example/bad-component");
    }

    #[test]
    fn temporal_child_has_the_target_zone_and_observed_validity() {
        let inputs = [Value::I64(i64::MIN), Value::Null, Value::I64(0)];
        let field = Field::new("value", DataType::Timestamp(ArrowUnit::Microsecond, Some("America/New_York".into())), false);
        let result = temporal_array(&field, &Cells::Values(&inputs), |cell| {
            let Cell::Number(NumericCell::I64(value)) = cell else { panic!("present integer"); };
            Ok(Some(value))
        }).unwrap().unwrap();
        let values = result.as_any().downcast_ref::<arrow_array::TimestampMicrosecondArray>().unwrap();
        assert_eq!(values.data_type(), field.data_type());
        assert_eq!(values.iter().collect::<Vec<_>>(), [Some(i64::MIN), None, Some(0)]);
    }
}
