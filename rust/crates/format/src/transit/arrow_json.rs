use std::{io::Write, mem::size_of, sync::Arc};

use arrow_array::{Array, FixedSizeBinaryArray, RecordBatch, cast::AsArray, types::*};
use arrow_schema::{DataType, TimeUnit as ArrowTimeUnit};
use base64::write::EncoderWriter;
use rtdb_columnar::ArrowCell;
use rtdb_types::{ListKind, MemoryBudget, Result, TimeUnit};

use super::{MAX_DEPTH, STANDARD, TransitValue as T, invalid, stringable, value_to_transit};
use crate::temporal_text::{TimestampText, date_text, interval_text, nanos, time_text, timestamp_text};

/// Writes one Arrow cell in the Transit JSON representation used by PostgreSQL.
pub fn write_arrow_json(output: &mut impl Write, cell: ArrowCell<'_>) -> Result<()> {
    write_cell(output, cell, false, false, 0, true)
}

/// Appends an existing batch row in the embedded client's typed Transit JSON representation.
/// A failed write can leave a partial row in the output.
pub fn write_arrow_row_json(output: &mut impl Write, batch: &RecordBatch, row: usize) -> Result<()> {
    let cells = batch.schema_ref().fields().iter().zip(batch.columns())
        .map(|(field, array)| ArrowCell { field, array: array.as_ref(), row });
    write_struct(output, cells, true, 0)
}

/// Writes a batch as an array of embedded typed Transit rows.
/// The memory budget covers routing scratch. The caller owns the output buffer and its admission.
pub fn write_arrow_batch_json(
    output: &mut impl Write,
    batch: &RecordBatch,
    memory: &Arc<MemoryBudget>,
) -> Result<()> {
    if batch.num_rows() == 0 {
        return bytes(output, b"[]");
    }
    let fields = batch.schema_ref().fields();
    let count = fields.len();
    if fields.windows(2).all(|pair| pair[0].name() < pair[1].name())
        || (count <= 8 && fields.iter().enumerate().all(|(index, field)| {
            fields[..index].iter().all(|previous| previous.name() != field.name())
        }))
    {
        return write_rows(output, batch, 0..count);
    }

    let scratch = count.checked_mul(size_of::<(usize, usize)>())
        .ok_or_else(|| invalid("Transit column routing exceeds addressable memory"))?;
    let reservation = memory.reserve(scratch)?;
    let mut routes = Vec::new();
    routes.try_reserve_exact(count).map_err(invalid)?;
    routes.extend((0..count).map(|index| (index, index)));
    routes.sort_unstable_by(|&(left, _), &(right, _)| {
        fields[left].name().cmp(fields[right].name()).then_with(|| left.cmp(&right))
    });
    routes.dedup_by(|&mut (next, _), (first, last)| {
        if fields[next].name() == fields[*first].name() {
            *last = next;
            true
        } else {
            false
        }
    });
    if routes.len() == count {
        drop(routes);
        drop(reservation);
        return write_rows(output, batch, 0..count);
    }
    routes.sort_unstable_by_key(|&(first, _)| first);
    write_rows(output, batch, routes.iter().map(|&(_, last)| last))
}

fn write_rows(
    output: &mut impl Write,
    batch: &RecordBatch,
    indices: impl Iterator<Item = usize> + Clone,
) -> Result<()> {
    let fields = batch.schema_ref().fields();
    bytes(output, b"[")?;
    for row in 0..batch.num_rows() {
        if row > 0 {
            bytes(output, b",")?;
        }
        let cells = indices.clone().map(|index| ArrowCell {
            field: &fields[index],
            array: batch.column(index).as_ref(),
            row,
        });
        write_struct_fields(output, cells, true, 0)?;
    }
    bytes(output, b"]")
}

fn bytes(output: &mut impl Write, data: &[u8]) -> Result<()> {
    output.write_all(data).map_err(invalid)
}

fn text(output: &mut impl Write, value: &str) -> Result<()> {
    if value.starts_with(['~', '^', '`']) {
        prefixed_text(output, "~", value)
    } else {
        serde_json::to_writer(output, value).map_err(invalid)
    }
}

fn prefixed_text(output: &mut impl Write, prefix: &str, value: &str) -> Result<()> {
    serde::Serializer::collect_str(
        &mut serde_json::Serializer::new(output),
        &format_args!("{prefix}{value}"),
    ).map_err(invalid)
}

fn write_struct<'a>(output: &mut impl Write, cells: impl Iterator<Item = ArrowCell<'a>>, typed_structs: bool, depth: usize) -> Result<()> {
    if !typed_structs {
        return write_struct_fields(output, cells, false, depth);
    }
    let mut positions = std::collections::HashMap::new();
    let mut unique = Vec::new();
    for cell in cells {
        let position = *positions.entry(cell.field.name()).or_insert(unique.len());
        if position == unique.len() {
            unique.push(cell);
        } else {
            unique[position] = cell;
        }
    }
    write_struct_fields(output, unique.into_iter(), true, depth)
}

fn write_struct_fields<'a>(
    output: &mut impl Write, cells: impl Iterator<Item = ArrowCell<'a>>, typed_structs: bool, depth: usize,
) -> Result<()> {
    if typed_structs { bytes(output, br#"["~#rtdb/struct","#)?; }
    bytes(output, br#"["^ ""#)?;
    for cell in cells {
        if cell.is_null()? { continue; }
        bytes(output, b",")?;
        text(output, cell.field.name())?;
        bytes(output, b",")?;
        write_cell(output, cell, false, typed_structs, depth + 1 + usize::from(typed_structs), false)?;
    }
    bytes(output, if typed_structs { b"]]" } else { b"]" })
}

fn map_cells(entry: ArrowCell<'_>) -> Result<(ArrowCell<'_>, ArrowCell<'_>)> {
    let mut fields = entry.structure().ok_or_else(|| invalid("Transit map entry has no fields"))?;
    let key = fields.next().ok_or_else(|| invalid("Transit map entry has no key"))?;
    let value = fields.next().ok_or_else(|| invalid("Transit map entry has no value"))?;
    Ok((key, value))
}

fn cell_stringable(cell: ArrowCell<'_>, typed_structs: bool) -> Result<bool> {
    let cell = cell.physical()?;
    if cell.is_null()? || cell.utf8().is_some() || cell.binary().is_some() {
        return Ok(true);
    }
    if cell.structure().is_some() || cell.sequence_kind().is_some() {
        return Ok(false);
    }
    let ArrowCell { field, array, row } = cell;
    match (field.data_type(), field.extension_type_name()) {
        (DataType::Utf8, Some("xt/clj-keyword"))
        | (DataType::FixedSizeBinary(16), Some("uuid"))
        | (DataType::FixedSizeBinary(_), None) => return Ok(true),
        (DataType::Utf8, Some("uri")) => {
            rtdb_types::UriParts::parse(array.as_string::<i32>().value(row))?;
            return Ok(true);
        }
        (DataType::Int32, Some("xt/oid" | "xt/regclass" | "xt/regproc")) => return Ok(false),
        (DataType::Binary, Some("xt/transit+msgpack")) => {
            let value = super::decode_msgpack(array.as_binary::<i32>().value(row))?;
            return Ok(stringable(&value));
        }
        (DataType::Decimal32(..) | DataType::Decimal64(..) | DataType::Decimal128(..) | DataType::Decimal256(..), None) => {
            rtdb_columnar::decimal_field_parameters(field)?;
            return Ok(true);
        }
        _ => {}
    }
    Ok(match primitive(cell, typed_structs) {
        Some(value) => value.stringable(),
        None => stringable(&read_leaf(cell)?),
    })
}

fn write_cell(output: &mut impl Write, cell: ArrowCell<'_>, key: bool, typed_structs: bool, depth: usize, top: bool) -> Result<()> {
    if depth > MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
    let cell = cell.physical()?;
    if cell.is_null()? {
        return scalar(output, if key { br#""~_""# } else { b"null" }, top);
    }
    if let Some(value) = cell.utf8() {
        if top { bytes(output, br#"["~#'","#)?; }
        text(output, value)?;
        if top { bytes(output, b"]")?; }
        return Ok(());
    }
    if let Some(value) = cell.binary() {
        quoted(output, top, |output| binary(output, value))?;
        return Ok(());
    }
    if let Some(fields) = cell.structure() {
        return write_struct(output, fields, typed_structs, depth);
    }
    match cell.sequence_kind() {
        Some(ListKind::Map { .. }) => {
            let entries = || cell.sequence().ok_or_else(|| invalid("Transit map has no child cells"));
            let mut simple = true;
            for entry in entries()? {
                if !cell_stringable(map_cells(entry)?.0, typed_structs)? {
                    simple = false;
                    break;
                }
            }
            bytes(output, if simple { br#"["^ ""# } else { br#"["~#cmap",["# })?;
            for (index, entry) in entries()?.enumerate() {
                if simple || index != 0 { bytes(output, b",")?; }
                let (key, value) = map_cells(entry)?;
                let entry_depth = depth + 1;
                write_cell(output, key, simple, typed_structs, entry_depth, false)?;
                bytes(output, b",")?;
                write_cell(output, value, false, typed_structs, entry_depth, false)?;
            }
            bytes(output, if simple { b"]" } else { b"]]" })
        }
        Some(kind @ (ListKind::List | ListKind::FixedSizeList(_) | ListKind::Set)) => {
            let set = kind == ListKind::Set;
            if set { bytes(output, br#"["~#set","#)?; }
            bytes(output, b"[")?;
            for (index, child) in cell.sequence().ok_or_else(|| invalid("Transit sequence has no child cells"))?.enumerate() {
                if index != 0 { bytes(output, b",")?; }
                write_cell(output, child, false, typed_structs, depth + 1 + usize::from(set), false)?;
            }
            bytes(output, if set { b"]]" } else { b"]" })
        }
        _ => match primitive(cell, typed_structs) {
            Some(value) => value.write(output, key, depth, top),
            None if write_typed_leaf(output, cell, key, depth, top)? => Ok(()),
            None => write_leaf(output, &read_leaf(cell)?, key, depth, top),
        },
    }
}

fn quoted<W: Write>(output: &mut W, quote: bool, write: impl FnOnce(&mut W) -> Result<()>) -> Result<()> {
    if quote { bytes(output, br#"["~#'","#)?; }
    write(output)?;
    if quote { bytes(output, b"]")?; }
    Ok(())
}

fn scalar(output: &mut impl Write, value: &[u8], top: bool) -> Result<()> {
    quoted(output, top, |output| bytes(output, value))
}

fn json(output: &mut impl Write, value: &impl serde::Serialize) -> Result<()> {
    serde_json::to_writer(output, value).map_err(invalid)
}

fn binary(output: &mut impl Write, value: &[u8]) -> Result<()> {
    bytes(output, br#""~b"#)?;
    let mut encoder = EncoderWriter::new(&mut *output, &STANDARD);
    encoder.write_all(value).map_err(invalid)?;
    encoder.finish().map_err(invalid)?;
    drop(encoder);
    bytes(output, b"\"")
}

fn integer(output: &mut impl Write, value: i64, key: bool) -> Result<()> {
    if key || !(-9_007_199_254_740_991..=9_007_199_254_740_991).contains(&value) {
        write!(output, "\"~i{value}\"").map_err(invalid)
    } else {
        json(output, &value)
    }
}

fn float(output: &mut impl Write, value: f64, key: bool) -> Result<()> {
    if !value.is_finite() {
        bytes(output, if value.is_nan() { br#""~zNaN""# } else if value.is_sign_positive() { br#""~zINF""# } else { br#""~z-INF""# })
    } else if key {
        write!(output, "\"~d{value}\"").map_err(invalid)
    } else {
        json(output, &value)
    }
}

fn tagged<W: Write>(output: &mut W, tag: &str, key: bool, depth: usize, write: impl FnOnce(&mut W) -> Result<()>) -> Result<()> {
    if key { return Err(invalid("Compound Transit tags cannot be map keys")); }
    if depth >= MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
    bytes(output, b"[")?;
    if matches!(tag, "i8" | "i16" | "i32" | "f32") {
        write!(output, "\"~#{tag}\"").map_err(invalid)?;
    } else {
        json(output, &format!("~#{tag}"))?;
    }
    bytes(output, b",")?;
    write(output)?;
    bytes(output, b"]")
}

fn write_leaf(output: &mut impl Write, value: &T, key: bool, depth: usize, top: bool) -> Result<()> {
    let quote = top && !matches!(value, T::Array(_) | T::Map(_))
        && !matches!(value, T::Tagged { tag, .. } if tag.chars().count() > 1);
    quoted(output, quote, |output| wire(output, value, key, depth))
}

fn wire(output: &mut impl Write, value: &T, key: bool, depth: usize) -> Result<()> {
    if depth > MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
    match value {
        T::Null => bytes(output, if key { br#""~_""# } else { b"null" }),
        T::Bool(value) => boolean(output, *value, key),
        T::Integer(value) => integer(output, *value, key),
        T::Float(value) => float(output, *value, key),
        T::Float32(value) if !value.is_finite() => float(output, f64::from(*value), key),
        T::Float32(value) => {
            let value = rtdb_types::java_f32_text(*value);
            if key { return json(output, &format!("~d{value}")); }
            let number: serde_json::Number = serde_json::from_str(&value).map_err(invalid)?;
            json(output, &number)
        }
        T::String(value) => text(output, value),
        T::Keyword(value) => json(output, &format!("~:{value}")),
        T::Symbol(value) => json(output, &format!("~${value}")),
        T::Bytes(value) => binary(output, value),
        T::Array(values) => {
            bytes(output, b"[")?;
            for (index, value) in values.iter().enumerate() {
                if index > 0 { bytes(output, b",")?; }
                wire(output, value, false, depth + 1)?;
            }
            bytes(output, b"]")
        }
        T::Map(entries) => {
            let simple = entries.iter().all(|(key, _)| stringable(key));
            bytes(output, if simple { br#"["^ ""# } else { br#"["~#cmap",["# })?;
            for (index, (key, value)) in entries.iter().enumerate() {
                if simple || index > 0 { bytes(output, b",")?; }
                wire(output, key, simple, depth + 1)?;
                bytes(output, b",")?;
                wire(output, value, false, depth + 1)?;
            }
            bytes(output, if simple { b"]" } else { b"]]" })
        }
        T::Tagged { tag, representation } => {
            if tag.is_empty() { return Err(invalid("Transit tag is empty")); }
            if let T::String(value) = representation.as_ref() && tag.chars().count() == 1 {
                return json(output, &format!("~{tag}{value}"));
            }
            tagged(output, tag, key, depth, |output| wire(output, representation, false, depth + 1))
        }
    }
}

fn boolean(output: &mut impl Write, value: bool, key: bool) -> Result<()> {
    bytes(output, match (value, key) {
        (true, true) => br#""~?t""#,
        (false, true) => br#""~?f""#,
        (true, false) => b"true",
        (false, false) => b"false",
    })
}

enum Primitive { Bool(bool), Int(i64, Option<&'static str>), UInt(u64), Float(f64, bool) }

impl Primitive {
    fn stringable(&self) -> bool { !matches!(self, Self::Int(_, Some(_)) | Self::Float(_, true)) }

    fn write(self, output: &mut impl Write, key: bool, depth: usize, top: bool) -> Result<()> {
        quoted(output, top && self.stringable(), |output| match self {
            Self::Bool(value) => boolean(output, value, key),
            Self::Int(value, None) => integer(output, value, key),
            Self::Int(value, Some(tag)) => tagged(output, tag, key, depth, |output| integer(output, value, false)),
            Self::UInt(value) => write!(output, "\"~n{value}\"").map_err(invalid),
            Self::Float(value, false) => float(output, value, key),
            Self::Float(value, true) => tagged(output, "f32", key, depth, |output| float(output, value, false)),
        })
    }
}

fn primitive(cell: ArrowCell<'_>, typed_structs: bool) -> Option<Primitive> {
    if !cell.field.metadata().is_empty() { return None; }
    let array = cell.array;
    let row = cell.row;
    macro_rules! int {
        ($ty:ty) => { Primitive::Int(i64::from(array.as_primitive::<$ty>().value(row)), None) };
        ($ty:ty, $tag:literal) => { Primitive::Int(i64::from(array.as_primitive::<$ty>().value(row)), typed_structs.then_some($tag)) };
    }
    Some(match cell.field.data_type() {
        DataType::Boolean => Primitive::Bool(array.as_boolean().value(row)),
        DataType::Int8 => int!(Int8Type, "i8"),
        DataType::Int16 => int!(Int16Type, "i16"),
        DataType::Int32 => int!(Int32Type, "i32"),
        DataType::Int64 => int!(Int64Type),
        DataType::UInt8 => int!(UInt8Type),
        DataType::UInt16 => int!(UInt16Type),
        DataType::UInt32 => int!(UInt32Type),
        DataType::UInt64 => {
            let value = array.as_primitive::<UInt64Type>().value(row);
            i64::try_from(value).map_or(Primitive::UInt(value), |value| Primitive::Int(value, None))
        }
        DataType::Float16 => Primitive::Float(array.as_primitive::<Float16Type>().value(row).to_f64(), false),
        DataType::Float32 => Primitive::Float(f64::from(array.as_primitive::<Float32Type>().value(row)), typed_structs),
        DataType::Float64 => Primitive::Float(array.as_primitive::<Float64Type>().value(row), false),
        _ => return None,
    })
}

fn read_leaf(cell: ArrowCell<'_>) -> Result<T> {
    value_to_transit(&rtdb_columnar::read_value(cell.field, cell.array, cell.row)?)
}

fn write_typed_leaf(output: &mut impl Write, cell: ArrowCell<'_>, key: bool, depth: usize, top: bool) -> Result<bool> {
    let ArrowCell { field, array, row } = cell;
    let extension = field.extension_type_name();
    macro_rules! value { ($ty:ty) => { array.as_primitive::<$ty>().value(row) }; }
    macro_rules! temporal {
        ($unit:expr, $second:ty, $milli:ty, $micro:ty, $nano:ty) => {
            match $unit {
                ArrowTimeUnit::Second => (value!($second), TimeUnit::Second),
                ArrowTimeUnit::Millisecond => (value!($milli), TimeUnit::Millisecond),
                ArrowTimeUnit::Microsecond => (value!($micro), TimeUnit::Microsecond),
                ArrowTimeUnit::Nanosecond => (value!($nano), TimeUnit::Nanosecond),
            }
        };
    }
    let (tag, text) = match (field.data_type(), extension) {
        (DataType::Utf8, Some("xt/clj-keyword" | "uri")) => {
            let value = array.as_string::<i32>().value(row);
            let prefix = if extension == Some("uri") { rtdb_types::UriParts::parse(value)?; "~r" } else { "~:" };
            quoted(output, top, |output| prefixed_text(output, prefix, value))?;
            return Ok(true);
        }
        (DataType::Int32, Some(tag @ ("xt/oid" | "xt/regclass" | "xt/regproc"))) => {
            tagged(output, tag, key, depth, |output| integer(output, i64::from(value!(Int32Type)), false))?;
            return Ok(true);
        }
        (DataType::Binary, Some("xt/transit+msgpack")) => {
            let value = super::decode_msgpack(array.as_binary::<i32>().value(row))?;
            write_leaf(output, &value, key, depth, top)?;
            return Ok(true);
        }
        (DataType::FixedSizeList(_, 2), Some("xt/tstz-range")) => {
            let Some((field, values, range)) = cell.sequence_range() else { return Ok(false); };
            let DataType::Timestamp(ArrowTimeUnit::Microsecond, Some(timezone)) = field.data_type() else { return Ok(false); };
            if timezone.as_ref() != "UTC" || !field.metadata().is_empty() { return Ok(false); }
            let values = values.as_primitive::<TimestampMicrosecondType>();
            if values.is_null(range.start) || values.is_null(range.start + 1) { return Ok(false); }
            tagged(output, "xtdb/tstz-range", key, depth, |output| {
                if depth + 2 > MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
                bytes(output, b"[")?;
                for (index, (row, absent)) in range.zip([i64::MIN, i64::MAX]).enumerate() {
                    if index > 0 { bytes(output, b",")?; }
                    let value = values.value(row);
                    if value == absent { bytes(output, b"null")?; }
                    else {
                        tagged(output, "time/zoned-date-time", false, depth + 2, |output| {
                            json(output, &timestamp_text(value, TimeUnit::Microsecond, Some("UTC"), TimestampText::Iso)?)
                        })?;
                    }
                }
                bytes(output, b"]")
            })?;
            return Ok(true);
        }
        (DataType::FixedSizeBinary(16), Some("uuid")) => {
            let array = array.as_any().downcast_ref::<FixedSizeBinaryArray>().expect("fixed binary layout");
            let value = uuid::Uuid::from_bytes(array.value(row).try_into().expect("16-byte UUID"));
            quoted(output, top, |output| json(output, &format!("~u{value}")))?;
            return Ok(true);
        }
        (DataType::FixedSizeBinary(_), None) => {
            let array = array.as_any().downcast_ref::<FixedSizeBinaryArray>().expect("fixed binary layout");
            quoted(output, top, |output| binary(output, array.value(row)))?;
            return Ok(true);
        }
        (DataType::Date32, None) => ("time/date", date_text(i64::from(value!(Date32Type)))?),
        (DataType::Date64, None) => ("time/date", date_text(value!(Date64Type).div_euclid(rtdb_types::java_date::MILLIS_PER_DAY))?),
        (DataType::Time32(ArrowTimeUnit::Second), None) => ("time/time", time_text(nanos(i64::from(value!(Time32SecondType)), TimeUnit::Second), false)?),
        (DataType::Time32(ArrowTimeUnit::Millisecond), None) => ("time/time", time_text(nanos(i64::from(value!(Time32MillisecondType)), TimeUnit::Millisecond), false)?),
        (DataType::Time64(ArrowTimeUnit::Microsecond), None) => ("time/time", time_text(nanos(value!(Time64MicrosecondType), TimeUnit::Microsecond), false)?),
        (DataType::Time64(ArrowTimeUnit::Nanosecond), None) => ("time/time", time_text(nanos(value!(Time64NanosecondType), TimeUnit::Nanosecond), false)?),
        (DataType::Timestamp(unit, timezone), None) => {
            let (value, unit) = temporal!(unit, TimestampSecondType, TimestampMillisecondType, TimestampMicrosecondType, TimestampNanosecondType);
            let tag = if timezone.is_some() { "time/zoned-date-time" } else { "time/date-time" };
            (tag, timestamp_text(value, unit, timezone.as_deref(), TimestampText::Iso)?)
        }
        (DataType::Duration(unit), None) => {
            let (value, unit) = temporal!(unit, DurationSecondType, DurationMillisecondType, DurationMicrosecondType, DurationNanosecondType);
            ("time/duration", rtdb_types::format_iso_duration(nanos(value, unit)))
        }
        (DataType::Interval(arrow_schema::IntervalUnit::YearMonth), None) => ("xtdb/interval", interval_text(value!(IntervalYearMonthType), 0, 0)),
        (DataType::Interval(arrow_schema::IntervalUnit::DayTime), None) => {
            let value = value!(IntervalDayTimeType);
            ("xtdb/interval", interval_text(0, value.days, i64::from(value.milliseconds) * 1_000_000))
        }
        (DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano), None | Some("xt/intervalDMMType")) => {
            let value = value!(IntervalMonthDayNanoType);
            ("xtdb/interval", interval_text(value.months, value.days, value.nanoseconds))
        }
        (_, None) => {
            let Some((parameters, coefficient)) = cell.decimal_coefficient()? else { return Ok(false); };
            let width = usize::from(parameters.bit_width / 8);
            let coefficient = num_bigint::BigInt::from_signed_bytes_be(&coefficient[coefficient.len() - width..]);
            let value = rtdb_types::jvm_number::decimal_parts_text(&coefficient, parameters.scale);
            quoted(output, top, |output| json(output, &format!("~f{value}")))?;
            return Ok(true);
        }
        _ => return Ok(false),
    };
    tagged(output, tag, key, depth, |output| json(output, &text))?;
    Ok(true)
}
