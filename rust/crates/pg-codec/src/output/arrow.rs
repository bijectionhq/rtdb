use arrow_array::{
    FixedSizeBinaryArray,
    cast::AsArray,
    types::{Float16Type, Float32Type, Float64Type},
};
use arrow_schema::DataType;
use rtdb_columnar::ArrowCell;
use rtdb_types::{ListKind, LogicalType};

use super::{
    Anomaly, ArrayElement, EncodedValue, Format, MemoryBudget, MemoryReservation, PgType, Result,
    SessionEnvironment, TimestampText, Value, Write, array_element_type, array_payload, date_binary,
    date_text, duration_text, encode, encode_value, float_text, interval_text, invalid, nanos,
    size_error, time_binary, time_text, timestamp_text, translate_output_error, unsupported,
    write_array_payload, write_bytes, write_error, write_integer, write_json, write_json_error, write_range,
    write_timestamp,
};
use std::sync::Arc;

mod scalar;
use scalar::{json_integer, range_bounds, temporal_text, write_scalar};

pub enum ArrowWrite {
    Unsupported,
    Null,
    Written,
}

/// Writes an Arrow cell's payload and leaves its length prefix to the caller.
/// Unsupported and null cells leave the writer untouched.
/// An error can leave a partial payload.
pub fn write_arrow_cell(
    ty: PgType,
    format: Format,
    cell: ArrowCell<'_>,
    environment: &SessionEnvironment,
    memory: &Arc<MemoryBudget>,
    output: &mut impl Write,
) -> Result<ArrowWrite> {
    let ty = if ty == PgType::Default {
        environment.fallback()
    } else {
        ty
    };
    match ty {
        PgType::Transit => {
            if cell.is_null()? {
                return Ok(ArrowWrite::Null);
            }
            if !transit_streamable(cell, 0) {
                return Ok(ArrowWrite::Unsupported);
            }
            let _reservation = memory.reserve(structured_bound(cell)?)?;
            write_transit(cell, output)?;
            return Ok(ArrowWrite::Written);
        }
        PgType::Int4s | PgType::Int8s | PgType::Texts | PgType::Float4s | PgType::Float8s => {
            return write_arrow_array(ty, format, cell, memory, output);
        }
        PgType::Numeric => return write_arrow_numeric(format, cell, memory, output),
        PgType::Keyword => {
            let cell = cell.physical()?;
            if cell.field.data_type() == &DataType::Utf8 && cell.field.extension_type_name() == Some("xt/clj-keyword") {
                if cell.is_null()? {
                    return Ok(ArrowWrite::Null);
                }
                write_bytes(output, cell.array.as_string::<i32>().value(cell.row).as_bytes())?;
                return Ok(ArrowWrite::Written);
            }
        }
        PgType::Int2 | PgType::Int4 | PgType::Int8 if matches!(cell.field.data_type(),
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
                | DataType::RunEndEncoded(..) | DataType::Union(..)
        ) => {
            let active = cell.physical()?;
            if matches!(active.field.data_type(), DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
                | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64)
                && active.field.extension_type_name().is_none()
            {
                if active.is_null()? {
                    return Ok(ArrowWrite::Null);
                }
                let integer = json_integer(active).expect("Integer storage");
                write_integer(ty, format, integer, output)?;
                return Ok(ArrowWrite::Written);
            }
        }
        _ => {}
    }
    if !matches!(
        ty,
        PgType::Json
            | PgType::JsonLd
            | PgType::Timestamp
            | PgType::TimestampTz
            | PgType::Date
            | PgType::Time
            | PgType::Interval
            | PgType::Duration
            | PgType::TsTzRange
            | PgType::Oid
            | PgType::RegClass
            | PgType::RegProc
    ) {
        return Ok(ArrowWrite::Unsupported);
    }
    rtdb_columnar::estimate_read_cell_bytes(cell.field, cell.array, cell.row)?;
    if cell.is_null()? {
        return Ok(ArrowWrite::Null);
    }
    if matches!(ty, PgType::Json | PgType::JsonLd) {
        json_cell(cell, ty == PgType::JsonLd, output, Some(memory), 0)?;
    } else if !write_scalar(ty, format, cell.physical()?, environment, output)? {
        return Ok(ArrowWrite::Unsupported);
    }
    Ok(ArrowWrite::Written)
}

fn write_arrow_numeric(
    format: Format,
    cell: ArrowCell<'_>,
    memory: &Arc<MemoryBudget>,
    output: &mut impl Write,
) -> Result<ArrowWrite> {
    if cell.is_null()? {
        return Ok(ArrowWrite::Null);
    }
    let Some((parameters, mut buffer)) = cell.decimal_coefficient()? else {
        return Ok(ArrowWrite::Unsupported);
    };
    let start = buffer.len() - usize::from(parameters.bit_width / 8);
    match format {
        Format::Binary => {
            let coefficient = &buffer[start..];
            crate::binary::numeric_allocation_bound(coefficient, parameters.scale)?;
            let _scratch = memory.reserve(8192)?;
            crate::binary::write_numeric(coefficient, parameters.scale, output)?;
        }
        Format::Text => {
            let _scratch = memory.reserve(8192)?;
            let sign = if buffer[start] & 0x80 != 0 { 0xff } else { 0 };
            buffer[..start].fill(sign);
            let decimal = rtdb_types::jvm_number::decimal_coefficient_display(buffer, parameters.scale);
            write!(output, "{decimal}").map_err(write_error)?;
        }
    }
    Ok(ArrowWrite::Written)
}

pub fn encode_arrow(
    ty: PgType,
    format: Format,
    logical: &LogicalType,
    cell: ArrowCell<'_>,
    environment: &SessionEnvironment,
    memory: &Arc<MemoryBudget>,
) -> Result<Option<EncodedValue>> {
    if cell.is_null()? {
        return Ok(None);
    }
    let ty = if ty == PgType::Default {
        environment.fallback()
    } else {
        ty
    };
    let decimal = if ty == PgType::Numeric {
        cell.decimal_coefficient()?
    } else {
        None
    };
    if let Some((parameters, buffer)) = decimal {
        let width = usize::from(parameters.bit_width / 8);
        let coefficient = &buffer[buffer.len() - width..];
        let binary_bytes = if format == Format::Binary {
            crate::binary::numeric_allocation_bound(coefficient, parameters.scale)?
        } else {
            0
        };
        let bound = 8192usize.checked_add(binary_bytes).ok_or_else(size_error)?;
        let reservation = memory.reserve(bound)?;
        let bytes = if format == Format::Binary {
            crate::binary::numeric_write(coefficient, parameters.scale)?
        } else {
            decimal_parts_text(coefficient, parameters.scale).into_bytes()
        };
        return settle(bytes, reservation);
    }
    if matches!(ty, PgType::Json | PgType::JsonLd | PgType::Transit) {
        let reservation = memory.reserve(structured_bound(cell)?)?;
        let mut bytes = Vec::new();
        if ty == PgType::Transit {
            write_transit(cell, &mut bytes)?;
        } else {
            json_cell(cell, ty == PgType::JsonLd, &mut bytes, None, 0)?;
        }
        return settle(bytes, reservation);
    }
    if matches!(ty, PgType::Int4s | PgType::Int8s | PgType::Texts | PgType::Float4s | PgType::Float8s)
        && matches!(cell.sequence_kind(), Some(ListKind::List | ListKind::FixedSizeList(_)))
        && let Some(values) = cell.sequence()
    {
        let reservation = memory.reserve(array_bound(ty, cell, values.len())?)?;
        let bytes = array_payload(ty, format, values, |element, cell| {
            let cell = cell.physical()?;
            if let Some(value) = typed_array_element(element, cell)? {
                return Ok(value);
            }
            encode_value(element, format, &read_leaf(cell)?, environment).map(ArrayElement::Encoded)
        })?;
        return settle(bytes, reservation);
    }
    let _scratch = memory.reserve(rtdb_columnar::estimate_read_cell_bytes(cell.field, cell.array, cell.row)?)?;
    let value = rtdb_columnar::read_value_as(logical, cell.field, cell.array, cell.row)?;
    encode(ty, format, &value, environment, memory)
}

fn structured_bound(cell: ArrowCell<'_>) -> Result<usize> {
    cell.payload_bytes()?
        .checked_mul(32)
        .and_then(|bytes| bytes.checked_add(8192))
        .ok_or_else(size_error)
}

fn write_transit(cell: ArrowCell<'_>, output: &mut impl Write) -> Result<()> {
    rtdb_format::transit::write_arrow_json(output, cell).map_err(translate_output_error)
}

fn transit_streamable(cell: ArrowCell<'_>, depth: usize) -> bool {
    if depth > rtdb_types::transit_value::MAX_DEPTH {
        return false;
    }
    let Ok(cell) = cell.physical() else { return false };
    if matches!(cell.is_null(), Ok(true)) {
        return true;
    }
    // MessagePack cache references can expand past the owned codec's wire reservation.
    if cell.field.extension_type_name() == Some("xt/transit+msgpack") {
        return false;
    }
    if let Some(mut children) = cell.structure() {
        return children.all(|child| transit_streamable(child, depth + 1));
    }
    if let Some(mut children) = cell.sequence() {
        return children.all(|child| transit_streamable(child, depth + 1));
    }
    true
}

fn array_bound(ty: PgType, cell: ArrowCell<'_>, len: usize) -> Result<usize> {
    let element_bytes = if matches!(ty, PgType::Float4s | PgType::Float8s) {
        len.checked_mul(64).ok_or_else(size_error)?
    } else {
        0
    };
    cell.payload_bytes()?
        .checked_mul(16)
        .and_then(|bytes| bytes.checked_add(element_bytes))
        .and_then(|bytes| bytes.checked_add(8192))
        .ok_or_else(size_error)
}

fn write_arrow_array(
    ty: PgType,
    format: Format,
    cell: ArrowCell<'_>,
    memory: &Arc<MemoryBudget>,
    output: &mut impl Write,
) -> Result<ArrowWrite> {
    if cell.is_null()? {
        return Ok(ArrowWrite::Null);
    }
    if !matches!(cell.sequence_kind(), Some(ListKind::List | ListKind::FixedSizeList(_))) {
        return Ok(ArrowWrite::Unsupported);
    }
    let Some((field, array, range)) = cell.sequence_range() else {
        return Ok(ArrowWrite::Unsupported);
    };
    let values = || range.clone().map(move |row| ArrowCell { field, array, row });
    let element = array_element_type(ty);
    let mut has_nulls = false;
    for value in values() {
        let Ok(Some(value)) = value.physical().and_then(|value| typed_array_element(element, value))
        else {
            return Ok(ArrowWrite::Unsupported);
        };
        has_nulls |= matches!(value, ArrayElement::Null);
    }
    let _reservation = memory.reserve(array_bound(ty, cell, range.len())?)?;
    write_array_payload(
        ty,
        format,
        values(),
        has_nulls,
        |element, value| typed_array_element(element, value.physical()?)?.ok_or_else(size_error),
        output,
    )?;
    Ok(ArrowWrite::Written)
}

fn typed_array_element(element: PgType, cell: ArrowCell<'_>) -> Result<Option<ArrayElement<'_>>> {
    if cell.is_null()? {
        return Ok(Some(ArrayElement::Null));
    }
    if matches!(element, PgType::Int4 | PgType::Int8) {
        return Ok(cell.integer().map(ArrayElement::Integer));
    }
    if element == PgType::Text {
        let text = if cell.field.data_type() == &DataType::Utf8
            && cell.field.extension_type_name() == Some("xt/clj-keyword")
        {
            Some(cell.array.as_string::<i32>().value(cell.row))
        } else {
            cell.utf8()
        };
        return Ok(text.map(ArrayElement::Text));
    }
    if !cell.field.metadata().is_empty() {
        return Ok(None);
    }
    let ArrowCell { field, array, row } = cell;
    let typed = match (element, field.data_type()) {
        (PgType::Float4, DataType::Float32) =>
            Some(ArrayElement::Float4(array.as_primitive::<Float32Type>().value(row))),
        (PgType::Float4, DataType::Float64) =>
            Some(ArrayElement::Float4(array.as_primitive::<Float64Type>().value(row) as f32)),
        (PgType::Float8, DataType::Float32) =>
            Some(ArrayElement::Float8(f64::from(array.as_primitive::<Float32Type>().value(row)))),
        (PgType::Float8, DataType::Float64) =>
            Some(ArrayElement::Float8(array.as_primitive::<Float64Type>().value(row))),
        _ => {
            let number = match field.data_type() {
                DataType::Float16 => Some(array.as_primitive::<Float16Type>().value(row).to_f64()),
                _ => cell.integer().map(|value| value as f64),
            };
            match (element, number) {
                (PgType::Float4, Some(number)) => Some(ArrayElement::Float4(number as f32)),
                (PgType::Float8, Some(number)) => Some(ArrayElement::Float8(number)),
                _ => None,
            }
        }
    };
    Ok(typed)
}

fn settle(bytes: Vec<u8>, reservation: Arc<MemoryReservation>) -> Result<Option<EncodedValue>> {
    if bytes.capacity() > reservation.bytes() {
        return Err(size_error());
    }
    Ok(Some(EncodedValue {
        bytes,
        _reservation: reservation,
    }))
}

fn decimal_parts_text(coefficient: &[u8], scale: i32) -> String {
    rtdb_types::jvm_number::decimal_parts_text(
        &num_bigint::BigInt::from_signed_bytes_be(coefficient),
        scale,
    )
}

fn read_leaf(cell: ArrowCell<'_>) -> Result<Value> {
    rtdb_columnar::read_value(cell.field, cell.array, cell.row)
}

fn json_cell(
    cell: ArrowCell<'_>,
    ld: bool,
    output: &mut impl Write,
    memory: Option<&Arc<MemoryBudget>>,
    depth: usize,
) -> Result<()> {
    if depth > 128 {
        return Err(size_error());
    }
    let cell = cell.physical()?;
    if cell.is_null()? {
        return write_bytes(output, b"null");
    }
    if let Some(text) = cell.utf8() {
        return write_json(output, &text);
    }
    if let Some((parameters, buffer)) = cell.decimal_coefficient()? {
        let coefficient = &buffer[buffer.len() - usize::from(parameters.bit_width / 8)..];
        let text = decimal_parts_text(coefficient, parameters.scale);
        return if ld {
            tagged(output, "xt:decimal", &text)
        } else {
            write_bytes(output, text.as_bytes())
        };
    }
    if let Some(children) = cell.structure() {
        write_bytes(output, b"{")?;
        let mut comma = false;
        for child in children {
            if child.is_null()? {
                continue;
            }
            if comma {
                write_bytes(output, b",")?;
            }
            comma = true;
            write_json(output, child.field.name())?;
            write_bytes(output, b":")?;
            json_cell(child, ld, output, memory, depth + 1)?;
        }
        return write_bytes(output, b"}");
    }
    match cell.sequence_kind() {
        Some(ListKind::Map { .. }) => {
            write_bytes(output, b"{")?;
            for (index, entry) in cell.sequence().ok_or_else(size_error)?.enumerate() {
                let (key, value) = map_entry(entry)?;
                if index > 0 {
                    write_bytes(output, b",")?;
                }
                json_map_key(key, output, memory)?;
                write_bytes(output, b":")?;
                json_cell(value, ld, output, memory, depth + 1)?;
            }
            write_bytes(output, b"}")
        }
        Some(kind @ (ListKind::List | ListKind::FixedSizeList(_) | ListKind::Set)) => {
            let set = ld && kind == ListKind::Set;
            if set {
                write_bytes(output, b"{\"@type\":\"xt:set\",\"@value\":")?;
            }
            write_bytes(output, b"[")?;
            for (index, child) in cell.sequence().ok_or_else(size_error)?.enumerate() {
                if index > 0 {
                    write_bytes(output, b",")?;
                }
                json_cell(child, ld, output, memory, depth + 1)?;
            }
            write_bytes(output, if set { b"]}" } else { b"]" })
        }
        _ => json_leaf(cell, ld, output, memory),
    }
}

fn tagged(output: &mut impl Write, tag: &str, text: &str) -> Result<()> {
    write_bytes(output, b"{\"@type\":")?;
    write_json(output, &tag)?;
    write_bytes(output, b",\"@value\":")?;
    write_json(output, &text)?;
    write_bytes(output, b"}")
}

fn json_scalar_text(output: &mut impl Write, ld: bool, tag: &str, text: &str) -> Result<()> {
    if ld {
        tagged(output, tag, text)
    } else {
        write_json(output, &text)
    }
}

fn json_leaf(
    cell: ArrowCell<'_>,
    ld: bool,
    output: &mut impl Write,
    memory: Option<&Arc<MemoryBudget>>,
) -> Result<()> {
    if let Some(value) = json_integer(cell) {
        return write!(output, "{value}").map_err(write_error);
    }
    let ArrowCell { field, array, row } = cell;
    match (field.data_type(), field.extension_type_name()) {
        (DataType::Boolean, None) => write_bytes(
            output,
            if array.as_boolean().value(row) {
                b"true"
            } else {
                b"false"
            },
        ),
        (DataType::Float32 | DataType::Float64, None) => {
            let single = (field.data_type() == &DataType::Float32)
                .then(|| array.as_primitive::<Float32Type>().value(row));
            let value =
                single.map_or_else(|| array.as_primitive::<Float64Type>().value(row), f64::from);
            if !value.is_finite() {
                return Err(invalid("JSON cannot encode a non-finite number"));
            }
            write_bytes(output, float_text(value, single).as_bytes())
        }
        (DataType::Utf8, Some("xt/clj-keyword")) => json_scalar_text(
            output,
            ld,
            "xt:keyword",
            array.as_string::<i32>().value(row),
        ),
        (DataType::FixedSizeBinary(16), Some("uuid")) => {
            let bytes = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("A UUID uses fixed-size binary")
                .value(row);
            let uuid = uuid::Uuid::from_slice(bytes).map_err(invalid)?;
            json_scalar_text(
                output,
                ld,
                "xt:uuid",
                uuid.hyphenated()
                    .encode_lower(&mut uuid::Uuid::encode_buffer()),
            )
        }
        (DataType::Binary, Some("xt/transit+msgpack")) => {
            let bytes = array.as_binary::<i32>().value(row);
            let _scratch = memory
                .map(|memory| {
                    let bound = bytes
                        .len()
                        .checked_mul(32)
                        .and_then(|n| n.checked_add(8192))
                        .ok_or_else(size_error)?;
                    memory.reserve(bound)
                })
                .transpose()?;
            let error = rtdb_format::transit::decode_anomaly(bytes)?;
            write_json_error(&error, ld, output)
        }
        _ => {
            if let Some((tag, text)) = temporal_text(cell)? {
                let text = if !ld && tag == "xt:timestamptz" {
                    text.split('[').next().unwrap_or(&text)
                } else {
                    &text
                };
                return json_scalar_text(output, ld, tag, text);
            }
            validate_unrenderable_leaf(cell, memory)?;
            Err(json_representation_error())
        }
    }
}

fn json_map_key(
    key: ArrowCell<'_>,
    output: &mut impl Write,
    memory: Option<&Arc<MemoryBudget>>,
) -> Result<()> {
    let key = key.physical()?;
    if let Some(text) = key.utf8() {
        return write_json(output, &text);
    }
    if key.is_null()? {
        return write_bytes(output, b"\"null\"");
    }
    if let Some(value) = json_integer(key) {
        return write!(output, "\"{value}\"").map_err(write_error);
    }
    let ArrowCell { field, array, row } = key;
    match (field.data_type(), field.extension_type_name()) {
        (DataType::Boolean, None) => write_bytes(
            output,
            if array.as_boolean().value(row) {
                b"\"true\""
            } else {
                b"\"false\""
            },
        ),
        (DataType::Utf8, Some("xt/clj-keyword")) => {
            write_json(output, &array.as_string::<i32>().value(row))
        }
        (DataType::FixedSizeBinary(16), Some("uuid")) => json_leaf(key, false, output, None),
        _ => {
            if let Some((_, text)) = temporal_text(key)? {
                return write_json(output, &text);
            }
            validate_unrenderable_leaf(key, memory)?;
            Err(json_representation_error())
        }
    }
}

fn validate_unrenderable_leaf(
    cell: ArrowCell<'_>,
    memory: Option<&Arc<MemoryBudget>>,
) -> Result<()> {
    if cell.field.extension_type_name() == Some("uri") {
        rtdb_types::UriParts::parse(cell.array.as_string::<i32>().value(cell.row))?;
        return Ok(());
    }
    let needs_layout_validation = match cell.sequence_kind() {
        Some(ListKind::TimestampTzRange) => range_bounds(cell)?.is_none(),
        Some(_) => true,
        None => cell.structure().is_some(),
    };
    if needs_layout_validation {
        let _scratch = memory
            .map(|memory| {
                memory.reserve(rtdb_columnar::estimate_read_value_bytes(
                    cell.field, cell.array, cell.row,
                )?)
            })
            .transpose()?;
        read_leaf(cell)?;
    }
    Ok(())
}

fn json_representation_error() -> Anomaly {
    Anomaly::incorrect(
        "xtdb/json-encode-error",
        "Value has no pinned JSON representation",
    )
}

fn map_entry(cell: ArrowCell<'_>) -> Result<(ArrowCell<'_>, ArrowCell<'_>)> {
    let mut fields = cell.structure().ok_or_else(size_error)?;
    let key = fields.next().ok_or_else(size_error)?;
    let value = fields.next().ok_or_else(size_error)?;
    Ok((key, value))
}

#[cfg(test)]
mod tests {
    use super::{
        ArrowCell, ArrowWrite, Format, MemoryBudget, PgType, SessionEnvironment, Value, encode,
        encode_arrow, encode_value, write_arrow_cell,
    };
    use rtdb_format::transit::TransitValue;
    use rtdb_types::{DecimalValue, EntityId, Keyword, StructFields};

    #[test]
    fn arrow_array_codecs_emit_the_reference_row_bytes() {
        let memory = MemoryBudget::new(1 << 20);
        let environment = SessionEnvironment::default();
        let cases = [
            (PgType::Int4s, Value::list(vec![Value::I32(i32::MIN), Value::I32(17)])),
            (PgType::Int8s, Value::list(vec![Value::I64(i64::MIN), Value::I64(i64::MAX)])),
            (PgType::Texts, Value::list(vec![
                Value::Utf8("hello,\"\\é".into()),
                Value::Utf8("NULL".into()),
                Value::Utf8(String::new()),
                Value::Null,
            ])),
            (PgType::Texts, Value::list(vec![
                Value::Keyword(Keyword::from_name("named/a\\\"é")),
                Value::Utf8("plain".into()),
                Value::Null,
            ])),
            (PgType::Float4s, Value::list(vec![
                Value::F32(-0.0),
                Value::Null,
                Value::F32(f32::from_bits(0x7fc0_1345)),
                Value::F32(f32::INFINITY),
            ])),
            (PgType::Float8s, Value::list(vec![
                Value::F32(0.2),
                Value::I64(i64::MAX),
                Value::F64(-0.0),
                Value::F64(f64::NEG_INFINITY),
                Value::Null,
            ])),
            (PgType::Float8s, Value::list(vec![])),
            (PgType::Transit, Value::list(vec![Value::F32(0.2), Value::Null, Value::F64(-0.0)])),
        ];
        for (ty, value) in cases {
            let logical = value.logical_type();
            let array = rtdb_columnar::copy_array(&logical, std::slice::from_ref(&value)).unwrap();
            let field = rtdb_columnar::to_field("result", &logical).unwrap();
            let cell = ArrowCell { field: &field, array: array.as_ref(), row: 0 };
            for format in [Format::Text, Format::Binary] {
                let expected = encode_value(ty, format, &value, &environment).unwrap();
                let mut bytes = Vec::new();
                assert!(matches!(
                    write_arrow_cell(ty, format, cell, &environment, &memory, &mut bytes).unwrap(),
                    ArrowWrite::Written
                ));
                assert_eq!(bytes, expected, "{ty:?} {format:?}");
                if ty == PgType::Texts && format == Format::Text && expected.starts_with(b"{\"hello") {
                    assert_eq!(bytes.as_slice(), r#"{"hello,\"\\é","NULL","",NULL}"#.as_bytes());
                }
            }
        }
    }

    #[test]
    fn binary_arrow_arrays_preserve_element_lengths_and_float_bits() {
        let value = Value::FixedSizeList(vec![Value::F32(f32::from_bits(0x7fa0_0001)), Value::Null].into());
        let logical = value.logical_type();
        let field = rtdb_columnar::to_field("result", &logical).unwrap();
        let array = rtdb_columnar::copy_array(&logical, &[value]).unwrap();
        let cell = ArrowCell { field: &field, array: array.as_ref(), row: 0 };
        let mut output = Vec::new();
        let written = write_arrow_cell(
            PgType::Float4s, Format::Binary, cell, &SessionEnvironment::default(),
            &MemoryBudget::new(1 << 20), &mut output,
        ).unwrap();
        assert!(matches!(written, ArrowWrite::Written));
        let words = [1i32, 1, 700, 2, 1, 4, 0x7fa0_0001, -1];
        assert_eq!(output, words.into_iter().flat_map(i32::to_be_bytes).collect::<Vec<_>>());
    }

    #[test]
    fn arrow_integer_array_admission_precedes_binary_null_refusal() {
        let value = Value::list(vec![Value::I64(-1), Value::Null]);
        let logical = value.logical_type();
        let field = rtdb_columnar::to_field("result", &logical).unwrap();
        let array = rtdb_columnar::copy_array(&logical, std::slice::from_ref(&value)).unwrap();
        let cell = ArrowCell { field: &field, array: array.as_ref(), row: 0 };
        let environment = SessionEnvironment::default();
        let mut output = Vec::new();
        let error = write_arrow_cell(
            PgType::Int8s, Format::Binary, cell, &environment, &MemoryBudget::new(0), &mut output,
        ).err().unwrap();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert!(output.is_empty());

        let error = write_arrow_cell(
            PgType::Int8s, Format::Binary, cell, &environment, &MemoryBudget::new(1 << 20), &mut output,
        ).err().unwrap();
        assert_eq!(error, encode_value(PgType::Int8s, Format::Binary, &value, &environment).unwrap_err());
    }

    #[test]
    fn arrow_array_with_incompatible_element_declines_before_writing() {
        let value = Value::list(vec![
            Value::Utf8("plain".into()),
            Value::I32(17),
        ]);
        let logical = value.logical_type();
        let field = rtdb_columnar::to_field("result", &logical).unwrap();
        let array = rtdb_columnar::copy_array(&logical, std::slice::from_ref(&value)).unwrap();
        let cell = ArrowCell { field: &field, array: array.as_ref(), row: 0 };
        let environment = SessionEnvironment::default();
        let memory = MemoryBudget::new(1 << 20);
        for format in [Format::Text, Format::Binary] {
            let mut output = b"previous cell".to_vec();
            let written = write_arrow_cell(PgType::Texts, format, cell, &environment, &memory, &mut output).unwrap();
            assert!(matches!(written, ArrowWrite::Unsupported));
            assert_eq!(output.as_slice(), b"previous cell");
            let actual = encode_arrow(PgType::Texts, format, &logical, cell, &environment, &memory).err().unwrap();
            let expected = encode_value(PgType::Texts, format, &value, &environment).unwrap_err();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn arrow_keyword_and_unsigned_cells_keep_wire_bytes_and_integer_width_errors() {
        let environment = SessionEnvironment::default();
        let memory = MemoryBudget::new(1 << 20);
        let cases = [
            (PgType::Keyword, Value::Keyword(Keyword::from_name("topic/été"))),
            (PgType::Int2, Value::U16(i16::MAX as u16)),
            (PgType::Int4, Value::U32(i32::MAX as u32)),
            (PgType::Int8, Value::U64(u64::MAX)),
        ];
        for (ty, value) in cases {
            let logical = value.logical_type();
            let field = rtdb_columnar::to_field("result", &logical).unwrap();
            let array = rtdb_columnar::copy_array(&logical, std::slice::from_ref(&value)).unwrap();
            let cell = ArrowCell { field: &field, array: array.as_ref(), row: 0 };
            for format in [Format::Text, Format::Binary] {
                let mut output = Vec::new();
                let actual = write_arrow_cell(ty, format, cell, &environment, &memory, &mut output);
                match encode_value(ty, format, &value, &environment) {
                    Ok(expected) => {
                        assert!(matches!(actual.unwrap(), ArrowWrite::Written));
                        assert_eq!(output, expected);
                    }
                    Err(error) => {
                        assert_eq!(actual.err().unwrap(), error);
                        assert!(output.is_empty());
                    }
                }
            }
        }
    }

    #[test]
    fn arrow_integer_and_keyword_cells_use_the_reached_union_leg() {
        let values = [Value::I32(-17), Value::Keyword(Keyword::from_name("active/key")), Value::U32(40)];
        let logical = rtdb_types::LogicalType::from_legs(values.iter().map(Value::logical_type));
        let field = rtdb_columnar::to_field("result", &logical).unwrap();
        let array = rtdb_columnar::copy_array(&logical, &values).unwrap();
        let environment = SessionEnvironment::default();
        let memory = MemoryBudget::new(1 << 20);
        for (row, (ty, expected)) in [
            (PgType::Int4, (-17i32).to_be_bytes().as_slice()),
            (PgType::Keyword, b"active/key"),
            (PgType::Int4, 40i32.to_be_bytes().as_slice()),
        ].into_iter().enumerate() {
            let cell = ArrowCell { field: &field, array: array.as_ref(), row };
            let mut output = Vec::new();
            let written = write_arrow_cell(ty, Format::Binary, cell, &environment, &memory, &mut output).unwrap();
            assert!(matches!(written, ArrowWrite::Written));
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn arrow_json_list_does_not_reserve_row_decoder_scratch_per_item() {
        let values = Value::list(vec![Value::I64(0); 20_000]);
        let logical = values.logical_type();
        let array = rtdb_columnar::copy_array(&logical, &[values]).unwrap();
        let field = rtdb_columnar::to_field("values", &logical).unwrap();
        let cell = ArrowCell {
            array: array.as_ref(),
            field: &field,
            row: 0,
        };
        let memory = MemoryBudget::new(48 << 20);
        let wire = encode_arrow(
            PgType::Json,
            Format::Text,
            &logical,
            cell,
            &SessionEnvironment::default(),
            &memory,
        )
        .unwrap()
        .unwrap();
        assert_eq!(wire.as_slice().len(), 40_001);
    }

    #[test]
    fn arrow_transit_keeps_cached_message_pack_expansion_within_its_wire_budget() {
        let keyword = TransitValue::Keyword("k".repeat(4096));
        let payload =
            rtdb_format::transit::encode_msgpack(&TransitValue::Array(vec![keyword; 30])).unwrap();
        let value = Value::Transit(payload);
        let logical = value.logical_type();
        let array = rtdb_columnar::copy_array(&logical, std::slice::from_ref(&value)).unwrap();
        let field = rtdb_columnar::to_field("values", &logical).unwrap();
        let cell = ArrowCell {
            array: array.as_ref(),
            field: &field,
            row: 0,
        };
        let memory = MemoryBudget::new(1 << 20);
        let environment = SessionEnvironment::default();
        let mut direct = b"previous cell".to_vec();
        assert!(matches!(
            write_arrow_cell(PgType::Transit, Format::Text, cell, &environment, &memory, &mut direct).unwrap(),
            ArrowWrite::Unsupported
        ));
        assert_eq!(direct.as_slice(), b"previous cell");
        let arrow = encode_arrow(
            PgType::Transit,
            Format::Text,
            &logical,
            cell,
            &environment,
            &memory,
        )
        .unwrap()
        .unwrap();
        let row = encode(PgType::Transit, Format::Text, &value, &environment, &memory)
            .unwrap()
            .unwrap();
        assert_eq!(arrow.as_slice(), row.as_slice());
    }

    #[test]
    fn structured_arrow_output_matches_postgres_wire_encodings() {
        let decimal = |coefficient: i128, scale| {
            Value::Decimal(
                DecimalValue::new(coefficient.to_be_bytes().to_vec(), 20, scale, 128).unwrap(),
            )
        };
        let record = Value::Struct(StructFields::from([
            (
                "id".into(),
                EntityId::uuid("6ebd7d7e-1f4b-4332-a36e-6048a03e38ed")
                    .unwrap()
                    .as_value(),
            ),
            (
                "nested".into(),
                Value::list(vec![
                    Value::Struct(StructFields::from([
                        ("null".into(), Value::Null),
                        ("number".into(), decimal(-123, 129)),
                    ])),
                    Value::Null,
                ]),
            ),
            (
                "set".into(),
                Value::Set(vec![Value::Utf8("escape \"\\".into()), Value::I64(-17)]),
            ),
            (
                "map".into(),
                Value::Map {
                    sorted: false,
                    entries: vec![(Value::Utf8("kept".into()), Value::Null)],
                },
            ),
        ]));
        let cases = [
            (PgType::Json, record.clone()),
            (PgType::JsonLd, record.clone()),
            (PgType::Transit, record),
            (PgType::Numeric, decimal(-123, 129)),
            (PgType::Numeric, decimal(3, -130)),
        ];
        let environment = SessionEnvironment::default();
        let memory = MemoryBudget::new(1 << 24);
        for (ty, value) in cases {
            let logical = value.logical_type();
            let field = rtdb_columnar::to_field("out", &logical).unwrap();
            let array = rtdb_columnar::copy_array(&logical, std::slice::from_ref(&value)).unwrap();
            let cell = ArrowCell {
                field: &field,
                array: array.as_ref(),
                row: 0,
            };
            for format in [Format::Text, Format::Binary] {
                let expected = encode(ty, format, &value, &environment, &memory)
                    .unwrap()
                    .unwrap();
                let encoded = encode_arrow(ty, format, &logical, cell, &environment, &memory)
                    .unwrap()
                    .unwrap();
                assert_eq!(encoded.as_slice(), expected.as_slice(), "{ty:?} {format:?}");
                if ty == PgType::Transit {
                    let mut output = Vec::new();
                    assert!(matches!(
                        write_arrow_cell(ty, format, cell, &environment, &memory, &mut output).unwrap(),
                        ArrowWrite::Written
                    ));
                    assert_eq!(output.as_slice(), expected.as_slice());
                }
            }
        }
    }
}
