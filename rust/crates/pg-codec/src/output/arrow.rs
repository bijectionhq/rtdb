use arrow_array::{cast::AsArray, types::{Float32Type, Float64Type}};
use arrow_schema::DataType;
use rtdb_columnar::ArrowCell;
use rtdb_types::{ListKind, LogicalType};

use super::*;

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
        let bound = cell
            .payload_bytes()?
            .checked_mul(32)
            .and_then(|n| n.checked_add(8192))
            .ok_or_else(size_error)?;
        let reservation = memory.reserve(bound)?;
        let bytes = if ty == PgType::Transit {
            let mut output = Vec::new();
            rtdb_format::transit::write_arrow_json(&mut output, cell).map_err(translate_output_error)?;
            output
        } else {
            let mut output = String::new();
            json_cell(cell, ty == PgType::JsonLd, &mut output, 0)?;
            output.into_bytes()
        };
        return settle(bytes, reservation);
    }
    if matches!(ty, PgType::Int4s | PgType::Int8s | PgType::Texts | PgType::Float4s | PgType::Float8s)
        && matches!(cell.sequence_kind(), Some(ListKind::List | ListKind::FixedSizeList(_)))
        && let Some(values) = cell.sequence()
    {
        let element_bytes = if matches!(ty, PgType::Float4s | PgType::Float8s) {
            values.len().checked_mul(64).ok_or_else(size_error)?
        } else { 0 };
        let bound = cell.payload_bytes()?.checked_mul(16).and_then(|bytes| bytes.checked_add(element_bytes)).and_then(|bytes| bytes.checked_add(8192)).ok_or_else(size_error)?;
        let reservation = memory.reserve(bound)?;
        let bytes = array_payload(ty, format, values, |element, cell| {
            let cell = cell.physical()?;
            if cell.is_null()? { return Ok(None); }
            if matches!(element, PgType::Int4 | PgType::Int8) && let Some(value) = cell.integer() {
                return Ok(Some(integer_bytes(element, format, value)));
            }
            if element == PgType::Text && let Some(text) = cell.utf8() {
                return Ok(Some(text.as_bytes().to_vec()));
            }
            if cell.field.metadata().is_empty() {
                match (element, cell.field.data_type()) {
                    (PgType::Float4, DataType::Float32) =>
                        return Ok(Some(float4_bytes(format, cell.array.as_primitive::<Float32Type>().value(cell.row)))),
                    (PgType::Float8, DataType::Float32) =>
                        return Ok(Some(float8_bytes(format, f64::from(cell.array.as_primitive::<Float32Type>().value(cell.row))))),
                    (PgType::Float8, DataType::Float64) =>
                        return Ok(Some(float8_bytes(format, cell.array.as_primitive::<Float64Type>().value(cell.row)))),
                    _ => {}
                }
            }
            encode_value(element, format, &read_leaf(cell)?, environment).map(Some)
        })?;
        return settle(bytes, reservation);
    }
    let _scratch = memory.reserve(rtdb_columnar::estimate_read_cell_bytes(cell.field, cell.array, cell.row)?)?;
    let value = rtdb_columnar::read_value_as(logical, cell.field, cell.array, cell.row)?;
    encode(ty, format, &value, environment, memory)
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

fn json_cell(cell: ArrowCell<'_>, ld: bool, output: &mut String, depth: usize) -> Result<()> {
    if depth > 128 {
        return Err(size_error());
    }
    let cell = cell.physical()?;
    if cell.is_null()? {
        output.push_str("null");
        return Ok(());
    }
    if let Some(text) = cell.utf8() {
        return json_string(text, output);
    }
    if let Some(integer) = cell.integer() {
        use std::fmt::Write;
        write!(output, "{integer}").map_err(invalid)?;
        return Ok(());
    }
    if cell.field.metadata().is_empty() {
        match cell.field.data_type() {
            DataType::Boolean => {
                output.push_str(if cell.array.as_boolean().value(cell.row) { "true" } else { "false" });
                return Ok(());
            }
            DataType::Float32 => {
                let value = cell.array.as_primitive::<Float32Type>().value(cell.row);
                if !value.is_finite() { return Err(invalid("JSON cannot encode a non-finite number")); }
                output.push_str(&float_text(f64::from(value), Some(value)));
                return Ok(());
            }
            DataType::Float64 => {
                let value = cell.array.as_primitive::<Float64Type>().value(cell.row);
                if !value.is_finite() { return Err(invalid("JSON cannot encode a non-finite number")); }
                output.push_str(&float_text(value, None));
                return Ok(());
            }
            _ => (),
        }
    }
    if let Some((parameters, buffer)) = cell.decimal_coefficient()? {
        let coefficient = &buffer[buffer.len() - usize::from(parameters.bit_width / 8)..];
        let text = decimal_parts_text(coefficient, parameters.scale);
        if ld {
            tagged_json("xt:decimal", &text, output)?;
        } else {
            output.push_str(&text);
        }
        return Ok(());
    }
    if let Some(children) = cell.structure() {
        output.push('{');
        let mut comma = false;
        for child in children {
            if child.is_null()? {
                continue;
            }
            if comma {
                output.push(',');
            }
            comma = true;
            json_string(child.field.name(), output)?;
            output.push(':');
            json_cell(child, ld, output, depth + 1)?;
        }
        output.push('}');
        return Ok(());
    }
    match cell.sequence_kind() {
        Some(ListKind::Map { .. }) => {
            output.push('{');
            for (index, entry) in cell.sequence().ok_or_else(size_error)?.enumerate() {
                let (key, value) = map_entry(entry)?;
                if index > 0 {
                    output.push(',');
                }
                if let Some(text) = key.utf8() {
                    json_string(text, output)?;
                } else {
                    json_string(&display_key(&read_leaf(key)?)?, output)?;
                }
                output.push(':');
                json_cell(value, ld, output, depth + 1)?;
            }
            output.push('}');
        }
        Some(kind @ (ListKind::List | ListKind::FixedSizeList(_) | ListKind::Set)) => {
            let set = ld && kind == ListKind::Set;
            if set {
                output.push_str("{\"@type\":\"xt:set\",\"@value\":");
            }
            output.push('[');
            for (index, child) in cell.sequence().ok_or_else(size_error)?.enumerate() {
                if index > 0 {
                    output.push(',');
                }
                json_cell(child, ld, output, depth + 1)?;
            }
            output.push(']');
            if set {
                output.push('}');
            }
        }
        _ => return json(&read_leaf(cell)?, ld, output, depth),
    }
    Ok(())
}

fn map_entry(cell: ArrowCell<'_>) -> Result<(ArrowCell<'_>, ArrowCell<'_>)> {
    let mut fields = cell.structure().ok_or_else(size_error)?;
    let key = fields.next().ok_or_else(size_error)?;
    let value = fields.next().ok_or_else(size_error)?;
    Ok((key, value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::{DecimalValue, EntityId, StructFields};

    #[test]
    fn arrow_array_codecs_emit_the_reference_row_bytes() {
        let memory = MemoryBudget::new(1 << 20);
        let environment = SessionEnvironment::default();
        let cases = [
            (PgType::Int4s, Value::list(vec![Value::I32(-4), Value::I32(17)])),
            (PgType::Texts, Value::list(vec![Value::Utf8("hello, world".into()), Value::Null])),
            (PgType::Transit, Value::list(vec![Value::F32(0.2), Value::Null, Value::F64(-0.0)])),
        ];
        for (ty, value) in cases {
            let logical = value.logical_type();
            let array = rtdb_columnar::copy_array(&logical, std::slice::from_ref(&value)).unwrap();
            let field = rtdb_columnar::to_field("result", &logical).unwrap();
            let cell = ArrowCell { field: &field, array: array.as_ref(), row: 0 };
            for format in [Format::Text, Format::Binary] {
                let expected = encode_value(ty, format, &value, &environment).unwrap();
                let output = encode_arrow(ty, format, &logical, cell, &environment, &memory).unwrap().unwrap();
                assert_eq!(output.bytes, expected);
            }
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
            }
        }
    }
}
