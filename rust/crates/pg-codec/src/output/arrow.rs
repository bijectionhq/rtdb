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
    let decode_bound = rtdb_columnar::estimate_read_cell_bytes(cell.field, cell.array, cell.row)?;
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
            rtdb_format::transit::encode_json(&transit_cell(cell, true, 0)?)?
        } else {
            let mut output = String::new();
            json_cell(cell, ty == PgType::JsonLd, &mut output, 0)?;
            output.into_bytes()
        };
        return settle(bytes, reservation);
    }
    let _scratch = memory.reserve(decode_bound)?;
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

fn transit_cell(cell: ArrowCell<'_>, omit_null_fields: bool, depth: usize) -> Result<TransitValue> {
    if depth > rtdb_types::transit_value::MAX_DEPTH {
        return Err(size_error());
    }
    let cell = cell.physical()?;
    if cell.is_null()? {
        return Ok(TransitValue::Null);
    }
    if let Some(text) = cell.utf8() {
        return Ok(TransitValue::String(text.into()));
    }
    if let Some(bytes) = cell.binary() {
        return Ok(TransitValue::Bytes(bytes.to_vec()));
    }
    if let Some(children) = cell.structure() {
        let mut fields = Vec::with_capacity(children.len());
        for child in children {
            if omit_null_fields && child.is_null()? {
                continue;
            }
            fields.push((
                TransitValue::String(child.field.name().clone()),
                transit_cell(child, omit_null_fields, depth + 1)?,
            ));
        }
        return Ok(TransitValue::Map(fields));
    }
    Ok(match cell.sequence_kind() {
        Some(ListKind::Map { .. }) => {
            let entries = cell
                .sequence()
                .ok_or_else(size_error)?
                .map(|entry| {
                    let (key, value) = map_entry(entry)?;
                    Ok((
                        transit_cell(key, false, depth + 1)?,
                        transit_cell(value, omit_null_fields, depth + 1)?,
                    ))
                })
                .collect::<Result<_>>()?;
            TransitValue::Map(entries)
        }
        Some(kind @ (ListKind::List | ListKind::FixedSizeList(_) | ListKind::Set)) => {
            let values = cell
                .sequence()
                .ok_or_else(size_error)?
                .map(|child| transit_cell(child, omit_null_fields, depth + 1))
                .collect::<Result<_>>()?;
            let array = TransitValue::Array(values);
            if kind == ListKind::Set {
                TransitValue::Tagged {
                    tag: "set".into(),
                    representation: Box::new(array),
                }
            } else {
                array
            }
        }
        _ => rtdb_format::transit::value_to_transit(&read_leaf(cell)?)
            .map_err(translate_output_error)?,
    })
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
