use std::io::Write;

use arrow_array::{RecordBatch, cast::AsArray, types::*};
use arrow_schema::DataType;
use base64::Engine;
use rtdb_columnar::ArrowCell;
use rtdb_types::{ListKind, Result};

use super::{MAX_DEPTH, STANDARD, TransitValue as T, invalid, json_wire, stringable, value_to_transit};

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

fn bytes(output: &mut impl Write, data: &[u8]) -> Result<()> {
    output.write_all(data).map_err(invalid)
}

fn text(output: &mut impl Write, value: &str) -> Result<()> {
    if value.starts_with(['~', '^', '`']) {
        serde_json::to_writer(output, &format!("~{value}")).map_err(invalid)
    } else {
        serde_json::to_writer(output, value).map_err(invalid)
    }
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
    if cell.structure().is_some() || matches!(cell.sequence_kind(), Some(ListKind::List | ListKind::Set | ListKind::FixedSizeList(_) | ListKind::Map { .. })) {
        return Ok(false);
    }
    Ok(stringable(&leaf(cell, typed_structs)?))
}

fn write_cell(output: &mut impl Write, cell: ArrowCell<'_>, key: bool, typed_structs: bool, depth: usize, top: bool) -> Result<()> {
    if depth > MAX_DEPTH { return Err(invalid("Transit nesting limit exceeded")); }
    let cell = cell.physical()?;
    if cell.is_null()? {
        return write_leaf(output, T::Null, key, depth, top);
    }
    if let Some(value) = cell.utf8() {
        if top { bytes(output, br#"["~#'","#)?; }
        text(output, value)?;
        if top { bytes(output, b"]")?; }
        return Ok(());
    }
    if let Some(value) = cell.binary() {
        if top { bytes(output, br#"["~#'","#)?; }
        bytes(output, br#""~b"#)?;
        bytes(output, STANDARD.encode(value).as_bytes())?;
        bytes(output, if top { b"\"]" } else { b"\"" })?;
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
        _ => write_leaf(output, leaf(cell, typed_structs)?, key, depth, top),
    }
}

fn write_leaf(output: &mut impl Write, value: T, key: bool, depth: usize, top: bool) -> Result<()> {
    let quote = top && !matches!(&value, T::Array(_) | T::Map(_))
        && !matches!(&value, T::Tagged { tag, .. } if tag.chars().count() > 1);
    if quote { bytes(output, br#"["~#'","#)?; }
    serde_json::to_writer(&mut *output, &json_wire(&value, key, depth)?).map_err(invalid)?;
    if quote { bytes(output, b"]")?; }
    Ok(())
}

fn leaf(cell: ArrowCell<'_>, typed_structs: bool) -> Result<T> {
    let tagged = |tag: &str, representation| T::Tagged { tag: tag.to_owned(), representation: Box::new(representation) };
    if cell.field.metadata().is_empty() {
        let array = cell.array;
        let row = cell.row;
        macro_rules! int {
            ($ty:ty, $tag:literal) => {{
                let value = T::Integer(i64::from(array.as_primitive::<$ty>().value(row)));
                if typed_structs { tagged($tag, value) } else { value }
            }};
        }
        let primitive = match cell.field.data_type() {
            DataType::Boolean => T::Bool(array.as_boolean().value(row)),
            DataType::Int8 => int!(Int8Type, "i8"),
            DataType::Int16 => int!(Int16Type, "i16"),
            DataType::Int32 => int!(Int32Type, "i32"),
            DataType::Int64 => T::Integer(array.as_primitive::<Int64Type>().value(row)),
            DataType::UInt8 => T::Integer(i64::from(array.as_primitive::<UInt8Type>().value(row))),
            DataType::UInt16 => T::Integer(i64::from(array.as_primitive::<UInt16Type>().value(row))),
            DataType::UInt32 => T::Integer(i64::from(array.as_primitive::<UInt32Type>().value(row))),
            DataType::UInt64 => {
                let number = array.as_primitive::<UInt64Type>().value(row);
                i64::try_from(number).map_or_else(|_| tagged("n", T::String(number.to_string())), T::Integer)
            }
            DataType::Float16 => T::Float(array.as_primitive::<Float16Type>().value(row).to_f64()),
            DataType::Float32 => {
                let number = array.as_primitive::<Float32Type>().value(row);
                if typed_structs { tagged("f32", T::Float(f64::from(number))) } else { T::Float(f64::from(number)) }
            }
            DataType::Float64 => T::Float(array.as_primitive::<Float64Type>().value(row)),
            _ => return value_to_transit(&rtdb_columnar::read_value(cell.field, array, row)?),
        };
        return Ok(primitive);
    }
    value_to_transit(&rtdb_columnar::read_value(cell.field, cell.array, cell.row)?)
}
