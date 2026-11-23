use arrow_array::{Array, StringArray, cast::AsArray, types::{Int16Type, Int32Type, Int64Type}};
use arrow_schema::{DataType, Field, UnionMode};
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType};

use crate::{from_field, leg_name};

pub enum TextCell<'a> {
    Missing,
    Utf8(&'a str),
    Other,
}

/// Borrows an ordinary string from a logical Arrow cell, including a union leg or a run end.
pub fn text_cell<'a>(logical: &LogicalType, field: &Field, array: &'a dyn Array, row: usize) -> Result<TextCell<'a>> {
    if row >= array.len() || array.data_type() != field.data_type() {
        return Err(invalid("Text cell is outside its Arrow layout"));
    }
    let logical = match logical {
        LogicalType::Maybe(inner) => inner.as_ref(),
        ty => ty,
    };
    match field.data_type() {
        DataType::Union(legs, UnionMode::Dense) => {
            let union = array.as_union();
            let tag = union.type_id(row);
            let child = legs.iter().find_map(|(id, child)| (id == tag).then_some(child)).ok_or_else(|| invalid("Text union type has no leg"))?;
            let derived;
            let logical = match logical {
                LogicalType::Union(types) => types.iter().find(|ty| leg_name(ty).is_ok_and(|name| name == *child.name())),
                _ => None,
            };
            let logical = match logical {
                Some(logical) => logical,
                None => {
                    derived = from_field(child)?;
                    &derived
                }
            };
            text_cell(logical, child, union.child(tag).as_ref(), union.value_offset(row))
        }
        DataType::RunEndEncoded(ends, child) => {
            let derived;
            let logical = match logical {
                LogicalType::RunEndEncoded { value, .. } => value.as_ref(),
                _ => {
                    derived = from_field(child)?;
                    &derived
                }
            };
            macro_rules! in_run {
                ($ty:ty) => {{
                    let runs = array.as_run::<$ty>();
                    text_cell(logical, child, runs.values().as_ref(), runs.get_physical_index(row))
                }};
            }
            match ends.data_type() {
                DataType::Int16 => in_run!(Int16Type),
                DataType::Int32 => in_run!(Int32Type),
                DataType::Int64 => in_run!(Int64Type),
                _ => Err(invalid("Text run ends have an unsupported layout")),
            }
        }
        _ if array.is_null(row) => Ok(TextCell::Missing),
        _ => match logical {
            LogicalType::Null | LogicalType::Nothing => Ok(TextCell::Missing),
            LogicalType::Scalar(ScalarType::Utf8) => array.as_any().downcast_ref::<StringArray>()
                .map(|strings| TextCell::Utf8(strings.value(row))).ok_or_else(|| invalid("Text has an unsupported Arrow array")),
            _ => Ok(TextCell::Other),
        },
    }
}

fn invalid(message: &str) -> Anomaly {
    Anomaly::fault("rtdb.columnar/text-cell", message)
}

/// Finds the concrete non-null cell selected by Arrow union and run-end columns.
pub fn selected_cell<'a>(field: &'a Field, array: &'a dyn Array, row: usize) -> Result<Option<(&'a Field, &'a dyn Array, usize)>> {
    if row >= array.len() || array.data_type() != field.data_type() { return Err(invalid("Cell is outside its Arrow layout")); }
    match field.data_type() {
        DataType::Union(fields, UnionMode::Dense) => {
            let union = array.as_union();
            let id = union.type_id(row);
            let (_, child) = fields.iter().find(|(tag, _)| *tag == id).ok_or_else(|| invalid("Union has no selected leg"))?;
            selected_cell(child, union.child(id).as_ref(), union.value_offset(row))
        }
        DataType::RunEndEncoded(ends, child) => {
            macro_rules! run {
                ($ty:ty) => {{
                    let runs = array.as_run::<$ty>();
                    selected_cell(child, runs.values().as_ref(), runs.get_physical_index(row))
                }};
            }
            match ends.data_type() {
                DataType::Int16 => run!(Int16Type),
                DataType::Int32 => run!(Int32Type),
                DataType::Int64 => run!(Int64Type),
                _ => Err(invalid("Cell has an unsupported run end width")),
            }
        }
        _ if array.is_null(row) => Ok(None),
        _ => Ok(Some((field, array, row))),
    }
}

/// Borrows an ordinary Binary value from the active Arrow cell.
pub fn binary_cell<'a>(field: &'a Field, array: &'a dyn Array, row: usize) -> Result<Option<&'a [u8]>> {
    let Some((field, array, row)) = selected_cell(field, array, row)? else { return Ok(None); };
    if field.data_type() != &DataType::Binary || field.metadata().contains_key(crate::EXTENSION_NAME_KEY) {
        return Err(Anomaly::incorrect("rtdb.columnar/binary-cell", "Expected an ordinary Binary cell"));
    }
    Ok(Some(array.as_binary::<i32>().value(row)))
}

/// Borrows the elements of an ordinary Arrow string-list cell.
pub fn utf8_list<'a>(field: &'a Field, array: &'a dyn Array, row: usize) -> Result<Option<impl Iterator<Item = Result<&'a str>>>> {
    let Some((field, array, row)) = selected_cell(field, array, row)? else { return Ok(None); };
    let DataType::List(child) = field.data_type() else { return Err(invalid("Expected an Arrow list of strings")); };
    if field.metadata().contains_key(crate::EXTENSION_NAME_KEY) { return Err(invalid("Expected an ordinary list of strings")); }
    let list = array.as_list::<i32>();
    let logical = from_field(child)?;
    let offsets = list.value_offsets();
    let cells = offsets[row] as usize..offsets[row + 1] as usize;
    Ok(Some(cells.map(move |index| match text_cell(&logical, child, list.values().as_ref(), index)? {
        TextCell::Utf8(text) => Ok(text),
        _ => Err(invalid("List contains a missing or non-text element")),
    })))
}
