use arrow_array::{Array, cast::AsArray, downcast_dictionary_array, types::{Int16Type, Int32Type, Int64Type}};
use arrow_schema::{DataType, Field};
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType};

use crate::{from_field, leg_name};

pub enum TextCell<'a> {
    Missing,
    Utf8(&'a str),
    Other,
}

/// Borrows UTF-8 text from a logical Arrow cell, including a union leg or a run end.
pub fn text_cell<'a>(logical: &LogicalType, field: &Field, array: &'a dyn Array, row: usize) -> Result<TextCell<'a>> {
    if row >= array.len() || array.data_type() != field.data_type() {
        return Err(invalid("Text cell is outside its Arrow layout"));
    }
    let logical = match logical {
        LogicalType::Maybe(inner) => inner.as_ref(),
        ty => ty,
    };
    match field.data_type() {
        DataType::Union(legs, _) => {
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
            LogicalType::Scalar(ScalarType::Utf8) => Ok(physical_text(array, row)?.map_or(TextCell::Missing, TextCell::Utf8)),
            _ => Ok(TextCell::Other),
        },
    }
}

pub fn physical_text(array: &dyn Array, row: usize) -> Result<Option<&str>> {
    if array.is_null(row) { return Ok(None); }
    Ok(Some(match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().value(row),
        DataType::LargeUtf8 => array.as_string::<i64>().value(row),
        DataType::Utf8View => array.as_string_view().value(row),
        DataType::Dictionary(_, _) => {
            return downcast_dictionary_array!(
                array => physical_text(array.values().as_ref(), array.key(row).expect("valid dictionary key")),
                _ => Err(invalid("Expected a dictionary"))
            );
        }
        _ => return Err(invalid("Text has an unsupported Arrow array")),
    }))
}

fn invalid(message: &str) -> Anomaly {
    Anomaly::fault("rtdb.columnar/text-cell", message)
}

/// Finds the concrete non-null cell selected by Arrow union and run-end columns.
pub fn selected_cell<'a>(field: &'a Field, array: &'a dyn Array, row: usize) -> Result<Option<(&'a Field, &'a dyn Array, usize)>> {
    if row >= array.len() || array.data_type() != field.data_type() { return Err(invalid("Cell is outside its Arrow layout")); }
    match field.data_type() {
        DataType::Union(fields, _) => {
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
        DataType::Dictionary(_, _) if physical_text(array, row)?.is_none() => Ok(None),
        _ if array.is_null(row) || field.data_type() == &DataType::Null => Ok(None),
        _ => Ok(Some((field, array, row))),
    }
}

/// Child positions of a list cell. The child vector may be shared by overlapping list views.
pub fn list_span<'a>(field: &'a Field, array: &'a dyn Array, row: usize) -> Option<(&'a Field, &'a dyn Array, std::ops::Range<usize>)> {
    macro_rules! offsets {
        ($child:expr, $width:ty) => {{
            let list = array.as_list::<$width>();
            let offsets = list.value_offsets();
            ($child.as_ref(), list.values().as_ref(), offsets[row] as usize..offsets[row + 1] as usize)
        }};
    }
    macro_rules! view {
        ($child:expr, $width:ty) => {{
            let list = array.as_list_view::<$width>();
            let start = list.value_offset(row) as usize;
            ($child.as_ref(), list.values().as_ref(), start..start + list.value_size(row) as usize)
        }};
    }
    Some(match field.data_type() {
        DataType::List(child) => offsets!(child, i32),
        DataType::LargeList(child) => offsets!(child, i64),
        DataType::ListView(child) => view!(child, i32),
        DataType::LargeListView(child) => view!(child, i64),
        DataType::FixedSizeList(child, width) => {
            let list = array.as_fixed_size_list();
            let start = list.value_offset(row) as usize;
            (child.as_ref(), list.values().as_ref(), start..start + *width as usize)
        }
        _ => return None,
    })
}

/// Borrows the elements of an ordinary Arrow string-list cell.
pub fn utf8_list<'a>(field: &'a Field, array: &'a dyn Array, row: usize) -> Result<Option<impl Iterator<Item = Result<&'a str>>>> {
    let Some((field, array, row)) = selected_cell(field, array, row)? else { return Ok(None); };
    if field.metadata().contains_key(crate::EXTENSION_NAME_KEY) { return Err(invalid("Expected an ordinary list of strings")); }
    let (child, values, cells) = list_span(field, array, row).ok_or_else(|| invalid("Expected an Arrow list of strings"))?;
    let logical = from_field(child)?;
    Ok(Some(cells.map(move |index| match text_cell(&logical, child, values, index)? {
        TextCell::Utf8(text) => Ok(text),
        _ => Err(invalid("List contains a missing or non-text element")),
    })))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, DictionaryArray, Int32Array, Int64Array, LargeListViewArray, LargeStringArray, RecordBatch, StringViewArray, UnionArray, types::Int32Type};
    use arrow_buffer::NullBuffer;
    use arrow_schema::{Schema, UnionFields, UnionMode};
    use rtdb_types::Value;

    use super::*;

    #[test]
    fn alternate_text_layouts_resolve_sliced_dictionary_and_sparse_union_nulls() {
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(1), Some(0), None, Some(2)]),
            Arc::new(LargeStringArray::from(vec![Some("invoice"), Some("ignored"), None])),
        ).unwrap();
        let strings: ArrayRef = Arc::new(dictionary.slice(1, 3));
        let children = UnionFields::try_new([2, 7], [
            Field::new("utf8", strings.data_type().clone(), true),
            Field::new("i64", DataType::Int64, false),
        ]).unwrap();
        let union: ArrayRef = Arc::new(UnionArray::try_new(children, vec![2_i8, 7, 2].into(), None, vec![
            strings, Arc::new(Int64Array::from(vec![0, 9, 0])),
        ]).unwrap());
        let field = Field::new("body", union.data_type().clone(), false);
        assert!(matches!(field.data_type(), DataType::Union(_, UnionMode::Sparse)));
        let logical = from_field(&field).unwrap();
        assert!(matches!(text_cell(&logical, &field, union.as_ref(), 0).unwrap(), TextCell::Utf8("invoice")));
        assert!(matches!(text_cell(&logical, &field, union.as_ref(), 1).unwrap(), TextCell::Other));
        assert!(matches!(text_cell(&logical, &field, union.as_ref(), 2).unwrap(), TextCell::Missing));
        let canonical = crate::to_field("body", &logical).unwrap();
        let (field, array) = crate::relayout_arrow_column(union, &field, &canonical).unwrap();
        assert_eq!(field, canonical);
        assert_eq!(crate::read_value(&field, array.as_ref(), 0).unwrap(), Value::Utf8("invoice".into()));
        assert_eq!(crate::read_value(&field, array.as_ref(), 1).unwrap(), Value::I64(9));
        assert_eq!(crate::read_value(&field, array.as_ref(), 2).unwrap(), Value::Null);
    }

    #[test]
    fn result_conformance_compacts_overlapping_list_views_with_view_text() {
        let child = Arc::new(Field::new("element", DataType::Utf8View, false));
        let strings = Arc::new(StringViewArray::from(vec!["invoice", "paid", "overdue"]));
        let lists: ArrayRef = Arc::new(LargeListViewArray::new(child, vec![1, 0, 1].into(), vec![2, 2, 1].into(),
            strings, Some(NullBuffer::from(vec![true, false, true]))));
        let field = Field::new("terms", lists.data_type().clone(), true);
        let actual = utf8_list(&field, lists.as_ref(), 0).unwrap().unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(actual, ["paid", "overdue"]);
        assert!(utf8_list(&field, lists.as_ref(), 1).unwrap().is_none());
        let declared = Schema::new(vec![crate::to_field("terms", &from_field(&field).unwrap()).unwrap()]);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema, vec![lists]).unwrap();
        let memory = crate::MemoryBudget::new(1 << 20);
        let conformed = crate::conform_batch(crate::OwnedBatch::new(batch, &memory).unwrap(), &declared, &memory).unwrap();
        let list = conformed.batch().column(0).as_list::<i32>();
        assert_eq!(list.value_offsets(), [0, 2, 2, 3]);
        assert_eq!(list.values().as_string::<i32>().iter().collect::<Vec<_>>(), [Some("paid"), Some("overdue"), Some("paid")]);
    }
}
