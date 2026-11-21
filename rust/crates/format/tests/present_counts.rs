use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, FixedSizeBinaryArray, Float64Array, Int64Array, StringArray, StructArray, TimestampMicrosecondArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{Field, Fields};
use rtdb_types::Value;

fn count(column: &Value) -> Value { column.field("count").cloned().unwrap() }

fn present(field: &Field, array: &dyn Array, rows: impl Iterator<Item = usize>) -> Value {
    let present = rows.filter(|row| !matches!(rtdb_columnar::read_value(field, array, *row).unwrap(), Value::Null | Value::Undefined)).count();
    Value::I64(present as i64)
}

#[test]
fn flat_column_counts_are_the_cells_that_read_as_present() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(vec![Some(1), None, Some(3), None, Some(5)])),
        Arc::new(Float64Array::from(vec![Some(f64::NAN), Some(2.0), None])),
        Arc::new(StringArray::from(vec![Some("a"), None, Some(""), Some("d")])),
        Arc::new(BooleanArray::from(vec![Some(false), None, Some(true)])),
        Arc::new(TimestampMicrosecondArray::from(vec![None, Some(7)]).with_timezone("UTC")),
        Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(vec![Some(vec![1u8; 16]), None].into_iter(), 16).unwrap()),
        Arc::new(Int64Array::from(vec![Some(1), None, Some(3), None, Some(5), Some(6)]).slice(1, 4)),
        Arc::new(StringArray::from(vec!["x", "y"])),
    ];
    for array in arrays {
        let field = Field::new("v", array.data_type().clone(), true);
        let mut columns = Vec::new();
        rtdb_format::metadata::column_metadata(&field, array.as_ref(), true, true, &mut columns).unwrap();
        assert_eq!(count(&columns[0]), present(&field, array.as_ref(), 0..array.len()), "{:?}", array.data_type());
    }
}

#[test]
fn a_nullable_structs_child_counts_only_the_present_cells_of_present_rows() {
    let child: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), None, Some("c"), Some("d"), None]));
    let child_field = Arc::new(Field::new("s", child.data_type().clone(), true));
    let parent = StructArray::new(Fields::from(vec![Arc::clone(&child_field)]), vec![Arc::clone(&child)], Some(NullBuffer::from(vec![true, true, false, true, true])));
    let field = Field::new("v", parent.data_type().clone(), true);
    let mut columns = Vec::new();
    rtdb_format::metadata::column_metadata(&field, &parent, true, true, &mut columns).unwrap();
    let child_column = columns.iter().find(|column| column.field("col-name") == Some(&Value::Utf8("s".into()))).unwrap();
    assert_eq!(count(child_column), present(&child_field, child.as_ref(), [0, 1, 3, 4].into_iter()));
}
