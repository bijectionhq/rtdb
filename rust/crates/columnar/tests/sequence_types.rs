use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array, ListArray, NullArray, UInt8Array, UnionArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, UnionFields};
use rtdb_columnar::ArrowCell;
use rtdb_types::LogicalType;

fn per_element(cell: ArrowCell<'_>) -> LogicalType {
    let types = cell.sequence().unwrap().map(|child| child.observed_type().unwrap()).collect::<Vec<_>>();
    LogicalType::join_all(&types)
}

fn per_element_bytes(cell: ArrowCell<'_>) -> usize {
    cell.sequence().unwrap().fold(cell.field.size() * 16 + 256, |bytes, child| bytes + child.observed_type_bytes().unwrap())
}

fn assert_rows_match(field: &Field, array: &dyn Array) {
    for row in 0..array.len() {
        let cell = ArrowCell { field, array, row };
        let LogicalType::List { element, .. } = cell.observed_type().unwrap() else { panic!("list type") };
        assert_eq!(*element, per_element(cell), "row {row}");
        assert_eq!(cell.observed_type_bytes().unwrap(), per_element_bytes(cell), "row {row}");
    }
}

fn list(child: Field, values: ArrayRef, offsets: &[i32]) -> (Field, ListArray) {
    let child = Arc::new(child);
    let array = ListArray::new(Arc::clone(&child), OffsetBuffer::new(ScalarBuffer::from(offsets.to_vec())), values, None);
    (Field::new("embedding", DataType::List(child), true), array)
}

#[test]
fn leaf_sequences_derive_the_element_types_their_elements_observe_in_first_seen_order() {
    let values: ArrayRef = Arc::new(Float32Array::from(vec![None, Some(1.0), Some(2.0), None, Some(3.0)]));
    let (field, array) = list(Field::new("item", DataType::Float32, true), values, &[0, 2, 3, 5, 5]);
    assert_rows_match(&field, &array);

    let values: ArrayRef = Arc::new(UInt8Array::from(vec![Some(1), None, Some(255)]));
    let (field, array) = list(Field::new("item", DataType::UInt8, true), values, &[0, 3]);
    assert_rows_match(&field, &array);

    let values: ArrayRef = Arc::new(NullArray::new(2));
    let (field, array) = list(Field::new("item", DataType::Null, true), values, &[0, 2, 2]);
    assert_rows_match(&field, &array);

    let child = Arc::new(Field::new("item", DataType::Float64, true));
    let values = Arc::new(Float64Array::from(vec![Some(1.0), None, Some(f64::NAN), Some(-0.0)]));
    let array = FixedSizeListArray::new(Arc::clone(&child), 2, values, None);
    let field = Field::new("embedding", DataType::FixedSizeList(child, 2), true);
    assert_rows_match(&field, &array);
}

#[test]
fn fixed_size_sequence_keeps_child_offsets_above_i32_max() {
    let width = 1_i32 << 30;
    let values = Arc::new(arrow_array::StructArray::new_empty_fields(3 * width as usize, None));
    let child = Arc::new(Field::new("item", values.data_type().clone(), false));
    let array = FixedSizeListArray::new(child, width, values, None);
    let field = Field::new("items", array.data_type().clone(), false);
    let cell = ArrowCell { field: &field, array: &array, row: 2 };

    let (_, _, rows) = cell.sequence_range().unwrap();
    assert_eq!(rows, 2 * width as usize..3 * width as usize);
    let sliced = array.slice(2, 1);
    let cell = ArrowCell { field: &field, array: &sliced, row: 0 };
    let (_, _, rows) = cell.sequence_range().unwrap();
    assert_eq!(rows, 0..width as usize);
}

#[test]
fn float_union_sequences_derive_their_element_types_per_selected_leg() {
    let fields = UnionFields::try_new(
        vec![0, 1, 2],
        vec![
            Field::new("f64", DataType::Float64, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("null", DataType::Null, true),
        ],
    ).unwrap();
    let union = UnionArray::try_new(
        fields.clone(),
        ScalarBuffer::from(vec![1i8, 0, 2, 0, 1, 2, 0]),
        Some(ScalarBuffer::from(vec![0i32, 0, 0, 1, 1, 1, 2])),
        vec![
            Arc::new(Float64Array::from(vec![Some(1.0), None, Some(2.0)])) as ArrayRef,
            Arc::new(Float32Array::from(vec![Some(1.5), None])),
            Arc::new(NullArray::new(2)),
        ],
    )
    .unwrap();
    let child = Field::new("item", union.data_type().clone(), true);
    let (field, array) = list(child, Arc::new(union), &[0, 3, 4, 7, 7]);
    assert_rows_match(&field, &array);
}

#[test]
fn an_element_on_a_bottom_leg_still_refuses_its_type_bound() {
    let fields = UnionFields::try_new(vec![0, 1], vec![Field::new("f32", DataType::Float32, true), Field::new("bottom", DataType::Null, false)]).unwrap();
    let union = UnionArray::try_new(
        fields,
        ScalarBuffer::from(vec![0i8, 1]),
        Some(ScalarBuffer::from(vec![0i32, 0])),
        vec![Arc::new(Float32Array::from(vec![1.0])) as ArrayRef, Arc::new(NullArray::new(1))],
    )
    .unwrap();
    let child = Field::new("item", union.data_type().clone(), true);
    let (field, array) = list(child, Arc::new(union), &[0, 1, 2]);
    let cell = |row| ArrowCell { field: &field, array: &array, row };
    assert!(cell(0).observed_type_bytes().is_ok());
    assert_eq!(cell(1).observed_type_bytes().unwrap_err().code, "xtdb/zero-leg-read");
    let LogicalType::List { element, .. } = cell(1).observed_type().unwrap() else { panic!("list type") };
    assert_eq!(*element, LogicalType::Nothing);
}
