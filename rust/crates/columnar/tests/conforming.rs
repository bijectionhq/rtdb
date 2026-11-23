use rtdb_types::StructFields;
use std::sync::Arc;

use arrow_array::{Array, Int64Array, NullArray, RecordBatch, StringArray, StructArray, UnionArray};
use arrow_buffer::ScalarBuffer;
use arrow_schema::{DataType, Field, Schema, TimeUnit, UnionFields, UnionMode};
use rtdb_columnar::{MemoryBudget, OwnedBatch, conform_batch, copy_array, read_value, to_field};
use rtdb_types::{DecimalValue, LogicalType, ScalarType, Value};

#[test]
fn signed_execution_buffers_retain_bits_in_unsigned_declared_fields() {
    let memory = MemoryBudget::new(1 << 22);
    for (signed, unsigned, value, physical) in [
        (ScalarType::I8, ScalarType::U8, Value::I8(-1), Value::U8(u8::MAX)),
        (ScalarType::I16, ScalarType::U16, Value::I16(i16::MIN), Value::U16(1 << 15)),
        (ScalarType::I32, ScalarType::U32, Value::I32(-1), Value::U32(u32::MAX)),
        (ScalarType::I64, ScalarType::U64, Value::I64(i64::MIN), Value::U64(1 << 63)),
    ] {
        let source_type = LogicalType::list(LogicalType::Scalar(signed).maybe());
        let target_type = LogicalType::list(LogicalType::Scalar(unsigned).maybe());
        let source = RecordBatch::try_new(Arc::new(Schema::new(vec![to_field("v", &source_type).unwrap()])), vec![copy_array(&source_type, &[Value::list(vec![value.clone(), Value::Null])]).unwrap()]).unwrap();
        let target = Schema::new(vec![to_field("v", &target_type).unwrap()]);
        let page = conform_batch(OwnedBatch::new(source, &memory).unwrap(), &target, &memory).unwrap();
        assert_eq!(page.declared_schema().as_ref(), &target);
        assert_eq!(page.batch().schema().as_ref(), &target);
        assert_eq!(read_value(target.field(0), page.batch().column(0).as_ref(), 0).unwrap(), Value::list(vec![physical, Value::Null]));
        assert_eq!(rtdb_columnar::read_execution_value(target.field(0), page.batch().column(0).as_ref(), 0).unwrap(), Value::list(vec![value, Value::Null]));
        drop(page);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn conforming_unloader_promotes_present_nulls_without_changing_declared_fields() {
    let memory = MemoryBudget::new(1 << 22);
    let source = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("v", DataType::Null, true)])), vec![Arc::new(NullArray::new(2))]).unwrap();
    let target = Schema::new(vec![Field::new("v", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), false)]);
    let page = conform_batch(OwnedBatch::new(source, &memory).unwrap(), &target, &memory).unwrap();
    assert_eq!(page.declared_schema().as_ref(), &target);
    assert!(page.batch().schema().field(0).is_nullable());
    assert_eq!(page.batch().column(0).null_count(), 2);
    assert_eq!(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), Value::Null);
    assert!(memory.used_bytes() > 0);
    drop(page);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn matching_columns_reuse_buffers_when_other_columns_need_conformance() {
    let memory = MemoryBudget::new(1 << 22);
    let text = StringArray::from(vec![Some("keep this buffer"), None]);
    let address = text.value_data().as_ptr();
    let text_field = Field::new("text", DataType::Utf8, true);
    let source = RecordBatch::try_new(
        Arc::new(Schema::new(vec![text_field.clone(), Field::new("number", DataType::Null, true)])),
        vec![Arc::new(text), Arc::new(NullArray::new(2))],
    ).unwrap();
    let target = Schema::new(vec![Field::new("number", DataType::Int64, true), text_field]);

    let page = conform_batch(OwnedBatch::new(source, &memory).unwrap(), &target, &memory).unwrap();

    assert_eq!(page.batch().schema().as_ref(), &target);
    assert_eq!(page.batch().column(0).null_count(), 2);
    let text = page.batch().column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(text.value_data().as_ptr(), address);
    assert_eq!(text.value(0), "keep this buffer");
    assert!(text.is_null(1));
    drop(page);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn declared_layout_preserves_nested_nulls_and_reorders_columns() {
    let memory = MemoryBudget::new(1 << 22);
    let input_type = LogicalType::list(LogicalType::Null);
    let target_type = LogicalType::list(LogicalType::Scalar(ScalarType::I64));
    let source = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("absent", DataType::Null, true), to_field("items", &input_type).unwrap()])),
        vec![Arc::new(NullArray::new(1)), copy_array(&input_type, &[Value::list(vec![Value::Null])]).unwrap()]).unwrap();
    let target = Schema::new(vec![to_field("items", &target_type).unwrap(), Field::new("absent", DataType::Int64, false)]);
    let page = conform_batch(OwnedBatch::new(source, &memory).unwrap(), &target, &memory).unwrap();
    assert_eq!(page.declared_schema().as_ref(), &target);
    assert_eq!(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), Value::list(vec![Value::Null]));
    assert_eq!(page.batch().column(1).null_count(), 1);
}

#[test]
fn declared_layout_rejects_required_missing_fields_and_decimal_relabeling() {
    let memory = MemoryBudget::new(1 << 22);
    let source = RecordBatch::new_empty(Arc::new(Schema::empty()));
    let target = Schema::new(vec![Field::new("v", DataType::Int64, false)]);
    assert_eq!(conform_batch(OwnedBatch::new(source, &memory).unwrap(), &target, &memory).err().unwrap().code, "xtdb/page-missing-non-nullable");
    let mut coefficient = vec![0; 32]; coefficient[31] = 1;
    let value = Value::Decimal(DecimalValue::new(coefficient, 256, i32::MAX, 256).unwrap());
    let source_type = value.logical_type();
    let source = RecordBatch::try_new(Arc::new(Schema::new(vec![to_field("v", &source_type).unwrap()])), vec![copy_array(&source_type, &[value]).unwrap()]).unwrap();
    let target = Schema::new(vec![to_field("v", &LogicalType::Scalar(ScalarType::Decimal { precision: 257, scale: i32::MAX, bit_width: 256 })).unwrap()]);
    assert!(conform_batch(OwnedBatch::new(source, &memory).unwrap(), &target, &memory).is_err());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn adapting_struct_order_preserves_declared_metadata_and_values() {
    let memory = MemoryBudget::new(1 << 22);
    let fields = vec![Field::new("z", DataType::Int64, false), Field::new("a", DataType::Utf8, false)].into();
    let target = Schema::new(vec![Field::new("v", DataType::Struct(fields), false).with_metadata(std::collections::HashMap::from([("caller".into(), "retained".into())]))]);
    let value = Value::Struct(StructFields::from([("a".into(), Value::Utf8("text".into())), ("z".into(), Value::I64(7))]));
    let logical = value.logical_type();
    let source = RecordBatch::try_new(Arc::new(Schema::new(vec![to_field("v", &logical).unwrap()])), vec![copy_array(&logical, &[value.clone()]).unwrap()]).unwrap();
    let structure = source.column(0).as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(structure.fields()[0].name(), "a");
    let address = structure.column(0).as_any().downcast_ref::<StringArray>().unwrap().value_data().as_ptr();
    let page = conform_batch(OwnedBatch::new(source, &memory).unwrap(), &target, &memory).unwrap();
    assert_eq!(page.batch().schema().as_ref(), &target);
    assert_eq!(page.declared_schema().as_ref(), &target);
    assert_eq!(read_value(target.field(0), page.batch().column(0).as_ref(), 0).unwrap(), value);
    let structure = page.batch().column(0).as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(structure.fields().iter().map(|field| field.name().as_str()).collect::<Vec<_>>(), ["z", "a"]);
    assert_eq!(structure.column(1).as_any().downcast_ref::<StringArray>().unwrap().value_data().as_ptr(), address);
    drop(page);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn adapting_union_ids_wraps_single_typed_columns() {
    let memory = MemoryBudget::new(1 << 22);
    let fields = UnionFields::try_new([5, 9], [Field::new("i64", DataType::Int64, false), Field::new("utf8", DataType::Utf8, false)]).unwrap();
    let target = Schema::new(vec![Field::new("v", DataType::Union(fields, UnionMode::Dense), false)]);
    let source = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])), vec![Arc::new(Int64Array::from(vec![7]))]).unwrap();
    let page = conform_batch(OwnedBatch::new(source, &memory).unwrap(), &target, &memory).unwrap();
    assert_eq!(page.batch().schema().as_ref(), &target);
    let union = page.batch().column(0).as_any().downcast_ref::<UnionArray>().unwrap();
    assert_eq!(union.type_ids().as_ref(), &[5]);
    assert_eq!(union.child(9).len(), 0);
    assert_eq!(read_value(target.field(0), union, 0).unwrap(), Value::I64(7));
    drop(page);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn exact_schema_pages_keep_noncanonical_struct_order_and_union_ids() {
    let memory = MemoryBudget::new(1 << 22);
    let struct_fields = vec![Field::new("z", DataType::Int64, false), Field::new("a", DataType::Utf8, false)].into();
    let structure = StructArray::try_new(struct_fields, vec![Arc::new(Int64Array::from(vec![7, 8])), Arc::new(StringArray::from(vec!["a", "b"]))], None).unwrap();
    let union_fields = UnionFields::try_new([5, 9], [Field::new("i64", DataType::Int64, false), Field::new("utf8", DataType::Utf8, false)]).unwrap();
    let union = UnionArray::try_new(union_fields, ScalarBuffer::from(vec![5, 9]), Some(ScalarBuffer::from(vec![0, 0])), vec![Arc::new(Int64Array::from(vec![11])), Arc::new(StringArray::from(vec!["union"]))]).unwrap();
    let target = Arc::new(Schema::new(vec![Field::new("s", structure.data_type().clone(), false), Field::new("u", union.data_type().clone(), false)]));
    let source = RecordBatch::try_new(Arc::clone(&target), vec![Arc::new(structure), Arc::new(union)]).unwrap();
    let page = conform_batch(OwnedBatch::new(source, &memory).unwrap(), &target, &memory).unwrap();
    assert_eq!(page.batch().schema(), target);
    assert_eq!(page.declared_schema(), &target);
    let structure = page.batch().column(0).as_any().downcast_ref::<StructArray>().unwrap();
    assert_eq!(structure.fields().iter().map(|field| field.name().as_str()).collect::<Vec<_>>(), ["z", "a"]);
    let union = page.batch().column(1).as_any().downcast_ref::<UnionArray>().unwrap();
    assert_eq!(union.type_ids().as_ref(), &[5, 9]);
    assert_eq!(read_value(target.field(1), union, 1).unwrap(), Value::Utf8("union".into()));
    drop(page);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn dense_union_reordering_remaps_ids_and_preserves_selected_values() {
    let memory = MemoryBudget::new(1 << 22);
    let logical = LogicalType::from_legs([LogicalType::Scalar(ScalarType::I64), LogicalType::Scalar(ScalarType::Utf8), LogicalType::Null]);
    let values = vec![Value::Utf8("left".into()), Value::Null, Value::I64(11), Value::Utf8("right".into())];
    let source = RecordBatch::try_new(Arc::new(Schema::new(vec![to_field("v", &logical).unwrap()])), vec![copy_array(&logical, &values).unwrap()]).unwrap();
    let fields = UnionFields::try_new([19, 5, 9], [Field::new("utf8", DataType::Utf8, false), Field::new("null", DataType::Null, true), Field::new("i64", DataType::Int64, false)]).unwrap();
    let target = Schema::new(vec![Field::new("v", DataType::Union(fields, UnionMode::Dense), false)]);
    let page = conform_batch(OwnedBatch::new(source, &memory).unwrap(), &target, &memory).unwrap();
    assert_eq!(page.batch().schema().as_ref(), &target);
    let union = page.batch().column(0).as_any().downcast_ref::<UnionArray>().unwrap();
    assert_eq!(union.type_ids().as_ref(), &[19, 5, 9, 19]);
    for (row, expected) in values.iter().enumerate() { assert_eq!(&read_value(target.field(0), union, row).unwrap(), expected); }
    drop(page);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn nullable_scalar_wrapping_selects_the_declared_null_leg() {
    let memory = MemoryBudget::new(1 << 22);
    let source = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)])), vec![Arc::new(Int64Array::from(vec![Some(7), None, Some(9)]))]).unwrap();
    let fields = UnionFields::try_new([31, 12], [Field::new("i64", DataType::Int64, false), Field::new("null", DataType::Null, true)]).unwrap();
    let target = Schema::new(vec![Field::new("v", DataType::Union(fields, UnionMode::Dense), false)]);
    let page = conform_batch(OwnedBatch::new(source, &memory).unwrap(), &target, &memory).unwrap();
    assert_eq!(page.declared_schema().as_ref(), &target);
    let union = page.batch().column(0).as_any().downcast_ref::<UnionArray>().unwrap();
    assert_eq!(union.type_ids().as_ref(), &[31, 12, 31]);
    assert_eq!((0..3).map(|row| read_value(page.batch().schema().field(0), union, row).unwrap()).collect::<Vec<_>>(), [Value::I64(7), Value::Null, Value::I64(9)]);
    drop(page);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn nested_list_layout_keeps_child_names_metadata_and_retained_buffers() {
    let memory = MemoryBudget::new(1 << 23);
    let value = Value::list(vec![Value::Struct(StructFields::from([("a".into(), Value::I64(5)), ("z".into(), Value::Utf8("nested".into()))]))]);
    let logical = value.logical_type();
    let source = RecordBatch::try_new(Arc::new(Schema::new(vec![to_field("v", &logical).unwrap()])), vec![copy_array(&logical, &[value.clone()]).unwrap()]).unwrap();
    let child = Field::new("items", DataType::Struct(vec![Field::new("z", DataType::Utf8, false), Field::new("a", DataType::Int64, false)].into()), false)
        .with_metadata(std::collections::HashMap::from([("source".into(), "declared".into())]));
    let target = Schema::new(vec![Field::new("v", DataType::List(Arc::new(child)), false)]);
    let page = conform_batch(OwnedBatch::new(source, &memory).unwrap(), &target, &memory).unwrap();
    assert_eq!(page.batch().schema().as_ref(), &target);
    let retained = Arc::clone(page.batch().column(0));
    drop(page);
    assert!(memory.used_bytes() > 0);
    assert_eq!(read_value(target.field(0), retained.as_ref(), 0).unwrap(), value);
    drop(retained);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn declared_schema_metadata_is_reserved_before_exact_or_adapted_clones() {
    for exact in [false, true] {
        let memory = MemoryBudget::new(1 << 20);
        let target = Schema::empty().with_metadata(std::collections::HashMap::from([("large".into(), "x".repeat(128 * 1024))]));
        let source = RecordBatch::new_empty(Arc::new(if exact { target.clone() } else { Schema::empty() }));
        let source = OwnedBatch::new(source, &memory).unwrap();
        let error = conform_batch(source, &target, &memory).err().unwrap();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(error.category, rtdb_types::AnomalyCategory::Incorrect);
        assert_eq!(memory.used_bytes(), 0);
    }
}

fn promoted_union_field() -> Field {
    let fields = UnionFields::try_new([0, 1, 2], [Field::new("f64", DataType::Float64, true), Field::new("utf8", DataType::Utf8, false), Field::new("null", DataType::Null, true)]).unwrap();
    Field::new("v", DataType::Union(fields, UnionMode::Dense), false)
}

fn leg_names(field: &Field, union: &UnionArray) -> Vec<String> {
    let DataType::Union(fields, _) = field.data_type() else { panic!("Expected a union field") };
    (0..union.len()).map(|row| fields.iter().find(|(id, _)| *id == union.type_id(row)).unwrap().1.name().clone()).collect()
}

#[test]
fn written_union_nulls_land_in_the_null_leg_even_after_a_nullable_leg() {
    let declared = promoted_union_field();
    let logical = rtdb_columnar::from_field(&declared).unwrap();
    assert_eq!(logical, LogicalType::from_legs([LogicalType::Scalar(ScalarType::F64), LogicalType::Scalar(ScalarType::Utf8), LogicalType::Null]));
    let values = vec![Value::F64(1.5), Value::Null, Value::Utf8("a".into()), Value::Null];
    let built = rtdb_columnar::build_array(&logical, &values).unwrap();
    let built_field = to_field("v", &logical).unwrap();
    let union = built.as_any().downcast_ref::<UnionArray>().unwrap();
    assert_eq!(leg_names(&built_field, union), ["f64", "null", "utf8", "null"]);
    let DataType::Union(fields, _) = built_field.data_type() else { unreachable!() };
    let f64_leg = fields.iter().find(|(_, field)| field.name() == "f64").unwrap().0;
    assert_eq!(union.child(f64_leg).null_count(), 0);
    assert_eq!((0..4).map(|row| read_value(&built_field, union, row).unwrap()).collect::<Vec<_>>(), values);
}

#[test]
fn nulls_already_in_a_nullable_leg_read_as_null_and_rewrite_into_the_null_leg() {
    let declared = promoted_union_field();
    let DataType::Union(fields, _) = declared.data_type() else { unreachable!() };
    let children: Vec<Arc<dyn Array>> = vec![Arc::new(arrow_array::Float64Array::from(vec![Some(1.5), None])), Arc::new(StringArray::from(vec!["a"])), Arc::new(NullArray::new(1))];
    let promoted = UnionArray::try_new(fields.clone(), ScalarBuffer::from(vec![0, 0, 1, 2]), Some(ScalarBuffer::from(vec![0, 1, 0, 0])), children).unwrap();
    let values = (0..4).map(|row| read_value(&declared, &promoted, row).unwrap()).collect::<Vec<_>>();
    assert_eq!(values, [Value::F64(1.5), Value::Null, Value::Utf8("a".into()), Value::Null]);
    let logical = rtdb_columnar::from_field(&declared).unwrap();
    let copied = copy_array(&logical, &values).unwrap();
    let union = copied.as_any().downcast_ref::<UnionArray>().unwrap();
    assert_eq!(leg_names(&to_field("v", &logical).unwrap(), union), ["f64", "null", "utf8", "null"]);
}

#[test]
fn a_nullable_vector_union_conforms_to_its_declared_plain_layout_with_typed_components() {
    use arrow_array::{ArrayRef, FixedSizeListArray, Float32Array};
    let memory = MemoryBudget::new(1 << 22);
    let vector = LogicalType::List { kind: rtdb_types::ListKind::FixedSizeList(2), element: Box::new(LogicalType::Scalar(ScalarType::F32).maybe()) };
    let vector_field = to_field(&rtdb_columnar::leg_name(&vector).unwrap(), &vector).unwrap();
    let null_field = to_field(&rtdb_columnar::leg_name(&LogicalType::Null).unwrap(), &LogicalType::Null).unwrap();
    let DataType::FixedSizeList(item, _) = vector_field.data_type() else { panic!("vector layout") };
    let vectors = FixedSizeListArray::new(Arc::clone(item), 2, Arc::new(Float32Array::from(vec![Some(-0.0), None, Some(f32::NAN), Some(2.5)])), None);
    let fields = UnionFields::try_new(vec![0, 1], vec![vector_field.clone(), null_field]).unwrap();
    let union = UnionArray::try_new(fields.clone(), ScalarBuffer::from(vec![0i8, 1, 0]), Some(ScalarBuffer::from(vec![1i32, 0, 0])),
        vec![Arc::new(vectors) as ArrayRef, Arc::new(NullArray::new(1))]).unwrap();
    let source = Schema::new(vec![Field::new("embedding", DataType::Union(fields, UnionMode::Dense), false)]);
    let target = Schema::new(vec![to_field("embedding", &vector.clone().maybe()).unwrap()]);
    let union: ArrayRef = Arc::new(union);
    assert!(rtdb_columnar::relayout_arrow_column(Arc::clone(&union), source.field(0), target.field(0)).is_err(), "the union stays a union outside result conformance");
    let page = conform_batch(OwnedBatch::new(RecordBatch::try_new(Arc::new(source), vec![union]).unwrap(), &memory).unwrap(), &target, &memory).unwrap();
    let column = page.batch().column(0).as_any().downcast_ref::<FixedSizeListArray>().unwrap();
    assert_eq!(column.nulls().map(|nulls| nulls.iter().collect::<Vec<_>>()), Some(vec![true, false, true]));
    let components = column.values().as_any().downcast_ref::<Float32Array>().unwrap();
    let rows = |row: usize| (0..2).map(|index| components.is_valid(row * 2 + index).then(|| components.value(row * 2 + index).to_bits())).collect::<Vec<_>>();
    assert_eq!(rows(0), [Some(f32::NAN.to_bits()), Some(2.5f32.to_bits())]);
    assert_eq!(rows(2), [Some((-0.0f32).to_bits()), None]);
    drop(page);
    assert_eq!(memory.used_bytes(), 0);
}
