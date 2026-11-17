use std::sync::Arc;
use arrow_array::{Array, ArrayRef, BinaryArray, Decimal128Array, DurationMicrosecondArray, FixedSizeBinaryArray, Int32Array, Int64Array, NullArray, StringArray, TimestampMicrosecondArray, UInt32Array};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields, TimeUnit, UnionFields, UnionMode};
use rtdb_columnar::{ExecutionVector, MemoryBudget, NullChecks, PhysicalLayout, RetainedBuffer, Selection, VectorReadError};
use rtdb_types::{LogicalType, ScalarType, Value};

fn anomaly(error: VectorReadError) -> rtdb_types::Anomaly {
    match error { VectorReadError::Anomaly(error) => error, other => panic!("expected an existing anomaly, got {other:?}") }
}

fn open(field: Field, array: ArrayRef, memory: &Arc<MemoryBudget>) -> ExecutionVector {
    let charge = memory.reserve(array.get_array_memory_size()).unwrap();
    ExecutionVector::with_reservation(Arc::new(field), array, charge).unwrap()
}

fn buffer(bytes: &[u8], memory: &Arc<MemoryBudget>) -> RetainedBuffer {
    let charge = memory.reserve(bytes.len() + 256).unwrap();
    RetainedBuffer::with_reservation(Buffer::from_slice_ref(bytes), charge).unwrap()
}

fn offsets(values: &[i32], memory: &Arc<MemoryBudget>) -> RetainedBuffer {
    let bytes = values.iter().flat_map(|value| value.to_le_bytes()).collect::<Vec<_>>();
    buffer(&bytes, memory)
}

fn absent_parent(child: ExecutionVector, positions: &[i32], memory: &Arc<MemoryBudget>) -> ExecutionVector {
    let fields = UnionFields::try_new([0], vec![child.ingress_field().arrow().unwrap().clone()]).unwrap();
    ExecutionVector::from_physical(Arc::new(Field::new("parent", DataType::Union(fields, UnionMode::Dense), false)), PhysicalLayout::DenseUnion {
        type_ids: buffer(&vec![255; positions.len()], memory), offsets: offsets(positions, memory), children: vec![child],
    }, memory).unwrap()
}

#[test]
fn empty_required_leg_admits_methods_before_signed_payload_bounds() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let child = open(Field::new("numbers", DataType::Int64, false), Arc::new(Int64Array::from(Vec::<i64>::new())), &memory);
    assert!(child.cell(0).is_err());
    let parent = absent_parent(child, &[0, -1, i32::MAX], &memory);
    let named = parent.vector_for("numbers").unwrap().unwrap();
    let selected = named.select(&Selection::All).unwrap();
    let composed = selected.select(&Selection::Range { offset: 0, length: 3 }).unwrap().select(&Selection::Indices(vec![2, 0, 1])).unwrap();
    for reader in [&named, &selected, &composed] {
        for row in 0..reader.checked_len().unwrap() {
            let cell = reader.cell(row).unwrap();
            assert_eq!(cell.leaf_descriptor().unwrap().name(), "numbers");
            for checks in [NullChecks::Enabled, NullChecks::Disabled] {
                assert_eq!(anomaly(cell.get_boolean(checks).unwrap_err()).code, "rtdb/unsupported-vector-reader");
                assert_eq!(anomaly(cell.get_long(checks).unwrap_err()).code, "rtdb/row-index");
            }
            assert_eq!(anomaly(cell.get_bytes().unwrap_err()).code, "rtdb/unsupported-vector-reader");
            assert_eq!(cell.get_long_long_high().unwrap_err().code, "rtdb/unsupported-vector-reader");
            assert_eq!(cell.type_id().unwrap_err().code, "rtdb/unsupported-vector-reader");
            assert_eq!(cell.leg_name().unwrap_err().code, "rtdb/unsupported-vector-reader");
            assert_eq!(cell.list_start_index().unwrap_err().code, "rtdb/unsupported-vector-reader");
            assert_eq!(cell.list_count().unwrap_err().code, "rtdb/unsupported-vector-reader");
        }
    }
    for row in 0..3 {
        assert!(named.cell(row).unwrap().is_null().unwrap());
        assert!(!selected.cell(row).unwrap().is_null().unwrap());
        assert!(!composed.cell(row).unwrap().is_null().unwrap());
        assert_eq!(named.cell(row).unwrap().byte_span().unwrap_err().code, "rtdb/unsupported-vector-reader");
        assert_eq!(selected.cell(row).unwrap().byte_span().unwrap_err().code, "rtdb/row-index");
    }
    drop(parent);
    drop(named);
    drop(selected);
    drop(composed);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn empty_null_leg_nullness_and_object_read_do_not_consume_child_storage() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let child = open(Field::new("nulls", DataType::Null, true), Arc::new(NullArray::new(0)), &memory);
    let parent = absent_parent(child, &[0, -1, i32::MAX], &memory);
    let named = parent.vector_for("nulls").unwrap().unwrap();
    let selected = named.select(&Selection::All).unwrap().select(&Selection::All).unwrap();
    for reader in [&named, &selected] {
        for row in 0..3 {
            let cell = reader.cell(row).unwrap();
            assert!(cell.is_null().unwrap());
            assert_eq!(cell.object().unwrap().value(), &Value::Null);
            assert_eq!(anomaly(cell.get_boolean(NullChecks::Enabled).unwrap_err()).code, "rtdb/unsupported-vector-reader");
            assert_eq!(anomaly(cell.get_bytes().unwrap_err()).code, "rtdb/unsupported-vector-reader");
            assert_eq!(cell.byte_span().unwrap_err().code, "rtdb/unsupported-vector-reader");
            assert_eq!(cell.get_long_long_low().unwrap_err().code, "rtdb/unsupported-vector-reader");
        }
    }
    drop(parent);
    drop(named);
    drop(selected);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn empty_nothing_leg_is_null_without_admitting_a_logical_value() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let child = open(Field::new("nothing", DataType::Null, false), Arc::new(NullArray::new(0)), &memory);
    let parent = absent_parent(child, &[0, -1, i32::MAX], &memory);
    let selected = parent.vector_for("nothing").unwrap().unwrap().select(&Selection::All).unwrap();
    for row in 0..3 {
        let cell = selected.cell(row).unwrap();
        assert!(cell.is_null().unwrap());
        assert_eq!(cell.object().unwrap_err().code, "xtdb/zero-leg-read");
        assert_eq!(anomaly(cell.get_long(NullChecks::Disabled).unwrap_err()).code, "rtdb/unsupported-vector-reader");
    }
}

#[test]
fn admitted_empty_fixed_binary_getters_check_payload_bounds_after_wrapper_admission() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let array = FixedSizeBinaryArray::try_new(16, Buffer::from(Vec::<u8>::new()), None).unwrap();
    let child = open(Field::new("bytes", DataType::FixedSizeBinary(16), false), Arc::new(array), &memory);
    let parent = absent_parent(child, &[0, -1, i32::MAX], &memory);
    let named = parent.vector_for("bytes").unwrap().unwrap();
    let selected = named.select(&Selection::All).unwrap();
    for row in 0..3 {
        assert_eq!(named.cell(row).unwrap().get_long_long_high().unwrap_err().code, "rtdb/unsupported-vector-reader");
        assert_eq!(anomaly(named.cell(row).unwrap().get_bytes().unwrap_err()).code, "rtdb/row-index");
        let cell = selected.cell(row).unwrap();
        assert_eq!(cell.get_long_long_high().unwrap_err().code, "rtdb/row-index");
        assert_eq!(cell.get_long_long_low().unwrap_err().code, "rtdb/row-index");
        assert_eq!(cell.byte_span().unwrap_err().code, "rtdb/row-index");
        assert_eq!(anomaly(cell.get_bytes().unwrap_err()).code, "rtdb/row-index");
    }
}

#[test]
fn nullable_empty_leaf_checks_validity_only_after_supported_method_admission() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let child = open(Field::new("numbers", DataType::Int64, true), Arc::new(Int64Array::from(Vec::<i64>::new())), &memory);
    let parent = absent_parent(child, &[0, -1, i32::MAX], &memory);
    let named = parent.vector_for("numbers").unwrap().unwrap();
    let selected = named.select(&Selection::All).unwrap();
    for row in 0..3 {
        assert!(named.cell(row).unwrap().is_null().unwrap());
        let cell = selected.cell(row).unwrap();
        assert_eq!(cell.is_null().unwrap_err().code, "rtdb/row-index");
        assert_eq!(anomaly(cell.get_boolean(NullChecks::Enabled).unwrap_err()).code, "rtdb/unsupported-vector-reader");
        assert_eq!(anomaly(cell.get_bytes().unwrap_err()).code, "rtdb/unsupported-vector-reader");
        assert_eq!(anomaly(cell.get_long(NullChecks::Enabled).unwrap_err()).code, "rtdb/row-index");
    }
}

#[test]
fn external_empty_run_end_null_values_remain_rejected() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let ends = open(Field::new("run_ends", DataType::Int32, false), Arc::new(Int32Array::from(Vec::<i32>::new())), &memory);
    let values = open(Field::new("values", DataType::Null, true), Arc::new(NullArray::new(0)), &memory);
    let field = Arc::new(Field::new("runs", DataType::RunEndEncoded(Arc::new(ends.ingress_field().arrow().unwrap().clone()), Arc::new(values.ingress_field().arrow().unwrap().clone())), false));
    let error = ExecutionVector::from_physical(field, PhysicalLayout::RunEndEncoded { length: 0, run_ends: ends, values }, &memory).unwrap_err();
    assert_eq!(error.code, "rtdb/run-end-type");
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn external_union_fields_reject_run_end_encoded_children() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let runs = arrow_array::RunArray::<arrow_array::types::Int32Type>::try_new(&Int32Array::from(vec![1]), &Int64Array::from(vec![73])).unwrap();
    let child = open(Field::new("runs", runs.data_type().clone(), false), Arc::new(runs), &memory);
    let fields = UnionFields::try_new([0], vec![child.ingress_field().arrow().unwrap().clone()]).unwrap();
    let error = ExecutionVector::from_physical(Arc::new(Field::new("parent", DataType::Union(fields, UnionMode::Dense), false)), PhysicalLayout::DenseUnion {
        type_ids: buffer(&[0], &memory), offsets: offsets(&[0], &memory), children: vec![child],
    }, &memory).unwrap_err();
    assert_eq!(error.code, "rtdb/encoded-union-leg");
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn fixed_list_offset_getters_keep_signed_arithmetic_without_row_access() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let elements = open(Field::new("item", DataType::Int64, false), Arc::new(Int64Array::from(Vec::<i64>::new())), &memory);
    let field = Arc::new(Field::new("items", DataType::FixedSizeList(Arc::new(elements.ingress_field().arrow().unwrap().clone()), 3), false));
    let list = ExecutionVector::from_physical(field, PhysicalLayout::FixedSizeList { length: 0, validity: None, elements }, &memory).unwrap();
    let parent = absent_parent(list, &[0, -1, i32::MAX], &memory);
    let named = parent.vector_for("items").unwrap().unwrap();
    let selected = named.select(&Selection::All).unwrap();
    for reader in [&named, &selected] {
        for (row, expected) in [0, -3, i32::MAX.wrapping_mul(3)].into_iter().enumerate() {
            let cell = reader.cell(row).unwrap();
            assert_eq!(cell.list_start_index().unwrap(), expected);
            assert_eq!(cell.list_count().unwrap(), 3);
            assert!(cell.raw_list_elements().is_err());
        }
    }
}

#[test]
fn variable_list_start_getter_reads_terminal_offset_without_consuming_end() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let elements = open(Field::new("item", DataType::Int64, false), Arc::new(Int64Array::from(Vec::<i64>::new())), &memory);
    let field = Arc::new(Field::new("items", DataType::List(Arc::new(elements.ingress_field().arrow().unwrap().clone())), false));
    let list = ExecutionVector::from_physical(field, PhysicalLayout::List { length: 0, validity: None, offsets: offsets(&[0], &memory), elements }, &memory).unwrap();
    let parent = absent_parent(list, &[0, -1, i32::MAX], &memory);
    let named = parent.vector_for("items").unwrap().unwrap();
    let selected = named.select(&Selection::All).unwrap();
    assert_eq!(selected.cell(0).unwrap().list_start_index().unwrap(), 0);
    for row in 0..3 {
        assert_eq!(selected.cell(row).unwrap().list_count().unwrap_err().code, "rtdb/row-index");
        if row > 0 { assert_eq!(selected.cell(row).unwrap().list_start_index().unwrap_err().code, "rtdb/row-index"); }
    }
}

fn union(name: &str, ids: &[u8], positions: &[i32], memory: &Arc<MemoryBudget>) -> ExecutionVector {
    let duration_field = Field::new("custom-duration", DataType::Duration(TimeUnit::Microsecond), true);
    let null_field = Field::new("explicit-null", DataType::Null, true);
    let fields = UnionFields::try_new([7, 2], vec![duration_field.clone(), null_field.clone()]).unwrap();
    let duration = open(duration_field, Arc::new(DurationMicrosecondArray::new(vec![71, 83].into(), Some(NullBuffer::from(vec![false, true])))), memory);
    let null = open(null_field, Arc::new(NullArray::new(1)), memory);
    ExecutionVector::from_physical(Arc::new(Field::new(name, DataType::Union(fields, UnionMode::Dense), false)), PhysicalLayout::DenseUnion {
        type_ids: buffer(ids, memory), offsets: offsets(positions, memory), children: vec![duration, null],
    }, memory).unwrap()
}

fn assert_absent_primitives(cell: rtdb_columnar::CellRef<'_>) {
    for checks in [NullChecks::Enabled, NullChecks::Disabled] {
        for result in [
            cell.get_boolean(checks).map(|_| ()), cell.get_byte(checks).map(|_| ()),
            cell.get_short(checks).map(|_| ()), cell.get_int(checks).map(|_| ()),
            cell.get_long(checks).map(|_| ()), cell.get_float(checks).map(|_| ()),
            cell.get_double(checks).map(|_| ()), cell.get_bytes().map(|_| ()),
        ] {
            assert!(matches!(result, Err(VectorReadError::AbsentPrimitiveReceiver)));
        }
    }
}

#[test]
fn absent_primitive_receivers_remain_raw_through_indirect_and_run_end_readers() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let values = union("values", &[255, 128], &[-1, i32::MAX], &memory);
    let ends = open(Field::new("run_ends", DataType::Int32, false), Arc::new(Int32Array::from(vec![2, 5])), &memory);
    let field = Arc::new(Field::new("r", DataType::RunEndEncoded(Arc::new(ends.ingress_field().arrow().unwrap().clone()), Arc::new(values.ingress_field().arrow().unwrap().clone())), false));
    let runs = ExecutionVector::from_physical(field, PhysicalLayout::RunEndEncoded { length: 5, run_ends: ends, values: values.clone() }, &memory).unwrap();
    for vector in [&values, &runs] {
        for row in 0..vector.checked_len().unwrap() { assert_absent_primitives(vector.cell(row).unwrap()); }
        let selected = vector.select(&Selection::Indices(vec![(vector.checked_len().unwrap() - 1) as u32, 0])).unwrap();
        let selected = selected.select(&Selection::All).unwrap();
        assert_absent_primitives(selected.cell(0).unwrap());
        assert_absent_primitives(selected.cell(1).unwrap());
        assert_eq!(selected.cell(0).unwrap().object().unwrap_err().message.as_deref(), Some("no leg for index 1"));
    }
    for checks in [NullChecks::Enabled, NullChecks::Disabled] {
        let cell = values.cell(0).unwrap();
        for error in [cell.get_as_int(checks).unwrap_err(), cell.get_as_long(checks).unwrap_err(), cell.get_as_float(checks).unwrap_err(), cell.get_as_double(checks).unwrap_err()] {
            assert_eq!(anomaly(error).code, "rtdb/unsupported-vector-reader");
        }
    }
    drop(values);
    drop(runs);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn named_reader_primitive_absence_comes_from_inner_receiver_not_outer_mask() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let inner = union("inner", &[255, 7], &[0, 1], &memory);
    let fields = UnionFields::try_new([3], vec![inner.ingress_field().arrow().unwrap().clone()]).unwrap();
    let outer = ExecutionVector::from_physical(Arc::new(Field::new("outer", DataType::Union(fields, UnionMode::Dense), false)), PhysicalLayout::DenseUnion {
        type_ids: buffer(&[3, 255], &memory), offsets: offsets(&[0, 1], &memory), children: vec![inner],
    }, &memory).unwrap();
    let leg = outer.vector_for("inner").unwrap().unwrap();
    assert_absent_primitives(leg.cell(0).unwrap());
    assert_eq!(leg.cell(0).unwrap().object().unwrap_err().message.as_deref(), Some("no leg for index 0"));
    assert!(leg.cell(1).unwrap().is_null().unwrap());
    assert_eq!(leg.cell(1).unwrap().get_long(NullChecks::Enabled).unwrap(), 83);
    let indirect_leg = outer.select(&Selection::Range { offset: 0, length: 2 }).unwrap().vector_for("inner").unwrap().unwrap();
    let range = indirect_leg.select(&Selection::Range { offset: 0, length: 2 }).unwrap();
    assert_absent_primitives(range.cell(0).unwrap());
    assert!(range.cell(1).unwrap().is_null().unwrap());
    assert_eq!(range.cell(1).unwrap().get_long(NullChecks::Enabled).unwrap(), 83);
    let selected = range.select(&Selection::Indices(vec![1, 0])).unwrap();
    assert!(!selected.cell(0).unwrap().is_null().unwrap());
    assert_absent_primitives(selected.cell(1).unwrap());
    drop(outer);
    drop(leg);
    drop(indirect_leg);
    drop(range);
    drop(selected);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn indirect_range_indices_are_reserved_before_publication_and_released_with_the_view() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let vector = open(Field::new("i", DataType::Int64, false), Arc::new(Int64Array::from(vec![11, 23, 37])), &memory);
    let original = memory.used_bytes();
    let range = vector.select(&Selection::Range { offset: 1, length: 2 }).unwrap();
    assert_eq!(memory.used_bytes(), original);
    let held = memory.reserve(memory.limit_bytes() - original).unwrap();
    assert_eq!(range.select(&Selection::Range { offset: 1, length: 1 }).unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), memory.limit_bytes());
    drop(held);
    let selected = range.select(&Selection::Range { offset: 1, length: 1 }).unwrap();
    let with_selection = memory.used_bytes();
    assert!(with_selection > original);
    assert_eq!(selected.cell(0).unwrap().get_long(NullChecks::Enabled).unwrap(), 37);
    let composed = selected.select(&Selection::All).unwrap();
    assert!(memory.used_bytes() > with_selection);
    drop(composed);
    assert_eq!(memory.used_bytes(), with_selection);
    drop(selected);
    assert_eq!(memory.used_bytes(), original);
    drop(range);
    drop(vector);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn absent_union_bytes_are_not_explicit_null_or_nullable_duration() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let vector = union("u", &[255, 128, 7, 2, 7], &[-1, i32::MAX, 0, 0, 1], &memory);
    assert_eq!(vector.leg_names().unwrap().collect::<Vec<_>>(), ["custom-duration", "explicit-null"]);
    for (row, id) in [(0, -1), (1, -128)] {
        let cell = vector.cell(row).unwrap();
        assert!(cell.is_null().unwrap());
        assert_eq!(cell.type_id().unwrap(), id);
        assert_eq!(cell.leg_name().unwrap(), None);
        assert_eq!(cell.object().unwrap_err().message, Some(format!("no leg for index {row}")));
        assert!(matches!(cell.get_long(NullChecks::Disabled), Err(VectorReadError::AbsentPrimitiveReceiver)));
        assert!(cell.getter_index().is_err());
    }
    let duration = vector.cell(2).unwrap();
    assert!(duration.is_null().unwrap());
    assert_eq!(duration.leaf_descriptor().unwrap().name(), "custom-duration");
    assert_eq!(duration.get_long(NullChecks::Disabled).unwrap(), 71);
    assert_eq!(anomaly(duration.get_long(NullChecks::Enabled).unwrap_err()).message.as_deref(), Some("null at index 0"));
    assert_eq!(duration.object().unwrap().value(), &Value::Null);
    let explicit = vector.cell(3).unwrap();
    assert_eq!(explicit.leg_name().unwrap(), Some("explicit-null"));
    assert_eq!(anomaly(explicit.get_long(NullChecks::Disabled).unwrap_err()).code, "rtdb/unsupported-vector-reader");
    assert_eq!(explicit.object().unwrap().value(), &Value::Null);
    assert_eq!(vector.cell(4).unwrap().get_long(NullChecks::Enabled).unwrap(), 83);
    drop(vector);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn named_leg_mask_does_not_guard_payload_and_selection_has_reader_specific_semantics() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let vector = union("u", &[2, 7, 255], &[0, 1, 1], &memory);
    let leg = vector.vector_for("custom-duration").unwrap().unwrap();
    assert!(leg.cell(0).unwrap().is_null().unwrap());
    assert_eq!(leg.cell(0).unwrap().get_long(NullChecks::Disabled).unwrap(), 71);
    assert!(leg.cell(2).unwrap().is_null().unwrap());
    assert_eq!(leg.cell(2).unwrap().object().unwrap().value(), &Value::Duration { value: 83, unit: rtdb_types::TimeUnit::Microsecond });
    assert!(leg.cell(1).unwrap().type_id().is_err());
    assert!(leg.cell(1).unwrap().leg_name().is_err());
    let all = leg.select(&Selection::All).unwrap();
    assert!(!all.cell(2).unwrap().is_null().unwrap());
    assert_eq!(all.cell(2).unwrap().getter_index().unwrap(), 1);
    let selected = vector.select(&Selection::Indices(vec![2, 1, 0])).unwrap();
    let indirect_leg = selected.vector_for("custom-duration").unwrap().unwrap();
    let range = indirect_leg.select(&Selection::Range { offset: 0, length: 2 }).unwrap();
    assert!(range.cell(0).unwrap().is_null().unwrap());
    assert!(!range.cell(1).unwrap().is_null().unwrap());
    let indices = range.select(&Selection::Indices(vec![0])).unwrap();
    assert!(!indices.cell(0).unwrap().is_null().unwrap());
    assert_eq!(indices.cell(0).unwrap().getter_index().unwrap(), 1);
}

#[test]
fn inactive_leg_offset_is_checked_only_when_the_reader_consumes_it() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let vector = union("u", &[255], &[i32::MAX], &memory);
    let leg = vector.vector_for("custom-duration").unwrap().unwrap();
    assert!(leg.cell(0).unwrap().is_null().unwrap());
    assert_eq!(anomaly(leg.cell(0).unwrap().get_long(NullChecks::Disabled).unwrap_err()).code, "rtdb/row-index");
    let selected = leg.select(&Selection::All).unwrap();
    assert_eq!(selected.value_count(), 1);
    assert_eq!(anomaly(selected.cell(0).unwrap().get_long(NullChecks::Disabled).unwrap_err()).code, "rtdb/row-index");
}

#[test]
fn absent_union_empty_leg_selects_before_final_child_bounds_are_consumed() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let child = open(Field::new("numbers", DataType::Int64, false), Arc::new(Int64Array::from(Vec::<i64>::new())), &memory);
    let fields = UnionFields::try_new([0], vec![child.ingress_field().arrow().unwrap().clone()]).unwrap();
    let vector = ExecutionVector::from_physical(Arc::new(Field::new("u", DataType::Union(fields, UnionMode::Dense), false)), PhysicalLayout::DenseUnion {
        type_ids: buffer(&[255, 255, 255], &memory), offsets: offsets(&[0, -1, i32::MAX], &memory), children: vec![child],
    }, &memory).unwrap();
    let leg = vector.vector_for("numbers").unwrap().unwrap();
    for selection in [Selection::All, Selection::Range { offset: 0, length: 3 }, Selection::Indices(vec![2, 1, 0])] {
        let selected = leg.select(&selection).unwrap();
        assert_eq!(selected.value_count(), 3);
        assert_eq!(selected.descriptor().name(), "numbers");
        for row in 0..3 {
            assert_eq!(anomaly(selected.cell(row).unwrap().get_long(NullChecks::Disabled).unwrap_err()).code, "rtdb/row-index");
        }
        let composed = selected.select(&Selection::Range { offset: 1, length: 2 }).unwrap().select(&Selection::Indices(vec![1, 0])).unwrap();
        assert_eq!(composed.value_count(), 2);
        assert_eq!(anomaly(composed.cell(0).unwrap().get_long(NullChecks::Disabled).unwrap_err()).code, "rtdb/row-index");
    }
    let retained = leg.select(&Selection::Indices(vec![1])).unwrap().owned_cell(0).unwrap();
    drop(vector);
    drop(leg);
    assert_eq!(anomaly(retained.as_ref().get_long(NullChecks::Disabled).unwrap_err()).code, "rtdb/row-index");
    drop(retained);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn nested_leg_selection_reads_intermediate_offsets_but_defers_final_leaf_bounds() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let leaf = open(Field::new("numbers", DataType::Int64, false), Arc::new(Int64Array::from(Vec::<i64>::new())), &memory);
    let fields = UnionFields::try_new([3], vec![leaf.ingress_field().arrow().unwrap().clone()]).unwrap();
    let inner = ExecutionVector::from_physical(Arc::new(Field::new("inner", DataType::Union(fields, UnionMode::Dense), false)), PhysicalLayout::DenseUnion {
        type_ids: buffer(&[255], &memory), offsets: offsets(&[-7], &memory), children: vec![leaf],
    }, &memory).unwrap();
    let fields = UnionFields::try_new([4], vec![inner.ingress_field().arrow().unwrap().clone()]).unwrap();
    let outer = ExecutionVector::from_physical(Arc::new(Field::new("outer", DataType::Union(fields, UnionMode::Dense), false)), PhysicalLayout::DenseUnion {
        type_ids: buffer(&[255, 255, 255], &memory), offsets: offsets(&[0, -1, i32::MAX], &memory), children: vec![inner],
    }, &memory).unwrap();
    let nested = outer.vector_for("inner").unwrap().unwrap().vector_for("numbers").unwrap().unwrap();
    assert!(nested.cell(0).unwrap().is_null().unwrap());
    assert_eq!(anomaly(nested.cell(0).unwrap().get_boolean(NullChecks::Enabled).unwrap_err()).code, "rtdb/unsupported-vector-reader");
    assert_eq!(nested.cell(0).unwrap().leg_name().unwrap_err().code, "rtdb/unsupported-vector-reader");
    for row in [1, 2] {
        assert_eq!(anomaly(nested.cell(row).unwrap().get_boolean(NullChecks::Enabled).unwrap_err()).code, "rtdb/row-index");
        assert_eq!(nested.cell(row).unwrap().leg_name().unwrap_err().code, "rtdb/row-index");
        assert_eq!(nested.cell(row).unwrap().type_id().unwrap_err().code, "rtdb/unsupported-vector-reader");
    }
    let selected = nested.select(&Selection::Indices(vec![0])).unwrap();
    assert_eq!(anomaly(selected.cell(0).unwrap().get_long(NullChecks::Disabled).unwrap_err()).code, "rtdb/row-index");
    let before = memory.used_bytes();
    for selection in [Selection::All, Selection::Range { offset: 1, length: 1 }, Selection::Indices(vec![2])] {
        assert_eq!(nested.select(&selection).unwrap_err().code, "rtdb/row-index");
        assert_eq!(memory.used_bytes(), before);
    }
    let indirect = outer.select(&Selection::Indices(vec![2, 0])).unwrap().vector_for("inner").unwrap().unwrap().vector_for("numbers").unwrap().unwrap();
    let range = indirect.select(&Selection::Range { offset: 0, length: 1 }).unwrap();
    assert!(range.cell(0).unwrap().is_null().unwrap());
    assert_eq!(anomaly(range.cell(0).unwrap().get_long(NullChecks::Disabled).unwrap_err()).code, "rtdb/row-index");
    assert_eq!(indirect.select(&Selection::Indices(vec![0])).unwrap_err().code, "rtdb/row-index");
    let selected = indirect.select(&Selection::Indices(vec![1])).unwrap();
    assert_eq!(anomaly(selected.cell(0).unwrap().get_long(NullChecks::Disabled).unwrap_err()).code, "rtdb/row-index");
}

#[test]
fn absent_union_under_nullable_struct_is_hidden_only_by_logical_object_guard() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    for nullable in [true, false] {
        let child = union("u", &[255], &[0], &memory);
        let field = Arc::new(Field::new("s", DataType::Struct(Fields::from(vec![child.ingress_field().arrow().unwrap().clone()])), nullable));
        let vector = ExecutionVector::from_physical(field, PhysicalLayout::Struct { length: 1, validity: Some(buffer(&[0], &memory)), children: vec![child] }, &memory).unwrap();
        assert_eq!(vector.cell(0).unwrap().is_null().unwrap(), nullable);
        assert!(vector.cell(0).unwrap().raw_struct_field("u").unwrap().unwrap().as_ref().object().is_err());
        if nullable { assert_eq!(vector.cell(0).unwrap().object().unwrap().value(), &Value::Null); }
        else { assert!(vector.cell(0).unwrap().object().is_err()); }
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_list_fixed_list_and_run_end_retain_absent_children() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    for fixed in [false, true] {
        let child = union("item", &[7, 255], &[1, 0], &memory);
        let ty = if fixed { DataType::FixedSizeList(Arc::new(child.ingress_field().arrow().unwrap().clone()), 2) } else { DataType::List(Arc::new(child.ingress_field().arrow().unwrap().clone())) };
        let layout = if fixed { PhysicalLayout::FixedSizeList { length: 1, validity: None, elements: child } }
            else { PhysicalLayout::List { length: 1, validity: None, offsets: offsets(&[0, 2], &memory), elements: child } };
        let vector = ExecutionVector::from_physical(Arc::new(Field::new("l", ty, false)), layout, &memory).unwrap();
        assert_eq!(vector.cell(0).unwrap().list_count().unwrap(), 2);
        let elements = vector.cell(0).unwrap().raw_list_elements().unwrap();
        assert_eq!(elements.cell(0).unwrap().get_long(NullChecks::Enabled).unwrap(), 83);
        assert_eq!(elements.cell(1).unwrap().type_id().unwrap(), -1);
        assert!(vector.cell(0).unwrap().object().is_err());
    }
    let values = union("values", &[255, 7], &[0, 1], &memory);
    let ends = open(Field::new("run_ends", DataType::Int32, false), Arc::new(Int32Array::from(vec![2, 4])), &memory);
    let field = Arc::new(Field::new("r", DataType::RunEndEncoded(Arc::new(ends.ingress_field().arrow().unwrap().clone()), Arc::new(values.ingress_field().arrow().unwrap().clone())), false));
    let runs = ExecutionVector::from_physical(field, PhysicalLayout::RunEndEncoded { length: 4, run_ends: ends, values }, &memory).unwrap();
    assert_eq!(runs.cell(0).unwrap().leg_name().unwrap(), None);
    assert_eq!(runs.cell(3).unwrap().leg_name().unwrap(), Some("custom-duration"));
    assert_eq!(runs.cell(3).unwrap().getter_index().unwrap(), 1);
    assert_eq!(runs.cell(3).unwrap().get_long(NullChecks::Enabled).unwrap(), 83);
    assert!(runs.leg_names().is_none());
    assert!(runs.vector_for("custom-duration").is_err());
    assert!(runs.cell(0).unwrap().type_id().is_err());
    assert!(runs.cell(0).unwrap().object().is_err());
    drop(runs);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn opened_metadata_canonicalizes_signed_width_and_zone_without_changing_ingress() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let integers = open(Field::new("unsigned-name", DataType::UInt32, false), Arc::new(UInt32Array::from(vec![u32::MAX])), &memory);
    assert_eq!(integers.ingress_field().arrow().unwrap().data_type(), &DataType::UInt32);
    assert_eq!(integers.descriptor().scalar_field().unwrap().data_type(), &DataType::Int32);
    assert_eq!(integers.logical_type().unwrap(), &LogicalType::Scalar(ScalarType::I32));
    assert_eq!(integers.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), -1);
    assert_eq!(integers.cell(0).unwrap().object().unwrap().value(), &Value::I32(-1));
    let array = TimestampMicrosecondArray::from(vec![123]).with_timezone("+01");
    let timestamps = open(Field::new("zone-name", array.data_type().clone(), false), Arc::new(array), &memory);
    assert_eq!(timestamps.ingress_field().arrow().unwrap().data_type(), &DataType::Timestamp(TimeUnit::Microsecond, Some("+01".into())));
    assert_eq!(timestamps.descriptor().scalar_field().unwrap().data_type(), &DataType::Timestamp(TimeUnit::Microsecond, Some("+01:00".into())));
    let parent = ExecutionVector::from_physical(Arc::new(Field::new("s", DataType::Struct(Fields::from(vec![integers.ingress_field().arrow().unwrap().clone(), timestamps.ingress_field().arrow().unwrap().clone()])), false)), PhysicalLayout::Struct { length: 1, validity: None, children: vec![integers, timestamps] }, &memory).unwrap();
    let projected = parent.arrow_field().unwrap();
    let DataType::Struct(fields) = projected.field().data_type() else { panic!() };
    assert_eq!(fields[0].data_type(), &DataType::Int32);
    assert_eq!(fields[1].data_type(), &DataType::Timestamp(TimeUnit::Microsecond, Some("+01:00".into())));
    let value = parent.cell(0).unwrap().object().unwrap();
    let Value::Struct(fields) = value.value() else { panic!() };
    assert_eq!(fields["zone-name"], Value::Timestamp { value: 123, unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("+01:00".into()) });
}

#[test]
fn retained_buffer_slices_and_cells_keep_charges_until_last_owner() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let bytes = buffer(&[1, 2, 3, 4], &memory);
    let slice = bytes.slice(1, 2).unwrap();
    drop(bytes);
    assert_eq!(slice.as_slice(), &[2, 3]);
    assert!(memory.used_bytes() > 0);
    assert!(slice.slice(usize::MAX, 1).is_err());
    drop(slice);
    assert_eq!(memory.used_bytes(), 0);
    let retained = union("u", &[7], &[1], &memory).owned_cell(0).unwrap();
    let clone = retained.clone();
    drop(retained);
    assert_eq!(clone.as_ref().get_long(NullChecks::Enabled).unwrap(), 83);
    drop(clone);
    assert_eq!(memory.used_bytes(), 0);
    let charge = memory.reserve(0).unwrap();
    assert!(RetainedBuffer::with_reservation(Buffer::from_slice_ref([1u8]), charge).is_err());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn raw_bytes_and_pointer_admission_preserve_null_payloads() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let bytes = BinaryArray::new(OffsetBuffer::new(vec![0, 3].into()), Buffer::from_slice_ref(b"abc"), Some(NullBuffer::from(vec![false])));
    let vector = open(Field::new("b", DataType::Binary, true), Arc::new(bytes), &memory);
    assert_eq!(vector.cell(0).unwrap().get_bytes().unwrap(), b"abc");
    assert_eq!(vector.cell(0).unwrap().byte_span().unwrap(), b"abc");
    assert_eq!(vector.cell(0).unwrap().object().unwrap().value(), &Value::Null);
    let scalar = open(Field::new("i", DataType::Int32, true), Arc::new(Int32Array::new(vec![17].into(), Some(NullBuffer::from(vec![false])))), &memory);
    assert_eq!(scalar.cell(0).unwrap().byte_span().unwrap(), 17i32.to_le_bytes());
    assert!(scalar.cell(0).unwrap().get_bytes().is_err());
    let vector = union("u", &[7], &[1], &memory);
    assert!(vector.cell(0).unwrap().byte_span().is_err());
    assert!(vector.vector_for("custom-duration").unwrap().unwrap().cell(0).unwrap().byte_span().is_err());
}

#[test]
fn long_long_and_numeric_class_methods_do_not_invent_wrapper_capabilities() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let raw: [u8; 16] = [0, 0, 0, 0, 0, 0, 0, 17, 255, 255, 255, 255, 255, 255, 255, 253];
    let binary = FixedSizeBinaryArray::try_new(16, Buffer::from_slice_ref(raw), Some(NullBuffer::from(vec![false]))).unwrap();
    let vector = open(Field::new("b", DataType::FixedSizeBinary(16), true), Arc::new(binary), &memory);
    assert_eq!(vector.cell(0).unwrap().get_long_long_high().unwrap(), 17);
    assert_eq!(vector.select(&Selection::Indices(vec![0])).unwrap().cell(0).unwrap().get_long_long_low().unwrap(), -3);
    let scalar = open(Field::new("i", DataType::Int64, false), Arc::new(Int64Array::from(vec![i64::MAX])), &memory);
    assert_eq!(scalar.cell(0).unwrap().get_as_int(NullChecks::Enabled).unwrap(), -1);
    assert_eq!(scalar.cell(0).unwrap().get_as_float(NullChecks::Enabled).unwrap(), i64::MAX as f32);
    assert!(scalar.select(&Selection::Indices(vec![0])).unwrap().cell(0).unwrap().get_as_long(NullChecks::Enabled).is_err());
    assert_eq!(scalar.select(&Selection::Indices(vec![0])).unwrap().cell(0).unwrap().get_long(NullChecks::Enabled).unwrap(), i64::MAX);
}

#[test]
fn decimal_class_reads_null_coefficients_and_retains_result_reservation() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let array = Decimal128Array::new(vec![123456789i128].into(), Some(NullBuffer::from(vec![false]))).with_precision_and_scale(20, 3).unwrap();
    let vector = open(Field::new("d", array.data_type().clone(), true), Arc::new(array), &memory);
    assert_eq!(vector.cell(0).unwrap().get_as_double(NullChecks::Enabled).unwrap(), 123456.789);
    assert_eq!(vector.cell(0).unwrap().get_as_float(NullChecks::Enabled).unwrap(), 123456.789f32);
    assert_eq!(vector.cell(0).unwrap().object().unwrap().value(), &Value::Null);
    let decimal = vector.cell(0).unwrap().get_decimal().unwrap();
    drop(vector);
    let Value::Decimal(value) = decimal.value() else { panic!() };
    assert_eq!(value.unscaled, 123456789i128.to_be_bytes());
    assert_eq!(value.scale, 3);
    drop(decimal);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn decimal_numeric_reads_keep_full_scale_signed_zero_and_overflow() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    for (scale, expected) in [(i32::MAX, -0.0f64), (i32::MIN, f64::NEG_INFINITY)] {
        let array = Decimal128Array::from(vec![-1i128]).with_precision_and_scale(20, 1).unwrap();
        let field = rtdb_columnar::with_decimal_parameters(Field::new("d", array.data_type().clone(), false), rtdb_columnar::DecimalParameters { precision: 20, scale, bit_width: 128 }).unwrap();
        let vector = open(field, Arc::new(array), &memory);
        assert_eq!(vector.cell(0).unwrap().get_as_double(NullChecks::Enabled).unwrap().to_bits(), expected.to_bits());
        assert_eq!(vector.cell(0).unwrap().get_as_float(NullChecks::Enabled).unwrap().to_bits(), (expected as f32).to_bits());
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn fixed_binary_long_long_stride_differs_from_declared_pointer_width() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let raw = [10i64, 20, 30].into_iter().flat_map(i64::to_be_bytes).collect::<Vec<_>>();
    let array = FixedSizeBinaryArray::try_new(8, Buffer::from(raw), None).unwrap();
    let vector = open(Field::new("b", DataType::FixedSizeBinary(8), false), Arc::new(array), &memory);
    assert_eq!(vector.cell(1).unwrap().get_long_long_high().unwrap(), 30);
    assert_eq!(vector.cell(1).unwrap().byte_span().unwrap(), 20i64.to_be_bytes());
    assert_eq!(vector.cell(2).unwrap().get_long_long_high().unwrap_err().code, "rtdb/row-index");
}

#[test]
fn pointer_spans_preserve_arrow_slices_and_run_end_forwarding() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let array = Int64Array::from(vec![13, 42, 79]).slice(1, 2);
    let vector = open(Field::new("i", DataType::Int64, false), Arc::new(array), &memory);
    assert_eq!(vector.cell(0).unwrap().byte_span().unwrap(), 42i64.to_le_bytes());
    let values = Int64Array::from(vec![103, 107]);
    let runs = arrow_array::RunArray::<arrow_array::types::Int32Type>::try_new(&Int32Array::from(vec![2, 5]), &values).unwrap().slice(1, 3);
    let vector = open(Field::new("r", runs.data_type().clone(), false), Arc::new(runs), &memory);
    assert_eq!(vector.cell(0).unwrap().byte_span().unwrap(), 103i64.to_le_bytes());
    assert_eq!(vector.cell(2).unwrap().byte_span().unwrap(), 107i64.to_le_bytes());
    assert!(vector.cell(0).unwrap().get_as_long(NullChecks::Enabled).is_err());
}

#[test]
fn named_struct_child_retains_outer_offset_and_discriminator() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let required = open(Field::new("required", DataType::Int64, false), Arc::new(Int64Array::from(vec![11, 29])), &memory);
    let optional = open(Field::new("optional", DataType::Int64, true), Arc::new(Int64Array::new(vec![41, 43].into(), Some(NullBuffer::from(vec![true, false])))), &memory);
    let structure = ExecutionVector::from_physical(Arc::new(Field::new("named-struct", DataType::Struct(Fields::from(vec![required.ingress_field().arrow().unwrap().clone(), optional.ingress_field().arrow().unwrap().clone()])), true)), PhysicalLayout::Struct {
        length: 2, validity: Some(buffer(&[0], &memory)), children: vec![required, optional],
    }, &memory).unwrap();
    let fields = UnionFields::try_new([4], vec![structure.ingress_field().arrow().unwrap().clone()]).unwrap();
    let vector = ExecutionVector::from_physical(Arc::new(Field::new("u", DataType::Union(fields, UnionMode::Dense), false)), PhysicalLayout::DenseUnion {
        type_ids: buffer(&[4, 255], &memory), offsets: offsets(&[1, 0], &memory), children: vec![structure],
    }, &memory).unwrap();
    let leg = vector.select(&Selection::Indices(vec![1, 0])).unwrap().vector_for("named-struct").unwrap().unwrap();
    assert_eq!(leg.key_names().unwrap().collect::<Vec<_>>(), ["required", "optional"]);
    let required = leg.struct_child("required").unwrap().unwrap();
    assert!(required.cell(0).unwrap().is_null().unwrap());
    assert!(!required.cell(1).unwrap().is_null().unwrap());
    assert_eq!(required.cell(0).unwrap().get_long(NullChecks::Enabled).unwrap(), 11);
    assert_eq!(required.cell(1).unwrap().get_long(NullChecks::Enabled).unwrap(), 29);
    let optional = leg.struct_child("optional").unwrap().unwrap();
    assert_eq!(optional.cell(1).unwrap().get_long(NullChecks::Disabled).unwrap(), 43);
    assert_eq!(anomaly(optional.cell(1).unwrap().get_long(NullChecks::Enabled).unwrap_err()).message.as_deref(), Some("null at index 1"));
    assert_eq!(leg.map_keys().unwrap().cell(0).unwrap().get_long(NullChecks::Enabled).unwrap(), 11);
    drop(vector);
    drop(leg);
    assert_eq!(required.cell(1).unwrap().get_long(NullChecks::Enabled).unwrap(), 29);
}

#[test]
fn native_container_validation_rejects_bad_offsets_and_selected_children() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    for positions in [vec![0, 3], vec![1, 0], vec![-1, 1], vec![0]] {
        let child = open(Field::new("item", DataType::Int32, false), Arc::new(Int32Array::from(vec![1, 2])), &memory);
        let field = Arc::new(Field::new("l", DataType::List(Arc::new(child.ingress_field().arrow().unwrap().clone())), false));
        assert!(ExecutionVector::from_physical(field, PhysicalLayout::List { length: 1, validity: None, offsets: offsets(&positions, &memory), elements: child }, &memory).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
    let child = open(Field::new("item", DataType::Int32, false), Arc::new(Int32Array::from(vec![1, 2])), &memory).select(&Selection::Range { offset: 0, length: 2 }).unwrap();
    let field = Arc::new(Field::new("l", DataType::List(Arc::new(child.ingress_field().arrow().unwrap().clone())), false));
    assert_eq!(ExecutionVector::from_physical(field, PhysicalLayout::List { length: 1, validity: None, offsets: offsets(&[0, 2], &memory), elements: child }, &memory).unwrap_err().code, "rtdb/physical-child-view");
    assert_eq!(memory.used_bytes(), 0);
    let field = Arc::new(Field::new("s", DataType::Struct(Fields::empty()), false));
    assert_eq!(ExecutionVector::from_physical(field, PhysicalLayout::Struct { length: i32::MAX as usize + 1, validity: None, children: vec![] }, &memory).unwrap_err().code, "rtdb/vector-length");
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_union_rejects_unknown_positive_ids_and_live_child_bounds() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    for (id, position) in [(8, 0), (7, 2), (7, -1)] {
        let child = open(Field::new("i", DataType::Int64, false), Arc::new(Int64Array::from(vec![1, 2])), &memory);
        let fields = UnionFields::try_new([7], vec![child.ingress_field().arrow().unwrap().clone()]).unwrap();
        let field = Arc::new(Field::new("u", DataType::Union(fields, UnionMode::Dense), false));
        assert!(ExecutionVector::from_physical(field, PhysicalLayout::DenseUnion { type_ids: buffer(&[id], &memory), offsets: offsets(&[position], &memory), children: vec![child] }, &memory).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn union_opened_parent_has_no_validity_even_when_ingress_declares_nullable() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let child = open(Field::new("i", DataType::Int64, false), Arc::new(Int64Array::from(vec![1])), &memory);
    let fields = UnionFields::try_new([0], vec![child.ingress_field().arrow().unwrap().clone()]).unwrap();
    let vector = ExecutionVector::from_physical(Arc::new(Field::new("u", DataType::Union(fields, UnionMode::Dense), true)), PhysicalLayout::DenseUnion {
        type_ids: buffer(&[0], &memory), offsets: offsets(&[0], &memory), children: vec![child],
    }, &memory).unwrap();
    assert!(vector.ingress_field().arrow().unwrap().is_nullable());
    assert!(!vector.descriptor().is_nullable());
    assert_eq!(vector.logical_type().unwrap(), &LogicalType::Scalar(ScalarType::I64));
}

#[test]
fn native_run_admission_rejects_unused_large_ends_and_releases_children() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    for length in [0, 1] {
        let ends = open(Field::new("run_ends", DataType::Int64, false), Arc::new(Int64Array::from(vec![1, i64::from(i32::MAX) + 1])), &memory);
        let values = open(Field::new("values", DataType::Int64, false), Arc::new(Int64Array::from(vec![42, 73])), &memory);
        let field = Arc::new(Field::new("runs", DataType::RunEndEncoded(Arc::new(ends.ingress_field().arrow().unwrap().clone()), Arc::new(values.ingress_field().arrow().unwrap().clone())), false));
        assert_eq!(ExecutionVector::from_physical(field, PhysicalLayout::RunEndEncoded { length, run_ends: ends, values }, &memory).unwrap_err().code, "rtdb/vector-length");
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn native_run_end_validation_and_declared_child_identity_release_owners() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    for end_values in [vec![0, 2], vec![2, 2], vec![2, 3]] {
        let ends = open(Field::new("run_ends", DataType::Int32, false), Arc::new(Int32Array::from(end_values)), &memory);
        let values = open(Field::new("values", DataType::Utf8, false), Arc::new(StringArray::from(vec!["a", "b"])), &memory);
        let field = Arc::new(Field::new("r", DataType::RunEndEncoded(Arc::new(ends.ingress_field().arrow().unwrap().clone()), Arc::new(values.ingress_field().arrow().unwrap().clone())), false));
        assert!(ExecutionVector::from_physical(field, PhysicalLayout::RunEndEncoded { length: 4, run_ends: ends, values }, &memory).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
    let child = open(Field::new("actual", DataType::Int32, false), Arc::new(Int32Array::from(vec![1])), &memory);
    let field = Arc::new(Field::new("s", DataType::Struct(Fields::from(vec![Field::new("declared", DataType::Int32, false)])), false));
    assert!(ExecutionVector::from_physical(field, PhysicalLayout::Struct { length: 1, validity: None, children: vec![child] }, &memory).is_err());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_admission_and_leg_selection_failures_release_only_new_allocations() {
    let memory = MemoryBudget::new(1024 * 1024);
    let child = open(Field::new("item", DataType::Int32, false), Arc::new(Int32Array::from(vec![1, 2])), &memory);
    let positions = offsets(&[0, 2], &memory);
    let field = Arc::new(Field::new("l", DataType::List(Arc::new(child.ingress_field().arrow().unwrap().clone())), false));
    let before = memory.used_bytes();
    let held = memory.reserve(memory.limit_bytes() - before).unwrap();
    assert_eq!(ExecutionVector::from_physical(field, PhysicalLayout::List { length: 1, validity: None, offsets: positions.clone(), elements: child.clone() }, &memory).unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), memory.limit_bytes());
    drop(held);
    assert_eq!(memory.used_bytes(), before);
    drop(child);
    drop(positions);
    let vector = union("u", &[7], &[1], &memory);
    let leg = vector.vector_for("custom-duration").unwrap().unwrap();
    let before = memory.used_bytes();
    let held = memory.reserve(memory.limit_bytes() - before).unwrap();
    assert_eq!(leg.select(&Selection::All).unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(vector.vector_for("custom-duration").unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(vector.cell(0).unwrap().object().unwrap_err().code, "rtdb/memory-budget-exceeded");
    drop(held);
    assert_eq!(memory.used_bytes(), before);
    drop(vector);
    drop(leg);
    assert_eq!(memory.used_bytes(), 0);
}
