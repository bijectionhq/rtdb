use super::*;
use arrow_array::{ArrayRef, BinaryArray, BooleanArray, Int32Array};

fn memory() -> Arc<MemoryBudget> { MemoryBudget::new(4 * 1024 * 1024) }
fn writer(data_type: DataType, nullable: bool, memory: &Arc<MemoryBudget>) -> ScalarWriter {
    ScalarWriter::new(Arc::new(Field::new("v", data_type, nullable)), Arc::clone(memory)).unwrap()
}
fn storage(vector: &ExecutionVector) -> &NativeScalarStorage {
    let NodeLayout::Scalar(storage) = &vector.node().storage else { panic!() };
    storage
}
fn bounds<T>(result: WriteResult<T>) { assert!(matches!(result, Err(WriteError::Reference(ReferenceFailure::Bounds)))); }
fn unsafe_write<T>(result: WriteResult<T>) { assert!(matches!(result, Err(WriteError::Native(error)) if error.code == "rtdb/unsafe-arrow-write-cursor")); }

#[test]
fn null_counts_wrap_without_payload_and_recover_by_signed_max() {
    let memory = memory();
    let mut output = writer(DataType::Null, false, &memory);
    let retained = memory.used_bytes();
    output.ensure_capacity(i32::MAX).unwrap();
    output.write_undefined().unwrap();
    assert_eq!(output.value_count(), i32::MIN);
    assert!(output.checked_len().is_err());
    assert!(output.reader().is_null(i32::MIN).unwrap());
    output.set_null(i32::MAX).unwrap();
    assert_eq!(output.value_count(), i32::MIN);
    assert_eq!(memory.used_bytes(), retained);
    output.ensure_capacity(-7).unwrap();
    output.write_undefined().unwrap();
    assert_eq!(output.value_count(), -6);
    output.ensure_capacity(0).unwrap();
    assert_eq!(output.checked_len().unwrap(), 0);
    output.write_null().unwrap();
    assert_eq!(output.value_count(), 1);
    assert!(output.field().is_nullable());
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_scalar_finish_retains_negative_null_count_and_its_lease() {
    let memory = memory();
    let mut output = writer(DataType::Null, false, &memory);
    output.ensure_capacity(i32::MAX).unwrap();
    output.write_undefined().unwrap();
    let output = output.finish().unwrap();
    assert_eq!(output.value_count(), i32::MIN);
    assert!(!output.is_empty());
    assert!(output.cell(0).is_err());
    assert!(output.logical_type().is_ok());
    let copied = output.snapshot_reader(&memory).unwrap();
    let cloned = output.clone();
    drop(output);
    assert_eq!(copied.value_count(), i32::MIN);
    drop(cloned);
    assert!(memory.used_bytes() > 0);
    drop(copied);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn fixed_count_wrap_retains_tiny_payload_before_checked_reader_admission() {
    let memory = memory();
    let mut output = writer(DataType::Int32, false, &memory);
    let before = memory.used_bytes();
    output.ensure_capacity(i32::MAX).unwrap();
    assert_eq!(memory.used_bytes(), before);
    output.write_int(7).unwrap();
    assert_eq!(output.value_count(), i32::MIN);
    assert!(!output.reader().is_null(0).unwrap());
    assert!(matches!(output.reader().get_boolean(0, NullChecks::Enabled), Err(VectorReadError::Anomaly(error)) if error.code == "rtdb/unsupported-vector-reader"));
    assert!(matches!(output.reader().get_int(0, NullChecks::Enabled), Err(VectorReadError::Anomaly(error)) if error.code == "rtdb/row-index"));
    let output = output.finish().unwrap();
    assert_eq!(output.value_count(), i32::MIN);
    let NativeScalarData::Fixed(data) = &storage(&output).data else { panic!() };
    assert_eq!(data.append, 4);
    assert_eq!(&data.bytes.as_slice()[..4], &7i32.to_le_bytes());
    assert_eq!(data.bytes.len(), 128);
    assert!(output.cell(0).is_err());
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn negative_fixed_count_capacity_uses_signed_max_without_new_payload() {
    let memory = memory();
    let mut output = writer(DataType::Int32, false, &memory);
    output.ensure_capacity(i32::MAX).unwrap();
    output.write_int(1).unwrap();
    let before = memory.used_bytes();
    output.ensure_capacity(-1).unwrap();
    assert_eq!(output.value_count(), -1);
    output.ensure_capacity(i32::MIN).unwrap();
    assert_eq!(output.value_count(), -1);
    output.ensure_capacity(0).unwrap();
    output.write_int(2).unwrap();
    assert_eq!(output.value_count(), 1);
    assert_eq!(memory.used_bytes(), before);
    assert_eq!(output.reader().get_int(0, NullChecks::Enabled).unwrap(), 1);
    let output = output.finish().unwrap();
    let NativeScalarData::Fixed(data) = &storage(&output).data else { panic!() };
    assert_eq!(data.append, 8);
}

#[test]
fn float_wrap_precedes_native_append_failure_while_double_count_waits() {
    for float in [false, true] {
        let memory = memory();
        let mut output = writer(if float { DataType::Float32 } else { DataType::Float64 }, false, &memory);
        output.ensure_capacity(i32::MAX).unwrap();
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
        let result = if float { output.write_float(1.0) } else { output.write_double(1.0) };
        assert!(matches!(result, Err(WriteError::Native(error)) if error.code == "rtdb/memory-budget-exceeded"));
        assert_eq!(output.value_count(), if float { i32::MIN } else { i32::MAX });
        drop(blocker);
        if float {
            assert!(output.ready().is_err());
            assert_eq!(output.finish().unwrap_err().code, "rtdb/aborted-vector-writer");
        } else {
            output.ready().unwrap();
            output.write_double(2.0).unwrap();
            assert_eq!(output.value_count(), i32::MIN);
            drop(output.finish().unwrap());
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn boolean_count_increment_waits_for_validity_but_precedes_data_set_failure() {
    for nullable in [false, true] {
        let memory = memory();
        let mut output = writer(DataType::Boolean, nullable, &memory);
        output.fixture_state().count = SourceCount::new(-1);
        bounds(output.write_boolean(true));
        assert_eq!(output.value_count(), if nullable { -1 } else { 0 });
        output.ready().unwrap();
        output.ensure_capacity(0).unwrap();
        output.write_boolean(true).unwrap();
        assert_eq!(output.reader().get_boolean(0, NullChecks::Enabled).unwrap(), true);
    }
}

#[test]
fn negative_bit_capacity_is_consumed_even_without_logical_count_growth() {
    let memory = memory();
    let mut output = writer(DataType::Boolean, false, &memory);
    output.write_boolean(true).unwrap();
    let before = memory.used_bytes();
    output.ensure_capacity(-1).unwrap();
    assert_eq!(output.value_count(), 1);
    assert_eq!(memory.used_bytes(), before);
    assert!(matches!(output.ensure_capacity(-8), Err(WriteError::Native(error)) if error.code == "rtdb/memory-budget-exceeded"));
    assert_eq!(output.value_count(), 1);
    output.ready().unwrap();
    output.write_boolean(false).unwrap();
    assert_eq!(output.value_count(), 2);
}

#[test]
fn negative_write_ones_retains_partial_byte_rewrite_and_signed_cursor() {
    let memory = memory();
    let mut bits = MutableBits::new(&memory);
    let mut mutation = Mutation::default();
    bits.write(true, &mut mutation).unwrap();
    bits.ones(-2, &mut mutation).unwrap();
    assert_eq!(bits.append, -1);
    assert_eq!(bits.bytes()[0], 1);
    bits.ones(1, &mut mutation).unwrap();
    assert_eq!(bits.append, 0);
    assert_eq!(bits.bytes()[0], 1);
    bits.ones(-1, &mut mutation).unwrap();
    assert_eq!(bits.append, 0);
    assert_eq!(bits.byte_append(), 0);
    assert_eq!(bits.bytes()[0], 1);
    assert!(matches!(bits.ones(-8, &mut mutation), Err(WriteError::Native(_))));
    assert_eq!(bits.append, 0);
}

#[test]
fn checked_writes_and_unchecked_primitive_admission_distinguish_negative_cursors() {
    let memory = memory();
    let mut bytes = MutableBytes::new(&memory);
    let mut mutation = Mutation::default();
    bytes.append = -1;
    unsafe_write(bytes.append_primitive_reserved(&[1], &mut mutation));
    bounds(bytes.append_array(&[], &mut mutation));
    bounds(bytes.append_array(&[1], &mut mutation));
    bounds(bytes.set(-1, &[1], &mut mutation));
    bounds(bytes.fill(-1, 1, 0, &mut mutation));
    bounds(bytes.zero(1, &mut mutation));
    bytes.zero(0, &mut mutation).unwrap();
    let mut demanded = false;
    bounds(bytes.copy_from_reserved(0, || { demanded = true; Ok(&[]) }, &mut mutation));
    assert!(!demanded);
    assert_eq!(bytes.append, -1);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn reserved_primitive_writable_check_precedes_native_unsafe_span_admission() {
    let memory = memory();
    let mut bytes = MutableBytes::new(&memory);
    let mut mutation = Mutation::default();
    bytes.append = 1;
    bounds(bytes.append_primitive_reserved(&[1], &mut mutation));
    assert_eq!(bytes.append, 1);
    bytes.append_primitive(&[2], &mut mutation).unwrap();
    assert_eq!(bytes.append, 2);
    assert_eq!(&bytes.bytes()[..2], &[0, 2]);
}

#[test]
fn raw_copy_checks_destination_before_source_even_for_an_empty_span() {
    let memory = memory();
    let mut bytes = MutableBytes::new(&memory);
    let mut mutation = Mutation::default();
    let mut demanded = false;
    bytes.append = 1;
    bounds(bytes.copy_from_reserved(0, || { demanded = true; Err(WriteError::Native(layout())) }, &mut mutation));
    assert!(!demanded);
    bytes.append = 0;
    assert!(matches!(bytes.copy_from_reserved(0, || { demanded = true; Err(WriteError::Native(layout())) }, &mut mutation), Err(WriteError::Native(_))));
    assert!(demanded);
    bytes.copy_from_reserved(0, || Ok(&[]), &mut mutation).unwrap();
    assert_eq!(bytes.append, 0);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn negative_fixed_ranges_use_wrapped_spans_before_count_addition() {
    let memory = memory();
    let source = writer(DataType::Int32, false, &memory).finish().unwrap();
    let mut output = writer(DataType::Int32, false, &memory);
    let before = memory.used_bytes();
    ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_range(i32::MIN, i32::MIN).unwrap();
    assert_eq!(output.value_count(), i32::MIN);
    ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_range(0, i32::MIN).unwrap();
    assert_eq!(output.value_count(), 0);
    bounds(ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_range(-1, i32::MIN));
    bounds(ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_range(0, -1));
    assert_eq!(output.value_count(), 0);
    assert_eq!(memory.used_bytes(), before);
    output.write_int(9).unwrap();
    assert_eq!(output.reader().get_int(0, NullChecks::Enabled).unwrap(), 9);
}

#[test]
fn copy_only_fixed_and_boolean_sources_read_retained_spans_without_logical_rows() {
    let memory = memory();
    for boolean in [false, true] {
        let data_type = if boolean { DataType::Boolean } else { DataType::Int32 };
        let mut source = writer(data_type.clone(), false, &memory);
        if boolean { source.write_boolean(true).unwrap(); } else { source.write_int(7).unwrap(); }
        source.fixture_state().count = SourceCount::new(0);
        assert!(if boolean { source.reader().get_boolean(0, NullChecks::Enabled).is_err() } else { source.reader().get_int(0, NullChecks::Enabled).is_err() });
        let source = source.finish().unwrap();
        assert!(source.cell(0).is_err());
        let mut output = writer(data_type, false, &memory);
        ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_row(0).unwrap();
        assert_eq!(output.value_count(), 1);
        if boolean { assert!(output.reader().get_boolean(0, NullChecks::Enabled).unwrap()); }
        else { assert_eq!(output.reader().get_int(0, NullChecks::Enabled).unwrap(), 7); }
    }
}

#[test]
fn bulk_validity_initial_negative_one_cache_differs_from_direct_bit_getter() {
    let memory = memory();
    let mut source = writer(DataType::Int32, true, &memory);
    source.write_int(7).unwrap();
    let source = source.finish().unwrap();
    for (bulk, row, expected) in [(false, -8, None), (true, -8, Some(true)), (true, -1, Some(false))] {
        let mut output = writer(DataType::Int32, true, &memory);
        let mut copier = ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap();
        let result = if bulk { copier.copy_rows(&[row]) } else { copier.copy_row(row) };
        bounds(result);
        drop(copier);
        let validity = output.fixture_state().validity.as_ref().unwrap();
        assert_eq!(validity.append, if bulk { 1 } else { 0 });
        assert_eq!(validity.byte_append(), if bulk { 1 } else { 0 });
        if let Some(expected) = expected { assert_eq!(validity.bytes()[0] & 1 != 0, expected); }
        assert_eq!(output.value_count(), 0);
        output.ready().unwrap();
    }
}

#[test]
fn variable_copy_offsets_remain_raw_demands_when_logical_count_is_zero() {
    let memory = memory();
    let mut source = writer(DataType::Binary, false, &memory);
    source.write_bytes(b"a").unwrap();
    source.fixture_state().count = SourceCount::new(0);
    let source = source.finish().unwrap();
    assert!(source.cell(0).is_err());
    let mut output = writer(DataType::Binary, false, &memory);
    ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_range(i32::MIN, 1).unwrap();
    assert_eq!(output.value_count(), 1);
    assert_eq!(output.reader().get_bytes(0).unwrap(), b"a");
}

#[test]
fn negative_repeat_ranges_and_indirect_negative_array_creation_stay_distinct() {
    let memory = memory();
    for data_type in [DataType::Null, DataType::Boolean, DataType::Binary] {
        let source = writer(data_type.clone(), false, &memory).finish().unwrap();
        let mut output = writer(data_type, false, &memory);
        ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_range(i32::MAX, -1).unwrap();
        assert_eq!(output.value_count(), 0);
        let selected = source.select(&crate::Selection::All).unwrap();
        assert!(matches!(ScalarCopier::new(ScalarSource::new(&selected).unwrap(), &mut output).unwrap().copy_range(0, -1), Err(WriteError::Reference(ReferenceFailure::NegativeArraySize(-1)))));
        assert_eq!(output.value_count(), 0);
        output.ready().unwrap();
    }
}

#[test]
fn small_wrapped_prefix_fixture_captures_a_negative_byte_cursor_without_a_payload_read() {
    let memory = memory();
    let target = MemoryBudget::new(4 * 1024 * 1024);
    let source = writer(DataType::Int32, true, &memory).finish().unwrap();
    let mut output = writer(DataType::Int32, true, &memory);
    output.fixture_state().count = SourceCount::new(i32::MIN);
    output.fixture_state().validity.as_mut().unwrap().append = i32::MIN;
    ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_range(0, i32::MIN).unwrap();
    assert_eq!(output.value_count(), 0);
    let output = output.finish().unwrap();
    let validity = storage(&output).validity.as_ref().unwrap();
    assert_eq!(validity.append, i32::MIN);
    assert_eq!(validity.byte_append, -268_435_456);
    assert!(validity.bytes.is_empty());
    let copied = output.snapshot_reader(&target).unwrap();
    let cloned = output.clone();
    drop(output);
    drop(cloned);
    drop(source);
    assert_eq!(storage(&copied).validity.as_ref().unwrap().byte_append, -268_435_456);
    assert!(memory.used_bytes() < 128 * 1024);
    drop(copied);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(target.used_bytes(), 0);
}

fn aborted_bit_boundary(memory: &Arc<MemoryBudget>) -> ScalarWriter {
    let mut source = writer(DataType::Int32, true, memory);
    source.write_int(7).unwrap();
    let source = source.finish().unwrap();
    let mut output = writer(DataType::Int32, true, memory);
    output.fixture_state().validity.as_mut().unwrap().append = -8;
    unsafe_write(ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_range(0, 8));
    let validity = output.fixture_state().validity.as_ref().unwrap();
    assert_eq!(validity.append, -8);
    assert_eq!(validity.byte_append(), -1);
    assert_eq!(output.value_count(), 0);
    output
}

#[test]
fn unchecked_boundary_write_admission_seals_only_after_its_mutation_prefix() {
    let memory = memory();
    let mut output = aborted_bit_boundary(&memory);
    assert!(output.write_int(3).is_err());
    output.clear();
    output.write_int(9).unwrap();
    assert_eq!(output.reader().get_int(0, NullChecks::Enabled).unwrap(), 9);
    drop(output.finish().unwrap());
    assert_eq!(memory.used_bytes(), 0);
    let output = aborted_bit_boundary(&memory);
    assert_eq!(output.finish().unwrap_err().code, "rtdb/aborted-vector-writer");
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn negative_count_promotion_failure_retains_descriptor_prefix_until_clear() {
    let memory = memory();
    let mut output = writer(DataType::Int32, false, &memory);
    output.ensure_capacity(i32::MAX).unwrap();
    output.write_int(1).unwrap();
    assert!(matches!(output.write_null(), Err(WriteError::Native(error)) if error.code == "rtdb/memory-budget-exceeded"));
    assert_eq!(output.value_count(), i32::MIN);
    assert!(output.field().is_nullable());
    assert!(output.ready().is_err());
    output.clear();
    output.write_int(3).unwrap();
    assert_eq!(output.reader().get_int(0, NullChecks::Enabled).unwrap(), 3);
}

#[test]
fn sdk_copy_span_failure_keeps_native_error_and_aborts_after_output_growth() {
    let memory = memory();
    let array: ArrayRef = Arc::new(Int32Array::from(vec![7]));
    let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
    let source = ExecutionVector::with_reservation(Arc::new(Field::new("v", DataType::Int32, false)), array, reservation).unwrap();
    let mut output = writer(DataType::Int32, false, &memory);
    assert!(matches!(ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_range(-1, 1), Err(WriteError::Native(_))));
    assert_eq!(output.value_count(), 0);
    assert!(output.ready().is_err());
    output.clear();
    ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_row(0).unwrap();
    assert_eq!(output.reader().get_int(0, NullChecks::Enabled).unwrap(), 7);
}

#[test]
fn sdk_boolean_copy_preserves_value_and_validity_bit_offsets() {
    let memory = memory();
    let array: ArrayRef = Arc::new(BooleanArray::from(vec![Some(false), Some(true), None, Some(false)]).slice(1, 2));
    let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
    let source = ExecutionVector::with_reservation(Arc::new(Field::new("v", DataType::Boolean, true)), array, reservation).unwrap();
    let mut output = writer(DataType::Boolean, false, &memory);
    ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_rows(&[0, 1]).unwrap();
    assert!(output.reader().get_boolean(0, NullChecks::Enabled).unwrap());
    assert!(output.reader().is_null(1).unwrap());
    assert!(!output.reader().get_boolean(1, NullChecks::Disabled).unwrap());
    assert_eq!(output.value_count(), 2);
}

#[test]
fn sdk_fixed_copy_preserves_its_retained_payload_base_offset() {
    let memory = memory();
    let array: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]).slice(1, 1));
    let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
    let source = ExecutionVector::with_reservation(Arc::new(Field::new("v", DataType::Int32, false)), array, reservation).unwrap();
    let mut output = writer(DataType::Int32, false, &memory);
    ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_range(0, 1).unwrap();
    assert_eq!(output.reader().get_int(0, NullChecks::Enabled).unwrap(), 20);
}

#[test]
fn sdk_nullable_all_valid_source_keeps_selected_validity_cursor() {
    let memory = memory();
    for nullable in [false, true] {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![7]));
        let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
        let source = ExecutionVector::with_reservation(Arc::new(Field::new("v", DataType::Int32, nullable)), array, reservation).unwrap();
        let mut output = writer(DataType::Int32, true, &memory);
        ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_rows(&[0]).unwrap();
        let validity = output.fixture_state().validity.as_ref().unwrap();
        assert_eq!(validity.append, 1);
        assert_eq!(validity.byte_append(), if nullable { 1 } else { 0 });
        assert_eq!(validity.bytes()[0] & 1, 1);
        assert_eq!(output.reader().get_int(0, NullChecks::Enabled).unwrap(), 7);
    }
}

#[test]
fn sdk_nullable_all_valid_source_failure_precedes_validity_flush_and_payload_copy() {
    let memory = memory();
    let array: ArrayRef = Arc::new(Int32Array::from(vec![7]));
    let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
    let source = ExecutionVector::with_reservation(Arc::new(Field::new("v", DataType::Int32, true)), array, reservation).unwrap();
    let mut output = writer(DataType::Int32, true, &memory);
    assert!(matches!(ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_rows(&[0, 1]), Err(WriteError::Native(_))));
    let validity = output.fixture_state().validity.as_ref().unwrap();
    assert_eq!(validity.append, 0);
    assert_eq!(validity.byte_append(), 0);
    assert_eq!(validity.bytes()[0], 0);
    assert_eq!(output.value_count(), 0);
    let owner::MutableScalarData::Fixed { data, .. } = &output.fixture_state().data else { panic!() };
    assert_eq!(data.append, 0);
    assert!(output.ready().is_err());
}

#[test]
fn sliced_sdk_boolean_bitmap_rejects_logically_absent_rows_with_retained_bits() {
    let memory = memory();
    let array: ArrayRef = Arc::new(BooleanArray::from(vec![Some(false), Some(true), None, Some(false)]).slice(1, 2));
    let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
    let source = ExecutionVector::with_reservation(Arc::new(Field::new("v", DataType::Boolean, true)), array, reservation).unwrap();
    let NodeLayout::Leaf { array, .. } = &source.node().storage else { panic!() };
    assert!(array.nulls().is_some());
    let mut output = writer(DataType::Boolean, true, &memory);
    let before = memory.used_bytes();
    assert!(matches!(ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_row(2), Err(WriteError::Native(_))));
    assert_eq!(output.value_count(), 0);
    assert_eq!(memory.used_bytes(), before);
    output.ready().unwrap();
    ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_row(0).unwrap();
    assert!(output.reader().get_boolean(0, NullChecks::Enabled).unwrap());
}

#[test]
fn sliced_sdk_fixed_storage_rejects_a_wrapped_copy_address_outside_logical_rows() {
    let memory = memory();
    let array: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]).slice(1, 1));
    let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
    let source = ExecutionVector::with_reservation(Arc::new(Field::new("v", DataType::Int32, false)), array, reservation).unwrap();
    let NodeLayout::Leaf { payload: Some(payload), .. } = &source.node().storage else { panic!() };
    assert!(payload.capacity() > payload.len());
    let mut output = writer(DataType::Int32, false, &memory);
    assert!(matches!(ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_row(1_073_741_824), Err(WriteError::Native(_))));
    assert_eq!(output.value_count(), 0);
    let owner::MutableScalarData::Fixed { data, .. } = &output.fixture_state().data else { panic!() };
    assert_eq!(data.append, 0);
    assert!(output.ready().is_err());
    output.clear();
    ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_row(0).unwrap();
    assert_eq!(output.reader().get_int(0, NullChecks::Enabled).unwrap(), 20);
}

#[test]
fn sliced_sdk_variable_storage_rejects_wrapped_offset_rows_before_output_work() {
    let memory = memory();
    let array: ArrayRef = Arc::new(BinaryArray::from(vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]).slice(1, 1));
    let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
    let source = ExecutionVector::with_reservation(Arc::new(Field::new("v", DataType::Binary, false)), array, reservation).unwrap();
    let NodeLayout::Leaf { array, .. } = &source.node().storage else { panic!() };
    assert_eq!(array.as_any().downcast_ref::<BinaryArray>().unwrap().value_data(), b"abc");
    let mut output = writer(DataType::Binary, false, &memory);
    let before = memory.used_bytes();
    assert!(matches!(ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_range(i32::MIN, 1), Err(WriteError::Native(_))));
    assert_eq!(memory.used_bytes(), before);
    assert_eq!(output.value_count(), 0);
    output.ready().unwrap();
    ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_row(0).unwrap();
    assert_eq!(output.reader().get_bytes(0).unwrap(), b"b");
}
