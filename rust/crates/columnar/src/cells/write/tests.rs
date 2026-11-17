use super::*;
use arrow_array::{ArrayRef, BooleanArray, Int32Array, Int64Array, StringArray};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::{TimeUnit, UnionFields, UnionMode};
use rtdb_types::{ScalarType, TimeUnit as Unit};

fn memory() -> Arc<MemoryBudget> { MemoryBudget::new(16 * 1024 * 1024) }
fn writer(data_type: DataType, nullable: bool, memory: &Arc<MemoryBudget>) -> ScalarWriter {
    ScalarWriter::new(Arc::new(Field::new("v", data_type, nullable)), Arc::clone(memory)).unwrap()
}
fn typed(scalar: ScalarType, memory: &Arc<MemoryBudget>) -> ScalarWriter {
    ScalarWriter::new(Arc::new(crate::to_field("v", &LogicalType::Scalar(scalar)).unwrap()), Arc::clone(memory)).unwrap()
}
fn open(data_type: DataType, nullable: bool, array: ArrayRef, memory: &Arc<MemoryBudget>) -> ExecutionVector {
    let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
    ExecutionVector::with_reservation(Arc::new(Field::new("v", data_type, nullable)), array, reservation).unwrap()
}
fn frozen(writer: ScalarWriter) -> ExecutionVector { writer.finish().unwrap() }
fn copy(source: &ExecutionVector, destination: &mut ScalarWriter, rows: &[i32]) {
    ScalarCopier::new(ScalarSource::new(source).unwrap(), destination).unwrap().copy_rows(rows).unwrap();
}
fn native_data(vector: &ExecutionVector) -> &NativeScalarData {
    let NodeLayout::Scalar(storage) = &vector.node().storage else { panic!("expected native scalar"); };
    &storage.data
}
fn retained(bytes: &[u8], memory: &Arc<MemoryBudget>) -> RetainedBuffer {
    let reservation = memory.reserve(bytes.len() + 256).unwrap();
    RetainedBuffer::with_reservation(Buffer::from(bytes.to_vec()), reservation).unwrap()
}
fn expect_unsupported<T>(result: WriteResult<T>, method: &'static str) {
    assert!(matches!(result, Err(WriteError::Reference(ReferenceFailure::Unsupported(name))) if name == method));
}

#[test]
fn setters_leave_the_append_cursor_and_initialized_rows_separate() {
    let memory = memory();
    let mut output = writer(DataType::Int64, false, &memory);
    output.ensure_capacity(1).unwrap();
    assert_eq!(output.reader().get_long(0, NullChecks::Enabled).unwrap(), 0);
    output.set_long(0, 42).unwrap();
    output.write_long(9).unwrap();
    assert_eq!(output.len(), 2);
    let vector = frozen(output);
    assert_eq!(vector.cell(0).unwrap().get_long(NullChecks::Enabled).unwrap(), 9);
    assert_eq!(vector.cell(1).unwrap().get_long(NullChecks::Enabled).unwrap(), 0);
    let NativeScalarData::Fixed(bytes) = native_data(&vector) else { panic!(); };
    assert_eq!(bytes.append, 8);
    assert_eq!(bytes.bytes.len(), 128);
    drop(vector);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn reference_setter_bounds_leave_count_and_payload_recoverable() {
    let memory = memory();
    let mut output = writer(DataType::Int32, false, &memory);
    assert!(matches!(output.set_long(20, 99), Err(WriteError::Reference(ReferenceFailure::Bounds))));
    assert_eq!(output.len(), 21);
    assert_eq!(output.reader().get_int(20, NullChecks::Enabled).unwrap(), 0);
    output.write_int(7).unwrap();
    assert_eq!(output.len(), 22);
    let vector = frozen(output);
    assert_eq!(vector.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 7);
    assert_eq!(vector.cell(20).unwrap().get_int(NullChecks::Enabled).unwrap(), 0);
}

#[test]
fn fixed_short_and_long_long_writes_retain_actual_initialized_extents() {
    let memory = memory();
    let mut short = writer(DataType::FixedSizeBinary(8), false, &memory);
    short.write_bytes(&[1]).unwrap();
    let short = frozen(short);
    assert_eq!(short.cell(0).unwrap().get_bytes().unwrap(), &[1, 0, 0, 0, 0, 0, 0, 0]);
    let mut wide = writer(DataType::FixedSizeBinary(256), false, &memory);
    wide.write_bytes(&[1]).unwrap();
    let wide = frozen(wide);
    assert_eq!(wide.value_count(), 1);
    assert!(wide.cell(0).unwrap().get_bytes().is_err());
    let mut pair = writer(DataType::FixedSizeBinary(4), false, &memory);
    pair.write_long_long(i64::MIN + 1, i64::MAX - 1).unwrap();
    let pair = frozen(pair);
    assert_eq!(pair.cell(0).unwrap().get_long_long_high().unwrap(), i64::MIN + 1);
    assert_eq!(pair.cell(0).unwrap().get_long_long_low().unwrap(), i64::MAX - 1);
    assert_eq!(pair.cell(0).unwrap().get_bytes().unwrap(), &(i64::MIN + 1).to_be_bytes()[..4]);
}

#[test]
fn native_extent_does_not_admit_an_inactive_leg_past_its_logical_count() {
    let memory = memory();
    let mut output = writer(DataType::Int64, false, &memory);
    output.ensure_capacity(1).unwrap();
    let child = frozen(output);
    let field = Arc::new(Field::new("u", DataType::Union(UnionFields::try_new([0], [child.ingress_field().arrow().unwrap().clone()]).unwrap(), UnionMode::Dense), false));
    let parent = ExecutionVector::from_physical(field, PhysicalLayout::DenseUnion {
        type_ids: retained(&[255, 255, 255], &memory),
        offsets: retained(&[1i32, -1, i32::MAX].into_iter().flat_map(i32::to_le_bytes).collect::<Vec<_>>(), &memory),
        children: vec![child],
    }, &memory).unwrap();
    let leg = parent.vector_for("v").unwrap().unwrap();
    for index in 0..3 {
        assert!(leg.cell(index).unwrap().get_long(NullChecks::Disabled).is_err());
        assert!(leg.cell(index).unwrap().get_boolean(NullChecks::Enabled).is_err());
    }
}

#[test]
fn scalar_null_undefined_and_set_null_have_distinct_storage_effects() {
    let memory = memory();
    let mut output = writer(DataType::Int64, false, &memory);
    output.write_long(41).unwrap();
    output.set_null(0).unwrap();
    assert!(!output.reader().is_null(0).unwrap());
    assert_eq!(output.reader().get_long(0, NullChecks::Enabled).unwrap(), 41);
    output.write_null().unwrap();
    output.set_null(0).unwrap();
    assert!(output.reader().is_null(0).unwrap());
    assert_eq!(output.reader().get_long(0, NullChecks::Disabled).unwrap(), 41);
    assert_eq!(output.reader().get_long(1, NullChecks::Disabled).unwrap(), 0);
    let mut bottom = writer(DataType::Null, false, &memory);
    bottom.write_undefined().unwrap();
    bottom.set_null(3).unwrap();
    assert!(!bottom.field().is_nullable());
    assert!(bottom.reader().is_null(-1).unwrap());
    assert_eq!(bottom.len(), 4);
    bottom.write_null().unwrap();
    assert!(bottom.field().is_nullable());
    bottom.clear();
    assert!(bottom.field().is_nullable());
    assert_eq!(bottom.len(), 0);
}

#[test]
fn fixed_and_variable_copy_keep_null_payload_boolean_copy_clears_it() {
    let memory = memory();
    let mut integer = writer(DataType::Int64, true, &memory);
    integer.write_long(73).unwrap();
    integer.set_null(0).unwrap();
    let integer = frozen(integer);
    let mut target = writer(DataType::Int64, false, &memory);
    copy(&integer, &mut target, &[0]);
    assert!(target.reader().is_null(0).unwrap());
    assert_eq!(target.reader().get_long(0, NullChecks::Disabled).unwrap(), 73);
    let offsets = OffsetBuffer::new(vec![0i32, 3].into());
    let strings = StringArray::new(offsets, Buffer::from(b"abc".to_vec()), Some(NullBuffer::from(vec![false])));
    let strings = open(DataType::Utf8, true, Arc::new(strings), &memory);
    let mut target = writer(DataType::Utf8, false, &memory);
    copy(&strings, &mut target, &[0]);
    assert!(target.reader().is_null(0).unwrap());
    assert_eq!(target.reader().get_bytes(0).unwrap(), b"abc");
    let bits = BooleanArray::new(BooleanBuffer::from(vec![true]), Some(NullBuffer::from(vec![false])));
    let bits = open(DataType::Boolean, true, Arc::new(bits), &memory);
    let mut target = writer(DataType::Boolean, false, &memory);
    copy(&bits, &mut target, &[0]);
    assert!(target.reader().is_null(0).unwrap());
    assert!(!target.reader().get_boolean(0, NullChecks::Disabled).unwrap());
}

#[test]
fn empty_copier_factories_promote_at_their_concrete_source_step() {
    let memory = memory();
    let mut source = writer(DataType::Int64, true, &memory);
    source.write_null().unwrap();
    let source = frozen(source);
    let mut fixed = writer(DataType::Int32, false, &memory);
    assert!(matches!(ScalarCopier::storage_factory(ScalarSource::new(&source).unwrap(), &mut fixed), Err(WriteError::Reference(ReferenceFailure::CheckFailed))));
    assert!(fixed.field().is_nullable());
    fixed.write_int(8).unwrap();
    let mut strings = writer(DataType::Utf8, false, &memory);
    assert!(ScalarCopier::storage_factory(ScalarSource::new(&source).unwrap(), &mut strings).is_err());
    assert!(!strings.field().is_nullable());
    let source = open(DataType::Boolean, true, Arc::new(BooleanArray::from(vec![None])), &memory);
    let mut bits = writer(DataType::Boolean, false, &memory);
    ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut bits).unwrap().copy_rows(&[]).unwrap();
    assert!(!bits.field().is_nullable());
    copy(&source, &mut bits, &[0]);
    assert!(bits.field().is_nullable());
}

#[test]
fn outer_copy_type_admission_precedes_fixed_factory_promotion() {
    let memory = memory();
    let source = open(DataType::Int64, true, Arc::new(Int64Array::from(vec![None])), &memory);
    let mut destination = writer(DataType::Int32, false, &memory);
    assert!(matches!(ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut destination), Err(WriteError::Reference(ReferenceFailure::InvalidCopySource))));
    assert!(!destination.field().is_nullable());
    destination.write_int(12).unwrap();
    assert_eq!(frozen(destination).cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 12);
}

#[test]
fn raw_copy_keeps_nan_signed_zero_decimal_and_malformed_utf8_bits() {
    let memory = memory();
    let mut source = writer(DataType::Float64, true, &memory);
    source.write_double(f64::from_bits(0x7ff8_0000_0000_0042)).unwrap();
    source.write_double(-0.0).unwrap();
    source.set_null(0).unwrap();
    let source = frozen(source);
    let mut target = writer(DataType::Float64, false, &memory);
    copy(&source, &mut target, &[1, 0]);
    let target = frozen(target);
    assert_eq!(target.cell(0).unwrap().get_double(NullChecks::Disabled).unwrap().to_bits(), (-0.0f64).to_bits());
    assert_eq!(target.cell(1).unwrap().get_double(NullChecks::Disabled).unwrap().to_bits(), 0x7ff8_0000_0000_0042);
    let mut decimal = writer(DataType::Decimal128(32, 2), true, &memory);
    decimal.write_bytes(&(-999i128).to_le_bytes()).unwrap();
    decimal.set_null(0).unwrap();
    let decimal = frozen(decimal);
    let mut target = writer(DataType::Decimal128(32, 2), false, &memory);
    copy(&decimal, &mut target, &[0]);
    let target = frozen(target);
    assert_eq!(target.cell(0).unwrap().get_decimal().unwrap().value(), &Value::Decimal(rtdb_types::DecimalValue { unscaled: (-999i128).to_be_bytes().to_vec(), precision: 32, scale: 2, bit_width: 128 }));
    let mut text = writer(DataType::Utf8, false, &memory);
    text.write_bytes(&[0xf0, 0x80, 0x80, 0x80]).unwrap();
    let text = frozen(text);
    let mut target = writer(DataType::Utf8, false, &memory);
    copy(&text, &mut target, &[0]);
    let target = frozen(target);
    assert_eq!(target.cell(0).unwrap().get_bytes().unwrap(), &[0xf0, 0x80, 0x80, 0x80]);
    assert_eq!(target.cell(0).unwrap().object().unwrap().value(), &Value::Utf8(rtdb_types::decode_java_utf8(&[0xf0, 0x80, 0x80, 0x80])));
}

#[test]
fn date_milli_append_wraps_days_while_setters_and_copy_retain_raw_milliseconds() {
    let memory = memory();
    let mut output = writer(DataType::Date64, false, &memory);
    output.write_long(i64::MAX).unwrap();
    output.write_long(2).unwrap();
    output.set_long(1, -86_400_001).unwrap();
    let source = frozen(output);
    assert_eq!(source.cell(0).unwrap().byte_span().unwrap(), &i64::MAX.wrapping_mul(86_400_000).to_le_bytes());
    assert_eq!(source.cell(1).unwrap().get_long(NullChecks::Enabled).unwrap(), -1);
    let mut target = writer(DataType::Date64, false, &memory);
    copy(&source, &mut target, &[1]);
    let target = frozen(target);
    assert_eq!(target.cell(0).unwrap().byte_span().unwrap(), &(-86_400_001i64).to_le_bytes());
}

#[test]
fn scalar_write_method_admission_preserves_temporal_and_extension_classes() {
    let memory = memory();
    let mut time = writer(DataType::Time32(TimeUnit::Millisecond), false, &memory);
    time.write_long(i64::MAX).unwrap();
    assert_eq!(time.reader().get_int(0, NullChecks::Enabled).unwrap(), -1);
    assert!(time.reader().get_long(0, NullChecks::Disabled).is_err());
    let mut date = writer(DataType::Date32, false, &memory);
    expect_unsupported(date.write_long(1), "writeLong");
    date.write_int(2).unwrap();
    let mut oid = typed(ScalarType::Oid, &memory);
    oid.write_int(9).unwrap();
    expect_unsupported(oid.set_int(0, 5), "setInt");
    expect_unsupported(oid.ensure_capacity(2), "ensureCapacity");
    expect_unsupported(oid.set_null(0), "setNull");
    oid.write_null().unwrap();
    assert_eq!(oid.reader().get_int(0, NullChecks::Enabled).unwrap(), 9);
    assert!(oid.reader().is_null(1).unwrap());
    let mut uuid = typed(ScalarType::Uuid, &memory);
    expect_unsupported(uuid.write_long_long(1, 2), "writeLongLong");
    uuid.write_bytes(&[7; 16]).unwrap();
    assert_eq!(frozen(uuid).cell(0).unwrap().object().unwrap().value(), &Value::Uuid([7; 16]));
    let mut duration = typed(ScalarType::Duration(Unit::Nanosecond), &memory);
    duration.write_long(99).unwrap();
    assert_eq!(duration.reader().get_long(0, NullChecks::Enabled).unwrap(), 99);
}

#[test]
fn every_scalar_primitive_and_inherited_fixed_setter_keeps_its_stride() {
    let memory = memory();
    let mut byte = writer(DataType::Int8, false, &memory);
    byte.write_byte(-7).unwrap();
    assert_eq!(byte.reader().get_byte(0, NullChecks::Enabled).unwrap(), -7);
    expect_unsupported(byte.write_int(5), "writeInt");
    let mut short = writer(DataType::Int16, false, &memory);
    short.write_short(-55).unwrap();
    assert_eq!(short.reader().get_short(0, NullChecks::Enabled).unwrap(), -55);
    assert_eq!(short.reader().get_long(0, NullChecks::Enabled).unwrap(), -55);
    let mut float = writer(DataType::Float32, false, &memory);
    float.write_float(1.5).unwrap();
    float.set_float(0, -2.5).unwrap();
    assert_eq!(float.reader().get_float(0, NullChecks::Enabled).unwrap(), -2.5);
    assert_eq!(float.reader().get_double(0, NullChecks::Enabled).unwrap(), -2.5);
    let mut double = writer(DataType::Float64, false, &memory);
    double.write_double(1.0).unwrap();
    double.set_double(0, 2.0).unwrap();
    assert_eq!(double.reader().get_double(0, NullChecks::Enabled).unwrap(), 2.0);
    let mut bits = writer(DataType::Boolean, false, &memory);
    bits.set_boolean(2, true).unwrap();
    bits.write_boolean(false).unwrap();
    assert_eq!(bits.len(), 4);
    assert!(bits.reader().get_boolean(2, NullChecks::Enabled).unwrap());
    assert!(!bits.reader().get_boolean(3, NullChecks::Enabled).unwrap());
}

#[test]
fn copy_selection_uses_source_indices_then_admits_destination_indices() {
    let memory = memory();
    let source = open(DataType::Int32, false, Arc::new(Int32Array::from(vec![10, 20, 30, 40])), &memory);
    let selected = source.select(&Selection::Range { offset: 1, length: 3 }).unwrap().select(&Selection::Indices(vec![2, 0, 1])).unwrap();
    let mut output = writer(DataType::Int32, false, &memory);
    let mut copier = ScalarCopier::new(ScalarSource::new(&selected).unwrap(), &mut output).unwrap();
    copier.copy_row(0).unwrap();
    copier.copy_range(1, 2).unwrap();
    let vector = frozen(output);
    for (row, expected) in [40, 20, 30].into_iter().enumerate() {
        assert_eq!(vector.cell(row).unwrap().get_int(NullChecks::Enabled).unwrap(), expected);
        assert_eq!(vector.cell(row).unwrap().getter_index().unwrap(), row);
    }
}

#[test]
fn variable_range_and_rows_preserve_offsets_and_empty_range_behavior() {
    let memory = memory();
    let mut source = writer(DataType::Binary, false, &memory);
    for bytes in [b"a".as_slice(), b"bc".as_slice(), b"def".as_slice()] { source.write_bytes(bytes).unwrap(); }
    let source = frozen(source);
    let mut output = writer(DataType::Binary, false, &memory);
    {
        let mut copier = ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap();
        copier.copy_range(1, 2).unwrap();
        copier.copy_rows(&[2, 0]).unwrap();
        copier.copy_range(-1, -1).unwrap();
    }
    let vector = frozen(output);
    for (row, expected) in [b"bc".as_slice(), b"def".as_slice(), b"def".as_slice(), b"a".as_slice()].into_iter().enumerate() {
        assert_eq!(vector.cell(row).unwrap().get_bytes().unwrap(), expected);
    }
    let NativeScalarData::Variable { offsets, data, last_offset } = native_data(&vector) else { panic!(); };
    assert_eq!(*last_offset, 9);
    assert_eq!(offsets.append, 20);
    assert_eq!(data.append, 9);
}

#[test]
fn null_source_copier_writes_null_without_reading_a_source_payload() {
    let memory = memory();
    let mut source = writer(DataType::Null, false, &memory);
    source.write_undefined().unwrap();
    let source = frozen(source);
    let mut output = writer(DataType::Int64, false, &memory);
    ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_rows(&[-1, i32::MAX]).unwrap();
    assert_eq!(output.len(), 2);
    assert!(output.reader().is_null(0).unwrap());
    assert_eq!(output.reader().get_long(0, NullChecks::Disabled).unwrap(), 0);
}

#[test]
fn reference_copy_bounds_keep_completed_validity_and_payload_prefixes() {
    let memory = memory();
    let mut source = writer(DataType::FixedSizeBinary(256), true, &memory);
    source.write_bytes(&[1]).unwrap();
    let source = frozen(source);
    let mut output = writer(DataType::FixedSizeBinary(256), false, &memory);
    assert!(matches!(ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_row(0), Err(WriteError::Reference(ReferenceFailure::Bounds))));
    assert_eq!(output.len(), 0);
    assert!(output.field().is_nullable());
    output.write_bytes(&[8; 256]).unwrap();
    assert_eq!(frozen(output).cell(0).unwrap().get_bytes().unwrap(), &[8; 256]);
}

#[test]
fn failed_indirect_translation_precedes_destination_mutation() {
    let memory = memory();
    let source = open(DataType::Int32, false, Arc::new(Int32Array::from(vec![1, 2])), &memory);
    let selected = source.select(&Selection::Indices(vec![1, 0])).unwrap();
    let mut output = writer(DataType::Int32, false, &memory);
    assert!(matches!(ScalarCopier::new(ScalarSource::new(&selected).unwrap(), &mut output).unwrap().copy_rows(&[0, 2]), Err(WriteError::Native(_))));
    output.write_int(7).unwrap();
    assert_eq!(output.len(), 1);
}

#[test]
fn native_partial_copy_failure_requires_clear_but_retained_reads_are_safe() {
    let memory = memory();
    let source = open(DataType::Int32, false, Arc::new(Int32Array::from(vec![1, 2])), &memory);
    let mut output = writer(DataType::Int32, false, &memory);
    output.write_int(9).unwrap();
    assert!(matches!(ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_rows(&[0, 5]), Err(WriteError::Native(_))));
    assert_eq!(output.reader().get_int(0, NullChecks::Enabled).unwrap(), 9);
    assert!(matches!(output.write_int(3), Err(WriteError::Native(_))));
    output.clear();
    output.write_int(3).unwrap();
    assert_eq!(frozen(output).cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 3);
}

#[test]
fn growth_failure_before_mutation_preserves_the_old_allocation() {
    let memory = memory();
    let mut output = writer(DataType::Int64, false, &memory);
    for value in 0..16 { output.write_long(value).unwrap(); }
    let before = memory.used_bytes();
    let pressure = memory.reserve(memory.limit_bytes() - before).unwrap();
    assert!(matches!(output.write_long(16), Err(WriteError::Native(_))));
    assert_eq!(output.len(), 16);
    assert_eq!(output.reader().get_long(15, NullChecks::Enabled).unwrap(), 15);
    drop(pressure);
    assert_eq!(memory.used_bytes(), before);
    output.write_long(16).unwrap();
    drop(frozen(output));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn float_count_before_payload_failure_aborts_publication_until_clear() {
    let memory = memory();
    let mut output = writer(DataType::Float32, false, &memory);
    let pressure = memory.reserve(memory.limit_bytes() - memory.used_bytes()).unwrap();
    assert!(matches!(output.write_float(1.0), Err(WriteError::Native(_))));
    assert_eq!(output.len(), 1);
    assert!(output.reader().get_float(0, NullChecks::Disabled).is_err());
    drop(pressure);
    assert!(output.write_float(2.0).is_err());
    output.clear();
    output.write_float(2.0).unwrap();
    assert_eq!(frozen(output).cell(0).unwrap().get_float(NullChecks::Enabled).unwrap(), 2.0);
}

#[test]
fn failed_native_writer_drops_all_charges_when_finish_rejects_it() {
    let memory = memory();
    let mut output = writer(DataType::Binary, false, &memory);
    let pressure = memory.reserve(memory.limit_bytes() - memory.used_bytes() - 256).unwrap();
    assert!(matches!(output.write_bytes(b"payload"), Err(WriteError::Native(_))));
    assert_eq!(output.len(), 1);
    drop(pressure);
    assert_eq!(output.finish().unwrap_err().code, "rtdb/aborted-vector-writer");
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn clear_zeros_capacity_and_keeps_nullability_and_owner_lifetime() {
    let memory = memory();
    let mut output = writer(DataType::Int64, true, &memory);
    output.write_long(99).unwrap();
    let retained = memory.used_bytes();
    output.clear();
    assert_eq!(memory.used_bytes(), retained);
    assert!(output.field().is_nullable());
    assert!(output.reader().get_long(0, NullChecks::Disabled).is_err());
    output.ensure_capacity(1).unwrap();
    assert!(output.reader().is_null(0).unwrap());
    assert_eq!(output.reader().get_long(0, NullChecks::Disabled).unwrap(), 0);
    let vector = frozen(output);
    let cell = vector.owned_cell(0).unwrap();
    drop(vector);
    assert!(memory.used_bytes() > 0);
    assert_eq!(cell.as_ref().get_long(NullChecks::Disabled).unwrap(), 0);
    drop(cell);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_run_ends_validate_every_retained_end_with_the_shared_getter() {
    let memory = memory();
    let mut ends = ScalarWriter::new(Arc::new(Field::new("run_ends", DataType::Int32, false)), Arc::clone(&memory)).unwrap();
    ends.write_int(2).unwrap();
    ends.write_int(5).unwrap();
    let ends = frozen(ends);
    let mut values = ScalarWriter::new(Arc::new(Field::new("values", DataType::Int64, false)), Arc::clone(&memory)).unwrap();
    values.write_long(3).unwrap();
    values.write_long(7).unwrap();
    let values = frozen(values);
    let field = Arc::new(Field::new("runs", DataType::RunEndEncoded(Arc::new(ends.ingress_field().arrow().unwrap().clone()), Arc::new(values.ingress_field().arrow().unwrap().clone())), false));
    let vector = ExecutionVector::from_physical(field, PhysicalLayout::RunEndEncoded { length: 3, run_ends: ends, values }, &memory).unwrap();
    assert_eq!(vector.cell(2).unwrap().get_long(NullChecks::Enabled).unwrap(), 7);
}

#[test]
fn signed_set_null_preserves_constant_nonnullable_and_null_receiver_paths() {
    let memory = memory();
    for data_type in [DataType::Int64, DataType::Null] {
        let mut output = writer(data_type, false, &memory);
        output.set_null(-1).unwrap();
        output.set_null(i32::MAX).unwrap();
        assert_eq!(output.len(), 0);
        assert!(!output.field().is_nullable());
        output.write_null().unwrap();
        assert_eq!(output.len(), 1);
    }
    let mut output = writer(DataType::Int64, false, &memory);
    assert!(matches!(output.set_long(-1, 4), Err(WriteError::Reference(ReferenceFailure::Bounds))));
    assert!(matches!(output.set_long(i32::MAX, 4), Err(WriteError::Reference(ReferenceFailure::Bounds))));
    assert_eq!(output.len(), 0);
    output.write_long(8).unwrap();
    assert_eq!(output.reader().get_long(0, NullChecks::Enabled).unwrap(), 8);
}

#[test]
fn default_scalar_range_loops_ignore_nonpositive_lengths_but_indirect_translation_does_not() {
    let memory = memory();
    for data_type in [DataType::Null, DataType::Boolean] {
        let source = frozen(writer(data_type.clone(), true, &memory));
        let mut output = writer(data_type.clone(), false, &memory);
        ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_range(i32::MAX, -1).unwrap();
        assert_eq!(output.len(), 0);
        assert!(!output.field().is_nullable());
        let indirect = source.select(&Selection::All).unwrap();
        assert!(ScalarCopier::new(ScalarSource::new(&indirect).unwrap(), &mut output).unwrap().copy_range(0, -1).is_err());
    }
}

#[test]
fn extension_copier_wrapper_admission_does_not_become_inner_class_admission() {
    let memory = memory();
    let plain = open(DataType::Int32, true, Arc::new(Int32Array::from(vec![None])), &memory);
    let mut extension = typed(ScalarType::Oid, &memory);
    assert!(matches!(ScalarCopier::storage_factory(ScalarSource::new(&plain).unwrap(), &mut extension), Err(WriteError::Reference(ReferenceFailure::RequireFailed))));
    assert!(!extension.field().is_nullable());
    extension.write_null().unwrap();
    let extension = frozen(extension);
    let mut plain_target = writer(DataType::Int32, false, &memory);
    assert!(matches!(ScalarCopier::storage_factory(ScalarSource::new(&extension).unwrap(), &mut plain_target), Err(WriteError::Reference(ReferenceFailure::CheckFailed))));
    assert!(plain_target.field().is_nullable());
    let mut extension_target = typed(ScalarType::Oid, &memory);
    copy(&extension, &mut extension_target, &[0]);
    assert!(extension_target.reader().is_null(0).unwrap());
}

#[test]
fn write_ones_clears_new_tail_high_bits_and_preserves_an_existing_partial_byte() {
    let memory = memory();
    let mut bits = MutableBits::new(&memory);
    let mut mutation = Mutation::default();
    bits.ensure_capacity(8, &mut mutation).unwrap();
    bits.set(7, true, &mut mutation).unwrap();
    bits.ones(1, &mut mutation).unwrap();
    assert_eq!(bits.bytes()[0], 1);
    bits.set(7, true, &mut mutation).unwrap();
    bits.ones(1, &mut mutation).unwrap();
    assert_eq!(bits.bytes()[0], 0x83);
    assert_eq!(bits.append, 2);
}

#[test]
fn partial_fixed_copy_reference_failure_retains_data_cursor_without_completing_rows() {
    let memory = memory();
    let mut source = writer(DataType::FixedSizeBinary(256), true, &memory);
    source.write_bytes(&[1; 256]).unwrap();
    source.write_bytes(&[2]).unwrap();
    source.write_bytes(&[]).unwrap();
    let source = frozen(source);
    let mut output = writer(DataType::FixedSizeBinary(256), false, &memory);
    assert!(matches!(ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_rows(&[0, 2]), Err(WriteError::Reference(ReferenceFailure::Bounds))));
    assert_eq!(output.len(), 0);
    output.write_bytes(&[9; 256]).unwrap();
    let output = frozen(output);
    assert_eq!(output.value_count(), 1);
    assert_eq!(output.cell(0).unwrap().get_bytes().unwrap(), &[1; 256]);
    let NativeScalarData::Fixed(bytes) = native_data(&output) else { panic!(); };
    assert_eq!(bytes.append, 512);
}

#[test]
fn fixed_bulk_validity_retains_bit_and_byte_append_positions() {
    let memory = memory();
    let source = open(DataType::Int32, true, Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])), &memory);
    let mut output = writer(DataType::Int32, false, &memory);
    copy(&source, &mut output, &[0, 1, 2]);
    let output = frozen(output);
    let NodeLayout::Scalar(storage) = &output.node().storage else { panic!(); };
    let validity = storage.validity.as_ref().unwrap();
    assert_eq!(validity.bytes.as_slice()[0], 5);
    assert_eq!(validity.append, 3);
    assert_eq!(validity.byte_append, 1);
}

#[test]
fn wrapped_capacity_and_getters_preserve_wide_setter_addresses() {
    let memory = MemoryBudget::new(1024 * 1024);
    let row = 1_073_741_824;
    let mut integer = writer(DataType::Int32, false, &memory);
    assert!(matches!(integer.set_int(row, 9), Err(WriteError::Reference(ReferenceFailure::Bounds))));
    assert_eq!(integer.len(), row as usize + 1);
    assert_eq!(integer.reader().get_int(row, NullChecks::Enabled).unwrap(), 0);
    integer.write_int(7).unwrap();
    let integer = frozen(integer);
    assert_eq!(integer.cell(row as usize).unwrap().get_int(NullChecks::Enabled).unwrap(), 7);
    assert_eq!(integer.cell(row as usize).unwrap().byte_span().unwrap(), &7i32.to_le_bytes());
    let NativeScalarData::Fixed(bytes) = native_data(&integer) else { panic!(); };
    assert_eq!(bytes.bytes.len(), 128);
    let mut copied = writer(DataType::Int32, false, &memory);
    ScalarCopier::new(ScalarSource::new(&integer).unwrap(), &mut copied).unwrap().copy_row(row).unwrap();
    assert_eq!(copied.reader().get_int(0, NullChecks::Enabled).unwrap(), 7);
    let mut long = writer(DataType::Int64, false, &memory);
    let row = 536_870_912;
    assert!(matches!(long.set_long(row, 9), Err(WriteError::Reference(ReferenceFailure::Bounds))));
    long.write_long(-8).unwrap();
    assert_eq!(long.reader().get_long(row, NullChecks::Enabled).unwrap(), -8);
    let mut float = writer(DataType::Float32, false, &memory);
    let row = 1_073_741_824;
    assert!(matches!(float.set_float(row, 9.0), Err(WriteError::Reference(ReferenceFailure::Bounds))));
    float.write_float(-3.5).unwrap();
    assert_eq!(float.reader().get_float(row, NullChecks::Enabled).unwrap(), -3.5);
    let mut double = writer(DataType::Float64, false, &memory);
    let row = 536_870_912;
    assert!(matches!(double.set_double(row, 9.0), Err(WriteError::Reference(ReferenceFailure::Bounds))));
    double.write_double(-6.5).unwrap();
    assert_eq!(double.reader().get_double(row, NullChecks::Enabled).unwrap(), -6.5);
    assert!(memory.used_bytes() < 512 * 1024);
}

#[test]
fn wrapped_fixed_range_copies_zero_bytes_and_retains_its_logical_count() {
    let memory = MemoryBudget::new(1024 * 1024);
    let length = 1_073_741_824;
    let mut source = writer(DataType::Int32, false, &memory);
    source.ensure_capacity(length as usize + 1).unwrap();
    let source = frozen(source);
    let mut output = writer(DataType::Int32, false, &memory);
    ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_range(0, length).unwrap();
    assert_eq!(output.len(), length as usize);
    assert!(output.reader().get_int(0, NullChecks::Disabled).is_err());
    output.write_int(5).unwrap();
    assert_eq!(output.reader().get_int(length, NullChecks::Disabled).unwrap(), 5);
    assert!(memory.used_bytes() < 256 * 1024);
}

#[test]
fn long_long_wraps_but_decimal_utility_keeps_its_wide_byte_index() {
    let memory = MemoryBudget::new(1024 * 1024);
    let row = 268_435_456;
    let mut pair = writer(DataType::FixedSizeBinary(16), false, &memory);
    pair.ensure_capacity(row + 1).unwrap();
    pair.write_long_long(13, 29).unwrap();
    let pair = frozen(pair);
    assert_eq!(pair.cell(row).unwrap().get_long_long_high().unwrap(), 13);
    assert_eq!(pair.cell(row).unwrap().get_long_long_low().unwrap(), 29);
    let mut decimal = writer(DataType::Decimal128(32, 0), false, &memory);
    decimal.ensure_capacity(row + 1).unwrap();
    decimal.write_bytes(&37i128.to_le_bytes()).unwrap();
    let decimal = frozen(decimal);
    assert_eq!(decimal.cell(row).unwrap().byte_span().unwrap(), &37i128.to_le_bytes());
    assert!(decimal.cell(row).unwrap().get_decimal().is_err());
}

#[test]
fn offset_buffer_getter_uses_the_same_wrapped_int_address_as_extensible_buffer() {
    let bytes = [11i32.to_le_bytes(), 29i32.to_le_bytes()].concat();
    let native = ScalarOffsets::Native(&bytes);
    let sdk_words = [11, 29];
    let sdk = ScalarOffsets::Sdk(&sdk_words);
    for offsets in [native, sdk] {
        assert_eq!(offsets.get(1_073_741_824).unwrap(), 11);
        assert_eq!(offsets.get(1_073_741_825).unwrap(), 29);
        assert!(offsets.get(536_870_912).is_err());
    }
}

#[test]
fn promotion_catch_matches_only_outer_invalid_copy_source() {
    fn needs_promotion(error: WriteError) -> bool {
        match error { WriteError::Reference(ReferenceFailure::InvalidCopySource) => true, _ => false }
    }
    let memory = memory();
    let source = open(DataType::Int64, true, Arc::new(Int64Array::from(vec![None])), &memory);
    let mut destination = writer(DataType::Int32, false, &memory);
    let error = match ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut destination) { Err(error) => error, Ok(_) => panic!("expected outer type mismatch") };
    assert!(needs_promotion(error));
    assert!(!destination.field().is_nullable());
    let error = match ScalarCopier::storage_factory(ScalarSource::new(&source).unwrap(), &mut destination) { Err(error) => error, Ok(_) => panic!("expected concrete width mismatch") };
    assert!(!needs_promotion(error));
    assert!(destination.field().is_nullable());
    let mut extension = typed(ScalarType::Oid, &memory);
    let error = match ScalarCopier::storage_factory(ScalarSource::new(&source).unwrap(), &mut extension) { Err(error) => error, Ok(_) => panic!("expected extension requirement failure") };
    assert!(matches!(&error, WriteError::Reference(ReferenceFailure::RequireFailed)));
    assert!(!needs_promotion(error));
    assert!(!extension.field().is_nullable());
    extension.write_int(7).unwrap();
    assert_eq!(extension.reader().get_int(0, NullChecks::Enabled).unwrap(), 7);
}

#[test]
fn variable_copy_row_retains_nullable_source_bit_and_byte_cursor_schedules() {
    let memory = memory();
    for data_type in [DataType::Binary, DataType::Utf8] {
        for nullable in [false, true] {
            let mut source = writer(data_type.clone(), nullable, &memory);
            source.write_bytes(b"a").unwrap();
            let source = frozen(source);
            let mut output = writer(data_type.clone(), true, &memory);
            let mut mutation = Mutation::default();
            let validity = output.fixture_state().validity.as_mut().unwrap();
            validity.ensure_capacity(8, &mut mutation).unwrap();
            validity.set(7, true, &mut mutation).unwrap();
            ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_row(0).unwrap();
            let validity = output.fixture_state().validity.as_mut().unwrap();
            assert_eq!(validity.bytes()[0], 1);
            validity.set(7, true, &mut mutation).unwrap();
            ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_row(0).unwrap();
            let output = frozen(output);
            let NodeLayout::Scalar(storage) = &output.node().storage else { panic!(); };
            let validity = storage.validity.as_ref().unwrap();
            assert_eq!(validity.bytes.as_slice()[0], 0x83);
            assert_eq!(validity.append, 2);
            assert_eq!(validity.byte_append, if nullable { 1 } else { 0 });
            assert_eq!(output.cell(0).unwrap().get_bytes().unwrap(), b"a");
            assert_eq!(output.cell(1).unwrap().get_bytes().unwrap(), b"a");
        }
    }
}

#[test]
fn fixed_copy_row_keeps_write_boolean_tail_and_byte_cursor() {
    let memory = memory();
    for nullable in [false, true] {
        let source = open(DataType::Int32, nullable, Arc::new(Int32Array::from(vec![1])), &memory);
        let mut output = writer(DataType::Int32, true, &memory);
        output.set_int(7, 99).unwrap();
        ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_row(0).unwrap();
        let output = frozen(output);
        let NodeLayout::Scalar(storage) = &output.node().storage else { panic!(); };
        let validity = storage.validity.as_ref().unwrap();
        assert_eq!(validity.bytes.as_slice()[0], 0x81);
        assert_eq!(validity.append, 1);
        assert_eq!(validity.byte_append, 0);
        assert_eq!(output.cell(7).unwrap().get_int(NullChecks::Enabled).unwrap(), 99);
    }
}

#[test]
fn direct_null_source_factories_keep_destination_checks_and_promotion_prefixes() {
    let memory = memory();
    for nullable in [false, true] {
        let mut source = writer(DataType::Null, nullable, &memory);
        source.write_undefined().unwrap();
        let source = frozen(source);
        let mut fixed = writer(DataType::Int32, false, &memory);
        assert!(matches!(ScalarCopier::storage_factory(ScalarSource::new(&source).unwrap(), &mut fixed), Err(WriteError::Reference(ReferenceFailure::CheckFailed))));
        assert_eq!(fixed.field().is_nullable(), nullable);
        fixed.write_int(11).unwrap();
        assert_eq!(fixed.reader().get_int(0, NullChecks::Enabled).unwrap(), 11);
        for data_type in [DataType::Binary, DataType::Boolean] {
            let mut target = writer(data_type.clone(), false, &memory);
            assert!(matches!(ScalarCopier::storage_factory(ScalarSource::new(&source).unwrap(), &mut target), Err(WriteError::Reference(ReferenceFailure::CheckFailed))));
            assert!(!target.field().is_nullable());
            if data_type == DataType::Boolean { target.write_boolean(true).unwrap(); }
            else { target.write_bytes(b"a").unwrap(); }
            assert_eq!(target.len(), 1);
        }
        let mut extension = typed(ScalarType::Oid, &memory);
        assert!(matches!(ScalarCopier::storage_factory(ScalarSource::new(&source).unwrap(), &mut extension), Err(WriteError::Reference(ReferenceFailure::RequireFailed))));
        assert!(!extension.field().is_nullable());
        extension.write_int(9).unwrap();
        assert_eq!(extension.reader().get_int(0, NullChecks::Enabled).unwrap(), 9);
        let mut null = writer(DataType::Null, false, &memory);
        ScalarCopier::storage_factory(ScalarSource::new(&source).unwrap(), &mut null).unwrap().copy_rows(&[-1, i32::MAX]).unwrap();
        assert_eq!(null.len(), 2);
        assert!(null.field().is_nullable());
    }
    let source = frozen(writer(DataType::Int32, false, &memory));
    let mut null = writer(DataType::Null, false, &memory);
    ScalarCopier::storage_factory(ScalarSource::new(&source).unwrap(), &mut null).unwrap().copy_row(i32::MAX).unwrap();
    assert_eq!(null.len(), 1);
    assert!(null.field().is_nullable());
}

#[test]
fn outer_null_copy_still_rejects_a_native_aborted_destination_until_clear() {
    let memory = memory();
    let source = open(DataType::Int32, false, Arc::new(Int32Array::from(vec![1])), &memory);
    let null = frozen(writer(DataType::Null, false, &memory));
    let mut output = writer(DataType::Int32, false, &memory);
    assert!(matches!(ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_rows(&[0, 2]), Err(WriteError::Native(_))));
    assert!(matches!(ScalarCopier::new(ScalarSource::new(&null).unwrap(), &mut output), Err(WriteError::Native(_))));
    output.clear();
    ScalarCopier::new(ScalarSource::new(&null).unwrap(), &mut output).unwrap().copy_row(-1).unwrap();
    assert_eq!(output.len(), 1);
    assert!(output.reader().is_null(0).unwrap());
}

#[test]
fn variable_preflight_offset_failure_leaves_destination_buffers_unchanged() {
    let memory = memory();
    let mut source = writer(DataType::Binary, false, &memory);
    source.write_bytes(b"a").unwrap();
    source.fixture_state().count = 32;
    let source = frozen(source);
    for method in 0..3 {
        let mut output = writer(DataType::Binary, true, &memory);
        output.write_bytes(b"old").unwrap();
        let before = memory.used_bytes();
        let mut copier = ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap();
        let result = match method { 0 => copier.copy_row(31), 1 => copier.copy_range(30, 2), _ => copier.copy_rows(&[0, 31]) };
        assert!(matches!(result, Err(WriteError::Reference(ReferenceFailure::Bounds))));
        drop(copier);
        assert_eq!(memory.used_bytes(), before);
        assert_eq!(output.len(), 1);
        assert_eq!(output.reader().get_bytes(0).unwrap(), b"old");
        let validity = output.fixture_state().validity.as_ref().unwrap();
        assert_eq!(validity.append, 1);
        assert_eq!(validity.bytes()[0], 1);
        let scalar::MutableScalarData::Variable { offsets, data, last_offset } = &output.fixture_state().data else { panic!(); };
        assert_eq!(offsets.append, 8);
        assert_eq!(data.append, 3);
        assert_eq!(*last_offset, 3);
        output.write_bytes(b"next").unwrap();
        let output = frozen(output);
        assert_eq!(output.cell(1).unwrap().get_bytes().unwrap(), b"next");
        let NodeLayout::Scalar(storage) = &output.node().storage else { panic!(); };
        assert_eq!(storage.validity.as_ref().unwrap().byte_append, 0);
    }
}

#[test]
fn bit_capacity_receives_the_wrapped_signed_request_even_without_count_growth() {
    let memory = MemoryBudget::new(1024 * 1024);
    let mut output = writer(DataType::Boolean, false, &memory);
    assert!(matches!(output.set_null(i32::MAX), Err(WriteError::Native(_))));
    assert_eq!(output.len(), 0);
    output.write_boolean(true).unwrap();
    assert!(output.reader().get_boolean(0, NullChecks::Enabled).unwrap());
}

#[test]
fn bit_cursor_failure_order_differs_between_appending_and_explicit_index_writes() {
    let memory = memory();
    let mut bits = MutableBits::new(&memory);
    let mut mutation = Mutation::default();
    bits.append = -1;
    assert!(matches!(bits.write(true, &mut mutation), Err(WriteError::Reference(ReferenceFailure::Bounds))));
    assert_eq!(bits.append, 0);
    bits.write(true, &mut mutation).unwrap();
    assert_eq!(bits.append, 1);
    assert!(matches!(bits.write_at(1024, true, &mut mutation), Err(WriteError::Reference(ReferenceFailure::Bounds))));
    assert_eq!(bits.append, 1);
}

#[test]
fn native_negative_variable_spans_keep_reference_failure_after_offset_prefixes() {
    let memory = memory();
    for (start, end) in [(-1i32, 0i32), (1, 0)] {
        let mut source = writer(DataType::Binary, false, &memory);
        source.write_bytes(b"a").unwrap();
        let scalar::MutableScalarData::Variable { offsets, .. } = &mut source.fixture_state().data else { panic!(); };
        let mut mutation = Mutation::default();
        offsets.set(0, &start.to_le_bytes(), &mut mutation).unwrap();
        offsets.set(4, &end.to_le_bytes(), &mut mutation).unwrap();
        let source = frozen(source);
        let mut output = writer(DataType::Binary, false, &memory);
        assert!(matches!(ScalarCopier::new(ScalarSource::new(&source).unwrap(), &mut output).unwrap().copy_row(0), Err(WriteError::Reference(ReferenceFailure::Bounds))));
        assert_eq!(output.len(), 0);
        let scalar::MutableScalarData::Variable { offsets, .. } = &output.fixture_state().data else { panic!(); };
        assert_eq!(offsets.append, 8);
        output.write_bytes(b"z").unwrap();
        output.clear();
        output.write_bytes(b"ok").unwrap();
        assert_eq!(frozen(output).cell(0).unwrap().get_bytes().unwrap(), b"ok");
    }
}

#[test]
fn bulk_nonnullable_validity_copy_clears_a_seeded_high_bit_in_its_new_tail() {
    let memory = memory();
    let source = open(DataType::Int32, false, Arc::new(Int32Array::from(vec![1])), &memory);
    let mut output = writer(DataType::Int32, true, &memory);
    output.set_int(7, 99).unwrap();
    assert!(!output.reader().is_null(7).unwrap());
    copy(&source, &mut output, &[0]);
    assert!(output.reader().is_null(7).unwrap());
    assert_eq!(output.reader().get_int(7, NullChecks::Disabled).unwrap(), 99);
    assert_eq!(output.len(), 9);
}

#[test]
fn native_scalar_materialization_retains_only_its_result_after_vector_drop() {
    let memory = memory();
    let mut output = writer(DataType::Utf8, false, &memory);
    output.write_bytes(&[0xff, b'a']).unwrap();
    let vector = frozen(output);
    let value = vector.cell(0).unwrap().object().unwrap();
    drop(vector);
    assert!(memory.used_bytes() > 0);
    assert_eq!(value.value(), &Value::Utf8("�a".into()));
    drop(value);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn copied_native_output_releases_source_and_keeps_its_own_lease() {
    let source_memory = memory();
    let output_memory = memory();
    let mut source = writer(DataType::Binary, false, &source_memory);
    source.write_bytes(b"payload").unwrap();
    let source = frozen(source);
    let mut output = writer(DataType::Binary, false, &output_memory);
    copy(&source, &mut output, &[0]);
    drop(source);
    assert_eq!(source_memory.used_bytes(), 0);
    let output = frozen(output);
    assert!(output_memory.used_bytes() > 0);
    assert_eq!(output.cell(0).unwrap().get_bytes().unwrap(), b"payload");
    drop(output);
    assert_eq!(output_memory.used_bytes(), 0);
}
