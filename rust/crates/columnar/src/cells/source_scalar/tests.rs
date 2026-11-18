use super::*;
use crate::MemoryBudget;
use super::super::descriptor::{ContainerShapeRef, StructFieldRef};
use super::super::native_scalar::{NativeScalarData, NativeScalarStorage, StoredBytes};
use super::super::write::{ScalarCopier, ScalarSource, ScalarWriter, VectorWriter, ReferenceFailure, WriteError, WriteResult};
use arrow_array::Int32Array;
use arrow_buffer::Buffer;
use rtdb_types::{ScalarType, TimeUnit};

fn memory() -> Arc<MemoryBudget> { MemoryBudget::new(16 * 1024 * 1024) }
fn descriptor(declaration: SourceScalarDeclaration, nullable: bool, memory: &Arc<MemoryBudget>) -> Arc<VectorDescriptor> {
    VectorDescriptor::source_scalar("scalar", nullable, declaration, memory).unwrap()
}
fn writer(declaration: SourceScalarDeclaration, nullable: bool, memory: &Arc<MemoryBudget>) -> ScalarWriter {
    ScalarWriter::from_descriptor(&descriptor(declaration, nullable, memory), Arc::clone(memory)).unwrap()
}
fn block(memory: &Arc<MemoryBudget>, remaining: usize) -> Arc<MemoryReservation> {
    memory.reserve(memory.limit_bytes() - memory.used_bytes() - remaining).unwrap()
}
fn reference<T>(result: WriteResult<T>, expected: ReferenceFailure) {
    match result { Err(WriteError::Reference(actual)) => assert_eq!(actual, expected), _ => panic!("expected reference failure") }
}
fn read_reference<T>(result: SourceScalarReadResult<T>, expected: SourceScalarReadFailure) {
    match result { Err(SourceScalarReadError::Reference(actual)) => assert_eq!(actual, expected), _ => panic!("expected source read failure") }
}
fn read_native<T>(result: SourceScalarReadResult<T>) {
    match result { Err(SourceScalarReadError::Native(error)) => assert_eq!(error.code, "rtdb/unretained-byte-view"), _ => panic!("expected native demand failure") }
}
fn storage(vector: &ExecutionVector) -> &NativeScalarStorage {
    let NodeLayout::Scalar(storage) = &vector.node().storage else { panic!("expected native scalar") };
    storage
}
fn data(vector: &ExecutionVector) -> &StoredBytes {
    let NativeScalarData::Fixed(data) = &storage(vector).data else { panic!("expected fixed bytes") };
    data
}
fn declaration(vector: &ExecutionVector) -> SourceScalarDeclaration { vector.descriptor().scalar().unwrap().source_declaration().unwrap() }
fn retained(bytes: &[u8], memory: &Arc<MemoryBudget>) -> RetainedBuffer {
    RetainedBuffer::with_reservation(Buffer::from_slice_ref(bytes), memory.reserve(bytes.len() + 128).unwrap()).unwrap()
}
fn fixture(descriptor: Arc<VectorDescriptor>, count: i32, storage: NodeLayout, memory: &Arc<MemoryBudget>) -> ExecutionVector {
    let owner = Arc::new(NodeOwner { backing: BackingOwner::Native, metadata: memory.reserve(512).unwrap() });
    ExecutionVector::direct(Arc::new(PhysicalNode { declaration: Declaration::Native, descriptor, count: SourceCount::new(count), storage, owner }))
}
fn declarations() -> Vec<SourceScalarDeclaration> {
    let mut declarations = Vec::new();
    for width in [i32::MIN, -1, 0, 1, i32::MAX] { declarations.push(SourceScalarDeclaration::FixedBinary(width)); }
    for unit in [TimeUnit::Second, TimeUnit::Millisecond, TimeUnit::Microsecond, TimeUnit::Nanosecond] {
        declarations.push(SourceScalarDeclaration::Time32(unit));
        declarations.push(SourceScalarDeclaration::Time64(unit));
    }
    for inner_width in [i32::MIN, -1, 0, 8, 16, 32, i32::MAX] { declarations.push(SourceScalarDeclaration::Uuid { inner_width }); }
    declarations
}

#[derive(Debug, PartialEq)]
struct Prefix {
    count: i32,
    field: Field,
    logical: LogicalType,
    declaration: SourceScalarDeclaration,
    validity: Option<(Vec<u8>, i32, i64)>,
    data: Vec<u8>,
    append: i64,
}
fn prefix(vector: &ExecutionVector) -> Prefix {
    Prefix {
        count: vector.value_count(), field: vector.descriptor().scalar_field().unwrap().clone(), logical: vector.logical_type().unwrap().clone(), declaration: declaration(vector),
        validity: storage(vector).validity.as_ref().map(|bits| (bits.bytes.as_slice().to_vec(), bits.append, bits.byte_append)),
        data: data(vector).bytes.as_slice().to_vec(), append: data(vector).append,
    }
}

#[test]
fn direct_declarations_retain_raw_parameters_and_independent_uuid_widths() {
    let memory = memory();
    for declaration in declarations() {
        for nullable in [false, true] {
            let descriptor = descriptor(declaration, nullable, &memory);
            assert_eq!(descriptor.name(), "scalar");
            assert_eq!(descriptor.is_nullable(), nullable);
            assert_eq!(descriptor.reader_nullable(), nullable);
            let scalar = descriptor.scalar().unwrap();
            assert_eq!(scalar.source_declaration(), Some(declaration));
            assert_eq!(scalar.fixed_width().unwrap(), Some(declaration.fixed_width()));
            let projected = descriptor.arrow_field().unwrap();
            assert_eq!(projected.field(), scalar.field());
            let strict = match declaration {
                SourceScalarDeclaration::FixedBinary(width) => width >= 0,
                SourceScalarDeclaration::Time32(unit) => unit <= TimeUnit::Millisecond,
                SourceScalarDeclaration::Time64(unit) => unit >= TimeUnit::Microsecond,
                SourceScalarDeclaration::Uuid { .. } => true,
            };
            assert_eq!(descriptor.logical_type().unwrap().validate().is_ok(), strict);
            assert_eq!(crate::from_field(projected.field()).is_ok(), strict);
            if matches!(declaration, SourceScalarDeclaration::Uuid { .. }) {
                assert_eq!(scalar.field().data_type(), &DataType::FixedSizeBinary(16));
                assert_eq!(scalar.field().metadata().get(crate::EXTENSION_NAME_KEY).map(String::as_str), Some("uuid"));
                assert_eq!(scalar.field().metadata().get(crate::EXTENSION_METADATA_KEY).map(String::as_str), Some(""));
            }
        }
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn uuid_adopts_the_actual_fixed_binary_buffers_and_outlives_the_inner() {
    let parent = memory();
    let memory = parent.scope();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(24), true, &memory);
    let bytes = *b"0123456789abcdefghijklmn";
    output.write_null().unwrap();
    output.write_bytes(&bytes).unwrap();
    let inner = output.finish().unwrap();
    let inner_charge = inner.node().owner.metadata.bytes();
    let retained_before = memory.used_bytes();

    let uuid = inner.adopt_source_uuid().unwrap().unwrap();
    let uuid_charge = uuid.node().owner.metadata.bytes();
    assert!(Arc::ptr_eq(inner.budget(), &memory));
    assert!(Arc::ptr_eq(uuid.budget(), &memory));
    assert_eq!(memory.used_bytes(), retained_before + uuid_charge);
    assert_eq!(parent.used_bytes(), memory.used_bytes());
    assert_eq!(uuid.value_count(), 2);
    assert_eq!(uuid.descriptor().name(), inner.descriptor().name());
    assert!(uuid.reader_nullable());
    assert_eq!(declaration(&uuid), SourceScalarDeclaration::Uuid { inner_width: 24 });
    assert_eq!(uuid.descriptor().scalar_field().unwrap().data_type(), &DataType::FixedSizeBinary(16));
    assert_eq!(uuid.logical_type().unwrap(), &LogicalType::Scalar(ScalarType::Uuid).maybe());
    assert!(std::ptr::eq(data(&inner).bytes.as_slice(), data(&uuid).bytes.as_slice()));
    assert_eq!(data(&inner).append, data(&uuid).append);
    let inner_validity = storage(&inner).validity.as_ref().unwrap();
    let uuid_validity = storage(&uuid).validity.as_ref().unwrap();
    assert!(std::ptr::eq(inner_validity.bytes.as_slice(), uuid_validity.bytes.as_slice()));
    assert_eq!(inner_validity.append, uuid_validity.append);
    assert_eq!(inner_validity.byte_append, uuid_validity.byte_append);
    let inner_reader = SourceScalarReader::new(&inner).unwrap().unwrap();
    assert_eq!(inner_reader.get_bytes(1).unwrap().demand_native().unwrap(), &bytes);
    drop(inner);

    assert_eq!(memory.used_bytes(), retained_before + uuid_charge - inner_charge);
    assert_eq!(parent.used_bytes(), memory.used_bytes());
    let reader = SourceScalarReader::new(&uuid).unwrap().unwrap();
    let destination = MemoryBudget::new(8192 + 4 * bytes.len());
    assert!(reader.binary_object(0, &destination).unwrap().unwrap().is_none());
    assert_eq!(destination.peak_bytes(), 0);
    let value = reader.binary_object(1, &destination).unwrap().unwrap().unwrap();
    assert_eq!(reader.get_bytes(1).unwrap().demand_native().unwrap(), &bytes);
    drop(uuid);
    assert_eq!(value.value(), &Value::Uuid(*b"0123456789abcdef"));
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(parent.used_bytes(), 0);
    drop(value);
    assert_eq!(destination.used_bytes(), 0);
}

#[test]
fn uuid_adoption_preserves_signed_width_and_defers_object_failures() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(-1), true, &memory);
    output.ensure_capacity(1).unwrap();
    let inner = output.finish().unwrap();
    let uuid = inner.adopt_source_uuid().unwrap().unwrap();
    assert_eq!(declaration(&uuid), SourceScalarDeclaration::Uuid { inner_width: -1 });
    let reader = SourceScalarReader::new(&uuid).unwrap().unwrap();
    let destination = MemoryBudget::new(0);
    assert!(reader.binary_object(0, &destination).unwrap().unwrap().is_none());
    read_reference(reader.binary_object0(0, &destination).unwrap(), SourceScalarReadFailure::NegativeViewLength(-1));
    assert_eq!(destination.peak_bytes(), 0);

    let mut output = writer(SourceScalarDeclaration::FixedBinary(8), false, &memory);
    output.write_bytes(&[42; 8]).unwrap();
    let short = output.finish().unwrap().adopt_source_uuid().unwrap().unwrap();
    assert!(!short.reader_nullable());
    assert!(storage(&short).validity.is_none());
    let destination = MemoryBudget::new(4096 + 4 * 8);
    let reader = SourceScalarReader::new(&short).unwrap().unwrap();
    read_reference(reader.binary_object(0, &destination).unwrap(), SourceScalarReadFailure::UuidSecondLongUnderflow);
    assert_eq!(destination.peak_bytes(), 4096 + 4 * 8);
    assert_eq!(destination.used_bytes(), 0);
}

#[test]
fn uuid_adoption_carries_wrapped_count_without_admitting_rows() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(0), false, &memory);
    output.ensure_capacity(i32::MAX).unwrap();
    output.write_undefined().unwrap();
    let inner = output.finish().unwrap();
    let uuid = inner.adopt_source_uuid().unwrap().unwrap();
    assert_eq!(inner.value_count(), i32::MIN);
    assert_eq!(uuid.value_count(), inner.value_count());
    assert_eq!(uuid.checked_len().unwrap_err().code, "rtdb/vector-length");
    assert_eq!(declaration(&uuid), SourceScalarDeclaration::Uuid { inner_width: 0 });
    let reader = SourceScalarReader::new(&uuid).unwrap().unwrap();
    assert!(reader.get_bytes(i32::MIN).unwrap().demand_native().unwrap().is_empty());
}

#[test]
fn uuid_adoption_rejects_other_receivers_and_preserves_the_inner_on_budget_failure() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(8), false, &memory);
    output.write_bytes(&[37; 8]).unwrap();
    let inner = output.finish().unwrap();
    let selected = inner.select(&Selection::All).unwrap();
    let uuid = inner.adopt_source_uuid().unwrap().unwrap();
    let field = Arc::new(Field::new("sdk", DataType::FixedSizeBinary(8), false));
    let sdk = ScalarWriter::new(field, Arc::clone(&memory)).unwrap().finish().unwrap();
    let retained_before = memory.used_bytes();
    let blocker = block(&memory, 0);
    assert!(sdk.adopt_source_uuid().unwrap().is_none());
    assert!(selected.adopt_source_uuid().unwrap().is_none());
    assert!(uuid.adopt_source_uuid().unwrap().is_none());
    assert_eq!(inner.adopt_source_uuid().unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), retained_before + blocker.bytes());
    let reader = SourceScalarReader::new(&inner).unwrap().unwrap();
    assert_eq!(reader.get_bytes(0).unwrap().demand_native().unwrap(), &[37; 8]);
    drop(blocker);
    assert_eq!(memory.used_bytes(), retained_before);
    let retry = inner.adopt_source_uuid().unwrap().unwrap();
    assert!(std::ptr::eq(data(&inner).bytes.as_slice(), data(&retry).bytes.as_slice()));
    drop(retry);
    assert_eq!(memory.used_bytes(), retained_before);
}

#[test]
fn strict_sdk_factories_and_raw_reader_provenance_stay_separate() {
    let memory = memory();
    for data_type in [DataType::FixedSizeBinary(-1), DataType::Time32(arrow_schema::TimeUnit::Microsecond), DataType::Time64(arrow_schema::TimeUnit::Second)] {
        let field = Arc::new(Field::new("strict", data_type, false));
        assert!(crate::from_field(&field).is_err());
        assert!(ScalarWriter::new(field, Arc::clone(&memory)).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
    for scalar in [ScalarType::FixedSizeBinary(16), ScalarType::Time { unit: TimeUnit::Microsecond, bit_width: 64 }, ScalarType::Uuid] {
        let field = Arc::new(crate::to_field("strict", &LogicalType::Scalar(scalar)).unwrap());
        let output = ScalarWriter::new(field, Arc::clone(&memory)).unwrap();
        assert!(output.source_reader().unwrap().is_none());
        let frozen = output.finish().unwrap();
        assert!(SourceScalarReader::new(&frozen).unwrap().is_none());
        assert!(frozen.ingress_field().arrow().is_some());
    }
    let array: ArrayRef = Arc::new(Int32Array::from(vec![7]));
    let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
    let leaf = ExecutionVector::with_reservation(Arc::new(Field::new("sdk", DataType::Int32, false)), array, reservation).unwrap();
    assert!(SourceScalarReader::new(&leaf).unwrap().is_none());
    drop(leaf);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn source_metadata_is_rebuilt_in_the_target_budget_before_publication() {
    let source = memory();
    let target = memory();
    let input = descriptor(SourceScalarDeclaration::Time32(TimeUnit::Microsecond), false, &source);
    let source_bytes = source.used_bytes();
    let output = ScalarWriter::from_descriptor(&input, Arc::clone(&target)).unwrap();
    assert!(target.used_bytes() > 0);
    assert_eq!(source.used_bytes(), source_bytes);
    drop(input);
    assert_eq!(source.used_bytes(), 0);
    assert_eq!(output.field().data_type(), &DataType::Time32(arrow_schema::TimeUnit::Microsecond));
    let frozen = output.finish().unwrap();
    assert!(matches!(frozen.ingress_field(), DeclarationRef::Native(_)));
    assert_eq!(declaration(&frozen), SourceScalarDeclaration::Time32(TimeUnit::Microsecond));
    drop(frozen);
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn metadata_admission_failure_keeps_input_ownership_and_target_empty() {
    let source = memory();
    let declaration = SourceScalarDeclaration::Uuid { inner_width: -1 };
    let input = descriptor(declaration, false, &source);
    let bytes = input.scalar().unwrap().source_metadata().unwrap().reservation().bytes();
    let target = MemoryBudget::new(bytes - 1);
    let before = source.used_bytes();
    assert!(ScalarWriter::from_descriptor(&input, Arc::clone(&target)).is_err());
    assert_eq!(target.used_bytes(), 0);
    assert_eq!(source.used_bytes(), before);
    assert_eq!(input.scalar().unwrap().source_declaration(), Some(declaration));
    assert!(VectorDescriptor::source_scalar("scalar", false, declaration, &target).is_err());
    assert_eq!(target.used_bytes(), 0);
    drop(input);
    assert_eq!(source.used_bytes(), 0);
}

#[test]
fn source_nullable_rebuild_requires_overlap_and_leaves_ready_before_swap_failure() {
    let memory = memory();
    let declaration = SourceScalarDeclaration::Time32(TimeUnit::Microsecond);
    let input = descriptor(declaration, false, &memory);
    let bytes = input.scalar().unwrap().source_metadata().unwrap().reservation().bytes();
    let mut output = ScalarWriter::from_descriptor(&input, Arc::clone(&memory)).unwrap();
    drop(input);
    let before = memory.used_bytes();
    let blocker = block(&memory, bytes - 1);
    assert!(matches!(output.write_null(), Err(WriteError::Native(_))));
    assert!(!output.field().is_nullable());
    assert_eq!(output.value_count(), 0);
    assert_eq!(output.source_reader().unwrap().unwrap().get_pointer(0).length(), 4);
    assert_eq!(memory.used_bytes(), memory.limit_bytes() - (bytes - 1));
    drop(blocker);
    assert_eq!(memory.used_bytes(), before);
    output.write_int(7).unwrap();
    let captured = output.finish().unwrap();
    assert!(!captured.reader_nullable());
    assert_eq!(captured.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 7);
    drop(captured);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn source_nullable_metadata_is_installed_before_a_failed_validity_fill() {
    let memory = memory();
    let declaration = SourceScalarDeclaration::Time32(TimeUnit::Microsecond);
    let input = descriptor(declaration, false, &memory);
    let bytes = input.scalar().unwrap().source_metadata().unwrap().reservation().bytes();
    let mut output = ScalarWriter::from_descriptor(&input, Arc::clone(&memory)).unwrap();
    drop(input);
    output.ensure_capacity(i32::MAX).unwrap();
    let blocker = block(&memory, bytes);
    assert!(matches!(output.write_null(), Err(WriteError::Native(_))));
    assert!(output.field().is_nullable());
    assert_eq!(output.value_count(), i32::MAX);
    let reader = output.source_reader().unwrap().unwrap();
    assert_eq!(reader.get_pointer(0).length(), 4);
    read_reference(reader.get_bytes(0), SourceScalarReadFailure::Unsupported { declaration, method: "getBytes" });
    assert!(matches!(output.write_int(1), Err(WriteError::Native(_))));
    drop(blocker);
    output.clear();
    output.write_int(9).unwrap();
    let captured = output.finish().unwrap();
    assert!(captured.reader_nullable());
    assert_eq!(declaration_of(&captured), declaration);
    assert_eq!(captured.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 9);
    drop(captured);
    assert_eq!(memory.used_bytes(), 0);
}

fn declaration_of(vector: &ExecutionVector) -> SourceScalarDeclaration { declaration(vector) }

#[test]
fn negative_fixed_bytes_and_undefined_retain_distinct_count_and_validity_prefixes() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(-1), true, &memory);
    reference(output.write_undefined(), ReferenceFailure::Bounds);
    assert_eq!(output.value_count(), 0);
    read_reference(output.source_reader().unwrap().unwrap().get_bytes(0), SourceScalarReadFailure::NegativeViewLength(-1));
    output.write_bytes(&[42]).unwrap();
    assert_eq!(output.value_count(), 1);
    let captured = output.finish().unwrap();
    assert_eq!(data(&captured).append, 1);
    assert_eq!(data(&captured).bytes.as_slice()[0], 42);
    let bits = storage(&captured).validity.as_ref().unwrap();
    assert_eq!(bits.append, 1);
    assert_eq!(bits.byte_append, 0);
    assert_eq!(bits.bytes.as_slice()[0] & 1, 1);
    assert_eq!(captured.descriptor().scalar_field().unwrap().data_type(), &DataType::FixedSizeBinary(-1));
    assert!(captured.cell(0).unwrap().get_bytes().is_err());
    assert!(captured.cell(1).is_err());
    drop(captured);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn negative_write_null_promotes_before_checked_zero_and_stays_publishable() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(-1), false, &memory);
    reference(output.write_null(), ReferenceFailure::Bounds);
    assert!(output.field().is_nullable());
    assert_eq!(output.value_count(), 0);
    let captured = output.finish().unwrap();
    let bits = storage(&captured).validity.as_ref().unwrap();
    assert_eq!(bits.append, 1);
    assert_eq!(bits.bytes.as_slice()[0] & 1, 0);
    assert_eq!(data(&captured).append, 0);
    assert_eq!(data(&captured).bytes.len(), 0);
    assert_eq!(captured.logical_type().unwrap(), &LogicalType::Scalar(ScalarType::FixedSizeBinary(-1)).maybe());
    drop(captured);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn signed_capacity_uses_wrapped_products_without_payload_or_cursor_admission() {
    let memory = memory();
    for (width, count) in [(-1, 1), (i32::MIN, 2), (i32::MAX, 2), (2, i32::MAX)] {
        let mut output = writer(SourceScalarDeclaration::FixedBinary(width), false, &memory);
        let before = memory.used_bytes();
        output.ensure_capacity(count).unwrap();
        assert_eq!(output.value_count(), count);
        assert_eq!(memory.used_bytes(), before);
        let frozen = output.finish().unwrap();
        assert_eq!(data(&frozen).bytes.len(), 0);
        assert_eq!(data(&frozen).append, 0);
        assert_eq!(SourceScalarReader::new(&frozen).unwrap().unwrap().get_pointer(0).length(), i64::from(width));
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn zero_width_writes_advance_count_and_actual_byte_appends_independently() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(0), false, &memory);
    let before = memory.used_bytes();
    output.write_undefined().unwrap();
    assert_eq!(memory.used_bytes(), before);
    output.write_bytes(&[42]).unwrap();
    let frozen = output.finish().unwrap();
    assert_eq!(frozen.value_count(), 2);
    assert_eq!(data(&frozen).append, 1);
    assert!(frozen.cell(0).unwrap().get_bytes().unwrap().is_empty());
    let reader = SourceScalarReader::new(&frozen).unwrap().unwrap();
    let view = reader.get_bytes(i32::MIN).unwrap();
    assert_eq!(view.offset(), 0);
    assert!(view.is_empty());
    assert!(view.demand_native().unwrap().is_empty());
    drop(frozen);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn negative_fixed_setter_prefix_and_long_long_lanes_ignore_declared_stride() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(-1), false, &memory);
    reference(output.set_int(3, 7), ReferenceFailure::Bounds);
    assert_eq!(output.value_count(), 4);
    output.write_long_long(i64::MIN + 1, i64::MAX - 1).unwrap();
    assert_eq!(output.value_count(), 5);
    let frozen = output.finish().unwrap();
    assert_eq!(data(&frozen).append, 16);
    assert_eq!(frozen.cell(0).unwrap().get_long_long_high().unwrap(), i64::MIN + 1);
    assert_eq!(frozen.cell(0).unwrap().get_long_long_low().unwrap(), i64::MAX - 1);
    assert!(frozen.cell(5).is_err());
    drop(frozen);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn all_time_constructor_units_keep_concrete_write_and_checked_lane_widths() {
    let memory = memory();
    for unit in [TimeUnit::Second, TimeUnit::Millisecond, TimeUnit::Microsecond, TimeUnit::Nanosecond] {
        let mut narrow = writer(SourceScalarDeclaration::Time32(unit), false, &memory);
        narrow.write_int(7).unwrap();
        narrow.write_long(4_294_967_301).unwrap();
        assert_eq!(narrow.reader().get_int(1, NullChecks::Enabled).unwrap(), 5);
        let frozen = narrow.finish().unwrap();
        assert_eq!(data(&frozen).append, 8);
        assert_eq!(frozen.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 7);
        assert_eq!(frozen.cell(1).unwrap().get_int(NullChecks::Enabled).unwrap(), 5);
        assert!(frozen.cell(0).unwrap().get_long(NullChecks::Enabled).is_err());
        let reader = SourceScalarReader::new(&frozen).unwrap().unwrap();
        assert_eq!(reader.get_pointer(1).demand_native().unwrap(), &5i32.to_le_bytes());
        read_reference(reader.get_bytes(0), SourceScalarReadFailure::Unsupported { declaration: SourceScalarDeclaration::Time32(unit), method: "getBytes" });
        let mut wide = writer(SourceScalarDeclaration::Time64(unit), false, &memory);
        wide.write_long(9).unwrap();
        let wide = wide.finish().unwrap();
        assert_eq!(data(&wide).append, 8);
        assert_eq!(wide.cell(0).unwrap().get_long(NullChecks::Enabled).unwrap(), 9);
        read_reference(SourceScalarReader::new(&wide).unwrap().unwrap().get_bytes(0), SourceScalarReadFailure::Unsupported { declaration: SourceScalarDeclaration::Time64(unit), method: "getBytes" });
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn uuid_capacity_rejects_before_allocation_and_preserves_complete_prefix() {
    for inner_width in [8, 0, -1] {
        for nullable in [false, true] {
            let declaration = SourceScalarDeclaration::Uuid { inner_width };
            let control_memory = memory();
            let mut control = writer(declaration, nullable, &control_memory);
            control.write_bytes(&[42]).unwrap();
            let control = control.finish().unwrap();
            let expected = prefix(&control);
            let target = memory();
            let mut output = writer(declaration, nullable, &target);
            output.write_bytes(&[42]).unwrap();
            let count = output.value_count();
            let before = target.used_bytes();
            let blocker = block(&target, 0);
            reference(output.ensure_capacity(i32::MAX), ReferenceFailure::Unsupported("ensureCapacity"));
            assert_eq!(output.value_count(), count);
            assert_eq!(output.field().is_nullable(), nullable);
            assert_eq!(output.source_reader().unwrap().unwrap().get_pointer(0).length(), i64::from(inner_width));
            assert_eq!(target.used_bytes(), target.limit_bytes());
            let frozen = output.finish().unwrap();
            assert_eq!(prefix(&frozen), expected);
            drop(blocker);
            assert_eq!(target.used_bytes(), before);
            drop(frozen);
            assert_eq!(target.used_bytes(), 0);
            drop(control);
            assert_eq!(control_memory.used_bytes(), 0);
        }
    }
}

#[test]
fn uuid_inner_width_controls_byte_adapters_and_keeps_outer_metadata_sixteen() {
    let memory = memory();
    for inner_width in [0, 8, 16, 32, -1] {
        let mut output = writer(SourceScalarDeclaration::Uuid { inner_width }, false, &memory);
        output.write_bytes(&[42; 16]).unwrap();
        reference(output.write_long_long(1, 2), ReferenceFailure::Unsupported("writeLongLong"));
        reference(output.set_long(0, 1), ReferenceFailure::Unsupported("setLong"));
        let frozen = output.finish().unwrap();
        assert_eq!(data(&frozen).append, 16);
        assert_eq!(frozen.descriptor().scalar_field().unwrap().data_type(), &DataType::FixedSizeBinary(16));
        let reader = SourceScalarReader::new(&frozen).unwrap().unwrap();
        assert_eq!(reader.get_pointer(0).length(), i64::from(inner_width));
        if inner_width < 0 {
            read_reference(reader.get_bytes(0), SourceScalarReadFailure::NegativeViewLength(i64::from(inner_width)));
            assert!(frozen.cell(0).unwrap().get_bytes().is_err());
        } else {
            assert_eq!(reader.get_bytes(0).unwrap().len(), inner_width as usize);
            assert_eq!(frozen.cell(0).unwrap().get_bytes().unwrap().len(), inner_width as usize);
            assert_eq!(frozen.cell(0).unwrap().byte_span().unwrap().len(), inner_width as usize);
        }
        assert_eq!(frozen.cell(0).unwrap().object().is_ok(), inner_width == 16);
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn negative_views_fail_at_view_creation_but_pointer_facts_survive() {
    let memory = memory();
    for width in [i32::MIN, -1] {
        for declaration in [SourceScalarDeclaration::FixedBinary(width), SourceScalarDeclaration::Uuid { inner_width: width }] {
            let output = writer(declaration, false, &memory);
            let reader = output.source_reader().unwrap().unwrap();
            for row in [i32::MIN, -1, 0, 1, i32::MAX] {
                let pointer = reader.get_pointer(row);
                assert_eq!(pointer.offset(), i64::from(row.wrapping_mul(width)));
                assert_eq!(pointer.length(), i64::from(width));
                read_reference(reader.get_bytes(row), SourceScalarReadFailure::NegativeViewLength(i64::from(width)));
                read_native(pointer.demand_native());
            }
        }
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn positive_extreme_widths_keep_wrapped_offsets_without_eager_extent_checks() {
    let memory = memory();
    for width in [1, i32::MAX] {
        for declaration in [SourceScalarDeclaration::FixedBinary(width), SourceScalarDeclaration::Uuid { inner_width: width }] {
            let output = writer(declaration, false, &memory);
            let reader = output.source_reader().unwrap().unwrap();
            for row in [i32::MIN, -1, 0, 1, i32::MAX] {
                let pointer = reader.get_pointer(row);
                let view = reader.get_bytes(row).unwrap();
                assert_eq!(pointer.offset(), i64::from(row.wrapping_mul(width)));
                assert_eq!(view.offset(), pointer.offset());
                assert_eq!(view.len(), width as usize);
                read_native(view.demand_native());
            }
        }
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn positive_views_expose_facts_before_native_payload_admission_without_row_checks() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(1), false, &memory);
    output.write_bytes(&[42]).unwrap();
    let reader = output.source_reader().unwrap().unwrap();
    let past_count = reader.get_bytes(127).unwrap();
    assert_eq!(past_count.offset(), 127);
    assert_eq!(past_count.len(), 1);
    assert!(!past_count.is_empty());
    assert_eq!(past_count.demand_native().unwrap(), &[0]);
    let past_storage = reader.get_bytes(128).unwrap();
    assert_eq!(past_storage.len(), 1);
    read_native(past_storage.demand_native());
    let negative_offset = reader.get_bytes(-1).unwrap();
    assert_eq!(negative_offset.offset(), -1);
    read_native(negative_offset.demand_native());
    assert!(output.reader().get_bytes(1).is_err());
    let frozen = output.finish().unwrap();
    assert!(frozen.cell(1).is_err());
    let reader = SourceScalarReader::new(&frozen).unwrap().unwrap();
    assert_eq!(reader.get_bytes(127).unwrap().demand_native().unwrap(), &[0]);
    drop(frozen);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn admitted_slices_and_facts_outlive_intermediate_readers_with_the_owner_borrow() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(1), false, &memory);
    output.write_bytes(&[42]).unwrap();
    let view = { let reader = output.source_reader().unwrap().unwrap(); reader.get_bytes(0).unwrap() };
    let bytes = view.demand_native().unwrap();
    drop(view);
    assert_eq!(bytes, &[42]);
    let frozen = output.finish().unwrap();
    let pointer = { let reader = SourceScalarReader::new(&frozen).unwrap().unwrap(); reader.get_pointer(0) };
    let bytes = pointer.demand_native().unwrap();
    drop(pointer);
    assert_eq!(bytes, &[42]);
    drop(frozen);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn aborted_writer_keeps_borrowed_prefix_inspection_without_recovering_its_gate() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(128), true, &memory);
    output.write_bytes(&[42; 128]).unwrap();
    let blocker = block(&memory, 0);
    assert!(matches!(output.write_undefined(), Err(WriteError::Native(_))));
    assert_eq!(output.value_count(), 1);
    assert!(output.field().is_nullable());
    let reader = output.source_reader().unwrap().unwrap();
    assert_eq!(reader.get_pointer(1).offset(), 128);
    assert_eq!(reader.get_bytes(0).unwrap().demand_native().unwrap(), &[42; 128]);
    read_native(reader.get_bytes(1).unwrap().demand_native());
    assert_eq!(output.reader().get_bytes(0).unwrap(), &[42; 128]);
    assert!(matches!(output.write_bytes(&[]), Err(WriteError::Native(_))));
    assert_eq!(memory.used_bytes(), memory.limit_bytes());
    drop(blocker);
    output.clear();
    output.write_bytes(&[9]).unwrap();
    let captured = output.finish().unwrap();
    assert_eq!(captured.value_count(), 1);
    assert_eq!(data(&captured).append, 1);
    assert_eq!(data(&captured).bytes.as_slice()[0], 9);
    assert_eq!(storage(&captured).validity.as_ref().unwrap().append, 1);
    drop(captured);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn payload_before_failed_validity_is_inspectable_even_when_logical_count_is_zero() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(1), true, &memory);
    let blocker = block(&memory, 256);
    assert!(matches!(output.write_bytes(&[42]), Err(WriteError::Native(_))));
    assert_eq!(output.value_count(), 0);
    assert_eq!(output.source_reader().unwrap().unwrap().get_bytes(0).unwrap().demand_native().unwrap(), &[42]);
    assert!(output.reader().get_bytes(0).is_err());
    assert!(matches!(output.write_bytes(&[9]), Err(WriteError::Native(_))));
    assert!(output.source_reader().unwrap().is_some());
    assert!(output.finish().is_err());
    assert_eq!(memory.used_bytes(), blocker.bytes());
    drop(blocker);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn negative_width_and_negative_copy_count_produce_a_positive_byte_extent() {
    let memory = memory();
    let mut input = writer(SourceScalarDeclaration::FixedBinary(-1), false, &memory);
    input.write_bytes(&[42]).unwrap();
    let input = input.finish().unwrap();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(-1), false, &memory);
    ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap().copy_range(0, -1).unwrap();
    assert_eq!(output.value_count(), -1);
    assert!(output.checked_len().is_err());
    let frozen = output.finish().unwrap();
    assert_eq!(frozen.value_count(), -1);
    assert_eq!(data(&frozen).append, 1);
    assert_eq!(data(&frozen).bytes.as_slice()[0], 42);
    assert!(frozen.cell(0).is_err());
    assert!(SourceScalarReader::new(&frozen).unwrap().is_some());
    drop((input, frozen));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn negative_copy_rejects_destination_span_after_validity_before_source_payload() {
    let memory = memory();
    let input = fixture(descriptor(SourceScalarDeclaration::FixedBinary(-1), false, &memory), 0, NodeLayout::Scalar(NativeScalarStorage { validity: None, data: NativeScalarData::Null }), &memory);
    let mut output = writer(SourceScalarDeclaration::FixedBinary(-1), true, &memory);
    reference(ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap().copy_row(i32::MAX), ReferenceFailure::Bounds);
    assert_eq!(output.value_count(), 0);
    let frozen = output.finish().unwrap();
    assert_eq!(data(&frozen).append, 0);
    assert_eq!(data(&frozen).bytes.len(), 0);
    let validity = storage(&frozen).validity.as_ref().unwrap();
    assert_eq!(validity.append, 1);
    assert_eq!(validity.bytes.as_slice()[0] & 1, 1);
    drop((input, frozen));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn uuid_copy_factory_promotes_nullable_before_comparing_actual_inner_widths() {
    let memory = memory();
    let mut input = writer(SourceScalarDeclaration::Uuid { inner_width: 8 }, true, &memory);
    input.write_bytes(&[42; 16]).unwrap();
    let input = input.finish().unwrap();
    let mut output = writer(SourceScalarDeclaration::Uuid { inner_width: 16 }, false, &memory);
    output.write_bytes(&[9; 16]).unwrap();
    reference(ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output), ReferenceFailure::CheckFailed);
    assert!(output.field().is_nullable());
    assert_eq!(output.value_count(), 1);
    assert_eq!(output.source_reader().unwrap().unwrap().get_pointer(0).length(), 16);
    let frozen = output.finish().unwrap();
    assert_eq!(declaration(&frozen), SourceScalarDeclaration::Uuid { inner_width: 16 });
    assert_eq!(data(&frozen).append, 16);
    assert_eq!(&data(&frozen).bytes.as_slice()[..16], &[9; 16]);
    let validity = storage(&frozen).validity.as_ref().unwrap();
    assert_eq!(validity.append, 1);
    assert_eq!(validity.byte_append, 0);
    assert_eq!(validity.bytes.as_slice()[0] & 1, 1);
    drop((input, frozen));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn uuid_same_inner_width_copies_actual_bytes_without_using_outer_sixteen() {
    let memory = memory();
    let mut input = writer(SourceScalarDeclaration::Uuid { inner_width: 8 }, false, &memory);
    input.write_bytes(&[42; 16]).unwrap();
    let input = input.finish().unwrap();
    let mut output = writer(SourceScalarDeclaration::Uuid { inner_width: 8 }, false, &memory);
    ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap().copy_row(0).unwrap();
    let frozen = output.finish().unwrap();
    assert_eq!(data(&frozen).append, 8);
    assert_eq!(frozen.value_count(), 1);
    assert_eq!(frozen.cell(0).unwrap().get_bytes().unwrap(), &[42; 8]);
    assert_eq!(frozen.descriptor().scalar_field().unwrap().data_type(), &DataType::FixedSizeBinary(16));
    drop((input, frozen));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn concrete_time_outer_type_rejects_before_nullable_promotion_or_copy() {
    let memory = memory();
    let mut input = writer(SourceScalarDeclaration::Time32(TimeUnit::Microsecond), true, &memory);
    input.write_int(42).unwrap();
    let input = input.finish().unwrap();
    for declaration in [SourceScalarDeclaration::Time64(TimeUnit::Microsecond), SourceScalarDeclaration::Time32(TimeUnit::Nanosecond)] {
        let mut output = writer(declaration, false, &memory);
        let before = memory.used_bytes();
        reference(ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output), ReferenceFailure::InvalidCopySource);
        assert!(!output.field().is_nullable());
        assert_eq!(output.value_count(), 0);
        assert_eq!(memory.used_bytes(), before);
    }
    let mut output = writer(SourceScalarDeclaration::Time32(TimeUnit::Microsecond), false, &memory);
    ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap().copy_rows(&[0, 0]).unwrap();
    let frozen = output.finish().unwrap();
    assert!(frozen.reader_nullable());
    assert_eq!(frozen.value_count(), 2);
    assert_eq!(data(&frozen).append, 8);
    assert_eq!(frozen.cell(1).unwrap().get_int(NullChecks::Enabled).unwrap(), 42);
    drop((input, frozen));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn zero_width_native_copy_keeps_raw_rows_and_zero_extents_independent() {
    let memory = memory();
    let input = writer(SourceScalarDeclaration::FixedBinary(0), false, &memory).finish().unwrap();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(0), false, &memory);
    let before = memory.used_bytes();
    let mut copier = ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap();
    copier.copy_row(i32::MAX).unwrap();
    copier.copy_range(i32::MIN, -1).unwrap();
    drop(copier);
    assert_eq!(output.value_count(), 0);
    assert_eq!(memory.used_bytes(), before);
    let frozen = output.finish().unwrap();
    assert_eq!(data(&frozen).append, 0);
    assert_eq!(data(&frozen).bytes.len(), 0);
    drop((input, frozen));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn copy_budget_failure_before_mutation_leaves_retry_ready() {
    let memory = memory();
    let mut input = writer(SourceScalarDeclaration::FixedBinary(1), false, &memory);
    input.write_bytes(&[42]).unwrap();
    let input = input.finish().unwrap();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(1), false, &memory);
    let before = memory.used_bytes();
    let blocker = block(&memory, 0);
    assert!(matches!(ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap().copy_row(0), Err(WriteError::Native(_))));
    assert_eq!(output.value_count(), 0);
    assert!(output.source_reader().unwrap().is_some());
    drop(blocker);
    assert_eq!(memory.used_bytes(), before);
    ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap().copy_row(0).unwrap();
    let frozen = output.finish().unwrap();
    assert_eq!(frozen.value_count(), 1);
    assert_eq!(frozen.cell(0).unwrap().get_bytes().unwrap(), &[42]);
    drop((input, frozen));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn immutable_factory_requires_identity_and_concrete_source_fixed_provenance() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(1), false, &memory);
    output.write_bytes(&[42, 9]).unwrap();
    let frozen = output.finish().unwrap();
    assert!(SourceScalarReader::new(&frozen).unwrap().is_some());
    assert!(SourceScalarReader::new(&frozen.clone()).unwrap().is_some());
    let range = frozen.select(&Selection::Range { offset: 0, length: 1 }).unwrap();
    let indices = frozen.select(&Selection::Indices(vec![0])).unwrap();
    assert!(matches!(&range.view, ReaderView::Direct(_)));
    assert!(matches!(&indices.view, ReaderView::Direct(_)));
    assert!(SourceScalarReader::new(&range).unwrap().is_none());
    assert!(SourceScalarReader::new(&indices).unwrap().is_none());
    let malformed = fixture(descriptor(SourceScalarDeclaration::FixedBinary(1), false, &memory), 0, NodeLayout::Scalar(NativeScalarStorage { validity: None, data: NativeScalarData::Null }), &memory);
    assert!(SourceScalarReader::new(&malformed).is_err());
    let leaf_array: ArrayRef = Arc::new(Int32Array::from(vec![7]));
    let leaf = fixture(descriptor(SourceScalarDeclaration::FixedBinary(1), false, &memory), 1, NodeLayout::Leaf { array: leaf_array, payload: None, payload_offset: 0 }, &memory);
    assert!(SourceScalarReader::new(&leaf).unwrap().is_none());
    drop((frozen, range, indices, malformed, leaf));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn container_run_and_named_leg_topologies_are_factory_absence_not_source_failures() {
    let memory = memory();
    let child = writer(SourceScalarDeclaration::FixedBinary(1), false, &memory).finish().unwrap().into_direct().unwrap();
    let fields = [StructFieldRef { key: "key", descriptor: &child.descriptor }];
    let struct_descriptor = VectorDescriptor::native_container("struct", false, ContainerShapeRef::Struct(&fields), &memory).unwrap();
    let parent = fixture(struct_descriptor, 0, NodeLayout::Struct { validity: None, children: vec![Arc::clone(&child)] }, &memory);
    assert!(SourceScalarReader::new(&parent).unwrap().is_none());
    let children = [Arc::clone(&child.descriptor)];
    let union_descriptor = VectorDescriptor::ordinal_union("union", &children, &memory).unwrap();
    let union = fixture(union_descriptor, 1, NodeLayout::Union { type_ids: retained(&[0], &memory), offsets: retained(&0i32.to_le_bytes(), &memory), children: vec![Arc::clone(&child)] }, &memory);
    let named = union.vector_for("scalar").unwrap().unwrap();
    assert!(SourceScalarReader::new(&union).unwrap().is_none());
    assert!(SourceScalarReader::new(&named).unwrap().is_none());
    let ends = ScalarWriter::new(Arc::new(Field::new("ends", DataType::Int32, false)), Arc::clone(&memory)).unwrap().finish().unwrap().into_direct().unwrap();
    let run_descriptor = VectorDescriptor::native_container("run", false, ContainerShapeRef::RunEnd { ends: &ends.descriptor, values: &child.descriptor }, &memory).unwrap();
    let run = fixture(run_descriptor, 0, NodeLayout::RunEnd(RunEndReader::new(RunStorage { logical_offset: 0, ends, values: Arc::clone(&child) })), &memory);
    assert!(SourceScalarReader::new(&run).unwrap().is_none());
    drop((parent, union, named, run, children, child));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn snapshots_share_actual_declaration_and_payload_leases_after_original_drop() {
    let source = memory();
    let target = memory();
    let mut output = writer(SourceScalarDeclaration::Uuid { inner_width: 8 }, false, &source);
    output.write_bytes(&[42; 16]).unwrap();
    let frozen = output.finish().unwrap();
    let snapshot = frozen.snapshot_reader(&target).unwrap();
    assert!(Arc::ptr_eq(&frozen.node().descriptor, &snapshot.node().descriptor));
    assert_eq!(declaration(&snapshot), SourceScalarDeclaration::Uuid { inner_width: 8 });
    assert_eq!(snapshot.descriptor().scalar_field().unwrap().data_type(), &DataType::FixedSizeBinary(16));
    assert_eq!(SourceScalarReader::new(&snapshot).unwrap().unwrap().get_bytes(0).unwrap().demand_native().unwrap(), &[42; 8]);
    drop(frozen);
    assert!(source.used_bytes() > 0);
    assert!(target.used_bytes() > 0);
    assert_eq!(snapshot.cell(0).unwrap().get_bytes().unwrap(), &[42; 8]);
    assert_eq!(data(&snapshot).append, 16);
    drop(snapshot);
    assert_eq!(source.used_bytes(), 0);
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn source_scalar_roots_and_children_capture_their_typed_declaration_without_sdk_decode() {
    let source = memory();
    for declaration in declarations() {
        let input = descriptor(declaration, false, &source);
        let target = memory();
        let mut output = VectorWriter::from_descriptor(&input, Arc::clone(&target)).unwrap();
        output.write_bytes(&[42]).unwrap();
        let frozen = output.finish().unwrap();
        assert_eq!(declaration_of(&frozen), declaration);
        assert_eq!(frozen.value_count(), 1);
        assert_eq!(data(&frozen).append, 1);
        assert!(SourceScalarReader::new(&frozen).unwrap().is_some());
        drop(frozen);
        assert_eq!(target.used_bytes(), 0);
    }
    let child = descriptor(SourceScalarDeclaration::Time32(TimeUnit::Microsecond), false, &source);
    let fields = [StructFieldRef { key: "lookup", descriptor: &child }];
    let parent = VectorDescriptor::native_container("parent", false, ContainerShapeRef::Struct(&fields), &source).unwrap();
    let target = memory();
    let mut output = VectorWriter::from_descriptor(&parent, Arc::clone(&target)).unwrap();
    output.end_struct().unwrap();
    let frozen = output.finish().unwrap();
    let value = frozen.vector_for("lookup").unwrap().unwrap();
    assert_eq!(declaration(&value), SourceScalarDeclaration::Time32(TimeUnit::Microsecond));
    assert_eq!(value.value_count(), 1);
    assert!(value.reader_nullable());
    assert!(value.cell(0).unwrap().is_null().unwrap());
    assert_eq!(value.cell(0).unwrap().get_int(NullChecks::Disabled).unwrap(), 0);
    assert!(SourceScalarReader::new(&value).unwrap().is_some());
    drop((frozen, value));
    assert_eq!(target.used_bytes(), 0);
    drop((parent, child));
    assert_eq!(source.used_bytes(), 0);
}

#[test]
fn typed_capture_rejects_validity_mismatch_and_releases_every_consumed_input() {
    let memory = memory();
    let metadata = SourceScalarMetadata::new("capture", true, SourceScalarDeclaration::FixedBinary(-1), &memory).unwrap();
    let bytes = StoredBytes { bytes: retained(&[42], &memory), append: 1 };
    assert!(super::super::open::finish_source_fixed(metadata, SourceCount::new(1), None, bytes).is_err());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sdk_slice_copy_to_a_source_time_writer_retains_native_logical_row_failure() {
    let memory = memory();
    let logical = LogicalType::Scalar(ScalarType::Time { unit: TimeUnit::Second, bit_width: 32 });
    let values = [10, 20, 30].map(|value| Value::Time { value, unit: TimeUnit::Second, bit_width: 32 });
    let array = crate::copy_array(&logical, &values).unwrap().slice(1, 1);
    let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
    let input = ExecutionVector::with_reservation(Arc::new(crate::to_field("sdk", &logical).unwrap()), array, reservation).unwrap();
    let mut output = writer(SourceScalarDeclaration::Time32(TimeUnit::Second), false, &memory);
    assert!(matches!(ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap().copy_row(1_073_741_824), Err(WriteError::Native(_))));
    assert_eq!(output.value_count(), 0);
    assert_eq!(output.source_reader().unwrap().unwrap().get_pointer(0).demand_native().unwrap(), &[0; 4]);
    assert!(matches!(output.write_int(1), Err(WriteError::Native(_))));
    output.clear();
    ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap().copy_row(0).unwrap();
    let frozen = output.finish().unwrap();
    assert_eq!(frozen.value_count(), 1);
    assert_eq!(frozen.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 20);
    drop((input, frozen));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn run_end_negative_scalar_failure_keeps_nullable_value_prefix_without_a_run() {
    let memory = memory();
    let ends = ScalarWriter::new(Arc::new(Field::new("ends", DataType::Int32, false)), Arc::clone(&memory)).unwrap().finish().unwrap();
    let values = descriptor(SourceScalarDeclaration::FixedBinary(-1), false, &memory);
    let input = VectorDescriptor::native_container("run", false, ContainerShapeRef::RunEnd { ends: &ends.node().descriptor, values: &values }, &memory).unwrap();
    let mut output = VectorWriter::from_descriptor(&input, Arc::clone(&memory)).unwrap();
    reference(output.write_null(), ReferenceFailure::Bounds);
    assert_eq!(output.value_count(), 0);
    assert!(output.nullable());
    let captured = output.finish().unwrap();
    let NodeLayout::RunEnd(run) = &captured.node().storage else { panic!() };
    assert_eq!(run.ends().count.get(), 0);
    assert_eq!(run.values().count.get(), 0);
    let child = ExecutionVector::direct(Arc::clone(run.values()));
    assert_eq!(declaration(&child), SourceScalarDeclaration::FixedBinary(-1));
    assert!(child.reader_nullable());
    assert_eq!(storage(&child).validity.as_ref().unwrap().append, 1);
    assert_eq!(data(&child).append, 0);
    read_reference(SourceScalarReader::new(&child).unwrap().unwrap().get_bytes(0), SourceScalarReadFailure::NegativeViewLength(-1));
    drop((captured, child, input, values, ends));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn binary_objects_copy_complete_bytes_from_mutable_and_immutable_readers() {
    let source = memory();
    let destination = memory();
    for width in [0, 1, 16, 32] {
        let initial: Vec<u8> = (0..width).map(|byte| byte as u8).collect();
        let replacement = vec![91; width as usize];
        let mut output = writer(SourceScalarDeclaration::FixedBinary(width), false, &source);
        output.write_bytes(&initial).unwrap();
        let first = output.source_reader().unwrap().unwrap().binary_object0(0, &destination).unwrap().unwrap();
        output.clear();
        output.write_bytes(&replacement).unwrap();
        assert_eq!(first.value(), &Value::FixedSizeBinary(initial));
        let captured = output.finish().unwrap();
        let second = SourceScalarReader::new(&captured).unwrap().unwrap().binary_object0(0, &destination).unwrap().unwrap();
        drop(captured);
        assert_eq!(source.used_bytes(), 0);
        assert_eq!(second.value(), &Value::FixedSizeBinary(replacement));
        assert_eq!(destination.used_bytes(), 2 * (width as usize * 4 + 4096));
        drop((first, second));
        assert_eq!(destination.used_bytes(), 0);
    }
}

#[test]
fn binary_object_negative_view_failure_precedes_blocked_destination_admission() {
    let source = memory();
    let destination = MemoryBudget::new(0);
    for width in [i32::MIN, -1] {
        for declaration in [SourceScalarDeclaration::FixedBinary(width), SourceScalarDeclaration::Uuid { inner_width: width }] {
            let captured = writer(declaration, false, &source).finish().unwrap();
            let before = prefix(&captured);
            let reader = SourceScalarReader::new(&captured).unwrap().unwrap();
            for row in [i32::MIN, -1, 0, 1, i32::MAX] {
                read_reference(reader.binary_object0(row, &destination).unwrap(), SourceScalarReadFailure::NegativeViewLength(i64::from(width)));
            }
            assert_eq!(prefix(&captured), before);
            assert_eq!(destination.used_bytes(), 0);
            assert_eq!(destination.peak_bytes(), 0);
        }
    }
    assert_eq!(source.used_bytes(), 0);
}

#[test]
fn binary_object_native_span_admission_precedes_copy_reservation() {
    let source = memory();
    let destination = MemoryBudget::new(0);
    for width in [1, i32::MAX] {
        for declaration in [SourceScalarDeclaration::FixedBinary(width), SourceScalarDeclaration::Uuid { inner_width: width }] {
            let captured = writer(declaration, false, &source).finish().unwrap();
            let before = prefix(&captured);
            let reader = SourceScalarReader::new(&captured).unwrap().unwrap();
            for row in [i32::MIN, -1, 0, 1, i32::MAX] {
                assert_eq!(reader.get_pointer(row).offset(), i64::from(row.wrapping_mul(width)));
                read_native(reader.binary_object0(row, &destination).unwrap());
            }
            assert_eq!(prefix(&captured), before);
            assert_eq!(destination.peak_bytes(), 0);
        }
    }
    assert_eq!(source.used_bytes(), 0);
}

#[test]
fn binary_objects_use_wrapped_signed_rows_without_checked_cell_admission() {
    let source = memory();
    let destination = memory();
    for declaration in [SourceScalarDeclaration::FixedBinary(16), SourceScalarDeclaration::Uuid { inner_width: 16 }] {
        let mut output = writer(declaration, false, &source);
        output.write_bytes(&[37; 16]).unwrap();
        let captured = output.finish().unwrap();
        let reader = SourceScalarReader::new(&captured).unwrap().unwrap();
        for row in [i32::MIN, 0, 268_435_456] {
            let object = reader.binary_object0(row, &destination).unwrap().unwrap();
            match declaration {
                SourceScalarDeclaration::FixedBinary(_) => assert_eq!(object.value(), &Value::FixedSizeBinary(vec![37; 16])),
                SourceScalarDeclaration::Uuid { .. } => assert_eq!(object.value(), &Value::Uuid([37; 16])),
                _ => panic!(),
            }
        }
        assert!(captured.cell(268_435_456).is_err());
        read_native(reader.binary_object0(i32::MAX, &destination).unwrap());
    }
    assert_eq!(destination.used_bytes(), 0);
    assert_eq!(source.used_bytes(), 0);
}

#[test]
fn uuid_object_underflow_identifies_each_relative_long_after_full_copy_admission() {
    let source = memory();
    for (width, expected) in [
        (0, SourceScalarReadFailure::UuidFirstLongUnderflow),
        (7, SourceScalarReadFailure::UuidFirstLongUnderflow),
        (8, SourceScalarReadFailure::UuidSecondLongUnderflow),
        (15, SourceScalarReadFailure::UuidSecondLongUnderflow),
    ] {
        let copy_bound = width as usize * 4 + 4096;
        let destination = MemoryBudget::new(copy_bound);
        let mut output = writer(SourceScalarDeclaration::Uuid { inner_width: width }, false, &source);
        output.write_bytes(&[27; 16]).unwrap();
        read_reference(output.source_reader().unwrap().unwrap().binary_object0(0, &destination).unwrap(), expected);
        assert_eq!(destination.used_bytes(), 0);
        assert_eq!(destination.peak_bytes(), copy_bound);
        let captured = output.finish().unwrap();
        let before = prefix(&captured);
        read_reference(SourceScalarReader::new(&captured).unwrap().unwrap().binary_object0(0, &destination).unwrap(), expected);
        assert_eq!(prefix(&captured), before);
        assert_eq!(destination.used_bytes(), 0);
    }
    assert_eq!(source.used_bytes(), 0);
}

#[test]
fn uuid_object_surplus_bytes_keep_full_copy_peak_and_only_inline_result_retention() {
    let source = memory();
    for width in [16, 32] {
        let bytes: Vec<u8> = (0..width).map(|byte| byte as u8).collect();
        let copy_bound = width as usize * 4 + 4096;
        let destination = MemoryBudget::new(copy_bound + 4096);
        let mut output = writer(SourceScalarDeclaration::Uuid { inner_width: width }, false, &source);
        output.write_bytes(&bytes).unwrap();
        let captured = output.finish().unwrap();
        let before = prefix(&captured);
        let object = SourceScalarReader::new(&captured).unwrap().unwrap().binary_object0(0, &destination).unwrap().unwrap();
        assert_eq!(object.value(), &Value::Uuid([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]));
        assert_eq!(destination.peak_bytes(), copy_bound + 4096);
        assert_eq!(destination.used_bytes(), 4096);
        assert_eq!(prefix(&captured), before);
        assert_eq!(declaration(&captured), SourceScalarDeclaration::Uuid { inner_width: width });
        assert_eq!(captured.descriptor().scalar_field().unwrap().data_type(), &DataType::FixedSizeBinary(16));
        assert_eq!(captured.cell(0).unwrap().object().is_ok(), width == 16);
        drop(captured);
        assert_eq!(source.used_bytes(), 0);
        assert_eq!(destination.used_bytes(), 4096);
        drop(object);
        assert_eq!(destination.used_bytes(), 0);
    }
}

#[test]
fn uuid_object_demands_unretained_surplus_even_when_first_sixteen_bytes_exist() {
    let source = memory();
    let destination = MemoryBudget::new(0);
    let mut output = writer(SourceScalarDeclaration::Uuid { inner_width: 129 }, false, &source);
    output.write_bytes(&[71; 16]).unwrap();
    let captured = output.finish().unwrap();
    assert_eq!(&data(&captured).bytes.as_slice()[..16], &[71; 16]);
    assert_eq!(data(&captured).bytes.len(), 128);
    let before = prefix(&captured);
    read_native(SourceScalarReader::new(&captured).unwrap().unwrap().binary_object0(0, &destination).unwrap());
    assert_eq!(destination.peak_bytes(), 0);
    assert_eq!(prefix(&captured), before);
    drop(captured);
    assert_eq!(source.used_bytes(), 0);
}

#[test]
fn binary_object0_reads_null_payload_while_checked_objects_keep_their_guard() {
    let source = memory();
    let destination = memory();
    for declaration in [SourceScalarDeclaration::FixedBinary(16), SourceScalarDeclaration::Uuid { inner_width: 16 }] {
        let mut output = writer(declaration, true, &source);
        output.write_undefined().unwrap();
        let captured = output.finish().unwrap();
        let before = prefix(&captured);
        assert!(captured.cell(0).unwrap().is_null().unwrap());
        assert_eq!(captured.cell(0).unwrap().object().unwrap().value(), &Value::Null);
        let object = SourceScalarReader::new(&captured).unwrap().unwrap().binary_object0(0, &destination).unwrap().unwrap();
        match declaration {
            SourceScalarDeclaration::FixedBinary(_) => assert_eq!(object.value(), &Value::FixedSizeBinary(vec![0; 16])),
            SourceScalarDeclaration::Uuid { .. } => assert_eq!(object.value(), &Value::Uuid([0; 16])),
            _ => panic!(),
        }
        assert_eq!(prefix(&captured), before);
    }
    assert_eq!(source.used_bytes(), 0);
    assert_eq!(destination.used_bytes(), 0);
}

#[test]
fn binary_object_copy_reservation_is_admitted_before_allocating_owned_bytes() {
    let source = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(32), false, &source);
    output.write_bytes(&[64; 32]).unwrap();
    let captured = output.finish().unwrap();
    let before = prefix(&captured);
    let denied = MemoryBudget::new(4223);
    assert!(matches!(SourceScalarReader::new(&captured).unwrap().unwrap().binary_object0(0, &denied).unwrap(), Err(SourceScalarReadError::Native(error)) if error.code == "rtdb/memory-budget-exceeded"));
    assert_eq!(denied.used_bytes(), 0);
    assert_eq!(denied.peak_bytes(), 0);
    let admitted = MemoryBudget::new(4224);
    let object = SourceScalarReader::new(&captured).unwrap().unwrap().binary_object0(0, &admitted).unwrap().unwrap();
    assert_eq!(object.value(), &Value::FixedSizeBinary(vec![64; 32]));
    assert_eq!(admitted.used_bytes(), 4224);
    assert_eq!(admitted.peak_bytes(), 4224);
    assert_eq!(prefix(&captured), before);
    drop((captured, object));
    assert_eq!(source.used_bytes(), 0);
    assert_eq!(admitted.used_bytes(), 0);
}

#[test]
fn uuid_result_denial_releases_the_admitted_full_copy_without_changing_source() {
    let source = memory();
    let destination = MemoryBudget::new(8319);
    let mut output = writer(SourceScalarDeclaration::Uuid { inner_width: 32 }, false, &source);
    output.write_bytes(&[82; 32]).unwrap();
    let captured = output.finish().unwrap();
    let before = prefix(&captured);
    assert!(matches!(SourceScalarReader::new(&captured).unwrap().unwrap().binary_object0(0, &destination).unwrap(), Err(SourceScalarReadError::Native(error)) if error.code == "rtdb/memory-budget-exceeded"));
    assert_eq!(destination.peak_bytes(), 4224);
    assert_eq!(destination.used_bytes(), 0);
    assert_eq!(prefix(&captured), before);
    drop(captured);
    assert_eq!(source.used_bytes(), 0);
}

#[test]
fn binary_object_read_preserves_aborted_writer_gate_after_success_and_denial() {
    let source = memory();
    let destination = memory();
    let denied = MemoryBudget::new(0);
    let mut output = writer(SourceScalarDeclaration::FixedBinary(128), true, &source);
    output.write_bytes(&[42; 128]).unwrap();
    let blocker = block(&source, 0);
    assert!(matches!(output.write_undefined(), Err(WriteError::Native(_))));
    let object = output.source_reader().unwrap().unwrap().binary_object0(0, &destination).unwrap().unwrap();
    assert_eq!(object.value(), &Value::FixedSizeBinary(vec![42; 128]));
    assert_eq!(output.value_count(), 1);
    assert!(matches!(output.source_reader().unwrap().unwrap().binary_object0(0, &denied).unwrap(), Err(SourceScalarReadError::Native(error)) if error.code == "rtdb/memory-budget-exceeded"));
    read_native(output.source_reader().unwrap().unwrap().binary_object0(1, &destination).unwrap());
    assert_eq!(output.reader().get_bytes(0).unwrap(), &[42; 128]);
    assert!(matches!(output.write_bytes(&[]), Err(WriteError::Native(error)) if error.code == "rtdb/aborted-vector-writer"));
    assert!(output.finish().is_err());
    assert_eq!(source.used_bytes(), blocker.bytes());
    drop(blocker);
    assert_eq!(source.used_bytes(), 0);
    assert_eq!(destination.used_bytes(), 4608);
    drop(object);
    assert_eq!(destination.used_bytes(), 0);
}

#[test]
fn uuid_underflow_preserves_aborted_zero_count_payload_and_releases_copy() {
    let source = memory();
    let destination = MemoryBudget::new(4128);
    let mut output = writer(SourceScalarDeclaration::Uuid { inner_width: 8 }, true, &source);
    let blocker = block(&source, 256);
    assert!(matches!(output.write_bytes(&[42; 16]), Err(WriteError::Native(_))));
    assert_eq!(output.value_count(), 0);
    read_reference(output.source_reader().unwrap().unwrap().binary_object0(0, &destination).unwrap(), SourceScalarReadFailure::UuidSecondLongUnderflow);
    assert_eq!(destination.peak_bytes(), 4128);
    assert_eq!(destination.used_bytes(), 0);
    assert_eq!(output.source_reader().unwrap().unwrap().get_bytes(0).unwrap().demand_native().unwrap(), &[42; 8]);
    assert!(matches!(output.write_bytes(&[]), Err(WriteError::Native(error)) if error.code == "rtdb/aborted-vector-writer"));
    assert!(output.finish().is_err());
    assert_eq!(source.used_bytes(), blocker.bytes());
    drop(blocker);
    assert_eq!(source.used_bytes(), 0);
}

#[test]
fn binary_object_result_and_snapshot_keep_independent_owner_budgets() {
    let source = memory();
    let snapshots = memory();
    let destination = memory();
    for declaration in [SourceScalarDeclaration::FixedBinary(32), SourceScalarDeclaration::Uuid { inner_width: 32 }] {
        let mut output = writer(declaration, false, &source);
        output.write_bytes(&[63; 32]).unwrap();
        let captured = output.finish().unwrap();
        let snapshot = captured.snapshot_reader(&snapshots).unwrap();
        let object = SourceScalarReader::new(&snapshot).unwrap().unwrap().binary_object0(0, &destination).unwrap().unwrap();
        drop(captured);
        assert!(source.used_bytes() > 0);
        assert!(snapshots.used_bytes() > 0);
        drop(snapshot);
        assert_eq!(source.used_bytes(), 0);
        assert_eq!(snapshots.used_bytes(), 0);
        match declaration {
            SourceScalarDeclaration::FixedBinary(_) => {
                assert_eq!(object.value(), &Value::FixedSizeBinary(vec![63; 32]));
                assert_eq!(destination.used_bytes(), 4224);
            }
            SourceScalarDeclaration::Uuid { .. } => {
                assert_eq!(object.value(), &Value::Uuid([63; 16]));
                assert_eq!(destination.used_bytes(), 4096);
            }
            _ => panic!(),
        }
        drop(object);
        assert_eq!(destination.used_bytes(), 0);
    }
}

#[test]
fn uuid_copy_and_result_overlap_respects_ancestor_limits_and_retains_budget_owners() {
    let source = memory();
    let root = MemoryBudget::new(8320);
    let destination = root.scope();
    let sibling = root.scope();
    let blocker = sibling.reserve(1).unwrap();
    let mut output = writer(SourceScalarDeclaration::Uuid { inner_width: 32 }, false, &source);
    output.write_bytes(&[29; 32]).unwrap();
    let captured = output.finish().unwrap();
    assert!(matches!(SourceScalarReader::new(&captured).unwrap().unwrap().binary_object0(0, &destination).unwrap(), Err(SourceScalarReadError::Native(error)) if error.code == "rtdb/memory-budget-exceeded"));
    assert_eq!(destination.used_bytes(), 0);
    assert_eq!(destination.peak_bytes(), 4224);
    assert_eq!(root.used_bytes(), 1);
    assert_eq!(root.peak_bytes(), 4225);
    drop((blocker, sibling));
    let object = SourceScalarReader::new(&captured).unwrap().unwrap().binary_object0(0, &destination).unwrap().unwrap();
    assert_eq!(object.value(), &Value::Uuid([29; 16]));
    assert_eq!(destination.peak_bytes(), 8320);
    assert_eq!(root.peak_bytes(), 8320);
    assert_eq!(destination.used_bytes(), 4096);
    assert_eq!(root.used_bytes(), 4096);
    let weak_root = Arc::downgrade(&root);
    let weak_destination = Arc::downgrade(&destination);
    drop((captured, root, destination));
    assert_eq!(source.used_bytes(), 0);
    assert_eq!(weak_root.upgrade().unwrap().used_bytes(), 4096);
    assert_eq!(weak_destination.upgrade().unwrap().used_bytes(), 4096);
    drop(object);
    assert!(weak_root.upgrade().is_none());
    assert!(weak_destination.upgrade().is_none());
}

#[test]
fn binary_object_capability_is_absent_for_every_time_declaration_before_admission() {
    let source = memory();
    let destination = MemoryBudget::new(0);
    for unit in [TimeUnit::Second, TimeUnit::Millisecond, TimeUnit::Microsecond, TimeUnit::Nanosecond] {
        for declaration in [SourceScalarDeclaration::Time32(unit), SourceScalarDeclaration::Time64(unit)] {
            let output = writer(declaration, true, &source);
            for row in [i32::MIN, -1, 0, 1, i32::MAX] {
                assert!(output.source_reader().unwrap().unwrap().binary_object0(row, &destination).is_none());
            }
            let captured = output.finish().unwrap();
            assert!(SourceScalarReader::new(&captured).unwrap().unwrap().binary_object0(0, &destination).is_none());
        }
    }
    assert_eq!(source.used_bytes(), 0);
    assert_eq!(destination.used_bytes(), 0);
    assert_eq!(destination.peak_bytes(), 0);
}

#[test]
fn uuid_copy_reservation_denial_precedes_both_long_underflow_producers() {
    let source = memory();
    for width in [0, 7, 8, 15] {
        let destination = MemoryBudget::new(width as usize * 4 + 4095);
        let mut output = writer(SourceScalarDeclaration::Uuid { inner_width: width }, false, &source);
        output.write_bytes(&[42; 16]).unwrap();
        let captured = output.finish().unwrap();
        let before = prefix(&captured);
        assert!(matches!(SourceScalarReader::new(&captured).unwrap().unwrap().binary_object0(0, &destination).unwrap(), Err(SourceScalarReadError::Native(error)) if error.code == "rtdb/memory-budget-exceeded"));
        assert_eq!(destination.used_bytes(), 0);
        assert_eq!(destination.peak_bytes(), 0);
        assert_eq!(prefix(&captured), before);
    }
    assert_eq!(source.used_bytes(), 0);
}

#[test]
fn signed_slice_bytes_feed_existing_scalar_writers() {
    let source = memory();
    let destination = memory();
    for declaration in [SourceScalarDeclaration::FixedBinary(2), SourceScalarDeclaration::Uuid { inner_width: 2 }] {
        let mut input = writer(declaration, false, &source);
        input.write_bytes(&[11, 12]).unwrap();
        input.write_bytes(&[21, 22]).unwrap();
        input.write_bytes(&[31, 32]).unwrap();
        let slice = input.source_reader().unwrap().unwrap().slice(1, 1);
        let mut output = writer(declaration, false, &destination);
        output.write_bytes(slice.get_bytes(0).unwrap().demand_native().unwrap()).unwrap();
        output.write_bytes(slice.get_pointer(1).demand_native().unwrap()).unwrap();
        assert_eq!(slice.value_count(), 1);
        assert_eq!(input.value_count(), 3);
        let captured = output.finish().unwrap();
        assert_eq!(captured.value_count(), 2);
        assert_eq!(data(&captured).append, 4);
        assert_eq!(&data(&captured).bytes.as_slice()[..4], &[21, 22, 31, 32]);
        assert_eq!(SourceScalarReader::new(&captured).unwrap().unwrap().slice(1, -1).get_bytes(0).unwrap().demand_native().unwrap(), &[31, 32]);
    }
    assert_eq!(source.used_bytes(), 0);
    assert_eq!(destination.used_bytes(), 0);
}

#[test]
fn signed_slice_length_is_independent_of_raw_reads_and_logical_count() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(1), false, &memory);
    output.write_bytes(&[42]).unwrap();
    for length in [i32::MIN, -1, 0, 1, i32::MAX] {
        let slice = output.source_reader().unwrap().unwrap().slice(0, length);
        assert_eq!(slice.value_count(), length);
        assert_eq!(slice.get_bytes(0).unwrap().demand_native().unwrap(), &[42]);
        assert_eq!(slice.get_pointer(127).demand_native().unwrap(), &[0]);
        let view = slice.get_bytes(128).unwrap();
        assert_eq!(view.offset(), 128);
        assert_eq!(view.len(), 1);
        read_native(view.demand_native());
    }
    assert_eq!(output.value_count(), 1);
    assert!(output.reader().get_bytes(1).is_err());
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn signed_slice_addition_wraps_before_the_inner_width_product() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(16), false, &memory);
    output.write_bytes(&[42; 16]).unwrap();
    for (start, row, offset) in [
        (i32::MAX, 1, 0), (i32::MIN, i32::MIN, 0), (-1, 1, 0),
        (1, -1, 0), (i32::MIN, 0, 0), (0, i32::MIN, 0),
        (1, i32::MAX, 0), (i32::MAX, 0, -16), (0, i32::MAX, -16),
        (i32::MIN, -1, -16), (-1, 2, 16),
    ] {
        let slice = output.source_reader().unwrap().unwrap().slice(start, -1);
        let pointer = slice.get_pointer(row);
        let view = slice.get_bytes(row).unwrap();
        assert_eq!(pointer.offset(), offset);
        assert_eq!(pointer.length(), 16);
        assert_eq!(view.offset(), offset);
        assert_eq!(view.len(), 16);
        match offset {
            0 => {
                assert_eq!(pointer.demand_native().unwrap(), &[42; 16]);
                assert_eq!(view.demand_native().unwrap(), &[42; 16]);
            }
            16 => {
                assert_eq!(pointer.demand_native().unwrap(), &[0; 16]);
                assert_eq!(view.demand_native().unwrap(), &[0; 16]);
            }
            -16 => { read_native(pointer.demand_native()); read_native(view.demand_native()); }
            _ => panic!(),
        }
    }
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn signed_slice_negative_width_fails_before_payload_demand() {
    let memory = memory();
    for width in [i32::MIN, -1] {
        for declaration in [SourceScalarDeclaration::FixedBinary(width), SourceScalarDeclaration::Uuid { inner_width: width }] {
            let output = writer(declaration, false, &memory);
            let blocker = block(&memory, 0);
            for (start, row) in [(0, 0), (i32::MAX, 1), (i32::MIN, -1), (-1, 0)] {
                let slice = output.source_reader().unwrap().unwrap().slice(start, i32::MIN);
                let pointer = slice.get_pointer(row);
                assert_eq!(pointer.length(), i64::from(width));
                read_reference(slice.get_bytes(row), SourceScalarReadFailure::NegativeViewLength(i64::from(width)));
                read_native(pointer.demand_native());
                assert_eq!(memory.used_bytes(), memory.limit_bytes());
            }
            assert_eq!(output.value_count(), 0);
            drop(blocker);
        }
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn signed_slice_zero_width_demand_accepts_empty_backing_at_every_signed_index() {
    let memory = memory();
    for declaration in [SourceScalarDeclaration::FixedBinary(0), SourceScalarDeclaration::Uuid { inner_width: 0 }] {
        let output = writer(declaration, false, &memory);
        let blocker = block(&memory, 0);
        for start in [i32::MIN, -1, 0, 1, i32::MAX] {
            for length in [i32::MIN, -1, 0, 1, i32::MAX] {
                let slice = output.source_reader().unwrap().unwrap().slice(start, length);
                assert_eq!(slice.value_count(), length);
                for row in [i32::MIN, -1, 0, 1, i32::MAX] {
                    let pointer = slice.get_pointer(row);
                    let view = slice.get_bytes(row).unwrap();
                    assert_eq!(pointer.offset(), 0);
                    assert_eq!(pointer.length(), 0);
                    assert!(pointer.demand_native().unwrap().is_empty());
                    assert_eq!(view.offset(), 0);
                    assert!(view.is_empty());
                    assert!(view.demand_native().unwrap().is_empty());
                }
            }
        }
        assert_eq!(memory.used_bytes(), memory.limit_bytes());
        drop(blocker);
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn signed_slice_uses_actual_binary_and_uuid_inner_widths() {
    let memory = memory();
    for width in [1, 8, 16, 24, i32::MAX] {
        for declaration in [SourceScalarDeclaration::FixedBinary(width), SourceScalarDeclaration::Uuid { inner_width: width }] {
            let mut output = writer(declaration, false, &memory);
            output.write_bytes(&[63; 24]).unwrap();
            let captured = output.finish().unwrap();
            let before = prefix(&captured);
            let slice = SourceScalarReader::new(&captured).unwrap().unwrap().slice(-1, 0);
            let pointer = slice.get_pointer(1);
            let view = slice.get_bytes(1).unwrap();
            assert_eq!(pointer.offset(), 0);
            assert_eq!(pointer.length(), i64::from(width));
            assert_eq!(view.offset(), 0);
            assert_eq!(view.len(), width as usize);
            if width == i32::MAX {
                read_native(pointer.demand_native());
                read_native(view.demand_native());
            } else {
                assert_eq!(pointer.demand_native().unwrap(), &[63; 24][..width as usize]);
                assert_eq!(view.demand_native().unwrap(), &[63; 24][..width as usize]);
            }
            if matches!(declaration, SourceScalarDeclaration::Uuid { .. }) {
                assert_eq!(captured.descriptor().scalar_field().unwrap().data_type(), &DataType::FixedSizeBinary(16));
            }
            assert_eq!(prefix(&captured), before);
        }
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn signed_slice_time_bytes_remain_unsupported_before_span_admission() {
    let memory = memory();
    for unit in [TimeUnit::Second, TimeUnit::Millisecond, TimeUnit::Microsecond, TimeUnit::Nanosecond] {
        for declaration in [SourceScalarDeclaration::Time32(unit), SourceScalarDeclaration::Time64(unit)] {
            let mut output = writer(declaration, false, &memory);
            let expected: &[u8] = match declaration {
                SourceScalarDeclaration::Time32(_) => { output.write_int(42).unwrap(); &[42, 0, 0, 0] }
                SourceScalarDeclaration::Time64(_) => { output.write_long(42).unwrap(); &[42, 0, 0, 0, 0, 0, 0, 0] }
                _ => panic!(),
            };
            let blocker = block(&memory, 0);
            let slice = output.source_reader().unwrap().unwrap().slice(-1, -1);
            assert_eq!(slice.get_pointer(1).demand_native().unwrap(), expected);
            assert_eq!(slice.get_pointer(1).length(), expected.len() as i64);
            read_native(slice.get_pointer(0).demand_native());
            for row in [i32::MIN, -1, 0, 1, i32::MAX] {
                read_reference(slice.get_bytes(row), SourceScalarReadFailure::Unsupported { declaration, method: "getBytes" });
            }
            assert_eq!(output.value_count(), 1);
            assert_eq!(memory.used_bytes(), memory.limit_bytes());
            drop(blocker);
        }
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn signed_slice_read_failures_preserve_ready_writer_prefix_and_gate() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(1), true, &memory);
    output.write_bytes(&[42]).unwrap();
    let slice = output.source_reader().unwrap().unwrap().slice(128, i32::MIN);
    let view = slice.get_bytes(0).unwrap();
    assert_eq!(view.offset(), 128);
    read_native(view.demand_native());
    read_native(slice.get_pointer(i32::MIN).demand_native());
    assert_eq!(output.value_count(), 1);
    assert_eq!(slice.get_bytes(-128).unwrap().demand_native().unwrap(), &[42]);
    output.write_bytes(&[9]).unwrap();
    let captured = output.finish().unwrap();
    assert_eq!(captured.value_count(), 2);
    assert_eq!(data(&captured).append, 2);
    assert_eq!(&data(&captured).bytes.as_slice()[..2], &[42, 9]);
    drop(captured);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn signed_slice_and_byte_facts_borrow_the_owner_after_intermediate_values_drop() {
    let source = memory();
    let snapshots = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(1), false, &source);
    output.write_bytes(&[11]).unwrap();
    output.write_bytes(&[42]).unwrap();
    let slice = { let reader = output.source_reader().unwrap().unwrap(); reader.slice(1, i32::MIN) };
    let view = slice.get_bytes(0).unwrap();
    let pointer = slice.get_pointer(0);
    drop(slice);
    let bytes = view.demand_native().unwrap();
    drop(view);
    assert_eq!(bytes, &[42]);
    assert_eq!(pointer.demand_native().unwrap(), &[42]);
    drop(pointer);
    let captured = output.finish().unwrap();
    let snapshot = captured.snapshot_reader(&snapshots).unwrap();
    drop(captured);
    let source_charge = source.used_bytes();
    let snapshot_charge = snapshots.used_bytes();
    assert!(source_charge > 0);
    assert!(snapshot_charge > 0);
    let pointer = {
        let reader = SourceScalarReader::new(&snapshot).unwrap().unwrap();
        let slice = reader.slice(1, -1);
        slice.get_pointer(0)
    };
    assert_eq!(pointer.demand_native().unwrap(), &[42]);
    assert_eq!(source.used_bytes(), source_charge);
    assert_eq!(snapshots.used_bytes(), snapshot_charge);
    drop(pointer);
    drop(snapshot);
    assert_eq!(source.used_bytes(), 0);
    assert_eq!(snapshots.used_bytes(), 0);
}

#[test]
fn signed_slice_forwarding_needs_no_additional_memory_admission() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(1), false, &memory);
    output.write_bytes(&[42]).unwrap();
    let blocker = block(&memory, 0);
    let before_peak = memory.peak_bytes();
    for (start, row) in [(0, 0), (-1, 1), (1, -1), (i32::MIN, i32::MIN)] {
        let slice = output.source_reader().unwrap().unwrap().slice(start, i32::MIN);
        assert_eq!(slice.value_count(), i32::MIN);
        assert_eq!(slice.get_bytes(row).unwrap().demand_native().unwrap(), &[42]);
        assert_eq!(slice.get_pointer(row).demand_native().unwrap(), &[42]);
        assert_eq!(memory.used_bytes(), memory.limit_bytes());
        assert_eq!(memory.peak_bytes(), before_peak);
    }
    drop(output);
    assert_eq!(memory.used_bytes(), blocker.bytes());
    drop(blocker);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn signed_slice_reads_aborted_prefix_without_reopening_publication() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(128), true, &memory);
    output.write_bytes(&[42; 128]).unwrap();
    let blocker = block(&memory, 0);
    assert!(matches!(output.write_undefined(), Err(WriteError::Native(_))));
    let slice = output.source_reader().unwrap().unwrap().slice(-1, i32::MIN);
    assert_eq!(slice.value_count(), i32::MIN);
    assert_eq!(slice.get_bytes(1).unwrap().demand_native().unwrap(), &[42; 128]);
    read_native(slice.get_bytes(2).unwrap().demand_native());
    read_native(slice.get_pointer(0).demand_native());
    assert_eq!(output.value_count(), 1);
    assert!(matches!(output.write_bytes(&[]), Err(WriteError::Native(error)) if error.code == "rtdb/aborted-vector-writer"));
    assert!(output.finish().is_err());
    assert_eq!(memory.used_bytes(), blocker.bytes());
    drop(blocker);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn signed_slice_demands_retained_payload_when_aborted_count_is_zero() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(1), true, &memory);
    let blocker = block(&memory, 256);
    assert!(matches!(output.write_bytes(&[42]), Err(WriteError::Native(_))));
    assert_eq!(output.value_count(), 0);
    let slice = output.source_reader().unwrap().unwrap().slice(1, 0);
    assert_eq!(slice.get_pointer(-1).demand_native().unwrap(), &[42]);
    assert_eq!(slice.get_bytes(-1).unwrap().demand_native().unwrap(), &[42]);
    assert!(output.reader().get_bytes(0).is_err());
    assert!(matches!(output.write_bytes(&[9]), Err(WriteError::Native(error)) if error.code == "rtdb/aborted-vector-writer"));
    assert!(output.finish().is_err());
    assert_eq!(memory.used_bytes(), blocker.bytes());
    drop(blocker);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn guarded_fixed_binary_reads_validity_before_signed_width_or_destination_admission() {
    let source = memory();
    let destination = MemoryBudget::new(0);
    let mut output = writer(SourceScalarDeclaration::FixedBinary(-1), true, &source);
    let empty = output.source_reader().unwrap().unwrap();
    read_reference(empty.binary_object(0, &destination).unwrap(),
        SourceScalarReadFailure::ValidityByteBounds { offset: 0, capacity: 0 });

    output.ensure_capacity(1).unwrap();
    let reader = output.source_reader().unwrap().unwrap();
    assert!(reader.binary_object(0, &destination).unwrap().unwrap().is_none());
    assert!(reader.binary_object(7, &destination).unwrap().unwrap().is_none());
    read_reference(reader.binary_object0(0, &destination).unwrap(), SourceScalarReadFailure::NegativeViewLength(-1));

    let captured = output.finish().unwrap();
    let capacity = storage(&captured).validity.as_ref().unwrap().bytes.as_slice().len();
    assert_eq!(captured.value_count(), 1);
    let reader = SourceScalarReader::new(&captured).unwrap().unwrap();
    read_reference(reader.binary_object(-1, &destination).unwrap(),
        SourceScalarReadFailure::ValidityByteBounds { offset: -1, capacity });
    let after_bitmap = i32::try_from(capacity).unwrap() * 8;
    read_reference(reader.binary_object(after_bitmap, &destination).unwrap(),
        SourceScalarReadFailure::ValidityByteBounds { offset: after_bitmap >> 3, capacity });
    let slice = reader.slice(i32::MAX, 0);
    read_reference(slice.binary_object(1, &destination).unwrap(),
        SourceScalarReadFailure::ValidityByteBounds { offset: i32::MIN >> 3, capacity });
    assert_eq!(destination.peak_bytes(), 0);
    drop(captured);
    assert_eq!(source.used_bytes(), 0);
}

#[test]
fn guarded_uuid_null_does_not_copy_the_payload_consumed_by_unguarded_underflow() {
    let source = memory();
    let destination = memory();
    let mut output = writer(SourceScalarDeclaration::Uuid { inner_width: 7 }, true, &source);
    output.write_null().unwrap();
    let reader = output.source_reader().unwrap().unwrap();
    assert!(reader.binary_object(0, &destination).unwrap().unwrap().is_none());
    assert_eq!(destination.peak_bytes(), 0);
    read_reference(reader.binary_object0(0, &destination).unwrap(), SourceScalarReadFailure::UuidFirstLongUnderflow);
    assert_eq!(destination.peak_bytes(), 4096 + 4 * 7);
    assert_eq!(destination.used_bytes(), 0);
    drop(output);
    assert_eq!(source.used_bytes(), 0);
}

#[test]
fn guarded_slice_uses_the_translated_validity_bit_and_retains_only_the_owned_result() {
    let source = memory();
    let destination = MemoryBudget::new(4096 + 4 * 4);
    let mut output = writer(SourceScalarDeclaration::FixedBinary(4), true, &source);
    for _ in 0..9 { output.write_null().unwrap(); }
    output.write_bytes(&[3, 1, 4, 1]).unwrap();
    let captured = output.finish().unwrap();
    let object = {
        let reader = SourceScalarReader::new(&captured).unwrap().unwrap();
        let slice = reader.slice(10, -1);
        assert!(slice.binary_object(-2, &destination).unwrap().unwrap().is_none());
        slice.binary_object(-1, &destination).unwrap().unwrap().unwrap()
    };
    drop(captured);
    assert_eq!(object.value(), &Value::FixedSizeBinary(vec![3, 1, 4, 1]));
    assert_eq!(source.used_bytes(), 0);
    assert_eq!(destination.used_bytes(), destination.limit_bytes());
    drop(object);
    assert_eq!(destination.used_bytes(), 0);
}

#[test]
fn missing_validity_preserves_signed_payload_addresses_and_time_capability_absence() {
    let source = memory();
    let destination = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(4), false, &source);
    output.write_bytes(&[1, 2, 3, 4]).unwrap();
    let reader = output.source_reader().unwrap().unwrap();
    let object = reader.slice(i32::MAX, 0).binary_object(1, &destination).unwrap().unwrap().unwrap();
    assert_eq!(object.value(), &Value::FixedSizeBinary(vec![1, 2, 3, 4]));
    read_native(reader.binary_object(-1, &destination).unwrap());

    for declaration in [SourceScalarDeclaration::Time32(TimeUnit::Second), SourceScalarDeclaration::Time64(TimeUnit::Nanosecond)] {
        let output = writer(declaration, true, &source);
        let reader = output.source_reader().unwrap().unwrap();
        assert!(reader.binary_object(-1, &destination).is_none());
        assert!(reader.slice(i32::MAX, 0).binary_object(1, &destination).is_none());
    }
    assert_eq!(destination.used_bytes(), 4096 + 4 * 4);
    drop((object, output));
    assert_eq!(destination.used_bytes(), 0);
    assert_eq!(source.used_bytes(), 0);
}

#[test]
fn borrowed_fixed_binary_long_long_uses_its_own_stride_even_at_a_null_row() {
    let memory = memory();
    let high = i64::MIN + 0x1234_5678;
    let low = i64::MAX - 0x7654_3210;
    let mut output = writer(SourceScalarDeclaration::FixedBinary(8), true, &memory);
    output.write_bytes(&11i64.to_be_bytes()).unwrap();
    output.write_null().unwrap();
    output.write_bytes(&high.to_be_bytes()).unwrap();
    output.write_bytes(&low.to_be_bytes()).unwrap();
    {
        let reader = output.source_reader().unwrap().unwrap();
        assert_eq!(reader.get_long_long_high(1).unwrap().unwrap(), high);
        assert_eq!(reader.get_long_long_low(1).unwrap().unwrap(), low);
        assert_eq!(reader.get_pointer(1).offset(), 8);
        assert_eq!(reader.get_bytes(1).unwrap().demand_native().unwrap(), &[0u8; 8]);
    }
    let captured = output.finish().unwrap();
    let cell = captured.cell(1).unwrap();
    assert!(cell.is_null().unwrap());
    let reader = SourceScalarReader::new(&captured).unwrap().unwrap();
    assert_eq!(reader.get_long_long_high(1).unwrap().unwrap(), cell.get_long_long_high().unwrap());
    assert_eq!(reader.get_long_long_low(1).unwrap().unwrap(), cell.get_long_long_low().unwrap());
    drop(captured);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn borrowed_long_long_wraps_the_two_multiplications_after_signed_slice_translation() {
    let memory = memory();
    let high = i64::MIN + 0x1234_5678_9abc;
    let low = i64::MAX - 0x1234_5678_9abc;
    let mut output = writer(SourceScalarDeclaration::FixedBinary(-1), false, &memory);
    output.write_long_long(high, low).unwrap();
    let captured = output.finish().unwrap();
    let reader = SourceScalarReader::new(&captured).unwrap().unwrap();
    for row in [0, 1 << 28] {
        assert_eq!(reader.get_long_long_high(row).unwrap().unwrap(), high);
        assert_eq!(reader.get_long_long_low(row).unwrap().unwrap(), low);
    }
    let slice = reader.slice(i32::MAX, -1);
    assert_eq!(slice.value_count(), -1);
    assert_eq!(slice.get_long_long_high(1).unwrap(), high);
    assert_eq!(slice.get_long_long_low(1).unwrap(), low);
    read_reference(slice.get_bytes(1), SourceScalarReadFailure::NegativeViewLength(-1));
    drop(captured);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn borrowed_long_long_demands_eight_bytes_from_the_retained_capacity() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(0), false, &memory);
    {
        let empty = output.source_reader().unwrap().unwrap();
        read_reference(empty.get_long_long_high(0).unwrap(),
            SourceScalarReadFailure::LongLongLaneBounds { offset: 0, capacity: 0 });
        read_reference(empty.get_long_long_low(0).unwrap(),
            SourceScalarReadFailure::LongLongLaneBounds { offset: 8, capacity: 0 });
    }
    output.write_long_long(5, -8).unwrap();
    let captured = output.finish().unwrap();
    assert_eq!(captured.value_count(), 1);
    assert_eq!(data(&captured).append, 16);
    let capacity = data(&captured).bytes.as_slice().len();
    let first_missing = i32::try_from(capacity / 16).unwrap();
    let last_retained = first_missing - 1;
    assert!(last_retained >= captured.value_count());
    assert!(matches!(captured.cell(last_retained as usize), Err(error) if error.code == "rtdb/row-index"));

    let capped = block(&memory, 0);
    let used = memory.used_bytes();
    let peak = memory.peak_bytes();
    let reader = SourceScalarReader::new(&captured).unwrap().unwrap();
    read_reference(reader.get_long_long_high(-1).unwrap(),
        SourceScalarReadFailure::LongLongLaneBounds { offset: -16, capacity });
    read_reference(reader.get_long_long_low(-1).unwrap(),
        SourceScalarReadFailure::LongLongLaneBounds { offset: -8, capacity });
    assert_eq!(reader.get_long_long_low(last_retained).unwrap().unwrap(), 0);
    read_reference(reader.get_long_long_high(first_missing).unwrap(),
        SourceScalarReadFailure::LongLongLaneBounds { offset: first_missing * 16, capacity });
    assert_eq!(memory.used_bytes(), used);
    assert_eq!(memory.peak_bytes(), peak);
    drop((capped, captured));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn first_signed_slice_reports_the_inner_long_long_cast_before_row_demand() {
    let memory = memory();
    for (declaration, width) in [
        (SourceScalarDeclaration::Uuid { inner_width: 8 }, 8i64),
        (SourceScalarDeclaration::Time32(TimeUnit::Second), 4),
        (SourceScalarDeclaration::Time64(TimeUnit::Nanosecond), 8),
    ] {
        let output = writer(declaration, true, &memory);
        let reader = output.source_reader().unwrap().unwrap();
        assert!(reader.get_long_long_high(i32::MIN).is_none());
        assert!(reader.get_long_long_low(i32::MIN).is_none());
        let slice = reader.slice(i32::MAX, -1);
        let expected = SourceScalarReadFailure::LongLongReceiverClass { declaration };
        read_reference(slice.get_long_long_high(1), expected);
        read_reference(slice.get_long_long_low(1), expected);
        assert_eq!(slice.get_pointer(1).offset(), 0);
        assert_eq!(slice.get_pointer(1).length(), width);
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn pointer_reuse_keeps_slot_identity_and_retargets_the_live_backing() {
    let memory = memory();
    let mut initial = writer(SourceScalarDeclaration::FixedBinary(3), false, &memory);
    initial.write_bytes(&[5, 6, 7]).unwrap();
    let captured = initial.finish().unwrap();
    let mut replacement = writer(SourceScalarDeclaration::FixedBinary(2), false, &memory);
    replacement.write_bytes(&[13, 17]).unwrap();
    replacement.write_bytes(&[19, 23]).unwrap();

    let mut pointer = SourceScalarReader::new(&captured).unwrap().unwrap().get_pointer(0);
    let address = std::ptr::from_ref(&pointer);
    let original_bytes = pointer.demand_native().unwrap();
    {
        let reader = replacement.source_reader().unwrap().unwrap();
        let reused = reader.get_pointer_into(1, &mut pointer);
        assert_eq!(std::ptr::from_ref(reused), address);
        assert_eq!((reused.offset(), reused.length()), (2, 2));
        assert_eq!(reused.demand_native().unwrap(), &[19u8, 23]);
    }
    assert_eq!(pointer.demand_native().unwrap(), &[19u8, 23]);
    assert_eq!(original_bytes, &[5u8, 6, 7]);
    drop((replacement, captured));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn signed_slice_pointer_reuse_defers_span_denial_and_allows_the_next_retarget() {
    let memory = memory();
    let mut valid = writer(SourceScalarDeclaration::FixedBinary(1), false, &memory);
    valid.write_bytes(&[29]).unwrap();
    let signed = writer(SourceScalarDeclaration::FixedBinary(-1), true, &memory);
    let reader = valid.source_reader().unwrap().unwrap();
    let mut pointer = reader.get_pointer(0);
    let address = std::ptr::from_ref(&pointer);
    let blocker = block(&memory, 0);
    let used = memory.used_bytes();
    let peak = memory.peak_bytes();

    {
        let slice = signed.source_reader().unwrap().unwrap().slice(i32::MAX, -1);
        let reused = slice.get_pointer_into(1, &mut pointer);
        assert_eq!(std::ptr::from_ref(reused), address);
        assert_eq!((reused.offset(), reused.length()), (i64::from(i32::MIN), -1));
    }
    read_native(pointer.demand_native());
    let reused = reader.get_pointer_into(0, &mut pointer);
    assert_eq!(std::ptr::from_ref(reused), address);
    assert_eq!((reused.offset(), reused.length()), (0, 1));
    assert_eq!(reused.demand_native().unwrap(), &[29u8]);
    assert_eq!(memory.used_bytes(), used);
    assert_eq!(memory.peak_bytes(), peak);
    drop((blocker, signed, valid));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn reused_int_pointer_accepts_uuid_inner_width_and_time_pointer_forwarding() {
    let memory = memory();
    let mut int = ScalarWriter::source_int_arrow19_default("seed", false, 1, Arc::clone(&memory)).unwrap();
    int.set_int(0, 42).unwrap();
    let int = int.finish().unwrap();
    let mut uuid = writer(SourceScalarDeclaration::Uuid { inner_width: 24 }, false, &memory);
    uuid.write_bytes(&[31; 24]).unwrap();
    let uuid = uuid.finish().unwrap();
    let declaration = SourceScalarDeclaration::Time32(TimeUnit::Second);
    let mut time = writer(declaration, false, &memory);
    time.write_int(0x1234_5678).unwrap();

    let mut pointer = SourceBytePointer::from_int_arrow19_default(&int, 0).unwrap().unwrap();
    let address = std::ptr::from_ref(&pointer);
    assert_eq!(pointer.demand_native().unwrap(), &42i32.to_le_bytes());
    let uuid_reader = SourceScalarReader::new(&uuid).unwrap().unwrap();
    let reused = uuid_reader.get_pointer_into(0, &mut pointer);
    assert_eq!(std::ptr::from_ref(reused), address);
    assert_eq!(reused.length(), 24);
    assert_eq!(reused.demand_native().unwrap(), &[31u8; 24]);

    let slice = time.source_reader().unwrap().unwrap().slice(i32::MAX, 0);
    let reused = slice.get_pointer_into(1, &mut pointer);
    assert_eq!(std::ptr::from_ref(reused), address);
    assert_eq!((reused.offset(), reused.length()), (0, 4));
    assert_eq!(reused.demand_native().unwrap(), &0x1234_5678i32.to_le_bytes());
    read_reference(slice.get_bytes(1), SourceScalarReadFailure::Unsupported { declaration, method: "getBytes" });
    drop((int, uuid, time));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn source_pointer_comparison_orders_unsigned_bytes_at_lane_and_tail_boundaries() {
    let memory = memory();
    for (width, differing_byte) in [(80usize, 7), (76, 65), (76, 73), (79, 78)] {
        let declaration = SourceScalarDeclaration::FixedBinary(i32::try_from(width).unwrap());
        let mut high = [0x35u8; 80];
        let mut low = high;
        high[differing_byte] = 0x80;
        low[differing_byte] = 0x7f;
        let mut left = writer(declaration, false, &memory);
        left.write_bytes(&high[..width]).unwrap();
        let mut right = writer(declaration, false, &memory);
        right.write_bytes(&low[..width]).unwrap();
        let right = right.finish().unwrap();

        let blocker = block(&memory, 0);
        let used = memory.used_bytes();
        let peak = memory.peak_bytes();
        let high = left.source_reader().unwrap().unwrap().get_pointer(0);
        let low = SourceScalarReader::new(&right).unwrap().unwrap().slice(-1, 0).get_pointer(1);
        assert_eq!(high.compare_to(&low).unwrap(), Ordering::Greater);
        assert_eq!(low.compare_to(&high).unwrap(), Ordering::Less);
        assert_eq!(memory.used_bytes(), used);
        assert_eq!(memory.peak_bytes(), peak);
        drop((blocker, left, right));
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn source_pointer_comparison_orders_equal_prefixes_by_length() {
    let memory = memory();
    let mut short = writer(SourceScalarDeclaration::FixedBinary(5), false, &memory);
    short.write_bytes(&[255, 17, 51, 8, 0]).unwrap();
    let mut copy = writer(SourceScalarDeclaration::FixedBinary(5), false, &memory);
    copy.write_bytes(&[255, 17, 51, 8, 0]).unwrap();
    let copy = copy.finish().unwrap();
    let mut long = writer(SourceScalarDeclaration::FixedBinary(6), false, &memory);
    long.write_bytes(&[255, 17, 51, 8, 0, 0]).unwrap();
    let empty_owner = writer(SourceScalarDeclaration::FixedBinary(0), false, &memory);
    let empty_copy = writer(SourceScalarDeclaration::FixedBinary(0), false, &memory).finish().unwrap();

    let shorter = short.source_reader().unwrap().unwrap().get_pointer(0);
    let equal = SourceScalarReader::new(&copy).unwrap().unwrap().get_pointer(0);
    let longer = long.source_reader().unwrap().unwrap().get_pointer(0);
    let zero = empty_owner.source_reader().unwrap().unwrap().get_pointer(i32::MIN);
    let zero_copy = SourceScalarReader::new(&empty_copy).unwrap().unwrap().get_pointer(i32::MAX);
    assert_eq!(shorter.compare_to(&equal).unwrap(), Ordering::Equal);
    assert_eq!(shorter.compare_to(&longer).unwrap(), Ordering::Less);
    assert_eq!(longer.compare_to(&shorter).unwrap(), Ordering::Greater);
    assert_eq!(zero.compare_to(&zero_copy).unwrap(), Ordering::Equal);
    assert_eq!(zero.compare_to(&longer).unwrap(), Ordering::Less);
    assert_eq!(longer.compare_to(&zero).unwrap(), Ordering::Greater);
    drop((short, copy, long, empty_owner, empty_copy));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn source_pointer_comparison_validates_both_operands_in_arrow_order() {
    let memory = memory();
    let signed = writer(SourceScalarDeclaration::FixedBinary(-1), false, &memory);
    let empty_owner = writer(SourceScalarDeclaration::FixedBinary(0), false, &memory);
    let mut oversized = writer(SourceScalarDeclaration::FixedBinary(256), false, &memory);
    oversized.write_bytes(&[0x80]).unwrap();
    let oversized = oversized.finish().unwrap();
    let capacity = data(&oversized).bytes.as_slice().len();
    assert!(capacity < 256);
    let mut valid = writer(SourceScalarDeclaration::FixedBinary(1), false, &memory);
    valid.write_bytes(&[0x7f]).unwrap();
    valid.write_bytes(&[0x80]).unwrap();
    valid.write_bytes(&[0x20]).unwrap();
    let valid = valid.finish().unwrap();

    let blocker = block(&memory, 0);
    let used = memory.used_bytes();
    let peak = memory.peak_bytes();
    let negative = signed.source_reader().unwrap().unwrap().slice(i32::MAX, 0).get_pointer(1);
    assert_eq!((negative.offset(), negative.length()), (i64::from(i32::MIN), -1));
    let oversized_reader = SourceScalarReader::new(&oversized).unwrap().unwrap();
    let oversized_pointer = oversized_reader.get_pointer(0);
    let empty = empty_owner.source_reader().unwrap().unwrap().get_pointer(0);
    let valid_reader = SourceScalarReader::new(&valid).unwrap().unwrap();
    let left = valid_reader.get_pointer(0);
    let left_negative = SourceScalarReadFailure::PointerComparisonNegativeLength {
        operand: PointerComparisonOperand::Left, length: -1,
    };
    let right_negative = SourceScalarReadFailure::PointerComparisonNegativeLength {
        operand: PointerComparisonOperand::Right, length: -1,
    };
    let left_bounds = SourceScalarReadFailure::PointerComparisonBounds {
        operand: PointerComparisonOperand::Left, offset: 0, length: 256, capacity,
    };
    let right_bounds = SourceScalarReadFailure::PointerComparisonBounds {
        operand: PointerComparisonOperand::Right, offset: 0, length: 256, capacity,
    };
    read_reference(negative.compare_to(&oversized_pointer), left_negative);
    read_reference(negative.compare_to(&negative), left_negative);
    read_reference(oversized_pointer.compare_to(&negative), left_bounds);
    read_reference(left.compare_to(&negative), right_negative);
    read_reference(left.compare_to(&oversized_pointer), right_bounds);
    read_reference(empty.compare_to(&oversized_pointer), right_bounds);
    read_reference(left.compare_to(&valid_reader.get_pointer(-1)),
        SourceScalarReadFailure::PointerComparisonBounds {
            operand: PointerComparisonOperand::Right, offset: -1, length: 1,
            capacity: data(&valid).bytes.as_slice().len(),
        });
    read_native(oversized_pointer.demand_native());

    let mut slot = valid_reader.get_pointer(1);
    let slot_address = std::ptr::from_ref(&slot);
    let old_bytes = slot.demand_native().unwrap();
    let reused = oversized_reader.get_pointer_into(0, &mut slot);
    assert_eq!(std::ptr::from_ref(reused), slot_address);
    read_reference(left.compare_to(reused), right_bounds);
    let slice = valid_reader.slice(3, -1);
    let reused = slice.get_pointer_into(-1, &mut slot);
    assert_eq!(std::ptr::from_ref(reused), slot_address);
    assert_eq!(left.compare_to(reused).unwrap(), Ordering::Greater);
    assert_eq!(old_bytes, &[0x80u8]);
    assert_eq!(memory.used_bytes(), used);
    assert_eq!(memory.peak_bytes(), peak);
    drop((blocker, signed, empty_owner, oversized, valid));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn source_pointer_equality_checks_object_identity_before_the_signed_span() {
    let memory = memory();
    let writer = writer(SourceScalarDeclaration::FixedBinary(-1), false, &memory);
    let reader = writer.source_reader().unwrap().unwrap();
    let slice = reader.slice(i32::MAX, 0);
    let first = slice.get_pointer(1);
    let second = slice.get_pointer(1);
    let alias = &first;
    assert_eq!((first.offset(), first.length()), (i64::from(i32::MIN), -1));
    assert_eq!((first.offset(),first.length()), (second.offset(),second.length()));
    assert!(first.equals(alias).unwrap());
    assert!(second.equals(&second).unwrap());
    let failure = SourceScalarReadFailure::PointerComparisonNegativeLength {
        operand: PointerComparisonOperand::Left, length: -1,
    };
    read_reference(first.equals(&second), failure);
    read_reference(first.compare_to(alias), failure);
    drop(writer);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn source_pointer_equality_checks_both_spans_before_length_and_byte_results() {
    let memory = memory();
    let mut left = writer(SourceScalarDeclaration::FixedBinary(1), false, &memory);
    left.write_bytes(&[0x80]).unwrap();
    left.write_bytes(&[0x7f]).unwrap();
    let mut equal = writer(SourceScalarDeclaration::FixedBinary(1), false, &memory);
    equal.write_bytes(&[0x80]).unwrap();
    let equal = equal.finish().unwrap();
    let mut longer = writer(SourceScalarDeclaration::FixedBinary(2), false, &memory);
    longer.write_bytes(&[0x80, 0]).unwrap();
    let mut out_of_bounds = writer(SourceScalarDeclaration::FixedBinary(256), false, &memory);
    out_of_bounds.write_bytes(&[0x80]).unwrap();
    let out_of_bounds = out_of_bounds.finish().unwrap();
    let capacity = data(&out_of_bounds).bytes.as_slice().len();
    assert!(capacity < 256);
    let empty = writer(SourceScalarDeclaration::FixedBinary(0), false, &memory);

    let blocker = block(&memory, 0);
    let used = memory.used_bytes();
    let left_reader = left.source_reader().unwrap().unwrap();
    let first = left_reader.get_pointer(0);
    let alternate = left_reader.get_pointer(1);
    let equal_pointer = SourceScalarReader::new(&equal).unwrap().unwrap().get_pointer(0);
    let longer_pointer = longer.source_reader().unwrap().unwrap().get_pointer(0);
    let invalid = SourceScalarReader::new(&out_of_bounds).unwrap().unwrap().get_pointer(0);
    let empty_reader = empty.source_reader().unwrap().unwrap();
    let empty_a = empty_reader.get_pointer(i32::MIN);
    let empty_b = empty_reader.get_pointer(i32::MAX);
    assert!(first.equals(&equal_pointer).unwrap());
    assert!(equal_pointer.equals(&first).unwrap());
    assert!(!first.equals(&alternate).unwrap());
    assert!(!first.equals(&longer_pointer).unwrap());
    assert!(!empty_a.equals(&first).unwrap());
    assert!(empty_a.equals(&empty_b).unwrap());
    read_reference(empty_a.equals(&invalid), SourceScalarReadFailure::PointerComparisonBounds {
        operand: PointerComparisonOperand::Right, offset: 0, length: 256, capacity,
    });
    read_reference(invalid.equals(&first), SourceScalarReadFailure::PointerComparisonBounds {
        operand: PointerComparisonOperand::Left, offset: 0, length: 256, capacity,
    });
    assert_eq!(memory.used_bytes(), used);
    drop((blocker, left, equal, longer, out_of_bounds, empty));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn source_pointer_hash_uses_little_endian_lanes_and_a_signed_byte_tail() {
    let memory = memory();
    let bytes = [
        0xff, 0x80, 0x02, 0xfe, 0x44, 0x00, 0x00, 0x80,
        0xff, 0x7f, 0x01, 0xff, 0x02, 0x11, 0x80, 0x00,
        0x80, 0xff, 0x01, 0x41, 0x80, 0xff, 0x7f,
    ];
    for (width, expected) in [
        (1usize, -1), (3, -6_103), (4, -33_390_337), (7, 896_467_655),
        (8, 2_114_093_243), (12, 895_359_750), (15, 2_097_741_525),
        (16, 903_744_004), (23, 1_287_533_342),
    ] {
        let mut output = writer(SourceScalarDeclaration::FixedBinary(i32::try_from(width).unwrap()), false, &memory);
        output.write_bytes(&bytes[..width]).unwrap();
        let captured = output.finish().unwrap();
        let blocker = block(&memory, 0);
        let used = memory.used_bytes();
        let peak = memory.peak_bytes();
        let mut pointer = SourceScalarReader::new(&captured).unwrap().unwrap()
            .slice(i32::MIN, 0).get_pointer(i32::MIN);
        assert_eq!(pointer.cached_hash, None);
        assert_eq!(pointer.hash_code().unwrap(), expected);
        assert_eq!(pointer.cached_hash, Some(expected));
        assert_eq!(pointer.hash_code().unwrap(), expected);
        assert_eq!(memory.used_bytes(), used);
        assert_eq!(memory.peak_bytes(), peak);
        drop((blocker, captured));
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn source_pointer_hash_retarget_clears_zero_and_nonzero_cached_results() {
    let memory = memory();
    let mut int = ScalarWriter::source_int_arrow19_default("seed", false, 1, Arc::clone(&memory)).unwrap();
    int.set_int(0, i32::MIN).unwrap();
    {
        let blocker = block(&memory, 0);
        let used = memory.used_bytes();
        let peak = memory.peak_bytes();
        let mut pointer = int.source_int_pointer(0).unwrap().unwrap();
        assert_eq!(pointer.cached_hash, None);
        assert_eq!(pointer.hash_code().unwrap(), i32::MIN);
        assert_eq!(pointer.cached_hash, Some(i32::MIN));
        assert_eq!(memory.used_bytes(), used);
        assert_eq!(memory.peak_bytes(), peak);
        drop(blocker);
    }

    let captured = int.finish().unwrap();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(1), false, &memory);
    output.write_bytes(&[0x00]).unwrap();
    output.write_bytes(&[0xff]).unwrap();
    let empty = writer(SourceScalarDeclaration::FixedBinary(0), false, &memory);
    let blocker = block(&memory, 0);
    let used = memory.used_bytes();
    let peak = memory.peak_bytes();

    let mut pointer = SourceBytePointer::from_int_arrow19_default(&captured, 0).unwrap().unwrap();
    let address = std::ptr::from_ref(&pointer);
    let int_bytes = pointer.demand_native().unwrap();
    assert_eq!(pointer.cached_hash, None);
    assert_eq!(pointer.hash_code().unwrap(), i32::MIN);
    let reader = output.source_reader().unwrap().unwrap();
    let zero = reader.get_pointer_into(0, &mut pointer);
    assert_eq!(std::ptr::from_ref(zero), address);
    assert_eq!(zero.cached_hash, None);
    assert_eq!(zero.hash_code().unwrap(), 0);
    assert_eq!(zero.cached_hash, Some(0));
    assert_eq!(zero.hash_code().unwrap(), 0);

    let distinct = reader.get_pointer(0);
    assert!(zero.equals(&distinct).unwrap());
    assert_eq!(zero.compare_to(&distinct).unwrap(), Ordering::Equal);
    assert_eq!(zero.cached_hash, Some(0));
    assert_eq!(distinct.cached_hash, None);

    let same_facts = reader.get_pointer_into(0, &mut pointer);
    assert_eq!(std::ptr::from_ref(same_facts), address);
    assert_eq!(same_facts.cached_hash, None);
    assert_eq!(same_facts.hash_code().unwrap(), 0);
    let wrapped = reader.slice(i32::MIN, 0).get_pointer_into(i32::MIN + 1, &mut pointer);
    assert_eq!(std::ptr::from_ref(wrapped), address);
    assert_eq!((wrapped.offset(), wrapped.length()), (1, 1));
    assert_eq!(wrapped.cached_hash, None);
    assert_eq!(wrapped.hash_code().unwrap(), -1);

    let empty_pointer = empty.source_reader().unwrap().unwrap().get_pointer_into(i32::MAX, &mut pointer);
    assert_eq!(std::ptr::from_ref(empty_pointer), address);
    assert_eq!((empty_pointer.offset(), empty_pointer.length()), (0, 0));
    assert_eq!(empty_pointer.cached_hash, None);
    assert_eq!(empty_pointer.hash_code().unwrap(), 0);
    assert_eq!(empty_pointer.cached_hash, Some(0));
    assert_eq!(int_bytes, &i32::MIN.to_le_bytes());
    assert_eq!(memory.used_bytes(), used);
    assert_eq!(memory.peak_bytes(), peak);
    drop((blocker, captured, output, empty));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn source_pointer_hash_checks_signed_span_before_reading_or_caching() {
    let memory = memory();
    let signed = writer(SourceScalarDeclaration::FixedBinary(-1), false, &memory);
    let width = i32::MAX - 7;
    let wide = writer(SourceScalarDeclaration::FixedBinary(width), false, &memory);
    let mut valid = writer(SourceScalarDeclaration::FixedBinary(1), false, &memory);
    valid.write_bytes(&[0x80]).unwrap();
    let blocker = block(&memory, 0);
    let used = memory.used_bytes();
    let peak = memory.peak_bytes();
    let reader = valid.source_reader().unwrap().unwrap();
    let mut pointer = reader.get_pointer(0);
    let address = std::ptr::from_ref(&pointer);
    assert_eq!(pointer.hash_code().unwrap(), -128);

    let negative = signed.source_reader().unwrap().unwrap().slice(i32::MAX, 0)
        .get_pointer_into(1, &mut pointer);
    assert_eq!(std::ptr::from_ref(negative), address);
    assert_eq!((negative.offset(), negative.length()), (i64::from(i32::MIN), -1));
    for _ in 0..2 {
        read_reference(negative.hash_code(), SourceScalarReadFailure::PointerHashNegativeLength { length: -1 });
        assert_eq!(negative.cached_hash, None);
    }

    let oversized = wide.source_reader().unwrap().unwrap().get_pointer_into(0, &mut pointer);
    assert_eq!(std::ptr::from_ref(oversized), address);
    assert_eq!(oversized.backing.len(), 0);
    for _ in 0..2 {
        read_reference(oversized.hash_code(), SourceScalarReadFailure::PointerHashBounds {
            offset: 0, length: i64::from(width), capacity: 0,
        });
        assert_eq!(oversized.cached_hash, None);
    }

    let recovered = reader.get_pointer_into(0, &mut pointer);
    assert_eq!(std::ptr::from_ref(recovered), address);
    assert_eq!(recovered.hash_code().unwrap(), -128);
    assert_eq!(recovered.cached_hash, Some(-128));
    assert_eq!(memory.used_bytes(), used);
    assert_eq!(memory.peak_bytes(), peak);
    drop((blocker, signed, wide, valid));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn source_pointer_hash_cursor_refuses_the_first_wrapped_eight_byte_guard() {
    let last_aligned = i32::MAX as usize - 7;
    admit_simple_hasher_cursor(last_aligned - 1).unwrap();
    for length in [last_aligned, i32::MAX as usize] {
        let Err(SourceScalarReadError::Native(error)) = admit_simple_hasher_cursor(length) else {
            panic!("expected native cursor refusal");
        };
        assert_eq!(error.code, "rtdb/pointer-hash-cursor-overflow");
    }
}

#[test]
fn source_selection_maps_signed_rows_before_raw_bytes_and_fixed_long_long_lanes() {
    let memory = memory();
    let mut output = writer(SourceScalarDeclaration::FixedBinary(8), true, &memory);
    output.write_bytes(&[0x80; 8]).unwrap();
    output.write_null().unwrap();
    output.write_bytes(&[0x20; 8]).unwrap();
    output.write_bytes(&[0xff; 8]).unwrap();
    let captured = output.finish().unwrap();
    assert!(captured.cell(1).unwrap().is_null().unwrap());
    let capacity = data(&captured).bytes.as_slice().len();
    let past_storage = i32::try_from(capacity / 8).unwrap();
    let indices = [1, 0, 1, i32::MIN, -1, past_storage];
    let blocker = block(&memory, 0);
    let used = memory.used_bytes();
    let peak = memory.peak_bytes();
    let reader = SourceScalarReader::new(&captured).unwrap().unwrap();
    let selected = reader.select(&indices).unwrap();

    assert_eq!(selected.value_count(), 6);
    for index in [0, 2] {
        assert_eq!(selected.get_bytes(index).unwrap().demand_native().unwrap(), &[0; 8]);
        assert_eq!(selected.get_long_long_high(index).unwrap(), i64::from_be_bytes([0x20; 8]));
        assert_eq!(selected.get_long_long_low(index).unwrap(), -1);
    }
    for index in [1, 3] {
        assert_eq!(selected.get_bytes(index).unwrap().demand_native().unwrap(), &[0x80; 8]);
        assert_eq!(selected.get_long_long_high(index).unwrap(), i64::from_be_bytes([0x80; 8]));
        assert_eq!(selected.get_long_long_low(index).unwrap(), 0);
    }
    let pointer = selected.get_pointer(4).unwrap();
    assert_eq!((pointer.offset(), pointer.length()), (-8, 8));
    read_native(pointer.demand_native());
    read_reference(selected.get_long_long_high(4), SourceScalarReadFailure::LongLongLaneBounds { offset: -16, capacity });
    read_reference(selected.get_long_long_low(4), SourceScalarReadFailure::LongLongLaneBounds { offset: -8, capacity });
    let past = selected.get_bytes(5).unwrap();
    assert_eq!(past.offset(), i64::from(past_storage) * 8);
    read_native(past.demand_native());
    assert_eq!(memory.used_bytes(), used);
    assert_eq!(memory.peak_bytes(), peak);
    drop((blocker, captured));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn source_selection_reports_lookup_and_receiver_failures_in_source_order() {
    let memory = memory();
    for declaration in [SourceScalarDeclaration::FixedBinary(-1), SourceScalarDeclaration::Uuid { inner_width: -1 },
                        SourceScalarDeclaration::Time32(TimeUnit::Second)] {
        let output = writer(declaration, false, &memory);
        let reader = output.source_reader().unwrap().unwrap();
        let selected = reader.select(&[0]).unwrap();
        for row in [-1, 1, i32::MAX] {
            let bounds = SourceScalarReadFailure::SelectionIndexBounds { index: row, length: 1 };
            read_reference(selected.get_bytes(row), bounds);
            read_reference(selected.get_pointer(row), bounds);
            let long_long = if matches!(declaration, SourceScalarDeclaration::FixedBinary(_)) { bounds }
                else { SourceScalarReadFailure::LongLongReceiverClass { declaration } };
            read_reference(selected.get_long_long_high(row), long_long);
            read_reference(selected.get_long_long_low(row), long_long);
        }
        let bytes = if matches!(declaration, SourceScalarDeclaration::Time32(_)) {
            SourceScalarReadFailure::Unsupported { declaration, method: "getBytes" }
        } else { SourceScalarReadFailure::NegativeViewLength(-1) };
        read_reference(selected.get_bytes(0), bytes);
        let pointer = selected.get_pointer(0).unwrap();
        assert_eq!(pointer.length(), i64::from(declaration.fixed_width()));
        let empty = reader.select(&[]).unwrap();
        assert_eq!(empty.value_count(), 0);
        read_reference(empty.get_bytes(0), SourceScalarReadFailure::SelectionIndexBounds { index: 0, length: 0 });
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn source_selection_pointer_reuse_keeps_both_owners_and_outlives_the_selector() {
    let memory = memory();
    let mut int = ScalarWriter::source_int_arrow19_default("seed", false, 1, Arc::clone(&memory)).unwrap();
    int.set_int(0, 42).unwrap();
    let int = int.finish().unwrap();
    let mut uuid = writer(SourceScalarDeclaration::Uuid { inner_width: 24 }, false, &memory);
    uuid.write_bytes(&[31; 24]).unwrap();
    uuid.write_bytes(&[62; 24]).unwrap();
    let uuid = uuid.finish().unwrap();
    let blocker = block(&memory, 0);
    let used = memory.used_bytes();
    let peak = memory.peak_bytes();
    let mut pointer = SourceBytePointer::from_int_arrow19_default(&int, 0).unwrap().unwrap();
    let slot = std::ptr::from_ref(&pointer);
    let before = pointer.demand_native().unwrap();
    assert_eq!(pointer.hash_code().unwrap(), 42);

    let (view, span, standalone) = {
        let indices = vec![1, -1];
        let reader = SourceScalarReader::new(&uuid).unwrap().unwrap();
        let selected = reader.select(&indices).unwrap();
        read_reference(selected.get_pointer_into(2, &mut pointer),
            SourceScalarReadFailure::SelectionIndexBounds { index: 2, length: 2 });
        assert_eq!((pointer.offset(), pointer.length(), pointer.cached_hash), (0, 4, Some(42)));
        assert_eq!(pointer.demand_native().unwrap(), before);
        let retargeted = selected.get_pointer_into(0, &mut pointer).unwrap();
        assert_eq!(std::ptr::from_ref(retargeted), slot);
        assert_eq!((retargeted.offset(), retargeted.length(), retargeted.cached_hash), (24, 24, None));
        let span = retargeted.demand_native().unwrap();
        let negative = selected.get_pointer_into(1, &mut pointer).unwrap();
        assert_eq!(std::ptr::from_ref(negative), slot);
        assert_eq!((negative.offset(), negative.length()), (-24, 24));
        read_native(negative.demand_native());
        (selected.get_bytes(0).unwrap(), span, selected.get_pointer(0).unwrap())
    };

    assert_eq!(before, &42i32.to_le_bytes());
    assert_eq!(span, &[62; 24]);
    assert_eq!(view.demand_native().unwrap(), span);
    assert_eq!(standalone.demand_native().unwrap(), span);
    assert_eq!(memory.used_bytes(), used);
    assert_eq!(memory.peak_bytes(), peak);
    drop((blocker, int, uuid));
    assert_eq!(memory.used_bytes(), 0);
}
