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
