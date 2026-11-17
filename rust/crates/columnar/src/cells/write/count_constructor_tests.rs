use super::*;
use super::owner::MutableScalarData;
use super::super::descriptor::{ContainerShapeRef, StructFieldRef};
use super::super::native_container::{CapturedChildren, NativeContainerParts, publish_native};
use super::super::open::{CountScalarPayload, finish_count_scalar};
use arrow_buffer::Buffer;

fn memory() -> Arc<MemoryBudget> { MemoryBudget::new(16 * 1024 * 1024) }
fn int(nullable: bool, count: i32, memory: &Arc<MemoryBudget>) -> ScalarWriter {
    ScalarWriter::source_int_arrow19_default("n", nullable, count, Arc::clone(memory)).unwrap()
}
fn null(nullable: bool, count: i32, memory: &Arc<MemoryBudget>) -> ScalarWriter {
    ScalarWriter::source_null("n", nullable, count, Arc::clone(memory)).unwrap()
}
fn header() -> usize {
    let memory = memory();
    let output = null(false, 0, &memory);
    let bytes = memory.used_bytes();
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
    bytes
}
fn block(memory: &Arc<MemoryBudget>, remaining: usize) -> Arc<MemoryReservation> {
    memory.reserve(memory.limit_bytes() - memory.used_bytes() - remaining).unwrap()
}
fn reference<T>(result: WriteResult<T>, expected: ReferenceFailure) {
    match result { Err(WriteError::Reference(actual)) => assert_eq!(actual, expected), _ => panic!("expected reference failure") }
}
fn native<T>(result: WriteResult<T>, code: &str) {
    match result { Err(WriteError::Native(error)) => assert_eq!(error.code, code), _ => panic!("expected native failure") }
}
fn unretained<T>(result: SourceScalarReadResult<T>) {
    match result { Err(SourceScalarReadError::Native(error)) => assert_eq!(error.code, "rtdb/unretained-byte-view"), _ => panic!("expected native demand failure") }
}
fn storage(vector: &ExecutionVector) -> &NativeScalarStorage {
    let NodeLayout::Scalar(storage) = &vector.node().storage else { panic!() };
    storage
}
fn data(vector: &ExecutionVector) -> &StoredBytes {
    let NativeScalarData::Fixed(data) = &storage(vector).data else { panic!() };
    data
}
fn mutable_data(writer: &mut ScalarWriter) -> &MutableBytes {
    let MutableScalarData::Fixed { width: 4, data } = &writer.fixture_state().data else { panic!() };
    data
}
fn count_class(vector: &ExecutionVector) -> CountScalarClass { vector.descriptor().scalar().unwrap().count_class().unwrap() }
fn stored(values: &[u8], append: i64, memory: &Arc<MemoryBudget>) -> StoredBytes {
    let reservation = memory.reserve(values.len() + 128).unwrap();
    StoredBytes { bytes: RetainedBuffer::with_reservation(Buffer::from_slice_ref(values), reservation).unwrap(), append }
}
fn fixture(descriptor: Arc<VectorDescriptor>, count: i32, storage: NodeLayout, memory: &Arc<MemoryBudget>) -> ExecutionVector {
    let owner = Arc::new(NodeOwner { backing: BackingOwner::Native, metadata: memory.reserve(512).unwrap() });
    ExecutionVector::direct(Arc::new(PhysicalNode { declaration: Declaration::Native, descriptor, count: SourceCount::new(count), storage, owner }))
}

#[test]
fn null_constructor_retains_every_signed_initial_count_without_payload() {
    for nullable in [false, true] {
        for count in [i32::MIN, -1, 0, i32::MAX] {
            let memory = memory();
            let output = null(nullable, count, &memory);
            assert_eq!(output.value_count(), count);
            assert_eq!(output.checked_len().is_ok(), count >= 0);
            assert_eq!(output.field(), &Field::new("n", DataType::Null, nullable));
            assert!(output.reader().is_null(i32::MIN).unwrap());
            let retained = memory.used_bytes();
            let output = output.finish().unwrap();
            assert_eq!(memory.used_bytes(), retained);
            assert_eq!(output.value_count(), count);
            assert_eq!(output.is_empty(), count == 0);
            assert_eq!(output.checked_len().is_ok(), count >= 0);
            assert_eq!(count_class(&output), CountScalarClass::Null);
            assert!(matches!(output.ingress_field(), DeclarationRef::Native(_)));
            assert_eq!(output.logical_type().unwrap(), if nullable { &LogicalType::Null } else { &LogicalType::Nothing });
            assert!(storage(&output).validity.is_none());
            assert!(matches!(storage(&output).data, NativeScalarData::Null));
            assert!(SourceBytePointer::from_int_arrow19_default(&output, 0).unwrap().is_none());
            drop(output);
            assert_eq!(memory.used_bytes(), 0);
        }
    }
}

#[test]
fn null_constructor_header_admission_releases_failed_and_successful_owners() {
    let bytes = header();
    let too_small = MemoryBudget::new(bytes - 1);
    native(ScalarWriter::source_null("n", true, i32::MIN, Arc::clone(&too_small)), "rtdb/memory-budget-exceeded");
    assert_eq!(too_small.used_bytes(), 0);
    assert_eq!(too_small.peak_bytes(), 0);
    let exact = MemoryBudget::new(bytes);
    let output = null(true, i32::MAX, &exact);
    assert_eq!(exact.used_bytes(), bytes);
    assert_eq!(exact.peak_bytes(), bytes);
    drop(output);
    assert_eq!(exact.used_bytes(), 0);
}

#[test]
fn null_initial_count_wraps_and_signed_capacity_recovers_without_buffers() {
    let memory = memory();
    let mut output = null(false, i32::MAX, &memory);
    let retained = memory.used_bytes();
    output.write_undefined().unwrap();
    assert_eq!(output.value_count(), i32::MIN);
    output.set_null(i32::MAX).unwrap();
    assert_eq!(output.value_count(), i32::MIN);
    output.ensure_capacity(-7).unwrap();
    assert_eq!(output.value_count(), -7);
    assert_eq!(memory.used_bytes(), retained);
    output.write_null().unwrap();
    assert_eq!(output.value_count(), -6);
    assert!(output.field().is_nullable());
    assert_eq!(output.logical(), &LogicalType::Null);
    assert!(output.fixture_state().validity.is_none());
    output.clear();
    assert_eq!(output.value_count(), 0);
    assert!(output.field().is_nullable());
    let output = output.finish().unwrap();
    assert!(matches!(storage(&output).data, NativeScalarData::Null));
    assert!(storage(&output).validity.is_none());
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn negative_int_request_fails_before_metadata_for_scalar_and_root_constructors() {
    for nullable in [false, true] {
        for (count, request) in [(-1, -4), (i32::MIN, -8_589_934_592)] {
            for limit in [0, 1, header() + 256] {
                let memory = MemoryBudget::new(limit);
                reference(ScalarWriter::source_int_arrow19_default("n", nullable, count, Arc::clone(&memory)), ReferenceFailure::NegativeAllocationSize(request));
                reference(VectorWriter::source_int_arrow19_default("n", nullable, count, Arc::clone(&memory)), ReferenceFailure::NegativeAllocationSize(request));
                assert_eq!(memory.used_bytes(), 0);
                assert_eq!(memory.peak_bytes(), 0);
            }
        }
    }
}

#[test]
fn arrow19_stock_rounding_preserves_zero_one_and_strict_chunk_threshold() {
    for (request, expected) in [
        (0, 0), (1, 2), (2, 2), (3, 4), (4, 4), (5, 8), (12, 16), (36, 64),
        (16_777_212, 16_777_216), (16_777_215, 16_777_216), (16_777_216, 16_777_216),
        (16_777_217, 16_777_217), (16_777_220, 16_777_220), (268_435_456, 268_435_456),
        (8_589_934_588, 8_589_934_588),
    ] {
        assert_eq!(buffer::arrow19_default_capacity(request).unwrap() as u64, expected);
    }
    assert_eq!(buffer::arrow19_default_capacity(u64::MAX).unwrap_err().code, "rtdb/allocation-overflow");
    assert_eq!(buffer::arrow19_default_capacity(i64::MAX as u64).unwrap() as u64, i64::MAX as u64);
}

#[test]
fn zero_int_constructor_charges_only_metadata_and_preserves_empty_cursors() {
    for nullable in [false, true] {
        let memory = MemoryBudget::new(header());
        let mut output = int(nullable, 0, &memory);
        assert_eq!(output.value_count(), 0);
        assert_eq!(memory.used_bytes(), header());
        assert!(mutable_data(&mut output).bytes().is_empty());
        assert_eq!(mutable_data(&mut output).append, 0);
        assert_eq!(output.fixture_state().validity.is_some(), nullable);
        if let Some(bits) = &output.fixture_state().validity {
            assert!(bits.bytes().is_empty());
            assert_eq!(bits.append, 0);
            assert_eq!(bits.byte_append(), 0);
        }
        let output = output.finish().unwrap();
        assert_eq!(output.value_count(), 0);
        assert_eq!(data(&output).bytes.len(), 0);
        assert_eq!(data(&output).append, 0);
        assert_eq!(storage(&output).validity.is_some(), nullable);
        assert_eq!(memory.used_bytes(), header());
        drop(output);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn int_initial_buffers_use_exposed_default_capacity_and_full_zeroing() {
    for (count, data_capacity, bit_capacity) in [(1, 4, 2), (3, 16, 2), (9, 64, 2), (17, 128, 4)] {
        for nullable in [false, true] {
            let memory = memory();
            let mut output = int(nullable, count, &memory);
            assert_eq!(output.value_count(), count);
            assert_eq!(output.field(), &Field::new("n", DataType::Int32, nullable));
            assert_eq!(mutable_data(&mut output).bytes(), vec![0; data_capacity]);
            assert_eq!(mutable_data(&mut output).append, 0);
            assert_eq!(memory.used_bytes(), header() + data_capacity + 128 + if nullable { bit_capacity + 128 } else { 0 });
            if let Some(bits) = &output.fixture_state().validity {
                assert_eq!(bits.bytes(), vec![0; bit_capacity]);
                assert_eq!(bits.append, 0);
                assert_eq!(bits.byte_append(), 0);
            }
            let output = output.finish().unwrap();
            assert_eq!(count_class(&output), CountScalarClass::IntArrow19Default);
            assert_eq!(output.descriptor().scalar().unwrap().fixed_width().unwrap(), Some(4));
            assert!(output.descriptor().scalar().unwrap().source_declaration().is_none());
            assert_eq!(data(&output).bytes.len(), data_capacity);
            assert_eq!(data(&output).append, 0);
            assert_eq!(storage(&output).validity.is_some(), nullable);
            if nullable {
                assert!(output.cell(0).unwrap().is_null().unwrap());
            } else {
                assert_eq!(output.cell((count - 1) as usize).unwrap().get_int(NullChecks::Enabled).unwrap(), 0);
            }
            drop(output);
            assert_eq!(memory.used_bytes(), 0);
        }
    }
}

#[test]
fn nonnullable_initial_four_byte_span_rejects_long_without_growing_or_mutating() {
    let memory = memory();
    let mut output = int(false, 1, &memory);
    let blocker = block(&memory, 0);
    reference(output.set_long(0, 42), ReferenceFailure::Bounds);
    assert_eq!(output.value_count(), 1);
    assert_eq!(mutable_data(&mut output).bytes(), &[0; 4]);
    assert_eq!(mutable_data(&mut output).append, 0);
    output.set_int(0, 9).unwrap();
    assert_eq!(output.reader().get_int(0, NullChecks::Enabled).unwrap(), 9);
    let output = output.finish().unwrap();
    assert_eq!(data(&output).bytes.len(), 4);
    assert_eq!(data(&output).append, 0);
    drop(output);
    assert_eq!(memory.used_bytes(), blocker.bytes());
    drop(blocker);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn nullable_long_failure_keeps_the_validity_prefix_and_ready_gate() {
    let memory = memory();
    let mut output = int(true, 1, &memory);
    assert!(output.reader().is_null(0).unwrap());
    let blocker = block(&memory, 0);
    reference(output.set_long(0, 42), ReferenceFailure::Bounds);
    assert!(!output.reader().is_null(0).unwrap());
    assert_eq!(output.reader().get_int(0, NullChecks::Enabled).unwrap(), 0);
    assert_eq!(output.value_count(), 1);
    let bits = output.fixture_state().validity.as_ref().unwrap();
    assert_eq!(bits.bytes(), &[1, 0]);
    assert_eq!((bits.append, bits.byte_append()), (0, 0));
    assert_eq!(mutable_data(&mut output).bytes(), &[0; 4]);
    output.set_int(0, 7).unwrap();
    let output = output.finish().unwrap();
    assert_eq!(data(&output).append, 0);
    assert_eq!(storage(&output).validity.as_ref().unwrap().append, 0);
    drop(output);
    drop(blocker);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn strict_sdk_initialization_keeps_its_existing_growth_policy() {
    let memory = memory();
    let mut output = ScalarWriter::new(Arc::new(Field::new("n", DataType::Int32, false)), Arc::clone(&memory)).unwrap();
    output.ensure_capacity(1).unwrap();
    output.set_long(0, 42).unwrap();
    assert_eq!(mutable_data(&mut output).bytes().len(), 128);
    assert!(output.source_int_pointer(0).unwrap().is_none());
    let output = output.finish().unwrap();
    assert!(matches!(output.ingress_field(), DeclarationRef::Arrow(_)));
    assert!(SourceBytePointer::from_int_arrow19_default(&output, 0).unwrap().is_none());
    assert_eq!(output.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 42);
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn initial_count_does_not_move_append_cursors_and_later_growth_preserves_prefix() {
    let memory = memory();
    let mut output = int(true, 1, &memory);
    output.write_int(42).unwrap();
    assert_eq!(output.value_count(), 2);
    assert_eq!(mutable_data(&mut output).bytes(), &42i32.to_le_bytes());
    assert_eq!(mutable_data(&mut output).append, 4);
    assert!(output.reader().is_null(0).unwrap());
    assert!(!output.reader().is_null(1).unwrap());
    assert!(matches!(output.reader().get_int(1, NullChecks::Enabled), Err(VectorReadError::Anomaly(error)) if error.code == "rtdb/row-index"));
    let bits = output.fixture_state().validity.as_ref().unwrap();
    assert_eq!(bits.bytes(), &[2, 0]);
    assert_eq!((bits.append, bits.byte_append()), (2, 0));
    let pointer = output.source_int_pointer(1).unwrap().unwrap();
    assert_eq!((pointer.offset(), pointer.length()), (4, 4));
    unretained(pointer.demand_native());
    output.write_int(9).unwrap();
    assert_eq!(output.value_count(), 3);
    assert_eq!(mutable_data(&mut output).append, 8);
    assert_eq!(mutable_data(&mut output).bytes().len(), 128);
    assert_eq!(&mutable_data(&mut output).bytes()[..8], &[42, 0, 0, 0, 9, 0, 0, 0]);
    assert!(mutable_data(&mut output).bytes()[8..].iter().all(|byte| *byte == 0));
    assert_eq!(output.reader().get_int(1, NullChecks::Enabled).unwrap(), 9);
    assert_eq!(output.reader().get_int(2, NullChecks::Enabled).unwrap(), 0);
    let output = output.finish().unwrap();
    assert_eq!(data(&output).append, 8);
    assert_eq!(storage(&output).validity.as_ref().unwrap().bytes.as_slice(), &[6, 0]);
    assert_eq!(storage(&output).validity.as_ref().unwrap().append, 3);
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn clear_zeros_initial_capacity_and_resets_all_cursors_without_reallocating() {
    let memory = memory();
    let mut output = int(true, 9, &memory);
    output.set_int(8, 71).unwrap();
    output.write_int(42).unwrap();
    let retained = memory.used_bytes();
    output.clear();
    assert_eq!(output.value_count(), 0);
    assert_eq!(memory.used_bytes(), retained);
    assert_eq!(mutable_data(&mut output).bytes(), &[0; 64]);
    assert_eq!(mutable_data(&mut output).append, 0);
    let bits = output.fixture_state().validity.as_ref().unwrap();
    assert_eq!(bits.bytes(), &[0; 2]);
    assert_eq!((bits.append, bits.byte_append()), (0, 0));
    output.write_int(11).unwrap();
    assert_eq!(output.value_count(), 1);
    assert_eq!(output.reader().get_int(0, NullChecks::Enabled).unwrap(), 11);
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn initial_allocations_admit_metadata_then_data_then_overlapping_validity() {
    let h = header();
    for (limit, peak) in [(h - 1, 0), (h + 131, h), (h + 132 + 129, h + 132)] {
        let memory = MemoryBudget::new(limit);
        native(ScalarWriter::source_int_arrow19_default("n", true, 1, Arc::clone(&memory)), "rtdb/memory-budget-exceeded");
        assert_eq!(memory.used_bytes(), 0);
        assert_eq!(memory.peak_bytes(), peak);
    }
    let exact = MemoryBudget::new(h + 132 + 130);
    let output = int(true, 1, &exact);
    assert_eq!(exact.used_bytes(), h + 132 + 130);
    assert_eq!(exact.peak_bytes(), h + 132 + 130);
    drop(output);
    assert_eq!(exact.used_bytes(), 0);
}

#[test]
fn failed_validity_construction_releases_data_and_all_ancestor_leases() {
    let h = header();
    let root = MemoryBudget::new(h + 132 + 129);
    let child = root.scope();
    native(VectorWriter::source_int_arrow19_default("n", true, 1, Arc::clone(&child)), "rtdb/memory-budget-exceeded");
    assert_eq!(root.used_bytes(), 0);
    assert_eq!(child.used_bytes(), 0);
    assert_eq!(child.peak_bytes(), h + 132);
    assert_eq!(root.peak_bytes(), h + 132);
    drop(child);
    assert_eq!(root.used_bytes(), 0);
}

#[test]
fn positive_maximum_count_is_a_native_resource_request_not_a_wrapped_source_error() {
    let h = header();
    for nullable in [false, true] {
        let memory = MemoryBudget::new(h + 4096);
        native(ScalarWriter::source_int_arrow19_default("n", nullable, i32::MAX, Arc::clone(&memory)), "rtdb/memory-budget-exceeded");
        assert_eq!(memory.used_bytes(), 0);
        assert_eq!(memory.peak_bytes(), h);
    }
    assert_eq!(buffer::arrow19_default_capacity(8_589_934_588).unwrap() as u64, 8_589_934_588);
    assert_eq!(buffer::arrow19_default_capacity(268_435_456).unwrap(), 268_435_456);
}

#[test]
fn pointer_facts_keep_signed_wrapping_without_logical_or_null_admission() {
    let memory = memory();
    let output = int(true, 1, &memory);
    for (row, offset, retained) in [(0, 0, true), (1, 4, false), (-1, -4, false), (i32::MIN, 0, true), (i32::MAX, -4, false), (1 << 30, 0, true)] {
        let pointer = output.source_int_pointer(row).unwrap().unwrap();
        assert_eq!((pointer.offset(), pointer.length()), (offset, 4));
        if retained { assert_eq!(pointer.demand_native().unwrap(), &[0; 4]); } else { unretained(pointer.demand_native()); }
    }
    assert!(output.reader().is_null(0).unwrap());
    assert!(output.reader().get_int(i32::MIN, NullChecks::Disabled).is_err());
    let output = output.finish().unwrap();
    for (row, offset, retained) in [(0, 0, true), (1, 4, false), (-1, -4, false), (i32::MIN, 0, true), (i32::MAX, -4, false), (1 << 30, 0, true)] {
        let pointer = SourceBytePointer::from_int_arrow19_default(&output, row).unwrap().unwrap();
        assert_eq!((pointer.offset(), pointer.length()), (offset, 4));
        if retained { assert_eq!(pointer.demand_native().unwrap(), &[0; 4]); } else { unretained(pointer.demand_native()); }
    }
    assert!(output.cell(1).is_err());
    assert!(SourceScalarReader::new(&output).unwrap().is_none());
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn rounded_padding_is_retained_for_raw_pointer_demand_beyond_logical_count() {
    let memory = memory();
    let output = int(false, 3, &memory).finish().unwrap();
    let pointer = SourceBytePointer::from_int_arrow19_default(&output, 3).unwrap().unwrap();
    assert_eq!((pointer.offset(), pointer.length()), (12, 4));
    assert_eq!(pointer.demand_native().unwrap(), &[0; 4]);
    assert!(output.cell(3).is_err());
    unretained(SourceBytePointer::from_int_arrow19_default(&output, 4).unwrap().unwrap().demand_native());
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn int_pointer_factory_requires_counted_native_identity_and_concrete_class() {
    let memory = memory();
    let input = int(false, 1, &memory).finish().unwrap();
    assert!(SourceBytePointer::from_int_arrow19_default(&input, 0).unwrap().is_some());
    for selection in [Selection::All, Selection::Range { offset: 0, length: 1 }, Selection::Indices(vec![0])] {
        let selected = input.select(&selection).unwrap();
        assert!(SourceBytePointer::from_int_arrow19_default(&selected, 0).unwrap().is_none());
    }
    let nothing = null(false, -1, &memory).finish().unwrap();
    assert!(SourceBytePointer::from_int_arrow19_default(&nothing, 0).unwrap().is_none());
    let descriptor = VectorDescriptor::source_scalar("n", false, SourceScalarDeclaration::FixedBinary(4), &memory).unwrap();
    let parameterized = ScalarWriter::from_descriptor(&descriptor, Arc::clone(&memory)).unwrap().finish().unwrap();
    assert!(SourceScalarReader::new(&parameterized).unwrap().is_some());
    assert!(SourceBytePointer::from_int_arrow19_default(&parameterized, 0).unwrap().is_none());
    let sdk = ScalarWriter::new(Arc::new(Field::new("n", DataType::Int32, false)), Arc::clone(&memory)).unwrap().finish().unwrap();
    assert!(SourceBytePointer::from_int_arrow19_default(&sdk, 0).unwrap().is_none());
    let mut ends = int(false, 1, &memory);
    ends.set_int(0, 1).unwrap();
    let runs = publish_native("runs", SourceCount::new(1), NativeContainerParts::RunEnd { ends: ends.finish().unwrap(), values: input.clone() }, &memory).unwrap();
    assert!(SourceBytePointer::from_int_arrow19_default(&runs, 0).unwrap().is_none());
    let mut children = CapturedChildren::new(1, &memory).unwrap();
    children.push(input.clone()).unwrap();
    let union = publish_native("u", SourceCount::new(1), NativeContainerParts::Union {
        type_ids: stored(&[0], 0, &memory), offsets: stored(&0i32.to_le_bytes(), 0, &memory), children,
    }, &memory).unwrap();
    assert!(SourceBytePointer::from_int_arrow19_default(&union, 0).unwrap().is_none());
    let leg = union.vector_for("n").unwrap().unwrap();
    assert!(SourceBytePointer::from_int_arrow19_default(&leg, 0).unwrap().is_none());
    drop((leg, union, runs, sdk, parameterized, descriptor, nothing, input));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn matched_int_provenance_with_malformed_storage_is_native_layout_failure() {
    let memory = memory();
    let template = int(false, 1, &memory).finish().unwrap();
    let malformed = fixture(Arc::clone(&template.node().descriptor), 1, NodeLayout::Scalar(NativeScalarStorage { validity: None, data: NativeScalarData::Null }), &memory);
    assert!(matches!(SourceBytePointer::from_int_arrow19_default(&malformed, 0), Err(error) if error.code == "rtdb/invalid-arrow"));
    let mut output = int(false, 1, &memory);
    output.fixture_state().data = MutableScalarData::Fixed { width: 8, data: MutableBytes::new(&memory) };
    assert!(matches!(output.source_int_pointer(0), Err(error) if error.code == "rtdb/invalid-arrow"));
    output.fixture_state().data = MutableScalarData::Null;
    assert!(matches!(output.source_int_pointer(0), Err(error) if error.code == "rtdb/invalid-arrow"));
    drop((output, malformed, template));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn counted_descriptor_rebuilds_zero_state_in_the_target_budget() {
    let source = memory();
    for constructor_class in [CountScalarClass::Null, CountScalarClass::IntArrow19Default] {
        for nullable in [false, true] {
            let input = match constructor_class { CountScalarClass::Null => null(nullable, -7, &source), CountScalarClass::IntArrow19Default => int(nullable, 17, &source) }.finish().unwrap();
            let descriptor = Arc::clone(&input.node().descriptor);
            let source_retained = source.used_bytes();
            let target = MemoryBudget::new(header());
            let mut output = ScalarWriter::from_descriptor(&descriptor, Arc::clone(&target)).unwrap();
            assert_eq!(source.used_bytes(), source_retained);
            assert_eq!(target.used_bytes(), header());
            assert_eq!(output.value_count(), 0);
            assert_eq!(output.field(), descriptor.scalar_field().unwrap());
            assert!(output.source_reader().unwrap().is_none());
            if constructor_class == CountScalarClass::IntArrow19Default {
                assert!(mutable_data(&mut output).bytes().is_empty());
                assert_eq!(mutable_data(&mut output).append, 0);
                unretained(output.source_int_pointer(0).unwrap().unwrap().demand_native());
                if let Some(bits) = &output.fixture_state().validity {
                    assert!(bits.bytes().is_empty());
                    assert_eq!((bits.append, bits.byte_append()), (0, 0));
                }
            } else {
                assert!(output.fixture_state().validity.is_none());
                assert!(output.source_int_pointer(0).unwrap().is_none());
            }
            let output = output.finish().unwrap();
            assert_eq!(count_class(&output), constructor_class);
            assert_eq!(output.value_count(), 0);
            drop((output, input, descriptor));
            assert_eq!(source.used_bytes(), 0);
            assert_eq!(target.used_bytes(), 0);
        }
    }
}

#[test]
fn descriptor_rebuild_failure_preserves_the_input_and_releases_target_admission() {
    let source = memory();
    let input = int(true, 9, &source).finish().unwrap();
    let retained = source.used_bytes();
    let target = MemoryBudget::new(header() - 1);
    assert!(matches!(ScalarWriter::from_descriptor(input.descriptor(), Arc::clone(&target)), Err(error) if error.code == "rtdb/memory-budget-exceeded"));
    native(VectorWriter::from_descriptor(input.descriptor(), Arc::clone(&target)), "rtdb/memory-budget-exceeded");
    assert_eq!(target.used_bytes(), 0);
    assert_eq!(source.used_bytes(), retained);
    assert_eq!(input.value_count(), 9);
    assert_eq!(data(&input).bytes.len(), 64);
    drop(input);
    assert_eq!(source.used_bytes(), 0);
}

#[test]
fn recursive_tree_rebuild_retains_counted_child_class_without_initial_rows() {
    let memory = memory();
    let input = int(true, 17, &memory).finish().unwrap();
    let child = Arc::clone(&input.node().descriptor);
    let fields = [StructFieldRef { key: "value", descriptor: &child }];
    let descriptor = VectorDescriptor::native_container("s", false, ContainerShapeRef::Struct(&fields), &memory).unwrap();
    let mut output = VectorWriter::from_descriptor(&descriptor, Arc::clone(&memory)).unwrap();
    assert_eq!(output.value_count(), 0);
    output.write_undefined().unwrap();
    let output = output.finish().unwrap();
    assert_eq!(output.value_count(), 1);
    let value = output.vector_for("value").unwrap().unwrap();
    assert_eq!(value.value_count(), 1);
    assert_eq!(count_class(&value), CountScalarClass::IntArrow19Default);
    assert_eq!(data(&value).append, 4);
    assert_eq!(data(&value).bytes.len(), 128);
    assert_eq!(storage(&value).validity.as_ref().unwrap().append, 1);
    assert!(value.cell(0).unwrap().is_null().unwrap());
    assert_eq!(input.value_count(), 17);
    assert_eq!(data(&input).append, 0);
    assert_eq!(data(&input).bytes.len(), 128);
    drop((value, output, descriptor, child, input));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn nullable_metadata_admission_failure_leaves_both_counted_classes_ready() {
    let memory = memory();
    for mut output in [null(false, 1, &memory), int(false, 1, &memory)] {
        let blocker = block(&memory, header() - 1);
        native(output.write_null(), "rtdb/memory-budget-exceeded");
        assert!(!output.field().is_nullable());
        assert_eq!(output.value_count(), 1);
        assert!(output.fixture_state().validity.is_none());
        output.ensure_capacity(1).unwrap();
        drop(blocker);
        output.write_null().unwrap();
        assert!(output.field().is_nullable());
        assert_eq!(output.value_count(), 2);
        drop(output);
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn null_promotion_finishes_after_metadata_swap_without_bitmap_admission() {
    let h = header();
    let memory = MemoryBudget::new(2 * h);
    let mut output = null(false, i32::MIN, &memory);
    output.write_null().unwrap();
    assert_eq!(output.value_count(), i32::MIN + 1);
    assert_eq!(output.logical(), &LogicalType::Null);
    assert!(output.field().is_nullable());
    assert!(output.fixture_state().validity.is_none());
    assert_eq!(memory.used_bytes(), h);
    assert_eq!(memory.peak_bytes(), 2 * h);
    output.write_undefined().unwrap();
    assert_eq!(output.value_count(), i32::MIN + 2);
    let output = output.finish().unwrap();
    assert!(storage(&output).validity.is_none());
    assert!(matches!(storage(&output).data, NativeScalarData::Null));
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn failed_large_validity_fill_keeps_the_installed_nullable_metadata_and_prefix() {
    let memory = memory();
    let mut output = int(false, 65_536, &memory);
    assert!(header() < 8192 + 128);
    let blocker = block(&memory, header());
    native(output.write_null(), "rtdb/memory-budget-exceeded");
    assert_eq!(output.value_count(), 65_536);
    assert!(output.field().is_nullable());
    assert_eq!(output.logical(), &LogicalType::Scalar(rtdb_types::ScalarType::I32).maybe());
    let bits = output.fixture_state().validity.as_ref().unwrap();
    assert!(bits.bytes().is_empty());
    assert_eq!((bits.append, bits.byte_append()), (0, 0));
    assert_eq!(output.source_int_pointer(0).unwrap().unwrap().demand_native().unwrap(), &[0; 4]);
    native(output.write_int(9), "rtdb/aborted-vector-writer");
    drop(blocker);
    output.clear();
    assert_eq!(output.value_count(), 0);
    output.write_int(9).unwrap();
    assert_eq!(output.reader().get_int(0, NullChecks::Enabled).unwrap(), 9);
    let output = output.finish().unwrap();
    assert_eq!(count_class(&output), CountScalarClass::IntArrow19Default);
    assert_eq!(data(&output).append, 4);
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn failed_validity_after_data_write_exposes_borrowed_prefix_without_recovering_gate() {
    let memory = memory();
    let mut output = int(true, 0, &memory);
    let blocker = block(&memory, 256);
    native(output.write_int(42), "rtdb/memory-budget-exceeded");
    assert_eq!(output.value_count(), 0);
    let pointer = output.source_int_pointer(0).unwrap().unwrap();
    assert_eq!(pointer.demand_native().unwrap(), &42i32.to_le_bytes());
    assert_eq!(mutable_data(&mut output).append, 4);
    assert!(output.reader().get_int(0, NullChecks::Disabled).is_err());
    native(output.write_undefined(), "rtdb/aborted-vector-writer");
    assert!(matches!(output.finish(), Err(error) if error.code == "rtdb/aborted-vector-writer"));
    assert_eq!(memory.used_bytes(), blocker.bytes());
    drop(blocker);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn typed_capture_keeps_full_native_storage_and_metadata_until_last_snapshot_owner() {
    let parent = memory();
    let source = parent.scope();
    let mut output = int(true, 3, &source);
    output.set_int(2, 42).unwrap();
    let retained = source.used_bytes();
    let output = output.finish().unwrap();
    assert_eq!(source.used_bytes(), retained);
    assert_eq!(parent.used_bytes(), retained);
    assert_eq!(output.value_count(), 3);
    assert_eq!(data(&output).bytes.len(), 16);
    assert_eq!(data(&output).append, 0);
    assert_eq!(storage(&output).validity.as_ref().unwrap().bytes.as_slice(), &[4, 0]);
    assert_eq!(storage(&output).validity.as_ref().unwrap().append, 0);
    let field = output.arrow_field().unwrap();
    assert_eq!(field.field(), &Field::new("n", DataType::Int32, true));
    assert!(crate::from_field(field.field()).is_ok());
    drop(field);
    let descriptor = Arc::clone(&output.node().descriptor);
    let clone = output.clone();
    let snapshot_memory = memory();
    let snapshot = output.snapshot_reader(&snapshot_memory).unwrap();
    assert_eq!(snapshot.value_count(), 3);
    assert_eq!(count_class(&snapshot), CountScalarClass::IntArrow19Default);
    assert_eq!(snapshot.cell(2).unwrap().get_int(NullChecks::Enabled).unwrap(), 42);
    assert_eq!(SourceBytePointer::from_int_arrow19_default(&snapshot, 2).unwrap().unwrap().demand_native().unwrap(), &42i32.to_le_bytes());
    drop((output, clone, descriptor));
    assert_eq!(source.used_bytes(), retained);
    assert!(snapshot_memory.used_bytes() > 0);
    drop(snapshot);
    assert_eq!(source.used_bytes(), 0);
    assert_eq!(parent.used_bytes(), 0);
    assert_eq!(snapshot_memory.used_bytes(), 0);
}

#[test]
fn typed_capture_rejects_mismatched_closed_payloads_and_releases_all_leases() {
    let memory = memory();
    let metadata = CountScalarMetadata::new("n", true, CountScalarClass::Null, &memory).unwrap();
    let payload = CountScalarPayload::Int { validity: None, bytes: stored(&[0; 4], 0, &memory) };
    assert_eq!(finish_count_scalar(metadata, SourceCount::new(1), payload).unwrap_err().code, "rtdb/invalid-arrow");
    assert_eq!(memory.used_bytes(), 0);
    let metadata = CountScalarMetadata::new("n", false, CountScalarClass::IntArrow19Default, &memory).unwrap();
    assert_eq!(finish_count_scalar(metadata, SourceCount::new(1), CountScalarPayload::Null).unwrap_err().code, "rtdb/invalid-arrow");
    assert_eq!(memory.used_bytes(), 0);
    let metadata = CountScalarMetadata::new("n", true, CountScalarClass::IntArrow19Default, &memory).unwrap();
    let payload = CountScalarPayload::Int { validity: None, bytes: stored(&[0; 4], 0, &memory) };
    assert_eq!(finish_count_scalar(metadata, SourceCount::new(1), payload).unwrap_err().code, "rtdb/invalid-arrow");
    assert_eq!(memory.used_bytes(), 0);
    let metadata = CountScalarMetadata::new("n", false, CountScalarClass::IntArrow19Default, &memory).unwrap();
    let bits = stored(&[0; 2], 0, &memory);
    let payload = CountScalarPayload::Int { validity: Some(StoredBits { bytes: bits.bytes, append: 0, byte_append: 0 }), bytes: stored(&[0; 4], 0, &memory) };
    assert_eq!(finish_count_scalar(metadata, SourceCount::new(1), payload).unwrap_err().code, "rtdb/invalid-arrow");
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn copier_uses_retained_native_padding_for_rows_selections_and_ranges() {
    let memory = memory();
    let mut input = int(false, 3, &memory);
    input.set_int(0, 11).unwrap();
    input.set_int(1, 22).unwrap();
    input.set_long(1, i64::from_le_bytes([33, 0, 0, 0, 44, 0, 0, 0])).unwrap();
    assert_eq!(input.value_count(), 3);
    let input = input.finish().unwrap();
    assert_eq!(data(&input).bytes.len(), 16);
    assert!(input.cell(3).is_err());
    let mut output = int(false, 0, &memory);
    ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap().copy_rows(&[3, 1, 0]).unwrap();
    assert_eq!(output.value_count(), 3);
    assert_eq!(output.reader().get_int(0, NullChecks::Enabled).unwrap(), 44);
    assert_eq!(output.reader().get_int(1, NullChecks::Enabled).unwrap(), 22);
    assert_eq!(output.reader().get_int(2, NullChecks::Enabled).unwrap(), 11);
    let mut range = int(false, 0, &memory);
    ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut range).unwrap().copy_range(0, 4).unwrap();
    assert_eq!(range.value_count(), 4);
    for (row, expected) in [11, 22, 33, 44].into_iter().enumerate() {
        assert_eq!(range.reader().get_int(row as i32, NullChecks::Enabled).unwrap(), expected);
    }
    let output = output.finish().unwrap();
    let range = range.finish().unwrap();
    assert_eq!(data(&output).append, 12);
    assert_eq!(data(&range).append, 16);
    assert_eq!(count_class(&output), CountScalarClass::IntArrow19Default);
    assert_eq!(count_class(&range), CountScalarClass::IntArrow19Default);
    drop((output, range, input));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn copied_initial_destination_count_does_not_shift_its_data_cursor() {
    let memory = memory();
    let mut input = int(false, 1, &memory);
    input.set_int(0, 42).unwrap();
    let input = input.finish().unwrap();
    let mut output = int(false, 1, &memory);
    let retained = memory.used_bytes();
    ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap().copy_row(0).unwrap();
    assert_eq!(memory.used_bytes(), retained);
    assert_eq!(output.value_count(), 2);
    assert_eq!(mutable_data(&mut output).bytes(), &42i32.to_le_bytes());
    assert_eq!(mutable_data(&mut output).append, 4);
    unretained(output.source_int_pointer(1).unwrap().unwrap().demand_native());
    let output = output.finish().unwrap();
    assert_eq!(output.value_count(), 2);
    assert_eq!(data(&output).bytes.len(), 4);
    assert_eq!(data(&output).append, 4);
    drop((output, input));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_copy_source_bounds_preserve_validity_prefix_without_aborting() {
    let memory = memory();
    let mut input = int(true, 1, &memory);
    input.set_int(0, 42).unwrap();
    let input = input.finish().unwrap();
    let mut output = int(true, 0, &memory);
    reference(ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap().copy_row(1), ReferenceFailure::Bounds);
    assert_eq!(output.value_count(), 0);
    assert_eq!(mutable_data(&mut output).append, 0);
    assert_eq!(mutable_data(&mut output).bytes(), &[0; 128]);
    let bits = output.fixture_state().validity.as_ref().unwrap();
    assert_eq!((bits.append, bits.byte_append()), (1, 0));
    assert!(bits.bytes().iter().all(|byte| *byte == 0));
    ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap().copy_row(0).unwrap();
    assert_eq!(output.value_count(), 1);
    assert_eq!(output.source_int_pointer(0).unwrap().unwrap().demand_native().unwrap(), &42i32.to_le_bytes());
    assert!(output.reader().is_null(0).unwrap());
    assert_eq!(output.reader().get_int(0, NullChecks::Disabled).unwrap(), 42);
    let output = output.finish().unwrap();
    assert_eq!(data(&output).append, 4);
    assert_eq!(storage(&output).validity.as_ref().unwrap().append, 2);
    assert_eq!(storage(&output).validity.as_ref().unwrap().bytes.as_slice()[0], 2);
    drop((output, input));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn zero_extent_native_copy_still_demands_the_wrapped_source_offset() {
    let memory = memory();
    let input = int(true, 0, &memory).finish().unwrap();
    let mut output = int(true, 0, &memory);
    let retained = memory.used_bytes();
    reference(ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap().copy_range(i32::MAX, 0), ReferenceFailure::Bounds);
    ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap().copy_range(1 << 30, 0).unwrap();
    assert_eq!(output.value_count(), 0);
    assert!(mutable_data(&mut output).bytes().is_empty());
    assert_eq!(mutable_data(&mut output).append, 0);
    let bits = output.fixture_state().validity.as_ref().unwrap();
    assert!(bits.bytes().is_empty());
    assert_eq!((bits.append, bits.byte_append()), (0, 0));
    assert_eq!(memory.used_bytes(), retained);
    drop((output, input));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sdk_sliced_backing_keeps_strict_row_admission_and_native_failure_gate() {
    let memory = memory();
    let array: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![11, 22, 33, 44]).slice(0, 3));
    let reservation = memory.reserve(array.get_array_memory_size() + 1024).unwrap();
    let input = ExecutionVector::with_reservation(Arc::new(Field::new("n", DataType::Int32, false)), array, reservation).unwrap();
    assert_eq!(input.value_count(), 3);
    assert!(SourceBytePointer::from_int_arrow19_default(&input, 0).unwrap().is_none());
    let mut output = int(false, 0, &memory);
    native(ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap().copy_row(3), "rtdb/row-index");
    assert_eq!(output.value_count(), 0);
    assert_eq!(mutable_data(&mut output).append, 0);
    native(output.write_int(9), "rtdb/aborted-vector-writer");
    output.clear();
    ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap().copy_row(2).unwrap();
    assert_eq!(output.reader().get_int(0, NullChecks::Enabled).unwrap(), 33);
    drop((output, input));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn null_copier_ignores_negative_initial_count_and_out_of_count_source_rows() {
    let memory = memory();
    let input = null(false, i32::MIN, &memory).finish().unwrap();
    let mut output = int(false, 0, &memory);
    ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap().copy_rows(&[i32::MIN, -1, i32::MAX]).unwrap();
    assert_eq!(output.value_count(), 3);
    assert!(output.field().is_nullable());
    for row in 0..3 { assert!(output.reader().is_null(row).unwrap()); }
    assert_eq!(mutable_data(&mut output).append, 12);
    assert!(mutable_data(&mut output).bytes().iter().all(|byte| *byte == 0));
    drop((output, input));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn scalar_and_root_fresh_constructors_publish_equivalent_concrete_state() {
    let memory = memory();
    for nullable in [false, true] {
        for count in [0, 1, 3, 9] {
            let scalar = int(nullable, count, &memory).finish().unwrap();
            let root = VectorWriter::source_int_arrow19_default("n", nullable, count, Arc::clone(&memory)).unwrap().finish().unwrap();
            assert_eq!(root.value_count(), scalar.value_count());
            assert_eq!(root.descriptor().scalar_field(), scalar.descriptor().scalar_field());
            assert_eq!(root.logical_type(), scalar.logical_type());
            assert_eq!(count_class(&root), CountScalarClass::IntArrow19Default);
            assert_eq!(data(&root).bytes.as_slice(), data(&scalar).bytes.as_slice());
            assert_eq!(data(&root).append, data(&scalar).append);
            assert_eq!(storage(&root).validity.as_ref().map(|bits| (bits.bytes.as_slice(), bits.append, bits.byte_append)), storage(&scalar).validity.as_ref().map(|bits| (bits.bytes.as_slice(), bits.append, bits.byte_append)));
        }
        for count in [i32::MIN, -1, 0, i32::MAX] {
            let scalar = null(nullable, count, &memory).finish().unwrap();
            let root = VectorWriter::source_null("n", nullable, count, Arc::clone(&memory)).unwrap().finish().unwrap();
            assert_eq!(root.value_count(), scalar.value_count());
            assert_eq!(root.logical_type(), scalar.logical_type());
            assert_eq!(count_class(&root), CountScalarClass::Null);
            assert!(matches!(storage(&root).data, NativeScalarData::Null));
            assert!(storage(&root).validity.is_none());
        }
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn root_scalar_methods_keep_count_cursor_order_and_clear_the_single_gate() {
    let memory = memory();
    let mut output = VectorWriter::source_int_arrow19_default("n", true, 1, Arc::clone(&memory)).unwrap();
    output.write_int(42).unwrap();
    assert_eq!(output.value_count(), 2);
    let output = output.finish().unwrap();
    assert_eq!(data(&output).bytes.as_slice(), &42i32.to_le_bytes());
    assert_eq!(data(&output).append, 4);
    assert_eq!(storage(&output).validity.as_ref().unwrap().bytes.as_slice(), &[2, 0]);
    let mut root = VectorWriter::source_int_arrow19_default("n", true, 0, Arc::clone(&memory)).unwrap();
    let blocker = block(&memory, 256);
    native(root.write_int(9), "rtdb/memory-budget-exceeded");
    assert_eq!(root.value_count(), 0);
    native(root.write_undefined(), "rtdb/aborted-vector-writer");
    drop(blocker);
    root.clear();
    root.write_int(11).unwrap();
    let root = root.finish().unwrap();
    assert_eq!(root.value_count(), 1);
    assert_eq!(root.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 11);
    let mut bottom = VectorWriter::source_null("n", false, i32::MAX, Arc::clone(&memory)).unwrap();
    bottom.write_undefined().unwrap();
    assert_eq!(bottom.value_count(), i32::MIN);
    bottom.write_null().unwrap();
    assert_eq!(bottom.value_count(), i32::MIN + 1);
    assert!(bottom.nullable());
    let bottom = bottom.finish().unwrap();
    assert_eq!(bottom.logical_type().unwrap(), &LogicalType::Null);
    drop((bottom, root, output));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn root_descriptor_rebuild_is_fresh_and_does_not_replay_initial_capacity() {
    let source = memory();
    let input = int(true, 17, &source).finish().unwrap();
    let target = MemoryBudget::new(header());
    let output = VectorWriter::from_descriptor(input.descriptor(), Arc::clone(&target)).unwrap();
    assert_eq!(output.value_count(), 0);
    assert!(output.nullable());
    let output = output.finish().unwrap();
    assert_eq!(count_class(&output), CountScalarClass::IntArrow19Default);
    assert!(data(&output).bytes.as_slice().is_empty());
    assert_eq!(data(&output).append, 0);
    let bits = storage(&output).validity.as_ref().unwrap();
    assert!(bits.bytes.as_slice().is_empty());
    assert_eq!((bits.append, bits.byte_append), (0, 0));
    assert_eq!(input.value_count(), 17);
    assert_eq!(data(&input).bytes.len(), 128);
    drop((output, input));
    assert_eq!(source.used_bytes(), 0);
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn counted_capture_projects_strict_fields_without_changing_parameterized_ingress() {
    let memory = memory();
    let counted = int(false, 1, &memory).finish().unwrap();
    let field = counted.arrow_field().unwrap();
    assert_eq!(field.field(), &Field::new("n", DataType::Int32, false));
    assert!(crate::from_field(field.field()).is_ok());
    let descriptor = VectorDescriptor::source_scalar("t", false, SourceScalarDeclaration::Time32(rtdb_types::TimeUnit::Microsecond), &memory).unwrap();
    let parameterized = ScalarWriter::from_descriptor(&descriptor, Arc::clone(&memory)).unwrap().finish().unwrap();
    assert!(SourceScalarReader::new(&parameterized).unwrap().is_some());
    assert!(SourceBytePointer::from_int_arrow19_default(&parameterized, 0).unwrap().is_none());
    assert!(crate::from_field(&Field::new("t", DataType::Time32(arrow_schema::TimeUnit::Microsecond), false)).is_err());
    assert!(rtdb_types::ScalarType::Time { unit: rtdb_types::TimeUnit::Microsecond, bit_width: 32 }.validate().is_err());
    let mut output = int(false, 1, &memory);
    assert!(output.source_reader().unwrap().is_none());
    assert!(matches!(output.reader().get_bytes(0), Err(VectorReadError::Anomaly(error)) if error.code == "rtdb/unsupported-vector-reader"));
    assert_eq!(output.source_int_pointer(0).unwrap().unwrap().length(), 4);
    assert_eq!(mutable_data(&mut output).bytes().len(), 4);
    drop((output, parameterized, descriptor, field, counted));
    assert_eq!(memory.used_bytes(), 0);
}
