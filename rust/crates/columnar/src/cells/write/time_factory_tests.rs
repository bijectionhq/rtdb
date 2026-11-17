use super::*;
use super::owner::MutableScalarData;
use rtdb_types::{ScalarType, TimeUnit};

fn memory() -> Arc<MemoryBudget> { MemoryBudget::new(16 * 1024 * 1024) }
fn factory(unit: TimeUnit, raw_width: i32, nullable: bool, memory: &Arc<MemoryBudget>) -> ScalarWriter {
    ScalarWriter::source_time_factory("t", nullable, Some(unit), raw_width, Arc::clone(memory)).unwrap()
}
fn cases() -> [(TimeUnit, SourceScalarDeclaration, DataType, usize); 4] {
    [
        (TimeUnit::Second, SourceScalarDeclaration::Time32(TimeUnit::Second), DataType::Time32(arrow_schema::TimeUnit::Second), 4),
        (TimeUnit::Millisecond, SourceScalarDeclaration::Time32(TimeUnit::Millisecond), DataType::Time32(arrow_schema::TimeUnit::Millisecond), 4),
        (TimeUnit::Microsecond, SourceScalarDeclaration::Time64(TimeUnit::Microsecond), DataType::Time64(arrow_schema::TimeUnit::Microsecond), 8),
        (TimeUnit::Nanosecond, SourceScalarDeclaration::Time64(TimeUnit::Nanosecond), DataType::Time64(arrow_schema::TimeUnit::Nanosecond), 8),
    ]
}
fn header() -> usize {
    let memory = memory();
    let descriptor = VectorDescriptor::source_scalar("t", false, SourceScalarDeclaration::Time32(TimeUnit::Second), &memory).unwrap();
    let bytes = memory.used_bytes();
    drop(descriptor);
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
fn read_reference<T>(result: SourceScalarReadResult<T>, expected: SourceScalarReadFailure) {
    match result { Err(SourceScalarReadError::Reference(actual)) => assert_eq!(actual, expected), _ => panic!("expected source read failure") }
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
fn declaration(vector: &ExecutionVector) -> SourceScalarDeclaration { vector.descriptor().scalar().unwrap().source_declaration().unwrap() }
fn mutable_data(writer: &mut ScalarWriter) -> &MutableBytes {
    let MutableScalarData::Fixed { data, .. } = &writer.fixture_state().data else { panic!() };
    data
}

#[test]
fn time_factory_chooses_actual_class_by_unit_for_every_signed_declared_width() {
    let memory = memory();
    for (unit, expected, data_type, width) in cases() {
        for nullable in [false, true] {
            for raw_width in [i32::MIN, -1, 0, 1, 31, 32, 64, i32::MAX] {
                let mut output = factory(unit, raw_width, nullable, &memory);
                assert_eq!(output.value_count(), 0);
                assert_eq!(output.field(), &Field::new("t", data_type.clone(), nullable));
                let logical = LogicalType::Scalar(ScalarType::Time { unit, bit_width: (width * 8) as u8 });
                assert_eq!(output.logical(), &if nullable { logical.maybe() } else { logical });
                assert!(mutable_data(&mut output).bytes().is_empty());
                assert_eq!(mutable_data(&mut output).append, 0);
                assert_eq!(output.fixture_state().validity.is_some(), nullable);
                if let Some(bits) = &output.fixture_state().validity {
                    assert!(bits.bytes().is_empty());
                    assert_eq!((bits.append, bits.byte_append()), (0, 0));
                }
                let reader = output.source_reader().unwrap().unwrap();
                assert_eq!(reader.get_pointer(0).length(), width as i64);
                unretained(reader.get_pointer(0).demand_native());
                let output = output.finish().unwrap();
                assert_eq!(declaration(&output), expected);
                assert_eq!(output.value_count(), 0);
                assert_eq!(data(&output).append, 0);
                assert_eq!(data(&output).bytes.len(), 0);
                assert!(matches!(output.ingress_field(), DeclarationRef::Native(_)));
                assert!(SourceScalarReader::new(&output).unwrap().is_some());
                assert!(SourceBytePointer::from_int_arrow19_default(&output, 0).unwrap().is_none());
                drop(output);
                assert_eq!(memory.used_bytes(), 0);
            }
        }
    }
}

#[test]
fn absent_time_unit_is_reference_failure_before_any_scalar_or_root_admission() {
    for nullable in [false, true] {
        for raw_width in [i32::MIN, -1, 0, 1, 32, 64, i32::MAX] {
            for limit in [0, 1, 16 * 1024 * 1024] {
                let memory = MemoryBudget::new(limit);
                reference(ScalarWriter::source_time_factory("t", nullable, None, raw_width, Arc::clone(&memory)), ReferenceFailure::AbsentTimeUnit);
                reference(VectorWriter::source_time_factory("t", nullable, None, raw_width, Arc::clone(&memory)), ReferenceFailure::AbsentTimeUnit);
                assert_eq!(memory.used_bytes(), 0);
                assert_eq!(memory.peak_bytes(), 0);
            }
        }
    }
}

#[test]
fn empty_factory_construction_and_capture_share_the_same_exact_header_budget() {
    let h = header();
    for (unit, expected, _, _) in cases() {
        for nullable in [false, true] {
            let parent = MemoryBudget::new(h);
            let target = parent.scope();
            let output = factory(unit, i32::MIN, nullable, &target);
            assert_eq!(target.used_bytes(), h);
            assert_eq!(target.peak_bytes(), h);
            assert_eq!(parent.used_bytes(), h);
            let output = output.finish().unwrap();
            assert_eq!(declaration(&output), expected);
            assert_eq!(output.value_count(), 0);
            assert_eq!(target.used_bytes(), h);
            assert_eq!(target.peak_bytes(), h);
            assert_eq!(parent.peak_bytes(), h);
            assert!(data(&output).bytes.as_slice().is_empty());
            assert_eq!(storage(&output).validity.is_some(), nullable);
            drop(output);
            assert_eq!(target.used_bytes(), 0);
            assert_eq!(parent.used_bytes(), 0);
        }
    }
}

#[test]
fn root_constructor_and_capture_also_need_only_one_header_lease() {
    let h = header();
    for (unit, expected, _, _) in cases() {
        let target = MemoryBudget::new(h);
        let output = VectorWriter::source_time_factory("t", true, Some(unit), i32::MAX, Arc::clone(&target)).unwrap();
        assert_eq!(output.value_count(), 0);
        assert!(output.nullable());
        assert_eq!(target.used_bytes(), h);
        let output = output.finish().unwrap();
        assert_eq!(declaration(&output), expected);
        assert!(data(&output).bytes.as_slice().is_empty());
        let bits = storage(&output).validity.as_ref().unwrap();
        assert!(bits.bytes.as_slice().is_empty());
        assert_eq!((bits.append, bits.byte_append), (0, 0));
        assert_eq!(target.used_bytes(), h);
        assert_eq!(target.peak_bytes(), h);
        drop(output);
        assert_eq!(target.used_bytes(), 0);
    }
}

#[test]
fn nonnull_unit_below_one_header_fails_natively_without_publishing_an_owner() {
    let h = header();
    for (unit, _, _, _) in cases() {
        for nullable in [false, true] {
            for limit in [0, h - 1] {
                let parent = MemoryBudget::new(limit);
                let target = parent.scope();
                native(ScalarWriter::source_time_factory("t", nullable, Some(unit), -1, Arc::clone(&target)), "rtdb/memory-budget-exceeded");
                native(VectorWriter::source_time_factory("t", nullable, Some(unit), -1, Arc::clone(&target)), "rtdb/memory-budget-exceeded");
                assert_eq!(target.used_bytes(), 0);
                assert_eq!(target.peak_bytes(), 0);
                assert_eq!(parent.used_bytes(), 0);
                assert_eq!(parent.peak_bytes(), 0);
            }
        }
    }
}

#[test]
fn factory_normalization_keeps_direct_time_declarations_distinct() {
    let memory = memory();
    let value = 0x0000_0001_0000_0002i64;
    let mut factory32 = factory(TimeUnit::Second, 64, false, &memory);
    let direct64_descriptor = VectorDescriptor::source_scalar("t", false, SourceScalarDeclaration::Time64(TimeUnit::Second), &memory).unwrap();
    let mut direct64 = ScalarWriter::from_descriptor(&direct64_descriptor, Arc::clone(&memory)).unwrap();
    drop(direct64_descriptor);
    factory32.write_long(value).unwrap();
    direct64.write_long(value).unwrap();
    assert_eq!(factory32.source_reader().unwrap().unwrap().get_pointer(0).demand_native().unwrap(), &2i32.to_le_bytes());
    assert_eq!(direct64.source_reader().unwrap().unwrap().get_pointer(0).demand_native().unwrap(), &value.to_le_bytes());
    let factory32 = factory32.finish().unwrap();
    let direct64 = direct64.finish().unwrap();
    assert_eq!(declaration(&factory32), SourceScalarDeclaration::Time32(TimeUnit::Second));
    assert_eq!(declaration(&direct64), SourceScalarDeclaration::Time64(TimeUnit::Second));
    assert_eq!(data(&factory32).append, 4);
    assert_eq!(data(&direct64).append, 8);
    let mut factory64 = factory(TimeUnit::Microsecond, 32, false, &memory);
    let direct32_descriptor = VectorDescriptor::source_scalar("t", false, SourceScalarDeclaration::Time32(TimeUnit::Microsecond), &memory).unwrap();
    let mut direct32 = ScalarWriter::from_descriptor(&direct32_descriptor, Arc::clone(&memory)).unwrap();
    drop(direct32_descriptor);
    factory64.write_long(value).unwrap();
    direct32.write_long(value).unwrap();
    assert_eq!(factory64.source_reader().unwrap().unwrap().get_pointer(0).demand_native().unwrap(), &value.to_le_bytes());
    assert_eq!(direct32.source_reader().unwrap().unwrap().get_pointer(0).demand_native().unwrap(), &2i32.to_le_bytes());
    let factory64 = factory64.finish().unwrap();
    let direct32 = direct32.finish().unwrap();
    assert_eq!(declaration(&factory64), SourceScalarDeclaration::Time64(TimeUnit::Microsecond));
    assert_eq!(declaration(&direct32), SourceScalarDeclaration::Time32(TimeUnit::Microsecond));
    assert_eq!(data(&factory64).append, 8);
    assert_eq!(data(&direct32).append, 4);
    drop((factory32, direct64, factory64, direct32));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn selected_time_class_controls_writes_and_checked_primitive_methods() {
    let memory = memory();
    for unit in [TimeUnit::Second, TimeUnit::Millisecond] {
        let mut output = factory(unit, i32::MAX, false, &memory);
        output.write_long(0x0000_0001_0000_0002).unwrap();
        assert_eq!(output.value_count(), 1);
        assert_eq!(output.reader().get_int(0, NullChecks::Enabled).unwrap(), 2);
        assert!(matches!(output.reader().get_long(0, NullChecks::Enabled), Err(VectorReadError::Anomaly(error)) if error.code == "rtdb/unsupported-vector-reader"));
        output.write_int(9).unwrap();
        assert_eq!(mutable_data(&mut output).append, 8);
        assert_eq!(output.reader().get_int(1, NullChecks::Enabled).unwrap(), 9);
    }
    for unit in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
        let mut output = factory(unit, -1, false, &memory);
        let blocker = block(&memory, 0);
        reference(output.write_int(9), ReferenceFailure::Unsupported("writeInt"));
        assert_eq!(output.value_count(), 0);
        assert!(mutable_data(&mut output).bytes().is_empty());
        drop(blocker);
        output.write_long(0x0000_0001_0000_0002).unwrap();
        assert_eq!(mutable_data(&mut output).append, 8);
        assert_eq!(output.reader().get_long(0, NullChecks::Enabled).unwrap(), 0x0000_0001_0000_0002);
        assert!(matches!(output.reader().get_int(0, NullChecks::Enabled), Err(VectorReadError::Anomaly(error)) if error.code == "rtdb/unsupported-vector-reader"));
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn time_byte_facts_use_actual_width_and_defer_demand_while_get_bytes_is_unsupported() {
    let memory = memory();
    for (unit, expected, _, width) in cases() {
        let mut output = factory(unit, -1, false, &memory);
        let reader = output.source_reader().unwrap().unwrap();
        assert_eq!((reader.get_pointer(i32::MAX).offset(), reader.get_pointer(i32::MAX).length()), (-(width as i64), width as i64));
        assert_eq!(reader.get_pointer(i32::MIN).offset(), 0);
        unretained(reader.get_pointer(0).demand_native());
        read_reference(reader.get_bytes(i32::MIN), SourceScalarReadFailure::Unsupported { declaration: expected, method: "getBytes" });
        output.write_long(42).unwrap();
        let pointer = { let reader = output.source_reader().unwrap().unwrap(); reader.get_pointer(0) };
        assert_eq!(pointer.length(), width as i64);
        assert_eq!(pointer.demand_native().unwrap(), &42i64.to_le_bytes()[..width]);
        let output = output.finish().unwrap();
        let reader = SourceScalarReader::new(&output).unwrap().unwrap();
        assert_eq!(reader.get_pointer(0).demand_native().unwrap(), &42i64.to_le_bytes()[..width]);
        read_reference(reader.get_bytes(0), SourceScalarReadFailure::Unsupported { declaration: expected, method: "getBytes" });
        let selected = output.select(&Selection::Range { offset: 0, length: 1 }).unwrap();
        assert!(SourceScalarReader::new(&selected).unwrap().is_none());
        assert!(SourceBytePointer::from_int_arrow19_default(&output, 0).unwrap().is_none());
        drop((selected, output));
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn same_unit_factory_outputs_copy_by_actual_type_despite_different_raw_widths() {
    let memory = memory();
    for (unit, expected, _, width) in cases() {
        let mut input = factory(unit, i32::MIN, false, &memory);
        input.write_long(42).unwrap();
        let input = input.finish().unwrap();
        let mut output = factory(unit, i32::MAX, false, &memory);
        ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output).unwrap().copy_row(0).unwrap();
        assert_eq!(output.value_count(), 1);
        assert_eq!(output.source_reader().unwrap().unwrap().get_pointer(0).demand_native().unwrap(), &42i64.to_le_bytes()[..width]);
        let output = output.finish().unwrap();
        assert_eq!(declaration(&input), expected);
        assert_eq!(declaration(&output), expected);
        assert_eq!(data(&output).append, width as i64);
        drop((output, input));
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn copy_rejects_different_actual_units_and_direct_mismatch_before_mutation() {
    let memory = memory();
    let mut input = factory(TimeUnit::Second, 64, false, &memory);
    input.write_long(42).unwrap();
    let input = input.finish().unwrap();
    let mut output = factory(TimeUnit::Millisecond, 64, false, &memory);
    let blocker = block(&memory, 0);
    reference(ScalarCopier::new(ScalarSource::new(&input).unwrap(), &mut output), ReferenceFailure::InvalidCopySource);
    assert_eq!(output.value_count(), 0);
    assert!(mutable_data(&mut output).bytes().is_empty());
    output.ensure_capacity(0).unwrap();
    drop(blocker);
    output.write_long(9).unwrap();
    assert_eq!(output.reader().get_int(0, NullChecks::Enabled).unwrap(), 9);
    let descriptor = VectorDescriptor::source_scalar("t", false, SourceScalarDeclaration::Time64(TimeUnit::Second), &memory).unwrap();
    let mut direct = ScalarWriter::from_descriptor(&descriptor, Arc::clone(&memory)).unwrap();
    direct.write_long(42).unwrap();
    let direct = direct.finish().unwrap();
    let mut normalized = factory(TimeUnit::Second, 64, false, &memory);
    let blocker = block(&memory, 0);
    reference(ScalarCopier::new(ScalarSource::new(&direct).unwrap(), &mut normalized), ReferenceFailure::InvalidCopySource);
    assert_eq!(normalized.value_count(), 0);
    assert!(mutable_data(&mut normalized).bytes().is_empty());
    drop(blocker);
    normalized.write_long(11).unwrap();
    assert_eq!(normalized.reader().get_int(0, NullChecks::Enabled).unwrap(), 11);
    drop((normalized, direct, descriptor, output, input));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn canonical_factory_projection_does_not_relax_invalid_sdk_time_fields() {
    let memory = memory();
    for (unit, raw_width, expected_type, invalid_type) in [
        (TimeUnit::Second, 64, DataType::Time32(arrow_schema::TimeUnit::Second), DataType::Time64(arrow_schema::TimeUnit::Second)),
        (TimeUnit::Microsecond, 32, DataType::Time64(arrow_schema::TimeUnit::Microsecond), DataType::Time32(arrow_schema::TimeUnit::Microsecond)),
    ] {
        let output = factory(unit, raw_width, true, &memory).finish().unwrap();
        let field = output.arrow_field().unwrap();
        assert_eq!(field.field(), &Field::new("t", expected_type, true));
        assert!(field.field().metadata().is_empty());
        assert!(crate::from_field(field.field()).is_ok());
        assert!(matches!(output.ingress_field(), DeclarationRef::Native(_)));
        let invalid = Field::new("t", invalid_type, true);
        assert_eq!(crate::from_field(&invalid).unwrap_err().code, "xtdb/invalid-type");
        assert!(matches!(ScalarWriter::new(Arc::new(invalid), Arc::clone(&memory)), Err(error) if error.code == "xtdb/invalid-type"));
        drop((field, output));
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn nullable_promotion_requires_metadata_overlap_and_keeps_actual_factory_class() {
    let memory = memory();
    for (unit, expected, _, width) in cases() {
        let mut output = factory(unit, -1, false, &memory);
        output.write_long(42).unwrap();
        let blocker = block(&memory, header() - 1);
        native(output.write_null(), "rtdb/memory-budget-exceeded");
        assert_eq!(output.value_count(), 1);
        assert!(!output.field().is_nullable());
        assert!(output.fixture_state().validity.is_none());
        assert_eq!(output.source_reader().unwrap().unwrap().get_pointer(0).demand_native().unwrap(), &42i64.to_le_bytes()[..width]);
        output.ensure_capacity(1).unwrap();
        drop(blocker);
        output.write_null().unwrap();
        assert_eq!(output.value_count(), 2);
        assert!(output.field().is_nullable());
        assert!(!output.reader().is_null(0).unwrap());
        assert!(output.reader().is_null(1).unwrap());
        assert_eq!(mutable_data(&mut output).append, (2 * width) as i64);
        let output = output.finish().unwrap();
        assert_eq!(declaration(&output), expected);
        assert_eq!(storage(&output).validity.as_ref().unwrap().append, 2);
        assert_eq!(storage(&output).validity.as_ref().unwrap().bytes.as_slice()[0], 1);
        drop(output);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn nullable_first_write_failure_keeps_actual_payload_before_count_and_blocks_capture() {
    let memory = memory();
    for (unit, expected, _, width) in cases() {
        let mut output = factory(unit, i32::MIN, true, &memory);
        let blocker = block(&memory, 256);
        native(output.write_long(42), "rtdb/memory-budget-exceeded");
        assert_eq!(output.value_count(), 0);
        assert_eq!(mutable_data(&mut output).append, width as i64);
        assert!(output.fixture_state().validity.as_ref().unwrap().bytes().is_empty());
        let reader = output.source_reader().unwrap().unwrap();
        assert_eq!(reader.get_pointer(0).demand_native().unwrap(), &42i64.to_le_bytes()[..width]);
        read_reference(reader.get_bytes(0), SourceScalarReadFailure::Unsupported { declaration: expected, method: "getBytes" });
        assert!(output.reader().get_int(0, NullChecks::Disabled).is_err());
        native(output.write_undefined(), "rtdb/aborted-vector-writer");
        assert!(matches!(output.finish(), Err(error) if error.code == "rtdb/aborted-vector-writer"));
        assert_eq!(memory.used_bytes(), blocker.bytes());
        drop(blocker);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn clear_recovers_after_post_data_failure_and_resets_both_empty_validity_cursors() {
    let memory = memory();
    for (unit, expected, _, width) in cases() {
        let mut output = factory(unit, 0, true, &memory);
        let blocker = block(&memory, 256);
        native(output.write_long(42), "rtdb/memory-budget-exceeded");
        native(output.write_null(), "rtdb/aborted-vector-writer");
        assert_eq!(output.source_reader().unwrap().unwrap().get_pointer(0).length(), width as i64);
        drop(blocker);
        output.clear();
        assert_eq!(output.value_count(), 0);
        assert_eq!(mutable_data(&mut output).append, 0);
        assert!(mutable_data(&mut output).bytes().iter().all(|byte| *byte == 0));
        let bits = output.fixture_state().validity.as_ref().unwrap();
        assert_eq!((bits.append, bits.byte_append()), (0, 0));
        output.write_long(9).unwrap();
        assert_eq!(output.value_count(), 1);
        assert!(!output.reader().is_null(0).unwrap());
        let output = output.finish().unwrap();
        assert_eq!(declaration(&output), expected);
        assert_eq!(data(&output).append, width as i64);
        assert_eq!(storage(&output).validity.as_ref().unwrap().append, 1);
        assert_eq!(storage(&output).validity.as_ref().unwrap().byte_append, 0);
        drop(output);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn factory_capture_clones_and_snapshot_retain_actual_state_and_source_leases() {
    let parent = memory();
    for (unit, expected, _, width) in cases() {
        let source = parent.scope();
        let mut output = factory(unit, i32::MAX, true, &source);
        output.write_long(42).unwrap();
        let retained = source.used_bytes();
        let output = output.finish().unwrap();
        assert_eq!(source.used_bytes(), retained);
        assert_eq!(declaration(&output), expected);
        assert_eq!(data(&output).append, width as i64);
        assert_eq!(storage(&output).validity.as_ref().unwrap().append, 1);
        let descriptor = Arc::clone(&output.node().descriptor);
        let clone = output.clone();
        let snapshot_memory = memory();
        let snapshot = output.snapshot_reader(&snapshot_memory).unwrap();
        assert_eq!(snapshot.value_count(), 1);
        assert_eq!(declaration(&snapshot), expected);
        assert_eq!(data(&snapshot).append, width as i64);
        assert_eq!(SourceScalarReader::new(&snapshot).unwrap().unwrap().get_pointer(0).demand_native().unwrap(), &42i64.to_le_bytes()[..width]);
        drop((output, clone, descriptor));
        assert_eq!(source.used_bytes(), retained);
        assert_eq!(parent.used_bytes(), retained);
        assert!(snapshot_memory.used_bytes() > 0);
        drop(snapshot);
        assert_eq!(source.used_bytes(), 0);
        assert_eq!(parent.used_bytes(), 0);
        assert_eq!(snapshot_memory.used_bytes(), 0);
    }
}

#[test]
fn target_descriptor_rebuild_uses_actual_class_and_fresh_zero_cursors() {
    let source = memory();
    let h = header();
    for (unit, expected, _, width) in cases() {
        let mut input = factory(unit, -1, true, &source);
        input.write_long(42).unwrap();
        let input = input.finish().unwrap();
        let retained = source.used_bytes();
        let target = MemoryBudget::new(h);
        let mut output = ScalarWriter::from_descriptor(input.descriptor(), Arc::clone(&target)).unwrap();
        assert_eq!(source.used_bytes(), retained);
        assert_eq!(target.used_bytes(), h);
        assert_eq!(output.value_count(), 0);
        assert!(mutable_data(&mut output).bytes().is_empty());
        assert_eq!(mutable_data(&mut output).append, 0);
        let bits = output.fixture_state().validity.as_ref().unwrap();
        assert!(bits.bytes().is_empty());
        assert_eq!((bits.append, bits.byte_append()), (0, 0));
        assert_eq!(output.source_reader().unwrap().unwrap().get_pointer(0).length(), width as i64);
        let output = output.finish().unwrap();
        assert_eq!(declaration(&output), expected);
        assert_eq!(target.used_bytes(), h);
        assert_eq!(target.peak_bytes(), h);
        drop((output, input));
        assert_eq!(source.used_bytes(), 0);
        assert_eq!(target.used_bytes(), 0);
    }
}

#[test]
fn root_and_scalar_factory_paths_share_actual_class_count_and_cursor_order() {
    let memory = memory();
    for (unit, expected, _, width) in cases() {
        for nullable in [false, true] {
            let mut scalar = factory(unit, i32::MIN, nullable, &memory);
            let mut root = VectorWriter::source_time_factory("t", nullable, Some(unit), i32::MAX, Arc::clone(&memory)).unwrap();
            scalar.write_long(42).unwrap();
            root.write_long(42).unwrap();
            assert_eq!(root.value_count(), scalar.value_count());
            assert_eq!(root.value_count(), 1);
            assert_eq!(root.nullable(), nullable);
            let scalar = scalar.finish().unwrap();
            let root = root.finish().unwrap();
            assert_eq!(declaration(&scalar), expected);
            assert_eq!(declaration(&root), expected);
            assert_eq!(root.descriptor().scalar_field(), scalar.descriptor().scalar_field());
            assert_eq!(root.logical_type(), scalar.logical_type());
            assert_eq!(data(&root).bytes.as_slice(), data(&scalar).bytes.as_slice());
            assert_eq!(data(&root).append, width as i64);
            assert_eq!(storage(&root).validity.as_ref().map(|bits| (bits.bytes.as_slice(), bits.append, bits.byte_append)), storage(&scalar).validity.as_ref().map(|bits| (bits.bytes.as_slice(), bits.append, bits.byte_append)));
        }
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn root_factory_rebuild_and_native_failure_keep_one_publication_gate() {
    let memory = memory();
    let h = header();
    for (unit, expected, _, width) in cases() {
        let mut output = VectorWriter::source_time_factory("t", true, Some(unit), 0, Arc::clone(&memory)).unwrap();
        let blocker = block(&memory, 256);
        native(output.write_long(42), "rtdb/memory-budget-exceeded");
        assert_eq!(output.value_count(), 0);
        native(output.write_undefined(), "rtdb/aborted-vector-writer");
        drop(blocker);
        output.clear();
        output.write_long(9).unwrap();
        let output = output.finish().unwrap();
        assert_eq!(output.value_count(), 1);
        assert_eq!(declaration(&output), expected);
        assert_eq!(data(&output).append, width as i64);
        let target = MemoryBudget::new(h);
        let rebuilt = VectorWriter::from_descriptor(output.descriptor(), Arc::clone(&target)).unwrap();
        assert_eq!(rebuilt.value_count(), 0);
        assert!(rebuilt.nullable());
        let rebuilt = rebuilt.finish().unwrap();
        assert_eq!(declaration(&rebuilt), expected);
        assert!(data(&rebuilt).bytes.as_slice().is_empty());
        assert_eq!(data(&rebuilt).append, 0);
        assert_eq!(target.peak_bytes(), h);
        drop((rebuilt, output));
        assert_eq!(target.used_bytes(), 0);
        assert_eq!(memory.used_bytes(), 0);
    }
}
