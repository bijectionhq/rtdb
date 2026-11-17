use super::*;
use super::tree::*;
use super::super::super::descriptor::{ContainerShapeRef, StructFieldRef};
use super::super::super::native_container::{NativeContainerStorage, NativeStructInput};
use arrow_buffer::Buffer;
use arrow_schema::{UnionFields, UnionMode};

fn memory() -> Arc<MemoryBudget> { MemoryBudget::new(128 * 1024 * 1024) }
fn scalar(name: &str, data_type: DataType, nullable: bool, memory: &Arc<MemoryBudget>) -> Arc<VectorDescriptor> {
    let vector = ScalarWriter::new(Arc::new(Field::new(name, data_type, nullable)), Arc::clone(memory)).unwrap().finish().unwrap();
    Arc::clone(&vector.node().descriptor)
}
fn shape(name: &str, nullable: bool, kind: ContainerShapeRef<'_>, memory: &Arc<MemoryBudget>) -> Arc<VectorDescriptor> {
    VectorDescriptor::native_container(name, nullable, kind, memory).unwrap()
}
fn run(ends: &Arc<VectorDescriptor>, values: &Arc<VectorDescriptor>, memory: &Arc<MemoryBudget>) -> Arc<VectorDescriptor> {
    shape("runs", false, ContainerShapeRef::RunEnd { ends, values }, memory)
}
fn writer(descriptor: &VectorDescriptor, memory: &Arc<MemoryBudget>) -> VectorWriter { VectorWriter::from_descriptor(descriptor, Arc::clone(memory)).unwrap() }
fn block(memory: &Arc<MemoryBudget>, remaining: usize) -> Arc<MemoryReservation> {
    memory.reserve(memory.limit_bytes() - memory.used_bytes() - remaining).unwrap()
}
fn reference<T>(result: WriteResult<T>, expected: ReferenceFailure) {
    match result { Err(WriteError::Reference(actual)) => assert_eq!(actual, expected), _ => panic!("expected reference failure") }
}
fn native<T>(result: WriteResult<T>) {
    assert!(matches!(result, Err(WriteError::Native(_))));
}
fn lane(node: &WriterNode, row: i32) -> i32 {
    let WriterNode::Scalar(state) = node else { panic!() };
    ScalarReader { scalar: state }.get_int(row, NullChecks::Disabled).unwrap()
}
fn empty_buffer(memory: &Arc<MemoryBudget>) -> RetainedBuffer {
    RetainedBuffer::with_reservation(Buffer::from(Vec::<u8>::new()), memory.reserve(0).unwrap()).unwrap()
}

#[test]
fn descriptor_construction_preserves_concrete_shapes_without_logical_metadata_demand() {
    let source = memory();
    let bottom = scalar("item", DataType::Null, false, &source);
    let ends = scalar("ends", DataType::Int32, false, &source);
    let fields = [StructFieldRef { key: "lookup", descriptor: &bottom }];
    let descriptors = [
        shape("struct", true, ContainerShapeRef::Struct(&fields), &source),
        shape("list", true, ContainerShapeRef::List(&bottom), &source),
        shape("set", true, ContainerShapeRef::Set(&bottom), &source),
        shape("map", true, ContainerShapeRef::Map { sorted: true, entries: &bottom }, &source),
        shape("fixed", true, ContainerShapeRef::FixedList { size: -3, elements: &bottom }, &source),
        shape("range", true, ContainerShapeRef::TimestampRange(&bottom), &source),
        shape("union", false, ContainerShapeRef::OrdinalUnion(&[]), &source),
        run(&ends, &bottom, &source),
    ];
    assert!(descriptors[7].logical_type().is_err());
    for descriptor in &descriptors {
        let target = memory();
        let output = writer(descriptor, &target);
        assert_eq!(output.name(), descriptor.name());
        assert_eq!(output.value_count(), 0);
        assert_eq!(output.checked_len().unwrap(), 0);
        assert_eq!(output.nullable(), descriptor.reader_nullable());
        let captured = output.finish().unwrap();
        assert_eq!(captured.value_count(), 0);
        assert_eq!(captured.descriptor().name(), descriptor.name());
        assert_eq!(captured.logical_type(), descriptor.logical_type());
        drop(captured);
        assert_eq!(target.used_bytes(), 0);
    }
}

#[test]
fn scalar_root_methods_reuse_concrete_scalar_admission_and_storage() {
    let source = memory();
    for data_type in [DataType::Boolean, DataType::Int8, DataType::Int16, DataType::Int32, DataType::Int64, DataType::Float32, DataType::Float64, DataType::Utf8, DataType::FixedSizeBinary(0)] {
        let descriptor = scalar("scalar", data_type.clone(), false, &source);
        let target = memory();
        let mut output = writer(&descriptor, &target);
        match data_type {
            DataType::Boolean => output.write_boolean(true).unwrap(), DataType::Int8 => output.write_byte(-7).unwrap(),
            DataType::Int16 => output.write_short(-258).unwrap(), DataType::Int32 => output.write_int(123_456).unwrap(),
            DataType::Int64 => output.write_long(-987_654).unwrap(), DataType::Float32 => output.write_float(1.25).unwrap(),
            DataType::Float64 => output.write_double(-2.5).unwrap(), _ => output.write_bytes(b"x").unwrap(),
        }
        assert_eq!(output.value_count(), 1);
        let captured = output.finish().unwrap();
        let cell = captured.cell(0).unwrap();
        match data_type {
            DataType::Boolean => assert!(cell.get_boolean(NullChecks::Enabled).unwrap()), DataType::Int8 => assert_eq!(cell.get_byte(NullChecks::Enabled).unwrap(), -7),
            DataType::Int16 => assert_eq!(cell.get_short(NullChecks::Enabled).unwrap(), -258), DataType::Int32 => assert_eq!(cell.get_int(NullChecks::Enabled).unwrap(), 123_456),
            DataType::Int64 => assert_eq!(cell.get_long(NullChecks::Enabled).unwrap(), -987_654), DataType::Float32 => assert_eq!(cell.get_float(NullChecks::Enabled).unwrap(), 1.25),
            DataType::Float64 => assert_eq!(cell.get_double(NullChecks::Enabled).unwrap(), -2.5), DataType::Utf8 => assert_eq!(cell.get_bytes().unwrap(), b"x"),
            _ => assert!(cell.get_bytes().unwrap().is_empty()),
        }
        drop(captured);
        assert_eq!(target.used_bytes(), 0);
    }
}

#[test]
fn scalar_extension_identity_and_zero_width_undefined_survive_tree_capture() {
    let source = memory();
    for scalar_type in [ScalarType::Uuid, ScalarType::Oid] {
        let field = crate::to_field("extension", &LogicalType::Scalar(scalar_type.clone())).unwrap();
        let input = ScalarWriter::new(Arc::new(field), Arc::clone(&source)).unwrap().finish().unwrap();
        let target = memory();
        let mut output = writer(input.descriptor(), &target);
        if scalar_type == ScalarType::Uuid { output.write_bytes(&[3; 16]).unwrap(); } else { output.write_int(42).unwrap(); }
        let captured = output.finish().unwrap();
        assert_eq!(captured.logical_type().unwrap(), &LogicalType::Scalar(scalar_type));
        drop(captured);
        assert_eq!(target.used_bytes(), 0);
    }
    let descriptor = scalar("zero", DataType::FixedSizeBinary(0), false, &source);
    let target = memory();
    let mut output = writer(&descriptor, &target);
    let baseline = target.used_bytes();
    output.write_undefined().unwrap();
    assert_eq!(output.value_count(), 1);
    assert_eq!(target.used_bytes(), baseline);
    let captured = output.finish().unwrap();
    assert!(captured.cell(0).unwrap().get_bytes().unwrap().is_empty());
    drop(captured);
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn scalar_ingress_scratch_covers_metadata_before_clone_and_survives_scalar_admission() {
    let source = memory();
    let field = Field::new("n".repeat(512), DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("Europe/Paris".into())), false)
        .with_metadata(std::collections::HashMap::from([("note".to_owned(), "x".repeat(16_384))]));
    let input = ScalarWriter::new(Arc::new(field), Arc::clone(&source)).unwrap().finish().unwrap();
    let field = input.descriptor().scalar_field().unwrap();
    let (bound, depth) = super::super::super::open::schema_bound(field, 1).unwrap();
    let scratch = bound * depth * 16;
    let target = MemoryBudget::new(scratch - 1);
    native(VectorWriter::from_descriptor(input.descriptor(), Arc::clone(&target)));
    assert_eq!(target.used_bytes(), 0);
    assert_eq!(target.peak_bytes(), 0);
    let target = MemoryBudget::new(scratch * 2 - 1);
    native(VectorWriter::from_descriptor(input.descriptor(), Arc::clone(&target)));
    assert_eq!(target.used_bytes(), 0);
    assert_eq!(target.peak_bytes(), scratch);
    let target = MemoryBudget::new(scratch * 2);
    let output = writer(input.descriptor(), &target);
    assert_eq!(target.used_bytes(), scratch);
    drop(output);
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn struct_keys_names_bottom_padding_and_ahead_children_keep_their_source_roles() {
    let source = memory();
    let child = scalar("actual", DataType::Null, false, &source);
    let fields = [StructFieldRef { key: "left", descriptor: &child }, StructFieldRef { key: "right", descriptor: &child }];
    let descriptor = shape("struct", false, ContainerShapeRef::Struct(&fields), &source);
    let target = memory();
    let mut output = writer(&descriptor, &target);
    output.write_null().unwrap();
    let WriterNode::Struct(state) = &output.root else { panic!() };
    assert_eq!(state.count.get(), 1);
    assert!(!state.children.values[0].node.nullable());
    output.end_struct().unwrap();
    output.operation(|node, mutation| {
        let WriterNode::Struct(state) = node else { panic!() };
        state.children.values[0].node.undefined(2, mutation)?;
        state.children.values[0].node.undefined(2, mutation)
    }).unwrap();
    output.end_struct().unwrap();
    let captured = output.finish().unwrap();
    let left = captured.vector_for("left").unwrap().unwrap();
    let right = captured.vector_for("right").unwrap().unwrap();
    assert_eq!(captured.value_count(), 3);
    assert_eq!(left.value_count(), 4);
    assert_eq!(right.value_count(), 3);
    assert_eq!(left.descriptor().name(), "actual");
    assert_eq!(right.descriptor().name(), "actual");
    assert!(left.reader_nullable());
    let Some(ContainerKind::Struct(fields)) = captured.descriptor().container() else { panic!() };
    assert_eq!([fields[0].key(), fields[1].key()], ["left", "right"]);
    drop((left, right, captured));
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn struct_parent_and_ordered_child_prefix_abort_only_the_root_after_native_failure() {
    let source = memory();
    let first = scalar("first", DataType::Int32, false, &source);
    let second = scalar("second", DataType::Null, false, &source);
    let fields = [StructFieldRef { key: "first", descriptor: &first }, StructFieldRef { key: "second", descriptor: &second }];
    let descriptor = shape("struct", true, ContainerShapeRef::Struct(&fields), &source);
    let target = memory();
    let mut output = writer(&descriptor, &target);
    let blocker = block(&target, 256);
    native(output.end_struct());
    assert_eq!(output.state, PublicationState::NativeAborted);
    let WriterNode::Struct(state) = &output.root else { panic!() };
    assert_eq!(state.count.get(), 1);
    assert_eq!(state.validity.as_ref().unwrap().append, 1);
    assert_eq!(state.validity.as_ref().unwrap().bytes()[0], 1);
    assert!(state.children.values[0].node.nullable());
    assert_eq!(state.children.values[0].node.count().get(), 0);
    assert!(!state.children.values[1].node.nullable());
    drop(blocker);
    output.clear();
    assert_eq!(output.state, PublicationState::Ready);
    assert!(output.nullable());
    output.end_struct().unwrap();
    drop(output.finish().unwrap());
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn native_failure_before_mutation_leaves_ready_but_nullable_installation_is_a_prefix() {
    let source = memory();
    for nullable in [true, false] {
        let descriptor = shape("struct", nullable, ContainerShapeRef::Struct(&[]), &source);
        let target = memory();
        let mut output = writer(&descriptor, &target);
        if !nullable { output.end_struct().unwrap(); }
        let blocker = block(&target, 255);
        if nullable { native(output.end_struct()); assert_eq!(output.state, PublicationState::Ready); assert_eq!(output.value_count(), 0); }
        else { native(output.write_null()); assert_eq!(output.state, PublicationState::NativeAborted); assert_eq!(output.value_count(), 1); assert!(output.nullable()); }
        drop(blocker);
        output.clear();
        output.write_null().unwrap();
        drop(output.finish().unwrap());
        assert_eq!(target.used_bytes(), 0);
    }
}

#[test]
fn list_offsets_last_offset_capture_and_snapshot_are_independent() {
    let source = memory();
    let child = scalar("item", DataType::Int32, false, &source);
    let descriptor = shape("list", false, ContainerShapeRef::List(&child), &source);
    let target = memory();
    let mut output = writer(&descriptor, &target);
    output.write_undefined().unwrap();
    output.operation(|node, mutation| {
        let WriterNode::List(state) = node else { panic!() };
        assert_eq!(state.offsets.append, 8);
        state.elements.primitive("writeInt", &7i32.to_le_bytes(), 2, mutation)?;
        state.elements.primitive("writeInt", &9i32.to_le_bytes(), 2, mutation)
    }).unwrap();
    output.end_list().unwrap();
    output.operation(|node, mutation| {
        let WriterNode::List(state) = node else { panic!() };
        assert_eq!(state.last_offset, 2);
        state.last_offset = 41;
        mutation.mark();
        state.offsets.set(8, &7i32.to_le_bytes(), mutation)
    }).unwrap();
    let captured = output.finish().unwrap();
    let NodeLayout::NativeContainer(NativeContainerStorage::List { last_offset, offsets, .. }) = &captured.node().storage else { panic!() };
    assert_eq!(*last_offset, 41);
    assert_eq!(offsets.append, 12);
    let snapshot = captured.snapshot_reader(&target).unwrap();
    let NodeLayout::NativeContainer(NativeContainerStorage::List { last_offset, .. }) = &snapshot.node().storage else { panic!() };
    assert_eq!(*last_offset, 7);
    assert!(captured.cell(1).unwrap().raw_list_elements().is_err());
    drop((captured, snapshot));
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn list_offset_prefix_precedes_validity_native_admission() {
    let source = memory();
    let child = scalar("item", DataType::Null, false, &source);
    let descriptor = shape("list", true, ContainerShapeRef::List(&child), &source);
    let target = memory();
    let mut output = writer(&descriptor, &target);
    let blocker = block(&target, 256);
    native(output.write_undefined());
    let WriterNode::List(state) = &output.root else { panic!() };
    assert_eq!(state.offsets.append, 8);
    assert_eq!(state.last_offset, 0);
    assert_eq!(state.count.get(), 0);
    assert_eq!(state.validity.as_ref().unwrap().append, 0);
    assert_eq!(output.state, PublicationState::NativeAborted);
    drop(blocker);
    output.clear();
    output.end_list().unwrap();
    drop(output.finish().unwrap());
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn clear_retains_capacity_nullability_and_slots_while_zeroing_payload() {
    let source = memory();
    let child = scalar("item", DataType::Null, false, &source);
    let descriptor = shape("list", false, ContainerShapeRef::List(&child), &source);
    let target = memory();
    let mut output = writer(&descriptor, &target);
    output.end_list().unwrap();
    output.write_null().unwrap();
    let retained = target.used_bytes();
    output.clear();
    assert_eq!(target.used_bytes(), retained);
    assert_eq!(output.value_count(), 0);
    assert!(output.nullable());
    let WriterNode::List(state) = &output.root else { panic!() };
    assert_eq!(state.last_offset, 0);
    assert_eq!(state.offsets.append, 0);
    assert!(state.offsets.bytes().iter().all(|byte| *byte == 0));
    assert!(state.validity.as_ref().unwrap().bytes().iter().all(|byte| *byte == 0));
    output.write_undefined().unwrap();
    let captured = output.finish().unwrap();
    assert!(captured.cell(0).unwrap().is_null().unwrap());
    let alias = captured.clone();
    let retained = target.used_bytes();
    drop(captured);
    assert_eq!(target.used_bytes(), retained);
    drop(alias);
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn fixed_widths_and_wrapper_end_methods_follow_concrete_source_classes() {
    let source = memory();
    let child = scalar("item", DataType::Null, false, &source);
    for width in [-3, 0, 3] {
        let descriptor = shape("fixed", false, ContainerShapeRef::FixedList { size: width, elements: &child }, &source);
        let target = memory();
        let mut output = writer(&descriptor, &target);
        output.write_undefined().unwrap();
        output.end_list().unwrap();
        let captured = output.finish().unwrap();
        assert_eq!(captured.value_count(), 2);
        assert_eq!(captured.cell(0).unwrap().list_count().unwrap(), width);
        assert_eq!(captured.cell(1).unwrap().list_start_index().unwrap(), width);
        assert_eq!(captured.list_elements().unwrap().value_count(), width.max(0));
        drop(captured);
        assert_eq!(target.used_bytes(), 0);
    }
    let descriptors = [shape("map", false, ContainerShapeRef::Map { sorted: true, entries: &child }, &source), shape("set", false, ContainerShapeRef::Set(&child), &source), shape("range", false, ContainerShapeRef::TimestampRange(&child), &source)];
    for (index, descriptor) in descriptors.iter().enumerate() {
        let target = memory();
        let mut output = writer(descriptor, &target);
        if index == 0 { output.end_list().unwrap(); } else { reference(output.end_list(), ReferenceFailure::Unsupported("endList")); assert_eq!(output.value_count(), 0); }
        reference(output.write_long(7), ReferenceFailure::Unsupported("writeLong"));
        output.write_undefined().unwrap();
        output.write_null().unwrap();
        drop(output.finish().unwrap());
        assert_eq!(target.used_bytes(), 0);
    }
}

#[test]
fn union_undefined_null_reuse_type_failure_and_clear_keep_distinct_prefixes() {
    let source = memory();
    let descriptor = shape("union", false, ContainerShapeRef::OrdinalUnion(&[]), &source);
    let target = memory();
    let mut output = writer(&descriptor, &target);
    output.write_undefined().unwrap();
    output.write_null().unwrap();
    output.write_null().unwrap();
    let WriterNode::Union(state) = &output.root else { panic!() };
    assert_eq!(state.type_ids.bytes()[..3], [255, 0, 0]);
    assert_eq!(state.children.values.len(), 1);
    assert_eq!(state.children.values[0].count().get(), 2);
    assert_eq!(state.offsets.append, 12);
    output.clear();
    let WriterNode::Union(state) = &output.root else { panic!() };
    assert_eq!(state.children.values.len(), 1);
    assert_eq!(state.children.values[0].count().get(), 0);
    assert!(state.type_ids.bytes().iter().all(|byte| *byte == 0));
    drop(output.finish().unwrap());
    assert_eq!(target.used_bytes(), 0);
    let child = scalar("null", DataType::Int32, false, &source);
    let descriptor = shape("union", false, ContainerShapeRef::OrdinalUnion(&[child]), &source);
    let mut output = writer(&descriptor, &target);
    let blocker = block(&target, 0);
    reference(output.write_null(), ReferenceFailure::CannotPromoteUnionLeg);
    assert_eq!(output.state, PublicationState::Ready);
    assert_eq!(output.value_count(), 0);
    drop(blocker);
    output.write_undefined().unwrap();
    drop(output);
    assert_eq!(target.used_bytes(), 0);
}

fn wide_union(count: usize, source: &Arc<MemoryBudget>) -> Arc<VectorDescriptor> {
    let child = scalar("value", DataType::Null, false, source);
    let _slots = source.reserve(count * std::mem::size_of::<Arc<VectorDescriptor>>() + 128).unwrap();
    let children = vec![child; count];
    shape("wide", false, ContainerShapeRef::OrdinalUnion(&children), source)
}

#[test]
fn union_ordinal_narrowing_preserves_every_child() {
    let source = memory();
    for count in [128, 256] {
        let descriptor = wide_union(count, &source);
        let target = memory();
        let mut output = writer(&descriptor, &target);
        output.write_null().unwrap();
        let captured = output.finish().unwrap();
        let Some(ContainerKind::DenseUnion { children, .. }) = captured.descriptor().container() else { panic!() };
        assert_eq!(children.len(), count + 1);
        assert_eq!(captured.cell(0).unwrap().type_id().unwrap(), count as i8);
        assert!(captured.arrow_field().is_err());
        drop(captured);
        assert_eq!(target.used_bytes(), 0);
    }
}

#[test]
fn declared_union_ids_choose_the_direct_source_ordinal_constructor() {
    let source = memory();
    let left = ScalarWriter::new(Arc::new(Field::new("left", DataType::Int32, false)), Arc::clone(&source)).unwrap().finish().unwrap();
    let right = ScalarWriter::new(Arc::new(Field::new("right", DataType::Int64, false)), Arc::clone(&source)).unwrap().finish().unwrap();
    let fields = UnionFields::try_new([5, 9], [Arc::new(Field::new("left", DataType::Int32, false)), Arc::new(Field::new("right", DataType::Int64, false))]).unwrap();
    let _slots = source.reserve(2 * std::mem::size_of::<ExecutionVector>() + 128).unwrap();
    let input = ExecutionVector::from_physical(Arc::new(Field::new("union", DataType::Union(fields, UnionMode::Dense), false)), PhysicalLayout::DenseUnion { type_ids: empty_buffer(&source), offsets: empty_buffer(&source), children: vec![left, right] }, &source).unwrap();
    let target = memory();
    let captured = writer(input.descriptor(), &target).finish().unwrap();
    let Some(ContainerKind::DenseUnion { encoding, children }) = captured.descriptor().container() else { panic!() };
    assert!(matches!(encoding, super::super::super::descriptor::UnionEncoding::Ordinal));
    assert_eq!(children.len(), 2);
    let field = captured.arrow_field().unwrap();
    let DataType::Union(fields, _) = field.field().data_type() else { panic!() };
    assert_eq!(fields.iter().map(|(id, _)| id).collect::<Vec<_>>(), [0, 1]);
    drop((captured, field));
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn null_candidate_construction_precedes_slot_growth_and_failed_growth_keeps_ready() {
    let source = memory();
    let descriptor = wide_union(512, &source);
    assert_eq!(WriterNode::admit(&descriptor, 1).unwrap(), 513);
    let target = memory();
    let mut output = writer(&descriptor, &target);
    let field = Field::new("null", DataType::Null, true);
    let (bound, depth) = super::super::super::open::schema_bound(&field, 1).unwrap();
    let scalar = bound * depth * 16;
    let peak = scalar * 2 + 4096 + std::mem::size_of::<Field>();
    assert!(1024 * std::mem::size_of::<WriterNode>() + 128 > peak - scalar);
    let blocker = block(&target, peak);
    let before = target.used_bytes();
    native(output.write_null());
    assert_eq!(target.used_bytes(), before);
    assert_eq!(output.state, PublicationState::Ready);
    let WriterNode::Union(state) = &output.root else { panic!() };
    assert_eq!(state.children.values.len(), 512);
    assert_eq!(state.type_ids.append, 0);
    assert_eq!(state.count.get(), 0);
    output.write_undefined().unwrap();
    drop((blocker, output));
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn retained_new_null_slot_before_native_cursor_failure_aborts_publication() {
    let source = memory();
    let descriptor = shape("union", false, ContainerShapeRef::OrdinalUnion(&[]), &source);
    let target = memory();
    let mut output = writer(&descriptor, &target);
    output.operation(|node, mutation| { let WriterNode::Union(state) = node else { panic!() }; state.type_ids.append = -1; mutation.mark(); Ok(()) }).unwrap();
    native(output.write_null());
    assert_eq!(output.state, PublicationState::NativeAborted);
    let WriterNode::Union(state) = &output.root else { panic!() };
    assert_eq!(state.children.values.len(), 1);
    assert_eq!(state.children.values[0].count().get(), 0);
    assert_eq!(state.count.get(), 0);
    assert!(output.finish().is_err());
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn union_type_append_before_offset_native_failure_is_retained_until_clear() {
    let source = memory();
    let descriptor = shape("union", false, ContainerShapeRef::OrdinalUnion(&[]), &source);
    let target = memory();
    let mut output = writer(&descriptor, &target);
    output.operation(|node, mutation| { let WriterNode::Union(state) = node else { panic!() }; state.type_ids.ensure_capacity(128, mutation) }).unwrap();
    let blocker = block(&target, 255);
    native(output.write_undefined());
    let WriterNode::Union(state) = &output.root else { panic!() };
    assert_eq!(state.type_ids.append, 1);
    assert_eq!(state.type_ids.bytes()[0], 255);
    assert_eq!(state.offsets.append, 0);
    assert_eq!(state.count.get(), 0);
    assert_eq!(output.state, PublicationState::NativeAborted);
    drop(blocker);
    output.clear();
    output.write_undefined().unwrap();
    drop(output.finish().unwrap());
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn ree_singletons_nested_values_and_end_widths_preserve_counts_without_coalescing() {
    let source = memory();
    let value = scalar("values", DataType::Int64, false, &source);
    for end_type in [DataType::Int16, DataType::Int32, DataType::Int64] {
        let ends = scalar("ends", end_type, false, &source);
        let inner = run(&ends, &value, &source);
        let descriptor = run(&ends, &inner, &source);
        let target = memory();
        let mut output = writer(&descriptor, &target);
        output.write_long(17).unwrap();
        output.write_long(17).unwrap();
        let WriterNode::RunEnd(state) = &output.root else { panic!() };
        assert_eq!(state.ends.count().get(), 2);
        assert_eq!(state.values.count().get(), 2);
        let captured = output.finish().unwrap();
        assert_eq!(captured.value_count(), 2);
        assert_eq!(captured.cell(1).unwrap().get_long(NullChecks::Enabled).unwrap(), 17);
        drop(captured);
        assert_eq!(target.used_bytes(), 0);
    }
}

#[test]
fn ree_bytes_boolean_and_unsupported_inner_calls_finish_only_after_inner_success() {
    let source = memory();
    let ends = scalar("ends", DataType::Int32, false, &source);
    for data_type in [DataType::Utf8, DataType::Boolean] {
        let value = scalar("values", data_type.clone(), false, &source);
        let descriptor = run(&ends, &run(&ends, &value, &source), &source);
        let target = memory();
        let mut output = writer(&descriptor, &target);
        reference(output.write_long(1), ReferenceFailure::Unsupported("writeLong"));
        assert_eq!(output.value_count(), 0);
        if data_type == DataType::Utf8 { output.write_bytes(b"abc").unwrap(); } else { output.write_boolean(true).unwrap(); }
        let captured = output.finish().unwrap();
        if data_type == DataType::Utf8 { assert_eq!(captured.cell(0).unwrap().get_bytes().unwrap(), b"abc"); } else { assert!(captured.cell(0).unwrap().get_boolean(NullChecks::Enabled).unwrap()); }
        drop(captured);
        assert_eq!(target.used_bytes(), 0);
    }
}

#[test]
fn ree_float_and_double_native_prefixes_share_the_single_root_gate() {
    let source = memory();
    let ends = scalar("ends", DataType::Int32, false, &source);
    for data_type in [DataType::Float32, DataType::Float64] {
        let values = scalar("values", data_type.clone(), false, &source);
        let target = memory();
        let mut output = writer(&run(&ends, &values, &source), &target);
        let blocker = block(&target, 255);
        if data_type == DataType::Float32 { native(output.write_float(1.25)); } else { native(output.write_double(1.25)); }
        let WriterNode::RunEnd(state) = &output.root else { panic!() };
        assert_eq!(state.count.get(), 0);
        assert_eq!(state.ends.count().get(), 0);
        assert_eq!(state.values.count().get(), if data_type == DataType::Float32 { 1 } else { 0 });
        assert_eq!(output.state, if data_type == DataType::Float32 { PublicationState::NativeAborted } else { PublicationState::Ready });
        drop(blocker);
        if data_type == DataType::Float32 { output.clear(); output.write_float(1.25).unwrap(); } else { output.write_double(1.25).unwrap(); }
        assert_eq!(output.value_count(), 1);
        drop(output.finish().unwrap());
        assert_eq!(target.used_bytes(), 0);
    }
}

#[test]
fn ree_null_wrapper_paths_and_union_nullable_failure_use_concrete_receivers() {
    let source = memory();
    let ends = scalar("ends", DataType::Int32, false, &source);
    let bottom = scalar("item", DataType::Null, false, &source);
    let values = [shape("struct", false, ContainerShapeRef::Struct(&[]), &source), shape("list", false, ContainerShapeRef::List(&bottom), &source), shape("fixed", false, ContainerShapeRef::FixedList { size: 2, elements: &bottom }, &source), shape("map", false, ContainerShapeRef::Map { sorted: false, entries: &bottom }, &source), shape("set", false, ContainerShapeRef::Set(&bottom), &source), shape("range", false, ContainerShapeRef::TimestampRange(&bottom), &source), shape("union", false, ContainerShapeRef::OrdinalUnion(&[]), &source)];
    for (index, value) in values.iter().enumerate() {
        for nested in [false, true] {
            let value = if nested { run(&ends, value, &source) } else { Arc::clone(value) };
            let descriptor = run(&ends, &value, &source);
            let target = memory();
            let mut output = writer(&descriptor, &target);
            if index == 6 {
                reference(output.write_null(), ReferenceFailure::CannotSetUnionNullable);
                assert_eq!(output.value_count(), 0);
                assert_eq!(output.state, PublicationState::Ready);
                assert!(!output.nullable());
                output.write_undefined().unwrap();
            } else { output.write_null().unwrap(); assert!(output.nullable()); }
            assert_eq!(output.value_count(), 1);
            drop(output.finish().unwrap());
            assert_eq!(target.used_bytes(), 0);
        }
    }
}

#[test]
fn ree_concrete_end_receiver_and_value_count_checks_keep_their_failure_order() {
    let source = memory();
    let integer = scalar("ends", DataType::Int32, false, &source);
    let encoded_ends = run(&integer, &integer, &source);
    let value = scalar("values", DataType::Int64, false, &source);
    let descriptor = run(&encoded_ends, &value, &source);
    let target = memory();
    let mut output = writer(&descriptor, &target);
    reference(output.write_long(7), ReferenceFailure::RunEndReceiverClass);
    assert_eq!(output.value_count(), 0);
    reference(output.write_long(9), ReferenceFailure::CheckFailed);
    assert_eq!(output.state, PublicationState::Ready);
    let WriterNode::RunEnd(state) = &output.root else { panic!() };
    assert_eq!(state.values.count().get(), 2);
    assert_eq!(state.ends.count().get(), 0);
    reference(output.operation(|node, mutation| { let WriterNode::RunEnd(state) = node else { panic!() }; state.end_run(0, mutation) }), ReferenceFailure::RequireFailed);
    output.clear();
    reference(output.write_long(11), ReferenceFailure::RunEndReceiverClass);
    let captured = output.finish().unwrap();
    let NodeLayout::RunEnd(reader) = &captured.node().storage else { panic!() };
    assert_eq!(captured.value_count(), 0);
    assert_eq!(reader.values().count.get(), 1);
    assert_eq!(reader.ends().count.get(), 0);
    drop(captured);
    assert_eq!(target.used_bytes(), 0);
}

fn append_run(output: &mut VectorWriter, value: i64, length: i32) -> WriteResult<()> {
    output.operation(|node, mutation| { let WriterNode::RunEnd(state) = node else { panic!() }; state.values.primitive("writeLong", &value.to_le_bytes(), 2, mutation)?; state.end_run(length, mutation) })
}

#[test]
fn ree_wrapped_counts_short_upper_bound_and_recovery_keep_native_extents_separate() {
    let source = memory();
    let value = scalar("values", DataType::Int64, false, &source);
    let ends = scalar("ends", DataType::Int32, false, &source);
    let descriptor = run(&ends, &value, &source);
    let target = memory();
    let mut output = writer(&descriptor, &target);
    append_run(&mut output, 11, i32::MAX).unwrap();
    append_run(&mut output, 22, i32::MAX).unwrap();
    assert_eq!(output.value_count(), -2);
    assert!(output.checked_len().is_err());
    let negative = output.finish().unwrap();
    assert_eq!(negative.value_count(), -2);
    assert!(negative.cell(0).is_err());
    drop(negative);
    assert_eq!(target.used_bytes(), 0);
    let mut output = writer(&descriptor, &target);
    append_run(&mut output, 11, i32::MAX).unwrap(); append_run(&mut output, 22, i32::MAX).unwrap(); append_run(&mut output, 33, 3).unwrap();
    assert_eq!(output.value_count(), 1);
    let WriterNode::RunEnd(state) = &output.root else { panic!() };
    assert_eq!([lane(&state.ends, 0), lane(&state.ends, 1), lane(&state.ends, 2)], [i32::MAX, -2, 1]);
    let captured = output.finish().unwrap();
    assert_eq!(captured.cell(0).unwrap().get_long(NullChecks::Enabled).unwrap(), 11);
    drop(captured);
    let short = scalar("ends", DataType::Int16, false, &source);
    let mut output = writer(&run(&short, &value, &source), &target);
    append_run(&mut output, 1, 1).unwrap(); append_run(&mut output, 2, i32::MAX).unwrap();
    assert_eq!(output.value_count(), i32::MIN);
    let WriterNode::RunEnd(state) = &output.root else { panic!() };
    let WriterNode::Scalar(ends) = state.ends.as_ref() else { panic!() };
    assert_eq!(ScalarReader { scalar: ends }.get_short(1, NullChecks::Disabled).unwrap(), 0);
    drop(output.finish().unwrap());
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn short_end_overflow_preserves_the_value_prefix_and_later_count_check() {
    let source = memory();
    let value = scalar("values", DataType::Int64, false, &source);
    let ends = scalar("ends", DataType::Int16, false, &source);
    let target = memory();
    let mut output = writer(&run(&ends, &value, &source), &target);
    append_run(&mut output, 1, i32::from(i16::MAX)).unwrap();
    reference(append_run(&mut output, 2, 1), ReferenceFailure::RequireFailed);
    assert_eq!(output.value_count(), i32::from(i16::MAX));
    assert_eq!(output.state, PublicationState::Ready);
    let WriterNode::RunEnd(state) = &output.root else { panic!() };
    assert_eq!(state.values.count().get(), 2);
    assert_eq!(state.ends.count().get(), 1);
    reference(output.write_long(3), ReferenceFailure::CheckFailed);
    output.clear();
    output.write_long(4).unwrap();
    drop(output.finish().unwrap());
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn null_exposed_ree_leg_retains_union_prefix_before_child_end_failure() {
    let source = memory();
    let integer = scalar("ends", DataType::Int32, false, &source);
    let encoded_ends = run(&integer, &integer, &source);
    let bottom = scalar("values", DataType::Null, false, &source);
    let leg = shape("null", false, ContainerShapeRef::RunEnd { ends: &encoded_ends, values: &bottom }, &source);
    let descriptor = shape("union", false, ContainerShapeRef::OrdinalUnion(&[leg]), &source);
    let target = memory();
    let mut output = writer(&descriptor, &target);
    reference(output.write_null(), ReferenceFailure::RunEndReceiverClass);
    assert_eq!(output.value_count(), 1);
    assert_eq!(output.state, PublicationState::Ready);
    let WriterNode::Union(state) = &output.root else { panic!() };
    assert_eq!(state.type_ids.bytes()[0], 0);
    assert_eq!(state.offsets.append, 4);
    let WriterNode::RunEnd(leg) = &state.children.values[0] else { panic!() };
    assert_eq!(leg.count.get(), 0);
    assert_eq!(leg.values.count().get(), 1);
    assert!(leg.values.nullable());
    drop(output.finish().unwrap());
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn capture_input_key_publisher_and_descriptor_admission_failures_release_every_owner() {
    let source = memory();
    let child = scalar("actual", DataType::Int32, false, &source);
    let descriptor = shape("struct", false, ContainerShapeRef::Struct(&[StructFieldRef { key: "key", descriptor: &child }]), &source);
    let inputs = std::mem::size_of::<NativeStructInput>() + 128;
    let key = 3 + 128;
    let publisher = std::mem::size_of::<ExecutionVector>() + 3 * std::mem::size_of::<Arc<PhysicalNode>>() + std::mem::size_of::<StructFieldRef<'_>>() + 2048;
    for remaining in [0, inputs, inputs + key, inputs + key + publisher] {
        let target = memory();
        let output = writer(&descriptor, &target);
        let blocker = block(&target, remaining);
        assert!(output.finish().is_err());
        assert_eq!(target.used_bytes(), blocker.bytes());
        drop(blocker);
        assert_eq!(target.used_bytes(), 0);
    }
}

#[test]
fn native_depth128_supports_failed_metadata_writes_clear_capture_and_drop() {
    let source = memory();
    let integer = scalar("ends", DataType::Int32, false, &source);
    let mut descriptor = scalar("values", DataType::Null, false, &source);
    for _ in 1..128 { descriptor = run(&integer, &descriptor, &source); }
    assert_eq!(WriterNode::admit(&descriptor, 1).unwrap(), 255);
    assert!(descriptor.logical_type().is_err());
    let target = memory();
    let mut output = writer(&descriptor, &target);
    assert!(!output.nullable());
    output.write_null().unwrap();
    assert!(output.nullable());
    assert_eq!(output.value_count(), 1);
    output.clear();
    output.write_undefined().unwrap();
    let captured = output.finish().unwrap();
    assert_eq!(captured.value_count(), 1);
    assert!(captured.logical_type().is_err());
    drop(captured);
    assert_eq!(target.used_bytes(), 0);
    drop(writer(&descriptor, &target));
    assert_eq!(target.used_bytes(), 0);
    let small = MemoryBudget::new(200_000);
    native(VectorWriter::from_descriptor(&descriptor, Arc::clone(&small)));
    assert_eq!(small.used_bytes(), 0);
    let mut output = writer(&descriptor, &target);
    let blocker = block(&target, 255);
    native(output.write_null());
    assert_eq!(output.state, PublicationState::NativeAborted);
    drop((blocker, output));
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn adding_a_129th_level_rejects_before_null_child_installation() {
    let source = memory();
    let mut descriptor = shape("union", false, ContainerShapeRef::OrdinalUnion(&[]), &source);
    for _ in 1..128 { descriptor = shape("parent", false, ContainerShapeRef::Struct(&[StructFieldRef { key: "child", descriptor: &descriptor }]), &source); }
    let target = memory();
    let mut output = writer(&descriptor, &target);
    native(output.operation(|mut node, mutation| {
        for _ in 1..128 { let WriterNode::Struct(state) = node else { panic!() }; node = &mut state.children.values[0].node; }
        node.write_null(128, mutation)
    }));
    assert_eq!(output.state, PublicationState::Ready);
    let mut node = &output.root;
    for _ in 1..128 { let WriterNode::Struct(state) = node else { panic!() }; node = &state.children.values[0].node; }
    let WriterNode::Union(state) = node else { panic!() };
    assert!(state.children.values.is_empty());
    assert_eq!(state.type_ids.append, 0);
    drop(output);
    assert_eq!(target.used_bytes(), 0);
}
