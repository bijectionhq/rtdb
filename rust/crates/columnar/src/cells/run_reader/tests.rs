use arrow_array::{Int32Array, Int64Array, NullArray};
use arrow_buffer::Buffer;
use crate::MemoryBudget;
use super::super::descriptor::ContainerShapeRef;
use super::super::native_scalar::{NativeScalarData, NativeScalarStorage, StoredBytes};
use super::*;

fn arrow(name: &str, array: ArrayRef, nullable: bool, memory: &Arc<MemoryBudget>) -> ExecutionVector {
    let field = Arc::new(Field::new(name, array.data_type().clone(), nullable));
    let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
    ExecutionVector::with_reservation(field, array, reservation).unwrap()
}
fn ints(name: &str, values: &[i32], memory: &Arc<MemoryBudget>) -> ExecutionVector {
    arrow(name, Arc::new(Int32Array::from(values.to_vec())), false, memory)
}
fn bytes(value: &[u8], memory: &Arc<MemoryBudget>) -> RetainedBuffer {
    let reservation = memory.reserve(value.len() + 128).unwrap();
    RetainedBuffer::with_reservation(Buffer::from_slice_ref(value), reservation).unwrap()
}
fn native(descriptor: Arc<VectorDescriptor>, length: usize, storage: NodeLayout, memory: &Arc<MemoryBudget>) -> ExecutionVector {
    let owner = Arc::new(NodeOwner { backing: BackingOwner::Native, metadata: memory.reserve(512).unwrap() });
    ExecutionVector::direct(Arc::new(PhysicalNode { declaration: Declaration::Native, descriptor, count: SourceCount::from_len(length).unwrap(), storage, owner }))
}
fn run(ends: ExecutionVector, values: ExecutionVector, length: usize, logical_offset: usize, memory: &Arc<MemoryBudget>) -> ExecutionVector {
    let ends = ends.into_direct().unwrap();
    let values = values.into_direct().unwrap();
    let descriptor = VectorDescriptor::native_container("runs", false, ContainerShapeRef::RunEnd { ends: &ends.descriptor, values: &values.descriptor }, memory).unwrap();
    native(descriptor, length, NodeLayout::RunEnd(RunEndReader::new(RunStorage { logical_offset, ends, values })), memory)
}
fn reader(vector: &ExecutionVector) -> &RunEndReader {
    let NodeLayout::RunEnd(reader) = &vector.node().storage else { panic!() };
    reader
}
fn wrapped(memory: &Arc<MemoryBudget>) -> ExecutionVector {
    run(ints("ends", &[i32::MAX, -2, 1], memory), ints("values", &[11, 22, 33], memory), 1, 0, memory)
}
fn move_past_end(vector: &ExecutionVector) { assert_eq!(reader(vector).index(PhysicalIndex(i32::MAX)).unwrap().0, 3); }
fn raw_fixed(name: &str, length: usize, payload: &[u8], memory: &Arc<MemoryBudget>) -> ExecutionVector {
    let template = ints(name, &[], memory);
    native(Arc::clone(&template.node().descriptor), length, NodeLayout::Scalar(NativeScalarStorage {
        validity: None, data: NativeScalarData::Fixed(StoredBytes { bytes: bytes(payload, memory), append: payload.len() }),
    }), memory)
}
fn variable(length: usize, offsets: &[u8], last_offset: i32, memory: &Arc<MemoryBudget>) -> ExecutionVector {
    let template = arrow("bytes", Arc::new(arrow_array::BinaryArray::from(Vec::<&[u8]>::new())), false, memory);
    native(Arc::clone(&template.node().descriptor), length, NodeLayout::Scalar(NativeScalarStorage {
        validity: None, data: NativeScalarData::Variable {
            offsets: StoredBytes { bytes: bytes(offsets, memory), append: offsets.len() },
            data: StoredBytes { bytes: bytes(b"abc", memory), append: 3 }, last_offset,
        },
    }), memory)
}
fn last_offset(vector: &ExecutionVector) -> i32 {
    let NodeLayout::Scalar(NativeScalarStorage { data: NativeScalarData::Variable { last_offset, .. }, .. }) = &vector.node().storage else { panic!() };
    *last_offset
}

#[test]
fn wrapped_run_ends_keep_hint_dependent_lookup_and_alias_identity() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let runs = wrapped(&memory);
    let alias = runs.clone();
    let slice = runs.select(&Selection::All).unwrap();
    let indirect = slice.select(&Selection::Indices(vec![0])).unwrap();
    assert_eq!(runs.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 11);
    move_past_end(&runs);
    for view in [&alias, &slice, &indirect] { assert_eq!(view.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 33); }
    assert_eq!(reader(&runs).hint.load(Ordering::Relaxed), 2);
    drop((runs, alias, slice, indirect));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn named_and_indirect_leg_aliases_share_hints_but_snapshots_reset_them() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let snapshot_memory = MemoryBudget::new(8 * 1024 * 1024);
    let runs = wrapped(&memory);
    let child = runs.clone().into_direct().unwrap();
    let children = [Arc::clone(&child.descriptor)];
    let descriptor = VectorDescriptor::native_container("union", false, ContainerShapeRef::OrdinalUnion(&children), &memory).unwrap();
    let union = native(descriptor, 1, NodeLayout::Union {
        type_ids: bytes(&[0], &memory), offsets: bytes(&0i32.to_le_bytes(), &memory), children: vec![child],
    }, &memory);
    let named = union.vector_for("runs").unwrap().unwrap();
    let selected = named.select(&Selection::All).unwrap().select(&Selection::All).unwrap();
    move_past_end(&runs);
    assert_eq!(named.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 33);
    assert_eq!(selected.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 33);
    let snapshot = named.snapshot_reader(&snapshot_memory).unwrap();
    assert!(matches!(&snapshot.view, ReaderView::UnionLeg { .. }));
    assert!(!snapshot.cell(0).unwrap().is_null().unwrap());
    assert_eq!(snapshot.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 11);
    assert_eq!(named.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 33);
    assert!(Arc::ptr_eq(&snapshot.node().descriptor, &runs.node().descriptor));
    drop((runs, union, named, selected, children));
    assert!(memory.used_bytes() > 0);
    assert!(snapshot_memory.used_bytes() > 0);
    assert_eq!(snapshot.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 11);
    drop(snapshot);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(snapshot_memory.used_bytes(), 0);
}

#[test]
fn lookup_failure_keeps_hint_but_delegated_failure_retains_successful_update() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let ends = raw_fixed("ends", 2, &1i32.to_le_bytes(), &memory);
    let broken = run(ends, ints("values", &[10, 20], &memory), 2, 0, &memory);
    assert_eq!(broken.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 10);
    assert!(broken.cell(1).unwrap().get_int(NullChecks::Enabled).is_err());
    assert_eq!(reader(&broken).hint.load(Ordering::Relaxed), 0);
    let missing = run(ints("ends", &[1, 3], &memory), ints("values", &[10], &memory), 3, 0, &memory);
    assert!(missing.cell(2).unwrap().get_int(NullChecks::Enabled).is_err());
    assert_eq!(reader(&missing).hint.load(Ordering::Relaxed), 1);
    let unsupported_value = run(ints("ends", &[1, 3], &memory), ints("values", &[10, 20], &memory), 3, 0, &memory);
    assert!(unsupported_value.cell(2).unwrap().get_boolean(NullChecks::Enabled).is_err());
    assert_eq!(reader(&unsupported_value).hint.load(Ordering::Relaxed), 1);
    drop((broken, missing, unsupported_value));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn long_long_class_admission_precedes_lookup_while_pointer_and_bytes_follow_it() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let runs = run(ints("ends", &[1, 3], &memory), ints("values", &[10, 20], &memory), 3, 0, &memory);
    assert_eq!(runs.cell(2).unwrap().get_long_long_high().unwrap_err().code, "rtdb/unsupported-vector-reader");
    assert_eq!(reader(&runs).hint.load(Ordering::Relaxed), 0);
    assert_eq!(runs.cell(2).unwrap().byte_span().unwrap(), &20i32.to_le_bytes());
    assert_eq!(reader(&runs).hint.load(Ordering::Relaxed), 1);
    reader(&runs).hint.store(0, Ordering::Relaxed);
    assert!(runs.cell(2).unwrap().get_bytes().is_err());
    assert_eq!(reader(&runs).hint.load(Ordering::Relaxed), 1);
    reader(&runs).hint.store(0, Ordering::Relaxed);
    assert_eq!(runs.cell(2).unwrap().leg_name().unwrap_err().code, "rtdb/unsupported-vector-reader");
    assert_eq!(reader(&runs).hint.load(Ordering::Relaxed), 1);
}

#[test]
fn nullness_lookup_updates_hint_before_constant_null_and_nonnull_receivers() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    for null in [false, true] {
        let values = if null { arrow("values", Arc::new(NullArray::new(0)), true, &memory) } else { ints("values", &[], &memory) };
        let runs = run(ints("ends", &[1, 3], &memory), values, 3, 0, &memory);
        assert_eq!(runs.cell(2).unwrap().is_null().unwrap(), null);
        assert_eq!(reader(&runs).hint.load(Ordering::Relaxed), 1);
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn long_ends_narrow_for_lookup_while_external_admission_keeps_wide_validation() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let ends = arrow("ends", Arc::new(Int64Array::from(vec![(1i64 << 32) + 2])), false, &memory);
    let values = ints("values", &[42], &memory);
    assert!(storage::check_runs(ends.node(), values.node(), 2).is_err());
    let runs = run(ends, values, 2, 0, &memory);
    assert_eq!(runs.cell(1).unwrap().get_int(NullChecks::Enabled).unwrap(), 42);
    assert_eq!(reader(&runs).index(PhysicalIndex(2)).unwrap().0, 1);
}

#[test]
fn nested_encoded_ends_read_through_their_own_hint_and_long_getter() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let ends = run(ints("inner-ends", &[1, 2], &memory), ints("inner-values", &[1, 3], &memory), 2, 0, &memory);
    let alias = ends.clone();
    let outer = run(ends, ints("values", &[7, 9], &memory), 3, 0, &memory);
    assert_eq!(outer.cell(2).unwrap().get_int(NullChecks::Enabled).unwrap(), 9);
    assert_eq!(reader(&outer).hint.load(Ordering::Relaxed), 1);
    assert_eq!(reader(&alias).hint.load(Ordering::Relaxed), 1);
    let snapshot = outer.snapshot_reader(&memory).unwrap();
    assert_eq!(reader(&snapshot).hint.load(Ordering::Relaxed), 0);
    let NodeLayout::RunEnd(inner) = &reader(&snapshot).ends().storage else { panic!() };
    assert_eq!(inner.hint.load(Ordering::Relaxed), 0);
    assert_eq!(reader(&alias).hint.load(Ordering::Relaxed), 1);
}

#[test]
fn signed_hint_and_logical_offset_wrap_before_retained_span_admission() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let runs = run(ints("ends", &[1], &memory), ints("values", &[9], &memory), 1, 0, &memory);
    reader(&runs).hint.store(i32::MAX, Ordering::Relaxed);
    assert_eq!(reader(&runs).index(PhysicalIndex(0)).unwrap_err().code, "rtdb/row-index");
    assert_eq!(reader(&runs).hint.load(Ordering::Relaxed), i32::MAX);
    reader(&runs).hint.store(i32::MIN, Ordering::Relaxed);
    assert_eq!(reader(&runs).index(PhysicalIndex(0)).unwrap_err().code, "rtdb/row-index");
    assert_eq!(reader(&runs).hint.load(Ordering::Relaxed), i32::MIN);
    let sliced = run(ints("ends", &[1, 3], &memory), ints("values", &[7, 9], &memory), 1, 1, &memory);
    assert_eq!(sliced.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 9);
    let wrapped = run(ints("ends", &[1], &memory), ints("values", &[7], &memory), 1, i32::MAX as usize, &memory);
    assert_eq!(reader(&wrapped).index(PhysicalIndex(1)).unwrap().0, 0);
}

#[test]
fn snapshot_admits_metadata_before_cloning_and_retains_its_lease_through_selected_aliases() {
    let source_memory = MemoryBudget::new(8 * 1024 * 1024);
    let target_memory = MemoryBudget::new(1);
    let runs = wrapped(&source_memory);
    move_past_end(&runs);
    let source_charge = source_memory.used_bytes();
    let node_refs = match &runs.view { ReaderView::Direct(node) => Arc::strong_count(node), _ => panic!() };
    assert!(runs.snapshot_reader(&target_memory).is_err());
    assert_eq!(source_memory.used_bytes(), source_charge);
    assert_eq!(target_memory.used_bytes(), 0);
    let ReaderView::Direct(node) = &runs.view else { panic!() };
    assert_eq!(Arc::strong_count(node), node_refs);
    assert_eq!(reader(&runs).hint.load(Ordering::Relaxed), 3);
    let target_memory = MemoryBudget::new(8 * 1024 * 1024);
    let slice = runs.select(&Selection::All).unwrap();
    let selected = slice.select(&Selection::All).unwrap();
    let snapshot = selected.snapshot_reader(&target_memory).unwrap();
    let (Address::Indices { selection: original, .. }, Address::Indices { selection: copied, .. }) = (&selected.address, &snapshot.address) else { panic!() };
    assert!(Arc::ptr_eq(original, copied));
    let retained = snapshot.owned_cell(0).unwrap();
    drop((runs, slice, selected, snapshot));
    assert!(source_memory.used_bytes() > 0);
    assert!(target_memory.used_bytes() > 0);
    assert_eq!(retained.as_ref().get_int(NullChecks::Enabled).unwrap(), 11);
    drop(retained);
    assert_eq!(source_memory.used_bytes(), 0);
    assert_eq!(target_memory.used_bytes(), 0);
}

#[test]
fn variable_snapshot_demands_terminal_offset_after_capture_and_recomputes_only_copy() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let target = MemoryBudget::new(8 * 1024 * 1024);
    let broken = variable(1, &0i32.to_le_bytes(), 19, &memory);
    let before = memory.used_bytes();
    assert_eq!(broken.snapshot_reader(&target).unwrap_err().code, "rtdb/row-index");
    assert_eq!(memory.used_bytes(), before);
    assert_eq!(target.used_bytes(), 0);
    assert_eq!(last_offset(&broken), 19);
    let empty = variable(0, &[], 19, &memory);
    let empty_snapshot = empty.snapshot_reader(&target).unwrap();
    assert_eq!(last_offset(&empty_snapshot), 0);
    assert_eq!(last_offset(&empty), 19);
    let offsets = [0i32.to_le_bytes(), 3i32.to_le_bytes()].concat();
    let complete = variable(1, &offsets, 19, &memory);
    let copied = complete.snapshot_reader(&target).unwrap();
    assert_eq!(last_offset(&copied), 3);
    assert_eq!(last_offset(&complete), 19);
    assert_eq!(copied.cell(0).unwrap().get_bytes().unwrap(), b"abc");
    let (NodeLayout::Scalar(original), NodeLayout::Scalar(snapshot)) = (&complete.node().storage, &copied.node().storage) else { panic!() };
    let (NativeScalarData::Variable { data: original, .. }, NativeScalarData::Variable { data: snapshot, .. }) = (&original.data, &snapshot.data) else { panic!() };
    assert!(std::ptr::eq(original.bytes.as_slice().as_ptr(), snapshot.bytes.as_slice().as_ptr()));
    drop((broken, empty, empty_snapshot, complete, copied));
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn failed_recursive_snapshot_releases_captured_children_without_changing_source_hints() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let target = MemoryBudget::new(8 * 1024 * 1024);
    let runs = wrapped(&memory);
    move_past_end(&runs);
    let broken = variable(1, &[], 23, &memory);
    let children = vec![runs.clone().into_direct().unwrap(), broken.clone().into_direct().unwrap()];
    let descriptors = children.iter().map(|child| Arc::clone(&child.descriptor)).collect::<Vec<_>>();
    let fields = [
        super::super::descriptor::StructFieldRef { key: descriptors[0].name(), descriptor: &descriptors[0] },
        super::super::descriptor::StructFieldRef { key: descriptors[1].name(), descriptor: &descriptors[1] },
    ];
    let descriptor = VectorDescriptor::native_container("parent", false, ContainerShapeRef::Struct(&fields), &memory).unwrap();
    let parent = native(descriptor, 1, NodeLayout::Struct { validity: None, children }, &memory);
    let before = memory.used_bytes();
    assert_eq!(parent.snapshot_reader(&target).unwrap_err().code, "rtdb/row-index");
    assert_eq!(memory.used_bytes(), before);
    assert_eq!(target.used_bytes(), 0);
    assert_eq!(reader(&runs).hint.load(Ordering::Relaxed), 3);
    assert_eq!(last_offset(&broken), 23);
    drop((runs, broken, descriptors, parent));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn snapshot_resets_each_repeated_child_reader_without_changing_shared_payload_or_source_hint() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let target = MemoryBudget::new(8 * 1024 * 1024);
    let runs = wrapped(&memory);
    move_past_end(&runs);
    let child = runs.clone().into_direct().unwrap();
    let children = vec![Arc::clone(&child), Arc::clone(&child)];
    let descriptors = children.iter().map(|child| Arc::clone(&child.descriptor)).collect::<Vec<_>>();
    let descriptor = VectorDescriptor::native_container("union", false, ContainerShapeRef::OrdinalUnion(&descriptors), &memory).unwrap();
    let parent = native(descriptor, 1, NodeLayout::Union {
        type_ids: bytes(&[0], &memory), offsets: bytes(&0i32.to_le_bytes(), &memory), children,
    }, &memory);
    let snapshot = parent.snapshot_reader(&target).unwrap();
    let NodeLayout::Union { children, .. } = &snapshot.node().storage else { panic!() };
    assert!(!Arc::ptr_eq(&children[0], &children[1]));
    let (NodeLayout::RunEnd(first), NodeLayout::RunEnd(second)) = (&children[0].storage, &children[1].storage) else { panic!() };
    assert_eq!(first.hint.load(Ordering::Relaxed), 0);
    assert_eq!(second.hint.load(Ordering::Relaxed), 0);
    first.index(PhysicalIndex(i32::MAX)).unwrap();
    assert_eq!(first.hint.load(Ordering::Relaxed), 3);
    assert_eq!(second.hint.load(Ordering::Relaxed), 0);
    assert_eq!(reader(&runs).hint.load(Ordering::Relaxed), 3);
    let (NodeLayout::Leaf { array: old, .. }, NodeLayout::Leaf { array: copied, .. }) = (&reader(&runs).values().storage, &first.values().storage) else { panic!() };
    assert!(Arc::ptr_eq(old, copied));
    drop((runs, child, descriptors, parent, snapshot));
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn list_snapshot_captures_child_before_terminal_offset_and_skips_terminal_at_zero_count() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let target = MemoryBudget::new(8 * 1024 * 1024);
    let runs = wrapped(&memory);
    move_past_end(&runs);
    let child = runs.clone().into_direct().unwrap();
    let descriptor = VectorDescriptor::native_container("list", false, ContainerShapeRef::List(&child.descriptor), &memory).unwrap();
    let broken = native(Arc::clone(&descriptor), 1, NodeLayout::List { validity: None, offsets: bytes(&[], &memory), elements: Arc::clone(&child) }, &memory);
    let before = memory.used_bytes();
    assert_eq!(broken.snapshot_reader(&target).unwrap_err().code, "rtdb/row-index");
    assert_eq!(memory.used_bytes(), before);
    assert_eq!(target.used_bytes(), 0);
    assert_eq!(reader(&runs).hint.load(Ordering::Relaxed), 3);
    let empty = native(descriptor, 0, NodeLayout::List { validity: None, offsets: bytes(&[], &memory), elements: child }, &memory);
    let copied = empty.snapshot_reader(&target).unwrap();
    let NodeLayout::List { elements, .. } = &copied.node().storage else { panic!() };
    let NodeLayout::RunEnd(reader) = &elements.storage else { panic!() };
    assert_eq!(reader.hint.load(Ordering::Relaxed), 0);
    drop((runs, broken, empty, copied));
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(target.used_bytes(), 0);
}
