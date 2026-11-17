use arrow_array::{Int32Array, NullArray};
use arrow_buffer::Buffer;
use crate::MemoryBudget;
use super::*;

fn open(name: &str, array: ArrayRef, nullable: bool, memory: &Arc<MemoryBudget>) -> ExecutionVector {
    let field = Arc::new(Field::new(name, array.data_type().clone(), nullable));
    let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
    ExecutionVector::with_reservation(field, array, reservation).unwrap()
}
fn ints(name: &str, values: &[i32], memory: &Arc<MemoryBudget>) -> ExecutionVector { open(name, Arc::new(Int32Array::from(values.to_vec())), false, memory) }
fn bytes(value: &[u8], append: usize, memory: &Arc<MemoryBudget>) -> StoredBytes {
    let reservation = memory.reserve(value.len() + 128).unwrap();
    StoredBytes { bytes: RetainedBuffer::with_reservation(Buffer::from_slice_ref(value), reservation).unwrap(), append }
}
fn offsets(value: &[i32], append: usize, memory: &Arc<MemoryBudget>) -> StoredBytes {
    bytes(&value.iter().flat_map(|n| n.to_le_bytes()).collect::<Vec<_>>(), append, memory)
}
fn bits(value: &[u8], append: i32, byte_append: usize, memory: &Arc<MemoryBudget>) -> StoredBits {
    StoredBits { bytes: bytes(value, byte_append, memory).bytes, append, byte_append }
}
fn union(child: ExecutionVector, positions: &[i32], ids: &[u8], memory: &Arc<MemoryBudget>) -> ExecutionVector {
    publish_native("parent", positions.len(), NativeContainerParts::Union {
        type_ids: bytes(ids, 0, memory), offsets: offsets(positions, 0, memory), children: vec![child],
    }, memory).unwrap()
}
fn list(child: ExecutionVector, length: usize, positions: &[i32], last_offset: i32, memory: &Arc<MemoryBudget>) -> ExecutionVector {
    publish_native("items", length, NativeContainerParts::List {
        kind: NativeListKind::List, validity: None, offsets: offsets(positions, 0, memory), last_offset, elements: child,
    }, memory).unwrap()
}
fn anomaly(error: VectorReadError) -> Anomaly { match error { VectorReadError::Anomaly(error) => error, other => panic!("expected anomaly, got {other:?}") } }

#[test]
fn empty_run_end_null_values_keep_constant_nullness_after_named_selection() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let ends = ints("run_ends", &[], &memory);
    let values = open("values", Arc::new(NullArray::new(0)), true, &memory);
    let runs = publish_native("runs", 0, NativeContainerParts::RunEnd { ends, values }, &memory).unwrap();
    assert!(runs.logical_type().is_err());
    assert!(runs.arrow_field().is_ok());
    let parent = union(runs, &[0, -1, i32::MAX], &[255, 255, 255], &memory);
    assert!(parent.logical_type().is_err());
    assert!(parent.arrow_field().is_ok());
    let selected = parent.vector_for("runs").unwrap().unwrap().select(&Selection::All).unwrap();
    for row in 0..3 {
        let cell = selected.cell(row).unwrap();
        assert!(cell.is_null().unwrap());
        assert_eq!(cell.object().unwrap().value(), &Value::Null);
        assert_eq!(anomaly(cell.get_long(NullChecks::Enabled).unwrap_err()).code, "rtdb/unsupported-vector-reader");
        assert_eq!(cell.byte_span().unwrap_err().code, "rtdb/unsupported-vector-reader");
    }
    drop((parent, selected));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_struct_retains_independent_parent_and_child_counts() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let child = ints("child", &[7], &memory);
    let parent = publish_native("parent", 2, NativeContainerParts::Struct { validity: None, children: vec![child.clone()] }, &memory).unwrap();
    assert!(parent.ingress_field().arrow().is_none());
    assert!(!parent.cell(1).unwrap().is_null().unwrap());
    let field = parent.vector_for("child").unwrap().unwrap();
    assert_eq!(field.len(), 1);
    assert!(matches!(&field.address, Address::Identity { len: 1 }));
    assert_eq!(field.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 7);
    assert!(field.cell(1).is_err());
    assert!(parent.cell(1).unwrap().raw_struct_field("child").is_err());
    let sliced = parent.select(&Selection::All).unwrap().vector_for("child").unwrap().unwrap();
    assert_eq!(sliced.len(), 2);
    assert!(matches!(&sliced.address, Address::Range { offset: 0, len: 2 }));
    assert_eq!(anomaly(sliced.cell(1).unwrap().get_int(NullChecks::Enabled).unwrap_err()).code, "rtdb/row-index");
    let selected = parent.select(&Selection::Indices(vec![1, 0])).unwrap().vector_for("child").unwrap().unwrap();
    assert_eq!(selected.len(), 2);
    assert!(matches!(&selected.address, Address::Indices { len: 2, .. }));
    assert_eq!(anomaly(selected.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap_err()).code, "rtdb/row-index");
    assert_eq!(selected.cell(1).unwrap().get_int(NullChecks::Enabled).unwrap(), 7);
    let behind = publish_native("behind", 0, NativeContainerParts::Struct { validity: None, children: vec![child.clone()] }, &memory).unwrap();
    let behind_child = behind.vector_for("child").unwrap().unwrap();
    assert_eq!(behind_child.len(), 1);
    assert_eq!(behind_child.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 7);
    assert_eq!(behind.select(&Selection::All).unwrap().vector_for("child").unwrap().unwrap().len(), 0);
    assert_eq!(behind.map_keys().unwrap().len(), 1);
    let Some(ContainerKind::Struct(fields)) = parent.descriptor().container() else { panic!() };
    assert!(Arc::ptr_eq(&fields[0], &child.node().descriptor));
    let Some(ContainerStorageRef::Struct { children, .. }) = parent.node().container() else { panic!() };
    assert!(Arc::ptr_eq(&children[0].descriptor, &fields[0]));
    drop((child, parent, field, sliced, selected, behind, behind_child));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_nullable_bits_use_initialized_span_independently_of_cursors_and_parent_length() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let parent = publish_native("parent", 9, NativeContainerParts::Struct {
        validity: Some(bits(&[0b10000001], -17, 0, &memory)), children: vec![],
    }, &memory).unwrap();
    assert!(!parent.cell(0).unwrap().is_null().unwrap());
    assert!(parent.cell(1).unwrap().is_null().unwrap());
    assert!(!parent.cell(7).unwrap().is_null().unwrap());
    assert_eq!(parent.cell(8).unwrap().is_null().unwrap_err().code, "rtdb/row-index");
    let NodeLayout::NativeContainer(NativeContainerStorage::Struct { validity: Some(validity), .. }) = &parent.node().storage else { panic!() };
    assert_eq!((validity.append, validity.byte_append), (-17, 0));
    let empty = publish_native("empty", 0, NativeContainerParts::Struct { validity: Some(bits(&[1], 0, 0, &memory)), children: vec![] }, &memory).unwrap();
    let masked = union(empty, &[0], &[255], &memory);
    let named = masked.vector_for("empty").unwrap().unwrap();
    assert!(named.cell(0).unwrap().is_null().unwrap());
    let unmasked = named.select(&Selection::All).unwrap();
    assert!(!unmasked.cell(0).unwrap().is_null().unwrap());
}

#[test]
fn nested_struct_selection_retains_child_validity_without_parent_mask() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    for nullable_parent in [false, true] {
        let leaf = open("leaf", Arc::new(Int32Array::from(vec![Some(11), None])), true, &memory);
        let child = publish_native("child", 2, NativeContainerParts::Struct { validity: Some(bits(&[0], 2, 1, &memory)), children: vec![leaf] }, &memory).unwrap();
        let parent = publish_native("parent", 2, NativeContainerParts::Struct {
            validity: nullable_parent.then(|| bits(&[0], 2, 1, &memory)), children: vec![child],
        }, &memory).unwrap();
        let wrapper = union(parent, &[1, 0], &[255, 255], &memory);
        let named = wrapper.vector_for("parent").unwrap().unwrap().vector_for("child").unwrap().unwrap().vector_for("leaf").unwrap().unwrap();
        let selected = named.select(&Selection::Indices(vec![1, 0])).unwrap().select(&Selection::All).unwrap();
        assert_eq!(selected.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 11);
        assert!(selected.cell(1).unwrap().is_null().unwrap());
        assert!(selected.cell(1).unwrap().get_int(NullChecks::Enabled).is_err());
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn list_offsets_cursors_and_last_offset_remain_independent_until_snapshot() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let child = ints("item", &[4, 5, 6], &memory);
    let value = list(child, 1, &[1, 2], -31, &memory);
    let NodeLayout::NativeContainer(NativeContainerStorage::List { offsets, last_offset, .. }) = &value.node().storage else { panic!() };
    assert_eq!(offsets.append, 0);
    assert_eq!(*last_offset, -31);
    assert_eq!(value.cell(0).unwrap().list_start_index().unwrap(), 1);
    assert_eq!(value.cell(0).unwrap().list_count().unwrap(), 1);
    assert_eq!(value.cell(0).unwrap().raw_list_elements().unwrap().cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 5);
    assert_eq!(value.cell(0).unwrap().object().unwrap().value(), &Value::List(vec![Value::I32(5)]));
    let snapshot = value.snapshot_reader(&memory).unwrap();
    let NodeLayout::NativeContainer(NativeContainerStorage::List { offsets, last_offset, .. }) = &snapshot.node().storage else { panic!() };
    assert_eq!(offsets.append, 0);
    assert_eq!(*last_offset, 2);
    let NodeLayout::NativeContainer(NativeContainerStorage::List { last_offset, .. }) = &value.node().storage else { panic!() };
    assert_eq!(*last_offset, -31);
    let broken = list(ints("item", &[], &memory), 1, &[0], 71, &memory);
    assert_eq!(broken.cell(0).unwrap().list_start_index().unwrap(), 0);
    assert!(broken.cell(0).unwrap().list_count().is_err());
    let before = memory.used_bytes();
    assert!(broken.snapshot_reader(&memory).is_err());
    assert_eq!(memory.used_bytes(), before);
    let empty = list(ints("item", &[], &memory), 0, &[], 71, &memory);
    let copied = empty.snapshot_reader(&memory).unwrap();
    let NodeLayout::NativeContainer(NativeContainerStorage::List { last_offset, .. }) = &copied.node().storage else { panic!() };
    assert_eq!(*last_offset, 0);
}

#[test]
fn native_list_offsets_wrap_signed_stride_and_count_without_admitting_invalid_ranges() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let child = list(ints("item", &[], &memory), 0, &[i32::MAX, i32::MIN], 0, &memory);
    let parent = union(child, &[0, 1_073_741_824, -1_073_741_824], &[255; 3], &memory);
    let named = parent.vector_for("items").unwrap().unwrap();
    let selected = named.select(&Selection::All).unwrap();
    for view in [&named, &selected] {
        for row in 0..3 {
            assert_eq!(view.cell(row).unwrap().list_start_index().unwrap(), i32::MAX);
            assert_eq!(view.cell(row).unwrap().list_count().unwrap(), 1);
            assert!(view.cell(row).unwrap().raw_list_elements().is_err());
        }
    }
    let reversed = list(ints("item", &[1, 2], &memory), 1, &[2, 1], 0, &memory);
    assert_eq!(reversed.cell(0).unwrap().list_count().unwrap(), -1);
    assert!(reversed.cell(0).unwrap().raw_list_elements().is_err());
    assert!(reversed.cell(0).unwrap().object().is_err());
}

#[test]
fn fixed_list_preserves_signed_width_and_partial_child_count() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    for width in [-3, 0, 3] {
        let fixed = publish_native("fixed", 1, NativeContainerParts::FixedList { kind: NativeFixedKind::List(width), validity: None, elements: ints("item", &[7], &memory) }, &memory).unwrap();
        assert_eq!(fixed.cell(0).unwrap().list_count().unwrap(), width);
        assert_eq!(fixed.cell(0).unwrap().list_start_index().unwrap(), 0);
        if width != 0 { assert!(fixed.cell(0).unwrap().raw_list_elements().is_err()); }
        else { assert_eq!(fixed.cell(0).unwrap().raw_list_elements().unwrap().len(), 0); }
        assert!(fixed.arrow_field().is_ok());
        let parent = union(fixed, &[i32::MAX, -1], &[255, 255], &memory);
        let selected = parent.vector_for("fixed").unwrap().unwrap().select(&Selection::All).unwrap();
        assert_eq!(selected.cell(0).unwrap().list_start_index().unwrap(), i32::MAX.wrapping_mul(width));
        assert_eq!(selected.cell(1).unwrap().list_start_index().unwrap(), (-1i32).wrapping_mul(width));
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn union_retains_type_offset_cursors_and_defers_consuming_failures() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let child = ints("number", &[7], &memory);
    let parent = publish_native("parent", 3, NativeContainerParts::Union {
        type_ids: bytes(&[255, 0], 0, &memory), offsets: offsets(&[i32::MIN], 0, &memory), children: vec![child],
    }, &memory).unwrap();
    assert!(parent.cell(0).unwrap().is_null().unwrap());
    assert!(matches!(parent.cell(0).unwrap().get_int(NullChecks::Enabled), Err(VectorReadError::AbsentPrimitiveReceiver)));
    assert_eq!(parent.cell(1).unwrap().type_id().unwrap(), 0);
    assert!(parent.cell(1).unwrap().get_int(NullChecks::Enabled).is_err());
    assert_eq!(parent.cell(2).unwrap().type_id().unwrap_err().code, "rtdb/row-index");
    let NodeLayout::NativeContainer(NativeContainerStorage::Union { type_ids, offsets, .. }) = &parent.node().storage else { panic!() };
    assert_eq!((type_ids.append, offsets.append), (0, 0));
    let selected = parent.select(&Selection::Range { offset: 0, length: 1 }).unwrap();
    assert!(selected.cell(0).unwrap().is_null().unwrap());
}

#[test]
fn union_retains_all_ordinal_children_and_named_negative_discriminator() {
    let memory = MemoryBudget::new(64 * 1024 * 1024);
    let children = (0..130).map(|n| ints(&format!("leg{n}"), &[n], &memory)).collect::<Vec<_>>();
    let parent = publish_native("parent", 1, NativeContainerParts::Union {
        type_ids: bytes(&[128], 1, &memory), offsets: offsets(&[0], 4, &memory), children,
    }, &memory).unwrap();
    assert_eq!(parent.leg_names().unwrap().count(), 130);
    assert_eq!(parent.cell(0).unwrap().type_id().unwrap(), -128);
    assert!(parent.cell(0).unwrap().is_null().unwrap());
    assert!(parent.arrow_field().is_err());
    let named = parent.vector_for("leg128").unwrap().unwrap();
    assert!(!named.cell(0).unwrap().is_null().unwrap());
    assert_eq!(named.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 128);
    let snapshot = parent.snapshot_reader(&memory).unwrap();
    assert_eq!(snapshot.leg_names().unwrap().count(), 130);
    assert_eq!(snapshot.vector_for("leg129").unwrap().unwrap().cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 129);
    drop((parent, named, snapshot));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_run_publication_keeps_partial_counts_and_reviewed_hint_behavior() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let run = publish_native("runs", 1, NativeContainerParts::RunEnd {
        ends: ints("ends", &[i32::MAX, -2, 1], &memory), values: ints("values", &[11, 22, 33, 44], &memory),
    }, &memory).unwrap();
    assert_eq!(run.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 11);
    let parent = union(run.clone(), &[i32::MAX, 0], &[0, 0], &memory);
    let selected = parent.vector_for("runs").unwrap().unwrap().select(&Selection::All).unwrap();
    assert_eq!(selected.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 44);
    assert_eq!(selected.cell(1).unwrap().get_int(NullChecks::Enabled).unwrap(), 33);
    let snapshot = run.snapshot_reader(&memory).unwrap();
    assert_eq!(snapshot.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 11);
    let missing = publish_native("missing", 3, NativeContainerParts::RunEnd { ends: ints("ends", &[1, 3], &memory), values: ints("values", &[7], &memory) }, &memory).unwrap();
    assert!(missing.cell(2).unwrap().get_int(NullChecks::Enabled).is_err());
    drop((run, parent, selected, snapshot, missing));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn list_wrappers_retain_descriptor_identity_and_only_concrete_reader_methods() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    for kind in [NativeListKind::List, NativeListKind::Set, NativeListKind::Map { sorted: true }] {
        let list = publish_native("list", 1, NativeContainerParts::List {
            kind, validity: None, offsets: offsets(&[0, 0], 8, &memory), last_offset: 0, elements: ints("item", &[], &memory),
        }, &memory).unwrap();
        assert_eq!(list.cell(0).unwrap().list_count().unwrap(), 0);
        assert_eq!(list.list_elements().unwrap().len(), 0);
        assert!(list.vector_for("item").is_err());
        assert!(list.cell(0).unwrap().get_int(NullChecks::Enabled).is_err());
        assert!(list.key_names().is_none());
    }
    let range = publish_native("range", 1, NativeContainerParts::FixedList {
        kind: NativeFixedKind::TimestampRange, validity: None, elements: ints("raw", &[], &memory),
    }, &memory).unwrap();
    assert_eq!(range.cell(0).unwrap().list_count().unwrap(), 2);
    assert!(matches!(range.descriptor().container(), Some(ContainerKind::TimestampRange(_))));
    assert!(range.logical_type().is_ok());
    assert!(range.cell(0).unwrap().raw_list_elements().is_err());
}

#[test]
fn native_publication_rejects_uncovered_cursors_and_selected_children_without_leaks() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let child = ints("item", &[9], &memory);
    let before = memory.used_bytes();
    let bad_offsets = StoredBytes { bytes: bytes(&[], 0, &memory).bytes, append: 1 };
    assert!(publish_native("list", 1, NativeContainerParts::List { kind: NativeListKind::List, validity: None, offsets: bad_offsets, last_offset: 0, elements: child.clone() }, &memory).is_err());
    assert_eq!(memory.used_bytes(), before);
    assert!(publish_native("parent", 1, NativeContainerParts::Struct { validity: Some(bits(&[], 0, 1, &memory)), children: vec![child.clone()] }, &memory).is_err());
    assert_eq!(memory.used_bytes(), before);
    let selected = child.select(&Selection::All).unwrap();
    assert!(publish_native("parent", 1, NativeContainerParts::Struct { validity: None, children: vec![selected] }, &memory).is_err());
    assert_eq!(memory.used_bytes(), before);
    assert_eq!(child.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 9);
    drop(child);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_publication_admission_failure_preserves_children_and_retry_retains_all_leases() {
    let source = MemoryBudget::new(8 * 1024 * 1024);
    let target = MemoryBudget::new(1024 * 1024);
    let child = ints("child", &[41], &source);
    let before = source.used_bytes();
    let owners = match &child.view { ReaderView::Direct(node) => Arc::strong_count(node), _ => panic!() };
    let blocker = target.reserve(target.limit_bytes()).unwrap();
    assert!(publish_native("parent", 1, NativeContainerParts::Struct { validity: None, children: vec![child.clone()] }, &target).is_err());
    assert_eq!(source.used_bytes(), before);
    let ReaderView::Direct(node) = &child.view else { panic!() };
    assert_eq!(Arc::strong_count(node), owners);
    drop(blocker);
    assert_eq!(target.used_bytes(), 0);
    let parent = publish_native("parent", 1, NativeContainerParts::Struct { validity: None, children: vec![child.clone()] }, &target).unwrap();
    let cell = parent.cell(0).unwrap().raw_struct_field("child").unwrap().unwrap();
    let retained_parent = parent.owned_cell(0).unwrap();
    drop((child, parent));
    assert_eq!(cell.as_ref().get_int(NullChecks::Enabled).unwrap(), 41);
    assert!(source.used_bytes() > 0);
    assert!(target.used_bytes() > 0);
    drop((cell, retained_parent));
    assert_eq!(source.used_bytes(), 0);
    assert_eq!(target.used_bytes(), 0);
}

#[test]
fn native_snapshot_failure_drops_captured_children_and_preserves_initial_cursor_facts() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let target = MemoryBudget::new(8 * 1024 * 1024);
    let runs = publish_native("runs", 1, NativeContainerParts::RunEnd {
        ends: ints("ends", &[i32::MAX, -2, 1], &memory), values: ints("values", &[11, 22, 33, 44], &memory),
    }, &memory).unwrap();
    let advance = union(runs.clone(), &[i32::MAX], &[0], &memory);
    assert_eq!(advance.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 44);
    let broken = list(ints("item", &[], &memory), 1, &[], 97, &memory);
    let parent = publish_native("parent", 1, NativeContainerParts::Struct {
        validity: Some(bits(&[1], -9, 0, &memory)), children: vec![runs.clone(), broken.clone()],
    }, &memory).unwrap();
    let before = memory.used_bytes();
    assert_eq!(parent.snapshot_reader(&target).unwrap_err().code, "rtdb/row-index");
    assert_eq!(memory.used_bytes(), before);
    assert_eq!(target.used_bytes(), 0);
    assert_eq!(runs.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 33);
    let NodeLayout::NativeContainer(NativeContainerStorage::List { last_offset, .. }) = &broken.node().storage else { panic!() };
    assert_eq!(*last_offset, 97);
    let NodeLayout::NativeContainer(NativeContainerStorage::Struct { validity: Some(bits), .. }) = &parent.node().storage else { panic!() };
    assert_eq!((bits.append, bits.byte_append), (-9, 0));
    drop((runs, advance, broken, parent));
    assert_eq!(memory.used_bytes(), 0);
}
