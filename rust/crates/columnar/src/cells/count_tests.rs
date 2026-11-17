use arrow_buffer::Buffer;
use crate::MemoryBudget;
use super::native_container::{CapturedChildren, NativeContainerParts, NativeFixedKind, NativeListKind, NativeStructInput, publish_native};
use super::native_scalar::{NativeScalarData, NativeScalarStorage, ScalarOffsets, StoredBytes};
use super::*;

fn bytes(values: &[u8], memory: &Arc<MemoryBudget>) -> RetainedBuffer {
    let reservation = memory.reserve(values.len() + 128).unwrap();
    RetainedBuffer::with_reservation(Buffer::from_slice_ref(values), reservation).unwrap()
}
fn offsets(values: &[i32], memory: &Arc<MemoryBudget>) -> StoredBytes {
    let reservation = memory.reserve(values.len() * 4 + 128).unwrap();
    let mut data = Vec::with_capacity(values.len() * 4);
    for value in values { data.extend_from_slice(&value.to_le_bytes()); }
    StoredBytes { bytes: RetainedBuffer::with_reservation(Buffer::from(data), reservation).unwrap(), append: 0 }
}
fn scalar(name: &str, data_type: DataType, nullable: bool, count: i32, data: NativeScalarData, memory: &Arc<MemoryBudget>) -> ExecutionVector {
    let template = write::ScalarWriter::new(Arc::new(Field::new(name, data_type, nullable)), Arc::clone(memory)).unwrap().finish().unwrap();
    let Declaration::Arrow(ingress) = &template.node().declaration else { panic!() };
    let owner = Arc::new(NodeOwner { backing: BackingOwner::Native, metadata: memory.reserve(1024).unwrap() });
    ExecutionVector::direct(Arc::new(PhysicalNode {
        declaration: Declaration::Arrow(Arc::clone(ingress)), descriptor: Arc::clone(&template.node().descriptor), count: SourceCount::new(count),
        storage: NodeLayout::Scalar(NativeScalarStorage { validity: None, data }), owner,
    }))
}
fn ints(name: &str, values: &[i32], memory: &Arc<MemoryBudget>) -> ExecutionVector {
    let mut writer = write::ScalarWriter::new(Arc::new(Field::new(name, DataType::Int32, false)), Arc::clone(memory)).unwrap();
    for value in values { writer.write_int(*value).unwrap(); }
    writer.finish().unwrap()
}
fn variable(data_type: DataType, count: i32, positions: &[i32], memory: &Arc<MemoryBudget>) -> ExecutionVector {
    scalar("bytes", data_type, false, count, NativeScalarData::Variable {
        offsets: offsets(positions, memory), data: StoredBytes { bytes: bytes(b"abc", memory), append: 3 }, last_offset: 97,
    }, memory)
}
fn fields(entries: &[(&str, &ExecutionVector)], memory: &Arc<MemoryBudget>) -> CapturedChildren<NativeStructInput> {
    let mut captured = CapturedChildren::new(entries.len(), memory).unwrap();
    for (key, child) in entries { captured.push(NativeStructInput::new(key, (*child).clone(), memory).unwrap()).unwrap(); }
    captured
}
fn union(child: &ExecutionVector, positions: &[i32], count: i32, memory: &Arc<MemoryBudget>) -> ExecutionVector {
    let mut children = CapturedChildren::new(1, memory).unwrap();
    children.push(child.clone()).unwrap();
    let charge = memory.reserve(positions.len() + 128).unwrap();
    let type_ids = StoredBytes { bytes: RetainedBuffer::with_reservation(Buffer::from(vec![0u8; positions.len()]), charge).unwrap(), append: 0 };
    publish_native("outer", SourceCount::new(count), NativeContainerParts::Union { type_ids, offsets: offsets(positions, memory), children }, memory).unwrap()
}
fn list(kind: NativeListKind, count: i32, positions: &[i32], memory: &Arc<MemoryBudget>) -> ExecutionVector {
    let elements = if matches!(kind, NativeListKind::Map { .. }) {
        let key = ints("key", &[7], memory);
        let value = ints("value", &[11], memory);
        publish_native("entries", SourceCount::new(1), NativeContainerParts::Struct { validity: None, children: fields(&[("key", &key), ("value", &value)], memory) }, memory).unwrap()
    } else { ints("item", &[7], memory) };
    publish_native("items", SourceCount::new(count), NativeContainerParts::List {
        kind, validity: None, offsets: offsets(positions, memory), last_offset: 97, elements,
    }, memory).unwrap()
}
fn runs(count: i32, ends: &[i32], values: &[i32], memory: &Arc<MemoryBudget>) -> ExecutionVector {
    publish_native("runs", SourceCount::new(count), NativeContainerParts::RunEnd {
        ends: ints("ends", ends, memory), values: ints("values", values, memory),
    }, memory).unwrap()
}
fn reader(vector: &ExecutionVector) -> &RunEndReader {
    let NodeLayout::RunEnd(reader) = &vector.node().storage else { panic!() };
    reader
}
fn last_offset(vector: &ExecutionVector) -> i32 {
    match &vector.node().storage {
        NodeLayout::Scalar(NativeScalarStorage { data: NativeScalarData::Variable { last_offset, .. }, .. })
        | NodeLayout::NativeContainer(NativeContainerStorage::List { last_offset, .. }) => *last_offset,
        _ => panic!(),
    }
}
fn anomaly(error: VectorReadError) -> Anomaly { match error { VectorReadError::Anomaly(error) => error, _ => panic!() } }

#[test]
fn source_counts_distinguish_signed_facts_from_admitted_native_lengths() {
    for value in [i32::MIN, -1, 0, 1, i32::MAX] {
        let count = SourceCount::new(value);
        assert_eq!(count.get(), value);
        assert_eq!(count.index().0, value);
        assert_eq!(count.is_zero(), value == 0);
        assert_eq!(count.contains(0), value > 0);
        assert!(!count.contains(usize::MAX));
        if value < 0 { assert_eq!(count.checked_len().unwrap_err().code, "rtdb/vector-length"); }
        else { assert_eq!(SourceCount::from_len(count.checked_len().unwrap()).unwrap(), count); }
    }
    assert_eq!(SourceCount::from_len(i32::MAX as usize + 1).unwrap_err().code, "rtdb/vector-length");
}

#[test]
fn negative_native_counts_survive_aliases_metadata_and_snapshots_without_row_allocation() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let child = ints("child", &[7], &memory);
    let vectors = [
        publish_native("struct", SourceCount::new(i32::MIN), NativeContainerParts::Struct { validity: None, children: fields(&[("key", &child)], &memory) }, &memory).unwrap(),
        list(NativeListKind::List, i32::MIN, &[7], &memory),
        publish_native("fixed", SourceCount::new(i32::MIN), NativeContainerParts::FixedList { kind: NativeFixedKind::List(2), validity: None, elements: child.clone() }, &memory).unwrap(),
        union(&child, &[0], i32::MIN, &memory),
        runs(i32::MIN, &[1, i32::MIN], &[7, 11], &memory),
    ];
    for vector in vectors {
        let before = memory.used_bytes();
        assert_eq!(vector.value_count(), i32::MIN);
        assert!(!vector.is_empty());
        assert_eq!(vector.checked_len().unwrap_err().code, "rtdb/vector-length");
        assert_eq!(vector.cell(0).unwrap_err().code, "rtdb/row-index");
        assert_eq!(vector.owned_cell(0).unwrap_err().code, "rtdb/row-index");
        assert!(vector.logical_type().is_ok());
        assert!(vector.arrow_field().is_ok());
        for selection in [Selection::All, Selection::Range { offset: 0, length: 0 }, Selection::Indices(vec![])] {
            assert_eq!(vector.select(&selection).unwrap_err().code, "rtdb/vector-length");
        }
        assert_eq!(memory.used_bytes(), before);
        let alias = vector.clone();
        let snapshot = vector.snapshot_reader(&memory).unwrap();
        assert_eq!(alias.value_count(), i32::MIN);
        assert_eq!(snapshot.value_count(), i32::MIN);
        drop((alias, snapshot));
        assert_eq!(memory.used_bytes(), before);
    }
    drop(child);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn direct_and_selected_struct_children_keep_their_own_count_contracts() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let child = ints("child", &[7], &memory);
    let parent = publish_native("parent", SourceCount::new(-2), NativeContainerParts::Struct { validity: None, children: fields(&[("key", &child)], &memory) }, &memory).unwrap();
    assert_eq!(parent.key_names().unwrap().collect::<Vec<_>>(), vec!["key"]);
    assert_eq!(parent.vector_for("key").unwrap().unwrap().value_count(), 1);
    assert_eq!(parent.map_keys().unwrap().value_count(), 1);
    let negative = scalar("negative", DataType::Int32, false, -2, NativeScalarData::Fixed(StoredBytes { bytes: bytes(&7i32.to_le_bytes(), &memory), append: 0 }), &memory);
    let positive = publish_native("positive", SourceCount::new(1), NativeContainerParts::Struct { validity: None, children: fields(&[("key", &negative)], &memory) }, &memory).unwrap();
    assert_eq!(positive.vector_for("key").unwrap().unwrap().value_count(), -2);
    let selected = positive.select(&Selection::All).unwrap().vector_for("key").unwrap().unwrap();
    assert_eq!(selected.value_count(), 1);
    assert!(!selected.cell(0).unwrap().is_null().unwrap());
    assert_eq!(anomaly(selected.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap_err()).code, "rtdb/row-index");
    let snapshot = selected.snapshot_reader(&memory).unwrap();
    assert_eq!(snapshot.value_count(), 1);
    assert_eq!(snapshot.node().count.get(), -2);
    drop((child, parent, negative, positive, selected, snapshot));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn negative_inner_counts_do_not_preempt_constant_nullness_or_unsupported_getters() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    for data_type in [DataType::Null, DataType::Int32] {
        let is_null = data_type == DataType::Null;
        let data = if is_null { NativeScalarData::Null } else { NativeScalarData::Fixed(StoredBytes { bytes: bytes(&7i32.to_le_bytes(), &memory), append: 0 }) };
        let child = scalar("child", data_type, is_null, i32::MIN, data, &memory);
        let outer = union(&child, &[0], 1, &memory);
        let named = outer.vector_for("child").unwrap().unwrap();
        let cell = named.cell(0).unwrap();
        assert_eq!(named.value_count(), 1);
        assert_eq!(cell.is_null().unwrap(), is_null);
        assert_eq!(cell.leaf_descriptor().unwrap().name(), "child");
        assert_eq!(anomaly(cell.get_boolean(NullChecks::Enabled).unwrap_err()).code, "rtdb/unsupported-vector-reader");
        assert_eq!(anomaly(cell.get_bytes().unwrap_err()).code, "rtdb/unsupported-vector-reader");
        assert_eq!(cell.getter_index().unwrap_err().code, "rtdb/row-index");
        if is_null { assert_eq!(cell.object().unwrap().value(), &Value::Null); }
        else { assert_eq!(anomaly(cell.get_long(NullChecks::Enabled).unwrap_err()).code, "rtdb/row-index"); }
        drop((child, outer, named));
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn signed_offset_indexes_wrap_before_retained_span_admission() {
    let memory = MemoryBudget::new(1024 * 1024);
    let stored = offsets(&[7, 11], &memory);
    for reader in [ScalarOffsets::Native(stored.bytes.as_slice()), ScalarOffsets::Sdk(&[7, 11])] {
        assert_eq!(reader.get_index(PhysicalIndex(i32::MIN)).unwrap(), 7);
        assert_eq!(reader.get_index(PhysicalIndex(i32::MIN + 1)).unwrap(), 11);
        assert_eq!(reader.get_index(PhysicalIndex(-1)).unwrap_err().code, "rtdb/row-index");
        assert_eq!(reader.get_index(PhysicalIndex(536_870_912)).unwrap_err().code, "rtdb/row-index");
        assert_eq!(reader.get(0).unwrap(), 7);
        assert_eq!(reader.get(usize::MAX).unwrap_err().code, "rtdb/row-index");
    }
    drop(stored);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn variable_snapshots_recompute_signed_terminal_offsets_without_changing_original_cursors() {
    let source = MemoryBudget::new(8 * 1024 * 1024);
    let target = MemoryBudget::new(1024 * 1024);
    for data_type in [DataType::Utf8, DataType::Binary] {
        for (count, expected) in [(i32::MIN, Some(7)), (i32::MIN + 1, Some(11)), (-1, None), (536_870_912, None)] {
            let original = variable(data_type.clone(), count, &[7, 11], &source);
            let before = source.used_bytes();
            let result = original.snapshot_reader(&target);
            if let Some(expected) = expected {
                let copied = result.unwrap();
                assert_eq!(copied.value_count(), count);
                assert_eq!(last_offset(&copied), expected);
                drop(copied);
            } else { assert_eq!(result.unwrap_err().code, "rtdb/row-index"); }
            assert_eq!(last_offset(&original), 97);
            let NodeLayout::Scalar(NativeScalarStorage { data: NativeScalarData::Variable { offsets, data, .. }, .. }) = &original.node().storage else { panic!() };
            assert_eq!((offsets.append, data.append), (0, 3));
            assert_eq!(original.value_count(), count);
            assert_eq!(source.used_bytes(), before);
            assert_eq!(target.used_bytes(), 0);
            drop(original);
            assert_eq!(source.used_bytes(), 0);
        }
    }
}

#[test]
fn list_wrapper_snapshots_keep_signed_terminal_demand_and_child_ownership() {
    let source = MemoryBudget::new(8 * 1024 * 1024);
    let target = MemoryBudget::new(1024 * 1024);
    for kind in [NativeListKind::List, NativeListKind::Set, NativeListKind::Map { sorted: false }] {
        for (count, expected) in [(i32::MIN, Some(7)), (i32::MIN + 1, Some(11)), (-1, None), (536_870_912, None)] {
            let original = list(kind, count, &[7, 11], &source);
            let before = source.used_bytes();
            assert_eq!(original.list_elements().unwrap().value_count(), 1);
            let result = original.snapshot_reader(&target);
            if let Some(expected) = expected {
                let copied = result.unwrap();
                assert_eq!(copied.value_count(), count);
                assert_eq!(last_offset(&copied), expected);
                assert_eq!(copied.list_elements().unwrap().value_count(), 1);
                drop(copied);
            } else { assert_eq!(result.unwrap_err().code, "rtdb/row-index"); }
            assert_eq!(last_offset(&original), 97);
            let NodeLayout::NativeContainer(NativeContainerStorage::List { offsets, .. }) = &original.node().storage else { panic!() };
            assert_eq!(offsets.append, 0);
            assert_eq!(source.used_bytes(), before);
            assert_eq!(target.used_bytes(), 0);
            drop(original);
            assert_eq!(source.used_bytes(), 0);
        }
    }
}

#[test]
fn only_zero_count_skips_a_missing_terminal_offset() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    for count in [0, i32::MIN] {
        for vector in [variable(DataType::Binary, count, &[], &memory), list(NativeListKind::List, count, &[], &memory)] {
            let before = memory.used_bytes();
            let result = vector.snapshot_reader(&memory);
            if count == 0 {
                let copied = result.unwrap();
                assert_eq!(last_offset(&copied), 0);
                drop(copied);
            } else { assert_eq!(result.unwrap_err().code, "rtdb/row-index"); }
            assert_eq!(last_offset(&vector), 97);
            assert_eq!(memory.used_bytes(), before);
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn recursive_signed_terminal_failure_releases_captures_without_resetting_original_run_hint() {
    let source = MemoryBudget::new(8 * 1024 * 1024);
    let target = MemoryBudget::new(1024 * 1024);
    let run = runs(1, &[i32::MAX, -2, 1], &[11, 22, 33], &source);
    assert_eq!(reader(&run).index(PhysicalIndex(i32::MAX)).unwrap().0, 3);
    let broken = variable(DataType::Binary, -1, &[7], &source);
    let parent = publish_native("parent", SourceCount::new(1), NativeContainerParts::Struct { validity: None, children: fields(&[("first", &run), ("later", &broken)], &source) }, &source).unwrap();
    let before = source.used_bytes();
    assert_eq!(parent.snapshot_reader(&target).unwrap_err().code, "rtdb/row-index");
    assert_eq!(target.used_bytes(), 0);
    assert_eq!(source.used_bytes(), before);
    assert_eq!(last_offset(&broken), 97);
    assert_eq!(broken.value_count(), -1);
    assert_eq!(run.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 33);
    drop((run, broken, parent));
    assert_eq!(source.used_bytes(), 0);
}

#[test]
fn wrapped_run_prefix_counts_and_hints_remain_independent_of_logical_row_admission() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let negative = runs(-2, &[i32::MAX, -2], &[11, 22], &memory);
    assert_eq!(negative.value_count(), -2);
    let alias = union(&negative, &[0], 1, &memory);
    assert_eq!(alias.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 11);
    let negative_snapshot = negative.snapshot_reader(&memory).unwrap();
    assert_eq!(negative_snapshot.value_count(), -2);
    assert_eq!(reader(&negative_snapshot).ends().count.get(), 2);
    let recovered = runs(1, &[i32::MAX, -2, 1], &[11, 22, 33], &memory);
    let addresses = union(&recovered, &[i32::MAX, 0], 2, &memory);
    assert_eq!(anomaly(addresses.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap_err()).code, "rtdb/row-index");
    assert_eq!(addresses.cell(1).unwrap().get_int(NullChecks::Enabled).unwrap(), 33);
    let fresh = recovered.snapshot_reader(&memory).unwrap();
    assert_eq!(fresh.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 11);
    drop((negative, alias, negative_snapshot, recovered, addresses, fresh));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn negative_run_count_short_circuits_end_payload_reads_and_survives_snapshot() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let ends = scalar("ends", DataType::Int32, false, -2, NativeScalarData::Fixed(StoredBytes { bytes: bytes(&[], &memory), append: 0 }), &memory);
    let runs = publish_native("runs", SourceCount::new(1), NativeContainerParts::RunEnd { ends, values: ints("values", &[17], &memory) }, &memory).unwrap();
    assert_eq!(reader(&runs).index(PhysicalIndex(i32::MAX)).unwrap().0, 0);
    assert_eq!(runs.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 17);
    let snapshot = runs.snapshot_reader(&memory).unwrap();
    assert_eq!(reader(&snapshot).ends().count.get(), -2);
    assert_eq!(snapshot.cell(0).unwrap().get_int(NullChecks::Enabled).unwrap(), 17);
    drop((runs, snapshot));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn strict_physical_ingress_rejects_negative_declared_child_counts() {
    let source = MemoryBudget::new(8 * 1024 * 1024);
    let target = MemoryBudget::new(1024 * 1024);
    let child = scalar("child", DataType::Null, true, -2, NativeScalarData::Null, &source);
    let declaration = child.node().ingress_arrow().unwrap().clone();
    let field = Arc::new(Field::new("parent", DataType::Struct(vec![Arc::new(declaration)].into()), false));
    let before = source.used_bytes();
    let result = ExecutionVector::from_physical(field, PhysicalLayout::Struct { length: 0, validity: None, children: vec![child.clone()] }, &target);
    assert_eq!(result.unwrap_err().code, "rtdb/vector-length");
    assert_eq!(source.used_bytes(), before);
    assert_eq!(target.used_bytes(), 0);
    assert_eq!(child.value_count(), -2);
    let ends = scalar("ends", DataType::Int32, false, -2, NativeScalarData::Fixed(StoredBytes { bytes: bytes(&[], &source), append: 0 }), &source);
    assert_eq!(storage::check_runs(ends.node(), child.node(), 0).unwrap_err().code, "rtdb/vector-length");
    drop((child, ends));
    assert_eq!(source.used_bytes(), 0);
}

#[test]
fn signed_snapshot_budget_failure_is_retryable_and_success_retains_original_backing() {
    let source = MemoryBudget::new(8 * 1024 * 1024);
    let target = MemoryBudget::new(4096);
    let original = variable(DataType::Binary, i32::MIN, &[11], &source);
    let before = source.used_bytes();
    let blocker = target.reserve(target.limit_bytes()).unwrap();
    assert!(original.snapshot_reader(&target).is_err());
    assert_eq!(last_offset(&original), 97);
    assert_eq!(source.used_bytes(), before);
    drop(blocker);
    let copied = original.snapshot_reader(&target).unwrap();
    drop(original);
    assert_eq!(copied.value_count(), i32::MIN);
    assert_eq!(last_offset(&copied), 11);
    assert!(source.used_bytes() > 0);
    assert!(target.used_bytes() > 0);
    drop(copied);
    assert_eq!(source.used_bytes(), 0);
    assert_eq!(target.used_bytes(), 0);
}
