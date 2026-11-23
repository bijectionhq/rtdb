use rtdb_types::StructFields;
use std::collections::BTreeMap;
use std::sync::Arc;

use rtdb_columnar::MemoryBudget;
use rtdb_format::{block::proto as pb, catalog::BlockCatalog, keys::TrieKey};
use rtdb_index::{BlockPreparation, CatalogTrie, PartitionState, Snapshot, StagedWrite, StoredOperation, TransactionBuffer};
use rtdb_temporal::HistoryResolver;
use rtdb_trie::{EventMerge, TrieConfig};
use rtdb_types::{BlockIndex, EntityId, Interval, LogicalType, ScalarType, TableRef, TemporalBounds, Timestamp, TransactionKey, TxId, Value};

fn table() -> TableRef { TableRef::new("public", "docs") }
fn key(id: i64) -> TransactionKey { TransactionKey { tx_id: TxId(id), system_time: Timestamp(100 + id) } }
fn document(id: i64, value: i64) -> StructFields<Value> { StructFields::from([("_id".into(), Value::I64(id)), ("v".into(), Value::I64(value))]) }
fn rows(snapshot: &Snapshot) -> u32 { snapshot.segments(&table()).iter().map(|segment| segment.row_count()).sum() }

fn stage(state: &PartitionState, budget: &Arc<MemoryBudget>, id: i64) -> StagedWrite {
    let mut tx = TransactionBuffer::new(state.resolving_snapshot(&[]).unwrap(), key(id), Arc::clone(budget)).unwrap();
    tx.put(table(), document(id, id), Interval::ALL).unwrap();
    tx.stage().unwrap()
}

#[test]
fn stored_event_copy_preserves_raw_decimal_and_retains_charge_after_index_release() {
    use rtdb_index::{IndexBatch, StoredEvent};
    use rtdb_types::{DecimalValue, Iid};
    let memory = MemoryBudget::new(16 << 20);
    for precision in [i32::MIN, 0, 1, 256, i32::MAX] {
        let value = Value::Decimal(DecimalValue::new(123i128.to_be_bytes().to_vec(), precision, i32::MAX, 128).unwrap());
        let declared = StructFields::from([("v".into(), value.logical_type())]);
        let events = [StoredEvent { origin: None, commit_position: 0, entity_key: None, iid: Iid([0; 16]), system_from: Timestamp(10), valid_time: Interval::ALL,
            operation: StoredOperation::Put(StructFields::from([("v".into(), value)])), recency: None }];
        if precision < 3 { assert!(IndexBatch::from_events(&events, &declared, &memory).is_err()); }
        let copied = IndexBatch::copy_events_with_put_schema(&events, &declared, &memory).unwrap();
        assert_eq!(copied.put_types(), &declared);
        assert_eq!(copied.view().read_event(0).unwrap(), events[0]);
        let held = copied.arrow().clone();
        drop(copied);
        assert!(memory.used_bytes() > 0);
        drop(held);
        assert_eq!(memory.used_bytes(), 0);
        assert!(IndexBatch::copy_events_with_put_schema(&events, &StructFields::new(), &memory).is_err());
        let changed = StructFields::from([("v".into(), LogicalType::Scalar(ScalarType::Decimal { precision: precision.wrapping_add(1), scale: i32::MAX, bit_width: 128 }))]);
        assert!(IndexBatch::copy_events_with_put_schema(&events, &changed, &memory).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
    let exhausted = MemoryBudget::new(1);
    assert!(IndexBatch::copy_events_with_put_schema(&[], &StructFields::new(), &exhausted).is_err());
    assert_eq!(exhausted.used_bytes(), 0);
}

fn block(prepared: &BlockPreparation) -> BlockCatalog {
    let catalog = prepared.table_catalog().unwrap();
    let basis = prepared.value().live().latest_completed_tx();
    BlockCatalog::from_proto(pb::Block {
        block_index: Some(prepared.block_index().0),
        latest_completed_tx: basis.map(|basis| pb::TxKey { tx_id: Some(basis.tx_id.0), system_time: Some(basis.system_time.0) }),
        table_names: catalog.entries().map(|entry| entry.table.schema_and_table()).collect(),
        tables: catalog.entries().map(|entry| entry.to_proto()).collect(),
        ..pb::Block::default()
    }).unwrap()
}

#[test]
fn uncommitted_rows_are_visible_only_to_transaction_snapshot() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(1 << 24);
    let mut tx = TransactionBuffer::new(state.resolving_snapshot(&[]).unwrap(), key(0), Arc::clone(&budget)).unwrap();
    tx.put(table(), document(0, 42), Interval::ALL).unwrap();
    assert_eq!(rows(&state.open_snapshot(None)), 0);
    assert_eq!(rows(&tx.snapshot().unwrap()), 1);
    assert!(!state.open_snapshot(None).contains_table(&table()));
    assert!(tx.snapshot().unwrap().contains_table(&table()));
    assert_eq!(state.value().live().block_row_count(), 0);
    drop(tx);
    assert_eq!(rows(&state.open_snapshot(None)), 0);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn rejected_event_releases_its_reservation_and_does_not_create_a_table() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(4096);
    let mut tx = TransactionBuffer::new(state.resolving_snapshot(&[]).unwrap(), key(0), Arc::clone(&budget)).unwrap();
    assert!(tx.put(table(), document(0, 42), Interval::ALL).is_err());
    assert_eq!(budget.used_bytes(), 0);
    assert!(!tx.snapshot().unwrap().tables().contains(&table()));
}

#[test]
fn events_appended_by_reference_and_by_value_stage_as_one_tables_rows() {
    use rtdb_index::StoredEvent;
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(1 << 24);
    let mut tx = TransactionBuffer::new(state.resolving_snapshot(&[]).unwrap(), key(0), Arc::clone(&budget)).unwrap();
    let event = |id| StoredEvent { origin: None, commit_position: 0, entity_key: None, iid: EntityId::Integer(id).iid(), system_from: key(0).system_time, valid_time: Interval::ALL, operation: StoredOperation::Put(document(id, id)), recency: None };
    tx.append_to(&table(), event(1)).unwrap();
    tx.append(table(), event(2)).unwrap();
    tx.append_to(&table(), event(3)).unwrap();
    let staged = tx.stage().unwrap();
    assert_eq!(staged.tables().keys().collect::<Vec<_>>(), [&table()]);
    assert_eq!(staged.row_count().unwrap(), 3);
}

#[test]
fn staged_predecessors_and_own_writes_are_each_scanned_once() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(1 << 24);
    state.apply(&stage(&state, &budget, 0)).unwrap();
    let staged = Arc::new(stage(&state, &budget, 1));
    let base = state.resolving_snapshot(&[Arc::clone(&staged)]).unwrap();
    let mut own = TransactionBuffer::new(base, key(2), Arc::clone(&budget)).unwrap();
    own.put(table(), document(2, 2), Interval::ALL).unwrap();
    assert_eq!(rows(&own.snapshot().unwrap()), 3);
    assert_eq!(rows(&state.open_snapshot(Some(key(0).tx_id))), 1);
    assert_eq!(state.value().live().block_row_count(), 1);
    state.apply(&staged).unwrap();
    state.apply(&own.stage().unwrap()).unwrap();
    assert_eq!(state.value().live().block_row_count(), 3);
}

#[test]
fn cached_snapshot_rebuilds_only_when_a_newer_basis_is_demanded() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(1 << 24);
    state.apply(&stage(&state, &budget, 0)).unwrap();
    let held = state.open_snapshot(Some(key(0).tx_id));
    state.apply(&stage(&state, &budget, 1)).unwrap();
    let reused = state.open_snapshot(Some(key(0).tx_id));
    assert_eq!(rows(&reused), 1);
    assert!(Arc::ptr_eq(&held.segments(&table())[0], &reused.segments(&table())[0]));
    let fresh = state.open_snapshot(Some(key(1).tx_id));
    assert_eq!(rows(&fresh), 2);
    assert_eq!(fresh.tx_basis(), Some(key(1)));
    assert_eq!(rows(&held), 1);
}

#[test]
fn cached_snapshot_minimum_is_the_transaction_position_even_when_system_times_tie() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let first = key(0);
    let second = TransactionKey { system_time: first.system_time, ..key(1) };
    state.apply(&StagedWrite::new(first, None, state.snapshot().block_index(), Default::default()).unwrap()).unwrap();
    let held = state.open_snapshot(Some(first.tx_id));
    state.apply(&StagedWrite::new(second, Some(first), held.block_index(), Default::default()).unwrap()).unwrap();
    assert_eq!(state.open_snapshot(Some(second.tx_id)).tx_basis(), Some(second));
    assert_eq!(held.tx_basis(), Some(first));
}

#[test]
fn published_l0_hides_live_rows_but_keeps_types_and_pinned_catalogs() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(1 << 24);
    state.apply(&stage(&state, &budget, 0)).unwrap();
    let held = state.open_snapshot(None);
    let additions = BTreeMap::from([(table(), vec![CatalogTrie::notification(TrieKey::l0(BlockIndex(0)).unwrap(), 10, None).unwrap()])]);
    state.add_tries(&additions, Timestamp(101)).unwrap();
    let after = state.open_snapshot(None);
    assert_eq!(rows(&after), 0);
    assert_eq!(rows(&held), 1);
    assert!(after.contains_table(&table()));
    assert!(held.contains_table(&table()));
    assert_eq!(after.column_types(&table())["v"], LogicalType::Scalar(ScalarType::I64));
    assert_eq!(after.trie_entries(&table()).len(), 1);
    assert!(held.trie_entries(&table()).is_empty());
}

#[test]
fn block_adoption_publishes_catalogs_and_rollover_together() {
    let state = Arc::new(PartitionState::empty(TrieConfig::default()).unwrap());
    let budget = MemoryBudget::new(1 << 24);
    state.apply(&stage(&state, &budget, 0)).unwrap();
    let held = state.open_snapshot(None);
    let prepared = state.prepare_block().unwrap();
    let durable = block(&prepared);
    let tries = BTreeMap::from([(table(), vec![CatalogTrie::notification(TrieKey::l0(BlockIndex(0)).unwrap(), 10, None).unwrap()])]);
    let observer = Arc::clone(&state);
    let reader = std::thread::spawn(move || {
        for _ in 0..128 {
            let value = observer.value();
            if value.trie_catalog().l0_max_block_index(&table()) == BlockIndex(0) {
                assert_eq!(value.table_catalog().block_index(), Some(BlockIndex(0)));
                assert_eq!(value.live().block_index(), BlockIndex(1));
                assert!(value.live().tables().is_empty());
            }
        }
    });
    let table_blocks = prepared.table_blocks(&tries, Timestamp(101)).unwrap();
    state.adopt_block(prepared, durable, &table_blocks, &tries, Timestamp(101)).unwrap();
    reader.join().unwrap();
    assert_eq!(rows(&held), 1);
    assert_eq!(rows(&state.open_snapshot(None)), 0);
    assert!(state.open_snapshot(None).contains_table(&table()));
    assert_eq!(state.value().table_catalog().metadata(&table()).unwrap().row_count, 1);
    assert_eq!(state.value().live().block_row_count(), 0);
}

#[test]
fn retained_snapshots_resolve_their_trie_rows_after_discard_and_rollover() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(1 << 24);
    let table = table();
    state.apply(&stage(&state, &budget, 0)).unwrap();
    let pinned = state.open_snapshot(Some(key(0).tx_id));

    let mut tx = TransactionBuffer::new(
        state.resolving_snapshot(&[]).unwrap(), key(1), Arc::clone(&budget),
    ).unwrap();
    tx.put(table.clone(), document(42, 42), Interval::ALL).unwrap();
    let discarded = tx.snapshot().unwrap();
    drop(tx);
    assert_eq!(rows(&state.open_snapshot(None)), 1);

    let prepared = state.prepare_block().unwrap();
    let durable = block(&prepared);
    let catalog_trie = CatalogTrie::notification(TrieKey::l0(BlockIndex(0)).unwrap(), 10, None).unwrap();
    let tries = BTreeMap::from([(table.clone(), vec![catalog_trie])]);
    let table_blocks = prepared.table_blocks(&tries, Timestamp(101)).unwrap();
    state.adopt_block(prepared, durable, &table_blocks, &tries, Timestamp(101)).unwrap();
    state.apply(&stage(&state, &budget, 1)).unwrap();
    let next = state.open_snapshot(Some(key(1).tx_id));
    drop(state);

    for (snapshot, expected) in [(pinned, vec![0]), (discarded, vec![42, 0]), (next, vec![1])] {
        let segments = snapshot.segments(&table);
        assert_eq!(segments.len(), expected.len());
        for (segment, id) in segments.iter().zip(expected) {
            assert_eq!(segment.trie().sorted_rows(segment.as_ref()).unwrap(), [0]);
            let location = segment.locate(0).unwrap();
            let header = segment.header_at(location).unwrap();
            assert_eq!(header.iid, EntityId::Integer(id).iid());
            assert_eq!(segment.row(0).unwrap().header().unwrap(), header);
        }
    }
}

#[test]
fn failed_adoption_and_stale_apply_leave_one_unchanged_value() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(1 << 24);
    let first = stage(&state, &budget, 0);
    state.apply(&first).unwrap();
    let prepared = state.prepare_block().unwrap();
    let durable = block(&prepared);
    state.apply(&stage(&state, &budget, 1)).unwrap();
    let before = state.value();
    assert!(state.apply(&first).is_err());
    let table_blocks = prepared.table_blocks(&BTreeMap::new(), Timestamp(102)).unwrap();
    assert!(state.adopt_block(prepared, durable, &table_blocks, &BTreeMap::new(), Timestamp(102)).is_err());
    assert!(Arc::ptr_eq(&before, &state.value()));
    assert_eq!(state.value().live().block_row_count(), 2);
}

#[test]
fn empty_declared_table_is_visible_to_later_staged_transactions() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(1 << 24);
    let mut creator = TransactionBuffer::new(state.resolving_snapshot(&[]).unwrap(), key(0), Arc::clone(&budget)).unwrap();
    creator.declare_table(table(), ["v".into()]).unwrap();
    let staged = Arc::new(creator.stage().unwrap());
    let snapshot = state.resolving_snapshot(&[Arc::clone(&staged)]).unwrap();
    assert!(snapshot.tables().contains(&table()));
    assert!(snapshot.contains_table(&table()));
    assert_eq!(snapshot.column_types(&table())["v"], LogicalType::Nothing);
    assert_eq!(rows(&snapshot), 0);
    assert!(!state.open_snapshot(None).tables().contains(&table()));
    assert!(!state.open_snapshot(None).contains_table(&table()));
    state.apply(&staged).unwrap();
    assert!(state.open_snapshot(Some(key(0).tx_id)).tables().contains(&table()));
    assert!(state.open_snapshot(Some(key(0).tx_id)).contains_table(&table()));
}

#[test]
fn put_omitting_a_declared_column_preserves_its_null_field() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(1 << 24);
    let mut tx = TransactionBuffer::new(state.resolving_snapshot(&[]).unwrap(), key(0), Arc::clone(&budget)).unwrap();
    tx.declare_table(table(), ["v".into()]).unwrap();
    tx.put(table(), StructFields::from([("_id".into(), Value::I64(1))]), Interval::ALL).unwrap();

    let staged = tx.stage().unwrap();
    state.apply(&staged).unwrap();
    let snapshot = state.open_snapshot(Some(key(0).tx_id));
    assert_eq!(snapshot.column_types(&table())["v"], LogicalType::Null);
    let events = snapshot.segments(&table())[0].ordered_events().unwrap();
    assert_eq!(events[0].source.event().unwrap().operation, StoredOperation::Put(StructFields::from([
        ("_id".into(), Value::I64(1)), ("v".into(), Value::Null),
    ])));
}

#[test]
fn declared_document_types_do_not_make_written_columns_nullable() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(1 << 24);
    let mut tx = TransactionBuffer::new(state.resolving_snapshot(&[]).unwrap(), key(0), Arc::clone(&budget)).unwrap();
    tx.put(table(), StructFields::from([("_id".into(), Value::I64(1)), ("error".into(), Value::Null)]), Interval::ALL).unwrap();
    let error = "error".to_owned();
    tx.declare_document_types(&table(), [(&error, &LogicalType::Scalar(ScalarType::Transit).maybe())]).unwrap();
    state.apply(&tx.stage().unwrap()).unwrap();
    let types = state.open_snapshot(Some(key(0).tx_id)).segments(&table())[0].column_types().clone();
    assert_eq!(types, StructFields::from([("_id".into(), LogicalType::Scalar(ScalarType::I64)), ("error".into(), LogicalType::Scalar(ScalarType::Transit).maybe())]));
}

#[test]
fn table_membership_includes_seeded_empty_catalogs_and_keeps_its_captured_generation() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let before = state.open_snapshot(None);
    for name in [TableRef::new("xt", "txs"), TableRef::new("xt", "role_membership")] {
        assert!(before.contains_table(&name));
        assert!(before.segments(&name).is_empty());
    }
    assert!(!before.contains_table(&table()));
    let budget = MemoryBudget::new(1 << 24);
    state.apply(&stage(&state, &budget, 0)).unwrap();
    assert!(!before.contains_table(&table()));
    assert!(state.open_snapshot(Some(key(0).tx_id)).contains_table(&table()));
    assert!(!state.open_snapshot(Some(key(0).tx_id)).contains_table(&TableRef::new("missing", "docs")));
}

#[test]
fn put_delete_erase_and_reinsert_preserve_same_transaction_order() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(1 << 24);
    let mut tx = TransactionBuffer::new(state.resolving_snapshot(&[]).unwrap(), key(0), budget).unwrap();
    tx.put(table(), document(1, 10), Interval::ALL).unwrap();
    tx.delete(table(), &EntityId::Integer(1), Interval::ALL).unwrap();
    tx.put(table(), document(1, 20), Interval::ALL).unwrap();
    tx.erase(table(), &EntityId::Integer(1)).unwrap();
    tx.put(table(), document(1, 30), Interval::ALL).unwrap();
    let snapshot = tx.snapshot().unwrap();
    let streams: Vec<_> = snapshot.segments(&table()).iter().map(|segment| segment.ordered_events().unwrap()).collect();
    let merge = EventMerge::new(streams.iter().map(Vec::as_slice).collect()).unwrap();
    let mut resolver = HistoryResolver::new(TemporalBounds::default(), false);
    let mut documents = Vec::new();
    for event in merge {
        for row in resolver.resolve(&event.header, event.source.clone()).unwrap() {
            if let StoredOperation::Put(document) = row.source.event().unwrap().operation { documents.push(document); }
        }
    }
    assert_eq!(documents, vec![document(1, 30)]);
}

#[test]
fn retained_row_keeps_its_memory_budget_charged_after_partition_close() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(1 << 24);
    state.apply(&stage(&state, &budget, 0)).unwrap();
    let snapshot = state.open_snapshot(None);
    let row = snapshot.segments(&table())[0].row(0).unwrap();
    drop(snapshot);
    drop(state);
    assert!(budget.used_bytes() > 0);
    assert_eq!(row.event().unwrap().operation, StoredOperation::Put(document(0, 0)));
    drop(row);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn staging_releases_the_segments_the_transactions_reads_built_before_building_the_write() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(1 << 26);
    let mut tx = TransactionBuffer::new(state.resolving_snapshot(&[]).unwrap(), key(0), Arc::clone(&budget)).unwrap();
    for id in 0..500 { tx.put(table(), document(id, id), Interval::ALL).unwrap(); }
    drop(tx.snapshot().unwrap());
    let before = budget.peak_bytes();
    let staged = tx.stage().unwrap();
    assert_eq!(budget.peak_bytes(), before, "staging held the cached segments and the write together");
    drop(staged);
}
