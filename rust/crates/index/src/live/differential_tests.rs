use super::*;
use std::cmp::Reverse;

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, bound: u64) -> u64 { self.next() % bound }
}

/// A table's rows as the writes that made them hold them, and the column types folded from those writes.
#[derive(Clone, Default)]
struct ExpectedTable {
    rows: Vec<(Arc<IndexBatch>, usize)>,
    column_types: StructFields<LogicalType>,
}

impl ExpectedTable {
    fn header(&self, row: u32) -> EventHeader {
        let (batch, offset) = &self.rows[row as usize];
        batch.header(*offset).unwrap()
    }
    fn event(&self, row: u32) -> StoredEvent {
        let (batch, offset) = &self.rows[row as usize];
        batch.view().read_event(*offset).unwrap()
    }
}

fn random_event(rng: &mut Rng, system_from: Timestamp) -> StoredEvent {
    let id = rng.below(40) as i64;
    let operation = match rng.below(10) {
        0 | 1 => StoredOperation::Delete,
        2 => StoredOperation::Erase,
        _ => {
            let mut document = StructFields::from([("_id".into(), Value::I64(id))]);
            match rng.below(4) {
                0 => { document.insert("v".into(), Value::I64(rng.below(1000) as i64)); }
                1 => { document.insert("v".into(), Value::Utf8(format!("v{}", rng.below(1000)))); }
                2 => { document.insert("v".into(), Value::F64(rng.below(1000) as f64 / 8.0)); }
                _ => {}
            }
            if rng.below(5) == 0 { document.insert("w".into(), Value::Bool(rng.below(2) == 0)); }
            StoredOperation::Put(document)
        }
    };
    let valid_time = match operation {
        StoredOperation::Erase => Interval::ALL,
        _ if rng.below(3) == 0 => Interval::new(system_from, Timestamp::MAX).unwrap(),
        _ => Interval::ALL,
    };
    StoredEvent {
        position: None, origin: None, commit_position: 0, entity_key: None, iid: EntityId::Integer(id).iid(),
        system_from, valid_time, operation, recency: None,
    }
}

fn verify(table: &LiveTable, expected: &ExpectedTable, memory: &Arc<MemoryBudget>) {
    let rows = u32::try_from(expected.rows.len()).unwrap();
    assert_eq!(table.row_count(), rows);
    assert!(identical_types(table.column_types(), &expected.column_types), "{:?} differs from {:?}", table.column_types(), expected.column_types);
    assert_eq!(table.batches().map(|batch| batch.len()).sum::<usize>(), expected.rows.len());
    assert!(table.batches_from(rows).next().is_none());
    for row in 0..rows {
        let header = expected.header(row);
        assert_eq!(table.header(row).unwrap(), header);
        assert_eq!(table.iid(row).unwrap(), header.iid);
        assert_eq!(table.row(row).unwrap().event().unwrap(), expected.event(row));
        let at = table.locate(row).unwrap();
        assert_eq!(table.header_at(at).unwrap(), header);
        let (batch, offset) = table.batch_at(at).unwrap();
        assert_eq!(batch.header(offset as usize).unwrap(), header);
        assert_eq!(table.row_at(at).unwrap().event().unwrap(), expected.event(row));
        let (batch, skip) = table.batches_from(row).next().unwrap();
        assert_eq!(batch.header(skip).unwrap(), header);
    }
    let mut order = (0..rows).collect::<Vec<_>>();
    order.sort_by_key(|&row| (expected.header(row).iid, Reverse(row)));
    assert_eq!(table.trie().sorted_rows(table).unwrap(), order);
    for leaf in table.trie().leaves() {
        let located = table.located_rows(leaf).unwrap();
        let leaf_rows = leaf.sorted_rows(table).unwrap();
        assert_eq!(located.located.len(), leaf_rows.len());
        for ((at, header), row) in located.located.iter().zip(&located.headers).zip(leaf_rows.iter()) {
            assert_eq!(*at, table.locate(*row).unwrap());
            assert_eq!(*header, expected.header(*row));
        }
    }
    let iids = (0..rows).map(|row| expected.header(row).iid).collect::<std::collections::BTreeSet<_>>();
    for iid in iids {
        let history = (0..rows).rev().map(|row| expected.header(row)).filter(|header| header.iid == iid).collect::<Vec<_>>();
        assert_eq!(table.iid_headers(iid).unwrap().collect::<Vec<_>>(), history);
    }
    if rows > 0 {
        let pages = table.block_pages(memory).unwrap();
        let actual = pages.copy(&order).unwrap();
        let selected = order.iter().map(|&row| expected.event(row)).collect::<Vec<_>>();
        let flushed = IndexBatch::copy_events_with_put_schema(&selected, table.column_types(), memory).unwrap();
        assert_eq!(actual.batch(), flushed.arrow());
    }
}

fn verify_index(live: &LiveIndex, expected: &BTreeMap<TableRef, ExpectedTable>, memory: &Arc<MemoryBudget>) {
    assert_eq!(live.tables().keys().collect::<Vec<_>>(), expected.keys().collect::<Vec<_>>());
    for (table, expected) in expected { verify(&live.tables()[table], expected, memory); }
}

#[test]
fn appended_live_versions_hold_exactly_their_writes_rows_and_keep_them_after_later_and_forked_appends() {
    let memory = MemoryBudget::new(1 << 30);
    let tables = ["a", "b", "c"].map(|name| TableRef::new("public", name));
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut versions = vec![(LiveIndex::new(BlockIndex(0), None, TrieConfig::default()), BTreeMap::<TableRef, ExpectedTable>::new())];
    for tx in 1..=700i64 {
        let base_index = if rng.below(12) == 0 { rng.below(versions.len() as u64) as usize } else { versions.len() - 1 };
        let (base, base_expected) = versions[base_index].clone();
        if rng.below(120) == 0 {
            versions.push((base.next_block().unwrap(), BTreeMap::new()));
            continue;
        }
        let system_time = Timestamp(tx);
        let mut writes = BTreeMap::new();
        let mut expected = base_expected.clone();
        for table in &tables {
            if rng.below(3) == 0 { continue; }
            let count = match rng.below(20) { 0 => 1 + rng.below(60) as usize, 1 => 2 + rng.below(4) as usize, _ => 1 };
            let events = (0..count).map(|_| random_event(&mut rng, system_time)).collect::<Vec<_>>();
            let batch = IndexBatch::copy_events(&events, &StructFields::new(), &memory).unwrap();
            let write = TableWrite::new(StructFields::new(), Some(Arc::clone(&batch)));
            let table_expected = expected.entry(table.clone()).or_default();
            table_expected.column_types = merge_column_types(&table_expected.column_types, write.column_types());
            table_expected.rows.extend((0..batch.len()).map(|row| (Arc::clone(&batch), row)));
            writes.insert(table.clone(), write);
        }
        let key = TransactionKey { tx_id: rtdb_types::TxId(tx), system_time };
        let staged = StagedWrite::new(key, base.latest_completed_tx(), base.block_index(), writes).unwrap();
        let next = base.apply(&staged).unwrap();
        verify_index(&next, &expected, &memory);
        verify_index(&base, &base_expected, &memory);
        versions.push((next, expected));
    }
    assert!(versions.iter().any(|(live, _)| live.tables().values().any(|table| table.relation.len() > TrieConfig::default().log_limit())));
    for (live, expected) in &versions { verify_index(live, expected, &memory); }
    drop(versions);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn appending_from_a_version_whose_next_slot_is_taken_keeps_both_branches() {
    let memory = MemoryBudget::new(1 << 26);
    let table = TableRef::new("public", "docs");
    let write = |id: i64, time: i64| {
        let event = StoredEvent {
            position: None, origin: None, commit_position: 0, entity_key: None, iid: EntityId::Integer(id).iid(),
            system_from: Timestamp(time), valid_time: Interval::ALL, recency: None,
            operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(id))])),
        };
        TableWrite::new(StructFields::new(), Some(IndexBatch::copy_events(&[event], &StructFields::new(), &memory).unwrap()))
    };
    let mut base = LiveTable::new(table, BlockIndex(0), TrieConfig::default());
    for id in 0..5 { base = base.append(&write(id, id), Timestamp(id)).unwrap(); }
    let bytes_at_base = memory.used_bytes();
    let left = base.append(&write(100, 10), Timestamp(10)).unwrap();
    let right = base.append(&write(200, 11), Timestamp(11)).unwrap();
    let right_next = right.append(&write(300, 12), Timestamp(12)).unwrap();
    assert_eq!((base.row_count(), left.row_count(), right.row_count(), right_next.row_count()), (5, 6, 6, 7));
    assert_eq!(left.header(5).unwrap().iid, EntityId::Integer(100).iid());
    assert_eq!(right.header(5).unwrap().iid, EntityId::Integer(200).iid());
    assert_eq!(right_next.header(5).unwrap().iid, EntityId::Integer(200).iid());
    assert_eq!(right_next.header(6).unwrap().iid, EntityId::Integer(300).iid());
    assert!(base.header(5).is_err());
    for row in 0..5 {
        assert_eq!(left.header(row).unwrap(), base.header(row).unwrap());
        assert_eq!(right.header(row).unwrap(), base.header(row).unwrap());
        assert!(Arc::ptr_eq(left.batch_at((row, 0)).unwrap().0, base.batch_at((row, 0)).unwrap().0));
        assert!(Arc::ptr_eq(right.batch_at((row, 0)).unwrap().0, base.batch_at((row, 0)).unwrap().0));
    }
    drop((left, right, right_next));
    assert_eq!(memory.used_bytes(), bytes_at_base, "discarded branches release their batches with no orphaned slots");
    drop(base);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn growing_relation_exceeds_trie_log_limit_with_stable_prefix_and_no_orphaned_slots() {
    let memory = MemoryBudget::new(1 << 26);
    let mut live = LiveTable::new(TableRef::new("public", "docs"), BlockIndex(0), TrieConfig::default());
    let beyond_limit = TrieConfig::default().log_limit() * 3 + 7;
    let mut snapshots = Vec::new();
    for id in 0..beyond_limit as i64 {
        let event = StoredEvent {
            position: None, origin: None, commit_position: 0, entity_key: None, iid: EntityId::Integer(id).iid(),
            system_from: Timestamp(id), valid_time: Interval::ALL, recency: None,
            operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(id))])),
        };
        let batch = IndexBatch::copy_events(&[event], &StructFields::new(), &memory).unwrap();
        live = live.append(&TableWrite::new(StructFields::new(), Some(batch)), Timestamp(id)).unwrap();
        if id % 13 == 0 {
            snapshots.push((id as u32 + 1, live.clone()));
        }
    }
    assert_eq!(live.relation.len(), beyond_limit);
    assert_eq!(live.row_count(), beyond_limit as u32);
    for (prefix_rows, snap) in &snapshots {
        assert_eq!(snap.row_count(), *prefix_rows);
        assert_eq!(snap.relation.len(), *prefix_rows as usize);
        assert!(snap.header(*prefix_rows).is_err());
        for row in 0..*prefix_rows {
            assert_eq!(snap.locate(row).unwrap(), (row, 0));
            assert_eq!(live.locate(row).unwrap(), (row, 0));
            assert!(Arc::ptr_eq(snap.batch_at((row, 0)).unwrap().0, live.batch_at((row, 0)).unwrap().0));
        }
    }
    let bytes_before_discard = memory.used_bytes();
    let discarded_event = StoredEvent {
        position: None, origin: None, commit_position: 0, entity_key: None, iid: EntityId::Integer(999_999).iid(),
        system_from: Timestamp(beyond_limit as i64), valid_time: Interval::ALL, recency: None,
        operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(999_999))])),
    };
    let discarded = live.append(
        &TableWrite::new(StructFields::new(), Some(IndexBatch::copy_events(&[discarded_event], &StructFields::new(), &memory).unwrap())),
        Timestamp(beyond_limit as i64),
    ).unwrap();
    assert!(memory.used_bytes() > bytes_before_discard);
    drop(discarded);
    assert_eq!(memory.used_bytes(), bytes_before_discard, "discarded optimistic append leaves no orphaned slot");
    let committed_event = StoredEvent {
        position: None, origin: None, commit_position: 0, entity_key: None, iid: EntityId::Integer(beyond_limit as i64).iid(),
        system_from: Timestamp(beyond_limit as i64), valid_time: Interval::ALL, recency: None,
        operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(beyond_limit as i64))])),
    };
    let next = live.append(
        &TableWrite::new(StructFields::new(), Some(IndexBatch::copy_events(&[committed_event], &StructFields::new(), &memory).unwrap())),
        Timestamp(beyond_limit as i64),
    ).unwrap();
    assert_eq!(next.locate(beyond_limit as u32).unwrap(), (beyond_limit as u32, 0));
    assert_eq!(next.header(beyond_limit as u32).unwrap().iid, EntityId::Integer(beyond_limit as i64).iid());
    drop((snapshots, live, next));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn concurrent_readers_see_stable_prefix_while_writer_appends() {
    let memory = MemoryBudget::new(1 << 26);
    let mut live = LiveTable::new(TableRef::new("public", "docs"), BlockIndex(0), TrieConfig::default());
    let make_write = |id: i64| {
        let event = StoredEvent {
            position: None, origin: None, commit_position: 0, entity_key: None, iid: EntityId::Integer(id).iid(),
            system_from: Timestamp(id), valid_time: Interval::ALL, recency: None,
            operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(id))])),
        };
        TableWrite::new(StructFields::new(), Some(IndexBatch::copy_events(&[event], &StructFields::new(), &memory).unwrap()))
    };
    for id in 0..32i64 {
        live = live.append(&make_write(id), Timestamp(id)).unwrap();
    }
    let pinned = Arc::new(live.clone());
    let readers: Vec<_> = (0..4).map(|_| {
        let snapshot = Arc::clone(&pinned);
        std::thread::spawn(move || {
            for _ in 0..64 {
                assert_eq!(snapshot.row_count(), 32);
                assert!(snapshot.header(32).is_err());
                for row in 0..32u32 {
                    assert_eq!(snapshot.locate(row).unwrap(), (row, 0));
                    assert_eq!(snapshot.header(row).unwrap().iid, EntityId::Integer(i64::from(row)).iid());
                }
            }
        })
    }).collect();
    for id in 32..256i64 {
        live = live.append(&make_write(id), Timestamp(id)).unwrap();
    }
    for reader in readers { reader.join().unwrap(); }
    assert_eq!(live.row_count(), 256);
    assert_eq!(pinned.row_count(), 32);
    drop((pinned, live));
    assert_eq!(memory.used_bytes(), 0);
}
