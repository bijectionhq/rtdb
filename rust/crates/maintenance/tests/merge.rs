use rtdb_types::StructFields;
use std::collections::BTreeMap;
use std::sync::Arc;
use bytes::Bytes;
use rtdb_columnar::MemoryBudget;
use rtdb_format::{data::{StoredEvent, StoredOperation, EventBatch, build_event_batch}, ipc::{ArrowFile, write_file}, keys::{TableSlug, TrieKey}, metadata::{MetadataBuilder, MetadataFile, MetadataNode}};
use rtdb_index::{PartitionState, PartitionValue, TableCatalog, TrieCatalog};
use rtdb_maintenance::{CompactionExecutor, CompactionJob, MergeConfig, RecencyMode, SegmentCompactor, RecencyPartition, recency_partition};
use rtdb_storage::{MemoryStore, ObjectKey, ObjectStore};
use rtdb_trie::TrieConfig;
use rtdb_types::{BlockIndex, Iid, Interval, LogicalType, ScalarType, StorageEpoch, StorageVersion, TableRef, Timestamp, Value};
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn mixed_trie_depths_preserve_every_row_in_the_requested_partition() {
    use rtdb_format::data::build_event_batch_with_recency;

    let temporary = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let table = TableRef::new("public", "docs");
    let slug = TableSlug::for_unrecorded_table(&table).unwrap();
    let fields = StructFields::from([("value".into(), LogicalType::Scalar(ScalarType::I64))]);
    let rows = |keys: &[u8], origin: i64, recency| -> Vec<StoredEvent> {
        keys.iter().map(|key| {
            let mut iid = [0; 16];
            iid[0] = *key;
            StoredEvent {
                origin: None,
                commit_position: 0,
                entity_key: None,
                iid: Iid(iid),
                system_from: Timestamp(origin),
                valid_time: Interval::from_micros(origin * 10, origin * 10 + 5).unwrap(),
                operation: StoredOperation::Put(StructFields::from([("value".into(), Value::I64(origin * 256 + i64::from(*key)))])),
                recency,
            }
        }).collect()
    };
    let broad = rows(&[0x00, 0x10, 0x20, 0x30, 0x40, 0x50, 0x80, 0xc0], 1, None);
    let broad_batch = build_event_batch(&broad, Some(fields.clone()), None).unwrap();
    let mut broad_metadata = MetadataBuilder::new(false, true);
    broad_metadata.write_leaf(&broad_batch).unwrap();
    let mut input_rows = broad;

    let mut refined_metadata = MetadataBuilder::new(true, true);
    let mut refined_batches = Vec::new();
    let mut add_leaf = |keys| {
        let events = rows(keys, 2, Some(Timestamp(10)));
        let batch = build_event_batch_with_recency(&events, Some(fields.clone()), None, true).unwrap();
        let node = refined_metadata.write_leaf(&batch).unwrap();
        refined_batches.push(batch);
        input_rows.extend(events);
        node
    };
    let zero = add_leaf(&[0x00, 0x0f]);
    let one = add_leaf(&[0x10]);
    let two = add_leaf(&[0x20]);
    let shallow = add_leaf(&[0x40, 0x50]);
    let older = refined_metadata.write_iid_branch([Some(zero), None, Some(two), None]).unwrap();
    let newer = refined_metadata.write_iid_branch([None, Some(one), None, None]).unwrap();
    let recency = refined_metadata.write_recency_branch(vec![(Timestamp(10), older), (Timestamp(10), newer)]).unwrap();
    refined_metadata.write_iid_branch([Some(recency), Some(shallow), None, None]).unwrap();

    let recency_partition = rtdb_types::CivilDate::from_ymd(1970, 1, 5).unwrap();
    let mut inputs = Vec::new();
    for (index, (batches, builder)) in [(vec![broad_batch], broad_metadata), (refined_batches, refined_metadata)].into_iter().enumerate() {
        let key = TrieKey::new(2, Some(recency_partition), Vec::new(), BlockIndex(index as i64)).unwrap();
        let data = write_file(&batches[0].schema(), &batches).unwrap();
        let (metadata, _) = builder.finish().unwrap();
        store.put(&ObjectKey::new(slug.data_key(&key)).unwrap(), data.into()).await.unwrap();
        store.put(&ObjectKey::new(slug.metadata_key(&key)).unwrap(), metadata.write().unwrap().into()).await.unwrap();
        inputs.push(key);
    }

    let tables = TableCatalog::default().with_seeded_table(table.clone(), ["value".into()]).unwrap();
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(TrieCatalog::default(), tables, TrieConfig::default()).unwrap()));
    let memory = MemoryBudget::new(64 * 1024 * 1024);
    let executor = SegmentCompactor::new(store.clone(), state, Arc::clone(&memory), StorageVersion(6), StorageEpoch(0), MergeConfig {
        page_size: 2, temporary_directory: temporary.path().into(), ..Default::default()
    }).unwrap();
    let in_partition = |row: &StoredEvent, part: &[u8]| part.iter().enumerate().all(|(index, bucket)| {
        (row.iid.0[0] >> (6 - 2 * index)) & 3 == *bucket
    });
    for part in [vec![], vec![0], vec![0, 1], vec![1, 0], vec![2], vec![3, 3]] {
        let output = TrieKey::new(3, Some(recency_partition), part.clone(), BlockIndex(1)).unwrap();
        let job = CompactionJob { table: table.clone(), inputs: inputs.clone(), part: part.clone(), output: output.clone(), recency_mode: RecencyMode::Preserve };
        let notice = executor.execute(&job, &CancellationToken::new()).await.unwrap();
        assert_eq!(notice.tries.len(), 1);
        let data = store.get(&ObjectKey::new(slug.data_key(&output)).unwrap()).await.unwrap();
        let mut reader = ArrowFile::open(Arc::from(data.as_ref()), None).unwrap();
        let mut observed = Vec::new();
        for page in 0..reader.page_count() {
            let batch = EventBatch::new(reader.read_page(page).unwrap()).unwrap();
            for row in 0..batch.len() { observed.push(batch.read_event(row).unwrap()); }
        }
        let mut expected: Vec<_> = input_rows.iter().filter(|row| in_partition(row, &part)).cloned().collect();
        expected.sort_by_key(|row| (row.iid, std::cmp::Reverse(row.system_from)));
        for row in &mut expected { row.recency = None; }
        assert_eq!(observed, expected, "compaction changed row occurrences for partition {part:?}");
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn scan_tie_cutover_reuses_existing_legacy_bytes_and_publishes_new_bytes_at_the_new_epoch() {
    let temporary = tempfile::tempdir().unwrap();
    let store = Arc::new(MemoryStore::default());
    let table = TableRef::new("public", "docs");
    let slug = TableSlug::for_unrecorded_table(&table).unwrap();
    let fields = StructFields::from([("value".into(), LogicalType::Scalar(ScalarType::I64))]);
    let mut inputs = Vec::new();
    for value in [2, 1] {
        let key = TrieKey::new(1, None, Vec::new(), BlockIndex(value)).unwrap();
        let rows = [StoredEvent {
            origin: None,
            commit_position: 0, entity_key: None, iid: Iid([1; 16]), system_from: Timestamp(100),
            valid_time: Interval::from_micros(10, 20).unwrap(),
            operation: StoredOperation::Put(StructFields::from([("value".into(), Value::I64(value))])),
            recency: None,
        }];
        let data = build_event_batch(&rows, Some(fields.clone()), None).unwrap();
        let mut metadata = MetadataBuilder::new(false, true);
        metadata.write_leaf(&data).unwrap();
        let (metadata, _) = metadata.finish().unwrap();
        store.put(&ObjectKey::new(slug.data_key(&key)).unwrap(), write_file(&data.schema(), &[data]).unwrap().into()).await.unwrap();
        store.put(&ObjectKey::new(slug.metadata_key(&key)).unwrap(), metadata.write().unwrap().into()).await.unwrap();
        inputs.push(key);
    }
    let tables = TableCatalog::default().with_seeded_table(table.clone(), ["value".into()]).unwrap();
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(TrieCatalog::default(), tables, TrieConfig::default()).unwrap()));
    let memory = MemoryBudget::new(64 * 1024 * 1024);
    let executor = |cutover| {
        SegmentCompactor::new(store.clone(), state.clone(), Arc::clone(&memory), StorageVersion(6), StorageEpoch(0), MergeConfig {
            temporary_directory: temporary.path().into(), scan_tie_order_from_naming_epoch: cutover, ..Default::default()
        }).unwrap()
    };
    let output = TrieKey::new(2, None, Vec::new(), BlockIndex(2)).unwrap();
    let mut job = CompactionJob { table, inputs, part: vec![], output: output.clone(), recency_mode: RecencyMode::Preserve };
    let cancel = CancellationToken::new();
    executor(None).execute(&job, &cancel).await.unwrap();
    let legacy = ObjectKey::new(slug.data_key(&output)).unwrap();
    let before = store.get(&legacy).await.unwrap();

    let upgraded = executor(std::num::NonZeroU64::new(1));
    upgraded.execute(&job, &cancel).await.unwrap();
    assert_eq!(store.get(&legacy).await.unwrap(), before);

    job.output = output.with_epoch(1);
    upgraded.execute(&job, &cancel).await.unwrap();
    let replacement = store.get(&ObjectKey::new(slug.data_key(&job.output)).unwrap()).await.unwrap();
    let first = |bytes: Bytes| {
        let mut reader = ArrowFile::open(Arc::from(bytes.as_ref()), None).unwrap();
        let batch = EventBatch::new(reader.read_page(0).unwrap()).unwrap();
        let StoredOperation::Put(document) = batch.read_event(0).unwrap().operation else { panic!("expected a put") };
        document.get("value").unwrap().clone()
    };
    assert_eq!(first(before), Value::I64(2));
    assert_eq!(first(replacement), Value::I64(1));
}

#[tokio::test]
async fn same_time_clipping_and_erase_preserve_the_surrounding_history() {
    let temporary = tempfile::tempdir().unwrap();
    let table = TableRef::new("public", "docs");
    let slug = TableSlug::for_unrecorded_table(&table).unwrap();
    let input = TrieKey::l0(BlockIndex(1)).unwrap();
    let output = TrieKey::new(1, None, Vec::new(), BlockIndex(1)).unwrap();
    let store = Arc::new(MemoryStore::default());
    let a = Iid([1; 16]);
    let b = Iid([2; 16]);
    let row = |iid, system, lower, upper, operation| StoredEvent { origin: None, commit_position: 0, entity_key: None,
        iid, system_from: Timestamp(system), valid_time: Interval::from_micros(lower, upper).unwrap(), operation, recency: None,
    };
    let put = |value| StoredOperation::Put(StructFields::from([("value".into(), Value::I64(value))]));
    let selected = [
        row(a, 300, 5, 10, StoredOperation::Delete),
        row(a, 300, 0, 10, put(30)),
        row(a, 200, i64::MIN, i64::MAX, StoredOperation::Erase),
        row(a, 100, -4, 12, put(10)),
        row(b, 150, 5, 13, put(15)),
    ];
    let untouched = [row(a, 50, -4, 12, put(5)), row(b, 50, -6, 2, put(6))];
    let fields = StructFields::from([("value".into(), LogicalType::Scalar(ScalarType::I64))]);
    let data = build_event_batch(&selected, Some(fields), None).unwrap();
    let mut metadata = MetadataBuilder::new(false, true);
    metadata.write_leaf(&data).unwrap();
    let (metadata, _) = metadata.finish().unwrap();
    let bytes = write_file(&data.schema(), &[data]).unwrap();
    store.put(&ObjectKey::new(slug.data_key(&input)).unwrap(), bytes.into()).await.unwrap();
    store.put(&ObjectKey::new(slug.metadata_key(&input)).unwrap(), metadata.write().unwrap().into()).await.unwrap();

    let tables = TableCatalog::default().with_seeded_table(table.clone(), ["value".into()]).unwrap();
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(TrieCatalog::default(), tables, TrieConfig::default()).unwrap()));
    let memory = MemoryBudget::new(64 * 1024 * 1024);
    let executor = SegmentCompactor::new(store.clone(), state, Arc::clone(&memory), StorageVersion(6), StorageEpoch(0), MergeConfig {
        page_size: 2, temporary_directory: temporary.path().into(), ..Default::default()
    }).unwrap();
    let job = CompactionJob { table, inputs: vec![input], part: Vec::new(), output: output.clone(), recency_mode: RecencyMode::Preserve };
    let notice = executor.execute(&job, &CancellationToken::new()).await.unwrap();
    assert_eq!(notice.tries.len(), 1);
    let compacted = store.get(&ObjectKey::new(slug.data_key(&output)).unwrap()).await.unwrap();
    let mut reader = ArrowFile::open(Arc::from(compacted.as_ref()), None).unwrap();
    let mut rows = Vec::new();
    for page in 0..reader.page_count() {
        let batch = EventBatch::new(reader.read_page(page).unwrap()).unwrap();
        for index in 0..batch.len() { rows.push(batch.read_event(index).unwrap()); }
    }
    assert!(rows.iter().all(|row| row.iid != a || row.system_from != Timestamp(100)));

    let read = |input: &[StoredEvent], iid, system: i64, valid: i64| {
        for row in input.iter().filter(|row| row.iid == iid) {
            if matches!(row.operation, StoredOperation::Erase) { return None; }
            if row.system_from.0 <= system && row.valid_time.lower().0 <= valid && valid < row.valid_time.upper().0 {
                return match &row.operation {
                    StoredOperation::Put(document) => document.get("value").cloned(),
                    StoredOperation::Delete => None,
                    StoredOperation::Erase => unreachable!(),
                };
            }
        }
        None
    };
    let original: Vec<_> = selected.into_iter().chain(untouched.iter().cloned()).collect();
    rows.extend(untouched);
    for iid in [a, b] {
        for system in [0, 50, 99, 100, 150, 199, 200, 299, 300, 301] {
            for valid in -7..=14 {
                assert_eq!(read(&rows, iid, system, valid), read(&original, iid, system, valid),
                    "compaction changed the historical point for {iid}, system {system}, valid {valid}");
            }
        }
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn l0_merge_splits_one_history_across_recency_files_and_pages() {
    let temp = tempfile::tempdir().unwrap();
    let table = TableRef::new("public", "docs");
    let slug = TableSlug::for_unrecorded_table(&table).unwrap();
    let key = TrieKey::l0(BlockIndex(0)).unwrap();
    let store = Arc::new(MemoryStore::default());
    let fields = BTreeMap::from([("value".into(), LogicalType::Scalar(ScalarType::I64))]);
    let week = 7 * 86_400_000_000i64;
    let rows: Vec<_> = (1..=13).rev().map(|time| if time <= 6 { time } else { week + time })
        .map(|time| StoredEvent { origin: None, commit_position: 0, entity_key: None, iid: Iid([0; 16]), system_from: Timestamp(time), valid_time: Interval::new(Timestamp(0), Timestamp::MAX).unwrap(),
        operation: StoredOperation::Put(StructFields::from([("value".into(), Value::I64(time))])), recency: None }).collect();
    let data = build_event_batch(&rows, Some(fields.into()), None).unwrap();
    let mut metadata = MetadataBuilder::new(false, true);
    metadata.write_leaf(&data).unwrap();
    let (metadata, _) = metadata.finish().unwrap();
    store.put(&ObjectKey::new(slug.data_key(&key)).unwrap(), Bytes::from(write_file(&data.schema(), &[data]).unwrap())).await.unwrap();
    store.put(&ObjectKey::new(slug.metadata_key(&key)).unwrap(), Bytes::from(metadata.write().unwrap())).await.unwrap();
    let tables = TableCatalog::default().with_seeded_table(table.clone(), ["value".into()]).unwrap();
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(TrieCatalog::default(), tables, TrieConfig::default()).unwrap()));
    let memory = MemoryBudget::new(64 * 1024 * 1024);
    let executor = SegmentCompactor::new(store.clone(), state, memory.clone(), StorageVersion(6), StorageEpoch(0), MergeConfig {
        page_size: 2, within_file_recency_branching: true, run_end_encoded_iids: true, temporary_directory: temp.path().into(), ..Default::default()
    }).unwrap();
    let job = CompactionJob { table, inputs: vec![key], part: vec![], output: TrieKey::new(1, None, vec![], BlockIndex(0)).unwrap(), recency_mode: RecencyMode::Partition };
    let notification = executor.execute(&job, &CancellationToken::new()).await.unwrap();
    assert_eq!(notification.tries.len(), 3);
    let mut counts = BTreeMap::new();
    let mut versions = Vec::new();
    for detail in &notification.tries {
        let key: TrieKey = detail.trie_key.as_ref().unwrap().parse().unwrap();
        let bytes = store.get(&ObjectKey::new(slug.data_key(&key)).unwrap()).await.unwrap();
        let mut reader = ArrowFile::open(Arc::from(bytes.as_ref()), None).unwrap();
        let mut count = 0;
        for page in 0..reader.page_count() {
            let page = EventBatch::new(reader.read_page(page).unwrap()).unwrap();
            assert!(page.len() <= 2);
            count += page.len();
            for row in 0..page.len() {
                let header = page.header(row).unwrap();
                assert_eq!(header.iid, Iid([0; 16]));
                versions.push(header.system_from);
            }
        }
        counts.insert(key.recency(), count);
        if key.recency().is_some() {
            assert!(reader.page_count() >= 3);
            let metadata = store.get(&ObjectKey::new(slug.metadata_key(&key)).unwrap()).await.unwrap();
            assert!(MetadataFile::read(Arc::from(metadata.as_ref())).unwrap().nodes().iter().any(|node| matches!(node, MetadataNode::RecencyBranch(_))));
        }
    }
    assert_eq!(counts, BTreeMap::from([
        (None, 1),
        (Some(recency_partition(Timestamp(6), RecencyPartition::Week).unwrap()), 5),
        (Some(recency_partition(Timestamp(week + 7), RecencyPartition::Week).unwrap()), 7),
    ]));
    versions.sort_unstable();
    assert_eq!(versions, rows.iter().rev().map(|row| row.system_from).collect::<Vec<_>>());
    assert_eq!(executor.execute(&job, &CancellationToken::new()).await.unwrap(), notification);
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn compressed_data_and_metadata_compact_with_identical_retry_bytes() {
    use arrow_ipc::writer::{FileWriter, IpcWriteOptions};
    for codec in [arrow_ipc::CompressionType::LZ4_FRAME, arrow_ipc::CompressionType::ZSTD] {
        let temporary = tempfile::tempdir().unwrap();
        let table = TableRef::new("public", "compressed"); let slug = TableSlug::for_unrecorded_table(&table).unwrap();
        let input = TrieKey::l0(BlockIndex(0)).unwrap(); let store = Arc::new(MemoryStore::default());
        let rows = vec![StoredEvent { origin: None, commit_position: 0, entity_key: None, iid: Iid([0; 16]), system_from: Timestamp(10), valid_time: Interval::ALL, operation: StoredOperation::Put(StructFields::from([("value".into(), Value::I64(42))])), recency: None }];
        let data = build_event_batch(&rows, Some(StructFields::from([("value".into(), LogicalType::Scalar(ScalarType::I64))])), None).unwrap();
        let mut metadata = MetadataBuilder::new(false, true); metadata.write_leaf(&data).unwrap(); let (metadata, _) = metadata.finish().unwrap();
        for (key, bytes) in [(ObjectKey::new(slug.data_key(&input)).unwrap(), write_file(&data.schema(), &[data]).unwrap()), (ObjectKey::new(slug.metadata_key(&input)).unwrap(), metadata.write().unwrap())] {
            let mut reader = ArrowFile::open(bytes.into(), None).unwrap();
            let options = IpcWriteOptions::default().try_with_compression(Some(codec)).unwrap();
            let mut writer = FileWriter::try_new_with_options(Vec::new(), &reader.schema(), options).unwrap();
            for page in 0..reader.page_count() { writer.write(&reader.read_page(page).unwrap()).unwrap(); }
            writer.finish().unwrap(); store.put(&key, writer.into_inner().unwrap().into()).await.unwrap();
        }
        let tables = TableCatalog::default().with_seeded_table(table.clone(), ["value".into()]).unwrap();
        let state = Arc::new(PartitionState::new(PartitionValue::loaded(TrieCatalog::default(), tables, TrieConfig::default()).unwrap()));
        let memory = MemoryBudget::new(1 << 29);
        let executor = SegmentCompactor::new(store.clone(), state, Arc::clone(&memory), StorageVersion(6), StorageEpoch(0), MergeConfig { temporary_directory: temporary.path().into(), ..Default::default() }).unwrap();
        let job = CompactionJob { table, inputs: vec![input], part: vec![], output: TrieKey::new(1, None, vec![], BlockIndex(0)).unwrap(), recency_mode: RecencyMode::Partition };
        let first = executor.execute(&job, &CancellationToken::new()).await.unwrap();
        let output: TrieKey = first.tries[0].trie_key.as_ref().unwrap().parse().unwrap();
        let key = ObjectKey::new(slug.data_key(&output)).unwrap(); let bytes = store.get(&key).await.unwrap();
        let mut reader = ArrowFile::open(Arc::from(bytes.as_ref()), None).unwrap();
        assert_eq!(EventBatch::new(reader.read_page(0).unwrap()).unwrap().read_event(0).unwrap(), rows[0]);
        assert_eq!(executor.execute(&job, &CancellationToken::new()).await.unwrap(), first);
        assert_eq!(store.get(&key).await.unwrap(), bytes);
        assert_eq!(memory.used_bytes(), 0); assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 0);
    }
}

#[test]
fn recency_partition_boundaries_are_inclusive() {
    let monday = rtdb_types::CivilDate::from_ymd(2020, 1, 6).unwrap();
    let micros = monday.epoch_day() * 86_400_000_000;
    assert_eq!(recency_partition(Timestamp(micros), RecencyPartition::Week).unwrap(), monday);
    assert_eq!(recency_partition(Timestamp(micros + 1), RecencyPartition::Week).unwrap(), rtdb_types::CivilDate::from_ymd(2020, 1, 13).unwrap());
}

#[test]
fn recency_partitions_cover_wide_calendar_years_and_all_timestamp_values() {
    use rtdb_types::CivilDate;
    for year in [-280_000, 0, 280_000] {
        for (partition, month, day, next_year, next_month, next_day) in [
            (RecencyPartition::Week, 1, 3, year, 1, 10),
            (RecencyPartition::Month, 1, 1, year, 2, 1),
            (RecencyPartition::Quarter, 1, 1, year, 4, 1),
            (RecencyPartition::Year, 1, 1, year + 1, 1, 1),
        ] {
            let boundary = CivilDate::from_ymd(year, month, day).unwrap();
            let micros = boundary.epoch_day() * 86_400_000_000;
            assert_eq!(recency_partition(Timestamp(micros), partition).unwrap(), boundary);
            assert_eq!(recency_partition(Timestamp(micros + 1), partition).unwrap(), CivilDate::from_ymd(next_year, next_month, next_day).unwrap());
        }
    }
    for partition in [RecencyPartition::Week, RecencyPartition::Month, RecencyPartition::Quarter, RecencyPartition::Year] {
        assert!(recency_partition(Timestamp::MIN, partition).is_ok());
        assert!(recency_partition(Timestamp::MAX, partition).is_ok());
    }
}

#[tokio::test]
async fn compaction_preserves_raw_decimal_coefficients_in_current_and_historical_pages() {
    use rtdb_format::data::copy_event_batch;
    use rtdb_types::DecimalValue;
    let temporary = tempfile::tempdir().unwrap();
    let table = TableRef::new("public", "decimals");
    let slug = TableSlug::for_unrecorded_table(&table).unwrap();
    let input = TrieKey::l0(BlockIndex(0)).unwrap();
    let store = Arc::new(MemoryStore::default());
    let ty = LogicalType::Scalar(ScalarType::Decimal { precision: -1, scale: i32::MAX, bit_width: 128 });
    let fields = BTreeMap::from([("value".into(), ty)]);
    let events: Vec<_> = [123i128, 456].into_iter().enumerate().map(|(index, coefficient)| StoredEvent {
        origin: None,
        commit_position: 0,
        entity_key: None,
        iid: Iid([0; 16]), system_from: Timestamp(20 - index as i64), valid_time: Interval::ALL,
        operation: StoredOperation::Put(StructFields::from([("value".into(), Value::Decimal(DecimalValue::new(coefficient.to_be_bytes().to_vec(), -1, i32::MAX, 128).unwrap()))])), recency: None,
    }).collect();
    let data = copy_event_batch(&events, Some(fields.into()), None).unwrap();
    let mut metadata = MetadataBuilder::new(false, true);
    metadata.write_leaf(&data).unwrap();
    let (metadata, _) = metadata.finish().unwrap();
    store.put(&ObjectKey::new(slug.data_key(&input)).unwrap(), Bytes::from(write_file(&data.schema(), &[data]).unwrap())).await.unwrap();
    store.put(&ObjectKey::new(slug.metadata_key(&input)).unwrap(), Bytes::from(metadata.write().unwrap())).await.unwrap();
    let tables = TableCatalog::default().with_seeded_table(table.clone(), ["value".into()]).unwrap();
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(TrieCatalog::default(), tables, TrieConfig::default()).unwrap()));
    let memory = MemoryBudget::new(64 << 20);
    let executor = SegmentCompactor::new(store.clone(), state, memory.clone(), StorageVersion(6), StorageEpoch(0), MergeConfig {
        page_size: 1, run_end_encoded_iids: true, temporary_directory: temporary.path().into(), ..Default::default()
    }).unwrap();
    let job = CompactionJob { table, inputs: vec![input], part: vec![], output: TrieKey::new(1, None, vec![], BlockIndex(0)).unwrap(), recency_mode: RecencyMode::Partition };
    let notification = executor.execute(&job, &CancellationToken::new()).await.unwrap();
    let mut actual = BTreeMap::new();
    let mut written = BTreeMap::new();
    for detail in &notification.tries {
        let key: TrieKey = detail.trie_key.as_ref().unwrap().parse().unwrap();
        let object = ObjectKey::new(slug.data_key(&key)).unwrap();
        let bytes = store.get(&object).await.unwrap();
        written.insert(object, bytes.clone());
        let mut file = ArrowFile::open(Arc::from(bytes.as_ref()), None).unwrap();
        for page in 0..file.page_count() {
            let batch = EventBatch::new(file.read_page(page).unwrap()).unwrap();
            for row in 0..batch.len() {
                let event = batch.read_event(row).unwrap();
                assert_eq!(event.valid_time, Interval::ALL);
                actual.insert((key.recency().is_some(), event.system_from), event.operation);
            }
        }
    }
    assert_eq!(actual, BTreeMap::from([((false, events[0].system_from), events[0].operation.clone()), ((true, events[1].system_from), events[1].operation.clone())]));
    assert_eq!(executor.execute(&job, &CancellationToken::new()).await.unwrap(), notification);
    for (object, bytes) in written { assert_eq!(store.get(&object).await.unwrap(), bytes); }
    assert_eq!(std::fs::read_dir(temporary.path()).unwrap().count(), 0);
    assert_eq!(memory.used_bytes(), 0);
}
