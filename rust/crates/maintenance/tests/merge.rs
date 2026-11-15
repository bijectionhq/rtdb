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
async fn l0_merge_splits_recency_retries_identically_and_removes_temporary_files() {
    let temp = tempfile::tempdir().unwrap();
    let table = TableRef::new("public", "docs");
    let slug = TableSlug::for_unrecorded_table(&table).unwrap();
    let key = TrieKey::l0(BlockIndex(0)).unwrap();
    let store = Arc::new(MemoryStore::default());
    let fields = BTreeMap::from([("value".into(), LogicalType::Scalar(ScalarType::I64))]);
    let rows: Vec<_> = (1..=7).rev().map(|time| StoredEvent { iid: Iid([0; 16]), system_from: Timestamp(time), valid_time: Interval::new(Timestamp(0), Timestamp::MAX).unwrap(),
        operation: StoredOperation::Put(BTreeMap::from([("value".into(), Value::I64(time))])), recency: None }).collect();
    let data = build_event_batch(&rows, Some(fields), None).unwrap();
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
    assert_eq!(notification.tries.len(), 2);
    let mut counts = BTreeMap::new();
    for detail in &notification.tries {
        let key: TrieKey = detail.trie_key.as_ref().unwrap().parse().unwrap();
        let bytes = store.get(&ObjectKey::new(slug.data_key(&key)).unwrap()).await.unwrap();
        let mut reader = ArrowFile::open(Arc::from(bytes.as_ref()), None).unwrap();
        let mut count = 0;
        for page in 0..reader.page_count() { count += EventBatch::new(reader.read_page(page).unwrap()).unwrap().len(); }
        counts.insert(key.recency().is_some(), count);
        if key.recency().is_some() {
            assert!(reader.page_count() >= 3);
            let metadata = store.get(&ObjectKey::new(slug.metadata_key(&key)).unwrap()).await.unwrap();
            assert!(MetadataFile::read(Arc::from(metadata.as_ref())).unwrap().nodes().iter().any(|node| matches!(node, MetadataNode::RecencyBranch(_))));
        }
    }
    assert_eq!(counts, BTreeMap::from([(false, 1), (true, 6)]));
    assert_eq!(executor.execute(&job, &CancellationToken::new()).await.unwrap(), notification);
    assert_eq!(std::fs::read_dir(temp.path()).unwrap().count(), 0);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn recency_partition_boundaries_are_inclusive() {
    let monday = chrono::NaiveDate::from_ymd_opt(2020, 1, 6).unwrap();
    let micros = monday.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp_micros();
    assert_eq!(recency_partition(Timestamp(micros), RecencyPartition::Week).unwrap(), monday);
    assert_eq!(recency_partition(Timestamp(micros + 1), RecencyPartition::Week).unwrap(), chrono::NaiveDate::from_ymd_opt(2020, 1, 13).unwrap());
}
