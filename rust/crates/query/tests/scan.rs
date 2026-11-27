use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_format::{block::proto as pb, catalog::{BlockCatalog, TableBlock}, data::build_event_batch, ipc::write_file, keys::{TrieKey, block_key}, metadata::MetadataBuilder};
use rtdb_index::{CatalogTrie, PartitionState, PartitionValue, Snapshot, TableCatalog, TransactionBuffer, TrieCatalog};
use rtdb_plan::{BinaryOp, Catalog, ColumnId, Expr, Field, QualifiedTable, Relation, Scan, TemporalFilter};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, PreparedQuery, QueryContext, QueryOptions};
use rtdb_storage::{MemoryStore, ObjectKey, ObjectStore, cache::BufferPool};
use rtdb_trie::TrieConfig;
use rtdb_types::{EntityId, Iid, Interval, LogicalType, Result, ScalarType, TableRef, TimeUnit, Timestamp, TransactionKey, TxId, Value};
use tokio_util::sync::CancellationToken;

fn table() -> TableRef { TableRef::new("public", "docs") }
fn document(id: i64, value: Value) -> StructFields<Value> { StructFields::from([("_id".into(), Value::I64(id)), ("v".into(), value)]) }
fn tx(state: &PartitionState, budget: &Arc<MemoryBudget>, id: i64, system: i64) -> TransactionBuffer {
    TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(id), system_time: Timestamp(system) }, Arc::clone(budget)).unwrap()
}
fn timestamp(value: i64) -> Value { Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) } }

#[tokio::test]
async fn delegated_scan_refuses_a_later_erase_horizon_and_falls_back() {
    use rtdb_query::{FragmentDepth, ScanFragment, ScanFragmenter, evaluate_fragment};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Replica {
        context: Arc<QueryContext>,
        refusals: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl ScanFragmenter for Replica {
        fn depth(&self, _: &str, _: &TableRef, _: u64) -> Option<FragmentDepth> { FragmentDepth::new(1).ok() }

        async fn dispatch(&self, fragment: &ScanFragment) -> Result<Option<Vec<u8>>> {
            match evaluate_fragment(Arc::clone(&self.context), fragment).await {
                Ok(bytes) => Ok(Some(bytes)),
                Err(error) => {
                    if error.code == "rtdb.query/fragment-basis" { self.refusals.fetch_add(1, Ordering::Relaxed); }
                    Err(error)
                }
            }
        }
    }

    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let memory = MemoryBudget::new(1 << 27);
    let store = Arc::new(MemoryStore::default());
    let mut put = tx(&state, &memory, 0, 10);
    put.put(table(), document(1, Value::Utf8("captured".into())), Interval::ALL).unwrap();
    state.apply(&put.stage().unwrap()).unwrap();
    persist(&state, &store).await;
    let captured = state.snapshot();

    let mut erase = tx(&state, &memory, 1, 20);
    erase.erase(table(), &EntityId::Integer(1)).unwrap();
    state.apply(&erase.stage().unwrap()).unwrap();
    let later = query_context(state.snapshot(), Arc::clone(&store), false);
    let later_scan = scan(&later, &["v"], TemporalFilter::All, at(10), None);
    assert!(collect(later_scan, Arc::clone(&later)).await.unwrap().is_empty());
    let replica = Arc::new(Replica { context: later, refusals: AtomicUsize::new(0) });

    let database = DatabaseSnapshot { partitions: vec![captured], storage: Arc::new(BufferPool::new(store, 1 << 26)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap()
        .with_fragmenter(Some(replica.clone()), None);
    let coordinator = QueryContext::new(catalog, memory, Timestamp(50), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
    let relation = scan(&coordinator, &["v"], TemporalFilter::All, at(10), None);
    assert_eq!(collect(relation, coordinator).await.unwrap(), vec![vec![Value::Utf8("captured".into())]],
        "local fallback must preserve the coordinator's captured row");
    assert_eq!(replica.refusals.load(Ordering::Relaxed), 4);
}

#[tokio::test]
async fn a_fragment_reads_locally_only_when_declined_or_unavailable() {
    use rtdb_query::{FragmentDepth, ScanFragment, ScanFragmenter};
    use rtdb_types::{Anomaly, AnomalyCategory};

    enum Answer { Declined, Unavailable, Faulted, Corrupt }
    struct Peer(Answer);

    #[async_trait::async_trait]
    impl ScanFragmenter for Peer {
        fn depth(&self, _: &str, _: &TableRef, _: u64) -> Option<FragmentDepth> { FragmentDepth::new(1).ok() }

        async fn dispatch(&self, _: &ScanFragment) -> Result<Option<Vec<u8>>> {
            match self.0 {
                Answer::Declined => Ok(None),
                Answer::Unavailable => Err(Anomaly::new(AnomalyCategory::Unavailable, "test/peer-down", "The fragment's reader is down")),
                Answer::Faulted => Err(Anomaly::fault("test/peer-fault", "The fragment's reader failed")),
                Answer::Corrupt => Ok(Some(vec![0, 0, 0, 0, 0, 0, 0, 4, 0xff, 0xff, 0xff, 0xff])),
            }
        }
    }

    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let memory = MemoryBudget::new(1 << 27);
    let store = Arc::new(MemoryStore::default());
    let mut put = tx(&state, &memory, 0, 10);
    put.put(table(), document(1, Value::Utf8("persisted".into())), Interval::ALL).unwrap();
    state.apply(&put.stage().unwrap()).unwrap();
    persist(&state, &store).await;
    let snapshot = state.snapshot();
    let run = async |answer| {
        let database = DatabaseSnapshot { partitions: vec![snapshot.clone()], storage: Arc::new(BufferPool::new(Arc::clone(&store) as Arc<dyn rtdb_storage::ObjectStore>, 1 << 26)) };
        let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap()
            .with_fragmenter(Some(Arc::new(Peer(answer))), None);
        let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(50), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
        let relation = scan(&context, &["v"], TemporalFilter::All, at(10), None);
        collect(relation, context).await
    };
    let local = vec![vec![Value::Utf8("persisted".into())]];
    assert_eq!(run(Answer::Declined).await.unwrap(), local);
    assert_eq!(run(Answer::Unavailable).await.unwrap(), local);
    assert_eq!(run(Answer::Faulted).await.unwrap_err().code, "test/peer-fault");
    assert!(run(Answer::Corrupt).await.is_err(), "a corrupt reply must not be replaced by a local read");
}

#[tokio::test]
async fn a_cold_fragment_sends_selected_fixed_lists_as_arrow() {
    use arrow_array::{Array, cast::AsArray};
    use arrow_schema::DataType;
    use rtdb_query::{ScanFragment, TriePrefix, evaluate_fragment};

    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(1 << 27);
    let store = Arc::new(MemoryStore::default());
    let width = 1 << 16;
    let selected = Value::FixedSizeList(vec![Value::I64(7)].into());
    let skipped = Value::FixedSizeList(vec![Value::FixedSizeBinary(vec![13; width])].into());
    let mut write = tx(&state, &budget, 0, 10);
    write.put(table(), document(1, selected.clone()), Interval::ALL).unwrap();
    write.put(table(), document(2, skipped), Interval::ALL).unwrap();
    state.apply(&write.stage().unwrap()).unwrap();
    persist(&state, &store).await;
    let snapshot = state.snapshot();
    let basis = snapshot.tx_basis().unwrap();
    let context = query_context(snapshot, store, true);
    let relation = scan(&context, &["_id", "v"], TemporalFilter::All, at(10), None);
    let rtdb_plan::RelationNode::Scan(scan) = relation.node() else { unreachable!() };
    let mut scan = scan.clone();
    scan.predicates.push(Expr::binary(BinaryOp::NotEq, scan.columns[0].expression(), Expr::Literal(Value::I64(2))));
    let fragment = ScanFragment {
        scan,
        prefix: TriePrefix::new(Vec::new()).unwrap(),
        basis,
        residence: None,
        bounds: rtdb_types::TemporalBounds { valid_time: Interval::ALL, system_time: Interval::from_micros(10, 11).unwrap() },
        timezone: "UTC".into(),
    };

    let wire = evaluate_fragment(context, &fragment).await.unwrap();

    let (length, bytes) = wire.split_first_chunk::<8>().unwrap();
    assert_eq!(u64::from_be_bytes(*length) as usize, bytes.len());
    assert!(bytes.len() < width);
    let mut reader = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None).unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert_eq!(batch.num_rows(), 1);
    assert!(reader.next().is_none());
    let schema = batch.schema();
    let field = schema.field(1);
    assert_eq!(field.metadata()["rtdb.fragment.column"], "arrow");
    assert!(matches!(field.data_type(), DataType::FixedSizeList(_, 1)));
    let list = batch.column(1).as_fixed_size_list();
    let storage_union = list.values().as_union();
    let DataType::Union(legs, _) = storage_union.data_type() else { unreachable!() };
    let wide = legs.iter().find(|(_, field)| field.data_type() == &DataType::FixedSizeBinary(width as i32)).unwrap().0;
    assert!(storage_union.child(wide).is_empty());
    assert_eq!(read_value(field, list, 0).unwrap(), selected);
}

#[tokio::test]
async fn delegated_scan_keeps_its_snapshot_until_cancelled_provider_reads_drain() {
    use rtdb_query::{ScanFragment, TriePrefix, evaluate_fragment};
    use rtdb_storage::{IoRegion, StoredObject};
    use std::{ops::Range, path::Path, sync::{Mutex, mpsc}, time::Duration};
    use tokio::sync::oneshot;

    struct ReadGate { started: oneshot::Sender<()>, finish: mpsc::Receiver<()> }
    struct PausedStore { inner: MemoryStore, io: Arc<IoRegion>, gate: Mutex<Option<ReadGate>> }

    #[async_trait::async_trait]
    impl ObjectStore for PausedStore {
        async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head(key).await }
        async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<bytes::Bytes> { self.inner.get(key).await }
        async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<bytes::Bytes> {
            let bytes = self.inner.get_range(key, range).await?;
            let gate = self.gate.lock().unwrap().take();
            if let Some(gate) = gate {
                return self.io.run_blocking(move || {
                    let _ = gate.started.send(());
                    let _ = gate.finish.recv();
                    Ok(bytes)
                }).await;
            }
            Ok(bytes)
        }
        async fn put(&self, key: &ObjectKey, bytes: bytes::Bytes) -> rtdb_storage::Result<()> { self.inner.put(key, bytes).await }
        async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> { self.inner.put_file(key, source).await }
        async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> { self.inner.list(directory).await }
        async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.inner.delete(key).await }
    }

    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let memory = MemoryBudget::new(1 << 27);
    let inner = MemoryStore::default();
    let mut put = tx(&state, &memory, 0, 10);
    put.put(table(), document(1, Value::Utf8("captured".into())), Interval::ALL).unwrap();
    state.apply(&put.stage().unwrap()).unwrap();
    persist(&state, &inner).await;
    let snapshot = state.snapshot();
    let basis = snapshot.tx_basis().unwrap();
    let io = IoRegion::new();
    let (started, reading) = oneshot::channel();
    let (resume, finish) = mpsc::channel();
    let store = Arc::new(PausedStore { inner, io: Arc::clone(&io), gate: Mutex::new(Some(ReadGate { started, finish })) });
    let database = DatabaseSnapshot { partitions: vec![snapshot], storage: Arc::new(BufferPool::new(store, 1 << 26)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    let cancellation = CancellationToken::new();
    let context = QueryContext::new(catalog, memory, Timestamp(50), "UTC", QueryOptions::default(), &cancellation).unwrap();
    let relation = scan(&context, &["v"], TemporalFilter::All, at(10), None);
    let rtdb_plan::RelationNode::Scan(scan) = relation.node() else { unreachable!() };
    let fragment = ScanFragment {
        scan: scan.clone(),
        prefix: TriePrefix::new(Vec::new()).unwrap(),
        basis,
        residence: None,
        bounds: rtdb_types::TemporalBounds { valid_time: Interval::ALL, system_time: Interval::from_micros(10, 11).unwrap() },
        timezone: "UTC".into(),
    };
    let lifetime = Arc::downgrade(&context);
    let request = tokio::spawn(async move { evaluate_fragment(context, &fragment).await });
    tokio::time::timeout(Duration::from_secs(5), reading).await.unwrap().unwrap();
    cancellation.cancel();
    let error = tokio::time::timeout(Duration::from_secs(5), request).await.unwrap().unwrap().unwrap_err();
    assert_eq!(error.code, "xtdb/query-cancelled");
    assert!(lifetime.upgrade().is_some(), "the admitted provider read must retain the fragment snapshot");
    resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), io.close_and_wait()).await.unwrap();
    assert!(lifetime.upgrade().is_none());
}

#[tokio::test]
async fn live_unsigned_cells_open_signed_while_native_metadata_rejects_unsigned_storage() {
    let budget = MemoryBudget::new(1 << 27);
    let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
    let mut transaction = tx(&state, &budget, 0, 0);
    transaction.put(table(), document(1, Value::Struct(StructFields::from([("scalar".into(), Value::U64(u64::MAX)), ("list".into(), Value::list(vec![Value::U8(255)]))]))), Interval::from_micros(0, i64::MAX).unwrap()).unwrap();
    state.apply(&transaction.stage().unwrap()).unwrap();
    let physical = state.value().snapshot().column_types(&table())["v"].clone();
    let store = Arc::new(MemoryStore::default());
    let live = state.value().snapshot();
    let error = rtdb_format::columns::type_to_proto(&physical).unwrap_err();
    assert_eq!(error.category, rtdb_types::AnomalyCategory::Unsupported);
    assert_eq!(error.code, "xtdb/col-meta-unsigned-int");
    assert_eq!(error.message.as_deref(), Some("unsigned ints"));
    {
        let context = query_context(live, Arc::clone(&store), false);
        let relation = scan(&context, &["v"], TemporalFilter::Default, TemporalFilter::Default, None);
        let mut cursor = PreparedQuery::new(relation).unwrap().execute(context, vec![]).await.unwrap();
        assert_eq!(cursor.schema().fields()[0].data_type, physical);
        assert_eq!(cursor.runtime_schema().fields()[0].data_type, rtdb_columnar::execution_type(&physical));
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), Value::Struct(StructFields::from([("scalar".into(), Value::I64(-1)), ("list".into(), Value::list(vec![Value::I8(-1)]))])));
    }
    assert_eq!(state.value().snapshot().column_types(&table())["v"], physical);
}

#[tokio::test]
async fn compressed_trie_metadata_and_pages_preserve_history_and_release_decode_charges() {
    use rtdb_format::ipc::ArrowFile;
    use arrow_ipc::writer::{FileWriter, IpcWriteOptions};
    let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
    let memory = MemoryBudget::new(1 << 27);
    for (id, time, value) in [(0, 10, "old"), (1, 20, "new")] {
        let mut transaction = tx(&state, &memory, id, time);
        transaction.put(table(), document(1, Value::Utf8(value.into())), Interval::ALL).unwrap();
        state.apply(&transaction.stage().unwrap()).unwrap();
    }
    let original = MemoryStore::default(); persist(&state, &original).await;
    for codec in [arrow_ipc::CompressionType::LZ4_FRAME, arrow_ipc::CompressionType::ZSTD] {
        let store = Arc::new(MemoryStore::default());
        for object in original.list(None).await.unwrap() {
            let bytes = original.get(&object.key).await.unwrap();
            let bytes = if object.key.as_str().ends_with(".arrow") {
                let mut reader = ArrowFile::open(Arc::from(bytes.as_ref()), None).unwrap();
                let options = IpcWriteOptions::default().try_with_compression(Some(codec)).unwrap();
                let mut writer = FileWriter::try_new_with_options(Vec::new(), &reader.schema(), options).unwrap();
                for page in 0..reader.page_count() { writer.write(&reader.read_page(page).unwrap()).unwrap(); }
                writer.finish().unwrap(); writer.into_inner().unwrap().into()
            } else { bytes };
            store.put(&object.key, bytes).await.unwrap();
        }
        for (time, expected) in [(15, "old"), (25, "new")] {
            let context = query_context(state.value().snapshot().with_system_time_cap(None).unwrap(), Arc::clone(&store), false);
            let relation = scan(&context, &["v"], TemporalFilter::All, at(time), None);
            assert_eq!(collect(relation, Arc::clone(&context)).await.unwrap(), vec![vec![Value::Utf8(expected.into())]]);
            assert_eq!(context.memory.used_bytes(), 0);
        }
        if codec == arrow_ipc::CompressionType::LZ4_FRAME {
            let database = DatabaseSnapshot { partitions: vec![state.value().snapshot().with_system_time_cap(None).unwrap()], storage: Arc::new(BufferPool::new(store, 1 << 26)) };
            let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
            let budget = MemoryBudget::new(64 << 20);
            let context = QueryContext::new(catalog, Arc::clone(&budget), Timestamp(50), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
            let relation = scan(&context, &["v"], TemporalFilter::All, at(25), None);
            assert_eq!(collect(relation, context).await.unwrap_err().code, "rtdb/memory-budget-exceeded");
            assert_eq!(budget.used_bytes(), 0);
        }
    }
}

#[tokio::test]
async fn historical_trie_recency_beyond_chrono_range_remains_queryable() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let memory = MemoryBudget::new(1 << 27);
    let store = Arc::new(MemoryStore::default());
    let mut transaction = tx(&state, &memory, 0, 10);
    transaction.put(table(), document(1, Value::Utf8("wide recency".into())), Interval::ALL).unwrap();
    state.apply(&transaction.stage().unwrap()).unwrap();
    persist(&state, &store).await;
    let value = state.value();
    let original = value.trie_catalog().current_tries(&table())[0].clone();
    let slug = &value.table_catalog().entry(&table()).unwrap().slug;
    let key: TrieKey = "l02-r2800000103-b00".parse().unwrap();
    for (source, target) in [(slug.data_key(&original.key), slug.data_key(&key)), (slug.metadata_key(&original.key), slug.metadata_key(&key))] {
        let bytes = store.get(&ObjectKey::new(source).unwrap()).await.unwrap();
        store.put(&ObjectKey::new(target).unwrap(), bytes).await.unwrap();
    }
    let trie = CatalogTrie::notification(key, original.data_file_size, original.metadata).unwrap();
    let catalog = TrieCatalog::default().with_tries(table(), [trie], Timestamp(20)).unwrap();
    let snapshot = Snapshot::new(Arc::new(catalog), Arc::clone(value.table_catalog()), Arc::clone(value.live()));
    for bypass in [false, true] {
        let context = query_context(snapshot.clone(), Arc::clone(&store), bypass);
        assert_eq!(collect(scan(&context, &["v"], TemporalFilter::All, TemporalFilter::All, None), context).await.unwrap(), vec![vec![Value::Utf8("wide recency".into())]]);
    }
}

#[tokio::test]
async fn trie_stats_admits_java_maximum_recency_with_date32_narrowing() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let memory = MemoryBudget::new(1 << 27);
    let mut transaction = tx(&state, &memory, 0, 10);
    transaction.put(table(), document(1, Value::I64(1)), Interval::ALL).unwrap();
    state.apply(&transaction.stage().unwrap()).unwrap();
    let trie = CatalogTrie::notification("l02-r9999999991231-b00".parse().unwrap(), 0, None).unwrap();
    state.add_tries(&BTreeMap::from([(table(), vec![trie])]), Timestamp(20)).unwrap();
    let context = query_context(state.value().snapshot(), Arc::new(MemoryStore::default()), false);
    let relation = system_scan(&context, "trie_stats", &["recency"]);
    assert_eq!(collect(relation, context).await.unwrap(), vec![vec![Value::DateDay(169_560_311)]]);
}

#[tokio::test]
async fn live_catalog_shows_staged_columns_before_applied_columns_and_respects_l0_coverage() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let memory = MemoryBudget::new(1 << 27);
    let mut applied = tx(&state, &memory, 0, 10);
    applied.put(table(), StructFields::from([("_id".into(), Value::I64(1)), ("applied".into(), Value::I64(10))]), Interval::ALL).unwrap();
    state.apply(&applied.stage().unwrap()).unwrap();
    let mut pending = tx(&state, &memory, 1, 20);
    pending.put(table(), StructFields::from([("_id".into(), Value::I64(2)), ("staged".into(), Value::I64(20))]), Interval::ALL).unwrap();
    let staged = [Arc::new(pending.stage().unwrap())];
    let uncovered = state.value().snapshot().with_staged(&staged).unwrap();
    let key = TrieKey::l0(uncovered.block_index()).unwrap();
    let covering = CatalogTrie::notification(key, 0, None).unwrap();
    state.add_tries(&BTreeMap::from([(table(), vec![covering])]), Timestamp(30)).unwrap();
    let covered = state.value().snapshot().with_staged(&staged).unwrap();

    for (snapshot, row_count, columns) in [
        (uncovered, 2, &["_id", "staged", "_id", "applied"][..]),
        (covered, 1, &["_id", "staged"][..]),
    ] {
        let context = query_context(snapshot, Arc::new(MemoryStore::default()), false);
        let docs = Value::Utf8("docs".into());
        let counts = collect(system_scan(&context, "live_tables", &["table_name", "row_count"]), Arc::clone(&context)).await.unwrap();
        let live_rows: Vec<_> = counts.into_iter().filter(|row| row[0] == docs).collect();
        assert_eq!(live_rows, [vec![docs.clone(), Value::I64(row_count)]]);

        let names = collect(system_scan(&context, "live_columns", &["table_name", "col_name"]), context).await.unwrap();
        let live_columns: Vec<_> = names.into_iter().filter(|row| row[0] == docs).map(|mut row| row.remove(1)).collect();
        assert_eq!(live_columns, columns.iter().map(|name| Value::Utf8((*name).into())).collect::<Vec<_>>());
    }
}

#[tokio::test]
async fn live_catalog_omits_tables_whose_live_rows_are_all_flushed() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let memory = MemoryBudget::new(1 << 27);
    let mut applied = tx(&state, &memory, 0, 10);
    applied.put(table(), StructFields::from([("_id".into(), Value::I64(1)), ("applied".into(), Value::I64(10))]), Interval::ALL).unwrap();
    state.apply(&applied.stage().unwrap()).unwrap();
    let covering = CatalogTrie::notification(TrieKey::l0(state.value().snapshot().block_index()).unwrap(), 0, None).unwrap();
    state.add_tries(&BTreeMap::from([(table(), vec![covering])]), Timestamp(30)).unwrap();
    let context = query_context(state.value().snapshot(), Arc::new(MemoryStore::default()), false);
    let docs = Value::Utf8("docs".into());
    let tables = collect(system_scan(&context, "live_tables", &["table_name"]), Arc::clone(&context)).await.unwrap();
    assert!(!tables.iter().any(|row| row[0] == docs), "{tables:?}");
    let columns = collect(system_scan(&context, "live_columns", &["table_name"]), context).await.unwrap();
    assert!(!columns.iter().any(|row| row[0] == docs), "{columns:?}");
}

fn system_scan(context: &QueryContext, table: &str, columns: &[&str]) -> Arc<Relation> {
    let binding = context.catalog.table(&QualifiedTable { database: "xtdb".into(), table: TableRef::new("xt", table) }).unwrap().unwrap();
    let columns = columns.iter().enumerate().map(|(id, column)| Field {
        id: ColumnId(id as u32), name: (*column).into(), qualifier: None, qualified_only: false, unnamed: false,
        data_type: binding.columns[*column].clone(),
    }).collect();
    Arc::new(Relation::scan(Scan { table: binding, columns, valid_time: TemporalFilter::All, system_time: TemporalFilter::All,
        clamp_valid_time: false, iids: None, predicates: Vec::new(), top_k: None, ordered_top_k: None, matches: Vec::new() }).unwrap())
}

fn at(value: i64) -> TemporalFilter { TemporalFilter::At(Expr::Literal(timestamp(value))) }
fn query_context(snapshot: Snapshot, store: Arc<MemoryStore>, bypass: bool) -> Arc<QueryContext> {
    let database = DatabaseSnapshot { partitions: vec![snapshot], storage: Arc::new(BufferPool::new(store, 1 << 26)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    QueryContext::new(catalog, MemoryBudget::new(1 << 28), Timestamp(50), "UTC", QueryOptions { batch_rows: 2, bypass_temporal_pruning: bypass, ..QueryOptions::default() }, &CancellationToken::new()).unwrap()
}
fn scan(context: &QueryContext, columns: &[&str], valid_time: TemporalFilter, system_time: TemporalFilter, iids: Option<BTreeSet<Iid>>) -> Arc<Relation> {
    let binding = context.catalog.table(&QualifiedTable { database: "xtdb".into(), table: table() }).unwrap().unwrap();
    let fields = columns.iter().enumerate().map(|(id, name)| {
        let data_type = match *name {
            "_iid" => LogicalType::Scalar(ScalarType::FixedSizeBinary(16)),
            "_valid_from" | "_system_from" => LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }),
            "_valid_to" | "_system_to" => LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }).maybe(),
            name => binding.columns.get(name).cloned().unwrap_or(LogicalType::Null),
        };
        Field { id: ColumnId(id as u32), name: (*name).into(), qualifier: None, qualified_only: false, unnamed: false, data_type }
    }).collect();
    Arc::new(Relation::scan(Scan { table: binding, columns: fields, valid_time, system_time, clamp_valid_time: false, iids, predicates: Vec::new(), top_k: None, ordered_top_k: None, matches: Vec::new() }).unwrap())
}
async fn collect(relation: Arc<Relation>, context: Arc<QueryContext>) -> Result<Vec<Vec<Value>>> {
    collect_rows(PreparedQuery::new(relation)?.execute(context, vec![]).await?).await
}
async fn collect_rows(mut cursor: rtdb_query::QueryCursor) -> Result<Vec<Vec<Value>>> {
    let mut result = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() {
            result.push(batch.batch().schema().fields().iter().enumerate().map(|(column, field)| read_value(field, batch.batch().column(column).as_ref(), row)).collect::<Result<_>>()?);
        }
    }
    Ok(result)
}
async fn persist(state: &PartitionState, store: &MemoryStore) -> (BlockCatalog, BTreeMap<TableRef, TableBlock>) {
    persist_with_blooms(state, store, false).await
}
async fn persist_with_blooms(state: &PartitionState, store: &MemoryStore, blooms: bool) -> (BlockCatalog, BTreeMap<TableRef, TableBlock>) {
    let prepared = state.prepare_block().unwrap();
    let catalog = prepared.table_catalog().unwrap();
    let mut additions = BTreeMap::new();
    for (table, live) in prepared.value().live().tables() {
        let events = live.ordered_events().unwrap().iter().map(|event| event.source.event()).collect::<Result<Vec<_>>>().unwrap();
        let batch = build_event_batch(&events, Some(live.column_types().clone()), None).unwrap();
        let mut metadata = MetadataBuilder::new(false, blooms);
        metadata.write_leaf(&batch).unwrap();
        let (metadata, statistics) = metadata.finish().unwrap();
        let bytes = write_file(batch.schema().as_ref(), &[batch]).unwrap();
        let key = TrieKey::l0(prepared.block_index()).unwrap();
        let slug = &catalog.entry(table).unwrap().slug;
        let size = bytes.len() as i64;
        store.put(&ObjectKey::new(slug.data_key(&key)).unwrap(), bytes.into()).await.unwrap();
        store.put(&ObjectKey::new(slug.metadata_key(&key)).unwrap(), metadata.write().unwrap().into()).await.unwrap();
        additions.insert(table.clone(), vec![CatalogTrie::notification(key, size, Some(statistics)).unwrap()]);
    }
    let basis = prepared.value().live().latest_completed_tx();
    let block = BlockCatalog::from_proto(pb::Block {
        block_index: Some(prepared.block_index().0), latest_completed_tx: basis.map(|key| pb::TxKey { tx_id: Some(key.tx_id.0), system_time: Some(key.system_time.0) }),
        tables: catalog.entries().map(|entry| entry.to_proto()).collect(), table_names: catalog.entries().map(|entry| entry.table.schema_and_table()).collect(), ..pb::Block::default()
    }).unwrap();
    let tables = prepared.table_blocks(&additions, Timestamp(1000)).unwrap();
    for (table, table_block) in &tables {
        store.put(&ObjectKey::new(catalog.entry(table).unwrap().slug.block_key(prepared.block_index()).unwrap()).unwrap(), table_block.encode().into()).await.unwrap();
    }
    store.put(&ObjectKey::new(block_key(prepared.block_index()).unwrap()).unwrap(), block.encode().into()).await.unwrap();
    state.adopt_block(prepared, block.clone(), &tables, &additions, Timestamp(1000)).unwrap();
    (block, tables)
}

fn string_join(context: &QueryContext, value: &str) -> Arc<Relation> {
    use rtdb_plan::{JoinKind, RelationNode, Schema};
    let field = Field { id: ColumnId(100), name: "key".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::Utf8) };
    let left = Arc::new(Relation::new(Schema::new(vec![field.clone()]).unwrap(), RelationNode::Values { rows: vec![vec![Expr::Literal(Value::Utf8(value.into()))]] }).unwrap());
    let right = scan(context, &["v"], TemporalFilter::Default, TemporalFilter::Default, None);
    let condition = Expr::binary(BinaryOp::Eq, field.expression(), right.schema().fields()[0].expression());
    Arc::new(Relation::new(left.schema().concat(right.schema()).unwrap(), RelationNode::Join { left, right, kind: JoinKind::Inner, condition: Some(condition) }).unwrap())
}

#[tokio::test]
async fn computed_projection_reusing_a_column_id_stops_an_inherited_iid_filter() {
    use rtdb_plan::{JoinKind, NamedExpr, RelationNode, Schema};
    let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
    let memory = MemoryBudget::new(1 << 27);
    let mut transaction = tx(&state, &memory, 0, 1_000);
    transaction.put(table(), document(1, Value::Utf8("original".into())), Interval::ALL).unwrap();
    state.apply(&transaction.stage().unwrap()).unwrap();
    let context = query_context(state.value().snapshot(), Arc::new(MemoryStore::default()), false);
    let source = scan(&context, &["_id"], TemporalFilter::Default, TemporalFilter::Default, None);
    let field = source.schema().fields()[0].clone();
    let right = Arc::new(source.project(vec![NamedExpr { field: field.clone(), expr: Expr::Literal(Value::I64(2)) }]).unwrap());
    let build = Field { id: ColumnId(100), name: "key".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) };
    let left = Arc::new(Relation::new(Schema::new(vec![build.clone()]).unwrap(), RelationNode::Values { rows: vec![vec![Expr::Literal(Value::I64(2))]] }).unwrap());
    let condition = Expr::binary(BinaryOp::Eq, build.expression(), field.expression());
    let join = Arc::new(Relation::new(left.schema().concat(right.schema()).unwrap(), RelationNode::Join { left, right, kind: JoinKind::Inner, condition: Some(condition) }).unwrap());
    assert_eq!(collect(join, Arc::clone(&context)).await.unwrap(), [vec![Value::I64(2), Value::I64(2)]]);
    assert_eq!(context.memory.used_bytes(), 0); assert_eq!(context.spill_bytes(), 0);
}

#[tokio::test]
async fn join_bloom_prunes_a_nonmatching_older_page_and_preserves_a_newer_superseder() {
    for newer_superseder in [false, true] {
        let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
        let memory = MemoryBudget::new(1 << 27); let store = Arc::new(MemoryStore::default());
        let mut first = tx(&state, &memory, 0, 1_000);
        first.put(table(), document(1, Value::Utf8(if newer_superseder { "match" } else { "absent" }.into())), Interval::ALL).unwrap();
        state.apply(&first.stage().unwrap()).unwrap(); persist_with_blooms(&state, &store, true).await;
        let mut second = tx(&state, &memory, 1, 2_000);
        second.put(table(), document(if newer_superseder { 1 } else { 2 }, Value::Utf8(if newer_superseder { "absent" } else { "match" }.into())), Interval::ALL).unwrap();
        state.apply(&second.stage().unwrap()).unwrap(); persist_with_blooms(&state, &store, true).await;
        let context = query_context(state.value().snapshot(), store, false);
        let (tracer, mut receiver) = rtdb_metrics::TraceRecorder::new(Arc::clone(&context.memory), 16, true, true).unwrap();
        let context = context.with_tracer(Some(tracer.clone()), None);
        let result = collect(string_join(&context, "match"), Arc::clone(&context)).await.unwrap();
        if newer_superseder { assert!(result.is_empty()); }
        else { assert_eq!(result, [vec![Value::Utf8("match".into()), Value::Utf8("match".into())]]); }
        tracer.close();
        let mut scan = None;
        while let Some(span) = receiver.next().await { if span.name == "query.cursor.scan.public.docs" { scan = Some(span); } }
        let scan = scan.unwrap(); let attributes: BTreeMap<_, _> = scan.attributes().collect();
        assert_eq!(attributes["scan_pages_used"], if newer_superseder { "2" } else { "1" });
        assert_eq!(attributes["scan_pages_pruned"], if newer_superseder { "0" } else { "1" });
        assert_eq!(context.spill_bytes(), 0);
    }
}

#[tokio::test]
async fn join_iid_admission_uses_distinct_keys_and_reports_header_pruning() {
    use rtdb_plan::{JoinKind, RelationNode, Schema};

    let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
    let memory = MemoryBudget::new(1 << 27);
    let mut write = tx(&state, &memory, 0, 100);
    for id in 1..=4 {
        write.put(table(), document(id, Value::I64(id)), Interval::ALL).unwrap();
    }
    state.apply(&write.stage().unwrap()).unwrap();

    for (keys, expected, admitted) in [
        (vec![3, 1, 3, 1, 1], vec![1, 3], true),
        (vec![3, 1, 2, 4, 1], vec![1, 2, 3, 4], false),
    ] {
        let context = query_context(state.value().snapshot(), Arc::new(MemoryStore::default()), false);
        let (tracer, mut receiver) = rtdb_metrics::TraceRecorder::new(Arc::clone(&context.memory), 16, true, true).unwrap();
        let context = context.with_tracer(Some(tracer.clone()), None);
        let probe = scan(&context, &["_id"], TemporalFilter::Default, TemporalFilter::Default, None);
        let field = Field { id: ColumnId(100), name: "key".into(), qualifier: None, qualified_only: false, unnamed: false,
            data_type: LogicalType::Scalar(ScalarType::I64) };
        let condition = Expr::binary(BinaryOp::Eq, probe.schema().fields()[0].expression(), field.expression());
        let build = Arc::new(Relation::new(Schema::new(vec![field]).unwrap(), RelationNode::Values {
            rows: keys.into_iter().map(|key| vec![Expr::Literal(Value::I64(key))]).collect(),
        }).unwrap());
        let join = Arc::new(Relation::join(probe, build, JoinKind::Semi, Some(condition)).unwrap());
        let mut rows = collect(join, context).await.unwrap();
        rows.sort_by_key(|row| match row.as_slice() {
            [Value::I64(id)] => *id,
            _ => panic!("Expected one integer ID per probe row"),
        });
        assert_eq!(rows, expected.into_iter().map(|id| vec![Value::I64(id)]).collect::<Vec<_>>());

        tracer.close();
        let mut scan_span = None;
        while let Some(span) = receiver.next().await {
            if span.name == "query.cursor.scan.public.docs" { scan_span = Some(span); }
        }
        let scan_span = scan_span.unwrap();
        let attributes: BTreeMap<_, _> = scan_span.attributes().collect();
        assert_eq!(attributes["cursor.join_filter.build_distinct_keys"], if admitted { "2" } else { "4" });
        assert_eq!(attributes["cursor.join_filter.build_distinct_keys_complete"], if admitted { "1" } else { "0" });
        assert_eq!(attributes["cursor.join_filter.admitted_iids"], if admitted { "2" } else { "0" });
        if admitted {
            assert_eq!(attributes["cursor.join_filter.checked_headers"], "4");
            assert_eq!(attributes["cursor.join_filter.rejected_headers"], "2");
        } else {
            assert!(!attributes.contains_key("cursor.join_filter.checked_headers"));
            assert!(!attributes.contains_key("cursor.join_filter.rejected_headers"));
        }
    }
}

#[tokio::test]
async fn tracing_scan_reports_only_consulted_files_pages_and_loaded_rows() {
    let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
    let memory = MemoryBudget::new(1 << 27);
    let store = Arc::new(MemoryStore::default());
    let mut first = tx(&state, &memory, 0, 100);
    first.put(table(), document(1, Value::I64(1)), Interval::ALL).unwrap();
    state.apply(&first.stage().unwrap()).unwrap(); persist(&state, &store).await;
    let context = query_context(state.value().snapshot(), store, false);
    let (tracer, mut receiver) = rtdb_metrics::TraceRecorder::new(Arc::clone(&context.memory), 16, true, true).unwrap();
    let context = context.with_tracer(Some(tracer.clone()), None);
    let relation = scan(&context, &["v"], TemporalFilter::All, TemporalFilter::All, None);
    assert_eq!(collect(relation, context).await.unwrap(), vec![vec![Value::I64(1)]]);
    tracer.close();
    let mut scan = None; while let Some(span) = receiver.next().await { if span.name == "query.cursor.scan.public.docs" { scan = Some(span); } }
    let scan = scan.unwrap(); let attributes: BTreeMap<_, _> = scan.attributes().collect();
    assert_eq!(attributes["scan_db"], "xtdb"); assert_eq!(attributes["scan_source"], "public.docs");
    assert_eq!(attributes["scan_files_used"], "1"); assert_eq!(attributes["scan_files_pruned"], "0");
    assert_eq!(attributes["scan_pages_used"], "1"); assert_eq!(attributes["scan_pages_pruned"], "0");
    assert_eq!(attributes["scan_rows_read"], "1");
}

#[tokio::test]
async fn persisted_and_live_history_resolves_before_value_filtering() {
    let state = PartitionState::empty(TrieConfig::new(2, 2).unwrap()).unwrap();
    let budget = MemoryBudget::new(1 << 27);
    let store = Arc::new(MemoryStore::default());
    let mut first = tx(&state, &budget, 0, 100);
    first.put(table(), document(1, Value::I64(1)), Interval::from_micros(0, 100).unwrap()).unwrap();
    state.apply(&first.stage().unwrap()).unwrap();
    let pinned = state.value().snapshot();
    persist(&state, &store).await;
    let mut second = tx(&state, &budget, 1, 200);
    second.put(table(), document(1, Value::I64(2)), Interval::from_micros(20, 80).unwrap()).unwrap();
    state.apply(&second.stage().unwrap()).unwrap();
    let current = state.value().snapshot();
    for bypass in [false, true] {
        let context = query_context(current.clone(), Arc::clone(&store), bypass);
        let scan = scan(&context, &["v"], TemporalFilter::Default, TemporalFilter::Default, None);
        assert_eq!(collect(Arc::clone(&scan), Arc::clone(&context)).await.unwrap(), vec![vec![Value::I64(2)]]);
        let predicate = Expr::binary(BinaryOp::Eq, scan.schema().fields()[0].expression(), Expr::Literal(Value::I64(1)));
        assert!(collect(Arc::new(scan.filter(predicate).unwrap()), context).await.unwrap().is_empty());
    }
    let context = query_context(pinned, Arc::clone(&store), false);
    assert_eq!(collect(scan(&context, &["v"], TemporalFilter::Default, TemporalFilter::Default, None), context).await.unwrap(), vec![vec![Value::I64(1)]]);
    let context = query_context(current, store, false);
    let history = scan(&context, &["v", "_valid_from", "_valid_to", "_system_from", "_system_to"], TemporalFilter::All, TemporalFilter::All, None);
    assert_eq!(collect(history, context).await.unwrap(), vec![
        vec![Value::I64(1), timestamp(0), timestamp(20), timestamp(100), Value::Null],
        vec![Value::I64(1), timestamp(20), timestamp(80), timestamp(100), timestamp(200)],
        vec![Value::I64(1), timestamp(80), timestamp(100), timestamp(100), Value::Null],
        vec![Value::I64(2), timestamp(20), timestamp(80), timestamp(200), Value::Null],
    ]);
}

#[tokio::test]
async fn scan_project_and_sort_pages_preserve_patch_history_across_output_batch_sizes() {
    use rtdb_plan::{NamedExpr,RelationNode,Schema,SortExpr};
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let memory = MemoryBudget::new(1 << 27);
    let store = Arc::new(MemoryStore::default());
    let mut first = tx(&state,&memory,0,100);
    first.put(table(),document(1,Value::I64(1)),Interval::from_micros(10,20).unwrap()).unwrap();
    state.apply(&first.stage().unwrap()).unwrap();persist(&state,&store).await;
    let mut second = tx(&state,&memory,1,200);
    second.put(table(),document(1,Value::I64(2)),Interval::from_micros(30,40).unwrap()).unwrap();
    state.apply(&second.stage().unwrap()).unwrap();
    for batch_rows in [1,2,1024] {
        let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()],storage: Arc::new(BufferPool::new(Arc::clone(&store) as Arc<dyn ObjectStore>,1 << 26)) };
        let catalog = CatalogSnapshot::new("xtdb",BTreeMap::from([("xtdb".into(),database)]),false).unwrap();
        let context = QueryContext::new(catalog,MemoryBudget::new(1 << 28),Timestamp(0),"UTC",QueryOptions { batch_rows,sort_memory_bytes: 4096,..Default::default() },&CancellationToken::new()).unwrap();
        let source = scan(&context,&["_iid","_valid_from","_valid_to","v"],TemporalFilter::All,TemporalFilter::Default,None);
        let source = Arc::new(source.clone().sort(vec![SortExpr { expr: source.schema().fields()[0].expression(),descending: false,nulls_first: false },SortExpr { expr: source.schema().fields()[1].expression(),descending: false,nulls_first: false }]).unwrap());
        let expressions = source.schema().fields().iter().map(|field|NamedExpr { field: field.clone(),expr: field.expression() }).collect();
        let source = Arc::new(source.project(expressions).unwrap());
        let mut fields = source.schema().fields().to_vec();fields[3].data_type = fields[3].data_type.clone().maybe();
        let patch = Arc::new(Relation::new(Schema::new(fields).unwrap(),RelationNode::PatchGaps { input: source,iid: ColumnId(0),valid_from: ColumnId(1),valid_to: ColumnId(2),document: ColumnId(3),from: Expr::Literal(timestamp(0)),to: Expr::Literal(timestamp(50)) }).unwrap());
        let rows = collect(patch,Arc::clone(&context)).await.unwrap();
        assert_eq!(rows.iter().map(|row|(row[1].clone(),row[2].clone(),row[3].clone())).collect::<Vec<_>>(),[(timestamp(0),timestamp(10),Value::Null),(timestamp(10),timestamp(20),Value::I64(1)),(timestamp(20),timestamp(30),Value::Null),(timestamp(30),timestamp(40),Value::I64(2)),(timestamp(40),timestamp(50),Value::Null)]);
        assert_eq!(context.memory.used_bytes(),0);assert_eq!(context.spill_bytes(),0);
    }
}

#[tokio::test]
async fn erase_suppresses_history_before_its_system_time_and_reinsert_starts_new_history() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(1 << 27);
    let store = Arc::new(MemoryStore::default());
    let mut first = tx(&state, &budget, 0, 100);
    first.put(table(), document(1, Value::I64(1)), Interval::ALL).unwrap();
    state.apply(&first.stage().unwrap()).unwrap();
    persist(&state, &store).await;
    let mut erased = tx(&state, &budget, 1, 200);
    erased.erase(table(), &EntityId::Integer(1)).unwrap();
    state.apply(&erased.stage().unwrap()).unwrap();
    for bypass in [false, true] {
        let context = query_context(state.value().snapshot(), Arc::clone(&store), bypass);
        assert!(collect(scan(&context, &["v"], TemporalFilter::All, at(150), None), context).await.unwrap().is_empty());
    }
    persist(&state, &store).await;
    let mut reinsert = tx(&state, &budget, 2, 300);
    reinsert.put(table(), document(1, Value::I64(3)), Interval::ALL).unwrap();
    state.apply(&reinsert.stage().unwrap()).unwrap();
    let context = query_context(state.value().snapshot(), store, false);
    assert_eq!(collect(scan(&context, &["v"], TemporalFilter::All, TemporalFilter::All, None), context).await.unwrap(), vec![vec![Value::I64(3)]]);
}

#[tokio::test]
async fn iid_lookup_and_pruned_scans_match_general_history_across_live_leaves() {
    let state = PartitionState::empty(TrieConfig::new(1, 2).unwrap()).unwrap();
    let budget = MemoryBudget::new(1 << 27);
    let store = Arc::new(MemoryStore::default());
    let mut write = tx(&state, &budget, 0, 100);
    for id in 0..20 { write.put(table(), document(id, Value::I64(id)), Interval::ALL).unwrap(); }
    state.apply(&write.stage().unwrap()).unwrap();
    let snapshot = state.value().snapshot();
    let wanted = BTreeSet::from([EntityId::Integer(7).iid(), EntityId::Integer(11).iid()]);
    let context = query_context(snapshot.clone(), Arc::clone(&store), false);
    let fast = collect(scan(&context, &["_iid", "v"], TemporalFilter::All, TemporalFilter::All, Some(wanted.clone())), context).await.unwrap();
    let context = query_context(snapshot, Arc::clone(&store), true);
    let all = collect(scan(&context, &["_iid", "v"], TemporalFilter::All, TemporalFilter::All, None), context).await.unwrap();
    let expected: Vec<_> = all.into_iter().filter(|row| match &row[0] { Value::FixedSizeBinary(bytes) => wanted.contains(&Iid::from_slice(bytes).unwrap()), _ => false }).collect();
    assert_eq!(fast, expected);
    persist(&state, &store).await;
    for bypass in [false, true] {
        let context = query_context(state.value().snapshot(), Arc::clone(&store), bypass);
        assert_eq!(collect(scan(&context, &["_iid", "v"], TemporalFilter::All, TemporalFilter::All, Some(wanted.clone())), context).await.unwrap(), expected);
    }
}

#[tokio::test]
async fn reopened_catalog_reads_persisted_data_and_reads_a_later_system_time_as_of_its_basis() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(1 << 27);
    let store = Arc::new(MemoryStore::default());
    let mut write = tx(&state, &budget, 0, 100);
    write.put(table(), document(1, Value::I64(9)), Interval::ALL).unwrap();
    state.apply(&write.stage().unwrap()).unwrap();
    let (block, table_blocks) = persist(&state, &store).await;
    let table_blocks = table_blocks.into_iter().map(|(table, block)| (table, TableBlock::decode(&block.encode()).unwrap())).collect();
    let catalog = TableCatalog::load(Some(BlockCatalog::decode(&block.encode()).unwrap()), &table_blocks).unwrap();
    let tries = TrieCatalog::load(&table_blocks, 100_000_000, Timestamp(1000)).unwrap();
    let reopened = PartitionState::new(PartitionValue::loaded(tries, catalog, TrieConfig::default()).unwrap());
    let context = query_context(reopened.value().snapshot(), Arc::clone(&store), false);
    assert_eq!(collect(scan(&context, &["v"], TemporalFilter::Default, TemporalFilter::Default, None), context).await.unwrap(), vec![vec![Value::I64(9)]]);
    let context = query_context(reopened.value().snapshot(), store, false);
    assert_eq!(collect(scan(&context, &["v"], TemporalFilter::All, at(200), None), context).await.unwrap(), vec![vec![Value::I64(9)]]);
}

#[tokio::test]
async fn changing_union_schema_requires_repreparation_and_old_snapshot_remains_valid() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(1 << 27);
    let store = Arc::new(MemoryStore::default());
    let mut first = tx(&state, &budget, 0, 100);
    first.put(table(), document(1, Value::I64(1)), Interval::ALL).unwrap();
    state.apply(&first.stage().unwrap()).unwrap();
    let old = query_context(state.value().snapshot(), Arc::clone(&store), false);
    let prepared = PreparedQuery::new(scan(&old, &["v"], TemporalFilter::Default, TemporalFilter::Default, None)).unwrap();
    let mut second = tx(&state, &budget, 1, 200);
    second.put(table(), document(2, Value::Utf8("x".into())), Interval::ALL).unwrap();
    state.apply(&second.stage().unwrap()).unwrap();
    let new = query_context(state.value().snapshot(), store, false);
    let error = match prepared.execute(new, vec![]).await { Ok(_) => panic!("changed schema reused a stale plan"), Err(error) => error };
    assert_eq!(error.code, "rtdb.query/stale-plan");
    let mut old_cursor = prepared.execute(old, vec![]).await.unwrap();
    assert!(old_cursor.next_batch().await.unwrap().is_some());
}

#[tokio::test]
async fn independent_query_cap_keeps_the_captured_live_generation_and_durable_identity() {
    let state = PartitionState::empty(TrieConfig::default()).unwrap();
    let budget = MemoryBudget::new(1 << 27);
    let store = Arc::new(MemoryStore::default());
    let mut first = tx(&state, &budget, 0, 100);
    first.put(table(), document(1, Value::I64(1)), Interval::ALL).unwrap();
    state.apply(&first.stage().unwrap()).unwrap();
    let mut second = tx(&state, &budget, 1, 200);
    second.put(table(), document(1, Value::I64(2)), Interval::ALL).unwrap();
    state.apply(&second.stage().unwrap()).unwrap();
    let current = state.value().snapshot();
    let capped = current.clone().with_system_time_cap(Some(Timestamp(100))).unwrap();
    assert_eq!(capped.tx_basis(), current.tx_basis());
    persist(&state, &store).await;
    let context = query_context(capped, Arc::clone(&store), false);
    assert_eq!(collect(scan(&context, &["v"], TemporalFilter::Default, TemporalFilter::Default, None), context).await.unwrap(), vec![vec![Value::I64(1)]]);
    for system_time in [TemporalFilter::Default, TemporalFilter::All] {
        let context = query_context(current.clone().with_system_time_cap(None).unwrap(), Arc::clone(&store), false);
        let result = collect(scan(&context, &["v"], TemporalFilter::All, system_time, None), context).await.unwrap();
        assert_eq!(result.len(), 2);
        assert!(result.contains(&vec![Value::I64(1)]));
        assert!(result.contains(&vec![Value::I64(2)]));
    }
    let context = query_context(current.clone().with_system_time_cap(None).unwrap(), Arc::clone(&store), false);
    assert_eq!(collect(scan(&context, &["v"], TemporalFilter::All, at(150), None), context).await.unwrap(), vec![vec![Value::I64(1)]]);
    assert!(current.with_system_time_cap(Some(Timestamp(201))).is_err());
}

#[tokio::test]
async fn each_partition_uses_its_own_read_basis() {
    let budget = MemoryBudget::new(1 << 27);
    let store = Arc::new(MemoryStore::default());
    let mut partitions = Vec::new();
    for id in [1, 2] {
        let state = PartitionState::empty(TrieConfig::default()).unwrap();
        for (tx_id, system, value) in [(0, 100, 1), (1, 200, 2)] {
            let mut write = tx(&state, &budget, tx_id, system);
            write.put(table(), document(id, Value::I64(value)), Interval::ALL).unwrap();
            state.apply(&write.stage().unwrap()).unwrap();
        }
        partitions.push(state.value().snapshot().with_system_time_cap(Some(Timestamp(if id == 1 { 100 } else { 200 }))).unwrap());
    }
    let database = DatabaseSnapshot { partitions, storage: Arc::new(BufferPool::new(store, 1 << 26)) };
    let catalog = CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
    let context = QueryContext::new(catalog, Arc::clone(&budget), Timestamp(50), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
    let rows = collect(scan(&context, &["_id", "v"], TemporalFilter::Default, TemporalFilter::Default, None), context).await.unwrap();
    assert_eq!(rows, vec![vec![Value::I64(1), Value::I64(1)], vec![Value::I64(2), Value::I64(2)]]);
}

async fn persisted_root_count(snapshot: &Snapshot, store: &MemoryStore, block: &BlockCatalog, name: &str) -> Option<i64> {
    use rtdb_format::metadata::{MetadataFile, MetadataNode};

    let slug = &snapshot.table_entry(&table()).unwrap().slug;
    let trie_key = TrieKey::l0(block.block_index()).unwrap();
    let key = ObjectKey::new(slug.metadata_key(&trie_key)).unwrap();
    let bytes = store.get(&key).await.unwrap();
    let metadata = MetadataFile::read_reader(std::io::Cursor::new(bytes)).unwrap();
    let [MetadataNode::Leaf { columns, .. }] = metadata.nodes() else {
        panic!("Expected one metadata leaf for the persisted block");
    };
    let columns = columns.values().unwrap();
    let column = columns.iter().find(|column| {
        matches!(column.field("col-name"), Some(Value::Utf8(column_name)) if column_name.as_str() == name)
            && column.field("root-col?") == Some(&Value::Bool(true))
    })?;
    let Some(Value::I64(count)) = column.field("count") else {
        panic!("Expected a non-null count for the persisted column");
    };
    Some(*count)
}

async fn collect_with_scan_counts(relation: Arc<Relation>, context: Arc<QueryContext>) -> (Vec<Vec<Value>>, (u64, u64, u64)) {
    use rtdb_plan::{PreparedStatement, Statement};

    let (tracer, mut receiver) = rtdb_metrics::TraceRecorder::new(Arc::clone(&context.memory), 16, true, true).unwrap();
    let context = context.with_tracer(Some(tracer.clone()), None);
    let prepared = PreparedQuery::from_prepared(PreparedStatement {
        statement: Statement::Query(relation),
        parameter_types: BTreeMap::new(),
        source: String::new(),
        notices: Vec::new(),
        settings: Default::default(),
    }, &context.memory).unwrap();
    let rows = collect_rows(prepared.execute(context, vec![]).await.unwrap()).await.unwrap();
    tracer.close();
    let mut scan_span = None;
    while let Some(span) = receiver.next().await {
        if span.name == "query.cursor.scan.public.docs" {
            scan_span = Some(span);
        }
    }
    let scan_span = scan_span.unwrap();
    let attributes: BTreeMap<_, _> = scan_span.attributes().collect();
    let pages_used = attributes["scan_pages_used"].parse().unwrap();
    let pages_pruned = attributes["scan_pages_pruned"].parse().unwrap();
    let rows_read = attributes["scan_rows_read"].parse().unwrap();
    (rows, (pages_used, pages_pruned, rows_read))
}

#[tokio::test]
async fn is_not_null_prunes_a_known_zero_root_through_direct_columns_and_identity_aliases() {
    use rtdb_plan::{NamedExpr, UnaryOp};

    let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
    let budget = MemoryBudget::new(1 << 27);
    let store = Arc::new(MemoryStore::default());
    let valid = Interval::from_micros(0, 100).unwrap();

    let mut first = tx(&state, &budget, 0, 100);
    first.put(table(), document(1, Value::Null), valid).unwrap();
    state.apply(&first.stage().unwrap()).unwrap();
    let (all_null, _) = persist(&state, &store).await;

    let mut second = tx(&state, &budget, 1, 200);
    for id in [2, 6] {
        second.put(table(), StructFields::from([("_id".into(), Value::I64(id))]), valid).unwrap();
    }
    state.apply(&second.stage().unwrap()).unwrap();
    let (missing, _) = persist(&state, &store).await;

    let mut third = tx(&state, &budget, 2, 300);
    third.put(table(), document(3, Value::Null), valid).unwrap();
    third.put(table(), document(4, Value::I64(7)), valid).unwrap();
    third.put(table(), document(5, Value::Utf8("mixed".into())), valid).unwrap();
    state.apply(&third.stage().unwrap()).unwrap();
    let (mixed, _) = persist(&state, &store).await;

    let snapshot = state.value().snapshot();
    assert_eq!(persisted_root_count(&snapshot, &store, &all_null, "v").await, Some(0));
    assert_eq!(persisted_root_count(&snapshot, &store, &missing, "v").await, None);
    assert_eq!(persisted_root_count(&snapshot, &store, &mixed, "v").await, Some(2));

    for bypass in [false, true] {
        let context = query_context(snapshot.clone(), Arc::clone(&store), bypass);
        let source = scan(&context, &["v"], at(50), at(300), None);
        let field = source.schema().fields()[0].clone();
        let alias = Field { id: ColumnId(100), name: "alias".into(), ..field.clone() };
        let identity = Arc::new(Arc::clone(&source).project(vec![NamedExpr {
            field: alias.clone(),
            expr: field.expression(),
        }]).unwrap());
        for (input, column) in [(Arc::clone(&source), field.expression()), (identity, alias.expression())] {
            let filter = Arc::new(input.filter(Expr::unary(UnaryOp::IsNotNull, column)).unwrap());
            let (rows, counts) = collect_with_scan_counts(filter, Arc::clone(&context)).await;
            assert_eq!(rows.len(), 2);
            assert!(rows.contains(&vec![Value::I64(7)]));
            assert!(rows.contains(&vec![Value::Utf8("mixed".into())]));
            assert_eq!(counts, if bypass { (3, 0, 6) } else { (2, 1, 5) });
        }

        let computed = Field { data_type: LogicalType::Scalar(ScalarType::Bool), ..field.clone() };
        let project = Arc::new(source.project(vec![NamedExpr {
            field: computed.clone(),
            expr: Expr::unary(UnaryOp::IsNull, field.expression()),
        }]).unwrap());
        let filter = Arc::new(project.filter(Expr::unary(UnaryOp::IsNotNull, computed.expression())).unwrap());
        let (rows, counts) = collect_with_scan_counts(filter, context).await;
        assert_eq!(rows.len(), 6);
        assert_eq!(rows.iter().filter(|row| row[0] == Value::Bool(true)).count(), 4);
        assert_eq!(rows.iter().filter(|row| row[0] == Value::Bool(false)).count(), 2);
        assert_eq!(counts, (3, 0, 6));
    }
}

#[tokio::test]
async fn is_not_null_keeps_zero_count_pages_that_correct_delete_or_erase_matching_history() {
    use rtdb_plan::UnaryOp;
    use rtdb_types::EventOp;

    for operation in [EventOp::Put, EventOp::Delete, EventOp::Erase] {
        let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
        let budget = MemoryBudget::new(1 << 27);
        let store = Arc::new(MemoryStore::default());
        let original = Interval::from_micros(0, 100).unwrap();
        let corrected = Interval::from_micros(20, 80).unwrap();
        let target = EntityId::Integer(1);

        let mut first = tx(&state, &budget, 0, 100);
        first.put(table(), document(1, Value::I64(1)), original).unwrap();
        first.put(table(), document(2, Value::I64(2)), original).unwrap();
        state.apply(&first.stage().unwrap()).unwrap();
        persist(&state, &store).await;

        let mut second = tx(&state, &budget, 1, 200);
        match operation {
            EventOp::Put => second.put(table(), document(1, Value::Null), corrected),
            EventOp::Delete => second.delete(table(), &target, corrected),
            EventOp::Erase => second.erase(table(), &target),
            EventOp::Redacted => unreachable!(),
        }.unwrap();
        second.put(table(), document(3, Value::Null), corrected).unwrap();
        state.apply(&second.stage().unwrap()).unwrap();
        let (superseder, _) = persist(&state, &store).await;
        let snapshot = state.value().snapshot();
        assert_eq!(persisted_root_count(&snapshot, &store, &superseder, "v").await, Some(0));

        for bypass in [false, true] {
            let context = query_context(snapshot.clone(), Arc::clone(&store), bypass);
            let points = match operation {
                EventOp::Erase => vec![(50, TemporalFilter::FromTo {
                    from: Expr::Literal(timestamp(120)),
                    to: Expr::Literal(timestamp(150)),
                })],
                EventOp::Put | EventOp::Delete => vec![(50, at(200)), (10, at(200))],
                EventOp::Redacted => unreachable!(),
            };
            for (valid, system) in points {
                let source = scan(&context, &["v", "_valid_from", "_valid_to", "_system_from", "_system_to"], at(valid), system, None);
                let predicate = Expr::unary(UnaryOp::IsNotNull, source.schema().fields()[0].expression());
                let filter = Arc::new(source.filter(predicate).unwrap());
                let (rows, counts) = collect_with_scan_counts(filter, Arc::clone(&context)).await;
                let unaffected = vec![Value::I64(2), timestamp(0), timestamp(100), timestamp(100), Value::Null];
                if valid == 10 {
                    let shortened = vec![Value::I64(1), timestamp(0), timestamp(20), timestamp(100), Value::Null];
                    assert_eq!(rows.len(), 2);
                    assert!(rows.contains(&unaffected));
                    assert!(rows.contains(&shortened));
                } else {
                    assert_eq!(rows, [unaffected]);
                }
                assert_eq!(counts, (2, 0, 4));
            }
        }
    }
}

#[tokio::test]
async fn an_inverted_temporal_range_matches_nothing() {
    let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
    let budget = MemoryBudget::new(1 << 27);
    let mut transaction = tx(&state, &budget, 0, 10);
    transaction.put(table(), document(1, Value::I64(1)), Interval::ALL).unwrap();
    state.apply(&transaction.stage().unwrap()).unwrap();
    let context = query_context(state.value().snapshot(), Arc::new(MemoryStore::default()), false);
    for valid_time in [
        TemporalFilter::FromTo { from: Expr::Literal(timestamp(500)), to: Expr::Literal(timestamp(100)) },
        TemporalFilter::Between { from: Expr::Literal(timestamp(500)), to: Expr::Literal(timestamp(100)) },
    ] {
        assert_eq!(collect(scan(&context, &["v"], valid_time, TemporalFilter::All, None), Arc::clone(&context)).await.unwrap(), Vec::<Vec<Value>>::new());
    }
}

#[tokio::test]
async fn a_nonmatching_page_overlapping_no_emitted_version_in_valid_time_is_pruned() {
    let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
    let budget = MemoryBudget::new(1 << 27); let store = Arc::new(MemoryStore::default());
    for (index, (system, v, from, to)) in [(100, 1, 100, 200), (200, 2, 300, 400), (300, 1, 500, 700), (400, 3, 600, 800)].into_iter().enumerate() {
        let mut transaction = tx(&state, &budget, index as i64, system);
        transaction.put(table(), document(1, Value::I64(v)), Interval::from_micros(from, to).unwrap()).unwrap();
        state.apply(&transaction.stage().unwrap()).unwrap();
        persist_with_blooms(&state, &store, true).await;
    }
    let context = query_context(state.value().snapshot(), store, false);
    let source = scan(&context, &["v", "_valid_from", "_valid_to"], TemporalFilter::All, at(1_000), None);
    let v = source.schema().fields()[0].clone();
    let filter = Arc::new(source.filter(Expr::binary(BinaryOp::Eq, v.expression(), Expr::Literal(Value::I64(1)))).unwrap());
    let (mut rows, (_, pruned, _)) = collect_with_scan_counts(filter, context).await;
    rows.sort_by_key(|row| match row[1] { Value::Timestamp { value, .. } => value, _ => 0 });
    assert_eq!(rows, [vec![Value::I64(1), timestamp(100), timestamp(200)], vec![Value::I64(1), timestamp(500), timestamp(600)]]);
    assert_eq!(pruned, 1);
}

#[tokio::test]
async fn future_system_time_ranges_see_versions_live_at_the_basis() {
    let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
    let budget = MemoryBudget::new(1 << 27);
    let store = Arc::new(MemoryStore::default());

    let mut first = tx(&state, &budget, 0, 100);
    first.put(table(), document(1, Value::I64(1)), Interval::ALL).unwrap();
    first.put(table(), document(2, Value::I64(10)), Interval::ALL).unwrap();
    state.apply(&first.stage().unwrap()).unwrap();
    persist(&state, &store).await;

    let mut second = tx(&state, &budget, 1, 200);
    second.put(table(), document(1, Value::I64(2)), Interval::ALL).unwrap();
    second.delete(table(), &EntityId::Integer(2), Interval::ALL).unwrap();
    state.apply(&second.stage().unwrap()).unwrap();

    let snapshot = state.value().snapshot();
    for bypass in [false, true] {
        let context = query_context(snapshot.clone(), Arc::clone(&store), bypass);
        for system_time in [
            at(300),
            TemporalFilter::FromTo { from: Expr::Literal(timestamp(300)), to: Expr::Literal(timestamp(400)) },
            TemporalFilter::FromTo { from: Expr::Literal(timestamp(300)), to: Expr::Literal(Value::Null) },
            TemporalFilter::Between { from: Expr::Literal(timestamp(300)), to: Expr::Literal(timestamp(400)) },
            TemporalFilter::Between { from: Expr::Literal(timestamp(300)), to: Expr::Literal(timestamp(300)) },
        ] {
            let rows = collect(scan(&context, &["_id", "v", "_system_from", "_system_to"], TemporalFilter::All, system_time, None), Arc::clone(&context)).await.unwrap();
            assert_eq!(rows, vec![vec![Value::I64(1), Value::I64(2), timestamp(200), Value::Null]]);
        }
        for empty in [
            at(Timestamp::MAX.0),
            TemporalFilter::FromTo { from: Expr::Literal(timestamp(300)), to: Expr::Literal(timestamp(300)) },
            TemporalFilter::FromTo { from: Expr::Literal(timestamp(400)), to: Expr::Literal(timestamp(300)) },
            TemporalFilter::Between { from: Expr::Literal(timestamp(400)), to: Expr::Literal(timestamp(300)) },
        ] {
            let rows = collect(scan(&context, &["v"], TemporalFilter::All, empty, None), Arc::clone(&context)).await.unwrap();
            assert!(rows.is_empty());
        }
    }
}
