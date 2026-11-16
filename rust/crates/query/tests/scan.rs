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
fn document(id: i64, value: Value) -> BTreeMap<String, Value> { BTreeMap::from([("_id".into(), Value::I64(id)), ("v".into(), value)]) }
fn tx(state: &PartitionState, budget: &Arc<MemoryBudget>, id: i64, system: i64) -> TransactionBuffer {
    TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(id), system_time: Timestamp(system) }, Arc::clone(budget)).unwrap()
}
fn timestamp(value: i64) -> Value { Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) } }

#[tokio::test]
async fn stored_unsigned_cells_open_signed_without_mutating_snapshot_types() {
    let budget = MemoryBudget::new(1 << 27);
    let state = PartitionState::empty(TrieConfig::new(32, 32).unwrap()).unwrap();
    let mut transaction = tx(&state, &budget, 0, 0);
    transaction.put(table(), document(1, Value::Struct(BTreeMap::from([("scalar".into(), Value::U64(u64::MAX)), ("list".into(), Value::List(vec![Value::U8(255)]))]))), Interval::from_micros(0, i64::MAX).unwrap()).unwrap();
    state.apply(&transaction.stage().unwrap()).unwrap();
    let physical = state.value().snapshot().column_types(&table())["v"].clone();
    let store = Arc::new(MemoryStore::default());
    let live = state.value().snapshot();
    persist(&state, &store).await;
    for snapshot in [live, state.value().snapshot()] {
        let context = query_context(snapshot, Arc::clone(&store), false);
        let relation = scan(&context, &["v"], TemporalFilter::Default, TemporalFilter::Default, None);
        let mut cursor = PreparedQuery::new(relation).unwrap().execute(context, vec![]).await.unwrap();
        assert_eq!(cursor.schema().fields()[0].data_type, physical);
        assert_eq!(cursor.runtime_schema().fields()[0].data_type, rtdb_columnar::execution_type(&physical));
        let page = cursor.next_batch().await.unwrap().unwrap();
        assert_eq!(read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap(), Value::Struct(BTreeMap::from([("scalar".into(), Value::I64(-1)), ("list".into(), Value::List(vec![Value::I8(-1)]))])));
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
            let context = query_context(state.value().snapshot(), Arc::clone(&store), false);
            let relation = scan(&context, &["v"], TemporalFilter::All, at(time), None);
            assert_eq!(collect(relation, Arc::clone(&context)).await.unwrap(), vec![vec![Value::Utf8(expected.into())]]);
            assert_eq!(context.memory.used_bytes(), 0);
        }
        if codec == arrow_ipc::CompressionType::LZ4_FRAME {
            let database = DatabaseSnapshot { partitions: vec![state.value().snapshot()], storage: Arc::new(BufferPool::new(store, 1 << 26)) };
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
    let binding = context.catalog.table(&QualifiedTable { database: "xtdb".into(), table: TableRef::new("xt", "trie_stats") }).unwrap().unwrap();
    let field = Field { id: ColumnId(0), name: "recency".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: binding.columns["recency"].clone() };
    let relation = Arc::new(Relation::scan(Scan { table: binding, columns: vec![field], valid_time: TemporalFilter::All, system_time: TemporalFilter::All,
        clamp_valid_time: false, iids: None, predicates: Vec::new() }).unwrap());
    assert_eq!(collect(relation, context).await.unwrap(), vec![vec![Value::DateDay(169_560_311)]]);
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
    Arc::new(Relation::scan(Scan { table: binding, columns: fields, valid_time, system_time, clamp_valid_time: false, iids, predicates: Vec::new() }).unwrap())
}
async fn collect(relation: Arc<Relation>, context: Arc<QueryContext>) -> Result<Vec<Vec<Value>>> {
    let mut cursor = PreparedQuery::new(relation)?.execute(context, vec![]).await?;
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
    state.adopt_block(prepared, block.clone(), &additions, Timestamp(1000)).unwrap();
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
async fn reopened_catalog_reads_persisted_data_and_caps_system_time_at_its_basis() {
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
    assert!(collect(scan(&context, &["v"], TemporalFilter::All, at(200), None), context).await.unwrap().is_empty());
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
