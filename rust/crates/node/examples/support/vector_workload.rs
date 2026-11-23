use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, atomic::AtomicI64};
use std::time::{Duration, Instant};

use arrow_array::{Array, ArrayRef, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int64Array, RecordBatch, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema};
use rtdb_format::database::proto::{DatabaseConfig, LocalLog, LocalStorage, database_config};
use rtdb_node::{Membership, NativeSession, NativeResources, Node};
use rtdb_plan::{Relation, RelationNode, Statement};
use rtdb_query::{PreparedQuery, QueryContext};
use rtdb_storage::cache::BufferPool;
use rtdb_tx::{ArrowDocuments, EntityIds, TxOp};
use rtdb_types::{BlockIndex, EntityId, EventOp, Iid, TableRef, Timestamp};
use serde_json::{Value as Json, json};

use super::{Identity, TickClock, bench_options, bench_services, commit, flush, sql};
#[path = "vector_index.rs"]
mod index;
use index::{Embeddings, Filter, VectorIndex};

const VALID_AT: Timestamp = Timestamp(0);
const TOP: usize = 10;

type Rows = Vec<(i64, f64)>;

fn documents<'a>(node: &Node, rows: impl IntoIterator<Item = (i64, &'a [f32], u32, u32)>, dimensions: usize, valid_from: Timestamp, valid_to: Option<Timestamp>) -> ArrowDocuments {
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let width = i32::try_from(dimensions).unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("_id", DataType::Int64, false),
        Field::new("embedding", DataType::FixedSizeList(Arc::clone(&item), width), false),
        Field::new("random_group", DataType::Int64, false),
        Field::new("cluster_group", DataType::Int64, false),
        Field::new("_valid_from", DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())), false),
        Field::new("_valid_to", DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())), true),
    ]));
    let rows = rows.into_iter();
    let count = rows.size_hint().0;
    let mut ids = Vec::with_capacity(count);
    let mut components = Vec::with_capacity(count * dimensions);
    let mut random = Vec::with_capacity(count);
    let mut cluster = Vec::with_capacity(count);
    for (id, embedding, random_group, cluster_group) in rows {
        assert_eq!(embedding.len(), dimensions);
        ids.push(id);
        components.extend_from_slice(embedding);
        random.push(i64::from(random_group));
        cluster.push(i64::from(cluster_group));
    }
    let embeddings = FixedSizeListArray::try_new(item, width, Arc::new(Float32Array::from(components)), None).unwrap();
    let count = ids.len();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids)), Arc::new(embeddings),
        Arc::new(Int64Array::from(random)), Arc::new(Int64Array::from(cluster)),
        Arc::new(TimestampMicrosecondArray::from(vec![valid_from.0; count]).with_timezone("UTC")),
        Arc::new(TimestampMicrosecondArray::from(vec![valid_to.map(|to| to.0); count]).with_timezone("UTC")),
    ];
    let input = RecordBatch::try_new(schema, columns).unwrap();
    let input = rtdb_columnar::OwnedBatch::new(input, node.memory()).unwrap();
    ArrowDocuments::from_ingest_batch(&input, node.memory()).unwrap().unwrap()
}

async fn put(session: &mut NativeSession, table: &TableRef, documents: ArrowDocuments) {
    commit(session, vec![TxOp::PutArrowDocs { table: table.clone(), documents }]).await;
}

fn scope_scan(relation: &Arc<Relation>, iids: &BTreeSet<Iid>) -> Arc<Relation> {
    let mut node = relation.node().clone();
    match &mut node {
        RelationNode::Scan(scan) => {
            assert_eq!(scan.table.name.table, TableRef::new("public", "vectors"));
            assert!(scan.iids.is_none());
            scan.iids = Some(iids.clone());
        }
        RelationNode::Project { input, .. } | RelationNode::Extend { input, .. } |
        RelationNode::Rename { input, .. } | RelationNode::Filter { input, .. } |
        RelationNode::Sort { input, .. } | RelationNode::SortLimit { input, .. } |
        RelationNode::Limit { input, .. } => *input = scope_scan(input, iids),
        node => panic!("unexpected vector query plan: {node:?}"),
    }
    Arc::new(Relation::new(relation.schema().clone(), node).unwrap())
}

async fn search(context: &Arc<QueryContext>, query: &[f32], valid: Timestamp, filter: Filter, iids: Option<&BTreeSet<Iid>>) -> Rows {
    search_with_scorer(context, query, valid, filter, iids, false).await
}

async fn search_with_scorer(context: &Arc<QueryContext>, query: &[f32], valid: Timestamp, filter: Filter,
    iids: Option<&BTreeSet<Iid>>, scalar: bool) -> Rows {
    if iids.is_some_and(BTreeSet::is_empty) { return Rows::new(); }
    let predicate = filter.sql();
    let distance = if scalar {
        "CASE WHEN $3 THEN COSINE_DISTANCE(embedding, $1) ELSE NULL END"
    } else {
        "COSINE_DISTANCE(embedding, $1)"
    };
    let statement = format!(
        "SELECT _id, {distance} AS distance FROM vectors \
         FOR VALID_TIME AS OF $2 WHERE {predicate} ORDER BY distance, _id LIMIT {TOP}"
    );
    let options = rtdb_sql::PrepareOptions { default_timezone: context.timezone.clone(), ..Default::default() };
    let mut prepared = rtdb_sql::prepare_in(&statement, &context.catalog, &options, &context.memory).unwrap();
    if let Some(iids) = iids {
        let Statement::Query(relation) = &mut prepared.statement else { unreachable!() };
        *relation = scope_scan(relation, iids);
    }
    let item = Arc::new(Field::new("item", DataType::Float32, false));
    let query = FixedSizeListArray::try_new(item, i32::try_from(query.len()).unwrap(), Arc::new(Float32Array::from(query.to_vec())), None).unwrap();
    let mut args: Vec<ArrayRef> = vec![Arc::new(query), Arc::new(TimestampMicrosecondArray::from(vec![valid.0]).with_timezone("UTC"))];
    if scalar { args.push(Arc::new(BooleanArray::from(vec![true]))); }
    let fields = args.iter().enumerate().map(|(index, array)| Field::new(format!("?{index}"), array.data_type().clone(), false)).collect::<Vec<_>>();
    let args = rtdb_columnar::OwnedBatch::new(RecordBatch::try_new(Arc::new(Schema::new(fields)), args).unwrap(), &context.memory).unwrap();
    let types = args.batch().schema_ref().fields().iter().map(|field| rtdb_columnar::from_field(field).unwrap()).collect::<Vec<_>>();
    let mut cursor = PreparedQuery::from_prepared(prepared, &context.memory).unwrap()
        .execute_arrow_columns(Arc::clone(context), args, 0, &types, None).await.unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await.unwrap() {
        let batch = batch.batch();
        let ids = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let distances = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!((ids.null_count(), distances.null_count()), (0, 0));
        rows.extend(ids.values().iter().copied().zip(distances.values().iter().copied()));
    }
    rows
}

struct Coverage {
    rows: usize,
    flushed: BlockIndex,
    system_at: Timestamp,
}

impl Coverage {
    fn capture(context: &QueryContext, table: &TableRef, rows: usize) -> Self {
        let partition = &context.catalog.database("xtdb").unwrap().partitions[0];
        Self { rows, flushed: partition.trie_catalog().l0_max_block_index(table), system_at: partition.tx_basis().unwrap().system_time }
    }

    fn tail(&self, context: &QueryContext, table: &TableRef, valid: Timestamp) -> Option<BTreeSet<Iid>> {
        let [partition] = context.catalog.database("xtdb").ok()?.partitions.as_slice() else { return None };
        let system = partition.system_time_cap()?;
        if valid != VALID_AT || system < self.system_at || partition.has_overlays() ||
            partition.trie_catalog().l0_max_block_index(table) != self.flushed {
            return None;
        }
        let mut ids = BTreeSet::new();
        let mut events = 0;
        for segment in partition.iter_segments(table) {
            for row in 0..segment.row_count() {
                let header = segment.header(row).unwrap();
                if header.system_from <= self.system_at || header.system_from > system { continue; }
                events += 1;
                if events > 1024 { return None; }
                if header.op != EventOp::Erase && !(header.valid_time.lower() <= valid && valid < header.valid_time.upper()) {
                    continue;
                }
                ids.insert(header.iid);
            }
        }
        Some(ids)
    }
}

struct CapturedIndex<'a> {
    table: &'a TableRef,
    coverage: &'a Coverage,
    index: &'a VectorIndex,
    cache: &'a BufferPool,
}

impl CapturedIndex<'_> {
    async fn approximate(&self, context: &Arc<QueryContext>, query: &[f32], valid: Timestamp, filter: Filter, probes: usize) -> (Rows, Json) {
        let started = Instant::now();
        let Some(mut ids) = self.coverage.tail(context, self.table, valid) else {
            let rows = search(context, query, valid, filter, None).await;
            return (rows, json!({"route": "exact", "ms": started.elapsed().as_secs_f64() * 1000.0}));
        };
        let changed = ids.len();
        let candidates = self.index.candidates(self.cache, query, filter, probes, 64, &ids).await;
        assert!(candidates.rows.iter().all(|(id, _)| *id >= 0 && (*id as usize) < self.coverage.rows));
        ids.extend(candidates.rows.into_iter().map(|(_, iid)| iid));
        let rows = search(context, query, valid, filter, Some(&ids)).await;
        (rows, json!({"route": "ann", "changed_iids": changed, "ms": started.elapsed().as_secs_f64() * 1000.0,
            "requested_ranges": candidates.requested_ranges, "requested_bytes": candidates.requested_bytes,
            "object_reads": candidates.object_reads, "object_bytes": candidates.object_bytes,
            "vectors_scored": candidates.scored, "reranked": ids.len()}))
    }
}

fn compare(expected: &Rows, observed: &Rows) -> f64 {
    let matched = observed.iter().filter(|row| expected.iter().any(|known| known == *row)).count();
    if expected.is_empty() { return f64::from(observed.is_empty()); }
    matched as f64 / expected.len() as f64
}

async fn compare_routes(
    context: &Arc<QueryContext>,
    captured: &CapturedIndex<'_>,
    data: &Embeddings,
    query_count: usize,
    filters: &[Filter],
    probes: &[usize],
) -> Vec<Json> {
    let exhaustive = Arc::clone(context).with_accelerators(Arc::from([]));
    let mut samples = Vec::new();
    for (number, query) in data.queries.values().chunks_exact(data.dimensions).take(query_count).enumerate() {
        for &filter in filters {
            let paths = [(&exhaustive, false), (context, false), (&exhaustive, true)];
            let mut measured = std::array::from_fn(|_| (Rows::new(), 0.0));
            for order in 0..paths.len() {
                let position = (number + order) % paths.len();
                let (context, scalar) = paths[position];
                let start = Instant::now();
                let rows = search_with_scorer(context, query, VALID_AT, filter, None, scalar).await;
                measured[position] = (rows, start.elapsed().as_secs_f64() * 1000.0);
            }
            let [(expected, exact_ms), (ordered, ordered_ms), (scalar, scalar_exact_ms)] = measured;
            assert_eq!(ordered, expected, "query {number}, filter {filter:?}");
            let bits = |rows: &Rows| rows.iter().map(|(id, distance)| (*id, distance.to_bits())).collect::<Vec<_>>();
            assert_eq!(bits(&scalar), bits(&expected), "scalar and batch scoring diverged for query {number}, filter {filter:?}");
            for &probe in probes {
                let cold_cache = BufferPool::new(captured.index.store.clone(), 256 << 20);
                let cold = CapturedIndex { cache: &cold_cache, ..*captured };
                let (cold_rows, cold) = cold.approximate(context, query, VALID_AT, filter, probe).await;
                let (warm_rows, warm) = captured.approximate(context, query, VALID_AT, filter, probe).await;
                assert_eq!(cold_rows, warm_rows, "cache changed the result");
                assert_eq!(cold["object_reads"], cold["requested_ranges"]);
                assert_eq!(warm["object_reads"], 0);
                samples.push(json!({"query": number, "filter": format!("{filter:?}"), "probes": probe,
                    "exact_ms": exact_ms, "ordered_ms": ordered_ms, "scalar_exact_ms": scalar_exact_ms, "expected_count": expected.len(),
                    "returned": warm_rows.len(), "recall": compare(&expected, &warm_rows), "cold": cold, "warm": warm}));
            }
        }
        eprintln!("completed query {}/{} against the captured basis", number + 1, query_count);
    }
    samples
}

async fn churn(node: &Node, session: &mut NativeSession, captured: &CapturedIndex<'_>, data: &Embeddings,
    query_count: usize, filters: &[Filter], probes: &[usize]) -> Json {
    let query = &data.queries.values()[..data.dimensions];
    let all_clusters = data.centroids.len() / data.dimensions;
    let stale = captured.index.candidates(captured.cache, query, Filter::All, all_clusters, 64, &BTreeSet::new()).await;
    assert_eq!(stale.rows.len(), 64);
    let removed: BTreeSet<_> = stale.rows.into_iter().map(|(id, _)| id).collect();
    let entity_ids = |ids: &[i64]| {
        EntityIds::new(ids.iter().copied().map(EntityId::Integer).collect(), node.memory()).unwrap()
    };
    let (deleted, erased): (Vec<_>, Vec<_>) = removed.iter().copied().partition(|id| id % 2 == 0);
    let reassigned = (0..data.clusters.len()).find(|id| {
        let (random, _) = index::groups(*id, data.clusters.value(*id) as usize);
        random >= 10 && !removed.contains(&(*id as i64))
    }).unwrap();
    let fresh = data.clusters.len() as i64 + 1;
    let initial = session.query_context().await.unwrap();
    let before = search(&initial, query, VALID_AT, Filter::All, None).await;
    let reassigned_only = BTreeSet::from([EntityId::Integer(reassigned as i64).iid()]);
    let filter = Filter::RandomBelow(10);
    assert!(search(&initial, query, VALID_AT, filter, Some(&reassigned_only)).await.is_empty());

    let documents = documents(node, [(reassigned as i64, data.vector(reassigned), 0, 0), (fresh, query, 0, 0)], data.dimensions, VALID_AT, None);
    let operations = vec![
        TxOp::DeleteDocs { table: captured.table.clone(), ids: entity_ids(&deleted), valid_from: Some(VALID_AT), valid_to: None },
        TxOp::EraseDocs { table: captured.table.clone(), ids: entity_ids(&erased) },
        TxOp::PutArrowDocs { table: captured.table.clone(), documents },
    ];
    let write = async {
        let started = Instant::now();
        commit(session, operations).await;
        started.elapsed().as_secs_f64() * 1000.0
    };
    let (held, commit_ms) = tokio::join!(search(&initial, query, VALID_AT, Filter::All, None), write);
    assert_eq!(held, before);

    let current = session.query_context().await.unwrap();
    assert_eq!(search(&current, query, VALID_AT, filter, Some(&reassigned_only)).await[0].0, reassigned as i64);
    let expected = search(&current, query, VALID_AT, Filter::All, None).await;
    assert_eq!(expected.len(), TOP);
    assert_eq!(expected[0].0, fresh);
    let (result, all_probes) = captured.approximate(&current, query, VALID_AT, Filter::All, all_clusters).await;
    assert_eq!(result, expected);
    assert!(all_probes["changed_iids"].as_u64().unwrap() >= 66);

    let stale_ids = removed.into_iter().chain([reassigned as i64, fresh]).map(|id| EntityId::Integer(id).iid()).collect();
    let without_shadowing = search(&current, query, VALID_AT, Filter::All, Some(&stale_ids)).await;
    assert!(without_shadowing.len() < TOP);
    let samples = compare_routes(&current, captured, data, query_count, filters, probes).await;
    json!({"deletes": deleted.len(), "erases": erased.len(), "puts": 2, "commit_ms": commit_ms,
        "result_count_without_shadowing": without_shadowing.len(), "result_count_with_shadowing": result.len(),
        "all_probes": all_probes, "samples": samples})
}

async fn fixture(data: Embeddings, query_count: usize, filters: &[Filter], probes: &[usize], workload: &str, file_backed: bool) -> Json {
    let count = data.clusters.len();
    let table = TableRef::new("public", "vectors");
    let clock = Arc::new(TickClock(AtomicI64::new(1_000_000)));
    let scratch = tempfile::tempdir().unwrap();
    let mut options = bench_options();
    options.cache_bytes = 512 << 20;
    options.execution_memory_bytes = 2 << 30;
    let mut services = bench_services(&clock);
    if file_backed {
        let primary = DatabaseConfig {
            storage: Some(database_config::Storage::LocalStorage(LocalStorage {
                path: Some(scratch.path().join("table-store").to_str().unwrap().into()), epoch: Some(0),
            })),
            log: Some(database_config::Log::LocalLog(LocalLog {
                path: Some(scratch.path().join("table-log").to_str().unwrap().into()), epoch: Some(0),
            })),
            ..Default::default()
        };
        options.membership = Membership::Durable { primary, skipped: BTreeSet::new() };
        services.resources = Arc::new(NativeResources::default().with_local_log_clock(clock, false));
    }
    let (node, driver) = Node::open(options, services).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Identity)).unwrap();
    commit(&mut session, vec![
        sql("CREATE INDEX vectors_random ON vectors (random_group)"),
        sql("CREATE INDEX vectors_cluster ON vectors (cluster_group)"),
    ]).await;
    let load = Instant::now();
    let batch = if file_backed { 5_000 } else { 2_000 };
    for offset in (0..count).step_by(batch) {
        let end = (offset + batch).min(count);
        let rows = (offset..end).map(|id| {
            let (random, cluster) = index::groups(id, data.clusters.value(id) as usize);
            (id as i64, data.vector(id), random, cluster)
        });
        put(&mut session, &table, documents(&node, rows, data.dimensions, VALID_AT, None)).await;
        if (offset + batch) % 20_000 == 0 { flush(&node).await; }
    }
    flush(&node).await;
    let load_ms = load.elapsed().as_secs_f64() * 1000.0;
    eprintln!("loaded {count} vectors in {load_ms:.0} ms");
    let database = node.catalog().database("xtdb").unwrap();
    let ordered_started = Instant::now();
    database.build_artefacts().await.unwrap();
    let ordered_ms = ordered_started.elapsed().as_secs_f64() * 1000.0;
    eprintln!("built ordered indexes in {ordered_ms:.0} ms");
    let active = session.query_context().await.unwrap();
    let coverage = Coverage::capture(&active, &table, count);
    let build = Instant::now();
    let index = VectorIndex::build(&data, scratch.path()).await;
    let build_ms = build.elapsed().as_secs_f64() * 1000.0;
    let cache = index.warm_cache().await;
    let captured_index = CapturedIndex { table: &table, coverage: &coverage, index: &index, cache: &cache };
    let samples = if workload == "vectors-churn" { Vec::new() } else {
        compare_routes(&active, &captured_index, &data, query_count, filters, probes).await
    };

    let query = &data.queries.values()[..data.dimensions];
    let before = search(&active, query, VALID_AT, Filter::All, None).await;
    let update_started = Instant::now();
    put(&mut session, &table, documents(&node, [(count as i64, query, 0, 0)], data.dimensions, VALID_AT, None)).await;
    let update_ms = update_started.elapsed().as_secs_f64() * 1000.0;
    let newer = session.query_context().await.unwrap();
    let expected = search(&newer, query, VALID_AT, Filter::All, None).await;
    let all_clusters = index.centroids.len() / data.dimensions;
    let (overlay, plan) = captured_index.approximate(&newer, query, VALID_AT, Filter::All, all_clusters).await;
    assert_eq!(expected, overlay);
    assert_eq!(plan["changed_iids"], 1);
    assert_eq!(overlay[0].0, count as i64);
    assert_eq!(search(&active, query, VALID_AT, Filter::All, None).await, before);

    let affected = before[0].0;
    let previous = data.vector(affected as usize);
    let reversed: Vec<_> = previous.iter().map(|value| -*value).collect();
    put(&mut session, &table, documents(&node, [(affected, reversed.as_slice(), 999, 999)], data.dimensions, VALID_AT, Some(Timestamp(1)))).await;
    put(&mut session, &table, documents(&node, [(-1, query, 0, 0)], data.dimensions, Timestamp(1), None)).await;
    let erased = EntityIds::new(vec![EntityId::Integer(count as i64)], node.memory()).unwrap();
    commit(&mut session, vec![TxOp::EraseDocs { table: table.clone(), ids: erased }]).await;

    let corrected = session.query_context().await.unwrap();
    let expected = search(&corrected, query, VALID_AT, Filter::All, None).await;
    let (overlay, plan) = captured_index.approximate(&corrected, query, VALID_AT, Filter::All, all_clusters).await;
    assert_eq!(plan["route"], "ann");
    assert_eq!(overlay, expected);
    assert!(overlay.iter().all(|row| ![-1, count as i64, affected].contains(&row.0)));
    let (later, fallback) = captured_index.approximate(&corrected, query, Timestamp(1), Filter::All, all_clusters).await;
    assert_eq!(fallback["route"], "exact");
    assert_eq!(later, search(&corrected, query, Timestamp(1), Filter::All, None).await);
    assert!(later.iter().any(|row| row.0 == -1));
    assert!(later.iter().any(|row| row.0 == affected));

    let churn = if workload != "vectors" {
        eprintln!("running churn replay");
        churn(&node, &mut session, &captured_index, &data, query_count.min(4), filters, probes).await
    } else { Json::Null };
    let expected = if !churn.is_null() {
        let captured = session.query_context().await.unwrap();
        search(&captured, query, VALID_AT, Filter::All, None).await
    } else { expected };
    flush(&node).await;
    let flushed = session.query_context().await.unwrap();
    let (after_flush, fallback) = captured_index.approximate(&flushed, query, VALID_AT, Filter::All, all_clusters).await;
    assert_eq!(fallback["route"], "exact");
    assert_eq!(after_flush, expected);
    index.store.close_and_wait().await;
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(30), driver).await.unwrap().unwrap().unwrap();

    json!({"documents": count, "dimensions": data.dimensions, "queries": query_count,
        "primary_storage": if file_backed { "local" } else { "memory" },
        "load_ms": load_ms, "ordered_build_ms": ordered_ms, "ann_build_ms": build_ms,
        "index_bytes": index.bytes, "iid_lookup_bytes": 0,
        "tail_insert_ms": update_ms, "temporal_canaries": 5, "samples": samples, "churn": churn})
}

pub(super) async fn run(path: &Path, queries: usize, workload: &str, file_backed: bool) {
    let data = Embeddings::read(path);
    assert!(queries > 0 && queries <= data.queries.len() / data.dimensions);
    let filters = [Filter::All, Filter::RandomBelow(1), Filter::RandomBelow(10), Filter::RandomBelow(100), Filter::ClusterBelow(10)];
    let file_backed = file_backed || data.clusters.len() >= 1_000_000;
    println!("{}", fixture(data, queries, &filters, &[8, 32], workload, file_backed).await);
}

#[cfg(test)]
mod tests {
    use arrow_array::{Float32Array, UInt32Array};
    use super::{Embeddings, Filter, fixture};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ann_reranks_on_the_captured_basis_and_declines_uncovered_history() {
        let data = Embeddings {
            vectors: Float32Array::from_iter_values((1..=96).flat_map(|value| [1.0, value as f32])),
            queries: Float32Array::from(vec![1.0, 0.0]),
            centroids: Float32Array::from(vec![1.0, 0.0, 0.0, 1.0]),
            clusters: UInt32Array::from_iter_values((0..96).map(|row| row % 2)),
            dimensions: 2,
        };
        let report = fixture(data, 1, &[Filter::All, Filter::RandomBelow(10)], &[2], "vectors-churn", true).await;
        assert_eq!(report["temporal_canaries"], 5);
        assert_eq!(report["churn"]["result_count_without_shadowing"], 2);
        assert_eq!(report["churn"]["result_count_with_shadowing"], 10);
        for sample in report["churn"]["samples"].as_array().unwrap() {
            assert_eq!(sample["recall"], 1.0);
            assert_eq!(sample["warm"]["route"], "ann");
        }
    }
}
