use std::collections::BTreeSet;
use std::path::Path;
use std::sync::{Arc, atomic::AtomicI64};
use std::time::{Duration, Instant};

use rtdb_columnar::read_value;
use rtdb_format::data::StoredOperation;
use rtdb_node::{NativeSession, Node};
use rtdb_query::{PreparedQuery, QueryContext};
use rtdb_storage::cache::BufferPool;
use rtdb_tx::{Documents, EntityIds, TxOp};
use rtdb_types::{BlockIndex, StructFields, TableRef, TimeUnit, Timestamp, Value};
use serde_json::{Value as Json, json};

use super::{Identity, TickClock, bench_options, bench_services, commit, flush, sql};
#[path = "vector_index.rs"]
mod index;
use index::{Embeddings, Filter, VectorIndex};

const VALID_AT: Timestamp = Timestamp(0);
const TOP: usize = 10;

type Rows = Vec<Vec<Value>>;

fn vector(values: &[f32]) -> Value {
    Value::FixedSizeList(values.iter().copied().map(Value::F32).collect())
}

fn time(value: Timestamp) -> Value {
    Value::Timestamp { value: value.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }
}

fn document(id: i64, values: &[f32], random: u32, cluster: u32) -> StructFields<Value> {
    StructFields::from([
        ("_id".into(), Value::I64(id)),
        ("embedding".into(), vector(values)),
        ("random_group".into(), Value::I64(i64::from(random))),
        ("cluster_group".into(), Value::I64(i64::from(cluster))),
    ])
}

async fn put(node: &Node, session: &mut NativeSession, table: &TableRef, rows: Vec<StructFields<Value>>, start: Timestamp, end: Option<Timestamp>) {
    let input = Documents::new(rows, node.memory()).unwrap();
    commit(session, vec![TxOp::PutDocs { table: table.clone(), documents: input, valid_from: Some(start), valid_to: end }]).await;
}

async fn search(context: &Arc<QueryContext>, query: &[f32], valid: Timestamp, filter: Filter, ids: Option<&BTreeSet<i64>>) -> Rows {
    if ids.is_some_and(BTreeSet::is_empty) { return Rows::new(); }
    let mut predicate = filter.sql();
    if let Some(ids) = ids {
        let disjuncts: Vec<_> = ids.iter().map(|id| format!("_id = {id}")).collect();
        predicate.push_str(" AND (");
        predicate.push_str(&disjuncts.join(" OR "));
        predicate.push(')');
    }
    let statement = format!(
        "SELECT _id, COSINE_DISTANCE(embedding, $1) AS distance FROM vectors \
         FOR VALID_TIME AS OF $2 WHERE {predicate} ORDER BY distance, _id LIMIT {TOP}"
    );
    let options = rtdb_sql::PrepareOptions { default_timezone: context.timezone.clone(), ..Default::default() };
    let prepared = rtdb_sql::prepare_in(&statement, &context.catalog, &options, &context.memory).unwrap();
    let mut cursor = PreparedQuery::from_prepared(prepared, &context.memory).unwrap()
        .execute(Arc::clone(context), vec![vector(query), time(valid)]).await.unwrap();
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await.unwrap() {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            rows.push(batch.schema().fields().iter().zip(batch.columns())
                .map(|(field, data)| read_value(field, data.as_ref(), row).unwrap()).collect());
        }
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

    fn tail(&self, context: &QueryContext, table: &TableRef, valid: Timestamp) -> Option<BTreeSet<i64>> {
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
                if let StoredOperation::Put(document) = segment.row(row).unwrap().event().unwrap().operation {
                    let Some(Value::I64(id)) = document.get("_id") else { return None };
                    ids.insert(*id);
                }
            }
        }
        Some(ids)
    }
}

async fn approximate(context: &Arc<QueryContext>, table: &TableRef, coverage: &Coverage, index: &VectorIndex,
    cache: &BufferPool, query: &[f32], valid: Timestamp, filter: Filter, probes: usize) -> (Rows, Json) {
    let started = Instant::now();
    let Some(mut ids) = coverage.tail(context, table, valid) else {
        let rows = search(context, query, valid, filter, None).await;
        return (rows, json!({"route": "exact", "ms": started.elapsed().as_secs_f64() * 1000.0}));
    };
    let tail = ids.len();
    let candidates = index.candidates(cache, query, filter, probes, 64).await;
    assert!(candidates.ids.iter().all(|id| *id >= 0 && (*id as usize) < coverage.rows));
    ids.extend(candidates.ids);
    let rows = search(context, query, valid, filter, Some(&ids)).await;
    (rows, json!({"route": "ann", "tail_put_iids": tail, "ms": started.elapsed().as_secs_f64() * 1000.0,
        "requested_ranges": candidates.requested_ranges, "requested_bytes": candidates.requested_bytes,
        "object_reads": candidates.object_reads, "object_bytes": candidates.object_bytes,
        "vectors_scored": candidates.scored, "reranked": ids.len()}))
}

fn compare(expected: &Rows, observed: &Rows) -> f64 {
    let matched = observed.iter().filter(|row| expected.iter().any(|known| known == *row)).count();
    if expected.is_empty() { return f64::from(observed.is_empty()); }
    matched as f64 / expected.len() as f64
}

async fn fixture(data: Embeddings, query_count: usize, filters: &[Filter], probes: &[usize]) -> Json {
    let count = data.clusters.len();
    let table = TableRef::new("public", "vectors");
    let clock = Arc::new(TickClock(AtomicI64::new(1_000_000)));
    let mut options = bench_options();
    options.cache_bytes = 512 << 20;
    options.execution_memory_bytes = 2 << 30;
    let (node, driver) = Node::open(options, bench_services(&clock)).await.unwrap();
    let driver = tokio::spawn(driver.run());
    let mut session = node.session("xtdb", Arc::new(Identity)).unwrap();
    commit(&mut session, vec![
        sql("CREATE INDEX vectors_random ON vectors (random_group)"),
        sql("CREATE INDEX vectors_cluster ON vectors (cluster_group)"),
    ]).await;
    let load = Instant::now();
    for offset in (0..count).step_by(2000) {
        let docs = (offset..(offset + 2000).min(count)).map(|id| {
            let (random, cluster) = index::groups(id, data.clusters[id]);
            document(id as i64, data.vector(id), random, cluster)
        }).collect();
        put(&node, &mut session, &table, docs, VALID_AT, None).await;
        if (offset + 2000) % 20_000 == 0 { flush(&node).await; }
    }
    flush(&node).await;
    let load_ms = load.elapsed().as_secs_f64() * 1000.0;
    let database = node.catalog().database("xtdb").unwrap();
    let ordered_started = Instant::now();
    database.build_artefacts().await.unwrap();
    let ordered_ms = ordered_started.elapsed().as_secs_f64() * 1000.0;
    let active = session.query_context().await.unwrap();
    let exhaustive = Arc::clone(&active).with_accelerators(Arc::from([]));
    let coverage = Coverage::capture(&active, &table, count);
    let scratch = tempfile::tempdir().unwrap();
    let build = Instant::now();
    let index = VectorIndex::build(&data, scratch.path()).await;
    let build_ms = build.elapsed().as_secs_f64() * 1000.0;
    let cache = index.warm_cache().await;
    let mut samples = Vec::new();
    for (number, query) in data.queries.chunks_exact(data.dimensions).take(query_count).enumerate() {
        for &filter in filters {
            let contexts = [&exhaustive, &active];
            let mut measured = [(Rows::new(), 0.0), (Rows::new(), 0.0)];
            for order in 0..contexts.len() {
                let position = (number + order) % contexts.len();
                let start = Instant::now();
                let rows = search(contexts[position], query, VALID_AT, filter, None).await;
                measured[position] = (rows, start.elapsed().as_secs_f64() * 1000.0);
            }
            let [(expected, exact_ms), (ordered, ordered_ms)] = measured;
            assert_eq!(ordered, expected, "query {number}, filter {filter:?}");
            for &probe in probes {
                let cold = BufferPool::new(index.store.clone(), 256 << 20);
                let (cold_rows, cold) = approximate(&active, &table, &coverage, &index, &cold, query, VALID_AT, filter, probe).await;
                let (warm_rows, warm) = approximate(&active, &table, &coverage, &index, &cache, query, VALID_AT, filter, probe).await;
                assert_eq!(cold_rows, warm_rows, "cache changed the result");
                assert_eq!(cold["object_reads"], cold["requested_ranges"]);
                assert_eq!(warm["object_reads"], 0);
                samples.push(json!({"query": number, "filter": format!("{filter:?}"), "probes": probe,
                    "exact_ms": exact_ms, "ordered_ms": ordered_ms, "expected_count": expected.len(),
                    "returned": warm_rows.len(), "recall": compare(&expected, &warm_rows), "cold": cold, "warm": warm}));
            }
        }
    }

    let query = &data.queries[..data.dimensions];
    let before = search(&active, query, VALID_AT, Filter::All, None).await;
    let update_started = Instant::now();
    put(&node, &mut session, &table, vec![document(count as i64, query, 0, 0)], VALID_AT, None).await;
    let update_ms = update_started.elapsed().as_secs_f64() * 1000.0;
    let newer = session.query_context().await.unwrap();
    let expected = search(&newer, query, VALID_AT, Filter::All, None).await;
    let all_clusters = index.centroids.len() / data.dimensions;
    let (overlay, plan) = approximate(&newer, &table, &coverage, &index, &cache, query, VALID_AT, Filter::All, all_clusters).await;
    assert_eq!(expected, overlay);
    assert_eq!(plan["tail_put_iids"], 1);
    assert_eq!(overlay[0][0], Value::I64(count as i64));
    assert_eq!(search(&active, query, VALID_AT, Filter::All, None).await, before);

    let affected = match before[0][0] { Value::I64(id) => id, _ => unreachable!() };
    let previous = data.vector(affected as usize);
    let reversed: Vec<_> = previous.iter().map(|value| -*value).collect();
    put(&node, &mut session, &table, vec![document(affected, &reversed, 999, 999)], VALID_AT, Some(Timestamp(1))).await;
    put(&node, &mut session, &table, vec![document(-1, query, 0, 0)], Timestamp(1), None).await;
    let erased = EntityIds::new(vec![Value::I64(count as i64).entity_id().unwrap()], node.memory()).unwrap();
    commit(&mut session, vec![TxOp::EraseDocs { table: table.clone(), ids: erased }]).await;

    let corrected = session.query_context().await.unwrap();
    let expected = search(&corrected, query, VALID_AT, Filter::All, None).await;
    let (overlay, plan) = approximate(&corrected, &table, &coverage, &index, &cache, query, VALID_AT, Filter::All, all_clusters).await;
    assert_eq!(plan["route"], "ann");
    assert_eq!(overlay, expected);
    assert!(overlay.iter().all(|row| ![Value::I64(-1), Value::I64(count as i64), Value::I64(affected)].contains(&row[0])));
    let (later, fallback) = approximate(&corrected, &table, &coverage, &index, &cache, query, Timestamp(1), Filter::All, all_clusters).await;
    assert_eq!(fallback["route"], "exact");
    assert_eq!(later, search(&corrected, query, Timestamp(1), Filter::All, None).await);
    assert!(later.iter().any(|row| row[0] == Value::I64(-1)));
    assert!(later.iter().any(|row| row[0] == Value::I64(affected)));

    flush(&node).await;
    let flushed = session.query_context().await.unwrap();
    let (after_flush, fallback) = approximate(&flushed, &table, &coverage, &index, &cache, query, VALID_AT, Filter::All, all_clusters).await;
    assert_eq!(fallback["route"], "exact");
    assert_eq!(after_flush, expected);
    index.store.close_and_wait().await;
    node.shutdown();
    tokio::time::timeout(Duration::from_secs(30), driver).await.unwrap().unwrap().unwrap();

    json!({"documents": count, "dimensions": data.dimensions, "queries": query_count,
        "load_ms": load_ms, "ordered_build_ms": ordered_ms, "ann_build_ms": build_ms,
        "index_bytes": index.bytes, "tail_insert_ms": update_ms, "temporal_canaries": 5, "samples": samples})
}

pub(super) async fn run(path: &Path, queries: usize) {
    let data = Embeddings::read(path);
    assert!(queries > 0 && queries <= data.queries.len() / data.dimensions);
    let filters = [Filter::All, Filter::RandomBelow(1), Filter::RandomBelow(10), Filter::RandomBelow(100), Filter::ClusterBelow(10)];
    println!("{}", fixture(data, queries, &filters, &[8, 32]).await);
}

#[cfg(test)]
mod tests {
    use super::{Embeddings, Filter, fixture};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn ann_reranks_on_the_captured_basis_and_declines_uncovered_history() {
        let data = Embeddings {
            vectors: (1..=32).flat_map(|value| [1.0, value as f32]).collect(),
            queries: vec![1.0, 0.0],
            centroids: vec![1.0, 0.0, 0.0, 1.0],
            clusters: (0..32).map(|row| row % 2).collect(),
            dimensions: 2,
        };
        let report = fixture(data, 1, &[Filter::All, Filter::RandomBelow(10)], &[2]).await;
        assert_eq!(report["temporal_canaries"], 5);
        for sample in report["samples"].as_array().unwrap() {
            assert_eq!(sample["recall"], 1.0);
            assert_eq!(sample["warm"]["route"], "ann");
        }
    }
}
