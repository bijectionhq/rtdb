mod checkpoint;
mod export;
pub(super) mod wire;

use super::{Error, artifacts, combine, flush, instant, store::Reads, support::{FixtureClock, Identity, RunningNode}, write_new};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema as ArrowSchema};
use rtdb_columnar::{IidView, build_iids, read_value, to_field};
use rtdb_format::{ipc::{ArrowFile, write_file}, keys::{CURRENT_STORAGE_VERSION, TrieKey, storage_root}};
use rtdb_node::{AutonomousTxOptions, NativeResources, Node};
use rtdb_plan::{AggregateExpr, AggregateFunction, BinaryOp, Catalog, ColumnId, Expr, Field, JoinKind, NamedExpr, QualifiedTable, Relation, RelationNode, Scan, Schema, SetOperator, TemporalFilter};
use rtdb_query::{PreparedQuery, QueryCursor};
use rtdb_tx::{Documents, EntityIds, TxOp};
use rtdb_types::{EntityId, LogicalType, RunEndWidth, ScalarType, StorageEpoch, TableRef, TimeUnit, Timestamp, TransactionKey, Value};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};
use rtdb_types::StructFields;
use std::{collections::{BTreeMap, BTreeSet}, fs::{self, File}, io::Write, ops::Range, path::{Path, PathBuf}, sync::{Arc, atomic::{AtomicI64, Ordering}}, time::Duration};

const SOURCE_SHA256: &str = "7cdd0bf680e9311f9d814c4c0b07fd6db2282995528d80510ebc5b7f80fe9d39";
const MAX_REWRITE_BYTES: u64 = 32 << 20;
const HOUR_MICROS: i64 = 3_600_000_000;
const SORTED_IIDS: &str = "SELECT _id, _iid FROM readings FOR ALL VALID_TIME FOR ALL SYSTEM_TIME ORDER BY _id";
const HISTORY: &str = "SELECT *, _valid_from, _valid_to, _system_from, _system_to FROM readings FOR ALL VALID_TIME FOR ALL SYSTEM_TIME ORDER BY _id, _system_from, _valid_from";
const LIVE_TRIES: &str = "SELECT trie_key FROM xt.trie_stats WHERE table_name = 'readings' AND trie_state = 'live' AND level > 0";
const PATCH: &str = "PATCH INTO readings FOR VALID_TIME FROM TIMESTAMP '2020-01-03T00:00:00Z' RECORDS {_id: 7, extra: 1}";
const PATCH_READ: &str = "SELECT *, _valid_from, _valid_to FROM readings FOR ALL VALID_TIME WHERE _id = 7 ORDER BY _valid_from";

async fn open(directory: &Path, clock: Arc<FixtureClock>) -> Result<RunningNode, Error> {
    let resources = Arc::new(NativeResources::default().with_local_log_clock(clock.clone(), false));
    RunningNode::open_configured(directory, clock, Arc::new(Reads::default()), resources, false, |options| {
        options.default_timezone = "Europe/London".into();
        options.replication.rows_per_block = 200;
        options.file_size_target = 16 * 1024;
        options.replication.flush_timeout = Duration::from_secs(2 * 366 * 24 * 60 * 60);
    }).await
}

async fn submit(node: &Node, clock: &FixtureClock, time: Timestamp, operations: Vec<TxOp>) -> Result<TransactionKey, Error> {
    clock.0.store(time.0, Ordering::SeqCst);
    let mut session = node.session("xtdb", Arc::new(Identity))?;
    let transaction = session.state_mut().execute_tx(operations, AutonomousTxOptions::default()).await?;
    if !transaction.committed() || transaction.tx_key().system_time != time {
        return Err(format!("Mixed IID transaction did not commit at {time:?}: {transaction:?}").into());
    }
    Ok(transaction.tx_key())
}

async fn ingest(node: &Node, clock: &FixtureClock, ticks: Range<i64>) -> Result<Vec<Json>, Error> {
    let table = TableRef::new("public", "readings");
    let start = instant("2020-01-01T00:00:00Z")?;
    let mut committed = Vec::new();
    for tick in ticks {
        let time = Timestamp(start.0 + tick * 6 * HOUR_MICROS);
        let documents = (0..30).map(|id| StructFields::from([
            ("_id".into(), Value::I64(id)), ("value".into(), Value::I64(tick)),
        ])).collect();
        let mut operations = vec![TxOp::PutDocs {
            table: table.clone(), documents: Documents::new(documents, node.memory())?,
            valid_from: Some(time), valid_to: None,
        }];
        let delete = (tick % 10 == 0).then(|| Timestamp(time.0 + HOUR_MICROS));
        if let Some(valid_from) = delete {
            operations.push(TxOp::DeleteDocs {
                table: table.clone(), ids: EntityIds::new((0..3).map(EntityId::Integer).collect(), node.memory())?,
                valid_from: Some(valid_from), valid_to: None,
            });
        }
        let key = submit(node, clock, time, operations).await?;
        committed.push(json!({
            "tick": tick, "tx_id": key.tx_id.0, "system_time_micros": time.0,
            "put": {"ids": (0..30).collect::<Vec<_>>(), "value": tick, "valid_from_micros": time.0},
            "delete": delete.map(|from| json!({"ids": [0,1,2], "valid_from_micros": from.0})),
        }));
    }
    flush(node).await?;
    let database = node.catalog().database("xtdb")?;
    let block = database.status().adopted_block.ok_or("Mixed IID fixture has no adopted block")?;
    let last_tx = committed.last().and_then(|last| last["tx_id"].as_i64());
    if block.latest_completed_tx().map(|key| key.tx_id.0) != last_tx {
        return Err("Mixed IID flush did not adopt the last fixture transaction".into());
    }
    let mut session = node.session("xtdb", Arc::new(Identity))?;
    let context = session.query_context().await?;
    let snapshot = &context.catalog.database("xtdb")?.partitions[0];
    if !snapshot.segments(&table).is_empty() || !snapshot.trie_entries(&table).iter().any(|trie| trie.key.level() == 0) {
        return Err("Mixed IID flush did not expose persisted L0 readings before compaction".into());
    }
    drop((context, session));
    tokio::time::timeout(Duration::from_secs(120), database.compaction().ok_or("Mixed IID compactor is missing")?.compact_all()).await??;
    Ok(committed)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize)]
enum IidEncoding {
    #[serde(rename = "plain")]
    Plain,
    #[serde(rename = "run-end-i32")]
    RunEndI32,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PageShape {
    rows: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    runs: Option<usize>,
}

#[derive(Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FileShape {
    bytes: u64,
    sha256: String,
    encoding: IidEncoding,
    pages: Vec<PageShape>,
}

fn file_reader(path: &Path) -> Result<ArrowFile, Error> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.len() > MAX_REWRITE_BYTES {
        return Err(format!("Mixed IID fixture file is not a bounded regular file: {}", path.display()).into());
    }
    Ok(ArrowFile::from_reader(File::open(path)?, None)?)
}

fn iid_encoding(schema: &ArrowSchema, iid: usize) -> Result<IidEncoding, Error> {
    match schema.field(iid).data_type() {
        DataType::FixedSizeBinary(16) => Ok(IidEncoding::Plain),
        DataType::RunEndEncoded(ends, values) if ends.data_type() == &DataType::Int32
            && values.data_type() == &DataType::FixedSizeBinary(16) => Ok(IidEncoding::RunEndI32),
        other => Err(format!("Unexpected mixed IID fixture field: {other:?}").into()),
    }
}

fn page_shape(batch: &RecordBatch, iid: usize) -> Result<PageShape, Error> {
    let runs = match IidView::new(batch.column(iid).as_ref())? {
        IidView::Plain(_) => None,
        IidView::Run32(runs) => Some(runs.values().len()),
        _ => return Err("Mixed IID fixture has the wrong run-end width".into()),
    };
    Ok(PageShape { rows: batch.num_rows(), runs })
}

fn inspect_file(path: &Path) -> Result<FileShape, Error> {
    let mut reader = file_reader(path)?;
    let schema = reader.schema();
    let iid = schema.index_of("_iid")?;
    let encoding = iid_encoding(&schema, iid)?;
    let pages = (0..reader.page_count()).map(|index| page_shape(&reader.read_page(index)?, iid)).collect::<Result<_, Error>>()?;
    Ok(FileShape { bytes: fs::metadata(path)?.len(), sha256: crate::controls::digest_file(path)?, encoding, pages })
}

fn with_run_end_iids(batch: &RecordBatch, iid: usize, schema: Arc<ArrowSchema>) -> Result<RecordBatch, Error> {
    let source = IidView::new(batch.column(iid).as_ref())?;
    let iids = (0..source.len()).map(|row| source.get(row)).collect::<rtdb_types::Result<Vec<_>>>()?;
    let mut columns = batch.columns().to_vec();
    columns[iid] = build_iids(&iids, Some(RunEndWidth::I32))?;
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn run_end_schema(schema: &ArrowSchema, iid: usize) -> Result<Arc<ArrowSchema>, Error> {
    let dtype = LogicalType::RunEndEncoded { width: RunEndWidth::I32,
        value: Box::new(LogicalType::Scalar(ScalarType::FixedSizeBinary(16))) };
    let mut fields = schema.fields().iter().cloned().collect::<Vec<_>>();
    fields[iid] = Arc::new(schema.field(iid).clone().with_data_type(to_field("_iid", &dtype)?.data_type().clone()));
    Ok(Arc::new(ArrowSchema::new_with_metadata(fields, schema.metadata().clone())))
}

fn rewrite(path: &Path) -> Result<(FileShape, FileShape), Error> {
    let before = inspect_file(path)?;
    if before.encoding != IidEncoding::Plain { return Err("Original compacted fixture must store plain IIDs".into()); }
    let mut source = file_reader(path)?;
    let iid = source.schema().index_of("_iid")?;
    let schema = run_end_schema(&source.schema(), iid)?;
    let pages = (0..source.page_count()).map(|index| with_run_end_iids(&source.read_page(index)?, iid, schema.clone())).collect::<Result<Vec<_>, Error>>()?;
    let mut temporary = tempfile::NamedTempFile::new_in(path.parent().ok_or("Fixture data file has no parent")?)?;
    temporary.write_all(&write_file(&schema, &pages)?)?;
    temporary.as_file().sync_all()?;
    let mut staged = file_reader(temporary.path())?;
    if staged.schema() != schema || staged.page_count() != source.page_count() {
        return Err("Rewritten Arrow file changed its expected schema or page inventory".into());
    }
    for index in 0..source.page_count() {
        let original = source.read_page(index)?;
        let encoded = staged.read_page(index)?;
        let left = IidView::new(original.column(iid).as_ref())?;
        let right = IidView::new(encoded.column(iid).as_ref())?;
        if original.num_rows() != encoded.num_rows() || !matches!(right, IidView::Run32(_)) {
            return Err("Rewritten Arrow page changed its row count or IID encoding".into());
        }
        for row in 0..left.len() {
            if left.get(row)? != right.get(row)? { return Err("Rewritten Arrow page changed an IID value".into()); }
        }
        for (column, (left, right)) in original.columns().iter().zip(encoded.columns()).enumerate() {
            if column != iid && left.to_data() != right.to_data() {
                return Err("Rewritten Arrow page changed a non-IID array".into());
            }
        }
    }
    drop((source, staged));
    let after = inspect_file(temporary.path())?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok((before, after))
}

fn physical_root() -> Result<PathBuf, Error> {
    Ok(Path::new("storage").join(storage_root(CURRENT_STORAGE_VERSION, StorageEpoch(0), 0, 1)?))
}

fn layout_directory(output: &Path, name: &str) -> PathBuf {
    output.join(format!("mixed-iid-{name}"))
}

fn compacted_key(path: &Path, root: &Path) -> Result<Option<TrieKey>, Error> {
    let Ok(relative) = path.strip_prefix(root.join("tables")) else { return Ok(None); };
    let parts: Vec<_> = relative.components().collect();
    if parts.len() != 3 || parts[1].as_os_str() != "data" || path.extension().and_then(|extension| extension.to_str()) != Some("arrow") {
        return Ok(None);
    }
    let key: TrieKey = path.file_stem().and_then(|stem| stem.to_str()).ok_or("Missing compacted data key")?.parse()?;
    Ok((key.level() > 0).then_some(key))
}

fn closed_rewrite(directory: &Path, describe: fn(&Path) -> Result<Vec<Json>, Error>) -> Result<Vec<Json>, Error> {
    let root = physical_root()?;
    let before = describe(directory)?;
    let mut paths = BTreeSet::new();
    let mut files = Vec::new();
    for file in &before {
        let relative = file["path"].as_str().ok_or("Missing fixture artifact path")?;
        if let Some(trie_key) = compacted_key(Path::new(relative), &root)? {
            let (before, after) = rewrite(&directory.join(relative))?;
            paths.insert(relative);
            files.push(json!({"path":relative, "trie_key":trie_key.to_string(), "before":before, "after":after}));
        }
    }
    let after = describe(directory)?;
    let retained_before = before.iter().filter(|file| !file["path"].as_str().is_some_and(|path| paths.contains(path)));
    let retained_after = after.iter().filter(|file| !file["path"].as_str().is_some_and(|path| paths.contains(path)));
    if !retained_before.eq(retained_after) { return Err("Closed IID rewrite changed non-data artifacts".into()); }
    if !files.iter().any(|file| file["after"]["pages"].as_array().is_some_and(|pages|
        pages.iter().any(|page| page["runs"].as_u64().zip(page["rows"].as_u64()).is_some_and(|(runs, rows)| runs < rows)))) {
        return Err("Closed IID rewrite produced no multi-row run".into());
    }
    Ok(files)
}

async fn live_layout(node: &Node, directory: &Path, rewrites: &[Json]) -> Result<Json, Error> {
    let mut session = node.session("xtdb", Arc::new(Identity))?;
    let mut cursor = session.query_sql(LIVE_TRIES, vec![]).await?;
    let mut live = BTreeSet::new();
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            let Value::Utf8(key) = read_value(batch.schema().field(0), batch.column(0).as_ref(), row)? else {
                return Err("Live trie query did not return string keys".into());
            };
            live.insert(key.parse::<TrieKey>()?);
        }
    }
    drop(cursor);
    let context = session.query_context().await?;
    let snapshot = &context.catalog.database("xtdb")?.partitions[0];
    let table = TableRef::new("public", "readings");
    let slug = &snapshot.table_entry(&table).ok_or("Missing recorded readings table")?.slug;
    let catalog = snapshot.trie_entries(&table).into_iter().filter(|trie| trie.key.level() > 0)
        .map(|trie| (trie.key.clone(), trie.data_file_size)).collect::<BTreeMap<_, _>>();
    if !live.iter().eq(catalog.keys()) { return Err("Live trie query and captured readings catalog disagree".into()); }
    let root = physical_root()?;
    let mut files = Vec::new();
    for key in live {
        let path = root.join(slug.data_key(&key));
        let shape = inspect_file(&directory.join(&path))?;
        let rewritten = rewrites.iter().find(|rewrite| rewrite["path"].as_str() == path.to_str());
        let recorded_bytes = if let Some(rewrite) = rewritten {
            if json!(shape) != rewrite["after"] { return Err("Live rewritten data differs from its closed rewrite".into()); }
            rewrite["before"]["bytes"].as_u64().ok_or("Missing original data file size")?
        } else { shape.bytes };
        if u64::try_from(catalog[&key])? != recorded_bytes {
            return Err("The live catalog did not retain the original data file size".into());
        }
        files.push(json!({"path":path, "trie_key":key.to_string(), "catalog_bytes":catalog[&key],
            "rewritten":rewritten.is_some(), "file":shape}));
    }
    let run_encoded = files.iter().any(|file| file["rewritten"] == true && file["file"]["encoding"] == "run-end-i32");
    let plain = files.iter().any(|file| file["rewritten"] == false && file["file"]["encoding"] == "plain");
    Ok(json!({"run_encoded":run_encoded, "plain":plain, "files":files}))
}

fn typed_cell(value: Value) -> Result<Json, Error> {
    Ok(match value {
        Value::Null => json!({"kind":"null"}),
        Value::I8(value) => json!({"kind":"i64","value":value}),
        Value::I16(value) => json!({"kind":"i64","value":value}),
        Value::I32(value) => json!({"kind":"i64","value":value}),
        Value::I64(value) => json!({"kind":"i64","value":value}),
        Value::FixedSizeBinary(bytes) if bytes.len() == 16 => json!({"kind":"fixed-size-binary",
            "signed_bytes":bytes.into_iter().map(|byte| byte as i8).collect::<Vec<_>>()}),
        Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some(zone) } => {
            let (seconds, nanos) = Timestamp(value).epoch_parts();
            json!({"kind":"timestamp","epoch_second":seconds,"nanosecond":nanos,"zone":zone})
        }
        other => return Err(format!("Unexpected mixed IID result cell: {other:?}").into()),
    })
}

async fn rows(mut cursor: QueryCursor) -> Result<Json, Error> {
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        let batch = batch.batch();
        let schema = batch.schema();
        for index in 0..batch.num_rows() {
            let mut row = BTreeMap::new();
            for (field, column) in schema.fields().iter().zip(batch.columns()) {
                let name = if let Some(name) = field.name().strip_prefix('_') {
                    format!("xt/{}", name.replace('_', "-"))
                } else { field.name().clone() };
                if row.insert(name, typed_cell(read_value(field, column.as_ref(), index)?)?).is_some() {
                    return Err("Duplicate mixed IID result column".into());
                }
            }
            rows.push(row);
        }
    }
    Ok(json!(rows))
}

fn count_iids(catalog: &dyn Catalog, join: bool) -> Result<Relation, Error> {
    let table = catalog.table(&QualifiedTable { database: "xtdb".into(), table: TableRef::new("public", "readings") })?
        .ok_or("Missing readings binding")?;
    let iid_type = table.scan_column_type("_iid");
    let field = |id, name: &str, data_type| Field { id: ColumnId(id), name: name.into(), qualifier: None,
        qualified_only: false, unnamed: false, data_type };
    let scan = |id| Relation::scan(Scan { table: table.clone(), columns: vec![field(id, "_iid", iid_type.clone())],
        valid_time: TemporalFilter::All, system_time: TemporalFilter::All, clamp_valid_time: false, iids: None, predicates: vec![], top_k: None, ordered_top_k: None, matches: Vec::new() });
    let left = Arc::new(scan(0)?);
    let right = Arc::new(scan(1)?);
    let input = if join {
        let right_column = NamedExpr { field: field(1, "r_iid", iid_type), expr: right.schema().fields()[0].expression() };
        let right = Arc::new(Relation::new(Schema::new(vec![right_column.field.clone()])?,
            RelationNode::Rename { input: right, prefix: String::new(), expressions: vec![right_column] })?);
        let condition = Expr::binary(BinaryOp::Eq, left.schema().fields()[0].expression(), right.schema().fields()[0].expression());
        Relation::new(left.schema().concat(right.schema())?, RelationNode::Join { left, right, kind: JoinKind::Inner, condition: Some(condition) })?
    } else {
        Relation::new(left.schema().clone(), RelationNode::Set { left, right, operator: SetOperator::Intersect, all: true })?
    };
    let count = AggregateExpr { field: field(2, "n", LogicalType::Scalar(ScalarType::I64)),
        function: AggregateFunction::CountStar, argument: None, distinct: false, filter: None, order_by: vec![] };
    Ok(Relation::new(Schema::new(vec![count.field.clone()])?, RelationNode::Aggregate {
        input: Arc::new(input), groups: vec![], aggregates: vec![count],
    })?)
}

async fn observations(node: &Node) -> Result<Json, Error> {
    let mut session = node.session("xtdb", Arc::new(Identity))?;
    let sorted = rows(session.query_sql(SORTED_IIDS, vec![]).await?).await?;
    let history = rows(session.query_sql(HISTORY, vec![]).await?).await?;
    let mut counts = Vec::new();
    for join in [true, false] {
        let context = session.query_context().await?;
        let relation = count_iids(&context.catalog, join)?;
        let cursor = PreparedQuery::new_in(Arc::new(relation), &context.memory)?.execute(context, vec![]).await?;
        counts.push(rows(cursor).await?);
    }
    Ok(json!({"sorted-iids":sorted, "history":history, "iid-join":counts[0], "iid-intersect":counts[1]}))
}

async fn patch(node: &Node, clock: &FixtureClock) -> Result<(Json, Json), Error> {
    let time = instant("2021-01-01T00:00:00Z")?;
    let key = submit(node, clock, time, vec![TxOp::Sql { sql: PATCH.into(), args: None }]).await?;
    let mut session = node.session("xtdb", Arc::new(Identity))?;
    let rows = rows(session.query_sql(PATCH_READ, vec![]).await?).await?;
    Ok((json!({"tx_id":key.tx_id.0, "system_time_micros":key.system_time.0, "sql":PATCH}), rows))
}

async fn second_phase(control: &Node, mixed: &Node, control_clock: &FixtureClock, mixed_clock: &FixtureClock,
                      output: &Path, rewritten: &[Json]) -> Result<(Json, Json, Json), Error> {
    for node in [control, mixed] { node.sync_all(Duration::from_secs(30), node.cancellation()).await?; }
    let control_ticks = ingest(control, control_clock, 40..70).await?;
    let mixed_ticks = ingest(mixed, mixed_clock, 40..70).await?;
    let control_layout = live_layout(control, &layout_directory(output, "control"), &[]).await?;
    let mixed_layout = live_layout(mixed, &layout_directory(output, "mixed"), rewritten).await?;
    let layout = json!({"control":control_layout,"mixed":mixed_layout});
    write_new(&output.join("live-layout.json"), &layout)?;
    if layout["control"]["plain"] != true || layout["control"]["files"].as_array()
        .is_none_or(|files| files.iter().any(|file| file["file"]["encoding"] != "plain")) {
        return Err("The control readings layout does not contain only plain compaction files".into());
    }
    if layout["mixed"]["run_encoded"] != true || layout["mixed"]["plain"] != true {
        return Err("The live mixed readings layout does not have both original encodings".into());
    }
    let mut control_rows = observations(control).await?;
    let mut mixed_rows = observations(mixed).await?;
    let control_pgwire = wire::pgwire(control, SORTED_IIDS).await?;
    let mixed_pgwire = wire::pgwire(mixed, SORTED_IIDS).await?;
    let (control_patch, control_result) = patch(control, control_clock).await?;
    control_rows["patch"] = control_result;
    let (mixed_patch, mixed_result) = patch(mixed, mixed_clock).await?;
    mixed_rows["patch"] = mixed_result;
    let control_flight = wire::flight_sql(control).await?;
    let mixed_flight = wire::flight_sql(mixed).await?;
    Ok((layout, json!({"ticks":control_ticks,"patch":control_patch,"observations":control_rows,
                       "wire":{"pgwire":control_pgwire,"flight_sql":control_flight}}),
               json!({"ticks":mixed_ticks,"patch":mixed_patch,"observations":mixed_rows,
                       "wire":{"pgwire":mixed_pgwire,"flight_sql":mixed_flight}})))
}

struct AuthoredPrefix {
    ticks: [Vec<Json>; 2],
    rewritten: Vec<Json>,
}

async fn authored_prefix(
    output: &Path, clocks: [&Arc<FixtureClock>; 2], describe: fn(&Path) -> Result<Vec<Json>, Error>,
) -> Result<AuthoredPrefix, Error> {
    fs::create_dir(output)?;
    let mut ticks = [Vec::new(), Vec::new()];
    for ((side, clock), ticks) in ["control", "mixed"].into_iter().zip(clocks).zip(&mut ticks) {
        let directory = layout_directory(output, side);
        fs::create_dir(&directory)?;
        let running = open(&directory, Arc::clone(clock)).await?;
        let prefix = ingest(&running.node, clock, 0..40).await;
        *ticks = combine(prefix, running.close().await)?;
    }
    let rewritten = closed_rewrite(&layout_directory(output, "mixed"), describe)?;
    write_new(&output.join("closed-rewrite.json"), &json!(rewritten))?;
    Ok(AuthoredPrefix { ticks, rewritten })
}

fn prefix_clocks() -> Result<[Arc<FixtureClock>; 2], Error> {
    let time = instant("2020-01-01T00:00:00Z")?;
    Ok(std::array::from_fn(|_| Arc::new(FixtureClock(AtomicI64::new(time.0)))))
}

/// Runs the native mixed IID fixture in a fresh directory and joins both local node owners.
/// The parent of `output` must exist, and `output` must be a new directory.
/// Returns the internal and wire observations from the same reopened nodes.
/// Writes the returned JSON to `output/execution.json` after both nodes have joined.
pub async fn execute(output: &Path) -> Result<Json, Error> {
    let [control_clock, mixed_clock] = prefix_clocks()?;
    let prefix = authored_prefix(output, [&control_clock, &mixed_clock], artifacts).await?;

    observe_reopened(output, control_clock, mixed_clock,
                     FirstIngest::Authored { prefixes: &prefix.ticks, rewritten: &prefix.rewritten }).await
}

/// Captures both closed native stores at the mixed IID rewrite boundary before tick 40.
/// The output must be a new absolute directory beneath a canonical existing parent.
/// Writes `output/closed-rewrite.json` and returns the manifest written to `output/execution.json`.
/// A failure can leave partial output, so retries use a fresh directory.
pub async fn capture_checkpoint(output: &Path) -> Result<Json, Error> {
    let parent = output.parent().ok_or("Native checkpoint output has no parent")?;
    let name = output.file_name().ok_or("Native checkpoint output must name a fresh directory")?;
    if !parent.is_absolute() || parent.canonicalize()?.join(name) != output {
        return Err("Native checkpoint output must be canonical and absolute".into());
    }
    let [control_clock, mixed_clock] = prefix_clocks()?;
    let prefix = authored_prefix(output, [&control_clock, &mixed_clock], export::bounded_artifacts).await?;
    let execution = export::manifest(output, &prefix)?;
    write_new(&output.join("execution.json"), &execution)?;
    Ok(execution)
}

/// Runs the second native ingest and records observations on copies of the closed JVM checkpoint.
/// `request` must be a canonical absolute regular file beside the existing canonical `output` directory.
/// `output` must contain the writable copies declared in `request`.
/// Returns the same JSON written to `output/execution.json` after both node owners join.
pub async fn replay_checkpoint(request: &Path, output: &Path) -> Result<Json, Error> {
    let replay = checkpoint::load(request, output)?;
    let clock = || Arc::new(FixtureClock(AtomicI64::new(checkpoint::LAST_PREFIX_TIME.0)));
    observe_reopened(output, clock(), clock(), FirstIngest::Recovered(&replay)).await
}

enum FirstIngest<'a> {
    Authored { prefixes: &'a [Vec<Json>], rewritten: &'a [Json] },
    Recovered(&'a checkpoint::Replay),
}

async fn observe_reopened(
    output: &Path, control_clock: Arc<FixtureClock>, mixed_clock: Arc<FixtureClock>, first: FirstIngest<'_>,
) -> Result<Json, Error> {
    let control = open(&layout_directory(output, "control"), control_clock.clone()).await?;
    let mixed = match open(&layout_directory(output, "mixed"), mixed_clock.clone()).await {
        Ok(mixed) => mixed,
        Err(error) => return combine(Err(error), control.close().await),
    };
    let rewritten: &[Json] = match &first {
        FirstIngest::Authored { rewritten, .. } => rewritten,
        FirstIngest::Recovered(replay) => &replay.rewritten,
    };
    let work = async {
        let recovery = if let FirstIngest::Recovered(replay) = &first {
            let control_basis = checkpoint::observe_recovery(&control.node, &replay.heads.control).await?;
            let mixed_basis = checkpoint::observe_recovery(&mixed.node, &replay.heads.mixed).await?;
            let recovered = json!({"control":control_basis,"mixed":mixed_basis});
            write_new(&output.join("recovered-checkpoint.json"), &recovered)?;
            Some(recovered)
        } else { None };
        let (layout, control, mixed) = second_phase(&control.node, &mixed.node, &control_clock, &mixed_clock, output, rewritten).await?;
        Ok::<_, Error>((layout, control, mixed, recovery))
    }.await;
    let mixed_closed = mixed.close().await;
    let control_closed = control.close().await;
    let (layout, mut control, mut mixed, recovery) = combine(work, combine(mixed_closed, control_closed))?;
    for (index, (name, receipt)) in [("control", &mut control), ("mixed", &mut mixed)].into_iter().enumerate() {
        if let FirstIngest::Authored { prefixes, .. } = &first { receipt["first_ticks"] = json!(prefixes[index]); }
        receipt["artifacts"] = json!(artifacts(&layout_directory(output, name))?);
    }
    let mut execution = json!({"schema_version":if recovery.is_some() { 3 } else { 2 },"engine":"rust","source_sha256":SOURCE_SHA256,
        "binary_sha256":crate::controls::digest_file(&std::env::current_exe()?)?,
        "artifact_revision":env!("RTDB_CONFORMANCE_REVISION"),"source_dirty":env!("RTDB_CONFORMANCE_DIRTY")!="false",
        "execution_complete":true,"complete_parity":false,
        "settings":{"timezone":"Europe/London","rows_per_block":200,"file_size_target":16384,
            "automatic_compaction":false,"automatic_gc":false,"plain_compaction":true},
        "rewritten_files":rewritten,"live_layout":layout,"control":control,"mixed":mixed});
    if let FirstIngest::Recovered(replay) = first {
        execution["mode"] = json!("jvm-checkpoint");
        execution["checkpoint_request_sha256"] = json!(replay.request_sha256);
        execution["reference_result_sha256"] = json!(replay.reference_result_sha256);
        execution["reference_execution_sha256"] = json!(replay.reference_execution_sha256);
        execution["recovered"] = json!(recovery);
    }
    write_new(&output.join("execution.json"), &execution)?;
    Ok(execution)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;
    use arrow_schema::Field as ArrowField;
    use rtdb_types::Iid;

    #[test]
    fn iid_rewrite_shares_other_arrays_and_round_trips_a_multi_row_run() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fixture.arrow");
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("_iid", DataType::FixedSizeBinary(16), false),
            ArrowField::new("value", DataType::Int64, false),
        ]));
        let values = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let original = RecordBatch::try_new(schema.clone(), vec![
            build_iids(&[Iid([128; 16]), Iid([128; 16]), Iid([255; 16])], None).unwrap(), values,
        ]).unwrap();
        let encoded = with_run_end_iids(&original, 0, run_end_schema(&schema, 0).unwrap()).unwrap();
        assert!(Arc::ptr_eq(original.column(1), encoded.column(1)));
        fs::write(&path, write_file(&schema, &[original]).unwrap()).unwrap();
        let (before, after) = rewrite(&path).unwrap();
        assert_eq!(before.encoding, IidEncoding::Plain);
        assert_eq!(after.encoding, IidEncoding::RunEndI32);
        assert_ne!(before.sha256, after.sha256);
        assert_eq!(after.pages, [PageShape { rows: 3, runs: Some(2) }]);
    }

    #[test]
    fn iid_rewrite_preserves_a_compacted_file_without_pages() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("empty.arrow");
        let schema = ArrowSchema::new(vec![ArrowField::new("_iid", DataType::FixedSizeBinary(16), false)]);
        fs::write(&path, write_file(&schema, &[]).unwrap()).unwrap();
        let (before, after) = rewrite(&path).unwrap();
        assert_eq!(before.encoding, IidEncoding::Plain);
        assert_eq!(after.encoding, IidEncoding::RunEndI32);
        assert!(before.pages.is_empty() && after.pages.is_empty());
    }
}
