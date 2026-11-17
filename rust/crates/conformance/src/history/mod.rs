mod store;
mod support;
pub mod recovery;

use rtdb_columnar::read_value;
use rtdb_node::{AutonomousTxOptions, Node};
use rtdb_query::PreparedQuery;
use rtdb_tx::{Documents, EntityIds, TxOp};
use rtdb_types::{EntityId, LogicalType, ScalarType, TableRef, TimeUnit, Timestamp, Value};
use serde::Deserialize;
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};
use std::{collections::{BTreeMap, BTreeSet}, fs, io::Write, path::Path, sync::{Arc, atomic::{AtomicI64, Ordering}}, time::Duration};
use store::Reads;
use support::{FixtureClock, Identity, RunningNode};

type Error = Box<dyn std::error::Error>;
const LAYOUTS: [&str; 4] = ["live", "flushed", "compacted", "restarted"];

#[derive(Deserialize)]
struct Fixture { schema_version: u32, kind: String, timezone: String, transactions: Vec<Transaction>, prefixes: Vec<Prefix>, queries: Vec<Query> }
#[derive(Deserialize)]
struct Transaction { system_time: String, #[serde(flatten)] operation: Operation }
#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
enum Operation {
    Put { prices: BTreeMap<String, i64>, valid_from: String, valid_to: Option<String> },
    Delete { id: String, valid_from: String },
    Erase { id: String },
}
#[derive(Deserialize)]
struct Prefix { id: String, transactions: usize }
#[derive(Deserialize)]
struct Query { id: String, prefix: String, valid_time: String, system_time: String }

fn instant(value: &str) -> Result<Timestamp, Error> { Ok(rtdb_types::Instant::parse_iso(value)?.as_micros()?) }
fn valid_id(value: &str) -> bool { !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-')) }
impl Fixture {
    fn validate(&self) -> Result<(), Error> {
        if self.schema_version != 1 || self.kind != "historical_prices" || self.timezone != "UTC" || self.transactions.is_empty() { return Err("invalid historical-price fixture header".into()); }
        let mut ids = BTreeSet::new();
        for prefix in &self.prefixes {
            if !valid_id(&prefix.id) || !ids.insert(&prefix.id) || prefix.transactions == 0 || prefix.transactions > self.transactions.len() { return Err("invalid history prefix".into()); }
        }
        let mut queries = BTreeSet::new();
        for query in &self.queries {
            if !valid_id(&query.id) || !queries.insert(&query.id) || !ids.contains(&query.prefix) { return Err("invalid history query identity".into()); }
            instant(&query.valid_time)?; instant(&query.system_time)?;
        }
        if ids.is_empty() || queries.is_empty() || ids.iter().any(|id| !self.queries.iter().any(|query| &query.prefix == *id)) { return Err("every history prefix requires queries".into()); }
        let mut previous = None;
        for transaction in &self.transactions {
            let time = instant(&transaction.system_time)?;
            if previous.is_some_and(|previous| previous >= time) { return Err("history transaction times must increase".into()); }
            previous = Some(time);
            match &transaction.operation {
                Operation::Put { prices, valid_from, valid_to } => {
                    let from = instant(valid_from)?;
                    if prices.is_empty() || prices.keys().any(|id| !matches!(id.as_str(), "A" | "B")) || valid_to.as_deref().map(instant).transpose()?.is_some_and(|to| to <= from) { return Err("invalid history put".into()); }
                }
                Operation::Delete { id, valid_from } => { if id != "A" { return Err("history deletion must target A".into()); } instant(valid_from)?; }
                Operation::Erase { id } => if id != "A" { return Err("history erasure must target A".into()); },
            }
        }
        Ok(())
    }
}

async fn submit(node: &Node, clock: &FixtureClock, transaction: &Transaction) -> Result<Json, Error> {
    let time = instant(&transaction.system_time)?;
    clock.0.store(time.0, Ordering::SeqCst);
    let table = TableRef::new("public", "products");
    let operation = match &transaction.operation {
        Operation::Put { prices, valid_from, valid_to } => TxOp::PutDocs { table,
            documents: Documents::new(prices.iter().map(|(id, price)| BTreeMap::from([("_id".into(), Value::Utf8(id.clone())), ("price".into(), Value::I64(*price))])).collect(), node.memory())?,
            valid_from: Some(instant(valid_from)?), valid_to: valid_to.as_deref().map(instant).transpose()? },
        Operation::Delete { id, valid_from } => TxOp::DeleteDocs { table, ids: EntityIds::new(vec![EntityId::String(id.clone())], node.memory())?, valid_from: Some(instant(valid_from)?), valid_to: None },
        Operation::Erase { id } => TxOp::EraseDocs { table, ids: EntityIds::new(vec![EntityId::String(id.clone())], node.memory())? },
    };
    let mut session = node.session("xtdb", Arc::new(Identity))?;
    let result = session.state_mut().execute_tx(vec![operation], AutonomousTxOptions { system_time: Some(time), ..Default::default() }).await?;
    if !result.committed() || result.tx_key().system_time != time { return Err(format!("history transaction did not commit at its requested system time: {result:?}").into()); }
    Ok(json!({"tx_id":result.tx_key().tx_id.0,"system_time_micros":time.0,"committed":true}))
}

async fn flush(node: &Node) -> Result<(), Error> {
    let database = node.catalog().database("xtdb")?;
    let previous = database.status().adopted_block.as_ref().map_or(-1, |block| block.block_index().0);
    let position = database.flush().await?;
    tokio::time::timeout(Duration::from_secs(30), async {
        database.progress().await_source(position, node.cancellation()).await?;
        loop {
            if database.status().adopted_block.as_ref().is_some_and(|block| block.block_index().0 > previous) { return Ok::<_, rtdb_types::Anomaly>(()); }
            if let Some(error) = database.progress().fault() { return Err(error); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await??;
    Ok(())
}

async fn checkpoint(node: &Node) -> Result<Json, Error> {
    let database = node.catalog().database("xtdb")?;
    let status = database.status();
    let progress = database.progress().current();
    let mut session = node.session("xtdb", Arc::new(Identity))?;
    let context = session.query_context().await?;
    let snapshot = &context.catalog.database("xtdb")?.partitions[0];
    let table = TableRef::new("public", "products");
    let tries: Vec<_> = snapshot.trie_entries(&table).iter().map(|trie| json!({"key":trie.key.to_string(),"level":trie.key.level(),"data_file_size":trie.data_file_size})).collect();
    Ok(json!({"source_position":progress.source_position.0,"replica_position":status.replica_position.0,
        "tx_key":progress.tx_key.map(|key| json!({"tx_id":key.tx_id.0,"system_time_micros":key.system_time.0})),
        "open_block":status.open_block.0,"adopted_block":status.adopted_block.as_ref().map(|block| json!({
            "index":block.block_index().0,"source_position":block.latest_source_position().map(|position|position.0),
            "boundary_replica_position":block.boundary_replica_position().map(|position|position.0),"term":block.boundary_term().0,
            "term_sequence":block.boundary_term_sequence().map(|sequence|sequence.0)})),
        "live_product_segments":snapshot.segments(&table).len(),"live_product_tries":tries}))
}

fn type_tag(kind: &LogicalType) -> Result<Json, Error> {
    let mut tags = Vec::new();
    for leg in kind.legs() {
        tags.push(match leg {
            LogicalType::Null => json!({"kind":"null"}),
            LogicalType::Scalar(ScalarType::I64) => json!({"kind":"i64"}),
            LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone }) => json!({"kind":"timestamp","unit":"microsecond","timezone":timezone}),
            other => return Err(format!("unexpected history projection type: {other:?}").into()),
        });
    }
    tags.sort_by_key(Json::to_string);
    Ok(json!(tags))
}
fn cell(value: Value) -> Result<Json, Error> {
    Ok(match value {
        Value::Null => json!({"kind":"null"}),
        Value::I64(value) => json!({"kind":"i64","value":value}),
        Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone } => json!({"kind":"timestamp","unit":"microsecond","timezone":timezone,"value":value}),
        other => return Err(format!("unexpected history cell: {other:?}").into()),
    })
}

async fn observe(node: &Node, reads: &Reads, prefix: &str, layout: &str, query: &Query, id: &str, bypass: bool) -> Result<Json, Error> {
    let pruning = if bypass { "bypassed" } else { "normal" };
    let mut session = node.session("xtdb", Arc::new(Identity))?;
    let mut context = session.query_context().await?;
    Arc::get_mut(&mut context).ok_or("fixture query context is not exclusively owned")?.options.bypass_temporal_pruning = bypass;
    let valid = rtdb_types::Instant::parse_iso(&query.valid_time)?.to_iso_string()?;
    let system = rtdb_types::Instant::parse_iso(&query.system_time)?.to_iso_string()?;
    let sql = format!("SELECT price, _valid_from AS valid_from, _valid_to AS valid_to, _system_from AS system_from, _system_to AS system_to FROM products FOR VALID_TIME AS OF TIMESTAMP '{valid}' FOR SYSTEM_TIME AS OF TIMESTAMP '{system}' WHERE _id = '{id}'");
    let before = reads.snapshot();
    let prepared = rtdb_sql::prepare_in(&sql, &context.catalog, &Default::default(), &context.memory)?;
    let mut cursor = PreparedQuery::from_prepared(prepared, &context.memory)?.execute(context, vec![]).await?;
    let schema = cursor.schema().fields().iter().map(|field| Ok(json!({"name":field.name,"types":type_tag(&field.data_type)?}))).collect::<Result<Vec<_>, Error>>()?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() {
            if rows.len() == 2 { return Err("history point query returned more than two rows".into()); }
            rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns()).map(|(field, array)| cell(read_value(field, array.as_ref(), row)?)).collect::<Result<Vec<_>, Error>>()?);
        }
    }
    drop(cursor);
    let after = reads.snapshot();
    Ok(json!({"id":format!("{prefix}/{layout}/{pruning}/{}/{id}",query.id),"executed":true,"query":query.id,"product":id,
        "valid_time":query.valid_time,"system_time":query.system_time,"pruning":pruning,"sql":sql,"schema":schema,"rows":rows,
        "read_boundary":"object_store_payload","read_bytes":after.0-before.0,"read_calls":after.1-before.1}))
}

async fn observations(node: &Node, reads: &Reads, fixture: &Fixture, prefix: &Prefix, layout: &str) -> Result<Json, Error> {
    let checkpoint = checkpoint(node).await?;
    let last = instant(&fixture.transactions[prefix.transactions-1].system_time)?.0;
    if checkpoint["tx_key"]["system_time_micros"].as_i64() != Some(last) { return Err("physical checkpoint changed the user transaction prefix".into()); }
    if layout == "live" && (checkpoint["live_product_segments"].as_u64().unwrap_or(0) == 0 || !checkpoint["live_product_tries"].as_array().ok_or("missing trie receipt")?.is_empty()) { return Err("live fixture lost its unflushed layout".into()); }
    if layout == "flushed" && (checkpoint["adopted_block"].is_null() || checkpoint["live_product_segments"] != 0
        || checkpoint["live_product_tries"].as_array().ok_or("missing trie receipt")?.is_empty()
        || checkpoint["live_product_tries"].as_array().ok_or("missing trie receipt")?.iter().any(|trie|trie["level"] != 0)) { return Err("flushed fixture does not contain only adopted L0 products".into()); }
    if matches!(layout, "compacted" | "restarted") && !checkpoint["live_product_tries"].as_array().ok_or("missing trie receipt")?.iter().any(|trie|trie["level"].as_i64().is_some_and(|level|level>0)) { return Err("compaction did not produce a live products trie above level zero".into()); }
    let mut results = Vec::new();
    for query in fixture.queries.iter().filter(|query|query.prefix==prefix.id) {
        for bypass in [false, true] {
            for id in ["A", "B"] { results.push(observe(node, reads, &prefix.id, layout, query, id, bypass).await?); }
        }
    }
    Ok(json!({"checkpoint":checkpoint,"observations":results}))
}

async fn layout(directory: &Path, fixture: &Fixture, prefix: &Prefix, layout: &str) -> Result<Json, Error> {
    fs::create_dir(directory)?;
    let clock = Arc::new(FixtureClock(AtomicI64::new(instant(&fixture.transactions[0].system_time)?.0)));
    let reads = Arc::new(Reads::default());
    let running = RunningNode::open(directory, clock.clone(), reads.clone()).await?;
    let work = async {
        let mut transactions = Vec::new();
        for transaction in &fixture.transactions[..prefix.transactions] { transactions.push(submit(&running.node, &clock, transaction).await?); }
        if layout != "live" { flush(&running.node).await?; }
        if matches!(layout, "compacted" | "restarted") {
            let database = running.node.catalog().database("xtdb")?;
            tokio::time::timeout(Duration::from_secs(60), database.compaction().ok_or("missing fixture compactor")?.compact_all()).await??;
        }
        let result = if layout == "restarted" { Json::Null } else { observations(&running.node, &reads, fixture, prefix, layout).await? };
        Ok::<_, Error>((transactions, result))
    }.await;
    let closed = running.close().await;
    let (transactions, mut result) = combine(work, closed)?;
    if layout == "restarted" {
        let restarted = RunningNode::open(directory, clock, reads.clone()).await?;
        let work = async {
            restarted.node.sync_all(Duration::from_secs(30), restarted.node.cancellation()).await?;
            observations(&restarted.node, &reads, fixture, prefix, layout).await
        }.await;
        result = combine(work, restarted.close().await)?;
    }
    result["transactions"] = json!(transactions);
    result["artifacts"] = json!(artifacts(directory)?);
    result["prefix"] = json!(prefix.id); result["layout"] = json!(layout); result["status"] = json!("observed");
    Ok(result)
}

fn combine<T>(work: Result<T, Error>, close: Result<(), Error>) -> Result<T, Error> {
    match (work, close) { (Ok(value), Ok(())) => Ok(value), (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error), (Err(error), Err(close)) => Err(format!("{error}; node shutdown also failed: {close}").into()) }
}

fn artifacts(root: &Path) -> Result<Vec<Json>, Error> {
    fn visit(root: &Path, path: &Path, output: &mut Vec<Json>) -> Result<(), Error> {
        let mut entries = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?; entries.sort_by_key(|entry|entry.file_name());
        for entry in entries {
            let path = entry.path(); let metadata = fs::symlink_metadata(&path)?;
            if metadata.is_symlink() { return Err("checkpoint contains a symbolic link".into()); }
            if metadata.is_dir() { visit(root, &path, output)?; }
            else if metadata.is_file() { output.push(json!({"path":path.strip_prefix(root)?.to_str().ok_or("non-UTF8 checkpoint path")?,"bytes":metadata.len(),"sha256":crate::controls::digest_file(&path)?})); }
            else { return Err("checkpoint contains a non-regular artifact".into()); }
        }
        Ok(())
    }
    let mut output = Vec::new(); visit(root, root, &mut output)?; Ok(output)
}

fn write_new(path: &Path, value: &Json) -> Result<(), Error> {
    let mut file = fs::OpenOptions::new().write(true).create_new(true).open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?; file.write_all(b"\n")?; file.sync_all()?; Ok(())
}

/// Runs every requested prefix through native local nodes and retains their closed database directories.
/// The output directory must not exist; the caller must await completion so every started driver is joined.
/// Expected observations are ignored, and successful execution alone does not establish parity.
pub async fn execute(fixture: &Path, output: &Path) -> Result<Json, Error> {
    let bytes = fs::read(fixture)?;
    let fixture: Fixture = serde_json::from_slice(&bytes)?; fixture.validate()?;
    fs::create_dir(output)?;
    let mut receipts = Vec::new();
    for prefix in &fixture.prefixes {
        for name in LAYOUTS {
            let directory = output.join(format!("{}-{name}",prefix.id));
            let receipt = match layout(&directory, &fixture, prefix, name).await {
                Ok(receipt) => receipt,
                Err(error) => json!({"prefix":prefix.id,"layout":name,"status":"failed","error":error.to_string(),"observations":[]}),
            };
            write_new(&output.join(format!("{}-{name}.json",prefix.id)), &receipt)?;
            receipts.push(receipt);
        }
    }
    let complete = receipts.iter().all(|receipt|receipt["status"]=="observed");
    let result = json!({"schema_version":1,"engine":"rust","fixture_sha256":format!("{:x}",Sha256::digest(&bytes)),
        "binary_sha256":crate::controls::digest_file(&std::env::current_exe()?)?,"artifact_revision":env!("RTDB_CONFORMANCE_REVISION"),
        "source_dirty":env!("RTDB_CONFORMANCE_DIRTY")!="false","execution_complete":complete,"complete_parity":false,
        "read_measurement":"successful object-store payload bytes, including cache misses, in each sequential query interval; caches remain warm between queries",
        "layouts":receipts});
    write_new(&output.join("execution.json"), &result)?;
    Ok(result)
}
