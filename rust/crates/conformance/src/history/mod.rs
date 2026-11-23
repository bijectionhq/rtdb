mod store;
mod support;
pub(crate) mod as_of_erase;
pub mod mixed_iid;
pub mod parser_wire;
#[cfg(test)] mod noncanonical_iid;
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
use rtdb_types::StructFields;
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
            documents: Documents::new(prices.iter().map(|(id, price)| StructFields::from([("_id".into(), Value::Utf8(id.clone())), ("price".into(), Value::I64(*price))])).collect(), node.memory())?,
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

#[cfg(test)]
mod join_iid_tests {
    use super::{AutonomousTxOptions, Documents, Duration, Error, FixtureClock, Identity, Node,
        PreparedQuery, Reads, RunningNode, TableRef, Timestamp, TxOp, Value, combine, flush, read_value};
    use rtdb_format::transit::{TransitValue, decode_msgpack};
    use rtdb_types::{Keyword, StructFields, TransactionKey};
    use std::sync::{Arc, atomic::AtomicI64};

    const TABLES: [&str; 3] = ["mixed_ids", "keyword_ids", "raw_ids"];
    const RAW_KEY: [u8; 16] = *b"abcdefghijklmnop";
    const MIXED_JOIN: &str = "SELECT l.build_key, r._id, r.label
        FROM (VALUES ('byte-key')) AS l(build_key) JOIN mixed_ids r ON l.build_key = r._id";
    const RAW_JOIN: &str = "SELECT l._iid AS build_key, r._iid AS probe_key
        FROM raw_ids l JOIN raw_ids r ON l._iid = r._iid";

    fn document(id: Value, label: &str) -> StructFields<Value> {
        StructFields::from([("_id".into(), id), ("label".into(), Value::Utf8(label.into()))])
    }

    fn keyword(value: &str) -> Value { Value::Keyword(Keyword::from_name(value)) }
    fn text(value: &str) -> Value { Value::Utf8(value.into()) }

    async fn put_join_rows(node: &Node, system_time: Timestamp) -> Result<TransactionKey, Error> {
        let ops = [
            ("mixed_ids", vec![
                document(text("byte-key"), "string"),
                document(keyword("byte-key"), "keyword"),
                document(text("abcdefghijklmnop"), "long-string"),
                document(Value::Uuid(RAW_KEY), "uuid"),
                document(text("absent"), "unmatched"),
            ]),
            ("keyword_ids", vec![
                document(keyword("exact-first"), "first"),
                document(keyword("byte-key"), "second"),
            ]),
            ("raw_ids", vec![document(Value::Uuid(RAW_KEY), "raw")]),
        ].into_iter().map(|(table, docs)| {
            Ok(TxOp::PutDocs {
                table: TableRef::new("public", table),
                documents: Documents::new(docs, node.memory())?,
                valid_from: Some(Timestamp(0)),
                valid_to: None,
            })
        }).collect::<Result<Vec<_>, Error>>()?;
        let mut session = node.session("xtdb", Arc::new(Identity))?;
        let result = session.state_mut().execute_tx(ops, AutonomousTxOptions {
            system_time: Some(system_time),
            ..Default::default()
        }).await?;
        if !result.committed() || result.tx_key().system_time != system_time {
            return Err(format!("Join fixture did not commit at its requested system time: {result:?}").into());
        }
        Ok(result.tx_key())
    }

    async fn check_layout(node: &Node, prefix: TransactionKey, level: Option<i64>) -> Result<(), Error> {
        let mut session = node.session("xtdb", Arc::new(Identity))?;
        let context = session.query_context().await?;
        let snapshot = &context.catalog.database("xtdb")?.partitions[0];
        if snapshot.tx_basis() != Some(prefix) {
            return Err("The join fixture layout advanced its transaction prefix".into());
        }
        for name in TABLES {
            let table = TableRef::new("public", name);
            let tries = snapshot.trie_entries(&table);
            let live = !snapshot.segments(&table).is_empty();
            let expected = match level {
                None => live && tries.is_empty(),
                Some(0) => !live && !tries.is_empty() && tries.iter().all(|trie| trie.key.level() == 0),
                Some(_) => !live && !tries.is_empty() && tries.iter().all(|trie| trie.key.level() > 0),
            };
            if !expected { return Err(format!("Unexpected join fixture layout for {name} at {level:?}").into()); }
        }
        Ok(())
    }

    async fn join_rows(node: &Node, sql: &str, build_rows: i64) -> Result<Vec<Vec<Value>>, Error> {
        let mut session = node.session("xtdb", Arc::new(Identity))?;
        let mut context = session.query_context().await?;
        Arc::get_mut(&mut context).ok_or("Join fixture context is shared")?.options.join_build_rows = build_rows;
        let prepared = rtdb_sql::prepare_in(sql, &context.catalog, &Default::default(), &context.memory)?;
        let prepared = PreparedQuery::from_prepared(prepared, &context.memory)?;
        let mut cursor = prepared.execute(Arc::clone(&context), vec![]).await?;
        let mut rows = Vec::new();
        while let Some(batch) = cursor.next_batch().await? {
            for row in 0..batch.batch().num_rows() {
                rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns())
                    .map(|(field, array)| read_value(field, array.as_ref(), row))
                    .collect::<rtdb_types::Result<_>>()?);
            }
        }
        drop(cursor);
        if (context.spill_peak_bytes() > 0) != (build_rows == 0) || context.spill_bytes() != 0 {
            return Err(format!("Unexpected join spill state for {sql} with build threshold {build_rows}").into());
        }
        Ok(rows)
    }

    async fn check_join_results(node: &Node) -> Result<(), Error> {
        let raw = Value::FixedSizeBinary(RAW_KEY.to_vec());
        let cases = [
            (
                MIXED_JOIN,
                vec![
                    vec![text("byte-key"), text("byte-key"), text("string")],
                    vec![text("byte-key"), keyword("byte-key"), text("keyword")],
                ],
            ),
            (
                "SELECT l.build_key, r._id, r.label
                 FROM (VALUES (KEYWORD 'exact-first'), ('byte-key')) AS l(build_key)
                 JOIN keyword_ids r ON l.build_key = r._id",
                vec![
                    vec![keyword("exact-first"), keyword("exact-first"), text("first")],
                    vec![text("byte-key"), keyword("byte-key"), text("second")],
                ],
            ),
            (
                "SELECT l.build_key, r._id, r.label
                 FROM (VALUES (X'627974652d6b6579')) AS l(build_key) JOIN keyword_ids r ON l.build_key = r._id",
                vec![vec![Value::Binary(b"byte-key".to_vec()), keyword("byte-key"), text("second")]],
            ),
            (
                "SELECT l._iid AS build_key, r._iid AS probe_key, r.label
                 FROM raw_ids l JOIN (SELECT _id AS _iid, label FROM mixed_ids) r ON l._iid = r._iid",
                vec![
                    vec![raw.clone(), text("abcdefghijklmnop"), text("long-string")],
                    vec![raw.clone(), Value::Uuid(RAW_KEY), text("uuid")],
                ],
            ),
            (
                RAW_JOIN,
                vec![vec![raw.clone(), raw]],
            ),
        ];
        for (sql, expected) in cases {
            for build_rows in [i64::MAX, 0] {
                let rows = join_rows(node, sql, build_rows).await?;
                if rows.len() != expected.len() || !expected.iter().all(|row| rows.contains(row)) {
                    return Err(format!("Unexpected join rows for {sql} with build threshold {build_rows}: {rows:?}").into());
                }
            }
        }
        Ok(())
    }

    async fn check_compacted_probe_filters(node: &Node) -> Result<(), Error> {
        for (sql, source, count) in [(MIXED_JOIN, "public.mixed_ids", None), (RAW_JOIN, "public.raw_ids", Some(1))] {
            let rows = join_rows(node, &format!("EXPLAIN ANALYZE {sql}"), i64::MAX).await?;
            let source = text(source);
            let (attributes, pushdowns) = rows.iter().find_map(|row| match row.as_slice() {
                [_, op, attributes, _, _, _, _, _, Value::Transit(bytes), ..]
                    if *op == keyword("scan") && attributes.field("scan_source") == Some(&source) => Some((attributes, bytes)),
                _ => None,
            }).ok_or("Compacted probe scan metrics are missing")?;
            if !matches!(attributes.field("scan_pages_used"), Some(Value::I64(pages)) if *pages > 0) {
                return Err("Compacted probe did not load any persisted pages".into());
            }
            let TransitValue::Array(filters) = decode_msgpack(pushdowns)? else { return Err("Expected probe filters".into()); };
            let [TransitValue::Map(entries)] = filters.as_slice() else { return Err("Expected one probe filter".into()); };
            let entry = |key: &str| entries.iter().find_map(|(name, value)| {
                matches!(name, TransitValue::Keyword(name) if name.as_str() == key).then_some(value)
            });
            if !matches!(entry("bloom-filter"), Some(TransitValue::Map(_))) {
                return Err("Compacted probe scan did not receive a Bloom filter".into());
            }
            match (count, entry("iids")) {
                (None, None) => {}
                (Some(count), Some(TransitValue::Map(iids)))
                    if matches!(iids.as_slice(),
                        [(TransitValue::Keyword(key), TransitValue::Integer(actual))]
                            if key.as_str() == "count" && *actual == count) => {}
                _ => return Err("Unexpected IID proof for compacted probe".into()),
            }
        }
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn joins_keep_equal_byte_keys_through_live_flush_and_compaction() {
        let directory = tempfile::tempdir().unwrap();
        let system_time = Timestamp(1_000_000);
        let clock = Arc::new(FixtureClock(AtomicI64::new(system_time.0)));
        let running = RunningNode::open(directory.path(), clock, Arc::new(Reads::default())).await.unwrap();
        let work = async {
            let node = &running.node;
            let database = node.catalog().database("xtdb")?;
            let compactor = database.compaction().ok_or("Join fixture compactor is missing")?;
            tokio::time::timeout(Duration::from_secs(60), compactor.compact_all()).await??;
            let prefix = put_join_rows(node, system_time).await?;
            check_layout(node, prefix, None).await?;
            check_join_results(node).await?;

            flush(node).await?;
            check_layout(node, prefix, Some(0)).await?;
            check_join_results(node).await?;

            tokio::time::timeout(Duration::from_secs(60), compactor.compact_all()).await??;
            check_layout(node, prefix, Some(1)).await?;
            check_join_results(node).await?;
            check_compacted_probe_filters(node).await
        }.await;
        combine(work, running.close().await).unwrap();
    }
}
