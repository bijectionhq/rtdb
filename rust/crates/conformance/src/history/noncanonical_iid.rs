use std::{path::Path, sync::{Arc, atomic::AtomicI64}, time::Duration};

use rtdb_columnar::{MemoryBudget, read_value};
use rtdb_format::{envelope::{SourceBody, SourceMessage}, ipc::read_stream, transit::{TransitValue, decode_msgpack}};
use rtdb_node::{NativeResources, Node};
use rtdb_query::PreparedQuery;
use rtdb_storage::log::{LocalLog, LocalLogOptions, Log};
use rtdb_tx::{Documents, SourceTransaction, SubmitOptions, TxOp};
use rtdb_types::{BlockIndex, SourcePosition, TableRef, Timestamp, TransactionKey, TxId, Value};
use tokio_util::sync::CancellationToken;

use rtdb_types::StructFields;
use super::{Error, FixtureClock, Identity, Reads, RunningNode, combine, flush};

const FIRST_TIME: Timestamp = Timestamp(1_000_000);
const SECOND_TIME: Timestamp = Timestamp(2_000_000);
const QUERY_TIME: Timestamp = Timestamp(3_000_000);
const HISTORICAL: &str = "FOR SYSTEM_TIME AS OF TIMESTAMP '1970-01-01T00:00:01Z'";
const TARGET: [u8; 16] = *b"replay-target-id";
const REPLACED: [u8; 16] = *b"replaced-doc-iid";
const SURVIVOR: [u8; 16] = *b"survivor-doc-iid";
const OTHER: [u8; 16] = *b"unmatched-doc-id";
const REPLAY: &str = "replay_iid_docs";
const CANONICAL: &str = "replay_iid_canonical";
const KEYS: &str = "replay_iid_keys";

fn operation_values(stream: &[u8]) -> Result<Vec<Value>, Error> {
    let (schema, pages) = read_stream(Arc::from(stream))?;
    let [page] = pages.as_slice() else { return Err("Expected one source operation batch".into()); };
    let index = schema.index_of("tx-ops")?;
    (0..page.num_rows()).map(|row| {
        Ok(read_value(schema.field(index), page.column(index).as_ref(), row)?)
    }).collect()
}

fn replace_first_put_iids(message: &mut SourceMessage, iids: &[[u8; 16]]) -> Result<(), Error> {
    let SourceMessage::Protobuf(SourceBody::Tx(tx)) = message else { return Err("Expected a protobuf source transaction".into()); };
    let stream = tx.tx_ops.as_ref().ok_or("Source transaction is missing its operation stream")?;
    let mut expected = operation_values(stream)?;
    let Some(Value::Struct(fields)) = expected.first_mut() else { return Err("Expected a source put operation".into()); };
    let Some(Value::List(previous)) = fields.get("iids") else { return Err("Source put did not supply IIDs".into()); };
    if previous.len() != iids.len() { return Err("Fixture IID count differs from the source put".into()); }
    let mut canonical_bytes = Vec::new();
    for value in previous {
        let Value::FixedSizeBinary(bytes) = value else { return Err("Expected a fixed-width source IID".into()); };
        canonical_bytes.extend_from_slice(bytes);
    }
    let replacement = iids.concat();
    if canonical_bytes.is_empty() || canonical_bytes == replacement {
        return Err("Fixture must replace an IID with a different value".into());
    }
    fields.insert("iids".into(), Value::List(iids.iter().map(|iid| Value::FixedSizeBinary(iid.to_vec())).collect()));

    let mut selected = None;
    for (offset, bytes) in stream.windows(canonical_bytes.len()).enumerate() {
        if bytes != canonical_bytes.as_slice() { continue; }
        let mut candidate = stream.clone();
        candidate[offset..offset + replacement.len()].copy_from_slice(&replacement);
        if operation_values(&candidate).is_ok_and(|actual| actual == expected) {
            if selected.replace(candidate).is_some() { return Err("More than one buffer matched the IID replacement".into()); }
        }
    }
    tx.tx_ops = Some(selected.ok_or("No buffer matched the IID replacement without changing other cells")?);
    Ok(())
}

fn put(memory: &Arc<MemoryBudget>, table: &str, rows: &[([u8; 16], &str)]) -> Result<TxOp, Error> {
    let documents = rows.iter().map(|(id, label)| StructFields::from([
        ("_id".into(), Value::Uuid(*id)),
        ("label".into(), Value::Utf8((*label).into())),
    ])).collect();
    Ok(TxOp::PutDocs {
        table: TableRef::new("public", table),
        documents: Documents::new(documents, memory)?,
        valid_from: Some(Timestamp(0)),
        valid_to: None,
    })
}

fn encode_source(memory: &Arc<MemoryBudget>, operations: Vec<TxOp>, time: Timestamp, replay_iids: Option<&[[u8; 16]]>) -> Result<bytes::Bytes, Error> {
    let transaction = SourceTransaction::new(operations, SubmitOptions { system_time: Some(time), ..Default::default() }, memory)?;
    let encoded = transaction.encode()?;
    let mut message = SourceMessage::decode(encoded.as_slice())?.ok_or("Expected a source transaction")?;
    if let Some(iids) = replay_iids {
        replace_first_put_iids(&mut message, iids)?;
    }
    SourceTransaction::decode(&message, memory)?;
    Ok(message.encode()?.into())
}

async fn seed_source_log(directory: &Path) -> Result<SourcePosition, Error> {
    let memory = MemoryBudget::new(1 << 25);
    let first = encode_source(&memory, vec![
        put(&memory, REPLAY, &[(TARGET, "original"), (TARGET, "survivor"), (OTHER, "unmatched")])?,
        put(&memory, CANONICAL, &[(TARGET, "canonical"), (OTHER, "unmatched")])?,
        put(&memory, KEYS, &[(REPLACED, "key")])?,
    ], FIRST_TIME, Some(&[REPLACED, SURVIVOR, OTHER]))?;
    let second = encode_source(&memory, vec![put(&memory, REPLAY, &[(REPLACED, "replacement")])?], SECOND_TIME, None)?;
    let (log, driver) = LocalLog::open(directory.join("log/LOG"), LocalLogOptions {
        maximum_message_bytes: 1 << 20,
        queue_capacity: 2,
    })?;
    let shutdown = CancellationToken::new();
    let writer = tokio::spawn(driver.run(shutdown.clone()));
    let work = async {
        log.enqueue(first, FIRST_TIME).await?.durable().await?;
        let last = log.enqueue(second, SECOND_TIME).await?.durable().await?;
        Ok(SourcePosition(last.offset.0))
    }.await;
    shutdown.cancel();
    let closed = writer.await.map_err(Error::from).and_then(|result| result.map_err(Error::from));
    log.close_and_wait().await;
    combine(work, closed)
}

async fn query_rows(node: &Node, sql: &str, build_rows: i64) -> Result<Vec<Vec<Value>>, Error> {
    let mut session = node.session("xtdb", Arc::new(Identity))?;
    let mut context = session.query_context().await?;
    Arc::get_mut(&mut context).ok_or("Fixture query context is shared")?.options.join_build_rows = build_rows;
    let plan = rtdb_sql::prepare_in(sql, &context.catalog, &Default::default(), &context.memory)?;
    let prepared = PreparedQuery::from_prepared(plan, &context.memory)?;
    let mut cursor = prepared.execute(Arc::clone(&context), vec![]).await?;
    let mut rows = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() {
            rows.push(batch.batch().schema().fields().iter().zip(batch.batch().columns())
                .map(|(field, array)| read_value(field, array.as_ref(), row)).collect::<rtdb_types::Result<_>>()?);
        }
    }
    drop(cursor);
    if (context.spill_peak_bytes() > 0) != (build_rows == 0) || context.spill_bytes() != 0 {
        return Err(format!("Unexpected spill state for {sql} at build threshold {build_rows}").into());
    }
    Ok(rows)
}

fn logical_join(table: &str, period: &str) -> String {
    format!("SELECT r._id, r._iid, r.label
        FROM (VALUES (UUID '{}')) AS l(build_key) JOIN {table} {period} r ON l.build_key = r._id",
        uuid::Uuid::from_bytes(TARGET))
}

fn raw_join(period: &str) -> String {
    format!("SELECT r._id, r._iid, r.label FROM {KEYS} l JOIN {REPLAY} {period} r ON l._iid = r._iid")
}

fn row(id: [u8; 16], iid: [u8; 16], label: &str) -> Vec<Value> {
    vec![Value::Uuid(id), Value::FixedSizeBinary(iid.to_vec()), Value::Utf8(label.into())]
}

async fn check_join_rows(node: &Node) -> Result<(), Error> {
    for (sql, expected) in [
        (logical_join(REPLAY, ""), vec![row(TARGET, SURVIVOR, "survivor")]),
        (logical_join(REPLAY, HISTORICAL), vec![row(TARGET, REPLACED, "original"), row(TARGET, SURVIVOR, "survivor")]),
        (raw_join(""), vec![row(REPLACED, REPLACED, "replacement")]),
        (raw_join(HISTORICAL), vec![row(TARGET, REPLACED, "original")]),
    ] {
        for build_rows in [i64::MAX, 0] {
            let actual = query_rows(node, &sql, build_rows).await?;
            if actual.len() != expected.len() || !expected.iter().all(|row| actual.contains(row)) {
                return Err(format!("Unexpected rows for {sql} at build threshold {build_rows}: {actual:?}").into());
            }
        }
    }
    Ok(())
}

fn transit_field<'a>(value: &'a TransitValue, key: &str) -> Option<&'a TransitValue> {
    let TransitValue::Map(fields) = value else { return None; };
    fields.iter().find_map(|(name, value)| {
        matches!(name, TransitValue::Keyword(name) if name == key).then_some(value)
    })
}

async fn check_pushdown(node: &Node, sql: &str, table: &str, column: &str, expected_iids: Option<i64>) -> Result<(), Error> {
    let rows = query_rows(node, &format!("EXPLAIN ANALYZE {sql}"), i64::MAX).await?;
    let source = Value::Utf8(format!("public.{table}"));
    let bytes = rows.iter().find_map(|row| match row.as_slice() {
        [_, Value::Keyword(op), attributes, _, _, _, _, _, Value::Transit(bytes), ..]
            if op.as_str() == "scan" && attributes.field("scan_source") == Some(&source) => Some(bytes),
        _ => None,
    }).ok_or("Expected pushdowns for the probe scan")?;
    let TransitValue::Array(filters) = decode_msgpack(bytes)? else { return Err("Expected probe filters".into()); };
    let [filter] = filters.as_slice() else { return Err("Expected one probe filter".into()); };
    let bloom = transit_field(filter, "bloom-filter").and_then(|value| transit_field(value, "cardinality"));
    let iids = transit_field(filter, "iids");
    if transit_field(filter, "column") != Some(&TransitValue::String(column.into()))
        || !matches!(bloom, Some(TransitValue::Integer(count)) if *count > 0)
        || match expected_iids {
            Some(count) => iids.and_then(|value| transit_field(value, "count")) != Some(&TransitValue::Integer(count)),
            None => iids.is_some(),
        }
    {
        return Err(format!("Unexpected probe filters for {sql}: {filter:?}").into());
    }
    Ok(())
}

async fn check_layout(node: &Node, tx: TransactionKey, adopted: Option<BlockIndex>) -> Result<(), Error> {
    let mut session = node.session("xtdb", Arc::new(Identity))?;
    let context = session.query_context().await?;
    let partition = &context.catalog.database("xtdb")?.partitions[0];
    if partition.tx_basis() != Some(tx) { return Err("Fixture query changed its transaction basis".into()); }
    for name in [REPLAY, CANONICAL, KEYS] {
        let table = TableRef::new("public", name);
        let live = !partition.segments(&table).is_empty();
        let tries = partition.trie_entries(&table);
        let expected = match adopted {
            None => live && tries.is_empty(),
            Some(index) => !live && tries.len() == 1
                && tries[0].key.level() == 0 && tries[0].key.block_index() == index,
        };
        if !expected { return Err(format!("Unexpected live or adopted L0 layout for {name}").into()); }
    }
    check_join_rows(node).await?;
    check_pushdown(node, &logical_join(REPLAY, ""), REPLAY, "_id", None).await?;
    check_pushdown(node, &raw_join(""), REPLAY, "_iid", Some(1)).await?;
    check_pushdown(node, &logical_join(CANONICAL, ""), CANONICAL, "_id", adopted.is_none().then_some(1)).await
}

async fn await_restart(node: &Node) -> Result<(), Error> {
    let database = node.catalog().database("xtdb")?;
    let target = database.startup_replica_target()?.ok_or("Missing durable replica target on restart")?;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if database.status().replica_position >= target { return Ok(()); }
            if let Some(error) = database.progress().fault() { return Err(error); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await??;
    node.sync_all(Duration::from_secs(30), node.cancellation()).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn joins_on_replayed_iids_keep_visible_rows_through_l0_and_restart() -> Result<(), Error> {
    let directory = tempfile::tempdir()?;
    let source = seed_source_log(directory.path()).await?;
    let tx = TransactionKey { tx_id: TxId(source.0), system_time: SECOND_TIME };
    let clock = Arc::new(FixtureClock(AtomicI64::new(QUERY_TIME.0)));
    let running = RunningNode::open(directory.path(), Arc::clone(&clock), Arc::new(Reads::default())).await?;
    let work = async {
        let node = &running.node;
        node.sync_all(Duration::from_secs(30), node.cancellation()).await?;
        let database = node.catalog().database("xtdb")?;
        let progress = database.progress().current();
        if progress.source_position != source
            || !progress.latest_result.is_some_and(|completed| completed.result().committed() && completed.result().tx_key() == tx)
        {
            return Err("Fixture source transactions did not commit through the local log".into());
        }
        let compactor = database.compaction().ok_or("Fixture compactor is missing")?;
        tokio::time::timeout(Duration::from_secs(60), compactor.compact_all()).await??;
        check_layout(node, tx, None).await?;
        flush(node).await?;
        let block = database.status().adopted_block.clone().ok_or("Fixture block was not adopted")?;
        if block.latest_completed_tx() != Some(tx) || block.latest_source_position().is_none_or(|position| position < source) {
            return Err("Adopted fixture block does not cover the replayed source".into());
        }
        check_layout(node, tx, Some(block.block_index())).await?;
        Ok(block)
    }.await;
    let block = combine(work, running.close().await)?;

    let resources = Arc::new(NativeResources::default().with_local_log_clock(clock.clone(), false));
    let restarted = RunningNode::open_with_resources(directory.path(), clock, Arc::new(Reads::default()), resources, true).await?;
    let work = async {
        let node = &restarted.node;
        await_restart(node).await?;
        let database = node.catalog().database("xtdb")?;
        let recovered = database.status().adopted_block.clone().ok_or("Reopened fixture has no adopted block")?;
        if recovered.block_index() != block.block_index()
            || recovered.latest_completed_tx() != block.latest_completed_tx()
            || recovered.latest_source_position() != block.latest_source_position()
            || recovered.boundary_replica_position() != block.boundary_replica_position()
        {
            return Err("Reopened fixture has another persisted basis".into());
        }
        check_layout(node, tx, Some(block.block_index())).await
    }.await;
    combine(work, restarted.close().await)
}
