//! The reference's `CrashLogger`: a document write that fails for a reason other than the caller's leaves `crashes/<node-id>/<instant>/`
//! in the database's object store before the failure stops ingestion: `crash.edn`, the transaction's `tx-ops.arrow`, and the failed
//! table's `live-trie.binpb` and `live-table.arrow`.
//!
//! The reference also writes `open-tx-table.arrow` and `open-tx-trie.binpb`, the failed transaction's own staged rows for the table.
//! The resolver drops its transaction state on failure, so those are not written yet.

use std::fmt::Write as _;
use std::sync::Arc;

use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_index::{LiveIndex, LiveTable};
use rtdb_storage::{ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, AnomalyCategory, Instant, Result, TableRef, Timestamp, transit_value::{TransitValue, from_json_value}};

/// The context the resolver attaches to a failed put, delete or erase, which is what the reference's `withCrashLog` wraps.
const DATA_KEYS: [&str; 3] = ["table", "tx-key", "tx-op-idx"];

pub(crate) fn applies(error: &Anomaly) -> bool {
    !error.is_caller_error() && DATA_KEYS.iter().all(|key| error.context.contains_key(*key))
}

/// What the reference writes next to `crash.edn`.
pub(crate) struct Slices<'a> {
    /// The failed transaction's operations as they arrived on the source log (an Arrow IPC stream).
    pub tx_ops: Option<&'a Bytes>,
    /// The live index when the write failed.
    pub live: &'a LiveIndex,
}

/// A failure to write any part of the log is reported and swallowed, so that it never masks the error being logged.
/// `crash.edn` is written first; each other file is attempted even when an earlier one failed.
pub(crate) async fn write(store: &dyn ObjectStore, node_id: &str, now: Timestamp, error: &Anomaly, slices: Slices<'_>, memory: &Arc<MemoryBudget>) {
    let directory = match directory(node_id, now) {
        Ok(directory) => directory,
        Err(failure) => { tracing::warn!("crash log for {error} could not be named: {failure}"); return; }
    };
    tracing::warn!("writing crash log: {directory}");
    let mut complete = report(&directory, "crash.edn", put_edn(store, &directory, error, memory).await);
    if let Some(table) = context_table(error)
        && let Some(live) = slices.live.tables().get(&table)
    {
        complete &= report(&directory, "live-trie.binpb", put_live_trie(store, &directory, live, memory).await);
        if live.row_count() > 0 {
            complete &= report(&directory, "live-table.arrow", put_live_table(store, &directory, live, memory).await);
        }
    }
    if let Some(tx_ops) = slices.tx_ops {
        complete &= report(&directory, "tx-ops.arrow", put_tx_ops(store, &directory, tx_ops, memory).await);
    }
    if complete { tracing::warn!("crash log written: {directory}"); }
}

fn report(directory: &str, file: &str, outcome: Result<()>) -> bool {
    match outcome {
        Ok(()) => true,
        Err(failure) => { tracing::warn!("crash log {directory}/{file} could not be written: {failure}"); false }
    }
}

fn directory(node_id: &str, now: Timestamp) -> Result<String> {
    let seconds = Timestamp(now.0.div_euclid(1_000_000).saturating_mul(1_000_000));
    Ok(format!("crashes/{node_id}/{}", Instant::from(seconds).to_iso_string()?))
}

struct Payload { bytes: Vec<u8>, _reservation: Arc<MemoryReservation> }
impl AsRef<[u8]> for Payload { fn as_ref(&self) -> &[u8] { &self.bytes } }

async fn put(store: &dyn ObjectStore, directory: &str, file: &str, bytes: Vec<u8>, memory: &Arc<MemoryBudget>) -> Result<()> {
    let key = ObjectKey::new(format!("{directory}/{file}"))?;
    let reservation = memory.reserve(bytes.len().saturating_add(4096))?;
    store.put(&key, Bytes::from_owner(Payload { bytes, _reservation: reservation })).await?;
    Ok(())
}

async fn put_edn(store: &dyn ObjectStore, directory: &str, error: &Anomaly, memory: &Arc<MemoryBudget>) -> Result<()> {
    let _scratch = memory.reserve(4096)?;
    put(store, directory, "crash.edn", crash_edn(error)?.into_bytes(), memory).await
}

/// The reference's `trie.asProto`: the table's in-memory trie with its logs compacted.
async fn put_live_trie(store: &dyn ObjectStore, directory: &str, live: &LiveTable, memory: &Arc<MemoryBudget>) -> Result<()> {
    let _scratch = memory.reserve(usize::try_from(live.row_count()).unwrap_or(usize::MAX).saturating_mul(8).saturating_add(4096))?;
    let encoded = rtdb_format::memory_trie::encode(&live.trie().persisted(live)?)?;
    put(store, directory, "live-trie.binpb", encoded, memory).await
}

/// The table's live rows in the order they were written, in the layout of a block's data file.
async fn put_live_table(store: &dyn ObjectStore, directory: &str, live: &LiveTable, memory: &Arc<MemoryBudget>) -> Result<()> {
    let encoded = {
        let pages = live.block_pages(memory)?;
        let rows: Vec<u32> = (0..live.row_count()).collect();
        let page = pages.copy(&rows)?;
        let _scratch = memory.reserve(page.batch().get_array_memory_size().saturating_add(65_536))?;
        rtdb_format::ipc::write_file(page.batch().schema_ref(), std::slice::from_ref(page.batch()))?
    };
    put(store, directory, "live-table.arrow", encoded, memory).await
}

/// The reference writes the transaction's `tx-ops` vector; a legacy envelope stream also carries the submit options, which are dropped.
async fn put_tx_ops(store: &dyn ObjectStore, directory: &str, stream: &Bytes, memory: &Arc<MemoryBudget>) -> Result<()> {
    let encoded = {
        let _decoded = memory.reserve(rtdb_format::ipc::stream_allocation_bound(stream)?.saturating_mul(2))?;
        let (schema, pages) = rtdb_format::ipc::read_stream(Arc::from(stream.as_ref()))?;
        let projection = |failure: &dyn std::fmt::Display| Anomaly::fault("rtdb.crash-log/tx-ops", failure.to_string());
        match schema.index_of("tx-ops") {
            Ok(column) if schema.fields().len() > 1 => {
                let pages = pages.iter().map(|page| page.project(&[column]).map_err(|failure| projection(&failure)))
                    .collect::<Result<Vec<_>>>()?;
                rtdb_format::ipc::write_file(&schema.project(&[column]).map_err(|failure| projection(&failure))?, &pages)?
            }
            _ => rtdb_format::ipc::write_file(&schema, &pages)?,
        }
    };
    put(store, directory, "tx-ops.arrow", encoded, memory).await
}

/// The failed table, from the `#xt/table` the resolver attached to the error.
fn context_table(error: &Anomaly) -> Option<TableRef> {
    let TransitValue::Tagged { tag, representation } = from_json_value(error.context.get("table")?).ok()? else { return None; };
    let TransitValue::Map(entries) = *representation else { return None; };
    if tag != "xt/table" { return None; }
    let field = |name: &str| entries.iter().find_map(|(key, value)| match (key, value) {
        (TransitValue::Keyword(key), TransitValue::String(value)) if key == name => Some(value.clone()),
        _ => None,
    });
    Some(TableRef::new(field("schema-name").unwrap_or_else(|| "public".into()), field("table-name")?))
}

/// The reference's map: the wrapped operation's data, its message, then the anomaly as `#xt/error`.
/// The anomaly is printed on one line, where the reference pretty-prints it.
fn crash_edn(error: &Anomaly) -> Result<String> {
    let mut out = String::from("{");
    for key in DATA_KEYS {
        write_keyword(&mut out, key);
        out.push(' ');
        write_value(&mut out, &from_json_value(&error.context[key])?);
        out.push_str(", ");
    }
    out.push_str(":msg ");
    write_string(&mut out, "error indexing documents");
    out.push_str(", :ex #xt/error [");
    write_keyword(&mut out, category(error.category));
    out.push(' ');
    write_keyword(&mut out, &error.code);
    out.push(' ');
    match &error.message { Some(message) => write_string(&mut out, message), None => out.push_str("nil") }
    out.push_str(" {");
    for (index, (key, value)) in error.context.iter().enumerate() {
        if index > 0 { out.push_str(", "); }
        write_keyword(&mut out, key);
        out.push(' ');
        write_value(&mut out, &from_json_value(value)?);
    }
    out.push_str("}]}\n");
    Ok(out)
}

fn category(category: AnomalyCategory) -> &'static str {
    match category {
        AnomalyCategory::Incorrect => "incorrect", AnomalyCategory::Unsupported => "unsupported", AnomalyCategory::NotFound => "not-found",
        AnomalyCategory::Forbidden => "forbidden", AnomalyCategory::Conflict => "conflict", AnomalyCategory::Busy => "busy",
        AnomalyCategory::Unavailable => "unavailable", AnomalyCategory::Interrupted => "interrupted", AnomalyCategory::Fault => "fault",
    }
}

fn write_keyword(out: &mut String, name: &str) {
    out.push(':');
    out.push_str(name);
}

fn write_string(out: &mut String, text: &str) {
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""), '\\' => out.push_str("\\\\"), '\n' => out.push_str("\\n"), '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"), '\u{c}' => out.push_str("\\f"), '\u{8}' => out.push_str("\\b"),
            character => out.push(character),
        }
    }
    out.push('"');
}

fn write_float(out: &mut String, value: f64) {
    if value.is_nan() { out.push_str("##NaN") }
    else if value.is_infinite() { out.push_str(if value > 0.0 { "##Inf" } else { "##-Inf" }) }
    else { let _ = write!(out, "{value:?}"); }
}

fn write_value(out: &mut String, value: &TransitValue) {
    match value {
        TransitValue::Null => out.push_str("nil"),
        TransitValue::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
        TransitValue::Integer(value) => { let _ = write!(out, "{value}"); }
        TransitValue::Float(value) => write_float(out, *value),
        TransitValue::Float32(value) => write_float(out, f64::from(*value)),
        TransitValue::String(value) => write_string(out, value),
        TransitValue::Keyword(value) => write_keyword(out, value),
        TransitValue::Symbol(value) => out.push_str(value),
        TransitValue::Bytes(value) => {
            out.push_str("#bytes \"");
            for byte in value { let _ = write!(out, "{byte:02x}"); }
            out.push('"');
        }
        TransitValue::Array(values) => {
            out.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 { out.push(' '); }
                write_value(out, value);
            }
            out.push(']');
        }
        TransitValue::Map(entries) => {
            out.push('{');
            for (index, (key, value)) in entries.iter().enumerate() {
                if index > 0 { out.push_str(", "); }
                write_value(out, key);
                out.push(' ');
                write_value(out, value);
            }
            out.push('}');
        }
        TransitValue::Tagged { tag, representation } => write_tagged(out, tag, representation),
    }
}

fn write_tagged(out: &mut String, tag: &str, representation: &TransitValue) {
    match (tag, representation) {
        ("xt/table", TransitValue::Map(entries)) => {
            let field = |name: &str| entries.iter().find_map(|(key, value)| match (key, value) {
                (TransitValue::Keyword(key), TransitValue::String(value)) if key == name => Some(value.as_str()),
                _ => None,
            });
            out.push_str("#xt/table ");
            match (field("schema-name"), field("table-name")) {
                (Some("public") | None, Some(table)) => out.push_str(table),
                (Some(schema), Some(table)) => { out.push_str(schema); out.push('/'); out.push_str(table); }
                _ => write_value(out, representation),
            }
        }
        _ => {
            let tag = match tag { "time/instant" => "xt/instant", "xtdb/tx-key" => "xt/tx-key", tag => tag };
            out.push('#');
            out.push_str(tag);
            out.push(' ');
            write_value(out, representation);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_storage::MemoryStore;
    use rtdb_types::{TableRef, transit_value::to_json_value};

    fn failed_put(category: AnomalyCategory) -> Anomaly {
        let tx_key = TransitValue::Tagged { tag: "xtdb/tx-key".into(), representation: Box::new(TransitValue::Map(vec![
            (TransitValue::Keyword("tx-id".into()), TransitValue::Integer(1)),
            (TransitValue::Keyword("system-time".into()), TransitValue::instant(Timestamp(0).into()).unwrap()),
        ])) };
        Anomaly::new(category, "rtdb.live/append", "Live append failed")
            .with_context("table", to_json_value(&TransitValue::table_ref(&TableRef::new("public", "foo"))).unwrap())
            .with_context("tx-key", to_json_value(&tx_key).unwrap())
            .with_context("tx-op-idx", 0.into())
    }

    #[test]
    fn only_document_writes_that_fail_for_reasons_other_than_the_callers_are_logged() {
        assert!(applies(&failed_put(AnomalyCategory::Fault)));
        assert!(applies(&failed_put(AnomalyCategory::Interrupted)));
        assert!(!applies(&failed_put(AnomalyCategory::Incorrect)));
        assert!(!applies(&Anomaly::fault("rtdb.live/append", "Live append failed")));
    }

    #[tokio::test]
    async fn a_failed_document_write_leaves_crash_edn_under_the_node_and_the_second_it_failed() {
        let store = MemoryStore::default();
        let memory = MemoryBudget::new(1 << 20);
        let live = LiveIndex::new(rtdb_types::BlockIndex(0), None, rtdb_trie::TrieConfig::default());
        write(&store, "xtdb-foo-node", Timestamp(1_500_000), &failed_put(AnomalyCategory::Fault), Slices { tx_ops: None, live: &live }, &memory).await;
        assert!(store.get(&ObjectKey::new("crashes/xtdb-foo-node/1970-01-01T00:00:01Z/live-trie.binpb").unwrap()).await.is_err(),
            "a table the live index does not hold has no live trie to write");
        let edn = store.get(&ObjectKey::new("crashes/xtdb-foo-node/1970-01-01T00:00:01Z/crash.edn").unwrap()).await.unwrap();
        let tx_key = "#xt/tx-key {:tx-id 1, :system-time #xt/instant \"1970-01-01T00:00:00Z\"}";
        assert_eq!(std::str::from_utf8(&edn).unwrap(), format!(
            "{{:table #xt/table foo, :tx-key {tx_key}, :tx-op-idx 0, :msg \"error indexing documents\", \
             :ex #xt/error [:fault :rtdb.live/append \"Live append failed\" {{:table #xt/table foo, :tx-key {tx_key}, :tx-op-idx 0}}]}}\n"));
    }

    #[tokio::test]
    async fn the_crash_log_holds_the_transactions_operations_and_the_failed_tables_live_rows_and_trie() {
        use rtdb_format::data::{StoredEvent, StoredOperation};
        use rtdb_index::{IndexBatch, StagedWrite, TableWrite};
        use rtdb_types::{BlockIndex, EntityId, Interval, StructFields, TransactionKey, TxId, Value};

        let store = MemoryStore::default();
        let memory = MemoryBudget::new(1 << 24);
        let table = TableRef::new("public", "foo");
        let mut live = LiveIndex::new(BlockIndex(0), None, rtdb_trie::TrieConfig::default());
        for id in 0..2i64 {
            let event = StoredEvent { position: None, origin: None, commit_position: 0, entity_key: None, iid: EntityId::Integer(id).iid(),
                system_from: Timestamp(id), valid_time: Interval::ALL, operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(id))])), recency: None };
            let write = TableWrite::new(StructFields::new(), Some(IndexBatch::copy_events(&[event], &StructFields::new(), &memory).unwrap()));
            let key = TransactionKey { tx_id: TxId(id), system_time: Timestamp(id) };
            let staged = StagedWrite::new(key, live.latest_completed_tx(), live.block_index(), std::collections::BTreeMap::from([(table.clone(), write)])).unwrap();
            live = live.apply(&staged).unwrap();
        }
        let operations = arrow_array::RecordBatch::try_from_iter([
            ("tx-ops", Arc::new(arrow_array::Int64Array::from(vec![1, 2, 3])) as arrow_array::ArrayRef),
            ("default-tz", Arc::new(arrow_array::StringArray::from(vec!["UTC", "UTC", "UTC"])) as arrow_array::ArrayRef),
        ]).unwrap();
        let stream = Bytes::from(rtdb_format::ipc::write_stream(operations.schema_ref(), std::slice::from_ref(&operations)).unwrap());

        write(&store, "node", Timestamp(1_500_000), &failed_put(AnomalyCategory::Fault), Slices { tx_ops: Some(&stream), live: &live }, &memory).await;

        let file = |name: &str| ObjectKey::new(format!("crashes/node/1970-01-01T00:00:01Z/{name}")).unwrap();
        assert!(store.get(&file("crash.edn")).await.is_ok());
        let arrow = |bytes: Bytes| rtdb_format::ipc::ArrowFile::open(Arc::from(bytes.as_ref()), None).unwrap();
        let mut tx_ops = arrow(store.get(&file("tx-ops.arrow")).await.unwrap());
        assert_eq!(tx_ops.schema().fields().iter().map(|field| field.name().as_str()).collect::<Vec<_>>(), ["tx-ops"]);
        assert_eq!(tx_ops.read_page(0).unwrap().num_rows(), 3);
        let mut rows = arrow(store.get(&file("live-table.arrow")).await.unwrap());
        assert_eq!(rows.read_page(0).unwrap().num_rows(), 2);
        let trie = rtdb_format::memory_trie::decode(&store.get(&file("live-trie.binpb")).await.unwrap()).unwrap();
        assert_eq!(trie, live.tables()[&table].trie().persisted(live.tables()[&table].as_ref()).unwrap());
    }
}
