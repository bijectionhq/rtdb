use std::{fs::File, io::{Read, Write}, path::Path, sync::Arc};
use arrow_array::RecordBatch;
use rtdb_columnar::{MemoryBudget, estimate_read_value_bytes, read_value};
use rtdb_format::{catalog::{BlockCatalog, TableBlock}, ipc::{self, ArrowFile, ArrowStream}};
use rtdb_types::{Anomaly, Result};
use serde_json::{Value, json};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileKind { ArrowFile, ArrowStream, HashTrie, TableBlock, Block }

/// Writes one JSON document containing physical schemas, typed cells or catalog fields.
/// The caller keeps the input immutable; decoding and retained row values use `memory`.
/// Output is written incrementally and may be incomplete if decoding or the output sink fails.
pub fn file(kind: FileKind, path: &Path, memory: &Arc<MemoryBudget>, output: &mut impl Write) -> Result<()> {
    let input = File::open(path).map_err(invalid)?;
    let size = usize::try_from(input.metadata().map_err(invalid)?.len()).map_err(invalid)?;
    let _encoded = memory.reserve(bound(size, 2, 8192)?)?;
    let mut bytes = Vec::with_capacity(size);
    input.take(u64::try_from(size).map_err(invalid)?.saturating_add(1)).read_to_end(&mut bytes).map_err(invalid)?;
    if bytes.len() != size { return Err(invalid("Inspection input changed while being read")); }
    match kind {
        FileKind::ArrowFile => {
            let _decoded = memory.reserve(ipc::file_allocation_bound(&bytes)?)?;
            let mut reader = ArrowFile::open(bytes.into(), None)?;
            output.write_all(b"{\"schema\":").map_err(invalid)?;
            write_json(output, &schema_json(reader.schema().as_ref())?)?;
            output.write_all(b",\"batches\":[").map_err(invalid)?;
            for page in 0..reader.page_count() {
                if page > 0 { output.write_all(b",").map_err(invalid)?; }
                batch(output, &reader.read_page(page)?, memory)?;
            }
            output.write_all(b"]}\n").map_err(invalid)
        }
        FileKind::ArrowStream => {
            let _decoded = memory.reserve(ipc::stream_allocation_bound(&bytes)?)?;
            let mut reader = ArrowStream::open(bytes.into(), None)?;
            output.write_all(b"{\"schema\":").map_err(invalid)?;
            write_json(output, &schema_json(reader.schema().as_ref())?)?;
            output.write_all(b",\"batch\":").map_err(invalid)?;
            match reader.next().transpose()? { Some(page) => batch(output, &page, memory)?, None => output.write_all(b"[]").map_err(invalid)? }
            output.write_all(b"}\n").map_err(invalid)
        }
        FileKind::HashTrie => {
            let _decoded = memory.reserve(bound(size, 128, 65_536)?)?;
            let trie = rtdb_format::memory_trie::decode(&bytes)?;
            write_json(output, &json!({ "log-limit": trie.log_limit, "page-limit": trie.page_limit, "root-path": trie.root_path, "tree": trie_node(&trie.root) }))?;
            output.write_all(b"\n").map_err(invalid)
        }
        FileKind::TableBlock => {
            let reservation = memory.reserve(bound(size, 256, 65_536)?)?;
            let table = TableBlock::decode_reserved(&bytes, reservation)?;
            write_json(output, &table_block(&table)?)?;
            output.write_all(b"\n").map_err(invalid)
        }
        FileKind::Block => {
            let reservation = memory.reserve(bound(size, 128, 65_536)?)?;
            let block = BlockCatalog::decode_reserved(&bytes, reservation)?;
            write_json(output, &block_catalog(&block))?;
            output.write_all(b"\n").map_err(invalid)
        }
    }
}

fn schema_json(schema: &arrow_schema::Schema) -> Result<Value> {
    fn patch_type(value: &mut Value, index: usize, parameter: i32) -> Result<()> {
        let object = value.as_object_mut().ok_or_else(|| invalid("Invalid Arrow datatype JSON"))?;
        for name in ["Decimal32", "Decimal64", "Decimal128", "Decimal256"] {
            if let Some(Value::Array(parameters)) = object.get_mut(name) { parameters[index] = Value::from(parameter); return Ok(()); }
        }
        if let Some(Value::Array(parameters)) = object.get_mut("Dictionary") { return patch_type(&mut parameters[1], index, parameter); }
        Err(invalid("Decimal annotation requires decimal JSON storage"))
    }
    fn patch(value: &mut Value) -> Result<()> {
        match value {
            Value::Array(values) => for value in values { patch(value)?; },
            Value::Object(object) => {
                for (index, key) in [(0, rtdb_columnar::INTERNAL_DECIMAL_PRECISION_KEY), (1, rtdb_columnar::INTERNAL_DECIMAL_SCALE_KEY)] {
                    let parameter = object.get_mut("metadata").and_then(Value::as_object_mut).and_then(|metadata| metadata.remove(key));
                    if let Some(parameter) = parameter {
                        let parameter = parameter.as_str().ok_or_else(|| invalid("Invalid decimal parameter JSON"))?.parse::<i32>().map_err(invalid)?;
                        patch_type(object.get_mut("data_type").ok_or_else(|| invalid("Missing decimal JSON datatype"))?, index, parameter)?;
                    }
                }
                for value in object.values_mut() { patch(value)?; }
            }
            _ => {}
        }
        Ok(())
    }
    let mut json = serde_json::to_value(schema).map_err(invalid)?;
    patch(&mut json)?;
    Ok(json)
}

fn batch(output: &mut impl Write, batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<()> {
    output.write_all(b"[").map_err(invalid)?;
    let schema = batch.schema();
    for row in 0..batch.num_rows() {
        if row > 0 { output.write_all(b",").map_err(invalid)?; }
        let size = schema.fields().iter().enumerate().try_fold(8192usize, |size, (column, field)| size.checked_add(estimate_read_value_bytes(field, batch.column(column).as_ref(), row)?).ok_or_else(|| invalid("Row allocation exceeds addressable memory")))?;
        let _reservation = memory.reserve(size)?;
        let values = schema.fields().iter().enumerate().map(|(column, field)| read_value(field, batch.column(column).as_ref(), row)).collect::<Result<Vec<_>>>()?;
        write_json(output, &values)?;
    }
    output.write_all(b"]").map_err(invalid)
}

fn trie_node(node: &rtdb_trie::PersistedNode) -> Value {
    match node {
        rtdb_trie::PersistedNode::Leaf(rows) => json!(["leaf", rows]),
        rtdb_trie::PersistedNode::Branch(children) => {
            let mut values = vec![Value::String("branch".into())];
            values.extend(children.iter().map(|node| node.as_ref().map(trie_node).unwrap_or(Value::Null)));
            Value::Array(values)
        }
    }
}
fn block_catalog(block: &BlockCatalog) -> Value {
    let block = block.as_proto();
    json!({
        "block-idx": block.block_index,
        "latest-completed-tx": block.latest_completed_tx.as_ref().map(|key| json!({"tx-id": key.tx_id, "system-time-micros": key.system_time})),
        "latest-processed-msg-id": block.latest_processed_msg_id,
        "table-names": block.table_names,
        "tables": block.tables.iter().map(|table| json!({"oid": table.oid, "schema-name": table.schema_name, "table-name": table.table_name, "slug": table.slug})).collect::<Vec<_>>(),
        "secondary-dbs": block.secondary_databases.iter().map(|(name, config)| (name.clone(), database(config))).collect::<serde_json::Map<_, _>>(),
        "boundary-replica-msg-id": block.boundary_replica_msg_id, "boundary-term-seq": block.boundary_term_seq,
        "term-id": block.term_id, "external-source-token": block.external_source_token,
    })
}
fn database(config: &rtdb_format::database::proto::DatabaseConfig) -> Value {
    use rtdb_format::database::proto::database_config::{Log, Storage};
    let log = config.log.as_ref().map(|log| match log {
        Log::InMemoryLog(log) => json!({"type": "memory", "epoch": log.epoch, "replay-buffer-size": log.replay_buffer_size}),
        Log::LocalLog(log) => json!({"type": "local", "epoch": log.epoch, "path": log.path}),
        Log::OtherLog(log) => json!({"type": log.type_url, "protobuf": log.value}),
    });
    let storage = config.storage.as_ref().map(|storage| match storage {
        Storage::InMemoryStorage(storage) => json!({"type": "memory", "epoch": storage.epoch}),
        Storage::LocalStorage(storage) => json!({"type": "local", "epoch": storage.epoch, "path": storage.path}),
        Storage::RemoteStorage(storage) => json!({"type": "remote", "epoch": storage.epoch, "object-store": storage.object_store.as_ref().map(|store| json!({"type": store.type_url, "protobuf": store.value}))}),
    });
    json!({"log": log, "storage": storage, "mode": config.mode, "critical": config.critical, "partitions": config.partitions,
        "external-source": config.external_source.as_ref().map(|source| json!({"type": source.type_url, "protobuf": source.value}))})
}
fn table_block(table: &TableBlock) -> Result<Value> {
    let wire = table.as_proto();
    let fields = table.column_types()?;
    let hlls = table.column_hlls().into_iter().map(|(name, bytes)| {
        let hll = rtdb_format::hll::Hll::from_bytes(bytes.to_vec())?;
        Ok((name, json!({"register-bytes": bytes, "estimate": hll.estimate(), "estimate-bits": hll.estimate().to_bits()})))
    }).collect::<Result<serde_json::Map<_, _>>>()?;
    Ok(json!({"row-count": wire.row_count, "fields": fields, "hlls": hlls,
        "partitions": wire.partitions.iter().map(|part| json!({"level": part.level, "recency-micros": part.recency, "part": part.part,
            "max-block-idx": part.max_block_index, "tries": part.tries.iter().map(trie_details).collect::<Vec<_>>()})).collect::<Vec<_>>(),
        "legacy-tries": wire.tries.iter().map(trie_details).collect::<Vec<_>>(), "arrow-schema-ipc": wire.arrow_schema}))
}
fn trie_details(trie: &rtdb_format::log::proto::TrieDetails) -> Value {
    json!({"table-name": trie.table_name, "trie-key": trie.trie_key, "data-file-size": trie.data_file_size,
        "state": trie.trie_state, "garbage-as-of": trie.garbage_as_of,
        "trie-metadata": trie.trie_metadata.as_ref().map(|metadata| json!({"row-count": metadata.row_count, "iid-bloom": metadata.iid_bloom,
            "temporal": metadata.temporal_metadata.as_ref().map(|time| json!({"min-valid-from": time.min_valid_from, "max-valid-from": time.max_valid_from,
                "min-valid-to": time.min_valid_to, "max-valid-to": time.max_valid_to, "min-system-from": time.min_system_from, "max-system-from": time.max_system_from}))}))})
}
fn write_json(output: &mut impl Write, value: &impl serde::Serialize) -> Result<()> { serde_json::to_writer(output, value).map_err(invalid) }
fn bound(size: usize, factor: usize, extra: usize) -> Result<usize> { size.checked_mul(factor).and_then(|size| size.checked_add(extra)).ok_or_else(|| invalid("Inspection allocation exceeds addressable memory")) }
fn invalid(error: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("rtdb.inspect/file", error.to_string()) }
