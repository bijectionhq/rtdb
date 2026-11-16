mod namespace;

use std::{collections::BTreeMap, fs::{self, OpenOptions}, io::{Read, Write}, path::{Path, PathBuf}, sync::Arc, time::Duration};

use prost::Message;
use rtdb_columnar::MemoryBudget;
use rtdb_format::{catalog::{BlockCatalog, TableBlock}, envelope::{ReplicaBody, ReplicaEnvelope}, keys::{block_index_from_key, TableSlug, TrieKey}};
use rtdb_node::{DatabaseResources, LogAccess, ResourceFactory};
use rtdb_server::config::{ParsedConfiguration, ProcessEnvironment};
use rtdb_storage::{ObjectKey, ObjectStore, ReadReservation, log::{Log, LogOffset}};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    schema_version: u32,
    namespace: String,
    configuration: PathBuf,
    configuration_sha256: String,
    capability: String,
    token_kind: TokenKind,
    action: Action,
    maximum_bytes: u64,
    timeout_seconds: u64,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TokenKind { None, Postgres, KafkaConnect }

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    Capture,
    DenialProbe { key: String, policy_sha256: String },
    Purge { manifest: PathBuf, manifest_sha256: String },
}

fn sha(bytes: &[u8]) -> String { format!("{:x}", Sha256::digest(bytes)) }
fn is_sha(value: &str) -> bool { value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()) }
fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>> {
    let mut input = fs::File::open(path)?.take(limit + 1);
    let mut bytes = Vec::new();
    input.read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit { return Err("backend helper input exceeds its byte bound".into()); }
    Ok(bytes)
}
fn write_json(path: &Path, value: &Value) -> Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    serde_json::to_writer_pretty(&mut file, value)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn token(bytes: Option<&[u8]>, kind: TokenKind) -> Result<Value> {
    let Some(bytes) = bytes else { return Ok(Value::Null); };
    let decoded = match kind {
        TokenKind::None => return Err("non-external database contains an external source token".into()),
        TokenKind::Postgres => {
            let value = rtdb_format::postgres::proto::PostgresSourceToken::decode(bytes)?;
            json!({"kind":"postgres", "lsn":value.latest_committed_lsn.unwrap_or_default(), "tx_id":value.latest_tx_id.unwrap_or_default(), "snapshot_completed":value.snapshot_completed.unwrap_or_default()})
        }
        TokenKind::KafkaConnect => {
            let value = rtdb_format::generated::xtdb::kafka::connectsrc::proto::KafkaConnectSourceToken::decode(bytes)?;
            json!({"kind":"kafka_connect", "offset":value.offset.unwrap_or_default()})
        }
    };
    Ok(json!({"sha256":sha(bytes), "bytes":bytes.len(), "decoded":decoded}))
}

async fn copy_object(store: &dyn ObjectStore, key: &ObjectKey, size: u64, output: &Path, memory: &Arc<MemoryBudget>) -> Result<String> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(output)?;
    let mut digest = Sha256::new();
    let mut offset = 0;
    while offset < size {
        let length = (size - offset).min(1024 * 1024) as usize;
        let bytes = store.get_reserved_range(key, offset..offset + length as u64, ReadReservation::new(length, memory.reserve(length)?)).await?;
        digest.update(bytes.as_slice());
        file.write_all(bytes.as_slice())?;
        offset += length as u64;
    }
    if store.head(key).await?.size != size { return Err("object changed size during capture".into()); }
    file.sync_all()?;
    Ok(format!("{:x}", digest.finalize()))
}

async fn capture_log(log: &dyn Log, replica: bool, kind: TokenKind, output: &Path, memory: &Arc<MemoryBudget>, remaining: &mut u64) -> Result<Value> {
    fs::create_dir(output)?;
    let tip = log.latest()?.0;
    let end = tip.checked_add(1).ok_or("log tip overflow")?;
    let mut range = log.range(LogOffset(0), LogOffset(end)).await?;
    let mut records = Vec::new();
    loop {
        let bound = range.allocation_bound();
        let Some(record) = range.next(ReadReservation::new(bound, memory.reserve(bound)?)).await? else { break; };
        if records.len() == 100_000 { return Err("backend capture exceeds log-record limit".into()); }
        *remaining = remaining.checked_sub(record.payload.len() as u64).ok_or("backend capture exceeds total byte limit")?;
        let name = format!("{:020}.bin", record.metadata.offset.0);
        let mut file = OpenOptions::new().write(true).create_new(true).open(output.join(&name))?;
        file.write_all(&record.payload)?;
        file.sync_all()?;
        let decoded = if replica {
            let _scratch = memory.reserve(record.payload.len().checked_mul(64).ok_or("replica decode bound overflow")?.checked_add(65536).ok_or("replica decode bound overflow")?)?;
            ReplicaEnvelope::decode(&record.payload)?.map(|envelope| -> Result<Value> {
                let body = match envelope.message {
                    ReplicaBody::ResolvedTx(tx) => json!({"kind":"resolved_tx", "tx_id":tx.tx_id, "system_time_micros":tx.system_time_micros,
                        "source_position":tx.src_msg_id, "external_token":token(tx.external_source_token.as_deref(), kind)?}),
                    ReplicaBody::BlockBoundary(block) => json!({"kind":"block_boundary", "block_index":block.block_index,
                        "source_position":block.latest_processed_msg_id, "external_token":token(block.external_source_token.as_deref(), kind)?}),
                    _ => Value::Null,
                };
                Ok(json!({"term":envelope.term.0, "term_sequence":envelope.term_sequence.map(|value|value.0), "body":body}))
            }).transpose()?.unwrap_or(Value::Null)
        } else { Value::Null };
        records.push(json!({"offset":record.metadata.offset.0,"timestamp_micros":record.metadata.timestamp.0,
            "bytes":record.payload.len(),"sha256":sha(&record.payload),"file":name,"decoded":decoded}));
    }
    if tip >= 0 && records.last().and_then(|record|record["offset"].as_i64()) != Some(tip) { return Err("captured log range does not reach the observed durable tip".into()); }
    Ok(json!({"tip":tip,"records":records}))
}

fn catalog(output: &Path, objects: &[Value], kind: TokenKind, memory: &Arc<MemoryBudget>) -> Result<Value> {
    let latest = objects.iter().filter_map(|item| block_index_from_key(item["key"].as_str()?).ok().map(|index|(index,item))).max_by_key(|(index,_)|*index);
    let Some((index, object)) = latest else { return Ok(Value::Null); };
    let block_bytes = read_bounded(&output.join(object["file"].as_str().ok_or("catalog file absent")?), 16 * 1024 * 1024)?;
    let block = BlockCatalog::decode_reserved(&block_bytes, memory.reserve(block_bytes.len().checked_mul(64).ok_or("catalog bound overflow")?.checked_add(65536).ok_or("catalog bound overflow")?)?)?;
    if index != block.block_index() { return Err("catalog filename and stored block index disagree".into()); }
    let mut tables = BTreeMap::new();
    let mut identities = BTreeMap::new();
    for entry in block.table_entries()? {
        let key = entry.slug.block_key(index)?;
        let item = objects.iter().find(|item|item["key"] == key).ok_or("latest block is missing its recorded table block")?;
        let bytes = read_bounded(&output.join(item["file"].as_str().ok_or("table-block file absent")?), 16 * 1024 * 1024)?;
        let table = TableBlock::decode_reserved(&bytes, memory.reserve(bytes.len().checked_mul(64).ok_or("table catalog bound overflow")?.checked_add(65536).ok_or("table catalog bound overflow")?)?)?;
        identities.insert(entry.table.clone(), entry.slug);
        tables.insert(entry.table, table);
    }
    let tries = rtdb_index::TrieCatalog::load(&tables, 100 * 1024 * 1024, block.latest_completed_tx().map_or(rtdb_types::Timestamp(0),|tx|tx.system_time))?;
    let jobs = rtdb_maintenance::available_jobs(&tries)?.into_iter().map(|job| {
        let slug = identities.get(&job.table).ok_or("job table lacks a recorded slug")?;
        Ok(json!({"table":job.table.schema_and_table(),"output":job.output.to_string(),"data_key":slug.data_key(&job.output),
            "metadata_key":slug.metadata_key(&job.output),"inputs":job.inputs.iter().map(ToString::to_string).collect::<Vec<_>>()}))
    }).collect::<Result<Vec<_>>>()?;
    Ok(json!({"block_index":index.0,"source_position":block.latest_source_position().map(|value|value.0),
        "replica_position":block.boundary_replica_position().map(|value|value.0),"term":block.boundary_term().0,
        "term_sequence":block.boundary_term_sequence().map(|value|value.0),"tx_key":block.latest_completed_tx().map(|tx|json!({"tx_id":tx.tx_id.0,"system_time_micros":tx.system_time.0})),
        "external_token":token(block.external_source_token(),kind)?,"available_compactions":jobs}))
}

async fn capture(request: &Request, resources: &DatabaseResources, output: &Path, memory: &Arc<MemoryBudget>) -> Result<Value> {
    let listed = resources.store.list(None).await?;
    if listed.len() > 10000 { return Err("backend capture exceeds object-count limit".into()); }
    fs::create_dir(output.join("objects"))?;
    let mut remaining = request.maximum_bytes;
    let mut objects = Vec::new();
    for (index, object) in listed.iter().enumerate() {
        remaining = remaining.checked_sub(object.size).ok_or("backend capture exceeds total byte limit")?;
        let file = format!("objects/{index:08}.bin");
        let digest = copy_object(resources.store.as_ref(), &object.key, object.size, &output.join(&file), memory).await?;
        objects.push(json!({"key":object.key.as_str(),"bytes":object.size,"sha256":digest,"file":file}));
    }
    let before = listed.iter().map(|item|(&item.key,item.size)).collect::<Vec<_>>();
    let after = resources.store.list(None).await?;
    if before != after.iter().map(|item|(&item.key,item.size)).collect::<Vec<_>>() { return Err("object inventory changed during capture".into()); }
    let catalog = catalog(output, &objects, request.token_kind, memory)?;
    let source = capture_log(resources.logs.source.as_ref(), false, request.token_kind, &output.join("source"), memory, &mut remaining).await?;
    let replica = capture_log(resources.logs.replica.as_ref(), true, request.token_kind, &output.join("replica"), memory, &mut remaining).await?;
    Ok(json!({"objects":objects,"catalog":catalog,"source":source,"replica":replica,"captured_bytes":request.maximum_bytes-remaining,
        "storage_version":resources.storage_version.0,"storage_epoch":resources.storage_epoch.0,
        "source_epoch":resources.logs.source_epoch.0,"replica_epoch":resources.logs.replica_epoch.0}))
}

fn metadata_key(value: &str) -> Result<ObjectKey> {
    let key = ObjectKey::new(value)?;
    let (table, filename) = value.strip_prefix("tables/").and_then(|value|value.split_once("/meta/")).ok_or("denial probe requires an exact table metadata key")?;
    TableSlug::recorded(table)?;
    let trie: TrieKey = filename.strip_suffix(".arrow").ok_or("metadata key lacks Arrow suffix")?.parse()?;
    if trie.level() == 0 { return Err("denial probe cannot target a live-index L0 output".into()); }
    Ok(key)
}

async fn action(request: &Request, resources: &DatabaseResources, output: &Path, memory: &Arc<MemoryBudget>) -> Result<Value> {
    match &request.action {
        Action::Capture => capture(request, resources, output, memory).await,
        Action::DenialProbe { key, policy_sha256 } => {
            if !is_sha(policy_sha256) { return Err("fault policy digest is missing or malformed".into()); }
            let key = metadata_key(key)?;
            match resources.store.head(&key).await { Err(rtdb_storage::Error::MissingObject(_))=>(), Ok(_)=>return Err("denial probe target already exists".into()),Err(error)=>return Err(error.into()) }
            let error = resources.store.put(&key, bytes::Bytes::from_static(b"RTDB recovery metadata denial probe\n")).await.err().ok_or("metadata denial probe unexpectedly published an object")?;
            let permission_denied = match &error {
                rtdb_storage::Error::Io { source, .. } => source.kind() == std::io::ErrorKind::PermissionDenied,
                rtdb_storage::Error::PermissionDenied { .. } => true,
                _ => false,
            };
            if !permission_denied { return Err("metadata write failed without a recognized provider permission denial".into()); }
            Ok(json!({"operation":"immutable_put","key":key.as_str(),"permission_denied":true,"error_sha256":sha(error.to_string().as_bytes()),"policy_sha256":policy_sha256}))
        }
        Action::Purge { manifest, manifest_sha256 } => {
            let bytes = read_bounded(manifest,32*1024*1024)?;
            if sha(&bytes) != *manifest_sha256 { return Err("cleanup manifest digest differs".into()); }
            let value: Value = serde_json::from_slice(&bytes)?;
            if value["namespace"] != request.namespace || value["configuration_sha256"] != request.configuration_sha256 || value["execution_complete"] != true { return Err("cleanup manifest does not own this backend namespace".into()); }
            let objects = value["result"]["objects"].as_array().ok_or("cleanup manifest lacks objects")?;
            let listed = resources.store.list(None).await?;
            if listed.len() != objects.len() { return Err("backend contains objects outside the cleanup manifest".into()); }
            for (index,item) in objects.iter().enumerate() {
                let key = ObjectKey::new(item["key"].as_str().ok_or("cleanup object key absent")?)?;
                let size = item["bytes"].as_u64().ok_or("cleanup object byte count absent")?;
                let file = output.join(format!("verify-{index:08}.bin"));
                if copy_object(resources.store.as_ref(), &key, size, &file, memory).await? != item["sha256"] { return Err("cleanup object bytes differ from captured ownership manifest".into()); }
                fs::remove_file(file)?;
            }
            for item in objects { resources.store.delete(&ObjectKey::new(item["key"].as_str().ok_or("cleanup object key absent")?)?).await?; }
            if !resources.store.list(None).await?.is_empty() { return Err("namespace did not empty after object cleanup".into()); }
            Ok(json!({"deleted_objects":objects.len(),"manifest_sha256":manifest_sha256}))
        }
    }
}

/// Executes one bounded backend invocation without starting a database or any returned driver.
/// - Input: a version-1 request naming an isolated namespace and hashed production YAML configuration.
/// - Output: a new directory containing raw object/log captures and the final receipt.
/// - Ownership: await this future to completion; aborting it bypasses the explicit resource drain.
///   Internal timeout and SIGTERM cancel the operation before awaiting resource close.
///   The process caller joins the invocation before accepting its receipt.
/// - Errors: input, provider, capture, cancellation, and cleanup failures prevent a completed receipt.
pub async fn execute(input: &Path, output: &Path) -> Result<Value> {
    let request_bytes = read_bounded(input, 1024*1024)?;
    let request: Request = serde_json::from_slice(&request_bytes)?;
    if request.schema_version != 1 || !(1..=8*1024*1024*1024).contains(&request.maximum_bytes) || !(1..=3600).contains(&request.timeout_seconds) {
        return Err("invalid backend helper request limits".into());
    }
    let configuration = read_bounded(&request.configuration, 1024*1024)?;
    if sha(&configuration) != request.configuration_sha256 { return Err("backend configuration digest differs".into()); }
    let memory = MemoryBudget::new(512*1024*1024);
    let parsed = ParsedConfiguration::from_yaml(std::str::from_utf8(&configuration)?, &ProcessEnvironment, &memory, false)?;
    let config = parsed.configuration();
    let identity = namespace::validate(config, &request.namespace, &request.capability)?;
    fs::create_dir(output)?;
    let metrics = rtdb_metrics::Registry::new("backend-inspection", memory.clone())?;
    let (factory, sources, registrations) = rtdb_server::open_backend_resources(config,&memory,&metrics)?;
    let cancellation = CancellationToken::new();
    let operation = async {
        let resources = factory.open("xtdb", &config.primary, LogAccess::ReadOnly, cancellation.child_token()).await?;
        if !resources.logs.drivers.is_empty() { return Err::<Value, Error>("read-only inspection unexpectedly opened log drivers".into()); }
        action(&request,&resources,output,&memory).await
    };
    #[cfg(unix)]
    let signal = async { tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?.recv().await; Ok::<(),std::io::Error>(()) };
    #[cfg(not(unix))]
    let signal = tokio::signal::ctrl_c();
    let result = tokio::select! {
        result = operation => result,
        signal = signal => match signal { Ok(())=>Err("backend invocation cancelled".into()),Err(error)=>Err(error.into()) },
        _ = tokio::time::sleep(Duration::from_secs(request.timeout_seconds)) => Err("backend invocation timed out".into()),
    };
    cancellation.cancel();
    drop(sources);
    drop(registrations);
    let close = factory.close().await;
    let receipt = json!({"schema_version":1,"namespace":request.namespace,"configuration_sha256":request.configuration_sha256,
        "request_sha256":sha(&request_bytes),"artifact_revision":env!("RTDB_CONFORMANCE_REVISION"),"source_dirty":env!("RTDB_CONFORMANCE_DIRTY")=="true",
        "binary_sha256":crate::controls::digest_file(&std::env::current_exe()?).map_err(|error|error.to_string())?,
        "identity":identity,"execution_complete":result.is_ok()&&close.is_ok(),"resources_closed":close.is_ok(),
        "result":result.as_ref().ok(),"failed":result.is_err(),"cleanup_failed":close.is_err()});
    write_json(&output.join("execution.json"), &receipt)?;
    match (result,close) {
        (Ok(_),Ok(()))=>Ok(receipt),
        (Err(error),Ok(()))=>Err(error),
        (Ok(_),Err(error))=>Err(error.into()),
        (Err(_),Err(_))=>Err("backend operation and resource drain both failed; inspect the retained failure receipt".into()),
    }
}
