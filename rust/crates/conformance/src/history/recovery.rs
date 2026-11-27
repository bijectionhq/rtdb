use super::*;
use async_trait::async_trait;
use bytes::Bytes;
use rtdb_format::{
    data::EventBatch, database::proto::DatabaseConfig, ipc::ArrowFile,
    keys::{TableSlug, TrieKey}, metadata::MetadataFile,
};
use rtdb_node::{DatabaseStorage, LogAccess, NativeResources, OpenedLogs, ResourceFactory};
use rtdb_storage::{ObjectKey, ObjectStore, ReadReservation, RetainedBytes, StoredObject};
use std::{ops::Range, path::PathBuf, sync::atomic::AtomicBool};
use tokio_util::sync::CancellationToken;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    schema_version: u32,
    fixture: PathBuf,
    fixture_sha256: String,
    directory: PathBuf,
    manifest: PathBuf,
    manifest_sha256: String,
    prefix: String,
    action: Action,
    canary: Canary,
    retry_compaction: bool,
    compaction_target: Option<String>,
}
#[derive(Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum Action { Capture, Recover, Canary, PrepareCompaction, PartialCompaction, InspectCompaction }
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Canary { id: String, price: i64, system_time: String, valid_time: String }

fn read_json(path: &Path) -> Result<(Vec<u8>, Json), Error> {
    if fs::symlink_metadata(path)?.len() > 16 * 1024 * 1024 { return Err("recovery input exceeds sixteen MiB".into()); }
    let bytes = fs::read(path)?;
    let value = serde_json::from_slice(&bytes)?;
    Ok((bytes, value))
}
fn sha(bytes: &[u8]) -> String { format!("{:x}", Sha256::digest(bytes)) }
fn durable_json(path: &Path, value: &Json) -> Result<(), Error> {
    if path.exists() { return Err("recovery receipt already exists".into()); }
    let temporary = path.with_extension("json.pending");
    write_new(&temporary, value)?;
    fs::rename(&temporary, path)?;
    fs::File::open(path.parent().ok_or("receipt has no directory")?)?.sync_all()?;
    Ok(())
}
fn publish_boundary(path: &Path, value: &Json) -> Result<(), Error> {
    durable_json(path, value)?;
    durable_json(&path.with_file_name("boundary-ready.json"), &json!({"boundary_sha256":crate::controls::digest_file(path)?}))
}

fn validate(request: &Request) -> Result<(Fixture, Json), Error> {
    if request.schema_version != 1 || !request.directory.is_absolute() || !request.fixture.is_absolute() || !request.manifest.is_absolute()
        || fs::symlink_metadata(&request.directory)?.is_symlink() || !request.directory.is_dir()
        || request.canary.id != "C" || request.canary.price != 991 { return Err("invalid recovery request".into()); }
    let (bytes, _) = read_json(&request.fixture)?;
    if sha(&bytes) != request.fixture_sha256 { return Err("recovery fixture digest differs".into()); }
    let fixture: Fixture = serde_json::from_slice(&bytes)?; fixture.validate()?;
    if !fixture.prefixes.iter().any(|prefix| prefix.id == request.prefix) { return Err("unknown recovery prefix".into()); }
    let (bytes, manifest) = read_json(&request.manifest)?;
    if sha(&bytes) != request.manifest_sha256 || manifest["schema_version"] != 1 || manifest["closed"] != true
        || manifest["fixture_sha256"] != request.fixture_sha256 || manifest["prefix"] != request.prefix
        || manifest["storage_version"] != rtdb_format::keys::CURRENT_STORAGE_VERSION.0 || manifest["storage_epoch"] != 0
        || manifest["partitions"] != 1 || manifest["external_source_token"] != Json::Null { return Err("recovery manifest identity differs".into()); }
    if json!(artifacts(&request.directory)?) != manifest["files"] { return Err("recovery input files differ from the closed manifest".into()); }
    let needs_target = matches!(request.action, Action::PartialCompaction | Action::InspectCompaction) || request.retry_compaction;
    if needs_target != request.compaction_target.is_some() || (request.retry_compaction && request.action != Action::Recover) {
        return Err("invalid recovery compaction request".into());
    }
    if let Some(target) = &request.compaction_target { compaction_keys(target)?; }
    let canary_time = instant(&request.canary.system_time)?;
    if canary_time <= instant(&fixture.transactions.last().ok_or("empty fixture")?.system_time)? || instant(&request.canary.valid_time)? != canary_time {
        return Err("canary time must follow the complete fixture history".into());
    }
    Ok((fixture, manifest))
}

fn compaction_keys(target: &str) -> Result<(String, String), Error> {
    let trie: TrieKey = target.parse()?;
    if trie.level() == 0 || trie.epoch() != 0 || trie.to_string() != target {
        return Err(format!("invalid epoch-zero products compaction key: {target}").into());
    }
    let slug = TableSlug::recorded("public$products")?;
    Ok((slug.data_key(&trie), slug.metadata_key(&trie)))
}

fn inspect_compaction(directory: &Path, target: &str, files: &[Json]) -> Result<Json, Error> {
    let (data_key, metadata_key) = compaction_keys(target)?;
    let data_path = format!("storage/v06/{data_key}");
    let metadata_path = format!("storage/v06/{metadata_key}");
    let data = files.iter().find(|file| file["path"] == data_path).ok_or_else(|| format!("compaction data absent: {data_key}"))?;
    let metadata = files.iter().find(|file| file["path"] == metadata_path);
    let mut reader = ArrowFile::from_reader(fs::File::open(directory.join(data_path))?, None)?;
    let mut page_rows = Vec::new();
    let mut events = Vec::new();
    for page in 0..reader.page_count() {
        let batch = EventBatch::new(reader.read_page(page)?)?;
        page_rows.push(batch.len());
        for row in 0..batch.len() {
            let fingerprint = batch.read_event(row)?.history_fingerprint()?;
            events.push(fingerprint.map(|byte| format!("{byte:02x}")).concat());
        }
    }
    events.sort_unstable();
    if events.is_empty() { return Err(format!("products compaction has no events: {data_key}").into()); }
    let leaves = if metadata.is_some() {
        let file = MetadataFile::read_reader(fs::File::open(directory.join(metadata_path))?)?;
        let leaves = file.leaves();
        let mut page_indices: Vec<_> = leaves.iter().map(|(_, page)| *page).collect();
        page_indices.sort_unstable();
        if page_indices.into_iter().ne(0..page_rows.len()) {
            return Err(format!("compaction metadata does not address every data page exactly once: {metadata_key}").into());
        }
        Some(leaves)
    } else { None };
    Ok(json!({"target":target,"data":data,"metadata":metadata,"metadata_leaves":leaves,"page_rows":page_rows,"events":events}))
}

async fn recovered(node: &Node, manifest: &Json, capture: bool) -> Result<Json, Error> {
    let target = manifest["log_heads"]["replica"].as_i64().ok_or("manifest has no replica head")?;
    let database = node.catalog().database("xtdb")?;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(error) = database.progress().fault() { return Err(Error::from(error)); }
            let position = database.status().replica_position.0;
            if position == target { break; }
            if position > target { return Err("recovery passed the protected replica head".into()); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, Error>(())
    }).await??;
    let actual = checkpoint(node).await?;
    if actual["tx_key"] != manifest["checkpoint"]["tx_key"] || (!capture && (actual["source_position"] != manifest["checkpoint"]["source_position"]
        || actual["adopted_block"] != manifest["checkpoint"]["adopted_block"] || actual["open_block"] != manifest["checkpoint"]["open_block"])) {
        return Err(format!("recovered checkpoint differs from protected boundary: {actual}").into());
    }
    Ok(actual)
}

async fn reads(node: &Node, counts: &Reads, fixture: &Fixture, request: &Request, manifest: &Json) -> Result<Vec<Json>, Error> {
    let mut result = Vec::new();
    for query in fixture.queries.iter().filter(|query| query.prefix == request.prefix) {
        for bypass in [false, true] { for product in ["A", "B"] {
            result.push(observe(node, counts, &request.prefix, "recovery", query, product, bypass).await?);
        } }
    }
    let current = manifest["checkpoint"]["tx_key"]["system_time_micros"].as_i64().ok_or("manifest has no transaction time")?;
    let current = rtdb_types::Instant::from(Timestamp(current)).to_iso_string()?;
    let query = Query { id: "current".into(), prefix: request.prefix.clone(), valid_time: current.clone(), system_time: current };
    for product in ["A", "B", "C"] { result.push(observe(node, counts, &request.prefix, "recovery", &query, product, false).await?); }
    Ok(result)
}

struct PartialUpload { marker: PathBuf, identity: Json, data_key: String, metadata_key: String, fired: AtomicBool }
struct PartialResources { inner: Arc<dyn ResourceFactory>, fault: Arc<PartialUpload> }
#[async_trait]
impl ResourceFactory for PartialResources {
    fn validate(&self, name: &str, configuration: &DatabaseConfig) -> rtdb_types::Result<()> { self.inner.validate(name, configuration) }
    async fn open_storage(&self, name: &str, configuration: &DatabaseConfig) -> rtdb_types::Result<DatabaseStorage> {
        let mut resources = self.inner.open_storage(name, configuration).await?;
        resources.store = Arc::new(PartialStore { inner: resources.store, fault: self.fault.clone() });
        Ok(resources)
    }
    async fn open_logs(&self, name: &str, configuration: &DatabaseConfig, access: LogAccess, cancellation: CancellationToken, storage: &DatabaseStorage) -> rtdb_types::Result<OpenedLogs> { self.inner.open_logs(name, configuration, access, cancellation, storage).await }
    async fn close(&self) -> rtdb_types::Result<()> { self.inner.close().await }
}
struct PartialStore { inner: Arc<dyn ObjectStore>, fault: Arc<PartialUpload> }
impl PartialStore {
    async fn uploaded(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        if key.as_str() != self.fault.data_key { return Ok(()); }
        if self.fault.fired.swap(true, Ordering::SeqCst) {
            return Err(rtdb_storage::Error::Task(format!("repeated upload at recovery boundary: {key}")));
        }
        let metadata = ObjectKey::new(self.fault.metadata_key.clone())?;
        match self.inner.head_fresh(&metadata).await {
            Err(rtdb_storage::Error::MissingObject(_)) => {},
            Ok(_) => return Err(rtdb_storage::Error::Task("partial-output boundary already has metadata".into())),
            Err(error) => return Err(error),
        }
        let data = self.inner.get_fresh(key).await?;
        let mut marker = self.fault.identity.clone();
        marker["boundary"] = json!("after_compaction_data_upload"); marker["data_key"] = json!(key.as_str());
        marker["metadata_key"] = json!(metadata.as_str()); marker["data_bytes"] = json!(data.len());
        marker["data_sha256"] = json!(sha(&data));
        publish_boundary(&self.fault.marker, &marker).map_err(|error| rtdb_storage::Error::Task(error.to_string()))?;
        std::future::pending().await
    }
}
#[async_trait]
impl ObjectStore for PartialStore {
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head(key).await }
    async fn head_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head_fresh(key).await }
    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.inner.get(key).await }
    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> { self.inner.get_range(key, range).await }
    async fn get_reserved_range(&self, key: &ObjectKey, range: Range<u64>, reservation: ReadReservation) -> rtdb_storage::Result<RetainedBytes> { self.inner.get_reserved_range(key, range, reservation).await }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
        self.inner.put(key, bytes).await?; self.uploaded(key).await
    }
    async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> {
        self.inner.put_file(key, source).await?; self.uploaded(key).await
    }
    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> { self.inner.list(directory).await }
    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.inner.delete(key).await }
    async fn copy(&self, source: &ObjectKey, target: &ObjectKey) -> rtdb_storage::Result<()> { self.inner.copy(source, target).await }
}

async fn run(request: &Request, fixture: &Fixture, manifest: &Json, output: &Path, identity: &Json) -> Result<Json, Error> {
    if request.action == Action::InspectCompaction {
        return inspect_compaction(&request.directory, request.compaction_target.as_deref().ok_or("missing compaction target")?,
            manifest["files"].as_array().ok_or("missing recovery file inventory")?);
    }
    let time = manifest["checkpoint"]["tx_key"]["system_time_micros"].as_i64().ok_or("manifest transaction is missing")?;
    let clock = Arc::new(FixtureClock(AtomicI64::new(time)));
    let counts = Arc::new(Reads::default());
    let resources = Arc::new(NativeResources::default().with_local_log_clock(clock.clone(), false));
    let follower = RunningNode::open_with_resources(&request.directory, clock.clone(), counts.clone(), resources, true).await?;
    let work = async {
        let checkpoint = recovered(&follower.node, manifest, request.action == Action::Capture).await?;
        let observations = reads(&follower.node, &counts, fixture, request, manifest).await?;
        Ok::<_, Error>(json!({"checkpoint":checkpoint,"observations":observations}))
    }.await;
    let mut result = combine(work, follower.close().await)?;
    if request.action == Action::Capture || (request.action == Action::Recover && !request.retry_compaction) { return Ok(result); }
    let native = Arc::new(NativeResources::default().with_local_log_clock(clock.clone(), false));
    let resources: Arc<dyn ResourceFactory> = if request.action == Action::PartialCompaction {
        let (data_key, metadata_key) = compaction_keys(request.compaction_target.as_deref().ok_or("missing compaction target")?)?;
        if [data_key.as_str(), metadata_key.as_str()].into_iter().any(|key| request.directory.join("storage/v06").join(key).exists()) {
            return Err("partial compaction writer requires an unoccupied output key".into());
        }
        Arc::new(PartialResources { inner: native, fault: Arc::new(PartialUpload {
            marker: output.join("boundary.json"), identity: identity.clone(), data_key, metadata_key, fired: AtomicBool::new(false),
        }) })
    } else { native };
    let running = RunningNode::open_with_resources(&request.directory, clock.clone(), counts.clone(), resources, false).await?;
    let work = async {
        running.node.sync_all(Duration::from_secs(30), running.node.cancellation()).await?;
        if matches!(request.action, Action::Canary | Action::PrepareCompaction) {
            let transaction = Transaction { system_time: request.canary.system_time.clone(), operation: Operation::Put {
                prices: BTreeMap::from([(request.canary.id.clone(), request.canary.price)]), valid_from: request.canary.valid_time.clone(), valid_to: None } };
            let key = submit(&running.node, &clock, &transaction).await?;
            let mut acknowledged = identity.clone();
            acknowledged["boundary"] = json!("after_canary_acknowledged"); acknowledged["transaction"] = key;
            acknowledged["canary"] = json!({"id":request.canary.id,"price":request.canary.price,"system_time":request.canary.system_time,"valid_time":request.canary.valid_time});
            acknowledged["checkpoint"] = checkpoint(&running.node).await?;
            durable_json(&output.join("acknowledged.json"), &acknowledged)?;
            if request.action == Action::Canary {
                publish_boundary(&output.join("boundary.json"), &acknowledged)?;
                return std::future::pending::<Result<Json, Error>>().await;
            }
            flush(&running.node).await?;
            result["prepared_checkpoint"] = checkpoint(&running.node).await?;
            return Ok(result);
        }
        let database = running.node.catalog().database("xtdb")?;
        let outcome = tokio::time::timeout(Duration::from_secs(60), database.compaction().ok_or("recovery compactor missing")?.compact_all()).await?;
        let compaction_error = match outcome {
            Ok(()) => None,
            Err(error) if request.action == Action::Recover && error.code == "rtdb.storage/immutable-object-conflict" => Some(error),
            Err(error) => return Err(error.into()),
        };
        if request.action == Action::PartialCompaction { return Err("compaction completed without reaching the requested partial upload boundary".into()); }
        running.node.sync_all(Duration::from_secs(30), running.node.cancellation()).await?;
        let mut after = manifest.clone(); after["checkpoint"] = checkpoint(&running.node).await?;
        result["after_retry"] = json!({"checkpoint":after["checkpoint"],"observations":reads(&running.node, &counts, fixture, request, &after).await?});
        if let Some(error) = compaction_error { result["after_retry"]["compaction_error"] = json!(error); }
        Ok(result)
    }.await;
    combine(work, running.close().await)
}

/// Opens only the isolated directory named by a verified closed manifest.
/// Ordinary completion joins every node; crash actions publish a durable boundary and remain pending for the process owner to kill and join.
pub async fn execute(request_path: &Path, output: &Path) -> Result<Json, Error> {
    let (bytes, _) = read_json(request_path)?;
    let request: Request = serde_json::from_slice(&bytes)?;
    fs::create_dir(output)?;
    let mut receipt = json!({"schema_version":1,"engine":"rust","request_sha256":sha(&bytes),"fixture_sha256":request.fixture_sha256,
        "binary_sha256":crate::controls::digest_file(&std::env::current_exe()?)?,"artifact_revision":env!("RTDB_CONFORMANCE_REVISION"),
        "source_dirty":env!("RTDB_CONFORMANCE_DIRTY")!="false","execution_complete":false,"complete_parity":false});
    let result = match validate(&request) { Ok((fixture, manifest)) => run(&request, &fixture, &manifest, output, &receipt).await, Err(error) => Err(error) };
    match result {
        Ok(value) => { receipt["execution_complete"] = json!(true); receipt["result"] = value; },
        Err(error) => { receipt["error"] = json!(error.to_string()); },
    }
    receipt["closure"] = json!("graceful");
    match artifacts(&request.directory) {
        Ok(files) => receipt["artifacts"] = json!(files),
        Err(error) => { receipt["execution_complete"] = json!(false); receipt["artifact_error"] = json!(error.to_string()); },
    }
    durable_json(&output.join("execution.json"), &receipt)?;
    Ok(receipt)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::{data::{StoredEvent, StoredOperation, build_event_batch}, ipc::write_file, metadata::MetadataBuilder};
    use rtdb_types::{Iid, Interval};

    #[test]
    fn inspection_compares_events_across_page_layouts_and_rejects_unaddressed_pages() -> Result<(), Error> {
        let target = "l01-rc-b01";
        let (data_key, metadata_key) = compaction_keys(target)?;
        let directory = tempfile::tempdir()?;
        let data = directory.path().join("storage/v06").join(data_key);
        let metadata = directory.path().join("storage/v06").join(metadata_key);
        fs::create_dir_all(data.parent().unwrap())?;
        fs::create_dir_all(metadata.parent().unwrap())?;
        let event = |iid| StoredEvent { iid: Iid([iid; 16]), position: None, origin: None, entity_key: None,
            system_from: Timestamp(1), commit_position: 0, valid_time: Interval::ALL, operation: StoredOperation::Delete, recency: None };
        let events = [event(1), event(2)];
        let page = build_event_batch(&events, None, None)?;
        fs::write(&data, write_file(&page.schema(), std::slice::from_ref(&page))?)?;
        let inspect = || inspect_compaction(directory.path(), target, &artifacts(directory.path())?);
        let orphan = inspect()?;
        assert!(orphan["metadata"].is_null());
        assert_eq!(orphan["page_rows"], json!([2]));
        let mut meta = MetadataBuilder::new(false, true);
        meta.write_leaf(&page)?;
        let metadata_bytes = meta.finish()?.0.write()?;
        fs::write(&metadata, metadata_bytes)?;
        let complete = inspect()?;
        assert_eq!(complete["metadata_leaves"], json!([[[], 0]]));
        assert_eq!(complete["events"], orphan["events"]);
        let pages = [page.slice(0, 1), page.slice(1, 1)];
        fs::write(&data, write_file(&page.schema(), &pages)?)?;
        assert!(inspect().unwrap_err().to_string().contains("does not address every data page"));
        fs::remove_file(&metadata)?;
        let split = inspect()?;
        assert_eq!(split["page_rows"], json!([1, 1]));
        assert_eq!(split["events"], orphan["events"]);
        assert_ne!(split["data"], orphan["data"]);
        Ok(())
    }
}
