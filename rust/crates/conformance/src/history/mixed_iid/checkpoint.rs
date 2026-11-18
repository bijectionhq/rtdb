use super::{Error, FileShape, IidEncoding, Identity, Node, SOURCE_SHA256, compacted_key, inspect_file, layout_directory, physical_root};
use rtdb_format::keys::block_index_from_key;
use rtdb_storage::log::{Log, ReadOnlyLocalLog, ReadOnlyLocalLogOptions};
use rtdb_types::{ReplicaPosition, SourcePosition, Timestamp};
use serde::Deserialize;
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};
use std::{collections::{BTreeMap, BTreeSet}, fs::{self, File}, io::Read, path::{Path, PathBuf}, sync::Arc, time::Duration};

const MAX_REQUEST: u64 = 8 << 20;
const MAX_ENTRIES: usize = 4096;
const MAX_BYTES: u64 = 64 << 20;
const CASE: &str = "[engine:dev.clojurephant.jovial.clojure-test]/[namespace:xtdb.mixed-iid-encoding-test]/[name:a-scan-over-plain-and-run-end-encoded-iid-pages-answers-as-over-plain-ones-6258]";
const JVM_CASE_DIRECTORY: &str = "target/mixed-iid-encoding/";
pub(super) const LAST_PREFIX_TIME: Timestamp = Timestamp(1_578_679_200_000_000);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Archive {
    schema_version: u32,
    stage: String,
    files: Vec<ArchivedFile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchivedFile {
    path: String,
    bytes: u64,
    sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Copy {
    source: String,
    destination: String,
    bytes: u64,
    sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Rewrite {
    before: Json,
    after: Json,
    result: Json,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Heads {
    source: i64,
    replica: i64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Sides {
    pub(super) control: Heads,
    pub(super) mixed: Heads,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    schema_version: u32,
    case: String,
    source_sha256: String,
    reference_result_sha256: String,
    reference_execution_sha256: String,
    checkpoint: Archive,
    copies: Vec<Copy>,
    heads: Sides,
    reference_rewrites: Vec<Rewrite>,
}

pub(super) struct Replay {
    pub(super) request_sha256: String,
    pub(super) reference_result_sha256: String,
    pub(super) reference_execution_sha256: String,
    pub(super) heads: Sides,
    pub(super) rewritten: Vec<Json>,
}

fn digest_text(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn destination(source: &str) -> Result<String, Error> {
    let parts: Vec<_> = source.split('/').collect();
    if parts.len() > 16 || parts.iter().any(|part| matches!(*part, "" | "." | "..") || part.contains(['\\', '\0'])) {
        return Err(format!("Invalid JVM checkpoint path: {source}").into());
    }
    let [side, kind, tail @ ..] = parts.as_slice() else { return Err("Missing JVM checkpoint side or root".into()); };
    if !matches!(*side, "control" | "mixed") { return Err("Invalid JVM checkpoint side".into()); }
    let directory = match (*kind, tail) {
        ("log", ["LOG"] | ["REPLICA_LOG"]) => "log",
        ("objects", ["v06", _, ..]) => "storage",
        _ => return Err(format!("Invalid JVM checkpoint root: {source}").into()),
    };
    Ok(format!("mixed-iid-{side}/{directory}/{}", tail.join("/")))
}

fn verify_file(path: &Path, expected: &ArchivedFile) -> Result<(), Error> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.is_file() || metadata.len() != expected.bytes {
        return Err(format!("Copied checkpoint file size or kind differs: {}", path.display()).into());
    }
    let mut contents = File::open(path)?.take(expected.bytes + 1);
    let mut digest = Sha256::new();
    let count = std::io::copy(&mut contents, &mut DigestWriter(&mut digest))?;
    if count != expected.bytes || format!("{:x}", digest.finalize()) != expected.sha256 {
        return Err(format!("Copied checkpoint bytes differ: {}", path.display()).into());
    }
    Ok(())
}

struct DigestWriter<'a>(&'a mut Sha256);

impl std::io::Write for DigestWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}

fn verify_copies(root: &Path, request: &Request) -> Result<(), Error> {
    let archive = &request.checkpoint;
    if archive.schema_version != 1 || archive.stage != "after-rewrite-before-reopen" || archive.files.is_empty()
        || archive.files.len() > MAX_ENTRIES || request.copies.len() != archive.files.len()
    {
        return Err("Invalid JVM checkpoint manifest".into());
    }
    let mut destinations = BTreeMap::new();
    let mut files = BTreeMap::new();
    let mut size = 0_u64;
    for entry in &archive.files {
        size = size.checked_add(entry.bytes).ok_or("Checkpoint size overflow")?;
        let mapped = destination(&entry.path)?;
        if size > MAX_BYTES || !digest_text(&entry.sha256) || files.insert(entry.path.as_str(), entry).is_some()
            || destinations.insert(mapped, entry).is_some()
        {
            return Err("Invalid or repeated JVM checkpoint file".into());
        }
    }
    let mut copied = BTreeSet::new();
    for copy in &request.copies {
        let entry = files.get(copy.source.as_str()).ok_or("Copy omitted its archived source")?;
        if destination(&copy.source)? != copy.destination || !copied.insert(copy.destination.as_str())
            || copy.bytes != entry.bytes || copy.sha256 != entry.sha256
        {
            return Err("Copied checkpoint manifest differs from its original paths and bytes".into());
        }
    }
    let directories: BTreeSet<_> = destinations.keys().flat_map(|path| {
        Path::new(path).ancestors().skip(1).filter(|parent| !parent.as_os_str().is_empty()).map(Path::to_path_buf)
    }).collect();
    let mut seen = BTreeSet::new();
    let mut pending = vec![root.to_path_buf()];
    let mut entries = 0;
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let path = entry?.path();
            let relative = path.strip_prefix(root)?;
            let kind = fs::symlink_metadata(&path)?.file_type();
            entries += 1;
            if entries > MAX_ENTRIES { return Err("Copied checkpoint exceeds its entry limit".into()); }
            if kind.is_dir() && directories.contains(relative) {
                pending.push(path);
                continue;
            }
            let name = relative.to_str().ok_or("Non-UTF8 checkpoint path")?;
            let expected = destinations.get(name).ok_or("Unexpected copied checkpoint entry")?;
            verify_file(&path, expected)?;
            seen.insert(name.to_owned());
        }
    }
    if seen.len() != destinations.len() { return Err("Copied checkpoint omitted archived files".into()); }
    for side in ["control", "mixed"] {
        let directory = format!("mixed-iid-{side}/");
        for log in ["LOG", "REPLICA_LOG"] {
            if !destinations.get(&format!("{directory}log/{log}")).is_some_and(|entry| entry.bytes > 0) {
                return Err("Copied checkpoint omitted an archived local log".into());
            }
        }
        let storage = format!("{directory}storage/v06/");
        if !destinations.iter().any(|(name, entry)| entry.bytes > 0 &&
            name.strip_prefix(storage.as_str()).and_then(|key| key.strip_prefix("blocks/"))
                .is_some_and(|name| !name.contains('/') && block_index_from_key(name).is_ok()))
        {
            return Err("Copied checkpoint omitted a block catalog".into());
        }
    }
    Ok(())
}

fn file_shape(mut original: Json) -> Result<(String, FileShape), Error> {
    let path = original.as_object_mut().and_then(|object| object.remove("path"))
        .and_then(|value| value.as_str().map(str::to_owned)).ok_or("JVM rewrite omitted its path")?;
    let shape: FileShape = serde_json::from_value(original)?;
    if shape.bytes == 0 || !digest_text(&shape.sha256) {
        return Err("JVM rewrite omitted its file digest or size".into());
    }
    Ok((path, shape))
}

fn verify_rewrites(root: &Path, rewrites: Vec<Rewrite>) -> Result<Vec<Json>, Error> {
    if rewrites.is_empty() || rewrites.len() > MAX_ENTRIES { return Err("Invalid JVM rewrite count".into()); }
    let storage = physical_root()?;
    let mut result = Vec::new();
    let mut paths = BTreeSet::new();
    let mut multi_row_run = false;
    for rewrite in rewrites {
        let (before_path, before) = file_shape(rewrite.before)?;
        let (after_path, after) = file_shape(rewrite.after)?;
        let source = after_path.strip_prefix(JVM_CASE_DIRECTORY).ok_or("JVM rewrite has a different workspace")?;
        let mapped = destination(source)?;
        let relative = mapped.strip_prefix("mixed-iid-mixed/").ok_or("JVM rewrite is outside the mixed store")?;
        let path = Path::new(relative);
        compacted_key(path, &storage)?.ok_or("JVM rewrite is outside compacted mixed table data files")?;
        let key = path.file_stem().and_then(|stem| stem.to_str()).ok_or("JVM rewrite omitted its trie key")?;
        let rows = after.pages.iter().try_fold(0_usize, |total, page| total.checked_add(page.rows)).ok_or("JVM rewrite row count overflow")?;
        let runs = after.pages.iter().try_fold(0_usize, |total, page| total.checked_add(page.runs.unwrap_or(0))).ok_or("JVM rewrite run count overflow")?;
        if !paths.insert(mapped.clone()) || before_path != after_path
            || before.encoding != IidEncoding::Plain || after.encoding != IidEncoding::RunEndI32
            || before.sha256 == after.sha256 || before.pages.iter().map(|page| page.rows).ne(after.pages.iter().map(|page| page.rows))
            || rewrite.result != json!({"rows":rows,"runs":runs})
            || inspect_file(&root.join(&mapped))? != after
        {
            return Err("Copied checkpoint does not match the observed JVM IID rewrite".into());
        }
        multi_row_run |= after.pages.iter().any(|page| page.runs.is_some_and(|runs| runs < page.rows));
        result.push(json!({"path":relative, "trie_key":key, "before":before, "after":after}));
    }
    if !multi_row_run { return Err("Copied JVM rewrite did not establish a multi-row run".into()); }
    Ok(result)
}

fn verify_log_heads(root: &Path, heads: &Heads) -> Result<(), Error> {
    for (name, expected) in [("LOG", heads.source), ("REPLICA_LOG", heads.replica)] {
        let log = ReadOnlyLocalLog::open(root.join("log").join(name), ReadOnlyLocalLogOptions::default())?;
        if expected < 0 || log.latest()?.0 != expected { return Err(format!("Copied checkpoint log tip differs: {name}").into()); }
    }
    Ok(())
}

pub(super) fn load(request: &Path, output: &Path) -> Result<Replay, Error> {
    if fs::symlink_metadata(request)?.is_file() && request.canonicalize()? == request
        && fs::symlink_metadata(output)?.is_dir() && output.canonicalize()? == output
        && request.parent() == output.parent()
    {
        let mut bytes = Vec::new();
        File::open(request)?.take(MAX_REQUEST + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_REQUEST { return Err("Portable checkpoint request exceeds its size bound".into()); }
        let request: Request = serde_json::from_slice(&bytes)?;
        if request.schema_version != 1 || request.case != CASE || request.source_sha256 != SOURCE_SHA256
            || !digest_text(&request.reference_result_sha256) || !digest_text(&request.reference_execution_sha256)
        {
            return Err("Invalid portable mixed IID request identity".into());
        }
        verify_copies(output, &request)?;
        verify_log_heads(&layout_directory(output, "control"), &request.heads.control)?;
        verify_log_heads(&layout_directory(output, "mixed"), &request.heads.mixed)?;
        let rewritten = verify_rewrites(output, request.reference_rewrites)?;
        return Ok(Replay { request_sha256: format!("{:x}", Sha256::digest(&bytes)),
            reference_result_sha256: request.reference_result_sha256, reference_execution_sha256: request.reference_execution_sha256,
            heads: request.heads, rewritten });
    }
    Err("Portable mixed IID request and copies must be canonical siblings".into())
}

pub(super) async fn observe_recovery(node: &Node, heads: &Heads) -> Result<Json, Error> {
    let database = node.catalog().database("xtdb")?;
    if database.latest_submitted_position()? != SourcePosition(heads.source) {
        return Err("Native source log did not open the archived source tip".into());
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(error) = database.progress().fault() { return Err(Error::from(error)); }
            if database.status().replica_position >= ReplicaPosition(heads.replica) { return Ok(()); }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await??;
    node.sync_all(Duration::from_secs(30), node.cancellation()).await?;

    let progress = database.progress().current();
    let status = database.status();
    let key = progress.tx_key.ok_or("Native checkpoint has no recovered transaction")?;
    let adopted = status.adopted_block.as_ref().ok_or("Native checkpoint has no recovered block catalog")?;
    let mut session = node.session("xtdb", Arc::new(Identity))?;
    let context = session.query_context().await?;
    let snapshot = &context.catalog.database("xtdb")?.partitions[0];
    if progress.source_position != SourcePosition(heads.source) || status.replica_position < ReplicaPosition(heads.replica)
        || key.system_time != LAST_PREFIX_TIME || key.tx_id.0 < 0 || key.tx_id.0 > heads.source
        || snapshot.tx_basis() != Some(key) || adopted.latest_completed_tx() != Some(key)
        || adopted.block_index().0.checked_add(1) != Some(status.open_block.0)
    {
        return Err("Native startup did not recover the protected JVM tick-39 basis".into());
    }
    Ok(json!({"archive_source_position":heads.source, "archive_replica_position":heads.replica,
              "source_position":progress.source_position.0, "replica_position":status.replica_position.0,
              "tx_id":key.tx_id.0, "system_time_micros":key.system_time.0,
              "adopted_block":adopted.block_index().0, "open_block":status.open_block.0}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_paths_preserve_log_names_and_object_keys() {
        assert_eq!(destination("control/log/LOG").unwrap(), "mixed-iid-control/log/LOG");
        assert_eq!(destination("mixed/log/REPLICA_LOG").unwrap(), "mixed-iid-mixed/log/REPLICA_LOG");
        assert_eq!(destination("mixed/objects/v06/blocks/b00.binpb").unwrap(), "mixed-iid-mixed/storage/v06/blocks/b00.binpb");
        for path in ["mixed/objects/v07/key", "mixed/objects/v06/../key", "mixed/storage/v06/key", "mixed/log/LOCK", "mixed//log/LOG"] {
            assert!(destination(path).is_err(), "{path}");
        }
    }

    #[test]
    fn validates_arrow_rewrites_from_each_compacted_table_path() {
        use super::super::{ArrowSchema, DataType, RecordBatch, rewrite, write_file};
        use arrow_schema::Field;
        use rtdb_columnar::build_iids;
        use rtdb_format::keys::TrieKey;
        use rtdb_types::{BlockIndex, Iid};

        let directory = tempfile::tempdir().unwrap();
        let key = TrieKey::new(1, None, Vec::new(), BlockIndex(0)).unwrap().to_string();
        let schema = Arc::new(ArrowSchema::new(vec![Field::new("_iid", DataType::FixedSizeBinary(16), false)]));
        let page = RecordBatch::try_new(schema.clone(), vec![build_iids(&[Iid([19; 16]), Iid([19; 16])], None).unwrap()])
            .unwrap();
        let plain = write_file(&schema, &[page]).unwrap();
        let archived_rewrite = |table_directory: &str| {
            let source = format!("mixed/objects/v06/tables/{table_directory}/{key}.arrow");
            let copied = directory.path().join(destination(&source).unwrap());
            fs::create_dir_all(copied.parent().unwrap()).unwrap();
            fs::write(&copied, &plain).unwrap();
            let (before, after) = rewrite(&copied).unwrap();
            let mut before = json!(before);
            let mut after = json!(after);
            let archive_path = format!("{JVM_CASE_DIRECTORY}{source}");
            before["path"] = json!(archive_path);
            after["path"] = json!(archive_path);
            Rewrite { before, after, result: json!({"rows":2, "runs":1}) }
        };
        let readings = archived_rewrite("public$readings/data");
        let transactions = archived_rewrite("xt$txs/data");
        let digests = [readings.after["sha256"].clone(), transactions.after["sha256"].clone()];
        let verified = verify_rewrites(directory.path(), vec![readings, transactions]).unwrap();
        for (entry, (slug, digest)) in verified.iter().zip(["public$readings", "xt$txs"].into_iter().zip(digests)) {
            assert_eq!(entry["path"], format!("storage/v06/tables/{slug}/data/{key}.arrow"));
            assert_eq!(entry["trie_key"], key);
            assert_eq!(entry["after"]["sha256"], digest);
        }

        let metadata = archived_rewrite("xt$txs/meta");
        let error = verify_rewrites(directory.path(), vec![metadata]).unwrap_err();
        assert!(error.to_string().contains("outside compacted mixed table data files"));
    }

    #[test]
    fn copy_validation_rejects_changed_bytes_before_open() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("LOG");
        fs::write(&file, b"present").unwrap();
        let expected = ArchivedFile { path: "control/log/LOG".into(), bytes: 7, sha256: format!("{:x}", Sha256::digest(b"present")) };
        verify_file(&file, &expected).unwrap();
        fs::write(&file, b"changed").unwrap();
        assert!(verify_file(&file, &expected).unwrap_err().to_string().contains("bytes differ"));
    }
}
