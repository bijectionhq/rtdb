use super::{AuthoredPrefix, Error, SOURCE_SHA256, checkpoint::{CASE, DigestWriter, LAST_PREFIX_TIME, MAX_BYTES, MAX_ENTRIES, read_log_tip}, layout_directory, physical_root};
use rtdb_format::keys::{CURRENT_STORAGE_VERSION, block_index_from_key};
use serde::Serialize;
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs::{self, File}, io::Read, path::Path};

const STAGE: &str = "after-rewrite-before-reopen";
const MAX_DEPTH: usize = 16;

#[derive(Serialize)]
struct ArchivedFile {
    path: String,
    bytes: u64,
    sha256: String,
}

#[derive(Default)]
struct Extent {
    entries: usize,
    bytes: u64,
}

fn visit(
    root: &Path, path: &Path, depth: usize, extent: &mut Extent, found: &mut BTreeMap<String, ArchivedFile>,
) -> Result<(), Error> {
    if depth > MAX_DEPTH || extent.entries == MAX_ENTRIES {
        return Err("Native checkpoint exceeds its inventory limit".into());
    }
    extent.entries += 1;
    let metadata = fs::symlink_metadata(path)?;
    if metadata.is_dir() {
        for entry in fs::read_dir(path)? {
            visit(root, &entry?.path(), depth + 1, extent, found)?;
        }
        return Ok(());
    }
    if !metadata.is_file() {
        return Err("Native checkpoint contains a redirected or non-regular file".into());
    }
    let relative = path.strip_prefix(root)?.iter().map(|part| {
        let part = part.to_str().ok_or("Native checkpoint path is not UTF-8")?;
        if part.contains('\\') { return Err("Native checkpoint path contains a backslash"); }
        Ok(part)
    }).collect::<Result<Vec<_>, _>>()?.join("/");
    let bytes = metadata.len();
    extent.bytes = extent.bytes.checked_add(bytes).ok_or("Native checkpoint size overflow")?;
    if extent.bytes > MAX_BYTES { return Err("Native checkpoint exceeds its byte limit".into()); }
    let entry = ArchivedFile { path: relative.clone(), bytes, sha256: digest_extent(path, bytes)? };
    found.insert(relative, entry);
    Ok(())
}

fn digest_extent(path: &Path, bytes: u64) -> Result<String, Error> {
    let mut digest = Sha256::new();
    let mut input = File::open(path)?.take(bytes + 1);
    if std::io::copy(&mut input, &mut DigestWriter(&mut digest))? != bytes {
        return Err("Native checkpoint file changed length during capture".into());
    }
    Ok(format!("{:x}", digest.finalize()))
}

pub(super) fn bounded_artifacts(root: &Path) -> Result<Vec<Json>, Error> {
    let mut found = BTreeMap::new();
    visit(root, root, 0, &mut Extent::default(), &mut found)?;
    Ok(found.into_values().map(|entry| json!(entry)).collect())
}

fn inventory(root: &Path, storage: &Path, extent: &mut Extent) -> Result<BTreeMap<String, ArchivedFile>, Error> {
    let mut found = BTreeMap::new();
    let log = root.join("log");
    let objects = root.join(storage);
    for directory in [&log, &objects] {
        if !fs::symlink_metadata(directory)?.is_dir() || directory.canonicalize()? != *directory {
            return Err("Native checkpoint subtree is absent or redirected".into());
        }
        let depth = directory.strip_prefix(root)?.components().count();
        visit(root, directory, depth, extent, &mut found)?;
    }
    let logs = found.keys().filter(|path| path.starts_with("log/")).map(String::as_str);
    if !logs.eq(["log/LOG", "log/REPLICA_LOG"]) ||
        ["log/LOG", "log/REPLICA_LOG"].iter().any(|name| found[*name].bytes == 0)
    {
        return Err("Native checkpoint requires both nonempty local logs".into());
    }
    let block_root = storage.join("blocks");
    if !found.values().any(|entry| entry.bytes > 0 && Path::new(&entry.path).parent() == Some(block_root.as_path())
        && Path::new(&entry.path).file_name().and_then(|name| name.to_str()).is_some_and(|name| block_index_from_key(name).is_ok()))
    {
        return Err("Native checkpoint has no nonempty block catalog".into());
    }
    Ok(found)
}

fn verify_rewrites(files: &BTreeMap<String, ArchivedFile>, rewritten: &[Json]) -> Result<(), Error> {
    for rewrite in rewritten {
        let path = rewrite["path"].as_str().ok_or("Native rewrite omitted its data path")?;
        let file = files.get(path).ok_or("Native checkpoint omitted a rewritten data file")?;
        let after = &rewrite["after"];
        if after["bytes"] != file.bytes || after["sha256"] != file.sha256.as_str() {
            return Err("Native checkpoint differs from its rewritten Arrow file".into());
        }
    }
    Ok(())
}

fn checked_log_tip(root: &Path, name: &str, archived: &ArchivedFile) -> Result<i64, Error> {
    let tip = read_log_tip(root, name)?;
    let path = root.join(&archived.path);
    let file = fs::symlink_metadata(&path)?;
    if !file.is_file() || file.len() != archived.bytes || digest_extent(&path, archived.bytes)? != archived.sha256 {
        return Err("Native checkpoint log changed while measuring its tip".into());
    }
    Ok(tip)
}

fn side(root: &Path, ticks: &[Json], files: BTreeMap<String, ArchivedFile>) -> Result<Json, Error> {
    let source = checked_log_tip(root, "LOG", &files["log/LOG"])?;
    let replica = checked_log_tip(root, "REPLICA_LOG", &files["log/REPLICA_LOG"])?;
    let last = ticks.last().ok_or("Native checkpoint has no transaction witness")?;
    let id = last["tx_id"].as_i64().ok_or("Native checkpoint has no transaction ID")?;
    if ticks.len() != 40 || last["tick"] != 39 || last["system_time_micros"] != LAST_PREFIX_TIME.0
        || replica < 0 || !(0..=source).contains(&id)
    {
        return Err("Native checkpoint did not reach the archived tick-39 boundary".into());
    }
    let directory = root.file_name().and_then(|name| name.to_str()).ok_or("Invalid native checkpoint directory name")?;
    Ok(json!({"directory":directory,"files":files.into_values().collect::<Vec<_>>(),
        "log_heads":{"source":source,"replica":replica},"first_ticks":ticks}))
}

pub(super) fn manifest(output: &Path, prefix: &AuthoredPrefix) -> Result<Json, Error> {
    let control = layout_directory(output, "control");
    let mixed = layout_directory(output, "mixed");
    let storage = physical_root()?;
    let mut extent = Extent::default();
    let control_files = inventory(&control, &storage, &mut extent)?;
    let mixed_files = inventory(&mixed, &storage, &mut extent)?;
    verify_rewrites(&mixed_files, &prefix.rewritten)?;
    let control = side(&control, &prefix.ticks[0], control_files)?;
    let mixed = side(&mixed, &prefix.ticks[1], mixed_files)?;
    Ok(json!({"schema_version":1,"engine":"rust","mode":"closed-checkpoint","stage":STAGE,"closed":true,
        "case":CASE,"source_sha256":SOURCE_SHA256,"execution_complete":true,"complete_parity":false,
        "binary_sha256":crate::controls::digest_file(&std::env::current_exe()?)?,
        "artifact_revision":env!("RTDB_CONFORMANCE_REVISION"),"source_dirty":env!("RTDB_CONFORMANCE_DIRTY")!="false",
        "storage_version":CURRENT_STORAGE_VERSION.0,"storage_epoch":0,"partitions":1,
        "control":control,"mixed":mixed,"rewritten_files":prefix.rewritten}))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap().join("mixed-iid-mixed");
        let storage = physical_root().unwrap();
        fs::create_dir_all(root.join("log")).unwrap();
        fs::create_dir_all(root.join(storage.join("blocks"))).unwrap();
        for name in ["LOG", "REPLICA_LOG"] {
            fs::write(root.join("log").join(name), [1, 2]).unwrap();
        }
        fs::write(root.join(storage.join(rtdb_format::keys::block_key(rtdb_types::BlockIndex(0)).unwrap())), [3]).unwrap();
        (directory, root, storage)
    }

    #[test]
    fn native_checkpoint_census_binds_rewrite_postimages_by_full_relative_path() {
        let (_directory, root, storage) = fixture();
        let relative = storage.join("tables/readings/data/l01-r0000.arrow");
        fs::create_dir_all(root.join(relative.parent().unwrap())).unwrap();
        fs::write(root.join(&relative), [4, 5, 6]).unwrap();
        let relative = relative.iter().map(|part| part.to_str().unwrap()).collect::<Vec<_>>().join("/");
        let relative = relative.as_str();
        let files = inventory(&root, &storage, &mut Extent::default()).unwrap();
        let mut rewrite = json!({"path":relative,"after":{"bytes":3,"sha256":files[relative].sha256}});
        verify_rewrites(&files, std::slice::from_ref(&rewrite)).unwrap();
        fs::write(root.join(relative), [4, 5, 7]).unwrap();
        let changed = inventory(&root, &storage, &mut Extent::default()).unwrap();
        assert!(verify_rewrites(&changed, std::slice::from_ref(&rewrite)).is_err());
        rewrite["path"] = json!("storage/v06/tables/other/data/l01-r0000.arrow");
        assert!(verify_rewrites(&files, &[rewrite]).is_err());
    }

    #[test]
    fn native_checkpoint_census_rejects_incomplete_logs_and_admits_size_before_digest() {
        let (_directory, root, storage) = fixture();
        let log = root.join("log/REPLICA_LOG");
        fs::remove_file(&log).unwrap();
        let error = inventory(&root, &storage, &mut Extent::default()).err().unwrap();
        assert_eq!(error.to_string(), "Native checkpoint requires both nonempty local logs");
        let file = File::create(log).unwrap();
        file.set_len(MAX_BYTES + 1).unwrap();
        let error = inventory(&root, &storage, &mut Extent::default()).err().unwrap();
        assert_eq!(error.to_string(), "Native checkpoint exceeds its byte limit");
        let error = bounded_artifacts(&root).unwrap_err();
        assert_eq!(error.to_string(), "Native checkpoint exceeds its byte limit");
    }

    #[tokio::test]
    async fn native_checkpoint_tip_requires_the_censused_durable_log_bytes() {
        use rtdb_storage::log::{LocalLog, LocalLogOptions, Log};
        let (_directory, root, storage) = fixture();
        let path = root.join("log/LOG");
        fs::remove_file(&path).unwrap();
        let (log, driver) = LocalLog::open(&path, LocalLogOptions {
            maximum_message_bytes: 64,
            queue_capacity: 2,
        }).unwrap();
        let stop = tokio_util::sync::CancellationToken::new();
        let owner = tokio::spawn(driver.run(stop.clone()));
        log.enqueue(bytes::Bytes::from_static(b"original"), rtdb_types::Timestamp(0)).await.unwrap()
            .durable().await.unwrap();
        stop.cancel();
        owner.await.unwrap().unwrap();
        log.close_and_wait().await;
        drop(log);
        let files = inventory(&root, &storage, &mut Extent::default()).unwrap();
        let entry = &files["log/LOG"];
        assert_eq!(checked_log_tip(&root, "LOG", entry).unwrap(), 0);

        let mut replacement = fs::read(&path).unwrap();
        replacement[13] = b'x';
        fs::write(path, replacement).unwrap();
        assert_eq!(read_log_tip(&root, "LOG").unwrap(), 0);
        let error = checked_log_tip(&root, "LOG", entry).unwrap_err();
        assert_eq!(error.to_string(), "Native checkpoint log changed while measuring its tip");
    }

    #[cfg(unix)]
    #[test]
    fn native_checkpoint_census_rejects_symlinks_in_object_trees() {
        let (_directory, root, storage) = fixture();
        std::os::unix::fs::symlink(root.join("log/LOG"), root.join(storage.join("redirected"))).unwrap();
        let error = inventory(&root, &storage, &mut Extent::default()).err().unwrap();
        assert_eq!(error.to_string(), "Native checkpoint contains a redirected or non-regular file");
    }
}
