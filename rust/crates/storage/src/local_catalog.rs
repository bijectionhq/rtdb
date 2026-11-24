use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fs2::FileExt;
use rtdb_format::{catalog::same_publication_digest, keys::{is_catalog_retirement_file, is_compacted_primary_file}, search_manifest::is_ranged_run};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, CATALOG_FILE_PATH_BYTES, CatalogCut, CatalogFilePaths, ReplicaPosition, Result, StorageEpoch};

use crate::{Error, repair_anchor_decision::{self, AnchorInstallation, RecoveryCollection}};

#[derive(Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CatalogHold {
    pub(crate) cut: Option<CatalogCut>,
    catalog_file_paths: Option<CatalogFilePaths>,
}

impl CatalogHold {
    pub(super) fn new(cut: Option<CatalogCut>, paths: Option<&[String]>) -> Self {
        let catalog_file_paths = paths.filter(|paths| {
            cut.is_some() && paths.windows(2).all(|pair| pair[0] < pair[1])
                && paths.iter().all(|path| is_catalog_retirement_file(path))
                && paths.iter().fold(0usize, |charge, path| charge.saturating_add(path.len().saturating_mul(6).saturating_add(16)))
                    <= CATALOG_FILE_PATH_BYTES
        }).map(|paths| CatalogFilePaths { members: paths.to_vec() });
        Self { cut, catalog_file_paths }
    }

    pub(super) fn retains(&self, cut: CatalogCut, path: Option<&str>) -> bool {
        match path {
            Some(path) => self.catalog_file_paths.as_ref().is_none_or(|paths| {
                paths.binary_search_by(|held| held.as_str().cmp(path)).is_ok()
            }),
            None => self.cut.is_none_or(|held| !covers(held, cut)),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalRepairAnchor {
    pub block: BlockIndex,
    pub boundary: ReplicaPosition,
    pub sha256: [u8; 32],
}

impl LocalRepairAnchor {
    fn well_formed(self) -> bool { repair_anchor_decision::valid_anchor(self.block.0, self.boundary.0) }
}

fn terminal_name(key: &str) -> bool {
    is_compacted_primary_file(key) || is_ranged_run(key)
}

pub(crate) struct LocalCatalogLedger {
    directory: PathBuf,
    root: Arc<PathBuf>,
    replica_epoch: StorageEpoch,
    search_witness: parking_lot::Mutex<Option<(uuid::Uuid, File)>>,
}

impl LocalCatalogLedger {
    pub(crate) fn new(root: Arc<PathBuf>, replica_epoch: StorageEpoch) -> Self {
        Self { directory: root.join(".tmp/catalog-ledger"), root, replica_epoch,
            search_witness: parking_lot::Mutex::new(None) }
    }

    pub(crate) fn retain(&self, cut: Option<CatalogCut>, paths: Option<&[String]>) -> Result<tempfile::NamedTempFile> {
        let _gate = self.lock()?;
        let claim = CatalogHold::new(cut, paths);
        if cut.is_some_and(|cut| cut.replica_epoch != self.replica_epoch || !well_formed(cut))
            || self.floor()?.is_some_and(|floor| floor.replica_epoch >= self.replica_epoch
                && !cut.is_some_and(|cut| covers(cut, floor)))
        {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.storage/catalog-not-current",
                "The captured catalog precedes the local collection floor or belongs to another journal"));
        }
        if let Some(paths) = &claim.catalog_file_paths {
            for name in paths.iter().filter(|name| terminal_name(name)) {
                if self.is_terminal(name)? {
                    return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.storage/catalog-not-current",
                        "The captured catalog names a terminal local publication"));
                }
            }
        }
        let mut ticket = tempfile::Builder::new().prefix("reader-").tempfile_in(&self.directory)
            .map_err(|error| Error::io(&self.directory, error))?;
        fs2::FileExt::lock_shared(ticket.as_file()).map_err(|error| Error::io(ticket.path(), error))?;
        ticket.write_all(&serde_json::to_vec(&claim).map_err(invalid)?)
            .map_err(|error| Error::io(ticket.path(), error))?;
        Ok(ticket)
    }

    pub(crate) fn close_admission(&self, cut: CatalogCut, paths: &[Option<&str>]) -> Result<Vec<bool>> {
        let _gate = self.lock()?;
        if cut.replica_epoch != self.replica_epoch || !well_formed(cut) || cut.through.after_offset(cut.replica_epoch) < 0
            || paths.iter().flatten().any(|path| !terminal_name(path))
        {
            return Err(invalid("Invalid local catalog retirement"));
        }
        let prior = self.floor()?;
        let closed = prior.is_some_and(|prior| covers(prior, cut));
        if !closed && prior.is_some_and(|prior| {
            !covers(cut, prior) && !(same_storage(cut, prior) && cut.replica_epoch > prior.replica_epoch)
        }) {
            return Err(invalid("The local catalog floor belongs to a different storage root or newer journal"));
        }
        let holds = self.held_catalogs()?;
        let advance = !closed && (paths.iter().any(Option::is_some)
            || holds.iter().all(|hold| !hold.retains(cut, None)));
        if advance {
            let mut next = tempfile::NamedTempFile::new_in(&self.directory).map_err(|error| Error::io(&self.directory, error))?;
            next.write_all(&serde_json::to_vec(&cut).map_err(invalid)?).map_err(|error| Error::io(next.path(), error))?;
            crate::sync_file(next.as_file()).map_err(|error| Error::io(next.path(), error))?;
            let path = self.directory.join("floor");
            next.persist(&path).map_err(|error| Error::io(&path, error.error))?;
        }
        if !closed && !advance { return Ok(vec![false; paths.len()]); }
        self.sync_ledger()?;
        let mut allowed = Vec::with_capacity(paths.len());
        for path in paths {
            let release = holds.iter().all(|hold| !hold.retains(cut, *path));
            if release && let Some(path) = path { self.close_publication(path)?; }
            allowed.push(release);
        }
        Ok(allowed)
    }

    fn held_catalogs(&self) -> Result<Vec<CatalogHold>> {
        let mut holds = Vec::new();
        for entry in fs::read_dir(&self.directory).map_err(|error| Error::io(&self.directory, error))? {
            let path = entry.map_err(|error| Error::io(&self.directory, error))?.path();
            if !path.file_name().and_then(|name| name.to_str()).is_some_and(|name| name.starts_with("reader-")) { continue; }
            let mut ticket = match File::open(&path) {
                Ok(file) => file,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(error) => return Err(Error::io(&path, error).into()),
            };
            match FileExt::try_lock_exclusive(&ticket) {
                Ok(()) => remove_ticket(&path)?,
                Err(error) if error.kind() == fs2::lock_contended_error().kind() => {
                    let mut bytes = Vec::new();
                    Read::by_ref(&mut ticket).take((CATALOG_FILE_PATH_BYTES + 1024) as u64)
                        .read_to_end(&mut bytes).map_err(|error| Error::io(&path, error))?;
                    holds.push(serde_json::from_slice(&bytes).map_err(invalid)?);
                }
                Err(error) => return Err(Error::io(&path, error).into()),
            }
        }
        Ok(holds)
    }

    /// The same gate serializes immutable creation and terminal retirement.
    pub(crate) fn publication_gate(&self, key: &crate::ObjectKey) -> crate::Result<Option<File>> {
        if !terminal_name(key.as_str()) { return Ok(None); }
        let gate = self.lock()?;
        if self.is_terminal(key.as_str())? { return Err(Error::ConflictingObject(key.to_string())); }
        Ok(Some(gate))
    }

    fn is_terminal(&self, key: &str) -> crate::Result<bool> {
        let terminal = self.terminal_path(key);
        match fs::metadata(&terminal) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(Error::io(&terminal, error)),
        }
    }

    fn terminal_path(&self, key: &str) -> PathBuf {
        let digest = ring::digest::digest(&ring::digest::SHA256, key.as_bytes());
        let name: String = digest.as_ref().iter().map(|byte| format!("{byte:02x}")).collect();
        self.directory.join("terminal").join(name)
    }

    fn close_publication(&self, key: &str) -> Result<()> {
        let path = self.terminal_path(key);
        let parent = path.parent().ok_or_else(|| invalid("Local terminal publication has no parent"))?;
        fs::create_dir_all(parent).map_err(|error| Error::io(parent, error))?;
        let file = OpenOptions::new().write(true).create(true).truncate(false).open(&path)
            .map_err(|error| Error::io(&path, error))?;
        crate::sync_file(&file).map_err(|error| Error::io(&path, error))?;
        crate::sync_directory(parent).map_err(|error| Error::io(parent, error))?;
        self.sync_ledger()
    }

    pub(crate) fn search_attachment(&self) -> crate::Result<uuid::Uuid> {
        let mut live = self.search_witness.lock();
        if let Some((attachment, _)) = live.as_ref() { return Ok(*attachment); }
        let _gate = self.lock()?;
        let attachment = uuid::Uuid::new_v4();
        let path = self.directory.join(format!("search-writer-{attachment}"));
        let file = OpenOptions::new().write(true).read(true).create_new(true).open(&path)
            .map_err(|error| Error::io(&path, error))?;
        FileExt::lock_exclusive(&file).map_err(|error| Error::io(&path, error))?;
        crate::sync_file(&file).map_err(|error| Error::io(&path, error))?;
        crate::sync_directory(&self.directory).map_err(|error| Error::io(&self.directory, error))?;
        *live = Some((attachment, file));
        Ok(attachment)
    }

    pub(crate) fn search_drained(&self, attachment: uuid::Uuid) -> crate::Result<bool> {
        let path = self.directory.join(format!("search-writer-{attachment}"));
        let file = match OpenOptions::new().write(true).read(true).open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(error) => return Err(Error::io(&path, error)),
        };
        match FileExt::try_lock_exclusive(&file) {
            Ok(()) => Ok(true),
            Err(error) if error.kind() == fs2::lock_contended_error().kind() => Ok(false),
            Err(error) => Err(Error::io(path, error)),
        }
    }

    pub(crate) fn install_repair_anchor(&self, next: LocalRepairAnchor) -> Result<()> {
        let _gate = self.lock()?;
        if !next.well_formed() {
            return Err(invalid("Invalid local repair anchor"));
        }
        let prior = self.repair_anchor()?;
        let decision = repair_anchor_decision::install_anchor(
            prior.map(|anchor| (anchor.block.0, anchor.boundary.0)), next.block.0, next.boundary.0,
            prior.is_some_and(|anchor| same_publication_digest(anchor.sha256, next.sha256)),
        );
        match decision {
            AnchorInstallation::Replace => (),
            AnchorInstallation::Confirm => return self.sync_ledger(),
            AnchorInstallation::Refuse => return Err(invalid("The repair anchor must advance in the same replica journal")),
        }

        let mut staged = tempfile::NamedTempFile::new_in(&self.directory)
            .map_err(|error| Error::io(&self.directory, error))?;
        staged.write_all(&serde_json::to_vec(&(next.block, next.boundary, next.sha256)).map_err(invalid)?)
            .map_err(|error| Error::io(staged.path(), error))?;
        crate::sync_file(staged.as_file()).map_err(|error| Error::io(staged.path(), error))?;
        let path = self.directory.join("repair-anchor");
        staged.persist(&path).map_err(|error| Error::io(&path, error.error))?;
        self.sync_ledger()
    }

    pub(crate) fn hold_recovery_open(&self) -> Result<File> {
        let path = self.directory.join("recovery-open");
        let gate = self.open_gate(&path)?;
        FileExt::lock_shared(&gate).map_err(|error| Error::io(path, error))?;
        Ok(gate)
    }

    pub(crate) fn try_lock_recovery_deletion(&self) -> Result<Option<File>> {
        let path = self.directory.join("recovery-open");
        let gate = self.open_gate(&path)?;
        match FileExt::try_lock_exclusive(&gate) {
            Ok(()) => Ok(Some(gate)),
            Err(error) if error.kind() == fs2::lock_contended_error().kind() => Ok(None),
            Err(error) => Err(Error::io(path, error).into()),
        }
    }

    pub(crate) fn collect_recovery_file(&self, block: BlockIndex, remove: impl FnOnce() -> crate::Result<()>) -> Result<()> {
        let Some(_recovery) = self.try_lock_recovery_deletion()? else { return Ok(()); };
        let _gate = self.lock()?;
        let first = self.repair_anchor()?.map(|anchor| anchor.block.0);
        match repair_anchor_decision::collect_recovery_file(first, block.0) {
            RecoveryCollection::Keep => return Ok(()),
            RecoveryCollection::SynchronizeAndRemove => self.sync_ledger()?,
            RecoveryCollection::Remove => (),
        }
        Ok(remove()?)
    }

    pub(crate) fn retained_repair_anchor(&self) -> Result<Option<LocalRepairAnchor>> {
        let _gate = self.lock()?;
        let anchor = self.repair_anchor()?;
        if anchor.is_some() { self.sync_ledger()?; }
        Ok(anchor)
    }

    fn repair_anchor(&self) -> Result<Option<LocalRepairAnchor>> {
        let path = self.directory.join("repair-anchor");
        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(Error::io(path, error).into()),
        };
        let mut bytes = Vec::new();
        Read::by_ref(&mut file).take(1025).read_to_end(&mut bytes).map_err(|error| Error::io(&path, error))?;
        if bytes.len() > 1024 { return Err(invalid("Local repair anchor exceeds its format bound")); }
        let (block, boundary, sha256) = serde_json::from_slice(&bytes).map_err(invalid)?;
        let anchor = LocalRepairAnchor { block, boundary, sha256 };
        if !anchor.well_formed() { return Err(invalid("Malformed local repair anchor")); }
        Ok(Some(anchor))
    }

    fn sync_ledger(&self) -> Result<()> {
        for directory in [&self.directory, &self.root.join(".tmp"), self.root.as_ref()] {
            crate::sync_directory(directory).map_err(|error| Error::io(directory, error))?;
        }
        Ok(())
    }

    fn lock(&self) -> crate::Result<File> {
        let path = self.directory.join("lock");
        let file = self.open_gate(&path)?;
        FileExt::lock_exclusive(&file).map_err(|error| Error::io(path, error))?;
        Ok(file)
    }

    fn open_gate(&self, path: &Path) -> crate::Result<File> {
        fs::create_dir_all(&self.directory).map_err(|error| Error::io(&self.directory, error))?;
        Ok(OpenOptions::new().read(true).write(true).create(true).truncate(false)
            .open(path).map_err(|error| Error::io(path, error))?)
    }

    fn floor(&self) -> Result<Option<CatalogCut>> {
        let path = self.directory.join("floor");
        match fs::read(&path) {
            Ok(bytes) => {
                let cut = serde_json::from_slice(&bytes).map_err(invalid)?;
                if !well_formed(cut) { return Err(invalid("Malformed local catalog floor")); }
                Ok(Some(cut))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(Error::io(path, error).into()),
        }
    }
}

fn remove_ticket(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(Error::io(path, error).into()),
    }
}

pub(crate) fn well_formed(cut: CatalogCut) -> bool {
    cut.through.after_offset(cut.replica_epoch) >= 0
        || ReplicaPosition::from_epoch_offset(cut.replica_epoch, -1).ok() == Some(cut.through)
}

pub(crate) fn same_storage(left: CatalogCut, right: CatalogCut) -> bool {
    left.storage_version == right.storage_version && left.storage_epoch == right.storage_epoch
}

pub(crate) fn covers(reader: CatalogCut, floor: CatalogCut) -> bool {
    well_formed(reader) && same_storage(reader, floor) && reader.replica_epoch == floor.replica_epoch
        && reader.through >= floor.through
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::fault("rtdb.storage/catalog-ledger", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::keys::{CURRENT_STORAGE_VERSION, TableSlug, TrieKey};

    #[test]
    fn exact_local_close_preserves_only_the_primary_named_by_a_held_catalog() {
        let root = tempfile::tempdir().unwrap();
        let epoch = StorageEpoch(0);
        let ledger = LocalCatalogLedger::new(Arc::new(root.path().to_owned()), epoch);
        let cut = |offset| CatalogCut {
            storage_version: CURRENT_STORAGE_VERSION,
            storage_epoch: epoch,
            replica_epoch: epoch,
            through: ReplicaPosition::from_epoch_offset(epoch, offset).unwrap(),
        };
        let name = |block| TableSlug::recorded("public$docs").unwrap()
            .data_key(&TrieKey::new(1, None, Vec::new(), BlockIndex(block)).unwrap());
        let held = name(2);
        let unrelated = name(3);
        let registration = ledger.retain(Some(cut(7)), Some(std::slice::from_ref(&held))).unwrap();
        assert_eq!(ledger.close_admission(cut(8), &[Some(&held), Some(&unrelated), None]).unwrap(), [false, true, false]);
        assert!(ledger.retain(Some(cut(9)), Some(std::slice::from_ref(&unrelated))).is_err());
        assert!(ledger.publication_gate(&crate::ObjectKey::new(unrelated).unwrap()).is_err());
        drop(registration);
        assert_eq!(ledger.close_admission(cut(8), &[Some(&held)]).unwrap(), [true]);
    }
}
