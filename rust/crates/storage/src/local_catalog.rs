use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fs2::FileExt;
use rtdb_format::{catalog::same_publication_digest, keys::is_compacted_primary_file, search_manifest::is_ranged_run};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, CatalogCut, ReplicaPosition, Result, StorageEpoch};

use crate::{AppleSync, Error, repair_anchor_decision::{self, AnchorInstallation, RecoveryCollection}};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalRepairAnchor {
    pub block: BlockIndex,
    pub boundary: ReplicaPosition,
    pub sha256: [u8; 32],
    pub primary_inventory_sha256: [u8; 32],
}

impl LocalRepairAnchor {
    fn well_formed(self) -> bool { repair_anchor_decision::valid_anchor(self.block.0, self.boundary.0) }
}

fn terminal_name(key: &str) -> bool {
    is_compacted_primary_file(key) || is_ranged_run(key)
}

/// The retirement and erasure ledger of one local storage root, shared by every process on that filesystem.
/// Readers register nothing here: a superseded file stays readable for the garbage-collection window, and only an
/// erasure installs a floor below which a captured catalog is refused.
pub(crate) struct LocalCatalogLedger {
    directory: PathBuf,
    root: Arc<PathBuf>,
    replica_epoch: StorageEpoch,
    apple_sync: AppleSync,
    search_witness: parking_lot::Mutex<Option<(uuid::Uuid, File)>>,
}

impl LocalCatalogLedger {
    pub(crate) fn new(root: Arc<PathBuf>, replica_epoch: StorageEpoch, apple_sync: AppleSync) -> Self {
        Self { directory: root.join(".tmp/catalog-ledger"), root, replica_epoch, apple_sync,
            search_witness: parking_lot::Mutex::new(None) }
    }

    pub(crate) fn with_apple_sync(&self, apple_sync: AppleSync) -> Self {
        Self::new(Arc::clone(&self.root), self.replica_epoch, apple_sync)
    }

    /// Records an installed retirement and closes the publication of each exact path it names, so a retired name is never
    /// published again. No reader holds a catalog here, so every candidate is released.
    pub(crate) fn close_admission(&self, cut: CatalogCut, paths: &[Option<&str>]) -> Result<Vec<bool>> {
        let _gate = self.lock()?;
        if paths.iter().flatten().any(|path| !terminal_name(path)) {
            return Err(invalid("Invalid local catalog retirement"));
        }
        self.check_cut(cut)?;
        for path in paths.iter().flatten() { self.close_publication(path)?; }
        Ok(vec![true; paths.len()])
    }

    /// Installs the erasure floor: a catalog captured before `cut` is refused from now on.
    pub(crate) fn close_erasure_admission(&self, cut: CatalogCut) -> Result<()> {
        let _gate = self.lock()?;
        if !self.covers_floor(cut)? { self.install_floor(cut)?; }
        self.sync_ledger()
    }

    pub(crate) fn require_erasure_admission(&self, cut: Option<CatalogCut>) -> Result<()> {
        let _gate = self.lock()?;
        if self.floor()?.is_some_and(|floor| !cut.is_some_and(|cut| covers(cut, floor))) {
            return Err(Anomaly::new(AnomalyCategory::NotFound, "rtdb/erased-under-view",
                "The captured catalog precedes the Local erasure floor"));
        }
        Ok(())
    }

    pub(crate) fn verifies_erasure_admission(&self, cut: CatalogCut) -> Result<bool> {
        let _gate = self.lock()?;
        self.covers_floor(cut)
    }

    fn check_cut(&self, cut: CatalogCut) -> Result<()> {
        if cut.replica_epoch != self.replica_epoch || !well_formed(cut)
            || cut.through.after_offset(cut.replica_epoch) < 0
        {
            return Err(invalid("Invalid local catalog retirement"));
        }
        Ok(())
    }

    fn covers_floor(&self, cut: CatalogCut) -> Result<bool> {
        self.check_cut(cut)?;
        let prior = self.floor()?;
        let closed = prior.is_some_and(|prior| covers(prior, cut));
        if !closed && prior.is_some_and(|prior| {
            !covers(cut, prior) && !(same_storage(cut, prior) && cut.replica_epoch > prior.replica_epoch)
        }) {
            return Err(invalid("The local catalog floor belongs to a different storage root or newer journal"));
        }
        Ok(closed)
    }

    fn install_floor(&self, cut: CatalogCut) -> Result<()> {
        let mut next = tempfile::NamedTempFile::new_in(&self.directory).map_err(|error| Error::io(&self.directory, error))?;
        next.write_all(&serde_json::to_vec(&cut).map_err(invalid)?).map_err(|error| Error::io(next.path(), error))?;
        crate::sync_file(next.as_file(), self.apple_sync).map_err(|error| Error::io(next.path(), error))?;
        let path = self.directory.join("floor");
        next.persist(&path).map_err(|error| Error::io(&path, error.error))?;
        Ok(())
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
        crate::sync_file(&file, self.apple_sync).map_err(|error| Error::io(&path, error))?;
        crate::sync_directory(parent, self.apple_sync).map_err(|error| Error::io(parent, error))?;
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
        crate::sync_file(&file, self.apple_sync).map_err(|error| Error::io(&path, error))?;
        crate::sync_directory(&self.directory, self.apple_sync).map_err(|error| Error::io(&self.directory, error))?;
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
            prior.is_some_and(|anchor| same_publication_digest(anchor.sha256, next.sha256)
                && same_publication_digest(anchor.primary_inventory_sha256, next.primary_inventory_sha256)),
        );
        match decision {
            AnchorInstallation::Replace => (),
            AnchorInstallation::Confirm => return self.sync_ledger(),
            AnchorInstallation::Refuse => return Err(invalid("The repair anchor must advance in the same replica journal")),
        }

        let mut staged = tempfile::NamedTempFile::new_in(&self.directory)
            .map_err(|error| Error::io(&self.directory, error))?;
        staged.write_all(&serde_json::to_vec(&(next.block, next.boundary, next.sha256, next.primary_inventory_sha256)).map_err(invalid)?)
            .map_err(|error| Error::io(staged.path(), error))?;
        crate::sync_file(staged.as_file(), self.apple_sync).map_err(|error| Error::io(staged.path(), error))?;
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
        let (block, boundary, sha256, primary_inventory_sha256) = serde_json::from_slice(&bytes).map_err(invalid)?;
        let anchor = LocalRepairAnchor { block, boundary, sha256, primary_inventory_sha256 };
        if !anchor.well_formed() { return Err(invalid("Malformed local repair anchor")); }
        Ok(Some(anchor))
    }

    fn sync_ledger(&self) -> Result<()> {
        for directory in [&self.directory, &self.root.join(".tmp"), self.root.as_ref()] {
            crate::sync_directory(directory, self.apple_sync).map_err(|error| Error::io(directory, error))?;
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

    fn cut(epoch: StorageEpoch, offset: i64) -> CatalogCut {
        CatalogCut {
            storage_version: CURRENT_STORAGE_VERSION,
            storage_epoch: epoch,
            replica_epoch: epoch,
            through: ReplicaPosition::from_epoch_offset(epoch, offset).unwrap(),
        }
    }

    #[test]
    fn a_retirement_releases_every_candidate_and_closes_each_exact_name_for_good() {
        let root = tempfile::tempdir().unwrap();
        let epoch = StorageEpoch(0);
        let ledger = LocalCatalogLedger::new(Arc::new(root.path().to_owned()), epoch, AppleSync::Full);
        let name = |block| TableSlug::recorded("public$docs").unwrap()
            .data_key(&TrieKey::new(1, None, Vec::new(), BlockIndex(block)).unwrap());
        let (retired, other) = (name(2), name(3));
        assert_eq!(ledger.close_admission(cut(epoch, 8), &[Some(&retired), None]).unwrap(), [true, true]);
        assert!(ledger.publication_gate(&crate::ObjectKey::new(retired).unwrap()).is_err(), "a retired name is never published again");
        assert!(ledger.publication_gate(&crate::ObjectKey::new(other).unwrap()).unwrap().is_some());
        assert!(ledger.floor().unwrap().is_none(), "a retirement installs no floor: readers are bounded by the GC window alone");
        assert!(ledger.require_erasure_admission(Some(cut(epoch, 0))).is_ok());
    }

    #[test]
    fn only_an_erasure_floor_refuses_an_older_catalog() {
        let root = tempfile::tempdir().unwrap();
        let epoch = StorageEpoch(0);
        let ledger = LocalCatalogLedger::new(Arc::new(root.path().to_owned()), epoch, AppleSync::Full);
        ledger.close_erasure_admission(cut(epoch, 8)).unwrap();
        assert_eq!(ledger.require_erasure_admission(Some(cut(epoch, 7))).unwrap_err().code, "rtdb/erased-under-view");
        assert_eq!(ledger.require_erasure_admission(None).unwrap_err().code, "rtdb/erased-under-view");
        assert!(ledger.require_erasure_admission(Some(cut(epoch, 8))).is_ok());
        assert!(ledger.verifies_erasure_admission(cut(epoch, 8)).unwrap());
        assert!(!ledger.verifies_erasure_admission(cut(epoch, 9)).unwrap());
    }
}
