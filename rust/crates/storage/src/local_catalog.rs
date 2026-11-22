use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fs2::FileExt;
use rtdb_format::catalog::same_publication_digest;
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, CatalogCut, ReplicaPosition, Result, StorageEpoch};

use crate::{Error, repair_anchor_decision::{self, AnchorInstallation, RecoveryCollection}};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LocalRepairAnchor {
    pub block: BlockIndex,
    pub boundary: ReplicaPosition,
    pub sha256: [u8; 32],
}

impl LocalRepairAnchor {
    fn well_formed(self) -> bool { repair_anchor_decision::valid_anchor(self.block.0, self.boundary.0) }
}

pub(crate) struct LocalCatalogLedger {
    directory: PathBuf,
    root: Arc<PathBuf>,
    replica_epoch: StorageEpoch,
}

impl LocalCatalogLedger {
    pub(crate) fn new(root: Arc<PathBuf>, replica_epoch: StorageEpoch) -> Self {
        Self { directory: root.join(".tmp/catalog-ledger"), root, replica_epoch }
    }

    pub(crate) fn retain(&self, cut: Option<CatalogCut>) -> Result<tempfile::NamedTempFile> {
        let _gate = self.lock()?;
        if cut.is_some_and(|cut| cut.replica_epoch != self.replica_epoch || !well_formed(cut))
            || self.floor()?.is_some_and(|floor| floor.replica_epoch >= self.replica_epoch
                && !cut.is_some_and(|cut| covers(cut, floor)))
        {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.storage/catalog-not-current",
                "The captured catalog precedes the local collection floor or belongs to another journal"));
        }
        let mut ticket = tempfile::Builder::new().prefix("reader-").tempfile_in(&self.directory)
            .map_err(|error| Error::io(&self.directory, error))?;
        fs2::FileExt::lock_shared(ticket.as_file()).map_err(|error| Error::io(ticket.path(), error))?;
        ticket.write_all(&serde_json::to_vec(&cut).map_err(invalid)?)
            .map_err(|error| Error::io(ticket.path(), error))?;
        Ok(ticket)
    }

    pub(crate) fn advance(&self, cut: CatalogCut) -> Result<bool> {
        let _gate = self.lock()?;
        if cut.replica_epoch != self.replica_epoch || !well_formed(cut) || cut.through.after_offset(cut.replica_epoch) < 0 {
            return Err(invalid("Invalid local catalog floor"));
        }
        if let Some(prior) = self.floor()? {
            if covers(prior, cut) {
                self.sync_ledger()?;
                return Ok(true);
            }
            let replaces_journal = same_storage(cut, prior) && cut.replica_epoch > prior.replica_epoch;
            if !covers(cut, prior) && !replaces_journal {
                return Err(invalid("The local catalog floor belongs to a different storage root or newer journal"));
            }
        }
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
                    Read::by_ref(&mut ticket).take(512).read_to_end(&mut bytes).map_err(|error| Error::io(&path, error))?;
                    let held: Option<CatalogCut> = serde_json::from_slice(&bytes).map_err(invalid)?;
                    if !held.is_some_and(|held| covers(held, cut)) { return Ok(false); }
                }
                Err(error) => return Err(Error::io(&path, error).into()),
            }
        }
        let mut next = tempfile::NamedTempFile::new_in(&self.directory).map_err(|error| Error::io(&self.directory, error))?;
        next.write_all(&serde_json::to_vec(&cut).map_err(invalid)?).map_err(|error| Error::io(next.path(), error))?;
        crate::sync_file(next.as_file()).map_err(|error| Error::io(next.path(), error))?;
        let path = self.directory.join("floor");
        next.persist(&path).map_err(|error| Error::io(&path, error.error))?;
        self.sync_ledger()?;
        Ok(true)
    }

    pub(crate) fn permits_retirement(&self, cut: CatalogCut) -> Result<bool> {
        let _gate = self.lock()?;
        if cut.replica_epoch != self.replica_epoch || !self.floor()?.is_some_and(|floor| covers(floor, cut)) {
            return Ok(false);
        }
        self.sync_ledger()?;
        Ok(true)
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

    pub(crate) fn collect_recovery_file(&self, block: BlockIndex, remove: impl FnOnce() -> crate::Result<()>) -> Result<()> {
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

    fn lock(&self) -> Result<File> {
        fs::create_dir_all(&self.directory).map_err(|error| Error::io(&self.directory, error))?;
        let path = self.directory.join("lock");
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false)
            .open(&path).map_err(|error| Error::io(&path, error))?;
        FileExt::lock_exclusive(&file).map_err(|error| Error::io(path, error))?;
        Ok(file)
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

fn well_formed(cut: CatalogCut) -> bool {
    cut.through.after_offset(cut.replica_epoch) >= 0
        || ReplicaPosition::from_epoch_offset(cut.replica_epoch, -1).ok() == Some(cut.through)
}

fn same_storage(left: CatalogCut, right: CatalogCut) -> bool {
    left.storage_version == right.storage_version && left.storage_epoch == right.storage_epoch
}

fn covers(reader: CatalogCut, floor: CatalogCut) -> bool {
    well_formed(reader) && same_storage(reader, floor) && reader.replica_epoch == floor.replica_epoch
        && reader.through >= floor.through
}

fn invalid(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::fault("rtdb.storage/catalog-ledger", error.to_string())
}
