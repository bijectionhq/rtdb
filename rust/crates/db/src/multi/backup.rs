use std::{ops::AsyncFnOnce, sync::Arc};

use journal_contract::{CheckpointAnchor, JournalError, Lsn, StreamId};
use rtdb_crypt::{Context, Generation, Kms, RecoveryKeyHold};
use rtdb_format::journal::{Checkpoint, Manifest, ObjectId, Record};
use rtdb_index::branches::BranchCut;
use rtdb_store::{ObjectStore, ObjectVersionHold, read_file_bounded, read_fresh_manifest, read_versioned_file, read_versioned_manifest};
use rtdb_tx::transact::{Receipt, TransactState};
use rtdb_types::{Anomaly, AnomalyCategory, PhysicalObjectVersion, Result, Timestamp, rtdb_v1::Basis};
use sha2::{Digest, Sha256};
use stream_logkeeper::stream::consensus::{ConsensusJournal, authenticated::{Archive, CatalogRoot, Head}};
use uuid::Uuid;

use crate::{journal::{Stream, journal_error}, replay::{BackupAuthentication, BackupSource, Committed, JournalCoordinates, RestoreOrigin}};

use super::{Lease, MultiDatabase, next, no_writer};

const BACKUP_HEADER: &[u8; 8] = b"RTDBBKP\x01";
const BACKUP_FILES_HEADER: &[u8; 8] = b"RTDBBKP\x02";
const AUTHENTICATED_BACKUP_HEADER: &[u8; 8] = b"RTDBBKP\x03";
const AUTHENTICATED_BACKUP_FILES_HEADER: &[u8; 8] = b"RTDBBKP\x04";
const HEADER_BYTES: usize = BACKUP_HEADER.len() + Basis::WIDTH + 8 + 8;

mod files;
pub use files::BackupFileRelocation;
use files::ArchivedFiles;

pub struct PortableBackup(Vec<u8>);

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthenticatedCut {
    pub head: Head,
    pub catalog_root: CatalogRoot,
}

impl AuthenticatedCut {
    pub fn matches(self, incarnation: [u8; 16], at: Lsn) -> bool {
        self.catalog_root.0 != [0; 32] && self.head.valid()
            && self.head.stream == StreamId(u128::from_be_bytes(incarnation))
            && self.head.size == at.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstallationAnchor {
    pub catalog: Uuid,
    pub catalog_root: CatalogRoot,
    pub position: Lsn,
}

impl InstallationAnchor {
    pub fn valid(self) -> bool {
        !self.catalog.is_nil() && self.catalog_root.0 != [0; 32] && self.position.0 != 0
    }

    pub fn is_in(self, cut: AuthenticatedCut) -> bool {
        cut.head.catalog == self.catalog && cut.catalog_root == self.catalog_root && cut.head.size >= self.position.0
    }
}

struct BackupHeader<'a> {
    cut: BranchCut,
    through_lsn: Lsn,
    with_files: bool,
    proof: Option<&'a [u8]>,
    context: &'a [u8],
    ciphertext: &'a [u8],
}

impl<'a> BackupHeader<'a> {
    fn parse(bytes: &'a [u8]) -> Result<Self> {
        if bytes.len() > PortableBackup::MAX_BYTES { return Err(backup_too_large()); }
        let (header, body) = bytes.split_first_chunk::<HEADER_BYTES>().ok_or_else(invalid_backup)?;
        let (with_files, authenticated) = match &header[..BACKUP_HEADER.len()] {
            marker if marker == BACKUP_HEADER => (false, false),
            marker if marker == BACKUP_FILES_HEADER => (true, false),
            marker if marker == AUTHENTICATED_BACKUP_HEADER => (false, true),
            marker if marker == AUTHENTICATED_BACKUP_FILES_HEADER => (true, true),
            _ => return Err(invalid_backup()),
        };
        let coordinates = &header[BACKUP_HEADER.len()..];
        let (basis, coordinates) = coordinates.split_first_chunk::<{ Basis::WIDTH }>().unwrap();
        let (time, lsn) = coordinates.split_first_chunk::<8>().unwrap();
        let basis = Basis::from_bytes(basis)?;
        let through_lsn = Lsn(u64::from_be_bytes(lsn.try_into().unwrap()));
        if basis.incarnation() == [0; 16] || through_lsn.0 == 0 { return Err(invalid_backup()); }
        let (proof, ciphertext) = if authenticated {
            let (length, body) = body.split_first_chunk::<4>().ok_or_else(invalid_backup)?;
            let (proof, ciphertext) = body.split_at_checked(u32::from_be_bytes(*length) as usize).ok_or_else(invalid_backup)?;
            (Some(proof), ciphertext)
        } else {
            (None, body)
        };
        Ok(Self {
            cut: BranchCut { basis, system_time: Timestamp(i64::from_be_bytes(*time)) },
            through_lsn, with_files, proof, context: &bytes[..bytes.len() - ciphertext.len()], ciphertext,
        })
    }

    fn archive(&self) -> Result<Option<Archive>> {
        let Some(proof) = self.proof else { return Ok(None); };
        let mut proof = proof;
        let archive = Archive::read_from(&mut proof).map_err(|_| invalid_backup())?;
        if !proof.is_empty() { return Err(invalid_backup()); }
        let source = archive_cut(&archive);
        if !source.matches(self.cut.basis.incarnation(), self.through_lsn) { return Err(invalid_backup()); }
        Ok(Some(archive))
    }
}

fn archive_cut(archive: &Archive) -> AuthenticatedCut {
    AuthenticatedCut { head: archive.signed_head.head, catalog_root: archive.history.genesis.root() }
}

impl PortableBackup {
    pub const MAX_BYTES: usize = 256 << 20;

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    pub fn lineage_archive(bytes: &[u8], catalog_root: CatalogRoot) -> Result<Option<Archive>> {
        let archive = BackupHeader::parse(bytes)?.archive()?;
        if let Some(archive) = &archive {
            archive.verify(catalog_root).map_err(|_| invalid_backup())?;
        }
        Ok(archive)
    }

    pub async fn verify(bytes: &[u8], source_kms: Arc<dyn Kms>) -> Result<VerifiedBackup> {
        require_durable_custody(source_kms.as_ref())?;
        let header = BackupHeader::parse(bytes)?;
        let cut = header.cut;
        let incarnation = Uuid::from_bytes(cut.basis.incarnation());
        let plaintext = rtdb_crypt::open(source_kms.as_ref(), incarnation, backup_context(header.context), header.ciphertext).await?;
        let source = header.archive()?.map(|archive| -> Result<AuthenticatedCut> {
            let source = archive_cut(&archive);
            archive.verify(source.catalog_root).map_err(|_| invalid_backup())?;
            Ok(source)
        }).transpose()?;
        let source_generation = plaintext.generation;
        let (transactions, files) = if header.with_files {
            let (transactions, files) = ArchivedFiles::decode(plaintext, cut)?;
            (transactions, Some(files))
        } else {
            (TransactState::decode_binary_checkpoint(&plaintext.bytes, cut)?, None)
        };
        transactions.require_portable_checkpoint()?;
        source_kms.check_readable(source_generation).await?;
        Ok(VerifiedBackup {
            source: BackupSource { cut, through_lsn: header.through_lsn, authentication: BackupAuthentication { source, installation: None } },
            manifest_sha256: Sha256::digest(bytes).into(),
            transactions,
            files,
            source_kms,
            source_generation,
        })
    }
}

pub struct VerifiedBackup {
    source: BackupSource,
    manifest_sha256: [u8; 32],
    transactions: TransactState,
    files: Option<ArchivedFiles>,
    source_kms: Arc<dyn Kms>,
    source_generation: Generation,
}

impl VerifiedBackup {
    pub fn authenticated_cut(&self) -> Option<AuthenticatedCut> {
        self.source.authentication.source
    }

    pub fn source_basis(&self) -> Basis {
        self.source.cut.basis
    }

    pub fn source_lsn(&self) -> Lsn {
        self.source.through_lsn
    }

    pub fn source_system_time(&self) -> Timestamp {
        self.source.cut.system_time
    }

    pub fn manifest_sha256(&self) -> [u8; 32] {
        self.manifest_sha256
    }

    pub async fn source_receipt(&self, key: &[u8]) -> Result<Receipt> {
        self.source_kms.check_readable(self.source_generation).await?;
        self.transactions.tx_status(key)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstalledBackup {
    source: BackupSource,
    destination_basis: Basis,
    manifest_sha256: [u8; 32],
    activation_scope: Option<crate::ActivationScope>,
}

pub struct BackupObjectReadback {
    pub installation: InstalledBackup,
    pub journal_head: Lsn,
    pub journal_anchor: Option<CheckpointAnchor>,
    pub checkpoint: ObjectId,
    pub generation: Generation,
    pub versions: Vec<PhysicalObjectVersion>,
}

impl InstalledBackup {
    pub fn authenticated_cut(&self) -> Option<AuthenticatedCut> {
        self.source.authentication.source
    }

    pub fn installation_anchor(&self) -> Option<InstallationAnchor> {
        self.source.authentication.installation
    }

    pub fn source_incarnation(&self) -> Uuid {
        Uuid::from_bytes(self.source_basis().incarnation())
    }

    pub fn source_basis(&self) -> Basis {
        self.source.cut.basis
    }

    pub fn source_lsn(&self) -> Lsn {
        self.source.through_lsn
    }

    pub fn source_system_time(&self) -> Timestamp {
        self.source.cut.system_time
    }

    pub fn destination_basis(&self) -> Basis {
        self.destination_basis
    }

    pub fn manifest_sha256(&self) -> [u8; 32] {
        self.manifest_sha256
    }

    pub fn activation_scope(&self) -> Option<crate::ActivationScope> {
        self.activation_scope
    }
}

impl MultiDatabase {
    pub async fn portable_backup(&mut self) -> Result<PortableBackup> {
        Box::pin(async move {
            require_durable_custody(self.stream.kms.as_ref())?;
            self.refresh().await?;
            self.require_active()?;
            let writer_lease = if self.stream.immutable_history() { None } else { Some(self.authority()?) };
            self.require_destroyed_generation()?;
            let state = self.state();
            let through_lsn = state.head;
            if through_lsn.0 == 0 {
                return Err(invalid_backup());
            }
            let generation = Generation { database: self.options.database, number: state.generation };
            if writer_lease.is_some() {
                let writer = self.writer.as_ref().ok_or_else(no_writer)?;
                if writer.pipeline.next_position() != state.coordinates.position(next(through_lsn)?)? {
                    return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb/backup-pending-writes", "The writer is settling an admitted transaction"));
                }
            }
            if state.records.iter().any(|record| matches!(record, Record::Tx { objects, .. } if !objects.is_empty())) {
                return Err(backup_dependencies());
            }
            let read_epoch = writer_lease.map_or(state.epoch, |lease| lease.epoch);
            let closure = state.closure_inputs().without_ranked_catalogues()
                .resolve(&self.stream, self.store.as_ref(), read_epoch).await?;
            if closure.pins().next().is_some() || closure.contains_pinned_files() {
                return Err(backup_dependencies());
            }
            let transactions = if writer_lease.is_some() { self.durable_transactions() } else { &state.transactions };
            let cut = transactions.index().head();
            let proof = if let Some(journal) = &self.stream.authenticated_journal {
                let stream = self.journal_stream();
                if has_authentication(journal, stream).await? {
                    let archive = journal.authenticated_archive(stream, Some(through_lsn.0)).await.map_err(journal_error)?;
                    let source = archive_cut(&archive);
                    if !source.matches(self.incarnation(), through_lsn) {
                        return Err(invalid_backup());
                    }
                    archive.verify(source.catalog_root).map_err(|_| invalid_backup())?;
                    let mut proof = Vec::new();
                    archive.write_to(&mut proof).map_err(|_| invalid_backup())?;
                    Some(proof)
                } else {
                    None
                }
            } else {
                None
            };
            let checkpoint = if proof.is_some() {
                transactions.encode_authenticated_portable_checkpoint()?
            } else {
                transactions.require_portable_checkpoint()?;
                transactions.encode_binary_checkpoint()?
            };
            let header_len = HEADER_BYTES + proof.as_ref().map_or(0, |proof| 4 + proof.len());
            let body_limit = PortableBackup::MAX_BYTES.checked_sub(header_len).ok_or_else(backup_too_large)?;
            let prior_index = if let Some((index, relocations)) = read_relocation_index(
                &self.stream, self.store.as_ref(), state.coordinates.origin,
            ).await? {
                if relocation_ids(index, &relocations).any(|id| !closure.contains(id)) {
                    return Err(invalid_backup());
                }
                Some(index)
            } else {
                None
            };
            let source_files: Vec<_> = closure.files().filter(|id| Some(*id) != prior_index).collect();
            let with_files = !source_files.is_empty();
            let body = if with_files {
                let mut body = files::archive_prefix(&checkpoint, source_files.len())?;
                for id in source_files {
                    let remaining = body_limit.checked_sub(body.len()).ok_or_else(backup_too_large)?;
                    let file = read_file_bounded(self.store.as_ref(), self.stream.kms.as_ref(), id, remaining).await?;
                    if file.generation != generation {
                        return Err(backup_dependencies());
                    }
                    files::append_archive_file(&mut body, id, &file.bytes)?;
                }
                bytes::Bytes::from(body)
            } else {
                checkpoint
            };
            if body.len() > body_limit {
                return Err(backup_too_large());
            }
            let mut header = Vec::with_capacity(header_len);
            header.extend_from_slice(match (with_files, proof.is_some()) {
                (false, false) => BACKUP_HEADER,
                (true, false) => BACKUP_FILES_HEADER,
                (false, true) => AUTHENTICATED_BACKUP_HEADER,
                (true, true) => AUTHENTICATED_BACKUP_FILES_HEADER,
            });
            header.extend_from_slice(&cut.basis.to_bytes());
            header.extend_from_slice(&cut.system_time.0.to_be_bytes());
            header.extend_from_slice(&through_lsn.0.to_be_bytes());
            if let Some(proof) = proof {
                let length = u32::try_from(proof.len()).map_err(|_| backup_too_large())?;
                header.extend_from_slice(&length.to_be_bytes());
                header.extend_from_slice(&proof);
            }
            let ciphertext = rtdb_crypt::seal(self.stream.kms.as_ref(), generation, backup_context(&header), &body).await?;
            if ciphertext.len() > body_limit {
                return Err(backup_too_large());
            }
            self.stream.kms.check_readable(generation).await?;
            if let Some(lease) = writer_lease {
                self.require_authority(lease)?;
            }
            header.extend_from_slice(&ciphertext);
            Ok(PortableBackup(header))
        }).await
    }

    pub async fn install_portable_backup(&mut self, backup: VerifiedBackup) -> Result<InstalledBackup> {
        self.install_portable_backup_with_custody(backup, None).await
    }

    /// Retains the destination generation for the selected authority and recovery operation before writing a commissioned backup.
    /// A fresh install requires the destination writer lease.
    /// A retry of an installed backup requires the original active hold.
    pub async fn install_portable_backup_holding_key(
        &mut self, backup: VerifiedBackup, hold: RecoveryKeyHold,
    ) -> Result<InstalledBackup> {
        self.install_portable_backup_with_custody(backup, Some(hold)).await
    }

    async fn install_portable_backup_with_custody(
        &mut self, backup: VerifiedBackup, hold: Option<RecoveryKeyHold>,
    ) -> Result<InstalledBackup> {
        Box::pin(async move {
            let VerifiedBackup { mut source, manifest_sha256, mut transactions, files, source_kms, source_generation } = backup;
            if source.cut.basis.incarnation() == self.incarnation() {
                return Err(Anomaly::incorrect("rtdb/restore-same-deployment", "Restore needs a fresh deployment identity"));
            }
            self.refresh().await?;
            self.require_active()?;
            if let Some(installed) = self.applied_backup()? {
                if installed.manifest_sha256 == manifest_sha256 && installed.source.cut == source.cut
                    && installed.source.through_lsn == source.through_lsn
                    && installed.authenticated_cut() == source.authentication.source
                {
                    self.verify_installed_files().await?;
                    if let Some(hold) = hold {
                        self.check_backup_key_hold(hold, self.staged_backup_checkpoint()?.key_generation)?;
                        self.stream.kms.require_recovery_key(hold).await?;
                    }
                    return Ok(installed);
                }
                return Err(nonempty());
            }
            let lease = self.authority()?;
            self.require_destroyed_generation()?;
            let state = self.state();
            if state.coordinates.origin.is_some() || state.transactions.index().head().basis.position() != 0
                || state.records.iter().any(|record| !record.permits_fresh_restore())
            {
                return Err(nonempty());
            }
            let writer = self.writer.as_ref().ok_or_else(no_writer)?;
            if writer.pipeline.next_position() != next(state.head)?.0 {
                return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb/restore-pending-writes", "The writer is settling an admitted transaction"));
            }
            source_kms.check_readable(source_generation).await?;
            transactions.restore_in(self.incarnation())?;
            if let Some(hold) = hold {
                self.check_backup_key_hold(hold, state.generation)?;
                self.stream.kms.retain_recovery_key(hold).await?;
            }
            let (portable_file_index, destination_files) = match files {
                Some(files) => {
                    let mut relocations = Vec::with_capacity(files.entries().len());
                    for (source_file, bytes) in files.entries() {
                        source_kms.check_readable(source_generation).await?;
                        let destination_file = self.write_file(bytes).await?;
                        relocations.push(BackupFileRelocation { source_file, destination_file });
                    }
                    source_kms.check_readable(source_generation).await?;
                    let index = self.write_file(&files::encode_relocations(&relocations)?).await?;
                    (Some(index), relocation_ids(index, &relocations).collect())
                }
                None => (None, Vec::new()),
            };
            if let Some(journal) = &self.stream.authenticated_journal
                && has_authentication(journal, self.journal_stream()).await?
            {
                let history = journal.catalog_history().await.map_err(journal_error)?;
                let anchor = InstallationAnchor {
                    catalog: history.genesis.catalog,
                    catalog_root: history.genesis.root(),
                    position: next(self.state().head)?,
                };
                if !anchor.valid() { return Err(invalid_backup()); }
                source.authentication.installation = Some(anchor);
            }
            let origin = RestoreOrigin {
                digest: manifest_sha256,
                through: source.cut.basis.position(),
                source: None,
                backup: Some(source),
                portable_file_index,
            };
            let cut = self.install_restored_state(lease, transactions, origin, destination_files, async || {
                source_kms.check_readable(source_generation).await
            }).await?;
            Ok(InstalledBackup {
                source,
                destination_basis: cut.basis,
                manifest_sha256,
                activation_scope: self.stream.activation_scope,
            })
        }).await
    }

    fn check_backup_key_hold(&self, hold: RecoveryKeyHold, generation: u64) -> Result<()> {
        if self.stream.activation_scope.is_none()
            || hold.generation != (Generation { database: self.options.database, number: generation })
        {
            return Err(invalid_backup());
        }
        Ok(())
    }

    pub async fn installed_backup(&mut self) -> Result<Option<InstalledBackup>> {
        self.refresh().await?;
        self.require_active()?;
        self.verify_installed_files().await?;
        self.applied_backup()
    }

    pub async fn installed_backup_file_relocations(&mut self) -> Result<Vec<BackupFileRelocation>> {
        self.refresh().await?;
        self.require_active()?;
        self.verify_installed_files().await
    }

    /// Checks provider holds for the commissioned, staged backup's exact object closure.
    pub async fn check_installed_backup_object_holds(
        &mut self, archive_sha256: [u8; 32], holds: &dyn ObjectVersionHold,
    ) -> Result<BackupObjectReadback> {
        self.read_installed_backup_objects(archive_sha256, Some(holds)).await
    }

    /// Authenticates the staged backup's object closure and observes the exact versions read.
    pub async fn observe_installed_backup_object_versions(&mut self, archive_sha256: [u8; 32]) -> Result<BackupObjectReadback> {
        self.read_installed_backup_objects(archive_sha256, None).await
    }

    async fn read_installed_backup_objects(
        &mut self, archive_sha256: [u8; 32], holds: Option<&dyn ObjectVersionHold>,
    ) -> Result<BackupObjectReadback> {
        self.refresh().await?;
        self.require_active()?;
        let installed = self.applied_backup()?.filter(|installed| {
            installed.activation_scope().is_some() && installed.manifest_sha256() == archive_sha256
        }).ok_or_else(invalid_backup)?;
        let state = self.state();
        let head = state.head;
        let checkpoint = self.staged_backup_checkpoint()?.clone();
        let store = self.store.as_ref();
        let kms = self.stream.kms.as_ref();
        let (manifest, checkpoint_version) = read_versioned_manifest(store, kms, checkpoint.manifest, holds).await?;
        if manifest.generation.number != checkpoint.key_generation || !manifest.contents().pins.is_empty() {
            return Err(invalid_backup());
        }
        let members = &manifest.contents().files;
        if members.iter().any(|file| file.database != self.options.database) {
            return Err(invalid_backup());
        }
        let mut versions = Vec::with_capacity(members.len() + 2);
        versions.push(checkpoint_version);
        let index_id = state.coordinates.origin.and_then(|origin| origin.portable_file_index);
        if let Some(index_id) = index_id {
            if !members.contains(&index_id) {
                return Err(invalid_backup());
            }
            let (index, version) = read_versioned_file(store, kms, index_id, files::MAX_INDEX_BYTES, holds).await?;
            let relocations = files::decode_relocations(&index.bytes, installed.source_incarnation(), index_id)?;
            if relocations.iter().any(|relocation| !members.contains(&relocation.destination_file)) {
                return Err(invalid_backup());
            }
            versions.push(version);
        }
        for &file in members {
            if Some(file) != index_id {
                let (_, version) = read_versioned_file(store, kms, file, PortableBackup::MAX_BYTES, holds).await?;
                versions.push(version);
            }
        }
        versions.push(self.token_keys.read_object_version(checkpoint.key_generation, kms, holds).await?);
        let journal_anchor = if let Some(base) = &self.state().base {
            self.stream.certify_checkpoint(base, head).await?.immutable_anchor()
        } else {
            None
        };
        self.refresh().await?;
        if self.state().head != head || self.staged_backup_checkpoint()? != &checkpoint {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb/backup-installation-changed",
                "The staged backup changed during the object hold readback"));
        }
        Ok(BackupObjectReadback {
            installation: installed, journal_head: head, journal_anchor, checkpoint: checkpoint.manifest,
            generation: manifest.generation, versions,
        })
    }

    async fn verify_installed_files(&self) -> Result<Vec<BackupFileRelocation>> {
        let state = self.state();
        if !state.coordinates.origin.is_some_and(|origin| origin.backup.is_some()) {
            return Ok(Vec::new());
        }
        let checkpoint = self.installed_backup_checkpoint()?;
        let manifest = read_fresh_manifest(self.store.as_ref(), self.stream.kms.as_ref(), checkpoint.manifest).await?;
        if manifest.generation.number != checkpoint.key_generation {
            return Err(invalid_backup());
        }
        let Some((index_id, relocations)) = read_relocation_index(
            &self.stream, self.store.as_ref(), state.coordinates.origin,
        ).await? else {
            return Ok(Vec::new());
        };
        if relocation_ids(index_id, &relocations).any(|id| !manifest.contents().files.contains(&id)) {
            return Err(invalid_backup());
        }
        for relocation in &relocations {
            read_file_bounded(
                self.store.as_ref(), self.stream.kms.as_ref(), relocation.destination_file, PortableBackup::MAX_BYTES,
            ).await?;
        }
        Ok(relocations)
    }

    fn installed_backup_checkpoint(&self) -> Result<&Checkpoint> {
        let state = self.state();
        state.base.as_ref().map(|base| &base.checkpoint)
            .or_else(|| state.records.iter().find_map(|record| match record {
                Record::Checkpoint(checkpoint) if checkpoint.restore => Some(checkpoint),
                _ => None,
            }))
            .ok_or_else(invalid_backup)
    }

    fn staged_backup_checkpoint(&self) -> Result<&Checkpoint> {
        let checkpoint = self.installed_backup_checkpoint()?;
        if self.state().records.iter().any(|record| !record.permits_fresh_restore()
            && !matches!(record, Record::Checkpoint(selected) if selected == checkpoint))
        {
            return Err(backup_dependencies());
        }
        Ok(checkpoint)
    }

    fn applied_backup(&self) -> Result<Option<InstalledBackup>> {
        let state = self.state();
        let Some((origin, source)) = state.coordinates.origin.and_then(|origin| origin.backup.map(|source| (origin, source))) else {
            return Ok(None);
        };
        Ok(Some(InstalledBackup {
            source,
            destination_basis: Basis::new(self.incarnation(), origin.through),
            manifest_sha256: origin.digest,
            activation_scope: self.stream.activation_scope,
        }))
    }

    pub(super) async fn install_restored_state(
        &mut self,
        lease: Lease,
        transactions: TransactState,
        origin: RestoreOrigin,
        files: Vec<ObjectId>,
        check_source: impl AsyncFnOnce() -> Result<()>,
    ) -> Result<BranchCut> {
        let through = self.state().head;
        let generation = self.state().generation;
        let cut = transactions.index().head();
        let coordinates = JournalCoordinates {
            offset: cut.basis.position().saturating_sub(through.0),
            origin: Some(origin),
        };
        coordinates.position(next(next(through)?)?)?;
        let manifest = Manifest {
            files,
            pins: Vec::new(),
            payload: Committed::checkpoint_payload(&transactions, lease.until, false, coordinates)?,
        };
        let image = self.writer.as_mut().ok_or_else(no_writer)?.objects
            .manifest(self.stream.kms.as_ref(), generation, &manifest).await?;
        image.put(self.store.as_ref()).await?;
        check_source().await?;
        self.require_authority(lease)?;
        self.install_checkpoint(lease, Checkpoint {
            manifest: image.id(),
            through: through.0,
            key_generation: generation,
            restore: true,
            recovery_epoch: self.state().checkpoint_recovery_epoch(through),
        }, lease.until, 0).await?;
        Ok(cut)
    }
}

async fn has_authentication(journal: &ConsensusJournal, stream: StreamId) -> Result<bool> {
    match journal.require_authenticated(stream).await {
        Ok(()) => Ok(true),
        Err(JournalError::Unsupported) => Ok(false),
        Err(error) => Err(journal_error(error)),
    }
}

pub(super) async fn portable_attachment_files(
    stream: &Stream, store: &dyn ObjectStore, origin: Option<RestoreOrigin>,
) -> Result<Vec<ObjectId>> {
    Ok(match read_relocation_index(stream, store, origin).await? {
        Some((index_id, relocations)) => relocation_ids(index_id, &relocations).collect(),
        None => Vec::new(),
    })
}

async fn read_relocation_index(
    stream: &Stream, store: &dyn ObjectStore, origin: Option<RestoreOrigin>,
) -> Result<Option<(ObjectId, Vec<BackupFileRelocation>)>> {
    let Some(RestoreOrigin { portable_file_index: Some(index_id), backup, .. }) = origin else {
        return Ok(None);
    };
    let source_database = Uuid::from_bytes(backup.ok_or_else(invalid_backup)?.cut.basis.incarnation());
    let index = read_file_bounded(store, stream.kms.as_ref(), index_id, files::MAX_INDEX_BYTES).await?;
    let relocations = files::decode_relocations(&index.bytes, source_database, index_id)?;
    Ok(Some((index_id, relocations)))
}

fn relocation_ids(index_id: ObjectId, relocations: &[BackupFileRelocation]) -> impl Iterator<Item = ObjectId> + '_ {
    std::iter::once(index_id).chain(relocations.iter().map(|relocation| relocation.destination_file))
}

fn backup_context(header: &[u8]) -> Context {
    Context::PortableBackup { header_digest: Sha256::digest(header).into() }
}

fn require_durable_custody(kms: &dyn Kms) -> Result<()> {
    if !kms.supports_durable_generation_retirement() {
        return Err(Anomaly::unsupported("rtdb/backup-keys", "Portable backups require independently available durable key custody"));
    }
    Ok(())
}

fn invalid_backup() -> Anomaly {
    Anomaly::incorrect("rtdb/invalid-backup", "The portable backup does not identify a valid application cut")
}

fn backup_too_large() -> Anomaly {
    Anomaly::unsupported("rtdb/backup-size", "The portable backup exceeds the maximum size")
}

fn backup_dependencies() -> Anomaly {
    Anomaly::unsupported("rtdb/backup-dependencies", "The application cut needs a portable transfer of its external object or pin inventory")
}

fn nonempty() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb/restore-not-empty", "Restore needs a fresh deployment")
}
