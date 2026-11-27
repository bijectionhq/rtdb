use std::{ops::AsyncFnOnce, sync::Arc};

use journal_contract::Lsn;
use rtdb_crypt::{Context, Generation, Kms};
use rtdb_format::journal::{Checkpoint, Manifest, ObjectId, Record};
use rtdb_index::branches::BranchCut;
use rtdb_store::{ObjectStore, read_file_bounded};
use rtdb_tx::transact::{Receipt, TransactState};
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp, rtdb_v1::Basis};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{journal::Stream, replay::{BackupSource, Committed, JournalCoordinates, RestoreOrigin}};

use super::{Lease, MultiDatabase, next, no_writer};

const BACKUP_HEADER: &[u8; 8] = b"RTDBBKP\x01";
const BACKUP_FILES_HEADER: &[u8; 8] = b"RTDBBKP\x02";
const HEADER_BYTES: usize = BACKUP_HEADER.len() + Basis::WIDTH + 8 + 8;

mod files;
pub use files::BackupFileRelocation;
use files::ArchivedFiles;

pub struct PortableBackup(Vec<u8>);

impl PortableBackup {
    pub const MAX_BYTES: usize = 256 << 20;

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    pub async fn verify(bytes: &[u8], source_kms: Arc<dyn Kms>) -> Result<VerifiedBackup> {
        if bytes.len() > Self::MAX_BYTES {
            return Err(backup_too_large());
        }
        require_durable_custody(source_kms.as_ref())?;
        let (header, ciphertext) = bytes.split_first_chunk::<HEADER_BYTES>().ok_or_else(invalid_backup)?;
        let with_files = header.starts_with(BACKUP_FILES_HEADER);
        let coordinates = header.strip_prefix(if with_files { BACKUP_FILES_HEADER } else { BACKUP_HEADER })
            .ok_or_else(invalid_backup)?;
        let (basis, coordinates) = coordinates.split_first_chunk::<{ Basis::WIDTH }>().unwrap();
        let (time, lsn) = coordinates.split_first_chunk::<8>().unwrap();
        let basis = Basis::from_bytes(basis)?;
        let through_lsn = Lsn(u64::from_be_bytes(lsn.try_into().unwrap()));
        let incarnation = Uuid::from_bytes(basis.incarnation());
        if incarnation.is_nil() || through_lsn.0 == 0 {
            return Err(invalid_backup());
        }
        let cut = BranchCut { basis, system_time: Timestamp(i64::from_be_bytes(*time)) };
        let plaintext = rtdb_crypt::open(source_kms.as_ref(), incarnation, backup_context(header), ciphertext).await?;
        let source_generation = plaintext.generation;
        let (transactions, files) = if with_files {
            let (transactions, files) = ArchivedFiles::decode(plaintext, cut)?;
            (transactions, Some(files))
        } else {
            (TransactState::decode_binary_checkpoint(&plaintext.bytes, cut)?, None)
        };
        transactions.require_portable_checkpoint()?;
        source_kms.check_readable(source_generation).await?;
        Ok(VerifiedBackup {
            source: BackupSource { cut, through_lsn },
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
    pub fn source_basis(&self) -> Basis {
        self.source.cut.basis
    }

    pub fn source_lsn(&self) -> Lsn {
        self.source.through_lsn
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

impl InstalledBackup {
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
            if state.records.iter().any(|record| match record {
                Record::Tx { objects, .. } => !objects.is_empty(),
                Record::RankedCatalog(_) => true,
                _ => false,
            }) {
                return Err(backup_dependencies());
            }
            let read_epoch = writer_lease.map_or(state.epoch, |lease| lease.epoch);
            let closure = state.reachability(&self.stream, self.store.as_ref(), read_epoch).await?;
            if closure.pins().next().is_some() || closure.contains_pinned_files() {
                return Err(backup_dependencies());
            }
            let transactions = if writer_lease.is_some() { self.durable_transactions() } else { &state.transactions };
            transactions.require_portable_checkpoint()?;
            let cut = transactions.index().head();
            let checkpoint = transactions.encode_binary_checkpoint()?;
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
                    let remaining = (PortableBackup::MAX_BYTES - HEADER_BYTES)
                        .checked_sub(body.len()).ok_or_else(backup_too_large)?;
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
            if body.len() > PortableBackup::MAX_BYTES - HEADER_BYTES {
                return Err(backup_too_large());
            }
            let mut header = Vec::with_capacity(HEADER_BYTES);
            header.extend_from_slice(if with_files { BACKUP_FILES_HEADER } else { BACKUP_HEADER });
            header.extend_from_slice(&cut.basis.to_bytes());
            header.extend_from_slice(&cut.system_time.0.to_be_bytes());
            header.extend_from_slice(&through_lsn.0.to_be_bytes());
            let ciphertext = rtdb_crypt::seal(self.stream.kms.as_ref(), generation, backup_context(&header), &body).await?;
            if ciphertext.len() > PortableBackup::MAX_BYTES - HEADER_BYTES {
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
        Box::pin(async move {
            let VerifiedBackup { source, manifest_sha256, mut transactions, files, source_kms, source_generation } = backup;
            if source.cut.basis.incarnation() == self.incarnation() {
                return Err(Anomaly::incorrect("rtdb/restore-same-deployment", "Restore needs a fresh deployment identity"));
            }
            self.refresh().await?;
            self.require_active()?;
            if let Some(installed) = self.applied_backup() {
                if installed.manifest_sha256 == manifest_sha256 && installed.source == source {
                    self.verify_installed_files().await?;
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

    pub async fn installed_backup(&mut self) -> Result<Option<InstalledBackup>> {
        self.refresh().await?;
        self.require_active()?;
        self.verify_installed_files().await?;
        Ok(self.applied_backup())
    }

    pub async fn installed_backup_file_relocations(&mut self) -> Result<Vec<BackupFileRelocation>> {
        self.refresh().await?;
        self.require_active()?;
        self.verify_installed_files().await
    }

    async fn verify_installed_files(&self) -> Result<Vec<BackupFileRelocation>> {
        let state = self.state();
        let Some((index_id, relocations)) = read_relocation_index(
            &self.stream, self.store.as_ref(), state.coordinates.origin,
        ).await? else {
            return Ok(Vec::new());
        };
        let closure = state.reachability(&self.stream, self.store.as_ref(), state.epoch).await?;
        if relocation_ids(index_id, &relocations).any(|id| !closure.contains(id)) {
            return Err(invalid_backup());
        }
        for relocation in &relocations {
            read_file_bounded(
                self.store.as_ref(), self.stream.kms.as_ref(), relocation.destination_file, PortableBackup::MAX_BYTES,
            ).await?;
        }
        Ok(relocations)
    }

    fn applied_backup(&self) -> Option<InstalledBackup> {
        let origin = self.state().coordinates.origin?;
        Some(InstalledBackup {
            source: origin.backup?,
            destination_basis: Basis::new(self.incarnation(), origin.through),
            manifest_sha256: origin.digest,
            activation_scope: self.stream.activation_scope,
        })
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
        self.append_control(lease, Record::Checkpoint(Checkpoint {
            manifest: image.id(),
            through: through.0,
            key_generation: generation,
            restore: true,
            recovery_epoch: self.state().checkpoint_recovery_epoch(through),
        })).await?;
        self.trim_committed_checkpoint(lease, self.state().head, through).await?;
        Ok(cut)
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
