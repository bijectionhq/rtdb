use std::{ops::AsyncFnOnce, sync::Arc};

use journal_contract::Lsn;
use rtdb_crypt::{Context, Generation, Kms};
use rtdb_format::journal::{Checkpoint, Manifest, Record};
use rtdb_index::branches::BranchCut;
use rtdb_tx::transact::{Receipt, TransactState};
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp, rtdb_v1::Basis};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::replay::{BackupSource, Committed, JournalCoordinates, RestoreOrigin};

use super::{Lease, MultiDatabase, next, no_writer};

const BACKUP_HEADER: &[u8; 8] = b"RTDBBKP\x01";
const HEADER_BYTES: usize = BACKUP_HEADER.len() + Basis::WIDTH + 8 + 8;

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
        let coordinates = header.strip_prefix(BACKUP_HEADER).ok_or_else(invalid_backup)?;
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
        let transactions = TransactState::decode_binary_checkpoint(&plaintext.bytes, cut)?;
        transactions.require_portable_checkpoint()?;
        source_kms.check_readable(plaintext.generation).await?;
        Ok(VerifiedBackup {
            source: BackupSource { cut, through_lsn },
            manifest_sha256: Sha256::digest(bytes).into(),
            transactions,
            source_kms,
            source_generation: plaintext.generation,
        })
    }
}

pub struct VerifiedBackup {
    source: BackupSource,
    manifest_sha256: [u8; 32],
    transactions: TransactState,
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

    pub fn destination_basis(&self) -> Basis {
        self.destination_basis
    }

    pub fn manifest_sha256(&self) -> [u8; 32] {
        self.manifest_sha256
    }
}

impl MultiDatabase {
    pub async fn portable_backup(&mut self) -> Result<PortableBackup> {
        Box::pin(async move {
            require_durable_custody(self.stream.kms.as_ref())?;
            self.refresh().await?;
            let lease = self.authority()?;
            self.require_destroyed_generation()?;
            let state = self.state();
            let through_lsn = state.head;
            let generation = Generation { database: self.options.database, number: state.generation };
            let writer = self.writer.as_ref().ok_or_else(no_writer)?;
            if writer.pipeline.next_position() != state.coordinates.position(next(through_lsn)?)? {
                return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb/backup-pending-writes", "The writer is settling an admitted transaction"));
            }
            if state.records.iter().any(|record| match record {
                Record::Tx { objects, .. } => !objects.is_empty(),
                Record::RankedCatalog(_) => true,
                _ => false,
            }) {
                return Err(backup_dependencies());
            }
            let closure = self.reachability(lease).await?;
            if closure.contains_files() || closure.pins().next().is_some() {
                return Err(backup_dependencies());
            }
            let transactions = self.durable_transactions();
            transactions.require_portable_checkpoint()?;
            let cut = transactions.index().head();
            let body = transactions.encode_binary_checkpoint()?;
            if body.len() > PortableBackup::MAX_BYTES - HEADER_BYTES {
                return Err(backup_too_large());
            }
            let mut header = Vec::with_capacity(HEADER_BYTES);
            header.extend_from_slice(BACKUP_HEADER);
            header.extend_from_slice(&cut.basis.to_bytes());
            header.extend_from_slice(&cut.system_time.0.to_be_bytes());
            header.extend_from_slice(&through_lsn.0.to_be_bytes());
            let ciphertext = rtdb_crypt::seal(self.stream.kms.as_ref(), generation, backup_context(&header), &body).await?;
            if ciphertext.len() > PortableBackup::MAX_BYTES - HEADER_BYTES {
                return Err(backup_too_large());
            }
            self.stream.kms.check_readable(generation).await?;
            self.require_authority(lease)?;
            header.extend_from_slice(&ciphertext);
            Ok(PortableBackup(header))
        }).await
    }

    pub async fn install_portable_backup(&mut self, backup: VerifiedBackup) -> Result<InstalledBackup> {
        Box::pin(async move {
            let VerifiedBackup { source, manifest_sha256, mut transactions, source_kms, source_generation } = backup;
            if source.cut.basis.incarnation() == self.incarnation() {
                return Err(Anomaly::incorrect("rtdb/restore-same-deployment", "Restore needs a fresh deployment identity"));
            }
            self.refresh().await?;
            self.require_active()?;
            if let Some(installed) = self.applied_backup() {
                if installed.manifest_sha256 == manifest_sha256 && installed.source == source {
                    return Ok(installed);
                }
                return Err(nonempty());
            }
            let lease = self.authority()?;
            self.require_destroyed_generation()?;
            let state = self.state();
            if state.coordinates.origin.is_some() || state.transactions.index().head().basis.position() != 0
                || state.records.iter().any(|record| !matches!(record, Record::Lease { .. } | Record::WriterRelease { .. }))
            {
                return Err(nonempty());
            }
            let writer = self.writer.as_ref().ok_or_else(no_writer)?;
            if writer.pipeline.next_position() != next(state.head)?.0 {
                return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb/restore-pending-writes", "The writer is settling an admitted transaction"));
            }
            source_kms.check_readable(source_generation).await?;
            transactions.restore_in(self.incarnation())?;
            let origin = RestoreOrigin {
                digest: manifest_sha256,
                through: source.cut.basis.position(),
                source: None,
                backup: Some(source),
            };
            let cut = self.install_restored_state(lease, transactions, origin, async || {
                source_kms.check_readable(source_generation).await
            }).await?;
            Ok(InstalledBackup { source, destination_basis: cut.basis, manifest_sha256 })
        }).await
    }

    pub async fn installed_backup(&mut self) -> Result<Option<InstalledBackup>> {
        self.refresh().await?;
        self.require_active()?;
        Ok(self.applied_backup())
    }

    fn applied_backup(&self) -> Option<InstalledBackup> {
        let origin = self.state().coordinates.origin?;
        Some(InstalledBackup {
            source: origin.backup?,
            destination_basis: Basis::new(self.incarnation(), origin.through),
            manifest_sha256: origin.digest,
        })
    }

    pub(super) async fn install_restored_state(
        &mut self,
        lease: Lease,
        transactions: TransactState,
        origin: RestoreOrigin,
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
            files: Vec::new(),
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
        })).await?;
        self.trim(lease, through).await?;
        Ok(cut)
    }
}

fn backup_context(header: &[u8]) -> Context {
    Context::CustodyValue { binding: Sha256::digest(header).into() }
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
