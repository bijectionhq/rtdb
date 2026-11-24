use std::{ops::AsyncFnOnce, sync::Arc};

use rtdb_crypt::{Kms, Plaintext, tokens::ExportTicket};
use rtdb_format::journal::{Checkpoint, Manifest, ObjectId, Pin, Record};
use rtdb_index::branches::BranchCut;
use rtdb_store::{ObjectStore, VerifiedManifest, read_file, read_manifest};
use rtdb_tx::transact::TransactState;
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp, rtdb_v1::Basis};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::replay::{CheckpointSource, Committed, JournalCoordinates, RestoreOrigin};

use super::{MultiDatabase, next, no_writer};

const EXACT_EXPORT: &[u8; 5] = b"RTEX1";

pub struct PinnedExport {
    pin: Uuid,
    image: VerifiedManifest,
    store: Arc<dyn ObjectStore>,
    kms: Arc<dyn Kms>,
}

impl PinnedExport {
    pub fn manifest(&self) -> &Manifest {
        self.image.contents()
    }

    /// Binds a managed transfer reservation to this exact source manifest.
    pub fn manifest_digest(&self) -> [u8; 32] {
        manifest_identity(self.image.id())
    }

    pub async fn file(&self, id: ObjectId) -> Result<Plaintext> {
        if !self.image.contents().files.contains(&id) {
            return Err(Anomaly::incorrect(
                "rtdb/unpinned-file",
                "The file is outside the pinned checkpoint",
            ));
        }
        read_file(self.store.as_ref(), self.kms.as_ref(), id).await
    }

    pub async fn check_readable(&self) -> Result<()> {
        self.kms.check_readable(self.image.generation).await
    }

    pub async fn exact_checkpoint(&self) -> Result<(BranchCut, TransactState)> {
        self.check_readable().await?;
        let (cut, bytes) = exact_payload(&self.image)?;
        let state = TransactState::decode_checkpoint(bytes, cut)?;
        self.check_readable().await?;
        Ok((cut, state))
    }
}

impl MultiDatabase {
    /// Publish an immutable checkpoint for one exact applied basis and order
    /// its pin before returning the reference. The caller supplies the pin ID
    /// so it can retry the same export after an uncertain append.
    pub async fn pin_checkpoint_at(&mut self, id: Uuid, basis: Basis, until: Option<Timestamp>) -> Result<Pin> {
        self.refresh().await?;
        let lease = self.authority()?;
        self.require_destroyed_generation()?;
        if id.is_nil() { return Err(Anomaly::incorrect("rtdb/invalid-pin", "An export pin needs a nonzero ID")); }
        if until.is_some_and(|deadline| !self.before(deadline.0)) {
            return Err(Anomaly::incorrect("rtdb/pin-expired", "The pin admission deadline has elapsed"));
        }
        let closure = self.reachability(lease).await?;
        if let Some(pin) = closure.pins().find(|pin| pin.id == id) {
            let image = read_manifest(self.store.as_ref(), self.stream.kms.as_ref(), pin.manifest).await?;
            if exact_payload(&image)?.0.basis != basis || pin.until != until.map(|time| time.0) {
                return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb/pin-id", "The pin names a different checkpoint or lifetime"));
            }
            self.require_authority(lease)?;
            return Ok(pin.clone());
        }
        let journal_head = self.state().head;
        let generation = self.state().generation;
        let writer = self.writer.as_ref().ok_or_else(no_writer)?;
        if writer.pipeline.next_position() != self.state().coordinates.position(next(journal_head)?)? {
            return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb/export-pending-writes", "The writer is settling an admitted transaction"));
        }
        let transactions = self.durable_transactions();
        let cut = transactions.index().exact(basis)?;
        let checkpoint = transactions.encode_checkpoint_at(basis)?;
        let mut payload = Vec::with_capacity(EXACT_EXPORT.len() + 16 + checkpoint.len());
        payload.extend_from_slice(EXACT_EXPORT);
        payload.extend_from_slice(&cut.basis.position().to_be_bytes());
        payload.extend_from_slice(&cut.system_time.0.to_be_bytes());
        payload.extend_from_slice(&checkpoint);
        let manifest = Manifest { files: Vec::new(), pins: Vec::new(), payload };
        let image = self.writer.as_mut().ok_or_else(no_writer)?.objects
            .manifest(self.stream.kms.as_ref(), generation, &manifest).await?;
        image.put(self.store.as_ref()).await?;
        self.require_authority(lease)?;
        if until.is_some_and(|deadline| !self.before(deadline.0)) {
            return Err(Anomaly::incorrect("rtdb/pin-expired", "The pin admission deadline has elapsed"));
        }
        let pin = Pin { id, manifest: image.id(), until: until.map(|time| time.0) };
        self.append_control(lease, Record::Pin(pin.clone())).await?;
        Ok(pin)
    }

    pub async fn open_export(&mut self, id: Uuid) -> Result<PinnedExport> {
        let image = self.open_pin(id).await?;
        Ok(PinnedExport {
            pin: id,
            image,
            store: Arc::clone(&self.store),
            kms: Arc::clone(&self.stream.kms),
        })
    }

    pub async fn export_ticket(&mut self, pin: Uuid, basis: Basis, until: Option<Timestamp>) -> Result<Vec<u8>> {
        let pin = self.pin_checkpoint_at(pin, basis, until).await?;
        self.tokens.seal_export(ExportTicket { pin: pin.id, basis, manifest_sha256: manifest_identity(pin.manifest) }).await
    }

    pub async fn open_export_ticket(&mut self, ticket: &[u8]) -> Result<PinnedExport> {
        let ticket = self.tokens.open_export(ticket).await?;
        let export = self.open_export(ticket.pin).await?;
        if manifest_identity(export.image.id()) != ticket.manifest_sha256
            || exact_payload(&export.image)?.0.basis != ticket.basis
        {
            return Err(Anomaly::incorrect("rtdb/invalid-export", "The pinned checkpoint does not match the export ticket"));
        }
        Ok(export)
    }

    /// Orders release of the exact export once its trusted owner has drained the transfer.
    pub async fn release_export_ticket(&mut self, ticket: &[u8]) -> Result<()> {
        let ticket = self.tokens.open_export(ticket).await?;
        self.refresh().await?;
        let lease = self.authority()?;
        let closure = self.reachability(lease).await?;
        let Some(pin) = closure.pins().find(|pin| pin.id == ticket.pin) else { return Ok(()); };
        if manifest_identity(pin.manifest) != ticket.manifest_sha256 {
            return Err(Anomaly::incorrect("rtdb/invalid-export", "The pinned checkpoint does not match the export ticket"));
        }
        self.append_control(lease, Record::Unpin { id: ticket.pin }).await
    }

    /// Install an independent backing for an exact export. The destination's
    /// journal and KMS own all bytes needed to recover after the append.
    pub async fn restore_export(&mut self, export: &PinnedExport) -> Result<BranchCut> {
        let source = export.image.id();
        if source.database == self.options.database {
            return Err(Anomaly::incorrect("rtdb/restore-same-deployment", "Restore needs a distinct deployment"));
        }
        if !export.manifest().files.is_empty() {
            return Err(Anomaly::unsupported("rtdb/export-files", "This export needs independent destination file placement"));
        }
        self.refresh().await?;
        let lease = self.authority()?;
        let source_identity = manifest_identity(source);
        let state = self.state();
        let through = state.head;
        let generation = state.generation;
        let destination = self.incarnation();
        if let Some(origin) = state.coordinates.origin
            && origin.digest == source_identity
        {
            return state.transactions.index().exact(Basis::new(destination, origin.through));
        }
        if state.transactions.index().head().basis.position() != 0
            || state.records.iter().any(|record| !matches!(record, Record::Lease { .. } | Record::WriterRelease { .. }))
        {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb/restore-not-empty", "Restore needs a fresh deployment"));
        }
        let writer = self.writer.as_ref().ok_or_else(no_writer)?;
        if writer.pipeline.next_position() != next(through)?.0 {
            return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb/restore-pending-writes", "The writer is settling an admitted transaction"));
        }
        let (source_cut, mut transactions) = export.exact_checkpoint().await?;
        transactions.restore_in(destination)?;
        let cut = transactions.index().head();
        let coordinates = JournalCoordinates {
            offset: cut.basis.position().saturating_sub(through.0),
            origin: Some(RestoreOrigin {
                digest: source_identity,
                through: cut.basis.position(),
                source: Some(CheckpointSource { cut: source_cut, pin: export.pin }),
            }),
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
        export.check_readable().await?;
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

    pub async fn checkpoint_source(&mut self) -> Result<Option<CheckpointSource>> {
        self.refresh().await?;
        Ok(self.state().coordinates.origin.and_then(|origin| origin.source))
    }

    /// Recovers the applied destination terminal even after the source pin has expired.
    pub async fn restored_checkpoint_basis(&mut self, manifest_digest: [u8; 32]) -> Result<Option<Basis>> {
        self.refresh().await?;
        match self.state().coordinates.origin {
            None => Ok(None),
            Some(origin) if origin.digest == manifest_digest =>
                Ok(Some(Basis::new(self.incarnation(), origin.through))),
            Some(_) => Err(Anomaly::new(
                AnomalyCategory::Conflict, "rtdb/restore-origin-mismatch",
                "The destination contains a different checkpoint transfer",
            )),
        }
    }

    /// The destination must durably publish its independent backing before returning success.
    pub async fn transfer_fork<T>(
        &mut self,
        id: Uuid,
        persist_destination: impl for<'a> AsyncFnOnce(&'a PinnedExport) -> Result<T>,
    ) -> Result<T> {
        let export = self.open_export(id).await?;
        let destination = persist_destination(&export).await?;
        self.release_pin(id).await?;
        Ok(destination)
    }
}

fn manifest_identity(manifest: ObjectId) -> [u8; 32] {
    Sha256::digest(manifest.to_string().as_bytes()).into()
}

fn exact_payload(image: &VerifiedManifest) -> Result<(BranchCut, &[u8])> {
    let invalid = || Anomaly::incorrect("rtdb/invalid-export", "The manifest is missing an exact export coordinate");
    let payload = image.contents().payload.strip_prefix(EXACT_EXPORT).ok_or_else(invalid)?;
    let (position, payload) = payload.split_first_chunk::<8>().ok_or_else(invalid)?;
    let (time, checkpoint) = payload.split_first_chunk::<8>().ok_or_else(invalid)?;
    Ok((BranchCut {
        basis: Basis::new(*image.id().database.as_bytes(), u64::from_be_bytes(*position)),
        system_time: Timestamp(i64::from_be_bytes(*time)),
    }, checkpoint))
}
