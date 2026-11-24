use std::{ops::AsyncFnOnce, sync::Arc};

use rtdb_crypt::{Kms, Plaintext};
use rtdb_format::journal::{Manifest, ObjectId, Pin, Record};
use rtdb_index::branches::BranchCut;
use rtdb_store::{ObjectStore, VerifiedManifest, read_file, read_manifest};
use rtdb_tx::transact::TransactState;
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp, rtdb_v1::Basis};
use uuid::Uuid;

use super::{MultiDatabase, next, no_writer};

const EXACT_EXPORT: &[u8; 5] = b"RTEX1";

pub struct PinnedExport {
    image: VerifiedManifest,
    store: Arc<dyn ObjectStore>,
    kms: Arc<dyn Kms>,
}

impl PinnedExport {
    pub fn manifest(&self) -> &Manifest {
        self.image.contents()
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
        if writer.pipeline.next_position() != next(journal_head)?.0 {
            return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb/export-pending-writes", "The writer is settling an admitted transaction"));
        }
        let transactions = self.durable_transactions();
        let cut = transactions.index().exact(basis)?;
        if transactions.index().has_custody_groups() {
            return Err(Anomaly::unsupported("rtdb/export-custody", "A protected export needs an admitted destination custody binding"));
        }
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
            image,
            store: Arc::clone(&self.store),
            kms: Arc::clone(&self.stream.kms),
        })
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
