use std::{ops::AsyncFnOnce, sync::Arc};

use rtdb_crypt::{Kms, Plaintext, tokens::ExportTicket};
use rtdb_format::journal::{Manifest, ObjectId, Pin, Record};
use rtdb_index::branches::{BranchCut, BranchIndex, CustodyOccurrence, CustodyPayload};
use rtdb_store::{ObjectStore, VerifiedManifest, read_file, read_manifest};
use rtdb_tx::transact::{CustodyWriterFence, Outcome, Receipt, Transact, TransactState};
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp, rtdb_v1::{Basis, DependentCustodyReceipt, ManagedCheckpointReservation, ManagedCheckpointTarget, NativeCheckpointCustody, NativeCustodyFence, NativeDependentCustodyOrder, NativeDependentCustodyScope, NativeDependentRead, NativeDependentSourceGeneration, PinnedCheckpointIdentity}};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::replay::{CheckpointSource, RestoreOrigin};

use super::{MultiDatabase, next, no_writer};

const EXACT_EXPORT: &[u8; 5] = b"RTEX1";
const MANAGED_EXPORT: &[u8; 5] = b"RTEX2";
const MANAGED_HEADER_BYTES: usize = 16 + 32 + 16;

#[derive(Clone, Copy)]
enum Restore<'a> {
    Plain,
    Managed,
    Native(&'a NativeCheckpointCustody),
}

/// Evidence read from the committed destination checkpoint and its readable generation.
pub struct NativeCheckpointInstallation {
    basis: Basis,
    custody: NativeCheckpointCustody,
}

pub struct NativeDependentOriginal {
    pub source_generation: NativeDependentSourceGeneration,
    pub owner: [u8; 32],
    pub application_scope: [u8; 32],
    pub enrollment: DependentCustodyReceipt,
    pub original: [u8; 32],
    pub carrier_sha256: [u8; 32],
    pub link_sha256: Option<[u8; 32]>,
    pub envelope_sha256: [u8; 32],
    pub receipt: Receipt,
}

pub struct NativeDependentCarrier {
    pub original: NativeDependentOriginal,
    pub contributors: Vec<(CustodyOccurrence, Receipt)>,
    pub payload: CustodyPayload,
}

impl NativeCheckpointInstallation {
    pub fn basis(&self) -> Basis { self.basis }
    pub fn custody(&self) -> &NativeCheckpointCustody { &self.custody }
}

pub struct PinnedExport {
    pin: Pin,
    image: VerifiedManifest,
    store: Arc<dyn ObjectStore>,
    kms: Arc<dyn Kms>,
}

impl PinnedExport {
    pub fn require_managed_reservation(&self, reservation: ManagedCheckpointReservation) -> Result<()> {
        if managed_binding(&self.image)? != Some((reservation.target, self.pin.id))
            || self.checkpoint_identity()? != reservation.source
            || reservation.reserved_at.incarnation() == [0; 16] || reservation.reserved_at.position() == 0
        { return Err(managed_export_required()); }
        Ok(())
    }

    pub fn checkpoint_identity(&self) -> Result<PinnedCheckpointIdentity> {
        Ok(PinnedCheckpointIdentity {
            basis: exact_payload(&self.image)?.0.basis,
            pin: *self.pin.id.as_bytes(),
            manifest_sha256: manifest_identity(self.image.id()),
            until: self.pin.until.map(Timestamp),
        })
    }

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
        self.pin_exact_checkpoint(id, basis, until, None).await
    }

    pub async fn pin_managed_checkpoint(&mut self, id: Uuid, basis: Basis, target: ManagedCheckpointTarget) -> Result<PinnedCheckpointIdentity> {
        if target.incarnation == [0; 16] || target.incarnation == self.incarnation() || target.request == [0; 32] {
            return Err(managed_export_required());
        }
        let pin = self.pin_exact_checkpoint(id, basis, None, Some(target)).await?;
        Ok(PinnedCheckpointIdentity { basis, pin: *pin.id.as_bytes(), manifest_sha256: manifest_identity(pin.manifest), until: None })
    }

    async fn pin_exact_checkpoint(&mut self, id: Uuid, basis: Basis, until: Option<Timestamp>, target: Option<ManagedCheckpointTarget>) -> Result<Pin> {
        Box::pin(async move {
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
            if managed_binding(&image)? != target.map(|target| (target, id)) || exact_payload(&image)?.0.basis != basis || pin.until != until.map(|time| time.0) {
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
        if target.is_some() && transactions.index().native_checkpoint_pin_used(*id.as_bytes()) {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb/native-checkpoint-pin-used", "The source pin already belongs to a durable native transfer"));
        }
        let cut = transactions.index().exact(basis)?;
        if target.is_none() { require_ordinary_inventory(transactions.index(), basis)?; }
        let checkpoint = if target.is_some() && transactions.index().has_custody_groups() {
            transactions.encode_native_checkpoint_at(basis)?
        } else {
            transactions.encode_checkpoint_at(basis)?
        };
        let mut payload = Vec::with_capacity(EXACT_EXPORT.len() + 16 + target.map_or(0, |_| MANAGED_HEADER_BYTES) + checkpoint.len());
        if let Some(target) = target {
            payload.extend_from_slice(MANAGED_EXPORT);
            payload.extend_from_slice(&target.incarnation);
            payload.extend_from_slice(&target.request);
            payload.extend_from_slice(id.as_bytes());
        } else {
            payload.extend_from_slice(EXACT_EXPORT);
        }
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
        }).await
    }

    pub async fn open_export(&mut self, id: Uuid) -> Result<PinnedExport> {
        let export = self.open_exact_pin(id).await?;
        require_ordinary_pin(&export.image)?;
        Ok(export)
    }

    pub async fn open_managed_checkpoint(&mut self, reservation: ManagedCheckpointReservation) -> Result<PinnedExport> {
        let export = self.open_exact_pin(Uuid::from_bytes(reservation.source.pin)).await?;
        export.require_managed_reservation(reservation)?;
        Ok(export)
    }

    async fn open_exact_pin(&mut self, id: Uuid) -> Result<PinnedExport> {
        let (pin, image) = self.open_pinned_manifest(id, false).await?;
        Ok(PinnedExport {
            pin,
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
        let identity = export.checkpoint_identity()?;
        if identity.manifest_sha256 != ticket.manifest_sha256 || identity.basis != ticket.basis
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
        require_ordinary_pin(&read_manifest(self.store.as_ref(), self.stream.kms.as_ref(), pin.manifest).await?)?;
        self.require_authority(lease)?;
        self.append_control(lease, Record::Unpin { id: ticket.pin }).await
    }

    /// The installed host supplies the exact reservation after its independent two-sided copy drain.
    pub async fn release_managed_checkpoint(&mut self, reservation: ManagedCheckpointReservation) -> Result<()> {
        Box::pin(async move {
        if self.incarnation() != reservation.source.basis.incarnation() { return Err(managed_export_required()); }
        self.refresh().await?;
        let lease = self.authority()?;
        let id = Uuid::from_bytes(reservation.source.pin);
        let closure = self.reachability(lease).await?;
        let Some(pin) = closure.pins().find(|pin| pin.id == id) else { return Ok(()); };
        let image = read_manifest(self.store.as_ref(), self.stream.kms.as_ref(), pin.manifest).await?;
        if managed_binding(&image)? != Some((reservation.target, id)) || manifest_identity(pin.manifest) != reservation.source.manifest_sha256
            || exact_payload(&image)?.0.basis != reservation.source.basis || pin.until.is_some() || reservation.source.until.is_some()
        { return Err(managed_export_required()); }
        let (cut, checkpoint) = exact_payload(&image)?;
        if TransactState::decode_checkpoint(checkpoint, cut)?.index().has_custody_groups()
            && !self.durable_transactions().index().native_checkpoint_forward(reservation)?
                .is_some_and(|(_, destination)| destination.is_some())
        {
            return Err(native_terminal_required());
        }
        self.require_authority(lease)?;
        self.append_control(lease, Record::Unpin { id }).await
        }).await
    }

    pub async fn claim_native_checkpoint_copy(&mut self, reservation: ManagedCheckpointReservation) -> Result<NativeCheckpointCustody> {
        Box::pin(async move {
        self.require_managed_checkpoint_writer().await?;
        if self.durable_transactions().index().native_checkpoint_forward(reservation)?.is_some() {
            return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb/native-checkpoint-copy-claimed", "The exact transfer already has a copy owner"));
        }
        let export = self.open_managed_checkpoint(reservation).await?;
        let (_, checkpoint) = export.exact_checkpoint().await?;
        checkpoint.require_native_checkpoint_receipts()?;
        let groups = checkpoint.index().native_checkpoint_groups()?;
        let vault = self.tokens.custody_vault();
        for &(group, _) in &groups { vault.check_readable(group).await?; }
        export.check_readable().await?;
        let state = self.state();
        let position = state.coordinates.position(next(state.head)?)?;
        if self.writer.as_ref().ok_or_else(no_writer)?.pipeline.next_position() != position
            || state.transactions.recover_pending().is_some()
        {
            return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb/native-checkpoint-copy-pending", "The original writer is settling an admitted transaction"));
        }
        let enrolled_at = Basis::new(self.incarnation(), position);
        let custody = NativeCheckpointCustody { reservation, groups, enrolled_at };
        let receipt = self.transact(&Transact::native_custody_transfer(custody.clone(), None)).await?;
        match receipt.outcome {
            Outcome::Then if !receipt.replayed && receipt.basis == enrolled_at => Ok(custody),
            Outcome::Refused { error } => Err(error),
            _ => Err(native_terminal_required()),
        }
        }).await
    }

    pub async fn record_native_checkpoint_drain(&mut self, installed: NativeCheckpointInstallation) -> Result<()> {
        Box::pin(async move {
        let NativeCheckpointInstallation { basis, custody } = installed;
        self.require_managed_checkpoint_writer().await?;
        match self.durable_transactions().index().native_checkpoint_forward(custody.reservation)? {
            Some((prior, Some(installed))) if prior == &custody && installed == basis => return Ok(()),
            Some((prior, None)) if prior == &custody => (),
            _ => return Err(native_terminal_required()),
        }
        let receipt = self.transact(&Transact::native_custody_transfer(custody, Some(basis))).await?;
        match receipt.outcome {
            Outcome::Then => Ok(()),
            Outcome::Refused { error } => Err(error),
            _ => Err(native_terminal_required()),
        }
        }).await
    }

    pub async fn require_native_dependent_fence(&mut self, scope: NativeDependentCustodyScope, fence: &NativeCustodyFence) -> Result<()> {
        self.require_managed_checkpoint_writer().await?;
        let state = self.durable_transactions();
        state.index().require_native_dependent_fence(scope, fence)?;
        state.require_native_custody_fence(fence)
    }

    pub async fn native_dependent_custody_status(&mut self, scope: NativeDependentCustodyScope, fence: &NativeCustodyFence) -> Result<Option<Receipt>> {
        self.require_managed_checkpoint_writer().await?;
        self.durable_transactions().native_dependent_custody_status(scope, fence)
    }

    pub async fn record_native_dependent_custody(&mut self, order: NativeDependentCustodyOrder) -> Result<Receipt> {
        self.record_native_dependent_in_generation(order, None).await
    }

    pub async fn record_native_dependent_in_generation(
        &mut self,
        order: NativeDependentCustodyOrder,
        expected: Option<NativeDependentSourceGeneration>,
    ) -> Result<Receipt> {
        Box::pin(async move {
        self.require_managed_checkpoint_writer().await?;
        if let Some(expected) = expected {
            let current = self.native_dependent_source_generation(order.scope()).await?;
            if current != expected { return Err(native_terminal_required()); }
        }
        let state = self.durable_transactions();
        let prior = state.index().native_dependent_custody(order.scope())?;
        let recorded = match &order {
            NativeDependentCustodyOrder::Enroll(enrollment) => prior.is_some_and(|(prior, _)| prior == enrollment),
            NativeDependentCustodyOrder::Dispose(disposition) => {
                state.require_native_custody_fence(&disposition.fence)?;
                prior.is_some_and(|(_, prior)| prior == Some(disposition))
            }
        };
        if !recorded {
            state.index().check_native_dependent_custody(&order)?;
        }
        let receipt = self.transact(&Transact::native_dependent_custody(order)).await?;
        match &receipt.outcome {
            Outcome::Then => Ok(receipt),
            Outcome::Refused { error } => Err(error.clone()),
            _ => Err(native_terminal_required()),
        }
        }).await
    }

    /// Install an independent backing for an exact export. The destination's
    /// journal and KMS own all bytes needed to recover after the append.
    pub async fn restore_export(&mut self, export: &PinnedExport) -> Result<BranchCut> {
        require_ordinary_pin(&export.image)?;
        self.restore_exact_export(export, Restore::Plain).await
    }

    pub async fn restore_managed_checkpoint(&mut self, export: &PinnedExport, reservation: ManagedCheckpointReservation) -> Result<BranchCut> {
        export.require_managed_reservation(reservation)?;
        if self.incarnation() != reservation.target.incarnation { return Err(managed_export_required()); }
        self.restore_exact_export(export, Restore::Managed).await
    }

    pub async fn restore_native_checkpoint(&mut self, export: &PinnedExport, custody: &NativeCheckpointCustody) -> Result<BranchCut> {
        export.require_managed_reservation(custody.reservation)?;
        if self.incarnation() != custody.reservation.target.incarnation { return Err(managed_export_required()); }
        self.restore_exact_export(export, Restore::Native(custody)).await
    }

    pub async fn require_managed_checkpoint_writer(&mut self) -> Result<()> {
        self.refresh().await?;
        self.require_active()?;
        self.authority()?;
        Ok(())
    }

    /// The host holds the private catalog reservation while checking this writer.
    pub async fn require_managed_checkpoint_destination(&mut self, reservation: ManagedCheckpointReservation) -> Result<()> {
        self.refresh().await?;
        let lease = self.authority()?;
        let state = self.state();
        if self.incarnation() != reservation.target.incarnation || state.coordinates.origin.is_some()
            || state.transactions.index().head().basis.position() != 0
            || state.records.iter().any(|record| !matches!(record, Record::Lease { .. } | Record::WriterRelease { .. }))
            || self.writer.as_ref().ok_or_else(no_writer)?.pipeline.next_position() != next(state.head)?.0
        {
            return Err(managed_export_required());
        }
        self.check_generation(state.generation).await?;
        self.require_authority(lease)
    }

    async fn restore_exact_export(&mut self, export: &PinnedExport, restore: Restore<'_>) -> Result<BranchCut> {
        Box::pin(async move {
        let source = export.image.id();
        if source.database == self.options.database {
            return Err(Anomaly::incorrect("rtdb/restore-same-deployment", "Restore needs a distinct deployment"));
        }
        if !export.manifest().files.is_empty() || matches!(restore, Restore::Native(_)) && !export.manifest().pins.is_empty() {
            return Err(Anomaly::unsupported("rtdb/export-files", "This export needs independent destination file placement"));
        }
        self.refresh().await?;
        let lease = self.authority()?;
        let source_identity = manifest_identity(source);
        let state = self.state();
        let through = state.head;
        let destination = self.incarnation();
        if let Some(origin) = state.coordinates.origin
            && origin.digest == source_identity
        {
            let expected = match restore { Restore::Native(custody) => Some(custody), _ => None };
            if state.transactions.index().inherited_custody() != expected { return Err(native_terminal_required()); }
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
        match restore {
            Restore::Native(custody) => transactions.restore_native_checkpoint(custody)?,
            Restore::Plain | Restore::Managed => {
                if matches!(restore, Restore::Plain) { require_ordinary_inventory(transactions.index(), source_cut.basis)?; }
                transactions.restore_in(destination)?;
            }
        }
        let origin = RestoreOrigin {
            digest: source_identity,
            through: transactions.index().head().basis.position(),
            source: Some(CheckpointSource { cut: source_cut, pin: export.pin.id }),
            backup: None,
        };
        self.install_restored_state(lease, transactions, origin, async || export.check_readable().await).await
        }).await
    }

    pub async fn checkpoint_source(&mut self) -> Result<Option<CheckpointSource>> {
        self.refresh().await?;
        Ok(self.state().coordinates.origin.and_then(|origin| origin.source))
    }

    pub async fn require_managed_checkpoint_restore(&mut self, reservation: ManagedCheckpointReservation) -> Result<(Basis, BranchIndex, u64)> {
        self.refresh().await?;
        self.require_active()?;
        let state = self.state();
        let origin = state.coordinates.origin.ok_or_else(managed_export_required)?;
        if self.incarnation() != reservation.target.incarnation || origin.digest != reservation.source.manifest_sha256
            || origin.source.is_none_or(|source| source.cut.basis != reservation.source.basis || *source.pin.as_bytes() != reservation.source.pin)
        { return Err(managed_export_required()); }
        let index = state.transactions.index();
        let basis = index.exact(Basis::new(self.incarnation(), origin.through))?.basis;
        self.check_generation(state.generation).await?;
        Ok((basis, index.clone(), state.generation))
    }

    pub async fn native_checkpoint_installation(&mut self, reservation: ManagedCheckpointReservation) -> Result<NativeCheckpointInstallation> {
        let (basis, index, _) = self.require_managed_checkpoint_restore(reservation).await?;
        let custody = index.inherited_custody().filter(|custody| custody.reservation == reservation)
            .ok_or_else(native_terminal_required)?;
        Ok(NativeCheckpointInstallation { basis, custody: custody.clone() })
    }

    pub async fn native_dependent_original(
        &mut self,
        custody: &NativeCheckpointCustody,
        read: &NativeDependentRead,
        writer: &CustodyWriterFence,
    ) -> Result<NativeDependentOriginal> {
        self.refresh().await?;
        let lease = self.authority()?;
        let generation = self.state().generation;
        self.check_generation(generation).await?;
        self.require_authority(lease)?;
        let now = (self.clock)();
        let transactions = self.durable_transactions();
        let index = transactions.index();
        if !writer.matches(index, now)? {
            return Err(native_terminal_required());
        }
        let (enrollment, original, occurrence) = index.native_dependent_original(custody, read, now)?;
        let receipt = transactions.original_custody_receipt(&occurrence.receipt_key, occurrence.written)?;
        Ok(NativeDependentOriginal {
            source_generation: NativeDependentSourceGeneration {
                pinned_source: read.scope.reservation.source.basis,
                writer_epoch: lease.epoch.0,
                key_generation: generation,
            },
            owner: enrollment.owner,
            application_scope: enrollment.application_scope,
            enrollment: enrollment.receipt,
            original,
            carrier_sha256: occurrence.carrier_sha256,
            link_sha256: occurrence.link_sha256,
            envelope_sha256: occurrence.envelope_sha256,
            receipt,
        })
    }

    pub async fn native_dependent_carrier(
        &mut self, custody: &NativeCheckpointCustody, read: &NativeDependentRead,
        writer: &CustodyWriterFence, binding: [u8; 32],
    ) -> Result<NativeDependentCarrier> {
        let original = self.native_dependent_original(custody, read, writer).await?;
        let transactions = self.durable_transactions();
        let index = transactions.index();
        let payload = index.custody_occurrence_payload(read.scope.reservation.source.basis, read.scope.group, read.occurrence)?;
        if payload.binding != binding || payload.envelope.len() > 768 * 1024 {
            return Err(native_terminal_required());
        }
        let contributors = index.native_dependent_contributors(read)?.into_values().map(|occurrence| {
            let receipt = transactions.original_custody_receipt(&occurrence.receipt_key, occurrence.written)?;
            Ok((occurrence.clone(), receipt))
        }).collect::<Result<_>>()?;
        Ok(NativeDependentCarrier { original, contributors, payload: payload.clone() })
    }

    pub async fn native_dependent_source_generation(
        &mut self,
        scope: NativeDependentCustodyScope,
    ) -> Result<NativeDependentSourceGeneration> {
        self.refresh().await?;
        let lease = self.authority()?;
        self.durable_transactions().index().native_dependent_custody(scope)?;
        let generation = self.state().generation;
        self.check_generation(generation).await?;
        self.require_authority(lease)?;
        Ok(NativeDependentSourceGeneration {
            pinned_source: scope.reservation.source.basis,
            writer_epoch: lease.epoch.0,
            key_generation: generation,
        })
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
    let raw = &image.contents().payload;
    let payload = if let Some(managed) = raw.strip_prefix(MANAGED_EXPORT) {
        managed.get(MANAGED_HEADER_BYTES..).ok_or_else(invalid)?
    } else {
        raw.strip_prefix(EXACT_EXPORT).ok_or_else(invalid)?
    };
    let (position, payload) = payload.split_first_chunk::<8>().ok_or_else(invalid)?;
    let (time, checkpoint) = payload.split_first_chunk::<8>().ok_or_else(invalid)?;
    Ok((BranchCut {
        basis: Basis::new(*image.id().database.as_bytes(), u64::from_be_bytes(*position)),
        system_time: Timestamp(i64::from_be_bytes(*time)),
    }, checkpoint))
}

fn managed_binding(image: &VerifiedManifest) -> Result<Option<(ManagedCheckpointTarget, Uuid)>> {
    let Some(raw) = image.contents().payload.strip_prefix(MANAGED_EXPORT) else { return Ok(None); };
    let (incarnation, raw) = raw.split_first_chunk::<16>().ok_or_else(managed_export_required)?;
    let (request, raw) = raw.split_first_chunk::<32>().ok_or_else(managed_export_required)?;
    let (pin, _) = raw.split_first_chunk::<16>().ok_or_else(managed_export_required)?;
    if *incarnation == [0; 16] || *request == [0; 32] || *pin == [0; 16] { return Err(managed_export_required()); }
    Ok(Some((ManagedCheckpointTarget { incarnation: *incarnation, request: *request }, Uuid::from_bytes(*pin))))
}

pub(super) fn require_ordinary_pin(image: &VerifiedManifest) -> Result<()> {
    if managed_binding(image)?.is_some() { return Err(managed_export_required()); }
    Ok(())
}

fn require_ordinary_inventory(index: &BranchIndex, basis: Basis) -> Result<()> {
    if index.retained_images().any(|(_, table, event, image)| event.cut.basis <= basis && BranchIndex::unowned_checkpoint_image(table, image)) {
        return Err(managed_export_required());
    }
    Ok(())
}

fn managed_export_required() -> Anomaly {
    Anomaly::unsupported("rtdb/managed-checkpoint-custody", "The checkpoint requires its exact managed custody reservation")
}

fn native_terminal_required() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb/native-checkpoint-terminal", "The native copy has no matching ordered destination terminal")
}
