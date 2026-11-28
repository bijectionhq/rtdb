//! Deferred checkpoints: capture under the database lock, build without it,
//! announce under it again.
//!
//! A deferred checkpoint describes the cut `through`, the head when the writer
//! captured it. Commits continue while the manifest is built and uploaded, so
//! the announcing record lands later. Installation selects the checkpoint
//! and trims through its cut. Replay uses the checkpoint at floor `through + 1`
//! and applies every retained record after it, so the commits between the cut
//! and the announcement are kept.

use std::sync::Arc;

use rtdb_crypt::Generation;
use rtdb_format::journal::{OrderedStoreHold, Checkpoint, Manifest, ObjectId};
use rtdb_store::{ObjectStore, ReservedManifest};
use rtdb_tx::transact::TransactState;
use rtdb_types::{Anomaly, Result};

use super::{Lease, MultiDatabase, backup::portable_attachment_files, no_writer, timing};
use crate::{
    journal::Stream,
    replay::{ClosureInputs, Committed, JournalCoordinates},
};
use journal_contract::Lsn;

/// Everything a checkpoint needs, captured under the database lock without
/// store I/O or serialization.
pub struct CheckpointCapture {
    lease: Lease,
    generation: u64,
    through: Lsn,
    coordinates: JournalCoordinates,
    store_purge_floor: u64,
    store_holds: Vec<OrderedStoreHold>,
    transactions: TransactState,
    closure: ClosureInputs,
    slot: ReservedManifest,
    binary: bool,
}

impl CheckpointCapture {
    /// The cut this checkpoint describes.
    pub fn through(&self) -> Lsn {
        self.through
    }
}

/// A checkpoint manifest uploaded for a captured cut and not yet announced.
pub struct PreparedCheckpoint {
    lease: Lease,
    generation: u64,
    through: Lsn,
    coordinates: JournalCoordinates,
    store_history_floor: u64,
    manifest: ObjectId,
}

impl PreparedCheckpoint {
    pub fn manifest(&self) -> ObjectId {
        self.manifest
    }

    pub fn through(&self) -> Lsn {
        self.through
    }
}

/// The store and keys a build needs, detached from the database.
pub struct CheckpointBuilder {
    stream: Stream,
    store: Arc<dyn ObjectStore>,
}

impl CheckpointBuilder {
    /// Resolves reachability from the captured closure, serializes the
    /// captured state, seals and uploads the manifest. Runs without the
    /// database lock.
    pub async fn build(&self, capture: CheckpointCapture) -> Result<PreparedCheckpoint> {
        let mut timing = timing::Phases::start();
        let closure = capture.closure.resolve(&self.stream, self.store.as_ref(), capture.lease.epoch).await?;
        let files = portable_attachment_files(&self.stream, self.store.as_ref(), capture.coordinates.origin).await?;
        closure.validate_publication(files.iter().copied())?;
        timing.mark("reachability");
        // Serializing the whole state is the longest step; keep it off the
        // runtime's worker threads.
        let (transactions, until, coordinates, store_purge_floor, binary) =
            (capture.transactions, capture.lease.until, capture.coordinates, capture.store_purge_floor, capture.binary);
        let store_history_floor = transactions.index().store_history_floor();
        let payload = tokio::task::spawn_blocking(move || {
            Committed::checkpoint_payload_in(&transactions, until, false, coordinates, store_purge_floor, binary)
        })
        .await
        .map_err(|error| Anomaly::fault("rtdb/checkpoint-build", error.to_string()))??;
        let manifest = Manifest { files, pins: closure.pins().cloned().collect(), store_holds: capture.store_holds, payload };
        timing.mark("payload");
        let image = capture.slot.seal(self.stream.kms.as_ref(), capture.generation, &manifest).await?;
        timing.mark("seal");
        image.put(self.store.as_ref()).await?;
        self.stream.manifests.insert(image.manifest_summary(
            &manifest,
            Generation { database: self.stream.database, number: capture.generation },
        )?);
        timing.mark("put");
        timing.report_phase("deferred_build", self.stream.database, manifest.payload.len(), capture.through);
        Ok(PreparedCheckpoint {
            lease: capture.lease,
            generation: capture.generation,
            through: capture.through,
            coordinates: capture.coordinates,
            store_history_floor,
            manifest: image.id(),
        })
    }
}

impl MultiDatabase {
    /// Charges the manifest summaries reachability keeps to `memory`. Without
    /// a budget, reachability reads every manifest from the store.
    pub fn with_manifest_budget(self, memory: Arc<rtdb_types::MemoryBudget>) -> Self {
        self.stream.manifests.charge_to(memory);
        self
    }

    /// Captures a deferred checkpoint of the current head once the retained
    /// suffix reaches `limit`. Cheap: one journal head read, a clone of the
    /// persistent transaction state and the closure inputs, and a manifest
    /// slot. Returns `None` when no checkpoint is due, this node is not the
    /// writer, or an erasure, key rotation or pending transaction must finish
    /// first.
    pub async fn capture_checkpoint(&mut self, limit: u64) -> Result<Option<CheckpointCapture>> {
        if self.writer.is_none() || self.authority().is_err() {
            return Ok(None);
        }
        let mut timing = timing::Phases::start();
        self.refresh().await?;
        timing.mark("refresh");
        let Ok(lease) = self.authority() else { return Ok(None) };
        let state = self.state();
        let retained = state.head.0.saturating_add(1).saturating_sub(state.floor.0);
        if retained < limit
            || !self.preserves_store_watch_history(state.head)?
            || state.transactions.pending_erasure().is_some()
            || state.transactions.recover_pending().is_some()
            || state.rotation.is_some()
        {
            return Ok(None);
        }
        let (through, generation, coordinates) = (state.head, state.generation, state.coordinates);
        let store_purge_floor = state.store_purge_floor;
        let store_holds = state.store_holds.checkpoint();
        let closure = state.closure_inputs();
        let transactions = self.durable_transactions().clone();
        let slot = self.writer.as_mut().ok_or_else(no_writer)?.objects.reserve_manifest()?;
        timing.mark("capture");
        timing.report_phase("deferred_capture", self.options.database, 0, through);
        Ok(Some(CheckpointCapture {
            lease,
            generation,
            through,
            coordinates,
            store_purge_floor,
            store_holds,
            transactions,
            closure,
            slot,
            binary: self.binary_checkpoints,
        }))
    }

    pub fn checkpoint_builder(&self) -> CheckpointBuilder {
        CheckpointBuilder { stream: self.stream.clone(), store: Arc::clone(&self.store) }
    }

    /// Installs a prepared checkpoint through its cut. Returns
    /// [`Announcement::Invalidated`] when the writer epoch, key generation or
    /// restored history changed since the capture, or an erasure or key
    /// rotation started; the next bound check captures again.
    pub async fn announce_checkpoint(&mut self, prepared: PreparedCheckpoint) -> Result<Announcement> {
        let mut timing = timing::Phases::start();
        self.refresh().await?;
        let lease = self.authority()?;
        let checkpoint = Checkpoint {
            manifest: prepared.manifest,
            through: prepared.through.0,
            key_generation: prepared.generation,
            restore: false,
            recovery_epoch: self.state().checkpoint_recovery_epoch(prepared.through),
        };
        let state = self.state();
        if lease.epoch != prepared.lease.epoch
            || !self.preserves_store_watch_history(prepared.through)?
            || state.generation != prepared.generation
            || state.coordinates.offset != prepared.coordinates.offset
            || state.coordinates.origin != prepared.coordinates.origin
            || state.transactions.pending_erasure().is_some()
            || state.rotation.is_some()
            || !(prepared.through == state.head || state.announces_deferred(&checkpoint))
        {
            return Ok(Announcement::Invalidated);
        }
        timing.mark("validate");
        self.install_checkpoint(lease, checkpoint, prepared.lease.until, prepared.store_history_floor).await?;
        timing.mark("install");
        timing.report_phase("deferred_announce", self.options.database, 0, prepared.through);
        Ok(Announcement::Announced(prepared.manifest))
    }
}

/// How the announcement of a prepared checkpoint ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Announcement {
    /// The checkpoint record committed and the journal was trimmed through its cut.
    Announced(ObjectId),
    /// The writer changed since the capture, so nothing was announced; its
    /// manifest is unreachable and collected like any orphan.
    Invalidated,
}

impl Announcement {
    pub fn manifest(self) -> Option<ObjectId> {
        match self {
            Self::Announced(manifest) => Some(manifest),
            Self::Invalidated => None,
        }
    }
}
