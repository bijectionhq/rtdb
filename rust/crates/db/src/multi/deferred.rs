//! Deferred checkpoints: capture under the database lock, build without it,
//! announce under it again.
//!
//! A deferred checkpoint describes the cut `through`, the head when the writer
//! captured it. Commits continue while the manifest is built and uploaded, so
//! the announcing record lands later. The writer then trims only through the
//! cut. Replay installs the checkpoint as the base of the floor `through + 1`
//! and applies every retained record after it, so the commits between the cut
//! and the announcement are kept.

use std::sync::Arc;

use rtdb_crypt::Generation;
use rtdb_format::journal::{Checkpoint, Manifest, ObjectId, Record};
use rtdb_store::{ObjectStore, ReservedManifest};
use rtdb_tx::transact::TransactState;
use rtdb_types::{Anomaly, Result};

use super::{Lease, MultiDatabase, no_writer, timing};
use crate::{
    journal::Stream,
    replay::{ClosureInputs, Committed, JournalCoordinates, ReplayBase},
};
use journal_contract::Lsn;

/// Everything a checkpoint needs, captured under the database lock without
/// store I/O or serialization.
pub struct CheckpointCapture {
    lease: Lease,
    generation: u64,
    through: Lsn,
    coordinates: JournalCoordinates,
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
        timing.mark("reachability");
        // Serializing the whole state is the longest step; keep it off the
        // runtime's worker threads.
        let (transactions, until, coordinates, binary) =
            (capture.transactions, capture.lease.until, capture.coordinates, capture.binary);
        let payload = tokio::task::spawn_blocking(move || {
            Committed::checkpoint_payload_in(&transactions, until, false, coordinates, binary)
        })
        .await
        .map_err(|error| Anomaly::fault("rtdb/checkpoint-build", error.to_string()))??;
        let manifest = Manifest { files: Vec::new(), pins: closure.pins().cloned().collect(), payload };
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
            || state.transactions.pending_erasure().is_some()
            || state.transactions.recover_pending().is_some()
            || state.rotation.is_some()
        {
            return Ok(None);
        }
        let (through, generation, coordinates) = (state.head, state.generation, state.coordinates);
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
            transactions,
            closure,
            slot,
            binary: self.binary_checkpoints,
        }))
    }

    pub fn checkpoint_builder(&self) -> CheckpointBuilder {
        CheckpointBuilder { stream: self.stream.clone(), store: Arc::clone(&self.store) }
    }

    /// Announces a prepared checkpoint and trims through its cut. Returns
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
        };
        let state = self.state();
        if lease.epoch != prepared.lease.epoch
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
        self.append_control(lease, Record::Checkpoint(checkpoint.clone())).await?;
        timing.mark("append");
        let announced = self.state().head;
        self.trim_to_base(lease, ReplayBase { announced, checkpoint }, prepared.lease.until).await?;
        timing.mark("trim");
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
