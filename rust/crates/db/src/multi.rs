use std::{collections::BTreeSet, net::IpAddr, sync::{Arc, Weak}};

use journal_contract::{Epoch, Journal, JournalError, Lsn};
use rtdb_crypt::{Generation, Kms, tokens::TokenAuthority};
use rtdb_format::journal::{Checkpoint, Manifest, ObjectId, ObjectKind, Pin, Record};
use rtdb_index::branches::{BranchEffect, BranchIndex};
use rtdb_store::{
    ObjectKey, ObjectStore, Reachability, StoredTokenKeys, VerifiedManifest, WriterObjects, read_manifest,
};
use rtdb_tx::transact::{
    Erasure, Receipt, ScopedTransact, Transact, TransactPipeline, TransactState, TransactionRecord,
};
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp, rtdb_v1::Basis};
use uuid::Uuid;

use crate::{
    journal::{Entry, JournalBatchAppender, Stream, journal_error, next},
    placement::preferred,
    replay::{ChangeHistory, Committed, JournalCoordinates, ReplayError},
};

mod classic_migration;
mod pin;
mod pipeline;
mod ranked;
mod rotation;
mod retirement;
mod timing;
pub use classic_migration::{ClassicInstallation, classic_import_basis};
pub use pin::{NativeCheckpointInstallation, NativeDependentCarrier, NativeDependentOriginal, PinnedExport};
pub use pipeline::WritePipeline;
pub use ranked::{RankedBuild, RankedFiles, RankedPublication};
use pipeline::{Append, AttemptError, Planned, receipt_for};
use rotation::ArmedRotation;
pub use rotation::RotationReceipt;
pub use retirement::DeploymentRetirement;

pub struct MultiOptions {
    pub database: Uuid,
    pub node: IpAddr,
    pub lease_micros: i64,
    pub drift_micros: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
    Follower,
    Writer(Epoch),
}

#[derive(Clone, Copy)]
struct Lease {
    epoch: Epoch,
    until: i64,
}

struct Writer {
    lease: Lease,
    pipeline: TransactPipeline,
    acknowledged: Lsn,
    recovering: bool,
    rotation: Option<ArmedRotation>,
    objects: WriterObjects,
    uploaded: BTreeSet<ObjectId>,
}

/// One serialized database residence. The owner drives ticks and serving calls.
pub struct MultiDatabase {
    options: MultiOptions,
    stream: Stream,
    store: Arc<dyn ObjectStore>,
    clock: Arc<dyn Fn() -> Timestamp + Send + Sync>,
    token_keys: Arc<StoredTokenKeys>,
    tokens: TokenAuthority,
    row_custody: bool,
    committed: Option<Committed>,
    applied: tokio::sync::watch::Sender<u64>,
    writer: Option<Writer>,
    candidate: Option<Lease>,
    last_allocated: Epoch,
    competing_seal: Option<Lease>,
    assigned_follower: Weak<()>,
}

impl MultiDatabase {
    pub fn new(
        options: MultiOptions,
        journal: Arc<dyn Journal>,
        store: Arc<dyn ObjectStore>,
        kms: Arc<dyn Kms>,
        clock: Arc<dyn Fn() -> Timestamp + Send + Sync>,
    ) -> Result<Self> {
        if options.drift_micros < 0
            || options.lease_micros <= options.drift_micros.saturating_mul(2)
        {
            return Err(Anomaly::incorrect(
                "rtdb/invalid-writer-lease",
                "Writer lease must exceed twice the clock drift",
            ));
        }
        let token_keys = Arc::new(StoredTokenKeys::new(
            Arc::clone(&store), format!("~rtdb-v1/{}", options.database.simple()), options.database, 1,
        ));
        let tokens = TokenAuthority::new(options.database, Arc::clone(&kms), token_keys.clone());
        Ok(Self {
            stream: Stream::new(options.database, journal, kms),
            options,
            store,
            clock,
            token_keys,
            tokens,
            row_custody: false,
            committed: None,
            applied: tokio::sync::watch::channel(0).0,
            writer: None,
            candidate: None,
            last_allocated: Epoch(0),
            competing_seal: None,
            assigned_follower: Weak::new(),
        })
    }

    pub async fn create(&self) -> Result<()> {
        self.stream.create().await?;
        self.token_keys.install(self.stream.kms.as_ref(), 1).await?;
        Ok(())
    }

    pub fn token_authority(&self) -> TokenAuthority { self.tokens.clone() }

    pub fn with_row_custody(mut self) -> Result<Self> {
        if !self.stream.kms.supports_durable_generation_retirement() {
            return Err(Anomaly::unsupported("rtdb/row-custody", "Multi row custody requires durable key retirement"));
        }
        self.row_custody = true;
        Ok(self)
    }

    pub fn supports_row_custody(&self) -> bool { self.row_custody }

    pub async fn finish_token_rotation(&mut self, generation: u64, unreadable_by: i64) -> Result<()> {
        self.refresh().await?;
        if self.state().generation < generation { return Err(no_writer()); }
        self.token_keys.rotate(self.stream.kms.as_ref(), generation, unreadable_by).await
    }

    pub async fn destroy_retired_token_key(&self, generation: u64) -> Result<()> {
        self.token_keys.destroy_retired(self.stream.kms.as_ref(), generation).await
    }

    pub fn with_batch_appender(mut self, batch: Arc<dyn JournalBatchAppender>) -> Self {
        self.stream.batch = Some(batch);
        self
    }

    pub fn incarnation(&self) -> [u8; 16] {
        *self.options.database.as_bytes()
    }

    pub fn role(&self) -> Role {
        self.writer
            .as_ref()
            .filter(|writer| !writer.recovering && self.before(writer.lease.until))
            .map_or(Role::Follower, |writer| Role::Writer(writer.lease.epoch))
    }

    pub async fn tick(&mut self, live_nodes: &BTreeSet<IpAddr>) -> Result<Role> {
        if self.assigned_follower.strong_count() != 0 {
            self.refresh().await?;
            return Ok(Role::Follower);
        }
        let selected = preferred(self.options.database, live_nodes) == Some(self.options.node);
        self.recover_writer().await?;
        if let Err(error) = self.refresh().await {
            if selected
                && error.code == "rtdb/journal-unavailable"
                && self.committed.is_some()
                && self.candidate.is_some_and(|lease| self.before(lease.until))
            {
                self.promote().await?;
                return Ok(self.role());
            }
            return Err(error);
        }
        if self.state().retired.is_some() { return Ok(Role::Follower); }
        if let Some(writer) = &self.writer {
            let lease = writer.lease;
            self.refuse_pending(lease).await?;
            self.finish_pending_erasure().await?;
            if !selected && !live_nodes.is_empty() {
                self.append_writer_release(lease).await?;
                self.withdraw_writer();
                return Ok(Role::Follower);
            }
            if self.upper().saturating_add(self.options.lease_micros / 2) >= lease.until {
                self.renew().await?;
            }
            return Ok(self.role());
        }
        if selected {
            self.promote().await?;
        } else {
            self.candidate = None;
        }
        Ok(self.role())
    }

    async fn promote(&mut self) -> Result<()> {
        if !self.observed_lapse() {
            return Ok(());
        }
        let seen = self.state().epoch.max(self.last_allocated);
        let attempt = match self
            .candidate
            .filter(|lease| lease.epoch > seen && self.before(lease.until))
        {
            Some(attempt) => attempt,
            None => {
                let epoch = match self.competing_seal.filter(|lease| lease.epoch > seen) {
                    Some(lease) if !self.observed_deadline(lease.until) => return Ok(()),
                    Some(lease) => lease.epoch,
                    None => seen,
                };
                let attempt = Lease {
                    epoch: Epoch(next(Lsn(epoch.0))?.0),
                    until: self.lease_end(),
                };
                self.candidate = Some(attempt);
                attempt
            }
        };
        let end = match self.stream.seal(attempt.epoch).await {
            Ok(end) => end,
            Err(JournalError::Fenced { current }) => {
                self.candidate = None;
                self.note_competing_seal(current);
                return Ok(());
            }
            Err(error) => return Err(journal_error(error)),
        };
        self.catch_up(end).await?;
        if !self.observed_lapse() || !self.before(attempt.until) {
            return Ok(());
        }
        self.candidate = None;
        self.last_allocated = attempt.epoch;
        self.refuse_pending(attempt).await?;
        self.state()
            .reachability(&self.stream, self.store.as_ref(), attempt.epoch)
            .await?;
        if self.before(attempt.until) {
            self.writer = Some(Writer {
                lease: attempt,
                pipeline: self.new_pipeline()?,
                acknowledged: self.state().head,
                recovering: false,
                rotation: None,
                objects: WriterObjects::new(self.options.database, attempt.epoch.0),
                uploaded: BTreeSet::new(),
            });
            if let Err(error) = self.finish_pending_erasure().await {
                self.withdraw_writer();
                return Err(error);
            }
            if let Err(error) = self
                .append_control(
                    attempt,
                    Record::Lease {
                        until: attempt.until,
                    },
                )
                .await
            {
                self.withdraw_writer();
                return Err(error);
            }
        }
        Ok(())
    }

    fn note_competing_seal(&mut self, epoch: Epoch) {
        if self.competing_seal.is_none_or(|lease| epoch > lease.epoch) {
            self.competing_seal = Some(Lease {
                epoch,
                until: self.lease_end(),
            });
        }
    }

    async fn renew(&mut self) -> Result<()> {
        let current = self.authority()?;
        let until = self.lease_end();
        if until > current.until {
            self.append_control(current, Record::Lease { until })
                .await?;
            self.writer.as_mut().ok_or_else(no_writer)?.lease.until = until;
        }
        Ok(())
    }

    async fn refuse_pending(&mut self, lease: Lease) -> Result<()> {
        if let Some(terminal) = self.state().transactions.recover_pending() {
            if self.writer.is_some() {
                self.withdraw_writer();
                return Err(no_writer());
            }
            self.append(lease, envelope(terminal, self.state().coordinates)?).await?;
        }
        Ok(())
    }

    pub async fn transact(&mut self, request: &Transact) -> Result<Receipt> {
        self.transact_in(request, None).await
    }

    pub async fn transact_scoped(
        &mut self,
        request: &Transact,
        scope: ScopedTransact<'_>,
    ) -> Result<Receipt> {
        self.transact_in(request, Some(scope)).await
    }

    async fn transact_in(
        &mut self,
        request: &Transact,
        scope: Option<ScopedTransact<'_>>,
    ) -> Result<Receipt> {
        let result = self.transact_serial(request, scope).await;
        if result.is_err() {
            let _ = self.recover_writer().await;
        }
        self.project_receipt(result?, scope)
    }

    async fn transact_serial(
        &mut self,
        request: &Transact,
        scope: Option<ScopedTransact<'_>>,
    ) -> Result<Receipt> {
        self.refresh().await?;
        self.require_active()?;
        if request.uses_erasure() {
            self.finish_pending_erasure().await?;
            if self.rotation_destroyed() && self.check_erase_storage(self.authority()?).await.is_ok() {
                self.checkpoint(Vec::new()).await?;
            }
        }
        let claims = self
            .tokens
            .verify_claims_in(request.claims(), scope.map(|scope| scope.labels))
            .await;
        let write = match self.decide_one(request, scope, &claims)? {
            Planned::Answer(receipt) => return Ok(receipt),
            Planned::Append(write) => write,
        };
        let lease = self.authority()?;
        if request.uses_erasure() && write_erases(&write)?
            && let Err(error) = self.check_erase_storage(lease).await
        {
            let pipeline = self.new_pipeline()?;
            self.writer.as_mut().ok_or_else(no_writer)?.pipeline = pipeline;
            return Err(error);
        }
        let original = write.at;
        let (upper, receipts) = self.commit_one(lease, write).await?;
        if request.window.before.is_none() {
            let receipt = receipt_for(receipts, self.state().coordinates.position(original)?, &request.key)?;
            return self.complete_erase_receipt(receipt, scope).await;
        }
        let (terminal, _) = self
            .decide_terminal(lease, original, upper)
            .map_err(|error| self.begin_write_recovery(AttemptError::Local(error)))?;
        let position = terminal.at;
        let (_, receipts) = self.commit_one(lease, terminal).await?;
        let receipt = receipt_for(receipts, self.state().coordinates.position(position)?, &request.key)?;
        self.complete_erase_receipt(receipt, scope).await
    }

    pub async fn original_custody_receipt(&mut self, key: &[u8], basis: Basis) -> Result<Receipt> {
        self.refresh().await?;
        self.require_active()?;
        self.durable_transactions().original_custody_receipt(key, basis)
    }

    pub async fn original_writer_receipt(&mut self, key: &[u8], basis: Basis) -> Result<Receipt> {
        self.refresh().await?;
        self.require_active()?;
        self.durable_transactions().original_writer_receipt(key, basis)
    }

    pub async fn original_line_creation_receipt(&mut self, key: &[u8], basis: Basis) -> Result<Receipt> {
        self.refresh().await?;
        self.require_active()?;
        self.durable_transactions().original_line_creation_receipt(key, basis)
    }

    pub async fn tx_status(&mut self, key: &[u8]) -> Result<Receipt> {
        self.tx_status_in(key, None).await
    }

    pub async fn tx_status_scoped(
        &mut self,
        key: &[u8],
        scope: ScopedTransact<'_>,
    ) -> Result<Receipt> {
        self.tx_status_in(key, Some(scope)).await
    }

    async fn tx_status_in(
        &mut self,
        key: &[u8],
        scope: Option<ScopedTransact<'_>>,
    ) -> Result<Receipt> {
        self.refresh().await?;
        self.require_active()?;
        let writer = self.writer.as_ref().filter(|writer| !writer.recovering);
        let receipt = match (writer, scope) {
            (Some(writer), Some(scope)) => writer.pipeline.tx_status_scoped(key, scope),
            (Some(writer), None) => writer.pipeline.tx_status(key),
            (None, Some(scope)) => self.state().transactions.tx_status_scoped(key, scope),
            (None, None) => self.state().transactions.tx_status(key),
        }?;
        if receipt.erasure == Some(Erasure::Pending) {
            return Err(erasure_pending());
        }
        self.project_receipt(receipt, scope)
    }

    fn project_receipt(
        &self,
        receipt: Receipt,
        scope: Option<ScopedTransact<'_>>,
    ) -> Result<Receipt> {
        match scope {
            Some(scope) => self
                .durable_transactions()
                .project_receipt(receipt, scope.labels),
            None => Ok(receipt),
        }
    }

    pub async fn fresh_index(&mut self) -> Result<&BranchIndex> {
        self.refresh().await?;
        self.require_active()?;
        Ok(self.durable_transactions().index())
    }

    pub async fn fresh_view(&mut self) -> Result<(BranchIndex, u64)> {
        self.refresh().await?;
        self.require_active()?;
        let state = self.state();
        Ok((
            self.durable_transactions().index().clone(),
            state.generation,
        ))
    }

    pub async fn assign_follower(&mut self, owner: &Arc<()>) -> Result<()> {
        if self.writer.is_some() || self.candidate.is_some() || self.assigned_follower.strong_count() != 0 {
            return Err(follower_unavailable());
        }
        self.refresh().await?;
        self.require_active()?;
        self.assigned_follower = Arc::downgrade(owner);
        Ok(())
    }

    pub fn follower_view(&self, owner: &Arc<()>) -> Result<(BranchIndex, u64)> {
        let state = self.follower_state(owner)?;
        Ok((state.transactions.index().clone(), state.generation))
    }

    fn follower_state(&self, owner: &Arc<()>) -> Result<&Committed> {
        if !self.assigned_follower.ptr_eq(&Arc::downgrade(owner)) || self.writer.is_some() {
            return Err(follower_unavailable());
        }
        self.require_active()?;
        self.committed.as_ref().ok_or_else(follower_unavailable)
    }

    pub async fn follower_writer_head(&mut self, owner: &Arc<()>) -> Result<(Basis, u64)> {
        self.follower_state(owner)?;
        self.refresh().await?;
        let state = self.follower_state(owner)?;
        if !state.lease.is_some_and(|(epoch, until)| epoch == state.epoch && self.before(until)) {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb/follower-progress-unavailable", "The committed journal has no current writer lease"));
        }
        let index = state.transactions.index();
        let cut = index.head();
        Ok((cut.basis, index.applied_position(cut)?))
    }

    pub async fn fresh_history(&mut self) -> Result<ChangeHistory<'_>> {
        self.refresh().await?;
        self.require_active()?;
        let state = self.state();
        let after_floor = state.coordinates.position(Lsn(state.floor.0 - 1))?
            .max(state.coordinates.origin.map_or(0, |origin| origin.through));
        Ok(ChangeHistory {
            index: self.durable_transactions().index(),
            after_floor,
            through: state.coordinates.position(state.head)?,
            generation: state.generation,
            groups: &state.effects,
            now: (self.clock)(),
        })
    }

    pub async fn check_generation(&self, generation: u64) -> Result<()> {
        self.stream
            .kms
            .check_readable(Generation {
                database: self.options.database,
                number: generation,
            })
            .await
    }

    pub fn generation(&self) -> Option<u64> {
        self.committed.as_ref().map(|state| state.generation)
    }

    /// The journal position this residence has applied, marked changed
    /// whenever it moves.
    pub fn watch_applied(&self) -> tokio::sync::watch::Receiver<u64> {
        self.applied.subscribe()
    }

    fn install_committed(&mut self, state: Committed) {
        let head = state.head.0;
        self.applied.send_if_modified(|applied| std::mem::replace(applied, head) != head);
        self.committed = Some(state);
    }

    /// The journal stream holding this database.
    pub fn journal_stream(&self) -> journal_contract::StreamId {
        journal_contract::StreamId(u128::from_be_bytes(*self.stream.database.as_bytes()))
    }

    pub fn retained_floor(&self) -> Option<Lsn> {
        self.committed.as_ref().map(|state| state.floor)
    }

    pub async fn write_file(&mut self, data: &[u8]) -> Result<ObjectId> {
        self.refresh().await?;
        let lease = self.authority()?;
        self.require_armed_rotation()?;
        let generation = self.state().generation;
        let writer = self.writer.as_mut().ok_or_else(no_writer)?;
        let file = writer
            .objects
            .file(self.stream.kms.as_ref(), generation, data)
            .await?;
        file.put(self.store.as_ref()).await?;
        self.require_authority(lease)?;
        self.writer
            .as_mut()
            .ok_or_else(no_writer)?
            .uploaded
            .insert(file.id());
        Ok(file.id())
    }

    /// Checkpoints and trims when the writer retains at least `limit`
    /// journal records. A journal that bounds its retained records, such as
    /// a consensus stream, refuses appends once it reaches that bound.
    /// Returns the new checkpoint, or `None` when no checkpoint is due or
    /// this node is not the writer.
    pub async fn checkpoint_retained_suffix(&mut self, limit: u64) -> Result<Option<ObjectId>> {
        if self.writer.is_none() || self.authority().is_err() {
            return Ok(None);
        }
        let Some(state) = self.committed.as_ref() else {
            return Ok(None);
        };
        let retained = state.head.0.saturating_add(1).saturating_sub(state.floor.0);
        if retained < limit || self.state().transactions.pending_erasure().is_some() {
            return Ok(None);
        }
        self.checkpoint(Vec::new()).await.map(Some)
    }

    pub async fn checkpoint(&mut self, files: Vec<ObjectId>) -> Result<ObjectId> {
        let mut timing = timing::Phases::start();
        self.refresh().await?;
        timing.mark("refresh");
        let lease = self.authority()?;
        if self.state().transactions.pending_erasure().is_some() {
            return Err(erasure_pending());
        }
        self.require_destroyed_generation()?;
        let closure = self.reachability(lease).await?;
        timing.mark("reachability");
        self.validate_files(&closure, &files)?;
        let manifest = Manifest {
            files,
            pins: closure.pins().cloned().collect(),
            payload: Committed::checkpoint_payload(
                self.durable_transactions(),
                lease.until,
                false,
                self.state().coordinates,
            )?,
        };
        timing.mark("payload");
        let through = self.state().head;
        let generation = self.state().generation;
        let writer = self.writer.as_mut().ok_or_else(no_writer)?;
        let image = writer
            .objects
            .manifest(self.stream.kms.as_ref(), generation, &manifest)
            .await?;
        timing.mark("seal");
        image.put(self.store.as_ref()).await?;
        timing.mark("put");
        let checkpoint = Checkpoint {
            manifest: image.id(),
            through: through.0,
            key_generation: generation,
            restore: false,
        };
        self.append_control(lease, Record::Checkpoint(checkpoint))
            .await?;
        timing.mark("append");
        self.trim(lease, through).await?;
        timing.mark("trim");
        timing.report(self.options.database, manifest.payload.len(), through);
        Ok(image.id())
    }

    pub async fn pin_checkpoint(
        &mut self,
        manifest: ObjectId,
        until: Option<Timestamp>,
    ) -> Result<Pin> {
        self.refresh().await?;
        let lease = self.authority()?;
        self.require_armed_rotation()?;
        let closure = self.reachability(lease).await?;
        if manifest.kind != ObjectKind::Manifest || !closure.contains(manifest) {
            return Err(Anomaly::incorrect(
                "rtdb/unreachable-manifest",
                "The manifest is outside committed reachability",
            ));
        }
        pin::require_ordinary_pin(&read_manifest(self.store.as_ref(), self.stream.kms.as_ref(), manifest).await?)?;
        if until.is_some_and(|deadline| !self.before(deadline.0)) {
            return Err(Anomaly::incorrect(
                "rtdb/pin-expired",
                "The pin admission deadline has elapsed",
            ));
        }
        let pin = Pin {
            id: Uuid::new_v4(),
            manifest,
            until: until.map(|time| time.0),
        };
        self.append_control(lease, Record::Pin(pin.clone())).await?;
        Ok(pin)
    }

    pub async fn open_pin(&mut self, id: Uuid) -> Result<VerifiedManifest> {
        let (_, image) = self.open_pinned_manifest(id, true).await?;
        pin::require_ordinary_pin(&image)?;
        Ok(image)
    }

    async fn open_pinned_manifest(&mut self, id: Uuid, require_writer: bool) -> Result<(Pin, VerifiedManifest)> {
        self.refresh().await?;
        self.require_active()?;
        let state = self.state();
        let epoch = if require_writer { self.authority()?.epoch } else { state.epoch };
        let observed = state.head;
        let closure = state.reachability(&self.stream, self.store.as_ref(), epoch).await?;
        let pin = closure
            .pins()
            .find(|pin| pin.id == id && pin.until.is_none_or(|until| self.before(until)))
            .ok_or_else(|| {
                Anomaly::new(
                    AnomalyCategory::NotFound,
                    "rtdb/unknown-pin",
                    "The pin is unavailable",
                )
            })?;
        let image = read_manifest(self.store.as_ref(), self.stream.kms.as_ref(), pin.manifest).await?;
        if !require_writer {
            self.refresh().await?;
            self.require_active()?;
            let state = self.state();
            if state.head != observed
                && !state.reachability(&self.stream, self.store.as_ref(), state.epoch)
                    .await?.pins().any(|current| current == pin)
            {
                return Err(Anomaly::new(AnomalyCategory::NotFound, "rtdb/unknown-pin", "The exact export pin was withdrawn"));
            }
            self.check_generation(image.generation.number).await?;
        }
        if pin.until.is_some_and(|until| !self.before(until)) {
            return Err(Anomaly::new(
                AnomalyCategory::NotFound,
                "rtdb/unknown-pin",
                "The pin is unavailable",
            ));
        }
        Ok((pin.clone(), image))
    }

    pub async fn release_pin(&mut self, id: Uuid) -> Result<()> {
        self.refresh().await?;
        let lease = self.authority()?;
        let closure = self.reachability(lease).await?;
        if let Some(pin) = closure.pins().find(|pin| pin.id == id) {
            pin::require_ordinary_pin(&read_manifest(self.store.as_ref(), self.stream.kms.as_ref(), pin.manifest).await?)?;
            self.require_authority(lease)?;
            self.append_control(lease, Record::Unpin { id }).await?;
        }
        Ok(())
    }

    pub async fn collect(&mut self) -> Result<usize> {
        self.refresh().await?;
        let lease = self.authority()?;
        self.require_destroyed_generation()?;
        let closure = self.reachability(lease).await?;
        let prefix =
            ObjectKey::new(format!("db/{}", self.options.database)).map_err(Anomaly::from)?;
        let listing = self
            .store
            .list(Some(&prefix))
            .await
            .map_err(Anomaly::from)?;
        let plan = closure.plan_gc(listing.iter().map(|object| &object.key));
        self.require_authority(lease)?;
        plan.execute(self.store.as_ref()).await?;
        Ok(plan.keys().len())
    }

    fn validate_files(&self, closure: &Reachability, files: &[ObjectId]) -> Result<()> {
        closure.validate_publication(files.iter().copied())?;
        let writer = self.writer.as_ref().ok_or_else(no_writer)?;
        if files.iter().any(|id| {
            id.kind != ObjectKind::File
                || (id.epoch == writer.lease.epoch.0 && !writer.uploaded.contains(id))
        }) {
            return Err(Anomaly::incorrect(
                "rtdb/unpublished-file",
                "The checkpoint references an unpublished file",
            ));
        }
        Ok(())
    }

    async fn reachability(&self, lease: Lease) -> Result<Reachability> {
        self.state()
            .reachability(&self.stream, self.store.as_ref(), lease.epoch)
            .await
    }

    async fn trim(&mut self, lease: Lease, through: Lsn) -> Result<()> {
        self.require_authority(lease)?;
        match self.stream.trim(lease.epoch, through).await {
            Ok(()) => self.refresh().await,
            Err(error) => Err(self.mutation_failed(error)),
        }
    }

    async fn append(&mut self, lease: Lease, record: Record) -> Result<Option<Receipt>> {
        let at = next(self.state().head)?;
        let entry = Entry {
            epoch: lease.epoch,
            generation: self.state().generation,
            record,
        };
        let bytes = self.stream.encode(at, &entry).await?;
        self.require_authority(lease)?;
        match self.stream.append(lease.epoch, at, bytes).await {
            Ok(lsn) if lsn == at => {}
            Ok(_) => {
                self.writer = None;
                self.committed = None;
                return Err(Anomaly::fault(
                    "rtdb/journal-position",
                    "The append acknowledged a different position",
                ));
            }
            Err(error) => return Err(self.mutation_failed(error)),
        }
        let mut state = self
            .committed
            .take()
            .expect("append requires a recovered prefix");
        let receipt = match state.apply(at, entry) {
            Ok(receipt) => receipt,
            Err(error) => {
                self.writer = None;
                return Err(error);
            }
        };
        self.install_committed(state);
        self.require_authority(lease)?;
        Ok(receipt)
    }

    pub(super) async fn shutdown_writer(&mut self) -> Result<()> {
        let result = async {
            let Ok(lease) = self.authority() else {
                return Ok(());
            };
            self.refuse_pending(lease).await?;
            self.finish_pending_erasure().await?;
            self.append_writer_release(lease).await
        }
        .await;
        self.candidate = None;
        self.withdraw_writer();
        result
    }

    async fn append_writer_release(&mut self, lease: Lease) -> Result<()> {
        self.append_control(lease, Record::WriterRelease { lease_until: lease.until }).await
    }

    async fn append_control(&mut self, lease: Lease, record: Record) -> Result<()> {
        self.append_control_in_generation(lease, self.state().generation, record)
            .await
    }

    async fn append_control_in_generation(
        &mut self,
        lease: Lease,
        generation: u64,
        record: Record,
    ) -> Result<()> {
        self.authority()?;
        let coordinates = self.state().coordinates;
        let at = coordinates.lsn(self
            .writer
            .as_mut()
            .ok_or_else(no_writer)?
            .pipeline
            .reserve()?)?;
        let write = Append {
            at,
            admit_before: None,
            entry: Entry {
                epoch: lease.epoch,
                generation,
                record,
            },
        };
        self.commit_one(lease, write).await?;
        Ok(())
    }

    fn mutation_failed(&mut self, error: JournalError) -> Anomaly {
        match error {
            JournalError::Fenced { current } => {
                self.note_competing_seal(current);
                self.withdraw_writer();
            }
            JournalError::Conflict { .. } | JournalError::Unavailable | JournalError::Deleted => {
                self.withdraw_writer();
            }
            _ => {}
        }
        journal_error(error)
    }

    async fn refresh(&mut self) -> Result<()> {
        if self
            .writer
            .as_ref()
            .is_some_and(|writer| !self.before(writer.lease.until))
        {
            self.withdraw_writer();
        }
        let end = self.stream.head().await?;
        self.catch_up(end).await?;
        if self.writer.as_ref().is_some_and(|writer| {
            self.state().retired.is_some() || self.state().epoch > writer.lease.epoch || self.state().released == Some(writer.lease.epoch)
                || !self.before(writer.lease.until)
        }) {
            self.withdraw_writer();
        }
        Ok(())
    }

    async fn catch_up(&mut self, end: Lsn) -> Result<()> {
        let previous = self.committed.take();
        let replayed = match Committed::read_through(&self.stream, self.store.as_ref(), end, previous).await {
            Ok(replayed) => replayed,
            // Only an acknowledged recovery lets a seal return a head below
            // what this process replayed. Report the lost range once, then
            // continue from the recovered journal.
            Err(shortened @ ReplayError::Shortened { .. }) => {
                let shortened = shortened.into_anomaly();
                eprintln!("Multi database {}: {}", self.options.database, shortened.message.as_deref().unwrap_or(""));
                Committed::read_through(&self.stream, self.store.as_ref(), end, None)
                    .await
                    .map_err(|error| self.keep_replayed(error))?
            }
            Err(error) => return Err(self.keep_replayed(error)),
        };
        self.install_committed(replayed);
        Ok(())
    }

    fn keep_replayed(&mut self, error: ReplayError) -> Anomaly {
        match error {
            ReplayError::Failed { error, resume } => {
                self.committed = resume.map(|resume| *resume);
                error
            }
            shortened => shortened.into_anomaly(),
        }
    }

    fn state(&self) -> &Committed {
        self.committed
            .as_ref()
            .expect("operation requires a recovered prefix")
    }

    fn durable_transactions(&self) -> &TransactState {
        let replay = &self.state().transactions;
        self.writer
            .as_ref()
            .map(|writer| writer.pipeline.durable())
            .filter(|durable| durable.index().head() == replay.index().head())
            .unwrap_or(replay)
    }

    fn authority(&self) -> Result<Lease> {
        self.require_active()?;
        self.writer
            .as_ref()
            .filter(|writer| !writer.recovering && self.before(writer.lease.until))
            .map(|writer| writer.lease)
            .ok_or_else(no_writer)
    }

    fn require_active(&self) -> Result<()> {
        if self.committed.as_ref().is_some_and(|state| state.retired.is_some()) {
            return Err(Anomaly::new(AnomalyCategory::NotFound, "rtdb/deployment-retired", "The deployment has permanently closed admission"));
        }
        Ok(())
    }

    fn require_authority(&mut self, lease: Lease) -> Result<()> {
        if !self.before(lease.until) {
            self.withdraw_writer();
            return Err(no_writer());
        }
        Ok(())
    }

    fn lease_end(&self) -> i64 {
        (self.clock)().0.saturating_add(self.options.lease_micros)
    }

    fn upper(&self) -> i64 {
        (self.clock)().0.saturating_add(self.options.drift_micros)
    }

    fn before(&self, deadline: i64) -> bool {
        self.upper() < deadline
    }

    fn observed_lapse(&self) -> bool {
        self.state()
            .lease
            .is_none_or(|(_, until)| self.observed_deadline(until))
    }

    fn observed_deadline(&self, until: i64) -> bool {
        (self.clock)().0 > until.saturating_add(self.options.drift_micros)
    }
}

fn envelope(record: TransactionRecord, coordinates: JournalCoordinates) -> Result<Record> {
    Ok(match record {
        TransactionRecord::Tx(_) => Record::Tx {
            payload: record.encode()?.to_vec(),
            objects: Vec::new(),
        },
        TransactionRecord::Settle { tx_lsn } => Record::Settle { tx_lsn: coordinates.lsn(tx_lsn)?.0 },
        TransactionRecord::Refuse { tx_lsn } => Record::Refuse { tx_lsn: coordinates.lsn(tx_lsn)?.0 },
    })
}

fn no_writer() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/no-writer",
        "This node has no active writer lease",
    )
}

fn follower_unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb/follower-unavailable", "An exclusive local Multi follower is required")
}

fn write_erases(write: &Append) -> Result<bool> {
    let Record::Tx { payload, .. } = &write.entry.record else {
        return Err(missing_receipt());
    };
    let TransactionRecord::Tx(tx) = TransactionRecord::decode(payload)? else {
        return Err(missing_receipt());
    };
    Ok(tx.effects().iter().any(|effect| matches!(effect, BranchEffect::Erase { .. })))
}

fn erasure_pending() -> Anomaly {
    Anomaly::new(AnomalyCategory::Busy, "rtdb/erasure-pending", "The erase receipt is waiting for durable key retirement")
}

fn missing_receipt() -> Anomaly {
    Anomaly::fault(
        "rtdb/missing-receipt",
        "The committed transaction did not produce a receipt",
    )
}

#[cfg(test)]
mod tests;
