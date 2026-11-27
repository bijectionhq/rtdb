use std::{collections::BTreeSet, net::IpAddr, sync::{Arc, Weak}, time::Duration};

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
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    journal::{Entry, JournalBatchAppender, Stream, journal_error, next},
    placement::preferred,
    replay::{ChangeHistory, Committed, JournalCoordinates, JournalRecovery, ReplayBase, ReplayError},
};

static RECOVERIES_ACKNOWLEDGED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Operator journal recoveries acknowledged with an atomic marker by this process's Multi writers.
pub fn journal_recoveries_acknowledged() -> u64 {
    RECOVERIES_ACKNOWLEDGED.load(std::sync::atomic::Ordering::Relaxed)
}

mod backup;
/// How a writer's acknowledgement of an operator recovery ended.
enum Acknowledgement {
    /// This node's marker lifted the fence; the journal head after it.
    Recorded(Lsn),
    /// Another node or an administrator lifted the fence first.
    Elsewhere,
    /// The journal has no recovery markers.
    Unsupported,
}

mod classic_migration;
mod deferred;
mod pin;
mod pipeline;
mod ranked;
mod rotation;
mod retirement;
mod store_floor;
mod store_hold;
mod timing;
pub use backup::{BackupFileRelocation, BackupObjectReadback, InstalledBackup, PortableBackup, VerifiedBackup};
pub use classic_migration::{ClassicInstallation, classic_import_basis};
pub use pin::{NativeCheckpointInstallation, NativeDependentCarrier, NativeDependentOriginal, PinnedExport};
pub use pipeline::WritePipeline;
pub use ranked::{RankedBuild, RankedFiles, RankedPublication};
use pipeline::{Append, AttemptError, Planned, receipt_for};
use rotation::ArmedRotation;
pub use rotation::RotationReceipt;
pub use retirement::DeploymentRetirement;
pub use store_hold::{StoreVersionHold, StoreVersionHoldKey, StoreVersionHoldReply, StoreVersionHoldRequest};

/// Wakes a waiter when its residence applies a new journal position, or when the journal holds an entry the residence has not applied, such as one another writer appended.
/// It never holds the residence, so the waiter reads afresh after each wake.
pub struct AppliedWatch {
    applied: tokio::sync::watch::Receiver<u64>,
    stream: Stream,
}

impl AppliedWatch {
    /// Marks the residence's current applied position seen.
    pub fn mark_seen(&mut self) {
        let _ = self.applied.borrow_and_update();
    }

    /// Returns at `until`, or once the residence applies a position not marked seen, or once the journal holds an entry past the residence's applied position.
    pub async fn changed(&mut self, until: tokio::time::Instant) -> Result<()> {
        let applied = Lsn(*self.applied.borrow());
        tokio::select! {
            changed = self.applied.changed() => changed.map_err(|_| Anomaly::new(
                AnomalyCategory::Unavailable, "rtdb/residence-closed", "The database residence closed",
            )),
            held = self.stream.wait_after(applied, until) => held,
            _ = tokio::time::sleep_until(until) => Ok(()),
        }
    }

    /// Waits until this residence applies a local journal position not yet marked seen,
    /// without starting a journal long poll.
    pub async fn local_changed(&mut self) -> Result<()> {
        self.applied.changed().await.map_err(|_| Anomaly::new(
            AnomalyCategory::Unavailable, "rtdb/residence-closed", "The database residence closed",
        ))
    }

    /// Waits until this residence's applied position reaches at least `target`.
    pub async fn wait_local_at_least(&mut self, target: u64) -> Result<()> {
        self.applied
            .wait_for(|&applied| applied >= target)
            .await
            .map(|_| ())
            .map_err(|_| Anomaly::new(
                AnomalyCategory::Unavailable, "rtdb/residence-closed", "The database residence closed",
            ))
    }
}

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
    reclaimed_store_history_through: Option<Lsn>,
    retired_store_checkpoints: BTreeSet<ObjectId>,
    store_holds: store_hold::StoreHolds,
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
    deferred_checkpoints: bool,
    binary_checkpoints: bool,
    hold_writes_after_recovery: bool,
    /// The next catch-up replays from the journal floor instead of resuming.
    replay_afresh: bool,
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
            deferred_checkpoints: false,
            binary_checkpoints: false,
            hold_writes_after_recovery: false,
            replay_afresh: false,
        })
    }

    pub async fn create(&self) -> Result<()> {
        self.stream.create().await?;
        self.token_keys.install(self.stream.kms.as_ref(), 1).await?;
        self.stream.commission().await?;
        Ok(())
    }

    pub fn with_activation_scope(mut self, scope: crate::ActivationScope) -> Result<Self> {
        if !self.stream.immutable_history() {
            return Err(crate::commissioning::scope_mismatch());
        }
        self.stream.activation_scope = Some(scope);
        Ok(self)
    }

    pub async fn check_activation_scope(&self, head: Lsn) -> Result<()> {
        self.stream.check_commissioning(head).await
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

    /// Takes journal-bound checkpoints off the commit path: the writer
    /// captures the state under its lock, builds and uploads the manifest
    /// without it, and announces a checkpoint whose cut precedes the
    /// announcement. Enable only once every reader of the stream replays a
    /// deferred base.
    pub fn with_deferred_checkpoints(mut self, enabled: bool) -> Self {
        self.deferred_checkpoints = enabled;
        self
    }

    pub fn deferred_checkpoints(&self) -> bool { self.deferred_checkpoints }

    /// Refuses the writer role after an operator recovery of the journal
    /// until someone else acknowledges it. By default the writer
    /// acknowledges the recovery itself, records it in the journal and
    /// resumes.
    pub fn with_hold_writes_after_recovery(mut self, hold: bool) -> Self {
        self.hold_writes_after_recovery = hold;
        self
    }

    /// The latest operator recovery recorded in the retained journal.
    pub fn journal_recovery(&self) -> Option<JournalRecovery> {
        self.committed.as_ref().and_then(|state| state.recovery)
    }

    /// Writes checkpoint transaction state in CBOR instead of JSON. Enable
    /// only once every reader of the stream installs binary checkpoints.
    pub fn with_binary_checkpoints(mut self, enabled: bool) -> Self {
        self.binary_checkpoints = enabled;
        self
    }

    pub async fn finish_token_rotation(&mut self, generation: u64, unreadable_by: i64) -> Result<()> {
        self.refresh().await?;
        if self.state().generation < generation { return Err(no_writer()); }
        self.token_keys.rotate(self.stream.kms.as_ref(), generation, unreadable_by).await
    }

    pub async fn destroy_retired_token_key(&self, generation: u64) -> Result<()> {
        self.token_keys.destroy_retired(self.stream.kms.as_ref(), generation).await
    }

    pub fn with_immutable_journal(mut self) -> Self {
        self.stream = self.stream.with_immutable_history();
        self
    }

    pub fn with_serving_authority(mut self, authority: Arc<dyn crate::ServingAuthority>) -> Self {
        self.stream.serving_authority = Some(authority);
        self
    }

    pub fn serving_authority(&self) -> Option<Arc<dyn crate::ServingAuthority>> {
        self.stream.serving_authority.clone()
    }

    pub fn activation_required(&self) -> bool {
        self.stream.activation_scope.is_some() || self.stream.serving_authority.is_some()
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
        Box::pin(async move {
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
        }).await
    }

    async fn promote(&mut self) -> Result<()> {
        Box::pin(async move {
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
        let (attempt, end) = match self.seal_attempt(attempt).await? {
            Some(sealed) => sealed,
            None => return Ok(()),
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
                reclaimed_store_history_through: None,
                retired_store_checkpoints: BTreeSet::new(),
                store_holds: store_hold::StoreHolds::default(),
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
        }).await
    }

    /// Seals the attempt's epoch. After an operator recovery the writer
    /// acknowledges the recovery itself, unless it holds writes, and seals
    /// above the recovery's fence. Returns `None` when a competing writer
    /// holds the stream.
    async fn seal_attempt(&mut self, mut attempt: Lease) -> Result<Option<(Lease, Lsn)>> {
        let mut acknowledged = None;
        loop {
            match self.stream.seal(attempt.epoch).await {
                Ok(end) => return Ok(Some((attempt, end))),
                Err(JournalError::Recovered { head }) if !self.stream.immutable_history() && !self.hold_writes_after_recovery && acknowledged.is_none() => {
                    tracing::error!(
                        database = %self.options.database, code = "rtdb/journal-recovered", head = head.0,
                        epoch = attempt.epoch.0,
                        "An operator recovered the Multi journal after it lost two of its three voters. Commits acknowledged after the recovered head may be lost",
                    );
                    // Only the node that lifted the fence seals above it
                    // at once; a node that lost the race competes as usual.
                    acknowledged = match self.acknowledge_recovery_with_record(head).await? {
                        Acknowledgement::Recorded(after) => Some(after),
                        Acknowledgement::Elsewhere => Some(Lsn(u64::MAX)),
                        Acknowledgement::Unsupported => {
                            self.stream.acknowledge_recovery().await.map_err(journal_error)?;
                            // A journal that records recoveries wrote its
                            // entry after the recovered head.
                            Some(self.stream.head().await?)
                        }
                    };
                    self.replay_afresh = true;
                }
                Err(JournalError::Fenced { current }) => {
                    // The recovery's fence stands above the epoch this node
                    // chose before it saw the recovery. Seal once above it,
                    // unless a writer already appended after the recovery
                    // (and after its marker).
                    if let Some(head) = acknowledged.take()
                        && self.stream.head().await? == head
                    {
                        attempt = Lease { epoch: Epoch(next(Lsn(current.0))?.0), until: attempt.until };
                        self.candidate = Some(attempt);
                        continue;
                    }
                    self.candidate = None;
                    self.note_competing_seal(current);
                    return Ok(None);
                }
                Err(error) => return Err(journal_error(error)),
            }
        }
    }

    /// Acknowledges the operator recovery that left the journal at `head`
    /// and records it in the same journal decision. The journal writes its
    /// typed recovery entry right after the recovered head, under the
    /// recovery's epoch, with this writer's `Recovered` record inside it as
    /// the consumer's marker, and lifts exactly that fence. That entry is
    /// the only record of a recovery: the writer never appends one of its
    /// own, and replay reads the entry (`Stream::decode_recovery`). An older
    /// journal appends the marker as an ordinary record instead. So
    /// whichever node acknowledges also records, and a racing node's marker
    /// is refused rather than written twice. Returns the journal head after
    /// the acknowledgement, `Elsewhere` when another node or an
    /// administrator lifted the fence (an administrator's acknowledgement
    /// still leaves the journal's entry, without a marker), or `Unsupported`
    /// when the journal has no recovery markers: the caller then only
    /// acknowledges, and the error event, the counters and the followers'
    /// rebuilds are what remain of the recovery.
    async fn acknowledge_recovery_with_record(&mut self, head: Lsn) -> Result<Acknowledgement> {
        let fence = match self.stream.recovery_fence().await {
            Ok(Some(fence)) => fence,
            Ok(None) => {
                tracing::debug!(database = %self.options.database, head = head.0, "The journal recovery fence is already clear");
                return Ok(Acknowledgement::Elsewhere);
            }
            Err(JournalError::Unsupported) => return Ok(Acknowledgement::Unsupported),
            Err(error) => return Err(journal_error(error)),
        };
        if fence.head != head {
            return Err(journal_error(JournalError::Recovered { head: fence.head }));
        }
        // Rebuild through the recovered head first, so the record is sealed
        // under the key generation the recovered history ends with.
        self.replay_afresh = true;
        self.catch_up(fence.head).await?;
        let at = next(fence.head)?;
        let entry = Entry {
            epoch: fence.epoch,
            generation: self.state().generation,
            record: Record::Recovered { head: fence.head.0, epoch: fence.epoch.0, time: (self.clock)().0 },
        };
        let marker = self.stream.encode(at, &entry).await?;
        match self.stream.acknowledge_recovery_with_marker(fence, marker).await {
            Ok(at) => {
                RECOVERIES_ACKNOWLEDGED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(Acknowledgement::Recorded(at))
            }
            // Another node lifted this fence first, with its own marker or
            // an administrator's acknowledgement; that stands.
            Err(JournalError::Conflict { head: current }) => {
                tracing::debug!(database = %self.options.database, head = head.0, epoch = fence.epoch.0,
                    current_head = current.0, "The journal recovery fence changed before the marker was accepted");
                Ok(Acknowledgement::Elsewhere)
            }
            Err(JournalError::Unsupported) => Ok(Acknowledgement::Unsupported),
            Err(error) => Err(journal_error(error)),
        }
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
        let result = Box::pin(self.transact_serial(request, scope)).await;
        if result.is_err() {
            Box::pin(self.recover_after_failure()).await;
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
        let index = self.durable_transactions().index();
        let after_floor = state.coordinates.position(Lsn(state.floor.0 - 1))?
            .max(state.coordinates.origin.map_or(0, |origin| origin.through))
            .max(index.store_history_floor());
        Ok(ChangeHistory {
            index,
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

    /// Watches the journal position this residence has applied, and the journal past it.
    pub fn watch_applied(&self) -> AppliedWatch {
        AppliedWatch { applied: self.applied.subscribe(), stream: self.stream.clone() }
    }

    /// Duration until the next lease renewal, predecessor or competing-seal lapse,
    /// or armed key-rotation destruction deadline, relative to this residence's clock.
    pub fn next_deadline(&self, live_nodes: &BTreeSet<IpAddr>) -> Option<Duration> {
        if self.assigned_follower.strong_count() != 0
            || self.committed.as_ref().is_some_and(|state| state.retired.is_some())
        {
            return None;
        }
        let now = (self.clock)().0;
        let mut earliest: Option<i64> = None;
        let mut consider = |at: i64| {
            earliest = Some(earliest.map_or(at, |previous| previous.min(at)));
        };
        if let Some(writer) = &self.writer {
            let renew_at = writer
                .lease
                .until
                .saturating_sub(self.options.drift_micros)
                .saturating_sub(self.options.lease_micros / 2);
            consider(renew_at);
            if let Some(destroy_at) = writer.rotation.and_then(|armed| armed.pending_destruction_at()) {
                consider(destroy_at);
            }
        } else if preferred(self.options.database, live_nodes) == Some(self.options.node)
            && let Some(state) = &self.committed
        {
            if let Some((_, until)) = state.lease
                && !self.observed_deadline(until)
            {
                consider(until.saturating_add(self.options.drift_micros).saturating_add(1));
            } else {
                let seen = state.epoch.max(self.last_allocated);
                if let Some(lease) = self.competing_seal.filter(|lease| lease.epoch > seen)
                    && !self.observed_deadline(lease.until)
                {
                    consider(lease.until.saturating_add(self.options.drift_micros).saturating_add(1));
                } else {
                    consider(now);
                }
            }
        }
        earliest.map(|at| Duration::from_micros(at.saturating_sub(now).max(0) as u64))
    }

    /// The journal position at which `checkpoint_retained_suffix(limit)` becomes due,
    /// or `None` when this node is not an active writer.
    pub fn next_checkpoint_lsn(&self, limit: u64) -> Option<u64> {
        if self.writer.is_none() || self.authority().is_err() {
            return None;
        }
        let state = self.committed.as_ref()?;
        Some(state.floor.0.saturating_add(limit).saturating_sub(1))
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
        if self.deferred_checkpoints {
            let Some(capture) = self.capture_checkpoint(limit).await? else { return Ok(None) };
            let prepared = self.checkpoint_builder().build(capture).await?;
            return self.announce_checkpoint(prepared).await.map(deferred::Announcement::manifest);
        }
        self.checkpoint(Vec::new()).await.map(Some)
    }

    pub async fn checkpoint(&mut self, files: Vec<ObjectId>) -> Result<ObjectId> {
        Box::pin(async move {
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
        let mut files = files;
        let portable_files = backup::portable_attachment_files(&self.stream, self.store.as_ref(), self.state().coordinates.origin).await?;
        for id in portable_files {
            if !files.contains(&id) {
                files.push(id);
            }
        }
        self.validate_files(&closure, &files)?;
        let store_history_floor = self.durable_transactions().index().store_history_floor();
        let manifest = Manifest {
            files,
            pins: closure.pins().cloned().collect(),
            payload: Committed::checkpoint_payload_in(
                self.durable_transactions(),
                lease.until,
                false,
                self.state().coordinates,
                self.state().store_purge_floor,
                self.binary_checkpoints,
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
        self.stream.manifests.insert(image.manifest_summary(&manifest, Generation { database: self.options.database, number: generation })?);
        timing.mark("put");
        let checkpoint = Checkpoint {
            manifest: image.id(),
            through: through.0,
            key_generation: generation,
            restore: false,
            recovery_epoch: self.state().checkpoint_recovery_epoch(through),
        };
        self.install_checkpoint(lease, checkpoint, lease.until, store_history_floor).await?;
        timing.mark("install");
        timing.report(self.options.database, manifest.payload.len(), through);
        Ok(image.id())
        }).await
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
        let state = self.state();
        let history_floor = state.transactions.index().store_history_floor();
        if history_floor != 0 && (state.checkpointed_store_history_floor < history_floor
            || state.base.as_ref().is_none_or(|base| base.checkpoint.manifest != manifest))
        {
            return Err(Anomaly::new(AnomalyCategory::NotFound, "rtdb/cursor-expired",
                "The manifest has no retained store history coverage"));
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
        Box::pin(async move {
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
        }).await
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
        Box::pin(self.state().reachability(&self.stream, self.store.as_ref(), lease.epoch)).await
    }

    async fn install_checkpoint(
        &mut self,
        lease: Lease,
        checkpoint: Checkpoint,
        until: i64,
        store_history_floor: u64,
    ) -> Result<()> {
        Box::pin(async move {
            let covered_through_lsn = Lsn(checkpoint.through);
            let write = self.reserve_control(
                lease, checkpoint.key_generation, Record::Checkpoint(checkpoint),
            )?;
            let announced = write.at;
            let installed = self.commit_checkpoint(lease, write, covered_through_lsn).await?;
            self.finish_checkpoint(lease, announced, until, store_history_floor, installed).await
        }).await
    }

    /// Keeps the applied state over the selected floor when it still covers
    /// the same checkpoint. Restores and changed histories replay instead.
    async fn finish_checkpoint(
        &mut self, lease: Lease, announced: Lsn, until: i64,
        store_history_floor: u64, installed: bool,
    ) -> Result<()> {
        let base = self.state().checkpoint_at(announced)?;
        let retired = if store_history_floor != 0 && self.state().store_purge_floor != 0 {
            self.state().checkpoint_manifests_through(Lsn(base.checkpoint.through))
        } else {
            BTreeSet::new()
        };
        if !installed {
            self.trim_checkpoint(lease, &base).await?;
        }
        if let Some(writer) = self.writer.as_mut() {
            writer.retired_store_checkpoints.extend(retired);
        }
        if let Some(state) = self.committed.as_mut()
            && (installed || base.checkpoint.through >= state.floor.0)
        {
            state.adopt_checkpoint(base, until, lease.epoch, store_history_floor);
        }
        self.refresh().await
    }

    async fn trim_committed_checkpoint(
        &mut self,
        lease: Lease,
        announced: Lsn,
        through: Lsn,
    ) -> Result<()> {
        let base = self.state().checkpoint_at(announced)?;
        if base.checkpoint.through != through.0 {
            return Err(Anomaly::fault(
                "rtdb/checkpoint-trim-proof",
                "The checkpoint announces a different trim position",
            ));
        }
        self.trim_checkpoint(lease, &base).await?;
        self.refresh().await
    }

    async fn trim_checkpoint(&mut self, lease: Lease, base: &ReplayBase) -> Result<()> {
        self.require_authority(lease)?;
        let checkpoint = self.stream.certify_checkpoint(base, self.state().head).await?;
        self.require_authority(lease)?;
        self.stream
            .trim(lease.epoch, checkpoint)
            .await
            .map_err(|error| self.mutation_failed(error))
    }

    async fn append(&mut self, lease: Lease, record: Record) -> Result<Option<Receipt>> {
        let at = next(self.state().head)?;
        let entry = Entry {
            epoch: lease.epoch,
            generation: self.state().generation,
            record,
        };
        let bytes = self.stream.encode(at, &entry).await?;
        let tail = Sha256::digest(&bytes).into();
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
        state.read_back(tail);
        self.install_committed(state);
        self.require_authority(lease)?;
        Ok(receipt)
    }

    /// Attempts an ordered lease release and withdraws this process's writer ownership.
    /// Call before closing the database's storage.
    pub async fn shutdown_writer(&mut self) -> Result<()> {
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
        Box::pin(async move {
        let write = self.reserve_control(lease, generation, record)?;
        self.commit_one(lease, write).await?;
        Ok(())
        }).await
    }

    fn reserve_control(&mut self, lease: Lease, generation: u64, record: Record) -> Result<Append> {
        self.authority()?;
        let coordinates = self.state().coordinates;
        let at = coordinates.lsn(self
            .writer
            .as_mut()
            .ok_or_else(no_writer)?
            .pipeline
            .reserve()?)?;
        Ok(Append {
            at,
            admit_before: None,
            entry: Entry {
                epoch: lease.epoch,
                generation,
                record,
            },
        })
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
        Box::pin(async move {
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
        }).await
    }

    async fn catch_up(&mut self, end: Lsn) -> Result<()> {
        Box::pin(async move {
        let previous = self.committed.take().filter(|_| !std::mem::take(&mut self.replay_afresh));
        let replayed = match Committed::read_through(&self.stream, self.store.as_ref(), end, previous).await {
            Ok(replayed) => replayed,
            // Only an operator recovery shortens the journal below what this
            // process replayed or replaces records it applied. Report the
            // loss once, then rebuild from the recovered journal.
            Err(lost @ (ReplayError::Shortened { .. } | ReplayError::Rewritten { .. })) if !self.stream.immutable_history() => {
                let lost = lost.into_anomaly();
                tracing::error!(database = %self.options.database, code = %lost.code,
                    "{}; replaying the recovered journal from its floor", lost.message.as_deref().unwrap_or(""));
                Committed::read_through(&self.stream, self.store.as_ref(), end, None)
                    .await
                    .map_err(|error| self.keep_replayed(error))?
            }
            Err(error) => return Err(self.keep_replayed(error)),
        };
        self.install_committed(replayed);
        Ok(())
        }).await
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

    /// The writer clock lease (T7): a writer samples only while its clock plus
    /// the drift bound is before its durable lease end, so every sample it
    /// issues lies below `until`.
    fn before(&self, deadline: i64) -> bool {
        self.upper() < deadline
    }

    /// A successor writer (or one restored from the lineage's lease) starts
    /// only once its clock passes the predecessor's lease end plus the drift
    /// bound. It waits once, at takeover, on the lease alone: never on the
    /// last stamp, which would make the wait depend on hidden commits, and
    /// never per append. A sample at or below the head's time is still
    /// smoothed one tick past it, as XTDB smooths system time.
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
    ).with_kind(rtdb_types::AnomalyKind::NoWriter)
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
