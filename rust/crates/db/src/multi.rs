use std::{collections::BTreeSet, net::IpAddr, sync::Arc};

use journal_contract::{Epoch, Journal, JournalError, Lsn};
use rtdb_crypt::{Generation, Kms};
use rtdb_format::journal::{Checkpoint, Manifest, ObjectId, ObjectKind, Pin, Record};
use rtdb_index::branches::BranchIndex;
use rtdb_store::{
    ObjectKey, ObjectStore, Reachability, VerifiedManifest, WriterObjects, read_manifest,
};
use rtdb_tx::transact::{
    Op, Receipt, Transact, TransactPipeline, TransactState, TransactionRecord,
};
use rtdb_types::{
    Anomaly, AnomalyCategory, Result, Timestamp,
    rtdb_v1::{Basis, Claim, ClaimScope, ClaimSealer, Layer},
};
use uuid::Uuid;

use crate::{
    journal::{Entry, Stream, journal_error, next},
    placement::preferred,
    replay::{ChangeHistory, Committed},
};

mod pin;
mod pipeline;
mod rotation;
pub use pin::PinnedExport;
pub use pipeline::WritePipeline;
use pipeline::{Append, Planned, interrupted, receipt_for};
use rotation::ArmedRotation;
pub use rotation::{RotationReceipt, RotationTiming};

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
    claims: Arc<ClaimSealer>,
    rotation_timing: Option<Arc<dyn RotationTiming>>,
    committed: Option<Committed>,
    writer: Option<Writer>,
    candidate: Option<Lease>,
    last_allocated: Epoch,
    competing_seal: Option<Lease>,
}

impl MultiDatabase {
    pub fn new(
        options: MultiOptions,
        journal: Arc<dyn Journal>,
        store: Arc<dyn ObjectStore>,
        kms: Arc<dyn Kms>,
        clock: Arc<dyn Fn() -> Timestamp + Send + Sync>,
        claims: Arc<ClaimSealer>,
    ) -> Result<Self> {
        if options.drift_micros < 0
            || options.lease_micros <= options.drift_micros.saturating_mul(2)
        {
            return Err(Anomaly::incorrect(
                "rtdb/invalid-writer-lease",
                "Writer lease must exceed twice the clock drift",
            ));
        }
        Ok(Self {
            stream: Stream::new(options.database, journal, kms),
            options,
            store,
            clock,
            claims,
            rotation_timing: None,
            committed: None,
            writer: None,
            candidate: None,
            last_allocated: Epoch(0),
            competing_seal: None,
        })
    }

    pub async fn create(&self) -> Result<()> {
        self.stream.create().await
    }

    pub fn incarnation(&self) -> [u8; 16] {
        *self.options.database.as_bytes()
    }

    pub fn role(&self) -> Role {
        self.writer
            .as_ref()
            .filter(|writer| self.before(writer.lease.until))
            .map_or(Role::Follower, |writer| Role::Writer(writer.lease.epoch))
    }

    pub async fn tick(&mut self, live_nodes: &BTreeSet<IpAddr>) -> Result<Role> {
        let selected = preferred(self.options.database, live_nodes) == Some(self.options.node);
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
        if let Some(writer) = &self.writer {
            let lease = writer.lease;
            self.refuse_pending(lease).await?;
            self.finish_key_rotation().await?;
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
                rotation: None,
                objects: WriterObjects::new(self.options.database, attempt.epoch.0),
                uploaded: BTreeSet::new(),
            });
            if let Err(error) = self.finish_key_rotation().await {
                self.withdraw_writer();
                return Err(error);
            }
            self.append_control(
                attempt,
                Record::Lease {
                    until: attempt.until,
                },
            )
            .await?;
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
            self.append(lease, envelope(terminal)?).await?;
        }
        Ok(())
    }

    pub async fn transact(&mut self, request: &Transact) -> Result<Receipt> {
        self.refresh().await?;
        reject_erasure(request)?;
        let write = match self.decide_one(request)? {
            Planned::Answer(receipt) => return Ok(receipt),
            Planned::Append(write) => write,
        };
        let authority = self.append_authority(write.entry.epoch)?;
        let completed = authority.append(&write).await;
        let receipts = self
            .publish_staged(authority.lease, write, completed)
            .await?;
        if request.window.before.is_none() || !receipts.is_empty() {
            return receipt_for(receipts, &request.key);
        }
        let terminal = match self.decide_terminal(authority.lease) {
            Ok(terminal) => terminal,
            Err(error) => {
                self.withdraw_writer();
                return Err(interrupted(error));
            }
        };
        let completed = authority.append(&terminal).await;
        let receipts = self
            .publish_staged(authority.lease, terminal, completed)
            .await?;
        receipt_for(receipts, &request.key)
    }

    pub async fn tx_status(&mut self, key: &[u8]) -> Result<Receipt> {
        self.refresh().await?;
        if let Some(writer) = &self.writer {
            return writer.pipeline.tx_status(key);
        }
        self.state().transactions.tx_status(key)
    }

    pub async fn fresh_index(&mut self) -> Result<&BranchIndex> {
        self.refresh().await?;
        Ok(self.durable_transactions().index())
    }

    pub async fn fresh_view(&mut self) -> Result<(BranchIndex, u64)> {
        self.refresh().await?;
        let state = self.state();
        Ok((
            self.durable_transactions().index().clone(),
            state.generation,
        ))
    }

    pub fn seal_claim(&self, branch: String, basis: Basis, layer: Layer) -> Result<Claim> {
        self.claims.seal(&ClaimScope {
            branch,
            basis,
            layer,
        })
    }

    pub async fn fresh_history(&mut self) -> Result<ChangeHistory<'_>> {
        self.refresh().await?;
        let state = self.state();
        Ok(ChangeHistory {
            index: self.durable_transactions().index(),
            floor: state.floor,
            head: state.head,
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

    pub async fn checkpoint(&mut self, files: Vec<ObjectId>) -> Result<ObjectId> {
        self.refresh().await?;
        let lease = self.authority()?;
        self.require_destroyed_generation()?;
        let closure = self.reachability(lease).await?;
        self.validate_files(&closure, &files)?;
        let manifest = Manifest {
            files,
            pins: closure.pins().cloned().collect(),
            payload: Committed::checkpoint_payload(
                self.durable_transactions(),
                lease.until,
                false,
            )?,
        };
        let through = self.state().head;
        let generation = self.state().generation;
        let writer = self.writer.as_mut().ok_or_else(no_writer)?;
        let image = writer
            .objects
            .manifest(self.stream.kms.as_ref(), generation, &manifest)
            .await?;
        image.put(self.store.as_ref()).await?;
        let checkpoint = Checkpoint {
            manifest: image.id(),
            through: through.0,
            key_generation: generation,
        };
        self.append_control(lease, Record::Checkpoint(checkpoint))
            .await?;
        self.trim(lease, through).await?;
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
        self.refresh().await?;
        let closure = self.reachability(self.authority()?).await?;
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
        let image =
            read_manifest(self.store.as_ref(), self.stream.kms.as_ref(), pin.manifest).await?;
        if pin.until.is_some_and(|until| !self.before(until)) {
            return Err(Anomaly::new(
                AnomalyCategory::NotFound,
                "rtdb/unknown-pin",
                "The pin is unavailable",
            ));
        }
        Ok(image)
    }

    pub async fn release_pin(&mut self, id: Uuid) -> Result<()> {
        self.refresh().await?;
        let lease = self.authority()?;
        if self
            .reachability(lease)
            .await?
            .pins()
            .any(|pin| pin.id == id)
        {
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
        self.committed = Some(state);
        self.require_authority(lease)?;
        Ok(receipt)
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
        let authority = self.append_authority(lease.epoch)?;
        let at = Lsn(self
            .writer
            .as_mut()
            .ok_or_else(no_writer)?
            .pipeline
            .reserve());
        let write = Append {
            at,
            entry: Entry {
                epoch: lease.epoch,
                generation,
                record,
            },
        };
        let completed = authority.append(&write).await;
        self.publish_staged(lease, write, completed).await?;
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
        if self.role() == Role::Follower {
            self.withdraw_writer();
        }
        let end = self.stream.head().await?;
        self.catch_up(end).await?;
        if self.writer.as_ref().is_some_and(|writer| {
            self.state().epoch > writer.lease.epoch || !self.before(writer.lease.until)
        }) {
            self.withdraw_writer();
        }
        Ok(())
    }

    async fn catch_up(&mut self, end: Lsn) -> Result<()> {
        let previous = self.committed.take();
        self.committed =
            Some(Committed::read_through(&self.stream, self.store.as_ref(), end, previous).await?);
        Ok(())
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
        self.writer
            .as_ref()
            .filter(|writer| self.before(writer.lease.until))
            .map(|writer| writer.lease)
            .ok_or_else(no_writer)
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

fn envelope(record: TransactionRecord) -> Result<Record> {
    Ok(match record {
        TransactionRecord::Tx(_) => Record::Tx {
            payload: record.encode()?.to_vec(),
            objects: Vec::new(),
        },
        TransactionRecord::Settle { tx_lsn } => Record::Settle { tx_lsn },
        TransactionRecord::Refuse { tx_lsn } => Record::Refuse { tx_lsn },
    })
}

fn no_writer() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/no-writer",
        "This node has no active writer lease",
    )
}

fn reject_erasure(request: &Transact) -> Result<()> {
    if request
        .ops
        .iter()
        .chain(&request.otherwise)
        .any(|op| matches!(op, Op::Erase { .. }))
    {
        return Err(Anomaly::unsupported(
            "rtdb/erasure-required",
            "Erasure requires a qualified key rotation path",
        ));
    }
    Ok(())
}

fn missing_receipt() -> Anomaly {
    Anomaly::fault(
        "rtdb/missing-receipt",
        "The committed transaction did not produce a receipt",
    )
}

#[cfg(test)]
mod tests;
