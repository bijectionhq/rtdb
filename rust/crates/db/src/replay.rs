use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

use journal_contract::{Epoch, JournalError, Lsn};
use rtdb_format::journal::{Checkpoint, ObjectId, ObjectKind, Record};
use rtdb_index::branches::{BranchCut, BranchEffect, BranchIndex};
use rtdb_store::{ObjectStore, Reachability, VerifiedManifest, read_manifest};
use rtdb_tx::transact::{Receipt, TransactState, TransactionRecord};
use rtdb_types::{Anomaly, Result, Timestamp, rtdb_v1::Basis};
use uuid::Uuid;

use crate::journal::{Entry, Stream, journal_error, journal_shortened, next};

const ROTATING_CHECKPOINT: u8 = 1;
const RESTORED_HISTORY: u8 = 2;
const CHECKPOINT_SOURCE: u8 = 4;
/// The transaction state is CBOR rather than JSON. Readers accept both.
const BINARY_STATE: u8 = 8;

pub struct AppliedEffects {
    pub cut: BranchCut,
    pub previous: BranchCut,
    pub effects: Vec<BranchEffect>,
}

pub struct ChangeHistory<'a> {
    pub index: &'a BranchIndex,
    pub after_floor: u64,
    pub through: u64,
    pub generation: u64,
    pub groups: &'a [AppliedEffects],
    pub now: Timestamp,
}

/// A restored history occupies the positions before the new stream's suffix.
#[derive(Clone, Copy, Default)]
pub(crate) struct JournalCoordinates {
    pub(crate) offset: u64,
    pub(crate) origin: Option<RestoreOrigin>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct RestoreOrigin {
    pub(crate) digest: [u8; 32],
    pub(crate) through: u64,
    pub(crate) source: Option<CheckpointSource>,
}

/// The original ordered pin and cut, authenticated by the source manifest and retained by the destination.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointSource {
    pub cut: BranchCut,
    pub pin: Uuid,
}

impl JournalCoordinates {
    pub(crate) fn position(self, lsn: Lsn) -> Result<u64> {
        self.offset.checked_add(lsn.0).ok_or_else(|| invalid("The journal position overflows the basis"))
    }

    pub(crate) fn lsn(self, position: u64) -> Result<Lsn> {
        position.checked_sub(self.offset).map(Lsn)
            .ok_or_else(|| invalid("The basis precedes the journal suffix"))
    }
}

#[derive(Clone, Copy)]
pub(crate) struct Rotation {
    pub(crate) cut: Lsn,
    pub(crate) generation: u64,
    pub(crate) committed: bool,
    pub(crate) retirement: Option<RetirementObservation>,
    pub(crate) retirement_armed: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct RetirementObservation {
    pub(crate) observed_at: i64,
    pub(crate) unreadable_by: Option<i64>,
}

/// Why a replay stopped short of the position it was asked to reach.
pub(crate) enum ReplayError {
    /// The journal ends at `head`, before the `replayed` position this process
    /// already applied: a recovery lost the records between them.
    Shortened { head: Lsn, replayed: Lsn },
    /// `resume` is the longest prefix replay had fully applied, which a later
    /// replay continues from instead of starting over.
    Failed { error: Anomaly, resume: Option<Box<Committed>> },
}

impl ReplayError {
    fn failed(error: Anomaly, resume: Option<Committed>) -> Self {
        // A state with no records was never seeded from the journal.
        let resume = resume.filter(|state| !state.records.is_empty()).map(Box::new);
        Self::Failed { error, resume }
    }

    pub(crate) fn into_anomaly(self) -> Anomaly {
        match self {
            Self::Shortened { head, replayed } => journal_shortened(head, replayed),
            Self::Failed { error, .. } => error,
        }
    }
}

/// Verified manifest summaries without their checkpoint payloads, by object.
/// Manifests are immutable and named by their ciphertext digest, so a summary
/// never goes stale. Reachability reads them instead of the object store.
///
/// The cache holds the manifests the last reachability closure reached, plus
/// those inserted since it started: each closure drops the rest, so the cache
/// is bounded by the retained journal suffix rather than by a count.
pub(crate) type ManifestCache = SummaryCache<VerifiedManifest>;

pub(crate) struct SummaryCache<T>(Mutex<Summaries<T>>);

struct Summaries<T> {
    entries: BTreeMap<ObjectId, (u64, Arc<T>)>,
    inserted: u64,
}

impl<T> Default for SummaryCache<T> {
    fn default() -> Self {
        Self(Mutex::new(Summaries { entries: BTreeMap::new(), inserted: 0 }))
    }
}

impl<T> SummaryCache<T> {
    fn get(&self, id: ObjectId) -> Option<Arc<T>> {
        self.0.lock().expect("manifest cache").entries.get(&id).map(|(_, summary)| Arc::clone(summary))
    }

    fn insert_as(&self, id: ObjectId, summary: T) -> Arc<T> {
        let summary = Arc::new(summary);
        let mut cache = self.0.lock().expect("manifest cache");
        cache.inserted += 1;
        let stamp = cache.inserted;
        cache.entries.insert(id, (stamp, Arc::clone(&summary)));
        summary
    }

    /// Marks the start of a closure, for [`Self::retain_reached`].
    fn mark(&self) -> u64 {
        self.0.lock().expect("manifest cache").inserted
    }

    /// Keeps the summaries a closure started at `mark` reached, and every
    /// summary inserted after it, which a newer closure may need.
    fn retain_reached(&self, mark: u64, reached: &BTreeMap<ObjectId, Arc<T>>) {
        self.0.lock().expect("manifest cache").entries
            .retain(|id, (stamp, _)| *stamp > mark || reached.contains_key(id));
    }
}

impl ManifestCache {
    pub(crate) fn insert(&self, manifest: VerifiedManifest) -> Arc<VerifiedManifest> {
        self.insert_as(manifest.id(), manifest)
    }
}

/// The retained records reachability reads, captured as data so a writer can
/// resolve them without holding its database.
#[derive(Clone)]
pub(crate) struct ClosureInputs {
    base: Option<Checkpoint>,
    records: Vec<Record>,
}

impl ClosureInputs {
    pub(crate) async fn resolve(&self, stream: &Stream, store: &dyn ObjectStore, epoch: Epoch) -> Result<Reachability> {
        closure(stream, store, epoch, self.base.as_ref(), &self.records).await
    }
}

/// The checkpoint a replay installed before its first retained record, and
/// the position of the record that announced it. A classic base is announced
/// at the floor. A deferred base is announced later in the retained suffix
/// and covers exactly the records before the floor.
#[derive(Clone)]
pub(crate) struct ReplayBase {
    pub(crate) announced: Lsn,
    pub(crate) checkpoint: Checkpoint,
}

pub(crate) struct Committed {
    pub(crate) transactions: TransactState,
    pub(crate) floor: Lsn,
    pub(crate) base: Option<ReplayBase>,
    pub(crate) head: Lsn,
    pub(crate) epoch: Epoch,
    pub(crate) generation: u64,
    pub(crate) lease: Option<(Epoch, i64)>,
    pub(crate) released: Option<Epoch>,
    pub(crate) retired: Option<(Lsn, i64)>,
    pub(crate) records: Vec<Record>,
    pub(crate) effects: Vec<AppliedEffects>,
    pub(crate) rotation: Option<Rotation>,
    pub(crate) coordinates: JournalCoordinates,
}

impl Committed {
    fn empty(stream: &Stream, floor: Lsn) -> Self {
        Self {
            transactions: TransactState::new(*stream.database.as_bytes()),
            floor,
            base: None,
            head: Lsn(floor.0 - 1),
            epoch: Epoch(0),
            generation: 1,
            lease: None,
            released: None,
            retired: None,
            records: Vec::new(),
            effects: Vec::new(),
            rotation: None,
            coordinates: JournalCoordinates::default(),
        }
    }

    pub(crate) async fn read_through(
        stream: &Stream,
        store: &dyn ObjectStore,
        end: Lsn,
        previous: Option<Self>,
    ) -> std::result::Result<Self, ReplayError> {
        match Self::read_prefix(stream, store, end, previous).await {
            Err(ReplayError::Failed { error, resume }) if error.code == "rtdb/erased-under-view" => {
                let cut = match find_clean_checkpoint(stream, end).await {
                    Ok(Some(cut)) => cut,
                    Ok(None) => return Err(ReplayError::Failed { error, resume }),
                    Err(error) => return Err(ReplayError::Failed { error, resume }),
                };
                Self::read_prefix(stream, store, end, Some(Self::empty(stream, cut))).await
            }
            result => result,
        }
    }

    async fn read_prefix(
        stream: &Stream,
        store: &dyn ObjectStore,
        end: Lsn,
        mut previous: Option<Self>,
    ) -> std::result::Result<Self, ReplayError> {
        if let Some(state) = previous.as_ref().filter(|state| state.head > end) {
            return Err(ReplayError::Shortened { head: end, replayed: state.head });
        }
        let mut from = next(previous.as_ref().map_or(Lsn(0), |state| state.head))
            .map_err(|error| ReplayError::failed(error, previous.take()))?;
        loop {
            let page = match stream.read(from).await {
                Ok(page) => page,
                Err(JournalError::Trimmed { floor }) if from < floor && floor <= end => {
                    previous = None;
                    from = floor;
                    continue;
                }
                Err(error) => return Err(ReplayError::failed(journal_error(error), previous)),
            };
            if page.floor == Lsn(0) || page.head < Lsn(page.floor.0 - 1) {
                return Err(ReplayError::failed(invalid("The journal page has invalid bounds"), previous));
            }
            if previous
                .as_ref()
                .is_some_and(|state| state.floor < page.floor)
            {
                previous = None;
                from = page.floor;
                continue;
            }
            let mut state = previous
                .take()
                .unwrap_or_else(|| Self::empty(stream, page.floor));
            if state.awaiting_base() {
                let classic = match page.records.first() {
                    Some((lsn, bytes)) if *lsn == state.floor => matches!(
                        stream.decode(*lsn, bytes).await.map_err(|error| ReplayError::failed(error, None))?.record,
                        Record::Checkpoint(checkpoint) if checkpoint.through.checked_add(1) == Some(state.floor.0)
                    ),
                    _ => false,
                };
                if !classic {
                    match find_deferred_base(stream, state.floor).await.map_err(|error| ReplayError::failed(error, None))? {
                        BaseSearch::Found { announced, epoch, checkpoint } => {
                            state.install_checkpoint(stream, store, announced, epoch, &checkpoint).await
                                .map_err(|error| ReplayError::failed(error, None))?;
                            state.generation = checkpoint.key_generation;
                            state.base = Some(ReplayBase { announced, checkpoint });
                        }
                        BaseSearch::Moved(floor) => {
                            from = floor;
                            continue;
                        }
                    }
                }
            }
            for (lsn, bytes) in page.records.into_iter().take_while(|(lsn, _)| *lsn <= end) {
                let entry = match stream.decode(lsn, &bytes).await {
                    Ok(entry) => entry,
                    Err(error) => return Err(ReplayError::failed(error, Some(state))),
                };
                // A failed entry may have applied part of itself, so the state
                // is no longer a prefix of the journal.
                state
                    .apply_entry(stream, store, lsn, entry)
                    .await
                    .map_err(|error| ReplayError::failed(error, None))?;
            }
            if state.head == end {
                return Ok(state);
            }
            let following = match next(state.head) {
                Ok(following) if following > from => following,
                Ok(_) => return Err(ReplayError::failed(journal_error(JournalError::Unavailable), Some(state))),
                Err(error) => return Err(ReplayError::failed(error, Some(state))),
            };
            from = following;
            previous = Some(state);
        }
    }

    /// A suffix that starts after LSN 1 replays over a checkpoint of the
    /// records before its floor. Until one is installed, no record applies.
    fn awaiting_base(&self) -> bool {
        self.records.is_empty() && self.base.is_none() && self.floor != Lsn(1)
    }

    pub(crate) async fn apply_entry(
        &mut self, stream: &Stream, store: &dyn ObjectStore, lsn: Lsn, entry: Entry,
    ) -> Result<Option<Receipt>> {
        if let Record::Checkpoint(checkpoint) = &entry.record {
            let first = self.records.is_empty() && self.base.is_none();
            if first || checkpoint.restore {
                if checkpoint.restore && (self.transactions.index().head().basis.position() != 0
                    || self.coordinates.origin.is_some() || self.rotation.is_some()
                    || checkpoint.key_generation != self.generation
                    || self.records.iter().any(|record| !matches!(record, Record::Lease { .. } | Record::WriterRelease { .. })))
                {
                    return Err(invalid("Restore requires a fresh deployment"));
                }
                self.install_checkpoint(stream, store, lsn, entry.epoch, checkpoint).await?;
                self.generation = checkpoint.key_generation;
                if first {
                    self.base = Some(ReplayBase { announced: lsn, checkpoint: checkpoint.clone() });
                }
            }
        } else if self.awaiting_base() {
            return Err(invalid("The retained floor does not name a checkpoint"));
        }
        self.apply(lsn, entry)
    }

    pub(crate) fn apply(&mut self, lsn: Lsn, entry: Entry) -> Result<Option<Receipt>> {
        if lsn != next(self.head)? || entry.epoch < self.epoch {
            return Err(invalid("The journal records are out of order"));
        }
        if self.released == Some(entry.epoch) {
            return Err(invalid("A released writer epoch cannot append again"));
        }
        if self.retired.is_some() {
            return Err(invalid("A retired deployment cannot append again"));
        }
        if let Some(rotation) = self.rotation
            && !rotation.committed
            && lsn > rotation.cut
            && !matches!(&entry.record, Record::KeyRotate { .. })
        {
            return Err(invalid("The clean checkpoint is awaiting key rotation"));
        }
        let mut receipt = None;
        let mut effects = None;
        let previous = self.transactions.index().head();
        let generation = match &entry.record {
            Record::Tx { payload, .. } => {
                let record = TransactionRecord::decode(payload)?;
                let TransactionRecord::Tx(transaction) = &record else {
                    return Err(invalid("A Tx envelope must contain a resolved transaction"));
                };
                effects = Some(transaction.effects().to_vec());
                receipt = self.transactions.apply(self.coordinates.position(lsn)?, record)?;
                self.generation
            }
            Record::Settle { tx_lsn } => {
                let offset = tx_lsn
                    .checked_sub(self.floor.0)
                    .and_then(|offset| usize::try_from(offset).ok());
                let Some(Record::Tx { payload, .. }) =
                    offset.and_then(|offset| self.records.get(offset))
                else {
                    return Err(invalid("Settle has no retained transaction"));
                };
                let TransactionRecord::Tx(transaction) = TransactionRecord::decode(payload)? else {
                    return Err(invalid("Settle does not refer to a resolved transaction"));
                };
                effects = Some(transaction.effects().to_vec());
                receipt = self
                    .transactions
                    .apply(self.coordinates.position(lsn)?, TransactionRecord::Settle {
                        tx_lsn: self.coordinates.position(Lsn(*tx_lsn))?,
                    })?;
                self.generation
            }
            Record::Refuse { tx_lsn } => {
                receipt = self
                    .transactions
                    .apply(self.coordinates.position(lsn)?, TransactionRecord::Refuse {
                        tx_lsn: self.coordinates.position(Lsn(*tx_lsn))?,
                    })?;
                self.generation
            }
            Record::Lease { until } => {
                if self
                    .lease
                    .is_some_and(|(epoch, old)| entry.epoch == epoch && *until <= old)
                {
                    return Err(invalid("The writer lease did not advance"));
                }
                self.lease = Some((entry.epoch, *until));
                self.generation
            }
            Record::WriterRelease { lease_until } => {
                if self.lease != Some((entry.epoch, *lease_until))
                    || self.transactions.recover_pending().is_some()
                    || self.transactions.pending_erasure().is_some()
                    || self.rotation.is_some_and(|rotation| !rotation.retirement_armed)
                {
                    return Err(invalid("Writer release does not close a settled current lease"));
                }
                self.lease = None;
                self.released = Some(entry.epoch);
                self.generation
            }
            Record::DeploymentRetirement { lease_until } => {
                if self.lease != Some((entry.epoch, *lease_until))
                    || self.transactions.recover_pending().is_some()
                    || self.transactions.pending_erasure().is_some()
                    || self.rotation.is_some_and(|rotation| !rotation.retirement_armed)
                    || self.transactions.index().has_custody_groups()
                {
                    return Err(invalid("Deployment retirement requires a settled writer and custody closure"));
                }
                self.lease = None;
                self.retired = Some((lsn, *lease_until));
                self.generation
            }
            Record::Checkpoint(checkpoint) => {
                if checkpoint.key_generation != entry.generation
                    || checkpoint.through != self.head.0 && !self.announces_deferred(checkpoint)
                {
                    return Err(invalid(
                        "The checkpoint does not cover the preceding position",
                    ));
                }
                if checkpoint.key_generation != self.generation {
                    if self.generation.checked_add(1) != Some(checkpoint.key_generation) {
                        return Err(invalid("The checkpoint skips a key generation"));
                    }
                    self.rotation = Some(Rotation {
                        cut: lsn,
                        generation: checkpoint.key_generation,
                        committed: false,
                        retirement: None,
                        retirement_armed: false,
                    });
                }
                checkpoint.key_generation
            }
            Record::RankedCatalog(catalog) => {
                let cut = BranchCut {
                    basis: Basis::new(previous.basis.incarnation(), catalog.position),
                    system_time: Timestamp(catalog.system_time),
                };
                if catalog.manifest.database.as_bytes() != &previous.basis.incarnation()
                    || catalog.manifest.epoch != entry.epoch.0
                    || self.lease.map(|(epoch, _)| epoch) != Some(entry.epoch)
                    || catalog.key_generation != self.generation
                    || catalog.position == 0
                    || self.transactions.index().exact(cut.basis).ok() != Some(cut)
                    || self.transactions.recover_pending().is_some()
                    || self.transactions.pending_erasure().is_some()
                {
                    return Err(invalid("Ranked catalogue does not cover the current fenced cut"));
                }
                self.generation
            }
            Record::KeyRotate { generation, at } => {
                if *at != self.head.0
                    || !matches!(self.records.last(), Some(Record::Checkpoint(checkpoint))
                    if checkpoint.key_generation == *generation)
                {
                    return Err(invalid("Key rotation must follow its clean checkpoint"));
                }
                let rotation = self
                    .rotation
                    .as_mut()
                    .filter(|rotation| {
                        !rotation.committed
                            && rotation.cut == Lsn(*at)
                            && rotation.generation == *generation
                    })
                    .ok_or_else(|| invalid("Key rotation has no staged checkpoint"))?;
                rotation.committed = true;
                *generation
            }
            Record::KeyRetirement { generation, rotation_lsn, observed_at, unreadable_by, armed } => {
                let rotation = self.rotation.as_mut().filter(|rotation| {
                    rotation.committed
                        && rotation.generation == *generation
                        && rotation.cut.0.checked_add(1) == Some(*rotation_lsn)
                }).ok_or_else(|| invalid("Key retirement does not match an unfenced rotation"))?;
                if unreadable_by.is_some_and(|deadline| deadline < *observed_at) {
                    return Err(invalid("The key retirement deadline precedes the rotation observation"));
                }
                if *armed {
                    let deadline = unreadable_by.ok_or_else(|| invalid("The armed retirement has no deadline"))?;
                    if rotation.retirement_armed || !rotation.retirement.is_some_and(|prior| {
                        prior.observed_at == *observed_at && prior.unreadable_by.is_none_or(|prior| prior == deadline)
                    }) {
                        return Err(invalid("The armed retirement changed the observed deadline"));
                    }
                    rotation.retirement_armed = true;
                    if let Some(basis) = self.transactions.pending_erasure() {
                        self.transactions.arm_erasure(basis, Timestamp(deadline))?;
                    }
                } else {
                    if rotation.retirement.is_some() {
                        return Err(invalid("The rotation already has a retirement observation"));
                    }
                    rotation.retirement = Some(RetirementObservation { observed_at: *observed_at, unreadable_by: *unreadable_by });
                }
                self.generation
            }
            Record::Pin(_) | Record::Unpin { .. } => self.generation,
        };
        if generation != entry.generation {
            return Err(invalid(
                "The journal record uses an unexpected key generation",
            ));
        }
        self.generation = generation;
        self.epoch = entry.epoch;
        self.head = lsn;
        self.records.push(entry.record);
        if let (Some(receipt), Some(effects)) = (&receipt, effects)
            && !effects.is_empty()
        {
            self.effects.push(AppliedEffects {
                cut: BranchCut {
                    basis: receipt.basis,
                    system_time: receipt.system_time,
                },
                previous,
                effects,
            });
        }
        Ok(receipt)
    }

    /// A deferred checkpoint describes a cut before the record that announces
    /// it. The records between stay retained and replay after it, so its cut
    /// must lie in this suffix, in the current key generation, with no
    /// restored history installed after it.
    pub(crate) fn announces_deferred(&self, checkpoint: &Checkpoint) -> bool {
        let Some(first) = checkpoint.through.checked_add(1) else { return false };
        !checkpoint.restore
            && checkpoint.through < self.head.0
            && first >= self.floor.0
            && checkpoint.key_generation == self.generation
            && self.records.get((first - self.floor.0) as usize..).is_some_and(|after| {
                !after.iter().any(|record| matches!(record,
                    Record::Checkpoint(Checkpoint { restore: true, .. }) | Record::KeyRotate { .. } | Record::KeyRetirement { .. }))
            })
    }

    /// Installs a checkpoint of every record through `self.head`. A classic or
    /// restored checkpoint is announced at the next position. A deferred base
    /// is announced later and is never rotating or restored.
    async fn install_checkpoint(
        &mut self,
        stream: &Stream,
        store: &dyn ObjectStore,
        lsn: Lsn,
        epoch: Epoch,
        checkpoint: &Checkpoint,
    ) -> Result<()> {
        let image = read_manifest(store, stream.kms.as_ref(), checkpoint.manifest).await?;
        let deferred = next(self.head)? != lsn;
        if checkpoint.manifest.database != stream.database
            || checkpoint.through != self.head.0
            || deferred && (lsn <= self.head || checkpoint.restore)
            || image.generation.number != checkpoint.key_generation
        {
            return Err(invalid(
                "The manifest does not belong to the retained checkpoint",
            ));
        }
        stream.manifests.insert(image.summary());
        let (header, mut bytes) = image
            .contents()
            .payload
            .split_first_chunk::<25>()
            .ok_or_else(|| invalid("The checkpoint image has no basis"))?;
        let position = u64::from_be_bytes(header[..8].try_into().unwrap());
        let system_time = Timestamp(i64::from_be_bytes(header[8..16].try_into().unwrap()));
        let until = i64::from_be_bytes(header[16..24].try_into().unwrap());
        let flags = header[24];
        if flags & !(ROTATING_CHECKPOINT | RESTORED_HISTORY | CHECKPOINT_SOURCE | BINARY_STATE) != 0
            || flags & CHECKPOINT_SOURCE != 0 && flags & RESTORED_HISTORY == 0
        {
            return Err(invalid("The checkpoint has an unknown generation marker"));
        }
        if flags & RESTORED_HISTORY != 0 {
            let (offset, rest) = bytes.split_first_chunk::<8>().ok_or_else(|| invalid("The checkpoint has no restoration offset"))?;
            let (digest, rest) = rest.split_first_chunk::<32>().ok_or_else(|| invalid("The checkpoint has no restoration identity"))?;
            let (through, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("The checkpoint has no restoration cut"))?;
            let through = u64::from_be_bytes(*through);
            let (source, rest) = if flags & CHECKPOINT_SOURCE != 0 {
                let (basis, rest) = rest.split_first_chunk::<24>().ok_or_else(|| invalid("The checkpoint has no source basis"))?;
                let (time, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("The checkpoint has no source system time"))?;
                let (pin, rest) = rest.split_first_chunk::<16>().ok_or_else(|| invalid("The checkpoint has no source pin"))?;
                let basis = Basis::from_bytes(basis)?;
                let time = Timestamp(i64::from_be_bytes(*time));
                let pin = Uuid::from_bytes(*pin);
                if pin.is_nil() || basis.incarnation() == *stream.database.as_bytes()
                    || basis.position() != through || checkpoint.restore && time != system_time
                {
                    return Err(invalid("The source pin does not match the restored cut"));
                }
                (Some(CheckpointSource { cut: BranchCut { basis, system_time: time }, pin }), rest)
            } else {
                (None, rest)
            };
            self.coordinates = JournalCoordinates {
                offset: u64::from_be_bytes(*offset),
                origin: Some(RestoreOrigin { digest: *digest, through, source }),
            };
            bytes = rest;
        } else if checkpoint.restore {
            return Err(invalid("The restored checkpoint has no origin"));
        }
        if flags & ROTATING_CHECKPOINT != 0 {
            if checkpoint.restore { return Err(invalid("A restored checkpoint cannot stage a rotation")); }
            if deferred { return Err(invalid("A deferred checkpoint cannot stage a rotation")); }
            self.rotation = Some(Rotation {
                cut: lsn,
                generation: checkpoint.key_generation,
                committed: false,
                retirement: None,
                retirement_armed: false,
            });
        }
        if position > self.coordinates.position(Lsn(checkpoint.through))?
            || self.coordinates.origin.is_some_and(|origin| {
                origin.through > position || (checkpoint.restore && origin.through != position)
            })
        {
            return Err(invalid("The checkpoint image extends past its cut"));
        }
        let expected = BranchCut {
            basis: Basis::new(*stream.database.as_bytes(), position),
            system_time,
        };
        self.transactions = if flags & BINARY_STATE != 0 {
            TransactState::decode_binary_checkpoint(bytes, expected)?
        } else {
            TransactState::decode_checkpoint(bytes, expected)?
        };
        self.lease = Some((epoch, until));
        Ok(())
    }

    pub(crate) fn checkpoint_payload(
        transactions: &TransactState,
        until: i64,
        rotating: bool,
        coordinates: JournalCoordinates,
    ) -> Result<Vec<u8>> {
        Self::checkpoint_payload_in(transactions, until, rotating, coordinates, false)
    }

    /// The checkpoint payload with the transaction state in CBOR when
    /// `binary`, which only readers that know [`BINARY_STATE`] install.
    pub(crate) fn checkpoint_payload_in(
        transactions: &TransactState,
        until: i64,
        rotating: bool,
        coordinates: JournalCoordinates,
        binary: bool,
    ) -> Result<Vec<u8>> {
        let cut = transactions.index().head();
        let bytes = if binary { transactions.encode_binary_checkpoint()? } else { transactions.encode_checkpoint()? };
        let source = coordinates.origin.and_then(|origin| origin.source);
        let flags = if rotating { ROTATING_CHECKPOINT } else { 0 }
            | if binary { BINARY_STATE } else { 0 }
            | if coordinates.origin.is_some() { RESTORED_HISTORY } else { 0 }
            | if source.is_some() { CHECKPOINT_SOURCE } else { 0 };
        let extra = coordinates.origin.map_or(0, |_| 48) + source.map_or(0, |_| 48);
        let mut payload = Vec::with_capacity(25 + extra + bytes.len());
        payload.extend_from_slice(&cut.basis.position().to_be_bytes());
        payload.extend_from_slice(&cut.system_time.0.to_be_bytes());
        payload.extend_from_slice(&until.to_be_bytes());
        payload.push(flags);
        if let Some(origin) = coordinates.origin {
            payload.extend_from_slice(&coordinates.offset.to_be_bytes());
            payload.extend_from_slice(&origin.digest);
            payload.extend_from_slice(&origin.through.to_be_bytes());
        }
        if let Some(source) = source {
            payload.extend_from_slice(&source.cut.basis.to_bytes());
            payload.extend_from_slice(&source.cut.system_time.0.to_be_bytes());
            payload.extend_from_slice(source.pin.as_bytes());
        }
        payload.extend_from_slice(&bytes);
        Ok(payload)
    }

    pub(crate) async fn reachability(
        &self,
        stream: &Stream,
        store: &dyn ObjectStore,
        epoch: Epoch,
    ) -> Result<Reachability> {
        let base = self.base.as_ref().map(|base| &base.checkpoint);
        closure(stream, store, epoch, base, &self.records).await
    }

    /// The base and the retained records reachability reads, without
    /// transaction payloads.
    pub(crate) fn closure_inputs(&self) -> ClosureInputs {
        ClosureInputs {
            base: self.base.as_ref().map(|base| base.checkpoint.clone()),
            records: self.records.iter().filter_map(|record| match record {
                Record::Tx { objects, .. } if objects.is_empty() => None,
                Record::Tx { objects, .. } => Some(Record::Tx { payload: Vec::new(), objects: objects.clone() }),
                Record::Checkpoint(_) | Record::RankedCatalog(_) | Record::Pin(_) | Record::Unpin { .. } => Some(record.clone()),
                _ => None,
            }).collect(),
        }
    }

    /// Drops the records before `floor` after this writer trimmed through a
    /// checkpoint it announced, without reading the journal or the manifest
    /// again. The result is the state a fresh replay from `floor` installs:
    /// the transaction state is unchanged, and the lease, release and rotation
    /// are those the base installs unless a retained record set them.
    pub(crate) fn advance_floor(&mut self, base: ReplayBase, until: i64, epoch: Epoch) -> bool {
        let floor = Lsn(base.checkpoint.through + 1);
        if floor <= self.floor || floor > next(self.head).unwrap_or(self.head) || base.announced < floor
            || base.announced > self.head || self.rotation.is_some() || self.retired.is_some()
            || base.checkpoint.restore || base.checkpoint.key_generation != self.generation
            || !matches!(self.records.get((base.announced.0 - self.floor.0) as usize),
                Some(Record::Checkpoint(announced)) if *announced == base.checkpoint)
        {
            return false;
        }
        let Ok(position) = self.coordinates.position(floor) else { return false };
        self.records.drain(..(floor.0 - self.floor.0) as usize);
        self.effects.retain(|group| group.cut.basis.position() >= position);
        self.floor = floor;
        self.base = Some(base);
        if !self.records.iter().any(|record| matches!(record, Record::Lease { .. })) {
            self.lease = Some((epoch, until));
        }
        if !self.records.iter().any(|record| matches!(record, Record::WriterRelease { .. })) {
            self.released = None;
        }
        true
    }
}

async fn closure(
    stream: &Stream,
    store: &dyn ObjectStore,
    epoch: Epoch,
    base: Option<&Checkpoint>,
    records: &[Record],
) -> Result<Reachability> {
    let mut roots = BTreeSet::new();
    let mut checkpoints = BTreeSet::new();
    if let Some(checkpoint) = base {
        roots.insert(checkpoint.manifest);
        checkpoints.insert(checkpoint.manifest);
    }
    for record in records {
        match record {
            Record::Tx { objects, .. } => roots.extend(
                objects
                    .iter()
                    .copied()
                    .filter(|id| id.kind == ObjectKind::Manifest),
            ),
            Record::Checkpoint(checkpoint) => {
                roots.insert(checkpoint.manifest);
                checkpoints.insert(checkpoint.manifest);
            }
            Record::RankedCatalog(catalog) => {
                roots.insert(catalog.manifest);
            }
            Record::Pin(pin) => {
                roots.insert(pin.manifest);
            }
            _ => {}
        }
    }
    let mark = stream.manifests.mark();
    let mut manifests = BTreeMap::<ObjectId, Arc<VerifiedManifest>>::new();
    while let Some(id) = roots.pop_first() {
        if manifests.contains_key(&id) {
            continue;
        }
        if id.database != stream.database || id.epoch > epoch.0 {
            return Err(invalid("A manifest is outside the writer scope"));
        }
        let manifest = match stream.manifests.get(id) {
            Some(manifest) => manifest,
            None => stream.manifests.insert(read_manifest(store, stream.kms.as_ref(), id).await?.summary()),
        };
        if checkpoints.contains(&id) {
            roots.extend(manifest.contents().pins.iter().map(|pin| pin.manifest));
        }
        manifests.insert(id, manifest);
    }
    stream.manifests.retain_reached(mark, &manifests);
    Reachability::from_retained(stream.database, epoch.0, base, records, manifests.values().map(Arc::as_ref))
}

enum BaseSearch {
    Found { announced: Lsn, epoch: Epoch, checkpoint: Checkpoint },
    /// The floor advanced during the search. Replay starts again from it.
    Moved(Lsn),
}

/// Finds the deferred base of a suffix whose floor is an ordinary record: the
/// first retained checkpoint whose cut is the record before the floor. The
/// writer announces it before trimming, so it lies within the retained head.
async fn find_deferred_base(stream: &Stream, floor: Lsn) -> Result<BaseSearch> {
    let mut at = floor;
    loop {
        let page = match stream.read(at).await {
            Ok(page) => page,
            Err(JournalError::Trimmed { floor: moved }) if moved > floor => return Ok(BaseSearch::Moved(moved)),
            Err(error) => return Err(journal_error(error)),
        };
        if page.floor > floor {
            return Ok(BaseSearch::Moved(page.floor));
        }
        if page.records.first().is_none_or(|(lsn, _)| *lsn != at) {
            if at > page.head {
                return Err(invalid("The retained floor names no checkpoint"));
            }
            return Err(journal_error(JournalError::Unavailable));
        }
        for (lsn, bytes) in page.records {
            if lsn != at {
                return Err(journal_error(JournalError::Unavailable));
            }
            if let Entry { epoch, record: Record::Checkpoint(checkpoint), .. } = stream.decode(lsn, &bytes).await?
                && checkpoint.through.checked_add(1) == Some(floor.0)
            {
                return Ok(BaseSearch::Found { announced: lsn, epoch, checkpoint });
            }
            at = next(lsn)?;
        }
    }
}

async fn find_clean_checkpoint(stream: &Stream, head: Lsn) -> Result<Option<Lsn>> {
    let mut at = Lsn(1);
    let mut possible = None;
    let mut clean = None;
    while at <= head {
        let page = match stream.read(at).await {
            Ok(page) => page,
            Err(JournalError::Trimmed { floor }) if floor > at => {
                possible = None;
                at = floor;
                continue;
            }
            Err(error) => return Err(journal_error(error)),
        };
        if page.records.first().is_none_or(|(lsn, _)| *lsn != at) {
            return Err(journal_error(JournalError::Unavailable));
        }
        for (lsn, bytes) in page.records.into_iter().take_while(|(lsn, _)| *lsn <= head) {
            if lsn != at {
                return Err(invalid(
                    "The journal page omitted part of the rotation prefix",
                ));
            }
            at = next(lsn)?;
            match stream.decode(lsn, &bytes).await {
                Ok(Entry {
                    record: Record::Checkpoint(checkpoint),
                    ..
                }) => possible = Some((lsn, checkpoint.key_generation)),
                Ok(Entry {
                    record:
                        Record::KeyRotate {
                            generation,
                            at: cut,
                        },
                    ..
                }) if possible == Some((Lsn(cut), generation)) => {
                    clean = Some(Lsn(cut));
                    possible = None;
                }
                Err(error) if error.code != "rtdb/erased-under-view" => return Err(error),
                _ => possible = None,
            }
        }
    }
    Ok(clean)
}

fn invalid(message: &str) -> Anomaly {
    Anomaly::fault("rtdb/multi-replay", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(seq: u64) -> ObjectId {
        ObjectId { database: Uuid::nil(), kind: ObjectKind::Manifest, epoch: 1, seq, digest: [0; 32] }
    }

    #[test]
    fn a_closure_drops_the_summaries_it_no_longer_reaches() {
        let cache = SummaryCache::<u64>::default();
        for seq in 0..4 {
            cache.insert_as(id(seq), seq);
        }
        let mark = cache.mark();
        // Committed while the closure ran, so the closure could not reach it.
        cache.insert_as(id(9), 9);
        let reached: BTreeMap<_, _> = [2, 3].into_iter().map(|seq| (id(seq), cache.get(id(seq)).unwrap())).collect();
        cache.retain_reached(mark, &reached);
        assert!(cache.get(id(0)).is_none());
        assert!(cache.get(id(1)).is_none());
        assert_eq!(cache.get(id(2)).as_deref(), Some(&2));
        assert_eq!(cache.get(id(3)).as_deref(), Some(&3));
        assert_eq!(cache.get(id(9)).as_deref(), Some(&9));
    }
}
