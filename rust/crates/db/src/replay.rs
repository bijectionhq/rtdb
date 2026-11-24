use std::collections::{BTreeMap, BTreeSet};

use journal_contract::{Epoch, JournalError, Lsn};
use rtdb_format::journal::{Checkpoint, ObjectId, ObjectKind, Record};
use rtdb_index::branches::{BranchCut, BranchEffect, BranchIndex};
use rtdb_store::{ObjectStore, Reachability, VerifiedManifest, read_manifest};
use rtdb_tx::transact::{Receipt, TransactState, TransactionRecord};
use rtdb_types::{Anomaly, Result, Timestamp, rtdb_v1::Basis};

use crate::journal::{Entry, Stream, journal_error, next};

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

pub(crate) struct Committed {
    pub(crate) transactions: TransactState,
    pub(crate) floor: Lsn,
    pub(crate) head: Lsn,
    pub(crate) epoch: Epoch,
    pub(crate) generation: u64,
    pub(crate) lease: Option<(Epoch, i64)>,
    pub(crate) released: Option<Epoch>,
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
            head: Lsn(floor.0 - 1),
            epoch: Epoch(0),
            generation: 1,
            lease: None,
            released: None,
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
    ) -> Result<Self> {
        match Self::read_prefix(stream, store, end, previous).await {
            Err(error) if error.code == "rtdb/erased-under-view" => {
                let cut = find_clean_checkpoint(stream, end).await?.ok_or(error)?;
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
    ) -> Result<Self> {
        let mut from = next(previous.as_ref().map_or(Lsn(0), |state| state.head))?;
        loop {
            let page = match stream.read(from).await {
                Ok(page) => page,
                Err(JournalError::Trimmed { floor }) if from < floor && floor <= end => {
                    previous = None;
                    from = floor;
                    continue;
                }
                Err(error) => return Err(journal_error(error)),
            };
            if page.floor == Lsn(0) || page.head < Lsn(page.floor.0 - 1) {
                return Err(invalid("The journal page has invalid bounds"));
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
            for (lsn, bytes) in page.records.into_iter().take_while(|(lsn, _)| *lsn <= end) {
                let entry = stream.decode(lsn, &bytes).await?;
                state.apply_entry(stream, store, lsn, entry).await?;
            }
            if state.head == end {
                return Ok(state);
            }
            let following = next(state.head)?;
            if following <= from {
                return Err(journal_error(JournalError::Unavailable));
            }
            from = following;
            previous = Some(state);
        }
    }

    pub(crate) async fn apply_entry(
        &mut self, stream: &Stream, store: &dyn ObjectStore, lsn: Lsn, entry: Entry,
    ) -> Result<Option<Receipt>> {
        if let Record::Checkpoint(checkpoint) = &entry.record {
            if self.records.is_empty() || checkpoint.restore {
                if checkpoint.restore && (self.transactions.index().head().basis.position() != 0
                    || self.coordinates.origin.is_some() || self.rotation.is_some()
                    || checkpoint.key_generation != self.generation
                    || self.records.iter().any(|record| !matches!(record, Record::Lease { .. } | Record::WriterRelease { .. })))
                {
                    return Err(invalid("Restore requires a fresh deployment"));
                }
                self.install_checkpoint(stream, store, lsn, entry.epoch, checkpoint).await?;
                self.generation = checkpoint.key_generation;
            }
        } else if self.records.is_empty() && self.floor != Lsn(1) {
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
            Record::Checkpoint(checkpoint) => {
                if checkpoint.through != self.head.0
                    || checkpoint.key_generation != entry.generation
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

    async fn install_checkpoint(
        &mut self,
        stream: &Stream,
        store: &dyn ObjectStore,
        lsn: Lsn,
        epoch: Epoch,
        checkpoint: &Checkpoint,
    ) -> Result<()> {
        let image = read_manifest(store, stream.kms.as_ref(), checkpoint.manifest).await?;
        if checkpoint.manifest.database != stream.database
            || checkpoint.through != lsn.0 - 1
            || image.generation.number != checkpoint.key_generation
        {
            return Err(invalid(
                "The manifest does not belong to the retained checkpoint",
            ));
        }
        let (header, mut bytes) = image
            .contents()
            .payload
            .split_first_chunk::<25>()
            .ok_or_else(|| invalid("The checkpoint image has no basis"))?;
        let position = u64::from_be_bytes(header[..8].try_into().unwrap());
        let system_time = Timestamp(i64::from_be_bytes(header[8..16].try_into().unwrap()));
        let until = i64::from_be_bytes(header[16..24].try_into().unwrap());
        if header[24] > 3 {
            return Err(invalid("The checkpoint has an unknown generation marker"));
        }
        if header[24] & 2 != 0 {
            let (offset, rest) = bytes.split_first_chunk::<8>().ok_or_else(|| invalid("The checkpoint has no restoration offset"))?;
            let (digest, rest) = rest.split_first_chunk::<32>().ok_or_else(|| invalid("The checkpoint has no restoration identity"))?;
            let (through, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("The checkpoint has no restoration cut"))?;
            self.coordinates = JournalCoordinates {
                offset: u64::from_be_bytes(*offset),
                origin: Some(RestoreOrigin { digest: *digest, through: u64::from_be_bytes(*through) }),
            };
            bytes = rest;
        } else if checkpoint.restore {
            return Err(invalid("The restored checkpoint has no origin"));
        }
        if header[24] & 1 != 0 {
            if checkpoint.restore { return Err(invalid("A restored checkpoint cannot stage a rotation")); }
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
        self.transactions = TransactState::decode_checkpoint(bytes, expected)?;
        self.lease = Some((epoch, until));
        Ok(())
    }

    pub(crate) fn checkpoint_payload(
        transactions: &TransactState,
        until: i64,
        rotating: bool,
        coordinates: JournalCoordinates,
    ) -> Result<Vec<u8>> {
        let cut = transactions.index().head();
        let bytes = transactions.encode_checkpoint()?;
        let mut payload = Vec::with_capacity(25 + coordinates.origin.map_or(0, |_| 48) + bytes.len());
        payload.extend_from_slice(&cut.basis.position().to_be_bytes());
        payload.extend_from_slice(&cut.system_time.0.to_be_bytes());
        payload.extend_from_slice(&until.to_be_bytes());
        payload.push(u8::from(rotating) | (u8::from(coordinates.origin.is_some()) << 1));
        if let Some(origin) = coordinates.origin {
            payload.extend_from_slice(&coordinates.offset.to_be_bytes());
            payload.extend_from_slice(&origin.digest);
            payload.extend_from_slice(&origin.through.to_be_bytes());
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
        let mut roots = BTreeSet::new();
        let mut checkpoints = BTreeSet::new();
        for record in &self.records {
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
                Record::Pin(pin) => {
                    roots.insert(pin.manifest);
                }
                _ => {}
            }
        }
        let mut manifests = BTreeMap::<ObjectId, VerifiedManifest>::new();
        while let Some(id) = roots.pop_first() {
            if manifests.contains_key(&id) {
                continue;
            }
            if id.database != stream.database || id.epoch > epoch.0 {
                return Err(invalid("A manifest is outside the writer scope"));
            }
            let manifest = read_manifest(store, stream.kms.as_ref(), id).await?;
            if checkpoints.contains(&id) {
                roots.extend(manifest.contents().pins.iter().map(|pin| pin.manifest));
            }
            manifests.insert(id, manifest);
        }
        Reachability::from_records(stream.database, epoch.0, &self.records, manifests.values())
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
