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
    pub floor: Lsn,
    pub head: Lsn,
    pub groups: &'a [AppliedEffects],
    pub now: Timestamp,
}

pub(crate) struct Committed {
    pub(crate) transactions: TransactState,
    pub(crate) floor: Lsn,
    pub(crate) head: Lsn,
    pub(crate) epoch: Epoch,
    pub(crate) generation: u64,
    pub(crate) lease: Option<(Epoch, i64)>,
    pub(crate) records: Vec<Record>,
    pub(crate) effects: Vec<AppliedEffects>,
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
            records: Vec::new(),
            effects: Vec::new(),
        }
    }

    pub(crate) async fn read_through(
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
                if state.records.is_empty() {
                    match &entry.record {
                        Record::Checkpoint(checkpoint) => {
                            state
                                .install_checkpoint(stream, store, lsn, entry.epoch, checkpoint)
                                .await?;
                            state.generation = checkpoint.key_generation;
                        }
                        _ if state.floor != Lsn(1) => {
                            return Err(invalid("The retained floor does not name a checkpoint"));
                        }
                        _ => {}
                    }
                }
                state.apply(lsn, entry)?;
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

    pub(crate) fn apply(&mut self, lsn: Lsn, entry: Entry) -> Result<Option<Receipt>> {
        if lsn != next(self.head)? || entry.epoch < self.epoch {
            return Err(invalid("The journal records are out of order"));
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
                receipt = self.transactions.apply(lsn.0, record)?;
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
                    .apply(lsn.0, TransactionRecord::Settle { tx_lsn: *tx_lsn })?;
                self.generation
            }
            Record::Refuse { tx_lsn } => {
                receipt = self
                    .transactions
                    .apply(lsn.0, TransactionRecord::Refuse { tx_lsn: *tx_lsn })?;
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
            Record::Checkpoint(checkpoint) => {
                if checkpoint.through != self.head.0
                    || checkpoint.key_generation != entry.generation
                {
                    return Err(invalid(
                        "The checkpoint does not cover the preceding position",
                    ));
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
                *generation
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
        let (header, bytes) = image
            .contents()
            .payload
            .split_first_chunk::<24>()
            .ok_or_else(|| invalid("The checkpoint image has no basis"))?;
        let position = u64::from_be_bytes(header[..8].try_into().unwrap());
        let system_time = Timestamp(i64::from_be_bytes(header[8..16].try_into().unwrap()));
        let until = i64::from_be_bytes(header[16..].try_into().unwrap());
        if position > checkpoint.through {
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

    pub(crate) fn checkpoint_payload(&self, until: i64) -> Result<Vec<u8>> {
        let cut = self.transactions.index().head();
        let bytes = self.transactions.encode_checkpoint()?;
        let mut payload = Vec::with_capacity(24 + bytes.len());
        payload.extend_from_slice(&cut.basis.position().to_be_bytes());
        payload.extend_from_slice(&cut.system_time.0.to_be_bytes());
        payload.extend_from_slice(&until.to_be_bytes());
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

fn invalid(message: &str) -> Anomaly {
    Anomaly::fault("rtdb/multi-replay", message)
}
