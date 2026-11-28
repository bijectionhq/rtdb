use std::{collections::BTreeMap, sync::{Arc, Weak}};

use rtdb_format::journal::{OrderedStoreHold, Record, StoreHoldChange};
use rtdb_index::branches::BranchIndex;
use rtdb_types::{
    Anomaly, AnomalyCategory, Result, Timestamp, rtdb_v1::{At, Basis, Layer, MAIN_BRANCH},
};

use super::{Lsn, MultiDatabase, Uuid, store_floor::prefix_unsettled};

const MAX_DATABASE_HOLDS: usize = 256;
const MAX_READER_HOLDS: usize = 32;
const MAX_HOLD_MICROS: i64 = 3_600_000_000;

pub(super) fn stale_read_turn() -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.store/stale-read-turn",
        "The Store read requires its exact live writer turn")
}

/// A local writer read keeps its basis through materialization and encoding.
/// The original attachment still needs its final delivery check.
pub struct StoreReadTurn {
    basis: Basis,
    retained_from: Basis,
    expires_at: Option<Timestamp>,
    epoch: u64,
    generation: u64,
    writer_fence_position: Option<u64>,
    owner: Arc<()>,
    watch: Option<StoreVersionHoldKey>,
    drain: Option<StoreWatchDrain>,
}

pub(super) struct StoreWatchDrain(Option<Box<dyn FnOnce() + Send + Sync>>);

impl StoreWatchDrain {
    pub(super) fn new(finish: impl FnOnce() + Send + Sync + 'static) -> Self {
        Self(Some(Box::new(finish)))
    }
}

impl Drop for StoreWatchDrain {
    fn drop(&mut self) {
        if let Some(finish) = self.0.take() {
            finish();
        }
    }
}

impl StoreReadTurn {
    pub fn basis(&self) -> Basis { self.basis }

    pub(super) fn settle_watch_on_drop(&mut self, drain: StoreWatchDrain) {
        self.drain = Some(drain);
    }

    pub(super) fn require_unexpired(&self, upper: i64) -> Result<()> {
        if self.expires_at.is_some_and(|expires_at| upper >= expires_at.0) {
            return Err(watch_gap());
        }
        Ok(())
    }

    pub fn same_writer(&self, other: &Self) -> bool {
        self.basis.incarnation() == other.basis.incarnation()
            && self.epoch == other.epoch && self.generation == other.generation
            && self.writer_fence_position == other.writer_fence_position
    }
}

struct InFlightStoreRead {
    retained_from: Basis,
    owner: Weak<()>,
    change_history: bool,
}

#[derive(Default)]
pub(super) struct StoreReads(Vec<InFlightStoreRead>);

impl StoreReads {
    fn add(&mut self, turn: &StoreReadTurn) {
        self.reap();
        self.0.push(InFlightStoreRead {
            retained_from: turn.retained_from,
            owner: Arc::downgrade(&turn.owner),
            change_history: turn.expires_at.is_some(),
        });
    }

    fn reap(&mut self) {
        self.0.retain(|read| read.owner.strong_count() > 0);
    }

    fn retains_changes_before(&self, through: u64) -> bool {
        self.0.iter().any(|read| read.change_history
            && read.retained_from.position() < through && read.owner.strong_count() > 0)
    }

    pub(super) fn check_floor(&mut self, keep_from: u64) -> Result<()> {
        self.reap();
        if self.0.iter().any(|read| read.retained_from.position() < keep_from) {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.store/purge-retention",
                "An active Store read still retains history"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreVersionHoldKey {
    pub basis: Basis,
    pub id: [u8; 16],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoreVersionHold {
    pub key: StoreVersionHoldKey,
    pub expires_at: Timestamp,
    pub change_history: bool,
}

#[derive(Clone, Copy, Debug)]
pub enum StoreVersionHoldRequest {
    Retain { basis: Basis, expires_at: Timestamp, change_history: bool },
    Reopen(StoreVersionHoldKey),
    Release(StoreVersionHoldKey),
}

impl StoreVersionHoldRequest {
    fn basis(self) -> Basis {
        match self {
            Self::Retain { basis, .. } => basis,
            Self::Reopen(key) | Self::Release(key) => key.basis,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StoreVersionHoldReply {
    Held(StoreVersionHold),
    Released(StoreVersionHoldKey),
}

/// The settled writer's byte promises are replayed with its journal.
#[derive(Default)]
pub(crate) struct StoreHolds(BTreeMap<[u8; 16], OrderedStoreHold>);

fn retains(hold: &OrderedStoreHold, lower: i64) -> bool {
    hold.watch_receipt.is_some() || lower < hold.expires_at_micros
}

fn valid_watch_admission(hold: &OrderedStoreHold) -> bool {
    hold.watch_receipt.is_none_or(|parent| hold.change_history && parent != [0; 16] && parent != hold.id)
}

impl StoreHolds {
    pub(crate) fn checkpoint(&self) -> Vec<OrderedStoreHold> {
        self.0.values().copied().collect()
    }

    pub(crate) fn from_checkpoint(holds: &[OrderedStoreHold], head: Basis, floor: u64) -> Result<Self> {
        if holds.len() > MAX_DATABASE_HOLDS || holds.windows(2).any(|pair| pair[0].id >= pair[1].id)
            || holds.iter().any(|hold| hold.basis.incarnation() != head.incarnation()
                || !(floor..=head.position()).contains(&hold.basis.position())
                || hold.id == [0; 16] || hold.reader == [0; 32] || hold.expires_at_micros == i64::MIN
                || !valid_watch_admission(hold))
        {
            return Err(invalid_hold("The checkpoint has an invalid Store hold inventory"));
        }
        let holds = Self(holds.iter().map(|hold| (hold.id, *hold)).collect());
        if holds.0.values().any(|hold| holds.count_for(hold.reader, i64::MIN) > MAX_READER_HOLDS) {
            return Err(invalid_hold("A checkpoint Store reader exceeds its budget"));
        }
        Ok(holds)
    }

    pub(crate) fn expire(&mut self, lower: i64) {
        self.0.retain(|_, hold| retains(hold, lower));
    }

    pub(crate) fn check_floor(&self, keep_from: u64, lower: i64) -> Result<()> {
        if self.0.values().any(|hold| retains(hold, lower) && hold.basis.position() < keep_from) {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.store/purge-retention",
                "An admitted Store reader still retains history"));
        }
        Ok(())
    }

    fn retains_changes_before(&self, through: u64, lower: i64) -> bool {
        self.0.values().any(|hold| hold.change_history
            && hold.basis.position() < through && retains(hold, lower))
    }

    fn count_for(&self, reader: [u8; 32], lower: i64) -> usize {
        self.0.values().filter(|hold| hold.reader == reader && retains(hold, lower)).count()
    }

    fn check_new(&self, hold: OrderedStoreHold, lower: i64) -> Result<()> {
        if hold.id == [0; 16] || hold.reader == [0; 32] || self.0.contains_key(&hold.id) || hold.expires_at_micros <= lower
            || !valid_watch_admission(&hold)
        {
            return Err(invalid_hold("A Store hold must be new, live and bound to its reader"));
        }
        if let Some(parent) = hold.watch_receipt {
            let key = StoreVersionHoldKey { basis: hold.basis, id: parent };
            let receipt = self.find(key, hold.reader, lower)?;
            if !receipt.change_history || receipt.expires_at_micros != hold.expires_at_micros {
                return Err(invalid_hold("A Store watch turn must match its exact live receipt"));
            }
        }
        if self.0.values().filter(|hold| retains(hold, lower)).count() >= MAX_DATABASE_HOLDS
            || self.count_for(hold.reader, lower) >= MAX_READER_HOLDS
        {
            return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb.store/hold-quota",
                "The Store retention quota is full"));
        }
        Ok(())
    }

    pub(crate) fn apply(&mut self, change: StoreHoldChange, lower: i64) -> Result<()> {
        match change {
            StoreHoldChange::Retain(hold) => {
                self.check_new(hold, lower)?;
                self.0.insert(hold.id, hold);
            },
            StoreHoldChange::Release(hold) => {
                if self.0.get(&hold.id) != Some(&hold) || !retains(&hold, lower) {
                    return Err(invalid_hold("A Store hold release must name its exact ordered admission"));
                }
                self.0.remove(&hold.id);
            },
        }
        self.expire(lower);
        Ok(())
    }

    fn plan(
        &self, request: StoreVersionHoldRequest, reader: [u8; 32], lower: i64, upper: i64,
    ) -> Result<(StoreVersionHoldReply, Option<StoreHoldChange>)> {
        match request {
            StoreVersionHoldRequest::Retain { basis, expires_at, change_history } => {
                if !(1..=MAX_HOLD_MICROS).contains(&expires_at.0.saturating_sub(upper)) {
                    return Err(Anomaly::incorrect("rtdb.store/hold-deadline",
                        "A Store hold needs a live deadline within its maximum lifetime"));
                }
                let id = *Uuid::new_v4().as_bytes();
                let ordered = OrderedStoreHold {
                    id, reader, basis, expires_at_micros: expires_at.0, change_history, watch_receipt: None,
                };
                self.check_new(ordered, lower)?;
                let held = StoreVersionHold { key: StoreVersionHoldKey { basis, id }, expires_at, change_history };
                Ok((StoreVersionHoldReply::Held(held), Some(StoreHoldChange::Retain(ordered))))
            },
            StoreVersionHoldRequest::Reopen(key) => {
                let hold = self.find(key, reader, upper)?;
                Ok((StoreVersionHoldReply::Held(StoreVersionHold {
                    key, expires_at: Timestamp(hold.expires_at_micros), change_history: hold.change_history,
                }), None))
            },
            StoreVersionHoldRequest::Release(key) => {
                let hold = self.find(key, reader, lower)?;
                Ok((StoreVersionHoldReply::Released(key), Some(StoreHoldChange::Release(hold))))
            },
        }
    }

    fn in_flight(&self, key: StoreVersionHoldKey) -> Option<OrderedStoreHold> {
        self.0.get(&key.id).filter(|hold| hold.watch_receipt.is_some() && hold.basis == key.basis).copied()
    }

    fn find(&self, key: StoreVersionHoldKey, reader: [u8; 32], now: i64) -> Result<OrderedStoreHold> {
        self.0.get(&key.id)
            .filter(|hold| hold.watch_receipt.is_none() && hold.reader == reader && hold.basis == key.basis && now < hold.expires_at_micros)
            .copied().ok_or_else(unavailable)
    }
}

fn invalid_hold(message: &str) -> Anomaly {
    Anomaly::fault("rtdb.store/hold-record", message)
}

fn watch_gap() -> Anomaly {
    Anomaly::new(AnomalyCategory::NotFound, "rtdb/cursor-expired",
        "The exact Store watch has no current retention receipt")
}

enum StoreReadPoint {
    Read(At),
    Watch(Basis),
}

fn require_plain_store(index: &BranchIndex) -> Result<()> {
    if index.has_custody_groups() || index.has_visibility_history() {
        return Err(Anomaly::unsupported("rtdb.store/unsupported",
            "The ordered-bytes store serves only an unlabelled database"));
    }
    Ok(())
}

impl MultiDatabase {
    pub async fn enter_local_store_read(&mut self, at: At) -> Result<StoreReadTurn> {
        self.refresh().await?;
        self.admit_store_turn(StoreReadPoint::Read(at), None)
    }

    pub async fn enter_local_store_watch(&mut self, after: Basis) -> Result<StoreReadTurn> {
        self.refresh().await?;
        self.admit_store_turn(StoreReadPoint::Watch(after), None)
    }

    /// The receipt lookup and the in-flight byte turn share the writer lock
    /// with hold release and floor publication.
    pub(super) async fn enter_held_store_watch(
        &mut self, after: Basis, reader: [u8; 32], hold: StoreVersionHoldKey, id: [u8; 16],
    ) -> Result<StoreReadTurn> {
        if hold.basis.incarnation() != after.incarnation() || hold.basis.position() > after.position() {
            return Err(watch_gap());
        }
        self.stream.require_serving().await?;
        self.refresh().await?;
        let lease = self.settled_store_writer()?;
        if self.state().transactions.pending_erasure().is_some() {
            return Err(prefix_unsettled());
        }
        let retained = self.state().store_holds.find(hold, reader, self.upper()).map_err(|_| watch_gap())?;
        if !retained.change_history || hold.basis.position() < self.state().retained_changes_after()? {
            return Err(watch_gap());
        }
        let mut turn = self.admit_store_turn(StoreReadPoint::Watch(after), Some(retained))?;
        let lower = self.lower();
        let use_record = OrderedStoreHold { id, watch_receipt: Some(hold.id), ..retained };
        self.state().store_holds.check_new(use_record, lower)?;
        self.append_control(lease, Record::StoreHold {
            change: StoreHoldChange::Retain(use_record), lower_bound_micros: lower,
        }).await?;
        turn.require_unexpired(self.upper())?;
        turn.watch = Some(StoreVersionHoldKey { basis: hold.basis, id });
        Ok(turn)
    }

    pub(super) async fn finish_held_store_watch(&mut self, key: StoreVersionHoldKey, reader: [u8; 32]) -> Result<()> {
        self.refresh().await?;
        let Some(hold) = self.state().store_holds.in_flight(key) else { return Ok(()); };
        if hold.reader != reader {
            return Err(stale_read_turn());
        }
        let lease = self.settled_store_writer()?;
        if self.state().transactions.pending_erasure().is_some() {
            return Err(prefix_unsettled());
        }
        let lower = self.lower();
        self.append_control(lease, Record::StoreHold {
            change: StoreHoldChange::Release(hold), lower_bound_micros: lower,
        }).await
    }

    fn admit_store_turn(&mut self, point: StoreReadPoint, retained: Option<OrderedStoreHold>) -> Result<StoreReadTurn> {
        self.require_active()?;
        let writer = self.authority()?;
        let state = self.state();
        let index = self.durable_transactions().index();
        require_plain_store(index)?;
        let floor = state.store_purge_floor.max(index.store_history_floor());
        let expired = || Anomaly::new(AnomalyCategory::NotFound, "rtdb/cursor-expired",
            "The requested basis precedes the admitted Store floor");
        let basis = match point {
            StoreReadPoint::Read(at) => {
                if matches!(at, At::Exactly(basis) if basis.incarnation() == self.incarnation() && basis.position() < floor) {
                    return Err(expired());
                }
                let basis = index.cut(at)?.basis;
                if basis.position() < floor {
                    return Err(expired());
                }
                basis
            }
            StoreReadPoint::Watch(after) => {
                if after.incarnation() != index.head().basis.incarnation() {
                    return Err(Anomaly::incorrect("rtdb/foreign-basis", "The watch cursor belongs to another database incarnation"));
                }
                if after.position() < floor.max(state.retained_changes_after()?) {
                    return Err(expired());
                }
                // A completed page can end on a control record. A watch
                // selects an inspected prefix, while a read selects a cut.
                if after.position() > state.coordinates.position(state.head)? {
                    return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb/change-basis-ahead",
                        "The requested basis has not applied"));
                }
                after
            }
        };
        let retained_from = retained.map_or(basis, |hold| hold.basis);
        if retained_from.position() < floor {
            return Err(watch_gap());
        }
        let turn = StoreReadTurn {
            basis,
            retained_from,
            expires_at: retained.map(|hold| Timestamp(hold.expires_at_micros)),
            epoch: writer.epoch.0,
            generation: state.generation,
            writer_fence_position: index.store_writer_fence_position(),
            owner: Arc::new(()),
            watch: None,
            drain: None,
        };
        self.store_reads.add(&turn);
        Ok(turn)
    }

    pub(super) async fn require_local_store_turn(&mut self, basis: Basis, turn: &StoreReadTurn) -> Result<()> {
        let writer = self.authority()?;
        let owner = Arc::downgrade(&turn.owner);
        let index = self.durable_transactions().index();
        require_plain_store(index)?;
        if basis != turn.basis || writer.epoch.0 != turn.epoch || self.state().generation != turn.generation
            || index.store_writer_fence_position() != turn.writer_fence_position
            || turn.watch.is_some_and(|key| self.state().store_holds.in_flight(key).is_none())
            || !self.store_reads.0.iter().any(|read| read.retained_from == turn.retained_from && read.owner.ptr_eq(&owner))
        {
            return Err(stale_read_turn());
        }
        let head = self.state().head;
        self.require_store_writer_prefix(writer, head).await
    }

    pub(super) fn preserves_store_watch_history(&self, through: Lsn) -> Result<bool> {
        let state = self.state();
        let through = state.changes_lost_through(through)?;
        Ok(!state.store_holds.retains_changes_before(through, self.lower())
            && !self.store_reads.retains_changes_before(through))
    }

    pub(super) async fn store_version_hold(
        &mut self, reader: [u8; 32], request: StoreVersionHoldRequest,
    ) -> Result<StoreVersionHoldReply> {
        let basis = request.basis();
        if basis.incarnation() != self.incarnation() {
            return Err(Anomaly::incorrect("rtdb/foreign-basis", "The basis belongs to another database incarnation"));
        }
        self.stream.require_serving().await?;
        self.refresh().await?;
        let lease = self.settled_store_writer()?;
        let state = self.state();
        if state.transactions.pending_erasure().is_some() {
            return Err(prefix_unsettled());
        }
        let head = state.head;
        let generation = state.generation;
        self.check_generation(generation).await?;

        self.require_store_writer_prefix(lease, head).await?;
        let upper = self.upper();
        let lower = self.lower();

        if !matches!(request, StoreVersionHoldRequest::Release(_)) {
            let state = self.state();
            let index = state.transactions.index();
            if index.has_custody_groups() || index.has_visibility_history() {
                return Err(Anomaly::unsupported("rtdb.store/unsupported",
                    "The ordered-bytes store serves only an unlabelled database"));
            }
            if basis.position() < state.store_purge_floor.max(index.store_history_floor()) {
                return Err(Anomaly::new(AnomalyCategory::NotFound, "rtdb/cursor-expired",
                    "The requested basis precedes the admitted Store floor"));
            }
            index.view(MAIN_BRANCH, At::Exactly(basis), Layer::Merged, Timestamp(upper))?;
        }
        let (reply, change) = self.state().store_holds.plan(request, reader, lower, upper)?;
        if matches!(reply, StoreVersionHoldReply::Held(hold) if hold.change_history)
            && basis.position() < self.state().retained_changes_after()?
        {
            return Err(watch_gap());
        }
        if let Some(change) = change {
            self.append_control(lease, Record::StoreHold { change, lower_bound_micros: lower }).await?;
        }
        if matches!(reply, StoreVersionHoldReply::Held(hold) if self.upper() >= hold.expires_at.0) {
            return Err(unavailable());
        }
        Ok(reply)
    }
}

fn unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::NotFound, "rtdb.store/hold-unavailable",
        "The Store Version hold is unavailable")
}
