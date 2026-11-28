use std::{collections::BTreeMap, sync::{Arc, Weak}};

use rtdb_types::{
    Anomaly, AnomalyCategory, Result, Timestamp, rtdb_v1::{At, Basis, Layer, MAIN_BRANCH},
};

use super::{MultiDatabase, Uuid, no_writer, store_floor::prefix_unsettled};

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
    epoch: u64,
    generation: u64,
    writer_fence_position: Option<u64>,
    owner: Arc<()>,
}

impl StoreReadTurn {
    pub fn basis(&self) -> Basis { self.basis }

    pub fn same_writer(&self, other: &Self) -> bool {
        self.basis.incarnation() == other.basis.incarnation()
            && self.epoch == other.epoch && self.generation == other.generation
            && self.writer_fence_position == other.writer_fence_position
    }
}

#[derive(Default)]
pub(super) struct StoreReads(Vec<(Basis, Weak<()>)>);

impl StoreReads {
    fn add(&mut self, turn: &StoreReadTurn) {
        self.reap();
        self.0.push((turn.basis, Arc::downgrade(&turn.owner)));
    }

    fn reap(&mut self) {
        self.0.retain(|(_, owner)| owner.strong_count() > 0);
    }

    pub(super) fn check_floor(&mut self, keep_from: u64) -> Result<()> {
        self.reap();
        if self.0.iter().any(|(basis, _)| basis.position() < keep_from) {
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
}

#[derive(Clone, Copy, Debug)]
pub enum StoreVersionHoldRequest {
    Retain { basis: Basis, expires_at: Timestamp },
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

struct Held {
    reader: [u8; 32],
    generation: u64,
    hold: StoreVersionHold,
}

#[derive(Default)]
pub(super) struct StoreHolds(BTreeMap<[u8; 16], Held>);

impl StoreHolds {
    fn expire(&mut self, lower: i64, generation: u64) {
        self.0.retain(|_, held| held.generation == generation && lower < held.hold.expires_at.0);
    }

    pub(super) fn check_floor(&mut self, keep_from: u64, lower: i64, generation: u64) -> Result<()> {
        self.expire(lower, generation);
        if self.0.values().any(|held| held.hold.key.basis.position() < keep_from) {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.store/purge-retention",
                "An admitted Store reader still retains history"));
        }
        Ok(())
    }

    fn apply(
        &mut self, request: StoreVersionHoldRequest, reader: [u8; 32], lower: i64, upper: i64, generation: u64,
    ) -> Result<StoreVersionHoldReply> {
        self.expire(lower, generation);
        match request {
            StoreVersionHoldRequest::Retain { basis, expires_at } => {
                if !(1..=MAX_HOLD_MICROS).contains(&expires_at.0.saturating_sub(upper)) {
                    return Err(Anomaly::incorrect("rtdb.store/hold-deadline",
                        "A Store hold needs a live deadline within its maximum lifetime"));
                }
                if self.0.len() >= MAX_DATABASE_HOLDS
                    || self.0.values().filter(|held| held.reader == reader).count() >= MAX_READER_HOLDS
                {
                    return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb.store/hold-quota",
                        "The Store retention quota is full"));
                }
                let id = *Uuid::new_v4().as_bytes();
                let std::collections::btree_map::Entry::Vacant(entry) = self.0.entry(id) else {
                    return Err(unavailable());
                };
                let hold = StoreVersionHold { key: StoreVersionHoldKey { basis, id }, expires_at };
                entry.insert(Held { reader, generation, hold });
                Ok(StoreVersionHoldReply::Held(hold))
            }
            StoreVersionHoldRequest::Reopen(key) => {
                let hold = self.find(key, reader)?;
                if upper >= hold.expires_at.0 {
                    return Err(unavailable());
                }
                Ok(StoreVersionHoldReply::Held(hold))
            },
            StoreVersionHoldRequest::Release(key) => {
                self.find(key, reader)?;
                self.0.remove(&key.id);
                Ok(StoreVersionHoldReply::Released(key))
            }
        }
    }

    fn find(&self, key: StoreVersionHoldKey, reader: [u8; 32]) -> Result<StoreVersionHold> {
        self.0.get(&key.id)
            .filter(|held| held.reader == reader && held.hold.key == key)
            .map(|held| held.hold)
            .ok_or_else(unavailable)
    }
}

impl MultiDatabase {
    pub async fn enter_local_store_read(&mut self, at: At) -> Result<StoreReadTurn> {
        self.refresh().await?;
        self.require_active()?;
        let writer = self.authority()?;
        let state = self.state();
        let index = self.durable_transactions().index();
        if index.has_custody_groups() || index.has_visibility_history() {
            return Err(Anomaly::unsupported("rtdb.store/unsupported",
                "The ordered-bytes store serves only an unlabelled database"));
        }
        let floor = state.store_purge_floor.max(index.store_history_floor());
        if matches!(at, At::Exactly(basis) if basis.incarnation() == self.incarnation() && basis.position() < floor) {
            return Err(Anomaly::new(AnomalyCategory::NotFound, "rtdb/cursor-expired",
                "The requested basis precedes the admitted Store floor"));
        }
        let basis = index.cut(at)?.basis;
        if basis.position() < floor {
            return Err(Anomaly::new(AnomalyCategory::NotFound, "rtdb/cursor-expired",
                "The requested basis precedes the admitted Store floor"));
        }
        let turn = StoreReadTurn {
            basis,
            epoch: writer.epoch.0,
            generation: state.generation,
            writer_fence_position: index.store_writer_fence_position(),
            owner: Arc::new(()),
        };
        self.store_reads.add(&turn);
        Ok(turn)
    }

    pub(super) async fn require_local_store_turn(&mut self, basis: Basis, turn: &StoreReadTurn) -> Result<()> {
        let writer = self.authority()?;
        let owner = Arc::downgrade(&turn.owner);
        if basis != turn.basis || writer.epoch.0 != turn.epoch || self.state().generation != turn.generation
            || self.durable_transactions().index().store_writer_fence_position() != turn.writer_fence_position
            || !self.store_reads.0.iter().any(|(at, active)| *at == basis && active.ptr_eq(&owner))
        {
            return Err(stale_read_turn());
        }
        let head = self.state().head;
        // A successor can fence the original writer before appending a
        // record. Confirm the original seal in addition to the byte floor.
        let sealed = self.stream.seal(writer.epoch).await.map_err(|error| self.mutation_failed(error))?;
        if sealed > head || !self.stream.immutable_history() && sealed != head {
            return Err(prefix_unsettled());
        }
        self.require_authority(writer)
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
        let head = state.head;
        let generation = state.generation;
        self.check_generation(generation).await?;

        // An empty successor seal fences this writer before a newer lease appears in the prefix.
        // Immutable journals return the original seal's head on an exact retry.
        let sealed = self.stream.seal(lease.epoch).await.map_err(|error| self.mutation_failed(error))?;
        if sealed > head || !self.stream.immutable_history() && sealed != head {
            return Err(prefix_unsettled());
        }
        self.require_authority(lease)?;
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
        self.writer.as_mut().ok_or_else(no_writer)?.store_holds.apply(request, reader, lower, upper, generation)
    }
}

fn unavailable() -> Anomaly {
    Anomaly::new(AnomalyCategory::NotFound, "rtdb.store/hold-unavailable",
        "The Store Version hold is unavailable")
}
