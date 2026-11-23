use rtdb_types::{
    Anomaly, EntityId, OwnedRelation, PlacedAttemptScope, Result, StructFields, TableRef,
    TimeUnit, Timestamp, Value,
};

mod replay_worlds;
mod world_readmission;
pub(crate) use replay_worlds::ReplayWorlds;
pub(crate) use world_readmission::{WorldReadmission, world_readmission_control};

pub(crate) const CONTROL_TABLES: &str = "hosted_control_tables";
pub(crate) const HOSTED_CAPACITY_DECLARATIONS: &str = "hosted_capacity_declarations";
const RESTORE: &str = "bj_rtdb_restore_hold";
const NATIVE_FRONTIER: &str = "native-frontier.v1";
const REPLAY_TRANSACTIONS: &str = "bj_rtdb_restore_transactions";
const MAX_REPLAY_POSITIONS: usize = 4096;
const MAX_REPLAY_RECEIPT_BYTES: usize = 64 * 1024;
const MAX_REPLAY_LIFETIMES: usize = 32;
const MAX_REPLAY_LIFETIME_BYTES: usize = 16 * 1024;
const MAX_REPLAY_EXPIRIES: usize = 64;
const MAX_REPLAY_EXPIRY_BYTES: usize = 32 * 1024;
const MAX_REPLAY_CONSTRAINTS: usize = 64;
// A catalog commit can declare rules across all of its Root relations.
const MAX_REPLAY_CONSTRAINT_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReplayConstraintKind {
    Unique,
    Capacity,
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayConstraint {
    relation: String,
    kind: ReplayConstraintKind,
    writes: i64,
    through: i64,
    members: [u8; 32],
    capacity_declarations: Vec<ReplayCapacityDeclaration>,
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplayCapacityDeclaration {
    pub(crate) column: String,
    pub(crate) when: Vec<u8>,
    pub(crate) cap: i64,
}


#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct OriginalHandoff {
    handoff: [u8; 32],
    fence: rtdb_types::TransactionKey,
    through: rtdb_types::TransactionKey,
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostedReplay {
    binding: [u8; 32],
    archive: [u8; 32],
    original_authority: [u8; 16],
    captured: rtdb_types::TransactionKey,
    handoff: Option<OriginalHandoff>,
    through: u64,
    previous: Option<rtdb_types::TransactionKey>,
    source: rtdb_types::TransactionKey,
    logical_at: u64,
    fingerprint: [u8; 32],
    commit_positions: Vec<u32>,
    #[serde(skip)]
    lifetimes: Vec<ReplayLifetime>,
    #[serde(skip)]
    expiries: Vec<ReplayExpiry>,
    #[serde(skip)]
    constraints: Vec<ReplayConstraint>,
    #[serde(skip)]
    worlds: Vec<u8>,
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplayLifetime {
    pub(crate) relation: String,
    duration: i64,
    expired_created_through: i64,
    swept_present_through: i64,
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayExpiry {
    relation: String,
    identity: Vec<u8>,
    created_micros: i64,
    expiry_micros: i64,
}

impl ReplayExpiry {
    fn interval(&self) -> Result<rtdb_types::Interval> {
        rtdb_types::expired_lifetime_interval(Timestamp(self.created_micros), Timestamp(self.expiry_micros))
            .map_err(|_| replay_invalid())
    }
}

impl ReplayLifetime {
    pub(crate) fn floor(&self) -> rtdb_types::OwnedLifetimeFloor {
        rtdb_types::OwnedLifetimeFloor {
            duration: self.duration,
            expired_created_through: Timestamp(self.expired_created_through),
            swept_present_through: Timestamp(self.swept_present_through),
        }
    }
}

/// The only admitted hosted write shape for the held native restore ledger.
/// The resolver checks each observation and its contents in the ordered base.
pub fn hosted_replay_controls<'a>(
    controls: impl Iterator<Item = &'a HostedControlWrite> + Clone,
) -> Result<Option<HostedReplay>> {
    if !controls.clone().any(|control| control.name() == RESTORE && !control.is_check()) {
        return Ok(None);
    }
    if world_readmission_control(controls.clone())?.is_some() {
        return Ok(None);
    }
    let selected = |relation: &str, id: &str| controls.clone().find(|control| {
        control.name() == relation && matches!(control.id(), EntityId::String(key) if key == id)
    });
    let check = |relation: &str, id: &str, exists: bool| {
        selected(relation, id).is_some_and(|control| control.is_check() && control.expected().is_some() == exists)
    };
    let head = selected(RESTORE, NATIVE_FRONTIER).ok_or_else(replay_invalid)?;
    let encoded = replay_body(head).ok_or_else(replay_invalid)?;
    let mut replay: HostedReplay = serde_json::from_slice(encoded).map_err(|_| replay_invalid())?;
    let source = replay.source;
    let receipt_id = format!("native-transaction/{}", source.tx_id.0);
    let receipt = selected(RESTORE, &receipt_id).ok_or_else(replay_invalid)?;
    let coordinates = selected(REPLAY_TRANSACTIONS, &receipt_id).ok_or_else(replay_invalid)?;
    let lifetime_id = format!("native-lifetimes/{}", source.tx_id.0);
    let lifetimes = selected(RESTORE, &lifetime_id);
    let constraint_id = format!("native-constraints/{}", source.tx_id.0);
    let constraints = selected(RESTORE, &constraint_id);
    let expiry_id = format!("native-expiries/{}", source.tx_id.0);
    let expiries = selected(RESTORE, &expiry_id);
    let prefix = format!("native-worlds/{}/", source.tx_id.0);
    let mut chunks = std::collections::BTreeMap::new();
    let mut bytes = 0usize;
    for control in controls.clone().filter(|control| control.name() == RESTORE) {
        let EntityId::String(id) = control.id() else { return Err(replay_invalid()); };
        let Some(index) = id.strip_prefix(&prefix) else { continue; };
        let number = index.parse::<usize>().map_err(|_| replay_invalid())?;
        let body = control_body(control, replay_worlds::CHUNK_BYTES)
            .filter(|body| control.expected().is_none() && !body.is_empty())
            .ok_or_else(replay_invalid)?;
        bytes = bytes.checked_add(body.len()).filter(|&size| size <= replay_worlds::MAX_BYTES)
            .ok_or_else(replay_invalid)?;
        if number.to_string() != index || chunks.insert(number, body).is_some() {
            return Err(replay_invalid());
        }
    }
    if controls.clone().filter(|control| control.name() == RESTORE).count()
        != 5 + usize::from(lifetimes.is_some()) + usize::from(constraints.is_some()) + usize::from(expiries.is_some()) + chunks.len()
        || controls.clone().filter(|control| control.name() == "bj_installation").count() != 1
        || controls.clone().filter(|control| control.name() == REPLAY_TRANSACTIONS).count() != 1
        || !check("bj_installation", "authority", true)
        || !check(RESTORE, "restore", true) || !check(RESTORE, "archive", true)
        || !check(RESTORE, "replayed", false)
        || head.expected().is_some() != replay.previous.is_some()
        || receipt.expected().is_some() || replay_body(receipt) != Some(encoded)
        || !replay.has_original_prefix()
        || !replay.positions_are_ordered()
        || replay.logical_at == 0 || replay.logical_at % 1_000 != 0
        || u64::try_from(source.system_time.0).ok().and_then(|time| time.checked_mul(1_000))
            .is_none_or(|time| time < replay.logical_at || time > replay.through)
        || replay.previous.is_some_and(|previous| previous.tx_id.0 < 0 || previous.tx_id >= source.tx_id
            || previous.system_time.0 <= 0 || previous.system_time >= source.system_time)
        || !replay.matches_coordinates(coordinates)
    {
        return Err(replay_invalid());
    }
    if let Some(lifetimes) = lifetimes {
        let encoded = control_body(lifetimes, MAX_REPLAY_LIFETIME_BYTES)
            .filter(|_| lifetimes.expected().is_none()).ok_or_else(replay_invalid)?;
        replay.lifetimes = serde_json::from_slice(encoded).map_err(|_| replay_invalid())?;
        if !(1..=MAX_REPLAY_LIFETIMES).contains(&replay.lifetimes.len())
            || !replay.lifetimes.windows(2).all(|pair| pair[0].relation < pair[1].relation)
        {
            return Err(replay_invalid());
        }
        for lifetime in &replay.lifetimes {
            let relation = OwnedRelation::new(lifetime.relation.as_str()).map_err(|_| replay_invalid())?;
            if !relation.is_root_data()
                || lifetime.floor().check(lifetime.duration, source.system_time).is_err()
            {
                return Err(replay_invalid());
            }
        }
    }
    if let Some(expiries) = expiries {
        let encoded = control_body(expiries, MAX_REPLAY_EXPIRY_BYTES)
            .filter(|_| expiries.expected().is_none()).ok_or_else(replay_invalid)?;
        replay.expiries = serde_json::from_slice(encoded).map_err(|_| replay_invalid())?;
        if !(1..=MAX_REPLAY_EXPIRIES).contains(&replay.expiries.len())
            || !replay.expiries.windows(2).all(|pair|
                (&pair[0].relation, &pair[0].identity) < (&pair[1].relation, &pair[1].identity))
        {
            return Err(replay_invalid());
        }
        for original in &replay.expiries {
            let relation = OwnedRelation::new(original.relation.as_str()).map_err(|_| replay_invalid())?;
            let identity = EntityId::from_handle_bytes(&original.identity).map_err(|_| replay_invalid())?;
            let floor = replay.lifetime(&relation).ok_or_else(replay_invalid)?;
            let interval = original.interval()?;
            if !relation.is_root_data() || identity.handle_bytes() != original.identity
                || interval.lower().0.checked_add(floor.duration) != Some(interval.upper().0)
                || interval.upper() > floor.swept_present_through
                || interval.upper() > rtdb_types::OwnedLifetimeFloor::expiry_replay_cutoff(source.system_time)
            {
                return Err(replay_invalid());
            }
        }
    }
    if let Some(constraints) = constraints {
        let encoded = control_body(constraints, MAX_REPLAY_CONSTRAINT_BYTES)
            .filter(|_| constraints.expected().is_none()).ok_or_else(replay_invalid)?;
        replay.constraints = serde_json::from_slice(encoded).map_err(|_| replay_invalid())?;
        if !(1..=MAX_REPLAY_CONSTRAINTS).contains(&replay.constraints.len())
            || !replay.constraints.windows(2).all(|pair| {
                (&pair[0].relation, pair[0].kind) < (&pair[1].relation, pair[1].kind)
            })
        {
            return Err(replay_invalid());
        }
        for proof in &replay.constraints {
            let relation = OwnedRelation::new(proof.relation.as_str()).map_err(|_| replay_invalid())?;
            let declarations = &proof.capacity_declarations;
            if !relation.is_root_data() || proof.writes < 0 || proof.through < proof.writes
                || (proof.writes == 0 && declarations.is_empty())
                || (!declarations.is_empty() && proof.kind != ReplayConstraintKind::Capacity)
                || declarations.len() > 16
                || !declarations.windows(2).all(|pair|
                    (&pair[0].column, &pair[0].when) < (&pair[1].column, &pair[1].when))
            {
                return Err(replay_invalid());
            }
        }
    }
    let count = chunks.len();
    for (index, (number, body)) in chunks.into_iter().enumerate() {
        if number != index || (index + 1 != count && body.len() != replay_worlds::CHUNK_BYTES) {
            return Err(replay_invalid());
        }
        replay.worlds.extend_from_slice(body);
    }
    Ok(Some(replay))
}

fn replay_constraint_mismatch() -> Anomaly {
    Anomaly::incorrect("rtdb.hosted/constraint-proof",
        "The held transaction does not reproduce the Original constraint effects")
}

fn original_prefix(event: rtdb_types::TransactionKey, captured: rtdb_types::TransactionKey) -> bool {
    event.tx_id.0 >= 0 && event.system_time.0 > 0
        && (event == captured
            || (event.tx_id < captured.tx_id && event.system_time < captured.system_time))
}

/// Recognize an exact archived transaction through its accepted held receipt.
pub fn replayed_parent_logical_time(
    body: &[u8], binding: [u8; 32], archive_pin: &[u8], original: rtdb_types::TransactionKey,
) -> Result<Timestamp> {
    let replay = decode_accepted_replay(body, binding, archive_pin)?;
    if replay.source != original {
        return Err(replay_invalid());
    }
    Ok(replay.logical_time())
}

pub fn replayed_archive_cut(body: &[u8], binding: [u8; 32], archive_pin: &[u8]) -> Result<Timestamp> {
    Ok(decode_accepted_replay(body, binding, archive_pin)?.captured.system_time)
}

fn decode_accepted_replay(body: &[u8], binding: [u8; 32], archive_pin: &[u8]) -> Result<HostedReplay> {
    if body.len() > MAX_REPLAY_RECEIPT_BYTES { return Err(replay_invalid()); }
    let replay: HostedReplay = serde_json::from_slice(body).map_err(|_| replay_invalid())?;
    let archive_matches = archive_pin.split_first_chunk::<32>().is_some_and(|(archive, cut)| {
        archive == &replay.archive && cut == replay.through.to_be_bytes().as_slice()
    });
    if replay.binding != binding || !archive_matches || !replay.has_original_prefix()
        || !replay.positions_are_ordered()
        || replay.logical_at == 0 || replay.logical_at % 1_000 != 0
        || replay.logical_time() > replay.source.system_time
    {
        return Err(replay_invalid());
    }
    Ok(replay)
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReplayedWorldParent {
    pub(crate) binding: [u8; 32],
    pub(crate) world: [u8; 16],
    pub(crate) pin: [u8; 16],
    pub(crate) original: rtdb_types::TransactionKey,
    pub(crate) destination: Timestamp,
}

impl ReplayedWorldParent {
    pub(crate) fn locator(world: rtdb_types::Iid, pin: rtdb_types::Iid) -> String {
        format!("native-world-parent/{world}/{pin}")
    }
}

pub fn replayed_world_parent_locator(world: &rtdb_types::OwnedWorld) -> String {
    ReplayedWorldParent::locator(world.id, world.pin)
}

pub fn replayed_world_parent(
    body: &[u8], binding: [u8; 32], world: &rtdb_types::OwnedWorld,
) -> Result<Timestamp> {
    if body.len() > 4096 { return Err(replay_invalid()); }
    let parent: ReplayedWorldParent = serde_json::from_slice(body).map_err(|_| replay_invalid())?;
    if parent.binding != binding || parent.world != world.id.0 || parent.pin != world.pin.0
        || parent.original != world.basis || parent.destination <= world.basis.system_time
    {
        return Err(replay_invalid());
    }
    Ok(parent.destination)
}

fn replay_body(control: &HostedControlWrite) -> Option<&[u8]> {
    control_body(control, MAX_REPLAY_RECEIPT_BYTES)
}

fn control_body(control: &HostedControlWrite, max_bytes: usize) -> Option<&[u8]> {
    let document = control.replacement()?;
    let Value::Binary(body) = document.get("binding")? else { return None; };
    (document.len() == 2 && body.len() <= max_bytes).then_some(body.as_slice())
}

impl HostedReplay {
    pub(crate) fn commit_positions(&self) -> &[u32] { &self.commit_positions }

    fn positions_are_ordered(&self) -> bool {
        self.commit_positions.len() <= MAX_REPLAY_POSITIONS
            && self.commit_positions.first().is_none_or(|first| *first != 0)
            && self.commit_positions.windows(2).all(|pair| pair[0] < pair[1])
    }

    fn has_original_prefix(&self) -> bool {
        self.original_authority != [0; 16]
            && u64::try_from(self.captured.system_time.0).ok()
                .and_then(|micros| micros.checked_mul(1_000)) == Some(self.through)
            && original_prefix(self.source, self.captured)
            && self.handoff.as_ref().is_none_or(|seal| {
                seal.handoff != [0; 32]
                    && original_prefix(seal.through, self.captured)
                    && original_prefix(seal.fence, seal.through)
            })
    }

    fn matches_coordinates(&self, control: &HostedControlWrite) -> bool {
        let Some(document) = control.replacement() else { return false; };
        let matches_bytes = |name: &str, bytes: &[u8]| {
            matches!(document.get(name), Some(Value::Binary(value)) if value.as_slice() == bytes)
        };
        let matches_time = |name: &str, micros: i64| {
            matches!(document.get(name), Some(Value::Timestamp {
                value, unit: TimeUnit::Microsecond, timezone: Some(_),
            }) if *value == micros)
        };
        control.expected().is_none()
            && document.len() == 6
            && matches_bytes("binding", &self.binding)
            && matches_bytes("digest", &self.fingerprint)
            && matches_time("source_at", self.source.system_time.0)
            && matches_time("logical_at", (self.logical_at / 1_000) as i64)
            && matches_time("source_cut", (self.through / 1_000) as i64)
    }

    pub(crate) fn check_order(&self, ordered: Timestamp) -> Result<()> {
        if u64::try_from(ordered.0).ok().and_then(|time| time.checked_mul(1_000)).is_none_or(|time| time <= self.through) {
            return Err(replay_invalid());
        }
        Ok(())
    }

    pub(crate) fn requires_base_payload(&self, control: &HostedControlWrite) -> bool {
        control.name() == RESTORE && match control.id() {
            EntityId::String(id) => matches!(id.as_str(), "restore" | "archive")
                || (id == NATIVE_FRONTIER && self.previous.is_some()),
            _ => false,
        }
    }

    fn same_original_capture(&self, other: &Self) -> bool {
        other.binding == self.binding && other.archive == self.archive && other.through == self.through
            && other.original_authority == self.original_authority
            && other.captured == self.captured && other.handoff == self.handoff
    }

    pub(crate) fn matches_parent_receipt(&self, body: &[u8], parent: rtdb_types::TransactionKey) -> bool {
        body.len() <= 4096 && serde_json::from_slice::<Self>(body).is_ok_and(|candidate| {
            self.same_original_capture(&candidate) && candidate.has_original_prefix()
                && candidate.source == parent && original_prefix(parent, self.source)
        })
    }

    pub(crate) fn holds_binding(&self) -> [u8; 32] { self.binding }

    pub(crate) fn relocation_locator(&self, world: rtdb_types::Iid) -> String {
        format!("native-world-relocated/{}/{world}", self.source.tx_id.0)
    }

    pub(crate) fn check_base(&self, control: &HostedControlWrite, row: Option<&arrow_array::RecordBatch>) -> Result<()> {
        use arrow_array::{Array, BinaryArray};
        if control.name() != RESTORE { return Ok(()); }
        let EntityId::String(id) = control.id() else { return Err(replay_invalid()); };
        let binding = || -> Result<&[u8]> {
            let row = row.filter(|row| row.num_rows() == 1).ok_or_else(replay_invalid)?;
            let bytes = row.column_by_name("binding").and_then(|array| array.as_any().downcast_ref::<BinaryArray>())
                .filter(|bytes| bytes.is_valid(0)).ok_or_else(replay_invalid)?;
            Ok(bytes.value(0))
        };
        let matches = match id.as_str() {
            "restore" => binding()? == self.binding.as_slice(),
            "archive" => binding()?.split_first_chunk::<32>().is_some_and(|(archive, cut)| {
                archive == &self.archive && cut == self.through.to_be_bytes().as_slice()
            }),
            NATIVE_FRONTIER if self.previous.is_some() => {
                let bytes = binding()?;
                if bytes.len() > 4096 { return Err(replay_invalid()); }
                let previous: Self = serde_json::from_slice(bytes).map_err(|_| replay_invalid())?;
                self.same_original_capture(&previous) && previous.has_original_prefix()
                    && Some(previous.source) == self.previous && previous.logical_at < self.logical_at
            }
            _ => true,
        };
        if !matches { return Err(replay_invalid()); }
        Ok(())
    }

    pub(crate) fn capacity_declarations(&self) -> impl Iterator<Item = (&str, &ReplayCapacityDeclaration)> {
        self.constraints.iter().flat_map(|proof| {
            proof.capacity_declarations.iter().map(move |rule| (proof.relation.as_str(), rule))
        })
    }

    pub(crate) fn match_constraint(
        &mut self, relation: &OwnedRelation, kind: ReplayConstraintKind, writes: i64, through: i64,
        members: [u8; 32], declarations: usize,
    ) -> Result<()> {
        let index = self.constraints.binary_search_by(|proof| {
            (proof.relation.as_str(), proof.kind).cmp(&(relation.name(), kind))
        }).map_err(|_| replay_constraint_mismatch())?;
        let proof = self.constraints.remove(index);
        if proof.writes != writes || proof.through != through || proof.members != members
            || proof.capacity_declarations.len() != declarations
        {
            return Err(replay_constraint_mismatch());
        }
        Ok(())
    }

    pub(crate) fn require_constraints_matched(&self) -> Result<()> {
        if !self.constraints.is_empty() {
            return Err(replay_constraint_mismatch());
        }
        Ok(())
    }


    pub(crate) fn logical_time(&self) -> Timestamp { Timestamp((self.logical_at / 1_000) as i64) }

    pub(crate) fn world_body(&self) -> &[u8] { &self.worlds }

    /// The co-ordered replay receipt commits to the Original inventory.
    /// Bulk clear world input stays in its destination relations.
    pub(crate) fn is_transient_world_control(&self, control: &HostedControlWrite) -> bool {
        !self.worlds.is_empty() && control.name() == RESTORE
            && matches!(control.id(), EntityId::String(id) if id.starts_with("native-worlds/"))
    }

    pub(crate) fn take_expiry(
        &mut self, relation: &OwnedRelation, id: &EntityId,
    ) -> Result<rtdb_types::Interval> {
        let handle = id.handle_bytes();
        let index = self.expiries.binary_search_by(|proof| {
            (proof.relation.as_str(), proof.identity.as_slice()).cmp(&(relation.name(), handle.as_slice()))
        }).map_err(|_| replay_invalid())?;
        self.expiries.remove(index).interval()
    }

    pub(crate) fn require_expiries_matched(&self) -> Result<()> {
        if !self.expiries.is_empty() { return Err(replay_invalid()); }
        Ok(())
    }

    pub(crate) fn source_time(&self) -> Timestamp { self.source.system_time }

    pub(crate) fn lifetimes(&self) -> &[ReplayLifetime] { &self.lifetimes }

    pub(crate) fn lifetime(&self, relation: &OwnedRelation) -> Option<rtdb_types::OwnedLifetimeFloor> {
        self.lifetimes.iter().find(|entry| entry.relation == relation.name()).map(ReplayLifetime::floor)
    }
}

fn replay_invalid() -> Anomaly {
    Anomaly::new(rtdb_types::AnomalyCategory::Conflict, "rtdb.hosted/restore", "Native replay requires its exact original transaction and held archive controls")
}

pub(crate) fn admits_name(name: &str) -> bool {
    name.starts_with("bj_") && !name.contains('-') && OwnedRelation::new(name).is_ok()
}

pub(crate) fn hosted_root_world_reads(operations: &[crate::source::Operation]) -> bool {
    use crate::{TxOp, source::Operation};

    let mut scopes = 0;
    let mut guards = 0;
    let mut clocks = 0;
    let mut hosted = false;
    let mut preceding_guard = None;
    for operation in operations {
        let guard = match operation {
            Operation::Public(TxOp::OwnedCheckWorldPurge { relation, .. })
                if relation.world().is_some() =>
            {
                guards += 1;
                Some(relation)
            },
            Operation::Public(TxOp::OwnedCheckWorldScope { clock, .. }) => {
                if clock.is_some() {
                    clocks += 1;
                } else {
                    scopes += 1;
                }
                None
            },
            Operation::Public(TxOp::HostedControl(_)) => {
                hosted = true;
                None
            },
            Operation::Public(TxOp::OwnedDeadline { .. }) => None,
            Operation::Public(TxOp::OwnedCheck { relation, .. }) if relation.world().is_some() => {
                if preceding_guard != Some(relation) {
                    return false;
                }
                None
            },
            Operation::Public(TxOp::OwnedCheck { relation, .. } | TxOp::OwnedCheckRange { relation, .. }
                | TxOp::OwnedCheckCapacity { relation, .. } | TxOp::OwnedInsertDocs { relation, .. }
                | TxOp::OwnedReplaceDocs { relation, .. } | TxOp::OwnedArrowDocs { relation, .. } | TxOp::OwnedDelete { relation, .. })
            | Operation::OwnedWriteIids { relation, .. } | Operation::OwnedArrowIids { relation, .. }
                if relation.world().is_none() && !relation.is_world_catalog() => None,
            _ => return false,
        };
        preceding_guard = guard;
    }
    hosted && scopes == 1 && clocks <= 8 && (guards != 0 || clocks != 0)
}

/// A retained drop can settle a bounded set of existing owner rows.
pub fn retained_release_controls<'a>(
    controls: impl IntoIterator<Item = &'a HostedControlWrite>,
) -> bool {
    let mut checks = 0;
    let mut replacements = 0;
    for control in controls {
        if control.is_check() {
            checks += 1;
        } else if control.expected().is_some() {
            replacements += 1;
        } else {
            return false;
        }
    }
    checks != 0 && (1..=2).contains(&replacements)
}

pub fn retained_delete_controls<'a>(controls: impl IntoIterator<Item = &'a HostedControlWrite>) -> bool {
    let mut checks = 0;
    let mut replacements = 0;
    let mut existing = 0;
    for control in controls {
        if control.is_check() {
            checks += 1;
        } else {
            replacements += 1;
            existing += usize::from(control.expected().is_some());
        }
    }
    checks > 0 && existing > 0 && (1..=5).contains(&replacements)
}

pub(crate) fn retained_release_world(operations: &[crate::source::Operation]) -> Option<rtdb_types::Iid> {
    use crate::{TxOp, source::Operation};
    let mut world = None;
    let mut controls = Vec::new();
    let mut delete = None;
    let mut root_checks = 0;
    let mut delete_root = false;
    if !root_partition_frontiers_match(operations) { return None; }
    for op in operations {
        if root_partition_frontier_rows(op).is_some() { continue; }
        match op {
            Operation::Public(TxOp::HostedControl(control)) => controls.push(control),
            Operation::Public(TxOp::OwnedDeadline { .. } | TxOp::OwnedCheckWorldScope { .. }) => (),
            Operation::Public(TxOp::OwnedCheck { relation, .. } | TxOp::OwnedCheckRange { relation, .. })
                if relation.is_root_data() => { root_checks += 1; }
            Operation::Public(TxOp::OwnedDelete { relation, id, valid_from: None, valid_to: None })
                if relation.is_root_data() && delete.is_none() => delete = Some((relation, id)),
            Operation::Public(TxOp::OwnedPublishWorld {
                world: next, expected: None, advance_to: None, drop_world: true, delete_root: writes, land_root: None, ..
            }) if world.is_none() => {
                world = Some(*next);
                delete_root = writes.is_some();
            }
            _ => return None,
        }
    }
    world.filter(|_| {
        if !delete_root {
            return delete.is_none() && root_checks == 0 && retained_release_controls(controls);
        }
        let Some((relation, id)) = delete else { return false; };
        let checked = operations.iter().any(|operation| matches!(
            operation,
            Operation::Public(TxOp::OwnedCheck { relation: checked, id: observed, expected: Some(_), .. })
                if checked == relation && observed == id
        ));
        checked && retained_delete_controls(controls)
    })
}

/// An assertion or checked replacement of one installed hosted control row.
#[derive(Clone, Debug)]
pub struct HostedControlWrite {
    relation: OwnedRelation,
    scope: PlacedAttemptScope,
    id: EntityId,
    expected: Option<Timestamp>,
    document: StructFields<Value>,
    check_only: bool,
}

impl HostedControlWrite {
    pub fn new(
        relation: OwnedRelation,
        scope: PlacedAttemptScope,
        expected: Option<Timestamp>,
        document: StructFields<Value>,
    ) -> Result<Self> {
        if !admits_name(relation.name())
            || document.len() > 128
            || document
                .keys()
                .any(|name| name != "_id" && name.starts_with('_'))
        {
            return Err(invalid());
        }
        let Some(Value::Utf8(id)) = document.get("_id") else {
            return Err(invalid());
        };
        if id.is_empty() || id.len() > 4096 {
            return Err(invalid());
        }
        let id = EntityId::String(id.clone());
        let result = Self {
            relation,
            scope,
            id,
            expected,
            document,
            check_only: false,
        };
        if result.retained_bytes()? > 1024 * 1024 {
            return Err(invalid());
        }
        Ok(result)
    }

    pub fn check(
        relation: OwnedRelation,
        scope: PlacedAttemptScope,
        expected: Option<Timestamp>,
        document: StructFields<Value>,
    ) -> Result<Self> {
        if document.len() != 1 {
            return Err(invalid());
        }
        let mut result = Self::new(relation, scope, expected, document)?;
        result.check_only = true;
        Ok(result)
    }

    pub fn is_check(&self) -> bool {
        self.check_only
    }

    pub(crate) fn name(&self) -> &str {
        self.relation.name()
    }

    pub fn table(&self) -> TableRef {
        TableRef::new("public", self.relation.name())
    }
    pub fn scope(&self) -> PlacedAttemptScope {
        self.scope
    }
    pub fn id(&self) -> &EntityId {
        &self.id
    }
    pub fn expected(&self) -> Option<Timestamp> {
        self.expected
    }
    pub fn replacement(&self) -> Option<&StructFields<Value>> {
        (!self.check_only).then_some(&self.document)
    }

    pub fn retained_bytes(&self) -> Result<usize> {
        Value::document_retained_bytes(&self.document)?
            .checked_add(self.id.as_value().retained_bytes()?)
            .and_then(|n| n.checked_add(self.relation.name().len() + 512))
            .ok_or_else(invalid)
    }

    pub(crate) fn body(&self) -> StructFields<Value> {
        StructFields::from([
            ("relation".into(), Value::Utf8(self.relation.name().into())),
            ("scope".into(), Value::Binary(self.scope.encode().to_vec())),
            (
                "expected".into(),
                self.expected.map_or(Value::Null, |time| Value::I64(time.0)),
            ),
            ("document".into(), Value::Struct(self.document.clone())),
        ])
    }
}

fn invalid() -> Anomaly {
    Anomaly::incorrect("rtdb.hosted/control", "Invalid hosted control replacement")
}

#[cfg(test)]
mod world_read_tests {
    use super::*;
    use crate::{TxOp, source::Operation};
    use rtdb_types::Iid;

    #[test]
    fn hosted_world_reads_require_placement_for_clock_only_batches() -> Result<()> {
        let scope = PlacedAttemptScope::new("hosted-clock-batch", 1)?;
        let owner = HostedControlWrite::check(
            OwnedRelation::new("bj_owner")?, scope, Some(Timestamp(1)),
            StructFields::from([("_id".into(), Value::Utf8("installed".into()))]),
        )?;
        let clock = |line| TxOp::OwnedCheckWorldScope {
            scope,
            clock: Some(rtdb_types::WorldClockLimit {
                world: Iid([line; 16]), pin: Iid([line; 16]), before: Timestamp(7),
            }),
        };
        let mut operations = [
            TxOp::owned_deadline(Timestamp(8)),
            TxOp::HostedControl(owner),
            TxOp::OwnedCheckWorldScope { scope, clock: None },
            clock(1),
            clock(2),
        ].into_iter().map(Operation::Public).collect::<Vec<_>>();
        assert!(hosted_root_world_reads(&operations));
        operations.remove(2);
        assert!(!hosted_root_world_reads(&operations));
        Ok(())
    }

    #[test]
    fn hosted_world_reads_admit_root_writes_with_guarded_world_points() -> Result<()> {
        let scope = PlacedAttemptScope::new("hosted-child-read", 1)?;
        let root = OwnedRelation::new("facts")?;
        let branch = root.in_world(Iid([1; 16]))?;
        let id = EntityId::String("entity".into());
        let controls = OwnedRelation::new("bj_owner")?;
        let value = StructFields::from([("_id".into(), Value::Utf8("installed".into()))]);
        let owner = HostedControlWrite::check(controls, scope, Some(Timestamp(1)), value)?;
        let point = TxOp::OwnedCheck {
            relation: branch.clone(), id: id.clone(), valid_at: Timestamp(4), expected: None,
        };
        let purge = TxOp::OwnedCheckWorldPurge {
            relation: branch.clone(), pin: Iid([2; 16]), frontier: 0, local_frontier: None,
        };
        let mut operations = [
            TxOp::owned_deadline(Timestamp(8)),
            TxOp::HostedControl(owner),
            TxOp::OwnedDelete { relation: root, id: id.clone(), valid_from: None, valid_to: None },
            TxOp::OwnedCheckWorldScope { scope, clock: None },
            purge,
            point,
        ].into_iter().map(Operation::Public).collect::<Vec<_>>();
        assert!(hosted_root_world_reads(&operations));
        operations.swap(4, 5);
        assert!(!hosted_root_world_reads(&operations));
        operations.swap(4, 5);
        operations[2] = Operation::Public(TxOp::OwnedDelete {
            relation: branch, id, valid_from: None, valid_to: None,
        });
        assert!(!hosted_root_world_reads(&operations));
        Ok(())
    }
}

#[cfg(test)]
mod replay_tests {
    use super::*;

    #[test]
    fn replay_requires_the_hold_and_identical_create_only_receipt() -> Result<()> {
        let scope = PlacedAttemptScope::new("native-restore", 1)?;
        let hold = OwnedRelation::new(RESTORE)?;
        let fields = |id: &str| StructFields::from([("_id".into(), Value::Utf8(id.into()))]);
        let (binding, archive, fingerprint) = ([1u8; 32], [2u8; 32], [3u8; 32]);
        let receipt = serde_json::json!({"binding": binding, "archive": archive, "through": 4000,
            "original_authority": ([4u8; 16]), "captured": {"tx_id": 3, "system_time": 4},
            "handoff": {"handoff": ([5u8; 32]), "fence": {"tx_id": 1, "system_time": 2},
                "through": {"tx_id": 2, "system_time": 3}},
            "previous": null, "source": {"tx_id": 3, "system_time": 4}, "logical_at": 3000,
            "fingerprint": fingerprint, "commit_positions": [1, 3]});
        let body = receipt.to_string().into_bytes();
        let replace = |id: &str, expected, body: &[u8]| {
            let mut document = fields(id);
            document.insert("binding".into(), Value::Binary(body.to_vec()));
            HostedControlWrite::new(hold.clone(), scope, expected, document)
        };
        let projection = |logical_at| {
            let time = |value| Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) };
            let mut document = fields("native-transaction/3");
            document.insert("binding".into(), Value::Binary(binding.to_vec()));
            document.insert("digest".into(), Value::Binary(fingerprint.to_vec()));
            document.insert("source_at".into(), time(4));
            document.insert("logical_at".into(), time(logical_at));
            document.insert("source_cut".into(), time(4));
            HostedControlWrite::new(OwnedRelation::new(REPLAY_TRANSACTIONS)?, scope, None, document)
        };
        let mut controls = vec![
            HostedControlWrite::check(OwnedRelation::new("bj_installation")?, scope, Some(Timestamp(9)), fields("authority"))?,
            HostedControlWrite::check(hold.clone(), scope, Some(Timestamp(9)), fields("archive"))?,
            HostedControlWrite::check(hold.clone(), scope, None, fields("replayed"))?,
            replace(NATIVE_FRONTIER, None, &body)?,
            replace("native-transaction/3", None, &body)?,
            projection(3)?,
            HostedControlWrite::check(hold.clone(), scope, Some(Timestamp(9)), fields("restore"))?,
        ];
        let replay = hosted_replay_controls(controls.iter())?.unwrap();
        assert_eq!(replay.commit_positions(), &[1, 3]);
        assert_eq!(replay.source_time(), Timestamp(4));
        assert!(replay.check_order(Timestamp(4)).is_err());
        assert!(replay.check_order(Timestamp(5)).is_ok());
        let positions = (u32::MAX - 1023..=u32::MAX).collect::<Vec<_>>();
        let mut large = receipt.clone();
        large["commit_positions"] = serde_json::json!(&positions);
        let large = large.to_string().into_bytes();
        controls[3] = replace(NATIVE_FRONTIER, None, &large)?;
        controls[4] = replace("native-transaction/3", None, &large)?;
        let replay = hosted_replay_controls(controls.iter())?.unwrap();
        assert_eq!(replay.commit_positions(), positions.as_slice());
        let pin = [archive.as_slice(), 4000u64.to_be_bytes().as_slice()].concat();
        assert_eq!(decode_accepted_replay(&large, binding, &pin)?.commit_positions(), positions.as_slice());
        for (field, value) in [
            ("captured", serde_json::json!({"tx_id": 4, "system_time": 4})),
            ("original_authority", serde_json::json!(([0u8; 16]))),
            ("handoff", serde_json::json!({"handoff": ([5u8; 32]),
                "fence": {"tx_id": 3, "system_time": 4}, "through": {"tx_id": 2, "system_time": 3}})),
        ] {
            let mut invalid = receipt.clone();
            invalid[field] = value;
            let invalid = invalid.to_string().into_bytes();
            controls[3] = replace(NATIVE_FRONTIER, None, &invalid)?;
            controls[4] = replace("native-transaction/3", None, &invalid)?;
            assert!(hosted_replay_controls(controls.iter()).is_err());
        }
        controls[3] = replace(NATIVE_FRONTIER, None, &body)?;
        controls[4] = replace("native-transaction/3", None, &body)?;
        controls[4] = replace("native-transaction/3", Some(Timestamp(9)), &body)?;
        assert!(hosted_replay_controls(controls.iter()).is_err());
        controls[4] = replace("native-transaction/3", None, &body)?;
        controls[5] = projection(2)?;
        assert!(hosted_replay_controls(controls.iter()).is_err());
        controls[5] = projection(3)?;
        let lifetime = |swept_present_through| serde_json::json!([{
            "relation": "root_lifetime", "duration": 2, "expired_created_through": 2,
            "swept_present_through": swept_present_through,
        }]).to_string();
        controls.push(replace("native-lifetimes/3", None, lifetime(3).as_bytes())?);
        let archived = hosted_replay_controls(controls.iter())?.unwrap()
            .lifetime(&OwnedRelation::new("root_lifetime")?).unwrap();
        assert_eq!(archived.swept_present_through, Timestamp(3));
        *controls.last_mut().unwrap() = replace("native-lifetimes/3", None, lifetime(5).as_bytes())?;
        assert!(hosted_replay_controls(controls.iter()).is_err());
        controls.pop();
        let members = [0u8; 32];
        let proof = |kind, through| serde_json::json!([{
            "relation": "root_capacity", "kind": kind, "writes": 0,
            "through": through, "members": members,
            "capacity_declarations": [{"column": "partition", "when": [], "cap": 2}],
        }]).to_string();
        let companion = |kind, through| {
            replace("native-constraints/3", None, proof(kind, through).as_bytes())
        };
        controls.push(companion("capacity", 7)?);
        let parsed = || hosted_replay_controls(controls.iter()).map(Option::unwrap);
        let relation = OwnedRelation::new("root_capacity")?;
        let mut replay = parsed()?;
        let rules = replay.capacity_declarations().collect::<Vec<_>>();
        assert_eq!(rules.len(), 1);
        assert_eq!((rules[0].0, rules[0].1.column.as_str(), rules[0].1.cap),
            ("root_capacity", "partition", 2));
        assert!(replay.require_constraints_matched().is_err());
        replay.match_constraint(&relation, ReplayConstraintKind::Capacity, 0, 7, [0; 32], 1)?;
        replay.require_constraints_matched()?;
        assert!(parsed()?.match_constraint(&relation, ReplayConstraintKind::Capacity, 0, 6, [0; 32], 1).is_err());
        assert!(parsed()?.match_constraint(&relation, ReplayConstraintKind::Capacity, 0, 7, [0; 32], 0).is_err());
        *controls.last_mut().unwrap() = companion("unique", 7)?;
        assert!(hosted_replay_controls(controls.iter()).is_err());
        controls.pop();
        controls.pop();
        assert!(hosted_replay_controls(controls.iter()).is_err());
        Ok(())
    }
}

#[cfg(test)]
mod retained_release_tests {
    use super::*;
    use crate::{TxOp, source::Operation};
    use rtdb_types::Iid;

    #[test]
    fn retained_release_requires_existing_replacements_and_one_terminal_world() -> Result<()> {
        let scope = PlacedAttemptScope::new("root-release", 1)?;
        let relation = OwnedRelation::new("bj_owner")?;
        let control = |id: &str| StructFields::from([("_id".into(), Value::Utf8(id.into()))]);
        let check = HostedControlWrite::check(relation.clone(), scope, Some(Timestamp(1)), control("installed"))?;
        let replacement = |expected| HostedControlWrite::new(relation.clone(), scope, expected, control("holder"));
        let drop = |world| TxOp::OwnedPublishWorld {
            world, parent_pin: world,
            attempt: "0000000000000000000000".into(),
            fingerprint: [1; 32],
            expected: None,
            advance_to: None,
            drop_world: true,
            settlement_pin: None,
            delete_root: None,
            land_root: None,
        };
        let world = Iid([1; 16]);
        let mut operations = [
            TxOp::owned_deadline(Timestamp(8)),
            TxOp::OwnedCheckWorldScope { scope, clock: None },
            TxOp::HostedControl(check),
            TxOp::HostedControl(replacement(Some(Timestamp(2)))?),
            drop(world),
        ].into_iter().map(Operation::Public).collect::<Vec<_>>();
        assert_eq!(retained_release_world(&operations), Some(world));
        operations.push(Operation::Public(drop(Iid([2; 16]))));
        assert_eq!(retained_release_world(&operations), None);
        operations.pop();
        operations[3] = Operation::Public(TxOp::HostedControl(replacement(None)?));
        assert_eq!(retained_release_world(&operations), None);
        Ok(())
    }

    #[test]
    fn proposal_period_requires_its_entity_point_and_original_overlay_frontier() -> Result<()> {
        let scope = PlacedAttemptScope::new("proposal-period", 1)?;
        let world = Iid([1; 16]);
        let facts = OwnedRelation::new("facts")?.in_world(world)?;
        let control_relation = OwnedRelation::new("bj_owner")?;
        let control = |id: &str| StructFields::from([("_id".into(), Value::Utf8(id.into()))]);
        let id = EntityId::String("entity".into());
        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let shadow = rtdb_types::WorldShadow::Deleted.record(&id)?;
        let mut operations = [
            TxOp::HostedControl(HostedControlWrite::check(control_relation.clone(), scope, Some(Timestamp(1)), control("installed"))?),
            TxOp::HostedControl(HostedControlWrite::new(control_relation, scope, Some(Timestamp(2)), control("owner"))?),
            TxOp::OwnedPublishWorld {
                world, parent_pin: world, attempt: "0000000000000000000000".into(), fingerprint: [1; 32],
                expected: None, advance_to: None, drop_world: false, settlement_pin: None,
                delete_root: None, land_root: None,
            },
            TxOp::OwnedReplaceDocs {
                relation: facts.clone(), documents: crate::Documents::new(vec![shadow], &memory)?,
                valid_from: Some(Timestamp(10)), valid_to: Some(Timestamp(20)),
            },
        ].into_iter().map(Operation::Public).collect::<Vec<_>>();
        operations.push(Operation::Public(TxOp::OwnedCheckWorldPurge {
            relation: facts.clone(), pin: world, frontier: 0, local_frontier: Some(0),
        }));
        assert_eq!(hosted_proposal_overlay(&operations), None);
        operations.push(Operation::Public(TxOp::OwnedCheck {
            relation: facts.clone(), id: id.clone(), valid_at: Timestamp(11), expected: None,
        }));
        assert_eq!(hosted_proposal_overlay(&operations), None);
        operations.pop();
        operations.push(Operation::Public(TxOp::OwnedCheck {
            relation: facts, id, valid_at: Timestamp(10), expected: None,
        }));
        assert_eq!(hosted_proposal_overlay(&operations), Some(world));
        operations.remove(4);
        assert_eq!(hosted_proposal_overlay(&operations), None);
        operations.push(Operation::Public(TxOp::OwnedCheckWorldPurge {
            relation: OwnedRelation::new("facts")?.in_world(Iid([2; 16]))?,
            pin: world, frontier: 0, local_frontier: Some(0),
        }));
        assert_eq!(hosted_proposal_overlay(&operations), None);
        Ok(())
    }

    #[test]
    fn later_proposal_appends_require_absence_for_each_carrier() -> Result<()> {
        let scope = PlacedAttemptScope::new("proposal-appends", 1)?;
        let world = Iid([1; 16]);
        let relation = OwnedRelation::new("samples")?.in_world(world)?;
        let control = OwnedRelation::new("bj_owner")?;
        let row = |id: &str| StructFields::from([("_id".into(), Value::Utf8(id.into()))]);
        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let mut operations = vec![
            TxOp::HostedControl(HostedControlWrite::check(control.clone(), scope, Some(Timestamp(1)), row("installed"))?),
            TxOp::HostedControl(HostedControlWrite::new(control, scope, Some(Timestamp(2)), row("owner"))?),
            TxOp::OwnedPublishWorld {
                world, parent_pin: world, attempt: "0000000000000000000000".into(), fingerprint: [1; 32],
                expected: Some(Timestamp(3)), advance_to: None, drop_world: false, settlement_pin: None,
                delete_root: None, land_root: None,
            },
        ].into_iter().map(Operation::Public).collect::<Vec<_>>();
        for id in ["first", "second"] {
            operations.push(Operation::Public(TxOp::OwnedInsertDocs {
                relation: relation.clone(), documents: crate::Documents::new(vec![row(id)], &memory)?,
                valid_from: None, valid_to: None,
            }));
        }
        for id in ["first", "second"] {
            assert_eq!(hosted_proposal_overlay(&operations), None);
            operations.push(Operation::Public(TxOp::OwnedCheck {
                relation: relation.clone(), id: EntityId::String(id.into()),
                valid_at: Timestamp(4), expected: None,
            }));
        }
        assert_eq!(hosted_proposal_overlay(&operations), Some(world));
        Ok(())
    }

    #[test]
    fn world_cut_partition_frontier_tracks_one_written_root_partition() -> Result<()> {
        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let publications = OwnedRelation::new(rtdb_types::OWNED_ROOT_PUBLICATION_RELATION)?;
        let row = |id: &str| StructFields::from([("_id".into(), Value::Utf8(id.into()))]);
        let frontier = |id: &str| Ok::<_, Anomaly>(Operation::Public(TxOp::OwnedReplaceDocs {
            relation: publications.clone(), documents: crate::Documents::new(vec![row(id)], &memory)?,
            valid_from: Some(Timestamp::MIN), valid_to: None,
        }));
        let mut ops = vec![frontier("fact_partition_head:fact_row")?];
        assert!(!root_partition_frontiers_match(&ops));
        ops.push(Operation::Public(TxOp::OwnedInsertDocs {
            relation: OwnedRelation::new("fact_row")?, documents: crate::Documents::new(vec![row("key")], &memory)?,
            valid_from: None, valid_to: None,
        }));
        assert!(root_partition_frontiers_match(&ops));
        ops.push(frontier("fact_partition_head:fact_row")?);
        assert!(!root_partition_frontiers_match(&ops));
        Ok(())
    }

    #[test]
    fn proposal_landing_orders_frontiers_with_the_fact_and_both_receipt_locators() -> Result<()> {
        let scope = PlacedAttemptScope::new("proposal-landing", 1)?;
        let world = Iid([1; 16]);
        let pin = Iid([2; 16]);
        let relation = OwnedRelation::new("facts")?;
        let local = relation.in_world(world)?;
        let admission = OwnedRelation::new("bj_fact_root_admission")?;
        let publications = OwnedRelation::new(rtdb_types::OWNED_ROOT_PUBLICATION_RELATION)?;
        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let row = |id: &str| StructFields::from([("_id".into(), Value::Utf8(id.into()))]);
        let id = EntityId::String("key".into());
        let control = |slot: &str, expected| {
            HostedControlWrite::new(admission.clone(), scope, expected, row(slot)).map(TxOp::HostedControl)
        };
        let frontier = |id| Ok::<_, Anomaly>(TxOp::OwnedReplaceDocs {
            relation: publications.clone(), documents: crate::Documents::new(vec![row(id)], &memory)?,
            valid_from: Some(Timestamp::MIN), valid_to: None,
        });
        let mut operations = [
            TxOp::HostedControl(HostedControlWrite::check(admission.clone(), scope, Some(Timestamp(1)), row("installed"))?),
            control(&format!("root_proposal_head:{}", "a".repeat(64)), Some(Timestamp(2)))?,
            control(&format!("root_proposal_landing:{}", "b".repeat(64)), None)?,
            TxOp::OwnedCheck { relation: relation.clone(), id: id.clone(), valid_at: Timestamp(4), expected: None },
            TxOp::OwnedInsertDocs {
                relation: relation.clone(), documents: crate::Documents::new(vec![row("key")], &memory)?,
                valid_from: None, valid_to: None,
            },
            TxOp::OwnedCheckWorldPurge { relation: local.clone(), pin, frontier: 0, local_frontier: None },
            TxOp::OwnedCheck { relation: local.clone(), id, valid_at: Timestamp(4), expected: Some(Timestamp(3)) },
            TxOp::OwnedPublishWorld {
                world, parent_pin: pin, attempt: "0000000000000000000000".into(), fingerprint: [1; 32],
                expected: Some(Timestamp(3)), advance_to: None, drop_world: true,
                settlement_pin: None, delete_root: None, land_root: Some(pin),
            },
        ].into_iter().map(Operation::Public).collect::<Vec<_>>();
        assert_eq!(hosted_proposal_land(&operations), None);
        operations.push(Operation::Public(control(&format!("root_proposal_landing_proof:{}", "c".repeat(64)), None)?));

        let marker = frontier("public_fact_head:AAAAAAAAAAAAAAAAAAAAAA")?;
        assert!(is_proposal_landing_frontier(&marker));
        operations.push(Operation::Public(marker));
        assert_eq!(hosted_proposal_land(&operations), Some(HostedProposalLanding {
            world, pin, effect: ProposalLandingEffect::WithPoints,
        }));

        operations.push(Operation::Public(TxOp::OwnedReplaceDocs {
            relation: relation.clone(), documents: crate::Documents::new(vec![row("key")], &memory)?,
            valid_from: None, valid_to: None,
        }));
        assert_eq!(hosted_proposal_land(&operations), Some(HostedProposalLanding {
            world, pin, effect: ProposalLandingEffect::WithPoints,
        }));
        operations.push(Operation::Public(TxOp::OwnedInsertDocs {
            relation: relation.clone(), documents: crate::Documents::new(vec![row("key")], &memory)?,
            valid_from: None, valid_to: None,
        }));
        assert_eq!(hosted_proposal_land(&operations), None);
        operations.pop();
        operations.push(Operation::Public(TxOp::OwnedReplaceDocs {
            relation: relation.clone(), documents: crate::Documents::new(vec![row("key")], &memory)?,
            valid_from: Some(Timestamp(10)), valid_to: Some(Timestamp(20)),
        }));
        assert_eq!(hosted_proposal_land(&operations), None);
        operations.pop();

        operations.push(Operation::Public(frontier("public_fact_head:BAAAAAAAAAAAAAAAAAAAAA")?));
        assert_eq!(hosted_proposal_land(&operations), None);
        operations.pop();
        operations.push(Operation::Public(frontier("head")?));
        assert_eq!(hosted_proposal_land(&operations), None);
        operations.pop();

        let replacement = EntityId::String("existing".into());
        operations.extend([
            TxOp::OwnedReplaceDocs {
                relation: relation.clone(), documents: crate::Documents::new(vec![row("existing")], &memory)?,
                valid_from: None, valid_to: None,
            },
            TxOp::OwnedCheckWorldPurge { relation: local.clone(), pin, frontier: 0, local_frontier: None },
            TxOp::OwnedCheck { relation: local, id: replacement.clone(), valid_at: Timestamp(4), expected: Some(Timestamp(2)) },
            TxOp::OwnedCheck { relation: relation.clone(), id: replacement.clone(), valid_at: Timestamp(4), expected: None },
        ].into_iter().map(Operation::Public));
        assert_eq!(hosted_proposal_land(&operations), None);
        operations.pop();
        operations.push(Operation::Public(TxOp::OwnedCheck {
            relation, id: replacement, valid_at: Timestamp(4), expected: Some(Timestamp(1)),
        }));
        assert_eq!(hosted_proposal_land(&operations), Some(HostedProposalLanding {
            world, pin, effect: ProposalLandingEffect::WithPoints,
        }));
        Ok(())
    }

    #[test]
    fn landing_orders_task_intents_with_their_dispatch_reservation() -> Result<()> {
        let scope = PlacedAttemptScope::new("proposal-landing-tasks", 1)?;
        let admission = OwnedRelation::new("bj_fact_root_admission")?;
        let row = |id: &str| StructFields::from([("_id".into(), Value::Utf8(id.into()))]);
        let insert = |slot: String| HostedControlWrite::new(admission.clone(), scope, None, row(&slot));
        let mut controls = vec![
            HostedControlWrite::check(admission.clone(), scope, Some(Timestamp(1)), row("installed"))?,
            HostedControlWrite::new(admission.clone(), scope, Some(Timestamp(2)), row(&format!("root_proposal_head:{}", "a".repeat(64))))?,
            insert(format!("root_proposal_landing:{}", "b".repeat(64)))?,
            insert(format!("root_proposal_landing_proof:{}", "c".repeat(64)))?,
        ];
        assert!(proposal_land_controls(&controls));

        controls.push(insert(format!("clock-task:{}", "d".repeat(64)))?);
        assert!(proposal_land_controls(&controls));
        controls.push(insert(format!("action-task:{}", "k".repeat(32)))?);
        assert!(!proposal_land_controls(&controls), "an action intent needs its reservation carrier");
        controls.push(insert(format!("task-acceptance:{}", "e".repeat(64)))?);
        assert!(proposal_land_controls(&controls));
        controls.push(insert(format!("action-task:{}", "m".repeat(32)))?);
        assert!(!proposal_land_controls(&controls), "one landing admits one dispatched action");
        controls.pop();
        controls.push(insert(format!("task-acceptance:{}", "f".repeat(64)))?);
        assert!(!proposal_land_controls(&controls), "one landing carries one reservation batch");
        controls.pop();
        controls.push(HostedControlWrite::new(admission.clone(), scope, Some(Timestamp(3)), row(&format!("clock-task:{}", "0".repeat(64))))?);
        assert!(!proposal_land_controls(&controls), "a landing cannot replace an existing task");
        controls.pop();
        for index in 0..MAX_PROPOSAL_LAND_TASKS - 2 {
            controls.push(insert(format!("clock-task:{index:064x}"))?);
        }
        assert!(proposal_land_controls(&controls));
        controls.push(insert(format!("clock-task:{}", "f".repeat(64)))?);
        assert!(!proposal_land_controls(&controls));
        Ok(())
    }

    #[test]
    fn finite_period_landing_requires_the_original_starts_and_both_windows() -> Result<()> {
        let scope = PlacedAttemptScope::new("proposal-period-landing", 1)?;
        let world = Iid([1; 16]);
        let pin = Iid([2; 16]);
        let relation = OwnedRelation::new("facts")?;
        let local = relation.in_world(world)?;
        let admission = OwnedRelation::new("bj_fact_root_admission")?;
        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let row = |id: &str| StructFields::from([("_id".into(), Value::Utf8(id.into()))]);
        let id = EntityId::String("entity".into());
        let start = Timestamp(10);
        let generation = Timestamp(3);
        let control = |prefix: &str, expected| {
            HostedControlWrite::new(admission.clone(), scope, expected, row(&format!("{prefix}{}", "a".repeat(64))))
                .map(TxOp::HostedControl)
        };
        let mut operations = [
            TxOp::HostedControl(HostedControlWrite::check(admission.clone(), scope, Some(Timestamp(1)), row("installed"))?),
            control("root_proposal_head:", Some(Timestamp(2)))?,
            control("root_proposal_landing:", None)?,
            control("root_proposal_landing_proof:", None)?,
            TxOp::OwnedReplaceDocs {
                relation: relation.clone(), documents: crate::Documents::new(vec![row("entity")], &memory)?,
                valid_from: Some(start), valid_to: Some(Timestamp(20)),
            },
            TxOp::OwnedCheckWorldPurge { relation: local.clone(), pin, frontier: 0, local_frontier: None },
            TxOp::OwnedCheck { relation: local.clone(), id: id.clone(), valid_at: start, expected: Some(Timestamp(2)) },
            TxOp::OwnedPublishWorld {
                world, parent_pin: pin, attempt: "0000000000000000000000".into(), fingerprint: [1; 32],
                expected: Some(generation), advance_to: None, drop_world: true,
                settlement_pin: None, delete_root: None, land_root: Some(pin),
            },
        ].into_iter().map(Operation::Public).collect::<Vec<_>>();
        operations.push(Operation::Public(TxOp::OwnedCheckRange {
            relation: relation.clone(), basis: rtdb_types::TransactionKey { tx_id: rtdb_types::TxId(3), system_time: Timestamp(4) },
            scope, frontier: 0, until: Timestamp(100), region: None,
        }));
        let local_window = TxOp::OwnedCheckWorldPurge {
            relation: local.clone(), pin, frontier: 0, local_frontier: Some(0),
        };
        operations.push(Operation::Public(local_window.clone()));
        let root_point = |valid_at| Operation::Public(TxOp::OwnedCheck {
            relation: relation.clone(), id: id.clone(), valid_at, expected: None,
        });
        operations.push(root_point(Timestamp(11)));
        assert_eq!(hosted_proposal_land(&operations), None);
        operations.pop();
        operations.push(root_point(start));
        assert_eq!(hosted_proposal_land(&operations), Some(HostedProposalLanding {
            world, pin, effect: ProposalLandingEffect::FixedPeriods,
        }));
        let root_point = operations.pop().unwrap();
        operations.pop();
        operations.push(root_point);
        assert_eq!(hosted_proposal_land(&operations), None);
        operations.push(Operation::Public(local_window));

        let later_start = Timestamp(15);
        operations.push(Operation::Public(TxOp::OwnedReplaceDocs {
            relation: relation.clone(), documents: crate::Documents::new(vec![row("entity")], &memory)?,
            valid_from: Some(later_start), valid_to: Some(Timestamp(25)),
        }));
        assert_eq!(hosted_proposal_land(&operations), None);
        operations.extend([
            TxOp::OwnedCheckWorldPurge { relation: local.clone(), pin, frontier: 0, local_frontier: None },
            TxOp::OwnedCheck { relation: local.clone(), id: id.clone(), valid_at: later_start, expected: Some(generation) },
        ].into_iter().map(Operation::Public));
        assert_eq!(hosted_proposal_land(&operations), None);
        operations.push(Operation::Public(TxOp::OwnedCheck {
            relation: relation.clone(), id: id.clone(), valid_at: later_start, expected: None,
        }));
        assert_eq!(hosted_proposal_land(&operations), Some(HostedProposalLanding {
            world, pin, effect: ProposalLandingEffect::FixedPeriods,
        }));

        let second = EntityId::String("second".into());
        let second_start = Timestamp(21);
        operations.extend([
            TxOp::OwnedReplaceDocs {
                relation: relation.clone(), documents: crate::Documents::new(vec![row("second")], &memory)?,
                valid_from: Some(second_start), valid_to: Some(Timestamp(30)),
            },
            TxOp::OwnedCheckWorldPurge { relation: local.clone(), pin, frontier: 0, local_frontier: None },
            TxOp::OwnedCheck { relation: local, id: second.clone(), valid_at: second_start, expected: Some(generation) },
            TxOp::OwnedCheck {
                relation, id: second, valid_at: second_start, expected: None,
            },
        ].into_iter().map(Operation::Public));
        assert_eq!(hosted_proposal_land(&operations), Some(HostedProposalLanding {
            world, pin, effect: ProposalLandingEffect::FixedPeriods,
        }));

        let points = OwnedRelation::new("points")?;
        let local = points.in_world(world)?;
        let point = EntityId::String("key".into());
        operations.extend([
            TxOp::OwnedReplaceDocs {
                relation: points.clone(), documents: crate::Documents::new(vec![row("key")], &memory)?,
                valid_from: None, valid_to: None,
            },
            TxOp::OwnedCheckWorldPurge { relation: local.clone(), pin, frontier: 0, local_frontier: None },
            TxOp::OwnedCheck { relation: local, id: point.clone(), valid_at: start, expected: Some(generation) },
            TxOp::OwnedCheck { relation: points, id: point, valid_at: start, expected: Some(Timestamp(1)) },
        ].into_iter().map(Operation::Public));
        assert_eq!(hosted_proposal_land(&operations), Some(HostedProposalLanding {
            world, pin, effect: ProposalLandingEffect::WithPoints,
        }));
        Ok(())
    }
}

/// Proposal content settlements may also replace their exact generation row.
pub fn proposal_overlay_controls<'a>(controls: impl IntoIterator<Item = &'a HostedControlWrite>) -> bool {
    let mut checks = 0;
    let mut existing = 0;
    let mut replacements = 0;
    for control in controls {
        if control.is_check() {
            checks += 1;
        } else {
            existing += usize::from(control.expected().is_some());
            replacements += 1;
        }
    }
    checks > 0 && (1..=6).contains(&replacements)
        && existing >= if replacements == 6 { 2 } else { 1 }
}

/// Classify an overlay settled with hosted owner controls and its original checks.
pub(crate) fn hosted_proposal_overlay(operations: &[crate::source::Operation]) -> Option<rtdb_types::Iid> {
    use std::collections::{BTreeMap, BTreeSet};
    use crate::{TxOp, source::Operation};
    let mut world = None;
    let mut controls = Vec::new();
    let mut writes = Vec::new();
    let mut terminal = false;
    let mut points = BTreeMap::<_, BTreeSet<_>>::new();
    let mut timelines = BTreeSet::new();
    for operation in operations {
        match operation {
            Operation::Public(TxOp::HostedControl(control)) => controls.push(control),
            Operation::Public(TxOp::OwnedDeadline { .. } | TxOp::OwnedCheckWorldScope { .. }
                | TxOp::OwnedCheckRange { .. }) => (),
            Operation::Public(TxOp::OwnedCheckWorldPurge { relation, local_frontier, .. }) => {
                if local_frontier.is_some() { timelines.insert(relation); }
            }
            Operation::Public(TxOp::OwnedCheck { relation, id, valid_at, expected }) => {
                points.entry((relation, id.handle_bytes())).or_default().insert((*valid_at, expected.is_none()));
            }
            Operation::Public(TxOp::OwnedPublishWorld {
                world: next, advance_to: None, drop_world: false, settlement_pin, delete_root: None, ..
            }) if world.is_none() => {
                world = Some(*next);
                terminal = settlement_pin.is_some();
            },
            operation => {
                let write = proposal_overlay_write(operation)?;
                let start = match (write.from, write.to) {
                    (None, None) => None,
                    (Some(from), Some(to)) if Timestamp::MIN < from && from < to && to < Timestamp::MAX => Some(from),
                    _ => return None,
                };
                if write.documents.rows().len() != 1 { return None; }
                let id = write.documents.rows()[0].get("_id")?.entity_id().ok()?;
                if write.create_only && start.is_some() { return None; }
                writes.push((write.relation, id.handle_bytes(), start, write.create_only));
            }
        }
    }
    let world = world?;
    let controls_allowed = if terminal { retained_delete_controls(controls) } else { proposal_overlay_controls(controls) };
    (terminal == writes.is_empty() && controls_allowed && writes.into_iter().all(|(relation, id, start, create_only)| {
        relation.world() == Some(world)
            && points.remove(&(relation, id)).is_some_and(|instants| {
                if create_only {
                    instants.iter().all(|(_, absent)| *absent)
                } else {
                    start.is_none_or(|start| instants.iter().any(|(at, _)| *at == start) && timelines.contains(&relation))
                }
            })
    })).then_some(world)
}

pub(crate) struct ProposalOverlayWrite<'a> {
    pub(crate) relation: &'a OwnedRelation,
    pub(crate) documents: &'a crate::Documents,
    pub(crate) from: Option<Timestamp>,
    pub(crate) to: Option<Timestamp>,
    pub(crate) create_only: bool,
}

pub(crate) fn proposal_overlay_write(
    operation: &crate::source::Operation,
) -> Option<ProposalOverlayWrite<'_>> {
    use crate::{TxOp, source::Operation};
    let (relation, documents, from, to, create_only) = match operation {
        Operation::Public(TxOp::OwnedReplaceDocs { relation, documents, valid_from, valid_to }) =>
            (relation, documents, valid_from, valid_to, false),
        Operation::Public(TxOp::OwnedInsertDocs { relation, documents, valid_from, valid_to }) =>
            (relation, documents, valid_from, valid_to, true),
        Operation::OwnedWriteIids { relation, documents, valid_from, valid_to, create_only, .. } =>
            (relation, documents, valid_from, valid_to, *create_only),
        _ => return None,
    };
    relation.world().map(|_| ProposalOverlayWrite {
        relation, documents, from: *from, to: *to, create_only,
    })
}

/// Fresh native task records a landing may order with its receipt.
pub const MAX_PROPOSAL_LAND_TASKS: usize = 16;

/// A landing settles its replay head, receipt, and proof locator together.
/// The reviewed command's task intents publish in the same commit: fresh
/// clock tasks, and at most one dispatched action task whose dispatch
/// reservation carrier is inserted beside it, so the intent exists exactly
/// when the landing receipt does.
pub fn proposal_land_controls<'a>(controls: impl IntoIterator<Item = &'a HostedControlWrite>) -> bool {
    let mut checks = 0;
    let mut head = false;
    let mut receipt = false;
    let mut proof = false;
    let mut carrier = false;
    let mut action = false;
    let mut tasks = 0;
    for control in controls {
        if control.is_check() {
            checks += 1;
            continue;
        }
        let EntityId::String(id) = control.id() else { return false; };
        if control.name() != "bj_fact_root_admission" { return false; }
        let slot = match control.expected() {
            Some(_) if landing_control_id(id, "root_proposal_head:") => &mut head,
            None if landing_control_id(id, "root_proposal_landing:") => &mut receipt,
            None if landing_control_id(id, "root_proposal_landing_proof:") => &mut proof,
            None if landing_control_id(id, "task-acceptance:") => &mut carrier,
            None if landing_action_task_id(id) => {
                tasks += 1;
                &mut action
            }
            None if landing_control_id(id, "clock-task:") => {
                tasks += 1;
                if tasks > MAX_PROPOSAL_LAND_TASKS { return false; }
                continue;
            }
            _ => return false,
        };
        if std::mem::replace(slot, true) { return false; }
    }
    checks > 0 && head && receipt && proof && carrier == action && tasks <= MAX_PROPOSAL_LAND_TASKS
}

/// `action-task:` followed by the occurrence's encoded document id.
fn landing_action_task_id(id: &str) -> bool {
    id.strip_prefix("action-task:").is_some_and(|occurrence| {
        (16..=64).contains(&occurrence.len())
            && occurrence.bytes().all(|byte| byte.is_ascii_digit() || byte.is_ascii_lowercase())
    })
}

fn landing_control_id(id: &str, prefix: &str) -> bool {
    id.strip_prefix(prefix).is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

/// Last-holder closure replaces the exact census, owner, and quota, and writes a receipt.
pub fn proposal_close_controls<'a>(controls: impl IntoIterator<Item = &'a HostedControlWrite>) -> bool {
    let mut checks = 0;
    let mut replacements = 0;
    let mut inserts = 0;
    for control in controls {
        if control.is_check() {
            checks += 1;
        } else if control.expected().is_some() {
            replacements += 1;
        } else {
            inserts += 1;
        }
    }
    checks > 0 && (3..=5).contains(&replacements) && (1..=2).contains(&inserts)
}

pub const MAX_PROPOSAL_LAND_ROWS: usize = 16;

pub(crate) fn public_fact_frontier(id: &str) -> bool {
    let Some(encoded) = id.strip_prefix("public_fact_head:") else { return false; };
    let Some((tail, digits)) = encoded.as_bytes().split_last() else { return false; };
    encoded.len() == 22
        && matches!(*tail, b'A' | b'Q' | b'g' | b'w')
        && digits.iter().all(|byte| byte.is_ascii_alphanumeric() || matches!(*byte, b'-' | b'_'))
}

fn root_partition_frontier(id: &str) -> Option<&str> {
    let relation = id.strip_prefix("fact_partition_head:")?;
    (relation.starts_with("fact_") && OwnedRelation::new(relation).is_ok_and(|relation| relation.is_root_data()))
        .then_some(relation)
}

fn partition_frontier_rows(
    relation: &OwnedRelation, documents: &crate::Documents, from: Option<Timestamp>, to: Option<Timestamp>,
) -> bool {
    relation.name() == rtdb_types::OWNED_ROOT_PUBLICATION_RELATION
        && from == Some(Timestamp::MIN) && to.is_none_or(|to| to == Timestamp::MAX)
        && documents.has_exact_presence() && !documents.rows().is_empty()
        && documents.rows().iter().all(|row| {
            row.len() == 1 && matches!(row.get("_id"), Some(Value::Utf8(id)) if root_partition_frontier(id).is_some())
        })
}

pub fn is_root_partition_frontier(op: &crate::TxOp) -> bool {
    matches!(op, crate::TxOp::OwnedReplaceDocs { relation, documents, valid_from, valid_to }
        if partition_frontier_rows(relation, documents, *valid_from, *valid_to))
}

pub(crate) fn root_partition_frontier_rows(op: &crate::source::Operation) -> Option<&crate::Documents> {
    use crate::{TxOp, source::Operation};
    match op {
        Operation::Public(TxOp::OwnedReplaceDocs { relation, documents, valid_from, valid_to })
        | Operation::OwnedWriteIids { relation, documents, valid_from, valid_to, create_only: false, .. }
            if partition_frontier_rows(relation, documents, *valid_from, *valid_to) => Some(documents),
        _ => None,
    }
}

/// A world cut may advance each written Root relation's frontier once.
pub(crate) fn root_partition_frontiers_match(operations: &[crate::source::Operation]) -> bool {
    use crate::{TxOp, source::Operation};
    let mut markers = operations.iter().filter_map(root_partition_frontier_rows).peekable();
    if markers.peek().is_none() { return true; }
    let written = operations.iter().filter_map(|op| {
        if let Some((relation, rows)) = op.owned_documents() && rows != 0 && relation.is_root_data() {
            return Some(relation.name());
        }
        match op {
            Operation::Public(TxOp::OwnedDelete { relation, .. }) if relation.is_root_data() => Some(relation.name()),
            _ => None,
        }
    }).collect::<std::collections::BTreeSet<_>>();
    let mut seen = std::collections::BTreeSet::new();
    markers.flat_map(crate::Documents::rows).all(|row| {
        matches!(row.get("_id"), Some(Value::Utf8(id)) if root_partition_frontier(id)
            .is_some_and(|relation| written.contains(relation) && seen.insert(relation)))
    })
}

fn proposal_frontier_rows(
    relation: &OwnedRelation, documents: &crate::Documents, from: Option<Timestamp>, to: Option<Timestamp>,
) -> bool {
    relation.name() == rtdb_types::OWNED_ROOT_PUBLICATION_RELATION
        && from == Some(Timestamp::MIN) && to.is_none_or(|to| to == Timestamp::MAX)
        && documents.has_exact_presence()
        && (1..=MAX_PROPOSAL_LAND_ROWS).contains(&documents.rows().len())
        && documents.rows().iter().all(|row| {
            row.len() == 1 && matches!(row.get("_id"), Some(Value::Utf8(id)) if public_fact_frontier(id))
        })
}

pub fn is_proposal_landing_frontier(op: &crate::TxOp) -> bool {
    matches!(op, crate::TxOp::OwnedReplaceDocs { relation, documents, valid_from, valid_to }
        if proposal_frontier_rows(relation, documents, *valid_from, *valid_to))
}

fn finite_landing_period(from: Option<Timestamp>, to: Option<Timestamp>) -> Option<Timestamp> {
    let (from, to) = (from?, to?);
    (Timestamp::MIN < from && from < to && to < Timestamp::MAX).then_some(from)
}

pub fn proposal_landing_write_rows(op: &crate::TxOp) -> Option<usize> {
    use crate::TxOp;
    if let TxOp::OwnedArrowDocs { relation, documents, create_only, valid_from, valid_to } = op {
        let valid = (valid_from.is_none() && valid_to.is_none())
            || (!create_only && finite_landing_period(*valid_from, *valid_to).is_some());
        return (relation.is_root_data() && valid && documents.row_count() != 0).then_some(documents.row_count());
    }
    let (relation, documents) = match op {
        TxOp::OwnedInsertDocs { relation, documents, valid_from: None, valid_to: None } =>
            (relation, documents),
        TxOp::OwnedReplaceDocs { relation, documents, valid_from, valid_to }
            if (valid_from.is_none() && valid_to.is_none())
                || finite_landing_period(*valid_from, *valid_to).is_some() =>
            (relation, documents),
        _ => return None,
    };
    (relation.is_root_data() && !documents.rows().is_empty()).then_some(documents.rows().len())
}

pub(crate) fn proposal_landing_frontier_rows(op: &crate::source::Operation) -> Option<&crate::Documents> {
    use crate::{TxOp, source::Operation};
    match op {
        Operation::Public(TxOp::OwnedReplaceDocs { relation, documents, valid_from, valid_to })
        | Operation::OwnedWriteIids { relation, documents, valid_from, valid_to, create_only: false, .. }
            if proposal_frontier_rows(relation, documents, *valid_from, *valid_to) => Some(documents),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProposalLandingEffect {
    WithPoints,
    FixedPeriods,
    Close,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct HostedProposalLanding {
    pub(crate) world: rtdb_types::Iid,
    pub(crate) pin: rtdb_types::Iid,
    pub(crate) effect: ProposalLandingEffect,
}

struct ProposalLandingKey<'a> {
    relation: &'a OwnedRelation,
    id: EntityId,
    starts: Vec<Option<Timestamp>>,
    create_only: bool,
}

/// One exact Proposal terminal effect and its matching witnessed Root writes.
pub(crate) fn hosted_proposal_land(
    operations: &[crate::source::Operation],
) -> Option<HostedProposalLanding> {
    use crate::{TxOp, source::Operation};
    let mut landing = None;
    let mut writes = std::collections::BTreeMap::new();
    let mut assertion_count = 0;
    let mut close = false;
    let mut controls = Vec::new();
    let mut frontiers = std::collections::BTreeSet::new();
    if !root_partition_frontiers_match(operations) { return None; }
    for op in operations {
        if root_partition_frontier_rows(op).is_some() { continue; }
        if let Some(documents) = proposal_landing_frontier_rows(op) {
            for row in documents.rows() {
                let Value::Utf8(id) = row.get("_id")? else { return None; };
                if !frontiers.insert(id) { return None; }
            }
            continue;
        }
        match op {
            Operation::Public(TxOp::HostedControl(control)) => controls.push(control),
            Operation::Public(TxOp::OwnedDeadline { .. } | TxOp::OwnedCheckWorldScope { .. }
                | TxOp::OwnedCheck { .. } | TxOp::OwnedCheckRange { .. } | TxOp::OwnedCheckWorldPurge { .. }) => (),
            Operation::Public(TxOp::OwnedReplaceDocs { relation, documents, valid_from, valid_to })
            | Operation::OwnedWriteIids { relation, documents, valid_from, valid_to, create_only: false, .. }
                if relation.is_root_data() && !close && !documents.rows().is_empty() =>
            {
                let start = match (valid_from, valid_to) {
                    (None, None) => None,
                    _ => Some(finite_landing_period(*valid_from, *valid_to)?),
                };
                for row in documents.rows() {
                    let id = row.get("_id")?.entity_id().ok()?;
                    assertion_count += 1;
                    if assertion_count > MAX_PROPOSAL_LAND_ROWS { return None; }
                    match writes.entry((relation.name(), id.handle_bytes())) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(ProposalLandingKey { relation, id, starts: vec![start], create_only: false });
                        }
                        std::collections::btree_map::Entry::Occupied(mut entry) => {
                            let starts = &mut entry.get_mut().starts;
                            if start.is_some() != starts[0].is_some() { return None; }
                            if !starts.contains(&start) { starts.push(start); }
                        }
                    }
                }
            }
            Operation::Public(TxOp::OwnedInsertDocs { relation, documents, valid_from: None, valid_to: None })
            | Operation::OwnedWriteIids { relation, documents, valid_from: None, valid_to: None, create_only: true, .. }
                if relation.is_root_data() && !close && !documents.rows().is_empty() =>
            {
                for row in documents.rows() {
                    let id = row.get("_id")?.entity_id().ok()?;
                    assertion_count += 1;
                    if writes.insert((relation.name(), id.handle_bytes()), ProposalLandingKey {
                        relation, id, starts: vec![None], create_only: true,
                    }).is_some()
                        || assertion_count > MAX_PROPOSAL_LAND_ROWS { return None; }
                }
            }
            Operation::Public(TxOp::OwnedArrowDocs { relation, documents, valid_from, valid_to, create_only })
            | Operation::OwnedArrowIids { relation, documents, valid_from, valid_to, create_only, .. }
                if relation.is_root_data() && !close && documents.row_count() != 0 =>
            {
                let start = match (valid_from, valid_to) {
                    (None, None) => None,
                    _ if !create_only => Some(finite_landing_period(*valid_from, *valid_to)?),
                    _ => return None,
                };
                for row in 0..documents.row_count() {
                    let id = documents.owned_id(row).ok()?;
                    assertion_count += 1;
                    if assertion_count > MAX_PROPOSAL_LAND_ROWS { return None; }
                    match writes.entry((relation.name(), id.handle_bytes())) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(ProposalLandingKey { relation, id, starts: vec![start], create_only: *create_only });
                        }
                        std::collections::btree_map::Entry::Occupied(mut entry) if !create_only => {
                            let starts = &mut entry.get_mut().starts;
                            if start.is_some() != starts[0].is_some() { return None; }
                            if !starts.contains(&start) { starts.push(start); }
                        }
                        _ => return None,
                    }
                }
            }
            Operation::Public(TxOp::OwnedDelete { relation, id, valid_from: None, valid_to: None })
                if relation.is_root_data() && writes.is_empty() => {
                close = true;
                writes.insert((relation.name(), id.handle_bytes()), ProposalLandingKey {
                    relation, id: id.clone(), starts: vec![None], create_only: false,
                });
            }
            Operation::Public(TxOp::OwnedPublishWorld {
                world, land_root: Some(pin), delete_root: None, advance_to: None,
                drop_world: true, expected, ..
            }) if landing.is_none() => landing = Some((*world, *pin, *expected)),
            _ => return None,
        }
    }
    let (world, pin, expected_clock) = landing?;
    if writes.is_empty() || frontiers.len() > writes.len() || close && !frontiers.is_empty() {
        return None;
    }
    let all_fixed_periods = writes.values().all(|key| key.starts[0].is_some());
    if operations.iter().filter_map(Operation::owned_relation)
        .any(|relation| relation.is_world_catalog() || relation.world().is_some_and(|other| other != world))
    {
        return None;
    }
    for ProposalLandingKey { relation, id, starts, create_only } in writes.into_values() {
        let root_checked = starts.iter().all(|start| operations.iter().any(|op| matches!(op,
            Operation::Public(TxOp::OwnedCheck { relation: checked, id: observed, valid_at, expected })
                if checked == relation && observed == &id
                    && start.map_or_else(|| expected.is_none() == create_only, |start| start == *valid_at)
        )));
        if close {
            return (root_checked && proposal_close_controls(controls)
                && !operations.iter().any(|op| match op {
                    Operation::Public(TxOp::OwnedCheckWorldPurge { .. }) => true,
                    _ => op.owned_relation().is_some_and(|relation| relation.world().is_some()),
                })).then_some(HostedProposalLanding {
                    world, pin, effect: ProposalLandingEffect::Close,
                });
        }
        let local = relation.in_world(world).ok()?;
        let local_checked = starts.iter().all(|start| operations.windows(2).any(|pair| matches!(pair, [
            Operation::Public(TxOp::OwnedCheckWorldPurge { relation: guarded, pin: original, .. }),
            Operation::Public(TxOp::OwnedCheck { relation: checked, id: observed, valid_at, expected: Some(revision) })
        ] if guarded == &local && checked == &local && observed == &id && *original == pin
            && start.is_none_or(|start| {
                start == *valid_at && expected_clock.is_some_and(|tip| *revision <= tip)
            }))));
        if starts[0].is_some() {
            let root_window = operations.iter().any(|op| matches!(op,
                Operation::Public(TxOp::OwnedCheckRange { relation: checked, region: None, .. }) if checked == relation
            ));
            let local_window = operations.iter().any(|op| matches!(op,
                Operation::Public(TxOp::OwnedCheckWorldPurge { relation: checked, pin: original, local_frontier: Some(_), .. })
                    if checked == &local && *original == pin
            ));
            if !root_window || !local_window { return None; }
        }
        if !root_checked || !local_checked { return None; }
    }
    (expected_clock.is_some() && proposal_land_controls(controls)).then_some(HostedProposalLanding {
        world,
        pin,
        effect: if all_fixed_periods { ProposalLandingEffect::FixedPeriods } else { ProposalLandingEffect::WithPoints },
    })
}
