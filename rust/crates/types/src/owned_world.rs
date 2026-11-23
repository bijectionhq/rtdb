use std::{collections::{BTreeMap, BTreeSet}, num::NonZeroU32};

use sha2::{Digest, Sha256};

use crate::{
    Anomaly, EntityId, Iid, OwnedRelation, OwnedSchema, Result, StructFields, Timestamp,
    TransactionKey, TxId, Value,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorldSchemas {
    encoded: Vec<u8>,
    fields: BTreeMap<String, OwnedSchema>,
}

impl WorldSchemas {
    pub const MAX_BYTES: usize = 1024 * 1024;

    pub fn new(fields: BTreeMap<String, OwnedSchema>) -> Result<Self> {
        let documents = fields
            .iter()
            .map(|(name, schema)| {
                OwnedRelation::new(name.as_str())?;
                let encoded =
                    String::from_utf8(schema.to_json()?).map_err(|_| OwnedWorld::unavailable())?;
                Ok((name.as_str(), encoded))
            })
            .collect::<Result<BTreeMap<_, _>>>()?;
        let encoded = serde_json::to_vec(&documents).map_err(|_| OwnedWorld::unavailable())?;
        if encoded.len() > Self::MAX_BYTES {
            return Err(OwnedWorld::unavailable());
        }
        Ok(Self { encoded, fields })
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > Self::MAX_BYTES {
            return Err(OwnedWorld::unavailable());
        }
        let documents: BTreeMap<String, String> =
            serde_json::from_slice(bytes).map_err(|_| OwnedWorld::unavailable())?;
        let fields = documents
            .into_iter()
            .map(|(name, json)| Ok((name, OwnedSchema::from_json(json.as_bytes())?)))
            .collect::<Result<_>>()?;
        let normalized = Self::new(fields)?;
        if normalized.encoded.as_slice() != bytes {
            return Err(OwnedWorld::unavailable());
        }
        Ok(normalized)
    }

    pub fn digest(&self) -> [u8; 32] {
        Sha256::digest(&self.encoded).into()
    }

    pub fn into_catalog_json(self) -> Vec<u8> {
        self.encoded
    }

    pub fn relations(&self) -> impl Iterator<Item = (&str, &OwnedSchema)> {
        self.fields.iter().map(|(name, schema)| (name.as_str(), schema))
    }

    pub fn relation(&self, relation: &OwnedRelation) -> Option<&OwnedSchema> {
        self.fields.get(relation.name())
    }

    pub fn includes(&self, required: &Self) -> bool {
        required.fields.iter().all(|(name, schema)| self.fields.get(name) == Some(schema))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum WorldClockMode {
    Retained,
    Held { start: Timestamp },
    Proposal,
}

/// The winning event's line and ordinal at the returned physical revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OwnedWorldAssertion {
    pub line: Option<Iid>,
    pub ordinal: NonZeroU32,
    pub held_reading: Option<Timestamp>,
}

impl WorldClockMode {
    pub fn is_writable(self) -> bool {
        self != Self::Retained
    }
}

/// An exact Held publication selected within independently retained media.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OwnedWorldAncestor {
    pub world: [u8; 16],
    pub pin: [u8; 16],
    pub capture: [u8; 16],
    pub suffix: [u8; 16],
    pub manifest: String,
    /// The exact end of the archived replica suffix.
    pub basis: TransactionKey,
    /// The selected application publication, which can precede the media end.
    pub system: Timestamp,
    pub source_root: TransactionKey,
    pub logical: Timestamp,
}

impl OwnedWorldAncestor {
    pub fn covers(&self, system: Timestamp, logical: Timestamp) -> bool {
        (system == self.system && logical == self.logical)
            || (system < self.system && logical < self.logical)
    }
}

/// One original checked parent registration and the inherited publication.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnedWorldForkLink {
    pub world: [u8; 16],
    pub pin: [u8; 16],
    pub registered: Timestamp,
    pub system: Timestamp,
    pub logical: Timestamp,
}

impl OwnedWorldForkLink {
    const WIDTH: usize = 56;

    fn encode(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&self.world);
        bytes.extend_from_slice(&self.pin);
        bytes.extend_from_slice(&self.registered.0.to_be_bytes());
        bytes.extend_from_slice(&self.system.0.to_be_bytes());
        bytes.extend_from_slice(&self.logical.0.to_be_bytes());
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let unavailable = OwnedWorld::unavailable;
        let read = |offset| -> Result<Timestamp> {
            let bytes = bytes.get(offset..offset + 8).ok_or_else(unavailable)?;
            Ok(Timestamp(i64::from_be_bytes(bytes.try_into().map_err(|_| unavailable())?)))
        };
        Ok(Self {
            world: bytes[..16].try_into().map_err(|_| unavailable())?,
            pin: bytes[16..32].try_into().map_err(|_| unavailable())?,
            registered: read(32)?,
            system: read(40)?,
            logical: read(48)?,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub struct WorldClockLimit {
    pub world: Iid,
    pub pin: Iid,
    pub before: Timestamp,
}

impl WorldClockLimit {
    pub fn admits(self, world: &OwnedWorld, reading: Timestamp) -> bool {
        self.world == world.id && self.pin == world.pin && !world.dropped && reading < self.before
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedWorld {
    pub id: Iid,
    pub pin: Iid,
    pub capture: Iid,
    pub ancestor: Option<OwnedWorldAncestor>,
    pub fork_lineage: Vec<OwnedWorldForkLink>,
    pub suffix: Option<Iid>,
    pub manifest: String,
    pub basis: TransactionKey,
    pub parent_publication: Option<Timestamp>,
    pub mode: WorldClockMode,
    pub until: Timestamp,
    pub schemas: WorldSchemas,
    pub dropped: bool,
}

impl OwnedWorld {
    pub const MAX_FORK_LINEAGE: usize = 8;

    /// Extend the independently retained certificate before publishing a child.
    pub fn retained_child_lineage(&self, next: &Self, registered: Timestamp) -> Result<Vec<OwnedWorldForkLink>> {
        let mut lineage = self.fork_lineage.clone();
        lineage.push(OwnedWorldForkLink {
            world: self.id.0,
            pin: self.pin.0,
            registered,
            system: next.inherited_system(),
            logical: next.inherited_clock(),
        });
        Self::validate_fork_lineage(next.id, next.mode, next.ancestor.as_ref(), next.inherited_clock(), &lineage)?;
        Ok(lineage)
    }

    fn validate_fork_lineage(
        id: Iid, mode: WorldClockMode, ancestor: Option<&OwnedWorldAncestor>,
        inherited: Timestamp, lineage: &[OwnedWorldForkLink],
    ) -> Result<()> {
        let unavailable = Self::unavailable;
        if lineage.len() > Self::MAX_FORK_LINEAGE {
            return Err(unavailable());
        }
        if lineage.is_empty() {
            return if ancestor.is_none() { Ok(()) } else { Err(unavailable()) };
        }
        if mode != WorldClockMode::Retained || lineage.last().is_none_or(|link| link.logical != inherited)
            || ancestor.is_some_and(|source| lineage.first().is_none_or(|link|
                link.world != source.world || link.pin != source.pin))
        {
            return Err(unavailable());
        }
        let mut worlds = BTreeSet::from([id.0]);
        let mut registered = Timestamp::MIN;
        for link in lineage {
            if link.world == [0; 16] || link.pin == [0; 16] || !worlds.insert(link.world)
                || link.registered <= registered || link.registered.0 <= 0 || link.registered == Timestamp::MAX
                || link.system.0 <= 0 || link.system == Timestamp::MAX
                || link.logical.0 <= 0 || link.logical == Timestamp::MAX
            {
                return Err(unavailable());
            }
            registered = link.registered;
        }
        Ok(())
    }

    /// The publication inherited when this world began its own journal.
    pub fn inherited_system(&self) -> Timestamp {
        self.ancestor.as_ref().map_or(self.basis.system_time, |ancestor| ancestor.system)
    }

    pub fn root_clock(&self) -> Timestamp {
        self.parent_publication.unwrap_or(self.basis.system_time)
    }

    pub fn inherited_clock(&self) -> Timestamp {
        self.ancestor.as_ref().map_or_else(|| self.root_clock(), |ancestor| ancestor.logical)
    }

    pub fn initial_clock(&self) -> Timestamp {
        match self.mode {
            WorldClockMode::Retained | WorldClockMode::Proposal => self.inherited_clock(),
            WorldClockMode::Held { start } => start,
        }
    }

    pub fn admits_clock_publication(&self, reading: Timestamp, revision: Timestamp) -> bool {
        self.initial_clock() < reading && reading < Timestamp::MAX && match self.mode {
            WorldClockMode::Held { .. } => true,
            WorldClockMode::Proposal => reading <= revision,
            WorldClockMode::Retained => false,
        }
    }

    pub fn is_held_root_fork(&self) -> bool {
        self.ancestor.is_none()
            && self.parent_publication == Some(self.basis.system_time)
            && self.mode == (WorldClockMode::Held { start: self.basis.system_time })
    }

    pub fn record(&self) -> StructFields<Value> {
        let (mode, start) = match self.mode {
            WorldClockMode::Retained => ("retained", Value::Null),
            WorldClockMode::Held { start } => ("held", Value::I64(start.0)),
            WorldClockMode::Proposal => ("proposal", Value::Null),
        };
        let mut row = StructFields::from([
            ("_id".into(), Value::Uuid(self.id.0)),
            ("pin".into(), Value::Uuid(self.pin.0)),
            ("capture".into(), Value::Uuid(self.capture.0)),
            ("manifest".into(), Value::Utf8(self.manifest.clone())),
            ("basis_id".into(), Value::I64(self.basis.tx_id.0)),
            ("basis_time".into(), Value::I64(self.basis.system_time.0)),
            ("parent_publication".into(), self.parent_publication.map_or(Value::Null, |at| Value::I64(at.0))),
            ("mode".into(), Value::Utf8(mode.into())),
            ("held_start".into(), start),
            ("until".into(), Value::I64(self.until.0)),
            ("dropped".into(), Value::Bool(self.dropped)),
            (
                "schemas".into(),
                Value::Binary(self.schemas.encoded.clone()),
            ),
        ]);
        if let Some(ancestor) = &self.ancestor {
            row.insert("ancestor".into(), Value::Struct(StructFields::from([
                ("world".into(), Value::Uuid(ancestor.world)),
                ("pin".into(), Value::Uuid(ancestor.pin)),
                ("capture".into(), Value::Uuid(ancestor.capture)),
                ("suffix".into(), Value::Uuid(ancestor.suffix)),
                ("manifest".into(), Value::Utf8(ancestor.manifest.clone())),
                ("basis_id".into(), Value::I64(ancestor.basis.tx_id.0)),
                ("basis_time".into(), Value::I64(ancestor.basis.system_time.0)),
                ("system".into(), Value::I64(ancestor.system.0)),
                ("source_root_id".into(), Value::I64(ancestor.source_root.tx_id.0)),
                ("source_root_time".into(), Value::I64(ancestor.source_root.system_time.0)),
                ("logical".into(), Value::I64(ancestor.logical.0)),
            ])));
        }
        if !self.fork_lineage.is_empty() {
            let mut bytes = Vec::with_capacity(self.fork_lineage.len() * OwnedWorldForkLink::WIDTH);
            for link in &self.fork_lineage {
                link.encode(&mut bytes);
            }
            row.insert("fork_lineage".into(), Value::Binary(bytes));
        }
        if let Some(suffix) = self.suffix {
            row.insert("suffix".into(), Value::Uuid(suffix.0));
        }
        row
    }

    pub fn from_record(row: &StructFields<Value>) -> Result<Self> {
        let (
            Some(Value::Uuid(id)),
            Some(Value::Uuid(pin)),
            Some(Value::Uuid(capture)),
            Some(Value::Utf8(manifest)),
            Some(Value::I64(tx_id)),
            Some(Value::I64(time)),
            Some(Value::I64(until)),
            Some(Value::Binary(schemas)),
        ) = (
            row.get("_id"),
            row.get("pin"),
            row.get("capture"),
            row.get("manifest"),
            row.get("basis_id"),
            row.get("basis_time"),
            row.get("until"),
            row.get("schemas"),
        )
        else {
            return Err(Self::unavailable());
        };
        let parent_publication = match row.get("parent_publication") {
            Some(Value::Null) => None,
            Some(Value::I64(at)) if Timestamp::MIN.0 < *at && *at <= *time => Some(Timestamp(*at)),
            _ => return Err(Self::unavailable()),
        };
        let inherited = parent_publication.unwrap_or(Timestamp(*time));
        let mode = match (row.get("mode"), row.get("held_start")) {
            (Some(Value::Utf8(mode)), Some(Value::Null)) if mode == "retained" => WorldClockMode::Retained,
            (Some(Value::Utf8(mode)), Some(Value::Null)) if mode == "proposal" => WorldClockMode::Proposal,
            (Some(Value::Utf8(mode)), Some(Value::I64(start))) if mode == "held"
                && *start >= inherited.0 && *start < Timestamp::MAX.0 => WorldClockMode::Held { start: Timestamp(*start) },
            _ => return Err(Self::unavailable()),
        };
        let Some(Value::Bool(dropped)) = row.get("dropped") else { return Err(Self::unavailable()); };
        let suffix = match row.get("suffix") {
            None | Some(Value::Null | Value::Undefined) => None,
            Some(Value::Uuid(id)) if *id != [0; 16] => Some(Iid(*id)),
            _ => return Err(Self::unavailable()),
        };
        let ancestor = match row.get("ancestor") {
            None | Some(Value::Null | Value::Undefined) => None,
            Some(Value::Struct(fields)) if fields.len() == 11 => {
                let uuid = |name| match fields.get(name) {
                    Some(Value::Uuid(id)) if *id != [0; 16] => Ok(*id),
                    _ => Err(Self::unavailable()),
                };
                let integer = |name| match fields.get(name) {
                    Some(Value::I64(value)) => Ok(*value),
                    _ => Err(Self::unavailable()),
                };
                let manifest = match fields.get("manifest") {
                    Some(Value::Utf8(manifest)) if valid_manifest(manifest) => manifest.clone(),
                    _ => return Err(Self::unavailable()),
                };
                let basis = TransactionKey {
                    tx_id: TxId(integer("basis_id")?),
                    system_time: Timestamp(integer("basis_time")?),
                };
                let source_root = TransactionKey {
                    tx_id: TxId(integer("source_root_id")?),
                    system_time: Timestamp(integer("source_root_time")?),
                };
                let logical = Timestamp(integer("logical")?);
                let system = Timestamp(integer("system")?);
                let world = uuid("world")?;
                if world == *id || *pin != *id || mode != WorldClockMode::Retained
                    || basis.tx_id.0 <= *tx_id || basis.system_time.0 <= *time
                    || basis.system_time.0 >= *until || system.0 <= *time || system > basis.system_time
                    || logical <= inherited || logical == Timestamp::MAX
                    || source_root.tx_id.0 < 0 || source_root.system_time.0 <= 0 || source_root.system_time.0 > *time
                {
                    return Err(Self::unavailable());
                }
                Some(OwnedWorldAncestor { world, pin: uuid("pin")?, capture: uuid("capture")?,
                    suffix: uuid("suffix")?, manifest, basis, system, source_root, logical })
            }
            _ => return Err(Self::unavailable()),
        };
        let fork_lineage = match row.get("fork_lineage") {
            None | Some(Value::Null | Value::Undefined) => Vec::new(),
            Some(Value::Binary(bytes)) if !bytes.is_empty() && bytes.len() % OwnedWorldForkLink::WIDTH == 0
                && bytes.len() / OwnedWorldForkLink::WIDTH <= Self::MAX_FORK_LINEAGE =>
            {
                bytes.chunks_exact(OwnedWorldForkLink::WIDTH).map(OwnedWorldForkLink::decode).collect::<Result<Vec<_>>>()?
            }
            _ => return Err(Self::unavailable()),
        };
        let inherited = ancestor.as_ref().map_or(inherited, |overlay| overlay.logical);
        Self::validate_fork_lineage(Iid(*id), mode, ancestor.as_ref(), inherited, &fork_lineage)?;
        if row.len() != 12 + usize::from(row.contains_key("suffix")) + usize::from(row.contains_key("ancestor"))
            + usize::from(row.contains_key("fork_lineage"))
            || *id == [0; 16] || *pin == [0; 16]
            || *capture == [0; 16]
            || *until <= *time
            || !valid_manifest(manifest)
        {
            return Err(Self::unavailable());
        }
        Ok(Self {
            ancestor,
            fork_lineage,
            id: Iid(*id),
            pin: Iid(*pin),
            capture: Iid(*capture),
            suffix,
            manifest: manifest.clone(),
            basis: TransactionKey {
                tx_id: TxId(*tx_id),
                system_time: Timestamp(*time),
            },
            parent_publication,
            mode,
            until: Timestamp(*until),
            schemas: WorldSchemas::from_bytes(schemas)?,
            dropped: *dropped,
        })
    }

    pub fn can_move_to(&self, next: &Self) -> bool {
        self.until == next.until && self.can_advance_to(next)
    }

    pub fn can_advance_to(&self, next: &Self) -> bool {
        self.ancestor.is_none() && next.ancestor.is_none() && self.id == next.id
            && self.fork_lineage.is_empty() && next.fork_lineage.is_empty()
            && self.pin != next.pin
            && !self.dropped && !next.dropped
            && self.mode == WorldClockMode::Retained && next.mode == WorldClockMode::Retained
            && self.basis.system_time < next.basis.system_time
            && self.basis.tx_id < next.basis.tx_id
    }

    pub fn can_renew_to(&self, next: &Self) -> bool {
        self.id == next.id && self.shares_retained_archive(next)
    }

    pub fn can_seed_sibling(&self, next: &Self) -> bool {
        self.id != next.id && next.id == next.pin && self.fork_lineage.is_empty() && self.shares_retained_archive(next)
    }

    /// Retain the exact Root publication already held by this world.
    pub fn can_retain_parent(&self, next: &Self) -> bool {
        self.ancestor.is_none()
            && next.ancestor.is_none()
            && self.mode != WorldClockMode::Proposal
            && self.parent_publication.is_some()
            && self.id != next.id
            && next.id == next.pin
            && !self.dropped && !next.dropped
            && next.mode == WorldClockMode::Retained
            && next.until <= self.until
            && self.same_parent_archive(next)
    }

    /// A retained child takes its own custody of the source's exact media.
    pub fn can_retain_overlay(&self, next: &Self) -> bool {
        let Some(overlay) = &next.ancestor else { return false; };
        let same_overlay = match &self.ancestor {
            Some(inherited) => self.mode == WorldClockMode::Retained
                && inherited.world == overlay.world && inherited.pin == overlay.pin
                && inherited.capture == overlay.capture && inherited.suffix == overlay.suffix
                && inherited.manifest == overlay.manifest && inherited.basis == overlay.basis
                && inherited.source_root == overlay.source_root
                && inherited.covers(overlay.system, overlay.logical),
            None => matches!(self.mode, WorldClockMode::Held { .. })
                && self.id.0 == overlay.world && self.pin.0 == overlay.pin && self.basis == overlay.source_root
                && overlay.system == overlay.basis.system_time,
        };
        same_overlay && self.id != next.id && next.id == next.pin && !self.dropped && !next.dropped
            && next.mode == WorldClockMode::Retained && next.until <= self.until
            && self.same_parent_archive(next)
    }

    fn shares_retained_archive(&self, next: &Self) -> bool {
        self.ancestor.is_none() && next.ancestor.is_none() && self.pin != next.pin && !self.dropped && !next.dropped
            && self.fork_lineage == next.fork_lineage
            && self.mode == WorldClockMode::Retained && next.mode == WorldClockMode::Retained
            && self.same_parent_archive(next)
    }

    fn same_parent_archive(&self, next: &Self) -> bool {
        self.capture == next.capture && self.suffix == next.suffix
            && self.manifest == next.manifest && self.basis == next.basis
            && self.parent_publication == next.parent_publication && self.schemas == next.schemas
    }

    pub fn can_hold_proposal(&self, next: &Self) -> bool {
        self.shares_proposal_archive(next) && self.until <= next.until
    }

    pub fn can_shorten_proposal(&self, next: &Self) -> bool {
        self.shares_proposal_archive(next) && next.until < self.until
    }

    fn shares_proposal_archive(&self, next: &Self) -> bool {
        self.id == next.id && self.pin != next.pin && !self.dropped && !next.dropped
            && self.mode == WorldClockMode::Proposal && next.mode == self.mode
            && self.capture == next.capture && self.suffix == next.suffix
            && self.manifest == next.manifest && self.basis == next.basis
            && self.parent_publication == next.parent_publication && self.schemas == next.schemas
    }

    pub fn same_fork(&self, other: &Self) -> bool {
        self.id == other.id && self.ancestor == other.ancestor && self.fork_lineage == other.fork_lineage
            && self.pin == other.pin
            && self.capture == other.capture
            && self.suffix == other.suffix
            && self.manifest == other.manifest
            && self.basis == other.basis
            && self.parent_publication == other.parent_publication
            && self.mode == other.mode
            && self.until == other.until
            && self.schemas == other.schemas
    }

    pub fn unavailable() -> Anomaly {
        Anomaly::incorrect("rtdb.world/unavailable", "The owned world is unavailable")
    }
}

fn valid_manifest(manifest: &str) -> bool {
    manifest.len() == 43
        && manifest.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

/// Every local fact is either a complete replacement or a tombstone.
#[derive(Clone, Debug, PartialEq)]
pub enum WorldShadow {
    Present(StructFields<Value>),
    Deleted,
}

impl WorldShadow {
    pub const FIELDS: &'static str = "_world_fields";

    pub fn allocation_bound(&self) -> Result<usize> {
        match self {
            Self::Present(row) => Value::document_retained_bytes(row)?
                .checked_mul(8)
                .and_then(|bytes| bytes.checked_add(4096))
                .ok_or_else(OwnedWorld::unavailable),
            Self::Deleted => Ok(4096),
        }
    }

    pub fn record(self, id: &EntityId) -> Result<StructFields<Value>> {
        let (mut row, fields) = match self {
            Self::Present(row) => {
                if row.get("_id").map(Value::entity_id).transpose()?.as_ref() != Some(id)
                    || row.keys().any(|name| name != "_id" && name.starts_with('_'))
                {
                    return Err(OwnedWorld::unavailable());
                }
                let fields = row.keys().filter(|name| *name != "_id")
                    .cloned().map(Value::Utf8).collect();
                (row, Value::list(fields))
            }
            Self::Deleted => (StructFields::from([("_id".into(), id.as_value())]), Value::Null),
        };
        row.insert(Self::FIELDS.into(), fields);
        Ok(row)
    }

    pub fn is_tombstone(row: &StructFields<Value>) -> bool {
        matches!(row.get(Self::FIELDS), Some(Value::Null))
    }

    pub fn from_record(mut row: StructFields<Value>) -> Result<Self> {
        let unavailable = OwnedWorld::unavailable;
        let identity = row.remove("_id").ok_or_else(unavailable)?;
        identity.entity_id()?;
        let fields = row.remove(Self::FIELDS).ok_or_else(unavailable)?;
        let fact = match fields {
            Value::Null => Self::Deleted,
            Value::List(fields) => {
                let mut fact = StructFields::from([("_id".into(), identity)]);
                for field in fields.into_values() {
                    let Value::Utf8(name) = field else { return Err(unavailable()); };
                    if name.starts_with('_') { return Err(unavailable()); }
                    let value = row.remove(&name).ok_or_else(unavailable)?;
                    if matches!(value, Value::Undefined) || fact.insert(name, value).is_some() {
                        return Err(unavailable());
                    }
                }
                Self::Present(fact)
            }
            _ => return Err(unavailable()),
        };
        if row.values().any(|value| !matches!(value, Value::Null | Value::Undefined)) {
            return Err(unavailable());
        }
        Ok(fact)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn held_parent_retention_preserves_the_original_root_and_lease() -> Result<()> {
        let source = OwnedWorld {
            id: Iid([1; 16]), pin: Iid([1; 16]), capture: Iid([2; 16]), suffix: None,
            ancestor: None, fork_lineage: Vec::new(), manifest: "a".repeat(43),
            basis: TransactionKey { tx_id: TxId(10), system_time: Timestamp(100) },
            parent_publication: Some(Timestamp(100)), mode: WorldClockMode::Held { start: Timestamp(120) },
            until: Timestamp(300), schemas: WorldSchemas::new(BTreeMap::new())?, dropped: false,
        };
        let child = OwnedWorld {
            id: Iid([3; 16]), pin: Iid([3; 16]), until: Timestamp(200),
            mode: WorldClockMode::Retained, ..source.clone()
        };
        assert!(source.admits_clock_publication(Timestamp(150), Timestamp(140)));
        assert!(!source.admits_clock_publication(Timestamp(120), Timestamp(140)));
        assert!(!source.admits_clock_publication(Timestamp::MAX, Timestamp(140)));
        let proposal = OwnedWorld { mode: WorldClockMode::Proposal, ..source.clone() };
        assert!(!proposal.admits_clock_publication(Timestamp(150), Timestamp(140)));
        assert!(!source.is_held_root_fork());
        let exact = OwnedWorld { mode: WorldClockMode::Held { start: source.basis.system_time }, ..source.clone() };
        assert!(OwnedWorld::from_record(&exact.record())?.is_held_root_fork());
        assert!(!OwnedWorld { parent_publication: None, ..exact }.is_held_root_fork());
        assert!(source.can_retain_parent(&child));
        assert_eq!(child.initial_clock(), Timestamp(100));
        assert!(!source.can_retain_parent(&OwnedWorld { until: Timestamp(301), ..child.clone() }));
        assert!(!source.can_retain_parent(&OwnedWorld { capture: Iid([4; 16]), ..child.clone() }));
        let dropped = OwnedWorld { dropped: true, ..source };
        assert!(!dropped.can_retain_parent(&child));
        Ok(())
    }

    #[test]
    fn exact_held_ancestor_requires_independent_retained_child() -> Result<()> {
        let mut child = OwnedWorld {
            id: Iid([1; 16]), pin: Iid([1; 16]), capture: Iid([2; 16]), suffix: None,
            ancestor: Some(OwnedWorldAncestor {
                world: [3; 16], pin: [3; 16], capture: [4; 16], suffix: [5; 16],
                manifest: "b".repeat(43),
                basis: TransactionKey { tx_id: TxId(20), system_time: Timestamp(200) },
                system: Timestamp(200),
                source_root: TransactionKey { tx_id: TxId(10), system_time: Timestamp(100) },
                logical: Timestamp(150),
            }),
            fork_lineage: vec![OwnedWorldForkLink {
                world: [3; 16], pin: [3; 16], registered: Timestamp(125),
                system: Timestamp(200), logical: Timestamp(150),
            }],
            manifest: "a".repeat(43),
            basis: TransactionKey { tx_id: TxId(10), system_time: Timestamp(100) },
            parent_publication: Some(Timestamp(100)), mode: WorldClockMode::Retained,
            until: Timestamp(300), schemas: WorldSchemas::new(BTreeMap::new())?, dropped: false,
        };
        let stored = child.record();
        assert_eq!(OwnedWorld::from_record(&stored)?, child);
        assert_eq!(child.initial_clock(), Timestamp(150));

        assert_eq!(child.inherited_system(), Timestamp(200));
        let limit = WorldClockLimit { world: child.id, pin: child.pin, before: Timestamp(175) };
        assert!(limit.admits(&child, Timestamp(150)));
        assert!(limit.admits(&child, Timestamp(174)));
        assert!(!limit.admits(&child, Timestamp(175)));

        let mut descendant = child.clone();
        descendant.id = Iid([6; 16]);
        descendant.pin = descendant.id;
        descendant.until = Timestamp(250);
        assert!(child.can_retain_overlay(&descendant));
        assert!(!limit.admits(&descendant, Timestamp(150)));
        assert_eq!(descendant.ancestor, child.ancestor);
        let selection = descendant.ancestor.as_mut().unwrap();
        selection.system = Timestamp(190);
        selection.logical = Timestamp(149);
        assert!(child.can_retain_overlay(&descendant));
        descendant.fork_lineage = child.retained_child_lineage(&descendant, Timestamp(225))?;
        let reopened = OwnedWorld::from_record(&descendant.record())?;
        assert_eq!(reopened.inherited_system(), Timestamp(190));
        assert_eq!(&reopened.fork_lineage[..1], child.fork_lineage.as_slice());
        assert_eq!(reopened.fork_lineage[1].world, child.id.0);
        assert_eq!(reopened.fork_lineage[1].registered, Timestamp(225));
        assert_eq!(reopened.fork_lineage[1].logical, Timestamp(149));
        descendant.ancestor.as_mut().unwrap().logical = Timestamp(150);
        assert!(!child.can_retain_overlay(&descendant));
        descendant.ancestor = child.ancestor.clone();
        descendant.ancestor.as_mut().unwrap().suffix = [7; 16];
        assert!(!child.can_retain_overlay(&descendant));
        descendant.ancestor = child.ancestor.clone();
        descendant.until = Timestamp(301);
        assert!(!child.can_retain_overlay(&descendant));
        descendant.until = Timestamp(250);
        child.dropped = true;
        assert!(!child.can_retain_overlay(&descendant));
        child.dropped = false;

        child.mode = WorldClockMode::Held { start: Timestamp(150) };
        assert!(OwnedWorld::from_record(&child.record()).is_err());

        child.mode = WorldClockMode::Retained;
        child.ancestor.as_mut().expect("fixture contains an ancestor").logical = Timestamp(201);
        assert!(OwnedWorld::from_record(&child.record()).is_err());
        child.fork_lineage[0].logical = Timestamp(201);
        let retained = OwnedWorld::from_record(&child.record())?;
        assert_eq!(retained.initial_clock(), Timestamp(201));
        assert_eq!(retained.inherited_system(), Timestamp(200));
        child.ancestor.as_mut().unwrap().logical = Timestamp::MAX;
        child.fork_lineage[0].logical = Timestamp::MAX;
        assert!(OwnedWorld::from_record(&child.record()).is_err());
        Ok(())
    }
}
