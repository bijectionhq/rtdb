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
    pub publication: TransactionKey,
    pub source_root: TransactionKey,
    pub logical: Timestamp,
}

impl OwnedWorldAncestor {
    const WIDTH: usize = 163;

    fn encode(&self, bytes: &mut Vec<u8>) {
        bytes.extend_from_slice(&self.world);
        bytes.extend_from_slice(&self.pin);
        bytes.extend_from_slice(&self.capture);
        bytes.extend_from_slice(&self.suffix);
        bytes.extend_from_slice(self.manifest.as_bytes());
        for value in [self.basis.tx_id.0, self.basis.system_time.0, self.publication.tx_id.0, self.publication.system_time.0,
            self.source_root.tx_id.0, self.source_root.system_time.0, self.logical.0]
        {
            bytes.extend_from_slice(&value.to_be_bytes());
        }
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let unavailable = OwnedWorld::unavailable;
        let uuid = |offset| {
            let id: [u8; 16] = bytes[offset..offset + 16].try_into().map_err(|_| unavailable())?;
            if id == [0; 16] { return Err(unavailable()); }
            Ok(id)
        };
        let number = |offset: usize| {
            i64::from_be_bytes(bytes[107 + offset * 8..115 + offset * 8].try_into().expect("fixed ancestor width"))
        };
        let manifest = std::str::from_utf8(&bytes[64..107]).map_err(|_| unavailable())?;
        if !valid_manifest(manifest) { return Err(unavailable()); }
        Ok(Self {
            world: uuid(0)?, pin: uuid(16)?, capture: uuid(32)?, suffix: uuid(48)?,
            manifest: manifest.to_owned(),
            basis: TransactionKey { tx_id: TxId(number(0)), system_time: Timestamp(number(1)) },
            publication: TransactionKey { tx_id: TxId(number(2)), system_time: Timestamp(number(3)) },
            source_root: TransactionKey { tx_id: TxId(number(4)), system_time: Timestamp(number(5)) },
            logical: Timestamp(number(6)),
        })
    }

    fn holds_same_media(&self, other: &Self) -> bool {
        self.world == other.world && self.pin == other.pin
            && self.capture == other.capture && self.suffix == other.suffix
            && self.manifest == other.manifest && self.basis == other.basis
            && self.source_root == other.source_root
    }

    pub fn covers(&self, publication: TransactionKey, logical: Timestamp) -> bool {
        (publication == self.publication && logical == self.logical)
            || (publication.tx_id < self.publication.tx_id && publication.at_or_before(self.publication) && logical < self.logical)
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
    pub ancestors: Vec<OwnedWorldAncestor>,
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
    /// Bound the retained certificate by encoded bytes.
    pub const MAX_FORK_CERTIFICATE_BYTES: usize = 32 * 1024;

    pub fn ancestor(&self) -> Option<&OwnedWorldAncestor> {
        self.ancestors.last()
    }

    /// Select the first frozen writer containing a fresh SQL as-of selector.
    pub fn ancestor_at_system(&self, system: Timestamp) -> Option<&OwnedWorldAncestor> {
        if system <= self.basis.system_time { return None; }
        self.ancestors.iter().find(|layer| system <= layer.publication.system_time)
    }

    pub fn ancestor_covering(&self, publication: TransactionKey) -> Option<&OwnedWorldAncestor> {
        if publication.tx_id <= self.basis.tx_id { return None; }
        self.ancestors.iter().find(|layer| publication.at_or_before(layer.publication))
    }

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
        Self::validate_fork_lineage(next.id, next.mode, &next.ancestors, next.inherited_clock(), &lineage)?;
        Ok(lineage)
    }

    fn validate_fork_lineage(
        id: Iid, mode: WorldClockMode, ancestors: &[OwnedWorldAncestor],
        inherited: Timestamp, lineage: &[OwnedWorldForkLink],
    ) -> Result<()> {
        let unavailable = Self::unavailable;
        if lineage.len() * OwnedWorldForkLink::WIDTH + ancestors.len() * OwnedWorldAncestor::WIDTH > Self::MAX_FORK_CERTIFICATE_BYTES {
            return Err(unavailable());
        }
        if lineage.is_empty() {
            return if ancestors.is_empty() { Ok(()) } else { Err(unavailable()) };
        }
        let admitted_mode = mode == WorldClockMode::Retained
            || mode == (WorldClockMode::Held { start: inherited });
        if !admitted_mode || lineage.last().is_none_or(|link| link.logical != inherited)
            || ancestors.iter().any(|source| !lineage.iter().any(|link|
                link.world == source.world && link.pin == source.pin))
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
        self.inherited_position().system_time
    }

    pub fn inherited_position(&self) -> TransactionKey {
        self.ancestor().map_or(self.basis, |ancestor| ancestor.publication)
    }

    pub fn root_clock(&self) -> Timestamp {
        self.parent_publication.unwrap_or(self.basis.system_time)
    }

    pub fn inherited_clock(&self) -> Timestamp {
        self.ancestor().map_or_else(|| self.root_clock(), |ancestor| ancestor.logical)
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
        self.ancestors.is_empty()
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
        if !self.ancestors.is_empty() {
            let mut bytes = Vec::with_capacity(self.ancestors.len() * OwnedWorldAncestor::WIDTH);
            for layer in &self.ancestors {
                layer.encode(&mut bytes);
            }
            row.insert("ancestors".into(), Value::Binary(bytes));
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
        let ancestors = match row.get("ancestors") {
            None | Some(Value::Null | Value::Undefined) => Vec::new(),
            Some(Value::Binary(bytes)) if !bytes.is_empty() && bytes.len() % OwnedWorldAncestor::WIDTH == 0
                && bytes.len() <= Self::MAX_FORK_CERTIFICATE_BYTES =>
            {
                bytes.chunks_exact(OwnedWorldAncestor::WIDTH).map(OwnedWorldAncestor::decode).collect::<Result<Vec<_>>>()?
            }
            _ => return Err(Self::unavailable()),
        };
        let mut writers = BTreeSet::from([*id]);
        let mut previous = TransactionKey { tx_id: TxId(*tx_id), system_time: Timestamp(*time) };
        let mut previous_logical = inherited;
        for layer in &ancestors {
            if !writers.insert(layer.world)
                || *pin != *id || layer.basis.tx_id.0 <= *tx_id
                || layer.basis.system_time.0 >= *until
                || layer.publication.tx_id <= previous.tx_id || !previous.at_or_before(layer.publication)
                || !layer.publication.at_or_before(layer.basis)
                || layer.logical <= previous_logical || layer.logical == Timestamp::MAX
                || layer.source_root.tx_id.0 < 0 || layer.source_root.system_time.0 <= 0
                || layer.source_root.system_time.0 > *time
            {
                return Err(Self::unavailable());
            }
            previous = layer.publication;
            previous_logical = layer.logical;
        }
        let fork_lineage = match row.get("fork_lineage") {
            None | Some(Value::Null | Value::Undefined) => Vec::new(),
            Some(Value::Binary(bytes)) if !bytes.is_empty() && bytes.len() % OwnedWorldForkLink::WIDTH == 0
                && bytes.len() <= Self::MAX_FORK_CERTIFICATE_BYTES =>
            {
                bytes.chunks_exact(OwnedWorldForkLink::WIDTH).map(OwnedWorldForkLink::decode).collect::<Result<Vec<_>>>()?
            }
            _ => return Err(Self::unavailable()),
        };
        Self::validate_fork_lineage(Iid(*id), mode, &ancestors, previous_logical, &fork_lineage)?;
        if row.len() != 12 + usize::from(row.contains_key("suffix")) + usize::from(row.contains_key("ancestors"))
            + usize::from(row.contains_key("fork_lineage"))
            || *id == [0; 16] || *pin == [0; 16]
            || *capture == [0; 16]
            || *until <= *time
            || !valid_manifest(manifest)
        {
            return Err(Self::unavailable());
        }
        Ok(Self {
            ancestors,
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
        self.ancestors.is_empty() && next.ancestors.is_empty() && self.id == next.id
            && self.fork_lineage.is_empty() && next.fork_lineage.is_empty()
            && self.pin != next.pin
            && !self.dropped && !next.dropped
            && self.mode == WorldClockMode::Retained && next.mode == WorldClockMode::Retained
            && self.basis.system_time <= next.basis.system_time
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
        self.ancestors.is_empty()
            && next.ancestors.is_empty()
            && self.mode != WorldClockMode::Proposal
            && self.parent_publication.is_some()
            && self.id != next.id
            && next.id == next.pin
            && !self.dropped && !next.dropped
            && (next.mode == WorldClockMode::Retained
                || next.mode == (WorldClockMode::Held { start: self.inherited_clock() }))
            && next.until <= self.until
            && self.same_parent_archive(next)
    }

    /// A child owns the complete selected prefix, optionally freezing this writer.
    pub fn can_retain_overlay(&self, next: &Self) -> bool {
        let Some((overlay, prefix)) = next.ancestors.split_last() else { return false; };
        let source = if prefix == self.ancestors.as_slice() {
            matches!(self.mode, WorldClockMode::Held { .. })
                && self.id.0 == overlay.world && self.pin.0 == overlay.pin && self.basis == overlay.source_root
                && overlay.publication == overlay.basis
                && self.inherited_position().tx_id < overlay.publication.tx_id
                && self.inherited_position().at_or_before(overlay.publication) && overlay.logical > self.initial_clock()
        } else {
            self.mode != WorldClockMode::Proposal
                && self.ancestors.get(..prefix.len()) == Some(prefix)
                && self.ancestors.get(prefix.len()).is_some_and(|inherited|
                    inherited.holds_same_media(overlay) && inherited.covers(overlay.publication, overlay.logical))
        };
        source && self.id != next.id && next.id == next.pin && !self.dropped && !next.dropped
            && (next.mode == WorldClockMode::Retained
                || next.mode == (WorldClockMode::Held { start: overlay.logical }))
            && next.until <= self.until && self.same_parent_archive(next)
    }

    fn shares_retained_archive(&self, next: &Self) -> bool {
        self.ancestors.is_empty() && next.ancestors.is_empty() && self.pin != next.pin && !self.dropped && !next.dropped
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
        self.id == other.id && self.ancestors == other.ancestors && self.fork_lineage == other.fork_lineage
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
            ancestors: Vec::new(), fork_lineage: Vec::new(), manifest: "a".repeat(43),
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
        let mut writable = OwnedWorld {
            mode: WorldClockMode::Held { start: Timestamp(100) }, ..child.clone()
        };
        assert!(source.can_retain_parent(&writable));
        writable.fork_lineage = source.retained_child_lineage(&writable, Timestamp(130))?;
        assert_eq!(OwnedWorld::from_record(&writable.record())?, writable);
        writable.mode = WorldClockMode::Held { start: Timestamp(101) };
        assert!(!source.can_retain_parent(&writable));
        assert!(OwnedWorld::from_record(&writable.record()).is_err());
        assert!(!source.can_retain_parent(&OwnedWorld { until: Timestamp(301), ..child.clone() }));
        assert!(!source.can_retain_parent(&OwnedWorld { capture: Iid([4; 16]), ..child.clone() }));
        let dropped = OwnedWorld { dropped: true, ..source };
        assert!(!dropped.can_retain_parent(&child));
        Ok(())
    }

    #[test]
    fn writable_forks_freeze_each_writer_and_can_select_an_earlier_prefix() -> Result<()> {
        let mut source = OwnedWorld {
            id: Iid([1; 16]), pin: Iid([1; 16]), capture: Iid([31; 16]), suffix: None,
            ancestors: Vec::new(), fork_lineage: Vec::new(), manifest: "a".repeat(43),
            basis: TransactionKey { tx_id: TxId(1), system_time: Timestamp(10) },
            parent_publication: Some(Timestamp(10)), mode: WorldClockMode::Held { start: Timestamp(10) },
            until: Timestamp(600), schemas: WorldSchemas::new(BTreeMap::new())?, dropped: false,
        };
        for writer in 1..=12u8 {
            let system = Timestamp(20 * i64::from(writer) + 10);
            let mut next = source.clone();
            next.id = Iid([writer + 1; 16]);
            next.pin = next.id;
            next.mode = WorldClockMode::Held { start: system };
            next.ancestors.push(OwnedWorldAncestor {
                world: source.id.0, pin: source.pin.0, capture: [31; 16], suffix: [32 + writer; 16],
                manifest: "b".repeat(43), basis: TransactionKey { tx_id: TxId(i64::from(writer) * 10 + 1), system_time: system },
                publication: TransactionKey { tx_id: TxId(i64::from(writer) * 10 + 1), system_time: system },
                source_root: source.basis, logical: system,
            });
            assert!(source.can_retain_overlay(&next));
            next.fork_lineage = source.retained_child_lineage(&next, Timestamp(system.0 - 1))?;
            assert_eq!(OwnedWorld::from_record(&next.record())?, next);
            source = next;
        }
        assert_eq!(source.ancestor_at_system(Timestamp(55)).unwrap().world, [3; 16]);
        let earlier = TransactionKey { tx_id: TxId(30), system_time: Timestamp(70) };
        let next = TransactionKey { tx_id: TxId(32), system_time: Timestamp(70) };
        let layer = source.ancestor_covering(earlier).unwrap();
        assert_eq!(layer.world, [3; 16]);
        assert!(layer.covers(earlier, Timestamp(65)));
        assert!(!layer.covers(next, Timestamp(65)));
        let mut child = source.clone();
        child.id = Iid([14; 16]);
        child.pin = child.id;
        child.ancestors.truncate(3);
        let selected = child.ancestors.last_mut().unwrap();
        selected.publication = TransactionKey { tx_id: TxId(25), system_time: Timestamp(55) };
        selected.logical = Timestamp(54);
        child.mode = WorldClockMode::Held { start: Timestamp(54) };
        assert!(source.can_retain_overlay(&child));
        child.fork_lineage = source.retained_child_lineage(&child, Timestamp(255))?;
        assert_eq!(OwnedWorld::from_record(&child.record())?, child);
        child.ancestors.remove(1);
        assert!(!source.can_retain_overlay(&child));
        Ok(())
    }

    #[test]
    fn exact_held_ancestor_requires_independent_retained_child() -> Result<()> {
        let mut child = OwnedWorld {
            id: Iid([1; 16]), pin: Iid([1; 16]), capture: Iid([2; 16]), suffix: None,
            ancestors: vec![OwnedWorldAncestor {
                world: [3; 16], pin: [3; 16], capture: [4; 16], suffix: [5; 16],
                manifest: "b".repeat(43),
                basis: TransactionKey { tx_id: TxId(20), system_time: Timestamp(200) },
                publication: TransactionKey { tx_id: TxId(20), system_time: Timestamp(200) },
                source_root: TransactionKey { tx_id: TxId(10), system_time: Timestamp(100) },
                logical: Timestamp(150),
            }],
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
        assert_eq!(descendant.ancestors, child.ancestors);
        let selection = descendant.ancestors.last_mut().unwrap();
        selection.publication = TransactionKey { tx_id: TxId(19), system_time: Timestamp(190) };
        selection.logical = Timestamp(149);
        assert!(child.can_retain_overlay(&descendant));
        descendant.fork_lineage = child.retained_child_lineage(&descendant, Timestamp(225))?;
        let reopened = OwnedWorld::from_record(&descendant.record())?;
        assert_eq!(reopened.inherited_system(), Timestamp(190));
        assert_eq!(&reopened.fork_lineage[..1], child.fork_lineage.as_slice());
        assert_eq!(reopened.fork_lineage[1].world, child.id.0);
        assert_eq!(reopened.fork_lineage[1].registered, Timestamp(225));
        assert_eq!(reopened.fork_lineage[1].logical, Timestamp(149));
        descendant.ancestors.last_mut().unwrap().logical = Timestamp(150);
        assert!(!child.can_retain_overlay(&descendant));
        descendant.ancestors = child.ancestors.clone();
        descendant.ancestors.last_mut().unwrap().suffix = [7; 16];
        assert!(!child.can_retain_overlay(&descendant));
        descendant.ancestors = child.ancestors.clone();
        descendant.until = Timestamp(301);
        assert!(!child.can_retain_overlay(&descendant));
        descendant.until = Timestamp(250);
        child.dropped = true;
        assert!(!child.can_retain_overlay(&descendant));
        child.dropped = false;

        child.mode = WorldClockMode::Held { start: Timestamp(150) };
        assert_eq!(OwnedWorld::from_record(&child.record())?, child);
        child.mode = WorldClockMode::Held { start: Timestamp(151) };
        assert!(OwnedWorld::from_record(&child.record()).is_err());

        child.mode = WorldClockMode::Retained;
        child.ancestors.last_mut().expect("fixture contains an ancestor").logical = Timestamp(201);
        assert!(OwnedWorld::from_record(&child.record()).is_err());
        child.fork_lineage[0].logical = Timestamp(201);
        let retained = OwnedWorld::from_record(&child.record())?;
        assert_eq!(retained.initial_clock(), Timestamp(201));
        assert_eq!(retained.inherited_system(), Timestamp(200));
        child.ancestors.last_mut().unwrap().logical = Timestamp::MAX;
        child.fork_lineage[0].logical = Timestamp::MAX;
        assert!(OwnedWorld::from_record(&child.record()).is_err());
        Ok(())
    }
}
