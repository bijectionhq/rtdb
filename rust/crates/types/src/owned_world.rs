use std::collections::BTreeMap;

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

    pub fn relation(&self, relation: &OwnedRelation) -> Option<&OwnedSchema> {
        self.fields.get(relation.name())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum WorldClockMode {
    Retained,
    Held { start: Timestamp },
    Proposal,
}

impl WorldClockMode {
    pub fn is_writable(self) -> bool {
        self != Self::Retained
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedWorld {
    pub id: Iid,
    pub pin: Iid,
    pub capture: Iid,
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
    pub fn inherited_clock(&self) -> Timestamp {
        self.parent_publication.unwrap_or(self.basis.system_time)
    }

    pub fn initial_clock(&self) -> Timestamp {
        match self.mode {
            WorldClockMode::Retained | WorldClockMode::Proposal => self.inherited_clock(),
            WorldClockMode::Held { start } => start,
        }
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
        if row.len() != 12 + usize::from(row.contains_key("suffix"))
            || *id == [0; 16] || *pin == [0; 16]
            || *capture == [0; 16]
            || *until <= *time
            || manifest.len() != 43
            || !manifest
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(Self::unavailable());
        }
        Ok(Self {
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
        self.id == next.id
            && self.pin != next.pin
            && !self.dropped && !next.dropped
            && self.mode == WorldClockMode::Retained && next.mode == WorldClockMode::Retained
            && self.basis.system_time < next.basis.system_time
            && self.basis.tx_id < next.basis.tx_id
    }

    pub fn same_fork(&self, other: &Self) -> bool {
        self.id == other.id
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
                (row, Value::List(fields))
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
                for field in fields {
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
