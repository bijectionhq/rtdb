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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedWorld {
    pub id: Iid,
    pub capture: Iid,
    pub manifest: String,
    pub basis: TransactionKey,
    pub until: Timestamp,
    pub schemas: WorldSchemas,
    pub dropped: bool,
}

impl OwnedWorld {
    pub fn record(&self) -> StructFields<Value> {
        StructFields::from([
            ("_id".into(), Value::Uuid(self.id.0)),
            ("capture".into(), Value::Uuid(self.capture.0)),
            ("manifest".into(), Value::Utf8(self.manifest.clone())),
            ("basis_id".into(), Value::I64(self.basis.tx_id.0)),
            ("basis_time".into(), Value::I64(self.basis.system_time.0)),
            ("until".into(), Value::I64(self.until.0)),
            ("dropped".into(), Value::Bool(self.dropped)),
            (
                "schemas".into(),
                Value::Binary(self.schemas.encoded.clone()),
            ),
        ])
    }

    pub fn from_record(row: &StructFields<Value>) -> Result<Self> {
        let (
            Some(Value::Uuid(id)),
            Some(Value::Uuid(capture)),
            Some(Value::Utf8(manifest)),
            Some(Value::I64(tx_id)),
            Some(Value::I64(time)),
            Some(Value::I64(until)),
            Some(Value::Binary(schemas)),
        ) = (
            row.get("_id"),
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
        let dropped = match row.get("dropped") {
            None | Some(Value::Null | Value::Undefined) => false,
            Some(Value::Bool(dropped)) => *dropped,
            _ => return Err(Self::unavailable()),
        };
        if row.len() != 7 + usize::from(row.contains_key("dropped"))
            || *id == [0; 16]
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
            capture: Iid(*capture),
            manifest: manifest.clone(),
            basis: TransactionKey {
                tx_id: TxId(*tx_id),
                system_time: Timestamp(*time),
            },
            until: Timestamp(*until),
            schemas: WorldSchemas::from_bytes(schemas)?,
            dropped,
        })
    }

    pub fn same_fork(&self, other: &Self) -> bool {
        self.id == other.id
            && self.capture == other.capture
            && self.manifest == other.manifest
            && self.basis == other.basis
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
        let fact = match self {
            Self::Present(mut row) => {
                row.values_mut().for_each(Value::admit_object_dates);
                Value::Binary(serde_json::to_vec(&row).map_err(|_| OwnedWorld::unavailable())?)
            }
            Self::Deleted => Value::Null,
        };
        Ok(StructFields::from([
            ("_id".into(), id.as_value()),
            ("world_fact".into(), fact),
        ]))
    }

    pub fn decode_bound(row: &StructFields<Value>) -> Result<usize> {
        let bytes = match row.get("world_fact") {
            Some(Value::Binary(bytes)) => bytes.len(),
            Some(Value::Null) => 0,
            _ => return Err(OwnedWorld::unavailable()),
        };
        bytes
            .checked_mul(32)
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(OwnedWorld::unavailable)
    }

    pub fn from_record(row: &StructFields<Value>) -> Result<Self> {
        if row.len() != 2 {
            return Err(OwnedWorld::unavailable());
        }
        let id = row
            .get("_id")
            .ok_or_else(OwnedWorld::unavailable)?
            .entity_id()?;
        match row.get("world_fact") {
            Some(Value::Null) => Ok(Self::Deleted),
            Some(Value::Binary(bytes)) => {
                let fact: StructFields<Value> =
                    serde_json::from_slice(bytes).map_err(|_| OwnedWorld::unavailable())?;
                if fact
                    .get("_id")
                    .and_then(|value| value.entity_id().ok())
                    .as_ref()
                    != Some(&id)
                    || fact
                        .keys()
                        .any(|name| name != "_id" && name.starts_with('_'))
                {
                    return Err(OwnedWorld::unavailable());
                }
                Ok(Self::Present(fact))
            }
            _ => Err(OwnedWorld::unavailable()),
        }
    }
}
