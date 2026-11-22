use crate::{Anomaly, EntityId, Iid, Result, StructFields, Timestamp, TransactionKey, TxId, Value};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnedWorld {
    pub id: Iid,
    pub capture: Iid,
    pub manifest: String,
    pub basis: TransactionKey,
    pub until: Timestamp,
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
        ) = (
            row.get("_id"),
            row.get("capture"),
            row.get("manifest"),
            row.get("basis_id"),
            row.get("basis_time"),
            row.get("until"),
        )
        else {
            return Err(Self::unavailable());
        };
        if row.len() != 6
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
        })
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
    pub fn record(self, id: &EntityId) -> StructFields<Value> {
        let fact = match self {
            Self::Present(row) => Value::Struct(row),
            Self::Deleted => Value::Null,
        };
        StructFields::from([("_id".into(), id.as_value()), ("world_fact".into(), fact)])
    }

    pub fn validate_record(row: &StructFields<Value>) -> Result<()> {
        if row.len() != 2 {
            return Err(OwnedWorld::unavailable());
        }
        let id = row
            .get("_id")
            .ok_or_else(OwnedWorld::unavailable)?
            .entity_id()?;
        match row.get("world_fact") {
            Some(Value::Null) => Ok(()),
            Some(Value::Struct(fact))
                if fact
                    .get("_id")
                    .and_then(|value| value.entity_id().ok())
                    .as_ref()
                    == Some(&id)
                    && fact
                        .keys()
                        .all(|name| name == "_id" || !name.starts_with('_')) =>
            {
                Ok(())
            }
            _ => Err(OwnedWorld::unavailable()),
        }
    }

    pub fn from_record(mut row: StructFields<Value>) -> Result<Self> {
        Self::validate_record(&row)?;
        match row.remove("world_fact") {
            Some(Value::Null) => Ok(Self::Deleted),
            Some(Value::Struct(fact)) => Ok(Self::Present(fact)),
            _ => Err(OwnedWorld::unavailable()),
        }
    }
}
