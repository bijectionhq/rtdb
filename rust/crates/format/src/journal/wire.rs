use prost::{Message, Oneof};
use rtdb_types::{Anomaly, Result};
use uuid::Uuid;

use super::{ObjectKind, invalid, require_kind};

#[derive(Message)]
pub(super) struct Record {
    #[prost(oneof = "Kind", tags = "1, 2, 3, 4, 5, 6, 7, 8")]
    kind: Option<Kind>,
}

#[derive(Oneof)]
enum Kind {
    #[prost(message, tag = "1")]
    Tx(Tx),
    #[prost(message, tag = "2")]
    Checkpoint(Checkpoint),
    #[prost(message, tag = "3")]
    Lease(Lease),
    #[prost(message, tag = "4")]
    Pin(Pin),
    #[prost(message, tag = "5")]
    Unpin(Unpin),
    #[prost(message, tag = "6")]
    KeyRotate(KeyRotate),
    #[prost(message, tag = "7")]
    Settle(TimedDecision),
    #[prost(message, tag = "8")]
    Refuse(TimedDecision),
}

#[derive(Message)]
struct Tx {
    #[prost(bytes, tag = "1")]
    payload: Vec<u8>,
    #[prost(string, repeated, tag = "2")]
    objects: Vec<String>,
}

#[derive(Message)]
struct Checkpoint {
    #[prost(string, tag = "1")]
    manifest: String,
    #[prost(uint64, tag = "2")]
    through: u64,
    #[prost(uint64, tag = "3")]
    key_generation: u64,
}

#[derive(Message)]
struct Lease {
    #[prost(sint64, tag = "1")]
    until: i64,
}

#[derive(Message)]
struct Pin {
    #[prost(bytes, tag = "1")]
    id: Vec<u8>,
    #[prost(string, tag = "2")]
    manifest: String,
    #[prost(sint64, optional, tag = "3")]
    until: Option<i64>,
}

#[derive(Message)]
struct Unpin {
    #[prost(bytes, tag = "1")]
    id: Vec<u8>,
}

#[derive(Message)]
struct KeyRotate {
    #[prost(uint64, tag = "1")]
    generation: u64,
    #[prost(uint64, tag = "2")]
    at: u64,
}

#[derive(Message)]
struct TimedDecision {
    #[prost(uint64, tag = "1")]
    tx_lsn: u64,
}

#[derive(Message)]
pub(super) struct Manifest {
    #[prost(string, repeated, tag = "1")]
    files: Vec<String>,
    #[prost(message, repeated, tag = "2")]
    pins: Vec<Pin>,
    #[prost(bytes, tag = "3")]
    payload: Vec<u8>,
}

impl TryFrom<&super::Record> for Record {
    type Error = Anomaly;

    fn try_from(record: &super::Record) -> Result<Self> {
        use super::Record::*;
        let kind = match record {
            Tx { payload, objects } => Kind::Tx(self::Tx {
                payload: payload.clone(),
                objects: objects.iter().map(ToString::to_string).collect(),
            }),
            Checkpoint(checkpoint) => Kind::Checkpoint(self::Checkpoint {
                manifest: require_kind(checkpoint.manifest, ObjectKind::Manifest)?.to_string(),
                through: checkpoint.through,
                key_generation: checkpoint.key_generation,
            }),
            Lease { until } => Kind::Lease(self::Lease { until: *until }),
            Pin(pin) => Kind::Pin(pin.try_into()?),
            Unpin { id } => Kind::Unpin(self::Unpin {
                id: id.as_bytes().to_vec(),
            }),
            KeyRotate { generation, at } => Kind::KeyRotate(self::KeyRotate {
                generation: *generation,
                at: *at,
            }),
            Settle { tx_lsn } => Kind::Settle(TimedDecision { tx_lsn: *tx_lsn }),
            Refuse { tx_lsn } => Kind::Refuse(TimedDecision { tx_lsn: *tx_lsn }),
        };
        Ok(Self { kind: Some(kind) })
    }
}

impl TryFrom<Record> for super::Record {
    type Error = Anomaly;

    fn try_from(record: Record) -> Result<Self> {
        Ok(
            match record
                .kind
                .ok_or_else(|| invalid("Journal record has no recognised kind"))?
            {
                Kind::Tx(tx) => Self::Tx {
                    payload: tx.payload,
                    objects: tx
                        .objects
                        .iter()
                        .map(|key| key.parse())
                        .collect::<Result<_>>()?,
                },
                Kind::Checkpoint(checkpoint) => Self::Checkpoint(super::Checkpoint {
                    manifest: require_kind(checkpoint.manifest.parse()?, ObjectKind::Manifest)?,
                    through: checkpoint.through,
                    key_generation: checkpoint.key_generation,
                }),
                Kind::Lease(lease) => Self::Lease { until: lease.until },
                Kind::Pin(pin) => Self::Pin(pin.try_into()?),
                Kind::Unpin(unpin) => Self::Unpin {
                    id: uuid(&unpin.id)?,
                },
                Kind::KeyRotate(rotation) => Self::KeyRotate {
                    generation: rotation.generation,
                    at: rotation.at,
                },
                Kind::Settle(decision) => Self::Settle {
                    tx_lsn: decision.tx_lsn,
                },
                Kind::Refuse(decision) => Self::Refuse {
                    tx_lsn: decision.tx_lsn,
                },
            },
        )
    }
}

impl TryFrom<&super::Pin> for Pin {
    type Error = Anomaly;

    fn try_from(pin: &super::Pin) -> Result<Self> {
        Ok(Self {
            id: pin.id.as_bytes().to_vec(),
            manifest: require_kind(pin.manifest, ObjectKind::Manifest)?.to_string(),
            until: pin.until,
        })
    }
}

impl TryFrom<Pin> for super::Pin {
    type Error = Anomaly;

    fn try_from(pin: Pin) -> Result<Self> {
        Ok(Self {
            id: uuid(&pin.id)?,
            manifest: require_kind(pin.manifest.parse()?, ObjectKind::Manifest)?,
            until: pin.until,
        })
    }
}

impl TryFrom<&super::Manifest> for Manifest {
    type Error = Anomaly;

    fn try_from(manifest: &super::Manifest) -> Result<Self> {
        Ok(Self {
            files: manifest
                .files
                .iter()
                .map(|id| require_kind(*id, ObjectKind::File).map(|id| id.to_string()))
                .collect::<Result<_>>()?,
            pins: manifest
                .pins
                .iter()
                .map(Pin::try_from)
                .collect::<Result<_>>()?,
            payload: manifest.payload.clone(),
        })
    }
}

impl TryFrom<Manifest> for super::Manifest {
    type Error = Anomaly;

    fn try_from(manifest: Manifest) -> Result<Self> {
        Ok(Self {
            files: manifest
                .files
                .iter()
                .map(|key| require_kind(key.parse()?, ObjectKind::File))
                .collect::<Result<_>>()?,
            pins: manifest
                .pins
                .into_iter()
                .map(super::Pin::try_from)
                .collect::<Result<_>>()?,
            payload: manifest.payload,
        })
    }
}

fn uuid(bytes: &[u8]) -> Result<Uuid> {
    Uuid::from_slice(bytes).map_err(|err| invalid(err.to_string()))
}
