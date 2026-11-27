use prost::{Message, Oneof};
use rtdb_types::{Anomaly, Result};
use uuid::Uuid;

use super::{ObjectKind, invalid, require_kind};

#[derive(Message)]
pub(super) struct Record {
    #[prost(oneof = "Kind", tags = "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14")]
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
    #[prost(message, tag = "9")]
    KeyRetirement(KeyRetirement),
    #[prost(message, tag = "10")]
    WriterRelease(Lease),
    #[prost(message, tag = "11")]
    DeploymentRetirement(Lease),
    #[prost(message, tag = "12")]
    RankedCatalog(RankedCatalog),
    #[prost(message, tag = "13")]
    Recovered(Recovered),
    #[prost(message, tag = "14")]
    Commissioned(Commissioned),
}

#[derive(Message)]
struct Commissioned {}

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
    #[prost(bool, tag = "4")]
    restore: bool,
}

#[derive(Message)]
struct RankedCatalog {
    #[prost(string, tag = "1")]
    manifest: String,
    #[prost(uint64, tag = "2")]
    position: u64,
    #[prost(sint64, tag = "3")]
    system_time: i64,
    #[prost(uint64, tag = "4")]
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
struct KeyRetirement {
    #[prost(uint64, tag = "1")]
    generation: u64,
    #[prost(uint64, tag = "2")]
    rotation_lsn: u64,
    #[prost(sint64, tag = "3")]
    observed_at: i64,
    #[prost(sint64, optional, tag = "4")]
    unreadable_by: Option<i64>,
    #[prost(bool, tag = "5")]
    armed: bool,
}

#[derive(Message)]
struct Recovered {
    #[prost(uint64, tag = "1")]
    head: u64,
    #[prost(uint64, tag = "2")]
    epoch: u64,
    #[prost(sint64, tag = "3")]
    time: i64,
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
                restore: checkpoint.restore,
            }),
            RankedCatalog(catalog) => Kind::RankedCatalog(self::RankedCatalog {
                manifest: require_kind(catalog.manifest, ObjectKind::Manifest)?.to_string(),
                position: catalog.position,
                system_time: catalog.system_time,
                key_generation: catalog.key_generation,
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
            KeyRetirement { generation, rotation_lsn, observed_at, unreadable_by, armed } => {
                Kind::KeyRetirement(self::KeyRetirement {
                    generation: *generation,
                    rotation_lsn: *rotation_lsn,
                    observed_at: *observed_at,
                    unreadable_by: *unreadable_by,
                    armed: *armed,
                })
            }
            WriterRelease { lease_until } => Kind::WriterRelease(self::Lease { until: *lease_until }),
            DeploymentRetirement { lease_until } => Kind::DeploymentRetirement(self::Lease { until: *lease_until }),
            Recovered { head, epoch, time } => Kind::Recovered(self::Recovered { head: *head, epoch: *epoch, time: *time }),
            Commissioned => Kind::Commissioned(self::Commissioned {}),
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
                    restore: checkpoint.restore,
                }),
                Kind::RankedCatalog(catalog) => Self::RankedCatalog(super::RankedCatalog {
                    manifest: require_kind(catalog.manifest.parse()?, ObjectKind::Manifest)?,
                    position: catalog.position,
                    system_time: catalog.system_time,
                    key_generation: catalog.key_generation,
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
                Kind::KeyRetirement(retirement) => Self::KeyRetirement {
                    generation: retirement.generation,
                    rotation_lsn: retirement.rotation_lsn,
                    observed_at: retirement.observed_at,
                    unreadable_by: retirement.unreadable_by,
                    armed: retirement.armed,
                },
                Kind::WriterRelease(release) => Self::WriterRelease { lease_until: release.until },
                Kind::DeploymentRetirement(retirement) => Self::DeploymentRetirement { lease_until: retirement.until },
                Kind::Recovered(recovered) => Self::Recovered {
                    head: recovered.head,
                    epoch: recovered.epoch,
                    time: recovered.time,
                },
                Kind::Commissioned(_) => Self::Commissioned,
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
