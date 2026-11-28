//! RTDB's versioned plaintext journal records and checkpoint manifest format.

mod wire;

use std::{fmt, str::FromStr};

use prost::Message;
use rtdb_types::{Anomaly, Result, rtdb_v1::Basis};
use uuid::Uuid;

const RECORD_HEADER: &[u8] = b"RTDBR\x01";
const MANIFEST_HEADER: &[u8] = b"RTDBM\x01";

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ObjectKind {
    File,
    Manifest,
}

impl ObjectKind {
    fn path(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::Manifest => "manifest",
        }
    }
}

/// A SHA-256 digest of the complete stored ciphertext identifies the expected bytes.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ObjectId {
    pub database: Uuid,
    pub kind: ObjectKind,
    pub epoch: u64,
    pub seq: u64,
    pub digest: [u8; 32],
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "db/{}/{}/{}/{}/",
            self.database,
            self.kind.path(),
            self.epoch,
            self.seq
        )?;
        for byte in self.digest {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for ObjectId {
    type Err = Anomaly;

    fn from_str(key: &str) -> Result<Self> {
        fn parse(key: &str) -> Option<ObjectId> {
            let mut parts = key.split('/');
            if parts.next()? != "db" {
                return None;
            }
            let database = parts.next()?.parse().ok()?;
            let kind = match parts.next()? {
                "file" => ObjectKind::File,
                "manifest" => ObjectKind::Manifest,
                _ => return None,
            };
            let epoch = parts.next()?.parse().ok()?;
            let seq = parts.next()?.parse().ok()?;
            let hex = parts.next()?;
            if hex.len() != 64 || !hex.is_ascii() || parts.next().is_some() {
                return None;
            }
            let mut digest = [0; 32];
            for (i, byte) in digest.iter_mut().enumerate() {
                *byte = u8::from_str_radix(&hex[2 * i..2 * i + 2], 16).ok()?;
            }
            Some(ObjectId {
                database,
                kind,
                epoch,
                seq,
                digest,
            })
        }

        parse(key)
            .filter(|id| id.to_string() == key)
            .ok_or_else(|| invalid("Expected a canonical epoch-scoped object key"))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Checkpoint {
    pub manifest: ObjectId,
    pub through: u64,
    pub key_generation: u64,
    /// Installs an independently rewritten state into a fresh deployment.
    pub restore: bool,
    /// The epoch of the last operator recovery the checkpointed state
    /// includes, or 0. A follower that has not seen that recovery rebuilds
    /// instead of resuming past a checkpoint that trimmed the recovery's
    /// record. Older checkpoints decode as 0.
    pub recovery_epoch: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RankedCatalog {
    pub manifest: ObjectId,
    pub position: u64,
    pub system_time: i64,
    pub key_generation: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Pin {
    pub id: Uuid,
    pub manifest: ObjectId,
    /// UTC microseconds. Expiry closes admission. Unpin records release of custody.
    pub until: Option<i64>,
}

/// A private byte retention promise in one Store incarnation. The reader
/// is an opaque identity and the deadline is exclusive in UTC microseconds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OrderedStoreHold {
    pub basis: Basis,
    pub id: [u8; 16],
    pub reader: [u8; 32],
    pub expires_at_micros: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StoreHoldChange {
    Retain(OrderedStoreHold),
    Release(OrderedStoreHold),
}

/// The requested retention census and the ordered writer's lower time bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StorePurgeClock {
    pub census_micros: i64,
    pub lower_bound_micros: i64,
}

impl StorePurgeClock {
    pub fn covers_census(self) -> bool {
        (0..=self.lower_bound_micros.max(0)).contains(&self.census_micros)
    }
}

/// All fields measured in journal positions are unsigned LSN values.
/// All wall-clock fields are UTC microseconds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Record {
    /// Storage dependencies of the resolved transactions are enumerated in `objects`.
    Tx {
        payload: Vec<u8>,
        objects: Vec<ObjectId>,
    },
    Checkpoint(Checkpoint),
    RankedCatalog(RankedCatalog),
    Lease {
        until: i64,
    },
    Pin(Pin),
    Unpin {
        id: Uuid,
    },
    KeyRotate {
        generation: u64,
        at: u64,
    },
    Settle {
        tx_lsn: u64,
    },
    Refuse {
        tx_lsn: u64,
    },
    /// Durable post-commit clock observation for the preceding key rotation.
    KeyRetirement {
        generation: u64,
        rotation_lsn: u64,
        observed_at: i64,
        unreadable_by: Option<i64>,
        armed: bool,
    },
    WriterRelease {
        lease_until: i64,
    },
    /// Permanently closes the stream's RTDB admission under its current writer.
    DeploymentRetirement {
        lease_until: i64,
    },
    /// The writer of `epoch` resumed at `time` after an operator recovery cut
    /// the journal back to `head`; records acknowledged after `head` may be lost.
    Recovered {
        head: u64,
        epoch: u64,
        time: i64,
    },
    /// Establishes the activation scope of a fresh immutable application history.
    Commissioned,
    /// Store versions are transaction positions in `since`'s incarnation.
    StorePurgeFloor {
        since: Basis,
        keep_from: u64,
    },
    StorePurgeReclaim {
        since: Basis,
        keep_from: u64,
        clock: StorePurgeClock,
    },
    StoreHold {
        change: StoreHoldChange,
        lower_bound_micros: i64,
    },
}

impl Record {
    pub fn permits_fresh_restore(&self) -> bool {
        matches!(self, Self::Commissioned | Self::Lease { .. } | Self::WriterRelease { .. })
    }
}

impl Record {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let wire = wire::Record::try_from(self)?;
        Ok(encode(RECORD_HEADER, wire))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        decode::<wire::Record>(RECORD_HEADER, bytes)?.try_into()
    }
}

/// The files are the complete storage closure for this checkpoint image.
/// The pins are the active pin state at this checkpoint's replay boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Manifest {
    pub files: Vec<ObjectId>,
    pub pins: Vec<Pin>,
    /// Byte retention promises at the local replay boundary, by hold ID.
    pub store_holds: Vec<OrderedStoreHold>,
    pub payload: Vec<u8>,
}

impl Manifest {
    pub fn encode(&self) -> Result<Vec<u8>> {
        let wire = wire::Manifest::try_from(self)?;
        Ok(encode(MANIFEST_HEADER, wire))
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        decode::<wire::Manifest>(MANIFEST_HEADER, bytes)?.try_into()
    }
}

fn encode(header: &[u8], wire: impl Message) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(header.len() + wire.encoded_len());
    bytes.extend_from_slice(header);
    wire.encode_raw(&mut bytes);
    bytes
}

fn decode<T: Message + Default>(header: &[u8], bytes: &[u8]) -> Result<T> {
    let payload = bytes
        .strip_prefix(header)
        .ok_or_else(|| invalid("Unknown journal format or version"))?;
    T::decode(payload).map_err(|err| invalid(err.to_string()))
}

fn require_kind(id: ObjectId, kind: ObjectKind) -> Result<ObjectId> {
    if id.kind != kind {
        return Err(invalid("Unexpected storage object kind"));
    }
    Ok(id)
}

fn invalid(message: impl Into<String>) -> Anomaly {
    Anomaly::fault("rtdb.format/invalid-journal-envelope", message)
}

#[cfg(test)]
mod tests;
