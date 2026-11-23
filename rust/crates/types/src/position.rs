use crate::{Anomaly, Instant, Result, Timestamp};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt};

macro_rules! position {
    ($($name:ident: $primitive:ty),+ $(,)?) => { $(
        #[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub struct $name(pub $primitive);
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { self.0.fmt(f) }
        }
    )+ };
}

position!(SourcePosition: i64, ReplicaPosition: i64, TxId: i64, BlockIndex: i64,
    Term: i64, TermSequence: i64, StorageEpoch: i32, StorageVersion: i32);

/// The installed replica prefix captured with a physical catalog.
/// The journal epoch is explicit because its empty prefix is encoded as epoch base minus one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogCut {
    pub storage_version: StorageVersion,
    pub storage_epoch: StorageEpoch,
    pub replica_epoch: StorageEpoch,
    pub through: ReplicaPosition,
}

/// Identifies the provider namespace within which object versions are meaningful.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3ObjectNamespace {
    pub bucket: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
}

/// An observed version of a physical object. This carries no permission to erase it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalObjectVersion {
    pub namespace: S3ObjectNamespace,
    /// The exact object key as stored by S3, including provider path encoding.
    pub key: String,
    pub version: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TerminalObjectPublication {
    pub namespace: S3ObjectNamespace,
    pub key: String,
    pub allocation: uuid::Uuid,
    pub etag: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PhysicalObjectPublication {
    Version(PhysicalObjectVersion),
    Terminal(TerminalObjectPublication),
}

impl PhysicalObjectPublication {
    pub fn into_version(self) -> Option<PhysicalObjectVersion> {
        match self {
            Self::Version(version) => Some(version),
            Self::Terminal(_) => None,
        }
    }
}

/// A conditional write at one physical S3 key in a bucket that has never been versioned.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3TombstoneTarget {
    pub namespace: S3ObjectNamespace,
    pub key: String,
    pub condition: S3TombstoneCondition,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum S3TombstoneCondition {
    Absent,
    Matches(String),
}

const OFFSET_LIMIT: i64 = 1_i64 << 48;
const EPOCH_LIMIT: i32 = 1_i32 << 15;

fn is_encodable_log_epoch(epoch: i32) -> bool { (0..EPOCH_LIMIT).contains(&epoch) }

fn encode_message_id(epoch: StorageEpoch, offset: i64) -> Result<i64> {
    if !is_encodable_log_epoch(epoch.0) {
        return Err(Anomaly::incorrect("xtdb/invalid-log-epoch", "Log epoch must be in 0..=32767"));
    }
    if offset >= OFFSET_LIMIT {
        return Err(Anomaly::incorrect("xtdb/invalid-log-offset", "Offset exceeds the 48-bit encoding limit"));
    }
    Ok((i64::from(epoch.0) << 48).wrapping_add(offset))
}

#[allow(unexpected_cfgs)]
#[cfg(kani)]
#[kani::proof]
fn encoder_admits_exactly_the_representable_log_epochs() {
    let epoch: i32 = kani::any();
    assert_eq!(is_encodable_log_epoch(epoch), i16::try_from(epoch).is_ok_and(|value| value >= 0));
}

macro_rules! message_position {
    ($($name:ident),+ $(,)?) => { $(
        impl $name {
            /// Packs an epoch and a raw offset using XTDB's signed addition, including offset -1.
            pub fn from_epoch_offset(epoch: StorageEpoch, offset: i64) -> Result<Self> {
                encode_message_id(epoch, offset).map(Self)
            }
            pub fn epoch(self) -> StorageEpoch { StorageEpoch((self.0 >> 48) as i32) }
            pub fn offset(self) -> i64 { self.0 & (OFFSET_LIMIT - 1) }
            /// A foreign epoch or a negative message ID resumes at the start of this epoch.
            pub fn after_offset(self, epoch: StorageEpoch) -> i64 {
                if self.0 < 0 || self.epoch() != epoch { -1 } else { self.offset() }
            }
        }
    )+ };
}

message_position!(SourcePosition, ReplicaPosition);

impl TermSequence {
    pub const CLAIM: Self = Self(0);
    pub fn next(self) -> Result<Self> {
        self.0.checked_add(1).map(Self)
            .ok_or_else(|| Anomaly::fault("xtdb/term-sequence-overflow", "Term sequence exhausted"))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransactionKey { pub tx_id: TxId, pub system_time: Timestamp }

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxBasis { pub databases: BTreeMap<String, Vec<TxId>> }

impl TxBasis {
    /// Merges matching partition positions by maximum; differing partition counts are rejected.
    pub fn merge(&self, other: &Self) -> Result<Self> {
        let mut merged = self.clone();
        for (name, positions) in &other.databases {
            match merged.databases.get_mut(name) {
                None => { merged.databases.insert(name.clone(), positions.clone()); }
                Some(existing) => {
                    if existing.len() != positions.len() {
                        return Err(Anomaly::incorrect("xtdb.basis/invalid-basis", "Partition counts differ")
                            .with_context("database", name.clone().into()));
                    }
                    for (left, right) in existing.iter_mut().zip(positions) { *left = (*left).max(*right); }
                }
            }
        }
        Ok(merged)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeBasis { pub databases: BTreeMap<String, Vec<Option<Instant>>> }

impl TimeBasis {
    pub fn capped_at(&self, upper: Instant) -> Self {
        Self { databases: self.databases.iter().map(|(name, times)| {
            (name.clone(), times.iter().map(|time| Some(time.map_or(upper, |time| time.min(upper)))).collect())
        }).collect() }
    }
}
