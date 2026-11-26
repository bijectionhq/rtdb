use crate::{Anomaly, BlockIndex, Iid, Result, Timestamp, interval_step, journal_evidence::valid_journal_cutoff};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "InstantParts", into = "InstantParts")]
pub struct Instant { seconds: i64, nanos: u32 }

#[derive(Serialize, Deserialize)]
struct InstantParts { seconds: i64, nanos: u32 }

impl Instant {
    pub fn new(seconds: i64, nanos: u32) -> Result<Self> {
        if nanos >= 1_000_000_000 {
            return Err(Anomaly::incorrect("xtdb/invalid-instant", "Nanoseconds must be within one second"));
        }
        Ok(Self { seconds, nanos })
    }
    pub fn seconds(self) -> i64 { self.seconds }
    pub fn nanos(self) -> u32 { self.nanos }
    pub fn as_micros(self) -> Result<Timestamp> { Timestamp::from_epoch_parts(self.seconds, self.nanos as i32) }
}

impl TryFrom<InstantParts> for Instant {
    type Error = Anomaly;
    fn try_from(value: InstantParts) -> Result<Self> { Self::new(value.seconds, value.nanos) }
}
impl From<Instant> for InstantParts {
    fn from(value: Instant) -> Self { Self { seconds: value.seconds, nanos: value.nanos } }
}
impl From<Timestamp> for Instant {
    fn from(value: Timestamp) -> Self {
        let (seconds, nanos) = value.epoch_parts();
        Self { seconds, nanos: nanos as u32 }
    }
}

/// Exclusive journal cutoff bound to the exact clock certificate that authorized it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimedJournalCutoff {
    before: Timestamp,
    certificate_sha256: [u8; 32],
}

impl TimedJournalCutoff {
    pub fn new(before: Timestamp, certificate_sha256: [u8; 32]) -> Result<Self> {
        if !valid_journal_cutoff(before.0, &certificate_sha256) {
            return Err(Anomaly::incorrect("rtdb.owned/timed-journal-cutoff", "A timed journal cutoff needs a positive time and exact certificate digest"));
        }
        Ok(Self { before, certificate_sha256 })
    }

    pub fn before(self) -> Timestamp { self.before }
    pub fn certificate_sha256(self) -> [u8; 32] { self.certificate_sha256 }
}

impl Timestamp {
    pub const MIN: Self = Self(i64::MIN);
    pub const MAX: Self = Self(i64::MAX);

    pub fn next(self) -> Result<Self> {
        self.0.checked_add(1).map(Self)
            .ok_or_else(|| Anomaly::incorrect("xtdb/time-overflow", "Timestamp has no following chronon"))
    }

    pub fn from_epoch_parts(seconds: i64, nanos: i32) -> Result<Self> {
        if !(0..1_000_000_000).contains(&nanos) {
            return Err(Anomaly::incorrect("xtdb/invalid-instant", "Nanoseconds must be within one second"));
        }
        let micros = (seconds as i128) * 1_000_000 + (nanos as i128) / 1_000;
        i64::try_from(micros).map(Self)
            .map_err(|_| Anomaly::incorrect("xtdb/time-overflow", "Instant exceeds microsecond timestamp range"))
    }

    pub fn epoch_parts(self) -> (i64, i32) {
        (self.0.div_euclid(1_000_000), (self.0.rem_euclid(1_000_000) * 1_000) as i32)
    }
}

/// A half-open interval in microseconds; equal bounds denote an empty range.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "IntervalParts", into = "IntervalParts")]
pub struct Interval { lower: Timestamp, upper: Timestamp }

#[derive(Serialize, Deserialize)]
struct IntervalParts { lower: Timestamp, upper: Timestamp }

impl Interval {
    pub const ALL: Self = Self { lower: Timestamp::MIN, upper: Timestamp::MAX };

    pub fn new(lower: Timestamp, upper: Timestamp) -> Result<Self> {
        if lower > upper {
            return Err(Anomaly::incorrect("xtdb/invalid-temporal-range", "Temporal range must have lower <= upper"));
        }
        Ok(Self { lower, upper })
    }
    pub fn from_micros(lower: i64, upper: i64) -> Result<Self> { Self::new(Timestamp(lower), Timestamp(upper)) }
    pub fn at(at: Timestamp) -> Result<Self> { Self::new(at, at.next()?) }
    pub fn between(lower: Timestamp, inclusive_upper: Timestamp) -> Result<Self> {
        Self::new(lower, if inclusive_upper == Timestamp::MAX { inclusive_upper } else { inclusive_upper.next()? })
    }
    pub fn lower(self) -> Timestamp { self.lower }
    pub fn upper(self) -> Timestamp { self.upper }
    pub fn is_empty(self) -> bool { self.lower == self.upper }
    pub fn contains(self, time: Timestamp) -> bool { self.lower <= time && time < self.upper }
    pub fn is_point(self) -> bool { self.lower.0.checked_add(1) == Some(self.upper.0) }
    pub fn intersects(self, other: Self) -> bool {
        self.intersection(other).is_some()
    }
    pub fn intersection(self, other: Self) -> Option<Self> {
        interval_step::intersect(self.endpoints(), other.endpoints())
            .map(|(lower, upper)| Self { lower: Timestamp(lower), upper: Timestamp(upper) })
    }
    fn endpoints(self) -> (i64, i64) { (self.lower.0, self.upper.0) }
}

impl TryFrom<IntervalParts> for Interval {
    type Error = Anomaly;
    fn try_from(parts: IntervalParts) -> Result<Self> { Self::new(parts.lower, parts.upper) }
}
impl From<Interval> for IntervalParts {
    fn from(value: Interval) -> Self { Self { lower: value.lower, upper: value.upper } }
}
impl Default for Interval { fn default() -> Self { Self::ALL } }

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TemporalBounds { pub valid_time: Interval, pub system_time: Interval }

impl TemporalBounds {
    pub fn intersects(self, other: Self) -> bool {
        self.valid_time.intersects(other.valid_time) && self.system_time.intersects(other.system_time)
    }

    /// Admits a history fragment that overlaps both requested axes, optionally clipping its valid time.
    pub fn admit_fragment(self, fragment: Self, clamp_valid: bool) -> Option<Self> {
        let (lower, upper) = interval_step::admit_fragment(
            fragment.valid_time.endpoints(), fragment.system_time.endpoints(),
            self.valid_time.endpoints(), self.system_time.endpoints(), clamp_valid,
        )?;
        Some(Self { valid_time: Interval { lower: Timestamp(lower), upper: Timestamp(upper) }, system_time: fragment.system_time })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum EventOp {
    Put,
    Delete,
    Erase,
    /// The payload-free image of one original owned assertion.
    Redacted,
}

/// Position within the partition history. The enclosing catalog or replica
/// record authenticates the history and partition to which this belongs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EventPosition {
    pub transaction: crate::TxId,
    pub ordinal: u64,
}

impl EventPosition {
    pub const WIDTH: i32 = 16;

    pub fn to_bytes(self) -> [u8; Self::WIDTH as usize] {
        let mut bytes = [0; Self::WIDTH as usize];
        bytes[..8].copy_from_slice(&self.transaction.0.to_be_bytes());
        bytes[8..].copy_from_slice(&self.ordinal.to_be_bytes());
        bytes
    }

    pub fn from_bytes(bytes: [u8; Self::WIDTH as usize]) -> Self {
        let (transaction, ordinal) = bytes.split_first_chunk::<8>().expect("fixed position width");
        Self {
            transaction: crate::TxId(i64::from_be_bytes(*transaction)),
            ordinal: u64::from_be_bytes(ordinal.try_into().expect("fixed ordinal width")),
        }
    }
}

/// Private assertion metadata used to compare a held point at ordering.
pub const OWNED_EVENT_POSITION_COLUMN: &str = "_rtdb_owned_event_position";

/// System-order endpoints of a private, frozen archive history scan.
pub const ARCHIVE_TX_FROM: &str = "_rtdb_archive_tx_from";
pub const ARCHIVE_TX_TO: &str = "_rtdb_archive_tx_to";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnedPointVersion {
    pub position: EventPosition,
    pub history: Timestamp,
}

/// The projection a reader revealed of what it observed. A check carrying
/// it compares that projection, not the version: every stored column except
/// system time, validity endpoints, private RTDB metadata and `hidden` (the
/// columns the reader could not open). The projection must be the same at the
/// reader's cut and at acceptance, and constant over the clock interval the
/// decision depends on: `clock`, or else the read's valid instant through
/// acceptance. A change inside that interval leaves the read undetermined.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedRevealed {
    pub hidden: Vec<String>,
    pub clock: Option<Interval>,
}

impl OwnedRevealed {
    pub const MAX_HIDDEN: usize = 64;

    /// `[flags][lower, upper: i64 BE if flags & 1][count: u16 BE][len: u16 BE, UTF-8 name]*`.
    pub fn from_wire(bytes: &[u8]) -> Option<Self> {
        let (&flags, mut rest) = bytes.split_first()?;
        let clock = match flags {
            0 => None,
            1 => {
                let (lower, tail) = rest.split_first_chunk::<8>()?;
                let (upper, tail) = tail.split_first_chunk::<8>()?;
                rest = tail;
                let clock = Interval::new(Timestamp(i64::from_be_bytes(*lower)), Timestamp(i64::from_be_bytes(*upper))).ok()?;
                if clock.is_empty() { return None; }
                Some(clock)
            }
            _ => return None,
        };
        let (count, mut rest) = rest.split_first_chunk::<2>()?;
        let count = usize::from(u16::from_be_bytes(*count));
        if count > Self::MAX_HIDDEN { return None; }
        let mut hidden = Vec::with_capacity(count);
        for _ in 0..count {
            let (length, tail) = rest.split_first_chunk::<2>()?;
            let (name, tail) = tail.split_at_checked(usize::from(u16::from_be_bytes(*length)))?;
            rest = tail;
            let name = std::str::from_utf8(name).ok()?;
            if name.is_empty() || hidden.last().is_some_and(|previous: &String| previous.as_str() >= name) { return None; }
            hidden.push(name.to_owned());
        }
        rest.is_empty().then_some(Self { hidden, clock })
    }

    pub fn to_wire(&self) -> Vec<u8> {
        let mut bytes = vec![u8::from(self.clock.is_some())];
        if let Some(clock) = self.clock {
            bytes.extend(clock.lower().0.to_be_bytes());
            bytes.extend(clock.upper().0.to_be_bytes());
        }
        bytes.extend((self.hidden.len() as u16).to_be_bytes());
        for name in &self.hidden {
            bytes.extend((name.len() as u16).to_be_bytes());
            bytes.extend(name.as_bytes());
        }
        bytes
    }

    pub fn retained_bytes(&self) -> usize {
        64 + self.hidden.iter().map(|name| name.len() + 24).sum::<usize>()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventHeader {
    pub position: Option<EventPosition>,
    pub iid: Iid,
    pub system_from: Timestamp,
    pub commit_position: u32,
    pub valid_time: Interval,
    pub op: EventOp,
}

impl EventHeader {
    pub fn precedence(self) -> (Timestamp, i128) {
        let order = self.position.map_or_else(
            || i128::MIN + i128::from(self.commit_position),
            |position| (i128::from(position.transaction.0) << 64) | i128::from(position.ordinal),
        );
        (self.system_from, order)
    }

    pub fn version(self) -> RowVersion {
        RowVersion { iid: self.iid, system_from: self.system_from, position: self.position, commit_position: self.commit_position }
    }

    /// Whether `key`'s transaction wrote this event: its position names that
    /// transaction, and its system time is that transaction's. Tied
    /// transactions share a system time, so only the position tells one from
    /// its twin (T7). The transaction buffer positions every staged event.
    pub fn written_in(self, key: crate::TransactionKey) -> bool {
        self.position.is_some_and(|position| position.transaction == key.tx_id)
            && self.system_from == key.system_time
    }
}

/// A version's logical identity within a table and transaction history.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowVersion {
    pub iid: Iid,
    pub system_from: Timestamp,
    pub position: Option<EventPosition>,
    pub commit_position: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct EventOrigin(pub [u8; 32]);

impl EventOrigin {
    pub fn new_commit() -> [u8; 16] { *uuid::Uuid::new_v4().as_bytes() }

    pub const WIDTH: i32 = 32;

    pub fn at(commit: [u8; 16], ordinal: u64, block: BlockIndex) -> Self {
        let mut value = [0; 32];
        value[..16].copy_from_slice(&commit);
        value[16..24].copy_from_slice(&ordinal.to_be_bytes());
        value[24..].copy_from_slice(&block.0.to_be_bytes());
        Self(value)
    }

    pub fn ordinal(self) -> u64 {
        u64::from_be_bytes(self.0[16..24].try_into().expect("event ordinal width"))
    }

    pub fn source_block(self) -> BlockIndex {
        BlockIndex(i64::from_be_bytes(self.0[24..].try_into().expect("event block width")))
    }
}
