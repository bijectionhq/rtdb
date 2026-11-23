use crate::{Anomaly, Iid, Result, Timestamp, interval_step};
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
pub enum EventOp { Put, Delete, Erase }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventHeader {
    pub iid: Iid,
    pub system_from: Timestamp,
    pub commit_position: u32,
    pub valid_time: Interval,
    pub op: EventOp,
}

impl EventHeader {
    pub fn system_position(self) -> (Timestamp, u32) {
        (self.system_from, self.commit_position)
    }

    pub fn version(self) -> RowVersion {
        RowVersion { iid: self.iid, system_from: self.system_from, commit_position: self.commit_position }
    }
}

/// A version's logical identity within a table and transaction history.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowVersion {
    pub iid: Iid,
    pub system_from: Timestamp,
    pub commit_position: u32,
}
