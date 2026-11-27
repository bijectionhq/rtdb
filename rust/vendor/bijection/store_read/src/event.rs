//! Owned events and their bitemporal resolution (pure).
//!
//! An owned row's history is a set of immutable events. Each event has a
//! precedence `(system_from, tx, ordinal)` and a valid interval. Resolution is
//! XTDB's rule: a higher-precedence event claims its valid interval first, and
//! a lower one keeps only the region left unclaimed. A point at valid instant
//! `v` is therefore the highest-precedence applicable event whose interval
//! contains `v` (RTDB `HistoryResolver::visit_winners`,
//! `rust/crates/temporal/src/lib.rs:420`).
use std::collections::BTreeMap;

use anyhow::{
    ensure,
    Context,
};

/// `i64::MAX` is the open end of a valid interval.
pub const VALID_MAX: i64 = i64::MAX;

/// A half-open valid interval `[from, to)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Interval {
    pub from: i64,
    pub to: i64,
}

impl Interval {
    pub const ALL: Self = Self { from: i64::MIN, to: VALID_MAX };

    pub fn new(from: i64, to: i64) -> anyhow::Result<Self> {
        ensure!(from < to, "An owned valid interval is non-empty");
        Ok(Self { from, to })
    }

    pub fn contains(self, at: i64) -> bool {
        self.from <= at && at < self.to
    }

    pub fn intersect(self, other: Self) -> Option<Self> {
        let from = self.from.max(other.from);
        let to = self.to.min(other.to);
        (from < to).then_some(Self { from, to })
    }
}

/// The order of events: later system time, then later transaction, then later
/// ordinal within the transaction wins.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Precedence {
    pub system_from: i64,
    pub tx: i64,
    pub ordinal: u64,
}

pub const PRECEDENCE_BYTES: usize = 24;

/// Big-endian with the sign bit flipped, so byte order is numeric order.
pub fn ordered_i64(value: i64) -> [u8; 8] {
    ((value as u64) ^ (1 << 63)).to_be_bytes()
}

pub fn unordered_i64(bytes: [u8; 8]) -> i64 {
    (u64::from_be_bytes(bytes) ^ (1 << 63)) as i64
}

impl Precedence {
    pub fn encode(self) -> [u8; PRECEDENCE_BYTES] {
        let mut out = [0; PRECEDENCE_BYTES];
        out[..8].copy_from_slice(&ordered_i64(self.system_from));
        out[8..16].copy_from_slice(&ordered_i64(self.tx));
        out[16..].copy_from_slice(&self.ordinal.to_be_bytes());
        out
    }

    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        ensure!(bytes.len() == PRECEDENCE_BYTES, "Invalid owned event precedence");
        Ok(Self {
            system_from: unordered_i64(bytes[..8].try_into()?),
            tx: unordered_i64(bytes[8..16].try_into()?),
            ordinal: u64::from_be_bytes(bytes[16..].try_into()?),
        })
    }

    /// RTDB's 16-byte event position: `transaction i64 BE ‖ ordinal u64 BE`
    /// (`rust/crates/types/src/time.rs:155`).
    pub fn event_position(self) -> [u8; 16] {
        let mut out = [0; 16];
        out[..8].copy_from_slice(&self.tx.to_be_bytes());
        out[8..].copy_from_slice(&self.ordinal.to_be_bytes());
        out
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    /// The region shows the image.
    Put = 1,
    /// The region shows absence.
    Delete = 2,
    /// Replayed residue of an erased assertion: reading it is an error.
    Redacted = 3,
}

impl TryFrom<u8> for Op {
    type Error = anyhow::Error;

    fn try_from(byte: u8) -> anyhow::Result<Self> {
        Ok(match byte {
            1 => Self::Put,
            2 => Self::Delete,
            3 => Self::Redacted,
            _ => anyhow::bail!("Invalid owned event op {byte}"),
        })
    }
}

/// A row image: field name to the field's encoded value. The engine never
/// interprets the values; the adapter that owns the relation's schema does.
pub type Image = BTreeMap<String, Vec<u8>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub precedence: Precedence,
    pub op: Op,
    pub valid: Interval,
    /// The 1-based logical assertion ordinal within the commit, if any.
    pub assertion_ordinal: Option<i64>,
    /// The event's position within its publication, for position masks.
    pub commit_position: u32,
    /// Present exactly for `Put`.
    pub image: Option<Image>,
}

impl Event {
    pub fn encode_value(&self) -> Vec<u8> {
        let mut out = vec![self.op as u8];
        out.extend_from_slice(&self.valid.from.to_be_bytes());
        out.extend_from_slice(&self.valid.to.to_be_bytes());
        out.extend_from_slice(&self.assertion_ordinal.unwrap_or(0).to_be_bytes());
        out.extend_from_slice(&self.commit_position.to_be_bytes());
        if let Some(image) = &self.image {
            encode_image(&mut out, image);
        }
        out
    }

    pub fn decode(precedence: Precedence, value: &[u8]) -> anyhow::Result<Self> {
        const HEADER: usize = 1 + 8 + 8 + 8 + 4;
        ensure!(value.len() >= HEADER, "Truncated owned event");
        let op = Op::try_from(value[0])?;
        let valid = Interval::new(
            i64::from_be_bytes(value[1..9].try_into()?),
            i64::from_be_bytes(value[9..17].try_into()?),
        )?;
        let ordinal = i64::from_be_bytes(value[17..25].try_into()?);
        let commit_position = u32::from_be_bytes(value[25..29].try_into()?);
        let rest = &value[HEADER..];
        let image = match op {
            Op::Put => Some(decode_image(rest)?),
            Op::Delete | Op::Redacted => {
                ensure!(rest.is_empty(), "Only an owned put carries an image");
                None
            },
        };
        Ok(Self {
            precedence,
            op,
            valid,
            assertion_ordinal: (ordinal != 0).then_some(ordinal),
            commit_position,
            image,
        })
    }
}

fn encode_image(out: &mut Vec<u8>, image: &Image) {
    out.extend_from_slice(&(image.len() as u32).to_be_bytes());
    for (name, value) in image {
        out.extend_from_slice(&(name.len() as u16).to_be_bytes());
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&(value.len() as u32).to_be_bytes());
        out.extend_from_slice(value);
    }
}

fn decode_image(mut bytes: &[u8]) -> anyhow::Result<Image> {
    fn take<'a>(bytes: &mut &'a [u8], len: usize) -> anyhow::Result<&'a [u8]> {
        ensure!(bytes.len() >= len, "Truncated owned image");
        let (head, rest) = bytes.split_at(len);
        *bytes = rest;
        Ok(head)
    }
    let count = u32::from_be_bytes(take(&mut bytes, 4)?.try_into()?);
    let mut image = Image::new();
    for _ in 0..count {
        let len = usize::from(u16::from_be_bytes(take(&mut bytes, 2)?.try_into()?));
        let name = std::str::from_utf8(take(&mut bytes, len)?).context("Owned field names are UTF-8")?.to_owned();
        let len = u32::from_be_bytes(take(&mut bytes, 4)?.try_into()?) as usize;
        let value = take(&mut bytes, len)?.to_vec();
        ensure!(image.insert(name, value).is_none(), "Duplicate owned field");
    }
    ensure!(bytes.is_empty(), "Trailing owned image bytes");
    Ok(image)
}

/// Which events a read sees.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Cut {
    /// Events at or below this system time.
    pub system: i64,
    /// Events of transactions at or below this one.
    pub tx: i64,
    /// Drop events of the publication at `commit_micros` whose commit
    /// position is at or past `first_excluded`: a logical prefix of one
    /// physical publication (RTDB `scan.rs:2461`).
    pub mask: Option<PositionMask>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PositionMask {
    pub commit_micros: i64,
    pub first_excluded: u32,
}

impl Cut {
    pub fn applies(&self, event: &Event) -> bool {
        event.precedence.system_from <= self.system
            && event.precedence.tx <= self.tx
            && !self.mask.is_some_and(|mask| {
                event.precedence.system_from == mask.commit_micros && event.commit_position >= mask.first_excluded
            })
    }
}

/// The resolution of one valid instant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resolved<'a> {
    Absent,
    Present(&'a Event),
    /// The winning event is a delete: absent, but with a winner.
    Deleted(&'a Event),
    Redacted(&'a Event),
}

impl<'a> Resolved<'a> {
    pub fn winner(self) -> Option<&'a Event> {
        match self {
            Self::Absent => None,
            Self::Present(event) | Self::Deleted(event) | Self::Redacted(event) => Some(event),
        }
    }

    pub fn present(self) -> Option<&'a Event> {
        match self {
            Self::Present(event) => Some(event),
            Self::Absent | Self::Deleted(_) | Self::Redacted(_) => None,
        }
    }
}

/// The highest-precedence event of `events` applicable at `cut` whose valid
/// interval contains `valid`.
pub fn point<'a>(events: &'a [Event], cut: &Cut, valid: i64) -> Resolved<'a> {
    let winner = events
        .iter()
        .filter(|event| cut.applies(event) && event.valid.contains(valid))
        .max_by_key(|event| event.precedence);
    match winner {
        None => Resolved::Absent,
        Some(event) => match event.op {
            Op::Put => Resolved::Present(event),
            Op::Delete => Resolved::Deleted(event),
            Op::Redacted => Resolved::Redacted(event),
        },
    }
}

/// One maximal clipped winner region of a valid-time history.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Slice<'a> {
    pub valid: Interval,
    pub event: &'a Event,
}

/// The present regions of `window` at `cut`, sorted by valid start. Deleted
/// regions are gaps. A redacted region is an error.
pub fn slices<'a>(events: &'a [Event], cut: &Cut, window: Interval) -> anyhow::Result<Vec<Slice<'a>>> {
    Ok(claims(events, cut, window)?.into_iter().filter(|slice| slice.event.op == Op::Put).collect())
}

/// Every claimed region of `window` at `cut`, puts and deletes, sorted by
/// valid start. A world level masks its parents inside these regions: a
/// tombstone hides the parent as a put replaces it. A redacted region is an
/// error.
pub fn claims<'a>(events: &'a [Event], cut: &Cut, window: Interval) -> anyhow::Result<Vec<Slice<'a>>> {
    let mut ordered = events.iter().filter(|event| cut.applies(event)).collect::<Vec<_>>();
    ordered.sort_by_key(|event| std::cmp::Reverse(event.precedence));
    // Disjoint claimed regions, sorted by start.
    let mut claimed: Vec<Interval> = Vec::new();
    let mut out = Vec::new();
    for event in ordered {
        let Some(region) = event.valid.intersect(window) else { continue };
        for piece in unclaimed(&claimed, region) {
            match event.op {
                Op::Put | Op::Delete => out.push(Slice { valid: piece, event }),
                Op::Redacted => anyhow::bail!("rtdb.temporal/erased-assertion: the history reaches an erased assertion"),
            }
        }
        claim(&mut claimed, region);
    }
    out.sort_by_key(|slice| slice.valid.from);
    Ok(out)
}

pub fn unclaimed(claimed: &[Interval], region: Interval) -> Vec<Interval> {
    let mut pieces = Vec::new();
    let mut from = region.from;
    for taken in claimed {
        if taken.to <= from {
            continue;
        }
        if taken.from >= region.to {
            break;
        }
        if taken.from > from {
            pieces.push(Interval { from, to: taken.from });
        }
        from = from.max(taken.to);
        if from >= region.to {
            return pieces;
        }
    }
    if from < region.to {
        pieces.push(Interval { from, to: region.to });
    }
    pieces
}

pub fn claim(claimed: &mut Vec<Interval>, region: Interval) {
    claimed.push(region);
    claimed.sort();
    let mut merged: Vec<Interval> = Vec::with_capacity(claimed.len());
    for interval in claimed.drain(..) {
        match merged.last_mut() {
            Some(last) if interval.from <= last.to => last.to = last.to.max(interval.to),
            _ => merged.push(interval),
        }
    }
    *claimed = merged;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(system_from: i64, op: Op, from: i64, to: i64, value: &[u8]) -> Event {
        Event {
            precedence: Precedence { system_from, tx: system_from, ordinal: 0 },
            op,
            valid: Interval { from, to },
            assertion_ordinal: None,
            commit_position: 0,
            image: (op == Op::Put).then(|| Image::from([("v".to_owned(), value.to_vec())])),
        }
    }

    const ALL: Cut = Cut { system: i64::MAX, tx: i64::MAX, mask: None };

    #[test]
    fn events_round_trip_through_their_key_and_value() {
        let mut put = event(5, Op::Put, 1, VALID_MAX, b"x");
        put.assertion_ordinal = Some(3);
        put.commit_position = 7;
        let decoded = Event::decode(Precedence::decode(&put.precedence.encode()).unwrap(), &put.encode_value()).unwrap();
        assert_eq!(decoded, put);
        let delete = event(6, Op::Delete, 1, 10, b"");
        assert_eq!(Event::decode(delete.precedence, &delete.encode_value()).unwrap(), delete);
        let low = Precedence { system_from: -1, tx: 0, ordinal: 0 }.encode();
        let high = Precedence { system_from: 1, tx: 0, ordinal: 0 }.encode();
        assert!(low < high);
    }

    #[test]
    fn later_events_claim_valid_time_first() {
        let events = [event(1, Op::Put, 0, VALID_MAX, b"a"), event(2, Op::Put, 10, 20, b"b"), event(3, Op::Delete, 15, 30, b"")];
        let image = |at| point(&events, &ALL, at).present().map(|event| event.image.clone().unwrap()["v"].clone());
        assert_eq!(image(5), Some(b"a".to_vec()));
        assert_eq!(image(12), Some(b"b".to_vec()));
        assert_eq!(image(16), None);
        assert!(matches!(point(&events, &ALL, 16), Resolved::Deleted(_)));
        assert_eq!(image(40), Some(b"a".to_vec()));
        let history = slices(&events, &ALL, Interval::ALL).unwrap();
        let regions = history.iter().map(|slice| (slice.valid.from, slice.valid.to)).collect::<Vec<_>>();
        assert_eq!(regions, [(0, 10), (10, 15), (30, VALID_MAX)]);
        // At system cut 2 the delete is not yet visible.
        let before = Cut { system: 2, tx: 2, mask: None };
        assert_eq!(slices(&events, &before, Interval::ALL).unwrap().len(), 3);
    }

    #[test]
    fn a_position_mask_reads_a_prefix_of_one_publication() {
        let mut first = event(4, Op::Put, 0, VALID_MAX, b"a");
        first.commit_position = 0;
        let mut second = event(4, Op::Put, 0, VALID_MAX, b"b");
        second.precedence.ordinal = 1;
        second.commit_position = 1;
        let events = [first, second];
        let masked = Cut { mask: Some(PositionMask { commit_micros: 4, first_excluded: 1 }), ..ALL };
        assert_eq!(point(&events, &masked, 0).present().unwrap().commit_position, 0);
        let whole = Cut { mask: Some(PositionMask { commit_micros: 4, first_excluded: 0 }), ..ALL };
        assert_eq!(point(&events, &whole, 0), Resolved::Absent);
    }

    #[test]
    fn a_redacted_region_is_an_error_to_read() {
        let events = [event(1, Op::Put, 0, VALID_MAX, b"a"), event(2, Op::Redacted, 0, VALID_MAX, b"")];
        assert!(matches!(point(&events, &ALL, 0), Resolved::Redacted(_)));
        assert!(slices(&events, &ALL, Interval::ALL).is_err());
    }
}
