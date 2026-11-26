//! The stored key layout (RTDB-PLAN §3): every key is
//!
//! ```text
//! partition prefix ‖ family ‖ world ‖ segments
//! ```
//!
//! The partition prefix is the label's ([`crate::partition`]); the family
//! byte is the kernel's key family; the world is one segment of its 16 id
//! bytes; the segments are [`crate::segment`]'s. `kernel_storage::persistence::layout`
//! builds kernel keys over it from labels and worlds.
use std::ops::Bound;

use crate::{
    partition,
    segment,
    store::{
        prefix_successor,
        KeyRange,
    },
};

/// Where a key lives: its label's 16-byte partition id and its world's
/// 16-byte id (the Root world is all zeroes).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub struct Place {
    pub partition: [u8; 16],
    pub world: [u8; 16],
}

/// The Root world's id.
pub const ROOT_WORLD: [u8; 16] = [0; 16];

/// The key families a read of owned relations consults (the values of
/// `kernel_storage::key::Family`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Family {
    /// Rows.
    Row = 16,
    /// Kernel-written index entries.
    Index = 17,
    /// The engine's catalog and persistence records.
    Persistence = 32,
}

/// The family byte and world segment every key of a family and world starts with.
pub fn family_world(family: u8, world: [u8; 16]) -> Vec<u8> {
    let mut bytes = vec![family];
    bytes.extend_from_slice(&segment::segment(&world));
    bytes
}

/// The stored key of `segments` (encoded segments after the family byte).
pub fn stored(place: Place, family: u8, segments: &[u8]) -> Vec<u8> {
    let mut inner = family_world(family, place.world);
    inner.extend_from_slice(segments);
    partition::key(&place.partition, &inner)
}

/// The stored range of `inner`, a range over the segments after the family
/// byte within one family and world. `end: None` is the rest of that
/// family and world.
pub fn stored_range(place: Place, family: u8, inner: &KeyRange) -> KeyRange {
    let base = family_world(family, place.world);
    let start = [base.as_slice(), &inner.start].concat();
    let end = match &inner.end {
        Some(end) => Some([base.as_slice(), end].concat()),
        None => prefix_successor(&base),
    };
    partition::range(&place.partition, &KeyRange { start, end })
}

/// A field name as its key segment.
pub fn field_name(name: &str) -> Vec<u8> {
    segment::segment(name.as_bytes())
}

/// The index's entries whose value lies within `(low, high)`, as one range
/// within `place`'s partition and world. Escaping preserves byte order and a
/// shorter segment sorts before its extensions, so the stored range holds
/// exactly the entries whose value is within the bounds.
pub fn index_values(place: Place, index: &[u8; 16], low: Bound<&[u8]>, high: Bound<&[u8]>) -> KeyRange {
    let base = segment::segment(index);
    let terminated = |value: &[u8]| [base.as_slice(), &segment::segment(value)].concat();
    let start = match low {
        Bound::Unbounded => base.clone(),
        Bound::Included(value) => [base.as_slice(), &segment::escaped(value)].concat(),
        Bound::Excluded(value) => prefix_successor(&terminated(value)).expect("A terminated segment ends below 0xff"),
    };
    let end = match high {
        Bound::Unbounded => prefix_successor(&base),
        Bound::Included(value) => prefix_successor(&terminated(value)),
        Bound::Excluded(value) => Some([base.as_slice(), &segment::escaped(value)].concat()),
    };
    stored_range(place, Family::Index as u8, &KeyRange { start, end })
}
