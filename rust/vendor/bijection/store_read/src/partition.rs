//! Labels as key partitions: every stored key starts with its label's
//! partition prefix, the partition id length-prefixed so that no prefix
//! extends another (`fact_store::persistence::partition` admits reads over
//! them).
use crate::store::KeyRange;

/// The partition prefix of `id`: a big-endian `u16` length, then the id.
/// Distinct ids give prefix-free prefixes.
pub fn prefix(id: &[u8]) -> Vec<u8> {
    let len = u16::try_from(id.len()).expect("a partition id has at most 65535 bytes");
    let mut prefix = Vec::with_capacity(2 + id.len());
    prefix.extend_from_slice(&len.to_be_bytes());
    prefix.extend_from_slice(id);
    prefix
}

/// The stored key of `key` in partition `id`.
pub fn key(id: &[u8], key: &[u8]) -> Vec<u8> {
    let mut stored = prefix(id);
    stored.extend_from_slice(key);
    stored
}

/// The stored range of `inner` (a range of keys within one partition).
pub fn range(id: &[u8], inner: &KeyRange) -> KeyRange {
    let start = key(id, &inner.start);
    let end = match &inner.end {
        Some(end) => Some(key(id, end)),
        None => KeyRange::prefix(&prefix(id)).end,
    };
    KeyRange { start, end }
}
