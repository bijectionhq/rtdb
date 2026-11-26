//! The store's primitives: versions, half-open key ranges and entries.
//!
//! These are the types the `Persistence` port (`fact_store::persistence`)
//! reads and appends with, and the types a ranked read consults. They are
//! plain data: the store's conflict rule stays `fact_store`'s.

/// A commit position within one store incarnation. Versions only increase.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct Version(pub u64);

/// A half-open key range `[start, end)`; `end: None` is unbounded.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct KeyRange {
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
}

impl KeyRange {
    /// Every key that starts with `prefix`.
    pub fn prefix(prefix: &[u8]) -> Self {
        Self { start: prefix.to_vec(), end: prefix_successor(prefix) }
    }

    /// Exactly one key.
    pub fn point(key: &[u8]) -> Self {
        let mut end = key.to_vec();
        end.push(0);
        Self { start: key.to_vec(), end: Some(end) }
    }

    pub fn is_empty(&self) -> bool {
        self.end.as_ref().is_some_and(|end| *end <= self.start)
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        self.start.as_slice() <= key && self.end.as_deref().is_none_or(|end| key < end)
    }
}

/// One live entry: its key, its projected value (absent on a keys-only read)
/// and the version that wrote it.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Entry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub written: Version,
}

/// The smallest key greater than every key starting with `prefix`: the key
/// with its trailing `0xFF` bytes dropped and its last remaining byte
/// incremented, or `None` when no byte remains.
// TWIN (interim, not landable): of `fact_store::world::rule::prefix_successor`; the prover lane hoists the shared byte-order spec layer into store_read and deletes the twin.
pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let at = prefix.iter().rposition(|&byte| byte != u8::MAX)?;
    let mut end = prefix[..=at].to_vec();
    end[at] += 1;
    Some(end)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ranges_are_half_open_and_prefixes_are_closed_under_extension() {
        let range = KeyRange::prefix(b"ab");
        assert!(range.contains(b"ab"));
        assert!(range.contains(b"ab\xff\xff"));
        assert!(!range.contains(b"ac"));
        assert!(KeyRange::point(b"k").contains(b"k"));
        assert!(!KeyRange::point(b"k").contains(b"k\0"));
        assert_eq!(prefix_successor(b"a\xff\xff"), Some(b"b".to_vec()));
        assert_eq!(prefix_successor(b"\xff"), None);
        assert_eq!(prefix_successor(b""), None);
    }
}
