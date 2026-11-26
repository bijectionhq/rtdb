//! The key scope of a store grant (`rtdb.store.v1`): the key prefixes the
//! grant may read and the key prefixes it may write.
//!
//! The store knows no relations, partitions' meanings or schemas, only keys.
//! The issuer derives a grant's prefixes from the relations and partitions its
//! role touches; RTDB admits a read range only inside one read prefix and a
//! write only under one write prefix, and refuses anything else before any
//! effect. Every prefix starts with one whole partition prefix (a big-endian
//! `u16` length, then that many bytes), so a scope names exact partitions.
use serde::{Deserialize, Serialize};

pub(crate) mod prefix;

/// A grant's prefixes, each a byte array in UserInfo.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StoreScope {
    /// Every key range a Read, an Append's read guards and a Watch name lies
    /// inside one of these.
    pub reads: Vec<Vec<u8>>,
    /// Every key an Append writes, stamped or not, and every bounded counter
    /// it updates lies under one of these.
    pub writes: Vec<Vec<u8>>,
}

impl StoreScope {
    /// The most prefixes one scope names, reads and writes together.
    pub const MAX_PREFIXES: usize = 64;
    /// The longest prefix.
    pub const MAX_PREFIX_BYTES: usize = 512;

    /// Bounded, nonempty reads, and every prefix a whole partition's.
    pub(crate) fn valid(&self) -> bool {
        !self.reads.is_empty()
            && self.reads.len() + self.writes.len() <= Self::MAX_PREFIXES
            && self.reads.iter().chain(&self.writes).all(|prefix| {
                (1..=Self::MAX_PREFIX_BYTES).contains(&prefix.len()) && prefix::names_partition(prefix)
            })
    }

    /// Whether every key of `[start, end)` (`end: None` is unbounded) lies
    /// under one read prefix.
    pub fn admits_read(&self, start: &[u8], end: Option<&[u8]>) -> bool {
        self.reads.iter().any(|prefix| prefix::range_within(prefix, start, end))
    }

    /// Whether `key` lies under one write prefix, with its stamp placeholder
    /// (16 bytes at `stamp`) past that prefix, so the stamped key does too.
    pub fn admits_write(&self, key: &[u8], stamp: Option<u32>) -> bool {
        self.writes.iter().any(|prefix| prefix::write_within(prefix, key, stamp))
    }

    pub(crate) fn memory_bytes(&self) -> usize {
        self.reads.iter().chain(&self.writes).map(|prefix| prefix.capacity() + 24).sum::<usize>() + 64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn partition(id: &[u8]) -> Vec<u8> {
        [&(id.len() as u16).to_be_bytes()[..], id].concat()
    }

    fn scope() -> StoreScope {
        let public = partition(&[1; 16]);
        StoreScope {
            reads: vec![[public.as_slice(), b"rows/"].concat(), [public.as_slice(), b"catalog/w"].concat()],
            writes: vec![[public.as_slice(), b"rows/"].concat()],
        }
    }

    #[test]
    fn a_scope_admits_only_ranges_and_keys_under_its_prefixes() {
        let scope = scope();
        assert!(scope.valid());
        let rows = [partition(&[1; 16]).as_slice(), b"rows/"].concat();
        let point = [rows.as_slice(), b"a"].concat();
        let after = [point.as_slice(), &[0]].concat();
        assert!(scope.admits_read(&point, Some(&after)));
        assert!(scope.admits_read(&rows, Some(&prefix::successor(&rows).unwrap())));
        assert!(!scope.admits_read(&rows, None), "an unbounded range leaves every prefix");
        let past = [partition(&[1; 16]).as_slice(), b"rows0a"].concat();
        assert!(!scope.admits_read(&point, Some(&past)), "a range running past its prefix refuses");
        let other = [partition(&[2; 16]).as_slice(), b"rows/a"].concat();
        assert!(!scope.admits_read(&other, Some(&[other.as_slice(), &[0]].concat())), "another partition refuses");
        assert!(scope.admits_write(&point, None));
        let stamped = [rows.as_slice(), &[0; 16]].concat();
        assert!(scope.admits_write(&stamped, Some(rows.len() as u32)));
        assert!(!scope.admits_write(&stamped, Some(rows.len() as u32 - 1)), "a stamp inside the prefix could leave it");
        let catalog = [partition(&[1; 16]).as_slice(), b"catalog/w"].concat();
        assert!(!scope.admits_write(&catalog, None), "a read prefix is not a write prefix");
    }

    #[test]
    fn a_scope_names_whole_partitions_within_its_bounds() {
        assert!(!StoreScope { reads: Vec::new(), writes: Vec::new() }.valid(), "a scope reads something");
        assert!(!StoreScope { reads: vec![vec![0, 16, 1]], writes: Vec::new() }.valid(), "a prefix holds its whole partition");
        let wide = StoreScope { reads: vec![partition(&[1; 16]); StoreScope::MAX_PREFIXES + 1], writes: Vec::new() };
        assert!(!wide.valid());
        let long = StoreScope { reads: vec![[partition(&[1; 16]), vec![7; StoreScope::MAX_PREFIX_BYTES]].concat()], writes: Vec::new() };
        assert!(!long.valid());
    }
}
