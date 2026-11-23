use std::cmp::Ordering;

use rtdb_types::{Anomaly, Iid, Result};
use serde::{Deserialize, Serialize};

pub(crate) fn invalid_bucket_depth() -> Anomaly {
    Anomaly::incorrect("rtdb/trie-depth", "IID bucketing exceeds 128 bits")
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bucketer { level_bits: u8 }

impl Bucketer {
    pub const DEFAULT: Self = Self { level_bits: 2 };

    pub fn new(level_bits: u8) -> Result<Self> {
        if matches!(level_bits, 2 | 4 | 8) { Ok(Self { level_bits }) }
        else { Err(Anomaly::incorrect("rtdb/trie-level-bits", "Trie levels consume 2, 4, or 8 bits")) }
    }

    pub fn width(self) -> usize { 1usize << self.level_bits }
    pub fn max_depth(self) -> usize { 128 / self.level_bits as usize }
    pub fn level_bits(self) -> u8 { self.level_bits }

    pub fn bucket(self, iid: Iid, level: usize) -> Result<u8> {
        if self == Self::DEFAULT {
            return crate::base4_bucket(&iid.0, level).ok_or_else(invalid_bucket_depth);
        }
        if level >= self.max_depth() { return Err(invalid_bucket_depth()); }
        let bit = level * self.level_bits as usize;
        Ok((iid.0[bit / 8] >> (8 - self.level_bits as usize - bit % 8)) & (self.width() - 1) as u8)
    }

    pub fn compare_path(self, iid: Iid, path: &Path) -> Result<Ordering> {
        self.validate(path)?;
        if self == Self::DEFAULT {
            return Ok(crate::partition_step::compare_base4_path(&iid.0, path.as_slice()));
        }
        for (level, value) in path.as_slice().iter().enumerate() {
            let order = self.bucket(iid, level)?.cmp(value);
            if order != Ordering::Equal { return Ok(order); }
        }
        Ok(Ordering::Equal)
    }

    pub fn start_iid(self, path: &Path) -> Result<Iid> {
        self.validate(path)?;
        let mut iid = [0; 16];
        for (level, bucket) in path.as_slice().iter().enumerate() {
            let bit = level * self.level_bits as usize;
            iid[bit / 8] |= bucket << (8 - self.level_bits as usize - bit % 8);
        }
        Ok(Iid(iid))
    }

    pub fn next_path(self, path: &Path) -> Result<Option<Path>> {
        self.validate(path)?;
        let mut next = path.0.clone();
        for value in next.iter_mut().rev() {
            if (*value as usize) + 1 < self.width() {
                *value += 1;
                return Ok(Some(Path(next)));
            }
            *value = 0;
        }
        Ok(None)
    }

    pub fn contains(self, path: &Path, iid: Iid) -> Result<bool> { Ok(self.compare_path(iid, path)? == Ordering::Equal) }

    pub fn validate(self, path: &Path) -> Result<()> {
        if path.len() <= self.max_depth() && path.0.iter().all(|bucket| (*bucket as usize) < self.width()) { Ok(()) }
        else { Err(Anomaly::incorrect("rtdb/trie-path", "Trie path contains an invalid bucket or exceeds IID length")) }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Path(Vec<u8>);

impl Path {
    pub fn new(buckets: Vec<u8>, bucketer: Bucketer) -> Result<Self> {
        let path = Self(buckets);
        bucketer.validate(&path)?;
        Ok(path)
    }
    pub fn as_slice(&self) -> &[u8] { &self.0 }
    pub fn len(&self) -> usize { self.0.len() }
    pub fn is_empty(&self) -> bool { self.0.is_empty() }
    pub(crate) fn into_buckets(self) -> Vec<u8> { self.0 }
    pub(crate) fn from_split_buckets(buckets: Vec<u8>) -> Self { Self(buckets) }
    pub fn child(&self, bucket: u8, bucketer: Bucketer) -> Result<Self> {
        let mut child = self.0.clone();
        child.push(bucket);
        Self::new(child, bucketer)
    }
}
