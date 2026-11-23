use std::convert::Infallible;
use std::sync::{Arc, OnceLock};

use rtdb_columnar::IidView;
use rtdb_types::{Anomaly, Iid, MemoryBudget, MemoryReservation, Result};
use serde::{Deserialize, Serialize};

use crate::bucketer::invalid_bucket_depth;
use crate::leaf_load_step::validate_persisted_order;
use crate::leaf_sort_step::{merge_overflow_rows, merge_pending_rows};
use crate::leaf_split_step::{split_subtree, SplitContents};
use crate::partition_step::group_base4_rows;
use crate::{Bucketer, Path};

/// Indexed rows must keep the same IID for every operation on a retained trie.
pub trait IidReader {
    fn iid(&self, row: u32) -> Result<Iid>;
}

impl IidReader for [Iid] {
    fn iid(&self, row: u32) -> Result<Iid> {
        self.get(row as usize).copied().ok_or_else(|| Anomaly::incorrect("rtdb/trie-row", "Trie row exceeds IID vector length"))
    }
}
impl IidReader for Vec<Iid> { fn iid(&self, row: u32) -> Result<Iid> { self.as_slice().iid(row) } }
impl IidReader for IidView<'_> { fn iid(&self, row: u32) -> Result<Iid> { self.get(row as usize) } }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TrieConfig { log_limit: usize, page_limit: usize }

impl TrieConfig {
    pub fn new(log_limit: usize, page_limit: usize) -> Result<Self> {
        if log_limit == 0 || page_limit == 0 {
            return Err(Anomaly::incorrect("rtdb/trie-limits", "Trie log and page limits must be positive"));
        }
        Ok(Self { log_limit, page_limit })
    }
    pub fn log_limit(self) -> usize { self.log_limit }
    pub fn page_limit(self) -> usize { self.page_limit }
}
impl Default for TrieConfig { fn default() -> Self { Self { log_limit: 64, page_limit: 1024 } } }

#[derive(Debug)]
pub struct Leaf {
    path: Path,
    data: Arc<[u32]>,
    log: Arc<[u32]>,
    sorted: OnceLock<Arc<[u32]>>,
}

impl Leaf {
    fn empty(path: Path) -> Self { Self::new(path, Arc::from([]), Arc::from([])) }
    fn new(path: Path, data: Arc<[u32]>, log: Arc<[u32]>) -> Self { Self { path, data, log, sorted: OnceLock::new() } }
    pub fn path(&self) -> &Path { &self.path }
    pub fn len(&self) -> usize { self.data.len() + self.log.len() }
    pub fn is_empty(&self) -> bool { self.len() == 0 }
    pub fn data(&self) -> &[u32] { &self.data }
    pub fn pending(&self) -> &[u32] { &self.log }

    pub fn sorted_rows<R: IidReader + ?Sized>(&self, reader: &R) -> Result<Arc<[u32]>> {
        if self.log.is_empty() { return Ok(Arc::clone(&self.data)); }
        if let Some(sorted) = self.sorted.get() { return Ok(Arc::clone(sorted)); }
        let merged: Arc<[u32]> = self.merge_rows(reader)?.into();
        let _ = self.sorted.set(Arc::clone(&merged));
        Ok(merged)
    }

    /// Copies and orders rows in the caller's budget without consulting or populating the shared leaf cache.
    pub fn sorted_rows_in<R: IidReader + ?Sized>(&self, reader: &R, memory: &Arc<MemoryBudget>) -> Result<(Vec<u32>, Arc<MemoryReservation>)> {
        let overflow = || Anomaly::incorrect("rtdb/trie-sort-allocation", "Trie sorting allocation exceeds addressable memory");
        let bytes = self.len().checked_mul(std::mem::size_of::<u32>()).and_then(|bytes| bytes.checked_add(256))
            .ok_or_else(overflow)?;
        let retained = memory.reserve(bytes)?;
        if self.log.is_empty() { return Ok((self.data.to_vec(), retained)); }
        let scratch = self.log.len().checked_mul(std::mem::size_of::<crate::leaf_sort_step::KeyedRow>() + std::mem::size_of::<u32>())
            .and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?;
        let _scratch = memory.reserve(scratch)?;
        Ok((self.merge_rows(reader)?, retained))
    }

    fn merge_rows<R: IidReader + ?Sized>(&self, reader: &R) -> Result<Vec<u32>> {
        merge_pending_rows(&self.data, self.log.iter().copied(), |row| reader.iid(row).map(|iid| iid.0))
    }
}

#[derive(Debug)]
pub enum TrieNode {
    Branch { path: Path, children: [Option<Arc<TrieNode>>; 4] },
    Leaf(Leaf),
}

impl TrieNode {
    pub fn path(&self) -> &Path { match self { Self::Branch { path, .. } => path, Self::Leaf(leaf) => leaf.path() } }
    fn try_visit_leaves<'a, E>(
        &'a self, visit: &mut impl FnMut(&'a Leaf) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        match self {
            Self::Leaf(leaf) => visit(leaf),
            Self::Branch { children, .. } => {
                for child in children.iter().flatten() {
                    child.try_visit_leaves(visit)?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct MemoryHashTrie { root: Arc<TrieNode>, config: TrieConfig }

impl MemoryHashTrie {
    pub fn new(config: TrieConfig) -> Self { Self { root: Arc::new(TrieNode::Leaf(Leaf::empty(Path::default()))), config } }
    pub fn with_root_path(config: TrieConfig, path: Path) -> Result<Self> {
        Bucketer::DEFAULT.validate(&path)?;
        Ok(Self { root: Arc::new(TrieNode::Leaf(Leaf::empty(path))), config })
    }
    pub fn root(&self) -> &Arc<TrieNode> { &self.root }
    pub fn config(&self) -> TrieConfig { self.config }

    /// Visits borrowed leaves in depth-first bucket order and stops at the first visitor error.
    /// The visitor also receives empty leaf nodes.
    pub fn try_visit_leaves<'a, E>(
        &'a self, mut visit: impl FnMut(&'a Leaf) -> std::result::Result<(), E>,
    ) -> std::result::Result<(), E> {
        self.root.try_visit_leaves(&mut visit)
    }

    pub fn leaves(&self) -> Vec<&Leaf> {
        let mut leaves = Vec::new();
        self.try_visit_leaves::<Infallible>(|leaf| {
            leaves.push(leaf);
            Ok(())
        }).unwrap();
        leaves
    }

    pub fn add<R: IidReader + ?Sized>(&self, reader: &R, row: u32) -> Result<Self> { self.add_all(reader, &[row]) }

    pub fn add_all<R: IidReader + ?Sized>(&self, reader: &R, rows: &[u32]) -> Result<Self> {
        self.add_rows(reader, rows.iter().copied())
    }

    /// Returns a trie with the indexes in `start..start + count` added.
    /// A zero count returns a clone that shares this root.
    ///
    /// # Errors
    ///
    /// An exclusive end outside `u32` returns `rtdb/trie-row-overflow` before any IID lookup.
    /// Reader errors propagate.
    /// Validation stops with `rtdb/trie-root-path` at the first IID outside this root's path.
    pub fn add_range<R: IidReader + ?Sized>(&self, reader: &R, start: u32, count: u32) -> Result<Self> {
        let end = start.checked_add(count).ok_or_else(|| Anomaly::incorrect("rtdb/trie-row-overflow", "Trie row range exceeds u32"))?;
        self.add_rows(reader, start..end)
    }

    fn add_rows<R: IidReader + ?Sized>(&self, reader: &R, rows: impl ExactSizeIterator<Item = u32> + Clone) -> Result<Self> {
        for row in rows.clone() {
            if !Bucketer::DEFAULT.contains(self.root.path(), reader.iid(row)?)? {
                return Err(Anomaly::incorrect("rtdb/trie-root-path", "Added IID lies outside the trie's root path"));
            }
        }
        if rows.len() == 0 { return Ok(self.clone()); }
        Ok(Self { root: insert_rows(&self.root, reader, rows, self.config)?, config: self.config })
    }

    pub fn compact_logs<R: IidReader + ?Sized>(&self, reader: &R) -> Result<Self> {
        Ok(Self { root: compact(&self.root, reader, self.config)?, config: self.config })
    }

    pub fn sorted_rows<R: IidReader + ?Sized>(&self, reader: &R) -> Result<Vec<u32>> {
        let mut rows = Vec::new();
        for leaf in self.leaves() { rows.extend_from_slice(&leaf.sorted_rows(reader)?); }
        Ok(rows)
    }

    /// Compacts pending logs before exporting nodes for a protobuf codec.
    pub fn persisted<R: IidReader + ?Sized>(&self, reader: &R) -> Result<PersistedTrie> {
        let compacted = self.compact_logs(reader)?;
        Ok(PersistedTrie { log_limit: self.config.log_limit, page_limit: self.config.page_limit,
            root_path: self.root.path().as_slice().to_vec(), root: export(&compacted.root) })
    }

    /// Validates every persisted row against its reconstructed path and sort order.
    pub fn from_persisted<R: IidReader + ?Sized>(value: PersistedTrie, reader: &R) -> Result<Self> {
        let config = TrieConfig::new(value.log_limit, value.page_limit)?;
        let path = Path::new(value.root_path, Bucketer::DEFAULT)?;
        Ok(Self { root: import(value.root, path, reader)?, config })
    }
}

impl Default for MemoryHashTrie { fn default() -> Self { Self::new(TrieConfig::default()) } }

fn insert_rows<R: IidReader + ?Sized>(
    node: &Arc<TrieNode>, reader: &R, rows: impl ExactSizeIterator<Item = u32>, config: TrieConfig,
) -> Result<Arc<TrieNode>> {
    match node.as_ref() {
        TrieNode::Branch { path, children } => {
            let buckets = group_base4_rows(
                rows, path.len(), |row| reader.iid(row).map(|iid| iid.0), invalid_bucket_depth,
            )?;
            let mut next = children.clone();
            for (bucket, rows) in buckets.iter().enumerate() {
                if rows.is_empty() { continue; }
                let child = match &children[bucket] {
                    Some(child) => Arc::clone(child),
                    None => Arc::new(TrieNode::Leaf(Leaf::empty(path.child(bucket as u8, Bucketer::DEFAULT)?))),
                };
                next[bucket] = Some(insert_rows(&child, reader, rows.iter().copied(), config)?);
            }
            Ok(Arc::new(TrieNode::Branch { path: path.clone(), children: next }))
        }
        TrieNode::Leaf(leaf) => {
            let count = leaf.log.len().checked_add(rows.len()).ok_or_else(|| Anomaly::incorrect("rtdb/trie-size", "Trie pending row count overflow"))?;
            if count <= config.log_limit {
                let mut log = leaf.log.to_vec();
                log.extend(rows);
                let next = Arc::new(TrieNode::Leaf(Leaf::new(leaf.path.clone(), Arc::clone(&leaf.data), log.into())));
                if count == config.log_limit { compact(&next, reader, config) } else { Ok(next) }
            } else {
                let merged = merge_overflow_rows(&leaf.data, leaf.log.iter().copied(), rows, |row| {
                    reader.iid(row).map(|iid| iid.0)
                })?;
                build_subtree(leaf.path.clone(), merged, reader, config, true)
            }
        }
    }
}

fn compact<R: IidReader + ?Sized>(node: &Arc<TrieNode>, reader: &R, config: TrieConfig) -> Result<Arc<TrieNode>> {
    match node.as_ref() {
        TrieNode::Leaf(leaf) if leaf.log.is_empty() => Ok(Arc::clone(node)),
        TrieNode::Leaf(leaf) => build_subtree(leaf.path.clone(), leaf.sorted_rows(reader)?.to_vec(), reader, config, false),
        TrieNode::Branch { path, children } => {
            let mut next = children.clone();
            for (i, child) in children.iter().enumerate() {
                if let Some(child) = child { next[i] = Some(compact(child, reader, config)?); }
            }
            Ok(Arc::new(TrieNode::Branch { path: path.clone(), children: next }))
        }
    }
}

fn build_subtree<R: IidReader + ?Sized>(path: Path, rows: Vec<u32>, reader: &R, config: TrieConfig, recursive: bool) -> Result<Arc<TrieNode>> {
    split_subtree(
        path.into_buckets(),
        rows,
        |row| reader.iid(row).map(|iid| iid.0),
        invalid_bucket_depth,
        |buckets, contents| {
            let path = Path::from_split_buckets(buckets);
            Arc::new(match contents {
                SplitContents::Leaf(rows) => TrieNode::Leaf(Leaf::new(path, rows.into(), Arc::from([]))),
                SplitContents::Branch(children) => TrieNode::Branch { path, children },
            })
        },
        config.page_limit,
        recursive,
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PersistedNode { Branch(Vec<Option<PersistedNode>>), Leaf(Vec<u32>) }

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistedTrie {
    pub log_limit: usize,
    pub page_limit: usize,
    pub root_path: Vec<u8>,
    pub root: PersistedNode,
}

fn export(node: &TrieNode) -> PersistedNode {
    match node {
        TrieNode::Leaf(leaf) => PersistedNode::Leaf(leaf.data.to_vec()),
        TrieNode::Branch { children, .. } => PersistedNode::Branch(children.iter().map(|child| child.as_ref().map(|node| export(node))).collect()),
    }
}

fn import<R: IidReader + ?Sized>(node: PersistedNode, path: Path, reader: &R) -> Result<Arc<TrieNode>> {
    match node {
        PersistedNode::Leaf(rows) => {
            for row in &rows {
                if !Bucketer::DEFAULT.contains(&path, reader.iid(*row)?)? {
                    return Err(Anomaly::incorrect("rtdb/trie-row-path", "Persisted row lies outside its trie path"));
                }
            }
            validate_persisted_order(&rows, |row| reader.iid(row).map(|iid| iid.0), || {
                Anomaly::incorrect("rtdb/trie-row-order", "Persisted trie rows are out of IID/newest-first order")
            })?;
            Ok(Arc::new(TrieNode::Leaf(Leaf::new(path, rows.into(), Arc::from([])))))
        }
        PersistedNode::Branch(children) => {
            if children.len() != 4 || path.len() == Bucketer::DEFAULT.max_depth() {
                return Err(Anomaly::incorrect("rtdb/trie-branch", "Persisted IID branch requires four children below maximum depth"));
            }
            let mut nodes = std::array::from_fn(|_| None);
            for (bucket, child) in children.into_iter().enumerate() {
                if let Some(child) = child { nodes[bucket] = Some(import(child, path.child(bucket as u8, Bucketer::DEFAULT)?, reader)?); }
            }
            Ok(Arc::new(TrieNode::Branch { path, children: nodes }))
        }
    }
}
