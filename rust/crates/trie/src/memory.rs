use std::cmp::Ordering;
use std::sync::{Arc, OnceLock};

use rtdb_columnar::IidView;
use rtdb_types::{Anomaly, Iid, Result};
use serde::{Deserialize, Serialize};

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
        let log = sort(reader, &self.log)?;
        let merged: Arc<[u32]> = merge(reader, &self.data, &log)?.into();
        let _ = self.sorted.set(Arc::clone(&merged));
        Ok(merged)
    }
}

#[derive(Debug)]
pub enum TrieNode {
    Branch { path: Path, children: [Option<Arc<TrieNode>>; 4] },
    Leaf(Leaf),
}

impl TrieNode {
    pub fn path(&self) -> &Path { match self { Self::Branch { path, .. } => path, Self::Leaf(leaf) => leaf.path() } }
    fn leaves<'a>(&'a self, leaves: &mut Vec<&'a Leaf>) {
        match self {
            Self::Leaf(leaf) => leaves.push(leaf),
            Self::Branch { children, .. } => for child in children.iter().flatten() { child.leaves(leaves); },
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
    pub fn leaves(&self) -> Vec<&Leaf> { let mut leaves = Vec::new(); self.root.leaves(&mut leaves); leaves }

    pub fn add<R: IidReader + ?Sized>(&self, reader: &R, row: u32) -> Result<Self> { self.add_all(reader, &[row]) }

    pub fn add_all<R: IidReader + ?Sized>(&self, reader: &R, rows: &[u32]) -> Result<Self> {
        for row in rows {
            if !Bucketer::DEFAULT.contains(self.root.path(), reader.iid(*row)?)? {
                return Err(Anomaly::incorrect("rtdb/trie-root-path", "Added IID lies outside the trie's root path"));
            }
        }
        if rows.is_empty() { return Ok(self.clone()); }
        Ok(Self { root: add_all(&self.root, reader, rows, self.config)?, config: self.config })
    }

    pub fn add_range<R: IidReader + ?Sized>(&self, reader: &R, start: u32, count: u32) -> Result<Self> {
        let end = start.checked_add(count).ok_or_else(|| Anomaly::incorrect("rtdb/trie-row-overflow", "Trie row range exceeds u32"))?;
        self.add_all(reader, &(start..end).collect::<Vec<_>>())
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

fn compare<R: IidReader + ?Sized>(reader: &R, left: u32, right: u32) -> Result<Ordering> {
    Ok(reader.iid(left)?.cmp(&reader.iid(right)?).then_with(|| right.cmp(&left)))
}

fn sort<R: IidReader + ?Sized>(reader: &R, rows: &[u32]) -> Result<Vec<u32>> {
    let mut keyed = rows.iter().map(|row| Ok((reader.iid(*row)?, *row))).collect::<Result<Vec<_>>>()?;
    keyed.sort_unstable_by(|a, b| a.0.cmp(&b.0).then_with(|| b.1.cmp(&a.1)));
    Ok(keyed.into_iter().map(|(_, row)| row).collect())
}

fn merge<R: IidReader + ?Sized>(reader: &R, left: &[u32], right: &[u32]) -> Result<Vec<u32>> {
    let mut merged = Vec::with_capacity(left.len() + right.len());
    let (mut li, mut ri) = (0, 0);
    while li < left.len() && ri < right.len() {
        if compare(reader, left[li], right[ri])? == Ordering::Less { merged.push(left[li]); li += 1; }
        else { merged.push(right[ri]); ri += 1; }
    }
    merged.extend_from_slice(&left[li..]);
    merged.extend_from_slice(&right[ri..]);
    Ok(merged)
}

fn add_all<R: IidReader + ?Sized>(node: &Arc<TrieNode>, reader: &R, rows: &[u32], config: TrieConfig) -> Result<Arc<TrieNode>> {
    match node.as_ref() {
        TrieNode::Branch { path, children } => {
            let mut buckets: [Vec<u32>; 4] = std::array::from_fn(|_| Vec::new());
            for row in rows { buckets[Bucketer::DEFAULT.bucket(reader.iid(*row)?, path.len())? as usize].push(*row); }
            let mut next = children.clone();
            for (bucket, rows) in buckets.iter().enumerate() {
                if rows.is_empty() { continue; }
                let child = match &children[bucket] {
                    Some(child) => Arc::clone(child),
                    None => Arc::new(TrieNode::Leaf(Leaf::empty(path.child(bucket as u8, Bucketer::DEFAULT)?))),
                };
                next[bucket] = Some(add_all(&child, reader, rows, config)?);
            }
            Ok(Arc::new(TrieNode::Branch { path: path.clone(), children: next }))
        }
        TrieNode::Leaf(leaf) => {
            let count = leaf.log.len().checked_add(rows.len()).ok_or_else(|| Anomaly::incorrect("rtdb/trie-size", "Trie pending row count overflow"))?;
            if count <= config.log_limit {
                let mut log = leaf.log.to_vec();
                log.extend_from_slice(rows);
                let next = Arc::new(TrieNode::Leaf(Leaf::new(leaf.path.clone(), Arc::clone(&leaf.data), log.into())));
                if count == config.log_limit { compact(&next, reader, config) } else { Ok(next) }
            } else {
                let new_rows = sort(reader, rows)?;
                let pending = sort(reader, &leaf.log)?;
                let merged = merge(reader, &leaf.data, &merge(reader, &pending, &new_rows)?)?;
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
    if rows.len() <= config.page_limit || path.len() == Bucketer::DEFAULT.max_depth() {
        return Ok(Arc::new(TrieNode::Leaf(Leaf::new(path, rows.into(), Arc::from([])))));
    }
    let mut buckets: [Vec<u32>; 4] = std::array::from_fn(|_| Vec::new());
    for row in rows { buckets[Bucketer::DEFAULT.bucket(reader.iid(row)?, path.len())? as usize].push(row); }
    let mut children = std::array::from_fn(|_| None);
    for (bucket, rows) in buckets.into_iter().enumerate() {
        if rows.is_empty() { continue; }
        let child_path = path.child(bucket as u8, Bucketer::DEFAULT)?;
        children[bucket] = Some(if recursive { build_subtree(child_path, rows, reader, config, true)? }
        else { Arc::new(TrieNode::Leaf(Leaf::new(child_path, rows.into(), Arc::from([])))) });
    }
    Ok(Arc::new(TrieNode::Branch { path, children }))
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
            for pair in rows.windows(2) {
                if compare(reader, pair[0], pair[1])? == Ordering::Greater {
                    return Err(Anomaly::incorrect("rtdb/trie-row-order", "Persisted trie rows are out of IID/newest-first order"));
                }
            }
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
