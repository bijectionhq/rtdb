use std::collections::BTreeMap;
use rtdb_format::keys::TrieKey;
use rtdb_index::{CatalogTrie, Shard, TrieCatalog};
use rtdb_types::{Result, TableRef};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecencyMode { Partition, Preserve }

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionJob {
    pub table: TableRef,
    pub inputs: Vec<TrieKey>,
    pub part: Vec<u8>,
    pub output: TrieKey,
    pub recency_mode: RecencyMode,
}

impl CompactionJob {
    pub fn key(&self) -> (TableRef, TrieKey) { (self.table.clone(), self.output.clone()) }
}

pub fn available_jobs(catalog: &TrieCatalog) -> Result<Vec<CompactionJob>> {
    let mut jobs = Vec::new();
    let target = catalog.file_size_target();
    let epoch = catalog.naming_epoch();
    for table in catalog.tables() {
        let mut shards: BTreeMap<Shard, Vec<&CatalogTrie>> = BTreeMap::new();
        for trie in catalog.current_tries(table) { shards.entry(Shard::of(&trie.key)).or_default().push(trie); }
        for tries in shards.values_mut() { tries.sort_by_key(|trie| trie.key.block_index()); }
        let root = |level, recency| Shard { level, recency, part: Vec::new() };
        if let Some(l0) = shards.get(&root(0, None)).and_then(|tries| tries.first()) {
            let mut inputs = Vec::new();
            if let Some(partial) = shards.get(&root(1, None)).and_then(|tries| tries.last()).filter(|trie| trie.data_file_size < target) {
                inputs.push(partial.key.clone());
            }
            inputs.push(l0.key.clone());
            jobs.push(CompactionJob { table: table.clone(), inputs, part: Vec::new(),
                output: TrieKey::new(1, None, Vec::new(), l0.key.block_index())?.with_epoch(epoch), recency_mode: RecencyMode::Partition });
        }
        for (shard, l1h) in &shards {
            if shard.level != 1 || shard.recency.is_none() { continue; }
            let partial = shards.get(&root(2, shard.recency)).and_then(|tries| tries.last()).filter(|trie| trie.data_file_size < target);
            let mut inputs: Vec<&CatalogTrie> = partial.into_iter().copied().collect();
            let mut size = inputs.iter().map(|trie| trie.data_file_size).sum::<i64>();
            for trie in l1h {
                inputs.push(trie);
                size = size.checked_add(trie.data_file_size).ok_or_else(|| crate::invalid("Compaction input size overflow"))?;
                if inputs.len() == 4 || size >= target {
                    jobs.push(CompactionJob { table: table.clone(), inputs: inputs.iter().map(|trie| trie.key.clone()).collect(), part: Vec::new(),
                        output: TrieKey::new(2, shard.recency, Vec::new(), trie.key.block_index())?.with_epoch(epoch), recency_mode: RecencyMode::Preserve });
                    break;
                }
            }
        }
        for (shard, tries) in &shards {
            if shard.level == 0 || (shard.level == 1 && shard.recency.is_some()) { continue; }
            let candidates: Vec<_> = tries.iter().copied().filter(|trie| {
                !(shard.level == 1 || (shard.level == 2 && shard.recency.is_some())) || trie.data_file_size >= target
            }).collect();
            if candidates.len() < 4 { continue; }
            let next_level = shard.level.checked_add(1).ok_or_else(|| crate::invalid("Compaction level overflow"))?;
            for bucket in 0..4 {
                let mut part = shard.part.clone();
                part.push(bucket);
                let output_shard = Shard { level: next_level, recency: shard.recency, part: part.clone() };
                let floor = catalog.max_block_index(table, &output_shard);
                let inputs: Vec<_> = candidates.iter().copied().filter(|trie| trie.key.block_index() > floor).take(4).collect();
                if inputs.len() == 4 {
                    jobs.push(CompactionJob { table: table.clone(), inputs: inputs.iter().map(|trie| trie.key.clone()).collect(),
                        output: TrieKey::new(next_level, shard.recency, part.clone(), inputs[3].key.block_index())?.with_epoch(epoch),
                        part, recency_mode: RecencyMode::Preserve });
                }
            }
        }
    }
    Ok(jobs)
}
