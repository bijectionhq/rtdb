use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, NaiveDate, Utc};
use rtdb_format::{block::proto as block_pb, catalog::TableBlock, keys::TrieKey, log::proto as log_pb};
use rtdb_types::{Anomaly, BlockIndex, Result, TableRef, Timestamp};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrieState { Nascent, Live, Garbage { as_of: Timestamp } }

#[derive(Clone, Debug, PartialEq)]
pub struct CatalogTrie {
    pub key: TrieKey,
    pub data_file_size: i64,
    pub metadata: Option<log_pb::TrieMetadata>,
    pub state: TrieState,
}

impl CatalogTrie {
    pub fn notification(key: TrieKey, data_file_size: i64, mut metadata: Option<log_pb::TrieMetadata>) -> Result<Self> {
        if data_file_size < 0 { return Err(invalid("Trie file size is negative")); }
        if let Some(metadata) = &mut metadata { metadata.iid_bloom = None; }
        Ok(Self { key, data_file_size, metadata, state: TrieState::Nascent })
    }

    pub fn from_proto(value: &log_pb::TrieDetails) -> Result<Self> {
        let key = value.trie_key.as_deref().unwrap_or_default().parse()?;
        let state = match value.trie_state.unwrap_or(0) {
            0 => TrieState::Nascent,
            1 => TrieState::Live,
            2 => TrieState::Garbage { as_of: Timestamp(value.garbage_as_of.unwrap_or_default()) },
            state => return Err(invalid(format!("Unknown trie state {state}"))),
        };
        let data_file_size = value.data_file_size.unwrap_or_default();
        if data_file_size < 0 { return Err(invalid("Trie file size is negative")); }
        Ok(Self { key, data_file_size, metadata: value.trie_metadata.clone(), state })
    }

    pub fn to_proto(&self, table: &TableRef) -> log_pb::TrieDetails {
        let (state, garbage_as_of) = match self.state {
            TrieState::Nascent => (0, None), TrieState::Live => (1, None),
            TrieState::Garbage { as_of } => (2, Some(as_of.0)),
        };
        log_pb::TrieDetails {
            table_name: Some(table.schema_and_table()), trie_key: Some(self.key.to_string()),
            data_file_size: Some(self.data_file_size), trie_metadata: self.metadata.clone(),
            trie_state: Some(state), garbage_as_of,
        }
    }

    fn garbage(&mut self, as_of: Timestamp) { self.state = TrieState::Garbage { as_of }; self.metadata = None; }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Shard { pub level: i64, pub recency: Option<NaiveDate>, pub part: Vec<u8> }

impl Shard {
    pub fn of(key: &TrieKey) -> Self { Self { level: key.level(), recency: key.recency(), part: key.part().to_vec() } }
    fn root(level: i64, recency: Option<NaiveDate>) -> Self { Self { level, recency, part: Vec::new() } }
}

#[derive(Clone, Debug)]
struct ShardValue { max_block_index: BlockIndex, tries: BTreeMap<BlockIndex, CatalogTrie> }
impl Default for ShardValue {
    fn default() -> Self { Self { max_block_index: BlockIndex(-1), tries: BTreeMap::new() } }
}
impl ShardValue {
    fn insert(&mut self, trie: CatalogTrie) {
        self.max_block_index = self.max_block_index.max(trie.key.block_index());
        self.tries.insert(trie.key.block_index(), trie);
    }
    fn promote(&mut self, block: BlockIndex) {
        if let Some(trie) = self.tries.get_mut(&block) {
            if trie.state == TrieState::Nascent { trie.state = TrieState::Live; }
        }
    }
    fn supersede(&mut self, block: BlockIndex, partial_limit: Option<i64>, as_of: Timestamp) {
        for (_, trie) in self.tries.range_mut(..=block) {
            if trie.state == TrieState::Live && partial_limit.is_none_or(|limit| trie.data_file_size < limit) {
                trie.garbage(as_of);
            }
        }
    }
}

#[derive(Clone, Debug, Default)]
struct TableTries {
    shards: BTreeMap<Shard, ShardValue>,
    l1h_recencies: BTreeMap<BlockIndex, BTreeSet<NaiveDate>>,
}

impl TableTries {
    fn shard_with_floor(&mut self, shard: Shard, block: BlockIndex) -> &mut ShardValue {
        self.shards.entry(shard).or_insert_with(|| ShardValue { max_block_index: block, ..ShardValue::default() })
    }
    fn drop_covered_l0(&mut self, block: BlockIndex) {
        self.shard_with_floor(Shard::root(0, None), block).tries.retain(|index, trie| *index > block || trie.state != TrieState::Live);
    }
    fn apply(&mut self, mut trie: CatalogTrie, file_size_target: i64, as_of: Timestamp) -> Result<()> {
        let shard = Shard::of(&trie.key);
        let block = trie.key.block_index();
        if self.shards.get(&shard).is_some_and(|state| state.max_block_index >= block) { return Ok(()); }
        if let Some(metadata) = &mut trie.metadata { metadata.iid_bloom = None; }
        match (shard.level, shard.recency) {
            (0, _) => { trie.state = TrieState::Live; self.shards.entry(shard).or_default().insert(trie); }
            (1, Some(recency)) => {
                let current = Shard { level: 1, recency: None, part: shard.part.clone() };
                trie.state = if self.shards.get(&current).is_some_and(|state| state.tries.range(block..).any(|(_, trie)| trie.state == TrieState::Live)) {
                    TrieState::Live
                } else { TrieState::Nascent };
                self.shards.entry(shard).or_default().insert(trie);
                self.l1h_recencies.entry(block).or_default().insert(recency);
            }
            (1, None) => {
                if let Some(recencies) = self.l1h_recencies.remove(&block) {
                    for recency in recencies {
                        if let Some(state) = self.shards.get_mut(&Shard::root(1, Some(recency))) { state.promote(block); }
                    }
                }
                let state = self.shard_with_floor(Shard::root(1, None), block);
                state.supersede(block, Some(file_size_target), as_of);
                trie.state = TrieState::Live;
                state.insert(trie);
                self.drop_covered_l0(block);
            }
            (2, Some(recency)) => {
                let state = self.shard_with_floor(shard, block);
                state.supersede(block, Some(file_size_target), as_of);
                trie.state = TrieState::Live;
                state.insert(trie);
                self.shard_with_floor(Shard::root(1, Some(recency)), block).supersede(block, None, as_of);
            }
            _ => {
                let mut parent_part = shard.part.clone();
                if parent_part.pop().is_none() { return Err(invalid("Tiered trie has no IID partition")); }
                trie.state = TrieState::Nascent;
                self.shards.entry(shard.clone()).or_default().insert(trie);
                let siblings: Vec<_> = (0..4).map(|bucket| {
                    let mut part = parent_part.clone(); part.push(bucket);
                    Shard { level: shard.level, recency: shard.recency, part }
                }).collect();
                if siblings.iter().all(|key| self.shards.get(key).is_some_and(|value| value.max_block_index >= block)) {
                    for sibling in siblings {
                        if let Some(value) = self.shards.get_mut(&sibling) { value.promote(block); }
                    }
                    self.shard_with_floor(Shard { level: shard.level - 1, recency: shard.recency, part: parent_part }, block)
                        .supersede(block, None, as_of);
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct TrieCatalog { tables: BTreeMap<TableRef, TableTries>, file_size_target: i64 }

impl Default for TrieCatalog { fn default() -> Self { Self { tables: BTreeMap::new(), file_size_target: 100 * 1024 * 1024 } } }

impl TrieCatalog {
    pub fn new(file_size_target: i64) -> Result<Self> {
        if file_size_target <= 0 { return Err(invalid("Trie file size target must be positive")); }
        Ok(Self { tables: BTreeMap::new(), file_size_target })
    }
    pub fn tables(&self) -> impl Iterator<Item = &TableRef> { self.tables.keys() }
    pub fn file_size_target(&self) -> i64 { self.file_size_target }
    pub fn max_block_index(&self, table: &TableRef, shard: &Shard) -> BlockIndex {
        self.tables.get(table).and_then(|table| table.shards.get(shard)).map_or(BlockIndex(-1), |state| state.max_block_index)
    }
    pub fn l0_max_block_index(&self, table: &TableRef) -> BlockIndex { self.max_block_index(table, &Shard::root(0, None)) }
    pub fn all_tries(&self, table: &TableRef) -> Vec<&CatalogTrie> {
        let mut result: Vec<_> = self.tables.get(table).into_iter().flat_map(|table| table.shards.values()).flat_map(|shard| shard.tries.values()).collect();
        result.sort_by(|left, right| {
            (left.key.level(), left.key.block_index(), left.key.recency().unwrap_or(NaiveDate::MAX), left.key.part())
                .cmp(&(right.key.level(), right.key.block_index(), right.key.recency().unwrap_or(NaiveDate::MAX), right.key.part()))
        });
        result
    }
    pub fn current_tries(&self, table: &TableRef) -> Vec<&CatalogTrie> {
        self.all_tries(table).into_iter().filter(|trie| trie.state == TrieState::Live).collect()
    }
    pub fn live_and_nascent_tries(&self, table: &TableRef) -> Vec<&CatalogTrie> {
        self.all_tries(table).into_iter().filter(|trie| !matches!(trie.state, TrieState::Garbage { .. })).collect()
    }
    pub fn garbage_tries(&self, table: &TableRef, as_of: Timestamp) -> Vec<&CatalogTrie> {
        self.all_tries(table).into_iter().filter(|trie| trie.key.level() != 0 && matches!(trie.state, TrieState::Garbage { as_of: time } if time <= as_of)).collect()
    }
    pub fn with_tries(&self, table: TableRef, tries: impl IntoIterator<Item = CatalogTrie>, as_of: Timestamp) -> Result<Self> {
        let mut next = self.clone();
        let state = next.tables.entry(table).or_default();
        for trie in tries { state.apply(trie, self.file_size_target, as_of)?; }
        Ok(next)
    }
    pub fn without_tries(&self, table: &TableRef, keys: &BTreeSet<TrieKey>) -> Self {
        let mut next = self.clone();
        if let Some(table) = next.tables.get_mut(table) {
            for key in keys {
                if let Some(shard) = table.shards.get_mut(&Shard::of(key)) {
                    if shard.tries.get(&key.block_index()).is_some_and(|trie| matches!(trie.state, TrieState::Garbage { .. })) {
                        shard.tries.remove(&key.block_index());
                    }
                }
            }
        }
        next
    }
    pub fn reset_to_l0(&self, replacements: BTreeMap<TableRef, Vec<CatalogTrie>>) -> Result<Self> {
        let mut next = self.clone();
        for (table, tries) in replacements {
            let mut state = TableTries::default();
            let mut shard = ShardValue::default();
            for mut trie in tries {
                if Shard::of(&trie.key) != Shard::root(0, None) { return Err(invalid("Reset accepts only unpartitioned L0 tries")); }
                trie.state = TrieState::Live;
                shard.insert(trie);
            }
            state.shards.insert(Shard::root(0, None), shard);
            next.tables.insert(table, state);
        }
        Ok(next)
    }
    pub fn partitions(&self, table: &TableRef) -> Result<Vec<block_pb::Partition>> {
        self.tables.get(table).into_iter().flat_map(|table| table.shards.iter()).map(|(shard, state)| {
            Ok(block_pb::Partition {
                level: Some(i32::try_from(shard.level).map_err(invalid)?), part: Some(shard.part.clone()),
                recency: shard.recency.map(date_micros).transpose()?, max_block_index: Some(state.max_block_index.0),
                tries: [0, 1, 2].into_iter().flat_map(|state_order| state.tries.values().rev().filter(move |trie| {
                    state_order == match trie.state { TrieState::Live => 0, TrieState::Nascent => 1, TrieState::Garbage { .. } => 2 }
                })).map(|trie| {
                    let mut wire = trie.to_proto(table);
                    wire.table_name = None;
                    wire
                }).collect(),
            })
        }).collect()
    }

    pub fn load(blocks: &BTreeMap<TableRef, TableBlock>, file_size_target: i64, as_of: Timestamp) -> Result<Self> {
        let partitions = blocks.iter().map(|(table, block)| Ok((table.clone(), table_partitions(block)?))).collect::<Result<BTreeMap<_, _>>>()?;
        let modern = partitions.values().filter_map(|partitions| partitions.iter().find_map(|partition| partition.tries.first()))
            .all(|trie| trie.trie_state.is_some());
        let mut catalog = Self::new(file_size_target)?;
        for (table, partitions) in partitions {
            if !modern {
                for partition in partitions {
                    catalog = catalog.with_tries(table.clone(), partition.tries.iter().map(CatalogTrie::from_proto).collect::<Result<Vec<_>>>()?, as_of)?;
                }
                continue;
            }
            let reconstruct_watermarks = partitions.first().is_some_and(|partition| partition.max_block_index.is_none());
            let mut state = TableTries::default();
            for partition in partitions {
                let shard = decode_shard(&partition)?;
                let mut value = ShardValue::default();
                for wire in partition.tries {
                    let trie = CatalogTrie::from_proto(&wire)?;
                    if Shard::of(&trie.key) != shard { return Err(invalid("Recorded trie differs from its partition")); }
                    if shard.level == 0 && matches!(trie.state, TrieState::Garbage { .. }) { continue; }
                    value.insert(trie);
                }
                value.max_block_index = partition.max_block_index.map(BlockIndex).unwrap_or(value.max_block_index.max(BlockIndex(0)));
                state.shards.insert(shard, value);
            }
            if reconstruct_watermarks {
                let floors: Vec<_> = state.shards.iter().map(|(shard, value)| (shard.clone(), value.max_block_index)).collect();
                for (mut shard, block) in floors {
                    while shard.level > 1 {
                        shard.level -= 1;
                        shard.part.pop();
                        let ancestor = state.shards.entry(shard.clone()).or_default();
                        ancestor.max_block_index = ancestor.max_block_index.max(block);
                    }
                }
            }
            if let Some(block) = state.shards.get(&Shard::root(1, None)).into_iter().flat_map(|state| state.tries.iter().rev())
                .find_map(|(block, trie)| (trie.state == TrieState::Live).then_some(*block)) {
                state.drop_covered_l0(block);
            }
            catalog.tables.insert(table, state);
        }
        Ok(catalog)
    }
}

fn table_partitions(block: &TableBlock) -> Result<Vec<block_pb::Partition>> {
    if !block.partitions().is_empty() { return Ok(block.partitions().to_vec()); }
    let mut shards: BTreeMap<Shard, Vec<log_pb::TrieDetails>> = BTreeMap::new();
    for wire in &block.as_proto().tries {
        let trie = CatalogTrie::from_proto(wire)?;
        shards.entry(Shard::of(&trie.key)).or_default().push(wire.clone());
    }
    shards.into_iter().map(|(shard, tries)| Ok(block_pb::Partition {
        level: Some(i32::try_from(shard.level).map_err(invalid)?), part: Some(shard.part), recency: shard.recency.map(date_micros).transpose()?,
        tries, max_block_index: None,
    })).collect()
}

fn decode_shard(partition: &block_pb::Partition) -> Result<Shard> {
    Ok(Shard { level: i64::from(partition.level.unwrap_or_default()), part: partition.part.clone().unwrap_or_default(),
        recency: partition.recency.map(|micros| DateTime::<Utc>::from_timestamp_micros(micros).map(|time| time.date_naive()).ok_or_else(|| invalid("Trie recency exceeds supported date range"))).transpose()? })
}
fn date_micros(date: NaiveDate) -> Result<i64> {
    date.and_hms_opt(0, 0, 0).map(|date| date.and_utc().timestamp_micros()).ok_or_else(|| invalid("Invalid trie recency date"))
}
fn invalid(message: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("rtdb.index/invalid-trie-catalog", message.to_string()) }
