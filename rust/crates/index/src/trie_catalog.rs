use std::{collections::{BTreeMap, BTreeSet}, ops::Bound::{Excluded, Included}, sync::Arc};

use rtdb_types::CivilDate;
use rtdb_format::{block::proto as block_pb, catalog::TableBlock, keys::{BRANCH_FACTOR, PartGroup, TrieKey}, log::proto as log_pb};
use rtdb_types::{Anomaly, BlockIndex, Result, TableRef, Timestamp};

#[path = "replayed_reset_epochs.rs"]
mod reset_epochs;

#[path = "trie_membership.rs"]
mod membership;

pub type TrieState = membership::TrieState<Timestamp>;

#[derive(Clone, Debug, PartialEq)]
pub struct CatalogTrie {
    pub key: TrieKey,
    pub data_file_size: i64,
    pub metadata: Option<log_pb::TrieMetadata>,
    pub state: TrieState,
    /// Files derived from this trie's data file, at most one per (kind, definition id, definition version).
    pub artefacts: Vec<log_pb::ArtefactRecord>,
    /// The provider's version for each file at publication, retained when the trie becomes garbage.
    pub published_files: Option<log_pb::TrieFileVersions>,
}

impl CatalogTrie {
    pub fn notification(key: TrieKey, data_file_size: i64, mut metadata: Option<log_pb::TrieMetadata>) -> Result<Self> {
        if data_file_size < 0 { return Err(invalid("Trie file size is negative")); }
        if let Some(metadata) = &mut metadata { metadata.iid_bloom = None; }
        Ok(Self { key, data_file_size, metadata, state: TrieState::Nascent, artefacts: Vec::new(), published_files: None })
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
        Ok(Self { key, data_file_size, metadata: value.trie_metadata.clone(), state, artefacts: value.artefacts.clone(), published_files: value.published_files.clone() })
    }

    pub fn to_proto(&self, table: &TableRef) -> log_pb::TrieDetails {
        let (state, garbage_as_of) = match self.state {
            TrieState::Nascent => (0, None), TrieState::Live => (1, None),
            TrieState::Garbage { as_of } => (2, Some(as_of.0)),
        };
        log_pb::TrieDetails {
            table_name: Some(table.schema_and_table()), trie_key: Some(self.key.to_string()),
            data_file_size: Some(self.data_file_size), trie_metadata: self.metadata.clone(),
            trie_state: Some(state), garbage_as_of, artefacts: self.artefacts.clone(), published_files: self.published_files.clone(),
        }
    }

    pub fn artefact(&self, kind: &str, definition_id: &str, definition_version: i64) -> Option<&log_pb::ArtefactRecord> {
        self.artefacts.iter().find(|record| same_artefact(record, kind, definition_id, definition_version))
    }

    fn garbage(&mut self, as_of: Timestamp) { self.state = TrieState::Garbage { as_of }; self.metadata = None; }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Shard { pub level: i64, pub recency: Option<CivilDate>, pub part: Vec<u8> }

impl Shard {
    pub fn of(key: &TrieKey) -> Self { Self { level: key.level(), recency: key.recency(), part: key.part().to_vec() } }
    fn root(level: i64, recency: Option<CivilDate>) -> Self { Self { level, recency, part: Vec::new() } }
}

#[derive(Clone, Debug)]
struct ShardValue { max_block_index: BlockIndex, tries: imbl::OrdMap<BlockIndex, CatalogTrie> }
impl Default for ShardValue {
    fn default() -> Self { Self { max_block_index: BlockIndex(-1), tries: imbl::OrdMap::new() } }
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
        let covered: Vec<_> = self.tries.range(..=block).filter_map(|(block, trie)| {
            (trie.state == TrieState::Live && partial_limit.is_none_or(|limit| trie.data_file_size < limit)).then_some(*block)
        }).collect();
        for block in covered { self.tries[&block].garbage(as_of); }
    }
    fn retire(&mut self, as_of: Timestamp) {
        let serving: Vec<_> = self.tries.iter().filter_map(|(block, trie)| {
            matches!(trie.state, TrieState::Live | TrieState::Nascent).then_some(*block)
        }).collect();
        for block in serving { self.tries[&block].garbage(as_of); }
    }
}

#[derive(Clone, Debug, Default)]
struct TableTries {
    shards: BTreeMap<Shard, ShardValue>,
    l1h_recencies: BTreeMap<BlockIndex, BTreeSet<CivilDate>>,
    search: std::sync::Arc<crate::search_manifest::SearchManifest>,
    /// When the table's relation was retired: every trie it holds or later receives is garbage from then on.
    retired: Option<Timestamp>,
    directory: std::sync::Arc<crate::entity_directory::DirectoryManifest>,
}

impl TableTries {
    fn shard_with_floor(&mut self, shard: Shard, block: BlockIndex) -> &mut ShardValue {
        self.shards.entry(shard).or_insert_with(|| ShardValue { max_block_index: block, ..ShardValue::default() })
    }
    fn cover_l0(&mut self, block: BlockIndex, as_of: Timestamp) {
        // Compaction stops primary scans using these tries. Their immutable
        // descriptors still certify the original input to derived indexes.
        self.shard_with_floor(Shard::root(0, None), block).supersede(block, None, as_of);
    }
    /// Supersedes every trie the table holds with no successor.
    fn retire(&mut self, as_of: Timestamp) {
        let at = *self.retired.get_or_insert(as_of);
        for shard in self.shards.values_mut() { shard.retire(at); }
        self.l1h_recencies.clear();
        std::sync::Arc::make_mut(&mut self.search).retire_all(at);
    }
    fn apply(&mut self, mut trie: CatalogTrie, file_size_target: i64, as_of: Timestamp, expiring_metadata: bool) -> Result<()> {
        let shard = Shard::of(&trie.key);
        let block = trie.key.block_index();
        if self.shards.get(&shard).is_some_and(|state| state.max_block_index >= block) { return Ok(()); }
        if let Some(metadata) = &mut trie.metadata { metadata.iid_bloom = None; }
        if let Some(retired) = self.retired {
            trie.garbage(retired);
            self.shards.entry(shard).or_default().insert(trie);
            return Ok(());
        }
        if expiring_metadata && shard == Shard::root(1, None) {
            for covered in self.shards.values_mut() { covered.supersede(block, None, as_of); }
            trie.state = TrieState::Live;
            self.shards.entry(shard).or_default().insert(trie);
            return Ok(());
        }
        match (shard.level, shard.recency) {
            (0, _) => { trie.state = TrieState::Live; self.shards.entry(shard).or_default().insert(trie); }
            (1, Some(recency)) => {
                let current = Shard { level: 1, recency: None, part: shard.part.clone() };
                trie.state = if self.shards.get(&current).is_some_and(|state| state.tries.range(block..).any(|(_, trie)| trie.state == TrieState::Live)) {
                    TrieState::Live
                } else { TrieState::Nascent };
                if trie.state == TrieState::Nascent && shard.part.is_empty() {
                    self.l1h_recencies.entry(block).or_default().insert(recency);
                }
                self.shards.entry(shard).or_default().insert(trie);
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
                self.cover_l0(block, as_of);
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
                let siblings: [Shard; BRANCH_FACTOR as usize] = std::array::from_fn(|bucket| {
                    let mut part = parent_part.clone();
                    part.push(bucket as u8);
                    Shard { level: shard.level, recency: shard.recency, part }
                });
                let members = siblings.each_ref().map(|key| {
                    let member = self.shards.get(key)?.tries.get(&block)?;
                    let usable = matches!(member.state, TrieState::Nascent | TrieState::Live);
                    Some((member.key.block_index().0, usable))
                });
                if crate::tiered_publication::can_publish(members, block.0) {
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
pub struct TrieCatalog { tables: imbl::OrdMap<TableRef, Arc<TableTries>>, file_size_target: i64, naming_epoch: u64 }

impl Default for TrieCatalog { fn default() -> Self { Self { tables: imbl::OrdMap::new(), file_size_target: 100 * 1024 * 1024, naming_epoch: 0 } } }

impl TrieCatalog {
    pub fn new(file_size_target: i64) -> Result<Self> {
        if file_size_target <= 0 { return Err(invalid("Trie file size target must be positive")); }
        Ok(Self { tables: imbl::OrdMap::new(), file_size_target, naming_epoch: 0 })
    }
    fn table_or_default(&mut self, table: TableRef) -> &mut TableTries {
        Arc::make_mut(self.tables.entry(table).or_default())
    }
    fn table_mut(&mut self, table: &TableRef) -> Option<&mut TableTries> {
        self.tables.get_mut(table).map(Arc::make_mut)
    }
    pub fn tables(&self) -> impl Iterator<Item = &TableRef> { self.tables.keys() }
    pub fn file_size_target(&self) -> i64 { self.file_size_target }
    /// The epoch compaction outputs planned from this catalog are named in.
    pub fn naming_epoch(&self) -> u64 { self.naming_epoch }
    pub fn with_naming_epoch(self, naming_epoch: u64) -> Self { Self { naming_epoch, ..self } }
    pub fn max_block_index(&self, table: &TableRef, shard: &Shard) -> BlockIndex {
        self.tables.get(table).and_then(|table| table.shards.get(shard)).map_or(BlockIndex(-1), |state| state.max_block_index)
    }
    pub fn l0_max_block_index(&self, table: &TableRef) -> BlockIndex { self.max_block_index(table, &Shard::root(0, None)) }
    /// The adopted source tries in order, including ones covered by primary compaction.
    pub fn l0_source(
        &self,
        table: &TableRef,
        after: BlockIndex,
        through: BlockIndex,
    ) -> impl Iterator<Item = &CatalogTrie> {
        self.tables.get(table)
            .and_then(|table| table.shards.get(&Shard::root(0, None)))
            .filter(|_| after < through)
            .into_iter()
            .flat_map(move |source| source.tries.range((Excluded(after), Included(through))).map(|(_, trie)| trie))
    }
    pub fn iter_tries(&self, table: &TableRef) -> impl Iterator<Item = &CatalogTrie> {
        self.tables.get(table).into_iter().flat_map(|table| table.shards.values()).flat_map(|shard| shard.tries.values())
    }
    pub fn all_tries(&self, table: &TableRef) -> Vec<&CatalogTrie> {
        let mut result: Vec<_> = self.tables.get(table).into_iter().flat_map(|table| table.shards.values()).flat_map(|shard| shard.tries.values()).collect();
        result.sort_by(|left, right| left.key.scan_precedence().cmp(&right.key.scan_precedence()));
        result
    }
    pub fn current_tries(&self, table: &TableRef) -> Vec<&CatalogTrie> {
        let inventory: Vec<_> = self.all_tries(table).into_iter().map(|trie| (trie, trie.state)).collect();
        membership::select_live(&inventory)
    }
    /// The entry the catalog holds for `key`, live, nascent or garbage.
    pub fn trie(&self, table: &TableRef, key: &TrieKey) -> Option<&CatalogTrie> {
        self.tables.get(table)?.shards.get(&Shard::of(key))?.tries.get(&key.block_index()).filter(|trie| &trie.key == key)
    }
    /// The entries the catalog holds for a partition group's members, in bucket order; a member it does not hold is left out.
    pub fn group_members(&self, table: &TableRef, group: &PartGroup) -> Vec<&CatalogTrie> {
        group.members().iter().filter_map(|key| self.trie(table, key)).collect()
    }
    pub(crate) fn has_current_tries(&self, table: &TableRef) -> bool {
        self.iter_tries(table).any(|trie| trie.state == TrieState::Live)
    }
    pub fn live_and_nascent_tries(&self, table: &TableRef) -> Vec<&CatalogTrie> {
        self.all_tries(table).into_iter().filter(|trie| !matches!(trie.state, TrieState::Garbage { .. })).collect()
    }
    pub fn garbage_tries(&self, table: &TableRef, as_of: Timestamp) -> Vec<&CatalogTrie> {
        self.all_tries(table).into_iter().filter(|trie| trie.key.level() != 0 && matches!(trie.state, TrieState::Garbage { as_of: time } if time <= as_of)).collect()
    }
    /// When the table's relation was retired, if it was.
    pub fn retired_as_of(&self, table: &TableRef) -> Option<Timestamp> {
        self.tables.get(table).and_then(|state| state.retired)
    }

    /// Supersedes every trie of each retired table with no successor, and makes each trie the table later receives garbage on arrival (trie-cat.allium: RetireTable).
    /// A table the catalog does not hold yet is recorded too, so its first flushed tries arrive as garbage.
    /// A table already retired keeps its first retirement time.
    pub fn with_retired_tables(&self, tables: impl IntoIterator<Item = TableRef>, as_of: Timestamp) -> Self {
        let mut next = self.clone();
        for table in tables { next.table_or_default(table).retire(as_of); }
        next
    }

    pub fn with_retirement(&self, table: &TableRef, block: TableBlock) -> TableBlock {
        block.with_retired_as_of(self.retired_as_of(table).map(|at| at.0))
    }

    pub fn search_runs(&self, table: &TableRef) -> impl Iterator<Item = &log_pb::SearchRun> {
        self.tables.get(table).into_iter().flat_map(|state| state.search.active())
    }
    pub fn active_search_run(&self, table: &TableRef, digest: &[u8; 32]) -> Option<&log_pb::SearchRun> {
        self.tables.get(table)?.search.active_run(digest)
    }
    pub fn retired_search_runs(&self, table: &TableRef) -> impl Iterator<Item = &log_pb::RetiredSearchRun> {
        self.tables.get(table).into_iter().flat_map(|state| state.search.retired())
    }

    pub(crate) fn search_tombstone_settled_at(&self, table: &TableRef, id: &[u8; 32]) -> Option<i64> {
        self.tables.get(table)?.search.settled_at(id)
    }

    pub fn with_search_manifest(&self, table: &TableRef, block: TableBlock) -> TableBlock {
        block.with_search_runs(self.search_runs(table).cloned().collect(), self.retired_search_runs(table).cloned().collect())
    }

    pub fn entity_directory(&self, table: &TableRef) -> Option<&log_pb::EntityDirectoryRoot> {
        self.tables.get(table)?.directory.active()
    }

    pub fn retired_entity_directories(&self, table: &TableRef) -> impl Iterator<Item = &log_pb::EntityDirectoryRoot> {
        self.tables.get(table).into_iter().flat_map(|state| state.directory.retired())
    }

    pub fn with_entity_directory(&self, table: &TableRef, block: TableBlock) -> TableBlock {
        block.with_entity_directory(self.entity_directory(table).cloned(), self.retired_entity_directories(table).cloned().collect())
    }

    pub fn accepts_entity_directory(&self, table: &TableRef, key: &TrieKey, record: &log_pb::ArtefactRecord) -> bool {
        self.trie(table, key).is_some() && self.tables.get(table).is_some_and(|state| {
            log_pb::EntityDirectoryChange::from_artefact(record).is_ok_and(|change| state.directory.accepts(key, &change))
        })
    }

    pub fn term_directory(&self, table: &TableRef, id: &str, version: i64) -> Option<&log_pb::TermDirectoryRoot> {
        self.tables.get(table)?.search.term_directory(id, version)
    }

    pub fn term_directories(&self, table: &TableRef) -> impl Iterator<Item = &log_pb::TermDirectoryRoot> {
        self.tables.get(table).into_iter().flat_map(|state| state.search.term_directories())
    }

    pub fn retired_term_directories(&self, table: &TableRef) -> impl Iterator<Item = &log_pb::RetiredTermDirectory> {
        self.tables.get(table).into_iter().flat_map(|state| state.search.retired_term_directories())
    }

    pub fn with_term_directories(&self, table: &TableRef, block: TableBlock) -> TableBlock {
        block.with_term_directories(self.term_directories(table).cloned().collect())
            .with_retired_term_directories(self.retired_term_directories(table).cloned().collect())
    }

    fn load_term_directories(state: &mut TableTries, block: &TableBlock, as_of: Timestamp) {
        let wire = block.as_proto();
        std::sync::Arc::make_mut(&mut state.search).load_term_directories(&wire.term_directories, &wire.retired_term_directories, as_of);
    }

    pub fn accepts_search_run(&self, table: &TableRef, record: &log_pb::ArtefactRecord) -> bool {
        let Ok(change) = log_pb::SearchRunChange::from_artefact(record) else { return false; };
        let Some(run) = &change.run else { return false; };
        change.prune_settled_at.is_none() && self.accepts_search_source(table, &change)
            && (change.abandon() || run.blocks().all(|block| TrieKey::l0(block).ok().is_some_and(|key| self.trie(table, &key).is_some())))
    }

    pub(crate) fn accepts_search_source(&self, table: &TableRef, change: &log_pb::SearchRunChange) -> bool {
        let Some(run) = change.run.as_ref() else { return false; };
        let Some(receipt) = &run.source_receipt else { return true; };
        if receipt.table() != table.schema_and_table() { return false; }
        if change.retire_only() || receipt.directory_root.is_none()
            || (change.replaces.is_empty() && self.search_runs(table).any(|active| active == run))
        {
            return true;
        }
        self.entity_directory(table).is_some_and(|directory| directory.root == receipt.directory_root
            && directory.erase_frontier_digest == receipt.erase_frontier_digest)
            || (!change.replaces.is_empty() && change.replaces.iter().all(|digest| self.search_runs(table).any(|run|
                digest.as_slice() == run.digest() && run.source_receipt.as_ref().is_some_and(|prior|
                    receipt.suffix_through == prior.suffix_through && receipt.extends(prior)))))
    }

    pub fn with_tries(&self, table: TableRef, tries: impl IntoIterator<Item = CatalogTrie>, as_of: Timestamp) -> Result<Self> {
        let mut next = self.clone();
        let expiring_metadata = table.stores_expiring_transaction_metadata();
        let state = next.table_or_default(table);
        for mut trie in tries {
            let (mut directory, mut search) = (Vec::new(), Vec::new());
            let mut held = Vec::with_capacity(trie.artefacts.len());
            for record in std::mem::take(&mut trie.artefacts) {
                if record.kind() == rtdb_format::entity_directory::RECORD_KIND {
                    directory.push(log_pb::EntityDirectoryChange::from_artefact(&record)?);
                } else if record.kind() == rtdb_format::search_manifest::RECORD_KIND {
                    search.push(log_pb::SearchRunChange::from_artefact(&record)?);
                } else {
                    held.push(record);
                }
            }
            trie.artefacts = held;
            for change in directory {
                std::sync::Arc::make_mut(&mut state.directory).install(&trie.key, change, as_of);
            }
            for change in search {
                if trie.key.level() == 0 && !change.retire_only() && change.replaces.is_empty()
                    && change.run.as_ref().is_some_and(|run| run.blocks == [trie.key.block_index().0]) {
                    std::sync::Arc::make_mut(&mut state.search).apply_uploaded(change, as_of);
                }
            }
            state.apply(trie, self.file_size_target, as_of, expiring_metadata)?;
        }
        Ok(next)
    }
    pub fn without_tries(&self, table: &TableRef, keys: &BTreeSet<TrieKey>) -> Self {
        let mut next = self.clone();
        let expiring_metadata = table.stores_expiring_transaction_metadata();
        if let Some(table) = next.table_mut(table) {
            for key in keys {
                if key.level() == 0 && !expiring_metadata { continue; }
                if let Some(shard) = table.shards.get_mut(&Shard::of(key)) {
                    if shard.tries.get(&key.block_index()).is_some_and(|trie| trie.key == *key && matches!(trie.state, TrieState::Garbage { .. })) {
                        shard.tries.remove(&key.block_index());
                    }
                }
            }
        }
        next
    }
    /// Keeps the first record for each definition version, except when a live trie replaces its obsolete group's record with its own.
    pub fn with_artefacts(&self, table: &TableRef, entries: impl IntoIterator<Item = (TrieKey, log_pb::ArtefactRecord)>) -> Self {
        self.with_artefacts_at(table, entries, Timestamp(0))
    }

    pub fn with_artefacts_at(&self, table: &TableRef, entries: impl IntoIterator<Item = (TrieKey, log_pb::ArtefactRecord)>, as_of: Timestamp) -> Self {
        let mut next = self.clone();
        if let Some(state) = next.table_mut(table) {
            for (key, record) in entries {
                if record.kind() == rtdb_format::entity_directory::RECORD_KIND {
                    let anchored = state.shards.get(&Shard::of(&key)).and_then(|shard| shard.tries.get(&key.block_index()))
                        .is_some_and(|trie| trie.key == key);
                    if anchored && let Ok(change) = log_pb::EntityDirectoryChange::from_artefact(&record) {
                        std::sync::Arc::make_mut(&mut state.directory).install(&key, change, as_of);
                    }
                    continue;
                }
                if record.kind() == rtdb_format::search_manifest::RECORD_KIND {
                    if key.level() == 0 && self.accepts_search_run(table, &record)
                        && let Ok(change) = log_pb::SearchRunChange::from_artefact(&record)
                        && change.run.as_ref().is_some_and(|run| run.blocks.first() == Some(&key.block_index().0)) {
                        std::sync::Arc::make_mut(&mut state.search).apply_uploaded(change, as_of);
                    }
                    continue;
                }
                let Ok(owner) = rtdb_format::keys::ArtefactOwner::of(&key, &record) else { continue; };
                let Some(trie) = state.shards.get_mut(&Shard::of(&key)).and_then(|shard| shard.tries.get_mut(&key.block_index())).filter(|trie| trie.key == key) else { continue };
                let (kind, id, version) = (record.kind(), record.definition_id(), record.definition_version());
                let held = trie.artefacts.iter_mut().find(|held| same_artefact(held, kind, id, version));
                match held {
                    None => trie.artefacts.push(record),
                    Some(held) if trie.state == TrieState::Live && held.scope() == log_pb::ArtefactScope::Trie
                        && matches!(owner, rtdb_format::keys::ArtefactOwner::Pack(_)) => *held = record,
                    Some(held) if trie.state == TrieState::Live && held.scope() == log_pb::ArtefactScope::Pack
                        && record.scope() == log_pb::ArtefactScope::Trie
                        && rtdb_format::keys::ArtefactOwner::of(&key, held).is_ok_and(|owner| owner.tries().iter().any(|member| {
                            self.trie(table, member).is_none_or(|trie| matches!(trie.state, TrieState::Garbage { .. }))
                        })) => *held = record,
                    Some(held) if held.scope() == log_pb::ArtefactScope::PartGroup
                        && record.scope() == log_pb::ArtefactScope::Trie
                        && trie.state == TrieState::Live
                        && key.part_group().is_some_and(|group| group.members().iter().any(|member| {
                            self.trie(table, member).is_none_or(|trie| matches!(trie.state, TrieState::Garbage { .. }))
                        })) => *held = record,
                    Some(_) => {}
                }
            }
        }
        next
    }
    /// A record matches an entry on kind, definition id and definition version alone.
    pub fn without_artefacts(&self, table: &TableRef, entries: impl IntoIterator<Item = (TrieKey, log_pb::ArtefactRecord)>) -> Self {
        self.without_artefacts_at(table, entries, None)
    }

    pub(crate) fn without_artefacts_at(&self, table: &TableRef, entries: impl IntoIterator<Item = (TrieKey, log_pb::ArtefactRecord)>,
        position: Option<rtdb_types::ReplicaPosition>) -> Self {
        let mut next = self.clone();
        if let Some(state) = next.table_mut(table) {
            for (key, record) in entries {
                if record.kind() == rtdb_format::entity_directory::RECORD_KIND {
                    if key.level() == 0 && let Ok(change) = log_pb::EntityDirectoryChange::from_artefact(&record)
                        && let Some(root) = change.root.filter(|root| root.through_block() == key.block_index().0) {
                        std::sync::Arc::make_mut(&mut state.directory).forget(&root);
                    }
                    continue;
                }
                if record.kind() == rtdb_format::term_directory::RELEASE_KIND {
                    if let Ok(released) = log_pb::RetiredTermDirectory::from_release(&record) {
                        std::sync::Arc::make_mut(&mut state.search).forget_term_directory(&released);
                    }
                    continue;
                }
                if record.kind() == rtdb_format::search_manifest::RECORD_KIND {
                    if key.level() == 0 && let Ok(change) = log_pb::SearchRunChange::from_artefact(&record) && change.retire_only()
                        && let Some(run) = change.run && run.blocks.first() == Some(&key.block_index().0) {
                        std::sync::Arc::make_mut(&mut state.search).forget(&run, change.prune_settled_at, position);
                    }
                    continue;
                }
                let Some(trie) = state.shards.get_mut(&Shard::of(&key)).and_then(|shard| shard.tries.get_mut(&key.block_index())).filter(|trie| trie.key == key) else { continue };
                trie.artefacts.retain(|held| (held.kind(), held.definition_id(), held.definition_version()) != (record.kind(), record.definition_id(), record.definition_version()));
            }
        }
        next
    }
    /// Selects new physical names for the recovered compacted pairs and keeps the original L0 identities needed by ordered replay.
    pub fn reset_from_primary(&self, mut primary: Self, block: BlockIndex, as_of: Timestamp) -> Result<(Self, u64)> {
        let files_epoch = reset_epochs::recovery_file_epoch(block.0).ok_or_else(|| invalid("Recovery file epoch exhausted"))?;
        primary.naming_epoch = reset_epochs::next_compaction_epoch(self.naming_epoch).ok_or_else(|| invalid("Naming epoch exhausted"))?;
        let table_names: Vec<_> = primary.tables.keys().cloned().collect();
        for table in table_names {
            let table = Arc::make_mut(&mut primary.tables[&table]);
            for (shard, value) in &mut table.shards {
                let blocks: Vec<_> = value.tries.keys().copied().collect();
                for block in blocks {
                    let trie = &mut value.tries[&block];
                    if matches!(trie.state, TrieState::Garbage { .. }) {
                        return Err(invalid("Primary recovery contains a garbage trie"));
                    }
                    if shard.level > 0 {
                        trie.key = trie.key.clone().with_epoch(files_epoch);
                        trie.published_files = None;
                    }
                    trie.artefacts.clear();
                }
            }
        }
        for (table, old) in &self.tables {
            let table = primary.table_or_default(table.clone());
            table.search = Arc::clone(&old.search);
            if let Some(retired) = old.retired { table.retire(retired); }
            table.directory = Arc::clone(&old.directory);
            for (shard, old) in old.shards.iter().filter(|(shard, _)| shard.level == 0) {
                let new = table.shards.entry(shard.clone()).or_default();
                new.max_block_index = new.max_block_index.max(old.max_block_index);
                for (block, previous) in &old.tries {
                    new.tries.entry(*block).or_insert_with(|| {
                        let mut source = previous.clone();
                        source.garbage(as_of);
                        source
                    });
                }
            }
        }
        Ok((primary, files_epoch))
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
            let wire = blocks[&table].as_proto();
            if wire.search_runs.iter().chain(wire.retired_search_runs.iter().filter_map(|retired| retired.run.as_ref()))
                .filter_map(|run| run.source_receipt.as_ref()).any(|receipt| receipt.table() != table.schema_and_table())
            {
                return Err(invalid("Search history receipt belongs to another table"));
            }
            state.directory = std::sync::Arc::new(crate::entity_directory::DirectoryManifest::load(
                wire.entity_directory.clone(), wire.retired_entity_directories.clone(), as_of)?);
            state.search = std::sync::Arc::new(crate::search_manifest::SearchManifest::load(&wire.search_runs, &wire.retired_search_runs)?);
            state.retired = wire.retired_as_of_micros.map(Timestamp);
            Self::load_term_directories(&mut state, &blocks[&table], as_of);
            for partition in partitions {
                let shard = decode_shard(&partition)?;
                let mut value = ShardValue::default();
                for wire in partition.tries {
                    let mut trie = CatalogTrie::from_proto(&wire)?;
                    trie.artefacts.retain(|record| rtdb_format::term_directory::sealed_change(record).is_none());
                    if Shard::of(&trie.key) != shard { return Err(invalid("Recorded trie differs from its partition")); }
                    value.insert(trie);
                }
                value.max_block_index = partition.max_block_index.map(BlockIndex).unwrap_or(value.max_block_index.max(BlockIndex(0)));
                state.shards.insert(shard, value);
            }
            for (shard, value) in &state.shards {
                let (1, Some(recency), []) = (shard.level, shard.recency, shard.part.as_slice()) else { continue; };
                for (block, trie) in &value.tries {
                    if trie.state == TrieState::Nascent {
                        state.l1h_recencies.entry(*block).or_default().insert(recency);
                    }
                }
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
                state.cover_l0(block, as_of);
            }
            catalog.tables.insert(table, Arc::new(state));
        }
        Ok(catalog)
    }
}

fn same_artefact(record: &log_pb::ArtefactRecord, kind: &str, definition_id: &str, definition_version: i64) -> bool {
    record.kind() == kind && record.definition_id() == definition_id && record.definition_version() == definition_version
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
        recency: partition.recency.map(|micros| CivilDate::from_epoch_day(micros.div_euclid(86_400_000_000))).transpose()? })
}
fn date_micros(date: CivilDate) -> Result<i64> {
    date.epoch_day().checked_mul(86_400_000_000).ok_or_else(|| invalid("Trie recency exceeds microsecond range"))
}
fn invalid(message: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("rtdb.index/invalid-trie-catalog", message.to_string()) }
