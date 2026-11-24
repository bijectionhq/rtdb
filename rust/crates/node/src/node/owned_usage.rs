use std::{collections::{BTreeMap, BTreeSet, btree_map::Entry}, sync::Arc, time::Duration};

use rtdb_index::TrieState;
use rtdb_types::{Anomaly, OwnedRelation, Result};

use super::OwnedReadSession;

const MAX_RELATIONS: usize = 32;
const MAX_FILES_PER_RELATION: usize = 65_536;

pub struct OwnedStorageUsage {
    pub current_data_bytes: i64,
    pub history_data_bytes: i64,
    pub current_search_bytes: Option<i64>,
    pub history_search_bytes: Option<i64>,
    pub ordered_index_bytes: Option<i64>,
}

impl OwnedReadSession {
    /// Accounts for known, reachable files from the exact held Root catalog.
    pub async fn storage_usage(&self, relations: &[OwnedRelation]) -> Result<Vec<OwnedStorageUsage>> {
        if !(1..=MAX_RELATIONS).contains(&relations.len())
            || relations.iter().collect::<BTreeSet<_>>().len() != relations.len()
            || relations.iter().any(|relation| relation.world().is_some() || relation.is_world_catalog())
        {
            return Err(Anomaly::incorrect("rtdb.owned/storage-usage-scope",
                "Storage usage requires one to 32 distinct Root relations"));
        }
        self.admit_read().await?;
        let guard = self.relation_release_guard_for(relations, true)?;
        guard()?;
        let context = Arc::clone(&self.context).with_owned_read_guard(Arc::clone(&guard));
        let database = context.catalog.database(&self.database)?;
        let [snapshot] = database.partitions.as_slice() else { return Err(invalid_inventory()); };
        if snapshot.tx_basis() != Some(self.present) { return Err(invalid_inventory()); }
        let mut totals = BTreeMap::new();
        let mut rows = Vec::with_capacity(relations.len());
        let _relations = context.memory.reserve(relations.len() * 1024)?;
        for relation in relations {
            let table = relation.table(&self.owner);
            totals.insert(table.clone(), 0);
            let mut data = [0, 0];
            let mut search = [Some(0), Some(0)];
            if let Some(entry) = snapshot.table_entry(&table) {
                let mut files = BTreeMap::new();
                let mut charges = Vec::new();
                let mut observed = 0;
                for trie in snapshot.trie_catalog().iter_tries(&table).filter(|trie| trie.state == TrieState::Live) {
                    context.check_cancelled()?;
                    observed += 1;
                    if observed > MAX_FILES_PER_RELATION { return Err(invalid_inventory()); }
                    let side = if trie.key.recency().is_some() { 1 } else { 0 };
                    data[side] = add_bytes(data[side], trie.data_file_size)?;
                    for artefact in trie.artefacts.iter().filter(|file| file.kind() == "search") {
                        context.check_cancelled()?;
                        observed += 1;
                        if observed > MAX_FILES_PER_RELATION || artefact.file_size.is_some_and(|size| size < 0) {
                            return Err(invalid_inventory());
                        }
                        let name = entry.slug.recorded_artefact_key(&trie.key, artefact)?;
                        match files.entry(name) {
                            Entry::Vacant(file) => {
                                charges.push(context.memory.reserve(file.key().len().saturating_add(256))?);
                                file.insert((side, artefact.file_size));
                            }
                            Entry::Occupied(mut file) => {
                                let (known_side, known_size) = file.get_mut();
                                if *known_side != side || (*known_size).zip(artefact.file_size).is_some_and(|(a, b)| a != b) {
                                    return Err(invalid_inventory());
                                }
                                *known_size = (*known_size).or(artefact.file_size);
                            }
                        }
                    }
                }
                for (_, (side, size)) in files {
                    search[side] = match (search[side], size) {
                        (Some(bytes), Some(size)) => Some(add_bytes(bytes, size)?),
                        _ => None,
                    };
                }
            }
            rows.push(OwnedStorageUsage {
                current_data_bytes: data[0], history_data_bytes: data[1],
                current_search_bytes: search[0], history_search_bytes: search[1],
                ordered_index_bytes: None,
            });
        }
        let ordered = crate::ordered_index::owned_file_bytes(&context, &self.database, &self.owner, &mut totals);
        let measured = tokio::time::timeout(Duration::from_secs(2),
            context.reading(ordered)).await;
        if matches!(measured, Ok(Ok(()))) {
            for (relation, row) in relations.iter().zip(&mut rows) {
                row.ordered_index_bytes = totals.get(&relation.table(&self.owner)).copied();
            }
        }
        self.check()?;
        guard()?;
        Ok(rows)
    }
}

fn add_bytes(total: i64, size: i64) -> Result<i64> {
    if size < 0 { return Err(invalid_inventory()); }
    total.checked_add(size).ok_or_else(invalid_inventory)
}

fn invalid_inventory() -> Anomaly {
    Anomaly::incorrect("rtdb.owned/storage-inventory", "Owned storage inventory is inconsistent or exceeds its scope budget")
}
