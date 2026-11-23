use std::collections::{BTreeMap, BTreeSet};

use rtdb_format::log::proto::{RetiredSearchRun, SearchRun, SearchRunChange};
use rtdb_types::{Anomaly, Result, Timestamp};

#[derive(Clone, Debug, Default)]
pub(crate) struct SearchManifest {
    active: BTreeMap<[u8; 32], SearchRun>,
    retired: BTreeMap<[u8; 32], RetiredSearchRun>,
}

impl SearchManifest {
    pub fn load(active: &[SearchRun], retired: &[RetiredSearchRun]) -> Result<Self> {
        let mut manifest = Self::default();
        for run in active {
            if !manifest.publish(SearchRunChange { run: Some(run.clone()), ..Default::default() }, Timestamp(0)) {
                return Err(invalid("Checkpoint contains overlapping search extents"));
            }
        }
        for retired in retired {
            let run = retired.run.as_ref().ok_or_else(|| invalid("Retired search object has no receipt"))?;
            run.validate()?;
            let id = run.identity()?;
            if manifest.active.contains_key(&id) || manifest.retired.insert(id, retired.clone()).is_some() {
                return Err(invalid("Checkpoint repeats a retired search object"));
            }
        }
        Ok(manifest)
    }

    pub fn active(&self) -> impl Iterator<Item = &SearchRun> { self.active.values() }

    pub fn retired(&self) -> impl Iterator<Item = &RetiredSearchRun> { self.retired.values() }

    pub fn forget(&mut self, run: &SearchRun) {
        if let Ok(id) = run.identity() && self.retired.get(&id).is_some_and(|retired| retired.run.as_ref() == Some(run)) {
            self.retired.remove(&id);
        }
    }

    pub fn apply_uploaded(&mut self, change: SearchRunChange, as_of: Timestamp) {
        let uploaded = if change.retire_only() { None } else { change.run.clone() };
        if self.publish(change, as_of) { return; }
        if let Some(run) = uploaded && run.validate().is_ok()
            && let Ok(id) = run.identity() && !self.active.contains_key(&id) {
            self.retired.entry(id).or_insert(RetiredSearchRun { run: Some(run), superseded_at: Some(as_of.0) });
        }
    }

    pub fn publish(&mut self, change: SearchRunChange, as_of: Timestamp) -> bool {
        let retire_only = change.retire_only();
        let Some(run) = change.run else { return false; };
        if run.validate().is_err() { return false; }
        let Ok(id) = run.identity() else { return false; };
        if retire_only {
            if self.active.get(&id) != Some(&run) { return false; }
            self.active.remove(&id);
            self.retired.insert(id, RetiredSearchRun { run: Some(run), superseded_at: Some(as_of.0) });
            return true;
        }
        if self.active.get(&id) == Some(&run) { return true; }
        if self.retired.contains_key(&id) { return false; }
        let owns = |other: &SearchRun| other.definition_id == run.definition_id && other.definition_version == run.definition_version;
        let Some(replaces) = change.replaces.iter().map(|id| <[u8; 32]>::try_from(id.as_slice()).ok()).collect::<Option<BTreeSet<_>>>() else {
            return false;
        };
        if replaces.len() != change.replaces.len() || replaces.contains(&id) { return false; }
        let mut covered = BTreeSet::new();
        let mut source_rows = 0i64;
        let mut has_erases = false;
        for key in &replaces {
            let Some(input) = self.active.get(key).filter(|input| owns(input)) else { return false; };
            covered.extend(input.blocks.iter().copied());
            let Some(rows) = source_rows.checked_add(input.source_rows()) else { return false; };
            source_rows = rows;
            has_erases |= input.has_erases();
        }
        if !replaces.is_empty() && (source_rows != run.source_rows() || has_erases != run.has_erases()
            || !covered.iter().copied().eq(run.blocks.iter().copied())) { return false; }
        if self.active.iter().filter(|(key, other)| !replaces.contains(*key) && owns(other)).any(|(_, other)| {
            other.blocks.iter().any(|block| run.blocks.binary_search(block).is_ok())
        }) { return false; }
        for key in replaces {
            let input = self.active.remove(&key).expect("replacement inputs were checked");
            self.retired.insert(key, RetiredSearchRun { run: Some(input), superseded_at: Some(as_of.0) });
        }
        self.active.insert(id, run);
        true
    }
}

fn invalid(reason: impl Into<String>) -> Anomaly { Anomaly::incorrect("rtdb.index/search-manifest", reason) }

#[cfg(test)]
mod tests {
    use super::*;

    fn run(id: u8, blocks: &[i64]) -> SearchRun {
        SearchRun { definition_id: Some("searchf16".into()), definition_version: Some(20),
            blocks: blocks.to_vec(), digest: Some(vec![id; 32]), file_size: Some(100), locator: Some(vec![1]),
            source_rows: Some(blocks.len() as i64), ..Default::default() }
    }

    #[test]
    fn replacement_requires_the_exact_coverage_of_still_active_inputs() {
        let mut search = SearchManifest::default();
        let a = run(1, &[0]);
        let b = run(2, &[3]);
        let publish = |search: &mut SearchManifest, run: SearchRun, replaces: &[u8]| {
            search.publish(SearchRunChange { run: Some(run), replaces: replaces.iter().map(|&id| vec![id; 32]).collect(),
                ..Default::default() }, Timestamp(42))
        };
        assert!(publish(&mut search, a.clone(), &[]));
        assert!(publish(&mut search, b.clone(), &[]));
        assert!(!publish(&mut search, run(3, &[0, 3]), &[]));
        assert!(!publish(&mut search, run(3, &[0, 1, 3]), &[1, 2]));
        let retained = search.clone();
        let merged = run(3, &[0, 3]);
        assert!(publish(&mut search, merged.clone(), &[1, 2]));
        assert!(!publish(&mut search, run(4, &[0, 3]), &[1, 2]));
        assert_eq!(retained.active().cloned().collect::<Vec<_>>(), [a.clone(), b]);
        assert_eq!(search.active().cloned().collect::<Vec<_>>(), [merged.clone()]);
        let retired: Vec<_> = search.retired().cloned().collect();
        assert_eq!(retired.len(), 2);
        assert!(retired.iter().all(|receipt| receipt.superseded_at() == 42));
        let mut recovered = SearchManifest::load(&[merged], &retired).unwrap();
        recovered.forget(&a);
        assert_eq!(recovered.retired().count(), 1);
    }

    #[test]
    fn a_pinned_catalog_prevents_retired_search_run_collection() {
        use rtdb_format::keys::TrieKey;
        use rtdb_types::{BlockIndex, TableRef};

        let state = crate::PartitionState::empty(rtdb_trie::TrieConfig::default()).unwrap();
        let table = TableRef::new("public", "docs");
        let key = TrieKey::l0(BlockIndex(0)).unwrap();
        let source = crate::CatalogTrie::notification(key.clone(), 1024, None).unwrap();
        state.add_tries(&BTreeMap::from([(table.clone(), vec![source])]), Timestamp(1)).unwrap();
        let notify = |run, replaces| SearchRunChange { run: Some(run), replaces, ..Default::default() }.to_artefact().unwrap();
        state.add_artefacts_at(&table, vec![(key.clone(), notify(run(1, &[0]), Vec::new()))], Timestamp(2));
        let pinned = state.snapshot();
        state.add_artefacts_at(&table, vec![(key, notify(run(2, &[0]), vec![vec![1; 32]]))], Timestamp(3));
        assert_eq!(state.trie_collection().eligible_search_runs(&table, Timestamp(4)).count(), 0);
        assert_eq!(pinned.trie_catalog().search_runs(&table).next(), Some(&run(1, &[0])));
        drop(pinned);
        assert_eq!(state.trie_collection().eligible_search_runs(&table, Timestamp(2)).count(), 0);
        assert_eq!(state.trie_collection().eligible_search_runs(&table, Timestamp(4)).count(), 1);
    }
}
