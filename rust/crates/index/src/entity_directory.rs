use rtdb_format::{
    keys::TrieKey,
    log::proto::{EntityDirectoryChange, EntityDirectoryRoot},
};
use rtdb_types::{Anomaly, Result};

#[derive(Clone, Debug, Default)]
pub(crate) struct DirectoryManifest {
    active: Option<EntityDirectoryRoot>,
    retired: Vec<EntityDirectoryRoot>,
}

impl DirectoryManifest {
    pub fn load(
        active: Option<EntityDirectoryRoot>,
        retired: Vec<EntityDirectoryRoot>,
    ) -> Result<Self> {
        if let Some(root) = &active {
            root.reference()?;
        }
        for root in &retired {
            root.reference()?;
            if active
                .as_ref()
                .is_none_or(|active| active.through_block() <= root.through_block())
            {
                return Err(Anomaly::fault(
                    "rtdb.index/entity-directory",
                    "Retired directory must precede the active root",
                ));
            }
        }
        Ok(Self { active, retired })
    }

    pub fn active(&self) -> Option<&EntityDirectoryRoot> {
        self.active.as_ref()
    }
    pub fn retired(&self) -> impl Iterator<Item = &EntityDirectoryRoot> {
        self.retired.iter()
    }

    pub fn accepts(&self, key: &TrieKey, change: &EntityDirectoryChange) -> bool {
        let Some(root) = &change.root else {
            return false;
        };
        key.level() == 0
            && root.through_block() == key.block_index().0
            && change.replaces_root()
                == self
                    .active
                    .as_ref()
                    .map_or(&[] as &[u8], |root| root.root())
            && self.active.as_ref().is_none_or(|old| {
                root.through_block() > old.through_block()
                    && root.writer_fence() >= old.writer_fence()
            })
    }

    pub fn install(&mut self, key: &TrieKey, change: EntityDirectoryChange) {
        if self.accepts(key, &change)
            && let Some(old) = std::mem::replace(&mut self.active, change.root)
        {
            self.retired.push(old);
        }
    }

    pub fn forget(&mut self, root: &EntityDirectoryRoot) {
        self.retired.retain(|recorded| {
            recorded.root() != root.root() || recorded.allocation() != root.allocation()
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_format::{
        catalog::TableBlock,
        cow_tree::{PageDigest, RootRef},
    };
    use rtdb_types::{BlockIndex, TableRef, Timestamp};

    use crate::{CatalogTrie, TrieCatalog};

    #[test]
    fn a_moved_directory_root_refuses_adoption_but_not_later_custody_steps() {
        use rtdb_format::log::proto::{CustodyEvidence, SearchRun, SearchRunChange, SourceReceipt, custody_evidence::Kind};

        let table = TableRef::new("public", "docs");
        let trie = |block| TrieKey::l0(BlockIndex(block)).unwrap();
        let receipt = |block: i64| EntityDirectoryRoot {
            root: Some(RootRef { digest: PageDigest([block as u8 + 1; 32]), length: 512, height: 0, summary: Vec::new() }.encode()),
            through_block: Some(block), source_rows: Some(10), has_erases: Some(false), allocation: Some(vec![block as u8; 16]),
            writer_fence: Some(3), erase_frontier_digest: Some(vec![0; 32]),
        };
        let (first, second) = (receipt(0), receipt(1));
        let catalog = TrieCatalog::new(4).unwrap()
            .with_tries(table.clone(), (0..2).map(|block| CatalogTrie::notification(trie(block), 10, None).unwrap()), Timestamp(0)).unwrap()
            .with_artefacts(&table, [(trie(0), EntityDirectoryChange { root: Some(first.clone()), replaces_root: None }.to_artefact().unwrap())])
            .with_artefacts(&table, [(trie(1), EntityDirectoryChange { root: Some(second), replaces_root: first.root.clone() }.to_artefact().unwrap())]);
        let run = SearchRun { allocation: Some(vec![9; 16]), writer_fence: Some(3), source_receipt: Some(SourceReceipt {
            table: Some(table.schema_and_table()), directory_root: first.root.clone(), erase_frontier_digest: first.erase_frontier_digest.clone(),
            ..Default::default() }), ..Default::default() };
        let uploaded = Some(CustodyEvidence { kind: Some(Kind::Uploaded as i32), fence: None });
        let change = |reserve, stage, evidence: Option<CustodyEvidence>| SearchRunChange { run: Some(run.clone()),
            reserve: Some(reserve), stage: Some(stage), custody_evidence: evidence, ..Default::default() };
        assert!(catalog.accepts_search_source(&table, &change(true, false, None)));
        assert!(catalog.accepts_search_source(&table, &change(false, true, uploaded.clone())));
        assert!(!catalog.accepts_search_source(&table, &change(false, false, uploaded)));
    }

    #[test]
    fn collection_keeps_garbage_l0_sources_the_directory_has_not_sealed() {
        use std::collections::BTreeMap;

        let table = TableRef::new("public", "docs");
        let trie = |block| TrieKey::l0(BlockIndex(block)).unwrap();
        let state = crate::PartitionState::empty(rtdb_trie::TrieConfig::default()).unwrap();
        state.add_tries(&BTreeMap::from([(table.clone(),
            (0..3).map(|block| CatalogTrie::notification(trie(block), 10, None).unwrap()).collect())]), Timestamp(1)).unwrap();
        let sealed = EntityDirectoryRoot {
            root: Some(RootRef { digest: PageDigest([1; 32]), length: 512, height: 0, summary: Vec::new() }.encode()),
            through_block: Some(0), source_rows: Some(10), has_erases: Some(false), allocation: Some(vec![0; 16]),
            writer_fence: Some(3), erase_frontier_digest: Some(vec![0; 32]),
        };
        state.add_artefacts_at(&table, vec![(trie(0), EntityDirectoryChange { root: Some(sealed), replaces_root: None }.to_artefact().unwrap())], Timestamp(2));
        let compacted = CatalogTrie::notification("l01-rc-b02".parse().unwrap(), 30, None).unwrap();
        state.add_tries(&BTreeMap::from([(table.clone(), vec![compacted])]), Timestamp(3)).unwrap();
        assert_eq!(state.trie_collection().eligible(&table, Timestamp(i64::MAX)), vec![trie(0)]);
    }

    #[test]
    fn a_stale_directory_replacement_does_not_change_either_catalog_generation() {
        let table = TableRef::new("public", "docs");
        let trie = |block| TrieKey::l0(BlockIndex(block)).unwrap();
        let receipt = |block| EntityDirectoryRoot {
            root: Some(
                RootRef {
                    digest: PageDigest([block as u8 + 1; 32]),
                    length: 512,
                    height: 0,
                    summary: Vec::new(),
                }
                .encode(),
            ),
            through_block: Some(block),
            source_rows: Some(10),
            has_erases: Some(false),
            allocation: Some(vec![block as u8; 16]),
            writer_fence: Some(3),
            erase_frontier_digest: Some(vec![0; 32]),
        };
        let publish = |next: EntityDirectoryRoot, previous: Option<&EntityDirectoryRoot>| {
            EntityDirectoryChange {
                root: Some(next),
                replaces_root: previous.and_then(|old| old.root.clone()),
            }
            .to_artefact()
            .unwrap()
        };
        let initial = TrieCatalog::new(4)
            .unwrap()
            .with_tries(
                table.clone(),
                (0..3).map(|block| CatalogTrie::notification(trie(block), 10, None).unwrap()),
                Timestamp(0),
            )
            .unwrap();
        let (first, second, stale) = (receipt(0), receipt(1), receipt(2));
        let pinned = initial.with_artefacts(&table, [(trie(0), publish(first.clone(), None))]);
        let current =
            pinned.with_artefacts(&table, [(trie(1), publish(second.clone(), Some(&first)))]);
        let rejected = publish(stale, Some(&first));
        assert!(!current.accepts_entity_directory(&table, &trie(2), &rejected));
        let current = current.with_artefacts(&table, [(trie(2), rejected)]);
        assert_eq!(pinned.entity_directory(&table), Some(&first));
        assert_eq!(current.entity_directory(&table), Some(&second));
        assert_eq!(
            current
                .retired_entity_directories(&table)
                .collect::<Vec<_>>(),
            [&first]
        );
        let block = TableBlock::from_parts(
            Default::default(),
            &Default::default(),
            10,
            current.partitions(&table).unwrap(),
        )
        .unwrap();
        let block =
            TableBlock::decode(&current.with_entity_directory(&table, block).encode()).unwrap();
        let recovered = TrieCatalog::load(
            &std::collections::BTreeMap::from([(table.clone(), block)]),
            4,
            Timestamp(0),
        )
        .unwrap();
        assert_eq!(recovered.entity_directory(&table), Some(&second));
        assert_eq!(
            recovered
                .retired_entity_directories(&table)
                .collect::<Vec<_>>(),
            [&first]
        );
    }
}
