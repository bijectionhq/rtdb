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
            && self.active.as_ref().is_none_or(|old| root.through_block() > old.through_block())
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
    fn a_moved_directory_root_refuses_new_adoption_and_allows_routing_for_an_active_run() {
        use rtdb_format::log::proto::{SearchRun, SearchRunChange, SourceReceipt};

        let table = TableRef::new("public", "docs");
        let trie = |block| TrieKey::l0(BlockIndex(block)).unwrap();
        let receipt = |block: i64, epoch| EntityDirectoryRoot {
            root: Some(RootRef { digest: PageDigest([block as u8 + 1; 32]), length: 512, height: 0, summary: Vec::new() }.encode()),
            through_block: Some(block), source_rows: Some(10), has_erases: Some(false), allocation: Some(vec![block as u8; 16]),
            writer_fence: Some(epoch), erase_frontier_digest: Some(vec![0; 32]),
        };
        let (first, second) = (receipt(0, 3), receipt(1, 1));
        let catalog = TrieCatalog::new(4).unwrap()
            .with_tries(table.clone(), (0..2).map(|block| CatalogTrie::notification(trie(block), 10, None).unwrap()), Timestamp(0)).unwrap()
            .with_artefacts(&table, [(trie(0), EntityDirectoryChange { root: Some(first.clone()), replaces_root: None }.to_artefact().unwrap())]);
        let run = SearchRun {
            definition_id: Some("bodyf16".into()), definition_version: Some(1), digest: Some(vec![1; 32]),
            locator: Some(vec![1]), file_size: Some(10), blocks: vec![0], source_rows: Some(10),
            allocation: Some(vec![9; 16]), source_receipt: Some(SourceReceipt {
                lineage: Some("hosted/docs".into()), checkpoint_boundary: Some(2), suffix_through: Some(2),
                table: Some(table.schema_and_table()), directory_root: first.root.clone(), erase_frontier_digest: first.erase_frontier_digest.clone(),
                checkpoint_catalog_sha256: Some(vec![2; 32]), shard_range: Some(rtdb_format::search_manifest::FULL_SHARD_RANGE.to_vec()),
                source_rows: Some(10), blocks: vec![0],
            }), ..Default::default()
        };
        let adoption = SearchRunChange { run: Some(run.clone()), ..Default::default() };
        let catalog = catalog.with_artefacts(&table, [(trie(0), adoption.to_artefact().unwrap())]);
        assert_eq!(catalog.search_runs(&table).next(), Some(&run));
        let catalog = catalog.with_artefacts(&table, [(trie(1), EntityDirectoryChange { root: Some(second.clone()), replaces_root: first.root }.to_artefact().unwrap())]);
        assert_eq!(catalog.entity_directory(&table), Some(&second));
        assert!(catalog.accepts_search_source(&table, &adoption));
        let other = SearchRun { digest: Some(vec![3; 32]), ..run.clone() };
        assert!(!catalog.accepts_search_source(&table, &SearchRunChange { run: Some(other), ..Default::default() }));
        let retirement = SearchRunChange { run: Some(run), retire_only: Some(true), ..Default::default() };
        assert!(catalog.accepts_search_source(&table, &retirement));
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
