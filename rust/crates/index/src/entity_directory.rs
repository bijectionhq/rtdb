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
