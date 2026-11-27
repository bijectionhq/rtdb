use rtdb_format::{
    keys::TrieKey,
    log::proto::{EntityDirectoryChange, EntityDirectoryRoot},
};
use rtdb_types::{Anomaly, Result, Timestamp};

#[derive(Clone, Debug, Default)]
pub(crate) struct DirectoryManifest {
    active: Option<EntityDirectoryRoot>,
    retired: Vec<EntityDirectoryRoot>,
}

impl DirectoryManifest {
    /// A retired root recorded before roots carried their supersession time counts as superseded at `loaded_at`;
    /// the next table block records that time, so the wait is not restarted by later loads.
    pub fn load(
        active: Option<EntityDirectoryRoot>,
        mut retired: Vec<EntityDirectoryRoot>,
        loaded_at: Timestamp,
    ) -> Result<Self> {
        if let Some(root) = &active {
            root.reference()?;
        }
        for root in &mut retired {
            root.superseded_at.get_or_insert(loaded_at.0);
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

    /// Retires the replaced root as superseded at `as_of`, the system time of the transaction that replaced it.
    pub fn install(&mut self, key: &TrieKey, change: EntityDirectoryChange, as_of: Timestamp) {
        if self.accepts(key, &change)
            && let Some(mut old) = std::mem::replace(&mut self.active, change.root)
        {
            old.superseded_at = Some(as_of.0);
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
            writer_fence: Some(epoch), erase_frontier_digest: Some(vec![0; 32]), superseded_at: None,
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
    fn a_trie_carrying_a_malformed_directory_record_fails_instead_of_losing_the_directory() {
        let table = TableRef::new("public", "docs");
        let key = TrieKey::l0(BlockIndex(0)).unwrap();
        let root = EntityDirectoryRoot {
            root: Some(RootRef { digest: PageDigest([1; 32]), length: 512, height: 0, summary: Vec::new() }.encode()),
            through_block: Some(0), source_rows: Some(10), has_erases: Some(false), allocation: Some(vec![0; 16]),
            writer_fence: Some(3), erase_frontier_digest: Some(vec![0; 32]), superseded_at: None,
        };
        let valid = EntityDirectoryChange { root: Some(root.clone()), replaces_root: None }.to_artefact().unwrap();
        let carrying = |record| {
            let mut trie = CatalogTrie::notification(key.clone(), 10, None).unwrap();
            trie.artefacts = vec![record];
            trie
        };
        let catalog = TrieCatalog::new(4).unwrap();

        let installed = catalog.with_tries(table.clone(), [carrying(valid.clone())], Timestamp(0)).unwrap();
        assert_eq!(installed.entity_directory(&table), Some(&root));

        let corrupt = rtdb_format::log::proto::ArtefactRecord { stats: Some(vec![0xff; 7]), ..valid.clone() };
        assert!(catalog.with_tries(table.clone(), [carrying(corrupt)], Timestamp(0)).is_err());
        let newer = rtdb_format::log::proto::ArtefactRecord { definition_version: Some(valid.definition_version() + 1), ..valid };
        assert!(catalog.with_tries(table.clone(), [carrying(newer)], Timestamp(0)).is_err());
    }

    #[test]
    fn a_malformed_search_record_on_a_trie_fails_the_catalog_update() {
        let table = TableRef::new("public", "docs");
        let mut trie = CatalogTrie::notification(TrieKey::l0(BlockIndex(0)).unwrap(), 10, None).unwrap();
        trie.artefacts = vec![rtdb_format::log::proto::ArtefactRecord {
            kind: Some(rtdb_format::search_manifest::RECORD_KIND.into()), stats: Some(vec![0xff; 7]), ..Default::default()
        }];
        assert!(TrieCatalog::new(4).unwrap().with_tries(table, [trie], Timestamp(0)).is_err());
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
            superseded_at: None,
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
        let retired = EntityDirectoryRoot { superseded_at: Some(0), ..first.clone() };
        assert_eq!(
            current
                .retired_entity_directories(&table)
                .collect::<Vec<_>>(),
            [&retired]
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
            [&retired]
        );
    }

    #[test]
    fn a_replaced_root_records_when_it_was_superseded_and_a_legacy_one_takes_its_load_time() {
        let key = |block| TrieKey::l0(BlockIndex(block)).unwrap();
        let receipt = |block: i64| EntityDirectoryRoot {
            root: Some(RootRef { digest: PageDigest([block as u8 + 1; 32]), length: 512, height: 0, summary: Vec::new() }.encode()),
            through_block: Some(block), source_rows: Some(10), has_erases: Some(false), allocation: Some(vec![block as u8; 16]),
            writer_fence: Some(1), erase_frontier_digest: Some(vec![0; 32]), superseded_at: None,
        };
        let mut manifest = DirectoryManifest::default();
        manifest.install(&key(0), EntityDirectoryChange { root: Some(receipt(0)), replaces_root: None }, Timestamp(5));
        manifest.install(&key(1), EntityDirectoryChange { root: Some(receipt(1)), replaces_root: receipt(0).root }, Timestamp(9));
        assert_eq!(manifest.retired().map(|root| root.superseded_at).collect::<Vec<_>>(), [Some(9)]);

        let legacy = DirectoryManifest::load(Some(receipt(2)), vec![receipt(1), manifest.retired().next().unwrap().clone()], Timestamp(40)).unwrap();
        assert_eq!(legacy.retired().map(|root| root.superseded_at).collect::<Vec<_>>(), [Some(40), Some(9)]);
    }
}
