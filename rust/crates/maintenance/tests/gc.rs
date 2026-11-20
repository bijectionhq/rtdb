use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use bytes::Bytes;
use rtdb_format::{block::proto as pb, catalog::BlockCatalog, keys::{TrieKey, block_key, oversized_key}};
use rtdb_index::{CatalogTrie, PartitionState, PartitionValue, TableCatalog, TrieCatalog, TrieState};
use rtdb_maintenance::{BlockCollector, CollectionObserver, CycleWorker, GarbageConfig, TrieCollector, TrieDeletionPublisher, block_is_garbage};
use rtdb_storage::{MemoryStore, ObjectKey, ObjectStore};
use rtdb_trie::TrieConfig;
use rtdb_types::{Anomaly, BlockIndex, ReplicaPosition, Result, TableRef, Timestamp};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Observer(Mutex<Vec<Anomaly>>);
impl CollectionObserver for Observer { fn table_failed(&self, _: Option<&TableRef>, error: &Anomaly) { self.0.lock().unwrap().push(error.clone()); } }

fn block(index: i64) -> BlockCatalog {
    BlockCatalog::from_proto(pb::Block { block_index: Some(index), latest_completed_tx: Some(pb::TxKey { tx_id: Some(index), system_time: Some(index * 1_000_000), ..Default::default() }), ..Default::default() }).unwrap()
}

#[test]
fn retention_never_selects_the_current_block() {
    assert!(!block_is_garbage(BlockIndex(10), BlockIndex(10), 0));
    assert!(block_is_garbage(BlockIndex(7), BlockIndex(10), 3));
    assert!(!block_is_garbage(BlockIndex(8), BlockIndex(10), 3));
}

#[tokio::test]
async fn block_gc_collects_old_blocks_and_offloaded_payloads() {
    let store = Arc::new(MemoryStore::default());
    for index in 1..=10 {
        store.put(&ObjectKey::new(block_key(BlockIndex(index)).unwrap()).unwrap(), Bytes::from(block(index).encode())).await.unwrap();
    }
    for index in [6, 9] {
        store.put(&ObjectKey::new(oversized_key(BlockIndex(index), "payload").unwrap()).unwrap(), Bytes::from_static(b"payload")).await.unwrap();
    }
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(TrieCatalog::default(), TableCatalog::load(Some(block(10)), &BTreeMap::new()).unwrap(), TrieConfig::default()).unwrap()));
    let observer = Arc::new(Observer::default());
    let collector = BlockCollector::new(store.clone(), state, observer.clone(), GarbageConfig { blocks_to_keep: 3, ..Default::default() }).unwrap();
    collector.collect(&CancellationToken::new()).await.unwrap();
    assert_eq!(store.list(Some(&ObjectKey::new("blocks").unwrap())).await.unwrap().len(), 3);
    assert_eq!(store.list(Some(&ObjectKey::new("oversized").unwrap())).await.unwrap().len(), 1);
    assert!(observer.0.lock().unwrap().is_empty());
}

struct DeletedPublisher { store: Arc<MemoryStore>, table: rtdb_format::catalog::TableEntry, commits: Mutex<Vec<Vec<TrieKey>>> }
#[async_trait]
impl TrieDeletionPublisher for DeletedPublisher {
    async fn publish(&self, table: TableRef, keys: Vec<TrieKey>) -> Result<ReplicaPosition> {
        assert_eq!(table, self.table.table);
        for key in &keys {
            assert!(self.store.head(&ObjectKey::new(self.table.slug.data_key(key)).unwrap()).await.is_err());
            assert!(self.store.head(&ObjectKey::new(self.table.slug.metadata_key(key)).unwrap()).await.is_err());
            assert!(self.store.list(Some(&ObjectKey::new(self.table.slug.artefact_prefix(key)).unwrap())).await.unwrap().is_empty());
        }
        self.commits.lock().unwrap().push(keys);
        Ok(ReplicaPosition(1))
    }
}

#[tokio::test]
async fn trie_gc_deletes_artefacts_and_both_files_before_publishing_and_preserves_l0_recovery() {
    let table = TableRef::new("public", "docs");
    let tables = TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let entry = tables.entry(&table).unwrap().clone();
    let mut catalog = TrieCatalog::new(16).unwrap();
    for key in ["l00-rc-b00", "l01-rc-b00", "l01-rc-b01"] {
        catalog = catalog.with_tries(table.clone(), [CatalogTrie { key: key.parse().unwrap(), data_file_size: 1, metadata: None, state: TrieState::Live, artefacts: Vec::new() }], Timestamp(100)).unwrap();
    }
    let store = Arc::new(MemoryStore::default());
    for key in ["l00-rc-b00", "l01-rc-b00"] {
        let key: TrieKey = key.parse().unwrap();
        store.put(&ObjectKey::new(entry.slug.data_key(&key)).unwrap(), Bytes::from_static(b"data")).await.unwrap();
        store.put(&ObjectKey::new(entry.slug.metadata_key(&key)).unwrap(), Bytes::from_static(b"metadata")).await.unwrap();
        for (kind, id) in [("search", "body"), ("search", "title")] {
            store.put(&ObjectKey::new(entry.slug.artefact_key(&key, kind, id, 1).unwrap()).unwrap(), Bytes::from_static(b"artefact")).await.unwrap();
        }
    }
    let live_artefact = ObjectKey::new(entry.slug.artefact_key(&"l01-rc-b01".parse().unwrap(), "search", "body", 1).unwrap()).unwrap();
    store.put(&live_artefact, Bytes::from_static(b"artefact")).await.unwrap();
    let publisher = Arc::new(DeletedPublisher { store: store.clone(), table: entry.clone(), commits: Mutex::new(Vec::new()) });
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap()));
    let collector = TrieCollector::new(store.clone(), state, publisher.clone(), Arc::new(Observer::default()), GarbageConfig::default()).unwrap();
    collector.collect_as_of(Timestamp(100), &CancellationToken::new()).await.unwrap();
    assert_eq!(*publisher.commits.lock().unwrap(), vec![vec!["l01-rc-b00".parse::<TrieKey>().unwrap()]]);
    assert!(store.head(&ObjectKey::new(entry.slug.data_key(&"l00-rc-b00".parse().unwrap())).unwrap()).await.is_ok());
    assert!(store.head(&live_artefact).await.is_ok(), "a live trie's artefacts stay");
    collector.collect_as_of(Timestamp(100), &CancellationToken::new()).await.unwrap();
    assert_eq!(publisher.commits.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn a_groups_artefacts_go_only_with_the_last_member_the_catalog_holds() {
    let table = TableRef::new("public", "docs");
    let tables = TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let entry = tables.entry(&table).unwrap().clone();
    let add = |catalog: TrieCatalog, key: &str, at: i64| catalog.with_tries(table.clone(), [CatalogTrie { key: key.parse().unwrap(), data_file_size: 1, metadata: None, state: TrieState::Live, artefacts: Vec::new() }], Timestamp(at)).unwrap();
    let mut catalog = TrieCatalog::new(16).unwrap();
    for bucket in 0..4 { catalog = add(catalog, &format!("l02-rc-p{bucket}-b00"), 100); }
    let supersede = |mut catalog: TrieCatalog, part: u8| { for bucket in 0..4 { catalog = add(catalog, &format!("l03-rc-p{part}{bucket}-b01"), 100 + i64::from(part)); } catalog };
    let group = "l02-rc-p0-b00".parse::<TrieKey>().unwrap().part_group().unwrap();
    let object = ObjectKey::new(entry.slug.group_artefact_prefix(&group) + "/search/body-v1.bin").unwrap();
    let store = Arc::new(MemoryStore::default());
    store.put(&object, Bytes::from_static(b"artefact")).await.unwrap();
    let collect = |catalog: TrieCatalog| {
        let (store, tables, entry) = (store.clone(), tables.clone(), entry.clone());
        async move {
            let publisher = Arc::new(DeletedPublisher { store: store.clone(), table: entry, commits: Mutex::new(Vec::new()) });
            let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap()));
            let collector = TrieCollector::new(store, state, publisher.clone(), Arc::new(Observer::default()), GarbageConfig::default()).unwrap();
            collector.collect_as_of(Timestamp(200), &CancellationToken::new()).await.unwrap();
            publisher.commits.lock().unwrap().concat()
        }
    };
    let one_superseded = supersede(catalog.clone(), 0);
    assert_eq!(collect(one_superseded.clone()).await, ["l02-rc-p0-b00".parse::<TrieKey>().unwrap()]);
    assert!(store.head(&object).await.is_ok(), "three members are still live");
    let collected = one_superseded.without_tries(&table, &["l02-rc-p0-b00".parse().unwrap()].into());
    let all_superseded = (1..4).fold(collected, supersede);
    assert_eq!(collect(all_superseded).await.len(), 3);
    assert!(store.head(&object).await.is_err(), "the last members take the group's artefacts with them");
}
