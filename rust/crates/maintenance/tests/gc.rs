use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, atomic::{AtomicI64, Ordering}};
use std::time::Duration;
use async_trait::async_trait;
use bytes::Bytes;
use rtdb_format::{block::proto as pb, catalog::BlockCatalog, keys::{TrieKey, block_key, oversized_key}};
use rtdb_index::{CatalogTrie, PartitionState, PartitionValue, TableCatalog, TrieCatalog, TrieState};
use rtdb_maintenance::{BlockCollector, CollectionObserver, CycleWorker, GarbageConfig, TrieCollector, TrieDeletionPublisher, block_is_garbage};
use rtdb_storage::{IoRegion, MemoryStore, ObjectKey, ObjectStore};
use rtdb_trie::TrieConfig;
use rtdb_types::{Anomaly, BlockIndex, ReplicaPosition, Result, TableRef, Timestamp};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Observer(Mutex<Vec<Anomaly>>);
impl CollectionObserver for Observer { fn table_failed(&self, _: Option<&TableRef>, error: &Anomaly) { self.0.lock().unwrap().push(error.clone()); } }

/// No reader on another node holds a lease, so collection need not wait.
fn unleased() -> GarbageConfig { GarbageConfig { reader_lease: Duration::ZERO, reader_lease_margin: Duration::ZERO, ..GarbageConfig::default() } }
fn fixed_clock() -> rtdb_maintenance::CollectorClock { Arc::new(|| Timestamp(0)) }

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

#[tokio::test]
async fn ending_the_leader_term_cancels_block_gc_without_recording_a_failure() {
    let store = Arc::new(MemoryStore::default());
    let oldest = ObjectKey::new(block_key(BlockIndex(1)).unwrap()).unwrap();
    store.put(&oldest, Bytes::from(block(1).encode())).await.unwrap();
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(TrieCatalog::default(), TableCatalog::load(Some(block(10)), &BTreeMap::new()).unwrap(), TrieConfig::default()).unwrap()));
    let observer = Arc::new(Observer::default());
    let collector = BlockCollector::new(store.clone(), state, observer.clone(), GarbageConfig { blocks_to_keep: 1, ..Default::default() }).unwrap();
    let term = CancellationToken::new();
    term.cancel();

    assert_eq!(collector.collect(&term).await.unwrap_err().code, "rtdb.maintenance/closed");
    assert!(observer.0.lock().unwrap().is_empty());
    assert!(store.head(&oldest).await.is_ok());
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
    let collector = TrieCollector::new(store.clone(), state, publisher.clone(), Arc::new(Observer::default()), unleased(), fixed_clock()).unwrap();
    collector.collect_as_of(Timestamp(100), &CancellationToken::new()).await.unwrap();
    assert_eq!(*publisher.commits.lock().unwrap(), vec![vec!["l01-rc-b00".parse::<TrieKey>().unwrap()]]);
    assert!(store.head(&ObjectKey::new(entry.slug.data_key(&"l00-rc-b00".parse().unwrap())).unwrap()).await.is_ok());
    assert!(store.head(&live_artefact).await.is_ok(), "a live trie's artefacts stay");
    collector.collect_as_of(Timestamp(100), &CancellationToken::new()).await.unwrap();
    assert_eq!(publisher.commits.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn collection_preserves_live_and_pending_tries_of_the_installed_checkpoint_until_replaced() {
    let table = TableRef::new("public", "docs");
    let tables = TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let entry = tables.entry(&table).unwrap().clone();
    let trie = |key: &str| CatalogTrie::notification(key.parse().unwrap(), 16, None).unwrap();
    let live = trie("l01-rc-b00");
    let pending = trie("l02-rc-p0-b00");
    let initial = TrieCatalog::new(16).unwrap()
        .with_tries(table.clone(), [live.clone(), pending.clone()], Timestamp(100)).unwrap();
    assert_eq!(initial.trie(&table, &live.key).unwrap().state, TrieState::Live);
    assert_eq!(initial.trie(&table, &pending.key).unwrap().state, TrieState::Nascent);
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(initial, tables, TrieConfig::default()).unwrap()));
    let checkpoint = |index: BlockIndex| BlockCatalog::from_proto(pb::Block {
        block_index: Some(index.0), tables: vec![entry.to_proto()], ..Default::default()
    }).unwrap();
    let no_new_l0 = BTreeMap::new();
    let prepared = state.prepare_block().unwrap();
    let root = checkpoint(prepared.block_index());
    let blocks = prepared.table_blocks(&no_new_l0, Timestamp(100)).unwrap();
    state.adopt_block(prepared, root, &blocks, &no_new_l0, Timestamp(100)).unwrap();
    state.add_tries(&BTreeMap::from([(table.clone(), [
        "l02-rc-p1-b00", "l02-rc-p2-b00", "l02-rc-p3-b00",
        "l03-rc-p00-b00", "l03-rc-p01-b00", "l03-rc-p02-b00", "l03-rc-p03-b00",
    ].into_iter().map(trie).collect())]), Timestamp(200)).unwrap();
    let current = state.value();
    for dependency in [&live, &pending] {
        assert!(matches!(current.trie_catalog().trie(&table, &dependency.key).unwrap().state, TrieState::Garbage { .. }));
    }

    let store = Arc::new(MemoryStore::default());
    let mut objects = Vec::new();
    for dependency in [&live, &pending] {
        for path in [entry.slug.data_key(&dependency.key), entry.slug.metadata_key(&dependency.key)] {
            let key = ObjectKey::new(path).unwrap();
            store.put(&key, Bytes::from_static(b"checkpoint dependency")).await.unwrap();
            objects.push(key);
        }
    }
    let observer = Arc::new(Observer::default());
    let publisher = Arc::new(DeletedPublisher { store: store.clone(), table: entry.clone(), commits: Mutex::new(Vec::new()) });
    let collector = TrieCollector::new(store.clone(), state.clone(), publisher.clone(), observer.clone(), unleased(), fixed_clock()).unwrap();
    let cancellation = CancellationToken::new();
    collector.collect_as_of(Timestamp(200), &cancellation).await.unwrap();
    assert!(publisher.commits.lock().unwrap().is_empty(), "an installed checkpoint still needs both tries");
    for object in &objects { store.head(object).await.unwrap(); }

    let prepared = state.prepare_block().unwrap();
    let root = checkpoint(prepared.block_index());
    let blocks = prepared.table_blocks(&no_new_l0, Timestamp(300)).unwrap();
    state.adopt_block(prepared, root, &blocks, &no_new_l0, Timestamp(300)).unwrap();
    collector.collect_as_of(Timestamp(200), &cancellation).await.unwrap();
    assert_eq!(publisher.commits.lock().unwrap().as_slice(), &[vec![live.key, pending.key]]);
    for object in &objects {
        assert!(matches!(store.head(object).await, Err(rtdb_storage::Error::MissingObject(_))));
    }
    assert!(observer.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn collection_preserves_open_readers_and_their_inflight_io() {
    let table = TableRef::new("public", "docs");
    let tables = TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let entry = tables.entry(&table).unwrap().clone();
    let trie = |key: &str| CatalogTrie {
        key: key.parse().unwrap(), data_file_size: 1, metadata: None, state: TrieState::Live, artefacts: Vec::new(),
    };
    let preserved = trie("l01-rc-b00");
    let unreferenced = trie("l01-rc-b01");
    let catalog = TrieCatalog::new(16).unwrap().with_tries(table.clone(), [preserved.clone()], Timestamp(100)).unwrap();
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap()));
    let first = state.open_snapshot(None);
    let second = state.snapshot();
    state.add_tries(&BTreeMap::from([(table.clone(), vec![unreferenced.clone(), trie("l01-rc-b02")])]), Timestamp(100)).unwrap();
    assert_eq!(first.trie_entries(&table)[0].key, preserved.key);
    assert_eq!(state.open_snapshot(None).trie_entries(&table)[0].key, trie("l01-rc-b02").key);

    let store = Arc::new(MemoryStore::default());
    let mut pinned_objects = Vec::new();
    for candidate in [&preserved, &unreferenced] {
        let objects = [
            entry.slug.data_key(&candidate.key),
            entry.slug.metadata_key(&candidate.key),
            entry.slug.artefact_key(&candidate.key, "search", "body", 1).unwrap(),
        ];
        for path in objects {
            let key = ObjectKey::new(path).unwrap();
            store.put(&key, Bytes::from_static(b"contents")).await.unwrap();
            if candidate.key == preserved.key { pinned_objects.push(key); }
        }
    }
    let publisher = Arc::new(DeletedPublisher { store: store.clone(), table: entry, commits: Mutex::new(Vec::new()) });
    let collector = TrieCollector::new(store.clone(), state.clone(), publisher.clone(), Arc::new(Observer::default()), unleased(), fixed_clock()).unwrap();
    let cancellation = CancellationToken::new();
    collector.collect_as_of(Timestamp(100), &cancellation).await.unwrap();
    assert_eq!(publisher.commits.lock().unwrap().as_slice(), &[vec![unreferenced.key.clone()]]);
    state.delete_tries(&BTreeMap::from([(table, [unreferenced.key].into())]));

    let region = IoRegion::new();
    let worker = region.clone();
    let (started, in_flight) = tokio::sync::oneshot::channel();
    let (finish, release) = std::sync::mpsc::channel();
    let caller = tokio::spawn(IoRegion::retaining(Arc::new(second), async move {
        worker.run_blocking(move || {
            started.send(()).unwrap();
            release.recv().unwrap();
            Ok(())
        }).await
    }));
    in_flight.await.unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    drop(first);
    collector.collect_as_of(Timestamp(100), &cancellation).await.unwrap();
    assert_eq!(publisher.commits.lock().unwrap().len(), 1);
    for key in &pinned_objects { assert!(store.get(key).await.is_ok()); }

    finish.send(()).unwrap();
    region.close_and_wait().await;
    collector.collect_as_of(Timestamp(100), &cancellation).await.unwrap();
    assert_eq!(publisher.commits.lock().unwrap()[1], vec![preserved.key]);
    for key in &pinned_objects { assert!(store.head(key).await.is_err()); }

    let collection = state.trie_collection();
    assert_eq!(state.reset_to_l0(BTreeMap::new()).unwrap_err().code, "rtdb.compactor-reset/collecting");
    drop(collection);
    state.reset_to_l0(BTreeMap::new()).unwrap();
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
            let collector = TrieCollector::new(store, state, publisher.clone(), Arc::new(Observer::default()), unleased(), fixed_clock()).unwrap();
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

#[tokio::test]
async fn collection_waits_a_whole_reader_lease_after_first_observing_garbage() {
    let table = TableRef::new("public", "docs");
    let tables = TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let entry = tables.entry(&table).unwrap().clone();
    let mut catalog = TrieCatalog::new(16).unwrap();
    for key in ["l00-rc-b00", "l01-rc-b00", "l01-rc-b01"] {
        catalog = catalog.with_tries(table.clone(), [CatalogTrie { key: key.parse().unwrap(), data_file_size: 1, metadata: None, state: TrieState::Live, artefacts: Vec::new() }], Timestamp(100)).unwrap();
    }
    let store = Arc::new(MemoryStore::default());
    let garbage: TrieKey = "l01-rc-b00".parse().unwrap();
    store.put(&ObjectKey::new(entry.slug.data_key(&garbage)).unwrap(), Bytes::from_static(b"data")).await.unwrap();
    store.put(&ObjectKey::new(entry.slug.metadata_key(&garbage)).unwrap(), Bytes::from_static(b"metadata")).await.unwrap();
    let publisher = Arc::new(DeletedPublisher { store: store.clone(), table: entry.clone(), commits: Mutex::new(Vec::new()) });
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap()));
    let now = Arc::new(AtomicI64::new(1_000));
    let clock = { let now = Arc::clone(&now); Arc::new(move || Timestamp(now.load(Ordering::Relaxed))) };
    let config = GarbageConfig { reader_lease: Duration::from_micros(500), reader_lease_margin: Duration::from_micros(100), ..GarbageConfig::default() };
    let collector = TrieCollector::new(store.clone(), state, publisher.clone(), Arc::new(Observer::default()), config, clock).unwrap();
    let cancellation = CancellationToken::new();

    collector.collect_as_of(Timestamp(100), &cancellation).await.unwrap();
    now.store(1_499, Ordering::Relaxed);
    collector.collect_as_of(Timestamp(100), &cancellation).await.unwrap();
    assert!(publisher.commits.lock().unwrap().is_empty());
    assert!(store.head(&ObjectKey::new(entry.slug.data_key(&garbage)).unwrap()).await.is_ok());

    now.store(1_500, Ordering::Relaxed);
    collector.collect_as_of(Timestamp(100), &cancellation).await.unwrap();
    assert_eq!(*publisher.commits.lock().unwrap(), vec![vec![garbage.clone()]]);
    assert!(store.head(&ObjectKey::new(entry.slug.data_key(&garbage)).unwrap()).await.is_err());
}

#[test]
fn a_reader_lease_must_exceed_its_margin() {
    let config = |lease, margin| GarbageConfig { reader_lease: Duration::from_secs(lease), reader_lease_margin: Duration::from_secs(margin), ..GarbageConfig::default() };
    let store = Arc::new(MemoryStore::default());
    let state = Arc::new(PartitionState::empty(TrieConfig::default()).unwrap());
    let table = TableCatalog::default().with_seeded_table(TableRef::new("public", "docs"), Vec::new()).unwrap();
    let entry = table.entry(&TableRef::new("public", "docs")).unwrap().clone();
    let publisher = Arc::new(DeletedPublisher { store: store.clone(), table: entry, commits: Mutex::new(Vec::new()) });
    for (lease, margin, valid) in [(60, 59, true), (60, 60, false), (0, 0, true), (0, 1, false)] {
        let built = TrieCollector::new(store.clone(), state.clone(), publisher.clone(), Arc::new(Observer::default()), config(lease, margin), fixed_clock());
        assert_eq!(built.is_ok(), valid, "lease {lease}s, margin {margin}s");
    }
}
