use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, atomic::{AtomicBool, AtomicI64, Ordering}};
use std::time::Duration;
use async_trait::async_trait;
use bytes::Bytes;
use rtdb_format::{block::proto as pb, catalog::BlockCatalog, cow_tree::RootRef, keys::{TRIE_RETIREMENT_DIRECTORY, TrieKey, block_key, oversized_key},
    log::proto::{ArtefactsAdded, ArtefactsRemoved, EntityDirectoryChange, EntityDirectoryRoot}};
use rtdb_index::{CatalogTrie, PartitionState, PartitionValue, TableCatalog, TrieCatalog, TrieState};
use rtdb_maintenance::{ArtefactPublisher, BlockCollector, CollectionObserver, CycleWorker, GarbageConfig, TableRetirements, TrieCollector, TrieDeletionPublisher, block_is_garbage};
use rtdb_storage::{LocalStore, MemoryStore, ObjectKey, ObjectStore};
use rtdb_trie::TrieConfig;
use rtdb_types::{Anomaly, BlockIndex, CatalogCut, ReplicaPosition, Result, PhysicalObjectVersion, S3ObjectNamespace, SourcePosition, StorageEpoch, StorageVersion, TableRef, Timestamp};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Observer(Mutex<Vec<Anomaly>>);
impl CollectionObserver for Observer { fn table_failed(&self, _: Option<&TableRef>, error: &Anomaly) { self.0.lock().unwrap().push(error.clone()); } }

/// No reader on another node holds a lease, so collection need not wait.
fn unleased() -> GarbageConfig { GarbageConfig { reader_lease: Duration::ZERO, reader_lease_margin: Duration::ZERO, ..GarbageConfig::default() } }
fn fixed_clock() -> rtdb_maintenance::CollectorClock { Arc::new(|| 0) }

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
async fn block_gc_collects_old_metadata_but_preserves_offloaded_replica_payloads() {
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
    for index in [6, 9] {
        let payload = ObjectKey::new(oversized_key(BlockIndex(index), "payload").unwrap()).unwrap();
        assert_eq!(store.get(&payload).await.unwrap(), Bytes::from_static(b"payload"));
    }
    assert!(observer.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn block_gc_queues_only_age_eligible_payloads_when_the_store_opts_in() {
    let store = Arc::new(ObservedStore {
        inner: Arc::new(MemoryStore::default()),
        versions: BTreeMap::new(),
        queued_payloads: Some(Mutex::default()),
    });
    let old = ObjectKey::new(oversized_key(BlockIndex(7), "old").unwrap()).unwrap();
    let young = ObjectKey::new(oversized_key(BlockIndex(8), "young").unwrap()).unwrap();
    for key in [&old, &young] {
        store.put(key, Bytes::from_static(b"payload")).await.unwrap();
    }
    let tables = TableCatalog::load(Some(block(10)), &BTreeMap::new()).unwrap();
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(TrieCatalog::default(), tables, TrieConfig::default()).unwrap()));
    let observer = Arc::new(Observer::default());
    let config = GarbageConfig { blocks_to_keep: 3, ..Default::default() };

    BlockCollector::new(store.clone(), state, observer.clone(), config).unwrap()
        .collect(&CancellationToken::new()).await.unwrap();

    assert_eq!(*store.queued_payloads.as_ref().unwrap().lock().unwrap(), [old.clone()]);
    for key in [old, young] {
        assert_eq!(store.get(&key).await.unwrap(), Bytes::from_static(b"payload"));
    }
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
        catalog = catalog.with_tries(table.clone(), [CatalogTrie { key: key.parse().unwrap(), data_file_size: 1, metadata: None, state: TrieState::Live, artefacts: Vec::new(), published_files: None }], Timestamp(100)).unwrap();
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
    let l0: TrieKey = "l00-rc-b00".parse().unwrap();
    let sealed = ObjectKey::new(entry.slug.artefact_key(&l0, "search", "body~a1f15", 1).unwrap()).unwrap();
    store.put(&sealed, Bytes::from_static(b"sealed search")).await.unwrap();
    let catalog = catalog.with_artefacts(&table, [(l0, rtdb_format::log::proto::ArtefactRecord {
        kind: Some("search".into()), definition_id: Some("body~a1f15".into()), definition_version: Some(1), ..Default::default()
    })]);
    let publisher = Arc::new(DeletedPublisher { store: store.clone(), table: entry.clone(), commits: Mutex::new(Vec::new()) });
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap()));
    let collector = TrieCollector::new(store.clone(), state, publisher.clone(), Arc::new(Observer::default()), unleased(), fixed_clock()).unwrap();
    collector.collect_as_of(Timestamp(100), &CancellationToken::new()).await.unwrap();
    assert_eq!(*publisher.commits.lock().unwrap(), vec![vec!["l01-rc-b00".parse::<TrieKey>().unwrap()]]);
    assert!(store.head(&ObjectKey::new(entry.slug.data_key(&"l00-rc-b00".parse().unwrap())).unwrap()).await.is_ok());
    assert!(store.head(&sealed).await.is_err());
    assert!(store.head(&live_artefact).await.is_ok(), "a live trie's artefacts stay");
    collector.collect_as_of(Timestamp(100), &CancellationToken::new()).await.unwrap();
    assert_eq!(publisher.commits.lock().unwrap().len(), 2);
}

struct DueRetirement { state: Arc<PartitionState>, table: TableRef, retired: Mutex<Vec<(Vec<TableRef>, Timestamp)>> }
#[async_trait]
impl TableRetirements for DueRetirement {
    async fn due(&self, _: &CancellationToken) -> Result<Option<(Vec<TableRef>, Timestamp)>> {
        Ok(self.state.value().trie_catalog().retired_as_of(&self.table).is_none().then(|| (vec![self.table.clone()], Timestamp(50))))
    }
    async fn retire(&self, tables: Vec<TableRef>, as_of: Timestamp) -> Result<()> {
        self.retired.lock().unwrap().push((tables.clone(), as_of));
        self.state.retire_tables(tables, as_of);
        Ok(())
    }
}

#[tokio::test]
async fn a_retired_table_loses_its_compacted_tries_through_the_ordinary_deletion_and_keeps_l0_recovery() {
    let table = TableRef::new("public", "retired");
    let tables = TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let entry = tables.entry(&table).unwrap().clone();
    let mut catalog = TrieCatalog::new(16).unwrap();
    for key in ["l00-rc-b00", "l01-rc-b00"] {
        catalog = catalog.with_tries(table.clone(), [CatalogTrie { key: key.parse().unwrap(), data_file_size: 1, metadata: None, state: TrieState::Live, artefacts: Vec::new(), published_files: None }], Timestamp(10)).unwrap();
    }
    let store = Arc::new(MemoryStore::default());
    for key in ["l00-rc-b00", "l01-rc-b00"] {
        let key: TrieKey = key.parse().unwrap();
        store.put(&ObjectKey::new(entry.slug.data_key(&key)).unwrap(), Bytes::from_static(b"data")).await.unwrap();
        store.put(&ObjectKey::new(entry.slug.metadata_key(&key)).unwrap(), Bytes::from_static(b"metadata")).await.unwrap();
    }
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap()));
    let publisher = Arc::new(DeletedPublisher { store: store.clone(), table: entry.clone(), commits: Mutex::new(Vec::new()) });
    let retirements = Arc::new(DueRetirement { state: state.clone(), table: table.clone(), retired: Mutex::new(Vec::new()) });
    let observer = Arc::new(Observer::default());
    let collector = TrieCollector::new(store.clone(), state.clone(), publisher.clone(), observer.clone(), unleased(), fixed_clock()).unwrap()
        .with_retirements(retirements.clone());

    collector.collect_as_of(Timestamp(100), &CancellationToken::new()).await.unwrap();
    assert_eq!(*retirements.retired.lock().unwrap(), vec![(vec![table.clone()], Timestamp(50))]);
    assert!(publisher.commits.lock().unwrap().is_empty(), "the cycle that retires collects from the catalog it captured before");
    assert!(state.value().trie_catalog().current_tries(&table).is_empty());

    collector.collect_as_of(Timestamp(100), &CancellationToken::new()).await.unwrap();
    assert_eq!(*publisher.commits.lock().unwrap(), vec![vec!["l01-rc-b00".parse::<TrieKey>().unwrap()]]);
    assert_eq!(retirements.retired.lock().unwrap().len(), 1);
    assert!(store.head(&ObjectKey::new(entry.slug.data_key(&"l00-rc-b00".parse().unwrap())).unwrap()).await.is_ok());
    assert!(observer.0.lock().unwrap().is_empty());
}

#[derive(Clone, Copy)]
enum Removal { StopBeforeInstall, StopAfterInstall, Complete }

struct InstallingPublisher {
    phase: Removal,
    state: Arc<PartitionState>,
    scope: CatalogCut,
    store: Arc<LocalStore>,
    pending: ObjectKey,
    requests: Mutex<Vec<Vec<TrieKey>>>,
}

#[async_trait]
impl TrieDeletionPublisher for InstallingPublisher {
    async fn publish(&self, table: TableRef, keys: Vec<TrieKey>) -> Result<ReplicaPosition> {
        if !matches!(self.phase, Removal::Complete) {
            assert_eq!(self.store.list(Some(&self.pending)).await.unwrap().len(), 1, "retirement intent was not durable before publication");
        }
        self.requests.lock().unwrap().push(keys.clone());
        if !matches!(self.phase, Removal::StopBeforeInstall) {
            let previous = self.state.value().snapshot().catalog_cut().unwrap();
            self.state.delete_tries(&BTreeMap::from([(table, keys.into_iter().collect())]));
            let through = ReplicaPosition(previous.through.0 + 1);
            self.state.publish_catalog_cut(CatalogCut { through, ..self.scope }).unwrap();
            if matches!(self.phase, Removal::Complete) { return Ok(through); }
        }
        Err(Anomaly::fault("test/stopped", "The writer stopped before persisting its receipt"))
    }
}

struct CatalogFloorStore {
    inner: Arc<LocalStore>,
    blocked: AtomicBool,
    proposals: Mutex<Vec<CatalogCut>>,
    fail_release: Mutex<Option<ObjectKey>>,
}

#[async_trait]
impl ObjectStore for CatalogFloorStore {
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<rtdb_storage::object_store::StoredObject> { self.inner.head(key).await }
    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.inner.get(key).await }
    async fn get_range(&self, key: &ObjectKey, range: std::ops::Range<u64>) -> rtdb_storage::Result<Bytes> { self.inner.get_range(key, range).await }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> { self.inner.put(key, bytes).await }
    async fn put_file(&self, key: &ObjectKey, path: &std::path::Path) -> rtdb_storage::Result<()> { self.inner.put_file(key, path).await }
    async fn list(&self, prefix: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<rtdb_storage::object_store::StoredObject>> { self.inner.list(prefix).await }
    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        {
            let mut fail = self.fail_release.lock().unwrap();
            if fail.as_ref() == Some(key) {
                *fail = None;
                return Err(rtdb_storage::Error::Task("simulated release failure".into()));
            }
        }
        self.inner.delete(key).await
    }
    fn defers_trie_releases(&self) -> bool { true }
    async fn advance_catalog_floor(&self, installed: CatalogCut) -> Result<bool> {
        self.proposals.lock().unwrap().push(installed);
        Ok(!self.blocked.load(Ordering::Relaxed))
    }
    async fn release_trie_files(&self, receipt: &ObjectKey, contents: Bytes) -> Result<bool> {
        let installed = rtdb_format::trie_retirement::InstalledRetirement::decode(receipt.as_str(), &contents)?;
        if !self.advance_catalog_floor(installed.cut()).await? { return Ok(false); }
        let mut files: Vec<_> = installed.files().iter().collect();
        files.sort_by_key(|file| file.release_order());
        for file in files { self.delete(&ObjectKey::new(&file.key)?).await?; }
        Ok(true)
    }
}

#[tokio::test]
async fn durable_retirement_intents_survive_writer_stop_on_both_sides_of_installation() {
    for failed in [Removal::StopBeforeInstall, Removal::StopAfterInstall] {
        let table = TableRef::new("public", "docs");
        let tables = TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
        let tables_for_footprint = tables.clone();
        let old: TrieKey = "l01-rc-b00".parse().unwrap();
        let newer: TrieKey = "l01-rc-b01".parse().unwrap();
        let catalog = TrieCatalog::new(16).unwrap().with_tries(table.clone(), [old.clone(), newer].into_iter().map(|key| {
            CatalogTrie::notification(key, 1, None).unwrap()
        }), Timestamp(100)).unwrap();
        let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap()));
        let scope = CatalogCut { storage_version: StorageVersion(6), storage_epoch: StorageEpoch(0), replica_epoch: StorageEpoch(0), through: ReplicaPosition(0) };
        state.publish_catalog_cut(scope).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let store = Arc::new(LocalStore::open(directory.path()).unwrap());
        let pending = ObjectKey::new(format!("{TRIE_RETIREMENT_DIRECTORY}/6-0-0/pending")).unwrap();
        let installed = ObjectKey::new(format!("{TRIE_RETIREMENT_DIRECTORY}/6-0-0/installed")).unwrap();
        let publisher = |phase, store: Arc<LocalStore>| Arc::new(InstallingPublisher {
            phase, state: state.clone(), scope, store, pending: pending.clone(), requests: Mutex::new(Vec::new()),
        });
        let collector = |store: Arc<dyn ObjectStore>, publisher: Arc<InstallingPublisher>, observer: Arc<Observer>| {
            TrieCollector::new(store, state.clone(), publisher, observer, unleased(), fixed_clock()).unwrap()
        };
        let slug = &tables_for_footprint.entry(&table).unwrap().slug;
        let data = ObjectKey::new(slug.data_key(&old)).unwrap();
        let metadata = ObjectKey::new(slug.metadata_key(&old)).unwrap();
        let artefact = ObjectKey::new(slug.artefact_key(&old, "search", "body", 1).unwrap()).unwrap();
        for key in [&artefact, &data, &metadata] { store.put(key, Bytes::from_static(b"trie file")).await.unwrap(); }
        let coordination = Arc::new(CatalogFloorStore {
            inner: store.clone(), blocked: AtomicBool::new(true),
            proposals: Mutex::new(Vec::new()), fail_release: Mutex::new(None),
        });
        let owner = publisher(failed, store.clone());
        let observer = Arc::new(Observer::default());
        let first = collector(coordination.clone(), owner.clone(), observer.clone());
        let cancellation = CancellationToken::new();
        if matches!(failed, Removal::StopBeforeInstall) {
            let malformed = ObjectKey::new(format!("{pending}/malformed")).unwrap();
            store.put(&malformed, Bytes::from_static(b"{")).await.unwrap();
            first.collect_as_of(Timestamp(100), &cancellation).await.unwrap();
            assert!(owner.requests.lock().unwrap().is_empty());
            let error = observer.0.lock().unwrap().pop().unwrap();
            assert_eq!(error.code, "rtdb.maintenance/invalid-state");
            store.delete(&malformed).await.unwrap();
        }
        first.collect_as_of(Timestamp(100), &cancellation).await.unwrap();
        drop(first);
        assert_eq!(*owner.requests.lock().unwrap(), [vec![old.clone()]]);
        assert_eq!(observer.0.lock().unwrap()[0].code, "test/stopped");
        let staged = store.list(Some(&pending)).await.unwrap();
        assert_eq!(staged.len(), 1);
        let staged_bytes = store.get(&staged[0].key).await.unwrap();
        let encoded: serde_json::Value = serde_json::from_slice(&staged_bytes).unwrap();
        assert_eq!(encoded["files"], serde_json::json!([
            {"key": artefact.as_str()},
            {"key": slug.data_key(&old)},
            {"key": slug.metadata_key(&old)},
        ]), "the staged intent must include the file plan");
        assert!(store.list(Some(&installed)).await.unwrap().is_empty());
        for key in [&artefact, &data, &metadata] {
            assert!(store.head(key).await.is_ok(), "a hosted file cannot be released before its receipt");
        }
        drop(coordination);
        drop(owner);
        drop(store);

        let store = Arc::new(LocalStore::open(directory.path()).unwrap());
        let replacement = publisher(Removal::Complete, store.clone());
        let coordination = Arc::new(CatalogFloorStore {
            inner: store.clone(), blocked: AtomicBool::new(true),
            proposals: Mutex::new(Vec::new()), fail_release: Mutex::new(None),
        });
        let next = collector(coordination.clone(), replacement.clone(), Arc::new(Observer::default()));
        next.collect_as_of(Timestamp(100), &cancellation).await.unwrap();
        assert_eq!(*replacement.requests.lock().unwrap(), [vec![old.clone()]], "the successor must confirm the staged removal");
        assert!(!state.value().retains_trie(&table, &old));
        assert_eq!(store.list(Some(&pending)).await.unwrap().len(), 1, "a hold blocking the floor keeps the intent durable");
        for key in [&artefact, &data, &metadata] {
            assert!(store.head(key).await.is_ok(), "a hosted file cannot be released below an admitted hold");
        }
        let receipts = store.list(Some(&installed)).await.unwrap();
        assert_eq!(receipts.len(), 1);
        let receipt = store.get(&receipts[0].key).await.unwrap();
        assert_eq!(&receipt[8..], staged_bytes.as_ref(), "the receipt retains the accepted intent");
        let installed_cut = state.value().snapshot().catalog_cut().unwrap();
        assert_eq!(*coordination.proposals.lock().unwrap(), [installed_cut]);

        // A successor can retry floor admission from the receipt without submitting another removal.
        drop(next);
        coordination.blocked.store(false, Ordering::Relaxed);
        *coordination.fail_release.lock().unwrap() = Some(metadata.clone());
        let observer = Arc::new(Observer::default());
        let next = collector(coordination.clone(), replacement.clone(), observer.clone());
        next.collect_as_of(Timestamp(100), &cancellation).await.unwrap();
        assert_eq!(store.list(Some(&pending)).await.unwrap().len(), 1, "a failed hosted release keeps the file plan durable");
        assert!(store.head(&metadata).await.is_ok());
        for key in [&artefact, &data] {
            assert!(matches!(store.head(key).await, Err(rtdb_storage::Error::MissingObject(_))),
                "artefact and data releases precede metadata");
        }
        assert_eq!(observer.0.lock().unwrap().len(), 1);
        drop(next);

        let next = collector(coordination.clone(), replacement.clone(), Arc::new(Observer::default()));
        next.collect_as_of(Timestamp(100), &cancellation).await.unwrap();
        assert_eq!(*coordination.proposals.lock().unwrap(), [installed_cut; 3]);
        assert!(store.list(Some(&pending)).await.unwrap().is_empty());
        for key in [&artefact, &data, &metadata] {
            assert!(matches!(store.head(key).await, Err(rtdb_storage::Error::MissingObject(_))));
        }

        store.put(&staged[0].key, staged_bytes).await.unwrap();
        next.collect_as_of(Timestamp(100), &cancellation).await.unwrap();
        assert_eq!(replacement.requests.lock().unwrap().len(), 1, "the durable receipt already covers the batch");
        assert!(store.list(Some(&pending)).await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn local_trie_collection_keeps_the_window_and_never_waits_for_a_reader() {
    let table = TableRef::new("public", "docs");
    let tables = TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let obsolete: TrieKey = "l01-rc-b00".parse().unwrap();
    let slug = &tables.entry(&table).unwrap().slug;
    let data = ObjectKey::new(slug.data_key(&obsolete)).unwrap();
    let metadata = ObjectKey::new(slug.metadata_key(&obsolete)).unwrap();
    let catalog = TrieCatalog::new(16).unwrap().with_tries(table.clone(),
        [obsolete.clone(), "l01-rc-b01".parse().unwrap()].into_iter()
            .map(|key| CatalogTrie::notification(key, 1, None).unwrap()), Timestamp(100)).unwrap();
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap()));
    let scope = CatalogCut {
        storage_version: StorageVersion(6), storage_epoch: StorageEpoch(0),
        replica_epoch: StorageEpoch(0), through: ReplicaPosition(0),
    };
    state.publish_catalog_cut(scope).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let open = || Arc::new(LocalStore::open(directory.path()).unwrap().with_catalog_ledger(StorageEpoch(0)));
    let reader = open();
    // A reader of the catalog that still names the obsolete trie registers nothing with storage.
    let captured = state.snapshot();
    let writer = open();
    for key in [&data, &metadata] { writer.put(key, Bytes::from_static(b"trie file")).await.unwrap(); }
    let pending = ObjectKey::new(format!("{TRIE_RETIREMENT_DIRECTORY}/6-0-0/pending")).unwrap();
    let publisher = Arc::new(InstallingPublisher {
        phase: Removal::Complete, state: state.clone(), scope, store: writer.clone(),
        pending: pending.clone(), requests: Mutex::new(Vec::new()),
    });
    let observer = Arc::new(Observer::default());
    let collect = |store: Arc<LocalStore>| TrieCollector::new(store, state.clone(), publisher.clone(),
        observer.clone(), unleased(), fixed_clock()).unwrap();
    let cancellation = CancellationToken::new();
    collect(writer.clone()).collect_as_of(Timestamp(99), &cancellation).await.unwrap();
    assert!(state.value().retains_trie(&table, &obsolete), "a trie superseded inside the garbage window is kept");
    assert!(publisher.requests.lock().unwrap().is_empty());
    for key in [&data, &metadata] { assert!(reader.get(key).await.is_ok()); }

    collect(writer.clone()).collect_as_of(Timestamp(100), &cancellation).await.unwrap();
    assert!(!state.value().retains_trie(&table, &obsolete));
    // A successor resumes any installed batch the first cycle left; nothing a reader holds can defer it.
    let successor = collect(open());
    successor.collect_as_of(Timestamp(100), &cancellation).await.unwrap();
    assert_eq!(*publisher.requests.lock().unwrap(), [vec![obsolete]], "the removal is published once");
    assert!(writer.list(Some(&pending)).await.unwrap().is_empty(), "the installed batch is released");
    for key in [&data, &metadata] {
        assert!(matches!(reader.get(key).await, Err(rtdb_storage::Error::MissingObject(_))),
            "a reader of a catalog older than the window does not hold its files");
    }
    assert!(captured.catalog_cut().is_some());
    assert!(observer.0.lock().unwrap().is_empty());
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
async fn collection_keeps_the_garbage_window_and_ignores_open_readers() {
    let table = TableRef::new("public", "docs");
    let tables = TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let entry = tables.entry(&table).unwrap().clone();
    let trie = |key: &str| CatalogTrie {
        key: key.parse().unwrap(), data_file_size: 1, metadata: None, state: TrieState::Live, artefacts: Vec::new(), published_files: None,
    };
    let superseded = trie("l01-rc-b00");
    let unreferenced = trie("l01-rc-b01");
    let catalog = TrieCatalog::new(16).unwrap().with_tries(table.clone(), [superseded.clone()], Timestamp(100)).unwrap();
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap()));
    let reader = state.open_snapshot(None);
    state.add_tries(&BTreeMap::from([(table.clone(), vec![unreferenced.clone(), trie("l01-rc-b02")])]), Timestamp(100)).unwrap();
    assert_eq!(reader.trie_entries(&table)[0].key, superseded.key);

    let store = Arc::new(MemoryStore::default());
    let mut objects = Vec::new();
    for candidate in [&superseded, &unreferenced] {
        for path in [
            entry.slug.data_key(&candidate.key),
            entry.slug.metadata_key(&candidate.key),
            entry.slug.artefact_key(&candidate.key, "search", "body", 1).unwrap(),
        ] {
            let key = ObjectKey::new(path).unwrap();
            store.put(&key, Bytes::from_static(b"contents")).await.unwrap();
            objects.push(key);
        }
    }
    let publisher = Arc::new(DeletedPublisher { store: store.clone(), table: entry, commits: Mutex::new(Vec::new()) });
    let collector = TrieCollector::new(store.clone(), state.clone(), publisher.clone(), Arc::new(Observer::default()), unleased(), fixed_clock()).unwrap();
    let cancellation = CancellationToken::new();
    collector.collect_as_of(Timestamp(99), &cancellation).await.unwrap();
    assert!(publisher.commits.lock().unwrap().is_empty(), "a trie superseded after the garbage-as-of horizon is kept");
    for key in &objects { assert!(store.get(key).await.is_ok()); }

    collector.collect_as_of(Timestamp(100), &cancellation).await.unwrap();
    let committed: std::collections::BTreeSet<_> = publisher.commits.lock().unwrap().iter().flatten().cloned().collect();
    assert_eq!(committed, std::collections::BTreeSet::from([superseded.key.clone(), unreferenced.key.clone()]),
        "an open reader of the superseded catalog never blocks collection past the horizon");
    for key in &objects { assert!(store.head(key).await.is_err()); }
    drop(reader);
}

struct ObservedStore {
    inner: Arc<MemoryStore>,
    versions: BTreeMap<ObjectKey, PhysicalObjectVersion>,
    queued_payloads: Option<Mutex<Vec<ObjectKey>>>,
}

fn provider_version(key: &ObjectKey, version: &str) -> PhysicalObjectVersion {
    PhysicalObjectVersion {
        namespace: S3ObjectNamespace { bucket: "test-cell".into(), endpoint: None, region: Some("us-east-1".into()) },
        key: format!("test-residence/{key}"),
        version: version.into(),
    }
}

#[async_trait]
impl ObjectStore for ObservedStore {
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<rtdb_storage::object_store::StoredObject> { self.inner.head(key).await }
    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.inner.get(key).await }
    async fn get_range(&self, key: &ObjectKey, range: std::ops::Range<u64>) -> rtdb_storage::Result<Bytes> { self.inner.get_range(key, range).await }
    async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> { self.inner.put(key, bytes).await }
    async fn put_file(&self, key: &ObjectKey, path: &std::path::Path) -> rtdb_storage::Result<()> { self.inner.put_file(key, path).await }
    async fn list(&self, prefix: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<rtdb_storage::object_store::StoredObject>> { self.inner.list(prefix).await }
    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.inner.delete(key).await }
    fn queues_offloaded_payload_reclamation(&self) -> bool { self.queued_payloads.is_some() }
    async fn collect_recovery_file(&self, key: &ObjectKey) -> Result<()> {
        if let Some(queued) = &self.queued_payloads && key.as_str().starts_with("oversized/") {
            queued.lock().unwrap().push(key.clone());
            return Ok(());
        }
        Ok(self.delete(key).await?)
    }
    async fn retirement_version(&self, key: &ObjectKey) -> rtdb_storage::Result<Option<PhysicalObjectVersion>> {
        let Some(version) = self.versions.get(key) else { return Ok(None); };
        self.inner.head(key).await?;
        Ok(Some(version.clone()))
    }
}

#[tokio::test]
async fn retirement_binds_only_the_same_version_the_catalog_published() {
    use rtdb_format::log::proto::TrieFileVersions;

    let table = TableRef::new("public", "docs");
    let tables = TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let entry = tables.entry(&table).unwrap().clone();
    let mut old = CatalogTrie::notification("l01-rc-b00".parse().unwrap(), 1, None).unwrap();
    let data = ObjectKey::new(entry.slug.data_key(&old.key)).unwrap();
    let metadata = ObjectKey::new(entry.slug.metadata_key(&old.key)).unwrap();
    let published_data = provider_version(&data, "data-version");
    let published_meta = provider_version(&metadata, "metadata-version");
    old.published_files = Some(TrieFileVersions {
        data: Some(published_data.clone().into()),
        metadata: Some(published_meta.into()),
    });
    let restored = CatalogTrie::from_proto(&old.to_proto(&table)).unwrap();
    assert_eq!(restored.published_files, old.published_files);
    let next = CatalogTrie::notification("l01-rc-b01".parse().unwrap(), 1, None).unwrap();
    let catalog = TrieCatalog::new(16).unwrap().with_tries(table, [restored, next], Timestamp(100)).unwrap();
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap()));
    state.publish_catalog_cut(CatalogCut { storage_version: StorageVersion(6), storage_epoch: StorageEpoch(0),
        replica_epoch: StorageEpoch(0), through: ReplicaPosition(0) }).unwrap();
    let store = Arc::new(MemoryStore::default());
    for key in [&data, &metadata] { store.put(key, Bytes::from_static(b"published")).await.unwrap(); }
    let observing = Arc::new(ObservedStore { inner: store.clone(), queued_payloads: None, versions: BTreeMap::from([
        (data.clone(), published_data), (metadata.clone(), provider_version(&metadata, "replacement-version")),
    ]) });
    let publisher = Arc::new(DeletedPublisher { store: store.clone(), table: entry, commits: Mutex::new(Vec::new()) });
    let collector = TrieCollector::new(observing, state, publisher, Arc::new(Observer::default()), unleased(), fixed_clock()).unwrap();
    collector.collect_as_of(Timestamp(100), &CancellationToken::new()).await.unwrap();
    let installed = ObjectKey::new(format!("{TRIE_RETIREMENT_DIRECTORY}/6-0-0/installed")).unwrap();
    let receipts = store.list(Some(&installed)).await.unwrap();
    assert_eq!(receipts.len(), 1);
    let bytes = store.get(&receipts[0].key).await.unwrap();
    let intent: serde_json::Value = serde_json::from_slice(&bytes[8..]).unwrap();
    assert_eq!(intent["files"][0]["key"], data.as_str());
    assert_eq!(intent["files"][0]["matches_publication"], true);
    assert_eq!(intent["files"][1]["key"], metadata.as_str());
    assert_eq!(intent["files"][1]["version"]["version"], "replacement-version");
    assert!(intent["files"][1].get("matches_publication").is_none(), "an observed replacement cannot stand in for the catalog's publication");
}

#[tokio::test]
async fn shared_artefacts_go_only_with_the_last_member_the_catalog_holds() {
    let table = TableRef::new("public", "docs");
    let tables = TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let entry = tables.entry(&table).unwrap().clone();
    let add = |catalog: TrieCatalog, key: &str, at: i64| catalog.with_tries(table.clone(), [CatalogTrie { key: key.parse().unwrap(), data_file_size: 1, metadata: None, state: TrieState::Live, artefacts: Vec::new(), published_files: None }], Timestamp(at)).unwrap();
    let mut catalog = TrieCatalog::new(16).unwrap();
    for bucket in 0..4 { catalog = add(catalog, &format!("l02-rc-p{bucket}-b00"), 100); }
    let supersede = |mut catalog: TrieCatalog, part: u8| { for bucket in 0..4 { catalog = add(catalog, &format!("l03-rc-p{part}{bucket}-b01"), 100 + i64::from(part)); } catalog };
    let group = "l02-rc-p0-b00".parse::<TrieKey>().unwrap().part_group().unwrap();
    let object = |definition| ObjectKey::new(entry.slug.group_artefact_prefix(&group) + &format!("/search/{definition}-v1.bin")).unwrap();
    let (agreed, mixed, legacy) = (object("body"), object("mixed"), object("legacy"));
    let store = Arc::new(MemoryStore::default());
    let members = group.members();
    let pack = rtdb_format::keys::ArtefactOwner::Pack(
        rtdb_format::keys::ArtefactPack::new(vec![members[0].clone(), members[1].clone()]).unwrap());
    let packed = ObjectKey::new(entry.slug.owner_artefact_key(&pack, "search", "packed", 1).unwrap()).unwrap();
    store.put(&packed, Bytes::from_static(b"shared pack")).await.unwrap();
    catalog = catalog.with_artefacts(&table, pack.tries().into_iter().map(|trie| (trie,
        rtdb_format::log::proto::ArtefactRecord {
            kind: Some("search".into()), definition_id: Some("packed".into()), definition_version: Some(1),
            scope: Some(rtdb_format::log::proto::ArtefactScope::Pack as i32),
            pack_members: pack.tries().iter().map(ToString::to_string).collect(), ..Default::default()
        })));
    for key in [&agreed, &mixed, &legacy] {
        store.put(key, Bytes::from_static(b"artefact")).await.unwrap();
    }
    for (member, trie) in group.members().into_iter().enumerate() {
        use rtdb_format::log::proto::{ArtefactRecord, ArtefactScope};
        let records = [("body", &agreed), ("mixed", &mixed), ("legacy", &legacy)].map(|(name, key)| {
            let version = if name == "mixed" && member == 2 { "stale-file-version" } else { "group-file-version" };
            (trie.clone(), ArtefactRecord {
                kind: Some("search".into()),
                definition_id: Some(name.into()),
                definition_version: Some(1),
                scope: Some(ArtefactScope::PartGroup as i32),
                published_version: (name != "legacy" || member != 3).then(|| provider_version(key, version).into()),
                ..Default::default()
            })
        });
        catalog = catalog.with_artefacts(&table, records);
    }
    let observing = Arc::new(ObservedStore {
        inner: store.clone(),
        queued_payloads: None,
        versions: [&agreed, &mixed, &legacy].into_iter()
            .map(|key| (key.clone(), provider_version(key, "group-file-version"))).collect(),
    });
    let collect = |catalog: TrieCatalog| {
        let (store, observing, tables, entry) = (store.clone(), observing.clone(), tables.clone(), entry.clone());
        async move {
            let publisher = Arc::new(DeletedPublisher { store, table: entry, commits: Mutex::new(Vec::new()) });
            let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap()));
            let cut = CatalogCut { storage_version: StorageVersion(6), storage_epoch: StorageEpoch(0), replica_epoch: StorageEpoch(0), through: ReplicaPosition(0) };
            state.publish_catalog_cut(cut).unwrap();
            let config = GarbageConfig { chunk_size: 1, ..unleased() };
            let observer = Arc::new(Observer::default());
            let collector = TrieCollector::new(observing, state, publisher.clone(), observer.clone(), config, fixed_clock()).unwrap();
            collector.collect_as_of(Timestamp(200), &CancellationToken::new()).await.unwrap();
            assert!(observer.0.lock().unwrap().is_empty());
            publisher.commits.lock().unwrap().concat()
        }
    };
    let receipts = async || {
        let installed = ObjectKey::new(format!("{TRIE_RETIREMENT_DIRECTORY}/6-0-0/installed")).unwrap();
        let mut recorded = BTreeMap::new();
        for receipt in store.list(Some(&installed)).await.unwrap() {
            let bytes = store.get(&receipt.key).await.unwrap();
            let intent: serde_json::Value = serde_json::from_slice(&bytes[8..]).unwrap();
            for file in intent["files"].as_array().unwrap() {
                let key = file["key"].as_str().unwrap();
                if [&agreed, &mixed, &legacy].iter().any(|object| object.as_str() == key) {
                    assert!(recorded.insert(key.to_owned(), (intent["tries"].clone(), file.clone())).is_none());
                }
            }
        }
        recorded
    };
    let one_superseded = supersede(catalog.clone(), 0);
    assert_eq!(collect(one_superseded.clone()).await, ["l02-rc-p0-b00".parse::<TrieKey>().unwrap()]);
    assert!(store.head(&agreed).await.is_ok(), "three members are still live");
    assert!(store.head(&packed).await.is_ok());
    assert!(receipts().await.is_empty());
    let collected = one_superseded.without_tries(&table, &["l02-rc-p0-b00".parse().unwrap()].into());
    let all_superseded = (1..4).fold(collected, supersede);
    let retired = collect(all_superseded).await;
    assert_eq!(retired.len(), 3);
    assert!(store.head(&packed).await.is_err());
    let recorded = receipts().await;
    assert_eq!(recorded.len(), 3);
    for key in [&agreed, &mixed, &legacy] {
        assert!(store.head(key).await.is_err(), "the last members take the group's artefacts with them");
        let (tries, file) = &recorded[key.as_str()];
        assert_eq!(*tries, serde_json::json!([retired.last().unwrap().to_string()]));
        assert_eq!(file["version"]["key"], format!("test-residence/{key}"));
        assert_eq!(file["version"]["version"], "group-file-version");
    }
    assert_eq!(recorded[agreed.as_str()].1["matches_publication"], true);
    assert!(recorded[mixed.as_str()].1.get("matches_publication").is_none(), "differing member publications cannot bind the group file");
    assert!(recorded[legacy.as_str()].1.get("matches_publication").is_none(), "a member without a publication cannot bind the group file");
}

#[tokio::test]
async fn a_replacement_collector_waits_a_whole_lease_on_its_own_clock() {
    let table = TableRef::new("public", "docs");
    let tables = TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let entry = tables.entry(&table).unwrap().clone();
    let mut catalog = TrieCatalog::new(16).unwrap();
    for key in ["l00-rc-b00", "l01-rc-b00", "l01-rc-b01"] {
        catalog = catalog.with_tries(table.clone(), [CatalogTrie { key: key.parse().unwrap(), data_file_size: 1, metadata: None, state: TrieState::Live, artefacts: Vec::new(), published_files: None }], Timestamp(100)).unwrap();
    }
    let store = Arc::new(MemoryStore::default());
    let garbage: TrieKey = "l01-rc-b00".parse().unwrap();
    let data = ObjectKey::new(entry.slug.data_key(&garbage)).unwrap();
    let metadata = ObjectKey::new(entry.slug.metadata_key(&garbage)).unwrap();
    store.put(&data, Bytes::from_static(b"data")).await.unwrap();
    store.put(&metadata, Bytes::from_static(b"metadata")).await.unwrap();
    let publisher = Arc::new(DeletedPublisher { store: store.clone(), table: entry.clone(), commits: Mutex::new(Vec::new()) });
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap()));
    let config = GarbageConfig { reader_lease: Duration::from_micros(500), reader_lease_margin: Duration::from_micros(100), ..GarbageConfig::default() };
    let cancellation = CancellationToken::new();
    let predecessor = TrieCollector::new(store.clone(), state.clone(), publisher.clone(), Arc::new(Observer::default()), config.clone(), Arc::new(|| 1_000)).unwrap();
    predecessor.collect_as_of(Timestamp(100), &cancellation).await.unwrap();
    assert!(publisher.commits.lock().unwrap().is_empty());
    drop(predecessor);

    let now = Arc::new(AtomicI64::new(9_000));
    let clock = { let now = Arc::clone(&now); Arc::new(move || now.load(Ordering::Relaxed)) };
    let successor = TrieCollector::new(store.clone(), state, publisher.clone(), Arc::new(Observer::default()), config, clock).unwrap();
    for elapsed in [0, 499] {
        now.store(9_000 + elapsed, Ordering::Relaxed);
        successor.collect_as_of(Timestamp(100), &cancellation).await.unwrap();
        assert!(publisher.commits.lock().unwrap().is_empty());
        assert!(store.head(&data).await.is_ok());
        assert!(store.head(&metadata).await.is_ok());
    }

    now.store(9_500, Ordering::Relaxed);
    successor.collect_as_of(Timestamp(100), &cancellation).await.unwrap();
    assert_eq!(*publisher.commits.lock().unwrap(), vec![vec![garbage]],
        "the replacement collector must reclaim at the elapsed lease boundary");
    assert!(store.head(&data).await.is_err());
    assert!(store.head(&metadata).await.is_err());
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

#[tokio::test]
async fn retired_search_bodies_and_catalog_receipts_wait_for_store_holds() {
    use rtdb_format::log::proto::{ArtefactRecord, ArtefactsAdded, ArtefactsRemoved, PublishedObjectVersion, SearchRun, SearchRunChange};
    use rtdb_maintenance::ArtefactPublisher;
    use rtdb_storage::object_store::StoredObject;
    use rtdb_types::SourcePosition;

    struct HeldStore { inner: Arc<MemoryStore>, held: AtomicBool }
    #[async_trait]
    impl ObjectStore for HeldStore {
        async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.inner.head(key).await }
        async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.inner.get(key).await }
        async fn get_range(&self, key: &ObjectKey, range: std::ops::Range<u64>) -> rtdb_storage::Result<Bytes> { self.inner.get_range(key, range).await }
        async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> { self.inner.put(key, bytes).await }
        async fn put_file(&self, key: &ObjectKey, path: &std::path::Path) -> rtdb_storage::Result<()> { self.inner.put_file(key, path).await }
        async fn list(&self, prefix: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> { self.inner.list(prefix).await }
        async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.inner.delete(key).await }
        async fn permits_search_release(&self, _: &ObjectKey, _: Option<&PublishedObjectVersion>, _: Option<CatalogCut>) -> Result<bool> {
            Ok(!self.held.load(Ordering::Relaxed))
        }
    }

    struct SearchRemoval { table: TableRef, state: Arc<PartitionState> }
    #[async_trait]
    impl ArtefactPublisher for SearchRemoval {
        async fn publish(&self, _: ArtefactsAdded) -> Result<SourcePosition> { panic!("no search upload is pending") }
        async fn remove(&self, message: ArtefactsRemoved) -> Result<SourcePosition> {
            self.state.remove_artefacts(&self.table, message.entries.into_iter().map(|entry| {
                (entry.trie_key.unwrap().parse::<TrieKey>().unwrap(), entry.record.unwrap())
            }).collect());
            Ok(SourcePosition(1))
        }
    }

    let table = TableRef::new("public", "docs");
    let tables = TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let entry = tables.entry(&table).unwrap().clone();
    let l0 = TrieKey::l0(BlockIndex(0)).unwrap();
    let run = SearchRun { definition_id: Some("docs_bodyf16".into()), definition_version: Some(1), blocks: vec![0],
        digest: Some(vec![7; 32]), locator: Some(vec![1]), file_size: Some(4), source_rows: Some(1), ..Default::default() };
    let body = ObjectKey::new(run.object_key(&entry.slug).unwrap()).unwrap();
    let seal = ObjectKey::new(entry.slug.artefact_key(&l0, "search", "body~a1f16w", 1).unwrap()).unwrap();
    let seal_record = ArtefactRecord { kind: Some("search".into()), definition_id: Some("body~a1f16w".into()),
        definition_version: Some(1), ..Default::default() };
    let run_record = SearchRunChange { run: Some(run), ..Default::default() }.to_artefact().unwrap();
    let catalog = TrieCatalog::default().with_tries(table.clone(), [CatalogTrie::notification(l0.clone(), 1, None).unwrap()], Timestamp(1)).unwrap()
        .with_artefacts(&table, [(l0.clone(), seal_record), (l0, run_record)])
        .with_retired_tables([table.clone()], Timestamp(2));
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap()));
    let store = Arc::new(HeldStore { inner: Arc::new(MemoryStore::default()), held: AtomicBool::new(true) });
    for key in [&body, &seal] { store.put(key, Bytes::from_static(b"body")).await.unwrap(); }
    let trie_deletion = Arc::new(DeletedPublisher { table: entry, store: store.inner.clone(), commits: Mutex::default() });
    let observer = Arc::new(Observer::default());
    let removal = Arc::new(SearchRemoval { table: table.clone(), state: state.clone() });
    let collector = TrieCollector::new(store.clone(), state.clone(), trie_deletion, observer.clone(), unleased(), fixed_clock()).unwrap()
        .with_search(removal, StorageVersion(0), StorageEpoch(0));
    let stop = CancellationToken::new();
    collector.collect_as_of(Timestamp(3), &stop).await.unwrap();
    for key in [&body, &seal] { assert!(store.head(key).await.is_ok()); }
    assert!(!state.snapshot().trie_catalog().retired_search_runs(&table).next().unwrap().released());

    store.held.store(false, Ordering::Relaxed);
    collector.collect_as_of(Timestamp(3), &stop).await.unwrap();
    for key in [&body, &seal] { assert!(matches!(store.head(key).await, Err(rtdb_storage::Error::MissingObject(_)))); }
    assert_eq!(state.snapshot().trie_catalog().retired_search_runs(&table).count(), 0);
    assert!(observer.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn a_search_upload_without_catalog_adoption_is_fenced_before_release() {
    use rtdb_format::log::proto::{ArtefactsAdded, ArtefactsRemoved, SearchRun, SearchRunChange};
    use rtdb_maintenance::ArtefactPublisher;
    use rtdb_types::SourcePosition;

    struct SearchPublisher { table: TableRef, state: Arc<PartitionState>, store: Arc<LocalStore>, abandoned: AtomicBool }
    #[async_trait]
    impl ArtefactPublisher for SearchPublisher {
        async fn publish(&self, message: ArtefactsAdded) -> Result<SourcePosition> {
            let entries = message.entries.into_iter().map(|entry| {
                let record = entry.record.unwrap();
                assert!(SearchRunChange::from_artefact(&record).unwrap().abandon());
                (entry.trie_key.unwrap().parse::<TrieKey>().unwrap(), record)
            }).collect();
            self.state.add_artefacts_at(&self.table, entries, Timestamp(100));
            self.abandoned.store(true, Ordering::Relaxed);
            Ok(SourcePosition(1))
        }

        async fn remove(&self, message: ArtefactsRemoved) -> Result<SourcePosition> {
            assert!(self.abandoned.load(Ordering::Relaxed));
            let entries = message.entries.into_iter().map(|entry| {
                (entry.trie_key.unwrap().parse::<TrieKey>().unwrap(), entry.record.unwrap())
            }).collect();
            self.state.remove_artefacts_at(&self.table, entries, Some(ReplicaPosition(2)));
            Ok(SourcePosition(2))
        }

        async fn remove_with_recovery_gate(&self, message: ArtefactsRemoved, gate: Arc<dyn Send + Sync>) -> Result<SourcePosition> {
            assert!(message.entries.iter().all(|entry| SearchRunChange::from_artefact(entry.record.as_ref().unwrap())
                .unwrap().prune_settled_at.is_some()));
            assert!(self.store.try_lock_recovery_deletion().await?.is_none());
            drop(gate);
            let _application = self.store.hold_recovery_open().await?;
            self.remove(message).await
        }
    }

    let table = TableRef::new("public", "docs");
    let tables = TableCatalog::load(Some(block(0)), &BTreeMap::new()).unwrap().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let entry = tables.entry(&table).unwrap().clone();
    let l0 = TrieKey::l0(BlockIndex(0)).unwrap();
    let catalog = TrieCatalog::default().with_tries(table.clone(), [CatalogTrie::notification(l0, 1, None).unwrap()], Timestamp(10)).unwrap();
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap()));
    let epoch = StorageEpoch(0);
    let cut = CatalogCut { storage_version: StorageVersion(0), storage_epoch: epoch, replica_epoch: epoch, through: ReplicaPosition(1) };
    state.publish_catalog_cut(cut).unwrap();
    let files = tempfile::tempdir().unwrap();
    let open = || LocalStore::open(files.path()).unwrap().with_catalog_ledger(epoch);
    let writer = open();
    let store = Arc::new(open());
    let pending = SearchRun { definition_id: Some("docs_bodyf16".into()), definition_version: Some(1), blocks: vec![0],
        digest: Some(vec![7; 32]), locator: Some(vec![1]), file_size: Some(4), source_rows: Some(1), allocation: Some(vec![16; 16]),
        ..Default::default() };
    let marker = ObjectKey::new(pending.pending_key(&entry.slug).unwrap()).unwrap();
    let body = ObjectKey::new(pending.object_key(&entry.slug).unwrap()).unwrap();
    let owner = writer.search_staging_owner().await.unwrap().unwrap();
    let absent = SearchRun { digest: Some(vec![8; 32]), ..pending.clone() };
    let absent_marker = ObjectKey::new(absent.pending_key(&entry.slug).unwrap()).unwrap();
    let absent = rtdb_format::search_manifest::StagedSearchRun { run: absent, owner: Some(owner) };
    writer.put(&absent_marker, absent.encode().unwrap().into()).await.unwrap();
    let staged = rtdb_format::search_manifest::StagedSearchRun { run: pending, owner: Some(owner) };
    writer.put(&marker, staged.encode().unwrap().into()).await.unwrap();
    let retirement = Arc::new(SearchPublisher { table: table.clone(), state: Arc::clone(&state), store: Arc::clone(&store), abandoned: AtomicBool::new(false) });
    let trie_deletion = Arc::new(DeletedPublisher { table: entry, store: Arc::new(MemoryStore::default()), commits: Mutex::default() });
    let observer = Arc::new(Observer::default());
    let collector = TrieCollector::new(store.clone(), state.clone(), trie_deletion, observer.clone(), unleased(), fixed_clock()).unwrap()
        .with_search(retirement.clone(), StorageVersion(0), StorageEpoch(0));
    let stop = CancellationToken::new();
    collector.collect_as_of(Timestamp(100), &stop).await.unwrap();
    assert!(!retirement.abandoned.load(Ordering::Relaxed));
    assert!(store.head(&absent_marker).await.is_ok());
    writer.put(&body, Bytes::from_static(b"body")).await.unwrap();
    collector.collect_as_of(Timestamp(100), &stop).await.unwrap();
    assert!(!retirement.abandoned.load(Ordering::Relaxed));
    writer.close_and_wait().await;
    drop(writer);
    collector.collect_as_of(Timestamp(100), &stop).await.unwrap();
    assert!(retirement.abandoned.load(Ordering::Relaxed));
    assert!(store.head(&body).await.is_ok());
    assert!(matches!(store.head(&absent_marker).await, Err(rtdb_storage::Error::MissingObject(_))));
    collector.collect_as_of(Timestamp(101), &stop).await.unwrap();
    assert!(store.head(&body).await.is_err());
    assert!(store.head(&marker).await.is_err());
    let settlement = || state.snapshot().trie_catalog().retired_search_runs(&table).next()
        .map(|retired| (retired.released(), retired.settled_at_replica_msg_id));
    assert_eq!(settlement(), Some((true, Some(2))));

    let at = Timestamp(102);
    let old = ObjectKey::new(block_key(BlockIndex(0)).unwrap()).unwrap();
    let old_block = BlockCatalog::from_proto(pb::Block {
        boundary_replica_msg_id: Some(2), ..block(0).as_proto().clone()
    }).unwrap();
    store.put(&old, old_block.encode().into()).await.unwrap();
    let prepared = state.prepare_block().unwrap();
    let checkpoint = BlockCatalog::from_proto(pb::Block {
        block_index: Some(1), boundary_replica_msg_id: Some(3),
        latest_completed_tx: old_block.as_proto().latest_completed_tx,
        tables: prepared.table_catalog().unwrap().entries().map(|entry| entry.to_proto()).collect(),
        ..Default::default()
    }).unwrap();
    let table_blocks = prepared.table_blocks(&BTreeMap::new(), at).unwrap();
    assert_eq!(table_blocks[&table].as_proto().retired_search_runs[0].settled_at_replica_msg_id, Some(2));
    store.put(&ObjectKey::new(block_key(BlockIndex(1)).unwrap()).unwrap(), checkpoint.encode().into()).await.unwrap();
    state.adopt_block(prepared, checkpoint, &table_blocks, &BTreeMap::new(), at).unwrap();
    state.publish_catalog_cut(CatalogCut { through: ReplicaPosition(3), ..cut }).unwrap();

    collector.collect_as_of(at, &stop).await.unwrap();
    assert_eq!(settlement(), Some((true, Some(2))));
    store.delete(&old).await.unwrap();

    // A reader of the older catalog registers nothing; only recovery custody defers the prune.
    let startup = store.hold_recovery_open().await.unwrap();
    collector.collect_as_of(at, &stop).await.unwrap();
    assert_eq!(settlement(), Some((true, Some(2))));
    drop(startup);

    collector.collect_as_of(at, &stop).await.unwrap();
    assert_eq!(settlement(), None);
    assert!(store.try_lock_recovery_deletion().await.unwrap().is_some());
    assert!(state.prepare_block().unwrap().table_blocks(&BTreeMap::new(), at).unwrap()[&table]
        .as_proto().retired_search_runs.is_empty());
    assert!(observer.0.lock().unwrap().is_empty());
}

struct DirectoryPublisher(Arc<PartitionState>);

#[async_trait]
impl ArtefactPublisher for DirectoryPublisher {
    async fn publish(&self, _: ArtefactsAdded) -> Result<SourcePosition> { unreachable!() }
    async fn remove(&self, message: ArtefactsRemoved) -> Result<SourcePosition> {
        let entries = message.entries.into_iter().map(|entry| {
            (entry.trie_key.unwrap().parse().unwrap(), entry.record.unwrap())
        }).collect();
        self.0.remove_artefacts(&TableRef::new("public", "docs"), entries);
        Ok(SourcePosition(1))
    }
}

fn adopt_directory(state: &PartitionState, table: &TableRef, through: i64, root: &RootRef, previous: Option<&RootRef>, count: usize) {
    let mut l0 = CatalogTrie::notification(TrieKey::l0(BlockIndex(through)).unwrap(), 1, None).unwrap();
    let receipt = EntityDirectoryRoot { root: Some(root.encode()), through_block: Some(through),
        source_rows: Some(count as i64), allocation: Some(vec![through as u8; 16]), writer_fence: Some(1),
        erase_frontier_digest: Some(vec![0; 32]), ..Default::default() };
    l0.artefacts.push(EntityDirectoryChange { root: Some(receipt), replaces_root: previous.map(RootRef::encode) }.to_artefact().unwrap());
    state.add_tries(&BTreeMap::from([(table.clone(), vec![l0])]), Timestamp(through)).unwrap();
}

#[tokio::test]
async fn directory_collection_keeps_only_pages_the_next_root_reuses_whatever_a_reader_holds() {
    use rtdb_format::{cow_tree::{self, Edit, NoSummary, Row}, entity_directory};
    use rtdb_storage::{cache::BufferPool, cow_pages::ObjectPageStore};
    use rtdb_types::MemoryBudget;

    let table = TableRef::new("public", "docs");
    let tables = TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let entry = tables.entry(&table).unwrap().clone();
    let store = Arc::new(MemoryStore::default());
    let input = ObjectPageStore { store: store.clone(), storage: Arc::new(BufferPool::new(store.clone(), 16 << 20)),
        prefix: entity_directory::prefix(&entry.slug) };
    let memory = MemoryBudget::new(16 << 20);
    let count = cow_tree::MAX_LEAF_ROWS + 1;
    let key = |index| u32::try_from(index).unwrap().to_be_bytes().to_vec();
    let edits = (0..count).map(|index| Edit::Put(Row { key: key(index), value: vec![0] })).collect();
    let (old, _) = cow_tree::apply(&input, None, edits, &NoSummary, &memory).await.unwrap();
    let old = old.unwrap();
    let (next, _) = cow_tree::apply(&input, Some(&old), vec![Edit::Put(Row { key: key(0), value: vec![1] })],
        &NoSummary, &memory).await.unwrap();
    let next = next.unwrap();
    let old_pages = cow_tree::reachable(&input, &old).await.unwrap();
    let new_pages = cow_tree::reachable(&input, &next).await.unwrap();

    let state = Arc::new(PartitionState::new(PartitionValue::loaded(TrieCatalog::default(), tables, TrieConfig::default()).unwrap()));
    adopt_directory(&state, &table, 0, &old, None, count);
    let reader = state.snapshot();
    adopt_directory(&state, &table, 1, &next, Some(&old), count);
    let primary = Arc::new(DeletedPublisher { store: store.clone(), table: entry, commits: Mutex::new(Vec::new()) });
    let observer = Arc::new(Observer::default());
    let collector = TrieCollector::new(store.clone(), state.clone(), primary, observer.clone(), unleased(), fixed_clock()).unwrap()
        .with_entity_directories(Arc::new(DirectoryPublisher(state.clone())), StorageVersion(6), StorageEpoch(0));
    let cancellation = CancellationToken::new();
    collector.collect_as_of(Timestamp(1), &cancellation).await.unwrap();
    assert!(observer.0.lock().unwrap().is_empty());
    assert!(store.head(&ObjectKey::new(cow_tree::page_key(&input.prefix, old.digest)).unwrap()).await.is_err(),
        "a reader of the old root does not hold its pages once the lease has elapsed");
    drop(reader);
    for page in old_pages {
        assert_eq!(store.head(&ObjectKey::new(cow_tree::page_key(&input.prefix, page)).unwrap()).await.is_ok(), new_pages.contains(&page));
    }
    assert!(state.value().trie_catalog().retired_entity_directories(&table).next().is_none());
}

#[tokio::test]
async fn directory_collection_preserves_shared_pages_until_the_newer_retired_root_lease_elapses() {
    use rtdb_format::{cow_tree::{self, Edit, NoSummary, Row}, entity_directory};
    use rtdb_storage::{cache::BufferPool, cow_pages::ObjectPageStore};
    use rtdb_types::MemoryBudget;

    let table = TableRef::new("public", "docs");
    let tables = TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let entry = tables.entry(&table).unwrap().clone();
    let store = Arc::new(MemoryStore::default());
    let pages = ObjectPageStore { store: store.clone(), storage: Arc::new(BufferPool::new(store.clone(), 16 << 20)),
        prefix: entity_directory::prefix(&entry.slug) };
    let memory = MemoryBudget::new(16 << 20);
    let count = cow_tree::MAX_LEAF_ROWS + 1;
    let edit = |index: usize, value| Edit::Put(Row { key: (index as u32).to_be_bytes().to_vec(), value: vec![value] });
    let (old, _) = cow_tree::apply(&pages, None, (0..count).map(|index| edit(index, 0)).collect(), &NoSummary, &memory).await.unwrap();
    let old = old.unwrap();
    let (newer, _) = cow_tree::apply(&pages, Some(&old), vec![edit(0, 1)], &NoSummary, &memory).await.unwrap();
    let newer = newer.unwrap();
    let (current, _) = cow_tree::apply(&pages, Some(&newer), vec![edit(count - 1, 2)], &NoSummary, &memory).await.unwrap();
    let current = current.unwrap();
    let old_pages = cow_tree::reachable(&pages, &old).await.unwrap();
    let newer_pages = cow_tree::reachable(&pages, &newer).await.unwrap();
    let current_pages = cow_tree::reachable(&pages, &current).await.unwrap();
    let shared = old_pages.iter().find(|page| newer_pages.contains(page) && !current_pages.contains(page)).unwrap();
    let shared = ObjectKey::new(cow_tree::page_key(&pages.prefix, *shared)).unwrap();
    let old_root = ObjectKey::new(cow_tree::page_key(&pages.prefix, old.digest)).unwrap();

    let state = Arc::new(PartitionState::new(PartitionValue::loaded(TrieCatalog::default(), tables, TrieConfig::default()).unwrap()));
    adopt_directory(&state, &table, 0, &old, None, count);
    adopt_directory(&state, &table, 1, &newer, Some(&old), count);
    let time = Arc::new(AtomicI64::new(0));
    let clock = { let time = time.clone(); Arc::new(move || time.load(Ordering::Relaxed)) };
    let primary = Arc::new(DeletedPublisher { store: store.clone(), table: entry, commits: Mutex::default() });
    let observer = Arc::new(Observer::default());
    let config = GarbageConfig { reader_lease: Duration::from_micros(10), ..unleased() };
    let collector = TrieCollector::new(store.clone(), state.clone(), primary, observer.clone(), config, clock).unwrap()
        .with_entity_directories(Arc::new(DirectoryPublisher(state.clone())), StorageVersion(6), StorageEpoch(0));
    let cancellation = CancellationToken::new();
    collector.collect_as_of(Timestamp(1), &cancellation).await.unwrap();

    adopt_directory(&state, &table, 2, &current, Some(&newer), count);
    time.store(10, Ordering::Relaxed);
    collector.collect_as_of(Timestamp(2), &cancellation).await.unwrap();
    assert!(matches!(store.head(&old_root).await, Err(rtdb_storage::Error::MissingObject(_))));
    assert!(store.head(&shared).await.is_ok());

    time.store(20, Ordering::Relaxed);
    collector.collect_as_of(Timestamp(2), &cancellation).await.unwrap();
    assert!(matches!(store.head(&shared).await, Err(rtdb_storage::Error::MissingObject(_))));
    assert!(state.value().trie_catalog().retired_entity_directories(&table).next().is_none());
    assert!(observer.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn directory_sealing_requires_page_releases_and_preserves_per_trie_history() {
    use rtdb_format::{data::{StoredEvent, StoredOperation, build_event_batch}, entity_directory, entity_history, ipc::write_file};
    use rtdb_maintenance::{ArtefactDatabase, ArtefactKind, EntityHistoryArtefacts};
    use rtdb_storage::cache::BufferPool;
    use rtdb_types::{EventOrigin, Iid, Interval, LogicalType, MemoryBudget, ScalarType, StructFields, Value};

    let directory = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalStore::open(directory.path()).unwrap());
    let coordinated = Arc::new(CatalogFloorStore { inner: local, blocked: false.into(), proposals: Mutex::default(), fail_release: Mutex::default() });
    let stores: [(Arc<dyn ObjectStore>, bool); 2] = [(Arc::new(MemoryStore::default()), true), (coordinated, false)];
    let table = TableRef::new("public", "docs");
    let tables = TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let slug = tables.entry(&table).unwrap().slug.clone();
    let value = Arc::new(PartitionValue::loaded(TrieCatalog::default(), tables, TrieConfig::default()).unwrap());
    let block = BlockIndex(0);
    let trie = TrieKey::l0(block).unwrap();
    let event = StoredEvent { position: None, origin: Some(EventOrigin::at([1; 16], 0, block)), commit_position: 0, entity_key: None,
        iid: Iid([1; 16]), system_from: Timestamp(1), valid_time: Interval::ALL, recency: None,
        operation: StoredOperation::Put(StructFields::from([("_id".into(), Value::I64(1))])) };
    let fields = StructFields::from([("_id".into(), LogicalType::Scalar(ScalarType::I64))]);
    let batch = build_event_batch(&[event], Some(fields), None).unwrap();
    let bytes = write_file(&batch.schema(), &[batch]).unwrap();
    let path = directory.path().join("input.arrow");
    std::fs::write(&path, &bytes).unwrap();

    for (store, can_release) in stores {
        let builder = EntityHistoryArtefacts.builder(ArtefactDatabase { name: "xtdb".into(), store: store.clone(),
            storage: Arc::new(BufferPool::new(store.clone(), 16 << 20)), memory: MemoryBudget::new(16 << 20),
            history: None, clock: Arc::new(|| Timestamp(1)) }).unwrap();
        let records = builder.seal_serving_search(&value, &table, &trie, &path, bytes.len() as u64, &CancellationToken::new()).await.unwrap();
        assert!(records.iter().any(|record| record.kind() == entity_history::KIND));
        assert_eq!(records.iter().any(|record| record.kind() == entity_directory::RECORD_KIND), can_release);
        let prefix = ObjectKey::new(entity_directory::prefix(&slug)).unwrap();
        assert_eq!(!store.list(Some(&prefix)).await.unwrap().is_empty(), can_release);
    }
}

#[tokio::test]
async fn unreachable_term_directory_pages_are_swept_when_tombstone_pruning_fails() {
    use rtdb_format::{cow_tree::{PageDigest, page_key}, log::proto::{SearchRun, SearchRunChange}, term_directory};
    use rtdb_maintenance::ArtefactPublisher;
    use rtdb_types::SourcePosition;

    struct Unused;
    #[async_trait]
    impl ArtefactPublisher for Unused {
        async fn publish(&self, _: ArtefactsAdded) -> Result<SourcePosition> { panic!("No search transition is due") }
        async fn remove(&self, _: ArtefactsRemoved) -> Result<SourcePosition> { panic!("No search transition is due") }
    }

    let table = TableRef::new("public", "docs");
    let tables = TableCatalog::load(Some(block(0)), &BTreeMap::new()).unwrap().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let entry = tables.entry(&table).unwrap().clone();
    let anchor = TrieKey::l0(BlockIndex(0)).unwrap();
    let catalog = TrieCatalog::default().with_tries(table.clone(), [CatalogTrie::notification(anchor.clone(), 1, None).unwrap()], Timestamp(10)).unwrap();
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap()));
    let run = SearchRun { definition_id: Some("docs_bodyf16".into()), definition_version: Some(1), blocks: vec![0],
        digest: Some(vec![7; 32]), locator: Some(vec![1]), file_size: Some(4), source_rows: Some(1), allocation: Some(vec![16; 16]),
        ..Default::default() };
    let release = SearchRunChange { run: Some(run), retire_only: Some(true), ..Default::default() };
    let abandon = SearchRunChange { abandon: Some(true), ..release.clone() }.to_artefact().unwrap();
    state.add_artefacts_at(&table, vec![(anchor.clone(), abandon)], Timestamp(10));
    state.remove_artefacts_at(&table, vec![(anchor, release.to_artefact().unwrap())], Some(ReplicaPosition(1)));
    state.publish_catalog_cut(CatalogCut { storage_version: StorageVersion(0), storage_epoch: StorageEpoch(0),
        replica_epoch: StorageEpoch(0), through: ReplicaPosition(2) }).unwrap();

    let store = Arc::new(MemoryStore::default());
    let checkpoint = ObjectKey::new(block_key(BlockIndex(0)).unwrap()).unwrap();
    store.put(&checkpoint, Bytes::from_static(b"\xff")).await.unwrap();
    let prefix = term_directory::page_prefix(&entry.slug);
    let orphans: Vec<_> = [1u8, 2].into_iter().map(|byte| ObjectKey::new(page_key(&prefix, PageDigest([byte; 32]))).unwrap()).collect();
    for key in &orphans { store.put(key, Bytes::from_static(b"page")).await.unwrap(); }
    let deletions = Arc::new(DeletedPublisher { table: entry.clone(), store: store.clone(), commits: Mutex::default() });
    let observer = Arc::new(Observer::default());
    let collector = TrieCollector::new(store.clone(), state.clone(), deletions, observer.clone(), unleased(), fixed_clock()).unwrap()
        .with_search(Arc::new(Unused), StorageVersion(0), StorageEpoch(0));
    collector.collect_as_of(Timestamp(100), &CancellationToken::new()).await.unwrap();
    for key in &orphans { assert!(matches!(store.head(key).await, Err(rtdb_storage::Error::MissingObject(_))), "{key:?}"); }
    assert_eq!(state.snapshot().trie_catalog().retired_search_runs(&table).next().unwrap().settled_at_replica_msg_id, Some(1));
    let errors = observer.0.lock().unwrap();
    assert_eq!(errors.iter().map(|error| error.code.as_str()).collect::<Vec<_>>(), ["rtdb.format/invalid-catalog"]);
}
