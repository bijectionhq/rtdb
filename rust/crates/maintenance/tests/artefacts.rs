use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rtdb_format::log::proto::{ArtefactRecord, ArtefactScope, ArtefactsAdded, ArtefactsRemoved};
use rtdb_index::{CatalogTrie, PartitionState, PartitionValue, TableCatalog, TrieCatalog, TrieState};
use rtdb_format::keys::{ArtefactOwner, TableSlug, TrieKey};
use rtdb_maintenance::{ArtefactBuilder, ArtefactDefinition, ArtefactDriver, ArtefactJob, ArtefactPublisher, SnapshotSource, available_artefact_jobs, retired_artefacts, stray_artefacts};
use rtdb_storage::{ObjectKey, ObjectStore};
use rtdb_trie::TrieConfig;
use rtdb_types::{Result, SourcePosition, StorageEpoch, StorageVersion, TableRef, Timestamp};
use tokio_util::sync::CancellationToken;

fn table() -> TableRef { TableRef::new("public", "docs") }
fn trie(key: &str, size: i64) -> CatalogTrie { CatalogTrie { key: key.parse().unwrap(), data_file_size: size, metadata: None, state: TrieState::Live, artefacts: Vec::new(), published_files: None } }
fn definition(id: &str) -> ArtefactDefinition { ArtefactDefinition { table: table(), kind: "search".into(), id: id.into(), version: 1, historical: true, group_bytes: None, packed: false } }
fn record(id: &str) -> ArtefactRecord { ArtefactRecord { kind: Some("search".into()), definition_id: Some(id.into()), definition_version: Some(1), stats: None, scope: None, file_size: None, published_version: None, pack_members: Vec::new() } }
fn catalog() -> TrieCatalog {
    let mut catalog = TrieCatalog::new(100).unwrap();
    for (key, size) in [("l00-rc-b00", 10), ("l01-rc-b00", 10), ("l01-r20200106-b00", 10), ("l02-rc-p0-b00", 100), ("l02-rc-p1-b00", 100), ("l02-rc-p2-b00", 100), ("l02-rc-p3-b00", 100), ("l01-rc-b01", 10), ("l00-rc-b02", 10)] {
        catalog = catalog.with_tries(table(), [trie(key, size)], Timestamp(100)).unwrap();
    }
    catalog
}
fn keys(jobs: &[ArtefactJob]) -> Vec<String> { jobs.iter().map(|job| format!("{}:{}", job.definition.id, job.owner)).collect() }

#[test]
fn settled_live_tries_lacking_an_artefact_become_jobs_current_side_and_higher_levels_first() {
    let catalog = catalog();
    let live: Vec<_> = catalog.current_tries(&table()).iter().map(|trie| trie.key.to_string()).collect();
    assert!(live.contains(&"l00-rc-b02".into()) && live.contains(&"l01-rc-b01".into()), "{live:?}");
    let jobs = available_artefact_jobs(&catalog, &[definition("body")]);
    assert_eq!(keys(&jobs), ["body:l02-rc-p0-b00", "body:l02-rc-p1-b00", "body:l02-rc-p2-b00", "body:l02-rc-p3-b00", "body:l01-r20200106-b00"]);
    let recorded = catalog.with_artefacts(&table(), jobs.iter().flat_map(|job| job.tries()).map(|trie| (trie, record("body"))));
    assert!(available_artefact_jobs(&recorded, &[definition("body")]).is_empty());
    assert_eq!(available_artefact_jobs(&recorded, &[definition("title")]).len(), jobs.len());
    let current_only = ArtefactDefinition { historical: false, ..definition("body") };
    assert_eq!(keys(&available_artefact_jobs(&catalog, &[current_only])), ["body:l02-rc-p0-b00", "body:l02-rc-p1-b00", "body:l02-rc-p2-b00", "body:l02-rc-p3-b00"], "a definition without history leaves historical tries alone");
}

struct Builder { built: Mutex<Vec<ArtefactJob>> }
#[async_trait]
impl ArtefactBuilder for Builder {
    async fn definitions(&self, _: &Arc<PartitionValue>, _: &CancellationToken) -> Result<Vec<ArtefactDefinition>> { Ok(vec![definition("body")]) }
    async fn build(&self, job: &ArtefactJob, _: rtdb_maintenance::ArtefactSource<'_>, _: &Arc<PartitionValue>, _: &CancellationToken) -> Result<ArtefactRecord> {
        self.built.lock().unwrap().push(job.clone());
        Ok(record(&job.definition.id))
    }
}
struct Applying(Arc<PartitionState>);
#[async_trait]
impl ArtefactPublisher for Applying {
    async fn publish(&self, message: ArtefactsAdded) -> Result<SourcePosition> {
        assert_eq!(message.storage_version, Some(7));
        let table = rtdb_format::catalog::legacy_table_ref(message.table_name.as_deref().unwrap()).unwrap();
        self.0.add_artefacts(&table, message.entries.into_iter().map(|entry| (entry.trie_key.unwrap().parse().unwrap(), entry.record.unwrap())).collect());
        Ok(SourcePosition(1))
    }
    async fn remove(&self, message: ArtefactsRemoved) -> Result<SourcePosition> {
        let table = rtdb_format::catalog::legacy_table_ref(message.table_name.as_deref().unwrap()).unwrap();
        self.0.remove_artefacts(&table, message.entries.into_iter().map(|entry| (entry.trie_key.unwrap().parse().unwrap(), entry.record.unwrap())).collect());
        Ok(SourcePosition(2))
    }
}

#[tokio::test]
async fn building_all_records_every_available_artefact_once() {
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog(), TableCatalog::default().with_seeded_table(table(), Vec::new()).unwrap(), TrieConfig::default()).unwrap()));
    let builder = Arc::new(Builder { built: Mutex::new(Vec::new()) });
    let (driver, handle) = ArtefactDriver::new(state.clone() as Arc<dyn SnapshotSource>, builder.clone(), Arc::new(Applying(state.clone())), (StorageVersion(7), StorageEpoch(0)), "search", Arc::new(rtdb_storage::MemoryStore::default()), 2, 4, false).unwrap();
    let cancellation = CancellationToken::new();
    let running = tokio::spawn(driver.run(cancellation.clone()));
    handle.build_all().await.unwrap();
    let built = builder.built.lock().unwrap().len();
    assert_eq!(built, available_artefact_jobs(&catalog(), &[definition("body")]).len());
    assert!(available_artefact_jobs(state.value().trie_catalog(), &[definition("body")]).is_empty());
    handle.build_all().await.unwrap();
    assert_eq!(builder.built.lock().unwrap().len(), built, "a recorded artefact is not built again");
    cancellation.cancel();
    running.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_stored_artefact_holds_its_catalog_until_the_build_drains() {
    use rtdb_storage::LocalStore;
    use rtdb_types::{CatalogCut, ReplicaPosition};
    use tokio::sync::Notify;

    #[derive(Default)]
    struct HeldBuild { started: Notify, finish: Notify }

    #[async_trait]
    impl ArtefactBuilder for HeldBuild {
        async fn definitions(&self, _: &Arc<PartitionValue>, _: &CancellationToken) -> Result<Vec<ArtefactDefinition>> {
            Ok(vec![definition("body")])
        }
        async fn build(&self, job: &ArtefactJob, _: rtdb_maintenance::ArtefactSource<'_>, _: &Arc<PartitionValue>,
            _: &CancellationToken,
        ) -> Result<ArtefactRecord> {
            self.started.notify_one();
            self.finish.notified().await;
            Ok(record(&job.definition.id))
        }
    }

    let catalog = TrieCatalog::new(100).unwrap().with_tries(table(), [trie("l01-rc-b00", 100)], Timestamp(100)).unwrap();
    let tables = TableCatalog::default().with_seeded_table(table(), Vec::new()).unwrap();
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap()));
    let selected = CatalogCut { storage_version: StorageVersion(7), storage_epoch: StorageEpoch(0),
        replica_epoch: StorageEpoch(0), through: ReplicaPosition(1) };
    state.publish_catalog_cut(selected).unwrap();

    let directory = tempfile::tempdir().unwrap();
    let open = || Arc::new(LocalStore::open(directory.path()).unwrap().with_catalog_ledger(StorageEpoch(0)));
    let collector = open();
    let builder = Arc::new(HeldBuild::default());
    let (driver, handle) = ArtefactDriver::new(state.clone(), builder.clone(), Arc::new(Applying(state)),
        (StorageVersion(7), StorageEpoch(0)), "search", open(), 1, 1, false).unwrap();
    let cancellation = CancellationToken::new();
    let running = tokio::spawn(driver.run(cancellation.clone()));
    let build = tokio::spawn(async move { handle.build_all().await });
    tokio::time::timeout(std::time::Duration::from_secs(5), builder.started.notified()).await.unwrap();
    let newer = CatalogCut { through: ReplicaPosition(2), ..selected };
    assert!(!collector.advance_catalog_floor(newer).await.unwrap());
    builder.finish.notify_one();
    build.await.unwrap().unwrap();
    assert!(collector.advance_catalog_floor(newer).await.unwrap());
    cancellation.cancel();
    running.await.unwrap().unwrap();
}

#[test]
fn a_stray_is_an_unrecorded_object_of_a_settled_definition_no_longer_in_force_under_a_live_trie() {
    let slug = TableSlug::recorded("public$docs").unwrap();
    let live = "l02-rc-p0-b00".parse().unwrap();
    let catalog = catalog().with_artefacts(&table(), [(live, record("body"))]);
    let object = |trie: &str, kind: &str, id: &str, version: i64| ObjectKey::new(slug.artefact_key(&trie.parse().unwrap(), kind, id, version).unwrap()).unwrap();
    let in_force = ArtefactDefinition { version: 5, ..definition("title") };
    let listed = [
        object("l02-rc-p0-b00", "search", "body", 1),
        object("l02-rc-p0-b00", "search", "title", 5),
        object("l02-rc-p0-b00", "search", "title", 4),
        object("l02-rc-p0-b00", "search", "title", 9),
        object("l02-rc-p0-b00", "other", "title", 4),
        object("l01-rc-b00", "search", "title", 4),
        object("l02-rc-p1-b00", "search", "body~a1f1", 1),
        ObjectKey::new("tables/public$docs/artefacts/l02-rc-p1-b00/search/scratch.tmp").unwrap(),
    ];
    let strays = stray_artefacts(&catalog, &table(), &slug, "search", &[in_force.clone()], 8, &listed);
    assert_eq!(strays, [listed[2].clone(), listed[6].clone()], "recorded, in force, newer than the basis, of another kind, under a superseded trie, or not an artefact key: kept");
}

#[test]
fn a_retired_record_is_one_of_the_kind_on_a_live_trie_whose_settled_definition_left_force() {
    let live: rtdb_format::keys::TrieKey = "l02-rc-p0-b00".parse().unwrap();
    let with = |id: &str, version: i64| ArtefactRecord { definition_version: Some(version), ..record(id) };
    let other = ArtefactRecord { kind: Some("other".into()), ..record("gone") };
    let catalog = catalog().with_artefacts(&table(), [(live.clone(), with("body", 1)), (live.clone(), with("title", 5)), (live.clone(), with("title", 9)), (live.clone(), other),
        ("l01-rc-b00".parse().unwrap(), with("body", 1))]);
    let in_force = ArtefactDefinition { version: 5, ..definition("title") };
    let retired = retired_artefacts(&catalog, &table(), "search", &[in_force], 8);
    assert_eq!(retired, [(live, with("body", 1))], "in force, newer than the basis, of another kind, or on a superseded trie: kept");
}

fn grouped(id: &str, limit: i64) -> ArtefactDefinition { ArtefactDefinition { group_bytes: Some(limit), ..definition(id) } }
fn group_record(id: &str) -> ArtefactRecord { ArtefactRecord { scope: Some(ArtefactScope::PartGroup as i32), ..record(id) } }

#[test]
fn a_partition_group_whose_members_all_lack_the_artefact_is_one_job_within_its_byte_limit() {
    let catalog = catalog();
    assert_eq!(keys(&available_artefact_jobs(&catalog, &[grouped("body", 400)])), ["body:g02-rc-b00", "body:l01-r20200106-b00"]);
    assert_eq!(keys(&available_artefact_jobs(&catalog, &[grouped("body", 399)])), keys(&available_artefact_jobs(&catalog, &[definition("body")])), "a group above the limit is built a member at a time");
    let one_recorded = catalog.with_artefacts(&table(), [("l02-rc-p2-b00".parse().unwrap(), record("body"))]);
    assert_eq!(keys(&available_artefact_jobs(&one_recorded, &[grouped("body", 400)])), ["body:l02-rc-p0-b00", "body:l02-rc-p1-b00", "body:l02-rc-p3-b00", "body:l01-r20200106-b00"], "a member that has the artefact already leaves its siblings to their own");
    let mut compacted = catalog;
    for bucket in 0..4 { compacted = compacted.with_tries(table(), [trie(&format!("l03-rc-p0{bucket}-b01"), 25)], Timestamp(101)).unwrap(); }
    assert!(matches!(compacted.trie(&table(), &"l02-rc-p0-b00".parse().unwrap()).unwrap().state, TrieState::Garbage { .. }));
    assert_eq!(keys(&available_artefact_jobs(&compacted, &[grouped("body", 400)])), ["body:g03-rc-p0-b01", "body:l02-rc-p1-b00", "body:l02-rc-p2-b00", "body:l02-rc-p3-b00", "body:l01-r20200106-b00"],
        "a group one of whose members has been superseded is built a live member at a time");
}

#[test]
fn a_pack_can_replace_existing_singletons_with_live_primaries_from_different_blocks() {
    let mut catalog = catalog();
    for bucket in 0..4 {
        catalog = catalog.with_tries(table(), [trie(&format!("l03-rc-p0{bucket}-b01"), 25)], Timestamp(101)).unwrap();
    }
    let body = ArtefactDefinition { packed: true, ..grouped("body", 400) };
    let first = "l03-rc-p00-b01".parse::<TrieKey>().unwrap();
    let second = "l02-rc-p1-b00".parse::<TrieKey>().unwrap();
    let catalog = catalog.with_artefacts(&table(), [(first.clone(), record("body")), (second.clone(), record("body"))]);
    let jobs = available_artefact_jobs(&catalog, &[body]);
    let job = jobs.iter().filter(|job| matches!(job.owner, ArtefactOwner::Pack(_)))
        .find(|job| job.tries().contains(&first)).unwrap();
    assert!(job.tries().contains(&second));
    let pack = ArtefactRecord { scope: Some(ArtefactScope::Pack as i32),
        pack_members: job.tries().iter().map(ToString::to_string).collect(), ..record("body") };
    let packed = catalog.with_artefacts(&table(), job.tries().into_iter().map(|trie| (trie, pack.clone())));
    for member in job.tries() {
        let trie = packed.trie(&table(), &member).unwrap();
        let recorded = trie.artefact("search", "body", 1).unwrap();
        assert_eq!(ArtefactOwner::of(&member, recorded).unwrap(), job.owner);
    }
}

#[test]
fn a_group_stray_is_an_unrecorded_group_object_of_a_retired_definition_with_a_live_member() {
    let slug = TableSlug::recorded("public$docs").unwrap();
    let group = |key: &str| ArtefactOwner::Group(key.parse::<TrieKey>().unwrap().part_group().unwrap());
    let object = |owner: &ArtefactOwner, id: &str| ObjectKey::new(slug.owner_artefact_key(owner, "search", id, 1).unwrap()).unwrap();
    let live = group("l02-rc-p0-b00");
    let listed = [object(&live, "body"), object(&live, "title"), object(&group("l02-rc-p0-b05"), "title")];
    let recorded = catalog().with_artefacts(&table(), live.tries().into_iter().map(|trie| (trie, group_record("body"))));
    let strays = stray_artefacts(&recorded, &table(), &slug, "search", &[], 8, &listed);
    assert_eq!(strays, [listed[1].clone()], "recorded by its members, or of a group the catalog holds no member of: kept");
}

struct Uploading {
    store: Arc<rtdb_storage::MemoryStore>,
    definitions: Mutex<Vec<ArtefactDefinition>>,
    built: Mutex<Vec<ArtefactJob>>,
    repair: Option<(ArtefactJob, ObjectKey)>,
}
#[async_trait]
impl ArtefactBuilder for Uploading {
    async fn definitions(&self, _: &Arc<PartitionValue>, _: &CancellationToken) -> Result<Vec<ArtefactDefinition>> { Ok(self.definitions.lock().unwrap().clone()) }
    async fn repairs(&self, snapshot: &Arc<PartitionValue>, _: &CancellationToken) -> Result<Vec<ArtefactJob>> {
        Ok(self.repair.iter().filter(|(job, _)| {
            job.tries().iter().filter_map(|key| snapshot.trie_catalog().trie(&job.definition.table, key))
                .filter(|trie| trie.state == TrieState::Live)
                .any(|trie| trie.artefact("search", &job.definition.id, 1).is_some_and(|record| record.scope() == ArtefactScope::PartGroup))
        }).map(|(job, _)| job.clone()).collect())
    }

    async fn build(&self, job: &ArtefactJob, _: rtdb_maintenance::ArtefactSource<'_>, _: &Arc<PartitionValue>, cancellation: &CancellationToken) -> Result<ArtefactRecord> {
        self.built.lock().unwrap().push(job.clone());
        if let Some((_, input)) = self.repair.as_ref().filter(|(repair, _)| repair == job) {
            let directory = tempfile::tempdir().unwrap();
            let memory = rtdb_columnar::MemoryBudget::new(1024);
            rtdb_maintenance::input::download(self.store.as_ref(), &memory, input, &directory.path().join("input"), cancellation).await?;
        }
        let slug = TableSlug::recorded("public$docs").unwrap();
        self.store.put(&ObjectKey::new(slug.owner_artefact_key(&job.owner, "search", &job.definition.id, 1).unwrap()).unwrap(), bytes::Bytes::from_static(b"artefact")).await.unwrap();
        Ok(record(&job.definition.id))
    }
}

#[tokio::test]
async fn a_group_artefact_is_recorded_on_every_member_and_retired_with_one_delete() {
    // The definition's version, 1, is at or below the basis, so it retires once it leaves force.
    let block = rtdb_format::catalog::BlockCatalog::from_proto(rtdb_format::block::proto::Block { block_index: Some(10),
        latest_completed_tx: Some(rtdb_format::block::proto::TxKey { tx_id: Some(10), system_time: Some(10_000_000), ..Default::default() }), ..Default::default() }).unwrap();
    let tables = TableCatalog::load(Some(block), &Default::default()).unwrap().with_seeded_table(table(), Vec::new()).unwrap();
    assert_eq!(tables.entry(&table()).unwrap().slug.as_str(), "public$docs");
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog(), tables, TrieConfig::default()).unwrap()));
    let store = Arc::new(rtdb_storage::MemoryStore::default());
    let builder = Arc::new(Uploading { store: store.clone(), definitions: Mutex::new(vec![ArtefactDefinition { historical: false, ..grouped("body", 400) }]), built: Mutex::new(Vec::new()), repair: None });
    let (driver, handle) = ArtefactDriver::new(state.clone() as Arc<dyn SnapshotSource>, builder.clone(), Arc::new(Applying(state.clone())), (StorageVersion(7), StorageEpoch(0)), "search", store.clone(), 2, 4, false).unwrap();
    let cancellation = CancellationToken::new();
    let running = tokio::spawn(driver.run(cancellation.clone()));
    handle.build_all().await.unwrap();
    assert_eq!(keys(&builder.built.lock().unwrap()), ["body:g02-rc-b00"]);
    let members = "l02-rc-p0-b00".parse::<TrieKey>().unwrap().part_group().unwrap().members();
    for member in &members {
        let catalog = state.value();
        let record = catalog.trie_catalog().trie(&table(), member).unwrap().artefact("search", "body", 1).cloned();
        assert_eq!(record.map(|record| record.scope()), Some(ArtefactScope::PartGroup), "{member}");
    }
    assert_eq!(store.list(None).await.unwrap().len(), 1);
    builder.definitions.lock().unwrap().clear();
    handle.build_all().await.unwrap();
    assert!(store.list(None).await.unwrap().is_empty(), "the group's one object is deleted");
    assert!(members.iter().all(|member| state.value().trie_catalog().trie(&table(), member).unwrap().artefacts.is_empty()), "every member's record is removed");
    cancellation.cancel();
    running.await.unwrap().unwrap();
}

#[tokio::test]
async fn a_shared_artefact_with_a_collected_input_is_replaced_by_the_survivors_own_files() {
    let definition = ArtefactDefinition { historical: false, ..grouped("body", 400) };
    let first = "l02-rc-p0-b00".parse::<TrieKey>().unwrap();
    let group = first.part_group().unwrap();
    let members = group.members();
    let mut catalog = catalog().with_artefacts(&table(), members.iter().cloned().map(|key| (key, group_record("body"))));
    let intact = catalog.with_artefacts(&table(), [(members[1].clone(), record("body"))]);
    assert_eq!(intact.trie(&table(), &members[1]).unwrap().artefact("search", "body", 1), Some(&group_record("body")));

    for bucket in 0..4 {
        let mut child = trie(&format!("l03-rc-p0{bucket}-b01"), 25);
        child.artefacts.push(group_record("body"));
        catalog = catalog.with_tries(table(), [child], Timestamp(101)).unwrap();
    }
    catalog = catalog.without_tries(&table(), &[first.clone()].into());
    assert!(catalog.trie(&table(), &first).is_none());
    let tables = TableCatalog::default().with_seeded_table(table(), Vec::new()).unwrap();
    let slug = tables.entry(&table()).unwrap().slug.clone();
    let input = ObjectKey::new(slug.data_key(&first)).unwrap();
    let store = Arc::new(rtdb_storage::MemoryStore::default());
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap()));
    let repair = ArtefactJob { definition: definition.clone(), owner: ArtefactOwner::Group(group) };
    let builder = Arc::new(Uploading {
        store: store.clone(), definitions: Mutex::new(vec![definition]), built: Mutex::new(Vec::new()), repair: Some((repair, input)),
    });
    let (driver, handle) = ArtefactDriver::new(state.clone(), builder.clone(), Arc::new(Applying(state.clone())),
        (StorageVersion(7), StorageEpoch(0)), "search", store.clone(), 2, 4, false).unwrap();
    let cancellation = CancellationToken::new();
    let running = tokio::spawn(driver.run(cancellation.clone()));

    handle.build_all().await.unwrap();
    state.add_artefacts(&table(), members.iter().cloned().map(|key| (key, group_record("body"))).collect());
    handle.build_all().await.unwrap();

    assert_eq!(keys(&builder.built.lock().unwrap()), ["body:g02-rc-b00", "body:l02-rc-p1-b00", "body:l02-rc-p2-b00", "body:l02-rc-p3-b00"]);
    assert_eq!(store.list(None).await.unwrap().len(), 3);
    for member in &members[1..] {
        let snapshot = state.value();
        let record = snapshot.trie_catalog().trie(&table(), member).unwrap().artefact("search", "body", 1).unwrap();
        assert_eq!(record.scope(), ArtefactScope::Trie);
        let object = ObjectKey::new(slug.recorded_artefact_key(member, record).unwrap()).unwrap();
        assert!(store.head(&object).await.is_ok());
    }
    cancellation.cancel();
    running.await.unwrap().unwrap();
}
