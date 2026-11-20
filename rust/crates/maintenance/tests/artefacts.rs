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
fn trie(key: &str, size: i64) -> CatalogTrie { CatalogTrie { key: key.parse().unwrap(), data_file_size: size, metadata: None, state: TrieState::Live, artefacts: Vec::new() } }
fn definition(id: &str) -> ArtefactDefinition { ArtefactDefinition { table: table(), kind: "search".into(), id: id.into(), version: 1, historical: true, group_bytes: None } }
fn record(id: &str) -> ArtefactRecord { ArtefactRecord { kind: Some("search".into()), definition_id: Some(id.into()), definition_version: Some(1), stats: None, scope: None } }
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

struct Uploading { store: Arc<rtdb_storage::MemoryStore>, definitions: Mutex<Vec<ArtefactDefinition>>, built: Mutex<Vec<ArtefactJob>> }
#[async_trait]
impl ArtefactBuilder for Uploading {
    async fn definitions(&self, _: &Arc<PartitionValue>, _: &CancellationToken) -> Result<Vec<ArtefactDefinition>> { Ok(self.definitions.lock().unwrap().clone()) }
    async fn build(&self, job: &ArtefactJob, _: &Arc<PartitionValue>, _: &CancellationToken) -> Result<ArtefactRecord> {
        self.built.lock().unwrap().push(job.clone());
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
    let builder = Arc::new(Uploading { store: store.clone(), definitions: Mutex::new(vec![ArtefactDefinition { historical: false, ..grouped("body", 400) }]), built: Mutex::new(Vec::new()) });
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
