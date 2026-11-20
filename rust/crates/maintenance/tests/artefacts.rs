use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rtdb_format::log::proto::{ArtefactRecord, ArtefactsAdded, ArtefactsRemoved};
use rtdb_index::{CatalogTrie, PartitionState, PartitionValue, TableCatalog, TrieCatalog, TrieState};
use rtdb_format::keys::TableSlug;
use rtdb_maintenance::{ArtefactBuilder, ArtefactDefinition, ArtefactDriver, ArtefactJob, ArtefactPublisher, SnapshotSource, available_artefact_jobs, retired_artefacts, stray_artefacts};
use rtdb_storage::ObjectKey;
use rtdb_trie::TrieConfig;
use rtdb_types::{Result, SourcePosition, StorageEpoch, StorageVersion, TableRef, Timestamp};
use tokio_util::sync::CancellationToken;

fn table() -> TableRef { TableRef::new("public", "docs") }
fn trie(key: &str, size: i64) -> CatalogTrie { CatalogTrie { key: key.parse().unwrap(), data_file_size: size, metadata: None, state: TrieState::Live, artefacts: Vec::new() } }
fn definition(id: &str) -> ArtefactDefinition { ArtefactDefinition { table: table(), kind: "search".into(), id: id.into(), version: 1 } }
fn record(id: &str) -> ArtefactRecord { ArtefactRecord { kind: Some("search".into()), definition_id: Some(id.into()), definition_version: Some(1), stats: None } }
fn catalog() -> TrieCatalog {
    let mut catalog = TrieCatalog::new(100).unwrap();
    for (key, size) in [("l00-rc-b00", 10), ("l01-rc-b00", 10), ("l01-r20200106-b00", 10), ("l02-rc-p0-b00", 100), ("l02-rc-p1-b00", 100), ("l02-rc-p2-b00", 100), ("l02-rc-p3-b00", 100), ("l01-rc-b01", 10), ("l00-rc-b02", 10)] {
        catalog = catalog.with_tries(table(), [trie(key, size)], Timestamp(100)).unwrap();
    }
    catalog
}
fn keys(jobs: &[ArtefactJob]) -> Vec<String> { jobs.iter().map(|job| format!("{}:{}", job.definition.id, job.trie)).collect() }

#[test]
fn settled_live_tries_lacking_an_artefact_become_jobs_current_side_and_higher_levels_first() {
    let catalog = catalog();
    let live: Vec<_> = catalog.current_tries(&table()).iter().map(|trie| trie.key.to_string()).collect();
    assert!(live.contains(&"l00-rc-b02".into()) && live.contains(&"l01-rc-b01".into()), "{live:?}");
    let jobs = available_artefact_jobs(&catalog, &[definition("body")]);
    assert_eq!(keys(&jobs), ["body:l02-rc-p0-b00", "body:l02-rc-p1-b00", "body:l02-rc-p2-b00", "body:l02-rc-p3-b00", "body:l01-r20200106-b00"]);
    let recorded = catalog.with_artefacts(&table(), jobs.iter().map(|job| (job.trie.clone(), record("body"))));
    assert!(available_artefact_jobs(&recorded, &[definition("body")]).is_empty());
    assert_eq!(available_artefact_jobs(&recorded, &[definition("title")]).len(), jobs.len());
}

struct Builder { built: Mutex<Vec<ArtefactJob>> }
#[async_trait]
impl ArtefactBuilder for Builder {
    async fn definitions(&self, _: &Arc<PartitionValue>, _: &CancellationToken) -> Result<Vec<ArtefactDefinition>> { Ok(vec![definition("body")]) }
    async fn build(&self, job: &ArtefactJob, _: &Arc<PartitionValue>, _: &CancellationToken) -> Result<ArtefactRecord> {
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
