use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rtdb_format::{keys::TrieKey, log::proto::{ArtefactRecord, ArtefactsAdded}};
use rtdb_index::{CatalogTrie, PartitionState, PartitionValue, TableCatalog, TrieCatalog, TrieState};
use rtdb_maintenance::{ArtefactBuilder, ArtefactDefinition, ArtefactDriver, ArtefactJob, ArtefactPublisher, SnapshotSource, available_artefact_jobs};
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
}

#[tokio::test]
async fn building_all_records_every_available_artefact_once() {
    let state = Arc::new(PartitionState::new(PartitionValue::loaded(catalog(), TableCatalog::default().with_seeded_table(table(), Vec::new()).unwrap(), TrieConfig::default()).unwrap()));
    let builder = Arc::new(Builder { built: Mutex::new(Vec::new()) });
    let (driver, handle) = ArtefactDriver::new(state.clone() as Arc<dyn SnapshotSource>, builder.clone(), Arc::new(Applying(state.clone())), (StorageVersion(7), StorageEpoch(0)), 2, 4, false).unwrap();
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
