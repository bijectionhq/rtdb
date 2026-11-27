use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    sync::Arc,
};

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use rtdb_format::{
    block::proto::{Block, TxKey},
    catalog::{BlockCatalog, TableBlock},
    keys::{TableSlug, TrieKey, block_key},
};
use rtdb_index::{
    CatalogTrie, PartitionState, PartitionValue, TableCatalog, TrieCatalog, TrieState,
};
use rtdb_maintenance::{
    CollectionObserver, CompactionJob, GarbageConfig, TrieCollector, TrieDeletionPublisher,
    available_jobs,
};
use rtdb_storage::{MemoryStore, ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, BlockIndex, ReplicaPosition, Result as DbResult, TableRef, Timestamp};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

use crate::faults::{FaultSchedule, FaultStore};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

const FILE_TARGET: i64 = 100 * 1024 * 1024;
const HOUR_MICROS: i64 = 60 * 60 * 1_000_000;
const REFERENCE_REVISION: &str = "0b9fa440f23d2165250d215209f7fa9bd7c26a6f";
const EXPECTED: [&str; 8] = [
    "l02-rc-p0-b03",
    "l02-rc-p1-b03",
    "l02-rc-p2-b03",
    "l02-rc-p3-b03",
    "l02-rc-p0-b07",
    "l02-rc-p1-b07",
    "l02-rc-p2-b07",
    "l02-rc-p3-b07",
];
const ASSERTIONS: [(usize, Option<usize>); 6] = [
    (506, Some(0)),
    (526, Some(0)),
    (526, Some(1)),
    (540, None),
    (541, None),
    (544, None),
];

struct Trace<'a> {
    storage: &'a FaultSchedule,
    events: Vec<Value>,
    assertions: BTreeMap<(usize, Option<usize>), bool>,
}

impl Trace<'_> {
    fn record(&mut self, kind: &str, detail: Value) {
        self.events.push(
            json!({"sequence":self.events.len(), "storage_position":self.storage.event_count(),
            "kind":kind, "detail":detail}),
        );
    }

    fn assertion(&mut self, line: usize, system: Option<usize>, passed: bool, operands: Value) {
        self.record(
            "assertion",
            json!({"source_line":line, "system":system, "passed":passed, "operands":operands}),
        );
        assert!(
            self.assertions.insert((line, system), passed).is_none(),
            "duplicate assertion evaluation"
        );
    }
}

#[derive(Default)]
struct CollectionFailures(Mutex<Vec<Anomaly>>);

impl CollectionObserver for CollectionFailures {
    fn table_failed(&self, _: Option<&TableRef>, error: &Anomaly) {
        self.0.lock().push(error.clone());
    }
}

struct DeleteTries {
    state: Arc<PartitionState>,
    notifications: Mutex<Vec<Vec<String>>>,
}

#[async_trait]
impl TrieDeletionPublisher for DeleteTries {
    async fn publish(&self, table: TableRef, keys: Vec<TrieKey>) -> DbResult<ReplicaPosition> {
        let mut notifications = self.notifications.lock();
        notifications.push(keys.iter().map(ToString::to_string).collect());
        self.state
            .delete_tries(&BTreeMap::from([(table, keys.into_iter().collect())]));
        Ok(ReplicaPosition(notifications.len() as i64 - 1))
    }
}

struct Publication {
    trie: CatalogTrie,
    time: Timestamp,
}

struct Trial<'a, 'b> {
    store: Arc<FaultStore>,
    table: TableRef,
    slug: TableSlug,
    systems: [Arc<PartitionState>; 2],
    trace: &'a mut Trace<'b>,
}

impl Trial<'_, '_> {
    fn catalog(&self, system: usize) -> BTreeMap<String, &'static str> {
        self.systems[system]
            .value()
            .trie_catalog()
            .iter_tries(&self.table)
            .map(|trie| {
                let state = match trie.state {
                    TrieState::Nascent => "nascent",
                    TrieState::Live => "live",
                    TrieState::Garbage { .. } => "garbage",
                };
                (trie.key.to_string(), state)
            })
            .collect()
    }

    fn check_l2_present(&mut self, line: usize, system: usize) {
        let catalog = self.catalog(system);
        self.trace.assertion(
            line,
            Some(system),
            EXPECTED.iter().all(|key| catalog.contains_key(*key)),
            json!({"expected_subset":EXPECTED, "catalog":catalog}),
        );
    }

    fn plan(&mut self, system: usize) -> DbResult<Vec<CompactionJob>> {
        let jobs = available_jobs(self.systems[system].value().trie_catalog())?;
        self.trace.record(
            "plan",
            json!({"system":system, "jobs":jobs.iter().map(|job| json!({
            "table":job.table.schema_and_table(), "output":job.output.to_string(),
            "inputs":job.inputs.iter().map(ToString::to_string).collect::<Vec<_>>(),
            "part":job.part, "recency_mode":format!("{:?}", job.recency_mode),
        })).collect::<Vec<_>>() }),
        );
        Ok(jobs)
    }

    fn start_job(&mut self, block: i64, part: u8) -> Result<CompactionJob> {
        let output = TrieKey::new(2, None, vec![part], BlockIndex(block))?;
        self.trace.record(
            "job_requested",
            json!({"system":0, "output":output.to_string()}),
        );
        let job = self
            .plan(0)?
            .into_iter()
            .find(|job| job.table == self.table && job.output == output)
            .ok_or_else(|| format!("system 0 could not plan scheduled output {output}"))?;
        let expected_inputs = (block - 3..=block)
            .map(|index| TrieKey::new(1, None, Vec::new(), BlockIndex(index)))
            .collect::<DbResult<Vec<_>>>()?;
        if job.inputs != expected_inputs {
            return Err(format!("unexpected merge inputs for {output}: {:?}", job.inputs).into());
        }
        self.trace.record(
            "job_started",
            json!({"system":0, "output":output.to_string()}),
        );
        Ok(job)
    }

    async fn publish(&mut self, job: CompactionJob) -> Result<Publication> {
        self.trace.record(
            "merge_writes",
            json!({"system":0, "output":job.output.to_string(), "placeholder_bytes":1}),
        );
        put_trie(self.store.as_ref(), &self.slug, &job.output).await?;
        let time = Timestamp(self.trace.events.len() as i64);
        let trie = CatalogTrie::notification(job.output, FILE_TARGET, None)?;
        self.trace.record(
            "broadcast",
            json!({"system":0, "table":self.table.schema_and_table(),
            "output":trie.key.to_string(), "data_file_size":FILE_TARGET, "time_micros":time.0}),
        );
        Ok(Publication { trie, time })
    }

    async fn apply(&mut self, system: usize, publication: &Publication, kind: &str) -> Result<()> {
        self.trace.record(
            kind,
            json!({"system":system, "output":publication.trie.key.to_string(),
            "time_micros":publication.time.0}),
        );
        put_trie(self.store.as_ref(), &self.slug, &publication.trie.key).await?;
        self.systems[system].add_tries(
            &BTreeMap::from([(self.table.clone(), vec![publication.trie.clone()])]),
            publication.time,
        )?;
        let catalog = self.catalog(system);
        self.trace.record(
            "publication_applied",
            json!({"system":system, "via":kind,
            "output":publication.trie.key.to_string(), "catalog":catalog}),
        );
        Ok(())
    }

    async fn complete_job(
        &mut self,
        job: CompactionJob,
        delay_broadcast: bool,
    ) -> Result<Publication> {
        let publication = self.publish(job).await?;
        self.apply(0, &publication, "broadcast_delivery").await?;
        if !delay_broadcast {
            self.apply(1, &publication, "broadcast_delivery").await?;
        }
        self.apply(0, &publication, "writer_apply").await?;
        self.trace.record(
            "job_finished",
            json!({"system":0, "output":publication.trie.key.to_string()}),
        );
        Ok(publication)
    }

    async fn collect(&mut self, system: usize, phase: &str) -> Result<()> {
        let state = &self.systems[system];
        let as_of = Timestamp(self.trace.events.len() as i64 + HOUR_MICROS);
        let publisher = Arc::new(DeleteTries {
            state: state.clone(),
            notifications: Mutex::default(),
        });
        let errors = Arc::new(CollectionFailures::default());
        let collector = TrieCollector::new(
            self.store.clone(),
            state.clone(),
            publisher.clone(),
            errors.clone(),
            GarbageConfig {
                blocks_to_keep: 2,
                garbage_lifetime: chrono::TimeDelta::seconds(60),
                ..Default::default()
            },
        )?;
        let before = self.catalog(system);
        self.trace.record(
            "gc_started",
            json!({"system":system, "phase":phase, "as_of_micros":as_of.0, "catalog":before}),
        );
        let result = collector
            .collect_as_of(as_of, &CancellationToken::new())
            .await;
        let failures: Vec<_> = errors.0.lock().iter().map(ToString::to_string).collect();
        let after = self.catalog(system);
        self.trace.record(
            "gc_finished",
            json!({"system":system, "phase":phase, "catalog":after,
            "deletion_publications":*publisher.notifications.lock(), "failures":failures}),
        );
        result?;
        if !failures.is_empty() {
            return Err(format!("system {system} GC reported {failures:?}").into());
        }
        Ok(())
    }

    async fn run(&mut self) -> Result<()> {
        self.trace
            .record("compact_all_requested", json!({"system":0}));
        let first = self.start_job(3, 0)?;
        let second = self.start_job(3, 2)?;
        self.collect(0, "initial").await?;
        self.complete_job(second, false).await?;
        self.complete_job(first, false).await?;
        for (block, part) in [(3, 1), (3, 3), (7, 2), (7, 0), (7, 3)] {
            let job = self.start_job(block, part)?;
            self.complete_job(job, false).await?;
        }
        let last = self.start_job(7, 1)?;
        let delayed = self.complete_job(last, true).await?;
        self.confirm_idle(0)?;
        self.trace.record("system_joined", json!({"system":0}));
        self.check_l2_present(506, 0);

        self.trace
            .record("compact_all_requested", json!({"system":1}));
        self.collect(1, "initial").await?;
        self.apply(1, &delayed, "broadcast_delivery").await?;
        self.confirm_idle(1)?;
        self.trace.record("system_joined", json!({"system":1}));
        for system in 0..2 {
            self.check_l2_present(526, system);
        }

        for system in 0..2 {
            self.collect(system, "final").await?;
        }
        let catalogs = [self.catalog(0), self.catalog(1)];
        let sets = catalogs
            .each_ref()
            .map(|catalog| catalog.keys().map(String::as_str).collect::<BTreeSet<_>>());
        let distinct = BTreeSet::from([&sets[0], &sets[1]]).len();
        let expected = BTreeSet::from(EXPECTED);
        self.trace.assertion(
            540,
            None,
            distinct == 1,
            json!({"expected":1, "actual":distinct, "sets":sets}),
        );
        self.trace.assertion(
            541,
            None,
            sets[0] == expected,
            json!({"expected":expected, "actual":sets[0]}),
        );

        let prefix = ObjectKey::new(format!("{}/data", self.slug.table_prefix()))?;
        let objects = self.store.list(Some(&prefix)).await?;
        let data_files: Vec<_> = objects.iter().map(|object| object.key.as_str()).collect();
        let stored: BTreeSet<_> = data_files
            .iter()
            .map(|path| {
                let name = path.rsplit('/').next().unwrap_or(path);
                name.strip_suffix(".arrow").unwrap_or(name)
            })
            .collect();
        self.trace.assertion(
            544,
            None,
            stored == expected,
            json!({"expected":expected, "actual":stored, "object_keys":data_files}),
        );
        Ok(())
    }

    fn confirm_idle(&mut self, system: usize) -> Result<()> {
        let jobs = self.plan(system)?;
        if !jobs.is_empty() {
            return Err(format!("system {system} did not settle: {jobs:?}").into());
        }
        self.trace
            .record("compact_all_finished", json!({"system":system}));
        Ok(())
    }
}

async fn put_trie(store: &dyn ObjectStore, slug: &TableSlug, key: &TrieKey) -> Result<()> {
    for path in [slug.data_key(key), slug.metadata_key(key)] {
        store
            .put(&ObjectKey::new(path)?, Bytes::from_static(&[0]))
            .await?;
    }
    Ok(())
}

async fn replay(store: Arc<FaultStore>, trace: &mut Trace<'_>) -> Result<()> {
    let table = TableRef::new("public", "docs");
    let slug = TableSlug::for_unrecorded_table(&table)?;
    let mut seed = Vec::new();
    for index in 0..8 {
        let key = TrieKey::new(1, None, Vec::new(), BlockIndex(index))?;
        trace.record("seed_trie", json!({"table":table.schema_and_table(), "key":key.to_string(), "data_file_size":FILE_TARGET}));
        put_trie(store.as_ref(), &slug, &key).await?;
        seed.push(CatalogTrie::notification(key, FILE_TARGET, None)?);
    }
    for index in 5..=7 {
        let block = BlockCatalog::from_proto(Block {
            block_index: Some(index),
            term_id: Some(0),
            latest_processed_msg_id: Some(index),
            latest_completed_tx: Some(TxKey {
                tx_id: Some(index),
                system_time: Some(index),
                ..Default::default()
            }),
            table_names: vec![table.schema_and_table()],
            ..Default::default()
        })?;
        trace.record("checkpoint_written", json!({"index":index, "tx_id":index, "source_position":index, "system_time_micros":index}));
        store
            .put(
                &ObjectKey::new(block_key(BlockIndex(index))?)?,
                Bytes::from(block.encode()),
            )
            .await?;
    }
    let latest_key = ObjectKey::new(block_key(BlockIndex(7))?)?;
    let table_block = TableBlock::from_parts(BTreeMap::new(), &Default::default(), 0, Vec::new())?;
    let table_blocks = BTreeMap::from([(table.clone(), table_block)]);
    let mut systems = Vec::new();
    for system in 0..2 {
        let block = BlockCatalog::decode(&store.get(&latest_key).await?)?;
        let index = block.block_index();
        let tables = TableCatalog::load(Some(block), &table_blocks)?;
        let state = Arc::new(PartitionState::new(PartitionValue::loaded(
            TrieCatalog::new(FILE_TARGET)?,
            tables,
            Default::default(),
        )?));
        trace.record(
            "checkpoint_loaded",
            json!({"system":system, "block":index.0, "checkpoint_tries":[]}),
        );
        state.add_tries(
            &BTreeMap::from([(table.clone(), seed.clone())]),
            Timestamp(0),
        )?;
        trace.record(
            "seed_catalog_applied",
            json!({"system":system, "time_micros":0,
            "keys":seed.iter().map(|trie| trie.key.to_string()).collect::<Vec<_>>() }),
        );
        systems.push(state);
    }
    let systems = systems
        .try_into()
        .map_err(|_| "expected two classic systems")?;
    Trial {
        store,
        table,
        slug,
        systems,
        trace,
    }
    .run()
    .await
}

async fn evidence() -> Result<Value> {
    let schedule = FaultSchedule::new(Vec::new(), 10_000)?;
    let store = FaultStore::new(
        Arc::new(MemoryStore::default()),
        "public/docs".into(),
        schedule.clone(),
    );
    let mut trace = Trace {
        storage: &schedule,
        events: Vec::new(),
        assertions: BTreeMap::new(),
    };
    let result = replay(store, &mut trace).await;
    schedule.close();
    let exhausted = schedule.assert_exhausted();
    let passed = ASSERTIONS
        .iter()
        .filter(|identity| trace.assertions.get(identity) == Some(&true))
        .count();
    let unobserved = ASSERTIONS
        .iter()
        .filter(|identity| !trace.assertions.contains_key(identity))
        .count();
    let total = ASSERTIONS.len();
    Ok(json!({
        "projection":"source-authored-interleaving", "execution_complete":result.is_ok() && exhausted.is_ok()
            && passed == total && trace.assertions.len() == total,
        "assertion_denominator":{"assertions":total, "passed":passed, "failed":total-passed, "excluded":0, "unobserved":unobserved},
        "assertion_plan":ASSERTIONS.iter().map(|(line, system)| json!({"source_line":line, "system":system})).collect::<Vec<_>>(),
        "failure":result.err().map(|error| error.to_string()),
        "storage_trace_failure":exhausted.err().map(|error| error.to_string()),
        "events":trace.events, "storage_events":schedule.events(),
    }))
}

pub async fn execute(output: &Path) -> Result<Value> {
    fs::create_dir(output)?;
    let evidence = json!({
        "schema_version":1, "reference_revision":REFERENCE_REVISION,
        "reference_source":"core/src/test/kotlin/xtdb/NodeSimulationTest.kt",
        "reference_source_sha256":"20863ebac2c3a100ccbca1d41b02b3d7ebef1e332bb085a33db023b12e926c61",
        "reference_case":"xtdb.NodeSimulationTest/staggered system startup during active compaction",
        "reference_declaration_line":455, "selected_schedules":1, "reference_invocations":0,
        "reference_fixture_sources":{
            "core/src/test/kotlin/xtdb/SimulationTestUtils.kt":"bf7865e494d8ebb3b3554733d904defa60e7fe810ceae7524f01eacf338b6244",
            "core/src/test/kotlin/xtdb/compactor/CompactorSimulationTest.kt":"19b76b7619c4d2bb8ca06afea22df2d884a35f523adc3a5da5c4d49f65ee0ab9",
        },
        "artifact_revision":env!("RTDB_CONFORMANCE_REVISION"), "source_dirty":env!("RTDB_CONFORMANCE_DIRTY")=="true",
        "binary_sha256":crate::controls::digest_file(&std::env::current_exe()?)?,
        "replay":evidence().await?,
    });
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output.join("execution.json"))?;
    writeln!(file, "{}", serde_json::to_string(&evidence)?)?;
    file.sync_all()?;
    Ok(evidence)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn staggered_compaction_replays_the_same_schedule_and_all_original_assertions() {
        let replay = evidence().await.unwrap();
        assert_eq!(replay["execution_complete"], true, "{replay}");
        assert_eq!(
            replay["assertion_denominator"],
            json!({"assertions":6, "passed":6, "failed":0, "excluded":0, "unobserved":0})
        );
        assert_eq!(replay, evidence().await.unwrap());

        let events = replay["events"].as_array().unwrap();
        let frames = |kind| events.iter().filter(move |event| event["kind"] == kind);
        let assertions: Vec<_> = frames("assertion")
            .map(|event| {
                let detail = &event["detail"];
                json!({"source_line":detail["source_line"], "system":detail["system"]})
            })
            .collect();
        assert_eq!(json!(assertions), replay["assertion_plan"]);
        assert_eq!(frames("seed_trie").count(), 8);
        assert_eq!(
            frames("checkpoint_written")
                .map(|event| event["detail"]["index"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            [5, 6, 7]
        );
        assert_eq!(frames("checkpoint_loaded").count(), 2);
        assert_eq!(frames("job_started").count(), 8);
        assert_eq!(frames("job_finished").count(), 8);
        assert_eq!(frames("broadcast").count(), 8);
        assert_eq!(frames("writer_apply").count(), 8);
        assert_eq!(frames("broadcast_delivery").count(), 16);
        for system in 0..2 {
            let outputs: BTreeSet<_> = frames("broadcast_delivery")
                .filter(|event| event["detail"]["system"] == system)
                .map(|event| event["detail"]["output"].as_str().unwrap())
                .collect();
            assert_eq!(outputs, BTreeSet::from(EXPECTED));
        }

        let collections: Vec<_> = frames("gc_finished").collect();
        assert_eq!(collections.len(), 4);
        assert_eq!(collections[0]["detail"]["deletion_publications"], json!([]));
        let second_system_start = frames("gc_started").nth(1).unwrap();
        let catalog = &second_system_start["detail"]["catalog"];
        assert_eq!(catalog["l02-rc-p1-b03"], "live");
        assert_eq!(catalog["l02-rc-p0-b07"], "nascent");
        assert!(catalog.get("l02-rc-p1-b07").is_none());
        assert_eq!(catalog["l01-rc-b07"], "live");
        let late_delivery = frames("broadcast_delivery")
            .find(|event| {
                event["detail"]["system"] == 1 && event["detail"]["output"] == "l02-rc-p1-b07"
            })
            .unwrap();
        assert!(
            collections[1]["sequence"].as_u64().unwrap()
                < late_delivery["sequence"].as_u64().unwrap()
        );
        for (collection, removed) in collections[1..].iter().zip([0..4, 0..8, 4..8]) {
            let actual: BTreeSet<_> = collection["detail"]["deletion_publications"][0]
                .as_array()
                .unwrap()
                .iter()
                .map(|key| key.as_str().unwrap().to_owned())
                .collect();
            assert_eq!(
                actual,
                removed
                    .map(|block| format!("l01-rc-b{block:02x}"))
                    .collect::<BTreeSet<_>>()
            );
        }
        let storage = replay["storage_events"].as_array().unwrap();
        assert_eq!(events.last().unwrap()["storage_position"], storage.len());
        let before = second_system_start["storage_position"].as_u64().unwrap() as usize;
        let after = collections[1]["storage_position"].as_u64().unwrap() as usize;
        let deleted: BTreeSet<_> = storage[before..after]
            .iter()
            .filter(|event| {
                event["kind"] == "provider_returned" && event["point"]["operation"] == "delete"
            })
            .map(|event| event["point"]["key"].as_str().unwrap().to_owned())
            .collect();
        let expected: BTreeSet<_> = (0..4)
            .flat_map(|block| {
                ["data", "meta"]
                    .map(|part| format!("tables/public$docs/{part}/l01-rc-b{block:02x}.arrow"))
            })
            .collect();
        assert_eq!(deleted, expected);
    }
}
