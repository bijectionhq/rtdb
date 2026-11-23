mod tape;
mod transport;

use rtdb_types::StructFields;
use std::{collections::BTreeMap, fs::{self, File, OpenOptions}, io::{Read, Write}, path::Path, sync::{Arc, OnceLock}, time::Duration};

use async_trait::async_trait;
use rtdb_columnar::MemoryBudget;
use rtdb_format::{data::{StoredEvent, StoredOperation}, database::proto::DatabaseConfig, envelope::{ReplicaBody, ReplicaEnvelope, SourceBody, SourceMessage}, keys::CURRENT_STORAGE_VERSION, log::proto::{FlushBlock, resolved_tx::DbOp}};
use rtdb_index::PartitionState;
use rtdb_replication::{BlockStore, DatabaseEffects, ExternalReceipt, PartitionLogs, ProgressMonitor, ReplicationClock, ReplicationConfig, ReplicationDriver, ReplicationHandle, Role, TermFence, Verdict};
use rtdb_storage::{MemoryStore, cache::BufferPool, log::{Log, LogOffset, MemoryLog}, object_store::ReadReservation};
use rtdb_tx::{OpenTransaction, ResolveContext, TransactionAuthorization, TransactionWriter, WriterResult};
use rtdb_types::{Anomaly, EntityId, Interval, ReplicaPosition, SourcePosition, StorageEpoch, TableRef, Term, Timestamp, TransactionKey, TxId, Value};
use serde_json::{Value as Json, json};
use sha2::{Digest, Sha256};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use self::{tape::{Boot, Event, Input, Tape, micros}, transport::{BootTransport, CommittedClaim}};

type Error = Box<dyn std::error::Error>;
const MAX_INPUT: u64 = 1024 * 1024;
const TRIAL: Duration = Duration::from_secs(30);
const SHUTDOWN: Duration = Duration::from_secs(5);
const SCOPE: &str = "jvm-refused-claim-boot-and-authored-inputs";

fn unexpected() -> Anomaly {
    Anomaly::incorrect("rtdb.conformance/unexpected-database-effect", "Processor simulation does not submit database or role changes")
}

struct DatabasePolicy;
#[async_trait]
impl DatabaseEffects for DatabasePolicy {
    fn check_db_op(&self, _: &DbOp) -> rtdb_types::Result<()> { Err(unexpected()) }
    async fn apply_db_op(&self, _: &DbOp) -> rtdb_types::Result<()> { Err(unexpected()) }
    fn secondary_databases(&self) -> rtdb_types::Result<BTreeMap<String, DatabaseConfig>> { Ok(BTreeMap::new()) }
}
impl TransactionAuthorization for DatabasePolicy {
    fn check_role_membership(&self, _: &str, _: Option<&str>) -> rtdb_types::Result<()> { Err(unexpected()) }
}

struct AuthoredWriter {
    docs: Option<Vec<uuid::Uuid>>,
    expected: TransactionKey,
    observed: Arc<OnceLock<TransactionKey>>,
}

#[async_trait]
impl TransactionWriter for AuthoredWriter {
    async fn write(&mut self, tx: &mut OpenTransaction<'_>) -> rtdb_types::Result<WriterResult> {
        let actual = tx.tx_key();
        if self.observed.set(actual).is_err() || actual != self.expected {
            return Err(Anomaly::fault("rtdb.conformance/writer-key", format!("Native writer assigned {actual:?}, expected {:?}", self.expected)));
        }
        if let Some(docs) = &self.docs {
            let table = TableRef::new("public", "docs");
            let valid_time = Interval::new(actual.system_time, Timestamp::MAX)?;
            for uuid in docs {
                let id = *uuid.as_bytes();
                tx.append(table.clone(), StoredEvent {
                    commit_position: 0,
                    entity_key: None,
                    iid: EntityId::Uuid(id).iid(),
                    system_from: actual.system_time,
                    valid_time,
                    operation: StoredOperation::Put(StructFields::from([
                        ("_id".into(), Value::Uuid(id)), ("tx_id".into(), Value::I64(actual.tx_id.0)),
                    ])),
                    recency: None,
                })?;
            }
            Ok(WriterResult::Committed { user_metadata: None })
        } else {
            Ok(WriterResult::Aborted { error: Anomaly::incorrect("xtdb/incorrect", "aborted"), user_metadata: None })
        }
    }
}

struct PendingExternal { receipt: ExternalReceipt, observed: Arc<OnceLock<TransactionKey>> }

struct Trial {
    source: Arc<MemoryLog>,
    replica: Arc<MemoryLog>,
    transport: Arc<BootTransport>,
    handles: Vec<ReplicationHandle>,
    partitions: Option<Vec<Arc<PartitionState>>>,
    jobs: JoinSet<(usize, rtdb_types::Result<()>)>,
    cancellation: CancellationToken,
    memory: Arc<MemoryBudget>,
}

enum Scheduled<'a> { Boot(&'a Event<Boot>), Input(&'a Event<Input>) }

impl Scheduled<'_> {
    fn sequence(&self) -> usize {
        match self { Self::Boot(frame) => frame.sequence, Self::Input(frame) => frame.sequence }
    }
}

impl Trial {
    async fn open(tape: &Tape) -> Result<Self, Error> {
        let memory = MemoryBudget::new(512 * 1024 * 1024);
        let source = Arc::new(MemoryLog::new(4 * 1024 * 1024));
        let replica = Arc::new(MemoryLog::new(4 * 1024 * 1024));
        let store = Arc::new(MemoryStore::default());
        let cancellation = CancellationToken::new();
        let transport = BootTransport::new(Arc::clone(&replica), cancellation.clone());
        let initial = Timestamp(tape.claim_time());
        let clock: ReplicationClock = Arc::new(move || initial);
        let mut actors = Vec::new();
        let mut partitions = tape.observes_live_indexes().then(Vec::new);
        for actor in 0..3 {
            let cache = Arc::new(BufferPool::new(store.clone(), 64 * 1024 * 1024));
            let logs = PartitionLogs::new(source.clone(), transport.actor(actor), StorageEpoch(0), StorageEpoch(0),
                CURRENT_STORAGE_VERSION, StorageEpoch(0), store.clone(), Arc::clone(&cache), memory.scope())?;
            let blocks = BlockStore::new(store.clone(), Arc::clone(&cache), memory.scope(), 100_000_000)?;
            let recovered = blocks.recover(Default::default(), logs.source_floor()?, initial, rtdb_replication::BlockDiscovery::List).await?;
            if let Some(partitions) = &mut partitions { partitions.push(Arc::clone(&recovered.partition)); }
            let progress = ProgressMonitor::new(recovered.progress.clone());
            let node_cancellation = cancellation.child_token();
            let context = Arc::new(ResolveContext {
                placed_receipt_retirement: false,
                owned_witness_scope: None,
                world_constraints: None,
                database: "test-db".into(), memory: memory.scope(), storage: cache,
                query_options: Default::default(), scan_accelerators: Default::default(), cancellation: node_cancellation.clone(), default_tz: "UTC".into(),
                authorization: Arc::new(DatabasePolicy), execution_observer: None, tracer: None,
            });
            let config = ReplicationConfig {
                external_source: true, rows_per_block: tape.rows_per_block, assertion_interval: Duration::from_secs(90),
                flush_timeout: Duration::from_secs(90), election_seed: tape.seed as u64, ..Default::default()
            };
            actors.push(ReplicationDriver::new_with_clock(config, logs, blocks, recovered, progress, context,
                Arc::new(DatabasePolicy), node_cancellation, Arc::clone(&clock))?);
        }
        let mut jobs = JoinSet::new();
        let handles = actors.into_iter().enumerate().map(|(actor, (handle, driver))| {
            jobs.spawn(async move { (actor, driver.run().await) });
            handle
        }).collect();
        Ok(Self { source, replica, transport, handles, partitions, jobs, cancellation, memory })
    }

    async fn run(&mut self, tape: &Tape, observed: &mut Vec<Json>, current: &mut Option<usize>) -> Result<Json, Error> {
        let mut schedule: Vec<_> = tape.boot.iter().map(Scheduled::Boot).chain(tape.inputs.iter().map(Scheduled::Input)).collect();
        schedule.sort_unstable_by_key(Scheduled::sequence);
        let work = scenario(&self.handles, &self.transport, &self.source, &self.replica, &self.memory, &self.cancellation,
            &schedule, observed, current, self.partitions.is_some());
        tokio::select! {
            result = tokio::time::timeout(TRIAL, work) => result.map_err(|_| "native refused-claim replay timed out")?,
            child = self.jobs.join_next() => Err(format!("native driver ended during the scheduled replay: {child:?}").into()),
        }
    }

    async fn finish(mut self, result: Result<Json, Error>) -> (Result<Json, Error>, Vec<String>) {
        let failures = self.join_drivers().await;
        let result = match (result, self.partitions.as_deref()) {
            (Ok(mut summary), Some(partitions)) if failures.is_empty() => {
                self.joined_live_indexes(&summary, partitions).await.map(|observation| {
                    summary["joined_live_index"] = observation;
                    summary
                })
            }
            (result, _) => result,
        };
        self.source.close();
        self.replica.close();
        (result, failures)
    }

    async fn join_drivers(&mut self) -> Vec<String> {
        self.cancellation.cancel();
        let collect = async {
            let mut failures = Vec::new();
            while let Some(result) = self.jobs.join_next().await {
                match result {
                    Ok((_, Ok(()))) => {}
                    Ok((actor, Err(error))) => failures.push(format!("actor {actor}: {error}")),
                    Err(error) => failures.push(error.to_string()),
                }
            }
            failures
        };
        match tokio::time::timeout(SHUTDOWN, collect).await {
            Ok(failures) => failures,
            Err(_) => {
                self.jobs.abort_all();
                while self.jobs.join_next().await.is_some() {}
                vec!["driver shutdown exceeded its deadline".into()]
            }
        }
    }

    #[cfg(test)]
    async fn shutdown(mut self) -> Vec<String> {
        let failures = self.join_drivers().await;
        self.source.close();
        self.replica.close();
        failures
    }

    async fn joined_live_indexes(&self, selected: &Json, partitions: &[Arc<PartitionState>]) -> Result<Json, Error> {
        let prefix = selected["native_replica"].as_array().ok_or("missing selected replica prefix")?;
        let tip = self.replica.latest()?;
        let (records, last_applied) = replica_summary(&self.replica, tip, &self.memory, true).await?;
        if !records.starts_with(prefix) { return Err("joined replica log differs from the selected prefix".into()); }
        let expected = last_applied.ok_or("joined replica log has no admitted resolved transaction")?;
        let mut actors = Vec::new();
        for (actor, partition) in partitions.iter().enumerate() {
            let actual = partition.value().live().latest_completed_tx();
            if actual.map(|key| key.tx_id) != Some(expected) {
                return Err(format!("native live index {actor} reached {actual:?}, expected {expected:?} from the joined replica log").into());
            }
            actors.push(json!({"actor":actor,"tx_id":actual.map(|key| key.tx_id.0)}));
        }
        let suffix: Vec<_> = records.into_iter().skip(prefix.len()).collect();
        Ok(json!({"replica_tip":tip.0,"last_applied_tx_id":expected.0,"actors":actors,"replica_suffix":suffix}))
    }
}

async fn wait_leading(handle: &ReplicationHandle) -> Result<(), Error> {
    let mut status = handle.watch_status();
    let reached = status.wait_for(|status| matches!(status.role, Role::Leading(_) | Role::Stopped)).await?;
    if reached.role != Role::Leading(Term(1)) { return Err("incumbent did not lead the recorded term".into()); }
    Ok(())
}

async fn run_boot(frame: &Event<Boot>, transport: &BootTransport, committed: &mut Option<CommittedClaim>) -> Result<Json, Error> {
    Ok(match frame.event {
        Boot::TailOpen { actor, tail, start } => {
            transport.open(actor).await?;
            json!({"kind":"replica_tail_opened", "actor":actor, "reference_tail":tail, "native_next_offset":start})
        }
        Boot::Claim { actor, .. } => {
            transport.attempt(actor).await?;
            json!({"kind":"claim_accepted", "actor":actor, "term":1, "term_sequence":0})
        }
        Boot::Refusal { actor, .. } => {
            transport.attempt(actor).await?;
            json!({"kind":"claim_refused", "actor":actor, "term":1, "term_sequence":0})
        }
        Boot::Durable { .. } => {
            let durable = transport.commit().await?;
            let observed = json!({"kind":"claim_provider_durable", "offset":durable.metadata.offset.0,
                "timestamp_micros":durable.metadata.timestamp.0});
            *committed = Some(durable);
            observed
        }
        Boot::Receipt { .. } => {
            committed.take().ok_or("recorded claim receipt precedes native durability")?.acknowledge()?;
            json!({"kind":"claim_receipt_released", "offset":0})
        }
        Boot::Delivery { actor, tail, from, to } => {
            transport.deliver(actor).await?;
            json!({"kind":"claim_delivered", "actor":actor, "reference_tail":tail, "reference_range":[from,to], "native_offset":0})
        }
    })
}

async fn scenario(
    handles: &[ReplicationHandle], transport: &BootTransport, source: &MemoryLog, replica: &MemoryLog, memory: &Arc<MemoryBudget>,
    cancellation: &CancellationToken, schedule: &[Scheduled<'_>], observed: &mut Vec<Json>, current: &mut Option<usize>,
    observe_live_indexes: bool,
) -> Result<Json, Error> {
    let mut committed = None;
    let mut pending = None;
    let mut last_source = SourcePosition(-1);
    let mut authored_sources = Vec::new();
    let mut final_key = None;
    let mut final_token = None;
    let mut final_commit = false;
    for item in schedule {
        *current = Some(item.sequence());
        let outcome = match item {
            Scheduled::Boot(frame) => run_boot(frame, transport, &mut committed).await?,
            Scheduled::Input(Event { event: Input::Flush { offset, time }, .. }) => {
                let position = handles[0].submit_source(
                    &SourceMessage::Protobuf(SourceBody::FlushBlock(FlushBlock { expected_block_idx: None })),
                    Timestamp(micros(*time).unwrap()),
                ).await?;
                if position <= last_source { return Err("authored source positions failed to advance".into()); }
                last_source = position;
                let mut range = source.range(LogOffset(position.0), LogOffset(position.0 + 1)).await?;
                let bytes = range.allocation_bound();
                let actual = range.next(ReadReservation::new(bytes, memory.reserve(bytes)?)).await?
                    .ok_or("native source receipt did not identify a durable record")?;
                if actual.metadata.offset.0 != position.0 || actual.metadata.timestamp.0 != micros(*time).unwrap()
                    || !matches!(SourceMessage::decode_wire(&actual.payload)?,
                        Some(SourceMessage::Protobuf(SourceBody::FlushBlock(FlushBlock { expected_block_idx: None }))))
                {
                    return Err("a native source record differs from its recorded absent-expectation flush".into());
                }
                authored_sources.push(position.0);
                json!({"kind":"source_flush_durable", "expected_block_idx":null, "reference_offset":offset,
                    "native_position":position.0, "timestamp_micros":actual.metadata.timestamp.0})
            }
            Scheduled::Input(Event { event: Input::ExternalStart { action, token, tx_id, time, documents }, .. }) => {
                wait_leading(&handles[0]).await?;
                let term = handles[0].term().ok_or("incumbent lost leadership before submitting a recorded action")?;
                let expected = TransactionKey { tx_id: TxId(*tx_id), system_time: Timestamp(micros(*time).unwrap()) };
                let observed = Arc::new(OnceLock::new());
                let writer = AuthoredWriter { docs: documents.clone(), expected, observed: Arc::clone(&observed) };
                let receipt = term.submit_external(Box::new(writer), Some(expected.system_time), Some(action.to_be_bytes().to_vec())).await?;
                pending = Some(PendingExternal { receipt, observed });
                json!({"kind":"external_command_submitted", "action":action, "input_timestamp_micros":expected.system_time.0, "token":token})
            }
            Scheduled::Input(Event { event: Input::ExternalApplied { action, tx_id, time, committed }, .. }) => {
                let PendingExternal { receipt, observed } = pending.take().ok_or("missing native external receipt")?;
                let result = receipt.applied().await?;
                let actual = result.tx_key();
                let progress = handles[0].progress().current();
                let token = action.to_be_bytes();
                if actual.tx_id != TxId(*tx_id) || actual.system_time != Timestamp(micros(*time).unwrap())
                    || observed.get() != Some(&actual) || result.committed() != *committed
                    || progress.tx_key != Some(actual) || progress.external_token.as_deref() != Some(token.as_slice())
                {
                    return Err(format!("native external observation differed: {result:?}, writer {:?}", observed.get()).into());
                }
                final_key = Some(actual);
                final_token = Some(token);
                final_commit = result.committed();
                json!({"kind":"external_applied", "action":action, "tx_id":actual.tx_id.0, "system_time_micros":actual.system_time.0,
                    "committed":final_commit, "token":tape::token(*action), "source_position":progress.source_position.0})
            }
        };
        observed.push(json!({"reference_sequence":item.sequence(), "observation":outcome}));
    }
    *current = None;
    let last_tx = final_key.ok_or("no recorded external action was applied")?;
    let token = final_token.ok_or("no native external token was applied")?;
    for handle in handles {
        handle.await_tx(last_tx.tx_id, cancellation).await?;
        handle.await_source(last_source, cancellation).await?;
    }
    let tip = replica.latest()?;
    let mut cohort = Vec::new();
    for (actor, handle) in handles.iter().enumerate() {
        let mut status = handle.watch_status();
        let reached = status.wait_for(|status| status.replica_position >= ReplicaPosition(tip.0) || status.role == Role::Stopped).await?;
        let expected_role = if actor == 0 { Role::Leading(Term(1)) } else { Role::Following };
        let progress = handle.progress().current();
        if reached.role != expected_role || reached.highest_term != Term(1) || reached.promotion_failure.is_some()
            || progress.tx_key != Some(last_tx) || progress.external_token.as_deref() != Some(token.as_slice())
            || progress.latest_result.as_ref().is_none_or(|completed| completed.result().committed() != final_commit)
            || handle.progress().fault().is_some()
        {
            return Err(format!("native actor {actor} did not converge at selected observation: {reached:?}, {progress:?}").into());
        }
        cohort.push(json!({"actor":actor, "role":if actor == 0 {"leading"} else {"following"},
            "term":reached.highest_term.0, "replica_position":reached.replica_position.0, "source_position":progress.source_position.0,
            "adopted_block":reached.adopted_block.as_ref().map(|catalog| catalog.block_index().0),
            "tx_id":last_tx.tx_id.0, "system_time_micros":last_tx.system_time.0, "committed":final_commit,
            "token":format!("{:08x}", u32::from_be_bytes(token))}));
    }
    let (records, _) = replica_summary(replica, tip, memory, observe_live_indexes).await?;
    for position in authored_sources {
        if records.iter().filter(|record| record["message"]["kind"] == "noop"
            && record["message"]["source_position"].as_i64() == Some(position)).count() != 1
        {
            return Err(format!("native source flush at position {position} did not publish its watermark").into());
        }
    }
    Ok(json!({"captured_replica_tip":tip.0,"actors":cohort,"native_replica":records}))
}

async fn replica_summary(
    replica: &MemoryLog, tip: LogOffset, memory: &Arc<MemoryBudget>, observe_admission: bool,
) -> Result<(Vec<Json>, Option<TxId>), Error> {
    if !(0..4096).contains(&tip.0) { return Err("native pilot exceeded its replica evidence bound".into()); }
    let mut output = Vec::new();
    let mut fence = observe_admission.then(|| TermFence::from_boundary(Term(0), None)).transpose()?;
    let mut last_applied = None;
    let mut range = replica.range(LogOffset(0), LogOffset(tip.0 + 1)).await?;
    let bytes = range.allocation_bound();
    while let Some(raw) = range.next(ReadReservation::new(bytes, memory.reserve(bytes)?)).await? {
        let bound = raw.payload.len().checked_mul(4).and_then(|bytes| bytes.checked_add(4096))
            .ok_or("native replica observation exceeded its decode allowance")?;
        let _decode = memory.reserve(bound)?;
        if observe_admission && raw.metadata.offset.0 != output.len() as i64 {
            return Err("native replica evidence has a missing offset".into());
        }
        let envelope = ReplicaEnvelope::decode(&raw.payload)?.ok_or("native replica record omitted its envelope")?;
        let admitted = fence.as_mut().map(|fence| {
            fence.admit(envelope.term, envelope.term_sequence).map(|admission| admission.verdict == Verdict::Apply)
        }).transpose()?;
        let detail = match envelope.message {
            ReplicaBody::NoOp(noop) => json!({"kind":"noop","source_position":noop.src_msg_id}),
            ReplicaBody::ResolvedTx(tx) => {
                if admitted == Some(true) {
                    let id = tx.tx_id.filter(|id| *id >= 0).ok_or("admitted native replica transaction has no valid ID")?;
                    last_applied = last_applied.max(Some(TxId(id)));
                }
                json!({"kind":"transaction","tx_id":tx.tx_id,"system_time_micros":tx.system_time_micros,
                    "source_position":tx.src_msg_id,"committed":tx.committed,"external_source_token":tx.external_source_token})
            }
            ReplicaBody::BlockBoundary(block) => json!({"kind":"boundary","block_index":block.block_index,"source_position":block.latest_processed_msg_id,
                "external_source_token":block.external_source_token}),
            ReplicaBody::BlockUploaded(block) => json!({"kind":"upload","block_index":block.block_index,"source_position":block.latest_processed_msg_id,
                "external_source_token":block.external_source_token,"storage_version":block.storage_version,"storage_epoch":block.storage_epoch}),
            ReplicaBody::TriesAdded(add) => json!({"kind":"tries_added","source_position":add.source_msg_id}),
            _ => return Err("unexpected message in native refused-claim replay".into()),
        };
        let mut record = json!({"offset":raw.metadata.offset.0, "timestamp_micros":raw.metadata.timestamp.0, "term":envelope.term.0,
            "term_sequence":envelope.term_sequence.map(|value| value.0),
            "payload_sha256":format!("{:x}", Sha256::digest(&raw.payload)), "message":detail});
        if let Some(admitted) = admitted { record["term_admitted"] = Json::Bool(admitted); }
        output.push(record);
    }
    if observe_admission && output.len() as i64 != tip.0 + 1 {
        return Err("native replica evidence ended before the sampled tip".into());
    }
    Ok((output, last_applied))
}

/// Runs three native processors from a projection of at most 1 MiB.
/// The guarded Python launcher validates the JVM capture and binds it to the projection.
/// Requires a fresh output directory and returns the same JSON it writes to `execution.json`.
/// Requires a Tokio runtime with time enabled.
/// An invalid projection or evidence I/O failure returns an error.
/// A trial or shutdown failure is recorded with `execution_complete: false`.
pub async fn execute(input: &Path, output: &Path) -> Result<Json, Error> {
    let file = File::open(input)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > MAX_INPUT { return Err("native simulation projection must be a file under one MiB".into()); }
    let mut raw = Vec::new();
    file.take(MAX_INPUT + 1).read_to_end(&mut raw)?;
    if raw.len() as u64 > MAX_INPUT { return Err("native simulation projection exceeds one MiB".into()); }
    let tape: Tape = serde_json::from_slice(&raw)?;
    tape.validate()?;
    fs::create_dir(output)?;
    let mut observed = Vec::new();
    let mut current = None;
    let (result, cleanup) = match Trial::open(&tape).await {
        Ok(mut trial) => {
            let result = trial.run(&tape, &mut observed, &mut current).await;
            trial.finish(result).await
        }
        Err(error) => (Err(error), Vec::new()),
    };
    let complete = result.is_ok() && cleanup.is_empty();
    let (summary, failure) = match result {
        Ok(summary) => (Some(summary), None),
        Err(error) => (None, Some(error.to_string())),
    };
    let evidence = json!({
        "schema_version":tape.schema_version, "scope": SCOPE, "execution_complete":complete,
        "reference_revision":tape.reference_revision, "unique_id":tape.unique_id, "seed":tape.seed,
        "reference_capture_sha256":tape.reference_capture_sha256, "tape_sha256":tape.tape_sha256,
        "input_sha256":format!("{:x}", Sha256::digest(&raw)), "binary_sha256":crate::controls::digest_file(&std::env::current_exe()?)?,
        "artifact_revision":env!("RTDB_CONFORMANCE_REVISION"), "source_dirty":env!("RTDB_CONFORMANCE_DIRTY")=="true",
        "observations":observed,"summary":summary,"failed_reference_sequence":current,"failure":failure,"shutdown_failures":cleanup,
    });
    let mut file = OpenOptions::new().write(true).create_new(true).open(output.join("execution.json"))?;
    writeln!(file, "{}", serde_json::to_string(&evidence)?)?;
    file.sync_all()?;
    Ok(evidence)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> Tape {
        let boot = [
            (0, Boot::TailOpen { actor: 0, tail: 8, start: 0 }),
            (2, Boot::Claim { actor: 0, term: 1, term_sequence: 0, time: [100, 1_234] }),
            (3, Boot::TailOpen { actor: 1, tail: 12, start: 0 }),
            (4, Boot::Refusal { actor: 1, term: 1, term_sequence: 0 }),
            (5, Boot::Durable { offset: 0 }),
            (6, Boot::Receipt { offset: 0 }),
            (7, Boot::Delivery { actor: 1, tail: 12, from: 0, to: 1 }),
            (8, Boot::Delivery { actor: 0, tail: 8, from: 0, to: 1 }),
            (9, Boot::TailOpen { actor: 2, tail: 16, start: 0 }),
            (10, Boot::Refusal { actor: 2, term: 1, term_sequence: 0 }),
            (13, Boot::Delivery { actor: 2, tail: 16, from: 0, to: 2 }),
        ].into_iter().map(|(sequence, event)| Event { sequence, event }).collect();
        let mut inputs = vec![Event { sequence: 1, event: Input::Flush { offset: 0, time: [100, 1_100] } }];
        for action in 0..30u32 {
            let committed = action != 29;
            let time = [99, 1000 * (i64::from(action) + 1)];
            let sequence = 11 + action as usize * 4;
            inputs.push(Event { sequence, event: Input::ExternalStart { action, token: tape::token(action),
                tx_id: i64::from(action), time, documents: committed.then(|| vec![uuid::Uuid::from_u128(u128::from(action))]) } });
            inputs.push(Event { sequence: sequence + 1, event: Input::ExternalApplied { action, tx_id: i64::from(action), time, committed } });
        }
        Tape { schema_version: 1, reference_revision: tape::REFERENCE.into(), unique_id: "synthetic-native-transport".into(),
            seed: 7, reference_capture_sha256: "a".repeat(64), tape_sha256: "b".repeat(64), rows_per_block: 18, boot, inputs }
    }

    #[tokio::test]
    async fn boot_grants_and_authored_receipts_reach_three_real_processors() {
        for schema_version in [1, 2] {
            let mut tape = fixture();
            tape.schema_version = schema_version;
            tape.validate().unwrap();
            let mut trial = Trial::open(&tape).await.unwrap();
            let mut observed = Vec::new();
            let mut current = None;
            let result = trial.run(&tape, &mut observed, &mut current).await;
            let (result, failures) = trial.finish(result).await;
            let summary = result.unwrap_or_else(|error| panic!("Replay at sequence {current:?} failed: {error}; shutdown: {failures:?}"));
            assert!(failures.is_empty(), "{failures:?}");
            assert_eq!(observed[1]["reference_sequence"], 1);
            assert_eq!(observed[1]["observation"]["kind"], "source_flush_durable");
            assert_eq!(observed[4]["observation"]["kind"], "claim_refused");
            assert_eq!(observed[5]["observation"]["kind"], "claim_provider_durable");
            assert_eq!(observed[5]["observation"]["offset"], 0);
            assert_eq!(observed[6]["observation"]["kind"], "claim_receipt_released");
            assert_eq!(observed[12]["reference_sequence"], 12);
            assert_eq!(observed[12]["observation"]["kind"], "external_applied");
            assert_eq!(observed[13]["observation"]["kind"], "claim_delivered");
            assert_eq!(observed.last().unwrap()["observation"]["committed"], false);
            assert_eq!(summary["actors"].as_array().unwrap().len(), 3);
            for actor in summary["actors"].as_array().unwrap() {
                assert_eq!(actor["tx_id"], 29);
                assert_eq!(actor["token"], "0000001d");
            }
            if tape.observes_live_indexes() {
                let joined = &summary["joined_live_index"];
                assert_eq!(joined["last_applied_tx_id"], 29);
                assert_eq!(joined["actors"], json!([{"actor":0,"tx_id":29},{"actor":1,"tx_id":29},{"actor":2,"tx_id":29}]));
                assert!(summary["native_replica"].as_array().unwrap().iter().any(|entry| {
                    entry["term_admitted"] == true && entry["message"]["kind"] == "transaction"
                        && entry["message"]["tx_id"] == 29 && entry["message"]["committed"] == false
                }));
            } else {
                assert!(summary.get("joined_live_index").is_none());
                assert!(summary["native_replica"].as_array().unwrap().iter().all(|entry| entry.get("term_admitted").is_none()));
            }
            assert!(current.is_none());
        }
    }

    #[test]
    fn replay_projection_accepts_only_the_two_defined_versions() {
        let mut tape = fixture();
        for schema_version in [0, 3, u32::MAX] {
            tape.schema_version = schema_version;
            assert!(tape.validate().is_err());
        }
    }

    #[tokio::test]
    async fn failure_after_native_boot_still_joins_all_processors() {
        let mut tape = fixture();
        tape.inputs[2].event = Input::ExternalApplied { action: 0, tx_id: 77, time: [101, 1000], committed: true };
        assert!(tape.validate().is_err());
        let mut trial = Trial::open(&tape).await.unwrap();
        let mut observed = Vec::new();
        let mut current = None;
        let error = trial.run(&tape, &mut observed, &mut current).await.unwrap_err();
        let failures = trial.shutdown().await;
        assert_eq!(current, Some(12), "Replay failed: {error}; shutdown: {failures:?}");
        assert!(failures.is_empty(), "{failures:?}");
    }

    #[tokio::test]
    async fn shutdown_joins_processors_waiting_for_their_first_tail() {
        let trial = Trial::open(&fixture()).await.unwrap();
        tokio::task::yield_now().await;
        let failures = trial.shutdown().await;
        assert!(failures.is_empty(), "{failures:?}");
    }

    #[test]
    fn a_different_first_delivery_offset_rejects_the_projection() {
        let mut tape = fixture();
        tape.boot[6].event = Boot::Delivery { actor: 1, tail: 12, from: 1, to: 2 };
        assert_eq!(tape.validate().unwrap_err(), "invalid boot event at reference sequence 7");
    }
}
