use std::collections::{BTreeMap, BTreeSet, VecDeque};
use rtdb_types::StructFields;
use std::sync::{Arc, atomic::{AtomicI64, Ordering}};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use rtdb_columnar::MemoryBudget;
use rtdb_format::{database::proto::DatabaseConfig, envelope::{ReplicaBody, ReplicaEnvelope, SourceBody, SourceMessage}, keys::CURRENT_STORAGE_VERSION, log::proto::{self, resolved_tx::DbOp}};
use rtdb_index::TransactionBuffer;
use rtdb_storage::{MemoryStore, ObjectStore, cache::BufferPool, log::{LocalLog, LocalLogOptions, Log, LogMetadata, LogOffset, LogRecord, MemoryLog, ReadOnlyLocalLog, ReadOnlyLocalLogOptions}};
use rtdb_trie::TrieConfig;
use rtdb_tx::{Documents, ResolveContext, SourceTransaction, SubmitOptions, TransactionAuthorization, TxOp};
use rtdb_types::{Anomaly, BlockIndex, Interval, ReplicaPosition, Result, SourcePosition, StorageEpoch, TableRef, Term, TermSequence, Timestamp, TransactionKey, TxId, Value};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError::Empty;
use tokio_util::sync::CancellationToken;

use super::{Applied, TailPoll, accept_artefacts, admit_search_intents, cached_objects, read_replay_head, source_decode_bound, start_poll};
use crate::*;
use crate::logs::replica_decode_bound;

struct NoEffects;
#[async_trait]
impl DatabaseEffects for NoEffects {
    fn check_db_op(&self, _: &DbOp) -> Result<()> { Ok(()) }
    async fn apply_db_op(&self, _: &DbOp) -> Result<()> { Ok(()) }
    fn secondary_databases(&self) -> Result<BTreeMap<String, DatabaseConfig>> { Ok(BTreeMap::new()) }
}
struct AllowRoles;
impl TransactionAuthorization for AllowRoles { fn check_role_membership(&self, _: &str, _: Option<&str>) -> Result<()> { Ok(()) } }
struct Fixture {
    driver: ReplicationDriver, handle: ReplicationHandle, logs: Arc<PartitionLogs>, store: Arc<MemoryStore>, blocks: Arc<BlockStore>, memory: Arc<MemoryBudget>, source: Arc<MemoryLog>, replica: Arc<MemoryLog>,
}
async fn fixture(config: ReplicationConfig, replica_message_limit: usize) -> Fixture {
    fixture_with_clock(config, replica_message_limit, None).await
}

async fn fixture_with_clock(config: ReplicationConfig, replica_message_limit: usize, clock: Option<ReplicationClock>) -> Fixture {
    fixture_with_replica(config, replica_message_limit, clock, |replica| replica).await
}

async fn fixture_with_replica(config: ReplicationConfig, replica_message_limit: usize, clock: Option<ReplicationClock>,
    wrap_replica: impl FnOnce(Arc<MemoryLog>) -> Arc<dyn Log>) -> Fixture {
    let store = Arc::new(MemoryStore::default());
    fixture_over(config, replica_message_limit, clock, wrap_replica, Arc::clone(&store), store).await
}

/// The driver reads `store` through `objects`.
async fn fixture_over(config: ReplicationConfig, replica_message_limit: usize, clock: Option<ReplicationClock>,
    wrap_replica: impl FnOnce(Arc<MemoryLog>) -> Arc<dyn Log>, store: Arc<MemoryStore>, objects: Arc<dyn ObjectStore>) -> Fixture {
    let memory = MemoryBudget::new(1 << 30);
    let source = Arc::new(MemoryLog::new(1 << 28));
    let replica = Arc::new(MemoryLog::new(replica_message_limit));
    let cache = Arc::new(BufferPool::new(Arc::clone(&objects), 1 << 27));
    let logs = PartitionLogs::new(source.clone(), wrap_replica(replica.clone()), StorageEpoch(0), StorageEpoch(0), CURRENT_STORAGE_VERSION, StorageEpoch(0), Arc::clone(&objects), Arc::clone(&cache), Arc::clone(&memory)).unwrap();
    let blocks = BlockStore::new(objects, Arc::clone(&cache), Arc::clone(&memory), 100_000_000).unwrap();
    let recovered = blocks.recover(TrieConfig::new(2, 2).unwrap(), SourcePosition(-1), Timestamp(0), crate::BlockDiscovery::List).await.unwrap();
    let progress = ProgressMonitor::new(recovered.progress.clone());
    let resolve = Arc::new(ResolveContext { decision_clock: None, write_event_origins: false, placed_receipt_retirement: false, owned_witness_scope: None, world_constraints: None, database: "xtdb".into(), storage: cache, memory: Arc::clone(&memory), query_options: Default::default(), scan_accelerators: Default::default(), cancellation: CancellationToken::new(), default_tz: "UTC".into(), authorization: Arc::new(AllowRoles), execution_observer: None, tracer: None });
    let effects = Arc::new(NoEffects);
    let cancellation = CancellationToken::new();
    let (handle, driver) = match clock {
        Some(clock) => ReplicationDriver::new_with_clock(
            config, Arc::clone(&logs), Arc::clone(&blocks), recovered, progress, resolve, effects, cancellation, clock,
        ),
        None => ReplicationDriver::new(
            config, Arc::clone(&logs), Arc::clone(&blocks), recovered, progress, resolve, effects, cancellation,
        ),
    }.unwrap();
    Fixture { driver, handle, logs, store, blocks, memory, source, replica }
}
fn raw(position: i64, term: i64, sequence: i64, body: ReplicaBody) -> LogRecord {
    LogRecord::new(LogMetadata { offset: LogOffset(position), timestamp: Timestamp(position * 100 + 1) },
        ReplicaEnvelope { term: Term(term), term_sequence: Some(TermSequence(sequence)), message: body }.encode().into())
}
fn boundary(index: i64, source: i64) -> ReplicaBody { ReplicaBody::BlockBoundary(proto::BlockBoundary { block_index: Some(index), latest_processed_msg_id: Some(source), external_source_token: None }) }
fn resolved(id: i64, source: i64) -> ReplicaBody { ReplicaBody::ResolvedTx(proto::ResolvedTx { tx_id: Some(id), system_time_micros: Some(id * 100 + 100), committed: Some(true), src_msg_id: Some(source), ..Default::default() }) }
fn upload(index: i64, source: i64) -> ReplicaBody {
    ReplicaBody::BlockUploaded(proto::BlockUploaded { storage_version: Some(CURRENT_STORAGE_VERSION.0), storage_epoch: Some(0), block_index: Some(index), latest_processed_msg_id: Some(source), tries: Vec::new(), external_source_token: None, catalog_sha256: None })
}

#[tokio::test]
async fn appended_log_records_release_their_encoding_charges() {
    let memory = MemoryBudget::new(1 << 20);
    let source = Arc::new(MemoryLog::new(1 << 20));
    let replica = Arc::new(MemoryLog::new(1 << 20));
    let store = Arc::new(MemoryStore::default());
    let cache = Arc::new(BufferPool::new(store.clone(), 1 << 20));
    let logs = PartitionLogs::new(source.clone(), replica.clone(), StorageEpoch(0), StorageEpoch(0), CURRENT_STORAGE_VERSION,
        StorageEpoch(0), store, cache, Arc::clone(&memory)).unwrap();
    let message = SourceMessage::Protobuf(SourceBody::FlushBlock(proto::FlushBlock::default()));
    for index in 0..256 {
        logs.append_source(&message, Timestamp(index)).await.unwrap();
        let envelope = ReplicaEnvelope { term: Term(1), term_sequence: Some(TermSequence(index)), message: resolved(index, index) };
        logs.enqueue_replica(&envelope, BlockIndex(0), Timestamp(index)).await.unwrap().durable().await.unwrap();
    }
    assert!(memory.used_bytes() < 1 << 18);
    assert_eq!(source.latest().unwrap(), LogOffset(255));
    assert_eq!(replica.latest().unwrap(), LogOffset(255));
    assert_eq!(memory.used_bytes(), 0, "the in-memory logs own their records outside the execution budget");
    drop(logs);
    drop(source);
    drop(replica);
}

#[tokio::test]
async fn two_ready_local_tails_leave_room_to_read_and_decode_the_selected_head() {
    let directory = tempfile::tempdir().unwrap();
    let options = || LocalLogOptions { maximum_message_bytes: 1 << 20, queue_capacity: 2 };
    let (source, source_driver) = LocalLog::open(directory.path().join("LOG"), options()).unwrap();
    let (replica, replica_driver) = LocalLog::open(directory.path().join("REPLICA_LOG"), options()).unwrap();
    let writers = CancellationToken::new();
    let source_writer = tokio::spawn(source_driver.run(writers.clone()));
    let replica_writer = tokio::spawn(replica_driver.run(writers.clone()));
    let source_payload = Bytes::from(vec![0xff; 2048]);
    let source_payload_len = source_payload.len();
    let source_metadata = source.enqueue(source_payload, Timestamp(11)).await.unwrap().durable().await.unwrap();
    let replica_record = raw(0, 1, 0, resolved(0, 0));
    let replica_payload_len = replica_record.payload.len();
    let replica_metadata = replica.enqueue(replica_record.payload, Timestamp(12)).await.unwrap().durable().await.unwrap();

    let source_decode_bytes = source_decode_bound(source_payload_len).unwrap();
    let replica_decode_bytes = replica_decode_bound(replica_payload_len).unwrap();
    let source_read_bytes = 256 + source_payload_len;
    let replica_read_bytes = 256 + replica_payload_len;
    let replica_needed = replica_read_bytes + replica_decode_bytes;
    let memory = MemoryBudget::new(source_read_bytes + source_decode_bytes);
    let store = Arc::new(MemoryStore::default());
    let cache = Arc::new(BufferPool::new(store.clone(), 1 << 20));
    let logs = PartitionLogs::new(source.clone(), replica.clone(), StorageEpoch(0), StorageEpoch(0), CURRENT_STORAGE_VERSION,
        StorageEpoch(0), store, cache, Arc::clone(&memory)).unwrap();
    let source_tail = logs.source_replay(logs.source_floor().unwrap()).await.unwrap();
    let replica_tail = logs.replica_replay(logs.replica_floor().unwrap()).await.unwrap();
    let ((mut source_tail, source_ready), (mut replica_tail, replica_ready)) =
        tokio::join!(start_poll(source_tail, Duration::ZERO), start_poll(replica_tail, Duration::ZERO));
    let TailPoll::Prepared(Some(source_head)) = source_ready.unwrap() else { panic!("expected a prepared source head"); };
    let TailPoll::Prepared(Some(replica_head)) = replica_ready.unwrap() else { panic!("expected a prepared replica head"); };
    assert_eq!(source_head.metadata, source_metadata);
    assert_eq!(source_head.allocation_bound, source_read_bytes);
    assert_eq!(replica_head.metadata, replica_metadata);
    assert_eq!(replica_head.allocation_bound, replica_read_bytes);
    assert_eq!(memory.used_bytes(), 0);
    assert!(source_read_bytes < replica_needed);

    let running = CancellationToken::new();
    let blocker_bytes = memory.limit_bytes() - replica_needed;
    let one_byte_short = memory.reserve(blocker_bytes + 1).unwrap();
    let refusal = read_replay_head(&mut replica_tail, replica_head, logs.memory(), replica_decode_bound, &running).await
        .err().expect("expected a replay admission failure");
    assert_eq!(refusal.code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), blocker_bytes + 1);
    assert_eq!(replica_tail.replay().unwrap().poll_head(Duration::ZERO).await.unwrap(), Some(replica_head));
    drop(one_byte_short);

    let blocker = memory.reserve(blocker_bytes).unwrap();
    let record = read_replay_head(&mut replica_tail, replica_head, logs.memory(), replica_decode_bound, &running).await.unwrap().unwrap();
    assert_eq!(memory.used_bytes(), memory.limit_bytes());
    let decoded = logs.decode_reserved_replica(record.raw, record.decode_reservation).unwrap().unwrap();
    assert_eq!(decoded.envelope.term, Term(1));
    assert_eq!(decoded.envelope.term_sequence, Some(TermSequence(0)));
    assert!(matches!(&decoded.envelope.message, ReplicaBody::ResolvedTx(message) if message.tx_id == Some(0)));
    assert_eq!(decoded.metadata.timestamp, Timestamp(12));
    assert_eq!(memory.used_bytes(), blocker_bytes + replica_decode_bytes);
    drop(decoded);
    drop(blocker);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(replica_tail.replay().unwrap().poll_head(Duration::ZERO).await.unwrap(), None);

    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert!(read_replay_head(&mut source_tail, source_head, &memory, source_decode_bound, &cancelled).await.unwrap().is_none());
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(source_tail.replay().unwrap().poll_head(Duration::ZERO).await.unwrap(), Some(source_head));
    let record = read_replay_head(&mut source_tail, source_head, &memory, source_decode_bound, &running).await.unwrap().unwrap();
    assert_eq!(record.raw.metadata, source_metadata);
    assert_eq!(memory.used_bytes(), memory.limit_bytes());
    let message = SourceMessage::decode(&record.raw.payload).unwrap().unwrap();
    drop(record.raw);
    assert!(matches!(message, SourceMessage::LegacyTransaction(bytes) if bytes.len() == source_payload_len));
    assert_eq!(memory.used_bytes(), source_decode_bytes);
    drop(record.decode_reservation);
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(source_tail.replay().unwrap().poll_head(Duration::ZERO).await.unwrap(), None);

    let legacy = ReadOnlyLocalLog::open(directory.path().join("REPLICA_LOG"), ReadOnlyLocalLogOptions::default()).unwrap();
    let tail = crate::logs::PartitionReplay::Legacy(legacy.tail(LogOffset(-1)).await.unwrap());
    let (_, result) = start_poll(tail, Duration::ZERO).await;
    let TailPoll::Legacy(batch) = result.unwrap() else { panic!("expected legacy batch delivery"); };
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].metadata, replica_metadata);
    let message = ReplicaEnvelope::decode(&batch[0].payload).unwrap().unwrap();
    assert_eq!(message.term_sequence, Some(TermSequence(0)));

    writers.cancel();
    source_writer.await.unwrap().unwrap();
    replica_writer.await.unwrap().unwrap();
    source.close_and_wait().await;
    replica.close_and_wait().await;
}

#[tokio::test]
async fn replay_admission_refusal_keeps_the_read_position_and_reports_the_budget_error() {
    let mut fixture = fixture(ReplicationConfig::default(), 1 << 27).await;
    let opening = raw(0, 1, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }));
    fixture.replica.enqueue(opening.payload.clone(), opening.metadata.timestamp).await.unwrap().durable().await.unwrap();
    fixture.driver.consume(opening).await.unwrap();
    let read_position = fixture.handle.status().replica_position;
    assert_eq!(fixture.handle.status().highest_term, Term(1));
    let next = raw(1, 1, 1, resolved(0, 0));
    let needed = 256 + replica_decode_bound(next.payload.len()).unwrap();
    let next_metadata = fixture.replica.enqueue(next.payload, next.metadata.timestamp).await.unwrap().durable().await.unwrap();
    let blocker = fixture.memory.reserve(fixture.memory.limit_bytes() - fixture.memory.used_bytes() - needed + 1).unwrap();

    let error = tokio::time::timeout(Duration::from_secs(5), fixture.driver.run()).await.unwrap().unwrap_err();
    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    assert_eq!(fixture.handle.progress().fault(), Some(error));
    assert_eq!(fixture.handle.status().replica_position, read_position);
    assert_eq!(fixture.handle.status().highest_term, Term(1));
    assert_eq!(fixture.replica.latest().unwrap(), next_metadata.offset);
    drop(blocker);
    let mut retry = fixture.replica.tail(LogOffset(0)).await.unwrap();
    assert_eq!(retry.poll(Duration::ZERO).await.unwrap()[0].metadata, next_metadata);
}

#[tokio::test]
async fn shutting_down_a_live_leader_joins_the_append_pump_without_an_ingestion_fault() {
    for election_seed in 0..64 {
        let fixture = fixture(ReplicationConfig { assertion_interval: Duration::from_millis(1), election_seed, ..Default::default() }, 1 << 27).await;
        let mut status = fixture.handle.watch_status();
        let driver = tokio::spawn(fixture.driver.run());
        tokio::time::timeout(Duration::from_secs(5), async {
            while !matches!(status.borrow_and_update().role, Role::Leading(_)) { status.changed().await.unwrap(); }
        }).await.unwrap();
        fixture.handle.shutdown();
        tokio::time::timeout(Duration::from_secs(5), driver).await.unwrap().unwrap().unwrap();
        assert!(fixture.handle.progress().fault().is_none());
        assert_eq!(fixture.handle.status().role, Role::Stopped);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admitted_replica_fault_reaches_an_unmet_waiter_and_preserves_the_applied_prefix() {
    let fixture = fixture(ReplicationConfig { may_lead: false, ..Default::default() }, 1 << 27).await;
    let handle = fixture.handle.clone();
    let driver = tokio::spawn(fixture.driver.run());
    let cancellation = CancellationToken::new();
    let claim = ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() });
    for record in [raw(0, 1, 0, claim), raw(1, 1, 1, resolved(0, 0))] {
        fixture.replica.enqueue(record.payload, record.metadata.timestamp).await.unwrap().durable().await.unwrap();
    }
    let applied = tokio::time::timeout(Duration::from_secs(5), handle.await_tx(TxId(0), &cancellation))
        .await.unwrap().unwrap().unwrap();

    let unmet = handle.await_tx(TxId(1), &cancellation);
    tokio::pin!(unmet);
    assert!(futures::poll!(&mut unmet).is_pending());
    let record = raw(2, 1, 2, upload(0, 0));
    fixture.replica.enqueue(record.payload, record.metadata.timestamp).await.unwrap().durable().await.unwrap();

    let waiter_error = tokio::time::timeout(Duration::from_secs(5), unmet).await.unwrap().unwrap_err();
    let driver_error = tokio::time::timeout(Duration::from_secs(5), driver).await.unwrap().unwrap().unwrap_err();
    assert_eq!(driver_error, Anomaly::fault("rtdb.replication/invalid-transition", "Block upload has no matching pending boundary"));
    assert_eq!(waiter_error, driver_error);
    assert_eq!(handle.progress().fault().as_ref(), Some(&driver_error));
    assert_eq!(handle.status().role, Role::Stopped);
    assert_eq!(handle.await_tx(TxId(0), &cancellation).await.unwrap().unwrap().tx_key(), applied.tx_key());
}

#[tokio::test]
async fn skipped_malformed_arrow_keeps_diagnostics_and_orders_the_original_attempt() {
    let mut fixture = fixture(ReplicationConfig { skipped_transactions: [SourcePosition(0)].into_iter().collect(), ..Default::default() }, 1 << 27).await;
    let scope = rtdb_types::PlacedAttemptScope::new("deployment/1/replica", 7).unwrap();
    Arc::get_mut(&mut fixture.driver.resolve).unwrap().owned_witness_scope = Some(scope);
    let seed = scope.issue();
    use rtdb_format::envelope::{SourceBody, SourceMessage};

    let diagnostic = proto::Tx {
        tx_ops: Some(vec![1, 2, 3].into()), default_tz: Some("UTC".into()),
        user: Some("alice".into()), ..Default::default()
    };
    let message = SourceMessage::Protobuf(SourceBody::Tx(proto::Tx {
        user_metadata: Some(vec![4, 5, 6].into()), placed_attempt_seed: Some(seed.encode().to_vec().into()),
        bounded_retry: Some(proto::BoundedRetry {
            nonce: Some(b"0123456789abcdef0123456789abcdef".to_vec()),
            not_after_micros: Some(100_000),
        }),
        ..diagnostic.clone()
    }));
    let expected = SourceMessage::Protobuf(SourceBody::Tx(diagnostic)).encode().unwrap();
    let handle = fixture.handle.clone();
    let driver = tokio::spawn(fixture.driver.run());
    let position = handle.submit_source(&message, Timestamp(100)).await.unwrap();
    assert_eq!(position, SourcePosition(0));
    let result = tokio::time::timeout(Duration::from_secs(5), handle.await_tx(TxId(0), &CancellationToken::new())).await.unwrap().unwrap().unwrap();
    assert_eq!(result.error().unwrap().code, "xtdb/skipped-tx");
    assert_eq!(result.tx_key().system_time, Timestamp(100));
    assert_eq!(fixture.store.get(&rtdb_storage::ObjectKey::new("skipped-txs/00").unwrap()).await.unwrap().as_ref(), expected.as_slice());
    let latest = fixture.replica.latest().unwrap().0;
    let installed = fixture.replica.read_range(LogOffset(0), LogOffset(latest + 1)).await.unwrap().into_iter()
        .find_map(|raw| match ReplicaEnvelope::decode(&raw.payload).unwrap().unwrap().message {
            ReplicaBody::ResolvedTx(resolved) => Some(resolved),
            _ => None,
        }).unwrap();
    let attempt = rtdb_types::PlacedAttempt::decode(installed.placed_attempt.as_deref().unwrap()).unwrap();
    assert_eq!(attempt, seed.identify(position, Some("alice")).unwrap());
    assert!(installed.table_data.contains_key("xt/placed_attempt_outcomes"));
    handle.shutdown();
    driver.await.unwrap().unwrap();
}

#[tokio::test]
async fn disabled_indexing_retains_submission_without_starting_replica_consumption() {
    let fixture = fixture(ReplicationConfig { enabled: false, ..Default::default() }, 1 << 27).await;
    let handle = fixture.handle.clone();
    let driver = tokio::spawn(fixture.driver.run());
    let source = SourceTransaction::new(Vec::new(), SubmitOptions::default(), &fixture.memory).unwrap();
    let position = handle.submit_source_encoded(source.encode().unwrap().into_bytes(), Timestamp(1)).await.unwrap();
    assert_eq!(position, SourcePosition(0));
    let cancellation = CancellationToken::new();
    assert!(tokio::time::timeout(Duration::from_millis(20), handle.await_source(position, &cancellation)).await.is_err());
    assert_eq!(handle.progress().current().source_position, SourcePosition(-1));
    assert_eq!(fixture.replica.latest().unwrap(), LogOffset(-1));
    handle.shutdown();
    driver.await.unwrap().unwrap();
}

#[tokio::test]
async fn held_records_replay_below_the_fence_after_a_higher_term_upload() {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, ..Default::default() }, 1 << 27).await;
    fixture.driver.consume(raw(0, 1, 0, boundary(0, 0))).await.unwrap();
    let pending = fixture.driver.pending_block.as_ref().unwrap();
    fixture.blocks.produce(&pending.prepared, &pending.boundary, BTreeMap::new()).await.unwrap();
    fixture.driver.consume(raw(1, 2, 0, resolved(7, 1))).await.unwrap();
    assert_eq!(fixture.handle.progress().current().tx_id(), TxId(-1));
    fixture.driver.consume(raw(2, 2, 1, upload(0, 0))).await.unwrap();
    assert!(fixture.driver.pending_block.is_none());
    assert_eq!(fixture.handle.progress().current().tx_id(), TxId(7));
    assert_eq!(fixture.handle.progress().current().source_position, SourcePosition(1));
    fixture.driver.consume(raw(3, 2, 2, resolved(8, 2))).await.unwrap();
    assert_eq!(fixture.handle.progress().current().tx_id(), TxId(8));
    assert_eq!(fixture.driver.fence.position(), TermPosition::Expecting(TermSequence(3)));
}

#[tokio::test]
async fn catalog_cut_tracks_the_captured_catalog_after_a_pending_block_drains() {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, ..Default::default() }, 1 << 27).await;
    let partition = Arc::clone(&fixture.driver.partition);
    let table = TableRef::new("public", "docs");
    assert_eq!(partition.open_snapshot(None).catalog_cut(), None);
    assert_eq!(fixture.handle.status().installed.map(|prefix| prefix.replica), None);
    fixture.driver.consume(raw(0, 1, 0, resolved(0, 0))).await.unwrap();
    let before = partition.open_snapshot(None);
    assert_eq!(before.catalog_cut().unwrap().through, ReplicaPosition(0));
    fixture.driver.consume(raw(1, 1, 1, boundary(0, 0))).await.unwrap();
    let pending = fixture.driver.pending_block.as_ref().unwrap();
    fixture.blocks.produce(&pending.prepared, &pending.boundary, BTreeMap::new()).await.unwrap();
    let added = ReplicaBody::TriesAdded(proto::TriesAdded {
        storage_version: Some(CURRENT_STORAGE_VERSION.0), storage_epoch: Some(0), source_msg_id: Some(1),
        tries: vec![proto::TrieDetails {
            table_name: Some(table.schema_and_table()), trie_key: Some("l00-rc-b00".into()), data_file_size: Some(1),
            ..Default::default()
        }],
    });
    fixture.driver.consume(raw(2, 1, 2, added)).await.unwrap();
    let held = partition.open_snapshot(None);
    assert_eq!(fixture.handle.status().replica_position, ReplicaPosition(2));
    assert_eq!(fixture.handle.status().installed.map(|prefix| prefix.replica), Some(ReplicaPosition(0)));
    assert_eq!(held.catalog_cut(), before.catalog_cut());
    assert!(held.trie_entries(&table).is_empty());

    fixture.driver.consume(raw(3, 1, 3, upload(0, 0))).await.unwrap();
    let installed = partition.open_snapshot(None);
    assert_eq!(installed.catalog_cut().unwrap().through, ReplicaPosition(3));
    assert_eq!(fixture.handle.status().installed.map(|prefix| prefix.replica), Some(ReplicaPosition(3)));
    assert_eq!(installed.trie_entries(&table).len(), 1);
    assert_eq!(held.catalog_cut().unwrap().through, ReplicaPosition(0));
    assert!(held.trie_entries(&table).is_empty());
}

#[tokio::test]
async fn catalog_cut_cannot_cross_an_unrecognized_replica_body() {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, ..Default::default() }, 1 << 27).await;
    let partition = Arc::clone(&fixture.driver.partition);
    fixture.driver.consume(raw(0, 1, 0, resolved(0, 0))).await.unwrap();
    let mut unknown = raw(1, 1, 1, resolved(1, 1));
    unknown.payload = unknown.payload.slice(..1);
    fixture.driver.consume(unknown).await.unwrap();
    fixture.driver.consume(raw(2, 1, 1, resolved(2, 2))).await.unwrap();

    assert_eq!(fixture.handle.status().replica_position, ReplicaPosition(2));
    assert_eq!(fixture.handle.status().installed.map(|prefix| prefix.replica), Some(ReplicaPosition(0)));
    assert_eq!(partition.snapshot().tx_basis().unwrap().tx_id, TxId(2));
    assert_eq!(partition.snapshot().catalog_cut().unwrap().through, ReplicaPosition(0));
}

#[tokio::test]
async fn catalog_cut_advances_when_resolution_releases_deferred_records() {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, external_source: true, ..Default::default() }, 1 << 27).await;
    let partition = Arc::clone(&fixture.driver.partition);
    fixture.driver.consume(raw(0, 1, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }))).await.unwrap();
    fixture.driver.promote().await.unwrap();
    let receipt = fixture.handle.term().unwrap()
        .submit_external(Box::new(CommitExternal), Some(Timestamp(100)), None).await.unwrap();
    let command = fixture.driver.commands.try_recv().unwrap();
    fixture.driver.command(command).await.unwrap();
    let table = TableRef::new("public", "docs");
    let deleted = ReplicaBody::TriesDeleted(proto::TriesDeleted {
        table_name: Some(table.schema_and_table()), trie_keys: vec!["l00-rc-b00".into()],
    });
    fixture.driver.consume(raw(1, 1, 1, deleted)).await.unwrap();
    assert_eq!(fixture.driver.leader.as_ref().unwrap().held.len(), 1);
    assert_eq!(fixture.handle.status().replica_position, ReplicaPosition(1));
    assert_eq!(fixture.handle.status().installed.map(|prefix| prefix.replica), Some(ReplicaPosition(0)));
    assert_eq!(partition.open_snapshot(None).catalog_cut().unwrap().through, ReplicaPosition(0));

    let resolution = &mut fixture.driver.leader.as_mut().unwrap().resolution;
    let (resolver, result) = super::pending_resolution(resolution).await;
    fixture.driver.resolution_finished(resolver, result).await.unwrap();
    assert_eq!(fixture.handle.status().installed.map(|prefix| prefix.replica), Some(ReplicaPosition(1)));
    assert_eq!(partition.open_snapshot(None).catalog_cut().unwrap().through, ReplicaPosition(1));
    fixture.driver.demote().await.unwrap();
    drop(receipt);
}

async fn queued_trie_removal(key: &str) -> (Fixture, TableRef, rtdb_format::keys::TrieKey, oneshot::Receiver<Result<Applied>>, LogRecord) {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, idle_heartbeat: false, ..Default::default() }, 1 << 27).await;
    let opening = raw(0, 1, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }));
    fixture.replica.enqueue(opening.payload.clone(), opening.metadata.timestamp).await.unwrap().durable().await.unwrap();
    fixture.driver.consume(opening).await.unwrap();
    fixture.driver.promote().await.unwrap();
    let table = TableRef::new("public", "docs");
    fixture.driver.partition.add_tries(&BTreeMap::from([(table.clone(), ["l01-rc-b00", "l01-rc-b01"].into_iter().map(|key| {
        rtdb_index::CatalogTrie::notification(key.parse().unwrap(), 1, None).unwrap()
    }).collect())]), Timestamp(100)).unwrap();
    let key: rtdb_format::keys::TrieKey = key.parse().unwrap();
    let (completion, receiver) = oneshot::channel();
    fixture.driver.command(super::Command::Delete {
        term: Term(1), table: table.clone(), keys: vec![key.clone()], completion,
    }).await.unwrap();
    let mut tail = fixture.replica.tail(LogOffset(0)).await.unwrap();
    let mut records = tail.poll(Duration::from_secs(5)).await.unwrap();
    assert_eq!(records.len(), 1);
    (fixture, table, key, receiver, records.pop().unwrap())
}

#[tokio::test]
async fn trie_removal_receipt_waits_for_the_installed_catalog_cut() {
    let (mut fixture, table, key, mut receipt, readback) = queued_trie_removal("l01-rc-b00").await;
    let record = fixture.logs.decode_replica(readback).unwrap().unwrap();
    let position = record.metadata.position;
    let (_, admitted) = fixture.driver.fence.admit_record(record).unwrap();
    fixture.driver.apply_admitted(admitted.unwrap()).await.unwrap();

    let view = fixture.driver.partition.value();
    assert!(!view.retains_trie(&table, &key));
    assert_eq!(view.snapshot().catalog_cut().unwrap().through, ReplicaPosition(0));
    assert!(matches!(receipt.try_recv(), Err(Empty)), "trie removal replied before the installed prefix");

    fixture.driver.read_position = position;
    fixture.driver.update_installed_prefix().unwrap();
    assert!(matches!(receipt.try_recv().unwrap().unwrap(), Applied::TrieRemoval(through) if through == position));
    assert_eq!(fixture.driver.partition.value().snapshot().catalog_cut().unwrap().through, position);
    fixture.driver.demote().await.unwrap();
}

#[tokio::test]
async fn trie_removal_receipt_refuses_a_remaining_live_trie() {
    let (mut fixture, table, key, receipt, readback) = queued_trie_removal("l01-rc-b01").await;
    fixture.driver.consume(readback).await.unwrap();
    assert_eq!(receipt.await.unwrap().unwrap_err().code, "rtdb.replication/trie-removal-unconfirmed");
    assert!(fixture.driver.partition.value().retains_trie(&table, &key));
    fixture.driver.demote().await.unwrap();
}

#[tokio::test]
async fn trie_removal_receipt_refuses_a_different_readback() {
    let (mut fixture, table, key, receipt, mut readback) = queued_trie_removal("l01-rc-b00").await;
    let mut envelope = ReplicaEnvelope::decode(&readback.payload).unwrap().unwrap();
    let ReplicaBody::TriesDeleted(message) = &mut envelope.message else { panic!("expected a trie removal"); };
    message.trie_keys = vec!["l01-rc-b09".into()];
    readback.payload = envelope.encode().into();
    fixture.driver.consume(readback).await.unwrap();
    assert_eq!(receipt.await.unwrap().unwrap_err().code, "rtdb.replication/trie-removal-unconfirmed");
    assert!(fixture.driver.partition.value().retains_trie(&table, &key));
    fixture.driver.demote().await.unwrap();
}

#[tokio::test]
async fn wrong_epoch_upload_is_held_and_record_overflow_stops_apply() {
    let config = ReplicationConfig { may_lead: false, pending_block_records: 1, ..Default::default() };
    let mut fixture = fixture(config, 1 << 27).await;
    fixture.driver.consume(raw(0, 1, 0, boundary(0, 0))).await.unwrap();
    let mut wrong = match upload(0, 0) { ReplicaBody::BlockUploaded(message) => message, _ => unreachable!() };
    wrong.storage_epoch = Some(1);
    fixture.driver.consume(raw(1, 1, 1, ReplicaBody::BlockUploaded(wrong))).await.unwrap();
    let error = fixture.driver.consume(raw(2, 1, 2, resolved(1, 1))).await.unwrap_err();
    assert_eq!(error.code, "xtdb.indexer/follower-buffer-overflow");
    assert_eq!(fixture.driver.read_position, rtdb_types::ReplicaPosition(1));
    assert_eq!(fixture.handle.progress().current().tx_id(), TxId(-1));
}

#[tokio::test]
async fn sequence_gap_voids_the_term_and_later_records_cannot_apply() {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, ..Default::default() }, 1 << 27).await;
    fixture.driver.consume(raw(0, 1, 0, resolved(0, 0))).await.unwrap();
    fixture.driver.consume(raw(1, 1, 2, resolved(1, 1))).await.unwrap();
    fixture.driver.consume(raw(2, 1, 3, resolved(2, 2))).await.unwrap();
    assert_eq!(fixture.handle.progress().current().tx_id(), TxId(0));
    assert_eq!(fixture.driver.fence.position(), TermPosition::Voided);
    fixture.driver.consume(raw(3, 2, 0, resolved(3, 3))).await.unwrap();
    assert_eq!(fixture.handle.progress().current().tx_id(), TxId(3));
}

#[tokio::test]
async fn block_production_is_repeatable_and_adoption_folds_rows_once() {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, ..Default::default() }, 1 << 27).await;
    let state = &fixture.driver.partition;
    let key = TransactionKey { tx_id: TxId(0), system_time: Timestamp(100) };
    let mut write = TransactionBuffer::new(state.value().snapshot(), key, Arc::clone(&fixture.memory)).unwrap();
    let table = TableRef::new("public", "docs");
    for id in 0..8 { write.put(table.clone(), StructFields::from([("_id".into(), Value::I64(id)), ("v".into(), Value::I64(id))]), Interval::ALL).unwrap(); }
    state.apply(&write.stage().unwrap()).unwrap();
    fixture.handle.progress().applied(Some(SourcePosition(0)), Some(Arc::new(rtdb_tx::TransactionResult::Committed { tx_key: key }).into()), None).unwrap();
    fixture.driver.consume(raw(0, 1, 0, boundary(0, 0))).await.unwrap();
    let pending = fixture.driver.pending_block.as_ref().unwrap();
    #[derive(Default)]
    struct Seal(AtomicI64);
    #[async_trait]
    impl ServingSearchBuilder for Seal {
        async fn prepare(&self, _: rtdb_index::Snapshot, _: &CancellationToken) -> Result<Option<rtdb_index::ServingSearch>> { Ok(None) }
        async fn seal(&self, _: &Arc<rtdb_index::PartitionValue>, _: &TableRef, _: &rtdb_format::keys::TrieKey,
            _: &std::path::Path, _: u64, _: &CancellationToken) -> Result<Vec<proto::ArtefactRecord>> {
            let generation = self.0.fetch_add(1, Ordering::Relaxed);
            Ok(vec![proto::ArtefactRecord { kind: Some("test".into()), definition_id: Some("sealed".into()),
                definition_version: Some(generation), ..Default::default() }])
        }
    }
    let seal = Seal::default();
    let stop = CancellationToken::new();
    let first = fixture.blocks.produce_with_search(&pending.prepared, &pending.boundary, BTreeMap::new(), &stop, Some(&seal), None).await.unwrap();
    let root = rtdb_storage::ObjectKey::new(rtdb_format::keys::block_key(BlockIndex(0)).unwrap()).unwrap();
    fixture.store.delete(&root).await.unwrap();
    let second = fixture.blocks.produce_with_search(&pending.prepared, &pending.boundary, BTreeMap::new(), &stop, None, None).await.unwrap();
    assert_eq!(seal.0.load(Ordering::Relaxed), 1);
    assert_eq!(first.catalog, second.catalog);
    assert_eq!(first.tries, second.tries);
    assert_eq!(fixture.driver.partition.value().live().block_row_count(), 8);
    assert_eq!(fixture.driver.partition.value().table_catalog().block_index(), None);
    let mut uploaded = match upload(0, 0) { ReplicaBody::BlockUploaded(message) => message, _ => unreachable!() };
    uploaded.tries = first.tries.iter().flat_map(|(table, tries)| tries.iter().map(|trie| trie.to_proto(table))).collect();
    fixture.driver.consume(raw(1, 1, 1, ReplicaBody::BlockUploaded(uploaded))).await.unwrap();
    assert_eq!(fixture.driver.partition.value().table_catalog().metadata(&table).unwrap().row_count, 8);
    assert_eq!(fixture.driver.partition.value().live().block_row_count(), 0);
    use crate::BlockDiscovery::{At, From, FromPublished, List};
    let published = PublishedCheckpoint { block: BlockIndex(0), boundary: ReplicaPosition(0), root_sha256: None };
    for discovery in [List, From(BlockIndex(0)), From(BlockIndex(5)), FromPublished(published), At(BlockIndex(0))] {
        let recovered = fixture.blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(1000), discovery).await.unwrap();
        assert_eq!(recovered.block.as_ref().map(|block| block.block_index()), Some(BlockIndex(0)), "{discovery:?} finds the latest block");
        assert_eq!(recovered.progress.tx_key, Some(key));
        assert_eq!(recovered.fence.position(), TermPosition::Expecting(TermSequence(1)));
        assert_eq!(recovered.partition.value().table_catalog().metadata(&table).unwrap().row_count, 8);
    }
    let missing = fixture.blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(1000),
        FromPublished(PublishedCheckpoint { block: BlockIndex(5), boundary: ReplicaPosition(5), root_sha256: None }))
        .await.err().expect("an admitted published bound cannot fall back to an older block");
    assert_eq!(missing.code, "rtdb.replication/block-gone");
}

#[tokio::test]
async fn an_erasure_checkpoint_recovers_without_an_upload_confirmation() {
    use rtdb_format::{catalog::BlockCatalog, keys::block_key};
    use rtdb_storage::ObjectKey;

    let fixture = produced_memory_checkpoint(true).await;
    let root_key = ObjectKey::new(block_key(BlockIndex(0)).unwrap()).unwrap();
    let root = BlockCatalog::decode(&fixture.store.get(&root_key).await.unwrap()).unwrap();
    let published = PublishedCheckpoint {
        block: root.block_index(),
        boundary: root.boundary_replica_position().unwrap(),
        root_sha256: root.publication_sha256(),
    };
    let native = fixture.blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(0),
        BlockDiscovery::AtPublished(published)).await.unwrap();
    assert!(native.confirmation(StorageEpoch(0), CURRENT_STORAGE_VERSION, StorageEpoch(0), &fixture.memory).unwrap().is_some());
    let survivor = fixture.blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(0),
        BlockDiscovery::AtErasure(published)).await.unwrap();
    assert!(survivor.confirmation(StorageEpoch(0), CURRENT_STORAGE_VERSION, StorageEpoch(0), &fixture.memory).unwrap().is_none());
    assert_eq!(survivor.block.as_ref().unwrap().boundary_replica_position(), Some(published.boundary));
    let wrong = fixture.blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(0),
        BlockDiscovery::AtErasure(PublishedCheckpoint { root_sha256: None, ..published })).await;
    assert!(wrong.is_err());
}

#[tokio::test]
async fn hosted_checkpoint_recovery_preserves_its_admitted_journal_boundary() {
    use rtdb_format::{block::proto::Block, catalog::BlockCatalog, keys::block_key};
    use rtdb_storage::ObjectKey;

    let fixture = produced_memory_checkpoint(true).await;
    let published = PublishedCheckpoint { block: BlockIndex(0), boundary: ReplicaPosition(1), root_sha256: None };
    let snapshot = fixture.blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(0),
        BlockDiscovery::AtPublished(published)).await.unwrap();
    assert_eq!(snapshot.block.as_ref().unwrap().boundary_replica_position(), Some(published.boundary));
    let another_log = PartitionLogs::new(fixture.source.clone(), fixture.replica.clone(),
        StorageEpoch(0), StorageEpoch(1), CURRENT_STORAGE_VERSION, StorageEpoch(0), fixture.store.clone(),
        Arc::new(BufferPool::new(fixture.store.clone(), 1 << 20)), fixture.memory.clone()).unwrap();
    let wrong_log = fixture.blocks.recover_for_replay(snapshot, &another_log, TrieConfig::default(),
        Timestamp(0), &CancellationToken::new()).await;
    assert_eq!(wrong_log.err().map(|error| error.code), Some("rtdb.replication/invalid-block".into()),
        "the admitted snapshot cannot be installed against a different replica journal");
    let wrong = PublishedCheckpoint { boundary: ReplicaPosition(0), ..published };
    let attempt = fixture.blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(0), BlockDiscovery::AtPublished(wrong)).await;
    assert_eq!(attempt.err().map(|error| error.code), Some("rtdb.replication/invalid-block".into()),
        "the exact snapshot must match the boundary the directory admitted");
    let recovered = fixture.blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(0),
        BlockDiscovery::FromPublished(published)).await.unwrap();

    // The first recovered checkpoint loses its log suffix while its replacement becomes visible.
    for sequence in [2, 3] {
        let envelope = ReplicaEnvelope { term: Term(1), term_sequence: Some(TermSequence(sequence)),
            message: ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }) };
        fixture.logs.enqueue_replica(&envelope, BlockIndex(1), Timestamp(0)).await.unwrap().durable().await.unwrap();
    }
    fixture.replica.truncate_before(LogOffset(3)).unwrap();
    assert!(matches!(fixture.logs.recovery_tail(published.boundary).await, Err(rtdb_storage::Error::UnavailableLogRange { .. })));
    let foreign = ReplicaPosition::from_epoch_offset(StorageEpoch(1), 3).unwrap();
    let later = BlockCatalog::from_proto(Block {
        block_index: Some(1), boundary_replica_msg_id: Some(foreign.0), ..Default::default()
    }).unwrap();
    fixture.store.put(&ObjectKey::new(block_key(BlockIndex(1)).unwrap()).unwrap(), later.encode().into()).await.unwrap();
    let retry = fixture.blocks.recover_for_replay(recovered, &fixture.logs, TrieConfig::default(),
        Timestamp(0), &CancellationToken::new()).await;
    assert_eq!(retry.err().map(|error| error.code), Some("rtdb.replication/invalid-block".into()),
        "a replay retry must reject a successor from a different journal");
}

#[tokio::test]
async fn a_receipted_checkpoint_checks_the_exact_root_bytes_before_opening() {
    use rtdb_format::{catalog::{BlockCatalog, publication_digest}, keys::block_key};
    use rtdb_storage::{ObjectKey, ObjectStore};

    let fixture = produced_memory_checkpoint(true).await;
    let block = BlockIndex(0);
    let key = ObjectKey::new(block_key(block).unwrap()).unwrap();
    let bytes = fixture.store.get(&key).await.unwrap();
    let cached = fixture.blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(0), BlockDiscovery::List).await.unwrap();
    let catalog = cached.block.unwrap();
    let admitted = BlockDiscovery::AtPublished(PublishedCheckpoint {
        block,
        boundary: ReplicaPosition(1),
        root_sha256: Some(publication_digest(&bytes)),
    });
    assert_eq!(catalog.publication_sha256(), Some(publication_digest(&bytes)));
    fixture.blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(0), admitted).await.unwrap();

    let mut different = bytes.to_vec();
    different.extend_from_slice(&[0xa0, 0x06, 0x01]);
    assert_eq!(BlockCatalog::decode(&different).unwrap(), catalog);
    fixture.store.delete(&key).await.unwrap();
    fixture.store.put(&key, different.into()).await.unwrap();
    let error = fixture.blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(0), admitted)
        .await.err().expect("receipt validation must read the current root bytes");
    assert_eq!(error.message.as_deref(), Some("The stored checkpoint root does not match the directory receipt"));
}

#[tokio::test]
async fn a_receipted_checkpoint_accepts_only_a_directory_admitted_successor() {
    let fixture = produced_memory_checkpoint(true).await;
    let (catalog, _) = fixture.blocks.load(BlockIndex(0)).await.unwrap();
    let admitted = BlockDiscovery::FromPublished(PublishedCheckpoint {
        block: BlockIndex(0), boundary: ReplicaPosition(1), root_sha256: catalog.publication_sha256(),
    });
    let mut writer = produce_newer_memory_checkpoint(&fixture).await;
    deliver_checkpoint_record(&fixture, &mut writer, 2, 2, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() })).await.unwrap();
    let (later, _) = fixture.blocks.load(BlockIndex(1)).await.unwrap();
    let recovered = fixture.blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(0), admitted)
        .await.expect("an unadmitted stored successor cannot replace the receipt");
    assert_eq!(recovered.block.as_ref().unwrap().block_index(), BlockIndex(0));
    fixture.replica.truncate_before(LogOffset(4)).unwrap();
    let error = fixture.blocks.recover_for_replay(recovered, &fixture.logs, TrieConfig::default(), Timestamp(0), &CancellationToken::new())
        .await.err().expect("a suffix gap requires fresh directory admission");
    assert_eq!(error.code, "rtdb.storage/log-range-unavailable", "the selected receipt must survive a replay gap");

    let admitted = BlockDiscovery::FromPublished(PublishedCheckpoint {
        block: later.block_index(), boundary: later.boundary_replica_position().unwrap(), root_sha256: later.publication_sha256(),
    });
    let recovered = fixture.blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(0), admitted).await.unwrap();
    let recovered = fixture.blocks.recover_for_replay(recovered, &fixture.logs, TrieConfig::default(), Timestamp(0), &CancellationToken::new()).await.unwrap();
    assert_eq!(recovered.block.unwrap().block_index(), BlockIndex(1));
}

#[tokio::test]
async fn hosted_checkpoint_recovery_can_use_a_later_boundary_after_prefix_collection() {
    let fixture = produced_memory_checkpoint(true).await;
    let published = PublishedCheckpoint { block: BlockIndex(0), boundary: ReplicaPosition(1), root_sha256: None };
    let recovered = fixture.blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(0),
        BlockDiscovery::FromPublished(published)).await.unwrap();
    let mut writer = produce_newer_memory_checkpoint(&fixture).await;
    deliver_checkpoint_record(&fixture, &mut writer, 2, 2, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() })).await.unwrap();
    fixture.replica.truncate_before(LogOffset(4)).unwrap();
    let selected = fixture.blocks.recover_for_replay(recovered, &fixture.logs, TrieConfig::default(),
        Timestamp(0), &CancellationToken::new()).await.unwrap();
    assert_eq!(selected.block.unwrap().boundary_replica_position(), Some(ReplicaPosition(3)));
}

#[tokio::test]
async fn a_checkpoint_manifest_refuses_a_different_tables_valid_metadata() {
    use rtdb_storage::ObjectKey;
    use sha2::{Digest, Sha256};

    let mut fixture = fixture(ReplicationConfig { may_lead: false, ..Default::default() }, 1 << 20).await;
    let partition = &fixture.driver.partition;
    let key = TransactionKey { tx_id: TxId(0), system_time: Timestamp(100) };
    let mut write = TransactionBuffer::new(partition.value().snapshot(), key, Arc::clone(&fixture.memory)).unwrap();
    for (name, rows) in [("left_table", 1), ("right_table", 2)] {
        for id in 0..rows {
            write.put(TableRef::new("public", name), StructFields::from([("_id".into(), Value::I64(id))]), Interval::ALL).unwrap();
        }
    }
    partition.apply(&write.stage().unwrap()).unwrap();
    fixture.driver.consume(raw(0, 1, 0, boundary(0, 0))).await.unwrap();
    let pending = fixture.driver.pending_block.as_ref().unwrap();
    let produced = fixture.blocks.produce(&pending.prepared, &pending.boundary, BTreeMap::new()).await.unwrap();
    let entries = produced.catalog.table_entries().unwrap();
    let left = entries.iter().find(|entry| entry.table.table == "left_table").unwrap();
    let right = entries.iter().find(|entry| entry.table.table == "right_table").unwrap();
    let key_for = |entry: &rtdb_format::catalog::TableEntry| ObjectKey::new(entry.slug.block_key(BlockIndex(0)).unwrap()).unwrap();
    let left_key = key_for(left);
    let left_bytes = fixture.store.get(&left_key).await.unwrap();
    let right_bytes = fixture.store.get(&key_for(right)).await.unwrap();
    assert_ne!(left_bytes, right_bytes);
    let expected: [u8; 32] = Sha256::digest(&left_bytes).into();
    assert_eq!(produced.catalog.table_block_digest(left.oid), Some(expected.as_slice()),
        "a newly published root commits to the bytes written for each table OID");
    fixture.blocks.load(BlockIndex(0)).await.unwrap();

    fixture.store.delete(&left_key).await.unwrap();
    fixture.store.put(&left_key, right_bytes).await.unwrap();
    let cache = Arc::new(BufferPool::new(fixture.store.clone(), 1 << 20));
    let reopened = BlockStore::new(fixture.store.clone(), cache, fixture.memory.clone(), 100_000_000).unwrap();
    let recovered = reopened.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(1000), BlockDiscovery::List).await;
    let Err(error) = recovered else { panic!("recovery must refuse mismatched table metadata even when it decodes"); };
    assert_eq!(error.code, "rtdb.replication/invalid-block");
    assert!(error.message.unwrap().contains("does not match its checkpoint manifest"));
}

#[tokio::test]
async fn applying_a_deletion_drops_this_nodes_cached_copies_of_the_collected_files() {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, ..Default::default() }, 1 << 27).await;
    let state = &fixture.driver.partition;
    let key = TransactionKey { tx_id: TxId(0), system_time: Timestamp(100) };
    let mut write = TransactionBuffer::new(state.value().snapshot(), key, Arc::clone(&fixture.memory)).unwrap();
    let table = TableRef::new("public", "docs");
    write.put(table.clone(), StructFields::from([("_id".into(), Value::I64(1))]), Interval::ALL).unwrap();
    state.apply(&write.stage().unwrap()).unwrap();
    fixture.handle.progress().applied(Some(SourcePosition(0)), Some(Arc::new(rtdb_tx::TransactionResult::Committed { tx_key: key }).into()), None).unwrap();
    fixture.driver.consume(raw(0, 1, 0, boundary(0, 0))).await.unwrap();
    let pending = fixture.driver.pending_block.as_ref().unwrap();
    let produced = fixture.blocks.produce(&pending.prepared, &pending.boundary, BTreeMap::new()).await.unwrap();
    let mut uploaded = match upload(0, 0) { ReplicaBody::BlockUploaded(message) => message, _ => unreachable!() };
    uploaded.tries = produced.tries.iter().flat_map(|(table, tries)| tries.iter().map(|trie| trie.to_proto(table))).collect();
    fixture.driver.consume(raw(1, 1, 1, ReplicaBody::BlockUploaded(uploaded))).await.unwrap();
    let slug = fixture.driver.partition.value().table_catalog().entry(&table).unwrap().slug.clone();
    let collected: rtdb_format::keys::TrieKey = "l01-rc-b07".parse().unwrap();
    let data = rtdb_storage::ObjectKey::new(slug.data_key(&collected)).unwrap();
    fixture.store.put(&data, Bytes::from_static(b"collected")).await.unwrap();
    assert_eq!(fixture.driver.resolve.storage.get(&data).await.unwrap().as_slice(), b"collected");
    fixture.store.delete(&data).await.unwrap();
    assert!(fixture.driver.resolve.storage.get(&data).await.is_ok(), "until the deletion is applied, the cached copy is served");
    let deleted = ReplicaBody::TriesDeleted(proto::TriesDeleted { table_name: Some(table.schema_and_table()), trie_keys: vec![collected.to_string()] });
    fixture.driver.consume(raw(2, 1, 2, deleted)).await.unwrap();
    assert!(fixture.driver.resolve.storage.get(&data).await.is_err());
}

#[tokio::test]
async fn recovery_advances_to_the_source_epoch_floor_without_changing_the_persisted_boundary() {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, ..Default::default() }, 1 << 27).await;
    fixture.driver.consume(raw(0, 1, 0, resolved(7, 3))).await.unwrap();
    let token = vec![1, 2, 3];
    fixture.driver.consume(raw(1, 1, 1, ReplicaBody::BlockBoundary(proto::BlockBoundary {
        block_index: Some(0), latest_processed_msg_id: Some(4), external_source_token: Some(token.clone()),
    }))).await.unwrap();
    let pending = fixture.driver.pending_block.as_ref().unwrap();
    fixture.blocks.produce(&pending.prepared, &pending.boundary, BTreeMap::new()).await.unwrap();
    for (floor, expected) in [(SourcePosition(-1), SourcePosition(4)), (SourcePosition::from_epoch_offset(StorageEpoch(1), -1).unwrap(), SourcePosition::from_epoch_offset(StorageEpoch(1), -1).unwrap())] {
        let recovered = fixture.blocks.recover(TrieConfig::default(), floor, Timestamp(1000), crate::BlockDiscovery::List).await.unwrap();
        assert_eq!(recovered.progress.source_position, expected);
        assert_eq!(recovered.progress.tx_key, Some(TransactionKey { tx_id: TxId(7), system_time: Timestamp(800) }));
        assert_eq!(recovered.progress.external_token.as_deref(), Some(token.as_slice()));
        assert_eq!(recovered.block.as_ref().unwrap().latest_source_position(), Some(SourcePosition(4)));
        assert_eq!(recovered.fence.position(), TermPosition::Expecting(TermSequence(2)));
    }
}

#[tokio::test]
async fn oversized_payload_keeps_outer_admission_identity_and_fenced_missing_payload_is_not_fetched() {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, ..Default::default() }, 256).await;
    let envelope = ReplicaEnvelope { term: Term(1), term_sequence: Some(TermSequence(0)), message: ReplicaBody::TriesDeleted(proto::TriesDeleted { table_name: Some("public/docs".into()), trie_keys: (0..100).map(|index| rtdb_format::keys::TrieKey::l0(BlockIndex(index)).unwrap().to_string()).collect() }) };
    let Err(error) = fixture.logs.enqueue_replica_with(envelope.term, envelope.term_sequence, &envelope.message, BlockIndex(0), || Timestamp(100), false, |_| Ok(())).await
        else { panic!("an inline-only append must fail before emitting an oversized proxy"); };
    assert_eq!(error.code, "rtdb.storage/message-too-large");
    assert_eq!(fixture.replica.latest().unwrap(), LogOffset(-1));
    fixture.logs.enqueue_replica(&envelope, BlockIndex(0), Timestamp(100)).await.unwrap().durable().await.unwrap();
    let record = fixture.replica.read_range(LogOffset(0), LogOffset(1)).await.unwrap().pop().unwrap();
    assert!(matches!(ReplicaEnvelope::decode(&record.payload).unwrap().unwrap().message, ReplicaBody::OversizedMessage(_)));
    fixture.driver.consume(record).await.unwrap();
    assert_eq!(fixture.driver.fence.position(), TermPosition::Expecting(TermSequence(1)));
    fixture.driver.consume(raw(1, 2, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }))).await.unwrap();
    let missing = ReplicaBody::OversizedMessage(proto::OversizedMessage { storage_version: Some(CURRENT_STORAGE_VERSION.0), storage_epoch: Some(0), block_index: Some(0), payload_id: Some("missing".into()), ..Default::default() });
    fixture.driver.consume(raw(2, 1, 1, missing)).await.unwrap();
    assert_eq!(fixture.driver.read_position, rtdb_types::ReplicaPosition(2));
}

#[tokio::test]
async fn strict_dispatch_checks_the_clock_after_preparing_the_exact_frame() {
    let fixture = fixture(ReplicationConfig { may_lead: false, ..Default::default() }, 1 << 16).await;
    let position = SourcePosition(1);
    let attempt = rtdb_types::PlacedAttemptScope::new("test-replica", 1).unwrap().issue()
        .identify(position, Some("owner")).unwrap();
    let envelope = ReplicaEnvelope {
        term: Term(1),
        term_sequence: Some(TermSequence(1)),
        message: ReplicaBody::ResolvedTx(proto::ResolvedTx {
            tx_id: Some(position.0),
            src_msg_id: Some(position.0),
            system_time_micros: Some(100),
            committed: Some(true),
            placed_attempt: Some(attempt.encode().to_vec().into()),
            acceptance_stage: Some(proto::AcceptanceStage {
                clock_upper_micros: Some(120), cutoff_micros: Some(125), ..Default::default()
            }),
            ..Default::default()
        }),
    };
    let wall = AtomicI64::new(119);
    for sample in [118, 124, 130] {
        let Err(error) = fixture.logs.enqueue_replica_with(
            envelope.term, envelope.term_sequence, &envelope.message, BlockIndex(0),
            || Timestamp(wall.load(Ordering::Relaxed)), false,
            |_| { wall.store(sample, Ordering::Relaxed); Ok(()) },
        ).await else { panic!("An invalid dispatch clock must leave the replica log untouched"); };
        assert_eq!(error.code, crate::logs::STRICT_DISPATCH_CUTOFF);
        assert_eq!(fixture.replica.latest().unwrap(), LogOffset(-1));
        wall.store(119, Ordering::Relaxed);
    }

    assert!(fixture.logs.enqueue_replica(&envelope, BlockIndex(0), Timestamp(119)).await.is_err());
    assert_eq!(fixture.replica.latest().unwrap(), LogOffset(-1));
    let receipt = fixture.logs.enqueue_replica_with(
        envelope.term, envelope.term_sequence, &envelope.message, BlockIndex(0), || Timestamp(123), false, |_| Ok(()),
    ).await.unwrap();
    assert_eq!(receipt.durable().await.unwrap().position, ReplicaPosition(0));
}

#[tokio::test]
async fn retained_replay_requires_the_offload_even_when_readback_cached_it() {
    let fixture = fixture(ReplicationConfig { may_lead: false, ..Default::default() }, 256).await;
    let envelope = ReplicaEnvelope {
        term: Term(1), term_sequence: Some(TermSequence(0)),
        message: ReplicaBody::TriesDeleted(proto::TriesDeleted {
            table_name: Some("public/docs".into()),
            trie_keys: (0..100).map(|index| rtdb_format::keys::TrieKey::l0(BlockIndex(index)).unwrap().to_string()).collect(),
        }),
    };
    fixture.logs.enqueue_replica(&envelope, BlockIndex(0), Timestamp(100)).await.unwrap().durable().await.unwrap();
    let record = fixture.replica.read_range(LogOffset(0), LogOffset(1)).await.unwrap().pop().unwrap();
    let record = fixture.logs.decode_replica(record).unwrap().unwrap();
    let stored = record.envelope.oversized_payload(CURRENT_STORAGE_VERSION, StorageEpoch(0)).unwrap().unwrap();
    let cached = fixture.logs.resolve_oversized(record.clone()).await.unwrap();
    assert!(matches!(cached.envelope.message, ReplicaBody::TriesDeleted(_)));
    fixture.store.delete(&rtdb_storage::ObjectKey::new(stored).unwrap()).await.unwrap();
    assert!(fixture.logs.resolve_oversized(record.clone()).await.is_ok());
    assert_eq!(fixture.logs.resolve_oversized_fresh(record).await.unwrap_err().code, "rtdb.storage/missing-object");
}

#[tokio::test]
async fn cancelled_fresh_offload_read_keeps_its_delivery_until_provider_drain() {
    use rtdb_storage::{IoRegion, ObjectKey, StoredObject};

    struct HeldDelivery { bytes: Vec<u8>, _hold: Arc<()> }
    impl AsRef<[u8]> for HeldDelivery {
        fn as_ref(&self) -> &[u8] { &self.bytes }
    }

    struct Provider {
        region: Arc<IoRegion>,
        memory: Arc<MemoryStore>,
        gate: Mutex<Option<(oneshot::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
    }
    #[async_trait]
    impl ObjectStore for Provider {
        async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.memory.head(key).await }
        async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.memory.get(key).await }
        async fn get_range(&self, key: &ObjectKey, range: std::ops::Range<u64>) -> rtdb_storage::Result<Bytes> { self.memory.get_range(key, range).await }
        async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> { self.memory.put(key, bytes).await }
        async fn put_file(&self, key: &ObjectKey, path: &std::path::Path) -> rtdb_storage::Result<()> { self.memory.put_file(key, path).await }
        async fn list(&self, key: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> { self.memory.list(key).await }
        async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> { self.memory.delete(key).await }
        async fn get_fresh_exact(&self, key: &ObjectKey, length: usize) -> rtdb_storage::Result<Bytes> {
            let bytes = self.memory.get_fresh_exact(key, length).await?;
            let (started, release) = self.gate.lock().take().unwrap();
            self.region.run_blocking(move || {
                started.send(()).unwrap();
                let _ = release.recv();
                Ok(bytes)
            }).await
        }
    }

    let memory = Arc::new(MemoryStore::default());
    let region = IoRegion::new();
    let (started, entering) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let provider = Arc::new(Provider { region: Arc::clone(&region), memory: Arc::clone(&memory), gate: Mutex::new(Some((started, released))) });
    let cache = Arc::new(BufferPool::new(provider.clone(), 1 << 20));
    let logs = PartitionLogs::new(Arc::new(MemoryLog::new(1024)), Arc::new(MemoryLog::new(1024)), StorageEpoch(0), StorageEpoch(0),
        CURRENT_STORAGE_VERSION, StorageEpoch(0), provider, cache, MemoryBudget::new(1 << 20)).unwrap();
    let envelope = ReplicaEnvelope { term: Term(1), term_sequence: Some(TermSequence(0)), message: resolved(0, 0) };
    let bytes = envelope.encode();
    let proxy = envelope.offloaded(CURRENT_STORAGE_VERSION, StorageEpoch(0), BlockIndex(0), "held-read".into(), &bytes);
    let key = proxy.oversized_payload(CURRENT_STORAGE_VERSION, StorageEpoch(0)).unwrap().unwrap();
    memory.put(&ObjectKey::new(key).unwrap(), Bytes::from(bytes)).await.unwrap();
    let hold = Arc::new(());
    let released_hold = Arc::downgrade(&hold);
    let delivery = Bytes::from_owner(HeldDelivery { bytes: proxy.encode(), _hold: hold });
    let record = logs.decode_replica(LogRecord::new(LogMetadata { offset: LogOffset(0), timestamp: Timestamp(0) }, delivery)).unwrap().unwrap();
    let waiter = tokio::spawn(async move { logs.resolve_oversized_fresh(record).await });
    entering.await.unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert!(released_hold.upgrade().is_some());
    release.send(()).unwrap();
    region.close_and_wait().await;
    assert!(released_hold.upgrade().is_none());
}

#[tokio::test]
async fn staged_leader_and_decoding_follower_publish_the_same_transaction_prefixes() {
    let config = ReplicationConfig { may_lead: false, idle_heartbeat: false, ..Default::default() };
    let mut leader = fixture(config.clone(), 1 << 27).await;
    let mut follower = fixture(config, 1 << 27).await;
    let claim = ReplicaEnvelope {
        term: Term(1), term_sequence: Some(TermSequence(0)), message: ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }),
    };
    leader.logs.enqueue_replica(&claim, BlockIndex(0), Timestamp(99)).await.unwrap().durable().await.unwrap();
    let mut tail = leader.logs.replica_tail(leader.driver.read_position).await.unwrap();
    let claim = tail.poll(Duration::from_secs(5)).await.unwrap().pop().unwrap();
    leader.driver.consume(claim.clone()).await.unwrap();
    follower.driver.consume(claim).await.unwrap();
    leader.driver.promote().await.unwrap();

    let transactions: [(&[&str], bool); 5] = [
        (&["CREATE TABLE empty_docs (_id, v)"], true),
        (&["INSERT INTO docs (_id, v) VALUES (1, 7), (2, 8)", "INSERT INTO other_docs (_id, v) VALUES (1, 10)"], true),
        (&["INSERT INTO docs (_id, v) VALUES (3, 11)", "ASSERT FALSE"], false),
        (&["UPDATE docs SET v = 9 WHERE _id = 1"], true),
        (&["DELETE FROM docs WHERE _id = 2"], true),
    ];
    for (index, (statements, committed)) in transactions.into_iter().enumerate() {
        let position = SourcePosition(index as i64);
        let clock = Timestamp(100 + position.0);
        let context = Arc::clone(&leader.driver.resolve);
        let operations = statements.iter().map(|sql| TxOp::Sql { sql: (*sql).into(), args: None }).collect();
        let transaction = SourceTransaction::new(operations, SubmitOptions::default(), &leader.memory).unwrap();
        let snapshot = leader.driver.partition.value().snapshot();
        let resolved = leader.driver.leader.as_mut().unwrap().resolver_mut().unwrap()
            .resolve_source(snapshot, position, clock, transaction, &context).await.unwrap();
        assert_eq!(resolved.result().committed(), committed, "{statements:?}: {:?}", resolved.result());
        leader.driver.enqueue_resolved(resolved, None).await.unwrap();
        let records = tail.poll(Duration::from_secs(5)).await.unwrap();
        assert_eq!(records.len(), 1);
        let record = records.into_iter().next().unwrap();

        leader.driver.consume(record.clone()).await.unwrap();
        follower.driver.consume(record).await.unwrap();

        let expected = leader.driver.partition.value();
        let actual = follower.driver.partition.value();
        assert_eq!(expected.live().latest_completed_tx(), actual.live().latest_completed_tx(), "{statements:?}");
        assert_eq!(expected.snapshot().tables(), actual.snapshot().tables(), "{statements:?}");
        for (name, staged) in expected.live().tables() {
            let decoded = &actual.live().tables()[name];
            assert_eq!(staged.column_types(), decoded.column_types(), "{statements:?}, {name:?}");
            let events = |table: &rtdb_index::LiveTable| {
                table.batches().flat_map(|batch| (0..batch.len()).map(move |row| batch.view().read_event(row).unwrap()))
                    .collect::<Vec<_>>()
            };
            assert_eq!(events(staged), events(decoded), "{statements:?}, {name:?}");
        }
        let expected = leader.handle.progress().current();
        let actual = follower.handle.progress().current();
        assert_eq!(expected.source_position, position);
        assert_eq!(actual.source_position, position);
        assert_eq!(actual.tx_key, expected.tx_key);
        assert_eq!(actual.latest_result, expected.latest_result, "{statements:?}");
    }
    leader.driver.demote().await.unwrap();
}

#[tokio::test]
async fn source_transaction_reads_back_applies_flushes_and_recovers() {
    let fixture = fixture(ReplicationConfig { rows_per_block: 2, assertion_interval: Duration::from_millis(10), ..Default::default() }, 1 << 27).await;
    let handle = fixture.handle.clone();
    let blocks = Arc::clone(&fixture.blocks);
    let writer = tokio::spawn(fixture.driver.run());
    let transaction = SourceTransaction::new(vec![TxOp::PutDocs { table: TableRef::new("public", "docs"), documents: Documents::new(vec![StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), Value::Utf8("value".into()))])], &fixture.memory).unwrap(), valid_from: Some(Timestamp(0)), valid_to: None }], SubmitOptions::default(), &fixture.memory).unwrap();
    let position = handle.submit_source_encoded(transaction.encode().unwrap().into_bytes(), Timestamp(100)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), handle.await_tx(TxId(position.0), &CancellationToken::new())).await.unwrap().unwrap();
    let mut status = handle.watch_status();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop { if status.borrow_and_update().adopted_block.is_some() { break; } status.changed().await.unwrap(); }
    }).await.unwrap();
    assert!(status.borrow().last_block_upload_epoch_seconds > 0);
    handle.shutdown();
    writer.await.unwrap().unwrap();
    assert!(status.borrow().last_block_upload_epoch_seconds > 0);
    let recovered = blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(1000), crate::BlockDiscovery::List).await.unwrap();
    assert_eq!(recovered.progress.tx_key.unwrap().tx_id, TxId(position.0));
    assert_eq!(recovered.partition.value().table_catalog().metadata(&TableRef::new("public", "docs")).unwrap().row_count, 1);
}

fn serving_source_put(key: u8) -> rtdb_tx::transact::Transact {
    use rtdb_tx::transact::{Cell, Op, Row, Transact, Window};
    use rtdb_types::EntityId;

    Transact {
        key: vec![key; 16], require: Vec::new(), window: Window::default(), guards: Vec::new(), requires: Vec::new(),
        ops: vec![Op::Put { branch: "main".into(), table: TableRef::new("public", "intake"), if_absent: false,
            rows: vec![Row { id: EntityId::Integer(i64::from(key)), valid: None,
                cells: StructFields::from([("value".into(), Cell::Value(Value::I64(i64::from(key))))]), custody_group: None,
                protected_columns: Vec::new(), custody_payload: None }] }],
        otherwise: Vec::new(), custody_request_digest: None, request_memory: None,
    }
}

#[tokio::test]
async fn default_serving_recovers_a_lost_source_reply_and_consumes_retries_in_order() {
    use rtdb_crypt::tokens::VerifiedClaims;
    use rtdb_tx::transact::{Op, Outcome};
    use rtdb_types::rtdb_v1::At;

    let clock = Arc::new(AtomicI64::new(100));
    let sample = Arc::clone(&clock);
    let mut fixture = fixture_with_clock(
        ReplicationConfig { assertion_interval: Duration::from_millis(5), ..Default::default() },
        1 << 27, Some(Arc::new(move || Timestamp(sample.load(Ordering::Acquire)))),
    ).await;
    let serving = fixture.driver.attach_native_serving(fixture.handle.clone(), NativeServingAuthority {
        incarnation: [7; 16], checkpoints: None,
    }, false).await.unwrap();
    let request = serving_source_put(1);
    let disconnected = CancellationToken::new();
    let submit = serving.transact(request.clone(), VerifiedClaims::default(), None, &disconnected);
    tokio::pin!(submit);
    assert!(futures::poll!(&mut submit).is_pending());
    assert_eq!(fixture.logs.latest_source_position().unwrap(), SourcePosition(0));
    assert_eq!(fixture.handle.progress().current().source_position, SourcePosition(-1));
    assert_eq!(serving.view(At::Latest, &disconnected).await.unwrap().index.head().basis.position(), 0);
    disconnected.cancel();
    assert_eq!(submit.await.unwrap_err().code, "rtdb/transaction-outcome-unknown");

    let forged = fixture.handle.submit_source(
        &SourceMessage::Protobuf(SourceBody::ServingTransact(Bytes::copy_from_slice(request.canonical_request().unwrap().get().as_bytes()))),
        Timestamp(100),
    ).await.unwrap();
    assert_eq!(forged, SourcePosition(1));
    let between = fixture.handle.submit_source(&SourceMessage::Protobuf(SourceBody::FlushBlock(Default::default())), Timestamp(100)).await.unwrap();
    assert_eq!(between, SourcePosition(2));
    clock.store(200, Ordering::Release);
    let driver = tokio::spawn(fixture.driver.run());
    let cancellation = CancellationToken::new();
    let (receipt, conflict) = tokio::time::timeout(Duration::from_secs(5), async {
        let mut other = request.clone();
        other.ops = vec![Op::DropBranch { branch: "different".into() }];
        tokio::join!(
            serving.transact(request, VerifiedClaims::default(), None, &cancellation),
            serving.transact(other, VerifiedClaims::default(), None, &cancellation),
        )
    }).await.unwrap();
    let receipt = receipt.unwrap();
    assert!(receipt.replayed);
    assert_eq!(receipt.outcome, Outcome::Then);
    assert_eq!(receipt.system_time, Timestamp(200));
    assert_eq!(receipt.basis.position(), 1);
    assert_eq!(conflict.unwrap_err().code, "rtdb/idempotency-key");
    assert_eq!(fixture.handle.progress().current().source_position, SourcePosition(4));
    assert_eq!(serving.view(At::Latest, &cancellation).await.unwrap().index.head().basis, receipt.basis);
    fixture.handle.shutdown();
    driver.await.unwrap().unwrap();
}

#[tokio::test]
async fn new_term_refuses_a_durable_timed_original_before_resuming_the_source() {
    use rtdb_crypt::tokens::VerifiedClaims;
    use rtdb_tx::transact::Outcome;

    let interval = Duration::from_millis(5);
    let mut fixture = fixture(ReplicationConfig { assertion_interval: interval, idle_heartbeat: false, ..Default::default() }, 1 << 27).await;
    let authority = || NativeServingAuthority { incarnation: [7; 16], checkpoints: None };
    let serving = fixture.driver.attach_native_serving(fixture.handle.clone(), authority(), false).await.unwrap();
    let mut request = serving_source_put(1);
    request.window.before = Some(Timestamp::MAX);
    let disconnected = CancellationToken::new();
    let submit = serving.transact(request.clone(), VerifiedClaims::default(), None, &disconnected);
    tokio::pin!(submit);
    assert!(futures::poll!(&mut submit).is_pending());
    assert_eq!(fixture.logs.latest_source_position().unwrap(), SourcePosition(0));
    disconnected.cancel();
    assert_eq!(submit.await.unwrap_err().code, "rtdb/transaction-outcome-unknown");

    let opening = raw(0, 1, 0, ReplicaBody::NoOp(proto::NoOp::default()));
    fixture.replica.enqueue(opening.payload.clone(), opening.metadata.timestamp).await.unwrap().durable().await.unwrap();
    fixture.driver.consume(opening).await.unwrap();
    fixture.driver.promote().await.unwrap();
    let mut source = fixture.source.read_range(LogOffset(0), LogOffset(1)).await.unwrap();
    fixture.driver.leader.as_mut().unwrap().source_batch.push_back(source.pop().unwrap());
    fixture.driver.resolve_next_source().await.unwrap();
    let mut replica_tail = fixture.replica.tail(LogOffset(0)).await.unwrap();
    let original = next_local_record(replica_tail.as_mut()).await;
    let envelope = ReplicaEnvelope::decode(&original.payload).unwrap().unwrap();
    assert!(matches!(envelope.message, ReplicaBody::NoOp(noop) if noop.src_msg_id.is_none() && noop.serving_record.is_some()));
    assert_eq!(fixture.handle.progress().current().source_position, SourcePosition(-1));
    fixture.driver.demote().await.unwrap();

    let (handle, mut driver) = local_recovery_driver(fixture.source.clone(), fixture.replica.clone(), fixture.store.clone(), true, interval).await;
    let serving = driver.attach_native_serving(handle.clone(), authority(), false).await.unwrap();
    let running = tokio::spawn(driver.run());
    let cancellation = CancellationToken::new();
    tokio::time::timeout(Duration::from_secs(5), handle.await_source(SourcePosition(0), &cancellation)).await.unwrap().unwrap();
    assert!(matches!(handle.status().role, Role::Leading(term) if term > Term(1)));
    let refusal = serving.tx_status(request.key.clone(), None, &cancellation).await.unwrap();
    assert!(matches!(&refusal.outcome, Outcome::Refused { error } if error.code == "rtdb/timed-transaction-refused"));
    assert_eq!(refusal.basis.position(), 2);
    let retry = serving.transact(request, VerifiedClaims::default(), None, &cancellation).await.unwrap();
    assert_eq!(retry, refusal);
    let next = serving.transact(serving_source_put(2), VerifiedClaims::default(), None, &cancellation).await.unwrap();
    assert_eq!(next.basis.position(), 3);
    assert_eq!(handle.progress().current().source_position, SourcePosition(2));
    handle.shutdown();
    running.await.unwrap().unwrap();
}

#[tokio::test]
async fn the_leader_records_artefacts_of_live_tries_and_deletes_the_files_of_entries_for_tries_it_does_not_hold() {
    let fixture = fixture(ReplicationConfig { rows_per_block: 2, assertion_interval: Duration::from_millis(10), ..Default::default() }, 1 << 27).await;
    let (handle, partition, store) = (fixture.handle.clone(), Arc::clone(&fixture.driver.partition), Arc::clone(&fixture.store));
    let writer = tokio::spawn(fixture.driver.run());
    let table = TableRef::new("public", "docs");
    let transaction = SourceTransaction::new(vec![TxOp::PutDocs { table: table.clone(), documents: Documents::new(vec![StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), Value::Utf8("value".into()))])], &fixture.memory).unwrap(), valid_from: Some(Timestamp(0)), valid_to: None }], SubmitOptions::default(), &fixture.memory).unwrap();
    handle.submit_source_encoded(transaction.encode().unwrap().into_bytes(), Timestamp(100)).await.unwrap();
    let mut status = handle.watch_status();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop { if status.borrow_and_update().adopted_block.is_some() { break; } status.changed().await.unwrap(); }
    }).await.unwrap();
    let value = partition.value();
    let live = value.trie_catalog().current_tries(&table)[0].key.clone();
    let slug = value.table_catalog().entry(&table).unwrap().slug.clone();
    let record = |id: &str| proto::ArtefactRecord { kind: Some("search".into()), definition_id: Some(id.into()), definition_version: Some(1), stats: None, scope: None, file_size: None, published_version: None, pack_members: Vec::new() };
    let unknown: rtdb_format::keys::TrieKey = "l01-rc-b07".parse().unwrap();
    let file = |key: &rtdb_format::keys::TrieKey, id: &str| rtdb_storage::ObjectKey::new(slug.artefact_key(key, "search", id, 1).unwrap()).unwrap();
    for key in [file(&live, "body"), file(&unknown, "body"), file(&live, "title")] { store.put(&key, Bytes::from_static(b"artefact")).await.unwrap(); }
    let entry = |key: &rtdb_format::keys::TrieKey, id: &str| proto::ArtefactEntry { trie_key: Some(key.to_string()), record: Some(record(id)) };
    let current = proto::ArtefactsAdded { storage_version: Some(CURRENT_STORAGE_VERSION.0), storage_epoch: Some(0), table_name: Some(table.schema_and_table()),
        entries: vec![entry(&live, "body"), entry(&unknown, "body")], source_msg_id: None };
    handle.publish_artefacts(current, &CancellationToken::new()).await.unwrap();
    let stale = proto::ArtefactsAdded { storage_version: Some(CURRENT_STORAGE_VERSION.0 + 1), storage_epoch: Some(0), table_name: Some(table.schema_and_table()),
        entries: vec![entry(&live, "title")], source_msg_id: None };
    handle.publish_artefacts(stale, &CancellationToken::new()).await.unwrap();
    let malformed = proto::ArtefactsAdded { storage_version: Some(CURRENT_STORAGE_VERSION.0), storage_epoch: Some(0), table_name: Some(table.schema_and_table()),
        entries: vec![proto::ArtefactEntry { trie_key: Some("not-a-trie".into()), record: Some(record("body")) }], source_msg_id: None };
    handle.publish_artefacts(malformed, &CancellationToken::new()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async { while store.head(&file(&unknown, "body")).await.is_ok() { tokio::task::yield_now().await; } }).await.unwrap();
    let catalog = partition.value();
    let trie = catalog.trie_catalog().iter_tries(&table).find(|trie| trie.key == live).unwrap().clone();
    assert_eq!(trie.artefacts, vec![record("body")]);
    assert!(store.head(&file(&live, "body")).await.is_ok());
    assert!(store.head(&file(&live, "title")).await.is_ok(), "an entry under another storage version is dropped, and the key this node would compute for it left alone");
    let removal = |version: i32| proto::ArtefactsRemoved { storage_version: Some(version), storage_epoch: Some(0), table_name: Some(table.schema_and_table()),
        entries: vec![entry(&live, "body")], source_msg_id: None };
    handle.remove_artefacts(removal(CURRENT_STORAGE_VERSION.0 + 1), &CancellationToken::new()).await.unwrap();
    assert_eq!(partition.value().trie_catalog().iter_tries(&table).find(|trie| trie.key == live).unwrap().artefacts, vec![record("body")], "a removal under another storage version removes nothing");
    handle.remove_artefacts(removal(CURRENT_STORAGE_VERSION.0), &CancellationToken::new()).await.unwrap();
    assert!(partition.value().trie_catalog().iter_tries(&table).find(|trie| trie.key == live).unwrap().artefacts.is_empty());
    handle.shutdown();
    writer.await.unwrap().unwrap();
}

async fn local_recovery_driver(
    source: Arc<dyn Log>, replica: Arc<dyn Log>, store: Arc<MemoryStore>, may_lead: bool, assertion_interval: Duration,
) -> (ReplicationHandle, ReplicationDriver) {
    let memory = MemoryBudget::new(1 << 28);
    let cache = Arc::new(BufferPool::new(store.clone(), 1 << 25));
    let blocks = BlockStore::new(store.clone(), Arc::clone(&cache), Arc::clone(&memory), 100_000_000).unwrap();
    let recovered = blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(0), crate::BlockDiscovery::List).await.unwrap();
    let progress = ProgressMonitor::new(recovered.progress.clone());
    let logs = PartitionLogs::new(source, replica, StorageEpoch(0), StorageEpoch(0), CURRENT_STORAGE_VERSION, StorageEpoch(0),
        store, Arc::clone(&cache), Arc::clone(&memory)).unwrap();
    let resolve = Arc::new(ResolveContext { decision_clock: None, write_event_origins: false, placed_receipt_retirement: false, owned_witness_scope: None, world_constraints: None, database: "xtdb".into(), storage: cache, memory, query_options: Default::default(),
        scan_accelerators: Default::default(),
        cancellation: CancellationToken::new(), default_tz: "UTC".into(), authorization: Arc::new(AllowRoles),
        execution_observer: None, tracer: None });
    ReplicationDriver::new(ReplicationConfig { may_lead, assertion_interval, ..Default::default() },
        logs, blocks, recovered, progress, resolve, Arc::new(NoEffects), CancellationToken::new()).unwrap()
}

async fn next_local_record(tail: &mut dyn rtdb_storage::log::LogTail) -> LogRecord {
    let mut records = tail.poll(Duration::from_secs(5)).await.unwrap();
    assert_eq!(records.len(), 1);
    records.pop().unwrap()
}

async fn submit_recovery_row(handle: &ReplicationHandle, memory: &Arc<MemoryBudget>, id: i64, time: i64) -> SourcePosition {
    let documents = Documents::new(vec![StructFields::from([("_id".into(), Value::I64(id))])], memory).unwrap();
    let tx = SourceTransaction::new(vec![TxOp::PutDocs {
        table: TableRef::new("public", "docs"), documents, valid_from: Some(Timestamp(0)), valid_to: None,
    }], SubmitOptions::default(), memory).unwrap();
    handle.submit_source_encoded(tx.encode().unwrap().into_bytes(), Timestamp(time)).await.unwrap()
}

#[tokio::test]
async fn native_local_restart_confirms_missing_retained_blocks_in_order_for_a_surviving_follower() {
    for (legacy_suffix, persisted_successors) in [(false, 0), (true, 0), (true, 1), (true, 2)] {
        let directory = tempfile::tempdir().unwrap();
        let source_path = directory.path().join("LOG");
        let replica_path = directory.path().join("REPLICA_LOG");
        let options = || LocalLogOptions { maximum_message_bytes: 1 << 20, queue_capacity: 8 };
        let (source, source_driver) = LocalLog::open(&source_path, options()).unwrap();
        let (replica, replica_driver) = LocalLog::open(&replica_path, options()).unwrap();
        let writers = CancellationToken::new();
        let source_writer = tokio::spawn(source_driver.run(writers.clone()));
        let replica_writer = tokio::spawn(replica_driver.run(writers.clone()));
        let store = Arc::new(MemoryStore::default());
        let read_only_source = ReadOnlyLocalLog::open(&source_path, ReadOnlyLocalLogOptions::default()).unwrap();
        let read_only_replica = ReadOnlyLocalLog::open(&replica_path, ReadOnlyLocalLogOptions::default()).unwrap();
        let interval = Duration::from_secs(3600);
        let (follower_handle, follower) = local_recovery_driver(read_only_source, read_only_replica, Arc::clone(&store), false, interval).await;
        let follower_partition = Arc::clone(&follower.partition);
        let follower_task = tokio::spawn(follower.run());
        let (old_handle, mut old) = local_recovery_driver(source.clone(), replica.clone(), Arc::clone(&store), true, interval).await;
        let mut old_tail = old.logs.replica_tail(old.read_position).await.unwrap();

        old.start_claim().unwrap();
        let result = super::pending_claim(&mut old.claiming).await;
        old.claim_finished(result).await.unwrap();
        old.consume(next_local_record(old_tail.as_mut()).await).await.unwrap();
        assert_eq!(old.role(), Role::Leading(Term(1)));

        let first = submit_recovery_row(&old_handle, &old.resolve.memory, 1, 100).await;
        let mut source_tail = old.logs.source_tail(SourcePosition(-1)).await.unwrap();
        let source_record = next_local_record(source_tail.as_mut()).await;
        old.leader.as_mut().unwrap().source_batch.push_back(source_record);
        old.resolve_next_source().await.unwrap();
        let (resolver, result) = super::pending_resolution(&mut old.leader.as_mut().unwrap().resolution).await;
        old.resolution_finished(resolver, result).await.unwrap();
        old.consume(next_local_record(old_tail.as_mut()).await).await.unwrap();

        old.cut().await.unwrap();
        let boundary_record = next_local_record(old_tail.as_mut()).await;
        let boundary_offset = boundary_record.metadata.offset;
        old.consume(boundary_record).await.unwrap();
        let mut production = old.leader.as_mut().unwrap().production.take();
        let super::ProductionResult::Written(Ok(produced)) = super::pending_production(&mut production).await else {
            panic!("expected the catalog to be persisted before the upload append");
        };
        drop(production);
        assert_eq!(replica.latest().unwrap(), boundary_offset);
        assert_eq!(produced.catalog.latest_source_position(), Some(first));
        let expected_tries: Vec<_> = produced.tries.iter().flat_map(|(table, tries)| tries.iter().map(|trie| trie.to_proto(table))).collect();
        assert!(!expected_tries.is_empty());
        old.demote().await.unwrap();
        let mut follower_status = follower_handle.watch_status();
        tokio::time::timeout(Duration::from_secs(5), async {
            while follower_status.borrow_and_update().replica_position.0 < boundary_offset.0 { follower_status.changed().await.unwrap(); }
        }).await.unwrap();
        assert!(follower_handle.status().adopted_block.is_none());

        let after = if legacy_suffix {
            let legacy = submit_recovery_row(&old_handle, &old.resolve.memory, 10, 200).await;
            for (seq, message) in [
                (0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() })),
                (1, ReplicaBody::ResolvedTx(proto::ResolvedTx {
                    tx_id: Some(legacy.0), system_time_micros: Some(200), committed: Some(true),
                    src_msg_id: Some(legacy.0), ..Default::default()
                })),
                (2, ReplicaBody::BlockBoundary(proto::BlockBoundary {
                    block_index: Some(1), latest_processed_msg_id: Some(legacy.0), external_source_token: Some(vec![1]),
                })),
            ] {
                old.logs.enqueue_replica(&ReplicaEnvelope { term: Term(2), term_sequence: Some(TermSequence(seq)), message }, BlockIndex(1), Timestamp(200))
                    .await.unwrap().durable().await.unwrap();
                if seq == 0 && persisted_successors > 0 {
                    let ReplicaBody::BlockUploaded(mut fenced) = upload(0, first.0) else { unreachable!(); };
                    fenced.tries = expected_tries.clone();
                    old.logs.enqueue_replica(&ReplicaEnvelope { term: Term(1), term_sequence: Some(TermSequence(3)), message: ReplicaBody::BlockUploaded(fenced) },
                        BlockIndex(1), Timestamp(200)).await.unwrap().durable().await.unwrap();
                }
            }
            if persisted_successors > 0 {
                let (_, mut pre_repair_reader) = local_recovery_driver(source.clone(), replica.clone(), Arc::clone(&store), false, interval).await;
                let mut tail = pre_repair_reader.logs.replica_tail(pre_repair_reader.read_position).await.unwrap();
                let first_tip = replica.latest().unwrap();
                while pre_repair_reader.read_position.0 < first_tip.0 {
                    let records = tail.poll(Duration::from_secs(5)).await.unwrap();
                    assert!(!records.is_empty());
                    for record in records {
                        pre_repair_reader.consume(record).await.unwrap();
                    }
                }
                let pending = pre_repair_reader.pending_block.as_ref().unwrap();
                let block = pre_repair_reader.blocks.produce(&pending.prepared, &pending.boundary, BTreeMap::new()).await.unwrap();
                assert_eq!(block.catalog.block_index(), BlockIndex(1));
                if persisted_successors > 1 {
                    let ReplicaBody::BlockUploaded(mut confirmed) = upload(1, legacy.0) else { unreachable!(); };
                    confirmed.tries = block.tries.iter().flat_map(|(table, tries)| tries.iter().map(|trie| trie.to_proto(table))).collect();
                    confirmed.external_source_token = Some(vec![1]);
                    let later_boundary = ReplicaBody::BlockBoundary(proto::BlockBoundary {
                        block_index: Some(2), latest_processed_msg_id: Some(legacy.0), external_source_token: Some(vec![2]),
                    });
                    for (sequence, message) in [(3, ReplicaBody::BlockUploaded(confirmed)), (4, later_boundary)] {
                        pre_repair_reader.logs.enqueue_replica(&ReplicaEnvelope { term: Term(2), term_sequence: Some(TermSequence(sequence)), message },
                            BlockIndex(2), Timestamp(200)).await.unwrap().durable().await.unwrap();
                        pre_repair_reader.consume(next_local_record(tail.as_mut()).await).await.unwrap();
                    }
                    let pending = pre_repair_reader.pending_block.as_ref().unwrap();
                    let block = pre_repair_reader.blocks.produce(&pending.prepared, &pending.boundary, BTreeMap::new()).await.unwrap();
                    assert_eq!(block.catalog.block_index(), BlockIndex(2));
                }
            }
            replica.latest().unwrap()
        } else { boundary_offset };
        drop((old, old_handle, old_tail, source_tail));
        writers.cancel();
        source_writer.await.unwrap().unwrap();
        replica_writer.await.unwrap().unwrap();
        source.close_and_wait().await;
        replica.close_and_wait().await;
        drop((source, replica));

        let (source, source_driver) = LocalLog::open(&source_path, options()).unwrap();
        let (replica, replica_driver) = LocalLog::open(&replica_path, options()).unwrap();
        let writers = CancellationToken::new();
        let source_writer = tokio::spawn(source_driver.run(writers.clone()));
        let replica_writer = tokio::spawn(replica_driver.run(writers.clone()));
        let (restarted_handle, restarted) = local_recovery_driver(source.clone(), replica.clone(), store, true, Duration::from_millis(10)).await;
        let second = submit_recovery_row(&restarted_handle, &restarted.resolve.memory, 2, 300).await;
        let mut output = replica.tail(after).await.unwrap();
        let restarted_task = tokio::spawn(restarted.run());
        let cancellation = CancellationToken::new();
        tokio::time::timeout(Duration::from_secs(5), async {
            for handle in [&restarted_handle, &follower_handle] {
                let tx = handle.await_tx(TxId(second.0), &cancellation).await.unwrap().unwrap();
                assert!(tx.committed());
                assert_eq!(tx.tx_key().tx_id, TxId(second.0));
                let progress = handle.progress().current();
                let token: Option<&[u8]> = if legacy_suffix {
                    Some(if persisted_successors > 1 { &[2] } else { &[1] })
                } else { None };
                assert_eq!(progress.source_position, second);
                assert_eq!(progress.external_token.as_deref(), token);
            }
        }).await.unwrap();
        let term = if legacy_suffix { Term(3) } else { Term(2) };
        let applied = tokio::time::timeout(Duration::from_secs(5), async {
            let mut applied = Vec::new();
            loop {
                for record in output.poll(Duration::from_secs(1)).await.unwrap() {
                    let envelope = ReplicaEnvelope::decode(&record.payload).unwrap().unwrap();
                    assert_eq!(envelope.term, term);
                    match envelope.message {
                        ReplicaBody::NoOp(_) => (),
                        ReplicaBody::BlockUploaded(upload) => {
                            if upload.block_index == Some(0) {
                                assert_eq!(envelope.term_sequence, Some(TermSequence(1)));
                                assert_eq!(upload.latest_processed_msg_id, Some(first.0));
                                assert_eq!(upload.tries, expected_tries);
                            }
                            applied.push((upload.block_index.unwrap(), None));
                        }
                        ReplicaBody::ResolvedTx(tx) if tx.tx_id == Some(second.0) => {
                            assert_eq!(tx.committed, Some(true));
                            applied.push((-1, tx.tx_id));
                            return applied;
                        }
                        _ => panic!("unexpected replica record"),
                    }
                }
            }
        }).await.unwrap();
        let mut expected = vec![(0, None)];
        if legacy_suffix { expected.push((persisted_successors.max(1), None)); }
        expected.push((-1, Some(second.0)));
        assert_eq!(applied, expected);
        let follower_value = follower_partition.value();
        let docs = TableRef::new("public", "docs");
        assert_eq!(follower_value.table_catalog().metadata(&docs).unwrap().row_count, 1);
        assert_eq!(follower_value.live().latest_completed_tx().unwrap().tx_id, TxId(second.0));
        let live = &follower_value.live().tables()[&docs];
        assert_eq!(live.row_count(), 1);
        let rtdb_index::StoredOperation::Put(document) = live.row(0).unwrap().event().unwrap().operation else {
            panic!("expected the follower to import the follow-on put");
        };
        assert_eq!(document.get("_id"), Some(&Value::I64(2)));

        restarted_handle.shutdown();
        follower_handle.shutdown();
        restarted_task.await.unwrap().unwrap();
        follower_task.await.unwrap().unwrap();
        writers.cancel();
        source_writer.await.unwrap().unwrap();
        replica_writer.await.unwrap().unwrap();
        source.close_and_wait().await;
        replica.close_and_wait().await;
    }
}

async fn produced_memory_checkpoint(sequenced: bool) -> Fixture {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, ..Default::default() }, 1 << 20).await;
    for (seq, message) in [(0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() })), (1, boundary(0, -1))] {
        let envelope = ReplicaEnvelope { term: Term(1), term_sequence: sequenced.then_some(TermSequence(seq)), message };
        fixture.logs.enqueue_replica(&envelope, BlockIndex(0), Timestamp(0)).await.unwrap().durable().await.unwrap();
        fixture.driver.consume(raw_with_envelope(seq, &envelope)).await.unwrap();
    }
    let pending = fixture.driver.pending_block.as_ref().unwrap();
    fixture.blocks.produce(&pending.prepared, &pending.boundary, BTreeMap::new()).await.unwrap();
    fixture
}

fn raw_with_envelope(position: i64, envelope: &ReplicaEnvelope) -> LogRecord {
    LogRecord::new(LogMetadata { offset: LogOffset(position), timestamp: Timestamp(0) }, envelope.encode().into())
}

async fn reopen_memory_checkpoint(fixture: &Fixture) -> (ReplicationHandle, ReplicationDriver) {
    reopen_memory_checkpoint_with_config(fixture, ReplicationConfig::default()).await
}

async fn reopen_memory_checkpoint_with_config(fixture: &Fixture, config: ReplicationConfig) -> (ReplicationHandle, ReplicationDriver) {
    let recovered = fixture.blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(0), crate::BlockDiscovery::List).await.unwrap();
    assert!(recovered.confirmation(StorageEpoch(1), CURRENT_STORAGE_VERSION, StorageEpoch(0), &fixture.memory).unwrap().is_none());
    let progress = ProgressMonitor::new(recovered.progress.clone());
    ReplicationDriver::new(config, Arc::clone(&fixture.logs), Arc::clone(&fixture.blocks), recovered,
        progress, Arc::clone(&fixture.driver.resolve), Arc::new(NoEffects), CancellationToken::new()).unwrap()
}

#[tokio::test]
async fn promotion_requires_the_recovered_checkpoint_to_still_be_stored() {
    let fixture = produced_memory_checkpoint(true).await;
    let (handle, mut restarted) = reopen_memory_checkpoint(&fixture).await;
    let key = rtdb_storage::ObjectKey::new(rtdb_format::keys::block_key(BlockIndex(0)).unwrap()).unwrap();
    let root = fixture.store.get(&key).await.unwrap();
    fixture.store.delete(&key).await.unwrap();
    restarted.fence.admit(Term(2), Some(TermSequence(0))).unwrap();
    restarted.try_promote().await.unwrap();
    assert!(restarted.leader.is_none(), "a missing recovered root cannot be submitted for confirmation");
    assert_eq!(handle.status().promotion_failure.unwrap().code, "rtdb.storage/missing-object");
    assert_eq!(fixture.replica.latest().unwrap(), LogOffset(1));

    fixture.store.put(&key, root).await.unwrap();
    restarted.fence.admit(Term(3), Some(TermSequence(0))).unwrap();
    restarted.try_promote().await.unwrap();
    assert!(restarted.leader.as_ref().unwrap().recovery_write.is_some(), "the restored root can be confirmed");
    restarted.demote().await.unwrap();
}

#[tokio::test]
async fn a_chosen_upload_cannot_confirm_different_catalog_bytes() {
    let fixture = produced_memory_checkpoint(true).await;
    let (_, mut restarted) = reopen_memory_checkpoint(&fixture).await;
    let mut root = restarted.recovered_confirmation.as_ref().unwrap().catalog.publication_sha256().unwrap();
    root[0] ^= 1;
    let ReplicaBody::BlockUploaded(upload) = upload(0, -1) else { unreachable!(); };
    let envelope = ReplicaEnvelope {
        term: Term(1), term_sequence: Some(TermSequence(2)),
        message: ReplicaBody::BlockUploaded(proto::BlockUploaded { catalog_sha256: Some(root.to_vec()), ..upload }),
    };
    let error = restarted.consume(raw_with_envelope(2, &envelope)).await.unwrap_err();
    assert_eq!(error.code, "rtdb.replication/checkpoint-digest");
    assert!(restarted.recovered_confirmation.is_some());
}

#[tokio::test]
async fn a_recovered_checkpoint_uses_only_matching_admitted_confirmation_and_accepts_legacy_epoch_zero() {
    for sequenced in [false, true] {
        let fixture = produced_memory_checkpoint(sequenced).await;
        let (handle, mut restarted) = reopen_memory_checkpoint(&fixture).await;
        let before = restarted.partition.value();
        let ReplicaBody::BlockUploaded(complete) = upload(0, -1) else { unreachable!(); };
        let wrong = [
            proto::BlockUploaded { storage_version: None, ..complete.clone() },
            proto::BlockUploaded { storage_epoch: Some(2), ..complete.clone() },
            proto::BlockUploaded { block_index: None, ..complete.clone() },
            proto::BlockUploaded { latest_processed_msg_id: None, ..complete.clone() },
            proto::BlockUploaded { external_source_token: Some(Vec::new()), ..complete.clone() },
        ];
        let matching = proto::BlockUploaded { storage_epoch: None, ..complete };
        for (index, confirmation) in (2_i64..).zip(wrong.into_iter().chain([matching])) {
            let envelope = ReplicaEnvelope {
                term: Term(1), term_sequence: sequenced.then_some(TermSequence(index)), message: ReplicaBody::BlockUploaded(confirmation),
            };
            fixture.logs.enqueue_replica(&envelope, BlockIndex(1), Timestamp(0)).await.unwrap().durable().await.unwrap();
            let mut records = fixture.replica.read_range(LogOffset(index), LogOffset(index + 1)).await.unwrap();
            restarted.consume(records.pop().unwrap()).await.unwrap();
            assert_eq!(restarted.recovered_confirmation.is_some(), index < 7);
        }
        assert!(restarted.recovered_confirmation.is_none());
        let current = restarted.partition.value();
        assert!(Arc::ptr_eq(current.trie_catalog(), before.trie_catalog()));
        assert!(Arc::ptr_eq(current.table_catalog(), before.table_catalog()));
        assert!(Arc::ptr_eq(current.live(), before.live()));
        assert_eq!(current.snapshot().catalog_cut().unwrap().through, ReplicaPosition(7));
        assert_eq!(handle.progress().current().source_position, SourcePosition(-1));
        assert!(handle.progress().current().tx_key.is_none());

        restarted.fence.admit(Term(2), Some(TermSequence(0))).unwrap();
        restarted.promote().await.unwrap();
        assert_eq!(restarted.role(), Role::Leading(Term(2)));
        assert!(restarted.leader.as_ref().unwrap().recovery_write.is_none());
        restarted.demote().await.unwrap();
    }
}

#[tokio::test]
async fn a_recovered_checkpoint_reannounces_when_an_older_term_upload_was_fenced() {
    let fixture = produced_memory_checkpoint(true).await;
    let (_handle, mut restarted) = reopen_memory_checkpoint(&fixture).await;
    let claim = ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() });
    for (index, term, sequence, message) in [(2, 2, 0, claim.clone()), (3, 1, 2, upload(0, -1)), (4, 3, 0, claim)] {
        let envelope = ReplicaEnvelope { term: Term(term), term_sequence: Some(TermSequence(sequence)), message };
        fixture.logs.enqueue_replica(&envelope, BlockIndex(1), Timestamp(0)).await.unwrap().durable().await.unwrap();
        let mut records = fixture.replica.read_range(LogOffset(index), LogOffset(index + 1)).await.unwrap();
        if index == 4 { restarted.claim = Some(rtdb_types::ReplicaPosition(index)); }
        restarted.consume(records.pop().unwrap()).await.unwrap();
        assert!(restarted.recovered_confirmation.is_some());
    }
    assert_eq!(restarted.role(), Role::Following);
    assert!(!restarted.leader.as_ref().unwrap().accepting());
    let mut tail = fixture.replica.tail(LogOffset(4)).await.unwrap();
    let mut batch = tail.poll(Duration::from_secs(5)).await.unwrap();
    let repair = batch.remove(0);
    assert_eq!(ReplicaEnvelope::decode(&repair.payload).unwrap().unwrap().term_sequence, Some(TermSequence(1)));
    restarted.consume(repair).await.unwrap();
    assert!(restarted.recovered_confirmation.is_none());
    assert_eq!(restarted.role(), Role::Leading(Term(3)));
    restarted.demote().await.unwrap();
}

#[tokio::test]
async fn a_recovered_checkpoint_does_not_replace_missing_required_replica_records_with_a_claim() {
    let fixture = produced_memory_checkpoint(true).await;
    for sequence in [2, 3] {
        let envelope = ReplicaEnvelope { term: Term(1), term_sequence: Some(TermSequence(sequence)), message: ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }) };
        fixture.logs.enqueue_replica(&envelope, BlockIndex(1), Timestamp(1)).await.unwrap().durable().await.unwrap();
    }
    fixture.replica.truncate_before(LogOffset(3)).unwrap();
    let (handle, restarted) = reopen_memory_checkpoint(&fixture).await;
    let error = restarted.run().await.unwrap_err();
    assert_eq!(error, Anomaly::from(rtdb_storage::Error::UnavailableLogRange { requested: 2, first_retained: 3 }));
    assert_eq!(fixture.replica.latest().unwrap(), LogOffset(3));
    assert_eq!(handle.status().replica_position, rtdb_types::ReplicaPosition(1));
}

async fn deliver_checkpoint_record(fixture: &Fixture, driver: &mut ReplicationDriver, term: i64, sequence: i64, message: ReplicaBody) -> Result<()> {
    let envelope = ReplicaEnvelope { term: Term(term), term_sequence: Some(TermSequence(sequence)), message };
    let position = fixture.logs.enqueue_replica(&envelope, BlockIndex(1), Timestamp(0)).await.unwrap().durable().await.unwrap().position;
    let mut records = fixture.replica.read_range(LogOffset(position.0), LogOffset(position.0 + 1)).await.unwrap();
    driver.consume(records.pop().unwrap()).await
}

async fn produce_newer_memory_checkpoint(fixture: &Fixture) -> ReplicationDriver {
    let (_, mut prior) = reopen_memory_checkpoint_with_config(fixture, ReplicationConfig {
        may_lead: false, ..Default::default()
    }).await;
    deliver_checkpoint_record(fixture, &mut prior, 2, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() })).await.unwrap();
    deliver_checkpoint_record(fixture, &mut prior, 2, 1, boundary(1, -1)).await.unwrap();
    let pending = prior.pending_block.as_ref().unwrap();
    let selected = prior.blocks.produce(&pending.prepared, &pending.boundary, BTreeMap::new()).await.unwrap();
    assert_eq!(selected.catalog.boundary_replica_position(), Some(rtdb_types::ReplicaPosition(3)));
    prior
}

#[tokio::test]
async fn promotion_ignores_a_deleted_historical_catalog_still_present_in_the_disk_cache() {
    let fixture = produced_memory_checkpoint(true).await;
    produce_newer_memory_checkpoint(&fixture).await;
    let directory = tempfile::tempdir().unwrap();
    let disk = rtdb_storage::disk_cache::DiskCache::open(directory.path(), 1 << 20, Arc::clone(&fixture.memory)).unwrap();
    let old_catalog = rtdb_storage::ObjectKey::new(rtdb_format::keys::block_key(BlockIndex(0)).unwrap()).unwrap();
    let cached = disk.scope(rtdb_storage::ObjectKey::new("recovery").unwrap(), fixture.store.clone()).unwrap();
    let buffer = Arc::new(BufferPool::new(Arc::clone(&cached), 1 << 20));
    let (handle, mut writer) = reopen_memory_checkpoint_with_config(&fixture, ReplicationConfig {
        assertion_interval: Duration::from_secs(3600), ..Default::default()
    }).await;
    writer.blocks = BlockStore::new(Arc::clone(&cached), buffer, Arc::clone(&fixture.memory), 100_000_000).unwrap();
    writer.historical_confirmations = writer.prove_historical_confirmations().await.unwrap();
    assert_eq!(writer.historical_confirmations.len(), 1);
    assert_eq!(writer.historical_confirmations[0].catalog.block_index(), BlockIndex(0));

    fixture.store.delete(&old_catalog).await.unwrap();
    assert!(cached.head(&old_catalog).await.is_ok());
    writer.claim = Some(rtdb_types::ReplicaPosition(4));
    deliver_checkpoint_record(&fixture, &mut writer, 3, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() })).await.unwrap();
    assert!(writer.historical_confirmations.is_empty());
    assert_eq!(writer.role(), Role::Following);
    let mut tail = fixture.replica.tail(LogOffset(4)).await.unwrap();
    let actual = next_local_record(tail.as_mut()).await;
    let envelope = ReplicaEnvelope::decode(&actual.payload).unwrap().unwrap();
    assert_eq!(envelope.term_sequence, Some(TermSequence(1)));
    let ReplicaBody::BlockUploaded(upload) = envelope.message else { panic!("expected a recovered block upload"); };
    assert_eq!(upload.block_index, Some(1));
    writer.consume(actual).await.unwrap();
    assert_eq!(handle.status().role, Role::Leading(Term(3)));
    writer.demote().await.unwrap();
    disk.close_and_wait().await;
}

#[tokio::test]
async fn recovery_confirms_a_later_anchor_when_the_oldest_catalog_precedes_the_log_floor() {
    let fixture = produced_memory_checkpoint(true).await;
    produce_newer_memory_checkpoint(&fixture).await;
    let (_, mut last) = reopen_memory_checkpoint_with_config(&fixture, ReplicationConfig {
        may_lead: false, ..Default::default()
    }).await;
    deliver_checkpoint_record(&fixture, &mut last, 3, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() })).await.unwrap();
    deliver_checkpoint_record(&fixture, &mut last, 3, 1, boundary(2, -1)).await.unwrap();
    let pending = last.pending_block.as_ref().unwrap();
    let latest = last.blocks.produce(&pending.prepared, &pending.boundary, BTreeMap::new()).await.unwrap();
    assert_eq!(latest.catalog.boundary_replica_position(), Some(rtdb_types::ReplicaPosition(5)));
    fixture.replica.truncate_before(LogOffset(4)).unwrap();

    let (handle, mut writer) = reopen_memory_checkpoint_with_config(&fixture, ReplicationConfig {
        assertion_interval: Duration::from_secs(3600), ..Default::default()
    }).await;
    let restored = writer.partition.value();
    let progress = handle.progress().current();
    writer.historical_confirmations = writer.prove_historical_confirmations().await.unwrap();
    assert_eq!(writer.historical_confirmations.len(), 1);
    assert_eq!(writer.historical_confirmations[0].catalog.block_index(), BlockIndex(1));
    assert!(Arc::ptr_eq(&writer.partition.value(), &restored));
    assert_eq!(handle.progress().current().source_position, progress.source_position);
    assert_eq!(handle.progress().current().tx_key, progress.tx_key);

    writer.claim = Some(rtdb_types::ReplicaPosition(6));
    deliver_checkpoint_record(&fixture, &mut writer, 4, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() })).await.unwrap();
    let mut tail = fixture.replica.tail(LogOffset(6)).await.unwrap();
    for (index, sequence) in [(1, 1), (2, 2)] {
        assert_eq!(writer.role(), Role::Following);
        let raw = next_local_record(tail.as_mut()).await;
        let envelope = ReplicaEnvelope::decode(&raw.payload).unwrap().unwrap();
        assert_eq!(envelope.term, Term(4));
        assert_eq!(envelope.term_sequence, Some(TermSequence(sequence)));
        let ReplicaBody::BlockUploaded(upload) = envelope.message else { panic!("expected a recovered block upload"); };
        assert_eq!(upload.block_index, Some(index));
        writer.consume(raw).await.unwrap();
    }
    assert_eq!(handle.status().role, Role::Leading(Term(4)));
    assert_eq!(handle.status().adopted_block.unwrap().block_index(), BlockIndex(2));
    writer.demote().await.unwrap();
}

#[tokio::test]
async fn historical_confirmation_recovers_an_older_block_at_a_legacy_selected_boundary() {
    let fixture = produced_memory_checkpoint(true).await;
    let (_, mut prior) = reopen_memory_checkpoint_with_config(&fixture, ReplicationConfig {
        may_lead: false, ..Default::default()
    }).await;
    let mut legacy_tail = fixture.replica.tail(LogOffset(1)).await.unwrap();
    for message in [ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }), boundary(1, -1)] {
        let envelope = ReplicaEnvelope { term: Term(2), term_sequence: None, message };
        fixture.logs.enqueue_replica(&envelope, BlockIndex(1), Timestamp(0)).await.unwrap().durable().await.unwrap();
        prior.consume(next_local_record(legacy_tail.as_mut()).await).await.unwrap();
    }
    let pending = prior.pending_block.as_ref().unwrap();
    let selected = prior.blocks.produce(&pending.prepared, &pending.boundary, BTreeMap::new()).await.unwrap();
    assert_eq!(selected.catalog.boundary_replica_position(), Some(rtdb_types::ReplicaPosition(3)));
    assert_eq!(selected.catalog.boundary_term(), Term(2));
    assert_eq!(selected.catalog.boundary_term_sequence(), None);

    let (handle, mut writer) = reopen_memory_checkpoint_with_config(&fixture, ReplicationConfig {
        assertion_interval: Duration::from_secs(3600), ..Default::default()
    }).await;
    let startup = writer.fence.clone();
    let restored = writer.partition.value();
    assert_eq!(startup.position(), TermPosition::Unknown);
    writer.historical_confirmations = writer.prove_historical_confirmations().await.unwrap();
    assert_eq!(writer.historical_confirmations.len(), 1);
    assert_eq!(writer.historical_confirmations[0].catalog.block_index(), BlockIndex(0));
    assert_eq!(writer.fence, startup);
    assert!(Arc::ptr_eq(&writer.partition.value(), &restored));

    let mut tail = writer.logs.replica_tail(writer.read_position).await.unwrap();
    writer.start_claim().unwrap();
    let durable = super::pending_claim(&mut writer.claiming).await;
    writer.claim_finished(durable).await.unwrap();
    assert_eq!(writer.fence, startup);
    let claim = next_local_record(tail.as_mut()).await;
    let envelope = ReplicaEnvelope::decode(&claim.payload).unwrap().unwrap();
    assert_eq!(envelope.term, Term(3));
    assert_eq!(envelope.term_sequence, Some(TermSequence(0)));
    writer.consume(claim).await.unwrap();
    assert_eq!(writer.fence.position(), TermPosition::Expecting(TermSequence(1)));
    for (index, sequence) in [(0, 1), (1, 2)] {
        assert_eq!(writer.role(), Role::Following);
        let raw = next_local_record(tail.as_mut()).await;
        let envelope = ReplicaEnvelope::decode(&raw.payload).unwrap().unwrap();
        assert_eq!(envelope.term, Term(3));
        assert_eq!(envelope.term_sequence, Some(TermSequence(sequence)));
        let ReplicaBody::BlockUploaded(upload) = envelope.message else { panic!("expected a recovered block upload"); };
        assert_eq!(upload.block_index, Some(index));
        writer.consume(raw).await.unwrap();
    }
    assert_eq!(handle.status().role, Role::Leading(Term(3)));
    assert_eq!(handle.status().adopted_block.unwrap().block_index(), BlockIndex(1));
    writer.demote().await.unwrap();
}

#[tokio::test]
async fn unavailable_older_confirmation_evidence_keeps_latest_checkpoint_reopen_and_writes_live() {
    for missing_catalog in [false, true] {
        let fixture = produced_memory_checkpoint(true).await;
        let mut prior = produce_newer_memory_checkpoint(&fixture).await;
        deliver_checkpoint_record(&fixture, &mut prior, 2, 2, upload(1, -1)).await.unwrap();

        if missing_catalog {
            let key = rtdb_storage::ObjectKey::new(rtdb_format::keys::block_key(BlockIndex(0)).unwrap()).unwrap();
            fixture.store.delete(&key).await.unwrap();
        } else {
            fixture.replica.truncate_before(LogOffset(4)).unwrap();
        }
        let (writer_handle, mut writer) = reopen_memory_checkpoint_with_config(&fixture, ReplicationConfig {
            assertion_interval: Duration::from_millis(10), ..Default::default()
        }).await;
        let visible = writer.partition.value();
        let fence = writer.fence.clone();
        let history = writer.prove_historical_confirmations().await;
        if missing_catalog {
            assert!(history.unwrap().is_empty());
        } else {
            assert_eq!(history.err().unwrap().code, "rtdb.storage/log-range-unavailable");
        }
        assert!(Arc::ptr_eq(&writer.partition.value(), &visible));
        assert_eq!(writer.fence, fence);
        assert_eq!(writer.read_position, rtdb_types::ReplicaPosition(3));
        assert_eq!(writer_handle.status().adopted_block.unwrap().block_index(), BlockIndex(1));

        let (follower_handle, follower) = reopen_memory_checkpoint_with_config(&fixture, ReplicationConfig {
            may_lead: false, ..Default::default()
        }).await;
        let follower_value = Arc::clone(&follower.partition);
        let submitted = submit_recovery_row(&writer_handle, &writer.resolve.memory, 3, 300).await;
        let writer_task = tokio::spawn(writer.run());
        let follower_task = tokio::spawn(follower.run());
        tokio::time::timeout(Duration::from_secs(5), async {
            for handle in [&writer_handle, &follower_handle] {
                let result = handle.await_tx(TxId(submitted.0), &CancellationToken::new()).await.unwrap().unwrap();
                assert!(result.committed());
                assert_eq!(result.tx_key().tx_id, TxId(submitted.0));
            }
        }).await.unwrap();
        let tip = fixture.replica.latest().unwrap();
        let records = fixture.replica.read_range(LogOffset(5), LogOffset(tip.0 + 1)).await.unwrap();
        for record in records {
            let envelope = ReplicaEnvelope::decode(&record.payload).unwrap().unwrap();
            assert!(!matches!(envelope.message, ReplicaBody::BlockUploaded(_)));
        }
        let docs = TableRef::new("public", "docs");
        let value = follower_value.value();
        let row = value.live().tables()[&docs].row(0).unwrap().event().unwrap();
        let rtdb_index::StoredOperation::Put(document) = row.operation else { panic!("expected the post-reopen row"); };
        assert_eq!(document.get("_id"), Some(&Value::I64(3)));

        writer_handle.shutdown();
        follower_handle.shutdown();
        writer_task.await.unwrap().unwrap();
        follower_task.await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn an_installed_block_upload_bypasses_a_newer_full_pending_block() {
    for may_lead in [false, true] {
        let fixture = produced_memory_checkpoint(true).await;
        let (handle, mut reader) = reopen_memory_checkpoint_with_config(&fixture, ReplicationConfig {
            may_lead, pending_block_records: 1, ..Default::default()
        }).await;
        deliver_checkpoint_record(&fixture, &mut reader, 1, 2, upload(0, -1)).await.unwrap();
        assert!(reader.recovered_confirmation.is_none());
        deliver_checkpoint_record(&fixture, &mut reader, 1, 3, boundary(1, -1)).await.unwrap();
        deliver_checkpoint_record(&fixture, &mut reader, 1, 4, resolved(1, 0)).await.unwrap();
        assert_eq!(reader.pending_block.as_ref().unwrap().buffered_len(), 1);
        deliver_checkpoint_record(&fixture, &mut reader, 1, 5, upload(0, -1)).await.unwrap();
        assert_eq!(reader.read_position, rtdb_types::ReplicaPosition(5));
        assert_eq!(reader.pending_block.as_ref().unwrap().block_index(), BlockIndex(1));
        assert_eq!(reader.pending_block.as_ref().unwrap().buffered_len(), 1);
        assert_eq!(handle.progress().current().source_position, SourcePosition(-1));
        let ReplicaBody::BlockUploaded(mut other_storage) = upload(0, -1) else { unreachable!(); };
        other_storage.storage_epoch = Some(1);
        let error = deliver_checkpoint_record(&fixture, &mut reader, 1, 6, ReplicaBody::BlockUploaded(other_storage)).await.unwrap_err();
        assert_eq!(error.code, "xtdb.indexer/follower-buffer-overflow");
        assert_eq!(reader.read_position, rtdb_types::ReplicaPosition(5));
    }
}

struct FailFirstInheritedProduction(std::sync::atomic::AtomicBool);

fn failed_inherited_production() -> Anomaly {
    Anomaly::fault("test/inherited-block", "Could not read secondary database configurations")
}

#[async_trait]
impl DatabaseEffects for FailFirstInheritedProduction {
    fn check_db_op(&self, _: &DbOp) -> Result<()> { Ok(()) }
    async fn apply_db_op(&self, _: &DbOp) -> Result<()> { Ok(()) }
    fn secondary_databases(&self) -> Result<BTreeMap<String, DatabaseConfig>> {
        if self.0.swap(false, std::sync::atomic::Ordering::Relaxed) {
            return Err(failed_inherited_production());
        }
        Ok(BTreeMap::new())
    }
}

#[tokio::test]
async fn recovered_confirmation_survives_a_failed_later_block_start_and_the_next_term_retries_it() {
    let fixture = produced_memory_checkpoint(true).await;
    let (handle, mut restarted) = reopen_memory_checkpoint_with_config(&fixture, ReplicationConfig {
        assertion_interval: Duration::from_secs(3600), ..Default::default()
    }).await;
    restarted.effects = Arc::new(FailFirstInheritedProduction(std::sync::atomic::AtomicBool::new(true)));
    let claim = ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() });
    deliver_checkpoint_record(&fixture, &mut restarted, 2, 0, claim.clone()).await.unwrap();
    deliver_checkpoint_record(&fixture, &mut restarted, 2, 1, boundary(1, -1)).await.unwrap();
    restarted.claim = Some(rtdb_types::ReplicaPosition(4));
    deliver_checkpoint_record(&fixture, &mut restarted, 3, 0, claim.clone()).await.unwrap();
    assert_eq!(restarted.role(), Role::Following);
    assert!(restarted.leader.as_ref().unwrap().production.is_none());
    let mut tail = fixture.replica.tail(LogOffset(4)).await.unwrap();

    let repair = next_local_record(tail.as_mut()).await;
    restarted.consume(repair).await.unwrap();
    assert!(restarted.recovered_confirmation.is_none());
    assert!(restarted.leader.is_none());
    assert_eq!(restarted.role(), Role::Following);
    assert_eq!(restarted.read_position, rtdb_types::ReplicaPosition(5));
    assert_eq!(restarted.pending_block.as_ref().unwrap().block_index(), BlockIndex(1));
    assert_eq!(handle.status().promotion_failure, Some(failed_inherited_production()));
    assert!(handle.progress().fault().is_none());

    restarted.claim = Some(rtdb_types::ReplicaPosition(6));
    deliver_checkpoint_record(&fixture, &mut restarted, 4, 0, claim).await.unwrap();
    assert_eq!(restarted.role(), Role::Following);
    assert!(handle.status().promotion_failure.is_none());
    let leader = restarted.leader.as_mut().unwrap();
    assert!(leader.recovery_write.is_none());
    assert!(leader.production.is_some());
    let written = tokio::time::timeout(Duration::from_secs(5), super::pending_production(&mut leader.production)).await.unwrap();
    restarted.production_finished(written).await.unwrap();
    let appended = tokio::time::timeout(Duration::from_secs(5),
        super::pending_production(&mut restarted.leader.as_mut().unwrap().production)).await.unwrap();
    restarted.production_finished(appended).await.unwrap();
    let mut tail = fixture.replica.tail(LogOffset(6)).await.unwrap();
    let next = next_local_record(tail.as_mut()).await;
    let envelope = ReplicaEnvelope::decode(&next.payload).unwrap().unwrap();
    assert!(matches!(envelope.message, ReplicaBody::BlockUploaded(upload) if upload.block_index == Some(1)));
    restarted.consume(next).await.unwrap();
    assert_eq!(restarted.role(), Role::Leading(Term(4)));
    assert!(restarted.pending_block.is_none());
    assert_eq!(handle.status().adopted_block.unwrap().block_index(), BlockIndex(1));
    restarted.demote().await.unwrap();
}

struct WaitingWriter {
    started: Option<tokio::sync::oneshot::Sender<()>>,
    dropped: Arc<std::sync::atomic::AtomicBool>,
}
impl Drop for WaitingWriter { fn drop(&mut self) { self.dropped.store(true, std::sync::atomic::Ordering::Release); } }
#[async_trait]
impl rtdb_tx::TransactionWriter for WaitingWriter {
    async fn write(&mut self, _: &mut rtdb_tx::OpenTransaction<'_>) -> Result<rtdb_tx::WriterResult> {
        if let Some(started) = self.started.take() { let _ = started.send(()); }
        futures::future::pending().await
    }
}

#[tokio::test]
async fn higher_term_drops_an_inflight_external_writer_and_replays_held_prefix_before_the_new_term() {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, external_source: true, ..Default::default() }, 1 << 27).await;
    fixture.driver.fence.admit(Term(1), Some(TermSequence(0))).unwrap();
    fixture.driver.promote().await.unwrap();
    let (started, receiver) = tokio::sync::oneshot::channel();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (completion, result) = tokio::sync::oneshot::channel();
    fixture.driver.command(super::Command::External { term: Term(1), writer: Box::new(WaitingWriter { started: Some(started), dropped: Arc::clone(&dropped) }), system_time: None, token: None, completion }).await.unwrap();
    {
        let resolution = &mut fixture.driver.leader.as_mut().unwrap().resolution;
        tokio::select! { _ = super::pending_resolution(resolution) => panic!("waiting writer completed"), _ = tokio::task::yield_now() => {} }
    }
    receiver.await.unwrap();
    fixture.driver.consume(raw(0, 1, 1, ReplicaBody::NoOp(proto::NoOp { src_msg_id: Some(0), ..Default::default() }))).await.unwrap();
    assert_eq!(fixture.handle.progress().current().source_position, SourcePosition(-1));
    fixture.driver.consume(raw(1, 2, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: Some(1), ..Default::default() }))).await.unwrap();
    assert!(dropped.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(result.await.unwrap().unwrap_err().code, "xtdb/leader-term-ended");
    assert_eq!(fixture.handle.progress().current().source_position, SourcePosition(1));
    assert!(fixture.handle.progress().fault().is_none());
}

#[tokio::test]
async fn demotion_fails_queued_gc_publication_without_failing_the_progress_monitor() {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, ..Default::default() }, 1 << 27).await;
    fixture.driver.fence.admit(Term(1), Some(TermSequence(0))).unwrap();
    fixture.driver.promote().await.unwrap();
    let (completion, result) = tokio::sync::oneshot::channel();
    fixture.driver.command(super::Command::Delete { term: Term(1), table: TableRef::new("public", "docs"), keys: vec!["l01-rc-b00".parse().unwrap()], completion }).await.unwrap();
    fixture.driver.consume(raw(0, 2, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }))).await.unwrap();
    assert_eq!(result.await.unwrap().unwrap_err().code, "xtdb/leader-term-ended");
    assert!(fixture.handle.progress().fault().is_none());
}

#[tokio::test]
async fn compactor_reset_receipt_requires_block_adoption_and_fails_on_term_loss() {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, ..Default::default() }, 1 << 27).await;
    fixture.logs.append_source(&rtdb_format::envelope::SourceMessage::Protobuf(rtdb_format::envelope::SourceBody::FlushBlock(Default::default())), Timestamp(1)).await.unwrap();
    fixture.driver.consume(raw(0, 1, 0, resolved(1, 0))).await.unwrap();
    fixture.driver.consume(raw(1, 1, 1, boundary(0, 0))).await.unwrap();
    let pending = fixture.driver.pending_block.as_ref().unwrap();
    let produced = fixture.blocks.produce(&pending.prepared, &pending.boundary, BTreeMap::new()).await.unwrap();
    let ReplicaBody::BlockUploaded(upload) = upload(0, 0) else { unreachable!(); };
    fixture.driver.consume(raw(2, 1, 2, ReplicaBody::BlockUploaded(proto::BlockUploaded {
        catalog_sha256: produced.catalog.publication_sha256().map(|digest| digest.to_vec()), ..upload
    }))).await.unwrap();

    fixture.driver.promote().await.unwrap();
    let old_snapshot = fixture.driver.partition.snapshot();
    assert_eq!(old_snapshot.catalog_cut().unwrap().through, ReplicaPosition(2));
    let value = fixture.driver.partition.value();
    let reset = PrimaryReset::from_installed(
        Arc::clone(&fixture.logs), &produced.catalog, old_snapshot.catalog_cut(),
        value.trie_catalog().file_size_target(), &CancellationToken::new(),
    ).await.unwrap();
    let primary = reset.publish_files(
        Arc::clone(value.trie_catalog()), value.live().block_index(), &CancellationToken::new(),
    ).await.unwrap();
    let (completion, mut receipt) = tokio::sync::oneshot::channel();
    fixture.driver.command(super::Command::ResetCompactor {
        term: Term(1), primary, reservations: vec![fixture.memory.reserve(8192).unwrap()].into(), completion,
    }).await.unwrap();
    assert!(matches!(receipt.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)));
    assert!(fixture.driver.leader.as_ref().unwrap().compactor_reset.is_some());
    assert_eq!(fixture.driver.partition.open_snapshot(None).catalog_cut(), None);
    fixture.driver.consume(raw(3, 2, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }))).await.unwrap();
    assert_eq!(receipt.await.unwrap().unwrap_err().code, "xtdb/leader-term-ended");
    assert!(fixture.handle.progress().fault().is_none());
    assert_eq!(fixture.driver.partition.snapshot().catalog_cut(), None);
    assert_eq!(old_snapshot.catalog_cut().unwrap().through, ReplicaPosition(2));
}

#[tokio::test]
async fn retained_leader_does_not_claim_again_while_confirmed_or_producing_an_inherited_block() {
    for pending_block in [false, true] {
        let mut fixture = fixture(ReplicationConfig::default(), 1 << 27).await;
        let body = if pending_block { boundary(0, 0) } else { ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }) };
        fixture.driver.consume(raw(0, 1, 0, body)).await.unwrap();
        fixture.driver.promote().await.unwrap();
        assert_eq!(fixture.driver.role(), if pending_block { Role::Following } else { Role::Leading(Term(1)) });
        assert!(fixture.driver.leader.is_some());
        fixture.driver.start_claim().unwrap();
        assert!(fixture.driver.claiming.is_none());
        assert!(fixture.driver.claim.is_none());
        fixture.driver.demote().await.unwrap();
        fixture.driver.start_claim().unwrap();
        assert_eq!(fixture.driver.claiming.as_ref().unwrap().term, Term(2));
    }
}

#[tokio::test]
async fn stalled_claim_keeps_replica_admission_and_shutdown_live() {
    let mut fixture = fixture(ReplicationConfig::default(), 1 << 27).await;
    fixture.driver.claiming = Some(super::Claim { term: Term(1), observed: None, future: Box::pin(futures::future::pending()) });
    let handle = fixture.handle.clone();
    let driver = tokio::spawn(fixture.driver.run());
    fixture.logs.enqueue_replica(&ReplicaEnvelope { term: Term(2), term_sequence: Some(TermSequence(0)), message: ReplicaBody::NoOp(proto::NoOp { src_msg_id: Some(1), ..Default::default() }) }, BlockIndex(0), Timestamp(1)).await.unwrap().durable().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), handle.await_source(SourcePosition(1), &CancellationToken::new())).await.unwrap().unwrap();
    assert_eq!(handle.status().highest_term, Term(2));
    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(1), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn claim_read_back_before_durability_promotes_only_after_its_matching_receipt() {
    let mut fixture = fixture(ReplicationConfig::default(), 1 << 27).await;
    fixture.driver.claiming = Some(super::Claim { term: Term(1), observed: None, future: Box::pin(futures::future::pending()) });
    fixture.driver.consume(raw(0, 1, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }))).await.unwrap();
    assert!(fixture.driver.leader.is_none());
    fixture.driver.claim_finished(Ok(ReplicaMetadata { position: rtdb_types::ReplicaPosition(0), timestamp: Timestamp(1) })).await.unwrap();
    assert_eq!(fixture.driver.role(), Role::Leading(Term(1)));
    fixture.driver.demote().await.unwrap();
}

#[tokio::test]
async fn losing_same_term_claim_never_promotes_after_durability() {
    let mut fixture = fixture(ReplicationConfig::default(), 1 << 27).await;
    fixture.driver.claiming = Some(super::Claim { term: Term(1), observed: None, future: Box::pin(futures::future::pending()) });
    fixture.driver.consume(raw(0, 1, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }))).await.unwrap();
    fixture.driver.consume(raw(1, 1, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }))).await.unwrap();
    fixture.driver.claim_finished(Ok(ReplicaMetadata { position: rtdb_types::ReplicaPosition(1), timestamp: Timestamp(1) })).await.unwrap();
    assert!(fixture.driver.leader.is_none());
}

struct HeldDatabaseEffect {
    called: Mutex<Option<oneshot::Sender<DbOp>>>,
    release: Mutex<Option<oneshot::Receiver<Result<()>>>>,
}

#[async_trait]
impl DatabaseEffects for HeldDatabaseEffect {
    fn check_db_op(&self, _: &DbOp) -> Result<()> { Ok(()) }

    async fn apply_db_op(&self, operation: &DbOp) -> Result<()> {
        let release = self.release.lock().take().unwrap();
        self.called.lock().take().unwrap().send(operation.clone()).unwrap();
        release.await.unwrap()
    }

    fn secondary_databases(&self) -> Result<BTreeMap<String, DatabaseConfig>> { Ok(BTreeMap::new()) }
}

async fn queued_attach_readback(fixture: &mut Fixture, operation: &DbOp, position: SourcePosition) -> (TransactionKey, oneshot::Receiver<Result<Applied>>, LogRecord) {
    fixture.driver.fence.admit(Term(1), Some(TermSequence(0))).unwrap();
    fixture.driver.promote().await.unwrap();

    let snapshot = fixture.driver.partition.value().snapshot();
    let resolve = Arc::clone(&fixture.driver.resolve);
    let effects = Arc::clone(&fixture.driver.effects);
    let resolver = fixture.driver.leader.as_mut().unwrap().resolver_mut().unwrap();
    let verdict = super::check_db_op(effects.as_ref(), resolver, operation);
    let staged = resolver.resolve_db_op(snapshot, position, Timestamp(100), operation.clone(), verdict, &resolve).await.unwrap();
    let key = staged.result().tx_key();
    let (completion, receipt) = oneshot::channel();
    fixture.driver.enqueue_resolved(staged, Some(completion)).await.unwrap();

    let mut tail = fixture.logs.replica_tail(fixture.driver.read_position).await.unwrap();
    let mut records = tail.poll(Duration::from_secs(5)).await.unwrap();
    assert_eq!(records.len(), 1);
    let readback = records.pop().unwrap();
    (key, receipt, readback)
}

#[tokio::test]
async fn a_strict_source_samples_the_clock_after_the_older_replica_write_applies() {
    let wall = Arc::new(AtomicI64::new(110));
    let clock: ReplicationClock = {
        let wall = Arc::clone(&wall);
        Arc::new(move || Timestamp(wall.load(Ordering::Relaxed)))
    };
    let config = ReplicationConfig { may_lead: false, idle_heartbeat: false, ..Default::default() };
    let mut fixture = fixture_with_clock(config, 1 << 16, Some(clock)).await;
    let scope = rtdb_types::PlacedAttemptScope::new("test-replica", 1).unwrap();
    Arc::get_mut(&mut fixture.driver.resolve).unwrap().owned_witness_scope = Some(scope);
    let operation = DbOp::AttachDatabase(proto::AttachDatabase {
        db_name: Some("a".repeat(1 << 17)), config: Some(DatabaseConfig::default()),
    });
    let (_, prior_applied, prior_record) = queued_attach_readback(&mut fixture, &operation, SourcePosition(0)).await;
    assert!(matches!(ReplicaEnvelope::decode(&prior_record.payload).unwrap().unwrap().message, ReplicaBody::OversizedMessage(_)));

    let strict = SourceTransaction::new(vec![TxOp::OwnedDeadline { until: Timestamp(150), recheck_at_staging: true, journal_before: None }],
        SubmitOptions { user: Some("owner".into()), ..Default::default() }, &fixture.memory,
    ).unwrap().with_placed_seed(Some(scope.issue()));
    fixture.driver.leader.as_mut().unwrap().source_batch.push_back(LogRecord::new(
        LogMetadata { offset: LogOffset(1), timestamp: Timestamp(101) },
        strict.encode().unwrap().into_bytes(),
    ));
    fixture.driver.resolve_next_source().await.unwrap();
    let leader = fixture.driver.leader.as_ref().unwrap();
    assert!(leader.resolution.is_none());
    assert!(!leader.ready_source());
    assert!(!leader.accepting_commands());

    wall.store(200, Ordering::Relaxed);
    let prior_record_offset = prior_record.metadata.offset;
    fixture.driver.consume(prior_record).await.unwrap();
    assert!(matches!(prior_applied.await.unwrap().unwrap(), Applied::Transaction(_)));
    assert!(fixture.driver.leader.as_ref().unwrap().ready_source());
    let after = prior_record_offset;
    fixture.driver.resolve_next_source().await.unwrap();
    assert!(fixture.driver.leader.as_ref().unwrap().resolution.is_none());
    let mut tail = fixture.replica.tail(after).await.unwrap();
    let reservation = tail.poll(Duration::from_secs(1)).await.unwrap().pop().unwrap();
    fixture.driver.consume(reservation).await.unwrap();
    fixture.driver.drive_placed_slot().unwrap();
    let (resolver, result) = super::pending_resolution(&mut fixture.driver.leader.as_mut().unwrap().resolution).await;
    assert_eq!(result.as_ref().unwrap().result().error().unwrap().code, "rtdb.owned/expired");
    fixture.driver.resolution_finished(resolver, result).await.unwrap();
    fixture.driver.demote().await.unwrap();
}

#[tokio::test]
async fn a_successor_fences_a_durably_admitted_write_that_missed_dispatch() {
    let wall = Arc::new(AtomicI64::new(110));
    let clock: ReplicationClock = {
        let wall = Arc::clone(&wall);
        Arc::new(move || Timestamp(wall.load(Ordering::Relaxed)))
    };
    let config = ReplicationConfig { idle_heartbeat: false, ..Default::default() };
    let mut fixture = fixture_with_clock(config, 1 << 16, Some(clock)).await;
    let scope = rtdb_types::PlacedAttemptScope::new("test-replica", 1).unwrap();
    Arc::get_mut(&mut fixture.driver.resolve).unwrap().owned_witness_scope = Some(scope);
    let claim = ReplicaEnvelope { term: Term(1), term_sequence: Some(TermSequence(0)),
        message: ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }) };
    fixture.logs.enqueue_replica(&claim, BlockIndex(0), Timestamp(100)).await.unwrap().durable().await.unwrap();
    let claim_record = fixture.replica.read_range(LogOffset(0), LogOffset(1)).await.unwrap().pop().unwrap();
    fixture.driver.consume(claim_record).await.unwrap();
    fixture.driver.promote().await.unwrap();

    let command = SourceTransaction::new(vec![TxOp::OwnedDeadline { until: Timestamp(150), recheck_at_staging: true, journal_before: None }],
        SubmitOptions { user: Some("owner".into()), ..Default::default() }, &fixture.memory,
    ).unwrap().with_placed_seed(Some(scope.issue()));
    let payload = command.encode().unwrap().into_bytes();
    let source = fixture.logs.append_source_encoded(payload.clone(), Timestamp(100)).await.unwrap();
    fixture.driver.leader.as_mut().unwrap().source_batch.push_back(LogRecord::new(
        LogMetadata { offset: LogOffset(source.0), timestamp: Timestamp(100) }, payload,
    ));
    fixture.driver.resolve_next_source().await.unwrap();
    let mut tail = fixture.replica.tail(LogOffset(0)).await.unwrap();
    let admission = tail.poll(Duration::from_secs(1)).await.unwrap().pop().unwrap();
    fixture.driver.consume(admission).await.unwrap();
    fixture.driver.drive_placed_slot().unwrap();
    let leader = fixture.driver.leader.as_mut().unwrap();
    let (resolver, result) = super::pending_resolution(&mut leader.resolution).await;
    assert!(result.as_ref().unwrap().staging_clock_upper().is_some());
    wall.store(160, Ordering::Relaxed);
    fixture.driver.resolution_finished(resolver, result).await.unwrap();
    assert!(!fixture.driver.leader.as_ref().unwrap().accepting_commands());

    let driver = tokio::spawn(fixture.driver.run());
    tokio::time::timeout(Duration::from_secs(1), fixture.handle.await_source(source, &CancellationToken::new()))
        .await.unwrap().unwrap();
    let records = fixture.replica.read_range(LogOffset(0), LogOffset(4)).await.unwrap();
    assert_eq!(records.len(), 4);
    let resolved = ReplicaEnvelope::decode(&records[3].payload).unwrap().unwrap();
    assert_eq!(resolved.term, Term(2));
    let ReplicaBody::ResolvedTx(tx) = resolved.message else { panic!("Expected an ordered refusal"); };
    assert_eq!(tx.src_msg_id, Some(source.0));
    assert_eq!(tx.committed, Some(false));
    assert!(tx.acceptance_stage.is_none());
    assert!(rtdb_types::PlacedTerminal::decode(tx.placed_slot_terminal.as_deref().unwrap()).unwrap().fenced);
    fixture.handle.shutdown();
    tokio::time::timeout(Duration::from_secs(1), driver).await.unwrap().unwrap().unwrap();
}

async fn attach_readback_fixture(replica_message_limit: usize) -> (Fixture, DbOp, oneshot::Receiver<DbOp>, oneshot::Sender<Result<()>>) {
    let mut fixture = fixture(ReplicationConfig {
        may_lead: false, assertion_interval: Duration::from_secs(3600), ..Default::default()
    }, replica_message_limit).await;
    let (operation, called, finish) = hold_catalog_effect(&mut fixture);
    (fixture, operation, called, finish)
}

fn hold_catalog_effect(fixture: &mut Fixture) -> (DbOp, oneshot::Receiver<DbOp>, oneshot::Sender<Result<()>>) {
    let (entered, called) = oneshot::channel();
    let (finish, release) = oneshot::channel();
    fixture.driver.effects = Arc::new(HeldDatabaseEffect { called: Mutex::new(Some(entered)), release: Mutex::new(Some(release)) });
    let operation = DbOp::AttachDatabase(proto::AttachDatabase {
        db_name: Some("secondary".into()), config: Some(DatabaseConfig::default()),
    });
    (operation, called, finish)
}

struct EnqueueReturnGate {
    log: Arc<MemoryLog>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl Log for EnqueueReturnGate {
    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> rtdb_storage::Result<rtdb_storage::log::AppendReceipt> {
        let receipt = self.log.enqueue(payload, timestamp).await?;
        self.release.notified().await;
        Ok(receipt)
    }

    fn latest(&self) -> rtdb_storage::Result<LogOffset> { self.log.latest() }
    async fn tail(&self, after: LogOffset) -> rtdb_storage::Result<Box<dyn rtdb_storage::log::LogTail>> { self.log.tail(after).await }
    async fn range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Box<dyn rtdb_storage::log::LogRange>> { self.log.range(from, to).await }
    async fn read_range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Vec<LogRecord>> { self.log.read_range(from, to).await }
}

#[tokio::test]
async fn own_transaction_can_apply_before_the_provider_returns_its_append_receipt() {
    let release = Arc::new(tokio::sync::Notify::new());
    let mut fixture = fixture_with_replica(ReplicationConfig { may_lead: false, idle_heartbeat: false, ..Default::default() },
        1 << 27, None, |log| Arc::new(EnqueueReturnGate { log, release: Arc::clone(&release) })).await;
    let (operation, mut called, finish) = hold_catalog_effect(&mut fixture);
    let (key, mut receipt, record) = queued_attach_readback(&mut fixture, &operation, SourcePosition(0)).await;
    finish.send(Ok(())).unwrap();

    tokio::time::timeout(Duration::from_secs(5), fixture.driver.consume(record)).await.unwrap().unwrap();
    assert_eq!(called.try_recv().unwrap(), operation);
    assert!(matches!(receipt.try_recv().unwrap().unwrap(), Applied::Transaction(result) if result.tx_key() == key));
    assert_eq!(fixture.handle.progress().current().tx_key, Some(key));
    release.notify_one();
    fixture.driver.demote().await.unwrap();
}

#[tokio::test]
async fn mismatched_readback_faults_before_buffering_during_resolution() {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, idle_heartbeat: false, external_source: true, ..Default::default() }, 1 << 27).await;
    let (operation, mut called, _finish) = hold_catalog_effect(&mut fixture);
    let (_, mut receipt, mut readback) = queued_attach_readback(&mut fixture, &operation, SourcePosition(0)).await;
    let before = fixture.driver.partition.value();
    let (completion, mut waiting) = oneshot::channel();
    let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
    fixture.driver.command(super::Command::External {
        term: Term(1), writer: Box::new(WaitingWriter { started: None, dropped: Arc::clone(&dropped) }),
        system_time: None, token: None, completion,
    }).await.unwrap();
    {
        let leader = fixture.driver.leader.as_mut().unwrap();
        assert!(leader.resolver.is_none());
        let resolution = super::pending_resolution(&mut leader.resolution);
        tokio::pin!(resolution);
        assert!(futures::poll!(&mut resolution).is_pending());
    }
    let mut envelope = ReplicaEnvelope::decode(&readback.payload).unwrap().unwrap();
    let ReplicaBody::ResolvedTx(message) = &mut envelope.message else { panic!("expected transaction"); };
    message.db_op = None;
    readback.payload = envelope.encode().into();

    let error = tokio::time::timeout(Duration::from_secs(5), fixture.driver.consume(readback)).await.unwrap().unwrap_err();
    assert_eq!(error.code, "xtdb.indexer/leader-read-back-mismatch");
    assert_eq!(receipt.try_recv().unwrap().unwrap_err(), error);
    assert!(Arc::ptr_eq(&fixture.driver.partition.value(), &before));
    assert!(matches!(called.try_recv(), Err(Empty)));
    assert!(fixture.handle.progress().current().tx_key.is_none());
    assert!(fixture.driver.demote().await.unwrap().is_empty());
    assert!(dropped.load(Ordering::Acquire));
    assert_eq!(waiting.try_recv().unwrap().unwrap_err().code, "xtdb/leader-term-ended");
}

#[tokio::test]
async fn own_attach_readback_mismatch_leaves_partition_catalog_and_progress_unchanged() {
    let (mut fixture, operation, mut called, _finish) = attach_readback_fixture(1 << 27).await;
    let (key, mut receipt, mut readback) = queued_attach_readback(&mut fixture, &operation, SourcePosition(1)).await;
    let before = fixture.driver.partition.value();
    let mut envelope = ReplicaEnvelope::decode(&readback.payload).unwrap().unwrap();
    let ReplicaBody::ResolvedTx(message) = &mut envelope.message else { panic!("expected an attach readback"); };
    let mismatched_id = key.tx_id.0 + 1;
    message.tx_id = Some(mismatched_id);
    readback.payload = envelope.encode().into();

    let error = {
        let consumed = fixture.driver.consume(readback);
        tokio::pin!(consumed);
        let std::task::Poll::Ready(Err(error)) = futures::poll!(&mut consumed) else {
            panic!("mismatched readback did not fail immediately");
        };
        error
    };
    assert_eq!(error, Anomaly::fault("xtdb.indexer/leader-read-back-mismatch", "Replica transaction does not match the oldest staged transaction")
        .with_context("replica-tx-id", mismatched_id.into()));
    assert!(matches!(called.try_recv(), Err(Empty)));
    assert!(Arc::ptr_eq(&fixture.driver.partition.value(), &before));
    let progress = fixture.handle.progress().current();
    assert_eq!(progress.source_position, SourcePosition(-1));
    assert!(progress.tx_key.is_none());
    assert!(progress.latest_result.is_none());
    assert_eq!(receipt.try_recv().unwrap().unwrap_err(), error);
    fixture.driver.demote().await.unwrap();
}

#[tokio::test]
async fn own_readback_requires_the_frame_written_for_the_staged_head() {
    let mutations: [(&str, fn(&mut proto::ResolvedTx)); 7] = [
        ("absent zero id", |message| message.tx_id = None),
        ("absent system time", |message| message.system_time_micros = None),
        ("changed commit status", |message| message.committed = Some(false)),
        ("changed source position", |message| message.src_msg_id = Some(17)),
        ("unwritten external token", |message| message.external_source_token = Some(vec![17])),
        ("missing catalog operation", |message| message.db_op = None),
        ("unwritten error bytes", |message| message.error = Some(vec![17].into())),
    ];
    for (field, mutate) in mutations {
        let (mut fixture, operation, mut called, _finish) = attach_readback_fixture(1 << 27).await;
        let (key, mut receipt, mut readback) = queued_attach_readback(&mut fixture, &operation, SourcePosition(0)).await;
        assert_eq!(key.tx_id, TxId(0));
        let before = fixture.driver.partition.value();
        let mut envelope = ReplicaEnvelope::decode(&readback.payload).unwrap().unwrap();
        let ReplicaBody::ResolvedTx(message) = &mut envelope.message else { panic!("expected an attach readback"); };
        mutate(message);
        readback.payload = envelope.encode().into();

        let error = {
            let consumed = fixture.driver.consume(readback);
            tokio::pin!(consumed);
            let std::task::Poll::Ready(Err(error)) = futures::poll!(&mut consumed) else {
                panic!("{field} did not fail before partition apply");
            };
            error
        };
        assert_eq!(error.code, "xtdb.indexer/leader-read-back-mismatch", "{field}");
        assert!(matches!(called.try_recv(), Err(Empty)), "{field}");
        assert!(Arc::ptr_eq(&fixture.driver.partition.value(), &before), "{field}");
        assert!(fixture.handle.progress().current().tx_key.is_none(), "{field}");
        assert_eq!(receipt.try_recv().unwrap().unwrap_err(), error, "{field}");
        fixture.driver.demote().await.unwrap();
    }
}

#[tokio::test]
async fn control_readback_cannot_replace_a_pending_transaction() {
    for src_msg_id in [None, Some(17)] {
        let (mut fixture, operation, mut called, _finish) = attach_readback_fixture(1 << 27).await;
        let (_, mut receipt, mut readback) = queued_attach_readback(&mut fixture, &operation, SourcePosition(0)).await;
        let before = fixture.driver.partition.value();
        let mut envelope = ReplicaEnvelope::decode(&readback.payload).unwrap().unwrap();
        envelope.message = ReplicaBody::NoOp(proto::NoOp { src_msg_id, ..Default::default() });
        readback.payload = envelope.encode().into();

        let error = fixture.driver.consume(readback).await.unwrap_err();
        assert_eq!(error.code, "xtdb.indexer/leader-read-back-mismatch");
        assert_eq!(receipt.try_recv().unwrap().unwrap_err(), error);
        assert!(Arc::ptr_eq(&fixture.driver.partition.value(), &before));
        assert!(matches!(called.try_recv(), Err(Empty)));
        assert_eq!(fixture.handle.progress().current().source_position, SourcePosition(-1));
        fixture.driver.demote().await.unwrap();
    }
}

#[tokio::test]
async fn foreign_term_readback_cannot_acknowledge_a_pending_write() {
    let (mut fixture, operation, mut called, finish) = attach_readback_fixture(1 << 27).await;
    let (key, mut receipt, readback) = queued_attach_readback(&mut fixture, &operation, SourcePosition(0)).await;
    assert!(matches!(receipt.try_recv(), Err(Empty)));
    let offset = readback.metadata.offset.0;
    let mut foreign = ReplicaEnvelope::decode(&readback.payload).unwrap().unwrap();
    foreign.term = Term(2);
    assert_eq!(foreign.term_sequence, Some(TermSequence(1)));

    fixture.driver.consume(raw(offset, 2, 0, ReplicaBody::NoOp(Default::default()))).await.unwrap();
    assert_eq!(receipt.try_recv().unwrap().unwrap_err().code, "xtdb/leader-term-ended");
    finish.send(Ok(())).unwrap();
    fixture.driver.consume(raw(offset + 1, 2, 1, foreign.message)).await.unwrap();

    assert_eq!(called.try_recv().unwrap(), operation);
    assert_eq!(fixture.handle.progress().current().tx_key, Some(key));
}

#[tokio::test]
async fn own_transaction_requires_a_pending_append_for_its_sequence() {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, idle_heartbeat: false, ..Default::default() }, 1 << 27).await;
    fixture.driver.fence.admit(Term(1), Some(TermSequence(0))).unwrap();
    fixture.driver.promote().await.unwrap();
    let before = fixture.driver.partition.value();

    let error = fixture.driver.consume(raw(0, 1, 1, resolved(0, 0))).await.unwrap_err();
    assert_eq!(error.code, "xtdb.indexer/leader-read-back-mismatch");
    assert!(Arc::ptr_eq(&fixture.driver.partition.value(), &before));
    assert!(fixture.handle.progress().current().tx_key.is_none());
    fixture.driver.demote().await.unwrap();
}

#[tokio::test]
async fn unrecognized_readback_only_faults_an_admitted_pending_transaction() {
    for unknown_field in [false, true] {
        let (mut fixture, operation, mut called, _finish) = attach_readback_fixture(1 << 27).await;
        let (_, mut receipt, readback) = queued_attach_readback(&mut fixture, &operation, SourcePosition(0)).await;
        let before = fixture.driver.partition.value();
        let frame = |term, sequence| {
            let mut frame = vec![rtdb_format::PROTOBUF_LOG_HEADER, 7 << 3, term, 9 << 3, sequence];
            if unknown_field { frame.extend([15 << 3 | 2, 0]); }
            let frame = Bytes::from(frame);
            assert!(ReplicaEnvelope::decode(&frame).unwrap().is_none());
            frame
        };
        let mut discarded = readback.clone();
        discarded.payload = frame(1, 0);
        fixture.driver.consume(discarded).await.unwrap();
        let mut foreign = readback.clone();
        foreign.metadata.offset = LogOffset(readback.metadata.offset.0 + 1);
        foreign.payload = frame(2, 1);
        fixture.driver.consume(foreign).await.unwrap();
        let advanced = fixture.driver.read_position;
        assert!(matches!(receipt.try_recv(), Err(Empty)));

        let mut own = readback;
        own.metadata.offset = LogOffset(own.metadata.offset.0 + 2);
        own.payload = frame(1, 1);
        let error = fixture.driver.consume(own).await.unwrap_err();
        assert_eq!(error.code, "xtdb.indexer/leader-read-back-mismatch");
        assert_eq!(receipt.try_recv().unwrap().unwrap_err(), error);
        assert!(matches!(called.try_recv(), Err(Empty)));
        assert!(Arc::ptr_eq(&fixture.driver.partition.value(), &before));
        assert!(fixture.handle.progress().current().tx_key.is_none());
        assert_eq!(fixture.driver.read_position, advanced);
        fixture.driver.demote().await.unwrap();
    }
}

#[tokio::test]
async fn confirmed_append_cannot_publish_without_its_staged_head() {
    let (mut fixture, operation, mut called, _finish) = attach_readback_fixture(1 << 27).await;
    let (_, mut receipt, readback) = queued_attach_readback(&mut fixture, &operation, SourcePosition(0)).await;
    fixture.driver.leader.as_mut().unwrap().resolver_mut().unwrap().take_head().unwrap();
    let before = fixture.driver.partition.value();

    let error = fixture.driver.consume(readback).await.unwrap_err();
    assert_eq!(error.code, "xtdb.indexer/leader-read-back-mismatch");
    assert_eq!(receipt.try_recv().unwrap().unwrap_err(), error);
    assert!(matches!(called.try_recv(), Err(Empty)));
    assert!(Arc::ptr_eq(&fixture.driver.partition.value(), &before));
    assert!(fixture.handle.progress().current().tx_key.is_none());
    fixture.driver.demote().await.unwrap();
}

#[tokio::test]
async fn own_transaction_readback_projects_verified_wire_bytes_without_decoding_table_data() {
    let fixture = fixture(ReplicationConfig::default(), 1 << 27).await;
    let body = proto::ResolvedTx {
        tx_id: Some(99), src_msg_id: Some(99), search_delta: Some(Bytes::from_static(b"delta")),
        table_data: [("public/large".into(), Bytes::from(vec![0xab; 64 * 1024]))].into(), ..Default::default()
    };
    let envelope = ReplicaEnvelope { term: Term(2), term_sequence: Some(TermSequence(9)), message: ReplicaBody::ResolvedTx(body) };
    let payload = Bytes::from(envelope.encode());
    let digest = crate::readback_identity::fingerprint(&payload);
    let record = LogRecord::new(LogMetadata { offset: LogOffset(1), timestamp: Timestamp(42) }, payload.clone());
    let expected = |sequence| (sequence == Some(TermSequence(9))).then_some(digest);
    let super::DecodedReplica::Known(projected) = fixture.driver.logs.decode_readback(record.clone(), None, Some(Term(2)), Some(&expected)).unwrap() else { panic!("expected replica transaction") };
    let ReplicaBody::ResolvedTx(tx) = &projected.envelope.message else { panic!("expected projected transaction") };
    assert!(tx.table_data.is_empty());
    let offset = payload.windows(5).position(|window| window == b"delta").unwrap();
    assert_eq!(tx.search_delta.as_ref().unwrap().as_ptr(), payload[offset..].as_ptr());

    let full = fixture.driver.logs.decode_replica(record).unwrap().unwrap();
    let ReplicaBody::ResolvedTx(tx) = &full.envelope.message else { panic!("expected decoded transaction") };
    assert_eq!(tx.table_data["public/large"].len(), 64 * 1024);

    let reference = envelope.offloaded(CURRENT_STORAGE_VERSION, StorageEpoch(0), BlockIndex(1), "readback".into(), &payload);
    let path = rtdb_storage::ObjectKey::new(reference.oversized_payload(CURRENT_STORAGE_VERSION, StorageEpoch(0)).unwrap().unwrap()).unwrap();
    fixture.store.put(&path, payload).await.unwrap();
    let raw = LogRecord::new(LogMetadata { offset: LogOffset(1), timestamp: Timestamp(42) }, reference.encode().into());
    let super::DecodedReplica::Known(proxy) = fixture.logs.decode_readback(raw, None, Some(Term(2)), Some(&expected)).unwrap() else { panic!("expected offload reference") };
    let projected = fixture.logs.resolve_oversized_for_readback(proxy, Some(digest)).await.unwrap();
    assert_eq!(projected.fingerprint, Some(digest));
    let ReplicaBody::ResolvedTx(tx) = &projected.envelope.message else { panic!("expected projected offload") };
    assert!(tx.table_data.is_empty());
    assert_eq!(tx.search_delta.as_deref(), Some(b"delta".as_slice()));
}

#[tokio::test]
async fn own_oversized_readback_checks_the_stored_transaction_bytes() {
    let substitutions: [Option<(&str, fn(&mut ReplicaEnvelope))>; 3] = [
        None,
        Some(("rtdb.format/oversized-payload-mismatch", |frame| {
            let ReplicaBody::ResolvedTx(message) = &mut frame.message else { panic!("expected a stored transaction"); };
            message.table_data.insert("public/forged".into(), vec![1, 2, 3].into());
        })),
        Some(("rtdb.format/oversized-payload-mismatch", |frame| {
            frame.term_sequence = Some(frame.term_sequence.unwrap().next().unwrap());
        })),
    ];
    for substitution in substitutions {
        let (mut fixture, mut operation, mut called, finish) = attach_readback_fixture(256).await;
        let DbOp::AttachDatabase(attach) = &mut operation else { panic!("expected an attach"); };
        attach.db_name = Some("a".repeat(384));
        let (key, mut receipt, readback) = queued_attach_readback(&mut fixture, &operation, SourcePosition(0)).await;
        let proxy = ReplicaEnvelope::decode(&readback.payload).unwrap().unwrap();
        let payload_key = proxy.oversized_payload(CURRENT_STORAGE_VERSION, StorageEpoch(0)).unwrap().unwrap();
        let before = fixture.driver.partition.value();
        if let Some((_, substitute)) = substitution {
            let payload_key = rtdb_storage::ObjectKey::new(payload_key).unwrap();
            let mut inner = proxy.unwrap_payload(&fixture.store.get(&payload_key).await.unwrap()).unwrap();
            substitute(&mut inner);
            fixture.store.delete(&payload_key).await.unwrap();
            fixture.store.put(&payload_key, inner.encode().into()).await.unwrap();
        }
        finish.send(Ok(())).unwrap();

        let result = fixture.driver.consume(readback).await;
        if let Some((code, _)) = substitution {
            let error = result.unwrap_err();
            assert_eq!(error.code, code);
            assert_eq!(receipt.try_recv().unwrap().unwrap_err(), error);
            assert!(matches!(called.try_recv(), Err(Empty)));
            assert!(Arc::ptr_eq(&fixture.driver.partition.value(), &before));
            assert!(fixture.handle.progress().current().tx_key.is_none());
        } else {
            result.unwrap();
            assert_eq!(called.try_recv().unwrap(), operation);
            assert!(matches!(receipt.try_recv().unwrap().unwrap(), Applied::Transaction(result) if result.tx_key() == key));
            assert_eq!(fixture.handle.progress().current().tx_key, Some(key));
        }
        fixture.driver.demote().await.unwrap();
    }
}

#[tokio::test]
async fn own_attach_readback_applies_before_the_effect_and_awaits_it_before_progress_and_receipt() {
    for effect in [Ok(()), Err(Anomaly::incorrect("test/attach-failed", "Applying the database attach failed"))] {
        let (mut fixture, operation, mut called, finish) = attach_readback_fixture(1 << 27).await;
        let (key, mut receipt, readback) = queued_attach_readback(&mut fixture, &operation, SourcePosition(1)).await;
        assert!(matches!(receipt.try_recv(), Err(Empty)));
        let partition = Arc::clone(&fixture.driver.partition);
        let progress = Arc::clone(fixture.handle.progress());

        let result = {
            let consumed = fixture.driver.consume(readback);
            tokio::pin!(consumed);
            assert!(futures::poll!(&mut consumed).is_pending());
            assert_eq!(called.try_recv().unwrap(), operation);
            assert_eq!(partition.value().live().latest_completed_tx(), Some(key));
            let before = progress.current();
            assert_eq!(before.source_position, SourcePosition(-1));
            assert!(before.tx_key.is_none());
            assert!(before.latest_result.is_none());
            assert!(matches!(receipt.try_recv(), Err(Empty)));

            finish.send(effect.clone()).unwrap();
            consumed.await
        };
        let after = progress.current();
        match effect {
            Ok(()) => {
                result.unwrap();
                assert_eq!(after.source_position, SourcePosition(1));
                assert_eq!(after.tx_key, Some(key));
                let Applied::Transaction(applied) = receipt.try_recv().unwrap().unwrap() else {
                    panic!("expected a transaction receipt");
                };
                assert_eq!(applied.tx_key(), key);
                assert!(applied.committed());
                assert!(std::ptr::eq(applied.as_ref(), after.latest_result.as_ref().unwrap().result()));
            }
            Err(error) => {
                assert_eq!(result.unwrap_err(), error);
                assert_eq!(after.source_position, SourcePosition(-1));
                assert!(after.tx_key.is_none());
                assert!(after.latest_result.is_none());
                assert_eq!(receipt.try_recv().unwrap().unwrap_err(), error);
            }
        }
        fixture.driver.demote().await.unwrap();
    }
}

struct CommitExternal;
#[async_trait]
impl rtdb_tx::TransactionWriter for CommitExternal {
    async fn write(&mut self, _: &mut rtdb_tx::OpenTransaction<'_>) -> Result<rtdb_tx::WriterResult> {
        Ok(rtdb_tx::WriterResult::Committed { user_metadata: None })
    }
}

#[tokio::test]
async fn one_clock_stamps_the_boot_claim_external_transaction_and_source_flush() {
    let time = Arc::new(AtomicI64::new(2_000_000));
    let clock: ReplicationClock = {
        let time = Arc::clone(&time);
        Arc::new(move || Timestamp(time.load(Ordering::SeqCst)))
    };
    let config = ReplicationConfig {
        external_source: true,
        assertion_interval: Duration::from_secs(60),
        flush_timeout: Duration::from_secs(60),
        ..Default::default()
    };
    let Fixture { driver, handle, source, replica, .. } = fixture_with_clock(config, 1 << 27, Some(clock)).await;
    let observer = handle.clone();
    let mut observation = tokio::spawn(async move {
        let mut status = observer.watch_status();
        status.wait_for(|status| status.role == Role::Leading(Term(1))).await.unwrap();
        let claim = replica.read_range(LogOffset(0), LogOffset(1)).await.unwrap().pop().unwrap();
        assert_eq!(claim.metadata.offset, LogOffset(0));
        assert_eq!(claim.metadata.timestamp, Timestamp(2_000_000));
        let claim = ReplicaEnvelope::decode(&claim.payload).unwrap().unwrap();
        assert_eq!(claim.term, Term(1));
        assert_eq!(claim.term_sequence, Some(TermSequence(0)));
        assert!(matches!(claim.message, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, placed_slot_reservation: None, serving_record: None })));

        time.store(3_000_000, Ordering::SeqCst);
        let result = observer.term().unwrap()
            .resolve_external(Box::new(CommitExternal), None, Some(b"clock".to_vec())).await.unwrap();
        assert!(result.committed());
        assert_eq!(result.tx_key().system_time, Timestamp(3_000_000));
        let progress = observer.progress().current();
        assert_eq!(progress.tx_key, Some(result.tx_key()));
        assert_eq!(progress.external_token.as_deref(), Some(b"clock".as_slice()));
        let transaction = replica.read_range(LogOffset(1), LogOffset(2)).await.unwrap().pop().unwrap();
        assert_eq!(transaction.metadata.timestamp, Timestamp(3_000_000));
        let transaction = ReplicaEnvelope::decode(&transaction.payload).unwrap().unwrap();
        let ReplicaBody::ResolvedTx(transaction) = transaction.message else { panic!("Expected the resolved external transaction"); };
        assert_eq!(transaction.tx_id, Some(result.tx_key().tx_id.0));
        assert_eq!(transaction.system_time_micros, Some(3_000_000));

        time.store(4_000_000, Ordering::SeqCst);
        let flush = observer.flush().await.unwrap();
        assert_eq!(flush, SourcePosition(0));
        let flush = source.read_range(LogOffset(0), LogOffset(1)).await.unwrap().pop().unwrap();
        assert_eq!(flush.metadata.timestamp, Timestamp(4_000_000));
        assert!(matches!(SourceMessage::decode(&flush.payload).unwrap(),
            Some(SourceMessage::Protobuf(SourceBody::FlushBlock(proto::FlushBlock { expected_block_idx: Some(-1) })))));
    });
    let stop = async {
        let result = tokio::time::timeout(Duration::from_secs(5), &mut observation).await;
        if result.is_err() {
            observation.abort();
            let _ = observation.await;
        }
        handle.shutdown();
        result
    };
    let (run, observation) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(driver.run(), stop)
    }).await.unwrap();
    run.unwrap();
    observation.unwrap().unwrap();
}

async fn queue_external_write(fixture: &mut Fixture, token: &[u8]) -> (ExternalReceipt, TransactionKey) {
    let receipt = fixture.handle.term().unwrap()
        .submit_external(Box::new(CommitExternal), Some(Timestamp(100)), Some(token.to_vec()))
        .await.unwrap();
    let command = fixture.driver.commands.try_recv().unwrap();
    fixture.driver.command(command).await.unwrap();
    let resolution = &mut fixture.driver.leader.as_mut().unwrap().resolution;
    let (resolver, result) = super::pending_resolution(resolution).await;
    let key = result.as_ref().unwrap().result().tx_key();
    fixture.driver.resolution_finished(resolver, result).await.unwrap();
    (receipt, key)
}

async fn queued_external_readback() -> (Fixture, [ExternalReceipt; 2], LogRecord, TransactionKey) {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, external_source: true, ..Default::default() }, 1 << 27).await;
    fixture.driver.fence.admit(Term(1), Some(TermSequence(0))).unwrap();
    fixture.driver.promote().await.unwrap();
    let (mut first, key) = queue_external_write(&mut fixture, b"first").await;
    let (mut sibling, sibling_key) = queue_external_write(&mut fixture, b"sibling").await;
    let mut tail = fixture.logs.replica_tail(fixture.driver.read_position).await.unwrap();
    let readback = tokio::time::timeout(Duration::from_secs(5), async {
        let mut first = None;
        loop {
            for raw in tail.poll(Duration::from_secs(5)).await.unwrap() {
                let record = fixture.logs.decode_replica(raw.clone()).unwrap().unwrap();
                match &record.envelope.message {
                    ReplicaBody::ResolvedTx(message) => {
                        if let Some(readback) = first.take() {
                            assert_eq!(message.tx_id, Some(sibling_key.tx_id.0));
                            return readback;
                        }
                        assert_eq!(message.tx_id, Some(key.tx_id.0));
                        first = Some(raw);
                    }
                    ReplicaBody::NoOp(message) if message.src_msg_id.is_none() => {
                        if first.is_none() {
                            fixture.driver.consume(raw).await.unwrap();
                        }
                    }
                    _ => panic!("unexpected record in external transaction readback"),
                }
            }
        }
    }).await.unwrap();
    assert!(matches!(first.receiver.try_recv(), Err(Empty)));
    assert!(matches!(sibling.receiver.try_recv(), Err(Empty)));
    (fixture, [first, sibling], readback, key)
}

async fn assert_external_readback_fault(
    fixture: &mut Fixture,
    [first, sibling]: [ExternalReceipt; 2],
    readback: LogRecord,
    expected: Anomaly,
) {
    let driver_result = fixture.driver.consume(readback).await;
    let first = first.applied();
    let sibling = sibling.applied();
    tokio::pin!(first, sibling);
    let first_result = futures::poll!(&mut first);
    let sibling_result = futures::poll!(&mut sibling);

    tokio::time::timeout(Duration::from_secs(5), fixture.driver.demote()).await.unwrap().unwrap();

    assert_eq!(driver_result.unwrap_err(), expected);
    let std::task::Poll::Ready(Err(receipt_error)) = first_result else {
        panic!("the popped transaction receipt did not fail before demotion");
    };
    assert_eq!(receipt_error, expected);
    assert!(sibling_result.is_pending());
    let sibling_error = tokio::time::timeout(Duration::from_secs(5), sibling).await.unwrap().unwrap_err();
    assert_eq!(sibling_error, super::term_ended());
}

#[tokio::test]
async fn own_external_partition_apply_fault_reaches_the_popped_transaction_receipt() {
    let (mut fixture, receipts, readback, _) = queued_external_readback().await;
    let partition = fixture.driver.partition.value();
    let competing_key = TransactionKey { tx_id: TxId(7), system_time: Timestamp(7) };
    let competing_write = rtdb_index::StagedWrite::new(
        competing_key,
        partition.live().latest_completed_tx(),
        partition.live().block_index(),
        BTreeMap::new(),
    ).unwrap();
    fixture.driver.partition.apply(&competing_write).unwrap();
    let installed = fixture.driver.partition.value();

    let expected = Anomaly::incorrect("rtdb.index/invalid-live-index", "Staged transaction no longer follows this live index");
    assert_external_readback_fault(&mut fixture, receipts, readback, expected).await;

    assert!(Arc::ptr_eq(&fixture.driver.partition.value(), &installed));
    assert_eq!(installed.live().latest_completed_tx(), Some(competing_key));
    let progress = fixture.handle.progress().current();
    assert_eq!(progress.source_position, SourcePosition(-1));
    assert!(progress.tx_key.is_none());
    assert!(progress.latest_result.is_none());
    assert!(progress.external_token.is_none());
}

#[tokio::test]
async fn own_external_progress_fault_reaches_the_popped_transaction_receipt() {
    let (mut fixture, receipts, readback, key) = queued_external_readback().await;
    let previous_token = b"published";
    fixture.handle.progress().applied(Some(SourcePosition(0)), None, Some(previous_token.to_vec())).unwrap();

    let expected = Anomaly::fault("rtdb.replication/source-regression", "Applied source watermark moved backwards");
    assert_external_readback_fault(&mut fixture, receipts, readback, expected).await;

    assert_eq!(fixture.driver.partition.value().live().latest_completed_tx(), Some(key));
    let progress = fixture.handle.progress().current();
    assert_eq!(progress.source_position, SourcePosition(0));
    assert!(progress.tx_key.is_none());
    assert!(progress.latest_result.is_none());
    assert_eq!(progress.external_token.as_deref(), Some(&previous_token[..]));
}

#[test]
fn replacement_clears_only_the_terminal_progress_it_was_prepared_for() {
    let monitor = ProgressMonitor::new(AppliedProgress {
        source_position: SourcePosition(-1), tx_key: None, latest_result: None, external_token: None,
    });
    let first = Anomaly::fault("test/first-fault", "First ingestion fault");
    assert!(monitor.prepare_driver_restart().is_none());
    monitor.driver_initialized(None).unwrap();
    monitor.ingestion_failed(first.clone());
    assert_eq!(monitor.driver_initialized(None).unwrap_err(), first);

    let restart = monitor.prepare_driver_restart();
    assert!(restart.is_some());
    monitor.driver_initialized(restart).unwrap();
    monitor.applied(Some(SourcePosition(0)), None, None).unwrap();

    monitor.ingestion_failed(first.clone());
    let restart = monitor.prepare_driver_restart();
    monitor.ingestion_failed(first.clone());
    assert_eq!(monitor.driver_initialized(restart).unwrap_err(), first);
    let restart = monitor.prepare_driver_restart();
    monitor.ingestion_failed(Anomaly::fault("test/later-fault", "Later ingestion fault"));
    assert_eq!(monitor.driver_initialized(restart).unwrap_err(), first);
    assert_eq!(monitor.fault(), Some(first));
    assert_eq!(monitor.current().source_position, SourcePosition(0));

    monitor.driver_restarted();
    monitor.stopped();
    let restart = monitor.prepare_driver_restart();
    monitor.stopped();
    assert_eq!(monitor.driver_initialized(restart).unwrap_err(), crate::progress::stopped());
    let restart = monitor.prepare_driver_restart();
    monitor.driver_initialized(restart).unwrap();
    monitor.applied(Some(SourcePosition(1)), None, None).unwrap();
}

async fn placed_driver(replica: Arc<MemoryLog>, config: ReplicationConfig) -> (ReplicationHandle, ReplicationDriver) {
    let store = Arc::new(MemoryStore::default());
    let memory = MemoryBudget::new(1 << 28);
    let cache = Arc::new(BufferPool::new(store.clone(), 1 << 25));
    let blocks = BlockStore::new(store.clone(), Arc::clone(&cache), Arc::clone(&memory), 100_000_000).unwrap();
    let recovered = blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(0), crate::BlockDiscovery::List).await.unwrap();
    let progress = ProgressMonitor::new(recovered.progress.clone());
    let logs = PartitionLogs::new(Arc::new(MemoryLog::new(1 << 20)), replica, StorageEpoch(0), StorageEpoch(0), CURRENT_STORAGE_VERSION, StorageEpoch(0),
        store, Arc::clone(&cache), Arc::clone(&memory)).unwrap();
    let resolve = Arc::new(ResolveContext { decision_clock: None, write_event_origins: false, placed_receipt_retirement: false, owned_witness_scope: None, world_constraints: None, database: "xtdb".into(), storage: cache, memory, query_options: Default::default(), scan_accelerators: Default::default(),
        cancellation: CancellationToken::new(), default_tz: "UTC".into(), authorization: Arc::new(AllowRoles),
        execution_observer: None, tracer: None });
    ReplicationDriver::new(config, logs, blocks, recovered, progress, resolve, Arc::new(NoEffects), CancellationToken::new()).unwrap()
}

#[tokio::test]
async fn a_placed_holder_claims_without_an_election_wait_and_an_idle_term_appends_nothing() {
    let replica = Arc::new(MemoryLog::new(1 << 20));
    // A previous term exists, so the claim is not the first-ever one that ClaimWhenNoTermHasLed makes at once.
    replica.enqueue(raw(0, 1, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() })).payload, Timestamp(0)).await.unwrap().durable().await.unwrap();
    let config = ReplicationConfig { assertion_interval: Duration::from_secs(3600), claim_wait: Some(Duration::from_millis(10)), idle_heartbeat: false, ..Default::default() };
    let (handle, driver) = placed_driver(Arc::clone(&replica), config).await;
    let task = tokio::spawn(driver.run());
    let mut status = handle.watch_status();
    tokio::time::timeout(Duration::from_secs(5), status.wait_for(|status| matches!(status.role, Role::Leading(_)))).await
        .expect("the holder claims within the claim wait, not the hour-long election wait").unwrap();
    let after_claim = replica.latest().unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(replica.latest().unwrap(), after_claim, "an idle term without heartbeats appends nothing");
    handle.shutdown();
    task.await.unwrap().unwrap();
}

#[test]
fn search_adoption_requires_a_provider_version_or_terminal_publication_when_the_store_requires_it() {
    let table = TableRef::new("public", "docs");
    let tables = rtdb_index::TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let key = rtdb_format::keys::TrieKey::l0(BlockIndex(0)).unwrap();
    let catalog = rtdb_index::TrieCatalog::default().with_tries(table.clone(),
        [rtdb_index::CatalogTrie::notification(key.clone(), 1, None).unwrap()], Timestamp(1)).unwrap();
    let snapshot = rtdb_index::PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap().snapshot();
    let accepts = |publication, retire: bool, required| {
        let run = proto::SearchRun { definition_id: Some("bodyf16".into()), definition_version: Some(1), blocks: vec![0],
            digest: Some(vec![1; 32]), locator: Some(vec![1]), file_size: Some(1), source_rows: Some(1), publication,
            allocation: Some(vec![1; 16]), ..Default::default() };
        let change = proto::SearchRunChange { run: Some(run), retire_only: retire.then_some(true), ..Default::default() };
        let mut message = proto::ArtefactsAdded { storage_version: Some(CURRENT_STORAGE_VERSION.0), storage_epoch: Some(0),
            table_name: Some(table.schema_and_table()), entries: vec![proto::ArtefactEntry { trie_key: Some(key.to_string()),
                record: Some(change.to_artefact().unwrap()) }], ..Default::default() };
        accept_artefacts(&mut message, &snapshot, CURRENT_STORAGE_VERSION.0, 0, required);
        !message.entries.is_empty()
    };
    let address = proto::PublishedObjectVersion { bucket: Some("bucket".into()), key: Some("path".into()), ..Default::default() };
    assert!(!accepts(None, false, true));
    assert!(!accepts(Some(address.clone()), false, true));
    assert!(!accepts(Some(proto::PublishedObjectVersion { version: Some("null".into()), ..address.clone() }), false, true));
    assert!(accepts(Some(proto::PublishedObjectVersion { version: Some("provider-version".into()), ..address.clone() }), false, true));
    assert!(accepts(Some(proto::PublishedObjectVersion { terminal: Some(proto::TerminalPublication {
        allocation: Some(vec![1; 16]), etag: Some("\"verified-tag\"".into()),
    }), ..address }), false, true));
    assert!(accepts(None, false, false));
    assert!(accepts(None, true, true));
}

fn search_intent_fixture() -> (TableRef, rtdb_index::PartitionState, proto::SearchRun, proto::ArtefactsAdded) {
    use rtdb_format::keys::TrieKey;
    use rtdb_index::{CatalogTrie, PartitionState, PartitionValue, TableCatalog, TrieCatalog};

    let table = TableRef::new("public", "docs");
    let tables = TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let anchor = TrieKey::l0(BlockIndex(0)).unwrap();
    let catalog = TrieCatalog::default().with_tries(table.clone(),
        [CatalogTrie::notification(anchor.clone(), 1, None).unwrap()], Timestamp(1)).unwrap();
    let state = PartitionState::new(PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap());
    let run = proto::SearchRun { definition_id: Some("bodyf16".into()), definition_version: Some(1), blocks: vec![0],
        digest: Some(vec![1; 32]), locator: Some(vec![1]), file_size: Some(1), source_rows: Some(1),
        allocation: Some(vec![2; 16]), ..Default::default() };
    let change = proto::SearchRunChange { run: Some(run.clone()), ..Default::default() };
    let request = proto::ArtefactsAdded { storage_version: Some(CURRENT_STORAGE_VERSION.0), storage_epoch: Some(0),
        table_name: Some(table.schema_and_table()), entries: vec![proto::ArtefactEntry {
            trie_key: Some(anchor.to_string()), record: Some(change.to_artefact().unwrap()),
        }], ..Default::default() };
    (table, state, run, request)
}

#[tokio::test]
async fn search_intent_admission_rejects_an_abandoned_allocation_after_pruning() {
    use rtdb_format::{keys::TrieKey, search_manifest::StagedSearchRun};
    use rtdb_storage::ObjectKey;

    let (table, state, run, request) = search_intent_fixture();
    let anchor = TrieKey::l0(BlockIndex(0)).unwrap();
    let snapshot = state.snapshot();
    let marker = ObjectKey::new(run.pending_key(&snapshot.table_entry(&table).unwrap().slug).unwrap()).unwrap();
    let store = MemoryStore::default();
    let stop = CancellationToken::new();
    let staged = StagedSearchRun { run: run.clone(), owner: None };
    store.put(&marker, staged.encode().unwrap().into()).await.unwrap();
    let mut before = request.clone();
    accept_artefacts(&mut before, &snapshot, CURRENT_STORAGE_VERSION.0, 0, false);
    let before = admit_search_intents(before, &snapshot, &store, &stop).await.unwrap();
    assert_eq!(before.entries, request.entries);

    let removal = proto::SearchRunChange { run: Some(run), retire_only: Some(true), ..Default::default() };
    let abandon = proto::SearchRunChange { abandon: Some(true), ..removal.clone() }.to_artefact().unwrap();
    state.add_artefacts_at(&table, vec![(anchor.clone(), abandon)], Timestamp(2));
    store.delete(&marker).await.unwrap();
    state.remove_artefacts_at(&table, vec![(anchor.clone(), removal.to_artefact().unwrap())], Some(ReplicaPosition(2)));
    let prune = proto::SearchRunChange { prune_settled_at: Some(2), ..removal }.to_artefact().unwrap();
    state.remove_artefacts_at(&table, vec![(anchor, prune)], Some(ReplicaPosition(4)));

    let snapshot = state.snapshot();
    assert_eq!(snapshot.trie_catalog().retired_search_runs(&table).count(), 0);
    let mut delayed = request;
    accept_artefacts(&mut delayed, &snapshot, CURRENT_STORAGE_VERSION.0, 0, false);
    assert_eq!(delayed.entries.len(), 1);
    let delayed = admit_search_intents(delayed, &snapshot, &store, &stop).await.unwrap();
    assert!(delayed.entries.is_empty());
}

#[tokio::test]
async fn search_intent_admission_retries_a_temporary_storage_failure_and_reports_a_persistent_one() {
    use rtdb_format::search_manifest::StagedSearchRun;
    use rtdb_storage::ObjectKey;

    let (table, state, run, request) = search_intent_fixture();
    let snapshot = state.snapshot();
    let marker = ObjectKey::new(run.pending_key(&snapshot.table_entry(&table).unwrap().slug).unwrap()).unwrap();
    let store = RecordingStore::default();
    let staged = StagedSearchRun { run, owner: None };
    store.put(&marker, staged.encode().unwrap().into()).await.unwrap();
    let stop = CancellationToken::new();

    store.head_faults.lock().push_back(HeadFault::Unavailable);
    let admitted = admit_search_intents(request.clone(), &snapshot, &store, &stop).await.unwrap();
    assert_eq!(admitted, request);

    store.head_faults.lock().extend([HeadFault::Unavailable, HeadFault::Unavailable]);
    let error = admit_search_intents(request, &snapshot, &store, &stop).await.unwrap_err();
    assert_eq!(error.code, "rtdb.storage/remote");
}

#[tokio::test]
async fn search_intent_admission_bounds_a_hanging_storage_lookup() {
    let (_, state, _, request) = search_intent_fixture();
    let snapshot = state.snapshot();
    let store = RecordingStore::default();
    let stop = CancellationToken::new();
    store.head_faults.lock().push_back(HeadFault::Hang);

    let error = tokio::time::timeout(Duration::from_secs(10), admit_search_intents(request.clone(), &snapshot, &store, &stop))
        .await.unwrap().unwrap_err();
    assert_eq!(error.code, "rtdb.replication/search-intent-timeout");

    store.head_faults.lock().push_back(HeadFault::Hang);
    let (result, ()) = tokio::join!(admit_search_intents(request, &snapshot, &store, &stop), async {
        tokio::task::yield_now().await;
        stop.cancel();
    });
    assert_eq!(result.unwrap_err().category, rtdb_types::AnomalyCategory::Interrupted);
}

#[tokio::test]
async fn search_intent_admission_keeps_a_republication_of_an_active_identity_without_its_intent() {
    let (table, state, mut run, mut request) = search_intent_fixture();
    let entry = &mut request.entries[0];
    state.add_artefacts_at(&table, vec![(entry.trie_key().parse().unwrap(), entry.record.clone().unwrap())], Timestamp(2));
    let snapshot = state.snapshot();
    assert_eq!(snapshot.trie_catalog().search_runs(&table).next(), Some(&run));

    run.publication = Some(proto::PublishedObjectVersion {
        bucket: Some("bucket".into()), key: Some("path".into()), version: Some("version".into()), ..Default::default()
    });
    entry.record = Some(proto::SearchRunChange { run: Some(run), ..Default::default() }.to_artefact().unwrap());
    let republish = request.clone();
    accept_artefacts(&mut request, &snapshot, CURRENT_STORAGE_VERSION.0, 0, true);
    let admitted = admit_search_intents(request, &snapshot, &MemoryStore::default(), &CancellationToken::new()).await.unwrap();
    assert_eq!(admitted, republish);
}

#[tokio::test]
async fn an_ordered_search_prune_releases_the_recovery_gate_after_source_append() {
    let fixture = fixture(ReplicationConfig::default(), 1 << 20).await;
    let files = tempfile::tempdir().unwrap();
    let store = rtdb_storage::LocalStore::open(files.path()).unwrap().with_catalog_ledger(StorageEpoch(0));
    let gate = store.try_lock_recovery_deletion().await.unwrap().unwrap();
    let cancellation = CancellationToken::new();
    let removal = proto::ArtefactsRemoved { storage_version: Some(CURRENT_STORAGE_VERSION.0), ..Default::default() };
    let mut publish = Box::pin(fixture.handle.remove_artefacts_retaining(removal, &cancellation, Some(gate)));

    assert!(futures::poll!(&mut publish).is_pending());
    assert_eq!(fixture.source.latest().unwrap(), LogOffset(0));
    assert!(store.try_lock_recovery_deletion().await.unwrap().is_some());
    cancellation.cancel();
    assert_eq!(publish.await.unwrap_err().category, rtdb_types::AnomalyCategory::Interrupted);
}

#[test]
fn a_group_objects_file_is_deleted_or_dropped_from_caches_only_with_the_groups_last_member() {
    use rtdb_format::keys::{ArtefactOwner, TrieKey};
    let table = TableRef::new("public", "docs");
    let tables = rtdb_index::TableCatalog::default().with_seeded_table(table.clone(), Vec::new()).unwrap();
    let slug = tables.entry(&table).unwrap().slug.clone();
    let add = |catalog: rtdb_index::TrieCatalog, key: &str| catalog.with_tries(table.clone(), [rtdb_index::CatalogTrie::notification(key.parse().unwrap(), 1, None).unwrap()], Timestamp(100)).unwrap();
    let mut catalog = rtdb_index::TrieCatalog::new(16).unwrap();
    for bucket in 0..4 { catalog = add(catalog, &format!("l02-rc-p{bucket}-b00")); }
    for bucket in 0..4 { catalog = add(catalog, &format!("l03-rc-p0{bucket}-b01")); }
    let record = proto::ArtefactRecord { kind: Some("search".into()), definition_id: Some("body".into()), definition_version: Some(1), stats: None, scope: Some(proto::ArtefactScope::PartGroup as i32), file_size: None, published_version: None, pack_members: Vec::new() };
    let value = rtdb_index::PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap();
    let group = |key: &str| key.parse::<TrieKey>().unwrap().part_group().unwrap();
    let object = |key: &str| rtdb_storage::ObjectKey::new(slug.owner_artefact_key(&ArtefactOwner::Group(group(key)), "search", "body", 1).unwrap()).unwrap();
    let message = |keys: &[&str]| proto::ArtefactsAdded { storage_version: Some(CURRENT_STORAGE_VERSION.0), storage_epoch: Some(0), table_name: Some(table.schema_and_table()),
        entries: keys.iter().map(|key| proto::ArtefactEntry { trie_key: Some((*key).into()), record: Some(record.clone()) }).collect(), source_msg_id: None };
    let members = ["l02-rc-p0-b00", "l02-rc-p1-b00", "l02-rc-p2-b00", "l02-rc-p3-b00"];
    let mut partly_superseded = message(&members);
    assert!(accept_artefacts(&mut partly_superseded, &value.snapshot(), CURRENT_STORAGE_VERSION.0, 0, false).is_empty(), "the superseded member's siblings still take the file");
    assert_eq!(partly_superseded.entries.len(), 3);
    let mut unheld = message(&["l02-rc-p0-b05", "l02-rc-p1-b05"]);
    assert_eq!(accept_artefacts(&mut unheld, &value.snapshot(), CURRENT_STORAGE_VERSION.0, 0, false), [object("l02-rc-p0-b05")], "a group the catalog holds no member of loses its file once");
    let one: BTreeSet<TrieKey> = ["l02-rc-p0-b00".parse().unwrap()].into();
    let recorded = rtdb_index::PartitionValue::loaded(value.trie_catalog().with_artefacts(&table, members.iter().map(|key| (key.parse().unwrap(), record.clone()))), value.table_catalog().as_ref().clone(), TrieConfig::default()).unwrap();
    assert!(!cached_objects(&recorded, &table, &one).contains(&object("l02-rc-p0-b00")));
    let all: BTreeSet<TrieKey> = members.iter().map(|key| key.parse().unwrap()).collect();
    assert_eq!(cached_objects(&recorded, &table, &all).iter().filter(|key| **key == object("l02-rc-p0-b00")).count(), 1);
}

#[tokio::test]
async fn a_promotion_that_inherits_a_flushed_block_resumes_the_source_after_the_flush() {
    let config = ReplicationConfig { assertion_interval: Duration::from_millis(10), claim_wait: Some(Duration::from_millis(10)), ..Default::default() };
    let fixture = fixture(config, 1 << 27).await;
    let first = submit_recovery_row(&fixture.handle, &fixture.memory, 1, 100).await;
    let flush = fixture.logs.append_source(&SourceMessage::Protobuf(SourceBody::FlushBlock(proto::FlushBlock { expected_block_idx: Some(-1) })), Timestamp(200)).await.unwrap();
    // The previous leader read back its transaction and cut the flush's boundary, then died before uploading the block.
    for record in [
        raw(0, 1, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() })),
        raw(1, 1, 1, resolved(first.0, first.0)),
        raw(2, 1, 2, boundary(0, flush.0)),
    ] {
        fixture.replica.enqueue(record.payload, record.metadata.timestamp).await.unwrap().durable().await.unwrap();
    }
    let next = submit_recovery_row(&fixture.handle, &fixture.memory, 2, 300).await;

    let handle = fixture.handle.clone();
    let driver = tokio::spawn(fixture.driver.run());
    let result = tokio::time::timeout(Duration::from_secs(5), handle.await_tx(TxId(next.0), &CancellationToken::new())).await.unwrap().unwrap().unwrap();
    assert_eq!(result.tx_key().tx_id, TxId(next.0));
    assert!(handle.status().adopted_block.is_some());
    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(5), driver).await.unwrap().unwrap().unwrap();

    let replica = fixture.replica.read_range(LogOffset(3), LogOffset(fixture.replica.latest().unwrap().0 + 1)).await.unwrap();
    let later_sources: Vec<_> = replica.iter().filter_map(|record| match ReplicaEnvelope::decode(&record.payload).unwrap().unwrap().message {
        ReplicaBody::ResolvedTx(message) => message.src_msg_id,
        ReplicaBody::NoOp(message) => message.src_msg_id,
        _ => None,
    }).collect();
    assert_eq!(later_sources, vec![next.0], "the new term resolves each later source message once and nothing at or before the flush");
}

#[tokio::test]
async fn exact_and_restored_recovery_require_the_selected_checkpoint() {
    use rtdb_format::{block::proto::Block, catalog::{BlockCatalog, TableEntry}, keys::block_key};
    use rtdb_storage::ObjectKey;
    use rtdb_types::ReplicaPosition;

    let fixture = fixture(ReplicationConfig::default(), 4096).await;
    let old = ObjectKey::new(block_key(BlockIndex(0)).unwrap()).unwrap();
    let latest = ObjectKey::new(block_key(BlockIndex(1)).unwrap()).unwrap();
    let incomplete = BlockCatalog::from_proto(Block {
        block_index: Some(0),
        tables: vec![TableEntry::new(16_384, TableRef::new("public", "docs")).unwrap().to_proto()],
        ..Default::default()
    }).unwrap();
    let complete = BlockCatalog::from_proto(Block { block_index: Some(1), ..Default::default() }).unwrap();
    fixture.store.put(&old, incomplete.encode().into()).await.unwrap();
    fixture.store.put(&latest, complete.encode().into()).await.unwrap();

    let exact = fixture.blocks.recover(
        TrieConfig::default(), SourcePosition(-1), Timestamp(1000), BlockDiscovery::At(BlockIndex(0)),
    ).await.err().unwrap();
    assert_eq!(exact.code, "rtdb.storage/missing-object");

    fixture.store.delete(&old).await.unwrap();
    let restored = fixture.blocks.recover(
        TrieConfig::default(), SourcePosition(-1), Timestamp(1000),
        BlockDiscovery::FromRestored { checkpoint: BlockIndex(0), compaction_cutoff: ReplicaPosition(0), root_sha256: None },
    ).await.err().unwrap();
    assert_eq!(restored.code, "rtdb.replication/block-gone");
}

#[tokio::test]
async fn startup_replay_preserves_exact_and_restored_anchors_across_prefix_loss() {
    let fixture = produced_memory_checkpoint(true).await;
    produce_newer_memory_checkpoint(&fixture).await;
    let (_, mut writer) = reopen_memory_checkpoint_with_config(&fixture, ReplicationConfig {
        may_lead: false, ..Default::default()
    }).await;
    deliver_checkpoint_record(&fixture, &mut writer, 3, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() })).await.unwrap();
    deliver_checkpoint_record(&fixture, &mut writer, 3, 1, boundary(2, -1)).await.unwrap();
    let pending = writer.pending_block.as_ref().unwrap();
    let later = writer.blocks.produce(&pending.prepared, &pending.boundary, BTreeMap::new()).await.unwrap();
    assert_eq!(later.catalog.block_index(), BlockIndex(2));
    fixture.store.delete(&rtdb_storage::ObjectKey::new(rtdb_format::keys::block_key(BlockIndex(1)).unwrap()).unwrap()).await.unwrap();
    fixture.replica.truncate_before(LogOffset(5)).unwrap();
    let boundary = rtdb_types::ReplicaPosition(1);
    assert!(matches!(fixture.logs.recovery_tail(boundary).await,
        Err(rtdb_storage::Error::UnavailableLogRange { requested: 2, first_retained: 5 })));

    let cancellation = CancellationToken::new();
    for (discovery, cutoff) in [
        (BlockDiscovery::At(BlockIndex(0)), None),
        (BlockDiscovery::FromRestored { checkpoint: BlockIndex(0), compaction_cutoff: boundary, root_sha256: None }, Some(boundary)),
    ] {
        let recovered = fixture.blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(0), discovery).await.unwrap();
        let selected = fixture.blocks.recover_for_replay(recovered, &fixture.logs, TrieConfig::default(), Timestamp(0), &cancellation).await.unwrap();
        assert_eq!(selected.block.unwrap().block_index(), BlockIndex(0));
        assert_eq!(selected.compaction_replay_cutoff, cutoff);
    }
}

enum HeadFault { Unavailable, Hang }

#[derive(Default)]
struct RecordingStore {
    inner: Arc<MemoryStore>,
    reads: Mutex<BTreeSet<rtdb_storage::ObjectKey>>,
    head_faults: Mutex<VecDeque<HeadFault>>,
}
impl RecordingStore {
    fn read(&self, key: &rtdb_storage::ObjectKey) { self.reads.lock().insert(key.clone()); }
    fn take(&self) -> BTreeSet<rtdb_storage::ObjectKey> { std::mem::take(&mut *self.reads.lock()) }
}
#[async_trait]
impl ObjectStore for RecordingStore {
    async fn head(&self, key: &rtdb_storage::ObjectKey) -> rtdb_storage::Result<rtdb_storage::StoredObject> { self.read(key); self.inner.head(key).await }
    async fn head_fresh(&self, key: &rtdb_storage::ObjectKey) -> rtdb_storage::Result<rtdb_storage::StoredObject> {
        self.read(key);
        let fault = self.head_faults.lock().pop_front();
        match fault {
            Some(HeadFault::Unavailable) => Err(rtdb_storage::Error::Remote { key: key.to_string(), message: "HTTP 503".into() }),
            Some(HeadFault::Hang) => std::future::pending().await,
            None => self.inner.head_fresh(key).await,
        }
    }
    async fn get(&self, key: &rtdb_storage::ObjectKey) -> rtdb_storage::Result<Bytes> { self.read(key); self.inner.get(key).await }
    async fn get_fresh(&self, key: &rtdb_storage::ObjectKey) -> rtdb_storage::Result<Bytes> { self.read(key); self.inner.get_fresh(key).await }
    async fn get_fresh_exact(&self, key: &rtdb_storage::ObjectKey, length: usize) -> rtdb_storage::Result<Bytes> { self.read(key); self.inner.get_fresh_exact(key, length).await }
    async fn get_range(&self, key: &rtdb_storage::ObjectKey, range: std::ops::Range<u64>) -> rtdb_storage::Result<Bytes> { self.read(key); self.inner.get_range(key, range).await }
    async fn put(&self, key: &rtdb_storage::ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> { self.inner.put(key, bytes).await }
    async fn put_file(&self, key: &rtdb_storage::ObjectKey, path: &std::path::Path) -> rtdb_storage::Result<()> { self.inner.put_file(key, path).await }
    async fn list(&self, key: Option<&rtdb_storage::ObjectKey>) -> rtdb_storage::Result<Vec<rtdb_storage::StoredObject>> { self.inner.list(key).await }
    async fn delete(&self, key: &rtdb_storage::ObjectKey) -> rtdb_storage::Result<()> { self.inner.delete(key).await }
}

/// A suffix replayed onto block 0 by a follower whose every object read was recorded.
struct ReplayedSuffix {
    base: BTreeSet<rtdb_storage::ObjectKey>,
    tail: BTreeSet<rtdb_storage::ObjectKey>,
    payloads: BTreeSet<rtdb_storage::ObjectKey>,
    reads: BTreeSet<rtdb_storage::ObjectKey>,
    stale_payload: rtdb_storage::ObjectKey,
    adopted: BTreeSet<rtdb_storage::ObjectKey>,
    checkpoint: rtdb_format::catalog::BlockCatalog,
    envelopes: Vec<ReplicaEnvelope>,
    blocks: Arc<BlockStore>,
}

fn produced_upload(produced: &ProducedBlock, index: i64, source: i64) -> ReplicaBody {
    ReplicaBody::BlockUploaded(proto::BlockUploaded {
        storage_version: Some(CURRENT_STORAGE_VERSION.0), storage_epoch: Some(0), block_index: Some(index), latest_processed_msg_id: Some(source),
        tries: produced.tries.iter().flat_map(|(table, tries)| tries.iter().map(|trie| trie.to_proto(table))).collect(),
        external_source_token: None, catalog_sha256: Some(produced.catalog.publication_sha256().unwrap().to_vec()),
    })
}

#[tokio::test]
async fn native_coordinated_startup_refuses_a_checkpoint_missing_primary_data() {
    use rtdb_storage::LocalStore;

    let replay = replayed_suffix(false).await;
    let (root, tables) = replay.blocks.load(BlockIndex(0)).await.unwrap();
    let objects = checkpoint_dependencies(&root, &tables).unwrap();
    let primary_data = objects.iter().find(|key| key.as_str().contains("/data/")).unwrap().clone();
    let path = tempfile::tempdir().unwrap();
    let local = Arc::new(LocalStore::open(path.path()).unwrap().with_catalog_ledger(StorageEpoch(0)));
    for object in objects {
        let bytes = replay.blocks.object_store().get_fresh(&object).await.unwrap();
        local.put(&object, bytes).await.unwrap();
    }
    let store: Arc<dyn ObjectStore> = local.clone();
    let blocks = BlockStore::new(store.clone(), Arc::new(BufferPool::new(store, 1 << 20)),
        MemoryBudget::new(1 << 26), 100_000_000).unwrap();
    let ready = || blocks.recover(TrieConfig::new(2, 2).unwrap(), SourcePosition(-1), Timestamp(0), BlockDiscovery::At(BlockIndex(0)));
    let hold = local.hold_recovery_open().await.unwrap();
    assert!(ready().await.is_ok());
    local.delete(&primary_data).await.unwrap();
    let missing = ready().await.err().unwrap();
    assert_eq!(missing.code, "rtdb.storage/missing-object");
    local.put(&primary_data, Bytes::from_static(b"x")).await.unwrap();
    let damaged = ready().await.err().unwrap();
    assert_eq!(damaged.code, "rtdb.replication/checkpoint-primary-extent");
    drop(hold);
}

async fn offloaded(store: &MemoryStore, position: i64, sequence: i64, body: ReplicaBody, block: BlockIndex) -> (LogRecord, rtdb_storage::ObjectKey) {
    let envelope = ReplicaEnvelope { term: Term(1), term_sequence: Some(TermSequence(sequence)), message: body };
    let payload = envelope.encode();
    let id = format!("closure-{sequence}");
    let proxy = envelope.offloaded(CURRENT_STORAGE_VERSION, StorageEpoch(0), block, id.clone(), &payload);
    let key = rtdb_storage::ObjectKey::new(rtdb_format::keys::oversized_key(block, &id).unwrap()).unwrap();
    store.put(&key, payload.into()).await.unwrap();
    (LogRecord::new(LogMetadata { offset: LogOffset(position), timestamp: Timestamp(position * 100 + 1) }, proxy.encode().into()), key)
}

async fn replayed_suffix(oversized_upload: bool) -> ReplayedSuffix {
    let config = || ReplicationConfig { may_lead: false, ..Default::default() };
    let store = Arc::new(MemoryStore::default());
    let mut producer = fixture_over(config(), 1 << 27, None, |replica| replica, Arc::clone(&store), store.clone()).await;
    let table = TableRef::new("public", "docs");
    let key = TransactionKey { tx_id: TxId(0), system_time: Timestamp(100) };
    let mut write = TransactionBuffer::new(producer.driver.partition.value().snapshot(), key, Arc::clone(&producer.memory)).unwrap();
    write.put(table.clone(), StructFields::from([("_id".into(), Value::I64(1))]), Interval::ALL).unwrap();
    producer.driver.partition.apply(&write.stage().unwrap()).unwrap();
    producer.handle.progress().applied(None, Some(Arc::new(rtdb_tx::TransactionResult::Committed { tx_key: key }).into()), None).unwrap();
    producer.driver.consume(raw(0, 1, 0, boundary(0, -1))).await.unwrap();
    let pending = producer.driver.pending_block.as_ref().unwrap();
    let first = producer.blocks.produce(&pending.prepared, &pending.boundary, BTreeMap::new()).await.unwrap();
    let mut suffix = vec![raw(1, 1, 1, produced_upload(&first, 0, -1))];
    producer.driver.consume(suffix[0].clone()).await.unwrap();

    let recording = Arc::new(RecordingStore { inner: Arc::clone(&store), ..Default::default() });
    let mut replayer = fixture_over(config(), 1 << 27, None, |replica| replica, Arc::clone(&store), recording.clone()).await;
    let (base, base_tables) = producer.blocks.load(BlockIndex(0)).await.unwrap();
    let base_keys = checkpoint_dependencies(&base, &base_tables).unwrap();
    let recovery = recording.take();
    assert!(recovery.is_subset(&base_keys), "recovery read outside the checkpoint closure: {:?}", recovery.difference(&base_keys).collect::<Vec<_>>());

    suffix.extend([raw(2, 1, 2, resolved(1, -1)), raw(3, 1, 3, boundary(1, -1))]);
    for record in &suffix[1..] { producer.driver.consume(record.clone()).await.unwrap(); }
    let pending = producer.driver.pending_block.as_ref().unwrap();
    let second = producer.blocks.produce(&pending.prepared, &pending.boundary, BTreeMap::new()).await.unwrap();
    let mut payloads = BTreeSet::new();
    if oversized_upload {
        let (record, payload) = offloaded(&store, 4, 4, produced_upload(&second, 1, -1), BlockIndex(1)).await;
        suffix.push(record);
        payloads.insert(payload);
    } else {
        suffix.push(raw(4, 1, 4, produced_upload(&second, 1, -1)));
    }

    let artefact = proto::ArtefactRecord { kind: Some("search".into()), definition_id: Some("body".into()), definition_version: Some(1), ..Default::default() };
    let compacted = rtdb_format::keys::TrieKey::l0(BlockIndex(5)).unwrap();
    suffix.push(raw(5, 1, 5, ReplicaBody::TriesAdded(proto::TriesAdded {
        storage_version: Some(CURRENT_STORAGE_VERSION.0), storage_epoch: Some(0), source_msg_id: Some(2),
        tries: vec![proto::TrieDetails { table_name: Some(table.schema_and_table()), trie_key: Some(compacted.to_string()), trie_state: Some(proto::TrieState::Live as i32),
            artefacts: vec![artefact.clone()], ..Default::default() }],
    })));
    let flushed = first.tries[&table][0].key.clone();
    suffix.push(raw(6, 1, 6, ReplicaBody::ArtefactsAdded(proto::ArtefactsAdded {
        storage_version: Some(CURRENT_STORAGE_VERSION.0), storage_epoch: Some(0), table_name: Some(table.schema_and_table()), source_msg_id: Some(3),
        entries: vec![proto::ArtefactEntry { trie_key: Some(flushed.to_string()), record: Some(artefact.clone()) }],
    })));
    let (stale, stale_payload) = offloaded(&store, 7, 7, produced_upload(&first, 0, -1), BlockIndex(0)).await;
    suffix.push(stale);
    payloads.insert(stale_payload.clone());

    for record in &suffix { replayer.driver.consume(record.clone()).await.unwrap(); }
    assert_eq!(replayer.driver.partition.value().table_catalog().block_index(), Some(BlockIndex(1)));
    let reads = recording.take();

    let envelopes = suffix.iter().map(|record| ReplicaEnvelope::decode(&record.payload).unwrap().unwrap()).collect::<Vec<_>>();
    let tail = tail_dependencies(replay_base(&base), &envelopes, producer.blocks.as_ref()).await.unwrap();
    let slug = base.table_entries().unwrap().into_iter().find(|entry| entry.table == table).unwrap().slug;
    let adopted = [rtdb_format::keys::block_key(BlockIndex(1)).unwrap(), slug.block_key(BlockIndex(1)).unwrap()]
        .into_iter().map(|key| rtdb_storage::ObjectKey::new(key).unwrap()).collect();
    for expected in [slug.data_key(&compacted), slug.metadata_key(&compacted),
        slug.recorded_artefact_key(&compacted, &artefact).unwrap(), slug.recorded_artefact_key(&flushed, &artefact).unwrap()]
    {
        assert!(tail.contains(&rtdb_storage::ObjectKey::new(expected.clone()).unwrap()), "the added trie or artefact {expected} is not enumerated");
    }
    ReplayedSuffix { base: base_keys, tail, payloads, reads, stale_payload, adopted, checkpoint: base, envelopes, blocks: Arc::clone(&producer.blocks) }
}

fn replay_base(checkpoint: &rtdb_format::catalog::BlockCatalog) -> ReplayBase<'_> {
    ReplayBase { storage_version: CURRENT_STORAGE_VERSION, storage_epoch: StorageEpoch(0), checkpoint: Some(checkpoint) }
}

impl ReplayedSuffix {
    fn outside(&self, closure: &BTreeSet<rtdb_storage::ObjectKey>) -> Vec<&rtdb_storage::ObjectKey> {
        self.reads.iter().filter(|key| !self.base.contains(key) && !closure.contains(key)).collect()
    }
}

#[tokio::test]
async fn inline_upload_replay_reads_only_its_typed_dependency_closure() {
    let replayed = replayed_suffix(false).await;
    assert_eq!(replayed.outside(&replayed.tail), Vec::<&rtdb_storage::ObjectKey>::new());
    assert!(replayed.adopted.is_subset(&replayed.reads), "the inline upload adopts block 1's root and table block");
    assert!(replayed.reads.contains(&replayed.stale_payload), "a stale oversized body still fetches its payload");
    assert!(replayed.tail.contains(&replayed.stale_payload));
}

#[tokio::test]
async fn oversized_upload_replay_reads_only_its_typed_dependency_closure() {
    let replayed = replayed_suffix(true).await;
    assert_eq!(replayed.outside(&replayed.tail), Vec::<&rtdb_storage::ObjectKey>::new());
    assert!(replayed.adopted.is_subset(&replayed.reads), "the decoded upload adopts block 1's root and table block");
    assert!(replayed.payloads.is_subset(&replayed.reads) && replayed.payloads.is_subset(&replayed.tail));
}

#[tokio::test]
async fn payload_only_closure_misses_the_catalog_an_upload_adopts() {
    for oversized_upload in [false, true] {
        let replayed = replayed_suffix(oversized_upload).await;
        let missed = replayed.outside(&replayed.payloads);
        assert!(!missed.is_empty(), "a payload-only closure must miss a recorded read");
        assert!(replayed.adopted.iter().all(|key| missed.contains(&key)), "it misses the adopted root and table block, read {missed:?}");
    }
}

#[tokio::test]
async fn stale_and_foreign_bodies_contribute_only_an_oversized_payload() {
    let replayed = replayed_suffix(false).await;
    let stale = replayed.envelopes.last().unwrap();
    let ReplicaBody::TriesAdded(mut foreign) = replayed.envelopes[4].message.clone() else { panic!("expected the added tries"); };
    foreign.storage_epoch = Some(1);
    let foreign = ReplicaEnvelope { message: ReplicaBody::TriesAdded(foreign), ..replayed.envelopes[4].clone() };
    let tail = tail_dependencies(replay_base(&replayed.checkpoint), [stale, &foreign], replayed.blocks.as_ref()).await.unwrap();
    assert_eq!(tail, BTreeSet::from([replayed.stale_payload.clone()]));
}

/// A journal that fences writer terms, counting what it charges to the maintenance lane.
struct FencedTermLog {
    log: Arc<MemoryLog>,
    holds_grant: std::sync::atomic::AtomicBool,
    maintenance: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl Log for FencedTermLog {
    fn fences_writer_terms(&self) -> bool { true }
    async fn holds_writer_grant(&self) -> rtdb_storage::Result<bool> {
        Ok(self.holds_grant.load(std::sync::atomic::Ordering::SeqCst))
    }
    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> rtdb_storage::Result<rtdb_storage::log::AppendReceipt> {
        self.log.enqueue(payload, timestamp).await
    }
    async fn enqueue_maintenance(&self, payload: Bytes, timestamp: Timestamp) -> rtdb_storage::Result<rtdb_storage::log::AppendReceipt> {
        self.maintenance.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.log.enqueue_maintenance(payload, timestamp).await
    }
    fn latest(&self) -> rtdb_storage::Result<LogOffset> { self.log.latest() }
    async fn tail(&self, after: LogOffset) -> rtdb_storage::Result<Box<dyn rtdb_storage::log::LogTail>> { self.log.tail(after).await }
    async fn range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Box<dyn rtdb_storage::log::LogRange>> { self.log.range(from, to).await }
    async fn read_range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Vec<LogRecord>> { self.log.read_range(from, to).await }
}

async fn fenced_term_fixture(holds_grant: bool) -> (Fixture, Arc<FencedTermLog>) {
    let fenced = std::sync::Mutex::new(None);
    let fixture = fixture_with_replica(ReplicationConfig { may_lead: false, ..Default::default() }, 1 << 27, None, |log| {
        let log = Arc::new(FencedTermLog { log, holds_grant: holds_grant.into(), maintenance: 0.into() });
        *fenced.lock().unwrap() = Some(Arc::clone(&log));
        log
    }).await;
    let fenced = fenced.into_inner().unwrap().unwrap();
    (fixture, fenced)
}

fn leading_config() -> ReplicationConfig {
    ReplicationConfig { assertion_interval: Duration::from_millis(1), claim_wait: Some(Duration::from_millis(1)), ..Default::default() }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_journal_fenced_term_opens_without_a_claim_or_maintenance_record_across_replacements() {
    let (fixture, fenced) = fenced_term_fixture(true).await;
    let mut terms = Vec::new();
    for _ in 0..6 {
        let (handle, driver) = reopen_memory_checkpoint_with_config(&fixture, leading_config()).await;
        let mut status = handle.watch_status();
        let running = tokio::spawn(driver.run());
        let term = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Role::Leading(term) = status.borrow_and_update().role { break term; }
                status.changed().await.unwrap();
            }
        }).await.unwrap();
        let written = fixture.replica.latest().unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while fixture.replica.latest().unwrap() <= written { tokio::time::sleep(Duration::from_millis(1)).await; }
        }).await.unwrap();
        handle.shutdown();
        tokio::time::timeout(Duration::from_secs(5), running).await.unwrap().unwrap().unwrap();
        assert!(handle.progress().fault().is_none());
        terms.push(term);
    }
    assert_eq!(terms, (1..=6).map(Term).collect::<Vec<_>>());
    assert_eq!(fenced.maintenance.load(std::sync::atomic::Ordering::SeqCst), 0,
        "replacements without block progress charge nothing to the maintenance lane");

    // Each term begins at sequence zero, and a follower admits every term without voiding one.
    let records = fixture.replica.read_range(LogOffset(0), LogOffset(fixture.replica.latest().unwrap().0 + 1)).await.unwrap();
    let mut fence = crate::TermFence::from_boundary(Term(0), None).unwrap();
    let mut opened = Vec::new();
    for record in records {
        let envelope = ReplicaEnvelope::decode(&record.payload).unwrap().unwrap();
        let admission = fence.admit(envelope.term, envelope.term_sequence).unwrap();
        assert_ne!(admission.verdict, crate::Verdict::Voided);
        if admission.opens_term() {
            assert_eq!(envelope.term_sequence, Some(TermSequence(0)));
            opened.push(envelope.term);
        }
    }
    assert_eq!(opened, terms);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_worker_without_the_current_grant_never_leads_a_journal_fenced_term() {
    let (fixture, fenced) = fenced_term_fixture(false).await;
    let (handle, driver) = reopen_memory_checkpoint_with_config(&fixture, leading_config()).await;
    let running = tokio::spawn(driver.run());
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(handle.status().role, Role::Following);
    assert_eq!(fixture.replica.latest().unwrap(), LogOffset(-1));
    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(5), running).await.unwrap().unwrap().unwrap();
    assert_eq!(fenced.maintenance.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn fenced_appends_charge_only_boundaries_and_uploads_to_maintenance() {
    let (fixture, fenced) = fenced_term_fixture(true).await;
    let cases = [
        (TermSequence(0), ReplicaBody::NoOp(proto::NoOp { src_msg_id: None, ..Default::default() }), 0),
        (TermSequence(1), boundary(0, -1), 1),
        (TermSequence(2), upload(0, -1), 2),
    ];
    for (sequence, message, maintenance) in cases {
        let envelope = ReplicaEnvelope { term: Term(1), term_sequence: Some(sequence), message };
        fixture.driver.logs.enqueue_replica(&envelope, BlockIndex(0), Timestamp(0)).await.unwrap().durable().await.unwrap();
        assert_eq!(fenced.maintenance.load(std::sync::atomic::Ordering::SeqCst), maintenance);
    }
}

#[path = "driver_trace_tests.rs"]
mod driver_trace_tests;
