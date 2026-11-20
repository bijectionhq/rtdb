use std::collections::{BTreeMap, BTreeSet};
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
use rtdb_types::{Anomaly, BlockIndex, Interval, Result, SourcePosition, StorageEpoch, TableRef, Term, TermSequence, Timestamp, TransactionKey, TxId, Value};
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::TryRecvError::Empty;
use tokio_util::sync::CancellationToken;

use super::{Applied, TailPoll, accept_artefacts, cached_objects, read_replay_head, source_decode_bound, start_poll};
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
    let memory = MemoryBudget::new(1 << 30);
    let source = Arc::new(MemoryLog::new(1 << 28));
    let replica = Arc::new(MemoryLog::new(replica_message_limit));
    let store = Arc::new(MemoryStore::default());
    let cache = Arc::new(BufferPool::new(store.clone(), 1 << 27));
    let logs = PartitionLogs::new(source.clone(), replica.clone(), StorageEpoch(0), StorageEpoch(0), CURRENT_STORAGE_VERSION, StorageEpoch(0), store.clone(), Arc::clone(&cache), Arc::clone(&memory)).unwrap();
    let blocks = BlockStore::new(store.clone(), Arc::clone(&cache), Arc::clone(&memory), 100_000_000).unwrap();
    let recovered = blocks.recover(TrieConfig::new(2, 2).unwrap(), SourcePosition(-1), Timestamp(0)).await.unwrap();
    let progress = ProgressMonitor::new(recovered.progress.clone());
    let resolve = Arc::new(ResolveContext { database: "xtdb".into(), storage: cache, memory: Arc::clone(&memory), query_options: Default::default(), cancellation: CancellationToken::new(), default_tz: "UTC".into(), authorization: Arc::new(AllowRoles), execution_observer: None, tracer: None });
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
    LogRecord { metadata: LogMetadata { offset: LogOffset(position), timestamp: Timestamp(position * 100 + 1) },
        payload: ReplicaEnvelope { term: Term(term), term_sequence: Some(TermSequence(sequence)), message: body }.encode().into() }
}
fn boundary(index: i64, source: i64) -> ReplicaBody { ReplicaBody::BlockBoundary(proto::BlockBoundary { block_index: Some(index), latest_processed_msg_id: Some(source), external_source_token: None }) }
fn resolved(id: i64, source: i64) -> ReplicaBody { ReplicaBody::ResolvedTx(proto::ResolvedTx { tx_id: Some(id), system_time_micros: Some(id * 100 + 100), committed: Some(true), src_msg_id: Some(source), ..Default::default() }) }
fn upload(index: i64, source: i64) -> ReplicaBody {
    ReplicaBody::BlockUploaded(proto::BlockUploaded { storage_version: Some(CURRENT_STORAGE_VERSION.0), storage_epoch: Some(0), block_index: Some(index), latest_processed_msg_id: Some(source), tries: Vec::new(), external_source_token: None })
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
    let source_tail = logs.source_tail(logs.source_floor().unwrap()).await.unwrap();
    let replica_tail = logs.replica_tail(logs.replica_floor().unwrap()).await.unwrap();
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
    let refusal = read_replay_head(replica_tail.as_mut(), replica_head, logs.memory(), replica_decode_bound, &running).await
        .err().expect("expected a replay admission failure");
    assert_eq!(refusal.code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), blocker_bytes + 1);
    assert_eq!(replica_tail.replay().unwrap().poll_head(Duration::ZERO).await.unwrap(), Some(replica_head));
    drop(one_byte_short);

    let blocker = memory.reserve(blocker_bytes).unwrap();
    let record = read_replay_head(replica_tail.as_mut(), replica_head, logs.memory(), replica_decode_bound, &running).await.unwrap().unwrap();
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
    assert!(read_replay_head(source_tail.as_mut(), source_head, &memory, source_decode_bound, &cancelled).await.unwrap().is_none());
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(source_tail.replay().unwrap().poll_head(Duration::ZERO).await.unwrap(), Some(source_head));
    let record = read_replay_head(source_tail.as_mut(), source_head, &memory, source_decode_bound, &running).await.unwrap().unwrap();
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
    let (_, result) = start_poll(legacy.tail(LogOffset(-1)).await.unwrap(), Duration::ZERO).await;
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
    let opening = raw(0, 1, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None }));
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
    let claim = ReplicaBody::NoOp(proto::NoOp { src_msg_id: None });
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
async fn skipped_malformed_arrow_payload_is_archived_and_applied_as_an_abort() {
    let fixture = fixture(ReplicationConfig { skipped_transactions: [SourcePosition(0)].into_iter().collect(), ..Default::default() }, 1 << 27).await;
    let message = rtdb_format::envelope::SourceMessage::Protobuf(rtdb_format::envelope::SourceBody::Tx(proto::Tx {
        tx_ops: Some(vec![1, 2, 3]), default_tz: Some("UTC".into()), system_time_micros: Some(-100), ..Default::default()
    }));
    let expected = message.encode().unwrap();
    let handle = fixture.handle.clone();
    let driver = tokio::spawn(fixture.driver.run());
    let position = handle.submit_source(&message, Timestamp(100)).await.unwrap();
    assert_eq!(position, SourcePosition(0));
    let result = tokio::time::timeout(Duration::from_secs(5), handle.await_tx(TxId(0), &CancellationToken::new())).await.unwrap().unwrap().unwrap();
    assert_eq!(result.error().unwrap().code, "xtdb/skipped-tx");
    assert_eq!(result.tx_key().system_time, Timestamp(100));
    assert_eq!(fixture.store.get(&rtdb_storage::ObjectKey::new("skipped-txs/00").unwrap()).await.unwrap().as_ref(), expected.as_slice());
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
    fixture.handle.progress().applied(Some(SourcePosition(0)), Some(Arc::new(rtdb_tx::TransactionResult::Committed { tx_key: key })), None).unwrap();
    fixture.driver.consume(raw(0, 1, 0, boundary(0, 0))).await.unwrap();
    let pending = fixture.driver.pending_block.as_ref().unwrap();
    let first = fixture.blocks.produce(&pending.prepared, &pending.boundary, BTreeMap::new()).await.unwrap();
    let second = fixture.blocks.produce(&pending.prepared, &pending.boundary, BTreeMap::new()).await.unwrap();
    assert_eq!(first.catalog, second.catalog);
    assert_eq!(fixture.driver.partition.value().live().block_row_count(), 8);
    assert_eq!(fixture.driver.partition.value().table_catalog().block_index(), None);
    let mut uploaded = match upload(0, 0) { ReplicaBody::BlockUploaded(message) => message, _ => unreachable!() };
    uploaded.tries = first.tries.iter().flat_map(|(table, tries)| tries.iter().map(|trie| trie.to_proto(table))).collect();
    fixture.driver.consume(raw(1, 1, 1, ReplicaBody::BlockUploaded(uploaded))).await.unwrap();
    assert_eq!(fixture.driver.partition.value().table_catalog().metadata(&table).unwrap().row_count, 8);
    assert_eq!(fixture.driver.partition.value().live().block_row_count(), 0);
    let recovered = fixture.blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(1000)).await.unwrap();
    assert_eq!(recovered.progress.tx_key, Some(key));
    assert_eq!(recovered.fence.position(), TermPosition::Expecting(TermSequence(1)));
    assert_eq!(recovered.partition.value().table_catalog().metadata(&table).unwrap().row_count, 8);
}

#[tokio::test]
async fn applying_a_deletion_drops_this_nodes_cached_copies_of_the_collected_files() {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, ..Default::default() }, 1 << 27).await;
    let state = &fixture.driver.partition;
    let key = TransactionKey { tx_id: TxId(0), system_time: Timestamp(100) };
    let mut write = TransactionBuffer::new(state.value().snapshot(), key, Arc::clone(&fixture.memory)).unwrap();
    let table = TableRef::new("public", "docs");
    write.put(table.clone(), BTreeMap::from([("_id".into(), Value::I64(1))]), Interval::ALL).unwrap();
    state.apply(&write.stage().unwrap()).unwrap();
    fixture.handle.progress().applied(Some(SourcePosition(0)), Some(Arc::new(rtdb_tx::TransactionResult::Committed { tx_key: key })), None).unwrap();
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
        let recovered = fixture.blocks.recover(TrieConfig::default(), floor, Timestamp(1000)).await.unwrap();
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
    fixture.logs.enqueue_replica(&envelope, BlockIndex(0), Timestamp(100)).await.unwrap().durable().await.unwrap();
    let record = fixture.replica.read_range(LogOffset(0), LogOffset(1)).await.unwrap().pop().unwrap();
    assert!(matches!(ReplicaEnvelope::decode(&record.payload).unwrap().unwrap().message, ReplicaBody::OversizedMessage(_)));
    fixture.driver.consume(record).await.unwrap();
    assert_eq!(fixture.driver.fence.position(), TermPosition::Expecting(TermSequence(1)));
    fixture.driver.consume(raw(1, 2, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None }))).await.unwrap();
    let missing = ReplicaBody::OversizedMessage(proto::OversizedMessage { storage_version: Some(CURRENT_STORAGE_VERSION.0), storage_epoch: Some(0), block_index: Some(0), payload_id: Some("missing".into()) });
    fixture.driver.consume(raw(2, 1, 1, missing)).await.unwrap();
    assert_eq!(fixture.driver.read_position, rtdb_types::ReplicaPosition(2));
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
    let recovered = blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(1000)).await.unwrap();
    assert_eq!(recovered.progress.tx_key.unwrap().tx_id, TxId(position.0));
    assert_eq!(recovered.partition.value().table_catalog().metadata(&TableRef::new("public", "docs")).unwrap().row_count, 1);
}

#[tokio::test]
async fn the_leader_records_artefacts_of_live_tries_and_deletes_the_files_of_entries_for_tries_it_does_not_hold() {
    let fixture = fixture(ReplicationConfig { rows_per_block: 2, assertion_interval: Duration::from_millis(10), ..Default::default() }, 1 << 27).await;
    let (handle, partition, store) = (fixture.handle.clone(), Arc::clone(&fixture.driver.partition), Arc::clone(&fixture.store));
    let writer = tokio::spawn(fixture.driver.run());
    let table = TableRef::new("public", "docs");
    let transaction = SourceTransaction::new(vec![TxOp::PutDocs { table: table.clone(), documents: Documents::new(vec![BTreeMap::from([("_id".into(), Value::I64(1)), ("v".into(), Value::Utf8("value".into()))])], &fixture.memory).unwrap(), valid_from: Some(Timestamp(0)), valid_to: None }], SubmitOptions::default(), &fixture.memory).unwrap();
    handle.submit_source_encoded(transaction.encode().unwrap().into_bytes(), Timestamp(100)).await.unwrap();
    let mut status = handle.watch_status();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop { if status.borrow_and_update().adopted_block.is_some() { break; } status.changed().await.unwrap(); }
    }).await.unwrap();
    let value = partition.value();
    let live = value.trie_catalog().current_tries(&table)[0].key.clone();
    let slug = value.table_catalog().entry(&table).unwrap().slug.clone();
    let record = |id: &str| proto::ArtefactRecord { kind: Some("search".into()), definition_id: Some(id.into()), definition_version: Some(1), stats: None, scope: None };
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
    let recovered = blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(0)).await.unwrap();
    let progress = ProgressMonitor::new(recovered.progress.clone());
    let logs = PartitionLogs::new(source, replica, StorageEpoch(0), StorageEpoch(0), CURRENT_STORAGE_VERSION, StorageEpoch(0),
        store, Arc::clone(&cache), Arc::clone(&memory)).unwrap();
    let resolve = Arc::new(ResolveContext { database: "xtdb".into(), storage: cache, memory, query_options: Default::default(),
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
                (0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None })),
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
    for (seq, message) in [(0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None })), (1, boundary(0, -1))] {
        let envelope = ReplicaEnvelope { term: Term(1), term_sequence: sequenced.then_some(TermSequence(seq)), message };
        fixture.logs.enqueue_replica(&envelope, BlockIndex(0), Timestamp(0)).await.unwrap().durable().await.unwrap();
        fixture.driver.consume(raw_with_envelope(seq, &envelope)).await.unwrap();
    }
    let pending = fixture.driver.pending_block.as_ref().unwrap();
    fixture.blocks.produce(&pending.prepared, &pending.boundary, BTreeMap::new()).await.unwrap();
    fixture
}

fn raw_with_envelope(position: i64, envelope: &ReplicaEnvelope) -> LogRecord {
    LogRecord { metadata: LogMetadata { offset: LogOffset(position), timestamp: Timestamp(0) }, payload: envelope.encode().into() }
}

async fn reopen_memory_checkpoint(fixture: &Fixture) -> (ReplicationHandle, ReplicationDriver) {
    reopen_memory_checkpoint_with_config(fixture, ReplicationConfig::default()).await
}

async fn reopen_memory_checkpoint_with_config(fixture: &Fixture, config: ReplicationConfig) -> (ReplicationHandle, ReplicationDriver) {
    let recovered = fixture.blocks.recover(TrieConfig::default(), SourcePosition(-1), Timestamp(0)).await.unwrap();
    assert!(recovered.confirmation(StorageEpoch(1), CURRENT_STORAGE_VERSION, StorageEpoch(0), &fixture.memory).unwrap().is_none());
    let progress = ProgressMonitor::new(recovered.progress.clone());
    ReplicationDriver::new(config, Arc::clone(&fixture.logs), Arc::clone(&fixture.blocks), recovered,
        progress, Arc::clone(&fixture.driver.resolve), Arc::new(NoEffects), CancellationToken::new()).unwrap()
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
        assert!(Arc::ptr_eq(&restarted.partition.value(), &before));
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
    let claim = ReplicaBody::NoOp(proto::NoOp { src_msg_id: None });
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
        let envelope = ReplicaEnvelope { term: Term(1), term_sequence: Some(TermSequence(sequence)), message: ReplicaBody::NoOp(proto::NoOp { src_msg_id: None }) };
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
    deliver_checkpoint_record(fixture, &mut prior, 2, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None })).await.unwrap();
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
    deliver_checkpoint_record(&fixture, &mut writer, 3, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None })).await.unwrap();
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
    deliver_checkpoint_record(&fixture, &mut last, 3, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None })).await.unwrap();
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
    deliver_checkpoint_record(&fixture, &mut writer, 4, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None })).await.unwrap();
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
    for message in [ReplicaBody::NoOp(proto::NoOp { src_msg_id: None }), boundary(1, -1)] {
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
    let claim = ReplicaBody::NoOp(proto::NoOp { src_msg_id: None });
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
    fixture.driver.consume(raw(0, 1, 1, ReplicaBody::NoOp(proto::NoOp { src_msg_id: Some(0) }))).await.unwrap();
    assert_eq!(fixture.handle.progress().current().source_position, SourcePosition(-1));
    fixture.driver.consume(raw(1, 2, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: Some(1) }))).await.unwrap();
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
    fixture.driver.command(super::Command::Delete { term: Term(1), table: TableRef::new("public", "docs"), keys: vec![rtdb_format::keys::TrieKey::l0(BlockIndex(0)).unwrap()], completion }).await.unwrap();
    fixture.driver.consume(raw(0, 2, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None }))).await.unwrap();
    assert_eq!(result.await.unwrap().unwrap_err().code, "xtdb/leader-term-ended");
    assert!(fixture.handle.progress().fault().is_none());
}

#[tokio::test]
async fn compactor_reset_receipt_requires_block_adoption_and_fails_on_term_loss() {
    let mut fixture = fixture(ReplicationConfig { may_lead: false, ..Default::default() }, 1 << 27).await;
    fixture.logs.append_source(&rtdb_format::envelope::SourceMessage::Protobuf(rtdb_format::envelope::SourceBody::FlushBlock(Default::default())), Timestamp(1)).await.unwrap();
    fixture.driver.consume(raw(0, 1, 0, resolved(1, 0))).await.unwrap();
    fixture.driver.promote().await.unwrap();
    let value = fixture.driver.partition.value();
    let (completion, mut receipt) = tokio::sync::oneshot::channel();
    fixture.driver.command(super::Command::ResetCompactor {
        term: Term(1), expected: Arc::clone(value.trie_catalog()), expected_block: value.live().block_index(),
        replacements: BTreeMap::new(), reservations: vec![fixture.memory.reserve(8192).unwrap()].into(), completion,
    }).await.unwrap();
    assert!(matches!(receipt.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)));
    assert!(fixture.driver.leader.as_ref().unwrap().compactor_reset.is_some());
    fixture.driver.consume(raw(1, 2, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None }))).await.unwrap();
    assert_eq!(receipt.await.unwrap().unwrap_err().code, "xtdb/leader-term-ended");
    assert!(fixture.handle.progress().fault().is_none());
}

#[tokio::test]
async fn retained_leader_does_not_claim_again_while_confirmed_or_producing_an_inherited_block() {
    for pending_block in [false, true] {
        let mut fixture = fixture(ReplicationConfig::default(), 1 << 27).await;
        let body = if pending_block { boundary(0, 0) } else { ReplicaBody::NoOp(proto::NoOp { src_msg_id: None }) };
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
    fixture.logs.enqueue_replica(&ReplicaEnvelope { term: Term(2), term_sequence: Some(TermSequence(0)), message: ReplicaBody::NoOp(proto::NoOp { src_msg_id: Some(1) }) }, BlockIndex(0), Timestamp(1)).await.unwrap().durable().await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), handle.await_source(SourcePosition(1), &CancellationToken::new())).await.unwrap().unwrap();
    assert_eq!(handle.status().highest_term, Term(2));
    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(1), driver).await.unwrap().unwrap().unwrap();
}

#[tokio::test]
async fn claim_read_back_before_durability_promotes_only_after_its_matching_receipt() {
    let mut fixture = fixture(ReplicationConfig::default(), 1 << 27).await;
    fixture.driver.claiming = Some(super::Claim { term: Term(1), observed: None, future: Box::pin(futures::future::pending()) });
    fixture.driver.consume(raw(0, 1, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None }))).await.unwrap();
    assert!(fixture.driver.leader.is_none());
    fixture.driver.claim_finished(Ok(ReplicaMetadata { position: rtdb_types::ReplicaPosition(0), timestamp: Timestamp(1) })).await.unwrap();
    assert_eq!(fixture.driver.role(), Role::Leading(Term(1)));
    fixture.driver.demote().await.unwrap();
}

#[tokio::test]
async fn losing_same_term_claim_never_promotes_after_durability() {
    let mut fixture = fixture(ReplicationConfig::default(), 1 << 27).await;
    fixture.driver.claiming = Some(super::Claim { term: Term(1), observed: None, future: Box::pin(futures::future::pending()) });
    fixture.driver.consume(raw(0, 1, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None }))).await.unwrap();
    fixture.driver.consume(raw(1, 1, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None }))).await.unwrap();
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

async fn queued_attach_readback(fixture: &mut Fixture, operation: &DbOp) -> (TransactionKey, oneshot::Receiver<Result<Applied>>, LogRecord) {
    fixture.driver.fence.admit(Term(1), Some(TermSequence(0))).unwrap();
    fixture.driver.promote().await.unwrap();

    let snapshot = fixture.driver.partition.value().snapshot();
    let resolve = Arc::clone(&fixture.driver.resolve);
    let effects = Arc::clone(&fixture.driver.effects);
    let resolver = fixture.driver.leader.as_mut().unwrap().resolver_mut().unwrap();
    let verdict = super::check_db_op(effects.as_ref(), resolver, operation);
    let staged = resolver.resolve_db_op(snapshot, SourcePosition(1), Timestamp(100), operation.clone(), verdict, &resolve).unwrap();
    let key = staged.result().tx_key();
    let (completion, receipt) = oneshot::channel();
    fixture.driver.enqueue_resolved(staged, Some(completion)).await.unwrap();

    let mut tail = fixture.logs.replica_tail(fixture.driver.read_position).await.unwrap();
    let mut records = tail.poll(Duration::from_secs(5)).await.unwrap();
    assert_eq!(records.len(), 1);
    let readback = records.pop().unwrap();
    let envelope = ReplicaEnvelope::decode(&readback.payload).unwrap().unwrap();
    assert!(matches!(envelope.message, ReplicaBody::ResolvedTx(message)
        if message.tx_id == Some(key.tx_id.0) && message.committed == Some(true) && message.db_op.as_ref() == Some(operation)));
    (key, receipt, readback)
}

async fn attach_readback_fixture() -> (Fixture, DbOp, oneshot::Receiver<DbOp>, oneshot::Sender<Result<()>>) {
    let mut fixture = fixture(ReplicationConfig {
        may_lead: false, assertion_interval: Duration::from_secs(3600), ..Default::default()
    }, 1 << 27).await;
    let (entered, called) = oneshot::channel();
    let (finish, release) = oneshot::channel();
    fixture.driver.effects = Arc::new(HeldDatabaseEffect { called: Mutex::new(Some(entered)), release: Mutex::new(Some(release)) });
    let operation = DbOp::AttachDatabase(proto::AttachDatabase {
        db_name: Some("secondary".into()), config: Some(DatabaseConfig::default()),
    });
    (fixture, operation, called, finish)
}

#[tokio::test]
async fn own_attach_readback_mismatch_leaves_partition_catalog_and_progress_unchanged() {
    let (mut fixture, operation, mut called, _finish) = attach_readback_fixture().await;
    let (key, mut receipt, mut readback) = queued_attach_readback(&mut fixture, &operation).await;
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
async fn own_attach_readback_applies_before_the_effect_and_awaits_it_before_progress_and_receipt() {
    for effect in [Ok(()), Err(Anomaly::incorrect("test/attach-failed", "Applying the database attach failed"))] {
        let (mut fixture, operation, mut called, finish) = attach_readback_fixture().await;
        let (key, mut receipt, readback) = queued_attach_readback(&mut fixture, &operation).await;
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
        assert!(matches!(claim.message, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None })));

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
    let record = proto::ArtefactRecord { kind: Some("search".into()), definition_id: Some("body".into()), definition_version: Some(1), stats: None, scope: Some(proto::ArtefactScope::PartGroup as i32) };
    let value = rtdb_index::PartitionValue::loaded(catalog, tables, TrieConfig::default()).unwrap();
    let group = |key: &str| key.parse::<TrieKey>().unwrap().part_group().unwrap();
    let object = |key: &str| rtdb_storage::ObjectKey::new(slug.owner_artefact_key(&ArtefactOwner::Group(group(key)), "search", "body", 1).unwrap()).unwrap();
    let message = |keys: &[&str]| proto::ArtefactsAdded { storage_version: Some(CURRENT_STORAGE_VERSION.0), storage_epoch: Some(0), table_name: Some(table.schema_and_table()),
        entries: keys.iter().map(|key| proto::ArtefactEntry { trie_key: Some((*key).into()), record: Some(record.clone()) }).collect(), source_msg_id: None };
    let members = ["l02-rc-p0-b00", "l02-rc-p1-b00", "l02-rc-p2-b00", "l02-rc-p3-b00"];
    let mut partly_superseded = message(&members);
    assert!(accept_artefacts(&mut partly_superseded, &value.snapshot(), CURRENT_STORAGE_VERSION.0, 0).is_empty(), "the superseded member's siblings still take the file");
    assert_eq!(partly_superseded.entries.len(), 3);
    let mut unheld = message(&["l02-rc-p0-b05", "l02-rc-p1-b05"]);
    assert_eq!(accept_artefacts(&mut unheld, &value.snapshot(), CURRENT_STORAGE_VERSION.0, 0), [object("l02-rc-p0-b05")], "a group the catalog holds no member of loses its file once");
    let one: BTreeSet<TrieKey> = ["l02-rc-p0-b00".parse().unwrap()].into();
    let recorded = rtdb_index::PartitionValue::loaded(value.trie_catalog().with_artefacts(&table, members.iter().map(|key| (key.parse().unwrap(), record.clone()))), value.table_catalog().as_ref().clone(), TrieConfig::default()).unwrap();
    assert!(!cached_objects(&recorded, &table, &one).contains(&object("l02-rc-p0-b00")));
    let all: BTreeSet<TrieKey> = members.iter().map(|key| key.parse().unwrap()).collect();
    assert_eq!(cached_objects(&recorded, &table, &all).iter().filter(|key| **key == object("l02-rc-p0-b00")).count(), 1);
}

#[tokio::test]
async fn a_promotion_that_inherits_a_flushed_block_resumes_the_source_after_the_flush() {
    let config = ReplicationConfig { assertion_interval: Duration::from_millis(10), ..Default::default() };
    let fixture = fixture(config, 1 << 27).await;
    let first = submit_recovery_row(&fixture.handle, &fixture.memory, 1, 100).await;
    let flush = fixture.logs.append_source(&SourceMessage::Protobuf(SourceBody::FlushBlock(proto::FlushBlock { expected_block_idx: Some(-1) })), Timestamp(200)).await.unwrap();
    // The previous leader read back its transaction and cut the flush's boundary, then died before uploading the block.
    for record in [
        raw(0, 1, 0, ReplicaBody::NoOp(proto::NoOp { src_msg_id: None })),
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
