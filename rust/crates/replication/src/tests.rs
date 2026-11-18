use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rtdb_columnar::MemoryBudget;
use rtdb_format::{database::proto::DatabaseConfig, envelope::{ReplicaBody, ReplicaEnvelope}, keys::CURRENT_STORAGE_VERSION, log::proto::{self, resolved_tx::DbOp}};
use rtdb_index::TransactionBuffer;
use rtdb_storage::{MemoryStore, ObjectStore, cache::BufferPool, log::{Log, LogMetadata, LogOffset, LogRecord, MemoryLog}};
use rtdb_trie::TrieConfig;
use rtdb_tx::{Documents, ResolveContext, SourceTransaction, SubmitOptions, TransactionAuthorization, TxOp};
use rtdb_types::{Anomaly, BlockIndex, Interval, Result, SourcePosition, StorageEpoch, TableRef, Term, TermSequence, Timestamp, TransactionKey, TxId, Value};
use tokio_util::sync::CancellationToken;

use crate::*;

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
    driver: ReplicationDriver, handle: ReplicationHandle, logs: Arc<PartitionLogs>, store: Arc<MemoryStore>, blocks: Arc<BlockStore>, memory: Arc<MemoryBudget>, replica: Arc<MemoryLog>,
}
async fn fixture(config: ReplicationConfig, replica_message_limit: usize) -> Fixture {
    let memory = MemoryBudget::new(1 << 30);
    let source = Arc::new(MemoryLog::new(1 << 28));
    let replica = Arc::new(MemoryLog::new(replica_message_limit));
    let store = Arc::new(MemoryStore::default());
    let cache = Arc::new(BufferPool::new(store.clone(), 1 << 27));
    let logs = PartitionLogs::new(source, replica.clone(), StorageEpoch(0), StorageEpoch(0), CURRENT_STORAGE_VERSION, StorageEpoch(0), store.clone(), Arc::clone(&cache), Arc::clone(&memory)).unwrap();
    let blocks = BlockStore::new(store.clone(), Arc::clone(&cache), Arc::clone(&memory), 100_000_000).unwrap();
    let recovered = blocks.recover(TrieConfig::new(2, 2).unwrap(), SourcePosition(-1), Timestamp(0)).await.unwrap();
    let progress = ProgressMonitor::new(recovered.progress.clone());
    let resolve = Arc::new(ResolveContext { database: "xtdb".into(), storage: cache, memory: Arc::clone(&memory), query_options: Default::default(), cancellation: CancellationToken::new(), default_tz: "UTC".into(), authorization: Arc::new(AllowRoles), execution_observer: None, tracer: None });
    let (handle, driver) = ReplicationDriver::new(config, Arc::clone(&logs), Arc::clone(&blocks), recovered, progress, resolve, Arc::new(NoEffects), CancellationToken::new()).unwrap();
    Fixture { driver, handle, logs, store, blocks, memory, replica }
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
    for id in 0..8 { write.put(table.clone(), BTreeMap::from([("_id".into(), Value::I64(id)), ("v".into(), Value::I64(id))]), Interval::ALL).unwrap(); }
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
    let transaction = SourceTransaction::new(vec![TxOp::PutDocs { table: TableRef::new("public", "docs"), documents: Documents::new(vec![BTreeMap::from([("_id".into(), Value::I64(1)), ("v".into(), Value::Utf8("value".into()))])], &fixture.memory).unwrap(), valid_from: Some(Timestamp(0)), valid_to: None }], SubmitOptions::default(), &fixture.memory).unwrap();
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

struct CommitExternal;
#[async_trait]
impl rtdb_tx::TransactionWriter for CommitExternal {
    async fn write(&mut self, _: &mut rtdb_tx::OpenTransaction<'_>) -> Result<rtdb_tx::WriterResult> {
        Ok(rtdb_tx::WriterResult::Committed { user_metadata: None })
    }
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
    use tokio::sync::oneshot::error::TryRecvError::Empty;
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
