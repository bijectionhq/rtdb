use super::*;
use crate::readback_identity::fingerprint;
use std::sync::atomic::AtomicBool;
use tokio::sync::Notify;

#[derive(Default)]
struct ProviderGate {
    revoked: AtomicBool,
    submitted: Notify,
    order: Notify,
    return_receipt: Notify,
}

struct TraceLog {
    log: Arc<MemoryLog>,
    gate: Arc<ProviderGate>,
}

#[async_trait]
impl Log for TraceLog {
    fn fences_writer_terms(&self) -> bool { true }

    async fn holds_writer_grant(&self) -> rtdb_storage::Result<bool> {
        Ok(!self.gate.revoked.load(Ordering::Acquire))
    }

    async fn enqueue(&self, payload: Bytes, timestamp: Timestamp) -> rtdb_storage::Result<rtdb_storage::log::AppendReceipt> {
        self.gate.submitted.notify_one();
        self.gate.order.notified().await;
        if self.gate.revoked.load(Ordering::Acquire) {
            return Err(rtdb_storage::Error::Closed("writer replaced".into()));
        }
        let receipt = self.log.enqueue(payload, timestamp).await?;
        self.gate.return_receipt.notified().await;
        Ok(receipt)
    }

    fn latest(&self) -> rtdb_storage::Result<LogOffset> { self.log.latest() }

    async fn tail(&self, after: LogOffset) -> rtdb_storage::Result<Box<dyn rtdb_storage::log::LogTail>> {
        self.log.tail(after).await
    }

    async fn range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Box<dyn rtdb_storage::log::LogRange>> {
        self.log.range(from, to).await
    }

    async fn read_range(&self, from: LogOffset, to: LogOffset) -> rtdb_storage::Result<Vec<LogRecord>> {
        self.log.read_range(from, to).await
    }
}

#[derive(Clone, Copy)]
enum Scenario { Replacement, Mismatch, Success, ApplyFault, Replay }

fn hex(digest: [u8; 32]) -> String {
    use std::fmt::Write;
    digest.iter().fold(String::with_capacity(64), |mut encoded, byte| {
        write!(encoded, "{byte:02x}").unwrap();
        encoded
    })
}

fn record_read(action: &str, record: &LogRecord) -> String {
    let frame = ReplicaEnvelope::decode(&record.payload).unwrap().unwrap();
    let resolved = matches!(frame.message, ReplicaBody::ResolvedTx(_));
    format!("{action} {} {} {} {}", frame.term.0, frame.term_sequence.unwrap().0,
        hex(fingerprint(&record.payload)), u8::from(resolved))
}

async fn capture(scenario: Scenario) -> Vec<String> {
    let gate = Arc::new(ProviderGate::default());
    let config = ReplicationConfig { idle_heartbeat: false, ..leading_config() };
    let mut fixture = fixture_with_replica(config, 1 << 27, None,
        |log| Arc::new(TraceLog { log, gate: Arc::clone(&gate) })).await;
    fixture.driver.update_installed_prefix().unwrap();
    let (operation, mut called, finish) = hold_catalog_effect(&mut fixture);
    let mut tail = fixture.logs.replica_tail(fixture.driver.read_position).await.unwrap();
    let empty = tail.poll(Duration::ZERO).await.unwrap();
    assert!(empty.is_empty());
    fixture.driver.lead_after_empty_read(empty.len()).await.unwrap();
    let Role::Leading(term) = fixture.handle.status().role else { panic!("expected fenced promotion"); };
    let mut trace = vec!["empty".into(), "grant 0".into(), format!("promote {}", term.0)];

    let snapshot = fixture.driver.partition.value().snapshot();
    let resolve = Arc::clone(&fixture.driver.resolve);
    let resolver = fixture.driver.leader.as_mut().unwrap().resolver_mut().unwrap();
    let staged = resolver.resolve_db_op(snapshot, SourcePosition(1), Timestamp(100),
        operation.clone(), Ok(()), &resolve).await.unwrap();
    let key = staged.result().tx_key();
    let (completion, mut receipt) = oneshot::channel();
    fixture.driver.enqueue_resolved(staged, Some(completion)).await.unwrap();
    trace.push(format!("enqueue 0 {}", key.tx_id.0));

    gate.submitted.notified().await;
    let pending = fixture.driver.leader.as_ref().unwrap().pending.front().unwrap();
    let stamp = pending.append_stamp().unwrap();
    trace.push(format!("dispatch 0 {} {} {}", term.0, stamp.sequence.0, hex(stamp.fingerprint.unwrap())));
    assert!(matches!(receipt.try_recv(), Err(Empty)));

    if matches!(scenario, Scenario::Replacement) {
        gate.revoked.store(true, Ordering::Release);
        trace.push("replace 1".into());
        gate.order.notify_one();
        assert!(receipt.await.unwrap().unwrap_err().to_string().contains("writer replaced"));
        assert_eq!(fixture.replica.latest().unwrap(), LogOffset(-1));
        trace.push("providerFault 0".into());
        assert!(fixture.driver.demote().await.is_err());
        assert_eq!(fixture.driver.role(), Role::Following);
        trace.push("termLoss".into());
        return trace;
    }

    gate.order.notify_one();
    let mut records = tail.poll(Duration::from_secs(5)).await.unwrap();
    assert_eq!(records.len(), 1);
    let mut record = records.pop().unwrap();
    let chosen_offset = record.metadata.offset.0;
    trace.push(format!("choose 0 {chosen_offset}"));
    if matches!(scenario, Scenario::Replay) {
        fixture.driver.demote().await.unwrap();
        assert_eq!(receipt.try_recv().unwrap().unwrap_err(), crate::progress::term_ended());
        assert!(fixture.handle.progress().current().latest_result.is_none());
        let (reopened, mut follower) = reopen_memory_checkpoint_with_config(
            &fixture, ReplicationConfig { may_lead: false, ..ReplicationConfig::default() },
        ).await;
        trace.push("crash".into());
        trace.push(record_read("replay", &record));
        follower.consume(record).await.unwrap();
        let applied = reopened.progress().current();
        let tx = applied.latest_result.as_ref().unwrap().result();
        assert_eq!(tx.tx_key(), key);
        assert_eq!(follower.read_position.after_offset(fixture.logs.replica_epoch), chosen_offset);
        trace.extend(["applyBegin".into(), format!("applyFinished {}", tx.tx_key().tx_id.0), "advance".into()]);
        return trace;
    }
    if matches!(scenario, Scenario::Mismatch) {
        let before = fixture.driver.partition.value();
        let mut frame = ReplicaEnvelope::decode(&record.payload).unwrap().unwrap();
        let ReplicaBody::ResolvedTx(message) = &mut frame.message else { panic!("expected resolved transaction"); };
        message.tx_id = Some(key.tx_id.0 + 1);
        record.payload = frame.encode().into();
        trace.push(record_read("read", &record));
        let error = fixture.driver.consume(record).await.unwrap_err();
        assert_eq!(error.code, "xtdb.indexer/leader-read-back-mismatch");
        assert_eq!(receipt.try_recv().unwrap().unwrap_err(), error);
        assert!(matches!(called.try_recv(), Err(Empty)));
        assert!(Arc::ptr_eq(&before, &fixture.driver.partition.value()));
        assert!(fixture.handle.progress().current().latest_result.is_none());
    } else {
        trace.push(record_read("read", &record));
        let progress = Arc::clone(fixture.handle.progress());
        let partition = Arc::clone(&fixture.driver.partition);
        let consumed = fixture.driver.consume(record);
        tokio::pin!(consumed);
        assert!(futures::poll!(&mut consumed).is_pending());
        assert_eq!(called.try_recv().unwrap(), operation);
        assert_eq!(partition.value().live().latest_completed_tx(), Some(key));
        assert!(progress.current().latest_result.is_none());
        assert!(matches!(receipt.try_recv(), Err(Empty)));
        trace.push("applyBegin".into());
        if matches!(scenario, Scenario::ApplyFault) {
            let error = Anomaly::incorrect("test/attach-failed", "Applying the database attach failed");
            finish.send(Err(error.clone())).unwrap();
            assert_eq!(consumed.await.unwrap_err(), error);
            assert_eq!(receipt.try_recv().unwrap().unwrap_err(), error);
            assert!(progress.current().latest_result.is_none());
            trace.push("applyFault".into());
        } else {
            finish.send(Ok(())).unwrap();
            consumed.await.unwrap();
            let published = progress.current();
            let Applied::Transaction(applied) = receipt.try_recv().unwrap().unwrap() else {
                panic!("expected applied transaction");
            };
            let published = published.latest_result.as_ref().unwrap().result();
            assert!(std::ptr::eq(applied.as_ref(), published));
            trace.push(format!("applyFinished {}", published.tx_key().tx_id.0));
            trace.push(format!("ack 0 {}", applied.tx_key().tx_id.0));
            assert!(matches!(receipt.try_recv(), Err(oneshot::error::TryRecvError::Closed)));

        }
    }
    if matches!(scenario, Scenario::Success) {
        assert_eq!(fixture.driver.read_position.after_offset(fixture.logs.replica_epoch), chosen_offset);
        trace.push("advance".into());
    }
    fixture.driver.demote().await.unwrap();
    trace.push("termLoss".into());
    trace
}

#[tokio::test]
async fn driver_trace_refinement() {
    for (name, scenario) in [
        ("writer-replacement", Scenario::Replacement),
        ("readback-mismatch", Scenario::Mismatch),
        ("apply-before-provider-return", Scenario::Success),
        ("apply-fault", Scenario::ApplyFault),
        ("replay-after-restart", Scenario::Replay),
    ] {
        let trace = tokio::time::timeout(Duration::from_secs(10), capture(scenario)).await.unwrap();
        if let Some(dir) = std::env::var_os("RTDB_DRIVER_TRACE_DIR") {
            let file = std::path::PathBuf::from(dir).join(format!("{name}.trace"));
            std::fs::write(file, trace.join("\n") + "\n").unwrap();
        }
    }
}
