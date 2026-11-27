use std::sync::Arc;
use std::time::Duration;

use rtdb_replication::{AppliedProgress, BlockCycle, FlushTimer, ProgressMonitor};
use rtdb_tx::TransactionResult;
use rtdb_types::{Anomaly, SourcePosition, Timestamp, TransactionKey, TxId};
use tokio_util::sync::CancellationToken;

fn monitor() -> Arc<ProgressMonitor> {
    ProgressMonitor::new(AppliedProgress { source_position: SourcePosition(-1), tx_key: None, latest_result: None, external_token: None })
}
fn committed(id: i64, time: i64) -> Arc<TransactionResult> { Arc::new(TransactionResult::Committed { tx_key: TransactionKey { tx_id: TxId(id), system_time: Timestamp(time) } }) }

#[tokio::test]
async fn applied_prefix_remains_readable_after_ingestion_failure_and_restart_releases_new_waits() {
    let monitor = monitor();
    let cancellation = CancellationToken::new();
    monitor.applied(Some(SourcePosition(9)), Some(committed(7, 70).into()), Some(vec![1, 2])).unwrap();
    let fault = Anomaly::fault("test/ingestion", "broken ingestion");
    monitor.ingestion_failed(fault.clone());
    monitor.await_source(SourcePosition(9), &cancellation).await.unwrap();
    assert_eq!(monitor.await_tx(TxId(7), &cancellation).await.unwrap().unwrap().tx_key().system_time, Timestamp(70));
    assert_eq!(monitor.await_tx(TxId(8), &cancellation).await.unwrap_err(), fault);
    monitor.driver_restarted();
    monitor.applied(Some(SourcePosition(10)), Some(committed(8, 80).into()), None).unwrap();
    assert_eq!(monitor.await_system_time(Timestamp(80), &cancellation).await.unwrap().external_token.as_deref(), Some(&[1, 2][..]));
}

#[tokio::test]
async fn invalid_progress_does_not_partially_publish_source_or_token() {
    let monitor = monitor();
    monitor.applied(Some(SourcePosition(9)), Some(committed(7, 70).into()), Some(vec![1])).unwrap();
    assert!(monitor.applied(Some(SourcePosition(10)), Some(committed(7, 80).into()), Some(vec![2])).is_err());
    let progress = monitor.current();
    assert_eq!(progress.source_position, SourcePosition(9));
    assert_eq!(progress.tx_key.unwrap().system_time, Timestamp(70));
    assert_eq!(progress.external_token.as_deref(), Some(&[1][..]));
    monitor.stopped();
    monitor.await_tx(TxId(7), &CancellationToken::new()).await.unwrap();
    assert!(monitor.await_source(SourcePosition(10), &CancellationToken::new()).await.is_err());
}

#[tokio::test]
async fn cancelling_a_wait_leaves_other_subscriptions_and_applied_progress_intact() {
    let monitor = monitor();
    let cancelled = CancellationToken::new();
    let surviving = CancellationToken::new();
    let source = monitor.await_source(SourcePosition(4), &cancelled);
    let transaction = monitor.await_system_time(Timestamp(70), &surviving);
    tokio::pin!(source, transaction);
    assert!(futures::poll!(&mut source).is_pending());
    assert!(futures::poll!(&mut transaction).is_pending());

    cancelled.cancel();
    let error = tokio::time::timeout(Duration::from_secs(5), &mut source).await.unwrap().unwrap_err();
    assert_eq!(error.code, "xtdb/ingestion-interrupted");
    assert!(monitor.fault().is_none());
    assert_eq!(monitor.current().source_position, SourcePosition(-1));
    assert!(futures::poll!(&mut transaction).is_pending());

    let expected = committed(7, 70);
    monitor.applied(Some(SourcePosition(4)), Some(Arc::clone(&expected).into()), Some(vec![1])).unwrap();
    let applied = tokio::time::timeout(Duration::from_secs(5), &mut transaction).await.unwrap().unwrap();
    assert_eq!(applied.source_position, SourcePosition(4));
    assert_eq!(applied.tx_key, Some(expected.tx_key()));
    assert_eq!(applied.latest_result, Some(expected.into()));
    assert_eq!(applied.external_token.as_deref(), Some(&[1][..]));
    let older = tokio::time::timeout(Duration::from_secs(5), monitor.await_tx(TxId(6), &surviving)).await.unwrap().unwrap();
    assert!(older.is_none());
}

#[tokio::test]
async fn a_synchronous_wait_can_read_a_recent_commit_after_the_applied_head_advances() {
    let monitor = ProgressMonitor::new(AppliedProgress {
        source_position: SourcePosition(-1), tx_key: None, latest_result: None, external_token: None,
    });
    for id in 0_i64..65 {
        let result = committed(id, id + 100);
        monitor.applied(Some(SourcePosition(id)), Some(result.into()), None).unwrap();
    }
    let cancellation = CancellationToken::new();
    assert!(monitor.await_tx(TxId(63), &cancellation).await.unwrap().is_none());
    let retained = monitor.await_tx_with_recent_commits(TxId(63), &cancellation).await.unwrap();
    assert_eq!(retained, Some(committed(63, 163)));
    let evicted = monitor.await_tx_with_recent_commits(TxId(0), &cancellation).await.unwrap();
    assert!(evicted.is_none());
}

#[test]
fn resolved_rows_seed_from_applied_rows_and_resolution_stops_until_adoption() {
    let mut cycle = BlockCycle::Filling { rows: 9 };
    assert!(!cycle.is_full(10));
    cycle.resolved(1).unwrap();
    assert!(cycle.is_full(10));
    cycle.cut().unwrap();
    assert!(!cycle.accepting());
    assert!(cycle.resolved(1).is_err());
    cycle.adopted();
    assert!(cycle.accepting());
    assert!(!cycle.is_full(0));
}

#[test]
fn live_bytes_cut_a_nonempty_block_at_low_row_count_and_leave_working_headroom() {
    let mut cycle = BlockCycle::Filling { rows: 0 };
    assert!(!cycle.should_cut_for_memory(200, 0, 300));
    cycle.resolved(1).unwrap();
    assert!(!cycle.is_full(10));
    assert!(!cycle.should_cut_for_memory(99, 200, 300));
    assert!(cycle.should_cut_for_memory(100, 200, 300));
    assert!(cycle.should_cut_for_memory(13, 100, 300));
    assert!(!cycle.should_cut_for_memory(11, 100, 300));
    cycle.cut().unwrap();
    assert!(!cycle.should_cut_for_memory(200, 0, 300));
    cycle.adopted();
    assert!(!cycle.should_cut_for_memory(200, 0, 300));
}

#[test]
fn flush_timeout_rearms_when_persisted_transaction_changes_and_cuts_idle_empty_blocks() {
    let mut timer = FlushTimer::new(Timestamp(0), None, Duration::from_micros(10)).unwrap();
    assert!(!timer.should_flush(Timestamp(10), None));
    assert!(timer.should_flush(Timestamp(11), None));
    assert!(!timer.should_flush(Timestamp(22), Some(TxId(1))));
    assert!(!timer.should_flush(Timestamp(32), Some(TxId(1))));
    assert!(timer.should_flush(Timestamp(33), Some(TxId(1))));
}
