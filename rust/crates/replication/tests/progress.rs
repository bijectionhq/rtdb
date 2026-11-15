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
    monitor.applied(Some(SourcePosition(9)), Some(committed(7, 70)), Some(vec![1, 2])).unwrap();
    let fault = Anomaly::fault("test/ingestion", "broken ingestion");
    monitor.ingestion_failed(fault.clone());
    monitor.await_source(SourcePosition(9), &cancellation).await.unwrap();
    assert_eq!(monitor.await_tx(TxId(7), &cancellation).await.unwrap().unwrap().tx_key().system_time, Timestamp(70));
    assert_eq!(monitor.await_tx(TxId(8), &cancellation).await.unwrap_err(), fault);
    monitor.driver_restarted();
    monitor.applied(Some(SourcePosition(10)), Some(committed(8, 80)), None).unwrap();
    assert_eq!(monitor.await_system_time(Timestamp(80), &cancellation).await.unwrap().external_token.as_deref(), Some(&[1, 2][..]));
}

#[tokio::test]
async fn invalid_progress_does_not_partially_publish_source_or_token() {
    let monitor = monitor();
    monitor.applied(Some(SourcePosition(9)), Some(committed(7, 70)), Some(vec![1])).unwrap();
    assert!(monitor.applied(Some(SourcePosition(10)), Some(committed(7, 80)), Some(vec![2])).is_err());
    let progress = monitor.current();
    assert_eq!(progress.source_position, SourcePosition(9));
    assert_eq!(progress.tx_key.unwrap().system_time, Timestamp(70));
    assert_eq!(progress.external_token.as_deref(), Some(&[1][..]));
    monitor.stopped();
    monitor.await_tx(TxId(7), &CancellationToken::new()).await.unwrap();
    assert!(monitor.await_source(SourcePosition(10), &CancellationToken::new()).await.is_err());
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
fn flush_timeout_rearms_when_persisted_transaction_changes_and_cuts_idle_empty_blocks() {
    let mut timer = FlushTimer::new(Timestamp(0), None, Duration::from_micros(10)).unwrap();
    assert!(!timer.should_flush(Timestamp(10), None));
    assert!(timer.should_flush(Timestamp(11), None));
    assert!(!timer.should_flush(Timestamp(22), Some(TxId(1))));
    assert!(!timer.should_flush(Timestamp(32), Some(TxId(1))));
    assert!(timer.should_flush(Timestamp(33), Some(TxId(1))));
}
