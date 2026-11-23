mod common;

use bytes::Bytes;
use common::{Ensemble, append};
use rtdb_logkeeper::{CopyFrom, Loglet, QuorumLog, StreamConfig, open_reader, open_reader_on, open_writer, open_writer_on};
use rtdb_storage::Error;
use rtdb_storage::log::{Log, LogOffset, LogRecord};
use rtdb_types::{AnomalyCategory, Timestamp};
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;

fn payloads(records: &[LogRecord]) -> Vec<String> {
    records.iter().map(|record| String::from_utf8(record.payload.to_vec()).unwrap()).collect()
}

async fn committed(log: &dyn Log) -> Vec<String> {
    payloads(&log.read_range(LogOffset(0), LogOffset(i64::MAX)).await.unwrap())
}

fn quorum_of(ensemble: &Ensemble, servers: &[usize], stream: &str) -> StreamConfig {
    StreamConfig { write_quorum: 2, ..ensemble.config_of(servers, stream) }
}

/// Commits a0..a4 on {A,B,C} at epoch 1, then leaves an unacknowledged "stranded" entry on A alone and stops A.
async fn history_with_a_minority_suffix(ensemble: &mut Ensemble, stream: &str) -> Arc<QuorumLog> {
    let a = open_writer(quorum_of(ensemble, &[0, 1, 2], stream), 1, CancellationToken::new()).await.unwrap();
    for position in 0..5 { append(a.as_ref(), &format!("a{position}"), position).await; }
    ensemble.stop(1).await;
    ensemble.stop(2).await;
    let stranded = a.enqueue(Bytes::from_static(b"stranded"), Timestamp(5)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    ensemble.stop(0).await;
    ensemble.restart(1).await;
    ensemble.restart(2).await;
    tokio::spawn(async move { let _ = stranded.durable().await; });
    a
}

fn expected(through: i64) -> Vec<String> {
    (0..5).map(|position| format!("a{position}")).chain((5..=through).map(|position| format!("b{position}"))).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_moved_to_a_new_ensemble_keeps_everything_acknowledged_and_fences_the_old_one() {
    let mut ensemble = Ensemble::start(5).await;
    let old = quorum_of(&ensemble, &[0, 1, 2], "moved");
    let new = quorum_of(&ensemble, &[2, 3, 4], "moved");
    let a = history_with_a_minority_suffix(&mut ensemble, "moved").await;

    let chain = vec![Loglet { servers: old.servers.clone(), start_lsn: 0, write_quorum: old.write_quorum }];
    let b = open_writer_on(chain, new.clone(), 2, CopyFrom::Retained, CancellationToken::new()).await.unwrap();
    assert_eq!(b.latest().unwrap(), LogOffset(4), "the minority suffix was never acknowledged and is dropped");
    assert_eq!(b.loglets(), [Loglet { servers: new.servers.clone(), start_lsn: 0, write_quorum: new.write_quorum }], "the new ensemble holds everything the old one retained");
    for position in 5..10 { assert_eq!(append(b.as_ref(), &format!("b{position}"), position).await.offset, LogOffset(position)); }
    b.confirm_leadership().await.unwrap();
    assert_eq!(committed(b.as_ref()).await, expected(9));

    let through_new = open_reader(new.clone(), CancellationToken::new()).await.unwrap();
    assert_eq!(committed(through_new.as_ref()).await, expected(9));
    let only_a_new_server = open_reader(ensemble.config_of(&[3], "moved"), CancellationToken::new()).await.unwrap();
    assert_eq!(committed(only_a_new_server.as_ref()).await, expected(9), "a server that joined with the move holds the copied history");

    ensemble.restart(0).await;
    let refused = open_writer(old.clone(), 1, CancellationToken::new()).await.err().unwrap();
    assert_eq!(refused.category, AnomalyCategory::Conflict);
    assert_eq!(a.confirm_leadership().await.unwrap_err().category, AnomalyCategory::Conflict, "the old writer learns it is deposed");
    assert!(matches!(a.enqueue(Bytes::from_static(b"late"), Timestamp(20)).await, Err(Error::Deposed { epoch: 1, promised: 2 })));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reader_handed_the_chain_reads_the_whole_history_across_a_move() {
    let mut ensemble = Ensemble::start(5).await;
    let old = quorum_of(&ensemble, &[0, 1, 2], "chained");
    let new = quorum_of(&ensemble, &[2, 3, 4], "chained");
    let _a = history_with_a_minority_suffix(&mut ensemble, "chained").await;

    let chain = vec![Loglet { servers: old.servers.clone(), start_lsn: 0, write_quorum: old.write_quorum }];
    let b = open_writer_on(chain.clone(), new.clone(), 2, CopyFrom::Lsn(3), CancellationToken::new()).await.unwrap();
    let loglets = b.loglets();
    assert_eq!(loglets, [chain[0].clone(), Loglet { servers: new.servers.clone(), start_lsn: 3, write_quorum: new.write_quorum }]);
    for position in 5..8 { assert_eq!(append(b.as_ref(), &format!("b{position}"), position).await.offset, LogOffset(position)); }
    assert_eq!(committed(b.as_ref()).await, expected(7), "the writer reads below its loglet from the earlier one");

    let joined = open_reader(ensemble.config_of(&[3], "chained"), CancellationToken::new()).await.unwrap();
    assert!(matches!(joined.read_range(LogOffset(0), LogOffset(8)).await, Err(Error::UnavailableLogRange { requested: 0, first_retained: 3 })), "a joining server holds only the new loglet");
    assert_eq!(payloads(&joined.read_range(LogOffset(3), LogOffset(8)).await.unwrap()), expected(7)[3..]);

    let reader = open_reader_on(loglets.clone(), new.clone(), CancellationToken::new()).await.unwrap();
    assert_eq!(committed(reader.as_ref()).await, expected(7));
    let mut tail = reader.tail(LogOffset(-1)).await.unwrap();
    let mut seen = Vec::new();
    while seen.len() < 8 { seen.extend(tail.poll(Duration::from_secs(1)).await.unwrap()); }
    assert_eq!(payloads(&seen), expected(7), "a tail crosses from one loglet into the next");

    ensemble.stop(2).await;
    let without_the_shared_server = open_reader_on(loglets, new, CancellationToken::new()).await.unwrap();
    assert_eq!(committed(without_the_shared_server.as_ref()).await, expected(7), "the earlier loglet's lsns are read from its remaining server");
}
