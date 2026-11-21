mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use common::Ensemble;
use rtdb_logkeeper::open_writer;
use rtdb_storage::log::{Log, LogOffset};
use rtdb_types::Timestamp;
use tokio::sync::Barrier;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

fn setting(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(text) => text.parse::<usize>().ok().filter(|value| *value > 0).unwrap_or_else(|| panic!("{name} must be a positive integer")),
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => panic!("cannot read {name}: {error}"),
    }
}

fn percentile(samples: &[Duration], percentile: usize) -> f64 {
    let index = (samples.len() * percentile).div_ceil(100) - 1;
    samples[index].as_secs_f64() * 1000.0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore]
async fn durable_quorum_append_throughput() {
    let streams = setting("RTDB_LOG_BENCH_STREAMS", 64);
    let appends = setting("RTDB_LOG_BENCH_APPENDS", 250);
    let payload_size = setting("RTDB_LOG_BENCH_BYTES", 512);
    let payload = Bytes::from(vec![42; payload_size]);
    let mut ensemble = Ensemble::start(3).await;
    let cancellation = CancellationToken::new();

    let writers = futures::future::try_join_all((0..streams).map(|index| {
        let mut configuration = ensemble.config(&format!("bench-{index}"));
        configuration.maximum_message_bytes = payload_size;
        open_writer(configuration, 1, cancellation.clone())
    })).await.unwrap();

    const WARMUP: i64 = 8;
    for writer in &writers {
        for sequence in 0..WARMUP {
            writer.enqueue(payload.clone(), Timestamp(sequence)).await.unwrap().durable().await.unwrap();
        }
    }

    let ready = Arc::new(Barrier::new(streams + 1));
    let mut workers = JoinSet::new();
    for writer in writers {
        let ready = Arc::clone(&ready);
        let payload = payload.clone();
        workers.spawn(async move {
            let mut latencies = Vec::with_capacity(appends);
            ready.wait().await;
            for sequence in WARMUP..(WARMUP + appends as i64) {
                let started = Instant::now();
                let committed = writer.enqueue(payload.clone(), Timestamp(sequence)).await.unwrap().durable().await.unwrap();
                latencies.push(started.elapsed());
                assert_eq!(committed.offset, LogOffset(sequence));
            }
            latencies
        });
    }

    let started = Instant::now();
    ready.wait().await;
    let mut latencies = Vec::with_capacity(streams * appends);
    while let Some(result) = workers.join_next().await { latencies.extend(result.unwrap()); }
    let elapsed = started.elapsed();
    assert_eq!(latencies.len(), streams * appends);
    latencies.sort_unstable();
    println!(
        "streams={streams} commits={} bytes={payload_size} duration_s={:.3} commits_per_s={:.0} p50_ms={:.3} p95_ms={:.3} p99_ms={:.3}",
        latencies.len(), elapsed.as_secs_f64(), latencies.len() as f64 / elapsed.as_secs_f64(),
        percentile(&latencies, 50), percentile(&latencies, 95), percentile(&latencies, 99),
    );

    cancellation.cancel();
    for server in 0..3 { ensemble.stop(server).await; }
}
