//! Measures end-to-end durable writes through a local cell under Zipf activity.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{Cell, values};
use rtdb_multi::admin;
use tokio::sync::{Barrier, Mutex};
use tokio::task::JoinSet;

fn setting(name: &str, default: usize) -> usize {
    match std::env::var(name) {
        Ok(value) => value.parse::<usize>().ok().filter(|value| *value > 0).unwrap_or_else(|| panic!("{name} must be a positive integer")),
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => panic!("cannot read {name}: {error}"),
    }
}

fn ranks(deployments: usize) -> Vec<f64> {
    let mut total = 0.0;
    (1..=deployments).map(|rank| {
        total += 1.0 / rank as f64;
        total
    }).collect()
}

fn pick_rank(state: &mut u64, ranks: &[f64]) -> usize {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    let fraction = (*state >> 11) as f64 / (1u64 << 53) as f64;
    let draw = fraction * ranks.last().unwrap();
    ranks.partition_point(|boundary| *boundary <= draw).min(ranks.len() - 1)
}

fn percentile(samples: &[Duration], percentile: usize) -> f64 {
    let index = (samples.len() * percentile).div_ceil(100) - 1;
    samples[index].as_secs_f64() * 1_000.0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn durable_pgwire_updates_under_zipf_activity() {
    let deployments = setting("RTDB_CELL_BENCH_DEPLOYMENTS", 32);
    let workers = setting("RTDB_CELL_BENCH_WORKERS", 16);
    let commands = setting("RTDB_CELL_BENCH_COMMANDS", 256);
    let cell = Cell::start_durable(3).await;

    let mut connections = Vec::with_capacity(deployments);
    for index in 0..deployments {
        let name = format!("bench-{index}");
        admin::create_deployment(cell.directory.as_ref(), &name, "bench", 3).await.unwrap();
        let client = cell.connect(&name).await;
        client.simple_query("INSERT INTO counters (_id, n) VALUES (1, 0)").await.unwrap();
        connections.push(Mutex::new(client));
    }
    let connections = Arc::new(connections);
    let ranks = Arc::new(ranks(deployments));
    let ready = Arc::new(Barrier::new(workers + 1));
    let mut tasks = JoinSet::new();
    for worker in 0..workers {
        let (connections, ranks, ready) = (connections.clone(), ranks.clone(), ready.clone());
        tasks.spawn(async move {
            let mut rng = (worker as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15);
            let mut totals = vec![0; ranks.len()];
            let mut samples = Vec::with_capacity(commands);
            ready.wait().await;
            for _ in 0..commands {
                let rank = pick_rank(&mut rng, &ranks);
                let started = Instant::now();
                connections[rank].lock().await.simple_query("UPDATE counters SET n = n + 1 WHERE _id = 1").await.unwrap();
                samples.push(started.elapsed());
                totals[rank] += 1;
            }
            (totals, samples)
        });
    }

    let started = Instant::now();
    ready.wait().await;
    let mut totals = vec![0; deployments];
    let mut latencies = Vec::with_capacity(workers * commands);
    while let Some(result) = tasks.join_next().await {
        let (updates, samples) = result.unwrap();
        for (total, updated) in totals.iter_mut().zip(updates) { *total += updated; }
        latencies.extend(samples);
    }
    let elapsed = started.elapsed();

    for (client, expected) in connections.iter().zip(&totals) {
        assert_eq!(values(&*client.lock().await, "SELECT n FROM counters").await, [expected.to_string()]);
    }
    assert_eq!(latencies.len(), workers * commands);
    latencies.sort_unstable();
    println!(
        "deployments={deployments} workers={workers} commits={} hottest_deployment_commits={} duration_s={:.3} commits_per_s={:.0} p50_ms={:.3} p95_ms={:.3} p99_ms={:.3}",
        latencies.len(), totals[0], elapsed.as_secs_f64(), latencies.len() as f64 / elapsed.as_secs_f64(),
        percentile(&latencies, 50), percentile(&latencies, 95), percentile(&latencies, 99),
    );
    drop(connections);
    cell.stop().await;
}
