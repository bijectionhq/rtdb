//! Measures end-to-end durable writes through a local cell under Zipf activity.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use common::{Cell, values};
use rtdb_multi::admin;
use tokio::sync::{Barrier, Mutex, Notify, OnceCell};
use tokio_postgres::Client;
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

struct WriteSample {
    started: Instant,
    acquired: Instant,
    sent: Instant,
    finished: Instant,
    rank: usize,
}

struct BenchmarkDeployment {
    initialized: OnceCell<()>,
    connections: Vec<Mutex<Option<Client>>>,
}

fn sample_write_latencies<'a>(phase: &str, samples: impl Iterator<Item = &'a WriteSample>) {
    let (mut total, mut queue, mut wire) = (Vec::new(), Vec::new(), Vec::new());
    for sample in samples {
        total.push(sample.finished - sample.started);
        queue.push(sample.acquired - sample.started);
        wire.push(sample.finished - sample.sent);
    }
    if total.is_empty() { return; }
    total.sort_unstable();
    queue.sort_unstable();
    wire.sort_unstable();
    println!("phase={phase} commits={} p95_ms={:.3} p99_ms={:.3} queue_p99_ms={:.3} wire_p99_ms={:.3}",
        total.len(), percentile(&total, 95), percentile(&total, 99), percentile(&queue, 99), percentile(&wire, 99));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn durable_pgwire_updates_under_zipf_activity() {
    let seconds = setting("RTDB_CELL_BENCH_TIMEOUT_SECONDS", 180) as u64;
    tokio::time::timeout(Duration::from_secs(seconds), exercise_zipf_activity()).await.expect("Zipf workload timed out");
}

async fn exercise_zipf_activity() {
    let deployments = setting("RTDB_CELL_BENCH_DEPLOYMENTS", 32);
    let workers = setting("RTDB_CELL_BENCH_WORKERS", 16);
    let connections_per_deployment = setting("RTDB_CELL_BENCH_CONNECTIONS", 1).min(workers);
    let scale = std::env::var_os("RTDB_CELL_BENCH_SCALE").is_some();
    let cold = std::env::var_os("RTDB_CELL_BENCH_COLD").is_some();
    let parked = setting("RTDB_CELL_BENCH_PARKED", 0);
    let commands = setting("RTDB_CELL_BENCH_COMMANDS", if scale { 2048 } else { 256 });
    let mut cell = Cell::start_durable(if scale { 2 } else { 3 }).await;
    let gateway = cell.gateway;
    if parked > 0 {
        let logkeepers: Vec<_> = cell.directory.members().await.unwrap().into_iter()
            .filter(|member| member.role == rtdb_directory::MemberRole::Logkeeper).map(|member| member.id).collect();
        let created_at = rtdb_directory::now_ms();
        for index in 0..parked {
            cell.directory.create_deployment(rtdb_directory::DeploymentRecord::new(
                format!("parked-{index}"), "bench", logkeepers.clone(), created_at)).await.unwrap();
        }
    }

    let mut databases = Vec::with_capacity(deployments);
    for index in 0..deployments {
        let name = format!("bench-{index}");
        admin::create_deployment(cell.directory.as_ref(), &name, "bench", 3).await.unwrap();
        let initialized = OnceCell::new();
        let mut connections = Vec::with_capacity(connections_per_deployment);
        for _ in 0..connections_per_deployment {
            let client = if cold { None } else {
                let client = cell.connect(&name).await;
                initialized.get_or_init(|| async { client.simple_query("INSERT INTO counters (_id, n) VALUES (1, 0)").await.unwrap(); }).await;
                Some(client)
            };
            connections.push(Mutex::new(client));
        }
        databases.push(BenchmarkDeployment { initialized, connections });
    }
    let databases = Arc::new(databases);
    let ranks = Arc::new(ranks(deployments));
    let ready = Arc::new(Barrier::new(workers + 1));
    let first_write = Arc::new(Notify::new());
    let completed = Arc::new(AtomicUsize::new(0));
    let activations = Arc::new(AtomicUsize::new(0));
    let mut tasks = JoinSet::new();
    for worker in 0..workers {
        let (databases, ranks, ready) = (databases.clone(), ranks.clone(), ready.clone());
        let (first_write, completed) = (first_write.clone(), completed.clone());
        let activations = activations.clone();
        tasks.spawn(async move {
            let mut rng = (worker as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15);
            let mut totals = vec![0; ranks.len()];
            let mut samples = Vec::with_capacity(commands);
            ready.wait().await;
            for _ in 0..commands {
                let rank = pick_rank(&mut rng, &ranks);
                let started = Instant::now();
                let database = &databases[rank];
                let mut connection = database.connections[worker % connections_per_deployment].lock().await;
                let acquired = Instant::now();
                if connection.is_none() {
                    let client = common::connect(&gateway.to_string(), &format!("bench-{rank}")).await;
                    database.initialized.get_or_init(|| async {
                        client.simple_query("INSERT INTO counters (_id, n) VALUES (1, 0)").await.unwrap();
                        activations.fetch_add(1, Ordering::Relaxed);
                    }).await;
                    *connection = Some(client);
                }
                let sent = Instant::now();
                connection.as_ref().unwrap().simple_query("UPDATE counters SET n = n + 1 WHERE _id = 1").await.unwrap();
                samples.push(WriteSample { started, acquired, sent, finished: Instant::now(), rank });
                totals[rank] += 1;
                if scale && completed.fetch_add(1, Ordering::Relaxed) == 0 { first_write.notify_one(); }
            }
            (totals, samples)
        });
    }

    let started = Instant::now();
    ready.wait().await;
    println!("starting Zipf writes with {workers} workers and {connections_per_deployment} connection slots per deployment");
    let measurements = async {
        let mut totals = vec![0; deployments];
        let mut latencies = Vec::with_capacity(workers * commands);
        while let Some(result) = tasks.join_next().await {
            let (updates, samples) = result.unwrap();
            for (total, updated) in totals.iter_mut().zip(updates) { *total += updated; }
            latencies.extend(samples);
        }
        (totals, latencies, started.elapsed())
    };
    let scaling = async {
        if !scale { return None; }
        tokio::time::timeout(Duration::from_secs(10), async {
            first_write.notified().await;
            if cold {
                while completed.load(Ordering::Relaxed) < deployments || cell.holder("bench-0").await.is_none() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }
        }).await.unwrap();
        let before = completed.load(Ordering::Relaxed);
        let scale_out_start = Instant::now();
        let added = cell.add_node().await;
        let moved = admin::rebalance(cell.directory.as_ref(), deployments, &common::control()).await.unwrap();
        assert!(moved > 0 && !cell.node(added).agent.held().is_empty());
        let scale_out_end = Instant::now();
        let after_out = completed.load(Ordering::Relaxed);
        assert!(after_out > before, "writes complete during scale-out");

        let drained = cell.holder("bench-0").await.unwrap();
        let draining = cell.node(drained);
        let holdings = draining.agent.held().len();
        let scale_in_start = Instant::now();
        admin::drain(cell.directory.as_ref(), &draining.lease.id()).await.unwrap();
        tokio::time::timeout(Duration::from_secs(30), async {
            while !draining.agent.held().is_empty() { tokio::time::sleep(Duration::from_millis(10)).await; }
        }).await.unwrap();
        cell.nodes[drained].take().unwrap().shutdown().await.unwrap();
        let scale_in_end = Instant::now();
        let after_in = completed.load(Ordering::Relaxed);
        assert!(after_in > after_out && after_in < workers * commands, "writes continue through the start and end of scale-in");
        println!("scale_out_moved={moved} scale_out_ms={:.3} commits_during_scale_out={} scale_in_drained={holdings} scale_in_ms={:.3} commits_during_scale_in={}",
            (scale_out_end - scale_out_start).as_secs_f64() * 1000.0, after_out - before,
            (scale_in_end - scale_in_start).as_secs_f64() * 1000.0, after_in - after_out);
        Some([(scale_out_start, scale_out_end), (scale_in_start, scale_in_end)])
    };
    let ((totals, samples, elapsed), windows) = tokio::join!(measurements, scaling);

    let databases = Arc::into_inner(databases).expect("all benchmark workers have returned");
    for (rank, (database, expected)) in databases.into_iter().zip(&totals).enumerate() {
        if let Some(client) = database.connections.into_iter().find_map(Mutex::into_inner) {
            assert_eq!(values(&client, "SELECT n FROM counters").await, [expected.to_string()]);
        } else {
            assert_eq!(*expected, 0);
            assert_eq!(cell.holder(&format!("bench-{rank}")).await, None);
        }
    }
    if parked > 0 {
        for index in [0, parked / 2, parked - 1] {
            assert_eq!(cell.holder(&format!("parked-{index}")).await, None);
        }
    }
    if cold || parked > 0 { println!("lazy_activations={} unrelated_parked={parked}", activations.load(Ordering::Relaxed)); }
    if let Some(windows) = windows {
        let overlaps = |sample: &WriteSample, (first, last): (Instant, Instant)| sample.started <= last && sample.finished >= first;
        for (name, window) in [("scale_out", Some(windows[0])), ("scale_in", Some(windows[1])), ("outside_moves", None)] {
            sample_write_latencies(name, samples.iter().filter(|sample| match window {
                Some(window) => overlaps(sample, window),
                None => windows.iter().all(|window| !overlaps(sample, *window)),
            }));
        }
    }
    for (phase, first, last) in [("first_third", started, started + elapsed / 3),
        ("second_third", started + elapsed / 3, started + elapsed * 2 / 3),
        ("last_third", started + elapsed * 2 / 3, started + elapsed)]
    {
        sample_write_latencies(phase, samples.iter().filter(|sample| sample.started >= first && sample.started < last));
    }
    sample_write_latencies("hottest_deployment", samples.iter().filter(|sample| sample.rank == 0));
    sample_write_latencies("other_deployments", samples.iter().filter(|sample| sample.rank > 0));
    let mut latencies: Vec<_> = samples.into_iter().map(|sample| sample.finished - sample.started).collect();
    assert_eq!(latencies.len(), workers * commands);
    latencies.sort_unstable();
    println!(
        "deployments={deployments} workers={workers} connections_per_deployment={connections_per_deployment} commits={} hottest_deployment_commits={} duration_s={:.3} commits_per_s={:.0} p50_ms={:.3} p95_ms={:.3} p99_ms={:.3}",
        latencies.len(), totals[0], elapsed.as_secs_f64(), latencies.len() as f64 / elapsed.as_secs_f64(),
        percentile(&latencies, 50), percentile(&latencies, 95), percentile(&latencies, 99),
    );
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn a_thousand_deployments_keep_their_history_through_handoff_and_failover() {
    use futures::future::join_all;

    let deployments = setting("RTDB_CELL_HANDOFF_DEPLOYMENTS", 1000);
    let concurrency = setting("RTDB_CELL_HANDOFF_CONCURRENCY", 8);
    let mut cell = Cell::start_durable(3).await;
    let names: Vec<_> = (0..deployments).map(|index| format!("placement-{index:04}")).collect();
    let start = Instant::now();
    let mut targets = Vec::with_capacity(deployments);
    for batch in names.chunks(concurrency) {
        targets.extend(join_all(batch.iter().map(|name| {
            let cell = &cell;
            async move {
                admin::create_deployment(cell.directory.as_ref(), name, "bench", 3).await.unwrap();
                let client = cell.connect(name).await;
                client.simple_query("INSERT INTO counters (_id, n) VALUES (1, 1)").await.unwrap();
                let holder = cell.holder(name).await.expect("the deployment is held");
                (holder + 1) % 3
            }
        })).await);
    }
    let activation = start.elapsed();
    println!("activated {deployments} deployments in {:.3} s", activation.as_secs_f64());

    let mut handoffs = Vec::with_capacity(deployments);
    for (batch_names, batch) in names.chunks(concurrency).zip(targets.chunks(concurrency)) {
        handoffs.extend(join_all(batch_names.iter().zip(batch).map(|(name, target)| {
            let cell = &cell;
            async move {
                let client = cell.connect(name).await;
                let started = Instant::now();
                admin::move_deployment(cell.directory.as_ref(), name, &cell.node(*target).lease.id(), &common::control()).await.unwrap();
                let move_elapsed = started.elapsed();
                client.simple_query("UPDATE counters SET n = n + 1 WHERE _id = 1").await.unwrap();
                let write_elapsed = started.elapsed();
                assert_eq!(cell.holder(name).await, Some(*target));
                (move_elapsed, write_elapsed)
            }
        })).await);
    }
    let (mut moves, mut resumed_writes): (Vec<_>, Vec<_>) = handoffs.into_iter().unzip();
    moves.sort_unstable();
    resumed_writes.sort_unstable();
    let memory_before_failure: Vec<_> = cell.nodes.iter().flatten().map(|node| node.server.node().memory().used_bytes()).collect();
    println!("retained_execution_bytes_before_kill={memory_before_failure:?}");

    let placements: Vec<_> = cell.nodes.iter().flatten().map(|node| node.agent.held().len()).collect();
    let failed = placements.iter().enumerate().max_by_key(|(_, count)| *count).unwrap().0;
    let lost = cell.node(failed).lease.id();
    let lost_epochs: std::collections::BTreeMap<_, _> = cell.directory.deployments_held_by(&lost).await.unwrap()
        .into_iter().map(|record| (record.name, record.epoch)).collect();
    assert!(!lost_epochs.is_empty());
    let failure_started = Instant::now();
    cell.nodes[failed].take().unwrap().kill().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while cell.directory.member(&lost).await.unwrap().unwrap().is_live(rtdb_directory::now_ms()) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("the failed member's lease expires");

    for batch in names.chunks(concurrency) {
        join_all(batch.iter().map(|name| {
            let cell = &cell;
            let dead_epoch = lost_epochs.get(name).copied();
            let lost = &lost;
            async move {
                let client = cell.connect(name).await;
                client.simple_query("UPDATE counters SET n = n + 1 WHERE _id = 1").await.unwrap();
                assert_eq!(values(&client, "SELECT n FROM counters").await, ["3"], "{name}");
                if let Some(epoch) = dead_epoch {
                    let record = cell.directory.deployment_by_name(name).await.unwrap().unwrap();
                    assert!(record.epoch > epoch);
                    assert_ne!(record.holder.as_ref(), Some(lost));
                }
            }
        })).await;
    }

    println!(
        "deployments={deployments} concurrency={concurrency} failed_placements={} activation_s={:.3} move_p50_ms={:.3} move_p95_ms={:.3} move_p99_ms={:.3} handoff_and_write_p50_ms={:.3} handoff_and_write_p95_ms={:.3} handoff_and_write_p99_ms={:.3} all_verified_after_kill_s={:.3}",
        lost_epochs.len(), activation.as_secs_f64(), percentile(&moves, 50), percentile(&moves, 95), percentile(&moves, 99),
        percentile(&resumed_writes, 50), percentile(&resumed_writes, 95), percentile(&resumed_writes, 99), failure_started.elapsed().as_secs_f64(),
    );
    let peaks: Vec<_> = cell.nodes.iter().flatten().map(|node| node.server.node().memory().peak_bytes()).collect();
    println!("survivor_peak_execution_bytes={peaks:?}");
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore]
async fn writes_keep_progress_during_repeated_handoffs() {
    let moves = setting("RTDB_CELL_ACTIVE_HANDOFFS", 48);
    let cell = Cell::start_durable(3).await;
    admin::create_deployment(cell.directory.as_ref(), "active", "bench", 3).await.unwrap();
    let client = cell.connect("active").await;
    client.simple_query("INSERT INTO counters (_id, n) VALUES (1, 0)").await.unwrap();
    let first_write = Arc::new(tokio::sync::Notify::new());
    let stop = tokio_util::sync::CancellationToken::new();
    let writer = {
        let stop = stop.clone();
        let first_write = first_write.clone();
        tokio::spawn(async move {
            let mut commits = Vec::new();
            while !stop.is_cancelled() {
                client.simple_query("UPDATE counters SET n = n + 1 WHERE _id = 1").await.unwrap();
                commits.push(Instant::now());
                if commits.len() == 1 { first_write.notify_one(); }
            }
            commits
        })
    };
    tokio::time::timeout(Duration::from_secs(10), first_write.notified()).await.unwrap();
    let holder = cell.holder("active").await.unwrap();
    let mut windows = Vec::with_capacity(moves);
    for step in 1..=moves {
        let target = cell.node((holder + step) % 3).lease.id();
        let start = Instant::now();
        admin::move_deployment(cell.directory.as_ref(), "active", &target, &common::control()).await.unwrap();
        windows.push((start, Instant::now()));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    stop.cancel();
    let commits = writer.await.unwrap();
    let verifier = cell.connect("active").await;
    assert_eq!(values(&verifier, "SELECT n FROM counters").await, [commits.len().to_string()]);

    let mut gaps: Vec<_> = windows.into_iter().map(|(start, end)| {
        let peak = commits.windows(2).filter(|pair| pair[0] <= end && pair[1] >= start)
            .map(|pair| pair[1] - pair[0]).max().expect("commits bracket the handoff");
        (peak, end - start)
    }).collect();
    gaps.sort_unstable_by_key(|(gap, _)| *gap);
    let slowest = gaps.last().copied().expect("at least one handoff");
    let gaps: Vec<_> = gaps.into_iter().map(|(gap, _)| gap).collect();
    println!("moves={moves} acknowledged_updates={} peak_commit_gap_p50_ms={:.3} peak_commit_gap_p95_ms={:.3} peak_commit_gap_p99_ms={:.3}",
        commits.len(), percentile(&gaps, 50), percentile(&gaps, 95), percentile(&gaps, 99));
    println!("worst_commit_gap_ms={:.3} admin_handoff_for_worst_gap_ms={:.3}",
        slowest.0.as_secs_f64() * 1000.0, slowest.1.as_secs_f64() * 1000.0);
    drop(verifier);
    cell.stop().await;
}
