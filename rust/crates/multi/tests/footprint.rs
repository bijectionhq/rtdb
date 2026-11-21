//! Measures the footprint of an open or parked deployment.
//! Run each measurement in a fresh process by naming its test before `-- --ignored --nocapture`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rtdb_directory::{Directory, MemberPool, MemoryDirectory};
use rtdb_multi::{admin, agent::AgentOptions, deployment::CellStorage, lease::LeaseOptions, process};
use rtdb_server::config::NodeConfiguration;

fn resident_bytes() -> u64 {
    let output = std::process::Command::new("ps").args(["-o", "rss=", "-p", &std::process::id().to_string()]).output().unwrap();
    String::from_utf8_lossy(&output.stdout).trim().parse::<u64>().unwrap_or(0) * 1024
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn the_resident_cost_of_an_idle_deployment() {
    let count: usize = std::env::var("RTDB_FOOTPRINT_DEPLOYMENTS").ok().and_then(|value| value.parse().ok()).unwrap_or(200);
    let data = tempfile::tempdir().unwrap();
    let directory: Arc<dyn Directory> = MemoryDirectory::new();
    let lease = LeaseOptions { lease: Duration::from_secs(30), heartbeat: Duration::from_secs(1) };
    let mut logkeepers = Vec::new();
    for index in 0..3 {
        logkeepers.push(process::start_logkeeper(Arc::clone(&directory), process::LogkeeperSettings {
            listen: "127.0.0.1:0".parse().unwrap(), data_dir: data.path().join(format!("lk{index}")), fsync: false, cache_bytes: rtdb_logkeeper::ServerOptions::DEFAULT_CACHE_BYTES, availability_zone: format!("z{index}"), lease: lease.clone(),
        }).await.unwrap());
    }
    let node = process::start_node(Arc::clone(&directory), process::NodeSettings {
        configuration: NodeConfiguration::from_yaml("server:\n  port: 0\n").unwrap(), allocation_limit_bytes: 4 << 30, control: "127.0.0.1:0".parse().unwrap(),
        availability_zone: "z0".into(), agent: AgentOptions::new(CellStorage::Local { root: data.path().join("objects") }), lease, claim_wait: Duration::from_millis(20),
        pool: MemberPool::General, keys: Arc::new(rtdb_multi::keys::LocalKeyService::new(data.path().join("keys")).unwrap()), cache_dir: data.path().join("cache"), local_compaction: true, capabilities: None,
    }).await.unwrap();
    for index in 0..count { admin::create_deployment(directory.as_ref(), &format!("d{index}"), "bench", 3).await.unwrap(); }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let (rss_before, budget_before, tasks_before) = (resident_bytes(), node.server.node().memory().used_bytes(), tokio::runtime::Handle::current().metrics().num_alive_tasks());
    let started = Instant::now();
    for index in 0..count { node.agent.activate(&format!("d{index}")).await.unwrap(); }
    let activation = started.elapsed();
    tokio::time::sleep(Duration::from_secs(2)).await;
    let (rss_after, budget_after, tasks_after) = (resident_bytes(), node.server.node().memory().used_bytes(), tokio::runtime::Handle::current().metrics().num_alive_tasks());
    println!("deployments: {count}");
    println!("activation: {:.1} ms each", activation.as_secs_f64() * 1000.0 / count as f64);
    println!("resident set per deployment: {} KiB", (rss_after.saturating_sub(rss_before)) / count as u64 / 1024);
    println!("execution budget per deployment: {} KiB", (budget_after.saturating_sub(budget_before)) as u64 / count as u64 / 1024);
    println!("tasks per deployment: {:.1}", (tasks_after.saturating_sub(tasks_before)) as f64 / count as f64);
    node.kill().await.unwrap();
    for logkeeper in logkeepers { logkeeper.stop().await.unwrap(); }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn parked_directory_density_with_an_active_watch() {
    use rtdb_directory::{DeploymentRecord, MemberId, watch_deployment};

    let count: usize = std::env::var("RTDB_PARKED_BENCH_DEPLOYMENTS").ok().and_then(|value| value.parse().ok()).unwrap_or(100_000);
    assert!(count > 0);
    let directory: Arc<dyn Directory> = MemoryDirectory::new();
    let ensemble = (0..3).map(|_| MemberId::fresh()).collect::<Vec<_>>();
    let hot = directory.create_deployment(DeploymentRecord::new("active", "bench", ensemble.clone(), 0)).await.unwrap();
    let mut watch = watch_deployment(directory.clone(), hot.id, Duration::from_millis(100));
    drop(watch.wait_for(|current| current.is_some()).await.unwrap());
    let runtime = tokio::runtime::Handle::current();
    let rss_before = resident_bytes();
    let tasks_before = runtime.metrics().num_alive_tasks();

    let started = Instant::now();
    for index in 0..count {
        let record = DeploymentRecord::new(format!("parked-{index}"), "bench", ensemble.clone(), 0);
        directory.create_deployment(record).await.unwrap();
    }
    let created_in = started.elapsed();
    let rss_after = resident_bytes();
    let tasks_after = runtime.metrics().num_alive_tasks();
    let final_record = directory.deployment_by_name(&format!("parked-{}", count - 1)).await.unwrap().unwrap();
    let serialized = serde_json::to_vec(&final_record).unwrap().len();

    assert!(final_record.holder.is_none());
    assert_eq!(tasks_after, tasks_before);
    println!("parked deployments: {count}");
    println!("directory creates per second: {:.0}", count as f64 / created_in.as_secs_f64());
    println!("resident bytes per parked deployment: {}", rss_after.saturating_sub(rss_before) / count as u64);
    println!("JSON bytes in the final deployment record: {serialized}");
    println!("additional Tokio tasks: {}", tasks_after - tasks_before);
}
