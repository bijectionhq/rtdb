use std::{collections::BTreeMap, sync::Arc, time::Duration};
use rtdb_columnar::MemoryBudget;
use rtdb_maintenance::CollectionObserver;
use rtdb_node::{Node, NodeOptions, NodeServices};
use rtdb_plan::{CatalogMetric, CatalogMetrics, CatalogRows, CatalogUser, CatalogUsers};
use rtdb_server::{health::{HealthConfig, HealthDriver}, prometheus};
use rtdb_tx::TransactionAuthorization;
use rtdb_types::{Anomaly, Result, TableRef};
use tokio_util::sync::CancellationToken;

struct Fixtures;
impl TransactionAuthorization for Fixtures { fn check_role_membership(&self, _: &str, _: Option<&str>) -> Result<()> { Ok(()) } }
impl CollectionObserver for Fixtures { fn table_failed(&self, _: Option<&TableRef>, error: &Anomaly) { panic!("{error}"); } }
impl CatalogUsers for Fixtures {
    fn users(&self) -> Result<CatalogRows<CatalogUser>> { Ok(CatalogRows::new(Vec::new(), Arc::new(()))) }
    fn is_superuser(&self, _: &str) -> bool { false }
}
impl CatalogMetrics for Fixtures {
    fn metrics(&self) -> Result<CatalogRows<CatalogMetric>> {
        Ok(CatalogRows::new(vec![CatalogMetric::Counter { name: "query.error".into(), tags: BTreeMap::new(), count: 3.0 }], Arc::new(())))
    }
}

#[tokio::test]
async fn health_listener_routes_real_requests_and_joins_idle_connections() {
    let mut options = NodeOptions::default();
    options.automatic_compaction = false;
    options.automatic_gc = false;
    let services = NodeServices::local(Arc::new(Fixtures), Arc::new(Fixtures), Arc::new(Fixtures), Arc::new(Fixtures));
    let (node, node_driver) = Node::open(options, services).await.unwrap();
    let node_task = tokio::spawn(node_driver.run());
    let config = HealthConfig { drain_timeout: Duration::from_millis(100), ..Default::default() };
    let (health, driver) = HealthDriver::bind(config, node.clone(), Arc::new(Fixtures), &CancellationToken::new()).await.unwrap();
    let task = tokio::spawn(driver.run());
    let client = reqwest::Client::new();
    let base = format!("http://{}", health.address());
    for (path, body) in [("ready", "Ready."), ("alive", "Alive."), ("started", "Started.")] {
        let response = client.get(format!("{base}/healthz/{path}")).send().await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(response.text().await.unwrap(), body);
    }
    let response = client.get(format!("{base}/metrics")).send().await.unwrap();
    assert_eq!(response.headers()["content-type"], "text/plain; version=0.0.4");
    assert!(response.text().await.unwrap().contains("query_error_total 3"));
    assert_eq!(client.get(format!("{base}/system/finish-block")).send().await.unwrap().status(), 405);
    let response = client.post(format!("{base}/system/finish-block?db=absent")).send().await.unwrap();
    assert_eq!(response.status(), 500);
    assert_eq!(client.get(format!("{base}/missing")).send().await.unwrap().status(), 404);
    let idle = tokio::net::TcpStream::connect(health.address()).await.unwrap();
    health.shutdown();
    tokio::time::timeout(Duration::from_secs(5), task).await.unwrap().unwrap().unwrap();
    drop(idle);
    assert!(tokio::net::TcpStream::connect(health.address()).await.is_err());
    node.shutdown();
    node_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn node_metrics_report_the_memory_cache_and_available_compaction_jobs() {
    let mut options = NodeOptions::default();
    options.automatic_compaction = false;
    options.automatic_gc = false;
    let services = NodeServices::local(Arc::new(Fixtures), Arc::new(Fixtures), Arc::new(Fixtures), Arc::new(Fixtures));
    let (node, node_driver) = Node::open(options, services).await.unwrap();
    let node_task = tokio::spawn(node_driver.run());
    let rows = node.metrics_provider().unwrap().metrics().unwrap();
    let gauge = |expected: &str| rows.rows().iter().find_map(|metric| match metric { CatalogMetric::Gauge { name, value, .. } if name == expected => Some(*value), _ => None });
    let (retained, limit) = (gauge("rtdb.cache.retained.bytes").unwrap(), gauge("rtdb.cache.limit.bytes").unwrap());
    assert_eq!(gauge("memory-cache.pinnedBytes"), Some(retained));
    assert_eq!(gauge("memory-cache.evictableBytes"), Some(0.0));
    assert_eq!(gauge("memory-cache.freeBytes"), Some(limit - retained));
    assert!(rows.rows().iter().any(|metric| matches!(metric, CatalogMetric::Gauge { name, tags, value, .. }
        if name == "compactor.jobs.available" && *value == 0.0 && tags.get("db").is_some_and(|db| db == "xtdb"))));
    let allocator_gauge = |expected: &str, allocator: &str, database: &str| rows.rows().iter().find_map(|metric| match metric {
        CatalogMetric::Gauge { name, tags, unit: Some(rtdb_plan::CatalogMetricUnit::Bytes), value }
            if name == expected && tags.get("allocator").map(String::as_str) == Some(allocator) && tags.get("database").map(String::as_str) == Some(database) => Some(*value),
        _ => None,
    });
    assert_eq!(allocator_gauge("xtdb.allocator.memory.allocated", "memory-cache", ""), Some(retained));
    assert_eq!(allocator_gauge("xtdb.allocator.memory.limit", "memory-cache", ""), Some(limit));
    assert!(allocator_gauge("xtdb.allocator.memory.allocated", "query", "").is_some());
    assert!(allocator_gauge("xtdb.allocator.memory.limit", "query", "").is_some());
    assert!(allocator_gauge("xtdb.allocator.memory.allocated", "buffer-pool", "xtdb").is_some());
    assert_eq!(allocator_gauge("xtdb.allocator.memory.limit", "buffer-pool", "xtdb"), Some(limit));
    drop(rows);
    node.shutdown();
    node_task.await.unwrap().unwrap();
}

#[test]
fn prometheus_names_escaping_units_and_payload_lifetime() {
    let memory = MemoryBudget::new(1_000_000);
    let tags = BTreeMap::from([("node-id".into(), "line\n\"\\".into())]);
    let metrics = vec![
        CatalogMetric::Gauge { name: "node.tx.lag.MsgId".into(), tags: tags.clone(), unit: None, value: f64::NAN },
        CatalogMetric::Timer { name: "query.timer".into(), tags, count: 2, total_ns: 1_000_000_001.0, mean_ns: Some(500_000_000), max_ns: Some(750_000_000), p75_ns: Some(750_000_000), p95_ns: None, p99_ns: None, p999_ns: None, percentiles_ns: [750_000_000.0; 6] },
        CatalogMetric::DistributionSummary { name: "commit.lag".into(), tags: BTreeMap::new(), unit: None, count: 2, total: 3.25, maximum: 2.0, percentiles: [1.25, 2.0, 2.0] },
        CatalogMetric::Gauge { name: "allocator.memory".into(), tags: BTreeMap::new(), unit: Some(rtdb_plan::CatalogMetricUnit::Bytes), value: 42.0 },
        CatalogMetric::Gauge { name: "block.upload.seconds".into(), tags: BTreeMap::new(), unit: Some(rtdb_plan::CatalogMetricUnit::Seconds), value: 7.0 },
    ];
    let bytes = prometheus::render(&metrics, &memory).unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(text.contains("node_tx_lag_MsgId{node_id=\"line\\n\\\"\\\\\"} NaN"));
    assert!(text.contains("query_timer_seconds_sum{node_id=\"line\\n\\\"\\\\\"} 1.000000001\n"));
    for quantile in ["0.75", "0.85", "0.95", "0.98", "0.99", "0.999"] { assert!(text.contains(&format!("quantile=\"{quantile}\""))); }
    assert!(text.contains("commit_lag{quantile=\"0.5\"} 1.25\n"));
    assert!(text.contains("commit_lag_count 2\ncommit_lag_sum 3.25\n# HELP commit_lag_max  \n# TYPE commit_lag_max gauge\ncommit_lag_max 2\n"));
    assert!(text.contains("allocator_memory_bytes 42\n"));
    assert!(text.contains("block_upload_seconds 7\n"));
    assert!(!text.contains("block_upload_seconds_seconds"));
    let retained = bytes.slice(0..1);
    drop(bytes);
    assert!(memory.used_bytes() > 0);
    drop(retained);
    assert_eq!(memory.used_bytes(), 0);
    assert!(prometheus::render(&metrics, &MemoryBudget::new(100)).is_err());
}

#[test]
fn prometheus_families_carry_help_and_type_and_group_their_series() {
    let memory = MemoryBudget::new(1_000_000);
    let db = |name: &str| BTreeMap::from([("db".into(), name.into())]);
    let metrics = vec![
        CatalogMetric::Gauge { name: "xtdb.log.leader".into(), tags: db("a"), unit: None, value: 1.0 },
        CatalogMetric::Counter { name: "tx.error".into(), tags: BTreeMap::new(), count: 2.0 },
        CatalogMetric::Gauge { name: "xtdb.log.leader".into(), tags: db("b"), unit: None, value: 0.0 },
        CatalogMetric::Timer { name: "query.timer".into(), tags: BTreeMap::new(), count: 1, total_ns: 2e9, mean_ns: None, max_ns: Some(2_000_000_000), p75_ns: None, p95_ns: None, p99_ns: None, p999_ns: None, percentiles_ns: [2e9; 6] },
    ];
    let bytes = prometheus::render(&metrics, &memory).unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(text.starts_with("# HELP xtdb_log_leader  \n# TYPE xtdb_log_leader gauge\nxtdb_log_leader{db=\"a\"} 1\nxtdb_log_leader{db=\"b\"} 0\n"), "{text}");
    assert!(text.contains("# HELP tx_error_total  \n# TYPE tx_error_total counter\ntx_error_total 2\n"), "{text}");
    assert!(text.contains("# TYPE query_timer_seconds summary\nquery_timer_seconds{quantile=\"0.75\"} 2\n"), "{text}");
    assert!(text.contains("query_timer_seconds_count 1\nquery_timer_seconds_sum 2\n# HELP query_timer_seconds_max  \n# TYPE query_timer_seconds_max gauge\nquery_timer_seconds_max 2\n"), "{text}");
    assert_eq!(text.matches("# TYPE ").count(), 4);
    assert_eq!(text.matches("# HELP ").count(), 4);
}

#[test]
fn prometheus_describes_follower_metrics_and_their_maxima() {
    let memory = MemoryBudget::new(1_000_000);
    let metrics = vec![
        CatalogMetric::Timer { name: "xtdb.replica.process.timer".into(), tags: BTreeMap::new(), count: 1, total_ns: 1e9, mean_ns: None, max_ns: Some(1_000_000_000), p75_ns: None, p95_ns: None, p99_ns: None, p999_ns: None, percentiles_ns: [1e9; 6] },
        CatalogMetric::Timer { name: "xtdb.replica.block.buffer.timer".into(), tags: BTreeMap::new(), count: 0, total_ns: 0.0, mean_ns: None, max_ns: None, p75_ns: None, p95_ns: None, p99_ns: None, p999_ns: None, percentiles_ns: [0.0; 6] },
        CatalogMetric::DistributionSummary { name: "xtdb.replica.block.buffered.records".into(), tags: BTreeMap::new(), unit: None, count: 1, total: 7.0, maximum: 7.0, percentiles: [7.0; 3] },
    ];
    let bytes = prometheus::render(&metrics, &memory).unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    for (family, help) in [
        ("xtdb_replica_process_timer_seconds", "Time spent processing replica log records, by message type"),
        ("xtdb_replica_block_buffer_timer_seconds", "Time the follower spends buffering records between BlockBoundary and BlockUploaded"),
        ("xtdb_replica_block_buffered_records", "Number of records buffered while a follower waits for BlockUploaded"),
    ] {
        assert!(text.contains(&format!("# HELP {family} {help}\n# TYPE {family} summary\n")));
        assert!(text.contains(&format!("# HELP {family}_max {help}\n# TYPE {family}_max gauge\n")));
    }
}
