use std::time::Duration;
use rtdb_server::{ServerDriver, config::NodeConfiguration};
use tokio::net::TcpStream;

#[tokio::test]
async fn configured_funded_receiver_requires_a_live_host_mount() {
    let config = NodeConfiguration::from_yaml_with_environment(
        r#"
fundedReceiverClock:
  installationFile: /etc/rtdb/site-receiver-clock.json
  domain: 12345678-1234-4234-8234-123456789abc
  placementEpoch: 7
  purpose: bijection.rtdb.durable-acceptance-clock.v4
"#,
        &|_| None,
        false,
    ).unwrap();
    let error = ServerDriver::open(config, 128 * 1024 * 1024).await.err().unwrap();
    assert_eq!(error.code, "rtdb.clock/installation");
}

#[tokio::test]
async fn production_composition_binds_drives_and_joins_all_listeners() {
    let config = NodeConfiguration::from_yaml_with_environment("server: {port: 0, readOnlyPort: 0}\nflightSql: {port: 0}\nhealthz: {port: 0}\ncompactor: {threads: 0}", &|_| None, false).unwrap();
    let (server, driver) = ServerDriver::open(config, 128 * 1024 * 1024).await.unwrap();
    let addresses = server.addresses().clone();
    let task = tokio::spawn(driver.run());
    let response = reqwest::get(format!("http://{}/healthz/ready", addresses.health.unwrap())).await.unwrap();
    assert_eq!(response.status(), 200);
    let idle = TcpStream::connect(addresses.pgwire.unwrap()).await.unwrap();
    let read_only = TcpStream::connect(addresses.read_only_pgwire.unwrap()).await.unwrap();
    server.shutdown();
    tokio::time::timeout(Duration::from_secs(10), task).await.unwrap().unwrap().unwrap();
    for address in [addresses.pgwire, addresses.read_only_pgwire, addresses.flight_sql, addresses.health].into_iter().flatten() { assert!(TcpStream::connect(address).await.is_err()); }
    assert!(server.node().cancellation().is_cancelled());
    drop((idle, read_only));
}

#[tokio::test]
async fn flight_sql_accepts_the_upstream_wildcard_host() {
    let yaml = "server: null\nflightSql: {host: '*', port: 0}\ncompactor: {threads: 0}";
    let config = NodeConfiguration::from_yaml_with_environment(yaml, &|_| None, false).unwrap();
    let (server, driver) = ServerDriver::open(config, 128 * 1024 * 1024).await.unwrap();
    let address = server.addresses().flight_sql.unwrap();
    assert_eq!(address.ip(), std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
    assert_ne!(address.port(), 0);
    driver.close_without_run().await.unwrap();
}

#[tokio::test]
async fn bind_failure_drops_previously_opened_listeners_without_starting_node_tasks() {
    let directory = tempfile::tempdir().unwrap();
    let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = occupied.local_addr().unwrap().port();
    let config = NodeConfiguration::from_yaml_with_environment(&format!("server: {{port: 0}}\nflightSql: null\nhealthz: {{port: {port}}}\ndiskCache: {{path: {}, maxSizeBytes: 12345}}", directory.path().display()), &|_| None, false).unwrap();
    assert!(ServerDriver::open(config, 128 * 1024 * 1024).await.is_err());
    assert!(TcpStream::connect(occupied.local_addr().unwrap()).await.is_ok());
    let cache = rtdb_storage::disk_cache::DiskCache::open(directory.path(), 12345, rtdb_columnar::MemoryBudget::new(1 << 20)).unwrap(); cache.close_and_wait().await;
}

#[tokio::test]
async fn closing_without_run_releases_bound_listeners_and_resource_owners() {
    let directory = tempfile::tempdir().unwrap();
    let yaml = format!("server: {{port: 0}}\nflightSql: {{port: 0}}\nhealthz: null\ncompactor: {{threads: 0}}\ndiskCache: {{path: {}, maxSizeBytes: 12345}}", directory.path().display());
    let config = NodeConfiguration::from_yaml_with_environment(&yaml, &|_| None, false).unwrap();
    let (server, driver) = ServerDriver::open(config, 128 * 1024 * 1024).await.unwrap(); let addresses = server.addresses().clone();
    driver.close_without_run().await.unwrap(); assert!(server.node().cancellation().is_cancelled());
    for address in [addresses.pgwire, addresses.flight_sql].into_iter().flatten() { assert!(TcpStream::connect(address).await.is_err()); }
    drop(server);
    let cache = rtdb_storage::disk_cache::DiskCache::open(directory.path(), 12345, rtdb_columnar::MemoryBudget::new(1 << 20)).unwrap(); cache.close_and_wait().await;
}

#[tokio::test]
async fn configured_disk_cache_is_opened_and_registered_without_remote_storage() {
    use rtdb_plan::{CatalogMetric, CatalogMetrics};
    let directory = tempfile::tempdir().unwrap();
    let yaml = format!("server: null\nflightSql: null\nhealthz: null\ncompactor: {{threads: 0}}\ndiskCache: {{path: {}, maxSizeBytes: 12345}}", directory.path().display());
    let config = NodeConfiguration::from_yaml_with_environment(&yaml, &|_| None, false).unwrap();
    let (server, driver) = ServerDriver::open(config, 128 * 1024 * 1024).await.unwrap();
    let metrics = server.metrics().metrics().unwrap();
    assert!(metrics.rows().iter().any(|metric| matches!(metric, CatalogMetric::Gauge { name, value, .. } if name == "disk-cache.maxSizeBytes" && *value == 12345.0)));
    drop(metrics);
    let task = tokio::spawn(driver.run());
    server.shutdown();
    task.await.unwrap().unwrap();
}
