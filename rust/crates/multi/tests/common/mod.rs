//! A whole cell in one process (a directory, three logkeepers, nodes and a gateway) for the cell and chaos tests.
#![allow(dead_code)]

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use rtdb_directory::{Directory, MemberPool, MemoryDirectory};
use rtdb_multi::{agent::AgentOptions, deployment::CellStorage, gateway, lease::LeaseOptions, process};
use rtdb_server::config::NodeConfiguration;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
use tokio_util::sync::CancellationToken;

pub fn s3_storage() -> Option<CellStorage> {
    s3_storage_in(&std::env::var("RTDB_S3_BUCKET").unwrap_or_else(|_| "rtdb-test".into()))
}

pub fn s3_archive_storage() -> Option<CellStorage> {
    s3_storage_in(&std::env::var("RTDB_S3_ARCHIVE_BUCKET").ok()?)
}

pub fn hosted_test_owner() -> rtdb_directory::HostedTask {
    rtdb_directory::HostedTask::new(
        "arn:aws:ecs:us-east-1:123456789012:cluster/rtdb-proof",
        "arn:aws:ecs:us-east-1:123456789012:task/rtdb-proof/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ).unwrap()
}

fn s3_storage_in(bucket: &str) -> Option<CellStorage> {
    let endpoint = std::env::var("RTDB_S3_ENDPOINT").ok()?;
    Some(CellStorage::parse(&format!("s3://{bucket}/run-{}?endpoint={endpoint}&path_style=true&region=us-east-1", uuid::Uuid::new_v4().simple())).unwrap())
}

pub async fn order_history(client: &tokio_postgres::Client) -> Vec<Vec<Option<String>>> {
    use tokio_postgres::SimpleQueryMessage;

    let history = "SELECT _id, total, _valid_from, _valid_to, _system_from, _system_to \
                   FROM orders FOR ALL SYSTEM_TIME FOR ALL VALID_TIME";
    let mut rows: Vec<Vec<Option<String>>> = client.simple_query(history).await.unwrap()
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some((0..6).map(|index| row.get(index).map(str::to_owned)).collect()),
            _ => None,
        })
        .collect();
    rows.sort();
    rows
}

async fn start_node(directory: &Arc<dyn Directory>, data: &std::path::Path, agent: AgentOptions, index: usize,
                    pool: MemberPool, compaction_workers: bool, engine_version: Option<String>) -> process::RunningNode {
    let keys: Arc<dyn rtdb_multi::keys::KeyService> = Arc::new(rtdb_multi::keys::LocalKeyService::new(data.join("keys")).unwrap());
    let claim_wait = std::env::var("RTDB_TEST_PLACED_CLAIM_WAIT_MS").ok().map_or(2, |ms| ms.parse().unwrap());
    let connections = pgwire_connections();
    let configuration = NodeConfiguration::from_yaml(&format!("authn: !SingleRootUser\n  password: cell-test\nserver:\n  port: 0\n  numThreads: {connections}\nflightSql: {{port: 0, ownedNativeActions: true}}\n")).unwrap();
    process::start_node(Arc::clone(directory), process::NodeSettings {
        hosted_task: None,
        advertise_host: None,
        configuration, allocation_limit_bytes: 1 << 30,
        control: "127.0.0.1:0".parse().unwrap(), availability_zone: format!("zone-{index}"), engine_version, agent, lease: lease(), claim_wait: Duration::from_millis(claim_wait), pool,
        keys, cache_dir: data.join(format!("cache-{index}")), local_compaction: !compaction_workers, capabilities: Some(secret()),
    }).await.unwrap()
}

pub fn pgwire_connections() -> usize {
    std::env::var("RTDB_TEST_PGWIRE_CONNECTIONS").ok().map_or(42, |count| count.parse::<usize>().unwrap())
}

/// The secret a cell's gateway and nodes share; a test cell's is fixed.
pub fn secret() -> Arc<rtdb_multi::capability::CellSecret> { Arc::new(rtdb_multi::capability::CellSecret::new(&[7; 32]).unwrap()) }

/// The control key a test cell's members sign with.
pub fn control() -> rtdb_multi::control::ControlKey { rtdb_multi::control::ControlKey(Some(secret())) }

pub fn lease() -> LeaseOptions { LeaseOptions { lease: Duration::from_millis(1500), heartbeat: Duration::from_millis(200) } }

pub struct Cell {
    pub directory: Arc<dyn Directory>,
    pub nodes: Vec<Option<process::RunningNode>>,
    pub logkeepers: Vec<process::RunningLogkeeper>,
    pub gateway: std::net::SocketAddr,
    pub gateway_cancellation: CancellationToken,
    pub data: tempfile::TempDir,
    pub objects: CellStorage,
}

impl Cell {
    pub async fn start(nodes: usize, park_after: Option<Duration>) -> Self { Self::start_with(vec![MemberPool::General; nodes], park_after).await }

    pub async fn start_with(pools: Vec<MemberPool>, park_after: Option<Duration>) -> Self { Self::start_watching(pools, park_after, None).await }

    /// A cell whose agents check their holdings against the directory only every `watch`, so other fences are what a test observes.
    pub async fn start_watching(pools: Vec<MemberPool>, park_after: Option<Duration>, watch: Option<Duration>) -> Self { Self::start_full(pools, park_after, watch, None).await }

    /// A cell named in a cell map, whose gateway forwards connections for deployments living in other cells.
    pub async fn start_in(cell: &str, map: Arc<dyn rtdb_multi::cells::CellMap>, nodes: usize) -> Self {
        Self::start_full(vec![MemberPool::General; nodes], None, None, Some((map, cell.to_owned()))).await
    }

    pub async fn start_full(pools: Vec<MemberPool>, park_after: Option<Duration>, watch: Option<Duration>, cells: Option<(Arc<dyn rtdb_multi::cells::CellMap>, String)>) -> Self {
        Self::start_on(None, pools, park_after, watch, cells).await
    }

    /// A cell keeping its objects in `objects`, or in a directory of its own.
    pub async fn start_on(objects: Option<CellStorage>, pools: Vec<MemberPool>, park_after: Option<Duration>, watch: Option<Duration>, cells: Option<(Arc<dyn rtdb_multi::cells::CellMap>, String)>) -> Self {
        Self::start_configured(objects, pools, park_after, watch, cells, true, |_| {}).await
    }

    pub async fn start_durable(nodes: usize) -> Self {
        Self::start_configured(None, vec![MemberPool::General; nodes], None, None, None, true, |_| {}).await
    }

    pub async fn start_without_log_fsync(nodes: usize) -> Self {
        Self::start_configured(None, vec![MemberPool::General; nodes], None, None, None, false, |_| {}).await
    }

    pub async fn start_rate_limited(limit: NonZeroUsize) -> Self {
        Self::start_configured(None, vec![MemberPool::General], None, None, None, false, |agent| agent.activations_per_second = limit).await
    }

    pub async fn start_with_resident_idle(nodes: usize, after: Duration, park_after: Option<Duration>) -> Self {
        Self::start_configured(None, vec![MemberPool::General; nodes], park_after, None, None, true, |agent| agent.resident_idle_after = Some(after)).await
    }

    pub async fn start_with_resident_deadline(directory: Arc<dyn Directory>, idle: Duration, park: Duration) -> Self {
        Self::start_configured_with_directory(directory, None, vec![MemberPool::General], Some(park), None, None, true,
            |agent| agent.resident_idle_after = Some(idle)).await
    }

    pub async fn start_with_idle_flush(delay: Duration) -> Self {
        Self::start_configured(None, vec![MemberPool::General], None, None, None, true, |agent| agent.clean_after_idle = delay).await
    }

    pub async fn start_with_minimum_residency(minimum: Duration) -> Self {
        Self::start_configured(None, vec![MemberPool::General], None, None, None, true, |agent| agent.min_residency = minimum).await
    }

    pub async fn start_scoped(objects: CellStorage, role: rtdb_multi::agent::S3WriteRole) -> Self {
        Self::start_configured(Some(objects), vec![MemberPool::General; 2], None, None, None, true, |agent| agent.s3_write_role = Some(role)).await
    }

    async fn start_configured(objects: Option<CellStorage>, pools: Vec<MemberPool>, park_after: Option<Duration>,
        watch: Option<Duration>, cells: Option<(Arc<dyn rtdb_multi::cells::CellMap>, String)>, fsync: bool, configure: impl FnOnce(&mut AgentOptions)) -> Self {
        Self::start_configured_with_directory(MemoryDirectory::new(), objects, pools, park_after, watch, cells, fsync, configure).await
    }

    pub async fn start_with_directory(directory: Arc<dyn Directory>) -> Self {
        Self::start_configured_with_directory(directory, None, vec![MemberPool::General], None, None, None, true, |_| {}).await
    }

    pub async fn start_configured_with_directory(directory: Arc<dyn Directory>, objects: Option<CellStorage>, pools: Vec<MemberPool>,
        park_after: Option<Duration>, watch: Option<Duration>, cells: Option<(Arc<dyn rtdb_multi::cells::CellMap>, String)>, fsync: bool,
        configure: impl FnOnce(&mut AgentOptions)) -> Self {
        let compaction_workers = pools.contains(&MemberPool::Compaction);
        let data = tempfile::tempdir().unwrap();
        let objects = objects.unwrap_or_else(|| CellStorage::Local { root: data.path().join("objects") });
        let mut logkeepers = Vec::new();
        for index in 0..3 {
            logkeepers.push(process::start_logkeeper(Arc::clone(&directory), process::LogkeeperSettings {
                authority: None,
                advertise_host: None,
                listen: "127.0.0.1:0".parse().unwrap(), data_dir: data.path().join(format!("logkeeper-{index}")), fsync, cache_bytes: rtdb_logkeeper::ServerOptions::DEFAULT_CACHE_BYTES,
                availability_zone: format!("zone-{index}"), lease: lease(),
            }).await.unwrap());
        }
        let mut agent = AgentOptions::new(objects.clone());
        if let Some(watch) = watch { agent.watch_interval = watch; }
        agent.park_after = park_after;
        configure(&mut agent);
        if agent.s3_write_role.is_some() {
            let keys = rtdb_multi::keys::LocalKeyService::new(data.path().join("keys")).unwrap();
            rtdb_multi::admin::initialize_storage(&objects, &keys).await.unwrap();
        }
        let mut running = Vec::new();
        for (index, pool) in pools.into_iter().enumerate() {
            running.push(Some(start_node(&directory, data.path(), agent.clone(), index, pool, compaction_workers, None).await));
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gateway = listener.local_addr().unwrap();
        let gateway_cancellation = CancellationToken::new();
        if let Some((map, cell)) = &cells { map.register_cell(cell, &gateway.to_string()).await.unwrap(); }
        tokio::spawn(gateway::serve(listener, Arc::clone(&directory), gateway::GatewayOptions { cells, touch_interval: Duration::from_millis(200), capabilities: Some(secret()), ..Default::default() }, gateway_cancellation.clone()));
        Self { directory, nodes: running, logkeepers, gateway, gateway_cancellation, data, objects }
    }

    /// Starts one more logkeeper.
    pub async fn add_logkeeper(&mut self) {
        let index = self.logkeepers.len() + 10;
        self.logkeepers.push(process::start_logkeeper(Arc::clone(&self.directory), process::LogkeeperSettings {
            authority: None,
            advertise_host: None,
            listen: "127.0.0.1:0".parse().unwrap(), data_dir: self.data.path().join(format!("logkeeper-{index}")), fsync: true, cache_bytes: rtdb_logkeeper::ServerOptions::DEFAULT_CACHE_BYTES,
            availability_zone: format!("zone-{index}"), lease: lease(),
        }).await.unwrap());
    }

    /// Starts one more general node, as autoscaling or a replacement would.
    pub async fn add_node(&mut self) -> usize {
        self.add_node_with_engine(None).await
    }

    pub async fn add_node_with_engine(&mut self, engine_version: Option<String>) -> usize {
        self.add_configured_node(engine_version, |_| {}).await
    }

    pub async fn add_configured_node(&mut self, engine_version: Option<String>, configure: impl FnOnce(&mut AgentOptions)) -> usize {
        let index = self.nodes.len();
        let mut agent = AgentOptions::new(self.objects.clone());
        configure(&mut agent);
        self.nodes.push(Some(start_node(&self.directory, self.data.path(), agent, index, MemberPool::General, false, engine_version).await));
        index
    }

    pub async fn connect(&self, database: &str) -> Client { connect(&self.gateway.to_string(), database).await }

    /// The index of the node the directory names as the deployment's holder.
    pub async fn holder(&self, name: &str) -> Option<usize> {
        let record = self.directory.deployment_by_name(name).await.unwrap().unwrap();
        let holder = record.holder?;
        self.nodes.iter().position(|node| node.as_ref().is_some_and(|node| node.lease.id() == holder))
    }

    pub fn storage(&self) -> CellStorage { self.objects.clone() }
    pub fn keys(&self) -> rtdb_multi::keys::LocalKeyService { rtdb_multi::keys::LocalKeyService::new(self.data.path().join("keys")).unwrap() }
    pub fn node(&self, index: usize) -> &process::RunningNode { self.nodes[index].as_ref().unwrap() }

    pub async fn stop(mut self) -> tempfile::TempDir {
        self.gateway_cancellation.cancel();
        for node in self.nodes.iter_mut().filter_map(Option::take) { node.kill().await.unwrap(); }
        for logkeeper in self.logkeepers.drain(..) { logkeeper.stop().await.unwrap(); }
        self.data
    }
}

pub async fn connect(address: &str, database: &str) -> Client {
    let (host, port) = address.rsplit_once(':').unwrap();
    let (client, connection) = tokio_postgres::connect(&format!("host={host} port={port} user=xtdb password=cell-test dbname={database}"), NoTls).await.unwrap();
    tokio::spawn(connection);
    client
}

/// Retries while the cell fails over: a connection to a holder that just died fails until its lease expires and another node claims.
pub async fn connect_eventually(cell: &Cell, database: &str) -> Client {
    let (host, port) = cell.gateway.to_string().rsplit_once(':').map(|(host, port)| (host.to_owned(), port.to_owned())).unwrap();
    for _ in 0..100 {
        if let Ok((client, connection)) = tokio_postgres::connect(&format!("host={host} port={port} user=xtdb password=cell-test dbname={database}"), NoTls).await {
            tokio::spawn(connection);
            if client.simple_query("SELECT 1").await.is_ok() { return client; }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no holder came up for {database}");
}

pub async fn values(client: &Client, sql: &str) -> Vec<String> {
    let mut values: Vec<String> = client.simple_query(sql).await.unwrap().into_iter().filter_map(|message| match message {
        SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
        _ => None,
    }).collect();
    values.sort();
    values
}


pub fn files_under(path: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            if entry.path().is_dir() { found.extend(files_under(&entry.path())); } else { found.push(entry.path()); }
        }
    }
    found
}
