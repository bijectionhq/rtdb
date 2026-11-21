//! A whole cell in one process (a directory, three logkeepers, nodes and a gateway) for the cell and chaos tests.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::Duration;

use rtdb_directory::{Directory, MemberPool, MemoryDirectory};
use rtdb_multi::{agent::AgentOptions, deployment::CellStorage, gateway, lease::LeaseOptions, process};
use rtdb_server::config::NodeConfiguration;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
use tokio_util::sync::CancellationToken;

async fn start_node(directory: &Arc<dyn Directory>, data: &std::path::Path, storage: &CellStorage, index: usize, pool: MemberPool, park_after: Option<Duration>, compaction_workers: bool, watch: Option<Duration>) -> process::RunningNode {
    let mut agent = AgentOptions::new(storage.clone());
    if let Some(watch) = watch { agent.watch_interval = watch; }
    agent.park_after = park_after;
    let keys: Arc<dyn rtdb_multi::keys::KeyService> = Arc::new(rtdb_multi::keys::LocalKeyService::new(data.join("keys")).unwrap());
    process::start_node(Arc::clone(directory), process::NodeSettings {
        advertise_host: None,
        configuration: NodeConfiguration::from_yaml("authn: !SingleRootUser\n  password: cell-test\nserver:\n  port: 0\n").unwrap(), allocation_limit_bytes: 1 << 30,
        control: "127.0.0.1:0".parse().unwrap(), availability_zone: format!("zone-{index}"), agent, lease: lease(), claim_wait: Duration::from_millis(20), pool,
        keys, cache_dir: data.join(format!("cache-{index}")), local_compaction: !compaction_workers, capabilities: Some(secret()),
    }).await.unwrap()
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
        let compaction_workers = pools.contains(&MemberPool::Compaction);
        let data = tempfile::tempdir().unwrap();
        let objects = objects.unwrap_or_else(|| CellStorage::Local { root: data.path().join("objects") });
        let directory: Arc<dyn Directory> = MemoryDirectory::new();
        let mut logkeepers = Vec::new();
        for index in 0..3 {
            logkeepers.push(process::start_logkeeper(Arc::clone(&directory), process::LogkeeperSettings {
                advertise_host: None,
                listen: "127.0.0.1:0".parse().unwrap(), data_dir: data.path().join(format!("logkeeper-{index}")), fsync: false, cache_bytes: rtdb_logkeeper::ServerOptions::DEFAULT_CACHE_BYTES,
                availability_zone: format!("zone-{index}"), lease: lease(),
            }).await.unwrap());
        }
        let mut running = Vec::new();
        for (index, pool) in pools.into_iter().enumerate() { running.push(Some(start_node(&directory, data.path(), &objects, index, pool, park_after, compaction_workers, watch).await)); }
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
            advertise_host: None,
            listen: "127.0.0.1:0".parse().unwrap(), data_dir: self.data.path().join(format!("logkeeper-{index}")), fsync: false, cache_bytes: rtdb_logkeeper::ServerOptions::DEFAULT_CACHE_BYTES,
            availability_zone: format!("zone-{index}"), lease: lease(),
        }).await.unwrap());
    }

    /// Starts one more general node, as autoscaling or a replacement would.
    pub async fn add_node(&mut self) -> usize {
        let index = self.nodes.len();
        self.nodes.push(Some(start_node(&self.directory, self.data.path(), &self.objects, index, MemberPool::General, None, false, None).await));
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

    pub async fn stop(mut self) {
        self.gateway_cancellation.cancel();
        for node in self.nodes.iter_mut().filter_map(Option::take) { node.kill().await.unwrap(); }
        for logkeeper in self.logkeepers.drain(..) { logkeeper.stop().await.unwrap(); }
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
