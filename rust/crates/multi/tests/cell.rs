//! A whole cell in one process: a directory, three logkeepers, two nodes and a gateway, driven through a real Postgres client.

use std::sync::Arc;
use std::time::Duration;

use rtdb_directory::{Directory, MemoryDirectory};
use rtdb_multi::{admin, agent::AgentOptions, deployment::CellStorage, gateway, lease::LeaseOptions, process};
use rtdb_server::config::NodeConfiguration;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};
use tokio_util::sync::CancellationToken;

fn lease() -> LeaseOptions { LeaseOptions { lease: Duration::from_millis(1500), heartbeat: Duration::from_millis(200) } }

struct Cell {
    directory: Arc<dyn Directory>,
    nodes: Vec<Option<process::RunningNode>>,
    logkeepers: Vec<process::RunningLogkeeper>,
    gateway: std::net::SocketAddr,
    gateway_cancellation: CancellationToken,
    _data: tempfile::TempDir,
}

impl Cell {
    async fn start(nodes: usize, park_after: Option<Duration>) -> Self {
        let data = tempfile::tempdir().unwrap();
        let directory: Arc<dyn Directory> = MemoryDirectory::new();
        let mut logkeepers = Vec::new();
        for index in 0..3 {
            logkeepers.push(process::start_logkeeper(Arc::clone(&directory), process::LogkeeperSettings {
                listen: "127.0.0.1:0".parse().unwrap(), data_dir: data.path().join(format!("logkeeper-{index}")), fsync: false,
                availability_zone: format!("zone-{index}"), lease: lease(),
            }).await.unwrap());
        }
        let mut running = Vec::new();
        for index in 0..nodes {
            let mut agent = AgentOptions::new(CellStorage::Local { root: data.path().join("objects") });
            agent.park_after = park_after;
            running.push(Some(process::start_node(Arc::clone(&directory), process::NodeSettings {
                configuration: NodeConfiguration::from_yaml("server:\n  port: 0\n").unwrap(), allocation_limit_bytes: 1 << 30,
                control: "127.0.0.1:0".parse().unwrap(), availability_zone: format!("zone-{index}"), agent, lease: lease(), claim_wait: Duration::from_millis(20),
            }).await.unwrap()));
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gateway = listener.local_addr().unwrap();
        let gateway_cancellation = CancellationToken::new();
        tokio::spawn(gateway::serve(listener, Arc::clone(&directory), gateway::GatewayOptions { touch_interval: Duration::from_millis(200), ..Default::default() }, gateway_cancellation.clone()));
        Self { directory, nodes: running, logkeepers, gateway, gateway_cancellation, _data: data }
    }

    async fn connect(&self, database: &str) -> Client { connect(&self.gateway.to_string(), database).await }

    /// The index of the node the directory names as the deployment's holder.
    async fn holder(&self, name: &str) -> Option<usize> {
        let record = self.directory.deployment_by_name(name).await.unwrap().unwrap();
        let holder = record.holder?;
        self.nodes.iter().position(|node| node.as_ref().is_some_and(|node| node.lease.id() == holder))
    }

    async fn stop(mut self) {
        self.gateway_cancellation.cancel();
        for node in self.nodes.iter_mut().filter_map(Option::take) { node.kill().await.unwrap(); }
        for logkeeper in self.logkeepers.drain(..) { logkeeper.stop().await.unwrap(); }
    }
}

async fn connect(address: &str, database: &str) -> Client {
    let (host, port) = address.rsplit_once(':').unwrap();
    let (client, connection) = tokio_postgres::connect(&format!("host={host} port={port} user=xtdb dbname={database}"), NoTls).await.unwrap();
    tokio::spawn(connection);
    client
}

/// Retries while the cell fails over: a connection to a holder that just died fails until its lease expires and another node claims.
async fn connect_eventually(cell: &Cell, database: &str) -> Client {
    let (host, port) = cell.gateway.to_string().rsplit_once(':').map(|(host, port)| (host.to_owned(), port.to_owned())).unwrap();
    for _ in 0..100 {
        if let Ok((client, connection)) = tokio_postgres::connect(&format!("host={host} port={port} user=xtdb dbname={database}"), NoTls).await {
            tokio::spawn(connection);
            if client.simple_query("SELECT 1").await.is_ok() { return client; }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no holder came up for {database}");
}

async fn values(client: &Client, sql: &str) -> Vec<String> {
    let mut values: Vec<String> = client.simple_query(sql).await.unwrap().into_iter().filter_map(|message| match message {
        SimpleQueryMessage::Row(row) => row.get(0).map(str::to_owned),
        _ => None,
    }).collect();
    values.sort();
    values
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deployment_is_served_through_the_gateway_and_survives_its_holder_dying() {
    let mut cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"]);
    drop(client);

    let first = cell.holder("shop").await.expect("the first connection placed the deployment");
    let epoch = cell.directory.deployment_by_name("shop").await.unwrap().unwrap().epoch;
    cell.nodes[first].take().unwrap().kill().await.unwrap();

    let client = connect_eventually(&cell, "shop").await;
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"], "nothing acknowledged is lost when the holder dies");
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10", "20"]);
    let record = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    assert!(record.epoch > epoch, "the survivor holds it under a later epoch");
    assert_ne!(cell.holder("shop").await, Some(first));
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_partitioned_holder_cannot_commit_once_another_member_takes_over() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(client);

    let zombie_index = cell.holder("shop").await.unwrap();
    let zombie = cell.nodes[zombie_index].as_ref().unwrap();
    let direct = connect(&zombie.pgwire_address.to_string(), "shop").await;
    let zombie_id = zombie.lease.id();
    zombie.stop_renewing();
    while cell.directory.member(&zombie_id).await.unwrap().unwrap().lease_expires_at_ms > rtdb_directory::now_ms() { tokio::time::sleep(Duration::from_millis(50)).await; }

    let client = connect_eventually(&cell, "shop").await;
    assert_ne!(cell.holder("shop").await, Some(zombie_index), "the other node took over");
    let refused = direct.simple_query("INSERT INTO orders (_id, total) VALUES (99, 990)").await;
    assert!(refused.is_err(), "a deposed holder's write does not commit: {refused:?}");
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10", "20"], "the deposed holder's write is visible nowhere");
    drop(client);
    drop(direct);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_deployment_parks_and_wakes_on_its_next_connection() {
    let cell = Cell::start(1, Some(Duration::from_millis(800))).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(client);

    let mut parked = false;
    for _ in 0..50 {
        let record = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
        if record.holder.is_none() && record.epoch > 0 { parked = true; break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(parked, "an untouched deployment is parked: no holder");
    assert!(cell.nodes[0].as_ref().unwrap().agent.held().is_empty(), "and its node runs nothing for it");

    let client = cell.connect("shop").await;
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"], "waking replays nothing lost");
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deployments_are_isolated_from_each_other() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    admin::create_deployment(cell.directory.as_ref(), "blog", "globex", 3).await.unwrap();
    let shop = cell.connect("shop").await;
    let blog = cell.connect("blog").await;
    shop.simple_query("INSERT INTO items (_id, label) VALUES (1, 'shop')").await.unwrap();
    blog.simple_query("INSERT INTO items (_id, label) VALUES (1, 'blog')").await.unwrap();
    assert_eq!(values(&shop, "SELECT label FROM items").await, ["shop"]);
    assert_eq!(values(&blog, "SELECT label FROM items").await, ["blog"]);
    let unknown = tokio_postgres::connect(&format!("host=127.0.0.1 port={} user=xtdb dbname=nowhere", cell.gateway.port()), NoTls).await;
    assert!(unknown.is_err(), "an unknown deployment is refused at the gateway");
    drop((shop, blog));
    cell.stop().await;
}
