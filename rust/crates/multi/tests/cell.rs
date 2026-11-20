//! A whole cell in one process: a directory, three logkeepers, two nodes and a gateway, driven through a real Postgres client.

use std::sync::Arc;
use std::time::Duration;

use rtdb_directory::{Directory, MemberPool, MemoryDirectory};
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
    data: tempfile::TempDir,
}

impl Cell {
    async fn start(nodes: usize, park_after: Option<Duration>) -> Self { Self::start_with(vec![MemberPool::General; nodes], park_after).await }

    async fn start_with(pools: Vec<MemberPool>, park_after: Option<Duration>) -> Self {
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
        for (index, pool) in pools.into_iter().enumerate() {
            let mut agent = AgentOptions::new(CellStorage::Local { root: data.path().join("objects") });
            let keys: Arc<dyn rtdb_multi::keys::KeyService> = Arc::new(rtdb_multi::keys::LocalKeyService::new(data.path().join("keys")).unwrap());
            agent.park_after = park_after;
            running.push(Some(process::start_node(Arc::clone(&directory), process::NodeSettings {
                configuration: NodeConfiguration::from_yaml("server:\n  port: 0\n").unwrap(), allocation_limit_bytes: 1 << 30,
                control: "127.0.0.1:0".parse().unwrap(), availability_zone: format!("zone-{index}"), agent, lease: lease(), claim_wait: Duration::from_millis(20), pool,
                keys, cache_dir: data.path().join(format!("cache-{index}")),
            }).await.unwrap()));
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let gateway = listener.local_addr().unwrap();
        let gateway_cancellation = CancellationToken::new();
        tokio::spawn(gateway::serve(listener, Arc::clone(&directory), gateway::GatewayOptions { touch_interval: Duration::from_millis(200), ..Default::default() }, gateway_cancellation.clone()));
        Self { directory, nodes: running, logkeepers, gateway, gateway_cancellation, data }
    }

    async fn connect(&self, database: &str) -> Client { connect(&self.gateway.to_string(), database).await }

    /// The index of the node the directory names as the deployment's holder.
    async fn holder(&self, name: &str) -> Option<usize> {
        let record = self.directory.deployment_by_name(name).await.unwrap().unwrap();
        let holder = record.holder?;
        self.nodes.iter().position(|node| node.as_ref().is_some_and(|node| node.lease.id() == holder))
    }

    fn storage(&self) -> CellStorage { CellStorage::Local { root: self.data.path().join("objects") } }
    fn keys(&self) -> rtdb_multi::keys::LocalKeyService { rtdb_multi::keys::LocalKeyService::new(self.data.path().join("keys")).unwrap() }
    fn node(&self, index: usize) -> &process::RunningNode { self.nodes[index].as_ref().unwrap() }

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_drained_node_hands_its_deployments_to_the_rest_of_the_cell() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(client);

    let drained = cell.holder("shop").await.unwrap();
    admin::drain(cell.directory.as_ref(), &cell.nodes[drained].as_ref().unwrap().lease.id()).await.unwrap();
    for _ in 0..100 {
        if cell.nodes[drained].as_ref().unwrap().agent.held().is_empty() { break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(cell.nodes[drained].as_ref().unwrap().agent.held().is_empty(), "a draining node parks what it holds");

    let client = cell.connect("shop").await;
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"]);
    let holder = cell.holder("shop").await.unwrap();
    assert_ne!(holder, drained, "the next request activates it on a node that may take placements");
    drop(client);
    cell.stop().await;
}


fn files_under(path: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            if entry.path().is_dir() { found.extend(files_under(&entry.path())); } else { found.push(entry.path()); }
        }
    }
    found
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_only_session_is_served_by_a_log_fed_replica() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let writer = cell.connect("shop").await;
    writer.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    admin::set_read_width(cell.directory.as_ref(), "shop", 2).await.unwrap();
    let holder = cell.holder("shop").await.unwrap();

    let (reader, connection) = tokio_postgres::Config::new().host("127.0.0.1").port(cell.gateway.port()).user("xtdb").dbname("shop")
        .options("-c default_transaction_read_only=on").connect(NoTls).await.unwrap();
    tokio::spawn(connection);
    let replica = 1 - holder;
    assert_eq!(cell.node(replica).agent.subscribed(), ["shop"], "the read is routed to the node beside the holder");
    let mut seen = Vec::new();
    for _ in 0..100 {
        // Until the replica has replayed the table's creation, the table is unknown there.
        if reader.simple_query("SELECT total FROM orders").await.is_ok() { seen = values(&reader, "SELECT total FROM orders").await; }
        if seen == ["10"] { break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(seen, ["10"], "the replica catches up from the log");
    assert!(reader.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.is_err(), "a replica refuses writes");
    drop((writer, reader));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_planned_move_and_a_rebalance_hand_deployments_over_without_losing_them() {
    let cell = Cell::start(2, None).await;
    for name in ["a1", "a2", "a3", "a4"] {
        admin::create_deployment(cell.directory.as_ref(), name, "acme", 3).await.unwrap();
        let client = cell.connect(name).await;
        client.simple_query(&format!("INSERT INTO items (_id, label) VALUES (1, '{name}')")).await.unwrap();
    }
    let target = cell.node(0).lease.id();
    for name in ["a1", "a2", "a3", "a4"] { admin::move_deployment(cell.directory.as_ref(), name, &target).await.unwrap(); }
    assert_eq!(cell.node(0).agent.held().len(), 4, "every deployment moved to the target");
    let moved = admin::rebalance(cell.directory.as_ref(), 10).await.unwrap();
    assert_eq!(moved, 2);
    assert_eq!((cell.node(0).agent.held().len(), cell.node(1).agent.held().len()), (2, 2), "rebalancing evens the holdings out");
    for name in ["a1", "a2", "a3", "a4"] {
        let client = cell.connect(name).await;
        assert_eq!(values(&client, "SELECT label FROM items").await, [name], "nothing is lost in a move");
    }
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deployment_whose_activations_crash_is_quarantined_to_the_isolation_pool() {
    let cell = Cell::start_with(vec![MemberPool::General, MemberPool::Isolation], None).await;
    let record = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(client);
    cell.node(0).agent.park("shop").await.unwrap();
    let current = cell.directory.deployment(&record.id).await.unwrap().unwrap();
    let mut broken = current.clone();
    broken.ensemble = vec![rtdb_directory::MemberId("no-such-logkeeper".into())];
    cell.directory.replace_deployment(broken, current.version).await.unwrap();

    for _ in 0..3 { let _ = tokio_postgres::connect(&format!("host=127.0.0.1 port={} user=xtdb dbname=shop", cell.gateway.port()), NoTls).await; }
    let quarantined = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    assert_eq!(quarantined.lifecycle, rtdb_directory::Lifecycle::Quarantined);
    assert!(cell.node(0).agent.held().is_empty(), "a general node no longer takes it");
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fork_starts_from_its_parents_data_and_diverges_and_a_deletion_leaves_nothing_readable() {
    let cell = Cell::start(2, None).await;
    let parent = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    drop(client);

    let child = admin::fork(cell.directory.as_ref(), &cell.storage(), &cell.keys(), "shop", "shop-preview").await.unwrap();
    assert_eq!((child.lineage.as_str(), child.parent.as_deref()), (parent.lineage.as_str(), Some(parent.id.as_str())));
    let preview = cell.connect("shop-preview").await;
    assert_eq!(values(&preview, "SELECT total FROM orders").await, ["10", "20"], "the fork starts from its parent's state");
    preview.simple_query("INSERT INTO orders (_id, total) VALUES (3, 30)").await.unwrap();
    let shop = cell.connect("shop").await;
    assert_eq!(values(&shop, "SELECT total FROM orders").await, ["10", "20"], "the fork's writes stay in the fork");
    shop.simple_query("INSERT INTO orders (_id, total) VALUES (4, 40)").await.unwrap();
    assert_eq!(values(&preview, "SELECT total FROM orders").await, ["10", "20", "30"], "and the parent's in the parent");
    drop((shop, preview));

    let deletion = admin::delete_deployment(cell.directory.as_ref(), &cell.storage(), &cell.keys(), "shop", 4 << 20).await.unwrap();
    assert!(deletion.verified_empty);
    let rtdb_multi::deployment::CellStorage::Local { root } = cell.storage();
    assert!(!root.join(parent.storage_prefix()).exists(), "the parent's objects are erased");
    assert!(!cell.data.path().join("keys").join(format!("{}.key", parent.id)).exists(), "its key is destroyed");
    assert_eq!(cell.directory.deployment_by_name("shop").await.unwrap().unwrap().lifecycle, rtdb_directory::Lifecycle::Deleted);
    assert!(tokio_postgres::connect(&format!("host=127.0.0.1 port={} user=xtdb dbname=shop", cell.gateway.port()), NoTls).await.is_err(), "a deleted deployment is refused");
    let preview = connect_eventually(&cell, "shop-preview").await;
    assert_eq!(values(&preview, "SELECT total FROM orders").await, ["10", "20", "30"], "a fork outlives its parent's deletion");
    drop(preview);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nothing_a_deployment_stores_is_readable_at_rest() {
    let cell = Cell::start(1, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, note) VALUES (1, 'plaintext-marker-zq81')").await.unwrap();
    drop(client);
    cell.node(0).agent.park("shop").await.unwrap();
    let files = files_under(cell.data.path());
    assert!(files.iter().any(|file| file.to_string_lossy().contains("/blocks/")), "the park flushed a block");
    for file in files {
        let bytes = std::fs::read(&file).unwrap();
        assert!(!bytes.windows(21).any(|window| window == b"plaintext-marker-zq81"), "{} holds the row in the clear", file.display());
    }
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_open_connection_survives_a_planned_move_and_its_holders_death() {
    let mut cell = Cell::start(3, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let statement = client.prepare("SELECT total FROM orders WHERE _id = 2").await.unwrap();

    let first = cell.holder("shop").await.unwrap();
    let target = (first + 1) % 3;
    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(target).lease.id()).await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"], "the same connection reads on the new holder");
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    let row = client.query_one(&statement, &[]).await.unwrap();
    assert_eq!(row.get::<_, i64>(0), 20, "a statement prepared before the move still runs after it");

    let holder = cell.holder("shop").await.unwrap();
    assert_eq!(holder, target);
    cell.nodes[holder].take().unwrap().kill().await.unwrap();
    tokio::time::sleep(Duration::from_millis(1800)).await;
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10", "20"], "and survives its holder's death while idle");
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_small_flush_writes_one_pack_and_its_catalog() {
    let cell = Cell::start(1, None).await;
    let record = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    for table in ["orders", "items", "users"] { client.simple_query(&format!("INSERT INTO {table} (_id, v) VALUES (1, 1)")).await.unwrap(); }
    drop(client);
    cell.node(0).agent.park("shop").await.unwrap();
    let rtdb_multi::deployment::CellStorage::Local { root } = cell.storage();
    let objects: Vec<String> = files_under(&root.join(record.storage_prefix())).into_iter().map(|path| path.to_string_lossy().into_owned()).collect();
    assert_eq!(objects.iter().filter(|path| path.contains("/~packs/")).count(), 1, "{objects:#?}");
    assert_eq!(objects.iter().filter(|path| path.contains("/tables/")).count(), 0, "no table file is its own object: {objects:#?}");
    let client = cell.connect("shop").await;
    assert_eq!(values(&client, "SELECT v FROM items").await, ["1"], "a packed block reads back after the park");
    drop(client);
    cell.stop().await;
}
