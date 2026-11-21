//! A whole cell in one process: a directory, three logkeepers, nodes and a gateway, driven through a real Postgres client.

mod common;

use std::time::Duration;

use common::*;
use rtdb_directory::{DeploymentRecord, Directory, Lifecycle, MemberId, MemberPool, MemoryDirectory};
use rtdb_multi::admin;
use rtdb_multi::deployment::CellStorage;
use rtdb_multi::keys::LocalKeyService;
use tokio_postgres::NoTls;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hosted_install_retries_recover_the_same_authority() {
    let cell = Cell::start(1, None).await;
    let id = uuid::Uuid::new_v4();
    let directory = cell.directory.as_ref();
    let (first, second) = tokio::join!(
        admin::install_deployment(directory, id, "shop", "acme", 3),
        admin::install_deployment(directory, id, "shop", "acme", 3),
    );
    let installed = first.unwrap();
    assert_eq!(second.unwrap(), installed);
    assert_eq!(installed.id, id.to_string());
    assert!(installed.linearizable_reads);
    assert!(admin::install_deployment(directory, uuid::Uuid::new_v4(), "shop", "acme", 3).await.is_err());
    assert!(admin::install_deployment(directory, id, "other", "acme", 3).await.is_err());
    let mut client = tokio_postgres::Config::new();
    client.host("127.0.0.1").port(cell.gateway.port()).user("xtdb").password("cell-test");
    client.dbname(&format!("shop@{}", uuid::Uuid::new_v4().simple()));
    assert!(client.connect(NoTls).await.is_err());
    assert!(cell.holder("shop").await.is_none());
    let authority = id.simple().to_string();
    client.dbname(&format!("shop@{authority}"));
    let (client, connection) = client.connect(NoTls).await.unwrap();
    assert_eq!(connection.parameter("rtdb_installation_id"), Some(authority.as_str()));
    tokio::spawn(connection);
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"]);
    drop(client);
    assert_eq!(admin::install_deployment(directory, id, "shop", "acme", 3).await.unwrap().id, installed.id);
    cell.stop().await;
}

#[tokio::test]
async fn failed_disposal_reuses_the_fence_on_retry() {
    let directory = MemoryDirectory::new();
    let data = tempfile::tempdir().unwrap();
    let storage = CellStorage::Local { root: data.path().join("objects") };
    let keys = LocalKeyService::new(data.path().join("keys")).unwrap();
    let installation = directory.create_deployment(
        DeploymentRecord::new("shop", "acme", vec![MemberId::fresh()], 0),
    ).await.unwrap();

    for _ in 0..2 {
        let error = admin::delete_deployment(directory.as_ref(), &storage, &keys, &installation.id, 4 << 20).await.unwrap_err();
        assert_eq!(error.code, "rtdb.multi/disposal-ensemble");
        let pending = directory.deployment(&installation.id).await.unwrap().unwrap();
        assert_eq!(pending.lifecycle, Lifecycle::Deleting);
        assert_eq!(pending.epoch, installation.epoch + 1);
        assert_eq!(pending.version, installation.version + 1);
        assert!(pending.disposal.is_none());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deployment_is_served_through_the_gateway_and_survives_its_holder_dying() {
    let mut cell = Cell::start(2, None).await;
    let history = uuid::Uuid::new_v4();
    admin::install_deployment(cell.directory.as_ref(), history, "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let basis = values(&client, "SELECT _system_from FROM orders WHERE _id = 1").await.remove(0);
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    drop(client);

    let first = cell.holder("shop").await.expect("the first connection placed the deployment");
    let epoch = cell.directory.deployment_by_name("shop").await.unwrap().unwrap().epoch;
    cell.nodes[first].take().unwrap().kill().await.unwrap();

    let client = connect_eventually(&cell, "shop").await;
    assert_eq!(values(&client, "SHOW rtdb_hosted_source_barrier").await, [history.to_string()]);
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10", "20"], "nothing acknowledged is lost when the holder dies");
    let exact = format!("SELECT total FROM orders FOR SYSTEM_TIME AS OF TIMESTAMP '{basis}'");
    assert_eq!(values(&client, &exact).await, ["10"]);
    client.simple_query("INSERT INTO orders (_id, total) VALUES (3, 30)").await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10", "20", "30"]);
    let record = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    assert!(record.epoch > epoch, "the survivor holds it under a later epoch");
    assert_ne!(cell.holder("shop").await, Some(first));
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_hosted_source_barrier_refuses_a_missing_directory_checkpoint() {
    let cell = Cell::start(1, None).await;
    let history = uuid::Uuid::new_v4();
    let directory = cell.directory.as_ref();
    admin::install_deployment(directory, history, "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    assert_eq!(values(&client, "SHOW rtdb_hosted_source_barrier").await, [history.to_string()]);

    let mut record = directory.deployment(&history.to_string()).await.unwrap().unwrap();
    let expected_version = record.version;
    record.checkpoint = Some(rtdb_directory::Checkpoint { block: 7, boundary: 11, source: 11, system_time: 100 });
    assert!(matches!(directory.replace_deployment(record, expected_version).await.unwrap(), rtdb_directory::Cas::Applied(_)));

    let refusal = client.simple_query("SHOW rtdb_hosted_source_barrier").await.unwrap_err();
    assert!(refusal.as_db_error().unwrap().message().contains("has not recovered the directory's durable checkpoint"));
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
    let unknown = tokio_postgres::connect(&format!("host=127.0.0.1 port={} user=xtdb password=cell-test dbname=nowhere", cell.gateway.port()), NoTls).await;
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



#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_only_session_is_served_by_a_log_fed_replica() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let writer = cell.connect("shop").await;
    writer.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    admin::set_read_width(cell.directory.as_ref(), "shop", 2).await.unwrap();
    let holder = cell.holder("shop").await.unwrap();

    let (reader, connection) = tokio_postgres::Config::new().host("127.0.0.1").port(cell.gateway.port()).user("xtdb").password("cell-test").dbname("shop")
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
    for name in ["a1", "a2", "a3", "a4"] { admin::move_deployment(cell.directory.as_ref(), name, &target, &control()).await.unwrap(); }
    assert_eq!(cell.node(0).agent.held().len(), 4, "every deployment moved to the target");
    let moved = admin::rebalance(cell.directory.as_ref(), 10, &control()).await.unwrap();
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

    for _ in 0..3 { let _ = tokio_postgres::connect(&format!("host=127.0.0.1 port={} user=xtdb password=cell-test dbname=shop", cell.gateway.port()), NoTls).await; }
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

    let child = admin::fork(cell.directory.as_ref(), &cell.storage(), &cell.keys(), "shop", "shop-preview", &control()).await.unwrap();
    assert_eq!((child.lineage.as_str(), child.parent.as_deref()), (parent.lineage.as_str(), Some(parent.id.as_str())));
    let root = cell.data.path().join("objects");
    let own = |prefix: &str| files_under(&root.join(prefix)).into_iter().filter(|path| !path.components().any(|part| part.as_os_str() == rtdb_multi::reference::BASE_MANIFEST)).count();
    assert_eq!(own(&child.storage_prefix()), 0, "a fork copies nothing: it references its parent's objects");
    let preview = cell.connect("shop-preview").await;
    assert_eq!(values(&preview, "SELECT total FROM orders").await, ["10", "20"], "the fork starts from its parent's state");
    preview.simple_query("INSERT INTO orders (_id, total) VALUES (3, 30)").await.unwrap();
    let shop = cell.connect("shop").await;
    assert_eq!(values(&shop, "SELECT total FROM orders").await, ["10", "20"], "the fork's writes stay in the fork");
    shop.simple_query("INSERT INTO orders (_id, total) VALUES (4, 40)").await.unwrap();
    assert_eq!(values(&preview, "SELECT total FROM orders").await, ["10", "20", "30"], "and the parent's in the parent");
    drop((shop, preview));
    let holder = cell.holder("shop-preview").await.unwrap();
    cell.node(holder).agent.park("shop-preview").await.unwrap();
    let written = own(&child.storage_prefix());

    let deletion = admin::delete_deployment(cell.directory.as_ref(), &cell.storage(), &cell.keys(), &parent.id, 4 << 20).await.unwrap();
    assert_eq!(deletion.deployment, parent.id);
    assert!(files_under(&root.join(parent.storage_prefix())).is_empty(), "the parent's objects are erased");
    assert!(own(&child.storage_prefix()) > written, "after the fork's shared objects were copied into it");
    assert!(rtdb_multi::keys::KeyService::key(&cell.keys(), &parent.id).await.is_err(), "its key is withdrawn");
    let tombstone = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    assert_eq!(tombstone.lifecycle, rtdb_directory::Lifecycle::Deleted);
    assert_eq!(tombstone.disposal, Some(deletion.clone()));
    let recovered = admin::delete_deployment(cell.directory.as_ref(), &cell.storage(), &cell.keys(), &parent.id, 4 << 20).await.unwrap();
    assert_eq!(recovered, deletion);
    assert_eq!(cell.directory.deployment(&parent.id).await.unwrap().unwrap().version, tombstone.version);
    assert!(tokio_postgres::connect(&format!("host=127.0.0.1 port={} user=xtdb password=cell-test dbname=shop", cell.gateway.port()), NoTls).await.is_err(), "a deleted deployment is refused");
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
    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(target).lease.id(), &control()).await.unwrap();
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
    let root = cell.data.path().join("objects");
    let objects: Vec<String> = files_under(&root.join(record.storage_prefix())).into_iter().map(|path| path.to_string_lossy().into_owned()).collect();
    assert_eq!(objects.iter().filter(|path| path.contains("/~packs/")).count(), 1, "{objects:#?}");
    assert_eq!(objects.iter().filter(|path| path.contains("/tables/")).count(), 0, "no table file is its own object: {objects:#?}");
    let client = cell.connect("shop").await;
    assert_eq!(values(&client, "SELECT v FROM items").await, ["1"], "a packed block reads back after the park");
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compaction_worker_compacts_a_deployment_its_holder_does_not() {
    let cell = Cell::start_with(vec![MemberPool::General, MemberPool::Compaction], None).await;
    let record = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    for round in 0..6 {
        client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({round}, {round})")).await.unwrap();
        cell.node(0).agent.flush("shop").await.unwrap();
    }
    let root = cell.data.path().join("objects");
    let compacted = || files_under(&root.join(record.storage_prefix())).iter().any(|path| { let path = path.to_string_lossy(); path.contains("/data/l") && !path.contains("/data/l00-") });
    for _ in 0..200 {
        if compacted() { break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(compacted(), "the worker wrote compaction outputs");
    assert_eq!(cell.node(1).agent.subscribed(), ["shop"], "the worker holds a compacting replica");
    assert!(cell.node(1).agent.held().is_empty(), "and holds no deployment");
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["0", "1", "2", "3", "4", "5"]);
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_controller_confirms_a_silent_node_dead_before_its_lease_runs_out() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(client);
    let status = admin::cell_status(cell.directory.as_ref(), 1).await.unwrap();
    assert_eq!((status.active, status.parked, status.recommended_nodes), (1, 0, 1));

    let silent = cell.holder("shop").await.unwrap();
    let silent_id = cell.node(silent).lease.id();
    let options = admin::ControllerOptions { probe_timeout: Duration::from_millis(100), control: control(), ..Default::default() };
    let mut failures = std::collections::BTreeMap::new();
    assert_eq!(admin::confirm_suspicions(cell.directory.as_ref(), &mut failures, &options).await.unwrap(), 0, "a node that answers is not suspected");
    // Simulate a hung agent: its probes go unanswered while its lease is still being renewed.
    let hung = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut record = cell.directory.member(&silent_id).await.unwrap().unwrap();
    for _ in 0..8 {
        let mut next = record.clone();
        next.control_address = Some(hung.local_addr().unwrap().to_string());
        match cell.directory.replace_member(next, record.version).await.unwrap() { rtdb_directory::Cas::Applied(_) => break, rtdb_directory::Cas::Conflict(current) => record = current.unwrap() }
    }
    let mut confirmed = 0;
    for _ in 0..2 { confirmed += admin::confirm_suspicions(cell.directory.as_ref(), &mut failures, &options).await.unwrap(); }
    assert_eq!(confirmed, 1, "two unanswered probes confirm it");
    assert_eq!(cell.directory.member(&silent_id).await.unwrap().unwrap().state, rtdb_directory::MemberState::Dead);
    let client = connect_eventually(&cell, "shop").await;
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"]);
    assert_ne!(cell.holder("shop").await, Some(silent), "another node took over without waiting out the lease");
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_holder_moves_a_degraded_stream_to_live_logkeepers() {
    let mut cell = Cell::start(2, None).await;
    cell.add_logkeeper().await;
    let record = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    drop(client);

    fn position(cell: &Cell, id: &rtdb_directory::MemberId) -> Option<usize> { cell.logkeepers.iter().position(|logkeeper| logkeeper.lease.id() == *id) }
    let first_dead = position(&cell, &record.ensemble[0]).unwrap();
    let dead_id = record.ensemble[0].clone();
    cell.logkeepers.remove(first_dead).stop().await.unwrap();
    tokio::time::sleep(Duration::from_millis(1700)).await;
    let holder = cell.holder("shop").await.unwrap();
    cell.node(holder).agent.park("shop").await.unwrap();

    let client = cell.connect("shop").await;
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"]);
    let moved = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    assert!(!moved.ensemble.contains(&dead_id), "the dead logkeeper left the ensemble: {:?}", moved.ensemble);
    assert_eq!(moved.ensemble.len(), 3);
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();

    let second = record.ensemble.iter().skip(1).find_map(|id| position(&cell, id)).unwrap();
    cell.logkeepers.remove(second).stop().await.unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (3, 30)").await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10", "20", "30"], "a second original logkeeper's death leaves a quorum of the new ensemble");
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deposed_holder_refuses_a_linearizable_read_instead_of_serving_it_stale() {
    let cell = Cell::start_watching(vec![MemberPool::General; 2], None, Some(Duration::from_secs(3600))).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    admin::set_linearizable_reads(cell.directory.as_ref(), "shop", true).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"], "a linearizable read is served while the holder holds");
    drop(client);

    let zombie_index = cell.holder("shop").await.unwrap();
    let zombie = cell.node(zombie_index);
    let direct = connect(&zombie.pgwire_address.to_string(), "shop").await;
    let zombie_id = zombie.lease.id();
    zombie.stop_renewing();
    while cell.directory.member(&zombie_id).await.unwrap().unwrap().lease_expires_at_ms > rtdb_directory::now_ms() { tokio::time::sleep(Duration::from_millis(50)).await; }
    let client = connect_eventually(&cell, "shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    assert!(direct.simple_query("SELECT total FROM orders").await.is_err(), "the deposed holder does not answer from its stale state");
    drop((client, direct));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn a_deployment_moves_cells_with_its_unflushed_tail_and_its_old_gateway_forwards() {
    let map: std::sync::Arc<dyn rtdb_multi::cells::CellMap> = rtdb_multi::cells::MemoryCellMap::new();
    let east = Cell::start_in("east", map.clone(), 1).await;
    let west = Cell::start_in("west", map.clone(), 1).await;
    let (east_storage, west_storage, east_keys, west_keys) = (east.storage(), west.storage(), east.keys(), west.keys());
    fn handle<'a>(name: &'a str, cell: &'a Cell, storage: &'a rtdb_multi::deployment::CellStorage, keys: &'a rtdb_multi::keys::LocalKeyService, maximum_deployments: usize) -> admin::CellHandle<'a> {
        admin::CellHandle { name, directory: cell.directory.as_ref(), storage, keys, maximum_deployments }
    }
    let (placed, _) = admin::create_in_cells(map.as_ref(), &[handle("east", &east, &east_storage, &east_keys, 10), handle("west", &west, &west_storage, &west_keys, 0)], "shop", "acme", 3).await.unwrap();
    assert_eq!(placed, "east", "a deployment goes to a cell with room");
    let client = east.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
    drop(client);

    let arrived = admin::move_cell(map.as_ref(), &handle("east", &east, &east_storage, &east_keys, 10), &handle("west", &west, &west_storage, &west_keys, 10), "shop", 4 << 20).await.unwrap();
    assert_eq!(map.cell_of("shop").await.unwrap().as_deref(), Some("west"));
    let through_east = connect_eventually(&east, "shop").await;
    assert_eq!(values(&through_east, "SELECT total FROM orders").await, ["10", "20"], "the unflushed tail moved with it, and the old cell's gateway forwards");
    through_east.simple_query("INSERT INTO orders (_id, total) VALUES (3, 30)").await.unwrap();
    let through_west = west.connect("shop").await;
    assert_eq!(values(&through_west, "SELECT total FROM orders").await, ["10", "20", "30"]);
    let settled = west.directory.deployment(&arrived.id).await.unwrap().unwrap();
    assert!(settled.move_from.is_none(), "the first holder in the new cell moved the stream onto the new cell's logkeepers");
    assert_eq!(east.directory.deployment_by_name("shop").await.unwrap().unwrap().lifecycle, rtdb_directory::Lifecycle::Deleted);

    let tombstone = east.directory.deployment_by_name("shop").await.unwrap().unwrap();
    let east_bucket = east.storage().bucket(rtdb_storage::IoRegion::new()).await.unwrap();
    let left_behind = || async { east_bucket.list(Some(&rtdb_storage::ObjectKey::new(tombstone.storage_prefix()).unwrap())).await.unwrap().len() };
    assert!(left_behind().await > 0, "the old cell still holds the deployment's objects right after the move");
    let deployments = east.directory.deployments().await.unwrap();
    let keys = east.keys();
    let hour = Duration::from_secs(3600);
    assert_eq!(rtdb_multi::store::finish_moves(east_bucket.as_ref(), &deployments, rtdb_directory::now_ms(), hour, Some(&keys)).await.unwrap(), 0, "and keeps them within the grace");
    assert_eq!(rtdb_multi::store::finish_moves(east_bucket.as_ref(), &deployments, rtdb_directory::now_ms() + hour.as_millis() as i64, hour, Some(&keys)).await.unwrap(), 1);
    assert_eq!(left_behind().await, 0, "after the grace, the old cell erases what the move left");
    assert!(!east.data.path().join("keys").join(format!("{}.key", tombstone.id)).exists(), "and destroys its copy of the key");
    assert_eq!(values(&through_west, "SELECT total FROM orders").await, ["10", "20", "30"], "while the deployment reads on in its new cell");
    drop((through_east, through_west));
    east.stop().await;
    west.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_over_its_tail_budget_flushes_its_largest_tail_first() {
    let cell = Cell::start(1, None).await;
    for name in ["big", "small"] { admin::create_deployment(cell.directory.as_ref(), name, "acme", 3).await.unwrap(); }
    let big = cell.connect("big").await;
    for id in 0..40 { big.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({id}, {id})")).await.unwrap(); }
    let small = cell.connect("small").await;
    small.simple_query("INSERT INTO orders (_id, total) VALUES (1, 1)").await.unwrap();
    let node = cell.node(0);
    let rows = |name: &str| node.server.node().catalog().database(name).unwrap().live_rows();
    let small_tail = rows("small");
    assert!(rows("big") > small_tail && small_tail > 0);
    node.agent.bound_tails_for_test(small_tail + 1).await;
    assert_eq!((rows("big"), rows("small")), (0, small_tail), "the largest tail is flushed, and only as much as needed to fit");
    assert_eq!(values(&big, "SELECT COUNT(*) FROM orders").await, ["40"]);
    drop((big, small));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stream_is_trimmed_below_its_last_published_block_and_still_recovers() {
    let mut cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    for round in 0..3 {
        for id in 0..5 { client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({}, 1)", round * 10 + id)).await.unwrap(); }
        let holder = cell.holder("shop").await.unwrap();
        cell.node(holder).agent.flush("shop").await.unwrap();
    }
    client.simple_query("INSERT INTO orders (_id, total) VALUES (99, 1)").await.unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    let record = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    let members: std::collections::BTreeMap<_, _> = cell.directory.members().await.unwrap().into_iter().map(|member| (member.id.clone(), member)).collect();
    let servers: Vec<String> = record.ensemble.iter().map(|id| members[id].address.clone()).collect();
    let reader = rtdb_logkeeper::open_reader(rtdb_logkeeper::StreamConfig { write_quorum: 2, servers, stream: record.stream("replica"), maximum_message_bytes: 4 << 20 }, Default::default()).await.unwrap();
    use rtdb_storage::log::Log;
    assert!(matches!(reader.read_range(rtdb_storage::log::LogOffset(0), rtdb_storage::log::LogOffset(1)).await, Err(rtdb_storage::Error::UnavailableLogRange { .. })), "the stream is trimmed below the published blocks");
    assert_eq!(record.checkpoint.map(|checkpoint| checkpoint.block), Some(2), "the directory names the last published block, which a cold start starts from");
    drop(client);

    let holder = cell.holder("shop").await.unwrap();
    cell.nodes[holder].take().unwrap().kill().await.unwrap();
    let client = connect_eventually(&cell, "shop").await;
    assert_eq!(values(&client, "SELECT COUNT(*) FROM orders").await, ["16"], "a new holder recovers from the block and the trimmed tail");
    client.simple_query("INSERT INTO orders (_id, total) VALUES (100, 1)").await.unwrap();
    let report = admin::audit(cell.directory.as_ref(), &cell.keys(), "shop", 4 << 20).await.unwrap();
    assert!(report.violations.is_empty(), "{:?}", report.violations);
    drop(client);
    cell.add_node().await;
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_command_that_parses_a_statement_is_resent_whole_after_a_move() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let first = cell.holder("shop").await.unwrap();
    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(1 - first).lease.id(), &control()).await.unwrap();
    let statement = client.prepare("SELECT total FROM orders WHERE _id = 1").await.expect("a named statement parsed as the first command after a move");
    assert_eq!(client.query_one(&statement, &[]).await.unwrap().get::<_, i64>(0), 10);
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_transaction_retried_with_its_idempotency_key_commits_once() {
    let mut cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let keyed = |key: &str, id: i64| format!("BEGIN READ WRITE WITH (METADATA = {{idempotency_key: '{key}'}}); INSERT INTO orders (_id, total) VALUES ({id}, 1); COMMIT");
    let client = cell.connect("shop").await;
    client.batch_execute(&keyed("order-1", 1)).await.unwrap();
    client.batch_execute(&keyed("order-1", 2)).await.expect("a retry with a committed key succeeds");
    assert_eq!(values(&client, "SELECT _id FROM orders").await, ["1"], "and does not run again");
    drop(client);

    let holder = cell.holder("shop").await.unwrap();
    cell.nodes[holder].take().unwrap().kill().await.unwrap();
    let client = connect_eventually(&cell, "shop").await;
    client.batch_execute(&keyed("order-1", 3)).await.expect("the key survives a change of holder");
    client.batch_execute(&keyed("order-2", 4)).await.unwrap();
    assert_eq!(values(&client, "SELECT _id FROM orders").await, ["1", "4"]);
    drop(client);
    cell.add_node().await;
    cell.stop().await;
}

/// The type of the first message a server answers `parameters`' startup with, and its first four body bytes.
async fn greeting(address: &str, parameters: &[(&str, &str)]) -> (u8, [u8; 4]) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut body = 196608i32.to_be_bytes().to_vec();
    for (key, value) in parameters { for field in [key, value] { body.extend_from_slice(field.as_bytes()); body.push(0); } }
    body.push(0);
    let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
    stream.write_all(&((body.len() + 4) as i32).to_be_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
    let tag = stream.read_u8().await.unwrap();
    let _length = stream.read_i32().await.unwrap();
    let mut first = [0; 4];
    stream.read_exact(&mut first).await.unwrap();
    (tag, first)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_admits_a_capability_only_for_its_deployment_and_the_gateway_never_forwards_a_clients() {
    let cell = Cell::start(1, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    admin::create_deployment(cell.directory.as_ref(), "blog", "acme", 3).await.unwrap();
    drop(cell.connect("shop").await);
    let node = cell.node(0).pgwire_address.to_string();
    let genuine = common::secret().mint("shop", "xtdb", false, Duration::from_secs(30));
    let ok = (b'R', [0, 0, 0, 0]);
    assert_eq!(greeting(&node, &[("user", "xtdb"), ("database", "shop"), ("rtdb.capability", &genuine)]).await, ok, "a genuine capability stands in for the password");
    assert_eq!(greeting(&node, &[("user", "xtdb"), ("database", "blog"), ("rtdb.capability", &genuine)]).await.0, b'E', "another deployment's capability is refused");
    let foreign = rtdb_multi::capability::CellSecret::new(&[9; 32]).unwrap().mint("shop", "xtdb", false, Duration::from_secs(30));
    assert_eq!(greeting(&node, &[("user", "xtdb"), ("database", "shop"), ("rtdb.capability", &foreign)]).await.0, b'E', "another cell's capability is refused");
    assert_eq!(greeting(&cell.gateway.to_string(), &[("user", "xtdb"), ("database", "shop"), ("rtdb.capability", &genuine)]).await, (b'R', [0, 0, 0, 3]), "through the gateway, a client's capability is dropped and the password asked for");
    let control = cell.node(0).control_address.to_string();
    let ping = |key: rtdb_multi::control::ControlKey| { let control = control.clone(); async move { rtdb_multi::control::call(&control, &rtdb_multi::control::Request::Ping, Duration::from_secs(5), &key).await.unwrap() } };
    assert!(matches!(ping(common::control()).await, rtdb_multi::control::Response::Done), "a request signed with the cell's secret is served");
    assert!(matches!(ping(rtdb_multi::control::ControlKey::default()).await, rtdb_multi::control::Response::Refused { .. }), "an unsigned one is refused");
    assert!(matches!(ping(rtdb_multi::control::ControlKey(Some(std::sync::Arc::new(rtdb_multi::capability::CellSecret::new(&[9; 32]).unwrap())))).await, rtdb_multi::control::Response::Refused { .. }), "and so is one signed by another cell");
    for grant in [genuine.clone(), "forged".to_owned(), rtdb_multi::capability::CellSecret::new(&[9; 32]).unwrap().node_grant("shop")] {
        let answer = rtdb_multi::control::call(&control, &rtdb_multi::control::Request::PublishCompaction { name: "shop".into(), notification: Vec::new(), grant }, Duration::from_secs(5), &common::control()).await.unwrap();
        assert!(matches!(answer, rtdb_multi::control::Response::Refused { .. }), "only another of the cell's nodes may ask for a deployment's data or publish for it: {answer:?}");
    }
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_session_reads_the_checkpoint_from_object_storage_on_another_node() {
    let mut cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    for id in 0..5 { client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({id}, 1)")).await.unwrap(); }
    let holder = cell.holder("shop").await.unwrap();
    cell.node(holder).agent.flush("shop").await.unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (99, 1)").await.unwrap();
    for _ in 0..50 {
        if cell.directory.deployment_by_name("shop").await.unwrap().unwrap().checkpoint.is_some() { break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for logkeeper in cell.logkeepers.drain(..) { logkeeper.stop().await.unwrap(); }

    let (snapshot, connection) = tokio_postgres::Config::new().host("127.0.0.1").port(cell.gateway.port()).user("xtdb").password("cell-test").dbname("shop")
        .options("-c rtdb.read=snapshot").connect(NoTls).await.unwrap();
    tokio::spawn(connection);
    assert_eq!(values(&snapshot, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4"], "a snapshot reads the deployment as of its checkpoint, with no logkeeper up");
    let second = values(&snapshot, "SELECT _system_from FROM orders WHERE _id = 1").await.remove(0);
    assert_eq!(values(&snapshot, &format!("SELECT _id FROM orders FOR SYSTEM_TIME AS OF TIMESTAMP '{second}'")).await, ["0", "1"], "and at any earlier basis");
    assert!(cell.node(1 - holder).agent.subscribed().contains(&"shop".to_owned()), "on a node that does not hold it");
    assert!(snapshot.simple_query("INSERT INTO orders (_id, total) VALUES (7, 1)").await.is_err(), "and it never writes");
    let reader = cell.node(1 - holder);
    assert!(matches!(reader.agent.subscribe("shop").await, Err(_) | Ok(rtdb_multi::control::Response::Subscribed { .. })));
    assert!(!reader.agent.subscribed_snapshots().contains(&"shop".to_owned()), "asked for a replica, a node replaces its snapshot rather than serve it as one");
    drop((snapshot, client));
    cell.gateway_cancellation.cancel();
    for node in cell.nodes.iter_mut().filter_map(Option::take) { node.kill().await.unwrap(); }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn updates_sent_back_to_back_across_a_holders_death_are_neither_lost_nor_doubled() {
    let mut cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO counters (_id, n) VALUES (1, 0)").await.unwrap();
    let holder = cell.holder("shop").await.unwrap();
    let doomed = cell.nodes[holder].take().unwrap();
    let killer = tokio::spawn(async move { tokio::time::sleep(Duration::from_millis(300)).await; doomed.kill().await.unwrap(); });
    // Commands run back to back from before the holder dies until well after, so it dies while one is in flight.
    let mut commands = 0;
    let mut after = 0;
    while after < 20 {
        client.simple_query("UPDATE counters SET n = n + 1 WHERE _id = 1").await.unwrap_or_else(|error| panic!("command {commands} failed across the holder's death: {error:?}"));
        commands += 1;
        if killer.is_finished() { after += 1; }
    }
    killer.await.unwrap();
    assert_eq!(values(&client, "SELECT n FROM counters").await, [commands.to_string()], "every command ran exactly once");
    drop(client);
    cell.add_node().await;
    cell.stop().await;
}

/// Every column of every row `sql` returns, sorted.
async fn rows(client: &tokio_postgres::Client, sql: &str) -> Vec<String> {
    let mut rows: Vec<String> = client.simple_query(sql).await.unwrap().into_iter().filter_map(|message| match message {
        tokio_postgres::SimpleQueryMessage::Row(row) => Some((0..row.len()).map(|column| row.get(column).unwrap_or("NULL").to_owned()).collect::<Vec<_>>().join("|")),
        _ => None,
    }).collect();
    rows.sort();
    rows
}

const FRAGMENT_QUERIES: [&str; 5] = [
    "SELECT * FROM orders",
    "SELECT _id, total FROM orders WHERE total > 2500",
    "SELECT count(*) AS n, sum(total) AS total, max(note) AS note FROM orders",
    "SELECT i._id, i.qty, o.note FROM items AS i JOIN orders AS o ON i.order_id = o._id",
    "SELECT _id, _system_from, _system_to, note FROM orders FOR ALL SYSTEM_TIME WHERE _id < 400",
];

/// A deployment with three tables spanning many trie leaves, flushed to a block and then written past it.
async fn large_deployment(cell: &Cell, name: &str) -> tokio_postgres::Client {
    admin::create_deployment(cell.directory.as_ref(), name, "acme", 3).await.unwrap();
    let client = cell.connect(name).await;
    for chunk in 0..150 {
        let rows: Vec<String> = (chunk * 20..(chunk + 1) * 20).map(|id| format!("({id}, {id}, 'order {id}')")).collect();
        client.simple_query(&format!("INSERT INTO orders (_id, total, note) VALUES {}", rows.join(", "))).await.unwrap();
    }
    for chunk in 0..75 {
        let items: Vec<String> = (chunk * 20..(chunk + 1) * 20).map(|id| format!("('item-{id}', {}, {})", id * 2, id % 9)).collect();
        client.simple_query(&format!("INSERT INTO items (_id, order_id, qty) VALUES {}", items.join(", "))).await.unwrap();
    }
    client.simple_query("INSERT INTO users (_id, name) VALUES (1, 'ada'), (2, 'grace')").await.unwrap();
    client.simple_query("UPDATE orders SET total = total + 0.5 WHERE _id % 7 = 0").await.unwrap();
    let holder = cell.holder(name).await.unwrap();
    cell.node(holder).agent.flush(name).await.unwrap();
    client.simple_query("UPDATE orders SET note = 'tail' WHERE _id BETWEEN 100 AND 300").await.unwrap();
    client.simple_query("DELETE FROM orders WHERE _id BETWEEN 2000 AND 2100").await.unwrap();
    client.simple_query("INSERT INTO orders (_id, total, note) VALUES (5000, 1, 'late'), (5001, 2, 'late')").await.unwrap();
    client
}

async fn fragment_answers(client: &tokio_postgres::Client) -> Vec<Vec<String>> {
    let mut answers = Vec::new();
    for sql in FRAGMENT_QUERIES { answers.push(rows(client, sql).await); }
    answers
}

fn split_above(cell: &Cell, bytes: u64) { for node in cell.nodes.iter().flatten() { node.fragmenter.split_above(bytes); } }

/// The fragments each node read for another node's scans, by node index.
fn fragments_read(cell: &Cell) -> Vec<std::collections::BTreeMap<(String, String, String), u64>> {
    cell.nodes.iter().map(|node| node.as_ref().map(|node| node.agent.fragments_read()).unwrap_or_default()).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_deployments_scans_read_as_fragments_on_the_nodes_their_slices_rank_first_on() {
    let cell = Cell::start(3, None).await;
    let client = large_deployment(&cell, "big").await;
    let whole = fragment_answers(&client).await;
    assert!(whole[0].len() > 2900 && !whole[3].is_empty() && whole[4].len() > 400);
    assert!(fragments_read(&cell).iter().all(|read| read.is_empty()), "a deployment within one node's cache is scanned whole");

    split_above(&cell, 0);
    let holder = cell.holder("big").await.unwrap();
    let warming = fragment_answers(&client).await;
    for ((sql, whole), warming) in FRAGMENT_QUERIES.iter().zip(&whole).zip(&warming) { assert_eq!(warming, whole, "{sql}"); }
    // A reader without the deployment open refuses and opens a replica on its next pass.
    tokio::time::sleep(Duration::from_millis(1000)).await;
    let refused = cell.node(holder).fragmenter.failures();
    let before: Vec<u64> = fragments_read(&cell).iter().map(|read| read.values().sum()).collect();
    let fragmented = fragment_answers(&client).await;
    for ((sql, whole), fragmented) in FRAGMENT_QUERIES.iter().zip(&whole).zip(&fragmented) { assert_eq!(fragmented, whole, "{sql}"); }
    let first = fragments_read(&cell);
    assert!(first[holder].is_empty(), "the coordinating holder reads its own fragments without sending them to itself");
    assert_eq!(first.iter().zip(&before).filter(|(read, before)| read.values().sum::<u64>() > **before).count(), 2, "both other nodes read the slices that rank them first: {first:?}");
    assert_eq!(cell.node(holder).fragmenter.failures(), refused, "once warm, every fragment sent is read, at or past the holder's basis");

    let record = cell.directory.deployment_by_name("big").await.unwrap().unwrap();
    let members = cell.directory.members().await.unwrap();
    let readers = rtdb_multi::fragments::readers(&members);
    for (index, read) in first.iter().enumerate() {
        for (_, table, prefix) in read.keys() {
            let (schema, table) = table.split_once('.').unwrap();
            let prefix = rtdb_query::TriePrefix::new(prefix.bytes().map(|digit| digit - b'0').collect()).unwrap();
            let key = rtdb_multi::fragments::fragment_key(&record.storage_identity(), &rtdb_types::TableRef::new(schema, table), &prefix);
            let ranked = rtdb_affinity::rank(&key, &readers, |member| member.id.as_str());
            assert_eq!(ranked[0].id, cell.node(index).lease.id(), "{key} is read on the node it ranks first on");
        }
    }

    let again = fragment_answers(&client).await;
    assert_eq!(again, whole);
    let second = fragments_read(&cell);
    for (first, second) in first.iter().zip(&second) {
        assert_eq!(first.keys().collect::<Vec<_>>(), second.keys().collect::<Vec<_>>(), "each slice is read on the same node again");
        assert!(first.iter().all(|(key, count)| second[key] > *count));
    }
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fragment_whose_reader_does_not_answer_is_read_by_the_coordinator() {
    let cell = Cell::start(2, None).await;
    let client = large_deployment(&cell, "big").await;
    let whole = fragment_answers(&client).await;

    // A reader the directory still lists, whose agent no longer listens, ranks first for some of the orders table's slices.
    let closed = { let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap(); listener.local_addr().unwrap().to_string() };
    let record = cell.directory.deployment_by_name("big").await.unwrap().unwrap();
    let members = cell.directory.members().await.unwrap();
    let template = members.iter().find(|member| member.role == rtdb_directory::MemberRole::Node).unwrap().clone();
    let orders = rtdb_types::TableRef::new("public", "orders");
    let ranks_first = |fake: &rtdb_directory::MemberRecord| {
        let mut all = members.clone();
        all.push(fake.clone());
        let readers = rtdb_multi::fragments::readers(&all);
        rtdb_query::FragmentDepth::new(2).unwrap().prefixes().iter().any(|prefix| {
            rtdb_affinity::rank(&rtdb_multi::fragments::fragment_key(&record.storage_identity(), &orders, prefix), &readers, |member| member.id.as_str())[0].id == fake.id
        })
    };
    let fake = (0..100).map(|index| rtdb_directory::MemberRecord { id: rtdb_directory::MemberId(format!("vanished-{index}")), control_address: Some(closed.clone()),
        lease_expires_at_ms: rtdb_directory::now_ms() + 3_600_000, ..template.clone() }).find(|fake| ranks_first(fake)).unwrap();
    cell.directory.register_member(fake).await.unwrap();

    split_above(&cell, 0);
    let fragmented = fragment_answers(&client).await;
    for ((sql, whole), fragmented) in FRAGMENT_QUERIES.iter().zip(&whole).zip(&fragmented) { assert_eq!(fragmented, whole, "{sql}"); }
    let holder = cell.holder("big").await.unwrap();
    assert!(cell.node(holder).fragmenter.failures() > 0, "the vanished reader's fragments failed and were read by the holder");
    drop(client);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fork_at_a_past_block_holds_that_blocks_state_and_diverges_from_it() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    for id in 0..3 { client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({id}, 1)")).await.unwrap(); }
    let holder = cell.holder("shop").await.unwrap();
    cell.node(holder).agent.flush("shop").await.unwrap();
    for id in 3..6 { client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({id}, 1)")).await.unwrap(); }
    cell.node(holder).agent.flush("shop").await.unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (99, 1)").await.unwrap();

    assert!(admin::fork_at_block(cell.directory.as_ref(), &cell.storage(), &cell.keys(), "shop", "too-old", 0, Duration::ZERO).await.is_err(), "a block older than the reaper's grace is refused");
    admin::fork_at_block(cell.directory.as_ref(), &cell.storage(), &cell.keys(), "shop", "shop-then", 0, Duration::from_secs(3600)).await.unwrap();
    let then = cell.connect("shop-then").await;
    assert_eq!(values(&then, "SELECT _id FROM orders").await, ["0", "1", "2"], "the fork holds the first block's state, and none of what came after");
    then.simple_query("INSERT INTO orders (_id, total) VALUES (50, 1)").await.unwrap();
    let fork_holder = cell.holder("shop-then").await.unwrap();
    cell.node(fork_holder).agent.flush("shop-then").await.unwrap();
    then.simple_query("INSERT INTO orders (_id, total) VALUES (51, 1)").await.unwrap();
    assert_eq!(values(&then, "SELECT _id FROM orders").await, ["0", "1", "2", "50", "51"], "and writes and flushes its own blocks");
    assert_eq!(values(&client, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4", "5", "99"], "while the parent is untouched");
    drop((then, client));
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fork_at_a_time_after_the_checkpoint_holds_the_tail_up_to_it() {
    let cell = Cell::start(2, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    for id in 0..3 { client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({id}, 1)")).await.unwrap(); }
    let holder = cell.holder("shop").await.unwrap();
    cell.node(holder).agent.flush("shop").await.unwrap();
    for _ in 0..50 {
        if cell.directory.deployment_by_name("shop").await.unwrap().unwrap().checkpoint.is_some() { break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for id in 3..5 { client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({id}, 1)")).await.unwrap(); }
    let written = values(&client, "SELECT _system_from FROM orders WHERE _id = 4").await.remove(0);
    let then = rtdb_types::Timestamp(chrono::DateTime::parse_from_str(&written, "%Y-%m-%d %H:%M:%S%.f%:z").unwrap().timestamp_micros());
    for id in 5..7 { client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({id}, 1)")).await.unwrap(); }

    assert!(admin::fork_at_time(cell.directory.as_ref(), &cell.storage(), &cell.keys(), "shop", "too-early", rtdb_types::Timestamp(0), Duration::from_secs(3600), 4 << 20).await.is_err(),
        "a time before the checkpoint is a past block's");
    admin::fork_at_time(cell.directory.as_ref(), &cell.storage(), &cell.keys(), "shop", "shop-then", then, Duration::from_secs(3600), 4 << 20).await.unwrap();
    let fork = connect_eventually(&cell, "shop-then").await;
    assert_eq!(values(&fork, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4"], "the fork holds the block and the tail up to the time, and nothing after");
    fork.simple_query("INSERT INTO orders (_id, total) VALUES (50, 1)").await.unwrap();
    assert_eq!(values(&fork, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4", "50"], "and takes writes of its own");
    assert_eq!(values(&client, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4", "5", "6"], "while the parent is untouched");
    drop((fork, client));
    let mut cell = cell;
    let fork_holder = cell.holder("shop-then").await.unwrap();
    cell.nodes[fork_holder].take().unwrap().kill().await.unwrap();
    cell.add_node().await;
    let fork = connect_eventually(&cell, "shop-then").await;
    assert_eq!(values(&fork, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4", "50"], "the copied tail is the fork's history, recovered by its next holder");
    drop(fork);
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fork_publishes_and_trims_only_after_adopting_a_block_from_its_own_log() {
    use rtdb_storage::log::{Log, LogOffset};

    let cell = Cell::start_watching(vec![rtdb_directory::MemberPool::General], None, Some(Duration::from_secs(60))).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let parent = cell.connect("shop").await;
    for id in 0..8 {
        parent.simple_query(&format!("INSERT INTO orders (_id) VALUES ({id})")).await.unwrap();
    }
    let child = admin::fork(cell.directory.as_ref(), &cell.storage(), &cell.keys(), "shop", "shop-preview", &control()).await.unwrap();
    let preview = cell.connect("shop-preview").await;
    preview.simple_query("INSERT INTO orders (_id) VALUES (8)").await.unwrap();
    let holder = cell.holder("shop-preview").await.unwrap();
    let agent = &cell.node(holder).agent;
    agent.reconcile().await;
    assert!(cell.directory.deployment(&child.id).await.unwrap().unwrap().checkpoint.is_none());
    let members: std::collections::BTreeMap<_, _> = cell.directory.members().await.unwrap().into_iter().map(|member| (member.id.clone(), member)).collect();
    let servers = child.ensemble.iter().map(|id| members[id].address.clone()).collect();
    let reader = rtdb_logkeeper::open_reader(rtdb_logkeeper::StreamConfig {
        write_quorum: 2, servers, stream: child.stream("replica"), maximum_message_bytes: 4 << 20,
    }, Default::default()).await.unwrap();
    assert_eq!(reader.read_range(LogOffset(0), LogOffset(1)).await.unwrap().len(), 1);

    agent.flush("shop-preview").await.unwrap();
    agent.reconcile().await;
    let checkpoint = cell.directory.deployment(&child.id).await.unwrap().unwrap().checkpoint.unwrap();
    assert!(checkpoint.boundary > 0 && checkpoint.source >= 0);
    assert!(matches!(reader.read_range(LogOffset(0), LogOffset(1)).await, Err(rtdb_storage::Error::UnavailableLogRange { .. })));
    assert_eq!(reader.read_range(LogOffset(checkpoint.boundary), LogOffset(checkpoint.boundary + 1)).await.unwrap().len(), 1);
    assert_eq!(values(&preview, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4", "5", "6", "7", "8"]);
    drop((parent, preview));
    cell.stop().await;
}
