//! A whole cell in one process: a directory, three logkeepers, nodes and a gateway, driven through a real Postgres client.

mod common;

use std::time::Duration;

use common::*;
use rtdb_directory::MemberPool;
use rtdb_multi::admin;
use tokio_postgres::NoTls;

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_compaction_worker_compacts_a_deployment_its_holder_does_not() {
    let cell = Cell::start_with(vec![MemberPool::General, MemberPool::Compaction], None).await;
    let record = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    for round in 0..6 {
        client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({round}, {round})")).await.unwrap();
        cell.node(0).agent.flush("shop").await.unwrap();
    }
    let rtdb_multi::deployment::CellStorage::Local { root } = cell.storage();
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
    let options = admin::ControllerOptions { probe_timeout: Duration::from_millis(100), ..Default::default() };
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
    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(1 - first).lease.id()).await.unwrap();
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
