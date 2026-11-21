//! A cell on S3: run against an S3-compatible server named by `RTDB_S3_ENDPOINT` (MinIO, say), with a bucket named by `RTDB_S3_BUCKET`, and credentials in the usual AWS environment variables.

mod common;

use std::time::Duration;

use common::*;
use rtdb_directory::MemberPool;
use rtdb_multi::{admin, deployment::CellStorage};

fn storage() -> Option<CellStorage> {
    let endpoint = std::env::var("RTDB_S3_ENDPOINT").ok()?;
    let bucket = std::env::var("RTDB_S3_BUCKET").unwrap_or_else(|_| "rtdb-test".into());
    Some(CellStorage::parse(&format!("s3://{bucket}/run-{}?endpoint={endpoint}&path_style=true&region=us-east-1", uuid::Uuid::new_v4().simple())).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cell_keeps_its_deployments_forks_and_deletions_in_s3() {
    let Some(storage) = storage() else { eprintln!("RTDB_S3_ENDPOINT is not set: skipped"); return };
    tokio::time::timeout(Duration::from_secs(60), exercise_cell(storage))
        .await.expect("S3 cell lifecycle stalled");
}

async fn exercise_cell(storage: CellStorage) {
    eprintln!("S3 phase: start cell and write");
    let cell = Cell::start_on(Some(storage.clone()), vec![MemberPool::General; 2], None, None, None).await;
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    for id in 0..5 { client.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({id}, {id})")).await.unwrap(); }
    let holder = cell.holder("shop").await.unwrap();
    eprintln!("S3 phase: flush parent");
    cell.node(holder).agent.flush("shop").await.unwrap();
    client.simple_query("INSERT INTO orders (_id, total) VALUES (5, 5)").await.unwrap();
    drop(client);
    eprintln!("S3 phase: park parent");
    cell.node(holder).agent.park("shop").await.unwrap();

    eprintln!("S3 phase: cold-start parent");
    let client = connect_eventually(&cell, "shop").await;
    assert_eq!(values(&client, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4", "5"], "a cold start reads the deployment back from S3");
    drop(client);

    let bucket = storage.bucket(rtdb_storage::IoRegion::new()).await.unwrap();
    let parent = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    eprintln!("S3 phase: fork parent");
    let child = admin::fork(cell.directory.as_ref(), &cell.storage(), &cell.keys(), "shop", "shop-preview", &control()).await.unwrap();
    let copied = rtdb_multi::reference::stores(bucket.as_ref(), &child.storage_prefix()).await.unwrap();
    for store in &copied {
        assert!(rtdb_multi::reference::physical_objects(bucket.as_ref(), store).await.unwrap().is_empty(), "a fork on S3 references its parent's objects rather than copying them");
    }
    eprintln!("S3 phase: read and write fork");
    let preview = connect_eventually(&cell, "shop-preview").await;
    assert_eq!(values(&preview, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4", "5"]);
    preview.simple_query("INSERT INTO orders (_id, total) VALUES (50, 50)").await.unwrap();
    drop(preview);
    let holder = cell.holder("shop-preview").await.unwrap();
    eprintln!("S3 phase: park fork");
    cell.node(holder).agent.park("shop-preview").await.unwrap();

    eprintln!("S3 phase: delete parent");
    let deletion = admin::delete_deployment(cell.directory.as_ref(), &cell.storage(), &cell.keys(), "shop", 4 << 20).await.unwrap();
    assert!(deletion.facts.verified_empty, "the parent's current objects are gone from S3");
    assert_eq!(deletion.facts.inventory, rtdb_directory::DeletionInventory::AllVersionsAndMultipartUploads);
    let prefix = rtdb_storage::ObjectKey::new(parent.storage_prefix()).unwrap();
    assert!(bucket.list(Some(&prefix)).await.unwrap().is_empty());
    let preview = connect_eventually(&cell, "shop-preview").await;
    assert_eq!(values(&preview, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4", "5", "50"], "the fork outlives its parent, its shared objects copied into it first");
    drop(preview);

    eprintln!("S3 phase: reap");
    let deployments = cell.directory.deployments().await.unwrap();
    let references = rtdb_multi::reference::References::scan(bucket.as_ref(), &deployments).await.unwrap();
    for record in deployments.iter().filter(|record| record.lifecycle != rtdb_directory::Lifecycle::Deleted) {
        rtdb_multi::store::reap(bucket.as_ref(), record, Duration::from_secs(3600), &references).await.unwrap();
    }
    eprintln!("S3 phase: stop cell");
    cell.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cell_flushes_and_moves_with_deployment_scoped_s3_writes() {
    use std::sync::atomic::Ordering;

    let Ok(endpoint) = std::env::var("RTDB_S3_ENDPOINT") else { return };
    if !endpoint.starts_with("http://127.0.0.1:") { return; }
    let CellStorage::S3 { mut config } = storage().unwrap() else { unreachable!() };
    let (address, writes, proxy) = pause_pack_upload(endpoint.parse().unwrap()).await;
    config.endpoint = Some(format!("http://{address}"));
    let cell = Cell::start_scoped(CellStorage::S3 { config }, rtdb_multi::agent::S3WriteRole {
        arn: "arn:aws:iam::123456789012:role/rtdb-cell-writer".into(), sts_endpoint: Some(endpoint),
        require_conditional_put: false,
    }).await;
    writes.observe_writes.store(true, Ordering::Relaxed);
    admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
    let client = cell.connect("shop").await;
    client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
    let holder = cell.holder("shop").await.unwrap();
    cell.node(holder).agent.flush("shop").await.unwrap();
    let successor = 1 - holder;
    admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(successor).lease.id(), &control()).await.unwrap();
    client.simple_query("UPDATE orders SET total = total + 5 WHERE _id = 1").await.unwrap();
    cell.node(successor).agent.flush("shop").await.unwrap();
    assert_eq!(values(&client, "SELECT total FROM orders").await, ["15"]);
    assert!(writes.assumed_puts.load(Ordering::Relaxed) >= 3);
    assert_eq!(writes.other_puts.load(Ordering::Relaxed), 0);
    assert_eq!(writes.unconditional_puts.load(Ordering::Relaxed), 0);
    drop(client);

    let deployed = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    assert!(deployed.active_s3_write_lease(rtdb_directory::now_ms()).is_some());
    let keys = cell.keys();
    let deletion = admin::delete_deployment(cell.directory.as_ref(), &cell.storage(), &keys, "shop", 4 << 20).await.unwrap_err();
    assert_eq!(deletion.code, "rtdb.multi/storage-lease-active");
    assert_eq!(cell.directory.deployment(&deployed.id).await.unwrap().unwrap().lifecycle, rtdb_directory::Lifecycle::Deleting);
    assert!(rtdb_multi::keys::KeyService::existing(&keys, &deployed.id).is_ok());
    cell.stop().await;
    proxy.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cell_move_refuses_overlapping_s3_locations() {
    use rtdb_multi::cells::MemoryCellMap;

    let Some(storage) = storage() else { eprintln!("RTDB_S3_ENDPOINT is not set: skipped"); return };
    let map = MemoryCellMap::new();
    let east = Cell::start_on(Some(storage.clone()), vec![], None, None, Some((map.clone(), "east".into()))).await;
    let west = Cell::start_in("west", map.clone(), 0).await;
    let (east_keys, west_keys) = (east.keys(), west.keys());
    let from = admin::CellHandle { name: "east", directory: east.directory.as_ref(), storage: &storage, keys: &east_keys, maximum_deployments: 10 };
    let deployment = admin::create_in_cells(map.as_ref(), std::slice::from_ref(&from), "shop", "acme", 3).await.unwrap().1;
    let CellStorage::S3 { mut config } = storage.clone() else { unreachable!() };
    let pack_prefix = format!("{}/v06/~packs", deployment.storage_prefix());
    config.prefix = Some(format!("{}/cell/{pack_prefix}", config.prefix.as_deref().unwrap()));
    let bucket = storage.bucket(rtdb_storage::IoRegion::new()).await.unwrap();
    let packs = rtdb_storage::ObjectKey::new(pack_prefix).unwrap();
    let nested = CellStorage::S3 { config };
    for destination in [&storage, &nested] {
        let to = admin::CellHandle { name: "west", directory: west.directory.as_ref(), storage: destination, keys: &west_keys, maximum_deployments: 10 };
        let error = admin::move_cell(map.as_ref(), &from, &to, "shop", 4 << 20).await.unwrap_err();
        assert_eq!(error.code, "rtdb.multi/shared-cell-storage");
        assert_eq!(east.directory.deployment(&deployment.id).await.unwrap().as_ref(), Some(&deployment));
    }
    assert!(bucket.list(Some(&packs)).await.unwrap().is_empty());
    east.stop().await;
    west.stop().await;
}

#[derive(Default)]
struct PackUpload {
    armed: std::sync::atomic::AtomicBool,
    held: tokio::sync::Notify,
    resume: tokio::sync::Notify,
    completed: tokio::sync::Notify,
    reply_status: std::sync::atomic::AtomicU16,
    observe_writes: std::sync::atomic::AtomicBool,
    assumed_puts: std::sync::atomic::AtomicUsize,
    other_puts: std::sync::atomic::AtomicUsize,
    unconditional_puts: std::sync::atomic::AtomicUsize,
}

async fn pause_pack_upload(upstream: hyper::Uri) -> (std::net::SocketAddr, std::sync::Arc<PackUpload>, tokio::task::JoinHandle<()>) {
    use std::sync::atomic::Ordering;
    use http_body_util::{BodyExt, Full};
    use hyper::body::{Bytes, Incoming};
    use hyper::service::service_fn;
    use hyper_util::client::legacy::Client;
    use hyper_util::rt::{TokioExecutor, TokioIo};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let upload = std::sync::Arc::new(PackUpload::default());
    let gate = upload.clone();
    let client: Client<_, Full<Bytes>> = Client::builder(TokioExecutor::new()).build_http();
    let proxy = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let (upstream, gate, client) = (upstream.clone(), gate.clone(), client.clone());
            tokio::spawn(async move {
                let service = service_fn(move |request: hyper::Request<Incoming>| {
                    let (upstream, gate, client) = (upstream.clone(), gate.clone(), client.clone());
                    async move {
                        let (mut parts, body) = request.into_parts();
                        let mut uri = upstream.into_parts();
                        uri.path_and_query = parts.uri.path_and_query().cloned();
                        parts.uri = hyper::Uri::from_parts(uri).unwrap();
                        let block_pack = parts.uri.path().rsplit('/').next()
                            .is_some_and(|name| name.starts_with('b') && name.ends_with(".pack"));
                        let request = hyper::Request::from_parts(parts, Full::new(body.collect().await.map_err(std::io::Error::other)?.to_bytes()));
                        if request.method() == hyper::Method::PUT && gate.observe_writes.load(Ordering::Relaxed) {
                            let assumed = request.headers().contains_key("x-amz-security-token");
                            let puts = if assumed { &gate.assumed_puts } else { &gate.other_puts };
                            puts.fetch_add(1, Ordering::Relaxed);
                            if request.headers().get("if-none-match").is_none_or(|value| value != "*") {
                                gate.unconditional_puts.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        if request.method() == hyper::Method::PUT && block_pack && gate.armed.swap(false, Ordering::Relaxed) {
                            // The complete request remains deliverable even if the deposed holder closes its socket.
                            let delayed = tokio::spawn(async move {
                                gate.held.notify_one();
                                gate.resume.notified().await;
                                let response = client.request(request).await;
                                gate.reply_status.store(response.as_ref().map_or(0, |reply| reply.status().as_u16()), Ordering::Relaxed);
                                gate.completed.notify_one();
                                response
                            });
                            return delayed.await.map_err(std::io::Error::other)?.map_err(std::io::Error::other);
                        }
                        client.request(request).await.map_err(std::io::Error::other)
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(stream), service).await;
            });
        }
    });
    (address, upload, proxy)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_holder_paused_at_a_pack_upload_cannot_replace_its_successors_block() {
    use std::sync::atomic::Ordering;

    let Some(upstream) = storage() else { return };
    let CellStorage::S3 { mut config } = upstream.clone() else { unreachable!() };
    let endpoint: hyper::Uri = config.endpoint.as_deref().unwrap().parse().unwrap();
    if endpoint.scheme_str() != Some("http") { return; }
    let (address, upload, proxy) = pause_pack_upload(endpoint).await;
    config.endpoint = Some(format!("http://{address}"));

    tokio::time::timeout(Duration::from_secs(90), async {
        let cell = Cell::start_on(Some(CellStorage::S3 { config }), vec![MemberPool::General; 2], None, None, None).await;
        let deployment = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
        let client = cell.connect("shop").await;
        client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();
        drop(client);

        let original = cell.holder("shop").await.unwrap();
        let old = cell.node(original);
        upload.armed.store(true, Ordering::Relaxed);
        let mut flushing = tokio::spawn({ let agent = old.agent.clone(); async move { agent.flush("shop").await } });
        tokio::select! {
            () = upload.held.notified() => {}
            result = &mut flushing => panic!("the flush completed before its upload was paused: {result:?}"),
            () = tokio::time::sleep(Duration::from_secs(10)) => panic!("the old holder did not reach its block upload"),
        }

        let old_member = old.lease.id();
        old.stop_renewing();
        while cell.directory.member(&old_member).await.unwrap().unwrap().is_live(rtdb_directory::now_ms()) {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let client = connect_eventually(&cell, "shop").await;
        let successor = cell.holder("shop").await.unwrap();
        assert_ne!(successor, original);
        client.simple_query("INSERT INTO orders (_id, total) VALUES (2, 20)").await.unwrap();
        cell.node(successor).agent.flush("shop").await.unwrap();

        let bucket = upstream.bucket(rtdb_storage::IoRegion::new()).await.unwrap();
        let catalog = rtdb_format::keys::block_key(rtdb_types::BlockIndex(0)).unwrap();
        let pack = rtdb_storage::ObjectKey::new(format!("{}/v06/{}", deployment.storage_prefix(), rtdb_multi::packing::block_pack(&catalog))).unwrap();
        let recovered = bucket.get(&pack).await.unwrap();
        let abandoned = tokio::time::timeout(Duration::from_secs(5), flushing).await.unwrap().unwrap().unwrap_err();
        assert_eq!(abandoned.code, rtdb_node::NOT_HELD);
        upload.resume.notify_one();
        tokio::time::timeout(Duration::from_secs(5), upload.completed.notified()).await.unwrap();
        assert_eq!(upload.reply_status.load(Ordering::Relaxed), 412);
        assert_eq!(bucket.get(&pack).await.unwrap(), recovered);
        assert_eq!(values(&client, "SELECT total FROM orders").await, ["10", "20"]);

        drop(client);
        cell.node(successor).agent.park("shop").await.unwrap();
        let restarted = connect_eventually(&cell, "shop").await;
        assert_eq!(values(&restarted, "SELECT total FROM orders").await, ["10", "20"]);
        drop(restarted);
        cell.stop().await;
    }).await.expect("paused S3 upload scenario stalled");
    proxy.abort();
}
