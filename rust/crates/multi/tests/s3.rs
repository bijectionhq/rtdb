//! A cell on S3: run against an S3-compatible server named by `RTDB_S3_ENDPOINT` (MinIO, say), with a bucket named by `RTDB_S3_BUCKET`, and credentials in the usual AWS environment variables.

mod common;

use std::time::Duration;

use common::*;
use rtdb_directory::MemberPool;
use rtdb_multi::{admin, deployment::CellStorage};

use common::s3_storage as storage;


#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn checkpoint_and_durable_tail_survive_loss_of_one_two_or_all_workers() {
    let Some(storage) = storage() else { eprintln!("RTDB_S3_ENDPOINT is not set: skipped"); return };
    tokio::time::timeout(Duration::from_secs(60), async {
        let mut cell = Cell::start_on(Some(storage), vec![MemberPool::General; 3], None, None, None).await;
        let deployment = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
        let client = cell.connect("shop").await;
        client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10), (2, 20), (3, 30)").await.unwrap();
        let holder = cell.holder("shop").await.unwrap();
        cell.node(holder).agent.flush("shop").await.unwrap();
        let checkpoint = cell.directory.deployment(&deployment.id).await.unwrap().unwrap().checkpoint.unwrap();
        client.simple_query("UPDATE orders SET total = 11 WHERE _id = 1").await.unwrap();
        client.simple_query("DELETE FROM orders WHERE _id = 2").await.unwrap();
        let mut expected_history = order_history(&client).await;
        drop(client);

        let mut expected = vec!["11".to_owned(), "30".to_owned()];
        for losses in 1..=3 {
            let before = cell.directory.deployment(&deployment.id).await.unwrap().unwrap();
            assert_eq!(before.checkpoint, Some(checkpoint));
            let holder = cell.holder("shop").await.unwrap();
            let victims: Vec<_> = std::iter::once(holder)
                .chain(cell.nodes.iter().enumerate().filter_map(|(index, node)| {
                    (index != holder && node.is_some()).then_some(index)
                }))
                .take(losses)
                .collect();
            assert_eq!(victims.len(), losses);
            let started = std::time::Instant::now();
            let departing = victims.iter().map(|&index| cell.nodes[index].take().unwrap());
            for result in futures::future::join_all(departing.map(|node| node.kill())).await {
                result.unwrap();
            }
            if losses == 3 {
                assert!(cell.nodes.iter().all(Option::is_none));
                cell.add_node().await;
            }

            let recovered = connect_eventually(&cell, "shop").await;
            let after = cell.directory.deployment(&deployment.id).await.unwrap().unwrap();
            assert!(after.epoch > before.epoch);
            assert_eq!(after.checkpoint, Some(checkpoint));
            assert_eq!(values(&recovered, "SELECT total FROM orders").await, expected);
            assert_eq!(order_history(&recovered).await, expected_history);
            eprintln!("loss of {losses} worker(s): recovered and verified results in {:?}", started.elapsed());
            let id = losses + 3;
            let total = 30 + losses;
            recovered.simple_query(&format!("INSERT INTO orders (_id, total) VALUES ({id}, {total})")).await.unwrap();
            expected.push(total.to_string());
            assert_eq!(values(&recovered, "SELECT total FROM orders").await, expected);
            expected_history = order_history(&recovered).await;
            drop(recovered);
            if losses < 3 {
                for _ in 0..losses {
                    cell.add_node().await;
                }
            }
        }
        cell.stop().await;
    }).await.expect("the worker-loss scenarios stalled");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cell_keeps_its_deployments_forks_and_deletions_in_s3() {
    let Some(storage) = storage() else { eprintln!("RTDB_S3_ENDPOINT is not set: skipped"); return };
    tokio::time::timeout(Duration::from_secs(180), exercise_cell(storage))
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
    let child = admin::fork(&cell.directory, &cell.storage(), &cell.keys(), "shop", "shop-preview", &control()).await.unwrap();
    prove_rejected_fork_disposal(&cell, &child).await;
    let copied = rtdb_multi::reference::stores(bucket.as_ref(), &child.storage_prefix()).await.unwrap();
    for store in &copied {
        assert!(rtdb_multi::reference::physical_objects(bucket.as_ref(), store).await.unwrap().is_empty(), "a fork on S3 references its parent's objects rather than copying them");
    }
    let external = admin::fork_into(
        &cell.directory, &cell.storage(), &cell.keys(), "shop",
        admin::ForkDestination { name: "outside-preview", org: Some("recipient"), hosted_task: None }, &control(),
    ).await.unwrap();
    let versioned = rtdb_multi::reference::stores(bucket.as_ref(), &external.storage_prefix()).await.unwrap();
    assert_eq!(versioned.len(), 1);
    assert!(rtdb_multi::reference::Base::load(bucket.as_ref(), &versioned[0]).await.unwrap().objects.is_empty());

    eprintln!("S3 phase: read and write fork");
    let preview = connect_eventually(&cell, "shop-preview").await;
    assert_eq!(values(&preview, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4", "5"]);
    preview.simple_query("INSERT INTO orders (_id, total) VALUES (50, 50)").await.unwrap();
    drop(preview);
    let holder = cell.holder("shop-preview").await.unwrap();
    eprintln!("S3 phase: park fork");
    cell.node(holder).agent.park("shop-preview").await.unwrap();

    eprintln!("S3 phase: delete parent");
    let deletion = prove_s3_erasure(&cell, &parent.id, true).await;
    assert!(deletion.facts.verified_empty, "the parent's current objects are gone from S3");
    assert_eq!(deletion.facts.inventory, rtdb_directory::DeletionInventory::AllVersionsAndMultipartUploads);
    let prefix = rtdb_storage::ObjectKey::new(parent.storage_prefix()).unwrap();
    assert!(bucket.list(Some(&prefix)).await.unwrap().is_empty());
    let preview = connect_eventually(&cell, "shop-preview").await;
    assert_eq!(values(&preview, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4", "5", "50"], "the fork outlives its parent, its shared objects copied into it first");
    drop(preview);

    let outside = connect_eventually(&cell, "outside-preview").await;
    assert_eq!(values(&outside, "SELECT _id FROM orders").await, ["0", "1", "2", "3", "4", "5"]);
    drop(outside);

    eprintln!("S3 phase: reap");
    let deployments = cell.directory.deployments().await.unwrap();
    for outcome in rtdb_multi::store::reaper_pass(bucket.as_ref(), &deployments, rtdb_directory::now_ms(), Duration::from_secs(3600)).await.unwrap() {
        outcome.unwrap();
    }
    eprintln!("S3 phase: stop cell");
    cell.stop().await;
}

async fn prove_rejected_fork_disposal(cell: &Cell, published: &rtdb_directory::DeploymentRecord) {
    use rtdb_directory::{DeletionObservation, Lifecycle, ServingInventory, now_ms};
    use rtdb_multi::keys::KeyService;
    use rtdb_storage::{IoRegion, ObjectKey};

    let directory = cell.directory.as_ref();
    let storage = cell.storage();
    let keys = cell.keys();
    assert_eq!(admin::fork(&cell.directory, &storage, &keys, "shop", &published.name, &control()).await.unwrap_err().code, "rtdb.directory/exists");
    let rejected = directory.deployments().await.unwrap().into_iter().find(|record| {
        record.name == published.name && record.hosted_fork_stage.is_some()
    }).unwrap();
    let id = &rejected.id;
    let bucket = storage.bucket(IoRegion::new()).await.unwrap();
    let prefix = ObjectKey::new(rejected.storage_prefix()).unwrap();
    assert_eq!(rejected.lifecycle, Lifecycle::Deleting);
    assert!(!bucket.list(Some(&prefix)).await.unwrap().is_empty());
    assert!(keys.existing_key(&rejected.key_id()).await.unwrap().is_some());
    assert_eq!(directory.deployment_by_name(&published.name).await.unwrap().unwrap().id, published.id);

    let finish = || admin::finish_disposal(directory, &storage, &keys, id, 4 << 20);
    let replace_observation = |observation| async move {
        let mut current = directory.deployment(id).await.unwrap().unwrap();
        current.deletion_observation = Some(observation);
        let version = current.version;
        assert!(directory.replace_deployment(current, version).await.unwrap().applied().is_some());
    };
    assert_eq!(finish().await.unwrap_err().code, "rtdb.multi/storage-lease-active");
    replace_observation(DeletionObservation::Fenced { leases_expire_at_ms: 0 }).await;
    assert_eq!(finish().await.unwrap_err().code, "rtdb.multi/deletion-verification-pending");
    let age = (rtdb_s3::SCOPED_S3_WRITE_TTL + rtdb_s3::SCOPED_S3_CLOCK_ALLOWANCE).as_millis() as i64;
    replace_observation(DeletionObservation::Empty { observed_at_ms: now_ms() - age }).await;
    let receipt = finish().await.unwrap();
    assert_eq!(receipt.deployment, *id);
    assert_eq!(receipt.serving_inventory, ServingInventory::S3VersionsAndUploads);
    assert!(bucket.list(Some(&prefix)).await.unwrap().is_empty());
    assert_eq!(keys.existing_key(&rejected.key_id()).await.unwrap_err().code, "rtdb.multi/key-destroyed");
    assert_eq!(directory.deployment_by_name(&published.name).await.unwrap().unwrap().id, published.id);
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
    assert!(writes.scoped_publications.load(Ordering::Relaxed) >= 3);
    assert_eq!(writes.unscoped_writes.load(Ordering::Relaxed), 0);
    assert_eq!(writes.unconditional_publications.load(Ordering::Relaxed), 0);
    drop(client);

    let deployed = cell.directory.deployment_by_name("shop").await.unwrap().unwrap();
    assert!(deployed.active_s3_write_lease(rtdb_directory::now_ms()).is_some());
    let keys = cell.keys();
    let deletion = admin::delete_deployment(cell.directory.as_ref(), &cell.storage(), &keys, "shop", 4 << 20).await.unwrap_err();
    assert_eq!(deletion.code, "rtdb.multi/storage-lease-active");
    assert_eq!(cell.directory.deployment(&deployed.id).await.unwrap().unwrap().lifecycle, rtdb_directory::Lifecycle::Deleting);
    assert!(rtdb_multi::keys::KeyService::existing(&keys, &deployed.id).await.is_ok());
    prove_s3_erasure(&cell, &deployed.id, true).await;
    cell.stop().await;
    proxy.abort();
}

async fn prove_s3_erasure(cell: &Cell, id: &str, inject_late_object: bool) -> rtdb_directory::DeletionRecord {
    use bytes::Bytes;
    use rtdb_directory::{DeletionInventory, DeletionObservation, now_ms};
    use rtdb_multi::keys::{KeyService, verify_deletion};
    use rtdb_storage::{IoRegion, ObjectKey};

    let storage = cell.storage();
    let keys = cell.keys();
    let directory = cell.directory.as_ref();
    let update = |record: rtdb_directory::DeploymentRecord| async move {
        let version = record.version;
        directory.replace_deployment(record, version).await.unwrap().applied().unwrap()
    };
    let delete = || admin::delete_deployment(directory, &storage, &keys, "shop", 4 << 20);
    let age = (rtdb_s3::SCOPED_S3_WRITE_TTL + rtdb_s3::SCOPED_S3_CLOCK_ALLOWANCE).as_millis() as i64;

    assert_eq!(delete().await.unwrap_err().code, "rtdb.multi/storage-lease-active");
    let mut record = directory.deployment(id).await.unwrap().unwrap();
    let namespace = ObjectKey::new(record.storage_prefix()).unwrap();
    let arrival = ObjectKey::new(format!("{namespace}/late-object")).unwrap();
    let bucket = storage.bucket(IoRegion::new()).await.unwrap();
    assert!(matches!(record.deletion_observation, Some(DeletionObservation::Fenced { leases_expire_at_ms }) if leases_expire_at_ms > now_ms()));
    assert!(keys.existing(id).await.is_ok());
    record.s3_write_lease_until_ms = record.s3_write_lease_until_ms.map(|_| now_ms() - 1);
    record.deletion_observation = Some(DeletionObservation::Fenced { leases_expire_at_ms: now_ms() - 1 });
    update(record).await;
    assert_eq!(delete().await.unwrap_err().code, "rtdb.multi/deletion-verification-pending");
    let mut record = directory.deployment(id).await.unwrap().unwrap();
    assert!(matches!(record.deletion_observation, Some(DeletionObservation::Empty { .. })));
    assert!(record.deletion_record.is_none());
    assert!(keys.existing(id).await.is_ok());
    assert_eq!(delete().await.unwrap_err().code, "rtdb.multi/deletion-verification-pending");

    if inject_late_object {
        bucket.put(&arrival, Bytes::from_static(b"arrived after first pass")).await.unwrap();
        record.deletion_observation = Some(DeletionObservation::Empty { observed_at_ms: now_ms() - age });
        update(record).await;
        assert_eq!(delete().await.unwrap_err().code, "rtdb.multi/deletion-incomplete");
        let mut record = directory.deployment(id).await.unwrap().unwrap();
        assert!(matches!(record.deletion_observation, Some(DeletionObservation::Fenced { .. })));
        assert!(bucket.head(&arrival).await.is_ok());
        assert!(keys.existing(id).await.is_ok());

        record.deletion_observation = Some(DeletionObservation::Verified { first_empty_at_ms: now_ms(), last_empty_at_ms: now_ms() });
        let mut record = update(record).await;
        assert_eq!(delete().await.unwrap_err().code, "rtdb.multi/deletion-observation");
        assert!(keys.existing(id).await.is_ok());
        record.deletion_observation = Some(DeletionObservation::Fenced { leases_expire_at_ms: now_ms() - 1 });
        update(record).await;
        assert_eq!(delete().await.unwrap_err().code, "rtdb.multi/deletion-verification-pending");
    }
    let mut record = directory.deployment(id).await.unwrap().unwrap();
    assert!(matches!(record.deletion_observation, Some(DeletionObservation::Empty { .. })));
    assert!(bucket.list(Some(&namespace)).await.unwrap().is_empty());
    let first_empty = now_ms() - age;
    record.deletion_observation = Some(DeletionObservation::Empty { observed_at_ms: first_empty });
    update(record).await;
    let receipt = delete().await.unwrap();
    assert_eq!(receipt.facts.first_empty_observed_at_ms, Some(first_empty));
    assert!(receipt.facts.objects_erased_at_ms >= first_empty + age);
    assert_eq!(receipt.facts.inventory, DeletionInventory::AllVersionsAndMultipartUploads);
    assert_eq!(keys.existing(id).await.unwrap_err().code, "rtdb.multi/key-destroyed");
    verify_deletion(&receipt, &keys.deletion_verification_key().await.unwrap()).unwrap();
    assert_eq!(delete().await.unwrap(), receipt);
    receipt
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cell_move_refuses_overlapping_s3_locations() {
    use rtdb_multi::cells::MemoryCellMap;

    let Some(storage) = storage() else { eprintln!("RTDB_S3_ENDPOINT is not set: skipped"); return };
    let map = MemoryCellMap::new();
    let east = Cell::start_on(Some(storage.clone()), vec![], None, None, Some((map.clone(), "east".into()))).await;
    let west = Cell::start_in("west", map.clone(), 0).await;
    let (east_keys, west_keys) = (east.keys(), west.keys());
    let from = admin::CellHandle { name: "east", directory: &east.directory, storage: &storage, keys: &east_keys, maximum_deployments: 10 };
    let deployment = admin::create_in_cells(map.as_ref(), std::slice::from_ref(&from), "shop", "acme", 3).await.unwrap().1;
    let CellStorage::S3 { mut config } = storage.clone() else { unreachable!() };
    let pack_prefix = format!("{}/v06/~packs", deployment.storage_prefix());
    config.prefix = Some(format!("{}/cell/{pack_prefix}", config.prefix.as_deref().unwrap()));
    let bucket = storage.bucket(rtdb_storage::IoRegion::new()).await.unwrap();
    let packs = rtdb_storage::ObjectKey::new(pack_prefix).unwrap();
    let nested = CellStorage::S3 { config };
    for destination in [&storage, &nested] {
        let to = admin::CellHandle { name: "west", directory: &west.directory, storage: destination, keys: &west_keys, maximum_deployments: 10 };
        let error = admin::move_cell(map.as_ref(), &from, &to, "shop", 4 << 20, None).await.unwrap_err();
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
    drop_completion_reply: std::sync::atomic::AtomicBool,
    dropped_reply_status: std::sync::atomic::AtomicU16,
    observe_writes: std::sync::atomic::AtomicBool,
    scoped_publications: std::sync::atomic::AtomicUsize,
    unscoped_writes: std::sync::atomic::AtomicUsize,
    unconditional_publications: std::sync::atomic::AtomicUsize,
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
                        let query = request.uri().query().unwrap_or_default();
                        let multipart = query.split('&').any(|pair| pair.starts_with("uploadId="));
                        let creates_upload = query.split('&').any(|pair| matches!(pair, "uploads" | "uploads="));
                        let commits_object = request.method() == hyper::Method::PUT && !multipart
                            || request.method() == hyper::Method::POST && multipart;
                        let writes_object = request.method() == hyper::Method::PUT
                            || request.method() == hyper::Method::POST && (multipart || creates_upload);
                        if writes_object && gate.observe_writes.load(Ordering::Relaxed) {
                            let scoped = request.headers().contains_key("x-amz-security-token");
                            if !scoped { gate.unscoped_writes.fetch_add(1, Ordering::Relaxed); }
                            if scoped && commits_object { gate.scoped_publications.fetch_add(1, Ordering::Relaxed); }
                            if commits_object && request.headers().get("if-none-match").is_none_or(|value| value != "*") {
                                gate.unconditional_publications.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        if commits_object && block_pack && gate.drop_completion_reply.swap(false, Ordering::Relaxed) {
                            let response = client.request(request).await.map_err(std::io::Error::other)?;
                            gate.dropped_reply_status.store(response.status().as_u16(), Ordering::Relaxed);
                            drop(response.into_body().collect().await.map_err(std::io::Error::other)?);
                            return Err(std::io::Error::other("S3 completion reply was discarded by the fixture"));
                        }
                        if commits_object && block_pack && gate.armed.swap(false, Ordering::Relaxed) {
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
async fn a_flush_recovers_a_pack_whose_s3_completion_reply_was_lost() {
    use std::sync::atomic::Ordering;

    let Some(CellStorage::S3 { mut config }) = storage() else { return; };
    let endpoint: hyper::Uri = config.endpoint.as_deref().unwrap().parse().unwrap();
    if endpoint.scheme_str() != Some("http") { return; }
    let (address, fault, proxy) = pause_pack_upload(endpoint).await;
    config.endpoint = Some(format!("http://{address}"));

    tokio::time::timeout(Duration::from_secs(60), async {
        let cell = Cell::start_on(Some(CellStorage::S3 { config }), vec![MemberPool::General; 2], None, None, None).await;
        let deployment = admin::create_deployment(cell.directory.as_ref(), "shop", "acme", 3).await.unwrap();
        let client = cell.connect("shop").await;
        client.simple_query("INSERT INTO orders (_id, total) VALUES (1, 10)").await.unwrap();

        fault.drop_completion_reply.store(true, Ordering::Relaxed);
        let holder = cell.holder("shop").await.unwrap();
        cell.node(holder).agent.flush("shop").await.unwrap();
        assert_eq!(fault.dropped_reply_status.load(Ordering::Relaxed), 200);
        let published = cell.directory.deployment(&deployment.id).await.unwrap().unwrap();
        assert_eq!(published.checkpoint.unwrap().block, 0);
        let catalog = rtdb_format::keys::block_key(rtdb_types::BlockIndex(0)).unwrap();
        let pack = format!("/{}", rtdb_multi::packing::block_pack(&catalog));
        assert!(!published.storage_regions.is_empty());
        assert!(published.storage_regions.values().all(|region| {
            matches!(region, rtdb_directory::StorageRegion::Remote { publications, .. }
                if publications.values().all(|upload| !upload.key.ends_with(&pack)))
        }));

        admin::move_deployment(cell.directory.as_ref(), "shop", &cell.node(1 - holder).lease.id(), &control()).await.unwrap();
        assert_eq!(values(&client, "SELECT total FROM orders").await, ["10"]);
        drop(client);
        cell.stop().await;
    }).await.expect("the lost completion reply stalled the deployment");
    proxy.abort();
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ordered_index_erasure_retires_the_old_hosted_provider_generation() {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use rtdb_storage::{IoRegion, ObjectKey};
    use tokio_util::sync::CancellationToken;

    let Some(storage @ CellStorage::S3 { .. }) = storage() else {
        eprintln!("RTDB_S3_ENDPOINT is not set: skipped");
        return;
    };
    tokio::time::timeout(Duration::from_secs(60), async {
        let CellStorage::S3 { config } = &storage else { unreachable!() };
        let root = ObjectKey::new("cell").unwrap();
        let inventory = config.versioned_inventory(&root, &BTreeMap::new()).await.unwrap();
        let bucket = storage.bucket(IoRegion::new()).await.unwrap();

        let cell = Cell::start_on(Some(storage.clone()), vec![MemberPool::General; 2], None, None, None).await;
        let deployment = admin::create_deployment(cell.directory.as_ref(), "ordered", "acme", 3).await.unwrap();
        let client = cell.connect("ordered").await;
        client.simple_query("CREATE INDEX order_keys ON orders (key, _id)").await.unwrap();
        client.simple_query("INSERT INTO orders RECORDS {_id: 1, key: 'erased'}, {_id: 2, key: 'kept'}").await.unwrap();

        let holder = cell.holder("ordered").await.unwrap();
        let node = cell.node(holder);
        let database = node.server.node().catalog().database("ordered").unwrap();
        node.agent.flush("ordered").await.unwrap();
        database.build_artefacts().await.unwrap();

        let deployed = cell.directory.deployment(&deployment.id).await.unwrap().unwrap();
        let index = deployed.ordered_indexes.keys().next().unwrap().clone();
        let original = ObjectKey::new(format!("{index}/generations/0000000000000000")).unwrap();
        assert!(!inventory.is_empty(&original).await.unwrap());
        assert_eq!(values(&client, "SELECT DISTINCT key FROM orders WHERE key >= 'a' ORDER BY key LIMIT 2").await,
                   ["erased", "kept"]);

        client.simple_query("ERASE FROM orders WHERE _id = 1").await.unwrap();
        node.agent.flush("ordered").await.unwrap();
        database.build_artefacts().await.unwrap();
        let advanced = cell.directory.deployment(&deployment.id).await.unwrap().unwrap();
        let scope = advanced.ordered_indexes[&index].oldest_generation;
        assert!(scope > 0);

        let stop = CancellationToken::new();
        let reaper = tokio::spawn(rtdb_multi::store::run_reaper(
            cell.directory.clone(), storage.clone(), Some(Arc::new(cell.keys())),
            Duration::from_secs(3600), Duration::from_millis(20), stop.clone(),
        ));
        let observed = loop {
            let current = cell.directory.deployment(&deployment.id).await.unwrap().unwrap();
            let state = &current.ordered_indexes[&index];
            if state.audited_before >= scope && !state.generations.contains_key(&0) {
                break current;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert!(inventory.is_empty(&original).await.unwrap());
        assert!(bucket.list(Some(&original)).await.unwrap().is_empty());
        assert!(observed.ordered_indexes[&index].generations.contains_key(&scope));
        assert_eq!(values(&client, "SELECT DISTINCT key FROM orders WHERE key >= 'a' ORDER BY key LIMIT 2").await,
                   ["kept"]);

        client.simple_query("DROP INDEX order_keys").await.unwrap();
        node.agent.flush("ordered").await.unwrap();
        database.build_artefacts().await.unwrap();
        loop {
            let current = cell.directory.deployment(&deployment.id).await.unwrap().unwrap();
            if !current.ordered_indexes.contains_key(&index) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let replacement = ObjectKey::new(format!("{index}/generations/{scope:016x}")).unwrap();
        assert!(inventory.is_empty(&replacement).await.unwrap());
        assert_eq!(values(&client, "SELECT DISTINCT key FROM orders WHERE key >= 'a' ORDER BY key LIMIT 2").await,
                   ["kept"]);

        stop.cancel();
        reaper.await.unwrap();
        drop(database);
        drop(client);
        cell.stop().await;
    }).await.expect("hosted ordered-index retirement stalled");
}
