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
    assert!(deletion.verified_empty, "the parent's objects are gone from S3");
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
