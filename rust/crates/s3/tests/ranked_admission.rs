use std::{collections::BTreeMap, sync::Arc};

use object_store::{ObjectStoreExt, aws::AmazonS3Builder, path::Path};
use rtdb_format::journal::{ObjectId, ObjectKind, RankedCatalog, Record};
use rtdb_s3::S3Config;
use rtdb_storage::{IoRegion, ObjectKey, remote::RankedAdmissionScope};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a disposable S3-compatible loopback bucket"]
async fn ranked_admission_closure_requires_the_original_s3_inventory() {
    let endpoint = std::env::var("RTDB_BACKUP_HOLD_TEST_ENDPOINT").unwrap();
    let endpoint_url = url::Url::parse(&endpoint).unwrap();
    assert_eq!(endpoint_url.host_str(), Some("127.0.0.1"));
    let bucket = std::env::var("RTDB_BACKUP_HOLD_TEST_BUCKET").unwrap();
    assert!(bucket.starts_with("rtdb-backup-hold-test-"));
    let mut config = S3Config::new(&bucket);
    config.endpoint = Some(endpoint.clone());
    config.region = Some("us-east-1".into());
    let root = ObjectKey::new("multi").unwrap();
    let scope = RankedAdmissionScope {
        incarnation: Uuid::new_v4(),
        activation_sha256: [5; 32],
    };
    let remotes = BTreeMap::new();
    let open = |io| config.ranked_admission(&root, &remotes, io, scope);
    let writer_io = IoRegion::new();
    let recovery_io = IoRegion::new();
    let writer = open(Arc::clone(&writer_io)).await.unwrap();
    let custodian = open(Arc::clone(&recovery_io)).await.unwrap();
    writer.commission().await.unwrap();
    let catalogue = RankedCatalog {
        manifest: ObjectId {
            database: scope.incarnation,
            kind: ObjectKind::Manifest,
            epoch: 1,
            seq: 1,
            digest: [8; 32],
        },
        position: 3,
        system_time: 4,
        key_generation: 1,
    };
    assert_eq!(writer.publish(catalogue).await.unwrap(), 1);
    let next = RankedCatalog {
        manifest: ObjectId { seq: 2, ..catalogue.manifest },
        position: 4,
        ..catalogue
    };
    assert_eq!(writer.publish(next).await.unwrap(), 2);

    let store = config.open(&root, &remotes, Arc::clone(&recovery_io)).await.unwrap();
    let entry = store.list(Some(&ObjectKey::new("ranked-admission/v1").unwrap())).await.unwrap()
        .into_iter().find(|entry| entry.key.as_str().ends_with("/entries/0000000000000001")).unwrap();
    let client = AmazonS3Builder::from_env()
        .with_bucket_name(&bucket)
        .with_endpoint(&endpoint)
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()
        .unwrap();
    let entry = Path::from(format!("{root}/{}", entry.key));
    let substituted = RankedCatalog {
        manifest: ObjectId { digest: [9; 32], ..catalogue.manifest },
        ..catalogue
    };
    client.put(&entry, Record::RankedCatalog(substituted).encode().unwrap().into()).await.unwrap();
    assert_eq!(custodian.close().await.unwrap_err().code, "rtdb.storage/ranked-admission-inventory");
    let retry = RankedCatalog {
        position: 5,
        ..catalogue
    };
    assert_eq!(writer.publish(retry).await.unwrap_err().code, "rtdb.storage/ranked-admission-closed");
    recovery_io.close_and_wait().await;
    writer_io.close_and_wait().await;

    client.put(&entry, Record::RankedCatalog(catalogue).encode().unwrap().into()).await.unwrap();
    let reopened_io = IoRegion::new();
    let reopened = open(Arc::clone(&reopened_io)).await.unwrap();
    let closed = reopened.close().await.unwrap();
    assert_eq!(closed.admitted, 2);
    assert_eq!(reopened.close().await.unwrap(), closed);
    assert_eq!(reopened.publish(catalogue).await.unwrap(), 1);
    let first = reopened.outcome_page(closed, 0, 1).await.unwrap();
    assert_eq!(first.catalogues, [catalogue]);
    let last = reopened.outcomes(closed, first.next_after.unwrap()).await.unwrap();
    assert_eq!(last.catalogues, [next]);
    assert_eq!(last.next_after, None);
    reopened_io.close_and_wait().await;
}
