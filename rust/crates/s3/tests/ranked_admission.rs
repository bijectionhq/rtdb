use std::{collections::BTreeMap, sync::Arc};

use rtdb_format::journal::{ObjectId, ObjectKind, RankedCatalog};
use rtdb_s3::S3Config;
use rtdb_storage::{IoRegion, ObjectKey, remote::RankedAdmissionScope};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires a disposable S3-compatible loopback bucket"]
async fn ranked_admission_closes_on_the_original_s3_provider() {
    let endpoint = std::env::var("RTDB_BACKUP_HOLD_TEST_ENDPOINT").unwrap();
    let endpoint_url = url::Url::parse(&endpoint).unwrap();
    assert_eq!(endpoint_url.host_str(), Some("127.0.0.1"));
    let bucket = std::env::var("RTDB_BACKUP_HOLD_TEST_BUCKET").unwrap();
    assert!(bucket.starts_with("rtdb-backup-hold-test-"));
    let mut config = S3Config::new(&bucket);
    config.endpoint = Some(endpoint);
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
    let closed = custodian.close().await.unwrap();
    assert_eq!(closed.admitted, 1);
    let retry = RankedCatalog {
        position: 4,
        ..catalogue
    };
    assert!(writer.publish(retry).await.is_err());
    assert_eq!(writer.publish(catalogue).await.unwrap(), 1);
    writer_io.close_and_wait().await;
    let recovered = custodian.outcomes(closed, 0).await.unwrap();
    assert_eq!(recovered.catalogues, [catalogue]);
    assert_eq!(recovered.next_after, None);
    recovery_io.close_and_wait().await;
}
