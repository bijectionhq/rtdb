use std::{collections::BTreeMap, sync::Arc};

use object_store::{ObjectStoreExt, aws::AmazonS3Builder, path::Path};
use rtdb_format::journal::{ObjectId, ObjectKind, RankedCatalog, Record};
use rtdb_s3::S3Config;
use rtdb_storage::{IoRegion, ObjectKey, remote::RankedAdmissionScope};
use uuid::Uuid;

fn loopback_config() -> S3Config {
    let endpoint = std::env::var("RTDB_BACKUP_HOLD_TEST_ENDPOINT").unwrap();
    let endpoint_url = url::Url::parse(&endpoint).unwrap();
    assert_eq!(endpoint_url.host_str(), Some("127.0.0.1"));
    let bucket = std::env::var("RTDB_BACKUP_HOLD_TEST_BUCKET").unwrap();
    assert!(bucket.starts_with("rtdb-backup-hold-test-"));
    let mut config = S3Config::new(bucket);
    config.endpoint = Some(endpoint);
    config.region = Some("us-east-1".into());
    config
}

#[tokio::test]
#[ignore = "requires a disposable S3-compatible loopback bucket"]
async fn ranked_admission_closure_requires_the_original_s3_inventory() {
    let config = loopback_config();
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
        manifest: ObjectId {
            seq: 2,
            ..catalogue.manifest
        },
        position: 4,
        ..catalogue
    };
    assert_eq!(writer.publish(next).await.unwrap(), 2);

    let store = config
        .open(&root, &remotes, Arc::clone(&recovery_io))
        .await
        .unwrap();
    let entry = store
        .list(Some(&ObjectKey::new("ranked-admission/v1").unwrap()))
        .await
        .unwrap()
        .into_iter()
        .find(|entry| entry.key.as_str().ends_with("/entries/0000000000000001"))
        .unwrap();
    let client = AmazonS3Builder::from_env()
        .with_bucket_name(&config.bucket)
        .with_endpoint(config.endpoint.as_deref().unwrap())
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .build()
        .unwrap();
    let entry = Path::from(format!("{root}/{}", entry.key));
    let substituted = RankedCatalog {
        manifest: ObjectId {
            digest: [9; 32],
            ..catalogue.manifest
        },
        ..catalogue
    };
    client
        .put(
            &entry,
            Record::RankedCatalog(substituted).encode().unwrap().into(),
        )
        .await
        .unwrap();
    assert_eq!(
        custodian.close().await.unwrap_err().code,
        "rtdb.storage/ranked-admission-inventory"
    );
    let retry = RankedCatalog {
        position: 5,
        ..catalogue
    };
    assert_eq!(
        writer.publish(retry).await.unwrap_err().code,
        "rtdb.storage/ranked-admission-closed"
    );
    recovery_io.close_and_wait().await;
    writer_io.close_and_wait().await;

    client
        .put(
            &entry,
            Record::RankedCatalog(catalogue).encode().unwrap().into(),
        )
        .await
        .unwrap();
    let reopened_io = IoRegion::new();
    let reopened = open(Arc::clone(&reopened_io)).await.unwrap();
    let closed = reopened.close().await.unwrap();
    assert_eq!(closed.admitted, 2);
    assert_eq!(reopened.close().await.unwrap(), closed);
    assert_eq!(reopened.publish(catalogue).await.unwrap(), 1);
    let first = reopened.outcome_page(closed, 0, 1).await.unwrap();
    assert_eq!(first.catalogues, [catalogue]);
    let last = reopened
        .outcomes(closed, first.next_after.unwrap())
        .await
        .unwrap();
    assert_eq!(last.catalogues, [next]);
    assert_eq!(last.next_after, None);
    reopened_io.close_and_wait().await;
}

#[cfg(feature = "versioning")]
#[tokio::test]
#[ignore = "requires a disposable versioned S3-compatible loopback bucket"]
async fn ranked_admission_closure_drains_only_the_recorded_multipart_upload() {
    use aws_sdk_s3::{
        error::ProvideErrorMetadata, primitives::ByteStream, types::ChecksumAlgorithm,
    };
    use bytes::Bytes;
    use object_store::aws::S3ConditionalPut;
    use ring::digest::{SHA256, digest};
    use rtdb_storage::remote::{PublicationResult, RemoteStore};

    let config = loopback_config();
    let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .endpoint_url(config.endpoint.as_deref().unwrap())
        .load()
        .await;
    let client = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&sdk)
            .force_path_style(true)
            .build(),
    );
    let root = ObjectKey::new("multi").unwrap();
    let scope = RankedAdmissionScope {
        incarnation: Uuid::new_v4(),
        activation_sha256: [6; 32],
    };
    let remotes = BTreeMap::new();
    let writer_io = IoRegion::new();
    let writer = config
        .ranked_admission(&root, &remotes, Arc::clone(&writer_io), scope)
        .await
        .unwrap();
    writer.commission().await.unwrap();
    let bytes = Bytes::from_static(b"ranked staging bytes");
    let first = ObjectId {
        database: scope.incarnation,
        kind: ObjectKind::File,
        epoch: 1,
        seq: 1,
        digest: digest(&SHA256, &bytes).as_ref().try_into().unwrap(),
    };
    let ready = writer.reserve_staging(first).await.unwrap();
    assert!(
        writer
            .complete_staging(ready.clone(), Bytes::from_static(b"different bytes"))
            .await
            .is_err()
    );
    assert!(matches!(
        writer.complete_staging(ready, bytes.clone()).await.unwrap(),
        PublicationResult::Published {
            version: Some(_),
            ..
        }
    ));
    let interrupted = ObjectId { seq: 2, ..first };
    let reserved = writer.reserve_staging(interrupted).await.unwrap();
    assert_eq!(
        writer.reserve_staging(first).await.unwrap_err().code,
        "rtdb.storage/ranked-staging-busy"
    );

    let path = format!("{root}/{interrupted}");
    let uploads = client
        .list_multipart_uploads()
        .bucket(&config.bucket)
        .prefix(&path)
        .send()
        .await
        .unwrap();
    let [upload] = uploads.uploads() else {
        panic!("expected one registered multipart upload");
    };
    assert_eq!(upload.key(), Some(path.as_str()));
    let upload_id = upload.upload_id().unwrap();
    client
        .upload_part()
        .bucket(&config.bucket)
        .key(&path)
        .upload_id(upload_id)
        .part_number(1)
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .body(ByteStream::from_static(b"uncommitted ranked bytes"))
        .send()
        .await
        .unwrap();

    let unrelated = format!(
        "{root}/{}",
        ObjectId {
            database: Uuid::new_v4(),
            ..interrupted
        }
    );
    let unrelated_upload = client
        .create_multipart_upload()
        .bucket(&config.bucket)
        .key(&unrelated)
        .send()
        .await
        .unwrap();
    let unrelated_id = unrelated_upload.upload_id().unwrap();
    drop(writer);
    writer_io.close_and_wait().await;

    let ledger_client = AmazonS3Builder::from_env()
        .with_bucket_name(&config.bucket)
        .with_endpoint(config.endpoint.as_deref().unwrap())
        .with_region("us-east-1")
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()
        .unwrap();
    let ledger_only = RemoteStore::new(Arc::new(ledger_client), Some(root.clone()))
        .ranked_admission(scope)
        .unwrap();
    assert_eq!(
        ledger_only.close().await.unwrap_err().code,
        "rtdb.storage/ranked-staging-provider"
    );
    let partial = client
        .list_parts()
        .bucket(&config.bucket)
        .key(&path)
        .upload_id(upload_id)
        .send()
        .await
        .unwrap();
    assert_eq!(partial.parts().len(), 1);

    let recovery_io = IoRegion::new();
    let recovered = config
        .ranked_admission(&root, &remotes, Arc::clone(&recovery_io), scope)
        .await
        .unwrap();
    let closed = recovered.close().await.unwrap();
    assert_eq!(closed.admitted, 0);
    let absent = client
        .list_parts()
        .bucket(&config.bucket)
        .key(&path)
        .upload_id(upload_id)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        absent.as_service_error().and_then(|error| error.code()),
        Some("NoSuchUpload")
    );
    client
        .list_parts()
        .bucket(&config.bucket)
        .key(&unrelated)
        .upload_id(unrelated_id)
        .send()
        .await
        .unwrap();
    assert_eq!(
        recovered
            .complete_staging(reserved, bytes)
            .await
            .unwrap_err()
            .code,
        "rtdb.storage/ranked-admission-closed"
    );
    assert_eq!(recovered.close().await.unwrap(), closed);
    assert!(
        recovered
            .outcomes(closed, 0)
            .await
            .unwrap()
            .catalogues
            .is_empty()
    );
    client
        .head_object()
        .bucket(&config.bucket)
        .key(format!("{root}/{first}"))
        .send()
        .await
        .unwrap();
    recovery_io.close_and_wait().await;
}
