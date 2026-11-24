//! Current deployment keys in a dedicated, never-versioned S3 bucket.

use std::{sync::Arc, time::Instant};

use aws_sdk_s3::{Client, primitives::ByteStream, types::ServerSideEncryption};
use bytes::Bytes;
use rtdb_storage::{Error, IoRegion};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use uuid::Uuid;

use super::{
    ArchiveKey, ArchiveKeyWithdrawal, AssertionEnvelope, AssertionKeyService, AssertionStage, DELETION_SIGNING_KEY_ID, KEY_BYTES,
    KeyService, NAMESPACE_KEY_ID, UnitOperation, UnitOutput, UnitScope, UnitSlot, malformed, new_key,
    require_matching_key, journal::key_error, unit::{self, OPERATION_LIMIT},
};

mod captures;
mod units;

#[derive(Clone)]
pub struct S3KeyService {
    client: Client,
    bucket: String,
    reads: Arc<IoRegion>,
}

enum Current {
    Absent,
    Live { key: [u8; KEY_BYTES], etag: String },
    Withdrawn,
}

enum Precondition<'a> {
    Absent,
    Matches(&'a str),
}

impl S3KeyService {
    pub async fn open(specification: &str) -> Result<Self> {
        let (bucket, region) = locator(specification)?;
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_owned()))
            .load()
            .await;
        let vault = Self {
            client: Client::new(&config),
            bucket: bucket.to_owned(),
            reads: IoRegion::new(),
        };
        vault.require_never_versioned().await?;
        Ok(vault)
    }

    async fn require_never_versioned(&self) -> Result<()> {
        let reply = self.reads.read_async(Arc::new(self.clone()), (), |vault, ()| async move {
            Ok(vault.client.get_bucket_versioning().bucket(&vault.bucket).send().await)
        }).await.map_err(|_| unavailable("validate bucket versioning"))?
            .map_err(|_| unavailable("validate bucket versioning"))?;
        if reply.status().is_some() || reply.mfa_delete().is_some() {
            return Err(versioned());
        }
        Ok(())
    }

    async fn read_vault_object(&self, path: &str, maximum: usize) -> Result<Option<(Bytes, String)>> {
        self.reads.read_async(Arc::new(self.clone()), path.to_owned(), move |vault, path| async move {
            Ok(vault.read_exact(&path, maximum).await)
        }).await.map_err(|_| unavailable("read current key"))?
    }

    async fn read_exact(&self, path: &str, maximum: usize) -> Result<Option<(Bytes, String)>> {
        let reply = match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await
        {
            Ok(reply) => reply,
            Err(error)
                if error
                    .raw_response()
                    .is_some_and(|r| r.status().as_u16() == 404)
                    && error.as_service_error().is_some_and(|e| e.is_no_such_key()) =>
            {
                return Ok(None);
            }
            Err(_) => return Err(unavailable("read current key")),
        };
        if reply.version_id().is_some() {
            return Err(versioned());
        }
        if reply.server_side_encryption() != Some(&ServerSideEncryption::Aes256) {
            return Err(malformed());
        }
        let length = reply.content_length().ok_or_else(malformed)?;
        if length < 0 || length as u64 > maximum as u64 {
            return Err(malformed());
        }
        let etag = reply
            .e_tag()
            .filter(|etag| !etag.is_empty())
            .ok_or_else(malformed)?
            .to_owned();
        let body = reply
            .body
            .collect()
            .await
            .map_err(|_| unavailable("read key body"))?
            .into_bytes();
        if body.len() as i64 != length {
            return Err(malformed());
        }
        Ok(Some((body, etag)))
    }

    async fn current(&self, path: &str) -> Result<Current> {
        match self.read_vault_object(path, KEY_BYTES).await? {
            None => Ok(Current::Absent),
            Some((body, _)) if body.is_empty() => Ok(Current::Withdrawn),
            Some((body, etag)) => {
                let key = body.as_ref().try_into().map_err(|_| malformed())?;
                Ok(Current::Live { key, etag })
            }
        }
    }

    async fn put(&self, path: &str, body: &[u8], condition: Precondition<'_>) -> Result<bool> {
        let put = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(path)
            .body(ByteStream::from(body.to_vec()))
            .server_side_encryption(ServerSideEncryption::Aes256);
        let put = match condition {
            Precondition::Absent => put.if_none_match("*"),
            Precondition::Matches(etag) => put.if_match(etag),
        };
        match put.send().await {
            Ok(reply) if reply.version_id().is_some() => Err(versioned()),
            Ok(_) => Ok(true),
            Err(error)
                if error
                    .raw_response()
                    .is_some_and(|r| matches!(r.status().as_u16(), 409 | 412)) =>
            {
                Ok(false)
            }
            Err(_) => Err(unavailable("write current key")),
        }
    }

    async fn withdraw_current(&self, path: &str) -> Result<()> {
        for _ in 0..4 {
            let current = self.current(path).await?;
            let condition = match &current {
                Current::Absent => Precondition::Absent,
                Current::Live { etag, .. } => Precondition::Matches(etag),
                Current::Withdrawn => return self.require_never_versioned().await,
            };
            self.put(path, &[], condition).await?;
        }
        match self.current(path).await? {
            Current::Withdrawn => self.require_never_versioned().await,
            _ => Err(unavailable("confirm key withdrawal")),
        }
    }
}

#[async_trait::async_trait]
impl KeyService for S3KeyService {
    fn retained_unit_ledger(&self, scope: UnitScope, io: Arc<IoRegion>) -> Result<Arc<dyn rtdb_storage::sealing::ObjectCipher>> {
        Ok(Arc::new(super::UnitLedger::new(Arc::new(self.clone()), scope, io)))
    }
    async fn grant_deployment_key(&self, id: &str, binding: [u8; KEY_BYTES]) -> rtdb_storage::Result<super::UnitOutput> {
        let material = self.existing_key(id).await.map_err(super::journal::key_error)?;
        super::delivery::require_deployment_key(material, binding)?;
        Ok(super::UnitOutput::Granted)
    }

    fn custody_namespace(&self) -> Result<String> {
        Ok(format!("s3:{}", self.bucket))
    }

    async fn existing_key(&self, id: &str) -> Result<Option<[u8; KEY_BYTES]>> {
        match self.current(&object_path(id)?).await? {
            Current::Absent => Ok(None),
            Current::Live { key, .. } => Ok(Some(key)),
            Current::Withdrawn => Err(withdrawn()),
        }
    }

    async fn key(&self, id: &str) -> Result<[u8; KEY_BYTES]> {
        if let Some(key) = self.existing_key(id).await? {
            return Ok(key);
        }
        let path = object_path(id)?;
        self.put(&path, &new_key()?, Precondition::Absent).await?;
        self.existing_key(id)
            .await?
            .ok_or_else(|| unavailable("confirm key creation"))
    }

    async fn copy(&self, from: &str, to: &str) -> Result<()> {
        self.install(to, self.existing(from).await?).await
    }

    async fn destroy(&self, id: &str) -> Result<()> {
        self.withdraw_current(&object_path(id)?).await?;
        self.withdraw_registered_scope(UnitScope::archive(id)).await?;
        Ok(())
    }

    async fn use_unit_key(&self, slot: UnitSlot, name: rtdb_storage::ObjectKey, operation: UnitOperation,
        _io: &Arc<IoRegion>) -> rtdb_storage::Result<UnitOutput> {
        let deadline = Instant::now() + OPERATION_LIMIT;
        tokio::time::timeout_at(deadline.into(), async {
            self.require_never_versioned().await.map_err(key_error)?;
            let material = self.load_unit_slot(&slot, operation.provisions()).await?;
            unit::with_material(material, deadline, |cipher| operation.apply(cipher, &name))
        }).await.map_err(|_| unit::expired())?
    }

    async fn withdraw_unit_key(&self, slot: UnitSlot, _io: &Arc<IoRegion>) -> Result<()> {
        self.require_never_versioned().await?;
        self.withdraw_current(&format!("keys/{}", slot.id())).await?;
        rtdb_storage::sealing::drain_unit_delivery().await;
        Ok(())
    }

    async fn withdraw_unit_scope(&self, scope: UnitScope, _io: &Arc<IoRegion>) -> Result<()> {
        self.withdraw_registered_scope(scope).await
    }

    async fn archive_key(&self, id: &str, provision: bool, region: Arc<IoRegion>) -> Result<Arc<dyn ArchiveKey>> {
        self.open_capture(id, provision, region).await
    }

    async fn scoped_unit_key(&self, id: &str, region: Arc<IoRegion>) -> Result<Arc<dyn ArchiveKey>> {
        self.open_capture(id, false, region).await
    }

    async fn withdraw_archive_key(&self, id: &str) -> Result<ArchiveKeyWithdrawal> {
        self.destroy(id).await?;
        Ok(ArchiveKeyWithdrawal::Vault)
    }

    fn assertion_keys(&self) -> Option<&dyn AssertionKeyService> {
        Some(self)
    }

    async fn install(&self, id: &str, key: [u8; KEY_BYTES]) -> Result<()> {
        self.put(&object_path(id)?, &key, Precondition::Absent)
            .await?;
        match self.existing_key(id).await? {
            Some(current) => require_matching_key(id, &key, &current),
            None => Err(unavailable("confirm key installation")),
        }
    }
}

#[async_trait::async_trait]
impl AssertionKeyService for S3KeyService {
    async fn seal_assertion_stage(
        &self, stage: AssertionStage, _region: &Arc<IoRegion>, plaintext: Bytes,
    ) -> Result<AssertionEnvelope> {
        self.require_never_versioned().await?;
        let key = new_key()?;
        let envelope = AssertionEnvelope::seal(stage, &key, &plaintext)?;
        drop(plaintext);
        let path = assertion_path(stage);
        if self.put(&path, &key, Precondition::Absent).await? {
            return Ok(envelope);
        }
        match self.current(&path).await? {
            Current::Live { .. } | Current::Withdrawn => Err(Anomaly::new(
                AnomalyCategory::Conflict, "rtdb.multi/assertion-stage-used",
                "This assertion stage already has a custody entry",
            )),
            Current::Absent => Err(unavailable("confirm assertion staging")),
        }
    }

    async fn open_assertion_stage(
        &self, stage: AssertionStage, _region: &Arc<IoRegion>, envelope: AssertionEnvelope,
    ) -> Result<Bytes> {
        self.require_never_versioned().await?;
        match self.current(&assertion_path(stage)).await? {
            Current::Live { key, .. } => envelope.open(stage, &key),
            Current::Withdrawn => Err(Error::ErasedUnderView.into()),
            Current::Absent => Err(unavailable("read assertion wrapping capability")),
        }
    }

    async fn retire_assertion_stage(
        &self, stage: AssertionStage, _region: &Arc<IoRegion>,
    ) -> Result<()> {
        self.require_never_versioned().await?;
        self.withdraw_current(&assertion_path(stage)).await
    }
}

fn assertion_path(stage: AssertionStage) -> String {
    format!("assertion-capabilities/{}", stage.key_id())
}

fn locator(specification: &str) -> Result<(&str, &str)> {
    let invalid = || {
        Anomaly::incorrect(
            "rtdb.multi/key-vault-config",
            "Expected a canonical s3://BUCKET?region=REGION key vault locator",
        )
    };
    let (bucket, region) = specification
        .strip_prefix("s3://")
        .and_then(|s| s.split_once("?region="))
        .ok_or_else(invalid)?;
    let valid_segment = |s: &str| {
        let alnum = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
        s.as_bytes().first().copied().is_some_and(alnum)
            && s.as_bytes().last().copied().is_some_and(alnum)
            && s.bytes().all(|b| alnum(b) || b == b'-')
    };
    if !(3..=63).contains(&bucket.len()) || !valid_segment(bucket) || !valid_segment(region) {
        return Err(invalid());
    }
    Ok((bucket, region))
}

fn object_path(id: &str) -> Result<String> {
    if matches!(id, NAMESPACE_KEY_ID | DELETION_SIGNING_KEY_ID) {
        return Ok(format!("metadata/{id}"));
    }
    let canonical_uuid = |text: &str| Uuid::try_parse(text).is_ok_and(|id| id.to_string() == text);
    if canonical_uuid(id) {
        return Ok(format!("keys/{id}-1"));
    }
    let invalid = || Anomaly::incorrect("rtdb.multi/key-id", "Invalid deployment key identity");
    let positive = |text: &str| {
        text.parse::<u64>().is_ok_and(|value| value > 0 && value.to_string() == text)
    };
    if let Some(rest) = id.strip_prefix("journal-") {
        let (deployment, rest) = rest.split_at_checked(36).ok_or_else(invalid)?;
        let rest = rest.strip_prefix('-').ok_or_else(invalid)?;
        let (incarnation, rest) = rest.split_at_checked(36).ok_or_else(invalid)?;
        let generation = rest.strip_prefix('-').ok_or_else(invalid)?;
        if !canonical_uuid(deployment) || !canonical_uuid(incarnation) || !positive(generation) {
            return Err(invalid());
        }
        return Ok(format!("keys/{id}"));
    }
    let (rest, checkpoint) = if let Some(rest) = id.strip_prefix("checkpoint-") {
        (rest, true)
    } else {
        (id.strip_prefix("stage-").unwrap_or(id), false)
    };
    let (deployment, rest) = rest.split_at_checked(36).ok_or_else(invalid)?;
    let rest = rest.strip_prefix('-').ok_or_else(invalid)?;
    let (incarnation, operation) = if checkpoint {
        let (incarnation, operation) = rest.split_once('-').ok_or_else(invalid)?;
        (incarnation, Some(operation))
    } else {
        (rest, None)
    };
    if !canonical_uuid(deployment)
        || !positive(incarnation)
        || operation.is_some_and(|text| !canonical_uuid(text))
    {
        return Err(invalid());
    }
    Ok(format!("keys/{id}"))
}

fn unavailable(operation: &str) -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.multi/key-vault",
        format!("S3 key vault could not {operation}"),
    )
}

fn versioned() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Forbidden,
        "rtdb.multi/key-vault-versioned",
        "The key vault must never retain object versions",
    )
}

fn withdrawn() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Forbidden,
        "rtdb.multi/key-destroyed",
        "The deployment key was destroyed",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hosted_vault_locators_are_canonical() {
        assert_eq!(
            locator("s3://bj-production-rtdb-keys?region=eu-north-1").unwrap(),
            ("bj-production-rtdb-keys", "eu-north-1")
        );
        for invalid in [
            "s3://bj-production-rtdb-keys",
            "s3://bj-production-rtdb-keys/prefix?region=eu-north-1",
            "s3://BJ-production-rtdb-keys?region=eu-north-1",
            "s3://bj-production-rtdb-keys?region=eu-north-1&endpoint=localhost",
        ] {
            assert!(locator(invalid).is_err());
        }
    }

    #[test]
    fn provider_metadata_has_a_separate_namespace() {
        assert_eq!(
            object_path(NAMESPACE_KEY_ID).unwrap(),
            "metadata/.rtdb-key-namespace"
        );
        assert_eq!(
            object_path(DELETION_SIGNING_KEY_ID).unwrap(),
            "metadata/.rtdb-deletion-signing-seed"
        );
        assert!(object_path("metadata/.rtdb-key-namespace").is_err());
    }

    #[test]
    fn the_first_residence_and_its_explicit_incarnation_share_one_vault_entry() {
        let mut record = rtdb_directory::DeploymentRecord::new("shop", "acme", Vec::new(), 0);
        assert_eq!(record.incarnation, 1);
        let first = object_path(&record.key_id()).unwrap();
        assert_eq!(first, object_path(&format!("{}-1", record.id)).unwrap());
        record.incarnation = 2;
        assert_ne!(first, object_path(&record.key_id()).unwrap());
    }

    #[test]
    fn keys_have_one_path_for_each_custody_identity() {
        const DEPLOYMENT: &str = "00000000-0000-0000-0000-00000000abcd";
        const OPERATION: &str = "00000000-0000-0000-0000-000000001234";
        for id in [
            format!("{DEPLOYMENT}-17"),
            format!("stage-{DEPLOYMENT}-17"),
            format!("checkpoint-{DEPLOYMENT}-17-{OPERATION}"),
            format!("journal-{DEPLOYMENT}-{OPERATION}-2"),
        ] {
            assert_eq!(object_path(&id).unwrap(), format!("keys/{id}"));
        }
        for id in [
            format!("../{DEPLOYMENT}-17"),
            format!("{DEPLOYMENT}-0"),
            format!("{DEPLOYMENT}-017"),
            format!("{}-17", DEPLOYMENT.to_ascii_uppercase()),
            format!("stage-{DEPLOYMENT}-17-{OPERATION}"),
            format!("checkpoint-{DEPLOYMENT}-17"),
            format!("journal-{DEPLOYMENT}-{OPERATION}-02"),
        ] {
            assert!(object_path(&id).is_err());
        }
    }
}

#[cfg(test)]
mod transport_tests {
    use super::*;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        task::JoinHandle,
    };

    const ID: &str = "00000000-0000-0000-0000-00000000abcd-17";
    const NEVER_VERSIONED: &[u8] = b"<VersioningConfiguration />";
    const ABSENT: &[u8] = b"<Error><Code>NoSuchKey</Code></Error>";
    const LIVE: &[u8; KEY_BYTES] = b"01234567890123456789012345678901";
    const ENCRYPTED: &str = "x-amz-server-side-encryption: AES256\r\nETag: \"tag\"\r\n";
    const ASSERTION_PATH: &str = "/assertion-capabilities/assertion-stage-00000000-0000-0000-0000-00000000abcd-17-00000000-0000-0000-0000-000000001234";

    struct Exchange {
        method: &'static str,
        request_header: &'static str,
        path: Option<&'static str>,
        status: &'static str,
        response_headers: &'static str,
        body: &'static [u8],
    }

    async fn mock_s3(exchanges: Vec<Exchange>) -> (S3KeyService, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let place = listener.local_addr().unwrap();
        let service = tokio::spawn(async move {
            for exchange in exchanges {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    request.push(stream.read_u8().await.unwrap());
                }
                let request = String::from_utf8(request).unwrap();
                let request_headers = request.to_ascii_lowercase();
                if request_headers.contains("expect: 100-continue") {
                    stream
                        .write_all(b"HTTP/1.1 100 Continue\r\n\r\n")
                        .await
                        .unwrap();
                }
                if let Some(length) = request_headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                {
                    let mut body = vec![0; length.parse().unwrap()];
                    stream.read_exact(&mut body).await.unwrap();
                }
                let path = if exchange.method == "VERSION" {
                    "/?versioning".to_owned()
                } else if let Some(path) = exchange.path {
                    path.to_owned()
                } else {
                    format!("/keys/{ID}")
                };
                let method = if exchange.method == "VERSION" {
                    "GET"
                } else {
                    exchange.method
                };
                assert!(
                    request.starts_with(&format!("{method} /test-vault{path}")),
                    "{request}"
                );
                if method == "PUT" {
                    assert!(request_headers.contains("x-amz-server-side-encryption: aes256"));
                }
                if !exchange.request_header.is_empty() {
                    assert!(request_headers.contains(exchange.request_header));
                }
                let response = format!(
                    "HTTP/1.1 {}\r\nConnection: close\r\nContent-Length: {}\r\n{}\r\n",
                    exchange.status,
                    exchange.body.len(),
                    exchange.response_headers,
                );
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.write_all(exchange.body).await.unwrap();
            }
        });
        let config = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "test", "test", None, None, "test",
            ))
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .force_path_style(true)
            .endpoint_url(format!("http://{place}"))
            .build();
        (
            S3KeyService {
                client: Client::from_conf(config),
                bucket: "test-vault".into(),
                reads: IoRegion::new(),
            },
            service,
        )
    }

    fn exchange(method: &'static str, status: &'static str, body: &'static [u8]) -> Exchange {
        Exchange {
            method,
            request_header: "",
            path: None,
            status,
            response_headers: "",
            body,
        }
    }

    fn get(body: &'static [u8]) -> Exchange {
        Exchange {
            response_headers: ENCRYPTED,
            ..exchange("GET", "200 OK", body)
        }
    }

    #[tokio::test]
    async fn the_vault_exposes_its_namespace_and_deletion_signing_identity() {
        let (vault, service) = mock_s3(vec![
            Exchange {
                path: Some("/metadata/.rtdb-key-namespace"),
                ..get(LIVE)
            },
            Exchange {
                path: Some("/metadata/.rtdb-deletion-signing-seed"),
                ..get(LIVE)
            },
        ])
        .await;
        assert_eq!(vault.namespace_id().await.unwrap(), *LIVE);
        let expected = super::super::verification_key(*LIVE).unwrap();
        assert_eq!(vault.deletion_verification_key().await.unwrap(), expected);
        service.await.unwrap();
    }

    #[tokio::test]
    async fn erasure_retries_a_competing_create_and_refuses_reinstallation() {
        let (vault, service) = mock_s3(vec![
            exchange("VERSION", "200 OK", NEVER_VERSIONED),
            exchange("GET", "404 Not Found", ABSENT),
            Exchange {
                request_header: "if-none-match: *",
                ..exchange(
                    "PUT",
                    "412 Precondition Failed",
                    b"<Error><Code>PreconditionFailed</Code></Error>",
                )
            },
            get(LIVE),
            Exchange {
                request_header: "if-match: \"tag\"",
                ..exchange("PUT", "200 OK", b"")
            },
            get(b""),
            exchange("VERSION", "200 OK", NEVER_VERSIONED),
            Exchange {
                request_header: "if-none-match: *",
                ..exchange(
                    "PUT",
                    "412 Precondition Failed",
                    b"<Error><Code>PreconditionFailed</Code></Error>",
                )
            },
            get(b""),
        ])
        .await;
        assert_eq!(vault.custody_namespace().unwrap(), "s3:test-vault");
        vault.require_never_versioned().await.unwrap();
        vault.destroy(ID).await.unwrap();
        assert_eq!(
            vault.install(ID, [3; KEY_BYTES]).await.unwrap_err().code,
            "rtdb.multi/key-destroyed"
        );
        service.await.unwrap();
    }

    #[tokio::test]
    async fn assertion_capabilities_have_a_separate_conditional_withdrawal_route() {
        let version = || exchange("VERSION", "200 OK", NEVER_VERSIONED);
        let assertion = |exchange| Exchange { path: Some(ASSERTION_PATH), ..exchange };
        let (vault, service) = mock_s3(vec![
            version(),
            assertion(get(LIVE)),
            version(),
            assertion(get(LIVE)),
            assertion(Exchange {
                request_header: "if-match: \"tag\"",
                ..exchange("PUT", "200 OK", b"")
            }),
            assertion(get(b"")),
            version(),
            version(),
            assertion(get(b"")),
            version(),
            assertion(Exchange {
                request_header: "if-none-match: *",
                ..exchange(
                    "PUT", "412 Precondition Failed",
                    b"<Error><Code>PreconditionFailed</Code></Error>",
                )
            }),
            assertion(get(b"")),
        ]).await;
        let stage = AssertionStage::new(
            "00000000-0000-0000-0000-00000000abcd".parse().unwrap(),
            std::num::NonZeroU64::new(17).unwrap(),
            "00000000-0000-0000-0000-000000001234".parse().unwrap(),
        ).unwrap();
        assert!(vault.existing_key(&stage.key_id()).await.is_err());
        let authority = vault.assertion_keys().unwrap();
        let region = IoRegion::new();
        let plaintext = Bytes::from_static(b"retained assertion");
        let envelope = AssertionEnvelope::seal(stage, LIVE, &plaintext).unwrap();
        assert_eq!(authority.open_assertion_stage(stage, &region, envelope.clone()).await.unwrap(), plaintext);
        authority.retire_assertion_stage(stage, &region).await.unwrap();
        assert_eq!(
            authority.open_assertion_stage(stage, &region, envelope).await.unwrap_err().code,
            "rtdb/erased-under-view",
        );
        assert_eq!(
            authority.seal_assertion_stage(stage, &region, plaintext).await.unwrap_err().code,
            "rtdb.multi/assertion-stage-used",
        );
        service.await.unwrap();
    }

    #[tokio::test]
    async fn never_confirms_withdrawal_through_versioned_storage() {
        let (vault, service) = mock_s3(vec![
            exchange(
                "VERSION",
                "200 OK",
                b"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>",
            ),
            Exchange {
                response_headers: "x-amz-version-id: null\r\n",
                ..get(b"")
            },
        ])
        .await;
        assert_eq!(
            vault.require_never_versioned().await.unwrap_err().code,
            "rtdb.multi/key-vault-versioned"
        );
        assert_eq!(
            vault.destroy(ID).await.unwrap_err().code,
            "rtdb.multi/key-vault-versioned"
        );
        service.await.unwrap();
    }
}
