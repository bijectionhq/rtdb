use aws_sdk_s3::types::ChecksumAlgorithm;
use aws_sdk_s3::{error::ProvideErrorMetadata, primitives::ByteStream};
use bytes::Bytes;
use object_store::path::Path;
use rtdb_storage::{
    ObjectKey,
    remote::{PublicationResult, S3_RETIREMENT_ATTRIBUTE, strong_entity_tag},
};
use rtdb_types::{Anomaly, AnomalyCategory, Result, S3TombstoneCondition, S3TombstoneTarget};
use uuid::Uuid;

use super::VersionedInventory;

pub enum NeverVersionedObject {
    Open(S3TombstoneTarget),
    Retired(Uuid),
}

/// The caller must hold the residence's retirement authority and enforce its
/// bucket policy for the full lifetime of every admitted publication.
pub struct NeverVersionedRetirement<'a> {
    inventory: &'a VersionedInventory,
}

impl VersionedInventory {
    pub async fn never_versioned_retirement(&self) -> Result<NeverVersionedRetirement<'_>> {
        let retirement = NeverVersionedRetirement { inventory: self };
        retirement.require_never_versioned().await?;
        Ok(retirement)
    }
}

impl NeverVersionedRetirement<'_> {
    async fn require_never_versioned(&self) -> Result<()> {
        let reply = self
            .inventory
            .client
            .get_bucket_versioning()
            .bucket(&self.inventory.namespace.bucket)
            .send()
            .await
            .map_err(|error| unavailable(format!("Checking bucket versioning failed: {error}")))?;
        if reply.status().is_some() || reply.mfa_delete().is_some() {
            return Err(unavailable(
                "The serving bucket has a versioning configuration",
            ));
        }
        Ok(())
    }

    fn path(&self, key: &ObjectKey) -> Path {
        Path::from(format!("{}/{key}", self.inventory.root))
    }

    pub async fn observe(&self, key: &ObjectKey) -> Result<NeverVersionedObject> {
        let path = self.path(key);
        let reply = self
            .inventory
            .client
            .head_object()
            .bucket(&self.inventory.namespace.bucket)
            .key(path.as_ref())
            .set_request_payer(self.inventory.request_payer.clone())
            .send()
            .await;
        let condition = match reply {
            Ok(reply) => {
                if reply.version_id().is_some() || reply.delete_marker().unwrap_or(false) {
                    return Err(unavailable(
                        "S3 returned a versioned object for the serving bucket",
                    ));
                }
                if let Some(marker) = reply
                    .metadata()
                    .and_then(|metadata| metadata.get(S3_RETIREMENT_ATTRIBUTE))
                {
                    let token = Uuid::parse_str(marker).ok().filter(|token| !token.is_nil());
                    return match (reply.content_length(), token) {
                        (Some(0), Some(token)) => Ok(NeverVersionedObject::Retired(token)),
                        _ => Err(unavailable("S3 returned a malformed retirement tombstone")),
                    };
                }
                if !reply.content_length().is_some_and(|length| length > 0) {
                    return Err(unavailable(
                        "The publication does not have a positive content length",
                    ));
                }
                let tag = reply
                    .e_tag()
                    .filter(|tag| strong_entity_tag(tag))
                    .ok_or_else(|| {
                        unavailable("The publication does not have a strong entity tag")
                    })?;
                S3TombstoneCondition::Matches(tag.to_owned())
            }
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|error| error.is_not_found())
                    && error.raw_response().is_some_and(|response| {
                        response.status().as_u16() == 404
                            && response.headers().get("x-amz-delete-marker").is_none()
                            && response.headers().get("x-amz-version-id").is_none()
                    }) =>
            {
                S3TombstoneCondition::Absent
            }
            Err(error) => {
                return Err(unavailable(format!(
                    "Reading the retirement target failed: {error}"
                )));
            }
        };
        Ok(NeverVersionedObject::Open(S3TombstoneTarget {
            namespace: self.inventory.namespace.clone(),
            key: path.to_string(),
            condition,
        }))
    }

    fn checked_path(&self, key: &ObjectKey, target: &S3TombstoneTarget) -> Result<Path> {
        let path = self.path(key);
        if target.namespace != self.inventory.namespace
            || target.key != path.as_ref()
            || matches!(&target.condition, S3TombstoneCondition::Matches(tag) if !strong_entity_tag(tag))
        {
            return Err(Anomaly::incorrect(
                "rtdb.s3/tombstone-target",
                "The retirement is bound to another physical object",
            ));
        }
        Ok(path)
    }

    /// Permanently fences a key whose publication path uses conditional create.
    /// A single provider write leaves no multipart staging to reconcile.
    pub async fn fence_create_only(&self, key: &ObjectKey, token: Uuid) -> Result<()> {
        if token.is_nil() {
            return Err(unavailable(
                "The terminal write needs an allocation identity",
            ));
        }
        let target = match self.observe(key).await? {
            NeverVersionedObject::Retired(previous) if previous == token => return Ok(()),
            NeverVersionedObject::Retired(_) => {
                return Err(unavailable("Another allocation retired the object"));
            }
            NeverVersionedObject::Open(target) => target,
        };
        let path = self.checked_path(key, &target)?;
        let request = self
            .inventory
            .client
            .put_object()
            .bucket(&self.inventory.namespace.bucket)
            .key(path.as_ref())
            .metadata(S3_RETIREMENT_ATTRIBUTE, token.simple().to_string())
            .body(ByteStream::from(Bytes::new()))
            .set_request_payer(self.inventory.request_payer.clone());
        let request = match target.condition {
            S3TombstoneCondition::Absent => request.if_none_match("*"),
            S3TombstoneCondition::Matches(tag) => request.if_match(tag),
        };
        match request.send().await {
            Ok(reply) if reply.version_id().is_some() => {
                return Err(unavailable("S3 versioned the retirement tombstone"));
            }
            Ok(_) => {}
            Err(error)
                if error.as_service_error().and_then(|error| error.code())
                    == Some("PreconditionFailed") => {}
            Err(error) => {
                return Err(unavailable(format!(
                    "Installing the terminal object fence failed: {error}"
                )));
            }
        }
        if !self.confirms(key, token).await? {
            return Err(unavailable(
                "The terminal object fence has not been confirmed",
            ));
        }
        Ok(())
    }

    pub async fn reserve(
        &self,
        key: &ObjectKey,
        target: &S3TombstoneTarget,
        token: Uuid,
    ) -> Result<String> {
        let path = self.checked_path(key, target)?;
        if token.is_nil() {
            return Err(Anomaly::incorrect(
                "rtdb.s3/tombstone-target",
                "The retirement needs an identity",
            ));
        }
        let reply = self
            .inventory
            .client
            .create_multipart_upload()
            .bucket(&self.inventory.namespace.bucket)
            .key(path.as_ref())
            .metadata(S3_RETIREMENT_ATTRIBUTE, token.simple().to_string())
            .checksum_algorithm(ChecksumAlgorithm::Sha256)
            .set_request_payer(self.inventory.request_payer.clone())
            .send()
            .await
            .map_err(|error| {
                unavailable(format!("Reserving the retirement upload failed: {error}"))
            })?;
        reply
            .upload_id()
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| unavailable("S3 did not identify the retirement upload"))
    }

    pub async fn complete(
        &self,
        key: &ObjectKey,
        target: &S3TombstoneTarget,
        upload: &str,
    ) -> Result<()> {
        let path = self.checked_path(key, target)?;
        self.require_never_versioned().await?;
        match self
            .inventory
            .complete_conditional(&path, upload, Bytes::new(), &target.condition)
            .await?
        {
            PublicationResult::Published { version: None, .. } | PublicationResult::Conflict => {
                Ok(())
            }
            PublicationResult::Published {
                version: Some(_), ..
            } => Err(unavailable("S3 versioned the retirement tombstone")),
        }
    }

    pub async fn drain(
        &self,
        key: &ObjectKey,
        target: &S3TombstoneTarget,
        upload: &str,
    ) -> Result<()> {
        let path = self.checked_path(key, target)?;
        self.inventory.abort_upload(path.as_ref(), upload).await
    }

    pub async fn confirms(&self, key: &ObjectKey, token: Uuid) -> Result<bool> {
        if !matches!(self.observe(key).await?, NeverVersionedObject::Retired(current) if current == token)
        {
            return Ok(false);
        }
        self.require_never_versioned().await?;
        Ok(true)
    }
}

fn unavailable(message: impl Into<String>) -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.s3/tombstone-pending",
        message,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::TcpListener;

    use super::*;
    use crate::{S3Config, S3Credentials};

    const BUCKET_CHECK: &str =
        "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"/>";
    const KEY_PATH: &str = "/cell/cell/owner/data";

    struct Exchange<'a> {
        method: &'a str,
        path: &'a str,
        conditions: &'a [(&'a str, &'a str)],
        status: &'a str,
        headers: &'a str,
        body: &'a str,
    }

    async fn fixture(
        exchanges: Vec<Exchange<'static>>,
    ) -> (VersionedInventory, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = S3Config::new("cell");
        config.endpoint = Some(format!("http://{}", listener.local_addr().unwrap()));
        config.region = Some("us-east-1".to_owned());
        config.path_style_access_enabled = true;
        config.credentials = Some(S3Credentials {
            access_key: "test".to_owned(),
            secret_key: "test-secret".to_owned(),
        });
        let inventory = config
            .versioned_inventory(&ObjectKey::new("cell").unwrap(), &BTreeMap::new())
            .await
            .unwrap();
        let server = tokio::spawn(async move {
            for expected in exchanges {
                let (socket, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(socket);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let mut request = line.split_whitespace();
                assert_eq!(request.next(), Some(expected.method));
                let address =
                    url::Url::parse(&format!("http://fixture{}", request.next().unwrap())).unwrap();
                let wanted = url::Url::parse(&format!("http://fixture{}", expected.path)).unwrap();
                assert_eq!(address.path(), wanted.path());
                let query = address
                    .query_pairs()
                    .filter(|(key, _)| key != "x-id")
                    .collect::<BTreeMap<_, _>>();
                assert_eq!(query, wanted.query_pairs().collect::<BTreeMap<_, _>>());
                let mut headers = BTreeMap::new();
                loop {
                    line.clear();
                    assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                    let (key, value) = line.split_once(':').unwrap();
                    headers.insert(key.to_lowercase(), value.trim().to_owned());
                }
                for (name, value) in expected.conditions {
                    assert_eq!(headers.get(*name).map(String::as_str), Some(*value));
                }
                if let Some(length) = headers.get("content-length") {
                    let mut body = vec![0; length.parse().unwrap()];
                    reader.read_exact(&mut body).await.unwrap();
                    if expected.method == "PUT" {
                        assert!(body.is_empty());
                    }
                }
                let size = if expected.method == "HEAD" {
                    String::new()
                } else {
                    format!("Content-Length: {}\r\n", expected.body.len())
                };
                let response = format!(
                    "HTTP/1.1 {}\r\nConnection: close\r\nContent-Type: application/xml\r\n{size}{}\r\n{}",
                    expected.status, expected.headers, expected.body
                );
                reader
                    .get_mut()
                    .write_all(response.as_bytes())
                    .await
                    .unwrap();
            }
        });
        (inventory, server)
    }

    fn profile(body: &'static str) -> Exchange<'static> {
        Exchange {
            method: "GET",
            path: "/cell/?versioning",
            conditions: &[],
            status: "200 OK",
            headers: "",
            body,
        }
    }

    fn head(status: &'static str, headers: &'static str) -> Exchange<'static> {
        Exchange {
            method: "HEAD",
            path: KEY_PATH,
            conditions: &[],
            status,
            headers,
            body: "",
        }
    }

    fn reserve() -> Exchange<'static> {
        Exchange {
            method: "POST",
            path: "/cell/cell/owner/data?uploads",
            status: "200 OK",
            headers: "",
            conditions: &[(
                "x-amz-meta-rtdb-retirement",
                "00000000000000000000000000000001",
            )],
            body: "<InitiateMultipartUploadResult><UploadId>one</UploadId></InitiateMultipartUploadResult>",
        }
    }

    fn part() -> Exchange<'static> {
        Exchange {
            method: "PUT",
            path: "/cell/cell/owner/data?partNumber=1&uploadId=one",
            status: "200 OK",
            conditions: &[],
            headers: "ETag: \"part\"\r\nx-amz-checksum-sha256: 47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=\r\n",
            body: "",
        }
    }

    fn complete(
        status: &'static str,
        header: &'static [(&'static str, &'static str)],
        body: &'static str,
    ) -> Exchange<'static> {
        Exchange {
            method: "POST",
            path: "/cell/cell/owner/data?uploadId=one",
            status,
            conditions: header,
            headers: "",
            body,
        }
    }

    fn drain() -> [Exchange<'static>; 2] {
        [
            Exchange {
                method: "DELETE",
                path: "/cell/cell/owner/data?uploadId=one",
                conditions: &[],
                status: "204 No Content",
                headers: "",
                body: "",
            },
            Exchange {
                method: "GET",
                path: "/cell/cell/owner/data?uploadId=one&max-parts=1",
                conditions: &[],
                status: "404 Not Found",
                headers: "",
                body: "<Error><Code>NoSuchUpload</Code></Error>",
            },
        ]
    }

    #[tokio::test]
    async fn conditional_completion_is_scoped_to_the_reserved_upload_and_requires_a_token_observation()
     {
        let token = Uuid::from_u128(1);
        let live = || head("200 OK", "Content-Length: 7\r\nETag: \"original\"\r\n");
        let tombstone = || {
            head(
                "200 OK",
                "Content-Length: 0\r\nx-amz-meta-rtdb-retirement: 00000000000000000000000000000001\r\n",
            )
        };
        let (inventory, server) = fixture(Vec::from([
            profile(BUCKET_CHECK), live(), reserve(), profile(BUCKET_CHECK), part(),
            complete("200 OK", &[("if-match", "\"original\"")], "<CompleteMultipartUploadResult><ETag>new</ETag></CompleteMultipartUploadResult>"),
        ]).into_iter().chain(drain()).chain([
            live(), tombstone(), profile(BUCKET_CHECK), tombstone(),
        ]).collect()).await;
        let key = ObjectKey::new("owner/data").unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            let retirement = inventory.never_versioned_retirement().await.unwrap();
            let NeverVersionedObject::Open(target) = retirement.observe(&key).await.unwrap() else {
                panic!("expected a publication")
            };
            assert_eq!(
                target.condition,
                S3TombstoneCondition::Matches("\"original\"".into())
            );
            let wrong_key = ObjectKey::new("owner/other").unwrap();
            assert_eq!(
                retirement
                    .reserve(&wrong_key, &target, token)
                    .await
                    .unwrap_err()
                    .code,
                "rtdb.s3/tombstone-target"
            );
            let upload = retirement.reserve(&key, &target, token).await.unwrap();
            assert_eq!(upload, "one");
            retirement.complete(&key, &target, &upload).await.unwrap();
            retirement.drain(&key, &target, &upload).await.unwrap();
            assert!(!retirement.confirms(&key, token).await.unwrap());
            assert!(retirement.confirms(&key, token).await.unwrap());
            assert!(!retirement.confirms(&key, Uuid::from_u128(2)).await.unwrap());
            server.await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_missing_key_completion_preserves_a_competing_create() {
        let token = Uuid::from_u128(1);
        let (inventory, server) = fixture(
            Vec::from([
                profile(BUCKET_CHECK),
                head("404 Not Found", "Content-Length: 0\r\n"),
                reserve(),
                profile(BUCKET_CHECK),
                part(),
                complete(
                    "412 Precondition Failed",
                    &[("if-none-match", "*")],
                    "<Error><Code>PreconditionFailed</Code></Error>",
                ),
            ])
            .into_iter()
            .chain(drain())
            .chain([head(
                "200 OK",
                "Content-Length: 11\r\nETag: \"replacement\"\r\n",
            )])
            .collect(),
        )
        .await;
        let key = ObjectKey::new("owner/data").unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            let retirement = inventory.never_versioned_retirement().await.unwrap();
            let NeverVersionedObject::Open(target) = retirement.observe(&key).await.unwrap() else {
                panic!("expected a missing key")
            };
            assert_eq!(target.condition, S3TombstoneCondition::Absent);
            let upload = retirement.reserve(&key, &target, token).await.unwrap();
            retirement.complete(&key, &target, &upload).await.unwrap();
            retirement.drain(&key, &target, &upload).await.unwrap();
            assert!(!retirement.confirms(&key, token).await.unwrap());
            server.await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_tombstone_cannot_settle_if_bucket_versioning_changes() {
        let (inventory, server) = fixture(vec![
            profile(BUCKET_CHECK),
            head("200 OK", "Content-Length: 0\r\nx-amz-meta-rtdb-retirement: 00000000000000000000000000000001\r\n"),
            profile("<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>"),
            profile("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"),
        ]).await;
        let key = ObjectKey::new("owner/data").unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            let retirement = inventory.never_versioned_retirement().await.unwrap();
            assert_eq!(
                retirement
                    .confirms(&key, Uuid::from_u128(1))
                    .await
                    .unwrap_err()
                    .code,
                "rtdb.s3/tombstone-pending"
            );
            assert!(inventory.never_versioned_retirement().await.is_err());
            server.await.unwrap();
        })
        .await
        .unwrap();
    }
}
