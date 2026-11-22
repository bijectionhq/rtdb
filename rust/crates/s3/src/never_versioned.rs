use aws_sdk_s3::primitives::ByteStream;
use object_store::path::Path;
use rtdb_storage::{ObjectKey, remote::S3_RETIREMENT_ATTRIBUTE};
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
                let tag = reply.e_tag().filter(|tag| strong_tag(tag)).ok_or_else(|| {
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

    pub async fn retire(
        &self,
        key: &ObjectKey,
        target: &S3TombstoneTarget,
        token: Uuid,
    ) -> Result<()> {
        let path = self.path(key);
        if token.is_nil()
            || target.namespace != self.inventory.namespace
            || target.key != path.as_ref()
            || matches!(&target.condition, S3TombstoneCondition::Matches(tag) if !strong_tag(tag))
        {
            return Err(Anomaly::incorrect(
                "rtdb.s3/tombstone-target",
                "The retirement is not bound to this physical object",
            ));
        }
        self.require_never_versioned().await?;
        match self.observe(key).await? {
            NeverVersionedObject::Retired(current) if current == token => {
                return self.require_never_versioned().await;
            }
            NeverVersionedObject::Open(current) if current == *target => {}
            _ => return Err(changed()),
        }
        let request = self
            .inventory
            .client
            .put_object()
            .bucket(&self.inventory.namespace.bucket)
            .key(path.as_ref())
            .set_request_payer(self.inventory.request_payer.clone())
            .body(ByteStream::from_static(b""))
            .metadata(S3_RETIREMENT_ATTRIBUTE, token.simple().to_string());
        let request = match &target.condition {
            S3TombstoneCondition::Absent => request.if_none_match("*"),
            S3TombstoneCondition::Matches(tag) => request.if_match(tag),
        };
        let reply = request.send().await;
        if reply
            .as_ref()
            .is_ok_and(|reply| reply.version_id().is_some())
        {
            return Err(unavailable("S3 versioned the retirement tombstone"));
        }
        match self.observe(key).await? {
            NeverVersionedObject::Retired(current) if current == token => {
                self.require_never_versioned().await
            }
            NeverVersionedObject::Open(current) if current == *target => {
                let message = reply.err().map_or_else(
                    || "S3 has not exposed the retirement tombstone".to_owned(),
                    |error| format!("Writing the retirement tombstone did not settle: {error}"),
                );
                Err(unavailable(message))
            }
            _ => Err(changed()),
        }
    }
}

fn strong_tag(tag: &str) -> bool {
    tag.strip_prefix('"')
        .and_then(|tag| tag.strip_suffix('"'))
        .is_some_and(|value| {
            !value.is_empty()
                && value != "*"
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'"' | b'\\' | b','))
        })
}

fn unavailable(message: impl Into<String>) -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb.s3/tombstone-pending",
        message,
    )
}

fn changed() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb.s3/tombstone-target-changed",
        "The current object differs from the retired publication",
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
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
                let path = request.next().unwrap().trim_end_matches('=');
                assert_eq!(path, expected.path);
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
                if expected.method == "PUT" {
                    assert_eq!(headers.get("content-length").map(String::as_str), Some("0"));
                    assert_ne!(
                        headers.contains_key("if-match"),
                        headers.contains_key("if-none-match")
                    );
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

    fn put(
        status: &'static str,
        conditions: &'static [(&'static str, &'static str)],
    ) -> Exchange<'static> {
        Exchange {
            method: "PUT",
            path: "/cell/cell/owner/data?x-id=PutObject",
            conditions,
            status,
            headers: "",
            body: "",
        }
    }

    #[tokio::test]
    async fn a_tombstone_reply_requires_a_fresh_observation_and_the_exact_retirement_token() {
        let token = Uuid::from_u128(1);
        let live = || head("200 OK", "Content-Length: 7\r\nETag: \"original\"\r\n");
        let tombstone = || {
            head(
                "200 OK",
                "Content-Length: 0\r\nx-amz-meta-rtdb-retirement: 00000000000000000000000000000001\r\n",
            )
        };
        let write = |status| {
            put(
                status,
                &[
                    ("if-match", "\"original\""),
                    (
                        "x-amz-meta-rtdb-retirement",
                        "00000000000000000000000000000001",
                    ),
                ],
            )
        };
        let (inventory, server) = fixture(vec![
            profile(BUCKET_CHECK),
            live(),
            profile(BUCKET_CHECK),
            live(),
            write("200 OK"),
            live(),
            profile(BUCKET_CHECK),
            live(),
            write("412 Precondition Failed"),
            tombstone(),
            profile(BUCKET_CHECK),
            profile(BUCKET_CHECK),
            tombstone(),
            profile(BUCKET_CHECK),
            profile(BUCKET_CHECK),
            tombstone(),
        ])
        .await;
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
                    .retire(&wrong_key, &target, token)
                    .await
                    .unwrap_err()
                    .code,
                "rtdb.s3/tombstone-target"
            );
            assert_eq!(
                retirement
                    .retire(&key, &target, token)
                    .await
                    .unwrap_err()
                    .code,
                "rtdb.s3/tombstone-pending"
            );
            retirement.retire(&key, &target, token).await.unwrap();
            retirement.retire(&key, &target, token).await.unwrap();
            assert_eq!(
                retirement
                    .retire(&key, &target, Uuid::from_u128(2))
                    .await
                    .unwrap_err()
                    .code,
                "rtdb.s3/tombstone-target-changed"
            );
            server.await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_missing_key_needs_a_conditional_tombstone_and_preserves_a_competing_create() {
        let token = Uuid::from_u128(1);
        let missing = || head("404 Not Found", "Content-Length: 0\r\n");
        let (inventory, server) = fixture(vec![
            profile(BUCKET_CHECK),
            missing(),
            profile(BUCKET_CHECK),
            missing(),
            put(
                "412 Precondition Failed",
                &[
                    ("if-none-match", "*"),
                    (
                        "x-amz-meta-rtdb-retirement",
                        "00000000000000000000000000000001",
                    ),
                ],
            ),
            head("200 OK", "Content-Length: 11\r\nETag: \"replacement\"\r\n"),
        ])
        .await;
        let key = ObjectKey::new("owner/data").unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            let retirement = inventory.never_versioned_retirement().await.unwrap();
            let NeverVersionedObject::Open(target) = retirement.observe(&key).await.unwrap() else {
                panic!("expected a missing key")
            };
            assert_eq!(target.condition, S3TombstoneCondition::Absent);
            assert_eq!(
                retirement
                    .retire(&key, &target, token)
                    .await
                    .unwrap_err()
                    .code,
                "rtdb.s3/tombstone-target-changed"
            );
            server.await.unwrap();
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn a_tombstone_cannot_settle_if_bucket_versioning_changes() {
        let (inventory, server) = fixture(vec![
            profile(BUCKET_CHECK), head("200 OK", "Content-Length: 7\r\nETag: \"original\"\r\n"),
            profile(BUCKET_CHECK), head("200 OK", "Content-Length: 7\r\nETag: \"original\"\r\n"),
            put("200 OK", &[("if-match", "\"original\"")]),
            head("200 OK", "Content-Length: 0\r\nx-amz-meta-rtdb-retirement: 00000000000000000000000000000001\r\n"),
            profile("<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>"),
            profile("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"),
        ]).await;
        let key = ObjectKey::new("owner/data").unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            let retirement = inventory.never_versioned_retirement().await.unwrap();
            let NeverVersionedObject::Open(target) = retirement.observe(&key).await.unwrap() else {
                panic!("expected a publication")
            };
            assert_eq!(
                retirement
                    .retire(&key, &target, Uuid::from_u128(1))
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
