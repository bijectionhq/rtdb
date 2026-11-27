use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use aws_credential_types::Credentials;
use aws_credential_types::provider::{ProvideCredentials, SharedCredentialsProvider, error::CredentialsError, future};
use aws_sdk_sts::operation::assume_role::builders::AssumeRoleFluentBuilder;
use rtdb_storage::{IoRegion, ObjectKey, ObjectStore, remote::RemoteStore};
use rtdb_types::{Anomaly, Result};
use serde_json::json;

use crate::{S3Authorization, S3Config};

pub const SCOPED_S3_WRITE_TTL: Duration = Duration::from_secs(15 * 60);
pub const SCOPED_S3_CLOCK_ALLOWANCE: Duration = Duration::from_secs(1);

#[async_trait]
pub trait WriteAuthorization: Send + Sync {
    /// Confirms authorization and records the session expiry when STS has supplied one.
    async fn authorize(&self, expires: Option<SystemTime>) -> Result<()>;
}

/// The STS configuration shared by a node's deployment stores.
pub struct ScopedS3Writes {
    role: String,
    partition: String,
    sts: aws_sdk_sts::Client,
    region: Option<String>,
    require_conditional_put: bool,
}

impl ScopedS3Writes {
    pub async fn new(role: &str, region: Option<&str>, sts_endpoint: Option<&str>, require_conditional_put: bool) -> Result<Self> {
        let parts: Vec<_> = role.splitn(6, ':').collect();
        let ["arn", partition, "iam", _, _, resource] = parts.as_slice() else { return Err(invalid_role()); };
        if partition.is_empty() || !partition.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            || resource.strip_prefix("role/").is_none_or(str::is_empty) {
            return Err(invalid_role());
        }
        let mut configuration = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = region { configuration = configuration.region(aws_config::Region::new(region.to_owned())); }
        if let Some(endpoint) = sts_endpoint {
            let endpoint = url::Url::parse(endpoint).map_err(|_| invalid_endpoint())?;
            if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() || !endpoint.username().is_empty()
                || endpoint.password().is_some() || endpoint.query().is_some() || endpoint.fragment().is_some() {
                return Err(invalid_endpoint());
            }
            configuration = configuration.endpoint_url(endpoint.to_string());
        }
        let sdk = configuration.load().await;
        let region = sdk.region().map(|region| region.as_ref().to_owned());
        Ok(Self { role: role.to_owned(), partition: if *partition == "minio" { "aws" } else { *partition }.to_owned(),
            sts: aws_sdk_sts::Client::new(&sdk), region, require_conditional_put })
    }

    pub async fn open(&self, storage: &S3Config, cell_root: &ObjectKey, root: &ObjectKey, authorization: Arc<dyn WriteAuthorization>,
                io: Arc<IoRegion>) -> Result<Arc<dyn ObjectStore>> {
        Ok(self.open_remote(storage, cell_root, root, authorization, io).await?)
    }

    pub async fn open_remote(&self, storage: &S3Config, cell_root: &ObjectKey, root: &ObjectKey, authorization: Arc<dyn WriteAuthorization>,
                io: Arc<IoRegion>) -> Result<Arc<RemoteStore>> {
        storage.require_cell_root(cell_root)?;
        let terminal_namespace = if storage.terminal_scope.is_some() {
            if !self.require_conditional_put || root == cell_root || !root.is_under(cell_root) {
                return Err(Anomaly::incorrect("rtdb.s3/write-scope", "A terminal writer requires conditional creation below its enrolled cell root"));
            }
            let remotes = std::collections::BTreeMap::new();
            storage.open_cell(cell_root, &remotes, IoRegion::new()).await?.s3_namespace().cloned()
        } else {
            None
        };
        let provider = self.provider(storage, root, authorization)?;
        let writer = storage.open_authorized(storage.object_prefix(cell_root)?, S3Authorization::Rotating { provider, region: self.region.clone() }, io, crate::StoreKind::Cell)?;
        if let Some(expected) = &terminal_namespace && writer.s3_namespace() != Some(expected) {
            return Err(Anomaly::incorrect("rtdb.s3/write-scope", "The writer credentials resolve to a different S3 namespace"));
        }
        Ok(writer)
    }

    fn provider(&self, storage: &S3Config, root: &ObjectKey, authorization: Arc<dyn WriteAuthorization>) -> Result<SharedCredentialsProvider> {
        let policy = write_policy(storage, root, &self.partition, self.require_conditional_put)?;
        let request = self.sts.assume_role().role_arn(&self.role).role_session_name("rtdb-deployment-write")
            .duration_seconds(SCOPED_S3_WRITE_TTL.as_secs() as i32).policy(policy);
        Ok(SharedCredentialsProvider::new(AuthorizedRole { request, authorization }))
    }
}

fn invalid_endpoint() -> Anomaly {
    Anomaly::incorrect("rtdb.s3/sts-endpoint", "The STS endpoint must be an HTTP URL without embedded credentials, query, or fragment")
}

fn invalid_role() -> Anomaly {
    Anomaly::incorrect("rtdb.s3/write-role", "The write role must be an IAM role ARN")
}

fn write_policy(storage: &S3Config, root: &ObjectKey, partition: &str, conditional: bool) -> Result<String> {
    let prefix = object_store::path::Path::from(storage.object_prefix(root)?.as_str()).to_string();
    if storage.bucket.is_empty() || !storage.bucket.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.'))
        || prefix.contains(['*', '?']) || prefix.contains("${") {
        return Err(Anomaly::incorrect("rtdb.s3/write-scope", "The S3 root must identify a literal bucket and prefix"));
    }
    let objects = format!("arn:{partition}:s3:::{}/{prefix}/*", storage.bucket);
    let mut statements = vec![
        json!({"Effect": "Allow", "Action": ["s3:GetObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"], "Resource": objects}),
        json!({"Effect": "Deny", "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion"], "Resource": "*"}),
        json!({"Effect": "Deny", "Action": "s3:PutObject", "NotResource": objects}),
    ];
    if conditional {
        statements.push(json!({"Effect": "Allow", "Action": "s3:PutObject", "Resource": objects,
            "Condition": {"StringEquals": {"s3:if-none-match": "*"}}}));
        // S3 reserves and uploads parts without object-creation headers.
        // The conditional header still applies when a complete object can become visible.
        statements.push(json!({"Effect": "Allow", "Action": "s3:PutObject", "Resource": objects,
            "Condition": {"Bool": {"s3:ObjectCreationOperation": "false"}}}));
        statements.push(json!({"Effect": "Deny", "Action": "s3:PutObject", "Resource": objects,
            "Condition": {"Bool": {"s3:ObjectCreationOperation": "true"},
                          "StringNotEqualsIfExists": {"s3:if-none-match": "*"}}}));
    } else {
        // MinIO supports a session prefix but rejects the S3 conditional-write policy key.
        statements.push(json!({"Effect": "Allow", "Action": "s3:PutObject", "Resource": objects}));
    }
    Ok(json!({"Version": "2012-10-17", "Statement": statements}).to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_role_policy_matches_the_encoded_scope_and_requires_a_conditional_put() {
        let mut storage = S3Config::new("rtdb-test");
        storage.prefix = Some("fixture/~a".into());
        let root = ObjectKey::new("cell/deployment/v06").unwrap();
        let policy: serde_json::Value = serde_json::from_str(&write_policy(&storage, &root, "aws", true).unwrap()).unwrap();
        let scope = "arn:aws:s3:::rtdb-test/fixture/%7Ea/cell/deployment/v06/*";
        let statements = policy["Statement"].as_array().unwrap();
        assert!(statements.iter().any(|statement| statement["Effect"] == "Allow" && statement["Action"] == "s3:PutObject"
            && statement["Resource"] == scope && statement["Condition"]["StringEquals"]["s3:if-none-match"] == "*"));
        assert!(statements.iter().any(|statement| statement["Effect"] == "Deny" && statement["NotResource"] == scope));
        assert!(statements.iter().any(|statement| statement["Effect"] == "Deny"
            && statement["Action"] == json!(["s3:DeleteObject", "s3:DeleteObjectVersion"])));
    }

    #[cfg(feature = "versioning")]
    #[tokio::test]
    async fn minio_restricts_the_assumed_session_to_its_deployment_and_denies_deletes() {
        use aws_sdk_s3::{error::ProvideErrorMetadata, primitives::ByteStream};

        let Ok(endpoint) = std::env::var("RTDB_S3_ENDPOINT") else { return; };
        let endpoint = url::Url::parse(&endpoint).unwrap();
        if !matches!(endpoint.host(), Some(url::Host::Ipv4(host)) if host.is_loopback()) { return; }
        let role = ScopedS3Writes::new("arn:aws:iam::123456789012:role/rtdb-scope-test", Some("us-east-1"), Some(endpoint.as_str()), false).await.unwrap();
        let sdk = aws_config::defaults(aws_config::BehaviorVersion::latest()).region(aws_config::Region::new("us-east-1")).load().await;
        let client = aws_sdk_s3::config::Builder::from(&sdk).endpoint_url(endpoint.as_str()).force_path_style(true);
        let admin = aws_sdk_s3::Client::from_conf(client.clone().build());
        let mut storage = S3Config::new(format!("rtdb-scoped-{}", uuid::Uuid::new_v4().simple()));
        storage.prefix = Some("encoded/~name".to_owned());
        let root = ObjectKey::new("cell/deployment/v06").unwrap();
        let own = format!("{}/value", object_store::path::Path::from(storage.object_prefix(&root).unwrap().as_str()));
        let sibling = own.replace("/deployment/", "/deployment-neighbor/");
        admin.create_bucket().bucket(&storage.bucket).send().await.unwrap();

        struct Permit;
        #[async_trait]
        impl WriteAuthorization for Permit { async fn authorize(&self, _: Option<SystemTime>) -> Result<()> { Ok(()) } }
        let provider = role.provider(&storage, &root, Arc::new(Permit)).unwrap();
        let scoped = aws_sdk_s3::Client::from_conf(client.credentials_provider(provider).build());
        scoped.put_object().bucket(&storage.bucket).key(&own).if_none_match("*")
            .body(ByteStream::from_static(b"scoped")).send().await.unwrap();

        let outside = scoped.put_object().bucket(&storage.bucket).key(&sibling).if_none_match("*")
            .body(ByteStream::from_static(b"outside")).send().await.unwrap_err();
        assert_eq!(outside.as_service_error().and_then(|error| error.code()), Some("AccessDenied"));
        let deletion = scoped.delete_object().bucket(&storage.bucket).key(&own).send().await.unwrap_err();
        assert_eq!(deletion.as_service_error().and_then(|error| error.code()), Some("AccessDenied"));
        admin.delete_object().bucket(&storage.bucket).key(&own).send().await.unwrap();
        admin.delete_bucket().bucket(&storage.bucket).send().await.unwrap();
    }
}

struct AuthorizedRole { request: AssumeRoleFluentBuilder, authorization: Arc<dyn WriteAuthorization> }

impl std::fmt::Debug for AuthorizedRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("AuthorizedRole") }
}

impl ProvideCredentials for AuthorizedRole {
    fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a> where Self: 'a {
        future::ProvideCredentials::new(async move {
            self.authorization.authorize(None).await.map_err(|error| CredentialsError::provider_error(error.to_string()))?;
            let response = self.request.clone().send().await.map_err(CredentialsError::provider_error)?;
            let credentials = response.credentials().ok_or_else(|| CredentialsError::provider_error("STS returned no credentials"))?;
            let expires = SystemTime::try_from(*credentials.expiration()).map_err(CredentialsError::provider_error)?;
            let now = SystemTime::now();
            let maximum = now + SCOPED_S3_WRITE_TTL + SCOPED_S3_CLOCK_ALLOWANCE;
            if credentials.session_token().is_empty() || expires <= now || expires > maximum {
                return Err(CredentialsError::provider_error("STS returned invalid session credentials"));
            }
            self.authorization.authorize(Some(expires)).await.map_err(|error| CredentialsError::provider_error(error.to_string()))?;
            Ok(Credentials::new(credentials.access_key_id(), credentials.secret_access_key(),
                Some(credentials.session_token().to_owned()), Some(expires), "rtdb-scoped-s3"))
        })
    }
}
