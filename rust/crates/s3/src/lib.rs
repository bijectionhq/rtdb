mod credential;
mod scoped;
pub use scoped::{SCOPED_S3_CLOCK_ALLOWANCE, SCOPED_S3_WRITE_TTL, ScopedS3Writes, WriteAuthorization};
#[cfg(feature = "versioning")]
mod versions;
#[cfg(feature = "versioning")]
mod terminal;

#[cfg(feature = "versioning")]
pub use versions::{NeverVersionedObject, NeverVersionedRetirement, VersionedInventory};

use std::sync::Arc;
use aws_credential_types::provider::SharedCredentialsProvider;
use object_store::{RetryConfig, StaticCredentialProvider, aws::{AmazonS3Builder, AmazonS3ConfigKey, AwsCredential, S3ConditionalPut, S3CopyIfNotExists}};
use rtdb_format::generated::xtdb::aws::proto as pb;
use rtdb_format::keys::StorageRoot;
use rtdb_storage::{IoRegion, ObjectKey, ObjectStore, remote::RemoteStore};
use rtdb_types::{Anomaly, Result, S3ObjectNamespace};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub const LEGACY_CELL_ROOT: &str = "cell";
pub const CELL_STORAGE_NAMESPACE_MARKER: &str = "~cell-namespace";
const TERMINAL_CELL_PREFIX: &str = "cell-terminal-";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TerminalS3Scope {
    pub allocation: Uuid,
    pub authority: [u8; 32],
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3Credentials { pub access_key: String, pub secret_key: String }

impl std::fmt::Debug for S3Credentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("S3Credentials([redacted])") }
}

pub trait S3CredentialResolver: Send + Sync {
    fn resolve_s3(&self, alias: &str) -> Result<S3Credentials>;
}

impl S3CredentialResolver for std::collections::BTreeMap<String, S3Credentials> {
    fn resolve_s3(&self, alias: &str) -> Result<S3Credentials> {
        self.get(alias).cloned().ok_or_else(|| Anomaly::incorrect("xtdb.aws/missing-remote", format!("No S3 remote configured with alias '{alias}'")))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: Option<String>,
    pub prefix: Option<String>,
    pub credentials: Option<S3Credentials>,
    pub endpoint: Option<String>,
    #[serde(default)]
    pub path_style_access_enabled: bool,
    pub remote: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_scope: Option<TerminalS3Scope>,
}

enum S3Authorization {
    Static(S3Credentials),
    Rotating { provider: SharedCredentialsProvider, region: Option<String> },
}

impl S3Config {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self { bucket: bucket.into(), region: None, prefix: None, credentials: None, endpoint: None, path_style_access_enabled: false, remote: None, terminal_scope: None }
    }

    pub fn resolve_credentials(&self, remotes: &dyn S3CredentialResolver) -> Result<Option<S3Credentials>> {
        match (&self.remote, &self.credentials) {
            (Some(_), Some(_)) => Err(Anomaly::incorrect("xtdb.aws/conflicting-config", "S3 has both remote and inline credentials")),
            (Some(alias), None) => remotes.resolve_s3(alias).map(Some),
            (None, credentials) => Ok(credentials.clone()),
        }
    }

    pub fn object_prefix(&self, storage_root: &ObjectKey) -> Result<ObjectKey> {
        if let Some(prefix) = self.prefix.as_deref().filter(|prefix| !prefix.is_empty()) { ObjectKey::new(prefix.to_owned()).map_err(Anomaly::from)?; }
        ObjectKey::new(StorageRoot::under_prefix(self.prefix.as_deref(), storage_root.as_str())).map_err(Anomaly::from)
    }

    pub fn cell_root(&self) -> Result<ObjectKey> {
        let Some(scope) = &self.terminal_scope else {
            if self.prefix.as_deref().is_some_and(is_terminal_root) { return Err(reserved_terminal_root()); }
            return Ok(ObjectKey::new(LEGACY_CELL_ROOT)?);
        };
        if self.prefix.as_deref().is_some_and(|prefix| !prefix.is_empty()) {
            return Err(Anomaly::incorrect("xtdb.aws/terminal-scope", "A terminal allocation starts at the bucket root"));
        }
        if scope.allocation.is_nil() {
            return Err(Anomaly::incorrect("xtdb.aws/terminal-scope", "A terminal S3 allocation must have a nonzero identity"));
        }
        Ok(ObjectKey::new(format!("{TERMINAL_CELL_PREFIX}{}", scope.allocation.simple()))?)
    }

    fn require_cell_root(&self, storage_root: &ObjectKey) -> Result<()> {
        let configured = self.cell_root()?;
        if self.terminal_scope.is_some() && *storage_root != configured {
            return Err(Anomaly::incorrect("xtdb.aws/terminal-scope", "The requested S3 root is outside its terminal allocation"));
        }
        if self.terminal_scope.is_none() && is_terminal_root(self.object_prefix(storage_root)?.as_str()) {
            return Err(reserved_terminal_root());
        }
        Ok(())
    }

    /// `storage_root` is appended to the configured prefix and must already contain the storage version and epoch.
    /// Credential aliases are resolved at opening; default credentials refresh during requests.
    /// The owner closes and joins `io` after store consumers finish and before shutting down the runtime.
    pub async fn open(&self, storage_root: &ObjectKey, remotes: &dyn S3CredentialResolver, io: Arc<IoRegion>) -> Result<Arc<dyn ObjectStore>> {
        if self.terminal_scope.is_some() {
            return Err(Anomaly::incorrect("xtdb.aws/terminal-scope", "A terminal S3 allocation must be opened as a cell"));
        }
        self.require_cell_root(storage_root)?;
        Ok(self.open_store(storage_root, remotes, io, false).await?)
    }

    /// Opens the cell's immutable namespace. Any result that leaves an
    /// individual write uncertain belongs to the region's retirement owner.
    pub async fn open_cell(&self, storage_root: &ObjectKey, remotes: &dyn S3CredentialResolver, io: Arc<IoRegion>) -> Result<Arc<RemoteStore>> {
        self.require_cell_root(storage_root)?;
        #[cfg(not(feature = "versioning"))]
        if self.terminal_scope.is_some() {
            return Err(Anomaly::incorrect("xtdb.aws/terminal-scope", "Terminal cells require S3 inventory support"));
        }
        let store = self.open_store(storage_root, remotes, io, true).await?;
        #[cfg(feature = "versioning")]
        if let Some(scope) = &self.terminal_scope {
            terminal::require_enrollment(store.as_ref(), &self.object_prefix(storage_root)?, scope).await?;
        }
        Ok(store)
    }

    async fn open_store(&self, storage_root: &ObjectKey, remotes: &dyn S3CredentialResolver, io: Arc<IoRegion>, cell: bool) -> Result<Arc<RemoteStore>> {
        if self.bucket.is_empty() { return Err(Anomaly::incorrect("xtdb.aws/invalid-config", "S3 bucket is empty")); }
        let prefix = self.object_prefix(storage_root)?;
        let authorization = if let Some(credentials) = self.resolve_credentials(remotes)? {
            S3Authorization::Static(credentials)
        } else {
            let mut configuration = aws_config::defaults(aws_config::BehaviorVersion::latest());
            if let Some(region) = &self.region { configuration = configuration.region(aws_config::Region::new(region.clone())); }
            let configuration = configuration.load().await;
            let provider = configuration.credentials_provider().ok_or_else(|| Anomaly::incorrect("xtdb.aws/credentials", "AWS default credential provider is unavailable"))?;
            S3Authorization::Rotating { provider, region: configuration.region().map(|region| region.as_ref().to_owned()) }
        };
        self.open_authorized(prefix, authorization, io, cell)
    }

    fn open_authorized(&self, prefix: ObjectKey, authorization: S3Authorization, io: Arc<IoRegion>, cell: bool) -> Result<Arc<RemoteStore>> {
        #[cfg(feature = "versioning")]
        let uploads = (cell && io.publication_journal().is_some())
            .then(|| self.authorized_inventory(prefix.clone(), &authorization)).transpose()?;
        #[cfg(not(feature = "versioning"))]
        if cell && io.publication_journal().is_some() {
            return Err(Anomaly::incorrect("xtdb.aws/managed-uploads", "Managed cell publications require S3 inventory support"));
        }
        let (endpoint, virtual_hosted) = self.endpoint_addressing()?;
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&self.bucket)
            .with_virtual_hosted_style_request(virtual_hosted)
            .with_skip_signature(false).with_conditional_put(S3ConditionalPut::ETagMatch);
        builder = if cell {
            builder.with_retry(RetryConfig { max_retries: 0, ..RetryConfig::default() })
        } else {
            builder.with_copy_if_not_exists(S3CopyIfNotExists::Multipart)
        };
        if let Some(region) = &self.region { builder = builder.with_region(region); }
        if let Some(endpoint) = &endpoint { builder = builder.with_config(AmazonS3ConfigKey::S3Endpoint, endpoint.as_str()).with_allow_http(endpoint.scheme() == "http"); }
        builder = match authorization {
            S3Authorization::Static(credentials) => {
                if credentials.access_key.is_empty() || credentials.secret_key.is_empty() { return Err(Anomaly::incorrect("xtdb.aws/invalid-config", "S3 credentials require nonempty access and secret keys")); }
                builder.with_credentials(Arc::new(StaticCredentialProvider::new(AwsCredential { key_id: credentials.access_key, secret_key: credentials.secret_key, token: None })))
            }
            S3Authorization::Rotating { provider, region } => {
                if let Some(region) = region { builder = builder.with_region(region); }
                builder.with_credentials(Arc::new(credential::CredentialBridge::new(provider)))
            }
        };
        let namespace = S3ObjectNamespace {
            bucket: self.bucket.clone(),
            endpoint: endpoint.map(|endpoint| endpoint.to_string()),
            region: Some(builder.get_config_value(&AmazonS3ConfigKey::Region).unwrap_or_else(|| "us-east-1".into())),
        };
        let build = |builder: AmazonS3Builder| {
            builder.build().map(Arc::new).map_err(|error| Anomaly::incorrect("xtdb.aws/invalid-config", error.to_string()))
        };
        let audience_reader = cell.then(|| build(builder.clone())).transpose()?;
        let client = build(builder)?;
        if !cell {
            return Ok(Arc::new(RemoteStore::new_in(client, Some(prefix), io)));
        }
        #[cfg(feature = "versioning")]
        let store = match uploads {
            Some(uploads) => RemoteStore::new_managed(client, prefix, io, Arc::new(uploads)),
            None => RemoteStore::new_single_put(client, prefix, io),
        };
        #[cfg(not(feature = "versioning"))]
        let store = RemoteStore::new_single_put(client, prefix, io);
        let store = store.with_audience_reader(audience_reader.expect("a cell opens a distinct read client"))
            .with_s3_versioning(namespace);
        Ok(Arc::new(match &self.terminal_scope {
            Some(scope) => store.with_terminal_allocation(scope.allocation),
            None => store,
        }))
    }

    #[cfg(feature = "versioning")]
    fn authorized_inventory(&self, root: ObjectKey, authorization: &S3Authorization) -> Result<VersionedInventory> {
        let endpoint = self.configured_endpoint()?;
        let environment = AmazonS3Builder::from_env();
        let (provider, provider_region) = match authorization {
            S3Authorization::Static(credentials) => (SharedCredentialsProvider::new(
                aws_credential_types::Credentials::new(&credentials.access_key, &credentials.secret_key, None, None, "rtdb-s3"),
            ), None),
            S3Authorization::Rotating { provider, region } => (provider.clone(), region.clone()),
        };
        let region = self.region.clone().or(provider_region)
            .or_else(|| environment.get_config_value(&AmazonS3ConfigKey::Region))
            .unwrap_or_else(|| "us-east-1".into());
        let requester_pays = environment.get_config_value(&AmazonS3ConfigKey::RequestPayer)
            .is_some_and(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "on" | "yes" | "y"));
        let namespace = self.inventory_namespace(&region)?;
        let mut builder = aws_sdk_s3::config::Builder::new()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region))
            .credentials_provider(provider)
            .force_path_style(!self.virtual_hosted(endpoint.as_ref()));
        let scope_multipart_listing = endpoint.is_none();
        builder.set_endpoint_url(endpoint.map(|endpoint| endpoint.to_string()));
        Ok(VersionedInventory::new(aws_sdk_s3::Client::from_conf(builder.build()), namespace, root, requester_pays, scope_multipart_listing))
    }

    #[cfg(feature = "versioning")]
    pub async fn versioned_inventory(&self, storage_root: &ObjectKey, remotes: &dyn S3CredentialResolver) -> Result<VersionedInventory> {
        self.require_cell_root(storage_root)?;
        let inventory = self.open_inventory(storage_root, remotes).await?;
        if let Some(scope) = &self.terminal_scope {
            let store = self.open_cell(storage_root, remotes, IoRegion::new()).await?;
            if store.s3_namespace() != Some(inventory.namespace()) {
                return Err(Anomaly::incorrect("xtdb.aws/terminal-scope", "The store and inventory resolve to different S3 namespaces"));
            }
            return Ok(inventory.with_terminal_allocation(scope.allocation));
        }
        Ok(inventory)
    }

    #[cfg(feature = "versioning")]
    async fn open_inventory(&self, storage_root: &ObjectKey, remotes: &dyn S3CredentialResolver) -> Result<VersionedInventory> {
        if self.bucket.is_empty() { return Err(Anomaly::incorrect("xtdb.aws/invalid-config", "S3 bucket is empty")); }
        let root = self.object_prefix(storage_root)?;
        let endpoint = self.configured_endpoint()?;
        let environment = AmazonS3Builder::from_env();
        let default_region = || environment.get_config_value(&AmazonS3ConfigKey::Region).unwrap_or_else(|| "us-east-1".into());
        let requester_pays = environment.get_config_value(&AmazonS3ConfigKey::RequestPayer)
            .is_some_and(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "on" | "yes" | "y"));
        let mut config = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = &self.region { config = config.region(aws_config::Region::new(region.clone())); }
        if let Some(credentials) = self.resolve_credentials(remotes)? {
            if credentials.access_key.is_empty() || credentials.secret_key.is_empty() { return Err(Anomaly::incorrect("xtdb.aws/invalid-config", "S3 credentials require nonempty access and secret keys")); }
            if self.region.is_none() { config = config.region(aws_config::Region::new(default_region())); }
            config = config.credentials_provider(aws_credential_types::Credentials::new(credentials.access_key, credentials.secret_key, None, None, "rtdb-s3"));
        }
        let config = config.load().await;
        let region = config.region().cloned().unwrap_or_else(|| aws_config::Region::new(default_region()));
        let namespace = self.inventory_namespace(region.as_ref())?;
        let mut builder = aws_sdk_s3::config::Builder::from(&config)
            .region(region)
            .force_path_style(!self.virtual_hosted(endpoint.as_ref()));
        // MinIO's multipart prefix query only returns exact key matches.
        let scope_multipart_listing = endpoint.is_none();
        builder.set_endpoint_url(endpoint.map(|endpoint| endpoint.to_string()));
        Ok(VersionedInventory::new(aws_sdk_s3::Client::from_conf(builder.build()), namespace, root, requester_pays, scope_multipart_listing))
    }

    #[cfg(feature = "versioning")]
    fn inventory_namespace(&self, region: &str) -> Result<S3ObjectNamespace> {
        let (endpoint, _) = self.endpoint_addressing()?;
        Ok(S3ObjectNamespace {
            bucket: self.bucket.clone(),
            endpoint: endpoint.map(|endpoint| endpoint.to_string()),
            region: Some(region.to_owned()),
        })
    }

    fn configured_endpoint(&self) -> Result<Option<url::Url>> {
        let environment = AmazonS3Builder::from_env();
        let endpoint = self.endpoint.clone().or_else(|| environment.get_config_value(&AmazonS3ConfigKey::S3Endpoint)).or_else(|| environment.get_config_value(&AmazonS3ConfigKey::Endpoint));
        let endpoint = endpoint.as_ref().map(|endpoint| url::Url::parse(endpoint)
            .map_err(|_| Anomaly::incorrect("xtdb.aws/invalid-config", "Invalid S3 endpoint"))).transpose()?;
        if endpoint.as_ref().is_some_and(|endpoint| !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() || !endpoint.username().is_empty() || endpoint.password().is_some() || endpoint.query().is_some() || endpoint.fragment().is_some()) {
            return Err(Anomaly::incorrect("xtdb.aws/invalid-config", "S3 endpoint must be an HTTP URL without embedded credentials, query, or fragment"));
        }
        Ok(endpoint)
    }

    fn virtual_hosted(&self, endpoint: Option<&url::Url>) -> bool {
        let dns_bucket = (3..=63).contains(&self.bucket.len()) && self.bucket.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'.')
            && self.bucket.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric) && self.bucket.as_bytes().last().is_some_and(u8::is_ascii_alphanumeric)
            && !self.bucket.contains("..") && self.bucket.parse::<std::net::IpAddr>().is_err();
        let https = endpoint.is_none_or(|endpoint| endpoint.scheme() == "https");
        !self.path_style_access_enabled && dns_bucket && !(https && self.bucket.contains('.'))
            && !endpoint.is_some_and(|endpoint| matches!(endpoint.host(), Some(url::Host::Ipv4(_) | url::Host::Ipv6(_))))
    }

    fn endpoint_addressing(&self) -> Result<(Option<url::Url>, bool)> {
        let endpoint = self.configured_endpoint()?;
        let virtual_hosted = self.virtual_hosted(endpoint.as_ref());
        let endpoint = endpoint.map(|mut endpoint| {
            if virtual_hosted {
                let host = format!("{}.{}", self.bucket, endpoint.host_str().unwrap_or_default());
                endpoint.set_host(Some(&host)).map_err(|_| Anomaly::incorrect("xtdb.aws/invalid-config", "Invalid virtual-hosted S3 endpoint"))?;
            }
            Ok::<_, Anomaly>(endpoint)
        }).transpose()?;
        Ok((endpoint, virtual_hosted))
    }

    pub fn to_proto(&self) -> Result<pb::S3ObjectStoreConfig> {
        if self.terminal_scope.is_some() {
            return Err(Anomaly::incorrect("xtdb.aws/terminal-scope", "The classic S3 configuration cannot encode a terminal allocation"));
        }
        if self.remote.is_some() && self.credentials.is_some() { return Err(Anomaly::incorrect("xtdb.aws/conflicting-config", "S3 has both remote and inline credentials")); }
        Ok(pb::S3ObjectStoreConfig { bucket: Some(self.bucket.clone()), region: self.region.clone(), prefix: self.prefix.clone(),
            credentials: self.credentials.as_ref().map(|credentials| pb::S3Credentials { access_key: Some(credentials.access_key.clone()), secret_key: Some(credentials.secret_key.clone()) }),
            endpoint: self.endpoint.clone(), path_style_access_enabled: Some(self.path_style_access_enabled), remote: self.remote.clone() })
    }

    pub fn from_proto(config: &pb::S3ObjectStoreConfig) -> Self {
        let nonempty = |value: &Option<String>| value.as_ref().filter(|value| !value.is_empty()).cloned();
        Self { bucket: config.bucket.clone().unwrap_or_default(), region: nonempty(&config.region), prefix: nonempty(&config.prefix),
            credentials: config.credentials.as_ref().map(|credentials| S3Credentials { access_key: credentials.access_key.clone().unwrap_or_default(), secret_key: credentials.secret_key.clone().unwrap_or_default() }),
            endpoint: nonempty(&config.endpoint), path_style_access_enabled: config.path_style_access_enabled.unwrap_or_default(), remote: nonempty(&config.remote), terminal_scope: None }
    }
}

fn is_terminal_root(path: &str) -> bool {
    let component = path.split_once('/').map_or(path, |(component, _)| component);
    component.strip_prefix(TERMINAL_CELL_PREFIX).is_some_and(|suffix| {
        suffix.len() == 32 && suffix.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn reserved_terminal_root() -> Anomaly {
    Anomaly::incorrect("xtdb.aws/terminal-scope", "An S3 prefix cannot enter the reserved space for terminal allocations")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_endpoints_keep_the_bucket_in_both_addressing_modes() {
        let mut config = S3Config::new("bucket");
        config.endpoint = Some("https://s3.example.test".into());
        let (endpoint, virtual_hosted) = config.endpoint_addressing().unwrap();
        assert!(virtual_hosted);
        assert_eq!(endpoint.unwrap().host_str(), Some("bucket.s3.example.test"));
        config.path_style_access_enabled = true;
        assert!(!config.endpoint_addressing().unwrap().1);
        config.path_style_access_enabled = false;
        config.endpoint = Some("http://127.0.0.1:9000".into());
        assert!(!config.endpoint_addressing().unwrap().1);
    }
    #[tokio::test]
    async fn ranged_observation_uses_the_version_from_the_payload_response() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = S3Config::new("cell");
        config.endpoint = Some(format!("http://{}", listener.local_addr().unwrap()));
        config.region = Some("us-east-1".into());
        config.credentials = Some(S3Credentials { access_key: "test".into(), secret_key: "test-secret".into() });
        let store = config.open_cell(&ObjectKey::new("root").unwrap(), &std::collections::BTreeMap::new(), IoRegion::new()).await.unwrap();
        let file = ObjectKey::new("pack").unwrap();
        let server = tokio::spawn(async move {
            for (method, is_range, reply) in [
                ("HEAD", false, "HTTP/1.1 200 OK\r\nContent-Length: 8\r\nx-amz-version-id: before\r\nConnection: close\r\n\r\n"),
                ("GET", true, "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-4/8\r\nContent-Length: 3\r\nx-amz-version-id: observed\r\nConnection: close\r\n\r\ncde"),
                ("GET", false, "HTTP/1.1 200 OK\r\nContent-Length: 8\r\nx-amz-version-id: whole\r\nConnection: close\r\n\r\nabcdefgh"),
            ] {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                assert_eq!(line, format!("{method} /cell/root/pack HTTP/1.1\r\n"));
                let mut ranged = false;
                while line != "\r\n" {
                    line.clear();
                    assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
                    ranged |= line.eq_ignore_ascii_case("range: bytes=2-4\r\n");
                }
                assert_eq!(ranged, is_range);
                reader.get_mut().write_all(reply.as_bytes()).await.unwrap();
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            let head = store.observe_head(&file).await.unwrap();
            assert_eq!(head.value.size, 8);
            assert_eq!(head.version.unwrap().version, "before");
            let read = store.observe_range(&file, 2..5).await.unwrap();
            assert_eq!(read.value.as_ref(), b"cde");
            let version = read.version.unwrap();
            assert_eq!(version.key, "root/pack");
            assert_eq!(version.version, "observed");
            let whole = store.observe_fresh_exact(&file, 8).await.unwrap();
            assert_eq!(whole.value.as_ref(), b"abcdefgh");
            assert_eq!(whole.version.as_ref().map(|version| (version.key.as_str(), version.version.as_str())),
                Some(("root/pack", "whole")), "the exact body must carry its provider response version");
            server.await.unwrap();
        }).await.unwrap();
        store.close_and_wait().await;
    }

    #[tokio::test]
    async fn retirement_observation_accepts_only_exact_version_ids() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let mut config = S3Config::new("cell");
        config.endpoint = Some(endpoint.clone());
        config.region = Some("us-east-1".into());
        config.credentials = Some(S3Credentials { access_key: "test".into(), secret_key: "test-secret".into() });
        let root = ObjectKey::new("encoded root").unwrap();
        let file = ObjectKey::new("data/loose file").unwrap();
        let store = config.open_cell(&root, &std::collections::BTreeMap::new(), IoRegion::new()).await.unwrap();
        let responses = [
            ("200 OK", "x-amz-version-id: version+/=\r\n"),
            ("200 OK", "x-amz-version-id: null\r\n"),
            ("200 OK", "x-amz-version-id:\r\n"),
            ("200 OK", ""),
            ("404 Not Found", ""),
        ];
        let server = tokio::spawn(async move {
            for (status, version_header) in responses {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                assert_eq!(line, "HEAD /cell/encoded%20root/data/loose%20file HTTP/1.1\r\n");
                while line != "\r\n" {
                    line.clear();
                    assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
                }
                let reply = format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n{version_header}\r\n");
                reader.get_mut().write_all(reply.as_bytes()).await.unwrap();
            }
        });
        let result = async {
            let observed = store.retirement_version(&file).await.unwrap().unwrap();
            assert_eq!(observed, rtdb_types::PhysicalObjectVersion {
                namespace: S3ObjectNamespace {
                    bucket: "cell".into(),
                    endpoint: Some(format!("{endpoint}/")),
                    region: Some("us-east-1".into()),
                },
                key: "encoded root/data/loose file".into(),
                version: "version+/=".into(),
            });
            for _ in 0..4 {
                assert_eq!(store.retirement_version(&file).await.unwrap(), None, "retirement observation returned an unusable provider version");
            }
            server.await.unwrap();
        };
        tokio::time::timeout(std::time::Duration::from_secs(10), result).await.unwrap();
        store.close_and_wait().await;
    }
}
