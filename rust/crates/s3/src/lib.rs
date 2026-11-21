mod credential;
mod scoped;
pub use scoped::{ScopedS3Writes, WriteAuthorization};
#[cfg(feature = "versioning")]
mod versions;

#[cfg(feature = "versioning")]
pub use versions::VersionedInventory;

use std::sync::Arc;
use aws_credential_types::provider::SharedCredentialsProvider;
use object_store::{StaticCredentialProvider, aws::{AmazonS3Builder, AmazonS3ConfigKey, AwsCredential, S3ConditionalPut, S3CopyIfNotExists}};
use rtdb_format::generated::xtdb::aws::proto as pb;
use rtdb_format::keys::StorageRoot;
use rtdb_storage::{IoRegion, ObjectKey, ObjectStore, remote::RemoteStore};
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};

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
}

enum S3Authorization {
    Static(S3Credentials),
    Rotating { provider: SharedCredentialsProvider, region: Option<String> },
}

impl S3Config {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self { bucket: bucket.into(), region: None, prefix: None, credentials: None, endpoint: None, path_style_access_enabled: false, remote: None }
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

    /// `storage_root` is appended to the configured prefix and must already contain the storage version and epoch.
    /// Credential aliases are resolved at opening; default credentials refresh during requests.
    /// The owner closes and joins `io` after store consumers finish and before shutting down the runtime.
    pub async fn open(&self, storage_root: &ObjectKey, remotes: &dyn S3CredentialResolver, io: Arc<IoRegion>) -> Result<Arc<dyn ObjectStore>> {
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
        self.open_authorized(prefix, authorization, io)
    }

    fn open_authorized(&self, prefix: ObjectKey, authorization: S3Authorization, io: Arc<IoRegion>) -> Result<Arc<dyn ObjectStore>> {
        let (endpoint, virtual_hosted) = self.endpoint_addressing()?;
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&self.bucket)
            .with_virtual_hosted_style_request(virtual_hosted)
            .with_skip_signature(false).with_conditional_put(S3ConditionalPut::ETagMatch).with_copy_if_not_exists(S3CopyIfNotExists::Multipart);
        if let Some(region) = &self.region { builder = builder.with_region(region); }
        if let Some(endpoint) = endpoint { builder = builder.with_config(AmazonS3ConfigKey::S3Endpoint, endpoint.as_str()).with_allow_http(endpoint.scheme() == "http"); }
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
        let client = builder.build().map_err(|error| Anomaly::incorrect("xtdb.aws/invalid-config", error.to_string()))?;
        Ok(Arc::new(RemoteStore::new_in(Arc::new(client), Some(prefix), io)))
    }

    #[cfg(feature = "versioning")]
    pub async fn versioned_inventory(&self, storage_root: &ObjectKey, remotes: &dyn S3CredentialResolver) -> Result<VersionedInventory> {
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
        let mut builder = aws_sdk_s3::config::Builder::from(&config)
            .region(region)
            .force_path_style(!self.virtual_hosted(endpoint.as_ref()));
        // MinIO's multipart prefix query only returns exact key matches.
        let scope_multipart_listing = endpoint.is_none();
        builder.set_endpoint_url(endpoint.map(|endpoint| endpoint.to_string()));
        Ok(VersionedInventory::new(aws_sdk_s3::Client::from_conf(builder.build()), self.bucket.clone(), root, requester_pays, scope_multipart_listing))
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
        if self.remote.is_some() && self.credentials.is_some() { return Err(Anomaly::incorrect("xtdb.aws/conflicting-config", "S3 has both remote and inline credentials")); }
        Ok(pb::S3ObjectStoreConfig { bucket: Some(self.bucket.clone()), region: self.region.clone(), prefix: self.prefix.clone(),
            credentials: self.credentials.as_ref().map(|credentials| pb::S3Credentials { access_key: Some(credentials.access_key.clone()), secret_key: Some(credentials.secret_key.clone()) }),
            endpoint: self.endpoint.clone(), path_style_access_enabled: Some(self.path_style_access_enabled), remote: self.remote.clone() })
    }

    pub fn from_proto(config: &pb::S3ObjectStoreConfig) -> Self {
        let nonempty = |value: &Option<String>| value.as_ref().filter(|value| !value.is_empty()).cloned();
        Self { bucket: config.bucket.clone().unwrap_or_default(), region: nonempty(&config.region), prefix: nonempty(&config.prefix),
            credentials: config.credentials.as_ref().map(|credentials| S3Credentials { access_key: credentials.access_key.clone().unwrap_or_default(), secret_key: credentials.secret_key.clone().unwrap_or_default() }),
            endpoint: nonempty(&config.endpoint), path_style_access_enabled: config.path_style_access_enabled.unwrap_or_default(), remote: nonempty(&config.remote) }
    }
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
}
