mod connection;
mod copy;
mod credential;
mod process;
mod powershell;
mod managed;
mod managed_error;
mod desktop;

use std::sync::Arc;
use std::time::Duration;
use object_store::{ClientOptions, StaticCredentialProvider, azure::{AzureAuthorizer, AzureCredentialProvider, MicrosoftAzure, MicrosoftAzureBuilder}, client::{HttpConnector, HttpRequest, HttpRequestBody, ReqwestConnector}};
use rtdb_format::generated::xtdb::azure::proto::AzureBlobStorageConfig;
use rtdb_format::keys::StorageRoot;
use rtdb_storage::{IoRegion, ObjectKey, ObjectStore, remote::{RemoteStore, cache_http}};
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AzureCredentials { pub connection_string: Option<String>, pub storage_account_key: Option<String> }

impl std::fmt::Debug for AzureCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("AzureCredentials([redacted])") }
}

pub trait AzureCredentialResolver: Send + Sync { fn resolve_azure(&self, alias: &str) -> Result<AzureCredentials>; }

impl AzureCredentialResolver for std::collections::BTreeMap<String, AzureCredentials> {
    fn resolve_azure(&self, alias: &str) -> Result<AzureCredentials> {
        self.get(alias).cloned().ok_or_else(|| Anomaly::incorrect("xtdb.azure/missing-remote", format!("No Azure remote configured with alias '{alias}'")))
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AzureConfig {
    pub storage_account: String,
    pub container: String,
    pub prefix: Option<String>,
    pub storage_account_key: Option<String>,
    pub user_managed_identity_client_id: Option<String>,
    pub storage_account_endpoint: Option<String>,
    pub remote: Option<String>,
}

impl std::fmt::Debug for AzureConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("AzureConfig").field("storage_account", &self.storage_account).field("container", &self.container)
            .field("prefix", &self.prefix).field("remote", &self.remote).field("storage_account_key", &self.storage_account_key.as_ref().map(|_| "[redacted]")).finish_non_exhaustive()
    }
}

impl AzureConfig {
    pub fn new(storage_account: impl Into<String>, container: impl Into<String>) -> Self {
        Self { storage_account: storage_account.into(), container: container.into(), prefix: None, storage_account_key: None,
            user_managed_identity_client_id: None, storage_account_endpoint: None, remote: None }
    }

    pub fn resolve_credentials(&self, remotes: &dyn AzureCredentialResolver) -> Result<AzureCredentials> {
        match (&self.remote, &self.storage_account_key) {
            (Some(_), Some(_)) => Err(Anomaly::incorrect("xtdb.azure/conflicting-config", "Azure has both remote and inline account key")),
            (Some(alias), None) => remotes.resolve_azure(alias),
            (None, key) => Ok(AzureCredentials { connection_string: None, storage_account_key: key.clone() }),
        }
    }

    pub fn object_prefix(&self, storage_root: &ObjectKey) -> Result<ObjectKey> {
        if let Some(prefix) = self.prefix.as_deref().filter(|prefix| !prefix.is_empty()) { ObjectKey::new(prefix.to_owned()).map_err(Anomaly::from)?; }
        ObjectKey::new(StorageRoot::under_prefix(self.prefix.as_deref(), storage_root.as_str())).map_err(Anomaly::from)
    }

    /// Upstream XTDB's `ObjectStore.location` URI (`xtdb.azure.BlobStorage`), used to key the shared `c1/` disk cache.
    pub fn storage_location(&self, storage_root: &ObjectKey, remotes: &dyn AzureCredentialResolver) -> Result<String> {
        if self.storage_account.is_empty() || self.container.is_empty() { return Err(invalid("Azure requires an account and container")); }
        let prefix = self.object_prefix(storage_root)?;
        let credentials = self.resolve_credentials(remotes)?;
        let connection = connection::resolve(self, credentials)?;
        let endpoint = &connection.endpoint;
        Ok(format!("{}://{}{}/{}/{}", endpoint.scheme(), endpoint.authority(), endpoint.path().trim_end_matches('/'), self.container, prefix.as_str()))
    }

    /// Creates the container if absent and appends `storage_root` to the configured prefix.
    /// `storage_root` must already contain the storage version and epoch.
    /// Default credential commands may occupy the caller's thread for up to 30 seconds per attempted provider.
    /// The owner closes and joins `io` after store consumers finish and before shutting down the runtime.
    pub async fn open(&self, storage_root: &ObjectKey, remotes: &dyn AzureCredentialResolver, io: Arc<IoRegion>) -> Result<Arc<dyn ObjectStore>> {
        if self.storage_account.is_empty() || self.container.is_empty() { return Err(invalid("Azure requires an account and container")); }
        let prefix = self.object_prefix(storage_root)?;
        let credentials = self.resolve_credentials(remotes)?;
        let connection = connection::resolve(self, credentials)?;
        let credentials: AzureCredentialProvider = match connection.credential {
            Some(credential) => Arc::new(StaticCredentialProvider::new(credential)),
            None => Arc::new(credential::CredentialBridge::new(self.user_managed_identity_client_id.as_deref(), Arc::clone(&io))?),
        };
        let connector = copy::CopyConnector::new(connection.account.clone(), Arc::clone(&credentials));
        let builder = MicrosoftAzureBuilder::from_env().with_account(&connection.account).with_container_name(&self.container)
            .with_use_emulator(false).with_skip_signature(false).with_endpoint(connection.endpoint.to_string()).with_allow_http(connection.endpoint.scheme() == "http")
            .with_credentials(credentials);
        let client = builder.clone().with_http_connector(connector.clone()).build().map_err(invalid)?;
        ensure_container(&client, &connection.account, &connection.endpoint, &self.container).await?;
        let downloads = cache_http::open(connector, |connector, retry| {
            builder.with_http_connector(connector).with_retry(retry).build()
        }).map_err(invalid)?;
        let store = RemoteStore::new_in(Arc::new(client), Some(prefix), io).with_catalog_ledger()?
            .with_cache_downloads(downloads);
        Ok(Arc::new(store))
    }

    pub fn to_proto(&self) -> Result<AzureBlobStorageConfig> {
        if self.remote.is_some() && self.storage_account_key.is_some() { return Err(Anomaly::incorrect("xtdb.azure/conflicting-config", "Azure has both remote and inline account key")); }
        Ok(AzureBlobStorageConfig { storage_account: Some(self.storage_account.clone()), container: Some(self.container.clone()), prefix: self.prefix.clone(),
            storage_account_key: self.storage_account_key.clone(), user_managed_identity_client_id: self.user_managed_identity_client_id.clone(),
            storage_account_endpoint: self.storage_account_endpoint.clone(), remote: self.remote.clone() })
    }

    pub fn from_proto(config: &AzureBlobStorageConfig) -> Self {
        let nonempty = |value: &Option<String>| value.as_ref().filter(|value| !value.is_empty()).cloned();
        Self { storage_account: config.storage_account.clone().unwrap_or_default(), container: config.container.clone().unwrap_or_default(),
            prefix: nonempty(&config.prefix), storage_account_key: nonempty(&config.storage_account_key), user_managed_identity_client_id: nonempty(&config.user_managed_identity_client_id),
            storage_account_endpoint: nonempty(&config.storage_account_endpoint), remote: nonempty(&config.remote) }
    }
}

async fn ensure_container(client: &MicrosoftAzure, account: &str, endpoint: &url::Url, container: &str) -> Result<()> {
    let mut url = endpoint.clone();
    url.path_segments_mut().map_err(|_| invalid("Azure endpoint cannot contain a relative path"))?.pop_if_empty().push(container);
    url.query_pairs_mut().append_pair("restype", "container");
    let http = ReqwestConnector::default().connect(&ClientOptions::default().with_allow_http(url.scheme() == "http").with_timeout(Duration::from_secs(30))).map_err(invalid)?;
    for attempt in 0..4 {
        let credential = client.credentials().get_credential().await.map_err(invalid)?;
        let mut request = HttpRequest::new(HttpRequestBody::empty());
        *request.method_mut() = "PUT".parse().map_err(invalid)?;
        *request.uri_mut() = url.as_str().parse().map_err(invalid)?;
        request.headers_mut().insert("content-length", "0".parse().map_err(invalid)?);
        AzureAuthorizer::new(credential.as_ref(), account).authorize(&mut request);
        match http.execute(request).await {
            Ok(response) if response.status().is_success() || (response.status().as_u16() == 409 && response.headers().get("x-ms-error-code").is_some_and(|value| value == "ContainerAlreadyExists")) => return Ok(()),
            Ok(response) if attempt < 3 && matches!(response.status().as_u16(), 408 | 429 | 500 | 502 | 503 | 504) => (),
            Ok(response) => return Err(Anomaly::fault("xtdb.azure/create-container", format!("Azure container creation returned HTTP {}", response.status()))),
            Err(_) if attempt < 3 => (),
            Err(error) => return Err(Anomaly::fault("xtdb.azure/create-container", error.to_string())),
        }
        tokio::time::sleep(Duration::from_millis(100 << attempt)).await;
    }
    Err(Anomaly::fault("xtdb.azure/create-container", "Azure container creation exhausted retries"))
}

fn invalid(error: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("xtdb.azure/invalid-config", error.to_string()) }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_location_matches_xtdb_blob_storage_uri() {
        let remotes = std::collections::BTreeMap::new();
        let root = ObjectKey::new("v06").unwrap();
        let mut config = AzureConfig::new("acct", "cont");
        assert_eq!(config.storage_location(&root, &remotes).unwrap(), "https://acct.blob.core.windows.net/cont/v06");
        config.prefix = Some("team/prod".into());
        assert_eq!(config.storage_location(&ObjectKey::new("v06_e1").unwrap(), &remotes).unwrap(), "https://acct.blob.core.windows.net/cont/team/prod/v06_e1");
        config.prefix = None;
        config.storage_account_endpoint = Some("https://private.example/".into());
        assert_eq!(config.storage_location(&root, &remotes).unwrap(), "https://private.example/cont/v06");
    }
}
