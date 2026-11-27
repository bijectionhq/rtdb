mod credential;

use std::sync::Arc;
use object_store::gcp::GoogleCloudStorageBuilder;
use rtdb_format::generated::xtdb::gcp::proto::GcsObjectStoreConfig;
use rtdb_format::keys::StorageRoot;
use rtdb_storage::{IoRegion, ObjectKey, ObjectStore, remote::{RemoteStore, cache_http}};
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcsConfig { pub project_id: String, pub bucket: String, pub prefix: Option<String> }

impl GcsConfig {
    pub fn new(project_id: impl Into<String>, bucket: impl Into<String>) -> Self { Self { project_id: project_id.into(), bucket: bucket.into(), prefix: None } }

    pub fn object_prefix(&self, storage_root: &ObjectKey) -> Result<ObjectKey> {
        if let Some(prefix) = self.prefix.as_deref().filter(|prefix| !prefix.is_empty()) { ObjectKey::new(prefix.to_owned()).map_err(Anomaly::from)?; }
        ObjectKey::new(StorageRoot::under_prefix(self.prefix.as_deref(), storage_root.as_str())).map_err(Anomaly::from)
    }

    /// Upstream XTDB's `ObjectStore.location` URI (`xtdb.gcp.CloudStorage`), used to key the shared `c1/` disk cache.
    pub fn storage_location(&self, storage_root: &ObjectKey) -> Result<String> {
        if self.bucket.is_empty() { return Err(Anomaly::incorrect("xtdb.gcp/invalid-config", "GCS requires a project ID and bucket")); }
        let prefix = self.object_prefix(storage_root)?;
        Ok(format!("gs://{}/{}", self.bucket, prefix.as_str()))
    }

    /// `storage_root` is appended to the configured prefix and must already contain the storage version and epoch.
    /// Application Default Credentials refresh during requests and retain their quota-project headers.
    /// The owner closes and joins `io` after store consumers finish and before shutting down the runtime.
    pub async fn open(&self, storage_root: &ObjectKey, io: Arc<IoRegion>) -> Result<Arc<dyn ObjectStore>> {
        if self.project_id.is_empty() || self.bucket.is_empty() { return Err(Anomaly::incorrect("xtdb.gcp/invalid-config", "GCS requires a project ID and bucket")); }
        let prefix = self.object_prefix(storage_root)?;
        let credentials = google_cloud_auth::credentials::Builder::default().with_scopes(["https://www.googleapis.com/auth/devstorage.full_control"]).build()
            .map_err(|error| Anomaly::incorrect("xtdb.gcp/credentials", error.to_string()))?;
        let bridge = credential::CredentialBridge::new(credentials);
        let builder = GoogleCloudStorageBuilder::new().with_bucket_name(&self.bucket).with_skip_signature(false)
            .with_credentials(Arc::new(bridge.clone()));
        let client = builder.clone().with_http_connector(bridge.clone()).build()
            .map_err(|error| Anomaly::incorrect("xtdb.gcp/invalid-config", error.to_string()))?;
        let downloads = cache_http::open(bridge, |connector, retry| {
            builder.with_http_connector(connector).with_retry(retry).build()
        })
            .map_err(|error| Anomaly::incorrect("xtdb.gcp/invalid-config", error.to_string()))?;
        let store = RemoteStore::new_in(Arc::new(client), Some(prefix), io).with_catalog_ledger()?
            .with_cache_downloads(downloads);
        Ok(Arc::new(store))
    }

    pub fn to_proto(&self) -> GcsObjectStoreConfig {
        GcsObjectStoreConfig { project_id: Some(self.project_id.clone()), bucket: Some(self.bucket.clone()), prefix: self.prefix.clone() }
    }

    pub fn from_proto(config: &GcsObjectStoreConfig) -> Self {
        Self { project_id: config.project_id.clone().unwrap_or_default(), bucket: config.bucket.clone().unwrap_or_default(), prefix: config.prefix.as_ref().filter(|prefix| !prefix.is_empty()).cloned() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_location_matches_xtdb_cloud_storage_uri() {
        let root = ObjectKey::new("v06").unwrap();
        let mut config = GcsConfig::new("project", "bucket");
        assert_eq!(config.storage_location(&root).unwrap(), "gs://bucket/v06");
        config.prefix = Some("team/prod".into());
        assert_eq!(config.storage_location(&ObjectKey::new("v06_e1").unwrap()).unwrap(), "gs://bucket/team/prod/v06_e1");
    }
}
