mod credential;

use std::sync::Arc;
use object_store::gcp::GoogleCloudStorageBuilder;
use rtdb_format::generated::xtdb::gcp::proto::GcsObjectStoreConfig;
use rtdb_format::keys::StorageRoot;
use rtdb_storage::{IoRegion, ObjectKey, ObjectStore, remote::RemoteStore};
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

    /// `storage_root` is appended to the configured prefix and must already contain the storage version and epoch.
    /// Application Default Credentials refresh during requests and retain their quota-project headers.
    /// The owner closes and joins `io` after store consumers finish and before shutting down the runtime.
    pub async fn open(&self, storage_root: &ObjectKey, io: Arc<IoRegion>) -> Result<Arc<dyn ObjectStore>> {
        if self.project_id.is_empty() || self.bucket.is_empty() { return Err(Anomaly::incorrect("xtdb.gcp/invalid-config", "GCS requires a project ID and bucket")); }
        let prefix = self.object_prefix(storage_root)?;
        let credentials = google_cloud_auth::credentials::Builder::default().with_scopes(["https://www.googleapis.com/auth/devstorage.full_control"]).build()
            .map_err(|error| Anomaly::incorrect("xtdb.gcp/credentials", error.to_string()))?;
        let bridge = credential::CredentialBridge::new(credentials);
        let client = GoogleCloudStorageBuilder::new().with_bucket_name(&self.bucket).with_skip_signature(false)
            .with_credentials(Arc::new(bridge.clone())).with_http_connector(bridge).build()
            .map_err(|error| Anomaly::incorrect("xtdb.gcp/invalid-config", error.to_string()))?;
        Ok(Arc::new(RemoteStore::new_in(Arc::new(client), Some(prefix), io).with_catalog_ledger()?))
    }

    pub fn to_proto(&self) -> GcsObjectStoreConfig {
        GcsObjectStoreConfig { project_id: Some(self.project_id.clone()), bucket: Some(self.bucket.clone()), prefix: self.prefix.clone() }
    }

    pub fn from_proto(config: &GcsObjectStoreConfig) -> Self {
        Self { project_id: config.project_id.clone().unwrap_or_default(), bucket: config.bucket.clone().unwrap_or_default(), prefix: config.prefix.as_ref().filter(|prefix| !prefix.is_empty()).cloned() }
    }
}
