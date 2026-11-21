use aws_sdk_s3::Client;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use rtdb_storage::ObjectKey;
use rtdb_types::{Anomaly, AnomalyCategory, Result};

pub struct VersionedInventory {
    client: Client,
    bucket: String,
    root: ObjectKey,
}

impl VersionedInventory {
    pub(crate) fn new(client: Client, bucket: String, root: ObjectKey) -> Self { Self { client, bucket, root } }

    /// Purges object versions and delete markers under a deployment prefix.
    /// Each batch is followed by a fresh listing from the beginning of that prefix.
    pub async fn purge(&self, deployment: &ObjectKey) -> Result<()> {
        let path = object_store::path::Path::from(format!("{}/{deployment}", self.root));
        let prefix = format!("{path}/");
        let mut previous = Vec::new();
        loop {
            let page = self.client.list_object_versions().bucket(&self.bucket).prefix(&prefix).max_keys(1000).send().await
                .map_err(|error| unavailable(format!("Listing object versions failed: {error}")))?;
            let mut versions = Vec::new();
            for (key, version) in page.versions().iter().map(|entry| (entry.key(), entry.version_id()))
                .chain(page.delete_markers().iter().map(|entry| (entry.key(), entry.version_id())))
            {
                let key = key.filter(|key| key.starts_with(&prefix))
                    .ok_or_else(|| unavailable("S3 returned a key outside the deployment prefix"))?;
                let version = version.filter(|version| !version.is_empty())
                    .ok_or_else(|| unavailable("S3 returned a key without a version identifier"))?;
                versions.push(ObjectIdentifier::builder().key(key).version_id(version).build()
                    .map_err(|error| unavailable(error.to_string()))?);
            }
            if versions.is_empty() {
                if page.is_truncated() == Some(true) { return Err(unavailable("S3 returned an empty truncated version listing")); }
                return Ok(());
            }
            if versions == previous { return Err(unavailable("A batch of deleted versions is still listed")); }
            let delete = Delete::builder().set_objects(Some(versions.clone())).quiet(true).build()
                .map_err(|error| unavailable(error.to_string()))?;
            let reply = self.client.delete_objects().bucket(&self.bucket).delete(delete).send().await
                .map_err(|error| unavailable(format!("Deleting object versions failed: {error}")))?;
            if let Some(error) = reply.errors().first() {
                return Err(unavailable(format!("S3 refused a version deletion: {}", error.code().unwrap_or("unknown error"))));
            }
            previous = versions;
        }
    }
}

fn unavailable(message: impl Into<String>) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.s3/version-inventory", message)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
    use object_store::path::Path;

    use super::*;
    use crate::{S3Config, S3Credentials};

    #[tokio::test]
    async fn versions_and_delete_markers_are_purged_under_the_encoded_deployment_prefix() {
        let Ok(endpoint) = std::env::var("RTDB_S3_ENDPOINT") else { return };
        let endpoint_url = url::Url::parse(&endpoint).unwrap();
        if !matches!(endpoint_url.host(), Some(url::Host::Ipv4(host)) if host.is_loopback()) { return; }
        let bucket = format!("rtdb-version-test-{}", uuid::Uuid::new_v4().simple());
        let mut config = S3Config::new(&bucket);
        config.endpoint = Some(endpoint);
        config.region = Some("us-east-1".into());
        config.path_style_access_enabled = true;
        config.prefix = Some("cells/encoded root/~org".into());
        let root = ObjectKey::new("cell").unwrap();
        let remotes = BTreeMap::<String, S3Credentials>::new();
        let inventory = config.versioned_inventory(&root, &remotes).await.unwrap();
        let client = &inventory.client;
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.put_bucket_versioning().bucket(&bucket)
            .versioning_configuration(VersioningConfiguration::builder().status(BucketVersioningStatus::Enabled).build())
            .send().await.unwrap();

        let deployment = ObjectKey::new("deployment").unwrap();
        let neighbor = ObjectKey::new("deployment-neighbor").unwrap();
        let physical = |deployment: &ObjectKey| Path::from(format!("{}/{deployment}/data/~file", inventory.root)).to_string();
        let target = physical(&deployment);
        let sibling = physical(&neighbor);
        for (key, payload) in [(&target, b"first".as_slice()), (&target, b"second"), (&sibling, b"kept")] {
            client.put_object().bucket(&bucket).key(key).body(ByteStream::from_static(payload)).send().await.unwrap();
        }
        client.delete_object().bucket(&bucket).key(&target).send().await.unwrap();
        let before = client.list_object_versions().bucket(&bucket).prefix(&target).send().await.unwrap();
        assert_eq!(before.versions().len(), 2);
        assert_eq!(before.delete_markers().len(), 1);

        inventory.purge(&deployment).await.unwrap();
        inventory.purge(&deployment).await.unwrap();
        let after = client.list_object_versions().bucket(&bucket).prefix(&target).send().await.unwrap();
        assert!(after.versions().is_empty());
        assert!(after.delete_markers().is_empty());
        let neighbor_body = client.get_object().bucket(&bucket).key(&sibling).send().await.unwrap().body.collect().await.unwrap();
        assert_eq!(neighbor_body.into_bytes().as_ref(), b"kept");

        inventory.purge(&neighbor).await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    }
}
