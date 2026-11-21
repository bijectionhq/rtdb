use std::collections::BTreeSet;

use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier, RequestPayer};
use bytes::Bytes;
use object_store::path::Path;
use rtdb_storage::{Error, ObjectKey, remote::PublicationUploads};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

fn collect_child(parent: &str, key: Option<&str>, children: &mut BTreeSet<String>) {
    if let Some((child, _)) = key.and_then(|key| key.strip_prefix(parent)).and_then(|relative| relative.split_once('/')) {
        children.insert(child.to_owned());
    }
}

pub struct VersionedInventory {
    client: Client,
    bucket: String,
    root: ObjectKey,
    request_payer: Option<RequestPayer>,
    scope_multipart_listing: bool,
}

impl VersionedInventory {
    pub(crate) fn new(client: Client, bucket: String, root: ObjectKey, requester_pays: bool, scope_multipart_listing: bool) -> Self {
        Self { client, bucket, root, request_payer: requester_pays.then_some(RequestPayer::Requester), scope_multipart_listing }
    }

    /// Retires one exact journaled upload rather than looking for a current
    /// object. A successful result proves that its upload ID no longer exists.
    pub async fn abort_publication(&self, key: &ObjectKey, upload: &str) -> Result<()> {
        let path = Path::from(format!("{}/{key}", self.root));
        self.abort_upload(path.as_ref(), upload).await
    }

    /// Empties multipart uploads, object versions and delete markers under a deployment prefix.
    pub async fn purge(&self, deployment: &ObjectKey) -> Result<()> {
        let prefix = self.prefix(deployment);
        self.abort_uploads(&prefix).await?;
        self.purge_versions(&prefix).await?;
        if self.is_empty(deployment).await? { Ok(()) }
        else { Err(unavailable("The version or multipart inventory changed during erasure")) }
    }

    /// Observes the version and multipart inventories without modifying either.
    pub async fn is_empty(&self, deployment: &ObjectKey) -> Result<bool> {
        let prefix = self.prefix(deployment);
        if !self.multipart_batch(&prefix).await?.is_empty() { return Ok(false); }
        Ok(self.version_batch(&prefix).await?.is_empty())
    }

    /// Finds immediate directories even when every object under them is
    /// represented only by a noncurrent version, a tombstone or an upload.
    pub async fn child_directories(&self, parent: &ObjectKey) -> Result<BTreeSet<String>> {
        let prefix = self.prefix(parent);
        let mut directories = BTreeSet::new();
        let mut marker = (None, None);
        loop {
            let page = self.client.list_object_versions().bucket(&self.bucket).prefix(&prefix)
                .delimiter("/").max_keys(1000).set_key_marker(marker.0.clone())
                .set_version_id_marker(marker.1.clone())
                .set_request_payer(self.request_payer.clone()).send().await
                .map_err(|error| unavailable(format!("Listing version directories failed: {error}")))?;
            for entry in page.common_prefixes() {
                collect_child(&prefix, entry.prefix(), &mut directories);
            }
            if page.is_truncated() != Some(true) { break; }
            let next = (page.next_key_marker().map(str::to_owned), page.next_version_id_marker().map(str::to_owned));
            if next.0.as_deref().is_none_or(str::is_empty) || next == marker {
                return Err(unavailable("S3 returned a non-advancing version directory listing"));
            }
            marker = next;
        }
        let mut marker = (None, None);
        loop {
            let page = self.client.list_multipart_uploads().bucket(&self.bucket).max_uploads(1000)
                .set_prefix(self.scope_multipart_listing.then(|| prefix.clone()))
                .set_delimiter(self.scope_multipart_listing.then(|| "/".to_owned()))
                .set_key_marker(marker.0.clone()).set_upload_id_marker(marker.1.clone())
                .set_request_payer(self.request_payer.clone()).send().await
                .map_err(|error| unavailable(format!("Listing multipart directories failed: {error}")))?;
            for entry in page.common_prefixes() {
                collect_child(&prefix, entry.prefix(), &mut directories);
            }
            for upload in page.uploads() {
                collect_child(&prefix, upload.key(), &mut directories);
            }
            if page.is_truncated() != Some(true) { return Ok(directories); }
            let next = (page.next_key_marker().map(str::to_owned), page.next_upload_id_marker().map(str::to_owned));
            if next.0.as_deref().is_none_or(str::is_empty) || next == marker {
                return Err(unavailable("S3 returned a non-advancing multipart directory listing"));
            }
            marker = next;
        }
    }

    fn prefix(&self, deployment: &ObjectKey) -> String {
        let path = object_store::path::Path::from(format!("{}/{deployment}", self.root));
        format!("{path}/")
    }

    async fn abort_uploads(&self, prefix: &str) -> Result<()> {
        let mut previous = Vec::new();
        loop {
            let uploads = self.multipart_batch(prefix).await?;
            if uploads.is_empty() { return Ok(()); }
            if uploads == previous { return Err(unavailable("A batch of aborted multipart uploads is still listed")); }
            for (key, id) in &uploads { self.abort_upload(key, id).await?; }
            previous = uploads;
        }
    }

    async fn multipart_batch(&self, prefix: &str) -> Result<Vec<(String, String)>> {
        let mut marker = (None, None);
        loop {
            let page = self.client.list_multipart_uploads().bucket(&self.bucket).max_uploads(1000)
                .set_prefix(self.scope_multipart_listing.then(|| prefix.to_owned()))
                .set_key_marker(marker.0.clone()).set_upload_id_marker(marker.1.clone())
                .set_request_payer(self.request_payer.clone()).send().await
                .map_err(|error| unavailable(format!("Listing multipart uploads failed: {error}")))?;
            let mut uploads = Vec::new();
            for upload in page.uploads() {
                let key = upload.key().ok_or_else(|| unavailable("S3 returned a multipart upload without a key"))?;
                if !key.starts_with(prefix) {
                    if self.scope_multipart_listing { return Err(unavailable("S3 returned an upload outside the deployment prefix")); }
                    continue;
                }
                let id = upload.upload_id().filter(|id| !id.is_empty())
                    .ok_or_else(|| unavailable("S3 returned a multipart upload without an identifier"))?;
                uploads.push((key.to_owned(), id.to_owned()));
            }
            if !uploads.is_empty() || page.is_truncated() != Some(true) { return Ok(uploads); }
            let next = (page.next_key_marker().map(str::to_owned), page.next_upload_id_marker().map(str::to_owned));
            if next.0.as_deref().is_none_or(str::is_empty) || next <= marker {
                return Err(unavailable("S3 returned a non-advancing multipart listing"));
            }
            marker = next;
        }
    }

    async fn abort_upload(&self, key: &str, id: &str) -> Result<()> {
        for _ in 0..4 {
            match self.client.abort_multipart_upload().bucket(&self.bucket).key(key).upload_id(id)
                .set_request_payer(self.request_payer.clone()).send().await
            {
                Ok(_) => {}
                Err(error) if error.as_service_error().is_some_and(|error| error.is_no_such_upload()) => {}
                Err(error) => return Err(unavailable(format!("Aborting a multipart upload failed: {error}"))),
            }
            match self.client.list_parts().bucket(&self.bucket).key(key).upload_id(id).max_parts(1)
                .set_request_payer(self.request_payer.clone()).send().await
            {
                Ok(_) => {}
                Err(error) if error.as_service_error().and_then(|error| error.code()) == Some("NoSuchUpload") => return Ok(()),
                Err(error) => return Err(unavailable(format!("Checking an aborted multipart upload failed: {error}"))),
            }
        }
        Err(unavailable("The aborted multipart upload is still addressable"))
    }

    async fn purge_versions(&self, prefix: &str) -> Result<()> {
        let mut previous = Vec::new();
        loop {
            let versions = self.version_batch(prefix).await?;
            if versions.is_empty() { return Ok(()); }
            if versions == previous { return Err(unavailable("A batch of deleted versions is still listed")); }
            let delete = Delete::builder().set_objects(Some(versions.clone())).quiet(true).build()
                .map_err(|error| unavailable(error.to_string()))?;
            let reply = self.client.delete_objects().bucket(&self.bucket).delete(delete)
                .set_request_payer(self.request_payer.clone()).send().await
                .map_err(|error| unavailable(format!("Deleting object versions failed: {error}")))?;
            if let Some(error) = reply.errors().first() {
                return Err(unavailable(format!("S3 refused a version deletion: {}", error.code().unwrap_or("unknown error"))));
            }
            previous = versions;
        }
    }

    async fn version_batch(&self, prefix: &str) -> Result<Vec<ObjectIdentifier>> {
        let page = self.client.list_object_versions().bucket(&self.bucket).prefix(prefix).max_keys(1000)
            .set_request_payer(self.request_payer.clone()).send().await
            .map_err(|error| unavailable(format!("Listing object versions failed: {error}")))?;
        let mut versions = Vec::new();
        for (key, version) in page.versions().iter().map(|entry| (entry.key(), entry.version_id()))
            .chain(page.delete_markers().iter().map(|entry| (entry.key(), entry.version_id())))
        {
            let key = key.filter(|key| key.starts_with(prefix))
                .ok_or_else(|| unavailable("S3 returned a key outside the deployment prefix"))?;
            let version = version.filter(|version| !version.is_empty())
                .ok_or_else(|| unavailable("S3 returned a key without a version identifier"))?;
            versions.push(ObjectIdentifier::builder().key(key).version_id(version).build()
                .map_err(|error| unavailable(error.to_string()))?);
        }
        if versions.is_empty() && page.is_truncated() == Some(true) { return Err(unavailable("S3 returned an empty truncated version listing")); }
        Ok(versions)
    }
}

#[async_trait::async_trait]
impl PublicationUploads for VersionedInventory {
    async fn reserve(&self, path: &Path, attempt: uuid::Uuid) -> rtdb_storage::Result<String> {
        let key = path.as_ref();
        let reserved = self.client.create_multipart_upload().bucket(&self.bucket).key(key)
            .metadata("rtdb-publication", attempt.to_string()).checksum_algorithm(ChecksumAlgorithm::Sha256)
            .set_request_payer(self.request_payer.clone()).send().await
            .map_err(|error| publication_failure(path, error.to_string()))?;
        reserved.upload_id().filter(|id| !id.is_empty()).map(str::to_owned)
            .ok_or_else(|| publication_failure(path, "S3 did not identify the reserved upload"))
    }

    async fn complete(&self, path: &Path, upload: &str, bytes: Bytes) -> rtdb_storage::Result<bool> {
        let key = path.as_ref();
        let part = self.client.upload_part().bucket(&self.bucket).key(key).upload_id(upload)
            .part_number(1).checksum_algorithm(ChecksumAlgorithm::Sha256).body(ByteStream::from(bytes))
            .set_request_payer(self.request_payer.clone()).send().await
            .map_err(|error| publication_failure(path, error.to_string()))?;
        let tag = part.e_tag().ok_or_else(|| publication_failure(path, "S3 returned an uploaded part without an ETag"))?;
        let checksum = part.checksum_sha256().ok_or_else(|| publication_failure(path, "S3 returned an uploaded part without its checksum"))?;
        let parts = CompletedMultipartUpload::builder()
            .parts(CompletedPart::builder().part_number(1).e_tag(tag).checksum_sha256(checksum).build())
            .build();
        match self.client.complete_multipart_upload().bucket(&self.bucket).key(key).upload_id(upload)
            .if_none_match("*").multipart_upload(parts).set_request_payer(self.request_payer.clone()).send().await
        {
            Ok(_) => Ok(true),
            Err(error) if error.as_service_error().and_then(|error| error.code()) == Some("PreconditionFailed") => Ok(false),
            Err(error) => Err(publication_failure(path, error.to_string())),
        }
    }

    async fn abort(&self, path: &Path, upload: &str) -> rtdb_storage::Result<()> {
        self.abort_upload(path.as_ref(), upload).await
            .map_err(|error| publication_failure(path, error.code))
    }
}

fn publication_failure(path: &Path, message: impl Into<String>) -> Error {
    Error::Remote { key: path.to_string(), message: message.into() }
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
    use rtdb_storage::ObjectStore;
    use crate::{S3Config, S3Credentials};

    #[tokio::test]
    async fn versions_delete_markers_and_pending_uploads_are_purged_under_the_encoded_deployment_prefix() {
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

        let cell = config.open_cell(&root, &remotes, rtdb_storage::IoRegion::new()).await.unwrap();
        let observed_key = ObjectKey::new("observed/logical ~file").unwrap();
        let physical_key = Path::from(format!("{}/{observed_key}", inventory.root)).to_string();
        assert_eq!(cell.retirement_version(&observed_key).await.unwrap(), None);
        let initial = client.put_object().bucket(&bucket).key(&physical_key)
            .body(ByteStream::from_static(b"old version")).send().await.unwrap();
        let observed = cell.retirement_version(&observed_key).await.unwrap().unwrap();
        assert_eq!(observed.namespace.bucket, bucket);
        assert_eq!(observed.namespace.endpoint, Some(endpoint_url.to_string()));
        assert_eq!(observed.namespace.region.as_deref(), Some("us-east-1"));
        assert_eq!(observed.key, physical_key);
        assert_eq!(Some(observed.version.as_str()), initial.version_id());
        let latest = client.put_object().bucket(&bucket).key(&physical_key)
            .body(ByteStream::from_static(b"new version")).send().await.unwrap();
        assert_ne!(initial.version_id(), latest.version_id());
        client.delete_object().bucket(&bucket).key(&observed.key).version_id(&observed.version).send().await.unwrap();
        assert_eq!(cell.get(&observed_key).await.unwrap().as_ref(), b"new version");
        inventory.purge(&ObjectKey::new("observed").unwrap()).await.unwrap();
        cell.close_and_wait().await;

        let upload_key = ObjectKey::new("managed/upload").unwrap();
        let upload_path = Path::from(format!("{}/{upload_key}", inventory.root));
        let attempt = uuid::Uuid::new_v4();
        let winner = inventory.reserve(&upload_path, attempt).await.unwrap();
        assert!(inventory.complete(&upload_path, &winner, Bytes::from_static(b"sealed")).await.unwrap());
        let loser = inventory.reserve(&upload_path, uuid::Uuid::new_v4()).await.unwrap();
        assert!(!inventory.complete(&upload_path, &loser, Bytes::from_static(b"other")).await.unwrap());
        inventory.abort_publication(&upload_key, &loser).await.unwrap();
        inventory.abort_publication(&upload_key, &winner).await.unwrap();
        let retained = client.get_object().bucket(&bucket).key(upload_path.as_ref()).send().await.unwrap();
        assert_eq!(retained.metadata().unwrap()["rtdb-publication"], attempt.to_string());
        assert_eq!(retained.body.collect().await.unwrap().into_bytes().as_ref(), b"sealed");
        inventory.purge(&ObjectKey::new("managed").unwrap()).await.unwrap();

        let index = ObjectKey::new("index/generations").unwrap();
        let deployment = ObjectKey::new(format!("{index}/old")).unwrap();
        let neighbor = ObjectKey::new(format!("{index}/old-neighbor")).unwrap();
        let pending_only = ObjectKey::new(format!("{index}/upload-without-version")).unwrap();
        let physical = |deployment: &ObjectKey| Path::from(format!("{}/{deployment}/data/~file", inventory.root)).to_string();
        let target = physical(&deployment);
        let sibling = physical(&neighbor);
        let unpublished = physical(&pending_only);
        for (key, payload) in [(&target, b"first".as_slice()), (&target, b"second"), (&sibling, b"kept")] {
            client.put_object().bucket(&bucket).key(key).body(ByteStream::from_static(payload)).send().await.unwrap();
        }
        client.delete_object().bucket(&bucket).key(&target).send().await.unwrap();
        let before = client.list_object_versions().bucket(&bucket).prefix(&target).send().await.unwrap();
        assert_eq!(before.versions().len(), 2);
        assert_eq!(before.delete_markers().len(), 1);
        let mut pending = Vec::new();
        for key in [&target, &sibling, &unpublished] {
            let upload = client.create_multipart_upload().bucket(&bucket).key(key).send().await.unwrap();
            let id = upload.upload_id().unwrap();
            client.upload_part().bucket(&bucket).key(key).upload_id(id).part_number(1)
                .body(ByteStream::from_static(b"unfinished")).send().await.unwrap();
            pending.push(id.to_owned());
        }

        assert_eq!(inventory.child_directories(&index).await.unwrap(), BTreeSet::from([
            "old".into(), "old-neighbor".into(), "upload-without-version".into(),
        ]));
        assert!(!inventory.is_empty(&deployment).await.unwrap());
        inventory.purge(&deployment).await.unwrap();
        inventory.purge(&deployment).await.unwrap();
        assert!(inventory.is_empty(&deployment).await.unwrap());
        assert!(!inventory.is_empty(&neighbor).await.unwrap());
        let after = client.list_object_versions().bucket(&bucket).prefix(&target).send().await.unwrap();
        assert!(after.versions().is_empty());
        assert!(after.delete_markers().is_empty());
        let uploads = client.list_multipart_uploads().bucket(&bucket).send().await.unwrap();
        assert_eq!(uploads.uploads().iter().map(|upload| upload.upload_id().unwrap()).collect::<Vec<_>>(), [pending[1].as_str(), pending[2].as_str()]);
        assert_eq!(inventory.child_directories(&index).await.unwrap(), BTreeSet::from([
            "old-neighbor".into(), "upload-without-version".into(),
        ]));
        let parts = client.list_parts().bucket(&bucket).key(&target).upload_id(&pending[0]).send().await.unwrap_err();
        assert_eq!(parts.as_service_error().and_then(|error| error.code()), Some("NoSuchUpload"));
        let neighbor_body = client.get_object().bucket(&bucket).key(&sibling).send().await.unwrap().body.collect().await.unwrap();
        assert_eq!(neighbor_body.into_bytes().as_ref(), b"kept");

        inventory.purge(&neighbor).await.unwrap();
        inventory.purge(&pending_only).await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
    }
}
