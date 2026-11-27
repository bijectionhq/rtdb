use std::{collections::BTreeSet, sync::Arc};

use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumAlgorithm, CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier, ObjectLockLegalHold, ObjectLockLegalHoldStatus, RequestPayer};
use bytes::Bytes;
use object_store::path::Path;
use rtdb_storage::{Error, IoRegion, ObjectKey, ObjectVersionHold, remote::{ClassicVersionRetirement, PublicationResult, PublicationUploads, RemoteKeyLayout, S3_RETIREMENT_ATTRIBUTE, strong_entity_tag, valid_object_version}};
use rtdb_types::{Anomaly, AnomalyCategory, PhysicalObjectVersion, Result, S3ObjectNamespace, S3TombstoneCondition, S3TombstoneTarget};

#[path = "never_versioned.rs"]
mod never_versioned;
pub use never_versioned::{NeverVersionedObject, NeverVersionedRetirement, ObservedTerminalObject};
mod pinned_reads;
mod retirement_decision;
use retirement_decision::{VersionStatus, exact_version_status, matches_terminal_target};

fn collect_child(parent: &str, key: Option<&str>, children: &mut BTreeSet<String>) {
    if let Some((child, _)) = key.and_then(|key| key.strip_prefix(parent)).and_then(|relative| relative.split_once('/')) {
        children.insert(child.to_owned());
    }
}

#[derive(Clone)]
pub struct VersionedInventory {
    reads: Arc<IoRegion>,
    client: Client,
    namespace: S3ObjectNamespace,
    root: ObjectKey,
    key_layout: RemoteKeyLayout,
    request_payer: Option<RequestPayer>,
    scope_multipart_listing: bool,
    terminal_allocation: Option<uuid::Uuid>,
}

/// Current exact evidence for one conditional upload attempt.
pub enum CreateAttemptPublication {
    Absent,
    Other,
    Published(PhysicalObjectVersion),
}

const RETIRED_VERSION_ATTRIBUTE: &str = "rtdb-retired-version";

#[async_trait::async_trait]
impl ObjectVersionHold for VersionedInventory {
    async fn require_held(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<()> {
        let path = self.observed_path(key, version)
            .map_err(|_| unavailable("The hold does not name an immutable version in this allocation"))?;
        self.inspect((path.to_string(), version.version.clone()), |owner, (path, version)| async move {
            if owner.legal_hold_status(&path, &version).await? != ObjectLockLegalHoldStatus::On {
                return Err(unavailable("The exact S3 object version has no legal hold"));
            }
            Ok(())
        }).await
    }
}

enum VersionedRetirementHead {
    Live(String),
    Tombstone(uuid::Uuid),
    Changed,
}

#[async_trait::async_trait]
impl ClassicVersionRetirement for VersionedInventory {
    async fn available(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> rtdb_storage::Result<bool> {
        self.exact_retirement_available(key, version).await.map_err(|error| version_error(key, error))
    }

    async fn erase(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> rtdb_storage::Result<()> {
        self.retire_observed_version(key, version).await.map_err(|error| version_error(key, error))
    }

    async fn require_never_versioned(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        self.never_versioned_retirement().await.map(drop).map_err(|error| version_error(key, error))
    }

    async fn settle_staging_upload(&self, upload: uuid::Uuid, version: Option<&str>) -> rtdb_storage::Result<()> {
        let path = self.key_layout.provider_path(&format!("{}/.tmp/upload-{upload}", self.root))?;
        let result = async {
            if let Some(version) = version.filter(|version| valid_object_version(version.as_bytes())) {
                return self.retire_version_at_path(path.as_ref(), version).await;
            }
            self.never_versioned_retirement().await?;
            self.client.delete_object().bucket(&self.namespace.bucket).key(path.as_ref())
                .set_request_payer(self.request_payer.clone()).send().await
                .map_err(|error| unavailable(format!("Removing the Classic staging upload failed: {error}")))?;
            if self.version_status(path.as_ref(), "null").await? != VersionStatus::Absent {
                return Err(unavailable("The Classic staging upload is still present"));
            }
            Ok(())
        }.await;
        result.map_err(|error| Error::Remote { key: path.to_string(), message: error.to_string() })
    }

}

fn version_error(key: &ObjectKey, error: Anomaly) -> Error {
    Error::Remote { key: key.to_string(), message: error.to_string() }
}

impl VersionedInventory {
    /// Validates an exact legal-hold target before contacting S3.
    pub fn check_legal_hold_target(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<()> {
        self.observed_path(key, version).map(drop)
    }

    /// Turns the caller's selected version's legal hold off and confirms the result from S3.
    /// The caller authorizes the release.
    pub async fn release_legal_hold(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<()> {
        let path = self.observed_path(key, version)?;
        self.inspect((path.to_string(), version.version.clone()), |owner, (path, version)| async move {
            match owner.legal_hold_status(&path, &version).await? {
                ObjectLockLegalHoldStatus::Off => return Ok(()),
                ObjectLockLegalHoldStatus::On => (),
                _ => return Err(unavailable("S3 reported an unknown legal hold state")),
            }
            let hold = ObjectLockLegalHold::builder().status(ObjectLockLegalHoldStatus::Off).build();
            owner.client.put_object_legal_hold().bucket(&owner.namespace.bucket)
                .key(&path).version_id(&version).legal_hold(hold)
                .set_request_payer(owner.request_payer.clone()).send().await
                .map_err(|error| unavailable(format!("Releasing the S3 legal hold failed: {error}")))?;
            if owner.legal_hold_status(&path, &version).await? != ObjectLockLegalHoldStatus::Off {
                return Err(unavailable("S3 did not confirm release of the exact version's legal hold"));
            }
            Ok(())
        }).await
    }

    async fn legal_hold_status(&self, path: &str, version: &str) -> Result<ObjectLockLegalHoldStatus> {
        let reply = self.client.get_object_legal_hold().bucket(&self.namespace.bucket)
            .key(path).version_id(version).set_request_payer(self.request_payer.clone()).send().await
            .map_err(|error| unavailable(format!("Reading the S3 legal hold failed: {error}")))?;
        reply.legal_hold().and_then(|hold| hold.status()).cloned()
            .ok_or_else(|| unavailable("S3 did not report the exact object's legal hold state"))
    }

    pub(crate) fn new(client: Client, namespace: S3ObjectNamespace, root: ObjectKey, requester_pays: bool, scope_multipart_listing: bool) -> Self {
        Self { reads: IoRegion::new(), client, namespace, root, key_layout: RemoteKeyLayout::Escaped, request_payer: requester_pays.then_some(RequestPayer::Requester), scope_multipart_listing, terminal_allocation: None }
    }

    pub(crate) fn with_key_layout(mut self, layout: RemoteKeyLayout) -> Self {
        self.key_layout = layout;
        self
    }

    async fn inspect<T, A, F>(&self, selection: A, inspect: impl FnOnce(Arc<Self>, A) -> F + Send + 'static) -> Result<T>
    where
        T: Send + 'static,
        A: Clone + Send + Sync + 'static,
        F: std::future::Future<Output = Result<T>> + Send + 'static,
    {
        self.reads.read_async(Arc::new(self.clone()), selection, move |owner, target| async move {
            Ok(inspect(owner, target).await)
        }).await?
    }

    pub(crate) fn namespace(&self) -> &S3ObjectNamespace { &self.namespace }

    pub(crate) fn with_terminal_allocation(mut self, allocation: uuid::Uuid) -> Self {
        self.terminal_allocation = Some(allocation);
        self
    }

    pub async fn terminal_release_targets(&self, key: &ObjectKey, release: &ObjectKey, publication: &rtdb_types::TerminalObjectPublication)
        -> Result<Option<[rtdb_types::S3TombstoneTarget; 2]>>
    {
        if self.terminal_allocation != Some(publication.allocation) || self.namespace != publication.namespace { return Ok(None); }
        let retirement = self.never_versioned_retirement().await?;
        let NeverVersionedObject::Open(data) = retirement.observe(key).await? else { return Ok(None); };
        if data.key != publication.key || data.condition != S3TombstoneCondition::Matches(publication.etag.clone()) { return Ok(None); }
        let NeverVersionedObject::Open(marker) = retirement.observe(release).await? else { return Ok(None); };
        if !matches!(marker.condition, S3TombstoneCondition::Matches(_)) { return Ok(None); }
        Ok(Some([data, marker]))
    }

    pub(crate) async fn enrollment_inventory_is_clean(&self, existing_evidence: Option<&ObjectKey>) -> Result<bool> {
        self.inspect(existing_evidence.cloned(), |owner, target| async move { owner.inspect_enrollment_inventory_is_clean(target.as_ref()).await }).await
    }

    async fn inspect_enrollment_inventory_is_clean(&self, existing_evidence: Option<&ObjectKey>) -> Result<bool> {
        let root = Path::from(self.root.as_str());
        let prefix = format!("{root}/");
        if !self.multipart_batch(&prefix).await?.is_empty() { return Ok(false); }
        let page = self.client.list_object_versions().bucket(&self.namespace.bucket).prefix(&prefix).max_keys(2)
            .set_request_payer(self.request_payer.clone()).send().await
            .map_err(|error| unavailable(format!("Checking terminal enrollment versions failed: {error}")))?;
        if page.is_truncated() != Some(false) || !page.delete_markers().is_empty() { return Ok(false); }
        Ok(match (existing_evidence, page.versions()) {
            (None, []) => true,
            (Some(evidence), [version]) => {
                let key = Path::from(format!("{}/{evidence}", self.root));
                version.key() == Some(key.as_ref()) && version.version_id() == Some("null")
            }
            _ => false,
        })
    }

    pub async fn observe_create_attempt(
        &self, key: &ObjectKey, attempt: Option<uuid::Uuid>,
    ) -> Result<CreateAttemptPublication> {
        self.inspect((key.clone(), attempt), |owner, (key, attempt)| async move {
            let path = Path::from(format!("{}/{key}", owner.root));
            match owner.client.head_object().bucket(&owner.namespace.bucket).key(path.as_ref())
                .set_request_payer(owner.request_payer.clone()).send().await
            {
                Ok(head) => {
                    let matched = attempt.is_some_and(|attempt| head.metadata().is_some_and(|metadata| {
                        metadata.get("rtdb-publication").is_some_and(|published| published == &attempt.to_string())
                    }));
                    if !matched { return Ok(CreateAttemptPublication::Other); }
                    let version = head.version_id().filter(|version| valid_object_version(version.as_bytes()))
                        .ok_or_else(|| unavailable("The successful upload has no exact version"))?;
                    Ok(CreateAttemptPublication::Published(PhysicalObjectVersion {
                        namespace: owner.namespace.clone(), key: path.to_string(), version: version.to_owned(),
                    }))
                }
                Err(error) if error.as_service_error().is_some_and(|error| error.is_not_found()) => {
                    let page = owner.client.list_object_versions().bucket(&owner.namespace.bucket)
                        .prefix(path.as_ref()).max_keys(1).set_request_payer(owner.request_payer.clone()).send().await
                        .map_err(|error| unavailable(format!("The staged version inventory could not be read: {error}")))?;
                    let had_history = page.is_truncated() != Some(false)
                        || page.versions().iter().any(|object| object.key() == Some(path.as_ref()))
                        || page.delete_markers().iter().any(|object| object.key() == Some(path.as_ref()));
                    Ok(if had_history { CreateAttemptPublication::Other } else { CreateAttemptPublication::Absent })
                }
                Err(error) => Err(unavailable(format!("The staged upload could not be observed: {error}"))),
            }
        }).await
    }

    /// All readable data versions at this exact path, including versions hidden by markers.
    pub async fn data_versions(&self, key: &ObjectKey) -> Result<Vec<PhysicalObjectVersion>> {
        self.inspect(key.clone(), |owner, key| async move { owner.inspect_data_versions(&key).await }).await
    }

    async fn inspect_data_versions(&self, key: &ObjectKey) -> Result<Vec<PhysicalObjectVersion>> {
        let path = Path::from(format!("{}/{key}", self.root));
        let mut cursor: (Option<String>, Option<String>) = (None, None);
        let mut visited = BTreeSet::new();
        let mut versions = Vec::new();
        loop {
            let page = self.client.list_object_versions().bucket(&self.namespace.bucket).prefix(path.as_ref())
                .max_keys(1000).set_key_marker(cursor.0.clone()).set_version_id_marker(cursor.1.clone())
                .set_request_payer(self.request_payer.clone()).send().await
                .map_err(|error| unavailable(format!("Reading exact S3 path versions failed: {error}")))?;
            for entry in page.versions() {
                let listed = entry.key().filter(|listed| listed.starts_with(path.as_ref()))
                    .ok_or_else(|| unavailable("S3 listed a version outside the requested path prefix"))?;
                if listed != path.as_ref() { continue; }
                let version = entry.version_id().filter(|version| valid_object_version(version.as_bytes()))
                    .ok_or_else(|| unavailable("The path has no exact version identity"))?;
                versions.push(PhysicalObjectVersion { namespace: self.namespace.clone(), key: listed.to_owned(), version: version.to_owned() });
            }
            match page.is_truncated() {
                Some(false) => return Ok(versions),
                Some(true) => (),
                None => return Err(unavailable("S3 omitted the exact version pagination state")),
            }
            let next = page.next_key_marker().filter(|next| next.starts_with(path.as_ref()))
                .ok_or_else(|| unavailable("S3 omitted the exact version key marker"))?;
            // After the requested key, the remaining results only share its prefix.
            if next != path.as_ref() { return Ok(versions); }
            cursor = (Some(next.to_owned()), Some(page.next_version_id_marker().filter(|id| !id.is_empty())
                .ok_or_else(|| unavailable("S3 omitted the exact version cursor"))?.to_owned()));
            if !visited.insert(cursor.clone()) { return Err(unavailable("S3 repeated the exact version cursor")); }
        }
    }

    /// Enumerates every immutable file version below an enrolled provider directory.
    pub async fn data_versions_under(&self, directory: &ObjectKey) -> Result<Vec<(ObjectKey, PhysicalObjectVersion, u64)>> {
        self.inspect(directory.clone(), |owner, directory| async move {
            let scope = owner.prefix(&directory);
            let base = format!("{}/", Path::from(owner.root.as_str()));
            let mut cursor = (None, None);
            let mut visited = BTreeSet::new();
            let mut versions = Vec::new();
            loop {
                let page = owner.client.list_object_versions().bucket(&owner.namespace.bucket).prefix(&scope)
                    .max_keys(1000).set_key_marker(cursor.0.clone()).set_version_id_marker(cursor.1.clone())
                    .set_request_payer(owner.request_payer.clone()).send().await
                    .map_err(|error| unavailable(format!("Reading a versioned directory failed: {error}")))?;
                for entry in page.versions() {
                    let path = entry.key().filter(|key| key.starts_with(&scope))
                        .ok_or_else(|| unavailable("S3 returned a version outside its directory"))?;
                    let key = rtdb_storage::remote::decode_provider_key(path.strip_prefix(&base)
                        .ok_or_else(|| unavailable("S3 returned a version outside the storage root"))?)?;
                    let id = entry.version_id().filter(|id| valid_object_version(id.as_bytes()))
                        .ok_or_else(|| unavailable("S3 returned a version with no immutable identity"))?;
                    let version = PhysicalObjectVersion { namespace: owner.namespace.clone(), key: path.to_owned(), version: id.to_owned() };
                    owner.observed_path(&key, &version)?;
                    let size = entry.size().and_then(|size| u64::try_from(size).ok())
                        .ok_or_else(|| unavailable("S3 returned a version without its byte length"))?;
                    versions.push((key, version, size));
                }
                match page.is_truncated() {
                    Some(false) => return Ok(versions),
                    Some(true) => (),
                    None => return Err(unavailable("S3 omitted the directory version pagination state")),
                }
                cursor = (page.next_key_marker().map(str::to_owned), page.next_version_id_marker().map(str::to_owned));
                if cursor.0.as_ref().is_none_or(|key| !key.starts_with(&scope)) || !visited.insert(cursor.clone()) {
                    return Err(unavailable("S3 returned an incomplete directory version cursor"));
                }
            }
        }).await
    }

    pub async fn read_version_prefix(&self, key: &ObjectKey, version: &PhysicalObjectVersion, length: u64) -> Result<Bytes> {
        if !(1..=4096).contains(&length) { return Err(unavailable("The version prefix exceeds its header bound")); }
        self.read_version_range(key, version, 0..length).await
    }

    pub async fn read_version_range(&self, key: &ObjectKey, version: &PhysicalObjectVersion, range: std::ops::Range<u64>) -> Result<Bytes> {
        let length = range.end.checked_sub(range.start).filter(|length| (1..=(16 << 20)).contains(length))
            .ok_or_else(|| unavailable("The exact version range exceeds its bound"))?;
        let path = self.observed_path(key, version)?;
        let start = range.start;
        let last = range.end - 1;
        self.inspect((path.to_string(), version.version.clone()), move |owner, (path, version)| async move {
            let mut result = owner.client.get_object().bucket(&owner.namespace.bucket).key(path).version_id(&version)
                .range(format!("bytes={start}-{last}")).set_request_payer(owner.request_payer.clone()).send().await
                .map_err(|error| unavailable(format!("Reading the S3 version prefix failed: {error}")))?;
            let prefix = format!("bytes {start}-{last}/");
            if result.version_id() != Some(version.as_str()) || result.delete_marker().unwrap_or(false)
                || result.content_length() != Some(length as i64)
                || !result.content_range().and_then(|range| range.strip_prefix(&prefix))
                    .and_then(|size| size.parse::<u64>().ok()).is_some_and(|size| size > last)
            {
                return Err(unavailable("S3 returned a different version or byte range"));
            }
            let mut bytes = Vec::with_capacity(length as usize);
            while let Some(chunk) = result.body.next().await {
                let chunk = chunk.map_err(|error| unavailable(format!("Reading the version body failed: {error}")))?;
                if chunk.len() > length as usize - bytes.len() { return Err(unavailable("S3 returned a longer version prefix")); }
                bytes.extend_from_slice(&chunk);
            }
            if bytes.len() != length as usize { return Err(unavailable("S3 returned an incomplete version prefix")); }
            Ok(Bytes::from(bytes))
        }).await
    }

    pub async fn exact_retirement_available(&self, key: &ObjectKey, observed: &PhysicalObjectVersion) -> Result<bool> {
        let path = self.observed_path(key, observed)?;
        Ok(self.version_status(path.as_ref(), &observed.version).await? != VersionStatus::WouldExposeOlderData)
    }

    pub fn owns_publication(&self, key: &ObjectKey, observed: &PhysicalObjectVersion) -> bool {
        self.observed_path(key, observed).is_ok()
    }

    /// Checks for durable absence of one version, even if the path has a newer publication.
    pub async fn exact_retirement_absent(&self, key: &ObjectKey, observed: &PhysicalObjectVersion) -> Result<bool> {
        let path = self.observed_path(key, observed)?;
        Ok(self.version_status(path.as_ref(), &observed.version).await? == VersionStatus::Absent)
    }

    /// Executes an already authorized retirement of one immutable physical version.
    /// Success means a fresh version inventory confirmed absence of that exact object.
    pub async fn retire_observed_version(&self, key: &ObjectKey, observed: &PhysicalObjectVersion) -> Result<()> {
        let path = self.observed_path(key, observed)?;
        self.retire_version_at_path(path.as_ref(), &observed.version).await
    }

    /// Captures the current ETag as well as the exact version selected by the
    /// retirement owner. The ETag is used only for the current-object CAS.
    pub async fn versioned_retirement_target(
        &self, key: &ObjectKey, version: &PhysicalObjectVersion,
    ) -> Result<Option<S3TombstoneTarget>> {
        let path = self.observed_path(key, version)?;
        let VersionedRetirementHead::Live(tag) = self.versioned_retirement_head(path.as_ref(), &version.version).await?
            else { return Ok(None); };
        Ok(Some(S3TombstoneTarget {
            namespace: version.namespace.clone(), key: version.key.clone(), condition: S3TombstoneCondition::Matches(tag),
        }))
    }

    /// The authorized grant closes concurrent RTDB publication admission.
    /// A current, zero-byte S3 version keeps conditional writers fenced while
    /// the old immutable version is deleted and after the grant settles.
    pub async fn retire_version_behind_tombstone(
        &self, key: &ObjectKey, version: &PhysicalObjectVersion, target: &S3TombstoneTarget, token: uuid::Uuid,
    ) -> Result<()> {
        let path = self.observed_path(key, version)?;
        let S3TombstoneCondition::Matches(tag) = &target.condition else {
            return Err(unavailable("A versioned tombstone requires a conditional entity tag"));
        };
        if target.namespace != version.namespace || target.key != version.key || !strong_entity_tag(tag) || token.is_nil() {
            return Err(unavailable("The tombstone does not name the authorized versioned object"));
        }
        self.require_versioned().await?;
        let path = path.to_string();
        let (tag, version) = (tag.clone(), version.version.clone());
        self.inspect((path, tag, version, token), |owner, (path, tag, version, token)| async move {
            match owner.inspect_versioned_retirement_head(&path, &version).await? {
                VersionedRetirementHead::Tombstone(prior) if prior == token => (),
                VersionedRetirementHead::Live(observed) if observed == tag => {
                    let reply = owner.client.put_object().bucket(&owner.namespace.bucket).key(&path)
                        .if_match(tag).metadata(S3_RETIREMENT_ATTRIBUTE, token.simple().to_string())
                        .metadata(RETIRED_VERSION_ATTRIBUTE, &version)
                        .body(ByteStream::from(Bytes::new()))
                        .set_request_payer(owner.request_payer.clone()).send().await;
                    if !matches!(owner.inspect_versioned_retirement_head(&path, &version).await?,
                        VersionedRetirementHead::Tombstone(prior) if prior == token)
                    {
                        return Err(unavailable(match reply {
                            Ok(_) => "The versioned object tombstone was not observed".to_owned(),
                            Err(error) => format!("Installing the versioned object tombstone failed: {error}"),
                        }));
                    }
                }
                _ => return Err(unavailable("The current object differs from the authorized version")),
            }
            owner.require_versioned().await?;
            owner.retire_version_at_path(&path, &version).await?;
            if !matches!(owner.inspect_versioned_retirement_head(&path, &version).await?,
                VersionedRetirementHead::Tombstone(prior) if prior == token)
            {
                return Err(unavailable("The versioned object lost its terminal fence"));
            }
            Ok(())
        }).await
    }

    async fn versioned_retirement_head(&self, path: &str, version: &str) -> Result<VersionedRetirementHead> {
        self.inspect((path.to_owned(), version.to_owned()), |owner, (path, version)| async move {
            owner.inspect_versioned_retirement_head(&path, &version).await
        }).await
    }

    async fn inspect_versioned_retirement_head(&self, path: &str, version: &str) -> Result<VersionedRetirementHead> {
        let head = match self.client.head_object().bucket(&self.namespace.bucket).key(path)
            .set_request_payer(self.request_payer.clone()).send().await
        {
            Ok(head) => head,
            Err(error) if error.as_service_error().is_some_and(|error| error.is_not_found()) =>
                return Ok(VersionedRetirementHead::Changed),
            Err(error) => return Err(unavailable(format!("Checking the versioned tombstone failed: {error}"))),
        };
        if let Some(marker) = head.metadata().and_then(|metadata| metadata.get(S3_RETIREMENT_ATTRIBUTE)) {
            let valid = head.content_length() == Some(0)
                && head.version_id().is_some_and(|seen| seen != version && valid_object_version(seen.as_bytes()))
                && head.metadata().and_then(|metadata| metadata.get(RETIRED_VERSION_ATTRIBUTE)).is_some_and(|seen| seen == version);
            return Ok(match (valid, uuid::Uuid::parse_str(marker)) {
                (true, Ok(token)) if !token.is_nil() => VersionedRetirementHead::Tombstone(token),
                _ => VersionedRetirementHead::Changed,
            });
        }
        if head.version_id() == Some(version)
            && let Some(tag) = head.e_tag().filter(|tag| strong_entity_tag(tag))
        {
            return Ok(VersionedRetirementHead::Live(tag.to_owned()));
        }
        Ok(VersionedRetirementHead::Changed)
    }

    async fn retire_version_at_path(&self, path: &str, version: &str) -> Result<()> {
        match self.version_status(path, version).await? {
            VersionStatus::Absent => return Ok(()),
            VersionStatus::WouldExposeOlderData => return Err(unavailable("Retiring the current version would expose older data")),
            VersionStatus::SafeToRemove => {},
        }
        match self.client.delete_object().bucket(&self.namespace.bucket).key(path).version_id(version)
            .set_request_payer(self.request_payer.clone()).send().await
        {
            Ok(_) => {},
            Err(error) if error.as_service_error().and_then(|error| error.code()) == Some("NoSuchVersion") => {},
            Err(error) => return Err(unavailable(format!("Removing the observed version failed: {error}"))),
        }
        if self.version_status(path, version).await? != VersionStatus::Absent {
            return Err(unavailable("The retired object version is still present"));
        }
        Ok(())
    }

    fn observed_path(&self, key: &ObjectKey, observed: &PhysicalObjectVersion) -> Result<Path> {
        let path = self.object_path(key)?;
        if observed.namespace != self.namespace || observed.key != path.as_ref() || !valid_object_version(observed.version.as_bytes()) {
            return Err(unavailable("The retirement does not identify an immutable version of this key"));
        }
        Ok(path)
    }

    fn object_path(&self, key: &ObjectKey) -> rtdb_storage::Result<Path> {
        self.key_layout.provider_path(&format!("{}/{key}", self.root))
    }

    async fn version_status(&self, key: &str, version: &str) -> Result<VersionStatus> {
        self.inspect((key.to_owned(), version.to_owned()), |owner, (key, version)| async move { owner.observe_version_status(&key, &version).await }).await
    }

    async fn observe_version_status(&self, key: &str, version: &str) -> Result<VersionStatus> {
        let mut marker: (Option<String>, Option<String>) = (None, None);
        let mut seen = BTreeSet::new();
        let mut target_latest = None;
        let mut other_data = false;
        loop {
            let page = self.client.list_object_versions().bucket(&self.namespace.bucket).prefix(key).max_keys(1000)
                .set_key_marker(marker.0.clone()).set_version_id_marker(marker.1.clone())
                .set_request_payer(self.request_payer.clone()).send().await
                .map_err(|error| unavailable(format!("Checking the retired version failed: {error}")))?;
            for (listed_key, listed_version, latest, data) in page.versions().iter().map(|entry| (entry.key(), entry.version_id(), entry.is_latest(), true))
                .chain(page.delete_markers().iter().map(|entry| (entry.key(), entry.version_id(), entry.is_latest(), false)))
            {
                let listed_key = listed_key.filter(|listed| listed.starts_with(key))
                    .ok_or_else(|| unavailable("S3 listed a key outside the requested prefix"))?;
                let listed_version = listed_version.filter(|listed| !listed.is_empty())
                    .ok_or_else(|| unavailable("S3 listed a key without its version identifier"))?;
                if listed_key != key { continue; }
                if listed_version == version {
                    if !data || target_latest.is_some() { return Err(unavailable("S3 returned an inconsistent data version")); }
                    target_latest = Some(latest.ok_or_else(|| unavailable("S3 omitted whether the data version is current"))?);
                } else if data {
                    other_data = true;
                }
            }
            match page.is_truncated() {
                Some(false) => return Ok(exact_version_status(target_latest, other_data)),
                Some(true) => {},
                None => return Err(unavailable("S3 omitted the version listing's pagination state")),
            }
            let next_key = page.next_key_marker().filter(|next| next.starts_with(key))
                .ok_or_else(|| unavailable("S3 omitted a usable version listing key marker"))?;
            let next_version = page.next_version_id_marker().filter(|next| !next.is_empty());
            if (next_key == key && next_version.is_none()) || marker.0.as_deref().is_some_and(|previous| next_key < previous) {
                return Err(unavailable("S3 returned an incomplete version listing marker"));
            }
            marker = (Some(next_key.to_owned()), next_version.map(str::to_owned));
            if !seen.insert(marker.clone()) {
                return Err(unavailable("S3 repeated a version listing marker"));
            }
        }
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

    /// Aborts every multipart upload under the prefix and confirms that none
    /// remains, so no upload begun earlier can still complete there.
    pub async fn drain_uploads(&self, scope: &ObjectKey) -> Result<()> {
        let prefix = self.prefix(scope);
        self.abort_uploads(&prefix).await?;
        if self.multipart_batch(&prefix).await?.is_empty() { Ok(()) }
        else { Err(unavailable("A multipart upload is still listed after draining the prefix")) }
    }

    /// Exact-version retirement needs every overwrite and deletion to leave
    /// an addressable version, which only an enabled configuration provides.
    pub async fn require_versioned(&self) -> Result<()> {
        let reply = self.client.get_bucket_versioning().bucket(&self.namespace.bucket).send().await
            .map_err(|error| unavailable(format!("Checking bucket versioning failed: {error}")))?;
        if reply.status() != Some(&aws_sdk_s3::types::BucketVersioningStatus::Enabled) || self.terminal_allocation.is_some() {
            return Err(unavailable("The serving bucket does not retain exact object versions"));
        }
        Ok(())
    }

    /// Observes the version and multipart inventories without modifying either.
    pub async fn is_empty(&self, deployment: &ObjectKey) -> Result<bool> {
        let prefix = self.prefix(deployment);
        self.prefix_is_empty(&prefix).await
    }

    async fn prefix_is_empty(&self, prefix: &str) -> Result<bool> {
        if !self.multipart_batch(prefix).await?.is_empty() { return Ok(false); }
        Ok(self.version_batch(prefix).await?.is_empty())
    }

    /// Finds immediate directories even when every object under them is
    /// represented only by a noncurrent version, a tombstone or an upload.
    pub async fn child_directories(&self, parent: &ObjectKey) -> Result<BTreeSet<String>> {
        self.inspect(parent.clone(), |owner, target| async move { owner.inspect_child_directories(&target).await }).await
    }

    async fn inspect_child_directories(&self, parent: &ObjectKey) -> Result<BTreeSet<String>> {
        let prefix = self.prefix(parent);
        let mut directories = BTreeSet::new();
        let mut marker = (None, None);
        loop {
            let page = self.client.list_object_versions().bucket(&self.namespace.bucket).prefix(&prefix)
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
            let page = self.client.list_multipart_uploads().bucket(&self.namespace.bucket).max_uploads(1000)
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
        self.inspect(prefix.to_owned(), |owner, target| async move { owner.inspect_multipart_batch(&target).await }).await
    }

    async fn inspect_multipart_batch(&self, prefix: &str) -> Result<Vec<(String, String)>> {
        let mut marker = (None, None);
        loop {
            let page = self.client.list_multipart_uploads().bucket(&self.namespace.bucket).max_uploads(1000)
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
            match self.client.abort_multipart_upload().bucket(&self.namespace.bucket).key(key).upload_id(id)
                .set_request_payer(self.request_payer.clone()).send().await
            {
                Ok(_) => {}
                Err(error) if error.as_service_error().is_some_and(|error| error.is_no_such_upload()) => {}
                Err(error) => return Err(unavailable(format!("Aborting a multipart upload failed: {error}"))),
            }
            match self.client.list_parts().bucket(&self.namespace.bucket).key(key).upload_id(id).max_parts(1)
                .set_request_payer(self.request_payer.clone()).send().await
            {
                Ok(_) => {}
                Err(error) if error.as_service_error().and_then(|error| error.code()) == Some("NoSuchUpload") => return Ok(()),
                Err(error) => return Err(unavailable(format!("Checking an aborted multipart upload failed: {error}"))),
            }
        }
        Err(unavailable("The aborted multipart upload is still addressable"))
    }

    async fn complete_conditional(&self, path: &Path, upload: &str, bytes: Bytes, condition: &S3TombstoneCondition) -> rtdb_storage::Result<PublicationResult> {
        let key = path.as_ref();
        let part = self.client.upload_part().bucket(&self.namespace.bucket).key(key).upload_id(upload)
            .part_number(1).checksum_algorithm(ChecksumAlgorithm::Sha256).body(ByteStream::from(bytes))
            .set_request_payer(self.request_payer.clone()).send().await
            .map_err(|error| publication_failure(path, error.to_string()))?;
        let tag = part.e_tag().ok_or_else(|| publication_failure(path, "S3 returned an uploaded part without an ETag"))?;
        let checksum = part.checksum_sha256().ok_or_else(|| publication_failure(path, "S3 returned an uploaded part without its checksum"))?;
        let parts = CompletedMultipartUpload::builder()
            .parts(CompletedPart::builder().part_number(1).e_tag(tag).checksum_sha256(checksum).build())
            .build();
        let request = self.client.complete_multipart_upload().bucket(&self.namespace.bucket).key(key).upload_id(upload)
            .multipart_upload(parts).set_request_payer(self.request_payer.clone());
        let request = match condition {
            S3TombstoneCondition::Absent => request.if_none_match("*"),
            S3TombstoneCondition::Matches(tag) => request.if_match(tag),
        };
        match request.send().await {
            Ok(output) => Ok(PublicationResult::Published {
                version: output.version_id().map(str::to_owned), etag: output.e_tag().map(str::to_owned),
            }),
            Err(error) if error.as_service_error().and_then(|error| error.code()) == Some("PreconditionFailed") => Ok(PublicationResult::Conflict),
            Err(error) => Err(publication_failure(path, error.to_string())),
        }
    }

    async fn purge_versions(&self, prefix: &str) -> Result<()> {
        let mut previous = Vec::new();
        loop {
            let versions = self.version_batch(prefix).await?;
            if versions.is_empty() { return Ok(()); }
            if versions == previous { return Err(unavailable("A batch of deleted versions is still listed")); }
            let delete = Delete::builder().set_objects(Some(versions.clone())).quiet(true).build()
                .map_err(|error| unavailable(error.to_string()))?;
            let reply = self.client.delete_objects().bucket(&self.namespace.bucket).delete(delete)
                .set_request_payer(self.request_payer.clone()).send().await
                .map_err(|error| unavailable(format!("Deleting object versions failed: {error}")))?;
            if let Some(error) = reply.errors().first() {
                return Err(unavailable(format!("S3 refused a version deletion: {}", error.code().unwrap_or("unknown error"))));
            }
            previous = versions;
        }
    }

    async fn version_batch(&self, prefix: &str) -> Result<Vec<ObjectIdentifier>> {
        self.inspect(prefix.to_owned(), |owner, target| async move { owner.inspect_version_batch(&target).await }).await
    }

    async fn inspect_version_batch(&self, prefix: &str) -> Result<Vec<ObjectIdentifier>> {
        let page = self.client.list_object_versions().bucket(&self.namespace.bucket).prefix(prefix).max_keys(1000)
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
        let reserved = self.client.create_multipart_upload().bucket(&self.namespace.bucket).key(key)
            .metadata("rtdb-publication", attempt.to_string()).checksum_algorithm(ChecksumAlgorithm::Sha256)
            .set_request_payer(self.request_payer.clone()).send().await
            .map_err(|error| publication_failure(path, error.to_string()))?;
        reserved.upload_id().filter(|id| !id.is_empty()).map(str::to_owned)
            .ok_or_else(|| publication_failure(path, "S3 did not identify the reserved upload"))
    }

    async fn complete(&self, path: &Path, upload: &str, bytes: Bytes) -> rtdb_storage::Result<PublicationResult> {
        self.complete_conditional(path, upload, bytes, &S3TombstoneCondition::Absent).await
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
    async fn an_exact_version_retires_only_when_the_fresh_inventory_confirms_absence() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let mut config = S3Config::new("cell");
        config.endpoint = Some(endpoint.clone());
        config.region = Some("us-east-1".into());
        config.credentials = Some(S3Credentials { access_key: "test".into(), secret_key: "test-secret".into() });
        let inventory = config.versioned_inventory(&ObjectKey::new("encoded root").unwrap(), &BTreeMap::new()).await.unwrap();
        let key = ObjectKey::new("data/object").unwrap();
        let observed = PhysicalObjectVersion {
            namespace: S3ObjectNamespace { bucket: "cell".into(), endpoint: Some(format!("{endpoint}/")), region: config.region },
            key: "encoded root/data/object".into(),
            version: "old+/=".into(),
        };
        let provider_key = observed.key.clone();
        let server = tokio::spawn(async move {
            let present = "<IsTruncated>false</IsTruncated><Version><Key>encoded root/data/object</Key><VersionId>old+/=</VersionId><IsLatest>false</IsLatest></Version>";
            let exposes_older = "<IsTruncated>false</IsTruncated><Version><Key>encoded root/data/object</Key><VersionId>old+/=</VersionId><IsLatest>true</IsLatest></Version><Version><Key>encoded root/data/object</Key><VersionId>older</VersionId><IsLatest>false</IsLatest></Version>";
            let continued = "<IsTruncated>true</IsTruncated><NextKeyMarker>encoded root/data/object</NextKeyMarker><NextVersionIdMarker>new</NextVersionIdMarker><Version><Key>encoded root/data/object</Key><VersionId>new</VersionId></Version>";
            let absent = "<IsTruncated>false</IsTruncated><Version><Key>encoded root/data/object-neighbor</Key><VersionId>old+/=</VersionId></Version>";
            let replacement_only = "<IsTruncated>false</IsTruncated><Version><Key>encoded root/data/object</Key><VersionId>new</VersionId><IsLatest>true</IsLatest></Version>";
            let incomplete = "<IsTruncated>true</IsTruncated><NextKeyMarker>encoded root/data/object</NextKeyMarker>";
            for (method, status, page, cursor) in [
                ("GET", "200 OK", Some(present), false),
                ("GET", "200 OK", Some(exposes_older), false),
                ("GET", "200 OK", Some(present), false),
                ("DELETE", "204 No Content", None, false),
                ("GET", "200 OK", Some(present), false),
                ("GET", "200 OK", Some(present), false),
                ("DELETE", "204 No Content", None, false),
                ("GET", "200 OK", Some(incomplete), false),
                ("GET", "200 OK", Some(present), false),
                ("DELETE", "204 No Content", None, false),
                ("GET", "200 OK", Some(continued), false),
                ("GET", "200 OK", Some(absent), true),
                ("GET", "200 OK", Some(replacement_only), false),
            ] {
                let (stream, _) = listener.accept().await.unwrap();
                let mut reader = BufReader::new(stream);
                let mut line = String::new();
                reader.read_line(&mut line).await.unwrap();
                let mut request = line.split_whitespace();
                assert_eq!(request.next(), Some(method));
                let url = url::Url::parse(&format!("http://localhost{}", request.next().unwrap())).unwrap();
                let query: BTreeMap<_, _> = url.query_pairs().collect();
                if method == "DELETE" {
                    assert_eq!(url.path(), "/cell/encoded%20root/data/object");
                    assert_eq!(query.get("versionId").map(|value| value.as_ref()), Some("old+/="));
                } else {
                    assert_eq!(query.get("prefix").map(|value| value.as_ref()), Some(provider_key.as_str()));
                    assert_eq!(query.get("key-marker").map(|value| value.as_ref()), cursor.then_some(provider_key.as_str()));
                    assert_eq!(query.get("version-id-marker").map(|value| value.as_ref()), cursor.then_some("new"));
                }
                while line != "\r\n" {
                    line.clear();
                    assert_ne!(reader.read_line(&mut line).await.unwrap(), 0);
                }
                let body = page.map(|page| format!("<ListVersionsResult>{page}</ListVersionsResult>")).unwrap_or_default();
                let reply = format!("HTTP/1.1 {status}\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                reader.get_mut().write_all(reply.as_bytes()).await.unwrap();
            }
        });
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            for rejected in [
                PhysicalObjectVersion { key: "encoded root/data/other".into(), ..observed.clone() },
                PhysicalObjectVersion { namespace: S3ObjectNamespace { bucket: "other".into(), ..observed.namespace.clone() }, ..observed.clone() },
                PhysicalObjectVersion { version: "null".into(), ..observed.clone() },
            ] {
                let error = inventory.retire_observed_version(&key, &rejected).await.expect_err("unbound retirement reached the provider");
                assert_eq!(error.message.as_deref(), Some("The retirement does not identify an immutable version of this key"));
            }
            assert!(!inventory.exact_retirement_absent(&key, &observed).await.unwrap());
            let error = inventory.retire_observed_version(&key, &observed).await.expect_err("the current version must keep any older data hidden");
            assert_eq!(error.message.as_deref(), Some("Retiring the current version would expose older data"));
            let error = inventory.retire_observed_version(&key, &observed).await.expect_err("a delete reply alone must not settle retirement");
            assert_eq!(error.message.as_deref(), Some("The retired object version is still present"));
            let error = inventory.retire_observed_version(&key, &observed).await.expect_err("an incomplete inventory must not settle retirement");
            assert_eq!(error.message.as_deref(), Some("S3 returned an incomplete version listing marker"));
            inventory.retire_observed_version(&key, &observed).await.expect("pagination must bind the key and version together");
            assert!(inventory.exact_retirement_absent(&key, &observed).await.unwrap());
            server.await.unwrap();
        }).await.unwrap();
    }

    #[tokio::test]
    async fn retiring_one_epoch_prefix_drains_and_purges_only_that_epoch() {
        let Ok(endpoint) = std::env::var("RTDB_S3_ENDPOINT") else { return };
        let endpoint_url = url::Url::parse(&endpoint).unwrap();
        if !matches!(endpoint_url.host(), Some(url::Host::Ipv4(host)) if host.is_loopback()) { return; }
        let remotes = BTreeMap::<String, S3Credentials>::new();
        let root = ObjectKey::new("cell").unwrap();
        let open = |bucket: &str| {
            let mut config = S3Config::new(bucket);
            config.endpoint = Some(endpoint.clone());
            config.region = Some("us-east-1".into());
            config.path_style_access_enabled = true;
            config
        };
        let unversioned = format!("rtdb-epoch-plain-{}", uuid::Uuid::new_v4().simple());
        let plain = open(&unversioned).versioned_inventory(&root, &remotes).await.unwrap();
        plain.client.create_bucket().bucket(&unversioned).send().await.unwrap();
        assert!(plain.require_versioned().await.is_err(), "an unversioned bucket cannot retire exact versions");

        let bucket = format!("rtdb-epoch-test-{}", uuid::Uuid::new_v4().simple());
        let config = open(&bucket);
        let inventory = config.versioned_inventory(&root, &remotes).await.unwrap();
        let client = &inventory.client;
        client.create_bucket().bucket(&bucket).send().await.unwrap();
        client.put_bucket_versioning().bucket(&bucket)
            .versioning_configuration(VersioningConfiguration::builder().status(BucketVersioningStatus::Enabled).build())
            .send().await.unwrap();
        inventory.require_versioned().await.unwrap();
        let physical = |key: &str| Path::from(format!("{}/{key}", inventory.root)).to_string();
        for key in ["prefix/1/v/block", "prefix/10/v/block"] {
            client.put_object().bucket(&bucket).key(physical(key)).body(ByteStream::from_static(b"block")).send().await.unwrap();
        }
        let mut uploads = Vec::new();
        for key in ["prefix/1/v/pending", "prefix/10/v/pending"] {
            let upload = client.create_multipart_upload().bucket(&bucket).key(physical(key)).send().await.unwrap();
            uploads.push(upload.upload_id().unwrap().to_owned());
        }
        let retired = ObjectKey::new("prefix/1").unwrap();
        let successor = ObjectKey::new("prefix/10").unwrap();
        inventory.drain_uploads(&retired).await.unwrap();
        let listed = client.list_multipart_uploads().bucket(&bucket).send().await.unwrap();
        let remaining: Vec<_> = listed.uploads().iter().filter_map(|upload| upload.upload_id()).collect();
        assert_eq!(remaining, [uploads[1].as_str()], "the successor's upload survives");
        inventory.purge(&retired).await.unwrap();
        assert!(inventory.is_empty(&retired).await.unwrap());
        assert!(!inventory.is_empty(&successor).await.unwrap());
        let survivor = client.get_object().bucket(&bucket).key(physical("prefix/10/v/block")).send().await.unwrap();
        assert_eq!(survivor.body.collect().await.unwrap().into_bytes().as_ref(), b"block");
        inventory.purge(&successor).await.unwrap();
        client.delete_bucket().bucket(&bucket).send().await.unwrap();
        client.delete_bucket().bucket(&unversioned).send().await.unwrap();
    }

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
        inventory.retire_observed_version(&observed_key, &observed).await.unwrap();
        inventory.retire_observed_version(&observed_key, &observed).await.unwrap();
        assert_eq!(cell.get(&observed_key).await.unwrap().as_ref(), b"new version");
        inventory.purge(&ObjectKey::new("observed").unwrap()).await.unwrap();
        cell.close_and_wait().await;

        let upload_key = ObjectKey::new("managed/upload").unwrap();
        let upload_path = Path::from(format!("{}/{upload_key}", inventory.root));
        let attempt = uuid::Uuid::new_v4();
        let winner = inventory.reserve(&upload_path, attempt).await.unwrap();
        let published = inventory.complete(&upload_path, &winner, Bytes::from_static(b"sealed")).await.unwrap();
        let PublicationResult::Published { version: Some(published_version), .. } = published else { panic!("versioned completion lacked its version") };
        let loser = inventory.reserve(&upload_path, uuid::Uuid::new_v4()).await.unwrap();
        assert_eq!(inventory.complete(&upload_path, &loser, Bytes::from_static(b"other")).await.unwrap(), PublicationResult::Conflict);
        inventory.abort_publication(&upload_key, &loser).await.unwrap();
        inventory.abort_publication(&upload_key, &winner).await.unwrap();
        let retained = client.get_object().bucket(&bucket).key(upload_path.as_ref()).send().await.unwrap();
        assert_eq!(retained.metadata().unwrap()["rtdb-publication"], attempt.to_string());
        assert_eq!(retained.version_id(), Some(published_version.as_str()));
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
