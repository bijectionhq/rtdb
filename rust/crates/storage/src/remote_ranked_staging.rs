use bytes::Bytes;
use futures::{StreamExt, stream};
use ring::digest::{Context, SHA256, digest};
use rtdb_format::journal::ObjectId;
use rtdb_types::{Anomaly, AnomalyCategory, PhysicalObjectVersion, Result, S3ObjectNamespace};
use serde::{Deserialize, Serialize};
use std::{ops::Range, sync::Arc};
use uuid::Uuid;

use super::{
    ClosedRankedAdmission, INVENTORY_READ_CONCURRENCY, Ledger, MAX_ATTEMPTS, ObjectKey, PAGE_SIZE,
    Path, RankedAdmission, RankedAdmissionScope, closed, contention, invalid,
};
use crate::ranked_admission::{
    RankedAttempt, RankedStagedObject, RankedStagingInventory, RankedStagingPage,
};
use crate::remote::{PublicationResult, PublicationUploads, valid_object_version};

const MAX_UPLOAD_ID_BYTES: usize = 2048;
const MAX_VERSION_RECORD_BYTES: u64 = 4096;

#[derive(Clone)]
pub(super) struct StagingProvider {
    namespace: S3ObjectNamespace,
    uploads: Arc<dyn PublicationUploads>,
}

/// The exact provider capability for one staged ranked object.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RankedStagingUpload {
    key: String,
    cut: RankedAttempt,
    attempt: Uuid,
    upload_id: String,
}

impl RankedStagingUpload {
    pub(super) fn object(&self, scope: RankedAdmissionScope) -> Result<ObjectId> {
        let object = staged_object(scope, self.cut, &self.key)?;
        if self.attempt.is_nil()
            || !(1..=MAX_UPLOAD_ID_BYTES).contains(&self.upload_id.len())
            || !self
                .upload_id
                .bytes()
                .all(|byte| byte.is_ascii_graphic() && !matches!(byte, b'"' | b'\\'))
        {
            return Err(invalid());
        }
        Ok(object)
    }
}

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RecordedVersion {
    key: String,
    cut: RankedAttempt,
    upload_attempt: Uuid,
    version: String,
}

impl RecordedVersion {
    fn new(upload: &RankedStagingUpload, version: String) -> Result<Self> {
        if !valid_object_version(version.as_bytes()) {
            return Err(invalid());
        }
        Ok(Self {
            key: upload.key.clone(),
            cut: upload.cut,
            upload_attempt: upload.attempt,
            version,
        })
    }

    fn bytes(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).map_err(|_| invalid())
    }
}

impl RankedAdmission {
    /// Attaches uploads for the ledger's original provider allocation.
    /// The caller must establish that both handles name the same allocation.
    pub fn with_staging_uploads(
        mut self,
        namespace: S3ObjectNamespace,
        uploads: Arc<dyn PublicationUploads>,
    ) -> Self {
        self.uploads = Some(StagingProvider { namespace, uploads });
        self
    }

    pub(super) async fn stage_object(
        &self,
        cut: RankedAttempt,
        object: ObjectId,
        bytes: Bytes,
    ) -> Result<PhysicalObjectVersion> {
        require_ciphertext(object, &bytes)?;
        let owner = self.clone();
        self.region
            .run_async(async move {
                Ok(async {
                    let upload = owner.reserve_upload(cut, object).await?;
                    owner.complete_upload(&upload, object, bytes).await
                }
                .await)
            })
            .await?
    }

    /// Reserves and records an empty provider upload.
    /// Only the scope's recorded upload may submit bytes.
    pub async fn reserve_staging(
        &self,
        cut: RankedAttempt,
        object: ObjectId,
    ) -> Result<RankedStagingUpload> {
        let owner = self.clone();
        self.region
            .run_async(async move { Ok(owner.reserve_upload(cut, object).await) })
            .await?
    }

    /// Submits the exact object's bytes to its recorded upload while admission is open.
    pub async fn complete_staging(
        &self,
        upload: RankedStagingUpload,
        bytes: Bytes,
    ) -> Result<PhysicalObjectVersion> {
        let object = upload.object(self.scope)?;
        require_ciphertext(object, &bytes)?;
        let owner = self.clone();
        self.region
            .run_async(async move { Ok(owner.complete_upload(&upload, object, bytes).await) })
            .await?
    }

    async fn reserve_upload(
        &self,
        cut: RankedAttempt,
        object: ObjectId,
    ) -> Result<RankedStagingUpload> {
        let path = self.staging_path(object)?;
        check_cut(cut, object)?;
        let (ledger, _) = self.root().await?;
        if !ledger.closed && let Some(upload) = ledger.staging.as_ref().filter(|upload| upload.cut.epoch < cut.epoch) {
            self.drain_staging(upload).await?;
            require_open_slot(&self.root().await?.0)?;
        } else {
            require_open_slot(&ledger)?;
        }
        let provider = self.staging_provider()?.uploads.as_ref();
        provider.require_versioned().await?;
        let attempt = Uuid::new_v4();
        let upload = RankedStagingUpload {
            key: object.to_string(),
            cut,
            attempt,
            upload_id: provider.reserve(&path, attempt).await?,
        };
        if let Err(error) = self.record_staging(&upload).await {
            provider.abort(&path, &upload.upload_id).await?;
            self.finish_staging(&upload, None).await?;
            return Err(error);
        }
        Ok(upload)
    }

    async fn record_staging(&self, upload: &RankedStagingUpload) -> Result<()> {
        upload.object(self.scope)?;
        for _ in 0..MAX_ATTEMPTS {
            let (mut ledger, version) = self.root().await?;
            require_open_slot(&ledger)?;
            ledger.staging = Some(upload.clone());
            if self.replace_root(&ledger, version).await? {
                return Ok(());
            }
        }
        Err(contention())
    }

    async fn complete_upload(
        &self,
        upload: &RankedStagingUpload,
        object: ObjectId,
        bytes: Bytes,
    ) -> Result<PhysicalObjectVersion> {
        let ledger = self.root().await?.0;
        if ledger.closed {
            return Err(closed());
        }
        if ledger.staging.as_ref() != Some(upload) {
            return Err(invalid());
        }
        let path = self.staging_path(object)?;
        let provider = self.staging_provider()?.uploads.as_ref();
        let result = provider.complete(&path, &upload.upload_id, bytes).await;
        let version = match result.as_ref() {
            Ok(PublicationResult::Published {
                version: Some(version),
                ..
            }) if valid_object_version(version.as_bytes()) => {
                Some(RecordedVersion::new(upload, version.clone())?)
            }
            _ => self.inspect_drained(upload, &path).await?,
        };
        if !self.finish_staging(upload, version.as_ref()).await? {
            return Err(closed());
        }
        match version {
            Some(version) => self.physical_version(object, version.version),
            None => {
                result?;
                Err(Anomaly::new(
                    AnomalyCategory::Conflict,
                    "rtdb.storage/ranked-staging-version",
                    "The original provider did not publish the exact staged object version",
                ))
            }
        }
    }

    pub(super) async fn drain_staging(&self, upload: &RankedStagingUpload) -> Result<()> {
        let path = self.staging_path(upload.object(self.scope)?)?;
        let version = self.inspect_drained(upload, &path).await?;
        self.finish_staging(upload, version.as_ref()).await?;
        Ok(())
    }

    async fn inspect_drained(
        &self,
        upload: &RankedStagingUpload,
        path: &Path,
    ) -> Result<Option<RecordedVersion>> {
        let provider = self.staging_provider()?.uploads.as_ref();
        provider.abort(path, &upload.upload_id).await?;
        provider
            .completed_version(path, upload.attempt)
            .await?
            .map(|version| RecordedVersion::new(upload, version))
            .transpose()
    }

    async fn finish_staging(
        &self,
        upload: &RankedStagingUpload,
        recorded: Option<&RecordedVersion>,
    ) -> Result<bool> {
        for _ in 0..MAX_ATTEMPTS {
            let (mut ledger, version) = self.root().await?;
            if ledger.staging.as_ref() != Some(upload) {
                return Ok(false);
            }
            let completed = ledger
                .completed_staging
                .completed
                .checked_add(1)
                .ok_or_else(invalid)?;
            let prior = match self.staging_entry(completed).await {
                Ok(entry) => Some(entry),
                Err(error) if error.category == AnomalyCategory::NotFound => None,
                Err(error) => return Err(error),
            };
            if let Some(recorded) = recorded.or(prior.as_ref()) {
                if recorded.upload_attempt != upload.attempt
                    || recorded.key != upload.key
                    || recorded.cut != upload.cut
                {
                    return Err(invalid());
                }
                let bytes = recorded.bytes()?;
                let selected = match prior.as_ref() {
                    Some(prior) => prior == recorded,
                    None if self
                        .create(
                            &self.staging_entry_path(completed),
                            Bytes::copy_from_slice(&bytes),
                        )
                        .await? =>
                    {
                        true
                    }
                    None => self.staging_entry(completed).await? == *recorded,
                };
                if !selected {
                    return Err(invalid());
                }
                ledger.completed_staging = RankedStagingInventory {
                    completed,
                    inventory_sha256: inventory_step(
                        ledger.completed_staging.inventory_sha256,
                        &bytes,
                    ),
                };
            }
            ledger.staging = None;
            if self.replace_root(&ledger, version).await? {
                return Ok(!ledger.closed);
            }
        }
        Err(contention())
    }

    /// Verifies the receipt's staging inventory before returning one page of recorded version identities.
    pub async fn staging_page(
        &self,
        receipt: ClosedRankedAdmission,
        after: u64,
        limit: usize,
    ) -> Result<RankedStagingPage> {
        if !(1..=PAGE_SIZE).contains(&limit) || after > receipt.staging.completed {
            return Err(invalid());
        }
        let owner = self.clone();
        self.region
            .run_async(async move {
                Ok(async {
                    let (ledger, _) = owner.root().await?;
                    if !ledger.closed
                        || ledger.staging.is_some()
                        || receipt != owner.receipt(&ledger)
                    {
                        return Err(invalid());
                    }
                    let end = after
                        .saturating_add(limit as u64)
                        .min(receipt.staging.completed);
                    let objects = owner.scan_staging(receipt.staging, after..end).await?;
                    Ok(RankedStagingPage {
                        objects,
                        next_after: (end < receipt.staging.completed).then_some(end),
                    })
                }
                .await)
            })
            .await?
    }

    pub(super) async fn verify_staging(&self, inventory: RankedStagingInventory) -> Result<()> {
        self.scan_staging(inventory, 0..0).await.map(drop)
    }

    async fn scan_staging(
        &self,
        inventory: RankedStagingInventory,
        selection: Range<u64>,
    ) -> Result<Vec<RankedStagedObject>> {
        let mut entries = stream::iter(0..inventory.completed)
            .map(|offset| async move {
                Ok::<_, Anomaly>((offset, self.staging_entry(offset + 1).await?))
            })
            .buffered(INVENTORY_READ_CONCURRENCY);
        let mut digest = [0; 32];
        let mut objects = Vec::with_capacity((selection.end - selection.start) as usize);
        while let Some(entry) = entries.next().await {
            let (offset, recorded) = entry?;
            digest = inventory_step(digest, &recorded.bytes()?);
            if selection.contains(&offset) {
                let object = staged_object(self.scope, recorded.cut, &recorded.key)?;
                objects.push(RankedStagedObject {
                    object,
                    attempt: recorded.cut,
                    physical: self.physical_version(object, recorded.version)?,
                });
            }
        }
        if digest != inventory.inventory_sha256 {
            return Err(Anomaly::fault(
                "rtdb.storage/ranked-staging-inventory",
                "The original ranked staging inventory does not match the closed ledger",
            ));
        }
        Ok(objects)
    }

    fn staging_entry_path(&self, sequence: u64) -> Path {
        self.path
            .clone()
            .join("staged")
            .join(format!("{sequence:016x}"))
    }

    async fn staging_entry(&self, sequence: u64) -> Result<RecordedVersion> {
        let (bytes, _) = self
            .read(&self.staging_entry_path(sequence), MAX_VERSION_RECORD_BYTES)
            .await?;
        let entry: RecordedVersion = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        staged_object(self.scope, entry.cut, &entry.key)?;
        if entry.upload_attempt.is_nil() || !valid_object_version(entry.version.as_bytes()) {
            return Err(invalid());
        }
        Ok(entry)
    }

    fn physical_version(&self, object: ObjectId, version: String) -> Result<PhysicalObjectVersion> {
        Ok(PhysicalObjectVersion {
            namespace: self.staging_provider()?.namespace.clone(),
            key: self.staging_path(object)?.to_string(),
            version,
        })
    }

    fn staging_provider(&self) -> Result<&StagingProvider> {
        self.uploads.as_ref().ok_or_else(|| {
            Anomaly::unsupported(
                "rtdb.storage/ranked-staging-provider",
                "The original ranked allocation requires abortable provider uploads",
            )
        })
    }

    fn staging_path(&self, object: ObjectId) -> Result<Path> {
        if object.database != self.scope.incarnation {
            return Err(invalid());
        }
        Ok(self.objects.path(&ObjectKey::new(object.to_string())?)?)
    }
}

fn staged_object(scope: RankedAdmissionScope, cut: RankedAttempt, key: &str) -> Result<ObjectId> {
    let object: ObjectId = key.parse()?;
    if object.database != scope.incarnation || object.to_string() != key {
        return Err(invalid());
    }
    check_cut(cut, object)?;
    Ok(object)
}

fn check_cut(cut: RankedAttempt, object: ObjectId) -> Result<()> {
    if cut.epoch == 0 || cut.epoch != object.epoch || cut.position == 0 || cut.key_generation == 0 {
        return Err(invalid());
    }
    Ok(())
}

fn inventory_step(previous: [u8; 32], encoded: &[u8]) -> [u8; 32] {
    let mut hash = Context::new(&SHA256);
    hash.update(b"rtdb.ranked-staging-inventory/v1\0");
    hash.update(&previous);
    hash.update(encoded);
    hash.finish().as_ref().try_into().expect("SHA-256 width")
}

fn require_ciphertext(object: ObjectId, bytes: &[u8]) -> Result<()> {
    if digest(&SHA256, bytes).as_ref() != object.digest.as_slice() {
        return Err(invalid());
    }
    Ok(())
}

fn require_open_slot(ledger: &Ledger) -> Result<()> {
    if ledger.closed {
        return Err(closed());
    }
    if ledger.staging.is_some() {
        return Err(Anomaly::new(
            AnomalyCategory::Busy,
            "rtdb.storage/ranked-staging-busy",
            "The original ranked scope already owns a provider upload",
        ));
    }
    Ok(())
}
