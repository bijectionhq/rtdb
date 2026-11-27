use bytes::Bytes;
use ring::digest::{SHA256, digest};
use rtdb_format::journal::ObjectId;
use rtdb_types::{Anomaly, AnomalyCategory, PhysicalObjectVersion, Result, S3ObjectNamespace};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

use super::{
    Ledger, MAX_ATTEMPTS, ObjectKey, Path, RankedAdmission, RankedAdmissionScope, closed,
    contention, invalid,
};
use crate::remote::{PublicationResult, PublicationUploads, valid_object_version};

const MAX_UPLOAD_ID_BYTES: usize = 2048;

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
    attempt: Uuid,
    upload_id: String,
}

impl RankedStagingUpload {
    pub(super) fn object(&self, scope: RankedAdmissionScope) -> Result<ObjectId> {
        let object: ObjectId = self.key.parse()?;
        if object.database != scope.incarnation
            || self.attempt.is_nil()
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
        object: ObjectId,
        bytes: Bytes,
    ) -> Result<PhysicalObjectVersion> {
        require_ciphertext(object, &bytes)?;
        let owner = self.clone();
        let result = self
            .region
            .run_async(async move {
                Ok(async {
                    let upload = owner.reserve_upload(object).await?;
                    owner.complete_upload(&upload, object, bytes).await
                }
                .await)
            })
            .await??;
        match result {
            PublicationResult::Published {
                version: Some(version),
                ..
            } if valid_object_version(version.as_bytes()) => Ok(PhysicalObjectVersion {
                namespace: self.staging_provider()?.namespace.clone(),
                key: self.staging_path(object)?.to_string(),
                version,
            }),
            _ => Err(Anomaly::new(
                AnomalyCategory::Conflict,
                "rtdb.storage/ranked-staging-version",
                "The original provider did not publish the exact staged object version",
            )),
        }
    }

    /// Reserves and records an empty provider upload.
    /// Only the scope's recorded upload may submit bytes.
    pub async fn reserve_staging(&self, object: ObjectId) -> Result<RankedStagingUpload> {
        let owner = self.clone();
        self.region
            .run_async(async move { Ok(owner.reserve_upload(object).await) })
            .await?
    }

    /// Submits the exact object's bytes to its recorded upload while admission is open.
    pub async fn complete_staging(
        &self,
        upload: RankedStagingUpload,
        bytes: Bytes,
    ) -> Result<PublicationResult> {
        let object = upload.object(self.scope)?;
        require_ciphertext(object, &bytes)?;
        let owner = self.clone();
        self.region
            .run_async(async move { Ok(owner.complete_upload(&upload, object, bytes).await) })
            .await?
    }

    async fn reserve_upload(&self, object: ObjectId) -> Result<RankedStagingUpload> {
        let path = self.staging_path(object)?;
        require_open_slot(&self.root().await?.0)?;
        let provider = self.staging_provider()?.uploads.as_ref();
        provider.require_versioned().await?;
        let attempt = Uuid::new_v4();
        let upload = RankedStagingUpload {
            key: object.to_string(),
            attempt,
            upload_id: provider.reserve(&path, attempt).await?,
        };
        if let Err(error) = self.record_staging(&upload).await {
            provider.abort(&path, &upload.upload_id).await?;
            self.clear_staging(&upload).await?;
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
    ) -> Result<PublicationResult> {
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
        if !matches!(&result, Ok(PublicationResult::Published { .. })) {
            provider.abort(&path, &upload.upload_id).await?;
        }
        if !self.clear_staging(upload).await? {
            return Err(closed());
        }
        Ok(result?)
    }

    pub(super) async fn drain_staging(&self, upload: &RankedStagingUpload) -> Result<()> {
        let path = self.staging_path(upload.object(self.scope)?)?;
        self.staging_provider()?
            .uploads
            .abort(&path, &upload.upload_id)
            .await?;
        self.clear_staging(upload).await?;
        Ok(())
    }

    async fn clear_staging(&self, upload: &RankedStagingUpload) -> Result<bool> {
        for _ in 0..MAX_ATTEMPTS {
            let (mut ledger, version) = self.root().await?;
            if ledger.staging.as_ref() != Some(upload) {
                return Ok(false);
            }
            ledger.staging = None;
            if self.replace_root(&ledger, version).await? {
                return Ok(!ledger.closed);
            }
        }
        Err(contention())
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
