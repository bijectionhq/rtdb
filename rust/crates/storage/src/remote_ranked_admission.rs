use std::sync::Arc;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures::{StreamExt, stream};
use object_store::{
    Error as ProviderError, ObjectStoreExt, PutMode, PutOptions,
    UpdateVersion, path::Path,
};
use ring::digest::{Context, SHA256};
use rtdb_format::journal::{ObjectId, ObjectKind, RankedCatalog, Record};
use rtdb_types::{Anomaly, AnomalyCategory, PhysicalObjectVersion, Result};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{RemoteObjects, RemoteStore, provider_error};
use crate::ranked_admission::{RankedAdmissionWriter, RankedAttempt, RankedStagingInventory};
use crate::{IoRegion, ObjectKey};

#[path = "remote_ranked_staging.rs"]
mod staging;
pub use staging::RankedStagingUpload;
use staging::StagingProvider;

const PAGE_SIZE: usize = 100;
const INVENTORY_READ_CONCURRENCY: usize = 16;
const MAX_ATTEMPTS: usize = 12;
const MAX_RECORD_BYTES: u64 = 1024;
const MAX_ROOT_BYTES: u64 = 4096;

pub use crate::ranked_admission::{
    ClosedRankedAdmission, RankedAdmissionPage, RankedAdmissionScope,
};

/// The original store orders publication and closure on one conditional root.
#[derive(Clone)]
pub struct RankedAdmission {
    objects: Arc<RemoteObjects>,
    region: Arc<IoRegion>,
    scope: RankedAdmissionScope,
    key: ObjectKey,
    path: Path,
    uploads: Option<StagingProvider>,
}

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Ledger {
    count: u64,
    inventory_sha256: [u8; 32],
    closed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    staging: Option<RankedStagingUpload>,
    #[serde(default, skip_serializing_if = "RankedStagingInventory::is_empty")]
    completed_staging: RankedStagingInventory,
}

impl RemoteStore {
    pub fn ranked_admission(&self, scope: RankedAdmissionScope) -> Result<RankedAdmission> {
        if scope.incarnation.is_nil() || scope.activation_sha256 == [0; 32] {
            return Err(invalid());
        }
        let identity = URL_SAFE_NO_PAD.encode(scope.activation_sha256);
        let key = ObjectKey::new(format!(
            "ranked-admission/v1/{identity}/{}",
            scope.incarnation
        ))?;
        Ok(RankedAdmission {
            objects: Arc::clone(&self.objects),
            region: self.io.clone(),
            scope,
            path: self.objects.path(&key)?,
            key,
            uploads: None,
        })
    }
}

impl RankedAdmission {
    pub async fn commission(&self) -> Result<()> {
        let owner = self.clone();
        self.region
            .run_async(async move {
                Ok(async {
                    let root = owner.path.clone().join("root");
                    owner.create(&root, encode_root(&Ledger::default())?).await?;
                    let (ledger, _) = owner.root().await?;
                    if ledger.closed {
                        return Err(closed());
                    }
                    Ok(())
                }
                .await)
            })
            .await?
    }

    /// An exact retry returns the original admission, including after closure.
    pub async fn publish(&self, catalogue: RankedCatalog) -> Result<u64> {
        let owner = self.clone();
        self.region
            .run_async(async move { Ok(owner.publish_catalogue(catalogue).await) })
            .await?
    }

    /// Closes admission, drains the recorded upload and verifies the committed inventory.
    /// A drain or verification error leaves admission closed for retry.
    pub async fn close(&self) -> Result<ClosedRankedAdmission> {
        let owner = self.clone();
        self.region
            .run_async(async move { Ok(owner.close_admission().await) })
            .await?
    }

    /// Reads the next page of the provider's immutable admission prefix.
    pub async fn outcomes(
        &self,
        scope: ClosedRankedAdmission,
        after: u64,
    ) -> Result<RankedAdmissionPage> {
        self.outcome_page(scope, after, PAGE_SIZE).await
    }

    pub async fn outcome_page(
        &self,
        scope: ClosedRankedAdmission,
        after: u64,
        limit: usize,
    ) -> Result<RankedAdmissionPage> {
        if !(1..=PAGE_SIZE).contains(&limit) {
            return Err(invalid());
        }
        let owner = self.clone();
        self.region
            .run_async(async move {
                Ok(async {
                    let (ledger, _) = owner.root().await?;
                    if !ledger.closed || ledger.staging.is_some() || scope != owner.receipt(&ledger) || after > ledger.count {
                        return Err(invalid());
                    }
                    let end = after.saturating_add(limit as u64).min(ledger.count);
                    let mut catalogues = Vec::with_capacity((end - after) as usize);
                    for sequence in after..end {
                        catalogues.push(owner.entry(sequence + 1).await?);
                    }
                    Ok(RankedAdmissionPage {
                        catalogues,
                        next_after: (end < ledger.count).then_some(end),
                    })
                }
                .await)
            })
            .await?
    }

    async fn publish_catalogue(&self, catalogue: RankedCatalog) -> Result<u64> {
        self.check_catalogue(catalogue)?;
        let attempt = RankedAttempt::from(catalogue);
        let encoded = Bytes::from(Record::RankedCatalog(catalogue).encode()?);
        let (initial, _) = self.root().await?;
        let mut checked = 0;
        if !initial.closed {
            if let Some(prior) = self.legacy_claim(attempt).await? {
                if prior != catalogue {
                    return Err(conflicting());
                }
            } else {
                let claim = self.claim_path(attempt);
                if self.create(&claim, encoded.clone()).await? {
                    checked = initial.count;
                } else if self.read_catalogue(&claim).await? != catalogue {
                    return Err(conflicting());
                }
            }
        }

        for _ in 0..MAX_ATTEMPTS {
            let (ledger, version) = self.root().await?;
            for sequence in checked..ledger.count {
                let admitted = self.entry(sequence + 1).await?;
                if RankedAttempt::from(admitted) == attempt {
                    return if admitted == catalogue {
                        Ok(sequence + 1)
                    } else {
                        Err(conflicting())
                    };
                }
            }
            checked = ledger.count;
            if ledger.closed {
                return Err(closed());
            }
            let sequence = ledger.count.checked_add(1).ok_or_else(invalid)?;
            let path = self.entry_path(sequence);
            self.create(&path, encoded.clone()).await?;
            let selected = self.read_catalogue(&path).await?;
            let next = Ledger {
                count: sequence,
                inventory_sha256: inventory_step(ledger.inventory_sha256, selected)?,
                ..ledger
            };
            if self.replace_root(&next, version).await? && selected == catalogue {
                return Ok(sequence);
            }
        }
        Err(contention())
    }

    async fn prepared_catalogue(&self, attempt: RankedAttempt) -> Result<Option<RankedCatalog>> {
        let (ledger, _) = self.root().await?;
        if ledger.closed {
            return Err(closed());
        }
        match self.optional_catalogue(&self.claim_path(attempt)).await? {
            Some(catalogue) if RankedAttempt::from(catalogue) == attempt => Ok(Some(catalogue)),
            Some(_) => Err(invalid()),
            None => self.legacy_claim(attempt).await,
        }
    }

    fn claim_path(&self, attempt: RankedAttempt) -> Path {
        self.path.clone().join("claims").join(format!("{:016x}", attempt.epoch))
            .join(cut_key(attempt))
    }

    async fn legacy_claim(&self, attempt: RankedAttempt) -> Result<Option<RankedCatalog>> {
        let path = self.path.clone().join("claims").join(cut_key(attempt));
        Ok(self.optional_catalogue(&path).await?.filter(|catalogue| RankedAttempt::from(*catalogue) == attempt))
    }

    async fn optional_catalogue(&self, path: &Path) -> Result<Option<RankedCatalog>> {
        match self.read_catalogue(path).await {
            Ok(catalogue) => Ok(Some(catalogue)),
            Err(error) if error.category == AnomalyCategory::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn close_admission(&self) -> Result<ClosedRankedAdmission> {
        for _ in 0..MAX_ATTEMPTS {
            let (mut ledger, version) = self.root().await?;
            if !ledger.closed {
                ledger.closed = true;
                if !self.replace_root(&ledger, version).await? {
                    continue;
                }
            }
            if let Some(upload) = &ledger.staging {
                self.drain_staging(upload).await?;
                continue;
            }
            self.verify_inventory(&ledger).await?;
            self.verify_staging(ledger.completed_staging).await?;
            return Ok(self.receipt(&ledger));
        }
        Err(contention())
    }

    async fn verify_inventory(&self, ledger: &Ledger) -> Result<()> {
        let mut catalogues = stream::iter(1..=ledger.count)
            .map(|sequence| self.entry(sequence))
            .buffered(INVENTORY_READ_CONCURRENCY);
        let mut digest = [0; 32];
        while let Some(catalogue) = catalogues.next().await {
            digest = inventory_step(digest, catalogue?)?;
        }
        if digest != ledger.inventory_sha256 {
            return Err(Anomaly::fault(
                "rtdb.storage/ranked-admission-inventory",
                "The original ranked catalogue inventory does not match the closed ledger",
            ));
        }
        Ok(())
    }

    fn receipt(&self, ledger: &Ledger) -> ClosedRankedAdmission {
        ClosedRankedAdmission {
            scope: self.scope,
            admitted: ledger.count,
            inventory_sha256: ledger.inventory_sha256,
            staging: ledger.completed_staging,
        }
    }

    async fn root(&self) -> Result<(Ledger, UpdateVersion)> {
        let (bytes, version) = self.read(&self.path.clone().join("root"), MAX_ROOT_BYTES).await?;
        let ledger: Ledger = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if version.e_tag.is_none() || (ledger.count == 0) != (ledger.inventory_sha256 == [0; 32])
            || !ledger.completed_staging.valid()
        {
            return Err(invalid());
        }
        if let Some(upload) = &ledger.staging {
            upload.object(self.scope)?;
        }
        Ok((ledger, version))
    }

    async fn replace_root(&self, ledger: &Ledger, version: UpdateVersion) -> Result<bool> {
        self.put(
            &self.path.clone().join("root"),
            encode_root(ledger)?,
            PutMode::Update(version),
        )
        .await
    }

    fn entry_path(&self, sequence: u64) -> Path {
        self.path
            .clone()
            .join("entries")
            .join(format!("{sequence:016x}"))
    }

    async fn entry(&self, sequence: u64) -> Result<RankedCatalog> {
        self.read_catalogue(&self.entry_path(sequence)).await
    }

    async fn read_catalogue(&self, path: &Path) -> Result<RankedCatalog> {
        let (bytes, _) = self.read(path, MAX_RECORD_BYTES).await?;
        let Record::RankedCatalog(catalogue) = Record::decode(&bytes)? else {
            return Err(invalid());
        };
        self.check_catalogue(catalogue)?;
        Ok(catalogue)
    }

    fn check_catalogue(&self, catalogue: RankedCatalog) -> Result<()> {
        if catalogue.manifest.database != self.scope.incarnation
            || catalogue.manifest.kind != ObjectKind::Manifest
            || catalogue.position == 0
            || catalogue.key_generation == 0
        {
            return Err(invalid());
        }
        Ok(())
    }

    async fn read(&self, path: &Path, max_bytes: u64) -> Result<(Bytes, UpdateVersion)> {
        let reply = self
            .objects.client
            .get(path)
            .await
            .map_err(|error| provider_error(&self.key, error))?;
        if reply.meta.size > max_bytes {
            return Err(invalid());
        }
        let version = UpdateVersion {
            e_tag: reply.meta.e_tag.clone(),
            version: reply.meta.version.clone(),
        };
        let bytes = reply
            .bytes()
            .await
            .map_err(|error| provider_error(&self.key, error))?;
        Ok((bytes, version))
    }

    async fn create(&self, path: &Path, bytes: Bytes) -> Result<bool> {
        self.put(path, bytes, PutMode::Create).await
    }

    async fn put(&self, path: &Path, bytes: Bytes, mode: PutMode) -> Result<bool> {
        let options = PutOptions {
            mode,
            ..Default::default()
        };
        match self.objects.client.put_opts(path, bytes.into(), options).await {
            Ok(_) => Ok(true),
            Err(ProviderError::AlreadyExists { .. } | ProviderError::Precondition { .. }) => {
                Ok(false)
            }
            Err(error) => Err(provider_error(&self.key, error).into()),
        }
    }
}

fn encode_root(ledger: &Ledger) -> Result<Bytes> {
    serde_json::to_vec(ledger)
        .map(Bytes::from)
        .map_err(|_| invalid())
}

#[async_trait::async_trait]
impl RankedAdmissionWriter for RankedAdmission {
    fn scope(&self) -> RankedAdmissionScope {
        self.scope
    }

    async fn prepared(&self, attempt: RankedAttempt) -> Result<Option<RankedCatalog>> {
        let owner = self.clone();
        self.region
            .run_async(async move { Ok(owner.prepared_catalogue(attempt).await) })
            .await?
    }

    async fn admit(&self, catalogue: RankedCatalog) -> Result<()> {
        self.publish(catalogue).await.map(drop)
    }

    async fn stage(&self, attempt: RankedAttempt, object: ObjectId, ciphertext: Bytes) -> Result<PhysicalObjectVersion> {
        self.stage_object(attempt, object, ciphertext).await
    }
}

fn cut_key(attempt: RankedAttempt) -> String {
    let RankedAttempt { position, system_time, key_generation, .. } = attempt;
    format!("{position:016x}-{system_time:016x}-{key_generation:016x}")
}

fn inventory_step(previous: [u8; 32], catalogue: RankedCatalog) -> Result<[u8; 32]> {
    let mut hash = Context::new(&SHA256);
    hash.update(b"rtdb.ranked-admissions/v1\0");
    hash.update(&previous);
    hash.update(&Record::RankedCatalog(catalogue).encode()?);
    Ok(hash.finish().as_ref().try_into().expect("SHA-256 width"))
}

fn invalid() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.storage/ranked-admission",
        "The ranked publication or closed scope is invalid",
    )
}

fn conflicting() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb.storage/ranked-admission",
        "This ranked cut has a different exact publication",
    )
}

fn closed() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb.storage/ranked-admission-closed",
        "The original ranked publication scope is closed",
    )
}

fn contention() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Busy,
        "rtdb.storage/ranked-admission",
        "The original ranked publication scope kept changing",
    )
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use rtdb_format::journal::ObjectId;

    use super::*;

    fn open(provider: Arc<InMemory>, scope: RankedAdmissionScope) -> RankedAdmission {
        RemoteStore::new(provider, Some(ObjectKey::new("serving").unwrap()))
            .ranked_admission(scope)
            .unwrap()
    }

    fn scope() -> RankedAdmissionScope {
        RankedAdmissionScope {
            incarnation: Uuid::new_v4(),
            activation_sha256: [3; 32],
        }
    }

    fn catalogue(scope: RankedAdmissionScope, position: u64) -> RankedCatalog {
        RankedCatalog {
            manifest: ObjectId {
                database: scope.incarnation,
                kind: ObjectKind::Manifest,
                epoch: 1,
                seq: position,
                digest: [7; 32],
            },
            position,
            system_time: -1,
            key_generation: 1,
        }
    }

    #[tokio::test]
    async fn ranked_admission_recovers_the_closed_prefix_without_operation_ids() {
        let provider = Arc::new(InMemory::new());
        let scope = scope();
        let writer = open(provider.clone(), scope);
        assert!(writer.close().await.is_err());
        assert!(writer.publish(catalogue(scope, 1)).await.is_err());
        writer.commission().await.unwrap();
        for position in 1..=101 {
            assert_eq!(
                writer.publish(catalogue(scope, position)).await.unwrap(),
                position
            );
        }

        let custodian = open(provider.clone(), scope);
        let closed = custodian.close().await.unwrap();
        assert_eq!(closed.admitted, 101);
        assert_eq!(custodian.close().await.unwrap(), closed);
        assert!(custodian.commission().await.is_err());
        assert!(writer.publish(catalogue(scope, 102)).await.is_err());
        assert_eq!(writer.publish(catalogue(scope, 20)).await.unwrap(), 20);
        let mut substituted = catalogue(scope, 20);
        substituted.manifest.digest = [8; 32];
        assert!(writer.publish(substituted).await.is_err());

        let first = custodian.outcomes(closed, 0).await.unwrap();
        assert_eq!(
            first.catalogues,
            (1..=100).map(|at| catalogue(scope, at)).collect::<Vec<_>>()
        );
        let last = custodian
            .outcomes(closed, first.next_after.unwrap())
            .await
            .unwrap();
        assert_eq!(last.catalogues, [catalogue(scope, 101)]);
        assert_eq!(last.next_after, None);
        let wrong = open(
            provider,
            RankedAdmissionScope {
                activation_sha256: [8; 32],
                ..scope
            },
        );
        wrong.commission().await.unwrap();
        wrong.close().await.unwrap();
        assert!(wrong.outcomes(closed, 0).await.is_err());
    }

    #[tokio::test]
    async fn ranked_admission_retries_an_exact_claim_and_admits_a_replacement_epoch() {
        let provider = Arc::new(InMemory::new());
        let scope = scope();
        let writer = open(provider.clone(), scope);
        writer.commission().await.unwrap();
        let first = catalogue(scope, 1);
        let attempt = RankedAttempt::from(first);
        let legacy = writer.path.clone().join("claims").join(cut_key(attempt));
        let encoded = Bytes::from(Record::RankedCatalog(first).encode().unwrap());
        assert!(writer.create(&legacy, encoded).await.unwrap());
        assert_eq!(writer.prepared(attempt).await.unwrap(), Some(first));
        assert_eq!(writer.publish(first).await.unwrap(), 1);
        let mut replacement = first;
        replacement.manifest.epoch += 1;
        replacement.manifest.digest = [9; 32];
        let next = RankedAttempt::from(replacement);
        assert!(writer.prepared(next).await.unwrap().is_none());
        assert_eq!(writer.publish(replacement).await.unwrap(), 2);

        let replacement_writer = open(provider, scope);
        assert_eq!(replacement_writer.prepared(next).await.unwrap(), Some(replacement));
        assert_eq!(replacement_writer.publish(replacement).await.unwrap(), 2);
        let closed = replacement_writer.close().await.unwrap();
        assert_eq!(replacement_writer.outcomes(closed, 0).await.unwrap().catalogues, [first, replacement]);
    }

    #[tokio::test]
    async fn ranked_admission_close_excludes_an_uncommitted_provider_slot() {
        let provider = Arc::new(InMemory::new());
        let scope = scope();
        let writer = open(provider.clone(), scope);
        writer.commission().await.unwrap();
        writer.publish(catalogue(scope, 1)).await.unwrap();

        let interrupted = catalogue(scope, 2);
        let bytes = Bytes::from(Record::RankedCatalog(interrupted).encode().unwrap());
        let (_, earlier_version) = writer.root().await.unwrap();
        assert!(writer.create(&writer.entry_path(2), bytes).await.unwrap());

        let custodian = open(provider, scope);
        let closed = custodian.close().await.unwrap();
        let proposed = Ledger {
            count: 2,
            inventory_sha256: [9; 32],
            ..Ledger::default()
        };
        assert!(
            !writer
                .replace_root(&proposed, earlier_version)
                .await
                .unwrap()
        );
        assert!(writer.publish(interrupted).await.is_err());
        assert_eq!(
            custodian.outcomes(closed, 0).await.unwrap().catalogues,
            [catalogue(scope, 1)]
        );
    }

    #[tokio::test]
    async fn ranked_admission_retries_a_single_provider_decision_under_concurrent_writers() {
        let provider = Arc::new(InMemory::new());
        let scope = scope();
        let first = open(provider.clone(), scope);
        let retry = open(provider.clone(), scope);
        let contender = open(provider, scope);
        first.commission().await.unwrap();
        let order = catalogue(scope, 1);
        let other = catalogue(scope, 2);
        let (first, second, third) = tokio::join!(
            first.publish(order),
            retry.publish(order),
            contender.publish(other)
        );
        assert_eq!(first.unwrap(), second.unwrap());
        let closed = contender.close().await.unwrap();
        let observed = contender.outcomes(closed, 0).await.unwrap().catalogues;
        assert_eq!(observed.len(), 2);
        assert_eq!(observed[(third.unwrap() - 1) as usize], other);
    }
}
