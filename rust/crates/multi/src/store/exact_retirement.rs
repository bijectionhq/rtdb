use async_trait::async_trait;
use rtdb_directory::{DeploymentRecord, Directory, Lifecycle, LineageChange, LineageRetirement, LineageState, RetirementWork, StorageRegion};
use rtdb_storage::{Error, ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, AnomalyCategory, PhysicalObjectVersion, Result};
use uuid::Uuid;

use super::{GARBAGE_DIRECTORY, StoredRelease, can_reap, permits_release};
use crate::reference::{Base, References, stores};

#[async_trait]
pub(super) trait ExactVersions: Send + Sync {
    async fn available(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<bool>;
    async fn erase(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<()>;
}

#[async_trait]
impl ExactVersions for rtdb_s3::VersionedInventory {
    async fn available(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<bool> {
        self.exact_retirement_available(key, version).await
    }

    async fn erase(&self, key: &ObjectKey, version: &PhysicalObjectVersion) -> Result<()> {
        self.retire_observed_version(key, version).await
    }
}

pub(super) async fn reap(directory: &dyn Directory, bucket: &dyn ObjectStore, provider: &dyn ExactVersions,
    candidate: &DeploymentRecord, threshold: i64, advisory: Option<&References>) -> Result<()> {
    let lineage = &candidate.lineage;
    let mut state = super::recover_hosted_lineage(directory, lineage).await?;
    if let Some(retirement) = state.retirement() {
        if let RetirementWork::S3 { deployment, .. } = &retirement.work && *deployment == candidate.id {
            finish(directory, bucket, provider, lineage, &state, retirement).await?;
        }
        return Ok(());
    }
    if !can_reap(candidate) { return Ok(()); }
    if state.preparations().next().is_some() || state.open_generation().is_none() { return Ok(()); }
    let source = candidate.storage_prefix();
    for store in stores(bucket, &source).await? {
        let markers = ObjectKey::new(format!("{store}/{GARBAGE_DIRECTORY}"))?;
        for marker in bucket.list(Some(&markers)).await? {
            let bytes = match bucket.get_fresh(&marker.key).await {
                Ok(bytes) => bytes,
                Err(Error::MissingObject(_)) => continue,
                Err(error) => return Err(error.into()),
            };
            let Ok(StoredRelease::Exact { record, version }) = serde_json::from_slice(&bytes) else { continue; };
            if record.trie_floor.is_none() || !permits_release(candidate, &store, &record) || record.released_at_ms > threshold { continue; }
            let target = ObjectKey::new(format!("{store}/{}", record.key))?;
            if advisory.is_some_and(|observed| observed.has_declared_reference(&target)) { continue; }
            if !provider.available(&target, &version).await? { continue; }
            let retirement = LineageRetirement {
                token: Uuid::new_v4(), source: source.clone(),
                work: RetirementWork::S3 { deployment: candidate.id.clone(), target: target.to_string(), release: marker.key.to_string(), version },
            };
            let reserved = directory.change_lineage(lineage, LineageChange::ReserveRetirement {
                retirement: retirement.clone(), observed_references: state.reference_revision(), generation: state.retirement_generation(),
            }).await?;
            if finish(directory, bucket, provider, lineage, &reserved, &retirement).await? { return Ok(()); }
            state = directory.lineage_state(lineage).await?;
            if state.preparations().next().is_some() || state.open_generation().is_none() { return Ok(()); }
        }
    }
    Ok(())
}

async fn finish(directory: &dyn Directory, bucket: &dyn ObjectStore, provider: &dyn ExactVersions,
    lineage: &str, state: &LineageState, retirement: &LineageRetirement) -> Result<bool> {
    let RetirementWork::S3 { deployment, target, release, version } = &retirement.work else { return Err(changed()); };
    let target = ObjectKey::new(target)?;
    let release = ObjectKey::new(release)?;
    if !state.exact_erasure_authorized() {
        let ready = prepare(directory, bucket, lineage, retirement, deployment, &target, &release, version).await;
        match ready {
            Ok(true) => {
                if let Err(error) = directory.change_lineage(lineage, LineageChange::AuthorizeExactErasure { retirement: retirement.clone() }).await {
                    if directory.lineage_state(lineage).await?.retirement() != Some(retirement) {
                        super::release_region(directory, deployment, retirement.token, false).await?;
                    }
                    return Err(error);
                }
            }
            outcome => {
                directory.change_lineage(lineage, LineageChange::AbandonExactRetirement { retirement: retirement.clone() }).await?;
                super::release_region(directory, deployment, retirement.token, false).await?;
                return outcome;
            }
        }
    }
    provider.erase(&target, version).await?;
    bucket.delete(&release).await?;
    super::release_region(directory, deployment, retirement.token, false).await?;
    directory.change_lineage(lineage, LineageChange::SettleRetirement { retirement: retirement.clone() }).await?;
    Ok(true)
}

async fn prepare(directory: &dyn Directory, bucket: &dyn ObjectStore, lineage: &str, retirement: &LineageRetirement,
    deployment: &str, target: &ObjectKey, release: &ObjectKey, version: &PhysicalObjectVersion) -> Result<bool> {
    let root = ObjectKey::new(&retirement.source)?;
    let Some((store, relative)) = target.as_str().strip_prefix(&format!("{root}/")).and_then(|suffix| suffix.split_once('/'))
        else { return Err(changed()); };
    let store = format!("{root}/{store}");
    let markers = ObjectKey::new(format!("{store}/{GARBAGE_DIRECTORY}"))?;
    if !release.is_under(&markers) { return Err(changed()); }
    let bytes = match bucket.get_fresh(release).await {
        Ok(bytes) => bytes,
        Err(Error::MissingObject(_)) => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let Ok(StoredRelease::Exact { record: release, version: observed }) = serde_json::from_slice(&bytes) else { return Err(changed()); };
    if release.key != relative || release.trie_floor.is_none() || *version != observed { return Err(changed()); }

    let deployments = directory.deployments().await?;
    for record in deployments.iter().filter(|record| record.lineage == lineage && record.lifecycle != Lifecycle::Deleted) {
        for referrer in stores(bucket, &record.storage_prefix()).await? {
            if Base::load(bucket, &referrer).await?.objects.get(relative) == Some(&store) { return Ok(false); }
        }
    }

    crate::admin::update_installation(directory, deployment, |record| {
        if record.lineage != lineage || record.storage_prefix() != retirement.source || !can_reap(record)
            || !permits_release(record, &store, &release)
        {
            return Err(changed());
        }
        if record.storage_regions.values().any(|region| match region {
            StorageRegion::ReferenceRead { object, .. } => object.as_deref().is_none_or(|key| key == target.as_str()),
            StorageRegion::Remote { archive: Some(_), .. } | StorageRegion::Local { archive: Some(_), .. } => true,
            _ => false,
        }) { return Err(changed()); }
        let fence = StorageRegion::ExactRetirement { object: target.to_string() };
        if let Some(previous) = record.storage_regions.insert(retirement.token, fence.clone()) && previous != fence {
            return Err(changed());
        }
        Ok(())
    }).await?;

    let base = Base::load(bucket, &store).await?;
    base.forget(bucket, &store, &[relative.to_owned()].into()).await?;
    Ok(true)
}

fn changed() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/exact-retirement", "The source residence has an incompatible storage admission")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use bytes::Bytes;
    use rtdb_directory::{CatalogAdmissionFloor, ForkPreparation, MemoryDirectory, ReplayCustody, StorageNamespaces};
    use rtdb_format::keys::{CURRENT_STORAGE_VERSION, StorageRoot};
    use rtdb_storage::MemoryStore;
    use rtdb_types::{CatalogCut, ReplicaPosition, S3ObjectNamespace, StorageEpoch};

    use super::*;

    struct Provider {
        version: PhysicalObjectVersion,
        attempts: AtomicUsize,
    }

    #[async_trait]
    impl ExactVersions for Provider {
        async fn available(&self, _: &ObjectKey, version: &PhysicalObjectVersion) -> Result<bool> {
            assert_eq!(version, &self.version);
            Ok(true)
        }

        async fn erase(&self, _: &ObjectKey, version: &PhysicalObjectVersion) -> Result<()> {
            assert_eq!(version, &self.version);
            if self.attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                return Err(Anomaly::new(AnomalyCategory::Unavailable, "fixture/provider-reply-lost", "Provider response was lost"));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn exact_retirement_waits_for_forks_and_reads_and_resumes_the_same_version() {
        let directory = MemoryDirectory::new();
        let bucket: crate::reference::Bucket = std::sync::Arc::new(MemoryStore::default());
        let mut parent = DeploymentRecord::new("shop", "acme", vec![], 0);
        let namespace = Uuid::new_v4();
        parent.storage_namespaces = Some(StorageNamespaces { objects: namespace, keys: [0; 32] });
        let epoch = StorageEpoch(0);
        let root = StorageRoot::new(CURRENT_STORAGE_VERSION, epoch).unwrap().key();
        let floor = CatalogAdmissionFloor { residence: parent.storage_prefix(), object_namespace: namespace, cut: CatalogCut {
            storage_version: CURRENT_STORAGE_VERSION, storage_epoch: epoch, replica_epoch: epoch, through: ReplicaPosition(2),
        } };
        parent.catalog_admission_floors.insert(root.clone(), floor.clone());
        let parent = directory.create_deployment(parent).await.unwrap();
        let store = format!("{}/{root}", parent.storage_prefix());
        let relative = ObjectKey::new("tables/orders/data/l01-rc-b00.arrow").unwrap();
        let target = ObjectKey::new(format!("{store}/{relative}")).unwrap();
        let versions = Provider { version: PhysicalObjectVersion {
            namespace: S3ObjectNamespace { bucket: "cell".into(), endpoint: None, region: None },
            key: format!("cell/{target}"), version: "retired".into(),
        }, attempts: AtomicUsize::new(0) };
        bucket.put(&target, Bytes::from_static(b"replacement")).await.unwrap();
        let physical = std::sync::Arc::new(crate::reference::PrefixedStore::new(bucket.clone(), &store));
        let garbage = super::super::GarbageStore::open_with_clock(physical, std::sync::Arc::new(|| 0)).await.unwrap();
        garbage.write_release(&relative, Some(floor.clone()), None).await.unwrap();
        garbage.write_release(&relative, Some(floor), Some(&versions.version)).await.unwrap();

        let preparation = ForkPreparation { token: Uuid::new_v4(), generation: 0 };
        directory.change_lineage(&parent.lineage, LineageChange::PrepareHostedFork { preparation, task: None }).await.unwrap();
        reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, 0, None).await.unwrap();
        assert!(directory.lineage_state(&parent.lineage).await.unwrap().retirement().is_none());

        let mut child = DeploymentRecord::new("preview", &parent.org, vec![], 0);
        child.lineage.clone_from(&parent.lineage);
        child.parent = Some(parent.id.clone());
        crate::reference::fork(bucket.as_ref(), &parent.storage_prefix(), &child.storage_prefix(), None).await.unwrap();
        let child = directory.publish_fork(child, parent.version, Some(rtdb_directory::ForkPublication::Hosted(preparation))).await.unwrap();
        reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, 0, None).await.unwrap();
        assert_eq!(versions.attempts.load(Ordering::Relaxed), 0);
        let child_store = format!("{}/{root}", child.storage_prefix());
        let base = Base::load(bucket.as_ref(), &child_store).await.unwrap();
        assert_eq!(base.objects.get(relative.as_str()), Some(&store));
        base.forget(bucket.as_ref(), &child_store, &[relative.to_string()].into()).await.unwrap();

        let reader = Uuid::new_v4();
        crate::admin::update_installation(directory.as_ref(), &parent.id, |record| {
            record.storage_regions.insert(reader, StorageRegion::ReferenceRead { custody: ReplayCustody::Local, object: Some(target.to_string()) });
            Ok(())
        }).await.unwrap();
        assert_eq!(reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, 0, None).await.unwrap_err().code, "rtdb.multi/exact-retirement");
        assert_eq!(versions.attempts.load(Ordering::Relaxed), 0);
        super::super::release_region(directory.as_ref(), &parent.id, reader, false).await.unwrap();

        assert_eq!(reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, 0, None).await.unwrap_err().code, "fixture/provider-reply-lost");
        let authorizing = directory.lineage_state(&parent.lineage).await.unwrap();
        assert!(authorizing.exact_erasure_authorized());
        let token = authorizing.retirement().unwrap().token;
        assert!(matches!(directory.deployment(&parent.id).await.unwrap().unwrap().storage_regions.get(&token), Some(StorageRegion::ExactRetirement { .. })));
        reap(directory.as_ref(), bucket.as_ref(), &versions, &parent, 0, None).await.unwrap();
        assert_eq!(versions.attempts.load(Ordering::Relaxed), 2);
        assert!(directory.lineage_state(&parent.lineage).await.unwrap().retirement().is_none());
        assert!(directory.deployment(&parent.id).await.unwrap().unwrap().storage_regions.is_empty());

        let records = directory.deployments().await.unwrap();
        for outcome in super::super::reaper_pass(bucket.as_ref(), &records, i64::MAX, Duration::ZERO).await.unwrap() {
            outcome.unwrap();
        }
        assert_eq!(bucket.get(&target).await.unwrap(), Bytes::from_static(b"replacement"));
    }
}
