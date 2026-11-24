//! Closing a conflicting cell-transfer copy and selecting a fresh destination
//! generation. The source record selects each attempt; the destination
//! record retains every closed generation until the provider shows it empty.

use rtdb_directory::{
    AbandonedCellGeneration, CellTransferAttempt, DeploymentRecord, Directory, GenerationDisposition, Lifecycle,
    ServingInventory, now_ms,
};
use rtdb_types::{Anomaly, AnomalyCategory, Result};

use super::{CellHandle, update_installation};
use crate::cell_attempt::{may_close, may_dispose, may_select_next};
use crate::deployment::{CellStorage, s3_write_lease_horizon, storage_namespaces};
use crate::keys::KeyService;

/// A copy into an existing generation found different immutable bytes. Only
/// a fresh generation can make progress, because the conflicting bytes stay.
pub(super) fn is_conflicting_copy(error: &Anomaly, staged: &DeploymentRecord) -> bool {
    let prefix = format!("{}/", staged.storage_prefix());
    error.code == "rtdb.storage/immutable-object-conflict"
        && error.context.get("key").and_then(|key| key.as_str()).is_some_and(|key| key.starts_with(&prefix))
}

fn attempt_of(record: &DeploymentRecord) -> Option<&CellTransferAttempt> {
    record.cell_transfer.as_ref().and_then(|transfer| transfer.attempt.as_ref())
}

fn ordinal(record: &DeploymentRecord) -> Option<u32> {
    attempt_of(record).map(|attempt| attempt.ordinal)
}

fn attempt_changed(name: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/cell-transfer-attempt", format!("The transfer attempt of {name} changed before it could be recorded"))
}

/// Closes admission to the staged generation. Preparation removes
/// `move_from`, so a prepared attempt can no longer be closed.
async fn close(to: &CellHandle<'_>, source: &DeploymentRecord, staged: &DeploymentRecord) -> Result<DeploymentRecord> {
    let changed = || attempt_changed(&staged.name);
    let generation = staged.storage_generation.ok_or_else(changed)?;
    let namespaces = staged.storage_namespaces.ok_or_else(changed)?;
    let prefix = staged.storage_prefix();
    update_installation(to.directory.as_ref(), &staged.id, |record| {
        if record.abandoned_generations.contains_key(&generation) && record.storage_generation == Some(generation) {
            return Ok(());
        }
        let current = record.cell_transfer == source.cell_transfer && record.storage_generation == Some(generation)
            && record.incarnation == staged.incarnation && record.lifecycle == Lifecycle::Moving;
        if !may_close(record.move_from.is_some(), current) {
            return Err(changed());
        }
        record.abandoned_generations.insert(generation, AbandonedCellGeneration {
            ordinal: ordinal(record).unwrap_or(0),
            namespaces,
            prefix: prefix.clone(),
            closed_at_ms: now_ms(),
            first_empty_at_ms: None,
            disposition: None,
        });
        Ok(())
    }).await
}

fn successor(source: &DeploymentRecord, closed: &DeploymentRecord) -> Result<CellTransferAttempt> {
    let changed = || attempt_changed(&closed.name);
    let journals = closed.move_from.as_ref().ok_or_else(changed)?;
    let sealed_journals = journals.iter()
        .map(|endpoint| endpoint.journal().map(str::to_owned).ok_or_else(changed))
        .collect::<Result<Vec<_>>>()?;
    let prior = ordinal(closed);
    Ok(CellTransferAttempt {
        ordinal: prior.map_or(Some(1), |prior| prior.checked_add(1)).ok_or_else(changed)?,
        generation: uuid::Uuid::new_v4(),
        namespaces: closed.storage_namespaces.ok_or_else(changed)?,
        checkpoint: source.checkpoint,
        sealed_journals,
    })
}

/// The source selects the successor of exactly the attempt the destination closed.
async fn select_next(from: &CellHandle<'_>, source: &DeploymentRecord, closed: &DeploymentRecord) -> Result<DeploymentRecord> {
    let changed = || attempt_changed(&closed.name);
    let closed_attempt = closed.cell_transfer.as_ref().ok_or_else(changed)?;
    let candidate = successor(source, closed)?;
    update_installation(from.directory.as_ref(), &source.id, |record| {
        let transfer = record.cell_transfer.as_deref_mut().ok_or_else(changed)?;
        if record.lifecycle != Lifecycle::Moving || transfer.intent != closed_attempt.intent
            || transfer.epoch != closed_attempt.epoch || transfer.destination_incarnation != closed.incarnation
        {
            return Err(changed());
        }
        if transfer.attempt.as_ref().map(|attempt| attempt.ordinal) == Some(candidate.ordinal) {
            return Ok(());
        }
        let fresh = closed.storage_generation != Some(candidate.generation)
            && !closed.abandoned_generations.contains_key(&candidate.generation);
        let drained = closed.storage_regions.is_empty();
        if transfer.attempt != closed_attempt.attempt
            || !may_select_next(true, drained, closed.move_from.is_none(), fresh, ordinal(closed), candidate.ordinal)
        {
            return Err(changed());
        }
        transfer.attempt = Some(candidate.clone());
        Ok(())
    }).await
}

/// Points the destination at the generation the source selected.
pub(super) async fn install_selected(to: &CellHandle<'_>, closed: &DeploymentRecord, source: &DeploymentRecord) -> Result<DeploymentRecord> {
    let changed = || attempt_changed(&closed.name);
    let selected = attempt_of(source).ok_or_else(changed)?.clone();
    update_installation(to.directory.as_ref(), &closed.id, |record| {
        if record.cell_transfer == source.cell_transfer && record.storage_generation == Some(selected.generation) {
            return Ok(());
        }
        let prior = record.storage_generation.ok_or_else(changed)?;
        let successor = match ordinal(record) {
            Some(prior) => prior.checked_add(1) == Some(selected.ordinal),
            None => selected.ordinal == 1,
        };
        if record.lifecycle != Lifecycle::Moving || record.incarnation != closed.incarnation
            || record.move_from.is_none() || !record.storage_regions.is_empty()
            || !record.abandoned_generations.contains_key(&prior)
            || record.abandoned_generations.contains_key(&selected.generation)
            || record.storage_namespaces != Some(selected.namespaces) || !successor
        {
            return Err(changed());
        }
        record.cell_transfer.clone_from(&source.cell_transfer);
        record.storage_generation = Some(selected.generation);
        record.catalog_readers.clear();
        Ok(())
    }).await
}

/// Closes the staged generation, waits until its admitted writes and uploads
/// are accounted for, and moves both records to a fresh generation.
pub(super) async fn replace(from: &CellHandle<'_>, to: &CellHandle<'_>, source: &DeploymentRecord, staged: &DeploymentRecord) -> Result<(DeploymentRecord, DeploymentRecord)> {
    let closed = close(to, source, staged).await?;
    super::await_storage_drain(to.directory.as_ref(), to.storage, to.keys, &closed).await?;
    let closed = to.directory.deployment(&closed.id).await?
        .filter(|record| record.version >= closed.version && record.cell_transfer == closed.cell_transfer)
        .ok_or_else(|| attempt_changed(&closed.name))?;
    let source = select_next(from, source, &closed).await?;
    let staged = install_selected(to, &closed, &source).await?;
    Ok((source, staged))
}

/// Empties each closed generation that neither record can still select.
/// Erasure stays exact to the recorded prefix and provider namespace.
pub async fn dispose_abandoned(handle: &CellHandle<'_>, id: &str) -> Result<Option<DeploymentRecord>> {
    dispose_generations(handle.directory.as_ref(), handle.storage, handle.keys, id).await
}

pub(crate) async fn dispose_generations(directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, id: &str) -> Result<Option<DeploymentRecord>> {
    let Some(mut record) = directory.deployment(id).await? else { return Ok(None); };
    let pending: Vec<_> = record.abandoned_generations.iter()
        .filter(|(_, entry)| entry.disposition.is_none())
        .map(|(generation, entry)| (*generation, entry.clone()))
        .collect();
    if pending.is_empty() { return Ok(Some(record)); }
    let bucket = storage.bucket(rtdb_storage::IoRegion::new()).await?;
    let namespaces = storage_namespaces(bucket.as_ref(), keys).await?;
    let others: Vec<_> = directory.deployments().await?.into_iter().filter(|other| other.id != id).collect();
    for (generation, entry) in pending {
        let current = record.storage_generation.map_or(0, |current| current.as_u128());
        let selected = attempt_of(&record).map(|attempt| attempt.generation.as_u128());
        // A generation closed just before its successor was installed is
        // still named by the record; disposal of the residence erases it.
        if !may_dispose(true, generation.as_u128(), current, selected) { continue; }
        if entry.namespaces != namespaces { return Err(attempt_changed(&record.name)); }
        let prefix = crate::reference::key(&entry.prefix)?;
        crate::reference::require_disjoint_prefix(&prefix, others.iter())?;
        let observed_at_ms = now_ms();
        let inventory = match storage {
            CellStorage::Local { .. } => {
                for object in bucket.list(Some(&prefix)).await? { bucket.delete(&object.key).await?; }
                ServingInventory::LocalDirectory
            }
            CellStorage::S3 { config } => {
                let inventory = CellStorage::s3_inventory(config).await?;
                inventory.purge(&prefix).await?;
                if !inventory.is_empty(&prefix).await? { continue; }
                ServingInventory::S3VersionsAndUploads
            }
        };
        if !bucket.list(Some(&prefix)).await?.is_empty() { continue; }
        // A remote write admitted before the generation closed can still land
        // until the scoped write horizon from the first empty observation.
        let settled = match (storage, entry.first_empty_at_ms) {
            (CellStorage::Local { .. }, _) => true,
            (CellStorage::S3 { .. }, Some(first)) => observed_at_ms >= s3_write_lease_horizon(first),
            (CellStorage::S3 { .. }, None) => false,
        };
        if settled {
            super::withdraw_unreferenced_publication_origin(keys, &record, &entry.prefix).await?;
        }
        record = update_installation(directory, id, |current| {
            let stored = current.abandoned_generations.get_mut(&generation)
                .filter(|stored| stored.prefix == entry.prefix && stored.namespaces == entry.namespaces)
                .ok_or_else(|| attempt_changed(&current.name))?;
            if stored.disposition.is_none() {
                if settled {
                    stored.disposition = Some(GenerationDisposition { inventory, observed_empty_at_ms: observed_at_ms });
                } else if stored.first_empty_at_ms.is_none() {
                    stored.first_empty_at_ms = Some(observed_at_ms);
                }
            }
            Ok(())
        }).await?;
    }
    Ok(Some(record))
}

pub(super) fn pending_disposal(name: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/abandoned-generation", format!("A closed transfer generation of {name} still has provider objects"))
}

pub(crate) fn has_pending_disposal(record: &DeploymentRecord) -> bool {
    record.abandoned_generations.values().any(|entry| entry.disposition.is_none())
}

pub(super) async fn require_disposed(handle: &CellHandle<'_>, record: &DeploymentRecord) -> Result<DeploymentRecord> {
    if !has_pending_disposal(record) { return Ok(record.clone()); }
    let disposed = dispose_abandoned(handle, &record.id).await?.ok_or_else(|| attempt_changed(&record.name))?;
    if has_pending_disposal(&disposed) { return Err(pending_disposal(&disposed.name)); }
    Ok(disposed)
}
