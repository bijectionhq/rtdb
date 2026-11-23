use std::{collections::BTreeMap, ops::Range, path::Path, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use rtdb_columnar::{MemoryBudget, OwnedBatch};
use rtdb_directory::{Directory, HostedTask};
use rtdb_node::NodeOptions;
use rtdb_plan::Catalog;
use rtdb_query::{
    CatalogSnapshot, DatabaseSnapshot, QueryContext, open_owned_assertion_point, owned_row_revision,
};
use rtdb_replication::{BlockDiscovery, BlockStore, PublishedCheckpoint};
use rtdb_storage::{
    Error, ObjectKey, ObjectObservation, ObjectReadAllocation, ObjectStore, ReadReservation, RetainedBytes, StoredObject,
    cache::{BufferPool, CacheBudget},
};
use rtdb_types::{
    BlockIndex, EntityId, Interval, OwnedRelation, ReplicaPosition, Result, SourcePosition,
    OwnedLifetimeFloor, StorageEpoch, Timestamp, TransactionKey,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ArchiveKey, ArchivedObject, CellStorage, KeyService, PrefixedStore, archive_witness,
    capture_receipt, inconsistent, key, observe_archive_object, read, read_object, replica_suffix, require_capture_open, stage,
    read_archive_lifetime_floors, unavailable, verified_archive, verified_held_archive,
};

/// Reopenable locator for one independently retained published cut.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointReadWorld {
    pub deployment: Uuid,
    pub capture: Uuid,
    pub manifest_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suffix: Option<Uuid>,
    pub basis: TransactionKey,
}

pub struct CheckpointPoint<'a> {
    /// The authenticated owner supplied by the calling host.
    pub owner: &'a str,
    pub relation: &'a OwnedRelation,
    pub id: &'a EntityId,
    pub valid: Option<Interval>,
}

#[derive(Clone)]
pub struct CheckpointReadResources<'a> {
    pub node: &'a NodeOptions,
    pub memory: &'a Arc<MemoryBudget>,
    pub cache: Arc<CacheBudget>,
    pub cancellation: &'a CancellationToken,
}

/// Read an owned point at the capture's exact transaction. The archive
/// admission remains held until the cursor and its storage have drained.
pub async fn read_checkpoint_owned_point(
    directory: &dyn Directory,
    archive: &CellStorage,
    keys: &dyn KeyService,
    hosted_task: Option<&HostedTask>,
    world: &CheckpointReadWorld,
    point: CheckpointPoint<'_>,
    resources: CheckpointReadResources<'_>,
) -> Result<Option<Vec<OwnedBatch>>> {
    with_checkpoint_context(
        directory, archive, keys, hosted_task, world, resources,
        |context| read_checkpoint_point_in_context(context, point, None),
    ).await
}

pub(super) fn require_relation_purge_history(context: &QueryContext, owner: &str) -> Result<()> {
    let database = context.catalog.database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else {
        return Err(inconsistent("The archived world has no exact purge history"));
    };
    if snapshot.contains_table(&OwnedRelation::obsolete_purge_table(owner)) {
        return Err(rtdb_types::Anomaly::new(
            rtdb_types::AnomalyCategory::Unavailable, "rtdb.owned/purge-history-reset",
            "Owner-wide archive purge history requires a reset before native held reads",
        ));
    }
    Ok(())
}

/// The original placed live hold supplies the replay present even when the
/// archived system and business-valid coordinates are older.
#[derive(Clone, Copy)]
pub(super) struct ArchiveLifetimeRead {
    pub policy: OwnedLifetimeFloor,
    pub present: Timestamp,
}

pub(super) async fn read_checkpoint_point_in_context(
    context: Arc<QueryContext>, point: CheckpointPoint<'_>, lifetime: Option<ArchiveLifetimeRead>,
) -> Result<Option<Vec<OwnedBatch>>> {
    require_relation_purge_history(&context, point.owner)?;
    let database = context.catalog.database(context.catalog.default_database())?;
    let [snapshot] = database.partitions.as_slice() else {
        return Err(inconsistent("The archived world has no exact relation lifetime scope"));
    };
    let root = point.relation.root_relation();
    let governed = snapshot.contains_table(&root.as_ref().unwrap_or(point.relation).lifetime_table(point.owner));
    if governed != lifetime.is_some() {
        return Err(unavailable("The held lifetime does not match the archived relation"));
    }
    if let Some(live) = lifetime {
        let duration = live.policy.duration;
        live.policy.admit_recorded_present(live.present)?;
        if live.present < context.current_time.as_micros()? {
            return Err(unavailable("The live lifetime present precedes the archived basis"));
        }
        let archived = rtdb_query::read_owned_relation_schema(Arc::clone(&context), point.owner, point.relation)
            .await?.and_then(|(schema, _)| schema.lifetime_micros());
        if archived != Some(duration) {
            return Err(unavailable("The held lifetime differs from the archived declaration"));
        }
        let floor = rtdb_query::read_owned_lifetime_floor(Arc::clone(&context), point.owner, point.relation).await?;
        floor.check(duration, context.current_time.as_micros()?)?;
        floor.admit_recorded_present(live.present)?;
    }
    let memory = Arc::clone(&context.memory);
    let cursor = if lifetime.is_some() {
        let valid = point.valid.ok_or_else(|| inconsistent("The archived lifetime requires an exact valid instant"))?;
        rtdb_query::open_owned_lifetime_point(context, point.owner, point.relation, point.id, valid).await?
    } else {
        open_owned_assertion_point(context, point.owner, point.relation, point.id, point.valid, false).await?
    };
    let Some(mut cursor) = cursor else { return Ok(None); };
    let mut batches = Vec::new();
    while let Some(batch) = cursor.next_batch().await? {
        for row in 0..batch.batch().num_rows() {
            owned_row_revision(batch.batch(), row, point.id, &memory)?;
        }
        batches.push(batch);
    }
    Ok(Some(batches))
}

pub(super) async fn with_checkpoint_context<T, F, Fut>(
    directory: &dyn Directory,
    archive: &CellStorage,
    keys: &dyn KeyService,
    hosted_task: Option<&HostedTask>,
    world: &CheckpointReadWorld,
    resources: CheckpointReadResources<'_>,
    run: F,
) -> Result<T>
where
    F: FnOnce(Arc<QueryContext>) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    with_registered_capture_context(
        directory, archive, keys, hosted_task,
        CaptureSelection { deployment: world.deployment, capture: world.capture, manifest: &world.manifest_sha256, suffix: world.suffix, expected_basis: Some(world.basis), require_replay: false },
        resources, |context, _| run(context),
    ).await
}

pub(super) struct CaptureSelection<'a> {
    pub deployment: Uuid,
    pub capture: Uuid,
    pub manifest: &'a str,
    pub suffix: Option<Uuid>,
    pub expected_basis: Option<TransactionKey>,
    pub require_replay: bool,
}

pub(super) async fn with_registered_capture_context<T, F, Fut>(
    directory: &dyn Directory,
    archive: &CellStorage,
    keys: &dyn KeyService,
    hosted_task: Option<&HostedTask>,
    selected: CaptureSelection<'_>,
    resources: CheckpointReadResources<'_>,
    run: F,
) -> Result<T>
where
    F: FnOnce(Arc<QueryContext>, TransactionKey) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    with_registered_capture_replay(directory, archive, keys, hosted_task, selected, resources, None,
        |context, basis, _| run(context, basis),
    ).await
}

pub(super) struct TerminalReplay<'a> {
    pub storage: &'a CellStorage,
    pub source_region: Uuid,
    pub source_io: Arc<rtdb_storage::IoRegion>,
    pub fence: &'a rtdb_directory::CheckpointTransferFence,
    pub stream: &'a rtdb_logkeeper::StreamConfig,
    pub certificate: Option<&'a rtdb_logkeeper::JournalDigest>,
    pub through: i64,
}

pub(super) async fn with_registered_capture_replay<T, F, Fut>(
    directory: &dyn Directory,
    archive: &CellStorage,
    keys: &dyn KeyService,
    hosted_task: Option<&HostedTask>,
    selected: CaptureSelection<'_>,
    resources: CheckpointReadResources<'_>,
    terminal: Option<TerminalReplay<'_>>,
    run: F,
) -> Result<T>
where
    F: FnOnce(Arc<QueryContext>, TransactionKey, Option<replica_suffix::TerminalReplayFrontier>) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    if !resources.node.owned_relations {
        return Err(unavailable(
            "Owned checkpoint reads require the owned-relation capability",
        ));
    }
    let deployment = selected.deployment.to_string();
    let terminal = terminal.as_ref();
    let admit = |record: &rtdb_directory::DeploymentRecord| {
        if resources.cancellation.is_cancelled() {
            return Err(unavailable("The retained checkpoint read was cancelled"));
        }
        require_capture_open(record, selected.capture)?;
        if let Some(terminal) = terminal
            && (selected.suffix.is_some() || record.checkpoint_transfer_fence.as_deref() != Some(terminal.fence)
                || record.lifecycle != rtdb_directory::Lifecycle::Restoring
                || record.epoch != terminal.fence.fenced_writer_epoch)
        {
            return Err(inconsistent("The terminal replica replay has no current source fence"));
        }
        let (capture, manifest) =
            capture_receipt(record, archive, selected.deployment, selected.capture)?
                .ok_or_else(|| unavailable("The read world has no retained checkpoint receipt"))?;
        let suffix = selected.suffix.map(|suffix|
            replica_suffix::accepted(record, &capture, selected.manifest, suffix)
        ).transpose()?.cloned();
        if manifest != selected.manifest
            || selected.expected_basis.is_some_and(|basis| match &suffix {
                Some(pin) => pin.basis != basis,
                None => terminal.is_none() && capture.checkpoint.system_time != basis.system_time.0,
            })
            || capture.checkpoint.boundary < 0
            || capture.checkpoint.root_sha256.is_none()
        {
            return Err(inconsistent(
                "The read world does not name an exact published cut",
            ));
        }
        Ok((capture, suffix))
    };
    let plaintext = rtdb_storage::sealing::PlaintextView::new(Arc::clone(resources.memory));
    let read = stage::with_region(
        directory,
        keys,
        &deployment,
        archive,
        hosted_task,
        Some((selected.capture, selected.suffix)),
        None,
        |record| admit(record).map(|_| None),
        |record, _, region| async move {
            resources.cancellation.run_until_cancelled(Box::pin(async {
                let (capture, suffix) = admit(&record)?;
                let bucket = archive.bucket(Arc::clone(&region)).await?;
                if read(
                    bucket.as_ref(),
                    &archive_witness(selected.deployment, selected.capture)?,
                )
                .await?
                .as_ref()
                    != Some(&capture)
                {
                    return Err(inconsistent(
                        "The archive belongs to another captured checkpoint",
                    ));
                }
                let held = !selected.require_replay && terminal.is_none()
                    && suffix.as_ref().is_none_or(|pin| pin.custody == rtdb_directory::CheckpointSuffixCustody::World);
                let (manifest, cipher) = if held {
                    verified_held_archive(bucket.clone(), &capture, selected.manifest, keys, Arc::clone(&region)).await?
                } else {
                    verified_archive(bucket.clone(), &capture, selected.manifest, keys, Arc::clone(&region)).await?
                };
                if selected.require_replay {
                    if manifest.format != rtdb_format::catalog::CheckpointContents::Replay {
                        return Err(inconsistent("An installation serving from a replay archive requires a complete replay restore"));
                    }
                    manifest.require_serving_replay_frames()?;
                }
                let store: Arc<dyn ObjectStore> = Arc::new(CapturedObjects {
                    files: PrefixedStore::new(
                        Arc::clone(&bucket),
                        format!(
                            "checkpoint-captures/{}/{}/objects",
                            selected.deployment, selected.capture
                        ),
                    ),
                    objects: manifest
                        .objects
                        .into_iter()
                        .map(|object| Ok((key(&object.name)?, object)))
                        .collect::<Result<_>>()?,
                    cipher: Arc::clone(&cipher),
                });
                let memory = resources.node.query.query_memory(resources.memory);
                let storage = Arc::new(BufferPool::with_metadata_budget(
                    store.clone(),
                    resources.cache,
                    memory.clone(),
                ));
                let blocks = BlockStore::new(
                    store,
                    storage.clone(),
                    memory.clone(),
                    resources.node.file_size_target,
                )?;
                let epoch = StorageEpoch(capture.log_epoch);
                let checkpoint = PublishedCheckpoint {
                    block: BlockIndex(capture.checkpoint.block),
                    boundary: ReplicaPosition::from_epoch_offset(epoch, capture.checkpoint.boundary)?,
                    root_sha256: capture.checkpoint.root_sha256,
                };
                let source_floor = SourcePosition::from_epoch_offset(epoch, capture.checkpoint.source)?;
                let now = Timestamp(capture.checkpoint.system_time);
                let mut recovered = if held {
                    blocks.recover_held(resources.node.trie, source_floor, now, checkpoint).await?
                } else {
                    blocks.recover(resources.node.trie, source_floor, now, BlockDiscovery::AtPublished(checkpoint)).await?
                };
                let base_snapshot = recovered.partition.snapshot();
                let base_basis = base_snapshot.tx_basis()
                    .filter(|basis| basis.system_time.0 == capture.checkpoint.system_time)
                    .ok_or_else(|| inconsistent("The retained checkpoint has a different transaction key"))?;
                let name = &record.name;
                let query_context = |partitions, basis: TransactionKey| {
                    let catalog = CatalogSnapshot::new(
                        name,
                        BTreeMap::from([(name.clone(), DatabaseSnapshot {
                            partitions,
                            storage: Arc::clone(&storage),
                        })]),
                        true,
                    )?;
                    QueryContext::new(
                        catalog, Arc::clone(&memory), basis.system_time, "UTC",
                        resources.node.query.clone(), resources.cancellation,
                    )
                };
                let base_floors = if suffix.is_some() || terminal.is_some() {
                    let base_snapshot = base_snapshot.with_system_time_cap(Some(base_basis.system_time))?;
                    let context = query_context(vec![base_snapshot], base_basis)?;
                    Some(read_archive_lifetime_floors(context, base_basis).await?)
                } else {
                    drop(base_snapshot);
                    None
                };
                match (selected.suffix, suffix) {
                    (Some(suffix), Some(pin)) => {
                        replica_suffix::apply_archived(
                            bucket.as_ref(), selected.deployment, suffix, record.stream("replica"), &pin, Arc::clone(&cipher),
                            &mut recovered, &memory,
                        ).await?;
                    }
                    (None, None) => (),
                    _ => return Err(inconsistent("The selected archived suffix is unavailable")),
                }
                let terminal_source = if let Some(terminal) = terminal {
                    Some(replica_suffix::replay_certified_terminal(
                        &record, terminal.storage, terminal.source_region, Arc::clone(&terminal.source_io), keys, terminal.stream, terminal.certificate, terminal.fence,
                        terminal.through, &mut recovered, &memory,
                    ).await?)
                } else {
                    None
                };
                let snapshot = recovered.partition.snapshot();
                let basis = snapshot.tx_basis().ok_or_else(|| inconsistent("The retained checkpoint has no transaction basis"))?;
                if selected.expected_basis.is_some_and(|expected| basis != expected) {
                    return Err(inconsistent("The retained checkpoint has a different transaction key"));
                }
                let snapshot = snapshot.with_system_time_cap(Some(basis.system_time))?;
                let context = query_context(vec![snapshot], basis)?;
                if let Some(prior) = base_floors {
                    let replayed = read_archive_lifetime_floors(Arc::clone(&context), basis).await?;
                    for (table, floor) in prior {
                        let preserved = replayed.get(&table).is_some_and(|next|
                            next.duration == floor.duration
                            && next.expired_created_through >= floor.expired_created_through
                            && next.swept_present_through >= floor.swept_present_through
                        );
                        if !preserved {
                            return Err(inconsistent("The certified replica suffix regresses a checkpoint lifetime floor"));
                        }
                    }
                }
                run(context, basis, terminal_source).await
            })).await.ok_or_else(|| unavailable("The retained checkpoint read was cancelled"))?
        },
    );
    let answer = plaintext.reading(read).await?;
    plaintext.grant().await?;
    Ok(answer)
}

/// The accepted inventory is the only source of bytes for a world read.
pub(super) struct CapturedObjects {
    pub(super) files: PrefixedStore,
    pub(super) objects: BTreeMap<ObjectKey, ArchivedObject>,
    pub(super) cipher: Arc<dyn ArchiveKey>,
}

impl CapturedObjects {
    fn object(&self, name: &ObjectKey) -> rtdb_storage::Result<&ArchivedObject> {
        self.objects
            .get(name)
            .ok_or_else(|| Error::MissingObject(name.to_string()))
    }

    fn write_denied(name: &ObjectKey) -> Error {
        Error::PermissionDenied {
            key: name.to_string(),
            message: "Captured checkpoint is read-only".into(),
        }
    }
}

#[async_trait]
impl ObjectStore for CapturedObjects {
    fn supports_scan_ranges(&self, _: &ObjectKey) -> bool {
        false
    }

    async fn head(&self, name: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        let size = self.object(name)?.plaintext_bytes(name)?;
        Ok(StoredObject {
            key: name.clone(),
            size,
        })
    }

    async fn head_fresh(&self, name: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        Ok(self.observe_head(name).await?.value)
    }

    fn whole_object_allocation(&self, name: &ObjectKey, length: usize) -> rtdb_storage::Result<ObjectReadAllocation> {
        let object = self.object(name)?;
        if object.plaintext_bytes(name)? != length as u64 { return Err(Error::ConflictingObject(name.to_string())); }
        let scratch_bytes = usize::try_from(object.bytes).ok().and_then(|n| n.checked_mul(2))
            .ok_or_else(|| Error::Task("Checkpoint object allocation size overflow".into()))?;
        Ok(ObjectReadAllocation { retained_bytes: length, scratch_bytes })
    }

    async fn get_reserved_object(&self, name: &ObjectKey, reservation: ReadReservation) -> rtdb_storage::Result<RetainedBytes> {
        if !reservation.covers(&self.whole_object_allocation(name, reservation.length())?) {
            return Err(Error::Closed("Checkpoint object read has no scratch reservation".into()));
        }
        let plaintext = reservation.retaining_io(self.get(name)).await?;
        reservation.finish(plaintext)
    }

    async fn observe_head(&self, name: &ObjectKey) -> rtdb_storage::Result<ObjectObservation<StoredObject>> {
        let object = self.object(name)?;
        let mut observed = self.files.observe_head(name).await?;
        if observed.value.size != object.bytes {
            return Err(Error::ConflictingObject(name.to_string()));
        }
        observed.value.size = object.plaintext_bytes(name)?;
        Ok(observed)
    }

    async fn observe_fresh_exact(&self, name: &ObjectKey, length: usize) -> rtdb_storage::Result<ObjectObservation<Bytes>> {
        let object = self.object(name)?;
        if object.plaintext_bytes(name)? != length as u64 {
            return Err(Error::ConflictingObject(name.to_string()));
        }
        let observed = observe_archive_object(&self.files, name, object).await?;
        let value = object.open(self.cipher.as_ref(), name, observed.value).await?;
        Ok(ObjectObservation { value, version: observed.version })
    }

    async fn get(&self, name: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        let sealed = read_object(&self.files, name, self.object(name)?).await?;
        self.object(name)?.open(self.cipher.as_ref(), name, sealed).await
    }

    async fn get_range(&self, name: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
        let length = self.head(name).await?.size;
        if range.start > range.end || range.end > length {
            return Err(Error::InvalidRange {
                key: name.to_string(),
                start: range.start,
                end: range.end,
                length,
            });
        }
        Ok(self
            .get(name)
            .await?
            .slice(range.start as usize..range.end as usize))
    }

    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
        let mut objects = Vec::new();
        let prefix = directory.map(|directory| format!("{}/", directory.as_str()));
        for name in self.objects.keys().filter(|name| {
            prefix
                .as_ref()
                .is_none_or(|prefix| name.as_str().starts_with(prefix))
        }) {
            objects.push(self.head(name).await?);
        }
        Ok(objects)
    }

    async fn put(&self, name: &ObjectKey, _: Bytes) -> rtdb_storage::Result<()> {
        Err(Self::write_denied(name))
    }

    async fn put_file(&self, name: &ObjectKey, _: &Path) -> rtdb_storage::Result<()> {
        Err(Self::write_denied(name))
    }

    async fn delete(&self, name: &ObjectKey) -> rtdb_storage::Result<()> {
        Err(Self::write_denied(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn checkpoint_world_store_reads_only_authenticated_inventory_members() -> Result<()> {
        let archive: Arc<dyn ObjectStore> = Arc::new(rtdb_storage::MemoryStore::default());
        let name = key("member")?;
        let other = key("outside-manifest")?;
        let material = [9; crate::keys::KEY_BYTES];
        let bytes = Bytes::from_static(b"checkpoint object");
        let files = PrefixedStore::new(archive.clone(), "world/objects");
        let cipher = crate::keys::legacy_archive_key(&material)?;
        let sealed = cipher.seal(name.as_str(), bytes.clone()).await?;
        files.put(&name, sealed.clone()).await?;
        files.put(&other, sealed.clone()).await?;
        let object = ArchivedObject {
            name: name.to_string(),
            bytes: sealed.len() as u64,
            sha256: super::super::digest(&sealed),
            frame_header_sha256: None,
        };
        let store = CapturedObjects {
            files,
            objects: BTreeMap::from([(name.clone(), object)]),
            cipher,
        };
        assert_eq!(store.get(&name).await?, bytes);
        assert!(matches!(
            store.get(&other).await,
            Err(Error::MissingObject(_))
        ));
        assert!(matches!(
            store.delete(&name).await,
            Err(Error::PermissionDenied { .. })
        ));
        let reopened = CapturedObjects {
            files: PrefixedStore::new(archive, "world/objects"),
            objects: store.objects,
            cipher: crate::keys::legacy_archive_key(&material)?,
        };
        assert_eq!(reopened.get(&name).await?, bytes);
        Ok(())
    }
}
