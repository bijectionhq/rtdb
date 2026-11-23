use std::{collections::BTreeMap, sync::Arc, time::Duration};

use rtdb_columnar::MemoryBudget;
use rtdb_directory::{CheckpointTransferBootstrap, Directory, HostedTask, Lifecycle};
use rtdb_format::{catalog::{BlockCatalog, CheckpointContents}, envelope::ReplicaEnvelope, keys::{CURRENT_STORAGE_VERSION, block_key}};
use rtdb_node::{NodeOptions, ReadFence};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryContext, read_owned_schema_catalog};
use rtdb_replication::{BlockDiscovery, BlockStore, PublishedCheckpoint, RecoveredPartition, TermFence};
use rtdb_storage::{ObjectKey, ObjectStore, cache::{BufferPool, CacheBudget}};
use rtdb_types::{BlockIndex, ReplicaPosition, Result, SourcePosition, StorageEpoch, TableRef, Timestamp, TransactionKey};
use serde::Serialize;
use ring::digest::Context;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{StageReadAuthority, StageSelector, inconsistent, unavailable, with_stage};
use crate::{deployment::CellStorage, keys::KeyService};
use super::super::replica_artifact::{PayloadBytes, ReplicaArtifact};

const MAX_OWNED_RELATIONS: usize = 4096;
const OWNED_FILES_ACK: &[u8] = b"RTDB checkpoint owned event files 1\n";
const OWNED_FILES_END: &[u8] = b"RTDB checkpoint owned event files end\n";

#[derive(Serialize)]
pub(crate) struct StageOwnedRelations {
    database: String,
    source_manifest_sha256: String,
    destination: Uuid,
    stage_operation: Uuid,
    staged_manifest_sha256: String,
    transaction: TransactionKey,
    owner_namespace: Option<String>,
    relations: Vec<String>,
    declarations: String,
}

pub(crate) async fn inspect_staged_owned_relations(
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    keys: &dyn KeyService,
    selector: StageSelector,
    hosted_task: Option<HostedTask>,
) -> Result<StageOwnedRelations> {
    if selector.source.is_none() {
        return Err(inconsistent("Owned relation inventory requires the original stage source"));
    }
    with_stage(directory, storage, keys, selector, hosted_task, |authority, _, verified| async move {
        if verified.contents != CheckpointContents::Replay {
            return Err(inconsistent("Owned relation inventory requires a complete replay stage"));
        }
        let stop = CancellationToken::new();
        let result = tokio::time::timeout(Duration::from_secs(25), inspect(&authority, verified.store, &stop)).await;
        stop.cancel();
        result.map_err(|_| unavailable("The staged owned relation inventory timed out"))?
    }).await
}

/// Materialize a replay input under the target key after verifying it against
/// the original terminal source receipt and the held destination checkpoint.
pub(crate) async fn verify_checkpoint_transfer_target(
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    keys: &dyn KeyService,
    selector: StageSelector,
    hosted_task: Option<HostedTask>,
) -> Result<CheckpointTransferBootstrap> {
    use crate::{
        admin::administration_stream,
        archive::{archive_lifetime_floors_digest, replica_suffix, stage::{RegionPublication, with_region}},
    };
    use super::super::replica_artifact::CertifiedSegments;
    use rtdb_format::envelope::ReplicaBody;

    let (source, capture, org) = selector.source.as_ref()
        .ok_or_else(|| inconsistent("Transfer replay requires the authenticated original source"))?;
    let source = directory.deployment(&source.to_string()).await?
        .filter(|record| record.org == *org && record.lifecycle == Lifecycle::Restoring && record.holder.is_none())
        .ok_or_else(|| unavailable("The fenced source installation is unavailable"))?;
    let terminal = source.checkpoint_transfer_terminal.as_deref()
        .filter(|terminal| source.checkpoint_transfer_fence.as_deref() == Some(&terminal.fence)
            && source.epoch == terminal.fence.fenced_writer_epoch
            && terminal.fence.destination.intent.capture == *capture
            && terminal.fence.destination.intent.destination == selector.destination
            && terminal.fence.destination.intent.operation == selector.operation
            && terminal.fence.destination.staged_manifest_sha256.as_deref() == Some(selector.manifest_sha256.as_str()))
        .ok_or_else(|| inconsistent("The source has no terminal receipt for the exact target attempt"))?
        .clone();
    let replay_directory = Arc::clone(&directory);
    let hosted_task_for_write = hosted_task.clone();
    with_stage(directory, storage, keys, selector, hosted_task, |authority, record, verified| async move {
        verified.require_serving_envelope()?;
        if verified.contents != CheckpointContents::Replay
            || record.checkpoint_transfer_fence.as_deref() != Some(&terminal.fence)
            || authority.stage != terminal.fence.destination
        {
            return Err(inconsistent("The destination inventory has another transfer fence"));
        }
        let operation = authority.stage.intent.operation;
        let prior = record.checkpoint_transfer_bootstrap.as_deref();
        if prior.is_some_and(|receipt| receipt.terminal != terminal) {
            return Err(inconsistent("The target already has another terminal replay artifact"));
        }
        let publication = prior.is_none().then(|| {
            RegionPublication::CheckpointTransfer(Arc::clone(&replay_directory), operation)
        });
        let (held_authority, held_terminal, owner) = (&authority, &terminal, replay_directory.as_ref());
        let hosted = hosted_task_for_write.as_ref();
        let ciphertext_sha256 = with_region(
            owner, keys, &record.id, storage, hosted, None, publication,
            |current| {
                held_authority.require_held(current)?;
                if current.checkpoint_transfer_fence.as_deref() != Some(&held_terminal.fence)
                    || current.checkpoint_transfer_bootstrap.as_deref()
                        .is_some_and(|receipt| receipt.terminal != *held_terminal)
                    || prior.is_some_and(|receipt| current.checkpoint_transfer_bootstrap.as_deref() != Some(receipt))
                {
                    return Err(inconsistent("The staged target no longer owns this bootstrap"));
                }
                Ok(None)
            },
            |current, _, region| async move {
                let bucket = storage.bucket(Arc::clone(&region)).await?;
                let artifact = ReplicaArtifact::open_transfer(bucket.as_ref(), keys, &current, held_terminal, Arc::clone(&region)).await?;
                if let Some(prior) = current.checkpoint_transfer_bootstrap.as_deref() {
                    artifact.read_ordered_transfer(prior.ciphertext_sha256).await?;
                    return Ok(prior.ciphertext_sha256);
                }
                let (mut recovered, _, pool, memory, options) =
                    open_owned_snapshot(held_authority, verified.store).await?;
                let fence = &held_terminal.fence;
                let mut replay = replica_suffix::TerminalReplayProgress::new(fence, &recovered)?;
                let mut chain = artifact.chain();
                if let Some(digest) = held_terminal.replica_sha256 {
                    let (replay, recovered, memory, artifact, chain) = (&mut replay, &mut recovered, &memory, &artifact, &mut chain);
                    replica_suffix::with_terminal_source(
                        owner, storage, keys, hosted, fence, held_terminal.replica_through, Some(held_terminal),
                        |source, source_io| async move {
                            let stream = administration_stream(owner, &source, 4 << 20).await?;
                            let first = transfer_first(&fence.destination)?;
                            let certificate = rtdb_logkeeper::JournalDigest::from_digest_bytes(
                                source.stream("replica"), [first, held_terminal.replica_through], digest,
                            );
                            let _pages = memory.reserve(32 << 20)?;
                            let mut segments = CertifiedSegments::new(rtdb_logkeeper::CertifiedRangeReader::new(&stream, &certificate)?);
                            let source_cipher = replica_suffix::TerminalReplicaCipher::open(&source, keys).await?;
                            let payloads = replica_suffix::ServingPayloads::new(&source, storage, keys, source_io, memory);
                            while let Some(mut entries) = segments.next().await? {
                                let first = entries.first().ok_or_else(|| inconsistent("An authenticated segment was empty"))?.lsn;
                                let through = entries.last().ok_or_else(|| inconsistent("An authenticated segment was empty"))?.lsn;
                                // A provisional read cannot occupy a canonical target name.
                                let proof = rtdb_logkeeper::certify_committed_range(&stream, first, through).await?;
                                rtdb_logkeeper::verify_certified_range(&proof, &entries)?;
                                source_cipher.open_page(&mut entries)?;
                                artifact.put_segment(&entries, chain).await?;
                                for entry in entries {
                                    let Some(envelope) = replay.admit(&entry, fence, recovered)? else { continue; };
                                    if matches!(&envelope.message, ReplicaBody::OversizedMessage(_)) {
                                        let (resolved, payload) = payloads.resolve_verified(&envelope).await?;
                                        replay.apply(resolved, recovered, memory)?;
                                        artifact.put_payload(entry.lsn, &payload.bytes, chain).await?;
                                    } else {
                                        replay.apply(envelope, recovered, memory)?;
                                    }
                                }
                            }
                            Ok(())
                        },
                    ).await?;
                }
                let snapshot = recovered.partition.snapshot();
                if replay.finish(held_terminal.replica_through)? != held_terminal.source_through
                    || snapshot.tx_basis() != Some(held_terminal.basis)
                    || recovered.progress.tx_key != Some(held_terminal.basis)
                    || recovered.progress.source_position.after_offset(StorageEpoch(held_authority.stage.source_log_epoch))
                        != held_terminal.source_through
                {
                    return Err(inconsistent("Destination replay has another terminal transaction or source position"));
                }
                let snapshot = snapshot.with_system_time_cap(Some(held_terminal.basis.system_time))?;
                let name = &held_authority.stage.intent.name;
                let catalog = CatalogSnapshot::new(
                    name, BTreeMap::from([(name.clone(), DatabaseSnapshot { partitions: vec![snapshot], storage: pool })]), true,
                )?;
                let stop = CancellationToken::new();
                let result = tokio::time::timeout(Duration::from_secs(25), async {
                    let context = QueryContext::new(catalog, memory, held_terminal.basis.system_time, "UTC", options.query, &stop)?;
                    archive_lifetime_floors_digest(context, held_terminal.basis).await
                }).await;
                stop.cancel();
                if result.map_err(|_| unavailable("Destination terminal lifetime inventory timed out"))??
                    != held_terminal.lifetime_floors_sha256
                {
                    return Err(inconsistent("Destination replay changed the authenticated lifetime floors"));
                }
                held_authority.confirm(name).await?;
                artifact.put_manifest(chain).await
            },
        ).await?;
        if let Some(prior) = prior {
            return Ok(prior.clone());
        }
        replay_directory.record_checkpoint_transfer_bootstrap(&CheckpointTransferBootstrap {
            terminal, ciphertext_sha256,
        }).await
    }).await
}

/// Provision the reserved target replica journals from the ordered
/// target-key artifact. The stage remains held after the quorum commit.
pub(crate) async fn seed_checkpoint_transfer_target(
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    keys: &dyn KeyService,
    selector: StageSelector,
    hosted_task: Option<HostedTask>,
) -> Result<rtdb_directory::CheckpointTransferReplica> {
    use crate::{
        admin::{choose_ensemble, replica_servers},
        archive::{checkpoint_storage_root, replica_suffix, stage::with_region, verify_cut},
        reference::{PrefixedStore, key},
        store::{EncryptingStore, ROOT_MARKER},
    };
    use rtdb_directory::CheckpointTransferReplica;
    use rtdb_storage::log::LogOffset;

    let bootstrap = verify_checkpoint_transfer_target(
        Arc::clone(&directory), storage, keys, selector, hosted_task.clone(),
    ).await?;
    let terminal = &bootstrap.terminal;
    let stage = &terminal.fence.destination;
    let target_id = stage.intent.destination.to_string();
    let owner = directory.as_ref();
    let source = owner.deployment(&stage.intent.source.to_string()).await?
        .filter(|record| record.checkpoint_transfer_terminal.as_deref() == Some(terminal))
        .ok_or_else(|| inconsistent("The original terminal source receipt changed"))?;
    let admit_target = |record: &rtdb_directory::DeploymentRecord| {
        if record.lifecycle != Lifecycle::Restoring || record.holder.is_some()
            || record.checkpoint_stage.as_deref() != Some(stage)
            || record.checkpoint_transfer_fence.as_deref() != Some(&terminal.fence)
            || record.checkpoint_transfer_bootstrap.as_deref() != Some(&bootstrap)
        {
            return Err(inconsistent("The target no longer holds this ordered bootstrap"));
        }
        Ok(None)
    };
    let ordered = &bootstrap;
    let task = hosted_task.as_ref();
    let directory = &directory;
    Box::pin(with_region(
        owner, keys, &target_id, storage, task, None, None, admit_target,
        |target, _, region| async move {
            let bucket = storage.bucket(Arc::clone(&region)).await?;
            let artifact = ReplicaArtifact::open_transfer(bucket.as_ref(), keys, &target, terminal, region).await?;
            let memory = MemoryBudget::new(NodeOptions::default().execution_memory_bytes);
            let expected_chain = artifact.read_ordered_transfer(ordered.ciphertext_sha256).await?;
            let start = transfer_first(stage)?;
            let through = terminal.replica_through;
            let root = format!("{}/{}", target.storage_prefix(), checkpoint_storage_root()?);
            let files = PrefixedStore::new(Arc::clone(&bucket), root);
            let marker = files.observe_fresh_exact(&key(ROOT_MARKER)?, target.id.len()).await?;
            if marker.value.as_ref() != target.id.as_bytes() {
                return Err(inconsistent("The replica payload store has another target owner"));
            }
            let media = keys.existing_key(&target.key_id()).await?
                .ok_or_else(|| unavailable("The staged serving key is unavailable"))?;
            let serving = EncryptingStore::new(Arc::new(files), &media)?;
            let initial_fence = {
                let root_key = key(&block_key(BlockIndex(stage.checkpoint.block))?)?;
                let bytes = PayloadBytes::read(&serving, &root_key, &memory).await?;
                let catalog = BlockCatalog::decode(&bytes.bytes)?;
                verify_cut(&catalog, stage.checkpoint, StorageEpoch(stage.source_log_epoch))?;
                TermFence::from_catalog(&catalog)?
            };
            let source_epoch = terminal.fence.source_writer_epoch;
            let mut fence = initial_fence.clone();
            let mut writer = None;
            let mut next = start;
            let mut chain = artifact.chain();
            // Confirm the complete ordered artifact before provisioning the
            // first journal. The streaming write pass reauthenticates each page.
            while next <= through {
                let _segment = memory.reserve(replica_suffix::MAX_SEGMENT_BYTES * 4)?;
                let entries = artifact.read_segment(next, through, &mut chain).await?;
                for entry in &entries {
                    if let Some(envelope) = replica_suffix::admit_record(entry, &mut fence, &mut writer, source_epoch)? {
                        observe_transfer_payload(&artifact, entry.lsn, &envelope, &mut chain, &memory).await?;
                    }
                }
                next += entries.len() as i64;
            }
            super::super::replica_artifact::require_chain(chain, expected_chain)?;

            let reserved = if let Some(existing) = target.checkpoint_transfer_replica.as_deref() {
                existing.clone()
            } else {
                let mut allocated = target.clone();
                allocated.ensemble = choose_ensemble(owner, &target, source.ensemble.len()).await?;
                let journals = replica_servers(owner, &allocated).await?;
                owner.reserve_checkpoint_transfer_replica(&CheckpointTransferReplica {
                    bootstrap: ordered.clone(), ensemble: allocated.ensemble, journals,
                }).await?
            };
            if reserved.bootstrap != *ordered {
                return Err(inconsistent("The reserved target replica belongs to another bootstrap"));
            }
            let stream = rtdb_logkeeper::StreamConfig {
                servers: reserved.journals.clone(), stream: target.stream("replica"),
                write_quorum: crate::deployment::write_quorum(reserved.journals.len()),
                maximum_message_bytes: crate::process::MAXIMUM_LOG_MESSAGE_BYTES,
            };
            let epoch = terminal.fence.fenced_writer_epoch;
            let checkpoint = LogOffset(stage.checkpoint.boundary);
            if start > through {
                rtdb_logkeeper::bootstrap_committed_page(&stream, epoch, checkpoint, LogOffset(start), &[], true).await?;
                return Ok(reserved);
            }
            let scope = target.replica_seal_scope();
            let journal_cipher = crate::cipher::Cipher::new(&media)?;
            let mut fence = initial_fence;
            let mut writer = None;
            let mut next = start;
            let mut chain = artifact.chain();
            while next <= through {
                let _segment = memory.reserve(replica_suffix::MAX_SEGMENT_BYTES * 4)?;
                let first = next;
                let mut entries = artifact.read_segment(first, through, &mut chain).await?;
                for entry in &mut entries {
                    let Some(envelope) = replica_suffix::admit_record(entry, &mut fence, &mut writer, source_epoch)? else { continue; };
                    let offloaded = observe_transfer_payload(&artifact, entry.lsn, &envelope, &mut chain, &memory).await?;
                    let resolved = offloaded.as_ref().map_or(&envelope, |payload| &payload.resolved);
                    if let Some(control) = replica_suffix::transfer_control(resolved, stage.checkpoint)? {
                        entry.payload = control;
                    } else if let Some(payload) = offloaded.as_ref() {
                        let proxy = target_payload_proxy(&envelope, resolved, &payload.body.bytes)?;
                        publish_target_payload(directory, storage, keys, task, &target, &reserved, &payload.path, &payload.body.bytes).await?;
                        if let Some(proxy) = proxy { entry.payload = proxy; }
                    }
                }
                next += entries.len() as i64;
                let last = next - 1;
                // Preserve the source transaction IDs and term sequences under
                // the target's own seal.
                for entry in &mut entries {
                    entry.payload = journal_cipher.seal_deterministic(&scope, &entry.payload)?;
                }
                rtdb_logkeeper::bootstrap_committed_page(
                    &stream, epoch, checkpoint, LogOffset(first), &entries, last == through,
                ).await?;
                let proof = rtdb_logkeeper::certify_committed_range(&stream, first, last).await?;
                rtdb_logkeeper::verify_certified_range(&proof, &entries)?;
            }
            super::super::replica_artifact::require_chain(chain, expected_chain)?;
            Ok(reserved)
        },
    )).await
}

struct DetachedTransferPayload {
    path: ObjectKey,
    resolved: ReplicaEnvelope,
    body: PayloadBytes,
}

async fn observe_transfer_payload(
    artifact: &ReplicaArtifact<'_>, offset: i64, envelope: &ReplicaEnvelope, chain: &mut Context,
    memory: &Arc<MemoryBudget>,
) -> Result<Option<DetachedTransferPayload>> {
    let Some(relative) = envelope.oversized_payload(CURRENT_STORAGE_VERSION, StorageEpoch(0))? else {
        return Ok(None);
    };
    let body = artifact.read_payload(offset, chain, memory).await?;
    let resolved = envelope.unwrap_payload(&body.bytes)?;
    Ok(Some(DetachedTransferPayload { path: crate::reference::key(&relative)?, resolved, body }))
}

pub(super) async fn verify_seeded_payloads(
    bucket: &dyn ObjectStore, serving: &dyn ObjectStore, staged: &dyn ObjectStore,
    keys: &dyn KeyService, record: &rtdb_directory::DeploymentRecord, stage: &rtdb_directory::CheckpointStage,
    mut extras: std::collections::BTreeSet<ObjectKey>, region: Arc<IoRegion>,
) -> Result<()> {
    use crate::archive::{admits_seeded_transfer_payload, replica_suffix, verify_cut};
    use crate::reference::key;

    if extras.iter().any(|path| !admits_seeded_transfer_payload(record, stage, path.as_str())) {
        return Err(inconsistent("The staged storage root differs from its accepted inventory"));
    }
    let bootstrap = record.checkpoint_transfer_bootstrap.as_deref()
        .ok_or_else(|| inconsistent("The staged payloads need an ordered bootstrap"))?;
    let terminal = &bootstrap.terminal;
    let artifact = ReplicaArtifact::open_transfer(bucket, keys, record, terminal, region).await?;
    let memory = MemoryBudget::new(NodeOptions::default().execution_memory_bytes);
    let expected = artifact.read_ordered_transfer(bootstrap.ciphertext_sha256).await?;
    let root_key = key(&block_key(BlockIndex(stage.checkpoint.block))?)?;
    let root = PayloadBytes::read(staged, &root_key, &memory).await?;
    let catalog = BlockCatalog::decode(&root.bytes)?;
    verify_cut(&catalog, stage.checkpoint, StorageEpoch(stage.source_log_epoch))?;
    let mut fence = TermFence::from_catalog(&catalog)?;
    let mut writer = None;
    let mut next = transfer_first(stage)?;
    let mut chain = artifact.chain();
    while next <= terminal.replica_through {
        let _segment = memory.reserve(replica_suffix::MAX_SEGMENT_BYTES * 4)?;
        let entries = artifact.read_segment(next, terminal.replica_through, &mut chain).await?;
        for entry in &entries {
            let Some(envelope) = replica_suffix::admit_record(entry, &mut fence, &mut writer, terminal.fence.source_writer_epoch)? else { continue; };
            let Some(payload) = observe_transfer_payload(&artifact, entry.lsn, &envelope, &mut chain, &memory).await? else { continue; };
            if replica_suffix::transfer_control(&payload.resolved, stage.checkpoint)?.is_none() && extras.remove(&payload.path)
                && serving.get_fresh_exact(&payload.path, payload.body.bytes.len()).await? != payload.body.bytes
            {
                return Err(inconsistent("A staged replica payload differs from its ordered bootstrap"));
            }
        }
        next += entries.len() as i64;
    }
    super::super::replica_artifact::require_chain(chain, expected)?;
    if !extras.is_empty() { return Err(inconsistent("The staged storage root contains an unordered replica payload")); }
    Ok(())
}

fn target_payload_proxy(original: &ReplicaEnvelope, resolved: &ReplicaEnvelope, body: &[u8]) -> Result<Option<bytes::Bytes>> {
    let rtdb_format::envelope::ReplicaBody::OversizedMessage(reference) = &original.message else {
        return Err(inconsistent("The target payload has no certified reference"));
    };
    if reference.payload_sha256.is_none() { return Ok(None); }
    let id = reference.payload_id.clone().ok_or_else(|| inconsistent("The target payload has no object identity"))?;
    let proxy = resolved.offloaded(CURRENT_STORAGE_VERSION, StorageEpoch(0), BlockIndex(reference.block_index.unwrap_or_default()), id, body);
    Ok(Some(proxy.encode().into()))
}

async fn publish_target_payload(
    directory: &Arc<dyn Directory>, storage: &CellStorage, keys: &dyn KeyService, task: Option<&HostedTask>,
    target: &rtdb_directory::DeploymentRecord, reserved: &rtdb_directory::CheckpointTransferReplica,
    path: &ObjectKey, bytes: &bytes::Bytes,
) -> Result<()> {
    use crate::{
        archive::{checkpoint_storage_root, admits_seeded_transfer_payload, stage::{RegionPublication, with_region}},
        reference::{PrefixedStore, key}, store::{CheckpointReplayPublication, EncryptingStore},
    };
    let root = format!("{}/{}", target.storage_prefix(), checkpoint_storage_root()?);
    let publication = RegionPublication::CheckpointReplay(Arc::clone(directory), CheckpointReplayPublication {
        bootstrap_sha256: reserved.bootstrap.ciphertext_sha256,
        object: key(&format!("{root}/{path}"))?,
    });
    let stage = &reserved.bootstrap.terminal.fence.destination;
    Box::pin(with_region(
        directory.as_ref(), keys, &target.id, storage, task, None, Some(publication),
        |current| {
            if current.lifecycle != Lifecycle::Restoring || current.holder.is_some()
                || current.checkpoint_transfer_replica.as_deref() != Some(reserved)
                || !admits_seeded_transfer_payload(current, stage, path.as_str())
            {
                return Err(inconsistent("The target no longer owns this detached replay payload"));
            }
            Ok(None)
        },
        |current, _, io| async move {
            let bucket = storage.bucket(io).await?;
            let media = keys.existing_key(&current.key_id()).await?
                .ok_or_else(|| unavailable("The staged serving key is unavailable"))?;
            let serving = EncryptingStore::new(Arc::new(PrefixedStore::new(bucket, root)), &media)?;
            serving.put_with_publication(path, bytes.clone()).await?;
            if serving.get_fresh_exact(path, bytes.len()).await? != *bytes {
                return Err(inconsistent("The target offloaded replica payload differs from its ordered artifact"));
            }
            Ok(())
        },
    )).await
}

fn transfer_first(stage: &rtdb_directory::CheckpointStage) -> Result<i64> {
    stage.checkpoint.boundary.checked_add(1)
        .ok_or_else(|| inconsistent("The checkpoint has no terminal replica successor"))
}

/// Activate only a still-held target after its exact replica seed and serving
/// root have both been observed through their production recovery owners.
pub(crate) async fn activate_checkpoint_transfer_target(
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    keys: &dyn KeyService,
    selector: StageSelector,
    hosted_task: Option<HostedTask>,
) -> Result<rtdb_directory::CheckpointTransferActivation> {
    use crate::{
        archive::{checkpoint_storage_root, recovery_manifest_sha256, stage::with_region},
        packing::PackDiscovery,
        reference::key,
        store::{EncryptingStore, ROOT_MARKER, open_deployment_store, wall_clock},
    };
    use rtdb_directory::CheckpointTransferActivation;

    let target_id = selector.destination.to_string();
    let target = directory.deployment(&target_id).await?
        .ok_or_else(|| unavailable("The checkpoint transfer target is unavailable"))?;
    if let Some(accepted) = target.checkpoint_transfer_activation.as_deref() {
        let stage = &accepted.replica.bootstrap.terminal.fence.destination;
        if stage.intent.destination != selector.destination || stage.intent.operation != selector.operation
            || stage.staged_manifest_sha256.as_deref() != Some(selector.manifest_sha256.as_str())
            || !selector.source.as_ref().is_some_and(|(source, capture, org)|
                *source == stage.intent.source && *capture == stage.intent.capture && *org == stage.intent.org)
        {
            return Err(inconsistent("The target already activated another transfer"));
        }
        return Ok(accepted.clone());
    }
    let replica = seed_checkpoint_transfer_target(
        Arc::clone(&directory), storage, keys, selector, hosted_task.clone(),
    ).await?;
    let terminal = &replica.bootstrap.terminal;
    let stage = &terminal.fence.destination;
    let recovery_manifest_sha256 = with_region(
        directory.as_ref(), keys, &target_id, storage, hosted_task.as_ref(), None, None,
        |record| {
            if record.lifecycle != Lifecycle::Restoring || record.holder.is_some()
                || record.checkpoint_stage.as_deref() != Some(stage)
                || record.checkpoint_transfer_replica.as_deref() != Some(&replica)
            {
                return Err(inconsistent("The target lost its reserved replica genesis"));
            }
            Ok(None)
        },
        |record, _, region| async move {
            let root = format!("{}/{}", record.storage_prefix(), checkpoint_storage_root()?);
            let bucket = storage.bucket(region).await?;
            let owner = bucket.get_fresh(&key(&format!("{root}/{ROOT_MARKER}"))?).await?;
            if owner.as_ref() != record.id.as_bytes() {
                return Err(inconsistent("The durable serving root has another owner"));
            }
            let material = keys.existing_key(&record.key_id()).await?
                .ok_or_else(|| unavailable("The staged serving key is unavailable"))?;
            let store = open_deployment_store(
                bucket, &root, PackDiscovery::From(stage.checkpoint.block), wall_clock(),
            ).await?;
            let plaintext = EncryptingStore::new(store, &material)?;
            recovery_manifest_sha256(&plaintext, stage.checkpoint, StorageEpoch(stage.source_log_epoch)).await
        },
    ).await?;
    directory.activate_checkpoint_transfer(&CheckpointTransferActivation { replica, recovery_manifest_sha256 }).await
}

pub(crate) async fn stream_staged_owned_event_files(
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    keys: &dyn KeyService,
    selector: StageSelector,
    hosted_task: Option<HostedTask>,
    stop: CancellationToken,
    mut stream: impl AsyncWrite + Unpin + Send,
) -> Result<()> {
    if selector.source.is_none() {
        return Err(inconsistent("Owned event files require the original stage source"));
    }
    with_stage(directory, storage, keys, selector, hosted_task, |authority, _, verified| async move {
        if verified.contents != CheckpointContents::Replay {
            return Err(inconsistent("Owned event files require a complete replay stage"));
        }
        // The cancelled operation drops its storage readers before with_stage releases the region.
        stop.run_until_cancelled(write_event_files(&authority, verified.store, &mut stream))
            .await.ok_or_else(|| unavailable("The staged owned event stream was cancelled"))?
    }).await
}

#[derive(Serialize)]
struct EventStreamHeader<'a> {
    database: &'a str,
    source: Uuid,
    capture: Uuid,
    source_manifest_sha256: &'a str,
    destination: Uuid,
    stage_operation: Uuid,
    staged_manifest_sha256: &'a str,
    transaction: TransactionKey,
    owner_namespace: &'a str,
    relations: usize,
}

#[derive(Serialize)]
struct EventRelationHeader<'a> {
    relation: &'a str,
    files: usize,
}

#[derive(Serialize)]
struct EventFileHeader {
    trie: String,
    bytes: usize,
}

async fn write_event_files(
    authority: &StageReadAuthority,
    store: Arc<dyn ObjectStore>,
    stream: &mut (impl AsyncWrite + Unpin),
) -> Result<()> {
    let stage = &authority.stage;
    let database = &stage.intent.name;
    let (recovered, transaction, storage, memory, _) = open_owned_snapshot(authority, store).await?;
    let snapshot = recovered.partition.snapshot();
    if snapshot.tables().iter().any(|table| table.owned_lifetime_relation().is_some()) {
        return Err(unavailable("Fresh checkpoint replay has no original lifetime-floor transfer authority"));
    }
    let _catalog_charge = memory.reserve(snapshot.schema_allocation_bound(database)?)?;
    let selected = select_namespace(snapshot.tables())?
        .ok_or_else(|| inconsistent("The staged checkpoint has no owned namespace"))?;
    for table in selected.relations.values() {
        if snapshot.iter_segments(table).next().is_some() {
            return Err(inconsistent("The staged checkpoint has unflushed owned events"));
        }
    }
    write_bytes(stream, OWNED_FILES_ACK).await?;
    write_header(stream, &EventStreamHeader {
        database,
        source: stage.intent.source,
        capture: stage.intent.capture,
        source_manifest_sha256: &stage.intent.source_manifest_sha256,
        destination: stage.intent.destination,
        stage_operation: stage.intent.operation,
        staged_manifest_sha256: staged_manifest(authority)?,
        transaction,
        owner_namespace: &selected.owner,
        relations: selected.relations.len(),
    }).await?;
    for (name, table) in &selected.relations {
        authority.confirm(database).await?;
        let catalog = snapshot.trie_catalog();
        let _trie_charge = memory.reserve(catalog.iter_tries(table).count().checked_mul(32)
            .ok_or_else(|| inconsistent("The staged trie inventory exceeds its bound"))?)?;
        let mut tries = snapshot.trie_entries(table);
        tries.sort_unstable_by(|left, right| {
            right.key.block_index().cmp(&left.key.block_index()).then_with(|| left.key.cmp(&right.key))
        });
        write_header(stream, &EventRelationHeader { relation: name, files: tries.len() }).await?;
        if tries.is_empty() { continue; }
        let entry = snapshot.table_entry(table)
            .ok_or_else(|| inconsistent("A staged owned relation has no registered table slug"))?;
        for trie in tries {
            authority.confirm(database).await?;
            let key = ObjectKey::new(entry.slug.data_key(&trie.key))?;
            let file = storage.get_or(&key, &memory).await?;
            if file.as_slice().is_empty() {
                return Err(inconsistent("A staged owned event file is empty"));
            }
            write_header(stream, &EventFileHeader { trie: trie.key.to_string(), bytes: file.as_slice().len() }).await?;
            write_bytes(stream, file.as_slice()).await?;
        }
    }
    authority.confirm(database).await?;
    write_bytes(stream, OWNED_FILES_END).await?;
    stream.shutdown().await.map_err(|_| unavailable("The staged owned event stream closed"))
}

async fn write_header(stream: &mut (impl AsyncWrite + Unpin), header: &impl Serialize) -> Result<()> {
    let mut bytes = serde_json::to_vec(header).map_err(|_| inconsistent("The staged owned file header is invalid"))?;
    if bytes.len() >= 4096 {
        return Err(inconsistent("The staged owned file header exceeds its bound"));
    }
    bytes.push(b'\n');
    write_bytes(stream, &bytes).await
}

async fn write_bytes(stream: &mut (impl AsyncWrite + Unpin), bytes: &[u8]) -> Result<()> {
    stream.write_all(bytes).await.map_err(|_| unavailable("The staged owned event stream closed"))
}

async fn inspect(
    authority: &StageReadAuthority,
    store: Arc<dyn ObjectStore>,
    stop: &CancellationToken,
) -> Result<StageOwnedRelations> {
    let stage = &authority.stage;
    let database = &stage.intent.name;
    let (recovered, transaction, storage, memory, options) = open_owned_snapshot(authority, store).await?;
    let snapshot = recovered.partition.snapshot();
    if snapshot.tables().iter().any(|table| table.owned_lifetime_relation().is_some()) {
        return Err(unavailable("Fresh checkpoint replay has no original lifetime-floor transfer authority"));
    }
    let _catalog_charge = memory.reserve(snapshot.schema_allocation_bound(database)?)?;
    let selected = select_namespace(snapshot.tables())?;
    let (owner_namespace, relations, declarations) = if let Some(selected) = selected {
        let snapshot = snapshot.with_system_time_cap(Some(transaction.system_time))?;
        let catalog = CatalogSnapshot::new(database.as_str(), BTreeMap::from([(
            database.clone(), DatabaseSnapshot { partitions: vec![snapshot], storage },
        )]), true)?;
        let context = QueryContext::new(catalog, memory, transaction.system_time, "UTC", options.query, stop)?;
        let (declarations, _charge) = read_owned_schema_catalog(context, selected.catalog).await?;
        let declarations = String::from_utf8(declarations.into_catalog_json())
            .map_err(|_| inconsistent("The staged relation declarations are malformed"))?;
        (Some(selected.owner), selected.relations.into_keys().collect(), declarations)
    } else {
        (None, Vec::new(), "{}".to_owned())
    };
    authority.confirm(database).await?;
    Ok(StageOwnedRelations {
        database: database.clone(),
        source_manifest_sha256: stage.intent.source_manifest_sha256.clone(),
        destination: stage.intent.destination,
        stage_operation: stage.intent.operation,
        staged_manifest_sha256: staged_manifest(authority)?.to_owned(),
        transaction,
        owner_namespace,
        relations,
        declarations,
    })
}

async fn open_owned_snapshot(
    authority: &StageReadAuthority,
    store: Arc<dyn ObjectStore>,
) -> Result<(RecoveredPartition, TransactionKey, Arc<BufferPool>, Arc<MemoryBudget>, NodeOptions)> {
    let stage = &authority.stage;
    let database = &stage.intent.name;
    authority.confirm(database).await?;
    let options = NodeOptions::default();
    let memory = options.query.query_memory(&MemoryBudget::new(options.execution_memory_bytes));
    let storage = Arc::new(BufferPool::with_metadata_budget(
        store.clone(), CacheBudget::new(options.cache_bytes), memory.clone(),
    ));
    let blocks = BlockStore::new(store, storage.clone(), memory.clone(), options.file_size_target)?;
    let checkpoint = stage.checkpoint;
    let root_sha256 = checkpoint.root_sha256
        .ok_or_else(|| inconsistent("The staged checkpoint has no original root digest"))?;
    let epoch = StorageEpoch(stage.source_log_epoch);
    let published = PublishedCheckpoint {
        block: BlockIndex(checkpoint.block),
        boundary: ReplicaPosition::from_epoch_offset(epoch, checkpoint.boundary)?,
        root_sha256: Some(root_sha256),
    };
    let recovered = blocks.recover(
        options.trie,
        SourcePosition::from_epoch_offset(epoch, checkpoint.source)?,
        Timestamp(checkpoint.system_time),
        BlockDiscovery::AtPublished(published),
    ).await?;
    let snapshot = recovered.partition.snapshot();
    let transaction = recovered.progress.tx_key.filter(|cut| {
        cut.tx_id.0 >= 0 && cut.system_time.0 == checkpoint.system_time && snapshot.tx_basis() == Some(*cut)
    })
        .ok_or_else(|| inconsistent("The staged checkpoint has a different transaction key"))?;
    Ok((recovered, transaction, storage, memory, options))
}

fn staged_manifest(authority: &StageReadAuthority) -> Result<&str> {
    authority.stage.staged_manifest_sha256.as_deref()
        .ok_or_else(|| inconsistent("The staged checkpoint has no accepted inventory"))
}

struct CapturedNamespace {
    owner: String,
    catalog: TableRef,
    relations: BTreeMap<String, TableRef>,
}

fn select_namespace(tables: impl IntoIterator<Item = TableRef>) -> Result<Option<CapturedNamespace>> {
    let mut owner: Option<String> = None;
    let mut catalog = None;
    let mut relations = BTreeMap::new();
    for table in tables.into_iter().filter(|table| TableRef::owned_schema(&table.schema)) {
        let name = table.owned_storage_name()
            .ok_or_else(|| inconsistent("The stage contains an unsupported owned relation"))?;
        let (namespace, _) = table.table.split_once('-')
            .ok_or_else(|| inconsistent("The staged owned relation has no namespace"))?;
        if owner.as_deref().is_some_and(|previous| previous != namespace) {
            return Err(inconsistent("The stage contains multiple owned relation namespaces"));
        }
        if relations.len() == MAX_OWNED_RELATIONS {
            return Err(inconsistent("The staged owned relation inventory exceeds its bound"));
        }
        if owner.is_none() { owner = Some(namespace.to_owned()); }
        if table.is_owned_relation_catalog() { catalog = Some(table.clone()); }
        if relations.insert(name.to_owned(), table).is_some() {
            return Err(inconsistent("The staged owned relation inventory contains a duplicate"));
        }
    }
    let Some(owner) = owner else { return Ok(None); };
    let catalog = catalog.ok_or_else(|| inconsistent("The stage has owned relations without their declaration catalog"))?;
    Ok(Some(CapturedNamespace { owner, catalog, relations }))
}

#[cfg(test)]
mod tests {
    use rtdb_types::OwnedRelation;

    use super::*;

    #[test]
    fn stage_inventory_is_bound_to_one_captured_declaration_namespace() -> Result<()> {
        let facts = OwnedRelation::new("facts")?;
        let catalog = OwnedRelation::relation_catalog("source-owner");
        let data = facts.table("source-owner");
        let selected = select_namespace([data.clone(), catalog.clone(), TableRef::new("public", "facts")])?.unwrap();
        assert_eq!(selected.catalog, catalog);
        assert_eq!(selected.relations, BTreeMap::from([
            (".relation-schemas".to_owned(), catalog.clone()),
            ("facts".to_owned(), data.clone()),
        ]));
        assert!(select_namespace([data]).is_err());
        assert!(select_namespace([catalog, facts.table("other-owner")]).is_err());
        assert!(select_namespace([TableRef::new("public", "ordinary")])?.is_none());
        assert!(select_namespace([TableRef::new("rtdb_owned_v1", "unsigned-facts")]).is_err());
        Ok(())
    }
}
