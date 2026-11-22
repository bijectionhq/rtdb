use std::{collections::BTreeMap, sync::Arc, time::Duration};

use rtdb_columnar::MemoryBudget;
use rtdb_directory::{CheckpointTransferBootstrap, Directory, HostedTask, Lifecycle};
use rtdb_format::catalog::CheckpointContents;
use rtdb_node::{NodeOptions, ReadFence};
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryContext, read_owned_schema_catalog};
use rtdb_replication::{BlockDiscovery, BlockStore, PublishedCheckpoint, RecoveredPartition};
use rtdb_storage::{ObjectKey, ObjectStore, cache::{BufferPool, CacheBudget}};
use rtdb_types::{BlockIndex, ReplicaPosition, Result, SourcePosition, StorageEpoch, TableRef, Timestamp, TransactionKey};
use serde::Serialize;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{StageReadAuthority, StageSelector, inconsistent, unavailable, with_stage};
use crate::{deployment::CellStorage, keys::KeyService};

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
        archive::{archive_lifetime_floors_digest, checkpoint_transfer_object, encode, replica_suffix, sha256,
            stage::{RegionPublication, put_exact, with_region}},
        cipher::Cipher,
        reference::key,
    };

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
        if verified.contents != CheckpointContents::Replay
            || record.checkpoint_transfer_fence.as_deref() != Some(&terminal.fence)
            || authority.stage != terminal.fence.destination
        {
            return Err(inconsistent("The destination inventory has another transfer fence"));
        }
        if let Some(prior) = record.checkpoint_transfer_bootstrap.as_deref() {
            if prior.terminal != terminal {
                return Err(inconsistent("The target already has another terminal replay artifact"));
            }
            with_region(
                replay_directory.as_ref(), keys, &record.id, storage, hosted_task_for_write.as_ref(),
                None, None,
                |current| {
                    authority.require_held(current)?;
                    if current.checkpoint_transfer_bootstrap.as_deref() != Some(prior) {
                        return Err(inconsistent("The target bootstrap receipt changed"));
                    }
                    Ok(None)
                },
                |_, _, region| async {
                    let object = key(&checkpoint_transfer_object(&record, authority.stage.intent.operation))?;
                    let bucket = storage.bucket(region).await?;
                    let size = usize::try_from(bucket.head_fresh(&object).await?.size)
                        .ok().filter(|size| *size <= replica_suffix::MAX_SUFFIX_BYTES + (16 << 10) + 64)
                        .ok_or_else(|| inconsistent("The target bootstrap object exceeds its storage bound"))?;
                    let bytes = bucket.observe_fresh_exact(&object, size).await?.value;
                    if sha256(&bytes) != prior.ciphertext_sha256 {
                        return Err(inconsistent("The ordered target bootstrap cannot be reopened"));
                    }
                    Ok(())
                },
            ).await?;
            return Ok(prior.clone());
        }
        let stop = CancellationToken::new();
        let result = tokio::time::timeout(Duration::from_secs(25), async {
            let entries = if let Some(digest) = terminal.replica_sha256 {
                let stream = administration_stream(replay_directory.as_ref(), &source, 4 << 20).await?;
                let first = authority.stage.checkpoint.boundary.checked_add(1)
                    .ok_or_else(|| inconsistent("The checkpoint has no terminal replica successor"))?;
                let certificate = rtdb_logkeeper::JournalDigest::from_digest_bytes(
                    source.stream("replica"), [first, terminal.replica_through], digest,
                );
                let entries = rtdb_logkeeper::copy_certified_range(
                    &stream, &certificate, replica_suffix::MAX_SUFFIX_BYTES, replica_suffix::MAX_SUFFIX_ENTRIES,
                ).await?;
                rtdb_logkeeper::verify_certified_range(&certificate, &entries)?;
                entries
            } else {
                if terminal.replica_through != authority.stage.checkpoint.boundary {
                    return Err(inconsistent("The terminal replica interval has no certificate"));
                }
                Vec::new()
            };

            let entries = replica_suffix::open_terminal_entries(&source, keys, entries).await?;
            let (mut recovered, _, pool, memory, options) =
                open_owned_snapshot(&authority, verified.store).await?;
            replica_suffix::apply_terminal(&entries, &terminal.fence, &mut recovered, &memory)?;
            let snapshot = recovered.partition.snapshot();
            if snapshot.tx_basis() != Some(terminal.basis)
                || recovered.progress.tx_key != Some(terminal.basis)
                || recovered.progress.source_position.after_offset(StorageEpoch(authority.stage.source_log_epoch))
                    != terminal.source_through
            {
                return Err(inconsistent("Destination replay has another terminal transaction or source position"));
            }
            let snapshot = snapshot.with_system_time_cap(Some(terminal.basis.system_time))?;
            let name = &authority.stage.intent.name;
            let catalog = CatalogSnapshot::new(
                name, BTreeMap::from([(name.clone(), DatabaseSnapshot { partitions: vec![snapshot], storage: pool })]), true,
            )?;
            let context = QueryContext::new(catalog, memory, terminal.basis.system_time, "UTC", options.query, &stop)?;
            if archive_lifetime_floors_digest(context, terminal.basis).await? != terminal.lifetime_floors_sha256 {
                return Err(inconsistent("Destination replay changed the authenticated lifetime floors"));
            }
            authority.confirm(name).await?;
            let receipt = encode(&terminal)?;
            if receipt.len() > 16 << 10 {
                return Err(inconsistent("The terminal receipt exceeds the bootstrap bound"));
            }
            let receipt_len = receipt.len() as u32;
            let replay = replica_suffix::encode_records(&entries)?;
            let mut plaintext = Vec::with_capacity(4 + receipt.len() + replay.len());
            plaintext.extend_from_slice(&receipt_len.to_be_bytes());
            plaintext.extend_from_slice(&receipt);
            plaintext.extend_from_slice(&replay);
            let operation = authority.stage.intent.operation;
            let object = key(&checkpoint_transfer_object(&record, operation))?;
            let material = keys.existing_key(&record.key_id()).await?
                .ok_or_else(|| unavailable("The original target media key is unavailable"))?;
            let bytes = Cipher::new(&material)?.seal_deterministic(object.as_str(), &plaintext)?;
            let bootstrap = CheckpointTransferBootstrap {
                terminal, ciphertext_sha256: sha256(&bytes),
            };
            Ok((object, bytes, bootstrap))
        }).await;
        stop.cancel();
        let (object, bytes, bootstrap) =
            result.map_err(|_| unavailable("Destination terminal replay timed out"))??;

        let operation = authority.stage.intent.operation;
        with_region(
            replay_directory.as_ref(), keys, &record.id, storage, hosted_task_for_write.as_ref(),
            None, Some(RegionPublication::CheckpointTransfer(Arc::clone(&replay_directory), operation)),
            |current| {
                authority.require_held(current)?;
                if current.checkpoint_transfer_fence.as_deref() != Some(&bootstrap.terminal.fence)
                    || current.checkpoint_transfer_bootstrap.as_deref().is_some_and(|prior| prior != &bootstrap)
                {
                    return Err(inconsistent("The staged target no longer owns this bootstrap"));
                }
                Ok(None)
            },
            |_, _, region| async {
                let bucket = storage.bucket(region).await?;
                put_exact(bucket.as_ref(), &object, bytes.clone()).await?;
                if bucket.get(&object).await? != bytes {
                    return Err(inconsistent("The target replay artifact changed before receipt ordering"));
                }
                Ok(())
            },
        ).await?;
        replay_directory.record_checkpoint_transfer_bootstrap(&bootstrap).await
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
        archive::{checkpoint_transfer_object, encode, replica_suffix, sha256, stage::with_region},
        cipher::Cipher,
        reference::key,
    };
    use rtdb_directory::CheckpointTransferReplica;
    use rtdb_storage::log::LogOffset;

    let bootstrap = verify_checkpoint_transfer_target(
        Arc::clone(&directory), storage, keys, selector, hosted_task.clone(),
    ).await?;
    let terminal = &bootstrap.terminal;
    let stage = &terminal.fence.destination;
    let target_id = stage.intent.destination.to_string();
    let source = directory.deployment(&stage.intent.source.to_string()).await?
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
    let (target, entries) = with_region(
        directory.as_ref(), keys, &target_id, storage, hosted_task.as_ref(), None, None, admit_target,
        |record, _, region| {
            let bootstrap = &bootstrap;
            async move {
                let object = key(&checkpoint_transfer_object(&record, stage.intent.operation))?;
                let bucket = storage.bucket(region).await?;
                let size = usize::try_from(bucket.head_fresh(&object).await?.size)
                    .ok().filter(|size| *size <= replica_suffix::MAX_SUFFIX_BYTES + (16 << 10) + 64)
                    .ok_or_else(|| inconsistent("The ordered bootstrap exceeds its storage bound"))?;
                let sealed = bucket.observe_fresh_exact(&object, size).await?.value;
                if sha256(&sealed) != bootstrap.ciphertext_sha256 {
                    return Err(inconsistent("The target-key artifact differs from the ordered digest"));
                }
                let key = keys.existing_key(&record.key_id()).await?
                    .ok_or_else(|| unavailable("The original target media key is unavailable"))?;
                let cipher = Cipher::new(&key)?;
                let opened = cipher.open(object.as_str(), &sealed)?;
                let receipt = encode(terminal)?;
                let length = (receipt.len() as u32).to_be_bytes();
                if opened.get(..4) != Some(length.as_slice()) || opened.get(4..4 + receipt.len()) != Some(receipt.as_ref()) {
                    return Err(inconsistent("The target-key artifact names another terminal receipt"));
                }
                let mut entries = replica_suffix::decode(opened.slice(4 + receipt.len()..))?;
                let through = entries.last().map_or(stage.checkpoint.boundary, |entry| entry.lsn);
                if through != terminal.replica_through
                    || replica_suffix::terminal_source_through(&entries, &terminal.fence)? != terminal.source_through
                {
                    return Err(inconsistent("The target replica artifact has another terminal boundary"));
                }
                let scope = record.replica_seal_scope();
                for entry in &mut entries {
                    entry.payload = cipher.seal_deterministic(&scope, &entry.payload)?;
                }
                Ok((record, entries))
            }
        },
    ).await?;

    let reserved = if let Some(existing) = target.checkpoint_transfer_replica.as_deref() {
        existing.clone()
    } else {
        let mut allocated = target.clone();
        allocated.ensemble = choose_ensemble(directory.as_ref(), &target, source.ensemble.len()).await?;
        let journals = replica_servers(directory.as_ref(), &allocated).await?;
        directory.reserve_checkpoint_transfer_replica(&CheckpointTransferReplica {
            bootstrap: bootstrap.clone(), ensemble: allocated.ensemble, journals,
        }).await?
    };
    if reserved.bootstrap != bootstrap {
        return Err(inconsistent("The reserved target replica belongs to another bootstrap"));
    }
    let stream = rtdb_logkeeper::StreamConfig {
        servers: reserved.journals.clone(), stream: target.stream("replica"),
        write_quorum: crate::deployment::write_quorum(reserved.journals.len()),
        maximum_message_bytes: 4 << 20,
    };
    // The source transaction IDs and term sequences are in the already
    // authenticated payload, and remain unchanged by the target log seal.
    rtdb_logkeeper::bootstrap_committed(
        &stream, terminal.fence.fenced_writer_epoch, LogOffset(stage.checkpoint.boundary), &entries,
    ).await?;
    if let Some(first) = entries.first() {
        let proof = rtdb_logkeeper::certify_committed_range(&stream, first.lsn, terminal.replica_through).await?;
        rtdb_logkeeper::verify_certified_range(&proof, &entries)?;
    }
    Ok(reserved)
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
