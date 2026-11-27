use std::sync::Arc;

mod application;
mod layout;
mod outcomes;
mod points;
mod projection;
mod receipt_policy;
pub(crate) use application::{ApplicationEvidence, ApplicationReadback};
pub(crate) use layout::ApplicationLayout;
pub(crate) use outcomes::{ReceiptHistory, ReceiptLookup, ReceiptLookups, ReceiptReadback};
pub(crate) use points::{PointHistory, PointHistoryRead, PointSelector};
pub(crate) use projection::{ClassicPrepared, ClassicProjection};
pub(crate) use receipt_policy::{ReceiptPolicyReadback, ReceiptPolicySelection};
use std::time::Duration;

use ring::digest::{Context, SHA256};
use rtdb_columnar::MemoryBudget;
use rtdb_directory::{
    CheckpointTransferBootstrap, DeploymentRecord, Directory, HostedTask, Lifecycle,
};
use rtdb_format::{catalog::CheckpointContents, data::{EncodedEvent, EventBatch}};
use rtdb_node::ReadFence;
use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryContext};
use rtdb_storage::cache::BufferPool;
use rtdb_types::{EventOp, Result, StorageEpoch, TableRef};
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::owned_inventory::{observe_transfer_payload, open_owned_snapshot, transfer_first};
use super::{StageSelector, inconsistent, unavailable, with_stage};
use crate::{
    archive::{
        archive_lifetime_floors_digest,
        replica_artifact::{ReplicaArtifact, require_chain},
        replica_suffix,
    },
    deployment::CellStorage,
    keys::KeyService,
};

#[derive(Serialize)]
pub(crate) struct ClassicMigrationInventory {
    pub(crate) bootstrap: CheckpointTransferBootstrap,
    relations: Vec<RelationInventory>,
    retained: RetainedInventory,
}

#[derive(Clone, Serialize)]
struct RelationInventory {
    table: TableRef,
    kind: RelationKind,
    events: u64,
    unpositioned: u64,
    erasures: u64,
    redacted: u64,
    sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RelationKind {
    Literal,
    ClassicOutcome,
    Owned,
    System,
}

impl RelationKind {
    fn of(table: &TableRef) -> Self {
        if table.stores_expiring_transaction_metadata()
            || table.schema == "xt" && table.table == "txs"
        {
            Self::ClassicOutcome
        } else if TableRef::owned_schema(&table.schema) {
            Self::Owned
        } else if matches!(
            table.schema.as_str(),
            "xt" | "information_schema" | "pg_catalog"
        ) {
            Self::System
        } else {
            Self::Literal
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize)]
struct RetainedInventory {
    worlds: usize,
    suffix_pins: usize,
    other_captures: usize,
    pending_capture_releases: usize,
    sha256: [u8; 32],
}

impl RetainedInventory {
    fn of(source: &DeploymentRecord, capture: Uuid) -> Result<Self> {
        let inventory = crate::archive::encode(&(
            &source.checkpoint_worlds,
            &source.checkpoint_suffix_pins,
            &source.checkpoint_captures,
            &source.checkpoint_capture_retirements,
        ))?;
        Ok(Self {
            worlds: source.checkpoint_worlds.len(),
            suffix_pins: source.checkpoint_suffix_pins.len(),
            other_captures: source
                .checkpoint_captures
                .keys()
                .filter(|id| **id != capture)
                .count(),
            pending_capture_releases: source
                .checkpoint_capture_retirements
                .values()
                .filter(|release| release.observed_empty_at_ms.is_none())
                .count(),
            sha256: crate::archive::sha256(&inventory),
        })
    }
}

pub(crate) async fn inspect_classic_migration_inventory(
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    keys: &dyn KeyService,
    selector: StageSelector,
    destination: Uuid,
    ciphertext_sha256: [u8; 32],
    hosted_task: Option<HostedTask>,
) -> Result<ClassicMigrationInventory> {
    Ok(Box::pin(read_classic_migration(
        directory, storage, keys, selector, destination, ciphertext_sha256, hosted_task, ClassicRead::Inventory,
    )).await?.0)
}

pub(crate) enum ClassicRead {
    Inventory,
    Projection([u8; 32]),
    Receipt(ReceiptLookups),
    Application {
        request: [u8; 32],
        begin: [u8; 32],
        cut: rtdb_types::TransactionKey,
        points: Option<PointHistoryRead>,
        receipt_policy: Option<ReceiptPolicySelection>,
        provider_release: Option<crate::recovery::classic_provider::ReleaseSelection>,
        layout: Arc<ApplicationLayout>,
    },
}

pub(crate) async fn read_classic_migration(
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    keys: &dyn KeyService,
    selector: StageSelector,
    destination: Uuid,
    ciphertext_sha256: [u8; 32],
    hosted_task: Option<HostedTask>,
    read: ClassicRead,
) -> Result<(ClassicMigrationInventory, Option<ClassicPrepared>, Option<ReceiptReadback>)> {
    if selector.source.is_none() || !selector.transfer_only {
        return Err(inconsistent(
            "Classic migration replay requires an authenticated transfer stage",
        ));
    }
    let source_directory = Arc::clone(&directory);
    let check_retention = !matches!(&read, ClassicRead::Application { points: Some(_), .. });
    with_stage(
        directory,
        storage,
        keys,
        selector,
        hosted_task,
        |authority, scratch, verified| async move {
            let cancellation = CancellationToken::new();
            let stop = &cancellation;
            let result = tokio::time::timeout(Duration::from_secs(60), async move {
                verified.require_serving_envelope()?;
                if verified.contents != CheckpointContents::Replay
                    || authority
                        .stage
                        .intent
                        .classic_migration
                        .is_none_or(|migration| migration.destination != destination)
                {
                    return Err(inconsistent(
                        "The held stage does not cover this Classic migration destination",
                    ));
                }
                let bootstrap = scratch
                    .checkpoint_transfer_bootstrap
                    .as_deref()
                    .filter(|bootstrap| {
                        bootstrap.ciphertext_sha256 == ciphertext_sha256
                            && bootstrap.terminal.fence.destination == authority.stage
                            && scratch.checkpoint_transfer_fence.as_deref()
                                == Some(&bootstrap.terminal.fence)
                    })
                    .ok_or_else(|| {
                        inconsistent("The scratch stage has another ordered replay receipt")
                    })?;
                let terminal = &bootstrap.terminal;
                let capture = authority.stage.intent.capture;
                let source = read_source(source_directory.as_ref(), bootstrap).await?;
                let retained = RetainedInventory::of(&source, capture)?;

                let bucket = storage.bucket(Arc::clone(&verified.region)).await?;
                let artifact = ReplicaArtifact::open_transfer(
                    bucket.as_ref(),
                    keys,
                    &scratch,
                    terminal,
                    verified.region,
                )
                .await?;
                let (mut recovered, _, pool, memory, options) =
                    open_owned_snapshot(&authority, verified.store).await?;
                let expected_chain = artifact.read_ordered_transfer(ciphertext_sha256).await?;
                let mut chain = artifact.chain();
                let mut replay =
                    replica_suffix::TerminalReplayProgress::new(&terminal.fence, &recovered)?;
                let mut serving_suffix = false;
                let mut next = transfer_first(&authority.stage)?;
                while next <= terminal.replica_through {
                    let _segment = memory.reserve(replica_suffix::MAX_SEGMENT_BYTES * 4)?;
                    let entries = artifact
                        .read_segment(next, terminal.replica_through, &mut chain)
                        .await?;
                    for entry in &entries {
                        let Some(envelope) =
                            replay.admit(entry, &terminal.fence, &mut recovered)?
                        else {
                            continue;
                        };
                        serving_suffix |= matches!(&envelope.message,
                            rtdb_format::envelope::ReplicaBody::NoOp(row) if row.serving_record.is_some());
                        if let Some(payload) = observe_transfer_payload(
                            &artifact, entry.lsn, &envelope, &mut chain, &memory,
                        )
                        .await?
                        {
                            serving_suffix |= matches!(&payload.resolved.message,
                                rtdb_format::envelope::ReplicaBody::NoOp(row) if row.serving_record.is_some());
                            let _body = payload.body;
                            replay.apply(payload.resolved, &mut recovered, &memory)?;
                        } else {
                            replay.apply(envelope, &mut recovered, &memory)?;
                        }
                    }
                    next = entries
                        .last()
                        .and_then(|entry| entry.lsn.checked_add(1))
                        .ok_or_else(|| {
                            inconsistent("Classic migration replay has no next position")
                        })?;
                }
                require_chain(chain, expected_chain)?;
                let frontier = replay.finish(terminal.replica_through)?;
                let snapshot = recovered
                    .partition
                    .snapshot()
                    .with_system_time_cap(Some(terminal.basis.system_time))?;
                if snapshot.tx_basis() != Some(terminal.basis)
                    || recovered.progress.tx_key != Some(terminal.basis)
                    || frontier.source_through != terminal.source_through
                    || frontier.placed_debt != terminal.placed_debt
                    || recovered
                        .progress
                        .source_position
                        .after_offset(StorageEpoch(authority.stage.source_log_epoch))
                        != terminal.source_through
                {
                    return Err(inconsistent(
                        "The retained replay ends at a different Classic transaction",
                    ));
                }
                let name = &authority.stage.intent.name;
                let _catalog_charge = memory.reserve(snapshot.schema_allocation_bound(name)?)?;
                let catalog = CatalogSnapshot::new(
                    name,
                    [(
                        name.clone(),
                        DatabaseSnapshot {
                            partitions: vec![snapshot.clone()],
                            storage: Arc::clone(&pool),
                        },
                    )]
                    .into(),
                    true,
                )?;
                let context = QueryContext::new(
                    catalog,
                    Arc::clone(&memory),
                    terminal.basis.system_time,
                    "UTC",
                    options.query,
                    stop,
                )?;
                if archive_lifetime_floors_digest(context, terminal.basis).await?
                    != terminal.lifetime_floors_sha256
                {
                    return Err(inconsistent(
                        "The retained replay has another lifetime-floor inventory",
                    ));
                }
                let tables = snapshot.tables();
                if tables.len() > 4096 {
                    return Err(unavailable(
                        "Classic migration has too many relations to return a complete inventory",
                    ));
                }
                let mut relations = Vec::with_capacity(tables.len());
                let (mut projection, mut receipt) = match read {
                    ClassicRead::Inventory => (None, None),
                    ClassicRead::Projection(request) => (Some(projection::Projection::new(destination, request, terminal.basis, Arc::clone(&memory))), None),
                    ClassicRead::Application { request, begin, cut, points, receipt_policy, provider_release, layout } if !serving_suffix => (Some(
                        projection::Projection::new(destination, request, terminal.basis, Arc::clone(&memory))
                            .with_application(application::Application::new(layout, begin, cut, destination)
                                .with_points(points).with_receipt_policy(receipt_policy).with_provider_release(provider_release)),
                    ), None),
                    ClassicRead::Application { .. } => return Err(unavailable("Application closure lookup requires the complete canonical suffix replay")),
                    ClassicRead::Receipt(lookup) if !serving_suffix => (None, Some(lookup)),
                    ClassicRead::Receipt(_) => return Err(unavailable("Original receipt lookup requires the complete canonical suffix replay")),
                };
                for table in tables {
                    let kind = RelationKind::of(&table);
                    let rows = projection.as_mut().filter(|_| kind == RelationKind::Literal || is_transactions(&table));
                    let outcome = receipt.as_mut().filter(|lookups| lookups.covers(&table));
                    let relation = inspect_relation(&snapshot, table, &pool, &memory, stop, rows, outcome).await?;
                    if let Some(lookup) = &mut receipt { lookup.observe_relation(&relation); }
                    relations.push(relation);
                }
                let prepared = projection.map(|projection| projection.finish(
                    &snapshot, &relations, &retained, &recovered, bootstrap, serving_suffix,
                )).transpose()?;
                authority.confirm(name).await?;
                let confirmed = read_source(source_directory.as_ref(), bootstrap).await?;
                if check_retention && RetainedInventory::of(&confirmed, capture)? != retained
                {
                    return Err(unavailable(
                        "Classic migration retention changed during inspection",
                    ));
                }
                Ok((ClassicMigrationInventory {
                    bootstrap: bootstrap.clone(),
                    relations,
                    retained,
                }, prepared, receipt.map(ReceiptLookups::finish)))
            })
            .await;
            cancellation.cancel();
            result.map_err(|_| unavailable("Classic migration replay inspection timed out"))?
        },
    )
    .await
}

async fn read_source(
    directory: &dyn Directory,
    bootstrap: &CheckpointTransferBootstrap,
) -> Result<DeploymentRecord> {
    let terminal = &bootstrap.terminal;
    let stage = &terminal.fence.destination;
    directory
        .deployment(&stage.intent.source.to_string())
        .await?
        .filter(|record| {
            record.lifecycle == Lifecycle::Restoring
                && record.holder.is_none()
                && record.epoch == terminal.fence.fenced_writer_epoch
                && record.incarnation == stage.source_incarnation
                && record.log_epoch == stage.source_log_epoch
                && record.checkpoint == Some(stage.checkpoint)
                && record.checkpoint_transfer_terminal.as_deref() == Some(terminal)
                && record.checkpoint_transfer_fence.as_deref() == Some(&terminal.fence)
        })
        .ok_or_else(|| inconsistent("The exact Classic migration source terminal is unavailable"))
}

fn is_transactions(table: &TableRef) -> bool {
    table.schema == "xt" && table.table == "txs"
}

async fn inspect_relation(
    snapshot: &rtdb_index::Snapshot,
    table: TableRef,
    pool: &Arc<BufferPool>,
    memory: &Arc<MemoryBudget>,
    stop: &CancellationToken,
    mut projection: Option<&mut projection::Projection>,
    mut receipt: Option<&mut ReceiptLookups>,
) -> Result<RelationInventory> {
    let mut relation = RelationInventory {
        kind: RelationKind::of(&table),
        table,
        events: 0,
        unpositioned: 0,
        erasures: 0,
        redacted: 0,
        sha256: [0; 32],
    };
    let mut digest = Context::new(&SHA256);
    digest.update(b"rtdb.multi/classic-migration-relation/v1\0");
    let name = relation.table.schema_and_table();
    digest.update(&(name.len() as u64).to_be_bytes());
    digest.update(name.as_bytes());
    let declarations = crate::archive::encode(&snapshot.declared_columns(&relation.table))?;
    digest.update(&(declarations.len() as u64).to_be_bytes());
    digest.update(&declarations);
    let mut visit = |batch: &EventBatch| -> Result<()> {
        for row in 0..batch.len() {
            if stop.is_cancelled() {
                return Err(unavailable("Classic migration inspection was cancelled"));
            }
            let borrowed = batch.borrow_event(row)?;
            let bound = borrowed.document_allocation_bound()?.checked_add(512)
                .ok_or_else(|| inconsistent("A Classic migration row exceeds the memory bound"))?;
            let copies = if projection.is_some() || receipt.is_some() { 2 } else { 1 };
            let charge = memory.reserve(bound.checked_mul(copies)
                .ok_or_else(|| inconsistent("A Classic migration row exceeds the memory bound"))?)?;
            let header = borrowed.header();
            relation.events += 1;
            relation.unpositioned += header.position.is_none() as u64;
            relation.erasures += (header.op == EventOp::Erase) as u64;
            relation.redacted += (header.op == EventOp::Redacted) as u64;
            let outcome = receipt.as_deref_mut().and_then(|lookups| {
                lookups.select(&relation.table, header.iid)
            });
            let fingerprint = if projection.is_some() || outcome.is_some() {
                let event = batch.read_event(row)?;
                let fingerprint = event.retained_history_fingerprint()?;
                if let Some(projection) = projection.as_deref_mut() {
                    projection.observe(&relation.table, event, fingerprint, charge)?;
                } else if let Some(outcome) = outcome {
                    outcome.observe(event, fingerprint, charge)?;
                }
                fingerprint
            } else if let Some(fingerprint) = borrowed.history_fingerprint_without_document() {
                fingerprint?
            } else {
                batch.read_event(row)?.history_fingerprint()?
            };
            digest.update(&fingerprint);
        }
        Ok(())
    };
    let table = relation.table.clone();
    let tries = snapshot.trie_entries(&table);
    if !tries.is_empty() {
        let entry = snapshot
            .table_entry(&table)
            .ok_or_else(|| inconsistent("A Classic relation has no trie address"))?;
        for trie in tries {
            rtdb_query::visit_full_trie_events(
                pool,
                memory,
                stop,
                &entry.slug,
                &trie.key,
                &mut visit,
            )
            .await?;
        }
    }
    for segment in snapshot.iter_segments(&table) {
        for batch in segment.batches() {
            visit(batch.view())?;
        }
    }
    relation.sha256.copy_from_slice(digest.finish().as_ref());
    Ok(relation)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classic_attempt_and_owned_tables_are_not_literal_imports() {
        assert_eq!(
            RelationKind::of(&TableRef::new("public", "facts")),
            RelationKind::Literal
        );
        assert_eq!(
            RelationKind::of(&TableRef::new("xt", "txs")),
            RelationKind::ClassicOutcome
        );
        assert_eq!(
            RelationKind::of(&TableRef::new("xt", "placed_attempt_outcomes")),
            RelationKind::ClassicOutcome
        );
        assert_eq!(
            RelationKind::of(&TableRef::new("rtdb_owned_v1", "facts")),
            RelationKind::Owned
        );
        assert_eq!(
            RelationKind::of(&TableRef::new("xt", "users")),
            RelationKind::System
        );
    }
}
