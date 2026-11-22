use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_directory::{CheckpointWorldRetention, DeploymentRecord, Directory, HostedTask, now_ms};
use rtdb_node::{NativeSession, OwnedReadSession, OwnedWorldPointWitness, OwnedWorldWrite, TransactionStatus};
use rtdb_types::{
    EntityId, Iid, Interval, OwnedRelation, OwnedWorld, PlacedAttemptScope, Result, StructFields,
    Timestamp, TransactionKey, Value, WorldShadow,
};
use uuid::Uuid;

use super::{
    CellStorage, CheckpointPoint, CheckpointReadResources, CheckpointReadWorld, KeyService,
    capture_receipt, read_world::{CaptureSelection, read_checkpoint_point_in_context, with_checkpoint_context, with_registered_capture_context},
    replica_suffix, require_capture_open, same_lifetime,
};
use crate::admin::update_installation;

mod held;
pub(crate) use held::{HeldCheckpointWorld, WorldArchive, open_held_checkpoint_world};

const MAX_RETAINED_WORLDS: usize = 64;

fn now() -> Timestamp {
    Timestamp(now_ms().saturating_mul(1_000))
}

fn admit_session(session: &NativeSession, record: &DeploymentRecord, owner: &str) -> Result<()> {
    let expected = PlacedAttemptScope::new(&record.stream("replica"), record.epoch)
        .map_err(|_| OwnedWorld::unavailable())?;
    if !session.state().auto_commit()
        || session.state().status() != TransactionStatus::Idle
        || session.state().database() != record.name
        || session.state().user() != Some(owner)
        || record.writer_pool().is_none()
        || session.placed_history()? != Some(expected)
    {
        return Err(OwnedWorld::unavailable());
    }
    Ok(())
}

fn admit_capture(
    record: &DeploymentRecord,
    archive: &CellStorage,
    parent: &CheckpointReadWorld,
) -> Result<()> {
    require_capture_open(record, parent.capture)?;
    let (point, manifest) = capture_receipt(record, archive, parent.deployment, parent.capture)?
        .ok_or_else(OwnedWorld::unavailable)?;
    if !same_lifetime(record, &point)
        || manifest != parent.manifest_sha256
        || point.checkpoint.root_sha256.is_none()
    {
        return Err(OwnedWorld::unavailable());
    }
    match parent.suffix {
        Some(suffix) if replica_suffix::accepted(record, &point, &manifest, suffix)?.basis == parent.basis => Ok(()),
        None if point.checkpoint.system_time == parent.basis.system_time.0 => Ok(()),
        _ => Err(OwnedWorld::unavailable()),
    }
}

fn world_record(
    id: Uuid,
    retained: &CheckpointWorldRetention,
    schemas: rtdb_types::WorldSchemas,
) -> OwnedWorld {
    OwnedWorld {
        id: Iid(*id.as_bytes()),
        capture: Iid(*retained.capture.as_bytes()),
        suffix: retained.suffix.map(|id| Iid(*id.as_bytes())),
        manifest: retained.manifest_sha256.clone(),
        basis: retained.basis,
        until: retained.until,
        schemas,
        dropped: false,
    }
}

/// Forks an independently archived, exact parent cut. The directory pin
/// precedes the ordered assertion, so a partial fork can only retain media.
pub async fn fork_checkpoint_world(
    directory: &dyn Directory,
    archive: &CellStorage,
    keys: &dyn KeyService,
    hosted_task: Option<&HostedTask>,
    session: &mut NativeSession,
    deployment_id: Uuid,
    capture: Uuid,
    until: Timestamp,
    resources: CheckpointReadResources<'_>,
) -> Result<Uuid> {
    fork_checkpoint_world_at(
        directory, archive, keys, hosted_task, session, deployment_id, capture, None,
        until, resources,
    ).await
}

pub(crate) async fn fork_checkpoint_world_at(
    directory: &dyn Directory,
    archive: &CellStorage,
    keys: &dyn KeyService,
    hosted_task: Option<&HostedTask>,
    session: &mut NativeSession,
    deployment_id: Uuid,
    capture: Uuid,
    suffix: Option<Uuid>,
    until: Timestamp,
    resources: CheckpointReadResources<'_>,
) -> Result<Uuid> {
    let (world, _) = fork_checkpoint_world_selected(
        directory, archive, keys, hosted_task, session, deployment_id, capture, suffix, until, resources, None,
    ).await?;
    Ok(world)
}

pub(crate) async fn fork_checkpoint_world_selected(
    directory: &dyn Directory, archive: &CellStorage, keys: &dyn KeyService, hosted_task: Option<&HostedTask>,
    session: &mut NativeSession, deployment_id: Uuid, capture: Uuid, suffix: Option<Uuid>,
    until: Timestamp, resources: CheckpointReadResources<'_>, fork: Option<rtdb_node::OwnedWorldFork>,
) -> Result<(Uuid, TransactionKey)> {
    let owner = session
        .state()
        .user()
        .filter(|owner| !owner.is_empty() && owner.len() <= 512)
        .ok_or_else(OwnedWorld::unavailable)?
        .to_owned();
    let deployment = deployment_id.to_string();
    let source = directory
        .deployment(&deployment)
        .await?
        .ok_or_else(OwnedWorld::unavailable)?;
    admit_session(session, &source, &owner)?;
    require_capture_open(&source, capture)?;
    let (point, manifest) = capture_receipt(&source, archive, deployment_id, capture)?
        .ok_or_else(OwnedWorld::unavailable)?;
    if !same_lifetime(&source, &point) || until <= now() || until.0 <= point.checkpoint.system_time {
        return Err(OwnedWorld::unavailable());
    }
    let expected_basis = suffix.map(|suffix|
        replica_suffix::accepted(&source, &point, &manifest, suffix).map(|pin| pin.basis)
    ).transpose()?;
    if expected_basis.is_some_and(|basis| until <= basis.system_time) {
        return Err(OwnedWorld::unavailable());
    }
    let live = session.open_owned_read().await?;
    let ((schemas, _schema_charge), basis) = with_registered_capture_context(
        directory, archive, keys, hosted_task,
        CaptureSelection { deployment: deployment_id, capture, manifest: &manifest, suffix, expected_basis },
        resources,
        |context, basis| {
            let owner = &owner;
            async move { Ok((rtdb_query::read_owned_parent_schemas(context, owner).await?, basis)) }
        },
    ).await?;
    live.check()?;
    if basis.system_time >= live.present().system_time {
        return Err(OwnedWorld::unavailable());
    }
    drop(live);
    let parent = CheckpointReadWorld { deployment: deployment_id, capture, manifest_sha256: manifest, suffix, basis };

    if fork.as_ref().is_some_and(|fork| fork.basis != basis.system_time) {
        return Err(OwnedWorld::unavailable());
    }
    let id = fork.as_ref().map_or_else(Uuid::new_v4, |fork| Uuid::from_bytes(fork.world.0));
    if id.is_nil() { return Err(OwnedWorld::unavailable()); }
    let retained = CheckpointWorldRetention {
        owner,
        capture: parent.capture,
        manifest_sha256: parent.manifest_sha256.clone(),
        schema_digest: schemas.digest(),
        suffix: parent.suffix,
        basis: parent.basis,
        until,
    };
    let scope = session.placed_history()?;
    let database = session.state().database().to_owned();
    let installed = update_installation(directory, &deployment, |record| {
        let eligible = PlacedAttemptScope::new(&record.stream("replica"), record.epoch)
            .map_err(|_| OwnedWorld::unavailable())?;
        if record.name != database || scope != Some(eligible) {
            return Err(OwnedWorld::unavailable());
        }
        admit_capture(record, archive, &parent)?;
        let now = now();
        if until <= now || fork.as_ref().is_some_and(|fork| fork.deadline <= now || Some(fork.scope) != scope) {
            return Err(OwnedWorld::unavailable());
        }
        record
            .checkpoint_worlds
            .retain(|_, world| world.until > now);
        if let Some(existing) = record.checkpoint_worlds.get(&id) {
            return if fork.is_some() && existing == &retained { Ok(()) } else { Err(OwnedWorld::unavailable()) };
        }
        if record.checkpoint_worlds.len() >= MAX_RETAINED_WORLDS {
            return Err(OwnedWorld::unavailable());
        }
        record.checkpoint_worlds.insert(id, retained.clone());
        Ok(())
    })
    .await?;
    admit_session(session, &installed, &retained.owner)?;
    let registration = world_record(id, &retained, schemas);
    let checked = fork.is_some();
    let outcome = if let Some(fork) = fork {
        session.register_owned_world_checked(&registration, fork).await?
    } else {
        session.register_owned_world(&registration).await?
    };
    if !outcome.committed() {
        // A competing retry can already have published this checked world.
        // Keep its exact pin available for recovery until drop or expiry.
        if !checked {
            let _ = retire_world_pin(directory, &deployment, id, &retained, installed.incarnation, installed.log_epoch).await;
        }
        return Err(outcome
            .error()
            .cloned()
            .unwrap_or_else(OwnedWorld::unavailable));
    }
    Ok((id, outcome.tx_key()))
}

fn matches_retention(world: &OwnedWorld, retained: &CheckpointWorldRetention) -> bool {
    world.capture.0 == *retained.capture.as_bytes()
        && world.suffix.map(|suffix| Uuid::from_bytes(suffix.0)) == retained.suffix
        && world.manifest == retained.manifest_sha256
        && world.basis == retained.basis
        && world.until == retained.until
        && world.schemas.digest() == retained.schema_digest
}

struct OpenWorld {
    retained: CheckpointWorldRetention,
    incarnation: u64,
    log_epoch: i32,
    snapshot: OwnedReadSession,
    record: OwnedWorld,
    clock: Option<Timestamp>,
    _schema_charge: Arc<MemoryReservation>,
}

async fn open_world(
    directory: &dyn Directory,
    archive: &CellStorage,
    session: &mut NativeSession,
    deployment: Uuid,
    id: Uuid,
) -> Result<OpenWorld> {
    let installation = directory
        .deployment(&deployment.to_string())
        .await?
        .ok_or_else(OwnedWorld::unavailable)?;
    let retained = installation
        .checkpoint_worlds
        .get(&id)
        .ok_or_else(OwnedWorld::unavailable)?;
    admit_session(session, &installation, &retained.owner)?;
    if retained.until <= now() {
        return Err(OwnedWorld::unavailable());
    }
    let parent = CheckpointReadWorld {
        deployment,
        capture: retained.capture,
        manifest_sha256: retained.manifest_sha256.clone(),
        suffix: retained.suffix,
        basis: retained.basis,
    };
    admit_capture(&installation, archive, &parent)?;
    let snapshot = session.open_owned_read().await?;
    let (world, charge) = snapshot
        .world_registration(Iid(*id.as_bytes()))
        .await?
        .ok_or_else(OwnedWorld::unavailable)?;
    if world.dropped
        || snapshot.present().system_time >= retained.until
        || !matches_retention(&world, retained)
    {
        return Err(OwnedWorld::unavailable());
    }
    let clock = snapshot.world_application_clock(world.id).await?;
    if clock.is_some_and(|clock| clock <= retained.basis.system_time || clock > snapshot.present().system_time) {
        return Err(OwnedWorld::unavailable());
    }
    Ok(OpenWorld {
        retained: retained.clone(),
        incarnation: installation.incarnation,
        log_epoch: installation.log_epoch,
        snapshot,
        record: world,
        clock,
        _schema_charge: charge,
    })
}

/// Orders a terminal world marker before allowing archive retirement.
pub async fn drop_checkpoint_world(
    directory: &dyn Directory,
    session: &mut NativeSession,
    deployment: Uuid,
    id: Uuid,
) -> Result<()> {
    let deployment = deployment.to_string();
    let owner = session
        .state()
        .user()
        .filter(|owner| !owner.is_empty())
        .ok_or_else(OwnedWorld::unavailable)?;
    let installation = directory
        .deployment(&deployment)
        .await?
        .ok_or_else(OwnedWorld::unavailable)?;
    admit_session(session, &installation, owner)?;
    let retained = installation.checkpoint_worlds.get(&id);
    if retained.is_some_and(|retained| retained.owner != owner) {
        return Err(OwnedWorld::unavailable());
    }
    let snapshot = session.open_owned_read().await?;
    let (world, _retained) = snapshot
        .world_registration(Iid(*id.as_bytes()))
        .await?
        .ok_or_else(OwnedWorld::unavailable)?;
    drop(snapshot);

    let Some(pin) = retained else {
        return if world.dropped {
            Ok(())
        } else {
            Err(OwnedWorld::unavailable())
        };
    };
    if !matches_retention(&world, pin) {
        return Err(OwnedWorld::unavailable());
    }
    if !world.dropped {
        let outcome = session.drop_owned_world(world).await?;
        if !outcome.committed() {
            return Err(outcome
                .error()
                .cloned()
                .unwrap_or_else(OwnedWorld::unavailable));
        }
    }

    retire_world_pin(directory, &deployment, id, pin, installation.incarnation, installation.log_epoch).await
}

async fn retire_world_pin(
    directory: &dyn Directory, deployment: &str, id: Uuid, pin: &CheckpointWorldRetention,
    incarnation: u64, log_epoch: i32,
) -> Result<()> {
    update_installation(directory, deployment, |current| {
        if current.incarnation != incarnation || current.log_epoch != log_epoch {
            return Err(OwnedWorld::unavailable());
        }
        if let Some(current_pin) = current.checkpoint_worlds.get(&id) {
            if current_pin != pin {
                return Err(OwnedWorld::unavailable());
            }
            current.checkpoint_worlds.remove(&id);
        }
        Ok(())
    }).await?;
    Ok(())
}

/// Applies complete local replacements and tombstones in one ordered write.
pub async fn commit_checkpoint_world(
    directory: &dyn Directory,
    archive: &CellStorage,
    session: &mut NativeSession,
    deployment: Uuid,
    id: Uuid,
    writes: Vec<OwnedWorldWrite>,
) -> Result<TransactionKey> {
    commit_checkpoint_world_checked(directory, archive, session, deployment, id, writes, Vec::new(), Vec::new(), None, None).await
}

pub(crate) async fn commit_checkpoint_world_checked(
    directory: &dyn Directory,
    archive: &CellStorage,
    session: &mut NativeSession,
    deployment: Uuid,
    id: Uuid,
    writes: Vec<OwnedWorldWrite>,
    checks: Vec<OwnedWorldPointWitness>,
    controls: Vec<rtdb_node::HostedControlWrite>,
    deadline: Option<Timestamp>,
    publication: Option<rtdb_node::OwnedWorldPublication>,
) -> Result<TransactionKey> {
    let OpenWorld {
        snapshot,
        record,
        _schema_charge,
        retained,
        incarnation,
        log_epoch,
        ..
    } = open_world(directory, archive, session, deployment, id).await?;
    drop(snapshot);
    let dropping = publication.as_ref().is_some_and(|publication| publication.drop_world);
    let outcome = session.commit_owned_world_checked(&record, writes, checks, controls, deadline, publication).await?;
    if !outcome.committed() {
        return Err(outcome
            .error()
            .cloned()
            .unwrap_or_else(OwnedWorld::unavailable));
    }
    if dropping {
        // The ordered terminal marker is authoritative. A failed retirement leaves
        // the exact pin retained until expiry or a retry via the archive drop owner.
        let _ = retire_world_pin(directory, &deployment.to_string(), id, &retained, incarnation, log_epoch).await;
    }
    Ok(outcome.tx_key())
}

pub(crate) async fn commit_checkpoint_world_batch(
    directory: &dyn Directory, archive: &CellStorage, session: &mut NativeSession, deployment: Uuid,
    sections: Vec<rtdb_node::OwnedWorldSection>, controls: Vec<rtdb_node::HostedControlWrite>, deadline: Timestamp,
) -> Result<TransactionKey> {
    if !(2..=8).contains(&sections.len()) { return Err(OwnedWorld::unavailable()); }
    let installation = directory.deployment(&deployment.to_string()).await?.ok_or_else(OwnedWorld::unavailable)?;
    let mut admitted = Vec::with_capacity(sections.len());
    for section in sections {
        let id = Uuid::from_bytes(section.world.0);
        let retained = installation.checkpoint_worlds.get(&id).ok_or_else(OwnedWorld::unavailable)?;
        admit_session(session, &installation, &retained.owner)?;
        if retained.basis != section.parent_basis || now() >= retained.until { return Err(OwnedWorld::unavailable()); }
        admit_capture(&installation, archive, &CheckpointReadWorld {
            deployment, capture: retained.capture, manifest_sha256: retained.manifest_sha256.clone(),
            suffix: retained.suffix, basis: retained.basis,
        })?;
        admitted.push((retained.until, section));
    }
    let outcome = session.commit_owned_world_batch(admitted, controls, deadline).await?;
    if !outcome.committed() {
        return Err(outcome.error().cloned().unwrap_or_else(OwnedWorld::unavailable));
    }
    Ok(outcome.tx_key())
}

pub struct WorldPoint {
    pub present: TransactionKey,
    pub fact: Option<WorldFact>,
    pub(crate) shadowed: bool,
    pub(crate) witness: Option<OwnedWorldPointWitness>,
}

pub struct WorldFact {
    row: StructFields<Value>,
    revision: Timestamp,
    _reservation: Arc<MemoryReservation>,
}

impl WorldFact {
    pub fn row(&self) -> &StructFields<Value> {
        &self.row
    }

    pub(crate) fn into_parts(self) -> (StructFields<Value>, Timestamp, Arc<MemoryReservation>) {
        (self.row, self.revision, self._reservation)
    }
}

fn decode_point(
    batches: Vec<OwnedBatch>,
    id: &EntityId,
    memory: &Arc<MemoryBudget>,
) -> Result<Option<WorldFact>> {
    let mut found = None;
    for batch in batches {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            if found.is_some() {
                return Err(OwnedWorld::unavailable());
            }
            let revision = rtdb_query::owned_row_revision(batch, row, id, memory)?;
            let columns = || {
                batch
                    .schema_ref()
                    .fields()
                    .iter()
                    .zip(batch.columns())
                    .filter(|(field, _)| field.name() != "_system_from")
            };
            let charge = columns().try_fold(4096usize, |bound, (field, column)| {
                bound
                    .checked_add(field.name().len() + 256)
                    .and_then(|bound| {
                        bound.checked_add(
                            rtdb_columnar::estimate_read_value_bytes(field, column.as_ref(), row)
                                .ok()?,
                        )
                    })
                    .ok_or_else(OwnedWorld::unavailable)
            })?;
            let reservation = memory.reserve(charge)?;
            let fields = columns()
                .map(|(field, column)| {
                    Ok((
                        field.name().clone(),
                        rtdb_columnar::read_value(field, column.as_ref(), row)?,
                    ))
                })
                .collect::<Result<_>>()?;
            found = Some(WorldFact {
                row: fields,
                revision,
                _reservation: reservation,
            });
        }
    }
    Ok(found)
}

impl OpenWorld {
    fn check(&self) -> Result<()> {
        self.snapshot.check()?;
        if self.retained.until <= now() { return Err(OwnedWorld::unavailable()); }
        Ok(())
    }

    fn parent(&self, deployment: Uuid) -> CheckpointReadWorld {
        CheckpointReadWorld {
            deployment,
            capture: self.retained.capture,
            manifest_sha256: self.retained.manifest_sha256.clone(),
            suffix: self.retained.suffix,
            basis: self.retained.basis,
        }
    }

    /// Outer absence allows archive fallback. A local tombstone answers absence.
    async fn read_local(
        &self, relation: &OwnedRelation, id: &EntityId, valid_at: Timestamp, memory: &Arc<MemoryBudget>,
        observed: bool,
    ) -> Result<(Option<Option<WorldFact>>, Option<OwnedWorldPointWitness>)> {
        let read = self.snapshot.read_world_shadow(self.record.id, relation, id, valid_at, observed).await?;
        let Some(fact) = decode_point(read.row.into_iter().collect(), id, memory)? else {
            return Ok((None, read.witness));
        };
        let reservation = memory.reserve(WorldShadow::decode_bound(&fact.row)?)?;
        let visible = match WorldShadow::from_record(&fact.row)? {
            WorldShadow::Deleted => None,
            WorldShadow::Present(row) => Some(WorldFact { row, revision: fact.revision, _reservation: reservation }),
        };
        Ok((Some(visible), read.witness))
    }
}

/// A local tombstone suppresses the pinned parent's row for this full identity.
pub async fn read_checkpoint_world_point(
    directory: &dyn Directory,
    archive: &CellStorage,
    keys: &dyn KeyService,
    hosted_task: Option<&HostedTask>,
    session: &mut NativeSession,
    deployment: Uuid,
    world: Uuid,
    relation: &OwnedRelation,
    id: &EntityId,
    valid_at: Timestamp,
    resources: CheckpointReadResources<'_>,
) -> Result<WorldPoint> {
    let valid = Interval::at(valid_at)?;
    let open = open_world(directory, archive, session, deployment, world).await?;
    let present = open.snapshot.present();
    let memory = resources.memory;
    let (local, _) = open.read_local(relation, id, valid_at, memory, false).await?;
    if let Some(fact) = local {
        open.check()?;
        open.snapshot.check_relation(relation)?;
        return Ok(WorldPoint { present, fact, shadowed: true, witness: None });
    }
    open.snapshot.check_point_purge_floor(relation, id, open.retained.basis.system_time).await?;
    let guard = open.snapshot.relation_release_guard([relation])?;
    let batches = with_checkpoint_context(
        directory, archive, keys, hosted_task, &open.parent(deployment), resources,
        |context| read_checkpoint_point_in_context(
            context.with_owned_read_guard(guard),
            CheckpointPoint { owner: &open.retained.owner, relation, id, valid: Some(valid) },
        ),
    ).await?;
    let fact = decode_point(batches.unwrap_or_default(), id, memory)?;
    open.check()?;
    open.snapshot.check_relation(relation)?;
    Ok(WorldPoint { present, fact, shadowed: false, witness: None })
}
