use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_directory::{CheckpointWorldRetention, DeploymentRecord, Directory, HostedTask, now_ms};
use rtdb_node::{NativeSession, OwnedReadSession, OwnedWorldWrite, TransactionStatus};
use rtdb_types::{
    EntityId, Iid, Interval, OwnedRelation, OwnedWorld, PlacedAttemptScope, Result, StructFields,
    Timestamp, TransactionKey, Value, WorldShadow,
};
use uuid::Uuid;

use super::{
    CellStorage, CheckpointPoint, CheckpointReadResources, CheckpointReadWorld, KeyService,
    capture_receipt, read_checkpoint_owned_point, read_world::with_checkpoint_context,
    require_capture_open, same_lifetime,
};
use crate::admin::update_installation;

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
        || point.checkpoint.system_time != parent.basis.system_time.0
    {
        return Err(OwnedWorld::unavailable());
    }
    Ok(())
}

fn world_record(
    id: Uuid,
    retained: &CheckpointWorldRetention,
    schemas: rtdb_types::WorldSchemas,
) -> OwnedWorld {
    OwnedWorld {
        id: Iid(*id.as_bytes()),
        capture: Iid(*retained.capture.as_bytes()),
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
    parent: &CheckpointReadWorld,
    until: Timestamp,
    resources: CheckpointReadResources<'_>,
) -> Result<Uuid> {
    let owner = session
        .state()
        .user()
        .filter(|owner| !owner.is_empty() && owner.len() <= 512)
        .ok_or_else(OwnedWorld::unavailable)?
        .to_owned();
    let deployment = parent.deployment.to_string();
    let source = directory
        .deployment(&deployment)
        .await?
        .ok_or_else(OwnedWorld::unavailable)?;
    admit_session(session, &source, &owner)?;
    admit_capture(&source, archive, parent)?;
    if until <= now() || until <= parent.basis.system_time {
        return Err(OwnedWorld::unavailable());
    }
    let (schemas, _schema_charge) = with_checkpoint_context(
        directory,
        archive,
        keys,
        hosted_task,
        parent,
        resources,
        |context| rtdb_query::read_owned_parent_schemas(context, &owner),
    )
    .await?;

    let id = Uuid::new_v4();
    let retained = CheckpointWorldRetention {
        owner,
        capture: parent.capture,
        manifest_sha256: parent.manifest_sha256.clone(),
        schema_digest: schemas.digest(),
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
        admit_capture(record, archive, parent)?;
        let now = now();
        if until <= now {
            return Err(OwnedWorld::unavailable());
        }
        record
            .checkpoint_worlds
            .retain(|_, world| world.until > now);
        if record.checkpoint_worlds.len() >= MAX_RETAINED_WORLDS
            || record.checkpoint_worlds.contains_key(&id)
        {
            return Err(OwnedWorld::unavailable());
        }
        record.checkpoint_worlds.insert(id, retained.clone());
        Ok(())
    })
    .await?;
    admit_session(session, &installed, &retained.owner)?;
    let outcome = session
        .register_owned_world(&world_record(id, &retained, schemas))
        .await?;
    if !outcome.committed() {
        return Err(outcome
            .error()
            .cloned()
            .unwrap_or_else(OwnedWorld::unavailable));
    }
    Ok(id)
}

fn matches_retention(world: &OwnedWorld, retained: &CheckpointWorldRetention) -> bool {
    world.capture.0 == *retained.capture.as_bytes()
        && world.manifest == retained.manifest_sha256
        && world.basis == retained.basis
        && world.until == retained.until
        && world.schemas.digest() == retained.schema_digest
}

struct OpenWorld {
    retained: CheckpointWorldRetention,
    snapshot: OwnedReadSession,
    record: OwnedWorld,
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
    Ok(OpenWorld {
        retained: retained.clone(),
        snapshot,
        record: world,
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

    update_installation(directory, &deployment, |current| {
        if current.incarnation != installation.incarnation
            || current.log_epoch != installation.log_epoch
        {
            return Err(OwnedWorld::unavailable());
        }
        if let Some(current_pin) = current.checkpoint_worlds.get(&id) {
            if current_pin != pin {
                return Err(OwnedWorld::unavailable());
            }
            current.checkpoint_worlds.remove(&id);
        }
        Ok(())
    })
    .await?;
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
    let OpenWorld {
        snapshot,
        record,
        _schema_charge,
        ..
    } = open_world(directory, archive, session, deployment, id).await?;
    drop(snapshot);
    let outcome = session.commit_owned_world(&record, writes).await?;
    if !outcome.committed() {
        return Err(outcome
            .error()
            .cloned()
            .unwrap_or_else(OwnedWorld::unavailable));
    }
    Ok(outcome.tx_key())
}

pub struct WorldFact {
    row: StructFields<Value>,
    _reservation: Arc<MemoryReservation>,
}

impl WorldFact {
    pub fn row(&self) -> &StructFields<Value> {
        &self.row
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
            rtdb_query::owned_row_revision(batch, row, id, memory)?;
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
                _reservation: reservation,
            });
        }
    }
    Ok(found)
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
) -> Result<Option<WorldFact>> {
    let valid = Interval::at(valid_at)?;
    let OpenWorld {
        retained, snapshot, ..
    } = open_world(directory, archive, session, deployment, world).await?;
    let local = relation.in_world(Iid(*world.as_bytes()))?;
    let memory = resources.memory;
    if let Some(mut cursor) = snapshot.open_point_at(&local, id, Some(valid_at)).await? {
        let mut batches = Vec::new();
        while let Some(batch) = cursor.next_batch().await? {
            if batch.batch().num_rows() != 0 {
                batches.push(batch);
            }
        }
        if let Some(fact) = decode_point(batches, id, memory)? {
            snapshot.check()?;
            if retained.until <= now() {
                return Err(OwnedWorld::unavailable());
            }
            let reservation = memory.reserve(WorldShadow::decode_bound(&fact.row)?)?;
            return Ok(match WorldShadow::from_record(&fact.row)? {
                WorldShadow::Deleted => None,
                WorldShadow::Present(row) => Some(WorldFact {
                    row,
                    _reservation: reservation,
                }),
            });
        }
    }
    let parent = CheckpointReadWorld {
        deployment,
        capture: retained.capture,
        manifest_sha256: retained.manifest_sha256,
        basis: retained.basis,
    };
    let batches = read_checkpoint_owned_point(
        directory,
        archive,
        keys,
        hosted_task,
        &parent,
        CheckpointPoint {
            owner: &retained.owner,
            relation,
            id,
            valid: Some(valid),
        },
        resources,
    )
    .await?;
    snapshot.check()?;
    if retained.until <= now() {
        return Err(OwnedWorld::unavailable());
    }
    decode_point(batches.unwrap_or_default(), id, memory)
}
