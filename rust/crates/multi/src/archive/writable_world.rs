use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_directory::{CheckpointWorldRetention, DeploymentRecord, Directory, HostedTask, now_ms};
use rtdb_node::{NativeSession, OwnedReadSession, OwnedWorldPointWitness, TransactionStatus};
use rtdb_types::{
    EntityId, Iid, Interval, OwnedRelation, OwnedWorld, PlacedAttemptScope, Result, StructFields,
    Timestamp, TransactionKey, Value, WorldClockMode, WorldShadow,
};
use uuid::Uuid;

use super::{
    CellStorage, CheckpointReadResources, CheckpointReadWorld, KeyService,
    capture_receipt, read_world::{CaptureSelection, with_registered_capture_context},
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
        parent_publication: retained.parent_publication,
        mode: retained.mode,
        until: retained.until,
        schemas,
        dropped: false,
    }
}

/// Forks an independently archived, exact parent cut. The directory pin
/// precedes the ordered assertion, so a partial fork can only retain media.
pub(crate) async fn fork_checkpoint_world_selected(
    directory: &dyn Directory, archive: &CellStorage, keys: &dyn KeyService, hosted_task: Option<&HostedTask>,
    session: &mut NativeSession, deployment_id: Uuid, capture: Uuid, suffix: Option<Uuid>,
    until: Timestamp, resources: CheckpointReadResources<'_>, fork: rtdb_node::OwnedWorldFork,
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
    let ((schemas, _schema_charge), parent_publication, basis) = with_registered_capture_context(
        directory, archive, keys, hosted_task,
        CaptureSelection { deployment: deployment_id, capture, manifest: &manifest, suffix, expected_basis },
        resources,
        |context, basis| {
            let owner = &owner;
            async move {
                let publication = rtdb_query::read_owned_root_publication(Arc::clone(&context), owner).await?;
                let schemas = rtdb_query::read_owned_parent_schemas(context, owner).await?;
                Ok((schemas, publication, basis))
            }
        },
    ).await?;
    live.check()?;
    if basis.system_time >= live.present().system_time {
        return Err(OwnedWorld::unavailable());
    }
    drop(live);
    let parent = CheckpointReadWorld { deployment: deployment_id, capture, manifest_sha256: manifest, suffix, basis };

    if fork.cut.basis() != basis.system_time || fork.cut.live().is_some_and(|original| original != basis)
        || parent_publication.is_some_and(|publication| publication > basis.system_time)
    {
        return Err(OwnedWorld::unavailable());
    }
    let inherited = parent_publication.unwrap_or(basis.system_time);
    let mode = match fork.requested {
        rtdb_node::WorldClockRequest::Retained => WorldClockMode::Retained,
        rtdb_node::WorldClockRequest::Held { start } => {
            let start = start.unwrap_or(inherited);
            if start < inherited || start == Timestamp::MAX || (start > inherited && fork.cut.live().is_none()) {
                return Err(OwnedWorld::unavailable());
            }
            WorldClockMode::Held { start }
        }
    };
    let id = Uuid::from_bytes(fork.world.0);
    if id.is_nil() { return Err(OwnedWorld::unavailable()); }
    let retained = CheckpointWorldRetention {
        owner,
        capture: parent.capture,
        manifest_sha256: parent.manifest_sha256.clone(),
        schema_digest: schemas.digest(),
        suffix: parent.suffix,
        basis: parent.basis,
        parent_publication,
        mode,
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
        if until <= now || fork.deadline.min(fork.cut.until()) <= now || Some(fork.cut.scope()) != scope {
            return Err(OwnedWorld::unavailable());
        }
        record
            .checkpoint_worlds
            .retain(|_, world| world.until > now);
        if let Some(existing) = record.checkpoint_worlds.get(&id) {
            return if existing == &retained { Ok(()) } else { Err(OwnedWorld::unavailable()) };
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
    let outcome = session.register_owned_world_checked(&registration, fork).await?;
    if !outcome.committed() {
        // A competing retry may already have registered this world.
        // Its exact pending pin remains eligible for recovery until expiry.
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
        && world.parent_publication == retained.parent_publication
        && world.mode == retained.mode
        && world.until == retained.until
        && world.schemas.digest() == retained.schema_digest
}

struct OpenWorld {
    retained: CheckpointWorldRetention,
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
    if clock.is_some_and(|clock| !world.is_held() || clock <= world.initial_clock() || clock > snapshot.present().system_time) {
        return Err(OwnedWorld::unavailable());
    }
    Ok(OpenWorld {
        retained: retained.clone(),
        snapshot,
        record: world,
        clock,
        _schema_charge: charge,
    })
}

async fn retire_world_pins(
    directory: &dyn Directory, deployment: &str,
    selected: &[(Uuid, &CheckpointWorldRetention)], incarnation: u64, log_epoch: i32,
) -> Result<()> {
    update_installation(directory, deployment, |current| {
        if current.incarnation != incarnation || current.log_epoch != log_epoch {
            return Err(OwnedWorld::unavailable());
        }
        for (id, expected) in selected {
            if current.checkpoint_worlds.get(id).is_some_and(|pin| pin != *expected) {
                return Err(OwnedWorld::unavailable());
            }
            current.checkpoint_worlds.remove(id);
        }
        Ok(())
    }).await?;
    Ok(())
}

pub(crate) async fn validate_checkpoint_world(
    directory: &dyn Directory, archive: &CellStorage, session: &mut NativeSession,
    deployment: Uuid, id: Uuid, checks: Vec<OwnedWorldPointWitness>,
    controls: Vec<rtdb_node::HostedControlWrite>, deadline: Timestamp,
) -> Result<TransactionKey> {
    let OpenWorld { snapshot, record, _schema_charge, .. } = open_world(directory, archive, session, deployment, id).await?;
    drop(snapshot);
    let outcome = session.validate_owned_world(&record, checks, controls, deadline).await?;
    if !outcome.committed() {
        return Err(outcome.error().cloned().unwrap_or_else(OwnedWorld::unavailable));
    }
    Ok(outcome.tx_key())
}

pub(crate) async fn commit_checkpoint_world_batch(
    directory: &dyn Directory, archive: &CellStorage, session: &mut NativeSession, deployment: Uuid,
    sections: Vec<rtdb_node::OwnedWorldSection>, root: rtdb_node::OwnedRootSection,
    fingerprint: [u8; 32], controls: Vec<rtdb_node::HostedControlWrite>, deadline: Timestamp,
) -> Result<TransactionKey> {
    if !(1..=8).contains(&sections.len()) { return Err(OwnedWorld::unavailable()); }
    let installation = directory.deployment(&deployment.to_string()).await?.ok_or_else(OwnedWorld::unavailable)?;
    let mut admitted = Vec::with_capacity(sections.len());
    let mut terminal = Vec::new();
    for section in sections {
        let id = Uuid::from_bytes(section.world.0);
        let retained = installation.checkpoint_worlds.get(&id).ok_or_else(OwnedWorld::unavailable)?;
        admit_session(session, &installation, &retained.owner)?;
        if retained.basis != section.parent_basis || now() >= retained.until { return Err(OwnedWorld::unavailable()); }
        admit_capture(&installation, archive, &CheckpointReadWorld {
            deployment, capture: retained.capture, manifest_sha256: retained.manifest_sha256.clone(),
            suffix: retained.suffix, basis: retained.basis,
        })?;
        if matches!(&section.effect, rtdb_node::OwnedWorldEffect::Drop) {
            terminal.push((id, retained));
        }
        admitted.push((retained.until, section));
    }
    let outcome = session.commit_owned_world_batch(admitted, root, fingerprint, controls, deadline).await?;
    if !outcome.committed() {
        return Err(outcome.error().cloned().unwrap_or_else(OwnedWorld::unavailable));
    }
    if !terminal.is_empty() {
        // The ordered terminal marker is authoritative. A failed retirement leaves
        // the exact pins retained until expiry.
        let _ = retire_world_pins(directory, &deployment.to_string(), &terminal, installation.incarnation, installation.log_epoch).await;
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

