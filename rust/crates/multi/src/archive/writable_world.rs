use std::sync::Arc;

use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_directory::{CheckpointWorldRetention, DeploymentRecord, Directory, HostedTask, MAX_RETAINED_WORLDS, now_ms};
use rtdb_node::{NativeSession, OwnedReadSession, OwnedWorldWitness, TransactionStatus};
use rtdb_types::{
    EntityId, Iid, Interval, OwnedRelation, OwnedWorld, PlacedAttemptScope, Result, StructFields,
    Timestamp, TransactionKey, Value, WorldClockMode, WorldSchemas, WorldShadow,
};
use uuid::Uuid;

use super::{
    CellStorage, CheckpointReadResources, CheckpointReadWorld, KeyService,
    archive_binding, capture_receipt, read_world::{CaptureSelection, with_registered_capture_context},
    replica_suffix, require_capture_open, same_lifetime,
};
use crate::admin::update_installation;

mod held;
pub(crate) use held::{HeldCheckpointWorld, WorldArchive, open_held_checkpoint_world};

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

/// Select an already published independent archive matching the original
/// held coordinate. The fork reopens and verifies that archive before it
/// writes the Directory pin or submits the ordered registration.
fn select_held_archive(
    source: &DeploymentRecord, archive: &CellStorage, deployment: Uuid, cut: &rtdb_node::OwnedForkCut,
    restore_binding: Option<[u8; 32]>,
) -> Result<Option<(Uuid, Option<Uuid>)>> {
    let binding = archive_binding(archive)?;
    let available = |capture: &Uuid, registered: &rtdb_directory::CheckpointCapture| {
        registered.archive == binding && registered.media_bound
            && registered.incarnation == source.incarnation && registered.log_epoch == source.log_epoch
            && restore_binding.is_none_or(|binding| registered.world_replay == Some(binding))
            && registered.checkpoint.root_sha256.is_some()
            && registered.manifest_sha256.is_some()
            && require_capture_open(source, *capture).is_ok()
    };
    if let Some((&capture, _)) = source.checkpoint_captures.iter().find(|(capture, registered)|
        available(capture, registered) && registered.checkpoint.system_time == cut.basis().0
    ) {
        return Ok(Some((capture, None)));
    }
    for (&suffix, pin) in &source.checkpoint_suffix_pins {
        if !cut.matches_basis(pin.basis) || pin.archive_sha256.is_none() || suffix.is_nil() {
            continue;
        }
        let capture = pin.capture;
        if !source.checkpoint_captures.get(&capture).is_some_and(|registered| available(&capture, registered)) {
            continue;
        }
        let (point, manifest) = capture_receipt(source, archive, deployment, capture)?
            .ok_or_else(OwnedWorld::unavailable)?;
        if replica_suffix::accepted(source, &point, &manifest, suffix).is_ok() {
            return Ok(Some((capture, Some(suffix))));
        }
    }
    Ok(None)
}

async fn copy_published_fork_input(
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    archive: &CellStorage,
    keys: &dyn KeyService,
    hosted_task: Option<&HostedTask>,
    deployment: Uuid,
    source: &DeploymentRecord,
    cut: &rtdb_node::OwnedForkCut,
    until: Timestamp,
) -> Result<Uuid> {
    let checkpoint = source.checkpoint.filter(|checkpoint| {
        checkpoint.root_sha256.is_some() && checkpoint.system_time == cut.basis().0
    }).ok_or_else(OwnedWorld::unavailable)?;
    if !cut.is_publication() || until <= now() || cut.until() <= now() {
        return Err(OwnedWorld::unavailable());
    }

    // The same exact source checkpoint and installed archive share one
    // Directory capture on every retry. A changed checkpoint gets a new ID.
    let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
    digest.update(b"rtdb.world-published-fork-input.v1\0");
    digest.update(deployment.as_bytes());
    digest.update(&source.incarnation.to_be_bytes());
    digest.update(&source.log_epoch.to_be_bytes());
    digest.update(&checkpoint.block.to_be_bytes());
    digest.update(&checkpoint.boundary.to_be_bytes());
    digest.update(&checkpoint.source.to_be_bytes());
    digest.update(&checkpoint.system_time.to_be_bytes());
    digest.update(&checkpoint.root_sha256.ok_or_else(OwnedWorld::unavailable)?);
    digest.update(archive_binding(archive)?.as_bytes());
    let mut id = [0; 16];
    id.copy_from_slice(&digest.finish().as_ref()[..16]);
    let operation = Uuid::from_bytes(id);
    super::capture_with_purpose(
        directory, storage, archive, keys, deployment, operation, hosted_task.cloned(), Some(source), super::CapturePurpose::AutomaticFork,
    ).await?;
    Ok(operation)
}

async fn copy_fork_suffix(
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    archive: &CellStorage,
    keys: &dyn KeyService,
    hosted_task: Option<&HostedTask>,
    agent: &crate::agent::NodeAgent,
    deployment: Uuid,
    source: &DeploymentRecord,
    cut: &rtdb_node::OwnedForkCut,
    deadline: Timestamp,
    restore_binding: Option<[u8; 32]>,
    memory: &Arc<MemoryBudget>,
) -> Result<(Uuid, Option<Uuid>)> {
    if (!cut.is_publication() && cut.overlay().is_none()) || deadline <= now() {
        return Err(OwnedWorld::unavailable());
    }
    let binding = archive_binding(archive)?;
    let capture = source.checkpoint_captures.iter().filter(|(capture, base)| {
        base.archive == binding && base.media_bound && base.manifest_sha256.is_some()
            && base.checkpoint.root_sha256.is_some() && base.incarnation == source.incarnation
            && base.log_epoch == source.log_epoch && base.checkpoint.system_time < cut.basis().0
            && restore_binding.is_none_or(|binding| base.world_replay == Some(binding))
            && require_capture_open(source, **capture).is_ok()
            && crate::hosted_recovery::replay_start(source, base.checkpoint.boundary).is_ok()
    }).max_by_key(|(_, base)| base.checkpoint.boundary)
        .map(|(capture, _)| *capture).ok_or_else(OwnedWorld::unavailable)?;

    let expires_at_ms = deadline.0.div_euclid(1000)
        .min(rtdb_directory::now_ms().saturating_add(60_000));
    let (suffix, pin) = super::stage::with_region(
        directory.as_ref(), keys, &source.id, archive, hosted_task, Some((capture, None)), None,
        |current| {
            require_capture_open(current, capture)?;
            let selected = current.checkpoint_captures.get(&capture)
                .filter(|selected| {
                    current.incarnation == source.incarnation && current.log_epoch == source.log_epoch
                        && source.checkpoint_captures.get(&capture) == Some(*selected)
                }).ok_or_else(OwnedWorld::unavailable)?;
            Ok(Some(crate::hosted_recovery::replay_start(current, selected.checkpoint.boundary)?))
        },
        |current, _, io| async move {
            let (point, manifest) = capture_receipt(&current, archive, deployment, capture)?
                .ok_or_else(OwnedWorld::unavailable)?;
            let (base, _) = replica_suffix::read_base_catalog(
                archive.bucket(Arc::clone(&io)).await?, &point, &manifest, keys, Arc::clone(&io),
            ).await?;
            agent.pin_held_fork_suffix(
                &source.name, deployment, capture, cut, keys, &base, io, expires_at_ms,
            ).await
        },
    ).await?;
    replica_suffix::capture(directory, storage, archive, keys, hosted_task, deployment, suffix, pin, memory).await?;
    Ok((capture, Some(suffix)))
}

/// Materializes a separately held exact overlay position from the original
/// placed replica stream. The result only identifies prepared archive media.
pub(crate) async fn prepare_checkpoint_world_overlay(
    directory: Arc<dyn Directory>, archive: &CellStorage, storage: &CellStorage,
    agent: &crate::agent::NodeAgent, keys: &dyn KeyService, hosted_task: Option<&HostedTask>,
    session: &mut NativeSession, deployment: Uuid, cut: rtdb_node::OwnedForkCut,
    child: Uuid, retain_until: Timestamp,
    resources: CheckpointReadResources<'_>,
) -> Result<rtdb_flight_sql::PreparedWorldOverlay> {
    let unavailable = OwnedWorld::unavailable;
    let (world, pin, logical) = cut.overlay().ok_or_else(unavailable)?;
    if child.is_nil() || child.as_bytes() == &world.0 || retain_until <= now() {
        return Err(unavailable());
    }
    let source = directory.deployment(&deployment.to_string()).await?.ok_or_else(unavailable)?;
    let retained = source.checkpoint_worlds.get(&Uuid::from_bytes(pin.0))
        .filter(|retained| retained.world.as_bytes() == &world.0 && retained.until >= retain_until)
        .ok_or_else(unavailable)?;
    admit_session(session, &source, &retained.owner)?;
    if session.placed_history()? != Some(cut.scope()) { return Err(unavailable()); }
    let live = session.open_owned_read().await?;
    let (original, _charge) = live.world_registration(world).await?.filter(|(original, _)| {
        original.pin == pin && !original.dropped && matches_retention(original, retained)
    }).ok_or_else(unavailable)?;
    if cut.basis() <= original.basis.system_time || cut.until() <= now() { return Err(unavailable()); }
    if live.world_registration(Iid(*child.as_bytes())).await?.is_some() { return Err(unavailable()); }
    let (ancestor, until) = if let Some(inherited) = &retained.ancestor {
        if !cut.admits_overlay(inherited.basis, cut.basis())
            || !original.ancestor.as_ref().is_some_and(|ancestor| ancestor.covers(cut.basis(), logical))
        {
            return Err(unavailable());
        }
        let mut selected = inherited.clone();
        selected.system = cut.basis();
        selected.logical = logical;
        (selected, cut.until())
    } else {
        let (capture, suffix) = if let Some(prior) = source.checkpoint_worlds.get(&child).filter(|prior| prior.until > now()) {
            let ancestor = prior.ancestor.as_ref().filter(|ancestor| {
                ancestor.world.as_bytes() == &world.0 && ancestor.pin.as_bytes() == &pin.0
                    && ancestor.logical == logical && cut.matches_basis(ancestor.basis)
            }).ok_or_else(unavailable)?;
            if prior.until != retain_until || prior.owner != retained.owner { return Err(unavailable()); }
            (ancestor.capture, ancestor.suffix)
        } else {
            let (capture, suffix) = copy_fork_suffix(
                Arc::clone(&directory), storage, archive, keys, hosted_task, agent, deployment,
                &source, &cut, cut.until(), None, resources.memory,
            ).await?;
            (capture, suffix.ok_or_else(unavailable)?)
        };
        let installed = directory.deployment(&deployment.to_string()).await?.ok_or_else(unavailable)?;
        admit_session(session, &installed, &retained.owner)?;
        let (point, manifest) = capture_receipt(&installed, archive, deployment, capture)?.ok_or_else(unavailable)?;
        let prepared = replica_suffix::accepted(&installed, &point, &manifest, suffix)?;
        if !cut.matches_basis(prepared.basis) || installed.checkpoint_worlds.get(&Uuid::from_bytes(pin.0)) != Some(retained) {
            return Err(unavailable());
        }
        let basis = prepared.basis;
        let reusable = installed.checkpoint_worlds.get(&child).is_some_and(|prior|
            prior.until > now() && prior.ancestor.as_ref().is_some_and(|ancestor| ancestor.suffix == suffix));
        let until = if reusable { cut.until() }
            else { cut.until().min(Timestamp(prepared.expires_at_ms.saturating_mul(1000))) };
        let owner = &retained.owner;
        let original = &original;
        with_registered_capture_context(
            directory.as_ref(), archive, keys, hosted_task,
            CaptureSelection { deployment, capture, manifest: &manifest, suffix: Some(suffix), expected_basis: Some(basis), require_replay: false },
            resources, |context, basis| async move {
                let (archived, _charge) = rtdb_query::read_owned_world(Arc::clone(&context), owner, world)
                    .await?.ok_or_else(unavailable)?;
                let publication = rtdb_query::read_owned_world_clock_revision(context, owner, world).await?;
                if &archived != original || publication != Some((logical, basis.system_time)) {
                    return Err(unavailable());
                }
                Ok(())
            },
        ).await?;
        let ancestor = rtdb_directory::CheckpointWorldAncestor {
            world: Uuid::from_bytes(world.0), pin: Uuid::from_bytes(pin.0), capture, suffix,
            manifest_sha256: manifest, basis, system: basis.system_time, source_root: original.basis, logical,
        };
        (ancestor, until)
    };
    let owner = &retained.owner;
    let reopened = open_world(directory.as_ref(), archive, session, deployment, Uuid::from_bytes(world.0)).await?;
    if reopened.record != original || now() >= until { return Err(unavailable()); }
    if original.ancestor.is_some() {
        reopened.snapshot.check_overlay_standing(&original).await?;
    } else {
        let refreshed = reopened.snapshot.fork_overlay_cut(cut.scope(), &original, ancestor.system, None).await?;
        if refreshed.overlay() != cut.overlay() || !refreshed.matches_basis(ancestor.basis) {
            return Err(unavailable());
        }
    }
    if now() >= until { return Err(unavailable()); }
    let binding = CheckpointWorldRetention {
        world: child, ancestor: Some(ancestor.clone()), owner: owner.clone(),
        capture: retained.capture, manifest_sha256: retained.manifest_sha256.clone(),
        schema_digest: retained.schema_digest, suffix: retained.suffix, basis: retained.basis,
        parent_publication: retained.parent_publication, mode: WorldClockMode::Retained, until: retain_until,
    };
    let key = deployment.to_string();
    update_installation(directory.as_ref(), &key, |current| {
        let active = now();
        if active >= until || active >= retain_until
            || current.incarnation != source.incarnation || current.log_epoch != source.log_epoch
            || PlacedAttemptScope::new(&current.stream("replica"), current.epoch)? != cut.scope()
            || current.checkpoint_worlds.get(&Uuid::from_bytes(pin.0)) != Some(retained)
        {
            return Err(unavailable());
        }
        admit_world_media(current, archive, deployment, &binding)?;
        current.checkpoint_worlds.retain(|_, world| world.until > active);
        if let Some(prior) = current.checkpoint_worlds.get(&child) {
            return if prior == &binding { Ok(()) } else { Err(unavailable()) };
        }
        if current.checkpoint_worlds.len() >= MAX_RETAINED_WORLDS { return Err(unavailable()); }
        current.checkpoint_worlds.insert(child, binding.clone());
        Ok(())
    }).await?;
    Ok(rtdb_flight_sql::PreparedWorldOverlay {
        world: world.0, parent_pin: pin.0, capture: *ancestor.capture.as_bytes(), suffix: *ancestor.suffix.as_bytes(),
        manifest: ancestor.manifest_sha256, basis: ancestor.basis, system: ancestor.system,
        logical, until: retain_until, child: *child.as_bytes(),
    })
}

/// Retain the pre-replay base once, then independently archive one exact
/// destination parent using the existing quorum-certified suffix owner.
pub(crate) async fn prepare_checkpoint_replay_parent(
    directory: Arc<dyn Directory>,
    archive: &CellStorage,
    storage: &CellStorage,
    agent: &crate::agent::NodeAgent,
    keys: &dyn KeyService,
    hosted_task: Option<&HostedTask>,
    session: &mut NativeSession,
    deployment: Uuid,
    binding: [u8; 32],
    original: Option<TransactionKey>,
    required_schemas: Option<&WorldSchemas>,
    resources: CheckpointReadResources<'_>,
) -> Result<Option<rtdb_flight_sql::PreparedReplayParent>> {
    let owner = session.state().user().filter(|owner| !owner.is_empty())
        .ok_or_else(OwnedWorld::unavailable)?.to_owned();
    let source = directory.deployment(&deployment.to_string()).await?
        .ok_or_else(OwnedWorld::unavailable)?;
    admit_session(session, &source, &owner)?;
    let held = session.open_owned_read().await?;
    held.replay_parent_hold(binding, false).await?;

    if original.is_none() {
        let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
        digest.update(b"rtdb.replayed-world-archive-base.v1\0");
        digest.update(deployment.as_bytes());
        digest.update(&binding);
        digest.update(archive_binding(archive)?.as_bytes());
        let mut id = [0; 16];
        id.copy_from_slice(&digest.finish().as_ref()[..16]);
        let operation = Uuid::from_bytes(id);
        if source.checkpoint_captures.get(&operation)
            .is_some_and(|base| base.world_replay != Some(binding))
        {
            return Err(OwnedWorld::unavailable());
        }
        if capture_receipt(&source, archive, deployment, operation)?.is_none() {
            held.replay_parent_hold(binding, true).await?;
            drop(held);
            let flushed = if source.checkpoint_captures.contains_key(&operation) {
                // The admission already fixed the checkpoint. Resume the same
                // copy even if a later fact-free reservation has been flushed.
                None
            } else {
                let crate::control::Response::Flushed { checkpoint } = agent.flush(&source.name).await?
                else { return Err(OwnedWorld::unavailable()); };
                let current = directory.deployment(&deployment.to_string()).await?
                    .filter(|record| record.checkpoint == Some(checkpoint) && record.epoch == source.epoch)
                    .ok_or_else(OwnedWorld::unavailable)?;
                admit_session(session, &current, &owner)?;
                Some(current)
            };
            session.open_owned_read().await?.replay_parent_hold(binding, true).await?;
            super::capture_with_purpose(
                Arc::clone(&directory), storage, archive, keys, deployment, operation,
                hosted_task.cloned(), flushed.as_ref(),
                super::CapturePurpose::ReplayedWorldBase(binding),
            ).await?;
        }
        return Ok(None);
    }

    let original = original.ok_or_else(OwnedWorld::unavailable)?;
    let scope = session.placed_history()?.ok_or_else(OwnedWorld::unavailable)?;
    let (cut, logical) = held.replayed_parent_cut(binding, original, scope).await?;
    let (capture, suffix) = if let Some(capture) = select_held_archive(&source, archive, deployment, &cut, Some(binding))? {
        capture
    } else {
        copy_fork_suffix(
            Arc::clone(&directory), storage, archive, keys, hosted_task, agent, deployment,
            &source, &cut, cut.until(), Some(binding), resources.memory,
        ).await?
    };
    let installed = directory.deployment(&deployment.to_string()).await?
        .ok_or_else(OwnedWorld::unavailable)?;
    admit_session(session, &installed, &owner)?;
    let (_, manifest) = capture_receipt(&installed, archive, deployment, capture)?
        .ok_or_else(OwnedWorld::unavailable)?;
    let basis = with_registered_capture_context(
        directory.as_ref(), archive, keys, hosted_task,
        CaptureSelection { deployment, capture, manifest: &manifest, suffix, expected_basis: None, require_replay: false },
        resources, |context, basis| {
            let owner = &owner;
            async move {
                if let Some(required) = required_schemas {
                    let (schemas, _charge) = rtdb_query::read_owned_parent_schemas(context, owner).await?;
                    if !schemas.includes(required) {
                        return Err(OwnedWorld::unavailable());
                    }
                }
                Ok(basis)
            }
        },
    ).await?;
    if !cut.matches_basis(basis) { return Err(OwnedWorld::unavailable()); }
    let parent = CheckpointReadWorld { deployment, capture, manifest_sha256: manifest.clone(), suffix, basis };
    admit_capture(&installed, archive, &parent)?;
    session.open_owned_read().await?.replay_parent_hold(binding, false).await?;
    Ok(Some(rtdb_flight_sql::PreparedReplayParent {
        capture: *capture.as_bytes(),
        suffix: suffix.map(|value| *value.as_bytes()),
        manifest, basis, logical,
    }))
}

/// Bind a provisional world to the destination's independently retained
/// parent using the pin identity already ordered by the Original history.
pub(crate) async fn retain_replayed_checkpoint_world(
    directory: Arc<dyn Directory>,
    archive: &CellStorage,
    storage: &CellStorage,
    agent: &crate::agent::NodeAgent,
    keys: &dyn KeyService,
    hosted_task: Option<&HostedTask>,
    session: &mut NativeSession,
    deployment: Uuid,
    binding: [u8; 32],
    id: Uuid,
    resources: CheckpointReadResources<'_>,
) -> Result<rtdb_flight_sql::RetainedReplayWorld> {
    let unavailable = OwnedWorld::unavailable;
    let owner = session.state().user().filter(|owner| !owner.is_empty())
        .ok_or_else(unavailable)?.to_owned();
    let iid = Iid(*id.as_bytes());
    let staged = session.open_owned_read().await?.replayed_world(binding, iid).await?;
    let parent = prepare_checkpoint_replay_parent(
        Arc::clone(&directory), archive, storage, agent, keys, hosted_task, session,
        deployment, binding, Some(staged.world.basis), Some(&staged.world.schemas), resources.clone(),
    ).await?.ok_or_else(unavailable)?;
    let ancestor = if let Some(original) = &staged.world.ancestor {
        let archived = prepare_checkpoint_replay_parent(
            Arc::clone(&directory), archive, storage, agent, keys, hosted_task, session,
            deployment, binding, Some(original.basis), None, resources.clone(),
        ).await?.ok_or_else(unavailable)?;
        if archived.basis.tx_id <= parent.basis.tx_id || archived.basis.system_time <= parent.basis.system_time {
            return Err(unavailable());
        }
        let suffix = archived.suffix.map(Uuid::from_bytes).ok_or_else(unavailable)?;
        let capture = Uuid::from_bytes(archived.capture);
        let source_world = Iid(original.world);
        let owner = &owner;
        let schemas = &staged.world.schemas;
        let system = with_registered_capture_context(
            directory.as_ref(), archive, keys, hosted_task,
            CaptureSelection {
                deployment, capture, manifest: &archived.manifest, suffix: Some(suffix),
                expected_basis: Some(archived.basis), require_replay: false,
            },
            resources,
            |context, basis| async move {
                let (record, _) = rtdb_query::read_owned_world(Arc::clone(&context), owner, source_world)
                    .await?.ok_or_else(unavailable)?;
                if record.dropped || record.pin.0 != original.pin || record.ancestor.is_some()
                    || record.basis != original.source_root
                    || !matches!(record.mode, WorldClockMode::Held { .. }) || &record.schemas != schemas
                {
                    return Err(unavailable());
                }
                let (system, logical) = rtdb_query::select_owned_world_clock(
                    &context, owner, source_world, record.basis.system_time, record.initial_clock(), original.logical,
                ).await?.ok_or_else(unavailable)?;
                if logical != original.logical
                    || (system == basis.system_time) != (original.system == original.basis.system_time)
                {
                    return Err(unavailable());
                }
                Ok(system)
            },
        ).await?;
        Some(rtdb_directory::CheckpointWorldAncestor {
            world: Uuid::from_bytes(original.world), pin: Uuid::from_bytes(original.pin),
            capture, suffix, manifest_sha256: archived.manifest, basis: archived.basis, system,
            source_root: original.source_root, logical: original.logical,
        })
    } else {
        None
    };
    let live = session.open_owned_read().await?;
    let current = live.replayed_world(binding, iid).await?;
    let world = current.world;
    if world != staged.world || current.source_cut != staged.source_cut
        || current.destination_parent != staged.destination_parent
        || parent.basis.system_time != current.destination_parent
    {
        return Err(unavailable());
    }
    let duration = world.until.0.checked_sub(current.source_cut.0)
        .filter(|duration| *duration > 0).ok_or_else(unavailable)?;
    let until = Timestamp(live.present().system_time.max(now()).0
        .checked_add(duration).filter(|until| *until < i64::MAX).ok_or_else(unavailable)?);
    live.check()?;
    let pin = Uuid::from_bytes(world.pin.0);
    let retained = CheckpointWorldRetention {
        ancestor,
        world: id,
        owner,
        capture: Uuid::from_bytes(parent.capture),
        manifest_sha256: parent.manifest,
        schema_digest: world.schemas.digest(),
        suffix: parent.suffix.map(Uuid::from_bytes),
        basis: parent.basis,
        parent_publication: Some(world.root_clock()),
        mode: world.mode,
        until,
    };
    let scope = session.placed_history()?;
    let database = session.state().database().to_owned();
    let installed = update_installation(directory.as_ref(), &deployment.to_string(), |record| {
        let eligible = PlacedAttemptScope::new(&record.stream("replica"), record.epoch)
            .map_err(|_| unavailable())?;
        if record.name != database || record.writer_pool().is_none() || scope != Some(eligible) {
            return Err(unavailable());
        }
        let now = now();
        record.checkpoint_worlds.retain(|_, world| world.until > now);
        let selected = if let Some(existing) = record.checkpoint_worlds.get(&pin) {
            if existing.world != id || existing.owner != retained.owner
                || existing.schema_digest != retained.schema_digest || existing.basis != retained.basis
                || existing.parent_publication != retained.parent_publication || existing.mode != retained.mode
                || existing.ancestor != retained.ancestor
            {
                return Err(unavailable());
            }
            existing
        } else {
            if until <= now || record.checkpoint_worlds.len() >= MAX_RETAINED_WORLDS {
                return Err(unavailable());
            }
            &retained
        };
        admit_world_media(record, archive, deployment, selected)?;
        record.checkpoint_worlds.entry(pin).or_insert_with(|| retained.clone());
        Ok(())
    }).await?;
    let retained = installed.checkpoint_worlds.get(&pin).ok_or_else(unavailable)?;
    Ok(rtdb_flight_sql::RetainedReplayWorld {
        pin: *pin.as_bytes(),
        ancestor: world_ancestor(retained),
        capture: *retained.capture.as_bytes(),
        suffix: retained.suffix.map(|suffix| *suffix.as_bytes()),
        manifest: retained.manifest_sha256.clone(),
        basis: retained.basis,
        until: retained.until,
    })
}

pub(crate) async fn finish_checkpoint_replay_parents(
    directory: &dyn Directory, session: &mut NativeSession, deployment: Uuid, binding: [u8; 32],
) -> Result<()> {
    let unavailable = OwnedWorld::unavailable;
    let id = deployment.to_string();
    let initial = directory.deployment(&id).await?.ok_or_else(unavailable)?;
    let owner = session.state().user().filter(|owner| !owner.is_empty()).ok_or_else(unavailable)?;
    admit_session(session, &initial, owner)?;
    session.open_owned_read().await?.sealed_replay_parents(binding).await?;
    update_installation(directory, &id, |record| {
        if record.incarnation != initial.incarnation || record.log_epoch != initial.log_epoch
            || record.epoch != initial.epoch || record.holder != initial.holder
        {
            return Err(unavailable());
        }
        for capture in record.checkpoint_captures.values_mut() {
            if let Some(owner) = capture.world_replay {
                if owner != binding || capture.incarnation != record.incarnation
                    || capture.log_epoch != record.log_epoch || capture.manifest_sha256.is_none()
                {
                    return Err(unavailable());
                }
                capture.world_replay = None;
                capture.automatic_fork = true;
            }
        }
        Ok(())
    }).await?;
    Ok(())
}

fn world_ancestor(retained: &CheckpointWorldRetention) -> Option<rtdb_types::OwnedWorldAncestor> {
    retained.ancestor.as_ref().map(|ancestor| rtdb_types::OwnedWorldAncestor {
        world: *ancestor.world.as_bytes(),
        pin: *ancestor.pin.as_bytes(),
        capture: *ancestor.capture.as_bytes(),
        suffix: *ancestor.suffix.as_bytes(),
        manifest: ancestor.manifest_sha256.clone(),
        basis: ancestor.basis,
        system: ancestor.system,
        source_root: ancestor.source_root,
        logical: ancestor.logical,
    })
}

fn world_record(
    pin: Uuid,
    retained: &CheckpointWorldRetention,
    schemas: rtdb_types::WorldSchemas,
) -> OwnedWorld {
    OwnedWorld {
        ancestor: world_ancestor(retained),
        id: Iid(*retained.world.as_bytes()),
        pin: Iid(*pin.as_bytes()),
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
    directory: Arc<dyn Directory>, archive: &CellStorage, capture_source: Option<&CellStorage>, agent: Option<&crate::agent::NodeAgent>, keys: &dyn KeyService, hosted_task: Option<&HostedTask>,
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
    let mut source = directory
        .deployment(&deployment)
        .await?
        .ok_or_else(OwnedWorld::unavailable)?;
    admit_session(session, &source, &owner)?;
    if let Some((world, pin, logical)) = fork.cut.overlay() {
        let unavailable = OwnedWorld::unavailable;
        let child = Uuid::from_bytes(fork.world.0);
        let retained = source.checkpoint_worlds.get(&child).ok_or_else(unavailable)?;
        let ancestor = retained.ancestor.as_ref().ok_or_else(unavailable)?;
        if !capture.is_nil() || suffix.is_some() || child.is_nil() || retained.world != child
            || fork.world != fork.pin || retained.owner != owner || retained.until != until
            || retained.mode != WorldClockMode::Retained || fork.cut.until() <= now() || until <= now()
            || ancestor.logical != logical || !fork.cut.admits_overlay(ancestor.basis, ancestor.system)
            || session.placed_history()? != Some(fork.cut.scope())
        {
            return Err(unavailable());
        }
        let source_retention = source.checkpoint_worlds.get(&Uuid::from_bytes(pin.0)).ok_or_else(unavailable)?;
        let live = session.open_owned_read().await?;
        let (registered, _charge) = live.world_registration(world).await?.ok_or_else(unavailable)?;
        let registration = world_record(child, retained, registered.schemas.clone());
        if registered.pin != pin || !matches_retention(&registered, source_retention)
            || registered.schemas.digest() != retained.schema_digest || !registered.can_retain_overlay(&registration)
        {
            return Err(unavailable());
        }
        admit_world_media(&source, archive, deployment_id, retained)?;
        live.check()?;
        drop(live);
        let outcome = session.register_owned_world_checked(&registration, fork).await?;
        if !outcome.committed() {
            return Err(outcome.error().cloned().unwrap_or_else(unavailable));
        }
        return Ok((child, outcome.tx_key()));
    }
    let authored_advance = fork.previous.is_some() && fork.cut.is_publication()
        && fork.root.as_ref().is_some_and(|root| root.publication.is_some())
        && matches!(fork.requested, rtdb_node::WorldClockRequest::Retained);
    let renewal = fork.cut.is_renewal();
    let proposal_hold = renewal && matches!(fork.requested, rtdb_node::WorldClockRequest::Proposal);
    let proposal_shorten = fork.shortens_proposal();
    let source_mode = if proposal_hold { WorldClockMode::Proposal } else { WorldClockMode::Retained };
    let (capture, suffix) = if renewal {
        let predecessor = fork.previous.ok_or_else(OwnedWorld::unavailable)?;
        let source_world = fork.sibling.unwrap_or(fork.world);
        let retained = source.checkpoint_worlds.get(&Uuid::from_bytes(predecessor.0))
            .filter(|retained| retained.world.as_bytes() == &source_world.0
                && retained.owner == owner && retained.mode == source_mode
                && retained.until > now() && fork.cut.matches_basis(retained.basis))
            .ok_or_else(OwnedWorld::unavailable)?;
        if !capture.is_nil() || suffix.is_some() {
            return Err(OwnedWorld::unavailable());
        }
        (retained.capture, retained.suffix)
    } else if capture.is_nil() {
        if suffix.is_some() || fork.root.is_none() || (fork.previous.is_some() && !authored_advance)
            || !(matches!(fork.requested, rtdb_node::WorldClockRequest::Retained)
                || (matches!(fork.requested, rtdb_node::WorldClockRequest::Proposal)
                    && fork.cut.is_publication()))
        {
            return Err(OwnedWorld::unavailable());
        }
        if fork.cut.live().is_none() && !fork.cut.is_publication() {
            return Err(OwnedWorld::unavailable());
        }
        if let Some(selected) = select_held_archive(&source, archive, deployment_id, &fork.cut, None)? {
            selected
        } else {
            let storage = capture_source.ok_or_else(OwnedWorld::unavailable)?;
            let deadline = until.min(fork.deadline).min(fork.cut.until());
            let selected = if source.checkpoint.is_some_and(|checkpoint|
                checkpoint.system_time == fork.cut.basis().0 && checkpoint.root_sha256.is_some()
            ) {
                let capture = copy_published_fork_input(
                    Arc::clone(&directory), storage, archive, keys, hosted_task, deployment_id,
                    &source, &fork.cut, deadline,
                ).await?;
                (capture, None)
            } else {
                copy_fork_suffix(
                    Arc::clone(&directory), storage, archive, keys, hosted_task,
                    agent.ok_or_else(OwnedWorld::unavailable)?, deployment_id, &source, &fork.cut, deadline, None, resources.memory,
                ).await?
            };
            source = directory.deployment(&deployment).await?.ok_or_else(OwnedWorld::unavailable)?;
            admit_session(session, &source, &owner)?;
            selected
        }
    } else {
        (capture, suffix)
    };
    require_capture_open(&source, capture)?;
    let (point, manifest) = capture_receipt(&source, archive, deployment_id, capture)?
        .ok_or_else(OwnedWorld::unavailable)?;
    if !same_lifetime(&source, &point) || until <= now() || until.0 <= point.checkpoint.system_time {
        return Err(OwnedWorld::unavailable());
    }
    let expected_basis = suffix.map(|suffix|
        replica_suffix::accepted(&source, &point, &manifest, suffix).map(|pin| pin.basis)
    ).transpose()?;
    if expected_basis.is_some_and(|basis|
        until <= basis.system_time || !fork.cut.matches_basis(basis)
    ) {
        return Err(OwnedWorld::unavailable());
    }
    let expected_basis = expected_basis.or(fork.cut.live());
    let live = session.open_owned_read().await?;
    let predecessor = if let Some(pin) = fork.previous.filter(|_| renewal) {
        let world = fork.sibling.unwrap_or(fork.world);
        let retained = source.checkpoint_worlds.get(&Uuid::from_bytes(pin.0))
            .ok_or_else(OwnedWorld::unavailable)?;
        let (record, charge) = live.world_registration(world).await?.ok_or_else(OwnedWorld::unavailable)?;
        if record.pin != pin || record.dropped || !matches_retention(&record, retained) {
            return Err(OwnedWorld::unavailable());
        }
        Some((record, charge))
    } else {
        None
    };
    let ((schemas, _schema_charge), parent_publication, basis) = with_registered_capture_context(
        directory.as_ref(), archive, keys, hosted_task,
        CaptureSelection { deployment: deployment_id, capture, manifest: &manifest, suffix, expected_basis, require_replay: false },
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
    let head = live.present();
    if (basis.system_time >= head.system_time || basis.tx_id >= head.tx_id)
        && (!fork.cut.is_publication() || basis != head)
    {
        return Err(OwnedWorld::unavailable());
    }
    drop(live);
    let parent = CheckpointReadWorld { deployment: deployment_id, capture, manifest_sha256: manifest, suffix, basis };

    if !fork.cut.matches_basis(basis)
        || parent_publication.is_some_and(|publication| publication > basis.system_time)
        || (fork.cut.is_publication() && parent_publication != Some(basis.system_time))
    {
        return Err(OwnedWorld::unavailable());
    }
    // A renewal keeps the parent already admitted at this exact archive.
    let (schemas, parent_publication) = match predecessor {
        Some((prior, _charge)) if schemas.includes(&prior.schemas) => {
            (prior.schemas, prior.parent_publication)
        }
        Some(_) => return Err(OwnedWorld::unavailable()),
        None => (schemas, parent_publication),
    };
    let inherited = parent_publication.unwrap_or(basis.system_time);
    let mode = match fork.requested {
        rtdb_node::WorldClockRequest::Retained => WorldClockMode::Retained,
        rtdb_node::WorldClockRequest::Proposal if fork.cut.live().is_some()
            || (fork.cut.is_publication() && fork.previous.is_none() && fork.root.is_some()) => WorldClockMode::Proposal,
        rtdb_node::WorldClockRequest::Proposal => return Err(OwnedWorld::unavailable()),
        rtdb_node::WorldClockRequest::Held { start } => {
            let start = start.unwrap_or(inherited);
            if start < inherited || start == Timestamp::MAX || (start > inherited && fork.cut.live().is_none()) {
                return Err(OwnedWorld::unavailable());
            }
            WorldClockMode::Held { start }
        }
    };
    let id = Uuid::from_bytes(fork.world.0);
    let pin = Uuid::from_bytes(fork.pin.0);
    if id.is_nil() || pin.is_nil() || ((fork.previous.is_none() || fork.sibling.is_some()) != (pin == id)) {
        return Err(OwnedWorld::unavailable());
    }
    let retained = CheckpointWorldRetention {
        ancestor: None,
        world: id,
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
    let installed = update_installation(directory.as_ref(), &deployment, |record| {
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
        if let Some(previous) = fork.previous {
            let previous = record.checkpoint_worlds.get(&Uuid::from_bytes(previous.0)).ok_or_else(OwnedWorld::unavailable)?;
            let expected_source = fork.sibling.map_or(id, |world| Uuid::from_bytes(world.0));
            if previous.world != expected_source || previous.owner != retained.owner
                || (!authored_advance && !renewal && previous.until != retained.until)
                || previous.mode != source_mode || retained.mode != source_mode || (proposal_hold && if proposal_shorten {
                    retained.until >= previous.until
                } else {
                    retained.until < previous.until
                })
                || if renewal {
                    previous.basis != retained.basis || previous.capture != retained.capture
                        || previous.suffix != retained.suffix || previous.manifest_sha256 != retained.manifest_sha256
                        || previous.schema_digest != retained.schema_digest
                        || previous.parent_publication != retained.parent_publication
                } else {
                    previous.basis.system_time >= retained.basis.system_time || previous.basis.tx_id >= retained.basis.tx_id
                }
            {
                return Err(OwnedWorld::unavailable());
            }
        }
        if let Some(existing) = record.checkpoint_worlds.get(&pin) {
            return if existing == &retained { Ok(()) } else { Err(OwnedWorld::unavailable()) };
        }
        if record.checkpoint_worlds.len() >= MAX_RETAINED_WORLDS {
            return Err(OwnedWorld::unavailable());
        }
        record.checkpoint_worlds.insert(pin, retained.clone());
        Ok(())
    })
    .await?;
    admit_session(session, &installed, &retained.owner)?;
    let registration = world_record(pin, &retained, schemas);
    let previous = if fork.sibling.is_some() { None } else { fork.previous.map(|previous| Uuid::from_bytes(previous.0)) };
    let outcome = session.register_owned_world_checked(&registration, fork).await?;
    if !outcome.committed() {
        // A competing retry may already have registered this world.
        // Its exact pending pin remains eligible for recovery until expiry.
        return Err(outcome
            .error()
            .cloned()
            .unwrap_or_else(OwnedWorld::unavailable));
    }
    if let Some(previous) = previous
        && let Some(prior) = installed.checkpoint_worlds.get(&previous)
    {
        let _ = retire_world_pins(directory.as_ref(), &deployment, &[(previous, prior)], installed.incarnation, installed.log_epoch).await;
    }
    Ok((id, outcome.tx_key()))
}

fn matches_retention(world: &OwnedWorld, retained: &CheckpointWorldRetention) -> bool {
    world.ancestor == world_ancestor(retained)
        && world.id.0 == *retained.world.as_bytes()
        && world.capture.0 == *retained.capture.as_bytes()
        && world.suffix.map(|suffix| Uuid::from_bytes(suffix.0)) == retained.suffix
        && world.manifest == retained.manifest_sha256
        && world.basis == retained.basis
        && world.parent_publication == retained.parent_publication
        && world.mode == retained.mode
        && world.until == retained.until
        && world.schemas.digest() == retained.schema_digest
}

fn parent_media(deployment: Uuid, retained: &CheckpointWorldRetention) -> CheckpointReadWorld {
    CheckpointReadWorld {
        deployment, capture: retained.capture, manifest_sha256: retained.manifest_sha256.clone(),
        suffix: retained.suffix, basis: retained.basis,
    }
}

fn ancestor_media(deployment: Uuid, ancestor: &rtdb_directory::CheckpointWorldAncestor) -> CheckpointReadWorld {
    CheckpointReadWorld {
        deployment, capture: ancestor.capture, manifest_sha256: ancestor.manifest_sha256.clone(),
        suffix: Some(ancestor.suffix), basis: ancestor.basis,
    }
}

fn admit_world_media(
    installation: &DeploymentRecord, archive: &CellStorage, deployment: Uuid, retained: &CheckpointWorldRetention,
) -> Result<()> {
    admit_capture(installation, archive, &parent_media(deployment, retained))?;
    if let Some(ancestor) = &retained.ancestor {
        admit_capture(installation, archive, &ancestor_media(deployment, ancestor))?;
    }
    Ok(())
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
    let snapshot = session.open_owned_read().await?;
    let (world, charge) = snapshot
        .world_registration(Iid(*id.as_bytes()))
        .await?
        .ok_or_else(OwnedWorld::unavailable)?;
    let installation = directory
        .deployment(&deployment.to_string())
        .await?
        .ok_or_else(OwnedWorld::unavailable)?;
    let retained = installation
        .checkpoint_worlds
        .get(&Uuid::from_bytes(world.pin.0))
        .ok_or_else(OwnedWorld::unavailable)?;
    admit_session(session, &installation, &retained.owner)?;
    if retained.until <= now() {
        return Err(OwnedWorld::unavailable());
    }
    admit_world_media(&installation, archive, deployment, retained)?;
    if world.dropped
        || snapshot.present().system_time >= retained.until
        || !matches_retention(&world, retained)
    {
        return Err(OwnedWorld::unavailable());
    }
    let clock = snapshot.world_application_clock(world.id).await?;
    if clock.is_some_and(|clock| !world.mode.is_writable() || clock <= world.initial_clock() || clock > snapshot.present().system_time) {
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
    deployment: Uuid, id: Uuid, checks: Vec<OwnedWorldWitness>,
    controls: Vec<rtdb_node::HostedControlWrite>, deadline: Timestamp, clock: Option<rtdb_types::WorldClockLimit>,
) -> Result<TransactionKey> {
    let OpenWorld { snapshot, record, _schema_charge, .. } = open_world(directory, archive, session, deployment, id).await?;
    drop(snapshot);
    let outcome = session.validate_owned_world(&record, checks, controls, deadline, clock).await?;
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
        let pin = Uuid::from_bytes(section.parent_pin.0);
        let retained = installation.checkpoint_worlds.get(&pin).ok_or_else(OwnedWorld::unavailable)?;
        admit_session(session, &installation, &retained.owner)?;
        if retained.world.as_bytes() != &section.world.0 || retained.basis != section.parent_basis
            || now() >= retained.until || (retained.ancestor.is_some() && !matches!(&section.effect, rtdb_node::OwnedWorldEffect::Drop))
        {
            return Err(OwnedWorld::unavailable());
        }
        admit_world_media(&installation, archive, deployment, retained)?;
        if matches!(&section.effect, rtdb_node::OwnedWorldEffect::Drop) {
            terminal.push((pin, retained));
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
    pub(crate) until: Option<Timestamp>,
    pub(crate) shadowed: bool,
    pub(crate) witness: Option<OwnedWorldWitness>,
}

pub struct WorldFact {
    row: StructFields<Value>,
    revision: Timestamp,
    lifetime: Option<Interval>,
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
    lifetime: Option<i64>,
) -> Result<Option<WorldFact>> {
    let mut found = None;
    for batch in batches {
        let batch = batch.batch();
        for row in 0..batch.num_rows() {
            if found.is_some() {
                return Err(OwnedWorld::unavailable());
            }
            let revision = rtdb_query::owned_row_revision(batch, row, id, memory)?;
            let lifetime = lifetime.map(|duration| {
                let (created, expires) = rtdb_query::owned_row_lifetime(batch, row, revision, duration)?;
                Interval::new(created, expires)
            }).transpose()?;
            let columns = || {
                batch
                    .schema_ref()
                    .fields()
                    .iter()
                    .zip(batch.columns())
                    .filter(|(field, _)| field.name() != "_system_from"
                        && !(lifetime.is_some() && matches!(field.name().as_str(), "_valid_from" | "_valid_to")))
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
                lifetime,
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
        parent_media(deployment, &self.retained)
    }

    /// Outer absence allows archive fallback. A local tombstone answers absence.
    async fn read_local(
        &self, relation: &OwnedRelation, id: &EntityId, valid_at: Timestamp, memory: &Arc<MemoryBudget>,
        observed: bool,
    ) -> Result<(Option<Option<WorldFact>>, Option<OwnedWorldWitness>)> {
        let read = self.snapshot.read_world_shadow(self.record.id, relation, id, valid_at, observed).await?;
        let Some(fact) = decode_point(read.row.into_iter().collect(), id, memory, None)? else {
            return Ok((None, read.witness));
        };
        let visible = match WorldShadow::from_record(fact.row)? {
            WorldShadow::Deleted => None,
            WorldShadow::Present(row) => Some(WorldFact { row, revision: fact.revision, lifetime: None, _reservation: fact._reservation }),
        };
        Ok((Some(visible), read.witness))
    }
}

