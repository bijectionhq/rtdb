use std::sync::Arc;

use arrow_array::{ArrayRef, NullArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_directory::{CheckpointWorldMedia, CheckpointWorldRetention, DeploymentRecord, Directory, HostedTask, MAX_RETAINED_WORLDS, WorldForkRequestLink, WorldForkReservation, now_ms};
use rtdb_node::{AppliedReadFloor, NativeSession, OwnedReadSession, OwnedWorldRegistrationOutcome, OwnedWorldWitness, TransactionStatus};
use rtdb_types::{
    EntityId, Iid, Interval, OwnedRelation, OwnedWorld, OwnedWorldAssertion, PlacedAttemptScope, Result, StructFields,
    Timestamp, TransactionKey, Value, WorldClockMode, WorldSchemas, WorldShadow,
};
use uuid::Uuid;

use super::{
    CellStorage, CheckpointReadResources, CheckpointReadWorld, KeyService,
    archive_binding, capture_receipt, read_world::{CaptureSelection, with_registered_capture_context},
    replica_suffix, require_capture_open, same_lifetime,
};
use crate::admin::update_installation;

mod fork_pin;
mod held;
use fork_pin::{
    CertifiedSourceImage, ForkDecisionOutcome, ForkDecisionRecord, ForkDisposition, ForkPin,
    ForkPinWrite, recover_pin_write, select_pin_write,
};
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
        // The capture's checkpoint is the parent's basis by position: a tied
        // transaction shares the time (T7).
        None if checkpoint_source_tx(record, point.checkpoint) == Some(parent.basis.tx_id)
            && point.checkpoint.system_time == parent.basis.system_time.0 => Ok(()),
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
        available(capture, registered) && matches_fork_checkpoint(source, cut, registered.checkpoint)
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

fn checkpoint_source_tx(source: &DeploymentRecord, checkpoint: rtdb_directory::Checkpoint) -> Option<rtdb_types::TxId> {
    rtdb_types::SourcePosition::from_epoch_offset(rtdb_types::StorageEpoch(source.log_epoch), checkpoint.source)
        .ok().map(|position| rtdb_types::TxId(position.0))
}

fn matches_fork_checkpoint(
    source: &DeploymentRecord, cut: &rtdb_node::OwnedForkCut, checkpoint: rtdb_directory::Checkpoint,
) -> bool {
    checkpoint_source_tx(source, checkpoint) == Some(cut.positioned_basis().tx_id)
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
        checkpoint.root_sha256.is_some() && matches_fork_checkpoint(source, cut, *checkpoint)
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
            && base.log_epoch == source.log_epoch
            && checkpoint_source_tx(source, base.checkpoint).is_some_and(|position| position < cut.positioned_basis().tx_id)
            && restore_binding.is_none_or(|binding| base.world_replay == Some(binding))
            && require_capture_open(source, **capture).is_ok()
            && crate::hosted_recovery::capture_replay_start(source, base).is_ok()
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
            Ok(Some(crate::hosted_recovery::capture_replay_start(current, selected)?))
        },
        |current, region, io| async move {
            let (point, manifest) = capture_receipt(&current, archive, deployment, capture)?
                .ok_or_else(OwnedWorld::unavailable)?;
            let (base, _) = replica_suffix::read_base_catalog(
                archive.bucket(Arc::clone(&io)).await?, &point, &manifest, keys, Arc::clone(&io),
            ).await?;
            agent.pin_held_fork_suffix(
                &source.name, deployment, capture, cut, keys, &base, region, io, expires_at_ms,
            ).await
        },
    ).await?;
    replica_suffix::capture(directory, archive, keys, agent.replay_broker(), hosted_task, deployment, suffix, pin, memory).await?;
    Ok((capture, Some(suffix)))
}

/// Materializes a separately held exact overlay position from the original
/// placed replica stream. The result only identifies prepared archive media.
pub(crate) async fn prepare_checkpoint_world_overlay(
    directory: Arc<dyn Directory>, archive: &CellStorage, storage: &CellStorage,
    agent: &crate::agent::NodeAgent, keys: &dyn KeyService, hosted_task: Option<&HostedTask>,
    session: &mut NativeSession, deployment: Uuid, cut: rtdb_node::OwnedForkCut,
    child: Uuid, retain_until: Timestamp, writable: bool,
    resources: CheckpointReadResources<'_>,
) -> Result<rtdb_flight_sql::PreparedWorldOverlay> {
    let unavailable = OwnedWorld::unavailable;
    let (world, pin, logical) = cut.overlay().ok_or_else(unavailable)?;
    let mode = if writable { WorldClockMode::Held { start: logical } } else { WorldClockMode::Retained };
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
    let publication = Some(cut.positioned_basis())
        .filter(|publication| publication.tx_id > original.basis.tx_id && original.basis.at_or_before(*publication))
        .ok_or_else(unavailable)?;
    if cut.until() <= now() { return Err(unavailable()); }
    if live.world_registration(Iid(*child.as_bytes())).await?.is_some() { return Err(unavailable()); }
    let inherited_at = original.ancestor_covering(publication);
    let (mut ancestors, ancestor, until) = if let Some(inherited_at) = inherited_at {
        let index = original.ancestors.iter().position(|layer| layer == inherited_at).ok_or_else(unavailable)?;
        let inherited = retained.ancestors.get(index).ok_or_else(unavailable)?;
        if !cut.admits_overlay(inherited.basis, publication) || !inherited_at.covers(publication, logical) {
            return Err(unavailable());
        }
        let mut selected = inherited.clone();
        selected.publication = publication;
        selected.logical = logical;
        (retained.ancestors[..index].to_vec(), selected, cut.until())
    } else {
        let (capture, suffix) = if let Some(prior) = source.checkpoint_worlds.get(&child).filter(|prior| prior.until > now()) {
            let ancestor = prior.ancestors.last().filter(|ancestor| {
                ancestor.world.as_bytes() == &world.0 && ancestor.pin.as_bytes() == &pin.0
                    && ancestor.logical == logical && cut.matches_basis(ancestor.basis)
            }).ok_or_else(unavailable)?;
            if prior.until != retain_until || prior.owner != retained.owner { return Err(unavailable()); }
            (ancestor.capture, ancestor.suffix)
        } else {
            let (capture, suffix) = copy_fork_suffix(
                Arc::clone(&directory), archive, keys, hosted_task, agent, deployment,
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
            prior.until > now() && prior.ancestors.iter().any(|ancestor| ancestor.suffix == suffix));
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
                let publication = rtdb_query::read_owned_world_clock_publication(context, owner, world).await?;
                if &archived != original || publication != Some((logical, basis)) {
                    return Err(unavailable());
                }
                Ok(())
            },
        ).await?;
        let ancestor = rtdb_directory::CheckpointWorldAncestor {
            world: Uuid::from_bytes(world.0), pin: Uuid::from_bytes(pin.0), capture, suffix,
            manifest_sha256: manifest, basis, publication: basis, source_root: original.basis, logical,
        };
        (retained.ancestors.clone(), ancestor, until)
    };
    let owner = &retained.owner;
    let reopened = open_world(directory.as_ref(), archive, session, deployment, Uuid::from_bytes(world.0)).await?;
    if reopened.record != original || now() >= until { return Err(unavailable()); }
    if inherited_at.is_some() {
        reopened.snapshot.check_overlay_standing(&original).await?;
    } else {
        let refreshed = reopened.snapshot.fork_overlay_cut(cut.scope(), &original, ancestor.publication, None).await?;
        if refreshed.overlay() != cut.overlay() || !refreshed.matches_basis(ancestor.basis) {
            return Err(unavailable());
        }
    }
    if now() >= until { return Err(unavailable()); }
    ancestors.push(ancestor.clone());
    let binding = CheckpointWorldRetention {
        fork_reservation: Some(WorldForkReservation::Prepared),
        fork_request_link: None,
        world: child, ancestors, owner: owner.clone(),
        capture: retained.capture, manifest_sha256: retained.manifest_sha256.clone(),
        schema_digest: retained.schema_digest, suffix: retained.suffix, basis: retained.basis,
        parent_publication: retained.parent_publication, mode, until: retain_until,
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
            return if same_fork_binding(prior, &binding) { Ok(()) } else { Err(unavailable()) };
        }
        if current.checkpoint_worlds.len() >= MAX_RETAINED_WORLDS { return Err(unavailable()); }
        current.checkpoint_worlds.insert(child, binding.clone());
        Ok(())
    }).await?;
    Ok(rtdb_flight_sql::PreparedWorldOverlay {
        world: world.0, parent_pin: pin.0, capture: *ancestor.capture.as_bytes(), suffix: *ancestor.suffix.as_bytes(),
        manifest: ancestor.manifest_sha256, basis: ancestor.basis, publication: ancestor.publication,
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
            Arc::clone(&directory), archive, keys, hosted_task, agent, deployment,
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
    let held = session.open_owned_read().await?;
    let staged = held.replayed_world(binding, iid).await?;
    let scope = session.placed_history()?.ok_or_else(unavailable)?;
    let parent = prepare_checkpoint_replay_parent(
        Arc::clone(&directory), archive, storage, agent, keys, hosted_task, session,
        deployment, binding, Some(staged.world.basis), Some(&staged.world.schemas), resources.clone(),
    ).await?.ok_or_else(unavailable)?;
    let mut ancestors = Vec::with_capacity(staged.world.ancestors.len());
    let mut floor = parent.basis;
    for original in &staged.world.ancestors {
        let archived = prepare_checkpoint_replay_parent(
            Arc::clone(&directory), archive, storage, agent, keys, hosted_task, session,
            deployment, binding, Some(original.basis), None, resources.clone(),
        ).await?.ok_or_else(unavailable)?;
        if archived.basis.tx_id <= parent.basis.tx_id || !parent.basis.at_or_before(archived.basis) {
            return Err(unavailable());
        }
        let suffix = archived.suffix.map(Uuid::from_bytes).ok_or_else(unavailable)?;
        let capture = Uuid::from_bytes(archived.capture);
        let (selected, _) = held.replayed_parent_cut(binding, original.publication, scope).await?;
        let publication = selected.positioned_basis();
        if floor.tx_id >= publication.tx_id || !floor.at_or_before(publication)
            || !publication.at_or_before(archived.basis)
            || (publication == archived.basis) != (original.publication == original.basis)
        {
            return Err(unavailable());
        }
        let source_world = Iid(original.world);
        let owner = &owner;
        let schemas = &staged.world.schemas;
        with_registered_capture_context(
            directory.as_ref(), archive, keys, hosted_task,
            CaptureSelection {
                deployment, capture, manifest: &archived.manifest, suffix: Some(suffix),
                expected_basis: Some(archived.basis), require_replay: false,
            },
            resources.clone(),
            |context, _| async move {
                let (record, logical, _charge) = rtdb_query::read_held_world_publication(
                    &context, owner, source_world, publication,
                ).await?;
                if record.pin.0 != original.pin || record.basis != original.source_root
                    || &record.schemas != schemas || logical != original.logical
                {
                    return Err(unavailable());
                }
                Ok(())
            },
        ).await?;
        floor = publication;
        ancestors.push(rtdb_directory::CheckpointWorldAncestor {
            world: Uuid::from_bytes(original.world), pin: Uuid::from_bytes(original.pin),
            capture, suffix, manifest_sha256: archived.manifest, basis: archived.basis, publication,
            source_root: original.source_root, logical: original.logical,
        });
    }
    let live = session.open_owned_read().await?;
    let current = live.replayed_world(binding, iid).await?;
    let world = current.world;
    if world != staged.world || current.source_cut != staged.source_cut
        || current.destination_parent != staged.destination_parent
        || parent.basis != current.destination_parent
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
        fork_reservation: None,
        fork_request_link: None,
        ancestors,
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
                || existing.ancestors != retained.ancestors
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
        ancestors: world_ancestors(retained),
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

fn world_ancestors(retained: &CheckpointWorldRetention) -> Vec<rtdb_types::OwnedWorldAncestor> {
    retained.ancestors.iter().map(|ancestor| rtdb_types::OwnedWorldAncestor {
        world: *ancestor.world.as_bytes(),
        pin: *ancestor.pin.as_bytes(),
        capture: *ancestor.capture.as_bytes(),
        suffix: *ancestor.suffix.as_bytes(),
        manifest: ancestor.manifest_sha256.clone(),
        basis: ancestor.basis,
        publication: ancestor.publication,
        source_root: ancestor.source_root,
        logical: ancestor.logical,
    }).collect()
}

fn world_record(
    pin: Uuid,
    retained: &CheckpointWorldRetention,
    schemas: rtdb_types::WorldSchemas,
) -> OwnedWorld {
    OwnedWorld {
        ancestors: world_ancestors(retained),
        fork_lineage: Vec::new(),
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
    let reservation = WorldForkReservation::Registering(Uuid::new_v4());
    if let Some((world, pin)) = fork.cut.retained_parent() {
        let unavailable = OwnedWorld::unavailable;
        let child = Uuid::from_bytes(fork.world.0);
        let source_pin = Uuid::from_bytes(pin.0);
        if !capture.is_nil() || suffix.is_some() || child.is_nil() || fork.world != fork.pin
            || until <= now() || fork.cut.until() <= now()
            || session.placed_history()? != Some(fork.cut.scope())
        {
            return Err(unavailable());
        }
        let opened = open_world(directory.as_ref(), archive, session, deployment_id, Uuid::from_bytes(world.0)).await?;
        if opened.record.pin != pin || !fork.cut.matches_basis(opened.record.basis)
            || source.checkpoint_worlds.get(&source_pin) != Some(&opened.retained)
        {
            return Err(unavailable());
        }
        opened.snapshot.check_overlay_standing(&opened.record).await?;
        let retained = opened.retained.clone();
        let mode = match fork.requested {
            rtdb_node::WorldClockRequest::Retained => WorldClockMode::Retained,
            rtdb_node::WorldClockRequest::Held { start: None } => {
                WorldClockMode::Held { start: opened.record.inherited_clock() }
            }
            _ => return Err(unavailable()),
        };
        let binding = CheckpointWorldRetention { fork_reservation: None, fork_request_link: None, world: child, until, mode, ..retained.clone() };
        let mut registration = world_record(child, &binding, opened.record.schemas.clone());
        if !opened.record.can_retain_parent(&registration) { return Err(unavailable()); }
        registration.fork_lineage = opened.record.retained_child_lineage(&registration, opened.registered)?;
        drop(opened);
        let installed = update_installation(directory.as_ref(), &deployment, |current| {
            let active = now();
            if active >= fork.cut.until() || active >= until
                || current.incarnation != source.incarnation || current.log_epoch != source.log_epoch
                || PlacedAttemptScope::new(&current.stream("replica"), current.epoch)? != fork.cut.scope()
                || current.checkpoint_worlds.get(&source_pin) != Some(&retained)
            {
                return Err(unavailable());
            }
            admit_world_media(current, archive, deployment_id, &binding)?;
            current.checkpoint_worlds.retain(|_, world| world.until > active);
            reserve_fork_pin(current, child, &binding, reservation, false)
        }).await?;
        let (key, _) = register_fork_pin(directory.as_ref(), &deployment, &installed, reservation,
            session, &registration, fork).await?;
        return Ok((child, key));
    }
    if let Some((world, pin, logical)) = fork.cut.overlay() {
        let unavailable = OwnedWorld::unavailable;
        let child = Uuid::from_bytes(fork.world.0);
        let retained = source.checkpoint_worlds.get(&child).ok_or_else(unavailable)?;
        let ancestor = retained.ancestors.last().ok_or_else(unavailable)?;
        let mode = match fork.requested {
            rtdb_node::WorldClockRequest::Retained => WorldClockMode::Retained,
            rtdb_node::WorldClockRequest::Held { start: None } => WorldClockMode::Held { start: logical },
            _ => return Err(unavailable()),
        };
        if !capture.is_nil() || suffix.is_some() || child.is_nil() || retained.world != child
            || fork.world != fork.pin || retained.owner != owner || retained.until != until
            || retained.mode != mode || fork.cut.until() <= now() || until <= now()
            || ancestor.logical != logical || !fork.cut.admits_overlay(ancestor.basis, ancestor.publication)
            || session.placed_history()? != Some(fork.cut.scope())
        {
            return Err(unavailable());
        }
        let source_retention = source.checkpoint_worlds.get(&Uuid::from_bytes(pin.0)).ok_or_else(unavailable)?;
        let live = session.open_owned_read().await?;
        let (registered, registered_at, _charge) = live.world_registration_with_revision(world)
            .await?.ok_or_else(unavailable)?;
        let mut registration = world_record(child, retained, registered.schemas.clone());
        if registered.pin != pin || !matches_retention(&registered, source_retention)
            || registered.schemas.digest() != retained.schema_digest || !registered.can_retain_overlay(&registration)
        {
            return Err(unavailable());
        }
        registration.fork_lineage = registered.retained_child_lineage(&registration, registered_at)?;
        admit_world_media(&source, archive, deployment_id, retained)?;
        live.check()?;
        drop(live);
        let installed = update_installation(directory.as_ref(), &deployment, |current| {
            if current.incarnation != source.incarnation || current.log_epoch != source.log_epoch
                || PlacedAttemptScope::new(&current.stream("replica"), current.epoch)? != fork.cut.scope()
                || retained.until <= now() || fork.cut.until() <= now()
            {
                return Err(unavailable());
            }
            reserve_fork_pin(current, child, retained, reservation, true)
        }).await?;
        let (key, _) = register_fork_pin(directory.as_ref(), &deployment, &installed, reservation,
            session, &registration, fork).await?;
        return Ok((child, key));
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
                || (matches!(fork.requested, rtdb_node::WorldClockRequest::Proposal | rtdb_node::WorldClockRequest::Held { start: None })
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
                matches_fork_checkpoint(&source, &fork.cut, checkpoint) && checkpoint.root_sha256.is_some()
            ) {
                let capture = copy_published_fork_input(
                    Arc::clone(&directory), storage, archive, keys, hosted_task, deployment_id,
                    &source, &fork.cut, deadline,
                ).await?;
                (capture, None)
            } else {
                copy_fork_suffix(
                    Arc::clone(&directory), archive, keys, hosted_task,
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
    let expected_basis = expected_basis.or(Some(fork.cut.positioned_basis()));
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
    if (basis.system_time > head.system_time || basis.tx_id >= head.tx_id)
        && (!fork.cut.is_publication() || basis != head)
    {
        return Err(OwnedWorld::unavailable());
    }
    drop(live);
    let parent = CheckpointReadWorld { deployment: deployment_id, capture, manifest_sha256: manifest, suffix, basis };

    if !fork.cut.matches_basis(basis)
        || parent_publication.is_some_and(|publication| publication.tx_id > basis.tx_id)
        || (fork.cut.is_publication() && parent_publication != Some(basis))
    {
        return Err(OwnedWorld::unavailable());
    }
    // A renewal keeps the parent already admitted at this exact archive.
    let (schemas, parent_publication, fork_lineage) = match predecessor {
        Some((prior, _charge)) if schemas.includes(&prior.schemas) => {
            (prior.schemas, prior.parent_publication, prior.fork_lineage)
        }
        Some(_) => return Err(OwnedWorld::unavailable()),
        None => (schemas, parent_publication.map(|publication| publication.system_time), Vec::new()),
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
        fork_reservation: None,
        fork_request_link: None,
        ancestors: Vec::new(),
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
                    previous.basis.system_time > retained.basis.system_time || previous.basis.tx_id >= retained.basis.tx_id
                }
            {
                return Err(OwnedWorld::unavailable());
            }
        }
        reserve_fork_pin(record, pin, &retained, reservation, false)
    }).await?;
    let mut registration = world_record(pin, &retained, schemas);
    registration.fork_lineage = fork_lineage;
    let previous = if fork.sibling.is_some() { None } else { fork.previous.map(|previous| Uuid::from_bytes(previous.0)) };
    let (key, registered) = register_fork_pin(directory.as_ref(), &deployment, &installed, reservation,
        session, &registration, fork).await?;
    if !registered { return Ok((id, key)); }
    if let Some(previous) = previous
        && let Some(prior) = installed.checkpoint_worlds.get(&previous)
    {
        // Confirm the new pin is still live before retiring its predecessor.
        if let Ok(held) = session.open_owned_read().await
            && let Ok(Some((current, _))) = held.world_registration(registration.id).await
            && current.pin == registration.pin && !current.dropped
        {
            let _ = retire_world_pins(directory.as_ref(), &deployment, &[(previous, prior)], installed.incarnation, installed.log_epoch).await;
        }
    }
    Ok((id, key))
}

fn same_fork_binding(existing: &CheckpointWorldRetention, requested: &CheckpointWorldRetention) -> bool {
    let mut requested = requested.clone();
    requested.fork_reservation = existing.fork_reservation;
    requested.fork_request_link = existing.fork_request_link;
    existing == &requested
}

fn fork_request_link_for(
    installation: &DeploymentRecord, pin: Uuid, token: Uuid, binding: &CheckpointWorldRetention,
) -> WorldForkRequestLink {
    let mut key_hasher = ring::digest::Context::new(&ring::digest::SHA256);
    key_hasher.update(b"rtdb.fork-pin.v8-request-key.v1\0");
    key_hasher.update(&installation.incarnation.to_be_bytes());
    key_hasher.update(&installation.log_epoch.to_be_bytes());
    key_hasher.update(pin.as_bytes());
    key_hasher.update(token.as_bytes());
    let mut request_key = [0u8; 32];
    request_key.copy_from_slice(key_hasher.finish().as_ref());

    let mut digest_hasher = ring::digest::Context::new(&ring::digest::SHA256);
    digest_hasher.update(b"rtdb.fork-pin.v8-request-digest.v1\0");
    digest_hasher.update(&request_key);
    digest_hasher.update(binding.world.as_bytes());
    digest_hasher.update(binding.owner.as_bytes());
    digest_hasher.update(binding.capture.as_bytes());
    digest_hasher.update(&binding.basis.tx_id.0.to_be_bytes());
    digest_hasher.update(&binding.until.0.to_be_bytes());
    let mut request_digest = [0u8; 32];
    request_digest.copy_from_slice(digest_hasher.finish().as_ref());

    WorldForkRequestLink { request_key, request_digest, retain_until: binding.until }
}

fn reserve_fork_pin(
    installation: &mut DeploymentRecord, pin: Uuid, binding: &CheckpointWorldRetention,
    reservation: WorldForkReservation, prepared: bool,
) -> Result<()> {
    let unavailable = OwnedWorld::unavailable;
    let link = match reservation {
        WorldForkReservation::Registering(token) => Some(fork_request_link_for(installation, pin, token, binding)),
        WorldForkReservation::Prepared => None,
    };
    if let Some(current) = installation.checkpoint_worlds.get_mut(&pin) {
        if !same_fork_binding(current, binding) { return Err(unavailable()); }
        return match current.fork_reservation {
            None => Ok(()),
            Some(WorldForkReservation::Prepared) if prepared => {
                current.fork_reservation = Some(reservation);
                current.fork_request_link = link;
                Ok(())
            }
            Some(_) => Err(unavailable()),
        };
    }
    if prepared || installation.checkpoint_worlds.len() >= MAX_RETAINED_WORLDS {
        return Err(unavailable());
    }
    let mut binding = binding.clone();
    binding.fork_reservation = Some(reservation);
    binding.fork_request_link = link;
    installation.checkpoint_worlds.insert(pin, binding);
    Ok(())
}

async fn register_fork_pin(
    directory: &dyn Directory, deployment: &str, installed: &DeploymentRecord,
    reservation: WorldForkReservation, session: &mut NativeSession, world: &OwnedWorld,
    fork: rtdb_node::OwnedWorldFork,
) -> Result<(TransactionKey, bool)> {
    let unavailable = OwnedWorld::unavailable;
    let pin = Uuid::from_bytes(world.pin.0);
    let binding = installed.checkpoint_worlds.get(&pin).ok_or_else(unavailable)?;
    let WorldForkReservation::Registering(token) = reservation else { return Err(unavailable()); };
    let owns_pin = binding.fork_reservation == Some(reservation);
    let has_alternative = fork.root.as_ref().is_some_and(|root| !root.refusal.is_empty());
    let outcome = match admit_session(session, installed, &binding.owner) {
        Ok(()) => session.register_owned_world_checked(world, fork).await?,
        Err(error) => OwnedWorldRegistrationOutcome::Refused(error),
    };
    let (result, disposition) = match outcome {
        OwnedWorldRegistrationOutcome::Refused(error) => (Err(error), ForkDisposition::NeverSubmitted),
        OwnedWorldRegistrationOutcome::Ordered(outcome) => {
            let key = outcome.tx_key();
            let floor = owns_pin.then(|| {
                let history = Uuid::parse_str(deployment).map_err(|_| unavailable())?;
                AppliedReadFloor::new(history, vec![key])
            }).transpose()?;
            let inspect_world = outcome.committed() && has_alternative;
            let (registered, certified) = match (floor.as_ref(), inspect_world) {
                (Some(floor), true) => {
                    let held = session.open_applied_owned_read(floor).await?;
                    (held.registered_world_at(world, key).await?, true)
                }
                (Some(floor), false) => {
                    let confirmed = session.confirm_hosted_source_prefix(floor.history(), Some(floor)).await?;
                    (outcome.committed(), confirmed.is_some())
                }
                (None, true) => (session.open_owned_read().await?.registered_world_at(world, key).await?, false),
                (None, false) => (outcome.committed(), false),
            };
            if owns_pin && !certified { return Err(unavailable()); }
            let result = if outcome.committed() {
                Ok((key, registered))
            } else {
                Err(outcome.error().cloned().unwrap_or_else(unavailable))
            };
            (result, ForkDisposition::Ordered { registered, certified })
        }
    };
    if owns_pin {
        let original = fork_pin_key(installed, pin, token);
        let settled = update_installation(directory, deployment, |current| {
            settle_fork_pin(current, original, binding, disposition);
            Ok(())
        }).await?;
        if settled.checkpoint_worlds.get(&pin).is_some_and(|binding| binding.fork_reservation == Some(reservation)) {
            return Err(unavailable());
        }
    }
    result
}

fn fork_pin_key(installation: &DeploymentRecord, pin: Uuid, token: Uuid) -> ForkPin {
    ForkPin {
        incarnation: installation.incarnation,
        log_epoch: installation.log_epoch,
        pin: pin.as_u128(),
        reservation: token.as_u128(),
    }
}

fn settle_fork_pin(
    current: &mut DeploymentRecord, original: ForkPin, expected: &CheckpointWorldRetention,
    disposition: ForkDisposition,
) {
    let pin = Uuid::from_u128(original.pin);
    let Some(binding) = current.checkpoint_worlds.get(&pin) else { return; };
    let Some(WorldForkReservation::Registering(token)) = binding.fork_reservation else { return; };
    let observed = fork_pin_key(current, pin, token);
    match select_pin_write(original, observed, binding == expected, disposition) {
        Some(ForkPinWrite::Publish(pin)) => {
            if let Some(binding) = current.checkpoint_worlds.get_mut(&Uuid::from_u128(pin)) {
                binding.fork_reservation = None;
                binding.fork_request_link = None;
            }
        }
        Some(ForkPinWrite::Release(pin)) => { current.checkpoint_worlds.remove(&Uuid::from_u128(pin)); }
        None => {}
    }
}

/// Replacement-process pin reconciliation: reads the durable V8 decision record
/// linked from `CheckpointWorldRetention::fork_request_link` and retries the
/// exact directory Publish or Release compare-and-set once source confirmation
/// and content-complete image apply cover the original transaction.
pub(crate) fn reconcile_fork_pin_reservation(
    current: &mut DeploymentRecord,
    original: ForkPin,
    expected: &CheckpointWorldRetention,
    record: Option<&ForkDecisionRecord>,
    source: CertifiedSourceImage,
) {
    let pin = Uuid::from_u128(original.pin);
    let Some(binding) = current.checkpoint_worlds.get(&pin) else { return; };
    let Some(WorldForkReservation::Registering(token)) = binding.fork_reservation else { return; };
    let Some(link) = binding.fork_request_link else { return; };
    let observed = fork_pin_key(current, pin, token);
    match recover_pin_write(
        original, observed, binding == expected, &link.request_key, &link.request_digest, record, source,
    ) {
        Some(ForkPinWrite::Publish(pin)) => {
            if let Some(binding) = current.checkpoint_worlds.get_mut(&Uuid::from_u128(pin)) {
                binding.fork_reservation = None;
                binding.fork_request_link = None;
            }
        }
        Some(ForkPinWrite::Release(pin)) => { current.checkpoint_worlds.remove(&Uuid::from_u128(pin)); }
        None => {}
    }
}

pub(crate) fn matches_retention(world: &OwnedWorld, retained: &CheckpointWorldRetention) -> bool {
    world.ancestors == world_ancestors(retained)
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

fn checkpoint_read_world(deployment: Uuid, media: CheckpointWorldMedia<'_>) -> CheckpointReadWorld {
    CheckpointReadWorld {
        deployment, capture: media.capture, manifest_sha256: media.manifest_sha256.to_owned(),
        suffix: media.suffix, basis: media.basis,
    }
}

fn parent_media(deployment: Uuid, retained: &CheckpointWorldRetention) -> CheckpointReadWorld {
    checkpoint_read_world(deployment, retained.parent_media())
}

fn ancestor_media(deployment: Uuid, ancestor: &rtdb_directory::CheckpointWorldAncestor) -> CheckpointReadWorld {
    checkpoint_read_world(deployment, ancestor.media())
}

fn admit_world_media(
    installation: &DeploymentRecord, archive: &CellStorage, deployment: Uuid, retained: &CheckpointWorldRetention,
) -> Result<()> {
    for media in retained.media() {
        admit_capture(installation, archive, &checkpoint_read_world(deployment, media))?;
    }
    Ok(())
}

struct OpenWorld {
    incarnation: u64,
    log_epoch: i32,
    writer: PlacedAttemptScope,
    storage_generation: Option<Uuid>,
    retained: CheckpointWorldRetention,
    snapshot: OwnedReadSession,
    record: OwnedWorld,
    registered: Timestamp,
    publication: Option<(Timestamp, TransactionKey)>,
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
    let (world, registered, charge) = snapshot
        .world_registration_with_revision(Iid(*id.as_bytes()))
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
        || registered > snapshot.present().system_time
        || world.fork_lineage.last().is_some_and(|link| link.registered >= registered)
        || snapshot.present().system_time >= retained.until
        || !matches_retention(&world, retained)
    {
        return Err(OwnedWorld::unavailable());
    }
    let publication = snapshot.world_application_publication(world.id).await?;
    if publication.is_some_and(|(reading, revision)| {
        revision.tx_id > snapshot.present().tx_id || revision.system_time > snapshot.present().system_time
            || !world.admits_clock_publication(reading, revision.system_time)
    }) {
        return Err(OwnedWorld::unavailable());
    }
    Ok(OpenWorld {
        incarnation: installation.incarnation,
        log_epoch: installation.log_epoch,
        writer: PlacedAttemptScope::new(&installation.stream("replica"), installation.epoch)?,
        storage_generation: installation.storage_generation,
        retained: retained.clone(),
        snapshot,
        record: world,
        registered,
        publication,
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


pub(crate) async fn commit_checkpoint_world_batch(
    directory: &dyn Directory, archive: &CellStorage, session: &mut NativeSession, deployment: Uuid,
    sections: Vec<rtdb_node::OwnedWorldSection>, root: rtdb_node::OwnedRootSection,
    observed: rtdb_node::OwnedWorldObservations, fingerprint: [u8; 32],
    controls: Vec<rtdb_node::HostedControlWrite>, deadline: Timestamp,
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
            || now() >= retained.until || (!retained.mode.is_writable() && !matches!(&section.effect, rtdb_node::OwnedWorldEffect::Drop))
        {
            return Err(OwnedWorld::unavailable());
        }
        admit_world_media(&installation, archive, deployment, retained)?;
        if matches!(&section.effect, rtdb_node::OwnedWorldEffect::Drop) {
            terminal.push((pin, retained));
        }
        admitted.push((retained.until, section));
    }
    let deciding_landing = !root.refusal.is_empty();
    let outcome = session.commit_owned_world_batch(admitted, root, observed, fingerprint, controls, deadline).await?;
    if !outcome.committed() {
        return Err(outcome.error().cloned().unwrap_or_else(OwnedWorld::unavailable));
    }
    if deciding_landing {
        // The same commit can retain the Proposal and record a refusal.
        // Retire its pins only when an ordered drop is actually present.
        terminal = match (terminal.pop(), session.open_owned_read().await) {
            (Some((pin, retained)), Ok(held)) => {
                let id = rtdb_types::Iid(*retained.world.as_bytes());
                match held.world_registration(id).await {
                    Ok(Some((world, _))) if world.dropped && world.pin.0 == *pin.as_bytes() => vec![(pin, retained)],
                    _ => Vec::new(),
                }
            }
            _ => Vec::new(),
        };
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
    row: RecordBatch,
    revision: Timestamp,
    assertion: Option<OwnedWorldAssertion>,
    authoring_world: Option<Iid>,
    lifetime: Option<Interval>,
    reservation: Arc<MemoryReservation>,
}

impl WorldFact {
    pub(crate) fn into_point_row(self) -> rtdb_flight_sql::WorldPointRow {
        rtdb_flight_sql::WorldPointRow { batch: self.row, revision: self.revision, assertion: self.assertion, reservation: self.reservation }
    }

    fn validate(&self, schema: &rtdb_types::OwnedSchema) -> Result<()> {
        let types = self.row.schema_ref().fields().iter().zip(self.row.columns()).map(|(field, array)| {
            let cell = ArrowCell { field, array: array.as_ref(), row: 0 };
            Ok((field.name().clone(), cell.logical_type()?))
        }).collect::<Result<_>>()?;
        schema.validate_types(&types)
    }

    fn document_bytes(&self) -> Result<usize> {
        rtdb_flight_sql::WorldPointRow::document_bytes(&self.row)
    }

    fn select_path(&mut self, names: &[String], trim: bool) -> Result<()> {
        let mut fields = Vec::new();
        let mut arrays = Vec::new();
        for (field, array) in self.row.schema_ref().fields().iter().zip(self.row.columns()) {
            if !trim || field.name() == "_id" || names.contains(field.name()) {
                fields.push(Arc::clone(field));
                arrays.push(Arc::clone(array));
            }
        }
        for name in names {
            if !fields.iter().any(|field| field.name() == name) {
                fields.push(Arc::new(Field::new(name, DataType::Null, true)));
                arrays.push(Arc::new(NullArray::new(1)) as ArrayRef);
            }
        }
        self.row = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).map_err(|_| OwnedWorld::unavailable())?;
        Ok(())
    }

    fn into_shadow(mut self, line: Iid) -> Result<Option<Self>> {
        let unavailable = OwnedWorld::unavailable;
        let schema = self.row.schema_ref();
        let identity = schema.index_of("_id").map_err(|_| unavailable())?;
        let marker = schema.index_of(WorldShadow::FIELDS).map_err(|_| unavailable())?;
        let field = schema.field(marker);
        let array = self.row.column(marker);
        let marker_cell = ArrowCell { field, array: array.as_ref(), row: 0 };
        if marker_cell.is_undefined()? { return Err(unavailable()); }
        let deleted = marker_cell.is_null()?;
        let mut selected = vec![identity];
        if !deleted {
            let fields = rtdb_columnar::utf8_list(field, array.as_ref(), 0).map_err(|_| unavailable())?.ok_or_else(unavailable)?;
            for name in fields {
                let name = name.map_err(|_| unavailable())?;
                if name.starts_with('_') { return Err(unavailable()); }
                let index = schema.index_of(name).map_err(|_| unavailable())?;
                let cell = ArrowCell { field: schema.field(index), array: self.row.column(index).as_ref(), row: 0 };
                if selected.contains(&index) || cell.is_undefined()? { return Err(unavailable()); }
                selected.push(index);
            }
        }
        for (index, field) in schema.fields().iter().enumerate() {
            if index == marker || selected.contains(&index) { continue; }
            let cell = ArrowCell { field, array: self.row.column(index).as_ref(), row: 0 };
            if !cell.is_null()? { return Err(unavailable()); }
        }
        if deleted { return Ok(None); }
        self.row = self.row.project(&selected).map_err(|_| unavailable())?;
        self.authoring_world = Some(line);
        if let Some(assertion) = &mut self.assertion {
            assertion.line = Some(line);
        }
        Ok(Some(self))
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
            let assertion = rtdb_query::owned_row_assertion_ordinal(batch, row)?
                .map(|ordinal| OwnedWorldAssertion { line: None, ordinal, held_reading: None });
            let lifetime = lifetime.map(|duration| {
                let (created, expires) = rtdb_query::owned_row_lifetime(batch, row, revision, duration)?;
                Interval::new(created, expires)
            }).transpose()?;
            let columns = || batch.schema_ref().fields().iter().enumerate().filter_map(|(index, field)| {
                (!matches!(field.name().as_str(), "_system_from" | "_assertion_ordinal" | rtdb_types::OWNED_EVENT_POSITION_COLUMN)
                    && !(lifetime.is_some() && matches!(field.name().as_str(), "_valid_from" | "_valid_to"))).then_some(index)
            });
            let charge = columns().try_fold(4096usize, |bound, index| {
                let field = batch.schema_ref().field(index);
                bound.checked_add(field.name().len() + 256).and_then(|bound| {
                    bound.checked_add(rtdb_columnar::estimate_read_value_bytes(field, batch.column(index).as_ref(), row).ok()?)
                }).ok_or_else(OwnedWorld::unavailable)
            })?;
            let reservation = memory.reserve(charge)?;
            let columns = columns().collect::<Vec<_>>();
            for &index in &columns {
                let field = batch.schema_ref().field(index);
                let logical = rtdb_columnar::from_field(field)?;
                rtdb_columnar::validate_readable_cell(field, batch.column(index).as_ref(), &logical, row, memory)?;
            }
            let row = batch.slice(row, 1).project(&columns).map_err(|_| OwnedWorld::unavailable())?;
            found = Some(WorldFact { row, revision, assertion, authoring_world: None, lifetime, reservation });
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

    async fn local_fact(&self, relation: &OwnedRelation, fact: WorldFact) -> Result<Option<WorldFact>> {
        let Some(mut fact) = fact.into_shadow(self.record.id)? else { return Ok(None); };
        if let Some(assertion) = &mut fact.assertion
            && matches!(self.record.mode, WorldClockMode::Held { .. })
        {
            let reading = self.snapshot.certify_world_accepted_scope(
                &self.record, std::slice::from_ref(relation), fact.revision,
            ).await?;
            assertion.held_reading = Some(reading);
        }
        Ok(Some(fact))
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
        let visible = self.local_fact(relation, fact).await?;
        Ok((Some(visible), read.witness))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refusing_a_fork_releases_only_its_own_directory_reservation() -> Result<()> {
        let mut installation = DeploymentRecord::new("application", "org", Vec::new(), 0);
        let pin = Uuid::new_v4();
        let token = Uuid::new_v4();
        let reservation = WorldForkReservation::Registering(token);
        let binding = CheckpointWorldRetention {
            fork_reservation: None, fork_request_link: None, world: pin, ancestors: Vec::new(), owner: "owner".into(),
            capture: Uuid::new_v4(), manifest_sha256: "manifest".into(), schema_digest: [0; 32],
            suffix: None, basis: TransactionKey { tx_id: rtdb_types::TxId(1), system_time: Timestamp(1) },
            parent_publication: None, mode: WorldClockMode::Retained, until: Timestamp(100),
        };
        let predecessor = Uuid::new_v4();
        installation.checkpoint_worlds.insert(predecessor, binding.clone());
        reserve_fork_pin(&mut installation, pin, &binding, reservation, false)?;
        let held = installation.checkpoint_worlds[&pin].clone();
        let original = fork_pin_key(&installation, pin, token);
        assert!(reserve_fork_pin(&mut installation, pin, &binding,
            WorldForkReservation::Registering(Uuid::new_v4()), false).is_err());

        let unconfirmed = ForkDisposition::Ordered { registered: false, certified: false };
        let refused = ForkDisposition::Ordered { registered: false, certified: true };
        let registered = ForkDisposition::Ordered { registered: true, certified: true };
        settle_fork_pin(&mut installation, original, &held, unconfirmed);
        assert_eq!(installation.checkpoint_worlds[&pin], held);
        let mut replacement = installation.clone();
        replacement.incarnation += 1;
        settle_fork_pin(&mut replacement, original, &held, refused);
        assert_eq!(replacement.checkpoint_worlds[&pin], held);
        settle_fork_pin(&mut installation, original, &held, refused);
        assert!(!installation.checkpoint_worlds.contains_key(&pin));
        assert_eq!(installation.checkpoint_worlds[&predecessor], binding);

        reserve_fork_pin(&mut installation, pin, &binding, reservation, false)?;
        settle_fork_pin(&mut installation, original, &held, registered);
        settle_fork_pin(&mut installation, original, &held, refused);
        assert_eq!(installation.checkpoint_worlds[&pin], binding);

        let prepared = Uuid::new_v4();
        let mut child = binding;
        child.fork_reservation = Some(WorldForkReservation::Prepared);
        child.world = prepared;
        installation.checkpoint_worlds.insert(prepared, child.clone());
        reserve_fork_pin(&mut installation, prepared, &child, reservation, true)?;
        let held = installation.checkpoint_worlds[&prepared].clone();
        let original = fork_pin_key(&installation, prepared, token);
        settle_fork_pin(&mut installation, original, &held, ForkDisposition::NeverSubmitted);
        assert!(!installation.checkpoint_worlds.contains_key(&prepared));
        Ok(())
    }

    #[test]
    fn world_shadow_keeps_the_winning_ordinal_on_its_authoring_line() -> Result<()> {
        let memory = MemoryBudget::new(1 << 20);
        let id = EntityId::String("item".into());
        let row = WorldShadow::Present(StructFields::from([("_id".into(), id.as_value())])).record(&id)?;
        let fields = row.iter().map(|(name, value)| rtdb_columnar::to_field(name, &value.logical_type())).collect::<Result<Vec<_>>>()?;
        let arrays = row.values().map(|value| rtdb_columnar::build_array(&value.logical_type(), std::slice::from_ref(value))).collect::<Result<Vec<_>>>()?;
        let row = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
        let ordinal = std::num::NonZeroU32::new(3).unwrap();
        let fact = WorldFact {
            row, revision: Timestamp(20), assertion: Some(OwnedWorldAssertion { line: None, ordinal, held_reading: None }), authoring_world: None,
            lifetime: None, reservation: memory.reserve(4096)?,
        };
        let line = Iid([7; 16]);
        let fact = fact.into_shadow(line)?.unwrap();
        assert_eq!(fact.revision, Timestamp(20));
        assert_eq!(fact.assertion, Some(OwnedWorldAssertion { line: Some(line), ordinal, held_reading: None }));
        assert_eq!(fact.row.num_columns(), 1);
        Ok(())
    }
}

#[cfg(test)]
mod world_point_tests {
    use super::*;
    use arrow_array::{Array, StringArray, TimestampMicrosecondArray};
    use rtdb_types::{LogicalType, ScalarType};

    #[test]
    fn world_point_shadow_distinguishes_a_tombstone_from_an_undefined_marker() -> Result<()> {
        let memory = MemoryBudget::new(1 << 20);
        let text = LogicalType::Scalar(ScalarType::Utf8);
        let line = Iid([7; 16]);
        let into_shadow = |marker: Value| -> Result<Option<WorldFact>> {
            let logical = marker.logical_type();
            let fields = vec![rtdb_columnar::to_field("_id", &text)?, rtdb_columnar::to_field(WorldShadow::FIELDS, &logical)?];
            let row = RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![
                Arc::new(StringArray::from(vec!["id"])),
                rtdb_columnar::build_array(&logical, &[marker])?,
            ]).unwrap();
            WorldFact { row, revision: Timestamp(2), assertion: None, authoring_world: None, lifetime: None, reservation: memory.reserve(4096)? }
                .into_shadow(line)
        };
        assert!(into_shadow(Value::Null)?.is_none());
        assert!(into_shadow(Value::Undefined).is_err());
        Ok(())
    }

    #[test]
    fn world_point_shadow_uses_the_marker_order_and_shares_selected_buffers() {
        let text = LogicalType::Scalar(ScalarType::Utf8);
        let nullable_text = text.clone().maybe();
        let marker = LogicalType::list(text.clone()).maybe();
        let label = Arc::new(StringArray::from(vec![Some("other"), Some("winning")]));
        let pointer = label.value(1).as_ptr();
        let fields = vec![
            rtdb_columnar::to_field("_id", &text).unwrap(),
            rtdb_columnar::to_field("unused", &nullable_text).unwrap(),
            rtdb_columnar::to_field("label", &text).unwrap(),
            rtdb_columnar::to_field("rank", &nullable_text).unwrap(),
            rtdb_columnar::to_field(WorldShadow::FIELDS, &marker).unwrap(),
            Field::new("_system_from", DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None), false),
        ];
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![
            Arc::new(StringArray::from(vec!["other", "id"])),
            Arc::new(StringArray::from(vec![None::<&str>, None])),
            label,
            Arc::new(StringArray::from(vec![None::<&str>, None])),
            rtdb_columnar::build_array(&marker, &[Value::Null, Value::list(vec![Value::Utf8("rank".into()), Value::Utf8("label".into())])]).unwrap(),
            Arc::new(TimestampMicrosecondArray::from(vec![1, 2])),
        ]).unwrap().slice(1, 1);
        let memory = MemoryBudget::new(1 << 20);
        let batch = OwnedBatch::new(batch, &memory).unwrap();
        let fact = decode_point(vec![batch], &EntityId::String("id".into()), &memory, None)
            .unwrap().unwrap().into_shadow(Iid([7; 16])).unwrap().unwrap();
        assert_eq!(fact.revision, Timestamp(2));
        assert_eq!(fact.row.schema_ref().fields().iter().map(|field| field.name().as_str()).collect::<Vec<_>>(), ["_id", "rank", "label"]);
        assert!(fact.row.column(1).is_null(0));
        let selected = ArrowCell::named(&fact.row, "label", 0).unwrap().utf8().unwrap();
        assert_eq!(selected, "winning");
        assert_eq!(selected.as_ptr(), pointer);
        drop(fact);
        assert_eq!(memory.used_bytes(), 0);
    }
}
