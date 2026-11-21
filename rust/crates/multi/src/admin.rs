//! Cell administration: creating, moving, rebalancing, forking and deleting deployments, and confirming dead members (`CreateDeployment`, `IntendMove`, `Rebalance`, `Fork`, `BeginDeletion`, `EraseAndProve`, `ExpireLease`).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

pub use rtdb_directory::DeletionRecord;
use rtdb_directory::{Cas, CellMove, CellMovePhase, DeletionFacts, DeploymentRecord, MoveNamespaces, Directory, Lifecycle, MemberId, MemberPool, MemberRecord, MemberRole, MemberState, PlacementMove, StorageNamespaces, effective_holder, now_ms};
use rtdb_logkeeper::StreamConfig;
use rtdb_storage::log::Log;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio_util::sync::CancellationToken;

use crate::control::{self, ControlKey, MoveTicket, Request, Response};
use crate::deployment::{CellStorage, ensemble_servers, live_holdings, require_storage_namespaces, select_logkeepers, storage_namespaces, write_quorum};
use crate::keys::KeyService;

const CONTROL_TIMEOUT: Duration = Duration::from_secs(35);

/// Creates a deployment, choosing its ensemble from the live logkeepers in its affinity order.
/// Allocates identity and nothing else: no objects, no log state, no compute.
pub async fn create_deployment(directory: &dyn Directory, name: &str, org: &str, replicas: usize) -> Result<DeploymentRecord> {
    let mut record = DeploymentRecord::new(name, org, Vec::new(), now_ms());
    record.ensemble = choose_ensemble(directory, &record, replicas).await?;
    directory.create_deployment(record).await
}

async fn choose_ensemble(directory: &dyn Directory, record: &DeploymentRecord, replicas: usize) -> Result<Vec<MemberId>> {
    let now = now_ms();
    let members = directory.members().await?;
    let logkeepers: Vec<_> = members.iter().filter(|member| member.role == MemberRole::Logkeeper && member.is_live(now)).collect();
    let selected = select_logkeepers(&record.storage_identity(), replicas, Vec::new(), &logkeepers);
    if selected.len() < replicas {
        return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/too-few-logkeepers", format!("{replicas} logkeeper addresses are needed and {} are live", selected.len())));
    }
    Ok(selected.into_iter().map(|member| member.id.clone()).collect())
}

async fn members(directory: &dyn Directory) -> Result<BTreeMap<MemberId, MemberRecord>> {
    Ok(directory.members().await?.into_iter().map(|member| (member.id.clone(), member)).collect())
}

async fn record(directory: &dyn Directory, name: &str) -> Result<DeploymentRecord> {
    directory.deployment_by_name(name).await?.ok_or_else(|| Anomaly::incorrect("rtdb.multi/unknown-deployment", format!("No deployment is named {name}")))
}

/// Applies `change` to the current record, replacing it conditionally if it changed.
async fn update(directory: &dyn Directory, name: &str, change: impl Fn(&mut DeploymentRecord) -> Result<()>) -> Result<DeploymentRecord> {
    for _ in 0..8 {
        let current = record(directory, name).await?;
        let mut next = current.clone();
        change(&mut next)?;
        if next == current { return Ok(current); }
        if let Cas::Applied(stored) = directory.replace_deployment(next, current.version).await? { return Ok(stored); }
    }
    Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/update-contended", format!("{name} kept changing while being updated")))
}

async fn replica_servers(directory: &dyn Directory, record: &DeploymentRecord) -> Result<Vec<String>> {
    match &record.move_from {
        Some(previous) => Ok(previous.clone()),
        None => ensemble_servers(record, &members(directory).await?),
    }
}

fn bind_admin_storage(record: &mut DeploymentRecord, namespaces: StorageNamespaces) -> Result<()> {
    if record.storage_namespaces.is_none() && record.lifecycle == Lifecycle::Live && record.epoch == 0
        && record.holder.is_none() && record.checkpoint.is_none() && record.parent.is_none()
        && record.storage_generation.is_none() && record.log_epoch == 0 && record.move_from.is_none()
    {
        record.storage_namespaces = Some(namespaces);
    }
    require_storage_namespaces(record, namespaces)
}

fn control_address(members: &BTreeMap<MemberId, MemberRecord>, member: &MemberId) -> Result<String> {
    members.get(member).and_then(|member| member.control_address.clone()).ok_or_else(|| Anomaly::incorrect("rtdb.multi/no-control", format!("Member {member} has no control address")))
}

async fn expect(address: &str, request: Request, control: &ControlKey) -> Result<Response> {
    match control::call(address, &request, CONTROL_TIMEOUT, control).await? {
        Response::Refused { code, message } => Err(Anomaly::new(AnomalyCategory::Unavailable, code, message)),
        response => Ok(response),
    }
}

/// Makes sure the deployment is held, activating it on the first node of its affinity order if not, and returns its holder.
pub async fn ensure_held(directory: &dyn Directory, name: &str, control: &ControlKey) -> Result<MemberId> {
    let current = record(directory, name).await?;
    let members = members(directory).await?;
    if let Some(holder) = effective_holder(&current, &members, now_ms()) { return Ok(holder.clone()); }
    let now = now_ms();
    let nodes: Vec<&MemberRecord> = members.values().filter(|member| current.may_place_on(member, now)).collect();
    let mut last = None;
    for node in rtdb_affinity::rank(&current.storage_identity(), &nodes, |member| member.id.as_str()) {
        let Some(address) = &node.control_address else { continue };
        match expect(address, Request::Activate { name: name.to_owned() }, control).await {
            Ok(Response::Held { .. }) => return Ok(node.id.clone()),
            Ok(Response::Redirect { holder, .. }) => return Ok(MemberId(holder)),
            Ok(_) => {}
            Err(error) => last = Some(error),
        }
    }
    Err(last.unwrap_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/no-node", "No node can hold this deployment")))
}

/// Reserves the next claim for `target` during the handoff.
pub async fn move_deployment(directory: &dyn Directory, name: &str, target: &MemberId, control: &ControlKey) -> Result<()> {
    let members = members(directory).await?;
    let address = control_address(&members, target)?;
    let reservation = PlacementMove {
        request: uuid::Uuid::new_v4(),
        target: target.clone(),
        expires_at_ms: now_ms().saturating_add((3 * CONTROL_TIMEOUT).as_millis() as i64),
    };
    let intended = update(directory, name, |record| {
        let now = now_ms();
        if !members.get(target).is_some_and(|member| record.may_place_on(member, now)) {
            return Err(Anomaly::incorrect("rtdb.multi/not-placeable", format!("{target} may not take placements of {name}")));
        }
        if effective_holder(record, &members, now) == Some(target) { return Ok(()); }
        if record.active_move(now).is_some_and(|intent| members.get(&intent.target).is_some_and(|member| record.may_place_on(member, now))) {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/move-in-progress", format!("{name} already has a reserved placement")));
        }
        record.move_intent = None;
        record.placement_move = Some(reservation.clone());
        Ok(())
    }).await?;
    if effective_holder(&intended, &members, now_ms()) == Some(target) { return Ok(()); }
    if intended.placement_move.as_ref() != Some(&reservation) {
        return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/move-reservation-lost", "The directory did not retain the move reservation"));
    }

    let outcome = async {
        let ticket = MoveTicket { deployment: intended.id.clone(), request: reservation.request };
        if let Some(holder) = effective_holder(&intended, &members, now_ms()) {
            expect(&control_address(&members, holder)?, Request::ParkForMove { name: name.to_owned(), epoch: intended.epoch, ticket: ticket.clone() }, control).await?;
        }
        let activation = Request::ActivateForMove { name: name.to_owned(), ticket };
        let reserved_for = Duration::from_millis(reservation.expires_at_ms.saturating_sub(now_ms()).max(0) as u64);
        let retry_window = reserved_for.saturating_sub(CONTROL_TIMEOUT).min(CONTROL_TIMEOUT);
        match control::retry_pressure(retry_window, || expect(&address, activation.clone(), control)).await? {
            Response::Held { .. } => Ok(()),
            other => Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/move-failed", format!("The target answered {other:?}"))),
        }
    }.await;
    if outcome.is_err()
        && let Err(error) = clear_move_reservation(directory, &intended.id, reservation.request).await {
        eprintln!("RTDB could not clear the move reservation for {name}: {error}");
    }
    outcome
}

async fn clear_move_reservation(directory: &dyn Directory, id: &str, request: uuid::Uuid) -> Result<()> {
    for _ in 0..8 {
        let Some(mut current) = directory.deployment(id).await? else { return Ok(()) };
        if !current.placement_move.as_ref().is_some_and(|reservation| reservation.request == request) { return Ok(()); }
        current.placement_move = None;
        let version = current.version;
        if let Cas::Applied(_) = directory.replace_deployment(current, version).await? { return Ok(()); }
    }
    Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/update-contended", "The deployment kept changing while clearing its move reservation"))
}

/// Balances holdings among eligible nodes in each placement pool.
pub async fn rebalance(directory: &dyn Directory, moves: usize, control: &ControlKey) -> Result<usize> {
    let members = members(directory).await?;
    let deployments = live_holdings(directory, &members, now_ms()).await?;
    let now = now_ms();
    let mut moved = 0;
    for pool in [MemberPool::General, MemberPool::Isolation] {
        let mut load: BTreeMap<MemberId, Vec<&str>> = members.values()
            .filter(|member| member.pool == pool && member.may_receive_placements(now) && member.control_address.is_some())
            .map(|member| (member.id.clone(), Vec::new())).collect();
        for record in deployments.iter().filter(|record| record.writer_pool() == Some(pool)) {
            if let Some(held) = effective_holder(record, &members, now).and_then(|holder| load.get_mut(holder)) {
                held.push(&record.name);
            }
        }
        while moved < moves {
            let Some((busiest, busy)) = load.iter().max_by_key(|(_, held)| held.len()).map(|(id, held)| (id.clone(), held.len())) else { break };
            let Some((idlest, idle)) = load.iter().min_by_key(|(_, held)| held.len()).map(|(id, held)| (id.clone(), held.len())) else { break };
            if busy <= idle + 1 { break; }
            let name = load.get_mut(&busiest).and_then(Vec::pop).expect("the busiest node holds something");
            move_deployment(directory, name, &idlest, control).await?;
            load.get_mut(&idlest).expect("listed").push(name);
            moved += 1;
        }
    }
    Ok(moved)
}

pub async fn set_read_width(directory: &dyn Directory, name: &str, width: u32) -> Result<DeploymentRecord> {
    update(directory, name, |record| { record.read_width = width.max(1); Ok(()) }).await
}

pub async fn set_linearizable_reads(directory: &dyn Directory, name: &str, on: bool) -> Result<DeploymentRecord> {
    update(directory, name, |record| { record.linearizable_reads = on; Ok(()) }).await
}

pub async fn unquarantine(directory: &dyn Directory, name: &str) -> Result<DeploymentRecord> {
    update(directory, name, |record| {
        if record.lifecycle == Lifecycle::Quarantined {
            record.lifecycle = Lifecycle::Live;
            record.epoch += 1;
            record.holder = None;
            record.move_intent = None;
            record.placement_move = None;
        }
        record.activation_crashes = 0;
        Ok(())
    }).await
}

async fn fork_source(directory: &dyn Directory, parent: &str) -> Result<DeploymentRecord> {
    let source = record(directory, parent).await?;
    if !source.can_fork() {
        return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/fork-unavailable", format!("{parent} cannot be forked in its current state")));
    }
    Ok(source)
}

async fn fork_bucket(storage: &CellStorage, keys: &dyn KeyService, source: &DeploymentRecord) -> Result<crate::reference::Bucket> {
    let bucket = storage.bucket(rtdb_storage::IoRegion::new()).await?;
    require_storage_namespaces(source, storage_namespaces(bucket.as_ref(), keys).await?)?;
    Ok(bucket)
}

/// Forks `parent` into a new deployment in its lineage.
/// The parent is flushed, and the child references its objects and starts on fresh logs.
pub async fn fork(directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, parent: &str, name: &str, control: &ControlKey) -> Result<DeploymentRecord> {
    let holder = ensure_held(directory, parent, control).await?;
    let members = members(directory).await?;
    expect(&control_address(&members, &holder)?, Request::Flush { name: parent.to_owned() }, control).await?;
    let source = fork_source(directory, parent).await?;
    let bucket = fork_bucket(storage, keys, &source).await?;
    fork_with_tail(directory, bucket.as_ref(), keys, &source, name, None, Vec::new(), 4 << 20).await
}

/// Forks `parent` as of one of its past blocks: the fork's state is that block's, and what the parent wrote after it is not the fork's.
/// Only a block younger than the reaper's `grace` can be forked from: an object it needs that the parent released since is erased only once the grace has passed.
pub async fn fork_at_block(directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, parent: &str, name: &str, block: i64, grace: std::time::Duration) -> Result<DeploymentRecord> {
    let source = fork_source(directory, parent).await?;
    let bucket = fork_bucket(storage, keys, &source).await?;
    let age = now_ms() - crate::reference::written_at_ms(bucket.as_ref(), &source.storage_prefix(), block).await?;
    if age >= grace.as_millis() as i64 { return Err(Anomaly::incorrect("rtdb.multi/block-too-old", format!("Block {block} was written {}s ago, beyond the reaper's grace of {}s", age / 1000, grace.as_secs()))); }
    fork_with_tail(directory, bucket.as_ref(), keys, &source, name, Some(block), Vec::new(), 4 << 20).await
}

/// Forks `parent` as of a system time at or after its checkpoint: the fork starts from the checkpoint's block, with the parent's transactions after it up to `system_time` copied into its own stream.
/// A time before the checkpoint is a past block's, which `fork_at_block` forks from.
pub async fn fork_at_time(directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, parent: &str, name: &str, system_time: rtdb_types::Timestamp, grace: std::time::Duration, maximum_message_bytes: usize) -> Result<DeploymentRecord> {
    let source = fork_source(directory, parent).await?;
    let checkpoint = source.checkpoint.ok_or_else(|| Anomaly::incorrect("rtdb.multi/no-checkpoint", format!("{parent} has published no block to fork from")))?;
    if system_time.0 < checkpoint.system_time { return Err(Anomaly::incorrect("rtdb.multi/before-checkpoint", format!("{parent}'s last published block is after that time: fork at a past block instead"))); }
    let bucket = fork_bucket(storage, keys, &source).await?;
    if now_ms() - crate::reference::written_at_ms(bucket.as_ref(), &source.storage_prefix(), checkpoint.block).await? >= grace.as_millis() as i64 {
        return Err(Anomaly::incorrect("rtdb.multi/block-too-old", format!("{parent}'s last published block is older than the reaper's grace")));
    }
    let servers = replica_servers(directory, &source).await?;
    let stream = StreamConfig { write_quorum: write_quorum(servers.len()), servers, stream: source.stream("replica"), maximum_message_bytes };
    let reader = crate::logs::SealedLog::new(rtdb_logkeeper::open_reader(stream.clone(), CancellationToken::new()).await?, &keys.existing(&source.id)?, &stream.stream)?;
    let tail = crate::logs::transactions_until(&reader, checkpoint.boundary, system_time).await?;
    fork_with_tail(directory, bucket.as_ref(), keys, &source, name, Some(checkpoint.block), tail, maximum_message_bytes).await
}

async fn fork_with_tail(directory: &dyn Directory, bucket: &dyn rtdb_storage::ObjectStore, keys: &dyn KeyService, source: &DeploymentRecord, name: &str, at_block: Option<i64>, mut tail: Vec<rtdb_format::envelope::ReplicaEnvelope>, maximum_message_bytes: usize) -> Result<DeploymentRecord> {
    let mut child = DeploymentRecord::new(name, source.org.clone(), Vec::new(), now_ms());
    child.lineage = source.lineage.clone();
    child.parent = Some(source.id.clone());
    child.log_epoch = source.log_epoch + 1;
    child.storage_namespaces = source.storage_namespaces;
    // The parent's checkpoint names positions in the parent's logs, not the fork's: the fork's first holder finds its block by listing, and publishes the fork's own.
    child.ensemble = choose_ensemble(directory, &child, source.ensemble.len()).await?;
    let key = keys.existing(&source.id)?;
    let child_stream = if tail.is_empty() {
        None
    } else {
        // The first holder claims the next epoch and seals the fork's tail.
        child.epoch = 1;
        crate::logs::renumber_into(&mut tail, rtdb_types::StorageEpoch(child.log_epoch))?;
        let servers = replica_servers(directory, &child).await?;
        Some(StreamConfig { write_quorum: write_quorum(servers.len()), servers, stream: child.stream("replica"), maximum_message_bytes })
    };
    let prepared = async {
        crate::reference::fork(bucket, &source.storage_prefix(), &child.storage_prefix(), at_block).await?;
        keys.install(&child.id, key)?;
        if let Some(stream) = &child_stream {
            let writer = rtdb_logkeeper::open_writer(stream.clone(), child.epoch, CancellationToken::new()).await?;
            let writer = crate::logs::SealedLog::new(writer, &key, &stream.stream)?;
            for envelope in tail {
                writer.enqueue(bytes::Bytes::from(envelope.encode()), rtdb_types::Timestamp(now_ms() * 1000)).await?.durable().await?;
            }
        }
        Ok(())
    }.await;
    let published = match prepared {
        Ok(()) => publish_fork(directory, source, &child).await,
        Err(error) => Err(ForkPublicationError::Rejected(error)),
    };
    match published {
        Ok(stored) => Ok(stored),
        Err(ForkPublicationError::Uncertain(error)) => Err(error),
        Err(ForkPublicationError::Rejected(error)) => {
            if let Err(cleanup) = discard_fork(bucket, keys, &child, child_stream.as_ref()).await {
                eprintln!("RTDB fork cleanup failed for {}: {cleanup}", child.id);
            }
            Err(error)
        }
    }
}

enum ForkPublicationError { Rejected(Anomaly), Uncertain(Anomaly) }

async fn publish_fork(directory: &dyn Directory, source: &DeploymentRecord, child: &DeploymentRecord) -> std::result::Result<DeploymentRecord, ForkPublicationError> {
    for _ in 0..8 {
        let current = directory.deployment(&source.id).await.map_err(ForkPublicationError::Rejected)?
            .filter(|current| current.can_fork() && current.epoch == source.epoch
                && current.log_epoch == source.log_epoch && current.storage_prefix() == source.storage_prefix()
                && current.storage_namespaces == child.storage_namespaces)
            .ok_or_else(|| ForkPublicationError::Rejected(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/fork-unavailable", format!("{} changed while the fork was prepared", source.name))))?;
        match directory.create_fork(child.clone(), current.version).await {
            Ok(stored) => return Ok(stored),
            Err(error) if error.code == "rtdb.directory/fork-parent-changed" => continue,
            Err(error) => {
                // Once the write is sent, a transport failure can hide successful publication.
                match directory.deployment(&child.id).await {
                    Ok(Some(stored)) if stored.parent == child.parent && stored.name == child.name => return Ok(stored),
                    Ok(None) if error.code == "rtdb.directory/exists" => return Err(ForkPublicationError::Rejected(error)),
                    _ => return Err(ForkPublicationError::Uncertain(error)),
                }
            }
        }
    }
    Err(ForkPublicationError::Rejected(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/update-contended", format!("{} kept changing while its fork was published", source.name))))
}

async fn discard_fork(bucket: &dyn rtdb_storage::ObjectStore, keys: &dyn KeyService, child: &DeploymentRecord, stream: Option<&StreamConfig>) -> Result<()> {
    let key_removed = keys.destroy(&child.id);
    let erase_objects = async {
        if !crate::reference::erase_deployment(bucket, &child.storage_prefix(), &[]).await? {
            return Err(Anomaly::fault("rtdb.multi/fork-cleanup", format!("Storage still contains staged objects for {}", child.id)));
        }
        Ok(())
    };
    let erase_tail = async {
        if let Some(stream) = stream {
            rtdb_logkeeper::drop_stream(stream, child.epoch + 1).await?;
        }
        Ok(())
    };
    let (objects_removed, tail_removed) = tokio::join!(erase_objects, erase_tail);
    key_removed.and(objects_removed).and(tail_removed)
}

/// One cell, as cell administration reaches it.
pub struct CellHandle<'a> { pub name: &'a str, pub directory: &'a dyn Directory, pub storage: &'a CellStorage, pub keys: &'a dyn KeyService, pub maximum_deployments: usize }

/// Creates a deployment in the least occupied cell with room, and records the cell in the cell map.
pub async fn create_in_cells(map: &dyn crate::cells::CellMap, cells: &[CellHandle<'_>], name: &str, org: &str, replicas: usize) -> Result<(String, DeploymentRecord)> {
    if map.cell_of(name).await?.is_some() { return Err(crate_exists(name)); }
    let mut best = None;
    for cell in cells {
        let occupied = cell.directory.deployments().await?.iter().filter(|record| record.lifecycle != Lifecycle::Deleted).count();
        if occupied < cell.maximum_deployments && best.as_ref().is_none_or(|(count, _)| occupied < *count) { best = Some((occupied, cell)); }
    }
    let (_, cell) = best.ok_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/cells-full", "Every cell is at its envelope"))?;
    if !map.assign(name, cell.name, None).await? { return Err(crate_exists(name)); }
    Ok((cell.name.to_owned(), create_deployment(cell.directory, name, org, replicas).await?))
}

fn crate_exists(name: &str) -> Anomaly { Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/exists", format!("A deployment named {name} exists in some cell")) }

/// Moves a deployment to another cell (`BeginCellMove`, `MoveCutover`).
/// Retrying with the same cells resumes after a durable cutover point or prepares a fresh copy at an isolated prefix.
pub async fn move_cell(map: &dyn crate::cells::CellMap, from: &CellHandle<'_>, to: &CellHandle<'_>, name: &str, maximum_message_bytes: usize) -> Result<DeploymentRecord> {
    let placement = map.cell_of(name).await?;
    if from.name == to.name || placement.is_none() {
        return Err(Anomaly::incorrect("rtdb.multi/move-source", format!("{name} must be placed in {} before it can be moved to a different cell", from.name)));
    }
    let current = record(from.directory, name).await?;
    let existing = match (current.lifecycle, current.cell_move.as_ref()) {
        (Lifecycle::Live, None) if placement.as_deref() == Some(from.name) => None,
        (Lifecycle::Moving, Some(existing)) if existing.destination == to.name => Some(existing),
        (Lifecycle::Deleted, Some(existing)) if existing.destination == to.name
            && current.moved_at_ms.is_some() && matches!(existing.phase, CellMovePhase::Ready { .. }) => Some(existing),
        _ => return Err(move_conflict(name, "the source cannot move to this destination")),
    };
    let ready = existing.filter(|existing| matches!(existing.phase, CellMovePhase::Ready { .. }));
    let generation = ready.map_or_else(uuid::Uuid::new_v4, |ready| ready.phase.generation());
    let mut destination = current.clone();
    destination.storage_generation = Some(generation);

    if ready.is_none() {
        if placement.as_deref() != Some(from.name) { return Err(move_conflict(name, "the source has no prepared destination")); }
        if to.directory.deployment_by_name(name).await?.is_some() || to.directory.deployment(&current.id).await?.is_some() {
            return Err(crate_exists(name));
        }
        choose_ensemble(to.directory, &current, current.ensemble.len()).await?;
    }
    let (source, target) = tokio::try_join!(
        from.storage.bucket(rtdb_storage::IoRegion::new()),
        to.storage.bucket(rtdb_storage::IoRegion::new()),
    )?;
    let source_prefix = rtdb_storage::ObjectKey::new(current.storage_prefix())?;
    let destination_prefix = rtdb_storage::ObjectKey::new(destination.storage_prefix())?;
    if from.storage.cleanup_contains(to.storage, &source_prefix)? || to.storage.cleanup_contains(from.storage, &destination_prefix)? {
        return Err(Anomaly::incorrect("rtdb.multi/shared-cell-storage", "Cell moves require object stores outside each other's deployment cleanup prefixes"));
    }
    let namespaces = move_namespaces(from, to, source.as_ref(), target.as_ref()).await?;
    if existing.is_some_and(|existing| existing.namespaces != namespaces) {
        return Err(move_conflict(name, "the move is bound to different storage or key namespaces"));
    }
    let mut fencing = current.clone();
    if existing.is_none() || current.storage_namespaces.is_some() {
        bind_admin_storage(&mut fencing, StorageNamespaces { objects: namespaces.source_objects, keys: namespaces.source_keys })?;
    }
    if ready.is_some() {
        return finish_cell_move(map, from, to, &current).await;
    }

    let previous_ensemble = replica_servers(from.directory, &current).await?;
    let key = if existing.is_some() { from.keys.existing(&current.id)? } else { from.keys.key(&current.id)? };
    to.keys.validate_install(&current.id, &key)?;

    if fencing.lifecycle == Lifecycle::Live {
        fencing.lifecycle = Lifecycle::Moving;
        fencing.epoch += 1;
        fencing.holder = None;
        fencing.move_intent = None;
        fencing.placement_move = None;
    }
    fencing.cell_move = Some(CellMove { destination: to.name.to_owned(), namespaces, phase: CellMovePhase::Preparing { generation } });
    let fenced = from.directory.replace_deployment(fencing, current.version).await?.applied()
        .ok_or_else(|| move_conflict(name, "the source changed before preparation started"))?;

    let stream = StreamConfig { write_quorum: write_quorum(previous_ensemble.len()), servers: previous_ensemble.clone(), stream: fenced.stream("replica"), maximum_message_bytes };
    rtdb_logkeeper::seal(&stream, fenced.epoch).await?;
    crate::reference::materialize(source.as_ref(), source_prefix.as_str(), target.as_ref(), destination_prefix.as_str()).await?;
    if let Some(checkpoint) = fenced.checkpoint {
        crate::reference::verify_block_pack(target.as_ref(), destination_prefix.as_str(), checkpoint.block).await?;
    }
    to.keys.install(&fenced.id, key)?;
    let ensemble = choose_ensemble(to.directory, &fenced, fenced.ensemble.len()).await?;

    let prepared = update(from.directory, name, |record| {
        if record.id != fenced.id || record.lifecycle != Lifecycle::Moving || record.epoch != fenced.epoch || record.cell_move != fenced.cell_move {
            return Err(move_conflict(name, "this copy no longer owns preparation"));
        }
        record.cell_move.as_mut().expect("the preparation owns this intent").phase =
            CellMovePhase::Ready { generation, previous_ensemble: previous_ensemble.clone(), ensemble: ensemble.clone() };
        Ok(())
    }).await?;
    finish_cell_move(map, from, to, &prepared).await
}

async fn move_namespaces(from: &CellHandle<'_>, to: &CellHandle<'_>, source: &dyn rtdb_storage::ObjectStore, target: &dyn rtdb_storage::ObjectStore) -> Result<MoveNamespaces> {
    let (source_objects, destination_objects) = tokio::try_join!(
        crate::deployment::storage_namespace(source),
        crate::deployment::storage_namespace(target),
    )?;
    if source_objects == destination_objects {
        return Err(Anomaly::incorrect("rtdb.multi/shared-cell-storage", "Cell moves require distinct source and destination object stores"));
    }
    let source_keys = from.keys.namespace_id()?;
    let destination_keys = to.keys.namespace_id()?;
    if source_keys == destination_keys {
        return Err(Anomaly::incorrect("rtdb.multi/shared-cell-keys", "Cell moves require distinct source and destination key namespaces"));
    }
    Ok(MoveNamespaces { source_objects, destination_objects, source_keys, destination_keys })
}

fn move_conflict(name: &str, reason: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/move-raced", format!("Cannot move {name}: {reason}"))
}

async fn finish_cell_move(map: &dyn crate::cells::CellMap, from: &CellHandle<'_>, to: &CellHandle<'_>, source: &DeploymentRecord) -> Result<DeploymentRecord> {
    let name = &source.name;
    let Some(CellMove { phase: CellMovePhase::Ready { generation, previous_ensemble, ensemble }, namespaces, .. }) = source.cell_move.as_ref() else {
        return Err(move_conflict(name, "the destination copy has not been recorded"));
    };
    let mut incoming = source.clone();
    incoming.lifecycle = Lifecycle::Moving;
    incoming.storage_generation = Some(*generation);
    incoming.storage_namespaces = Some(StorageNamespaces { objects: namespaces.destination_objects, keys: namespaces.destination_keys });
    incoming.cell_move = None;
    incoming.moved_at_ms = None;
    incoming.source_cleanup = None;
    incoming.move_intent = None;
    incoming.placement_move = None;
    incoming.activation_crashes = 0;
    incoming.move_from = Some(previous_ensemble.clone());
    incoming.ensemble = ensemble.clone();
    let incoming_prefix = incoming.storage_prefix();
    let matches_destination = |record: &DeploymentRecord| {
        record.id == incoming.id && record.log_epoch == incoming.log_epoch && record.storage_prefix() == incoming_prefix
            && record.epoch >= incoming.epoch
            && record.storage_namespaces.is_none_or(|binding| Some(binding) == incoming.storage_namespaces)
    };
    let waiting_for_cutover = |record: &DeploymentRecord| record.lifecycle == Lifecycle::Moving && record.cell_move.is_none();
    let valid_staged = |record: &DeploymentRecord| {
        record.epoch == incoming.epoch && record.holder.is_none() && record.checkpoint == incoming.checkpoint
            && record.ensemble == incoming.ensemble && record.move_from == incoming.move_from
    };
    let destination = match to.directory.deployment_by_name(name).await? {
        Some(found) => found,
        None if source.lifecycle == Lifecycle::Moving => {
            match to.directory.create_deployment(incoming.clone()).await {
                Ok(stored) => stored,
                Err(error) => to.directory.deployment_by_name(name).await?.ok_or(error)?,
            }
        }
        None => return Err(move_conflict(name, "the completed move has no destination record")),
    };
    if !matches_destination(&destination) || waiting_for_cutover(&destination) && !valid_staged(&destination) {
        return Err(move_conflict(name, "the destination record disagrees with its prepared state"));
    }

    if map.cell_of(name).await?.as_deref() == Some(from.name) {
        if !waiting_for_cutover(&destination) { return Err(move_conflict(name, "the destination is already serving")); }
        map.assign(name, to.name, Some(from.name)).await?;
    }
    let placement = map.cell_of(name).await?;
    if placement.as_deref() != Some(to.name)
        && (waiting_for_cutover(&destination) || placement.is_none() || placement.as_deref() == Some(from.name))
    {
        return Err(move_conflict(name, "placement did not switch to the destination"));
    }
    if source.lifecycle == Lifecycle::Moving {
        update(from.directory, name, |record| {
            if record.id != source.id || record.cell_move != source.cell_move {
                return Err(move_conflict(name, "the source lost its cutover record"));
            }
            if record.lifecycle == Lifecycle::Moving {
                record.lifecycle = Lifecycle::Deleted;
                record.moved_at_ms = Some(now_ms());
            }
            Ok(())
        }).await?;
    }
    let destination = if waiting_for_cutover(&destination) {
        update(to.directory, name, |record| {
            if !matches_destination(record) { return Err(move_conflict(name, "the destination changed before publication")); }
            if waiting_for_cutover(record) {
                if !valid_staged(record) { return Err(move_conflict(name, "the destination lost its staged state")); }
                record.storage_namespaces = incoming.storage_namespaces;
                record.lifecycle = Lifecycle::Live;
            }
            Ok(())
        }).await?
    } else { destination };

    Ok(destination)
}

/// Fences every writer, drops the deployment's stream, erases its objects, and removes its key after verifying the prefix is empty.
pub async fn delete_deployment(directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, name: &str, maximum_message_bytes: usize) -> Result<DeletionRecord> {
    let current = record(directory, name).await?;
    if current.lifecycle == Lifecycle::Deleted { return deletion_receipt(current); }
    let bucket = storage.bucket(rtdb_storage::IoRegion::new()).await?;
    let namespaces = storage_namespaces(bucket.as_ref(), keys).await?;
    let legacy_namespace = if current.lifecycle == Lifecycle::Deleting && current.storage_namespaces.is_none() {
        Some(configured_cell_storage(directory, name).await?)
    } else { None };
    let fenced = update(directory, name, |record| {
        if record.id != current.id { return Err(deletion_changed(name)); }
        match record.lifecycle {
            Lifecycle::Live | Lifecycle::Quarantined => {
                bind_admin_storage(record, namespaces)?;
                record.lifecycle = Lifecycle::Deleting;
                record.epoch += 1;
                record.holder = None;
                Ok(())
            }
            Lifecycle::Deleting => {
                if record.storage_namespaces.is_none() { record.storage_namespaces = legacy_namespace; }
                require_storage_namespaces(record, namespaces)
            }
            Lifecycle::Moving => Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/move-in-progress", format!("{name} is moving between cells"))),
            Lifecycle::Deleted => Ok(()),
        }
    }).await?;
    if fenced.lifecycle == Lifecycle::Deleted { return deletion_receipt(fenced); }
    let mut servers = ensemble_servers(&fenced, &members(directory).await?)?;
    if let Some(previous) = &fenced.move_from {
        for server in previous {
            if !servers.contains(server) { servers.push(server.clone()); }
        }
    }
    let stream = StreamConfig { write_quorum: write_quorum(servers.len()), servers, stream: fenced.stream("replica"), maximum_message_bytes };
    rtdb_logkeeper::drop_stream(&stream, fenced.epoch).await?;
    let others: Vec<DeploymentRecord> = directory.deployments().await?.into_iter().filter(|record| record.id != fenced.id).collect();
    let receipt = finish_deletion(storage, bucket.as_ref(), &fenced, &others, keys).await?;
    let deleted = update(directory, name, |record| {
        if record.id != fenced.id || record.epoch != fenced.epoch || record.storage_prefix() != receipt.facts.storage_prefix {
            return Err(deletion_changed(name));
        }
        require_storage_namespaces(record, namespaces)?;
        if record.lifecycle == Lifecycle::Deleted { return Ok(()); }
        if record.lifecycle != Lifecycle::Deleting { return Err(deletion_changed(name)); }
        record.deletion_record = Some(receipt.clone());
        record.lifecycle = Lifecycle::Deleted;
        Ok(())
    }).await?;
    deletion_receipt(deleted)
}

async fn configured_cell_storage(directory: &dyn Directory, name: &str) -> Result<StorageNamespaces> {
    let now = now_ms();
    let members = directory.members().await?;
    let mut nodes = members.iter().filter(|member| member.role == MemberRole::Node && member.is_live(now));
    nodes.next().and_then(|member| member.storage_namespaces)
        .filter(|expected| nodes.all(|member| member.storage_namespaces == Some(*expected)))
        .ok_or_else(|| Anomaly::incorrect("rtdb.multi/storage-binding-missing", format!("Resuming the legacy deletion of {name} requires live nodes that agree on the cell's storage configuration")))
}

fn deletion_changed(name: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/deletion-changed", format!("The deletion of {name} changed before it could be recorded"))
}

fn deletion_receipt(mut tombstone: DeploymentRecord) -> Result<DeletionRecord> {
    let receipt = tombstone.deletion_record.take()
        .ok_or_else(|| Anomaly::incorrect("rtdb.multi/deleted", format!("{} is already deleted", tombstone.name)))?;
    let facts = &receipt.facts;
    if tombstone.lifecycle != Lifecycle::Deleted || !facts.verified_empty || facts.deployment != tombstone.id
        || facts.name != tombstone.name || facts.epoch != tombstone.epoch || facts.storage_prefix != tombstone.storage_prefix()
        || Some(facts.namespaces) != tombstone.storage_namespaces
    {
        return Err(Anomaly::fault("rtdb.multi/deletion-receipt-mismatch", format!("The stored receipt for {} does not match its tombstone", tombstone.name)));
    }
    Ok(receipt)
}

async fn finish_deletion(storage: &CellStorage, bucket: &dyn rtdb_storage::ObjectStore, deployment: &DeploymentRecord, others: &[DeploymentRecord], keys: &dyn KeyService) -> Result<DeletionRecord> {
    let storage_prefix = deployment.storage_prefix();
    let namespaces = deployment.storage_namespaces.ok_or_else(|| {
        Anomaly::fault("rtdb.multi/storage-binding-missing", format!("{} has no storage binding", deployment.name))
    })?;
    if !crate::reference::erase_deployment(bucket, &storage_prefix, others).await? {
        return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/deletion-incomplete", format!("Storage still contains objects for {}", deployment.name)));
    }
    let inventory = storage.purge_versions(&rtdb_storage::ObjectKey::new(&storage_prefix)?).await?;
    let objects_erased_at_ms = now_ms();
    keys.destroy(&deployment.id)?;
    keys.sign_deletion(DeletionFacts {
        deployment: deployment.id.clone(),
        name: deployment.name.clone(),
        epoch: deployment.epoch,
        storage_prefix,
        namespaces,
        inventory,
        key_destroyed_at_ms: now_ms(),
        objects_erased_at_ms,
        verified_empty: true,
    })
}

/// What auditing a deployment's history found (`NoTwoWritersHaveEffect`, `SystemTimeRises`).
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct AuditReport { pub records: usize, pub transactions: usize, pub highest_term: i64, pub violations: Vec<String> }

/// Reads a deployment's whole replica stream through its logkeepers, decrypts it, and checks that it is one history: dense positions, terms that never go back, transaction ids, system times and source positions that strictly rise.
pub async fn audit(directory: &dyn Directory, keys: &dyn KeyService, name: &str, maximum_message_bytes: usize) -> Result<AuditReport> {
    use rtdb_format::envelope::{ReplicaBody, ReplicaEnvelope};
    let current = record(directory, name).await?;
    let servers = replica_servers(directory, &current).await?;
    let stream = StreamConfig { write_quorum: write_quorum(servers.len()), servers, stream: current.stream("replica"), maximum_message_bytes };
    let reader = rtdb_logkeeper::open_reader(stream, CancellationToken::new()).await?;
    let log = crate::logs::SealedLog::new(reader, &keys.key(&current.id)?, &current.stream("replica"))?;
    let latest = log.latest()?.0;
    let mut report = AuditReport::default();
    let (mut next, mut last_tx, mut last_time, mut last_source) = (0i64, i64::MIN, i64::MIN, i64::MIN);
    while next <= latest {
        let records = match log.read_range(rtdb_storage::log::LogOffset(next), rtdb_storage::log::LogOffset((next + 1024).min(latest + 1))).await {
            // The stream is trimmed below the last published block: the audit covers what remains.
            Err(rtdb_storage::Error::UnavailableLogRange { first_retained, .. }) if first_retained > next => { next = first_retained; continue; }
            records => records?,
        };
        if records.is_empty() { report.violations.push(format!("no record at position {next}")); break; }
        for record in records {
            if record.metadata.offset.0 != next { report.violations.push(format!("position {} follows {}", record.metadata.offset.0, next - 1)); }
            next = record.metadata.offset.0 + 1;
            report.records += 1;
            let Some(envelope) = ReplicaEnvelope::decode(&record.payload)? else { continue };
            if envelope.term.0 < report.highest_term { report.violations.push(format!("position {}: term {} after term {}", record.metadata.offset.0, envelope.term.0, report.highest_term)); }
            report.highest_term = report.highest_term.max(envelope.term.0);
            if let ReplicaBody::ResolvedTx(tx) = &envelope.message {
                report.transactions += 1;
                for (what, value, last) in [("transaction id", tx.tx_id, &mut last_tx), ("system time", tx.system_time_micros, &mut last_time), ("source position", tx.src_msg_id, &mut last_source)] {
                    if let Some(value) = value {
                        if value <= *last { report.violations.push(format!("position {}: {what} {value} does not rise past {}", record.metadata.offset.0, *last)); }
                        *last = value;
                    }
                }
            }
        }
    }
    Ok(report)
}

/// Starts scaling a node in: it keeps serving what it holds, receives no new placements, and parks everything it holds so that each deployment's next request activates it elsewhere.
pub async fn drain(directory: &dyn Directory, member: &MemberId) -> Result<()> {
    for _ in 0..8 {
        let current = directory.member(member).await?.ok_or_else(|| Anomaly::incorrect("rtdb.multi/unknown-member", format!("No member {member}")))?;
        if current.state != MemberState::Eligible { return Ok(()); }
        let mut draining = current.clone();
        draining.state = MemberState::Draining;
        if let Cas::Applied(_) = directory.replace_member(draining, current.version).await? { return Ok(()); }
    }
    Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/drain-contended", "The member kept changing while being drained"))
}

/// Marks every member whose lease expired more than `grace` ago as dead.
/// Placements need no write: a dead member's holdings stop counting through `effective_holder`; this keeps the member list honest.
pub async fn reap_members(directory: &dyn Directory, grace: Duration) -> Result<usize> {
    let mut reaped = 0;
    let threshold = now_ms() - grace.as_millis() as i64;
    for member in directory.members().await? {
        if matches!(member.state, MemberState::Dead | MemberState::Retired) || member.lease_expires_at_ms > threshold { continue; }
        let mut dead = member.clone();
        dead.state = MemberState::Dead;
        if let Cas::Applied(_) = directory.replace_member(dead, member.version).await? { reaped += 1; }
    }
    Ok(reaped)
}

#[derive(Clone, Debug)]
pub struct ControllerOptions {
    pub interval: Duration,
    pub grace: Duration,
    pub rebalance_moves: usize,
    /// Consecutive failed probes after which a node is confirmed dead, ahead of its lease.
    pub suspicion_probes: u32,
    pub probe_timeout: Duration,
    /// The cell's control key, with which the controller signs its probes and moves.
    pub control: ControlKey,
}

impl Default for ControllerOptions {
    fn default() -> Self { Self { interval: Duration::from_secs(1), grace: Duration::from_secs(5), rebalance_moves: 0, suspicion_probes: 2, probe_timeout: Duration::from_millis(300), control: ControlKey::default() } }
}

/// Probes every live node's agent, and confirms dead those that failed `suspicion_probes` probes in a row (`ConfirmSuspicion`).
/// Safety never depends on the verdict: a wrongly confirmed node's writes are already fenced by the epoch its successor claims.
pub async fn confirm_suspicions(directory: &dyn Directory, failures: &mut BTreeMap<MemberId, u32>, options: &ControllerOptions) -> Result<usize> {
    let now = now_ms();
    let mut confirmed = 0;
    for member in directory.members().await? {
        if member.role != MemberRole::Node || !member.is_live(now) { failures.remove(&member.id); continue; }
        let Some(address) = &member.control_address else { continue };
        let answered = matches!(control::call(address, &Request::Ping, options.probe_timeout, &options.control).await, Ok(Response::Done));
        if answered { failures.remove(&member.id); continue; }
        let count = failures.entry(member.id.clone()).or_default();
        *count += 1;
        if *count >= options.suspicion_probes {
            // The suspect may still be renewing its lease, so the confirmation re-reads and retries rather than losing to a renewal.
            let mut current = member.clone();
            for _ in 0..8 {
                if matches!(current.state, MemberState::Dead | MemberState::Retired) { break; }
                let mut dead = current.clone();
                dead.state = MemberState::Dead;
                match directory.replace_member(dead, current.version).await? {
                    Cas::Applied(_) => { confirmed += 1; break; }
                    Cas::Conflict(Some(stored)) => current = stored,
                    Cas::Conflict(None) => break,
                }
            }
            failures.remove(&member.id);
        }
    }
    Ok(confirmed)
}

pub async fn run_controller(directory: Arc<dyn Directory>, options: ControllerOptions, cancellation: CancellationToken) {
    let mut failures = BTreeMap::new();
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = tokio::time::sleep(options.interval) => {
                let _ = reap_members(directory.as_ref(), options.grace).await;
                if options.suspicion_probes > 0 { let _ = confirm_suspicions(directory.as_ref(), &mut failures, &options).await; }
                if options.rebalance_moves > 0 { let _ = rebalance(directory.as_ref(), options.rebalance_moves, &options.control).await; }
            }
        }
    }
}

/// The cell's load as autoscaling reads it: what each node holds, and how many nodes the active deployments need at `target_per_node`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CellStatus {
    pub deployments: usize,
    pub active: usize,
    pub parked: usize,
    pub quarantined: usize,
    pub nodes: BTreeMap<String, usize>,
    pub logkeepers: usize,
    pub recommended_nodes: usize,
}

pub async fn cell_status(directory: &dyn Directory, target_per_node: usize) -> Result<CellStatus> {
    let members = members(directory).await?;
    let now = now_ms();
    let mut nodes: BTreeMap<String, usize> = members.values().filter(|member| member.role == MemberRole::Node && member.is_live(now)).map(|member| (member.id.to_string(), 0)).collect();
    let deployments = directory.deployments().await?;
    let mut active = 0;
    let mut quarantined = 0;
    for deployment in &deployments {
        if deployment.lifecycle == Lifecycle::Quarantined { quarantined += 1; }
        if let Some(holder) = effective_holder(deployment, &members, now) { active += 1; *nodes.entry(holder.to_string()).or_default() += 1; }
    }
    let live = deployments.iter().filter(|deployment| matches!(deployment.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)).count();
    Ok(CellStatus {
        deployments: live, active, parked: live - active, quarantined, nodes,
        logkeepers: members.values().filter(|member| member.role == MemberRole::Logkeeper && member.is_live(now)).count(),
        recommended_nodes: active.div_ceil(target_per_node.max(1)).max(1),
    })
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::path::Path;

    use async_trait::async_trait;
    use bytes::Bytes;
    use rtdb_directory::MemoryDirectory;
    use rtdb_storage::{MemoryStore, ObjectKey, ObjectStore, object_store::StoredObject};

    use super::{CellStorage, DeploymentRecord, Directory, KeyService, Lifecycle, delete_deployment, finish_deletion};
    use crate::keys::LocalKeyService;

    struct RetainingStore<'a>(&'a MemoryStore);

    #[async_trait]
    impl ObjectStore for RetainingStore<'_> {
        async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> { self.0.head(key).await }
        async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> { self.0.get(key).await }
        async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> { self.0.get_range(key, range).await }
        async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> { self.0.put(key, bytes).await }
        async fn put_file(&self, key: &ObjectKey, source: &Path) -> rtdb_storage::Result<()> { self.0.put_file(key, source).await }
        async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> { self.0.list(directory).await }
        async fn delete(&self, _: &ObjectKey) -> rtdb_storage::Result<()> { Ok(()) }
    }

    #[tokio::test]
    async fn deletion_can_retry_and_keeps_the_key_if_the_prefix_is_still_populated() {
        let data = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(data.path()).unwrap();
        let storage = CellStorage::Local { root: data.path().join("objects") };
        let mut deployment = DeploymentRecord::new("shop", "acme", vec![], 0);
        deployment.lifecycle = Lifecycle::Deleting;
        let key = keys.key(&deployment.id).unwrap();
        let bucket = MemoryStore::default();
        deployment.storage_namespaces = Some(super::storage_namespaces(&bucket, &keys).await.unwrap());
        let object = ObjectKey::new(format!("{}/v06/rows", deployment.storage_prefix())).unwrap();
        bucket.put(&object, Bytes::from_static(b"data")).await.unwrap();

        let error = finish_deletion(&storage, &RetainingStore(&bucket), &deployment, &[], &keys).await.unwrap_err();
        assert_eq!(error.code, "rtdb.multi/deletion-incomplete");
        assert_eq!(keys.key(&deployment.id).unwrap(), key);

        let proof = finish_deletion(&storage, &bucket, &deployment, &[], &keys).await.unwrap();
        assert!(proof.facts.verified_empty);
        assert!(bucket.list(Some(&ObjectKey::new(deployment.storage_prefix()).unwrap())).await.unwrap().is_empty());
        assert_eq!(keys.existing(&deployment.id).unwrap_err().code, "rtdb.multi/key-destroyed");
    }

    #[tokio::test]
    async fn a_fork_survives_a_parent_record_update_but_discards_its_prepared_state_after_a_fence() {
        let directory = MemoryDirectory::new();
        let data = tempfile::tempdir().unwrap();
        let key_dir = data.path().join("keys");
        let keys = LocalKeyService::new(&key_dir).unwrap();
        let storage = CellStorage::Local { root: data.path().join("objects") };
        let bucket = storage.bucket(rtdb_storage::IoRegion::new()).await.unwrap();
        let mut source = DeploymentRecord::new("shop", "acme", vec![], 0);
        source.storage_namespaces = Some(super::storage_namespaces(bucket.as_ref(), &keys).await.unwrap());
        let source = directory.create_deployment(source).await.unwrap();
        let key = keys.key(&source.id).unwrap();
        let object = ObjectKey::new(format!("{}/v06/rows", source.storage_prefix())).unwrap();
        bucket.put(&object, Bytes::from_static(b"data")).await.unwrap();

        let mut updated = source.clone();
        updated.read_width = 2;
        let updated = directory.replace_deployment(updated, source.version).await.unwrap().applied().unwrap();
        let child = super::fork_with_tail(directory.as_ref(), bucket.as_ref(), &keys, &source, "child", None, Vec::new(), 4 << 20).await.unwrap();
        assert_eq!(keys.existing(&child.id).unwrap(), key);
        let before = bucket.list(None).await.unwrap().into_iter().map(|item| item.key).collect::<Vec<_>>();

        let mut deleting = updated.clone();
        deleting.lifecycle = Lifecycle::Deleting;
        deleting.epoch += 1;
        directory.replace_deployment(deleting, updated.version).await.unwrap().applied().unwrap();
        let error = super::fork_with_tail(directory.as_ref(), bucket.as_ref(), &keys, &updated, "refused", None, Vec::new(), 4 << 20).await.unwrap_err();
        assert_eq!(error.code, "rtdb.multi/fork-unavailable");
        assert!(directory.deployment_by_name("refused").await.unwrap().is_none());
        let after = bucket.list(None).await.unwrap().into_iter().map(|item| item.key).collect::<Vec<_>>();
        assert_eq!(after, before);
        let live_keys = std::fs::read_dir(key_dir).unwrap().map(|entry| entry.unwrap())
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "key")
                && entry.metadata().unwrap().len() == crate::keys::KEY_BYTES as u64).count();
        assert_eq!(live_keys, 2);
    }

    #[tokio::test]
    async fn deletion_cannot_claim_a_deployment_while_a_cell_move_owns_it() {
        let directory = MemoryDirectory::new();
        let data = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(data.path().join("keys")).unwrap();
        let mut deployment = DeploymentRecord::new("shop", "acme", vec![], 0);
        deployment.lifecycle = Lifecycle::Moving;
        deployment.epoch = 2;
        let deployment = directory.create_deployment(deployment).await.unwrap();
        let key = keys.key(&deployment.id).unwrap();
        let storage = CellStorage::Local { root: data.path().join("objects") };

        let error = delete_deployment(directory.as_ref(), &storage, &keys, "shop", 4 << 20).await.unwrap_err();
        assert_eq!(error.code, "rtdb.multi/move-in-progress");
        assert_eq!(directory.deployment(&deployment.id).await.unwrap().as_ref(), Some(&deployment));
        assert_eq!(keys.key(&deployment.id).unwrap(), key);
    }
}
