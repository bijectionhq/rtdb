//! Cell administration: creating, moving, rebalancing, forking and deleting deployments, and confirming dead members (`CreateDeployment`, `IntendMove`, `Rebalance`, `Fork`, `BeginDeletion`, `EraseAndProve`, `ExpireLease`).

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

pub use rtdb_directory::DeletionRecord;
use rtdb_directory::{Cas, CellMedia, CellResidence, CellTransfer, CellTransferIntent, PreparedCellTransfer, RetiredResidence, DeletionFacts, DeploymentRecord, DisposalReceipt, DeletionObservation, DeletionInventory, ServingInventory, Directory, Lifecycle, MemberId, MemberPool, MemberRecord, MemberRole, MemberState, PlacementMove, StorageNamespaces, effective_holder, now_ms};
use rtdb_logkeeper::StreamConfig;
use rtdb_storage::log::Log;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio_util::sync::CancellationToken;

use crate::control::{self, ControlKey, MoveTicket, Request, Response};
use crate::deployment::{CellStorage, ensemble_servers, live_holdings, require_storage_namespaces, s3_write_lease_horizon, select_logkeepers, storage_namespaces, write_quorum};
use crate::keys::KeyService;

const CONTROL_TIMEOUT: Duration = Duration::from_secs(35);
const CONCURRENT_SUSPICION_PROBES: usize = 16;

/// Binds an object namespace for a cell before nodes using scoped writes start.
pub async fn initialize_storage(storage: &CellStorage, keys: &dyn KeyService) -> Result<StorageNamespaces> {
    let bucket = storage.bucket(rtdb_storage::IoRegion::new()).await?;
    storage_namespaces(bucket.as_ref(), keys).await
}

/// Creates a deployment, choosing its ensemble from the live logkeepers in its affinity order.
/// Allocates identity and nothing else: no objects, no log state, no compute.
pub async fn create_deployment(directory: &dyn Directory, name: &str, org: &str, replicas: usize) -> Result<DeploymentRecord> {
    validate_org(org)?;
    let mut record = DeploymentRecord::new(name, org, Vec::new(), now_ms());
    record.ensemble = choose_ensemble(directory, &record, replicas).await?;
    directory.create_deployment(record).await
}

fn validate_org(org: &str) -> Result<()> {
    if org.contains('/') || rtdb_storage::ObjectKey::new(org).is_err() {
        return Err(Anomaly::incorrect("rtdb.multi/invalid-organisation", "The organisation must be a single storage path component"));
    }
    Ok(())
}

/// Installs the root named by a hosting authority. A retry recovers the
/// original installation even if the first directory response was lost.
pub async fn install_deployment(
    directory: &dyn Directory,
    id: uuid::Uuid,
    name: &str,
    org: &str,
    replicas: usize,
) -> Result<DeploymentRecord> {
    if replicas == 0 {
        return Err(Anomaly::incorrect("rtdb.multi/install-identity", "The installation needs a logkeeper ensemble"));
    }
    let mut installation = installation_record(id, name, org)?;
    let id = installation.id.clone();
    let confirm = |installed: DeploymentRecord| {
        if installed.id == id && installed.org == org && installed.parent.is_none()
            && installed.linearizable_reads && matches!(installed.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
            && installed.ensemble.len() == replicas
        {
            Ok(installed)
        } else {
            Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/install-conflict", format!("{name} belongs to another installation")))
        }
    };
    if let Some(existing) = directory.deployment_by_name(name).await? {
        return confirm(existing);
    }
    installation.ensemble = choose_ensemble(directory, &installation, replicas).await?;
    match directory.create_deployment(installation).await {
        Ok(stored) => Ok(stored),
        Err(error) => match directory.deployment_by_name(name).await {
            Ok(Some(existing)) => confirm(existing),
            Ok(None) | Err(_) => Err(error),
        },
    }
}

pub(crate) fn installation_record(id: uuid::Uuid, name: &str, org: &str) -> Result<DeploymentRecord> {
    if id.is_nil() || !valid_installation_name(name) || !valid_installation_name(org) {
        return Err(Anomaly::incorrect("rtdb.multi/install-identity", "Invalid deployment installation identity"));
    }
    let mut installation = DeploymentRecord::new(name, org, Vec::new(), now_ms());
    installation.id = id.to_string();
    installation.lineage = installation.id.clone();
    installation.linearizable_reads = true;
    Ok(installation)
}

fn valid_installation_name(part: &str) -> bool {
    !part.is_empty() && part.len() <= 128 && part != "." && part != ".."
        && part.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

async fn choose_ensemble(directory: &dyn Directory, record: &DeploymentRecord, replicas: usize) -> Result<Vec<MemberId>> {
    let now = now_ms();
    let members = directory.members().await?;
    let logkeepers: Vec<_> = members.iter().filter(|member| member.role == MemberRole::Logkeeper && member.state == MemberState::Eligible && member.is_live(now)).collect();
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

/// Ask the original installation's holder to publish through its source and
/// retain the directory cut from that same residence.
pub async fn flush_checkpoint(directory: &dyn Directory, original: &DeploymentRecord, control: &ControlKey) -> Result<DeploymentRecord> {
    let holder = ensure_held(directory, &original.name, control).await?;
    let before = record(directory, &original.name).await?;
    let matches_original = |current: &DeploymentRecord| current.id == original.id && current.org == original.org
        && current.incarnation == original.incarnation && current.log_epoch == original.log_epoch
        && current.holder.as_ref() == Some(&holder);
    if !matches_original(&before) {
        return Err(fork_unavailable(&original.name));
    }
    let address = control_address(&members(directory).await?, &holder)?;
    let unavailable = || Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/capture-flush", "The holder did not publish an exact checkpoint in the original residence");
    let Response::Flushed { block } = expect(
        &address, Request::Flush { name: original.name.clone() }, control,
    ).await? else { return Err(unavailable()); };
    let deadline = tokio::time::Instant::now() + CONTROL_TIMEOUT;
    loop {
        let after = record(directory, &original.name).await?;
        if !matches_original(&after) || after.epoch != before.epoch || tokio::time::Instant::now() > deadline {
            return Err(unavailable());
        }
        if after.checkpoint.is_some_and(|cut| cut.block >= block) {
            return Ok(after);
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// Reserves the next claim for `target` during the handoff.
pub async fn move_deployment(directory: &dyn Directory, name: &str, target: &MemberId, control: &ControlKey) -> Result<()> {
    move_from_holding(directory, name, target, None, control).await
}

pub(crate) async fn move_from_holding(directory: &dyn Directory, name: &str, target: &MemberId,
                                      source: Option<(&MemberId, u64)>, control: &ControlKey) -> Result<()> {
    let members = members(directory).await?;
    let address = control_address(&members, target)?;
    let reservation = PlacementMove {
        request: uuid::Uuid::new_v4(),
        target: target.clone(),
        expires_at_ms: now_ms().saturating_add((3 * CONTROL_TIMEOUT).as_millis() as i64),
    };
    let intended = update(directory, name, |record| {
        let now = now_ms();
        let holder = effective_holder(record, &members, now);
        if source.is_some_and(|(member, epoch)| record.epoch != epoch || holder != Some(member)) {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/move-source-changed", format!("{name} has already changed holders")));
        }
        if !members.get(target).is_some_and(|member| record.may_place_on(member, now)) {
            return Err(Anomaly::incorrect("rtdb.multi/not-placeable", format!("{target} may not take placements of {name}")));
        }
        if holder == Some(target) { return Ok(()); }
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
        match control::retry_activation(retry_window, || expect(&address, activation.clone(), control)).await? {
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
        let mut load: BTreeMap<MemberId, Vec<&DeploymentRecord>> = members.values()
            .filter(|member| member.pool == pool && member.may_receive_placements(now) && member.control_address.is_some())
            .map(|member| (member.id.clone(), Vec::new())).collect();
        for record in deployments.iter().filter(|record| record.writer_pool() == Some(pool)) {
            if let Some(held) = effective_holder(record, &members, now).and_then(|holder| load.get_mut(holder)) {
                held.push(record);
            }
        }
        while moved < moves {
            let mut ranked: Vec<_> = load.iter().collect();
            ranked.sort_by_key(|(_, held)| held.len());
            let planned = ranked.iter().rev().find_map(|&(source, holdings)| {
                ranked.iter().take_while(|(_, held)| holdings.len() > held.len() + 1).find_map(|&(target, _)| {
                    holdings.iter().rposition(|record| record.may_place_on(&members[target], now))
                        .map(|index| (source.clone(), target.clone(), index))
                })
            });
            let Some((source, target, index)) = planned else { break };
            let deployment = load.get_mut(&source).expect("listed").swap_remove(index);
            match move_from_holding(directory, &deployment.name, &target, Some((&source, deployment.epoch)), control).await {
                Ok(()) => {}
                Err(error) if matches!(error.code.as_str(), "rtdb.multi/move-source-changed" | "rtdb.multi/move-in-progress") => return Ok(moved),
                Err(error) => return Err(error),
            }
            load.get_mut(&target).expect("listed").push(deployment);
            moved += 1;
        }
    }
    Ok(moved)
}

pub(crate) fn validate_engine_version(version: &str) -> Result<()> {
    if version.is_empty() || version.len() > 128 || version.chars().any(|character| character.is_whitespace() || character.is_control()) {
        return Err(Anomaly::incorrect("rtdb.multi/engine-version", "An engine version must be a nonempty identifier of at most 128 bytes"));
    }
    Ok(())
}

pub async fn pin_engine(directory: &dyn Directory, name: &str, version: Option<&str>) -> Result<DeploymentRecord> {
    if let Some(version) = version { validate_engine_version(version)?; }
    let members = members(directory).await?;
    update(directory, name, |record| {
        if !record.can_fork() { return Err(Anomaly::incorrect("rtdb.multi/engine-pin", format!("{name} cannot change its engine pin in its current state"))); }
        if record.engine_pin.as_deref() == version { return Ok(()); }
        if let Some(version) = version {
            let now = now_ms();
            let serves_version = |id| members.get(id).is_some_and(|member| member.engine_version == version);
            if effective_holder(record, &members, now).is_some_and(|id| !serves_version(id))
                || record.active_move(now).is_some_and(|reservation| !serves_version(&reservation.target))
            {
                return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/engine-pin-in-use", format!("Park {name} and let its placement reservation finish before changing the engine pin")));
            }
            if !members.values().any(|member| member.engine_version == version && record.writer_pool() == Some(member.pool)
                && member.control_address.is_some() && member.may_receive_placements(now)
                && record.storage_namespaces.is_none_or(|namespaces| member.storage_namespaces == Some(namespaces)))
            {
                return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/engine-pin-no-target", format!("No eligible node can serve {name} on engine {version}")));
            }
        }
        record.engine_pin = version.map(str::to_owned);
        Ok(())
    }).await
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
    let _maintenance = storage.try_local_maintenance()?;
    let source = fork_source(directory, parent).await?;
    let bucket = fork_bucket(storage, keys, &source).await?;
    fork_with_tail(directory, bucket.as_ref(), keys, &source, name, None, Vec::new(), 4 << 20).await
}

/// Forks `parent` as of one of its past blocks: the fork's state is that block's, and what the parent wrote after it is not the fork's.
/// Only a block younger than the reaper's `grace` can be forked from: an object it needs that the parent released since is erased only once the grace has passed.
pub async fn fork_at_block(directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, parent: &str, name: &str, block: i64, grace: std::time::Duration) -> Result<DeploymentRecord> {
    let _maintenance = storage.try_local_maintenance()?;
    let source = fork_source(directory, parent).await?;
    let bucket = fork_bucket(storage, keys, &source).await?;
    let age = now_ms() - crate::reference::written_at_ms(bucket.as_ref(), &source.storage_prefix(), block).await?;
    if age >= grace.as_millis() as i64 { return Err(Anomaly::incorrect("rtdb.multi/block-too-old", format!("Block {block} was written {}s ago, beyond the reaper's grace of {}s", age / 1000, grace.as_secs()))); }
    fork_with_tail(directory, bucket.as_ref(), keys, &source, name, Some(block), Vec::new(), 4 << 20).await
}

/// Forks `parent` as of a system time at or after its checkpoint: the fork starts from the checkpoint's block, with the parent's transactions after it up to `system_time` copied into its own stream.
/// A time before the checkpoint is a past block's, which `fork_at_block` forks from.
pub async fn fork_at_time(directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, parent: &str, name: &str, system_time: rtdb_types::Timestamp, grace: std::time::Duration, maximum_message_bytes: usize) -> Result<DeploymentRecord> {
    let _maintenance = storage.try_local_maintenance()?;
    let source = fork_source(directory, parent).await?;
    let checkpoint = source.checkpoint.ok_or_else(|| Anomaly::incorrect("rtdb.multi/no-checkpoint", format!("{parent} has published no block to fork from")))?;
    if system_time.0 < checkpoint.system_time { return Err(Anomaly::incorrect("rtdb.multi/before-checkpoint", format!("{parent}'s last published block is after that time: fork at a past block instead"))); }
    let bucket = fork_bucket(storage, keys, &source).await?;
    if now_ms() - crate::reference::written_at_ms(bucket.as_ref(), &source.storage_prefix(), checkpoint.block).await? >= grace.as_millis() as i64 {
        return Err(Anomaly::incorrect("rtdb.multi/block-too-old", format!("{parent}'s last published block is older than the reaper's grace")));
    }
    let servers = replica_servers(directory, &source).await?;
    let stream = StreamConfig { write_quorum: write_quorum(servers.len()), servers, stream: source.stream("replica"), maximum_message_bytes };
    let material = fork_key(keys, &source).await?;
    let reader = crate::logs::SealedLog::new(rtdb_logkeeper::open_reader(stream.clone(), CancellationToken::new()).await?, &material, &source.replica_seal_scope())?;
    let tail = crate::logs::transactions_until(&reader, checkpoint.boundary, system_time).await?;
    fork_with_tail(directory, bucket.as_ref(), keys, &source, name, Some(checkpoint.block), tail, maximum_message_bytes).await
}

fn fork_unavailable(name: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/fork-unavailable", format!("{name} changed or cannot be forked in its current state"))
}

async fn fork_key(keys: &dyn KeyService, source: &DeploymentRecord) -> Result<[u8; crate::keys::KEY_BYTES]> {
    keys.existing_key(&source.key_id()).await?.ok_or_else(|| fork_unavailable(&source.name))
}

async fn fork_with_tail(directory: &dyn Directory, bucket: &dyn rtdb_storage::ObjectStore, keys: &dyn KeyService, source: &DeploymentRecord, name: &str, at_block: Option<i64>, mut tail: Vec<rtdb_format::envelope::ReplicaEnvelope>, maximum_message_bytes: usize) -> Result<DeploymentRecord> {
    validate_org(&source.org)?;
    let mut child = DeploymentRecord::new(name, source.org.clone(), Vec::new(), now_ms());
    child.lineage = source.lineage.clone();
    child.parent = Some(source.id.clone());
    child.engine_pin = source.engine_pin.clone();
    child.log_epoch = source.log_epoch + 1;
    child.storage_namespaces = source.storage_namespaces;
    // The parent's checkpoint names positions in the parent's logs, not the fork's: the fork's first holder finds its block by listing, and publishes the fork's own.
    child.ensemble = choose_ensemble(directory, &child, source.ensemble.len()).await?;
    let key = fork_key(keys, source).await?;
    let child_stream = if tail.is_empty() {
        None
    } else {
        // The first holder claims the next epoch and seals the fork's tail.
        child.epoch = 1;
        crate::logs::renumber_into(&mut tail, rtdb_types::StorageEpoch(child.log_epoch))?;
        let servers = replica_servers(directory, &child).await?;
        child.replica_journals.extend(servers.iter().cloned());
        Some(StreamConfig { write_quorum: write_quorum(servers.len()), servers, stream: child.stream("replica"), maximum_message_bytes })
    };
    let prepared = async {
        crate::reference::fork(bucket, &source.storage_prefix(), &child.storage_prefix(), at_block).await?;
        keys.install(&child.key_id(), key).await?;
        if let Some(stream) = &child_stream {
            let writer = rtdb_logkeeper::open_writer(stream.clone(), child.epoch, CancellationToken::new()).await?;
            let writer = crate::logs::SealedLog::new(writer, &key, &child.replica_seal_scope())?;
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
                && current.storage_namespaces == child.storage_namespaces && current.engine_pin == child.engine_pin)
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
    let key_removed = keys.destroy(&child.key_id()).await;
    let erase_objects = async {
        if !crate::reference::erase_deployment(bucket, &child.storage_prefix(), &[]).await? {
            return Err(Anomaly::fault("rtdb.multi/fork-cleanup", format!("Storage still contains staged objects for {}", child.id)));
        }
        Ok(())
    };
    let erase_tail = async {
        if let Some(stream) = stream {
            rtdb_logkeeper::retire(stream, child.epoch + 1).await?;
        }
        Ok(())
    };
    let (objects_removed, tail_removed) = tokio::join!(erase_objects, erase_tail);
    key_removed.and(objects_removed).and(tail_removed)
}

/// One cell, as cell administration reaches it.
pub struct CellHandle<'a> {
    pub name: &'a str,
    pub directory: &'a Arc<dyn Directory>,
    pub storage: &'a CellStorage,
    pub keys: &'a dyn KeyService,
    pub maximum_deployments: usize,
}

/// Creates a deployment in the least occupied cell with room, and records the cell in the cell map.
pub async fn create_in_cells(map: &dyn crate::cells::CellMap, cells: &[CellHandle<'_>], name: &str, org: &str, replicas: usize) -> Result<(String, DeploymentRecord)> {
    validate_org(org)?;
    if map.cell_of(name).await?.is_some() { return Err(crate_exists(name)); }
    let mut best = None;
    for cell in cells {
        let occupied = cell.directory.deployments().await?.iter().filter(|record| record.lifecycle != Lifecycle::Deleted).count();
        if occupied < cell.maximum_deployments && best.as_ref().is_none_or(|(count, _)| occupied < *count) { best = Some((occupied, cell)); }
    }
    let (_, cell) = best.ok_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/cells-full", "Every cell is at its envelope"))?;
    if !map.assign(name, cell.name, None).await? { return Err(crate_exists(name)); }
    Ok((cell.name.to_owned(), create_deployment(cell.directory.as_ref(), name, org, replicas).await?))
}

fn crate_exists(name: &str) -> Anomaly { Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/exists", format!("A deployment named {name} exists in some cell")) }

/// Moves a deployment after preparing the destination's storage and replica
/// quorum. The route is the activation decision and a lost reply leaves the
/// prepared destination fenced for a retry.
pub async fn move_cell(
    map: &dyn crate::cells::CellMap,
    from: &CellHandle<'_>,
    to: &CellHandle<'_>,
    name: &str,
    maximum_message_bytes: usize,
    hosted_task: Option<rtdb_directory::HostedTask>,
) -> Result<DeploymentRecord> {
    let routed = map.cell_of(name).await?;
    let at_source = routed.as_deref() == Some(from.name);
    let conflict = || cell_move_conflict(from, to, name);
    if from.name == to.name || !at_source && routed.as_deref() != Some(to.name) {
        return Err(conflict());
    }
    let current = record(from.directory.as_ref(), name).await?;
    let authority = current.id.parse().map_err(|_| conflict())?;
    let intent = CellTransferIntent {
        operation: None,
        authority,
        name: name.to_owned(),
        source: CellResidence { cell: from.name.to_owned(), org: current.org.clone() },
        destination: CellResidence { cell: to.name.to_owned(), org: current.org.clone() },
    };
    let (source, target) = if at_source {
        prepare_cell_move(from, to, &intent, maximum_message_bytes, hosted_task).await?
    } else {
        prepared_cell_move(from, to, &intent).await?
    };
    if at_source
        && !map.assign(name, to.name, Some(from.name)).await?
        && map.cell_of(name).await?.as_deref() != Some(to.name)
    {
        return Err(conflict());
    }
    settle_cell_move(from, to, &source, &target).await
}

/// Stages the exact move reserved by an external placement catalog. Effect
/// callers bind both cell handles to the catalog's immutable placements.
pub async fn prepare_managed_cell_transfer(
    from: &CellHandle<'_>,
    to: &CellHandle<'_>,
    intent: &CellTransferIntent,
    maximum_message_bytes: usize,
    hosted_task: Option<rtdb_directory::HostedTask>,
) -> Result<PreparedCellTransfer> {
    validate_managed_cell_move(from, to, intent)?;
    let (source, target) = prepare_cell_move(from, to, intent, maximum_message_bytes, hosted_task).await?;
    Ok(PreparedCellTransfer {
        intent: intent.clone(),
        source_incarnation: source.incarnation,
        destination_incarnation: target.incarnation,
        epoch: source.epoch,
    })
}

/// Called by the authenticated placement owner after it has retained this
/// receipt and selected the destination in its own catalog.
pub async fn activate_managed_cell_transfer(
    from: &CellHandle<'_>,
    to: &CellHandle<'_>,
    prepared: &PreparedCellTransfer,
) -> Result<DeploymentRecord> {
    let intent = &prepared.intent;
    validate_managed_cell_move(from, to, intent)?;
    let (source, target) = prepared_cell_move(from, to, intent).await?;
    if source.incarnation != prepared.source_incarnation || target.incarnation != prepared.destination_incarnation
        || source.epoch != prepared.epoch
    {
        return Err(cell_move_conflict(from, to, &intent.name));
    }
    settle_cell_move(from, to, &source, &target).await
}

fn validate_managed_cell_move(from: &CellHandle<'_>, to: &CellHandle<'_>, intent: &CellTransferIntent) -> Result<()> {
    if !intent.operation.is_some_and(|operation| !operation.is_nil())
        || intent.authority.is_nil()
        || from.name != intent.source.cell || to.name != intent.destination.cell
        || from.name == to.name
        || ![&intent.name, &intent.source.org, &intent.destination.org].into_iter()
            .all(|name| valid_installation_name(name))
    {
        return Err(cell_move_conflict(from, to, &intent.name));
    }
    Ok(())
}

fn same_residence(record: &DeploymentRecord, intent: &CellTransferIntent, residence: &CellResidence) -> bool {
    record.id == intent.authority.to_string() && record.name == intent.name && record.org == residence.org
        && (intent.operation.is_none() || record.linearizable_reads && record.parent.is_none())
}

fn intent_matches(record: &DeploymentRecord, intent: &CellTransferIntent) -> bool {
    record.cell_transfer.as_ref().is_some_and(|transfer| {
        &transfer.intent == intent && transfer.epoch == record.epoch && transfer.source_incarnation == record.incarnation
    })
}

async fn prepared_cell_move(
    from: &CellHandle<'_>,
    to: &CellHandle<'_>,
    intent: &CellTransferIntent,
) -> Result<(DeploymentRecord, DeploymentRecord)> {
    let conflict = || cell_move_conflict(from, to, &intent.name);
    let source = record(from.directory.as_ref(), &intent.name).await?;
    if !same_residence(&source, intent, &intent.source) || !intent_matches(&source, intent)
        || !(source.lifecycle == Lifecycle::Moving
            || source.lifecycle == Lifecycle::Deleted && source.moved_at_ms.is_some())
    {
        return Err(conflict());
    }
    let transfer = source.cell_transfer.as_ref().ok_or_else(&conflict)?;
    if transfer.source_media != cell_media(from.storage, from.keys)?
        || transfer.destination_media != cell_media(to.storage, to.keys)?
    {
        return Err(conflict());
    }
    let target = to.directory.deployment(&source.id).await?.filter(|target| {
        same_residence(target, intent, &intent.destination)
            && target.incarnation == transfer.destination_incarnation && target.cell_transfer == source.cell_transfer
            && target.move_from.is_none()
            && matches!(target.lifecycle, Lifecycle::Moving | Lifecycle::Live | Lifecycle::Quarantined)
    }).ok_or_else(conflict)?;
    Ok((source, target))
}

async fn settle_cell_move(
    from: &CellHandle<'_>,
    to: &CellHandle<'_>,
    source: &DeploymentRecord,
    target: &DeploymentRecord,
) -> Result<DeploymentRecord> {
    let conflict = || cell_move_conflict(from, to, &source.name);
    // The accepted route points to a destination with an independent
    // replica quorum. Settle the source before the destination can move again.
    update_installation(from.directory.as_ref(), &source.id, |record| {
        if !same_cell_move(record, source) || record.epoch != source.epoch {
            return Err(conflict());
        }
        match record.lifecycle {
            Lifecycle::Moving => {
                record.lifecycle = Lifecycle::Deleted;
                record.moved_at_ms = Some(now_ms());
            }
            Lifecycle::Deleted if record.moved_at_ms.is_some() => {}
            _ => return Err(conflict()),
        }
        Ok(())
    }).await?;
    update_installation(to.directory.as_ref(), &target.id, |record| {
        if !same_cell_move(record, target) || record.move_from.is_some()
            || record.storage_namespaces != target.storage_namespaces || record.storage_prefix() != target.storage_prefix()
        {
            return Err(conflict());
        }
        match record.lifecycle {
            Lifecycle::Moving if record.epoch == source.epoch => record.lifecycle = Lifecycle::Live,
            Lifecycle::Live | Lifecycle::Quarantined => {}
            _ => return Err(conflict()),
        }
        Ok(())
    }).await
}

/// Fences every writer, drops the deployment's stream, erases its objects, and removes its key after verifying the prefix is empty.
pub async fn delete_deployment(directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, name: &str, maximum_message_bytes: usize) -> Result<DeletionRecord> {
    let current = record(directory, name).await?;
    if current.lifecycle == Lifecycle::Deleted { return deletion_receipt(current); }
    let _maintenance = storage.try_local_maintenance()?;
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
            }
            Lifecycle::Deleting => {
                if record.storage_namespaces.is_none() { record.storage_namespaces = legacy_namespace; }
                require_storage_namespaces(record, namespaces)?;
            }
            Lifecycle::Moving | Lifecycle::Restoring => return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/move-in-progress", format!("{name} is moving between cells"))),
            Lifecycle::Deleted => return Ok(()),
        }
        if matches!(storage, CellStorage::S3 { .. }) && record.deletion_observation.is_none() {
            record.deletion_observation = Some(DeletionObservation::Fenced { leases_expire_at_ms: s3_write_lease_horizon(now_ms()) });
        }
        Ok(())
    }).await?;
    if fenced.lifecycle == Lifecycle::Deleted { return deletion_receipt(fenced); }
    await_storage_drain(directory, storage, keys, &fenced).await?;
    finish_disposal_with_lock(directory, storage, keys, &fenced.id, Some(fenced.incarnation), maximum_message_bytes).await?;
    let deleted = directory.deployment(&fenced.id).await?.ok_or_else(|| deletion_changed(name))?;
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
    let observation_matches = match tombstone.deletion_observation {
        None => facts.first_empty_observed_at_ms.is_none(),
        Some(DeletionObservation::Verified { first_empty_at_ms, last_empty_at_ms }) => {
            facts.first_empty_observed_at_ms == Some(first_empty_at_ms) && facts.objects_erased_at_ms == last_empty_at_ms
                && last_empty_at_ms >= first_empty_at_ms
        }
        Some(DeletionObservation::Empty { .. } | DeletionObservation::Fenced { .. }) => false,
    };
    if tombstone.lifecycle != Lifecycle::Deleted || !facts.verified_empty || facts.deployment != tombstone.id
        || facts.name != tombstone.name || facts.epoch != tombstone.epoch || facts.storage_prefix != tombstone.storage_prefix()
        || Some(facts.namespaces) != tombstone.storage_namespaces || !observation_matches
    {
        return Err(Anomaly::fault("rtdb.multi/deletion-receipt-mismatch", format!("The stored receipt for {} does not match its tombstone", tombstone.name)));
    }
    Ok(*receipt)
}

fn cell_move_conflict(from: &CellHandle<'_>, to: &CellHandle<'_>, name: &str) -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict, "rtdb.multi/cell-transfer",
        format!("The transfer of {name} from {} to {} conflicts with its recorded placement", from.name, to.name),
    )
}

fn same_cell_move(record: &DeploymentRecord, expected: &DeploymentRecord) -> bool {
    record.id == expected.id && record.name == expected.name && record.org == expected.org
        && record.incarnation == expected.incarnation && record.cell_transfer == expected.cell_transfer
}

pub(crate) fn cell_media(storage: &CellStorage, keys: &dyn KeyService) -> Result<CellMedia> {
    Ok(CellMedia {
        objects: storage.custody_namespace()?,
        keys: keys.custody_namespace()?,
    })
}

async fn prepare_cell_move(
    from: &CellHandle<'_>,
    to: &CellHandle<'_>,
    intent: &CellTransferIntent,
    maximum_message_bytes: usize,
    hosted_task: Option<rtdb_directory::HostedTask>,
) -> Result<(DeploymentRecord, DeploymentRecord)> {
    let conflict = || cell_move_conflict(from, to, &intent.name);
    let current = record(from.directory.as_ref(), &intent.name).await?;
    if !same_residence(&current, intent, &intent.source) {
        return Err(conflict());
    }
    if current.lifecycle == Lifecycle::Deleted {
        return prepared_cell_move(from, to, intent).await;
    }
    let target = to.directory.deployment_by_name(&intent.name).await?;
    let source_media = cell_media(from.storage, from.keys)?;
    let destination_media = cell_media(to.storage, to.keys)?;
    if from.storage.overlaps(to.storage)? {
        return Err(Anomaly::incorrect("rtdb.multi/shared-cell-storage", "Cell moves require independent serving storage"));
    }
    if source_media.keys == destination_media.keys {
        return Err(Anomaly::incorrect("rtdb.multi/shared-cell-keys", "Cell moves require independent key custody"));
    }
    if matches!(to.storage, CellStorage::S3 { .. }) && hosted_task.is_none() {
        return Err(Anomaly::incorrect("rtdb.multi/transfer-task", "A remote transfer needs a recoverable task identity"));
    }
    let _source_maintenance = from.storage.try_local_maintenance()?;
    let _target_maintenance = to.storage.try_local_maintenance()?;

    let same_media = |record: &DeploymentRecord| record.cell_transfer.as_ref().is_some_and(|transfer| {
        transfer.source_media == source_media && transfer.destination_media == destination_media
    });
    if current.lifecycle == Lifecycle::Moving && !same_media(&current) {
        return Err(conflict());
    }
    let mut arrival = current.clone();
    arrival.org = intent.destination.org.clone();
    arrival.incarnation = current.incarnation.checked_add(1).ok_or_else(|| {
        Anomaly::fault("rtdb.multi/incarnation-exhausted", format!("Installation {} has no remaining residences", current.id))
    })?;
    let mut pending = None;
    let mut prior_residence = None;
    if let Some(previous) = target {
        if !same_residence(&previous, intent, &intent.destination) {
            return Err(conflict());
        }
        match previous.lifecycle {
            Lifecycle::Moving if previous.incarnation == arrival.incarnation
                && intent_matches(&current, intent) && previous.cell_transfer == current.cell_transfer =>
            {
                if previous.move_from.is_none() {
                    return prepared_cell_move(from, to, intent).await;
                }
                arrival.ensemble.clone_from(&previous.ensemble);
                pending = Some(previous);
            }
            Lifecycle::Deleted if previous.incarnation < current.incarnation && previous.epoch <= current.epoch => {
                let transfer = previous.cell_transfer.as_ref()
                    .filter(|transfer| {
                        previous.moved_at_ms.is_some()
                            && transfer.intent.source == intent.destination
                            && transfer.source_incarnation == previous.incarnation
                            && transfer.source_media == destination_media
                            && transfer.epoch == previous.epoch
                    })
                    .ok_or_else(&conflict)?;
                let ready = previous.disposal.as_ref().is_some_and(|receipt| {
                    receipt.deployment == previous.id && receipt.incarnation == previous.incarnation
                        && receipt.epoch == transfer.epoch && receipt.journals == previous.replica_journals
                }) && previous.storage_regions.is_empty() && previous.archive_retirement.as_ref()
                    .is_none_or(|retirement| retirement.observed_empty_at_ms.is_some());
                if !ready {
                    return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/previous-residence", "The destination is still settling its previous residence"));
                }
                prior_residence = Some(previous);
            }
            _ => return Err(conflict()),
        }
    }
    if pending.is_none() {
        arrival.ensemble = choose_ensemble(to.directory.as_ref(), &arrival, current.ensemble.len()).await?;
    }
    let source_bucket = from.storage.bucket(rtdb_storage::IoRegion::new()).await?;
    let target_bucket = to.storage.bucket(rtdb_storage::IoRegion::new()).await?;
    let source_binding = storage_namespaces(source_bucket.as_ref(), from.keys).await?;
    let target_binding = storage_namespaces(target_bucket.as_ref(), to.keys).await?;
    if source_binding.objects == target_binding.objects || source_binding.keys == target_binding.keys {
        return Err(Anomaly::incorrect("rtdb.multi/shared-cell-storage", "Cell moves require independent serving storage and key custody"));
    }
    if let Some(pin) = current.engine_pin.as_ref() {
        let now = now_ms();
        if !to.directory.members().await?.iter().any(|member| {
            member.engine_version == *pin && member.role == MemberRole::Node && member.pool == MemberPool::General
                && member.may_receive_placements(now) && member.control_address.is_some()
                && member.storage_namespaces == Some(target_binding)
        }) {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/engine-pin-no-target", "No destination node serves the pinned engine version"));
        }
    }
    let mut bound_source = current.clone();
    bind_admin_storage(&mut bound_source, source_binding)?;
    let material = from.keys.existing_key(&current.key_id()).await?;
    if let Some(material) = material {
        to.keys.validate_install(&arrival.key_id(), &material).await?;
    } else if current.checkpoint.is_some() {
        return Err(Anomaly::fault("rtdb.multi/transfer-key", "The source checkpoint has no key in its original vault"));
    }
    if let Some(target) = to.directory.deployment(&current.id).await? {
        require_storage_namespaces(&target, target_binding)?;
    }
    if current.lifecycle == Lifecycle::Live
        && to.directory.deployments().await?.iter().filter(|record| record.lifecycle != Lifecycle::Deleted).count()
            >= to.maximum_deployments
    {
        return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/cell-full", "The destination is at its deployment limit"));
    }
    let source_stream = administration_stream(from.directory.as_ref(), &current, maximum_message_bytes).await?;
    let target_stream = administration_stream(to.directory.as_ref(), &arrival, maximum_message_bytes).await?;
    if target_stream.servers.iter().any(|server| source_stream.servers.contains(server) || current.replica_journals.contains(server)) {
        return Err(Anomaly::incorrect("rtdb.multi/shared-cell-journal", "Cell moves require independent replica journals"));
    }

    let source = update_installation(from.directory.as_ref(), &current.id, |record| {
        if record.lifecycle == Lifecycle::Moving && intent_matches(record, intent) && same_media(record) {
            return Ok(());
        }
        if record.lifecycle != Lifecycle::Live || record.incarnation != current.incarnation
            || record.epoch != current.epoch || record.checkpoint != current.checkpoint
            || record.storage_prefix() != current.storage_prefix() || record.storage_namespaces != current.storage_namespaces
            || record.ensemble != current.ensemble || record.replica_journals != current.replica_journals
            || record.log_copy_servers != current.log_copy_servers || record.log_epoch != current.log_epoch
            || record.engine_pin != current.engine_pin || record.move_from.is_some()
        {
            return Err(conflict());
        }
        record.lifecycle = Lifecycle::Moving;
        record.epoch = record.epoch.checked_add(1).ok_or_else(|| {
            Anomaly::fault("rtdb.multi/epoch-exhausted", format!("Installation {} has no remaining writer epochs", record.id))
        })?;
        record.holder = None;
        record.move_intent = None;
        record.placement_move = None;
        record.storage_namespaces = bound_source.storage_namespaces;
        record.replica_journals.extend(source_stream.servers.iter().cloned());
        record.cell_transfer = Some(Box::new(CellTransfer {
            intent: intent.clone(),
            source_incarnation: record.incarnation,
            destination_incarnation: arrival.incarnation,
            epoch: record.epoch,
            source_media: source_media.clone(),
            destination_media: destination_media.clone(),
        }));
        Ok(())
    }).await?;
    let staged = if let Some(pending) = pending {
        pending
    } else {
        let mut staged = source.clone();
        staged.org = arrival.org;
        staged.incarnation = arrival.incarnation;
        staged.ensemble = arrival.ensemble;
        staged.storage_generation = Some(uuid::Uuid::new_v4());
        staged.storage_namespaces = Some(target_binding);
        staged.disposal = None;
        staged.moved_at_ms = None;
        staged.source_cleanup = None;
        staged.cell_move = None;
        staged.deletion_observation = None;
        staged.deletion_record = None;
        staged.s3_write_lease_until_ms = None;
        staged.log_copy_servers.clear();
        staged.replica_journals = target_stream.servers.iter().cloned().collect();
        staged.move_from = Some(source_stream.servers.clone());
        staged.storage_regions.clear();
        staged.checkpoint_recovery = None;
        staged.checkpoint_captures.clear();
        staged.checkpoint_capture_retirements.clear();
        staged.archive_retirement = None;
        staged.retired_residences.clear();
        staged.activation_crashes = 0;
        if let Some(prior) = &prior_residence {
            staged.retired_residences.clone_from(&prior.retired_residences);
            staged.retired_residences.insert(prior.incarnation, RetiredResidence {
                transfer: prior.cell_transfer.as_deref().cloned().ok_or_else(&conflict)?,
                serving: prior.disposal.as_deref().cloned().ok_or_else(&conflict)?,
                archives: prior.archive_retirement.clone(),
            });
            if prior.archive_retirement.is_none() {
                staged.checkpoint_captures.clone_from(&prior.checkpoint_captures);
                staged.checkpoint_capture_retirements.clone_from(&prior.checkpoint_capture_retirements);
            }
        }
        let result = if let Some(prior) = prior_residence {
            to.directory.replace_deployment(staged.clone(), prior.version).await
                .and_then(|result| result.applied().ok_or_else(&conflict))
        } else {
            to.directory.create_deployment(staged.clone()).await
        };
        match result {
            Ok(created) => created,
            Err(error) => to.directory.deployment(&staged.id).await?.filter(|record| {
                same_cell_move(record, &staged) && record.epoch == staged.epoch
                    && record.ensemble == staged.ensemble && record.replica_journals == staged.replica_journals
                    && record.storage_namespaces == staged.storage_namespaces
                    && record.lifecycle == Lifecycle::Moving
            }).ok_or(error)?,
        }
    };
    if staged.move_from.is_none() {
        return Ok((source, staged));
    }

    if staged.move_from.as_ref() != Some(&source_stream.servers) {
        return Err(conflict());
    }
    rtdb_logkeeper::seal(&source_stream, source.epoch).await?;
    await_storage_drain(from.directory.as_ref(), from.storage, from.keys, &source).await?;
    crate::store::reconcile_storage_regions(to.directory.as_ref(), to.storage, to.keys, &staged).await?;
    stage_cell_storage(from, to, &source, &staged, hosted_task).await?;

    rtdb_logkeeper::copy_retained(source_stream, target_stream, source.epoch).await?;
    let prepared = update_installation(to.directory.as_ref(), &staged.id, |record| {
        if !same_cell_move(record, &staged) || record.lifecycle != Lifecycle::Moving || record.epoch != staged.epoch {
            return Err(conflict());
        }
        require_storage_drained(record)?;
        record.move_from = None;
        Ok(())
    }).await?;
    Ok((source, prepared))
}

async fn stage_cell_storage(
    from: &CellHandle<'_>,
    to: &CellHandle<'_>,
    source: &DeploymentRecord,
    staged: &DeploymentRecord,
    hosted_task: Option<rtdb_directory::HostedTask>,
) -> Result<()> {
    use rtdb_directory::StorageRegion;
    use rtdb_storage::IoRegion;
    let local = matches!(to.storage, CellStorage::Local { .. });
    let region = uuid::Uuid::new_v4();
    let witness = if local { to.keys.hold_local_storage_region(region)? } else { None };
    let mut io = IoRegion::new().child_holding(witness.map(|file| Arc::new(file) as Arc<dyn Send + Sync>));
    if !local {
        crate::store::track_transfer_publications(&mut io, to.directory.clone(), staged.id.clone(), region, staged.epoch);
    }
    let admitted = update_installation(to.directory.as_ref(), &staged.id, |record| {
        if !same_cell_move(record, staged) || record.epoch != staged.epoch
            || record.lifecycle != Lifecycle::Moving || record.move_from != staged.move_from
        {
            return Err(cell_move_conflict(from, to, &staged.name));
        }
        record.storage_regions.insert(region, if local {
            StorageRegion::Local { archive: None }
        } else {
            crate::store::remote_storage_region(None, hosted_task.clone())
        });
        Ok(())
    }).await;
    let uncertain = admitted.as_ref().is_err_and(|error| error.code != "rtdb.multi/cell-transfer");
    let result = async {
        admitted?;
        let material = from.keys.existing_key(&source.key_id()).await?;
        if source.checkpoint.is_some() && material.is_none() {
            return Err(Anomaly::fault("rtdb.multi/transfer-key", "The source checkpoint has no key in its original vault"));
        }
        let source_bucket = from.storage.bucket(IoRegion::new()).await?;
        let target_bucket = to.storage.bucket(io.clone()).await?;
        crate::reference::materialize(
            source_bucket.as_ref(), &source.storage_prefix(), target_bucket.as_ref(), &staged.storage_prefix(),
        ).await?;
        if let Some(archive) = crate::archive::ArchivedCheckpoint::load(source, from.keys, IoRegion::new()).await? {
            archive.materialize(staged, Arc::clone(&target_bucket)).await?;
        }
        if let Some(checkpoint) = staged.checkpoint {
            crate::reference::verify_block_pack(target_bucket.as_ref(), &staged.storage_prefix(), checkpoint.block).await?;
        }
        // A never-opened source need not provision a key merely to move.
        // The source vault is read-only for the transfer operator.
        if let Some(material) = material {
            to.keys.install(&staged.key_id(), material).await?;
        }
        Ok(())
    }.await;
    io.close_and_wait().await;
    drop(io);
    crate::store::release_region(to.directory.as_ref(), &staged.id, region, uncertain).await?;
    if local {
        to.keys.forget_local_storage_region(region)?;
    }
    result
}

pub(crate) async fn update_installation(
    directory: &dyn Directory,
    id: &str,
    change: impl Fn(&mut DeploymentRecord) -> Result<()>,
) -> Result<DeploymentRecord> {
    for _ in 0..8 {
        let current = directory.deployment(id).await?.ok_or_else(|| {
            Anomaly::incorrect("rtdb.multi/unknown-installation", format!("No installation has ID {id}"))
        })?;
        let mut next = current.clone();
        change(&mut next)?;
        if next == current {
            return Ok(current);
        }
        if let Cas::Applied(stored) = directory.replace_deployment(next, current.version).await? {
            return Ok(stored);
        }
    }
    Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/installation-contended", format!("Installation {id} kept changing")))
}

pub(crate) async fn administration_stream(directory: &dyn Directory, record: &DeploymentRecord, maximum_message_bytes: usize) -> Result<StreamConfig> {
    let members = members(directory).await?;
    let servers: Vec<String> = record.ensemble.iter().map(|id| {
        members.get(id).map(|member| member.address.clone()).ok_or_else(|| {
            Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/administration-ensemble", format!("Logkeeper {id} is missing from the directory"))
        })
    }).collect::<Result<_>>()?;
    Ok(StreamConfig { write_quorum: write_quorum(record.ensemble.len()), servers, stream: record.stream("replica"), maximum_message_bytes })
}

/// Accepts serving disposal by fencing this installation in the directory.
pub async fn begin_disposal(directory: &dyn Directory, id: &str) -> Result<DeploymentRecord> {
    begin_disposal_at(directory, id, None).await
}

async fn begin_disposal_at(directory: &dyn Directory, id: &str, incarnation: Option<u64>) -> Result<DeploymentRecord> {
    update_installation(directory, id, |record| {
        if incarnation.is_some_and(|expected| expected != record.incarnation) {
            return Err(Anomaly::incorrect("rtdb.multi/disposal-identity", "The request names another residence"));
        }
        match record.lifecycle {
            Lifecycle::Deleted if record.disposal.is_some() => Ok(()),
            Lifecycle::Deleting => Ok(()),
            Lifecycle::Live | Lifecycle::Quarantined | Lifecycle::Restoring
                if record.move_from.is_none()
                    && (record.lifecycle != Lifecycle::Restoring || record.checkpoint_stage.is_some()) =>
            {
                record.lifecycle = Lifecycle::Deleting;
                record.epoch = record.epoch.checked_add(1).ok_or_else(|| {
                    Anomaly::fault("rtdb.multi/epoch-exhausted", format!("Installation {id} has no remaining writer epochs"))
                })?;
                record.holder = None;
                record.move_intent = None;
                Ok(())
            }
            _ => Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/not-disposable", format!("Installation {id} cannot be disposed in this cell"))),
        }
    }).await
}

/// The hosting catalog can accept retirement before provisioning reaches the directory.
/// Reserve that exact identity as retiring so a late install cannot open it.
pub(crate) async fn begin_hosted_disposal(directory: &dyn Directory, id: uuid::Uuid, name: &str, org: &str, incarnation: u64) -> Result<DeploymentRecord> {
    if id.is_nil() || incarnation == 0 || !valid_installation_name(name) || !valid_installation_name(org) {
        return Err(Anomaly::incorrect("rtdb.multi/disposal-identity", "Invalid hosted retirement scope"));
    }
    let id = id.to_string();
    let installed = match directory.deployment_by_name(name).await? {
        Some(installed) => installed,
        None => {
            if incarnation != 1 {
                return Err(Anomaly::incorrect("rtdb.multi/disposal-identity", "A transferred residence must already exist"));
            }
            let mut record = DeploymentRecord::new(name, org, Vec::new(), now_ms());
            record.id = id.clone();
            record.lineage = id.clone();
            record.lifecycle = Lifecycle::Deleting;
            record.epoch = 1;
            match directory.create_deployment(record).await {
                Ok(accepted) => return Ok(accepted),
                Err(error) => directory.deployment_by_name(name).await?.ok_or(error)?,
            }
        }
    };
    if installed.id != id || installed.org != org || installed.parent.is_some() {
        return Err(Anomaly::incorrect("rtdb.multi/disposal-identity", "The requested name belongs to another installation"));
    }
    begin_disposal_at(directory, &id, Some(incarnation)).await
}

/// Fences and removes an installation's serving storage. The tombstone retains its receipt.
pub async fn delete_installation(directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, id: &str, maximum_message_bytes: usize) -> Result<DisposalReceipt> {
    let fence = begin_disposal(directory, id).await?;
    await_storage_drain(directory, storage, keys, &fence).await?;
    finish_disposal_at(directory, storage, keys, id, Some(fence.incarnation), maximum_message_bytes).await
}

/// Settles a disposal previously accepted by the directory.
pub async fn finish_disposal(directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, id: &str, maximum_message_bytes: usize) -> Result<DisposalReceipt> {
    finish_disposal_at(directory, storage, keys, id, None, maximum_message_bytes).await
}

pub(crate) async fn finish_disposal_at(
    directory: &dyn Directory,
    storage: &CellStorage,
    keys: &dyn KeyService,
    id: &str,
    incarnation: Option<u64>,
    maximum_message_bytes: usize,
) -> Result<DisposalReceipt> {
    let _maintenance = storage.try_local_maintenance()?;
    finish_disposal_with_lock(directory, storage, keys, id, incarnation, maximum_message_bytes).await
}

async fn finish_disposal_with_lock(
    directory: &dyn Directory,
    storage: &CellStorage,
    keys: &dyn KeyService,
    id: &str,
    incarnation: Option<u64>,
    maximum_message_bytes: usize,
) -> Result<DisposalReceipt> {
    let mut fenced = directory.deployment(id).await?.ok_or_else(|| {
        Anomaly::incorrect("rtdb.multi/unknown-installation", format!("No installation has ID {id}"))
    })?;
    let expected = incarnation.unwrap_or(fenced.incarnation);
    let earlier = |record: &DeploymentRecord| {
        record.retired_residences.get(&expected).map(|residence| residence.serving.clone())
            .ok_or_else(|| Anomaly::incorrect("rtdb.multi/disposal-identity", "The request names another residence"))
    };
    if fenced.incarnation != expected {
        return earlier(&fenced);
    }
    if let Some(stage) = &fenced.checkpoint_stage {
        if cell_media(storage, keys)? != stage.media {
            return Err(Anomaly::incorrect("rtdb.multi/stage-storage-custody", "The staged checkpoint belongs to different storage or keys"));
        }
    }
    if (fenced.lifecycle == Lifecycle::Deleting || fenced.moved_at_ms.is_some()) && !fenced.storage_regions.is_empty() {
        crate::store::reconcile_storage_regions(directory, storage, keys, &fenced).await?;
        fenced = directory.deployment(id).await?.ok_or_else(|| {
            Anomaly::incorrect("rtdb.multi/unknown-installation", format!("No installation has ID {id}"))
        })?;
    }
    if fenced.incarnation != expected {
        return earlier(&fenced);
    }
    match (fenced.lifecycle, fenced.disposal.as_deref()) {
        (Lifecycle::Deleted, Some(receipt)) => return Ok(receipt.clone()),
        (Lifecycle::Deleting, None) => {},
        (Lifecycle::Deleted, None) if fenced.moved_at_ms.is_some() => {},
        _ => return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/disposal-unaccepted", format!("The directory has not accepted disposal of {id}"))),
    }
    require_storage_drained(&fenced)?;
    if fenced.moved_at_ms.is_some() {
        let media = cell_media(storage, keys)?;
        if !fenced.cell_transfer.as_ref().is_some_and(|transfer| transfer.source_media == media) {
            return Err(Anomaly::incorrect("rtdb.multi/move-storage-custody", "The moved source is bound to different storage or keys"));
        }
    }
    if matches!(storage, CellStorage::S3 { .. }) && fenced.deletion_observation.is_none() {
        advance_observation(directory, &fenced, Some(DeletionObservation::Fenced {
            leases_expire_at_ms: s3_write_lease_horizon(now_ms()),
        })).await?;
        fenced = directory.deployment(id).await?.ok_or_else(|| deletion_changed(id))?;
    }
    let wait_until = match fenced.deletion_observation {
        Some(DeletionObservation::Fenced { leases_expire_at_ms }) => Some(leases_expire_at_ms),
        _ => None,
    }.max(fenced.s3_write_lease_until_ms);
    if let Some(deadline) = wait_until.filter(|&deadline| now_ms() < deadline) {
        return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb.multi/storage-lease-active", format!("The storage write lease expires at Unix milliseconds {deadline}")));
    }
    let mut stream = if fenced.moved_at_ms.is_some() {
        StreamConfig {
            write_quorum: write_quorum(fenced.replica_journals.len()),
            servers: Vec::new(),
            stream: fenced.stream("replica"),
            maximum_message_bytes,
        }
    } else {
        administration_stream(directory, &fenced, maximum_message_bytes).await?
    };
    let bucket = storage.bucket(rtdb_storage::IoRegion::new()).await?;
    let namespaces = storage_namespaces(bucket.as_ref(), keys).await?;
    if fenced.storage_namespaces.is_none() {
        let configured = configured_cell_storage(directory, &fenced.name).await?;
        let epoch = fenced.epoch;
        fenced = update_installation(directory, id, |record| {
            if record.lifecycle != Lifecycle::Deleting || record.epoch != epoch {
                return Err(deletion_changed(&record.name));
            }
            if record.storage_namespaces.is_none() {
                record.storage_namespaces = Some(configured);
            }
            require_storage_namespaces(record, namespaces)
        }).await?;
        require_storage_drained(&fenced)?;
    }
    require_storage_namespaces(&fenced, namespaces)?;
    let prefix = crate::reference::key(&fenced.storage_prefix())?;
    let others: Vec<DeploymentRecord> = directory.deployments().await?.into_iter()
        .filter(|record| record.id != fenced.id).collect();
    crate::reference::require_disjoint_prefix(&prefix, others.iter())?;
    stream.servers.extend(fenced.replica_journals.iter().cloned());
    stream.servers.extend(fenced.log_copy_servers.iter().cloned());
    if let Some(previous) = &fenced.move_from { stream.servers.extend(previous.iter().cloned()); }
    stream.servers.sort();
    stream.servers.dedup();
    stream.write_quorum = write_quorum(stream.servers.len());
    if !stream.servers.is_empty() {
        rtdb_logkeeper::retire(&stream, fenced.epoch).await?;
    }
    let journals_retired_at_ms = now_ms();
    let (signed, serving_inventory) = match storage {
        CellStorage::Local { .. } => erase_and_sign_disposal(
            storage, bucket.as_ref(), keys, &fenced, &prefix, &others, namespaces, journals_retired_at_ms,
        ).await?,
        CellStorage::S3 { config } => {
            let (first, last) = observe_s3_inventory(directory, storage, config, bucket.as_ref(), &fenced).await?;
            let signed = sign_disposal(&fenced, keys, namespaces, DeletionInventory::AllVersionsAndMultipartUploads, Some(first), last).await?;
            (signed, ServingInventory::S3VersionsAndUploads)
        }
    };
    let key_withdrawn_at_ms = signed.facts.key_destroyed_at_ms;
    let receipt = DisposalReceipt {
        deployment: fenced.id,
        incarnation: fenced.incarnation,
        epoch: fenced.epoch,
        journals: stream.servers.into_iter().collect(),
        journals_retired_at_ms,
        key_withdrawn_at_ms,
        serving_prefix_observed_empty_at_ms: now_ms().max(key_withdrawn_at_ms),
        serving_inventory,
    };
    let deleted = update_installation(directory, id, |record| {
        if record.epoch != receipt.epoch || record.incarnation != receipt.incarnation
            || record.storage_prefix() != prefix.as_str() || record.storage_namespaces != Some(namespaces) {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/disposal-fence", format!("Installation {id} changed during disposal")));
        }
        match record.lifecycle {
            Lifecycle::Deleted if record.disposal.is_some() => Ok(()),
            Lifecycle::Deleting | Lifecycle::Deleted if record.lifecycle == Lifecycle::Deleting || record.moved_at_ms.is_some() => {
                if let Some(first_empty_at_ms) = signed.facts.first_empty_observed_at_ms
                    && record.deletion_observation != Some(DeletionObservation::Verified {
                        first_empty_at_ms, last_empty_at_ms: signed.facts.objects_erased_at_ms,
                    })
                {
                    return Err(deletion_changed(id));
                }
                record.disposal = Some(Box::new(receipt.clone()));
                record.deletion_record = Some(Box::new(signed.clone()));
                record.lifecycle = Lifecycle::Deleted;
                Ok(())
            }
            Lifecycle::Deleted if record.moved_at_ms.is_some() => {
                record.disposal = Some(Box::new(receipt.clone()));
                Ok(())
            }
            _ => Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/disposal-fence", format!("Installation {id} left disposal"))),
        }
    }).await?;
    deleted.disposal.map(|receipt| *receipt).ok_or_else(|| Anomaly::fault("rtdb.multi/disposal-receipt", format!("Installation {id} lacks a disposal receipt")))
}

async fn erase_and_sign_disposal(
    storage: &CellStorage,
    bucket: &dyn rtdb_storage::ObjectStore,
    keys: &dyn KeyService,
    fenced: &DeploymentRecord,
    prefix: &rtdb_storage::ObjectKey,
    others: &[DeploymentRecord],
    namespaces: StorageNamespaces,
    journals_retired_at_ms: i64,
) -> Result<(DeletionRecord, ServingInventory)> {
    // Its forks keep what they share with it: those objects are copied into them before it goes.
    if !crate::reference::erase_deployment(bucket, prefix.as_str(), others).await? {
        return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/disposal-prefix", format!("Installation {} still has serving objects", fenced.id)));
    }
    let serving_inventory = storage.purge_serving_inventory(prefix).await?;
    let objects_erased_at_ms = now_ms().max(journals_retired_at_ms);
    let signed = sign_disposal(fenced, keys, namespaces, DeletionInventory::CurrentObjects, None, objects_erased_at_ms).await?;
    Ok((signed, serving_inventory))
}

async fn sign_disposal(
    fenced: &DeploymentRecord, keys: &dyn KeyService, namespaces: StorageNamespaces,
    inventory: DeletionInventory, first_empty_observed_at_ms: Option<i64>, objects_erased_at_ms: i64,
) -> Result<DeletionRecord> {
    keys.destroy(&fenced.key_id()).await?;
    keys.sign_deletion(DeletionFacts {
        deployment: fenced.id.clone(), name: fenced.name.clone(), epoch: fenced.epoch,
        storage_prefix: fenced.storage_prefix(), namespaces, inventory,
        key_destroyed_at_ms: now_ms().max(objects_erased_at_ms), objects_erased_at_ms,
        first_empty_observed_at_ms, verified_empty: true,
    }).await
}

fn require_storage_drained(record: &DeploymentRecord) -> Result<()> {
    if !record.storage_regions.is_empty() {
        return Err(Anomaly::new(
            AnomalyCategory::Unavailable,
            "rtdb.multi/storage-not-drained",
            format!("Installation {} still has admitted storage regions", record.id),
        ));
    }
    Ok(())
}

pub(crate) async fn await_storage_drain(directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, fence: &DeploymentRecord) -> Result<()> {
    let start = tokio::time::Instant::now();
    let mut current = fence.clone();
    loop {
        let pending = match require_storage_drained(&current) {
            Ok(()) => return Ok(()),
            Err(pending) => pending,
        };
        if start.elapsed() >= CONTROL_TIMEOUT {
            return Err(pending);
        }
        crate::store::reconcile_storage_regions(directory, storage, keys, &current).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        current = directory.deployment(&fence.id).await?
            .filter(|current| current.epoch == fence.epoch && (
                current.lifecycle == fence.lifecycle
                    || fence.lifecycle == Lifecycle::Deleting && current.lifecycle == Lifecycle::Deleted
            ))
            .ok_or_else(|| Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/storage-drain-fence", "The installation left its storage drain fence"))?;
    }
}

async fn deletion_neighbors(directory: &dyn Directory, deployment: &DeploymentRecord) -> Result<Vec<DeploymentRecord>> {
    Ok(directory.deployments().await?.into_iter().filter(|record| record.id != deployment.id).collect())
}

fn observation_pending(first_empty_at_ms: i64) -> Anomaly {
    let deadline = s3_write_lease_horizon(first_empty_at_ms);
    Anomaly::new(AnomalyCategory::Busy, "rtdb.multi/deletion-verification-pending", format!("The second empty inventory is due at Unix milliseconds {deadline}"))
}

async fn advance_observation(directory: &dyn Directory, deployment: &DeploymentRecord, observation: Option<DeletionObservation>) -> Result<()> {
    update_installation(directory, &deployment.id, |current| {
        if current.epoch != deployment.epoch || (current.lifecycle != Lifecycle::Deleting && current.moved_at_ms.is_none())
            || current.storage_namespaces != deployment.storage_namespaces || current.storage_prefix() != deployment.storage_prefix()
            || current.deletion_observation != deployment.deletion_observation
        {
            return Err(deletion_changed(&deployment.name));
        }
        current.deletion_observation = observation;
        Ok(())
    }).await?;
    Ok(())
}

async fn observe_s3_inventory(directory: &dyn Directory, storage: &CellStorage, config: &rtdb_s3::S3Config,
                             bucket: &dyn rtdb_storage::ObjectStore, deployment: &DeploymentRecord) -> Result<(i64, i64)> {
    let first_empty_at_ms = match deployment.deletion_observation {
        Some(DeletionObservation::Verified { first_empty_at_ms, last_empty_at_ms }) => {
            if first_empty_at_ms < 0 || last_empty_at_ms < s3_write_lease_horizon(first_empty_at_ms) || last_empty_at_ms > now_ms() {
                return Err(Anomaly::fault("rtdb.multi/deletion-observation", "The stored empty inventory timestamps are inconsistent"));
            }
            return Ok((first_empty_at_ms, last_empty_at_ms));
        }
        Some(DeletionObservation::Empty { observed_at_ms }) => observed_at_ms,
        Some(DeletionObservation::Fenced { .. }) => {
            let others = deletion_neighbors(directory, deployment).await?;
            erase_inventory(storage, bucket, deployment, &others).await?;
            let observed_at_ms = now_ms();
            advance_observation(directory, deployment, Some(DeletionObservation::Empty { observed_at_ms })).await?;
            return Err(observation_pending(observed_at_ms));
        }
        None => return Err(deletion_changed(&deployment.name)),
    };
    if now_ms() < s3_write_lease_horizon(first_empty_at_ms) { return Err(observation_pending(first_empty_at_ms)); }

    let cell = rtdb_storage::ObjectKey::new(crate::deployment::S3_CELL_ROOT)?;
    let inventory = config.versioned_inventory(&cell, &BTreeMap::new()).await?;
    let prefix = rtdb_storage::ObjectKey::new(deployment.storage_prefix())?;
    if !inventory.is_empty(&prefix).await? {
        advance_observation(directory, deployment, Some(DeletionObservation::Fenced { leases_expire_at_ms: now_ms() })).await?;
        return Err(deletion_incomplete(deployment));
    }
    let last_empty_at_ms = now_ms();
    if last_empty_at_ms < s3_write_lease_horizon(first_empty_at_ms) { return Err(observation_pending(first_empty_at_ms)); }
    advance_observation(directory, deployment, Some(DeletionObservation::Verified { first_empty_at_ms, last_empty_at_ms })).await?;
    Ok((first_empty_at_ms, last_empty_at_ms))
}

fn deletion_incomplete(deployment: &DeploymentRecord) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/deletion-incomplete", format!("Storage still contains objects for {}", deployment.name))
}

async fn erase_inventory(storage: &CellStorage, bucket: &dyn rtdb_storage::ObjectStore, deployment: &DeploymentRecord, others: &[DeploymentRecord]) -> Result<DeletionInventory> {
    let storage_prefix = deployment.storage_prefix();
    if !crate::reference::erase_deployment(bucket, &storage_prefix, others).await? { return Err(deletion_incomplete(deployment)); }
    storage.purge_inventory(&rtdb_storage::ObjectKey::new(&storage_prefix)?).await
}

/// What auditing a deployment's history found (`NoTwoWritersHaveEffect`, `SystemTimeRises`).
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct AuditReport {
    pub records: usize,
    pub transactions: usize,
    pub highest_term: i64,
    pub violations: Vec<String>,
    #[serde(default)]
    pub replica_interval: Option<[i64; 2]>,
    #[serde(default)]
    pub replica_digests: BTreeMap<String, String>,
    #[serde(default)]
    pub replica_windows: BTreeMap<String, Option<[i64; 2]>>,
    #[serde(default)]
    pub required_replica_quorum: usize,
    #[serde(default)]
    pub unavailable_replicas: BTreeMap<String, String>,
}

/// Audits the retained replica stream and compares the common committed range on its reachable replicas.
pub async fn audit(directory: &dyn Directory, keys: &dyn KeyService, name: &str, maximum_message_bytes: usize) -> Result<AuditReport> {
    use rtdb_format::envelope::{ReplicaBody, ReplicaEnvelope};
    let current = record(directory, name).await?;
    let servers = replica_servers(directory, &current).await?;
    let stream = StreamConfig { write_quorum: write_quorum(servers.len()), servers, stream: current.stream("replica"), maximum_message_bytes };
    let reader = rtdb_logkeeper::open_reader(stream.clone(), CancellationToken::new()).await?;
    let latest = reader.latest()?.0;
    let log = if latest >= 0 { Some(crate::logs::SealedLog::new(reader, &keys.existing(&current.key_id()).await?, &current.replica_seal_scope())?) } else { None };
    let mut report = AuditReport { required_replica_quorum: stream.write_quorum, ..AuditReport::default() };
    let (mut next, mut last_tx, mut last_time, mut last_source) = (0i64, i64::MIN, i64::MIN, i64::MIN);
    let entries_per_batch = ((16 << 20) / maximum_message_bytes.max(1)).clamp(1, 1024) as i64;
    while let Some(log) = log.as_ref().filter(|_| next <= latest) {
        let records = match log.read_range(rtdb_storage::log::LogOffset(next), rtdb_storage::log::LogOffset((next + entries_per_batch).min(latest + 1))).await {
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
    let retained = rtdb_logkeeper::audit_retained(&stream).await?;
    let mut digests = retained.digests.values();
    if let Some(first) = digests.next() && digests.any(|digest| digest != first) {
        let [from, through] = retained.interval.expect("digests describe an interval");
        report.violations.push(format!("replicas disagree on committed positions {from} through {through}"));
    }
    report.replica_interval = retained.interval;
    report.replica_digests = retained.digests;
    report.replica_windows = retained.windows;
    report.unavailable_replicas = retained.unavailable;
    Ok(report)
}

/// Stops adding placements to a node and lets its agent hand off or park its holdings.
pub async fn drain(directory: &dyn Directory, member: &MemberId) -> Result<()> {
    mark_draining(directory, member, None).await.map(|_| ())
}

async fn mark_draining(directory: &dyn Directory, member: &MemberId, target_engine: Option<&str>) -> Result<bool> {
    for _ in 0..8 {
        let current = directory.member(member).await?.ok_or_else(|| Anomaly::incorrect("rtdb.multi/unknown-member", format!("No member {member}")))?;
        if current.state != MemberState::Eligible { return Ok(false); }
        let mut draining = current.clone();
        draining.state = MemberState::Draining;
        draining.drain_target_engine = target_engine.map(str::to_owned);
        if let Cas::Applied(_) = directory.replace_member(draining, current.version).await? { return Ok(true); }
    }
    Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/drain-contended", "The member kept changing while being drained"))
}

/// Starts drains on older nodes whose current deployments can all run on the requested engine in the same pool.
pub async fn roll_engine(directory: &dyn Directory, version: &str) -> Result<Vec<MemberId>> {
    validate_engine_version(version)?;
    let members = members(directory).await?;
    let now = now_ms();
    let deployments = directory.deployments().await?;
    let mut parked_pins = HashSet::new();
    let mut held: BTreeMap<&MemberId, Vec<&DeploymentRecord>> = BTreeMap::new();
    for record in &deployments {
        let holder = effective_holder(record, &members, now);
        if let Some(holder) = holder { held.entry(holder).or_default().push(record); }
        if let Some((pin, pool)) = record.engine_pin.as_deref().zip(record.writer_pool())
            && holder.and_then(|id| members.get(id)).is_none_or(|member| !member.may_receive_placements(now) || member.control_address.is_none())
        {
            parked_pins.insert((pin, pool, record.storage_namespaces));
        }
    }
    let mut draining = Vec::new();
    for older in members.values().filter(|member| member.engine_version != version && member.may_receive_placements(now)
        && member.control_address.is_some() && member.pool != MemberPool::Compaction)
    {
        let serves_bound_pin = parked_pins.remove(&(older.engine_version.as_str(), older.pool, older.storage_namespaces));
        let serves_unbound_pin = parked_pins.remove(&(older.engine_version.as_str(), older.pool, None));
        if serves_bound_pin || serves_unbound_pin { continue; }
        let targets: Vec<_> = members.values().filter(|target| {
            target.engine_version == version && target.pool == older.pool && target.may_receive_placements(now) && target.control_address.is_some()
        }).collect();
        if targets.is_empty() { continue; }
        if held.get(&older.id).is_some_and(|records| records.iter().any(|record| !targets.iter().any(|target| {
            record.may_place_on(target, now) && record.storage_namespaces.is_none_or(|namespaces| target.storage_namespaces == Some(namespaces))
        }))) { continue; }
        if mark_draining(directory, &older.id, Some(version)).await? { draining.push(older.id.clone()); }
    }
    Ok(draining)
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

pub struct FailedMemberProbe { endpoint: String, consecutive: u32 }

/// Probes every live node's agent, and confirms dead those that failed `suspicion_probes` probes in a row (`ConfirmSuspicion`).
/// Safety never depends on the verdict: a wrongly confirmed node's writes are already fenced by the epoch its successor claims.
pub async fn confirm_suspicions(directory: &dyn Directory, failures: &mut BTreeMap<MemberId, FailedMemberProbe>, options: &ControllerOptions) -> Result<usize> {
    use futures::StreamExt;

    let now = now_ms();
    let mut members = directory.members().await?;
    members.retain(|member| {
        let probe = member.role == MemberRole::Node && member.is_live(now) && member.control_address.is_some();
        if !probe || failures.get(&member.id).is_some_and(|failure| Some(&failure.endpoint) != member.control_address.as_ref()) {
            failures.remove(&member.id);
        }
        probe
    });
    let mut probes = std::pin::pin!(futures::stream::iter(members).map(|member| async move {
        let address = member.control_address.as_deref().expect("members without control addresses were filtered");
        let answered = matches!(control::call(address, &Request::Ping, options.probe_timeout, &options.control).await, Ok(Response::Done));
        (member, answered)
    }).buffer_unordered(CONCURRENT_SUSPICION_PROBES));
    let mut confirmed = 0;
    while let Some((member, answered)) = probes.next().await {
        if answered { failures.remove(&member.id); continue; }
        let failure = failures.entry(member.id.clone()).or_insert_with(|| FailedMemberProbe {
            endpoint: member.control_address.clone().expect("the member was probed at this address"),
            consecutive: 0,
        });
        failure.consecutive = failure.consecutive.saturating_add(1);
        if failure.consecutive >= options.suspicion_probes {
            // The suspect may still be renewing its lease, so the confirmation re-reads and retries rather than losing to a renewal.
            let mut current = member.clone();
            for _ in 0..8 {
                if matches!(current.state, MemberState::Dead | MemberState::Retired) || current.control_address != member.control_address { break; }
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PlacementPoolStatus {
    pub eligible: usize,
    pub draining: usize,
    pub holdings: usize,
    pub recommended_nodes: usize,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PlacementStatus {
    pub general: PlacementPoolStatus,
    pub isolation: PlacementPoolStatus,
    pub nodes: BTreeMap<String, usize>,
    pub logkeepers: usize,
    pub compaction_workers: usize,
}

/// Placement demand per pool, read through the holder index rather than the deployment inventory.
pub async fn placement_status(directory: &dyn Directory, target_per_node: usize) -> Result<PlacementStatus> {
    let members = members(directory).await?;
    let now = now_ms();
    let mut nodes: BTreeMap<_, usize> = members.values().filter(|member| member.may_hold(now) && member.pool != MemberPool::Compaction)
        .map(|member| (&member.id, 0)).collect();
    for record in live_holdings(directory, &members, now).await? {
        if let Some(count) = effective_holder(&record, &members, now).and_then(|holder| nodes.get_mut(holder)) { *count += 1; }
    }
    let pool_status = |pool, minimum| {
        let mut status = PlacementPoolStatus { eligible: 0, draining: 0, holdings: 0, recommended_nodes: 0 };
        for (id, held) in &nodes {
            let member = &members[*id];
            if member.pool != pool { continue; }
            status.holdings += held;
            if member.may_receive_placements(now) { status.eligible += 1; }
            if member.state == MemberState::Draining { status.draining += 1; }
        }
        status.recommended_nodes = status.holdings.div_ceil(target_per_node.max(1)).max(minimum);
        status
    };
    Ok(PlacementStatus {
        general: pool_status(MemberPool::General, 1),
        isolation: pool_status(MemberPool::Isolation, 0),
        nodes: nodes.into_iter().map(|(id, held)| (id.to_string(), held)).collect(),
        logkeepers: members.values().filter(|member| member.role == MemberRole::Logkeeper && member.is_live(now)).count(),
        compaction_workers: members.values().filter(|member| member.role == MemberRole::Node && member.pool == MemberPool::Compaction && member.is_live(now)).count(),
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

    use super::{CellStorage, DeploymentRecord, Directory, KeyService, Lifecycle, create_deployment, delete_deployment, erase_and_sign_disposal};
    use crate::keys::LocalKeyService;

    #[tokio::test]
    async fn a_new_deployment_rejects_an_organisation_with_multiple_path_components() {
        let directory = MemoryDirectory::new();
        let error = create_deployment(directory.as_ref(), "shop", "acme/orders", 0).await.unwrap_err();
        assert_eq!(error.code, "rtdb.multi/invalid-organisation");
        assert!(directory.deployments().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn local_deletion_and_fork_retry_before_mutating_storage_when_maintenance_is_active() {
        let directory = MemoryDirectory::new();
        let data = tempfile::tempdir().unwrap();
        let storage = CellStorage::Local { root: data.path().join("objects") };
        let keys = LocalKeyService::new(data.path().join("keys")).unwrap();
        let deployment = create_deployment(directory.as_ref(), "shop", "acme", 0).await.unwrap();
        let key = keys.key(&deployment.key_id()).await.unwrap();
        let maintenance = storage.try_local_maintenance().unwrap().unwrap();

        let deletion = delete_deployment(directory.as_ref(), &storage, &keys, "shop", 4 << 20).await.unwrap_err();
        assert_eq!(deletion.code, "rtdb.multi/storage-maintenance-busy");
        let fork = super::fork_at_block(directory.as_ref(), &storage, &keys, "shop", "fork", 0, std::time::Duration::from_secs(60)).await.unwrap_err();
        assert_eq!(fork.code, "rtdb.multi/storage-maintenance-busy");
        assert_eq!(directory.deployment(&deployment.id).await.unwrap(), Some(deployment.clone()));
        assert!(directory.deployment_by_name("fork").await.unwrap().is_none());
        assert_eq!(keys.existing(&deployment.key_id()).await.unwrap(), key);

        drop(maintenance);
        assert!(storage.try_local_maintenance().unwrap().is_some());
    }

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
        let key = keys.key(&deployment.key_id()).await.unwrap();
        let bucket = MemoryStore::default();
        let namespaces = super::storage_namespaces(&bucket, &keys).await.unwrap();
        deployment.storage_namespaces = Some(namespaces);
        let prefix = ObjectKey::new(deployment.storage_prefix()).unwrap();
        let object = ObjectKey::new(format!("{}/v06/rows", deployment.storage_prefix())).unwrap();
        bucket.put(&object, Bytes::from_static(b"data")).await.unwrap();

        let error = erase_and_sign_disposal(&storage, &RetainingStore(&bucket), &keys, &deployment, &prefix, &[], namespaces, 0).await.unwrap_err();
        assert_eq!(error.code, "rtdb.multi/disposal-prefix");
        assert_eq!(keys.key(&deployment.key_id()).await.unwrap(), key);

        let (proof, _) = erase_and_sign_disposal(&storage, &bucket, &keys, &deployment, &prefix, &[], namespaces, 0).await.unwrap();
        assert!(proof.facts.verified_empty);
        assert!(bucket.list(Some(&ObjectKey::new(deployment.storage_prefix()).unwrap())).await.unwrap().is_empty());
        assert_eq!(keys.existing(&deployment.key_id()).await.unwrap_err().code, "rtdb.multi/key-destroyed");
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
        let key = keys.key(&source.key_id()).await.unwrap();
        let object = ObjectKey::new(format!("{}/v06/rows", source.storage_prefix())).unwrap();
        bucket.put(&object, Bytes::from_static(b"data")).await.unwrap();

        let mut updated = source.clone();
        updated.read_width = 2;
        let updated = directory.replace_deployment(updated, source.version).await.unwrap().applied().unwrap();
        let child = super::fork_with_tail(directory.as_ref(), bucket.as_ref(), &keys, &source, "child", None, Vec::new(), 4 << 20).await.unwrap();
        assert_eq!(keys.existing(&child.key_id()).await.unwrap(), key);
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
        let key = keys.key(&deployment.key_id()).await.unwrap();
        let storage = CellStorage::Local { root: data.path().join("objects") };

        let error = delete_deployment(directory.as_ref(), &storage, &keys, "shop", 4 << 20).await.unwrap_err();
        assert_eq!(error.code, "rtdb.multi/move-in-progress");
        assert_eq!(directory.deployment(&deployment.id).await.unwrap().as_ref(), Some(&deployment));
        assert_eq!(keys.key(&deployment.key_id()).await.unwrap(), key);
    }
}
