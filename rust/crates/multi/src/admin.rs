//! Cell administration: creating, moving, rebalancing, forking and deleting deployments, and confirming dead members (`CreateDeployment`, `IntendMove`, `Rebalance`, `Fork`, `BeginDeletion`, `EraseAndProve`, `ExpireLease`).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use rtdb_directory::{
    Cas, CellMedia, CellResidence, CellTransfer, CellTransferIntent, DeploymentRecord, Directory,
    DisposalReceipt, Lifecycle, MemberId, MemberRecord, MemberRole, MemberState, PreparedCellTransfer,
    RetiredResidence, effective_holder, now_ms,
};
use rtdb_logkeeper::StreamConfig;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio_util::sync::CancellationToken;

use crate::control::{self, ControlKey, Request, Response};
use crate::deployment::{CellStorage, write_quorum};
use crate::keys::KeyService;

const CONTROL_TIMEOUT: Duration = Duration::from_secs(35);

/// Creates a deployment, choosing its ensemble from the live logkeepers in its affinity order.
/// Allocates identity and nothing else: no objects, no log state, no compute.
pub async fn create_deployment(directory: &dyn Directory, name: &str, org: &str, replicas: usize) -> Result<DeploymentRecord> {
    let mut record = DeploymentRecord::new(name, org, Vec::new(), now_ms());
    record.ensemble = choose_ensemble(directory, &record, replicas).await?;
    directory.create_deployment(record).await
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
    let logkeepers: Vec<MemberRecord> = directory.members().await?.into_iter().filter(|member| member.role == MemberRole::Logkeeper && member.state == MemberState::Eligible && member.is_live(now)).collect();
    let ranked = rtdb_affinity::rank(&record.storage_identity(), &logkeepers, |member| member.id.as_str());
    let mut endpoints = BTreeSet::new();
    let ensemble: Vec<_> = ranked.into_iter()
        .filter(|member| endpoints.insert(member.address.as_str()))
        .take(replicas).map(|member| member.id.clone()).collect();
    if ensemble.len() != replicas {
        return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/too-few-logkeepers",
            format!("{replicas} distinct logkeepers are needed and {} are live", ensemble.len())));
    }
    Ok(ensemble)
}

async fn members(directory: &dyn Directory) -> Result<BTreeMap<MemberId, MemberRecord>> {
    Ok(directory.members().await?.into_iter().map(|member| (member.id.clone(), member)).collect())
}

async fn record(directory: &dyn Directory, name: &str) -> Result<DeploymentRecord> {
    directory.deployment_by_name(name).await?.ok_or_else(|| Anomaly::incorrect("rtdb.multi/unknown-deployment", format!("No deployment is named {name}")))
}

/// Applies `change` to the named deployment's current record until a conditional replace succeeds.
async fn update(directory: &dyn Directory, name: &str, change: impl Fn(&mut DeploymentRecord) -> Result<()>) -> Result<DeploymentRecord> {
    for _ in 0..8 {
        let current = record(directory, name).await?;
        let mut next = current.clone();
        change(&mut next)?;
        if let Cas::Applied(stored) = directory.replace_deployment(next, current.version).await? { return Ok(stored); }
    }
    Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/update-contended", format!("{name} kept changing while being updated")))
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
    let nodes: Vec<&MemberRecord> = members.values().filter(|member| member.role == MemberRole::Node && member.may_receive_placements(now_ms())).collect();
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

/// A planned move: reserves the next claim for `target`, has the holder park, and has `target` claim.
pub async fn move_deployment(directory: &dyn Directory, name: &str, target: &MemberId, control: &ControlKey) -> Result<()> {
    let members = members(directory).await?;
    if !members.get(target).is_some_and(|member| member.may_receive_placements(now_ms())) {
        return Err(Anomaly::incorrect("rtdb.multi/not-placeable", format!("{target} may not take placements")));
    }
    let intended = update(directory, name, |record| { record.move_intent = Some(target.clone()); Ok(()) }).await?;
    if let Some(holder) = effective_holder(&intended, &members, now_ms()).filter(|holder| *holder != target) {
        expect(&control_address(&members, holder)?, Request::Park { name: name.to_owned() }, control).await?;
    }
    match expect(&control_address(&members, target)?, Request::Activate { name: name.to_owned() }, control).await? {
        Response::Held { .. } => Ok(()),
        other => Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/move-failed", format!("The target answered {other:?}"))),
    }
}

/// Moves at most `moves` deployments from the most loaded nodes to the least loaded, where their holding counts differ by more than one.
pub async fn rebalance(directory: &dyn Directory, moves: usize, control: &ControlKey) -> Result<usize> {
    let members = members(directory).await?;
    let now = now_ms();
    let mut load: BTreeMap<MemberId, Vec<String>> = members.values().filter(|member| member.role == MemberRole::Node && member.may_receive_placements(now))
        .map(|member| (member.id.clone(), Vec::new())).collect();
    for deployment in directory.deployments().await? {
        if let Some(holder) = effective_holder(&deployment, &members, now) { if let Some(held) = load.get_mut(holder) { held.push(deployment.name.clone()); } }
    }
    let mut moved = 0;
    while moved < moves {
        let Some((busiest, busy)) = load.iter().max_by_key(|(_, held)| held.len()).map(|(id, held)| (id.clone(), held.len())) else { break };
        let Some((idlest, idle)) = load.iter().min_by_key(|(_, held)| held.len()).map(|(id, held)| (id.clone(), held.len())) else { break };
        if busy <= idle + 1 { break; }
        let name = load.get_mut(&busiest).and_then(Vec::pop).expect("the busiest node holds something");
        move_deployment(directory, &name, &idlest, control).await?;
        load.get_mut(&idlest).expect("listed").push(name);
        moved += 1;
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
    update(directory, name, |record| { if record.lifecycle == Lifecycle::Quarantined { record.lifecycle = Lifecycle::Live; } record.activation_crashes = 0; Ok(()) }).await
}

async fn fork_source(directory: &dyn Directory, name: &str) -> Result<DeploymentRecord> {
    let source = record(directory, name).await?;
    if !source.can_fork() {
        return Err(fork_unavailable(name));
    }
    Ok(source)
}

fn fork_unavailable(name: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/fork-unavailable", format!("{name} changed or cannot be forked in its current state"))
}

/// Forks a flushed parent into the same lineage. The child references its
/// objects and starts on fresh logs.
pub async fn fork(directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, parent: &str, name: &str, control: &ControlKey) -> Result<DeploymentRecord> {
    let holder = ensure_held(directory, parent, control).await?;
    let members = members(directory).await?;
    expect(&control_address(&members, &holder)?, Request::Flush { name: parent.to_owned() }, control).await?;
    let source = fork_source(directory, parent).await?;
    fork_with_tail(directory, storage, keys, &source, name, None, Vec::new(), 4 << 20).await
}

/// Forks `parent` as of one of its past blocks: the fork's state is that block's, and what the parent wrote after it is not the fork's.
/// Only a block younger than the reaper's `grace` can be forked from: an object it needs that the parent released since is erased only once the grace has passed.
pub async fn fork_at_block(directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, parent: &str, name: &str, block: i64, grace: std::time::Duration) -> Result<DeploymentRecord> {
    let source = fork_source(directory, parent).await?;
    let bucket = storage.bucket(rtdb_storage::IoRegion::new()).await?;
    let age = now_ms() - crate::reference::written_at_ms(bucket.as_ref(), &source.storage_prefix(), block).await?;
    if age >= grace.as_millis() as i64 { return Err(Anomaly::incorrect("rtdb.multi/block-too-old", format!("Block {block} was written {}s ago, beyond the reaper's grace of {}s", age / 1000, grace.as_secs()))); }
    fork_with_tail(directory, storage, keys, &source, name, Some(block), Vec::new(), 4 << 20).await
}

/// Forks `parent` as of a system time at or after its checkpoint: the fork starts from the checkpoint's block, with the parent's transactions after it up to `system_time` copied into its own stream.
/// A time before the checkpoint is a past block's, which `fork_at_block` forks from.
pub async fn fork_at_time(directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, parent: &str, name: &str, system_time: rtdb_types::Timestamp, grace: std::time::Duration, maximum_message_bytes: usize) -> Result<DeploymentRecord> {
    let source = fork_source(directory, parent).await?;
    let checkpoint = source.checkpoint.ok_or_else(|| Anomaly::incorrect("rtdb.multi/no-checkpoint", format!("{parent} has published no block to fork from")))?;
    if system_time.0 < checkpoint.system_time { return Err(Anomaly::incorrect("rtdb.multi/before-checkpoint", format!("{parent}'s last published block is after that time: fork at a past block instead"))); }
    let bucket = storage.bucket(rtdb_storage::IoRegion::new()).await?;
    if now_ms() - crate::reference::written_at_ms(bucket.as_ref(), &source.storage_prefix(), checkpoint.block).await? >= grace.as_millis() as i64 {
        return Err(Anomaly::incorrect("rtdb.multi/block-too-old", format!("{parent}'s last published block is older than the reaper's grace")));
    }
    let members = members(directory).await?;
    let servers: Vec<String> = source.ensemble.iter().filter_map(|id| members.get(id).map(|member| member.address.clone())).collect();
    let stream = StreamConfig { write_quorum: write_quorum(servers.len()), servers, stream: source.stream("replica"), maximum_message_bytes };
    let material = fork_key(keys, &source).await?;
    let reader = crate::logs::SealedLog::new(rtdb_logkeeper::open_reader(stream.clone(), CancellationToken::new()).await?, &material, &source.replica_seal_scope())?;
    let tail = crate::logs::transactions_until(&reader, checkpoint.boundary, system_time).await?;
    fork_with_tail(directory, storage, keys, &source, name, Some(checkpoint.block), tail, maximum_message_bytes).await
}

async fn fork_key(keys: &dyn KeyService, source: &DeploymentRecord) -> Result<[u8; crate::keys::KEY_BYTES]> {
    keys.existing_key(&source.key_id()).await?.ok_or_else(|| fork_unavailable(&source.name))
}

async fn fork_with_tail(directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, source: &DeploymentRecord, name: &str, at_block: Option<i64>, mut tail: Vec<rtdb_format::envelope::ReplicaEnvelope>, maximum_message_bytes: usize) -> Result<DeploymentRecord> {
    let material = fork_key(keys, source).await?;
    let mut child = DeploymentRecord::new(name, source.org.clone(), Vec::new(), now_ms());
    child.lineage = source.lineage.clone();
    child.parent = Some(source.id.clone());
    child.log_epoch = source.log_epoch + 1;
    // The parent's checkpoint names positions in the parent's logs, not the fork's: the fork's first holder finds its block by listing, and publishes the fork's own.
    child.ensemble = choose_ensemble(directory, &child, source.ensemble.len()).await?;
    let stream = if tail.is_empty() {
        None
    } else {
        child.epoch = 1;
        crate::logs::renumber_into(&mut tail, rtdb_types::StorageEpoch(child.log_epoch))?;
        let stream = administration_stream(directory, &child, maximum_message_bytes).await?;
        child.replica_journals.extend(stream.servers.iter().cloned());
        Some(stream)
    };
    let io = rtdb_storage::IoRegion::new();
    let bucket = storage.bucket(io.clone()).await?;
    let prepared = async {
        crate::reference::fork(bucket.as_ref(), &source.storage_prefix(), &child.storage_prefix(), at_block).await?;
        keys.install(&child.key_id(), material).await?;
        if let Some(stream) = &stream {
            use rtdb_storage::log::Log;
            let writer = rtdb_logkeeper::open_writer(stream.clone(), child.epoch, CancellationToken::new()).await?;
            let writer = crate::logs::SealedLog::new(writer, &material, &child.replica_seal_scope())?;
            for envelope in tail {
                writer.enqueue(bytes::Bytes::from(envelope.encode()), rtdb_types::Timestamp(now_ms() * 1000)).await?.durable().await?;
            }
        }
        Ok(())
    }.await;
    let result = match prepared {
        Ok(()) => publish_fork(directory, source, &child).await,
        Err(error) => Err(ForkPublicationError::Rejected(error)),
    };
    io.close_and_wait().await;
    match result {
        Ok(child) => Ok(child),
        Err(ForkPublicationError::Uncertain(error)) => Err(error),
        Err(ForkPublicationError::Rejected(error)) => {
            if let Err(cleanup) = discard_fork(storage, keys, &child, stream.as_ref()).await {
                eprintln!("RTDB fork cleanup is pending for {}: {}", child.id, cleanup.code);
            }
            Err(error)
        }
    }
}

enum ForkPublicationError {
    Rejected(Anomaly),
    Uncertain(Anomaly),
}

async fn publish_fork(directory: &dyn Directory, source: &DeploymentRecord, child: &DeploymentRecord) -> std::result::Result<DeploymentRecord, ForkPublicationError> {
    for _ in 0..8 {
        let current = directory.deployment(&source.id).await.map_err(ForkPublicationError::Rejected)?
            .filter(|current| current.can_fork() && current.epoch == source.epoch
                && current.log_epoch == source.log_epoch && current.storage_prefix() == source.storage_prefix())
            .ok_or_else(|| ForkPublicationError::Rejected(fork_unavailable(&source.name)))?;
        match directory.create_fork(child.clone(), current.version).await {
            Ok(stored) => return Ok(stored),
            Err(error) if error.code == "rtdb.directory/fork-parent-changed" => continue,
            Err(error) => {
                // The transaction can commit even when its reply is lost.
                match directory.deployment(&child.id).await {
                    Ok(Some(stored)) if stored.parent == child.parent && stored.name == child.name => return Ok(stored),
                    Ok(None) if error.code == "rtdb.directory/exists" => return Err(ForkPublicationError::Rejected(error)),
                    _ => return Err(ForkPublicationError::Uncertain(error)),
                }
            }
        }
    }
    Err(ForkPublicationError::Rejected(fork_unavailable(&source.name)))
}

async fn discard_fork(storage: &CellStorage, keys: &dyn KeyService, child: &DeploymentRecord, stream: Option<&StreamConfig>) -> Result<()> {
    keys.destroy(&child.key_id()).await?;
    if let Some(stream) = stream {
        rtdb_logkeeper::retire(stream, child.epoch + 1).await?;
    }
    let bucket = storage.bucket(rtdb_storage::IoRegion::new()).await?;
    if !crate::reference::erase_deployment(bucket.as_ref(), &child.storage_prefix(), &[]).await? {
        return Err(Anomaly::fault("rtdb.multi/fork-cleanup", format!("Storage still contains staged objects for {}", child.id)));
    }
    Ok(())
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
        if !same_cell_move(record, target) || record.move_from.is_some() {
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
    if from.storage.overlaps(to.storage)? || source_media.keys == destination_media.keys {
        return Err(Anomaly::incorrect("rtdb.multi/shared-cell-storage", "Cell moves require independent serving storage and key custody"));
    }
    if matches!(to.storage, CellStorage::S3 { .. }) && hosted_task.is_none() {
        return Err(Anomaly::incorrect("rtdb.multi/transfer-task", "A remote transfer needs a recoverable task identity"));
    }

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
                    return Ok((current, previous));
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
    let source_stream = administration_stream(from.directory.as_ref(), &current, maximum_message_bytes).await?;
    let target_stream = administration_stream(to.directory.as_ref(), &arrival, maximum_message_bytes).await?;
    if target_stream.servers.iter().any(|server| source_stream.servers.contains(server) || current.replica_journals.contains(server)) {
        return Err(Anomaly::incorrect("rtdb.multi/shared-cell-journal", "Cell moves require independent replica journals"));
    }

    let source = update_installation(from.directory.as_ref(), &current.id, |record| {
        if record.lifecycle == Lifecycle::Moving && intent_matches(record, intent) && same_media(record) {
            return Ok(());
        }
        if record.lifecycle != Lifecycle::Live || record.version != current.version || record.move_from.is_some() {
            return Err(conflict());
        }
        record.lifecycle = Lifecycle::Moving;
        record.epoch = record.epoch.checked_add(1).ok_or_else(|| {
            Anomaly::fault("rtdb.multi/epoch-exhausted", format!("Installation {} has no remaining writer epochs", record.id))
        })?;
        record.holder = None;
        record.move_intent = None;
        record.replica_journals.extend(source_stream.servers.iter().cloned());
        record.cell_transfer = Some(CellTransfer {
            intent: intent.clone(),
            source_incarnation: record.incarnation,
            destination_incarnation: arrival.incarnation,
            epoch: record.epoch,
            source_media: source_media.clone(),
            destination_media: destination_media.clone(),
        });
        Ok(())
    }).await?;
    let staged = if let Some(pending) = pending {
        pending
    } else {
        let mut staged = source.clone();
        staged.org = arrival.org;
        staged.incarnation = arrival.incarnation;
        staged.ensemble = arrival.ensemble;
        staged.replica_journals = target_stream.servers.iter().cloned().collect();
        staged.move_from = Some(source_stream.servers.clone());
        staged.storage_regions.clear();
        staged.replica_trim = None;
        staged.checkpoint_recovery = None;
        staged.checkpoint_captures.clear();
        staged.checkpoint_capture_retirements.clear();
        staged.archive_retirement = None;
        staged.retired_residences.clear();
        staged.activation_crashes = 0;
        if let Some(prior) = &prior_residence {
            staged.retired_residences.clone_from(&prior.retired_residences);
            staged.retired_residences.insert(prior.incarnation, RetiredResidence {
                transfer: prior.cell_transfer.clone().ok_or_else(&conflict)?,
                serving: prior.disposal.clone().ok_or_else(&conflict)?,
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
            StorageRegion::Local { archive: None, tracks_ordered_index_pages: true, checkpoint_metadata_from: None }
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
            archive.materialize(staged, target_bucket).await?;
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
pub async fn delete_deployment(directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, id: &str, maximum_message_bytes: usize) -> Result<DisposalReceipt> {
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
    if fenced.lifecycle == Lifecycle::Deleting && !fenced.storage_regions.is_empty() {
        crate::store::reconcile_storage_regions(directory, storage, keys, &fenced).await?;
        fenced = directory.deployment(id).await?.ok_or_else(|| {
            Anomaly::incorrect("rtdb.multi/unknown-installation", format!("No installation has ID {id}"))
        })?;
    }
    if fenced.incarnation != expected {
        return earlier(&fenced);
    }
    match (fenced.lifecycle, fenced.disposal.as_ref()) {
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
    let prefix = crate::reference::key(&fenced.storage_prefix())?;
    let others: Vec<DeploymentRecord> = directory.deployments().await?.into_iter()
        .filter(|record| record.id != fenced.id).collect();
    crate::reference::require_disjoint_prefix(&prefix, others.iter())?;
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
    stream.servers.extend(fenced.replica_journals.iter().cloned());
    stream.servers.sort();
    stream.servers.dedup();
    if !stream.servers.is_empty() {
        rtdb_logkeeper::retire(&stream, fenced.epoch).await?;
    }
    let journals_retired_at_ms = now_ms();
    keys.destroy(&fenced.key_id()).await?;
    let key_withdrawn_at_ms = now_ms().max(journals_retired_at_ms);
    let bucket = storage.bucket(rtdb_storage::IoRegion::new()).await?;
    // Its forks keep what they share with it: those objects are copied into them before it goes.
    if !crate::reference::erase_deployment(bucket.as_ref(), prefix.as_str(), &others).await? {
        return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/disposal-prefix", format!("Installation {id} still has serving objects")));
    }
    let serving_inventory = storage.purge_serving_inventory(&prefix).await?;
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
        if record.epoch != receipt.epoch || record.incarnation != receipt.incarnation {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/disposal-fence", format!("Installation {id} changed during disposal")));
        }
        match record.lifecycle {
            Lifecycle::Deleted if record.disposal.is_some() => Ok(()),
            Lifecycle::Deleting => {
                record.disposal = Some(receipt.clone());
                record.lifecycle = Lifecycle::Deleted;
                Ok(())
            }
            Lifecycle::Deleted if record.moved_at_ms.is_some() => {
                record.disposal = Some(receipt.clone());
                Ok(())
            }
            _ => Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/disposal-fence", format!("Installation {id} left disposal"))),
        }
    }).await?;
    deleted.disposal.ok_or_else(|| Anomaly::fault("rtdb.multi/disposal-receipt", format!("Installation {id} lacks a disposal receipt")))
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

/// What auditing a deployment's history found (`NoTwoWritersHaveEffect`, `SystemTimeRises`).
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct AuditReport { pub records: usize, pub transactions: usize, pub highest_term: i64, pub violations: Vec<String> }

/// Reads a deployment's whole replica stream through its logkeepers, decrypts it, and checks that it is one history: dense positions, terms that never go back, transaction ids, system times and source positions that strictly rise.
pub async fn audit(directory: &dyn Directory, keys: &dyn KeyService, name: &str, maximum_message_bytes: usize) -> Result<AuditReport> {
    use rtdb_format::envelope::{ReplicaBody, ReplicaEnvelope};
    use rtdb_storage::log::Log;
    let current = record(directory, name).await?;
    let members = members(directory).await?;
    let servers: Vec<String> = current.ensemble.iter().filter_map(|id| members.get(id).map(|member| member.address.clone())).collect();
    let stream = StreamConfig { write_quorum: write_quorum(servers.len()), servers, stream: current.stream("replica"), maximum_message_bytes };
    let key = keys.require_existing(&current.key_id()).await?;
    let reader = rtdb_logkeeper::open_reader(stream, CancellationToken::new()).await?;
    let log = crate::logs::SealedLog::new(reader, &key, &current.replica_seal_scope())?;
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

