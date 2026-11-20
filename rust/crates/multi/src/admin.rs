//! Cell administration: creating, moving, rebalancing, forking and deleting deployments, and confirming dead members (`CreateDeployment`, `IntendMove`, `Rebalance`, `Fork`, `BeginDeletion`, `EraseAndProve`, `ExpireLease`).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rtdb_directory::{Cas, DeploymentRecord, Directory, Lifecycle, MemberId, MemberRecord, MemberRole, MemberState, effective_holder, now_ms};
use rtdb_logkeeper::StreamConfig;
use rtdb_storage::log::LogOffset;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use tokio_util::sync::CancellationToken;

use crate::control::{self, Request, Response};
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

async fn choose_ensemble(directory: &dyn Directory, record: &DeploymentRecord, replicas: usize) -> Result<Vec<MemberId>> {
    let now = now_ms();
    let logkeepers: Vec<MemberRecord> = directory.members().await?.into_iter().filter(|member| member.role == MemberRole::Logkeeper && member.is_live(now)).collect();
    if logkeepers.len() < replicas {
        return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/too-few-logkeepers", format!("{replicas} logkeepers are needed and {} are live", logkeepers.len())));
    }
    let ranked = rtdb_affinity::rank(&record.storage_identity(), &logkeepers, |member| member.id.as_str());
    Ok(ranked.into_iter().take(replicas).map(|member| member.id.clone()).collect())
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

async fn expect(address: &str, request: Request) -> Result<Response> {
    match control::call(address, &request, CONTROL_TIMEOUT).await? {
        Response::Refused { code, message } => Err(Anomaly::new(AnomalyCategory::Unavailable, code, message)),
        response => Ok(response),
    }
}

/// Makes sure the deployment is held, activating it on the first node of its affinity order if not, and returns its holder.
pub async fn ensure_held(directory: &dyn Directory, name: &str) -> Result<MemberId> {
    let current = record(directory, name).await?;
    let members = members(directory).await?;
    if let Some(holder) = effective_holder(&current, &members, now_ms()) { return Ok(holder.clone()); }
    let nodes: Vec<&MemberRecord> = members.values().filter(|member| member.role == MemberRole::Node && member.may_receive_placements(now_ms())).collect();
    let mut last = None;
    for node in rtdb_affinity::rank(&current.storage_identity(), &nodes, |member| member.id.as_str()) {
        let Some(address) = &node.control_address else { continue };
        match expect(address, Request::Activate { name: name.to_owned() }).await {
            Ok(Response::Held { .. }) => return Ok(node.id.clone()),
            Ok(Response::Redirect { holder, .. }) => return Ok(MemberId(holder)),
            Ok(_) => {}
            Err(error) => last = Some(error),
        }
    }
    Err(last.unwrap_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/no-node", "No node can hold this deployment")))
}

/// A planned move: reserves the next claim for `target`, has the holder park, and has `target` claim.
pub async fn move_deployment(directory: &dyn Directory, name: &str, target: &MemberId) -> Result<()> {
    let members = members(directory).await?;
    if !members.get(target).is_some_and(|member| member.may_receive_placements(now_ms())) {
        return Err(Anomaly::incorrect("rtdb.multi/not-placeable", format!("{target} may not take placements")));
    }
    let intended = update(directory, name, |record| { record.move_intent = Some(target.clone()); Ok(()) }).await?;
    if let Some(holder) = effective_holder(&intended, &members, now_ms()).filter(|holder| *holder != target) {
        expect(&control_address(&members, holder)?, Request::Park { name: name.to_owned() }).await?;
    }
    match expect(&control_address(&members, target)?, Request::Activate { name: name.to_owned() }).await? {
        Response::Held { .. } => Ok(()),
        other => Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/move-failed", format!("The target answered {other:?}"))),
    }
}

/// Moves at most `moves` deployments from the most loaded nodes to the least loaded, where their holding counts differ by more than one.
pub async fn rebalance(directory: &dyn Directory, moves: usize) -> Result<usize> {
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
        move_deployment(directory, &name, &idlest).await?;
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

/// Forks `parent` into a new deployment `name` of the same lineage: the parent is flushed so its whole state is in blocks, its objects are copied under the child's prefix, and the child starts on fresh logs in the next log epoch.
pub async fn fork(directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, parent: &str, name: &str) -> Result<DeploymentRecord> {
    let holder = ensure_held(directory, parent).await?;
    let members = members(directory).await?;
    expect(&control_address(&members, &holder)?, Request::Flush { name: parent.to_owned() }).await?;
    let source = record(directory, parent).await?;
    let mut child = DeploymentRecord::new(name, source.org.clone(), Vec::new(), now_ms());
    child.lineage = source.lineage.clone();
    child.parent = Some(source.id.clone());
    child.log_epoch = source.log_epoch + 1;
    child.ensemble = choose_ensemble(directory, &child, source.ensemble.len()).await?;
    match storage {
        CellStorage::Local { root } => crate::reference::fork(root, &source.storage_prefix(), &child.storage_prefix())?,
    }
    keys.copy(&source.id, &child.id)?;
    directory.create_deployment(child).await
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

/// Moves a deployment to another cell (`BeginCellMove`, `MoveCutover`): the source fences it and seals its stream, its objects and key are copied, the target records it with the source's logkeepers to move from, the cell map flips, and the source keeps a tombstone that its gateway forwards.
/// The target's first holder recovers the unflushed tail from the source's logkeepers and moves it onto its own.
pub async fn move_cell(map: &dyn crate::cells::CellMap, from: &CellHandle<'_>, to: &CellHandle<'_>, name: &str, maximum_message_bytes: usize) -> Result<DeploymentRecord> {
    let fenced = update(from.directory, name, |record| {
        if record.lifecycle != Lifecycle::Live { return Err(Anomaly::incorrect("rtdb.multi/not-live", format!("{name} is not live"))); }
        record.lifecycle = Lifecycle::Moving;
        record.epoch += 1;
        record.holder = None;
        Ok(())
    }).await?;
    let members = members(from.directory).await?;
    let servers: Vec<String> = fenced.ensemble.iter().filter_map(|id| members.get(id).map(|member| member.address.clone())).collect();
    let stream = StreamConfig { write_quorum: write_quorum(servers.len()), servers: servers.clone(), stream: fenced.stream("replica"), maximum_message_bytes };
    rtdb_logkeeper::seal(&stream, fenced.epoch).await?;
    match (from.storage, to.storage) {
        (CellStorage::Local { root: source }, CellStorage::Local { root: target }) => crate::reference::materialize(source, &fenced.storage_prefix(), target, &fenced.storage_prefix())?,
    }
    to.keys.install(&fenced.id, from.keys.key(&fenced.id)?)?;
    let mut arrived = fenced.clone();
    arrived.lifecycle = Lifecycle::Live;
    arrived.move_intent = None;
    arrived.activation_crashes = 0;
    arrived.move_from = Some(servers);
    arrived.ensemble = choose_ensemble(to.directory, &arrived, fenced.ensemble.len()).await?;
    let arrived = to.directory.create_deployment(arrived).await?;
    if !map.assign(name, to.name, Some(from.name)).await? { return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/move-raced", format!("{name} was not in {} when the move completed", from.name))); }
    update(from.directory, name, |record| { record.lifecycle = Lifecycle::Deleted; Ok(()) }).await?;
    Ok(arrived)
}

/// What a deletion did, kept as its record.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DeletionRecord { pub deployment: String, pub name: String, pub key_destroyed_at_ms: i64, pub objects_erased_at_ms: i64, pub verified_empty: bool }

/// Fences every writer, seals and trims the deployment's stream, destroys its key, erases its objects and proves its prefix empty.
pub async fn delete_deployment(directory: &dyn Directory, storage: &CellStorage, keys: &dyn KeyService, name: &str, maximum_message_bytes: usize) -> Result<DeletionRecord> {
    let fenced = update(directory, name, |record| {
        if matches!(record.lifecycle, Lifecycle::Deleted) { return Err(Anomaly::incorrect("rtdb.multi/deleted", format!("{name} is already deleted"))); }
        record.lifecycle = Lifecycle::Deleting;
        record.epoch += 1;
        record.holder = None;
        Ok(())
    }).await?;
    let members = members(directory).await?;
    let servers: Vec<String> = fenced.ensemble.iter().filter_map(|id| members.get(id).map(|member| member.address.clone())).collect();
    let stream = StreamConfig { write_quorum: write_quorum(servers.len()), servers, stream: fenced.stream("replica"), maximum_message_bytes };
    let last = rtdb_logkeeper::seal(&stream, fenced.epoch).await?;
    if last.0 >= 0 { rtdb_logkeeper::trim(&stream, LogOffset(last.0)).await?; }
    keys.destroy(&fenced.id)?;
    let key_destroyed_at_ms = now_ms();
    let verified_empty = match storage {
        CellStorage::Local { root } => {
            let prefix = root.join(fenced.storage_prefix());
            // Its forks keep what they share with it: those objects are copied into them before it goes.
            let others: Vec<DeploymentRecord> = directory.deployments().await?.into_iter().filter(|record| record.id != fenced.id).collect();
            let references = crate::reference::References::scan(root, &others)?;
            for store in crate::reference::stores(&prefix) { references.erase_store(&store)?; }
            if prefix.exists() { std::fs::remove_dir_all(&prefix).map_err(io)?; }
            !prefix.exists()
        }
    };
    let deletion = DeletionRecord { deployment: fenced.id.clone(), name: name.to_owned(), key_destroyed_at_ms, objects_erased_at_ms: now_ms(), verified_empty };
    update(directory, name, |record| { record.lifecycle = Lifecycle::Deleted; Ok(()) }).await?;
    Ok(deletion)
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
}

impl Default for ControllerOptions {
    fn default() -> Self { Self { interval: Duration::from_secs(1), grace: Duration::from_secs(5), rebalance_moves: 0, suspicion_probes: 2, probe_timeout: Duration::from_millis(300) } }
}

/// Probes every live node's agent, and confirms dead those that failed `suspicion_probes` probes in a row (`ConfirmSuspicion`).
/// Safety never depends on the verdict: a wrongly confirmed node's writes are already fenced by the epoch its successor claims.
pub async fn confirm_suspicions(directory: &dyn Directory, failures: &mut BTreeMap<MemberId, u32>, options: &ControllerOptions) -> Result<usize> {
    let now = now_ms();
    let mut confirmed = 0;
    for member in directory.members().await? {
        if member.role != MemberRole::Node || !member.is_live(now) { failures.remove(&member.id); continue; }
        let Some(address) = &member.control_address else { continue };
        let answered = matches!(control::call(address, &Request::Ping, options.probe_timeout).await, Ok(Response::Done));
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
                if options.rebalance_moves > 0 { let _ = rebalance(directory.as_ref(), options.rebalance_moves).await; }
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

fn io(error: std::io::Error) -> Anomaly { Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/storage-io", error.to_string()) }
