//! Cell administration: creating, moving, rebalancing, forking and deleting deployments, and confirming dead members (`CreateDeployment`, `IntendMove`, `Rebalance`, `Fork`, `BeginDeletion`, `EraseAndProve`, `ExpireLease`).

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;

pub use rtdb_directory::DeletionRecord;
use rtdb_directory::{Cas, CellMedia, CellResidence, CellTransfer, CellTransferIntent, PreparedCellTransfer, RetiredResidence, DeletionFacts, DeploymentRecord, DisposalReceipt, DeletionObservation, DeletionInventory, ServingInventory, Directory, ForkPreparation, ForkPublication, Lifecycle, LineageChange, MemberId, MemberPool, MemberRecord, MemberRole, MemberState, PlacementMove, StorageNamespaces, effective_holder, now_ms};
use rtdb_format::{catalog::BlockCatalog, keys::block_key};
use rtdb_logkeeper::StreamConfig;
use rtdb_storage::{IoRegion, ObjectStore, log::Log};
use rtdb_types::{Anomaly, AnomalyCategory, BlockIndex, JournalEndpoint, Result, StorageEpoch};
use tokio_util::sync::CancellationToken;

use crate::control::{self, ControlKey, MoveTicket, Request, Response};
use crate::deployment::{CellStorage, ensemble_servers, journal_endpoint, live_holdings, require_storage_namespaces, s3_write_lease_horizon, select_logkeepers, storage_namespaces, write_quorum};
use crate::keys::KeyService;

mod cell_attempts;
pub use cell_attempts::dispose_abandoned;
pub(crate) use cell_attempts::{dispose_generations, has_pending_disposal};

mod holder;
pub use holder::{HolderObservation, observe_holder};
pub(crate) use holder::observe_holder_record;
mod holder_handoff;
mod journals;
pub use journals::place_journals;

const CONTROL_TIMEOUT: Duration = Duration::from_secs(35);
const CONCURRENT_SUSPICION_PROBES: usize = 16;

/// Binds an object namespace for a cell before nodes using scoped writes start.
pub async fn initialize_storage(storage: &CellStorage, keys: &dyn KeyService) -> Result<StorageNamespaces> {
    let bucket = storage.bucket(rtdb_storage::IoRegion::new()).await?;
    storage_namespaces(bucket.as_ref(), keys).await
}

pub async fn enroll_terminal_storage(storage: &CellStorage, evidence: bytes::Bytes) -> Result<uuid::Uuid> {
    let CellStorage::S3 { config } = storage else {
        return Err(Anomaly::incorrect("rtdb.multi/terminal-storage", "Terminal enrollment requires an S3 cell"));
    };
    config.enroll_terminal_cell(&BTreeMap::new(), evidence).await
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

pub(crate) async fn choose_ensemble(directory: &dyn Directory, record: &DeploymentRecord, replicas: usize) -> Result<Vec<MemberId>> {
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

pub(crate) async fn replica_servers(directory: &dyn Directory, record: &DeploymentRecord) -> Result<Vec<JournalEndpoint>> {
    match &record.move_from {
        Some(previous) if previous.iter().all(|endpoint| endpoint.journal().is_some()) => Ok(previous.clone()),
        Some(_) => Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/journal-identity-missing",
            "Resume the cell transfer from its source directory to bind the source journal identities")),
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
    let now = now_ms();
    let holder = effective_holder(&current, &members, now);
    if let Some(holder) = holder && !current.is_resident_idle() { return Ok(holder.clone()); }
    let resident = holder.and_then(|id| members.get(id));

    let nodes: Vec<&MemberRecord> = members.values().filter(|member| current.may_place_on(member, now)).collect();
    let mut last = None;
    let ranked = rtdb_affinity::rank(&current.storage_identity(), &nodes, |member| member.id.as_str());
    for node in resident.into_iter().chain(ranked.into_iter().copied().filter(|node| resident.is_none_or(|resident| node.id != resident.id))) {
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
    let Response::Flushed { checkpoint } = expect(
        &address, Request::Flush { name: original.name.clone() }, control,
    ).await? else { return Err(unavailable()); };
    let deadline = tokio::time::Instant::now() + CONTROL_TIMEOUT;
    loop {
        let after = record(directory, &original.name).await?;
        if !matches_original(&after) || after.epoch != before.epoch || tokio::time::Instant::now() > deadline {
            return Err(unavailable());
        }
        if after.checkpoint == Some(checkpoint) {
            return Ok(after);
        }
        if after.checkpoint.is_some_and(|cut| cut.block >= checkpoint.block) {
            return Err(unavailable());
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

pub(crate) async fn pin_checkpoint_suffix(
    directory: &dyn Directory, installation: &DeploymentRecord, control: &ControlKey,
    capture: uuid::Uuid, lease: uuid::Uuid, basis: rtdb_types::TransactionKey, expires_at_ms: i64,
) -> Result<()> {
    let unavailable = || Anomaly::new(
        AnomalyCategory::Unavailable, "rtdb.multi/checkpoint-suffix-unavailable",
        "The installation did not pin the requested checkpoint suffix",
    );
    let holder = installation.holder.as_ref().ok_or_else(unavailable)?;
    let address = control_address(&members(directory).await?, holder)?;
    let response = expect(&address, Request::PinCheckpointSuffix {
        name: installation.name.clone(), deployment: installation.id.parse().map_err(|_| unavailable())?,
        capture, lease, basis, expires_at_ms,
    }, control).await?;
    match response {
        Response::CheckpointSuffixPinned { pin } if pin.capture == capture && pin.basis == basis
            && pin.expires_at_ms == expires_at_ms => Ok(()),
        _ => Err(unavailable()),
    }
}

pub(crate) async fn fence_classic_migration(
    directory: &dyn Directory, stage: &rtdb_directory::CheckpointStage, control: &ControlKey,
) -> Result<rtdb_directory::CheckpointTransferFence> {
    let id = stage.intent.source.to_string();
    let conflict = || Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/migration-source",
        "The Classic writer or migration attempt changed");
    let selected = |record: &DeploymentRecord| -> Result<Option<rtdb_directory::CheckpointTransferFence>> {
        record.checkpoint_transfer_fence.as_deref().map(|fence| {
            if fence.destination == *stage && fence.drained_source_through.is_some() {
                Ok(fence.clone())
            } else {
                Err(conflict())
            }
        }).transpose()
    };
    let source = directory.deployment(&id).await?.ok_or_else(conflict)?;
    if let Some(fence) = selected(&source)? { return Ok(fence); }
    let expected = stage.intent.classic_migration.ok_or_else(conflict)?.source;
    if source.epoch != expected.writer_epoch || source.incarnation != expected.incarnation
        || source.log_epoch != expected.log_epoch
    {
        return Err(conflict());
    }
    let holder = source.holder.as_ref().ok_or_else(conflict)?;
    let address = control_address(&members(directory).await?, holder)?;
    let response = expect(&address, Request::FenceClassicMigration {
        name: source.name, stage: Box::new(stage.clone()),
    }, control).await;
    let current = directory.deployment(&id).await?.ok_or_else(conflict)?;
    if let Some(fence) = selected(&current)? { return Ok(fence); }
    response?;
    Err(conflict())
}

/// Reserves the next claim for `target` during the handoff.
pub async fn move_deployment(directory: &dyn Directory, name: &str, target: &MemberId, control: &ControlKey) -> Result<()> {
    move_from_holding(directory, name, target, None, control).await
}

pub(crate) async fn move_from_holding(directory: &dyn Directory, name: &str, target: &MemberId,
                                      source: Option<(&MemberId, u64)>, control: &ControlKey) -> Result<()> {
    move_placement(directory, name, target, source, None, control).await.map(|_| ())
}

/// Move only the installation named by an actual holder observation.
/// The source identity and epoch are checked inside the reservation CAS.
pub async fn move_observed_holder(
    directory: &dyn Directory,
    original: &HolderObservation,
    target: &MemberId,
    control: &ControlKey,
) -> Result<HolderObservation> {
    let changed = || Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/move-source-changed",
        "The commissioned writer or its database placement changed");
    if original.authority.is_nil() || original.incarnation == 0 || original.epoch == 0
        || original.directory_version == 0 || original.member_version == 0
        || original.observed_at_ms <= 0 || original.lease_expires_at_ms <= original.observed_at_ms
        || &original.holder == target
    {
        return Err(changed());
    }
    let name = directory.deployment(&original.authority.to_string()).await?
        .ok_or_else(changed)?.name;
    let settled_epoch = move_placement(
        directory, &name, target, Some((&original.holder, original.epoch)), Some(original), control,
    ).await?;
    let successor = observe_holder(directory, original.authority, original.incarnation).await?;
    if &successor.holder != target || successor.log_epoch != original.log_epoch
        || !holder_handoff::exact_holder_handoff(original.epoch, settled_epoch, successor.epoch)
        || successor.directory_version <= original.directory_version
        || successor.engine_version != original.engine_version
    {
        return Err(changed());
    }
    Ok(successor)
}

/// Fetch an exact writer handoff witness from its authenticated successor node.
pub async fn audience_writer_handoff(
    directory: &dyn Directory,
    plan: rtdb_directory::WriterHandoffPlan,
    action: control::WriterHandoffAction,
    control: &ControlKey,
) -> Result<Vec<u8>> {
    let unavailable = || Anomaly::new(
        AnomalyCategory::Unavailable, "rtdb.multi/writer-handoff", "The exact writer handoff witness is unavailable",
    );
    let secret = control.0.as_ref().ok_or_else(unavailable)?;
    let record = directory.deployment(&plan.identity.source().to_string()).await?
        .filter(|record| plan.matches(record)).ok_or_else(unavailable)?;
    let target = directory.member(&plan.successor).await?
        .filter(|member| record.may_hold_on(member, now_ms())).ok_or_else(unavailable)?;
    let address = target.control_address.as_deref().ok_or_else(unavailable)?;
    let command = control::WriterHandoffCommand::issue(secret, &record.name, plan, action);
    let response = expect(address, Request::WriterHandoff { name: record.name.clone(), command: command.clone() }, control).await?;
    let Response::WriterHandoff { witness, proof } = response else { return Err(unavailable()); };
    if !secret.verify_route(&command.binding(&record.name, Some(&witness)), &proof) {
        return Err(unavailable());
    }
    let expected = match action {
        control::WriterHandoffAction::Prepare => rtdb_directory::WRITER_HANDOFF_BYTES + 16,
        control::WriterHandoffAction::Read | control::WriterHandoffAction::Switch => rtdb_directory::WRITER_HANDOFF_BYTES + 32,
        control::WriterHandoffAction::Abandon => 0,
    };
    if witness.len() != expected
        || (action == control::WriterHandoffAction::Prepare && !witness.starts_with(&command.plan.identity.encode()))
    {
        return Err(unavailable());
    }
    if matches!(action, control::WriterHandoffAction::Read | control::WriterHandoffAction::Switch) {
        let width = rtdb_directory::WRITER_HANDOFF_BYTES;
        let coordinate = |offset| u64::from_be_bytes(witness[offset..offset + 8].try_into().expect("checked writer witness length"));
        let digest_halves = |data: &[u8]| {
            let digest = ring::digest::digest(&ring::digest::SHA256, data);
            let bytes = digest.as_ref();
            (u128::from_be_bytes(bytes[..16].try_into().expect("SHA-256 half")),
             u128::from_be_bytes(bytes[16..].try_into().expect("SHA-256 half")))
        };
        let (expected_lo, expected_hi) = digest_halves(&command.plan.identity.encode());
        let (observed_lo, observed_hi) = digest_halves(&witness[..width]);
        if !crate::journal::adapter_command::exact_writer_settlement(
            expected_lo, expected_hi, observed_lo, observed_hi,
            coordinate(width), coordinate(width + 16), coordinate(width + 24),
        ) {
            return Err(unavailable());
        }
    }
    Ok(witness)
}

async fn move_placement(directory: &dyn Directory, name: &str, target: &MemberId,
                        source: Option<(&MemberId, u64)>, observed: Option<&HolderObservation>,
                        control: &ControlKey) -> Result<u64> {
    let members = if let Some(observed) = observed {
        let mut selected = BTreeMap::new();
        for id in [&observed.holder, target] {
            if let Some(record) = directory.member(id).await? {
                selected.insert(record.id.clone(), record);
            }
        }
        selected
    } else {
        members(directory).await?
    };
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
        if observed.is_some_and(|observed| {
            record.id != observed.authority.to_string()
                || record.incarnation != observed.incarnation
                || record.log_epoch != observed.log_epoch
                || record.version < observed.directory_version
                || [&observed.holder, target].into_iter().any(|member| {
                    members.get(member).is_none_or(|member| member.engine_version != observed.engine_version)
                })
        }) {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/move-source-changed",
                "The observed installation or commissioned engine no longer matches"));
        }
        if !members.get(target).is_some_and(|member| record.may_place_on(member, now)) {
            return Err(Anomaly::incorrect("rtdb.multi/not-placeable", format!("{target} may not take placements of {name}")));
        }
        if holder == Some(target) { return Ok(()); }
        if record.active_move(now).is_some_and(|intent| observed.is_some()
            || members.get(&intent.target).is_some_and(|member| record.may_place_on(member, now)))
        {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/move-in-progress", format!("{name} already has a reserved placement")));
        }
        record.move_intent = None;
        record.placement_move = Some(reservation.clone());
        Ok(())
    }).await?;
    if effective_holder(&intended, &members, now_ms()) == Some(target) { return Ok(intended.epoch); }
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
            Response::Held { epoch, .. } => Ok(epoch),
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
        for record in deployments.iter().filter(|record| record.writer_pool() == Some(pool) && !record.is_resident_idle()) {
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
        if record.placed_receipt_retirement && (version.is_none() || record.lifecycle != Lifecycle::Live || record.holder.is_some()) {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/placed-receipt-cutover",
                "Keep a compatible engine pinned for a history using placed receipt retirement"));
        }
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

/// Starts emitting the versioned outcome format at the next activation.
pub async fn enable_placed_receipt_retirement(directory: &dyn Directory, name: &str) -> Result<DeploymentRecord> {
    update(directory, name, |record| {
        if record.placed_receipt_retirement { return Ok(()); }
        if record.lifecycle != Lifecycle::Live || record.holder.is_some()
            || record.move_intent.is_some() || record.active_move(now_ms()).is_some() || record.engine_pin.is_none()
        {
            return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/placed-receipt-cutover",
                "Park the deployment, finish its move, and pin a compatible engine before enabling placed receipt retirement"));
        }
        record.placed_receipt_retirement = true;
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
    if !source.can_fork() || source.serving_replay_capture.is_some() {
        return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/fork-unavailable", format!("{parent} cannot be forked in its current state")));
    }
    Ok(source)
}

struct ForkAccess {
    bucket: crate::reference::Bucket,
    io: Arc<rtdb_storage::IoRegion>,
    preparation: ForkPublication,
}

async fn with_fork_access<F: std::future::Future<Output = Result<DeploymentRecord>>>(
    directory: &dyn Directory,
    storage: &CellStorage,
    keys: &dyn KeyService,
    source: &DeploymentRecord,
    task: Option<&rtdb_directory::HostedTask>,
    operation: impl FnOnce(ForkAccess) -> F,
) -> Result<DeploymentRecord> {
    let maintenance = storage.try_local_maintenance()?.map(Arc::new);
    let lineage = &source.lineage;
    let generation = directory.lineage_state(lineage).await?.retirement_generation();
    let preparation = ForkPreparation { token: uuid::Uuid::new_v4(), generation };
    let (change, publication) = if maintenance.is_some() {
        let local_volume = storage.custody_namespace()?;
        (LineageChange::PrepareFork { preparation, local_volume }, ForkPublication::Local(preparation))
    } else {
        (LineageChange::PrepareHostedFork { preparation, task: task.cloned() }, ForkPublication::Hosted(preparation))
    };
    directory.change_lineage(lineage, change).await?;
    let io = rtdb_storage::IoRegion::new().child_holding(maintenance.clone().map(|lock| lock as Arc<dyn Send + Sync>));
    let result = async {
        let admitted = directory.deployment(&source.id).await?.is_some_and(|current| {
            current.can_fork() && current.serving_replay_capture.is_none()
                && current.epoch == source.epoch && current.storage_prefix() == source.storage_prefix()
        });
        if !admitted { return Err(fork_unavailable(&source.name)); }
        let bucket = storage.bucket(Arc::clone(&io)).await?;
        require_storage_namespaces(source, storage_namespaces(bucket.as_ref(), keys).await?)?;
        operation(ForkAccess { bucket, io: Arc::clone(&io), preparation: publication }).await
    }.await;
    io.close_and_wait().await;
    if result.is_err() && let Err(error) = directory.change_lineage(lineage, LineageChange::ReleaseFork { preparation }).await
    {
        eprintln!("RTDB fork preparation {} is retained: {}", preparation.token, error.code);
    }
    result
}

#[derive(Clone, Copy)]
pub struct ForkDestination<'a> {
    pub name: &'a str,
    pub org: Option<&'a str>,
    pub hosted_task: Option<&'a rtdb_directory::HostedTask>,
    pub broker: Option<&'a crate::replay_broker::Client>,
}

/// Forks `parent` into a new deployment in its lineage.
/// The parent is flushed, and the child references its objects and starts on fresh logs.
pub async fn fork(directory: &Arc<dyn Directory>, storage: &CellStorage, keys: &dyn KeyService, parent: &str, name: &str, broker: Option<&crate::replay_broker::Client>, control: &ControlKey) -> Result<DeploymentRecord> {
    fork_into(directory, storage, keys, parent, ForkDestination { name, org: None, hosted_task: None, broker }, control).await
}

pub async fn fork_into(directory: &Arc<dyn Directory>, storage: &CellStorage, keys: &dyn KeyService, parent: &str, destination: ForkDestination<'_>, control: &ControlKey) -> Result<DeploymentRecord> {
    let original = fork_source(directory.as_ref(), parent).await?;
    if let Some(org) = destination.org { validate_org(org)?; }
    let source = flush_checkpoint(directory.as_ref(), &original, control).await?;
    let source = &source;
    with_fork_access(directory.as_ref(), storage, keys, source, destination.hosted_task, |access|
        fork_with_tail(directory.clone(), storage, access, keys, source, destination, None, Vec::new(), 4 << 20)).await
}

/// Forks `parent` as of one of its past blocks: the fork's state is that block's, and what the parent wrote after it is not the fork's.
/// The fork is admitted only while the selected block's complete checkpoint inventory is still available.
pub async fn fork_at_block(directory: &Arc<dyn Directory>, storage: &CellStorage, keys: &dyn KeyService, parent: &str, name: &str, broker: Option<&crate::replay_broker::Client>, block: i64) -> Result<DeploymentRecord> {
    fork_block_into(directory, storage, keys, parent, ForkDestination { name, org: None, hosted_task: None, broker }, block).await
}

pub async fn fork_block_into(directory: &Arc<dyn Directory>, storage: &CellStorage, keys: &dyn KeyService, parent: &str, destination: ForkDestination<'_>, block: i64) -> Result<DeploymentRecord> {
    let source = fork_source(directory.as_ref(), parent).await?;
    let source = &source;
    with_fork_access(directory.as_ref(), storage, keys, source, destination.hosted_task, |access|
        fork_with_tail(directory.clone(), storage, access, keys, source, destination, Some(block), Vec::new(), 4 << 20)).await
}

/// Forks `parent` as of a system time at or after its checkpoint: the fork starts from the checkpoint's block, with the parent's transactions after it up to `system_time` copied into its own stream.
/// A time before the checkpoint is a past block's, which `fork_at_block` forks from.
pub async fn fork_at_time(directory: &Arc<dyn Directory>, storage: &CellStorage, keys: &dyn KeyService, parent: &str, name: &str, broker: Option<&crate::replay_broker::Client>, system_time: rtdb_types::Timestamp, maximum_message_bytes: usize) -> Result<DeploymentRecord> {
    fork_time_into(directory, storage, keys, parent, ForkDestination { name, org: None, hosted_task: None, broker }, broker, system_time, maximum_message_bytes).await
}

pub async fn fork_time_into(directory: &Arc<dyn Directory>, storage: &CellStorage, keys: &dyn KeyService, parent: &str, destination: ForkDestination<'_>, broker: Option<&crate::replay_broker::Client>, system_time: rtdb_types::Timestamp, maximum_message_bytes: usize) -> Result<DeploymentRecord> {
    let source = fork_source(directory.as_ref(), parent).await?;
    let checkpoint = source.checkpoint.ok_or_else(|| Anomaly::incorrect("rtdb.multi/no-checkpoint", format!("{parent} has published no block to fork from")))?;
    if system_time.0 < checkpoint.system_time { return Err(Anomaly::incorrect("rtdb.multi/before-checkpoint", format!("{parent}'s last published block is after that time: fork at a past block instead"))); }
    let source = &source;
    with_fork_access(directory.as_ref(), storage, keys, source, destination.hosted_task, |access| async move {
        let tail = crate::archive::stage::with_region(
            directory.as_ref(), keys, &source.id, storage, destination.hosted_task, None, None,
            |current| {
                if current.incarnation != source.incarnation || current.log_epoch != source.log_epoch
                    || !current.can_fork() || current.serving_replay_capture.is_some()
                {
                    return Err(fork_unavailable(parent));
                }
                Ok(Some(crate::hosted_recovery::replay_start(current, checkpoint.boundary)?))
            },
            |current, region, io| async move {
                let bucket = storage.bucket(Arc::clone(&io)).await?;
                let scope = format!("{}/{}", source.storage_prefix(), crate::archive::checkpoint_storage_root()?);
                let store = crate::store::open_deployment_store(bucket, &scope, crate::packing::PackDiscovery::From(checkpoint.block), crate::store::wall_clock()).await?;
                let publications = crate::store::directory_publications(keys, &current, Arc::clone(&io))?;
                let store = crate::store::EncryptingStore::published(store, publications);
                let root = crate::reference::key(&block_key(BlockIndex(checkpoint.block))?)?;
                let catalog = BlockCatalog::decode(&store.get_fresh(&root).await?)?;
                crate::archive::verify_cut(&catalog, checkpoint, StorageEpoch(source.log_epoch))?;
                let stream = administration_stream(directory.as_ref(), &current, maximum_message_bytes.max(4 << 20)).await?;
                tokio::time::timeout(CONTROL_TIMEOUT, async {
                    let reader = rtdb_logkeeper::open_reader(stream, CancellationToken::new()).await?;
                    let through = reader.read_index(current.epoch).await?.through();
                    let reader = crate::logs::SealedLog::new(reader, keys, &current.key_id(), &current.replica_seal_scope(), io).await?;
                    let history = crate::replay_broker::History::of(&current)?;
                    let memory = rtdb_columnar::MemoryBudget::new(rtdb_node::NodeOptions::default().execution_memory_bytes);
                    crate::logs::transactions_until(&reader, &catalog, checkpoint.boundary, through, system_time,
                        async |position, frame| {
                            let broker = broker.ok_or_else(|| fork_unavailable(parent))?;
                            broker.read(history, region, position, frame, &memory).await
                        },
                    ).await
                }).await.map_err(|_| fork_unavailable(parent))?
            },
        ).await?;
        fork_with_tail(directory.clone(), storage, access, keys, source, destination, Some(checkpoint.block), tail, maximum_message_bytes).await
    }).await
}

fn fork_unavailable(name: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/fork-unavailable", format!("{name} changed or cannot be forked in its current state"))
}

async fn fork_with_tail(directory: Arc<dyn Directory>, storage: &CellStorage, access: ForkAccess, keys: &dyn KeyService, source: &DeploymentRecord, destination: ForkDestination<'_>, at_block: Option<i64>, mut tail: Vec<rtdb_format::envelope::ReplicaEnvelope>, maximum_message_bytes: usize) -> Result<DeploymentRecord> {
    let ForkAccess { bucket, io, preparation, .. } = access;
    let org = destination.org.unwrap_or(&source.org);
    validate_org(org)?;
    let same_organisation = org == source.org;
    let mut child = DeploymentRecord::new(destination.name, org, Vec::new(), now_ms());
    if same_organisation { child.lineage.clone_from(&source.lineage); }
    child.parent = Some(source.id.clone());
    child.engine_pin = source.engine_pin.clone();
    child.log_epoch = source.log_epoch + 1;
    child.storage_namespaces = source.storage_namespaces;
    // The parent's checkpoint names positions in the parent's logs, not the fork's: the fork's first holder finds its block by listing, and publishes the fork's own.
    child.ensemble = choose_ensemble(directory.as_ref(), &child, source.ensemble.len()).await?;
    let published = source.checkpoint.ok_or_else(|| fork_unavailable(&source.name))?;
    let selected = at_block.unwrap_or(published.block);
    if selected > published.block { return Err(fork_unavailable(&source.name)); }
    if tail.iter().any(|envelope| matches!(&envelope.message,
        rtdb_format::envelope::ReplicaBody::ResolvedTx(tx)
            if tx.table_data.keys().any(|name| name.split_once('/').is_some_and(|(schema, _)| rtdb_types::TableRef::owned_schema(schema)))
    )) {
        return Err(fork_unavailable(&source.name));
    }
    let mut inherited = same_organisation.then(|| rtdb_replication::ForkEvents::new(
        rtdb_columnar::MemoryBudget::new(rtdb_node::NodeOptions::default().execution_memory_bytes)
    ));
    if let Some(events) = &mut inherited {
        for envelope in &tail {
            if let rtdb_format::envelope::ReplicaBody::ResolvedTx(transaction) = &envelope.message {
                events.add_replica(transaction)?;
            }
        }
    }
    let inherited_tail = if same_organisation {
        use ring::digest::{Context, SHA256};
        let mut digest = Context::new(&SHA256);
        digest.update(b"rtdb.multi/fork-original-resolved-suffix/v1\0");
        for envelope in &tail {
            let original = envelope.encode();
            digest.update(&(original.len() as u64).to_be_bytes());
            digest.update(&original);
        }
        Some((tail.len() as u64, digest.finish().as_ref().try_into().expect("SHA-256 width")))
    } else {
        None
    };
    let child_stream = if tail.is_empty() {
        None
    } else {
        // The first holder claims the next epoch and seals the fork's tail.
        child.epoch = 1;
        crate::logs::renumber_into(&mut tail, rtdb_types::StorageEpoch(child.log_epoch))?;
        let servers = replica_servers(directory.as_ref(), &child).await?;
        child.replica_journals.extend(servers.iter().cloned());
        Some(StreamConfig { write_quorum: write_quorum(servers.len()), servers, stream: child.stream("replica"), maximum_message_bytes })
    };
    let custody_broker = if same_organisation {
        Some(destination.broker.ok_or_else(||
            Anomaly::incorrect("rtdb.multi/fork-custody", "A shared fork needs the original custody broker"))?)
    } else {
        None
    };
    let fork_preparation = match preparation {
        ForkPublication::Local(preparation) | ForkPublication::Hosted(preparation) => preparation,
        ForkPublication::Remote { .. } => return Err(fork_unavailable(&source.name)),
    };
    let stage = directory.stage_fork(child.clone(), fork_preparation).await?;
    let hosted = matches!(preparation, ForkPublication::Hosted(_));
    let target_io = if hosted {
        let mut target_io = io.child();
        crate::store::track_hosted_fork_publications(&mut target_io, directory.clone(), child.id.clone(), fork_preparation.token);
        Some(target_io)
    } else {
        None
    };
    let prepared = async {
        if let Some(broker) = custody_broker {
            use crate::replay_broker::History;
            broker.enroll_fork(History::of(source)?, History::of(&child)?, fork_preparation).await?;
        }
        let bucket = match &target_io {
            Some(target_io) => storage.bucket(target_io.clone()).await?,
            None => bucket,
        };
        let checkpoint = if same_organisation {
            crate::archive::reference_fork_checkpoint(bucket, source, keys, &child, selected,
                Arc::clone(&io), inherited.as_mut()).await?
        } else {
            crate::archive::copy_fork_checkpoint(bucket, source, keys, &child, Some(selected), Arc::clone(&io)).await?
        };
        keys.key(&child.key_id()).await?;
        if let Some(stream) = &child_stream {
            let writer = rtdb_logkeeper::open_writer(stream.clone(), child.epoch, CancellationToken::new()).await?;
            let writer = crate::logs::SealedLog::new(writer, keys, &child.key_id(), &child.replica_seal_scope(), Arc::clone(&io)).await?;
            for envelope in tail {
                writer.enqueue(bytes::Bytes::from(envelope.encode()), rtdb_types::Timestamp(now_ms() * 1000)).await?.durable().await?;
            }
        }
        if let (Some(broker), Some((transactions, suffix_digest)), Some(events)) =
            (custody_broker, inherited_tail, inherited)
        {
            let events = events.encode(crate::replay_broker::MAX_BODY)?;
            let event_map = rtdb_format::catalog::publication_digest(&events);
            broker.record_fork_basis(crate::replay_broker::History::of(&child)?, fork_preparation,
                crate::replay_broker::domain::ForkBasis { checkpoint, suffix_digest, transactions, event_map }, events).await?;
        }
        Ok(checkpoint)
    }.await;
    io.close_and_wait().await;
    let prepared = if hosted {
        let released = crate::store::release_region(directory.as_ref(), &child.id, fork_preparation.token, false).await;
        prepared.and_then(|checkpoint| released.map(|()| checkpoint))
    } else {
        prepared
    };
    let published = match prepared {
        Ok(checkpoint) => publish_fork(directory.as_ref(), source, &child, preparation).await.map(|stored| {
            crate::residence_trace::record("fork-directory-published", || {
                let mut fields = crate::archive::fork_trace_scope(source, &stored, checkpoint);
                fields["revision"] = serde_json::json!(stored.version);
                fields
            });
            stored
        }),
        Err(error) => Err(ForkPublicationError::Rejected(error)),
    };
    match published {
        Ok(stored) => Ok(stored),
        Err(ForkPublicationError::Uncertain(error)) => Err(error),
        Err(ForkPublicationError::Rejected(error)) => {
            let cleanup = async {
                if let Some(current) = directory.deployment(&stage.id).await?
                    .filter(|current| current.lifecycle == Lifecycle::Restoring && current.fork_stage == stage.fork_stage)
                {
                    close_fork_stage(directory.as_ref(), current).await?;
                }
                Ok::<(), Anomaly>(())
            }.await;
            if let Err(cleanup) = cleanup {
                eprintln!("RTDB fork cleanup failed for {}: {cleanup}", child.id);
            }
            Err(error)
        }
    }
}

enum ForkPublicationError { Rejected(Anomaly), Uncertain(Anomaly) }

async fn publish_fork(directory: &dyn Directory, source: &DeploymentRecord, child: &DeploymentRecord, preparation: ForkPublication) -> std::result::Result<DeploymentRecord, ForkPublicationError> {
    for _ in 0..8 {
        let current = directory.deployment(&source.id).await.map_err(ForkPublicationError::Rejected)?
            .filter(|current| current.can_fork() && current.epoch == source.epoch
                && current.log_epoch == source.log_epoch && current.storage_prefix() == source.storage_prefix()
                && current.storage_namespaces == child.storage_namespaces && current.engine_pin == child.engine_pin)
            .ok_or_else(|| ForkPublicationError::Rejected(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/fork-unavailable", format!("{} changed while the fork was prepared", source.name))))?;
        match directory.publish_fork(child.clone(), current.version, Some(preparation)).await {
            Ok(stored) => return Ok(stored),
            Err(error) if error.code == "rtdb.directory/fork-parent-changed" => continue,
            Err(error) => {
                // Once the write is sent, a transport failure can hide successful publication.
                match directory.deployment(&child.id).await {
                    Ok(Some(stored)) if stored.parent == child.parent && stored.name == child.name && stored.fork_stage.is_none() => return Ok(stored),
                    Ok(_) if matches!(error.code.as_str(), "rtdb.directory/exists" | "rtdb.directory/lineage-changed" | "rtdb.directory/fork-stage-changed") => {
                        return Err(ForkPublicationError::Rejected(error));
                    }
                    _ => return Err(ForkPublicationError::Uncertain(error)),
                }
            }
        }
    }
    Err(ForkPublicationError::Rejected(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/update-contended", format!("{} kept changing while its fork was published", source.name))))
}

async fn close_fork_stage(directory: &dyn Directory, mut stage: DeploymentRecord) -> Result<()> {
    let Some(prepared) = &stage.fork_stage else { return Ok(()); };
    let region = prepared.preparation.token;
    match stage.storage_regions.get_mut(&region) {
        Some(rtdb_directory::StorageRegion::Remote { closed, publications, .. }) if !publications.is_empty() => *closed = true,
        Some(rtdb_directory::StorageRegion::Remote { .. }) => { stage.storage_regions.remove(&region); }
        _ => {}
    }
    let version = stage.version;
    stage.lifecycle = Lifecycle::Deleting;
    stage.epoch = stage.epoch.checked_add(1).ok_or_else(|| {
        Anomaly::fault("rtdb.multi/epoch-exhausted", format!("Installation {} has no remaining writer epochs", stage.id))
    })?;
    directory.replace_deployment(stage, version).await?;
    Ok(())
}

pub(crate) async fn close_abandoned_fork(directory: &dyn Directory, record: &DeploymentRecord) -> Result<()> {
    let Some(stage) = &record.fork_stage else { return Ok(()); };
    if record.lifecycle != Lifecycle::Restoring { return Ok(()); }
    let lineage = crate::store::recover_hosted_lineage(directory, &stage.source_lineage).await?;
    if lineage.preparations().any(|(preparation, _)| preparation == stage.preparation) { return Ok(()); }
    close_fork_stage(directory, record.clone()).await
}

pub(crate) struct ResidenceErasure<'a>(ErasureAuthority<'a>);

enum ErasureAuthority<'a> {
    Disposing(&'a rtdb_directory::ReferenceDisposal),
}

impl ResidenceErasure<'_> {
    pub(crate) fn prefix(&self) -> &str {
        match self.0 {
            ErasureAuthority::Disposing(permit) => permit.residence(),
        }
    }
}


/// One cell, as cell administration reaches it.
pub struct CellHandle<'a> {
    pub name: &'a str,
    pub directory: &'a Arc<dyn Directory>,
    pub storage: &'a CellStorage,
    pub keys: &'a dyn KeyService,
    pub broker: Option<&'a crate::replay_broker::Client>,
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
        Box::pin(prepare_cell_move(from, to, &intent, maximum_message_bytes, hosted_task)).await?
    } else {
        prepared_cell_move(from, to, &intent, Selection::Route).await?
    };
    if at_source
        && !map.assign(name, to.name, Some(from.name)).await?
        && map.cell_of(name).await?.as_deref() != Some(to.name)
    {
        return Err(conflict());
    }
    let arrived = settle_cell_move(from, to, &source, &target).await?;
    Ok(dispose_after_settlement(to, arrived).await)
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
    let (source, target) = Box::pin(prepare_cell_move(from, to, intent, maximum_message_bytes, hosted_task)).await?;
    Ok(PreparedCellTransfer {
        intent: intent.clone(),
        source_incarnation: source.incarnation,
        destination_incarnation: target.incarnation,
        epoch: source.epoch,
        attempt: source.cell_transfer.as_ref().and_then(|transfer| transfer.attempt.clone()),
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
    let receipt = Selection::Receipt(prepared.attempt.as_ref().map(|attempt| attempt.ordinal));
    let (source, target) = prepared_cell_move(from, to, intent, receipt).await?;
    let selected = source.cell_transfer.as_ref().and_then(|transfer| transfer.attempt.as_ref());
    if source.incarnation != prepared.source_incarnation || target.incarnation != prepared.destination_incarnation
        || source.epoch != prepared.epoch || prepared.attempt.as_ref() != selected
    {
        return Err(cell_move_conflict(from, to, &intent.name));
    }
    let arrived = settle_cell_move(from, to, &source, &target).await?;
    Ok(dispose_after_settlement(to, arrived).await)
}

/// Settlement already selected the destination. A provider that has not yet
/// emptied a closed generation leaves it recorded for the reaper's next pass.
async fn dispose_after_settlement(to: &CellHandle<'_>, arrived: DeploymentRecord) -> DeploymentRecord {
    match dispose_abandoned(to, &arrived.id).await {
        Ok(Some(disposed)) => disposed,
        Ok(None) | Err(_) => arrived,
    }
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

/// What selects the destination: the cell map route, which follows the
/// source's own attempt, or a placement catalog's retained receipt.
#[derive(Clone, Copy)]
enum Selection {
    Route,
    Receipt(Option<u32>),
}

async fn prepared_cell_move(
    from: &CellHandle<'_>,
    to: &CellHandle<'_>,
    intent: &CellTransferIntent,
    selection: Selection,
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
    let selected = transfer.attempt.as_ref();
    let generation_matches = selected.is_none_or(|attempt| target.storage_generation == Some(attempt.generation)
        && target.storage_namespaces == Some(attempt.namespaces));
    let abandoned = target.storage_generation.is_some_and(|generation| target.abandoned_generations.contains_key(&generation));
    let ordinal = |record: &DeploymentRecord| record.cell_transfer.as_ref()
        .and_then(|transfer| transfer.attempt.as_ref()).map(|attempt| attempt.ordinal);
    if !generation_matches
        || !crate::cell_attempt::may_activate(target.move_from.is_none(), abandoned, match selection {
            Selection::Route => ordinal(&source),
            Selection::Receipt(receipt) => receipt,
        }, ordinal(&source), ordinal(&target))
    {
        return Err(conflict());
    }
    if target.lifecycle == Lifecycle::Moving {
        require_storage_drained(&target)?;
        let bucket = to.storage.bucket(rtdb_storage::IoRegion::new()).await?;
        require_storage_namespaces(&target, storage_namespaces(bucket.as_ref(), to.keys).await?)?;
        let publications = crate::store::directory_publications(to.keys, &target, IoRegion::new())?;
        Box::pin(crate::archive::verify_cell_move_checkpoint(&target, bucket, &publications)).await?;
    }
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
            Lifecycle::Moving if record.epoch == source.epoch && record.log_epoch == target.log_epoch && record.checkpoint == target.checkpoint => {
                record.lifecycle = Lifecycle::Live;
            }
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
    let maintenance = storage.try_local_maintenance()?.map(Arc::new);
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
    finish_disposal_with_lock(directory, storage, keys, &fenced.id, Some(fenced.incarnation), maximum_message_bytes, maintenance).await?;
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
        return prepared_cell_move(from, to, intent, Selection::Route).await;
    }
    let current = if current.lifecycle == Lifecycle::Live {
        cell_attempts::require_disposed(from, &current).await?
    } else {
        current
    };
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
    let target = match target {
        Some(previous) if previous.lifecycle == Lifecycle::Moving && previous.incarnation == arrival.incarnation
            && previous.move_from.is_some() && intent_matches(&current, intent) && same_media(&current)
            && previous.cell_transfer != current.cell_transfer =>
        {
            // The source selected a fresh attempt before the destination
            // followed it. Only the successor of a closed generation installs.
            Some(cell_attempts::install_selected(to, &previous, &current).await?)
        }
        target => target,
    };
    if let Some(previous) = target {
        if !same_residence(&previous, intent, &intent.destination) {
            return Err(conflict());
        }
        match previous.lifecycle {
            Lifecycle::Moving if previous.incarnation == arrival.incarnation
                && intent_matches(&current, intent) && previous.cell_transfer == current.cell_transfer =>
            {
                if previous.move_from.is_none() {
                    return prepared_cell_move(from, to, intent, Selection::Route).await;
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
                }) && previous.storage_regions.is_empty() && !cell_attempts::has_pending_disposal(&previous)
                    && previous.archive_retirement.as_ref()
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
    let mut target_stream = administration_stream(to.directory.as_ref(), &arrival, maximum_message_bytes).await?;
    let source_journals = source_stream.servers.iter().chain(&current.replica_journals).chain(&current.log_copy_servers);
    if let Some(legacy) = source_journals.clone().find(|endpoint| endpoint.journal().is_none()) {
        return Err(unbound_inventory(legacy.address()));
    }
    let source_addresses: HashSet<_> = source_journals.clone().map(JournalEndpoint::address).collect();
    let source_ids: HashSet<_> = source_journals.filter_map(JournalEndpoint::journal).collect();
    if target_stream.servers.iter().any(|server| source_addresses.contains(server.address())
        || server.journal().is_some_and(|id| source_ids.contains(id))) {
        return Err(Anomaly::incorrect("rtdb.multi/shared-cell-journal", "Cell moves require independent replica journals"));
    }

    let sealed_journals = source_stream.servers.iter()
        .map(|endpoint| endpoint.journal().map(str::to_owned).ok_or_else(|| unbound_inventory(endpoint.address())))
        .collect::<Result<Vec<_>>>()?;
    let first_attempt = rtdb_directory::CellTransferAttempt {
        ordinal: 1,
        generation: uuid::Uuid::new_v4(),
        namespaces: target_binding,
        checkpoint: current.checkpoint,
        sealed_journals,
    };
    let serving_archive = if current.lifecycle == Lifecycle::Live {
        let archive = Box::pin(crate::archive::require_transferable_serving_archive(from.directory.as_ref(), from.storage, from.keys, &current, hosted_task.as_ref())).await?;
        if archive.is_none() {
            let publications = crate::store::directory_publications(from.keys, &current, IoRegion::new())?;
            Box::pin(crate::archive::verify_cell_move_checkpoint(&current, source_bucket, &publications)).await?;
        }
        archive
    } else {
        crate::archive::serving_archive_operation(&current)?
    };

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
            || crate::archive::serving_archive_operation(record)? != serving_archive
        {
            return Err(conflict());
        }
        if record.has_live_suffix_pins(now_ms()) {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/checkpoint-suffix-pinned", "A live checkpoint suffix pin retains this residence"));
        }
        record.checkpoint_suffix_pins.retain(|_, pin| {
            pin.custody != rtdb_directory::CheckpointSuffixCustody::Capture || pin.archive_sha256.is_some()
        });
        record.lifecycle = Lifecycle::Moving;
        record.epoch = record.epoch.checked_add(1).ok_or_else(|| {
            Anomaly::fault("rtdb.multi/epoch-exhausted", format!("Installation {} has no remaining writer epochs", record.id))
        })?;
        record.holder = None;
        record.move_intent = None;
        record.placement_move = None;
        record.journal_placement = None;
        record.storage_namespaces = bound_source.storage_namespaces;
        record.replica_journals.extend(source_stream.servers.iter().cloned());
        record.cell_transfer = Some(Box::new(CellTransfer {
            intent: intent.clone(),
            source_incarnation: record.incarnation,
            destination_incarnation: arrival.incarnation,
            epoch: record.epoch,
            source_media: source_media.clone(),
            destination_media: destination_media.clone(),
            attempt: Some(first_attempt.clone()),
        }));
        Ok(())
    }).await?;
    let mut staged = if let Some(pending) = pending {
        pending
    } else {
        let mut staged = source.clone();
        staged.org = arrival.org;
        staged.incarnation = arrival.incarnation;
        staged.replica_payload_trim = None;
        staged.ensemble = arrival.ensemble;
        staged.journal_placement = None;
        staged.storage_generation = Some(source.cell_transfer.as_ref()
            .and_then(|transfer| transfer.attempt.as_ref())
            .map_or_else(uuid::Uuid::new_v4, |attempt| attempt.generation));
        staged.abandoned_generations.clear();
        staged.transfer_replay_namespace = None;
        staged.recovery_manifest_sha256 = None;
        staged.checkpoint_decision = None;
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
        staged.local_publications.clear();
        staged.catalog_readers.clear();
        staged.catalog_admission_floors.clear();
        staged.checkpoint_recovery = None;
        staged.serving_replay_capture = None;
        staged.checkpoint_captures.clear();
        staged.checkpoint_worlds.clear();
        staged.checkpoint_suffix_pins.clear();
        staged.checkpoint_capture_retirements.clear();
        staged.archive_retirement = None;
        staged.retired_residences.clear();
        staged.activation_crashes = 0;
        if let Some(prior) = &prior_residence {
            staged.retired_residences.clone_from(&prior.retired_residences);
            staged.retired_residences.insert(prior.incarnation, RetiredResidence {
                transfer: prior.cell_transfer.as_deref().cloned().ok_or_else(&conflict)?,
                serving: prior.disposal.as_deref().cloned().ok_or_else(&conflict)?,
                deletion: prior.deletion_record.as_deref().cloned().ok_or_else(&conflict)?,
                archives: prior.archive_retirement.clone(),
            });
            if prior.archive_retirement.is_none() {
                staged.checkpoint_captures.clone_from(&prior.checkpoint_captures);
                staged.checkpoint_worlds.clone_from(&prior.checkpoint_worlds);
                staged.checkpoint_suffix_pins.clone_from(&prior.checkpoint_suffix_pins);
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
        return prepared_cell_move(from, to, intent, Selection::Route).await;
    }

    if staged.move_from.as_ref() != Some(&source_stream.servers) {
        let previous = staged.move_from.as_ref().expect("a pending transfer has source journals");
        let source_journals = &source_stream.servers;
        if !same_transfer_journals(previous, source_journals) {
            return Err(conflict());
        }
        staged = update_installation(to.directory.as_ref(), &staged.id, |record| {
            if record.version != staged.version { return Err(conflict()); }
            record.move_from = Some(source_journals.clone());
            Ok(())
        }).await?;
    }
    rtdb_logkeeper::seal(&source_stream, source.epoch).await?;
    await_storage_drain(from.directory.as_ref(), from.storage, from.keys, &source).await?;
    let mut source = source;
    let mut replaced = false;
    loop {
        let closed = staged.storage_generation.is_some_and(|generation| staged.abandoned_generations.contains_key(&generation));
        if closed && !replaced {
            // An earlier invocation closed this attempt before selecting its successor.
            (source, staged) = cell_attempts::replace(from, to, &source, &staged).await?;
            replaced = true;
            continue;
        }
        crate::store::reconcile_storage_regions(to.directory.as_ref(), to.storage, to.keys, &staged).await?;
        let hosted_lineage = matches!(from.storage, CellStorage::S3 { .. }).then_some(source.lineage.as_str());
        let staging = crate::store::with_source_lineage(from.directory.as_ref(), hosted_lineage, hosted_task.as_ref(),
            Box::pin(stage_cell_storage(from, to, &source, &staged, hosted_task.clone()))).await;
        match staging {
            Ok(()) => break,
            Err(error) if !replaced && cell_attempts::is_conflicting_copy(&error, &staged) => {
                (source, staged) = cell_attempts::replace(from, to, &source, &staged).await?;
                replaced = true;
            }
            Err(error) => return Err(error),
        }
    }

    target_stream.stream = staged.stream("replica");
    crate::archive::reseal_live_journal(
        from, &source, &source_stream, to, &staged, &target_stream, hosted_task.as_ref(),
    ).await?;
    let prepared = update_installation(to.directory.as_ref(), &staged.id, |record| {
        if !same_cell_move(record, &staged) || record.lifecycle != Lifecycle::Moving || record.epoch != staged.epoch
            || record.checkpoint != staged.checkpoint || record.storage_namespaces != staged.storage_namespaces
            || record.storage_prefix() != staged.storage_prefix() || record.log_epoch != staged.log_epoch
            || record.storage_generation.is_some_and(|generation| record.abandoned_generations.contains_key(&generation))
            || record.ensemble != staged.ensemble || record.replica_journals != staged.replica_journals
            || record.move_from.as_ref().is_some_and(|journals| Some(journals) != staged.move_from.as_ref())
        {
            return Err(conflict());
        }
        require_storage_drained(record)?;
        record.move_from = None;
        Ok(())
    }).await?;
    Ok((source, prepared))
}

fn same_transfer_journals(previous: &[JournalEndpoint], source_journals: &[JournalEndpoint]) -> bool {
    previous.len() == source_journals.len() && previous.iter().zip(source_journals).all(|(stored, source)| {
        match stored.journal() {
            Some(id) => source.journal() == Some(id),
            None => stored.address() == source.address(),
        }
    })
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
            || record.storage_generation.is_some_and(|generation| record.abandoned_generations.contains_key(&generation))
        {
            return Err(cell_move_conflict(from, to, &staged.name));
        }
        record.storage_regions.insert(region, if local {
            StorageRegion::Local { archive: None, checkpoint_suffix: None, repair_archive: None, tracks_ordered_index_pages: true, checkpoint_metadata_from: None, checkpoint_replay_from: None, }
        } else {
            crate::store::remote_storage_region(None, hosted_task.clone())
        });
        Ok(())
    }).await;
    let uncertain = admitted.as_ref().is_err_and(|error| error.code != "rtdb.multi/cell-transfer");
    let result = async {
        admitted?;
        use rtdb_storage::ObjectKey;

        to.keys.key(&staged.key_id()).await?;
        let source_cipher = crate::store::directory_publications(from.keys, source, Arc::clone(&io))?;
        let destination_cipher = crate::store::directory_publications(to.keys, staged, Arc::clone(&io))?;
        let reseal_member = async |name: &ObjectKey, body: bytes::Bytes| {
            let plaintext = source_cipher.open(name, body).await?;
            destination_cipher.seal(name, plaintext).await
        };
        let reseal = async |name: &ObjectKey, body: bytes::Bytes| {
            if name.as_str().strip_prefix(crate::store::GARBAGE_DIRECTORY).is_some_and(|path| path.starts_with('/')) {
                return Ok(body);
            }
            if crate::packing::is_pack_key(name.as_str()) {
                crate::packing::reseal_pack(name, body, &reseal_member).await
            } else {
                reseal_member(name, body).await
            }
        };
        let source_bucket = from.storage.bucket(io.clone()).await?;
        let target_bucket = to.storage.bucket(io.clone()).await?;
        crate::reference::materialize(
            source_bucket.as_ref(), &source.storage_prefix(), target_bucket.as_ref(), &staged.storage_prefix(), reseal,
        ).await?;
        Box::pin(crate::archive::materialize_serving_archive(
            from.directory.as_ref(), from.keys, source, staged, target_bucket.clone(), &destination_cipher, hosted_task.as_ref(),
        )).await?;
        if let Some(checkpoint) = staged.checkpoint {
            crate::reference::verify_block_pack(target_bucket.as_ref(), &staged.storage_prefix(), checkpoint.block).await?;
        }
        Box::pin(crate::archive::verify_cell_move_checkpoint(staged, target_bucket, &destination_cipher)).await?;
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
    update_installation_under_disposal(directory, id, None, change).await
}

async fn update_installation_under_disposal(
    directory: &dyn Directory,
    id: &str,
    disposal: Option<&rtdb_directory::ReferenceDisposal>,
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
        let replaced = match disposal {
            Some(permit) => directory.settle_reference_disposal(permit, next, current.version).await?,
            None => directory.replace_deployment(next, current.version).await?,
        };
        if let Cas::Applied(stored) = replaced {
            return Ok(stored);
        }
    }
    Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/installation-contended", format!("Installation {id} kept changing")))
}

pub(crate) async fn administration_stream(directory: &dyn Directory, record: &DeploymentRecord, maximum_message_bytes: usize) -> Result<StreamConfig> {
    let servers = ensemble_servers(record, &members(directory).await?).map_err(|error| {
        Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/administration-ensemble",
            error.message.unwrap_or_else(|| "The administration ensemble is unavailable".into()))
    })?;
    Ok(StreamConfig { write_quorum: write_quorum(record.ensemble.len()), servers, stream: record.stream("replica"), maximum_message_bytes })
}

fn unbound_inventory(address: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/journal-identity-missing",
        format!("Bind the verified journal inventory for the legacy address {address}"))
}

/// Installs operator-verified journal bindings against the inventory version that was inspected.
pub async fn bind_journal_inventory(
    directory: &dyn Directory, id: &str, expected: rtdb_directory::Version,
    bindings: BTreeMap<String, Vec<JournalEndpoint>>,
) -> Result<DeploymentRecord> {
    let changed = || Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/journal-inventory-changed",
        format!("Installation {id} has changed since its journal inventory was inspected"));
    let mut record = directory.deployment(id).await?.ok_or_else(&changed)?;
    if record.version != expected || record.disposal.is_some() { return Err(changed()); }
    let legacy: HashSet<_> = record.replica_journals.iter().chain(&record.log_copy_servers)
        .filter(|endpoint| endpoint.journal().is_none()).map(JournalEndpoint::address).collect();
    if bindings.len() != legacy.len() || bindings.is_empty() || !legacy.iter().all(|address| bindings.contains_key(*address))
        || bindings.values().any(|journals| journals.is_empty() || journals.iter().any(|endpoint|
            endpoint.address().is_empty() || !endpoint.journal().is_some_and(rtdb_types::valid_journal_identity)))
    {
        return Err(Anomaly::incorrect("rtdb.multi/journal-inventory", "Supply the verified journal identities and routes for every legacy address"));
    }
    let bind = |endpoint: JournalEndpoint| match endpoint {
        JournalEndpoint::Address(address) => bindings[&address].clone(),
        bound => vec![bound],
    };
    record.replica_journals = record.replica_journals.into_iter().flat_map(&bind).collect();
    record.log_copy_servers = record.log_copy_servers.into_iter().flat_map(bind).collect();
    directory.replace_deployment(record, expected).await?.applied().ok_or_else(changed)
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
        if record.has_live_suffix_pins(now_ms()) {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/checkpoint-suffix-pinned", "A live checkpoint suffix pin retains this residence"));
        }
        record.checkpoint_suffix_pins.retain(|_, pin| {
            pin.custody != rtdb_directory::CheckpointSuffixCustody::Capture || pin.archive_sha256.is_some()
        });
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
    let maintenance = storage.try_local_maintenance()?.map(Arc::new);
    finish_disposal_with_lock(directory, storage, keys, id, incarnation, maximum_message_bytes, maintenance).await
}

async fn finish_disposal_with_lock(
    directory: &dyn Directory,
    storage: &CellStorage,
    keys: &dyn KeyService,
    id: &str,
    incarnation: Option<u64>,
    maximum_message_bytes: usize,
    maintenance: Option<Arc<std::fs::File>>,
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
    if cell_attempts::has_pending_disposal(&fenced) {
        fenced = cell_attempts::dispose_generations(directory, storage, keys, id).await?.ok_or_else(|| {
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
    let mut stream = if fenced.moved_at_ms.is_some() || fenced.fork_stage.is_some() {
        StreamConfig {
            write_quorum: write_quorum(fenced.replica_journals.len()),
            servers: Vec::new(),
            stream: fenced.stream("replica"),
            maximum_message_bytes,
        }
    } else {
        administration_stream(directory, &fenced, maximum_message_bytes).await?
    };
    let io = rtdb_storage::IoRegion::new().child_holding(maintenance.map(|lock| lock as Arc<dyn Send + Sync>));
    let bucket = storage.bucket(io.clone()).await?;
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
    stream.servers.extend(fenced.replica_journals.iter().cloned());
    stream.servers.extend(fenced.log_copy_servers.iter().cloned());
    if let Some(previous) = &fenced.move_from {
        if previous.iter().any(|endpoint| endpoint.journal().is_none()) {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/journal-identity-missing",
                "Resume the cell transfer from its source directory to bind the source journal identities"));
        }
        stream.servers.extend(previous.iter().cloned());
    }
    if !stream.servers.is_empty() {
        stream.servers = retirement_journals(stream.servers, &members(directory).await?)?;
    }
    if matches!(storage, CellStorage::Local { .. }) {
        crate::store::recover_local_lineage(directory, &fenced.lineage, storage.custody_namespace()?).await?;
    } else {
        crate::store::recover_hosted_lineage(directory, &fenced.lineage).await?;
    }
    let reference_disposal = directory.begin_reference_disposal(id, fenced.version).await?;
    let erasure = ResidenceErasure(ErasureAuthority::Disposing(&reference_disposal));
    let others = deletion_neighbors(directory, &fenced).await?;
    crate::reference::require_disjoint_prefix(&prefix, others.iter())?;
    stream.write_quorum = write_quorum(stream.servers.len());
    if !stream.servers.is_empty() {
        rtdb_logkeeper::retire(&stream, fenced.epoch).await?;
    }
    let journals_retired_at_ms = now_ms();
    let (signed, serving_inventory) = match storage {
        CellStorage::Local { .. } => erase_and_sign_local_disposal(
            bucket.as_ref(), keys, &fenced, &erasure, &others, namespaces, journals_retired_at_ms,
        ).await?,
        CellStorage::S3 { config } => {
            let (first, last) = observe_s3_inventory(directory, config, bucket.as_ref(), &fenced, &erasure, &others).await?;
            withdraw_terminal_lineage_slots(keys, &fenced, &erasure, &others, namespaces).await?;
            let signed = sign_disposal(&fenced, keys, namespaces, DeletionInventory::AllVersionsAndMultipartUploads, Some(first), last).await?;
            (signed, ServingInventory::S3VersionsAndUploads)
        }
    };
    io.close_and_wait().await;
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
    let deleted = update_installation_under_disposal(directory, id, Some(&reference_disposal), |record| {
        if record.epoch != receipt.epoch || record.incarnation != receipt.incarnation
            || record.storage_prefix() != prefix.as_str() || record.storage_namespaces != Some(namespaces)
            || record.disposal.is_none() && (record.replica_journals != fenced.replica_journals
                || record.log_copy_servers != fenced.log_copy_servers || record.move_from != fenced.move_from) {
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
                record.replica_journals.clone_from(&receipt.journals);
                record.disposal = Some(Box::new(receipt.clone()));
                record.deletion_record = Some(Box::new(signed.clone()));
                record.lifecycle = Lifecycle::Deleted;
                Ok(())
            }
            Lifecycle::Deleted if record.moved_at_ms.is_some() => {
                record.replica_journals.clone_from(&receipt.journals);
                record.disposal = Some(Box::new(receipt.clone()));
                Ok(())
            }
            _ => Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb.multi/disposal-fence", format!("Installation {id} left disposal"))),
        }
    }).await?;
    deleted.disposal.map(|receipt| *receipt).ok_or_else(|| Anomaly::fault("rtdb.multi/disposal-receipt", format!("Installation {id} lacks a disposal receipt")))
}

pub(crate) fn retirement_journals(
    inventory: Vec<JournalEndpoint>, members: &BTreeMap<MemberId, MemberRecord>,
) -> Result<Vec<JournalEndpoint>> {
    let mut journals = BTreeMap::<String, JournalEndpoint>::new();
    for endpoint in inventory {
        let id = endpoint.journal().ok_or_else(|| unbound_inventory(endpoint.address()))?.to_owned();
        let endpoint = members.get(&MemberId(id.clone())).filter(|member| member.role == MemberRole::Logkeeper)
            .map_or(endpoint, journal_endpoint);
        if let Some(other) = journals.insert(id, endpoint.clone()) && other != endpoint {
            return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/journal-route-ambiguous",
                format!("The retired journal has conflicting routes {} and {}", other.address(), endpoint.address())));
        }
    }
    Ok(journals.into_values().collect())
}

async fn erase_and_sign_local_disposal(
    bucket: &dyn rtdb_storage::ObjectStore,
    keys: &dyn KeyService,
    fenced: &DeploymentRecord,
    erasure: &ResidenceErasure<'_>,
    others: &[DeploymentRecord],
    namespaces: StorageNamespaces,
    journals_retired_at_ms: i64,
) -> Result<(DeletionRecord, ServingInventory)> {
    // Its forks keep what they share with it: those objects are copied into them before it goes.
    if !crate::reference::erase_deployment(bucket, erasure, others).await? {
        return Err(Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/disposal-prefix", format!("Installation {} still has serving objects", fenced.id)));
    }
    let objects_erased_at_ms = now_ms().max(journals_retired_at_ms);
    withdraw_terminal_lineage_slots(keys, fenced, erasure, others, namespaces).await?;
    let signed = sign_disposal(fenced, keys, namespaces, DeletionInventory::CurrentObjects, None, objects_erased_at_ms).await?;
    Ok((signed, ServingInventory::LocalDirectory))
}

/// The current reference-disposal permit closes the lineage to new forks.
/// Older origins can remain live in relocated copies until the last residence
/// is gone, including origins whose own ciphertext prefix is already empty.
async fn withdraw_terminal_lineage_slots(
    keys: &dyn KeyService,
    record: &DeploymentRecord,
    erasure: &ResidenceErasure<'_>,
    others: &[DeploymentRecord],
    namespaces: StorageNamespaces,
) -> Result<()> {
    if !matches!(erasure.0, ErasureAuthority::Disposing(_)) { return Ok(()); }
    if record.fork_stage.is_some() {
        return withdraw_unreferenced_publication_origin(keys, record, &record.storage_prefix()).await;
    }
    let peers = others.iter().filter(|peer| peer.org == record.org && peer.lineage == record.lineage);
    if peers.clone().any(|peer| crate::reclamation_decision::retains_publication_scope(
        peer.lifecycle == Lifecycle::Deleted, peer.disposal.is_some(), peer.storage_regions.is_empty(),
    )) {
        return Ok(());
    }
    let mut origins = BTreeSet::from([record.storage_prefix()]);
    for residence in std::iter::once(record).chain(peers) {
        for deletion in residence.deletion_record.iter().map(Box::as_ref)
            .chain(residence.retired_residences.values().map(|retired| &retired.deletion))
        {
            let facts = &deletion.facts;
            let mut path = facts.storage_prefix.split('/');
            let _shard = path.next();
            if path.next() == Some(record.org.as_str()) && path.next() == Some(record.lineage.as_str())
                && facts.namespaces.keys == namespaces.keys
            {
                origins.insert(facts.storage_prefix.clone());
            }
        }
    }
    for origin in origins {
        withdraw_unreferenced_publication_origin(keys, record, &origin).await?;
    }
    Ok(())
}

async fn withdraw_unreferenced_publication_origin(keys: &dyn KeyService, record: &DeploymentRecord, prefix: &str) -> Result<()> {
    let scope = crate::keys::UnitScope::directory_serving(&record.org, &record.lineage)
        .publication_scope(crate::keys::UnitScope::publication_origin(prefix));
    keys.withdraw_unit_scope(scope, &rtdb_storage::IoRegion::new()).await
}

async fn sign_disposal(
    fenced: &DeploymentRecord, keys: &dyn KeyService, namespaces: StorageNamespaces,
    inventory: DeletionInventory, first_empty_observed_at_ms: Option<i64>, objects_erased_at_ms: i64,
) -> Result<DeletionRecord> {
    crate::ordered_index_storage::withdraw_residence_keys(keys, fenced).await?;
    if fenced.checkpoint_stage.is_some() || fenced.checkpoint_transfer_activation.is_some() {
        keys.withdraw_archive_key(&crate::archive::stage::staged_key_id(fenced)).await?;
    }
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

async fn observe_s3_inventory(directory: &dyn Directory, config: &rtdb_s3::S3Config,
                             bucket: &dyn rtdb_storage::ObjectStore, deployment: &DeploymentRecord,
                             erasure: &ResidenceErasure<'_>, others: &[DeploymentRecord]) -> Result<(i64, i64)> {
    let first_empty_at_ms = match deployment.deletion_observation {
        Some(DeletionObservation::Verified { first_empty_at_ms, last_empty_at_ms }) => {
            if first_empty_at_ms < 0 || last_empty_at_ms < s3_write_lease_horizon(first_empty_at_ms) || last_empty_at_ms > now_ms() {
                return Err(Anomaly::fault("rtdb.multi/deletion-observation", "The stored empty inventory timestamps are inconsistent"));
            }
            return Ok((first_empty_at_ms, last_empty_at_ms));
        }
        Some(DeletionObservation::Empty { observed_at_ms }) => observed_at_ms,
        Some(DeletionObservation::Fenced { .. }) => {
            crate::reference::preserve_residence(bucket, erasure, others).await?;
            let prefix = rtdb_storage::ObjectKey::new(erasure.prefix())?;
            CellStorage::s3_inventory(config).await?.purge(&prefix).await?;
            if !bucket.list(Some(&prefix)).await?.is_empty() { return Err(deletion_incomplete(deployment)); }
            let observed_at_ms = now_ms();
            advance_observation(directory, deployment, Some(DeletionObservation::Empty { observed_at_ms })).await?;
            return Err(observation_pending(observed_at_ms));
        }
        None => return Err(deletion_changed(&deployment.name)),
    };
    if now_ms() < s3_write_lease_horizon(first_empty_at_ms) { return Err(observation_pending(first_empty_at_ms)); }

    let inventory = CellStorage::s3_inventory(config).await?;
    let prefix = rtdb_storage::ObjectKey::new(erasure.prefix())?;
    if !inventory.is_empty(&prefix).await? || !bucket.list(Some(&prefix)).await?.is_empty() {
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

/// A saved replica prefix that can be passed to the offline verifier.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AuditAnchor {
    pub stream: String,
    pub root_incarnation: u64,
    pub interval: [i64; 2],
    pub digest: String,
}

/// Acceptance-stage coordinates from one retained replica record.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AuditAcceptanceStage {
    pub replica_offset: i64,
    pub source_position: i64,
    pub term: i64,
    pub ordered_at_micros: i64,
    pub staging_clock_upper_micros: i64,
    pub cutoff_micros: i64,
}

/// Exact retained quorum evidence, including when this audit first had the quorum proof.
#[derive(Debug, serde::Serialize)]
pub struct AuditAcceptanceQuorum {
    pub authority: uuid::Uuid,
    pub incarnation: u64,
    pub log_epoch: i32,
    pub replica_stream: String,
    pub replica_digest: String,
    pub required_replica_quorum: usize,
    pub original_replica_stream: Option<String>,
    pub clock_certificate_sha256: Option<String>,
    pub original_journals: Vec<String>,
    pub required_original_quorum: Option<u64>,
    pub acceptance: AuditAcceptanceStage,
    pub replica_append_sample_micros: i64,
    pub journal_cutoff_micros: Option<i64>,
    pub quorum_observed_clock_upper_micros: i64,
    pub journal_sync_observations: BTreeMap<String, i64>,
    pub quorum_sync_clock_upper_micros: Option<i64>,
}

/// Read one strict placed transaction whose encrypted replica entry matches a write quorum.
pub async fn audit_acceptance_quorum(
    directory: &dyn Directory, keys: &dyn KeyService, authority: uuid::Uuid,
    incarnation: u64, log_epoch: i32, offset: i64, maximum_message_bytes: usize,
) -> Result<AuditAcceptanceQuorum> {
    use rtdb_format::envelope::{ReplicaBody, ReplicaEnvelope};

    if authority.is_nil() || incarnation == 0 || log_epoch < 0 || !(0..(1_i64 << 48)).contains(&offset) {
        return Err(Anomaly::incorrect("rtdb.multi/acceptance-audit-scope", "Select an exact installation, log epoch and replica offset"));
    }
    let id = authority.to_string();
    let unavailable = || Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/acceptance-audit-scope",
        "The exact installation and replica stream are unavailable");
    let current = directory.deployment(&id).await?.filter(|record|
        record.id == id && record.incarnation == incarnation && record.log_epoch == log_epoch && record.lifecycle != Lifecycle::Deleted,
    ).ok_or_else(unavailable)?;
    let servers = replica_servers(directory, &current).await?;
    let stream = StreamConfig { write_quorum: write_quorum(servers.len()), servers, stream: current.stream("replica"), maximum_message_bytes };
    let (proof, journal_sync_observations) = rtdb_logkeeper::certify_observed_entry(&stream, offset).await?;
    let quorum_observed_clock_upper_micros = chrono::Utc::now().timestamp_micros().checked_add(1)
        .filter(|upper| *upper > 0).ok_or_else(|| Anomaly::fault("rtdb.multi/acceptance-audit-clock", "The audit clock has no valid upper sample"))?;

    let mut entries = rtdb_logkeeper::copy_certified_range(&stream, &proof, maximum_message_bytes.saturating_add(64), 1).await?;
    let entry = entries.pop().filter(|entry| entry.lsn == offset).ok_or_else(unavailable)?;
    let not_strict = || Anomaly::incorrect("rtdb.multi/acceptance-audit-record", "The certified record is not a strict placed acceptance");
    if !entry.guarded { return Err(not_strict()); }
    let units = crate::logs::JournalUnits::open(
        keys, &current.key_id(), &current.replica_seal_scope(), IoRegion::new(),
    ).await?;
    let payload = units.decrypt(entry.payload.clone()).await?;
    let Some(ReplicaEnvelope { term, message: ReplicaBody::ResolvedTx(tx), .. }) = ReplicaEnvelope::decode(&payload)? else {
        return Err(not_strict());
    };
    let stage = tx.checked_acceptance_stage()?.ok_or_else(not_strict)?;
    // The journal frame's cutoff and certificate are outside the deployment seal. They must
    // repeat the sealed stage, so the reported clock evidence is the writer's own decision.
    let sealed = stage.journal.map(|cutoff| (cutoff.before().0, cutoff.certificate_sha256()));
    let framed = entry.guarded_before.zip(entry.timed_origin.as_ref().map(|origin| &origin.clock_certificate_sha256[..]));
    if !rtdb_types::journal_frame_matches(sealed, framed) {
        return Err(Anomaly::fault("rtdb.multi/acceptance-audit-clock-evidence",
            "The journal frame does not repeat the sealed cutoff and clock certificate"));
    }
    let (original_journals, required_original_quorum, journal_sync_observations) = match &entry.timed_origin {
        Some(origin) => {
            let members = members(directory).await?;
            let mut known = BTreeMap::new();
            for endpoint in stream.servers.iter().chain(&current.replica_journals).chain(&current.log_copy_servers) {
                if let Some(journal) = endpoint.journal() {
                    known.entry(journal).or_insert(endpoint);
                }
            }
            let endpoints = origin.journals.iter().filter_map(|journal| {
                members.get(&MemberId(journal.clone())).filter(|member| member.role == MemberRole::Logkeeper)
                    .map(journal_endpoint).or_else(|| known.get(journal.as_str()).map(|endpoint| (**endpoint).clone()))
            }).collect();
            let observations = rtdb_logkeeper::original_sync_observations(&proof, &entry, endpoints, maximum_message_bytes).await?;
            (origin.journals.clone(), Some(origin.write_quorum), observations)
        }
        None if entry.guarded_before.is_none() => (Vec::new(), None, journal_sync_observations),
        None => return Err(not_strict()),
    };
    let quorum_sync_clock_upper_micros = rtdb_logkeeper::timely_original_sync_upper(&entry, &journal_sync_observations);
    let after = directory.deployment(&id).await?.filter(|record|
        record.id == id && record.incarnation == incarnation && record.log_epoch == log_epoch && record.lifecycle != Lifecycle::Deleted,
    ).ok_or_else(unavailable)?;
    if replica_servers(directory, &after).await? != stream.servers { return Err(unavailable()); }
    Ok(AuditAcceptanceQuorum {
        authority, incarnation, log_epoch, replica_stream: proof.stream, replica_digest: proof.digest,
        required_replica_quorum: stream.write_quorum,
        original_replica_stream: entry.timed_origin.as_ref().map(|origin| origin.stream.clone()),
        clock_certificate_sha256: sealed.map(|(_, certificate)| {
            certificate.iter().map(|byte| format!("{byte:02x}")).collect()
        }),
        original_journals, required_original_quorum,
        acceptance: AuditAcceptanceStage {
            replica_offset: offset, source_position: stage.source_position.0, term: term.0,
            ordered_at_micros: stage.ordered_at.0, staging_clock_upper_micros: stage.clock_upper.0,
            cutoff_micros: stage.cutoff.0,
        },
        replica_append_sample_micros: entry.timestamp,
        journal_cutoff_micros: sealed.map(|(before, _)| before),
        quorum_observed_clock_upper_micros,
        journal_sync_observations: journal_sync_observations.into_iter()
            .map(|observation| (observation.journal, observation.synced_clock_upper_micros)).collect(),
        quorum_sync_clock_upper_micros,
    })
}

/// What auditing a deployment's history found (`NoTwoWritersHaveEffect`, `SystemTimeRises`).
#[derive(Clone, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct AuditReport {
    pub records: usize,
    pub transactions: usize,
    #[serde(default)]
    pub retained_acceptance_stages: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_retained_acceptance_stage: Option<AuditAcceptanceStage>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_anchor: Option<AuditAnchor>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_continued_digest: Option<String>,
}

/// Audits the retained replica stream and compares the common committed range on its reachable replicas.
pub async fn audit(directory: &dyn Directory, keys: &dyn KeyService, name: &str, maximum_message_bytes: usize) -> Result<AuditReport> {
    use rtdb_format::envelope::{ReplicaBody, ReplicaEnvelope};
    let current = record(directory, name).await?;
    let servers = replica_servers(directory, &current).await?;
    let stream = StreamConfig { write_quorum: write_quorum(servers.len()), servers, stream: current.stream("replica"), maximum_message_bytes };
    let reader = rtdb_logkeeper::open_reader(stream.clone(), CancellationToken::new()).await?;
    let latest = reader.latest()?.0;
    let log = if latest >= 0 { Some(crate::logs::SealedLog::new(reader, keys, &current.key_id(), &current.replica_seal_scope(), IoRegion::new()).await?) } else { None };
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
                if let Some(stage) = tx.checked_acceptance_stage()? {
                    report.retained_acceptance_stages += 1;
                    report.last_retained_acceptance_stage = Some(AuditAcceptanceStage {
                        replica_offset: record.metadata.offset.0,
                        source_position: stage.source_position.0,
                        term: envelope.term.0,
                        ordered_at_micros: stage.ordered_at.0,
                        staging_clock_upper_micros: stage.clock_upper.0,
                        cutoff_micros: stage.cutoff.0,
                    });
                }
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
    if let Some(anchor) = current.replica_anchor {
        let prefix = rtdb_logkeeper::JournalDigest::from_digest_bytes(stream.stream.clone(), anchor.interval, anchor.digest);
        if let Some([_, through]) = retained.interval.filter(|[_, through]| *through > anchor.interval[1]) {
            match rtdb_logkeeper::certify_committed_prefix(&stream, through, Some(&prefix)).await {
                Ok(proof) => report.replica_continued_digest = Some(proof.digest),
                Err(error) if error.category == AnomalyCategory::Unavailable => (),
                Err(error) => return Err(error),
            }
        }
        report.replica_anchor = Some(AuditAnchor { stream: prefix.stream, root_incarnation: anchor.root_incarnation, interval: prefix.interval, digest: prefix.digest });
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
    pub load_limits: crate::load::LoadLimits,
    pub read_width: Option<crate::read_width::ReadWidthOptions>,
    /// Consecutive failed probes after which a node is confirmed dead, ahead of its lease.
    pub suspicion_probes: u32,
    pub probe_timeout: Duration,
    /// The cell's control key, with which the controller signs its probes and moves.
    pub control: ControlKey,
}

impl Default for ControllerOptions {
    fn default() -> Self { Self { interval: Duration::from_secs(1), grace: Duration::from_secs(5), rebalance_moves: 0, load_limits: Default::default(), read_width: None, suspicion_probes: 2, probe_timeout: Duration::from_millis(300), control: ControlKey::default() } }
}

pub struct FailedMemberProbe { control: String, pgwire: String, consecutive: u32 }

impl FailedMemberProbe {
    fn matches(&self, member: &MemberRecord) -> bool {
        member.control_address.as_deref() == Some(self.control.as_str()) && member.address == self.pgwire
    }
}

/// Probes every live node's control and pgwire endpoints and confirms consecutive failures for the same endpoint pair.
/// Safety never depends on the verdict: a wrongly confirmed node's writes are already fenced by the epoch its successor claims.
pub async fn confirm_suspicions(directory: &dyn Directory, failures: &mut BTreeMap<MemberId, FailedMemberProbe>, options: &ControllerOptions) -> Result<usize> {
    use futures::StreamExt;

    if options.suspicion_probes == 0 { return Ok(0); }
    let now = now_ms();
    let mut members = directory.members().await?;
    members.retain(|member| {
        let probe = member.role == MemberRole::Node && member.is_live(now) && member.control_address.is_some();
        if !probe || failures.get(&member.id).is_some_and(|failure| !failure.matches(member)) {
            failures.remove(&member.id);
        }
        probe
    });
    let mut probes = std::pin::pin!(futures::stream::iter(members).map(|member| async move {
        let address = member.control_address.as_deref().expect("members without control addresses were filtered");
        match control::ping(address, options.probe_timeout, &options.control).await {
            Ok(overflows) => {
                let pgwire_ok = crate::gateway::probe_pgwire(&member.address, options.probe_timeout, overflows.is_some()).await;
                let crowded = if !pgwire_ok && let Some(before) = overflows {
                    matches!(control::ping(address, options.probe_timeout, &options.control).await, Ok(Some(after)) if before != after)
                } else { false };
                (member, true, pgwire_ok || crowded)
            }
            Err(_) => (member, false, false),
        }
    }).buffer_unordered(CONCURRENT_SUSPICION_PROBES));
    let mut confirmed = 0;
    while let Some((member, control_ok, pgwire_ok)) = probes.next().await {
        let previous = failures.get(&member.id);
        let consecutive = crate::member_probe::failed_rounds(previous.map_or(0, |probe| probe.consecutive),
            previous.is_some_and(|probe| probe.matches(&member)), control_ok, pgwire_ok);
        if consecutive == 0 { failures.remove(&member.id); continue; }
        let failure = FailedMemberProbe {
            control: member.control_address.clone().expect("the member was probed at this address"),
            pgwire: member.address.clone(), consecutive,
        };
        if consecutive >= options.suspicion_probes {
            // The suspect may still be renewing its lease, so the confirmation re-reads and retries rather than losing to a renewal.
            let mut current = member.clone();
            for _ in 0..8 {
                if matches!(current.state, MemberState::Dead | MemberState::Retired) || !failure.matches(&current) { break; }
                let mut dead = current.clone();
                dead.state = MemberState::Dead;
                match directory.replace_member(dead, current.version).await? {
                    Cas::Applied(_) => { confirmed += 1; break; }
                    Cas::Conflict(Some(stored)) => current = stored,
                    Cas::Conflict(None) => break,
                }
            }
            failures.remove(&member.id);
        } else {
            failures.insert(member.id, failure);
        }
    }
    Ok(confirmed)
}

pub async fn run_controller(directory: Arc<dyn Directory>, options: ControllerOptions, cancellation: CancellationToken) {
    let mut failures = BTreeMap::new();
    let mut samples = crate::load::LoadSamples::default();
    let mut read_width = crate::read_width::ReadWidthScaler::default();
    loop {
        tokio::select! {
            _ = cancellation.cancelled() => return,
            _ = tokio::time::sleep(options.interval) => {
                let _ = reap_members(directory.as_ref(), options.grace).await;
                if options.suspicion_probes > 0 { let _ = confirm_suspicions(directory.as_ref(), &mut failures, &options).await; }
                if let Some(policy) = options.read_width {
                    let _ = read_width.adjust(directory.as_ref(), policy, &options.control, options.probe_timeout).await;
                }
                if options.rebalance_moves > 0 {
                    if options.load_limits.enabled() {
                        let _ = crate::load::rebalance(directory.as_ref(), options.rebalance_moves, options.load_limits, &options.control, &mut samples).await;
                    } else {
                        let _ = rebalance(directory.as_ref(), options.rebalance_moves, &options.control).await;
                    }
                }
            }
        }
    }
}

/// The cell's load as autoscaling reads it: what each node holds, and how many nodes the active deployments need at `target_per_node`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CellStatus {
    pub deployments: usize,
    pub active: usize,
    #[serde(default)]
    pub resident_idle: usize,
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
    let mut resident_idle = 0;
    let mut quarantined = 0;
    for deployment in &deployments {
        if deployment.lifecycle == Lifecycle::Quarantined { quarantined += 1; }
        if let Some(holder) = effective_holder(deployment, &members, now) {
            if deployment.is_resident_idle() { resident_idle += 1; } else { active += 1; }
            *nodes.entry(holder.to_string()).or_default() += 1;
        }
    }
    let live = deployments.iter().filter(|deployment| matches!(deployment.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)).count();
    Ok(CellStatus {
        deployments: live, active, resident_idle, parked: live - active - resident_idle, quarantined, nodes,
        logkeepers: members.values().filter(|member| member.role == MemberRole::Logkeeper && member.is_live(now)).count(),
        recommended_nodes: active.div_ceil(target_per_node.max(1)).max(1),
    })
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct PlacementPoolStatus {
    pub eligible: usize,
    pub draining: usize,
    pub holdings: usize,
    #[serde(default)]
    pub resident_idle: usize,
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
    let mut nodes: BTreeMap<_, (usize, usize)> = members.values().filter(|member| member.may_hold(now) && member.pool != MemberPool::Compaction)
        .map(|member| (&member.id, (0, 0))).collect();
    for record in live_holdings(directory, &members, now).await? {
        if let Some((held, idle)) = effective_holder(&record, &members, now).and_then(|holder| nodes.get_mut(holder)) {
            *held += 1;
            *idle += usize::from(record.is_resident_idle());
        }
    }
    let pool_status = |pool, minimum| {
        let mut status = PlacementPoolStatus { eligible: 0, draining: 0, holdings: 0, resident_idle: 0, recommended_nodes: 0 };
        for (id, (held, idle)) in &nodes {
            let member = &members[*id];
            if member.pool != pool { continue; }
            status.holdings += held;
            status.resident_idle += idle;
            if member.may_receive_placements(now) { status.eligible += 1; }
            if member.state == MemberState::Draining { status.draining += 1; }
        }
        status.recommended_nodes = (status.holdings - status.resident_idle).div_ceil(target_per_node.max(1)).max(minimum);
        status
    };
    Ok(PlacementStatus {
        general: pool_status(MemberPool::General, 1),
        isolation: pool_status(MemberPool::Isolation, 0),
        nodes: nodes.into_iter().map(|(id, (held, _))| (id.to_string(), held)).collect(),
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

    use super::{CellStorage, DeploymentRecord, Directory, ErasureAuthority, KeyService, Lifecycle, ResidenceErasure, create_deployment, delete_deployment, erase_and_sign_local_disposal};
    use crate::keys::LocalKeyService;

    async fn publish_test_fork_cut(
        directory: &dyn Directory, bucket: &crate::reference::Bucket, mut record: DeploymentRecord,
        keys: &LocalKeyService, block: i64,
    ) -> DeploymentRecord {
        use rtdb_format::{block::proto, catalog::BlockCatalog, keys::block_key};
        use rtdb_types::{BlockIndex, ReplicaPosition, SourcePosition, StorageEpoch};

        let root = format!("{}/{}", record.storage_prefix(), crate::archive::checkpoint_storage_root().unwrap());
        let store = crate::reference::PrefixedStore::new(bucket.clone(), root);
        crate::store::check_root(&store, &record.id, true).await.unwrap();
        let packed = crate::packing::PackingStore::open(std::sync::Arc::new(store), crate::packing::PackDiscovery::List).await.unwrap();
        keys.key(&record.key_id()).await.unwrap();
        let publications = crate::store::directory_publications(keys, &record, rtdb_storage::IoRegion::new()).unwrap();
        let plaintext = crate::store::EncryptingStore::published(packed, publications);
        let epoch = StorageEpoch(record.log_epoch);
        let mut catalog = BlockCatalog::from_proto(proto::Block {
            block_index: Some(block),
            boundary_replica_msg_id: Some(ReplicaPosition::from_epoch_offset(epoch, 0).unwrap().0),
            latest_processed_msg_id: Some(SourcePosition::from_epoch_offset(epoch, 0).unwrap().0),
            latest_completed_tx: Some(proto::TxKey { tx_id: Some(0), system_time: Some(1) }),
            ..Default::default()
        }).unwrap();
        plaintext.put(&ObjectKey::new(block_key(BlockIndex(block)).unwrap()).unwrap(), catalog.encode_for_publication().into()).await.unwrap();
        record.checkpoint = Some(rtdb_directory::Checkpoint {
            block, boundary: 0, source: 0, system_time: 1, root_sha256: catalog.publication_sha256(),
        });
        let version = record.version;
        directory.replace_deployment(record, version).await.unwrap().applied().unwrap()
    }

    #[tokio::test]
    async fn a_new_deployment_rejects_an_organisation_with_multiple_path_components() {
        let directory = MemoryDirectory::new();
        let error = create_deployment(directory.as_ref(), "shop", "acme/orders", 0).await.unwrap_err();
        assert_eq!(error.code, "rtdb.multi/invalid-organisation");
        assert!(directory.deployments().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn retirement_needs_versioned_custody_for_every_legacy_address() {
        use super::{BTreeMap, JournalEndpoint, MemberId, MemberPool, MemberRecord, MemberRole, MemberState, bind_journal_inventory, retirement_journals};

        let address = "127.0.0.1:7654";
        let replacement = MemberRecord {
            id: MemberId::fresh(), role: MemberRole::Logkeeper, address: address.into(), control_address: None,
            availability_zone: "local".into(), engine_version: String::new(), lease_expires_at_ms: i64::MAX,
            state: MemberState::Eligible, drain_target_engine: None, pool: MemberPool::default(),
            storage_namespaces: None, version: 1,
        };
        let members = BTreeMap::from([(replacement.id.clone(), replacement)]);
        let directory = MemoryDirectory::new();
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.replica_journals.insert(address.into());
        let record = directory.create_deployment(record).await.unwrap();
        let refused = retirement_journals(record.replica_journals.iter().cloned().collect(), &members).unwrap_err();
        assert_eq!(refused.code, "rtdb.multi/journal-identity-missing");

        let journals = [7655, 7656].map(|port| JournalEndpoint::Member {
            address: format!("127.0.0.1:{port}"), journal: MemberId::fresh().0,
        });
        let bindings = BTreeMap::from([(address.to_owned(), journals.to_vec())]);
        let stale = bind_journal_inventory(directory.as_ref(), &record.id, record.version + 1, bindings.clone()).await.unwrap_err();
        assert_eq!(stale.code, "rtdb.multi/journal-inventory-changed");
        let bound = bind_journal_inventory(directory.as_ref(), &record.id, record.version, bindings).await.unwrap();
        let admitted = retirement_journals(bound.replica_journals.into_iter().collect(), &members).unwrap();
        assert_eq!(admitted.into_iter().collect::<std::collections::BTreeSet<_>>(), journals.into_iter().collect());
    }

    #[test]
    fn a_staged_copy_can_follow_a_journal_to_its_new_route() {
        use super::{JournalEndpoint, MemberId, same_transfer_journals};

        let journal = MemberId::fresh().0;
        let previous = JournalEndpoint::Member { address: "127.0.0.1:7654".into(), journal: journal.clone() };
        let rerouted = JournalEndpoint::Member { address: "127.0.0.1:7655".into(), journal };
        let replacement = JournalEndpoint::Member { address: previous.address().into(), journal: MemberId::fresh().0 };
        assert!(same_transfer_journals(&[previous.clone()], &[rerouted.clone()]));
        assert!(!same_transfer_journals(&[previous.clone()], &[replacement]));
        assert!(!same_transfer_journals(&[previous.address().into()], &[rerouted]));
    }

    #[tokio::test]
    async fn local_deletion_and_fork_retry_before_mutating_storage_when_maintenance_is_active() {
        let directory: std::sync::Arc<dyn Directory> = MemoryDirectory::new();
        let data = tempfile::tempdir().unwrap();
        let storage = CellStorage::Local { root: data.path().join("objects") };
        let keys = LocalKeyService::new(data.path().join("keys")).unwrap();
        let deployment = create_deployment(directory.as_ref(), "shop", "acme", 0).await.unwrap();
        let key = keys.key(&deployment.key_id()).await.unwrap();
        let maintenance = storage.try_local_maintenance().unwrap().unwrap();

        let deletion = delete_deployment(directory.as_ref(), &storage, &keys, "shop", 4 << 20).await.unwrap_err();
        assert_eq!(deletion.code, "rtdb.multi/storage-maintenance-busy");
        let fork = super::fork_at_block(&directory, &storage, &keys, "shop", "fork", None, 0).await.unwrap_err();
        assert_eq!(fork.code, "rtdb.multi/storage-maintenance-busy");
        assert_eq!(directory.deployment(&deployment.id).await.unwrap(), Some(deployment.clone()));
        assert!(directory.deployment_by_name("fork").await.unwrap().is_none());
        assert_eq!(keys.existing(&deployment.key_id()).await.unwrap(), key);

        drop(maintenance);
        assert!(storage.try_local_maintenance().unwrap().is_some());
    }

    struct DisposalTraceStore<'a> {
        inner: &'a dyn ObjectStore,
        effects: parking_lot::Mutex<Vec<(&'static str, String)>>,
    }

    #[async_trait]
    impl ObjectStore for DisposalTraceStore<'_> {
        async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
            self.inner.head(key).await
        }
        async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
            self.inner.get(key).await
        }
        async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
            self.inner.get_range(key, range).await
        }
        async fn put(&self, key: &ObjectKey, bytes: Bytes) -> rtdb_storage::Result<()> {
            self.inner.put(key, bytes).await?;
            self.effects.lock().push(("copy-fork", key.to_string()));
            Ok(())
        }
        async fn put_file(&self, key: &ObjectKey, path: &Path) -> rtdb_storage::Result<()> {
            self.inner.put_file(key, path).await?;
            self.effects.lock().push(("copy-fork", key.to_string()));
            Ok(())
        }
        async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
            self.inner.list(directory).await
        }
        async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
            self.inner.delete(key).await?;
            assert!(matches!(self.inner.head(key).await, Err(rtdb_storage::Error::MissingObject(_))));
            self.effects.lock().push(("erase-residence", key.to_string()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn local_disposal_records_copy_and_removal_of_each_publication() {
        let data = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(data.path().join("keys")).unwrap();
        let storage = CellStorage::Local { root: data.path().join("objects") };
        let bucket = storage.bucket(rtdb_storage::IoRegion::new()).await.unwrap();
        let directory: std::sync::Arc<dyn Directory> = MemoryDirectory::new();
        let broker = fork_broker(&directory, &storage, data.path());
        let mut source = DeploymentRecord::new("source", "acme", Vec::new(), 0);
        let namespaces = super::storage_namespaces(bucket.as_ref(), &keys).await.unwrap();
        source.storage_namespaces = Some(namespaces);
        let source = directory.create_deployment(source).await.unwrap();
        let source = publish_test_fork_cut(directory.as_ref(), &bucket, source, &keys, 0).await;
        let source_prefix = source.storage_prefix();
        let source_store = format!("{source_prefix}/v06");
        for name in ["old", "shared"] {
            let key = ObjectKey::new(format!("{source_store}/{name}")).unwrap();
            bucket.put(&key, Bytes::from_static(name.as_bytes())).await.unwrap();
        }

        let destination = super::ForkDestination { name: "child", org: None, hosted_task: None, broker: Some(&broker) };
        let child = super::with_fork_access(directory.as_ref(), &storage, &keys, &source, None, |access|
            super::fork_with_tail(directory.clone(), &storage, access, &keys, &source, destination, None, Vec::new(), 4 << 20)).await.unwrap();
        let fork_store = format!("{}/v06", child.storage_prefix());
        for name in ["old", "shared"] {
            let owned = ObjectKey::new(format!("{fork_store}/{name}")).unwrap();
            assert!(matches!(bucket.head(&owned).await, Err(rtdb_storage::Error::MissingObject(_))));
        }
        let fenced = super::begin_disposal(directory.as_ref(), &source.id).await.unwrap();
        let permit = directory.begin_reference_disposal(&fenced.id, fenced.version).await.unwrap();
        assert_eq!(permit.residence(), source_prefix);
        let erasure = ResidenceErasure(ErasureAuthority::Disposing(&permit));
        let watched = DisposalTraceStore { inner: bucket.as_ref(), effects: Default::default() };
        let (proof, _) = erase_and_sign_local_disposal(
            &watched, &keys, &fenced, &erasure, &[child], namespaces, 0,
        ).await.unwrap();
        assert!(proof.facts.verified_empty);
        for name in ["old", "shared"] {
            let copied = ObjectKey::new(format!("{fork_store}/{name}")).unwrap();
            assert_eq!(bucket.get(&copied).await.unwrap(), name.as_bytes());
        }
        let effects: Vec<_> = watched.effects.into_inner().into_iter()
            .filter(|(_, path)| path.ends_with("/old") || path.ends_with("/shared"))
            .collect();
        assert_eq!(effects.len(), 4, "each publication is copied and removed once");
        if let Some(path) = std::env::var_os("RTDB_RESIDENCE_DISPOSAL_TRACE_PATH") {
            let trace = serde_json::json!({ "source_store": source_store, "fork_store": fork_store, "effects": effects });
            std::fs::write(path, serde_json::to_vec_pretty(&trace).unwrap()).unwrap();
        }
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

    fn fork_broker(directory: &std::sync::Arc<dyn Directory>, storage: &CellStorage, root: &std::path::Path)
        -> crate::replay_broker::Client
    {
        crate::replay_broker::Client::standalone(
            directory.clone(), CellStorage::Local { root: root.join("fork-custody") }, storage,
            std::sync::Arc::new(LocalKeyService::new(root.join("fork-custody-keys")).unwrap()),
            tokio_util::task::TaskTracker::new(),
        ).unwrap()
    }

    #[tokio::test]
    async fn a_hosted_fork_survives_restart_until_its_preparation_has_drained() {
        use rtdb_directory::{ForkPreparation, LineageChange};

        let root = tempfile::tempdir().unwrap();
        MemoryDirectory::initialize(root.path()).unwrap();
        let directory = MemoryDirectory::open(root.path()).unwrap();
        let parent = directory.create_deployment(DeploymentRecord::new("shop", "acme", vec![], 0)).await.unwrap();
        let mut child = DeploymentRecord::new("preview", "acme", vec![], 0);
        child.parent = Some(parent.id.clone());
        child.lineage.clone_from(&parent.lineage);
        let preparation = ForkPreparation { token: uuid::Uuid::new_v4(), generation: 0 };
        directory.change_lineage(&parent.lineage, LineageChange::PrepareHostedFork { preparation, task: None }).await.unwrap();
        let stage = directory.stage_fork(child.clone(), preparation).await.unwrap();
        drop(directory);

        let directory = MemoryDirectory::open(root.path()).unwrap();
        let stage = directory.deployment(&stage.id).await.unwrap().unwrap();
        super::close_abandoned_fork(directory.as_ref(), &stage).await.unwrap();
        assert_eq!(directory.deployment(&stage.id).await.unwrap().as_ref(), Some(&stage));
        directory.change_lineage(&parent.lineage, LineageChange::ReleaseFork { preparation }).await.unwrap();
        super::close_abandoned_fork(directory.as_ref(), &stage).await.unwrap();

        let closed = directory.deployment(&stage.id).await.unwrap().unwrap();
        assert_eq!(closed.lifecycle, Lifecycle::Deleting);
        assert_eq!(closed.storage_prefix(), child.storage_prefix());
        assert_eq!(closed.fork_stage, stage.fork_stage);
        assert!(closed.storage_regions.is_empty());
        assert!(directory.deployment_by_name(&child.name).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn hosted_disposal_materializes_inherited_objects_before_provider_erasure() {
        use crate::reference::{preserve_residence, simulate_fork};

        let bucket = MemoryStore::default();
        let directory = MemoryDirectory::new();
        let mut parent = DeploymentRecord::new("shop", "acme", vec![], 0);
        let inherited = DeploymentRecord::new("preview", "acme", vec![], 0);
        let shadowed = DeploymentRecord::new("shadowed-preview", "acme", vec![], 0);
        let source = ObjectKey::new(format!("{}/v06/rows", parent.storage_prefix())).unwrap();
        let inherited_copy = ObjectKey::new(format!("{}/v06/rows", inherited.storage_prefix())).unwrap();
        let shadow = ObjectKey::new(format!("{}/v06/rows", shadowed.storage_prefix())).unwrap();
        let released = ObjectKey::new(format!("{}/v06/~garbage/10-removed", parent.storage_prefix())).unwrap();
        bucket.put(&source, Bytes::from_static(b"parent")).await.unwrap();
        bucket.put(&released, Bytes::from_static(b"collected release marker")).await.unwrap();
        for child in [&inherited, &shadowed] {
            simulate_fork(&bucket, &parent.storage_prefix(), &child.storage_prefix(), None).await.unwrap();
        }
        bucket.put(&shadow, Bytes::from_static(b"shadow")).await.unwrap();
        bucket.delete(&released).await.unwrap();
        parent.lifecycle = Lifecycle::Deleting;
        let parent = directory.create_deployment(parent).await.unwrap();
        let permit = directory.begin_reference_disposal(&parent.id, parent.version).await.unwrap();
        let erasure = ResidenceErasure(ErasureAuthority::Disposing(&permit));
        let children = [inherited, shadowed];

        preserve_residence(&bucket, &erasure, &children).await.unwrap();
        assert_eq!(bucket.get(&source).await.unwrap(), Bytes::from_static(b"parent"));
        assert_eq!(bucket.get(&inherited_copy).await.unwrap(), Bytes::from_static(b"parent"));
        assert_eq!(bucket.get(&shadow).await.unwrap(), Bytes::from_static(b"shadow"));

        bucket.delete(&source).await.unwrap();
        preserve_residence(&bucket, &erasure, &children).await.unwrap();
    }

    #[tokio::test]
    async fn hosted_disposal_refuses_an_unreadable_declared_source_before_purging_the_inventory() {
        let bucket = MemoryStore::default();
        let directory = MemoryDirectory::new();
        let mut parent = DeploymentRecord::new("shop", "acme", vec![], 0);
        let child = DeploymentRecord::new("preview", "acme", vec![], 0);
        let source = ObjectKey::new(format!("{}/v06/rows", parent.storage_prefix())).unwrap();
        let unrelated = ObjectKey::new(format!("{}/v06/~local-only", parent.storage_prefix())).unwrap();
        bucket.put(&source, Bytes::from_static(b"parent")).await.unwrap();
        crate::reference::simulate_fork(&bucket, &parent.storage_prefix(), &child.storage_prefix(), None).await.unwrap();
        bucket.put(&unrelated, Bytes::from_static(b"untouched")).await.unwrap();
        bucket.delete(&source).await.unwrap();
        parent.lifecycle = Lifecycle::Deleting;
        parent.deletion_observation = Some(rtdb_directory::DeletionObservation::Fenced { leases_expire_at_ms: 0 });
        let parent = directory.create_deployment(parent).await.unwrap();
        let permit = directory.begin_reference_disposal(&parent.id, parent.version).await.unwrap();
        let erasure = ResidenceErasure(ErasureAuthority::Disposing(&permit));
        let config = rtdb_s3::S3Config::new("");

        let error = super::observe_s3_inventory(directory.as_ref(), &config, &bucket, &parent, &erasure, &[child]).await.unwrap_err();
        assert_eq!(error.code, "rtdb.storage/missing-object");
        assert!(error.to_string().contains(source.as_str()));
        assert_eq!(bucket.get(&unrelated).await.unwrap(), Bytes::from_static(b"untouched"));
        assert_eq!(directory.deployment(&parent.id).await.unwrap().unwrap().deletion_observation, parent.deletion_observation);
    }

    #[tokio::test]
    async fn deletion_can_retry_and_keeps_the_key_if_the_prefix_is_still_populated() {
        let data = tempfile::tempdir().unwrap();
        let keys = LocalKeyService::new(data.path()).unwrap();
        let mut deployment = DeploymentRecord::new("shop", "acme", vec![], 0);
        deployment.lifecycle = Lifecycle::Deleting;
        let key = keys.key(&deployment.key_id()).await.unwrap();
        let bucket = MemoryStore::default();
        let namespaces = super::storage_namespaces(&bucket, &keys).await.unwrap();
        deployment.storage_namespaces = Some(namespaces);
        let directory = MemoryDirectory::new();
        let deployment = directory.create_deployment(deployment).await.unwrap();
        let permit = directory.begin_reference_disposal(&deployment.id, deployment.version).await.unwrap();
        let erasure = ResidenceErasure(ErasureAuthority::Disposing(&permit));
        let object = ObjectKey::new(format!("{}/v06/rows", deployment.storage_prefix())).unwrap();
        bucket.put(&object, Bytes::from_static(b"data")).await.unwrap();

        let error = erase_and_sign_local_disposal(&RetainingStore(&bucket), &keys, &deployment, &erasure, &[], namespaces, 0).await.unwrap_err();
        assert_eq!(error.code, "rtdb.multi/disposal-prefix");
        assert_eq!(keys.key(&deployment.key_id()).await.unwrap(), key);

        let (proof, _) = erase_and_sign_local_disposal(&bucket, &keys, &deployment, &erasure, &[], namespaces, 0).await.unwrap();
        assert!(proof.facts.verified_empty);
        assert!(bucket.list(Some(&ObjectKey::new(deployment.storage_prefix()).unwrap())).await.unwrap().is_empty());
        assert_eq!(keys.existing(&deployment.key_id()).await.unwrap_err().code, "rtdb.multi/key-destroyed");
    }

    #[tokio::test]
    async fn a_fork_survives_a_parent_record_update_but_discards_its_prepared_state_after_a_fence() {
        let directory: std::sync::Arc<dyn Directory> = MemoryDirectory::new();
        let data = tempfile::tempdir().unwrap();
        let key_dir = data.path().join("keys");
        let keys = LocalKeyService::new(&key_dir).unwrap();
        let storage = CellStorage::Local { root: data.path().join("objects") };
        let broker = fork_broker(&directory, &storage, data.path());
        let bucket = storage.bucket(rtdb_storage::IoRegion::new()).await.unwrap();
        let mut source = DeploymentRecord::new("shop", "acme", vec![], 0);
        source.storage_namespaces = Some(super::storage_namespaces(bucket.as_ref(), &keys).await.unwrap());
        let source = directory.create_deployment(source).await.unwrap();
        let source = publish_test_fork_cut(directory.as_ref(), &bucket, source, &keys, 0).await;
        let key = keys.key(&source.key_id()).await.unwrap();
        let object = ObjectKey::new(format!("{}/v06/rows", source.storage_prefix())).unwrap();
        bucket.put(&object, Bytes::from_static(b"data")).await.unwrap();

        let mut updated = source.clone();
        updated.read_width = 2;
        let updated = directory.replace_deployment(updated, source.version).await.unwrap().applied().unwrap();
        let child = super::with_fork_access(directory.as_ref(), &storage, &keys, &source, None, |access|
            super::fork_with_tail(directory.clone(), &storage, access, &keys, &source,
                super::ForkDestination { name: "child", org: None, hosted_task: None, broker: Some(&broker) }, None, Vec::new(), 4 << 20)).await.unwrap();
        assert_eq!(keys.existing(&child.key_id()).await.unwrap(), key);
        let before = bucket.list(None).await.unwrap().into_iter().map(|item| item.key).collect::<Vec<_>>();

        let mut deleting = updated.clone();
        deleting.lifecycle = Lifecycle::Deleting;
        deleting.epoch += 1;
        directory.replace_deployment(deleting, updated.version).await.unwrap().applied().unwrap();
        let error = super::with_fork_access(directory.as_ref(), &storage, &keys, &updated, None, |access|
            super::fork_with_tail(directory.clone(), &storage, access, &keys, &updated,
                super::ForkDestination { name: "refused", org: None, hosted_task: None, broker: Some(&broker) }, None, Vec::new(), 4 << 20)).await.unwrap_err();
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
    async fn disposal_restart_copies_live_referrers_and_rejects_an_old_fork_preparation() {
        let data = tempfile::tempdir().unwrap();
        let state = data.path().join("directory");
        std::fs::create_dir(&state).unwrap();
        MemoryDirectory::initialize(&state).unwrap();
        let directory: std::sync::Arc<dyn Directory> = MemoryDirectory::open(&state).unwrap();
        let keys = LocalKeyService::new(data.path().join("keys")).unwrap();
        let storage = CellStorage::Local { root: data.path().join("objects") };
        let bucket = storage.bucket(rtdb_storage::IoRegion::new()).await.unwrap();
        let mut source = DeploymentRecord::new("source", "acme", Vec::new(), 0);
        let namespaces = super::storage_namespaces(bucket.as_ref(), &keys).await.unwrap();
        source.storage_namespaces = Some(namespaces);
        let source = directory.create_deployment(source).await.unwrap();
        let source = publish_test_fork_cut(directory.as_ref(), &bucket, source, &keys, 0).await;
        let object = ObjectKey::new(format!("{}/v06/rows", source.storage_prefix())).unwrap();
        let bytes = Bytes::from_static(b"rows");
        bucket.put(&object, bytes.clone()).await.unwrap();

        let broker = fork_broker(&directory, &storage, data.path());
        let destination = super::ForkDestination { name: "child", org: None, hosted_task: None, broker: Some(&broker) };
        let child = super::with_fork_access(directory.as_ref(), &storage, &keys, &source, None, |access|
            super::fork_with_tail(directory.clone(), &storage, access, &keys, &source, destination, None, Vec::new(), 4 << 20)).await.unwrap();
        let destination = super::ForkDestination { name: "grandchild", org: None, hosted_task: None, broker: Some(&broker) };
        let old_generation = directory.lineage_state(&child.lineage).await.unwrap().open_generation().unwrap();
        let held = super::ForkPreparation { token: uuid::Uuid::new_v4(), generation: old_generation };
        let preparation = super::ForkPublication::Local(held);
        directory.change_lineage(&child.lineage, super::LineageChange::PrepareFork {
            preparation: held, local_volume: storage.custody_namespace().unwrap(),
        }).await.unwrap();
        let mut staged = DeploymentRecord::new(destination.name, &child.org, Vec::new(), 0);
        staged.parent = Some(child.id.clone());
        staged.lineage.clone_from(&child.lineage);
        staged.storage_namespaces = child.storage_namespaces;
        let stage = directory.stage_fork(staged.clone(), held).await.unwrap();
        crate::reference::simulate_fork(bucket.as_ref(), &child.storage_prefix(), &staged.storage_prefix(), None).await.unwrap();

        directory.change_lineage(&child.lineage, super::LineageChange::ReleaseFork { preparation: held }).await.unwrap();
        let fenced = super::begin_disposal(directory.as_ref(), &source.id).await.unwrap();
        directory.begin_reference_disposal(&fenced.id, fenced.version).await.unwrap();
        drop(broker);
        drop(directory);
        let directory: std::sync::Arc<dyn Directory> = MemoryDirectory::open(&state).unwrap();
        let restarted = directory.lineage_state(&child.lineage).await.unwrap();
        assert_eq!(restarted.open_generation(), None, "a restarted directory retains the disposal cut");
        super::finish_disposal(directory.as_ref(), &storage, &keys, &source.id, 4 << 20).await.unwrap();
        assert_eq!(directory.lineage_state(&child.lineage).await.unwrap().open_generation(), Some(old_generation + 1),
            "the durable receipt reopens its disposal generation");

        let stale = super::publish_fork(directory.as_ref(), &child, &staged, preparation).await;
        assert!(matches!(stale, Err(super::ForkPublicationError::Rejected(ref error)) if error.code == "rtdb.directory/lineage-changed"),
            "a stale preparation cannot publish after disposal");
        assert!(directory.deployment_by_name(&staged.name).await.unwrap().is_none());
        super::close_fork_stage(directory.as_ref(), stage).await.unwrap();
        super::finish_disposal(directory.as_ref(), &storage, &keys, &staged.id, 4 << 20).await.unwrap();
        assert!(matches!(bucket.get(&object).await, Err(rtdb_storage::Error::MissingObject(_))));
        let copied = ObjectKey::new(format!("{}/v06/rows", child.storage_prefix())).unwrap();
        assert_eq!(bucket.get(&copied).await.unwrap(), bytes);

        let child = publish_test_fork_cut(directory.as_ref(), &bucket, child, &keys, 1).await;
        let broker = fork_broker(&directory, &storage, data.path());
        let destination = super::ForkDestination { name: "grandchild", org: None, hosted_task: None, broker: Some(&broker) };
        let grandchild = super::with_fork_access(directory.as_ref(), &storage, &keys, &child, None, |access|
            super::fork_with_tail(directory.clone(), &storage, access, &keys, &child, destination, None, Vec::new(), 4 << 20)).await.unwrap();
        let child_store = format!("{}/v06", child.storage_prefix());
        let manifest = crate::reference::Base::load(bucket.as_ref(), &format!("{}/v06", grandchild.storage_prefix())).await.unwrap();
        assert_eq!(manifest.objects.get("rows"), Some(&child_store));
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


#[cfg(test)]
mod receipt_cutover_tests {
    use super::*;
    use rtdb_directory::MemoryDirectory;

    #[tokio::test]
    async fn receipt_format_cutover_requires_a_parked_pinned_owner() {
        let directory = MemoryDirectory::new();
        directory.create_deployment(DeploymentRecord::new("unpinned", "acme", vec![], 0)).await.unwrap();
        assert_eq!(
            enable_placed_receipt_retirement(directory.as_ref(), "unpinned").await.unwrap_err().code,
            "rtdb.multi/placed-receipt-cutover",
        );
        let mut installed = DeploymentRecord::new("active", "acme", vec![], 0);
        installed.engine_pin = Some("installed-compatible-version".into());
        installed.holder = Some(MemberId("holder".into()));
        directory.create_deployment(installed).await.unwrap();
        assert_eq!(
            enable_placed_receipt_retirement(directory.as_ref(), "active").await.unwrap_err().code,
            "rtdb.multi/placed-receipt-cutover",
        );
        let mut parked = DeploymentRecord::new("parked", "acme", vec![], 0);
        parked.engine_pin = Some("installed-compatible-version".into());
        directory.create_deployment(parked).await.unwrap();
        assert!(enable_placed_receipt_retirement(directory.as_ref(), "parked").await.unwrap().placed_receipt_retirement);
        assert!(directory.deployment_by_name("parked").await.unwrap().unwrap().placed_receipt_retirement);
        assert_eq!(pin_engine(directory.as_ref(), "parked", None).await.unwrap_err().code, "rtdb.multi/placed-receipt-cutover");
    }
}
