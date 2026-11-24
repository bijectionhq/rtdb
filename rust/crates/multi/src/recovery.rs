//! Authenticated access to a cell's exact checkpoint archive.

use std::{path::Path, sync::Arc, time::Duration};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rtdb_directory::{
    CheckpointCaptureRetirement, CheckpointStageIntent, CheckpointSuffixCustody, DeploymentRecord, Directory, DisposalReceipt, HostedTask,
    ServingInventory,
};
use rtdb_storage::sealing::DeliveryIo;
use rtdb_types::{Anomaly, AnomalyCategory, MemoryBudget, Result};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};
use tokio_rustls::{TlsAcceptor, rustls::ServerConfig, server::TlsStream};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    admin::{flush_checkpoint, pin_checkpoint_suffix},
    archive::{self, CapturePoint},
    control::ControlKey,
    deployment::CellStorage,
    family_custody::{FAMILY_CUSTODY_GRANT_BYTES, endorse_family_custody, grant_id, original_family_custody},
    install::Token,
    keys::KeyService,
};

const FRAME_BYTES: u64 = 2048;
const MAX_REPLY_BYTES: usize = 4 << 20;
const SOCKET_DEADLINE: Duration = Duration::from_secs(15);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    token: String,
    name: String,
    authority: Uuid,
    capture: Uuid,
    action: Action,
}

#[derive(Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
enum Action {
    CaptureCheckpoint,
    ProtectedRootCustodyIdentity {
        manifest_sha256: String,
    },
    EndorseProtectedRootCustody {
        manifest_sha256: String,
        grant: String,
    },
    RetainServingReplay {
        manifest_sha256: String,
    },
    CaptureCheckpointSuffix {
        lease: Uuid,
        basis: rtdb_types::TransactionKey,
        expires_at_ms: i64,
    },
    VerifyCheckpointCapture {
        manifest_sha256: String,
        observation: Uuid,
    },
    DiscardCheckpointCapture {
        manifest_sha256: Option<String>,
    },
    PrepareCheckpointCaptureDiscard {
        manifest_sha256: Option<String>,
        deadline_ms: Option<u64>,
    },
    CheckpointCaptureDiscardStatus {
        manifest_sha256: Option<String>,
    },
    ScheduleCheckpointCaptureRevocation {
        manifest_sha256: String,
        due_at_ms: i64,
    },
    CommitCheckpointCaptureRevocation {
        manifest_sha256: String,
    },
    SettleCheckpointCaptureDiscard {
        manifest_sha256: Option<String>,
    },
    RecoverCheckpoint {
        manifest_sha256: String,
    },
    TransferCheckpoint {
        source_manifest_sha256: String,
        destination: Uuid,
        stage_operation: Uuid,
    },
    StageCheckpointTransfer {
        source_manifest_sha256: String,
        destination: Uuid,
        stage_operation: Uuid,
    },
    SealCheckpointTransferSource {
        destination: Uuid,
        stage_operation: Uuid,
        staged_manifest_sha256: String,
    },
    StageCheckpoint {
        source_manifest_sha256: String,
        destination: Uuid,
        stage_operation: Uuid,
    },
    DiscardCheckpointStage {
        source_manifest_sha256: String,
        destination: Uuid,
        stage_operation: Uuid,
    },
    OpenCheckpointStage {
        destination: Uuid,
        stage_operation: Uuid,
        staged_manifest_sha256: String,
    },
    OpenCheckpointStageOwnedEventFiles {
        destination: Uuid,
        stage_operation: Uuid,
        staged_manifest_sha256: String,
    },
    ActivateCheckpointTransferTarget {
        destination: Uuid,
        stage_operation: Uuid,
        staged_manifest_sha256: String,
    },
    SeedCheckpointTransferTarget {
        destination: Uuid,
        stage_operation: Uuid,
        staged_manifest_sha256: String,
    },
    VerifyCheckpointTransferTarget {
        destination: Uuid,
        stage_operation: Uuid,
        staged_manifest_sha256: String,
    },
    InspectCheckpointStageOwnedRelations {
        destination: Uuid,
        stage_operation: Uuid,
        staged_manifest_sha256: String,
    },
}

#[derive(Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
enum Reply {
    ProtectedRootCustodyIdentity {
        history: [u8; 32],
        custody: [u8; 32],
    },
    ProtectedRootCustodyEndorsed {
        grant_sha256: String,
        endorsement: String,
    },
    Captured {
        name: String,
        authority: Uuid,
        point: CapturePoint,
        manifest_sha256: String,
    },
    ServingReplayRetained {
        name: String,
        authority: Uuid,
        point: CapturePoint,
        manifest_sha256: String,
    },
    SuffixCaptured {
        name: String,
        authority: Uuid,
        capture: Uuid,
        lease: Uuid,
        basis: rtdb_types::TransactionKey,
        archive_sha256: String,
    },
    Verified {
        name: String,
        authority: Uuid,
        point: CapturePoint,
        manifest_sha256: String,
        observation: Uuid,
    },
    CaptureDiscardPrepared {
        name: String,
        authority: Uuid,
        capture: Uuid,
        manifest_sha256: Option<String>,
        authorized_at_ms: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        scheduled_revocation_at_ms: Option<i64>,
        key_withdrawn_at_ms: Option<i64>,
    },
    CaptureRevocationScheduled {
        name: String,
        authority: Uuid,
        capture: Uuid,
        manifest_sha256: String,
        authorized_at_ms: i64,
        due_at_ms: i64,
    },
    CaptureRevocationCommitted {
        name: String,
        authority: Uuid,
        capture: Uuid,
        manifest_sha256: String,
        authorized_at_ms: i64,
        operation_revoked_at_ms: i64,
    },
    CaptureDiscardDeadlineUnsupported,
    CaptureDiscarded {
        name: String,
        authority: Uuid,
        capture: Uuid,
        manifest_sha256: Option<String>,
        archive_disposal: CaptureDisposal,
    },
    Recovered {
        name: String,
        authority: Uuid,
        capture: Uuid,
        manifest_sha256: String,
        epoch: u64,
        completed_at_ms: i64,
    },
    Staged {
        name: String,
        authority: Uuid,
        capture: Uuid,
        source_manifest_sha256: String,
        database: String,
        destination: Uuid,
        stage_operation: Uuid,
        staged_manifest_sha256: String,
    },
    StageDiscarded {
        name: String,
        authority: Uuid,
        capture: Uuid,
        source_manifest_sha256: String,
        destination: Uuid,
        stage_operation: Uuid,
        disposal: DisposalReceipt,
    },
    Transferred {
        name: String,
        authority: Uuid,
        capture: Uuid,
        source_manifest_sha256: String,
        database: String,
        destination: Uuid,
        stage_operation: Uuid,
        staged_manifest_sha256: String,
        replica_through: i64,
        source_through: i64,
        basis: rtdb_types::TransactionKey,
        lifetime_floors_sha256: [u8; 32],
        recovery_manifest_sha256: [u8; 32],
    },
    TransferTargetActivated {
        activation: rtdb_directory::CheckpointTransferActivation,
    },
    TransferSourceSealed {
        terminal: rtdb_directory::CheckpointTransferTerminal,
    },
    TransferTargetSeeded {
        replica: rtdb_directory::CheckpointTransferReplica,
    },
    TransferTargetVerified {
        bootstrap: rtdb_directory::CheckpointTransferBootstrap,
    },
    StageOwnedRelations {
        name: String,
        authority: Uuid,
        capture: Uuid,
        #[serde(flatten)]
        inventory: archive::StageOwnedRelations,
    },
    Refused,
    Deferred,
}

#[derive(Serialize)]
struct CaptureDisposal {
    authorized_at_ms: i64,
    key_withdrawn_at_ms: i64,
    observed_empty_at_ms: i64,
    inventory: ServingInventory,
}

pub struct RecoveryService {
    token: Token,
    org: String,
    storage: CellStorage,
    archive: CellStorage,
    keys: Arc<dyn KeyService>,
    broker: Option<crate::replay_broker::Client>,
    hosted_task: Option<HostedTask>,
    control: ControlKey,
    memory: Arc<MemoryBudget>,
}

impl RecoveryService {
    pub fn from_file(
        token: &Path,
        org: String,
        storage: CellStorage,
        archive: CellStorage,
        keys: Arc<dyn KeyService>,
        broker: Option<crate::replay_broker::Client>,
        hosted_task: Option<HostedTask>,
        control: ControlKey,
    ) -> Result<Self> {
        Ok(Self {
            token: Token::new(&Token::read(token)?)?,
            org,
            storage,
            archive,
            keys,
            broker,
            hosted_task,
            control,
            memory: MemoryBudget::new(rtdb_node::NodeOptions::default().execution_memory_bytes),
        })
    }

    async fn authorize(
        &self,
        directory: &dyn Directory,
        request: &Request,
    ) -> Result<Option<DeploymentRecord>> {
        if !self.token.accepts(&request.token)
            || request.capture.is_nil()
            || request.authority.is_nil()
        {
            return Ok(None);
        }
        // Retain exact receipt access after the named route moves or closes.
        // New captures and repairs still have their own lifecycle guards.
        let Some(installation) = directory.deployment(&request.authority.to_string()).await? else {
            return Ok(None);
        };
        if installation.id != request.authority.to_string()
            || installation.name != request.name
            || installation.org != self.org
            || installation.parent.is_some()
        {
            return Ok(None);
        }
        Ok(Some(installation))
    }

    async fn dispatch(&self, directory: Arc<dyn Directory>, request: Request) -> Result<Reply> {
        let Some(installation) = self.authorize(directory.as_ref(), &request).await? else {
            return Ok(Reply::Refused);
        };
        let discard = matches!(request.action, Action::DiscardCheckpointStage { .. });
        let transfer_only = matches!(request.action, Action::StageCheckpointTransfer { .. });
        match request.action {
            Action::ProtectedRootCustodyIdentity { manifest_sha256 } => {
                self.protected_root_capture(&installation, request.authority, request.capture, &manifest_sha256).await?;
                let (history, custody) = original_family_custody(&installation, request.authority)?;
                Ok(Reply::ProtectedRootCustodyIdentity { history, custody })
            }
            Action::EndorseProtectedRootCustody { manifest_sha256, grant } => {
                let body = decode_family_custody_grant(&grant)?;
                let (secret, point) = self.protected_root_capture(&installation, request.authority, request.capture, &manifest_sha256).await?;
                let endorsement = endorse_family_custody(secret, &installation, request.authority, point.checkpoint, &body)?;
                Ok(Reply::ProtectedRootCustodyEndorsed {
                    grant_sha256: lower_hex(&grant_id(&body)),
                    endorsement: lower_hex(&endorsement),
                })
            }
            Action::CaptureCheckpoint => {
                let retained = archive::capture_receipt(
                    &installation,
                    &self.archive,
                    request.authority,
                    request.capture,
                )?;
                let (point, manifest_sha256) = match retained {
                    Some(receipt) => receipt,
                    None => {
                        archive::require_capture_open(&installation, request.capture)?;
                        let flushed = if installation.checkpoint_captures.contains_key(&request.capture) {
                            None
                        } else if installation.holder.is_none() && installation.checkpoint.is_some() {
                            Some(installation.clone())
                        } else {
                            Some(
                                flush_checkpoint(directory.as_ref(), &installation, &self.control)
                                    .await?,
                            )
                        };
                        let capture = archive::capture(
                            directory,
                            &self.storage,
                            &self.archive,
                            self.keys.as_ref(),
                            request.authority,
                            request.capture,
                            self.hosted_task.clone(),
                            flushed.as_ref(),
                        )
                        .await?;
                        let digest = capture.manifest_sha256()?;
                        (capture.point, digest)
                    }
                };
                Ok(Reply::Captured {
                    name: installation.name,
                    authority: request.authority,
                    point,
                    manifest_sha256,
                })
            }
            Action::RetainServingReplay { manifest_sha256 } => {
                let point = archive::retain_serving_replay(
                    directory.as_ref(), &self.storage, &self.archive, self.keys.as_ref(),
                    &installation, request.capture, &manifest_sha256, self.hosted_task.as_ref(),
                ).await?;
                Ok(Reply::ServingReplayRetained {
                    name: installation.name,
                    authority: request.authority,
                    point,
                    manifest_sha256,
                })
            }
            Action::CaptureCheckpointSuffix { lease, basis, expires_at_ms } => {
                if lease.is_nil() { return Ok(Reply::Refused); }
                if !installation.checkpoint_suffix_pins.contains_key(&lease) {
                    pin_checkpoint_suffix(directory.as_ref(), &installation, &self.control,
                        request.capture, lease, basis, expires_at_ms).await?;
                }
                let retained = directory.deployment(&installation.id).await?
                    .and_then(|current| current.checkpoint_suffix_pins.get(&lease).cloned())
                    .filter(|pin| pin.custody == CheckpointSuffixCustody::Capture
                        && pin.capture == request.capture && pin.basis == basis
                        && pin.expires_at_ms == expires_at_ms)
                    .ok_or_else(|| Anomaly::new(AnomalyCategory::Unavailable, "rtdb.multi/checkpoint-suffix-unavailable",
                        "The checkpoint suffix pin changed before archive admission"))?;
                let archive_sha256 = archive::capture_replica_suffix(
                    directory, &self.archive, self.keys.as_ref(), self.broker.as_ref(), self.hosted_task.as_ref(),
                    request.authority, lease, retained, &self.memory,
                ).await?;
                Ok(Reply::SuffixCaptured {
                    name: installation.name, authority: request.authority,
                    capture: request.capture, lease, basis, archive_sha256,
                })
            }
            Action::VerifyCheckpointCapture {
                manifest_sha256,
                observation,
            } => {
                if !valid_manifest(&manifest_sha256) || observation.is_nil() {
                    return Ok(Reply::Refused);
                }
                let point = archive::verify_checkpoint_capture(
                    directory.as_ref(),
                    &installation,
                    &self.archive,
                    self.keys.as_ref(),
                    request.authority,
                    request.capture,
                    &manifest_sha256,
                    self.hosted_task.as_ref(),
                )
                .await?;
                Ok(Reply::Verified {
                    name: installation.name,
                    authority: request.authority,
                    point,
                    manifest_sha256,
                    observation,
                })
            }
            Action::DiscardCheckpointCapture { .. }
            | Action::PrepareCheckpointCaptureDiscard { .. }
            | Action::CheckpointCaptureDiscardStatus { .. }
            | Action::ScheduleCheckpointCaptureRevocation { .. }
            | Action::CommitCheckpointCaptureRevocation { .. }
            | Action::SettleCheckpointCaptureDiscard { .. } => unreachable!("dispatched separately"),
            Action::RecoverCheckpoint { manifest_sha256 } => {
                if !valid_manifest(&manifest_sha256) {
                    return Ok(Reply::Refused);
                }
                let recovery = archive::restore_checkpoint(
                    directory.as_ref(),
                    &self.storage,
                    &self.archive,
                    self.keys.as_ref(),
                    request.authority,
                    request.capture,
                    4 << 20,
                    Some(&manifest_sha256),
                    self.hosted_task.as_ref(),
                )
                .await?;
                let completed_at_ms = recovery.completed_at_ms.ok_or_else(|| {
                    Anomaly::incorrect(
                        "rtdb.multi/recovery-endpoint",
                        "Recovery has no completion receipt",
                    )
                })?;
                Ok(Reply::Recovered {
                    name: installation.name,
                    authority: request.authority,
                    capture: recovery.operation,
                    manifest_sha256: recovery.manifest_sha256,
                    epoch: recovery.epoch,
                    completed_at_ms,
                })
            }
            Action::StageCheckpoint {
                source_manifest_sha256,
                destination,
                stage_operation,
            }
            | Action::StageCheckpointTransfer {
                source_manifest_sha256,
                destination,
                stage_operation,
            }
            | Action::DiscardCheckpointStage {
                source_manifest_sha256,
                destination,
                stage_operation,
            } => {
                if !valid_manifest(&source_manifest_sha256)
                    || destination.is_nil()
                    || destination == request.authority
                    || stage_operation.is_nil()
                    || installation.checkpoint_stage.is_some()
                {
                    return Ok(Reply::Refused);
                }
                let database = format!("checkpoint-stage-{destination}");
                let intent = CheckpointStageIntent {
                    operation: stage_operation,
                    source: request.authority,
                    capture: request.capture,
                    source_manifest_sha256: source_manifest_sha256.clone(),
                    destination,
                    name: database.clone(),
                    org: self.org.clone(),
                };
                if discard {
                    let disposal = archive::discard_checkpoint_stage(
                        directory.as_ref(),
                        &installation,
                        &self.archive,
                        &self.storage,
                        self.keys.as_ref(),
                        intent,
                    )
                    .await?;
                    return Ok(match disposal {
                        Some(disposal) => Reply::StageDiscarded {
                            name: installation.name,
                            authority: request.authority,
                            capture: request.capture,
                            source_manifest_sha256,
                            destination,
                            stage_operation,
                            disposal,
                        },
                        None => Reply::Deferred,
                    });
                }
                let stage = if transfer_only {
                    archive::stage_checkpoint_transfer(
                        directory, &self.archive, &self.storage, self.keys.as_ref(), intent, self.hosted_task.clone(),
                    ).await?
                } else {
                    archive::stage_checkpoint(
                        directory, &self.archive, &self.storage, self.keys.as_ref(), intent, self.hosted_task.clone(),
                    ).await?
                };
                let staged_manifest_sha256 = stage.staged_manifest_sha256.ok_or_else(|| {
                    Anomaly::incorrect(
                        "rtdb.multi/recovery-endpoint",
                        "Checkpoint stage has no inventory receipt",
                    )
                })?;
                Ok(Reply::Staged {
                    name: installation.name,
                    authority: request.authority,
                    capture: request.capture,
                    source_manifest_sha256,
                    database,
                    destination,
                    stage_operation,
                    staged_manifest_sha256,
                })
            }
            Action::TransferCheckpoint { source_manifest_sha256, destination, stage_operation } => {
                if installation.checkpoint_stage.is_some() || destination.is_nil()
                    || destination == request.authority || stage_operation.is_nil()
                    || !valid_manifest(&source_manifest_sha256)
                {
                    return Ok(Reply::Refused);
                }
                let database = format!("checkpoint-stage-{destination}");
                let stage = archive::stage_checkpoint_transfer(
                    Arc::clone(&directory), &self.archive, &self.storage, self.keys.as_ref(),
                    CheckpointStageIntent {
                        operation: stage_operation,
                        source: request.authority,
                        capture: request.capture,
                        source_manifest_sha256: source_manifest_sha256.clone(),
                        destination,
                        name: database.clone(),
                        org: self.org.clone(),
                    },
                    self.hosted_task.clone(),
                ).await?;
                let staged_manifest_sha256 = stage.staged_manifest_sha256
                    .filter(|digest| valid_manifest(digest))
                    .ok_or_else(|| Anomaly::incorrect(
                        "rtdb.multi/recovery-endpoint", "The private transfer stage has no exact inventory",
                    ))?;
                let selector = archive::StageSelector {
                    destination, operation: stage_operation,
                    manifest_sha256: staged_manifest_sha256.clone(),
                    source: Some((request.authority, request.capture, self.org.clone())),
                    transfer_only: true,
                };
                let sealed = Box::pin(archive::issue_checkpoint_transfer(
                    Arc::clone(&directory), &self.storage, &self.archive, self.keys.as_ref(),
                    selector.clone(), self.broker.as_ref(), self.hosted_task.clone(),
                )).await?;
                let activation = Box::pin(archive::activate_checkpoint_transfer_target(
                    directory, &self.storage, self.keys.as_ref(), selector, self.broker.as_ref(), self.hosted_task.clone(),
                )).await?;
                let terminal = activation.replica.bootstrap.terminal;
                if terminal != sealed {
                    return Err(Anomaly::incorrect(
                        "rtdb.multi/recovery-endpoint", "The target activation has another terminal receipt",
                    ));
                }
                Ok(Reply::Transferred {
                    name: installation.name,
                    authority: request.authority,
                    capture: request.capture,
                    source_manifest_sha256,
                    database,
                    destination,
                    stage_operation,
                    staged_manifest_sha256,
                    replica_through: terminal.replica_through,
                    source_through: terminal.source_through,
                    basis: terminal.basis,
                    lifetime_floors_sha256: terminal.lifetime_floors_sha256,
                    recovery_manifest_sha256: activation.recovery_manifest_sha256,
                })
            }
            Action::SealCheckpointTransferSource {
                destination, stage_operation, staged_manifest_sha256,
            } => {
                if installation.checkpoint_stage.is_some() || destination.is_nil()
                    || stage_operation.is_nil() || !valid_manifest(&staged_manifest_sha256)
                {
                    return Ok(Reply::Refused);
                }
                let selector = archive::StageSelector {
                    destination,
                    operation: stage_operation,
                    manifest_sha256: staged_manifest_sha256,
                    source: Some((request.authority, request.capture, self.org.clone())),
                    transfer_only: true,
                };
                let terminal = Box::pin(archive::issue_checkpoint_transfer(
                    directory, &self.storage, &self.archive, self.keys.as_ref(), selector, self.broker.as_ref(), self.hosted_task.clone(),
                )).await?;
                Ok(Reply::TransferSourceSealed { terminal })
            },
            Action::ActivateCheckpointTransferTarget {
                destination, stage_operation, staged_manifest_sha256,
            } => {
                if installation.checkpoint_stage.is_some() || destination.is_nil()
                    || stage_operation.is_nil() || !valid_manifest(&staged_manifest_sha256)
                {
                    return Ok(Reply::Refused);
                }
                let selector = archive::StageSelector {
                    destination,
                    operation: stage_operation,
                    manifest_sha256: staged_manifest_sha256,
                    source: Some((request.authority, request.capture, self.org.clone())),
                    transfer_only: true,
                };
                let activation = Box::pin(archive::activate_checkpoint_transfer_target(
                    directory, &self.storage, self.keys.as_ref(), selector, self.broker.as_ref(), self.hosted_task.clone(),
                )).await?;
                Ok(Reply::TransferTargetActivated { activation })
            },
            Action::SeedCheckpointTransferTarget {
                destination, stage_operation, staged_manifest_sha256,
            } => {
                if installation.checkpoint_stage.is_some() || destination.is_nil()
                    || stage_operation.is_nil() || !valid_manifest(&staged_manifest_sha256)
                {
                    return Ok(Reply::Refused);
                }
                let selector = archive::StageSelector {
                    destination,
                    operation: stage_operation,
                    manifest_sha256: staged_manifest_sha256,
                    source: Some((request.authority, request.capture, self.org.clone())),
                    transfer_only: true,
                };
                let replica = Box::pin(archive::seed_checkpoint_transfer_target(
                    directory, &self.storage, self.keys.as_ref(), selector, self.broker.as_ref(), self.hosted_task.clone(),
                )).await?;
                Ok(Reply::TransferTargetSeeded { replica })
            },
            Action::VerifyCheckpointTransferTarget {
                destination, stage_operation, staged_manifest_sha256,
            } => {
                if installation.checkpoint_stage.is_some() || destination.is_nil()
                    || stage_operation.is_nil() || !valid_manifest(&staged_manifest_sha256)
                {
                    return Ok(Reply::Refused);
                }
                let selector = archive::StageSelector {
                    destination,
                    operation: stage_operation,
                    manifest_sha256: staged_manifest_sha256,
                    source: Some((request.authority, request.capture, self.org.clone())),
                    transfer_only: true,
                };
                let bootstrap = Box::pin(archive::verify_checkpoint_transfer_target(
                    directory, &self.storage, self.keys.as_ref(), selector, self.broker.as_ref(), self.hosted_task.clone(),
                )).await?;
                Ok(Reply::TransferTargetVerified { bootstrap })
            },
            Action::InspectCheckpointStageOwnedRelations {
                destination,
                stage_operation,
                staged_manifest_sha256,
            } => {
                if installation.checkpoint_stage.is_some()
                    || destination.is_nil()
                    || stage_operation.is_nil()
                    || !valid_manifest(&staged_manifest_sha256)
                {
                    return Ok(Reply::Refused);
                }
                let selector = archive::StageSelector {
                    destination,
                    operation: stage_operation,
                    manifest_sha256: staged_manifest_sha256,
                    source: Some((request.authority, request.capture, self.org.clone())),
                    transfer_only: false,
                };
                let inventory = archive::inspect_staged_owned_relations(
                    directory,
                    &self.storage,
                    self.keys.as_ref(),
                    selector,
                    self.hosted_task.clone(),
                ).await?;
                Ok(Reply::StageOwnedRelations {
                    name: installation.name,
                    authority: request.authority,
                    capture: request.capture,
                    inventory,
                })
            },
            Action::OpenCheckpointStage { .. } | Action::OpenCheckpointStageOwnedEventFiles { .. } => Ok(Reply::Refused),
        }
    }

    async fn protected_root_capture(
        &self, record: &DeploymentRecord, authority: Uuid, capture: Uuid, manifest: &str,
    ) -> Result<(&crate::capability::CellSecret, CapturePoint)> {
        let unavailable = rtdb_types::OwnedWorld::unavailable;
        let secret = self.control.0.as_deref().filter(|_| self.hosted_task.is_some()).ok_or_else(unavailable)?;
        let namespaces = record.storage_namespaces.as_ref().ok_or_else(unavailable)?;
        if record.writer_pool().is_none() || !valid_manifest(manifest) {
            return Err(unavailable());
        }
        archive::require_capture_open(record, capture)?;
        let (point, accepted) = archive::capture_receipt(record, &self.archive, authority, capture)?.ok_or_else(unavailable)?;
        if accepted != manifest || point.incarnation != record.incarnation || point.log_epoch != record.log_epoch
            || point.checkpoint.system_time <= 0 || self.keys.namespace_id().await? != namespaces.keys
        {
            return Err(unavailable());
        }
        Ok((secret, point))
    }

    async fn dispatch_capture_discard(&self, directory: Arc<dyn Directory>, request: Request) -> Result<Reply> {
        let Some(installation) = self.authorize(directory.as_ref(), &request).await? else {
            return Ok(Reply::Refused);
        };
        let capture_discard = match &request.action {
            Action::DiscardCheckpointCapture { manifest_sha256 }
            | Action::PrepareCheckpointCaptureDiscard { manifest_sha256, .. }
            | Action::CheckpointCaptureDiscardStatus { manifest_sha256 }
            | Action::SettleCheckpointCaptureDiscard { manifest_sha256 } => Some(manifest_sha256.as_deref()),
            Action::ScheduleCheckpointCaptureRevocation { manifest_sha256, .. }
            | Action::CommitCheckpointCaptureRevocation { manifest_sha256 } => Some(Some(manifest_sha256.as_str())),
            _ => None,
        };
        if let Some(manifest) = capture_discard
            && (manifest.is_some_and(|value| !valid_manifest(value)) || installation.checkpoint_stage.is_some())
        {
            return Ok(Reply::Refused);
        }
        match request.action {
            Action::DiscardCheckpointCapture { manifest_sha256 } => {
                let release = archive::retire_capture(
                    directory.as_ref(), &installation, &self.storage, &self.archive,
                    self.keys.as_ref(), request.capture, manifest_sha256.as_deref(),
                ).await?;
                Ok(self.capture_discard_reply(installation.name, request.authority, request.capture, release))
            }
            Action::PrepareCheckpointCaptureDiscard { manifest_sha256, deadline_ms } => {
                if deadline_ms.is_some() {
                    return Ok(Reply::CaptureDiscardDeadlineUnsupported);
                }
                let release = archive::prepare_capture_retirement(
                    directory.as_ref(), &installation, &self.archive, request.capture, manifest_sha256.as_deref(),
                ).await?;
                Ok(self.capture_discard_reply(installation.name, request.authority, request.capture, release))
            }
            Action::CheckpointCaptureDiscardStatus { manifest_sha256 } => {
                let release = archive::capture_retirement_receipt(
                    &installation, &self.archive, request.capture, manifest_sha256.as_deref(),
                )?;
                Ok(self.capture_discard_reply(installation.name, request.authority, request.capture, release))
            }
            Action::ScheduleCheckpointCaptureRevocation { manifest_sha256, due_at_ms } => {
                let release = archive::schedule_capture_revocation(
                    directory.as_ref(), &installation, &self.archive, self.keys.as_ref(),
                    request.capture, &manifest_sha256, due_at_ms,
                ).await?;
                Ok(Reply::CaptureRevocationScheduled {
                    name: installation.name,
                    authority: request.authority,
                    capture: request.capture,
                    manifest_sha256,
                    authorized_at_ms: release.authorized_at_ms,
                    due_at_ms,
                })
            }
            Action::CommitCheckpointCaptureRevocation { manifest_sha256 } => {
                archive::capture_retirement_receipt(
                    &installation, &self.archive, request.capture, Some(&manifest_sha256),
                )?;
                let release = archive::commit_capture_revocation(
                    directory.as_ref(), &self.storage, self.keys.as_ref(), &installation.id, request.capture,
                ).await?;
                let operation_revoked_at_ms = release.operation_revoked_at_ms.ok_or_else(|| {
                    Anomaly::fault("rtdb.multi/archive-vault", "The operation-only revocation has no receipt")
                })?;
                Ok(Reply::CaptureRevocationCommitted {
                    name: installation.name,
                    authority: request.authority,
                    capture: request.capture,
                    manifest_sha256,
                    authorized_at_ms: release.authorized_at_ms,
                    operation_revoked_at_ms,
                })
            }
            Action::SettleCheckpointCaptureDiscard { manifest_sha256 } => {
                archive::capture_retirement_receipt(
                    &installation, &self.archive, request.capture, manifest_sha256.as_deref(),
                )?;
                let release = archive::finish_capture_retirement(
                    directory.as_ref(), &self.storage, self.keys.as_ref(), &installation.id, request.capture,
                ).await?;
                Ok(self.capture_discard_reply(installation.name, request.authority, request.capture, release))
            }
            _ => unreachable!("only capture retirement actions are routed here"),
        }
    }

    fn capture_discard_reply(
        &self, name: String, authority: Uuid, capture: Uuid, release: CheckpointCaptureRetirement,
    ) -> Reply {
        match (release.key_withdrawn_at_ms, release.observed_empty_at_ms) {
            (Some(key_withdrawn_at_ms), Some(observed_empty_at_ms)) => Reply::CaptureDiscarded {
                name,
                authority,
                capture,
                manifest_sha256: release.manifest_sha256,
                archive_disposal: CaptureDisposal {
                    authorized_at_ms: release.authorized_at_ms,
                    key_withdrawn_at_ms,
                    observed_empty_at_ms,
                    inventory: match &self.archive {
                        CellStorage::Local { .. } => ServingInventory::LocalDirectory,
                        CellStorage::S3 { .. } => ServingInventory::S3VersionsAndUploads,
                    },
                },
            },
            _ => Reply::CaptureDiscardPrepared {
                name, authority, capture, manifest_sha256: release.manifest_sha256,
                authorized_at_ms: release.authorized_at_ms, key_withdrawn_at_ms: release.key_withdrawn_at_ms,
                scheduled_revocation_at_ms: release.scheduled_revocation_at_ms,
            },
        }
    }

    async fn handle(&self, directory: Arc<dyn Directory>, request: Request) -> Reply {
        let result = match request.action {
            Action::DiscardCheckpointCapture { .. }
            | Action::PrepareCheckpointCaptureDiscard { .. }
            | Action::CheckpointCaptureDiscardStatus { .. }
            | Action::ScheduleCheckpointCaptureRevocation { .. }
            | Action::CommitCheckpointCaptureRevocation { .. }
            | Action::SettleCheckpointCaptureDiscard { .. } => self.dispatch_capture_discard(directory, request).await,
            _ => Box::pin(self.dispatch(directory, request)).await,
        };
        match result {
            Ok(reply) => reply,
            Err(error)
                if matches!(
                    error.category,
                    AnomalyCategory::Incorrect
                        | AnomalyCategory::Forbidden
                        | AnomalyCategory::Conflict
                ) =>
            {
                Reply::Refused
            }
            Err(_) => Reply::Deferred,
        }
    }

    async fn open_stage(
        &self,
        directory: Arc<dyn Directory>,
        request: Request,
        mut socket: TlsStream<DeliveryIo<TcpStream>>,
        stop: CancellationToken,
    ) -> Result<()> {
        let authorized = self.authorize(directory.as_ref(), &request).await?;
        let (destination, stage_operation, staged_manifest_sha256, owned_event_files) = match request.action {
            Action::OpenCheckpointStage { destination, stage_operation, staged_manifest_sha256 } => {
                (destination, stage_operation, staged_manifest_sha256, false)
            }
            Action::OpenCheckpointStageOwnedEventFiles { destination, stage_operation, staged_manifest_sha256 } => {
                (destination, stage_operation, staged_manifest_sha256, true)
            }
            _ => unreachable!("only stage-reader requests are routed here"),
        };
        if authorized.is_none_or(|source| source.checkpoint_stage.is_some())
            || destination.is_nil()
            || stage_operation.is_nil()
            || !valid_manifest(&staged_manifest_sha256)
        {
            let _ =
                tokio::time::timeout(SOCKET_DEADLINE, write_reply(&mut socket, &Reply::Refused))
                    .await;
            return Ok(());
        }
        let selector = archive::StageSelector {
            destination,
            operation: stage_operation,
            manifest_sha256: staged_manifest_sha256,
            source: Some((request.authority, request.capture, self.org.clone())),
            transfer_only: false,
        };
        let mut reader = std::pin::pin!(async {
            if owned_event_files {
                archive::stream_staged_owned_event_files(
                    directory, &self.storage, self.keys.as_ref(), selector,
                    self.hosted_task.clone(), stop.clone(), socket,
                ).await
            } else {
                archive::tunnel_staged_checkpoint(
                    directory, &self.storage, self.keys.as_ref(), selector,
                    self.hosted_task.clone(), stop.clone(), socket,
                ).await
            }
        });
        tokio::select! {
            result = &mut reader => result,
            _ = tokio::time::sleep(Duration::from_secs(29 * 60)) => {
                stop.cancel();
                reader.await
            },
        }
    }
}

fn decode_family_custody_grant(grant: &str) -> Result<[u8; FAMILY_CUSTODY_GRANT_BYTES]> {
    let invalid = || Anomaly::incorrect("rtdb.multi/family-custody-grant", "Invalid RFC1 request encoding");
    if grant.len() != FAMILY_CUSTODY_GRANT_BYTES * 2 { return Err(invalid()); }
    let digit = |byte| match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(invalid()),
    };
    let mut body = [0; FAMILY_CUSTODY_GRANT_BYTES];
    for (byte, encoded) in body.iter_mut().zip(grant.as_bytes().chunks_exact(2)) {
        *byte = digit(encoded[0])? << 4 | digit(encoded[1])?;
    }
    Ok(body)
}

fn lower_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn valid_manifest(digest: &str) -> bool {
    digest.len() == 43
        && URL_SAFE_NO_PAD
            .decode(digest)
            .is_ok_and(|bytes| bytes.len() == 32)
}

pub async fn serve(
    listener: TcpListener,
    tls: Arc<ServerConfig>,
    directory: Arc<dyn Directory>,
    service: Arc<RecoveryService>,
    cancellation: CancellationToken,
) -> Result<()> {
    let connections = Arc::new(Semaphore::new(16));
    // Bound concurrent custody operations, each of which can span many reads.
    let operations = Arc::new(Semaphore::new(1));
    let readers = cancellation.child_token();
    let mut requests = tokio::task::JoinSet::new();
    let acceptor = TlsAcceptor::from(tls);
    let result = loop {
        let accepted = tokio::select! {
            _ = cancellation.cancelled() => break Ok(()),
            _ = requests.join_next(), if !requests.is_empty() => continue,
            accepted = listener.accept() => accepted,
        };
        let (socket, _) = match accepted {
            Ok(connection) => connection,
            Err(_) => {
                break Err(Anomaly::new(
                    AnomalyCategory::Unavailable,
                    "rtdb.multi/recovery-endpoint",
                    "Cannot accept a recovery connection",
                ));
            }
        };
        let Ok(connection) = connections.clone().try_acquire_owned() else {
            continue;
        };
        let (service, directory, acceptor, operations) = (
            service.clone(),
            directory.clone(),
            acceptor.clone(),
            operations.clone(),
        );
        let stop = readers.child_token();
        requests.spawn(async move {
            let Ok(Ok((mut socket, line))) =
                tokio::time::timeout(SOCKET_DEADLINE, read_request(socket, acceptor)).await
            else {
                return;
            };
            let _connection = connection;
            let reply = if let Ok(_operation) = operations.try_acquire() {
                let Ok(request) = serde_json::from_str::<Request>(&line) else {
                    let _ = tokio::time::timeout(
                        SOCKET_DEADLINE,
                        write_reply(&mut socket, &Reply::Refused),
                    )
                    .await;
                    return;
                };
                if matches!(request.action, Action::OpenCheckpointStage { .. } | Action::OpenCheckpointStageOwnedEventFiles { .. }) {
                    let _ = service.open_stage(directory, request, socket, stop).await;
                    return;
                }
                // Finishing the operation releases its durable storage registration
                // even when the client disconnects before receiving the receipt.
                service.handle(directory, request).await
            } else {
                Reply::Deferred
            };
            let _ = tokio::time::timeout(SOCKET_DEADLINE, write_reply(&mut socket, &reply)).await;
        });
    };
    // Close the listener before joining. An accept failure must not abort
    // a running capture or restore that holds a durable admission.
    drop(listener);
    readers.cancel();
    while requests.join_next().await.is_some() {}
    result
}

async fn read_request(
    socket: TcpStream,
    acceptor: TlsAcceptor,
) -> std::io::Result<(TlsStream<DeliveryIo<TcpStream>>, String)> {
    let mut socket = acceptor.accept(DeliveryIo::new(socket)).await?;
    // The caller waits for the acknowledgement before it starts PgWire.
    // Reject trailing bytes so the line reader cannot swallow its first frame.
    let mut line = String::new();
    let mut reader = BufReader::new((&mut socket).take(FRAME_BYTES));
    reader.read_line(&mut line).await?;
    if !line.ends_with('\n') || !reader.buffer().is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Incomplete recovery request",
        ));
    }
    drop(reader);
    Ok((socket, line))
}

async fn write_reply(socket: &mut (impl AsyncWrite + Unpin), reply: &Reply) -> std::io::Result<()> {
    let mut bytes = serde_json::to_vec(reply).expect("the recovery reply serializes");
    if bytes.len() >= MAX_REPLY_BYTES {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Recovery reply exceeds its bound"));
    }
    bytes.push(b'\n');
    socket.write_all(&bytes).await?;
    socket.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_directory::MemoryDirectory;

    #[tokio::test]
    async fn protected_root_issuer_requires_the_authenticated_original_capture() -> Result<()> {
        let temporary = tempfile::tempdir().unwrap();
        let keys = Arc::new(crate::keys::LocalKeyService::new(temporary.path().join("keys"))?);
        let directory = MemoryDirectory::new();
        let mut installation = DeploymentRecord::new("tenant", "cell", Vec::new(), 0);
        installation.epoch = 1;
        installation.storage_namespaces = Some(rtdb_directory::StorageNamespaces {
            objects: Uuid::new_v4(), keys: keys.namespace_id().await?,
        });
        let authority = Uuid::parse_str(&installation.id).unwrap();
        let capture = Uuid::new_v4();
        let token = "c".repeat(64);
        let mut service = RecoveryService {
            token: Token::new(&token)?,
            org: "cell".into(),
            storage: CellStorage::Local { root: temporary.path().join("serving") },
            archive: CellStorage::Local { root: temporary.path().join("archive") },
            keys,
            broker: None,
            hosted_task: Some(HostedTask::new(
                "arn:aws:ecs:us-east-1:123456789012:cluster/cell",
                "arn:aws:ecs:us-east-1:123456789012:task/cell/11111111111111111111111111111111",
            )?),
            control: ControlKey(Some(Arc::new(crate::capability::CellSecret::new(&[3; 32])?))),
            memory: MemoryBudget::new(4096),
        };
        let manifest = URL_SAFE_NO_PAD.encode([4; 32]);
        installation.checkpoint_captures.insert(capture, rtdb_directory::CheckpointCapture {
            archive: serde_json::to_string(&service.archive).unwrap(),
            key_namespace: None,
            incarnation: installation.incarnation,
            log_epoch: installation.log_epoch,
            checkpoint: rtdb_directory::Checkpoint { block: 1, boundary: 2, source: 2, system_time: 10, root_sha256: Some([9; 32]) },
            automatic_fork: false,
            world_replay: None,
            media_bound: true,
            manifest_sha256: Some(manifest.clone()),
            remote_disposal: None,
        });
        directory.create_deployment(installation).await?;
        let request = |credential: &str, capture, digest: &str| Request {
            token: credential.into(), name: "tenant".into(), authority, capture,
            action: Action::ProtectedRootCustodyIdentity { manifest_sha256: digest.into() },
        };
        assert!(matches!(service.handle(directory.clone(), request(&token, capture, &manifest)).await,
            Reply::ProtectedRootCustodyIdentity { .. }));
        assert!(matches!(service.handle(directory.clone(), request("wrong", capture, &manifest)).await, Reply::Refused));
        assert!(matches!(service.handle(directory.clone(), request(&token, Uuid::new_v4(), &manifest)).await, Reply::Refused));
        assert!(matches!(service.handle(directory.clone(), request(&token, capture, &URL_SAFE_NO_PAD.encode([5; 32]))).await, Reply::Refused));
        service.control = ControlKey::default();
        assert!(matches!(service.handle(directory, request(&token, capture, &manifest)).await, Reply::Refused));
        Ok(())
    }

    #[test]
    fn recovery_endorsement_accepts_only_canonical_rfc1_bytes() -> Result<()> {
        let body = [0xaf; FAMILY_CUSTODY_GRANT_BYTES];
        let hex = lower_hex(&body);
        assert_eq!(decode_family_custody_grant(&hex)?, body);
        assert!(decode_family_custody_grant(&hex.to_uppercase()).is_err());
        assert!(decode_family_custody_grant(&hex[..hex.len() - 2]).is_err());
        Ok(())
    }

    async fn bind_test_archive_media(service: &RecoveryService, owner: &DeploymentRecord, operation: Uuid) -> Result<()> {
        let deployment = owner.id.parse().unwrap();
        let point = CapturePoint {
            deployment, operation, incarnation: owner.incarnation, log_epoch: owner.log_epoch,
            checkpoint: owner.checkpoint_captures[&operation].checkpoint,
        };
        let marker = rtdb_storage::ObjectKey::new(format!("checkpoint-authorities/{deployment}/{operation}.json"))?;
        let archive = service.archive.bucket(rtdb_storage::IoRegion::new()).await?;
        archive.put(&marker, serde_json::to_vec(&point).unwrap().into()).await?;
        Ok(())
    }

    #[tokio::test]
    async fn scheduled_capture_revocation_survives_reopening_the_key_authority() -> Result<()> {
        let temporary = tempfile::tempdir().unwrap();
        let key_root = temporary.path().join("keys");
        let keys = Arc::new(crate::keys::LocalKeyService::with_archive_broker(&key_root)?);
        let directory = MemoryDirectory::new();
        let mut installation = DeploymentRecord::new("tenant", "cell", Vec::new(), rtdb_directory::now_ms());
        let authority = Uuid::parse_str(&installation.id).unwrap();
        let capture = Uuid::new_v4();
        let token = "c".repeat(64);
        let mut service = RecoveryService {
            broker: None,
            hosted_task: None,
            control: ControlKey::default(),
            memory: MemoryBudget::new(rtdb_node::NodeOptions::default().execution_memory_bytes),
            token: Token::new(&token).unwrap(),
            org: "cell".into(),
            storage: CellStorage::Local { root: temporary.path().join("serving") },
            archive: CellStorage::Local { root: temporary.path().join("archive") },
            keys: keys.clone(),
        };
        let checkpoint = rtdb_directory::Checkpoint {
            block: 1, boundary: 2, source: 2, system_time: 10, root_sha256: Some([9; 32]),
        };
        let manifest = URL_SAFE_NO_PAD.encode([4; 32]);
        installation.checkpoint_captures.insert(capture, rtdb_directory::CheckpointCapture {
            archive: serde_json::to_string(&service.archive).unwrap(),
            key_namespace: Some(keys.namespace_id().await?),
            incarnation: installation.incarnation,
            log_epoch: installation.log_epoch,
            checkpoint,
            automatic_fork: false,
            world_replay: None,
            media_bound: true,
            manifest_sha256: Some(manifest.clone()),
            remote_disposal: None,
        });
        let reader = Uuid::new_v4();
        let witness = keys.hold_local_storage_region(reader)?.unwrap();
        installation.storage_regions.insert(reader, rtdb_directory::StorageRegion::Local {
            archive: Some(capture), checkpoint_suffix: None, repair_archive: None,
            tracks_ordered_index_pages: false, checkpoint_metadata_from: None, checkpoint_replay_from: None,
        });
        let key_id = format!("checkpoint-{authority}-{}-{capture}", installation.incarnation);
        let retained = keys.archive_key(&key_id, true, rtdb_storage::IoRegion::new()).await?;
        let plaintext = bytes::Bytes::from_static(b"held archive");
        let ciphertext = retained.seal("member", plaintext.clone()).await?;
        bind_test_archive_media(&service, &installation, capture).await?;
        directory.create_deployment(installation).await?;
        let request = |action: serde_json::Value| {
            serde_json::from_value(serde_json::json!({
                "token": token, "name": "tenant", "authority": authority, "capture": capture, "action": action,
            })).unwrap()
        };
        let due_at_ms = rtdb_directory::now_ms() + 400;
        let schedule = |due_at_ms: i64| request(serde_json::json!({
            "operation": "schedule_checkpoint_capture_revocation", "manifest_sha256": manifest, "due_at_ms": due_at_ms,
        }));
        let commit = || request(serde_json::json!({
            "operation": "commit_checkpoint_capture_revocation", "manifest_sha256": manifest,
        }));
        assert!(matches!(service.handle(directory.clone(), schedule(due_at_ms)).await,
            Reply::CaptureRevocationScheduled { due_at_ms: accepted, .. } if accepted == due_at_ms));
        assert!(matches!(service.handle(directory.clone(), schedule(due_at_ms + 1)).await, Reply::Refused));
        assert!(matches!(service.handle(directory.clone(), commit()).await, Reply::Deferred));

        drop(keys);
        let reopened = Arc::new(crate::keys::LocalKeyService::new(&key_root)?);
        service.keys = reopened.clone();
        assert!(matches!(service.handle(directory.clone(), schedule(due_at_ms)).await,
            Reply::CaptureRevocationScheduled { due_at_ms: accepted, .. } if accepted == due_at_ms));
        let remaining = due_at_ms.saturating_sub(rtdb_directory::now_ms());
        if remaining > 50 {
            assert_eq!(retained.open("member", ciphertext.clone()).await?, plaintext);
        }
        tokio::time::sleep(Duration::from_millis(remaining.max(0) as u64 + 5)).await;
        assert!(reopened.archive_key(&key_id, false, rtdb_storage::IoRegion::new()).await.is_err());
        assert!(retained.open("member", ciphertext).await.is_err());
        assert!(matches!(service.handle(directory.clone(), commit()).await, Reply::Deferred));

        drop(witness);
        assert!(matches!(service.handle(directory.clone(), commit()).await,
            Reply::CaptureRevocationCommitted { operation_revoked_at_ms, .. } if operation_revoked_at_ms >= due_at_ms));
        let settle = request(serde_json::json!({
            "operation": "settle_checkpoint_capture_discard", "manifest_sha256": manifest,
        }));
        assert!(matches!(service.handle(directory, settle).await, Reply::CaptureDiscarded { .. }));
        Ok(())
    }

    #[tokio::test]
    async fn capture_discard_separates_admission_drain_key_withdrawal_and_settlement() {
        let temporary = tempfile::tempdir().unwrap();
        let directory = MemoryDirectory::new();
        let mut installation = DeploymentRecord::new("tenant", "cell", Vec::new(), rtdb_directory::now_ms());
        let authority = Uuid::parse_str(&installation.id).unwrap();
        let capture = Uuid::new_v4();
        let reader = Uuid::new_v4();
        let token = "a".repeat(64);
        let keys = Arc::new(crate::keys::LocalKeyService::new(temporary.path().join("keys")).unwrap());
        let service = RecoveryService {
            broker: None,
            hosted_task: None,
            control: ControlKey::default(),
            memory: MemoryBudget::new(rtdb_node::NodeOptions::default().execution_memory_bytes),
            token: Token::new(&token).unwrap(),
            org: "cell".into(),
            storage: CellStorage::Local { root: temporary.path().join("serving") },
            archive: CellStorage::Local { root: temporary.path().join("archive") },
            keys: keys.clone(),
        };
        let point = rtdb_directory::Checkpoint {
            block: 1, boundary: 3, source: 2, system_time: 10, root_sha256: Some([7; 32]),
        };
        let manifest = URL_SAFE_NO_PAD.encode([0xd; 32]);
        installation.checkpoint_captures.insert(capture, rtdb_directory::CheckpointCapture {
            archive: serde_json::to_string(&service.archive).unwrap(),
            key_namespace: Some(keys.namespace_id().await.unwrap()),
            incarnation: installation.incarnation,
            log_epoch: installation.log_epoch,
            checkpoint: point,
            automatic_fork: false,
            world_replay: None,
            media_bound: true,
            manifest_sha256: Some(manifest.clone()),
            remote_disposal: None,
        });
        let archive_key_id = format!("checkpoint-{authority}-{}-{capture}", installation.incarnation);
        let original = keys.key(&archive_key_id).await.unwrap();
        let independent = keys.key("independent-capture").await.unwrap();
        let witness = keys.hold_local_storage_region(reader).unwrap().unwrap();
        installation.storage_regions.insert(reader, rtdb_directory::StorageRegion::Local {
            archive: Some(capture), checkpoint_suffix: None, repair_archive: None, tracks_ordered_index_pages: false,
            checkpoint_metadata_from: None, checkpoint_replay_from: None,
        });
        bind_test_archive_media(&service, &installation, capture).await.unwrap();
        directory.create_deployment(installation).await.unwrap();
        let run = |action: serde_json::Value| {
            let request = serde_json::from_value(serde_json::json!({
                "token": token, "name": "tenant", "authority": authority, "capture": capture, "action": action,
            })).unwrap();
            service.handle(directory.clone(), request)
        };
        let prepare = serde_json::json!({
            "operation": "prepare_checkpoint_capture_discard", "manifest_sha256": manifest,
        });
        let settle = serde_json::json!({
            "operation": "settle_checkpoint_capture_discard", "manifest_sha256": manifest,
        });
        let status = serde_json::json!({
            "operation": "checkpoint_capture_discard_status", "manifest_sha256": manifest,
        });
        assert!(matches!(run(settle.clone()).await, Reply::Refused));
        let mut wrong_manifest = prepare.clone();
        wrong_manifest["manifest_sha256"] = URL_SAFE_NO_PAD.encode([0xf; 32]).into();
        assert!(matches!(run(wrong_manifest).await, Reply::Refused));
        let mut timed = prepare.clone();
        timed["deadline_ms"] = 120_000.into();
        assert!(matches!(run(timed).await, Reply::CaptureDiscardDeadlineUnsupported));
        assert!(directory.deployment(&authority.to_string()).await.unwrap().unwrap()
            .checkpoint_capture_retirements.is_empty());

        let Reply::CaptureDiscardPrepared { authorized_at_ms, key_withdrawn_at_ms: None, .. } =
            run(prepare.clone()).await else { panic!("capture did not prepare") };
        assert!(matches!(run(prepare).await,
            Reply::CaptureDiscardPrepared { authorized_at_ms: same, key_withdrawn_at_ms: None, .. } if same == authorized_at_ms));
        let current = directory.deployment(&authority.to_string()).await.unwrap().unwrap();
        assert!(archive::require_capture_open(&current, capture).is_err());
        assert!(matches!(run(settle.clone()).await, Reply::Deferred));
        assert_eq!(keys.existing(&archive_key_id).await.unwrap(), original);

        drop(witness);
        let other_vault = crate::keys::LocalKeyService::new(temporary.path().join("other-vault")).unwrap();
        let wrong_vault_key = other_vault.key(&archive_key_id).await.unwrap();
        assert!(archive::finish_capture_retirement(
            directory.as_ref(), &service.storage, &other_vault, &authority.to_string(), capture,
        ).await.is_err());
        assert_eq!(other_vault.existing(&archive_key_id).await.unwrap(), wrong_vault_key);
        assert_eq!(keys.existing(&archive_key_id).await.unwrap(), original);
        assert!(matches!(run(status.clone()).await, Reply::CaptureDiscardPrepared { key_withdrawn_at_ms: None, .. }));
        let CellStorage::Local { root } = &service.archive else { unreachable!() };
        let detached = temporary.path().join("detached-media");
        std::fs::rename(root, &detached).unwrap();
        assert!(matches!(run(settle.clone()).await, Reply::Deferred));
        let Reply::CaptureDiscardPrepared { authorized_at_ms: same, key_withdrawn_at_ms: Some(withdrawn), .. } =
            run(status.clone()).await else { panic!("the durable key withdrawal is missing") };
        assert_eq!(same, authorized_at_ms);
        assert!(keys.existing(&archive_key_id).await.is_err());
        assert_eq!(keys.existing("independent-capture").await.unwrap(), independent);
        assert!(matches!(run(settle.clone()).await, Reply::Deferred));
        std::fs::remove_dir(root).unwrap();
        std::fs::rename(detached, root).unwrap();
        let Reply::CaptureDiscarded { archive_disposal, .. } = run(settle).await
            else { panic!("capture did not settle") };
        assert_eq!((archive_disposal.authorized_at_ms, archive_disposal.key_withdrawn_at_ms), (authorized_at_ms, withdrawn));
        assert!(archive_disposal.observed_empty_at_ms >= withdrawn);
        assert!(matches!(run(status).await, Reply::CaptureDiscarded { .. }));
    }

    #[tokio::test]
    async fn capture_requires_the_operator_and_exact_installation() {
        let temporary = tempfile::tempdir().unwrap();
        let directory = MemoryDirectory::new();
        let installation = rtdb_directory::DeploymentRecord::new(
            "tenant",
            "cell",
            Vec::new(),
            rtdb_directory::now_ms(),
        );
        let authority = Uuid::parse_str(&installation.id).unwrap();
        directory.create_deployment(installation).await.unwrap();
        let token = "a".repeat(64);
        let service = RecoveryService {
            broker: None,
            hosted_task: None,
            control: ControlKey::default(),
            memory: MemoryBudget::new(rtdb_node::NodeOptions::default().execution_memory_bytes),
            token: Token::new(&token).unwrap(),
            org: "cell".into(),
            storage: CellStorage::Local {
                root: temporary.path().join("serving"),
            },
            archive: CellStorage::Local {
                root: temporary.path().join("archive"),
            },
            keys: Arc::new(
                crate::keys::LocalKeyService::new(temporary.path().join("keys")).unwrap(),
            ),
        };
        let capture = Uuid::new_v4();
        let request = |token: &str, authority: Uuid| {
            serde_json::json!({
                "token": token,
                "name": "tenant",
                "authority": authority,
                "capture": capture,
                "action": { "operation": "capture_checkpoint" }
            })
        };
        assert!(matches!(
            service
                .handle(
                    directory.clone(),
                    serde_json::from_value(request("invalid", authority)).unwrap()
                )
                .await,
            Reply::Refused
        ));
        assert!(matches!(
            service
                .handle(
                    directory.clone(),
                    serde_json::from_value(request(&token, Uuid::new_v4())).unwrap()
                )
                .await,
            Reply::Refused
        ));
        assert!(matches!(
            service
                .handle(
                    directory.clone(),
                    serde_json::from_value(request(&token, authority)).unwrap()
                )
                .await,
            Reply::Deferred
        ));
        assert!(
            directory
                .deployment_by_name("tenant")
                .await
                .unwrap()
                .unwrap()
                .checkpoint_captures
                .is_empty()
        );
    }
}
