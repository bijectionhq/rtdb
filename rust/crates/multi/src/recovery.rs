//! Authenticated access to a cell's exact checkpoint archive.

use std::{path::Path, sync::Arc, time::Duration};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rtdb_directory::{
    CheckpointStageIntent, CheckpointSuffixCustody, DeploymentRecord, Directory, DisposalReceipt, HostedTask,
    ServingInventory,
};
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
        hosted_task: Option<HostedTask>,
        control: ControlKey,
    ) -> Result<Self> {
        Ok(Self {
            token: Token::new(&Token::read(token)?)?,
            org,
            storage,
            archive,
            keys,
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
                    directory, &self.storage, &self.archive, self.keys.as_ref(), self.hosted_task.as_ref(),
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
            Action::DiscardCheckpointCapture { manifest_sha256 } => {
                if manifest_sha256
                    .as_deref()
                    .is_some_and(|value| !valid_manifest(value))
                    || installation.checkpoint_stage.is_some()
                {
                    return Ok(Reply::Refused);
                }
                let release = archive::retire_capture(
                    directory.as_ref(),
                    &installation,
                    &self.storage,
                    &self.archive,
                    self.keys.as_ref(),
                    request.capture,
                    manifest_sha256.as_deref(),
                )
                .await?;
                let (Some(withdrawn), Some(empty)) =
                    (release.key_withdrawn_at_ms, release.observed_empty_at_ms)
                else {
                    return Ok(Reply::Deferred);
                };
                Ok(Reply::CaptureDiscarded {
                    name: installation.name,
                    authority: request.authority,
                    capture: request.capture,
                    manifest_sha256: release.manifest_sha256,
                    archive_disposal: CaptureDisposal {
                        authorized_at_ms: release.authorized_at_ms,
                        key_withdrawn_at_ms: withdrawn,
                        observed_empty_at_ms: empty,
                        inventory: match &self.archive {
                            CellStorage::Local { .. } => ServingInventory::LocalDirectory,
                            CellStorage::S3 { .. } => ServingInventory::S3VersionsAndUploads,
                        },
                    },
                })
            }
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
                let sealed = archive::issue_checkpoint_transfer(
                    Arc::clone(&directory), &self.storage, &self.archive, self.keys.as_ref(),
                    selector.clone(), self.hosted_task.clone(),
                ).await?;
                let activation = archive::activate_checkpoint_transfer_target(
                    directory, &self.storage, self.keys.as_ref(), selector, self.hosted_task.clone(),
                ).await?;
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
                let terminal = archive::issue_checkpoint_transfer(
                    directory, &self.storage, &self.archive, self.keys.as_ref(), selector, self.hosted_task.clone(),
                ).await?;
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
                let activation = archive::activate_checkpoint_transfer_target(
                    directory, &self.storage, self.keys.as_ref(), selector, self.hosted_task.clone(),
                ).await?;
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
                let replica = archive::seed_checkpoint_transfer_target(
                    directory, &self.storage, self.keys.as_ref(), selector, self.hosted_task.clone(),
                ).await?;
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
                let bootstrap = archive::verify_checkpoint_transfer_target(
                    directory, &self.storage, self.keys.as_ref(), selector, self.hosted_task.clone(),
                ).await?;
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

    async fn handle(&self, directory: Arc<dyn Directory>, request: Request) -> Reply {
        match self.dispatch(directory, request).await {
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
        mut socket: TlsStream<TcpStream>,
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
) -> std::io::Result<(TlsStream<TcpStream>, String)> {
    let mut socket = acceptor.accept(socket).await?;
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
