//! Authenticated access to a cell's exact checkpoint archive.

use std::{path::Path, sync::Arc, time::Duration};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rtdb_directory::{
    CheckpointStageIntent, DeploymentRecord, Directory, DisposalReceipt, HostedTask,
};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
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
    archive::{self, CapturePoint},
    deployment::CellStorage,
    install::Token,
    keys::KeyService,
};

const FRAME_BYTES: u64 = 2048;
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
    VerifyCheckpointCapture {
        manifest_sha256: String,
        observation: Uuid,
    },
    RecoverCheckpoint {
        manifest_sha256: String,
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
    Verified {
        name: String,
        authority: Uuid,
        point: CapturePoint,
        manifest_sha256: String,
        observation: Uuid,
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
    Refused,
    Deferred,
}

pub struct RecoveryService {
    token: Token,
    org: String,
    storage: CellStorage,
    archive: CellStorage,
    keys: Arc<dyn KeyService>,
    hosted_task: Option<HostedTask>,
}

impl RecoveryService {
    pub fn from_file(
        token: &Path,
        org: String,
        storage: CellStorage,
        archive: CellStorage,
        keys: Arc<dyn KeyService>,
        hosted_task: Option<HostedTask>,
    ) -> Result<Self> {
        Ok(Self {
            token: Token::new(&Token::read(token)?)?,
            org,
            storage,
            archive,
            keys,
            hosted_task,
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
                        let capture = archive::capture(
                            directory,
                            &self.storage,
                            &self.archive,
                            self.keys.as_ref(),
                            request.authority,
                            request.capture,
                            self.hosted_task.clone(),
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
                let stage = archive::stage_checkpoint(
                    directory,
                    &self.archive,
                    &self.storage,
                    self.keys.as_ref(),
                    intent,
                    self.hosted_task.clone(),
                )
                .await?;
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
            Action::OpenCheckpointStage { .. } => Ok(Reply::Refused),
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
        let Action::OpenCheckpointStage {
            destination,
            stage_operation,
            staged_manifest_sha256,
        } = request.action
        else {
            unreachable!("only stage-reader requests are routed here")
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
        };
        let mut reader = std::pin::pin!(archive::tunnel_staged_checkpoint(
            directory,
            &self.storage,
            self.keys.as_ref(),
            selector,
            self.hosted_task.clone(),
            stop.clone(),
            socket,
        ));
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
                if matches!(request.action, Action::OpenCheckpointStage { .. }) {
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
