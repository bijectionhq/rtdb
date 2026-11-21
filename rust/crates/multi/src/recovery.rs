//! Authenticated access to a cell's exact checkpoint archive.

use std::{path::Path, sync::Arc, time::Duration};

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rtdb_directory::{Directory, HostedTask};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};
use tokio_rustls::{TlsAcceptor, rustls::ServerConfig};
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
    RecoverCheckpoint { manifest_sha256: String },
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
    Recovered {
        name: String,
        authority: Uuid,
        capture: Uuid,
        manifest_sha256: String,
        epoch: u64,
        completed_at_ms: i64,
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

    async fn dispatch(&self, directory: Arc<dyn Directory>, request: Request) -> Result<Reply> {
        if !self.token.accepts(&request.token)
            || request.capture.is_nil()
            || request.authority.is_nil()
        {
            return Ok(Reply::Refused);
        }
        let Some(installation) = directory.deployment_by_name(&request.name).await? else {
            return Ok(Reply::Refused);
        };
        if installation.id != request.authority.to_string()
            || installation.org != self.org
            || installation.parent.is_some()
        {
            return Ok(Reply::Refused);
        }
        match request.action {
            Action::CaptureCheckpoint => {
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
                let manifest_sha256 = capture.manifest_sha256()?;
                Ok(Reply::Captured {
                    name: installation.name,
                    authority: request.authority,
                    point: capture.point,
                    manifest_sha256,
                })
            }
            Action::RecoverCheckpoint { manifest_sha256 } => {
                if manifest_sha256.len() != 43
                    || !URL_SAFE_NO_PAD
                        .decode(&manifest_sha256)
                        .is_ok_and(|bytes| bytes.len() == 32)
                {
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
        }
    }

    async fn handle(&self, directory: Arc<dyn Directory>, line: &str) -> Reply {
        let Ok(request) = serde_json::from_str::<Request>(line) else {
            return Reply::Refused;
        };
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
            Err(_) => break Err(Anomaly::new(
                AnomalyCategory::Unavailable,
                "rtdb.multi/recovery-endpoint",
                "Cannot accept a recovery connection",
            )),
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
        requests.spawn(async move {
            let Ok(Ok((mut socket, line))) =
                tokio::time::timeout(SOCKET_DEADLINE, read_request(socket, acceptor)).await
            else {
                return;
            };
            let _connection = connection;
            let reply = if let Ok(_operation) = operations.try_acquire() {
                // Finishing the operation releases its durable storage registration
                // even when the client disconnects before receiving the receipt.
                service.handle(directory, &line).await
            } else {
                Reply::Deferred
            };
            let _ = tokio::time::timeout(SOCKET_DEADLINE, write_reply(&mut socket, &reply)).await;
        });
    };
    // Close the listener before joining. An accept failure must not abort
    // a running capture or restore that holds a durable admission.
    drop(listener);
    while requests.join_next().await.is_some() {}
    result
}

async fn read_request(
    socket: TcpStream,
    acceptor: TlsAcceptor,
) -> std::io::Result<(impl AsyncRead + AsyncWrite + Unpin, String)> {
    let mut socket = acceptor.accept(socket).await?;
    let mut line = String::new();
    BufReader::new((&mut socket).take(FRAME_BYTES))
        .read_line(&mut line)
        .await?;
    if !line.ends_with('\n') {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Incomplete recovery request",
        ));
    }
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
            .to_string()
        };
        assert!(matches!(
            service
                .handle(directory.clone(), &request("invalid", authority))
                .await,
            Reply::Refused
        ));
        assert!(matches!(
            service
                .handle(directory.clone(), &request(&token, Uuid::new_v4()))
                .await,
            Reply::Refused
        ));
        assert!(matches!(
            service
                .handle(directory.clone(), &request(&token, authority))
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
