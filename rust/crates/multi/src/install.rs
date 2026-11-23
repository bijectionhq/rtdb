//! The hosted gateway's private installation and retirement endpoint.

use std::{
    path::Path,
    sync::Arc,
    time::Duration,
};

use ring::hmac;
use rtdb_directory::{DeploymentRecord, Directory, DisposalReceipt, Lifecycle};
use rtdb_types::{
    Anomaly,
    AnomalyCategory,
    Result,
};
use serde::{
    Deserialize,
    Serialize,
};
use tokio::{
    io::{
        AsyncBufReadExt,
        AsyncReadExt,
        AsyncWriteExt,
        BufReader,
    },
    net::{
        TcpListener,
        TcpStream,
    },
    sync::Semaphore,
};
use tokio_rustls::{
    rustls::ServerConfig,
    TlsAcceptor,
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::admin;

const FRAME_BYTES: u64 = 2048;
const DEADLINE: Duration = Duration::from_secs(15);
const TOKEN_LENGTH: usize = 64;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    operation: Operation,
    token: String,
    name: String,
    authority: Uuid,
    incarnation: u64,
    observation: Option<ArchiveObservation>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveObservation {
    epoch: u64,
    operation: Uuid,
    inventory: [u8; 32],
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Install,
    ScheduleDisposal,
    InspectDisposal,
    ScheduleRecoveryDisposal,
    ConfirmRecoveryArchive,
}

#[derive(Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
enum Reply {
    Installed {
        name: String,
        authority: Uuid,
    },
    Scheduled {
        name: String,
        authority: Uuid,
        epoch: u64,
    },
    Disposed {
        name: String,
        authority: Uuid,
        epoch: u64,
        serving_inventory: rtdb_directory::ServingInventory,
        evidence: [u8; 32],
    },
    RecoveryScheduled {
        name: String,
        authority: Uuid,
        epoch: u64,
    },
    RecoveryArchive {
        name: String,
        authority: Uuid,
        epoch: u64,
        archive: crate::archive::RemoteArchive,
    },
    RecoveryDisposed {
        name: String,
        authority: Uuid,
        epoch: u64,
        evidence: [u8; 32],
    },
    Refused,
    Deferred,
}

pub struct Installer {
    install: Token,
    disposal: Option<Token>,
    org: String,
    replicas: usize,
}

pub(crate) struct Token {
    expected: hmac::Tag,
    verifier: hmac::Key,
}

impl Token {
    pub(crate) fn read(path: &Path) -> Result<String> {
        let token = std::fs::read_to_string(path)
            .map_err(|_| invalid("Cannot read the hosted lifecycle credential"))?;
        let token = token.trim_end_matches('\n');
        if token.len() != TOKEN_LENGTH || !token.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(invalid("Lifecycle tokens must contain 32 hexadecimal bytes"));
        }
        Ok(token.to_owned())
    }

    pub(crate) fn new(token: &str) -> Result<Self> {
        let verifier = hmac::Key::generate(hmac::HMAC_SHA256, &ring::rand::SystemRandom::new())
            .map_err(|_| invalid("Cannot initialize the hosted lifecycle credential verifier"))?;
        let expected = hmac::sign(&verifier, token.as_bytes());
        Ok(Self { expected, verifier })
    }

    pub(crate) fn accepts(&self, token: &str) -> bool {
        hmac::verify(&self.verifier, token.as_bytes(), self.expected.as_ref()).is_ok()
    }
}

impl Installer {
    pub fn from_file(
        path: &Path,
        disposal_path: Option<&Path>,
        org: String,
        replicas: usize,
    ) -> Result<Self> {
        if replicas == 0 {
            return Err(invalid("Installation requires a positive replica count"));
        }
        let install = Token::new(&Token::read(path)?)?;
        let disposal = disposal_path
            .map(|path| {
                let token = Token::read(path)?;
                if install.accepts(&token) {
                    return Err(invalid("Installation and disposal require separate credentials"));
                }
                Token::new(&token)
            })
            .transpose()?;
        Ok(Self {
            install,
            disposal,
            org,
            replicas,
        })
    }

    async fn handle(&self, directory: &dyn Directory, line: &str) -> Reply {
        let Ok(request) = serde_json::from_str::<Request>(line) else {
            return Reply::Refused;
        };
        match self.dispatch(directory, request).await {
            Ok(reply) => reply,
            Err(error)
                if matches!(
                    error.category,
                    AnomalyCategory::Incorrect | AnomalyCategory::Forbidden | AnomalyCategory::Conflict
                ) =>
            {
                Reply::Refused
            },
            Err(_) => Reply::Deferred,
        }
    }

    async fn dispatch(&self, directory: &dyn Directory, request: Request) -> Result<Reply> {
        let token = match request.operation {
            Operation::Install => Some(&self.install),
            Operation::ScheduleDisposal | Operation::InspectDisposal
            | Operation::ScheduleRecoveryDisposal | Operation::ConfirmRecoveryArchive => self.disposal.as_ref(),
        };
        if !token.is_some_and(|token| token.accepts(&request.token)) {
            return Ok(Reply::Refused);
        }
        if request.incarnation == 0 || request.authority.is_nil()
            || request.observation.is_some() != matches!(request.operation, Operation::ConfirmRecoveryArchive)
        {
            return Ok(Reply::Refused);
        }
        let existing = directory.deployment_by_name(&request.name).await?;
        if let Some(installation) = existing.as_ref() {
            if installation.id != request.authority.to_string() || installation.org != self.org
                || installation.parent.is_some()
            {
                return Ok(Reply::Refused);
            }
            if installation.incarnation != request.incarnation {
                let Some(prior) = installation.retired_residences.get(&request.incarnation) else {
                    return Ok(Reply::Refused);
                };
                return match request.operation {
                    Operation::ScheduleDisposal | Operation::InspectDisposal => {
                        disposed_reply(installation.name.clone(), request.authority, &prior.serving)
                    },
                    Operation::ScheduleRecoveryDisposal => match &prior.archives {
                        Some(receipt) => Ok(Reply::RecoveryDisposed {
                            name: installation.name.clone(),
                            authority: request.authority,
                            epoch: receipt.epoch,
                            evidence: receipt_evidence(receipt)?,
                        }),
                        None => Ok(Reply::Deferred),
                    },
                    _ => Ok(Reply::Refused),
                };
            }
        }
        match request.operation {
            Operation::Install => {
                let record = if request.incarnation == 1 {
                    admin::install_deployment(directory, request.authority, &request.name, &self.org, self.replicas).await?
                } else {
                    let Some(existing) = existing.filter(|record| record.linearizable_reads
                        && record.ensemble.len() == self.replicas
                        && matches!(record.lifecycle, Lifecycle::Live | Lifecycle::Quarantined))
                    else { return Ok(Reply::Deferred); };
                    existing
                };
                if record.incarnation != request.incarnation {
                    return Ok(Reply::Refused);
                }
                Ok(Reply::Installed { name: record.name, authority: request.authority })
            }
            Operation::ScheduleDisposal => {
                let installation = admin::begin_hosted_disposal(
                    directory, request.authority, &request.name, &self.org, request.incarnation,
                ).await?;
                disposal_reply(installation, request.authority)
            }
            Operation::InspectDisposal | Operation::ScheduleRecoveryDisposal | Operation::ConfirmRecoveryArchive => {
                let Some(installation) = existing else { return Ok(Reply::Deferred); };
                if let Some(observation) = request.observation {
                    crate::archive::confirm_remote_archive(
                        directory,
                        request.authority,
                        observation.epoch,
                        observation.operation,
                        observation.inventory,
                    ).await?;
                    return Ok(Reply::RecoveryScheduled {
                        name: installation.name, authority: request.authority, epoch: observation.epoch,
                    });
                }
                if matches!(request.operation, Operation::ScheduleRecoveryDisposal) {
                    let receipt = crate::archive::begin_retirement_at(
                        directory, request.authority, Some(request.incarnation),
                    ).await?;
                    if receipt.observed_empty_at_ms.is_some() {
                        return Ok(Reply::RecoveryDisposed {
                            name: installation.name,
                            authority: request.authority,
                            epoch: receipt.epoch,
                            evidence: receipt_evidence(&receipt)?,
                        });
                    }
                    if let Some(archive) = crate::archive::next_remote_archive(directory, request.authority).await? {
                        return Ok(Reply::RecoveryArchive {
                            name: installation.name,
                            authority: request.authority,
                            epoch: receipt.epoch,
                            archive,
                        });
                    }
                    Ok(Reply::RecoveryScheduled {
                        name: installation.name,
                        authority: request.authority,
                        epoch: receipt.epoch,
                    })
                } else {
                    disposal_reply(installation, request.authority)
                }
            }
        }
    }
}

fn disposal_reply(installation: DeploymentRecord, authority: Uuid) -> Result<Reply> {
    match (installation.lifecycle, installation.disposal) {
        (Lifecycle::Deleted, Some(receipt)) => disposed_reply(installation.name, authority, &receipt),
        (Lifecycle::Deleting, None) => Ok(Reply::Scheduled {
            name: installation.name,
            authority,
            epoch: installation.epoch,
        }),
        _ => Ok(Reply::Deferred),
    }
}

fn disposed_reply(name: String, authority: Uuid, receipt: &DisposalReceipt) -> Result<Reply> {
    Ok(Reply::Disposed {
        name,
        authority,
        epoch: receipt.epoch,
        serving_inventory: receipt.serving_inventory,
        evidence: receipt_evidence(receipt)?,
    })
}

fn receipt_evidence(receipt: &impl Serialize) -> Result<[u8; 32]> {
    let bytes = serde_json::to_vec(receipt)
        .map_err(|_| invalid("Cannot encode the disposal receipt"))?;
    let digest = ring::digest::digest(&ring::digest::SHA256, &bytes);
    Ok(digest.as_ref().try_into().expect("SHA-256 has 32 bytes"))
}

pub async fn serve(
    listener: TcpListener,
    tls: Arc<ServerConfig>,
    directory: Arc<dyn Directory>,
    installer: Arc<Installer>,
    cancellation: CancellationToken,
) -> Result<()> {
    let capacity = Arc::new(Semaphore::new(32));
    let acceptor = TlsAcceptor::from(tls);
    loop {
        let (socket, _) = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            accepted = listener.accept() => accepted.map_err(|_| invalid("The installation listener stopped accepting connections"))?,
        };
        let Ok(permit) = capacity.clone().try_acquire_owned() else {
            continue;
        };
        let (installer, directory, acceptor, cancellation) = (
            installer.clone(),
            directory.clone(),
            acceptor.clone(),
            cancellation.clone(),
        );
        tokio::spawn(async move {
            let _permit = permit;
            let request =
                tokio::time::timeout(DEADLINE, exchange(socket, acceptor, directory, installer));
            let _ = cancellation.run_until_cancelled(request).await;
        });
    }
}

async fn exchange(
    socket: TcpStream,
    acceptor: TlsAcceptor,
    directory: Arc<dyn Directory>,
    installer: Arc<Installer>,
) -> std::io::Result<()> {
    let mut socket = acceptor.accept(socket).await?;
    let mut line = String::new();
    let mut bounded = BufReader::new((&mut socket).take(FRAME_BYTES));
    bounded.read_line(&mut line).await?;
    if !line.ends_with('\n') {
        return Ok(());
    }
    let reply = installer.handle(directory.as_ref(), &line).await;
    let mut bytes = serde_json::to_vec(&reply).expect("installation response serializes");
    bytes.push(b'\n');
    socket.write_all(&bytes).await?;
    socket.shutdown().await
}

fn invalid(message: &'static str) -> Anomaly {
    Anomaly::incorrect("rtdb.multi/installation-endpoint", message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_directory::MemoryDirectory;

    #[tokio::test]
    async fn hosted_retirement_cannot_create_a_tombstone_for_an_installed_journal() {
        let directory = MemoryDirectory::new();
        let token = "a".repeat(TOKEN_LENGTH);
        let installer = Installer {
            install: Token::new(&token).unwrap(),
            disposal: Some(Token::new(&token).unwrap()),
            org: "cell".into(),
            replicas: 3,
        };
        let installed = Uuid::new_v4();
        directory.reserve_installed_journal(installed).await.unwrap();
        let schedule = |authority| serde_json::json!({
            "operation": "schedule_disposal", "token": token, "name": "tenant", "authority": authority, "incarnation": 1
        }).to_string();

        assert!(matches!(installer.handle(directory.as_ref(), &schedule(installed)).await, Reply::Refused));
        assert!(directory.deployments().await.unwrap().is_empty());
        let legacy = Uuid::new_v4();
        assert!(matches!(installer.handle(directory.as_ref(), &schedule(legacy)).await,
            Reply::Scheduled { authority, epoch: 1, .. } if authority == legacy));
    }

    #[tokio::test]
    async fn retirement_credentials_fence_the_exact_vacant_installation() {
        let directory = MemoryDirectory::new();
        let install = "a".repeat(TOKEN_LENGTH);
        let disposal = "b".repeat(TOKEN_LENGTH);
        let installer = Installer {
            install: Token::new(&install).unwrap(),
            disposal: Some(Token::new(&disposal).unwrap()),
            org: "cell".into(),
            replicas: 3,
        };
        let authority = Uuid::new_v4();
        let request = |operation: &str, token: &str, authority: Uuid| {
            serde_json::json!({
                "operation": operation, "token": token, "name": "tenant", "authority": authority, "incarnation": 1
            })
            .to_string()
        };
        for (operation, token) in [
            ("schedule_disposal", install.as_str()),
            ("install", disposal.as_str()),
        ] {
            assert!(matches!(
                installer.handle(directory.as_ref(), &request(operation, token, authority)).await,
                Reply::Refused
            ));
        }
        assert!(matches!(
            installer.handle(directory.as_ref(), &request("inspect_disposal", &disposal, authority)).await,
            Reply::Deferred
        ));
        assert!(directory.deployment_by_name("tenant").await.unwrap().is_none());
        for operation in ["schedule_disposal", "schedule_disposal", "inspect_disposal"] {
            assert!(matches!(
                installer.handle(directory.as_ref(), &request(operation, &disposal, authority)).await,
                Reply::Scheduled { authority: confirmed, epoch: 1, .. } if confirmed == authority
            ));
        }
        assert!(matches!(
            installer.handle(directory.as_ref(), &request("schedule_disposal", &disposal, Uuid::new_v4())).await,
            Reply::Refused
        ));
        assert!(matches!(
            installer.handle(directory.as_ref(), &request("install", &install, authority)).await,
            Reply::Refused
        ));

        let first = directory.deployment_by_name("tenant").await.unwrap().unwrap();
        let mut returned = first.clone();
        returned.incarnation = 3;
        returned.epoch = 7;
        returned.lifecycle = Lifecycle::Live;
        let returned = directory.replace_deployment(returned, first.version).await.unwrap().applied().unwrap();
        assert!(matches!(
            installer.handle(directory.as_ref(), &request("schedule_disposal", &disposal, authority)).await,
            Reply::Refused
        ));
        assert_eq!(directory.deployment_by_name("tenant").await.unwrap(), Some(returned));
    }
}
