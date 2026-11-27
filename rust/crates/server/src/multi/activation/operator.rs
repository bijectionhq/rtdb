use std::{collections::BTreeMap, path::PathBuf};

use rtdb_columnar::MemoryBudget;
use rtdb_types::{Anomaly, Result};
use serde_json::{Value, json};
use uuid::Uuid;

use self::attestation::{Attestation, Attestors, Intent};
use super::journal_custody::KeeperJournal;
use super::{Access, ActivationAuthority, ActivationStatus, Binding, VerifiedSuccessor, recovery};
use crate::{
    config::NodeConfiguration,
    multi::{CONSENSUS_SCHEME, DEVELOPMENT_DATABASE, MultiRuntime},
};

pub(in crate::multi) mod attestation;
#[cfg(unix)]
mod broker;

pub const HELP: &str = "Usage: rtdb multi-activation status DATABASE AUTHORITY
       rtdb multi-activation bootstrap DATABASE AUTHORITY --incarnation UUID
       rtdb multi-activation upgrade-grants DATABASE AUTHORITY --expected-incarnation UUID --expected-revision N
       rtdb multi-activation freeze DATABASE AUTHORITY --expected-incarnation UUID --expected-revision N
       rtdb multi-activation candidate DATABASE AUTHORITY --expected-incarnation UUID --expected-revision N --operation-id UUID [--cell-id UUID]
       rtdb multi-activation publish DATABASE AUTHORITY --expected-incarnation UUID --expected-revision N --operation-id UUID
                 [--cell-id UUID --installation-proof FILE --manifest-sha256 HEX]
       rtdb multi-activation begin-recovery DATABASE AUTHORITY --expected-incarnation UUID --expected-revision N --operation-id UUID
                 --loss-policy lossless|accept-unknown-tail [--cell-id UUID] [--supersede-operation-id UUID]
       rtdb multi-activation recovery-status|freeze-recovery|publish-recovery DATABASE AUTHORITY
                 --expected-incarnation UUID --expected-revision N --operation-id UUID
       rtdb multi-activation seal-source DATABASE --authority-socket PATH
                 --expected-incarnation UUID --expected-revision N --operation-id UUID
       rtdb multi-activation record-install DATABASE AUTHORITY --expected-incarnation UUID --expected-revision N --operation-id UUID
                 [--cell-id UUID --installation-proof FILE --manifest-sha256 HEX]
       rtdb multi-activation attest xtdb --authority-id UUID --expected-incarnation UUID --expected-revision N --operation-id UUID
                 --data DIR --journal-url URL --tls-dir DIR --immutable-streams
                 --cell-id UUID --attestation-signing-key PKCS8 --manifest-sha256 HEX
                 [--authority-url URL --authority-tls-dir DIR [--journal-custodian-tls-dir DIR]]
       rtdb multi-activation release-recovery-key DATABASE --authority-id UUID --authority-url URL --authority-tls-dir DIR
                 --installation-proof FILE --key-vault DIR
       rtdb multi-activation release-recovery-journal DATABASE --authority-id UUID --authority-url URL --authority-tls-dir DIR
                 --installation-proof FILE --journal-url URL --journal-custodian-tls-dir DIR
       rtdb multi-activation serve --authority-socket PATH --broker-config PATH

AUTHORITY is --authority-socket PATH for a local operator broker, or
--authority-id UUID with either --authority-file PATH or
--authority-url URL --authority-tls-dir DIR for direct access. Direct publish
also requires --data DIR --journal-url URL --tls-dir DIR --immutable-streams.
The broker owns its writer TLS. It can verify a signed installation proof from
a registered destination or read its configured stopped development destination.
Run attest on the stopped destination with its own protected signing key.
Supplying activation reader credentials lets attest retain the destination generation
for a pending durable recovery. Release it from the original vault after settlement.
";

#[derive(Clone, Debug)]
enum Authority {
    Local {
        path: PathBuf,
        identity: Uuid,
    },
    Consensus {
        url: String,
        tls: PathBuf,
        identity: Uuid,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
enum Action {
    Status,
    #[cfg(all(unix, feature = "backup-object-holds"))]
    AuthorizeObjectRelease {
        proof: Attestation,
        objects_sha256: [u8; 32],
    },
    Bootstrap {
        incarnation: Uuid,
    },
    Freeze {
        expected: Binding,
    },
    UpgradeGrants {
        expected: Binding,
    },
    Candidate {
        expected: Binding,
        operation_id: Uuid,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cell: Option<Uuid>,
    },
    Publish {
        expected: Binding,
        operation_id: Uuid,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        installation: Option<RemoteInstallation>,
    },
    Recovery {
        operation: recovery::Operation,
        action: RecoveryAction,
    },
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
enum RecoveryAction {
    Begin { cell: Option<Uuid>, loss_policy: recovery::LossPolicy, supersedes: Option<Uuid> },
    Status,
    Freeze,
    SealSource,
    RecordInstall {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        installation: Option<RemoteInstallation>,
    },
    Publish,
}

impl Action {
    fn installation(&mut self) -> Option<&mut Option<RemoteInstallation>> {
        match self {
            Self::Publish { installation, .. }
            | Self::Recovery { action: RecoveryAction::RecordInstall { installation }, .. } => Some(installation),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteInstallation {
    cell: Uuid,
    manifest_sha256: [u8; 32],
    attestation: Attestation,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    database: String,
    action: Action,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Destination {
    data: PathBuf,
    journal_url: String,
    journal_tls_dir: PathBuf,
}

type JournalReaders = BTreeMap<Uuid, JournalReader>;

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct JournalReader {
    journal_url: String,
    journal_tls_dir: PathBuf,
}

impl JournalReader {
    fn connect(&self) -> Result<KeeperJournal> {
        KeeperJournal::connect(&self.journal_url, &crate::cli::journal_credentials(&self.journal_tls_dir)?)
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Source {
    incarnation: Uuid,
    cell: Option<Uuid>,
    location: Destination,
}

enum DestinationReader<'a> {
    Direct(&'a Destination),
    #[cfg(unix)]
    Broker(&'a Destination),
}

impl DestinationReader<'_> {
    async fn read(
        self,
        intent: &Intent,
        apple_sync: rtdb_storage::AppleSync,
    ) -> Result<rtdb_db::InstalledBackup> {
        let destination = match self {
            Self::Direct(destination) => destination,
            #[cfg(unix)]
            Self::Broker(destination) => {
                broker::validate_destination(destination)?;
                destination
            }
        };
        read_installation(destination, intent, apple_sync).await
    }
}

#[cfg(unix)]
#[derive(Clone, Debug)]
struct ProofFile {
    path: PathBuf,
    cell: Uuid,
    manifest_sha256: [u8; 32],
}

#[derive(Clone, Debug)]
enum Command {
    Direct {
        authority: Authority,
        request: Request,
        destination: Option<Destination>,
    },
    #[cfg(unix)]
    CallBroker {
        socket: PathBuf,
        request: Request,
        proof: Option<ProofFile>,
    },
    #[cfg(unix)]
    Attest {
        intent: Intent,
        manifest_sha256: [u8; 32],
        signing_key: PathBuf,
        destination: Destination,
        hold_authority: Option<(String, PathBuf)>,
        journal_custodian: Option<PathBuf>,
    },
    #[cfg(unix)]
    ReleaseRecoveryKey {
        database: String,
        identity: Uuid,
        url: String,
        tls: PathBuf,
        proof: PathBuf,
        vault: PathBuf,
    },
    #[cfg(unix)]
    ReleaseRecoveryJournal {
        database: String,
        identity: Uuid,
        url: String,
        tls: PathBuf,
        proof: PathBuf,
        journal_url: String,
        journal_tls: PathBuf,
    },
    #[cfg(unix)]
    ServeBroker { socket: PathBuf, config: PathBuf },
}

#[derive(Clone, Debug)]
pub struct Options(Command);

impl Options {
    pub(crate) fn parse(mut arguments: impl Iterator<Item = String>) -> Result<Self> {
        let operation = arguments.next().ok_or_else(arguments_error)?;
        #[cfg(unix)]
        if operation == "serve" {
            let (mut fields, immutable_streams) = fields(arguments)?;
            let socket = take(&mut fields, "--authority-socket")?.into();
            let config = take(&mut fields, "--broker-config")?.into();
            if immutable_streams || !fields.is_empty() {
                return Err(arguments_error());
            }
            return Ok(Self(Command::ServeBroker { socket, config }));
        }
        let database = arguments.next().ok_or_else(arguments_error)?;
        let (mut fields, immutable_streams) = fields(arguments)?;
        #[cfg(unix)]
        if operation == "attest" {
            if !immutable_streams || database != DEVELOPMENT_DATABASE {
                return Err(arguments_error());
            }
            let intent = Intent {
                authority_id: id(take(&mut fields, "--authority-id")?)?,
                database,
                expected: binding(&mut fields)?,
                operation_id: id(take(&mut fields, "--operation-id")?)?,
                cell: Some(id(take(&mut fields, "--cell-id")?)?),
            };
            let manifest_sha256 = manifest_digest(take(&mut fields, "--manifest-sha256")?)?;
            let signing_key = take(&mut fields, "--attestation-signing-key")?.into();
            let destination = destination(&mut fields)?;
            let hold_authority = match (fields.remove("--authority-url"), fields.remove("--authority-tls-dir")) {
                (Some(url), Some(tls)) => Some((url, tls.into())),
                (None, None) => None,
                _ => return Err(arguments_error()),
            };
            let journal_custodian = fields.remove("--journal-custodian-tls-dir").map(PathBuf::from);
            if !fields.is_empty() || journal_custodian.is_some() && hold_authority.is_none() {
                return Err(arguments_error());
            }
            return Ok(Self(Command::Attest {
                intent,
                manifest_sha256,
                signing_key,
                destination,
                hold_authority,
                journal_custodian,
            }));
        }
        #[cfg(unix)]
        if matches!(operation.as_str(), "release-recovery-key" | "release-recovery-journal") {
            let identity = id(take(&mut fields, "--authority-id")?)?;
            let url = take(&mut fields, "--authority-url")?;
            let tls = take(&mut fields, "--authority-tls-dir")?.into();
            let proof = take(&mut fields, "--installation-proof")?.into();
            let release = if operation == "release-recovery-key" {
                let vault = take(&mut fields, "--key-vault")?.into();
                Command::ReleaseRecoveryKey { database, identity, url, tls, proof, vault }
            } else {
                let journal_url = take(&mut fields, "--journal-url")?;
                let journal_tls = take(&mut fields, "--journal-custodian-tls-dir")?.into();
                Command::ReleaseRecoveryJournal { database, identity, url, tls, proof, journal_url, journal_tls }
            };
            if immutable_streams || !fields.is_empty() { return Err(arguments_error()); }
            return Ok(Self(release));
        }

        let action = match operation.as_str() {
            "status" => Action::Status,
            "bootstrap" => Action::Bootstrap {
                incarnation: id(take(&mut fields, "--incarnation")?)?,
            },
            "freeze" => Action::Freeze {
                expected: binding(&mut fields)?,
            },
            "upgrade-grants" => Action::UpgradeGrants {
                expected: binding(&mut fields)?,
            },
            "candidate" => Action::Candidate {
                expected: binding(&mut fields)?,
                operation_id: id(take(&mut fields, "--operation-id")?)?,
                cell: fields.remove("--cell-id").map(id).transpose()?,
            },
            "publish" => Action::Publish {
                expected: binding(&mut fields)?,
                operation_id: id(take(&mut fields, "--operation-id")?)?,
                installation: None,
            },
            "begin-recovery" | "recovery-status" | "freeze-recovery" | "seal-source"
            | "record-install" | "publish-recovery" => {
                let selection = recovery::Operation {
                    predecessor: binding(&mut fields)?,
                    operation_id: id(take(&mut fields, "--operation-id")?)?,
                };
                let action = match operation.as_str() {
                    "begin-recovery" => RecoveryAction::Begin {
                        cell: fields.remove("--cell-id").map(id).transpose()?,
                        supersedes: fields.remove("--supersede-operation-id").map(id).transpose()?,
                        loss_policy: match take(&mut fields, "--loss-policy")?.as_str() {
                            "lossless" => recovery::LossPolicy::Lossless,
                            "accept-unknown-tail" => recovery::LossPolicy::AcceptUnknownTail,
                            _ => return Err(arguments_error()),
                        },
                    },
                    "recovery-status" => RecoveryAction::Status,
                    "freeze-recovery" => RecoveryAction::Freeze,
                    "seal-source" => RecoveryAction::SealSource,
                    "record-install" => RecoveryAction::RecordInstall { installation: None },
                    _ => RecoveryAction::Publish,
                };
                Action::Recovery { operation: selection, action }
            }
            _ => return Err(arguments_error()),
        };
        let mut request = Request { database, action };
        #[cfg(unix)]
        if let Some(socket) = fields.remove("--authority-socket") {
            let proof = if let Some(path) = fields.remove("--installation-proof") {
                if request.action.installation().is_none() {
                    return Err(arguments_error());
                }
                Some(ProofFile {
                    path: path.into(),
                    cell: id(take(&mut fields, "--cell-id")?)?,
                    manifest_sha256: manifest_digest(take(&mut fields, "--manifest-sha256")?)?,
                })
            } else {
                None
            };
            if immutable_streams || !fields.is_empty() {
                return Err(arguments_error());
            }
            return Ok(Self(Command::CallBroker {
                socket: socket.into(),
                request,
                proof,
            }));
        }

        if matches!(request.action, Action::Recovery { action: RecoveryAction::SealSource, .. }) {
            return Err(arguments_error());
        }
        let identity = id(take(&mut fields, "--authority-id")?)?;
        let authority = match (
            fields.remove("--authority-file"),
            fields.remove("--authority-url"),
            fields.remove("--authority-tls-dir"),
        ) {
            (Some(path), None, None) => Authority::Local {
                path: path.into(),
                identity,
            },
            (None, Some(url), Some(tls)) => Authority::Consensus {
                url,
                tls: tls.into(),
                identity,
            },
            _ => return Err(arguments_error()),
        };
        let reads_installation = request.action.installation().is_some();
        let destination = if reads_installation && immutable_streams {
            Some(destination(&mut fields)?)
        } else if reads_installation || immutable_streams {
            return Err(arguments_error());
        } else {
            None
        };
        if !fields.is_empty() {
            return Err(arguments_error());
        }
        Ok(Self(Command::Direct {
            authority,
            request,
            destination,
        }))
    }

    pub async fn run(self, apple_sync: rtdb_storage::AppleSync) -> Result<Value> {
        match self.0 {
            Command::Direct {
                authority,
                request,
                destination,
            } => {
                let authority = match authority {
                    Authority::Local { path, identity } => {
                        if !matches!(request.action, Action::Bootstrap { .. }) && !path.is_file() {
                            return Err(super::unavailable());
                        }
                        ActivationAuthority::open_local(&path, identity)?
                    }
                    Authority::Consensus { url, tls, identity } => {
                        let access = if matches!(
                            request.action,
                            Action::Status | Action::Candidate { .. }
                            | Action::Recovery { action: RecoveryAction::Status, .. }
                        ) {
                            Access::ConsensusReader
                        } else {
                            Access::ConsensusOperator
                        };
                        ActivationAuthority::connect(
                            &url,
                            crate::cli::journal_credentials(&tls)?,
                            identity,
                            access,
                        )
                        .await?
                    }
                };
                request
                    .execute(
                        &authority,
                        destination.as_ref().map(DestinationReader::Direct),
                        None,
                        &Attestors::new(),
                        &JournalReaders::new(),
                        apple_sync,
                    )
                    .await
            }
            #[cfg(unix)]
            Command::CallBroker {
                socket,
                mut request,
                proof,
            } => {
                if let (Some(proof), Some(installation)) = (proof, request.action.installation()) {
                    *installation = Some(RemoteInstallation {
                        cell: proof.cell,
                        manifest_sha256: proof.manifest_sha256,
                        attestation: Attestation::read(&proof.path)?,
                    });
                }
                broker::call(&socket, request).await
            }
            #[cfg(unix)]
            Command::Attest {
                intent,
                manifest_sha256,
                signing_key,
                destination,
                hold_authority,
                journal_custodian,
            } => {
                broker::validate_destination(&destination)?;
                let key = attestation::signing_key(&signing_key)?;
                let authority = if let Some((url, tls)) = hold_authority {
                    super::require_independent_journals(&url, &destination.journal_url)?;
                    Some(ActivationAuthority::connect(
                        &url, crate::cli::journal_credentials(&tls)?, intent.authority_id, Access::ConsensusReader,
                    ).await?)
                } else { None };
                let custodian = journal_custodian.as_deref().map(|tls| {
                    KeeperJournal::connect(&destination.journal_url, &crate::cli::journal_credentials(tls)?)
                }).transpose()?;
                let proof = attest_installation(
                    &destination, intent, manifest_sha256, &key, authority.as_deref(), custodian.as_ref(),
                ).await?;
                Ok(json!(proof))
            }
            #[cfg(unix)]
            Command::ReleaseRecoveryKey { database, identity, url, tls, proof, vault } => {
                let proof = Attestation::read(&proof)?;
                let authority = ActivationAuthority::connect(
                    &url, crate::cli::journal_credentials(&tls)?, identity, Access::ConsensusReader,
                ).await?;
                let kms = rtdb_crypt::FileKms::open(&vault, || {
                    use rtdb_node::Clock;
                    rtdb_node::SystemClock.now().0
                })?;
                let hold = release_recovery_key(&authority, &database, &proof, &kms).await?;
                Ok(json!({ "released": hold }))
            }
            #[cfg(unix)]
            Command::ReleaseRecoveryJournal { database, identity, url, tls, proof, journal_url, journal_tls } => {
                let proof = Attestation::read(&proof)?;
                let authority = ActivationAuthority::connect(
                    &url, crate::cli::journal_credentials(&tls)?, identity, Access::ConsensusReader,
                ).await?;
                let (intent, key, journal) = proof.held_journal()?;
                require_hold_release(&authority, &database, intent, key).await?;
                let custodian = KeeperJournal::connect(&journal_url, &crate::cli::journal_credentials(&journal_tls)?)?;
                custodian.release(key.generation.database, journal).await?;
                Ok(json!({ "released": journal }))
            }
            #[cfg(unix)]
            Command::ServeBroker { socket, config } => {
                broker::serve(&socket, &config, apple_sync).await?;
                Ok(Value::Null)
            }
        }
    }
}

impl Request {
    async fn execute(
        self,
        authority: &ActivationAuthority,
        destination: Option<DestinationReader<'_>>,
        source: Option<&Source>,
        attestors: &Attestors,
        journals: &JournalReaders,
        apple_sync: rtdb_storage::AppleSync,
    ) -> Result<Value> {
        let database = self.database;
        match self.action {
            Action::Status => (),
            #[cfg(all(unix, feature = "backup-object-holds"))]
            Action::AuthorizeObjectRelease { proof, objects_sha256 } => {
                let (intent, hold, manifest_sha256) = proof.hosted_hold(objects_sha256)?;
                if intent.database != database || hold.authority != authority.identity()
                    || hold.authority_sha256 != authority.connection_digest()?
                {
                    return Err(super::conflict());
                }
                proof.verify(intent, manifest_sha256, attestors)?;
                let operation = recovery::Operation {
                    predecessor: intent.expected,
                    operation_id: intent.operation_id,
                };
                authority.require_abandoned_recovery(&database, operation, hold.generation.database).await?;
                return Ok(Value::Null);
            }
            Action::Bootstrap { incarnation } => {
                authority.bootstrap(&database, incarnation).await?;
            }
            Action::Freeze { expected } => {
                authority.close_grants(&database, expected).await?;
            }
            Action::UpgradeGrants { expected } => {
                authority.upgrade_grants(&database, expected).await?;
            }
            Action::Candidate {
                expected,
                operation_id,
                cell,
            } => {
                if authority.binding(&database).await? != expected {
                    return Err(super::conflict());
                }
                let incarnation = authority.successor_incarnation_in_cell(
                    &database,
                    expected,
                    operation_id,
                    cell,
                )?;
                return Ok(json!({ "database": database, "incarnation": incarnation }));
            }
            Action::Publish {
                expected,
                operation_id,
                installation,
            } => {
                let selected = installation
                    .as_ref()
                    .map(|proof| (proof.manifest_sha256, proof.cell));
                if let Some(committed) = authority
                    .publication_status(&database, expected, operation_id, selected)
                    .await?
                {
                    return Ok(report(&database, committed));
                }
                let intent = Intent {
                    authority_id: authority.identity(),
                    database: database.clone(),
                    expected,
                    operation_id,
                    cell: selected.map(|(_, cell)| cell),
                };
                let ready = if let Some(proof) = installation {
                    proof
                        .attestation
                        .verify(&intent, proof.manifest_sha256, attestors)?
                } else {
                    let destination = destination.filter(|_| database == DEVELOPMENT_DATABASE).ok_or_else(|| {
                        Anomaly::incorrect(
                            "rtdb/activation-destination",
                            "Publish requires a registered destination installation proof or configured stopped destination",
                        )
                    })?;
                    VerifiedSuccessor::from_installation(
                        &destination.read(&intent, apple_sync).await?,
                    )
                };
                if ready.key_hold.is_some() { return Err(super::conflict()); }
                let published = authority
                    .publish(&database, expected, operation_id, ready)
                    .await?;
                return Ok(report(&database, published));
            }
            Action::Recovery { operation, action } => {
                let status = match action {
                    RecoveryAction::Begin { cell, loss_policy, supersedes } => authority
                        .begin_recovery(&database, recovery::Intent { operation, destination_cell: cell, loss_policy }, supersedes).await?,
                    RecoveryAction::Status => authority.recovery_status(&database, operation).await?,
                    RecoveryAction::Freeze => authority.freeze_recovery(&database, operation).await?,
                    RecoveryAction::SealSource => seal_source(authority, &database, operation, source).await?,
                    RecoveryAction::RecordInstall { installation } => {
                        let verified = verify_recovery_installation(
                            authority, &database, operation, installation, destination, attestors, journals, apple_sync,
                        ).await?;
                        authority.record_recovery_installation(&database, operation, verified).await?
                    }
                    RecoveryAction::Publish => authority.publish_recovery(&database, operation).await?,
                };
                return Ok(json!(status));
            }
        }
        Ok(report(&database, authority.status(&database).await?))
    }
}

#[cfg(all(unix, feature = "backup-object-holds"))]
pub(in crate::multi) async fn authorize_object_release(
    socket: &std::path::Path, database: &str, proof: Attestation, objects_sha256: [u8; 32],
) -> Result<()> {
    broker::call(socket, Request {
        database: database.to_owned(),
        action: Action::AuthorizeObjectRelease { proof, objects_sha256 },
    }).await?;
    Ok(())
}

async fn verify_recovery_installation(
    authority: &ActivationAuthority,
    database: &str,
    operation: recovery::Operation,
    proof: Option<RemoteInstallation>,
    destination: Option<DestinationReader<'_>>,
    attestors: &Attestors,
    journals: &JournalReaders,
    apple_sync: rtdb_storage::AppleSync,
) -> Result<VerifiedSuccessor> {
    let (recovery_intent, published) = match authority.recovery_status(database, operation).await? {
        recovery::Status::Pending { intent, .. } => (intent, false),
        recovery::Status::Published { intent, .. } => (intent, true),
    };
    let intent = Intent {
        authority_id: authority.identity(),
        database: database.to_owned(),
        expected: operation.predecessor,
        operation_id: operation.operation_id,
        cell: recovery_intent.destination_cell,
    };
    if let Some(proof) = proof {
        if Some(proof.cell) != intent.cell {
            return Err(super::conflict());
        }
        let verified = proof.attestation.verify(&intent, proof.manifest_sha256, attestors)?;
        if !published {
            match (journals.get(&proof.cell), verified.journal_hold) {
                (Some(reader), Some(hold)) => reader.connect()?.require_retained(verified.successor, hold).await?,
                (None, None) => (),
                _ => return Err(Anomaly::incorrect(
                    "rtdb/activation-journal-custody",
                    "The destination must match its registered journal custody reader and hold",
                )),
            }
        }
        return Ok(verified);
    }
    let destination = destination
        .filter(|_| database == DEVELOPMENT_DATABASE && intent.cell.is_none())
        .ok_or_else(missing_installation)?;
    Ok(VerifiedSuccessor::from_installation(&destination.read(&intent, apple_sync).await?))
}

#[cfg(unix)]
async fn seal_source(
    authority: &ActivationAuthority, database: &str, operation: recovery::Operation,
    source: Option<&Source>,
) -> Result<recovery::Status> {
    use journal_contract::{Journal, Lsn, StreamId};

    let status = authority.recovery_status(database, operation).await?;
    match status {
        recovery::Status::Published { .. } | recovery::Status::Pending { final_source_lsn: Some(_), .. } => {
            return Ok(status);
        }
        recovery::Status::Pending { grants_closed: true, intent, .. }
            if intent.loss_policy == recovery::LossPolicy::Lossless => (),
        _ => return Err(super::conflict()),
    }
    let source = source.filter(|source| source.incarnation == operation.predecessor.incarnation)
        .ok_or_else(|| Anomaly::incorrect(
            "rtdb/activation-source",
            "The broker requires a commissioned predecessor with source journal credentials",
        ))?;
    let binding = authority.status(database).await?;
    if binding.binding != operation.predecessor || binding.destination_cell != source.cell {
        return Err(super::conflict());
    }
    broker::validate_destination(&source.location)?;
    let scope = rtdb_db::ActivationScope::for_cell(authority.identity(), database, source.cell)?;
    let stopped = StoppedDatabase::open(&source.location, source.incarnation, scope)?;
    let result = async {
        let head = stopped.journal
            .head(StreamId(source.incarnation.as_u128())).await.map_err(super::journal_error)?;
        if head == Lsn(0) {
            return Err(super::conflict());
        }
        stopped.database.check_activation_scope(head).await?;
        authority.seal_recovery_source(database, operation, &*stopped.journal, source.cell).await
    }.await;
    stopped.io.close_and_wait().await;
    result
}

#[cfg(not(unix))]
async fn seal_source(
    _: &ActivationAuthority, _: &str, _: recovery::Operation, _: Option<&Source>,
) -> Result<recovery::Status> {
    Err(Anomaly::unsupported("rtdb/activation-source", "Source sealing requires the activation broker"))
}

fn report(database: &str, status: ActivationStatus) -> Value {
    json!({
        "database": database,
        "incarnation": status.binding.incarnation,
        "revision": status.binding.revision,
        "destination_cell": status.destination_cell,
        "grants_closed": status.grants_closed,
        "grants_end_micros": status.grants_end_micros,
        "grant_protocol_version": status.grant_protocol_version,
    })
}

fn fields(mut arguments: impl Iterator<Item = String>) -> Result<(BTreeMap<String, String>, bool)> {
    let mut fields = BTreeMap::new();
    let mut immutable_streams = false;
    while let Some(name) = arguments.next() {
        if name == "--immutable-streams" && !immutable_streams {
            immutable_streams = true;
            continue;
        }
        let value = arguments.next().ok_or_else(arguments_error)?;
        if name == "--immutable-streams"
            || !name.starts_with("--")
            || value.starts_with("--")
            || fields.insert(name, value).is_some()
        {
            return Err(arguments_error());
        }
    }
    Ok((fields, immutable_streams))
}

fn take(fields: &mut BTreeMap<String, String>, name: &str) -> Result<String> {
    fields.remove(name).ok_or_else(arguments_error)
}

fn id(value: String) -> Result<Uuid> {
    Uuid::parse_str(&value)
        .ok()
        .filter(|id| !id.is_nil())
        .ok_or_else(arguments_error)
}

fn binding(fields: &mut BTreeMap<String, String>) -> Result<Binding> {
    let incarnation = id(fields
        .remove("--expected-incarnation")
        .ok_or_else(arguments_error)?)?;
    let revision = fields
        .remove("--expected-revision")
        .and_then(|value| value.parse().ok())
        .filter(|revision| *revision != 0)
        .ok_or_else(arguments_error)?;
    Ok(Binding {
        incarnation,
        revision,
    })
}

fn arguments_error() -> Anomaly {
    Anomaly::incorrect(
        "rtdb.cli/arguments",
        "Invalid multi-activation arguments. Use rtdb multi-activation --help",
    )
}

fn destination(fields: &mut BTreeMap<String, String>) -> Result<Destination> {
    let journal_url = take(fields, "--journal-url")?;
    if !journal_url.starts_with(CONSENSUS_SCHEME) {
        return Err(arguments_error());
    }
    Ok(Destination {
        data: take(fields, "--data")?.into(),
        journal_url,
        journal_tls_dir: take(fields, "--tls-dir")?.into(),
    })
}

#[cfg(unix)]
fn manifest_digest(input: String) -> Result<[u8; 32]> {
    let bytes = input.as_bytes();
    if bytes.len() != 64 || !bytes.iter().all(u8::is_ascii_hexdigit) {
        return Err(arguments_error());
    }
    let mut digest = [0; 32];
    for (byte, digits) in digest.iter_mut().zip(bytes.chunks_exact(2)) {
        let digits = std::str::from_utf8(digits).map_err(|_| arguments_error())?;
        *byte = u8::from_str_radix(digits, 16).map_err(|_| arguments_error())?;
    }
    Ok(digest)
}

async fn read_installation(
    destination: &Destination,
    intent: &Intent,
    apple_sync: rtdb_storage::AppleSync,
) -> Result<rtdb_db::InstalledBackup> {
    let data = &destination.data;
    if !data.join("database-id").is_file() || !data.join("journal").is_file() {
        return Err(missing_installation());
    }
    let mut configuration = NodeConfiguration::from_yaml_with_environment("", &|_| None, false)?;
    configuration.apple_sync = apple_sync;
    configuration.multi_journal.immutable_streams = true;
    let journal = crate::cli::development_stream_journal(
        &destination.journal_url,
        &destination.journal_tls_dir,
    )?;
    let runtime = MultiRuntime::development_with_access(
        data,
        &mut configuration,
        MemoryBudget::new(256 << 20),
        Some(intent.candidate()?),
        Some(journal),
        None,
        Some(intent.scope()?),
    )
    .await?;
    let result = async {
        let database = runtime
            .ensure_driver(
                DEVELOPMENT_DATABASE,
                runtime.bootstrap[DEVELOPMENT_DATABASE],
            )
            .await?;
        database
            .installed_backup()
            .await?
            .ok_or_else(missing_installation)
    }
    .await;
    runtime.close().await;
    result
}

#[cfg(unix)]
async fn attest_installation(
    destination: &Destination,
    intent: Intent,
    manifest_sha256: [u8; 32],
    key: &ring::signature::Ed25519KeyPair,
    authority: Option<&ActivationAuthority>,
    custodian: Option<&KeeperJournal>,
) -> Result<Attestation> {
    use rtdb_crypt::Kms;

    let candidate = intent.candidate()?;
    let mut stopped = StoppedDatabase::open(destination, candidate, intent.scope()?)?;
    let result = async {
        let selected = if let Some(custodian) = custodian {
            Some(custodian.selected(candidate).await?)
        } else { None };
        let installed = stopped.database.installed_backup().await?.ok_or_else(missing_installation)?;
        let Some(authority) = authority else {
            return Attestation::sign(intent, manifest_sha256, &installed, key);
        };
        let operation = recovery::Operation {
            predecessor: intent.expected, operation_id: intent.operation_id,
        };
        if !matches!(authority.recovery_status(&intent.database, operation).await?,
            recovery::Status::Pending { intent: pending, candidate: selected, .. }
                if candidate == selected && intent.cell == pending.destination_cell)
        {
            return Err(super::conflict());
        }
        let evidence = attestation::KeyEvidence {
            generation: stopped.database.generation().ok_or_else(missing_installation)?,
            authority_sha256: authority.connection_sha256.ok_or_else(super::configuration)?,
        };
        let mut proof = Attestation::sign_installation(intent, manifest_sha256, &installed, Some(evidence), key)?;
        let (_, hold) = proof.held_key()?;
        stopped.keys.retain_recovery_key(hold).await?;
        if let Some((custodian, selected)) = custodian.zip(selected) {
            let retained = proof.bind_journal(selected, key)?;
            custodian.retain(candidate, retained).await?;
        }
        Ok(proof)
    }.await;
    stopped.io.close_and_wait().await;
    result
}

#[cfg(unix)]
async fn release_recovery_key(
    authority: &ActivationAuthority,
    database: &str,
    proof: &Attestation,
    kms: &dyn rtdb_crypt::Kms,
) -> Result<rtdb_crypt::RecoveryKeyHold> {
    let (intent, hold) = proof.held_key()?;
    require_hold_release(authority, database, intent, hold).await?;
    kms.release_recovery_key(hold).await?;
    Ok(hold)
}

#[cfg(unix)]
async fn require_hold_release(
    authority: &ActivationAuthority, database: &str, intent: &Intent, hold: rtdb_crypt::RecoveryKeyHold,
) -> Result<()> {
    if intent.database != database || authority.identity() != hold.authority
        || authority.connection_sha256 != Some(hold.authority_sha256)
    {
        return Err(super::conflict());
    }
    let operation = recovery::Operation {
        predecessor: intent.expected, operation_id: intent.operation_id,
    };
    authority.require_custody_release(database, operation, intent.cell, hold.generation.database).await
}

#[cfg(unix)]
struct StoppedDatabase {
    _lock: std::fs::File,
    journal: std::sync::Arc<dyn journal_contract::Journal>,
    keys: std::sync::Arc<rtdb_crypt::FileKms>,
    database: rtdb_db::MultiDatabase,
    io: std::sync::Arc<rtdb_storage::IoRegion>,
}

#[cfg(unix)]
impl StoppedDatabase {
    fn open(
        destination: &Destination,
        incarnation: Uuid,
        scope: rtdb_db::ActivationScope,
    ) -> Result<Self> {
        use std::{net::Ipv4Addr, sync::Arc};

        use rtdb_crypt::FileKms;
        use rtdb_db::{MultiDatabase, MultiOptions};
        use rtdb_node::{Clock, SystemClock};
        use rtdb_storage::{IoRegion, LocalStore};
        use sha2::{Digest, Sha256};

        use crate::multi::{CLOCK_DRIFT_MICROS, WRITER_LEASE_MICROS, io_error};

        let data = &destination.data;
        // Serialize with local restore and serving without opening a development runtime.
        let lock = existing_marker(&data.join("journal"))?;
        match fs2::FileExt::try_lock_exclusive(&lock) {
            Ok(()) => (),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Err(Anomaly::new(
                    rtdb_types::AnomalyCategory::Busy,
                    "rtdb.multi/development-journal",
                    "Multi development data already has a local writer",
                ));
            }
            Err(error) => return Err(io_error(error)),
        }
        let journal_marker: [u8; 32] = Sha256::digest(destination.journal_url.as_bytes()).into();
        check_marker(&lock, &journal_marker)?;
        check_marker(
            &existing_marker(&data.join("database-id"))?,
            incarnation.as_bytes(),
        )?;

        let connection = crate::cli::development_stream_journal(
            &destination.journal_url,
            &destination.journal_tls_dir,
        )?;
        let journal = rtdb_stream_journal::connect_consensus(&connection.url, &connection.credentials)
            .ok_or_else(missing_installation)?
            .map_err(io_error)?;
        let journal: Arc<dyn journal_contract::Journal> = Arc::new(journal);
        let keys = Arc::new(FileKms::open(&data.join("keys"), || SystemClock.now().0)?);
        let objects = data.join("objects");
        if !objects.is_dir() {
            return Err(missing_installation());
        }

        let io = IoRegion::new();
        let store = Arc::new(LocalStore::open_in(objects, io.clone())?);
        let database = MultiDatabase::new(
            MultiOptions {
                database: incarnation,
                node: Ipv4Addr::LOCALHOST.into(),
                lease_micros: WRITER_LEASE_MICROS,
                drift_micros: CLOCK_DRIFT_MICROS,
            },
            journal.clone(),
            store,
            keys.clone(),
            Arc::new(|| SystemClock.now()),
        )?
        .with_immutable_journal()
        .with_activation_scope(scope)?;
        Ok(Self { _lock: lock, journal, keys, database, io })
    }
}

#[cfg(unix)]
fn existing_marker(path: &std::path::Path) -> Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;

    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(crate::multi::io_error)?;
    if !file.metadata().map_err(crate::multi::io_error)?.is_file() {
        return Err(missing_installation());
    }
    Ok(file)
}

#[cfg(unix)]
fn check_marker<const LENGTH: usize>(file: &std::fs::File, expected: &[u8; LENGTH]) -> Result<()> {
    use std::io::Read;

    let mut bytes = Vec::new();
    file.take((LENGTH + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(crate::multi::io_error)?;
    if bytes != expected {
        return Err(missing_installation());
    }
    Ok(())
}

fn missing_installation() -> Anomaly {
    Anomaly::incorrect(
        "rtdb/activation-destination",
        "Activation requires a committed portable restore on initialized destination data",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_requires_exact_predecessor_and_destination() {
        let authority = Uuid::new_v4();
        let predecessor = Uuid::new_v4();
        let operation = Uuid::new_v4();
        let arguments = format!(
            "publish xtdb --authority-id {authority} --authority-file file --expected-incarnation {predecessor} --expected-revision 7 --operation-id {operation} --data target --journal-url logkeeper-consensus:///journal.json --tls-dir tls --immutable-streams"
        );
        let options = Options::parse(arguments.split_whitespace().map(str::to_owned)).unwrap();
        assert!(matches!(options.0,
                Command::Direct { request: Request { action: Action::Publish {
                    expected: Binding { incarnation, revision: 7 }, operation_id, installation: None,
                }, .. }, destination: Some(_), .. }
                if incarnation == predecessor && operation_id == operation));
        assert!(Options::parse(arguments.split_whitespace().map(str::to_owned).take(12)).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn broker_publish_arguments_contain_no_destination_paths() {
        let predecessor = Uuid::new_v4();
        let operation = Uuid::new_v4();
        let base = format!(
            "publish xtdb --authority-socket /run/rtdb/activation.sock --expected-incarnation {predecessor} --expected-revision 7 --operation-id {operation}"
        );
        let parse =
            |arguments: &str| Options::parse(arguments.split_whitespace().map(str::to_owned));
        assert!(matches!(
            parse(&base).unwrap().0,
            Command::CallBroker { .. }
        ));
        assert!(parse(&format!("{base} --data arbitrary")).is_err());
        assert!(parse(&format!("{base} --tls-dir arbitrary")).is_err());
        assert!(matches!(
            parse("serve --authority-socket /run/rtdb/activation.sock --broker-config /etc/rtdb/broker.json").unwrap().0,
            Command::ServeBroker { .. }
        ));
    }

    #[tokio::test]
    async fn committed_publication_retries_use_the_authority_receipt() {
        use std::sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        };
        use std::time::Duration;

        let directory = tempfile::tempdir().unwrap();
        let mut authority =
            ActivationAuthority::open_local(&directory.path().join("authority"), Uuid::new_v4())
                .unwrap();
        let ticks = AtomicU64::new(0);
        Arc::get_mut(&mut authority).unwrap().clock =
            Arc::new(move || Some(Duration::from_secs(ticks.fetch_add(6, Ordering::Relaxed))));
        let database = DEVELOPMENT_DATABASE.to_owned();
        let expected = authority
            .bootstrap(&database, Uuid::new_v4())
            .await
            .unwrap();
        let operation_id = Uuid::new_v4();
        let successor = authority
            .successor_incarnation(&database, expected, operation_id)
            .unwrap();
        authority.close_grants(&database, expected).await.unwrap();
        let published = authority
            .publish_successor(
                &database,
                expected,
                operation_id,
                VerifiedSuccessor {
                    predecessor: expected.incarnation,
                    successor,
                    manifest_digest: [2; 32],
                    destination_cell: None,
                    key_hold: None,
                    journal_hold: None,
                    source_cut: recovery::SourceCut::default(),
                    activation_scope: Some(
                        rtdb_db::ActivationScope::new(authority.identity(), &database).unwrap(),
                    ),
                },
            )
            .await
            .unwrap();
        // Status has since changed, and no stopped destination is available.
        authority.close_grants(&database, published).await.unwrap();

        let request = |expected| Request {
            database: database.clone(),
            action: Action::Publish {
                expected,
                operation_id,
                installation: None,
            },
        };
        let reply = request(expected)
            .execute(
                &authority,
                None,
                None,
                &Attestors::new(),
                &JournalReaders::new(),
                rtdb_storage::AppleSync::default(),
            )
            .await
            .unwrap();
        assert_eq!(
            reply,
            report(
                &database,
                ActivationStatus {
                    binding: published,
                    destination_cell: None,
                    grants_closed: false,
                    grants_end_micros: None,
                    grant_protocol_version: super::super::ELAPSED_GRANT_PROTOCOL,
                }
            )
        );
        let wrong = Binding {
            incarnation: Uuid::new_v4(),
            ..expected
        };
        assert_eq!(
            request(wrong)
                .execute(
                    &authority,
                    None,
                    None,
                    &Attestors::new(),
                    &JournalReaders::new(),
                    rtdb_storage::AppleSync::default()
                )
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-conflict",
        );
    }
}

#[cfg(all(test, unix))]
pub(in crate::multi) mod testing {
    use ring::signature::{Ed25519KeyPair, KeyPair};

    use super::*;

    pub(in crate::multi) struct Installation {
        request: Request,
        attestors: Attestors,
    }

    impl Installation {
        pub(in crate::multi) fn attest(
            authority: &ActivationAuthority,
            database: &str,
            expected: Binding,
            operation_id: Uuid,
            cell: Uuid,
            installed: &rtdb_db::InstalledBackup,
        ) -> Self {
            let key = Ed25519KeyPair::from_seed_unchecked(&[7; 32]).unwrap();
            let intent = Intent {
                authority_id: authority.identity(),
                database: database.to_owned(),
                expected,
                operation_id,
                cell: Some(cell),
            };
            let manifest_sha256 = installed.manifest_sha256();
            let attestation = Attestation::sign(intent, manifest_sha256, installed, &key).unwrap();
            Self {
                request: Request {
                    database: database.to_owned(),
                    action: Action::Recovery {
                        operation: recovery::Operation { predecessor: expected, operation_id },
                        action: RecoveryAction::RecordInstall {
                            installation: Some(RemoteInstallation { cell, manifest_sha256, attestation }),
                        },
                    },
                },
                attestors: Attestors::from([(cell, key.public_key().as_ref().try_into().unwrap())]),
            }
        }

        pub(in crate::multi) async fn record(
            &self,
            authority: &ActivationAuthority,
        ) -> Result<Value> {
            broker::call_test_peer(authority, &self.attestors, &self.request).await
        }

        pub(in crate::multi) async fn publish_without_proof(
            &self,
            authority: &ActivationAuthority,
        ) -> Result<Value> {
            let mut request = self.request.clone();
            if let Action::Recovery { action, .. } = &mut request.action {
                *action = RecoveryAction::Publish;
            }
            broker::call_test_peer(authority, &Attestors::new(), &request).await
        }
    }
}
