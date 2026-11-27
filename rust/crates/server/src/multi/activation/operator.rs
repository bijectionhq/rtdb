use std::{collections::BTreeMap, path::PathBuf};

use rtdb_columnar::MemoryBudget;
use rtdb_types::{Anomaly, Result};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{Access, ActivationAuthority, ActivationStatus, Binding, VerifiedSuccessor};
use crate::{
    config::NodeConfiguration,
    multi::{CONSENSUS_SCHEME, DEVELOPMENT_DATABASE, MultiRuntime},
};

#[cfg(unix)]
mod broker;

pub const HELP: &str = "Usage: rtdb multi-activation status DATABASE AUTHORITY
       rtdb multi-activation bootstrap DATABASE AUTHORITY --incarnation UUID
       rtdb multi-activation upgrade-grants DATABASE AUTHORITY --expected-incarnation UUID --expected-revision N
       rtdb multi-activation freeze DATABASE AUTHORITY --expected-incarnation UUID --expected-revision N
       rtdb multi-activation candidate DATABASE AUTHORITY --expected-incarnation UUID --expected-revision N --operation-id UUID
       rtdb multi-activation publish DATABASE AUTHORITY --expected-incarnation UUID --expected-revision N --operation-id UUID
       rtdb multi-activation serve --authority-socket PATH --broker-config PATH

AUTHORITY is --authority-socket PATH for a local operator broker, or
--authority-id UUID with either --authority-file PATH or
--authority-url URL --authority-tls-dir DIR for direct access. Direct publish
also requires --data DIR --journal-url URL --tls-dir DIR --immutable-streams.
The broker owns its writer TLS and reads the committed install from its
configured stopped Multi development destination for xtdb.
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
    },
    Publish {
        expected: Binding,
        operation_id: Uuid,
    },
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

enum DestinationReader<'a> {
    Direct(&'a Destination),
    #[cfg(unix)]
    Broker(&'a Destination),
}

impl DestinationReader<'_> {
    async fn read(
        self,
        scope: rtdb_db::ActivationScope,
        apple_sync: rtdb_storage::AppleSync,
    ) -> Result<VerifiedSuccessor> {
        let destination = match self {
            Self::Direct(destination) => destination,
            #[cfg(unix)]
            Self::Broker(destination) => {
                broker::validate_destination(destination)?;
                destination
            }
        };
        read_installation(destination, scope, apple_sync).await
    }
}

#[derive(Clone, Debug)]
enum Command {
    Direct {
        authority: Authority,
        request: Request,
        destination: Option<Destination>,
    },
    #[cfg(unix)]
    CallBroker { socket: PathBuf, request: Request },
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
            },
            "publish" => Action::Publish {
                expected: binding(&mut fields)?,
                operation_id: id(take(&mut fields, "--operation-id")?)?,
            },
            _ => return Err(arguments_error()),
        };
        let request = Request { database, action };
        #[cfg(unix)]
        if let Some(socket) = fields.remove("--authority-socket") {
            if immutable_streams || !fields.is_empty() {
                return Err(arguments_error());
            }
            return Ok(Self(Command::CallBroker {
                socket: socket.into(),
                request,
            }));
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
        let destination = if matches!(request.action, Action::Publish { .. }) && immutable_streams {
            let journal_url = take(&mut fields, "--journal-url")?;
            if !journal_url.starts_with(CONSENSUS_SCHEME) {
                return Err(arguments_error());
            }
            Some(Destination {
                data: take(&mut fields, "--data")?.into(),
                journal_url,
                journal_tls_dir: take(&mut fields, "--tls-dir")?.into(),
            })
        } else if matches!(request.action, Action::Publish { .. }) || immutable_streams {
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
                        apple_sync,
                    )
                    .await
            }
            #[cfg(unix)]
            Command::CallBroker { socket, request } => broker::call(&socket, request).await,
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
        apple_sync: rtdb_storage::AppleSync,
    ) -> Result<Value> {
        let database = self.database;
        match self.action {
            Action::Status => (),
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
            } => {
                if authority.binding(&database).await? != expected {
                    return Err(super::conflict());
                }
                let incarnation =
                    authority.successor_incarnation(&database, expected, operation_id)?;
                return Ok(json!({ "database": database, "incarnation": incarnation }));
            }
            Action::Publish {
                expected,
                operation_id,
            } => {
                if let Some(committed) = authority
                    .publication_status(&database, expected, operation_id)
                    .await?
                {
                    return Ok(report(&database, committed));
                }
                let destination = destination.filter(|_| database == DEVELOPMENT_DATABASE).ok_or_else(|| {
                    Anomaly::incorrect(
                        "rtdb/activation-destination",
                        "Publish requires the configured stopped Multi development destination for xtdb",
                    )
                })?;
                let scope = rtdb_db::ActivationScope::new(authority.identity(), &database)?;
                let ready = destination.read(scope, apple_sync).await?;
                let binding = authority
                    .publish_successor(&database, expected, operation_id, ready)
                    .await?;
                return Ok(report(&database, ActivationStatus::published(binding)));
            }
        }
        Ok(report(&database, authority.status(&database).await?))
    }
}

fn report(database: &str, status: ActivationStatus) -> Value {
    json!({
        "database": database,
        "incarnation": status.binding.incarnation,
        "revision": status.binding.revision,
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

async fn read_installation(
    destination: &Destination,
    scope: rtdb_db::ActivationScope,
    apple_sync: rtdb_storage::AppleSync,
) -> Result<VerifiedSuccessor> {
    let missing = || {
        Anomaly::incorrect(
            "rtdb/activation-destination",
            "Publish requires a committed portable restore on initialized destination data",
        )
    };
    let data = &destination.data;
    if !data.join("database-id").is_file() || !data.join("journal").is_file() {
        return Err(missing());
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
        None,
        Some(journal),
        None,
        Some(scope),
    )
    .await?;
    let result = async {
        let database = runtime
            .ensure_driver(
                DEVELOPMENT_DATABASE,
                runtime.bootstrap[DEVELOPMENT_DATABASE],
            )
            .await?;
        let proof = database.installed_backup().await?.ok_or_else(missing)?;
        Ok(VerifiedSuccessor::from_installation(&proof))
    }
    .await;
    runtime.close().await;
    result
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
                    expected: Binding { incarnation, revision: 7 }, operation_id
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
            },
        };
        let reply = request(expected)
            .execute(&authority, None, rtdb_storage::AppleSync::default())
            .await
            .unwrap();
        assert_eq!(
            reply,
            report(&database, ActivationStatus::published(published))
        );
        let wrong = Binding {
            incarnation: Uuid::new_v4(),
            ..expected
        };
        assert_eq!(
            request(wrong)
                .execute(&authority, None, rtdb_storage::AppleSync::default())
                .await
                .unwrap_err()
                .code,
            "rtdb/activation-conflict",
        );
    }
}
