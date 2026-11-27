use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use rtdb_columnar::MemoryBudget;
use rtdb_types::{Anomaly, Result};
use serde_json::{Value, json};
use uuid::Uuid;

use super::{ActivationAuthority, Binding, VerifiedSuccessor};
use crate::{
    config::NodeConfiguration,
    multi::{CONSENSUS_SCHEME, DEVELOPMENT_DATABASE, MultiRuntime},
};

pub const HELP: &str = "Usage: rtdb multi-activation status DATABASE AUTHORITY
       rtdb multi-activation bootstrap DATABASE AUTHORITY --incarnation UUID
       rtdb multi-activation freeze DATABASE AUTHORITY --expected-incarnation UUID --expected-revision N
       rtdb multi-activation publish DATABASE AUTHORITY --expected-incarnation UUID --expected-revision N
                --operation-id UUID --data DIR [--journal-url URL --tls-dir DIR] [--immutable-streams]

AUTHORITY is --authority-id UUID and either --authority-file PATH for a local
rehearsal or --authority-url URL --authority-tls-dir DIR for an independent
immutable consensus journal. Publish reads the durable portable restore from
stopped Multi development data for the database xtdb.
";

#[derive(Clone, Debug)]
enum Authority {
    Local(PathBuf),
    Consensus { url: String, tls: PathBuf },
}

#[derive(Clone, Debug)]
enum Operation {
    Status,
    Bootstrap {
        incarnation: Uuid,
    },
    Freeze {
        expected: Binding,
    },
    Publish {
        expected: Binding,
        operation_id: Uuid,
        destination_data: PathBuf,
        journal: Option<(String, PathBuf)>,
        immutable_streams: bool,
    },
}

#[derive(Clone, Debug)]
pub struct Options {
    database: String,
    authority_id: Uuid,
    authority: Authority,
    operation: Operation,
}

impl Options {
    pub(crate) fn parse(mut arguments: impl Iterator<Item = String>) -> Result<Self> {
        let operation = arguments.next().ok_or_else(arguments_error)?;
        let database = arguments.next().ok_or_else(arguments_error)?;
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
        let mut take = |name: &str| fields.remove(name).ok_or_else(arguments_error);
        let authority_id = id(take("--authority-id")?)?;
        drop(take);
        let authority = match (
            fields.remove("--authority-file"),
            fields.remove("--authority-url"),
            fields.remove("--authority-tls-dir"),
        ) {
            (Some(path), None, None) => Authority::Local(path.into()),
            (None, Some(url), Some(tls)) => Authority::Consensus {
                url,
                tls: tls.into(),
            },
            _ => return Err(arguments_error()),
        };
        let operation = match operation.as_str() {
            "status" => Operation::Status,
            "bootstrap" => Operation::Bootstrap {
                incarnation: id(fields.remove("--incarnation").ok_or_else(arguments_error)?)?,
            },
            "freeze" => Operation::Freeze {
                expected: binding(&mut fields)?,
            },
            "publish" => {
                if database != DEVELOPMENT_DATABASE {
                    return Err(Anomaly::incorrect(
                        "rtdb/activation-destination",
                        "Publishing stopped development data requires the database xtdb",
                    ));
                }
                let expected = binding(&mut fields)?;
                let operation_id = id(fields
                    .remove("--operation-id")
                    .ok_or_else(arguments_error)?)?;
                let destination_data = fields.remove("--data").ok_or_else(arguments_error)?.into();
                let journal = match (fields.remove("--journal-url"), fields.remove("--tls-dir")) {
                    (Some(url), Some(tls)) => Some((url, tls.into())),
                    (None, None) => None,
                    _ => return Err(arguments_error()),
                };
                if immutable_streams && !journal.as_ref().is_some_and(|(url, _)| url.starts_with(CONSENSUS_SCHEME)) {
                    return Err(arguments_error());
                }
                Operation::Publish {
                    expected,
                    operation_id,
                    destination_data,
                    journal,
                    immutable_streams,
                }
            }
            _ => return Err(arguments_error()),
        };
        if !fields.is_empty() || immutable_streams && !matches!(operation, Operation::Publish { .. }) {
            return Err(arguments_error());
        }
        Ok(Self {
            database,
            authority_id,
            authority,
            operation,
        })
    }

    pub async fn run(self, apple_sync: rtdb_storage::AppleSync) -> Result<Value> {
        let authority = match self.authority {
            Authority::Local(path) => {
                if !matches!(self.operation, Operation::Bootstrap { .. }) && !path.is_file() {
                    return Err(super::unavailable());
                }
                ActivationAuthority::open_local(&path, self.authority_id)?
            }
            Authority::Consensus { url, tls } => {
                if !url.starts_with(CONSENSUS_SCHEME) {
                    return Err(arguments_error());
                }
                ActivationAuthority::connect(&url, crate::cli::journal_credentials(&tls)?, self.authority_id).await?
            }
        };
        let database = self.database;
        match self.operation {
            Operation::Status => (),
            Operation::Bootstrap { incarnation } => {
                authority.bootstrap(&database, incarnation).await?;
            }
            Operation::Freeze { expected } => {
                authority.close_grants(&database, expected).await?;
            }
            Operation::Publish {
                expected,
                operation_id,
                destination_data,
                journal,
                immutable_streams,
            } => {
                let ready = read_installation(
                    &destination_data, journal.as_ref(), immutable_streams, apple_sync,
                ).await?;
                authority
                    .publish_successor(&database, expected, operation_id, ready)
                    .await?;
            }
        }
        let status = authority.status(&database).await?;
        Ok(json!({
            "database": database,
            "incarnation": status.binding.incarnation,
            "revision": status.binding.revision,
            "grants_closed": status.grants_closed,
            "grants_end_micros": status.grants_end_micros,
        }))
    }
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
    data: &Path,
    journal: Option<&(String, PathBuf)>,
    immutable_streams: bool,
    apple_sync: rtdb_storage::AppleSync,
) -> Result<VerifiedSuccessor> {
    let missing = || {
        Anomaly::incorrect(
            "rtdb/activation-destination",
            "Publish requires a committed portable restore on initialized destination data",
        )
    };
    if !data.join("database-id").is_file() || !data.join("journal").is_file() {
        return Err(missing());
    }
    let mut configuration = NodeConfiguration::from_yaml_with_environment("", &|_| None, false)?;
    configuration.apple_sync = apple_sync;
    configuration.multi_journal.immutable_streams = immutable_streams;
    let journal = journal
        .map(|(url, tls)| crate::cli::development_stream_journal(url, tls))
        .transpose()?;
    let runtime = MultiRuntime::development_with_incarnation(
        data,
        &mut configuration,
        MemoryBudget::new(256 << 20),
        None,
        journal,
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
            "publish xtdb --authority-id {authority} --authority-file file --expected-incarnation {predecessor} --expected-revision 7 --operation-id {operation} --data target"
        );
        let options = Options::parse(arguments.split_whitespace().map(str::to_owned)).unwrap();
        assert!(
            matches!(options.operation, Operation::Publish { expected: Binding { incarnation, revision: 7 }, operation_id, .. }
            if incarnation == predecessor && operation_id == operation)
        );
        assert!(Options::parse(arguments.split_whitespace().map(str::to_owned).take(12)).is_err());
    }
}
