use std::{io::Read, path::PathBuf};

use rtdb_flight_sql::{
    admin_client::DeploymentAdminClient,
    rtdb::{admin::v1 as proto, v1 as data},
};
use rtdb_types::{Anomaly, AnomalyCategory, Result, rtdb_v1::Basis};
use serde_json::{Value, json};
use uuid::Uuid;
use zeroize::Zeroizing;

use super::invalid;

#[derive(Clone, Debug)]
pub struct AdminOptions {
    endpoint: String,
    authorization_file: PathBuf,
    operation: Operation,
}

#[derive(Clone, Debug)]
enum Operation {
    Create(proto::CreateDeploymentRequest),
    Delete(proto::DeleteDeploymentRequest),
    Fork(proto::ForkDeploymentRequest),
    Export(proto::ExportCheckpointRequest),
    Restore(proto::RestoreDeploymentRequest),
}

impl AdminOptions {
    pub(super) fn parse(
        mut args: impl Iterator<Item = String>,
        env: &impl Fn(&str) -> Option<String>,
    ) -> Result<Self> {
        let verb = args
            .next()
            .ok_or_else(|| invalid("admin requires create, delete, fork, export or restore"))?;
        let mut endpoint = None;
        let mut authorization_file = None;
        let mut retain_until_micros = None;
        let mut incarnation = None;
        let mut targets = Vec::new();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--endpoint" if endpoint.is_none() => {
                    endpoint = Some(option(&mut args, "--endpoint")?)
                }
                "--authorization-file" if authorization_file.is_none() => {
                    authorization_file =
                        Some(PathBuf::from(option(&mut args, "--authorization-file")?))
                }
                "--incarnation" if verb == "create" && incarnation.is_none() => {
                    let id = option(&mut args, "--incarnation")?;
                    let id = Uuid::parse_str(&id)
                        .ok()
                        .filter(|id| !id.is_nil())
                        .ok_or_else(|| invalid("Expected a nonzero deployment incarnation UUID"))?;
                    incarnation = Some(id.as_bytes().to_vec());
                }
                "--retain-until-micros"
                    if retain_until_micros.is_none()
                        && matches!(verb.as_str(), "fork" | "export") =>
                {
                    retain_until_micros = Some(
                        option(&mut args, "--retain-until-micros")?
                            .parse()
                            .map_err(|_| {
                                invalid("Expected an integer UTC microsecond timestamp")
                            })?,
                    );
                }
                _ if arg.starts_with('-') => {
                    return Err(invalid(
                        "Unknown or repeated deployment administration option",
                    ));
                }
                _ => targets.push(arg),
            }
        }
        let args = targets.iter().map(String::as_str).collect::<Vec<_>>();
        let operation = match (verb.as_str(), args.as_slice()) {
            ("create", [deployment]) => Operation::Create(proto::CreateDeploymentRequest {
                deployment: (*deployment).into(),
                incarnation,
            }),
            ("delete", [deployment]) => Operation::Delete(proto::DeleteDeploymentRequest {
                deployment: (*deployment).into(),
            }),
            ("export", [deployment, source_basis, pin_id]) => {
                Operation::Export(proto::ExportCheckpointRequest {
                    deployment: (*deployment).into(),
                    basis: Some(basis(source_basis)?),
                    pin: pin(pin_id)?,
                    retain_until_micros,
                })
            }
            ("fork", [deployment, from, source_basis, pin_id]) => {
                Operation::Fork(proto::ForkDeploymentRequest {
                    deployment: (*deployment).into(),
                    from: (*from).into(),
                    basis: Some(basis(source_basis)?),
                    pin: pin(pin_id)?,
                    retain_until_micros,
                })
            }
            ("restore", [deployment, from, checkpoint]) => {
                Operation::Restore(proto::RestoreDeploymentRequest {
                    deployment: (*deployment).into(),
                    from: (*from).into(),
                    checkpoint: decode_hex(checkpoint)?,
                })
            }
            _ => {
                return Err(invalid(
                    "Invalid admin verb or arguments. See rtdb admin --help",
                ));
            }
        };
        Ok(Self {
            endpoint: endpoint
                .or_else(|| env("RTDB_ADMIN_ENDPOINT"))
                .ok_or_else(|| invalid("Specify --endpoint or RTDB_ADMIN_ENDPOINT"))?,
            authorization_file: authorization_file
                .or_else(|| env("RTDB_ADMIN_AUTHORIZATION_FILE").map(PathBuf::from))
                .ok_or_else(|| {
                    invalid("Specify --authorization-file or RTDB_ADMIN_AUTHORIZATION_FILE")
                })?,
            operation,
        })
    }

    pub async fn run(self) -> Result<Value> {
        const MAX_AUTHORIZATION_BYTES: u64 = 16 * 1024;
        let mut authorization = Zeroizing::new(String::new());
        std::fs::File::open(self.authorization_file)
            .and_then(|file| {
                file.take(MAX_AUTHORIZATION_BYTES + 1)
                    .read_to_string(&mut authorization)
            })
            .map_err(|error| {
                Anomaly::new(
                    AnomalyCategory::Unavailable,
                    "rtdb.admin/credential-file",
                    error.to_string(),
                )
            })?;
        if authorization.len() as u64 > MAX_AUTHORIZATION_BYTES {
            return Err(invalid("The Flight authorization value is too long"));
        }
        let mut client = DeploymentAdminClient::connect(
            self.endpoint,
            authorization.trim_end_matches(['\r', '\n']),
        )
        .await?;
        drop(authorization);
        Ok(match self.operation {
            Operation::Create(request) => deployment(client.create(request).await?),
            Operation::Delete(request) => {
                let reply = client.delete(request).await?;
                json!({"deployment": reply.deployment, "proof": encode_hex(&reply.proof), "unreadable_by_micros": reply.unreadable_by_micros})
            }
            Operation::Fork(request) => deployment(client.fork(request).await?),
            Operation::Export(request) => {
                let reply = client.export(request).await?;
                json!({"basis": reply.basis.map(|basis| encode_hex(&basis.value)), "checkpoint": encode_hex(&reply.checkpoint)})
            }
            Operation::Restore(request) => deployment(client.restore(request).await?),
        })
    }
}

fn option(args: &mut impl Iterator<Item = String>, name: &str) -> Result<String> {
    args.next()
        .filter(|value| !value.starts_with("--"))
        .ok_or_else(|| invalid(format!("{name} requires a value")))
}

fn deployment(reply: proto::Deployment) -> Value {
    let incarnation = Uuid::from_slice(&reply.incarnation)
        .ok()
        .filter(|id| !id.is_nil());
    json!({"deployment": reply.deployment, "basis": reply.basis.map(|basis| encode_hex(&basis.value)), "incarnation": incarnation})
}

fn basis(text: &str) -> Result<data::Basis> {
    let value = decode_hex(text)?;
    if value.len() != Basis::WIDTH {
        return Err(invalid("Expected an opaque basis as 48 hex digits"));
    }
    Ok(data::Basis { value })
}

fn pin(text: &str) -> Result<Vec<u8>> {
    Uuid::parse_str(text)
        .ok()
        .filter(|id| !id.is_nil())
        .map(|id| id.as_bytes().to_vec())
        .ok_or_else(|| invalid("Expected a nonzero checkpoint pin UUID"))
}

fn decode_hex(text: &str) -> Result<Vec<u8>> {
    if text.is_empty() || !text.len().is_multiple_of(2) {
        return Err(invalid("Expected a nonempty hex value"));
    }
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
                .ok_or_else(|| invalid("Invalid hexadecimal value"))
        })
        .collect()
}

fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(encoded, "{byte:02x}").expect("writing to a String");
    }
    encoded
}
