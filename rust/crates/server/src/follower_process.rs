use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, BufRead, Read, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
};

use rtdb_auth::{AuthConfig, GrantedAccess, Method, MethodRule, PasswordHash};
use rtdb_server::{
    ServerDriver,
    config::ParsedConfiguration,
    multi::{MultiRuntime, NativeFollowerAssignment},
    tls::from_pem_identity,
};
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zeroize::Zeroizing;

use super::{close_unstarted, run_server, shutdown_signal, signal_error, write_report};

const MAX_INPUT: usize = 64 * 1024;

#[derive(Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct Assignment {
    database: String,
    incarnation: Uuid,
    placement_epoch: u64,
    assignment: Uuid,
    endpoint: SocketAddr,
}

impl Assignment {
    fn identity(&self) -> NativeFollowerAssignment {
        NativeFollowerAssignment {
            database: self.database.clone(),
            incarnation: self.incarnation,
            placement_epoch: self.placement_epoch,
            assignment: self.assignment,
        }
    }
}

#[derive(Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct Placement {
    slot: String,
    assignment: Assignment,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Launch {
    slot: String,
    assignment: Assignment,
    certificate: PathBuf,
    private_key: PathBuf,
    user: String,
    password_file: PathBuf,
}

pub(super) async fn serve(parsed: ParsedConfiguration, limit: usize, path: &Path) -> Result<()> {
    let mut launch: Launch = serde_json::from_slice(&read_private(path, MAX_INPUT)?)
        .map_err(|_| invalid("Invalid native follower launch file"))?;
    if launch.slot.is_empty()
        || launch.slot.len() > 255
        || launch.slot.chars().any(char::is_control)
        || launch.assignment.database.is_empty()
        || launch.assignment.database.contains('@')
        || launch.assignment.incarnation.is_nil()
        || launch.assignment.assignment.is_nil()
        || launch.assignment.placement_epoch == 0
        || !launch.assignment.endpoint.ip().is_loopback()
        || launch.user.is_empty()
        || launch.user.len() > 255
        || launch.user == "xtdb"
        || launch.user.chars().any(|character| character == ':' || character.is_control())
    {
        return Err(invalid("Invalid native follower assignment or Basic user"));
    }

    let (mut config, memory, _configuration) = parsed.into_follower_execution(limit)?;
    let password = Zeroizing::new(read_private(&launch.password_file, 64)?);
    if password.len() != 64 || !password.iter().all(u8::is_ascii_hexdigit) {
        return Err(invalid("The follower password must contain 64 hexadecimal digits"));
    }
    let password_hash = PasswordHash::argon2id(
        std::str::from_utf8(&password).expect("validated ASCII"),
        &memory,
    )?;
    drop(password);
    config.auth = AuthConfig::UserList {
        users: BTreeMap::from([(launch.user.clone(), password_hash)]),
        rules: vec![MethodRule {
            method: Method::Password,
            user: Some(launch.user.clone()),
            remote_address: None,
        }],
        grants: BTreeMap::from([(
            launch.user,
            BTreeMap::from([(launch.assignment.database.clone(), GrantedAccess::Read)]),
        )]),
    };
    let cert = read_private(&launch.certificate, MAX_INPUT)?;
    let key = Zeroizing::new(read_private(&launch.private_key, MAX_INPUT)?);
    let tls = from_pem_identity(&cert, &key)?;
    drop(key);

    let multi = MultiRuntime::production_follower(
        &mut config, memory.clone(), launch.assignment.identity(),
    ).await?;
    let stopped = async {
        let (handle, driver) = ServerDriver::open_with_multi_follower(
            config, limit, memory, &multi, launch.assignment.endpoint, tls,
        ).await?;
        launch.assignment.endpoint = handle.addresses().flight_sql.expect("bound follower listener");
        let placement = Placement { slot: launch.slot, assignment: launch.assignment };
        let detach = match start_control(placement) {
            Ok(detach) => detach,
            Err(error) => return close_unstarted(driver, Err(error)).await,
        };
        let stop = CancellationToken::new();
        let mut detached = None;
        run_server(&handle, driver, async {
            tokio::select! {
                result = shutdown_signal() => result,
                result = multi.drive(handle.node(), &stop) => result,
                result = detach => {
                    detached = Some(result.map_err(signal_error)??.assignment);
                    Ok(())
                }
            }
        }).await?;
        Ok(detached)
    }.await;
    multi.close().await;
    if let Some(assignment) = stopped? {
        write_json(&serde_json::json!({ "assignment": assignment }))?;
    }
    Ok(())
}

fn start_control(placement: Placement) -> Result<oneshot::Receiver<Result<Placement>>> {
    write_json(&placement)?;
    let (send, receive) = oneshot::channel();
    std::thread::Builder::new().name("rtdb-follower-stdin".into()).spawn(move || {
        let result = read_detach(&mut io::stdin().lock(), &placement, || {
            write_json(&serde_json::json!({ "error": "rtdb/follower-assignment" }))
        });
        let _ = send.send(result.map(|()| placement));
    }).map_err(signal_error)?;
    Ok(receive)
}

fn read_detach(input: &mut impl BufRead, expected: &Placement, mut refuse: impl FnMut() -> Result<()>) -> Result<()> {
    let mut line = Vec::new();
    loop {
        line.clear();
        (&mut *input).take((MAX_INPUT + 1) as u64)
            .read_until(b'\n', &mut line).map_err(signal_error)?;
        if line.len() > MAX_INPUT || line.last() != Some(&b'\n') {
            return Err(invalid("The follower control pipe closed or sent an invalid frame"));
        }
        if serde_json::from_slice::<Placement>(&line).is_ok_and(|received| &received == expected) {
            return Ok(());
        }
        refuse()?;
    }
}

fn write_json(value: &impl Serialize) -> Result<()> {
    write_report(io::stdout().lock(), |output| {
        serde_json::to_writer(&mut *output, value).map_err(io::Error::other)?;
        writeln!(output)
    })
}

fn read_private(path: &Path, max: usize) -> Result<Vec<u8>> {
    if !path.is_absolute() || !fs::symlink_metadata(path).map_err(signal_error)?.is_file() {
        return Err(invalid("Native follower inputs require absolute paths to regular files"));
    }
    let file = File::open(path).map_err(signal_error)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if file.metadata().map_err(signal_error)?.permissions().mode() & 0o077 != 0 {
            return Err(invalid("Native follower inputs require owner-only permissions"));
        }
    }
    let mut bytes = Vec::new();
    file.take((max + 1) as u64).read_to_end(&mut bytes).map_err(signal_error)?;
    if bytes.len() > max {
        return Err(invalid("Native follower input exceeds its size limit"));
    }
    Ok(bytes)
}

fn invalid(message: &str) -> Anomaly {
    Anomaly::incorrect("rtdb/follower-configuration", message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn placement() -> Placement {
        serde_json::from_value(serde_json::json!({
            "slot": "pool-a",
            "assignment": {
                "database": "app",
                "incarnation": Uuid::from_u128(1),
                "placement_epoch": 42,
                "assignment": Uuid::from_u128(2),
                "endpoint": "127.0.0.1:3000"
            }
        })).unwrap()
    }

    #[test]
    fn detach_compares_the_slot_and_every_assignment_coordinate() {
        let placement = placement();
        let exact = serde_json::to_value(&placement).unwrap();
        let mismatches = [
            ("/slot", serde_json::json!("pool-b")),
            ("/assignment/database", serde_json::json!("other")),
            ("/assignment/incarnation", serde_json::json!(Uuid::from_u128(3))),
            ("/assignment/placement_epoch", serde_json::json!(41)),
            ("/assignment/assignment", serde_json::json!(Uuid::from_u128(4))),
            ("/assignment/endpoint", serde_json::json!("127.0.0.1:3001")),
        ];
        let mut input = Vec::new();
        for (path, replacement) in &mismatches {
            let mut wrong = exact.clone();
            *wrong.pointer_mut(path).unwrap() = replacement.clone();
            writeln!(input, "{wrong}").unwrap();
        }
        writeln!(input, "{exact}").unwrap();
        let mut refused = 0;
        read_detach(&mut input.as_slice(), &placement, || {
            refused += 1;
            Ok(())
        }).unwrap();
        assert_eq!(refused, mismatches.len());
    }

    #[test]
    fn control_pipe_eof_does_not_authorize_detach() {
        assert!(read_detach(&mut &b""[..], &placement(), || Ok(())).is_err());
    }
}
