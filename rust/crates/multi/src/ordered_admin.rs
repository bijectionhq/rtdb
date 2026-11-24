use std::path::Path;

use ordered_logkeeper::ordered::{
    Command, Enrollment, Outcome,
    daemon::{ControlManifest, Manifest},
};
use rtdb_types::{Anomaly, Result};

use super::{Arguments, io, required_uuid, usage};

pub(super) async fn manifest(command: &str, arguments: &Arguments) -> Result<()> {
    let failure = |error: Box<dyn std::error::Error + Send + Sync>| {
        Anomaly::fault("rtdb.multi/ordered-journal", error.to_string())
    };
    let path = |name| arguments.required(name).map(Path::new);
    let manifest = Manifest::read(path("manifest")?).map_err(failure)?;
    let deployment = || required_uuid(arguments, "deployment");
    match command {
        "ordered-route" => {
            let route = manifest.application_route(deployment()?).map_err(failure)?;
            let document = serde_json::to_string_pretty(&route)
                .map_err(std::io::Error::other)
                .map_err(io)?;
            println!("{document}");
            Ok(())
        }
        "ordered-reserve-repair" => manifest
            .reserve_repair(deployment()?, path("output")?)
            .map_err(failure),
        "ordered-seal-repair" => {
            let tickets = arguments.required("tickets")?;
            if tickets.split(',').any(str::is_empty) {
                return Err(usage(
                    "--tickets requires a comma-separated list of file paths".into(),
                ));
            }
            let tickets: Vec<_> = tickets.split(',').map(Path::new).collect();
            manifest
                .seal_repair(path("destination")?, &tickets, path("output")?)
                .await
                .map_err(failure)
        }
        "ordered-install-repair" => manifest
            .install_repair(deployment()?, path("package")?)
            .map_err(failure),
        _ => unreachable!("Unknown ordered manifest command"),
    }
}

pub(super) async fn worker(command: &str, arguments: &Arguments) -> Result<()> {
    let path = Path::new(arguments.required("manifest")?);
    let worker = required_uuid(arguments, "worker")?;
    let enrollment = || -> Result<Enrollment> {
        let serial = arguments
            .required("serial")?
            .parse()
            .map_err(|_| usage("--serial must be an enrollment serial".into()))?;
        Ok(Enrollment { worker, serial })
    };
    let action = match command {
        "ordered-enroll" => Command::Enroll {
            worker,
            expected: arguments.present("serial").then(enrollment).transpose()?,
        },
        "ordered-withdraw" => Command::Withdraw {
            enrollment: enrollment()?,
        },
        "ordered-dispose-worker" => Command::DisposeWorker {
            enrollment: enrollment()?,
            disposition: disposition(arguments.required("disposition")?)?,
        },
        "ordered-dispose-expired-worker" => Command::DisposeExpired {
            enrollment: enrollment()?,
        },
        _ => unreachable!("Unknown ordered worker command"),
    };
    let code = if command == "ordered-dispose-expired-worker" {
        "rtdb.multi/ordered-dispose-expired-worker"
    } else {
        "rtdb.multi/ordered-worker-admin"
    };
    let outcome = ControlManifest::execute(path, action.clone())
        .await
        .map_err(|error| Anomaly::fault(code, error.to_string()))?;
    match (action, outcome) {
        (Command::Withdraw { .. }, Outcome::Withdrawn) => println!("withdrawn"),
        (Command::Enroll { worker, .. }, Outcome::Enrolled(enrolled))
            if enrolled.worker == worker =>
        {
            print_enrollment(enrolled)?
        }
        (
            Command::DisposeWorker { enrollment, .. } | Command::DisposeExpired { enrollment },
            Outcome::WorkerDisposed(disposed),
        ) if enrollment == disposed => print_enrollment(disposed)?,
        _ => {
            return Err(Anomaly::fault(
                code,
                "The journal returned a different administrative outcome",
            ));
        }
    }
    Ok(())
}

fn print_enrollment(enrollment: Enrollment) -> Result<()> {
    let json = serde_json::to_string(&enrollment)
        .map_err(std::io::Error::other)
        .map_err(io)?;
    println!("{json}");
    Ok(())
}

fn disposition(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(usage("--disposition must be a SHA-256 hex digest".into()));
    }
    let mut digest = [0; 32];
    for (index, byte) in digest.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[2 * index..2 * index + 2], 16)
            .expect("Validated hexadecimal pair");
    }
    Ok(digest)
}
