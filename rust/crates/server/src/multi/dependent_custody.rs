use std::{fs::File, io::Read, path::Path, sync::Arc, time::Duration};

use rtdb_types::{
    Anomaly, AnomalyCategory, Result,
    rtdb_v1::{
        Basis, NativeCustodyFence, NativeDependentCheckpointMetadata, NativeDependentCustodyScope,
    },
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use super::MultiRuntime;

mod client;
mod inherited;
mod ingress;
pub(crate) mod operator;
pub use inherited::{InheritedAdmission, InheritedAdmissionRegistration, InheritedCarrierBinding, InheritedDisposition, InheritedReadProof, InheritedRegistration, InheritedRelease};
pub(super) use ingress::Ingress;

const VERSION: u32 = 1;
const MAX_BYTES: usize = 1 << 20;
const MAX_CONFIG_BYTES: usize = 64 << 10;
const DEADLINE: Duration = Duration::from_secs(15);
const OBSERVE: &str = "/rtdb/native-dependent-custody/v1/observe";
const METADATA: &str = "/rtdb/native-dependent-custody/v1/metadata";
const ENROLLMENT_SOURCE: &str = "/rtdb/native-dependent-custody/v1/enrollment/source";
const INHERITED_READ: &str = "/rtdb/native-dependent-custody/v1/inherited/read";
const INHERITED_RELEASE: &str = "/rtdb/native-dependent-custody/v1/inherited/release";
const INHERITED_CARRIER: &str = "/rtdb/native-dependent-custody/v1/inherited/carrier";
const INHERITED_ADMIT: &str = "/rtdb/native-dependent-custody/v1/inherited/admit";
const INHERITED_DISPOSE: &str = "/rtdb/native-dependent-custody/v1/inherited/dispose";
const SETTLE: &str = "/rtdb/native-dependent-custody/v1/settle";
const STATUS: &str = "/rtdb/native-dependent-custody/v1/status";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Configuration {
    #[serde(default)]
    observers: Vec<client::ObserverProfile>,
    ingress: Option<ingress::Profile>,
}

pub(super) fn install(runtime: &mut MultiRuntime) -> Result<()> {
    let mounted_port = std::env::var_os("RTDB_NATIVE_DEPENDENT_CUSTODY_PORT");
    let Some(path) = std::env::var_os("RTDB_NATIVE_DEPENDENT_CUSTODY_FILE") else {
        return if mounted_port.is_none() { Ok(()) } else { Err(invalid()) };
    };
    let config: Configuration = read_json(Path::new(&path))?;
    let observers = client::Observer::from_profiles(config.observers)?;
    let ingress = config.ingress.map(Ingress::from_profile).transpose()?;
    if let Some(mounted_port) = mounted_port {
        let port = mounted_port
            .to_str()
            .and_then(|value| value.parse::<u16>().ok())
            .ok_or_else(invalid)?;
        if ingress.as_ref().map_or(0, Ingress::port) != port {
            return Err(invalid());
        }
    }
    if observers.is_empty() && ingress.is_none() {
        return Err(invalid());
    }
    if !observers.is_empty() {
        runtime.native_dependent_custody = Some(Arc::new(observers));
    }
    runtime.native_dependent_ingress = ingress.map(Arc::new);
    Ok(())
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Reply<T> {
    version: u32,
    request_sha256: [u8; 32],
    result: T,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataRequest {
    version: u32,
    scope: NativeDependentCustodyScope,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum MetadataResult {
    Pending,
    Ready {
        metadata: NativeDependentCheckpointMetadata,
    },
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SettlementRequest {
    version: u32,
    settlement: Settlement,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Settlement {
    destination: String,
    scope: NativeDependentCustodyScope,
    fence: NativeCustodyFence,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum SettlementResult {
    Pending { key: [u8; 32] },
    Settled { key: [u8; 32], basis: Basis },
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value).map_err(|_| refused())?;
    if bytes.len() > MAX_BYTES {
        return Err(refused());
    }
    Ok(bytes)
}

fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    serde_json::from_slice(&read_file(path)?).map_err(|_| invalid())
}

fn read_file(path: &Path) -> Result<Zeroizing<Vec<u8>>> {
    let mut bytes = Zeroizing::new(Vec::new());
    File::open(path)
        .and_then(|file| {
            file.take(MAX_CONFIG_BYTES as u64 + 1)
                .read_to_end(&mut bytes)
        })
        .map_err(|_| invalid())?;
    if bytes.is_empty() || bytes.len() > MAX_CONFIG_BYTES {
        return Err(invalid());
    }
    Ok(bytes)
}

fn invalid() -> Anomaly {
    Anomaly::incorrect(
        "rtdb/native-dependent-config",
        "Invalid private native dependent custody configuration",
    )
}

fn refused() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Conflict,
        "rtdb/native-dependent-evidence",
        "Native dependent custody evidence failed the exact private scope check",
    )
}

fn pending() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/native-dependent-pending",
        "Positive native dependent custody evidence is pending",
    )
}
