//! One local deployment using the same directory, log and gateway as a cell.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Write as _};
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use rtdb_directory::{Directory, Lifecycle, MemberId, MemberPool, MemberRole, MemoryDirectory};
use rtdb_flight_sql::HostedControlConfig;
use rtdb_logkeeper::{LocalListener, PreparedJournal, ServerOptions};
use rtdb_server::config::{FlightSettings, NodeConfiguration, parse_custody_provider_config};
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_rustls::rustls::{
    self,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

use super::{NodeSettings, io, start_node};
use crate::admin::installation_record;
use crate::agent::AgentOptions;
use crate::capability::CellSecret;
use crate::deployment::CellStorage;
use crate::flight_gateway;
use crate::gateway::{self, GatewayOptions};
use crate::keys::{KeyService, LocalKeyService};
use crate::lease::{LeaseOptions, MemberLease, Registration};

pub const STANDALONE_PROTOCOL: u32 = 2;
const AUTHORITY_FORMAT: u32 = 2;
const AUTHORITY: &str = "authority.json";
const DIRECTORY: &str = "directory";
const JOURNAL: &str = "journal";
const KEYS: &str = "keys";
const OBJECTS: &str = "objects";
const REPLAY_OBJECTS: &str = "replay-objects";
const REPLAY_KEYS: &str = "replay-keys";
const CACHE: &str = "cache";
const LOOPBACK: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0));

#[derive(Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Authority {
    format: u32,
    database: String,
    installation: Uuid,
    journal: MemberId,
    key_digest: String,
}

/// Creates the authority in an empty, caller-owned staging directory.
/// The caller publishes the directory only after initialization succeeds.
pub async fn initialize_standalone(root: &Path, name: &str, id: Uuid) -> Result<()> {
    initialize_standalone_with_sync(root, name, id, rtdb_storage::AppleSync::Full).await
}

pub async fn initialize_standalone_with_sync(
    root: &Path,
    name: &str,
    id: Uuid,
    apple_sync: rtdb_storage::AppleSync,
) -> Result<()> {
    private_directory(root)?;
    if fs::read_dir(root).map_err(io)?.next().is_some() {
        return Err(invalid(
            "Standalone RTDB initialization requires an empty directory",
        ));
    }

    let mut record = installation_record(id, name, "standalone")?;
    for directory in [DIRECTORY, JOURNAL, KEYS, OBJECTS, REPLAY_OBJECTS, REPLAY_KEYS] {
        private_mkdir(&root.join(directory))?;
    }

    let (journal, journal_id) = PreparedJournal::open_for_member(journal_options(root, apple_sync)).await?;
    let journal_id = MemberId(journal_id);
    record.ensemble = vec![journal_id.clone()];

    let directory_path = root.join(DIRECTORY);
    MemoryDirectory::initialize(&directory_path)?;
    let directory = MemoryDirectory::open(&directory_path)?;
    let keys = LocalKeyService::new(root.join(KEYS))?;
    let key = keys.key(&record.key_id()).await?;
    let authority = Authority {
        format: AUTHORITY_FORMAT,
        database: record.name.clone(),
        installation: id,
        journal: journal_id,
        key_digest: digest(&key),
    };
    directory.create_deployment(record).await?;
    write_private_with_sync(&root.join(AUTHORITY), &authority, Some(apple_sync))?;
    rtdb_storage::sync_directory(root, apple_sync).map_err(io)?;
    drop(journal);
    Ok(())
}

pub struct StandaloneSettings {
    pub root: PathBuf,
    pub name: String,
    pub id: Uuid,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub password_file: PathBuf,
    pub ready_file: PathBuf,
    pub owned_flight: bool,
    pub flight_control_relations: Option<BTreeSet<String>>,
    pub custody_provider_config: Option<PathBuf>,
}

#[derive(Serialize)]
struct Ready<'a> {
    format: u32,
    database: &'a str,
    installation: Uuid,
    host: std::net::IpAddr,
    port: u16,
    #[serde(skip_serializing_if = "Option::is_none")]
    owned_flight_port: Option<u16>,
}

pub async fn serve_standalone(
    settings: StandaloneSettings,
    cancellation: CancellationToken,
) -> Result<()> {
    serve_standalone_with_sync(settings, rtdb_storage::AppleSync::Full, cancellation).await
}

pub async fn serve_standalone_with_sync(
    settings: StandaloneSettings,
    apple_sync: rtdb_storage::AppleSync,
    cancellation: CancellationToken,
) -> Result<()> {
    private_directory(&settings.root)?;
    let authority = read_authority(&settings.root, &settings.name, settings.id)?;
    let directory: Arc<dyn Directory> = MemoryDirectory::open(&settings.root.join(DIRECTORY))?;
    let record = directory
        .deployment_by_name(&authority.database)
        .await?
        .ok_or_else(|| invalid("The original standalone RTDB deployment is missing"))?;
    if record.id != authority.installation.to_string()
        || record.lineage != record.id
        || record.org != "standalone"
        || record.lifecycle != Lifecycle::Live
        || record.parent.is_some()
        || !record.linearizable_reads
        || record.ensemble.as_slice() != [authority.journal.clone()]
    {
        return Err(invalid(
            "The stored deployment no longer matches its standalone RTDB authority",
        ));
    }

    let keys: Arc<dyn KeyService> = Arc::new(LocalKeyService::new(settings.root.join(KEYS))?);
    let key = keys
        .existing_key(&record.key_id())
        .await?
        .ok_or_else(|| invalid("The original standalone RTDB key is missing"))?;
    if authority.key_digest != digest(&key) {
        return Err(invalid(
            "The standalone RTDB key belongs to another authority",
        ));
    }

    let password = read_bounded(&settings.password_file, 64)?;
    if password.len() != 64 || !password.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid(
            "The standalone RTDB password must contain 64 hexadecimal digits",
        ));
    }
    let mut configuration = NodeConfiguration::from_yaml(&format!(
        "server:\n  host: 127.0.0.1\n  port: 0\nauthn: !SingleRootUser\n  password: \"{password}\"\n"
    ))?;
    configuration.apple_sync = apple_sync;
    if let Some(flight) = owned_flight_settings(
        settings.owned_flight, settings.flight_control_relations,
        settings.custody_provider_config.as_deref(), &settings.name,
    )? {
        configuration.flight_sql = Some(flight);
    }
    let tls = gateway_tls(&settings.certificate, &settings.private_key)?;
    let gateway = TcpListener::bind(LOOPBACK).await.map_err(io)?;
    let address = gateway.local_addr().map_err(io)?;
    let flight = if settings.owned_flight {
        Some(TcpListener::bind(LOOPBACK).await.map_err(io)?)
    } else {
        None
    };
    let owned_flight_port = flight.as_ref().map(|listener| {
        listener.local_addr().map(|address| address.port()).map_err(io)
    }).transpose()?;
    let capabilities = Arc::new(CellSecret::generate()?);

    let journal = PreparedJournal::reopen_member(
        journal_options(&settings.root, apple_sync),
        authority.journal.0.clone(),
    )
    .await?;
    let journal_address = format!("local://{}", authority.journal);
    let listener = LocalListener::bind(&journal_address).map_err(io)?;
    let lease = MemberLease::reclaim_logkeeper(
        Arc::clone(&directory),
        Registration {
            engine_version: super::ENGINE_VERSION.to_owned(),
            role: MemberRole::Logkeeper,
            address: journal_address,
            control_address: None,
            availability_zone: "local".into(),
            pool: MemberPool::General,
            storage_namespaces: None,
        },
        authority.journal,
        true,
        LeaseOptions::default(),
    )
    .await?;

    let journal_stop = CancellationToken::new();
    let journal_task = tokio::spawn(journal.serve_local(listener, journal_stop.clone()));
    let lease_task = tokio::spawn(Arc::clone(&lease).run(journal_stop.clone()));
    let broker_tasks = TaskTracker::new();
    let outcome = async {
        let storage = CellStorage::Local {
            root: settings.root.join(OBJECTS),
        };
        let payload_root = settings.root.join(REPLAY_OBJECTS);
        let key_root = settings.root.join(REPLAY_KEYS);
        private_directory(&payload_root)?;
        private_directory(&key_root)?;
        let mut agent = AgentOptions::new(storage.clone());
        agent.replay_broker = Some(crate::replay_broker::Client::standalone(
            Arc::clone(&directory), CellStorage::Local { root: payload_root }, &storage,
            Arc::new(LocalKeyService::new(key_root)?), broker_tasks.clone(),
        )?);
        let mut node = start_node(
            Arc::clone(&directory),
            NodeSettings {
                engine_version: None,
                configuration,
                allocation_limit_bytes: 1 << 30,
                control: LOOPBACK,
                hosted_task: None,
                advertise_host: None,
                availability_zone: "local".into(),
                agent,
                lease: LeaseOptions::default(),
                pool: MemberPool::General,
                claim_wait: Duration::from_millis(20),
                keys: Arc::clone(&keys),
                cache_dir: settings.root.join(CACHE),
                local_compaction: true,
                capabilities: Some(Arc::clone(&capabilities)),
                classic_retained_flight_key: None,
            },
        )
        .await?;
        let reaper = tokio::spawn(crate::store::run_reaper(
            Arc::clone(&directory),
            storage,
            Some(keys),
            None,
            Duration::from_secs(10),
            cancellation.clone(),
        ));
        let serving = async {
            if cancellation.is_cancelled() { return Ok(()); }
            write_private(&settings.ready_file, &Ready {
                format: STANDALONE_PROTOCOL, database: &settings.name, installation: settings.id,
                host: address.ip(), port: address.port(), owned_flight_port,
            }, false)?;
            let options = GatewayOptions { tls: Some(tls), capabilities: Some(capabilities), ..Default::default() };
            let journal_lost = lease.lost();
            let flight = async {
                match flight {
                    Some(listener) => flight_gateway::serve(
                        listener, Arc::clone(&directory), options.clone(), cancellation.clone(),
                    ).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                result = gateway::serve(gateway, Arc::clone(&directory), options.clone(), cancellation.clone()) => result,
                result = flight => result,
                _ = journal_lost.cancelled() => Err(invalid("The local RTDB journal lost its membership")),
                _ = node.tasks.join_next() => Err(invalid("A standalone RTDB node task stopped")),
            }
        }.await;
        cancellation.cancel();
        let reaped = reaper.await.map_err(|error| invalid(error.to_string()));
        let shutdown = node.shutdown().await;
        serving.and(reaped).and(shutdown)
    };

    let mut journal_task = journal_task;
    let mut outcome = std::pin::pin!(outcome);
    let result = tokio::select! {
        result = &mut outcome => {
            journal_stop.cancel();
            result.and(journal_task.await.map_err(|error| invalid(error.to_string())).and_then(|result| result))
        }
        journal_result = &mut journal_task => {
            cancellation.cancel();
            let _ = outcome.await;
            journal_result.map_err(|error| invalid(error.to_string()))
                .and_then(|result| result)
                .and(Err(invalid("The standalone RTDB journal stopped")))
        }
    };
    journal_stop.cancel();
    broker_tasks.close();
    broker_tasks.wait().await;
    let _ = lease_task.await;
    result.and(lease.retire().await)
}

fn owned_flight_settings(
    enabled: bool,
    relations: Option<BTreeSet<String>>,
    provider_config: Option<&Path>,
    database: &str,
) -> Result<Option<FlightSettings>> {
    if provider_config.is_some() && (!enabled || relations.is_none()) {
        return Err(invalid("A custody provider requires the installed Flight control owner"));
    }
    if !enabled {
        if relations.is_some() {
            return Err(invalid("Flight control relations require --owned-flight"));
        }
        return Ok(None);
    }
    let custody_providers = if let Some(path) = provider_config {
        let providers = parse_custody_provider_config(&read_bounded(path, 64 * 1024)?)?;
        if providers.len() != 1 || providers[0].database != database {
            return Err(invalid("A standalone custody provider must identify the original database"));
        }
        providers
    } else {
        Vec::new()
    };
    Ok(Some(FlightSettings {
        owned_native_actions: true,
        store_only: true,
        hosted_controls: relations.map(|relations| HostedControlConfig {
            owner: "xtdb".to_owned(),
            relations,
        }),
        custody_providers,
        ..FlightSettings::default()
    }))
}

fn read_authority(root: &Path, name: &str, id: Uuid) -> Result<Authority> {
    for directory in [DIRECTORY, JOURNAL, KEYS, OBJECTS] {
        private_directory(&root.join(directory))?;
    }
    let raw = read_bounded(&root.join(AUTHORITY), 4096)?;
    let authority: Authority =
        serde_json::from_str(&raw).map_err(|error| invalid(error.to_string()))?;
    if authority.format != AUTHORITY_FORMAT || authority.database != name || authority.installation != id {
        return Err(invalid(
            "The retained standalone RTDB authority identifies another deployment",
        ));
    }
    Ok(authority)
}

fn journal_options(root: &Path, apple_sync: rtdb_storage::AppleSync) -> ServerOptions {
    ServerOptions::new(root.join(JOURNAL), true, ServerOptions::DEFAULT_CACHE_BYTES)
        .with_apple_sync(apple_sync)
}

fn digest(key: &[u8; 32]) -> String {
    base64::engine::general_purpose::STANDARD
        .encode(ring::digest::digest(&ring::digest::SHA256, key))
}

pub(crate) fn gateway_tls(certificate: &Path, key: &Path) -> Result<Arc<rustls::ServerConfig>> {
    let chain = CertificateDer::pem_file_iter(certificate)
        .map_err(|error| invalid(error.to_string()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| invalid(error.to_string()))?;
    let key = PrivateKeyDer::from_pem_file(key).map_err(|error| invalid(error.to_string()))?;
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|error| invalid(error.to_string()))?
    .with_no_client_auth()
    .with_single_cert(chain, key)
    .map_err(|error| invalid(error.to_string()))?;
    Ok(Arc::new(config))
}

pub(crate) fn read_bounded(path: &Path, maximum: usize) -> Result<String> {
    if !fs::symlink_metadata(path).map_err(io)?.is_file() {
        return Err(invalid(
            "The standalone RTDB configuration must be a regular file",
        ));
    }
    let input = File::open(path).map_err(io)?;
    let mut text = String::new();
    input
        .take((maximum + 1) as u64)
        .read_to_string(&mut text)
        .map_err(io)?;
    if text.len() > maximum {
        return Err(invalid("The standalone RTDB configuration is too large"));
    }
    Ok(text)
}

pub(crate) fn write_private(path: &Path, value: &impl Serialize, durable: bool) -> Result<()> {
    write_private_with_sync(path, value, durable.then_some(rtdb_storage::AppleSync::Full))
}

fn write_private_with_sync(
    path: &Path,
    value: &impl Serialize,
    durable: Option<rtdb_storage::AppleSync>,
) -> Result<()> {
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let mut file = options.open(path).map_err(io)?;
    let json = serde_json::to_vec(value).map_err(|error| invalid(error.to_string()))?;
    file.write_all(&json).map_err(io)?;
    if let Some(apple_sync) = durable {
        rtdb_storage::sync_file(&file, apple_sync).map_err(io)?;
    }
    Ok(())
}

fn private_mkdir(path: &Path) -> Result<()> {
    let mut options = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt as _;
        options.mode(0o700);
    }
    options.create(path).map_err(io)
}

fn private_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(io)?;
    if !metadata.is_dir() {
        return Err(invalid(
            "The standalone RTDB authority requires an original directory",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(invalid("The standalone RTDB directory must be private"));
        }
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> Anomaly {
    Anomaly::incorrect("rtdb.multi/standalone", message.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standalone_owned_flight_requires_opt_in_and_scopes_control_relations() {
        let relations = BTreeSet::from(["bj_installation".to_owned()]);
        assert!(owned_flight_settings(false, None, None, "xtdb").unwrap().is_none());
        assert!(owned_flight_settings(false, Some(relations.clone()), None, "xtdb").is_err());
        assert!(owned_flight_settings(true, None, Some(Path::new("unused")), "xtdb").is_err());
        let ordinary = owned_flight_settings(true, None, None, "xtdb").unwrap().unwrap();
        assert!(ordinary.owned_native_actions);
        assert!(ordinary.hosted_controls.is_none());
        let owned = owned_flight_settings(true, Some(relations.clone()), None, "xtdb").unwrap().unwrap();
        let controls = owned.hosted_controls.unwrap();
        assert_eq!(controls.owner, "xtdb");
        assert_eq!(controls.relations, relations);
    }

    #[tokio::test]
    async fn initialization_installs_the_original_key_and_journal_only_once() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("rt");
        private_mkdir(&root).unwrap();
        let id = Uuid::new_v4();
        initialize_standalone(&root, "development", id)
            .await
            .unwrap();
        let first = read_authority(&root, "development", id).unwrap();
        assert!(
            initialize_standalone(&root, "development", Uuid::new_v4())
                .await
                .is_err()
        );
        assert!(read_authority(&root, "development", Uuid::new_v4()).is_err());
        let original = read_authority(&root, "development", id).unwrap();
        assert!(original == first);
        let directory = MemoryDirectory::open(&root.join(DIRECTORY)).unwrap();
        let record = directory
            .deployment_by_name("development")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.ensemble, vec![original.journal.clone()]);
        assert!(record.linearizable_reads);
        let key = LocalKeyService::new(root.join(KEYS))
            .unwrap()
            .existing_key(&record.key_id())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(original.key_digest, digest(&key));
        PreparedJournal::reopen_member(journal_options(&root), original.journal.0)
            .await
            .unwrap();
    }
}
