use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::Read,
    os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{UnixListener, UnixStream},
    signal::unix::{SignalKind, signal},
    task::JoinSet,
    time::timeout,
};
use uuid::Uuid;

use super::{Access, ActivationAuthority, Attestors, Destination, DestinationReader, JournalReaders, Request, Source};

use super::super::ranked::Providers;

const MAX_FRAME_BYTES: usize = 16 * 1024;
const MAX_CONNECTIONS: usize = 16;
const IO_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    authority_id: Uuid,
    authority_url: String,
    authority_tls_dir: PathBuf,
    operator_uids: BTreeSet<u32>,
    destination: Option<Destination>,
    source: Option<Source>,
    #[serde(default)]
    destination_attestors: Attestors,
    #[serde(default)]
    destination_journal_readers: JournalReaders,
    #[serde(default)]
    ranked_providers: Providers,
}

impl Config {
    fn read(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)
            .map_err(io_error)?;
        require_private_file(&file)?;
        let mut bytes = Vec::new();
        file.take((MAX_FRAME_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(io_error)?;
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(configuration());
        }
        let config: Self = serde_json::from_slice(&bytes).map_err(|_| configuration())?;
        if config.operator_uids.is_empty()
            || config.authority_id.is_nil()
            || config
                .destination_attestors
                .iter()
                .any(|(cell, key)| cell.is_nil() || *key == [0; 32])
            || config
                .destination_attestors
                .values()
                .collect::<BTreeSet<_>>()
                .len()
                != config.destination_attestors.len()
            || !config.authority_url.starts_with(super::CONSENSUS_SCHEME)
            || config.destination_journal_readers.iter().any(|(cell, reader)| {
                !config.destination_attestors.contains_key(cell)
                    || !reader.journal_url.starts_with(super::CONSENSUS_SCHEME)
                    || !reader.journal_tls_dir.is_absolute()
            })
            || !config.authority_tls_dir.is_absolute()
            || config.destination.as_ref().is_some_and(|destination| !absolute_location(destination))
            || config.source.as_ref().is_some_and(|source| {
                source.incarnation.is_nil() || source.cell.is_some_and(|cell| cell.is_nil())
                    || !absolute_location(&source.location)
            })
        {
            return Err(configuration());
        }
        config.ranked_providers.validate()?;
        Ok(config)
    }
}

fn absolute_location(location: &Destination) -> bool {
    location.data.is_absolute()
        && location.journal_url.starts_with(super::CONSENSUS_SCHEME)
        && location.journal_tls_dir.is_absolute()
}

pub(super) async fn serve(
    path: &Path,
    configuration: &Path,
    apple_sync: rtdb_storage::AppleSync,
) -> Result<()> {
    let config = Arc::new(Config::read(configuration)?);
    require_directory(&config.authority_tls_dir, 0o022)?;
    if let Some(destination) = &config.destination {
        super::super::require_independent_journals(
            &config.authority_url,
            &destination.journal_url,
        )?;
    }
    if let Some(source) = &config.source {
        super::super::require_independent_journals(
            &config.authority_url,
            &source.location.journal_url,
        )?;
    }
    for reader in config.destination_journal_readers.values() {
        require_directory(&reader.journal_tls_dir, 0o022)?;
        super::super::require_independent_journals(&config.authority_url, &reader.journal_url)?;
    }
    let authority = ActivationAuthority::connect(
        &config.authority_url,
        crate::cli::journal_credentials(&config.authority_tls_dir)?,
        config.authority_id,
        Access::ConsensusOperator,
    )
    .await?;
    let mut interrupt = signal(SignalKind::interrupt()).map_err(io_error)?;
    let mut terminate = signal(SignalKind::terminate()).map_err(io_error)?;
    let socket = BoundSocket::bind(path)?;
    let mut requests = JoinSet::new();
    loop {
        tokio::select! {
            _ = interrupt.recv() => break,
            _ = terminate.recv() => break,
            _ = requests.join_next(), if !requests.is_empty() => (),
            accepted = socket.listener.accept(), if requests.len() < MAX_CONNECTIONS => {
                let (stream, _) = accepted.map_err(io_error)?;
                let authority = authority.clone();
                let config = config.clone();
                requests.spawn(async move {
                    handle_peer(
                        stream, &authority, &config.operator_uids, config.destination.as_ref(),
                        config.source.as_ref(), &config.destination_attestors,
                        &config.destination_journal_readers, &config.ranked_providers, apple_sync,
                    )
                    .await;
                });
            }
        }
    }
    requests.shutdown().await;
    Ok(())
}

async fn handle_peer(
    mut stream: UnixStream,
    authority: &ActivationAuthority,
    operator_uids: &BTreeSet<u32>,
    destination: Option<&Destination>,
    source: Option<&Source>,
    attestors: &Attestors,
    journals: &JournalReaders,
    ranked: &Providers,
    apple_sync: rtdb_storage::AppleSync,
) {
    let result: Result<Value> = if stream
        .peer_cred()
        .is_ok_and(|peer| operator_uids.contains(&peer.uid()))
    {
        match timeout(IO_TIMEOUT, read_frame::<Request>(&mut stream)).await {
            Ok(Ok(request)) => {
                request
                    .execute(
                        authority,
                        destination.map(DestinationReader::Broker),
                        source,
                        attestors,
                        journals,
                        ranked,
                        apple_sync,
                    )
                    .await
            }
            Ok(Err(error)) => Err(error),
            Err(_) => Err(protocol()),
        }
    } else {
        Err(Anomaly::new(
            AnomalyCategory::Forbidden,
            "rtdb/activation-broker-operator",
            "The Unix peer is not an activation operator",
        ))
    };
    let _ = timeout(IO_TIMEOUT, write_frame(&mut stream, &result)).await;
}

pub(super) fn validate_destination(destination: &Destination) -> Result<()> {
    require_directory(&destination.data, 0o022)?;
    require_directory(&destination.journal_tls_dir, 0o022)
}

pub(super) async fn call(path: &Path, request: Request) -> Result<Value> {
    let mut stream = UnixStream::connect(path).await.map_err(io_error)?;
    timeout(IO_TIMEOUT, write_frame(&mut stream, &request))
        .await
        .map_err(|_| protocol())??;
    read_frame::<Result<Value>>(&mut stream).await?
}

async fn read_frame<T: DeserializeOwned>(stream: &mut UnixStream) -> Result<T> {
    let length = stream.read_u32().await.map_err(io_error)? as usize;
    if !(1..=MAX_FRAME_BYTES).contains(&length) {
        return Err(protocol());
    }
    let mut body = vec![0; length];
    stream.read_exact(&mut body).await.map_err(io_error)?;
    serde_json::from_slice(&body).map_err(|_| protocol())
}

async fn write_frame<T: Serialize>(stream: &mut UnixStream, value: &T) -> Result<()> {
    let body = serde_json::to_vec(value).map_err(|_| protocol())?;
    if body.len() > MAX_FRAME_BYTES {
        return Err(protocol());
    }
    stream
        .write_u32(body.len() as u32)
        .await
        .map_err(io_error)?;
    stream.write_all(&body).await.map_err(io_error)
}

struct BoundSocket {
    listener: UnixListener,
    path: PathBuf,
    _lock: File,
}

impl BoundSocket {
    fn bind(path: &Path) -> Result<Self> {
        let parent = path
            .parent()
            .filter(|_| path.is_absolute())
            .ok_or_else(configuration)?;
        require_directory(parent, 0o027)?;
        let mut lock_path = path.as_os_str().to_owned();
        lock_path.push(".lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(lock_path)
            .map_err(io_error)?;
        require_private_file(&lock)?;
        fs2::FileExt::try_lock_exclusive(&lock).map_err(io_error)?;
        match fs::symlink_metadata(path) {
            Ok(metadata)
                if metadata.file_type().is_socket() && metadata.uid() == effective_uid() =>
            {
                fs::remove_file(path).map_err(io_error)?;
            }
            Ok(_) => return Err(configuration()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
            Err(error) => return Err(io_error(error)),
        }
        let bound = Self {
            listener: UnixListener::bind(path).map_err(io_error)?,
            path: path.to_owned(),
            _lock: lock,
        };
        fs::set_permissions(path, fs::Permissions::from_mode(0o660)).map_err(io_error)?;
        Ok(bound)
    }
}

impl Drop for BoundSocket {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub(super) fn effective_uid() -> u32 {
    // geteuid reads the current process credential and takes no arguments.
    unsafe { libc::geteuid() }
}

fn require_directory(path: &Path, disallowed_permissions: u32) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if metadata.is_dir()
        && metadata.uid() == effective_uid()
        && metadata.mode() & disallowed_permissions == 0
    {
        Ok(())
    } else {
        Err(configuration())
    }
}

fn require_private_file(file: &File) -> Result<()> {
    let metadata = file.metadata().map_err(io_error)?;
    if metadata.is_file() && metadata.uid() == effective_uid() && metadata.mode() & 0o077 == 0 {
        Ok(())
    } else {
        Err(configuration())
    }
}

fn configuration() -> Anomaly {
    Anomaly::incorrect(
        "rtdb/activation-broker-configuration",
        "Activation requires a broker-owned private configuration and a protected Unix socket directory",
    )
}

fn protocol() -> Anomaly {
    Anomaly::incorrect(
        "rtdb/activation-broker-protocol",
        "Invalid activation broker frame",
    )
}

fn io_error(error: std::io::Error) -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/activation-broker-io",
        error.to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::super::Action;
    use super::*;

    #[tokio::test]
    async fn only_authenticated_local_operators_can_mutate() {
        let directory = tempfile::tempdir().unwrap();
        let authority =
            ActivationAuthority::open_local(&directory.path().join("authority"), Uuid::new_v4())
                .unwrap();
        let incarnation = Uuid::new_v4();
        let request = Request {
            database: "xtdb".into(),
            action: Action::Bootstrap { incarnation },
        };
        for permitted in [false, true] {
            let requested = request.clone();
            let (mut client, server) = UnixStream::pair().unwrap();
            let allowed = if permitted {
                BTreeSet::from([effective_uid()])
            } else {
                BTreeSet::new()
            };
            let authority = authority.clone();
            let job = tokio::spawn(async move {
                handle_peer(
                    server,
                    &authority,
                    &allowed,
                    None,
                    None,
                    &Attestors::new(),
                    &JournalReaders::new(),
                    &Providers::default(),
                    rtdb_storage::AppleSync::default(),
                )
                .await;
            });
            // Unauthenticated connections are rejected before a request is read.
            if permitted {
                write_frame(&mut client, &requested).await.unwrap();
            }
            let result = read_frame::<Result<Value>>(&mut client).await.unwrap();
            if permitted {
                assert_eq!(result.unwrap()["incarnation"], incarnation.to_string());
            } else {
                assert_eq!(result.unwrap_err().code, "rtdb/activation-broker-operator");
            }
            job.await.unwrap();
        }
        assert_eq!(
            authority.binding("xtdb").await.unwrap().incarnation,
            incarnation
        );
    }

    #[test]
    fn wire_publication_accepts_no_destination_paths_from_the_caller() {
        let mut request = serde_json::json!({
            "database": "xtdb",
            "action": {
                "Publish": {
                    "expected": { "incarnation": Uuid::new_v4(), "revision": 1 },
                    "operation_id": Uuid::new_v4(),
                }
            }
        });
        assert!(serde_json::from_value::<Request>(request.clone()).is_ok());
        request["action"]["Publish"]["data"] = serde_json::json!("/arbitrary");
        assert!(serde_json::from_value::<Request>(request).is_err());
    }

    #[tokio::test]
    async fn socket_is_exclusive_and_reclaims_only_stale_sockets() {
        let directory = tempfile::tempdir().unwrap();
        fs::set_permissions(directory.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let path = directory.path().join("activation.sock");
        let owned = BoundSocket::bind(&path).unwrap();
        assert!(BoundSocket::bind(&path).is_err());
        drop(owned);
        assert!(!path.exists());

        drop(UnixListener::bind(&path).unwrap());
        let restarted = BoundSocket::bind(&path).unwrap();
        drop(restarted);
        fs::write(&path, b"keep").unwrap();
        assert!(BoundSocket::bind(&path).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"keep");
    }
}

#[cfg(test)]
pub(super) async fn call_test_peer(
    authority: &ActivationAuthority,
    attestors: &Attestors,
    request: &Request,
) -> Result<Value> {
    let (mut client, server) = UnixStream::pair().unwrap();
    let allowed = BTreeSet::from([effective_uid()]);
    let journals = JournalReaders::new();
    let ranked = Providers::default();
    let (_, result) = tokio::join!(
        handle_peer(
            server,
            authority,
            &allowed,
            None,
            None,
            attestors,
            &journals,
            &ranked,
            rtdb_storage::AppleSync::default(),
        ),
        async {
            write_frame(&mut client, request).await?;
            read_frame::<Result<Value>>(&mut client).await?
        },
    );
    result
}
