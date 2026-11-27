use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, Read, Write},
    net::Ipv4Addr,
    path::{Path, PathBuf},
    sync::Arc,
};

use rtdb_auth::{AuthConfig, Secret};
use rtdb_crypt::{DataKey, FileKms, Generation, Kms};
use rtdb_format::database::proto::{
    LocalLog, LocalStorage,
    database_config::{Log, Storage},
};
use rtdb_node::{
    Clock, NativeResources, SystemClock,
    serving::{NativeCheckpointAuthority, NativeServingAuthority},
};
use rtdb_replication::Role;
use rtdb_storage::{
    AppleSync, IoRegion,
    log::{
        AppendTimestampSource, LocalLogOptions, Log as _, ReadOnlyLocalLog, ReadOnlyLocalLogOptions,
    },
};
use rtdb_types::{
    Anomaly, Result, Timestamp,
    rtdb_v1::{Basis, Claim, ClaimEvidence, ClaimScope, Layer, MAIN_BRANCH},
};
use serde::{Deserialize, Serialize};
use tokio_rustls::rustls;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;
use zeroize::Zeroizing;

use crate::{
    ServerDriver, ServerHandle,
    config::{FlightSettings, NodeConfiguration, PgwireSettings, parse_custody_provider_config},
};

pub const PROTOCOL: u32 = 3;
const CLASSIC_FORMAT: u32 = 1;
const SINGLE_JOURNAL_FORMAT: u32 = 2;
const AUTHORITY: &str = "native-authority.json";
const LOGS: &str = "journal";
const SOURCE: &str = "LOG";
const REPLICA: &str = "REPLICA_LOG";
const OBJECTS: &str = "objects";
const TOKEN_KEYS: &str = "token-keys";
const CHECKPOINT_KEYS: &str = "checkpoint-keys";
const CHECKPOINT_GENERATION: u64 = 1;
const ALLOCATION_LIMIT: usize = 1 << 30;

#[derive(Clone, Debug)]
pub struct Target {
    pub state: PathBuf,
    pub name: String,
    pub id: Uuid,
}

#[derive(Clone, Debug)]
pub struct Launch {
    pub target: Target,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub password_file: PathBuf,
    pub ready_file: PathBuf,
    pub custody_provider_config: Option<PathBuf>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Authority {
    format: u32,
    database: String,
    installation: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_lineage: Option<Uuid>,
    replica_lineage: Uuid,
    token_witness: Claim,
    checkpoint_witness: Vec<u8>,
}

#[derive(Clone, Copy)]
enum JournalMode {
    Classic,
    Single,
}

impl JournalMode {
    fn format(self) -> u32 {
        match self {
            Self::Classic => CLASSIC_FORMAT,
            Self::Single => SINGLE_JOURNAL_FORMAT,
        }
    }
}

impl Authority {
    fn journal_mode(&self) -> Result<JournalMode> {
        match (self.format, self.source_lineage) {
            (CLASSIC_FORMAT, Some(_)) => Ok(JournalMode::Classic),
            (SINGLE_JOURNAL_FORMAT, None) => Ok(JournalMode::Single),
            _ => Err(invalid("Invalid native standalone journal authority")),
        }
    }
}

impl Target {
    pub fn validate(&self) -> Result<()> {
        let name = self.name.as_str();
        if self.id.is_nil()
            || name.is_empty()
            || name.len() > 128
            || matches!(name, "." | "..")
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(invalid(
                "Standalone requires an installation name and a nonzero UUID",
            ));
        }
        Ok(())
    }

    fn configuration(&self, apple_sync: AppleSync) -> Result<NodeConfiguration> {
        let path = |name| {
            self.state
                .join(name)
                .into_os_string()
                .into_string()
                .map_err(|_| invalid("Standalone paths must be UTF-8"))
        };
        let mut config = NodeConfiguration::from_yaml_with_environment("", &|_| None, false)?;
        config.apple_sync = apple_sync;
        config.primary.log = Some(Log::LocalLog(LocalLog {
            path: Some(path(LOGS)?),
            epoch: Some(0),
        }));
        config.primary.storage = Some(Storage::LocalStorage(LocalStorage {
            path: Some(path(OBJECTS)?),
            epoch: Some(0),
        }));
        config.primary.critical = Some(true);
        config.fixed_databases = Some(BTreeMap::from([(
            self.name.clone(),
            config.primary.clone(),
        )]));
        config.default_v1_serving = true;
        config.server = None;
        config.flight_sql = None;
        Ok(config)
    }

    fn resources(
        &self, tokens: FileKms, checkpoints: Arc<FileKms>, apple_sync: AppleSync, journal_mode: JournalMode,
    ) -> NativeResources {
        let resources = NativeResources::default()
            .with_apple_sync(apple_sync)
            .with_token_kms(Arc::new(tokens))
            .with_native_serving_authority(
                self.name.clone(),
                NativeServingAuthority {
                    incarnation: *self.id.as_bytes(),
                    checkpoints: Some(NativeCheckpointAuthority {
                        kms: checkpoints,
                        generation: CHECKPOINT_GENERATION,
                    }),
                },
            );
        match journal_mode {
            JournalMode::Classic => resources,
            JournalMode::Single => resources.with_single_journal(),
        }
    }

    fn generation(&self, number: u64) -> Generation {
        Generation {
            database: self.id,
            number,
        }
    }

    fn token_evidence(&self) -> ClaimEvidence {
        ClaimEvidence {
            scope: ClaimScope {
                branch: MAIN_BRANCH.to_owned(),
                basis: Basis::new(*self.id.as_bytes(), 0),
                layer: Layer::Merged,
            },
            footprint: Default::default(),
        }
    }

    fn checkpoint_challenge(&self) -> DataKey {
        let mut bytes = [0; 32];
        bytes[..16].copy_from_slice(self.id.as_bytes());
        bytes[16..].copy_from_slice(self.id.as_bytes());
        DataKey::new(bytes)
    }
}

pub async fn initialize(target: Target) -> Result<()> {
    initialize_with_sync(target, AppleSync::Full).await
}

pub async fn initialize_with_sync(target: Target, apple_sync: AppleSync) -> Result<()> {
    initialize_journal_mode(target, apple_sync, JournalMode::Single).await
}

pub async fn initialize_classic_with_sync(target: Target, apple_sync: AppleSync) -> Result<()> {
    initialize_journal_mode(target, apple_sync, JournalMode::Classic).await
}

async fn initialize_journal_mode(target: Target, apple_sync: AppleSync, journal_mode: JournalMode) -> Result<()> {
    target.validate()?;
    private_directory(&target.state)?;
    if fs::read_dir(&target.state)
        .map_err(io_error)?
        .next()
        .is_some()
    {
        return Err(invalid(
            "Standalone initialization requires an empty private staging directory",
        ));
    }
    let config = target.configuration(apple_sync)?;
    for name in [LOGS, OBJECTS, TOKEN_KEYS, CHECKPOINT_KEYS] {
        let mut directory = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            directory.mode(0o700);
        }
        directory
            .create(target.state.join(name))
            .map_err(io_error)?;
    }

    let source_lineage = match journal_mode {
        JournalMode::Classic => Some(initialize_journal(&target.state.join(LOGS).join(SOURCE), apple_sync).await?),
        JournalMode::Single => None,
    };
    let replica_lineage = initialize_journal(&target.state.join(LOGS).join(REPLICA), apple_sync).await?;
    let (tokens, checkpoints) = open_keys(&target.state)?;
    let resources = target.resources(tokens, Arc::clone(&checkpoints), apple_sync, journal_mode);
    let (handle, driver) =
        ServerDriver::open_installed(config, ALLOCATION_LIMIT, resources, None).await?;
    let witness = async {
        let database = handle.node().catalog().database(&target.name)?;
        database
            .token_authority()
            .await?
            .seal_claim(&target.token_evidence())
            .await
    }
    .await;
    let token_witness = after_close(witness, driver.close_without_run().await)?;
    let checkpoint_witness = checkpoints
        .wrap(
            target.generation(CHECKPOINT_GENERATION),
            &target.checkpoint_challenge(),
            checkpoint_context(&target, journal_mode).as_bytes(),
        )
        .await?;
    publish_json(
        &target.state.join(AUTHORITY),
        &Authority {
            format: journal_mode.format(),
            database: target.name,
            installation: target.id,
            source_lineage,
            replica_lineage,
            token_witness,
            checkpoint_witness,
        },
        Some(apple_sync),
    )
}

pub async fn serve(launch: Launch, stop: CancellationToken) -> Result<()> {
    serve_with_sync(launch, AppleSync::Full, stop).await
}

pub async fn serve_with_sync(launch: Launch, apple_sync: AppleSync, stop: CancellationToken) -> Result<()> {
    let target = &launch.target;
    target.validate()?;
    private_directory(&target.state)?;
    match target.state.join("authority.json").symlink_metadata() {
        Ok(_) => {
            return Err(invalid(
                "Classic standalone authority requires an explicit migration and cutover",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => (),
        Err(error) => return Err(io_error(error)),
    }
    let authority_file = target.state.join(AUTHORITY);
    let file = private_input(&authority_file)?;
    fs2::FileExt::try_lock_exclusive(&file).map_err(io_error)?;
    let authority: Authority =
        serde_json::from_slice(&read_bounded(file.try_clone().map_err(io_error)?, 4096)?)
            .map_err(|_| invalid("Invalid native standalone authority record"))?;
    if authority.database != target.name || authority.installation != target.id {
        return Err(invalid(
            "Native standalone authority belongs to another installation",
        ));
    }

    let journal_mode = authority.journal_mode()?;
    for name in [LOGS, OBJECTS, TOKEN_KEYS, CHECKPOINT_KEYS] {
        private_directory(&target.state.join(name))?;
    }
    private_directory(parent(&launch.ready_file))?;
    match launch.ready_file.symlink_metadata() {
        Err(error) if error.kind() == io::ErrorKind::NotFound => (),
        Err(error) => return Err(io_error(error)),
        Ok(_) => return Err(invalid("The standalone ready file already exists")),
    }
    let (tokens, checkpoints) = open_keys(&target.state)?;
    tokens.check_readable(target.generation(0)).await?;
    let checkpoint = checkpoints
        .unwrap(
            target.generation(CHECKPOINT_GENERATION),
            &authority.checkpoint_witness,
            checkpoint_context(target, journal_mode).as_bytes(),
        )
        .await?;
    if *checkpoint != *target.checkpoint_challenge() {
        return Err(invalid(
            "The original standalone checkpoint authority is missing",
        ));
    }

    let replica = target.state.join(LOGS).join(REPLICA);
    match journal_mode {
        JournalMode::Classic => verify_journal(&replica, authority.replica_lineage)?,
        JournalMode::Single => verify_single_journal(&replica, authority.replica_lineage, apple_sync)?,
    }
    if let Some(lineage) = authority.source_lineage {
        verify_journal(&target.state.join(LOGS).join(SOURCE), lineage)?;
    }

    let password = Zeroizing::new(read_bounded(private_input(&launch.password_file)?, 64)?);
    if password.len() != 64 || !password.iter().all(u8::is_ascii_hexdigit) {
        return Err(invalid(
            "The standalone password must contain 64 hexadecimal digits",
        ));
    }
    let password = Secret::new(std::str::from_utf8(&password).expect("validated ASCII"));
    let mut flight = FlightSettings::default();
    flight.store_only = true;
    if let Some(path) = &launch.custody_provider_config {
        let bytes = Zeroizing::new(read_bounded(private_input(path)?, 64 * 1024)?);
        let config = std::str::from_utf8(&bytes)
            .map_err(|_| invalid("Invalid custody provider configuration"))?;
        flight.custody_providers = parse_custody_provider_config(config)?;
        if flight.custody_providers.len() != 1
            || !flight.custody_providers[0].matches_installation(&target.name, target.id)
            || flight.custody_providers[0].owner.as_deref().is_some_and(|owner| owner != "xtdb")
        {
            return Err(invalid(
                "The custody provider must identify the original standalone installation",
            ));
        }
    }
    let tls = tls(&launch.certificate, &launch.private_key)?;
    if stop.is_cancelled() {
        return Ok(());
    }

    let mut config = target.configuration(apple_sync)?;
    config.auth = AuthConfig::SingleRootUser {
        password: Some(password),
    };
    config.server = Some(PgwireSettings::default());
    config.flight_sql = Some(flight);
    let (handle, driver) = ServerDriver::open_installed(
        config,
        ALLOCATION_LIMIT,
        target.resources(tokens, checkpoints, apple_sync, journal_mode),
        Some(tls),
    )
    .await?;
    let attachment = async {
        let database = handle.node().catalog().database(&target.name)?;
        if database.wire_incarnation().await? != target.id
            || database
                .token_authority()
                .await?
                .open_claim(&authority.token_witness)
                .await?
                != target.token_evidence()
        {
            return Err(invalid(
                "The token authority belongs to another installation",
            ));
        }
        Ok(database)
    }
    .await;
    let database = match attachment {
        Ok(database) => database,
        Err(error) => return after_close(Err(error), driver.close_without_run().await),
    };
    if stop.is_cancelled() {
        return driver.close_without_run().await;
    }

    let running = driver.run();
    tokio::pin!(running);
    let outcome = {
        let acknowledge = async {
            let Some(ready) = stop
                .run_until_cancelled(ready(&handle, target, &database, &stop))
                .await
            else {
                return Ok(());
            };
            let ready = ready?;
            if stop.is_cancelled() {
                return Ok(());
            }
            publish_json(&launch.ready_file, &ready, None)?;
            let _acknowledgement = ReadyFile(&launch.ready_file);
            stop.cancelled().await;
            Ok(())
        };
        tokio::select! {
            biased;
            result = &mut running => return result,
            result = acknowledge => result,
        }
    };
    handle.shutdown();
    after_close(outcome, running.await)
}

#[derive(Serialize)]
struct Ready<'a> {
    format: u32,
    database: &'a str,
    installation: Uuid,
    host: Ipv4Addr,
    port: u16,
    native_flight_port: u16,
}

async fn ready<'a>(
    handle: &ServerHandle,
    target: &'a Target,
    database: &rtdb_node::Database,
    stop: &CancellationToken,
) -> Result<Ready<'a>> {
    let source = database.latest_submitted_position()?;
    let replica = database.startup_replica_target()?;
    database.progress().await_source(source, stop).await?;
    let mut status = database.watch_status();
    let status = status
        .wait_for(|status| {
            status.role == Role::Stopped
                || status.promotion_failure.is_some()
                || matches!(status.role, Role::Leading(_))
                    && status.installed.is_some_and(|installed| {
                        installed.source >= source
                            && replica.is_none_or(|position| installed.replica >= position)
                    })
        })
        .await
        .map_err(|_| invalid("The standalone writer stopped during recovery"))?;
    if let Some(error) = &status.promotion_failure {
        return Err(error.clone());
    }
    if !matches!(status.role, Role::Leading(_)) {
        return Err(invalid("The standalone writer stopped during recovery"));
    }
    drop(status);

    let native = database
        .serving_driver()
        .ok_or_else(|| invalid("The native serving route is missing"))?;
    if native.incarnation() != *target.id.as_bytes() || !native.features().contains(&"transact") {
        return Err(invalid(
            "The recovered native serving route does not match the installed database",
        ));
    }
    let port = handle
        .addresses()
        .pgwire
        .filter(|address| address.ip() == Ipv4Addr::LOCALHOST)
        .ok_or_else(|| invalid("The private PostgreSQL listener is missing"))?
        .port();
    let native_flight_port = handle
        .addresses()
        .flight_sql
        .filter(|address| address.ip() == Ipv4Addr::LOCALHOST)
        .ok_or_else(|| invalid("The private Flight listener is missing"))?
        .port();
    Ok(Ready {
        format: PROTOCOL,
        database: &target.name,
        installation: target.id,
        host: Ipv4Addr::LOCALHOST,
        port,
        native_flight_port,
    })
}

struct ReadyFile<'a>(&'a Path);
impl Drop for ReadyFile<'_> {
    fn drop(&mut self) {
        let _ = fs::remove_file(self.0);
    }
}

struct JournalTimestamp;
impl AppendTimestampSource for JournalTimestamp {
    fn timestamp(&self, _: &[u8], supplied: Timestamp) -> rtdb_storage::Result<Timestamp> {
        Ok(supplied)
    }
}

async fn initialize_journal(path: &Path, apple_sync: AppleSync) -> Result<Uuid> {
    let (log, driver) = rtdb_storage::log::LocalLog::open_replica_with_sync(
        path,
        LocalLogOptions {
            maximum_message_bytes: ReadOnlyLocalLogOptions::default().maximum_message_bytes,
            queue_capacity: 1,
        },
        Arc::new(JournalTimestamp),
        IoRegion::new(),
        None,
        apple_sync,
    )?;
    drop(driver);
    log.close_and_wait().await;
    log.journal_lineage()
        .ok_or_else(|| invalid("Native standalone journal did not persist its lineage"))
}

fn verify_journal(path: &Path, lineage: Uuid) -> Result<()> {
    if !fs::symlink_metadata(path).map_err(io_error)?.is_file()
        || ReadOnlyLocalLog::open(path, ReadOnlyLocalLogOptions::default())?.journal_lineage() != Some(lineage)
    {
        return Err(invalid("The original standalone journal lineage is missing"));
    }
    Ok(())
}

fn verify_single_journal(path: &Path, lineage: Uuid, apple_sync: AppleSync) -> Result<()> {
    if !fs::symlink_metadata(path).map_err(io_error)?.is_file() {
        return Err(invalid("The original standalone journal lineage is missing"));
    }
    let options = ReadOnlyLocalLogOptions::default();
    match ReadOnlyLocalLog::open(path, options.clone()) {
        Ok(log) if log.journal_lineage() == Some(lineage) => Ok(()),
        Ok(_) => Err(invalid("The original standalone journal lineage is missing")),
        Err(rtdb_storage::Error::CorruptLog { .. }) => {
            rtdb_storage::log::LocalLog::recover_incomplete_replica_tail(path, lineage, options.maximum_message_bytes, apple_sync)?;
            verify_journal(path, lineage)
        }
        Err(error) => Err(error.into()),
    }
}

fn open_keys(root: &Path) -> Result<(FileKms, Arc<FileKms>)> {
    let tokens = FileKms::open(&root.join(TOKEN_KEYS), || SystemClock.now().0)?;
    let checkpoints = FileKms::open(&root.join(CHECKPOINT_KEYS), || SystemClock.now().0)?;
    Ok((tokens, Arc::new(checkpoints)))
}

fn checkpoint_context(target: &Target, journal_mode: JournalMode) -> String {
    match journal_mode {
        JournalMode::Classic => format!("rtdb/installed-local/{}:{}", target.id, target.name),
        JournalMode::Single => format!("rtdb/installed-local/single-journal/{}:{}", target.id, target.name),
    }
}

fn tls(certificate: &Path, key: &Path) -> Result<Arc<rustls::ServerConfig>> {
    let certificate = read_bounded(private_input(certificate)?, 64 * 1024)?;
    let key = Zeroizing::new(read_bounded(private_input(key)?, 64 * 1024)?);
    crate::tls::from_pem_identity(&certificate, &key).map(Arc::new).map_err(|error| {
        invalid(format!("Invalid standalone TLS identity: {error}"))
    })
}

fn read_bounded(file: File, maximum: usize) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    file.take((maximum + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(io_error)?;
    if bytes.len() > maximum {
        return Err(invalid("Standalone input exceeds its size limit"));
    }
    Ok(bytes)
}

fn private_input(path: &Path) -> Result<File> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if !metadata.is_file() {
        return Err(invalid("Standalone inputs must be original regular files"));
    }
    require_private(&metadata)?;
    File::open(path).map_err(io_error)
}

fn private_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(io_error)?;
    if !metadata.is_dir() {
        return Err(invalid("Standalone requires an original private directory"));
    }
    require_private(&metadata)
}

fn require_private(_metadata: &fs::Metadata) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if _metadata.permissions().mode() & 0o077 != 0 {
            return Err(invalid("Standalone requires private files and directories"));
        }
    }
    Ok(())
}

fn parent(path: &Path) -> &Path {
    path.parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."))
}

fn publish_json(path: &Path, value: &impl Serialize, durable: Option<AppleSync>) -> Result<()> {
    let mut staged = tempfile::NamedTempFile::new_in(parent(path)).map_err(io_error)?;
    serde_json::to_writer(&mut staged, value).map_err(|error| invalid(error.to_string()))?;
    staged.flush().map_err(io_error)?;
    if let Some(apple_sync) = durable {
        rtdb_storage::sync_file(staged.as_file(), apple_sync).map_err(io_error)?;
    }
    staged
        .persist_noclobber(path)
        .map_err(|error| io_error(error.error))?;
    if let Some(apple_sync) = durable {
        rtdb_storage::sync_directory(parent(path), apple_sync).map_err(io_error)?;
    }
    Ok(())
}

fn after_close<T>(result: Result<T>, closed: Result<()>) -> Result<T> {
    match (result, closed) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Err(close)) => {
            Err(error.with_context("resource-close-error", close.to_string().into()))
        }
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
    }
}

fn invalid(message: impl Into<String>) -> Anomaly {
    Anomaly::incorrect("rtdb.server/standalone", message)
}

fn io_error(error: io::Error) -> Anomaly {
    Anomaly::fault("rtdb.server/standalone-io", error.to_string())
}
