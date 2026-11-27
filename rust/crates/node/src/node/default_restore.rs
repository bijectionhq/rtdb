use std::{fs, path::Path, sync::Arc};

use bytes::Bytes;
use rtdb_format::{database::proto::{DatabaseConfig, LocalLog as LocalLogConfig, LocalStorage, database_config::{Log, Storage}},
    envelope::{SourceBody, SourceMessage}, keys::{CURRENT_STORAGE_VERSION, StorageRoot}, log::proto::AttachDatabase};
use rtdb_replication::{LocalDefaultRestoreSeed, NativeServingHandle, PublishedCheckpoint};
use rtdb_storage::{AppleSync, LocalStore, ObjectKey, ObjectStore, log::{LocalLog, LocalLogOptions, Log as Journal, LogOffset}};
use rtdb_types::{Anomaly, AnomalyCategory, Result, StorageEpoch, Timestamp, rtdb_v1::Basis};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::Database;

const INSTALL: &str = ".rtdb-default-restore-v1";
const ATTACHED: &str = ".rtdb-default-restore-attached-v1";
const JOURNAL: &str = "journal";
const MAX_INSTALL: u64 = 16 << 10;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LocalInstall {
    source: String,
    destination: String,
    receipt: Vec<u8>,
    pub(crate) seed: LocalDefaultRestoreSeed,
}

impl LocalInstall {
    fn configuration(&self, root: &Path) -> Result<DatabaseConfig> {
        let path = |path: &Path| path.to_str().map(str::to_owned).ok_or_else(|| invalid("Local restore paths must be UTF-8"));
        Ok(DatabaseConfig {
            log: Some(Log::LocalLog(LocalLogConfig { path: Some(path(&root.join(JOURNAL))?), epoch: Some(self.seed.log_epoch.0) })),
            storage: Some(Storage::LocalStorage(LocalStorage { path: Some(path(root)?), epoch: Some(0) })),
            ..Default::default()
        })
    }

    fn matches(&self, source: &str, destination: &str, receipt: &[u8]) -> bool {
        self.source == source && self.destination == destination && self.receipt.as_slice() == receipt
    }
}

pub(crate) fn read_install(name: &str, configuration: &DatabaseConfig) -> Result<Option<LocalInstall>> {
    let Some(Storage::LocalStorage(storage)) = &configuration.storage else { return Ok(None); };
    let Some(path) = storage.path.as_deref() else { return Ok(None); };
    let root = Path::new(path);
    let Some(file) = root.file_name().and_then(|file| file.to_str()) else { return Ok(None); };
    let Some(digest) = file.strip_prefix("default-restore-") else { return Ok(None); };
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()) { return Ok(None); }
    if file != install_name(name) { return Err(invalid("The provisioned Default Local path belongs to a different attachment")); }
    let Some(install) = read_terminal(root)? else {
        return Err(invalid("The ordered Default Local attachment is missing its install terminal"));
    };
    if install.destination != name || &install.configuration(root)? != configuration {
        return Err(invalid("The Default Local restore terminal does not authorize this attachment"));
    }
    for file in ["LOG", "REPLICA_LOG"] {
        if !fs::symlink_metadata(root.join(JOURNAL).join(file)).map_err(io)?.is_file() {
            return Err(invalid("The ordered Default Local attachment is missing a regular journal file"));
        }
    }
    Ok(Some(install))
}

pub(crate) fn check_pending_attachment(name: &str, configuration: &DatabaseConfig) -> Result<()> {
    if let Some(install) = read_install(name, configuration)? {
        let root = installation_root(configuration)?;
        if attachment_ordered(root, &install)? { return Err(retired()); }
    }
    Ok(())
}

pub(crate) async fn mark_ordered_attachment(database: &Database) -> Result<()> {
    let configuration = database.configuration();
    let Some(install) = read_install(database.name(), configuration)? else { return Ok(()); };
    let store = LocalStore::open(installation_root(configuration)?)?.with_apple_sync(database.logs.store().apple_sync());
    let result = store.put(&ObjectKey::new(ATTACHED)?, Bytes::copy_from_slice(&install.seed.original_receipt)).await;
    store.close_and_wait().await;
    result?;
    Ok(())
}

fn installation_root(configuration: &DatabaseConfig) -> Result<&Path> {
    let Some(Storage::LocalStorage(storage)) = &configuration.storage else { return Err(unsupported()); };
    storage.path.as_deref().map(Path::new).ok_or_else(unsupported)
}

fn attachment_ordered(root: &Path, install: &LocalInstall) -> Result<bool> {
    let path = root.join(ATTACHED);
    match fs::symlink_metadata(&path) {
        Ok(metadata) if metadata.is_file() && metadata.len() == 32 => {
            if fs::read(path).map_err(io)?.as_slice() != install.seed.original_receipt.as_slice() {
                return Err(invalid("The Local activation differs from its install terminal"));
            }
            Ok(true)
        }
        Ok(_) => Err(invalid("The Local activation has an invalid terminal")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io(error)),
    }
}

fn read_terminal(root: &Path) -> Result<Option<LocalInstall>> {
    let path = root.join(INSTALL);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io(error)),
    };
    if !fs::symlink_metadata(root).map_err(io)?.is_dir() { return Err(invalid("The Local install must use its provisioned directory")); }
    if !metadata.is_file() || metadata.len() > MAX_INSTALL { return Err(invalid("The Local restore terminal is invalid")); }
    let install: LocalInstall = serde_json::from_slice(&fs::read(path).map_err(io)?).map_err(invalid)?;
    if install.seed.basis.incarnation() == install.seed.original_basis.incarnation()
        || install.seed.basis.incarnation() == [0; 16] || install.seed.basis.position() != install.seed.original_basis.position()
    { return Err(invalid("The Local restore terminal does not have a fresh incarnation")); }
    install.seed.validate_original_receipt(&install.receipt)?;
    Ok(Some(install))
}

impl super::Node {
    /// The trusted caller has authorized the named Default installation and the exact source Basis.
    pub async fn export_default_checkpoint(&self, source: &str, pin: Uuid, basis: Basis, until: Option<Timestamp>,
        publication: Option<PublishedCheckpoint>, cancellation: &CancellationToken,
    ) -> Result<Vec<u8>> {
        self.ensure_open()?;
        let (database, handle) = self.default_checkpoint_source(source)?;
        let ticket = match publication {
            Some(publication) => handle.pin_retained_checkpoint_export(pin, basis, until, publication, cancellation).await?,
            None => handle.pin_checkpoint_export(pin, basis, until, cancellation).await?,
        };
        self.catalog.with_current_attachment(&database, || Ok(ticket))
    }

    /// Releases the exact plain export for a trusted caller authorized for this source attachment.
    pub async fn release_default_checkpoint(&self, source: &str, receipt: &[u8], cancellation: &CancellationToken) -> Result<()> {
        self.ensure_open()?;
        let (database, handle) = self.default_checkpoint_source(source)?;
        handle.release_checkpoint_export(receipt, cancellation).await?;
        self.catalog.with_current_attachment(&database, || Ok(()))
    }

    /// Provisions and orders attachment only after the independent Local install has been verified and synced.
    pub async fn restore_default_checkpoint(&self, source: &str, destination: &str, receipt: &[u8],
        provisioned_parent: &Path, cancellation: &CancellationToken,
    ) -> Result<Basis> {
        self.ensure_open()?;
        if !self.factory.resources.supports_default_local_restore() || !self.factory.options.replication.enabled
            || self.factory.options.read_only || !self.factory.options.replication.may_lead
        { return Err(unsupported()); }
        if source == destination || destination == "xtdb" || receipt.len() > 2048 {
            return Err(invalid("The Default restore needs a distinct destination and a checkpoint receipt"));
        }
        if self.catalog.skipped(destination) { return Err(unsupported()); }
        let parent = fs::canonicalize(provisioned_parent).map_err(io)?;
        let root = parent.join(install_name(destination));
        let primary = self.catalog.database("xtdb")?;
        primary.require_writable()?;
        if primary.configuration().external_source.is_some() || !primary.logs.retains_recovery_history() { return Err(unsupported()); }
        let apple_sync = self.factory.resources.apple_sync();
        let install = match read_terminal(&root)? {
            Some(install) => install,
            None => {
                self.catalog.check_attach(destination, &DatabaseConfig::default())?;
                let (source_database, source_handle) = self.default_checkpoint_source(source)?;
                require_independent_parent(source_database.configuration(), &parent)?;
                let directory = tempfile::Builder::new().prefix(".default-install-").tempdir_in(&parent).map_err(io)?;
                let inbound = directory.path().join("inbound");
                fs::create_dir(&inbound).map_err(io)?;
                let detached = source_handle.materialize_checkpoint_export(receipt, &inbound, cancellation).await?;
                let staged = directory.path().join("deployment");
                let storage = StorageRoot::new(CURRENT_STORAGE_VERSION, StorageEpoch(0))?.key();
                let storage = Arc::new(LocalStore::open(staged.join(storage))?.with_apple_sync(apple_sync));
                let seed = detached.seed_local(storage.clone(), *Uuid::new_v4().as_bytes(), self.memory(), cancellation).await?;
                storage.close_and_wait().await;
                drop(storage);
                create_empty_journals(&staged.join(JOURNAL), apple_sync).await?;
                seed.validate_original_receipt(receipt)?;
                let prepared = LocalInstall { source: source.into(), destination: destination.into(), receipt: receipt.to_vec(), seed };
                let terminal = serde_json::to_vec(&prepared).map_err(invalid)?;
                let staging = LocalStore::open(&staged)?.with_apple_sync(apple_sync);
                staging.put(&ObjectKey::new(INSTALL)?, Bytes::from(terminal)).await?;
                staging.close_and_wait().await;
                self.catalog.with_current_attachment(&source_database, || {
                    if cancellation.is_cancelled() { return Err(interrupted()); }
                    if let Err(error) = fs::rename(&staged, &root)
                        && !root.try_exists().map_err(io)?
                    { return Err(io(error)); }
                    rtdb_storage::sync_directory(directory.path(), apple_sync).map_err(io)?;
                    rtdb_storage::sync_directory(&parent, apple_sync).map_err(io)
                })?;
                read_terminal(&root)?.ok_or_else(|| invalid("The Local restore terminal did not survive publication"))?
            }
        };
        if !install.matches(source, destination, receipt) { return Err(conflict()); }
        rtdb_storage::sync_directory(&parent, apple_sync).map_err(io)?;
        primary.replication.await_source(primary.logs.latest_source_position()?, cancellation).await?;
        let configuration = install.configuration(&root)?;
        if let Ok(database) = self.catalog.database(destination) {
            return self.check_restored_attachment(&database, &configuration, install.seed.basis);
        }
        if attachment_ordered(&root, &install)? {
            return match self.catalog.database(destination) {
                Ok(database) => self.check_restored_attachment(&database, &configuration, install.seed.basis),
                Err(_) => Err(retired()),
            };
        }
        if let Err(error) = self.catalog.check_attach(destination, &configuration) {
            return match self.catalog.database(destination) {
                Ok(database) => self.check_restored_attachment(&database, &configuration, install.seed.basis),
                Err(_) => Err(error),
            };
        }
        let message = SourceMessage::Protobuf(SourceBody::AttachDatabase(AttachDatabase {
            db_name: Some(destination.to_owned()), config: Some(configuration.clone()),
        }));
        let source_position = cancellation.run_until_cancelled(primary.replication.submit_source(&message, self.now())).await
            .ok_or_else(interrupted)??;
        primary.replication.await_source(source_position, cancellation).await?;
        let database = self.catalog.database(destination)?;
        self.check_restored_attachment(&database, &configuration, install.seed.basis)
    }

    fn check_restored_attachment(&self, database: &Arc<Database>, configuration: &DatabaseConfig, basis: Basis) -> Result<Basis> {
        self.catalog.with_current_attachment(database, || {
            if database.configuration() != configuration || database.default_native_handle.as_ref().is_none_or(|handle| {
                handle.authority().incarnation != basis.incarnation()
            }) { return Err(conflict()); }
            Ok(basis)
        })
    }

    fn default_checkpoint_source(&self, source: &str) -> Result<(Arc<Database>, NativeServingHandle)> {
        if !self.factory.resources.supports_default_local_restore() { return Err(unsupported()); }
        let database = self.catalog.database(source)?;
        let handle = self.catalog.with_current_attachment(&database, || {
            if !matches!(&database.configuration().log, Some(Log::LocalLog(_)))
                || !matches!(&database.configuration().storage, Some(Storage::LocalStorage(_)))
                || database.configuration().external_source.is_some() || !database.logs.retains_recovery_history()
            { return Err(unsupported()); }
            database.default_native_handle.as_ref().filter(|handle| handle.authority().checkpoints.is_none())
                .cloned().ok_or_else(unsupported)
        })?;
        Ok((database, handle))
    }
}

async fn create_empty_journals(directory: &Path, apple_sync: AppleSync) -> Result<()> {
    fs::create_dir_all(directory).map_err(io)?;
    for name in ["LOG", "REPLICA_LOG"] {
        let (log, writer) = LocalLog::open_with_sync(directory.join(name), LocalLogOptions { maximum_message_bytes: 1 << 20, queue_capacity: 1 }, apple_sync)?;
        if log.latest()? != LogOffset(-1) { return Err(invalid("The provisioned Local journal is not empty")); }
        drop(writer);
        log.close_and_wait().await;
    }
    rtdb_storage::sync_directory(directory, apple_sync).map_err(io)
}

fn require_independent_parent(source: &DatabaseConfig, parent: &Path) -> Result<()> {
    let (Some(Log::LocalLog(log)), Some(Storage::LocalStorage(storage))) = (&source.log, &source.storage) else { return Err(unsupported()); };
    for path in [&log.path, &storage.path] {
        let source_path = fs::canonicalize(path.as_deref().ok_or_else(unsupported)?).map_err(io)?;
        if parent.starts_with(source_path) { return Err(invalid("The provisioned directory overlaps the source storage or journal")); }
    }
    Ok(())
}

fn install_name(destination: &str) -> String { format!("default-restore-{:x}", Sha256::digest(destination.as_bytes())) }
fn invalid(message: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("rtdb/default-local-restore", message.to_string()) }
fn io(error: std::io::Error) -> Anomaly { invalid(error) }
fn conflict() -> Anomaly { Anomaly::new(AnomalyCategory::Conflict, "rtdb/default-restore-conflict", "The destination belongs to another Local restore") }
fn retired() -> Anomaly { Anomaly::new(AnomalyCategory::Conflict, "rtdb/default-restore-retired", "The ordered attachment has already consumed this Local installation") }
fn interrupted() -> Anomaly { Anomaly::new(AnomalyCategory::Interrupted, "rtdb/default-restore-interrupted", "Recover the Local restore using the same receipt and provisioned destination") }
pub(crate) fn unsupported() -> Anomaly { Anomaly::unsupported("rtdb/default-restore-provider", "Default restore requires plain Local storage and an independently provisioned Local journal") }
