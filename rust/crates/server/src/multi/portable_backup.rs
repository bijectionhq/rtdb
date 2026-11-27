use std::io::Read;

use rtdb_db::{PortableBackup, VerifiedBackup};
use serde_json::{Value, json};

use super::*;

#[cfg(feature = "backup-object-holds")]
mod held_objects;

pub const MULTI_BACKUP_HELP: &str = "\
Usage: rtdb multi-backup export --data DIR --archive FILE [--journal-url URL --tls-dir DIR] [--immutable-streams] [--authority-id UUID [--cell-id UUID]]
       rtdb multi-backup restore --data DIR --archive FILE --incarnation UUID --source-key-vault DIR
                                 [--journal-url URL --tls-dir DIR] [--immutable-streams] [--authority-id UUID [--cell-id UUID]]
       rtdb multi-backup verify-object-holds --archive FILE --incarnation UUID --authority-id UUID [--cell-id UUID]
                    --journal-url URL --tls-dir DIR --key-vault DIR --bucket BUCKET [--region REGION] [--s3-endpoint URL]
Export and restore operate on stopped, local RTDB Multi development data.
Verify-object-holds reads a staged immutable journal and an S3 allocation.
It requires the backup-object-holds build feature. These commands run without a serving listener.
";

#[derive(Clone, Debug)]
enum Operation {
    Export,
    Restore { incarnation: Uuid, source_key_vault: PathBuf },
}

#[derive(Clone, Debug)]
struct LocalBackupOptions {
    operation: Operation,
    data: PathBuf,
    archive: PathBuf,
    journal: Option<(String, PathBuf)>,
    immutable_streams: bool,
    activation_scope: Option<ActivationScope>,
}

#[derive(Clone, Debug)]
pub struct MultiBackupOptions(BackupCommand);

#[derive(Clone, Debug)]
enum BackupCommand {
    Local(LocalBackupOptions),
    #[cfg(feature = "backup-object-holds")]
    VerifyObjects(held_objects::Verifier),
}

impl MultiBackupOptions {
    pub(crate) fn parse(mut arguments: impl Iterator<Item = String>) -> Result<Self> {
        let operation = arguments.next().ok_or_else(arguments_error)?;
        let mut fields = BTreeMap::new();
        let mut immutable_streams = false;
        while let Some(name) = arguments.next() {
            if name == "--immutable-streams" && !immutable_streams {
                immutable_streams = true;
                continue;
            }
            let value = arguments.next().ok_or_else(arguments_error)?;
            if name == "--immutable-streams" || !name.starts_with("--") || value.starts_with("--") || fields.insert(name, value).is_some() {
                return Err(arguments_error());
            }
        }
        if operation == "verify-object-holds" {
            if immutable_streams { return Err(arguments_error()); }
            #[cfg(feature = "backup-object-holds")]
            return Ok(Self(BackupCommand::VerifyObjects(held_objects::Verifier::parse(fields)?)));
            #[cfg(not(feature = "backup-object-holds"))]
            return Err(Anomaly::unsupported("rtdb/backup-object-holds", "Object hold verification requires the backup-object-holds build feature"));
        }
        let data = fields.remove("--data").ok_or_else(arguments_error)?.into();
        let archive = fields.remove("--archive").ok_or_else(arguments_error)?.into();
        let journal = match (fields.remove("--journal-url"), fields.remove("--tls-dir")) {
            (Some(url), Some(directory)) => Some((url, directory.into())),
            (None, None) => None,
            _ => return Err(arguments_error()),
        };
        if immutable_streams && !journal.as_ref().is_some_and(|(url, _)| url.starts_with(CONSENSUS_SCHEME)) {
            return Err(arguments_error());
        }
        let cell = fields.remove("--cell-id").map(|cell| {
            Uuid::parse_str(&cell).ok().filter(|cell| !cell.is_nil()).ok_or_else(arguments_error)
        }).transpose()?;
        let activation_scope = fields.remove("--authority-id").map(|authority_id| {
            if !immutable_streams {
                return Err(arguments_error());
            }
            let authority_id = Uuid::parse_str(&authority_id).map_err(|_| arguments_error())?;
            ActivationScope::for_cell(authority_id, DEVELOPMENT_DATABASE, cell)
        }).transpose()?;
        if cell.is_some() && activation_scope.is_none() {
            return Err(arguments_error());
        }
        let operation = match operation.as_str() {
            "export" => Operation::Export,
            "restore" => {
                let incarnation = fields.remove("--incarnation")
                    .and_then(|value| Uuid::parse_str(&value).ok())
                    .filter(|id| !id.is_nil())
                    .ok_or_else(arguments_error)?;
                let source_key_vault = fields.remove("--source-key-vault").ok_or_else(arguments_error)?.into();
                Operation::Restore { incarnation, source_key_vault }
            }
            _ => return Err(arguments_error()),
        };
        if !fields.is_empty() { return Err(arguments_error()); }
        Ok(Self(BackupCommand::Local(LocalBackupOptions { operation, data, archive, journal, immutable_streams, activation_scope })))
    }

    pub async fn run(self, apple_sync: rtdb_storage::AppleSync) -> Result<Value> {
        match self.0 {
            BackupCommand::Local(options) => options.run(apple_sync).await,
            #[cfg(feature = "backup-object-holds")]
            BackupCommand::VerifyObjects(verifier) => verifier.run().await,
        }
    }
}

impl LocalBackupOptions {
    async fn run(self, apple_sync: rtdb_storage::AppleSync) -> Result<Value> {
        let (incarnation, backup) = match &self.operation {
            Operation::Restore { incarnation, source_key_vault } => {
                let keys = Arc::new(FileKms::open(source_key_vault, || SystemClock.now().0)?);
                let archive = read_archive(&self.archive)?;
                let backup = PortableBackup::verify(&archive, keys).await?;
                if backup.source_basis().incarnation() == *incarnation.as_bytes() {
                    return Err(Anomaly::incorrect("rtdb/restore-same-deployment", "Restore needs a fresh deployment identity"));
                }
                (Some(*incarnation), Some(backup))
            }
            Operation::Export => {
                if self.archive.try_exists().map_err(io_error)? {
                    return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb/backup-exists", "The portable backup path is already occupied"));
                }
                if !self.data.join("database-id").is_file() || !self.data.join("journal").is_file() {
                    return Err(backup_source());
                }
                if self.immutable_streams {
                    let (url, directory) = self.journal.as_ref().ok_or_else(arguments_error)?;
                    let connection = crate::cli::development_stream_journal(url, directory)?;
                    return self.export_immutable(connection, apple_sync).await;
                }
                (None, None)
            }
        };
        let mut configuration = NodeConfiguration::from_yaml_with_environment("", &|_| None, false)?;
        configuration.apple_sync = apple_sync;
        configuration.multi_journal.immutable_streams = self.immutable_streams;
        let journal = self.journal.as_ref()
            .map(|(url, directory)| crate::cli::development_stream_journal(url, directory))
            .transpose()?;
        let runtime = MultiRuntime::development_with_access(
            &self.data,
            &mut configuration,
            MemoryBudget::new(PortableBackup::MAX_BYTES),
            incarnation,
            journal,
            None,
            self.activation_scope,
        ).await?;
        let result = self.transfer(&runtime, backup).await;
        runtime.close().await;
        result
    }

    async fn export_immutable(&self, connection: DevelopmentStreamJournal, apple_sync: rtdb_storage::AppleSync) -> Result<Value> {
        let mut journal_marker = File::open(self.data.join("journal")).map_err(io_error)?;
        match fs2::FileExt::try_lock_exclusive(&journal_marker) {
            Ok(()) => (),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                return Err(Anomaly::new(AnomalyCategory::Busy, "rtdb.multi/development-journal",
                    "Multi development data already has a local writer"));
            }
            Err(error) => return Err(io_error(error)),
        }
        let expected: [u8; 32] = Sha256::digest(connection.url.as_bytes()).into();
        if read_marker::<32>(&mut journal_marker)? != expected {
            return Err(invalid("Multi development data belongs to another journal URL"));
        }
        let mut database_marker = File::open(self.data.join("database-id")).map_err(io_error)?;
        let incarnation = Uuid::from_bytes(read_marker(&mut database_marker)?);
        let objects = self.data.join("objects");
        if incarnation.is_nil() || !objects.is_dir() {
            return Err(backup_source());
        }
        let keys = Arc::new(FileKms::open(&self.data.join("keys"), || SystemClock.now().0)?);
        let JournalBackend { journal, batch, .. } =
            connect_stream_journal(&connection.url, &connection.credentials).await?;
        let stream = StreamId(incarnation.as_u128());
        if journal.head(stream).await.map_err(|error| match error {
            JournalError::NotFound => backup_source(),
            error => Anomaly::new(AnomalyCategory::Unavailable, "rtdb/journal-unavailable", error.to_string()),
        })?.0 == 0 {
            return Err(backup_source());
        }
        let io = IoRegion::new();
        let store = Arc::new(LocalStore::open_in(objects, Arc::clone(&io))?.with_apple_sync(apple_sync));
        let mut database = MultiDatabase::new(
            MultiOptions {
                database: incarnation,
                node: Ipv4Addr::LOCALHOST.into(),
                lease_micros: WRITER_LEASE_MICROS,
                drift_micros: CLOCK_DRIFT_MICROS,
            },
            journal,
            store,
            keys,
            Arc::new(|| SystemClock.now()),
        )?
            .with_immutable_journal()
            .with_batch_appender(batch)
            .with_manifest_budget(MemoryBudget::new(PortableBackup::MAX_BYTES));
        if let Some(scope) = self.activation_scope {
            database = database.with_activation_scope(scope)?;
        }
        let backup = database.portable_backup().await;
        io.close_and_wait().await;
        self.publish(backup?, apple_sync)
    }

    async fn transfer(&self, runtime: &MultiRuntime, backup: Option<VerifiedBackup>) -> Result<Value> {
        let database = runtime.ensure_driver(DEVELOPMENT_DATABASE, runtime.bootstrap[DEVELOPMENT_DATABASE]).await?;
        let live = BTreeSet::from([runtime.backends.node]);
        tokio::time::timeout(Duration::from_secs(12), async {
            loop {
                if matches!(database.tick(&live).await?, Role::Writer(_)) { return Ok::<(), Anomaly>(()); }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }).await.map_err(|_| Anomaly::new(AnomalyCategory::Busy, "rtdb/backup-writer", "The local Multi writer lease did not become available"))??;

        match backup {
            Some(backup) => {
                let installed = database.install_portable_backup(backup).await?;
                Ok(json!({
                    "source_incarnation": installed.source_incarnation(),
                    "source_lsn": installed.source_lsn().0,
                    "source_position": installed.source_basis().position(),
                    "destination_incarnation": Uuid::from_bytes(installed.destination_basis().incarnation()),
                    "destination_position": installed.destination_basis().position(),
                    "manifest_sha256": digest_hex(&installed.manifest_sha256()),
                }))
            }
            None => self.publish(database.portable_backup().await?, runtime.backends.store.apple_sync()),
        }
    }

    fn publish(&self, backup: PortableBackup, apple_sync: rtdb_storage::AppleSync) -> Result<Value> {
        let bytes = backup.into_bytes();
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        publish_archive(&self.archive, &bytes, apple_sync)?;
        Ok(json!({ "manifest_sha256": digest_hex(&digest) }))
    }
}

fn read_marker<const N: usize>(file: &mut File) -> Result<[u8; N]> {
    if !file.metadata().map_err(io_error)?.is_file() {
        return Err(backup_source());
    }
    let mut bytes = Vec::new();
    file.take(N as u64 + 1).read_to_end(&mut bytes).map_err(io_error)?;
    bytes.try_into().map_err(|_| backup_source())
}

fn backup_source() -> Anomaly {
    Anomaly::incorrect("rtdb/backup-source", "An initialized local Multi deployment is required")
}

fn read_archive(path: &Path) -> Result<Vec<u8>> {
    let file = File::open(path).map_err(io_error)?;
    if !file.metadata().map_err(io_error)?.is_file() {
        return Err(Anomaly::incorrect("rtdb/invalid-backup", "The portable backup must be a regular file"));
    }
    let mut bytes = Vec::new();
    file.take(PortableBackup::MAX_BYTES as u64 + 1).read_to_end(&mut bytes).map_err(io_error)?;
    Ok(bytes)
}

fn publish_archive(path: &Path, bytes: &[u8], apple_sync: rtdb_storage::AppleSync) -> Result<()> {
    let parent = path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or_else(|| Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(io_error)?;
    temporary.write_all(bytes).map_err(io_error)?;
    rtdb_storage::sync_file(temporary.as_file(), apple_sync).map_err(io_error)?;
    temporary.persist_noclobber(path).map_err(|error| io_error(error.error))?;
    rtdb_storage::sync_directory(parent, apple_sync).map_err(io_error)
}

fn digest_hex(digest: &[u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn arguments_error() -> Anomaly {
    Anomaly::incorrect("rtdb.cli/arguments", "Invalid multi-backup arguments. Use rtdb multi-backup --help")
}

#[cfg(test)]
mod tests {
    use super::*;
    use stream_logkeeper::stream::consensus::{CLUSTER_VERSION_CURRENT, Config, testing::LocalCluster};

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn commissioned_backup_exports_without_creating_a_catalog() {
        const PRINCIPAL: &str = "urn:logkeeper:client:commissioned-backup";
        const READER: &str = "urn:logkeeper:client:commissioned-backup-reader";
        let mut keepers = LocalCluster::start_replicated_catalog(&[PRINCIPAL]).await.unwrap();
        let config = keepers.journal_config(64 << 20, 4 << 20);
        let journal = Arc::new(keepers.journal(PRINCIPAL, &config).unwrap());
        journal.finalize_version(CLUSTER_VERSION_CURRENT).await.unwrap();
        let reader_credentials = keepers.credentials(READER).unwrap();
        let configs: Vec<_> = keepers.keepers().keys().map(|id| {
            let mut config = Config::read(&keepers.write_config(*id).unwrap()).unwrap();
            config.readers.push(READER.into());
            config
        }).collect();
        keepers.shutdown().await.unwrap();
        let stop = CancellationToken::new();
        let mut tasks = Vec::new();
        for config in configs {
            let listener = tokio::net::TcpListener::bind(config.listen).await.unwrap();
            tasks.push(tokio::spawn(config.serve(listener, stop.child_token())));
        }
        let reader = keepers.journal(READER, &config).unwrap();
        let home = tempfile::tempdir().unwrap();
        let config_path = home.path().join("journal.json");
        fs::write(&config_path, serde_json::to_vec(&json!({
            "keepers": config.keepers, "catalog": config.catalog, "voters": config.voters,
            "catalog_voters": config.catalog_voters,
            "quota_bytes": config.quota_bytes, "max_record_bytes": config.max_record_bytes,
        })).unwrap()).unwrap();
        let url = format!("{CONSENSUS_SCHEME}{}", config_path.display());
        let connection = || DevelopmentStreamJournal::new(&url, reader_credentials.clone()).unwrap();
        let data = home.path().join("source");
        fs::create_dir(&data).unwrap();
        let incarnation = Uuid::new_v4();
        fs::write(data.join("database-id"), incarnation.as_bytes()).unwrap();
        fs::write(data.join("journal"), Sha256::digest(url.as_bytes())).unwrap();
        let vault = data.join("keys");
        FileKms::create_directory(&vault).unwrap();
        let keys = Arc::new(FileKms::open(&vault, || SystemClock.now().0).unwrap());
        let store = Arc::new(LocalStore::open(data.join("objects")).unwrap());
        let scope = ActivationScope::for_cell(Uuid::new_v4(), DEVELOPMENT_DATABASE, Some(Uuid::new_v4())).unwrap();
        let database = MultiDatabase::new(
            MultiOptions {
                database: incarnation, node: Ipv4Addr::LOCALHOST.into(),
                lease_micros: WRITER_LEASE_MICROS, drift_micros: CLOCK_DRIFT_MICROS,
            },
            journal.clone(), store, keys.clone(), Arc::new(|| SystemClock.now()),
        ).unwrap().with_immutable_journal().with_activation_scope(scope).unwrap();
        let stream = database.journal_stream();
        let catalog = StreamId(catalog::identity(b"catalog", incarnation, "").as_u128());
        let mut options = LocalBackupOptions {
            operation: Operation::Export,
            data,
            archive: home.path().join("cut"),
            journal: None,
            immutable_streams: true,
            activation_scope: Some(scope),
        };
        assert!(reader.create_immutable(stream).await.is_err());
        assert_eq!(options.export_immutable(connection(), rtdb_storage::AppleSync::Full).await.unwrap_err().code, "rtdb/backup-source");
        assert_eq!(journal.head(stream).await, Err(JournalError::NotFound));
        assert!(!options.archive.exists());

        database.create().await.unwrap();
        let head = journal.head(stream).await.unwrap();
        options.activation_scope = Some(ActivationScope::new(Uuid::new_v4(), DEVELOPMENT_DATABASE).unwrap());
        assert_eq!(options.export_immutable(connection(), rtdb_storage::AppleSync::Full).await.unwrap_err().code, "rtdb/activation-scope");
        assert!(!options.archive.exists());

        options.activation_scope = Some(scope);
        options.export_immutable(connection(), rtdb_storage::AppleSync::Full).await.unwrap();
        let backup = PortableBackup::verify(&fs::read(options.archive).unwrap(), keys).await.unwrap();
        assert_eq!(backup.source_basis().incarnation(), *incarnation.as_bytes());
        assert_eq!(backup.source_lsn(), head);
        assert_eq!(journal.head(stream).await.unwrap(), head);
        assert_eq!(journal.head(catalog).await, Err(JournalError::NotFound));
        assert!(!options.data.join("catalog").exists());
        stop.cancel();
        for task in tasks {
            task.await.unwrap().unwrap();
        }
    }
}
