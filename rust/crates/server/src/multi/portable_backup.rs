use std::io::Read;

use rtdb_db::{PortableBackup, VerifiedBackup};
use serde_json::{Value, json};

use super::*;

pub const MULTI_BACKUP_HELP: &str = "\
Usage: rtdb multi-backup export --data DIR --archive FILE [--journal-url URL --tls-dir DIR] [--immutable-streams] [--authority-id UUID]
       rtdb multi-backup restore --data DIR --archive FILE --incarnation UUID --source-key-vault DIR
                                 [--journal-url URL --tls-dir DIR] [--immutable-streams] [--authority-id UUID]
Operates on stopped, local RTDB Multi development data. The source key vault
is supplied separately during restore. Neither command mounts a serving listener.
";

#[derive(Clone, Debug)]
enum Operation {
    Export,
    Restore { incarnation: Uuid, source_key_vault: PathBuf },
}

#[derive(Clone, Debug)]
pub struct MultiBackupOptions {
    operation: Operation,
    data: PathBuf,
    archive: PathBuf,
    journal: Option<(String, PathBuf)>,
    immutable_streams: bool,
    activation_scope: Option<ActivationScope>,
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
        let activation_scope = fields.remove("--authority-id").map(|authority_id| {
            if !immutable_streams {
                return Err(arguments_error());
            }
            let authority_id = Uuid::parse_str(&authority_id).map_err(|_| arguments_error())?;
            ActivationScope::new(authority_id, DEVELOPMENT_DATABASE)
        }).transpose()?;
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
        Ok(Self { operation, data, archive, journal, immutable_streams, activation_scope })
    }

    pub async fn run(self, apple_sync: rtdb_storage::AppleSync) -> Result<Value> {
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
                if !self.data.join("database-id").is_file() || !self.data.join("journal").is_file() {
                    return Err(Anomaly::incorrect("rtdb/backup-source", "An initialized local Multi deployment is required"));
                }
                if self.archive.try_exists().map_err(io_error)? {
                    return Err(Anomaly::new(AnomalyCategory::Conflict, "rtdb/backup-exists", "The portable backup path is already occupied"));
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

    async fn transfer(&self, runtime: &MultiRuntime, backup: Option<VerifiedBackup>) -> Result<Value> {
        let database = runtime.ensure_driver(DEVELOPMENT_DATABASE, runtime.bootstrap[DEVELOPMENT_DATABASE]).await?;
        if backup.is_some() || !self.immutable_streams {
            let live = BTreeSet::from([runtime.backends.node]);
            tokio::time::timeout(Duration::from_secs(12), async {
                loop {
                    if matches!(database.tick(&live).await?, Role::Writer(_)) { return Ok::<(), Anomaly>(()); }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }).await.map_err(|_| Anomaly::new(AnomalyCategory::Busy, "rtdb/backup-writer", "The local Multi writer lease did not become available"))??;
        }

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
            None => {
                let bytes = database.portable_backup().await?.into_bytes();
                let digest: [u8; 32] = Sha256::digest(&bytes).into();
                publish_archive(&self.archive, &bytes, runtime.backends.store.apple_sync())?;
                Ok(json!({ "manifest_sha256": digest_hex(&digest) }))
            }
        }
    }
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
