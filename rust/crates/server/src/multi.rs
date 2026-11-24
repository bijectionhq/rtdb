use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{self, Write},
    net::{IpAddr, Ipv4Addr},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use journal_contract::{FileJournal, Identity, Journal, JournalError, Limits, StreamId};
use rtdb_columnar::MemoryBudget;
use rtdb_crypt::{FileKms, Kms};
use rtdb_db::{JournalBatchAppender, MultiDatabase, MultiOptions, discover_nodes};
use rtdb_format::database::proto::{DatabaseConfig, LocalStorage, database_config::Storage};
use rtdb_node::{
    Clock, DatabaseStorage, LogAccess, MultiServing, NativeResources, OpenedLogs, ResourceFactory,
    SystemClock, serving::ServingDriver,
};
use rtdb_storage::{IoRegion, LocalStore, ObjectStore};
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use sha2::{Digest, Sha256};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::NodeConfiguration;

pub struct MultiRuntime {
    databases: BTreeMap<String, Arc<MultiServing>>,
    kms: Arc<dyn Kms>,
    io: Arc<IoRegion>,
    node: IpAddr,
    peers: Option<String>,
    _credential_slot: Option<File>,
}

impl MultiRuntime {
    pub async fn development(
        path: &Path,
        config: &mut NodeConfiguration,
        memory: Arc<MemoryBudget>,
    ) -> Result<Self> {
        fs::create_dir_all(path).map_err(io_error)?;
        let vault = path.join("keys");
        let database = Uuid::from_bytes(load_or_create(&path.join("database-id"), || {
            *Uuid::new_v4().as_bytes()
        })?);
        let journal =
            FileJournal::open(path.join("journal"), Limits::default()).map_err(io_error)?;
        let journal = Arc::new(journal.handle(Identity(Uuid::new_v4().as_u128())));
        let io = IoRegion::new();
        let store = Arc::new(LocalStore::open_in(path.join("objects"), Arc::clone(&io))?);
        let path = path
            .join("catalog")
            .into_os_string()
            .into_string()
            .map_err(|_| invalid("--data must be a UTF-8 path"))?;
        config.primary = rtdb_format::config::default_database_config();
        config.primary.storage = Some(Storage::LocalStorage(LocalStorage {
            path: Some(path),
            epoch: Some(0),
        }));
        let backends = Backends {
            journal,
            batch: None,
            store,
            io,
            vault,
            node: Ipv4Addr::LOCALHOST.into(),
            peers: None,
            credential_slot: None,
        };
        Self::open(
            config,
            memory,
            BTreeMap::from([("xtdb".into(), database)]),
            backends,
        )
        .await
    }

    #[cfg(feature = "s3")]
    pub async fn production(
        config: &mut NodeConfiguration,
        memory: Arc<MemoryBudget>,
    ) -> Result<Self> {
        use rtdb_journal_logkeeper::{Credentials, LogkeeperJournal};

        let node = installed_node_ip().await?;
        let peers = Some(required("RTDB_PEERS")?);
        let databases = serde_json::from_str(&required("RTDB_DATABASES")?)
            .map_err(|_| invalid("RTDB_DATABASES must map database names to UUIDs"))?;
        let vault = PathBuf::from(required("RTDB_KEY_DIRECTORY")?);
        let slots = required("RTDB_JOURNAL_SLOTS")?
            .parse::<usize>()
            .ok()
            .filter(|slots| (1..=8).contains(slots))
            .ok_or_else(|| invalid("RTDB_JOURNAL_SLOTS must be between 1 and 8"))?;
        let (slot, lock) = claim_credential_slot(&vault, slots)?;
        let bucket = required("RTDB_BUCKET")?;
        let mut s3 = rtdb_s3::S3Config::new(bucket);
        s3.region = Some(required("AWS_REGION")?);
        let journal_url = required("RTDB_JOURNAL_URL")?;
        let directory = tempfile::tempdir().map_err(io_error)?;
        let credentials = Credentials {
            certificate: credential(
                directory.path(),
                "cert.pem",
                &format!("RTDB_JOURNAL_{slot}_CERTIFICATE"),
            )?,
            private_key: credential(
                directory.path(),
                "key.pem",
                &format!("RTDB_JOURNAL_{slot}_PRIVATE_KEY"),
            )?,
            ca: credential(
                directory.path(),
                "ca.pem",
                &format!("RTDB_JOURNAL_{slot}_CA_CERTIFICATE"),
            )?,
        };
        let journal = LogkeeperJournal::connect(&journal_url, &credentials, Duration::from_secs(5))
            .await
            .map_err(|error| {
                Anomaly::new(
                    AnomalyCategory::Unavailable,
                    "rtdb/logkeeper-connect",
                    error.to_string(),
                )
            })?;
        let io = IoRegion::new();
        let store = s3
            .open(
                &rtdb_storage::ObjectKey::new("multi")?,
                &BTreeMap::new(),
                Arc::clone(&io),
            )
            .await?;
        let journal = Arc::new(journal);
        let backends = Backends {
            batch: Some(Arc::new(StreamJournalBatch(Arc::clone(&journal)))),
            journal,
            store,
            io,
            vault,
            node,
            peers,
            credential_slot: Some(lock),
        };
        Self::open(config, memory, databases, backends).await
    }

    async fn open(
        config: &mut NodeConfiguration,
        memory: Arc<MemoryBudget>,
        databases: BTreeMap<String, Uuid>,
        backends: Backends,
    ) -> Result<Self> {
        if !databases.contains_key("xtdb")
            || databases.values().any(Uuid::is_nil)
            || databases.keys().any(|name| name.is_empty())
            || databases.values().collect::<BTreeSet<_>>().len() != databases.len()
        {
            return Err(invalid(
                "Multi requires xtdb and distinct nonzero database IDs",
            ));
        }
        FileKms::create_directory(&backends.vault)?;
        let kms: Arc<dyn Kms> = Arc::new(FileKms::open(&backends.vault, || SystemClock.now().0)?);
        let mut serving = BTreeMap::new();
        for (name, database) in databases {
            let options = MultiOptions {
                database,
                node: backends.node,
                lease_micros: 5_000_000,
                drift_micros: 250_000,
            };
            let mut driver = MultiDatabase::new(
                options,
                Arc::clone(&backends.journal),
                Arc::clone(&backends.store),
                Arc::clone(&kms),
                Arc::new(|| SystemClock.now()),
            )?;
            if let Some(batch) = &backends.batch {
                driver = driver.with_batch_appender(Arc::clone(batch));
            }
            match backends.journal.head(StreamId(database.as_u128())).await {
                Ok(_) => (),
                Err(JournalError::NotFound) => driver.create().await?,
                Err(JournalError::Unavailable) => (),
                Err(error) => return Err(Anomaly::fault("rtdb/multi-open", error.to_string())),
            }
            serving.insert(
                name,
                Arc::new(MultiServing::new(driver, Arc::clone(&memory))),
            );
        }
        config.fixed_databases = Some(
            serving
                .keys()
                .map(|name| (name.clone(), config.primary.clone()))
                .collect(),
        );
        config.server = None;
        config.indexer.enabled = false;
        config.garbage.enabled = false;
        Ok(Self {
            databases: serving,
            kms,
            io: backends.io,
            node: backends.node,
            peers: backends.peers,
            _credential_slot: backends.credential_slot,
        })
    }

    pub(crate) fn resource_override(
        &self,
    ) -> Box<dyn FnOnce(NativeResources) -> Arc<dyn ResourceFactory> + Send> {
        let databases = self.databases.clone();
        let kms = Arc::clone(&self.kms);
        Box::new(move |native| {
            Arc::new(ServingResources {
                native,
                databases,
                kms,
            })
        })
    }

    pub async fn drive(&self, stop: &CancellationToken) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        stop.run_until_cancelled(async {
            loop {
                interval.tick().await;
                self.tick().await?;
            }
        })
        .await
        .unwrap_or(Ok(()))
    }

    async fn tick(&self) -> Result<()> {
        let live = match &self.peers {
            None => BTreeSet::from([self.node]),
            Some(host) => match discover_nodes(host).await {
                Ok(live) => live,
                Err(error) if error.category == AnomalyCategory::Unavailable => return Ok(()),
                Err(error) => return Err(error),
            },
        };
        for driver in self.databases.values() {
            match driver.tick(&live).await {
                Ok(_) => (),
                Err(error)
                    if matches!(
                        error.category,
                        AnomalyCategory::Unavailable
                            | AnomalyCategory::Busy
                            | AnomalyCategory::Conflict
                    ) =>
                {
                    ()
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    pub async fn close(self) {
        drop(self.databases);
        self.io.close_and_wait().await;
    }
}

struct Backends {
    journal: Arc<dyn Journal>,
    batch: Option<Arc<dyn JournalBatchAppender>>,
    store: Arc<dyn ObjectStore>,
    io: Arc<IoRegion>,
    vault: PathBuf,
    node: IpAddr,
    peers: Option<String>,
    credential_slot: Option<File>,
}

#[cfg(feature = "s3")]
struct StreamJournalBatch(Arc<rtdb_journal_logkeeper::LogkeeperJournal>);

#[cfg(feature = "s3")]
impl JournalBatchAppender for StreamJournalBatch {
    fn append_batch(
        &self,
        stream: StreamId,
        epoch: journal_contract::Epoch,
        records: Vec<(journal_contract::Lsn, bytes::Bytes)>,
    ) -> journal_contract::BoxFuture<'_, journal_contract::Result<journal_contract::Lsn>> {
        Box::pin(self.0.append_batch(stream, epoch, records))
    }
}

struct ServingResources {
    native: NativeResources,
    databases: BTreeMap<String, Arc<MultiServing>>,
    kms: Arc<dyn Kms>,
}

#[async_trait::async_trait]
impl ResourceFactory for ServingResources {
    fn serving_driver(
        &self,
        name: &str,
        _: &DatabaseConfig,
        _: &CancellationToken,
    ) -> Result<Option<Arc<dyn ServingDriver>>> {
        Ok(self
            .databases
            .get(name)
            .cloned()
            .map(|driver| driver as Arc<dyn ServingDriver>))
    }

    fn journal_native_sql(&self) -> bool {
        true
    }

    fn token_kms(&self) -> Option<Arc<dyn Kms>> {
        Some(Arc::clone(&self.kms))
    }

    fn validate(&self, name: &str, config: &DatabaseConfig) -> Result<()> {
        self.native.validate(name, config)
    }

    async fn open_storage(&self, name: &str, config: &DatabaseConfig) -> Result<DatabaseStorage> {
        let storage = self.native.open_storage(name, config).await?;
        let install = async {
            let driver = self
                .databases
                .get(name)
                .ok_or_else(|| invalid("Database is outside the Multi catalog"))?;
            let incarnation = driver.incarnation();
            let marker = rtdb_storage::ObjectKey::new(format!(
                "~rtdb-v1/{:x}/incarnation",
                Sha256::digest(name)
            ))?;
            let observed = match storage
                .store
                .get_fresh_exact(&marker, incarnation.len())
                .await
            {
                Ok(observed) => observed,
                Err(rtdb_storage::Error::MissingObject(_)) => {
                    let write = storage
                        .store
                        .put(&marker, bytes::Bytes::copy_from_slice(&incarnation))
                        .await;
                    match storage
                        .store
                        .get_fresh_exact(&marker, incarnation.len())
                        .await
                    {
                        Ok(observed) => observed,
                        Err(error) => {
                            write?;
                            return Err(Anomaly::from(error));
                        }
                    }
                }
                Err(error) => return Err(Anomaly::from(error)),
            };
            if observed.as_ref() != incarnation {
                return Err(invalid(
                    "The catalog already names a different Multi incarnation",
                ));
            }
            Ok(())
        }
        .await;
        if let Err(error) = install {
            storage.io.close_and_wait().await;
            return Err(error);
        }
        Ok(storage)
    }

    async fn open_logs(
        &self,
        name: &str,
        config: &DatabaseConfig,
        access: LogAccess,
        stop: CancellationToken,
        storage: &DatabaseStorage,
    ) -> Result<OpenedLogs> {
        self.native
            .open_logs(name, config, access, stop, storage)
            .await
    }

    async fn close(&self) -> Result<()> {
        self.native.close().await
    }
}

fn load_or_create<const N: usize>(
    path: &Path,
    generate: impl FnOnce() -> [u8; N],
) -> Result<[u8; N]> {
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path.with_extension("lock"))
        .map_err(io_error)?;
    lock.lock().map_err(io_error)?;
    match fs::read(path) {
        Ok(bytes) => bytes
            .try_into()
            .map_err(|_| invalid("Invalid durable Multi identity or claim key")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .ok_or_else(|| invalid("Multi identity path must have a parent"))?;
            let bytes = generate();
            let mut file = tempfile::NamedTempFile::new_in(parent).map_err(io_error)?;
            file.write_all(&bytes)
                .and_then(|()| file.as_file().sync_all())
                .map_err(io_error)?;
            file.persist(path).map_err(|error| io_error(error.error))?;
            File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(io_error)?;
            Ok(bytes)
        }
        Err(error) => Err(io_error(error)),
    }
}

#[cfg(feature = "s3")]
fn claim_credential_slot(vault: &Path, slots: usize) -> Result<(usize, File)> {
    for slot in 0..slots {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(vault.join(format!("node-slot-{slot}.lock")))
            .map_err(io_error)?;
        match fs2::FileExt::try_lock_exclusive(&file) {
            Ok(()) => return Ok((slot, file)),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => (),
            Err(error) => return Err(io_error(error)),
        }
    }
    Err(Anomaly::new(
        AnomalyCategory::Busy,
        "rtdb.multi/credential-slot",
        "All installed journal identities are in use",
    ))
}

#[cfg(feature = "s3")]
async fn installed_node_ip() -> Result<IpAddr> {
    if let Ok(value) = std::env::var("RTDB_NODE_IP") {
        return value
            .parse()
            .map_err(|_| invalid("RTDB_NODE_IP must be an IP address"));
    }
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "PascalCase")]
    struct Metadata {
        networks: Vec<Network>,
    }
    #[derive(serde::Deserialize)]
    struct Network {
        #[serde(rename = "IPv4Addresses")]
        addresses: Vec<IpAddr>,
    }
    let url = required("ECS_CONTAINER_METADATA_URI_V4")?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .map_err(metadata_error)?;
    let response = client
        .get(url)
        .send()
        .await
        .and_then(|response| response.error_for_status())
        .map_err(metadata_error)?;
    let bytes = response.bytes().await.map_err(metadata_error)?;
    let metadata: Metadata = serde_json::from_slice(&bytes).map_err(metadata_error)?;
    metadata
        .networks
        .into_iter()
        .flat_map(|network| network.addresses)
        .next()
        .ok_or_else(|| invalid("ECS has no RTDB node address"))
}

#[cfg(feature = "s3")]
fn metadata_error(error: impl std::fmt::Display) -> Anomaly {
    Anomaly::fault("rtdb/node-metadata", error.to_string())
}

#[cfg(feature = "s3")]
fn credential(path: &Path, file: &str, env: &str) -> Result<PathBuf> {
    let path = path.join(file);
    let value = zeroize::Zeroizing::new(required(env)?);
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
        .open(&path)
        .and_then(|mut file| file.write_all(value.as_bytes()))
        .map_err(io_error)?;
    Ok(path)
}

#[cfg(feature = "s3")]
fn required(name: &str) -> Result<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| invalid(format!("{name} is required for an installed Multi node")))
}

fn invalid(message: impl Into<String>) -> Anomaly {
    Anomaly::incorrect("rtdb.multi/configuration", message)
}
fn io_error(error: io::Error) -> Anomaly {
    Anomaly::fault("rtdb.multi/local-io", error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_tx::transact::{Cell, Op, Outcome, Row, Transact};
    use rtdb_types::{EntityId, TableRef, Value};

    #[test]
    fn multi_dev_reopens_an_applied_receipt_and_reads_it_over_native_sql() {
        std::thread::Builder::new()
            .stack_size(rtdb_node::WORKER_STACK_BYTES)
            .spawn(|| {
                tokio::runtime::Builder::new_multi_thread()
                    .thread_stack_size(rtdb_node::WORKER_STACK_BYTES)
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(restart());
            })
            .unwrap()
            .join()
            .unwrap();
    }

    async fn restart() {
        let data = tempfile::tempdir().unwrap();
        let mut expected = None;
        let key = vec![37; 16];
        for reopen in [false, true] {
            let limit = 256 << 20;
            let mut config =
                NodeConfiguration::from_yaml_with_environment("", &|_| None, false).unwrap();
            let memory =
                MemoryBudget::new(config.node_options(limit).unwrap().execution_memory_bytes);
            let runtime = MultiRuntime::development(data.path(), &mut config, Arc::clone(&memory))
                .await
                .unwrap();
            let (handle, server) =
                crate::ServerDriver::open_with_multi(config, limit, memory, &runtime)
                    .await
                    .unwrap();
            let server = tokio::spawn(server.run());
            let stop = CancellationToken::new();
            let database = handle.node().catalog().database("xtdb").unwrap();
            let driver = database.serving_driver().unwrap();
            if reopen {
                let receipt = driver.tx_status(&key, &stop).await.unwrap();
                assert_eq!(Some(receipt.basis), expected);
                let user = handle.authenticator().authenticate_trusted("xtdb").unwrap();
                let mut session = handle.node().session("xtdb", user).unwrap();
                let mut query = session
                    .query_sql("SELECT value FROM docs", vec![])
                    .await
                    .unwrap();
                let batch = query.next_batch().await.unwrap().unwrap();
                assert_eq!(
                    rtdb_columnar::read_value(
                        batch.batch().schema().field(0),
                        batch.batch().column(0).as_ref(),
                        0
                    )
                    .unwrap(),
                    Value::I64(42)
                );
            } else {
                runtime.tick().await.unwrap();
                let receipt = driver
                    .transact(
                        Transact {
                            custody_request_digest: None,
                            request_memory: None,
                            key: key.clone(),
                            require: vec![],
                            window: Default::default(),
                            guards: vec![],
                            requires: vec![],
                            otherwise: vec![],
                            ops: vec![Op::Put {
                                branch: "main".into(),
                                table: TableRef::new("public", "docs"),
                                if_absent: false,
                                rows: vec![Row {
                                    id: EntityId::Integer(1),
                                    valid: None,
                                    custody_group: None,
                                    protected_columns: Vec::new(),
                                    custody_payload: None,
                                    cells: [("value".into(), Cell::Value(Value::I64(42)))]
                                        .into_iter()
                                        .collect(),
                                }],
                            }],
                        },
                        &stop,
                    )
                    .await
                    .unwrap();
                assert_eq!(receipt.outcome, Outcome::Then);
                expected = Some(receipt.basis);
            }
            drop(database);
            handle.shutdown();
            server.await.unwrap().unwrap();
            drop(handle);
            runtime.close().await;
        }
    }

    #[cfg(feature = "s3")]
    #[test]
    fn journal_credential_slots_exclude_other_live_processes() {
        let directory = tempfile::tempdir().unwrap();
        let (first, lock) = claim_credential_slot(directory.path(), 2).unwrap();
        let (second, _other) = claim_credential_slot(directory.path(), 2).unwrap();
        assert_ne!(first, second);
        assert!(
            matches!(claim_credential_slot(directory.path(), 2), Err(error) if error.category == AnomalyCategory::Busy)
        );
        drop(lock);
        assert_eq!(claim_credential_slot(directory.path(), 2).unwrap().0, first);
    }
}
