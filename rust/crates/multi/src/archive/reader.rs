//! Private, read-only access to one held checkpoint stage.

use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
    ops::Range,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use prost_types::Any;
use rtdb_columnar::MemoryBudget;
use rtdb_directory::{CheckpointStage, DeploymentRecord, Directory, HostedTask, Lifecycle};
use rtdb_format::{catalog::CheckpointContents, database::proto::database_config};
use rtdb_node::{Database, LogAccess, LogFactory, OpenedLogs, ReadFence, RemoteStorageFactory};
use rtdb_replication::BlockDiscovery;
use rtdb_server::{PlacedOptions, ServerDriver, config::NodeConfiguration};
use rtdb_storage::{Error, IoRegion, ObjectKey, ObjectStore, object_store::StoredObject};
use rtdb_types::{Anomaly, BlockIndex, Result, StorageEpoch};
use serde::Serialize;
use tokio::{io::AsyncWriteExt, net::TcpStream};
use tokio_rustls::{rustls::ServerConfig, server::TlsStream};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{
    ArchivedObject, CapturePoint, checkpoint_objects, checkpoint_storage_root, digest, encode,
    inconsistent, read, read_object,
    stage::{STAGED_MANIFEST, StageInventory, with_region},
    unavailable,
};
use crate::{
    admin::cell_media,
    deployment::{CellStorage, LOGKEEPER_LOG_TYPE, Role, database_config},
    keys::KeyService,
    logs::frozen_checkpoint,
    process::standalone::{gateway_tls, read_bounded, write_private},
    reference::{PrefixedStore, key},
    store::{CELL_STORAGE_TYPE, EncryptingStore, ROOT_MARKER},
};

mod owned_inventory;
pub(crate) use owned_inventory::{activate_checkpoint_transfer_target, seed_checkpoint_transfer_target, verify_checkpoint_transfer_target, StageOwnedRelations, inspect_staged_owned_relations, stream_staged_owned_event_files};

pub struct StagedReaderSettings {
    pub destination: Uuid,
    pub operation: Uuid,
    pub manifest_sha256: String,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
    pub password_file: PathBuf,
    pub ready_file: PathBuf,
}

#[derive(Serialize)]
struct Identity<'a> {
    database: &'a str,
    source: Uuid,
    capture: Uuid,
    destination: Uuid,
    operation: Uuid,
    manifest_sha256: &'a str,
}

#[derive(Serialize)]
struct Ready<'a> {
    #[serde(flatten)]
    identity: Identity<'a>,
    host: IpAddr,
    port: u16,
}

#[derive(Clone)]
pub(crate) struct StageSelector {
    pub destination: Uuid,
    pub operation: Uuid,
    pub manifest_sha256: String,
    pub source: Option<(Uuid, Uuid, String)>,
    pub transfer_only: bool,
}

enum Transport {
    Loopback {
        password: String,
        tls: Arc<ServerConfig>,
        ready_file: PathBuf,
    },
    Tunnel(TlsStream<TcpStream>),
}

/// Opens only the held stage on an isolated loopback TLS listener. The reader
/// presents the capture's original history and has no writer, cell membership,
/// or application-level decryption keys.
pub async fn serve_staged_checkpoint(
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    keys: &dyn KeyService,
    settings: StagedReaderSettings,
    hosted_task: Option<HostedTask>,
    stop: CancellationToken,
) -> Result<()> {
    let password = read_bounded(&settings.password_file, 64)?;
    if password.len() != 64 || !password.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(inconsistent(
            "The stage reader password must contain 64 hexadecimal digits",
        ));
    }
    let tls = gateway_tls(&settings.certificate, &settings.private_key)?;
    let selector = StageSelector {
        destination: settings.destination,
        operation: settings.operation,
        manifest_sha256: settings.manifest_sha256,
        source: None,
        transfer_only: false,
    };
    serve_stage(
        directory,
        storage,
        keys,
        selector,
        hosted_task,
        stop,
        Transport::Loopback {
            password,
            tls,
            ready_file: settings.ready_file,
        },
    )
    .await
}

/// Transfers the authenticated archive-owner connection to the frozen stage.
pub(crate) async fn tunnel_staged_checkpoint(
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    keys: &dyn KeyService,
    selector: StageSelector,
    hosted_task: Option<HostedTask>,
    stop: CancellationToken,
    stream: TlsStream<TcpStream>,
) -> Result<()> {
    serve_stage(
        directory,
        storage,
        keys,
        selector,
        hosted_task,
        stop,
        Transport::Tunnel(stream),
    )
    .await
}

async fn serve_stage(
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    keys: &dyn KeyService,
    selector: StageSelector,
    hosted_task: Option<HostedTask>,
    stop: CancellationToken,
    transport: Transport,
) -> Result<()> {
    with_stage(directory, storage, keys, selector, hosted_task, |authority, record, verified| async move {
        Box::pin(serve_reader(&authority, &record, storage, verified.store, transport, stop)).await
    }).await
}

pub(super) async fn admit_transfer_destination(
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    keys: &dyn KeyService,
    selector: StageSelector,
    hosted_task: Option<HostedTask>,
) -> Result<CheckpointStage> {
    if !selector.transfer_only || selector.source.is_none() {
        return Err(inconsistent("Terminal transfer requires a private held target stage"));
    }
    with_stage(directory, storage, keys, selector, hosted_task, |authority, _, verified| async move {
        if verified.contents != CheckpointContents::Replay {
            return Err(inconsistent("Terminal transfer requires a complete replay checkpoint"));
        }
        Ok(authority.stage)
    }).await
}

async fn with_stage<T, F, Fut>(
    directory: Arc<dyn Directory>,
    storage: &CellStorage,
    keys: &dyn KeyService,
    selector: StageSelector,
    hosted_task: Option<HostedTask>,
    read: F,
) -> Result<T>
where
    F: FnOnce(StageReadAuthority, DeploymentRecord, VerifiedStage) -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let id = selector.destination.to_string();
    let record = directory
        .deployment(&id)
        .await?
        .ok_or_else(|| unavailable("The held checkpoint stage is unavailable"))?;
    let media = cell_media(storage, keys)?;
    let stage = record
        .checkpoint_stage
        .as_deref()
        .filter(|stage| {
            stage.intent.destination == selector.destination
                && stage.intent.operation == selector.operation
                && selector
                    .source
                    .as_ref()
                    .is_none_or(|(source, capture, org)| {
                        stage.intent.source == *source
                            && stage.intent.capture == *capture
                            && &stage.intent.org == org
                    })
                && stage.media == media
                && stage.transfer_only == selector.transfer_only
                && stage.staged_manifest_sha256.as_deref()
                    == Some(selector.manifest_sha256.as_str())
        })
        .ok_or_else(|| inconsistent("The reader requires the exact staged checkpoint receipt"))?;
    let authority = StageReadAuthority {
        directory: Arc::clone(&directory),
        stage: stage.clone(),
        id: id.clone(),
    };
    let reader = authority.clone();
    with_region(
        directory.as_ref(),
        keys,
        &id,
        storage,
        hosted_task.as_ref(),
        None,
        None,
        |current| authority.require_held(current).map(|_| None),
        |record, _, region| async move {
            let store = open_inventory(storage, keys, &record, &reader.stage, region).await?;
            read(reader, record, store).await
        },
    )
    .await
}

#[derive(Clone)]
struct StageReadAuthority {
    directory: Arc<dyn Directory>,
    stage: CheckpointStage,
    id: String,
}

impl StageReadAuthority {
    fn require_held(&self, record: &DeploymentRecord) -> Result<()> {
        if record.id != self.id
            || record.lineage != self.id
            || record.name != self.stage.intent.name
            || record.org != self.stage.intent.org
            || record.incarnation != 1
            || record.lifecycle != Lifecycle::Restoring
            || record.parent.is_some()
            || record.holder.is_some()
            || !record.ensemble.is_empty()
            || record.checkpoint.is_some()
            || record.checkpoint_stage.as_deref() != Some(&self.stage)
        {
            return Err(inconsistent(
                "The staged checkpoint is no longer held under this receipt",
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ReadFence for StageReadAuthority {
    async fn confirm(&self, database: &str) -> Result<()> {
        if database != self.stage.intent.name {
            return Err(inconsistent(
                "The checkpoint reader was asked for another database",
            ));
        }
        let record = self
            .directory
            .deployment(&self.id)
            .await?
            .ok_or_else(|| unavailable("The held checkpoint stage is unavailable"))?;
        self.require_held(&record)
    }

    async fn confirm_hosted(&self, database: &Database) -> Result<String> {
        self.confirm(database.name()).await?;
        Ok(self.stage.intent.source.to_string())
    }
}

struct VerifiedStage {
    store: Arc<dyn ObjectStore>,
    contents: CheckpointContents,
}

async fn open_inventory(
    storage: &CellStorage,
    keys: &dyn KeyService,
    record: &DeploymentRecord,
    stage: &CheckpointStage,
    region: Arc<IoRegion>,
) -> Result<VerifiedStage> {
    let root = format!("{}/{}", record.storage_prefix(), checkpoint_storage_root()?);
    let files = PrefixedStore::new(storage.bucket(region).await?, root);
    if files.get(&key(ROOT_MARKER)?).await?.as_ref() != record.id.as_bytes() {
        return Err(inconsistent(
            "The staged storage root belongs to another installation",
        ));
    }
    let manifest_path = key(STAGED_MANIFEST)?;
    let manifest = read::<StageInventory>(&files, &manifest_path)
        .await?
        .ok_or_else(|| unavailable("The staged checkpoint inventory is missing"))?;
    let observed_digest = digest(&encode(&manifest)?);
    if stage.staged_manifest_sha256.as_deref() != Some(observed_digest.as_str()) {
        return Err(inconsistent(
            "The staged inventory differs from its accepted digest",
        ));
    }
    let (contents, manifest) = manifest.into_contents();
    let root_marker = key(ROOT_MARKER)?;
    let mut objects = BTreeMap::new();
    let mut bytes = 0u64;
    for object in manifest {
        bytes = bytes
            .checked_add(object.bytes)
            .filter(|total| *total <= super::MAXIMUM_BYTES)
            .ok_or_else(|| inconsistent("The staged objects exceed the accepted byte budget"))?;
        let name = key(&object.name)?;
        if name == root_marker || name == manifest_path {
            return Err(inconsistent("The staged inventory claims a control object"));
        }
        if objects.insert(name, object).is_some() {
            return Err(inconsistent(
                "The staged inventory contains a duplicate object",
            ));
        }
    }
    let mut expected: BTreeSet<_> = objects.keys().cloned().collect();
    expected.insert(root_marker.clone());
    expected.insert(manifest_path.clone());
    let observed = files
        .list(None)
        .await?
        .into_iter()
        .map(|object| object.key)
        .collect::<BTreeSet<_>>();
    if !expected.is_subset(&observed) || observed.difference(&expected).any(|object| {
        !super::admits_seeded_transfer_payload(record, stage, object.as_str())
    }) {
        return Err(inconsistent(
            "The staged storage root differs from its accepted inventory",
        ));
    }
    let material = keys
        .existing_key(&record.key_id())
        .await?
        .ok_or_else(|| unavailable("The staged checkpoint key is unavailable"))?;
    let plaintext = Arc::new(EncryptingStore::new(
        Arc::new(StageObjects { files, objects }),
        &material,
    )?);
    let point = CapturePoint {
        operation: stage.intent.capture,
        deployment: stage.intent.source,
        incarnation: stage.source_incarnation,
        log_epoch: stage.source_log_epoch,
        checkpoint: stage.checkpoint,
    };
    let closure = checkpoint_objects(plaintext.as_ref(), &point, contents).await?;
    expected.remove(&root_marker);
    expected.remove(&manifest_path);
    if closure.keys().ne(expected.iter()) {
        return Err(inconsistent(
            "The staged object inventory differs from the checkpoint closure",
        ));
    }
    Ok(VerifiedStage { store: plaintext, contents })
}

struct StageObjects {
    files: PrefixedStore,
    objects: BTreeMap<ObjectKey, ArchivedObject>,
}

#[async_trait::async_trait]
impl ObjectStore for StageObjects {
    fn supports_scan_ranges(&self, key: &ObjectKey) -> bool { self.files.supports_scan_ranges(key) }
    async fn head(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        self.objects
            .get(key)
            .map(|object| StoredObject {
                key: key.clone(),
                size: object.bytes,
            })
            .ok_or_else(|| Error::MissingObject(key.to_string()))
    }

    async fn head_fresh(&self, key: &ObjectKey) -> rtdb_storage::Result<StoredObject> {
        let expected = self.head(key).await?;
        if self.files.head_fresh(key).await?.size != expected.size {
            return Err(Error::ConflictingObject(key.to_string()));
        }
        Ok(expected)
    }

    async fn get(&self, key: &ObjectKey) -> rtdb_storage::Result<Bytes> {
        let object = self
            .objects
            .get(key)
            .ok_or_else(|| Error::MissingObject(key.to_string()))?;
        read_object(&self.files, key, object).await
    }

    async fn get_range(&self, key: &ObjectKey, range: Range<u64>) -> rtdb_storage::Result<Bytes> {
        let bytes = self.get(key).await?;
        if range.start > range.end || range.end > bytes.len() as u64 {
            return Err(Error::InvalidRange {
                key: key.to_string(),
                start: range.start,
                end: range.end,
                length: bytes.len() as u64,
            });
        }
        Ok(bytes.slice(range.start as usize..range.end as usize))
    }

    async fn list(&self, directory: Option<&ObjectKey>) -> rtdb_storage::Result<Vec<StoredObject>> {
        Ok(self
            .objects
            .iter()
            .filter(|(key, _)| directory.is_none_or(|root| key.is_under(root)))
            .map(|(key, object)| StoredObject {
                key: key.clone(),
                size: object.bytes,
            })
            .collect())
    }

    async fn put(&self, key: &ObjectKey, _: Bytes) -> rtdb_storage::Result<()> {
        Err(Error::ReadOnlyLog(PathBuf::from(key.as_str())))
    }

    async fn put_file(&self, key: &ObjectKey, _: &std::path::Path) -> rtdb_storage::Result<()> {
        Err(Error::ReadOnlyLog(PathBuf::from(key.as_str())))
    }

    async fn delete(&self, key: &ObjectKey) -> rtdb_storage::Result<()> {
        Err(Error::ReadOnlyLog(PathBuf::from(key.as_str())))
    }
}

struct StageStorage {
    configuration: Any,
    store: Arc<dyn ObjectStore>,
    block: BlockIndex,
}

#[async_trait::async_trait]
impl RemoteStorageFactory for StageStorage {
    fn use_disk_cache(&self) -> bool {
        false
    }

    async fn open(
        &self,
        configuration: &Any,
        root: &ObjectKey,
        _: Arc<IoRegion>,
    ) -> Result<Arc<dyn ObjectStore>> {
        if configuration != &self.configuration || root.as_str() != checkpoint_storage_root()? {
            return Err(inconsistent("The reader cannot open another storage root"));
        }
        Ok(Arc::clone(&self.store))
    }

    async fn block_discovery(&self, configuration: &Any) -> Result<BlockDiscovery> {
        if configuration != &self.configuration {
            return Err(inconsistent("The reader cannot open another checkpoint"));
        }
        Ok(BlockDiscovery::At(self.block))
    }
}

struct StageLogs {
    configuration: Any,
    checkpoint: rtdb_directory::Checkpoint,
    epoch: StorageEpoch,
}

#[async_trait::async_trait]
impl LogFactory for StageLogs {
    async fn open(
        &self,
        configuration: &Any,
        access: LogAccess,
        _: CancellationToken,
    ) -> Result<OpenedLogs> {
        if configuration != &self.configuration || !matches!(access, LogAccess::ReadOnly) {
            return Err(inconsistent(
                "The staged checkpoint accepts only its frozen read logs",
            ));
        }
        frozen_checkpoint(self.checkpoint, self.epoch, 4 << 20)
    }
}

async fn serve_reader(
    authority: &StageReadAuthority,
    record: &DeploymentRecord,
    storage: &CellStorage,
    store: Arc<dyn ObjectStore>,
    transport: Transport,
    stop: CancellationToken,
) -> Result<()> {
    let stage = &authority.stage;
    let mut snapshot = record.clone();
    snapshot.log_epoch = stage.source_log_epoch;
    let configuration = database_config(
        &snapshot,
        Vec::new(),
        storage,
        Role::Snapshot(stage.checkpoint),
    );
    let storage_config = match configuration.storage.as_ref() {
        Some(database_config::Storage::RemoteStorage(remote)) => remote.object_store.clone(),
        _ => None,
    }
    .ok_or_else(|| inconsistent("The checkpoint reader lacks its scoped storage configuration"))?;
    let Some(database_config::Log::OtherLog(log_config)) = configuration.log.as_ref() else {
        return Err(inconsistent("The checkpoint reader lacks its frozen logs"));
    };
    let placed = PlacedOptions {
        storage_factories: vec![(
            CELL_STORAGE_TYPE.into(),
            Arc::new(StageStorage {
                configuration: storage_config,
                store,
                block: BlockIndex(stage.checkpoint.block),
            }),
        )],
        log_factories: vec![(
            LOGKEEPER_LOG_TYPE.into(),
            Arc::new(StageLogs {
                configuration: log_config.clone(),
                checkpoint: stage.checkpoint,
                epoch: StorageEpoch(stage.source_log_epoch),
            }),
        )],
        read_fence: Some(Arc::new((*authority).clone())),
        scan_fragmenter: None,
        compaction_relay: None,
        claim_wait: Duration::from_secs(1),
        capabilities: None,
        flight_peer: None,
        flight_owned_worlds: None,
    };
    let auth = match &transport {
        Transport::Loopback { password, .. } => format!("password: \"{password}\""),
        Transport::Tunnel(_) => "password: null".into(),
    };
    let mut options = NodeConfiguration::from_yaml(&format!(
        "server: null\nflightSql: null\nauthn: !SingleRootUser\n  {auth}\n",
    ))?;
    options.compactor.threads = 0;
    options.garbage.enabled = false;
    let limit = 1usize << 30;
    let budget = MemoryBudget::new(options.node_options(limit)?.execution_memory_bytes);
    let (server, driver) = ServerDriver::open_placed(options, limit, budget, placed).await?;
    let runner = tokio::spawn(driver.run());
    let result = async {
        let database = &stage.intent.name;
        if server
            .node()
            .database_or_attach(database.clone(), configuration)
            .await?
            .is_none()
        {
            return Err(unavailable(
                "The checkpoint reader could not open its staged block",
            ));
        }
        authority.confirm(database).await?;
        let identity = Identity {
            database,
            source: stage.intent.source,
            capture: stage.intent.capture,
            destination: stage.intent.destination,
            operation: stage.intent.operation,
            manifest_sha256: stage
                .staged_manifest_sha256
                .as_deref()
                .ok_or_else(|| inconsistent("The stage has no accepted inventory"))?,
        };
        let parameter = serde_json::to_string(&identity)
            .map_err(|_| inconsistent("Cannot encode the checkpoint reader identity"))?;
        let node = server.node().clone();
        let auth = Arc::clone(server.authenticator());
        let (tls, ready_file) = match transport {
            Transport::Loopback {
                tls, ready_file, ..
            } => (tls, ready_file),
            Transport::Tunnel(mut stream) => {
                let stop = stop.child_token();
                tokio::time::timeout(Duration::from_secs(15), async {
                    stream.write_all(b"RTDB checkpoint PgWire 1\n").await?;
                    stream.flush().await
                })
                .await
                .map_err(|_| unavailable("Checkpoint reader acknowledgement timed out"))?
                .map_err(|_| unavailable("The checkpoint reader caller disconnected"))?;
                let mut wire = std::pin::pin!(rtdb_pgwire::serve_read_only_tls(
                    stream,
                    node,
                    auth,
                    BTreeMap::from([(rtdb_pgwire::CHECKPOINT_STAGE_PARAMETER.into(), parameter)]),
                    stop.clone(),
                ));
                let fence = tokio::select! {
                    result = &mut wire => return result,
                    result = monitor(authority, &stop) => result,
                };
                stop.cancel();
                return fence.and(wire.await);
            }
        };
        let mut wire_config = rtdb_pgwire::ServerConfig {
            read_only: true,
            tls: Some(tls),
            require_tls: true,
            ..Default::default()
        };
        wire_config
            .parameters
            .insert(rtdb_pgwire::CHECKPOINT_STAGE_PARAMETER.into(), parameter);
        let (wire, driver) = rtdb_pgwire::ServerDriver::bind(wire_config, node, auth).await?;
        let address = wire.address();
        write_private(
            &ready_file,
            &Ready {
                identity,
                host: address.ip(),
                port: address.port(),
            },
            false,
        )?;
        let mut driver = std::pin::pin!(driver.run());
        let result = tokio::select! {
            result = &mut driver => return result,
            result = monitor(authority, &stop) => result,
        };
        wire.shutdown();
        result.and(driver.await)
    }
    .await;
    server.shutdown();
    let complete = runner
        .await
        .map_err(|error| Anomaly::fault("rtdb.multi/checkpoint-reader", error.to_string()))?;
    result.and(complete)
}

async fn monitor(authority: &StageReadAuthority, stop: &CancellationToken) -> Result<()> {
    let mut poll = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = stop.cancelled() => return Ok(()),
            _ = poll.tick() => authority.confirm(&authority.stage.intent.name).await?,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn kept_stage_objects_disallow_writes_and_substituted_bytes() -> Result<()> {
        let files = PrefixedStore::new(Arc::new(rtdb_storage::MemoryStore::default()), "stage");
        let selected = key("selected")?;
        let substitute = key("substitute")?;
        let unsigned = key("unsigned")?;
        let bytes = Bytes::from_static(b"accepted");
        files.put(&selected, bytes.clone()).await?;
        files
            .put(&substitute, Bytes::from_static(b"replaced"))
            .await?;
        files.put(&unsigned, bytes.clone()).await?;
        let store = StageObjects {
            files,
            objects: [selected.clone(), substitute.clone()]
                .into_iter()
                .map(|key| {
                    (
                        key.clone(),
                        ArchivedObject {
                            name: key.to_string(),
                            bytes: bytes.len() as u64,
                            sha256: digest(&bytes),
                        },
                    )
                })
                .collect(),
        };
        assert_eq!(store.get(&selected).await?, bytes);
        assert!(matches!(
            store.get(&substitute).await,
            Err(Error::ConflictingObject(_))
        ));
        assert!(matches!(
            store.get(&unsigned).await,
            Err(Error::MissingObject(_))
        ));
        assert!(store.put(&selected, bytes).await.is_err());
        assert!(store.delete(&selected).await.is_err());
        Ok(())
    }
}
