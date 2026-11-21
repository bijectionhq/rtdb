//! What a node opens for a deployment: the `DatabaseConfig` derived from its directory record.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesUnordered;
use rtdb_directory::{DeploymentRecord, Directory, MemberId, MemberRecord, MemberRole};
use rtdb_format::database::proto::{DatabaseConfig, RemoteStorage, database_config};
use rtdb_storage::{Error as StoreError, ObjectKey, ObjectStore};
use rtdb_types::{Anomaly, Result};
use serde::{Deserialize, Serialize};

/// The protobuf type name the logkeeper log factory is registered under.
pub const LOGKEEPER_LOG_TYPE: &str = "rtdb.multi.LogkeeperLog";

pub(crate) async fn live_holdings(directory: &dyn Directory, members: &BTreeMap<MemberId, MemberRecord>, now_ms: i64) -> Result<Vec<DeploymentRecord>> {
    const CONCURRENT_HOLDER_QUERIES: usize = 8;
    let eligible: Vec<_> = members.values().filter(|member| member.may_hold(now_ms) && member.pool != rtdb_directory::MemberPool::Compaction)
        .map(|member| member.id.clone()).collect();
    let mut eligible = eligible.into_iter();
    let mut queries = FuturesUnordered::<BoxFuture<'_, Result<Vec<DeploymentRecord>>>>::new();
    let mut deployments = BTreeMap::<String, DeploymentRecord>::new();
    loop {
        for member in eligible.by_ref().take(CONCURRENT_HOLDER_QUERIES - queries.len()) {
            queries.push(Box::pin(async move { directory.deployments_held_by(&member).await }));
        }
        let Some(found) = queries.next().await else { break };
        for record in found? {
            if deployments.get(&record.id).is_none_or(|stored| stored.version < record.version) {
                deployments.insert(record.id.clone(), record);
            }
        }
    }
    Ok(deployments.into_values().collect())
}

/// Where a cell keeps its deployments' objects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum CellStorage {
    /// A directory every node of the cell can reach, such as a shared volume.
    Local { root: PathBuf },
    /// An S3 bucket (or one that speaks its protocol), the cell's objects under its configured prefix.
    S3 { config: rtdb_s3::S3Config },
}

/// The key under an S3 configuration's prefix that a cell's objects are kept below.
const S3_CELL_ROOT: &str = "cell";
const STORAGE_NAMESPACE_MARKER: &str = "~cell-namespace";

impl CellStorage {
    /// The cell's objects as one store, keyed relative to the cell's storage root.
    pub async fn bucket(&self, io: std::sync::Arc<rtdb_storage::IoRegion>) -> rtdb_types::Result<crate::reference::Bucket> {
        match self {
            CellStorage::Local { root } => Ok(std::sync::Arc::new(rtdb_storage::LocalStore::open_in(root, io)?)),
            CellStorage::S3 { config } => {
                let root = rtdb_storage::ObjectKey::new(S3_CELL_ROOT).map_err(rtdb_types::Anomaly::from)?;
                config.open(&root, &std::collections::BTreeMap::<String, rtdb_s3::S3Credentials>::new(), io).await
            }
        }
    }

    pub(crate) async fn purge_inventory(&self, deployment: &ObjectKey) -> Result<rtdb_directory::DeletionInventory> {
        match self {
            Self::Local { .. } => Ok(rtdb_directory::DeletionInventory::CurrentObjects),
            Self::S3 { config } => {
                let cell = ObjectKey::new(S3_CELL_ROOT)?;
                let remotes = std::collections::BTreeMap::<String, rtdb_s3::S3Credentials>::new();
                let inventory = config.versioned_inventory(&cell, &remotes).await?;
                inventory.purge(deployment).await?;
                Ok(rtdb_directory::DeletionInventory::AllVersionsAndMultipartUploads)
            }
        }
    }

    pub(crate) fn cleanup_contains(&self, other: &Self, deployment: &ObjectKey) -> Result<bool> {
        match (self, other) {
            (Self::Local { root }, Self::Local { root: other_root }) => {
                let canonical = |path: &std::path::Path| {
                    std::fs::canonicalize(path).map_err(|error| {
                        Anomaly::new(rtdb_types::AnomalyCategory::Unavailable, "rtdb.multi/storage-namespace", format!("{}: {error}", path.display()))
                    })
                };
                Ok(canonical(other_root)?.starts_with(canonical(root)?.join(deployment.as_str())))
            }
            (Self::S3 { config }, Self::S3 { config: other_config }) if config.bucket == other_config.bucket => {
                let cell = ObjectKey::new(S3_CELL_ROOT)?;
                let root = config.object_prefix(&cell)?;
                let other_root = other_config.object_prefix(&cell)?;
                let Some(relative) = other_root.as_str().strip_prefix(&format!("{root}/")) else { return Ok(false) };
                Ok(ObjectKey::new(relative)?.is_under(deployment))
            }
            _ => Ok(false),
        }
    }

    /// The storage `spec` names: `s3://BUCKET[/PREFIX]`, with `endpoint`, `region` and `path_style` as query parameters, or a local directory.
    pub fn parse(spec: &str) -> rtdb_types::Result<Self> {
        let Some(location) = spec.strip_prefix("s3://") else { return Ok(CellStorage::Local { root: PathBuf::from(spec) }) };
        let (location, query) = location.split_once('?').unwrap_or((location, ""));
        let (bucket, prefix) = location.split_once('/').unwrap_or((location, ""));
        let mut config = rtdb_s3::S3Config::new(bucket);
        config.prefix = (!prefix.trim_matches('/').is_empty()).then(|| prefix.trim_matches('/').to_owned());
        for pair in query.split('&').filter(|pair| !pair.is_empty()) {
            match pair.split_once('=') {
                Some(("endpoint", endpoint)) => config.endpoint = Some(endpoint.to_owned()),
                Some(("region", region)) => config.region = Some(region.to_owned()),
                Some(("path_style", on)) => config.path_style_access_enabled = on == "true",
                _ => return Err(rtdb_types::Anomaly::incorrect("rtdb.multi/storage-spec", format!("Unknown S3 storage parameter {pair}"))),
            }
        }
        Ok(CellStorage::S3 { config })
    }
}

pub(crate) async fn storage_namespace(bucket: &dyn ObjectStore) -> Result<uuid::Uuid> {
    if let Some(id) = existing_storage_namespace(bucket).await? { return Ok(id); }
    let marker = ObjectKey::new(STORAGE_NAMESPACE_MARKER)?;
    let id = uuid::Uuid::new_v4();
    let bytes = match bucket.put(&marker, bytes::Bytes::copy_from_slice(id.as_bytes())).await {
        Ok(()) => return Ok(id),
        Err(StoreError::ConflictingObject(_)) => bucket.get(&marker).await?,
        Err(error) => return Err(error.into()),
    };
    namespace_id(&bytes)
}

pub(crate) async fn existing_storage_namespace(bucket: &dyn ObjectStore) -> Result<Option<uuid::Uuid>> {
    match bucket.get(&ObjectKey::new(STORAGE_NAMESPACE_MARKER)?).await {
        Ok(bytes) => namespace_id(&bytes).map(Some),
        Err(StoreError::MissingObject(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn namespace_id(bytes: &[u8]) -> Result<uuid::Uuid> {
    uuid::Uuid::from_slice(bytes).map_err(|_| Anomaly::fault("rtdb.multi/storage-namespace", "The cell storage has an invalid namespace marker"))
}

pub(crate) async fn storage_namespaces(bucket: &dyn ObjectStore, keys: &dyn crate::keys::KeyService) -> Result<rtdb_directory::StorageNamespaces> {
    Ok(rtdb_directory::StorageNamespaces { objects: storage_namespace(bucket).await?, keys: keys.namespace_id()? })
}

pub(crate) fn require_storage_namespaces(record: &DeploymentRecord, namespaces: rtdb_directory::StorageNamespaces) -> Result<()> {
    match record.storage_namespaces {
        Some(stored) if stored == namespaces => Ok(()),
        Some(_) => Err(rtdb_types::Anomaly::incorrect("rtdb.multi/storage-binding-mismatch", format!("{} belongs to a different object or key namespace", record.name))),
        None => Err(rtdb_types::Anomaly::incorrect("rtdb.multi/storage-binding-missing", format!("{} has no storage binding", record.name))),
    }
}

/// How a node holds a deployment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// The writer, under the record's epoch.
    Holder,
    /// A log-fed reader that never writes.
    Subscriber,
    /// A log-fed reader that compacts, writing only compaction outputs and publishing them through the holder.
    Compactor,
    /// A reader of the deployment as of its checkpoint, from object storage alone (`ServeSnapshot`).
    Snapshot(rtdb_directory::Checkpoint),
}

/// The logkeeper streams a deployment's database reads and writes, carried in its `DatabaseConfig`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogkeeperLogConfig {
    pub servers: Vec<String>,
    /// The deployment whose key seals the stream's payloads.
    #[serde(default)]
    pub deployment: String,
    pub replica_stream: String,
    /// The ensemble the stream moves from, when the holder opening it moves it to `servers`.
    #[serde(default)]
    pub previous_servers: Option<Vec<String>>,
    /// The members `servers` are, recorded as the deployment's ensemble once a move completes.
    #[serde(default)]
    pub ensemble: Vec<String>,
    /// The writer epoch a holder opens its streams at; ignored by a subscriber.
    pub epoch: u64,
    /// A snapshot reader's checkpoint: its logs end there and never advance, so it reads no logkeeper.
    #[serde(default)]
    pub frozen_at: Option<rtdb_directory::Checkpoint>,
    /// The namespace of the deployment's log positions.
    #[serde(default)]
    pub log_epoch: i32,
    pub write_quorum: usize,
}

pub fn write_quorum(ensemble: usize) -> usize { ensemble / 2 + 1 }

pub(crate) fn select_logkeepers<'a>(key: &str, width: usize, mut selected: Vec<&'a MemberRecord>, candidates: &[&'a MemberRecord]) -> Vec<&'a MemberRecord> {
    let mut addresses: BTreeSet<_> = selected.iter().map(|member| member.address.as_str()).collect();
    let mut zones: BTreeSet<_> = selected.iter().map(|member| member.availability_zone.as_str()).collect();
    let mut same_zone = Vec::new();
    for member in rtdb_affinity::rank(key, candidates, |member| member.id.as_str()).into_iter().copied() {
        if selected.len() == width { return selected; }
        if addresses.contains(member.address.as_str()) { continue; }
        if zones.insert(member.availability_zone.as_str()) {
            addresses.insert(member.address.as_str());
            selected.push(member);
        } else {
            same_zone.push(member);
        }
    }
    let unfilled = width.saturating_sub(selected.len());
    selected.extend(same_zone.into_iter().filter(|member| addresses.insert(member.address.as_str())).take(unfilled));
    selected
}

pub(crate) fn ensemble_servers(record: &DeploymentRecord, members: &BTreeMap<MemberId, MemberRecord>) -> Result<Vec<String>> {
    record.ensemble.iter().map(|id| {
        members.get(id).filter(|member| member.role == MemberRole::Logkeeper).map(|member| member.address.clone())
            .ok_or_else(|| Anomaly::fault("rtdb.multi/unknown-logkeeper", format!("Logkeeper {id} is not a member")))
    }).collect()
}

/// The configuration a node opens `record` with, given its ensemble's addresses.
pub fn database_config(record: &DeploymentRecord, servers: Vec<String>, storage: &CellStorage, role: Role) -> DatabaseConfig {
    moving_config(record, servers, None, storage, role)
}

/// The configuration a holder opens `record` with when it moves the stream from `previous` to `servers`, the ensemble `ensemble`.
pub fn moving_config(record: &DeploymentRecord, servers: Vec<String>, moving: Option<(Vec<String>, Vec<rtdb_directory::MemberId>)>, storage: &CellStorage, role: Role) -> DatabaseConfig {
    let mut log = LogkeeperLogConfig { write_quorum: write_quorum(servers.len()), servers, deployment: record.id.clone(), replica_stream: record.stream("replica"), epoch: record.epoch, log_epoch: record.log_epoch, frozen_at: match role { Role::Snapshot(checkpoint) => Some(checkpoint), _ => None }, previous_servers: None, ensemble: record.ensemble.iter().map(|id| id.0.clone()).collect() };
    if let Some((previous, ensemble)) = moving { log.previous_servers = Some(previous); log.ensemble = ensemble.into_iter().map(|id| id.0).collect(); }
    let store = crate::store::CellStoreConfig { storage: storage.clone(), prefix: record.storage_prefix(), deployment: record.id.clone(),
        writer_epoch: (role == Role::Holder).then_some(record.epoch), compactor: role == Role::Compactor,
        snapshot_block: match role { Role::Snapshot(checkpoint) => Some(checkpoint.block), _ => None } };
    let storage = database_config::Storage::RemoteStorage(RemoteStorage {
        object_store: Some(prost_types::Any { type_url: format!("type.googleapis.com/{}", crate::store::CELL_STORAGE_TYPE), value: serde_json::to_vec(&store).expect("a store configuration serializes") }),
        epoch: Some(0),
    });
    DatabaseConfig {
        log: Some(database_config::Log::OtherLog(prost_types::Any {
            type_url: format!("type.googleapis.com/{LOGKEEPER_LOG_TYPE}"),
            value: serde_json::to_vec(&log).expect("a log configuration serializes"),
        })),
        storage: Some(storage),
        mode: match role { Role::Holder => None, Role::Subscriber | Role::Compactor | Role::Snapshot(_) => Some(1) },
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn placement_reads_live_holdings_and_follows_holder_changes() {
        use rtdb_directory::{MemberPool, MemberState, MemoryDirectory};
        let directory = MemoryDirectory::new();
        let member = |id: &str, expires, pool| MemberRecord {
            id: MemberId(id.into()), role: MemberRole::Node, address: String::new(), control_address: None,
            availability_zone: "test".into(), engine_version: "test".into(), lease_expires_at_ms: expires,
            state: MemberState::Eligible, pool, storage_namespaces: None, version: 1,
        };
        let members: BTreeMap<_, _> = [
            member("node", 20, MemberPool::General),
            member("isolated", 20, MemberPool::Isolation),
            member("expired", 5, MemberPool::General),
            member("worker", 20, MemberPool::Compaction),
        ].into_iter().map(|member| (member.id.clone(), member)).collect();
        let mut active = DeploymentRecord::new("active", "acme", vec![], 0);
        active.holder = Some(MemberId("node".into()));
        let active = directory.create_deployment(active).await.unwrap();
        for holder in [None, Some("expired"), Some("worker")] {
            let mut record = DeploymentRecord::new(holder.unwrap_or("parked"), "acme", vec![], 0);
            record.holder = holder.map(|id| MemberId(id.into()));
            directory.create_deployment(record).await.unwrap();
        }
        assert_eq!(live_holdings(directory.as_ref(), &members, 10).await.unwrap(), [active.clone()]);
        let mut moved = active.clone();
        moved.holder = Some(MemberId("isolated".into()));
        moved.epoch += 1;
        moved.lifecycle = rtdb_directory::Lifecycle::Quarantined;
        let moved = directory.replace_deployment(moved, active.version).await.unwrap().applied().unwrap();
        assert!(directory.deployments_held_by(&MemberId("node".into())).await.unwrap().is_empty());
        assert_eq!(live_holdings(directory.as_ref(), &members, 10).await.unwrap(), [moved]);
    }

    #[test]
    fn an_ensemble_prefers_distinct_zones_and_keeps_its_existing_live_members() {
        let member = |id: &str, address: &str, zone: &str| MemberRecord {
            id: MemberId(id.into()), role: MemberRole::Logkeeper, address: address.into(), control_address: None,
            availability_zone: zone.into(), engine_version: "test".into(), lease_expires_at_ms: 100,
            state: rtdb_directory::MemberState::Eligible, pool: Default::default(), storage_namespaces: None, version: 1,
        };
        let (a, alternative, b, c, reused) = (member("a", "a", "one"), member("a2", "a2", "one"),
            member("b", "b", "two"), member("c", "c", "three"), member("reused", "a", "one"));
        let candidates = [&a, &alternative, &b, &c, &reused];
        for index in 0..50 {
            let key = format!("deployment-{index}");
            let selected = select_logkeepers(&key, 3, Vec::new(), &candidates);
            assert_eq!(selected.iter().map(|member| &member.availability_zone).collect::<BTreeSet<_>>().len(), 3);
            let replacement = select_logkeepers(&key, 3, vec![&a, &b], &candidates);
            assert_eq!(replacement, [&a, &b, &c]);
        }
        let updated_zone = member("updated", "a2", "two");
        for index in 0..50 {
            let candidate = select_logkeepers(&format!("moving-{index}"), 2, vec![&a], &[&alternative, &updated_zone]);
            assert_eq!(candidate, [&a, &updated_zone]);
        }
        let local = select_logkeepers("local", 3, Vec::new(), &[&a, &alternative, &reused]);
        assert_eq!(local.len(), 2);
        assert_ne!(local[0].address, local[1].address);
    }

    #[test]
    fn a_holder_opens_its_streams_at_the_record_epoch_under_its_own_prefix() {
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        record.epoch = 4;
        let config = database_config(&record, vec!["a".into(), "b".into(), "c".into()], &CellStorage::Local { root: "/cell".into() }, Role::Holder);
        let Some(database_config::Log::OtherLog(any)) = &config.log else { panic!("a logkeeper log") };
        let log: LogkeeperLogConfig = serde_json::from_slice(&any.value).unwrap();
        assert_eq!((log.epoch, log.write_quorum, log.replica_stream.as_str()), (4, 2, format!("{}/1/replica", record.id).as_str()));
        let Some(database_config::Storage::RemoteStorage(remote)) = &config.storage else { panic!("cell storage") };
        let store: crate::store::CellStoreConfig = serde_json::from_slice(&remote.object_store.as_ref().unwrap().value).unwrap();
        assert!(store.prefix.ends_with(&format!("{}/1", record.id)) && store.writer_epoch == Some(4));
        assert_eq!(config.mode, None);
        assert_eq!(database_config(&record, vec![], &CellStorage::Local { root: "/cell".into() }, Role::Subscriber).mode, Some(1), "a subscriber is read-only");
    }
}
