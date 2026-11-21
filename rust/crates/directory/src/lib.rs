//! The cell directory (`allium/rtdb-multi.allium`, contract `Directory`).
//!
//! The directory is the authority for every deployment's placement, writer epoch and lifecycle, and for every member's liveness.
//! Every change is a replace conditional on the record's version, so a writer that has been superseded can change nothing.
//! It is never on the commit path: a placed holder commits and serves reads while the directory is unavailable.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use parking_lot::Mutex;
use rtdb_types::{Anomaly, AnomalyCategory, Result};
use serde::{Deserialize, Serialize};

mod state_file;
pub mod feed;
pub mod policy;
pub mod watch;
#[cfg(feature = "network")]
pub mod network;
#[cfg(feature = "dynamodb")]
pub mod dynamo;

pub use feed::ChangeFeed;
pub use policy::{claim, effective_holder, park};
pub use watch::watch_deployment;
#[cfg(feature = "network")]
pub use network::{serve, RemoteDirectory};
#[cfg(feature = "dynamodb")]
pub use dynamo::{DynamoDirectory, DynamoTables};

/// Bumped by the directory on every applied replace; the condition every replace is checked against.
pub type Version = u64;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MemberId(pub String);

impl MemberId {
    pub fn fresh() -> Self { Self(uuid::Uuid::new_v4().to_string()) }
    pub fn as_str(&self) -> &str { &self.0 }
}

impl std::fmt::Display for MemberId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(&self.0) }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberRole { Node, Gateway, Logkeeper, Controller }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberState {
    /// Registered and warming; receives no placements.
    Joining,
    /// May hold, serve and receive placements.
    Eligible,
    /// Keeps and serves what it holds, receives nothing new, hands everything off.
    Draining,
    /// Holds nothing and has left.
    Retired,
    /// Its lease expired or it was confirmed suspect; everything it held is unheld.
    Dead,
}

/// A process assignment.
/// Compute members are scoped to one process lifetime. A logkeeper ID
/// belongs to its original journal and can regain a lease after restart.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemberRecord {
    pub id: MemberId,
    pub role: MemberRole,
    /// Where clients reach it: the pgwire address of a node or gateway, the log address of a logkeeper.
    pub address: String,
    /// Where the cell reaches its agent: activation, park and handoff requests to a node.
    pub control_address: Option<String>,
    pub availability_zone: String,
    pub engine_version: String,
    pub lease_expires_at_ms: i64,
    pub state: MemberState,
    /// Which nodes may hold what: a quarantined deployment is placed only on the isolation pool.
    #[serde(default)]
    pub pool: MemberPool,
    pub version: Version,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberPool {
    #[default]
    General,
    Isolation,
    /// Compaction workers: they hold nothing, and compact the deployments that rank them first.
    Compaction,
}

impl MemberRecord {
    pub fn is_live(&self, now_ms: i64) -> bool {
        matches!(self.state, MemberState::Joining | MemberState::Eligible | MemberState::Draining) && self.lease_expires_at_ms > now_ms
    }
    /// A holder counts only while this holds: a dead member's placements are unheld with no write to them.
    pub fn may_hold(&self, now_ms: i64) -> bool {
        self.role == MemberRole::Node && matches!(self.state, MemberState::Eligible | MemberState::Draining) && self.lease_expires_at_ms > now_ms
    }
    pub fn may_receive_placements(&self, now_ms: i64) -> bool {
        self.role == MemberRole::Node && self.state == MemberState::Eligible && self.lease_expires_at_ms > now_ms
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Lifecycle {
    Live,
    /// Activation crashed members repeatedly; placed only on the isolation pool.
    Quarantined,
    /// Being moved to another cell; fenced while the cutover completes.
    Moving,
    /// The current checkpoint is being repaired from its immutable archive.
    Restoring,
    /// Fenced; its objects, log and keys are being destroyed.
    Deleting,
    /// A tombstone.
    Deleted,
}

/// One deployment, as the directory records it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeploymentRecord {
    /// Minted at creation, never reused, never derived from the name.
    pub id: String,
    /// The database name clients connect with; unique within the cell.
    pub name: String,
    pub org: String,
    /// The id of the lineage's root; the deployment's own id for a root.
    pub lineage: String,
    /// Bumped when a deployment is recreated under the same name, so the new one's objects and logs never meet the old one's.
    pub incarnation: u64,
    pub lifecycle: Lifecycle,
    /// The writer fence. 0 means never held.
    pub epoch: u64,
    pub holder: Option<MemberId>,
    /// The logkeepers holding this deployment's streams.
    pub ensemble: Vec<MemberId>,
    /// Every journal endpoint authorized to receive this installation's
    /// replica stream, retained before a new ensemble receives a copy.
    pub replica_journals: BTreeSet<String>,
    /// Serving and archival storage admissions, retained until the owner
    /// drains its I/O and any ambiguous publications are confirmed.
    pub storage_regions: BTreeMap<uuid::Uuid, StorageRegion>,
    /// How many ranks of the affinity order may serve reads.
    pub read_width: u32,
    pub activation_crashes: u32,
    pub created_at_ms: i64,
    /// A planned move in progress: only this member may claim the deployment until it does.
    #[serde(default)]
    pub move_intent: Option<MemberId>,
    /// The namespace of the deployment's log positions; bumped when it starts again on fresh logs over existing objects, as a fork does.
    #[serde(default)]
    pub log_epoch: i32,
    /// The deployment a fork was taken from.
    #[serde(default)]
    pub parent: Option<String>,
    /// The logkeepers of the cell a deployment moved from, until its first holder here has moved the stream onto this cell's ensemble.
    #[serde(default)]
    pub move_from: Option<Vec<String>>,
    /// Whether its holder confirms its leadership with the logkeepers before serving each query.
    #[serde(default)]
    pub linearizable_reads: bool,
    /// The deployment's last published block, from which a cold start finds its latest without listing storage.
    #[serde(default)]
    pub checkpoint: Option<Checkpoint>,
    /// When a deployment that moved to another cell left this one: the tombstone's objects here are erased once the reaper's grace has passed since.
    #[serde(default)]
    pub moved_at_ms: Option<i64>,
    pub cell_transfer: Option<CellTransfer>,
    pub disposal: Option<DisposalReceipt>,
    pub checkpoint_recovery: Option<CheckpointRecovery>,
    /// Every independent archive admitted before a checkpoint copy started.
    pub checkpoint_captures: BTreeMap<uuid::Uuid, CheckpointCapture>,
    pub archive_retirement: Option<ArchiveRetirement>,
    pub version: Version,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum StorageRegion {
    Local,
    Remote {
        archive: Option<uuid::Uuid>,
        /// When closed, the region owner has joined its I/O and may issue
        /// no further requests. Uncertain publications still need evidence.
        closed: bool,
        /// Written before a conditional provider mutation is submitted.
        publications: BTreeMap<uuid::Uuid, String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointCapture {
    pub archive: String,
    pub incarnation: u64,
    pub log_epoch: i32,
    pub checkpoint: Checkpoint,
    /// Payload copying starts only after the archive's witness becomes durable.
    pub media_bound: bool,
    /// The bound archive provider confirmed no remaining versions or uploads.
    pub remote_disposal: Option<[u8; 32]>,
}

/// The recovery owner releases its hold independently of serving disposal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveRetirement {
    pub deployment: String,
    pub epoch: u64,
    pub authorized_at_ms: i64,
    pub observed_empty_at_ms: Option<i64>,
}

/// Both directories retain the same routing decision through a cell transfer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellTransfer {
    pub source: String,
    pub destination: String,
    pub epoch: u64,
}

/// The provider inventory shown empty by a serving disposal.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServingInventory {
    LocalDirectory,
    S3VersionsAndUploads,
}

/// The cell's completed serving disposal, retained on the installation's tombstone.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisposalReceipt {
    pub deployment: String,
    pub incarnation: u64,
    pub epoch: u64,
    pub journals: BTreeSet<String>,
    pub journals_retired_at_ms: i64,
    pub key_withdrawn_at_ms: i64,
    pub serving_prefix_observed_empty_at_ms: i64,
    pub serving_inventory: ServingInventory,
}

/// The directory's record of a deployment's last published block (value `Checkpoint`): its index, the replica offset of its boundary record, and the last source offset it covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub block: i64,
    pub boundary: i64,
    pub source: i64,
    /// The system time, in microseconds, of the last transaction the block holds.
    #[serde(default)]
    pub system_time: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointRecovery {
    pub operation: uuid::Uuid,
    pub checkpoint: Checkpoint,
    pub epoch: u64,
    pub previous_lifecycle: Lifecycle,
    pub archive: String,
    pub manifest_sha256: String,
    pub completed_at_ms: Option<i64>,
}

impl DeploymentRecord {
    pub fn new(name: impl Into<String>, org: impl Into<String>, ensemble: Vec<MemberId>, now_ms: i64) -> Self {
        let id = uuid::Uuid::new_v4().to_string();
        Self { lineage: id.clone(), id, name: name.into(), org: org.into(), incarnation: 1, lifecycle: Lifecycle::Live, epoch: 0, holder: None,
            ensemble, replica_journals: BTreeSet::new(), storage_regions: BTreeMap::new(), read_width: 1, activation_crashes: 0, created_at_ms: now_ms, move_intent: None, log_epoch: 0, parent: None, move_from: None, linearizable_reads: false, checkpoint: None, moved_at_ms: None, cell_transfer: None, disposal: None, checkpoint_recovery: None, checkpoint_captures: BTreeMap::new(), archive_retirement: None, version: 0 }
    }

    pub fn can_fork(&self) -> bool {
        matches!(self.lifecycle, Lifecycle::Live | Lifecycle::Quarantined)
            && self.move_from.is_none()
            && self.checkpoint_recovery.is_none()
    }

    /// The affinity key: stable across epochs and residency, distinct across incarnations.
    pub fn storage_identity(&self) -> String { format!("{}/{}", self.id, self.incarnation) }

    /// The only prefix this deployment's objects may live under.
    /// The leading hash spreads deployments over the object store's request partitions.
    pub fn storage_prefix(&self) -> String {
        format!("{:04x}/{}/{}/{}/{}", fnv16(self.id.as_bytes()), self.org, self.lineage, self.id, self.incarnation)
    }

    /// The name of one of this deployment's log streams on its ensemble.
    pub fn stream(&self, kind: &str) -> String { format!("{}/{}/{kind}", self.id, self.incarnation) }
}

/// FNV-1a folded to 16 bits, as Bijection's `tenant_prefix`.
pub fn fnv16(bytes: &[u8]) -> u16 {
    let mut hash: u32 = 0x811c_9dc5;
    for byte in bytes { hash ^= u32::from(*byte); hash = hash.wrapping_mul(0x0100_0193); }
    (hash ^ (hash >> 16)) as u16
}

/// The outcome of a conditional replace.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Cas<T> {
    /// The replace applied; this is the stored record, with its new version.
    Applied(T),
    /// The version did not match; this is the current record, absent if there is none.
    Conflict(Option<T>),
}

impl<T> Cas<T> {
    pub fn applied(self) -> Option<T> { match self { Self::Applied(value) => Some(value), Self::Conflict(_) => None } }
}

#[async_trait]
pub trait Directory: Send + Sync {
    /// Stores a new deployment; refuses if its id or name is taken.
    async fn create_deployment(&self, record: DeploymentRecord) -> Result<DeploymentRecord>;
    /// Publishes a fork and its name only while the expected parent is still eligible.
    async fn create_fork(&self, child: DeploymentRecord, expected_parent: Version) -> Result<DeploymentRecord>;
    async fn deployment(&self, id: &str) -> Result<Option<DeploymentRecord>>;
    async fn deployment_by_name(&self, name: &str) -> Result<Option<DeploymentRecord>>;
    async fn deployments(&self) -> Result<Vec<DeploymentRecord>>;
    /// Every deployment whose holder field names this member, whatever the member's liveness.
    async fn deployments_held_by(&self, member: &MemberId) -> Result<Vec<DeploymentRecord>>;
    /// Replaces the record with `record` only if the stored version is `expected`; the stored version becomes `expected + 1`.
    /// The name of a deployment never changes.
    async fn replace_deployment(&self, record: DeploymentRecord, expected: Version) -> Result<Cas<DeploymentRecord>>;

    /// Stores a new member; refuses if its id is taken.
    async fn register_member(&self, record: MemberRecord) -> Result<MemberRecord>;
    async fn member(&self, id: &MemberId) -> Result<Option<MemberRecord>>;
    async fn members(&self) -> Result<Vec<MemberRecord>>;
    async fn replace_member(&self, record: MemberRecord, expected: Version) -> Result<Cas<MemberRecord>>;

    /// Every deployment record applied from now on, as the backend learns of it; `None` if this backend has no feed, and its watches poll.
    /// A receiver sees each record's versions in order, possibly late; it may miss some, and then sees `RecvError::Lagged` or a later version only.
    /// Called within a Tokio runtime, since a backend may start following its source on the first call.
    fn deployment_changes(&self) -> Option<tokio::sync::broadcast::Receiver<DeploymentRecord>> { None }
}

pub fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|elapsed| elapsed.as_millis() as i64).unwrap_or(0)
}

pub fn exists(what: &str, key: &str) -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.directory/exists", format!("{what} already exists: {key}")).with_context("key", key.into())
}

fn fork_parent_id(child: &DeploymentRecord) -> Result<&str> {
    child.parent.as_deref().filter(|id| *id != child.id)
        .ok_or_else(|| Anomaly::incorrect("rtdb.directory/fork-parent", "A fork must name its parent deployment"))
}

fn fork_conflict() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.directory/fork-parent-changed", "The fork's parent changed before the child could be published")
}

fn check_fork_parent(child: &DeploymentRecord, parent: Option<&DeploymentRecord>, expected: Version) -> Result<()> {
    if parent.is_some_and(|parent| parent.version == expected && parent.can_fork()
        && parent.org == child.org && parent.lineage == child.lineage)
    {
        Ok(())
    } else {
        Err(fork_conflict())
    }
}

/// A single-process directory with an optional durable local state file.
#[derive(Default)]
pub struct MemoryDirectory {
    state: Mutex<MemoryState>,
    changes: ChangeFeed,
    file: Option<state_file::StateFile>,
}

#[derive(Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct MemoryState {
    deployments: BTreeMap<String, DeploymentRecord>,
    names: BTreeMap<String, String>,
    members: BTreeMap<MemberId, MemberRecord>,
    #[serde(skip)]
    failed: bool,
}

impl MemoryDirectory {
    pub fn new() -> Arc<Self> { Arc::new(Self::default()) }

    pub fn initialize(root: &Path) -> Result<()> {
        state_file::StateFile::lock(root)?.initialize()
    }

    pub fn open(root: &Path) -> Result<Arc<Self>> {
        let file = state_file::StateFile::lock(root)?;
        let state = file.load()?;
        Ok(Arc::new(Self { state: Mutex::new(state), changes: ChangeFeed::default(), file: Some(file) }))
    }

    fn state(&self) -> Result<parking_lot::MutexGuard<'_, MemoryState>> {
        let state = self.state.lock();
        if state.failed {
            return Err(Anomaly::new(
                AnomalyCategory::Unavailable,
                "rtdb.directory/state-uncertain",
                "The local directory must be reopened after a state write failed",
            ));
        }
        Ok(state)
    }

    fn persist(&self, state: &mut MemoryState) -> Result<()> {
        if let Some(file) = &self.file && let Err(error) = file.save(state) {
            state.failed = true;
            return Err(error);
        }
        Ok(())
    }
    fn insert_deployment(&self, state: &mut MemoryState, mut record: DeploymentRecord) -> Result<DeploymentRecord> {
        if state.deployments.contains_key(&record.id) { return Err(exists("Deployment", &record.id)); }
        if state.names.contains_key(&record.name) { return Err(exists("Deployment name", &record.name)); }
        record.version = 1;
        state.names.insert(record.name.clone(), record.id.clone());
        state.deployments.insert(record.id.clone(), record.clone());
        self.persist(state)?;
        self.changes.publish(record.clone());
        Ok(record)
    }
}

#[async_trait]
impl Directory for MemoryDirectory {
    async fn create_deployment(&self, record: DeploymentRecord) -> Result<DeploymentRecord> {
        let mut state = self.state()?;
        self.insert_deployment(&mut state, record)
    }
    async fn create_fork(&self, child: DeploymentRecord, expected_parent: Version) -> Result<DeploymentRecord> {
        let mut state = self.state()?;
        check_fork_parent(&child, state.deployments.get(fork_parent_id(&child)?), expected_parent)?;
        self.insert_deployment(&mut state, child)
    }
    async fn deployment(&self, id: &str) -> Result<Option<DeploymentRecord>> { Ok(self.state()?.deployments.get(id).cloned()) }
    async fn deployment_by_name(&self, name: &str) -> Result<Option<DeploymentRecord>> {
        let state = self.state()?;
        Ok(state.names.get(name).and_then(|id| state.deployments.get(id)).cloned())
    }
    async fn deployments(&self) -> Result<Vec<DeploymentRecord>> { Ok(self.state()?.deployments.values().cloned().collect()) }
    async fn deployments_held_by(&self, member: &MemberId) -> Result<Vec<DeploymentRecord>> {
        Ok(self.state()?.deployments.values().filter(|record| record.holder.as_ref() == Some(member)).cloned().collect())
    }
    async fn replace_deployment(&self, mut record: DeploymentRecord, expected: Version) -> Result<Cas<DeploymentRecord>> {
        let mut state = self.state()?;
        let Some(current) = state.deployments.get(&record.id) else { return Ok(Cas::Conflict(None)); };
        if current.version != expected { return Ok(Cas::Conflict(Some(current.clone()))); }
        if current.name != record.name { return Err(Anomaly::incorrect("rtdb.directory/rename", "A deployment's name never changes")); }
        record.version = expected + 1;
        state.deployments.insert(record.id.clone(), record.clone());
        self.persist(&mut state)?;
        self.changes.publish(record.clone());
        Ok(Cas::Applied(record))
    }
    async fn register_member(&self, mut record: MemberRecord) -> Result<MemberRecord> {
        let mut state = self.state()?;
        if state.members.contains_key(&record.id) { return Err(exists("Member", record.id.as_str())); }
        record.version = 1;
        state.members.insert(record.id.clone(), record.clone());
        self.persist(&mut state)?;
        Ok(record)
    }
    async fn member(&self, id: &MemberId) -> Result<Option<MemberRecord>> { Ok(self.state()?.members.get(id).cloned()) }
    async fn members(&self) -> Result<Vec<MemberRecord>> { Ok(self.state()?.members.values().cloned().collect()) }
    async fn replace_member(&self, mut record: MemberRecord, expected: Version) -> Result<Cas<MemberRecord>> {
        let mut state = self.state()?;
        let Some(current) = state.members.get(&record.id) else { return Ok(Cas::Conflict(None)); };
        if current.version != expected { return Ok(Cas::Conflict(Some(current.clone()))); }
        record.version = expected + 1;
        state.members.insert(record.id.clone(), record.clone());
        self.persist(&mut state)?;
        Ok(Cas::Applied(record))
    }
    fn deployment_changes(&self) -> Option<tokio::sync::broadcast::Receiver<DeploymentRecord>> { Some(self.changes.subscribe()) }
}

pub mod contract;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn memory_directory_meets_the_contract() {
        contract::run(MemoryDirectory::new().as_ref()).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_claims_on_memory_have_one_winner_per_epoch() {
        let directory: Arc<dyn Directory> = MemoryDirectory::new();
        contract::concurrent_claims(&[directory], 16, 25).await;
    }

    #[test]
    fn storage_prefix_names_its_deployment_and_incarnation() {
        let mut record = DeploymentRecord::new("shop", "acme", vec![], 0);
        let first = record.storage_prefix();
        assert!(first.ends_with(&format!("/acme/{}/{}/1", record.id, record.id)), "{first}");
        record.incarnation = 2;
        assert_ne!(first, record.storage_prefix());
    }
}
