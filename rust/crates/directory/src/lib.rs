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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drain_target_engine: Option<String>,
    /// Which nodes may hold what: a quarantined deployment is placed only on the isolation pool.
    #[serde(default)]
    pub pool: MemberPool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_namespaces: Option<StorageNamespaces>,
    pub version: Version,
}

#[derive(Clone, Copy, Debug, Default, Hash, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlacementMove {
    pub request: uuid::Uuid,
    pub target: MemberId,
    pub expires_at_ms: i64,
}

/// A cross-cell move owned by the source record until cutover is complete.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellMove {
    pub destination: String,
    pub namespaces: MoveNamespaces,
    pub phase: CellMovePhase,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveNamespaces {
    pub source_objects: uuid::Uuid,
    pub destination_objects: uuid::Uuid,
    pub source_keys: [u8; 32],
    pub destination_keys: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CellMovePhase {
    Preparing { generation: uuid::Uuid },
    Ready { generation: uuid::Uuid, previous_ensemble: Vec<String>, ensemble: Vec<MemberId> },
}

impl CellMovePhase {
    pub fn generation(&self) -> uuid::Uuid {
        match self {
            Self::Preparing { generation } | Self::Ready { generation, .. } => *generation,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceCleanup {
    ObjectsErased,
    ObjectsAndKeyErased,
}

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageNamespaces {
    pub objects: uuid::Uuid,
    pub keys: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionInventory {
    CurrentObjects,
    AllVersions,
    AllVersionsAndMultipartUploads,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeletionObservation {
    Fenced { leases_expire_at_ms: i64 },
    Empty { observed_at_ms: i64 },
    Verified { first_empty_at_ms: i64, last_empty_at_ms: i64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeletionFacts {
    pub deployment: String,
    pub name: String,
    pub epoch: u64,
    pub storage_prefix: String,
    pub namespaces: StorageNamespaces,
    pub inventory: DeletionInventory,
    pub key_destroyed_at_ms: i64,
    pub objects_erased_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_empty_observed_at_ms: Option<i64>,
    pub verified_empty: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeletionRecord {
    pub facts: DeletionFacts,
    pub signature: String,
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
    /// The physical residence. Each transfer gives the destination a new
    /// incarnation while clients keep the deployment ID.
    pub incarnation: u64,
    pub lifecycle: Lifecycle,
    /// The writer fence. 0 means never held.
    pub epoch: u64,
    pub holder: Option<MemberId>,
    /// The logkeepers holding this deployment's streams.
    pub ensemble: Vec<MemberId>,
    /// Every journal endpoint authorized to receive this installation's
    /// replica stream, retained before a new ensemble receives a copy.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub replica_journals: BTreeSet<String>,
    /// Serving and archival storage admissions, retained until the owner
    /// drains its I/O and any ambiguous publications are confirmed.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub storage_regions: BTreeMap<uuid::Uuid, StorageRegion>,
    /// How many ranks of the affinity order may serve reads.
    pub read_width: u32,
    pub activation_crashes: u32,
    pub created_at_ms: i64,
    /// Kept with its original wire type for readers that predate expiring placements.
    #[serde(default)]
    pub move_intent: Option<MemberId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement_move: Option<PlacementMove>,
    /// The namespace of the deployment's log positions; bumped when it starts again on fresh logs over existing objects, as a fork does.
    #[serde(default)]
    pub log_epoch: i32,
    /// The deployment a fork was taken from.
    #[serde(default)]
    pub parent: Option<String>,
    /// Source logkeepers still needed to stage the replica tail before a cell transfer can activate.
    #[serde(default)]
    pub move_from: Option<Vec<String>>,
    /// Endpoints a stream copy may have reached, including incomplete transfers and retired ensembles.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub log_copy_servers: Vec<String>,
    /// Whether its holder confirms its leadership with the logkeepers before serving each query.
    #[serde(default)]
    pub linearizable_reads: bool,
    /// The deployment's last published block, from which a cold start finds its latest without listing storage.
    #[serde(default)]
    pub checkpoint: Option<Checkpoint>,
    /// When a deployment that moved to another cell left this one: the tombstone's objects here are erased once the reaper's grace has passed since.
    #[serde(default)]
    pub moved_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_cleanup: Option<SourceCleanup>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cell_move: Option<CellMove>,
    /// Each attempted copy uses a different prefix, so a superseded copier can never modify a live destination.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_generation: Option<uuid::Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage_namespaces: Option<StorageNamespaces>,
    /// The last time a previously issued S3 write session may still be usable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3_write_lease_until_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deletion_observation: Option<DeletionObservation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine_pin: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deletion_record: Option<DeletionRecord>,
    pub cell_transfer: Option<CellTransfer>,
    pub disposal: Option<DisposalReceipt>,
    /// Serving and archive receipts for earlier stays in this same cell.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub retired_residences: BTreeMap<u64, RetiredResidence>,
    pub checkpoint_recovery: Option<CheckpointRecovery>,
    /// Replay retains logical transactions through this position, but the restored checkpoint
    /// replaces the former serving storage and its compaction effects through the same position.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_replay_cutoff: Option<i64>,
    /// Every independent archive admitted before a checkpoint copy started.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub checkpoint_captures: BTreeMap<uuid::Uuid, CheckpointCapture>,
    pub archive_retirement: Option<ArchiveRetirement>,
    pub version: Version,
}

/// The immutable identity of the ECS task that opened a hosted storage region.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostedTask {
    cluster_arn: String,
    task_arn: String,
}

impl HostedTask {
    pub fn new(cluster_arn: &str, task_arn: &str) -> Result<Self> {
        let invalid = || Anomaly::incorrect("rtdb.directory/hosted-task", "The task ARN does not belong to the installed ECS cluster");
        let (authority, cluster) = cluster_arn.split_once(":cluster/").ok_or_else(invalid)?;
        let (region, account) = authority.strip_prefix("arn:aws:ecs:")
            .and_then(|authority| authority.split_once(':')).ok_or_else(invalid)?;
        if region.is_empty() || !region.bytes().all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            || account.len() != 12 || !account.bytes().all(|byte| byte.is_ascii_digit())
            || cluster.is_empty() || !cluster.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(invalid());
        }
        let id = task_arn.strip_prefix(&format!("{authority}:task/{cluster}/")).ok_or_else(invalid)?;
        if id.len() != 32 || !id.bytes().all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase()) {
            return Err(invalid());
        }
        Ok(Self { cluster_arn: cluster_arn.to_owned(), task_arn: task_arn.to_owned() })
    }

    pub fn cluster_arn(&self) -> &str { &self.cluster_arn }
    pub fn task_arn(&self) -> &str { &self.task_arn }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum StorageRegion {
    Local,
    Remote {
        archive: Option<uuid::Uuid>,
        owner: Option<HostedTask>,
        /// When closed, the region owner has joined its I/O and may issue
        /// no further requests. Uncertain publications still need evidence.
        closed: bool,
        /// Written before a conditional provider mutation is submitted.
        publications: BTreeMap<uuid::Uuid, StoragePublication>,
    },
}

/// An exact provider upload, admitted before it received payload bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoragePublication {
    pub key: String,
    pub upload_id: String,
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

/// Both directories retain the same intent through a cell transfer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellTransfer {
    pub intent: CellTransferIntent,
    pub source_incarnation: u64,
    pub destination_incarnation: u64,
    pub epoch: u64,
    pub source_media: CellMedia,
    pub destination_media: CellMedia,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellTransferIntent {
    /// The external catalog's operation. An RTDB-managed move uses a cell map
    /// instead and has no external operation.
    pub operation: Option<uuid::Uuid>,
    pub authority: uuid::Uuid,
    pub name: String,
    pub source: CellResidence,
    pub destination: CellResidence,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CellResidence {
    pub cell: String,
    pub org: String,
}

/// The destination has an independent copy of the installation's storage and
/// sealed tail. The selecting catalog retains this receipt before publishing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedCellTransfer {
    pub intent: CellTransferIntent,
    pub source_incarnation: u64,
    pub destination_incarnation: u64,
    pub epoch: u64,
}

/// The cell providers authorized for a transfer, without their credentials.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellMedia {
    pub objects: [u8; 32],
    pub keys: String,
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

/// The retained evidence of an earlier stay after this cell reserved the
/// name again. Pending checkpoint captures stay in the local capture inventory.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetiredResidence {
    pub transfer: CellTransfer,
    pub serving: DisposalReceipt,
    pub archives: Option<ArchiveRetirement>,
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
            ensemble, replica_journals: BTreeSet::new(), storage_regions: BTreeMap::new(), read_width: 1, activation_crashes: 0, created_at_ms: now_ms, move_intent: None, placement_move: None, log_epoch: 0, parent: None, move_from: None, log_copy_servers: Vec::new(), linearizable_reads: false, checkpoint: None, moved_at_ms: None, source_cleanup: None, cell_move: None, storage_generation: None, storage_namespaces: None, deletion_record: None, s3_write_lease_until_ms: None, engine_pin: None, deletion_observation: None,
            cell_transfer: None, disposal: None, retired_residences: BTreeMap::new(), checkpoint_recovery: None, compaction_replay_cutoff: None, checkpoint_captures: BTreeMap::new(), archive_retirement: None, version: 0 }
    }

    pub fn writer_pool(&self) -> Option<MemberPool> {
        match self.lifecycle {
            Lifecycle::Live => Some(MemberPool::General),
            Lifecycle::Quarantined => Some(MemberPool::Isolation),
            _ => None,
        }
    }

    pub fn can_fork(&self) -> bool {
        self.writer_pool().is_some() && self.move_from.is_none() && self.checkpoint_recovery.is_none()
    }

    pub fn permits_engine(&self, member: &MemberRecord) -> bool {
        self.engine_pin.as_deref().is_none_or(|pin| member.engine_version == pin)
    }

    pub fn may_hold_on(&self, member: &MemberRecord, now_ms: i64) -> bool {
        self.writer_pool() == Some(member.pool) && self.permits_engine(member) && member.may_hold(now_ms)
    }

    pub fn may_place_on(&self, member: &MemberRecord, now_ms: i64) -> bool {
        self.writer_pool() == Some(member.pool) && self.permits_engine(member) && member.may_receive_placements(now_ms)
    }

    pub fn active_move(&self, now_ms: i64) -> Option<&PlacementMove> {
        self.placement_move.as_ref().filter(|reservation| reservation.expires_at_ms > now_ms)
    }

    pub fn active_s3_write_lease(&self, now_ms: i64) -> Option<i64> {
        self.s3_write_lease_until_ms.filter(|deadline| *deadline > now_ms)
    }

    /// The affinity key of a residence, stable across writer epochs.
    pub fn storage_identity(&self) -> String { format!("{}/{}", self.id, self.incarnation) }

    /// The only prefix this deployment's objects may live under.
    /// The leading hash spreads deployments over the object store's request partitions.
    pub fn storage_prefix(&self) -> String {
        let store = self.storage_generation.map_or_else(
            || self.incarnation.to_string(),
            |generation| format!("{}-{}", self.incarnation, generation.simple()),
        );
        format!("{:04x}/{}/{}/{}/{store}", fnv16(self.id.as_bytes()), self.org, self.lineage, self.id)
    }

    /// The key custody record for one residence. Withdrawing an old
    /// residence's key cannot withdraw the current residence's key record.
    pub fn key_id(&self) -> String {
        if self.incarnation == 1 { self.id.clone() } else { format!("{}-{}", self.id, self.incarnation) }
    }

    /// The name of a physical log stream on this residence's ensemble.
    pub fn stream(&self, kind: &str) -> String { format!("{}/{}/{kind}", self.id, self.incarnation) }

    /// The ciphertext binds to the logical deployment. A transfer copies
    /// the records into the destination journal at their original offsets.
    pub fn replica_seal_scope(&self) -> String { format!("{}/1/replica", self.id) }
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
    /// Creates the child and its name atomically with checking that its parent can still be forked at this version.
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

    /// Starts the backend's shared feed on demand, when supported.
    fn change_feed(&self) -> Option<&ChangeFeed> { None }
    /// A retained observation of the exact hosting task in terminal STOPPED.
    async fn hosted_task_stopped(&self, _task: &HostedTask) -> Result<bool> { Ok(false) }

    /// Every deployment record applied from now on, as the backend learns of it; `None` if this backend has no feed, and its watches poll.
    /// A receiver sees each record's versions in order, possibly late; it may miss some, and then sees `RecvError::Lagged` or a later version only.
    /// Called within a Tokio runtime, since a backend may start following its source on the first call.
    fn deployment_changes(&self) -> Option<tokio::sync::broadcast::Receiver<DeploymentRecord>> { self.change_feed().map(ChangeFeed::subscribe) }
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
    #[serde(skip)]
    held: BTreeMap<MemberId, BTreeSet<String>>,
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
        let mut state = file.load()?;
        for deployment in state.deployments.values() {
            if let Some(holder) = &deployment.holder {
                state.held.entry(holder.clone()).or_default().insert(deployment.id.clone());
            }
        }
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
        if let Some(holder) = &record.holder {
            state.held.entry(holder.clone()).or_default().insert(record.id.clone());
        }
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
        let state = self.state()?;
        Ok(state.held.get(member).into_iter().flatten().map(|id| state.deployments[id].clone()).collect())
    }
    async fn replace_deployment(&self, mut record: DeploymentRecord, expected: Version) -> Result<Cas<DeploymentRecord>> {
        let mut state = self.state()?;
        let Some(current) = state.deployments.get(&record.id) else { return Ok(Cas::Conflict(None)); };
        if current.version != expected { return Ok(Cas::Conflict(Some(current.clone()))); }
        if current.name != record.name { return Err(Anomaly::incorrect("rtdb.directory/rename", "A deployment's name never changes")); }
        if current.holder != record.holder {
            if let Some(previous) = current.holder.clone()
                && let Some(held) = state.held.get_mut(&previous)
            {
                held.remove(&record.id);
                if held.is_empty() { state.held.remove(&previous); }
            }
            if let Some(holder) = &record.holder {
                state.held.entry(holder.clone()).or_default().insert(record.id.clone());
            }
        }
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
    fn change_feed(&self) -> Option<&ChangeFeed> { Some(&self.changes) }
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

#[cfg(test)]
mod hosted_task_tests {
    use super::HostedTask;

    #[test]
    fn a_hosted_task_must_name_the_exact_installed_cluster() {
        let cluster = "arn:aws:ecs:eu-north-1:111122223333:cluster/bj-prod-cell-test";
        let task = "arn:aws:ecs:eu-north-1:111122223333:task/bj-prod-cell-test/84d0704dd3e548d496883a065ccc01c7";
        let owner = HostedTask::new(cluster, task).unwrap();
        assert_eq!(owner.task_arn(), task);
        assert_eq!(owner.cluster_arn(), cluster);
        assert!(HostedTask::new(&cluster.replace("cell-test", "cell-other"), task).is_err());
        assert!(HostedTask::new(&cluster.replace("eu-north-1", "eu-west-1"), task).is_err());
        assert!(HostedTask::new(cluster, &task[..task.len()-1]).is_err());
    }
}
