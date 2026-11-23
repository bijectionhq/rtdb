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
use rtdb_types::{Anomaly, AnomalyCategory, JournalEndpoint, Result};
use serde::{Deserialize, Serialize};

mod state_file;
pub mod feed;
pub mod policy;
pub mod ordered_index;
mod watch;
#[cfg(feature = "network")]
pub mod network;
#[cfg(feature = "dynamodb")]
pub mod dynamo;

pub use feed::ChangeFeed;
pub use policy::{claim, effective_holder, park, suspend_holding};
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResidentIdle {
    pub epoch: u64,
    pub last_activity_at_ms: i64,
}

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resident_idle: Option<ResidentIdle>,
    /// The logkeepers holding this deployment's streams.
    pub ensemble: Vec<MemberId>,
    /// Every journal endpoint authorized to receive this installation's
    /// replica stream, retained before a new ensemble receives a copy.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub replica_journals: BTreeSet<JournalEndpoint>,
    /// Serving and archival storage admissions, retained until the owner
    /// drains its I/O and any ambiguous publications are confirmed.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub storage_regions: BTreeMap<uuid::Uuid, StorageRegion>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub catalog_readers: BTreeMap<uuid::Uuid, CatalogReader>,
    /// Installed catalog admission floors, keyed by storage root.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub catalog_admission_floors: BTreeMap<String, CatalogAdmissionFloor>,
    /// Page custody for ordered indexes, keyed by their physical index prefix.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub ordered_indexes: BTreeMap<String, OrderedIndexStorage>,
    /// Published definition sets, keyed by their serving store prefix.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub ordered_index_catalogs: BTreeMap<String, OrderedIndexCatalog>,
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
    pub move_from: Option<Vec<JournalEndpoint>>,
    /// Endpoints a stream copy may have reached, including incomplete transfers and retired ensembles.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub log_copy_servers: Vec<JournalEndpoint>,
    /// Whether its holder confirms its leadership with the logkeepers before serving each query.
    #[serde(default)]
    pub linearizable_reads: bool,
    /// The deployment's last published block, from which a cold start finds its latest without listing storage.
    #[serde(default)]
    pub checkpoint: Option<Checkpoint>,
    /// Commitment to this checkpoint's recovery inventory in this residence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_manifest_sha256: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replica_anchor: Option<Box<JournalAnchor>>,
    /// When a deployment that moved to another cell left this one: the tombstone's objects here are erased once the reaper's grace has passed since.
    #[serde(default)]
    pub moved_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_cleanup: Option<SourceCleanup>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cell_move: Option<Box<CellMove>>,
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
    pub deletion_record: Option<Box<DeletionRecord>>,
    pub cell_transfer: Option<Box<CellTransfer>>,
    pub disposal: Option<Box<DisposalReceipt>>,
    /// Serving and archive receipts for earlier stays in this same cell.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub retired_residences: BTreeMap<u64, RetiredResidence>,
    pub checkpoint_recovery: Option<Box<CheckpointRecovery>>,
    /// Replay retains logical transactions through this position, but the restored checkpoint
    /// replaces the former serving storage and its compaction effects through the same position.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction_replay_cutoff: Option<i64>,
    /// A historical checkpoint staged under this fresh, held installation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_stage: Option<Box<CheckpointStage>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_transfer_fence: Option<Box<CheckpointTransferFence>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_transfer_terminal: Option<Box<CheckpointTransferTerminal>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_transfer_bootstrap: Option<Box<CheckpointTransferBootstrap>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_transfer_replica: Option<Box<CheckpointTransferReplica>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint_transfer_activation: Option<Box<CheckpointTransferActivation>>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hosted_fork_stage: Option<HostedForkStage>,
    /// Terminal same-history repair receipts survive later repairs and moves.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub checkpoint_repair_receipts: BTreeMap<uuid::Uuid, CheckpointRecovery>,
    /// Every independent archive admitted before a checkpoint copy started.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub checkpoint_captures: BTreeMap<uuid::Uuid, CheckpointCapture>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub checkpoint_worlds: BTreeMap<uuid::Uuid, CheckpointWorldRetention>,
    /// Named, expiring replay holds rooted in an independently archived checkpoint.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub checkpoint_suffix_pins: BTreeMap<uuid::Uuid, CheckpointSuffixPin>,
    /// Closed capture IDs cannot admit new copies, readers or archive keys.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub checkpoint_capture_retirements: BTreeMap<uuid::Uuid, CheckpointCaptureRetirement>,
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
pub struct CatalogAdmissionFloor {
    pub residence: String,
    pub object_namespace: uuid::Uuid,
    pub cut: rtdb_types::CatalogCut,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogReader {
    pub owner: uuid::Uuid,
    pub storage_root: String,
    pub replica_epoch: i32,
    /// Exact and restored captures can have no installed replica cut.
    pub cut: Option<rtdb_types::CatalogCut>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum StorageRegion {
    Local {
        archive: Option<uuid::Uuid>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        checkpoint_suffix: Option<uuid::Uuid>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        repair_archive: Option<uuid::Uuid>,
        #[serde(default)]
        tracks_ordered_index_pages: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        checkpoint_metadata_from: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        checkpoint_replay_from: Option<i64>,
    },
    Remote {
        #[serde(default)]
        tracks_ordered_index_pages: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        checkpoint_metadata_from: Option<i64>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        checkpoint_replay_from: Option<i64>,
        archive: Option<uuid::Uuid>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        checkpoint_suffix: Option<uuid::Uuid>,
        /// Serving I/O can read its repair archive while writing its own prefix.
        repair_archive: Option<uuid::Uuid>,
        owner: Option<HostedTask>,
        /// When closed, the region owner has joined its I/O and may issue
        /// no further requests. Uncertain publications still need evidence.
        closed: bool,
        /// Written before a conditional provider mutation is submitted.
        publications: BTreeMap<uuid::Uuid, StoragePublication>,
    },
    ReplicaReplay {
        first_required: i64,
        custody: ReplayCustody,
    },
    /// A fork has admitted provider I/O against this physical residence.
    ReferenceRead {
        custody: ReplayCustody,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        object: Option<String>,
    },
    ExactRetirement {
        object: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        release: Option<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "custody", rename_all = "snake_case")]
pub enum ReplayCustody {
    Local,
    Hosted { owner: Option<HostedTask> },
}

mod archive_custody;

impl StorageRegion {
    /// A closed region may still owe provider settlement for the archive.
    pub fn holds_archive(&self, capture: uuid::Uuid) -> bool {
        let (destination, repair) = match self {
            Self::Local { archive, repair_archive, .. } |
            Self::Remote { archive, repair_archive, .. } => (*archive, *repair_archive),
            Self::ReplicaReplay { .. } | Self::ReferenceRead { .. } | Self::ExactRetirement { .. } => return false,
        };
        archive_custody::covers_archive(destination.map(|id| id.as_u128()), repair.map(|id| id.as_u128()), capture.as_u128())
    }

    pub fn holds_checkpoint_suffix(&self, capture: uuid::Uuid, suffix: uuid::Uuid) -> bool {
        match self {
            Self::Local { archive, checkpoint_suffix, .. } |
            Self::Remote { archive, checkpoint_suffix, .. } =>
                *archive == Some(capture) && *checkpoint_suffix == Some(suffix),
            _ => false,
        }
    }

    /// Reads may begin only under an open region for the exact archive.
    pub fn admits_archive_read(&self, capture: uuid::Uuid) -> bool {
        archive_custody::may_read_archive(
            self.holds_archive(capture),
            matches!(self, Self::Remote { closed: true, .. }),
        )
    }

    pub fn checkpoint_metadata_from(&self) -> Option<i64> {
        match self {
            Self::Local { checkpoint_metadata_from, .. } |
            Self::Remote { checkpoint_metadata_from, closed: false, .. } => *checkpoint_metadata_from,
            Self::Remote { closed: true, .. } | Self::ReplicaReplay { .. } | Self::ReferenceRead { .. }
            | Self::ExactRetirement { .. } => None,
        }
    }

    pub fn release_checkpoint_startup(&mut self) {
        if let Self::Local { checkpoint_metadata_from, checkpoint_replay_from, .. }
            | Self::Remote { checkpoint_metadata_from, checkpoint_replay_from, .. } = self
        {
            *checkpoint_metadata_from = None;
            *checkpoint_replay_from = None;
        }
    }

    pub fn first_required_replica(&self) -> Option<i64> {
        match self {
            Self::Local { checkpoint_replay_from, .. }
            | Self::Remote { checkpoint_replay_from, closed: false, .. } => *checkpoint_replay_from,
            Self::Remote { closed: true, .. } | Self::ReferenceRead { .. } | Self::ExactRetirement { .. } => None,
            Self::ReplicaReplay { first_required, .. } => Some(*first_required),
        }
    }

    pub fn tracks_ordered_index_pages(&self) -> bool {
        match self {
            Self::Local { tracks_ordered_index_pages, .. } |
            Self::Remote { tracks_ordered_index_pages, .. } => *tracks_ordered_index_pages,
            Self::ReplicaReplay { .. } | Self::ReferenceRead { .. } | Self::ExactRetirement { .. } => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderedIndexCatalog {
    pub through: i64,
    pub active: BTreeSet<String>,
    pub prior_prefixes_discovered: bool,
}

/// Persistent fence and inventory of a derived index's physical generations.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderedIndexStorage {
    pub oldest_generation: u64,
    pub dropped: bool,
    pub legacy_pages: LegacyIndexPages,
    /// Lowest generation beyond the most recent discovery of closed prefixes.
    #[serde(default)]
    pub audited_before: u64,
    /// Each generation maps a retained permit to its serving storage region.
    pub generations: BTreeMap<u64, BTreeMap<uuid::Uuid, uuid::Uuid>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum LegacyIndexPages {
    #[default]
    Retained,
    Retiring,
    Removed,
}

impl OrderedIndexStorage {
    pub fn admits(&self, generation: u64) -> bool {
        ordered_index::admits(self.dropped, self.oldest_generation, generation)
    }

    pub fn retire_earlier(&mut self, generation: u64) {
        self.oldest_generation = ordered_index::advance(self.oldest_generation, generation);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointWorldRetention {
    pub world: uuid::Uuid,
    pub owner: String,
    pub capture: uuid::Uuid,
    pub manifest_sha256: String,
    #[serde(default)]
    pub schema_digest: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub suffix: Option<uuid::Uuid>,
    pub basis: rtdb_types::TransactionKey,
    pub parent_publication: Option<rtdb_types::Timestamp>,
    pub mode: rtdb_types::WorldClockMode,
    pub until: rtdb_types::Timestamp,
}

pub const MAX_RETAINED_WORLDS: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointSuffixCustody {
    Capture,
    World,
    RetiringWorld,
}

/// Exact replay prefix protected by the installation's replica trim authority.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointSuffixPin {
    pub capture: uuid::Uuid,
    pub custody: CheckpointSuffixCustody,
    pub manifest_sha256: String,
    pub checkpoint: Checkpoint,
    pub incarnation: u64,
    pub log_epoch: i32,
    pub writer_epoch: u64,
    pub basis: rtdb_types::TransactionKey,
    /// Exclusive checkpoint.boundary to inclusive replica_through.
    pub replica_through: i64,
    /// Exclusive checkpoint.source to inclusive source_through.
    pub source_through: i64,
    pub journal_interval: [i64; 2],
    pub journal_sha256: [u8; 32],
    pub expires_at_ms: i64,
    /// Accepted ciphertext digest after independent archive publication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive_sha256: Option<String>,
}

/// The original archive's release fence, including IDs cancelled before copy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointCaptureRetirement {
    pub archive: String,
    pub manifest_sha256: Option<String>,
    pub authorized_at_ms: i64,
    pub key_withdrawn_at_ms: Option<i64>,
    pub observed_empty_at_ms: Option<i64>,
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
    /// Automatically derived from a held fork's already published source.
    #[serde(default)]
    pub automatic_fork: bool,
    /// The held restore still needs this base's raw replica history to
    /// produce exact independent parents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub world_replay: Option<[u8; 32]>,
    /// Payload copying starts only after the archive's witness becomes durable.
    pub media_bound: bool,
    /// The immutable manifest accepted after verifying the complete archive.
    pub manifest_sha256: Option<String>,
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
    pub journals: BTreeSet<JournalEndpoint>,
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
    /// Digest of the adopted catalog's exact logical bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_sha256: Option<[u8; 32]>,
}

/// The digest of an inclusive replica interval, rooted in its first physical residence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalAnchor {
    pub root_incarnation: u64,
    pub interval: [i64; 2],
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointStageIntent {
    pub operation: uuid::Uuid,
    pub source: uuid::Uuid,
    pub capture: uuid::Uuid,
    pub source_manifest_sha256: String,
    pub destination: uuid::Uuid,
    pub name: String,
    pub org: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointStage {
    pub intent: CheckpointStageIntent,
    pub source_incarnation: u64,
    pub source_log_epoch: i32,
    pub checkpoint: Checkpoint,
    pub media: CellMedia,
    /// Only the closed terminal transfer owner may open this staged inventory.
    #[serde(default)]
    pub transfer_only: bool,
    /// Digest of the exact target object inventory under the destination key.
    pub staged_manifest_sha256: Option<String>,
}

/// An ordered Directory fence, distinct from a completed replica seal.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointTransferFence {
    pub destination: CheckpointStage,
    pub source_writer_epoch: u64,
    pub fenced_writer_epoch: u64,
}

/// Source owner's terminal receipt. The source log retains the certified interval.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointTransferTerminal {
    pub fence: CheckpointTransferFence,
    pub replica_through: i64,
    pub source_through: i64,
    pub basis: rtdb_types::TransactionKey,
    pub replica_sha256: Option<[u8; 32]>,
    pub lifetime_floors_sha256: [u8; 32],
}

/// Target-owned durable replay input authenticated against a source receipt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointTransferBootstrap {
    pub terminal: CheckpointTransferTerminal,
    pub ciphertext_sha256: [u8; 32],
}

/// The target's reserved genesis writer and exact physical journal ensemble.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointTransferReplica {
    pub bootstrap: CheckpointTransferBootstrap,
    pub ensemble: Vec<MemberId>,
    pub journals: Vec<JournalEndpoint>,
}

/// Consumed source receipt and the exact durable root ordered for serving.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointTransferActivation {
    pub replica: CheckpointTransferReplica,
    pub recovery_manifest_sha256: [u8; 32],
}

pub struct CheckpointTransferSelection {
    pub stage: CheckpointStage,
    pub source_version: Version,
    pub destination_version: Version,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostedForkStage {
    pub source_lineage: String,
    pub preparation: ForkPreparation,
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
        Self { lineage: id.clone(), id, name: name.into(), org: org.into(), incarnation: 1, lifecycle: Lifecycle::Live, epoch: 0, holder: None, resident_idle: None,
            ensemble, replica_journals: BTreeSet::new(), storage_regions: BTreeMap::new(), catalog_readers: BTreeMap::new(), catalog_admission_floors: BTreeMap::new(), ordered_indexes: BTreeMap::new(), ordered_index_catalogs: BTreeMap::new(), read_width: 1, activation_crashes: 0, created_at_ms: now_ms, move_intent: None, placement_move: None, log_epoch: 0, parent: None, move_from: None, log_copy_servers: Vec::new(), linearizable_reads: false, checkpoint: None, replica_anchor: None, moved_at_ms: None, source_cleanup: None, cell_move: None, storage_generation: None, storage_namespaces: None, deletion_record: None, s3_write_lease_until_ms: None, engine_pin: None, deletion_observation: None,
            cell_transfer: None, disposal: None, retired_residences: BTreeMap::new(), checkpoint_recovery: None, compaction_replay_cutoff: None, checkpoint_stage: None, checkpoint_transfer_fence: None, checkpoint_transfer_terminal: None, checkpoint_transfer_bootstrap: None, checkpoint_transfer_replica: None, checkpoint_transfer_activation: None, hosted_fork_stage: None, checkpoint_repair_receipts: BTreeMap::new(), recovery_manifest_sha256: None,
            checkpoint_captures: BTreeMap::new(), checkpoint_worlds: BTreeMap::new(), checkpoint_suffix_pins: BTreeMap::new(), checkpoint_capture_retirements: BTreeMap::new(), archive_retirement: None, version: 0 }
    }

    pub fn has_live_suffix_pins(&self, now_ms: i64) -> bool {
        self.checkpoint_suffix_pins.values().any(|pin| pin.expires_at_ms > now_ms)
    }

    pub fn retains_world_suffix(&self, suffix: uuid::Uuid, pin: &CheckpointSuffixPin, now_ms: i64) -> bool {
        (pin.archive_sha256.is_some() && self.lifecycle != Lifecycle::Deleted
            && self.checkpoint_captures.get(&pin.capture).is_some_and(|base|
                base.world_replay.is_some() && base.incarnation == pin.incarnation
                    && base.incarnation == self.incarnation
                    && base.log_epoch == pin.log_epoch && base.log_epoch == self.log_epoch))
            || self.checkpoint_worlds.values().any(|world| {
                world.capture == pin.capture && world.suffix == Some(suffix)
                    && world.until.0 > now_ms.saturating_mul(1000)
            })
    }

    pub fn is_resident_idle(&self) -> bool {
        self.holder.is_some() && self.resident_idle.is_some_and(|idle| idle.epoch == self.epoch) && self.writer_pool().is_some()
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
        if self.checkpoint_stage.is_some() || self.checkpoint_transfer_activation.is_some() {
            format!("stage-{}-{}", self.id, self.incarnation)
        } else if self.incarnation == 1 {
            self.id.clone()
        } else {
            format!("{}-{}", self.id, self.incarnation)
        }
    }

    /// The name of a physical log stream on this residence's ensemble.
    pub fn stream(&self, kind: &str) -> String { format!("{}/{}/{kind}", self.id, self.incarnation) }

    /// First replica offsets retained by open regions and captures that still need the journal.
    pub fn required_replica_offsets(&self, now_ms: i64) -> impl Iterator<Item = i64> + '_ {
        self.storage_regions.values().filter_map(StorageRegion::first_required_replica)
            .chain(self.checkpoint_captures.values().filter(|capture| {
                capture.world_replay.is_some() && capture.incarnation == self.incarnation
                    && capture.log_epoch == self.log_epoch && self.lifecycle != Lifecycle::Deleted
            }).map(|capture| capture.checkpoint.boundary.checked_add(1).filter(|offset| *offset >= 0).unwrap_or(0)))
            .chain(self.checkpoint_suffix_pins.values()
                .filter(move |pin| pin.expires_at_ms > now_ms && pin.archive_sha256.is_none())
                .map(|pin| {
                    if pin.incarnation == self.incarnation && pin.log_epoch == self.log_epoch {
                        match pin.checkpoint.boundary.checked_add(1) {
                            Some(first) if first >= 0 && first == pin.journal_interval[0] => first,
                            _ => 0,
                        }
                    } else {
                        0
                    }
                }))
    }

    /// Oversized replica records keep their payloads in the serving store
    /// until every raw replay owner has independently materialized its cut.
    pub fn required_oversized_payload_block(&self, now_ms: i64) -> Option<i64> {
        self.checkpoint_captures.values()
            .filter(|capture| capture.world_replay.is_some() && capture.incarnation == self.incarnation
                && capture.log_epoch == self.log_epoch && self.lifecycle != Lifecycle::Deleted)
            .map(|capture| capture.checkpoint.block)
            .chain(self.checkpoint_suffix_pins.values()
                .filter(|pin| pin.expires_at_ms > now_ms && pin.archive_sha256.is_none()
                    && pin.incarnation == self.incarnation && pin.log_epoch == self.log_epoch)
                .map(|pin| pin.checkpoint.block))
            .min()
    }

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
    /// Reserves the child ID before hosted fork preparation can write its objects or logs.
    async fn stage_hosted_fork(&self, child: DeploymentRecord, preparation: ForkPreparation) -> Result<DeploymentRecord>;
    /// Creates the child and its name with the parent version check and, for a shared fork, advances the lineage's reference revision in the same transaction.
    async fn create_fork(&self, child: DeploymentRecord, expected_parent: Version) -> Result<DeploymentRecord> {
        self.publish_fork(child, expected_parent, None).await
    }
    async fn publish_fork(&self, child: DeploymentRecord, expected_parent: Version, preparation: Option<ForkPublication>) -> Result<DeploymentRecord>;
    /// Monotone revision of shared fork publications in this cell. An untouched lineage starts at zero.
    async fn shared_reference_revision(&self, lineage: &str) -> Result<Version> {
        Ok(self.lineage_state(lineage).await?.reference_revision())
    }
    async fn lineage_state(&self, lineage: &str) -> Result<LineageState>;
    /// Authorizes the reserved effect against the owner revision that established its fence.
    async fn authorize_owned_retirement(&self, id: &str, expected: Version, retirement: LineageRetirement) -> Result<LineageState>;
    /// Removes the provider-confirmed retirement fence and reopens the lineage together.
    async fn settle_owned_version(&self, id: &str, expected: Version, retirement: LineageRetirement) -> Result<Cas<DeploymentRecord>>;
    async fn begin_reference_disposal(&self, id: &str, expected: Version) -> Result<ReferenceDisposal>;
    async fn settle_reference_disposal(&self, permit: &ReferenceDisposal, record: DeploymentRecord, expected: Version) -> Result<Cas<DeploymentRecord>>;
    async fn change_lineage(&self, lineage: &str, change: LineageChange) -> Result<LineageState>;
    async fn deployment(&self, id: &str) -> Result<Option<DeploymentRecord>>;
    async fn deployment_by_name(&self, name: &str) -> Result<Option<DeploymentRecord>>;
    async fn deployments(&self) -> Result<Vec<DeploymentRecord>>;
    /// Every deployment whose holder field names this member, whatever the member's liveness.
    async fn deployments_held_by(&self, member: &MemberId) -> Result<Vec<DeploymentRecord>>;
    /// Replaces the record with `record` only if the stored version is `expected`; the stored version becomes `expected + 1`.
    /// The name of a deployment never changes.
    async fn replace_deployment(&self, record: DeploymentRecord, expected: Version) -> Result<Cas<DeploymentRecord>>;

    /// Fences only the exact source writer, while reserving the exact staged target atomically.
    /// The caller must still drain and seal the original source log at the returned epoch.
    async fn fence_checkpoint_transfer(&self, _selection: &CheckpointTransferSelection) -> Result<CheckpointTransferFence> {
        Err(Anomaly::fault("rtdb.directory/unsupported-checkpoint-transfer", "This Directory cannot fence a checkpoint transfer"))
    }

    /// Records a terminal receipt minted by the installed source recovery owner.
    async fn record_checkpoint_transfer_terminal(&self, _terminal: &CheckpointTransferTerminal) -> Result<CheckpointTransferTerminal> {
        Err(Anomaly::fault("rtdb.directory/unsupported-checkpoint-transfer", "This Directory cannot record a checkpoint transfer"))
    }

    /// Orders the staged replay artifact against the original durable source receipt.
    async fn record_checkpoint_transfer_bootstrap(&self, _bootstrap: &CheckpointTransferBootstrap) -> Result<CheckpointTransferBootstrap> {
        Err(Anomaly::fault("rtdb.directory/unsupported-checkpoint-transfer", "This Directory cannot order a transfer bootstrap"))
    }

    /// Binds the target replica journals before a genesis can write to them.
    async fn reserve_checkpoint_transfer_replica(&self, _replica: &CheckpointTransferReplica) -> Result<CheckpointTransferReplica> {
        Err(Anomaly::fault("rtdb.directory/unsupported-checkpoint-transfer", "This Directory cannot reserve a transfer replica genesis"))
    }

    /// Consumes a verified replica reservation and enables its held target in the same order.
    async fn activate_checkpoint_transfer(&self, _activation: &CheckpointTransferActivation) -> Result<CheckpointTransferActivation> {
        Err(Anomaly::fault("rtdb.directory/unsupported-checkpoint-transfer", "This Directory cannot activate a checkpoint transfer"))
    }

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

mod reference_revision;
mod lineage;
pub use lineage::{ForkPreparation, ForkPublication, LineageChange, LineageRetirement, LineageState, PreparationOwner, ReferenceDisposal, RetirementWork};
use lineage::{StoredLineage, lineage_busy};

const REFERENCE_REVISION_PREFIX: &str = "~reference-revision/";

fn require_deployment_name(name: &str) -> Result<()> {
    if name.starts_with(REFERENCE_REVISION_PREFIX) {
        return Err(Anomaly::incorrect("rtdb.directory/name", "The name is reserved for the directory"));
    }
    Ok(())
}

fn shared_fork_lineage<'a>(child: &'a DeploymentRecord, parent: Option<&DeploymentRecord>, expected: Version) -> Result<Option<&'a str>> {
    let parent = parent.ok_or_else(fork_conflict)?;
    let shared = parent.org == child.org;
    let lineage = if shared { &parent.lineage } else { &child.id };
    if !reference_revision::may_publish_fork(parent.version, expected, parent.can_fork(), child.lineage == *lineage) {
        return Err(fork_conflict());
    }
    Ok(shared.then_some(child.lineage.as_str()))
}

fn fork_stage_changed() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.directory/fork-stage-changed", "The hosted fork's staged residence has changed")
}

fn stage_hosted_fork_record(mut child: DeploymentRecord, parent: &DeploymentRecord, lineage: &LineageState,
    preparation: ForkPreparation) -> Result<DeploymentRecord> {
    shared_fork_lineage(&child, Some(parent), parent.version)?;
    let owner = lineage.preparations().find_map(|(held, owner)| match owner {
        PreparationOwner::Hosted { task } if held == preparation => Some(task.clone()),
        _ => None,
    }).ok_or_else(fork_stage_changed)?;
    if child.lifecycle != Lifecycle::Live || child.holder.is_some() || child.hosted_fork_stage.is_some()
        || child.checkpoint_stage.is_some() || child.incarnation != 1 || child.epoch > 1
        || child.storage_namespaces != parent.storage_namespaces || !child.storage_regions.is_empty()
    {
        return Err(fork_stage_changed());
    }
    child.lifecycle = Lifecycle::Restoring;
    child.hosted_fork_stage = Some(HostedForkStage { source_lineage: parent.lineage.clone(), preparation });
    child.storage_regions.insert(preparation.token, StorageRegion::Remote {
        tracks_ordered_index_pages: false, checkpoint_metadata_from: None, checkpoint_replay_from: None,
        archive: None, checkpoint_suffix: None, repair_archive: None, owner, closed: false, publications: BTreeMap::new(),
    });
    Ok(child)
}

fn fork_stage_version(child: &DeploymentRecord, staged: Option<&DeploymentRecord>, parent: &DeploymentRecord,
    preparation: Option<ForkPublication>) -> Result<Option<Version>> {
    if child.hosted_fork_stage.is_some() { return Err(fork_stage_changed()); }
    let Some(ForkPublication::Hosted(preparation)) = preparation else { return Ok(None); };
    let staged = staged.ok_or_else(fork_stage_changed)?;
    let mut expected = child.clone();
    expected.lifecycle = Lifecycle::Restoring;
    expected.hosted_fork_stage = Some(HostedForkStage { source_lineage: parent.lineage.clone(), preparation });
    expected.version = staged.version;
    if child.lifecycle != Lifecycle::Live || !child.storage_regions.is_empty() || *staged != expected { return Err(fork_stage_changed()); }
    Ok(Some(staged.version))
}

fn transfer_changed() -> Anomaly {
    Anomaly::new(AnomalyCategory::Conflict, "rtdb.directory/checkpoint-transfer", "The source checkpoint or reserved destination stage changed")
}

fn prepare_checkpoint_transfer(
    source: &DeploymentRecord, destination: &DeploymentRecord, selection: &CheckpointTransferSelection,
) -> Result<(DeploymentRecord, DeploymentRecord, CheckpointTransferFence)> {
    let stage = &selection.stage;
    let intent = &stage.intent;
    let capture = source.checkpoint_captures.get(&intent.capture).ok_or_else(transfer_changed)?;
    if source.id != intent.source.to_string() || destination.id != intent.destination.to_string()
        || source.id == destination.id || source.org != intent.org || destination.org != intent.org
        || destination.name != intent.name || intent.operation.is_nil()
        || intent.capture.is_nil() || !stage.transfer_only || stage.staged_manifest_sha256.is_none()
        || source.incarnation != stage.source_incarnation || source.log_epoch != stage.source_log_epoch
        || source.checkpoint != Some(stage.checkpoint) || stage.checkpoint.root_sha256.is_none()
        || capture.checkpoint != stage.checkpoint || capture.incarnation != source.incarnation
        || capture.log_epoch != source.log_epoch || !capture.media_bound || capture.remote_disposal.is_some()
        || capture.manifest_sha256.as_deref() != Some(intent.source_manifest_sha256.as_str())
        || source.archive_retirement.is_some() || source.checkpoint_capture_retirements.contains_key(&intent.capture)
        || destination.checkpoint_stage.as_deref() != Some(stage)
        || destination.lifecycle != Lifecycle::Restoring || destination.holder.is_some()
        || destination.parent.is_some() || destination.hosted_fork_stage.is_some()
    {
        return Err(transfer_changed());
    }
    if let Some(fence) = source.checkpoint_transfer_fence.as_deref() {
        if fence.destination == *stage && destination.checkpoint_transfer_fence.as_deref() == Some(fence)
            && source.lifecycle == Lifecycle::Restoring && source.epoch == fence.fenced_writer_epoch
        {
            return Ok((source.clone(), destination.clone(), fence.clone()));
        }
        return Err(transfer_changed());
    }
    if source.version != selection.source_version || destination.version != selection.destination_version
        || source.writer_pool().is_none() || source.parent.is_some() || source.move_from.is_some()
        || source.cell_move.is_some() || source.checkpoint_stage.is_some() || source.hosted_fork_stage.is_some()
        || source.move_intent.is_some() || source.placement_move.is_some()
        || source.checkpoint_recovery.as_deref().is_some_and(|recovery| recovery.completed_at_ms.is_none())
        || source.has_live_suffix_pins(now_ms()) || destination.checkpoint_transfer_fence.is_some()
        || source.checkpoint_worlds.values().any(|world| world.until.0 > now_ms().saturating_mul(1000))
    {
        return Err(transfer_changed());
    }
    let mut source = source.clone();
    let mut destination = destination.clone();
    let fence = CheckpointTransferFence {
        destination: stage.clone(), source_writer_epoch: source.epoch,
        fenced_writer_epoch: source.epoch.checked_add(1).ok_or_else(transfer_changed)?,
    };
    source.lifecycle = Lifecycle::Restoring;
    source.epoch = fence.fenced_writer_epoch;
    source.holder = None;
    source.resident_idle = None;
    source.move_intent = None;
    source.placement_move = None;
    source.version = source.version.checked_add(1).ok_or_else(transfer_changed)?;
    destination.version = destination.version.checked_add(1).ok_or_else(transfer_changed)?;
    source.checkpoint_transfer_fence = Some(Box::new(fence.clone()));
    destination.checkpoint_transfer_fence = Some(Box::new(fence.clone()));
    Ok((source, destination, fence))
}

fn prepare_checkpoint_transfer_terminal(
    source: &DeploymentRecord, terminal: &CheckpointTransferTerminal,
) -> Result<DeploymentRecord> {
    let stage = &terminal.fence.destination;
    if source.id != stage.intent.source.to_string() || source.lifecycle != Lifecycle::Restoring
        || source.checkpoint_transfer_fence.as_deref() != Some(&terminal.fence)
        || source.epoch != terminal.fence.fenced_writer_epoch
        || terminal.replica_through < stage.checkpoint.boundary
        || terminal.source_through < stage.checkpoint.source
        || terminal.basis.system_time.0 < stage.checkpoint.system_time
        || terminal.replica_sha256.is_some() != (terminal.replica_through > stage.checkpoint.boundary)
    {
        return Err(transfer_changed());
    }
    if let Some(existing) = source.checkpoint_transfer_terminal.as_deref() {
        return if existing == terminal { Ok(source.clone()) } else { Err(transfer_changed()) };
    }
    let mut next = source.clone();
    next.version = next.version.checked_add(1).ok_or_else(transfer_changed)?;
    next.checkpoint_transfer_terminal = Some(Box::new(terminal.clone()));
    Ok(next)
}

fn prepare_checkpoint_transfer_bootstrap(
    source: &DeploymentRecord, destination: &DeploymentRecord, bootstrap: &CheckpointTransferBootstrap,
) -> Result<DeploymentRecord> {
    let terminal = &bootstrap.terminal;
    let stage = &terminal.fence.destination;
    if source.id != stage.intent.source.to_string() || destination.id != stage.intent.destination.to_string()
        || source.checkpoint_transfer_terminal.as_deref() != Some(terminal)
        || source.checkpoint_transfer_fence.as_deref() != Some(&terminal.fence)
        || source.lifecycle != Lifecycle::Restoring || source.epoch != terminal.fence.fenced_writer_epoch
        || destination.checkpoint_transfer_fence.as_deref() != Some(&terminal.fence)
        || destination.checkpoint_stage.as_deref() != Some(stage)
        || destination.lifecycle != Lifecycle::Restoring || destination.holder.is_some()
        || !destination.ensemble.is_empty() || destination.checkpoint.is_some()
    {
        return Err(transfer_changed());
    }
    if let Some(existing) = destination.checkpoint_transfer_bootstrap.as_deref() {
        return if existing == bootstrap { Ok(destination.clone()) } else { Err(transfer_changed()) };
    }
    let mut next = destination.clone();
    next.version = next.version.checked_add(1).ok_or_else(transfer_changed)?;
    next.checkpoint_transfer_bootstrap = Some(Box::new(bootstrap.clone()));
    Ok(next)
}

fn prepare_checkpoint_transfer_replica(
    source: &DeploymentRecord, destination: &DeploymentRecord, replica: &CheckpointTransferReplica,
) -> Result<DeploymentRecord> {
    if destination.checkpoint_transfer_bootstrap.as_deref() != Some(&replica.bootstrap)
        || prepare_checkpoint_transfer_bootstrap(source, destination, &replica.bootstrap)?.version != destination.version
        || replica.ensemble.is_empty() || replica.ensemble.len() != replica.journals.len()
        || replica.ensemble.iter().collect::<BTreeSet<_>>().len() != replica.ensemble.len()
        || replica.journals.iter().collect::<BTreeSet<_>>().len() != replica.journals.len()
        || replica.journals.iter().any(|endpoint| endpoint.journal().is_none())
    {
        return Err(transfer_changed());
    }
    if destination.checkpoint_transfer_replica.is_some() { return Ok(destination.clone()); }
    let mut next = destination.clone();
    next.version = next.version.checked_add(1).ok_or_else(transfer_changed)?;
    next.checkpoint_transfer_replica = Some(Box::new(replica.clone()));
    Ok(next)
}

fn prepare_checkpoint_transfer_activation(
    source: &DeploymentRecord, destination: &DeploymentRecord, activation: &CheckpointTransferActivation,
) -> Result<DeploymentRecord> {
    let replica = &activation.replica;
    let terminal = &replica.bootstrap.terminal;
    let stage = &terminal.fence.destination;
    if source.id != stage.intent.source.to_string()
        || source.checkpoint_transfer_terminal.as_deref() != Some(terminal)
        || source.checkpoint_transfer_fence.as_deref() != Some(&terminal.fence)
        || source.lifecycle != Lifecycle::Restoring || source.epoch != terminal.fence.fenced_writer_epoch
        || destination.id != stage.intent.destination.to_string()
        || destination.checkpoint_transfer_replica.as_deref() != Some(replica)
    {
        return Err(transfer_changed());
    }
    if let Some(existing) = destination.checkpoint_transfer_activation.as_deref() {
        return if existing == activation { Ok(destination.clone()) } else { Err(transfer_changed()) };
    }
    if prepare_checkpoint_transfer_replica(source, destination, replica)?.version != destination.version
        || !destination.storage_regions.is_empty()
        || terminal.fence.fenced_writer_epoch.checked_add(1).is_none()
    {
        return Err(transfer_changed());
    }
    let next_source = terminal.source_through.checked_add(1)
        .filter(|next| *next >= 0)
        .ok_or_else(transfer_changed)?;
    rtdb_types::SourcePosition::from_epoch_offset(rtdb_types::StorageEpoch(stage.source_log_epoch), next_source)?;
    let mut next = destination.clone();
    next.version = next.version.checked_add(1).ok_or_else(transfer_changed)?;
    next.checkpoint_transfer_activation = Some(Box::new(activation.clone()));
    next.checkpoint_stage = None;
    next.lifecycle = Lifecycle::Live;
    next.epoch = terminal.fence.fenced_writer_epoch;
    next.log_epoch = stage.source_log_epoch;
    next.ensemble.clone_from(&replica.ensemble);
    next.replica_journals = replica.journals.iter().cloned().collect();
    next.checkpoint = Some(stage.checkpoint);
    next.recovery_manifest_sha256 = Some(activation.recovery_manifest_sha256);
    Ok(next)
}

fn require_transfer_update(current: &DeploymentRecord, next: &DeploymentRecord) -> Result<()> {
    if current.checkpoint_transfer_fence != next.checkpoint_transfer_fence
        || current.checkpoint_transfer_terminal != next.checkpoint_transfer_terminal
        || current.checkpoint_transfer_bootstrap != next.checkpoint_transfer_bootstrap
        || current.checkpoint_transfer_replica != next.checkpoint_transfer_replica
        || current.checkpoint_transfer_activation != next.checkpoint_transfer_activation
    {
        return Err(transfer_changed());
    }
    let Some(fence) = current.checkpoint_transfer_fence.as_deref() else { return Ok(()); };
    if current.checkpoint_transfer_activation.is_some() {
        if next.checkpoint_stage.is_some() || next.log_epoch != current.log_epoch
            || next.incarnation != current.incarnation || next.storage_prefix() != current.storage_prefix()
        {
            return Err(transfer_changed());
        }
        return Ok(());
    }
    let capture = &fence.destination.intent.capture;
    if next.lifecycle != Lifecycle::Restoring || next.holder.is_some()
        || current.epoch != next.epoch || current.incarnation != next.incarnation
        || current.log_epoch != next.log_epoch || current.checkpoint != next.checkpoint
        || current.checkpoint_stage != next.checkpoint_stage || current.replica_anchor != next.replica_anchor
        || current.replica_journals != next.replica_journals || current.ensemble != next.ensemble
        || current.move_from != next.move_from || current.log_copy_servers != next.log_copy_servers
        || current.checkpoint_recovery != next.checkpoint_recovery
        || current.compaction_replay_cutoff != next.compaction_replay_cutoff
        || current.storage_namespaces != next.storage_namespaces || current.storage_prefix() != next.storage_prefix()
        || current.archive_retirement != next.archive_retirement
        || current.checkpoint_capture_retirements.get(capture) != next.checkpoint_capture_retirements.get(capture)
        || current.checkpoint_captures.get(capture) != next.checkpoint_captures.get(capture)
    {
        return Err(transfer_changed());
    }
    Ok(())
}

fn require_fork_stage_update(current: &DeploymentRecord, next: &DeploymentRecord) -> Result<()> {
    if current.hosted_fork_stage.is_none() && next.hosted_fork_stage.is_none() { return Ok(()); }
    if current.hosted_fork_stage != next.hosted_fork_stage || next.holder.is_some()
        || current.parent != next.parent || current.storage_prefix() != next.storage_prefix()
        || current.storage_namespaces != next.storage_namespaces || current.replica_journals != next.replica_journals
        || !matches!((current.lifecycle, next.lifecycle),
            (Lifecycle::Restoring, Lifecycle::Restoring | Lifecycle::Deleting)
            | (Lifecycle::Deleting, Lifecycle::Deleting | Lifecycle::Deleted)
            | (Lifecycle::Deleted, Lifecycle::Deleted))
    {
        return Err(fork_stage_changed());
    }
    Ok(())
}

fn next_reference_revision(current: Version) -> Result<Version> {
    reference_revision::advance_revision(current).ok_or_else(|| {
        Anomaly::fault("rtdb.directory/reference-revision-exhausted", "The lineage has no remaining shared reference revisions")
    })
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
    deployments: BTreeMap<String, Box<DeploymentRecord>>,
    names: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    reference_revisions: BTreeMap<String, StoredLineage>,
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
    fn replace_record(&self, mut record: DeploymentRecord, expected: Version, disposal: Option<&ReferenceDisposal>) -> Result<Cas<DeploymentRecord>> {
        let mut state = self.state()?;
        let MemoryState { deployments, held, reference_revisions, .. } = &mut *state;
        let Some(current) = deployments.get_mut(&record.id) else { return Ok(Cas::Conflict(None)); };
        if current.version != expected { return Ok(Cas::Conflict(Some(current.as_ref().clone()))); }
        if current.name != record.name { return Err(Anomaly::incorrect("rtdb.directory/rename", "A deployment's name never changes")); }
        require_fork_stage_update(current, &record)?;
        require_transfer_update(current, &record)?;
        if let Some(permit) = disposal {
            let references = reference_revisions.get(&permit.lineage).map(StoredLineage::state).unwrap_or_default();
            let next = references.finish_disposal(permit, current, &record)?;
            reference_revisions.insert(permit.lineage.clone(), StoredLineage::Current(next));
        }
        if current.holder != record.holder {
            if let Some(previous) = current.holder.clone()
                && let Some(existing) = held.get_mut(&previous)
            {
                existing.remove(&record.id);
                if existing.is_empty() { held.remove(&previous); }
            }
            if let Some(holder) = &record.holder {
                held.entry(holder.clone()).or_default().insert(record.id.clone());
            }
        }
        record.version = expected + 1;
        **current = record.clone();
        self.persist(&mut state)?;
        self.changes.publish(record.clone());
        Ok(Cas::Applied(record))
    }

    fn insert_deployment(&self, state: &mut MemoryState, mut record: DeploymentRecord,
        lineage: Option<(String, LineageState)>, stage_version: Option<Version>) -> Result<DeploymentRecord> {
        require_deployment_name(&record.name)?;
        if record.checkpoint_transfer_fence.is_some() || record.checkpoint_transfer_terminal.is_some() || record.checkpoint_transfer_bootstrap.is_some() || record.checkpoint_transfer_replica.is_some() || record.checkpoint_transfer_activation.is_some() { return Err(transfer_changed()); }
        if stage_version.is_none() && state.deployments.contains_key(&record.id) { return Err(exists("Deployment", &record.id)); }
        if record.hosted_fork_stage.is_none() && state.names.contains_key(&record.name) { return Err(exists("Deployment name", &record.name)); }
        if let Some((lineage, next)) = lineage {
            let stored = next.legacy_revision().map_or_else(|| StoredLineage::Current(next), StoredLineage::Legacy);
            state.reference_revisions.insert(lineage, stored);
        }
        record.version = stage_version.unwrap_or(0) + 1;
        if record.hosted_fork_stage.is_none() { state.names.insert(record.name.clone(), record.id.clone()); }
        if let Some(holder) = &record.holder {
            state.held.entry(holder.clone()).or_default().insert(record.id.clone());
        }
        state.deployments.insert(record.id.clone(), Box::new(record.clone()));
        self.persist(state)?;
        self.changes.publish(record.clone());
        Ok(record)
    }
}

#[async_trait]
impl Directory for MemoryDirectory {
    async fn create_deployment(&self, record: DeploymentRecord) -> Result<DeploymentRecord> {
        if record.hosted_fork_stage.is_some() { return Err(fork_stage_changed()); }
        let mut state = self.state()?;
        self.insert_deployment(&mut state, record, None, None)
    }
    async fn stage_hosted_fork(&self, child: DeploymentRecord, preparation: ForkPreparation) -> Result<DeploymentRecord> {
        let mut state = self.state()?;
        let parent = state.deployments.get(fork_parent_id(&child)?).ok_or_else(fork_conflict)?;
        let lineage = state.reference_revisions.get(&parent.lineage).map(StoredLineage::state).unwrap_or_default();
        let stage = stage_hosted_fork_record(child, parent, &lineage, preparation)?;
        self.insert_deployment(&mut state, stage, None, None)
    }
    async fn publish_fork(&self, child: DeploymentRecord, expected_parent: Version, preparation: Option<ForkPublication>) -> Result<DeploymentRecord> {
        let mut state = self.state()?;
        let parent = state.deployments.get(fork_parent_id(&child)?).map(Box::as_ref).ok_or_else(fork_conflict)?;
        let shared = shared_fork_lineage(&child, Some(parent), expected_parent)?.is_some();
        let stage_version = fork_stage_version(&child, state.deployments.get(&child.id).map(Box::as_ref), parent, preparation)?;
        let update = if shared || preparation.is_some() {
            let lineage = parent.lineage.clone();
            let current = state.reference_revisions.get(&lineage).map(StoredLineage::state).unwrap_or_default();
            Some((lineage, current.published(preparation, shared)?))
        } else { None };
        self.insert_deployment(&mut state, child, update, stage_version)
    }
    async fn lineage_state(&self, lineage: &str) -> Result<LineageState> {
        Ok(self.state()?.reference_revisions.get(lineage).map(StoredLineage::state).unwrap_or_default())
    }
    async fn change_lineage(&self, lineage: &str, change: LineageChange) -> Result<LineageState> {
        let mut state = self.state()?;
        let current = state.reference_revisions.get(lineage).map(StoredLineage::state).unwrap_or_default();
        let next = current.changed(change)?;
        if next != current {
            state.reference_revisions.insert(lineage.to_owned(), StoredLineage::Current(next.clone()));
            self.persist(&mut state)?;
        }
        Ok(next)
    }
    async fn authorize_owned_retirement(&self, id: &str, expected: Version, retirement: LineageRetirement) -> Result<LineageState> {
        let mut state = self.state()?;
        let owner = state.deployments.get(id).filter(|owner| owner.version == expected).ok_or_else(lineage_busy)?;
        let lineage = owner.lineage.clone();
        let current = state.reference_revisions.get(&lineage).map(StoredLineage::state).unwrap_or_default();
        let next = current.authorize_exact_owned(owner, &retirement)?;
        if next != current {
            state.reference_revisions.insert(lineage, StoredLineage::Current(next.clone()));
            self.persist(&mut state)?;
        }
        Ok(next)
    }
    async fn settle_owned_version(&self, id: &str, expected: Version, retirement: LineageRetirement) -> Result<Cas<DeploymentRecord>> {
        let mut state = self.state()?;
        let Some(owner) = state.deployments.get(id) else { return Ok(Cas::Conflict(None)); };
        if owner.version != expected { return Ok(Cas::Conflict(Some(owner.as_ref().clone()))); }
        let lineage = owner.lineage.clone();
        let current = state.reference_revisions.get(&lineage).map(StoredLineage::state).unwrap_or_default();
        let (next, next_lineage) = current.settle_exact_owned(owner, &retirement)?;
        require_transfer_update(owner, &next)?;
        state.deployments.insert(id.to_owned(), Box::new(next.clone()));
        state.reference_revisions.insert(lineage, StoredLineage::Current(next_lineage));
        self.persist(&mut state)?;
        self.changes.publish(next.clone());
        Ok(Cas::Applied(next))
    }
    async fn begin_reference_disposal(&self, id: &str, expected: Version) -> Result<ReferenceDisposal> {
        let mut state = self.state()?;
        let owner = state.deployments.get(id).filter(|owner| owner.version == expected).ok_or_else(lineage::disposal_changed)?;
        let lineage = owner.lineage.clone();
        let current = state.reference_revisions.get(&lineage).map(StoredLineage::state).unwrap_or_default();
        let (permit, next) = current.begin_disposal(owner)?;
        if current != next {
            state.reference_revisions.insert(lineage, StoredLineage::Current(next));
            self.persist(&mut state)?;
        }
        Ok(permit)
    }
    async fn settle_reference_disposal(&self, permit: &ReferenceDisposal, record: DeploymentRecord, expected: Version) -> Result<Cas<DeploymentRecord>> {
        self.replace_record(record, expected, Some(permit))
    }
    async fn deployment(&self, id: &str) -> Result<Option<DeploymentRecord>> { Ok(self.state()?.deployments.get(id).map(Box::as_ref).cloned()) }
    async fn deployment_by_name(&self, name: &str) -> Result<Option<DeploymentRecord>> {
        if name.starts_with(REFERENCE_REVISION_PREFIX) { return Ok(None); }
        let state = self.state()?;
        Ok(state.names.get(name).and_then(|id| state.deployments.get(id)).map(Box::as_ref).cloned())
    }
    async fn deployments(&self) -> Result<Vec<DeploymentRecord>> { Ok(self.state()?.deployments.values().map(Box::as_ref).cloned().collect()) }
    async fn deployments_held_by(&self, member: &MemberId) -> Result<Vec<DeploymentRecord>> {
        let state = self.state()?;
        Ok(state.held.get(member).into_iter().flatten().map(|id| state.deployments[id].as_ref().clone()).collect())
    }
    async fn fence_checkpoint_transfer(&self, selection: &CheckpointTransferSelection) -> Result<CheckpointTransferFence> {
        let mut state = self.state()?;
        let source_id = selection.stage.intent.source.to_string();
        let destination_id = selection.stage.intent.destination.to_string();
        let source = state.deployments.get(&source_id).ok_or_else(transfer_changed)?;
        let destination = state.deployments.get(&destination_id).ok_or_else(transfer_changed)?;
        let (fenced, held, fence) = prepare_checkpoint_transfer(source, destination, selection)?;
        if fenced.version == source.version { return Ok(fence); }
        if let Some(previous) = source.holder.clone()
            && let Some(installations) = state.held.get_mut(&previous)
        {
            installations.remove(&source_id);
            if installations.is_empty() { state.held.remove(&previous); }
        }
        state.deployments.insert(source_id, Box::new(fenced.clone()));
        state.deployments.insert(destination_id, Box::new(held.clone()));
        self.persist(&mut state)?;
        self.changes.publish(fenced);
        self.changes.publish(held);
        Ok(fence)
    }

    async fn record_checkpoint_transfer_terminal(&self, terminal: &CheckpointTransferTerminal) -> Result<CheckpointTransferTerminal> {
        let mut state = self.state()?;
        let id = terminal.fence.destination.intent.source.to_string();
        let source = state.deployments.get(&id).ok_or_else(transfer_changed)?;
        let next = prepare_checkpoint_transfer_terminal(source, terminal)?;
        if next.version != source.version {
            state.deployments.insert(id, Box::new(next.clone()));
            self.persist(&mut state)?;
            self.changes.publish(next);
        }
        Ok(terminal.clone())
    }

    async fn record_checkpoint_transfer_bootstrap(&self, bootstrap: &CheckpointTransferBootstrap) -> Result<CheckpointTransferBootstrap> {
        let mut state = self.state()?;
        let intent = &bootstrap.terminal.fence.destination.intent;
        let source = state.deployments.get(&intent.source.to_string()).ok_or_else(transfer_changed)?;
        let id = intent.destination.to_string();
        let destination = state.deployments.get(&id).ok_or_else(transfer_changed)?;
        let next = prepare_checkpoint_transfer_bootstrap(source, destination, bootstrap)?;
        if next.version != destination.version {
            state.deployments.insert(id, Box::new(next.clone()));
            self.persist(&mut state)?;
            self.changes.publish(next);
        }
        Ok(bootstrap.clone())
    }

    async fn reserve_checkpoint_transfer_replica(&self, replica: &CheckpointTransferReplica) -> Result<CheckpointTransferReplica> {
        let mut state = self.state()?;
        let intent = &replica.bootstrap.terminal.fence.destination.intent;
        let source = state.deployments.get(&intent.source.to_string()).ok_or_else(transfer_changed)?;
        let id = intent.destination.to_string();
        let destination = state.deployments.get(&id).ok_or_else(transfer_changed)?;
        let next = prepare_checkpoint_transfer_replica(source, destination, replica)?;
        let reserved = next.checkpoint_transfer_replica.as_deref().ok_or_else(transfer_changed)?.clone();
        if next.version != destination.version {
            state.deployments.insert(id, Box::new(next.clone()));
            self.persist(&mut state)?;
            self.changes.publish(next);
        }
        Ok(reserved)
    }

    async fn activate_checkpoint_transfer(&self, activation: &CheckpointTransferActivation) -> Result<CheckpointTransferActivation> {
        let mut state = self.state()?;
        let intent = &activation.replica.bootstrap.terminal.fence.destination.intent;
        let source = state.deployments.get(&intent.source.to_string()).ok_or_else(transfer_changed)?;
        let id = intent.destination.to_string();
        let destination = state.deployments.get(&id).ok_or_else(transfer_changed)?;
        let next = prepare_checkpoint_transfer_activation(source, destination, activation)?;
        if next.version != destination.version {
            state.deployments.insert(id, Box::new(next.clone()));
            self.persist(&mut state)?;
            self.changes.publish(next);
        }
        Ok(activation.clone())
    }

    async fn replace_deployment(&self, record: DeploymentRecord, expected: Version) -> Result<Cas<DeploymentRecord>> {
        self.replace_record(record, expected, None)
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

    #[tokio::test]
    async fn a_local_lineage_reservation_survives_reopening_a_revision_only_directory() {
        let root = std::env::temp_dir().join(format!("rtdb-lineage-{}", uuid::Uuid::new_v4()));
        let lineage = "retained-lineage";
        std::fs::create_dir(&root).unwrap();
        MemoryDirectory::initialize(&root).unwrap();
        let directory = MemoryDirectory::open(&root).unwrap();
        {
            let mut state = directory.state().unwrap();
            state.reference_revisions.insert(lineage.into(), StoredLineage::Legacy(2));
            directory.persist(&mut state).unwrap();
        }
        drop(directory);
        let directory = MemoryDirectory::open(&root).unwrap();
        let retirement = LineageRetirement { token: uuid::Uuid::new_v4(), source: "retained-residence".into(), work: RetirementWork::Local { local_volume: [1; 32] } };
        directory.change_lineage(lineage, LineageChange::ReserveRetirement {
            retirement: retirement.clone(), observed_references: 2, generation: 0,
        }).await.unwrap();
        drop(directory);
        let directory = MemoryDirectory::open(&root).unwrap();
        let state = directory.lineage_state(lineage).await.unwrap();
        assert_eq!(state.retirement(), Some(&retirement));
        assert_eq!(state.reference_revision(), 2);
        assert_eq!(state.retirement_generation(), 1);
        drop(directory);
        std::fs::remove_dir_all(root).unwrap();
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
