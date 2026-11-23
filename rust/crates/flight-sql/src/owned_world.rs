use std::sync::Arc;

use futures::future::BoxFuture;
use rtdb_columnar::MemoryReservation;
use rtdb_node::{Database, NativeSession, Node, OwnedCountRequest, OwnedRangeCount, OwnedRangePage, OwnedRangeRequest, OwnedWorldWitness};
use rtdb_types::{EntityId, Interval, OwnedRelation, Result, StructFields, Timestamp, TransactionKey, Value, WorldClockMode};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// The placed host's closed checkpoint-world authority. Flight supplies the
/// authenticated native session and its already admitted database attachment.
pub trait OwnedWorldHost: std::fmt::Debug + Send + Sync {
    fn prepare_replay_parent<'a>(
        &'a self, call: WorldCall<'a>, binding: [u8; 32], original: Option<TransactionKey>,
    ) -> BoxFuture<'a, Result<Option<PreparedReplayParent>>>;

    fn finish_replay_parents<'a>(
        &'a self, call: WorldCall<'a>, binding: [u8; 32],
    ) -> BoxFuture<'a, Result<()>>;

    fn retain_replayed_world<'a>(
        &'a self, call: WorldCall<'a>, binding: [u8; 32], world: Uuid,
    ) -> BoxFuture<'a, Result<RetainedReplayWorld>>;

    fn open_world_read<'a>(&'a self, call: WorldCall<'a>, world: Uuid) -> BoxFuture<'a, Result<Box<dyn OwnedWorldRead>>>;
    fn fork_world_checked<'a>(&'a self, call: WorldCall<'a>, capture: Uuid, suffix: Option<Uuid>, until: Timestamp, fork: rtdb_node::OwnedWorldFork) -> BoxFuture<'a, Result<(Uuid, TransactionKey)>>;
    fn validate_world<'a>(&'a self, call: WorldCall<'a>, world: Uuid, checks: Vec<OwnedWorldWitness>, controls: Vec<rtdb_node::HostedControlWrite>, deadline: Timestamp) -> BoxFuture<'a, Result<TransactionKey>>;
    fn commit_world_batch<'a>(
        &'a self, call: WorldCall<'a>, sections: Vec<rtdb_node::OwnedWorldSection>,
        root: rtdb_node::OwnedRootSection, fingerprint: [u8; 32],
        controls: Vec<rtdb_node::HostedControlWrite>, deadline: Timestamp,
    ) -> BoxFuture<'a, Result<TransactionKey>>;
}

/// An independent destination archive at an Original replayed Root cut.
#[derive(serde::Serialize)]
pub struct PreparedReplayParent {
    pub capture: [u8; 16],
    pub suffix: Option<[u8; 16]>,
    pub manifest: String,
    pub basis: TransactionKey,
    pub logical: Timestamp,
}

/// The Directory retention awaiting the held writer's local readmission.
#[derive(serde::Serialize)]
pub struct RetainedReplayWorld {
    pub pin: [u8; 16],
    pub capture: [u8; 16],
    pub suffix: Option<[u8; 16]>,
    pub manifest: String,
    pub basis: TransactionKey,
    pub until: Timestamp,
}

pub struct WorldCall<'a> {
    pub node: &'a Node,
    pub attachment: &'a Database,
    pub session: &'a mut NativeSession,
    pub cancellation: &'a CancellationToken,
}

pub struct WorldPointResult {
    pub present: Timestamp,
    pub row: Option<WorldPointRow>,
    pub until: Option<Timestamp>,
    pub shadowed: bool,
    pub witness: Option<OwnedWorldWitness>,
}

pub struct WorldPointRow {
    pub fields: StructFields<Value>,
    pub revision: Timestamp,
    pub reservation: Arc<MemoryReservation>,
}


pub struct WorldIdentityPage {
    pub keys: Vec<String>,
    pub more: bool,
    pub reservations: Vec<Arc<MemoryReservation>>,
}

/// A verified live cut and its already admitted archived parent.
pub trait OwnedWorldRead: Send + Sync {
    fn present(&self) -> Timestamp;
    fn application_clock(&self) -> Option<Timestamp>;
    fn initial_clock(&self) -> Timestamp;
    fn clock_mode(&self) -> WorldClockMode;
    fn parent_basis(&self) -> TransactionKey;
    fn ranked_search<'a>(
        &'a self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchPage, Vec<OwnedWorldWitness>)>>;
    fn ranked_search_at<'a>(
        &'a self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp, system: Timestamp, parent_only: bool,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchPage, Vec<OwnedWorldWitness>)>>;
    fn range<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'a>, valid: Timestamp,
    ) -> BoxFuture<'a, Result<(OwnedRangePage, OwnedWorldWitness)>>;
    fn count<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedCountRequest<'a>,
    ) -> BoxFuture<'a, Result<(u64, OwnedWorldWitness)>>;
    fn measure<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedCountRequest<'a>,
    ) -> BoxFuture<'a, Result<(OwnedRangeCount, OwnedWorldWitness)>>;
    fn parent_identity_page_at<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: Option<&'a str>, limit: usize, system: Timestamp,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>>;
    fn parent_schedule_at<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Interval, system: Timestamp,
    ) -> BoxFuture<'a, Result<(Vec<(Interval, Timestamp)>, OwnedWorldWitness)>>;
    fn ranked_search_parent_at<'a>(
        &'a self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp, system: Timestamp,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchPage, Vec<OwnedWorldWitness>)>>;
    fn parent_range_at<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'a>,
        valid: Timestamp, system: Timestamp,
    ) -> BoxFuture<'a, Result<(OwnedRangePage, OwnedWorldWitness)>>;
    fn parent_count_at<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedCountRequest<'a>, system: Timestamp,
    ) -> BoxFuture<'a, Result<(u64, OwnedWorldWitness)>>;
    fn overlay_range<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'a>, valid: Timestamp,
    ) -> BoxFuture<'a, Result<(OwnedRangePage, OwnedWorldWitness)>>;
    fn overlay_range_at<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'a>,
        valid: Timestamp, system: Timestamp,
    ) -> BoxFuture<'a, Result<OwnedRangePage>>;
    fn parent_pin(&self) -> rtdb_types::Iid;
    fn until(&self) -> Timestamp;
    fn evidence_until(&self) -> Timestamp;
    fn check_relation(&self, relation: &OwnedRelation) -> Result<()>;
    fn check_parent_point(&self, relation: &OwnedRelation, until: Option<Timestamp>) -> Result<()>;
    fn check(&self) -> Result<()>;
    fn close(&self);
    fn changes<'a>(&'a self, relations: &'a [OwnedRelation], after: Timestamp, through: Timestamp, limit: usize, descending: bool)
        -> BoxFuture<'a, Result<rtdb_node::OwnedMergedChanges>>;
    fn valid_during<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Interval, parent_only: bool,
    ) -> BoxFuture<'a, Result<(Vec<(Interval, Timestamp)>, Option<OwnedWorldWitness>)>>;
    fn valid_during_at<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Interval, system: Timestamp,
    ) -> BoxFuture<'a, Result<Vec<(Interval, Timestamp)>>>;
    fn identity_page<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: Option<&'a str>, limit: usize,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>>;
    fn identity_page_at<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: Option<&'a str>, limit: usize,
        system: Timestamp, parent_only: bool,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>>;
    fn overlay_identities<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: Option<&'a str>, limit: usize,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>>;
    fn overlay_identities_at<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: Option<&'a str>,
        limit: usize, system: Timestamp,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>>;
    fn version<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId,
        from: Timestamp, through: Timestamp, group: Timestamp,
    ) -> BoxFuture<'a, Result<(Timestamp, Vec<(Interval, WorldPointResult)>)>>;
    fn path_touch<'a>(
        &'a self, relation: &'a OwnedRelation, path: &'a OwnedRelation, id: &'a EntityId,
        before: Timestamp, group: Timestamp, valid: Timestamp, images: bool,
    ) -> BoxFuture<'a, Result<[WorldPointResult; 2]>>;
    fn select_accepted_scope<'a>(
        &'a self, relations: &'a [OwnedRelation], requested: Timestamp,
    ) -> BoxFuture<'a, Result<Option<(Timestamp, Timestamp)>>>;
    fn select_parent_as_of<'a>(
        &'a self, relations: &'a [OwnedRelation], requested: Timestamp,
    ) -> BoxFuture<'a, Result<Timestamp>>;
    fn parent_point_at<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Timestamp, system: Timestamp,
    ) -> BoxFuture<'a, Result<WorldPointResult>>;
    fn preflight_accepted_scope<'a>(
        &'a self, relations: &'a [OwnedRelation], system: Timestamp,
    ) -> BoxFuture<'a, Result<Timestamp>>;
    fn accepted_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Timestamp,
        system: Timestamp,
    ) -> BoxFuture<'a, Result<WorldPointResult>>;
    fn read_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid_at: Timestamp, observed: bool,
    ) -> BoxFuture<'a, Result<WorldPointResult>>;
    fn read_parent_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid_at: Timestamp, observed: bool,
    ) -> BoxFuture<'a, Result<WorldPointResult>>;
}
