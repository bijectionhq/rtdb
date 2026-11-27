use std::sync::Arc;

use arrow_array::{Float32Array, Float64Array, RecordBatch, cast::AsArray};
use arrow_schema::{DataType, Field};
use futures::future::BoxFuture;
use rtdb_columnar::{ArrowCell, MemoryReservation};
use rtdb_node::{OwnedChangeEdge, Database, NativeSession, Node, OwnedCountRequest, OwnedRangeCount, OwnedRangePage, OwnedRangeRequest, OwnedWorldWitness};
use rtdb_types::{EntityId, Iid, Interval, LogicalType, OwnedRelation, OwnedWorldAssertion, Result, ScalarType, StructFields, Timestamp, TransactionKey, Value, WorldClockMode};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// The placed host's closed checkpoint-world authority. Flight supplies the
/// authenticated native session and its already admitted database attachment.
pub trait OwnedWorldHost: std::fmt::Debug + Send + Sync {
    fn prepare_world_overlay<'a>(
        &'a self, call: WorldCall<'a>, cut: rtdb_node::OwnedForkCut, child: Uuid, retain_until: Timestamp, writable: bool,
    ) -> BoxFuture<'a, Result<PreparedWorldOverlay>>;

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
    fn commit_world_batch<'a>(
        &'a self, call: WorldCall<'a>, sections: Vec<rtdb_node::OwnedWorldSection>,
        root: rtdb_node::OwnedRootSection, observed: rtdb_node::OwnedWorldObservations, fingerprint: [u8; 32],
        controls: Vec<rtdb_node::HostedControlWrite>, deadline: Timestamp,
    ) -> BoxFuture<'a, Result<TransactionKey>>;

    fn root_rank_authority(&self, call: WorldCall<'_>) -> Result<Arc<dyn rtdb_storage::sealing::RootRankAuthority>>;

    /// The placed deployment's assertion-family vault (dev/doc/rtdb-assertion-families.md).
    fn assertion_family<'a>(
        &'a self, call: WorldCall<'a>, request: crate::FamilyRequest,
    ) -> BoxFuture<'a, Result<crate::FamilyReply>>;

    fn supports_retired_original_family(&self) -> bool { false }

    fn retired_family_fence<'a>(
        &'a self, _call: WorldCall<'a>, _request: &'a crate::RetiredFamilyRequest,
    ) -> BoxFuture<'a, Result<bytes::Bytes>> {
        Box::pin(async { Err(rtdb_types::OwnedWorld::unavailable()) })
    }
}

/// The independently archived overlay position and its bounded preparation lease.
#[derive(serde::Serialize)]
pub struct PreparedWorldOverlay {
    pub child: [u8; 16],
    pub world: [u8; 16],
    pub parent_pin: [u8; 16],
    pub capture: [u8; 16],
    pub suffix: [u8; 16],
    pub manifest: String,
    pub basis: TransactionKey,
    pub publication: TransactionKey,
    pub logical: Timestamp,
    pub until: Timestamp,
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
    pub ancestors: Vec<rtdb_types::OwnedWorldAncestor>,
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
    pub frozen_winner: Option<FrozenWorldWinner>,
}

/// The original event and accepted Held reading of a frozen overlay winner.
#[derive(Clone, Copy)]
pub struct FrozenWorldWinner {
    pub author: Iid,
    pub publication: TransactionKey,
    pub event_ordinal: u64,
    pub reading: Timestamp,
}

pub struct WorldPointRow {
    pub batch: RecordBatch,
    pub revision: Timestamp,
    pub assertion: Option<OwnedWorldAssertion>,
    pub reservation: Arc<MemoryReservation>,
}


impl WorldPointRow {
    #[doc(hidden)]
    pub fn document_bytes(batch: &RecordBatch) -> Result<usize> {
        let shape = batch.schema_ref().fields().iter().map(|field| Ok((field.name().clone(), Value::Null)))
            .collect::<Result<StructFields<_>>>()?;
        let size = Value::document_retained_bytes(&shape)?;
        batch.schema_ref().fields().iter().zip(batch.columns()).try_fold(size, |bytes, (field, array)| {
            let selected = ArrowCell { field, array: array.as_ref(), row: 0 }.physical()?;
            let logical = selected.logical_type()?;
            let heap = match &logical {
                LogicalType::List { .. } | LogicalType::Struct(_) => {
                    let cell = ArrowCell { field, array: array.as_ref(), row: 0 };
                    let concrete = rtdb_columnar::to_field(field.name(), &logical).ok()
                        .and_then(|target| concrete_point_bytes(cell, selected, &logical, &target));
                    match concrete {
                        Some(bytes) => bytes,
                        None => {
                            let value = rtdb_columnar::read_value(field, array.as_ref(), 0)?;
                            value.retained_bytes()?.checked_sub(std::mem::size_of::<Value>())
                                .ok_or_else(rtdb_types::OwnedWorld::unavailable)?
                        }
                    }
                }
                _ => scalar_point_bytes(selected, &logical),
            };
            bytes.checked_add(heap).ok_or_else(rtdb_types::OwnedWorld::unavailable)
        })
    }
}

fn scalar_point_bytes(cell: ArrowCell<'_>, logical: &LogicalType) -> usize {
    let ArrowCell { array, row, .. } = cell;
    match logical {
        LogicalType::Scalar(ScalarType::Utf8 | ScalarType::Keyword | ScalarType::Uri) =>
            array.as_string::<i32>().value(row).len(),
        LogicalType::Scalar(ScalarType::Binary | ScalarType::Transit) =>
            array.as_binary::<i32>().value(row).len(),
        LogicalType::Scalar(ScalarType::FixedSizeBinary(_)) => array.as_fixed_size_binary().value(row).len(),
        LogicalType::Scalar(ScalarType::Timestamp { timezone, .. }) => timezone.as_ref().map_or(0, String::len),
        LogicalType::Scalar(ScalarType::Decimal { bit_width, .. }) => usize::from(bit_width / 8),
        _ => 0,
    }
}

pub(crate) fn concrete_point_bytes(cell: ArrowCell<'_>, selected: ArrowCell<'_>, logical: &LogicalType, target: &Field) -> Option<usize> {
    if selected.field.data_type() != target.data_type() || selected.field.metadata() != target.metadata() {
        return None;
    }
    if !std::ptr::eq(cell.field, selected.field) {
        let (_, interpreted) = cell.logical_position(None).ok()?;
        if &rtdb_columnar::from_field(selected.field).ok()? != &interpreted { return None; }
    }
    concrete_heap_bytes(selected, logical)
}

fn concrete_heap_bytes(cell: ArrowCell<'_>, logical: &LogicalType) -> Option<usize> {
    match logical {
        LogicalType::Null | LogicalType::Nothing => Some(0),
        LogicalType::Scalar(ScalarType::Uri) => None,
        LogicalType::Scalar(_) if !cell.array.is_null(cell.row) => Some(scalar_point_bytes(cell, logical)),
        LogicalType::Struct(types) => {
            let mut shape = StructFields::new();
            let mut heap = 0usize;
            for child in cell.structure()? {
                let logical = types.get(child.field.name())?;
                heap = heap.checked_add(concrete_heap_bytes(child, logical)?)?;
                shape.insert(child.field.name().clone(), Value::Null);
            }
            heap.checked_add(Value::document_retained_bytes(&shape).ok()?)
        }
        LogicalType::List { kind: rtdb_types::ListKind::List | rtdb_types::ListKind::FixedSizeList(_), element } => {
            let (item, values, range) = cell.sequence_range()?;
            if range.is_empty() || item.is_nullable() || !item.metadata().is_empty()
                || values.nulls().is_some_and(|nulls| nulls.slice(range.start, range.len()).null_count() != 0)
            {
                return None;
            }
            let (header, width) = match (element.as_ref(), item.data_type()) {
                (LogicalType::Scalar(ScalarType::F32), DataType::Float32) => (std::mem::size_of::<Float32Array>(), 4),
                (LogicalType::Scalar(ScalarType::F64), DataType::Float64) => (std::mem::size_of::<Float64Array>(), 8),
                _ => return None,
            };
            // The owned float reader copies the selected components into a Vec of this length.
            header.checked_add(range.len().checked_mul(width)?)
        }
        _ => None,
    }
}


pub struct WorldIdentityPage {
    pub keys: Vec<EntityId>,
    pub more: bool,
    pub reservations: Vec<Arc<MemoryReservation>>,
}

pub struct WorldParentVersion {
    pub sequence: u64,
    pub system: Timestamp,
    pub timeline: Vec<rtdb_columnar::OwnedBatch>,
}

pub struct WorldParentVersions {
    pub floor: Timestamp,
    pub next_revision: u64,
    pub done: bool,
    pub scanned: usize,
    pub versions: Vec<WorldParentVersion>,
}

/// A verified live cut and its already admitted archived parent.
pub trait OwnedWorldRead: Send + Sync {
    fn retained_child_cut<'a>(
        &'a self, scope: rtdb_types::PlacedAttemptScope, publication: TransactionKey,
    ) -> BoxFuture<'a, Result<rtdb_node::OwnedForkCut>>;
    fn present(&self) -> Timestamp;
    fn ancestors(&self) -> Vec<(rtdb_types::Iid, Timestamp)>;
    fn ancestor(&self) -> Option<(rtdb_types::Iid, Timestamp)>;
    /// Local logical reading and its ordered physical revision.
    fn application_publication(&self) -> Option<(Timestamp, Timestamp)>;
    fn application_clock(&self) -> Option<Timestamp> {
        self.application_publication().map(|(reading, _)| reading)
    }
    fn initial_clock(&self) -> Timestamp;
    fn clock_mode(&self) -> WorldClockMode;
    fn parent_basis(&self) -> TransactionKey;
    fn ranked_search<'a>(
        &'a self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp, staged: Option<rtdb_node::TextShadows<'a>>,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)>>;
    fn ranked_search_at<'a>(
        &'a self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp, system: Timestamp, parent_only: bool,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)>>;
    /// Exact cosine neighbours of the effective world view (`NearestHeldWorld.v1`).
    fn nearest<'a>(
        &'a self, partitions: Vec<rtdb_node::nearest::NearestPartition>, query: rtdb_node::nearest::NearestQuery,
        valid: Timestamp, staged: Option<rtdb_node::nearest::HeldVectorOverlay>,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)>>;
    fn nearest_at<'a>(
        &'a self, partitions: Vec<rtdb_node::nearest::NearestPartition>, query: rtdb_node::nearest::NearestQuery,
        valid: Timestamp, system: Timestamp, parent_only: bool,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)>>;
    fn nearest_parent_at<'a>(
        &'a self, partitions: Vec<rtdb_node::nearest::NearestPartition>, query: rtdb_node::nearest::NearestQuery,
        valid: Timestamp, system: Timestamp,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)>>;
    fn range<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedRangeRequest<'a>, valid: Timestamp,
    ) -> BoxFuture<'a, Result<(OwnedRangePage, OwnedWorldWitness)>>;
    fn count<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedCountRequest<'a>,
    ) -> BoxFuture<'a, Result<(u64, OwnedWorldWitness)>>;
    fn measure<'a>(
        &'a self, relation: OwnedRelation, path: OwnedRelation, request: OwnedCountRequest<'a>,
    ) -> BoxFuture<'a, Result<(OwnedRangeCount, OwnedWorldWitness)>>;
    fn shared_column_census<'a>(
        &'a self, scope: rtdb_types::PlacedAttemptScope, valid: Timestamp,
        root_prefix: rtdb_types::SharedColumnCarrierPrefix<'a>, world_prefix: &'a str,
    ) -> BoxFuture<'a, Result<(WorldIdentityPage, [OwnedWorldWitness; 2])>>;
    fn parent_identity_page_at<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: &'a rtdb_types::OwnedIdentityScan<'a>, limit: usize, system: Timestamp,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>>;
    fn parent_schedule_at<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Interval, system: Timestamp,
    ) -> BoxFuture<'a, Result<(Vec<(Interval, Timestamp)>, OwnedWorldWitness)>>;
    fn ranked_search_parent_at<'a>(
        &'a self, partitions: Vec<(OwnedRelation, OwnedRelation)>, query: String, offset: usize, limit: usize,
        valid: Timestamp, system: Timestamp,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedSearchBatch, Vec<OwnedWorldWitness>)>>;
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
    fn changes<'a>(&'a self, relations: &'a [OwnedRelation], after: OwnedChangeEdge, through: OwnedChangeEdge, limit: usize, descending: bool)
        -> BoxFuture<'a, Result<rtdb_node::OwnedMergedChanges>>;
    fn valid_during<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Interval, parent_only: bool,
    ) -> BoxFuture<'a, Result<(Vec<(Interval, Timestamp)>, Option<OwnedWorldWitness>)>>;
    fn valid_during_at<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Interval, system: Timestamp,
    ) -> BoxFuture<'a, Result<Vec<(Interval, Timestamp)>>>;
    fn identity_page<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: &'a rtdb_types::OwnedIdentityScan<'a>, limit: usize,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>>;
    fn identity_page_at<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: &'a rtdb_types::OwnedIdentityScan<'a>, limit: usize,
        system: Timestamp, parent_only: bool,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>>;
    fn overlay_identities<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: &'a rtdb_types::OwnedIdentityScan<'a>, limit: usize,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>>;
    fn overlay_identities_at<'a>(
        &'a self, relation: &'a OwnedRelation, valid: Timestamp, after: &'a rtdb_types::OwnedIdentityScan<'a>,
        limit: usize, system: Timestamp,
    ) -> BoxFuture<'a, Result<WorldIdentityPage>>;
    fn parent_touch_images_at<'a>(
        &'a self, relation: &'a OwnedRelation, path: &'a OwnedRelation, id: &'a EntityId,
        selected: Timestamp, before: Timestamp, group: Timestamp, valid: Timestamp, images: bool,
    ) -> BoxFuture<'a, Result<[WorldPointResult; 2]>>;
    fn parent_changes_at<'a>(
        &'a self, relation: &'a OwnedRelation, selected: Timestamp, after: OwnedChangeEdge,
        through: OwnedChangeEdge, limit: usize, reverse: bool,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedMergedChanges, OwnedWorldWitness)>>;
    fn ancestor_changes<'a>(
        &'a self, relation: &'a OwnedRelation, selected: Timestamp, after: OwnedChangeEdge,
        through: OwnedChangeEdge, limit: usize, reverse: bool,
    ) -> BoxFuture<'a, Result<(rtdb_node::OwnedMergedChanges, OwnedWorldWitness)>>;
    fn parent_versions_at<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, selected: Timestamp,
        request: rtdb_node::OwnedVersionRequest,
    ) -> BoxFuture<'a, Result<(WorldParentVersions, OwnedWorldWitness)>>;
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
    ) -> BoxFuture<'a, Result<Option<(TransactionKey, Timestamp)>>>;
    fn select_parent_as_of<'a>(
        &'a self, relations: &'a [OwnedRelation], requested: Timestamp,
    ) -> BoxFuture<'a, Result<Option<Timestamp>>>;
    fn parent_point_at<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Timestamp, system: Timestamp,
        original_scope: Option<rtdb_types::PlacedAttemptScope>,
    ) -> BoxFuture<'a, Result<WorldPointResult>>;
    fn preflight_accepted_scope<'a>(
        &'a self, relations: &'a [OwnedRelation], system: Timestamp,
    ) -> BoxFuture<'a, Result<(TransactionKey, Timestamp)>>;
    fn accepted_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Timestamp,
        system: Timestamp, original_scope: Option<rtdb_types::PlacedAttemptScope>,
    ) -> BoxFuture<'a, Result<WorldPointResult>>;
    fn read_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid_at: Timestamp, observed: bool,
    ) -> BoxFuture<'a, Result<WorldPointResult>>;
    fn read_ancestor_root_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid: Timestamp,
    ) -> BoxFuture<'a, Result<WorldPointResult>>;
    fn read_parent_point<'a>(
        &'a self, relation: &'a OwnedRelation, id: &'a EntityId, valid_at: Timestamp, observed: bool,
        original_scope: Option<rtdb_types::PlacedAttemptScope>,
    ) -> BoxFuture<'a, Result<WorldPointResult>>;
}
