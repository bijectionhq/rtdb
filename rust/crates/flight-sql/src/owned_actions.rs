mod hosted_control;
mod hosted_observation;
mod unique;

pub(crate) use hosted_control::READ_HELD_HOSTED_CONTROL_V2;
pub(crate) use hosted_observation::{CONFIRM_APPLIED_HOSTED_V5, OBSERVE_HELD_HOSTED_V1, PREPARE_APPLIED_READ_V3};
pub(crate) use unique::{READ_HELD_UNIQUE_V1, READ_HELD_UNIQUE_WINDOW_V1};

use std::{collections::{BTreeMap, BTreeSet}, sync::{Arc, Mutex}, time::Duration};

use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, Decimal256Array, Float64Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray, UInt64Array};
use arrow_flight::Result as ActionResult;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use prost::Message;
use rtdb_columnar::{ArrowCell, EntityIdRef, MemoryBudget, MemoryReservation, OwnedBatch, binary_cell, entity_id_array, entity_id_type, read_entity_id, selected_cell};
use rtdb_format::ipc::{OwnedStream, write_arrow_stream};
use rtdb_node::{HeldSpatialNearest, NativeSession, OwnedCountRequest, OwnedRangeRequest, OwnedRangeWitness, OwnedReadSession, OwnedValidHistory, OwnedVersionOrder, OwnedVersionRequest, OwnedWorldWitness, TransactionStatus};
use rtdb_query::QueryCursor;
use rtdb_tx::{ArrowDocuments, Documents, OwnedRelativePeriod, TxOp};
use rtdb_types::{EntityId, Interval, LogicalType, OwnedRelation, ScalarType, StructFields, TimeUnit, Timestamp, Value};
use sha2::{Digest, Sha256};
use tokio::sync::OwnedSemaphorePermit;
use tonic::Status;
use uuid::Uuid;

use crate::{
    error::{arrow, invalid, status, unsupported},
    lease::Leased,
    service::{Call, Service, found},
    state::ResourceOwner,
    upload, wire,
};

pub(crate) const DECIDE_HOSTED_V1: &str = "rtdb.owned.DecideHosted.v1";
pub(crate) const DECLARE_OWNED_FIXED_RANGE_V1: &str = "rtdb.owned.DeclareFixedRange.v1";
pub(crate) const DECLARE_OWNED_CAPACITY_V2: &str = "rtdb.owned.DeclareCapacity.v2";
pub(crate) const DECLARE_OWNED_UNIQUE_PATH_V1: &str = "rtdb.owned.DeclareUniquePath.v1";
pub(crate) const SPATIAL_HELD_OWNED_V1: &str = "rtdb.owned.SpatialHeld.v1";
pub(crate) const SPATIAL_HELD_OWNED_V2: &str = "rtdb.owned.SpatialHeld.v2";
pub(crate) const SPATIAL_OPEN_OWNED: &str = "rtdb.owned.SpatialOpen.v1";
pub(crate) const SPATIAL_NEXT_OWNED: &str = "rtdb.owned.SpatialNext.v1";
pub(crate) const SPATIAL_CLOSE_OWNED: &str = "rtdb.owned.SpatialClose.v1";
pub(crate) const SPATIAL_RANK_OPEN_OWNED: &str = "rtdb.owned.SpatialRankOpen.v1";
pub(crate) const SPATIAL_RANK_NEXT_OWNED: &str = "rtdb.owned.SpatialRankNext.v1";
const SPATIAL_FILTER_TOKEN: &str = "rtdb.owned.spatial_filter";
const MAX_SPATIAL_FILTERS: usize = 32;
pub(crate) const SPATIAL_HELD_OWNED_V3: &str = "rtdb.owned.SpatialHeld.v3";

#[derive(Clone, Copy)]
pub(crate) enum SpatialHeldWire { V1, V2, V3, Open, RankOpen }
pub(crate) const DROP_OWNED_SPATIAL_PATH_V1: &str = "rtdb.owned.DropSpatialPath.v1";
pub(crate) const DROP_OWNED_SPATIAL_PATH_V2: &str = "rtdb.owned.DropSpatialPath.v2";
pub(crate) const DECLARE_OWNED_SPATIAL_PATH_V1: &str = "rtdb.owned.DeclareSpatialPath.v1";
pub(crate) const DECLARE_OWNED_SPATIAL_PATH_V2: &str = "rtdb.owned.DeclareSpatialPath.v2";
pub(crate) const OWNED_SPATIAL_READINESS_V1: &str = "rtdb.owned.SpatialPathReadiness.v1";

#[derive(Clone, Copy)]
pub(crate) enum PathDeclarationWire {
    Ordered,
    TaggedSpatial,
    EncodedSpatial,
}

pub(crate) const DECLARE_OWNED_SEARCH_PATH_V1: &str = "rtdb.owned.DeclareSearchPath.v1";
pub(crate) const DECLARE_OWNED_RELATION_V1: &str = "rtdb.owned.DeclareRelation.v1";
pub(crate) const DECLARE_OWNED_PATH_V1: &str = "rtdb.owned.DeclarePath.v1";
pub(crate) const CREATE_OWNED_V1: &str = "rtdb.owned.Create.v1";
pub(crate) const OFFER_CLOCK_SLOT_V1: &str = "rtdb.owned.OfferClockSlot.v1";
pub(crate) const COMMIT_OWNED_V1: &str = "rtdb.owned.Commit.v1";
pub(crate) const COMMIT_OWNED_V2: &str = "rtdb.owned.Commit.v2";
pub(crate) const COMMIT_ORDERED_V1: &str = "rtdb.owned.CommitOrdered.v1";
pub(crate) const COMMIT_OWNED_V7: &str = "rtdb.owned.Commit.v7";
pub(crate) const COMMIT_OWNED_V4: &str = "rtdb.owned.Commit.v4";
pub(crate) const COMMIT_OWNED_V3: &str = "rtdb.owned.Commit.v3";
const MAX_RANGE_WITNESSES: usize = 32;
const OWNED_RANGE_WITNESS: &str = "rtdb.owned.range_witness";
const MAX_COMMIT_GROUPS: usize = 32;
const MAX_GROUPED_COMMIT_ROWS: usize = 4096;

#[derive(Clone, Copy)]
pub(crate) enum CommitFormat {
    Single,
    Grouped,
    Ordered,
}

#[derive(Clone, Copy)]
enum CommitEffects {
    Facts,
    HostedRoot,
    HostedCatalog,
    Replay,
}

impl CommitEffects {
    fn hosted(replay: bool, conditional: bool, controls: &[rtdb_tx::HostedControlWrite]) -> Self {
        if replay {
            Self::Replay
        } else if !conditional && !controls.is_empty() {
            Self::HostedRoot
        } else if controls.iter().any(rtdb_tx::HostedControlWrite::replaces_root_catalog) {
            Self::HostedCatalog
        } else {
            Self::Facts
        }
    }

    fn allows_spatial_requirement(self) -> bool {
        matches!(self, Self::HostedRoot | Self::HostedCatalog)
    }
}

struct CommitRequest<'a> {
    deadline: Timestamp,
    groups: Vec<(OwnedRelation, &'a [u8])>,
}

impl<'a> CommitRequest<'a> {
    fn parse(body: &'a [u8], format: CommitFormat) -> Result<Self, Status> {
        let (single, body) = match format {
            CommitFormat::Single => {
                let (relation, rest) = split_relation(body)?;
                (Some(relation), rest)
            },
            CommitFormat::Grouped | CommitFormat::Ordered => (None, body),
        };
        let (deadline, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Owned commit has no deadline"))?;
        let deadline = Timestamp(i64::from_be_bytes(*deadline));
        if let Some(relation) = single {
            return Ok(Self { deadline, groups: vec![(relation, body)] });
        }
        Ok(Self { deadline, groups: owned_groups(body, false, matches!(format, CommitFormat::Ordered))? })
    }
}

fn owned_count_condition<'a>(call: &Call, body: &'a [u8], world: Option<rtdb_types::Iid>, signed_root: bool) -> Result<(TxOp, &'a [u8], Arc<MemoryReservation>), Status> {
    let (relation, body) = split_relation(body)?;
    let ([scope], body) = body.split_first_chunk::<1>()
        .ok_or_else(|| invalid("Missing final count scope"))?;
    let relation = match *scope {
        0 => relation,
        1 => relation.in_world(world.ok_or_else(|| invalid("A Proposal count requires its held world"))?).map_err(status)?,
        _ => return Err(invalid("Invalid final count scope")),
    };
    let (column, body) = count_component(body, 96)?;
    let column = std::str::from_utf8(column).map_err(|_| invalid("Owned count column must be UTF-8"))?;
    let (prefix, body) = count_component(body, 4096)?;
    let (maximum, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Missing owned count maximum"))?;
    let at_most = i64::from_be_bytes(*maximum);
    if (at_most < 0 && !(signed_root && *scope == 0)) || column.is_empty() || column.starts_with('_') {
        return Err(invalid("Invalid owned count maximum or column"));
    }
    let reservation = call.reserve(prefix.len() + column.len() + 1024)?;
    let condition = TxOp::OwnedCheckCapacity { relation, column: column.into(), prefix: prefix.to_vec(), at_most };
    Ok((condition, body, reservation))
}

fn owned_count_conditions<'a>(call: &Call, body: &'a [u8], world: Option<rtdb_types::Iid>, signed_root: bool) -> Result<(Vec<TxOp>, &'a [u8], Vec<Arc<MemoryReservation>>), Status> {
    let (count, mut body) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Missing final count conditions"))?;
    let count = usize::from(u16::from_be_bytes(*count));
    if !(1..=MAX_GROUPED_COMMIT_ROWS).contains(&count) {
        return Err(invalid("Invalid final count condition count"));
    }
    let framing = call.reserve(count * (std::mem::size_of::<TxOp>() + 32))?;
    let mut conditions = Vec::with_capacity(count);
    let mut charges = Vec::with_capacity(count + 1);
    charges.push(framing);
    for _ in 0..count {
        let (condition, tail, charge) = owned_count_condition(call, body, world, signed_root)?;
        body = tail;
        conditions.push(condition);
        charges.push(charge);
    }
    Ok((conditions, body, charges))
}

fn ordered_group_count(body: &[u8]) -> Result<usize, Status> {
    let (count, _) = body.split_first_chunk::<2>()
        .ok_or_else(|| invalid("Ordered owned commit has no group count"))?;
    let count = usize::from(u16::from_be_bytes(*count));
    if count > MAX_GROUPED_COMMIT_ROWS {
        return Err(invalid("Ordered owned commit has too many groups"));
    }
    Ok(count)
}

fn owned_groups(body: &[u8], require_writes: bool, ordered: bool) -> Result<Vec<(OwnedRelation, &[u8])>, Status> {
    let (count, mut rest) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Owned commit has no group count"))?;
    let count = usize::from(u16::from_be_bytes(*count));
    let limit = if ordered { MAX_GROUPED_COMMIT_ROWS } else { MAX_COMMIT_GROUPS };
    if count > limit || (require_writes && count == 0) {
        return Err(invalid("Owned commit has an invalid relation group count"));
    }
    let mut relations = BTreeSet::new();
    let mut groups = Vec::with_capacity(count);
    for _ in 0..count {
        let (relation, framed) = split_relation(rest)?;
        if !relations.insert(relation.clone()) && !ordered {
            return Err(invalid("Owned commit has a duplicate relation"));
        }
        if relations.len() > MAX_COMMIT_GROUPS {
            return Err(invalid("Owned commit exceeds its distinct relation limit"));
        }
        let (length, framed) = framed.split_first_chunk::<4>().ok_or_else(|| invalid("Owned commit group has no Arrow length"))?;
        let length = u32::from_be_bytes(*length) as usize;
        let (arrow, tail) = framed.split_at_checked(length).ok_or_else(|| invalid("Owned commit group Arrow stream is truncated"))?;
        if arrow.is_empty() {
            return Err(invalid("Owned commit group Arrow stream is empty"));
        }
        groups.push((relation, arrow));
        rest = tail;
    }
    if !rest.is_empty() {
        return Err(invalid("Owned commit has trailing bytes"));
    }
    Ok(groups)
}

pub(crate) const READ_OWNED_V1: &str = "rtdb.owned.ReadPoint.v1";
pub(crate) const OPEN_OWNED_SCOPED_READ_V1: &str = "rtdb.owned.OpenOwnedScopedRead.v1";
pub(crate) const OPEN_OWNED_READ_AS_OF_V1: &str = "rtdb.owned.OpenReadAsOf.v1";
pub(crate) const OPEN_OWNED_READ_V2: &str = "rtdb.owned.OpenRead.v2";
pub(crate) const OPEN_APPLIED_OWNED_READ_V4: &str = "rtdb.owned.OpenAppliedRead.v4";
pub(crate) const HELD_OWNED_POINT_V1: &str = "rtdb.owned.ReadHeldPoint.v1";
pub(crate) const HELD_OWNED_POINT_V2: &str = "rtdb.owned.ReadHeldPoint.v2";
pub(crate) const HELD_OWNED_FIXED_POINT_V1: &str = "rtdb.owned.ReadHeldFixedPoint.v1";
pub(crate) const HELD_WORLD_VERSION_V2: &str = "rtdb.owned.ReadHeldWorldVersion.v2";
pub(crate) const HELD_WORLD_TOUCH_IMAGES_V1: &str = "rtdb.owned.ReadHeldWorldTouchImages.v1";
pub(crate) const HELD_WORLD_PATH_TOUCH_V1: &str = "rtdb.owned.ReadHeldWorldPathTouch.v1";
pub(crate) const HELD_OWNED_PATH_TOUCH_V1: &str = "rtdb.owned.ReadHeldPathTouch.v1";
pub(crate) const HELD_OWNED_HISTORICAL_POINT_V1: &str = "rtdb.owned.ReadHeldHistoricalPoint.v1";
pub(crate) const HELD_OWNED_POSITION_POINT_V1: &str = "rtdb.owned.ReadHeldPositionPoint.v1";
pub(crate) const HELD_OWNED_POSITION_VALID_DURING_V1: &str = "rtdb.owned.ReadHeldPositionValidDuring.v1";
pub(crate) const HELD_OWNED_VALID_DURING_V2: &str = "rtdb.owned.ReadHeldValidDuring.v2";
pub(crate) const HELD_OWNED_VALID_REGION_V1: &str = "rtdb.owned.ReadHeldValidRegion.v1";
pub(crate) const HELD_WORLD_VALID_AT_V1: &str = "rtdb.owned.ReadHeldWorldValidDuringAt.v1";
pub(crate) const HELD_WORLD_VALID_V2: &str = "rtdb.owned.ReadHeldWorldValidDuring.v2";
pub(crate) const HELD_WORLD_PARENT_IDENTITIES_AT_V1: &str = "rtdb.owned.ReadHeldWorldParentIdentityPageAt.v1";
pub(crate) const HELD_WORLD_PARENT_VALID_AT_V1: &str = "rtdb.owned.ReadHeldWorldParentValidDuringAt.v1";
pub(crate) const SEARCH_HELD_WORLD_PARENT_AT_V1: &str = "rtdb.owned.SearchHeldWorldParentAt.v1";

pub(crate) enum WorldHistoricalRead { Current, EarlierEffective, EarlierParent }

pub(crate) const HELD_WORLD_IDENTITIES_AT_V1: &str = "rtdb.owned.ReadHeldWorldIdentityPageAt.v1";
pub(crate) enum WorldIdentityRead { Effective, Overlay, OverlayAt, EffectiveAt, ParentAt }
pub(crate) const HELD_WORLD_IDENTITIES_V1: &str = "rtdb.owned.ReadHeldWorldIdentityPage.v1";
pub(crate) const HELD_WORLD_OVERLAY_IDENTITIES_AT_V1: &str = "rtdb.owned.ReadHeldWorldOverlayIdentitiesAt.v1";
pub(crate) const HELD_WORLD_OVERLAY_IDENTITIES_V1: &str = "rtdb.owned.ReadHeldWorldOverlayIdentityPage.v1";
pub(crate) const HELD_PROTECTED_COVERAGE_V1: &str = "rtdb.owned.ReadHeldProtectedCarrierCoverage.v1";
pub(crate) const HELD_ROW_CARRIER_EXISTS_V1: &str = "rtdb.owned.ReadHeldRowCarrierExists.v1";
pub(crate) const HELD_PROTECTED_CARRIERS_V1: &str = "rtdb.owned.ReadHeldProtectedCarrierPage.v1";
pub(crate) const HELD_OWNED_IDENTITIES_V1: &str = "rtdb.owned.ReadHeldIdentityPage.v1";
pub(crate) const HELD_OWNED_IDENTITIES_ALL_VALID_V1: &str = "rtdb.owned.ReadHeldIdentityPageAllValid.v1";
pub(crate) const HELD_WORLD_PARENT_TOUCH_KEYS_AT_V1: &str = "rtdb.owned.ReadHeldWorldParentTouchKeysAt.v1";
pub(crate) const HELD_WORLD_PARENT_TOUCH_IMAGES_AT_V1: &str = "rtdb.owned.ReadHeldWorldParentTouchImagesAt.v1";
pub(crate) const PREPARE_HELD_WORLD_OVERLAY_V3: &str = "rtdb.owned.PrepareHeldWorldOverlay.v3";
pub(crate) const HELD_WORLD_ANCESTOR_CHANGES_V1: &str = "rtdb.owned.ReadHeldWorldAncestorChanges.v1";
pub(crate) const HELD_WORLD_PARENT_CHANGES_AT_V1: &str = "rtdb.owned.ReadHeldWorldParentChangesAt.v1";
pub(crate) const HELD_WORLD_PARENT_VERSIONS_AT_V1: &str = "rtdb.owned.ReadHeldWorldParentVersionsAt.v1";
pub(crate) const HELD_OWNED_VERSIONS_V2: &str = "rtdb.owned.ReadHeldVersions.v2";
pub(crate) const CLOSE_OWNED_READ_V1: &str = "rtdb.owned.CloseRead.v1";
pub(crate) const PREPARE_REPLAY_PARENT_V1: &str = "rtdb.owned.PrepareReplayParent.v1";
pub(crate) const FINISH_REPLAY_PARENTS_V1: &str = "rtdb.owned.FinishReplayParents.v1";
pub(crate) const RETAIN_REPLAYED_WORLD_V3: &str = "rtdb.owned.RetainReplayedWorld.v3";
pub(crate) const OPEN_WORLD_READ_V4: &str = "rtdb.owned.OpenWorldRead.v4";
pub(crate) const READ_HELD_WORLD_HEAD_V1: &str = "rtdb.owned.ReadHeldWorldHead.v1";
pub(crate) const SELECT_HELD_WORLD_PARENT_AS_OF_V1: &str = "rtdb.owned.SelectHeldWorldParentAsOf.v1";
pub(crate) const HELD_WORLD_PARENT_POINT_AT_V1: &str = "rtdb.owned.ReadHeldWorldParentPointAt.v1";

pub(crate) enum WorldCutSelection { ExactLocal, AsOfLocal, AsOfParent }

pub(crate) const SELECT_HELD_WORLD_AS_OF_V1: &str = "rtdb.owned.SelectHeldWorldAsOf.v1";
pub(crate) const HELD_WORLD_AT_PREFLIGHT_V1: &str = "rtdb.owned.PreflightHeldWorldAt.v1";
pub(crate) const HELD_WORLD_POINT_AT_V1: &str = "rtdb.owned.ReadHeldWorldPointAt.v1";
pub(crate) const HELD_WORLD_POINT_V2: &str = "rtdb.owned.ReadHeldWorldPoint.v2";
pub(crate) const CLOSE_WORLD_READ_V1: &str = "rtdb.owned.CloseWorldRead.v1";
pub(crate) const PUBLISH_HELD_WORLD_RETENTION_V3: &str = "rtdb.owned.PublishHeldWorldRetention.v3";
pub(crate) const PUBLISH_WORLD_CUT_V1: &str = "rtdb.owned.PublishWorldCut.v1";
pub(crate) const DECIDE_WORLD_CUT_V1: &str = "rtdb.owned.DecideWorldCut.v1";
pub(crate) const DECIDE_HELD_WORLDS_V1: &str = "rtdb.owned.DecideHeldWorlds.v1";
const MAX_WORLD_WITNESSES: usize = 128;
const WORLD_POINT_WITNESS: &str = "rtdb.owned.world_point_witness";
const WORLD_RANGE_WITNESS: &str = "rtdb.owned.world_range_witness";
pub(crate) const HELD_WORLD_PARENT_RANGE_AT_V1: &str = "rtdb.owned.ReadHeldWorldParentRangeAt.v1";
pub(crate) const HELD_WORLD_PARENT_COUNT_AT_V1: &str = "rtdb.owned.CountHeldWorldParentRangeAt.v1";
pub(crate) const HELD_WORLD_RANGE_V1: &str = "rtdb.owned.ReadHeldWorldRange.v1";
pub(crate) const HELD_WORLD_COUNT_V1: &str = "rtdb.owned.CountHeldWorldRange.v1";
pub(crate) const HELD_WORLD_MEASURE_V1: &str = "rtdb.owned.MeasureHeldWorldRange.v1";
pub(crate) const HELD_WORLD_OVERLAY_RANGE_AT_V1: &str = "rtdb.owned.ReadHeldWorldOverlayRangeAt.v1";
pub(crate) const HELD_WORLD_OVERLAY_RANGE_V1: &str = "rtdb.owned.ReadHeldWorldOverlayRange.v1";
#[derive(Clone, Copy)]
pub(crate) enum WorldOrder { ParentRange, ParentCount, ParentMeasure, OverlayRange, OverlayRangeAt, ParentRangeAt, ParentCountAt }
pub(crate) const SEARCH_HELD_WORLD_AT_V1: &str = "rtdb.owned.SearchHeldWorldAt.v1";
pub(crate) const SEARCH_HELD_WORLD_V2: &str = "rtdb.owned.SearchHeldWorld.v2";
pub(crate) const SEARCH_HELD_OWNED_V5: &str = "rtdb.owned.SearchHeld.v5";
pub(crate) const HELD_OWNED_RANGE_V1: &str = "rtdb.owned.ReadHeldRange.v1";
#[derive(Clone, Copy)]
pub(crate) enum HeldRangeFormat { Leading, Full, FixedValid, Historical }

pub(crate) const HELD_OWNED_RANGE_V3: &str = "rtdb.owned.ReadHeldRange.v3";
pub(crate) const HELD_OWNED_RANGE_V4: &str = "rtdb.owned.ReadHeldRange.v4";
pub(crate) const HELD_OWNED_RANGE_V2: &str = "rtdb.owned.ReadHeldRange.v2";
pub(crate) const HELD_OWNED_COUNT_V1: &str = "rtdb.owned.CountHeldRange.v1";
pub(crate) const HELD_OWNED_COUNT_V2: &str = "rtdb.owned.CountHeldRange.v2";
pub(crate) const HELD_OWNED_MEASURE_V1: &str = "rtdb.owned.MeasureHeldRange.v1";
pub(crate) const HELD_OWNED_MEASURE_AT_V1: &str = "rtdb.owned.MeasureHeldRangeAt.v1";
pub(crate) const READ_HELD_WORLD_CHANGES_DESCENDING_V1: &str = "rtdb.owned.ReadHeldWorldChangesDescending.v1";
pub(crate) const READ_HELD_WORLD_CHANGES_V3: &str = "rtdb.owned.ReadHeldWorldChanges.v3";
pub(crate) const READ_HELD_WORLD_REGION_CHANGES_V1: &str = "rtdb.owned.ReadHeldWorldRegionChanges.v1";
pub(crate) const HELD_OWNED_STORAGE_USAGE_V1: &str = "rtdb.owned.ReadHeldStorageUsage.v1";
pub(crate) const HELD_OWNED_MERGED_CHANGES_DESCENDING_V1: &str = "rtdb.owned.ReadHeldMergedChangesDescending.v1";
pub(crate) const HELD_OWNED_MERGED_CHANGES_V3: &str = "rtdb.owned.ReadHeldMergedChanges.v3";
pub(crate) const READ_HELD_EXPIRY_CANDIDATES_V1: &str = "rtdb.owned.ReadHeldExpiryCandidates.v1";
pub(crate) const APPLY_HELD_EXPIRY_V1: &str = "rtdb.owned.ApplyHeldExpiry.v1";
pub(crate) const NOMINATE_HELD_EXPIRY_V1: &str = "rtdb.owned.NominateHeldExpiry.v1";
pub(crate) const HELD_OWNED_CHANGES_V3: &str = "rtdb.owned.ReadHeldChanges.v3";
const READ_IDLE: Duration = Duration::from_secs(60);
const OWNED_IDENTITY_MORE: &str = "rtdb.owned.identity_more";
const OWNED_PRESENT: &str = "rtdb.owned.present_micros";
const OWNED_VALID: &str = "rtdb.owned.valid_micros";
const OWNED_VALID_FROM: &str = "rtdb.owned.valid_from_micros";
const OWNED_VALID_TO: &str = "rtdb.owned.valid_to_micros";
const MAX_HISTORY_SLICES: usize = 4096;
const OWNED_VERSION_REVISION: &str = "rtdb.owned.version_revision";
const OWNED_SEARCH_TOTAL: &str = "rtdb.owned.search_total";
const OWNED_SEARCH_SCOPE: &str = "rtdb.owned.search_scope";
const OWNED_SEARCH_WITNESSES: &str = "rtdb.owned.search_witnesses";
const OWNED_VERSION_TIME: &str = "rtdb.owned.version_micros";
const OWNED_REVISION: &str = "rtdb.owned.revision_micros";
const OWNED_ASSERTION_ORDINAL: &str = "rtdb.owned.assertion_ordinal";
const OWNED_ASSERTION_LINE: &str = "rtdb.owned.assertion_line";
const OWNED_ASSERTION_HELD_READING: &str = "rtdb.owned.assertion_held_reading_micros";
const OWNED_ASSERTION_ORDINALS: &str = "rtdb.owned.assertion_ordinals";
const OWNED_CHANGE_COVERAGE: &str = "rtdb.owned.change_coverage_micros";

pub(crate) struct OwnedReadResource {
    snapshot: OwnedReadSession,
    owner: ResourceOwner,
    session: String,
    database: String,
    audience_work: Option<rtdb_storage::audience_work::TurnAllowance>,
    audience_projection_work: Option<rtdb_storage::audience_work::TurnAllowance>,
    witnesses: Mutex<BTreeMap<Uuid, RetainedRangeWitness>>,
    spatial_filters: Mutex<BTreeMap<Uuid, Arc<RetainedSpatialFilter>>>,
    _permit: Option<OwnedSemaphorePermit>,
    _reservation: Arc<MemoryReservation>,
}

struct RetainedSpatialFilter {
    relation: OwnedRelation,
    batch: OwnedBatch,
    query: RetainedSpatialQuery,
    valid_at: Timestamp,
    _reservation: Arc<MemoryReservation>,
}

enum RetainedSpatialQuery {
    Range { path: OwnedRelation, region: rtdb_spatial::PreparedRegion },
    Nearest(HeldSpatialNearest),
}

struct RetainedRangeWitness {
    relation: OwnedRelation,
    evidence: OwnedRangeWitness,
    _reservation: Arc<MemoryReservation>,
}

type Witnesses = Vec<(Arc<OwnedReadResource>, OwnedRangeWitness)>;

impl OwnedReadResource {
    fn remember(&self, call: &Call, relation: &OwnedRelation, evidence: OwnedRangeWitness) -> Result<Uuid, Status> {
        self.snapshot.check_witness_relation(relation, &evidence).map_err(status)?;
        let mut witnesses = self.witnesses.lock().map_err(|_| Status::internal("Owned range witness storage is unavailable"))?;
        if let Some((nonce, _)) = witnesses.iter().find(|(_, witness)| &witness.relation == relation && witness.evidence.same_range(&evidence)) {
            return Ok(*nonce);
        }
        if witnesses.len() >= MAX_RANGE_WITNESSES {
            return Err(Status::resource_exhausted("The held read has too many range witnesses"));
        }
        let nonce = Uuid::new_v4();
        if witnesses.contains_key(&nonce) {
            return Err(Status::unavailable("Could not issue an owned range witness"));
        }
        let reservation = call.reserve(wire::add(wire::add(512, relation.name().len() * 2)?, evidence.retained_bytes())?)?;
        witnesses.insert(nonce, RetainedRangeWitness {
            relation: relation.clone(), evidence, _reservation: reservation,
        });
        Ok(nonce)
    }
}

pub(crate) enum ValidHistoryMode {
    Timeline,
    Positioned,
    Region,
}

enum HistoryRead<'a> {
    Observed { read: OwnedValidHistory<'a>, held: &'a OwnedReadResource, relation: &'a OwnedRelation },
    Version(QueryCursor),
    Archived(std::vec::IntoIter<OwnedBatch>),
}

impl HistoryRead<'_> {
    async fn next_batch(&mut self) -> rtdb_types::Result<Option<OwnedBatch>> {
        match self {
            Self::Observed { read, .. } => read.next_batch().await,
            Self::Version(cursor) => cursor.next_batch().await,
            Self::Archived(batches) => Ok(batches.next()),
        }
    }

    fn runtime_schema(&self) -> Option<&rtdb_plan::Schema> {
        match self {
            Self::Observed { read, .. } => read.runtime_schema(),
            Self::Version(cursor) => Some(cursor.runtime_schema()),
            Self::Archived(_) => None,
        }
    }

    fn witness_nonce(&self, call: &Call) -> Result<Option<Uuid>, Status> {
        match self {
            Self::Observed { read, held, relation } => {
                read.witness().map_err(status)?.map(|evidence| held.remember(call, relation, evidence)).transpose()
            }
            Self::Version(_) | Self::Archived(_) => Ok(None),
        }
    }
}

fn witnessed_schema(
    call: &Call, held: &OwnedReadResource, relation: &OwnedRelation, schema: &Schema,
    evidence: Option<OwnedRangeWitness>,
) -> Result<Option<(Schema, Arc<MemoryReservation>)>, Status> {
    let Some(evidence) = evidence else { return Ok(None); };
    let charge = call.reserve(wire::add(wire::schema_bound(schema)?, 256)?)?;
    let nonce = held.remember(call, relation, evidence)?;
    let mut metadata = schema.metadata().clone();
    metadata.insert(OWNED_RANGE_WITNESS.into(), nonce.simple().to_string());
    Ok(Some((schema.clone().with_metadata(metadata), charge)))
}

fn count_batch(count: u64) -> Result<RecordBatch, Status> {
    let schema = Arc::new(Schema::new(vec![Field::new("count", DataType::UInt64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![count]))]).map_err(arrow)
}

fn measure_batch(measured: &rtdb_node::OwnedRangeCount) -> Result<RecordBatch, Status> {
    let (name, sum): (&str, ArrayRef) = match (measured.sum, &measured.decimal_sum) {
        (Some(sum), None) => {
            let bytes = sum.to_be_bytes();
            ("sum_i128", Arc::new(BinaryArray::from_iter_values([bytes.as_slice()])))
        }
        (None, Some(sum)) => {
            let coefficient = arrow_buffer::i256::from_be_bytes(sum.coefficient);
            let values = Decimal256Array::from(vec![coefficient])
                .with_precision_and_scale(76, sum.scale).map_err(arrow)?;
            ("sum_decimal", Arc::new(values))
        }
        _ => return Err(invalid("Owned measure has no covered sum")),
    };
    RecordBatch::try_from_iter([
        ("count", Arc::new(UInt64Array::from(vec![measured.count])) as ArrayRef),
        (name, sum),
    ]).map_err(arrow)
}

impl Leased for OwnedReadResource {
    fn close(&self) {
        self.snapshot.close();
        if let Ok(mut filters) = self.spatial_filters.lock() { filters.clear(); }
    }
}

pub(crate) struct OwnedWorldReadResource {
    snapshot: Box<dyn crate::OwnedWorldRead>,
    world: Uuid,
    conditions: Mutex<BTreeMap<Uuid, RetainedWorldWitness>>,
    owner: ResourceOwner,
    session: String,
    database: String,
    _permit: OwnedSemaphorePermit,
    _reservation: Arc<MemoryReservation>,
}

impl Leased for OwnedWorldReadResource {
    fn close(&self) { self.snapshot.close(); }
}

struct RetainedWorldWitness {
    evidence: OwnedWorldWitness,
    absent_in_parent_and_overlay: bool,
    _reservation: Arc<MemoryReservation>,
}

impl OwnedWorldReadResource {
    fn remember(&self, call: &Call, evidence: OwnedWorldWitness, absent_in_parent_and_overlay: bool) -> Result<Uuid, Status> {
        self.snapshot.check().map_err(status)?;
        let mut conditions = self.conditions.lock().map_err(|_| Status::internal("Owned world witness storage is unavailable"))?;
        if conditions.len() >= MAX_WORLD_WITNESSES {
            return Err(Status::resource_exhausted("The held world read has too many point witnesses"));
        }
        let nonce = Uuid::new_v4();
        if conditions.contains_key(&nonce) {
            return Err(Status::unavailable("Could not issue an owned world point witness"));
        }
        let charge = call.reserve(evidence.allocation_bound())?;
        conditions.insert(nonce, RetainedWorldWitness { evidence, absent_in_parent_and_overlay, _reservation: charge });
        Ok(nonce)
    }

    fn conditions(&self, call: &Call, world: Uuid, nonces: &[u8]) -> Result<(Vec<OwnedWorldWitness>, Arc<MemoryReservation>), Status> {
        self.snapshot.check().map_err(status)?;
        if world != self.world { return Err(Status::not_found("unknown owned world point witness")); }
        let stored = self.conditions.lock().map_err(|_| Status::internal("Owned world witness storage is unavailable"))?;
        let mut seen = BTreeSet::new();
        let mut charge = call.reserve(4096)?;
        let conditions = nonces.chunks_exact(16).map(|bytes| {
            let nonce = Uuid::from_slice(bytes).map_err(arrow)?;
            if !seen.insert(nonce) { return Err(invalid("Owned world commit has a duplicate point witness")); }
            let condition = stored.get(&nonce).ok_or_else(|| Status::not_found("unknown owned world point witness"))?;
            let reservation = Arc::get_mut(&mut charge).expect("world commit owns its reservation");
            reservation.grow_to(wire::add(reservation.bytes(), condition.evidence.allocation_bound())?).map_err(status)?;
            Ok(condition.evidence.clone())
        }).collect::<Result<_, _>>()?;
        Ok((conditions, charge))
    }

    fn require_create_absence(
        &self, nonces: &[u8], write: &rtdb_node::OwnedWorldWrite, at: Option<Timestamp>,
    ) -> Result<(), Status> {
        let stored = self.conditions.lock().map_err(|_| Status::internal("Owned world witness storage is unavailable"))?;
        if nonces.chunks_exact(16).filter_map(|bytes| Uuid::from_slice(bytes).ok()).any(|nonce| {
            stored.get(&nonce).is_some_and(|condition| condition.absent_in_parent_and_overlay
                && condition.evidence.is_absent_point(&write.relation, &write.id, at))
        }) {
            return Ok(());
        }
        Err(Status::failed_precondition("A world create requires original absence in the parent and overlay"))
    }
}

struct HostedWorldRead {
    held: Arc<OwnedWorldReadResource>,
    witnesses: Vec<OwnedWorldWitness>,
    clock: Option<rtdb_types::WorldClockLimit>,
    _reservation: Arc<MemoryReservation>,
}

fn take_held_world_observations(worlds: &mut [HostedWorldRead]) -> Result<rtdb_node::OwnedWorldObservations, Status> {
    let mut observed = rtdb_node::OwnedWorldObservations::default();
    for read in worlds {
        read.held.snapshot.check().map_err(status)?;
        observed.checks.append(&mut read.witnesses);
        observed.clocks.extend(read.clock.map(|clock| (clock, read.held.snapshot.evidence_until())));
    }
    Ok(observed)
}

struct OwnedRankRequest<'a> {
    partitions: Vec<(OwnedRelation, OwnedRelation)>,
    frame_len: usize,
    offset: usize,
    limit: usize,
    observe: bool,
    query: &'a str,
    _reservation: Arc<MemoryReservation>,
}

fn owned_rank_request<'a>(call: &Call, request: &'a [u8]) -> Result<OwnedRankRequest<'a>, Status> {
    let (count, mut rest) = request.split_first_chunk::<2>().ok_or_else(|| invalid("Owned held search has no partition count"))?;
    let count = usize::from(u16::from_be_bytes(*count));
    if !(1..=32).contains(&count) { return Err(invalid("Owned held search partition count is out of range")); }
    let reservation = call.reserve(count * 512)?;
    let mut partitions = Vec::with_capacity(count);
    for _ in 0..count {
        let (relation, tail) = split_relation(rest)?;
        let (path, tail) = split_relation(tail)?;
        partitions.push((relation, path));
        rest = tail;
    }
    let frame_len = request.len() - rest.len();
    let (settings, query) = rest.split_first_chunk::<7>().ok_or_else(|| invalid("Owned held search requires an offset, limit and evidence flag"))?;
    let offset = u32::from_be_bytes([settings[0], settings[1], settings[2], settings[3]]) as usize;
    let limit = usize::from(u16::from_be_bytes([settings[4], settings[5]]));
    let observe = match settings[6] {
        0 => false,
        1 => true,
        _ => return Err(invalid("Invalid owned held search evidence flag")),
    };
    if query.len() > 4096 { return Err(invalid("Owned held search query exceeds its size bound")); }
    let query = std::str::from_utf8(query).map_err(arrow)?;
    Ok(OwnedRankRequest { partitions, frame_len, offset, limit, observe, query, _reservation: reservation })
}

fn owned_rank_scope(domain: &[u8], frame: &[u8], query: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update(frame);
    digest.update(query.as_bytes());
    let digest: [u8; 32] = digest.finalize().into();
    let mut scope = String::with_capacity(64);
    for bytes in digest.chunks_exact(16) {
        let chunk = Uuid::from_bytes(bytes.try_into().expect("16-byte SHA-256 chunk"));
        scope.push_str(&chunk.simple().to_string());
    }
    scope
}

fn count_component(body: &[u8], maximum: usize) -> Result<(&[u8], &[u8]), Status> {
    let (length, body) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Missing owned count component length"))?;
    let length = usize::from(u16::from_be_bytes(*length));
    if length > maximum {
        return Err(invalid("Owned count component exceeds its bound"));
    }
    body.split_at_checked(length).ok_or_else(|| invalid("Incomplete owned count component"))
}

fn split_relation(body: &[u8]) -> Result<(OwnedRelation, &[u8]), Status> {
    let (length, rest) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Owned action has no relation length"))?;
    let length = usize::from(u16::from_be_bytes(*length));
    let (name, arrow_bytes) = rest.split_at_checked(length).ok_or_else(|| invalid("Owned action relation is truncated"))?;
    if length > 96 { return Err(invalid("Owned action relation exceeds its length limit")); }
    let name = std::str::from_utf8(name).map_err(arrow)?;
    Ok((OwnedRelation::new(name).map_err(status)?, arrow_bytes))
}

fn require_audience_point(
    session: &crate::state::Session, relation: &OwnedRelation, id: &EntityId, recovering: bool,
) -> Result<(), Status> {
    let projection = session.audience_projection()?;
    if audience_point_admitted(&projection.relation, relation, id, recovering) {
        Ok(())
    } else {
        Err(Status::permission_denied("The point is outside the prepared audience projection"))
    }
}

fn audience_point_admitted(
    projection: &OwnedRelation, relation: &OwnedRelation, id: &EntityId, recovering: bool,
) -> bool {
    relation == projection
        || (recovering && relation.name() == "fact_layout_readiness")
        || (relation.name() == rtdb_types::OWNED_ROOT_PUBLICATION_RELATION
            && matches!(id, EntityId::String(id) if id == "head" || id == "sync_receipt_head"
                || id.strip_prefix("fact_partition_head:") == Some(projection.name())))
}

fn split_position_mask(body: &[u8]) -> Result<((Timestamp, u32), &[u8]), Status> {
    let (commit, rest) = body.split_first_chunk::<8>()
        .ok_or_else(|| invalid("Owned position read has no masked commit"))?;
    let (from, rest) = rest.split_first_chunk::<4>()
        .ok_or_else(|| invalid("Owned position read has no mask position"))?;
    Ok(((Timestamp(i64::from_be_bytes(*commit)), u32::from_be_bytes(*from)), rest))
}

fn decode_batch(call: &Call, arrow: &[u8]) -> Result<OwnedBatch, Status> {
    charge_audience_arrow(arrow)?;
    let stream = OwnedStream::read(arrow, call.memory()).map_err(status)?;
    single_owned_batch(stream)
}

fn decode_retained_batch(call: &Call, arrow: Bytes) -> Result<OwnedBatch, Status> {
    charge_audience_arrow(&arrow)?;
    let stream = OwnedStream::read_buffer(arrow_buffer::Buffer::from(arrow), call.memory()).map_err(status)?;
    single_owned_batch(stream)
}

fn charge_audience_arrow(arrow: &[u8]) -> Result<(), Status> {
    use rtdb_storage::audience_work;
    if audience_work::is_bounded() {
        let bound = rtdb_format::ipc::stream_allocation_bound(arrow).map_err(status)?;
        audience_work::decode_bytes(bound).map_err(|error| Status::resource_exhausted(error.to_string()))?;
    }
    Ok(())
}

fn single_owned_batch(stream: OwnedStream) -> Result<OwnedBatch, Status> {
    let mut stream = stream.into_pages();
    let [batch] = stream.as_slice() else {
        return Err(invalid("Owned action requires exactly one Arrow record batch"));
    };
    if batch.batch().num_rows() == 0 { return Err(invalid("Owned action requires a nonempty Arrow record batch")); }
    let schema = batch.batch().schema();
    let names = schema.fields().iter().map(|field| field.name().as_str()).collect::<BTreeSet<_>>();
    if names.len() != schema.fields().len() {
        return Err(invalid("Owned action requires unique column names"));
    }
    Ok(stream.pop().expect("validated one batch"))
}

fn spatial_filters_unavailable() -> Status { Status::internal("Owned spatial filter storage is unavailable") }

fn spatial_document_columns(batch: &RecordBatch) -> Result<Vec<&str>, Status> {
    let layout = batch.schema_ref().field_with_name("document_columns").map(Field::data_type);
    if !matches!(layout, Ok(DataType::List(item)) if item.data_type() == &DataType::Utf8
        && !item.metadata().contains_key(rtdb_columnar::EXTENSION_NAME_KEY)) {
        return Err(invalid("Spatial document_columns must be a List<Utf8>"));
    }
    let columns = ArrowCell::named(batch, "document_columns", 0).and_then(ArrowCell::list)
        .ok_or_else(|| invalid("Spatial document_columns must be a List<Utf8>"))?;
    if columns.len() > 128 {
        return Err(invalid("Spatial document_columns exceeds its column limit"));
    }
    let mut names = BTreeSet::new();
    for column in columns {
        let name = column.utf8().ok_or_else(|| invalid("Spatial document columns must be Utf8 values"))?;
        if name.is_empty() || name.len() > 128 || name.starts_with('_')
            || name.chars().any(char::is_control) || !names.insert(name) {
            return Err(invalid("Spatial document columns must be distinct owned field names"));
        }
    }
    Ok(names.into_iter().collect())
}

fn spatial_partition(batch: &RecordBatch) -> Result<Vec<std::borrow::Cow<'_, [u8]>>, Status> {
    let (parts, parts_row) = owned_scalar::<ListArray>(batch, "partition")?
        .ok_or_else(|| invalid("Owned held spatial request has no partition"))?;
    let offsets = parts.value_offsets();
        let indices = offsets[parts_row] as usize..offsets[parts_row + 1] as usize;
        if indices.len() > 15 {
            return Err(invalid("Owned held spatial request exceeds its byte or partition limit"));
        }
        let DataType::List(element) = parts.data_type() else { unreachable!() };
        indices.map(|row| {
            binary_cell(element, parts.values().as_ref(), row).map_err(status)?
                .filter(|value| value.len() <= 4096).map(std::borrow::Cow::Borrowed)
                .ok_or_else(|| invalid("Spatial partition values must be bounded Binary values"))
        }).collect()
}

fn owned_scalar<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<Option<(&'a T, usize)>, Status> {
    let Some((index, field)) = batch.schema_ref().column_with_name(name) else { return Ok(None); };
    let Some((field, array, row)) = selected_cell(field, batch.column(index).as_ref(), 0).map_err(status)? else {
        return Ok(None);
    };
    if field.metadata().contains_key(rtdb_columnar::EXTENSION_NAME_KEY) {
        return Err(invalid(format!("Owned field {name} has an unsupported Arrow extension")));
    }
    let array = array.as_any().downcast_ref::<T>()
        .ok_or_else(|| invalid(format!("Owned field {name} has an invalid Arrow type")))?;
    Ok(Some((array, row)))
}

fn request_batch(call: &Call, arrow: &[u8]) -> Result<OwnedBatch, Status> {
    require_identity(decode_batch(call, arrow)?)
}

fn require_identity(batch: OwnedBatch) -> Result<OwnedBatch, Status> {
    if batch.batch().schema_ref().index_of("_id").is_err() {
        return Err(invalid("Owned action requires an _id column"));
    }
    Ok(batch)
}

fn compound_bound(value: Option<&Value>) -> Result<Option<&StructFields<Value>>, Status> {
    match value {
        Some(Value::Null) => Ok(None),
        Some(Value::Struct(fields)) => Ok(Some(fields)),
        _ => Err(invalid("Owned full-key range endpoints must be structs or null")),
    }
}

fn require_idle(native: &NativeSession) -> Result<(), Status> {
    if native.state().auto_commit() && native.state().status() == TransactionStatus::Idle { return Ok(()); }
    Err(Status::failed_precondition("Owned actions require an idle autocommit session"))
}

impl Service {
    pub(crate) async fn declare_owned_relation_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, json) = split_relation(body)?;
        if json.len() > rtdb_types::OwnedSchema::MAX_JSON_BYTES {
            return Err(invalid("Owned declaration exceeds its byte limit"));
        }
        let _decoded = call.reserve(wire::add(wire::mul(json.len(), 8)?, 4096)?)?;
        let schema = rtdb_types::OwnedSchema::from_json(json).map_err(status)?;
        self.owned_write_reply(call, vec![TxOp::OwnedDeclareRelation { relation, schema }]).await
    }

    pub(crate) async fn drop_owned_spatial_path_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, body) = split_relation(body)?;
        let (name, tail) = split_relation(body)?;
        if !tail.is_empty() { return Err(invalid("Owned spatial path removal has trailing bytes")); }
        self.owned_write_reply(call, vec![TxOp::OwnedDropSpatialPath { relation, name }]).await
    }

    pub(crate) async fn drop_owned_spatial_path_batch_action(
        &self, call: &Call, body: Bytes,
    ) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let batch = decode_retained_batch(call, body)?;
        let batch = batch.batch();
        if batch.num_rows() != 1 || batch.num_columns() != 2 {
            return Err(invalid("Owned spatial path removal requires one row with relation and name"));
        }
        let text = |field| ArrowCell::named(batch, field, 0).and_then(ArrowCell::utf8)
            .ok_or_else(|| invalid("Owned spatial path removal requires a text relation and name"));
        let relation = OwnedRelation::new(text("relation")?).map_err(status)?;
        let name = OwnedRelation::new(text("name")?).map_err(status)?;
        self.owned_write_reply(call, vec![TxOp::OwnedDropSpatialPath { relation, name }]).await
    }

    pub(crate) async fn declare_owned_path_action(
        &self, call: &Call, body: &Bytes, wire: PathDeclarationWire,
    ) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, arrow) = split_relation(body)?;
        let batch = decode_retained_batch(call, body.slice_ref(arrow))?;
        let batch = batch.batch();
        let encoded = matches!(wire, PathDeclarationWire::EncodedSpatial);
        if batch.num_rows() != 1 || batch.num_columns() != 2 + usize::from(encoded) {
            return Err(invalid("Owned path declaration has an invalid Arrow layout"));
        }
        let name = ArrowCell::named(batch, "_id", 0).and_then(ArrowCell::utf8)
            .ok_or_else(|| invalid("Owned path declaration requires a text name"))?;
        let name = OwnedRelation::new(name).map_err(status)?;
        let columns = ArrowCell::named(batch, "columns", 0).and_then(ArrowCell::list)
            .ok_or_else(|| invalid("Owned path declaration requires a text column list"))?;
        if columns.len() > 16 {
            return Err(invalid("Owned path declaration exceeds its column limit"));
        }
        let names = columns.map(|cell| cell.utf8()
            .ok_or_else(|| invalid("Owned path key column is not text")))
            .collect::<Result<Vec<_>, _>>()?;
        let bytes = names.iter().try_fold(1024, |bytes, name| wire::add(bytes, wire::add(name.len(), 128)?))?;
        let _retained = call.reserve(bytes)?;
        let columns = names.into_iter().map(str::to_owned).collect();
        let declaration = match wire {
            PathDeclarationWire::Ordered => TxOp::OwnedDeclarePath { relation, name, columns },
            PathDeclarationWire::TaggedSpatial => TxOp::OwnedDeclareSpatialPath { relation, name, columns },
            PathDeclarationWire::EncodedSpatial => {
                let encoding = ArrowCell::named(batch, "geometry_encoding", 0).and_then(ArrowCell::utf8)
                    .ok_or_else(|| invalid("Owned spatial path requires a text geometry encoding"))?;
                TxOp::OwnedDeclareEncodedSpatialPath {
                    relation, name, columns,
                    encoding: rtdb_types::SpatialGeometryEncoding::parse(encoding).map_err(status)?,
                }
            }
        };
        self.owned_write_reply(call, vec![declaration]).await
    }

    pub(crate) async fn declare_owned_fixed_range_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, arrow) = split_relation(body)?;
        let batch = request_batch(call, arrow)?;
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != 3 {
            return Err(invalid("Fixed range declaration requires one row with path, column and valid_micros"));
        }
        let (rows, _decoded) = upload::domain_documents(batch.batch(), self)?;
        let row = &rows[0];
        let (Some(Value::Utf8(path)), Some(Value::Utf8(column)), Some(Value::I64(valid_micros))) =
            (row.get("path"), row.get("column"), row.get("valid_micros"))
        else { return Err(invalid("Fixed range declaration has invalid field types")); };
        let rule = rtdb_types::FixedRangeRule { path: path.clone(), column: column.clone(), valid_at: Timestamp(*valid_micros) };
        self.owned_write_reply(call, vec![TxOp::OwnedDeclareFixedRange { relation, rule }]).await
    }

    pub(crate) async fn declare_owned_capacity_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, arrow) = split_relation(body)?;
        let batch = request_batch(call, arrow)?;
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != 4 {
            return Err(invalid("Owned capacity declaration requires one row with column, prefix_bytes, cap and when"));
        }
        let (rows, _decoded) = upload::domain_documents(batch.batch(), self)?;
        let operation = owned_capacity_declaration(relation, &rows[0])?;
        self.owned_write_reply(call, vec![operation]).await
    }

    pub(crate) async fn declare_owned_single_column_path_action(&self, call: &Call, body: &[u8], unique: bool) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, arrow) = split_relation(body)?;
        let batch = request_batch(call, arrow)?;
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != 2 {
            return Err(invalid("Owned path declaration requires one row with _id and column"));
        }
        let (rows, _decoded) = upload::documents(batch.batch(), self)?;
        let (Some(Value::Utf8(name)), Some(Value::Utf8(column))) = (rows[0].get("_id"), rows[0].get("column")) else {
            return Err(invalid("Owned path declaration requires a text name and column"));
        };
        let name = OwnedRelation::new(name.as_str()).map_err(status)?;
        let column = column.clone();
        let operation = if unique {
            TxOp::OwnedDeclareUniquePath { relation, name, column }
        } else {
            TxOp::OwnedDeclareSearchPath { relation, name, column }
        };
        self.owned_write_reply(call, vec![operation]).await
    }

    pub(crate) async fn create_owned_action(&self, call: &Call, body: &Bytes) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, arrow) = split_relation(body)?;
        let batch = require_identity(decode_retained_batch(call, body.slice_ref(arrow))?)?;
        if relation.is_root_data() {
            if batch.batch().column_by_name("_absent").is_none() {
                if let Some(documents) = ArrowDocuments::from_owned_payload(&batch, self.memory()).map_err(status)? {
                    return self.owned_write_reply(call, vec![TxOp::OwnedArrowDocs {
                        relation, documents, create_only: true, valid_from: None, valid_to: None,
                    }]).await;
                }
            } else {
                let rows = batch.batch().num_rows();
                let _operations = call.reserve(wire::mul(rows, std::mem::size_of::<TxOp>() + 512)?)?;
                let mut operations = Vec::with_capacity(rows);
                for row in 0..rows {
                    let positions = owned_payload_columns(batch.batch(), row, &[])?;
                    operations.push(match self.owned_payload(&batch, row, &positions)? {
                        Some(documents) => TxOp::OwnedArrowDocs {
                            relation: relation.clone(), documents, create_only: true, valid_from: None, valid_to: None,
                        },
                        None => {
                            let (mut rows, _decoded) = upload::owned_documents(&batch.batch().slice(row, 1), call.memory())?;
                            strip_owned_absence(&mut rows[0])?;
                            TxOp::OwnedInsertDocs {
                                relation: relation.clone(), documents: Documents::new(rows, self.memory()).map_err(status)?,
                                valid_from: None, valid_to: None,
                            }
                        }
                    });
                }
                return self.owned_write_reply(call, operations).await;
            }
        }
        let (mut rows, _decoded) = upload::owned_documents(batch.batch(), call.memory())?;
        for row in &mut rows { strip_owned_absence(row)?; }
        let operation = TxOp::OwnedInsertDocs {
            relation, documents: Documents::new(rows, self.memory()).map_err(status)?,
            valid_from: None, valid_to: None,
        };
        self.owned_write_reply(call, vec![operation]).await
    }

    pub(crate) async fn commit_owned_action(&self, call: &Call, body: &Bytes, format: CommitFormat) -> Result<ActionResult, Status> {
        self.commit_owned(call, body, body, format, Vec::new(), false, Vec::new()).await
    }

    pub(crate) async fn commit_owned_counted_action(&self, call: &Call, envelope: &Bytes) -> Result<ActionResult, Status> {
        let body = envelope.as_ref();
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (conditions, body, _counts) = owned_count_conditions(call, body, None, false)?;
        let (witnesses, body, _framing) = self.owned_witness_prefix(call, body, true)?;
        self.commit_owned(call, envelope, body, CommitFormat::Ordered, witnesses, false, conditions).await
    }

    pub(crate) async fn commit_owned_witnessed_action(&self, call: &Call, envelope: &Bytes, format: CommitFormat, erase_allowed: bool) -> Result<ActionResult, Status> {
        let body = envelope.as_ref();
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let allow_empty = erase_allowed || matches!(format, CommitFormat::Ordered);
        let (witnesses, body, _framing) = self.owned_witness_prefix(call, body, allow_empty)?;
        self.commit_owned(call, envelope, body, format, witnesses, erase_allowed, Vec::new()).await
    }

    fn owned_witness_prefix<'a>(
        &self, call: &Call, body: &'a [u8], allow_empty: bool,
    ) -> Result<(Witnesses, &'a [u8], Arc<MemoryReservation>), Status> {
        let (length, mut body) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Owned commit has no witness count"))?;
        let count = usize::from(u16::from_be_bytes(*length));
        if count > MAX_RANGE_WITNESSES || (count == 0 && !allow_empty) {
            return Err(invalid("Invalid owned commit range witness count"));
        }
        let framing = call.reserve(count * 512)?;
        let mut witnesses = Vec::with_capacity(count);
        for _ in 0..count {
            let (handle, tail) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned range witness has no held handle"))?;
            let (nonce, tail) = tail.split_first_chunk::<16>().ok_or_else(|| invalid("Owned range witness has no nonce"))?;
            body = tail;
            let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
            held.snapshot.check().map_err(status)?;
            let nonce = Uuid::from_bytes(*nonce);
            let witness = held.witnesses.lock().map_err(|_| Status::internal("Owned range witness storage is unavailable"))?
                .get(&nonce)
                .map(|witness| {
                    held.snapshot.check_witness_relation(&witness.relation, &witness.evidence).map_err(status)?;
                    Ok::<_, Status>(witness.evidence.clone())
                }).transpose()?.ok_or_else(|| Status::not_found("Unknown owned range witness"))?;
            witnesses.push((held, witness));
        }
        Ok((witnesses, body, framing))
    }

    fn hosted_world_read_prefix<'a>(
        &self, call: &Call, body: &'a [u8],
    ) -> Result<(Vec<HostedWorldRead>, &'a [u8]), Status> {
        if self.0.config.owned_world_host.is_none() {
            return Err(unsupported());
        }
        let (count, mut body) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Missing held world count"))?;
        let count = usize::from(u16::from_be_bytes(*count));
        if !(1..=8).contains(&count) {
            return Err(invalid("A hosted commit requires one to eight held worlds"));
        }
        let mut handles = BTreeSet::new();
        let mut worlds = Vec::with_capacity(count);
        for _ in 0..count {
            let (clock, tail) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Missing held world clock bound"))?;
            let clock = Timestamp(i64::from_be_bytes(*clock));
            let (handle, tail) = tail.split_first_chunk::<16>().ok_or_else(|| invalid("Missing held world handle"))?;
            let (count, tail) = tail.split_first_chunk::<2>().ok_or_else(|| invalid("Missing held world witness count"))?;
            let count = usize::from(u16::from_be_bytes(*count));
            if !handles.insert(*handle) || count > MAX_WORLD_WITNESSES || (count == 0 && clock == Timestamp::MAX) {
                return Err(invalid("Invalid or repeated hosted world read"));
            }
            let (nonces, tail) = tail.split_at_checked(count * 16).ok_or_else(|| invalid("Truncated held world witnesses"))?;
            let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
            let (witnesses, reservation) = held.conditions(call, held.world, nonces)?;
            let clock = (clock != Timestamp::MAX).then(|| rtdb_types::WorldClockLimit {
                world: rtdb_types::Iid(*held.world.as_bytes()), pin: held.snapshot.parent_pin(), before: clock,
            });
            worlds.push(HostedWorldRead { held, witnesses, clock, _reservation: reservation });
            body = tail;
        }
        Ok((worlds, body))
    }

    fn clock_slot_missed() -> Status {
        status(rtdb_types::Anomaly::incorrect("rtdb.owned/clock-slot-missed", "The offered transaction clock is unavailable"))
    }

    pub(crate) async fn offer_clock_slot_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        let (tick, trailing) = self.owned_clock_slot(call, body).await?;
        if !trailing.is_empty() {
            return Err(invalid("A clock slot takes one original hold handle"));
        }
        let reading = rtdb_tx::clock_slot_reading(tick.system_time).ok_or_else(Self::clock_slot_missed)?;
        Ok(ActionResult { body: wire::retain(reading.0.to_be_bytes().to_vec(), call.reserve(8)?) })
    }

    async fn wait_for_clock_slot(&self, call: &Call, tick: rtdb_types::TransactionKey) -> Result<(), Status> {
        let reading = rtdb_tx::clock_slot_reading(tick.system_time).ok_or_else(Self::clock_slot_missed)?;
        let delay = reading.0.saturating_sub(self.0.node.now().0);
        if delay > 1_000_000 {
            return Err(Self::clock_slot_missed());
        }
        if delay > 0 {
            let wait = tokio::time::sleep(std::time::Duration::from_micros(delay as u64));
            call.cancellation.run_until_cancelled(wait).await
                .ok_or_else(|| Status::cancelled("The ordered clock slot was cancelled"))?;
        }
        Ok(())
    }

    async fn owned_clock_slot<'a>(
        &self, call: &Call, body: &'a [u8],
    ) -> Result<(rtdb_types::TransactionKey, &'a [u8]), Status> {
        self.hosted_control_policy(call)?;
        let scope = self.hosted_control_scope(call).await?;
        let (handle, body) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Missing clock slot hold"))?;
        let original = self.owned_read(call, &Bytes::copy_from_slice(handle)).map_err(|error| {
            if error.code() == tonic::Code::NotFound { Self::clock_slot_missed() } else { error }
        })?;
        original.snapshot.require_unscoped_root().map_err(status)?;
        let cut = original.snapshot.fork_cut(scope).await.map_err(|error| {
            if error.code == "rtdb.world/unavailable" { Self::clock_slot_missed() } else { status(error) }
        })?;
        if cut.live().is_none() {
            return Err(Status::failed_precondition("An ordered clock slot requires a live placed Root hold"));
        }
        Ok((original.snapshot.present(), body))
    }

    pub(crate) async fn decide_hosted_action(
        &self, call: &Call, envelope: &Bytes,
    ) -> Result<ActionResult, Status> {
        self.hosted_control_policy(call)?;
        let ([flags], body) = envelope.split_first_chunk::<1>()
            .ok_or_else(|| invalid("Missing hosted decision flags"))?;
        if *flags & !0b1_1111 != 0 {
            return Err(invalid("Invalid hosted decision flags"));
        }
        let counted = *flags & 1 != 0;
        let recheck_at_staging = *flags & 8 != 0;
        let (slot, body) = if *flags & 16 != 0 {
            let (slot, body) = self.owned_clock_slot(call, body).await?;
            (Some(slot), body)
        } else {
            (None, body)
        };
        let (refusal, primary) = if *flags & 4 != 0 {
            let (length, body) = body.split_first_chunk::<4>()
                .ok_or_else(|| invalid("Missing hosted refusal length"))?;
            let (refusal, primary) = body.split_at_checked(u32::from_be_bytes(*length) as usize)
                .ok_or_else(|| invalid("Truncated hosted refusal"))?;
            (Some(refusal), primary)
        } else {
            (None, body)
        };
        let (worlds, primary) = if *flags & 0b10 != 0 {
            self.hosted_world_read_prefix(call, primary)?
        } else {
            (Vec::new(), primary)
        };
        self.commit_hosted_with_reads(call, envelope, primary, counted, worlds, refusal, recheck_at_staging, slot).await
    }

    async fn commit_hosted_with_reads(
        &self, call: &Call, envelope: &Bytes, body: &[u8], counted: bool, worlds: Vec<HostedWorldRead>,
        refusal: Option<&[u8]>, recheck_at_staging: bool, slot: Option<rtdb_types::TransactionKey>,
    ) -> Result<ActionResult, Status> {
        let policy = self.hosted_control_policy(call)?;
        let (conditions, body, _counts) = if counted {
            owned_count_conditions(call, body, None, false)?
        } else {
            (Vec::new(), body, Vec::new())
        };
        let (witnesses, body, _witness_charge) = self.owned_witness_prefix(call, body, true)?;
        let (deadline, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Hosted commit has no deadline"))?;
        let deadline = Timestamp(i64::from_be_bytes(*deadline));
        let (journal_before, body) = if recheck_at_staging {
            let (before, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Missing physical journal deadline"))?;
            let before = Timestamp(i64::from_be_bytes(*before));
            let (certificate, body) = body.split_first_chunk::<32>().ok_or_else(|| invalid("Missing clock certificate digest"))?;
            if deadline.0 <= 0 || before < deadline { return Err(invalid("Invalid physical journal deadline")); }
            let cutoff = rtdb_types::TimedJournalCutoff::new(before, *certificate).map_err(status)?;
            (Some(cutoff), body)
        } else { (None, body) };
        let (length, body) = body.split_first_chunk::<4>().ok_or_else(|| invalid("Hosted commit has no fact group length"))?;
        let (facts, controls) = body.split_at_checked(u32::from_be_bytes(*length) as usize)
            .ok_or_else(|| invalid("Hosted fact group stream is truncated"))?;
        let fact_groups = ordered_group_count(facts)?;
        let _framing = call.reserve((fact_groups + MAX_COMMIT_GROUPS) * 512)?;
        let facts = owned_groups(facts, false, true)?;
        let controls = owned_groups(controls, true, false)?;
        let fact_relations = facts.iter().map(|(relation, _)| relation).collect::<BTreeSet<_>>();
        if fact_relations.len() + controls.len() > MAX_COMMIT_GROUPS {
            return Err(Status::permission_denied("Hosted control targets are outside the installed allowlist"));
        }
        hosted_control::admit_control_targets(policy, controls.iter().map(|(relation, _)| relation))?;
        let scope = self.hosted_control_scope(call).await?;

        let world_ops = worlds.iter()
            .map(|read| read.witnesses.len() * 2 + usize::from(read.clock.is_some()))
            .sum::<usize>() + usize::from(!worlds.is_empty());
        let mut rows_left = MAX_GROUPED_COMMIT_ROWS.checked_sub(world_ops + conditions.len() + usize::from(slot.is_some()))
            .ok_or_else(|| invalid("Hosted conditions exceed the commit limit"))?;
        let mut writes = vec![TxOp::OwnedDeadline { until: deadline, recheck_at_staging, journal_before }];
        writes.extend(slot.map(|predecessor| TxOp::OwnedClockSlot { predecessor }));
        writes.extend(conditions);
        let (controls, _held) = self.decode_hosted_control_groups(call, controls, scope, &mut rows_left, false)?;
        let replay = rtdb_tx::hosted_replay_controls(controls.iter()).map_err(status)?.is_some();
        if replay && slot.is_some() {
            return Err(invalid("A replay cannot consume an ordered clock slot"));
        }
        if recheck_at_staging && (replay || controls.iter().all(rtdb_tx::HostedControlWrite::is_check)) {
            return Err(invalid("A staging deadline requires a live hosted control replacement"));
        }
        if replay {
            for (held, _) in &witnesses {
                held.snapshot.require_unscoped_root().map_err(status)?;
            }
        }
        if replay && (counted || !worlds.is_empty()) {
            return Err(invalid("Native replay admits an original effect batch"));
        }
        let effects = CommitEffects::hosted(replay, counted || !worlds.is_empty(), &controls);
        let _facts = self.decode_owned_commit_groups(call, envelope, facts, &mut writes, &mut rows_left, effects)?;
        writes.extend(controls.into_iter().map(TxOp::HostedControl));
        let _refusal = if let Some(refusal) = refusal {
            if replay {
                return Err(invalid("A hosted constraint decision requires live Root custody"));
            }
            let (len, body) = refusal.split_first_chunk::<4>()
                .ok_or_else(|| invalid("Missing refusal fact stream length"))?;
            let (facts, controls) = body.split_at_checked(u32::from_be_bytes(*len) as usize)
                .ok_or_else(|| invalid("Truncated refusal fact stream"))?;
            let facts = owned_groups(facts, false, true)?;
            let controls = owned_groups(controls, true, false)?;
            if facts.len() != 2 || controls.is_empty() {
                return Err(invalid("A hosted refusal requires its original receipt groups"));
            }
            hosted_control::admit_control_targets(policy, controls.iter().map(|(relation, _)| relation))?;
            let mut alternative = Vec::new();
            let (controls, held) = self.decode_hosted_control_groups(
                call, controls, scope, &mut rows_left, false,
            )?;
            let facts = self.decode_owned_commit_groups(
                call, envelope, facts, &mut alternative, &mut rows_left, CommitEffects::Facts,
            )?;
            alternative.extend(controls.into_iter().map(TxOp::HostedControl));
            writes.push(TxOp::HostedConstraintRefusal);
            writes.extend(alternative);
            Some((facts, held))
        } else {
            None
        };
        if let Some(tick) = slot {
            self.wait_for_clock_slot(call, tick).await?;
        }
        self.owned_write_reply_with_reads(call, writes, witnesses, worlds).await
    }

    fn decode_hosted_control_groups(
        &self, call: &Call, groups: Vec<(OwnedRelation, &[u8])>, scope: rtdb_types::PlacedAttemptScope,
        rows_left: &mut usize, require_checks: bool,
    ) -> Result<(Vec<rtdb_tx::HostedControlWrite>, Vec<(Arc<MemoryReservation>, Arc<MemoryReservation>)>), Status> {
        let mut held = Vec::with_capacity(groups.len());
        let mut controls = Vec::new();
        for (relation, arrow) in groups {
            let batch = request_batch(call, arrow)?;
            let rows = batch.batch().num_rows();
            let schema = batch.batch().schema_ref();
            let exact_type = |name, kind| schema.field_with_name(name).is_ok_and(|field| field.data_type() == &kind);
            let unique = schema.fields().iter().map(|field| field.name()).collect::<BTreeSet<_>>();
            if rows == 0 || unique.len() != schema.fields().len() || schema.fields().len() > 131
                || (schema.index_of("_check_only").is_ok() && !exact_type("_check_only", DataType::Boolean))
                || !exact_type("_id", DataType::Utf8) || !exact_type("_expected_micros", DataType::Int64)
            {
                return Err(invalid("Hosted controls require distinct fields, Utf8 IDs and explicit Int64 expectations"));
            }
            *rows_left = rows_left.checked_sub(rows).ok_or_else(|| invalid("Hosted commit exceeds its total row limit"))?;
            let reservation = call.reserve(wire::mul(rows, std::mem::size_of::<TxOp>() + 512)?)?;
            controls.try_reserve_exact(rows).map_err(|_| Status::resource_exhausted("Hosted control allocation failed"))?;
            let (rows, documents) = upload::owned_documents(batch.batch(), call.memory())?;
            for mut row in rows {
                strip_owned_absence(&mut row)?;
                let expected = match row.remove("_expected_micros") {
                    Some(Value::I64(time)) => Some(Timestamp(time)),
                    Some(Value::Null) => None,
                    _ => return Err(invalid("Hosted control expectation must be Int64 or null")),
                };
                let check_only = match row.remove("_check_only") {
                    Some(Value::Bool(check_only)) => check_only,
                    None => false,
                    _ => return Err(invalid("Hosted control check flag must be Boolean")),
                };
                if require_checks && !check_only { return Err(invalid("Read validation permits only hosted control checks")); }
                let control = if check_only {
                    rtdb_tx::HostedControlWrite::check(relation.clone(), scope, expected, row)
                } else {
                    rtdb_tx::HostedControlWrite::new(relation.clone(), scope, expected, row)
                }.map_err(status)?;
                controls.push(control);
            }
            held.push((reservation, documents));
        }
        Ok((controls, held))
    }

    async fn commit_owned(&self, call: &Call, envelope: &Bytes, body: &[u8], format: CommitFormat, witnesses: Witnesses, erase_allowed: bool, conditions: Vec<TxOp>) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let group_limit = match format {
            CommitFormat::Single => 1,
            CommitFormat::Grouped => MAX_COMMIT_GROUPS,
            CommitFormat::Ordered => ordered_group_count(body.get(8..).unwrap_or_default())?,
        };
        let _framing = call.reserve(group_limit * 512)?;
        let CommitRequest { deadline, groups } = CommitRequest::parse(body, format)?;
        let mut rows_left = match format {
            CommitFormat::Single => usize::MAX,
            CommitFormat::Grouped | CommitFormat::Ordered => MAX_GROUPED_COMMIT_ROWS.checked_sub(conditions.len())
                .ok_or_else(|| invalid("Owned conditions exceed the commit limit"))?,
        };
        let mut writes = vec![TxOp::owned_deadline(deadline)];
        writes.extend(conditions);
        let effects = if erase_allowed { CommitEffects::Replay } else { CommitEffects::Facts };
        let _rows = self.decode_owned_commit_groups(call, envelope, groups, &mut writes, &mut rows_left, effects)?;
        self.owned_write_reply_with_witnesses(call, writes, witnesses).await
    }

    fn decode_owned_commit_groups(
        &self, call: &Call, envelope: &Bytes, groups: Vec<(OwnedRelation, &[u8])>, writes: &mut Vec<TxOp>,
        rows_left: &mut usize, effects: CommitEffects,
    ) -> Result<Vec<(Arc<MemoryReservation>, Arc<MemoryReservation>)>, Status> {
        let mut retained = Vec::with_capacity(groups.len());
        for (relation, arrow) in groups {
            let batch = require_identity(decode_retained_batch(call, envelope.slice_ref(arrow))?)?;
            let rows = batch.batch().num_rows();
            *rows_left = rows_left.checked_sub(rows)
                .ok_or_else(|| invalid("Owned commit exceeds its total row limit"))?;
            let operations = wire::mul(rows, 2)?;
            let mut operations_charge = call.reserve(wire::mul(operations, std::mem::size_of::<TxOp>() + 512)?)?;
            writes.try_reserve_exact(operations)
                .map_err(|_| Status::resource_exhausted("Owned commit operation allocation failed"))?;
            if relation.is_root_data() {
                let charge = Arc::get_mut(&mut operations_charge).expect("commit group owns its operation reservation");
                for row in 0..rows {
                    let start = writes.len();
                    let bytes = charge.bytes();
                    if !self.append_owned_arrow_row(writes, &relation, &batch, row, effects, charge)? {
                        writes.truncate(start);
                        charge.shrink_to(bytes).map_err(status)?;
                        let (row, _decoded) = upload::owned_documents(&batch.batch().slice(row, 1), call.memory())?;
                        self.append_owned_commit_group(writes, &relation, row, effects)?;
                    }
                }
                retained.push((operations_charge, Arc::clone(batch.reservation())));
                continue;
            }
            let (rows, documents_charge) = upload::owned_documents(batch.batch(), call.memory())?;
            self.append_owned_commit_group(writes, &relation, rows, effects)?;
            retained.push((operations_charge, documents_charge));
        }
        Ok(retained)
    }

    fn owned_payload(&self, batch: &OwnedBatch, row: usize, positions: &[usize]) -> Result<Option<ArrowDocuments>, Status> {
        let selected = batch.batch().slice(row, 1).project(positions).map_err(arrow)?;
        let selected = OwnedBatch::with_reservation(selected, batch.reservation().clone()).map_err(status)?;
        ArrowDocuments::from_owned_payload(&selected, self.memory()).map_err(status)
    }

    fn append_owned_arrow_row(
        &self, writes: &mut Vec<TxOp>, relation: &OwnedRelation, batch: &OwnedBatch, row: usize, effects: CommitEffects, charge: &mut MemoryReservation,
    ) -> Result<bool, Status> {
        let source = batch.batch();
        let controls = [
            "_op", "_read_valid_micros", "_read_system_micros", "_valid_from", "_valid_to",
            "_valid_from_offset_micros", "_valid_to_offset_micros",
        ];
        let positions = owned_payload_columns(source, row, &controls)?;
        let cell = |name| ArrowCell::named(source, name, row);
        let id = {
            let cell = cell("_id").ok_or_else(|| invalid("Owned commit row has no _id"))?;
            read_entity_id(cell.field, cell.array, cell.row).map_err(status)?
        };
        let mut own_id = || -> Result<EntityId, Status> {
            charge.grow_to(wire::add(charge.bytes(), id.owned_payload_bytes())?).map_err(status)?;
            Ok(id.into_owned())
        };
        let operation = cell("_op").and_then(ArrowCell::utf8).ok_or_else(|| invalid("Owned commit _op must be Utf8"))?;
        if operation == "require-spatial-path" && effects.allows_spatial_requirement() {
            let EntityIdRef::String(name) = id else { return Err(invalid("Spatial readiness needs a path name")); };
            if source.num_columns() != 4 { return Err(invalid("Invalid spatial readiness condition")); }
            let columns = cell("columns").and_then(ArrowCell::list)
                .filter(|columns| (1..=16).contains(&columns.len()))
                .ok_or_else(|| invalid("Spatial readiness requires a bounded column list"))?;
            let columns = columns.map(|column| column.utf8().ok_or_else(|| invalid("Spatial readiness column is not text")))
                .collect::<Result<Vec<_>, _>>()?;
            let bytes = columns.iter().try_fold(name.len() + 1024, |bytes, column| wire::add(bytes, column.len() + 64))?;
            charge.grow_to(wire::add(charge.bytes(), bytes)?).map_err(status)?;
            let encoding = cell("geometry_encoding").and_then(ArrowCell::utf8)
                .ok_or_else(|| invalid("Spatial readiness needs a geometry encoding"))?;
            writes.push(TxOp::OwnedRequireSpatialPath {
                relation: relation.clone(), name: OwnedRelation::new(name).map_err(status)?,
                columns: columns.into_iter().map(str::to_owned).collect(),
                encoding: rtdb_types::SpatialGeometryEncoding::parse(encoding).map_err(status)?,
            });
            return Ok(true);
        }
        if operation == "retire-relation" && matches!(effects, CommitEffects::HostedRoot) {
            let EntityIdRef::String(name) = id else { return Err(invalid("A relation retirement names its relation")); };
            if source.num_columns() != 3 || name != relation.name() {
                return Err(invalid("A relation retirement carries only its relation and retention"));
            }
            let retain_micros = cell("retain_micros").and_then(ArrowCell::int64)
                .ok_or_else(|| invalid("A relation retirement needs a signed microsecond retention"))?;
            writes.push(TxOp::OwnedRetireRelation { relation: relation.clone(), retain_micros });
            return Ok(true);
        }
        let optional = |name| cell(name).map(optional_bound).transpose().map(Option::flatten);
        let signed = |name| -> Result<Option<Timestamp>, Status> {
            optional(name)?.map(|cell| cell.int64().map(Timestamp)
                .ok_or_else(|| invalid("Owned observation requires signed microsecond integers"))).transpose()
        };
        let valid_at = signed("_read_valid_micros")?;
        let expected = signed("_read_system_micros")?;
        match valid_at {
            Some(valid_at) => writes.push(TxOp::OwnedCheck { relation: relation.clone(), id: own_id()?, valid_at, expected }),
            None if expected.is_some() => return Err(invalid("Owned point expectation has no valid instant")),
            None => (),
        }
        let endpoint = |name| -> Result<Option<Timestamp>, Status> {
            optional(name)?.map(|cell| cell.zoned_micros()
                .ok_or_else(|| invalid("Owned mutation validity bounds must be zoned microsecond timestamps"))).transpose()
        };
        let valid_from = endpoint("_valid_from")?;
        let valid_to = endpoint("_valid_to")?;
        let has_payload = || -> Result<bool, Status> {
            for &position in &positions {
                let field = source.schema_ref().field(position);
                let value = ArrowCell { field, array: source.column(position).as_ref(), row };
                if field.name() != "_id" && !value.is_null().map_err(status)? { return Ok(true); }
            }
            Ok(false)
        };
        let from_offset = signed("_valid_from_offset_micros")?;
        let to_offset = signed("_valid_to_offset_micros")?;
        if from_offset.is_some() || to_offset.is_some() {
            if matches!(effects, CommitEffects::Replay) {
                return Err(invalid("Invalid relative Root period"));
            }
            let period = OwnedRelativePeriod::from_fields(valid_from, valid_to, from_offset, to_offset)
                .ok_or_else(|| invalid("A relative Root period needs one coordinate for each finite bound"))?;
            let effect = match operation {
                "create" | "replace" => {
                    let Some(documents) = self.owned_payload(batch, row, &positions)? else { return Ok(false); };
                    rtdb_tx::OwnedPeriodEffect::Arrow { documents, create_only: operation == "create" }
                }
                "delete" if !has_payload()? => rtdb_tx::OwnedPeriodEffect::Delete(own_id()?),
                _ => return Err(invalid("A relative Root period requires a fact write")),
            };
            writes.push(TxOp::OwnedRelativePeriod {
                relation: relation.clone(), effect, period,
            });
            return Ok(true);
        }
        let operation = match operation {
            "check" => {
                if valid_at.is_none() || valid_from.is_some() || valid_to.is_some() || has_payload()? {
                    return Err(invalid("Owned check requires one observation and no mutation fields"));
                }
                return Ok(true);
            }
            "create" | "replace" => {
                let Some(documents) = self.owned_payload(batch, row, &positions)? else { return Ok(false); };
                TxOp::OwnedArrowDocs { relation: relation.clone(), documents, create_only: operation == "create", valid_from, valid_to }
            }
            "delete" => {
                if has_payload()? { return Err(invalid("Owned delete carries document values")); }
                TxOp::OwnedDelete { relation: relation.clone(), id: own_id()?, valid_from, valid_to }
            }
            "erase" if matches!(effects, CommitEffects::Replay) => {
                if valid_from.is_some() || valid_to.is_some() || has_payload()? {
                    return Err(invalid("Owned erase requires only a typed identity and optional observation"));
                }
                TxOp::OwnedErase { relation: relation.clone(), id: own_id()? }
            }
            "declare-capacity" if matches!(effects, CommitEffects::HostedRoot) => return Ok(false),
            _ => return Err(invalid("Unknown owned commit operation")),
        };
        writes.push(operation);
        Ok(true)
    }

    fn append_owned_commit_group(
        &self,
        writes: &mut Vec<TxOp>,
        relation: &OwnedRelation,
        rows: Vec<rtdb_types::StructFields<Value>>,
        effects: CommitEffects,
    ) -> Result<(), Status> {
        for mut row in rows {
            strip_owned_absence(&mut row)?;
            let id = row.get("_id").ok_or_else(|| invalid("Owned commit row has no _id"))?.entity_id().map_err(status)?;
            let Value::Utf8(operation) = row.remove("_op").ok_or_else(|| invalid("Owned commit row has no _op"))? else {
                return Err(invalid("Owned commit _op must be Utf8"));
            };
            let valid_at = optional_micros(row.remove("_read_valid_micros"))?;
            let expected = optional_micros(row.remove("_read_system_micros"))?;
            match valid_at {
                Some(valid_at) => writes.push(TxOp::OwnedCheck { relation: relation.clone(), id: id.clone(), valid_at, expected }),
                None if expected.is_some() => return Err(invalid("Owned point expectation has no valid instant")),
                None => (),
            }
            let valid_from = valid_endpoint(row.remove("_valid_from"))?;
            let valid_to = valid_endpoint(row.remove("_valid_to"))?;
            let from_offset = optional_micros(row.remove("_valid_from_offset_micros"))?;
            let to_offset = optional_micros(row.remove("_valid_to_offset_micros"))?;
            if from_offset.is_some() || to_offset.is_some() {
                if !relation.is_root_data() || matches!(effects, CommitEffects::Replay) {
                    return Err(invalid("Invalid relative Root period"));
                }
                let period = OwnedRelativePeriod::from_fields(valid_from, valid_to, from_offset, to_offset)
                    .ok_or_else(|| invalid("A relative Root period needs one coordinate for each finite bound"))?;
                let effect = match operation.as_str() {
                    "create" => rtdb_tx::OwnedPeriodEffect::Create(Documents::new(vec![row], self.memory()).map_err(status)?),
                    "replace" => rtdb_tx::OwnedPeriodEffect::Replace(Documents::new(vec![row], self.memory()).map_err(status)?),
                    "delete" if !has_payload(&row) => rtdb_tx::OwnedPeriodEffect::Delete(id),
                    _ => return Err(invalid("A relative Root period requires a fact write")),
                };
                writes.push(TxOp::OwnedRelativePeriod {
                    relation: relation.clone(), effect, period,
                });
                continue;
            }
            if operation == "check" {
                if valid_at.is_none() || valid_from.is_some() || valid_to.is_some() || has_payload(&row) {
                    return Err(invalid("Owned check requires one observation and no mutation fields"));
                }
                continue;
            }
            let operation = match operation.as_str() {
                "create" => TxOp::OwnedInsertDocs {
                    relation: relation.clone(), documents: Documents::new(vec![row], self.memory()).map_err(status)?, valid_from, valid_to,
                },
                "replace" => TxOp::OwnedReplaceDocs {
                    relation: relation.clone(), documents: Documents::new(vec![row], self.memory()).map_err(status)?, valid_from, valid_to,
                },
                "delete" => {
                    if has_payload(&row) { return Err(invalid("Owned delete carries document values")); }
                    TxOp::OwnedDelete { relation: relation.clone(), id, valid_from, valid_to }
                }
                "erase" if matches!(effects, CommitEffects::Replay) => {
                    if valid_from.is_some() || valid_to.is_some() || has_payload(&row) {
                        return Err(invalid("Owned erase requires only a typed identity and optional observation"));
                    }
                    TxOp::OwnedErase { relation: relation.clone(), id }
                }
                "declare-capacity" if matches!(effects, CommitEffects::HostedRoot) => {
                    if valid_at.is_some() || valid_from.is_some() || valid_to.is_some()
                        || row.remove("_id").as_ref() != row.get("column")
                    {
                        return Err(invalid("Hosted capacity declaration requires an unqualified projection identity"));
                    }
                    owned_capacity_declaration(relation.clone(), &row)?
                }
                _ => return Err(invalid("Unknown owned commit operation")),
            };
            writes.push(operation);
        }
        Ok(())
    }

    async fn owned_write_reply(&self, call: &Call, operations: Vec<TxOp>) -> Result<ActionResult, Status> {
        self.owned_write_reply_with_witnesses(call, operations, Vec::new()).await
    }

    async fn owned_write_reply_with_witnesses(
        &self, call: &Call, operations: Vec<TxOp>, witnesses: Witnesses,
    ) -> Result<ActionResult, Status> {
        self.owned_write_reply_with_reads(call, operations, witnesses, Vec::new()).await
    }

    async fn owned_write_reply_with_reads(
        &self, call: &Call, mut operations: Vec<TxOp>, witnesses: Witnesses,
        mut worlds: Vec<HostedWorldRead>,
    ) -> Result<ActionResult, Status> {
        let connection = self.connection(call, &call.database).await?;
        let mut native = connection.borrow().await?;
        require_idle(&native)?;
        let refusal = operations.iter().position(|op| matches!(op, TxOp::HostedConstraintRefusal))
            .map(|boundary| operations.split_off(boundary));
        if !witnesses.is_empty() {
            let scope = native.placed_history().map_err(status)?;
            operations.try_reserve_exact(witnesses.len())
                .map_err(|_| Status::resource_exhausted("Owned witness allocation failed"))?;
            for (held, evidence) in &witnesses {
                held.snapshot.check().map_err(status)?;
                if !evidence.matches_scope(scope) {
                    return Err(Status::failed_precondition("Owned range witness belongs to another placement"));
                }
                operations.push(evidence.clone().operation());
            }
        }
        if !worlds.is_empty() {
            let observed = take_held_world_observations(&mut worlds)?;
            native.append_hosted_world_read_checks(&mut operations, observed).map_err(status)?;
        }
        operations.extend(refusal.into_iter().flatten());
        let outcome = native.state_mut().execute_tx(operations, Default::default()).await.map_err(status)?;
        drop(worlds);
        if let Some(error) = outcome.error() { return Err(status(error.clone())); }
        if !outcome.committed() { return Err(Status::aborted("Owned write did not commit")); }
        self.owned_tx_reply(outcome.tx_key())
    }

    fn owned_tx_reply(&self, tx: rtdb_types::TransactionKey) -> Result<ActionResult, Status> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("tx_id", DataType::Int64, false),
            Field::new("system_time_micros", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
            Arc::new(Int64Array::from(vec![tx.tx_id.0])),
            Arc::new(Int64Array::from(vec![tx.system_time.0])),
        ]).map_err(arrow)?;
        self.owned_action_reply(&schema, Some(&batch))
    }

    pub(crate) async fn open_owned_read_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        if !body.is_empty() { return Err(invalid("Owned read open takes an empty body")); }
        self.retain_new_owned_read(call, None, 0).await
    }

    pub(crate) async fn open_applied_owned_read_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        if call.session.placed_database() != Some(call.database.as_str()) {
            return Err(Status::permission_denied("An applied owned read requires its placed owner session"));
        }
        let (floor, rest) = hosted_observation::applied_read_floor(body)?;
        let generation = u64::from_be_bytes(rest.try_into().map_err(|_| invalid("Applied read requires an exact generation"))?);
        if let Some(source) = call.session.audience_source() {
            if floor.history() != source {
                return Err(Status::permission_denied("The applied floor belongs to another audience source"));
            }
        } else if self.0.config.reserved_applied_source == Some(floor.history()) {
            return Err(Status::permission_denied("This source requires a reserved Flight session"));
        } else if generation != 0 {
            return Err(invalid("An ordinary applied read uses generation zero"));
        }
        self.retain_new_owned_read(call, Some(&floor), generation).await
    }

    async fn retain_new_owned_read(
        &self, call: &Call, applied_floor: Option<&rtdb_node::AppliedReadFloor>, generation: u64,
    ) -> Result<ActionResult, Status> {
        let audience = call.session.audience_source().is_some();
        let permit = if audience { None } else {
            Some(Arc::clone(&self.0.owned_read_slots).try_acquire_owned()
                .map_err(|_| Status::resource_exhausted("The node has too many retained owned reads"))?)
        };
        let connection = self.connection(call, &call.database).await?;
        let mut native = connection.borrow().await?;
        require_idle(&native)?;
        // The audience session owns one prepaid slot. A lost reply can leave its last handle behind.
        if audience { call.session.begin_audience_read(generation)?; }
        let snapshot = match applied_floor {
            Some(minimum) => native.open_applied_owned_read(minimum).await,
            None => native.open_owned_read().await,
        }.map_err(status)?;
        self.retain_owned_read(call, snapshot, permit, true, generation)
    }

    pub(crate) async fn open_owned_scoped_read_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        if call.session.placed_database() != Some(call.database.as_str()) {
            return Err(Status::permission_denied("A scoped owned read requires its placed owner session"));
        }
        let (handle, body) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Scoped owned read requires an original hold"))?;
        let (count, mut body) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Scoped owned read requires a relation count"))?;
        let count = usize::from(u16::from_be_bytes(*count));
        if !(1..=32).contains(&count) { return Err(invalid("Scoped owned read requires one to 32 relations")); }
        let _request = call.reserve(count * 512)?;
        let mut relations = Vec::with_capacity(count);
        for _ in 0..count {
            let (relation, tail) = split_relation(body)?;
            relations.push(relation);
            body = tail;
        }
        if !body.is_empty() { return Err(invalid("Scoped owned read has trailing bytes")); }
        let original = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let permit = Arc::clone(&self.0.owned_read_slots).try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("The node has too many retained owned reads"))?;
        let snapshot = original.snapshot.scope_root_read(relations).await.map_err(status)?;
        self.retain_owned_read(call, snapshot, Some(permit), true, 0)
    }

    pub(crate) async fn open_owned_read_as_of_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, body) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Historical read requires an existing held handle"))?;
        let (requested, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Historical read requires a requested system time"))?;
        let (publication, body) = split_relation(body)?;
        let (length, body) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Historical read requires a publication marker"))?;
        let length = usize::from(u16::from_be_bytes(*length));
        if !(1..=4096).contains(&length) { return Err(invalid("Invalid historical marker length")); }
        let (marker, body) = body.split_at_checked(length).ok_or_else(|| invalid("Incomplete historical marker"))?;
        let marker = std::str::from_utf8(marker).map_err(|_| invalid("Historical marker is not UTF-8"))?;
        let (count, mut body) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Historical read requires a relation count"))?;
        let count = usize::from(u16::from_be_bytes(*count));
        if !(1..=32).contains(&count) { return Err(invalid("Historical reads require one to 32 owner relations")); }
        let _request = call.reserve(count * 512 + marker.len() + 512)?;
        let mut relations = Vec::with_capacity(count);
        for _ in 0..count {
            let (relation, tail) = split_relation(body)?;
            relations.push(relation);
            body = tail;
        }
        if !body.is_empty() { return Err(invalid("Historical read has trailing bytes")); }
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let permit = Arc::clone(&self.0.owned_read_slots).try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("The node has too many retained owned reads"))?;
        let marker = EntityId::String(marker.to_owned());
        let snapshot = held.snapshot.derive_as_of(Timestamp(i64::from_be_bytes(*requested)), &publication, &marker, relations)
            .await.map_err(status)?;
        held.snapshot.check_relation(&publication).map_err(status)?;
        match snapshot {
            Some(snapshot) => self.retain_owned_read(call, snapshot, Some(permit), false, 0),
            None => Ok(ActionResult { body: Bytes::from_static(&[0]) }),
        }
    }

    fn retain_owned_read(&self, call: &Call, snapshot: OwnedReadSession, permit: Option<OwnedSemaphorePermit>, include_transaction: bool, generation: u64) -> Result<ActionResult, Status> {
        let reservation = call.reserve(wire::add(2048, wire::add(call.session.id.len(), call.database.len())?)?)?;
        let reply_charge = call.reserve(64)?;
        let handle = Uuid::new_v4();
        let mut reply = Vec::with_capacity(if include_transaction { 32 } else { 24 });
        reply.extend_from_slice(handle.as_bytes());
        reply.extend_from_slice(&snapshot.system_time().0.to_be_bytes());
        if include_transaction {
            reply.extend_from_slice(&snapshot.present().tx_id.0.to_be_bytes());
        }
        let held = Arc::new(OwnedReadResource {
            snapshot, owner: call.session.owner(), session: call.session.id.clone(), database: call.database.clone(),
            audience_work: rtdb_storage::audience_work::current_turn(),
            audience_projection_work: (generation > 1 && call.session.audience_source().is_some()).then(|| {
                rtdb_storage::audience_work::TurnAllowance::new(
                    crate::config::AUDIENCE_TURN_LIMITS, crate::config::AUDIENCE_TURN_ACTIONS,
                )
            }),
            witnesses: Mutex::default(), spatial_filters: Mutex::default(), _permit: permit, _reservation: reservation,
        });
        let handle = Bytes::copy_from_slice(handle.as_bytes());
        if call.session.audience_source().is_some() {
            call.session.install_audience_read(generation, handle, held)?;
        } else {
            self.0.owned_reads.insert(handle, held, READ_IDLE, READ_IDLE, false, 16).map_err(status)?;
        }
        Ok(ActionResult { body: wire::retain(reply, reply_charge) })
    }

    fn owned_read(&self, call: &Call, handle: &Bytes) -> Result<Arc<OwnedReadResource>, Status> {
        let entry = self.retained_owned_read(call, handle)?;
        if entry.snapshot.is_applied() {
            return Err(Status::permission_denied("The applied read handle is limited to point and hosted control observations"));
        }
        Ok(entry)
    }

    fn retained_owned_read(&self, call: &Call, handle: &Bytes) -> Result<Arc<OwnedReadResource>, Status> {
        let entry = if call.session.audience_source().is_some() {
            call.session.audience_read(handle).ok_or_else(|| Status::not_found("unknown owned read"))?
        } else {
            found(self.0.owned_reads.lookup(handle, false), "owned read")?
        };
        if entry.session != call.session.id || entry.database != call.database || !call.session.owns(&entry.owner) {
            return Err(Status::not_found("unknown owned read"));
        }
        Ok(entry)
    }

    pub(crate) fn audience_open_generation(&self, body: &[u8]) -> Result<u64, Status> {
        let (_, rest) = hosted_observation::applied_read_floor(body)?;
        let generation = u64::from_be_bytes(rest.try_into().map_err(|_| invalid("Applied read requires an exact generation"))?);
        if generation == 0 { return Err(invalid("An audience read requires a prepaid generation")); }
        Ok(generation)
    }

    pub(crate) fn audience_read_work(
        &self, call: &Call, action: &str, body: &[u8],
    ) -> Result<rtdb_storage::audience_work::TurnAllowance, Status> {
        let (handle, request) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("The audience observation has no original read handle"))?;
        let held = self.retained_owned_read(call, &Bytes::copy_from_slice(handle))?;
        let work = match action {
            HELD_OWNED_POINT_V2 | HELD_OWNED_RANGE_V2 => {
                if request.len() > 16 * 1024 {
                    return Err(invalid("The audience projection request exceeds its carrier bound"));
                }
                let (relation, _) = split_relation(request)?;
                let projection = call.session.audience_projection()?;
                if relation == projection.relation {
                    &held.audience_projection_work
                } else if action == HELD_OWNED_POINT_V2
                    && (relation.name() == rtdb_types::OWNED_ROOT_PUBLICATION_RELATION
                        || (held.audience_projection_work.is_none() && relation.name() == "fact_layout_readiness"))
                {
                    &held.audience_work
                } else {
                    return Err(Status::permission_denied("The relation is outside the prepared audience projection"));
                }
            },
            READ_HELD_HOSTED_CONTROL_V2 | CONFIRM_APPLIED_HOSTED_V5 => &held.audience_work,
            _ => return Err(unsupported()),
        };
        work.clone().ok_or_else(|| Status::failed_precondition("The original read has no audience work allowance"))
    }

    pub(crate) async fn held_owned_point_action(&self, call: &Call, body: &[u8], time: PointTime) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned held point has no handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.retained_owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Ok(entry) = self.take_owned_read(call, &handle) { entry.close(); }
            return Err(status(error));
        }
        let (relation, id, valid, _identity) = point_request(call, request, time)?;
        if call.session.audience_source().is_some() {
            require_audience_point(&call.session, &relation, &id, held.audience_projection_work.is_none())?;
        }
        let fixed = matches!(time, PointTime::Required);
        self.execute_owned_point(&held.snapshot, &relation, &id, valid, fixed, Some((call, &held))).await
    }

    pub(crate) async fn held_owned_historical_point_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned historical point has no handle"))?;
        let (system, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Owned historical point has no system time"))?;
        let system = Timestamp(i64::from_be_bytes(*system));
        let held = self.retained_owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, id, valid, _identity) = point_request(call, request, PointTime::Optional)?;
        let valid = valid.unwrap_or(system);
        let cursor = held.snapshot.open_historical_point(&relation, &id, system, valid).await.map_err(status)?;
        let reply = self.finish_owned_point(call, system, valid, &id, cursor, None).await?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_owned_position_point_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned position point has no handle"))?;
        let (system, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Owned position point has no system time"))?;
        let system = Timestamp(i64::from_be_bytes(*system));
        let (mask, request) = split_position_mask(request)?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, id, valid, _identity) = point_request(call, request, PointTime::Required)?;
        let valid = valid.ok_or_else(|| invalid("Owned position point has no valid instant"))?;
        let cursor = held.snapshot.open_historical_position_point(&relation, &id, system, valid, mask).await.map_err(status)?;
        let reply = self.finish_owned_point(call, system, valid, &id, cursor, None).await?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    fn path_touch_request(
        &self, call: &Call, request: &[u8],
    ) -> Result<(Timestamp, Timestamp, OwnedRelation, OwnedRelation, EntityId, Timestamp, Arc<MemoryReservation>), Status> {
        let (group, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Owned path touch has no group time"))?;
        let group = Timestamp(i64::from_be_bytes(*group));
        let before = Timestamp(group.0.checked_sub(1).ok_or_else(|| invalid("Owned path touch has no preceding cut"))?);
        let (relation, request) = split_relation(request)?;
        let (path, arrow_bytes) = split_relation(request)?;
        let batch = request_batch(call, arrow_bytes)?;
        let batch = batch.batch();
        if batch.num_columns() != 2 || batch.num_rows() != 1 {
            return Err(invalid("Owned path touch requires one identity and valid instant"));
        }
        let (id, identity) = point_identity(call, batch)?;
        let valid = history_request_endpoint(batch, "valid_micros")?;
        Ok((before, group, relation, path, id, valid, identity))
    }

    pub(crate) async fn held_world_version_action(
        &self, call: &Call, body: &[u8],
    ) -> Result<Vec<Result<ActionResult, Status>>, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("World version has no held handle"))?;
        let (group, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("World version has no group time"))?;
        let group = Timestamp(i64::from_be_bytes(*group));
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = request_batch(call, arrow_bytes)?;
        let batch = batch.batch();
        if batch.num_columns() != 3 || batch.num_rows() != 1 {
            return Err(invalid("World version requires one typed identity and two system endpoints"));
        }
        let (id, _identity) = point_identity(call, batch)?;
        let from = history_request_endpoint(batch, "system_from_micros")?;
        let through = history_request_endpoint(batch, "system_to_micros")?;
        let (clock, slices) = held.snapshot.version(&relation, &id, from, through, group).await.map_err(status)?;
        if slices.len() > 128 { return Err(history_invalid()); }
        let _framing = call.reserve(wire::mul(slices.len() + 1, 256)?)?;
        let mut header = Vec::with_capacity(16);
        header.extend_from_slice(&group.0.to_be_bytes());
        header.extend_from_slice(&clock.0.to_be_bytes());
        let header = ActionResult { body: wire::retain(header, call.reserve(16)?) };
        let mut bytes = header.encoded_len();
        let mut replies = vec![Ok(header)];
        let mut last = Timestamp::MIN;
        for (interval, point) in slices {
            if interval.lower() < last || interval.is_empty() || point.present != group || point.row.is_none()
                || point.witness.is_some() || point.until.is_some()
            {
                return Err(history_invalid());
            }
            last = interval.upper();
            let reply = self.world_point_reply_with_extent(call, point, interval.lower(), None, Some(interval))?;
            bytes = wire::add(bytes, reply.encoded_len())?;
            if bytes > self.0.config.maximum_message_bytes {
                return Err(Status::resource_exhausted("World version exceeds the Flight response limit"));
            }
            replies.push(Ok(reply));
        }
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(replies)
    }

    pub(crate) async fn held_world_path_touch_action(
        &self, call: &Call, body: &[u8], images: bool,
    ) -> Result<Vec<Result<ActionResult, Status>>, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("World path touch has no held handle"))?;
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let (before, group, relation, path, id, valid, _identity) = self.path_touch_request(call, request)?;
        let [prior, accepted] = held.snapshot.path_touch(&relation, &path, &id, before, group, valid, images).await.map_err(status)?;
        let prior = self.world_point_reply(call, prior, valid, None)?;
        let accepted = self.world_point_reply(call, accepted, valid, None)?;
        if images && wire::add(prior.encoded_len(), accepted.encoded_len())? > self.0.config.maximum_message_bytes {
            return Err(Status::resource_exhausted("World touch images exceed the Flight response limit"));
        }
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(vec![Ok(prior), Ok(accepted)])
    }

    pub(crate) async fn held_owned_path_touch_action(
        &self, call: &Call, body: &[u8],
    ) -> Result<Vec<Result<ActionResult, Status>>, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned path touch has no held read handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (before, group, relation, path, id, valid, _identity) = self.path_touch_request(call, request)?;
        let columns = held.snapshot.path_touch_columns(&relation, &path, &id, before, group, valid).await.map_err(status)?;
        let mut replies = Vec::with_capacity(2);
        for system in [before, group] {
            let cursor = held.snapshot.open_historical_point(&relation, &id, system, valid).await.map_err(status)?;
            let reply = self.finish_owned_point_inner(call.memory(), system, valid, &id, cursor, None, Some(&columns), |_| Ok(None)).await?;
            replies.push(Ok(reply));
        }
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(replies)
    }

    pub(crate) async fn held_owned_valid_during_action(
        &self, call: &Call, body: &[u8], mode: ValidHistoryMode,
    ) -> Result<ActionResult, Status> {
        use rtdb_types::Interval;

        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned history has no held read handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Ok(entry) = self.take_owned_read(call, &handle) { entry.close(); }
            return Err(status(error));
        }
        let (mask, request) = if matches!(mode, ValidHistoryMode::Positioned) {
            let (mask, remaining) = split_position_mask(request)?;
            (Some(mask), remaining)
        } else {
            (None, request)
        };
        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = request_batch(call, arrow_bytes)?;
        let batch = batch.batch();
        if batch.num_columns() != 4 || batch.num_rows() != 1 {
            return Err(invalid("Owned history requires one row with _id, a system cut and two valid endpoints"));
        }
        let (id, _identity) = point_identity(call, batch)?;
        let from = history_request_endpoint(batch, "valid_from_micros")?;
        let to = history_request_endpoint(batch, "valid_to_micros")?;
        let valid = Interval::new(from, to).map_err(status)?;
        if valid.is_empty() { return Err(invalid("Owned history requires a nonempty valid window")); }

        let system = history_request_endpoint(batch, "system_micros")?;
        let history = match mask {
            Some(mask) => held.snapshot.open_historical_position_valid_during(&relation, &id, valid, system, mask).await,
            None if matches!(mode, ValidHistoryMode::Region) =>
                held.snapshot.open_valid_during_region(&relation, &id, valid, system).await,
            None => held.snapshot.open_valid_during_observed(&relation, &id, valid, system).await,
        }.map_err(status)?;
        let read = HistoryRead::Observed { read: history, held: &held, relation: &relation };
        let reply = self.history_reply(call, &id, read, valid, system, None).await?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_owned_identities_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Owned identity page has no held read handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = decode_batch(call, arrow_bytes)?;
        let batch = batch.batch();
        if batch.num_columns() != 4 || batch.num_rows() != 1 {
            return Err(invalid("Owned identity page requires one row with a cut, a bound, and a limit"));
        }
        let system = history_request_endpoint(batch, "system_micros")?;
        let valid = history_request_endpoint(batch, "valid_micros")?;
        let limit = usize::try_from(history_request_integer(batch, "limit")?)
            .map_err(|_| invalid("Owned identity limit is out of range"))?;
        let (column, field) = batch.schema_ref().column_with_name("after")
            .ok_or_else(|| invalid("Owned identity page requires an exclusive bound"))?;
        let after = batch.column(column).as_any().downcast_ref::<StringArray>()
            .filter(|_| field.metadata().is_empty())
            .ok_or_else(|| invalid("Owned identity bound must be nullable Utf8"))?;
        let after = (!after.is_null(0)).then(|| after.value(0));
        let page = held.snapshot.identity_page(&relation, system, valid, after, limit).await.map_err(status)?;
        let bytes = page.keys.iter().try_fold(4096, |size, key| wire::add(size, key.len()))?;
        let _copy = call.reserve(bytes)?;
        let keys = StringArray::from_iter_values(page.keys.iter().map(String::as_str));
        let rows = RecordBatch::try_from_iter([("_id", Arc::new(keys) as Arc<dyn Array>)]).map_err(arrow)?;
        let schema = rows.schema_ref().as_ref().clone().with_metadata(
            [(OWNED_IDENTITY_MORE.into(), u8::from(page.more).to_string())].into()
        );
        let reply = self.owned_point_reply(&schema, Some(&rows), system, valid, None)?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    /// Identities current at the held system cut across all valid time,
    /// for a period table's bounded index build turns.
    pub(crate) async fn held_owned_identities_all_valid_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Owned identity page has no held read handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = decode_batch(call, arrow_bytes)?;
        let batch = batch.batch();
        if batch.num_columns() != 3 || batch.num_rows() != 1 {
            return Err(invalid("An all-valid identity page requires one row with a cut, a limit and a bound"));
        }
        let system = history_request_endpoint(batch, "system_micros")?;
        let limit = usize::try_from(history_request_integer(batch, "limit")?)
            .map_err(|_| invalid("Owned identity limit is out of range"))?;
        let (column, field) = batch.schema_ref().column_with_name("after")
            .ok_or_else(|| invalid("Owned identity page requires an exclusive bound"))?;
        let after = batch.column(column).as_any().downcast_ref::<StringArray>()
            .filter(|_| field.metadata().is_empty())
            .ok_or_else(|| invalid("Owned identity bound must be nullable Utf8"))?;
        let after = (!after.is_null(0)).then(|| after.value(0));
        let page = held.snapshot.identity_page_all_valid(&relation, system, after, limit).await.map_err(status)?;
        let bytes = page.keys.iter().try_fold(4096, |size, key| wire::add(size, key.len()))?;
        let _copy = call.reserve(bytes)?;
        let keys = StringArray::from_iter_values(page.keys.iter().map(String::as_str));
        let rows = RecordBatch::try_from_iter([("_id", Arc::new(keys) as Arc<dyn Array>)]).map_err(arrow)?;
        let schema = rows.schema_ref().as_ref().clone().with_metadata(
            [(OWNED_IDENTITY_MORE.into(), u8::from(page.more).to_string())].into()
        );
        // The reply's valid coordinate is the system cut it enumerated.
        let reply = self.owned_point_reply(&schema, Some(&rows), system, system, None)?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_protected_coverage_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        use base64::{Engine, engine::general_purpose::STANDARD};
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        self.hosted_control_policy(call)?;
        let scope = self.hosted_control_scope(call).await?;
        let (handle, request) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Protected carrier coverage has no held handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = decode_batch(call, arrow_bytes)?;
        let batch = batch.batch();
        if batch.num_columns() != 1 || batch.num_rows() != 1 {
            return Err(invalid("Protected carrier coverage needs one prefix"));
        }
        let prefix = batch.column_by_name("prefix").and_then(|array| array.as_any().downcast_ref::<StringArray>())
            .filter(|column| !column.is_null(0)).ok_or_else(|| invalid("Protected carrier prefix must be Utf8"))?;
        let (coverage, witness) = held.snapshot.protected_carrier_coverage(&relation, prefix.value(0), scope)
            .await.map_err(status)?;
        let nonce = held.remember(call, &relation, witness)?;
        let schema = Schema::empty().with_metadata([
            ("rtdb.owned.protected_carrier_coverage".into(), STANDARD.encode(coverage)),
            (OWNED_RANGE_WITNESS.into(), nonce.simple().to_string()),
        ].into());
        let at = held.snapshot.present().system_time;
        let reply = self.owned_point_reply(&schema, None, at, at, None)?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_row_carrier_exists_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        self.hosted_control_policy(call)?;
        let scope = self.hosted_control_scope(call).await?;
        let (handle, request) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Protected row existence needs its held handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, id, _, _identity) = point_request(call, request, PointTime::Default)?;
        let present = held.snapshot.protected_row_carrier_exists(&relation, &id, scope).await.map_err(status)?;
        Ok(ActionResult { body: wire::retain(vec![u8::from(present)], call.reserve(64)?) })
    }

    pub(crate) async fn held_protected_carriers_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        self.hosted_control_policy(call)?;
        let scope = self.hosted_control_scope(call).await?;
        let (handle, request) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Protected carrier page has no held handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = decode_batch(call, arrow_bytes)?;
        let batch = batch.batch();
        if batch.num_columns() != 3 || batch.num_rows() != 1 {
            return Err(invalid("Protected carrier page requires a prefix, bound, and limit"));
        }
        let text = |name: &str| {
            batch.column_by_name(name).and_then(|array| array.as_any().downcast_ref::<StringArray>())
                .ok_or_else(|| invalid("Protected carrier key must be Utf8"))
        };
        let prefix = text("prefix")?;
        if prefix.is_null(0) { return Err(invalid("Protected carrier prefix is absent")); }
        let after = text("after")?;
        let after = (!after.is_null(0)).then(|| after.value(0));
        let limit = usize::try_from(history_request_integer(batch, "limit")?)
            .map_err(|_| invalid("Protected carrier page limit is out of range"))?;
        let (page, witness) = held.snapshot.protected_carrier_page(
            &relation, prefix.value(0), after, limit, scope,
        ).await.map_err(status)?;
        let bytes = page.keys.iter().try_fold(4096, |size, key| wire::add(size, key.len()))?;
        let _copy = call.reserve(bytes)?;
        let keys = StringArray::from_iter_values(page.keys.iter().map(String::as_str));
        let rows = RecordBatch::try_from_iter([("_id", Arc::new(keys) as Arc<dyn Array>)]).map_err(arrow)?;
        let mut metadata = std::collections::HashMap::from([
            (OWNED_IDENTITY_MORE.into(), u8::from(page.more).to_string()),
        ]);
        if let Some(witness) = witness {
            let nonce = held.remember(call, &relation, witness)?;
            metadata.insert(OWNED_RANGE_WITNESS.into(), nonce.simple().to_string());
        }
        let schema = rows.schema_ref().as_ref().clone().with_metadata(metadata);
        let at = held.snapshot.present().system_time;
        let reply = self.owned_point_reply(&schema, Some(&rows), at, at, None)?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    fn owned_versions_request(
        &self, call: &Call, request: &[u8],
    ) -> Result<(OwnedRelation, EntityId, OwnedVersionRequest, Arc<MemoryReservation>), Status> {
        use rtdb_types::Interval;

        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = request_batch(call, arrow_bytes)?;
        let batch = batch.batch();
        if batch.num_columns() != 7 || batch.num_rows() != 1 {
            return Err(invalid("Owned versions requires one row with identity, system endpoints, and pagination fields"));
        }
        let (id, identity) = point_identity(call, batch)?;
        let from = history_request_endpoint(batch, "system_from_micros")?;
        let to = history_request_endpoint(batch, "system_to_micros")?;
        let system = Interval::new(from, to).map_err(status)?;
        let after = u64::try_from(history_request_integer(batch, "after_revision")?)
            .map_err(|_| invalid("Owned version revision must be nonnegative"))?;
        let scan_limit = usize::try_from(history_request_integer(batch, "scan_limit")?)
            .map_err(|_| invalid("Owned version scan limit is out of range"))?;
        let result_limit = usize::try_from(history_request_integer(batch, "result_limit")?)
            .map_err(|_| invalid("Owned version result limit is out of range"))?;
        let (column, field) = batch.schema_ref().column_with_name("descending")
            .ok_or_else(|| invalid("Owned versions requires a descending flag"))?;
        let descending = batch.column(column).as_any().downcast_ref::<BooleanArray>()
            .filter(|values| !values.is_null(0) && field.metadata().is_empty())
            .ok_or_else(|| invalid("Owned versions descending must be a non-null Boolean"))?;
        let order = if descending.value(0) { OwnedVersionOrder::Descending } else { OwnedVersionOrder::Ascending };
        Ok((relation, id, OwnedVersionRequest { system, after, scan_limit, result_limit, order }, identity))
    }

    pub(crate) async fn held_world_parent_touch_images_action(
        &self, call: &Call, body: &[u8], images: bool,
    ) -> Result<Vec<Result<ActionResult, Status>>, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Earlier parent touch has no held handle"))?;
        let (selected, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Earlier parent touch has no selected publication"))?;
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let selected = Timestamp(i64::from_be_bytes(*selected));
        let (before, group, relation, path, id, valid, _identity) = self.path_touch_request(call, request)?;
        let [prior, accepted] = held.snapshot.parent_touch_images_at(
            &relation, &path, &id, selected, before, group, valid, images,
        ).await.map_err(status)?;
        let prior = self.world_point_reply(call, prior, valid, None)?;
        let accepted = self.world_point_reply(call, accepted, valid, None)?;
        if wire::add(prior.encoded_len(), accepted.encoded_len())? > self.0.config.maximum_message_bytes {
            return Err(Status::resource_exhausted("Earlier parent touch images exceed the Flight response limit"));
        }
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(vec![Ok(prior), Ok(accepted)])
    }

    pub(crate) async fn archived_world_changes_action(
        &self, call: &Call, body: &[u8], ancestor: bool,
    ) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, rest) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Earlier parent changes require a held world"))?;
        let (selected, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("Earlier parent changes require a selected publication"))?;
        let (after, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("Earlier parent changes require an exclusive revision"))?;
        let (through, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("Earlier parent changes require an upper revision"))?;
        let (limit, rest) = rest.split_first_chunk::<2>().ok_or_else(|| invalid("Earlier parent changes require a group limit"))?;
        let (direction, rest) = rest.split_first().ok_or_else(|| invalid("Earlier parent changes require a direction"))?;
        let reverse = match *direction {
            0 => false,
            1 => true,
            _ => return Err(invalid("Invalid earlier parent changes direction")),
        };
        let (relation, tail) = split_relation(rest)?;
        if !tail.is_empty() { return Err(invalid("Earlier parent changes have trailing bytes")); }
        let selected = Timestamp(i64::from_be_bytes(*selected));
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let after = Timestamp(i64::from_be_bytes(*after));
        let through = Timestamp(i64::from_be_bytes(*through));
        let limit = usize::from(u16::from_be_bytes(*limit));
        let (page, witness) = if ancestor {
            held.snapshot.ancestor_changes(&relation, selected, after, through, limit, reverse).await
        } else {
            held.snapshot.parent_changes_at(&relation, selected, after, through, limit, reverse).await
        }.map_err(status)?;
        let nonce = held.remember(call, witness, false)?;
        let reply = self.owned_merged_changes_reply(&page, selected, reverse, Some(nonce))?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_world_parent_versions_action(
        &self, call: &Call, body: &[u8],
    ) -> Result<Vec<ActionResult>, Status> {
        use rtdb_types::Interval;
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Earlier parent versions require a held world"))?;
        let (selected, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Earlier parent versions have no selected publication"))?;
        let selected = Timestamp(i64::from_be_bytes(*selected));
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let (relation, id, query, _identity) = self.owned_versions_request(call, request)?;
        let (page, witness) = held.snapshot.parent_versions_at(&relation, &id, selected, query).await.map_err(status)?;
        let _framing = call.reserve(wire::mul(page.versions.len() + 1, 512)?)?;
        let header_charge = call.reserve(64)?;
        let mut header = owned_version_header(page.next_revision, page.done, page.floor, page.scanned)?;
        let mut bytes = ActionResult { body: Bytes::from_static(&[0; 35]) }.encoded_len();
        let mut results = Vec::with_capacity(page.versions.len() + 1);
        for version in page.versions {
            let reply = self.history_reply(call, &id, HistoryRead::Archived(version.timeline.into_iter()),
                Interval::ALL, selected, Some((version.sequence, version.system))).await?;
            bytes = wire::add(bytes, reply.encoded_len())?;
            if bytes > self.0.config.maximum_message_bytes {
                return Err(Status::resource_exhausted("Earlier parent version page exceeds the Flight response limit"));
            }
            results.push(reply);
        }
        held.snapshot.check_relation(&relation).map_err(status)?;
        header.extend_from_slice(held.remember(call, witness, false)?.as_bytes());
        results.insert(0, ActionResult { body: wire::retain(header, header_charge) });
        Ok(results)
    }

    pub(crate) async fn held_owned_versions_action(&self, call: &Call, body: &[u8]) -> Result<Vec<ActionResult>, Status> {
        use rtdb_types::Interval;

        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned versions has no held handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Ok(entry) = self.take_owned_read(call, &handle) { entry.close(); }
            return Err(status(error));
        }
        let (relation, id, request, _identity) = self.owned_versions_request(call, request)?;
        let page = held.snapshot.versions(&relation, &id, request).await.map_err(status)?;
        let present = held.snapshot.system_time();
        let _framing = call.reserve(wire::mul(page.versions.len() + 1, 512)?)?;
        let header_charge = call.reserve(64)?;
        let mut header = owned_version_header(page.next_revision, page.done, page.floor, page.scanned)?;

        let mut bytes = 0;
        let mut results = Vec::with_capacity(page.versions.len() + 1);
        for version in page.versions {
            let reply = self.history_reply(call, &id, HistoryRead::Version(version.timeline), Interval::ALL, present,
                Some((version.revision, version.system_time))).await?;
            bytes = wire::add(bytes, reply.encoded_len())?;
            if bytes > self.0.config.maximum_message_bytes {
                return Err(Status::resource_exhausted("Owned version page exceeds the Flight response limit"));
            }
            results.push(reply);
        }
        held.snapshot.check_relation(&relation).map_err(status)?;
        let header_len = header.len() + if page.witness.is_some() { 16 } else { 0 };
        let framing = ActionResult { body: Bytes::from_static(&[0; 35]).slice(..header_len) };
        if wire::add(bytes, framing.encoded_len())? > self.0.config.maximum_message_bytes {
            return Err(Status::resource_exhausted("Owned version page exceeds the Flight response limit"));
        }
        if let Some(witness) = page.witness {
            let nonce = held.remember(call, &relation, witness)?;
            header.extend_from_slice(nonce.as_bytes());
        }
        results.insert(0, ActionResult { body: wire::retain(header, header_charge) });
        Ok(results)
    }

    async fn history_reply(
        &self, call: &Call, id: &EntityId, mut read: HistoryRead<'_>, valid: rtdb_types::Interval,
        present: Timestamp, version: Option<(u64, Timestamp)>,
    ) -> Result<ActionResult, Status> {
        let (from, to) = (valid.lower(), valid.upper());
        let as_of = version.map_or(present, |(_, system)| system);
        let mut pages = Vec::new();
        let mut rows = 0usize;
        let mut retained_bytes = 0usize;
        let mut last_end = from;
        let mut ordinals = Vec::new();
        let empty_schema;
        while let Some(page) = read.next_batch().await.map_err(status)? {
            let batch = page.batch();
            if batch.num_rows() == 0 { continue; }
            rows = wire::add(rows, batch.num_rows())?;
            retained_bytes = wire::add(retained_bytes, batch.get_array_memory_size())?;
            if rows > MAX_HISTORY_SLICES || retained_bytes > self.0.config.maximum_message_bytes {
                return Err(Status::resource_exhausted("Owned history exceeds its slice or response limit"));
            }
            if let Some(values) = batch.column_by_name("_assertion_ordinal") {
                let values = values.as_any().downcast_ref::<Int64Array>().ok_or_else(history_invalid)?;
                for value in values {
                    let ordinal = value.and_then(|ordinal| u32::try_from(ordinal).ok())
                        .ok_or_else(history_invalid)?;
                    ordinals.push(ordinal.to_string());
                }
            }
            for row in 0..batch.num_rows() {
                let start = history_row_time(batch, "_valid_from", row)?.ok_or_else(history_invalid)?;
                let end = history_row_time(batch, "_valid_to", row)?.unwrap_or(Timestamp::MAX);
                let revision = rtdb_query::owned_row_revision(batch, row, id, self.memory()).map_err(status)?;
                if start < last_end || start >= end || end > to || revision > as_of {
                    return Err(history_invalid());
                }
                last_end = end;
            }
            pages.push(page);
        }
        let schema = if let Some(page) = pages.first() {
            page.batch().schema()
        } else if let Some(schema) = read.runtime_schema() {
            empty_schema = wire::plan_schema(schema, self.memory())?;
            Arc::new((*empty_schema).clone())
        } else {
            Arc::new(Schema::empty())
        };
        let columns = schema.fields().iter().enumerate()
            .filter_map(|(index, field)| (field.name() != "_assertion_ordinal").then_some(index))
            .collect::<Vec<_>>();
        let metadata_bound = wire::add(wire::schema_bound(&schema)?, wire::mul(pages.len(), 512)?)?;
        let _metadata = call.reserve(wire::add(metadata_bound, wire::mul(ordinals.len(), 32)?)?)?;
        let mut metadata = schema.metadata().clone();
        metadata.insert(OWNED_PRESENT.into(), present.0.to_string());
        metadata.insert(OWNED_VALID_FROM.into(), from.0.to_string());
        metadata.insert(OWNED_VALID_TO.into(), to.0.to_string());
        if !ordinals.is_empty() {
            if ordinals.len() != rows { return Err(history_invalid()); }
            metadata.insert(OWNED_ASSERTION_ORDINALS.into(), ordinals.join(","));
        }
        if let Some((revision, time)) = version {
            metadata.insert(OWNED_VERSION_REVISION.into(), revision.to_string());
            metadata.insert(OWNED_VERSION_TIME.into(), time.0.to_string());
        }
        if let Some(nonce) = read.witness_nonce(call)? {
            metadata.insert(OWNED_RANGE_WITNESS.into(), nonce.simple().to_string());
        }
        let schema = Arc::new(schema.project(&columns).map_err(arrow)?.with_metadata(metadata));
        let response = pages.iter().map(|page| {
            let batch = page.batch();
            RecordBatch::try_new(Arc::clone(&schema), columns.iter().map(|&index| Arc::clone(batch.column(index))).collect()).map_err(arrow)
        }).collect::<Result<Vec<_>, _>>()?;
        self.owned_stream_reply(&schema, &response)
    }

    pub(crate) async fn held_owned_range_action(&self, call: &Call, body: &[u8], version: HeldRangeFormat) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let audience = call.session.audience_source().is_some();
        if audience && (!matches!(version, HeldRangeFormat::Full) || body.len() > 16 * 1024) {
            return Err(invalid("The audience page exceeds its carrier bounds"));
        }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned held range has no handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = if audience {
            self.retained_owned_read(call, &handle)?
        } else {
            self.owned_read(call, &handle)?
        };
        if let Err(error) = held.snapshot.check() {
            if let Ok(entry) = self.take_owned_read(call, &handle) { entry.close(); }
            return Err(status(error));
        }
        let (relation, arrow) = split_relation(request)?;
        let batch = decode_batch(call, arrow)?;
        let expected_fields = match version {
            HeldRangeFormat::Leading => 4,
            HeldRangeFormat::Full => 6,
            HeldRangeFormat::FixedValid => 7,
            HeldRangeFormat::Historical => 8,
        };
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != expected_fields {
            return Err(invalid("Owned range requires one row with the fields for its action version"));
        }
        let (mut rows, _decoded) = upload::domain_documents(batch.batch(), self)?;
        let row = rows.pop().expect("validated one row");
        let (Some(Value::Utf8(path)), Some(lower), Some(upper), Some(Value::I64(limit))) =
            (row.get("path"), row.get("lower"), row.get("upper"), row.get("limit"))
        else { return Err(invalid("Owned range fields have invalid types")); };
        let path = OwnedRelation::new(path.as_str()).map_err(status)?;
        let limit = usize::try_from(*limit).map_err(|_| invalid("Owned range limit is out of range"))?;
        let valid_at = if matches!(version, HeldRangeFormat::FixedValid | HeldRangeFormat::Historical) {
            let Some(Value::I64(valid_at)) = row.get("valid_micros") else {
                return Err(invalid("Owned fixed range requires a microsecond valid instant"));
            };
            let valid_at = Timestamp(*valid_at);
            valid_at.next().map_err(status)?;
            Some(valid_at)
        } else { None };
        let system = if matches!(version, HeldRangeFormat::Historical) {
            let Some(Value::I64(system)) = row.get("system_micros") else {
                return Err(invalid("Owned historical range requires a microsecond system cut"));
            };
            Some(Timestamp(*system))
        } else { None };
        let (page, evidence, expiry) = if !matches!(version, HeldRangeFormat::Leading) {
            let Some(Value::Bool(descending)) = row.get("descending") else {
                return Err(invalid("Owned range requires a boolean descending field"));
            };
            let request = OwnedRangeRequest {
                lower: compound_bound(Some(lower))?,
                upper: compound_bound(Some(upper))?,
                after: compound_bound(row.get("after"))?,
                descending: *descending,
                limit,
            };
            if audience {
                let projection = call.session.audience_projection()?;
                if relation != projection.relation || path != projection.address_path
                    || !(1..=5).contains(&request.limit) || request.descending
                    || request.lower.is_some() || request.upper.is_some()
                {
                    return Err(invalid("The audience page requires its bounded inline address order"));
                }
            }
            if let (Some(system), Some(valid)) = (system, valid_at) {
                held.snapshot.range_page_by_key_at_system(&relation, &path, request, system, valid)
                    .await.map(|page| (page, None, None))
            } else {
                held.snapshot.range_page_by_key_lifetime(&relation, &path, request, valid_at).await
            }
        } else {
            let lower = (!matches!(lower, Value::Null | Value::Undefined)).then_some(lower);
            let upper = (!matches!(upper, Value::Null | Value::Undefined)).then_some(upper);
            held.snapshot.range(&relation, &path, lower, upper, limit).await.map(|page| (page, None, None))
        }.map_err(status)?;
        let present = system.unwrap_or(held.snapshot.present().system_time);
        let observed = witnessed_schema(call, &held, &relation, &page.schema, evidence)?;
        let schema = observed.as_ref().map_or(page.schema.as_ref(), |(schema, _)| schema);
        let reply = self.owned_point_reply_until(schema, page.batch.as_ref().map(OwnedBatch::batch),
            present, valid_at.unwrap_or(present), None, expiry)?;
        if system.is_none() && !matches!(version, HeldRangeFormat::Leading) {
            held.snapshot.check_lifetime_range_release(&relation, expiry).map_err(status)?;
        } else {
            held.snapshot.check_relation(&relation).map_err(status)?;
        }
        Ok(reply)
    }

    pub(crate) async fn held_owned_count_action(&self, call: &Call, body: &[u8], historical: bool, measured: bool) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned held count has no handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Ok(entry) = self.take_owned_read(call, &handle) { entry.close(); }
            return Err(status(error));
        }
        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = decode_batch(call, arrow_bytes)?;
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != 4 + usize::from(historical) {
            return Err(invalid("Owned count requires one row with the fields for its action version"));
        }
        let (mut rows, _decoded) = upload::domain_documents(batch.batch(), self)?;
        let row = rows.pop().expect("validated one row");
        let (Some(Value::Utf8(path)), Some(lower), Some(upper), Some(Value::I64(valid))) =
            (row.get("path"), row.get("lower"), row.get("upper"), row.get("valid_micros"))
        else { return Err(invalid("Owned count fields have invalid types")); };
        let path = OwnedRelation::new(path.as_str()).map_err(status)?;
        let request = OwnedCountRequest {
            lower: compound_bound(Some(lower))?,
            upper: compound_bound(Some(upper))?,
            valid_at: Timestamp(*valid),
        };
        let (counted, evidence) = if historical {
            let Some(Value::I64(system)) = row.get("system_micros") else {
                return Err(invalid("Owned historical count requires a microsecond system cut"));
            };
            let counted = if measured {
                held.snapshot.measure_range_at_system(&relation, &path, request, Timestamp(*system)).await
            } else {
                held.snapshot.count_range_at_system(&relation, &path, request, Timestamp(*system)).await
            };
            counted.map(|counted| (counted, None))
        } else if measured {
            held.snapshot.measure_range_observed(&relation, &path, request).await
        } else {
            held.snapshot.count_range_observed(&relation, &path, request).await
        }.map_err(status)?;
        let batch = if measured {
            measure_batch(&counted)?
        } else {
            count_batch(counted.count)?
        };
        let schema = batch.schema();
        let observed = witnessed_schema(call, &held, &relation, &schema, evidence)?;
        let output_schema = observed.as_ref().map_or(schema.as_ref(), |(schema, _)| schema);
        let reply = self.owned_point_reply_until(output_schema, Some(&batch),
            counted.present, counted.valid_at, None, counted.expiry)?;
        if historical {
            held.snapshot.check_relation(&relation).map_err(status)?;
        } else {
            held.snapshot.check_lifetime_range_release(&relation, counted.expiry).map_err(status)?;
        }
        Ok(reply)
    }

    pub(crate) async fn held_world_ordered_action(&self, call: &Call, body: &[u8], order: WorldOrder) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Missing held world range handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = if !matches!(order, WorldOrder::ParentMeasure) {
            self.owned_world_resource(call, &handle)?
        } else {
            self.owned_world_read(call, &handle)?
        };
        let (system, request) = if matches!(order, WorldOrder::OverlayRangeAt | WorldOrder::ParentRangeAt | WorldOrder::ParentCountAt) {
            let (system, rest) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Historical world range has no revision"))?;
            (Some(Timestamp(i64::from_be_bytes(*system))), rest)
        } else { (None, request) };
        let (relation, arrow) = split_relation(request)?;
        let batch = decode_batch(call, arrow)?;
        let count = matches!(order, WorldOrder::ParentCount | WorldOrder::ParentCountAt | WorldOrder::ParentMeasure);
        let fields = if count { 4 } else { 7 };
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != fields {
            return Err(invalid("Invalid held world range arguments"));
        }
        let (mut rows, _decoded) = upload::domain_documents(batch.batch(), self)?;
        let row = rows.pop().expect("validated one row");
        let (Some(Value::Utf8(path)), Some(lower), Some(upper), Some(Value::I64(valid_at))) =
            (row.get("path"), row.get("lower"), row.get("upper"), row.get("valid_micros"))
        else { return Err(invalid("Invalid held world range fields")); };
        let path = OwnedRelation::new(path.as_str()).map_err(status)?;
        let valid_at = Timestamp(*valid_at);
        let lower = compound_bound(Some(lower))?;
        let upper = compound_bound(Some(upper))?;
        let (schema, page, witness, count_rows) = if count {
            let request = OwnedCountRequest { lower, upper, valid_at };
            let (rows, witness) = if let Some(system) = system {
                let (count, witness) = held.snapshot.parent_count_at(relation.clone(), path, request, system).await.map_err(status)?;
                (count_batch(count)?, witness)
            } else if matches!(order, WorldOrder::ParentMeasure) {
                let (measured, witness) = held.snapshot.measure(relation.clone(), path, request).await.map_err(status)?;
                (measure_batch(&measured)?, witness)
            } else {
                let (count, witness) = held.snapshot.count(relation.clone(), path, request).await.map_err(status)?;
                (count_batch(count)?, witness)
            };
            (rows.schema(), None, Some(witness), Some(rows))
        } else {
            let (Some(after), Some(Value::Bool(descending)), Some(Value::I64(limit))) =
                (row.get("after"), row.get("descending"), row.get("limit"))
            else { return Err(invalid("Invalid held world range page")); };
            let request = OwnedRangeRequest {
                lower, upper, after: compound_bound(Some(after))?, descending: *descending,
                limit: usize::try_from(*limit).map_err(|_| invalid("Invalid held world range limit"))?,
            };
            let (page, witness) = if let Some(system) = system {
                if matches!(order, WorldOrder::ParentRangeAt) {
                    let (page, witness) = held.snapshot.parent_range_at(relation.clone(), path, request, valid_at, system).await.map_err(status)?;
                    (page, Some(witness))
                } else {
                    (held.snapshot.overlay_range_at(relation.clone(), path, request, valid_at, system).await.map_err(status)?, None)
                }
            } else {
                let (page, witness) = if matches!(order, WorldOrder::OverlayRange) {
                    held.snapshot.overlay_range(relation.clone(), path, request, valid_at).await
                } else {
                    held.snapshot.range(relation.clone(), path, request, valid_at).await
                }.map_err(status)?;
                (page, Some(witness))
            };
            (page.schema, page.batch, witness, None)
        };
        let _charge = call.reserve(wire::add(wire::schema_bound(&schema)?, 256)?)?;
        let mut metadata = schema.metadata().clone();
        if let Some(witness) = witness {
            let nonce = held.remember(call, witness, false)?;
            metadata.insert(WORLD_RANGE_WITNESS.into(), nonce.simple().to_string());
        }
        let schema = schema.as_ref().clone().with_metadata(metadata);
        let rows = count_rows.as_ref().or_else(|| page.as_ref().map(OwnedBatch::batch));
        let reply = self.owned_point_reply(&schema, rows, system.unwrap_or_else(|| held.snapshot.present()), valid_at, None)?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn spatial_path_readiness_action(&self, call: &Call, body: &Bytes) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Spatial path readiness has no held handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        let request = decode_retained_batch(call, body.slice_ref(request))?;
        let request = request.batch();
        if request.num_rows() != 1 || request.num_columns() != 2 {
            return Err(invalid("Spatial path readiness requires one row"));
        }
        let name = |field| ArrowCell::named(request, field, 0).and_then(ArrowCell::utf8)
            .ok_or_else(|| invalid("Spatial path readiness requires a text relation and name"));
        let relation = OwnedRelation::new(name("relation")?).map_err(status)?;
        let path = OwnedRelation::new(name("name")?).map_err(status)?;
        let ready = held.snapshot.spatial_path_ready(&relation, &path).await.map_err(status)?;
        let batch = RecordBatch::try_from_iter([("ready", Arc::new(BooleanArray::from(vec![ready])) as ArrayRef)])
            .map_err(arrow)?;
        let present = held.snapshot.system_time();
        self.owned_point_reply(batch.schema_ref(), Some(&batch), present, present, None)
    }

    pub(crate) async fn spatial_held_owned_action(&self, call: &Call, body: &Bytes, wire: SpatialHeldWire) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned held spatial read has no handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        let (relation, request) = split_relation(request)?;
        let (path, request) = split_relation(request)?;
        let batch = decode_retained_batch(call, body.slice_ref(request))?;
        let selected_documents = matches!(wire, SpatialHeldWire::V3 | SpatialHeldWire::Open | SpatialHeldWire::RankOpen);
        let expected_columns = 9 + usize::from(selected_documents);
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != expected_columns {
            return Err(invalid("Owned held spatial read requires one request row"));
        }
        let input = batch.batch();
        let document_columns;
        let projection = match wire {
            SpatialHeldWire::V1 => rtdb_spatial::Projection::Identities,
            SpatialHeldWire::V2 => rtdb_spatial::Projection::Documents,
            SpatialHeldWire::V3 | SpatialHeldWire::Open | SpatialHeldWire::RankOpen => {
                document_columns = spatial_document_columns(input)?;
                rtdb_spatial::Projection::SelectedDocuments(&document_columns)
            }
        };
        let (Some((region, region_row)), Some((predicate, predicate_row)),
             Some((valid_at, valid_row)), Some((limit, limit_row)), Some((observe, observe_row)),
             Some((reported_center, center_row))) = (
            owned_scalar::<BinaryArray>(input, "region")?, owned_scalar::<StringArray>(input, "predicate")?,
            owned_scalar::<Int64Array>(input, "valid_micros")?, owned_scalar::<Int64Array>(input, "limit")?,
            owned_scalar::<BooleanArray>(input, "observe")?, owned_scalar::<BooleanArray>(input, "reported_center")?,
        ) else { return Err(invalid("Owned held spatial request has invalid fields")); };
        let region = region.value(region_row);
        if region.len() > 1 << 20 { return Err(invalid("Owned held spatial region exceeds its byte limit")); }
        let partition = spatial_partition(input)?;
        let radius = owned_scalar::<Float64Array>(input, "radius_m")?.map(|(array, row)| array.value(row));
        let predicate = rtdb_spatial::Predicate::parse(predicate.value(predicate_row), radius, reported_center.value(center_row)).map_err(status)?;
        let after = match input.schema_ref().column_with_name("after") {
            Some((index, field)) => selected_cell(field, input.column(index).as_ref(), 0).map_err(status)?
                .map(|(field, array, row)| read_entity_id(field, array, row).map_err(status)).transpose()?,
            None => None,
        };
        use rtdb_spatial::{PageOrder, Predicate};
        let page = if matches!(predicate, Predicate::Nearest { .. }) {
            if after.is_some() { return Err(invalid("Nearest continuation requires a held ranking")); }
            PageOrder::NearestOnce
        } else {
            PageOrder::Identity { after }
        };
        let valid_at = Timestamp(valid_at.value(valid_row));
        let limit = usize::try_from(limit.value(limit_row)).map_err(|_| invalid("Invalid spatial limit"))?;
        let observe = observe.value(observe_row);
        let request = rtdb_spatial::Request { partition, region: region.into(), predicate, valid_at, page, limit };
        let (page, witness, prepared) = if matches!(wire, SpatialHeldWire::RankOpen) {
            let (ranking, page, witness) = held.snapshot.open_spatial_nearest(&relation, &path, request, observe, projection)
                .await.map_err(status)?;
            (page, witness, Some(RetainedSpatialQuery::Nearest(ranking)))
        } else {
            let prepared = if matches!(wire, SpatialHeldWire::Open) {
                held.snapshot.check_relation(&relation).map_err(status)?;
                Some(rtdb_spatial::PreparedRegion::new(region, predicate, call.memory()).map_err(status)?)
            } else {
                None
            };
            let (page, witness) = held.snapshot.spatial_page(&relation, &path, request, observe, projection, prepared.as_ref())
                .await.map_err(status)?;
            (page, witness, prepared.map(|region| RetainedSpatialQuery::Range { path, region }))
        };
        let mut retained = None;
        let mut token = None;
        if prepared.is_some() && page.more {
            let guard = held.spatial_filters.lock().map_err(|_| spatial_filters_unavailable())?;
            if guard.len() >= MAX_SPATIAL_FILTERS { return Err(Status::resource_exhausted("The held read has too many spatial filters")); }
            let nonce = Uuid::new_v4();
            if guard.contains_key(&nonce) { return Err(spatial_filters_unavailable()); }
            retained = Some((guard, call.reserve(1024)?));
            token = Some(nonce);
        }
        let reply = self.spatial_reply(call, &held, &relation, valid_at, page, witness, selected_documents, token)?;
        if let (Some((mut filters, reservation)), Some(token), Some(query)) = (retained, token, prepared) {
            filters.insert(token, Arc::new(RetainedSpatialFilter {
                relation, batch, query, valid_at, _reservation: reservation,
            }));
        }
        Ok(reply)
    }

    pub(crate) async fn next_spatial_owned_action(&self, call: &Call, body: &[u8], ranked: bool) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (held, token, body) = self.spatial_capability(call, body)?;
        let (header, after) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Spatial continuation has no page controls"))?;
        let limit = usize::from(header[0]);
        let observe = match header[1] { 0 => false, 1 => true, _ => return Err(invalid("Invalid spatial continuation observation flag")) };
        let (distance, after) = if ranked {
            let (distance, after) = after.split_first_chunk::<8>()
                .ok_or_else(|| invalid("Ranked spatial continuation has no distance"))?;
            (Some(f64::from_be_bytes(*distance)), after)
        } else {
            (None, after)
        };
        let after = EntityIdRef::from_handle_bytes(after).map_err(status)?;
        let filter = held.spatial_filters.lock().map_err(|_| spatial_filters_unavailable())?
            .get(&token).cloned().ok_or_else(|| Status::not_found("Unknown held spatial filter"))?;
        let columns = spatial_document_columns(filter.batch.batch())?;
        let projection = rtdb_spatial::Projection::SelectedDocuments(&columns);
        let (page, witness) = match (&filter.query, distance) {
            (RetainedSpatialQuery::Range { path, region }, None) => {
                let request = rtdb_spatial::Request {
                    partition: spatial_partition(filter.batch.batch())?,
                    region: Default::default(), predicate: rtdb_spatial::Predicate::All,
                    valid_at: filter.valid_at, page: rtdb_spatial::PageOrder::Identity { after: Some(after) }, limit,
                };
                held.snapshot.spatial_page(&filter.relation, path, request, observe, projection, Some(region)).await
            }
            (RetainedSpatialQuery::Nearest(ranking), Some(distance_m)) => {
                held.snapshot.next_spatial_nearest(ranking, rtdb_spatial::NearestPosition { distance_m, id: after },
                    limit, observe, projection).await
            }
            _ => return Err(invalid("Spatial continuation uses another query order")),
        }.map_err(status)?;
        self.spatial_reply(call, &held, &filter.relation, filter.valid_at, page, witness, true, Some(token))
    }

    pub(crate) fn close_spatial_owned_action(&self, call: &Call, body: &[u8]) -> Result<(), Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (held, token, tail) = self.spatial_capability(call, body)?;
        if !tail.is_empty() { return Err(invalid("Spatial filter close has trailing bytes")); }
        held.spatial_filters.lock().map_err(|_| spatial_filters_unavailable())?.remove(&token);
        Ok(())
    }

    fn spatial_capability<'a>(&self, call: &Call, body: &'a [u8]) -> Result<(Arc<OwnedReadResource>, Uuid, &'a [u8]), Status> {
        let (header, tail) = body.split_first_chunk::<32>().ok_or_else(|| invalid("Spatial continuation has no held filter capability"))?;
        let (handle, token) = header.split_at(16);
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        held.snapshot.check().map_err(status)?;
        let token = Uuid::from_slice(token).map_err(|_| invalid("Invalid held spatial filter capability"))?;
        Ok((held, token, tail))
    }

    fn spatial_reply(
        &self, call: &Call, held: &OwnedReadResource, relation: &OwnedRelation, valid_at: Timestamp,
        page: rtdb_spatial::Page, witness: Option<OwnedRangeWitness>, selected_documents: bool, token: Option<Uuid>,
    ) -> Result<ActionResult, Status> {
        let present = held.snapshot.system_time();
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("rtdb.owned.spatial_more".into(), if page.more { "1" } else { "0" }.into());
        if let Some(token) = token.filter(|_| page.more) {
            metadata.insert(SPATIAL_FILTER_TOKEN.into(), token.simple().to_string());
        }
        if let Some(distance) = page.next_distance_m {
            metadata.insert("rtdb.owned.spatial_next_distance_m".into(), distance.to_string());
        }
        if let Some(witness) = witness {
            let nonce = held.remember(call, relation, witness)?;
            metadata.insert(OWNED_RANGE_WITNESS.into(), nonce.simple().to_string());
        }
        let documents = page.documents();
        let document = documents.as_ref().map(|documents| {
            let mut batch = documents.batch().clone();
            if selected_documents { batch.remove_column(0); }
            StructArray::from(batch)
        });
        let mut fields = vec![
            rtdb_columnar::to_field("_id", &entity_id_type()).map_err(status)?,
            Field::new("distance_m", DataType::Float64, true),
        ];
        if let Some(document) = &document {
            fields.push(Field::new("document", DataType::Struct(document.fields().clone()), false));
        }
        let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
        let mut charge = None;
        let output = if let Some(rows) = page.rows().filter(|rows| rows.batch().num_rows() != 0) {
            let batch = rows.batch();
            let (source, target) = (batch.schema_ref().field(0), schema.field(0));
            let array = batch.column(0);
            let bound = rtdb_columnar::arrow_relayout_bound(array.as_ref(), source, target).map_err(status)?;
            charge = Some(call.reserve(wire::add(bound, 4096)?)?);
            let ids = match rtdb_columnar::relayout_arrow_column(Arc::clone(array), source, target) {
                Ok((_, ids)) if ids.data_type() == target.data_type() => ids,
                _ => {
                    let ids = (0..array.len()).map(|row| read_entity_id(source, array.as_ref(), row)).collect::<rtdb_types::Result<Vec<_>>>().map_err(status)?;
                    let bound = ids.iter().try_fold(4096, |bytes, id| {
                        let text = match id {
                            EntityIdRef::String(text) | EntityIdRef::Keyword(text) => text.len(),
                            _ => 0,
                        };
                        wire::add(bytes, wire::add(128, wire::mul(text, 4)?)?)
                    })?;
                    charge = Some(call.reserve(bound)?);
                    entity_id_array(ids).map_err(status)?
                }
            };
            let distances = Arc::new(page.distance_column());
            let mut columns = vec![ids, distances];
            if let Some(document) = document {
                columns.push(Arc::new(document));
            }
            Some(RecordBatch::try_new(Arc::clone(&schema), columns).map_err(arrow)?)
        } else { None };
        let reply = self.owned_point_reply(&schema, output.as_ref(), present, valid_at, None)?;
        drop(charge);
        held.snapshot.check_relations([relation]).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn search_held_owned_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned held search has no handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Ok(entry) = self.take_owned_read(call, &handle) { entry.close(); }
            return Err(status(error));
        }
        let (valid, request) = request.split_first_chunk::<8>()
            .ok_or_else(|| invalid("Held search has no valid instant"))?;
        let valid = i64::from_be_bytes(*valid);
        let valid = (valid != i64::MIN).then_some(Timestamp(valid));
        if let Some(valid) = valid { rtdb_types::Interval::at(valid).map_err(status)?; }
        let (length, request) = request.split_first_chunk::<4>()
            .ok_or_else(|| invalid("Held search has no overlay length"))?;
        let (ipc, request) = request.split_at_checked(u32::from_be_bytes(*length) as usize)
            .ok_or_else(|| invalid("Held search has a truncated overlay"))?;
        let (overlay, staged_scope) = if ipc.is_empty() {
            (None, None)
        } else {
            if valid.is_some() { return Err(invalid("A staged search requires current validity")); }
            self.hosted_control_policy(call)?;
            let scope = self.hosted_control_scope(call).await?;
            (Some(request_batch(call, ipc)?), Some(scope))
        };
        let prefix_len = body.len() - request.len();
        let rank = owned_rank_request(call, request)?;
        let overlay = overlay.as_ref().map(|input| self.search_shadows(&rank, input.batch())).transpose()?;
        let (page, witnesses) = held.snapshot.ranked_search_with_overlay(
            &rank.partitions, rank.query, rank.offset, rank.limit, rank.observe, valid, overlay,
        ).await.map_err(status)?;
        if let Some(scope) = staged_scope {
            if witnesses.len() != rank.partitions.len()
                || witnesses.iter().any(|witness| !witness.matches_scope(Some(scope)))
            {
                return Err(Status::failed_precondition("Staged search did not retain its original placed corpus"));
            }
        }
        let mut nonces = Vec::new();
        if !witnesses.is_empty() {
            if witnesses.len() != rank.partitions.len() { return Err(Status::internal("Incomplete owned search evidence")); }
            for ((relation, _), witness) in rank.partitions.iter().zip(witnesses) {
                nonces.push(held.remember(call, relation, witness)?);
            }
        }
        let present = held.snapshot.present().system_time;
        let scope = owned_rank_scope(b"rtdb.owned.search.held.v5\0", &body[..prefix_len + rank.frame_len], rank.query);
        let reply = self.owned_rank_reply(call, page, scope, nonces, present, valid.unwrap_or(present))?;
        held.snapshot.check_relations(rank.partitions.iter().map(|(relation, _)| relation)).map_err(status)?;
        Ok(reply)
    }

    fn search_shadows(&self, rank: &OwnedRankRequest<'_>, input: &RecordBatch) -> Result<rtdb_node::HeldTextCorpus, Status> {
        if !rank.observe || !(1..=64).contains(&input.num_rows()) || input.num_columns() != 3 {
            return Err(invalid("Staged search requires bounded rows and original evidence"));
        }
        let partitions = input.column_by_name("partition").and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
        let ids = input.column_by_name("_id").and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let text = input.column_by_name("text").and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let (Some(partitions), Some(ids), Some(text)) = (partitions, ids, text) else {
            return Err(invalid("Invalid staged search overlay layout"));
        };
        let mut corpus = rtdb_node::HeldTextCorpus::new(Arc::clone(self.memory()), rank.query).map_err(status)?;
        for row in 0..input.num_rows() {
            if partitions.is_null(row) || ids.is_null(row) || ids.value(row).is_empty()
                || partitions.value(row) >= rank.partitions.len() as u64
            {
                return Err(invalid("Invalid staged search identity"));
            }
            corpus.shadow(
                partitions.value(row) as usize,
                rtdb_types::EntityId::String(ids.value(row).to_owned()),
                (!text.is_null(row)).then(|| text.value(row)),
            ).map_err(status)?;
        }
        Ok(corpus)
    }

    pub(crate) async fn search_held_world_action(&self, call: &Call, body: &[u8], mode: WorldHistoricalRead) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Missing held world search handle"))?;
        let (earlier, request) = if matches!(mode, WorldHistoricalRead::EarlierEffective) {
            let (mode, rest) = request.split_first().ok_or_else(|| invalid("Missing historical world search selector"))?;
            let parent_only = match *mode {
                0 => true,
                1 => false,
                _ => return Err(invalid("Invalid historical world search selector")),
            };
            let (system, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("Missing historical world search system revision"))?;
            (Some((Timestamp(i64::from_be_bytes(*system)), parent_only)), rest)
        } else { (None, request) };
        let (parent_at, request) = if matches!(mode, WorldHistoricalRead::EarlierParent) {
            let (system, rest) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Missing earlier parent search revision"))?;
            (Some(Timestamp(i64::from_be_bytes(*system))), rest)
        } else { (None, request) };
        let (valid, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Missing held world search valid instant"))?;
        let valid = Timestamp(i64::from_be_bytes(*valid));
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_world_resource(call, &handle)?;
        let (overlay, staged_scope, request) = if matches!(mode, WorldHistoricalRead::Current) {
            let (length, request) = request.split_first_chunk::<4>()
                .ok_or_else(|| invalid("Held world search has no overlay length"))?;
            let (ipc, request) = request.split_at_checked(u32::from_be_bytes(*length) as usize)
                .ok_or_else(|| invalid("Held world search has a truncated overlay"))?;
            if ipc.is_empty() {
                (None, None, request)
            } else {
                if held.snapshot.clock_mode() != rtdb_types::WorldClockMode::Proposal
                    || valid != held.snapshot.present()
                {
                    return Err(invalid("Staged world search requires live Proposal validity"));
                }
                self.hosted_control_policy(call)?;
                let scope = self.hosted_control_scope(call).await?;
                (Some(request_batch(call, ipc)?), Some(scope), request)
            }
        } else {
            (None, None, request)
        };
        let prefix_len = body.len() - request.len();
        let rank = owned_rank_request(call, request)?;
        if !rank.observe {
            return Err(Status::failed_precondition("Search requires evidence for every selected world partition"));
        }
        let relations = rank.partitions.iter().map(|(relation, _)| relation.clone()).collect::<Vec<_>>();
        let domain = match mode {
            WorldHistoricalRead::Current => b"rtdb.owned.search.world.v2\0".as_slice(),
            WorldHistoricalRead::EarlierEffective => b"rtdb.owned.search.world-at.v1\0".as_slice(),
            WorldHistoricalRead::EarlierParent => b"rtdb.owned.search.world-parent-at.v1\0".as_slice(),
        };
        let scope = owned_rank_scope(domain, &body[..prefix_len + rank.frame_len], rank.query);
        let (page, witnesses) = if let Some(system) = parent_at {
            held.snapshot.ranked_search_parent_at(rank.partitions, rank.query.to_owned(), rank.offset, rank.limit, valid, system).await
        } else if let Some((system, parent_only)) = earlier {
            held.snapshot.ranked_search_at(rank.partitions, rank.query.to_owned(), rank.offset, rank.limit, valid, system, parent_only).await
        } else {
            let overlay = overlay.as_ref().map(|input| self.search_shadows(&rank, input.batch())).transpose()?;
            held.snapshot.ranked_search(rank.partitions, rank.query.to_owned(), rank.offset, rank.limit, valid, overlay).await
        }.map_err(status)?;
        if witnesses.len() != relations.len() { return Err(Status::internal("Incomplete retained search evidence")); }
        if let Some(scope) = staged_scope {
            if witnesses.iter().any(|witness| !witness.matches_scope(scope)) {
                return Err(Status::failed_precondition("Staged search did not retain its original placed corpus"));
            }
        }
        let nonces = witnesses.into_iter().map(|witness| held.remember(call, witness, false)).collect::<Result<Vec<_>, _>>()?;
        let reply = self.owned_rank_reply(call, page, scope, nonces, parent_at.or_else(|| earlier.map(|(system, _)| system)).unwrap_or_else(|| held.snapshot.present()), valid)?;
        for relation in &relations { held.snapshot.check_relation(relation).map_err(status)?; }
        Ok(reply)
    }

    fn owned_rank_reply(
        &self, call: &Call, page: rtdb_node::OwnedSearchPage, scope: String, witnesses: Vec<Uuid>,
        present: Timestamp, valid: Timestamp,
    ) -> Result<ActionResult, Status> {
        let hits = page.hits;
        let id_type = LogicalType::from_legs([ScalarType::Utf8, ScalarType::Keyword, ScalarType::I64, ScalarType::Uuid].map(LogicalType::Scalar));
        let mut metadata = std::collections::HashMap::from([
            (OWNED_SEARCH_TOTAL.into(), page.total.to_string()),
            (OWNED_SEARCH_SCOPE.into(), scope),
        ]);
        if !witnesses.is_empty() {
            let mut nonces = String::with_capacity(32 * witnesses.len());
            for witness in witnesses { nonces.push_str(&witness.simple().to_string()); }
            metadata.insert(OWNED_SEARCH_WITNESSES.into(), nonces);
        }
        let schema = Arc::new(Schema::new_with_metadata(vec![
            rtdb_columnar::to_field("_id", &id_type).map_err(status)?,
            Field::new("score", DataType::Float64, false),
            Field::new("partition", DataType::UInt64, false),
        ], metadata));
        let mut charge = call.reserve(4096)?;
        let output = if hits.is_empty() {
            None
        } else {
            let mut ids = Vec::with_capacity(hits.len());
            for hit in &hits {
                let id = hit.id.as_value();
                let reservation = Arc::get_mut(&mut charge).expect("search output owns its reservation");
                reservation.grow_to(wire::add(reservation.bytes(), wire::mul(id.retained_bytes().map_err(status)?, 4)?)?).map_err(status)?;
                ids.push(id);
            }
            let ids = rtdb_columnar::build_array(&id_type, &ids).map_err(status)?;
            let scores = Arc::new(Float64Array::from_iter_values(hits.iter().map(|hit| hit.score)));
            let ordinals = Arc::new(UInt64Array::from_iter_values(hits.iter().map(|hit| hit.partition as u64)));
            Some(RecordBatch::try_new(Arc::clone(&schema), vec![ids, scores, ordinals]).map_err(arrow)?)
        };
        self.owned_point_reply(&schema, output.as_ref(), present, valid, None)
    }

    pub(crate) async fn held_expiry_candidates_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        self.hosted_control_policy(call)?;
        let scope = self.hosted_control_scope(call).await?;
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Expiry candidates need the original held handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, page) = split_relation(request)?;
        let (after, rest) = page.split_first_chunk::<8>().ok_or_else(|| invalid("Expiry candidates need an exclusive journal position"))?;
        let groups: &[u8; 2] = rest.try_into().map_err(|_| invalid("Expiry candidates need one group limit"))?;
        let page = held.snapshot.expired_candidate_page(
            &relation, Timestamp(i64::from_be_bytes(*after)), usize::from(u16::from_be_bytes(*groups)), scope,
        ).await.map_err(status)?;
        let _charge = call.reserve(4096 + page.candidates.iter().map(|row| row.identity.len() + row.nomination.len() + 64).sum::<usize>())?;
        let fields = [
            ("identity", Arc::new(BinaryArray::from_iter_values(page.candidates.iter().map(|row| row.identity.as_slice()))) as ArrayRef),
            ("nomination", Arc::new(StringArray::from_iter_values(page.candidates.iter().map(|row| row.nomination.as_str()))) as ArrayRef),
            ("created_micros", Arc::new(Int64Array::from_iter_values(page.candidates.iter().map(|row| row.created.0))) as ArrayRef),
            ("expiry_micros", Arc::new(Int64Array::from_iter_values(page.candidates.iter().map(|row| row.expiry.0))) as ArrayRef),
        ];
        let batch = RecordBatch::try_from_iter(fields).map_err(arrow)?;
        let mut metadata = batch.schema().metadata().clone();
        metadata.insert("rtdb.owned.expiry_candidate_through".into(), page.through.0.to_string());
        metadata.insert("rtdb.owned.expiry_diagnostic_cutoff".into(), page.diagnostic_cutoff.0.to_string());
        metadata.insert("rtdb.owned.expiry_candidate_state".into(), page.state.into());
        if let Some(at) = page.next_eligible {
            metadata.insert("rtdb.owned.expiry_next_eligible_micros".into(), at.0.to_string());
        }
        let schema = batch.schema().as_ref().clone().with_metadata(metadata);
        let present = held.snapshot.present().system_time;
        let reply = self.owned_point_reply(&schema, Some(&batch), present, present, None)?;
        held.snapshot.check_live_change_relations(std::slice::from_ref(&relation)).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn nominate_held_expiry_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        self.hosted_control_policy(call)?;
        let scope = self.hosted_control_scope(call).await?;
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Expiry nomination needs the original held handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, id, _, _identity) = point_request(call, request, PointTime::Default)?;
        let (nomination, created, expiry) = held.snapshot.nominate_expired_identity(&relation, &id, scope).await.map_err(status)?;
        let batch = RecordBatch::try_from_iter([
            ("nomination", Arc::new(StringArray::from(vec![nomination])) as ArrayRef),
            ("created_micros", Arc::new(Int64Array::from(vec![created.0])) as ArrayRef),
            ("expiry_micros", Arc::new(Int64Array::from(vec![expiry.0])) as ArrayRef),
        ]).map_err(arrow)?;
        let present = held.snapshot.present().system_time;
        let reply = self.owned_point_reply(batch.schema_ref(), Some(&batch), present, present, None)?;
        held.snapshot.check_live_point_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn apply_held_expiry_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        self.hosted_control_policy(call)?;
        let (handle, rest) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Expiry apply needs the original held handle"))?;
        let (deadline, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("Expiry apply needs a deadline"))?;
        let (length, rest) = rest.split_first_chunk::<2>().ok_or_else(|| invalid("Expiry apply needs a nomination length"))?;
        let length = usize::from(u16::from_be_bytes(*length));
        if !(1..=208).contains(&length) { return Err(invalid("Expiry nomination exceeds its bound")); }
        let (nomination, request) = rest.split_at_checked(length).ok_or_else(|| invalid("Expiry apply nomination is truncated"))?;
        let nomination = std::str::from_utf8(nomination).map_err(|_| invalid("Expiry nomination is not UTF-8"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, id, _, _identity) = point_request(call, request, PointTime::Default)?;
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        require_idle(&session)?;
        let committed = session.sweep_owned_expired(
            &held.snapshot, relation, id, nomination, Timestamp(i64::from_be_bytes(*deadline)),
        ).await.map_err(status)?;
        if let Some(error) = committed.error() { return Err(status(error.clone())); }
        if !committed.committed() { return Err(Status::aborted("Owned expiry did not commit")); }
        self.owned_tx_reply(committed.tx_key())
    }

    pub(crate) async fn held_owned_changes_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned change read has no held handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Ok(entry) = self.take_owned_read(call, &handle) { entry.close(); }
            return Err(status(error));
        }
        let (relation, cursor) = split_relation(request)?;
        let (after, rest) = cursor.split_first_chunk::<8>().ok_or_else(|| invalid("Owned change read has no system time"))?;
        let (limit, tail) = rest.split_first_chunk::<2>().ok_or_else(|| invalid("Owned change read has no group limit"))?;
        let after = Timestamp(i64::from_be_bytes(*after));
        let limit = usize::from(u16::from_be_bytes(*limit));
        let page = if tail.is_empty() {
            held.snapshot.changes(&relation, after, limit).await
        } else {
            let (lower, upper, valid, _bounds) = change_region_request(call, tail)?;
            held.snapshot.changes_in_region(&relation, after, limit, lower.as_ref(), upper.as_ref(), valid).await
        }.map_err(status)?;
        let reply = self.owned_changes_reply(&page, held.snapshot.present().system_time)?;
        held.snapshot.check_live_change_relations(std::slice::from_ref(&relation)).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_owned_storage_usage_action(
        &self, call: &Call, body: &[u8],
    ) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned storage usage requires a held handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (count, mut request) = request.split_first_chunk::<2>().ok_or_else(|| invalid("Owned storage usage requires a relation count"))?;
        let count = usize::from(u16::from_be_bytes(*count));
        if !(1..=32).contains(&count) { return Err(invalid("Owned storage usage requires one to 32 relations")); }
        let _charge = call.reserve(count * 1024)?;
        let mut relations = Vec::with_capacity(count);
        for _ in 0..count {
            let (relation, tail) = split_relation(request)?;
            relations.push(relation);
            request = tail;
        }
        if !request.is_empty() { return Err(invalid("Owned storage usage has trailing bytes")); }
        let usage = held.snapshot.storage_usage(&relations).await.map_err(status)?;
        let schema = Arc::new(Schema::new(vec![
            Field::new("relation", DataType::Utf8, false),
            Field::new("current_data_bytes", DataType::Int64, false),
            Field::new("history_data_bytes", DataType::Int64, false),
            Field::new("current_search_bytes", DataType::Int64, true),
            Field::new("history_search_bytes", DataType::Int64, true),
            Field::new("ordered_index_bytes", DataType::Int64, true),
        ]).with_metadata(std::collections::HashMap::from([
            (OWNED_PRESENT.into(), held.snapshot.present().system_time.0.to_string()),
        ])));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
            Arc::new(StringArray::from_iter_values(relations.iter().map(|relation| relation.name()))),
            Arc::new(Int64Array::from_iter_values(usage.iter().map(|row| row.current_data_bytes))),
            Arc::new(Int64Array::from_iter_values(usage.iter().map(|row| row.history_data_bytes))),
            Arc::new(Int64Array::from_iter(usage.iter().map(|row| row.current_search_bytes))),
            Arc::new(Int64Array::from_iter(usage.iter().map(|row| row.history_search_bytes))),
            Arc::new(Int64Array::from_iter(usage.iter().map(|row| row.ordered_index_bytes))),
        ]).map_err(arrow)?;
        let reply = self.owned_action_reply(&schema, Some(&batch))?;
        held.snapshot.check().map_err(status)?;
        held.snapshot.check_live_change_relations(&relations).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_owned_merged_changes_action(&self, call: &Call, body: &[u8], descending: bool) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Merged changes require a held read handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Ok(entry) = self.take_owned_read(call, &handle) { entry.close(); }
            return Err(status(error));
        }
        let (after, through, limit, relations, _charge) = self.merged_changes_request(call, request)?;
        let page = if descending {
            held.snapshot.merged_changes_descending(&relations, after, through, limit).await
        } else {
            held.snapshot.merged_changes_at(&relations, after, through, limit).await
        }.map_err(status)?;
        let reply = self.owned_merged_changes_reply(&page, held.snapshot.present().system_time, descending, None)?;
        held.snapshot.check_live_change_relations(&relations).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_world_changes_action(&self, call: &Call, body: &[u8], descending: bool) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("World changes require a held world handle"))?;
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let (after, through, limit, relations, _charge) = self.merged_changes_request(call, request)?;
        let page = held.snapshot.changes(&relations, after, through, limit, descending).await.map_err(status)?;
        let reply = self.owned_merged_changes_reply(&page, held.snapshot.present(), descending, None)?;
        for relation in &relations { held.snapshot.check_relation(relation).map_err(status)?; }
        Ok(reply)
    }

    pub(crate) async fn held_world_region_changes_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("World region changes require a held world handle"))?;
        let (relation, request) = split_relation(request)?;
        let (after, request) = request.split_first_chunk::<8>()
            .ok_or_else(|| invalid("World region changes require an exclusive revision"))?;
        let (through, request) = request.split_first_chunk::<8>()
            .ok_or_else(|| invalid("World region changes require an inclusive upper revision"))?;
        let (limit, request) = request.split_first_chunk::<2>()
            .ok_or_else(|| invalid("World region changes require a group limit"))?;
        let (lower, upper, valid, _bounds) = change_region_request(call, request)?;
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let mut page = held.snapshot.changes(
            std::slice::from_ref(&relation), Timestamp(i64::from_be_bytes(*after)),
            Timestamp(i64::from_be_bytes(*through)), usize::from(u16::from_be_bytes(*limit)), false,
        ).await.map_err(status)?.into_relation_page().map_err(status)?;
        page.retain_identity_region(lower.as_ref(), upper.as_ref(), valid).map_err(status)?;
        let reply = self.owned_changes_reply(&page, held.snapshot.present())?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    fn merged_changes_request(
        &self, call: &Call, request: &[u8],
    ) -> Result<(Timestamp, Timestamp, usize, Vec<OwnedRelation>, Arc<MemoryReservation>), Status> {
        let (after, rest) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Merged changes require an exclusive revision"))?;
        let (through, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("Merged changes require an inclusive upper revision"))?;
        let (limit, rest) = rest.split_first_chunk::<2>().ok_or_else(|| invalid("Merged changes require a group limit"))?;
        let (count, mut rest) = rest.split_first_chunk::<2>().ok_or_else(|| invalid("Merged changes require a relation count"))?;
        let count = usize::from(u16::from_be_bytes(*count));
        if !(1..=rtdb_node::OWNED_MERGED_CHANGE_RELATIONS).contains(&count) {
            return Err(invalid("Merged changes require one to 32 owner relations"));
        }
        let charge = call.reserve(count * 512)?;
        let mut relations = Vec::with_capacity(count);
        for _ in 0..count {
            let (relation, tail) = split_relation(rest)?;
            relations.push(relation);
            rest = tail;
        }
        if !rest.is_empty() { return Err(invalid("Merged changes have trailing bytes")); }
        Ok((Timestamp(i64::from_be_bytes(*after)), Timestamp(i64::from_be_bytes(*through)),
            usize::from(u16::from_be_bytes(*limit)), relations, charge))
    }

    fn owned_merged_changes_reply(
        &self, page: &rtdb_node::OwnedMergedChanges, present: Timestamp, descending: bool, witness: Option<Uuid>,
    ) -> Result<ActionResult, Status> {
        use rtdb_types::{LogicalType, ScalarType};

        let changes_type = LogicalType::list(LogicalType::Struct(StructFields::from([
            ("relation".into(), LogicalType::Scalar(ScalarType::Utf8)),
            ("touches".into(), owned_touches_type()),
        ])));
        let mut metadata = std::collections::HashMap::from([
            (OWNED_PRESENT.into(), present.0.to_string()),
            (OWNED_CHANGE_COVERAGE.into(), page.covered_through().map_err(status)?.0.to_string()),
        ]);
        if let Some(witness) = witness {
            metadata.insert(WORLD_RANGE_WITNESS.into(), witness.simple().to_string());
        }
        if descending {
            metadata.insert("rtdb.owned.change_complete".into(), page.is_complete().to_string());
        }
        let schema = Schema::new(vec![
            Field::new("revision", DataType::Int64, false),
            rtdb_columnar::to_field("changes", &changes_type).map_err(status)?,
        ]).with_metadata(metadata);
        if page.commits.is_empty() {
            return self.owned_action_reply(&schema, None);
        }
        let schema = Arc::new(schema);
        let _rows = self.memory().reserve(wire::add(wire::mul(page.retained_bytes, 2)?, 8192)?).map_err(status)?;
        let revisions = Int64Array::from_iter_values(page.commits.iter().map(|commit| commit.revision.0));
        let changes: Vec<_> = page.commits.iter().map(|commit| Value::list(commit.changes.iter().map(|change| {
            Value::Struct(StructFields::from([
                ("relation".into(), Value::Utf8(change.relation.name().into())),
                ("touches".into(), Value::list(change.touches.iter().map(rtdb_types::OwnedChange::to_value).collect())),
            ]))
        }).collect())).collect();
        let _arrow = self.memory().reserve(rtdb_columnar::estimate_builder_bytes(&changes_type, &changes).map_err(status)?)
            .map_err(status)?;
        let changes = rtdb_columnar::build_array(&changes_type, &changes).map_err(status)?;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(revisions), changes]).map_err(arrow)?;
        self.owned_action_reply(&schema, Some(&batch))
    }

    fn owned_changes_reply(&self, page: &rtdb_node::OwnedChangePage, present: Timestamp) -> Result<ActionResult, Status> {
        let touches_type = owned_touches_type();
        let schema = Arc::new(Schema::new(vec![
            Field::new("revision", DataType::UInt64, false),
            Field::new("system_time_micros", DataType::Int64, false),
            rtdb_columnar::to_field("touches", &touches_type).map_err(status)?,
        ]).with_metadata(std::collections::HashMap::from([
            (OWNED_PRESENT.into(), present.0.to_string()),
            (OWNED_CHANGE_COVERAGE.into(), page.covered_through.0.to_string()),
        ])));
        if page.groups.is_empty() { return self.owned_action_reply(&schema, None); }
        let _rows = self.memory().reserve(wire::add(wire::mul(page.retained_bytes, 2)?, 8192)?).map_err(status)?;
        let revisions = UInt64Array::from(page.groups.iter().map(|group| group.revision).collect::<Vec<_>>());
        let times = Int64Array::from(page.groups.iter().map(|group| group.system_time.0).collect::<Vec<_>>());
        let touches: Vec<_> = page.groups.iter().map(|group| Value::list(
            group.touches.iter().map(rtdb_types::OwnedChange::to_value).collect(),
        )).collect();
        let _arrow = self.memory().reserve(rtdb_columnar::estimate_builder_bytes(&touches_type, &touches).map_err(status)?)
            .map_err(status)?;
        let touches = rtdb_columnar::build_array(&touches_type, &touches).map_err(status)?;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(revisions), Arc::new(times), touches]).map_err(arrow)?;
        self.owned_action_reply(&schema, Some(&batch))
    }

    fn take_owned_read(&self, call: &Call, handle: &Bytes) -> Result<Arc<OwnedReadResource>, Status> {
        if call.session.audience_source().is_some() {
            call.session.take_audience_read(handle).ok_or_else(|| Status::not_found("unknown owned read"))
        } else {
            found(self.0.owned_reads.take(handle), "owned read")
        }
    }

    pub(crate) fn close_owned_read_action(&self, call: &Call, body: &[u8]) -> Result<(), Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        if body.len() != 16 { return Err(invalid("Owned read close requires one 16-byte handle")); }
        let handle = Bytes::copy_from_slice(body);
        self.retained_owned_read(call, &handle)?;
        let entry = self.take_owned_read(call, &handle)?;
        entry.close();
        Ok(())
    }

    pub(crate) async fn prepare_held_world_overlay_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let host = self.0.config.owned_world_host.as_ref().ok_or_else(unsupported)?;
        let (handle, rest) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Overlay preparation requires a held world"))?;
        let (system, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("Overlay preparation requires an exact system publication"))?;
        let (child, rest) = rest.split_first_chunk::<16>().ok_or_else(|| invalid("Overlay preparation requires a child identity"))?;
        let retain_until = Timestamp(i64::from_be_bytes(rest.try_into().map_err(|_| invalid("Overlay preparation requires a retention deadline"))?));
        let system = Timestamp(i64::from_be_bytes(*system));
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let attachment = attachment.ok_or_else(unsupported)?;
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        require_idle(&session)?;
        let scope = session.placed_history().map_err(status)?.ok_or_else(unsupported)?;
        let cut = held.snapshot.retained_child_cut(scope, system).await.map_err(status)?;
        let charge = call.reserve(4096)?;
        let prepared = host.prepare_world_overlay(
            crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
            cut, Uuid::from_bytes(*child), retain_until,
        ).await.map_err(status)?;
        held.snapshot.check().map_err(status)?;
        let body = serde_json::to_vec(&prepared).map_err(|_| Status::internal("Could not encode prepared overlay"))?;
        Ok(ActionResult { body: wire::retain(body, charge) })
    }

    pub(crate) async fn prepare_replay_parent_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let host = self.0.config.owned_world_host.as_ref().ok_or_else(unsupported)?;
        let (binding, rest) = body.split_first_chunk::<32>().ok_or_else(|| invalid("Replay parent requires a hold binding"))?;
        let original = match rest.len() {
            0 => None,
            16 => Some(rtdb_types::TransactionKey {
                tx_id: rtdb_types::TxId(i64::from_be_bytes(rest[..8].try_into().expect("fixed length"))),
                system_time: Timestamp(i64::from_be_bytes(rest[8..].try_into().expect("fixed length"))),
            }),
            _ => return Err(invalid("Replay parent requires its complete Original transaction")),
        };
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let attachment = attachment.ok_or_else(unsupported)?;
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        require_idle(&session)?;
        let charge = call.reserve(4096)?;
        let parent = host.prepare_replay_parent(
            crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
            *binding, original,
        ).await.map_err(status)?;
        let body = serde_json::to_vec(&parent).map_err(|_| Status::internal("Could not encode replay parent"))?;
        Ok(ActionResult { body: wire::retain(body, charge) })
    }

    pub(crate) async fn finish_replay_parents_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let host = self.0.config.owned_world_host.as_ref().ok_or_else(unsupported)?;
        let binding = body.try_into().map_err(|_| invalid("Finishing replay parents requires the restore binding"))?;
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let attachment = attachment.ok_or_else(unsupported)?;
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        require_idle(&session)?;
        host.finish_replay_parents(
            crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
            binding,
        ).await.map_err(status)?;
        Ok(ActionResult { body: Bytes::new() })
    }

    pub(crate) async fn retain_replayed_world_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let host = self.0.config.owned_world_host.as_ref().ok_or_else(unsupported)?;
        let (binding, world) = body.split_first_chunk::<32>()
            .and_then(|(binding, tail)| <[u8; 16]>::try_from(tail).ok().map(|world| (binding, world)))
            .ok_or_else(|| invalid("Replay world retention requires a hold binding and world UUID"))?;
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let attachment = attachment.ok_or_else(unsupported)?;
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        require_idle(&session)?;
        let charge = call.reserve(4096)?;
        let retained = host.retain_replayed_world(
            crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
            *binding, Uuid::from_bytes(world),
        ).await.map_err(status)?;
        let body = serde_json::to_vec(&retained).map_err(|_| Status::internal("Could not encode replay world retention"))?;
        Ok(ActionResult { body: wire::retain(body, charge) })
    }

    pub(crate) async fn open_world_read_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let host = self.0.config.owned_world_host.as_ref().ok_or_else(unsupported)?;
        let bytes: [u8; 16] = body.try_into().map_err(|_| invalid("Owned world read open requires a world UUID"))?;
        let world = Uuid::from_bytes(bytes);
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let attachment = attachment.ok_or_else(unsupported)?;
        let permit = Arc::clone(&self.0.owned_read_slots).try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("The node has too many retained owned reads"))?;
        let reservation = call.reserve(wire::add(2048, wire::add(call.session.id.len(), call.database.len())?)?)?;
        let reply_charge = call.reserve(96)?;
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        require_idle(&session)?;
        let snapshot = host.open_world_read(
            crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
            world,
        ).await.map_err(status)?;
        let handle = Uuid::new_v4();
        let parent = snapshot.parent_basis();
        let mut reply = Vec::with_capacity(57);
        reply.extend_from_slice(handle.as_bytes());
        reply.extend_from_slice(&snapshot.present().0.to_be_bytes());
        reply.extend_from_slice(&parent.tx_id.0.to_be_bytes());
        reply.extend_from_slice(&parent.system_time.0.to_be_bytes());
        reply.extend_from_slice(&snapshot.application_clock().unwrap_or(Timestamp::MIN).0.to_be_bytes());
        reply.extend_from_slice(&snapshot.initial_clock().0.to_be_bytes());
        if let Some((world, system)) = snapshot.ancestor() {
            reply.push(3);
            reply.extend_from_slice(&world.0);
            reply.extend_from_slice(&system.0.to_be_bytes());
        } else {
            reply.push(match snapshot.clock_mode() {
                rtdb_types::WorldClockMode::Retained => 0,
                rtdb_types::WorldClockMode::Held { .. } => 1,
                rtdb_types::WorldClockMode::Proposal => 2,
            });
        }
        let held = Arc::new(OwnedWorldReadResource {
            snapshot, world, conditions: Mutex::new(BTreeMap::new()),
            owner: call.session.owner(), session: call.session.id.clone(), database: call.database.clone(),
            _permit: permit, _reservation: reservation,
        });
        self.0.owned_world_reads.insert(Bytes::copy_from_slice(handle.as_bytes()), held, READ_IDLE, READ_IDLE, false, 16).map_err(status)?;
        Ok(ActionResult { body: wire::retain(reply, reply_charge) })
    }

    pub(crate) fn read_held_world_head_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() {
            return Err(unsupported());
        }
        if body.len() != 16 { return Err(invalid("Held world head requires its original read handle")); }
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(body))?;
        held.snapshot.check().map_err(status)?;
        if !matches!(held.snapshot.clock_mode(), rtdb_types::WorldClockMode::Held { .. }) {
            return Err(status(rtdb_types::OwnedWorld::unavailable()));
        }
        let Some((reading, revision)) = held.snapshot.application_publication() else {
            return Ok(ActionResult { body: Bytes::from_static(&[0]) });
        };
        if revision <= held.snapshot.parent_basis().system_time || revision > held.snapshot.present() {
            return Err(status(rtdb_types::OwnedWorld::unavailable()));
        }
        let mut reply = Vec::with_capacity(16);
        reply.extend_from_slice(&revision.0.to_be_bytes());
        reply.extend_from_slice(&reading.0.to_be_bytes());
        Ok(ActionResult { body: wire::retain(reply, call.reserve(16)?) })
    }

    fn owned_world_read(&self, call: &Call, handle: &Bytes) -> Result<Arc<OwnedWorldReadResource>, Status> {
        let entry = self.owned_world_resource(call, handle)?;
        if entry.snapshot.ancestor().is_some() {
            return Err(Status::unimplemented("The requested read is unavailable on this retained world"));
        }
        Ok(entry)
    }

    fn owned_world_resource(&self, call: &Call, handle: &Bytes) -> Result<Arc<OwnedWorldReadResource>, Status> {
        let entry = found(self.0.owned_world_reads.lookup(handle, false), "owned world read")?;
        if entry.session != call.session.id || entry.database != call.database || !call.session.owns(&entry.owner) {
            return Err(Status::not_found("unknown owned world read"));
        }
        Ok(entry)
    }

    pub(crate) async fn held_world_at_preflight_action(&self, call: &Call, body: &[u8], selection: WorldCutSelection) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, body) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Historical world scope has no held handle"))?;
        let (system, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Historical world scope has no ordered present"))?;
        let system = Timestamp(i64::from_be_bytes(*system));
        let (count, mut body) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Historical world scope has no relation count"))?;
        let count = usize::from(u16::from_be_bytes(*count));
        if !(1..=32).contains(&count) { return Err(invalid("Historical world scope requires one to 32 relations")); }
        let _charge = call.reserve(count * 512)?;
        let mut relations = Vec::with_capacity(count);
        for _ in 0..count {
            let (relation, tail) = split_relation(body)?;
            relations.push(relation);
            body = tail;
        }
        if !body.is_empty() || relations.iter().collect::<BTreeSet<_>>().len() != count {
            return Err(invalid("Historical world scope has repeated relations or trailing bytes"));
        }
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_world_resource(call, &handle)?;
        if matches!(selection, WorldCutSelection::AsOfParent) {
            let selected = held.snapshot.select_parent_as_of(&relations, system).await.map_err(status)?;
            for relation in &relations { held.snapshot.check_relation(relation).map_err(status)?; }
            let Some(selected) = selected else {
                return Ok(ActionResult { body: Bytes::from_static(&[0]) });
            };
            return Ok(ActionResult { body: wire::retain(selected.0.to_be_bytes().to_vec(), call.reserve(8)?) });
        }
        let mode = match held.snapshot.clock_mode() {
            rtdb_types::WorldClockMode::Held { .. } => 1,
            rtdb_types::WorldClockMode::Proposal => 2,
            rtdb_types::WorldClockMode::Retained if held.snapshot.ancestor().is_some() => 1,
            rtdb_types::WorldClockMode::Retained => return Err(status(rtdb_types::OwnedWorld::unavailable())),
        };
        let selected = if matches!(selection, WorldCutSelection::AsOfLocal) {
            held.snapshot.select_accepted_scope(&relations, system).await.map_err(status)?
        } else {
            let clock = held.snapshot.preflight_accepted_scope(&relations, system).await.map_err(status)?;
            Some((system, clock))
        };
        for relation in &relations { held.snapshot.check_relation(relation).map_err(status)?; }
        let Some((system, clock)) = selected else {
            return Ok(ActionResult { body: Bytes::from_static(&[0]) });
        };
        let mut reply = Vec::with_capacity(17);
        reply.extend_from_slice(&system.0.to_be_bytes());
        reply.extend_from_slice(&clock.0.to_be_bytes());
        reply.push(mode);
        Ok(ActionResult { body: wire::retain(reply, call.reserve(17)?) })
    }

    pub(crate) async fn held_world_point_at_action(&self, call: &Call, body: &[u8], parent: bool) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, body) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Historical world point has no held handle"))?;
        let (system, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Historical world point has no ordered present"))?;
        let system = Timestamp(i64::from_be_bytes(*system));
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_world_resource(call, &handle)?;
        let (relation, id, valid, _identity) = point_request(call, body, PointTime::Required)?;
        let valid = valid.ok_or_else(|| invalid("Historical world point requires a valid instant"))?;
        let point = if parent {
            held.snapshot.parent_point_at(&relation, &id, valid, system).await
        } else {
            held.snapshot.accepted_point(&relation, &id, valid, system).await
        }.map_err(status)?;
        if point.present != system || point.witness.is_some() || point.until.is_some() {
            return Err(history_invalid());
        }
        let response = self.world_point_reply(call, point, valid, None)?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(response)
    }

    pub(crate) async fn held_world_point_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, body) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned world point has no held handle"))?;
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let (mode, body) = body.split_first().ok_or_else(|| invalid("Owned world point has no observation flag"))?;
        let (relation, id, valid, _identity) = point_request(call, body, PointTime::Required)?;
        let valid_at = valid.ok_or_else(|| invalid("Owned point requires a valid_micros instant"))?;
        let mut result = match mode {
            0 | 1 => held.snapshot.read_point(&relation, &id, valid_at, *mode == 1).await,
            2 | 3 => held.snapshot.read_parent_point(&relation, &id, valid_at, *mode == 3).await,
            4 => held.snapshot.read_ancestor_root_point(&relation, &id, valid_at).await,
            _ => return Err(invalid("Invalid owned world point source")),
        }.map_err(status)?;
        if result.present != held.snapshot.present() {
            return Err(Status::internal("Owned world point changed its held present"));
        }
        let until = result.until;
        let absent = result.row.is_none() && !result.shadowed;
        let nonce = result.witness.take().map(|evidence| held.remember(call, evidence, absent)).transpose()?;
        let reply = self.world_point_reply(call, result, valid_at, nonce)?;
        if matches!(*mode, 2 | 3 | 4) {
            held.snapshot.check_parent_point(&relation, until).map_err(status)?;
        } else {
            held.snapshot.check_relation(&relation).map_err(status)?;
        }
        Ok(reply)
    }

    pub(crate) async fn held_world_valid_during_action(&self, call: &Call, body: &[u8], selection: WorldHistoricalRead) -> Result<ActionResult, Status> {
        use rtdb_types::Interval;

        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, body) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned world history has no held handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_world_resource(call, &handle)?;
        let (system, body) = if !matches!(selection, WorldHistoricalRead::Current) {
            let (system, rest) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Historical world schedule has no revision"))?;
            (Some(Timestamp(i64::from_be_bytes(*system))), rest)
        } else { (None, body) };
        let (mode, body) = body.split_first().ok_or_else(|| invalid("Owned world history has no source"))?;
        let parent_only = match *mode {
            0 if !matches!(selection, WorldHistoricalRead::EarlierParent) => false,
            1 if !matches!(selection, WorldHistoricalRead::EarlierEffective) => true,
            _ => return Err(invalid("Invalid owned world history source")),
        };
        let (relation, arrow_bytes) = split_relation(body)?;
        let batch = request_batch(call, arrow_bytes)?;
        let batch = batch.batch();
        if batch.num_columns() != 3 || batch.num_rows() != 1 {
            return Err(invalid("Owned world history requires a typed identity and two valid endpoints"));
        }
        let (id, _identity) = point_identity(call, batch)?;
        let from = history_request_endpoint(batch, "valid_from_micros")?;
        let to = history_request_endpoint(batch, "valid_to_micros")?;
        let valid = Interval::new(from, to).map_err(status)?;
        if valid.is_empty() { return Err(invalid("Owned world history requires a nonempty valid window")); }
        let (slices, witness) = if let Some(system) = system {
            if parent_only {
                let (slices, witness) = held.snapshot.parent_schedule_at(&relation, &id, valid, system).await.map_err(status)?;
                (slices, Some(witness))
            } else {
                (held.snapshot.valid_during_at(&relation, &id, valid, system).await.map_err(status)?, None)
            }
        } else {
            held.snapshot.valid_during(&relation, &id, valid, parent_only).await.map_err(status)?
        };
        let _rows = call.reserve(4096 + slices.len() * 128)?;
        let mut metadata = std::collections::HashMap::from([
            (OWNED_PRESENT.into(), system.unwrap_or_else(|| held.snapshot.present()).0.to_string()),
            (OWNED_VALID_FROM.into(), from.0.to_string()),
            (OWNED_VALID_TO.into(), to.0.to_string()),
        ]);
        if let Some(witness) = witness {
            metadata.insert(WORLD_RANGE_WITNESS.into(), held.remember(call, witness, false)?.simple().to_string());
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("valid_from_micros", DataType::Int64, false),
            Field::new("valid_to_micros", DataType::Int64, false),
            Field::new("system_from_micros", DataType::Int64, false),
        ]).with_metadata(metadata));
        let rows = RecordBatch::try_new(Arc::clone(&schema), vec![
            Arc::new(Int64Array::from_iter_values(slices.iter().map(|(valid, _)| valid.lower().0))),
            Arc::new(Int64Array::from_iter_values(slices.iter().map(|(valid, _)| valid.upper().0))),
            Arc::new(Int64Array::from_iter_values(slices.iter().map(|(_, revision)| revision.0))),
        ]).map_err(arrow)?;
        let reply = self.owned_action_reply(&schema, Some(&rows))?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_world_identities_action(&self, call: &Call, body: &[u8], mode: WorldIdentityRead) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, body) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned world identities have no held handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_world_resource(call, &handle)?;
        held.snapshot.check().map_err(status)?;
        let (parent_only, body) = if matches!(mode, WorldIdentityRead::EffectiveAt) {
            let (selector, rest) = body.split_first().ok_or_else(|| invalid("Historical world identities have no selector"))?;
            match selector {
                0 => (true, rest),
                1 => (false, rest),
                _ => return Err(invalid("Invalid historical world identity selector")),
            }
        } else { (false, body) };
        let (system, body) = if matches!(mode, WorldIdentityRead::EffectiveAt | WorldIdentityRead::OverlayAt | WorldIdentityRead::ParentAt) {
            let (system, rest) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Historical world identities have no revision"))?;
            (Some(Timestamp(i64::from_be_bytes(*system))), rest)
        } else { (None, body) };
        let (relation, arrow_bytes) = split_relation(body)?;
        let request = decode_batch(call, arrow_bytes)?;
        let request = request.batch();
        if request.num_columns() != 3 || request.num_rows() != 1 {
            return Err(invalid("Owned world identities require one valid instant, exclusive bound, and limit"));
        }
        let valid = history_request_endpoint(request, "valid_micros")?;
        let limit = usize::try_from(history_request_integer(request, "limit")?)
            .map_err(|_| invalid("Owned world identity limit is out of range"))?;
        let (column, field) = request.schema_ref().column_with_name("after")
            .ok_or_else(|| invalid("Owned world identities require an exclusive bound"))?;
        let after = request.column(column).as_any().downcast_ref::<StringArray>()
            .filter(|_| field.metadata().is_empty())
            .ok_or_else(|| invalid("Owned world identity bound must be nullable Utf8"))?;
        let after = (!after.is_null(0)).then(|| after.value(0));
        let page = match (mode, system) {
            (WorldIdentityRead::ParentAt, Some(system)) =>
                held.snapshot.parent_identity_page_at(&relation, valid, after, limit, system).await,
            (WorldIdentityRead::EffectiveAt, Some(system)) =>
                held.snapshot.identity_page_at(&relation, valid, after, limit, system, parent_only).await,
            (WorldIdentityRead::OverlayAt, Some(system)) =>
                held.snapshot.overlay_identities_at(&relation, valid, after, limit, system).await,
            (WorldIdentityRead::Overlay, None) =>
                held.snapshot.overlay_identities(&relation, valid, after, limit).await,
            (WorldIdentityRead::Effective, None) =>
                held.snapshot.identity_page(&relation, valid, after, limit).await,
            _ => return Err(invalid("Invalid held world identity coordinates")),
        }.map_err(status)?;
        let bytes = page.keys.iter().try_fold(4096, |size, key| wire::add(size, key.len()))?;
        let _copy = call.reserve(bytes)?;
        let keys = StringArray::from_iter_values(page.keys.iter().map(String::as_str));
        let rows = RecordBatch::try_from_iter([("_id", Arc::new(keys) as Arc<dyn Array>)]).map_err(arrow)?;
        let schema = rows.schema_ref().as_ref().clone().with_metadata(
            [(OWNED_IDENTITY_MORE.into(), u8::from(page.more).to_string())].into()
        );
        let reply = self.owned_point_reply(&schema, Some(&rows), system.unwrap_or_else(|| held.snapshot.present()), valid, None)?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) fn close_world_read_action(&self, call: &Call, body: &[u8]) -> Result<(), Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        if body.len() != 16 { return Err(invalid("Owned world read close requires one held handle")); }
        let handle = Bytes::copy_from_slice(body);
        self.owned_world_resource(call, &handle)?;
        let entry = found(self.0.owned_world_reads.take(&handle), "owned world read")?;
        entry.close();
        Ok(())
    }

    fn world_point_reply(&self, call: &Call, result: crate::WorldPointResult, valid_at: Timestamp, nonce: Option<Uuid>) -> Result<ActionResult, Status> {
        self.world_point_reply_with_extent(call, result, valid_at, nonce, None)
    }

    fn world_point_reply_with_extent(
        &self, call: &Call, result: crate::WorldPointResult, valid_at: Timestamp, nonce: Option<Uuid>,
        extent: Option<rtdb_types::Interval>,
    ) -> Result<ActionResult, Status> {
        let mut metadata = std::collections::HashMap::from([
            ("rtdb.owned.world_shadowed".into(), u8::from(result.shadowed).to_string()),
        ]);
        if let Some(extent) = extent {
            metadata.insert(OWNED_VALID_FROM.into(), extent.lower().0.to_string());
            metadata.insert(OWNED_VALID_TO.into(), extent.upper().0.to_string());
        }
        if let Some(nonce) = nonce {
            metadata.insert(WORLD_POINT_WITNESS.into(), nonce.simple().to_string());
        }
        if let Some(row) = result.row {
            let crate::WorldPointRow { fields: row, revision, assertion, reservation: _retained } = row;
            if let Some(assertion) = assertion {
                metadata.insert(OWNED_ASSERTION_LINE.into(),
                    Uuid::from_bytes(assertion.line.map_or([0; 16], |line| line.0)).simple().to_string());
                metadata.insert(OWNED_ASSERTION_ORDINAL.into(), assertion.ordinal.to_string());
                if let Some(reading) = assertion.held_reading {
                    metadata.insert(OWNED_ASSERTION_HELD_READING.into(), reading.0.to_string());
                }
            }
            let bytes = Value::document_retained_bytes(&row).map_err(status)?;
            let _encoding = call.reserve(wire::add(4096, wire::mul(bytes, 8)?)?)?;
            let mut fields = Vec::with_capacity(row.len());
            let mut columns = Vec::with_capacity(row.len());
            for (name, value) in &row {
                let logical = value.logical_type();
                fields.push(rtdb_columnar::to_field(name, &logical).map_err(status)?);
                columns.push(rtdb_columnar::build_array(&logical, std::slice::from_ref(value)).map_err(status)?);
            }
            let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
            let batch = RecordBatch::try_new(Arc::clone(&schema), columns).map_err(arrow)?;
            self.owned_point_reply_until(&schema, Some(&batch), result.present, valid_at, Some(revision), result.until)
        } else {
            self.owned_point_reply(&Schema::empty().with_metadata(metadata), None, result.present, valid_at, None)
        }
    }

    pub(crate) async fn publish_held_world_retention_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let host = self.0.config.owned_world_host.as_ref().ok_or_else(unsupported)?;
        let (handle, body) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Retaining a Held publication requires its original hold"))?;
        let (system, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Retaining a Held publication requires its physical revision"))?;
        let (child, body) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Retaining a Held publication requires a minted child"))?;
        let (until, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("The retained child has no expiry"))?;
        let (deadline, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("The retained child has no commit deadline"))?;
        let (attempt, body) = body.split_first_chunk::<22>().ok_or_else(|| invalid("The retained child has no attempt identity"))?;
        let (fingerprint, body) = body.split_first_chunk::<32>().ok_or_else(|| invalid("The retained child has no fingerprint"))?;
        let attempt = std::str::from_utf8(attempt).map_err(|_| invalid("Invalid retained child attempt"))?;
        if child == &[0; 16] || !attempt.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) {
            return Err(invalid("Invalid retained child identity or attempt"));
        }
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let scope = self.hosted_control_scope(call).await?;
        let cut = held.snapshot.retained_child_cut(scope, Timestamp(i64::from_be_bytes(*system))).await.map_err(status)?;
        let policy = self.hosted_control_policy(call)?;
        let groups = owned_groups(body, true, false)?;
        hosted_control::admit_control_targets(policy, groups.iter().map(|(relation, _)| relation))?;
        let mut rows_left = MAX_GROUPED_COMMIT_ROWS - 3;
        let (controls, _charges) = self.decode_hosted_control_groups(call, groups, scope, &mut rows_left, true)?;
        if controls.is_empty() || controls.iter().any(|control| !control.is_check()) {
            return Err(invalid("Retaining a Held publication requires checked hosted ownership"));
        }
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let attachment = attachment.ok_or_else(unsupported)?;
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        require_idle(&session)?;
        let charge = call.reserve(64)?;
        let child = rtdb_types::Iid(*child);
        let deadline = Timestamp(i64::from_be_bytes(*deadline)).min(cut.until());
        let (registered, key) = host.fork_world_checked(
            crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
            Uuid::nil(), None, Timestamp(i64::from_be_bytes(*until)),
            rtdb_node::OwnedWorldFork {
                world: child, pin: child, previous: None, sibling: None, cut,
                requested: rtdb_node::WorldClockRequest::Retained,
                deadline, attempt: attempt.to_owned(), fingerprint: *fingerprint, controls, root: None,
            },
        ).await.map_err(status)?;
        if *registered.as_bytes() != child.0 { return Err(unsupported()); }
        let mut reply = Vec::with_capacity(32);
        reply.extend_from_slice(registered.as_bytes());
        reply.extend_from_slice(&key.tx_id.0.to_be_bytes());
        reply.extend_from_slice(&key.system_time.0.to_be_bytes());
        Ok(ActionResult { body: wire::retain(reply, charge) })
    }

    pub(crate) async fn publish_world_cut_action(&self, call: &Call, envelope: &Bytes, publish_root: bool) -> Result<ActionResult, Status> {
        let body = envelope.as_ref();
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let host = self.0.config.owned_world_host.as_ref().ok_or_else(unsupported)?;
        let (handle, rest) = body.split_first_chunk::<16>().ok_or_else(|| invalid("World fork has no held root handle"))?;
        let (world, rest) = rest.split_first_chunk::<16>().ok_or_else(|| invalid("World cut has no minted identity"))?;
        let (pin, rest) = rest.split_first_chunk::<16>().ok_or_else(|| invalid("World cut has no minted pin"))?;
        let (previous_handle, rest) = rest.split_first_chunk::<16>().ok_or_else(|| invalid("World cut has no predecessor handle"))?;
        let (capture, rest) = rest.split_first_chunk::<16>().ok_or_else(|| invalid("World fork has no capture"))?;
        let (suffix, rest) = rest.split_first_chunk::<16>().ok_or_else(|| invalid("World fork has no suffix selector"))?;
        if *capture == [0; 16] && (!publish_root || *suffix != [0; 16]) {
            return Err(invalid("Only a Root publication may discover its exact held archive"));
        }
        let (until, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("World fork has no expiry"))?;
        let (deadline, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("World fork has no deadline"))?;
        let ([mode], rest) = rest.split_first_chunk::<1>().ok_or_else(|| invalid("World fork has no clock mode"))?;
        let select_publication = matches!(*mode, 4 | 5) && publish_root;
        let proposed_publication = *mode == 5 && publish_root;
        let held_publication = *mode == 10 && publish_root;
        if held_publication && (*previous_handle != [0; 16] || *capture != [0; 16] || *suffix != [0; 16]) {
            return Err(invalid("A held Root fork derives its archive from the selected publication"));
        }
        let proposal_hold = matches!(*mode, 8 | 9) && publish_root;
        let proposal_shorten = *mode == 9 && publish_root;
        let sibling = *mode == 7 && publish_root;
        let renewal = matches!(*mode, 6 | 7 | 8 | 9) && publish_root;
        if renewal && (*previous_handle == [0; 16] || *capture != [0; 16] || *suffix != [0; 16]) {
            return Err(invalid("An authored renewal requires its original retained handle and derives its archive"));
        }
        let (held_basis, rest) = if held_publication {
            let (basis, rest) = rest.split_first_chunk::<8>()
                .ok_or_else(|| invalid("Held Root fork has no exact publication"))?;
            (Some(Timestamp(i64::from_be_bytes(*basis))), rest)
        } else {
            (None, rest)
        };
        let (requested, rest) = match *mode {
            10 if held_publication => (rtdb_node::WorldClockRequest::Held { start: None }, rest),
            0 => (rtdb_node::WorldClockRequest::Retained, rest),
            8 | 9 if proposal_hold => (rtdb_node::WorldClockRequest::Proposal, rest),
            6 | 7 if renewal => (rtdb_node::WorldClockRequest::Retained, rest),
            4 if select_publication => (rtdb_node::WorldClockRequest::Retained, rest),
            5 if proposed_publication => (rtdb_node::WorldClockRequest::Proposal, rest),
            1 => (rtdb_node::WorldClockRequest::Held { start: None }, rest),
            2 => {
                let (start, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("World fork has no explicit clock start"))?;
                (rtdb_node::WorldClockRequest::Held { start: Some(Timestamp(i64::from_be_bytes(*start))) }, rest)
            }
            3 => (rtdb_node::WorldClockRequest::Proposal, rest),
            _ => return Err(invalid("Invalid world fork clock mode")),
        };
        let (attempt, rest) = rest.split_first_chunk::<22>().ok_or_else(|| invalid("World fork has no attempt identity"))?;
        let (fingerprint, groups) = rest.split_first_chunk::<32>().ok_or_else(|| invalid("World fork has no fingerprint"))?;
        let attempt = std::str::from_utf8(attempt).map_err(|_| invalid("Invalid world fork attempt identity"))?;
        if !attempt.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) {
            return Err(invalid("Invalid world fork attempt identity"));
        }
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let policy = self.hosted_control_policy(call)?;
        let scope = self.hosted_control_scope(call).await?;
        let cut = if let Some(basis) = held_basis {
            held.snapshot.fork_held_root_publication_cut(scope, basis).await.map_err(status)?
        } else if select_publication {
            held.snapshot.fork_publication_cut(scope).await.map_err(status)?
        } else {
            held.snapshot.fork_cut(scope).await.map_err(status)?
        };
        let requested_until = Timestamp(i64::from_be_bytes(*until));
        let authored_advance = select_publication && !proposed_publication && *previous_handle != [0; 16];
        let authored_replacement = authored_advance || renewal;
        let previous = if *previous_handle == [0; 16] {
            if world != pin { return Err(invalid("An initial pin must name its world")); }
            None
        } else {
            if (requested_until == Timestamp::MIN) == authored_replacement
                || (!proposal_hold && !matches!(requested, rtdb_node::WorldClockRequest::Retained)) || ((world == pin) != sibling) {
                return Err(invalid("A retained advance preserves the original world lifetime and mode"));
            }
            let previous = self.owned_world_read(call, &Bytes::copy_from_slice(previous_handle))?;
            previous.snapshot.check().map_err(status)?;
            if ((previous.world.as_bytes() == world) == sibling) || (if proposal_hold { previous.snapshot.clock_mode() != rtdb_types::WorldClockMode::Proposal }
                    else { previous.snapshot.clock_mode().is_writable() })
                || (!renewal && previous.snapshot.parent_basis().system_time >= cut.basis())
                || previous.snapshot.parent_pin().0 == *pin
            {
                return Err(Status::failed_precondition("The original retained world cannot select this cut"));
            }
            Some(previous)
        };
        let cut = if renewal {
            let predecessor = previous.as_ref().ok_or_else(unsupported)?.snapshot.parent_basis();
            held.snapshot.fork_renewal_cut(scope, predecessor).await.map_err(status)?
        } else {
            cut
        };
        // Deadline, registration, one immutable attempt and a Root head
        // (or the standalone fork's optional tip check).
        let mut rows_left = MAX_GROUPED_COMMIT_ROWS - 4 - usize::from(select_publication);
        let mut root = rtdb_node::OwnedRootSection::default();
        let mut root_batches = None;
        let mut root_witness_charge = None;
        let mut root_framing = None;
        let mut root_groups = 0;
        let mut count_charges = Vec::new();
        let mut refusal_charges = Vec::new();
        let groups = if publish_root {
            if (previous.is_some() && !authored_replacement)
                || !(matches!(requested, rtdb_node::WorldClockRequest::Retained) || proposed_publication || proposal_hold || held_publication)
                || (cut.live().is_none() && !cut.is_publication())
            {
                return Err(invalid("A co-ordered fork requires an admitted Root hold and clock mode"));
            }
            let ([flags], groups) = groups.split_first_chunk::<1>()
                .ok_or_else(|| invalid("Missing Root cut decision flags"))?;
            if *flags & !3 != 0 {
                return Err(invalid("Invalid Root cut decision flags"));
            }
            let groups = if *flags & 1 != 0 {
                let (counts, groups, charges) = owned_count_conditions(call, groups, None, false)?;
                rows_left = rows_left.checked_sub(counts.len())
                    .ok_or_else(|| invalid("Fork exceeds its effect limit"))?;
                root.checks.extend(counts);
                count_charges = charges;
                groups
            } else { groups };
            let (witnesses, groups, charge) = self.owned_witness_prefix(call, groups, true)?;
            root_witness_charge = Some(charge);
            let (size, body) = groups.split_first_chunk::<4>().ok_or_else(|| invalid("Missing Root section length"))?;
            let (facts, tail) = body.split_at_checked(u32::from_be_bytes(*size) as usize)
                .ok_or_else(|| invalid("Incomplete Root groups"))?;
            let controls = if *flags & 2 != 0 {
                let (length, body) = tail.split_first_chunk::<4>()
                    .ok_or_else(|| invalid("Missing Root cut refusal length"))?;
                let (refusal, controls) = body.split_at_checked(u32::from_be_bytes(*length) as usize)
                    .ok_or_else(|| invalid("Incomplete Root cut refusal"))?;
                let refusal = owned_groups(refusal, true, false)?;
                hosted_control::admit_control_targets(policy, refusal.iter().map(|(relation, _)| relation))?;
                let (refusal, charges) = self.decode_hosted_control_groups(call, refusal, scope, &mut rows_left, false)?;
                if refusal.is_empty() { return Err(invalid("Missing Root cut refusal receipt")); }
                root.refusal = refusal;
                refusal_charges = charges;
                controls
            } else { tail };
            root_groups = ordered_group_count(facts)?;
            root_framing = Some(call.reserve(root_groups * 512)?);
            let mut operations = Vec::new();
            let mut fact_rows_left = rows_left;
            root_batches = Some(self.decode_owned_commit_groups(
                call, envelope, owned_groups(facts, false, true)?, &mut operations, &mut fact_rows_left, CommitEffects::Facts,
            )?);
            rows_left = rows_left.checked_sub(operations.len() + witnesses.len())
                .ok_or_else(|| invalid("Fork exceeds its effect limit"))?;
            for (original, evidence) in witnesses {
                if !Arc::ptr_eq(&held, &original) || !evidence.matches_scope(Some(scope)) {
                    return Err(Status::failed_precondition("Root range witness belongs to another hold or placement"));
                }
                root.checks.push(evidence.operation());
            }
            let mut writes = Vec::new();
            for operation in operations {
                match &operation {
                    TxOp::OwnedCheck { relation, .. } if relation.is_root_data() =>
                        root.checks.push(operation),
                    _ if rtdb_tx::is_root_partition_frontier(&operation) => writes.push(operation),
                    TxOp::OwnedInsertDocs { relation, .. } | TxOp::OwnedReplaceDocs { relation, .. }
                    | TxOp::OwnedArrowDocs { relation, .. } | TxOp::OwnedRelativePeriod { relation, .. } | TxOp::OwnedDelete { relation, .. } if relation.is_root_data() =>
                        writes.push(operation),
                    _ => return Err(invalid("Operation is outside the held Root section")),
                }
            }
            let facts = writes.iter().filter(|op| !rtdb_tx::is_root_partition_frontier(op)).collect::<Vec<_>>();
            if (authored_replacement && !sibling && !proposal_hold
                && !matches!(facts.as_slice(), [write] if write.is_single_unbounded_root_document(false)))
                || ((sibling || (proposal_hold && !proposal_shorten))
                    && !matches!(facts.as_slice(), [write] if write.is_single_unbounded_root_document(true)))
                || (proposal_shorten && !matches!(facts.as_slice(), [
                    TxOp::OwnedDelete { valid_from: None, valid_to: None, .. }
                ]))
            {
                return Err(invalid("The authored custody change requires one acceptance-valid Root effect"));
            }
            if !writes.is_empty() {
                root.publication = Some(rtdb_node::OwnedRootPublication { attempt: attempt.to_owned(), writes });
            }
            controls
        } else {
            groups
        };
        let _root_reservations = (root_batches, root_witness_charge, root_framing, count_charges, refusal_charges);
        let groups = owned_groups(groups, true, false)?;
        if root_groups + groups.len() > MAX_COMMIT_GROUPS {
            return Err(invalid("Fork exceeds its grouped effect limit"));
        }
        hosted_control::admit_control_targets(policy, groups.iter().map(|(relation, _)| relation))?;
        let (controls, _charges) = self.decode_hosted_control_groups(call, groups, scope, &mut rows_left, !publish_root)?;
        if !controls.iter().any(rtdb_tx::HostedControlWrite::is_check) {
            return Err(invalid("A retained fork requires a hosted ownership check"));
        }
        if publish_root && (root.publication.is_none() || proposed_publication || held_publication)
            && controls.iter().all(rtdb_tx::HostedControlWrite::is_check) {
            return Err(invalid("A co-ordered fork requires Root facts or a hosted control replacement"));
        }
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let attachment = attachment.ok_or_else(unsupported)?;
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        require_idle(&session)?;
        let reply_charge = call.reserve(64)?;
        let suffix = Uuid::from_bytes(*suffix);
        let original = previous.as_ref().map(|previous| &previous.snapshot);
        let mut deadline = Timestamp(i64::from_be_bytes(*deadline));
        if publish_root {
            held.snapshot.check().map_err(status)?;
            deadline = deadline.min(held.snapshot.evidence_until());
            if held.snapshot.present().system_time >= deadline {
                return Err(Status::failed_precondition("Root hold is outside the fork deadline"));
            }
        }
        if let Some(previous) = original {
            previous.check().map_err(status)?;
            deadline = deadline.min(previous.evidence_until());
            if sibling {
                deadline = deadline.min(Timestamp(previous.until().0.saturating_sub(rtdb_tx::SIBLING_PIN_MARGIN_MICROS)));
            }
        }
        let (registered, key) = host.fork_world_checked(
            crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
            Uuid::from_bytes(*capture), (!suffix.is_nil()).then_some(suffix),
            if authored_replacement { requested_until } else { original.map_or(requested_until, |world| world.until()) },
            rtdb_node::OwnedWorldFork {
                world: rtdb_types::Iid(*world), pin: rtdb_types::Iid(*pin),
                previous: original.map(|world| world.parent_pin()),
                sibling: previous.as_ref().filter(|_| sibling).map(|held| rtdb_types::Iid(*held.world.as_bytes())),
                cut, requested, deadline,
                attempt: attempt.into(), fingerprint: *fingerprint, controls, root: publish_root.then_some(root),
            },
        ).await.map_err(status)?;
        if registered.as_bytes() != world { return Err(unsupported()); }
        let mut reply = Vec::with_capacity(32);
        reply.extend_from_slice(registered.as_bytes());
        reply.extend_from_slice(&key.tx_id.0.to_be_bytes());
        reply.extend_from_slice(&key.system_time.0.to_be_bytes());
        Ok(ActionResult { body: wire::retain(Bytes::from(reply), reply_charge) })
    }

    pub(crate) async fn decide_held_worlds_action(&self, call: &Call, envelope: &Bytes) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let host = self.0.config.owned_world_host.as_ref().ok_or_else(unsupported)?;
        let policy = self.hosted_control_policy(call)?;
        let ([flags], body) = envelope.split_first_chunk::<1>().ok_or_else(|| invalid("Missing world decision flags"))?;
        if *flags & !7 != 0 { return Err(invalid("Invalid world decision flags")); }
        let (slot, body) = if *flags & 1 != 0 {
            let (slot, body) = self.owned_clock_slot(call, body).await?;
            (Some(slot), body)
        } else {
            (None, body)
        };
        let (deadline, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Missing world batch deadline"))?;
        let deadline = Timestamp(i64::from_be_bytes(*deadline));
        let (fingerprint, body) = body.split_first_chunk::<32>().ok_or_else(|| invalid("Missing world batch fingerprint"))?;
        let (count, mut body) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Missing world batch count"))?;
        let count = usize::from(u16::from_be_bytes(*count));
        if !(1..=8).contains(&count) { return Err(invalid("A world batch requires one to eight worlds")); }
        let _framing = call.reserve((count + 2) * MAX_COMMIT_GROUPS * 512)?;
        let mut release_eligible = count == 1;
        let mut proposal_eligible = count == 1;
        let mut terminal_proposal = false;
        let mut landing_eligible = count == 1;
        let mut close_eligible = count == 1;
        let mut worlds = BTreeSet::new();
        let mut attempts = BTreeSet::new();
        let mut sections = Vec::with_capacity(count);
        let mut held = Vec::with_capacity(count);
        let mut reservations = Vec::new();
        let mut rows_left = MAX_GROUPED_COMMIT_ROWS - 2 - usize::from(slot.is_some());
        for _ in 0..count {
            let (world, rest) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Missing world batch identity"))?;
            let world = Uuid::from_bytes(*world);
            if world.is_nil() || !worlds.insert(world) { return Err(invalid("Invalid or repeated world batch identity")); }
            let (handle, rest) = rest.split_first_chunk::<16>().ok_or_else(|| invalid("Missing world batch held handle"))?;
            let (count, rest) = rest.split_first_chunk::<2>().ok_or_else(|| invalid("Missing world batch witness count"))?;
            let count = usize::from(u16::from_be_bytes(*count));
            if count > MAX_WORLD_WITNESSES { return Err(invalid("Invalid world batch witness count")); }
            let (nonces, rest) = rest.split_at_checked(count * 16).ok_or_else(|| invalid("Incomplete world batch witnesses"))?;
            let (attempt, rest) = rest.split_first_chunk::<22>().ok_or_else(|| invalid("Missing world batch attempt identity"))?;
            let attempt = std::str::from_utf8(attempt).map_err(|_| invalid("Invalid world batch attempt identity"))?;
            if !attempt.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) || !attempts.insert(attempt) {
                return Err(invalid("Invalid or repeated world batch attempt identity"));
            }
            let ([kind], rest) = rest.split_first_chunk::<1>().ok_or_else(|| invalid("Missing world publication effect"))?;
            rows_left = rows_left.checked_sub(count * 2 + 2).ok_or_else(|| invalid("World batch exceeds its effect limit"))?;
            let resource = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
            if resource.snapshot.ancestor().is_some() && *kind != 2 {
                return Err(unsupported());
            }
            let (checks, charge) = resource.conditions(call, world, nonces)?;
            reservations.push(charge);
            let parent_basis = resource.snapshot.parent_basis();
            let parent_pin = resource.snapshot.parent_pin();
            let expected_clock = resource.snapshot.application_clock();
            let mode = resource.snapshot.clock_mode();
            close_eligible &= mode == rtdb_types::WorldClockMode::Proposal && *kind == 2 && count == 0;
            landing_eligible &= mode == rtdb_types::WorldClockMode::Proposal
                && *kind == 2 && count != 0 && expected_clock.is_some();
            release_eligible &= mode == rtdb_types::WorldClockMode::Retained
                && *kind == 2 && count == 0;
            proposal_eligible &= mode == rtdb_types::WorldClockMode::Proposal
                && ((*kind == 0 && count != 0) || *kind == 3);
            terminal_proposal |= *kind == 3;
            let standing_drop = *kind == 2 && mode != rtdb_types::WorldClockMode::Proposal;
            if count == 0 && !matches!(*kind, 0 | 1 | 4) && !standing_drop
                && !close_eligible && !(proposal_eligible && terminal_proposal)
            {
                return Err(invalid("This world effect requires original witnesses"));
            }
            if (*kind == 0 && !mode.is_writable())
                || (matches!(*kind, 1 | 4) && !matches!(mode, rtdb_types::WorldClockMode::Held { .. }))
            {
                return Err(Status::failed_precondition("The world clock mode does not admit this effect"));
            }
            let (effect, rest) = match *kind {
                0 | 4 => {
                    let (at, rest) = if *kind == 4 {
                        let (at, rest) = rest.split_first_chunk::<8>()
                            .ok_or_else(|| invalid("Missing held overlay reading"))?;
                        (Some(Timestamp(i64::from_be_bytes(*at))), rest)
                    } else {
                        (None, rest)
                    };
                    let (length, rest) = rest.split_first_chunk::<4>().ok_or_else(|| invalid("Missing world overlay length"))?;
                    let (overlay, rest) = rest.split_at_checked(u32::from_be_bytes(*length) as usize)
                        .ok_or_else(|| invalid("Incomplete world overlay"))?;
                    let mut writes = Vec::new();
                    self.decode_world_writes(call, owned_groups(overlay, true, false)?,
                        mode == rtdb_types::WorldClockMode::Proposal, &mut rows_left, &mut writes, &mut reservations)?;
                    if writes.is_empty() { return Err(invalid("The world overlay has no writes")); }
                    for write in writes.iter().filter(|write| write.create_only) {
                        let absence_at = match mode {
                            rtdb_types::WorldClockMode::Held { .. } =>
                                Some(expected_clock.unwrap_or(resource.snapshot.initial_clock())),
                            rtdb_types::WorldClockMode::Proposal => None,
                            rtdb_types::WorldClockMode::Retained => return Err(unsupported()),
                        };
                        resource.require_create_absence(nonces, write, absence_at)?;
                    }
                    let effect = match at {
                        Some(at) => rtdb_node::OwnedWorldEffect::TimedOverlay { at, writes },
                        None => rtdb_node::OwnedWorldEffect::Overlay(writes),
                    };
                    (effect, rest)
                }
                1 => {
                    let (target, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("Missing world clock target"))?;
                    (rtdb_node::OwnedWorldEffect::Clock(Timestamp(i64::from_be_bytes(*target))), rest)
                }
                2 => (rtdb_node::OwnedWorldEffect::Drop, rest),
                3 => (rtdb_node::OwnedWorldEffect::SettleProposal, rest),
                _ => return Err(invalid("Unknown world publication effect")),
            };
            held.push(resource);
            body = rest;
            sections.push(rtdb_node::OwnedWorldSection {
                world: rtdb_types::Iid(*world.as_bytes()), parent_pin, parent_basis, checks, expected_clock,
                attempt: attempt.to_owned(), effect,
            });
        }
        let (mut observations, body) = if let Some(body) = body.strip_prefix(&[0, 0]) {
            (Vec::new(), body)
        } else {
            self.hosted_world_read_prefix(call, body)?
        };
        if count + observations.len() > 8 || observations.iter().any(|read| worlds.contains(&read.held.world)) {
            return Err(invalid("World batch observations must fit the batch and use independent worlds"));
        }
        let observed_ops = observations.iter()
            .map(|read| read.witnesses.len() * 2 + usize::from(read.clock.is_some())).sum::<usize>();
        rows_left = rows_left.checked_sub(observed_ops).ok_or_else(|| invalid("World batch exceeds its effect limit"))?;
        let (counts, body, _counts) = if *flags & 2 != 0 {
            let world = (sections.len() == 1).then(|| sections[0].world);
            owned_count_conditions(call, body, world, landing_eligible && slot.is_some())?
        } else { (Vec::new(), body, Vec::new()) };
        rows_left = rows_left.checked_sub(counts.len())
            .ok_or_else(|| invalid("World batch exceeds its effect limit"))?;
        let ([root_kind], body) = body.split_first_chunk::<1>().ok_or_else(|| invalid("Missing Root section kind"))?;
        if !observations.is_empty() && *root_kind >= 3 {
            return Err(invalid("Independent observers require an ordinary world batch"));
        }
        let (root_read, body) = match *root_kind {
            0 => (None, body),
            1 | 2 | 3 | 4 | 5 | 6 | 7 => {
                let (handle, body) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Missing original Root hold"))?;
                let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
                held.snapshot.require_unscoped_root().map_err(status)?;
                (Some(held), body)
            }
            _ => return Err(invalid("Invalid Root section kind")),
        };
        let delete_root = *root_kind == 4;
        let proposal_overlay = *root_kind == 5;
        let proposal_land = *root_kind == 6;
        if !proposal_land && counts.iter().any(|condition| matches!(
            condition, TxOp::OwnedCheckCapacity { at_most, .. } if *at_most < 0
        )) {
            return Err(invalid("Only reviewed landing can carry a signed Root count bound"));
        }
        let proposal_close = *root_kind == 7;
        let (root_attempt, body) = if delete_root || proposal_land || proposal_close {
            if (delete_root && !release_eligible) || (proposal_land && !landing_eligible) || (proposal_close != close_eligible) {
                return Err(invalid("The terminal world does not admit this Root effect"));
            }
            rows_left = rows_left.checked_sub(1).ok_or_else(|| invalid("World batch exceeds its effect limit"))?;
            (Some(sections[0].attempt.as_str()), body)
        } else if *root_kind == 2 {
            let (attempt, body) = body.split_first_chunk::<22>().ok_or_else(|| invalid("Missing Root attempt identity"))?;
            let attempt = std::str::from_utf8(attempt).map_err(|_| invalid("Invalid Root attempt identity"))?;
            if !attempt.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) || !attempts.insert(attempt) {
                return Err(invalid("Invalid or repeated Root attempt identity"));
            }
            rows_left = rows_left.checked_sub(3).ok_or_else(|| invalid("World batch exceeds its effect limit"))?;
            (Some(attempt), body)
        } else {
            (None, body)
        };
        let (root_witnesses, body, _root_witness_charge) = self.owned_witness_prefix(call, body, true)?;
        let (root_length, body) = body.split_first_chunk::<4>().ok_or_else(|| invalid("Missing Root section length"))?;
        let (root, body) = body.split_at_checked(u32::from_be_bytes(*root_length) as usize)
            .ok_or_else(|| invalid("Incomplete Root groups"))?;
        let _root_framing = call.reserve(ordered_group_count(root)? * 512)?;
        let mut root_ops = Vec::new();
        let _root_groups = self.decode_owned_commit_groups(
            call, envelope, owned_groups(root, false, true)?, &mut root_ops, &mut rows_left, CommitEffects::Facts,
        )?;
        let mut root = rtdb_node::OwnedRootSection { checks: counts, ..Default::default() };
        let mut root_writes = Vec::new();
        for operation in root_ops {
            match &operation {
                TxOp::OwnedCheck { relation, .. } if relation.world().is_none() && !relation.is_world_catalog() =>
                    root.checks.push(operation),
                _ if root_attempt.is_some() && rtdb_tx::is_root_partition_frontier(&operation) =>
                    root_writes.push(operation),
                _ if proposal_land && rtdb_tx::is_proposal_landing_frontier(&operation) =>
                    root_writes.push(operation),
                TxOp::OwnedInsertDocs { relation, .. } | TxOp::OwnedReplaceDocs { relation, .. }
                | TxOp::OwnedArrowDocs { relation, .. } | TxOp::OwnedRelativePeriod { relation, .. } | TxOp::OwnedDelete { relation, .. }
                    if root_attempt.is_some() && relation.is_root_data() =>
                    root_writes.push(operation),
                _ => return Err(invalid("Operation is outside the held Root section")),
            }
        }
        if let Some(attempt) = root_attempt {
            if root_writes.is_empty() { return Err(invalid("A Root publication requires fact writes")); }
            let facts = root_writes.iter().filter(|op| !rtdb_tx::is_root_partition_frontier(op)).collect::<Vec<_>>();
            if (delete_root || proposal_close) && !matches!(facts.as_slice(), [
                TxOp::OwnedDelete { valid_from: None, valid_to: None, .. }
            ]) {
                return Err(invalid("A retained drop supports one acceptance-valid Root delete"));
            }
            root.publication = Some(rtdb_node::OwnedRootPublication { attempt: attempt.to_owned(), writes: root_writes });
        }
        root.proposal_overlay = proposal_overlay;
        root.clock_slot = slot;
        root.proposal_land = proposal_land;
        root.proposal_close = proposal_close;
        if (proposal_overlay && !proposal_eligible) || (terminal_proposal && !proposal_overlay) {
            return Err(invalid("Registered settlement requires one witnessed Proposal overlay"));
        }
        let observed_root = !root.checks.is_empty() || !root_witnesses.is_empty();
        let release = *root_kind == 3;
        if (*root_kind == 0 && observed_root) || (*root_kind == 1 && !observed_root)
            || ((release || delete_root) && !release_eligible) || (release && observed_root)
            || (proposal_close != close_eligible) || ((delete_root || proposal_land || proposal_close) && !observed_root)
        {
            return Err(invalid("The Root section kind disagrees with its observations"));
        }
        rows_left = rows_left.checked_sub(root_witnesses.len()).ok_or_else(|| invalid("World batch exceeds its effect limit"))?;
        let (refusal, body) = if *flags & 4 != 0 {
            let (length, body) = body.split_first_chunk::<4>().ok_or_else(|| invalid("Missing landing refusal length"))?;
            let (refusal, body) = body.split_at_checked(u32::from_be_bytes(*length) as usize)
                .ok_or_else(|| invalid("Incomplete landing refusal"))?;
            let refusal = owned_groups(refusal, true, false)?;
            if refusal.is_empty() || !(proposal_overlay || delete_root || proposal_land || proposal_close) {
                return Err(invalid("The world decision has no matching refusal alternative"));
            }
            (refusal, body)
        } else {
            (Vec::new(), body)
        };
        let controls = owned_groups(body, true, false)?;
        hosted_control::admit_control_targets(policy, controls.iter().map(|(relation, _)| relation))?;
        let scope = self.hosted_control_scope(call).await?;
        let (controls, _charges) = self.decode_hosted_control_groups(call, controls, scope, &mut rows_left, !release && !delete_root && !proposal_overlay && !proposal_land && !proposal_close)?;
        let _refusal_charge = if !refusal.is_empty() {
            hosted_control::admit_control_targets(policy, refusal.iter().map(|(relation, _)| relation))?;
            let (refusal, charge) = self.decode_hosted_control_groups(call, refusal, scope, &mut rows_left, false)?;
            if proposal_land && refusal.len() != 2 {
                return Err(invalid("A landing refusal requires its original receipt and proof"));
            }
            root.refusal = refusal;
            Some(charge)
        } else {
            None
        };
        if proposal_overlay && !rtdb_tx::proposal_overlay_controls(&controls) {
            return Err(invalid("A Proposal overlay requires checked hosted settlement"));
        }
        if proposal_close && !rtdb_tx::proposal_close_controls(&controls) {
            return Err(invalid("A Proposal holder close requires checked custody settlement"));
        }
        if proposal_land && !rtdb_tx::proposal_land_controls(&controls) {
            return Err(invalid("A Proposal landing requires its replay head, receipt and proof"));
        }
        if delete_root && !rtdb_tx::retained_delete_controls(&controls) {
            return Err(invalid("A Root delete with retained drop requires checked owner settlement"));
        }
        if release && !rtdb_tx::retained_release_controls(&controls) {
            return Err(invalid("A retained drop needs original checks and existing owner replacements"));
        }
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let attachment = attachment.ok_or_else(unsupported)?;
        if let Some(tick) = slot {
            self.wait_for_clock_slot(call, tick).await?;
        }
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        require_idle(&session)?;
        for resource in &held {
            resource.snapshot.check().map_err(status)?;
            if (terminal_proposal || proposal_close) && deadline > resource.snapshot.evidence_until() {
                return Err(Status::failed_precondition("Proposal hold is outside the settlement deadline"));
            }
        }
        if let Some(read) = &root_read {
            read.snapshot.check().map_err(status)?;
            if read.snapshot.present().system_time >= deadline
                || ((delete_root || proposal_overlay || proposal_land || proposal_close) && (deadline > read.snapshot.evidence_until()
                    || read.snapshot.fork_cut(scope).await.map_err(status)?.live().is_none()))
            {
                return Err(Status::failed_precondition("Root hold is outside the world batch deadline"));
            }
        }
        for (original, evidence) in &root_witnesses {
            if root_read.as_ref().is_none_or(|read| !Arc::ptr_eq(read, original)) || !evidence.matches_scope(Some(scope)) {
                return Err(Status::failed_precondition("Root range witness belongs to another hold or placement"));
            }
            root.checks.push(evidence.clone().operation());
        }
        let observed = take_held_world_observations(&mut observations)?;
        let committed = host.commit_world_batch(
            crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
            sections, root, observed, *fingerprint, controls, deadline,
        ).await.map_err(status)?;
        drop(observations);
        self.owned_tx_reply(committed)
    }

    fn decode_world_writes(
        &self, call: &Call, groups: Vec<(OwnedRelation, &[u8])>, proposal: bool, rows_left: &mut usize,
        writes: &mut Vec<rtdb_node::OwnedWorldWrite>, reservations: &mut Vec<Arc<MemoryReservation>>,
    ) -> Result<(), Status> {
        use rtdb_node::OwnedWorldWrite;
        use rtdb_types::{Interval, WorldShadow};

        for (relation, arrow) in groups {
            let batch = request_batch(call, arrow)?;
            let batch = batch.batch();
            let schema = batch.schema_ref();
            let field = |name| schema.column_with_name(name).map(|(_, field)| field.data_type());
            if schema.fields().len() != 5 || field("_op") != Some(&DataType::Utf8)
                || field("_valid_from_micros") != Some(&DataType::Int64)
                || field("_valid_to_micros") != Some(&DataType::Int64)
                || !matches!(field("fact"), Some(DataType::Struct(_)))
            {
                return Err(invalid("Owned world commit requires _id, _op, two Int64 valid endpoints, and a fact struct"));
            }
            *rows_left = rows_left.checked_sub(batch.num_rows())
                .ok_or_else(|| invalid("Owned world commit exceeds its total row limit"))?;
            let charge = call.reserve(wire::mul(batch.num_rows(), std::mem::size_of::<OwnedWorldWrite>() + 512)?)?;
            writes.try_reserve_exact(batch.num_rows()).map_err(|_| Status::resource_exhausted("Owned world commit allocation failed"))?;
            let (rows, decoded) = upload::owned_documents(batch, call.memory())?;
            reservations.extend([charge, decoded]);
            for mut row in rows {
                let id = row.remove("_id").ok_or_else(|| invalid("Owned world write has no _id"))?.entity_id().map_err(status)?;
                let (Some(Value::Utf8(operation)), Some(from), Some(to), Some(payload)) = (
                    row.remove("_op"), row.remove("_valid_from_micros"), row.remove("_valid_to_micros"), row.remove("fact"),
                ) else { return Err(invalid("Owned world write requires a typed operation and explicit valid endpoints")); };
                let valid = match (optional_micros(Some(from))?, optional_micros(Some(to))?) {
                    (None, None) => None,
                    (Some(from), Some(to)) => {
                        let valid = Interval::new(from, to).map_err(status)?;
                        if valid.is_empty() { return Err(invalid("Owned world write needs a nonempty valid interval")); }
                        Some(valid)
                    }
                    _ => return Err(invalid("Owned world write has only one valid endpoint")),
                };
                let create_only = operation == "insert";
                if create_only && valid.is_some() {
                    return Err(invalid("A create-only Proposal effect uses acceptance validity"));
                }
                let mut ordered_keys = None;
                let shadow = match (operation.as_str(), payload) {
                    ("delete", Value::Null) => WorldShadow::Deleted,
                    ("replace" | "insert", Value::Struct(mut fields)) => {
                        strip_owned_absence(&mut fields)?;
                        if let Some(slots) = fields.remove("_ordered_micros") {
                            ordered_keys = match slots {
                                Value::Binary(slots) if proposal => Some(slots),
                                Value::Undefined | Value::Null => None,
                                _ => return Err(invalid("Only an admitted Proposal can place ordered keys")),
                            };
                        }
                        if fields.keys().any(|name| name.starts_with('_')) {
                            return Err(invalid("Owned world fact contains a reserved field"));
                        }
                        let mut fact = StructFields::from([("_id".into(), id.as_value())]);
                        fact.extend(fields);
                        WorldShadow::Present(fact)
                    }
                    _ => return Err(invalid("Owned world writes require a complete fact or a null deletion")),
                };
                writes.push(OwnedWorldWrite { relation: relation.clone(), id, shadow, valid, create_only, ordered_keys });
            }
        }
        Ok(())
    }

    pub(crate) async fn read_owned_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, id, _, _identity) = point_request(call, body, PointTime::Default)?;
        let connection = self.connection(call, &call.database).await?;
        let mut native = connection.borrow().await?;
        require_idle(&native)?;
        let snapshot = native.open_owned_read().await.map_err(status)?;
        self.execute_owned_point(&snapshot, &relation, &id, None, false, None).await
    }

    async fn execute_owned_point(
        &self, snapshot: &OwnedReadSession, relation: &OwnedRelation, id: &EntityId, valid: Option<Timestamp>,
        fixed: bool, origin: Option<(&Call, &OwnedReadResource)>,
    ) -> Result<ActionResult, Status> {
        let present = snapshot.system_time();
        let lifetime = snapshot.lifetime_point_policy(relation).await.map_err(status)?;
        let cursor = snapshot.open_point_at(relation, id, valid).await.map_err(status)?;
        if fixed && lifetime.is_some() {
            return Err(invalid("An owned lifetime point requires its ordered present"));
        }
        let evidence = if lifetime.is_none() && origin.is_some() && (fixed || snapshot.is_scoped_root()) {
            let region = if fixed {
                Interval::at(valid.ok_or_else(|| invalid("An owned fixed point requires a valid instant"))?).map_err(status)?
            } else {
                Interval::ALL
            };
            snapshot.entity_region_witness(relation, id, region).await.map_err(status)?
        } else {
            None
        };
        let witness = |expiry| origin.map(|(call, held)| {
            let evidence = if lifetime.is_some() {
                snapshot.scoped_lifetime_point_witness(relation, expiry).map_err(status)?
            } else {
                evidence
            };
            evidence.map(|evidence| held.remember(call, relation, evidence)).transpose()
        }).transpose().map(Option::flatten);
        let memory = origin.map_or(self.memory(), |(call, _)| call.memory());
        let reply = self.finish_owned_point_inner(memory, present, valid.unwrap_or(present), id, cursor, lifetime, None, witness).await?;
        snapshot.check_live_point_relation(relation).map_err(status)?;
        Ok(reply)
    }

    async fn finish_owned_point(&self, call: &Call, present: Timestamp, valid: Timestamp, id: &EntityId, result: Option<QueryCursor>, lifetime: Option<rtdb_types::OwnedLifetimeFloor>) -> Result<ActionResult, Status> {
        self.finish_owned_point_inner(call.memory(), present, valid, id, result, lifetime, None, |_| Ok(None)).await
    }

    async fn finish_owned_point_inner(
        &self, memory: &Arc<MemoryBudget>, present: Timestamp, valid: Timestamp, id: &EntityId, result: Option<QueryCursor>,
        lifetime: Option<rtdb_types::OwnedLifetimeFloor>, path_columns: Option<&[String]>,
        witness: impl FnOnce(Option<Timestamp>) -> Result<Option<Uuid>, Status>,
    ) -> Result<ActionResult, Status> {
        let Some(mut cursor) = result else {
            let schema = with_point_witness(&Schema::empty(), witness(None)?);
            return self.owned_point_reply_in(memory, &schema, None, present, valid, None, None);
        };
        let mut result = None;
        while let Some(batch) = cursor.next_batch().await.map_err(status)? {
            if batch.batch().num_rows() == 0 { continue; }
            if result.is_some() || batch.batch().num_rows() != 1 {
                return Err(Status::internal("Owned point lookup returned multiple rows"));
            }
            result = Some(batch);
        }
        if let Some(batch) = result {
            let revision = rtdb_query::owned_row_revision(batch.batch(), 0, id, memory).map_err(status)?;
            let until = lifetime.map(|lifetime| {
                let (created, expires) = rtdb_query::owned_row_lifetime(batch.batch(), 0, revision, lifetime.duration).map_err(status)?;
                if created <= lifetime.expired_created_through || revision > present || present >= expires {
                    return Err(Status::failed_precondition("The retained point is outside its creation lifetime"));
                }
                Ok(expires)
            }).transpose()?;
            let projected = if let Some(path_columns) = path_columns {
                let schema = batch.batch().schema_ref();
                let mut fields = vec![schema.index_of("_id").map_err(arrow)?];
                for name in path_columns.iter().filter(|name| name.as_str() != "_id") {
                    fields.push(schema.index_of(name).map_err(arrow)?);
                }
                Some(batch.batch().project(&fields).map_err(arrow)?)
            } else if until.is_some() {
                let fields = batch.batch().schema_ref().fields().iter().enumerate()
                    .filter(|(_, field)| !matches!(field.name().as_str(), "_valid_from" | "_valid_to"))
                    .map(|(index, _)| index).collect::<Vec<_>>();
                Some(batch.batch().project(&fields).map_err(arrow)?)
            } else { None };
            let rows = projected.as_ref().unwrap_or_else(|| batch.batch());
            let schema = with_point_witness(rows.schema_ref(), witness(until)?);
            let reply = self.owned_point_reply_in(memory, &schema, Some(rows), present, valid, Some(revision), until)?;
            if until.is_some_and(|until| until <= self.0.node.now()) {
                return Err(Status::failed_precondition("The held point reached its creation lifetime"));
            }
            Ok(reply)
        } else {
            let schema = if path_columns.is_some() { wire::OwnedSchema::empty(memory)? }
                else { wire::plan_schema(cursor.runtime_schema(), memory)? };
            let schema = with_point_witness(&schema, witness(None)?);
            self.owned_point_reply_in(memory, &schema, None, present, valid, None, None)
        }
    }

    fn owned_point_reply(&self, schema: &Schema, batch: Option<&RecordBatch>, present: Timestamp, valid: Timestamp, revision: Option<Timestamp>) -> Result<ActionResult, Status> {
        self.owned_point_reply_until(schema, batch, present, valid, revision, None)
    }

    fn owned_point_reply_until(&self, schema: &Schema, batch: Option<&RecordBatch>, present: Timestamp, valid: Timestamp, revision: Option<Timestamp>, until: Option<Timestamp>) -> Result<ActionResult, Status> {
        self.owned_point_reply_in(self.memory(), schema, batch, present, valid, revision, until)
    }

    fn owned_point_reply_in(
        &self, memory: &Arc<MemoryBudget>, schema: &Schema, batch: Option<&RecordBatch>,
        present: Timestamp, valid: Timestamp, revision: Option<Timestamp>, until: Option<Timestamp>,
    ) -> Result<ActionResult, Status> {
        let _metadata = memory.reserve(wire::schema_bound(schema)?).map_err(status)?;
        let mut metadata = schema.metadata().clone();
        metadata.insert(OWNED_PRESENT.into(), present.0.to_string());
        metadata.insert(OWNED_VALID.into(), valid.0.to_string());
        if let Some(revision) = revision { metadata.insert(OWNED_REVISION.into(), revision.0.to_string()); }
        if let Some(until) = until { metadata.insert("rtdb.owned.until_micros".into(), until.0.to_string()); }
        if let Some(batch) = batch.filter(|batch| batch.column_by_name("_assertion_ordinal").is_some()) {
            if batch.num_rows() != 1 {
                return Err(Status::internal("Owned point has an invalid assertion ordinal"));
            }
            if let Some(ordinal) = rtdb_query::owned_row_assertion_ordinal(batch, 0).map_err(status)? {
                metadata.insert(OWNED_ASSERTION_ORDINAL.into(), ordinal.to_string());
            }
        }
        let columns: Vec<_> = schema.fields().iter().enumerate()
            .filter(|(_, field)| !matches!(field.name().as_str(), "_system_from" | "_assertion_ordinal"))
            .map(|(index, _)| index).collect();
        let schema = Arc::new(schema.project(&columns).map_err(arrow)?.with_metadata(metadata));
        let batch = batch.map(|batch| RecordBatch::try_new(Arc::clone(&schema), columns.iter().map(|&index| Arc::clone(batch.column(index))).collect()))
            .transpose().map_err(arrow)?;
        self.owned_stream_reply_in(memory, &schema, batch.as_ref().map(std::slice::from_ref).unwrap_or_default())
    }

    fn owned_action_reply(&self, schema: &Schema, batch: Option<&RecordBatch>) -> Result<ActionResult, Status> {
        self.owned_stream_reply(schema, batch.map(std::slice::from_ref).unwrap_or_default())
    }

    fn owned_stream_reply(&self, schema: &Schema, pages: &[RecordBatch]) -> Result<ActionResult, Status> {
        self.owned_stream_reply_in(self.memory(), schema, pages)
    }

    fn owned_stream_reply_in(&self, memory: &Arc<MemoryBudget>, schema: &Schema, pages: &[RecordBatch]) -> Result<ActionResult, Status> {
        let bytes = pages.iter().try_fold(0, |bytes, batch| wire::add(bytes, batch.get_array_memory_size()))?;
        let bound = wire::add(wire::schema_bound(schema)?, wire::mul(bytes, 8)?)?;
        let _encoding = memory.reserve(bound).map_err(status)?;
        let bytes = write_arrow_stream(schema, pages, IpcWriteOptions::default()).map_err(status)?;
        let reply = ActionResult { body: bytes.into() };
        if reply.encoded_len() > self.0.config.maximum_message_bytes {
            return Err(Status::resource_exhausted("Owned action response exceeds the Flight message limit"));
        }
        let retained = memory.reserve(reply.body.len()).map_err(status)?;
        Ok(ActionResult { body: wire::retain(reply.body, retained) })
    }
}

fn with_point_witness(schema: &Schema, nonce: Option<Uuid>) -> Schema {
    let Some(nonce) = nonce else { return schema.clone(); };
    let mut metadata = schema.metadata().clone();
    metadata.insert(OWNED_RANGE_WITNESS.into(), nonce.simple().to_string());
    schema.clone().with_metadata(metadata)
}

#[derive(Clone, Copy)]
pub(crate) enum PointTime { Default, Optional, Required }

fn point_request(
    call: &Call, body: &[u8], time: PointTime,
) -> Result<(OwnedRelation, EntityId, Option<Timestamp>, Arc<MemoryReservation>), Status> {
    let (relation, arrow) = split_relation(body)?;
    let batch = request_batch(call, arrow)?;
    let batch = batch.batch();
    let expected_columns = if matches!(time, PointTime::Default) { 1 } else { 2 };
    if batch.num_columns() != expected_columns || batch.num_rows() != 1 {
        return Err(invalid("Owned point action requires one row with the fields for its action version"));
    }
    let (id, identity) = point_identity(call, batch)?;
    let valid = if !matches!(time, PointTime::Default) {
        let (column, field) = batch.schema_ref().column_with_name("valid_micros")
            .ok_or_else(|| invalid("Owned point requires valid_micros"))?;
        if field.data_type() != &DataType::Int64 || !field.metadata().is_empty()
            || (matches!(time, PointTime::Optional) && !field.is_nullable())
        {
            return Err(invalid("Owned point valid_micros has an invalid Int64 field"));
        }
        let values = batch.column(column).as_any().downcast_ref::<Int64Array>()
            .ok_or_else(|| invalid("Owned point valid_micros must be Int64"))?;
        let valid = (!values.is_null(0)).then(|| Timestamp(values.value(0)));
        if matches!(time, PointTime::Required) && valid.is_none() {
            return Err(invalid("Owned world point requires a valid_micros instant"));
        }
        valid
    } else {
        None
    };
    Ok((relation, id, valid, identity))
}

fn change_region_request(
    call: &Call, request: &[u8],
) -> Result<(Option<EntityId>, Option<EntityId>, Interval, Arc<MemoryReservation>), Status> {
    let (from, tail) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Owned change region has no valid start"))?;
    let (to, tail) = tail.split_first_chunk::<8>().ok_or_else(|| invalid("Owned change region has no valid end"))?;
    let (lower, tail) = count_component(tail, 4096)?;
    let (upper, tail) = count_component(tail, 4096)?;
    if !tail.is_empty() {
        return Err(invalid("Owned change region has trailing bytes"));
    }
    let valid = Interval::from_micros(i64::from_be_bytes(*from), i64::from_be_bytes(*to)).map_err(status)?;
    let charge = call.reserve(8192 + lower.len() + upper.len())?;
    let bound = |bytes: &[u8]| (!bytes.is_empty()).then(|| EntityId::from_handle_bytes(bytes)).transpose().map_err(status);
    let (lower, upper) = (bound(lower)?, bound(upper)?);
    if valid.is_empty() || lower.as_ref().zip(upper.as_ref()).is_some_and(|(lower, upper)| lower >= upper) {
        return Err(invalid("Owned change region must have nonempty key and valid bounds"));
    }
    Ok((lower, upper, valid, charge))
}

fn point_identity(call: &Call, batch: &RecordBatch) -> Result<(EntityId, Arc<MemoryReservation>), Status> {
    let (column, field) = batch.schema_ref().column_with_name("_id").ok_or_else(|| invalid("Owned point has no _id field"))?;
    let values = batch.column(column).as_ref();
    let identity = call.reserve(wire::add(4096, rtdb_columnar::estimate_read_value_bytes(field, values, 0).map_err(status)?)?)?;
    let id = rtdb_columnar::read_execution_value(field, values, 0).map_err(status)?.entity_id().map_err(status)?;
    Ok((id, identity))
}

fn owned_version_header(next: u64, done: bool, floor: Timestamp, scanned: usize) -> Result<Vec<u8>, Status> {
    let scanned = u16::try_from(scanned).map_err(|_| invalid("Owned version scan count is out of range"))?;
    let mut header = Vec::with_capacity(35);
    header.extend_from_slice(&next.to_be_bytes());
    header.push(u8::from(done));
    header.extend_from_slice(&floor.0.to_be_bytes());
    header.extend_from_slice(&scanned.to_be_bytes());
    Ok(header)
}

fn history_request_endpoint(batch: &RecordBatch, name: &str) -> Result<Timestamp, Status> {
    history_request_integer(batch, name).map(Timestamp)
}

fn history_request_integer(batch: &RecordBatch, name: &str) -> Result<i64, Status> {
    let (column, field) = batch.schema_ref().column_with_name(name)
        .ok_or_else(|| invalid("Owned history is missing a required integer field"))?;
    if field.data_type() != &DataType::Int64 || !field.metadata().is_empty() {
        return Err(invalid("Owned history integer fields must be Int64"));
    }
    let values = batch.column(column).as_any().downcast_ref::<Int64Array>()
        .ok_or_else(|| invalid("Owned history integer fields must be Int64"))?;
    if values.is_null(0) { return Err(invalid("Owned history integer fields must be non-null")); }
    Ok(values.value(0))
}

fn history_row_time(batch: &RecordBatch, name: &str, row: usize) -> Result<Option<Timestamp>, Status> {
    let (column, _) = batch.schema_ref().column_with_name(name).ok_or_else(history_invalid)?;
    let values = batch.column(column).as_any().downcast_ref::<TimestampMicrosecondArray>().ok_or_else(history_invalid)?;
    Ok((!values.is_null(row)).then(|| Timestamp(values.value(row))))
}

fn history_invalid() -> Status {
    Status::internal("Owned history returned invalid or overlapping slices")
}

fn optional_bound(cell: ArrowCell<'_>) -> Result<Option<ArrowCell<'_>>, Status> {
    Ok((!cell.is_null().map_err(status)?).then_some(cell))
}

fn owned_payload_columns(batch: &RecordBatch, row: usize, controls: &[&str]) -> Result<Vec<usize>, Status> {
    let absent = ArrowCell::named(batch, "_absent", row).map(optional_bound).transpose()?.flatten();
    let mut omitted = BTreeSet::new();
    if let Some(absent) = absent {
        let fields = absent.list().filter(|fields| fields.len() <= 128)
            .ok_or_else(|| invalid("Owned _absent must be a list of at most 128 field names"))?;
        for field in fields {
            let name = field.utf8().ok_or_else(|| invalid("Owned _absent field must be text"))?;
            let value = ArrowCell::named(batch, name, row)
                .ok_or_else(|| invalid("Owned _absent may name only distinct null payload cells"))?;
            if name.starts_with('_') || !value.is_null().map_err(status)? || !omitted.insert(name) {
                return Err(invalid("Owned _absent may name only distinct null payload cells"));
            }
        }
    }
    let mut positions = Vec::new();
    for (position, field) in batch.schema_ref().fields().iter().enumerate() {
        let value = ArrowCell { field, array: batch.column(position).as_ref(), row };
        if field.name() != "_absent" && !controls.contains(&field.name().as_str())
            && !omitted.contains(field.name().as_str()) && !value.is_undefined().map_err(status)?
        {
            positions.push(position);
        }
    }
    Ok(positions)
}

fn strip_owned_absence(row: &mut rtdb_types::StructFields<Value>) -> Result<(), Status> {
    let absent = match row.remove("_absent") {
        None | Some(Value::Null | Value::Undefined) => return Ok(()),
        Some(Value::List(fields)) if fields.len() <= 128 => fields,
        _ => return Err(invalid("Owned _absent must be a list of at most 128 field names")),
    };
    for field in absent.iter() {
        let Value::Utf8(name) = field.as_ref() else { return Err(invalid("Owned _absent field must be text")); };
        if name.starts_with('_') || !matches!(row.get(name.as_str()), Some(Value::Null | Value::Undefined)) {
            return Err(invalid("Owned _absent may name only distinct null payload cells"));
        }
        row.remove(name.as_str());
    }
    Ok(())
}

fn optional_micros(value: Option<Value>) -> Result<Option<Timestamp>, Status> {
    match value {
        Some(Value::I64(time)) => Ok(Some(Timestamp(time))),
        None | Some(Value::Null | Value::Undefined) => Ok(None),
        _ => Err(invalid("Owned observation requires signed microsecond integers")),
    }
}

fn valid_endpoint(value: Option<Value>) -> Result<Option<Timestamp>, Status> {
    match value {
        Some(Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some(_) }) => Ok(Some(Timestamp(value))),
        None | Some(Value::Null | Value::Undefined) => Ok(None),
        _ => Err(invalid("Owned mutation validity bounds must be zoned microsecond timestamps")),
    }
}

fn has_payload(row: &rtdb_types::StructFields<Value>) -> bool {
    row.iter().any(|(name, value)| name != "_id" && !matches!(value, Value::Null | Value::Undefined))
}

fn owned_capacity_declaration(relation: OwnedRelation, row: &StructFields<Value>) -> Result<TxOp, Status> {
    let (Some(Value::Utf8(column)), Some(Value::I64(prefix_bytes)), Some(Value::I64(cap)), Some(Value::Binary(when))) =
        (row.get("column"), row.get("prefix_bytes"), row.get("cap"), row.get("when"))
    else { return Err(invalid("Owned capacity declaration has invalid field types")); };
    if row.len() != 4 || !relation.is_root_data() {
        return Err(invalid("Owned capacity declaration requires a Root projection"));
    }
    Ok(TxOp::OwnedDeclareCapacity {
        relation, column: column.clone(), prefix_bytes: *prefix_bytes, cap: *cap, when: when.clone(),
    })
}

#[cfg(test)]
mod commit_tests;

#[cfg(test)]
mod audience_projection_tests {
    use super::*;

    #[test]
    fn excludes_foreign_relations_and_their_markers_from_the_projection() {
        let visible = OwnedRelation::new("visible").unwrap();
        let hidden = OwnedRelation::new("hidden").unwrap();
        let markers = OwnedRelation::new(rtdb_types::OWNED_ROOT_PUBLICATION_RELATION).unwrap();
        let readiness = OwnedRelation::new("fact_layout_readiness").unwrap();
        let id = |value: &str| EntityId::String(value.into());
        assert!(audience_point_admitted(&visible, &visible, &id("doc"), false));
        assert!(!audience_point_admitted(&visible, &hidden, &id("doc"), false));
        assert!(audience_point_admitted(&visible, &markers, &id("fact_partition_head:visible"), false));
        assert!(!audience_point_admitted(&visible, &markers, &id("fact_partition_head:hidden"), false));
        assert!(!audience_point_admitted(&visible, &readiness, &id("layout"), false));
        assert!(audience_point_admitted(&visible, &readiness, &id("layout"), true));
    }
}

fn owned_touches_type() -> rtdb_types::LogicalType {
    use rtdb_types::{LogicalType, ScalarType};

    let logical_id = LogicalType::from_legs([ScalarType::I64, ScalarType::Utf8, ScalarType::Keyword, ScalarType::Uuid]
        .into_iter().map(LogicalType::Scalar));
    LogicalType::list(LogicalType::Struct(StructFields::from([
        ("_id".into(), logical_id),
        ("op".into(), LogicalType::Scalar(ScalarType::Utf8)),
        ("valid_from_micros".into(), LogicalType::Scalar(ScalarType::I64)),
        ("valid_to_micros".into(), LogicalType::Scalar(ScalarType::I64).maybe()),
        ("created_micros".into(), LogicalType::Scalar(ScalarType::I64).maybe()),
        ("expiry_micros".into(), LogicalType::Scalar(ScalarType::I64).maybe()),
        ("assertion_ordinal".into(), LogicalType::Scalar(ScalarType::I64).maybe()),
    ])))
}
