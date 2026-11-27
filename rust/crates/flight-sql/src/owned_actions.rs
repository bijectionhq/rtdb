mod changes;
mod hosted_control;
mod hosted_upload;
mod prepared_landing;
mod hosted_observation;
mod import_page;
mod ordered_controls;
mod packed_groups;
mod unique;

pub(crate) use hosted_control::{READ_HELD_HOSTED_CONTROL_AT_PUBLICATION_V1, READ_HELD_HOSTED_CONTROL_V3, READ_HELD_ROOT_CATALOG_V1};
pub(crate) use hosted_upload::{DECIDE_HOSTED_STREAM_V1, HOSTED_UPLOAD_HEADER};
pub(crate) use hosted_observation::{CONFIRM_APPLIED_HOSTED_V6, CONFIRM_HOSTED_SOURCE_V1, OBSERVE_HELD_HOSTED_V1};
pub(crate) use import_page::DECIDE_HOSTED_IMPORT_PAGE_V1;
pub(crate) use packed_groups::DECIDE_HOSTED_PACKED_V1;
pub(crate) use unique::{READ_HELD_UNIQUE_V1, READ_HELD_UNIQUE_WINDOW_V1};

use std::{collections::{BTreeMap, BTreeSet}, sync::{Arc, Mutex}, time::Duration};

use arrow_array::{Array, ArrayRef, BinaryArray, BooleanArray, Decimal256Array, Float64Array, Int64Array, ListArray, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray, UInt64Array};
use arrow_flight::Result as ActionResult;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use prost::Message;
use rtdb_columnar::{ArrowCell, EntityIdRef, MemoryBudget, MemoryReservation, OwnedBatch, binary_cell, entity_id_type, read_owned_entity_id, selected_cell, try_entity_id_array};
use rtdb_format::ipc::OwnedStream;
use rtdb_node::{ChangeEdgeOrder, HeldSpatialNearest, HeldSpatialProbe, OwnedChangeEdge, NativeSession, OwnedCountRequest, OwnedRangeRequest, OwnedRangeWitness, OwnedReadSession, OwnedValidHistory, OwnedVersionOrder, OwnedVersionRequest, OwnedWorldWitness, TransactionStatus};
use rtdb_query::QueryCursor;
use rtdb_tx::{ArrowDocuments, Documents, OwnedClockSlot, OwnedRelativePeriod, TxOp};
use rtdb_types::{Anomaly, OwnedIdentityScan, EntityId, TransactionKey, Interval, LogicalType, OwnedRelation,
    SHARED_COLUMN_CARRIERS, SharedColumnCarrierPrefix, StructFields, TimeUnit, Timestamp, Value};
use rtdb_types::AnomalyKind;
use sha2::{Digest, Sha256};
use tokio::sync::OwnedSemaphorePermit;
use tonic::Status;
use uuid::Uuid;

use crate::owned_landing_wire::{self, ControlCommand};
use crate::owned_world::PointCell;
use self::ordered_controls::OrderedControls;
use crate::{
    actions::{OwnedIpcReply, SelectionEncoder},
    error::{arrow, invalid, status, unsupported},
    lease::Leased,
    service::{Call, Service, found},
    state::ResourceOwner,
    upload, wire,
};

pub(crate) enum OwnedReplyFormat<'a> {
    Unary,
    Selection(&'a mut SelectionEncoder),
    StreamV1,
}

pub(crate) const SEAL_HOSTED_RANGES_V1: &str = "rtdb.owned.SealHostedRanges.v1";
pub(crate) const VALIDATE_FRESH_HOSTED_V1: &str = "rtdb.owned.ValidateFreshHosted.v1";
pub(crate) const DECIDE_HOSTED_V4: &str = "rtdb.owned.DecideHosted.v4";

#[derive(Clone, Copy)]
pub(crate) enum HostedDecisionWire {
    Original,
    ImportPage,
    Packed,
}

enum HostedDecisionInput<'a> {
    Inline(HostedDecisionWire),
    Streamed {
        packed: packed_groups::PackedGroups<'static>,
        controls: &'a Bytes,
    },
}
pub(crate) const DECLARE_OWNED_FIXED_RANGE_V1: &str = "rtdb.owned.DeclareFixedRange.v1";
pub(crate) const DECLARE_OWNED_CAPACITY_V2: &str = "rtdb.owned.DeclareCapacity.v2";
pub(crate) const DECLARE_OWNED_UNIQUE_PATH_V1: &str = "rtdb.owned.DeclareUniquePath.v1";
pub(crate) const SPATIAL_HELD_OWNED_V1: &str = "rtdb.owned.SpatialHeld.v1";
pub(crate) const SPATIAL_HELD_OWNED_V2: &str = "rtdb.owned.SpatialHeld.v2";
pub(crate) const SPATIAL_OPEN_OWNED: &str = "rtdb.owned.SpatialOpen.v1";
pub(crate) const SPATIAL_PROBE_OWNED: &str = "rtdb.owned.SpatialProbe.v1";
pub(crate) const SPATIAL_GATHER_OWNED: &str = "rtdb.owned.SpatialGather.v1";
pub(crate) const SPATIAL_NEXT_OWNED: &str = "rtdb.owned.SpatialNext.v1";
pub(crate) const SPATIAL_CLOSE_OWNED: &str = "rtdb.owned.SpatialClose.v1";
pub(crate) const SPATIAL_RANK_OPEN_OWNED: &str = "rtdb.owned.SpatialRankOpen.v1";
pub(crate) const SPATIAL_RANK_OPEN_OWNED_V2: &str = "rtdb.owned.SpatialRankOpen.v2";
pub(crate) const SPATIAL_RANK_NEXT_OWNED: &str = "rtdb.owned.SpatialRankNext.v1";
const SPATIAL_FILTER_TOKEN: &str = "rtdb.owned.spatial_filter";
const MAX_SPATIAL_FILTERS: usize = 32;
pub(crate) const SPATIAL_HELD_OWNED_V3: &str = "rtdb.owned.SpatialHeld.v3";

#[derive(Clone, Copy)]
pub(crate) enum SpatialHeldWire { V1, V2, V3, Open, Probe, RankOpen, RankWindow }
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

pub(crate) const DECLARE_OWNED_SEARCH_PATH_V2: &str = "rtdb.owned.DeclareSearchPath.v2";
pub(crate) const DECLARE_OWNED_RELATION_V1: &str = "rtdb.owned.DeclareRelation.v1";
pub(crate) const DECLARE_OWNED_RELATION_V2: &str = "rtdb.owned.DeclareRelation.v2";
pub(crate) const OWNED_SHARED_SEARCH_TEXT_V1: &str = "rtdb.owned.SharedSearchText.v1";
pub(crate) const OWNED_LAYOUT_CAPABILITIES_V1: &str = "rtdb.owned.LayoutCapabilities.v1";
pub(crate) const OWNED_RELATION_LAYOUT_V1: &str = "rtdb.owned.RelationLayout.v1";
pub(crate) const DECLARE_OWNED_PATH_V1: &str = "rtdb.owned.DeclarePath.v1";
pub(crate) const CREATE_OWNED_V1: &str = "rtdb.owned.Create.v1";
pub(crate) const OFFER_FUNDED_MUTATION_CLOCK_V1: &str = "rtdb.owned.OfferFundedMutationClock.v1";
pub(crate) const FOOTPRINT_GRANT_V1: &str = "rtdb.owned.FootprintGrant.v1";
pub(crate) const OFFER_CLOCK_SLOT_V2: &str = "rtdb.owned.OfferClockSlot.v2";
pub(crate) const COMMIT_OWNED_V1: &str = "rtdb.owned.Commit.v1";
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
    FamilyFence,
    Replay,
}

impl CommitEffects {
    fn hosted(replay: bool, conditional: bool, controls: &[rtdb_tx::HostedControlWrite]) -> Self {
        if replay {
            Self::Replay
        } else if controls.iter().any(rtdb_tx::HostedControlWrite::is_assertion_family_marker) {
            Self::FamilyFence
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

    fn allows_erase(self, relation: &OwnedRelation) -> bool {
        relation.is_root_data() && (matches!(self, Self::Replay | Self::FamilyFence)
            || matches!(self, Self::HostedRoot)
                && (relation.name() == SHARED_COLUMN_CARRIERS || relation.name().ends_with("_search")))
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

fn hosted_refusal_fact_groups(body: &[u8]) -> Result<Vec<(OwnedRelation, &[u8])>, Status> {
    let groups = owned_groups(body, false, true)?;
    match groups.as_slice() {
        [_, _] => Ok(groups),
        [_, _, (relation, _)] if relation.name() == rtdb_types::OWNED_PUBLIC_ROOT_VERSIONS_RELATION => Ok(groups),
        _ => Err(invalid("A hosted refusal requires its original attempt and publication groups")),
    }
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
    let count = usize::from(owned_landing_wire::group_u16(count));
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
        let length = owned_landing_wire::group_u32(length) as usize;
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
pub(crate) const OPEN_OWNED_SCOPED_READ_V2: &str = "rtdb.owned.OpenOwnedScopedRead.v2";
pub(crate) const OPEN_OWNED_READ_AS_OF_V2: &str = "rtdb.owned.OpenReadAsOf.v2";
pub(crate) const OPEN_OWNED_READ_AT_PUBLICATION_V1: &str = "rtdb.owned.OpenReadAtPublication.v1";
pub(crate) const OPEN_OWNED_READ_V3: &str = "rtdb.owned.OpenRead.v3";
pub(crate) const OPEN_FRESH_OWNED_READ_V2: &str = "rtdb.owned.OpenFreshRead.v2";
pub(crate) const OPEN_APPLIED_OWNED_READ_V5: &str = "rtdb.owned.OpenAppliedRead.v5";
pub(crate) const HELD_OWNED_POINT_V1: &str = "rtdb.owned.ReadHeldPoint.v1";
pub(crate) const HELD_OWNED_POINT_V2: &str = "rtdb.owned.ReadHeldPoint.v2";
pub(crate) const HELD_OWNED_ABSENT_KEYS_V1: &str = "rtdb.owned.ReadHeldAbsentKeys.v1";
pub(crate) const HELD_OWNED_FIXED_POINT_V1: &str = "rtdb.owned.ReadHeldFixedPoint.v1";
pub(crate) const HELD_WORLD_VERSION_V2: &str = "rtdb.owned.ReadHeldWorldVersion.v2";
pub(crate) const HELD_WORLD_TOUCH_IMAGES_V1: &str = "rtdb.owned.ReadHeldWorldTouchImages.v1";
pub(crate) const HELD_WORLD_PATH_TOUCH_V1: &str = "rtdb.owned.ReadHeldWorldPathTouch.v1";
pub(crate) const HELD_OWNED_PATH_TOUCH_V2: &str = "rtdb.owned.ReadHeldPathTouch.v2";
pub(crate) const HELD_OWNED_HISTORICAL_POINT_V1: &str = "rtdb.owned.ReadHeldHistoricalPoint.v1";
pub(crate) const HELD_OWNED_POSITION_POINT_V1: &str = "rtdb.owned.ReadHeldPositionPoint.v1";
pub(crate) const HELD_ROOT_CUT_POINT_V1: &str = "rtdb.owned.ReadHeldRootCutPoint.v1";
pub(crate) const HELD_OWNED_POSITION_VALID_DURING_V1: &str = "rtdb.owned.ReadHeldPositionValidDuring.v1";
pub(crate) const HELD_OWNED_VALID_DURING_V2: &str = "rtdb.owned.ReadHeldValidDuring.v2";
pub(crate) const HELD_OWNED_VALID_REGION_V1: &str = "rtdb.owned.ReadHeldValidRegion.v1";
pub(crate) const HELD_WORLD_VALID_AT_V1: &str = "rtdb.owned.ReadHeldWorldValidDuringAt.v1";
pub(crate) const HELD_WORLD_VALID_V2: &str = "rtdb.owned.ReadHeldWorldValidDuring.v2";
pub(crate) const HELD_WORLD_PARENT_IDENTITIES_AT_V1: &str = "rtdb.owned.ReadHeldWorldParentIdentityPageAt.v1";
pub(crate) const HELD_WORLD_PARENT_VALID_AT_V1: &str = "rtdb.owned.ReadHeldWorldParentValidDuringAt.v1";
pub(crate) const SEARCH_HELD_WORLD_PARENT_AT_V3: &str = "rtdb.owned.SearchHeldWorldParentAt.v3";

pub(crate) enum WorldHistoricalRead { Current, EarlierEffective, EarlierParent }

pub(crate) const HELD_WORLD_IDENTITIES_AT_V1: &str = "rtdb.owned.ReadHeldWorldIdentityPageAt.v1";
pub(crate) enum WorldIdentityRead { Effective, Overlay, OverlayAt, EffectiveAt, ParentAt }
pub(crate) const HELD_WORLD_IDENTITIES_V1: &str = "rtdb.owned.ReadHeldWorldIdentityPage.v1";
pub(crate) const HELD_WORLD_OVERLAY_IDENTITIES_AT_V1: &str = "rtdb.owned.ReadHeldWorldOverlayIdentitiesAt.v1";
pub(crate) const HELD_WORLD_OVERLAY_IDENTITIES_V1: &str = "rtdb.owned.ReadHeldWorldOverlayIdentityPage.v1";
pub(crate) const HELD_PROTECTED_COVERAGE_V2: &str = "rtdb.owned.ReadHeldProtectedCarrierCoverage.v2";
pub(crate) const HELD_ROW_CARRIER_EXISTS_V2: &str = "rtdb.owned.ReadHeldRowCarrierExists.v2";
pub(crate) const HELD_PROTECTED_CARRIERS_V2: &str = "rtdb.owned.ReadHeldProtectedCarrierPage.v2";
pub(crate) const HELD_SEALED_SEARCH_UNIT_ROWS_V2: &str = "rtdb.owned.ReadHeldSealedSearchUnitRows.v2";
pub(crate) const HELD_WORLD_SHARED_COLUMN_CENSUS_V2: &str = "rtdb.owned.ReadHeldWorldSharedColumnCensus.v2";
pub(crate) const HELD_OWNED_IDENTITIES_V1: &str = "rtdb.owned.ReadHeldIdentityPage.v1";
pub(crate) const HELD_OWNED_IDENTITIES_ALL_VALID_V1: &str = "rtdb.owned.ReadHeldIdentityPageAllValid.v1";
pub(crate) const HELD_WORLD_PARENT_TOUCH_KEYS_AT_V1: &str = "rtdb.owned.ReadHeldWorldParentTouchKeysAt.v1";
pub(crate) const HELD_WORLD_PARENT_TOUCH_IMAGES_AT_V1: &str = "rtdb.owned.ReadHeldWorldParentTouchImagesAt.v1";
pub(crate) const PREPARE_HELD_WORLD_OVERLAY_V5: &str = "rtdb.owned.PrepareHeldWorldOverlay.v5";
pub(crate) const HELD_WORLD_ANCESTOR_CHANGES_V2: &str = "rtdb.owned.ReadHeldWorldAncestorChanges.v2";
pub(crate) const HELD_WORLD_PARENT_CHANGES_AT_V2: &str = "rtdb.owned.ReadHeldWorldParentChangesAt.v2";
pub(crate) const HELD_WORLD_PARENT_VERSIONS_AT_V1: &str = "rtdb.owned.ReadHeldWorldParentVersionsAt.v1";
pub(crate) const HELD_OWNED_VERSIONS_V2: &str = "rtdb.owned.ReadHeldVersions.v2";
pub(crate) const CLOSE_OWNED_READ_V1: &str = "rtdb.owned.CloseRead.v1";
pub(crate) const PREPARE_REPLAY_PARENT_V1: &str = "rtdb.owned.PrepareReplayParent.v1";
pub(crate) const FINISH_REPLAY_PARENTS_V1: &str = "rtdb.owned.FinishReplayParents.v1";
pub(crate) const RETAIN_REPLAYED_WORLD_V5: &str = "rtdb.owned.RetainReplayedWorld.v5";
pub(crate) const OPEN_WORLD_READ_V6: &str = "rtdb.owned.OpenWorldRead.v6";
pub(crate) const READ_HELD_WORLD_HEAD_V1: &str = "rtdb.owned.ReadHeldWorldHead.v1";
pub(crate) const SELECT_HELD_WORLD_PARENT_AS_OF_V1: &str = "rtdb.owned.SelectHeldWorldParentAsOf.v1";
pub(crate) const HELD_WORLD_PARENT_POINT_AT_V1: &str = "rtdb.owned.ReadHeldWorldParentPointAt.v1";

pub(crate) enum WorldCutSelection { ExactLocal, AsOfLocal, AsOfParent }

pub(crate) const SELECT_HELD_WORLD_AS_OF_V2: &str = "rtdb.owned.SelectHeldWorldAsOf.v2";
pub(crate) const HELD_WORLD_AT_PREFLIGHT_V2: &str = "rtdb.owned.PreflightHeldWorldAt.v2";
pub(crate) const HELD_WORLD_POINT_AT_V2: &str = "rtdb.owned.ReadHeldWorldPointAt.v2";
pub(crate) const HELD_WORLD_POINT_V2: &str = "rtdb.owned.ReadHeldWorldPoint.v2";
pub(crate) const CLOSE_WORLD_READ_V1: &str = "rtdb.owned.CloseWorldRead.v1";
pub(crate) const PUBLISH_HELD_WORLD_RETENTION_V5: &str = "rtdb.owned.PublishHeldWorldRetention.v5";
pub(crate) const PUBLISH_WORLD_CUT_V2: &str = "rtdb.owned.PublishWorldCut.v2";
pub(crate) const DECIDE_WORLD_CUT_V2: &str = "rtdb.owned.DecideWorldCut.v2";
pub(crate) const DECIDE_HELD_WORLDS_V3: &str = "rtdb.owned.DecideHeldWorlds.v3";
pub(crate) const SWITCH_PREPARED_PROPOSAL_V2: &str = "rtdb.owned.SwitchPreparedProposal.v2";
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
pub(crate) const SEARCH_HELD_WORLD_AT_V3: &str = "rtdb.owned.SearchHeldWorldAt.v3";
pub(crate) const SEARCH_HELD_WORLD_V4: &str = "rtdb.owned.SearchHeldWorld.v4";
pub(crate) const SEARCH_HELD_OWNED_V7: &str = "rtdb.owned.SearchHeld.v7";
pub(crate) const NEAREST_HELD_OWNED_V4: &str = "rtdb.owned.NearestHeld.v4";
pub(crate) const OWNED_NEAREST_APPROXIMATE: &str = "rtdb.owned.nearest_approximate";
pub(crate) const DECLARE_OWNED_VECTOR_PATH_V1: &str = "rtdb.owned.DeclareVectorPath.v1";
pub(crate) const HYBRID_HELD_OWNED_V4: &str = "rtdb.owned.HybridHeld.v4";
pub(crate) const HYBRID_HELD_WORLD_V3: &str = "rtdb.owned.HybridHeldWorld.v3";
pub(crate) const HYBRID_HELD_WORLD_AT_V3: &str = "rtdb.owned.HybridHeldWorldAt.v3";
pub(crate) const HYBRID_HELD_WORLD_PARENT_AT_V3: &str = "rtdb.owned.HybridHeldWorldParentAt.v3";
pub(crate) const NEAREST_HELD_WORLD_V3: &str = "rtdb.owned.NearestHeldWorld.v3";
pub(crate) const NEAREST_HELD_WORLD_AT_V3: &str = "rtdb.owned.NearestHeldWorldAt.v3";
pub(crate) const NEAREST_HELD_WORLD_PARENT_AT_V3: &str = "rtdb.owned.NearestHeldWorldParentAt.v3";
pub(crate) const ROOT_RANK_ACTIONS: [&str; 3] = [
    SEARCH_HELD_OWNED_V7, NEAREST_HELD_OWNED_V4, HYBRID_HELD_OWNED_V4,
];
pub(crate) const WORLD_RANK_ACTIONS: [&str; 9] = [
    SEARCH_HELD_WORLD_V4, SEARCH_HELD_WORLD_AT_V3, SEARCH_HELD_WORLD_PARENT_AT_V3,
    NEAREST_HELD_WORLD_V3, NEAREST_HELD_WORLD_AT_V3, NEAREST_HELD_WORLD_PARENT_AT_V3,
    HYBRID_HELD_WORLD_V3, HYBRID_HELD_WORLD_AT_V3, HYBRID_HELD_WORLD_PARENT_AT_V3,
];
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
pub(crate) const READ_HELD_WORLD_CHANGES_DESCENDING_V2: &str = "rtdb.owned.ReadHeldWorldChangesDescending.v2";
pub(crate) const READ_HELD_WORLD_CHANGES_V4: &str = "rtdb.owned.ReadHeldWorldChanges.v4";
pub(crate) const READ_HELD_WORLD_REGION_CHANGES_V1: &str = "rtdb.owned.ReadHeldWorldRegionChanges.v1";
pub(crate) const HELD_OWNED_STORAGE_USAGE_V1: &str = "rtdb.owned.ReadHeldStorageUsage.v1";
pub(crate) const HELD_OWNED_MERGED_CHANGES_DESCENDING_V2: &str = "rtdb.owned.ReadHeldMergedChangesDescending.v2";
pub(crate) const HELD_OWNED_MERGED_CHANGES_V4: &str = "rtdb.owned.ReadHeldMergedChanges.v4";
pub(crate) const HELD_TABLE_PUBLICATION_SEQUENCE_V1: &str = "rtdb.owned.ReadHeldTablePublicationSequence.v1";
pub(crate) const HELD_LOOKUP_PUBLICATION_V1: &str = "rtdb.owned.LookupHeldPublication.v1";
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
const OWNED_ASSERTION_ORDINAL: &str = "rtdb.owned.assertion_ordinal";
const OWNED_ASSERTION_LINE: &str = "rtdb.owned.assertion_line";
const OWNED_ASSERTION_HELD_READING: &str = "rtdb.owned.assertion_held_reading_micros";
const OWNED_ASSERTION_ORDINALS: &str = "rtdb.owned.assertion_ordinals";
const OWNED_CHANGE_COVERAGE: &str = "rtdb.owned.change_coverage_micros";

pub(crate) struct OwnedReadResource {
    snapshot: OwnedReadSession,
    owner: ResourceOwner,
    pub(crate) session: String,
    database: String,
    purpose: ReadPurpose,
    witnesses: Mutex<BTreeMap<Uuid, RetainedRangeWitness>>,
    spatial_filters: Mutex<BTreeMap<Uuid, Arc<RetainedSpatialFilter>>>,
    _permit: OwnedSemaphorePermit,
    _reservation: Arc<MemoryReservation>,
}

#[derive(Clone, Copy)]
enum ReadPurpose {
    OrdinaryFresh,
    ClassicRetained([u8; 32]),
    Other,
}

struct RetainedSpatialFilter {
    relation: OwnedRelation,
    columns: Vec<zeroize::Zeroizing<String>>,
    controls: SpatialContinuationControls,
    query: RetainedSpatialQuery,
    valid_at: Timestamp,
    _reservation: Arc<MemoryReservation>,
}

enum SpatialContinuationControls {
    None,
    Earlier {
        after: SpatialAnchor,
        partition: Vec<zeroize::Zeroizing<Vec<u8>>>,
    },
    Probe {
        partition: Vec<zeroize::Zeroizing<Vec<u8>>>,
        fields: Vec<zeroize::Zeroizing<String>>,
        point_column: Option<zeroize::Zeroizing<String>>,
    },
}

enum SpatialAnchor {
    String(zeroize::Zeroizing<String>),
    Keyword(zeroize::Zeroizing<String>),
    Integer(i64),
    Uuid([u8; 16]),
    Binary(zeroize::Zeroizing<Vec<u8>>),
}

impl SpatialAnchor {
    fn new(id: EntityIdRef<'_>) -> Self {
        use zeroize::Zeroizing;
        match id {
            EntityIdRef::String(value) => Self::String(Zeroizing::new(value.to_owned())),
            EntityIdRef::Keyword(value) => Self::Keyword(Zeroizing::new(value.to_owned())),
            EntityIdRef::Integer(value) => Self::Integer(value),
            EntityIdRef::Uuid(value) => Self::Uuid(value),
            EntityIdRef::Binary(value) => Self::Binary(Zeroizing::new(value.to_vec())),
        }
    }

    fn as_ref(&self) -> EntityIdRef<'_> {
        match self {
            Self::String(value) => EntityIdRef::String(value.as_str()),
            Self::Keyword(value) => EntityIdRef::Keyword(value.as_str()),
            Self::Integer(value) => EntityIdRef::Integer(*value),
            Self::Uuid(value) => EntityIdRef::Uuid(*value),
            Self::Binary(value) => EntityIdRef::Binary(value.as_slice()),
        }
    }
}

fn borrow_spatial_partition(partition: &[zeroize::Zeroizing<Vec<u8>>]) -> Vec<std::borrow::Cow<'_, [u8]>> {
    partition.iter().map(|part| std::borrow::Cow::Borrowed(part.as_slice())).collect()
}

enum RetainedSpatialQuery {
    Range {
        matches: rtdb_node::HeldSpatialRange,
        earlier: Option<(OwnedRelation, rtdb_spatial::PreparedRegion)>,
    },
    Probe {
        path: OwnedRelation,
        region: rtdb_spatial::PreparedRegion,
        certify_point: bool,
        matches: Mutex<Option<Arc<HeldSpatialProbe>>>,
    },
    Nearest(HeldSpatialNearest),
}

impl RetainedSpatialQuery {
    fn discard_probe_matches(&self) -> Result<bool, Status> {
        match self {
            Self::Probe { matches, .. } => Ok(matches.lock().map_err(|_| spatial_filters_unavailable())?.take().is_some()),
            _ => Ok(false),
        }
    }
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
    pub(crate) session: String,
    database: String,
    _permit: OwnedSemaphorePermit,
    _reservation: Arc<MemoryReservation>,
}

impl Leased for OwnedWorldReadResource {
    fn close(&self) { self.snapshot.close(); }
}

struct RetainedWorldWitness {
    evidence: OwnedWorldWitness,
    second: Option<OwnedWorldWitness>,
    absent_in_parent_and_overlay: bool,
    _reservation: Arc<MemoryReservation>,
}

impl OwnedWorldReadResource {
    fn remember(&self, call: &Call, evidence: OwnedWorldWitness, absent_in_parent_and_overlay: bool) -> Result<Uuid, Status> {
        self.remember_checks(call, evidence, None, absent_in_parent_and_overlay)
    }

    fn remember_checks(
        &self, call: &Call, evidence: OwnedWorldWitness, second: Option<OwnedWorldWitness>,
        absent_in_parent_and_overlay: bool,
    ) -> Result<Uuid, Status> {
        self.snapshot.check().map_err(status)?;
        let mut conditions = self.conditions.lock().map_err(|_| Status::internal("Owned world witness storage is unavailable"))?;
        let retained = conditions.len() + conditions.values().filter(|witness| witness.second.is_some()).count();
        if retained + 1 + usize::from(second.is_some()) > MAX_WORLD_WITNESSES {
            return Err(Status::resource_exhausted("The held world read has too many point witnesses"));
        }
        let nonce = Uuid::new_v4();
        if conditions.contains_key(&nonce) {
            return Err(Status::unavailable("Could not issue an owned world point witness"));
        }
        let charge = call.reserve(wire::add(evidence.allocation_bound(),
            second.as_ref().map_or(0, OwnedWorldWitness::allocation_bound))?)?;
        conditions.insert(nonce, RetainedWorldWitness { evidence, second, absent_in_parent_and_overlay, _reservation: charge });
        Ok(nonce)
    }

    fn conditions(&self, call: &Call, world: Uuid, nonces: &[u8]) -> Result<(Vec<OwnedWorldWitness>, Arc<MemoryReservation>), Status> {
        self.snapshot.check().map_err(status)?;
        if world != self.world { return Err(Status::not_found("unknown owned world point witness")); }
        let stored = self.conditions.lock().map_err(|_| Status::internal("Owned world witness storage is unavailable"))?;
        let mut seen = BTreeSet::new();
        let mut charge = call.reserve(4096)?;
        let mut conditions = Vec::new();
        for bytes in nonces.chunks_exact(16) {
            let nonce = Uuid::from_slice(bytes).map_err(arrow)?;
            if !seen.insert(nonce) { return Err(invalid("Owned world commit has a duplicate point witness")); }
            let condition = stored.get(&nonce).ok_or_else(|| Status::not_found("unknown owned world point witness"))?;
            let reservation = Arc::get_mut(&mut charge).expect("world commit owns its reservation");
            for evidence in std::iter::once(&condition.evidence).chain(condition.second.as_ref()) {
                reservation.grow_to(wire::add(reservation.bytes(), evidence.allocation_bound())?).map_err(status)?;
                conditions.push(evidence.clone());
            }
        }
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

/// A hybrid read's limit, optional staged overlays, its text window and its nearest window over the same relations.
struct OwnedHybridRequest<'a> {
    limit: usize,
    text_overlay: Option<OwnedBatch>,
    vector_overlay: Option<OwnedBatch>,
    rank: OwnedRankRequest<'a>,
    nearest: OwnedNearestRequest,
}

impl OwnedHybridRequest<'_> {
    fn staged(&self) -> bool { self.text_overlay.is_some() || self.vector_overlay.is_some() }
}

/// The ranks a fused hit had in each window, beside its fused score.
pub(crate) const HYBRID_TEXT_RANK: &str = "text_rank";
pub(crate) const HYBRID_NEAREST_RANK: &str = "nearest_rank";

fn owned_hybrid_request<'a>(call: &Call, envelope: &Bytes, request: &'a [u8], overlays: bool) -> Result<OwnedHybridRequest<'a>, Status> {
    let (limit, mut request) = request.split_first_chunk::<2>().ok_or_else(|| invalid("Held hybrid search has no limit"))?;
    let limit = usize::from(u16::from_be_bytes(*limit));
    let mut staged = [None, None];
    if overlays {
        for slot in &mut staged {
            let (length, rest) = request.split_first_chunk::<4>().ok_or_else(|| invalid("Held hybrid search has no overlay length"))?;
            let (ipc, rest) = rest.split_at_checked(u32::from_be_bytes(*length) as usize)
                .ok_or_else(|| invalid("Held hybrid search has a truncated overlay"))?;
            if !ipc.is_empty() { *slot = Some(request_batch(call, call.retain_request_bytes(envelope.slice_ref(ipc)))?); }
            request = rest;
        }
    }
    let (length, request) = request.split_first_chunk::<4>().ok_or_else(|| invalid("Held hybrid search has no text request length"))?;
    let (text, nearest) = request.split_at_checked(u32::from_be_bytes(*length) as usize)
        .ok_or_else(|| invalid("Held hybrid search has a truncated text request"))?;
    let rank = owned_rank_request(call, text)?;
    let nearest = owned_nearest_request(call, nearest)?;
    let window = rank.limit;
    if rank.offset != 0 || !(1..=128).contains(&window) || nearest.query.k() != window
        || !(1..=window).contains(&limit) || !rank.observe || !nearest.observe
        || nearest.partitions.len() != rank.partitions.len()
        || nearest.partitions.iter().zip(&rank.partitions).any(|(vector, (relation, _))| &vector.relation != relation)
    {
        return Err(invalid("A held hybrid search ranks one window of the same relations by text and vector with evidence, and keeps at most that window"));
    }
    let [text_overlay, vector_overlay] = staged;
    Ok(OwnedHybridRequest { limit, text_overlay, vector_overlay, rank, nearest })
}

struct OwnedNearestRequest {
    partitions: Vec<rtdb_node::nearest::NearestPartition>,
    query: rtdb_node::nearest::NearestQuery,
    observe: bool,
    _reservation: Arc<MemoryReservation>,
}

fn owned_nearest_request(call: &Call, request: &[u8]) -> Result<OwnedNearestRequest, Status> {
    use rtdb_node::nearest::{MAX_NEAREST_DIMENSIONS, MAX_NEAREST_FILTERS, MAX_NEAREST_FILTER_BYTES, MAX_NEAREST_FILTER_VALUES, MAX_NEAREST_K};
    let (&filters, rest) = request.split_first().ok_or_else(|| invalid("Owned held nearest has no filter count"))?;
    let filters = usize::from(filters);
    if filters > MAX_NEAREST_FILTERS { return Err(invalid("Owned held nearest filter count is out of range")); }
    let (count, mut rest) = rest.split_first_chunk::<2>().ok_or_else(|| invalid("Owned held nearest has no partition count"))?;
    let count = usize::from(u16::from_be_bytes(*count));
    if !(1..=32).contains(&count) { return Err(invalid("Owned held nearest partition count is out of range")); }
    let mut reservation = call.reserve(count * 512 * (2 + filters))?;
    let charge = Arc::get_mut(&mut reservation).expect("nearest request owns its reservation");
    let mut partitions = Vec::with_capacity(count);
    for _ in 0..count {
        let (relation, tail) = split_relation(rest)?;
        let (vector, tail) = split_relation(tail)?;
        rest = tail;
        let mut columns = Vec::with_capacity(filters);
        for _ in 0..filters {
            let (column, tail) = split_relation(rest)?;
            columns.push(column.name().to_owned());
            rest = tail;
        }
        partitions.push(rtdb_node::nearest::NearestPartition { relation, vector: vector.name().to_owned(), filters: columns });
    }
    let distinct = partitions.iter().map(|partition| &partition.relation).collect::<BTreeSet<_>>();
    if distinct.len() != partitions.len() { return Err(invalid("An owned nearest relation must be selected once")); }
    let mut values = Vec::with_capacity(filters);
    for _ in 0..filters {
        let (length, tail) = rest.split_first_chunk::<2>().ok_or_else(|| invalid("Owned held nearest filter has no value count"))?;
        let length = usize::from(u16::from_be_bytes(*length));
        if !(1..=MAX_NEAREST_FILTER_VALUES).contains(&length) { return Err(invalid("Owned held nearest filter value count is out of range")); }
        rest = tail;
        charge.grow_to(charge.bytes() + 512).map_err(status)?;
        let mut allowed = BTreeSet::<Vec<u8>>::new();
        for _ in 0..length {
            let (size, tail) = rest.split_first_chunk::<2>().ok_or_else(|| invalid("Owned held nearest filter value has no length"))?;
            let size = usize::from(u16::from_be_bytes(*size));
            if size > MAX_NEAREST_FILTER_BYTES { return Err(invalid("Owned held nearest filter value exceeds its size bound")); }
            let (value, tail) = tail.split_at_checked(size).ok_or_else(|| invalid("Owned held nearest filter value is truncated"))?;
            if !allowed.contains(value) {
                charge.grow_to(charge.bytes() + 128 + size).map_err(status)?;
                allowed.insert(value.to_vec());
            }
            rest = tail;
        }
        values.push(allowed);
    }
    let (settings, rest) = rest.split_first_chunk::<4>().ok_or_else(|| invalid("Owned held nearest requires a limit, evidence flag and route"))?;
    let k = usize::from(u16::from_be_bytes([settings[0], settings[1]]));
    if !(1..=MAX_NEAREST_K).contains(&k) { return Err(invalid("Owned held nearest limit is out of range")); }
    let observe = match settings[2] {
        0 => false,
        1 => true,
        _ => return Err(invalid("Invalid owned held nearest evidence flag")),
    };
    let approximate = match settings[3] {
        0 => false,
        1 => true,
        _ => return Err(invalid("Invalid owned held nearest route")),
    };
    let (dimensions, rest) = rest.split_first_chunk::<2>().ok_or_else(|| invalid("Owned held nearest has no dimension count"))?;
    let dimensions = usize::from(u16::from_be_bytes(*dimensions));
    if !(1..=MAX_NEAREST_DIMENSIONS).contains(&dimensions) || rest.len() != dimensions * 4 {
        return Err(invalid("Owned held nearest probe does not match its dimension count"));
    }
    charge.grow_to(charge.bytes() + dimensions * size_of::<f32>()).map_err(status)?;
    let probe = rest.chunks_exact(4)
        .map(|bytes| f32::from_be_bytes(bytes.try_into().expect("four-byte probe component")))
        .collect::<Vec<_>>();
    let query = rtdb_node::nearest::NearestQuery::new(probe, values, k).map_err(status)?;
    let query = if approximate { query.approximate() } else { query };
    Ok(OwnedNearestRequest { partitions, query, observe, _reservation: reservation })
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

fn split_change_edge(body: &[u8]) -> Result<(OwnedChangeEdge, &[u8]), Status> {
    let (time, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Missing change history coordinate"))?;
    let (position, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Missing change position"))?;
    let (&kind, body) = body.split_first().ok_or_else(|| invalid("Missing change edge kind"))?;
    let time = Timestamp(i64::from_be_bytes(*time));
    let position = i64::from_be_bytes(*position);
    let order = match (kind, position) {
        (0, -1) => ChangeEdgeOrder::AsOf,
        (1 | 2, position) if position >= 0 =>
            ChangeEdgeOrder::Publication { position: rtdb_types::TxId(position), included: kind == 1 },
        _ => return Err(invalid("Invalid change edge")),
    };
    Ok((OwnedChangeEdge { time, order }, body))
}

fn split_world_publication(body: &[u8]) -> Result<(TransactionKey, &[u8]), Status> {
    let (position, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Missing world publication position"))?;
    let (history, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Missing world publication history"))?;
    let tx_id = rtdb_types::TxId(i64::from_be_bytes(*position));
    let system_time = Timestamp(i64::from_be_bytes(*history));
    if tx_id.0 < 0 || system_time <= Timestamp::MIN || system_time >= Timestamp::MAX {
        return Err(invalid("Invalid world publication key"));
    }
    Ok((TransactionKey { tx_id, system_time }, body))
}

fn split_relation(body: &[u8]) -> Result<(OwnedRelation, &[u8]), Status> {
    let (length, rest) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Owned action has no relation length"))?;
    let length = usize::from(owned_landing_wire::group_u16(length));
    let (name, arrow_bytes) = rest.split_at_checked(length).ok_or_else(|| invalid("Owned action relation is truncated"))?;
    if length > 96 { return Err(invalid("Owned action relation exceeds its length limit")); }
    let name = std::str::from_utf8(name).map_err(arrow)?;
    Ok((OwnedRelation::new(name).map_err(status)?, arrow_bytes))
}

/// `[commit position: i64 BE][commit system time: i64 BE][first masked: u32 BE]`.
/// The masked commit is named by its position (T7): a tied commit shares its time.
fn split_position_mask(body: &[u8]) -> Result<((TransactionKey, u32), &[u8]), Status> {
    let (commit, rest) = split_world_publication(body)?;
    let (from, rest) = rest.split_first_chunk::<4>()
        .ok_or_else(|| invalid("Owned position read has no mask position"))?;
    Ok(((commit, u32::from_be_bytes(*from)), rest))
}

fn decode_batch(call: &Call, arrow: Bytes) -> Result<OwnedBatch, Status> {
    let stream = OwnedStream::read_bytes(arrow, call.memory()).map_err(status)?;
    single_owned_batch(stream)
}

fn decode_retained_batch(call: &Call, arrow: Bytes) -> Result<OwnedBatch, Status> {
    let stream = OwnedStream::read_buffer(arrow_buffer::Buffer::from(arrow), call.memory()).map_err(status)?;
    single_owned_batch(stream)
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

fn spatial_memory_refusal(error: &Status) -> bool {
    error.metadata().get_bin("xtdb-anomaly-bin").and_then(|value| value.to_bytes().ok())
        .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
        .is_some_and(|anomaly| anomaly["code"].as_str().and_then(AnomalyKind::from_wire_code) == Some(AnomalyKind::ResourceExhausted))
}

fn spatial_reply_ids(
    memory: &Arc<MemoryBudget>, rows: &OwnedBatch, target: &Field,
) -> Result<(ArrayRef, Arc<MemoryReservation>), Status> {
    let batch = rows.batch();
    let source = batch.schema_ref().field(0);
    let array = batch.column(0);
    if source == target {
        return Ok((Arc::clone(array), memory.reserve(4096).map_err(status)?));
    }

    let bound = rtdb_columnar::arrow_relayout_bound(array.as_ref(), source, target).map_err(status)?;
    let charge = memory.reserve(wire::add(bound, 4096)?).map_err(status)?;
    if let Ok((_, ids)) = rtdb_columnar::relayout_arrow_column(Arc::clone(array), source, target)
        && ids.data_type() == target.data_type()
    {
        return Ok((ids, charge));
    }

    let ids = (0..array.len()).map(|row| read_owned_entity_id(source, array.as_ref(), row));
    let bound = ids.clone().try_fold(4096, |bytes, id| {
        let payload = id.map_err(status)?.owned_payload_bytes();
        wire::add(bytes, wire::add(128, wire::mul(payload, 4)?)?)
    })?;
    let fallback_charge = memory.reserve(bound).map_err(status)?;
    drop(charge);
    Ok((try_entity_id_array(ids).map_err(status)?, fallback_charge))
}

fn spatial_document_columns(batch: &RecordBatch) -> Result<Vec<&str>, Status> {
    spatial_column_names(batch, "document_columns")
}

fn spatial_column_names<'a>(batch: &'a RecordBatch, name: &str) -> Result<Vec<&'a str>, Status> {
    let layout = batch.schema_ref().field_with_name(name).map(Field::data_type);
    if !matches!(layout, Ok(DataType::List(item)) if item.data_type() == &DataType::Utf8
        && !item.metadata().contains_key(rtdb_columnar::EXTENSION_NAME_KEY)) {
        return Err(invalid("Spatial document_columns must be a List<Utf8>"));
    }
    let columns = ArrowCell::named(batch, name, 0).and_then(ArrowCell::list)
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

fn spatial_after(input: &RecordBatch) -> Result<Option<EntityIdRef<'_>>, Status> {
    match input.schema_ref().column_with_name("after") {
        Some((index, field)) => selected_cell(field, input.column(index).as_ref(), 0).map_err(status)?
            .map(|(field, array, row)| read_owned_entity_id(field, array, row).map_err(status)).transpose(),
        None => Ok(None),
    }
}

fn spatial_probe_point<'a>(
    batch: &'a RecordBatch, fields: &[&str], columns: &[&str],
) -> Result<Option<&'a str>, Status> {
    let point_column = owned_scalar::<StringArray>(batch, "point_column")?
        .map(|(array, row)| array.value(row));
    if fields.iter().chain(point_column.iter()).any(|field| !field.starts_with("c_") || !columns.contains(field)) {
        return Err(invalid("Spatial probe fields must belong to the full fact projection"));
    }
    Ok(point_column)
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

fn request_batch(call: &Call, arrow: Bytes) -> Result<OwnedBatch, Status> {
    require_identity(decode_batch(call, arrow)?)
}

fn require_identity(batch: OwnedBatch) -> Result<OwnedBatch, Status> {
    if batch.batch().schema_ref().index_of("_id").is_err() {
        return Err(invalid("Owned action requires an _id column"));
    }
    Ok(batch)
}

pub(crate) fn require_idle(native: &NativeSession) -> Result<(), Status> {
    if native.state().auto_commit() && native.state().status() == TransactionStatus::Idle { return Ok(()); }
    Err(Status::failed_precondition("Owned actions require an idle autocommit session"))
}

impl Service {
    pub(crate) fn owned_shared_search_text_action(&self, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        if !body.is_empty() { return Err(invalid("Owned shared search text takes an empty body")); }
        Ok(ActionResult { body: Bytes::from_static(b"\x01") })
    }

    pub(crate) fn owned_layout_capabilities_action(&self, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        if !body.is_empty() { return Err(invalid("Owned layout capabilities takes an empty body")); }
        Ok(ActionResult { body: Bytes::from_static(b"\x02") })
    }

    pub(crate) async fn owned_relation_layout_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, tail) = split_relation(body)?;
        if !tail.is_empty() || !relation.is_root_data() { return Err(invalid("Invalid owned layout relation")); }
        let connection = self.connection(call, &call.database).await?;
        let mut native = connection.borrow().await?;
        require_idle(&native)?;
        let layout = native.owned_relation_layout(&relation).await.map_err(status)?;
        let body = match layout {
            None => Bytes::from_static(b"\x00"),
            Some((schema, _held)) if schema.is_v2() => {
                let reservation = call.reserve(rtdb_types::OwnedSchema::MAX_JSON_BYTES * 3 + 1)?;
                let encoded = schema.to_json().map_err(status)?;
                let mut body = Vec::with_capacity(encoded.len() + 1);
                body.push(2);
                body.extend_from_slice(&encoded);
                wire::retain(Bytes::from(body), reservation)
            }
            Some(_) => Bytes::from_static(b"\x01"),
        };
        Ok(ActionResult { body })
    }

    pub(crate) async fn declare_owned_relation_action(&self, call: &Call, body: &[u8], v2: bool, ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, json) = split_relation(body)?;
        if json.len() > rtdb_types::OwnedSchema::MAX_JSON_BYTES {
            return Err(invalid("Owned declaration exceeds its byte limit"));
        }
        let _decoded = call.reserve(wire::add(wire::mul(json.len(), 8)?, 4096)?)?;
        let schema = rtdb_types::OwnedSchema::from_json(json).map_err(status)?;
        if schema.is_v2() != v2 { return Err(invalid("Owned declaration version disagrees with its action")); }
        self.owned_write_reply(call, vec![TxOp::OwnedDeclareRelation { relation, schema }], ipc_stream).await
    }

    pub(crate) async fn drop_owned_spatial_path_action(&self, call: &Call, body: &[u8], ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, body) = split_relation(body)?;
        let (name, tail) = split_relation(body)?;
        if !tail.is_empty() { return Err(invalid("Owned spatial path removal has trailing bytes")); }
        self.owned_write_reply(call, vec![TxOp::OwnedDropSpatialPath { relation, name }], ipc_stream).await
    }

    pub(crate) async fn drop_owned_spatial_path_batch_action(
        &self, call: &Call, body: Bytes, ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
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
        self.owned_write_reply(call, vec![TxOp::OwnedDropSpatialPath { relation, name }], ipc_stream).await
    }

    pub(crate) async fn declare_owned_path_action(
        &self, call: &Call, body: &Bytes, wire: PathDeclarationWire, ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
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
        self.owned_write_reply(call, vec![declaration], ipc_stream).await
    }

    pub(crate) async fn declare_owned_fixed_range_action(&self, call: &Call, body: &Bytes, ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, arrow) = split_relation(body)?;
        let batch = request_batch(call, call.retain_request_bytes(body.slice_ref(arrow)))?;
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != 3 {
            return Err(invalid("Fixed range declaration requires one row with path, column and valid_micros"));
        }
        let (rows, _decoded) = upload::domain_documents(batch.batch(), self)?;
        let row = &rows[0];
        let (Some(Value::Utf8(path)), Some(Value::Utf8(column)), Some(Value::I64(valid_micros))) =
            (row.get("path"), row.get("column"), row.get("valid_micros"))
        else { return Err(invalid("Fixed range declaration has invalid field types")); };
        let rule = rtdb_types::FixedRangeRule { path: path.clone(), column: column.clone(), valid_at: Timestamp(*valid_micros) };
        self.owned_write_reply(call, vec![TxOp::OwnedDeclareFixedRange { relation, rule }], ipc_stream).await
    }

    pub(crate) async fn declare_owned_capacity_action(&self, call: &Call, body: &Bytes, ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, arrow) = split_relation(body)?;
        let batch = request_batch(call, call.retain_request_bytes(body.slice_ref(arrow)))?;
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != 4 {
            return Err(invalid("Owned capacity declaration requires one row with column, prefix_bytes, cap and when"));
        }
        let (rows, _decoded) = upload::domain_documents(batch.batch(), self)?;
        let operation = owned_capacity_declaration(relation, &rows[0])?;
        self.owned_write_reply(call, vec![operation], ipc_stream).await
    }

    /// A unique path takes `_id` and `column`; a search path also takes its `analyzer`.
    pub(crate) async fn declare_owned_single_column_path_action(&self, call: &Call, body: &Bytes, unique: bool, ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, arrow) = split_relation(body)?;
        let batch = request_batch(call, call.retain_request_bytes(body.slice_ref(arrow)))?;
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != if unique { 2 } else { 3 } {
            return Err(invalid("Owned path declaration requires one row with _id, column and, for search, analyzer"));
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
            let Some(Value::Utf8(analyzer)) = rows[0].get("analyzer") else {
                return Err(invalid("Owned search path declaration requires a text analyzer"));
            };
            let analyzer = rtdb_plan::parse_analyzer(analyzer).map_err(status)?;
            TxOp::OwnedDeclareSearchPath { relation, name, column, analyzer }
        };
        self.owned_write_reply(call, vec![operation], ipc_stream).await
    }

    /// `rtdb.owned.DeclareVectorPath.v1`: a relation prefix, then one Arrow row of `_id`, `column`, `dimensions` and `recall`.
    pub(crate) async fn declare_owned_vector_path_action(&self, call: &Call, body: &Bytes, ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, arrow) = split_relation(body)?;
        let batch = request_batch(call, call.retain_request_bytes(body.slice_ref(arrow)))?;
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != 4 {
            return Err(invalid("Owned vector path declaration requires one row with _id, column, dimensions and recall"));
        }
        let (rows, _decoded) = upload::documents(batch.batch(), self)?;
        let (Some(Value::Utf8(name)), Some(Value::Utf8(column)), Some(Value::I64(dimensions)), Some(Value::F64(recall))) =
            (rows[0].get("_id"), rows[0].get("column"), rows[0].get("dimensions"), rows[0].get("recall"))
        else {
            return Err(invalid("Owned vector path declaration requires a text name and column, Int64 dimensions and Float64 recall"));
        };
        let name = OwnedRelation::new(name.as_str()).map_err(status)?;
        let operation = TxOp::OwnedDeclareVectorPath { relation, name, column: column.clone(), dimensions: *dimensions, recall: *recall };
        self.owned_write_reply(call, vec![operation], ipc_stream).await
    }

    fn require_generic_owned_target(&self, call: &Call, relation: &OwnedRelation) -> Result<(), Status> {
        if self.0.config.hosted_controls.is_some()
            && call.session.placed_database() == Some(call.database.as_str())
            && relation.name() != "fact_layout_readiness"
        {
            return Err(Status::permission_denied("Hosted fact writes require an installed authority decision"));
        }
        Ok(())
    }

    pub(crate) async fn create_owned_action(&self, call: &Call, body: &Bytes, ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, arrow) = split_relation(body)?;
        self.require_generic_owned_target(call, &relation)?;
        let batch = require_identity(decode_retained_batch(call, body.slice_ref(arrow))?)?;
        if relation.is_root_data() {
            if batch.batch().column_by_name("_absent").is_none() {
                if let Some(documents) = upload::owned_payload(&batch, self.memory())? {
                    return self.owned_write_reply(call, vec![TxOp::OwnedArrowDocs {
                        relation, documents, create_only: true, valid_from: None, valid_to: None,
                    }], ipc_stream).await;
                }
            } else {
                let rows = batch.batch().num_rows();
                let _operations = call.reserve(wire::mul(rows, std::mem::size_of::<TxOp>() + 512)?)?;
                let mut operations = Vec::with_capacity(rows);
                for row in 0..rows {
                    let positions = owned_payload_columns(batch.batch(), row, &[])?;
                    operations.push(match self.owned_payload(&relation, &batch, row, &positions)? {
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
                return self.owned_write_reply(call, operations, ipc_stream).await;
            }
        } else if relation.name() == rtdb_types::OWNED_ROOT_PUBLICATION_RELATION
            && batch.batch().column_by_name("_absent").is_none()
            && let Some(documents) = ArrowDocuments::from_legacy_owned_payload(&batch, self.memory()).map_err(status)?
        {
            return self.owned_write_reply(call, vec![TxOp::OwnedArrowDocs {
                relation, documents, create_only: true, valid_from: None, valid_to: None,
            }], ipc_stream).await;
        }
        let (mut rows, _decoded) = upload::owned_documents(batch.batch(), call.memory())?;
        for row in &mut rows { strip_owned_absence(row)?; }
        let operation = TxOp::OwnedInsertDocs {
            relation, documents: Documents::new(rows, self.memory()).map_err(status)?,
            valid_from: None, valid_to: None,
        };
        self.owned_write_reply(call, vec![operation], ipc_stream).await
    }

    pub(crate) async fn commit_owned_action(&self, call: &Call, body: &Bytes, format: CommitFormat, ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
        self.commit_owned(call, body, body, format, Vec::new(), false, Vec::new(), ipc_stream).await
    }

    pub(crate) async fn commit_owned_counted_action(&self, call: &Call, envelope: &Bytes, ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
        let body = envelope.as_ref();
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (conditions, body, _counts) = owned_count_conditions(call, body, None, false)?;
        let (witnesses, body, _framing) = self.owned_witness_prefix(call, body, true)?;
        self.commit_owned(call, envelope, body, CommitFormat::Ordered, witnesses, false, conditions, ipc_stream).await
    }

    pub(crate) async fn commit_owned_witnessed_action(&self, call: &Call, envelope: &Bytes, format: CommitFormat, erase_allowed: bool, ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
        let body = envelope.as_ref();
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let allow_empty = erase_allowed || matches!(format, CommitFormat::Ordered);
        let (witnesses, body, _framing) = self.owned_witness_prefix(call, body, allow_empty)?;
        self.commit_owned(call, envelope, body, format, witnesses, erase_allowed, Vec::new(), ipc_stream).await
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
            // What the committing reader revealed of this range; empty keeps frontier equality.
            let (length, tail) = tail.split_first_chunk::<2>().ok_or_else(|| invalid("Owned range witness has no revealed projection"))?;
            let (revealed, tail) = tail.split_at_checked(usize::from(u16::from_be_bytes(*length)))
                .ok_or_else(|| invalid("Truncated owned range revealed projection"))?;
            let revealed = (!revealed.is_empty()).then(|| rtdb_types::OwnedRevealed::from_wire(revealed)
                .ok_or_else(|| invalid("Invalid owned range revealed projection"))).transpose()?;
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
            witnesses.push((held, witness.revealing(revealed)));
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
        let (slot, trailing) = self.owned_clock_slot(call, body).await?;
        if !trailing.is_empty() {
            return Err(invalid("A clock slot takes one original hold handle"));
        }
        let mut body = Vec::with_capacity(48);
        body.extend_from_slice(&slot.reading.0.to_be_bytes());
        body.extend_from_slice(&slot.issued.encode());
        Ok(ActionResult { body: wire::retain(body, call.reserve(48)?) })
    }

    pub(crate) async fn footprint_grant_action(
        &self,
        call: &Call,
        body: &[u8],
        ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
        self.hosted_control_policy(call)?;
        let op: rtdb_tx::OwnedFootprintGrantOp = serde_json::from_slice(body)
            .map_err(|_| invalid("Invalid SS8 footprint grant action payload"))?;
        self.owned_write_reply_with_reads(
            call,
            vec![TxOp::OwnedFootprintGrant(op)],
            Vec::new(),
            Vec::new(),
            ipc_stream,
        )
        .await
    }

    pub(crate) async fn offer_funded_mutation_clock_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        let (slot, trailing) = self.owned_clock_slot(call, body).await?;
        let (fingerprint, receipt) = trailing.split_first_chunk::<32>().ok_or_else(|| invalid("Missing original request fingerprint"))?;
        let receipt = std::str::from_utf8(receipt).map_err(|_| invalid("Invalid original receipt coordinate"))?;
        let connection = self.connection(call, &call.database).await?;
        let native = connection.borrow().await?;
        require_idle(&native)?;
        let token = native.seal_funded_mutation_clock(slot, receipt, *fingerprint).map_err(status)?;
        let bound = 56 + token.len();
        let charge = call.reserve(bound)?;
        let mut body = Vec::with_capacity(bound);
        body.extend_from_slice(&slot.through.0.to_be_bytes());
        body.extend_from_slice(&slot.reading.0.to_be_bytes());
        body.extend_from_slice(&slot.issued.encode());
        body.extend_from_slice(token.as_bytes());
        Ok(ActionResult { body: wire::retain(body, charge) })
    }

    async fn wait_for_clock_slot(&self, call: &Call, slot: OwnedClockSlot) -> Result<(), Status> {
        let delay = rtdb_tx::clock_slot_wait(slot.reading, self.0.node.now()).ok_or_else(Self::clock_slot_missed)?;
        if !delay.is_zero() {
            let wait = tokio::time::sleep(delay);
            call.cancellation.run_until_cancelled(wait).await
                .ok_or_else(|| Status::cancelled("The ordered clock slot was cancelled"))?;
        }
        Ok(())
    }

    async fn owned_clock_slot<'a>(
        &self, call: &Call, body: &'a [u8],
    ) -> Result<(OwnedClockSlot, &'a [u8]), Status> {
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
        let through = cut.live().ok_or_else(|| Status::failed_precondition(
            "An ordered clock slot requires a live placed Root hold",
        ))?.tx_id;
        let issued = original.snapshot.receiver_observation().map_err(status)?;
        let reading = rtdb_tx::clock_slot_reading(issued.instant).ok_or_else(Self::clock_slot_missed)?;
        Ok((OwnedClockSlot { through, reading, issued, funded: false }, body))
    }

    /// Seal only observations the original native hold actually issued.
    pub(crate) async fn seal_hosted_ranges_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        self.hosted_control_policy(call)?;
        let (witnesses, rest, _witness_charge) = self.owned_witness_prefix(call, body, false)?;
        if !rest.is_empty() { return Err(invalid("Unexpected input after the hosted range witnesses")); }
        let mut bytes = Vec::new();
        let mut charges = Vec::with_capacity(witnesses.len());
        for (held, witness) in witnesses {
            let cap = wire::add(4096, wire::mul(witness.retained_bytes(), 12)?)?
                .min(3 * rtdb_node::MAX_DURABLE_RANGE_BYTES);
            charges.push(call.reserve(cap)?);
            let token = held.snapshot.seal_durable_range(&witness).map_err(status)?;
            bytes.extend_from_slice(&u32::try_from(token.len()).map_err(arrow)?.to_be_bytes());
            bytes.extend_from_slice(token.as_bytes());
        }
        let charge = call.reserve(bytes.len())?;
        Ok(ActionResult { body: wire::retain(Bytes::from(bytes), charge) })
    }

    fn durable_hosted_ranges<'a>(&self, body: &'a [u8]) -> Result<(Vec<&'a str>, &'a [u8]), Status> {
        let (length, mut body) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Hosted validation has no range count"))?;
        let count = usize::from(u16::from_be_bytes(*length));
        if count > MAX_RANGE_WITNESSES { return Err(invalid("Too many hosted range witnesses")); }
        let mut witnesses = Vec::with_capacity(count);
        for _ in 0..count {
            let (length, rest) = body.split_first_chunk::<4>().ok_or_else(|| invalid("Missing durable range length"))?;
            let length = u32::from_be_bytes(*length) as usize;
            if length == 0 || length > 2 * rtdb_node::MAX_DURABLE_RANGE_BYTES {
                return Err(invalid("Invalid durable range length"));
            }
            let (token, rest) = rest.split_at_checked(length).ok_or_else(|| invalid("Truncated durable range"))?;
            witnesses.push(std::str::from_utf8(token).map_err(|_| invalid("Invalid durable range encoding"))?);
            body = rest;
        }
        Ok((witnesses, body))
    }

    async fn open_durable_hosted_ranges(&self, call: &Call, tokens: Vec<&str>) -> Result<Vec<rtdb_node::DurableRange>, Status> {
        if tokens.is_empty() { return Ok(Vec::new()); }
        let connection = self.connection(call, &call.database).await?;
        let native = connection.borrow().await?;
        require_idle(&native)?;
        tokens.into_iter().map(|token| native.open_durable_range(token).map_err(status)).collect()
    }

    pub(crate) async fn validate_fresh_hosted_action(
        &self, call: &Call, envelope: &Bytes,
    ) -> Result<ActionResult, Status> {
        let policy = self.hosted_control_policy(call)?;
        let (floor, body) = hosted_observation::applied_read_floor(envelope)?;
        let (tokens, body) = self.durable_hosted_ranges(body)?;
        let token_bytes = tokens.iter().map(|token| token.len()).sum();
        let _witness_charge = call.reserve(wire::add(4096, wire::mul(token_bytes, 4)?)?)?;
        let ranges = self.open_durable_hosted_ranges(call, tokens).await?;
        let (until, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Hosted validation has no deadline"))?;
        let (length, body) = body.split_first_chunk::<4>().ok_or_else(|| invalid("Hosted validation has no fact group length"))?;
        let (facts, controls) = body.split_at_checked(u32::from_be_bytes(*length) as usize)
            .ok_or_else(|| invalid("Truncated hosted validation fact groups"))?;
        let count = ordered_group_count(facts)?;
        let _framing = call.reserve((count + MAX_COMMIT_GROUPS) * 512)?;
        let facts = owned_groups(facts, false, true)?;
        let controls = owned_groups(controls, true, false)?;
        let fact_relations = facts.iter().map(|(relation, _)| relation).collect::<BTreeSet<_>>();
        if fact_relations.len() + controls.len() > MAX_COMMIT_GROUPS {
            return Err(invalid("Hosted validation exceeds its group limit"));
        }
        hosted_control::admit_control_targets(policy, controls.iter().map(|(relation, _)| relation))?;
        let scope = self.hosted_control_scope(call).await?;
        let mut rows_left = MAX_GROUPED_COMMIT_ROWS.checked_sub(ranges.len())
            .ok_or_else(|| invalid("Hosted validation exceeds its condition limit"))?;
        let control_frame = call.retain_request_bytes(envelope.clone());
        let (controls, _controls) = self.decode_hosted_control_groups(call, &control_frame, controls, scope, &mut rows_left, true)?;
        let mut operations = Vec::new();
        let _facts = self.decode_owned_commit_groups(call, envelope, facts, &mut operations, &mut rows_left, CommitEffects::Facts)?;
        let _points = call.reserve(wire::mul(operations.len(), std::mem::size_of::<rtdb_node::AppliedOwnedPoint>())?)?;
        let points = operations.into_iter().map(|operation| match operation {
            // A durable hosted read confirms the exact version; that is at
            // least as strict as any revealed projection the check carries.
            TxOp::OwnedCheck { relation, id, valid_at, expected, .. } =>
                Ok(rtdb_node::AppliedOwnedPoint { relation, id, valid_at, expected }),
            _ => Err(invalid("Hosted validation supports only point and control checks")),
        }).collect::<Result<Vec<_>, _>>()?;
        let connection = self.connection(call, &call.database).await?;
        let mut native = connection.borrow().await?;
        require_idle(&native)?;
        let confirmation = native.validate_durable_hosted_read(
            scope, &floor, Timestamp(i64::from_be_bytes(*until)), &controls, &points, &ranges,
        ).await.map_err(status)?;
        hosted_observation::confirmation_reply(call, confirmation)
    }

    fn installed_authority(body: &[u8]) -> Result<(rtdb_tx::HostedAuthority, &[u8]), Status> {
        let (authority, rest) = body.split_first_chunk::<{ rtdb_tx::HostedAuthority::WIDTH }>()
            .ok_or_else(|| invalid("Missing installed authority proof"))?;
        Ok((rtdb_tx::HostedAuthority::decode(authority).map_err(status)?, rest))
    }

    pub(crate) async fn decide_hosted_action(
        &self, call: &Call, envelope: &Bytes, wire: HostedDecisionWire, ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
        self.decide_hosted_reply(call, envelope, HostedDecisionInput::Inline(wire), ipc_stream, None).await
    }

    async fn decide_hosted(
        &self, call: &Call, envelope: &Bytes, input: HostedDecisionInput<'_>,
    ) -> Result<ActionResult, Status> {
        self.decide_hosted_reply(call, envelope, input, false, None).await.map(OwnedIpcReply::into_unary)
    }

    pub(crate) async fn decide_retired_family(
        &self, call: &Call, envelope: &Bytes, enrollment: &[u8],
    ) -> Result<ActionResult, Status> {
        self.decide_hosted_reply(call, envelope, HostedDecisionInput::Inline(HostedDecisionWire::Original), false, Some(enrollment))
            .await.map(OwnedIpcReply::into_unary)
    }

    async fn decide_hosted_reply(
        &self, call: &Call, envelope: &Bytes, input: HostedDecisionInput<'_>, ipc_stream: bool,
        retired_enrollment: Option<&[u8]>,
    ) -> Result<OwnedIpcReply, Status> {
        self.hosted_control_policy(call)?;
        let (authority, body) = Self::installed_authority(envelope)?;
        let ([flags], body) = body.split_first_chunk::<1>()
            .ok_or_else(|| invalid("Missing hosted decision flags"))?;
        if *flags & !0b11_1111 != 0 || *flags & 48 == 48 {
            return Err(invalid("Invalid hosted decision flags"));
        }
        if retired_enrollment.is_some() && *flags != 0 {
            return Err(invalid("Retired family custody requires a Root retirement decision"));
        }
        if matches!(input, HostedDecisionInput::Inline(HostedDecisionWire::ImportPage)) && *flags != 0 {
            return Err(invalid("A hosted import page requires an ordinary Root decision"));
        }
        let counted = *flags & 1 != 0;
        let recheck_at_staging = *flags & 8 != 0;
        let (funded, body) = if *flags & 32 != 0 {
            let (length, body) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Missing funded clock offer length"))?;
            let length = usize::from(u16::from_be_bytes(*length));
            if length == 0 || length > rtdb_node::MAX_FUNDED_MUTATION_CLOCK_TOKEN_BYTES {
                return Err(invalid("Invalid funded clock offer length"));
            }
            let (token, body) = body.split_at_checked(length).ok_or_else(|| invalid("Truncated funded clock offer"))?;
            let token = std::str::from_utf8(token).map_err(|_| invalid("Invalid funded clock offer"))?;
            let connection = self.connection(call, &call.database).await?;
            let native = connection.borrow().await?;
            require_idle(&native)?;
            (Some(native.open_funded_mutation_clock(token).map_err(status)?), body)
        } else {
            (None, body)
        };
        let (slot, body) = if *flags & 16 != 0 {
            let (slot, body) = self.owned_clock_slot(call, body).await?;
            (Some(slot), body)
        } else {
            (funded.as_ref().map(|offer| offer.slot), body)
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
        self.commit_hosted_with_reads(
            call, envelope, primary, counted, worlds, refusal, recheck_at_staging, slot, funded.as_ref(), authority, input, ipc_stream, retired_enrollment,
        ).await
    }

    async fn commit_hosted_with_reads(
        &self, call: &Call, envelope: &Bytes, body: &[u8], counted: bool, worlds: Vec<HostedWorldRead>,
        refusal: Option<&[u8]>, recheck_at_staging: bool, slot: Option<OwnedClockSlot>,
        funded: Option<&rtdb_node::FundedMutationClock>, authority: rtdb_tx::HostedAuthority, input: HostedDecisionInput<'_>,
        ipc_stream: bool, retired_enrollment: Option<&[u8]>,
    ) -> Result<OwnedIpcReply, Status> {
        let policy = self.hosted_control_policy(call)?;
        let (conditions, body, _counts) = if counted {
            owned_count_conditions(call, body, None, false)?
        } else {
            (Vec::new(), body, Vec::new())
        };
        let (tokens, body) = self.durable_hosted_ranges(body)?;
        if retired_enrollment.is_some() && !tokens.is_empty() {
            return Err(invalid("Retired family custody uses exact Root point checks"));
        }
        let token_bytes = tokens.iter().map(|token| token.len()).sum();
        let _witness_charge = call.reserve(wire::add(4096, wire::mul(token_bytes, 4)?)?)?;
        let witnesses = self.open_durable_hosted_ranges(call, tokens).await?;
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
        let mut page = None;
        let mut packed = None;
        let control_envelope = match &input {
            HostedDecisionInput::Inline(_) => call.retain_request_bytes(envelope.clone()),
            HostedDecisionInput::Streamed { .. } => envelope.clone(),
        };
        let (facts, control_frame): (&[u8], Bytes) = match input {
            HostedDecisionInput::Inline(format) => {
                let (length, body) = body.split_first_chunk::<4>()
                    .ok_or_else(|| invalid("Hosted commit has no fact group length"))?;
                let (facts, controls) = body.split_at_checked(u32::from_be_bytes(*length) as usize)
                    .ok_or_else(|| invalid("Hosted fact group stream is truncated"))?;
                let facts = match format {
                    HostedDecisionWire::Original => facts,
                    HostedDecisionWire::ImportPage => {
                        let (import, inline) = import_page::ImportPage::split(call, facts)?;
                        page = Some(import);
                        inline
                    }
                    HostedDecisionWire::Packed => {
                        packed = Some(packed_groups::PackedGroups::parse(call, facts)?);
                        &[0, 0]
                    }
                };
                (facts, control_envelope.slice_ref(controls))
            }
            HostedDecisionInput::Streamed { packed: groups, controls } => {
                if !body.is_empty() { return Err(invalid("Hosted decision prefix has trailing bytes")); }
                packed = Some(groups);
                (&[0, 0], controls.clone())
            }
        };
        let fact_groups = ordered_group_count(facts)?;
        let _framing = call.reserve((fact_groups + MAX_COMMIT_GROUPS) * 512)?;
        let facts = owned_groups(facts, false, true)?;
        let controls = owned_groups(&control_frame, true, false)?;
        let fact_relations = facts.iter().map(|(relation, _)| relation)
            .chain(page.iter().map(import_page::ImportPage::relation))
            .chain(packed.iter().flat_map(packed_groups::PackedGroups::relations))
            .collect::<BTreeSet<_>>();
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
        writes.extend(slot.map(|slot| TxOp::OwnedClockSlot { slot }));
        writes.extend(conditions);
        let (mut controls, _held) = self.decode_hosted_control_groups(call, &control_frame, controls, scope, &mut rows_left, false)?;
        if let Some(enrollment) = retired_enrollment {
            rtdb_tx::bind_retired_family_authority(&mut controls, authority, enrollment).map_err(status)?;
        } else {
            rtdb_tx::bind_installed_authority(&mut controls, authority).map_err(status)?;
        }
        if let Some(offer) = funded {
            let reading = offer.slot.reading;
            let end = reading.0.checked_add(1_000_000).ok_or_else(Self::clock_slot_missed)?;
            let held_seat = controls.iter().any(|control| control.is_check() && control.expected().is_some()
                && control.table().table == "bj_fact_root_admission"
                && matches!(control.id(), EntityId::String(id) if id == "funded_mutation_seat:v1"));
            if deadline <= reading || journal_before.is_none_or(|cutoff| cutoff.before().0 > end)
                || !worlds.is_empty() || !held_seat || !offer.admits_terminal(&controls)
            {
                return Err(invalid("Funded mutation requires the original guarded receipt and clock window"));
            }
        }
        let replay = rtdb_tx::hosted_replay_controls(controls.iter()).map_err(status)?.is_some();
        if replay && slot.is_some() {
            return Err(invalid("A replay cannot consume an ordered clock slot"));
        }
        if recheck_at_staging && (replay || controls.iter().all(rtdb_tx::HostedControlWrite::is_check)) {
            return Err(invalid("A staging deadline requires a live hosted control replacement"));
        }
        if replay && witnesses.iter().any(rtdb_node::DurableRange::requires_live_root) {
            return Err(invalid("Scoped evidence requires live Root custody"));
        }
        if replay && (counted || !worlds.is_empty()) {
            return Err(invalid("Native replay admits an original effect batch"));
        }
        let effects = CommitEffects::hosted(replay, counted || !worlds.is_empty(), &controls);
        let _facts = if let Some(page) = page {
            if replay { return Err(invalid("A hosted import page requires a live Root decision")); }
            page.decode(self, call, envelope, facts, &mut writes, &mut rows_left, effects)?
        } else if let Some(packed) = packed {
            packed.decode(self, call, envelope, &mut writes, &mut rows_left, effects)?
        } else {
            self.decode_owned_commit_groups(call, envelope, facts, &mut writes, &mut rows_left, effects)?
        };
        writes.extend(controls.into_iter().map(TxOp::HostedControl));
        writes.extend(witnesses.into_iter().map(rtdb_node::DurableRange::operation));
        let _refusal = if let Some(refusal) = refusal {
            if replay {
                return Err(invalid("A hosted constraint decision requires live Root custody"));
            }
            let (len, body) = refusal.split_first_chunk::<4>()
                .ok_or_else(|| invalid("Missing refusal fact stream length"))?;
            let (facts, controls) = body.split_at_checked(u32::from_be_bytes(*len) as usize)
                .ok_or_else(|| invalid("Truncated refusal fact stream"))?;
            let facts = hosted_refusal_fact_groups(facts)?;
            let controls = owned_groups(controls, true, false)?;
            if controls.is_empty() {
                return Err(invalid("A hosted refusal requires its original receipt groups"));
            }
            hosted_control::admit_control_targets(policy, controls.iter().map(|(relation, _)| relation))?;
            let mut alternative = Vec::new();
            let (controls, held) = self.decode_hosted_control_groups(
                call, &control_envelope, controls, scope, &mut rows_left, false,
            )?;
            if funded.is_some_and(|offer| !offer.admits_terminal(&controls)) {
                return Err(invalid("Funded mutation refusal requires the original receipt"));
            }
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
        self.owned_write_reply_with_reads(call, writes, Vec::new(), worlds, ipc_stream).await
    }

    fn decode_hosted_control_groups(
        &self, call: &Call, frame: &Bytes, groups: Vec<(OwnedRelation, &[u8])>, scope: rtdb_types::PlacedAttemptScope,
        rows_left: &mut usize, require_checks: bool,
    ) -> Result<(Vec<rtdb_tx::HostedControlWrite>, Vec<Arc<MemoryReservation>>), Status> {
        use rtdb_tx::HostedControlWrite;

        let mut held = Vec::with_capacity(groups.len());
        let mut controls = Vec::new();
        for (relation, arrow) in groups {
            let batch = request_batch(call, frame.slice_ref(arrow))?;
            let rows = batch.batch().num_rows();
            let schema = batch.batch().schema_ref();
            let (id_column, expected_column, check_column) = owned_landing_wire::hosted_check_columns();
            let exact_type = |name, kind| schema.field_with_name(name).is_ok_and(|field| field.data_type() == &kind);
            let unique = schema.fields().iter().map(|field| field.name()).collect::<BTreeSet<_>>();
            if rows == 0 || unique.len() != schema.fields().len() || schema.fields().len() > 131
                || (schema.index_of(check_column).is_ok() && !exact_type(check_column, DataType::Boolean))
                || !exact_type(id_column, DataType::Utf8) || !exact_type(expected_column, DataType::Int64)
            {
                return Err(invalid("Hosted controls require distinct fields, Utf8 IDs and explicit Int64 expectations"));
            }
            *rows_left = rows_left.checked_sub(rows).ok_or_else(|| invalid("Hosted commit exceeds its total row limit"))?;
            let reservation = call.reserve(wire::mul(rows, std::mem::size_of::<TxOp>() + 512)?)?;
            controls.try_reserve_exact(rows).map_err(|_| Status::resource_exhausted("Hosted control allocation failed"))?;
            held.push(reservation);
            if !require_checks && HostedControlWrite::supports_arrow_catalog(&relation)
                && schema.fields().iter().all(|field| field.metadata().is_empty())
            {
                let expected = batch.batch().column_by_name(expected_column).and_then(|column| column.as_any().downcast_ref::<Int64Array>())
                    .ok_or_else(|| invalid("Hosted control expectation must be Int64 or null"))?;
                let checks = batch.batch().column_by_name(check_column).and_then(|column| column.as_any().downcast_ref::<BooleanArray>());
                for row in 0..rows {
                    if checks.is_none_or(|checks| !checks.is_null(row) && !checks.value(row)) {
                        let positions = owned_payload_columns(batch.batch(), row, &["_expected_micros", "_check_only"])?;
                        if let Some(documents) = self.owned_payload(&relation, &batch, row, &positions)? {
                            let time = (!expected.is_null(row)).then(|| Timestamp(expected.value(row)));
                            if let Some(control) = HostedControlWrite::arrow_catalog(relation.clone(), scope, time, documents).map_err(status)? {
                                controls.push(control);
                                continue;
                            }
                        }
                    }
                    let (decoded, charge) = upload::owned_documents(&batch.batch().slice(row, 1), call.memory())?;
                    for row in decoded {
                        controls.push(hosted_control_row(&relation, scope, row, require_checks)?);
                    }
                    held.push(charge);
                }
                held.push(Arc::clone(batch.reservation()));
                continue;
            }
            let (rows, documents) = upload::owned_documents(batch.batch(), call.memory())?;
            for row in rows {
                controls.push(hosted_control_row(&relation, scope, row, require_checks)?);
            }
            held.push(documents);
        }
        Ok((controls, held))
    }

    async fn commit_owned(&self, call: &Call, envelope: &Bytes, body: &[u8], format: CommitFormat, witnesses: Witnesses, erase_allowed: bool, conditions: Vec<TxOp>, ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let group_limit = match format {
            CommitFormat::Single => 1,
            CommitFormat::Grouped => MAX_COMMIT_GROUPS,
            CommitFormat::Ordered => ordered_group_count(body.get(8..).unwrap_or_default())?,
        };
        let _framing = call.reserve(group_limit * 512)?;
        let CommitRequest { deadline, groups } = CommitRequest::parse(body, format)?;
        for (relation, _) in &groups {
            self.require_generic_owned_target(call, relation)?;
        }
        let mut rows_left = match format {
            CommitFormat::Single => usize::MAX,
            CommitFormat::Grouped | CommitFormat::Ordered => MAX_GROUPED_COMMIT_ROWS.checked_sub(conditions.len())
                .ok_or_else(|| invalid("Owned conditions exceed the commit limit"))?,
        };
        let mut writes = vec![TxOp::owned_deadline(deadline)];
        writes.extend(conditions);
        let effects = if erase_allowed { CommitEffects::Replay } else { CommitEffects::Facts };
        let _rows = self.decode_owned_commit_groups(call, envelope, groups, &mut writes, &mut rows_left, effects)?;
        self.owned_write_reply_with_witnesses(call, writes, witnesses, ipc_stream).await
    }

    fn decode_owned_commit_groups(
        &self, call: &Call, envelope: &Bytes, groups: Vec<(OwnedRelation, &[u8])>, writes: &mut Vec<TxOp>,
        rows_left: &mut usize, effects: CommitEffects,
    ) -> Result<Vec<(Arc<MemoryReservation>, Arc<MemoryReservation>)>, Status> {
        let mut retained = Vec::with_capacity(groups.len());
        for (relation, arrow) in groups {
            let batch = require_identity(decode_retained_batch(call, envelope.slice_ref(arrow))?)?;
            retained.push(self.append_decoded_owned_group(call, &relation, &batch, writes, rows_left, effects)?);
        }
        Ok(retained)
    }

    fn append_decoded_owned_group(
        &self, call: &Call, relation: &OwnedRelation, batch: &OwnedBatch, writes: &mut Vec<TxOp>,
        rows_left: &mut usize, effects: CommitEffects,
    ) -> Result<(Arc<MemoryReservation>, Arc<MemoryReservation>), Status> {
        let rows = batch.batch().num_rows();
        *rows_left = rows_left.checked_sub(rows).ok_or_else(|| invalid("Owned commit exceeds its total row limit"))?;
        let operations = wire::mul(rows, 2)?;
        let mut operations_charge = call.reserve(wire::mul(operations, std::mem::size_of::<TxOp>() + 512)?)?;
        writes.try_reserve_exact(operations)
            .map_err(|_| Status::resource_exhausted("Owned commit operation allocation failed"))?;
        if relation.is_root_data() || relation.name() == rtdb_types::OWNED_ROOT_PUBLICATION_RELATION {
            let charge = Arc::get_mut(&mut operations_charge).expect("commit group owns its operation reservation");
            for row in 0..rows {
                let start = writes.len();
                let bytes = charge.bytes();
                let arrow = relation.is_root_data() || ArrowCell::named(batch.batch(), "_op", row)
                    .and_then(upload::owned_operation).is_some_and(|op| matches!(op, "create" | "replace"));
                if !arrow || !self.append_owned_arrow_row(writes, relation, batch, row, effects, charge)? {
                    writes.truncate(start);
                    charge.shrink_to(bytes).map_err(status)?;
                    let (row, _decoded) = upload::owned_documents(&batch.batch().slice(row, 1), call.memory())?;
                    self.append_owned_commit_group(writes, relation, row, effects)?;
                }
            }
            return Ok((operations_charge, Arc::clone(batch.reservation())));
        }
        let (rows, documents_charge) = upload::owned_documents(batch.batch(), call.memory())?;
        self.append_owned_commit_group(writes, relation, rows, effects)?;
        Ok((operations_charge, documents_charge))
    }

    fn owned_payload(&self, relation: &OwnedRelation, batch: &OwnedBatch, row: usize, positions: &[usize]) -> Result<Option<ArrowDocuments>, Status> {
        let selected = batch.batch().slice(row, 1).project(positions).map_err(arrow)?;
        let selected = batch.with_retained_buffers(selected);
        if rtdb_tx::HostedControlWrite::supports_arrow_catalog(relation) {
            return ArrowDocuments::from_owned_payload(&selected, self.memory()).map_err(status);
        }
        if relation.is_root_data()
            && let Some(documents) = ArrowDocuments::from_owned_payload(&selected, self.memory()).map_err(status)?
        {
            return Ok(Some(documents));
        }
        for (position, field) in batch.batch().schema_ref().fields().iter().enumerate() {
            let cell = ArrowCell { field, array: batch.batch().column(position).as_ref(), row };
            if !positions.contains(&position) && !field.name().starts_with('_') && cell.is_undefined().map_err(status)? {
                return Ok(None);
            }
        }
        if relation.is_root_data() {
            ArrowDocuments::from_legacy_root_payload(&selected, self.memory()).map_err(status)
        } else {
            ArrowDocuments::from_legacy_owned_payload(&selected, self.memory()).map_err(status)
        }
    }

    fn append_owned_arrow_row(
        &self, writes: &mut Vec<TxOp>, relation: &OwnedRelation, batch: &OwnedBatch, row: usize, effects: CommitEffects, charge: &mut MemoryReservation,
    ) -> Result<bool, Status> {
        let source = batch.batch();
        let controls = [
            "_op", "_read_valid_micros", "_read_system_micros", "_read_event_position", "_read_revealed", "_valid_from", "_valid_to",
            "_valid_from_offset_micros", "_valid_to_offset_micros", "_archive_origin",
        ];
        let positions = owned_payload_columns(source, row, &controls)?;
        let cell = |name| ArrowCell::named(source, name, row);
        let id = {
            let cell = cell("_id").ok_or_else(|| invalid("Owned commit row has no _id"))?;
            read_owned_entity_id(cell.field, cell.array, cell.row).map_err(status)?
        };
        let mut own_id = || -> Result<EntityId, Status> {
            charge.grow_to(wire::add(charge.bytes(), id.owned_payload_bytes())?).map_err(status)?;
            Ok(id.into_owned())
        };
        let operation = cell("_op").and_then(upload::owned_operation).ok_or_else(|| invalid("Owned commit _op must be Utf8"))?;
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
        if operation == "adopt-relation" && matches!(effects, CommitEffects::HostedRoot) {
            if source.num_columns() != 4 || !matches!(id, EntityIdRef::String(name) if name == relation.name()) {
                return Err(invalid("An adoption row must name its source, destination and retention"));
            }
            let destination = cell("destination").and_then(ArrowCell::utf8)
                .ok_or_else(|| invalid("An adoption must name its staged destination"))?;
            let retain_micros = cell("retain_micros").and_then(ArrowCell::int64)
                .ok_or_else(|| invalid("An adoption must carry its signed microsecond retention"))?;
            charge.grow_to(wire::add(charge.bytes(), relation.name().len() + destination.len())?).map_err(status)?;
            writes.push(TxOp::OwnedAdoptRelation {
                source: relation.clone(), destination: OwnedRelation::new(destination).map_err(status)?, retain_micros,
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
        let position = optional("_read_event_position")?.map(|cell| {
            cell.binary().ok_or_else(|| invalid("Owned observation position must be Binary"))
        }).transpose()?;
        let expected = owned_point_expectation(signed("_read_system_micros")?, position)?;
        let revealed = optional("_read_revealed")?.map(|cell| {
            cell.binary().and_then(rtdb_types::OwnedRevealed::from_wire)
                .ok_or_else(|| invalid("Invalid owned revealed projection"))
        }).transpose()?;
        match valid_at {
            Some(valid_at) => writes.push(TxOp::OwnedCheck { relation: relation.clone(), id: own_id()?, valid_at, expected, revealed }),
            None if expected.is_some() || revealed.is_some() => return Err(invalid("Owned point expectation has no valid instant")),
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
        let archive_origin = optional("_archive_origin")?.map(|cell| {
            cell.binary().ok_or_else(|| invalid("Archive residue origin must be Binary"))
        }).transpose()?;
        if operation != "redacted" && archive_origin.is_some() {
            return Err(invalid("An archive origin belongs to a residue"));
        }
        let from_offset = signed("_valid_from_offset_micros")?;
        let to_offset = signed("_valid_to_offset_micros")?;
        if from_offset.is_some() || to_offset.is_some() {
            if !relation.is_root_data() || matches!(effects, CommitEffects::Replay) {
                return Err(invalid("Invalid relative Root period"));
            }
            let period = OwnedRelativePeriod::from_fields(valid_from, valid_to, from_offset, to_offset)
                .ok_or_else(|| invalid("A relative Root period needs one coordinate for each finite bound"))?;
            let effect = match operation {
                "create" | "replace" => {
                    let Some(documents) = self.owned_payload(relation, batch, row, &positions)? else { return Ok(false); };
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
                let Some(documents) = self.owned_payload(relation, batch, row, &positions)? else { return Ok(false); };
                TxOp::OwnedArrowDocs { relation: relation.clone(), documents, create_only: operation == "create", valid_from, valid_to }
            }
            "delete" => {
                if has_payload()? { return Err(invalid("Owned delete carries document values")); }
                TxOp::OwnedDelete { relation: relation.clone(), id: own_id()?, valid_from, valid_to }
            }
            "redacted" if matches!(effects, CommitEffects::Replay) && relation.is_root_data() => {
                if valid_at.is_some() || has_payload()? {
                    return Err(invalid("An archive residue carries only its origin, identity and extent"));
                }
                replay_residue(relation, own_id()?, archive_origin, valid_from, valid_to)?
            }
            "erase" if effects.allows_erase(relation) => {
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
            let id = row.get("_id").ok_or_else(|| invalid("Owned commit row has no _id"))?.owned_entity_id().map_err(status)?;
            let Value::Utf8(operation) = row.remove("_op").ok_or_else(|| invalid("Owned commit row has no _op"))? else {
                return Err(invalid("Owned commit _op must be Utf8"));
            };
            let valid_at = optional_micros(row.remove("_read_valid_micros"))?;
            let position = match row.remove("_read_event_position") {
                None | Some(Value::Null | Value::Undefined) => None,
                Some(Value::Binary(bytes)) => Some(bytes),
                _ => return Err(invalid("Owned observation position must be Binary")),
            };
            let expected = owned_point_expectation(optional_micros(row.remove("_read_system_micros"))?, position.as_deref())?;
            let revealed = match row.remove("_read_revealed") {
                None | Some(Value::Null | Value::Undefined) => None,
                Some(Value::Binary(bytes)) => Some(rtdb_types::OwnedRevealed::from_wire(&bytes)
                    .ok_or_else(|| invalid("Invalid owned revealed projection"))?),
                _ => return Err(invalid("Owned revealed projection must be Binary")),
            };
            match valid_at {
                Some(valid_at) => writes.push(TxOp::OwnedCheck { relation: relation.clone(), id: id.clone(), valid_at, expected, revealed }),
                None if expected.is_some() || revealed.is_some() => return Err(invalid("Owned point expectation has no valid instant")),
                None => (),
            }
            let archive_origin = match row.remove("_archive_origin") {
                None | Some(Value::Null | Value::Undefined) => None,
                Some(Value::Binary(bytes)) if operation == "redacted" => Some(bytes),
                _ => return Err(invalid("An archive origin belongs to a residue")),
            };
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
                "redacted" if matches!(effects, CommitEffects::Replay) && relation.is_root_data() => {
                    if valid_at.is_some() || has_payload(&row) {
                        return Err(invalid("An archive residue carries only its origin, identity and extent"));
                    }
                    replay_residue(relation, id, archive_origin.as_deref(), valid_from, valid_to)?
                }
                "erase" if effects.allows_erase(relation) => {
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

    async fn owned_write_reply(&self, call: &Call, operations: Vec<TxOp>, ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
        self.owned_write_reply_with_witnesses(call, operations, Vec::new(), ipc_stream).await
    }

    async fn owned_write_reply_with_witnesses(
        &self, call: &Call, operations: Vec<TxOp>, witnesses: Witnesses, ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
        self.owned_write_reply_with_reads(call, operations, witnesses, Vec::new(), ipc_stream).await
    }

    async fn owned_write_reply_with_reads(
        &self, call: &Call, mut operations: Vec<TxOp>, witnesses: Witnesses,
        mut worlds: Vec<HostedWorldRead>, ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
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
        self.owned_tx_reply(&native, outcome.tx_key(), ipc_stream).await
    }

    async fn owned_tx_reply(&self, native: &NativeSession, tx: rtdb_types::TransactionKey, ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
        let original = native.owned_commit_observation(tx).await.map_err(status)?;
        let mut clock = arrow_array::builder::FixedSizeBinaryBuilder::with_capacity(1, rtdb_tx::OrderedClockEvidence::WIDTH);
        match original {
            Some(sample) => clock.append_value(sample.encode()).map_err(arrow)?,
            None => clock.append_null(),
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("tx_id", DataType::Int64, false),
            Field::new("system_time_micros", DataType::Int64, false),
            Field::new("decision_clock", DataType::FixedSizeBinary(rtdb_tx::OrderedClockEvidence::WIDTH), true),
        ]));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
            Arc::new(Int64Array::from(vec![tx.tx_id.0])),
            Arc::new(Int64Array::from(vec![tx.system_time.0])),
            Arc::new(clock.finish()),
        ]).map_err(arrow)?;
        self.encode_owned_stream_reply(self.memory(), &schema, &[batch], ipc_stream)
    }

    pub(crate) async fn open_owned_read_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        if !body.is_empty() { return Err(invalid("Owned read open takes an empty body")); }
        self.retain_new_owned_read(call, None).await
    }

    pub(crate) async fn open_fresh_owned_read_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let purpose = match call.classic_retained {
            Some(retained) if retained.deadline.is_live() => ReadPurpose::ClassicRetained(retained.scope_sha256),
            Some(_) => return Err(Status::permission_denied("The Classic retained read grant expired")),
            None if call.classic_flight_excluded => return Err(crate::service::classic_flight_refusal()),
            None => ReadPurpose::OrdinaryFresh,
        };
        if call.session.placed_database() != Some(call.database.as_str()) {
            return Err(Status::permission_denied("A fresh owned read requires its ordinary placed session"));
        }
        let (deployment, suffix) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Fresh read requires a deployment"))?;
        let deployment = Uuid::from_bytes(*deployment);
        let required = if matches!(suffix, [0]) {
            None
        } else {
            let (floor, rest) = hosted_observation::applied_read_floor(body)?;
            if !rest.is_empty() { return Err(invalid("Unexpected fresh read data")); }
            Some(floor)
        };
        let permit = Arc::clone(&self.0.owned_read_slots).try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("The node has too many retained owned reads"))?;
        let connection = self.connection(call, &call.database).await?;
        let mut native = connection.borrow().await?;
        require_idle(&native)?;
        let snapshot = native.open_fresh_owned_read(deployment, required.as_ref()).await.map_err(status)?;
        if call.classic_retained.is_some_and(|retained| !retained.deadline.is_live() || retained.snapshot != snapshot.present()) {
            return Err(Status::failed_precondition("The retained read did not capture the authorized Classic snapshot"));
        }
        self.retain_owned_read(call, snapshot, permit, purpose)
    }

    pub(crate) async fn open_applied_owned_read_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        if call.session.placed_database() != Some(call.database.as_str()) {
            return Err(Status::permission_denied("An applied owned read requires its placed owner session"));
        }
        let (floor, rest) = hosted_observation::applied_read_floor(body)?;
        let generation = u64::from_be_bytes(rest.try_into().map_err(|_| invalid("Applied read requires an exact generation"))?);
        if generation != 0 {
            return Err(invalid("An ordinary applied read uses generation zero"));
        }
        self.retain_new_owned_read(call, Some(&floor)).await
    }

    async fn retain_new_owned_read(
        &self, call: &Call, applied_floor: Option<&rtdb_node::AppliedReadFloor>,
    ) -> Result<ActionResult, Status> {
        let permit = Arc::clone(&self.0.owned_read_slots).try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("The node has too many retained owned reads"))?;
        let connection = self.connection(call, &call.database).await?;
        let mut native = connection.borrow().await?;
        require_idle(&native)?;
        let snapshot = match applied_floor {
            Some(minimum) => native.open_applied_owned_read(minimum).await,
            None => native.open_owned_read().await,
        }.map_err(status)?;
        self.retain_owned_read(call, snapshot, permit, ReadPurpose::Other)
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
        self.retain_owned_read(call, snapshot, permit, original.purpose)
    }

    pub(crate) async fn open_owned_read_as_of_action(&self, call: &Call, body: &[u8], positioned: bool) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, body) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Historical read requires an existing held handle"))?;
        let (system, exact, body) = if positioned {
            let (cut, body) = split_world_publication(body)?;
            (cut.system_time, Some(cut), body)
        } else {
            let (requested, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Historical read requires a requested system time"))?;
            (Timestamp(i64::from_be_bytes(*requested)), None, body)
        };
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
        let snapshot = match exact {
            Some(cut) => held.snapshot.derive_at_publication(cut, &publication, &marker, relations).await,
            None => held.snapshot.derive_as_of(system, &publication, &marker, relations).await,
        }.map_err(status)?;
        held.snapshot.check_relation(&publication).map_err(status)?;
        match snapshot {
            Some(snapshot) => self.retain_owned_read(call, snapshot, permit, held.purpose),
            None => Ok(ActionResult { body: Bytes::from_static(&[0]) }),
        }
    }

    fn retain_owned_read(
        &self, call: &Call, snapshot: OwnedReadSession, permit: OwnedSemaphorePermit, purpose: ReadPurpose,
    ) -> Result<ActionResult, Status> {
        let reservation = call.reserve(wire::add(2048, wire::add(call.session.id.len(), call.database.len())?)?)?;
        let reply_charge = call.reserve(64)?;
        let handle = Uuid::new_v4();
        let mut reply = Vec::with_capacity(40);
        reply.extend_from_slice(handle.as_bytes());
        reply.extend_from_slice(&snapshot.system_time().0.to_be_bytes());
        let position = snapshot.historical_publication().unwrap_or_else(|| snapshot.present()).tx_id;
        reply.extend_from_slice(&position.0.to_be_bytes());
        reply.extend_from_slice(&snapshot.query_time().map_err(status)?.0.to_be_bytes());
        let held = Arc::new(OwnedReadResource {
            snapshot, owner: call.session.owner(), session: call.session.id.clone(), database: call.database.clone(), purpose,
            witnesses: Mutex::default(), spatial_filters: Mutex::default(), _permit: permit, _reservation: reservation,
        });
        let handle = Bytes::copy_from_slice(handle.as_bytes());
        call.session.register_read(||
            self.0.owned_reads.insert(handle, held, READ_IDLE, READ_IDLE, false, 16).map_err(status))?;
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
        let entry = found(self.0.owned_reads.lookup(handle, false), "owned read")?;
        if entry.session != call.session.id || entry.database != call.database || !call.session.owns(&entry.owner) {
            return Err(Status::not_found("unknown owned read"));
        }
        match (entry.purpose, call.classic_retained) {
            (ReadPurpose::OrdinaryFresh, None) if call.classic_flight_excluded => {
                self.0.owned_reads.take(handle);
                entry.close();
                return Err(crate::service::classic_flight_refusal());
            }
            (ReadPurpose::ClassicRetained(expected), Some(admission))
                if admission.scope_sha256 == expected && admission.deadline.is_live() => {}
            (ReadPurpose::ClassicRetained(_), _) | (_, Some(_)) => return Err(Status::not_found("unknown owned read")),
            _ => {}
        }
        Ok(entry)
    }

    pub(crate) async fn held_owned_point(&self, call: &Call, body: &Bytes, time: PointTime, compact: Option<CompactIdentity>, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned held point has no handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.retained_owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Ok(entry) = self.take_owned_read(call, &handle) { entry.close(); }
            return Err(status(error));
        }
        let (relation, id, valid, _identity) = selected_point_request(call, body, request, time, compact)?;
        let fixed = matches!(time, PointTime::Required);
        self.execute_owned_point(&held.snapshot, &relation, &id, valid, fixed, Some((call, &held)), reply_format).await
    }

    pub(crate) async fn held_owned_absent_keys_action(&self, call: &Call, body: &Bytes) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        if body.len() > 96 * 1024 {
            return Err(invalid("Owned absence request exceeds its byte bound"));
        }
        let (handle, request) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Owned absence probe has no handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Ok(entry) = self.take_owned_read(call, &handle) { entry.close(); }
            return Err(status(error));
        }
        let (relation, arrow) = split_relation(request)?;
        if held.snapshot.is_scoped_root() || !relation.is_root_data()
            || held.snapshot.lifetime_point_policy(&relation).await.map_err(status)?.is_some()
        {
            return Err(unsupported());
        }

        let request = require_identity(decode_retained_batch(call, body.slice_ref(arrow))?)?;
        let batch = request.batch();
        let valid = absent_keys_valid(batch)?;
        let mut absent = 0;
        for row in 0..batch.num_rows() {
            let (id, _identity) = point_identity_at(call, batch, row)?;
            if let Some(mut cursor) = held.snapshot.open_point_at(&relation, &id, Some(valid)).await.map_err(status)? {
                let mut present = false;
                while let Some(page) = cursor.next_batch().await.map_err(status)? {
                    if page.batch().num_rows() != 0 {
                        present = true;
                        break;
                    }
                }
                if present { break; }
                // Empty ordinary point replies still validate the cursor schema.
                let _schema = wire::plan_schema(cursor.runtime_schema(), call.memory())?;
            }
            held.snapshot.check_live_point_relation(&relation).map_err(status)?;
            absent += 1;
        }
        held.snapshot.check_live_point_relation(&relation).map_err(status)?;
        let retained = call.reserve(18)?;
        let mut reply = Vec::with_capacity(18);
        reply.push(1);
        reply.extend_from_slice(&held.snapshot.system_time().0.to_be_bytes());
        reply.extend_from_slice(&valid.0.to_be_bytes());
        reply.push(u8::try_from(absent).expect("request has at most 64 keys"));
        Ok(ActionResult { body: wire::retain(Bytes::from(reply), retained) })
    }

    pub(crate) async fn held_owned_historical_point(&self, call: &Call, body: &Bytes, compact: Option<CompactIdentity>, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned historical point has no handle"))?;
        let (system, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Owned historical point has no system time"))?;
        let system = Timestamp(i64::from_be_bytes(*system));
        let held = self.retained_owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, id, valid, _identity) = selected_point_request(call, body, request, PointTime::Optional, compact)?;
        let valid = valid.unwrap_or(system);
        let cursor = held.snapshot.open_historical_point(&relation, &id, system, valid).await.map_err(status)?;
        let reply = self.finish_owned_point(call, system, valid, &id, cursor, None, reply_format).await?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_owned_position_point(&self, call: &Call, body: &Bytes, compact: Option<CompactIdentity>, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned position point has no handle"))?;
        let (system, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Owned position point has no system time"))?;
        let system = Timestamp(i64::from_be_bytes(*system));
        let (mask, request) = split_position_mask(request)?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, id, valid, _identity) = selected_point_request(call, body, request, PointTime::Required, compact)?;
        let valid = valid.ok_or_else(|| invalid("Owned position point has no valid instant"))?;
        let cursor = held.snapshot.open_historical_position_point(&relation, &id, system, valid, mask).await.map_err(status)?;
        let reply = self.finish_owned_point(call, system, valid, &id, cursor, None, reply_format).await?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_root_cut_point_action(&self, call: &Call, body: &Bytes, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Root cut point has no hold"))?;
        let (history, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Root cut point has no system time"))?;
        let (position, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Root cut point has no transaction position"))?;
        let (side, request) = request.split_first().ok_or_else(|| invalid("Root cut point has no side"))?;
        let before = match side {
            0 => true,
            1 => false,
            _ => return Err(invalid("Invalid Root cut point side")),
        };
        let system_time = Timestamp(i64::from_be_bytes(*history));
        let publication = rtdb_types::TransactionKey { system_time, tx_id: rtdb_types::TxId(i64::from_be_bytes(*position)) };
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, id, valid, _identity) = point_request(call, body, request, PointTime::Required)?;
        let valid = valid.ok_or_else(|| invalid("Root cut point has no valid instant"))?;
        let cursor = held.snapshot.open_root_cut_point(&relation, &id, publication, valid, before).await.map_err(status)?;
        let reply = self.finish_owned_point(call, system_time, valid, &id, cursor, None, reply_format).await?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    fn path_touch_request(
        &self, call: &Call, envelope: &Bytes, request: &[u8],
    ) -> Result<(Timestamp, Timestamp, OwnedRelation, OwnedRelation, EntityId, Timestamp, Arc<MemoryReservation>), Status> {
        let (group, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Owned path touch has no group time"))?;
        let group = Timestamp(i64::from_be_bytes(*group));
        let before = Timestamp(group.0.checked_sub(1).ok_or_else(|| invalid("Owned path touch has no preceding cut"))?);
        let (relation, request) = split_relation(request)?;
        let (path, arrow_bytes) = split_relation(request)?;
        let batch = request_batch(call, call.retain_request_bytes(envelope.slice_ref(arrow_bytes)))?;
        let batch = batch.batch();
        if batch.num_columns() != 2 || batch.num_rows() != 1 {
            return Err(invalid("Owned path touch requires one identity and valid instant"));
        }
        let (id, identity) = point_identity(call, batch)?;
        let valid = history_request_endpoint(batch, "valid_micros")?;
        Ok((before, group, relation, path, id, valid, identity))
    }

    pub(crate) async fn held_world_version_action(
        &self, call: &Call, body: &Bytes,
    ) -> Result<Vec<Result<ActionResult, Status>>, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("World version has no held handle"))?;
        let (group, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("World version has no group time"))?;
        let group = Timestamp(i64::from_be_bytes(*group));
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = request_batch(call, call.retain_request_bytes(body.slice_ref(arrow_bytes)))?;
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
            let reply = self.world_point_reply_with_extent(call, point, interval.lower(), None, Some(interval), OwnedReplyFormat::Unary)?.into_unary();
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
        &self, call: &Call, body: &Bytes, images: bool,
    ) -> Result<Vec<Result<ActionResult, Status>>, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("World path touch has no held handle"))?;
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let (before, group, relation, path, id, valid, _identity) = self.path_touch_request(call, body, request)?;
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
        &self, call: &Call, body: &Bytes,
    ) -> Result<Vec<Result<ActionResult, Status>>, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned path touch has no held read handle"))?;
        let (position, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Owned path touch has no transaction position"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (_, group, relation, path, id, valid, _identity) = self.path_touch_request(call, body, request)?;
        let publication = rtdb_types::TransactionKey { system_time: group, tx_id: rtdb_types::TxId(i64::from_be_bytes(*position)) };
        let columns = held.snapshot.path_touch_columns(&relation, &path, &id, publication, valid).await.map_err(status)?;
        let mut replies = Vec::with_capacity(2);
        for before in [true, false] {
            let cursor = held.snapshot.open_root_cut_point(&relation, &id, publication, valid, before).await.map_err(status)?;
            let reply = self.finish_owned_point_inner(call.memory(), group, valid, &id, cursor, None, Some(&columns), None, OwnedReplyFormat::Unary, |_| Ok(None)).await?.into_unary();
            replies.push(Ok(reply));
        }
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(replies)
    }

    pub(crate) async fn held_owned_valid_during_action(
        &self, call: &Call, body: &Bytes, mode: ValidHistoryMode, ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
        self.held_owned_valid_during(call, body, mode, None, ipc_stream).await
    }

    pub(crate) async fn held_owned_valid_during(
        &self, call: &Call, body: &Bytes, mode: ValidHistoryMode, compact: Option<CompactIdentity>, ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
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
        let (relation, request) = split_relation(request)?;
        let legacy = compact.is_none().then(|| request_batch(call, call.retain_request_bytes(body.slice_ref(request)))).transpose()?;
        let (id, _identity, system, valid) = if let Some(compact) = compact {
            let (system, request) = selection_time(request)?;
            let (from, request) = selection_time(request)?;
            let (to, request) = selection_time(request)?;
            let (id, identity) = selection_identity(call, request, compact)?;
            let valid = Interval::new(from, to).map_err(status)?;
            if valid.is_empty() { return Err(invalid("Owned history requires a nonempty valid window")); }
            (id, identity, system, valid)
        } else {
            let batch = legacy.as_ref().expect("scalar window request").batch();
            if batch.num_columns() != 4 || batch.num_rows() != 1 {
                return Err(invalid("Owned history requires one row with _id, a system cut and two valid endpoints"));
            }
            let (id, identity) = point_identity(call, batch)?;
            let from = history_request_endpoint(batch, "valid_from_micros")?;
            let to = history_request_endpoint(batch, "valid_to_micros")?;
            let valid = Interval::new(from, to).map_err(status)?;
            if valid.is_empty() { return Err(invalid("Owned history requires a nonempty valid window")); }
            let system = history_request_endpoint(batch, "system_micros")?;
            (id, identity, system, valid)
        };

        let history = match mask {
            Some(mask) => held.snapshot.open_historical_position_valid_during(&relation, &id, valid, system, mask).await,
            None if matches!(mode, ValidHistoryMode::Region) =>
                held.snapshot.open_valid_during_region(&relation, &id, valid, system).await,
            None => held.snapshot.open_valid_during_observed(&relation, &id, valid, system).await,
        }.map_err(status)?;
        let read = HistoryRead::Observed { read: history, held: &held, relation: &relation };
        let reply = self.history_reply(call, &id, read, valid, system, None, ipc_stream).await?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_owned_identities_action(
        &self, call: &Call, body: &Bytes, reply_format: OwnedReplyFormat<'_>,
    ) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Owned identity page has no held read handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = decode_batch(call, call.retain_request_bytes(body.slice_ref(arrow_bytes)))?;
        let batch = batch.batch();
        if batch.num_columns() != 4 || batch.num_rows() != 1 {
            return Err(invalid("Owned identity page requires one row with a cut, a bound, and a limit"));
        }
        let system = history_request_endpoint(batch, "system_micros")?;
        let valid = history_request_endpoint(batch, "valid_micros")?;
        let limit = usize::try_from(history_request_integer(batch, "limit")?)
            .map_err(|_| invalid("Owned identity limit is out of range"))?;
        let after = identity_scan_request(batch)?;
        let page = held.snapshot.identity_page(&relation, system, valid, &after, limit).await.map_err(status)?;
        let (rows, _copy) = identity_scan_reply(call, &after, &page.keys)?;
        let schema = rows.schema_ref().as_ref().clone().with_metadata(
            [(OWNED_IDENTITY_MORE.into(), u8::from(page.more).to_string())].into()
        );
        let reply = self.encode_owned_point_reply(self.memory(), &schema, Some(&rows), system, valid, None, None, reply_format)?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    /// Identities current at the held system cut across all valid time,
    /// for a period table's bounded index build turns.
    pub(crate) async fn held_owned_identities_all_valid_action(
        &self, call: &Call, body: &Bytes, reply_format: OwnedReplyFormat<'_>,
    ) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Owned identity page has no held read handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = decode_batch(call, call.retain_request_bytes(body.slice_ref(arrow_bytes)))?;
        let batch = batch.batch();
        if batch.num_columns() != 3 || batch.num_rows() != 1 {
            return Err(invalid("An all-valid identity page requires one row with a cut, a limit and a bound"));
        }
        let system = history_request_endpoint(batch, "system_micros")?;
        let limit = usize::try_from(history_request_integer(batch, "limit")?)
            .map_err(|_| invalid("Owned identity limit is out of range"))?;
        let after = identity_scan_request(batch)?;
        let page = held.snapshot.identity_page_all_valid(&relation, system, &after, limit).await.map_err(status)?;
        let (rows, _copy) = identity_scan_reply(call, &after, &page.keys)?;
        let schema = rows.schema_ref().as_ref().clone().with_metadata(
            [(OWNED_IDENTITY_MORE.into(), u8::from(page.more).to_string())].into()
        );
        // The reply's valid coordinate is the system cut it enumerated.
        let reply = self.encode_owned_point_reply(self.memory(), &schema, Some(&rows), system, system, None, None, reply_format)?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_protected_coverage_action(&self, call: &Call, body: &Bytes, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        use base64::{Engine, engine::general_purpose::STANDARD};
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        self.hosted_control_policy(call)?;
        let scope = self.hosted_control_scope(call).await?;
        let (handle, request) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Protected carrier coverage has no held handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = decode_batch(call, call.retain_request_bytes(body.slice_ref(arrow_bytes)))?;
        let batch = batch.batch();
        if batch.num_columns() != 1 || batch.num_rows() != 1 {
            return Err(invalid("Protected carrier coverage needs one prefix"));
        }
        let prefix = protected_carrier_prefix(batch)?;
        let (coverage, witness) = held.snapshot.protected_carrier_coverage(&relation, prefix, scope)
            .await.map_err(status)?;
        let nonce = held.remember(call, &relation, witness)?;
        let schema = Schema::empty().with_metadata([
            ("rtdb.owned.protected_carrier_coverage".into(), STANDARD.encode(coverage)),
            (OWNED_RANGE_WITNESS.into(), nonce.simple().to_string()),
        ].into());
        let at = held.snapshot.present().system_time;
        let reply = self.encode_owned_point_reply(self.memory(), &schema, None, at, at, None, None, reply_format)?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_row_carrier_exists_action(&self, call: &Call, body: &Bytes) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        self.hosted_control_policy(call)?;
        let scope = self.hosted_control_scope(call).await?;
        let (handle, request) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Protected row existence needs its held handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, arrow) = split_relation(request)?;
        let batch = request_batch(call, call.retain_request_bytes(body.slice_ref(arrow)))?;
        let batch = batch.batch();
        let fields = batch.schema_ref().fields();
        if batch.num_rows() != 1 || fields.len() != 2
            || fields[0].as_ref() != &Field::new("_id", DataType::Binary, false)
            || fields[1].as_ref() != &Field::new("valid_micros", DataType::Int64, true)
            || batch.column(1).is_valid(0)
        {
            return Err(invalid("Protected row existence needs one Binary identity and a null valid_micros"));
        }
        let (id, _identity) = point_identity(call, batch)?;
        let present = held.snapshot.protected_row_carrier_exists(&relation, &id, scope).await.map_err(status)?;
        Ok(ActionResult { body: wire::retain(vec![u8::from(present)], call.reserve(64)?) })
    }

    pub(crate) async fn held_protected_carriers_action(&self, call: &Call, body: &Bytes, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        self.hosted_control_policy(call)?;
        let scope = self.hosted_control_scope(call).await?;
        let (handle, request) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Protected carrier page has no held handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = decode_batch(call, call.retain_request_bytes(body.slice_ref(arrow_bytes)))?;
        let batch = batch.batch();
        if batch.num_columns() != 3 || batch.num_rows() != 1 {
            return Err(invalid("Protected carrier page requires a prefix, bound, and limit"));
        }
        let prefix = protected_carrier_prefix(batch)?;
        let scan = identity_scan_request(batch)?;
        let OwnedIdentityScan::Binary(after) = &scan else {
            return Err(invalid("Protected carrier bound must be Binary"));
        };
        let limit = usize::try_from(history_request_integer(batch, "limit")?)
            .map_err(|_| invalid("Protected carrier page limit is out of range"))?;
        let (page, witness) = held.snapshot.protected_carrier_page(
            &relation, prefix, after.as_deref(), limit, scope,
        ).await.map_err(status)?;
        self.held_prefix_reply(call, &held, &relation, &scan, page, witness, reply_format)
    }

    pub(crate) async fn held_sealed_search_unit_rows_action(
        &self, call: &Call, body: &Bytes, reply_format: OwnedReplyFormat<'_>,
    ) -> Result<OwnedIpcReply, Status> {
        if !self.supports_hosted_upload(call) { return Err(unsupported()); }
        let scope = self.hosted_control_scope(call).await?;
        let (handle, request) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Sealed search unit page has no held handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = decode_batch(call, call.retain_request_bytes(body.slice_ref(arrow_bytes)))?;
        let (prefix, after, limit) = sealed_search_unit_page_request(batch.batch())?;
        let (page, witness, original) = held.snapshot.sealed_search_unit_rows_page(
            &relation, prefix, after, limit, scope,
        ).await.map_err(status)?;
        let reply = self.held_prefix_reply(call, &held, &relation, &OwnedIdentityScan::binary(after), page, witness, reply_format)?;
        original().map_err(status)?;
        Ok(reply)
    }

    fn held_prefix_reply(
        &self, call: &Call, held: &OwnedReadResource, relation: &OwnedRelation, scan: &OwnedIdentityScan<'_>,
        page: rtdb_node::OwnedIdentityPage, witness: Option<OwnedRangeWitness>, reply_format: OwnedReplyFormat<'_>,
    ) -> Result<OwnedIpcReply, Status> {
        let (rows, _copy) = identity_scan_reply(call, scan, &page.keys)?;
        let mut metadata = std::collections::HashMap::from([
            (OWNED_IDENTITY_MORE.into(), u8::from(page.more).to_string()),
        ]);
        if let Some(witness) = witness {
            let nonce = held.remember(call, relation, witness)?;
            metadata.insert(OWNED_RANGE_WITNESS.into(), nonce.simple().to_string());
        }
        let schema = rows.schema_ref().as_ref().clone().with_metadata(metadata);
        let at = held.snapshot.present().system_time;
        let reply = self.encode_owned_point_reply(self.memory(), &schema, Some(&rows), at, at, None, None, reply_format)?;
        held.snapshot.check_relation(relation).map_err(status)?;
        Ok(reply)
    }

    fn owned_versions_request(
        &self, call: &Call, envelope: &Bytes, request: &[u8],
    ) -> Result<(OwnedRelation, EntityId, OwnedVersionRequest, Arc<MemoryReservation>), Status> {
        use rtdb_types::Interval;

        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = request_batch(call, call.retain_request_bytes(envelope.slice_ref(arrow_bytes)))?;
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
        &self, call: &Call, body: &Bytes, images: bool,
    ) -> Result<Vec<Result<ActionResult, Status>>, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Earlier parent touch has no held handle"))?;
        let (selected, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Earlier parent touch has no selected publication"))?;
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let selected = Timestamp(i64::from_be_bytes(*selected));
        let (before, group, relation, path, id, valid, _identity) = self.path_touch_request(call, body, request)?;
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
        &self, call: &Call, body: &[u8], ancestor: bool, ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, rest) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Earlier parent changes require a held world"))?;
        let (selected, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("Earlier parent changes require a selected publication"))?;
        let (after, rest) = split_change_edge(rest)?;
        let (through, rest) = split_change_edge(rest)?;
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
        let limit = usize::from(u16::from_be_bytes(*limit));
        let (page, witness) = if ancestor {
            held.snapshot.ancestor_changes(&relation, selected, after, through, limit, reverse).await
        } else {
            held.snapshot.parent_changes_at(&relation, selected, after, through, limit, reverse).await
        }.map_err(status)?;
        let nonce = held.remember(call, witness, false)?;
        let reply = self.owned_merged_changes_reply(&page, selected, reverse, Some(nonce), ipc_stream)?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_world_parent_versions_action(
        &self, call: &Call, body: &Bytes,
    ) -> Result<Vec<ActionResult>, Status> {
        use rtdb_types::Interval;
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Earlier parent versions require a held world"))?;
        let (selected, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Earlier parent versions have no selected publication"))?;
        let selected = Timestamp(i64::from_be_bytes(*selected));
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let (relation, id, query, _identity) = self.owned_versions_request(call, body, request)?;
        let (page, witness) = held.snapshot.parent_versions_at(&relation, &id, selected, query).await.map_err(status)?;
        let _framing = call.reserve(wire::mul(page.versions.len() + 1, 512)?)?;
        let header_charge = call.reserve(64)?;
        let mut header = owned_version_header(page.next_revision, page.done, page.floor, page.scanned)?;
        let mut bytes = ActionResult { body: Bytes::from_static(&[0; 35]) }.encoded_len();
        let mut results = Vec::with_capacity(page.versions.len() + 1);
        for version in page.versions {
            let reply = self.history_reply(call, &id, HistoryRead::Archived(version.timeline.into_iter()),
                Interval::ALL, selected, Some((version.sequence, version.system)), false).await?.into_unary();
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

    pub(crate) async fn held_owned_versions_action(&self, call: &Call, body: &Bytes) -> Result<Vec<ActionResult>, Status> {
        use rtdb_types::Interval;

        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned versions has no held handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Ok(entry) = self.take_owned_read(call, &handle) { entry.close(); }
            return Err(status(error));
        }
        let (relation, id, request, _identity) = self.owned_versions_request(call, body, request)?;
        let page = held.snapshot.versions(&relation, &id, request).await.map_err(status)?;
        let present = held.snapshot.system_time();
        let _framing = call.reserve(wire::mul(page.versions.len() + 1, 512)?)?;
        let header_charge = call.reserve(64)?;
        let mut header = owned_version_header(page.next_revision, page.done, page.floor, page.scanned)?;

        let mut bytes = 0;
        let mut results = Vec::with_capacity(page.versions.len() + 1);
        for version in page.versions {
            let reply = self.history_reply(call, &id, HistoryRead::Version(version.timeline), Interval::ALL, present,
                Some((version.revision, version.system_time)), false).await?.into_unary();
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
        present: Timestamp, version: Option<(u64, Timestamp)>, ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
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
            .filter_map(|(index, field)| (!matches!(field.name().as_str(),
                "_assertion_ordinal" | rtdb_types::OWNED_EVENT_POSITION_COLUMN)).then_some(index))
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
        self.encode_owned_stream_reply(self.memory(), &schema, &response, ipc_stream)
    }

    pub(crate) async fn held_owned_range_action(&self, call: &Call, body: &Bytes, version: HeldRangeFormat, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned held range has no handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Ok(entry) = self.take_owned_read(call, &handle) { entry.close(); }
            return Err(status(error));
        }
        let (relation, arrow) = split_relation(request)?;
        let batch = decode_batch(call, call.retain_request_bytes(body.slice_ref(arrow)))?;
        let covers_clock = batch.batch().column_by_name("cover_valid").is_some();
        if covers_clock && !matches!(version, HeldRangeFormat::Full) {
            return Err(invalid("Valid-time range coverage requires the original live hold"));
        }
        let expected_fields = match version {
            HeldRangeFormat::Leading => 4,
            HeldRangeFormat::Full => 6,
            HeldRangeFormat::FixedValid => 7,
            HeldRangeFormat::Historical => 8,
        };
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != expected_fields + usize::from(covers_clock) {
            return Err(invalid("Owned range requires one row with the fields for its action version"));
        }
        let (row, _decoded) = OrderedControls::read(batch.batch(), self)?;
        if covers_clock && row.boolean("cover_valid") != Some(true) {
            return Err(invalid("The valid-time coverage selector must be true"));
        }
        let (Some(path), true, true, Some(limit)) =
            (row.text("path"), row.contains("lower"), row.contains("upper"), row.integer("limit"))
        else { return Err(invalid("Owned range fields have invalid types")); };
        let path = OwnedRelation::new(path).map_err(status)?;
        let limit = usize::try_from(limit).map_err(|_| invalid("Owned range limit is out of range"))?;
        let valid_at = if matches!(version, HeldRangeFormat::FixedValid | HeldRangeFormat::Historical) {
            let Some(valid_at) = row.integer("valid_micros") else {
                return Err(invalid("Owned fixed range requires a microsecond valid instant"));
            };
            let valid_at = Timestamp(valid_at);
            valid_at.next().map_err(status)?;
            Some(valid_at)
        } else { None };
        let system = if matches!(version, HeldRangeFormat::Historical) {
            let Some(system) = row.integer("system_micros") else {
                return Err(invalid("Owned historical range requires a microsecond system cut"));
            };
            Some(Timestamp(system))
        } else { None };
        let (page, evidence, expiry) = if !matches!(version, HeldRangeFormat::Leading) {
            let Some(descending) = row.boolean("descending") else {
                return Err(invalid("Owned range requires a boolean descending field"));
            };
            let request = OwnedRangeRequest {
                lower: row.bound("lower")?,
                upper: row.bound("upper")?,
                after: row.bound("after")?,
                descending,
                limit,
            };
            if covers_clock {
                held.snapshot.range_page_by_key_covered(&relation, &path, request).await
            } else if let (Some(system), Some(valid)) = (system, valid_at) {
                held.snapshot.range_page_by_key_at_system(&relation, &path, request, system, valid)
                    .await.map(|page| (page, None, None))
            } else {
                held.snapshot.range_page_by_key_lifetime(&relation, &path, request, valid_at).await
            }
        } else {
            let lower = row.leading("lower")?;
            let upper = row.leading("upper")?;
            held.snapshot.range(&relation, &path, lower.as_deref(), upper.as_deref(), limit).await.map(|page| (page, None, None))
        }.map_err(status)?;
        let present = system.unwrap_or(held.snapshot.present().system_time);
        let observed = witnessed_schema(call, &held, &relation, &page.schema, evidence)?;
        let schema = observed.as_ref().map_or(page.schema.as_ref(), |(schema, _)| schema);
        let reply = self.encode_owned_point_reply(self.memory(), schema, page.batch.as_ref().map(OwnedBatch::batch),
            present, valid_at.unwrap_or(present), None, expiry, reply_format)?;
        if covers_clock {
            let until = expiry.ok_or_else(|| Status::internal("Covered range omitted its valid-time cutoff"))?;
            held.snapshot.check_covered_range_release(&relation, until).map_err(status)?;
        } else if system.is_none() && !matches!(version, HeldRangeFormat::Leading) {
            held.snapshot.check_lifetime_range_release(&relation, expiry).map_err(status)?;
        } else {
            held.snapshot.check_relation(&relation).map_err(status)?;
        }
        Ok(reply)
    }

    pub(crate) async fn held_owned_count_action(&self, call: &Call, body: &Bytes, historical: bool, measured: bool, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned held count has no handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Ok(entry) = self.take_owned_read(call, &handle) { entry.close(); }
            return Err(status(error));
        }
        let (relation, arrow_bytes) = split_relation(request)?;
        let batch = decode_batch(call, call.retain_request_bytes(body.slice_ref(arrow_bytes)))?;
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != 4 + usize::from(historical) {
            return Err(invalid("Owned count requires one row with the fields for its action version"));
        }
        let (row, _decoded) = OrderedControls::read(batch.batch(), self)?;
        let (Some(path), true, true, Some(valid)) =
            (row.text("path"), row.contains("lower"), row.contains("upper"), row.integer("valid_micros"))
        else { return Err(invalid("Owned count fields have invalid types")); };
        let path = OwnedRelation::new(path).map_err(status)?;
        let request = OwnedCountRequest {
            lower: row.bound("lower")?,
            upper: row.bound("upper")?,
            valid_at: Timestamp(valid),
        };
        let (counted, evidence) = if historical {
            let Some(system) = row.integer("system_micros") else {
                return Err(invalid("Owned historical count requires a microsecond system cut"));
            };
            let counted = if measured {
                held.snapshot.measure_range_at_system(&relation, &path, request, Timestamp(system)).await
            } else {
                held.snapshot.count_range_at_system(&relation, &path, request, Timestamp(system)).await
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
        let reply = self.encode_owned_point_reply(self.memory(), output_schema, Some(&batch),
            counted.present, counted.valid_at, None, counted.expiry, reply_format)?;
        if historical {
            held.snapshot.check_relation(&relation).map_err(status)?;
        } else {
            held.snapshot.check_lifetime_range_release(&relation, counted.expiry).map_err(status)?;
        }
        Ok(reply)
    }

    pub(crate) async fn held_world_ordered_action(&self, call: &Call, body: &Bytes, order: WorldOrder, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
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
        let batch = decode_batch(call, call.retain_request_bytes(body.slice_ref(arrow)))?;
        let count = matches!(order, WorldOrder::ParentCount | WorldOrder::ParentCountAt | WorldOrder::ParentMeasure);
        let fields = if count { 4 } else { 7 };
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != fields {
            return Err(invalid("Invalid held world range arguments"));
        }
        let (row, _decoded) = OrderedControls::read(batch.batch(), self)?;
        let (Some(path), true, true, Some(valid_at)) =
            (row.text("path"), row.contains("lower"), row.contains("upper"), row.integer("valid_micros"))
        else { return Err(invalid("Invalid held world range fields")); };
        let path = OwnedRelation::new(path).map_err(status)?;
        let valid_at = Timestamp(valid_at);
        let lower = row.bound("lower")?;
        let upper = row.bound("upper")?;
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
            let (true, Some(descending), Some(limit)) =
                (row.contains("after"), row.boolean("descending"), row.integer("limit"))
            else { return Err(invalid("Invalid held world range page")); };
            let request = OwnedRangeRequest {
                lower, upper, after: row.bound("after")?, descending,
                limit: usize::try_from(limit).map_err(|_| invalid("Invalid held world range limit"))?,
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
        let reply = self.encode_owned_point_reply(self.memory(), &schema, rows,
            system.unwrap_or_else(|| held.snapshot.present()), valid_at, None, None, reply_format)?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn spatial_path_readiness_action(&self, call: &Call, body: &Bytes, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
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
        self.encode_owned_point_reply(self.memory(), batch.schema_ref(), Some(&batch), present, present, None, None, reply_format)
    }

    pub(crate) async fn spatial_held_owned_action(&self, call: &Call, body: &Bytes, wire: SpatialHeldWire, ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned held spatial read has no handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        let (relation, request) = split_relation(request)?;
        let (path, request) = split_relation(request)?;
        let batch = decode_retained_batch(call, body.slice_ref(request))?;
        let ranked_window = matches!(wire, SpatialHeldWire::RankWindow);
        let ranked = ranked_window || matches!(wire, SpatialHeldWire::RankOpen);
        let probe = matches!(wire, SpatialHeldWire::Probe);
        let selected_documents = ranked || matches!(wire, SpatialHeldWire::V3 | SpatialHeldWire::Open | SpatialHeldWire::Probe);
        let expected_columns = 9 + usize::from(selected_documents) + usize::from(ranked_window) + 2 * usize::from(probe);
        if batch.batch().num_rows() != 1 || batch.batch().num_columns() != expected_columns {
            return Err(invalid("Owned held spatial read requires one request row"));
        }
        let input = batch.batch();
        let document_columns = selected_documents.then(|| spatial_document_columns(input)).transpose()?;
        let projection = match wire {
            SpatialHeldWire::V1 => rtdb_spatial::Projection::Identities,
            SpatialHeldWire::V2 => rtdb_spatial::Projection::Documents,
            SpatialHeldWire::V3 | SpatialHeldWire::Open | SpatialHeldWire::Probe | SpatialHeldWire::RankOpen | SpatialHeldWire::RankWindow => {
                rtdb_spatial::Projection::SelectedDocuments(document_columns.as_deref().expect("selected projection"))
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
        let after_radius = if ranked_window {
            let (values, row) = owned_scalar::<Float64Array>(input, "after_radius_m")?
                .ok_or_else(|| invalid("A spatial rank window requires a Float64 lower radius"))?;
            Some(values.value(row))
        } else {
            None
        };
        let predicate = rtdb_spatial::Predicate::parse(predicate.value(predicate_row), radius, reported_center.value(center_row)).map_err(status)?;
        if probe && (matches!(predicate, rtdb_spatial::Predicate::Nearest { .. })
            || Timestamp(valid_at.value(valid_row)) != held.snapshot.system_time()
            || held.snapshot.historical_publication().is_some()
            || !relation.is_root_data())
        {
            return Err(invalid("A spatial probe requires current validity and identity ordering"));
        }
        let after = spatial_after(input)?;
        use rtdb_spatial::{PageOrder, Predicate};
        let page = if matches!(predicate, Predicate::Nearest { .. }) {
            if after.is_some() { return Err(invalid("Nearest continuation requires a held ranking")); }
            PageOrder::NearestOnce
        } else {
            PageOrder::Identity { after }
        };
        if probe && after.is_some() {
            return Err(invalid("A spatial probe continues through its held filter capability"));
        }
        let valid_at = Timestamp(valid_at.value(valid_row));
        let limit = usize::try_from(limit.value(limit_row)).map_err(|_| invalid("Invalid spatial limit"))?;
        let observe = observe.value(observe_row);
        let request = rtdb_spatial::Request {
            partition: partition.iter().map(|part| std::borrow::Cow::Borrowed(part.as_ref())).collect(),
            region: region.into(), predicate, valid_at, page, limit,
        };
        let mut probe_controls = None;
        let (page, witness, prepared) = if ranked {
            let (ranking, page, witness) = held.snapshot.open_spatial_nearest(&relation, &path, request, after_radius, observe, projection)
                .await.map_err(status)?;
            (page, witness, Some(RetainedSpatialQuery::Nearest(ranking)))
        } else if probe {
            held.snapshot.check_relation(&relation).map_err(status)?;
            let region = rtdb_spatial::PreparedRegion::new(region, predicate, call.memory()).map_err(status)?;
            let columns = document_columns.as_deref().expect("selected projection");
            let fields = spatial_column_names(input, "omittable_columns")?;
            let point_column = spatial_probe_point(input, &fields, columns)?;
            let request_probe = rtdb_spatial::Probe { fields: &fields, point_column, certify_point: true };
            let (page, witness, matches) = held.snapshot.open_spatial_probe(
                &relation, &path, request, observe, columns, &region, request_probe,
            ).await.map_err(status)?;
            probe_controls = Some((fields, point_column));
            let certify_point = page.certifies_point_intersects();
            let matches = Mutex::new(matches.map(Arc::new));
            (page, witness, Some(RetainedSpatialQuery::Probe { path, region, certify_point, matches }))
        } else if matches!(wire, SpatialHeldWire::Open) {
            held.snapshot.check_relation(&relation).map_err(status)?;
            let region = rtdb_spatial::PreparedRegion::new(region, predicate, call.memory()).map_err(status)?;
            if matches!(page, PageOrder::Identity { .. }) {
                let (matches, page, witness) = held.snapshot.open_spatial_range(&relation, &path, request, observe, projection, &region)
                    .await.map_err(status)?;
                let earlier = after.map(|_| (path, region));
                (page, witness, Some(RetainedSpatialQuery::Range { matches, earlier }))
            } else {
                let (page, witness) = held.snapshot.spatial_page(&relation, &path, request, observe, projection, Some(&region))
                    .await.map_err(status)?;
                (page, witness, None)
            }
        } else {
            let (page, witness) = held.snapshot.spatial_page(&relation, &path, request, observe, projection, None)
                .await.map_err(status)?;
            (page, witness, None)
        };
        let mut retained = None;
        let mut token = None;
        if let Some(query) = prepared.as_ref().filter(|_| page.more) {
            let guard = held.spatial_filters.lock().map_err(|_| spatial_filters_unavailable())?;
            if guard.len() >= MAX_SPATIAL_FILTERS { return Err(Status::resource_exhausted("The held read has too many spatial filters")); }
            let nonce = Uuid::new_v4();
            if guard.contains_key(&nonce) { return Err(spatial_filters_unavailable()); }
            let columns = document_columns.as_deref().expect("a held filter has a selected projection");
            let measure_names = |names: &[&str]| names.iter().map(|name| name.len() + size_of::<String>()).sum::<usize>();
            let fallback = matches!(query, RetainedSpatialQuery::Range { earlier: Some(_), .. } | RetainedSpatialQuery::Probe { .. });
            let partition_bytes = if fallback {
                partition.iter().map(|part| part.len() + size_of::<Vec<u8>>()).sum::<usize>()
            } else { 0 };
            let after_bytes = after.map_or(0, EntityIdRef::owned_payload_bytes);
            let probe_bytes = probe_controls.as_ref().map_or(0, |(fields, point)| {
                measure_names(fields) + point.map_or(0, str::len)
            });
            let bytes = wire::add(1024 + measure_names(columns) + partition_bytes + probe_bytes, after_bytes)?;
            let reservation = match call.reserve(bytes) {
                Err(error) if spatial_memory_refusal(&error) && query.discard_probe_matches()? => call.reserve(bytes)?,
                reservation => reservation?,
            };
            let retain_names = |names: &[&str]| names.iter().map(|name| zeroize::Zeroizing::new((*name).to_owned())).collect();
            let retain_partition = || partition.iter().map(|part| zeroize::Zeroizing::new(part.as_ref().to_vec())).collect();
            let controls = match query {
                RetainedSpatialQuery::Range { earlier: Some(_), .. } => SpatialContinuationControls::Earlier {
                    after: SpatialAnchor::new(after.expect("an earlier range has an initial anchor")),
                    partition: retain_partition(),
                },
                RetainedSpatialQuery::Probe { .. } => {
                    let (fields, point_column) = probe_controls.as_ref().expect("probe controls were admitted");
                    SpatialContinuationControls::Probe {
                        partition: retain_partition(),
                        fields: retain_names(fields),
                        point_column: point_column.map(|column| zeroize::Zeroizing::new(column.to_owned())),
                    }
                }
                _ => SpatialContinuationControls::None,
            };
            retained = Some((guard, reservation, retain_names(columns), controls));
            token = Some(nonce);
        }
        drop(probe_controls);
        drop(document_columns);
        drop(partition);
        drop(batch);
        let reply = self.spatial_reply(call, &held, &relation, valid_at, &page, witness, selected_documents, token, prepared.as_ref(), ipc_stream)?;
        if let (Some((mut filters, reservation, columns, controls)), Some(token), Some(query)) = (retained, token, prepared) {
            filters.insert(token, Arc::new(RetainedSpatialFilter {
                relation, columns, controls, query, valid_at, _reservation: reservation,
            }));
        }
        Ok(reply)
    }

    pub(crate) async fn next_spatial_owned_action(&self, call: &Call, body: &[u8], ranked: bool, ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
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
        let columns = filter.columns.iter().map(|name| name.as_str()).collect::<Vec<_>>();
        let projection = rtdb_spatial::Projection::SelectedDocuments(&columns);
        let (page, witness) = match (&filter.query, distance) {
            (RetainedSpatialQuery::Range { matches, earlier }, None) => {
                if let Some((path, region)) = earlier
                    && let SpatialContinuationControls::Earlier { after: initial, partition } = &filter.controls
                    && after < initial.as_ref()
                {
                    let request = rtdb_spatial::Request {
                        partition: borrow_spatial_partition(partition),
                        region: Default::default(), predicate: rtdb_spatial::Predicate::All,
                        valid_at: filter.valid_at, page: rtdb_spatial::PageOrder::Identity { after: Some(after) }, limit,
                    };
                    held.snapshot.spatial_page(&filter.relation, path, request, observe, projection, Some(region)).await
                } else {
                    held.snapshot.next_spatial_range(matches, after, limit, observe, projection).await
                }
            }
            (RetainedSpatialQuery::Probe { path, region, certify_point, matches }, None) => {
                let SpatialContinuationControls::Probe { partition, fields, point_column } = &filter.controls else {
                    unreachable!("held probes retain their admitted controls")
                };
                let fields = fields.iter().map(|name| name.as_str()).collect::<Vec<_>>();
                let probe = rtdb_spatial::Probe {
                    fields: &fields,
                    point_column: point_column.as_ref().map(|name| name.as_str()),
                    certify_point: *certify_point,
                };
                let retained = matches.lock().map_err(|_| spatial_filters_unavailable())?.clone();
                let cached = if let Some(retained) = retained {
                    let result = held.snapshot.next_spatial_probe(&retained, after, limit, observe, &columns, probe).await;
                    drop(retained);
                    match result {
                        Err(error) if error.is(AnomalyKind::ResourceExhausted) => {
                            filter.query.discard_probe_matches()?;
                            None
                        }
                        result => Some(result),
                    }
                } else {
                    None
                };
                if let Some(result) = cached {
                    result
                } else {
                    let request = rtdb_spatial::Request {
                        partition: borrow_spatial_partition(partition),
                        region: Default::default(), predicate: rtdb_spatial::Predicate::All,
                        valid_at: filter.valid_at, page: rtdb_spatial::PageOrder::Identity { after: Some(after) }, limit,
                    };
                    held.snapshot.spatial_probe_page(&filter.relation, path, request, observe, &columns, region, probe).await
                }
            }
            (RetainedSpatialQuery::Nearest(ranking), Some(distance_m)) => {
                held.snapshot.next_spatial_nearest(ranking, rtdb_spatial::NearestPosition { distance_m, id: after },
                    limit, observe, projection).await
            }
            _ => return Err(invalid("Spatial continuation uses another query order")),
        }.map_err(status)?;
        self.spatial_reply(call, &held, &filter.relation, filter.valid_at, &page, witness, true, Some(token), Some(&filter.query), ipc_stream)
    }

    pub(crate) async fn gather_spatial_owned_action(&self, call: &Call, body: &Bytes, ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Spatial gather has no hold"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, request) = split_relation(request)?;
        if held.snapshot.historical_publication().is_some() || !relation.is_root_data() {
            return Err(invalid("A spatial gather requires a current Root relation"));
        }
        let batch = decode_retained_batch(call, body.slice_ref(request))?;
        let input = batch.batch();
        if input.num_rows() != 1 || input.num_columns() != 3 {
            return Err(invalid("Spatial gather requires one identity, revision, and projection"));
        }
        let columns = spatial_document_columns(input)?;
        let id = if input.column_by_name("_id").is_some_and(|ids| ids.data_type() == &DataType::Binary) {
            let (id, row) = owned_scalar::<BinaryArray>(input, "_id")?
                .ok_or_else(|| invalid("Spatial gather has no identity"))?;
            EntityIdRef::Binary(id.value(row))
        } else {
            let (id, row) = owned_scalar::<StringArray>(input, "_id")?
                .ok_or_else(|| invalid("Spatial gather has no identity"))?;
            EntityIdRef::String(id.value(row))
        };
        let (revision, row) = owned_scalar::<Int64Array>(input, "revision_micros")?
            .ok_or_else(|| invalid("Spatial gather has no revision"))?;
        let page = held.snapshot.gather_spatial_document(&relation, id, Timestamp(revision.value(row)), &columns)
            .await.map_err(status)?;
        self.spatial_reply(call, &held, &relation, held.snapshot.system_time(), &page, None, true, None, None, ipc_stream)
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
        page: &rtdb_spatial::Page, witness: Option<OwnedRangeWitness>, selected_documents: bool, token: Option<Uuid>,
        query: Option<&RetainedSpatialQuery>, ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
        let witness = match witness {
            Some(witness) => {
                let nonce = match held.remember(call, relation, witness.clone()) {
                    Err(error) if spatial_memory_refusal(&error)
                        && query.map(RetainedSpatialQuery::discard_probe_matches).transpose()? == Some(true) =>
                    {
                        held.remember(call, relation, witness)?
                    }
                    result => result?,
                };
                Some(nonce)
            }
            None => None,
        };
        let encode = || self.encode_spatial_reply(call, held, relation, valid_at, page, witness, selected_documents, token, ipc_stream);
        match encode() {
            Err(error) if spatial_memory_refusal(&error)
                && query.map(RetainedSpatialQuery::discard_probe_matches).transpose()? == Some(true) => encode(),
            result => result,
        }
    }

    fn encode_spatial_reply(
        &self, call: &Call, held: &OwnedReadResource, relation: &OwnedRelation, valid_at: Timestamp,
        page: &rtdb_spatial::Page, witness: Option<Uuid>, selected_documents: bool, token: Option<Uuid>, ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
        let present = held.snapshot.system_time();
        let mut metadata = std::collections::HashMap::new();
        metadata.insert("rtdb.owned.spatial_more".into(), if page.more { "1" } else { "0" }.into());
        if let Some(token) = token.filter(|_| page.more) {
            metadata.insert(SPATIAL_FILTER_TOKEN.into(), token.simple().to_string());
        }
        if let Some(distance) = page.next_distance_m {
            metadata.insert("rtdb.owned.spatial_next_distance_m".into(), distance.to_string());
        }
        if let Some(nonce) = witness {
            metadata.insert(OWNED_RANGE_WITNESS.into(), nonce.simple().to_string());
        }
        if page.certifies_point_intersects() {
            metadata.insert("rtdb.owned.spatial_exact".into(), "point-intersects-v1".into());
        }
        let omitted = page.omitted_sizes();
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
        if let Some(omitted) = omitted {
            fields.push(Field::new("omitted_sizes", DataType::Struct(omitted.fields().clone()), false));
        }
        let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
        let mut charge = None;
        let output = if let Some(rows) = page.rows().filter(|rows| rows.batch().num_rows() != 0) {
            let (ids, scratch) = spatial_reply_ids(call.memory(), rows, schema.field(0))?;
            charge = Some(scratch);
            let distances = Arc::new(page.distance_column());
            let mut columns = vec![ids, distances];
            if let Some(document) = document {
                columns.push(Arc::new(document));
            }
            if let Some(omitted) = omitted {
                columns.push(Arc::new(omitted.clone()));
            }
            Some(RecordBatch::try_new(Arc::clone(&schema), columns).map_err(arrow)?)
        } else { None };
        let format = if ipc_stream { OwnedReplyFormat::StreamV1 } else { OwnedReplyFormat::Unary };
        let reply = self.encode_owned_point_reply(self.memory(), &schema, output.as_ref(), present, valid_at, None, None, format)?;
        drop(output);
        drop(charge);
        held.snapshot.check_relations([relation]).map_err(status)?;
        Ok(reply)
    }

    async fn begin_held_rank(&self, call: &Call) -> Result<(), Status> {
        use rtdb_storage::sealing::PlaintextView;

        PlaintextView::begin_rank_inventory().map_err(status)?;
        if !self.supports_hosted_upload(call) { return Ok(()); }
        let Some(host) = self.0.config.owned_world_host.as_ref() else { return Ok(()); };
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let Some(attachment) = attachment else { return Ok(()); };
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        require_idle(&session)?;
        if let Ok(authority) = host.root_rank_authority(crate::WorldCall {
            node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation,
        }) {
            PlaintextView::bind_root_rank_authority(authority).map_err(status)?;
        }
        Ok(())
    }

    pub(crate) async fn search_held_owned_action(&self, call: &Call, body: &Bytes, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        self.begin_held_rank(call).await?;
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
            (Some(request_batch(call, call.retain_request_bytes(body.slice_ref(ipc)))?), Some(scope))
        };
        let prefix_len = body.len() - request.len();
        let rank = owned_rank_request(call, request)?;
        let overlay = overlay.as_ref().map(|input| self.search_shadows(&rank, input.batch())).transpose()?;
        let overlay = match overlay {
            Some(shadows) => {
                let analyzer = held.snapshot.search_analyzer(&rank.partitions).await.map_err(status)?;
                Some(shadows.into_corpus(Arc::clone(self.memory()), analyzer, rank.query, || held.snapshot.check())
                    .map_err(status)?)
            }
            None => None,
        };
        let (page, witnesses) = held.snapshot.ranked_search_batch(
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
        let reply = self.owned_rank_reply(call, page, scope, nonces, present, valid.unwrap_or(present), reply_format)?;
        held.snapshot.check_relations(rank.partitions.iter().map(|(relation, _)| relation)).map_err(status)?;
        Ok(reply)
    }

    fn search_shadows<'a>(
        &self, rank: &OwnedRankRequest<'_>, input: &'a RecordBatch,
    ) -> Result<rtdb_node::TextShadows<'a>, Status> {
        if !rank.observe || !(1..=64).contains(&input.num_rows()) || input.num_columns() != 3 {
            return Err(invalid("Staged search requires bounded rows and original evidence"));
        }
        let partitions = input.column_by_name("partition").and_then(|c| c.as_any().downcast_ref::<UInt64Array>());
        let ids = input.column_by_name("_id").map(|ids| ids.as_ref());
        let text = input.column_by_name("text").and_then(|c| c.as_any().downcast_ref::<StringArray>());
        let (Some(partitions), Some(ids), Some(text)) = (partitions, ids, text) else {
            return Err(invalid("Invalid staged search overlay layout"));
        };
        rtdb_node::TextShadows::try_new(partitions, ids, text, |partition, id| {
            rank.partitions.get(partition).is_some_and(|(relation, _)| staged_rank_identity(relation, id))
        }).ok_or_else(|| invalid("Invalid staged search identity"))
    }

    pub(crate) async fn nearest_held_owned_action(&self, call: &Call, body: &Bytes, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        self.begin_held_rank(call).await?;
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned held nearest has no handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Ok(entry) = self.take_owned_read(call, &handle) { entry.close(); }
            return Err(status(error));
        }
        let (valid, request) = request.split_first_chunk::<8>()
            .ok_or_else(|| invalid("Held nearest has no valid instant"))?;
        let valid = i64::from_be_bytes(*valid);
        let valid = (valid != i64::MIN).then_some(Timestamp(valid));
        if let Some(valid) = valid { rtdb_types::Interval::at(valid).map_err(status)?; }
        let (length, request) = request.split_first_chunk::<4>()
            .ok_or_else(|| invalid("Held nearest has no overlay length"))?;
        let (ipc, request) = request.split_at_checked(u32::from_be_bytes(*length) as usize)
            .ok_or_else(|| invalid("Held nearest has a truncated overlay"))?;
        let (overlay, staged_scope) = if ipc.is_empty() {
            (None, None)
        } else {
            if valid.is_some() { return Err(invalid("A staged nearest read requires current validity")); }
            self.hosted_control_policy(call)?;
            let scope = self.hosted_control_scope(call).await?;
            (Some(request_batch(call, call.retain_request_bytes(body.slice_ref(ipc)))?), Some(scope))
        };
        let nearest = owned_nearest_request(call, request)?;
        if overlay.is_some() && !nearest.observe {
            return Err(invalid("Staged nearest requires original evidence"));
        }
        let overlay = overlay
            .map(|input| self.nearest_shadows(&nearest, input))
            .transpose()?;
        let ((page, approximate), witnesses) = held.snapshot.nearest_batch(
            &nearest.partitions, &nearest.query, nearest.observe, valid, overlay,
        ).await.map_err(status)?;
        if let Some(scope) = staged_scope {
            if witnesses.len() != nearest.partitions.len()
                || witnesses.iter().any(|witness| !witness.matches_scope(Some(scope)))
            {
                return Err(Status::failed_precondition("Staged nearest did not retain its original placed corpus"));
            }
        }
        let mut nonces = Vec::new();
        if !witnesses.is_empty() {
            if witnesses.len() != nearest.partitions.len() { return Err(Status::internal("Incomplete owned nearest evidence")); }
            for (partition, witness) in nearest.partitions.iter().zip(witnesses) {
                nonces.push(held.remember(call, &partition.relation, witness)?);
            }
        }
        let present = held.snapshot.present().system_time;
        let scope = owned_rank_scope(b"rtdb.owned.nearest.held.v2\0", body, "");
        let reply = self.owned_rank_reply_with(call, page, scope, nonces, present, valid.unwrap_or(present), reply_format,
            [(OWNED_NEAREST_APPROXIMATE, if approximate { "1" } else { "0" })])?;
        held.snapshot.check_relations(nearest.partitions.iter().map(|partition| &partition.relation)).map_err(status)?;
        Ok(reply)
    }

    /// Fuses a held text ranking and a held nearest ranking of the same relations at the held cut by reciprocal rank fusion.
    pub(crate) async fn hybrid_held_owned_action(&self, call: &Call, body: &Bytes, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        self.begin_held_rank(call).await?;
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Owned held hybrid search has no handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Ok(entry) = self.take_owned_read(call, &handle) { entry.close(); }
            return Err(status(error));
        }
        let (valid, request) = request.split_first_chunk::<8>()
            .ok_or_else(|| invalid("Held hybrid search has no valid instant"))?;
        let valid = i64::from_be_bytes(*valid);
        let valid = (valid != i64::MIN).then_some(Timestamp(valid));
        if let Some(valid) = valid { rtdb_types::Interval::at(valid).map_err(status)?; }
        let hybrid = owned_hybrid_request(call, body, request, true)?;
        let staged_scope = if hybrid.staged() {
            if valid.is_some() { return Err(invalid("A staged hybrid search requires current validity")); }
            self.hosted_control_policy(call)?;
            Some(self.hosted_control_scope(call).await?)
        } else { None };
        let (rank, nearest) = (&hybrid.rank, &hybrid.nearest);
        let text_overlay = match hybrid.text_overlay.as_ref().map(|input| self.search_shadows(rank, input.batch())).transpose()? {
            Some(shadows) => {
                let analyzer = held.snapshot.search_analyzer(&rank.partitions).await.map_err(status)?;
                Some(shadows.into_corpus(Arc::clone(self.memory()), analyzer, rank.query, || held.snapshot.check())
                    .map_err(status)?)
            }
            None => None,
        };
        let vector_overlay = hybrid.vector_overlay.map(|input| self.nearest_shadows(nearest, input)).transpose()?;
        let (text_page, witnesses) = held.snapshot.ranked_search_batch(
            &rank.partitions, rank.query, 0, rank.limit, true, valid, text_overlay,
        ).await.map_err(status)?;
        let ((nearest_page, approximate), _) = held.snapshot.nearest_batch(&nearest.partitions, &nearest.query, false, valid, vector_overlay)
            .await.map_err(status)?;
        let page = rtdb_node::hybrid::fuse_ranked(
            &[(HYBRID_TEXT_RANK, &text_page), (HYBRID_NEAREST_RANK, &nearest_page)], hybrid.limit, self.memory(),
        ).map_err(status)?;
        drop((text_page, nearest_page));
        if witnesses.len() != rank.partitions.len() { return Err(Status::internal("Incomplete owned hybrid evidence")); }
        if let Some(scope) = staged_scope
            && witnesses.iter().any(|witness| !witness.matches_scope(Some(scope)))
        {
            return Err(Status::failed_precondition("Staged hybrid search did not retain its original placed corpus"));
        }
        let mut nonces = Vec::with_capacity(witnesses.len());
        for ((relation, _), witness) in rank.partitions.iter().zip(witnesses) {
            nonces.push(held.remember(call, relation, witness)?);
        }
        let present = held.snapshot.present().system_time;
        let scope = owned_rank_scope(b"rtdb.owned.hybrid.held.v2\0", body, "");
        let reply = self.owned_rank_reply_with(call, page, scope, nonces, present, valid.unwrap_or(present), reply_format,
            [(OWNED_NEAREST_APPROXIMATE, if approximate { "1" } else { "0" })])?;
        held.snapshot.check_relations(rank.partitions.iter().map(|(relation, _)| relation)).map_err(status)?;
        Ok(reply)
    }

    /// The world hybrid family: HybridHeldWorld (with staged overlays), HybridHeldWorldAt and HybridHeldWorldParentAt.
    pub(crate) async fn hybrid_held_world_action(&self, call: &Call, body: &Bytes, mode: WorldHistoricalRead, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        self.begin_held_rank(call).await?;
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Missing held world hybrid handle"))?;
        let (earlier, request) = if matches!(mode, WorldHistoricalRead::EarlierEffective) {
            let (mode, rest) = request.split_first().ok_or_else(|| invalid("Missing historical world hybrid selector"))?;
            let parent_only = match *mode {
                0 => true,
                1 => false,
                _ => return Err(invalid("Invalid historical world hybrid selector")),
            };
            let (system, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("Missing historical world hybrid system revision"))?;
            (Some((Timestamp(i64::from_be_bytes(*system)), parent_only)), rest)
        } else { (None, request) };
        let (parent_at, request) = if matches!(mode, WorldHistoricalRead::EarlierParent) {
            let (system, rest) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Missing earlier parent hybrid revision"))?;
            (Some(Timestamp(i64::from_be_bytes(*system))), rest)
        } else { (None, request) };
        let (valid, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Missing held world hybrid valid instant"))?;
        let valid = Timestamp(i64::from_be_bytes(*valid));
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_world_resource(call, &handle)?;
        let hybrid = owned_hybrid_request(call, body, request, matches!(mode, WorldHistoricalRead::Current))?;
        let staged_scope = if hybrid.staged() {
            if held.snapshot.clock_mode() != rtdb_types::WorldClockMode::Proposal || valid != held.snapshot.present() {
                return Err(invalid("Staged world hybrid search requires live Proposal validity"));
            }
            self.hosted_control_policy(call)?;
            Some(self.hosted_control_scope(call).await?)
        } else { None };
        let (rank, nearest) = (&hybrid.rank, &hybrid.nearest);
        let relations = rank.partitions.iter().map(|(relation, _)| relation.clone()).collect::<Vec<_>>();
        let domain = match mode {
            WorldHistoricalRead::Current => b"rtdb.owned.hybrid.world.v1\0".as_slice(),
            WorldHistoricalRead::EarlierEffective => b"rtdb.owned.hybrid.world-at.v1\0".as_slice(),
            WorldHistoricalRead::EarlierParent => b"rtdb.owned.hybrid.world-parent-at.v1\0".as_slice(),
        };
        let scope = owned_rank_scope(domain, body, "");
        let text_overlay = hybrid.text_overlay.as_ref().map(|input| self.search_shadows(rank, input.batch())).transpose()?;
        let vector_overlay = hybrid.vector_overlay.map(|input| self.nearest_shadows(nearest, input)).transpose()?;
        let (partitions, query) = (rank.partitions.clone(), rank.query.to_owned());
        let (vectors, probe) = (nearest.partitions.clone(), nearest.query.clone());
        let (text_read, vector_read) = if let Some(system) = parent_at {
            (held.snapshot.ranked_search_parent_at(partitions, query, 0, rank.limit, valid, system).await,
                held.snapshot.nearest_parent_at(vectors, probe, valid, system).await)
        } else if let Some((system, parent_only)) = earlier {
            (held.snapshot.ranked_search_at(partitions, query, 0, rank.limit, valid, system, parent_only).await,
                held.snapshot.nearest_at(vectors, probe, valid, system, parent_only).await)
        } else {
            (held.snapshot.ranked_search(partitions, query, 0, rank.limit, valid, text_overlay).await,
                held.snapshot.nearest(vectors, probe, valid, vector_overlay).await)
        };
        let (text_page, witnesses) = text_read.map_err(status)?;
        let (nearest_page, _) = vector_read.map_err(status)?;
        if witnesses.len() != relations.len() { return Err(Status::internal("Incomplete retained hybrid evidence")); }
        if let Some(scope) = staged_scope
            && witnesses.iter().any(|witness| !witness.matches_scope(scope))
        {
            return Err(Status::failed_precondition("Staged hybrid search did not retain its original placed corpus"));
        }
        let page = rtdb_node::hybrid::fuse_ranked(
            &[(HYBRID_TEXT_RANK, &text_page), (HYBRID_NEAREST_RANK, &nearest_page)], hybrid.limit, self.memory(),
        ).map_err(status)?;
        drop((text_page, nearest_page));
        let nonces = witnesses.into_iter().map(|witness| held.remember(call, witness, false)).collect::<Result<Vec<_>, _>>()?;
        let present = parent_at.or_else(|| earlier.map(|(system, _)| system)).unwrap_or_else(|| held.snapshot.present());
        let reply = self.owned_rank_reply(call, page, scope, nonces, present, valid, reply_format)?;
        for relation in &relations { held.snapshot.check_relation(relation).map_err(status)?; }
        Ok(reply)
    }

    fn nearest_shadows(&self, nearest: &OwnedNearestRequest, staged: OwnedBatch) -> Result<rtdb_node::nearest::HeldVectorOverlay, Status> {
        let input = staged.batch();
        let filters = nearest.query.filter_count();
        let malformed = || invalid("Invalid staged nearest overlay layout");
        if !(1..=rtdb_node::nearest::MAX_NEAREST_SHADOWS).contains(&input.num_rows()) || input.num_columns() != 3 + filters {
            return Err(invalid("Staged nearest requires bounded rows and its filter columns"));
        }
        let partitions = input.column_by_name("partition").and_then(|c| c.as_any().downcast_ref::<UInt64Array>()).ok_or_else(malformed)?;
        let (id_column, id_field) = input.schema_ref().column_with_name("_id")
            .filter(|(_, field)| matches!(field.data_type(), DataType::Utf8 | DataType::Binary))
            .ok_or_else(malformed)?;
        let ids = input.column(id_column).as_ref();
        let vectors = input.column_by_name("vector").and_then(|c| c.as_any().downcast_ref::<ListArray>()).ok_or_else(malformed)?;
        let components = vectors.values().as_any().downcast_ref::<arrow_array::Float32Array>().ok_or_else(malformed)?;
        let filter_columns = (0..filters).map(|ordinal| {
            input.column_by_name(&format!("filter_{ordinal}"))
                .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
                .ok_or_else(malformed)
        }).collect::<Result<Vec<_>, _>>()?;
        let mut overlay = rtdb_node::nearest::HeldVectorOverlay::new(self.memory()).map_err(status)?;
        for row in 0..input.num_rows() {
            let invalid_id = || invalid("Invalid staged nearest identity");
            if partitions.is_null(row) || partitions.value(row) >= nearest.partitions.len() as u64 {
                return Err(invalid_id());
            }
            let id = read_owned_entity_id(id_field, ids, row).map_err(|_| invalid_id())?;
            if !staged_rank_identity(&nearest.partitions[partitions.value(row) as usize].relation, id) {
                return Err(invalid_id());
            }
            let vector = if vectors.is_null(row) {
                None
            } else {
                let range = vectors.value_offsets()[row] as usize..vectors.value_offsets()[row + 1] as usize;
                if range.len() != nearest.query.dimensions() || range.clone().any(|index| components.is_null(index)) {
                    return Err(invalid("A staged nearest vector does not have the probe's dimensions"));
                }
                Some(&components.values()[range])
            };
            let cells = filter_columns.iter()
                .map(|column| column.is_valid(row).then(|| column.value(row)));
            overlay.shadow_staged(
                partitions.value(row) as usize,
                id,
                vector,
                cells,
                &nearest.query,
            ).map_err(status)?;
        }
        Ok(overlay)
    }

    /// `NearestHeldWorld.v3`, `NearestHeldWorldAt.v3` and `NearestHeldWorldParentAt.v3`:
    /// the SearchHeldWorld prefixes followed by the NearestHeld.v4 frame.
    pub(crate) async fn nearest_held_world_action(&self, call: &Call, body: &Bytes, mode: WorldHistoricalRead, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        self.begin_held_rank(call).await?;
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Missing held world nearest handle"))?;
        let (earlier, request) = if matches!(mode, WorldHistoricalRead::EarlierEffective) {
            let (mode, rest) = request.split_first().ok_or_else(|| invalid("Missing historical world nearest selector"))?;
            let parent_only = match *mode {
                0 => true,
                1 => false,
                _ => return Err(invalid("Invalid historical world nearest selector")),
            };
            let (system, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("Missing historical world nearest system revision"))?;
            (Some((Timestamp(i64::from_be_bytes(*system)), parent_only)), rest)
        } else { (None, request) };
        let (parent_at, request) = if matches!(mode, WorldHistoricalRead::EarlierParent) {
            let (system, rest) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Missing earlier parent nearest revision"))?;
            (Some(Timestamp(i64::from_be_bytes(*system))), rest)
        } else { (None, request) };
        let (valid, request) = request.split_first_chunk::<8>().ok_or_else(|| invalid("Missing held world nearest valid instant"))?;
        let valid = Timestamp(i64::from_be_bytes(*valid));
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_world_resource(call, &handle)?;
        let (overlay, staged_scope, request) = if matches!(mode, WorldHistoricalRead::Current) {
            let (length, request) = request.split_first_chunk::<4>()
                .ok_or_else(|| invalid("Held world nearest has no overlay length"))?;
            let (ipc, request) = request.split_at_checked(u32::from_be_bytes(*length) as usize)
                .ok_or_else(|| invalid("Held world nearest has a truncated overlay"))?;
            if ipc.is_empty() {
                (None, None, request)
            } else {
                if held.snapshot.clock_mode() != rtdb_types::WorldClockMode::Proposal
                    || valid != held.snapshot.present()
                {
                    return Err(invalid("Staged world nearest requires live Proposal validity"));
                }
                self.hosted_control_policy(call)?;
                let scope = self.hosted_control_scope(call).await?;
                (Some(request_batch(call, call.retain_request_bytes(body.slice_ref(ipc)))?), Some(scope), request)
            }
        } else {
            (None, None, request)
        };
        let nearest = owned_nearest_request(call, request)?;
        if !nearest.observe {
            return Err(Status::failed_precondition("Nearest requires evidence for every selected world partition"));
        }
        let relations = nearest.partitions.iter().map(|partition| partition.relation.clone()).collect::<Vec<_>>();
        let domain = match mode {
            WorldHistoricalRead::Current => b"rtdb.owned.nearest.world.v1\0".as_slice(),
            WorldHistoricalRead::EarlierEffective => b"rtdb.owned.nearest.world-at.v1\0".as_slice(),
            WorldHistoricalRead::EarlierParent => b"rtdb.owned.nearest.world-parent-at.v1\0".as_slice(),
        };
        let scope = owned_rank_scope(domain, body, "");
        let overlay = overlay
            .map(|input| self.nearest_shadows(&nearest, input))
            .transpose()?;
        let partitions = nearest.partitions.clone();
        let query = nearest.query.clone();
        let (page, witnesses) = if let Some(system) = parent_at {
            held.snapshot.nearest_parent_at(partitions, query, valid, system).await
        } else if let Some((system, parent_only)) = earlier {
            held.snapshot.nearest_at(partitions, query, valid, system, parent_only).await
        } else {
            held.snapshot.nearest(partitions, query, valid, overlay).await
        }.map_err(status)?;
        if witnesses.len() != relations.len() { return Err(Status::internal("Incomplete retained nearest evidence")); }
        if let Some(scope) = staged_scope {
            if witnesses.iter().any(|witness| !witness.matches_scope(scope)) {
                return Err(Status::failed_precondition("Staged nearest did not retain its original placed corpus"));
            }
        }
        let nonces = witnesses.into_iter().map(|witness| held.remember(call, witness, false)).collect::<Result<Vec<_>, _>>()?;
        let present = parent_at.or_else(|| earlier.map(|(system, _)| system)).unwrap_or_else(|| held.snapshot.present());
        let reply = self.owned_rank_reply(call, page, scope, nonces, present, valid, reply_format)?;
        for relation in &relations { held.snapshot.check_relation(relation).map_err(status)?; }
        Ok(reply)
    }

    pub(crate) async fn search_held_world_action(&self, call: &Call, body: &Bytes, mode: WorldHistoricalRead, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        self.begin_held_rank(call).await?;
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
                (Some(request_batch(call, call.retain_request_bytes(body.slice_ref(ipc)))?), Some(scope), request)
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
        let reply = self.owned_rank_reply(call, page, scope, nonces, parent_at.or_else(|| earlier.map(|(system, _)| system)).unwrap_or_else(|| held.snapshot.present()), valid, reply_format)?;
        for relation in &relations { held.snapshot.check_relation(relation).map_err(status)?; }
        Ok(reply)
    }

    fn owned_rank_reply(
        &self, call: &Call, page: rtdb_node::OwnedSearchBatch, scope: String, witnesses: Vec<Uuid>,
        present: Timestamp, valid: Timestamp, reply_format: OwnedReplyFormat<'_>,
    ) -> Result<OwnedIpcReply, Status> {
        self.owned_rank_reply_with(call, page, scope, witnesses, present, valid, reply_format, [])
    }

    #[allow(clippy::too_many_arguments)]
    fn owned_rank_reply_with<const N: usize>(
        &self, call: &Call, page: rtdb_node::OwnedSearchBatch, scope: String, witnesses: Vec<Uuid>,
        present: Timestamp, valid: Timestamp, reply_format: OwnedReplyFormat<'_>, extra: [(&str, &str); N],
    ) -> Result<OwnedIpcReply, Status> {
        let (family_inventory, family_charge) = rtdb_storage::sealing::PlaintextView::rank_inventory().map_err(status)?;
        if family_inventory.len() > 2 && !self.supports_hosted_upload(call) {
            return Err(unsupported());
        }
        if !matches!(reply_format, OwnedReplyFormat::StreamV1) {
            return Err(unsupported());
        }
        let hits = page.hits.batch();
        let mut metadata = hits.schema_ref().metadata().clone();
        for (key, value) in extra { metadata.insert(key.into(), value.into()); }
        metadata.insert(OWNED_SEARCH_TOTAL.into(), page.total.to_string());
        metadata.insert(OWNED_SEARCH_SCOPE.into(), scope);
        if !witnesses.is_empty() {
            let mut nonces = String::with_capacity(32 * witnesses.len());
            for witness in witnesses { nonces.push_str(&witness.simple().to_string()); }
            metadata.insert(OWNED_SEARCH_WITNESSES.into(), nonces);
        }
        let schema = hits.schema_ref().as_ref().clone().with_metadata(metadata);
        // The query owns the batch reservation. Flight still charges the caller's session.
        let ids = hits.column(0).as_ref();
        let charge = (0..hits.num_rows()).try_fold(4096, |bytes, row| {
            let id = read_owned_entity_id(schema.field(0), ids, row).map_err(status)?;
            let size = wire::add(std::mem::size_of::<Value>(), id.owned_payload_bytes())?;
            wire::add(bytes, wire::mul(size, 4)?)
        })?;
        let _reply_charge = call.reserve(charge)?;
        let reply = self.encode_owned_point_reply(self.memory(), &schema, (hits.num_rows() != 0).then_some(hits),
            present, valid, None, None, reply_format)?;
        let inventory = ActionResult { body: wire::retain(family_inventory, family_charge) };
        reply.prepend_frame(inventory, self.0.config.maximum_message_bytes)
    }

    pub(crate) async fn held_expiry_candidates_action(&self, call: &Call, body: &[u8], reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
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
        let reply = self.encode_owned_point_reply(self.memory(), &schema, Some(&batch), present, present, None, None, reply_format)?;
        held.snapshot.check_live_change_relations(std::slice::from_ref(&relation)).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn nominate_held_expiry_action(&self, call: &Call, body: &Bytes, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        self.hosted_control_policy(call)?;
        let scope = self.hosted_control_scope(call).await?;
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Expiry nomination needs the original held handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, id, _, _identity) = point_request(call, body, request, PointTime::Default)?;
        let (nomination, created, expiry) = held.snapshot.nominate_expired_identity(&relation, &id, scope).await.map_err(status)?;
        let batch = RecordBatch::try_from_iter([
            ("nomination", Arc::new(StringArray::from(vec![nomination])) as ArrayRef),
            ("created_micros", Arc::new(Int64Array::from(vec![created.0])) as ArrayRef),
            ("expiry_micros", Arc::new(Int64Array::from(vec![expiry.0])) as ArrayRef),
        ]).map_err(arrow)?;
        let present = held.snapshot.present().system_time;
        let reply = self.encode_owned_point_reply(self.memory(), batch.schema_ref(), Some(&batch), present, present, None, None, reply_format)?;
        held.snapshot.check_live_point_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn apply_held_expiry_action(&self, call: &Call, body: &Bytes, ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
        self.hosted_control_policy(call)?;
        let (handle, rest) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Expiry apply needs the original held handle"))?;
        let (deadline, rest) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("Expiry apply needs a deadline"))?;
        let (length, rest) = rest.split_first_chunk::<2>().ok_or_else(|| invalid("Expiry apply needs a nomination length"))?;
        let length = usize::from(u16::from_be_bytes(*length));
        if !(1..=208).contains(&length) { return Err(invalid("Expiry nomination exceeds its bound")); }
        let (nomination, request) = rest.split_at_checked(length).ok_or_else(|| invalid("Expiry apply nomination is truncated"))?;
        let nomination = std::str::from_utf8(nomination).map_err(|_| invalid("Expiry nomination is not UTF-8"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, id, _, _identity) = point_request(call, body, request, PointTime::Default)?;
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        require_idle(&session)?;
        let committed = session.sweep_owned_expired(
            &held.snapshot, relation, id, nomination, Timestamp(i64::from_be_bytes(*deadline)),
        ).await.map_err(status)?;
        if let Some(error) = committed.error() { return Err(status(error.clone())); }
        if !committed.committed() { return Err(Status::aborted("Owned expiry did not commit")); }
        self.owned_tx_reply(&session, committed.tx_key(), ipc_stream).await
    }

    pub(crate) async fn held_owned_changes_action(
        &self, call: &Call, body: &[u8], ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
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
        let reply = self.owned_changes_reply(&page, held.snapshot.present().system_time, ipc_stream)?;
        held.snapshot.check_live_change_relations(std::slice::from_ref(&relation)).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_owned_storage_usage_action(
        &self, call: &Call, body: &[u8], ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
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
        let reply = self.encode_owned_stream_reply(self.memory(), &schema, &[batch], ipc_stream)?;
        held.snapshot.check().map_err(status)?;
        held.snapshot.check_live_change_relations(&relations).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_table_publication_sequence_action(
        &self, call: &Call, body: &[u8],
    ) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Table publication sequence requires a held handle"))?;
        let held = self.retained_owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, request) = split_relation(request)?;
        let (publication, trailing) = split_world_publication(request)?;
        if !trailing.is_empty() { return Err(invalid("Unexpected table publication fields")); }
        let sequence = held.snapshot.table_publication_sequence(&relation, publication).await.map_err(status)?;
        held.snapshot.check().map_err(status)?;
        Ok(ActionResult { body: wire::retain(sequence.to_be_bytes().to_vec(), call.reserve(8)?) })
    }

    pub(crate) async fn held_lookup_publication_action(
        &self, call: &Call, body: &[u8],
    ) -> Result<ActionResult, Status> {
        use rtdb_node::OwnedPublicationLookup;

        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Publication lookup requires a held handle"))?;
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let (relation, request) = split_relation(request)?;
        let selector = match request.split_first() {
            Some((0, [])) => OwnedPublicationLookup::Latest,
            Some((1, sequence)) => OwnedPublicationLookup::Sequence(u64::from_be_bytes(
                sequence.try_into().map_err(|_| invalid("Invalid publication sequence"))?,
            )),
            Some((2, position)) => {
                let (key, trailing) = split_world_publication(position)?;
                if !trailing.is_empty() { return Err(invalid("Invalid publication position")); }
                OwnedPublicationLookup::Position(key)
            },
            Some((3, time)) => OwnedPublicationLookup::AtOrBefore(Timestamp(i64::from_be_bytes(
                time.try_into().map_err(|_| invalid("Invalid publication history cap"))?,
            ))),
            _ => return Err(invalid("Invalid publication selector")),
        };
        let selected = held.snapshot.lookup_publication(&relation, selector).await.map_err(status)?;
        let mut reply = Vec::with_capacity(24);
        if let Some((sequence, key)) = selected {
            reply.extend_from_slice(&sequence.to_be_bytes());
            reply.extend_from_slice(&key.tx_id.0.to_be_bytes());
            reply.extend_from_slice(&key.system_time.0.to_be_bytes());
        }
        Ok(ActionResult { body: wire::retain(reply, call.reserve(24)?) })
    }

    pub(crate) async fn held_owned_merged_changes_action(
        &self, call: &Call, body: &[u8], descending: bool, ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Merged changes require a held read handle"))?;
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_read(call, &handle)?;
        if let Err(error) = held.snapshot.check() {
            if let Ok(entry) = self.take_owned_read(call, &handle) { entry.close(); }
            return Err(status(error));
        }
        let (after, through, limit, relations, _charge) = self.merged_changes_request(call, request)?;
        let page = held.snapshot.merged_changes_window(&relations, after, through, limit, descending).await.map_err(status)?;
        let reply = self.owned_merged_changes_reply(&page, held.snapshot.present().system_time, descending, None, ipc_stream)?;
        held.snapshot.check_live_change_relations(&relations).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_world_changes_action(
        &self, call: &Call, body: &[u8], descending: bool, ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (handle, request) = body.split_first_chunk::<16>().ok_or_else(|| invalid("World changes require a held world handle"))?;
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let (after, through, limit, relations, _charge) = self.merged_changes_request(call, request)?;
        let page = held.snapshot.changes(&relations, after, through, limit, descending).await.map_err(status)?;
        let reply = self.owned_merged_changes_reply(&page, held.snapshot.present(), descending, None, ipc_stream)?;
        for relation in &relations { held.snapshot.check_relation(relation).map_err(status)?; }
        Ok(reply)
    }

    pub(crate) async fn held_world_region_changes_action(
        &self, call: &Call, body: &[u8], ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
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
            std::slice::from_ref(&relation), Timestamp(i64::from_be_bytes(*after)).into(),
            Timestamp(i64::from_be_bytes(*through)).into(), usize::from(u16::from_be_bytes(*limit)), false,
        ).await.map_err(status)?.into_relation_page().map_err(status)?;
        page.retain_identity_region(lower.as_ref(), upper.as_ref(), valid).map_err(status)?;
        let reply = self.owned_changes_reply(&page, held.snapshot.present(), ipc_stream)?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    fn merged_changes_request(
        &self, call: &Call, request: &[u8],
    ) -> Result<(OwnedChangeEdge, OwnedChangeEdge, usize, Vec<OwnedRelation>, Arc<MemoryReservation>), Status> {
        let (after, rest) = split_change_edge(request)?;
        let (through, rest) = split_change_edge(rest)?;
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
        Ok((after, through,
            usize::from(u16::from_be_bytes(*limit)), relations, charge))
    }

    fn owned_merged_changes_reply(
        &self, page: &rtdb_node::OwnedMergedChanges, present: Timestamp, descending: bool, witness: Option<Uuid>,
        ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
        let changes_type = changes::merged_type();
        let edge = page.covered_edge(descending).map_err(status)?;
        let (kind, position) = match edge.order {
            ChangeEdgeOrder::AsOf => (0, -1),
            ChangeEdgeOrder::Publication { position, included } => (if included { 1 } else { 2 }, position.0),
        };
        let mut metadata = std::collections::HashMap::from([
            (OWNED_PRESENT.into(), present.0.to_string()),
            (OWNED_CHANGE_COVERAGE.into(), edge.time.0.to_string()),
            ("rtdb.owned.change_position".into(), position.to_string()),
            ("rtdb.owned.change_edge_kind".into(), kind.to_string()),
            ("rtdb.owned.change_complete".into(), page.is_complete().to_string()),
            ("rtdb.owned.change_time_projectable".into(), page.is_time_projectable().to_string()),
        ]);
        if let Some(witness) = witness {
            metadata.insert(WORLD_RANGE_WITNESS.into(), witness.simple().to_string());
        }
        let schema = Schema::new(vec![
            Field::new("system_time_micros", DataType::Int64, false),
            Field::new("transaction_position", DataType::Int64, false),
            rtdb_columnar::to_field("changes", &changes_type).map_err(status)?,
        ]).with_metadata(metadata);
        if page.commits.is_empty() {
            return self.encode_owned_stream_reply(self.memory(), &schema, &[], ipc_stream);
        }
        let schema = Arc::new(schema);
        let _rows = self.memory().reserve(wire::add(wire::mul(page.retained_bytes, 2)?, 8192)?).map_err(status)?;
        let history = Int64Array::from_iter_values(page.commits.iter().map(|commit| commit.publication.system_time.0));
        let positions = Int64Array::from_iter_values(page.commits.iter().map(|commit| commit.publication.tx_id.0));
        let (changes, _arrow) = changes::merged_column(self.memory(), &changes_type, schema.field(2), &page.commits)?;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(history), Arc::new(positions), changes]).map_err(arrow)?;
        self.encode_owned_stream_reply(self.memory(), &schema, &[batch], ipc_stream)
    }

    fn owned_changes_reply(
        &self, page: &rtdb_node::OwnedChangePage, present: Timestamp, ipc_stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
        let touches_type = changes::touches_type();
        let schema = Arc::new(Schema::new(vec![
            Field::new("revision", DataType::UInt64, false),
            Field::new("system_time_micros", DataType::Int64, false),
            rtdb_columnar::to_field("touches", &touches_type).map_err(status)?,
        ]).with_metadata(std::collections::HashMap::from([
            (OWNED_PRESENT.into(), present.0.to_string()),
            (OWNED_CHANGE_COVERAGE.into(), page.covered_through.0.to_string()),
        ])));
        if page.groups.is_empty() {
            return self.encode_owned_stream_reply(self.memory(), &schema, &[], ipc_stream);
        }
        let _rows = self.memory().reserve(wire::add(wire::mul(page.retained_bytes, 2)?, 8192)?).map_err(status)?;
        let revisions = UInt64Array::from(page.groups.iter().map(|group| group.revision).collect::<Vec<_>>());
        let times = Int64Array::from(page.groups.iter().map(|group| group.system_time.0).collect::<Vec<_>>());
        let (touches, _arrow) = changes::touches_column(self.memory(), &touches_type, schema.field(2), &page.groups)?;
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(revisions), Arc::new(times), touches]).map_err(arrow)?;
        self.encode_owned_stream_reply(self.memory(), &schema, &[batch], ipc_stream)
    }

    fn take_owned_read(&self, call: &Call, handle: &Bytes) -> Result<Arc<OwnedReadResource>, Status> {
        found(self.0.owned_reads.take(handle), "owned read")
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
        let (publication, rest) = split_world_publication(rest)?;
        let (child, rest) = rest.split_first_chunk::<16>().ok_or_else(|| invalid("Overlay preparation requires a child identity"))?;
        let (until, mode) = rest.split_first_chunk::<8>().ok_or_else(|| invalid("Overlay preparation requires a retention deadline"))?;
        let retain_until = Timestamp(i64::from_be_bytes(*until));
        let writable = match mode {
            [0] => false,
            [1] => true,
            _ => return Err(invalid("Overlay preparation requires its exact clock mode")),
        };
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let attachment = attachment.ok_or_else(unsupported)?;
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        require_idle(&session)?;
        let scope = session.placed_history().map_err(status)?.ok_or_else(unsupported)?;
        let cut = held.snapshot.retained_child_cut(scope, publication).await.map_err(status)?;
        let charge = call.reserve(4096)?;
        let prepared = host.prepare_world_overlay(
            crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
            cut, Uuid::from_bytes(*child), retain_until, writable,
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
        let charge = call.reserve(128 * 1024)?;
        let retained = host.retain_replayed_world(
            crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
            *binding, Uuid::from_bytes(world),
        ).await.map_err(status)?;
        let body = serde_json::to_vec(&retained).map_err(|_| Status::internal("Could not encode replay world retention"))?;
        Ok(ActionResult { body: wire::retain(body, charge) })
    }

    pub(crate) async fn open_world_read_action(&self, call: &Call, body: &[u8]) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let bytes: [u8; 16] = body.try_into().map_err(|_| invalid("Owned world read open requires a world UUID"))?;
        let world = Uuid::from_bytes(bytes);
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let attachment = attachment.ok_or_else(unsupported)?;
        if attachment.requires_anchored_catalog() {
            return Err(status(Anomaly::unsupported("rtdb.owned/ordered-world-unheld",
                "The installed journal cannot protect the world and all of its parent checkpoints")));
        }
        let host = self.0.config.owned_world_host.as_ref().ok_or_else(unsupported)?;
        let permit = Arc::clone(&self.0.owned_read_slots).try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("The node has too many retained owned reads"))?;
        let reservation = call.reserve(wire::add(2048, wire::add(call.session.id.len(), call.database.len())?)?)?;
        let connection = self.connection(call, &call.database).await?;
        let mut session = connection.borrow().await?;
        require_idle(&session)?;
        let snapshot = host.open_world_read(
            crate::WorldCall { node: &self.0.node, attachment: &attachment, session: &mut session, cancellation: &call.cancellation },
            world,
        ).await.map_err(status)?;
        let handle = Uuid::new_v4();
        let parent = snapshot.parent_basis();
        let ancestors = snapshot.ancestors();
        let reply_size = 57 + ancestors.len() * 24;
        let reply_charge = call.reserve(reply_size + 64)?;
        let mut reply = Vec::with_capacity(reply_size);
        reply.extend_from_slice(handle.as_bytes());
        reply.extend_from_slice(&snapshot.present().0.to_be_bytes());
        reply.extend_from_slice(&parent.tx_id.0.to_be_bytes());
        reply.extend_from_slice(&parent.system_time.0.to_be_bytes());
        reply.extend_from_slice(&snapshot.application_clock().unwrap_or(Timestamp::MIN).0.to_be_bytes());
        reply.extend_from_slice(&snapshot.initial_clock().0.to_be_bytes());
        reply.push(match snapshot.clock_mode() {
            rtdb_types::WorldClockMode::Retained => 0,
            rtdb_types::WorldClockMode::Held { .. } => 1,
            rtdb_types::WorldClockMode::Proposal => 2,
        });
        for (world, system) in ancestors {
            reply.extend_from_slice(&world.0);
            reply.extend_from_slice(&system.0.to_be_bytes());
        }
        let held = Arc::new(OwnedWorldReadResource {
            snapshot, world, conditions: Mutex::new(BTreeMap::new()),
            owner: call.session.owner(), session: call.session.id.clone(), database: call.database.clone(),
            _permit: permit, _reservation: reservation,
        });
        call.session.register_read(|| self.0.owned_world_reads.insert(
            Bytes::copy_from_slice(handle.as_bytes()), held, READ_IDLE, READ_IDLE, false, 16,
        ).map_err(status))?;
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
            Some(held.snapshot.preflight_accepted_scope(&relations, system).await.map_err(status)?)
        };
        for relation in &relations { held.snapshot.check_relation(relation).map_err(status)?; }
        let Some((publication, clock)) = selected else {
            return Ok(ActionResult { body: Bytes::from_static(&[0]) });
        };
        let mut reply = Vec::with_capacity(25);
        reply.extend_from_slice(&publication.tx_id.0.to_be_bytes());
        reply.extend_from_slice(&publication.system_time.0.to_be_bytes());
        reply.extend_from_slice(&clock.0.to_be_bytes());
        reply.push(mode);
        Ok(ActionResult { body: wire::retain(reply, call.reserve(25)?) })
    }

    pub(crate) async fn held_world_point_at(&self, call: &Call, envelope: &Bytes, parent: bool, compact: Option<CompactIdentity>, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, body) = envelope.split_first_chunk::<16>().ok_or_else(|| invalid("Historical world point has no held handle"))?;
        let (system, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Historical world point has no ordered present"))?;
        let system = Timestamp(i64::from_be_bytes(*system));
        let handle = Bytes::copy_from_slice(handle);
        let held = self.owned_world_resource(call, &handle)?;
        let (relation, id, valid, _identity) = selected_point_request(call, envelope, body, PointTime::Required, compact)?;
        let valid = valid.ok_or_else(|| invalid("Historical world point requires a valid instant"))?;
        let frozen = held.snapshot.ancestor().is_some_and(|(_, inherited)|
            !held.snapshot.clock_mode().is_writable() || system <= inherited);
        let original_scope = if (parent || frozen) && matches!(relation.name(), "fact_unit_column_payloads" | rtdb_types::ROOT_ROW_CARRIERS) {
            if !self.supports_hosted_upload(call) { return Err(unsupported()); }
            Some(self.hosted_control_scope(call).await?)
        } else if !parent && frozen && self.supports_hosted_upload(call)
            && matches!(&id, EntityId::Binary(key) if rtdb_types::RootRowCarrier::is_assertion_key(key))
        {
            Some(self.hosted_control_scope(call).await?)
        } else {
            None
        };
        let point = if parent {
            held.snapshot.parent_point_at(&relation, &id, valid, system, original_scope).await
        } else {
            held.snapshot.accepted_point(&relation, &id, valid, system, original_scope).await
        }.map_err(status)?;
        if point.present != system || point.witness.is_some() || point.until.is_some() {
            return Err(history_invalid());
        }
        let response = self.world_point_reply_with_extent(call, point, valid, None, None, reply_format)?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(response)
    }

    pub(crate) async fn held_world_point(&self, call: &Call, envelope: &Bytes, compact: Option<CompactIdentity>, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, body) = envelope.split_first_chunk::<16>().ok_or_else(|| invalid("Owned world point has no held handle"))?;
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let (mode, body) = body.split_first().ok_or_else(|| invalid("Owned world point has no observation flag"))?;
        let (relation, id, valid, _identity) = selected_point_request(call, envelope, body, PointTime::Required, compact)?;
        let valid_at = valid.ok_or_else(|| invalid("Owned point requires a valid_micros instant"))?;
        let original_scope = if matches!(relation.name(), SHARED_COLUMN_CARRIERS | rtdb_types::ROOT_ROW_CARRIERS) {
            if !matches!(*mode, 2 | 3) || !self.supports_hosted_upload(call) { return Err(unsupported()); }
            Some(self.hosted_control_scope(call).await?)
        } else {
            None
        };
        let mut result = match mode {
            0 | 1 => held.snapshot.read_point(&relation, &id, valid_at, *mode == 1).await,
            2 | 3 => held.snapshot.read_parent_point(&relation, &id, valid_at, *mode == 3, original_scope).await,
            4 => held.snapshot.read_ancestor_root_point(&relation, &id, valid_at).await,
            _ => return Err(invalid("Invalid owned world point source")),
        }.map_err(status)?;
        if result.present != held.snapshot.present() {
            return Err(Status::internal("Owned world point changed its held present"));
        }
        let until = result.until;
        let absent = result.row.is_none() && !result.shadowed;
        let nonce = result.witness.take().map(|evidence| held.remember(call, evidence, absent)).transpose()?;
        let reply = self.world_point_reply_with_extent(call, result, valid_at, nonce, None, reply_format)?;
        if matches!(*mode, 2 | 3 | 4) {
            held.snapshot.check_parent_point(&relation, until).map_err(status)?;
        } else {
            held.snapshot.check_relation(&relation).map_err(status)?;
        }
        Ok(reply)
    }

    pub(crate) async fn held_world_valid_during_action(&self, call: &Call, envelope: &Bytes, selection: WorldHistoricalRead, ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
        use rtdb_types::Interval;

        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, body) = envelope.split_first_chunk::<16>().ok_or_else(|| invalid("Owned world history has no held handle"))?;
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
        let batch = request_batch(call, call.retain_request_bytes(envelope.slice_ref(arrow_bytes)))?;
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
        let reply = self.encode_owned_stream_reply(self.memory(), &schema, &[rows], ipc_stream)?;
        held.snapshot.check_relation(&relation).map_err(status)?;
        Ok(reply)
    }

    pub(crate) async fn held_world_shared_column_census_action(
        &self, call: &Call, body: &[u8], reply_format: OwnedReplyFormat<'_>,
    ) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none()
            || !self.supports_hosted_upload(call)
        {
            return Err(unsupported());
        }
        let scope = self.hosted_control_scope(call).await?;
        let (handle, body) = body.split_first_chunk::<16>()
            .ok_or_else(|| invalid("Shared field census has no held world"))?;
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let (valid, body) = body.split_first_chunk::<8>()
            .ok_or_else(|| invalid("Shared field census has no valid instant"))?;
        let valid = Timestamp(i64::from_be_bytes(*valid));
        let (root, world) = shared_column_scopes(body)?;
        let (page, [root_witness, overlay_witness]) = held.snapshot.shared_column_census(scope, valid, root, world).await.map_err(status)?;
        let bytes = page.keys.iter().try_fold(4096, |size, key| wire::add(size, key.retained_bytes()))?;
        let _copy = call.reserve(wire::add(wire::mul(bytes, 2)?, wire::mul(page.keys.len(), 64)?)?)?;
        let rows = shared_column_census_rows(&page.keys)?;
        let nonce = held.remember_checks(call, root_witness, Some(overlay_witness), false)?;
        let schema = rows.schema_ref().as_ref().clone().with_metadata(
            [(WORLD_RANGE_WITNESS.into(), nonce.simple().to_string())].into()
        );
        let reply = self.encode_owned_point_reply(self.memory(), &schema, Some(&rows), held.snapshot.present(), valid,
            None, None, reply_format)?;
        for name in [SHARED_COLUMN_CARRIERS, "fact_unit_column_payloads"] {
            held.snapshot.check_relation(&OwnedRelation::new(name).map_err(status)?).map_err(status)?;
        }
        Ok(reply)
    }

    pub(crate) async fn held_world_identities_action(
        &self, call: &Call, envelope: &Bytes, mode: WorldIdentityRead, reply_format: OwnedReplyFormat<'_>,
    ) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions || self.0.config.owned_world_host.is_none() { return Err(unsupported()); }
        let (handle, body) = envelope.split_first_chunk::<16>().ok_or_else(|| invalid("Owned world identities have no held handle"))?;
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
        let request = decode_batch(call, call.retain_request_bytes(envelope.slice_ref(arrow_bytes)))?;
        let request = request.batch();
        if request.num_columns() != 3 || request.num_rows() != 1 {
            return Err(invalid("Owned world identities require one valid instant, exclusive bound, and limit"));
        }
        let valid = history_request_endpoint(request, "valid_micros")?;
        let limit = usize::try_from(history_request_integer(request, "limit")?)
            .map_err(|_| invalid("Owned world identity limit is out of range"))?;
        let after = identity_scan_request(request)?;
        let page = match (mode, system) {
            (WorldIdentityRead::ParentAt, Some(system)) =>
                held.snapshot.parent_identity_page_at(&relation, valid, &after, limit, system).await,
            (WorldIdentityRead::EffectiveAt, Some(system)) =>
                held.snapshot.identity_page_at(&relation, valid, &after, limit, system, parent_only).await,
            (WorldIdentityRead::OverlayAt, Some(system)) =>
                held.snapshot.overlay_identities_at(&relation, valid, &after, limit, system).await,
            (WorldIdentityRead::Overlay, None) =>
                held.snapshot.overlay_identities(&relation, valid, &after, limit).await,
            (WorldIdentityRead::Effective, None) =>
                held.snapshot.identity_page(&relation, valid, &after, limit).await,
            _ => return Err(invalid("Invalid held world identity coordinates")),
        }.map_err(status)?;
        let (rows, _copy) = identity_scan_reply(call, &after, &page.keys)?;
        let schema = rows.schema_ref().as_ref().clone().with_metadata(
            [(OWNED_IDENTITY_MORE.into(), u8::from(page.more).to_string())].into()
        );
        let reply = self.encode_owned_point_reply(
            self.memory(), &schema, Some(&rows), system.unwrap_or_else(|| held.snapshot.present()), valid,
            None, None, reply_format,
        )?;
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
        self.world_point_reply_with_extent(call, result, valid_at, nonce, None, OwnedReplyFormat::Unary)
            .map(OwnedIpcReply::into_unary)
    }

    fn world_point_reply_with_extent(
        &self, call: &Call, result: crate::WorldPointResult, valid_at: Timestamp, nonce: Option<Uuid>,
        extent: Option<rtdb_types::Interval>,
        reply_format: OwnedReplyFormat<'_>,
    ) -> Result<OwnedIpcReply, Status> {
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
        if let Some(winner) = result.frozen_winner {
            let author = Uuid::from_bytes(winner.author.0).simple();
            let system = winner.publication.system_time.0;
            let transaction = winner.publication.tx_id.0;
            let ordinal = winner.event_ordinal;
            let reading = winner.reading.0;
            metadata.insert("rtdb.owned.frozen_world_winner".into(),
                format!("{author}{system:016x}{transaction:016x}{ordinal:016x}{reading:016x}"));
        }
        if let Some(row) = result.row {
            let crate::WorldPointRow { batch: row, revision, assertion, reservation: _retained } = row;
            if let Some(assertion) = assertion {
                metadata.insert(OWNED_ASSERTION_LINE.into(),
                    Uuid::from_bytes(assertion.line.map_or([0; 16], |line| line.0)).simple().to_string());
                metadata.insert(OWNED_ASSERTION_ORDINAL.into(), assertion.ordinal.to_string());
                if let Some(reading) = assertion.held_reading {
                    metadata.insert(OWNED_ASSERTION_HELD_READING.into(), reading.0.to_string());
                }
            }
            let (cells, _encoding) = prepare_world_point(&row, call.memory())?;
            let (fields, columns) = cells.into_iter().zip(row.schema_ref().fields()).map(|(cell, field)| cell.write(field.name()).map_err(status))
                .collect::<Result<Vec<_>, _>>()?.into_iter().unzip::<_, _, Vec<_>, Vec<_>>();
            let schema = Arc::new(Schema::new_with_metadata(fields, metadata));
            let batch = RecordBatch::try_new(Arc::clone(&schema), columns).map_err(arrow)?;
            self.encode_owned_point_reply(self.memory(), &schema, Some(&batch), result.present, valid_at, Some(revision), result.until, reply_format)
        } else {
            self.encode_owned_point_reply(self.memory(), &Schema::empty().with_metadata(metadata), None, result.present, valid_at, None, None, reply_format)
        }
    }

    pub(crate) async fn publish_held_world_retention_action(&self, call: &Call, envelope: &Bytes) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let host = self.0.config.owned_world_host.as_ref().ok_or_else(unsupported)?;
        let (authority, body) = Self::installed_authority(envelope)?;
        let (handle, body) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Retaining a Held publication requires its original hold"))?;
        let (publication, body) = split_world_publication(body)?;
        let (child, body) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Retaining a Held publication requires a minted child"))?;
        let (mode, body) = body.split_first().ok_or_else(|| invalid("The child has no clock mode"))?;
        let requested = match mode {
            0 => rtdb_node::WorldClockRequest::Retained,
            1 => rtdb_node::WorldClockRequest::Held { start: None },
            _ => return Err(invalid("The child has an invalid clock mode")),
        };
        let (until, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("The retained child has no expiry"))?;
        let (deadline, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("The retained child has no commit deadline"))?;
        let (attempt, body) = body.split_first_chunk::<22>().ok_or_else(|| invalid("The retained child has no attempt identity"))?;
        let (fingerprint, body) = owned_landing_wire::world_fingerprint(body).ok_or_else(|| invalid("The retained child has no fingerprint"))?;
        let attempt = std::str::from_utf8(attempt).map_err(|_| invalid("Invalid retained child attempt"))?;
        if child == &[0; 16] || !attempt.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) {
            return Err(invalid("Invalid retained child identity or attempt"));
        }
        let held = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
        let scope = self.hosted_control_scope(call).await?;
        let cut = held.snapshot.retained_child_cut(scope, publication).await.map_err(status)?;
        let policy = self.hosted_control_policy(call)?;
        let groups = owned_groups(body, true, false)?;
        hosted_control::admit_control_targets(policy, groups.iter().map(|(relation, _)| relation))?;
        let mut rows_left = MAX_GROUPED_COMMIT_ROWS - 3;
        let control_frame = call.retain_request_bytes(envelope.clone());
        let (mut controls, _charges) = self.decode_hosted_control_groups(call, &control_frame, groups, scope, &mut rows_left, true)?;
        rtdb_tx::bind_installed_authority(&mut controls, authority).map_err(status)?;
        if controls.iter().any(|control| !control.is_check()) {
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
                requested,
                deadline, attempt: attempt.to_owned(), fingerprint, controls, root: None,
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
        let (authority, body) = Self::installed_authority(envelope)?;
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
        let select_publication = matches!(*mode, 4 | 5 | 11) && publish_root;
        let public_version = *mode == 11 && publish_root;
        let proposed_publication = *mode == 5 && publish_root;
        let held_publication = *mode == 10 && publish_root;
        let retained_publication = *mode == 12 && publish_root;
        if (held_publication || retained_publication)
            && (*previous_handle != [0; 16] || *capture != [0; 16] || *suffix != [0; 16])
        {
            return Err(invalid("An exact Root fork derives its archive from the selected publication"));
        }
        let proposal_hold = matches!(*mode, 8 | 9) && publish_root;
        let proposal_shorten = *mode == 9 && publish_root;
        let sibling = *mode == 7 && publish_root;
        let renewal = matches!(*mode, 6 | 7 | 8 | 9) && publish_root;
        if renewal && (*previous_handle == [0; 16] || *capture != [0; 16] || *suffix != [0; 16]) {
            return Err(invalid("An authored renewal requires its original retained handle and derives its archive"));
        }
        let (public_basis, rest) = if public_version {
            let (publication, rest) = split_world_publication(rest)?;
            (Some(publication), rest)
        } else {
            (None, rest)
        };
        // An exact Root fork names its publication by position and time (T7).
        let (exact_basis, rest) = if held_publication || retained_publication {
            let (publication, rest) = split_world_publication(rest)?;
            (Some(publication), rest)
        } else {
            (None, rest)
        };
        let (requested, rest) = match *mode {
            10 if held_publication => (rtdb_node::WorldClockRequest::Held { start: None }, rest),
            12 if retained_publication => (rtdb_node::WorldClockRequest::Retained, rest),
            0 => (rtdb_node::WorldClockRequest::Retained, rest),
            8 | 9 if proposal_hold => (rtdb_node::WorldClockRequest::Proposal, rest),
            6 | 7 if renewal => (rtdb_node::WorldClockRequest::Retained, rest),
            4 | 11 if select_publication => (rtdb_node::WorldClockRequest::Retained, rest),
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
        let (fingerprint, groups) = owned_landing_wire::world_fingerprint(rest).ok_or_else(|| invalid("World fork has no fingerprint"))?;
        let attempt = std::str::from_utf8(attempt).map_err(|_| invalid("Invalid world fork attempt identity"))?;
        if !attempt.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) {
            return Err(invalid("Invalid world fork attempt identity"));
        }
        let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
        let policy = self.hosted_control_policy(call)?;
        let scope = self.hosted_control_scope(call).await?;
        let cut = if let Some(basis) = public_basis {
            held.snapshot.fork_public_version_cut(scope, basis).await.map_err(status)?
        } else if let Some(basis) = exact_basis {
            held.snapshot.fork_root_publication_at(scope, basis, requested).await.map_err(status)?
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
                || (!renewal && previous.snapshot.parent_basis().tx_id >= cut.positioned_basis().tx_id)
                || previous.snapshot.parent_pin().0 == *pin
            {
                return Err(Status::failed_precondition("The original retained world cannot select this cut"));
            }
            Some(previous)
        };
        let mut cut = if renewal {
            let predecessor = previous.as_ref().ok_or_else(unsupported)?.snapshot.parent_basis();
            held.snapshot.fork_renewal_cut(scope, predecessor).await.map_err(status)?
        } else {
            cut
        };
        if !retained_publication {
            let root_point = held.snapshot.root_point_at(cut.basis(), public_version).await.map_err(status)?;
            cut.original_root_point(root_point);
        }
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
        let control_frame = call.retain_request_bytes(envelope.clone());
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
                let (refusal, charges) = self.decode_hosted_control_groups(call, &control_frame, refusal, scope, &mut rows_left, false)?;
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
                    _ if rtdb_tx::is_root_partition_frontier(&operation) || rtdb_tx::is_public_root_version_marker(&operation) => writes.push(operation),
                    TxOp::OwnedInsertDocs { relation, .. } | TxOp::OwnedReplaceDocs { relation, .. }
                    | TxOp::OwnedArrowDocs { relation, .. } | TxOp::OwnedRelativePeriod { relation, .. } | TxOp::OwnedDelete { relation, .. } if relation.is_root_data() =>
                        writes.push(operation),
                    _ => return Err(invalid("Operation is outside the held Root section")),
                }
            }
            let facts = writes.iter().filter(|op| !rtdb_tx::is_root_partition_frontier(op) && !rtdb_tx::is_public_root_version_marker(op)).collect::<Vec<_>>();
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
            if retained_publication && (!writes.is_empty() || !root.checks.is_empty() || !root.refusal.is_empty()) {
                return Err(invalid("An exact retained Root publication accepts only hosted controls"));
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
        let (mut controls, _charges) = self.decode_hosted_control_groups(call, &control_frame, groups, scope, &mut rows_left, !publish_root)?;
        rtdb_tx::bind_installed_authority(&mut controls, authority).map_err(status)?;
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
            if held.snapshot.query_time().map_err(status)? >= deadline {
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
                attempt: attempt.into(), fingerprint, controls, root: publish_root.then_some(root),
            },
        ).await.map_err(status)?;
        if registered.as_bytes() != world { return Err(unsupported()); }
        let mut reply = Vec::with_capacity(32);
        reply.extend_from_slice(registered.as_bytes());
        reply.extend_from_slice(&key.tx_id.0.to_be_bytes());
        reply.extend_from_slice(&key.system_time.0.to_be_bytes());
        Ok(ActionResult { body: wire::retain(Bytes::from(reply), reply_charge) })
    }

    pub(crate) async fn decide_held_worlds_action(&self, call: &Call, envelope: &Bytes, prepared_proposal: bool, ipc_stream: bool) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let host = self.0.config.owned_world_host.as_ref().ok_or_else(unsupported)?;
        let policy = self.hosted_control_policy(call)?;
        let (authority, body) = Self::installed_authority(envelope)?;
        let ([flags], body) = body.split_first_chunk::<1>().ok_or_else(|| invalid("Missing world decision flags"))?;
        if *flags & !7 != 0 { return Err(invalid("Invalid world decision flags")); }
        let (slot, body) = if *flags & 1 != 0 {
            let (slot, body) = self.owned_clock_slot(call, body).await?;
            (Some(slot), body)
        } else {
            (None, body)
        };
        let (deadline, body) = body.split_first_chunk::<8>().ok_or_else(|| invalid("Missing world batch deadline"))?;
        let deadline = Timestamp(i64::from_be_bytes(*deadline));
        let (fingerprint, body) = owned_landing_wire::world_fingerprint(body).ok_or_else(|| invalid("Missing world batch fingerprint"))?;
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
            let resource = self.owned_world_resource(call, &Bytes::copy_from_slice(handle))?;
            if !resource.snapshot.clock_mode().is_writable() && *kind != 2 {
                return Err(unsupported());
            }
            let (checks, charge) = resource.conditions(call, world, nonces)?;
            rows_left = rows_left.checked_sub(checks.len() * 2 + 2).ok_or_else(|| invalid("World batch exceeds its effect limit"))?;
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
                    self.decode_world_writes(call, envelope, owned_groups(overlay, true, false)?,
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
        let proposal_family_retirement = *root_kind == 8 && !prepared_proposal;
        let (root_read, body) = match *root_kind {
            0 => (None, body),
            kind if (1..=7).contains(&kind) || proposal_family_retirement => {
                let (handle, body) = body.split_first_chunk::<16>().ok_or_else(|| invalid("Missing original Root hold"))?;
                let held = self.owned_read(call, &Bytes::copy_from_slice(handle))?;
                held.snapshot.require_unscoped_root().map_err(status)?;
                (Some(held), body)
            }
            _ => return Err(invalid("Invalid Root section kind")),
        };
        let delete_root = *root_kind == 4;
        let proposal_overlay = *root_kind == 5 || proposal_family_retirement;
        let proposal_land = *root_kind == 6;
        if !prepared_landing::admits(proposal_land, prepared_proposal, slot.is_some()) {
            return Err(invalid("A prepared Proposal switch requires its landing and offered clock"));
        }
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
        } else if *root_kind == 2 || proposal_family_retirement {
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
            call, envelope, owned_groups(root, false, true)?, &mut root_ops, &mut rows_left, CommitEffects::FamilyFence,
        )?;
        let mut root = rtdb_node::OwnedRootSection { checks: counts, ..Default::default() };
        let mut root_writes = Vec::new();
        for operation in root_ops {
            match &operation {
                TxOp::OwnedCheck { relation, .. } if relation.world().is_none() && !relation.is_world_catalog() =>
                    root.checks.push(operation),
                _ if proposal_family_retirement && rtdb_tx::proposal_family_root_write(&operation) =>
                    root_writes.push(operation),
                _ if proposal_family_retirement => return Err(invalid("Operation is outside the Proposal family retirement")),
                _ if root_attempt.is_some() && (rtdb_tx::is_root_partition_frontier(&operation) || rtdb_tx::is_public_root_version_marker(&operation)) =>
                    root_writes.push(operation),
                _ if proposal_land && rtdb_tx::is_proposal_landing_frontier(&operation) =>
                    root_writes.push(operation),
                TxOp::OwnedErase { relation, .. } if *root_kind == 2 && relation.is_root_data() =>
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
            let facts = root_writes.iter().filter(|op| !rtdb_tx::is_root_partition_frontier(op) && !rtdb_tx::is_public_root_version_marker(op)).collect::<Vec<_>>();
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
        if (proposal_overlay && !proposal_eligible) || (terminal_proposal && (!proposal_overlay || proposal_family_retirement)) {
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
        let family_fence = controls.iter().any(|(relation, _)| relation.name() == rtdb_types::ASSERTION_FAMILY_FENCES);
        let control_frame = call.retain_request_bytes(envelope.clone());
        let (mut controls, _charges) = self.decode_hosted_control_groups(call, &control_frame, controls, scope, &mut rows_left,
            !family_fence && !release && !delete_root && !proposal_overlay && !proposal_land && !proposal_close)?;
        rtdb_tx::bind_installed_authority(&mut controls, authority).map_err(status)?;
        if proposal_family_retirement && !controls.iter().any(rtdb_tx::HostedControlWrite::is_assertion_family_marker)
            || prepared_proposal && controls.iter().any(rtdb_tx::HostedControlWrite::is_assertion_family_marker)
        {
            return Err(invalid("The decision has no matching family retirement marker"));
        }
        let _refusal_charge = if !refusal.is_empty() {
            hosted_control::admit_control_targets(policy, refusal.iter().map(|(relation, _)| relation))?;
            let (refusal, charge) = self.decode_hosted_control_groups(call, &control_frame, refusal, scope, &mut rows_left, false)?;
            if proposal_land && refusal.len() != 2 {
                return Err(invalid("A landing refusal requires its original receipt and proof"));
            }
            root.refusal = refusal;
            Some(charge)
        } else {
            None
        };
        if proposal_overlay && !rtdb_tx::proposal_overlay_controls(&controls, !terminal_proposal) {
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
        if let Some(tick) = slot.filter(|_| !proposal_land) {
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
            if read.snapshot.query_time().map_err(status)? >= deadline
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
            sections, root, observed, fingerprint, controls, deadline,
        ).await.map_err(status)?;
        drop(observations);
        self.owned_tx_reply(&session, committed, ipc_stream).await
    }

    fn decode_world_writes(
        &self, call: &Call, envelope: &Bytes, groups: Vec<(OwnedRelation, &[u8])>, proposal: bool, rows_left: &mut usize,
        writes: &mut Vec<rtdb_node::OwnedWorldWrite>, reservations: &mut Vec<Arc<MemoryReservation>>,
    ) -> Result<(), Status> {
        use rtdb_node::OwnedWorldWrite;

        for (relation, arrow) in groups {
            let batch = request_batch(call, call.retain_request_bytes(envelope.slice_ref(arrow)))?;
            let batch = batch.batch();
            let schema = batch.schema_ref();
            let field = |name| schema.column_with_name(name).map(|(_, field)| field.data_type());
            let packed_operation = schema.column_with_name("_op")
                .is_some_and(|(_, field)| upload::operation_dictionary(field));
            if schema.fields().len() != 5 || !packed_operation
                || field("_valid_from_micros") != Some(&DataType::Int64)
                || field("_valid_to_micros") != Some(&DataType::Int64)
                || !matches!(field("fact"), Some(DataType::Struct(_)))
            {
                return Err(invalid("Owned world commit requires _id, dictionary _op, two Int64 valid endpoints, and a fact struct"));
            }
            *rows_left = rows_left.checked_sub(batch.num_rows())
                .ok_or_else(|| invalid("Owned world commit exceeds its total row limit"))?;
            let charge = call.reserve(wire::mul(batch.num_rows(), std::mem::size_of::<OwnedWorldWrite>() + 512)?)?;
            writes.try_reserve_exact(batch.num_rows()).map_err(|_| Status::resource_exhausted("Owned world commit allocation failed"))?;
            reservations.push(charge);
            if let Some(decoded) = try_world_arrow_writes(batch, &relation, proposal, call.memory(), writes) {
                reservations.push(decoded);
                continue;
            }
            let (rows, decoded) = upload::owned_documents(batch, call.memory())?;
            reservations.push(decoded);
            for mut row in rows {
                let id = row.remove("_id").ok_or_else(|| invalid("Owned world write has no _id"))?.owned_entity_id().map_err(status)?;
                let (Some(Value::Utf8(operation)), Some(from), Some(to), Some(payload)) = (
                    row.remove("_op"), row.remove("_valid_from_micros"), row.remove("_valid_to_micros"), row.remove("fact"),
                ) else { return Err(invalid("Owned world write requires a typed operation and explicit valid endpoints")); };
                let from = optional_micros(Some(from))?;
                let to = optional_micros(Some(to))?;
                writes.push(world_write(&relation, id, &operation, from, to, payload, proposal)?);
            }
        }
        Ok(())
    }

}

fn try_world_arrow_writes(
    batch: &RecordBatch, relation: &OwnedRelation, proposal: bool, memory: &Arc<MemoryBudget>,
    writes: &mut Vec<rtdb_node::OwnedWorldWrite>,
) -> Option<Arc<MemoryReservation>> {
    use arrow_array::{cast::AsArray, types::Int64Type};

    let id = ArrowCell::named(batch, "_id", 0)?;
    let fact = ArrowCell::named(batch, "fact", 0)?;
    let op = ArrowCell::named(batch, "_op", 0)?;
    let endpoint = |name| {
        let cell = ArrowCell::named(batch, name, 0)?;
        if !cell.field.metadata().is_empty() { return None; }
        cell.array.as_primitive_opt::<Int64Type>()
    };
    let from = endpoint("_valid_from_micros")?;
    let to = endpoint("_valid_to_micros")?;
    let bytes = (0..batch.num_rows()).try_fold(8192usize, |bytes, row| {
        let id_bytes = rtdb_columnar::estimate_read_value_bytes(id.field, id.array, row).ok()?;
        let fact_bytes = rtdb_columnar::estimate_read_value_bytes(fact.field, fact.array, row).ok()?;
        bytes.checked_add(id_bytes)?.checked_add(fact_bytes)
    })?;
    let charge = memory.reserve(bytes).ok()?;
    let ids = rtdb_columnar::ColumnReader::new(id.field).ok()?;
    let facts = rtdb_columnar::ColumnReader::new(fact.field).ok()?;
    let start = writes.len();
    let appended = (0..batch.num_rows()).try_for_each(|row| {
        let id = ids.read_owned_entity_id(id.array, row).ok()??.into_owned();
        let operation = upload::owned_operation(ArrowCell { row, ..op })?;
        let from = from.is_valid(row).then(|| Timestamp(from.value(row)));
        let to = to.is_valid(row).then(|| Timestamp(to.value(row)));
        let payload = facts.read(fact.array, row).ok()?;
        writes.push(world_write(relation, id, operation, from, to, payload, proposal).ok()?);
        Some(())
    });
    if appended.is_none() {
        writes.truncate(start);
        return None;
    }
    Some(charge)
}

fn world_write(
    relation: &OwnedRelation, id: EntityId, operation: &str, from: Option<Timestamp>, to: Option<Timestamp>,
    payload: Value, proposal: bool,
) -> Result<rtdb_node::OwnedWorldWrite, Status> {
    use rtdb_types::WorldShadow;

    let valid = match (from, to) {
        (None, None) => None,
        (Some(from), Some(to)) => {
            let valid = Interval::new(from, to).map_err(status)?;
            if valid.is_empty() { return Err(invalid("Owned world write needs a nonempty valid interval")); }
            Some(valid)
        }
        _ => return Err(invalid("Owned world write has only one valid endpoint")),
    };
    let create_only = operation == "insert";
    if create_only && valid.is_some()
        && !(relation.is_world_custody_control() && valid == Some(Interval::ALL))
    {
        return Err(invalid("World creates require acceptance validity or a full custody control interval"));
    }
    let mut ordered_keys = None;
    let shadow = match (operation, payload) {
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
    Ok(rtdb_node::OwnedWorldWrite { relation: relation.clone(), id, shadow, valid, create_only, ordered_keys })
}

#[test]
fn world_arrow_envelopes_keep_typed_identity_validity_and_shadows_in_order() {
    use arrow_array::{DictionaryArray, Int8Array, types::Int8Type};
    use arrow_buffer::NullBuffer;
    use rtdb_types::WorldShadow;

    let relation = OwnedRelation::new("facts").unwrap();
    let memory = MemoryBudget::new(1 << 20);
    let keys = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0, 0, 1, 2]), Arc::new(StringArray::from(vec!["replace", "delete", "unknown"])),
    ).unwrap();
    let ids = rtdb_columnar::entity_id_array([
        EntityIdRef::Integer(0), EntityIdRef::String("same"), EntityIdRef::Binary(b"same"), EntityIdRef::Integer(3),
    ]).unwrap();
    let fact = StructArray::new(
        vec![Field::new("name", DataType::Utf8, true)].into(),
        vec![Arc::new(StringArray::from(vec![None, Some("value"), None, None]))],
        Some(NullBuffer::from(vec![false, true, false, false])),
    );
    let fields = vec![
        Field::new("_op", keys.data_type().clone(), false),
        rtdb_columnar::to_field("_id", &entity_id_type()).unwrap(),
        Field::new("_valid_to_micros", DataType::Int64, true),
        Field::new("fact", fact.data_type().clone(), true),
        Field::new("_valid_from_micros", DataType::Int64, true),
    ];
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![
        Arc::new(keys), ids, Arc::new(Int64Array::from(vec![None, Some(20), None, None])),
        Arc::new(fact), Arc::new(Int64Array::from(vec![None, Some(10), None, None])),
    ]).unwrap();
    let mut writes = Vec::new();
    let charge = try_world_arrow_writes(&batch.slice(1, 2), &relation, false, &memory, &mut writes).unwrap();
    assert_eq!(writes.len(), 2);
    assert_eq!(writes[0].id, EntityId::String("same".into()));
    assert_eq!(writes[0].valid, Some(Interval::new(Timestamp(10), Timestamp(20)).unwrap()));
    assert_eq!(writes[0].shadow, WorldShadow::Present(StructFields::from([
        ("_id".into(), writes[0].id.as_value()), ("name".into(), Value::Utf8("value".into())),
    ])));
    assert_eq!(writes[1].id, EntityId::Binary(b"same".to_vec()));
    assert_eq!(writes[1].valid, None);
    assert_eq!(writes[1].shadow, WorldShadow::Deleted);
    assert!(try_world_arrow_writes(&batch.slice(1, 3), &relation, false, &memory, &mut writes).is_none());
    assert_eq!(writes.len(), 2);
    assert_eq!(memory.used_bytes(), charge.bytes());
}

impl Service {
    pub(crate) async fn read_owned_action(&self, call: &Call, body: &Bytes, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        if !self.0.config.owned_native_actions { return Err(unsupported()); }
        let (relation, id, _, _identity) = point_request(call, body, body, PointTime::Default)?;
        let connection = self.connection(call, &call.database).await?;
        let mut native = connection.borrow().await?;
        require_idle(&native)?;
        let snapshot = native.open_owned_read().await.map_err(status)?;
        self.execute_owned_point(&snapshot, &relation, &id, None, false, None, reply_format).await
    }

    async fn execute_owned_point(
        &self, snapshot: &OwnedReadSession, relation: &OwnedRelation, id: &EntityId, valid: Option<Timestamp>,
        fixed: bool, origin: Option<(&Call, &OwnedReadResource)>, reply_format: OwnedReplyFormat<'_>,
    ) -> Result<OwnedIpcReply, Status> {
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
        let attempt = (origin.is_some() && !snapshot.is_scoped_root()
            && relation.name() == rtdb_types::OWNED_ATTEMPT_RELATION)
            .then_some(snapshot);
        let reply = self.finish_owned_point_inner(memory, present, valid.unwrap_or(present), id, cursor, lifetime, None, attempt, reply_format, witness).await?;
        snapshot.check_live_point_relation(relation).map_err(status)?;
        Ok(reply)
    }

    async fn finish_owned_point(&self, call: &Call, present: Timestamp, valid: Timestamp, id: &EntityId, result: Option<QueryCursor>, lifetime: Option<rtdb_types::OwnedLifetimeFloor>, reply_format: OwnedReplyFormat<'_>) -> Result<OwnedIpcReply, Status> {
        self.finish_owned_point_inner(call.memory(), present, valid, id, result, lifetime, None, None, reply_format, |_| Ok(None)).await
    }

    async fn finish_owned_point_inner(
        &self, memory: &Arc<MemoryBudget>, present: Timestamp, valid: Timestamp, id: &EntityId, result: Option<QueryCursor>,
        lifetime: Option<rtdb_types::OwnedLifetimeFloor>, path_columns: Option<&[String]>,
        attempt: Option<&OwnedReadSession>,
        reply_format: OwnedReplyFormat<'_>,
        witness: impl FnOnce(Option<Timestamp>) -> Result<Option<Uuid>, Status>,
    ) -> Result<OwnedIpcReply, Status> {
        let Some(mut cursor) = result else {
            let schema = with_point_witness(&Schema::empty(), witness(None)?);
            return self.encode_owned_point_reply(memory, &schema, None, present, valid, None, None, reply_format);
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
            let mut schema = with_point_witness(rows.schema_ref(), witness(until)?);
            if let Some(attempt) = attempt {
                use std::fmt::Write;
                let tx_id = rtdb_query::owned_receipt_transaction(rows, 0).map_err(status)?;
                let key = TransactionKey { tx_id, system_time: revision };
                let mut metadata = schema.metadata().clone();
                metadata.insert("rtdb.owned.receipt_tx_id".into(), tx_id.0.to_string());
                if let Some(observation) = attempt.owned_attempt_observation(key).await.map_err(status)? {
                    let mut encoded = String::with_capacity(rtdb_tx::OrderedClockEvidence::WIDTH as usize * 2);
                    for byte in observation.encode() {
                        write!(encoded, "{byte:02x}").expect("String writes are infallible");
                    }
                    metadata.insert("rtdb.owned.receipt_clock".into(), encoded);
                }
                schema = schema.with_metadata(metadata);
            }
            let reply = self.encode_owned_point_reply(memory, &schema, Some(rows), present, valid, Some(revision), until, reply_format)?;
            if until.is_some_and(|until| until <= self.0.node.now()) {
                return Err(Status::failed_precondition("The held point reached its creation lifetime"));
            }
            Ok(reply)
        } else {
            let schema = if path_columns.is_some() { wire::OwnedSchema::empty(memory)? }
                else { wire::plan_schema(cursor.runtime_schema(), memory)? };
            let schema = with_point_witness(&schema, witness(None)?);
            self.encode_owned_point_reply(memory, &schema, None, present, valid, None, None, reply_format)
        }
    }

    fn encode_owned_point_reply(
        &self, memory: &Arc<MemoryBudget>, schema: &Schema, batch: Option<&RecordBatch>,
        present: Timestamp, valid: Timestamp, revision: Option<Timestamp>, until: Option<Timestamp>,
        reply_format: OwnedReplyFormat<'_>,
    ) -> Result<OwnedIpcReply, Status> {
        Self::with_owned_point_page(memory, schema, batch, present, valid, revision, until, |schema, batch| {
            let stream = match reply_format {
                OwnedReplyFormat::Selection(selection) => {
                    let maximum = self.0.config.maximum_message_bytes.min(crate::config::POINT_REPLY_BYTES);
                    if let Some(reply) = selection.point(memory, schema, batch, maximum)? {
                        return Ok(OwnedIpcReply::Unary(reply));
                    }
                    false
                }
                OwnedReplyFormat::Unary => false,
                OwnedReplyFormat::StreamV1 => true,
            };
            self.encode_owned_stream_reply(memory, schema, batch.map(std::slice::from_ref).unwrap_or_default(), stream)
        })
    }

    fn with_owned_point_page<T>(
        memory: &Arc<MemoryBudget>, schema: &Schema, batch: Option<&RecordBatch>,
        present: Timestamp, valid: Timestamp, revision: Option<Timestamp>, until: Option<Timestamp>,
        finish: impl FnOnce(&SchemaRef, Option<&RecordBatch>) -> Result<T, Status>,
    ) -> Result<T, Status> {
        let _metadata = memory.reserve(wire::schema_bound(schema)?).map_err(status)?;
        let mut metadata = schema.metadata().clone();
        metadata.insert(OWNED_PRESENT.into(), present.0.to_string());
        metadata.insert(OWNED_VALID.into(), valid.0.to_string());
        if let Some((key, at)) = owned_landing_wire::point_revision_metadata(revision.map(|time| time.0)) {
            metadata.insert(key.into(), at.to_string());
        }
        if let Some(until) = until { metadata.insert("rtdb.owned.until_micros".into(), until.0.to_string()); }
        if let Some(batch) = batch.filter(|batch| batch.column_by_name("_assertion_ordinal").is_some()) {
            if batch.num_rows() != 1 {
                return Err(Status::internal("Owned point has an invalid assertion ordinal"));
            }
            if let Some(ordinal) = rtdb_query::owned_row_assertion_ordinal(batch, 0).map_err(status)? {
                metadata.insert(OWNED_ASSERTION_ORDINAL.into(), ordinal.to_string());
            }
        }
        if let Some(batch) = batch.filter(|batch| revision.is_some()
            && batch.num_rows() == 1 && batch.column_by_name(rtdb_types::OWNED_EVENT_POSITION_COLUMN).is_some())
        {
            use std::fmt::Write;
            let position = rtdb_query::owned_row_event_position(batch, 0).map_err(status)?;
            let mut encoded = String::with_capacity(32);
            for byte in position.to_bytes() {
                write!(encoded, "{byte:02x}").expect("String writes are infallible");
            }
            metadata.insert("rtdb.owned.event_position".into(), encoded);
        }
        let columns: Vec<_> = schema.fields().iter().enumerate()
            .filter(|(_, field)| !matches!(field.name().as_str(),
                "_system_from" | "_assertion_ordinal" | rtdb_types::HOSTED_RECEIPT_TX_ID | rtdb_types::OWNED_EVENT_POSITION_COLUMN))
            .map(|(index, _)| index).collect();
        let schema = Arc::new(schema.project(&columns).map_err(arrow)?.with_metadata(metadata));
        let batch = batch.map(|batch| RecordBatch::try_new(Arc::clone(&schema), columns.iter().map(|&index| Arc::clone(batch.column(index))).collect()))
            .transpose().map_err(arrow)?;
        finish(&schema, batch.as_ref())
    }

    fn encode_owned_stream_reply(
        &self, memory: &Arc<MemoryBudget>, schema: &Schema, pages: &[RecordBatch], stream: bool,
    ) -> Result<OwnedIpcReply, Status> {
        OwnedIpcReply::encode(memory, schema, pages, stream, self.0.config.maximum_message_bytes)
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

fn selection_time(request: &[u8]) -> Result<(Timestamp, &[u8]), Status> {
    let (time, rest) = request.split_first_chunk::<8>()
        .ok_or_else(|| invalid("Truncated held selection time"))?;
    Ok((Timestamp(i64::from_be_bytes(*time)), rest))
}

#[derive(Clone, Copy)]
pub(crate) enum CompactIdentity { Text, Binary }

fn selection_identity(
    call: &Call, request: &[u8], format: CompactIdentity,
) -> Result<(EntityId, Arc<MemoryReservation>), Status> {
    let id = match format {
        CompactIdentity::Text => EntityIdRef::String(std::str::from_utf8(request).map_err(arrow)?),
        CompactIdentity::Binary => EntityIdRef::Binary(request),
    };
    let reservation = call.reserve(wire::add(4096, id.owned_payload_bytes())?)?;
    Ok((id.into_owned(), reservation))
}

fn selected_point_request(
    call: &Call, envelope: &Bytes, body: &[u8], time: PointTime, compact: Option<CompactIdentity>,
) -> Result<(OwnedRelation, EntityId, Option<Timestamp>, Arc<MemoryReservation>), Status> {
    let Some(compact) = compact else { return point_request(call, envelope, body, time); };
    let (relation, request) = split_relation(body)?;
    let (valid, request) = selection_time(request)?;
    let (id, reservation) = selection_identity(call, request, compact)?;
    Ok((relation, id, Some(valid), reservation))
}

fn point_request(
    call: &Call, envelope: &Bytes, body: &[u8], time: PointTime,
) -> Result<(OwnedRelation, EntityId, Option<Timestamp>, Arc<MemoryReservation>), Status> {
    let (relation, arrow) = split_relation(body)?;
    let batch = request_batch(call, call.retain_request_bytes(envelope.slice_ref(arrow)))?;
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

fn staged_rank_identity(relation: &OwnedRelation, id: EntityIdRef<'_>) -> bool {
    if relation.name().starts_with("fact_") && relation.name().ends_with("_search") {
        return matches!(id, EntityIdRef::Binary(id) if rtdb_types::is_rank_row_address(id));
    }
    match id {
        EntityIdRef::String(id) => !id.is_empty(),
        EntityIdRef::Binary(id) => !id.is_empty(),
        _ => false,
    }
}

fn identity_scan_request(batch: &RecordBatch) -> Result<OwnedIdentityScan<'_>, Status> {
    let (column, field) = batch.schema_ref().column_with_name("after")
        .filter(|(_, field)| field.metadata().is_empty())
        .ok_or_else(|| invalid("Owned identity page requires a plain exclusive bound"))?;
    let array = batch.column(column);
    if let Some(text) = array.as_any().downcast_ref::<StringArray>() {
        return Ok(OwnedIdentityScan::text((!text.is_null(0)).then(|| text.value(0))));
    }
    if let Some(binary) = array.as_any().downcast_ref::<BinaryArray>() {
        return Ok(OwnedIdentityScan::binary((!binary.is_null(0)).then(|| binary.value(0))));
    }
    Err(invalid("Owned identity bound must be nullable Utf8 or Binary"))
}

fn protected_carrier_prefix(batch: &RecordBatch) -> Result<SharedColumnCarrierPrefix<'_>, Status> {
    let (column, _) = batch.schema_ref().column_with_name("prefix")
        .filter(|(_, field)| field.metadata().is_empty())
        .ok_or_else(|| invalid("Protected carrier prefix is absent"))?;
    batch.column(column).as_any().downcast_ref::<BinaryArray>()
        .filter(|column| !column.is_null(0))
        .and_then(|column| SharedColumnCarrierPrefix::parse(column.value(0)))
        .ok_or_else(|| invalid("Protected carrier prefix must be raw Binary BFCP2 scope bytes"))
}

fn sealed_search_unit_page_request(batch: &RecordBatch) -> Result<(&[u8; 32], Option<&[u8]>, usize), Status> {
    if batch.num_columns() != 3 || batch.num_rows() != 1 {
        return Err(invalid("Sealed search unit page requires a prefix, bound, and limit"));
    }
    let (column, _) = batch.schema_ref().column_with_name("prefix")
        .filter(|(_, field)| field.metadata().is_empty() && !field.is_nullable())
        .ok_or_else(|| invalid("Sealed search unit prefix is absent"))?;
    let prefix = batch.column(column).as_any().downcast_ref::<BinaryArray>()
        .filter(|column| !column.is_null(0))
        .ok_or_else(|| invalid("Sealed search unit prefix must be Binary"))?;
    let (column, _) = batch.schema_ref().column_with_name("after")
        .filter(|(_, field)| field.metadata().is_empty() && field.is_nullable())
        .ok_or_else(|| invalid("Sealed search unit bound is absent"))?;
    let after = batch.column(column).as_any().downcast_ref::<BinaryArray>()
        .ok_or_else(|| invalid("Sealed search unit bound must be Binary"))?;
    let limit = usize::try_from(history_request_integer(batch, "limit")?)
        .map_err(|_| invalid("Sealed search unit page limit is out of range"))?;
    let prefix = prefix.value(0).try_into().map_err(|_| invalid("A sealed search unit alias must be 32 bytes"))?;
    Ok((prefix, (!after.is_null(0)).then(|| after.value(0)), limit))
}

fn shared_column_scopes(body: &[u8]) -> Result<(SharedColumnCarrierPrefix<'_>, &str), Status> {
    let (length, body) = body.split_first_chunk::<2>()
        .ok_or_else(|| invalid("Shared field census has no Root carrier scope"))?;
    let (root, world) = body.split_at_checked(usize::from(u16::from_be_bytes(*length)))
        .ok_or_else(|| invalid("Incomplete shared field census scope"))?;
    let root = SharedColumnCarrierPrefix::parse(root).ok_or_else(|| invalid("Invalid Root carrier scope"))?;
    let world = std::str::from_utf8(world).map_err(|_| invalid("Invalid world carrier scope"))?;
    Ok((root, world))
}

fn shared_column_census_rows(keys: &[EntityId]) -> Result<RecordBatch, Status> {
    let ids = rtdb_columnar::entity_id_array(keys.iter().map(EntityIdRef::from)).map_err(status)?;
    RecordBatch::try_from_iter([("_id", ids)]).map_err(arrow)
}

fn identity_scan_reply(
    call: &Call, scan: &OwnedIdentityScan<'_>, keys: &[EntityId],
) -> Result<(RecordBatch, Arc<MemoryReservation>), Status> {
    let bytes = keys.iter().try_fold(4096, |size, key| wire::add(size, key.retained_bytes()))?;
    let charge = call.reserve(bytes)?;
    let wrong_type = || invalid("Owned identity page has a different identity type");
    let ids: ArrayRef = match scan {
        OwnedIdentityScan::Text(_) => {
            let mut ids = arrow_array::builder::StringBuilder::with_capacity(keys.len(), bytes - 4096);
            for key in keys {
                let EntityId::String(key) = key else { return Err(wrong_type()); };
                ids.append_value(key);
            }
            Arc::new(ids.finish())
        }
        OwnedIdentityScan::Binary(_) => {
            let mut ids = arrow_array::builder::BinaryBuilder::with_capacity(keys.len(), bytes - 4096);
            for key in keys {
                let EntityId::Binary(key) = key else { return Err(wrong_type()); };
                ids.append_value(key);
            }
            Arc::new(ids.finish())
        }
        OwnedIdentityScan::Unbounded => return Err(wrong_type()),
    };
    let rows = RecordBatch::try_from_iter([("_id", ids)]).map_err(arrow)?;
    Ok((rows, charge))
}

fn absent_keys_valid(batch: &RecordBatch) -> Result<Timestamp, Status> {
    if batch.num_columns() != 2 || !(1..=64).contains(&batch.num_rows()) {
        return Err(invalid("Owned absence requires 1 to 64 keys and one valid instant"));
    }
    let ids = batch.column_by_name("_id")
        .filter(|column| matches!(column.data_type(), DataType::Utf8 | DataType::Binary))
        .ok_or_else(|| invalid("Owned absence requires Utf8 or Binary keys"))?;
    let times = batch.column_by_name("valid_micros")
        .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
        .ok_or_else(|| invalid("Owned absence requires Int64 valid instants"))?;
    let schema = batch.schema_ref();
    if ids.null_count() != 0 || times.null_count() != 0 || !schema.metadata().is_empty()
        || schema.fields().iter().any(|field| !field.metadata().is_empty())
    {
        return Err(invalid("Owned absence requires plain non-null keys and valid instants"));
    }
    let valid = times.value(0);
    if times.values().iter().any(|time| *time != valid) {
        return Err(invalid("Owned absence keys have different valid instants"));
    }
    Ok(Timestamp(valid))
}

fn point_identity(call: &Call, batch: &RecordBatch) -> Result<(EntityId, Arc<MemoryReservation>), Status> {
    point_identity_at(call, batch, 0)
}

fn point_identity_at(call: &Call, batch: &RecordBatch, row: usize) -> Result<(EntityId, Arc<MemoryReservation>), Status> {
    let (column, field) = batch.schema_ref().column_with_name("_id").ok_or_else(|| invalid("Owned point has no _id field"))?;
    let values = batch.column(column).as_ref();
    let identity = call.reserve(wire::add(4096, rtdb_columnar::estimate_read_value_bytes(field, values, row).map_err(status)?)?)?;
    let id = rtdb_columnar::read_execution_value(field, values, row).map_err(status)?.owned_entity_id().map_err(status)?;
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

fn hosted_control_row(
    relation: &OwnedRelation, scope: rtdb_types::PlacedAttemptScope, mut row: StructFields<Value>, require_checks: bool,
) -> Result<rtdb_tx::HostedControlWrite, Status> {
    use rtdb_tx::HostedControlWrite;

    strip_owned_absence(&mut row)?;
    let (_, expected_column, check_column) = owned_landing_wire::hosted_check_columns();
    let expected = match row.remove(expected_column) {
        Some(Value::I64(time)) => Some(time),
        Some(Value::Null) => None,
        _ => return Err(invalid("Hosted control expectation must be Int64 or null")),
    };
    let check_only = match row.remove(check_column) {
        Some(Value::Bool(check_only)) => check_only,
        None => false,
        _ => return Err(invalid("Hosted control check flag must be Boolean")),
    };
    match owned_landing_wire::hosted_control_command(expected, check_only, require_checks) {
        ControlCommand::Check(expected) if row.len() == 1 =>
            HostedControlWrite::check(relation.clone(), scope, expected.map(Timestamp), row),
        ControlCommand::Check(_) => return Err(invalid("Hosted control checks carry only the row identity")),
        ControlCommand::Replace(expected) => HostedControlWrite::new(relation.clone(), scope, expected.map(Timestamp), row),
        ControlCommand::Refuse => return Err(invalid("Read validation permits only hosted control checks")),
    }.map_err(status)
}

fn strip_owned_absence(row: &mut rtdb_types::StructFields<Value>) -> Result<(), Status> {
    let absent = match row.remove("_absent") {
        None | Some(Value::Null | Value::Undefined) => return Ok(()),
        Some(Value::List(fields)) if fields.len() <= 128 => fields,
        _ => return Err(invalid("Owned _absent must be a list of at most 128 field names")),
    };
    for field in absent.iter() {
        let Value::Utf8(name) = field.as_ref() else { return Err(invalid("Owned _absent field must be text")); };
        if !owned_landing_wire::absence_payload_column(name.as_bytes()) || !matches!(row.get(name.as_str()), Some(Value::Null | Value::Undefined)) {
            return Err(invalid("Owned _absent may name only distinct null payload cells"));
        }
        row.remove(name.as_str());
    }
    Ok(())
}

fn owned_point_expectation(history: Option<Timestamp>, position: Option<&[u8]>) -> Result<Option<rtdb_types::OwnedPointVersion>, Status> {
    match (history, position) {
        (None, None) => Ok(None),
        (Some(history), Some(bytes)) => {
            let bytes = bytes.try_into().map_err(|_| invalid("Owned observation has an invalid event position"))?;
            let position = rtdb_types::EventPosition::from_bytes(bytes);
            if position.transaction.0 < 0 { return Err(invalid("Invalid owned observation transaction")); }
            Ok(Some(rtdb_types::OwnedPointVersion { history, position }))
        }
        _ => Err(invalid("An owned expectation requires both its event position and history coordinate")),
    }
}

fn prepare_world_point<'a>(batch: &'a RecordBatch, memory: &Arc<MemoryBudget>)
    -> Result<(Vec<PointCell<'a>>, Arc<MemoryReservation>), Status> {
    let encoding_bytes = |bytes| wire::mul(bytes, 8).and_then(|size| wire::add(4096, size));
    let mut bytes = crate::WorldPointRow::document_header_bytes(batch).map_err(status)?;
    let mut cells = Vec::new();
    let mut reservation = memory.reserve(0).map_err(status)?;
    let lease = Arc::get_mut(&mut reservation).expect("Point preparation owns its reservation");
    let mut refused = false;
    for cell in crate::WorldPointRow::cells(batch) {
        let cell = PointCell::prepare(cell).map_err(status)?;
        bytes = bytes.checked_add(cell.heap_bytes()).ok_or_else(|| status(rtdb_types::OwnedWorld::unavailable()))?;
        if refused { continue; }
        match encoding_bytes(bytes).and_then(|size| {
            if size == lease.bytes() { Ok(()) } else { lease.grow_to(size).map_err(status) }
        }) {
            Ok(()) => cells.push(cell),
            Err(_) => {
                cells = Vec::new();
                lease.shrink_to(0).map_err(status)?;
                // Document reads precede reply admission, even if a prefix exceeds the budget.
                refused = true;
            }
        }
    }
    let bound = encoding_bytes(bytes)?;
    if refused {
        let reservation = memory.reserve(bound).map_err(status)?;
        let cells = crate::WorldPointRow::cells(batch).map(PointCell::prepare).collect::<rtdb_types::Result<_>>().map_err(status)?;
        return Ok((cells, reservation));
    }
    if lease.bytes() != bound { lease.grow_to(bound).map_err(status)?; }
    Ok((cells, reservation))
}

fn optional_micros(value: Option<Value>) -> Result<Option<Timestamp>, Status> {
    match value {
        Some(Value::I64(time)) => Ok(Some(Timestamp(time))),
        None | Some(Value::Null | Value::Undefined) => Ok(None),
        _ => Err(invalid("Owned observation requires signed microsecond integers")),
    }
}

fn replay_residue(
    relation: &OwnedRelation, id: EntityId, origin: Option<&[u8]>,
    from: Option<Timestamp>, to: Option<Timestamp>,
) -> Result<TxOp, Status> {
    let origin = origin.and_then(|bytes| bytes.try_into().ok())
        .map(rtdb_types::EventOrigin)
        .ok_or_else(|| invalid("An archive residue requires a 32-byte origin"))?;
    let (Some(from), Some(to)) = (from, to) else {
        return Err(invalid("An archive residue requires both original bounds"));
    };
    let valid = Interval::new(from, to).map_err(status)?;
    if valid.is_empty() {
        return Err(invalid("An archive residue has an empty extent"));
    }
    Ok(TxOp::OwnedReplayResidue { relation: relation.clone(), id, origin, valid })
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
mod spatial_reply_tests {
    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use arrow_schema::Schema;
    use rtdb_columnar::{EntityIdRef, OwnedBatch, entity_id_array, entity_id_type, read_owned_entity_id};
    use rtdb_types::MemoryBudget;

    use super::spatial_reply_ids;

    #[test]
    fn canonical_spatial_ids_match_relayout_and_keep_the_page_funding() {
        let memory = MemoryBudget::new(1 << 20);
        let field = rtdb_columnar::to_field("_id", &entity_id_type()).unwrap();
        let expected = [EntityIdRef::Binary(b"same\0\xff"), EntityIdRef::String("same")];
        let ids = entity_id_array([EntityIdRef::String("skipped")].into_iter().chain(expected)).unwrap();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field.clone()])), vec![ids]).unwrap();
        let rows = OwnedBatch::new(batch.slice(1, expected.len()), &memory).unwrap();
        drop(batch);
        let page_bytes = memory.used_bytes();

        let (ids, scratch) = spatial_reply_ids(&memory, &rows, &field).unwrap();
        let source = rows.batch().column(0);
        assert!(Arc::ptr_eq(&ids, source));
        assert_eq!(memory.used_bytes(), page_bytes + 4096);
        {
            let (_, baseline) = rtdb_columnar::relayout_arrow_column(Arc::clone(source), &field, &field).unwrap();
            assert_eq!(ids.data_type(), baseline.data_type());
            for (row, expected) in expected.into_iter().enumerate() {
                assert_eq!(read_owned_entity_id(&field, baseline.as_ref(), row).unwrap(), expected);
                assert_eq!(read_owned_entity_id(&field, ids.as_ref(), row).unwrap(), expected);
            }
        }

        drop(rows);
        drop(scratch);
        assert_eq!(memory.used_bytes(), page_bytes);
        drop(ids);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[cfg(test)]
mod protected_carrier_wire_tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, BinaryArray, RecordBatch, StringArray};
    use rtdb_columnar::{EntityIdRef, read_owned_entity_id};
    use rtdb_types::{EntityId, SharedColumnCarrierPrefix};

    use super::{protected_carrier_prefix, shared_column_census_rows, shared_column_scopes};

    #[test]
    fn protected_prefix_frames_require_raw_binary_and_keep_the_world_prefix_text() {
        let mut prefix = [0xff; SharedColumnCarrierPrefix::WIDTH];
        prefix[..6].copy_from_slice(b"BFCP2\0");
        let world = format!("world-column/{}/", "01".repeat(32));
        let frame = |prefix: &[u8]| [(prefix.len() as u16).to_be_bytes().as_slice(), prefix, world.as_bytes()].concat();
        let body = frame(&prefix);
        let (root, overlay) = shared_column_scopes(&body).unwrap();
        assert_eq!((root.as_bytes(), overlay), (prefix.as_slice(), world.as_str()));
        let hex = prefix.iter().map(|byte| format!("{byte:02x}")).collect::<Vec<_>>().concat();
        assert!(shared_column_scopes(&frame(hex.as_bytes())).is_err());
        assert!(shared_column_scopes(&frame(&prefix[..prefix.len() - 1])).is_err());

        let raw = RecordBatch::try_from_iter([("prefix", Arc::new(BinaryArray::from_iter_values([prefix.as_slice()])) as ArrayRef)]).unwrap();
        let text = RecordBatch::try_from_iter([("prefix", Arc::new(StringArray::from(vec![hex])) as ArrayRef)]).unwrap();
        let missing = RecordBatch::try_from_iter([("prefix", Arc::new(BinaryArray::from(vec![None::<&[u8]>])) as ArrayRef)]).unwrap();
        assert_eq!(protected_carrier_prefix(&raw).unwrap().as_bytes(), prefix.as_slice());
        assert!(protected_carrier_prefix(&text).is_err());
        assert!(protected_carrier_prefix(&missing).is_err());
    }

    #[test]
    fn census_replies_retain_the_binary_and_text_identity_legs_even_when_empty() {
        let mut raw = vec![0xff; SharedColumnCarrierPrefix::ID_WIDTH];
        raw[..6].copy_from_slice(b"BFCP2\0");
        let world = format!("world-column/{}/{}", "01".repeat(32), "02".repeat(32));
        let ids = [EntityId::Binary(raw.clone()), EntityId::String(world.clone())];
        let rows = shared_column_census_rows(&ids).unwrap();
        assert_eq!(shared_column_census_rows(&[]).unwrap().schema(), rows.schema());
        let field = rows.schema_ref().field(0);
        let column = rows.column(0).as_ref();
        assert_eq!(read_owned_entity_id(field, column, 0).unwrap(), EntityIdRef::Binary(&raw));
        assert_eq!(read_owned_entity_id(field, column, 1).unwrap(), EntityIdRef::String(&world));
    }
}

#[cfg(test)]
mod world_point_tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_ipc::writer::IpcWriteOptions;
    use rtdb_columnar::to_field;
    use rtdb_format::ipc::write_arrow_stream;
    use rtdb_types::{ListKind, LogicalType, ScalarType};

    fn world_point_cell(cell: ArrowCell<'_>) -> rtdb_types::Result<(Field, ArrayRef)> {
        PointCell::prepare(cell)?.write(cell.field.name())
    }

    #[test]
    fn world_point_narrows_to_the_selected_leg_and_retains_its_payload() {
        let logical = LogicalType::from_legs([LogicalType::Null,
            LogicalType::Scalar(ScalarType::Utf8), LogicalType::Scalar(ScalarType::F64)]);
        let field = rtdb_columnar::to_field("payload", &logical).unwrap();
        let bits = 0x7ff8_0000_0000_0013;
        let values = [Value::Utf8("earlier".into()), Value::F64(f64::from_bits(bits)),
            Value::Null, Value::Utf8("latest".into())];
        let array = rtdb_columnar::build_array(&logical, &values).unwrap();
        for (row, value) in values.into_iter().enumerate().skip(1) {
            let cell = ArrowCell { field: &field, array: array.as_ref(), row };
            let active = cell.physical().unwrap();
            let (actual_field, actual) = world_point_cell(cell).unwrap();
            let expected_type = value.logical_type();
            assert_eq!(actual_field, rtdb_columnar::to_field("payload", &expected_type).unwrap());
            if matches!(value, Value::Utf8(_)) {
                assert_eq!(actual.as_string::<i32>().value(0).as_ptr(), active.array.as_string::<i32>().value(active.row).as_ptr());
            }
            if matches!(value, Value::F64(_)) {
                assert_eq!(actual.as_primitive::<arrow_array::types::Float64Type>().value(0).to_bits(), bits);
            }
            let schema = Arc::new(Schema::new(vec![actual_field]));
            let expected = rtdb_columnar::build_array(&expected_type, std::slice::from_ref(&value)).unwrap();
            let actual = RecordBatch::try_new(schema.clone(), vec![actual]).unwrap();
            let expected = RecordBatch::try_new(schema.clone(), vec![expected]).unwrap();
            let document = std::iter::once(Ok(("payload".into(), value))).collect::<rtdb_types::Result<StructFields<_>>>().unwrap();
            assert_eq!(crate::WorldPointRow::document_bytes(&actual).unwrap(), Value::document_retained_bytes(&document).unwrap());
            let ipc = |batch: &RecordBatch| {
                write_arrow_stream(&schema, std::slice::from_ref(batch), IpcWriteOptions::default()).unwrap()
            };
            assert_eq!(ipc(&actual), ipc(&expected));
        }
        let nullable = LogicalType::Scalar(ScalarType::Utf8).maybe();
        let field = rtdb_columnar::to_field("payload", &nullable).unwrap();
        let array = Arc::new(StringArray::from(vec![None, Some("selected")]));
        let (field, narrowed) = world_point_cell(ArrowCell { field: &field, array: array.as_ref(), row: 1 }).unwrap();
        assert!(!field.is_nullable());
        assert!(narrowed.nulls().is_none());
        assert_eq!(narrowed.as_string::<i32>().value(0).as_ptr(), array.value(1).as_ptr());
    }

    #[test]
    fn world_point_sizes_and_writes_concrete_nested_fields() {
        let f32_list = LogicalType::List {
            kind: ListKind::List, element: Box::new(LogicalType::Scalar(ScalarType::F32)),
        };
        let f64_list = LogicalType::List {
            kind: ListKind::FixedSizeList(2), element: Box::new(LogicalType::Scalar(ScalarType::F64)),
        };
        let logical = LogicalType::Struct(StructFields::from([
            ("z_label".into(), LogicalType::Scalar(ScalarType::Utf8)),
            ("absent".into(), LogicalType::Nothing),
            ("null".into(), LogicalType::Null),
            ("variable".into(), f32_list),
            ("fixed".into(), f64_list),
        ]));
        let wider = LogicalType::from_legs([logical.clone(), LogicalType::Scalar(ScalarType::I64)]);
        let field = rtdb_columnar::to_field("payload", &wider).unwrap();
        let value = |label: &str, f32s: Vec<Value>, f64s: Vec<Value>| Value::Struct(StructFields::from([
            ("z_label".into(), Value::Utf8(label.into())),
            ("absent".into(), Value::Undefined),
            ("null".into(), Value::Null),
            ("variable".into(), Value::list(f32s)),
            ("fixed".into(), Value::FixedSizeList(f64s.into())),
        ]));
        let values = [value("earlier", vec![Value::F32(2.0)], vec![Value::F64(4.0), Value::F64(8.0)]),
            Value::I64(9), value("selected",
                vec![Value::F32(f32::from_bits(0x7fc0_0042)), Value::F32(-0.0)],
                vec![Value::F64(f64::from_bits(0x7ff8_0000_0000_0042)), Value::F64(-0.0)])];
        let input = RecordBatch::try_new(Arc::new(Schema::new(vec![field.clone()])),
            vec![rtdb_columnar::build_array(&wider, &values).unwrap()]).unwrap().slice(2, 1);
        let cell = ArrowCell::named(&input, "payload", 0).unwrap();
        let baseline = rtdb_columnar::read_value(cell.field, cell.array, cell.row).unwrap();
        let document = StructFields::from([("payload".into(), baseline.clone())]);
        assert_eq!(crate::WorldPointRow::document_bytes(&input).unwrap(), Value::document_retained_bytes(&document).unwrap());

        let (field, narrowed) = world_point_cell(cell).unwrap();
        assert_eq!(field, rtdb_columnar::to_field("payload", &baseline.logical_type()).unwrap());
        let schema = Arc::new(Schema::new(vec![field]));
        let baseline = rtdb_columnar::build_array(&baseline.logical_type(), &[baseline]).unwrap();
        assert!(narrowed.get_array_memory_size() <= baseline.get_array_memory_size());
        let ipc = |array| {
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
            write_arrow_stream(&schema, &[batch], IpcWriteOptions::default()).unwrap()
        };
        assert_eq!(ipc(narrowed), ipc(baseline));
    }

    #[test]
    fn world_point_retains_narrowing_and_validation_for_other_nested_layouts() {
        let logical = LogicalType::Struct(StructFields::from([
            ("nullable".into(), LogicalType::Scalar(ScalarType::Utf8).maybe()),
        ]));
        let field = rtdb_columnar::to_field("payload", &logical).unwrap();
        let value = Value::Struct(StructFields::from([("nullable".into(), Value::Utf8("present".into()))]));
        let array = rtdb_columnar::build_array(&logical, &[value.clone()]).unwrap();
        let (field, _) = world_point_cell(ArrowCell { field: &field, array: array.as_ref(), row: 0 }).unwrap();
        assert_eq!(field, rtdb_columnar::to_field("payload", &value.logical_type()).unwrap());

        let uri = Arc::new(rtdb_columnar::to_field("uri", &LogicalType::Scalar(ScalarType::Uri)).unwrap());
        let array = arrow_array::StructArray::new(vec![uri].into(), vec![Arc::new(StringArray::from(vec!["not a URI"]))], None);
        let field = Field::new("payload", array.data_type().clone(), false);
        let baseline = rtdb_columnar::read_value(&field, &array, 0).unwrap_err();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![Arc::new(array)]).unwrap();
        assert_eq!(crate::WorldPointRow::document_bytes(&batch).unwrap_err(), baseline);
        assert_eq!(world_point_cell(ArrowCell::named(&batch, "payload", 0).unwrap()).unwrap_err(), baseline);
    }

    #[test]
    fn world_point_keeps_the_exact_document_charge_for_mixed_children() {
        let text = LogicalType::Scalar(ScalarType::Utf8);
        let logical = LogicalType::Struct(StructFields::from([
            ("nullable".into(), text.clone().maybe()),
            ("omitted".into(), LogicalType::Nothing),
            ("explicit_null".into(), LogicalType::Null),
            ("set".into(), LogicalType::List { kind: ListKind::Set, element: Box::new(text) }),
        ]));
        let value = Value::Struct(StructFields::from([
            ("nullable".into(), Value::Utf8("present".into())),
            ("omitted".into(), Value::Undefined),
            ("explicit_null".into(), Value::Null),
            ("set".into(), Value::Set(vec![Value::Utf8("first".into()), Value::Utf8("second".into())])),
        ]));
        let field = to_field("payload", &logical).unwrap();
        let array = rtdb_columnar::build_array(&logical, std::slice::from_ref(&value)).unwrap();
        let baseline = rtdb_columnar::read_value(&field, array.as_ref(), 0).unwrap();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap();
        let document = StructFields::from([("payload".into(), baseline)]);
        let expected_bytes = Value::document_retained_bytes(&document).unwrap();
        assert_eq!(crate::WorldPointRow::document_bytes(&batch).unwrap(), expected_bytes);
        let memory = MemoryBudget::new(1 << 20);
        let (mut cells, charge) = prepare_world_point(&batch, &memory).unwrap();
        assert_eq!(charge.bytes(), 4096 + 8 * expected_bytes);
        let (field, array) = cells.pop().unwrap().write("payload").unwrap();
        assert_eq!(field, to_field("payload", &value.logical_type()).unwrap());
        assert_eq!(rtdb_columnar::read_value(&field, array.as_ref(), 0).unwrap(), value);
        drop((field, array, charge));
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn world_point_writes_empty_collections_and_timestamp_ranges_at_the_original_charge() {
        let text = LogicalType::Scalar(ScalarType::Utf8);
        let set_type = LogicalType::List { kind: ListKind::Set, element: Box::new(text.clone()) };
        let map_type = Value::Map {
            entries: vec![(Value::Utf8("elsewhere".into()), Value::I64(123))], sorted: true,
        }.logical_type();
        let range = Value::TimestampTzRange { lower: -7, upper: 13 };
        let values = [Value::Set(vec![]), Value::Map { entries: vec![], sorted: true }, range.clone()];
        let source = [
            ("s", set_type, Value::Set(vec![Value::Utf8("elsewhere".into())])),
            ("m", map_type, Value::Map { entries: vec![(Value::Utf8("elsewhere".into()), Value::I64(123))], sorted: true }),
            ("r", range.logical_type(), Value::TimestampTzRange { lower: -11, upper: -10 }),
        ];
        let columns = source.into_iter().zip(&values).map(|((name, logical, earlier), selected)| {
            let field = to_field(name, &logical).unwrap();
            let array = rtdb_columnar::build_array(&logical, &[earlier, selected.clone()]).unwrap();
            (field, array)
        }).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(columns.iter().map(|(field, _)| field.clone()).collect::<Vec<_>>())),
            columns.into_iter().map(|(_, array)| array).collect()).unwrap().slice(1, 1);
        let original = crate::WorldPointRow::cells(&batch).map(|cell| {
            (cell.field.name().clone(), rtdb_columnar::read_value(cell.field, cell.array, cell.row).unwrap())
        }).collect::<StructFields<_>>();
        let bytes = Value::document_retained_bytes(&original).unwrap();
        let memory = MemoryBudget::new(4096 + 8 * bytes);
        let (planned, charge) = prepare_world_point(&batch, &memory).unwrap();
        assert_eq!(charge.bytes(), 4096 + 8 * bytes);
        assert_eq!(crate::WorldPointRow::document_bytes(&batch).unwrap(), bytes);
        for (point, (name, expected)) in planned.into_iter().zip(original.iter()) {
            let (field, actual) = point.write(name).unwrap();
            let logical = expected.logical_type();
            assert_eq!(field, to_field(name, &logical).unwrap());
            let expected = rtdb_columnar::build_array_refs(&logical, &[expected]).unwrap();
            let schema = Arc::new(Schema::new(vec![field]));
            let ipc = |array| {
                let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
                write_arrow_stream(&schema, &[batch], IpcWriteOptions::default()).unwrap()
            };
            assert_eq!(ipc(actual), ipc(expected));
        }
        drop(charge);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn world_point_canonicalizes_other_timestamp_ranges_and_reaches_null_endpoint_errors_before_admission() {
        use arrow_array::{FixedSizeListArray, TimestampMicrosecondArray};

        let endpoint = LogicalType::Scalar(ScalarType::Timestamp {
            unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("Europe/Paris".into()),
        }).maybe();
        let logical = LogicalType::List { kind: ListKind::TimestampTzRange, element: Box::new(endpoint) };
        let field = to_field("r", &logical).unwrap();
        let DataType::FixedSizeList(item, width) = field.data_type() else { unreachable!(); };
        let array = Arc::new(FixedSizeListArray::new(Arc::clone(item), *width,
            Arc::new(TimestampMicrosecondArray::from(vec![Some(-7), Some(13), Some(-7), None]).with_timezone("Europe/Paris")), None));
        let (canonical, output) = world_point_cell(ArrowCell { field: &field, array: array.as_ref(), row: 0 }).unwrap();
        let expected = Value::TimestampTzRange { lower: -7, upper: 13 };
        assert_eq!(canonical, to_field("r", &expected.logical_type()).unwrap());
        assert_eq!(rtdb_columnar::read_value(&canonical, output.as_ref(), 0).unwrap(), expected);
        let expected = rtdb_columnar::read_value(&field, array.as_ref(), 1).unwrap_err();
        assert_eq!(expected.code, "rtdb/timestamp-range");
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![array]).unwrap().slice(1, 1);
        let error = prepare_world_point(&batch, &MemoryBudget::new(1)).err().unwrap();
        assert_eq!(error.message(), status(expected).message());
    }

    #[test]
    fn world_point_validates_only_the_selected_nested_decimal() {
        use arrow_array::Decimal128Array;

        let logical = LogicalType::Scalar(ScalarType::Decimal { precision: 3, scale: 2, bit_width: 128 });
        let amount = Arc::new(to_field("amount", &logical).unwrap());
        let DataType::Decimal128(precision, scale) = *amount.data_type() else { unreachable!() };
        let cents = Decimal128Array::from(vec![1234, -125]).with_precision_and_scale(precision, scale).unwrap();
        let array = StructArray::new(vec![amount].into(), vec![Arc::new(cents)], None);
        let field = Field::new("payload", array.data_type().clone(), false);
        let read = |row| ArrowCell { field: &field, array: &array, row };
        let invalid = rtdb_columnar::read_value(&field, &array, 0).unwrap();
        let expected = rtdb_columnar::build_array(&invalid.logical_type(), &[invalid]).unwrap_err();
        assert_eq!(world_point_cell(read(0)).unwrap_err(), expected);
        let (actual, narrowed) = world_point_cell(read(1)).unwrap();
        assert_eq!(actual, field);
        assert_eq!(rtdb_columnar::read_value(&actual, narrowed.as_ref(), 0).unwrap(), rtdb_columnar::read_value(&field, &array, 1).unwrap());
    }

    #[test]
    fn world_point_reads_later_fields_before_reporting_a_reply_budget_refusal() {
        let uri = Arc::new(to_field("uri", &LogicalType::Scalar(ScalarType::Uri)).unwrap());
        let first = Arc::new(StringArray::from(vec!["first"]));
        let second = Arc::new(StructArray::new(vec![uri].into(), vec![Arc::new(StringArray::from(vec!["not a URI"]))], None));
        let field = Field::new("payload", second.data_type().clone(), false);
        let expected = rtdb_columnar::read_value(&field, second.as_ref(), 0).unwrap_err();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(vec![Field::new("prefix", DataType::Utf8, false), field])), vec![first, second]).unwrap();
        let memory = MemoryBudget::new(1);
        let error = prepare_world_point(&batch, &memory).err().unwrap();
        assert_eq!(error.message(), status(expected).message());
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn world_point_budget_refusal_uses_the_complete_document_charge() {
        let first = Arc::new(StringArray::from(vec!["first"]));
        let long = "x".repeat(8192);
        let second = Arc::new(StringArray::from(vec![long.as_str()]));
        let schema = Schema::new(vec![Field::new("prefix", DataType::Utf8, false), Field::new("payload", DataType::Utf8, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![first, second]).unwrap();
        let memory = MemoryBudget::new(50_000);
        let bytes = crate::WorldPointRow::document_bytes(&batch).unwrap();
        let expected = status(memory.reserve(4096 + 8 * bytes).unwrap_err());
        let error = prepare_world_point(&batch, &memory).err().unwrap();
        assert_eq!((error.code(), error.message()), (expected.code(), expected.message()));
        assert_eq!(memory.used_bytes(), 0);
    }
}
