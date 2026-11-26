mod owned_batch;

use std::{collections::BTreeMap, sync::Arc};
use arrow_flight::{Action, Result as ActionResult, sql::{Any, ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, ActionEndTransactionRequest, EndTransaction, ProstMessageExt}};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use prost::Message;
use rtdb_node::LastSubmittedTx;
use tonic::{Response, Status};
use uuid::Uuid;
pub(crate) use owned_batch::{IpcFrames, OWNED_IPC_HEADER, OwnedIpcReply, SelectionEncoder};

use crate::{error::{arrow, cancelled, invalid, status, unsupported}, lease::{Leased, Lookup}, protocol, service::{CLOSE_SESSION, Call, Output, Service, found, action_result_stream}, state::{ConnectionResource, Prepared, PreparedScope, Transaction}, ticket, wire};

enum ActionReplies {
    Results(Vec<Result<ActionResult, Status>>),
    Owned(OwnedIpcReply),
}

fn fresh_replica_action(action: &str) -> bool {
    use crate::owned_actions::*;
    matches!(action, CLOSE_SESSION
        | owned_batch::READ_POINTS
        | owned_batch::READ_SELECTION
        | owned_batch::READ_AUDIENCE_POINTS
        | owned_batch::READ_RANGES
        | OWNED_SPATIAL_READINESS_V1
        | OBSERVE_HELD_HOSTED_V1
        | CONFIRM_APPLIED_HOSTED_V6
        | READ_HELD_HOSTED_CONTROL_V3
        | READ_HELD_HOSTED_CONTROL_AT_PUBLICATION_V1
        | READ_HELD_ROOT_CATALOG_V1
        | OPEN_OWNED_SCOPED_READ_V2
        | OPEN_OWNED_READ_AS_OF_V2
        | OPEN_OWNED_READ_AT_PUBLICATION_V1
        | SEAL_HOSTED_RANGES_V1
        | VALIDATE_FRESH_HOSTED_V1
        | OPEN_FRESH_OWNED_READ_V2
        | SPATIAL_OPEN_OWNED
        | SPATIAL_PROBE_OWNED
        | SPATIAL_GATHER_OWNED
        | SPATIAL_RANK_NEXT_OWNED
        | SPATIAL_NEXT_OWNED
        | SPATIAL_RANK_OPEN_OWNED
        | SPATIAL_RANK_OPEN_OWNED_V2
        | SPATIAL_CLOSE_OWNED
        | SPATIAL_HELD_OWNED_V1
        | SPATIAL_HELD_OWNED_V2
        | SPATIAL_HELD_OWNED_V3
        | SEARCH_HELD_OWNED_V7
        | NEAREST_HELD_OWNED_V4
        | HYBRID_HELD_OWNED_V4
        | HELD_OWNED_RANGE_V1
        | HELD_OWNED_RANGE_V2
        | HELD_OWNED_RANGE_V3
        | HELD_OWNED_RANGE_V4
        | HELD_OWNED_COUNT_V1
        | HELD_OWNED_COUNT_V2
        | HELD_OWNED_MEASURE_V1
        | HELD_OWNED_MEASURE_AT_V1
        | HELD_OWNED_STORAGE_USAGE_V1
        | HELD_OWNED_MERGED_CHANGES_DESCENDING_V2
        | HELD_OWNED_MERGED_CHANGES_V4
        | HELD_TABLE_PUBLICATION_SEQUENCE_V1
        | HELD_LOOKUP_PUBLICATION_V1
        | HELD_OWNED_CHANGES_V3
        | READ_HELD_UNIQUE_V1
        | READ_HELD_UNIQUE_WINDOW_V1
        | HELD_OWNED_POINT_V1
        | HELD_OWNED_POINT_V2
        | HELD_OWNED_ABSENT_KEYS_V1
        | HELD_OWNED_FIXED_POINT_V1
        | HELD_PROTECTED_COVERAGE_V2
        | HELD_OWNED_IDENTITIES_V1
        | HELD_OWNED_IDENTITIES_ALL_VALID_V1
        | HELD_OWNED_VERSIONS_V2
        | HELD_OWNED_PATH_TOUCH_V2
        | HELD_OWNED_HISTORICAL_POINT_V1
        | HELD_OWNED_POSITION_POINT_V1
        | HELD_ROOT_CUT_POINT_V1
        | HELD_OWNED_VALID_DURING_V2
        | HELD_OWNED_VALID_REGION_V1
        | HELD_OWNED_POSITION_VALID_DURING_V1
        | CLOSE_OWNED_READ_V1)
}

pub fn classic_retained_action(action: &str) -> bool {
    use crate::owned_actions::*;
    matches!(action, OPEN_FRESH_OWNED_READ_V2 | CLOSE_OWNED_READ_V1
        | OPEN_OWNED_SCOPED_READ_V2 | OPEN_OWNED_READ_AS_OF_V2 | OPEN_OWNED_READ_AT_PUBLICATION_V1
        | READ_HELD_ROOT_CATALOG_V1 | READ_HELD_HOSTED_CONTROL_V3 | READ_HELD_HOSTED_CONTROL_AT_PUBLICATION_V1
        | owned_batch::READ_POINTS | owned_batch::READ_SELECTION
        | HELD_OWNED_POINT_V1 | HELD_OWNED_POINT_V2 | HELD_OWNED_FIXED_POINT_V1
        | HELD_OWNED_POSITION_POINT_V1 | HELD_OWNED_HISTORICAL_POINT_V1 | HELD_ROOT_CUT_POINT_V1
        | HELD_OWNED_RANGE_V1 | HELD_OWNED_RANGE_V2 | HELD_OWNED_RANGE_V3 | HELD_OWNED_RANGE_V4
        | HELD_TABLE_PUBLICATION_SEQUENCE_V1 | HELD_LOOKUP_PUBLICATION_V1)
}

#[cfg(test)]
#[test]
fn classic_retained_grants_admit_only_the_selected_owned_read_surface() {
    use crate::owned_actions::*;
    assert!(classic_retained_action(OPEN_FRESH_OWNED_READ_V2));
    assert!(classic_retained_action(READ_HELD_ROOT_CATALOG_V1));
    assert!(classic_retained_action(CLOSE_OWNED_READ_V1));
    assert!(!classic_retained_action(COMMIT_OWNED_V4));
    assert!(!classic_retained_action(VALIDATE_FRESH_HOSTED_V1));
    assert!(!classic_retained_action(CLOSE_SESSION));
    assert!(!classic_retained_action("rtdb.v1.Query"));
}

impl Service {
    async fn confirm_audience_observation(&self, call: &Call) -> Result<(), Status> {
        let source = call.session.audience_source().ok_or_else(unsupported)?;
        let (_, attachment) = call.session.resolve_database(Some(&call.database), &self.0.node)?;
        let attachment = attachment.ok_or_else(unsupported)?;
        self.0.node.confirm_held_audience_route(&attachment, source).await.map_err(status)?;
        Ok(())
    }

    pub async fn action(&self, call: Call, action: Action, ipc_stream: bool) -> Result<Response<Output<ActionResult>>, Status> {
        use crate::config::{AUDIENCE_READ_LIMITS, AUDIENCE_REPLY_BYTES, AUDIENCE_TURN_ACTIONS, AUDIENCE_TURN_LIMITS};
        use crate::owned_actions::{CLOSE_OWNED_READ_V1, CONFIRM_APPLIED_HOSTED_V6, HELD_OWNED_POINT_V2, HELD_OWNED_RANGE_V2, HELD_TABLE_PUBLICATION_SEQUENCE_V1, OPEN_APPLIED_OWNED_READ_V5, PREPARE_APPLIED_READ_V3, READ_HELD_HOSTED_CONTROL_V3};
        use rtdb_storage::audience_work;
        if call.session.fresh_replica && !fresh_replica_action(&action.r#type) {
            return Err(call.error(Status::permission_denied("The fresh Flight route only admits reads and release of their holds")));
        }
        if let Some(batch) = owned_batch::BatchKind::for_action(&action.r#type) {
            if call.session.audience_source().is_some() != batch.audience() {
                return Err(call.error(Status::permission_denied("The reserved Flight session only admits audience observations")));
            }
            if !self.0.config.owned_native_actions {
                return Err(call.error(unsupported()));
            }
            return rtdb_types::boxed(|| self.held_read_batch(call, batch, action.body)).await;
        }
        let ipc_stream = ipc_stream && action.r#type.starts_with("rtdb.owned.")
            && call.session.audience_source().is_none();
        let turn = if call.session.audience_source().is_some() {
            let admit = || -> Result<_, Status> {
                Ok(match action.r#type.as_str() {
                    PREPARE_APPLIED_READ_V3 => call.session.audience_recovery_work(),
                    OPEN_APPLIED_OWNED_READ_V5 => {
                        let generation = self.audience_open_generation(&action.body)?;
                        call.session.audience_projection()?;
                        if generation == 1 {
                            call.session.audience_recovery_work()
                        } else {
                            Some(audience_work::TurnAllowance::new(AUDIENCE_TURN_LIMITS, AUDIENCE_TURN_ACTIONS))
                        }
                    },
                    HELD_OWNED_POINT_V2 | HELD_OWNED_RANGE_V2 | HELD_TABLE_PUBLICATION_SEQUENCE_V1 | READ_HELD_HOSTED_CONTROL_V3 | CONFIRM_APPLIED_HOSTED_V6 =>
                        Some(self.audience_read_work(&call, &action.r#type, &action.body)?),
                    _ => None,
                })
            };
            admit().map_err(|error| call.error(error))?
        } else { None };
        if let Some(turn) = &turn {
            turn.admit_action().map_err(|error| call.error(Status::resource_exhausted(error.to_string())))?;
        }
        let work = Box::pin(async {
            let action_type = action.r#type.as_str();
            if call.session.audience_source().is_some() {
                audience_work::decode_bytes(action.encoded_len())
                    .map_err(|error| Status::resource_exhausted(error.to_string()))?;
                match action_type {
                    // Catalog acquisition checks the placement and quorum at the captured cut.
                    PREPARE_APPLIED_READ_V3 | OPEN_APPLIED_OWNED_READ_V5 | CONFIRM_APPLIED_HOSTED_V6 => {}
                    HELD_OWNED_POINT_V2 | HELD_OWNED_RANGE_V2 | HELD_TABLE_PUBLICATION_SEQUENCE_V1 | READ_HELD_HOSTED_CONTROL_V3 => {
                        rtdb_types::boxed(|| self.confirm_audience_observation(&call)).await?;
                    }
                    // These actions only relinquish resources under the authenticated local session.
                    CLOSE_OWNED_READ_V1 | CLOSE_SESSION => {}
                    _ => return Err(Status::permission_denied("The reserved Flight session only admits audience observations")),
                }
            }
            use crate::owned_actions::{
                COMMIT_ORDERED_V1, COMMIT_OWNED_V1, COMMIT_OWNED_V3, COMMIT_OWNED_V4,
                CommitFormat, OwnedReplyFormat, PointTime, SPATIAL_HELD_OWNED_V1, SPATIAL_HELD_OWNED_V2, SPATIAL_HELD_OWNED_V3,
                SPATIAL_OPEN_OWNED, SPATIAL_PROBE_OWNED, SPATIAL_RANK_OPEN_OWNED, SPATIAL_RANK_OPEN_OWNED_V2, SpatialHeldWire,
                WorldHistoricalRead::{Current, EarlierEffective, EarlierParent},
            };
            // Sharing the await site keeps duplicate handler futures off the worker stack.
            let spatial_wire = match action_type {
                SPATIAL_OPEN_OWNED => Some(SpatialHeldWire::Open),
                SPATIAL_PROBE_OWNED => Some(SpatialHeldWire::Probe),
                SPATIAL_RANK_OPEN_OWNED => Some(SpatialHeldWire::RankOpen),
                SPATIAL_RANK_OPEN_OWNED_V2 => Some(SpatialHeldWire::RankWindow),
                SPATIAL_HELD_OWNED_V1 => Some(SpatialHeldWire::V1),
                SPATIAL_HELD_OWNED_V2 => Some(SpatialHeldWire::V2),
                SPATIAL_HELD_OWNED_V3 => Some(SpatialHeldWire::V3),
                _ => None,
            };
            if let Some(wire) = spatial_wire {
                return rtdb_types::boxed(|| self.spatial_held_owned_action(&call, &action.body, wire, ipc_stream)).await.map(ActionReplies::Owned);
            }
            if crate::owned_family::is_family_action(action_type) {
                return Ok(ActionReplies::Results(vec![Ok(rtdb_types::boxed(|| self.assertion_family_action(&call, action_type, &action.body)).await?)]));
            }
            let reply_format = if ipc_stream { OwnedReplyFormat::StreamV1 } else { OwnedReplyFormat::Unary };
            let results = match action_type {
                crate::owned_actions::SEAL_HOSTED_RANGES_V1 => Ok(vec![Ok(rtdb_types::boxed(|| self.seal_hosted_ranges_action(&call, &action.body)).await?)]),
                crate::owned_actions::VALIDATE_FRESH_HOSTED_V1 => Ok(vec![Ok(rtdb_types::boxed(|| self.validate_fresh_hosted_action(&call, &action.body)).await?)]),
                crate::owned_actions::DECIDE_HOSTED_V4 | crate::owned_actions::DECIDE_HOSTED_IMPORT_PAGE_V1
                | crate::owned_actions::DECIDE_HOSTED_PACKED_V1 => {
                    use crate::owned_actions::HostedDecisionWire;
                    let wire = match action_type {
                        crate::owned_actions::DECIDE_HOSTED_V4 => HostedDecisionWire::Original,
                        crate::owned_actions::DECIDE_HOSTED_IMPORT_PAGE_V1 => HostedDecisionWire::ImportPage,
                        _ => HostedDecisionWire::Packed,
                    };
                    return rtdb_types::boxed(|| self.decide_hosted_action(&call, &action.body, wire, ipc_stream)).await.map(ActionReplies::Owned);
                }
                crate::owned_actions::DECLARE_OWNED_FIXED_RANGE_V1 =>
                    return rtdb_types::boxed(|| self.declare_owned_fixed_range_action(&call, &action.body, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::DECLARE_OWNED_CAPACITY_V2 =>
                    return rtdb_types::boxed(|| self.declare_owned_capacity_action(&call, &action.body, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::DECLARE_OWNED_UNIQUE_PATH_V1 =>
                    return rtdb_types::boxed(|| self.declare_owned_single_column_path_action(&call, &action.body, true, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::DECLARE_OWNED_SEARCH_PATH_V2 =>
                    return rtdb_types::boxed(|| self.declare_owned_single_column_path_action(&call, &action.body, false, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::DECLARE_OWNED_VECTOR_PATH_V1 =>
                    return rtdb_types::boxed(|| self.declare_owned_vector_path_action(&call, &action.body, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::OWNED_SHARED_SEARCH_TEXT_V1 => Ok(vec![Ok(self.owned_shared_search_text_action(&action.body)?)]),
                crate::owned_actions::OWNED_LAYOUT_CAPABILITIES_V1 => Ok(vec![Ok(self.owned_layout_capabilities_action(&action.body)?)]),
                crate::owned_actions::OWNED_RELATION_LAYOUT_V1 => Ok(vec![Ok(Box::pin(self.owned_relation_layout_action(&call, &action.body)).await?)]),
                crate::owned_actions::DECLARE_OWNED_RELATION_V1 | crate::owned_actions::DECLARE_OWNED_RELATION_V2 => {
                    let v2 = action_type == crate::owned_actions::DECLARE_OWNED_RELATION_V2;
                    return rtdb_types::boxed(|| self.declare_owned_relation_action(&call, &action.body, v2, ipc_stream)).await.map(ActionReplies::Owned);
                }
                crate::owned_actions::DROP_OWNED_SPATIAL_PATH_V1 =>
                    return rtdb_types::boxed(|| self.drop_owned_spatial_path_action(&call, &action.body, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::DROP_OWNED_SPATIAL_PATH_V2 =>
                    return rtdb_types::boxed(|| self.drop_owned_spatial_path_batch_action(&call, action.body, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::DECLARE_OWNED_SPATIAL_PATH_V1 =>
                    return rtdb_types::boxed(|| self.declare_owned_path_action(&call, &action.body, crate::owned_actions::PathDeclarationWire::TaggedSpatial, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::DECLARE_OWNED_SPATIAL_PATH_V2 =>
                    return rtdb_types::boxed(|| self.declare_owned_path_action(&call, &action.body, crate::owned_actions::PathDeclarationWire::EncodedSpatial, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::OWNED_SPATIAL_READINESS_V1 =>
                    return rtdb_types::boxed(|| self.spatial_path_readiness_action(&call, &action.body, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::DECLARE_OWNED_PATH_V1 =>
                    return rtdb_types::boxed(|| self.declare_owned_path_action(&call, &action.body, crate::owned_actions::PathDeclarationWire::Ordered, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::CREATE_OWNED_V1 =>
                    return rtdb_types::boxed(|| self.create_owned_action(&call, &action.body, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::OBSERVE_HELD_HOSTED_V1 => Ok(vec![Ok(rtdb_types::boxed(|| self.observe_held_hosted_action(&call, &action.body)).await?)]),
                crate::owned_actions::CONFIRM_APPLIED_HOSTED_V6 => Ok(vec![Ok(rtdb_types::boxed(|| self.confirm_applied_hosted_action(&call, &action.body)).await?)]),
                crate::owned_actions::CONFIRM_HOSTED_SOURCE_V1 =>
                    return Box::pin(self.confirm_hosted_source_action(&call, &action.body, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::READ_HELD_HOSTED_CONTROL_V3 | crate::owned_actions::READ_HELD_HOSTED_CONTROL_AT_PUBLICATION_V1 => {
                    let positioned = action_type == crate::owned_actions::READ_HELD_HOSTED_CONTROL_AT_PUBLICATION_V1;
                    return rtdb_types::boxed(|| self.read_held_hosted_control_action(&call, &action.body, positioned, reply_format)).await.map(ActionReplies::Owned);
                }
                crate::owned_actions::READ_HELD_ROOT_CATALOG_V1 =>
                    return Box::pin(self.read_held_root_catalog_action(&call, &action.body, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::OFFER_FUNDED_MUTATION_CLOCK_V1 => Ok(vec![Ok(rtdb_types::boxed(|| self.offer_funded_mutation_clock_action(&call, &action.body)).await?)]),
                crate::owned_actions::FOOTPRINT_GRANT_V1 =>
                    return rtdb_types::boxed(|| self.footprint_grant_action(&call, &action.body, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::OFFER_CLOCK_SLOT_V2 => Ok(vec![Ok(rtdb_types::boxed(|| self.offer_clock_slot_action(&call, &action.body)).await?)]),
                COMMIT_OWNED_V1 =>
                    return rtdb_types::boxed(|| self.commit_owned_action(&call, &action.body, CommitFormat::Single, ipc_stream)).await.map(ActionReplies::Owned),
                COMMIT_OWNED_V3 | COMMIT_ORDERED_V1 | COMMIT_OWNED_V4 => {
                    let format = if action_type == COMMIT_ORDERED_V1 { CommitFormat::Ordered } else { CommitFormat::Grouped };
                    return rtdb_types::boxed(|| self.commit_owned_witnessed_action(&call, &action.body, format, action_type == COMMIT_OWNED_V4, ipc_stream)).await.map(ActionReplies::Owned);
                }
                crate::owned_actions::COMMIT_OWNED_V7 =>
                    return rtdb_types::boxed(|| self.commit_owned_counted_action(&call, &action.body, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::OPEN_OWNED_SCOPED_READ_V2 => Ok(vec![Ok(rtdb_types::boxed(|| self.open_owned_scoped_read_action(&call, &action.body)).await?)]),
                crate::owned_actions::OPEN_OWNED_READ_AS_OF_V2 => Ok(vec![Ok(rtdb_types::boxed(|| self.open_owned_read_as_of_action(&call, &action.body, false)).await?)]),
                crate::owned_actions::OPEN_OWNED_READ_AT_PUBLICATION_V1 => Ok(vec![Ok(rtdb_types::boxed(|| self.open_owned_read_as_of_action(&call, &action.body, true)).await?)]),
                crate::owned_actions::OPEN_OWNED_READ_V3 => Ok(vec![Ok(rtdb_types::boxed(|| self.open_owned_read_action(&call, &action.body)).await?)]),
                crate::owned_actions::OPEN_FRESH_OWNED_READ_V2 => Ok(vec![Ok(rtdb_types::boxed(|| self.open_fresh_owned_read_action(&call, &action.body)).await?)]),
                crate::owned_actions::SPATIAL_GATHER_OWNED =>
                    return Box::pin(self.gather_spatial_owned_action(&call, &action.body, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::SPATIAL_NEXT_OWNED | crate::owned_actions::SPATIAL_RANK_NEXT_OWNED => {
                    let ranked = action.r#type == crate::owned_actions::SPATIAL_RANK_NEXT_OWNED;
                    return rtdb_types::boxed(|| self.next_spatial_owned_action(&call, &action.body, ranked, ipc_stream)).await.map(ActionReplies::Owned);
                }
                crate::owned_actions::SPATIAL_CLOSE_OWNED => {
                    self.close_spatial_owned_action(&call, &action.body)?;
                    Ok(Vec::new())
                }
                PREPARE_APPLIED_READ_V3 => Ok(vec![Ok(rtdb_types::boxed(|| self.prepare_applied_read_action(&call, &action.body)).await?)]),
                crate::owned_actions::OPEN_APPLIED_OWNED_READ_V5 => Ok(vec![Ok(rtdb_types::boxed(|| self.open_applied_owned_read_action(&call, &action.body)).await?)]),
                crate::owned_actions::SEARCH_HELD_OWNED_V7 =>
                    return rtdb_types::boxed(|| self.search_held_owned_action(&call, &action.body, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::NEAREST_HELD_OWNED_V4 =>
                    return rtdb_types::boxed(|| self.nearest_held_owned_action(&call, &action.body, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HYBRID_HELD_OWNED_V4 =>
                    return rtdb_types::boxed(|| self.hybrid_held_owned_action(&call, &action.body, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HYBRID_HELD_WORLD_V3 =>
                    return rtdb_types::boxed(|| self.hybrid_held_world_action(&call, &action.body, Current, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HYBRID_HELD_WORLD_AT_V3 =>
                    return rtdb_types::boxed(|| self.hybrid_held_world_action(&call, &action.body, EarlierEffective, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HYBRID_HELD_WORLD_PARENT_AT_V3 =>
                    return rtdb_types::boxed(|| self.hybrid_held_world_action(&call, &action.body, EarlierParent, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::SEARCH_HELD_WORLD_PARENT_AT_V3 =>
                    return rtdb_types::boxed(|| self.search_held_world_action(&call, &action.body, EarlierParent, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::SEARCH_HELD_WORLD_AT_V3 =>
                    return rtdb_types::boxed(|| self.search_held_world_action(&call, &action.body, EarlierEffective, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::SEARCH_HELD_WORLD_V4 =>
                    return rtdb_types::boxed(|| self.search_held_world_action(&call, &action.body, Current, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::NEAREST_HELD_WORLD_PARENT_AT_V3 =>
                    return rtdb_types::boxed(|| self.nearest_held_world_action(&call, &action.body, EarlierParent, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::NEAREST_HELD_WORLD_AT_V3 =>
                    return rtdb_types::boxed(|| self.nearest_held_world_action(&call, &action.body, EarlierEffective, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::NEAREST_HELD_WORLD_V3 =>
                    return rtdb_types::boxed(|| self.nearest_held_world_action(&call, &action.body, Current, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_WORLD_PARENT_RANGE_AT_V1 =>
                    return rtdb_types::boxed(|| self.held_world_ordered_action(&call, &action.body, crate::owned_actions::WorldOrder::ParentRangeAt, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_WORLD_PARENT_COUNT_AT_V1 =>
                    return rtdb_types::boxed(|| self.held_world_ordered_action(&call, &action.body, crate::owned_actions::WorldOrder::ParentCountAt, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_WORLD_RANGE_V1 =>
                    return rtdb_types::boxed(|| self.held_world_ordered_action(&call, &action.body, crate::owned_actions::WorldOrder::ParentRange, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_WORLD_COUNT_V1 =>
                    return rtdb_types::boxed(|| self.held_world_ordered_action(&call, &action.body, crate::owned_actions::WorldOrder::ParentCount, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_WORLD_MEASURE_V1 =>
                    return rtdb_types::boxed(|| self.held_world_ordered_action(&call, &action.body, crate::owned_actions::WorldOrder::ParentMeasure, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_WORLD_OVERLAY_RANGE_AT_V1 =>
                    return rtdb_types::boxed(|| self.held_world_ordered_action(&call, &action.body, crate::owned_actions::WorldOrder::OverlayRangeAt, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_WORLD_OVERLAY_RANGE_V1 =>
                    return rtdb_types::boxed(|| self.held_world_ordered_action(&call, &action.body, crate::owned_actions::WorldOrder::OverlayRange, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_RANGE_V1 =>
                    return rtdb_types::boxed(|| self.held_owned_range_action(&call, &action.body, crate::owned_actions::HeldRangeFormat::Leading, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_RANGE_V2 =>
                    return rtdb_types::boxed(|| self.held_owned_range_action(&call, &action.body, crate::owned_actions::HeldRangeFormat::Full, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_RANGE_V3 =>
                    return rtdb_types::boxed(|| self.held_owned_range_action(&call, &action.body, crate::owned_actions::HeldRangeFormat::FixedValid, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_RANGE_V4 =>
                    return rtdb_types::boxed(|| self.held_owned_range_action(&call, &action.body, crate::owned_actions::HeldRangeFormat::Historical, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_COUNT_V1 =>
                    return rtdb_types::boxed(|| self.held_owned_count_action(&call, &action.body, false, false, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_COUNT_V2 =>
                    return rtdb_types::boxed(|| self.held_owned_count_action(&call, &action.body, true, false, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_MEASURE_V1 =>
                    return rtdb_types::boxed(|| self.held_owned_count_action(&call, &action.body, false, true, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_MEASURE_AT_V1 =>
                    return rtdb_types::boxed(|| self.held_owned_count_action(&call, &action.body, true, true, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::READ_HELD_WORLD_CHANGES_DESCENDING_V2 | crate::owned_actions::READ_HELD_WORLD_CHANGES_V4 => {
                    let descending = action_type == crate::owned_actions::READ_HELD_WORLD_CHANGES_DESCENDING_V2;
                    return rtdb_types::boxed(|| self.held_world_changes_action(&call, &action.body, descending, ipc_stream)).await.map(ActionReplies::Owned);
                }
                crate::owned_actions::READ_HELD_WORLD_REGION_CHANGES_V1 =>
                    return rtdb_types::boxed(|| self.held_world_region_changes_action(&call, &action.body, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_STORAGE_USAGE_V1 =>
                    return rtdb_types::boxed(|| self.held_owned_storage_usage_action(&call, &action.body, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_MERGED_CHANGES_DESCENDING_V2 | crate::owned_actions::HELD_OWNED_MERGED_CHANGES_V4 => {
                    let descending = action_type == crate::owned_actions::HELD_OWNED_MERGED_CHANGES_DESCENDING_V2;
                    return rtdb_types::boxed(|| self.held_owned_merged_changes_action(&call, &action.body, descending, ipc_stream)).await.map(ActionReplies::Owned);
                }
                crate::owned_actions::HELD_LOOKUP_PUBLICATION_V1 => Ok(vec![Ok(rtdb_types::boxed(|| self.held_lookup_publication_action(&call, &action.body)).await?)]),
                HELD_TABLE_PUBLICATION_SEQUENCE_V1 => Ok(vec![Ok(rtdb_types::boxed(|| self.held_table_publication_sequence_action(&call, &action.body)).await?)]),
                crate::owned_actions::READ_HELD_EXPIRY_CANDIDATES_V1 =>
                    return rtdb_types::boxed(|| self.held_expiry_candidates_action(&call, &action.body, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::APPLY_HELD_EXPIRY_V1 =>
                    return rtdb_types::boxed(|| self.apply_held_expiry_action(&call, &action.body, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::NOMINATE_HELD_EXPIRY_V1 =>
                    return rtdb_types::boxed(|| self.nominate_held_expiry_action(&call, &action.body, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_CHANGES_V3 =>
                    return rtdb_types::boxed(|| self.held_owned_changes_action(&call, &action.body, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::READ_HELD_UNIQUE_V1 => Ok(vec![Ok(rtdb_types::boxed(|| self.held_owned_unique_action(&call, &action.body)).await?)]),
                crate::owned_actions::READ_HELD_UNIQUE_WINDOW_V1 =>
                    return rtdb_types::boxed(|| self.held_owned_unique_window_action(&call, &action.body, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_POINT_V1 =>
                    return rtdb_types::boxed(|| self.held_owned_point(&call, &action.body, PointTime::Default, None, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_POINT_V2 =>
                    return rtdb_types::boxed(|| self.held_owned_point(&call, &action.body, PointTime::Optional, None, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_ABSENT_KEYS_V1 => Ok(vec![Ok(Box::pin(self.held_owned_absent_keys_action(&call, &action.body)).await?)]),
                crate::owned_actions::HELD_OWNED_FIXED_POINT_V1 =>
                    return rtdb_types::boxed(|| self.held_owned_point(&call, &action.body, PointTime::Required, None, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_WORLD_PARENT_VALID_AT_V1 =>
                    return rtdb_types::boxed(|| self.held_world_valid_during_action(&call, &action.body, crate::owned_actions::WorldHistoricalRead::EarlierParent, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_WORLD_VALID_AT_V1 =>
                    return rtdb_types::boxed(|| self.held_world_valid_during_action(&call, &action.body, crate::owned_actions::WorldHistoricalRead::EarlierEffective, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_WORLD_VALID_V2 =>
                    return rtdb_types::boxed(|| self.held_world_valid_during_action(&call, &action.body, crate::owned_actions::WorldHistoricalRead::Current, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_WORLD_PARENT_IDENTITIES_AT_V1 | crate::owned_actions::HELD_WORLD_IDENTITIES_AT_V1
                | crate::owned_actions::HELD_WORLD_IDENTITIES_V1 | crate::owned_actions::HELD_WORLD_OVERLAY_IDENTITIES_AT_V1
                | crate::owned_actions::HELD_WORLD_OVERLAY_IDENTITIES_V1 => {
                    use crate::owned_actions::{
                        HELD_WORLD_IDENTITIES_AT_V1, HELD_WORLD_IDENTITIES_V1, HELD_WORLD_OVERLAY_IDENTITIES_AT_V1,
                        HELD_WORLD_PARENT_IDENTITIES_AT_V1, WorldIdentityRead,
                    };
                    let mode = match action_type {
                        HELD_WORLD_PARENT_IDENTITIES_AT_V1 => WorldIdentityRead::ParentAt,
                        HELD_WORLD_IDENTITIES_AT_V1 => WorldIdentityRead::EffectiveAt,
                        HELD_WORLD_IDENTITIES_V1 => WorldIdentityRead::Effective,
                        HELD_WORLD_OVERLAY_IDENTITIES_AT_V1 => WorldIdentityRead::OverlayAt,
                        _ => WorldIdentityRead::Overlay,
                    };
                    return rtdb_types::boxed(|| self.held_world_identities_action(&call, &action.body, mode, reply_format)).await.map(ActionReplies::Owned);
                }
                crate::owned_actions::HELD_PROTECTED_COVERAGE_V2 =>
                    return rtdb_types::boxed(|| self.held_protected_coverage_action(&call, &action.body, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_ROW_CARRIER_EXISTS_V2 => Ok(vec![Ok(rtdb_types::boxed(|| self.held_row_carrier_exists_action(&call, &action.body)).await?)]),
                crate::owned_actions::HELD_PROTECTED_CARRIERS_V2 =>
                    return rtdb_types::boxed(|| self.held_protected_carriers_action(&call, &action.body, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_SEALED_SEARCH_UNIT_ROWS_V2 =>
                    return rtdb_types::boxed(|| self.held_sealed_search_unit_rows_action(&call, &action.body, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_WORLD_SHARED_COLUMN_CENSUS_V2 =>
                    return rtdb_types::boxed(|| self.held_world_shared_column_census_action(&call, &action.body, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_IDENTITIES_V1 =>
                    return rtdb_types::boxed(|| self.held_owned_identities_action(&call, &action.body, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_IDENTITIES_ALL_VALID_V1 =>
                    return rtdb_types::boxed(|| self.held_owned_identities_all_valid_action(&call, &action.body, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_WORLD_PARENT_TOUCH_IMAGES_AT_V1 => rtdb_types::boxed(|| self.held_world_parent_touch_images_action(&call, &action.body, true)).await,
                crate::owned_actions::HELD_WORLD_PARENT_TOUCH_KEYS_AT_V1 => rtdb_types::boxed(|| self.held_world_parent_touch_images_action(&call, &action.body, false)).await,
                crate::owned_actions::PREPARE_HELD_WORLD_OVERLAY_V5 => Ok(vec![Ok(rtdb_types::boxed(|| self.prepare_held_world_overlay_action(&call, &action.body)).await?)]),
                crate::owned_actions::HELD_WORLD_PARENT_CHANGES_AT_V2 | crate::owned_actions::HELD_WORLD_ANCESTOR_CHANGES_V2 => {
                    let ancestor = action_type == crate::owned_actions::HELD_WORLD_ANCESTOR_CHANGES_V2;
                    return rtdb_types::boxed(|| self.archived_world_changes_action(&call, &action.body, ancestor, ipc_stream)).await.map(ActionReplies::Owned);
                }
                crate::owned_actions::HELD_WORLD_PARENT_VERSIONS_AT_V1 => Ok(rtdb_types::boxed(|| self.held_world_parent_versions_action(&call, &action.body)).await?.into_iter().map(Ok).collect()),
                crate::owned_actions::HELD_OWNED_VERSIONS_V2 => Ok(rtdb_types::boxed(|| self.held_owned_versions_action(&call, &action.body)).await?.into_iter().map(Ok).collect()),
                crate::owned_actions::HELD_WORLD_VERSION_V2 => rtdb_types::boxed(|| self.held_world_version_action(&call, &action.body)).await,
                crate::owned_actions::HELD_WORLD_TOUCH_IMAGES_V1 => rtdb_types::boxed(|| self.held_world_path_touch_action(&call, &action.body, true)).await,
                crate::owned_actions::HELD_WORLD_PATH_TOUCH_V1 => rtdb_types::boxed(|| self.held_world_path_touch_action(&call, &action.body, false)).await,
                crate::owned_actions::HELD_OWNED_PATH_TOUCH_V2 => rtdb_types::boxed(|| self.held_owned_path_touch_action(&call, &action.body)).await,
                crate::owned_actions::HELD_OWNED_HISTORICAL_POINT_V1 =>
                    return rtdb_types::boxed(|| self.held_owned_historical_point(&call, &action.body, None, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_POSITION_POINT_V1 =>
                    return rtdb_types::boxed(|| self.held_owned_position_point(&call, &action.body, None, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_ROOT_CUT_POINT_V1 =>
                    return rtdb_types::boxed(|| self.held_root_cut_point_action(&call, &action.body, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_VALID_DURING_V2 =>
                    return rtdb_types::boxed(|| self.held_owned_valid_during_action(&call, &action.body, crate::owned_actions::ValidHistoryMode::Timeline, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_VALID_REGION_V1 =>
                    return rtdb_types::boxed(|| self.held_owned_valid_during_action(&call, &action.body, crate::owned_actions::ValidHistoryMode::Region, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_OWNED_POSITION_VALID_DURING_V1 =>
                    return rtdb_types::boxed(|| self.held_owned_valid_during_action(&call, &action.body, crate::owned_actions::ValidHistoryMode::Positioned, ipc_stream)).await.map(ActionReplies::Owned),
                crate::owned_actions::CLOSE_OWNED_READ_V1 => {
                    self.close_owned_read_action(&call, &action.body)?;
                    Ok(Vec::new())
                }
                crate::owned_actions::PREPARE_REPLAY_PARENT_V1 => Ok(vec![Ok(rtdb_types::boxed(|| self.prepare_replay_parent_action(&call, &action.body)).await?)]),
                crate::owned_actions::FINISH_REPLAY_PARENTS_V1 => Ok(vec![Ok(rtdb_types::boxed(|| self.finish_replay_parents_action(&call, &action.body)).await?)]),
                crate::owned_actions::RETAIN_REPLAYED_WORLD_V5 => Ok(vec![Ok(rtdb_types::boxed(|| self.retain_replayed_world_action(&call, &action.body)).await?)]),
                crate::owned_actions::OPEN_WORLD_READ_V6 => Ok(vec![Ok(rtdb_types::boxed(|| self.open_world_read_action(&call, &action.body)).await?)]),
                crate::owned_actions::READ_HELD_WORLD_HEAD_V1 => Ok(vec![Ok(self.read_held_world_head_action(&call, &action.body)?)]),
                crate::owned_actions::SELECT_HELD_WORLD_PARENT_AS_OF_V1 => Ok(vec![Ok(rtdb_types::boxed(|| self.held_world_at_preflight_action(&call, &action.body, crate::owned_actions::WorldCutSelection::AsOfParent)).await?)]),
                crate::owned_actions::HELD_WORLD_PARENT_POINT_AT_V1 =>
                    return rtdb_types::boxed(|| self.held_world_point_at(&call, &action.body, true, None, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::SELECT_HELD_WORLD_AS_OF_V2 => Ok(vec![Ok(rtdb_types::boxed(|| self.held_world_at_preflight_action(&call, &action.body, crate::owned_actions::WorldCutSelection::AsOfLocal)).await?)]),
                crate::owned_actions::HELD_WORLD_AT_PREFLIGHT_V2 => Ok(vec![Ok(rtdb_types::boxed(|| self.held_world_at_preflight_action(&call, &action.body, crate::owned_actions::WorldCutSelection::ExactLocal)).await?)]),
                crate::owned_actions::HELD_WORLD_POINT_AT_V2 =>
                    return rtdb_types::boxed(|| self.held_world_point_at(&call, &action.body, false, None, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::HELD_WORLD_POINT_V2 =>
                    return rtdb_types::boxed(|| self.held_world_point(&call, &action.body, None, reply_format)).await.map(ActionReplies::Owned),
                crate::owned_actions::CLOSE_WORLD_READ_V1 => {
                    self.close_world_read_action(&call, &action.body)?;
                    Ok(Vec::new())
                }
                crate::owned_actions::PUBLISH_HELD_WORLD_RETENTION_V5 => Ok(vec![Ok(rtdb_types::boxed(|| self.publish_held_world_retention_action(&call, &action.body)).await?)]),
                crate::owned_actions::PUBLISH_WORLD_CUT_V2 | crate::owned_actions::DECIDE_WORLD_CUT_V2 => {
                    Ok(vec![Ok(rtdb_types::boxed(|| self.publish_world_cut_action(&call, &action.body,
                        action_type == crate::owned_actions::DECIDE_WORLD_CUT_V2)).await?)])
                }
                crate::owned_actions::DECIDE_HELD_WORLDS_V3 | crate::owned_actions::SWITCH_PREPARED_PROPOSAL_V2 => {
                    return rtdb_types::boxed(|| self.decide_held_worlds_action(&call, &action.body,
                        action_type == crate::owned_actions::SWITCH_PREPARED_PROPOSAL_V2, ipc_stream)).await.map(ActionReplies::Owned);
                }
                crate::owned_actions::READ_OWNED_V1 =>
                    return rtdb_types::boxed(|| self.read_owned_action(&call, &action.body, reply_format)).await.map(ActionReplies::Owned),
                "BeginTransaction" => {
                    let _: ActionBeginTransactionRequest = unpack(&action.body)?;
                    let shared = rtdb_types::boxed(|| self.connection(&call, &call.database)).await?;
                    let shared = shared.borrow().await?;
                    let mut native = call.session.open_native(&call.database, &self.0.node)?;
                    native.state_mut().set_await_token(shared.state().effective_await_token().map(str::to_owned)).map_err(status)?;
                    native.state_mut().set_timezone(shared.state().effective_timezone()).map_err(status)?;
                    native.state_mut().set_auto_commit(false).await.map_err(status)?;
                    let handle = Bytes::copy_from_slice(Uuid::new_v4().as_bytes());
                    let tx = Arc::new(Transaction::new(call.session.owner(), ConnectionResource::new(native), self.memory().reserve(512).map_err(status)?));
                    self.0.transactions.insert(handle.clone(), tx, self.0.config.transaction_idle_timeout, self.0.config.transaction_idle_timeout, false, 16).map_err(status)?;
                    Ok(vec![Ok(packed(&ActionBeginTransactionResult { transaction_id: handle }, self)?)])
                }
                "EndTransaction" => {
                    let request: ActionEndTransactionRequest = unpack(&action.body)?;
                    let tx = self.claim_transaction(&call, &request.transaction_id)?;
                    let mut native = tx.connection.borrow().await?;
                    if request.action == EndTransaction::Commit as i32 { native.state_mut().commit().await.map_err(status)?; } else { native.state_mut().rollback().map_err(status)?; }
                    drop(native); tx.close(); Ok(Vec::new())
                }
                "xtdb.CommitTransaction" => {
                    let tx = self.claim_transaction(&call, &action.body)?;
                    let mut native = tx.connection.borrow().await?;
                    let committed = native.state_mut().commit().await;
                    let mut results = Vec::new();
                    if let Some(last) = native.state().last_submitted() {
                        let error_bound = match last { LastSubmittedTx::Executed(tx) => tx.error().map(crate::error::allocation_bound).transpose()?.unwrap_or(0), _ => 0 };
                        let await_token = native.state().last_submitted_await_token();
                        let bound = wire::add(error_bound, wire::add(8192, wire::mul(await_token.map_or(0, str::len), 16)?)?)?;
                        let _scratch = self.memory().reserve(bound).map_err(status)?;
                        let (tx_id, time, committed, error) = match last {
                            LastSubmittedTx::Submitted(tx) => (tx.tx_id.0, None, None, None),
                            LastSubmittedTx::Executed(tx) => (tx.tx_key().tx_id.0, Some(ticket::timestamp(tx.tx_key().system_time)?), Some(tx.committed()), tx.error().map(crate::error::anomaly_json).transpose()?),
                        };
                        let value = serde_json::json!({ "txId": tx_id, "systemTime": time, "committed": committed, "error": error, "awaitToken": await_token });
                        let bytes = serde_json::to_vec(&value).map_err(arrow)?;
                        let charge = self.memory().reserve(bytes.capacity()).map_err(status)?;
                        results.push(Ok(ActionResult { body: wire::retain(bytes, charge) }));
                    }
                    if let Err(error) = committed { results.push(Err(status(error))); }
                    drop(native); tx.close(); Ok(results)
                }
                "CreatePreparedStatement" => {
                    let request: ActionCreatePreparedStatementRequest = unpack(&action.body)?;
                    let connection = rtdb_types::boxed(|| self.tx_or_session(&call, request.transaction_id.as_ref())).await?;
                    let shape = rtdb_types::boxed(|| self.prepare_shape(connection, &request.query)).await?;
                    let scope = request.transaction_id.map_or_else(|| PreparedScope::Session(call.database.clone()), PreparedScope::Transaction);
                    let charge = self.memory().reserve(wire::add(request.query.capacity(), wire::add(call.database.len(), 1024)?)?).map_err(status)?;
                    let prepared = Arc::new(Prepared::new(call.session.owner(), request.query, shape.plan, scope, charge));
                    let handle = Bytes::copy_from_slice(Uuid::new_v4().as_bytes());
                    let _parameters = self.memory().reserve(wire::add(wire::mul(shape.parameters, 1024)?, 4096)?).map_err(status)?;
                    let parameters = Schema::new((0..shape.parameters).map(|_| Field::new("", DataType::Null, true)).collect::<Vec<_>>());
                    let result = protocol::PreparedResult { prepared_statement_handle: handle.clone(), dataset_schema: shape.schema.as_ref().map(|schema| wire::schema_message(schema, self.memory())).transpose()?.unwrap_or_default(), parameter_schema: wire::schema_message(&parameters, self.memory())?, is_update: shape.schema.is_none() };
                    let bytes = pack_as("type.googleapis.com/arrow.flight.protocol.sql.ActionCreatePreparedStatementResult", &result, self)?;
                    self.0.prepared.insert(handle, prepared, self.0.config.prepared_statement_idle_timeout, self.0.config.prepared_statement_idle_timeout, false, 16).map_err(status)?;
                    Ok(vec![Ok(bytes)])
                }
                "ClosePreparedStatement" => {
                    let request: ActionClosePreparedStatementRequest = unpack(&action.body)?;
                    if let Lookup::Found(prepared) = self.0.prepared.lookup(&request.prepared_statement_handle, false) { if call.session.owns(&prepared.owner) { if let Lookup::Found(prepared) = self.0.prepared.take(&request.prepared_statement_handle) { prepared.close(); } } }
                    Ok(Vec::new())
                }
                "GetSessionOptions" => {
                    protocol::Empty::decode(action.body.as_ref()).map_err(arrow)?;
                    let values = [("catalog".into(), call.database.clone()), ("schema".into(), "public".into())].into_iter().map(|(key, value)| (key, protocol::SessionOption { value: Some(protocol::OptionValue::String(value)) })).collect();
                    Ok(vec![Ok(raw(&protocol::SessionOptions { session_options: values }, self)?)])
                }
                "SetSessionOptions" => {
                    let request = protocol::SessionOptions::decode(action.body.as_ref()).map_err(arrow)?; let mut errors = BTreeMap::new();
                    for (name, value) in request.session_options {
                        let error = match name.as_str() {
                            "catalog" if call.session.placed_database().is_some() => {
                                if matches!(value.value, Some(protocol::OptionValue::String(value)) if value.is_empty() || value == call.database) { None } else { Some(2) }
                            }
                            "catalog" => match value.value {
                                Some(protocol::OptionValue::String(value)) if value.is_empty() => { call.session.select_catalog(None)?; None }
                                Some(protocol::OptionValue::String(value)) => { if rtdb_types::boxed(|| self.admit_database(&value)).await? { call.session.select_catalog(Some(value))?; None } else { Some(2) } }
                                _ => Some(2),
                            },
                            "schema" => if matches!(value.value, Some(protocol::OptionValue::String(value)) if value == "public") { None } else { Some(2) },
                            _ => Some(1),
                        };
                        if let Some(value) = error { errors.insert(name, protocol::OptionError { value }); }
                    }
                    Ok(vec![Ok(raw(&protocol::SetOptionsResult { errors }, self)?)])
                }
                CLOSE_SESSION => {
                    protocol::Empty::decode(action.body.as_ref()).map_err(arrow)?;
                    call.close_session();
                    if call.minted { return Err(Status::not_found("No session to close")); }
                    let result = protocol::CloseResult { status: 1 };
                    let charge = call.reserve(wire::add(result.encoded_len(), 256)?)?;
                    Ok(vec![Ok(ActionResult { body: wire::retain(result.encode_to_vec(), charge) })])
                }
                _ => Err(unsupported()),
            }?;
            Ok(ActionReplies::Results(results))
        });
        let owner = call.audience_io_owner();
        let work = async move {
            match owner {
                Some(owner) => rtdb_storage::IoRegion::retaining(owner,
                    audience_work::within_turn(Some(AUDIENCE_READ_LIMITS), turn, work)).await,
                None => work.await,
            }
        };
        let plaintext = rtdb_storage::sealing::PlaintextView::new(Arc::clone(call.memory()));
        let replies = call.cancellation.run_until_cancelled(plaintext.reading(work)).await
            .ok_or_else(cancelled).and_then(|result| result).map_err(|error| call.error(error))?;
        let mut results = match replies {
            ActionReplies::Results(results) => results,
            ActionReplies::Owned(OwnedIpcReply::Unary(reply)) => vec![Ok(reply)],
            ActionReplies::Owned(OwnedIpcReply::Frames(frames)) => {
                self.publish();
                return ipc_action_result_stream(call, frames.into_iter().map(Ok), plaintext).await;
            }
        };
        if call.session.audience_source().is_some()
            && (results.len() > 1 || results.iter().any(|result| result.as_ref().is_ok_and(|reply|
                reply.encoded_len() > AUDIENCE_REPLY_BYTES)))
        {
            return Err(call.error(Status::resource_exhausted("The audience reply exceeds its transport allowance")));
        }
        self.publish();
        if ipc_stream && matches!(results.as_slice(), [Ok(reply)] if owned_batch::is_ipc_stream(&reply.body)) {
            let reply = results.pop().expect("one action result")?;
            return ipc_action_result_stream(call, IpcFrames(reply.body), plaintext).await;
        }
        action_result_stream(call, results, plaintext).await
    }
    fn claim_transaction(&self, call: &Call, handle: &Bytes) -> Result<Arc<Transaction>, Status> {
        self.transaction(call, handle)?; found(self.0.transactions.take(handle), "transaction")
    }
}
async fn ipc_action_result_stream(
    call: Call,
    frames: impl Iterator<Item = Result<ActionResult, Status>> + Send + 'static,
    plaintext: Arc<rtdb_storage::sealing::PlaintextView>,
) -> Result<Response<Output<ActionResult>>, Status> {
    let mut response = crate::service::handoff_action_results(call, frames, plaintext, true).await?;
    response.metadata_mut().insert(OWNED_IPC_HEADER, tonic::metadata::MetadataValue::from_static("stream-v1"));
    Ok(response)
}

fn unpack<M: ProstMessageExt>(bytes: &[u8]) -> Result<M, Status> { Any::decode(bytes).map_err(arrow)?.unpack::<M>().map_err(arrow)?.ok_or_else(|| invalid("Action body has the wrong protobuf type")) }
fn packed<M: ProstMessageExt>(value: &M, service: &Service) -> Result<ActionResult, Status> { pack_as(M::type_url(), value, service) }
fn pack_as<M: Message>(type_url: &str, value: &M, service: &Service) -> Result<ActionResult, Status> {
    let charge = service.memory().reserve(wire::add(wire::mul(value.encoded_len(), 3)?, wire::add(type_url.len(), 1024)?)?).map_err(status)?;
    Ok(ActionResult { body: wire::retain(Any { type_url: type_url.into(), value: value.encode_to_vec().into() }.encode_to_vec(), charge) })
}
fn raw<M: Message>(value: &M, service: &Service) -> Result<ActionResult, Status> { let charge = service.memory().reserve(wire::add(value.encoded_len(), 256)?).map_err(status)?; Ok(ActionResult { body: wire::retain(value.encode_to_vec(), charge) }) }

#[cfg(test)]
#[test]
fn fresh_flight_sessions_admit_only_read_actions() {
    use crate::owned_actions::*;
    assert!(fresh_replica_action(OPEN_FRESH_OWNED_READ_V2));
    assert!(fresh_replica_action(HELD_OWNED_POINT_V2));
    assert!(fresh_replica_action(READ_HELD_ROOT_CATALOG_V1));
    assert!(fresh_replica_action(CLOSE_OWNED_READ_V1));
    assert!(!fresh_replica_action(OPEN_OWNED_READ_V3));
    assert!(!fresh_replica_action(OFFER_CLOCK_SLOT_V2));
    assert!(!fresh_replica_action(OFFER_FUNDED_MUTATION_CLOCK_V1));
    assert!(!fresh_replica_action(COMMIT_OWNED_V7));
    assert!(fresh_replica_action(SEAL_HOSTED_RANGES_V1));
    assert!(fresh_replica_action(VALIDATE_FRESH_HOSTED_V1));
    assert!(!fresh_replica_action(DECIDE_HOSTED_V4));
}
