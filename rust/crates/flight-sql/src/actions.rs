use std::{collections::BTreeMap, sync::Arc};
use arrow_flight::{Action, Result as ActionResult, sql::{Any, ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest, ActionEndTransactionRequest, EndTransaction, ProstMessageExt}};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use prost::Message;
use rtdb_node::LastSubmittedTx;
use tonic::{Response, Status};
use uuid::Uuid;
use crate::{error::{arrow, cancelled, invalid, status, unsupported}, lease::{Leased, Lookup}, protocol, service::{Call, Output, Service, found, result_stream}, state::{ConnectionResource, Prepared, PreparedScope, Transaction}, ticket, wire};

impl Service {
    pub async fn action(&self, call: Call, action: Action) -> Result<Response<Output<ActionResult>>, Status> {
        let work = async {
            match action.r#type.as_str() {
                crate::owned_actions::DECLARE_OWNED_FIXED_RANGE_V1 => Ok(vec![Ok(self.declare_owned_fixed_range_action(&call, &action.body).await?)]),
                crate::owned_actions::DECLARE_OWNED_CAPACITY_V2 => Ok(vec![Ok(self.declare_owned_capacity_action(&call, &action.body).await?)]),
                crate::owned_actions::DECLARE_OWNED_UNIQUE_PATH_V1 => Ok(vec![Ok(self.declare_owned_single_column_path_action(&call, &action.body, true).await?)]),
                crate::owned_actions::DECLARE_OWNED_SEARCH_PATH_V1 => Ok(vec![Ok(self.declare_owned_single_column_path_action(&call, &action.body, false).await?)]),
                crate::owned_actions::DECLARE_OWNED_RELATION_V1 => Ok(vec![Ok(self.declare_owned_relation_action(&call, &action.body).await?)]),
                crate::owned_actions::DECLARE_OWNED_PATH_V1 => Ok(vec![Ok(self.declare_owned_path_action(&call, &action.body).await?)]),
                crate::owned_actions::CREATE_OWNED_V1 => Ok(vec![Ok(self.create_owned_action(&call, &action.body).await?)]),
                crate::owned_actions::COMMIT_HOSTED_COUNTED_V2 => Ok(vec![Ok(self.commit_hosted_controls_action(&call, &action.body, true).await?)]),
                crate::owned_actions::OBSERVE_HELD_HOSTED_V1 => Ok(vec![Ok(self.observe_held_hosted_action(&call, &action.body).await?)]),
                crate::owned_actions::READ_HELD_HOSTED_CONTROL_V1 => Ok(vec![Ok(self.read_held_hosted_control_action(&call, &action.body).await?)]),
                crate::owned_actions::COMMIT_HOSTED_WORLD_READS_V2 => Ok(vec![Ok(self.commit_hosted_world_reads_action(&call, &action.body).await?)]),
                crate::owned_actions::COMMIT_HOSTED_V1 => Ok(vec![Ok(self.commit_hosted_controls_action(&call, &action.body, false).await?)]),
                crate::owned_actions::COMMIT_OWNED_V1 => Ok(vec![Ok(self.commit_owned_action(&call, &action.body, crate::owned_actions::CommitFormat::Single).await?)]),
                crate::owned_actions::COMMIT_OWNED_V2 => Ok(vec![Ok(self.commit_owned_action(&call, &action.body, crate::owned_actions::CommitFormat::Grouped).await?)]),
                crate::owned_actions::COMMIT_OWNED_V3 => Ok(vec![Ok(self.commit_owned_witnessed_action(&call, &action.body, crate::owned_actions::CommitFormat::Grouped, false).await?)]),
                crate::owned_actions::COMMIT_ORDERED_V1 => Ok(vec![Ok(self.commit_owned_witnessed_action(&call, &action.body, crate::owned_actions::CommitFormat::Ordered, false).await?)]),
                crate::owned_actions::COMMIT_OWNED_V4 => Ok(vec![Ok(self.commit_owned_witnessed_action(&call, &action.body, crate::owned_actions::CommitFormat::Grouped, true).await?)]),
                crate::owned_actions::COMMIT_OWNED_V7 => Ok(vec![Ok(self.commit_owned_counted_action(&call, &action.body).await?)]),
                crate::owned_actions::OPEN_OWNED_SCOPED_READ_V1 => Ok(vec![Ok(self.open_owned_scoped_read_action(&call, &action.body).await?)]),
                crate::owned_actions::OPEN_OWNED_READ_AS_OF_V1 => Ok(vec![Ok(self.open_owned_read_as_of_action(&call, &action.body).await?)]),
                crate::owned_actions::OPEN_OWNED_READ_V2 => Ok(vec![Ok(self.open_owned_read_action(&call, &action.body).await?)]),
                crate::owned_actions::OPEN_APPLIED_OWNED_READ_V1 => Ok(vec![Ok(self.open_applied_owned_read_action(&call, &action.body).await?)]),
                crate::owned_actions::SEARCH_HELD_OWNED_V5 => Ok(vec![Ok(self.search_held_owned_action(&call, &action.body).await?)]),
                crate::owned_actions::SEARCH_HELD_WORLD_V1 => Ok(vec![Ok(self.search_held_world_action(&call, &action.body).await?)]),
                crate::owned_actions::HELD_WORLD_RANGE_V1 => Ok(vec![Ok(self.held_world_ordered_action(&call, &action.body, crate::owned_actions::WorldOrder::ParentRange).await?)]),
                crate::owned_actions::HELD_WORLD_COUNT_V1 => Ok(vec![Ok(self.held_world_ordered_action(&call, &action.body, crate::owned_actions::WorldOrder::ParentCount).await?)]),
                crate::owned_actions::HELD_WORLD_MEASURE_V1 => Ok(vec![Ok(self.held_world_ordered_action(&call, &action.body, crate::owned_actions::WorldOrder::ParentMeasure).await?)]),
                crate::owned_actions::HELD_WORLD_OVERLAY_RANGE_V1 => Ok(vec![Ok(self.held_world_ordered_action(&call, &action.body, crate::owned_actions::WorldOrder::OverlayRange).await?)]),
                crate::owned_actions::HELD_OWNED_RANGE_V1 => Ok(vec![Ok(self.held_owned_range_action(&call, &action.body, crate::owned_actions::HeldRangeFormat::Leading).await?)]),
                crate::owned_actions::HELD_OWNED_RANGE_V2 => Ok(vec![Ok(self.held_owned_range_action(&call, &action.body, crate::owned_actions::HeldRangeFormat::Full).await?)]),
                crate::owned_actions::HELD_OWNED_RANGE_V3 => Ok(vec![Ok(self.held_owned_range_action(&call, &action.body, crate::owned_actions::HeldRangeFormat::FixedValid).await?)]),
                crate::owned_actions::HELD_OWNED_RANGE_V4 => Ok(vec![Ok(self.held_owned_range_action(&call, &action.body, crate::owned_actions::HeldRangeFormat::Historical).await?)]),
                crate::owned_actions::HELD_OWNED_COUNT_V1 => Ok(vec![Ok(self.held_owned_count_action(&call, &action.body, false, false).await?)]),
                crate::owned_actions::HELD_OWNED_COUNT_V2 => Ok(vec![Ok(self.held_owned_count_action(&call, &action.body, true, false).await?)]),
                crate::owned_actions::HELD_OWNED_MEASURE_V1 => Ok(vec![Ok(self.held_owned_count_action(&call, &action.body, false, true).await?)]),
                crate::owned_actions::HELD_OWNED_MEASURE_AT_V1 => Ok(vec![Ok(self.held_owned_count_action(&call, &action.body, true, true).await?)]),
                crate::owned_actions::READ_HELD_WORLD_CHANGES_DESCENDING_V1 => Ok(vec![Ok(self.held_world_changes_action(&call, &action.body, true).await?)]),
                crate::owned_actions::READ_HELD_WORLD_CHANGES_V3 => Ok(vec![Ok(self.held_world_changes_action(&call, &action.body, false).await?)]),
                crate::owned_actions::HELD_OWNED_STORAGE_USAGE_V1 => Ok(vec![Ok(self.held_owned_storage_usage_action(&call, &action.body).await?)]),
                crate::owned_actions::HELD_OWNED_MERGED_CHANGES_DESCENDING_V1 => Ok(vec![Ok(self.held_owned_merged_changes_action(&call, &action.body, true).await?)]),
                crate::owned_actions::HELD_OWNED_MERGED_CHANGES_V3 => Ok(vec![Ok(self.held_owned_merged_changes_action(&call, &action.body, false).await?)]),
                crate::owned_actions::READ_HELD_EXPIRY_CANDIDATES_V1 => Ok(vec![Ok(self.held_expiry_candidates_action(&call, &action.body).await?)]),
                crate::owned_actions::APPLY_HELD_EXPIRY_V1 => Ok(vec![Ok(self.apply_held_expiry_action(&call, &action.body).await?)]),
                crate::owned_actions::NOMINATE_HELD_EXPIRY_V1 => Ok(vec![Ok(self.nominate_held_expiry_action(&call, &action.body).await?)]),
                crate::owned_actions::HELD_OWNED_CHANGES_V3 => Ok(vec![Ok(self.held_owned_changes_action(&call, &action.body).await?)]),
                crate::owned_actions::READ_HELD_UNIQUE_V1 => Ok(vec![Ok(self.held_owned_unique_action(&call, &action.body).await?)]),
                crate::owned_actions::HELD_OWNED_POINT_V1 => Ok(vec![Ok(self.held_owned_point_action(&call, &action.body, false).await?)]),
                crate::owned_actions::HELD_OWNED_POINT_V2 => Ok(vec![Ok(self.held_owned_point_action(&call, &action.body, true).await?)]),
                crate::owned_actions::HELD_WORLD_VALID_V2 => Ok(vec![Ok(self.held_world_valid_during_action(&call, &action.body).await?)]),
                crate::owned_actions::HELD_WORLD_IDENTITIES_V1 => Ok(vec![Ok(self.held_world_identities_action(&call, &action.body, false).await?)]),
                crate::owned_actions::HELD_WORLD_OVERLAY_IDENTITIES_V1 => Ok(vec![Ok(self.held_world_identities_action(&call, &action.body, true).await?)]),
                crate::owned_actions::HELD_PROTECTED_COVERAGE_V1 => Ok(vec![Ok(self.held_protected_coverage_action(&call, &action.body).await?)]),
                crate::owned_actions::HELD_PROTECTED_CARRIERS_V1 => Ok(vec![Ok(self.held_protected_carriers_action(&call, &action.body).await?)]),
                crate::owned_actions::HELD_OWNED_IDENTITIES_V1 => Ok(vec![Ok(self.held_owned_identities_action(&call, &action.body).await?)]),
                crate::owned_actions::HELD_OWNED_VERSIONS_V2 => Ok(self.held_owned_versions_action(&call, &action.body).await?.into_iter().map(Ok).collect()),
                crate::owned_actions::HELD_WORLD_VERSION_V1 => self.held_world_version_action(&call, &action.body).await,
                crate::owned_actions::HELD_WORLD_TOUCH_AFTER_V1 => Ok(vec![Ok(self.held_world_touch_after_action(&call, &action.body).await?)]),
                crate::owned_actions::HELD_WORLD_PATH_TOUCH_V1 => self.held_world_path_touch_action(&call, &action.body).await,
                crate::owned_actions::HELD_OWNED_PATH_TOUCH_V1 => self.held_owned_path_touch_action(&call, &action.body).await,
                crate::owned_actions::HELD_OWNED_HISTORICAL_POINT_V1 => Ok(vec![Ok(self.held_owned_historical_point_action(&call, &action.body).await?)]),
                crate::owned_actions::HELD_OWNED_VALID_DURING_V2 => Ok(vec![Ok(self.held_owned_valid_during_action(&call, &action.body).await?)]),
                crate::owned_actions::CLOSE_OWNED_READ_V1 => {
                    self.close_owned_read_action(&call, &action.body)?;
                    Ok(Vec::new())
                }
                crate::owned_actions::PREPARE_REPLAY_PARENT_V1 => Ok(vec![Ok(self.prepare_replay_parent_action(&call, &action.body).await?)]),
                crate::owned_actions::FINISH_REPLAY_PARENTS_V1 => Ok(vec![Ok(self.finish_replay_parents_action(&call, &action.body).await?)]),
                crate::owned_actions::RETAIN_REPLAYED_WORLD_V1 => Ok(vec![Ok(self.retain_replayed_world_action(&call, &action.body).await?)]),
                crate::owned_actions::OPEN_WORLD_READ_V4 => Ok(vec![Ok(self.open_world_read_action(&call, &action.body).await?)]),
                crate::owned_actions::HELD_WORLD_POINT_V2 => Ok(vec![Ok(self.held_world_point_action(&call, &action.body).await?)]),
                crate::owned_actions::CLOSE_WORLD_READ_V1 => {
                    self.close_world_read_action(&call, &action.body)?;
                    Ok(Vec::new())
                }
                crate::owned_actions::PUBLISH_WORLD_CUT_V1 | crate::owned_actions::PUBLISH_WORLD_CUT_V2 => {
                    Ok(vec![Ok(self.publish_world_cut_action(&call, &action.body,
                        action.r#type == crate::owned_actions::PUBLISH_WORLD_CUT_V2).await?)])
                }
                crate::owned_actions::COMMIT_WORLD_V3 => Ok(vec![Ok(self.validate_world_action(&call, &action.body).await?)]),
                crate::owned_actions::COMMIT_HELD_WORLDS_V4 => Ok(vec![Ok(self.commit_world_batch_action(&call, &action.body).await?)]),
                crate::owned_actions::READ_OWNED_V1 => Ok(vec![Ok(self.read_owned_action(&call, &action.body).await?)]),
                "BeginTransaction" => {
                    let _: ActionBeginTransactionRequest = unpack(&action.body)?;
                    let shared = self.connection(&call, &call.database).await?;
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
                    let connection = self.tx_or_session(&call, request.transaction_id.as_ref()).await?;
                    let shape = self.prepare_shape(connection, &request.query).await?;
                    let scope = request.transaction_id.map_or_else(|| PreparedScope::Session(call.database.clone()), PreparedScope::Transaction);
                    let charge = self.memory().reserve(wire::add(request.query.capacity(), wire::add(call.database.len(), 1024)?)?).map_err(status)?;
                    let prepared = Arc::new(Prepared::new(call.session.owner(), request.query, scope, charge));
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
                                Some(protocol::OptionValue::String(value)) if value.is_empty() => { call.session.select_catalog(None, &self.0.node)?; None }
                                Some(protocol::OptionValue::String(value)) => { if self.admit_database(&value).await? { call.session.select_catalog(Some(value), &self.0.node)?; None } else { Some(2) } }
                                _ => Some(2),
                            },
                            "schema" => if matches!(value.value, Some(protocol::OptionValue::String(value)) if value == "public") { None } else { Some(2) },
                            _ => Some(1),
                        };
                        if let Some(value) = error { errors.insert(name, protocol::OptionError { value }); }
                    }
                    Ok(vec![Ok(raw(&protocol::SetOptionsResult { errors }, self)?)])
                }
                "CloseSession" => {
                    protocol::Empty::decode(action.body.as_ref()).map_err(arrow)?;
                    if let Lookup::Found(session) = self.0.sessions.take(&call.session.id) { session.close(); }
                    if call.minted { return Err(Status::not_found("No session to close")); }
                    Ok(vec![Ok(raw(&protocol::CloseResult { status: 1 }, self)?)])
                }
                _ => Err(unsupported()),
            }
        };
        let results = call.cancellation.run_until_cancelled(work).await.ok_or_else(cancelled).and_then(|result| result).map_err(|error| call.error(error))?;
        self.publish(); result_stream(call, results)
    }
    fn claim_transaction(&self, call: &Call, handle: &Bytes) -> Result<Arc<Transaction>, Status> {
        self.transaction(call, handle)?; found(self.0.transactions.take(handle), "transaction")
    }
}
fn unpack<M: ProstMessageExt>(bytes: &[u8]) -> Result<M, Status> { Any::decode(bytes).map_err(arrow)?.unpack::<M>().map_err(arrow)?.ok_or_else(|| invalid("Action body has the wrong protobuf type")) }
fn packed<M: ProstMessageExt>(value: &M, service: &Service) -> Result<ActionResult, Status> { pack_as(M::type_url(), value, service) }
fn pack_as<M: Message>(type_url: &str, value: &M, service: &Service) -> Result<ActionResult, Status> {
    let charge = service.memory().reserve(wire::add(wire::mul(value.encoded_len(), 3)?, wire::add(type_url.len(), 1024)?)?).map_err(status)?;
    Ok(ActionResult { body: wire::retain(Any { type_url: type_url.into(), value: value.encode_to_vec().into() }.encode_to_vec(), charge) })
}
fn raw<M: Message>(value: &M, service: &Service) -> Result<ActionResult, Status> { let charge = service.memory().reserve(wire::add(value.encoded_len(), 256)?).map_err(status)?; Ok(ActionResult { body: wire::retain(value.encode_to_vec(), charge) }) }
