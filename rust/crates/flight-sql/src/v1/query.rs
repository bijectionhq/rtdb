use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use arrow_flight::FlightData;
use prost::Message;
use rtdb_node::{Database, PreparedSql, ResultSignaturePolicy};
use rtdb_query::{QueryContext, QueryCursor};
use rtdb_types::{LogicalType, Timestamp};
use sha2::{Digest, Sha256};
use tonic::{Response, Status};

use crate::{
    service::{Call, Output, Service},
    wire,
};

use super::{
    MAX_PAGE_ROWS, MAX_WAIT, error, head,
    proto::{self, at::Selection},
    tokens::{Continuation, cursor_expired},
    trailer,
};

#[derive(Default)]
pub(super) struct PlanCache(Mutex<VecDeque<(uuid::Uuid, PreparedSql)>>);

impl PlanCache {
    pub fn prepare(
        &self,
        scope: uuid::Uuid,
        sql: &str,
        context: &Arc<QueryContext>,
        types: &[LogicalType],
    ) -> Result<PreparedSql, Status> {
        {
            let mut cached = self.0.lock().expect("plan cache lock");
            if let Some(index) = cached.iter().position(|(name, plan)| {
                *name == scope && plan.source() == sql && plan.parameter_types() == types
            }) {
                let entry = cached.remove(index).expect("matching prepared statement");
                let plan = entry.1.clone();
                cached.push_back(entry);
                return Ok(plan);
            }
        }
        let options = rtdb_sql::PrepareOptions {
            parameter_types: types.iter().cloned().enumerate().collect(),
            ..Default::default()
        };
        let prepared = PreparedSql::prepare(sql, context, options, ResultSignaturePolicy::Dynamic)
            .map_err(error::anomaly)?;
        if prepared.is_dml() || prepared.selects_read_settings() {
            return Err(error::incorrect(
                "Query requires read-only SQL and uses Source for its read settings",
            ));
        }
        let mut cached = self.0.lock().expect("plan cache lock");
        if cached.len() == 32 {
            cached.pop_front();
        }
        cached.push_back((scope, prepared.clone()));
        Ok(prepared)
    }
}

fn fingerprint(query: &proto::QueryRequest) -> [u8; 32] {
    let mut hash = Sha256::new();
    if let Some(source) = &query.source {
        hash.update(Sha256::digest(source.encode_to_vec()));
    }
    hash.update(Sha256::digest(query.sql.as_bytes()));
    if let Some(params) = &query.params_ipc {
        hash.update(Sha256::digest(params));
    }
    hash.update([query.track.into()]);
    hash.finalize().into()
}

impl Service {
    pub(super) async fn v1_query(
        &self,
        call: Call,
        request: proto::QueryRequest,
    ) -> Result<Response<Output<FlightData>>, Status> {
        let (mut cursor, database, page, limit, mut trailer_msg) = self
            .open_v1_query(&call, request)
            .await
            .map_err(|status| call.error(status))?;
        let memory = call.memory().clone();
        let schema = wire::plan_schema(cursor.schema(), &memory).map_err(error::map_status)?;
        let initial = cursor.delivery_grant().await.map_err(error::anomaly)?;
        let (headers, cutoff) = call
            .delivery_headers(&initial, true)
            .map_err(error::map_status)?;
        let service = self.clone();
        let stream: Output<FlightData> = Box::pin(async_stream::try_stream! {
            let mut encoder = wire::Encoder::new(&schema, &memory).map_err(error::map_status)?;
            service.check_attachment(&call, &database)?;
            call.handoff(initial, cutoff).map_err(error::map_status)?;
            yield wire::schema_data(&schema, &memory).map_err(error::map_status)?;
            let mut skip = page.offset;
            let mut remaining = limit;
            let mut sent = 0_u64;
            loop {
                service.check_attachment(&call, &database)?;
                let encoded = cursor.encode_next_batch(|batch| -> Result<_, Status> {
                    let rows = batch.batch().num_rows() as u64;
                    let discard = rows.min(skip);
                    skip -= discard;
                    let rows = rows - discard;
                    let take = rows.min(remaining);
                    remaining -= take;
                    sent += take;
                    let data = if take == 0 { Vec::new() } else {
                        let slice = batch.slice(discard as usize, take as usize).map_err(error::anomaly)?;
                        let declared = rtdb_columnar::conform_batch(slice, &schema, &memory).map_err(error::anomaly)?;
                        encoder.declared_batch(&declared, &memory).map_err(error::map_status)?
                    };
                    Ok((data, rows > take))
                }).await.map_err(error::anomaly)?.transpose()?;
                let Some((data, more)) = encoded else { break; };
                for item in data {
                    service.check_attachment(&call, &database)?;
                    call.handoff(cursor.delivery_grant().await.map_err(error::anomaly)?, cutoff).map_err(error::map_status)?;
                    yield item;
                }
                if more {
                    let next = Continuation { offset: page.offset.checked_add(sent).ok_or_else(|| error::incorrect("Page offset overflow"))?, ..page };
                    trailer_msg.next = Some(service.0.v1.tokens.continuation(database.wire_incarnation(), &next));
                    break;
                }
            }
            if skip != 0 { Err(cursor_expired())?; }
            service.check_attachment(&call, &database)?;
            call.handoff(cursor.delivery_grant().await.map_err(error::anomaly)?, cutoff).map_err(error::map_status)?;
            yield trailer(trailer_msg);
        });
        let mut response = Response::new(stream);
        *response.metadata_mut() = headers;
        Ok(response)
    }

    async fn open_v1_query(
        &self,
        call: &Call,
        request: proto::QueryRequest,
    ) -> Result<
        (
            QueryCursor,
            Arc<Database>,
            Continuation,
            u64,
            proto::QueryTrailer,
        ),
        Status,
    > {
        let source = request
            .source
            .as_ref()
            .ok_or_else(|| error::incorrect("Query requires a Source"))?;
        let layer = proto::Layer::try_from(source.layer)
            .map_err(|_| error::incorrect("Unknown source layer"))?;
        let selection = source.at.as_ref().and_then(|at| at.selection.as_ref());
        if source.at.is_some() && selection.is_none() {
            return Err(error::incorrect("Empty At selector"));
        }
        let database = self.wire_database(call, &source.branch, layer).await?;
        let tokens = &self.0.v1.tokens;
        let scope = database.wire_incarnation();
        let request_hash = fingerprint(&request);
        let (limit, after) = match &request.page {
            Some(page) if (1..=MAX_PAGE_ROWS).contains(&page.limit) => {
                (u64::from(page.limit), page.after.as_slice())
            }
            Some(_) => {
                return Err(error::incorrect(
                    "Query page limit is outside Capabilities limits",
                ));
            }
            None => (u64::MAX, &[][..]),
        };
        let continuation = if after.is_empty() {
            None
        } else {
            let page = tokens.read_continuation(scope, after)?;
            if page.request != request_hash {
                return Err(error::incorrect(
                    "Continuation belongs to a different query",
                ));
            }
            Some(page)
        };
        if continuation.is_none()
            && let Some(Selection::AtLeast(basis)) = selection
        {
            let minimum = tokens.read_basis(scope, basis)?;
            if minimum != Timestamp::MIN {
                self.0
                    .node
                    .wire_await_time(
                        &call.database,
                        call.session.caller.principal.as_ref(),
                        minimum,
                        MAX_WAIT,
                        &call.cancellation,
                    )
                    .await
                    .map_err(error::anomaly)?;
            }
        }
        let context = self
            .wire_context(
                call,
                &database,
                continuation.is_none() && matches!(selection, Some(Selection::Linearizable(_))),
            )
            .await?;
        let present = head(&context, &call.database)?;
        let page = match continuation {
            Some(page) => page,
            None => {
                let now = self.0.node.now();
                let (cut, clock) = match selection {
                    Some(Selection::AsOfMicros(time)) => {
                        (present.min(Timestamp(*time)), Timestamp(*time))
                    }
                    Some(Selection::Exactly(basis)) => {
                        let cut = tokens.read_basis(scope, basis)?;
                        (cut, cut)
                    }
                    _ => (present, now),
                };
                Continuation {
                    cut,
                    clock,
                    offset: 0,
                    request: request_hash,
                }
            }
        };
        if page.cut > present {
            return Err(error::unavailable(
                "Database has not applied the requested basis",
            ));
        }
        let context = context
            .with_wire_cut(page.cut, page.clock.into())
            .map_err(error::anomaly)?;
        let parameters = request
            .params_ipc
            .as_deref()
            .map(|bytes| Parameters::read(bytes, &context))
            .transpose()?;
        let types = parameters
            .as_ref()
            .map_or(&[][..], |params| params.types.as_slice());
        let prepared = self
            .0
            .v1
            .plans
            .prepare(scope, &request.sql, &context, types)?;
        let cursor = open_query(prepared, context, parameters.as_ref()).await?;
        let reply = proto::QueryTrailer {
            basis: Some(tokens.basis(scope, page.cut)),
            system_time_micros: page.cut.0,
            claim: request.track.then(|| tokens.claim(scope, page.cut)),
            next: None,
        };
        Ok((cursor, database, page, limit, reply))
    }
}

struct Parameters {
    batch: rtdb_columnar::OwnedBatch,
    types: wire::ParameterTypes,
}

impl Parameters {
    fn read(bytes: &[u8], context: &QueryContext) -> Result<Self, Status> {
        let stream =
            rtdb_format::ipc::OwnedStream::read(bytes, &context.memory).map_err(error::anomaly)?;
        let [batch] = stream.pages() else {
            return Err(error::incorrect(
                "Parameters require exactly one Arrow batch",
            ));
        };
        if batch.batch().num_rows() != 1 {
            return Err(error::incorrect("Parameters require exactly one row"));
        }
        let types = wire::ParameterTypes::from_batch(batch.batch(), &context.memory)
            .map_err(error::map_status)?;
        Ok(Self {
            batch: batch.clone(),
            types,
        })
    }
}

async fn open_query(
    prepared: PreparedSql,
    context: Arc<QueryContext>,
    parameters: Option<&Parameters>,
) -> Result<QueryCursor, Status> {
    let Some(parameters) = parameters else {
        return prepared
            .open_query_columns_typed(context, Vec::new(), &[])
            .await
            .map_err(error::anomaly);
    };
    let types = parameters.types.as_slice();
    let arguments =
        wire::arguments(&parameters.batch, &context.memory).map_err(error::map_status)?;
    let row = arguments
        .rows()
        .next()
        .ok_or_else(|| error::incorrect("Parameters require one row"))?
        .map_err(error::anomaly)?;
    let size = row
        .iter()
        .try_fold(1024usize, |bytes, value| {
            wire::add(
                wire::add(bytes, std::mem::size_of::<rtdb_types::Value>())?,
                value.retained_bytes().map_err(error::anomaly)?,
            )
        })
        .map_err(error::map_status)?;
    let _values = context.memory.reserve(size).map_err(error::anomaly)?;
    prepared
        .open_query_columns_typed(context, row.to_vec(), types)
        .await
        .map_err(error::anomaly)
}
