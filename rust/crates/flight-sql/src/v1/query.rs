use std::{
    collections::{BTreeSet, VecDeque},
    sync::{Arc, Mutex},
};

use arrow_buffer::Buffer;
use arrow_flight::FlightData;
use arrow_schema::DataType;
use prost::Message;
use rtdb_columnar::MemoryBudget;
use rtdb_crypt::tokens::{PageToken, TokenIssuer};
use rtdb_node::{Database, PreparedSql, ResultSignaturePolicy, serving::QuerySource};
use rtdb_query::{QueryContext, QueryCursor};
use rtdb_tx::transact::CustodyGroupId;
use rtdb_types::{LogicalType, Timestamp, rtdb_v1::{At, Basis, ClaimEvidence, ClaimFootprint, ClaimScope, LabelScope, Layer}};
use sha2::{Digest, Sha256};
use tonic::{Response, Status};

use crate::{
    service::{Call, Output, Service},
    wire,
};

use super::{
    MAX_PAGE_ROWS, MAX_WAIT, error, head,
    proto::{self, at::Selection},
    tokens::{self, cursor_expired},
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

struct OpenedQuery {
    cursor: QueryCursor,
    database: Arc<Database>,
    issuer: Option<TokenIssuer>,
    labels: Option<LabelScope>,
    page: PageToken,
    limit: u64,
    trailer: proto::QueryTrailer,
    custody_groups: BTreeSet<CustodyGroupId>,
}

impl Service {
    pub(super) async fn v1_query(
        &self,
        call: Call,
        request: proto::QueryRequest,
    ) -> Result<Response<Output<FlightData>>, Status> {
        let OpenedQuery { mut cursor, database, issuer, labels, page, limit, trailer: mut trailer_msg, custody_groups } = self
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
            check_custody(&call, &database, &custody_groups).await?;
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
                    if let Some(issuer) = &issuer { issuer.check_readable().await.map_err(error::anomaly)?; }
                    call.handoff(cursor.delivery_grant().await.map_err(error::anomaly)?, cutoff).map_err(error::map_status)?;
                    check_custody(&call, &database, &custody_groups).await?;
                    yield item;
                }
                if more {
                    let next = PageToken { offset: page.offset.checked_add(sent).ok_or_else(|| error::incorrect("Page offset overflow"))?, ..page };
                    let issuer = issuer.as_ref().ok_or_else(|| error::unsupported("query.page"))?;
                    trailer_msg.next = Some(issuer.seal_page_in(&next, labels.as_ref()).await.map_err(error::anomaly)?);
                    break;
                }
            }
            if skip != 0 { Err(cursor_expired())?; }
            if let Some(issuer) = &issuer { issuer.check_readable().await.map_err(error::anomaly)?; }
            service.check_attachment(&call, &database)?;
            call.handoff(cursor.delivery_grant().await.map_err(error::anomaly)?, cutoff).map_err(error::map_status)?;
            check_custody(&call, &database, &custody_groups).await?;
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
    ) -> Result<OpenedQuery, Status> {
        let source = request
            .source
            .as_ref()
            .ok_or_else(|| error::incorrect("Query requires a Source"))?;
        let layer = proto::Layer::try_from(source.layer)
            .map_err(|_| error::incorrect("Unknown source layer"))?;
        let claim_layer = match layer { proto::Layer::Merged => Layer::Merged, proto::Layer::Local => Layer::Local };
        let selection = source.at.as_ref().and_then(|at| at.selection.as_ref());
        if source.at.is_some() && selection.is_none() {
            return Err(error::incorrect("Empty At selector"));
        }
        let database = self.wire_database(call, &source.branch, layer).await?;
        let driver = database.serving_driver();
        let visibility = driver.map(|driver| self.serving_scope(call, driver)).transpose()?.flatten();
        let authority = if request.track || request.page.is_some() {
            if !database.wire_tokens_supported() {
                return Err(error::unsupported(if request.track { "query.track" } else { "query.page" }));
            }
            Some(database.token_authority().await.map_err(error::anomaly)?)
        } else { None };
        let raw_scope = match driver {
            Some(driver) => uuid::Uuid::from_bytes(driver.incarnation()),
            None => database.wire_incarnation().await.map_err(error::anomaly)?,
        };
        let scope = visibility.as_ref().map_or(raw_scope, |scope| {
            uuid::Uuid::from_bytes(scope.labels.main_read_incarnation(*raw_scope.as_bytes()))
        });
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
        let (continuation, issuer) = match authority {
            Some(authority) if !after.is_empty() => {
                let (page, issuer) = authority.resume_page_in(after, visibility.as_ref().map(|scope| &scope.labels)).await.map_err(error::anomaly)?;
                if page.request != request_hash {
                    return Err(error::incorrect("Continuation belongs to a different query"));
                }
                (Some(page), Some(issuer))
            }
            Some(authority) => (None, Some(authority.issuer().await.map_err(error::anomaly)?)),
            None => (None, None),
        };
        let (context, page, cut, scoped_claim, custody_groups) = if let Some(driver) = driver {
            let now = self.0.node.now();
            let exact = continuation.is_none() && matches!(selection, Some(Selection::Exactly(_)));
            let (query_source, clock, offset) = applied_source(source, claim_layer, selection, continuation, now)?;
            let view = match &visibility {
                Some(scope) => driver.query_scoped(query_source, request.track, now, scope, &call.cancellation).await,
                None => driver.query(query_source, false, now, &call.cancellation).await,
            }.map_err(error::anomaly)?;
            let cut = view.cut.system_time;
            let (context, clock) = if visibility.is_some() {
                let clock = continuation.map_or(now, |page| page.clock);
                let snapshot = rtdb_query::DatabaseSnapshot { partitions: vec![view.snapshot], storage: Arc::clone(database.storage()) };
                let context = self.scoped_wire_context(call, snapshot, clock)?
                    .with_wire_cut(cut, clock.into()).map_err(error::anomaly)?;
                (context, clock)
            } else {
                let clock = if exact { cut } else { clock };
                let context = self.wire_context(call, &database, false).await?
                    .with_wire_snapshot(view.snapshot, cut, clock.into()).map_err(error::anomaly)?;
                (context, clock)
            };
            let page = PageToken { basis: view.cut.basis, clock, offset, request: request_hash };
            (context, page, cut, view.claim, view.custody_groups)
        } else {
            let minimum = match (continuation, selection) {
                (Some(page), _) => Some(tokens::read_basis(scope, &page.basis.into())?),
                (None, Some(Selection::AtLeast(basis))) => Some(tokens::read_basis(scope, basis)?),
                _ => None,
            };
            if let Some(minimum) = minimum.filter(|minimum| *minimum != Timestamp::MIN) {
                self.0.node.wire_await_time(
                    &call.database, call.session.caller.principal.as_ref(), minimum, MAX_WAIT, &call.cancellation,
                ).await.map_err(error::anomaly)?;
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
                            let cut = tokens::read_basis(scope, basis)?;
                            (cut, cut)
                        }
                        _ => (present, now),
                    };
                    PageToken {
                        basis: tokens::basis(scope, cut),
                        clock,
                        offset: 0,
                        request: request_hash,
                    }
                }
            };
            let cut = tokens::read_basis(scope, &page.basis.into())?;
            if cut > present {
                return Err(error::unavailable(
                    "Database has not applied the requested basis",
                ));
            }
            let context = context
                .with_wire_cut(cut, page.clock.into())
                .map_err(error::anomaly)?;
            (context, page, cut, None, BTreeSet::new())
        };
        let parameters = request
            .params_ipc
            .map(|bytes| {
                Parameters::read(Buffer::from(call.retain_request(bytes)), &context.memory)
            })
            .transpose()?;
        let types = parameters
            .as_ref()
            .map_or(&[][..], |params| params.types.as_slice());
        let prepared = self
            .0
            .v1
            .plans
            .prepare(scope, &request.sql, &context, types)?;
        let (cursor, footprint) = open_query(prepared, context, parameters.as_ref(), request.track).await?;
        let claim = if visibility.is_some() && request.track {
            let claim = scoped_claim.ok_or_else(|| error::unsupported("query.track"))?;
            Some(proto::Claim { value: claim.into_bytes() })
        } else if request.track {
            let issuer = issuer.as_ref().ok_or_else(|| error::unsupported("query.track"))?;
            let evidence = ClaimEvidence {
                scope: ClaimScope { branch: source.branch.clone(), basis: page.basis, layer: claim_layer },
                footprint,
            };
            let value = issuer.seal_claim(&evidence).await.map_err(error::anomaly)?.into_bytes();
            Some(proto::Claim { value })
        } else { None };
        let trailer = proto::QueryTrailer {
            basis: Some(page.basis.into()),
            system_time_micros: cut.0,
            claim,
            next: None,
        };
        Ok(OpenedQuery { cursor, database, issuer, labels: visibility.map(|scope| scope.labels), page, limit, trailer, custody_groups })
    }
}

pub(super) async fn check_custody(call: &Call, database: &Database, groups: &BTreeSet<CustodyGroupId>) -> Result<(), Status> {
    if groups.is_empty() { return Ok(()); }
    let driver = database.serving_driver().ok_or_else(|| error::unsupported("row-custody"))?;
    driver.check_custody_delivery(groups, &call.cancellation).await.map_err(error::anomaly)
}

fn applied_source(
    source: &proto::Source,
    layer: Layer,
    selection: Option<&Selection>,
    continuation: Option<PageToken>,
    now: Timestamp,
) -> Result<(QuerySource, Timestamp, u64), Status> {
    let read_basis = |basis: &proto::Basis| Basis::from_bytes(&basis.value).map_err(error::anomaly);
    let (at, clock, offset) = match continuation {
        Some(page) => (At::Exactly(page.basis), page.clock, page.offset),
        None => match selection {
            None | Some(Selection::Latest(_)) => (At::Latest, now, 0),
            Some(Selection::AtLeast(basis)) => (At::AtLeast(read_basis(basis)?), now, 0),
            Some(Selection::Exactly(basis)) => (At::Exactly(read_basis(basis)?), now, 0),
            Some(Selection::Linearizable(_)) => (At::Linearizable, now, 0),
            Some(Selection::AsOfMicros(time)) => (At::AsOf(Timestamp(*time)), Timestamp(*time), 0),
        },
    };
    Ok((
        QuerySource {
            branch: source.branch.clone(),
            at,
            layer,
        },
        clock,
        offset,
    ))
}

struct Parameters {
    batch: rtdb_columnar::OwnedBatch,
    types: wire::ParameterTypes,
}

impl Parameters {
    fn read(bytes: Buffer, memory: &Arc<MemoryBudget>) -> Result<Self, Status> {
        let stream =
            rtdb_format::ipc::OwnedStream::read_buffer(bytes, memory).map_err(error::anomaly)?;
        let [batch] = stream.pages() else {
            return Err(error::incorrect(
                "Parameters require exactly one Arrow batch",
            ));
        };
        if batch.batch().num_rows() != 1 {
            return Err(error::incorrect("Parameters require exactly one row"));
        }
        let types = wire::ParameterTypes::from_batch(batch.batch(), memory)
            .map_err(error::map_status)?;
        Ok(Self {
            batch: batch.clone(),
            types,
        })
    }

    fn can_bind_arrow(&self) -> bool {
        let batch = self.batch.batch();
        !batch.columns().is_empty()
            && batch
                .schema_ref()
                .fields()
                .iter()
                .zip(batch.columns())
                .all(|(field, column)| {
                    field.metadata().is_empty()
                        && matches!(
                            field.data_type(),
                            DataType::Boolean
                                | DataType::Int8
                                | DataType::Int16
                                | DataType::Int32
                                | DataType::Int64
                        )
                        && column.is_valid(0)
                })
    }
}

async fn open_query(
    prepared: PreparedSql,
    context: Arc<QueryContext>,
    parameters: Option<&Parameters>,
    track: bool,
) -> Result<(QueryCursor, ClaimFootprint), Status> {
    let (row, types, _values) = if let Some(parameters) = parameters {
        let arguments = wire::arguments(&parameters.batch, &context.memory).map_err(error::map_status)?;
        if !track && parameters.can_bind_arrow()
            && let Some(cursor) = prepared
                .try_open_query_arrow(Arc::clone(&context), &parameters.batch, types)
                .await
                .map_err(error::anomaly)?
        {
            return Ok((cursor, ClaimFootprint::default()));
        }
        let row = arguments.rows().next().ok_or_else(|| error::incorrect("Parameters require one row"))?
            .map_err(error::anomaly)?;
        let size = row.iter().try_fold(1024usize, |bytes, value| {
            wire::add(wire::add(bytes, std::mem::size_of::<rtdb_types::Value>())?, value.retained_bytes().map_err(error::anomaly)?)
        }).map_err(error::map_status)?;
        let values = context.memory.reserve(size).map_err(error::anomaly)?;
        (row.to_vec(), parameters.types.as_slice(), Some(values))
    } else { (Vec::new(), &[][..], None) };
    if track {
        prepared.open_tracked_query_columns(context, row, types).await.map_err(error::anomaly)
    } else {
        let cursor = prepared.open_query_columns_typed(context, row, types).await.map_err(error::anomaly)?;
        Ok((cursor, ClaimFootprint::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, UInt64Array};

    #[test]
    fn arrow_execution_requires_present_plain_booleans_and_signed_integers() {
        let memory = MemoryBudget::new(1 << 20);
        let signed = Arc::new(Int64Array::from(vec![Some(42)])) as ArrayRef;
        for (other, expected) in [
            (Arc::new(BooleanArray::from(vec![true])) as ArrayRef, true),
            (Arc::new(Int64Array::from(vec![None])) as ArrayRef, false),
            (Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef, false),
        ] {
            let batch = RecordBatch::try_from_iter([("a", signed.clone()), ("b", other)]).unwrap();
            let ipc = rtdb_format::ipc::write_stream(batch.schema_ref(), std::slice::from_ref(&batch))
                .unwrap();
            let parameters = Parameters::read(Buffer::from_vec(ipc), &memory).unwrap();
            assert_eq!(parameters.can_bind_arrow(), expected);
        }
    }

    #[test]
    fn parameter_values_keep_their_input_allocation_and_both_charges() {
        let batch =
            RecordBatch::try_from_iter([("p", Arc::new(Int64Array::from(vec![42])) as ArrayRef)])
                .unwrap();
        let bytes = rtdb_format::ipc::write_stream(batch.schema_ref(), std::slice::from_ref(&batch))
            .unwrap();
        let start = bytes.as_ptr() as usize;
        let input = start..start + bytes.len();
        let input_memory = MemoryBudget::new(1 << 20);
        let decode_memory = MemoryBudget::new(1 << 20);
        let bytes = wire::retain(bytes, input_memory.reserve(4096).unwrap());
        let parameters = Parameters::read(Buffer::from(bytes), &decode_memory).unwrap();
        let array = Arc::clone(parameters.batch.batch().column(0));
        drop(parameters);

        let values = array.as_any().downcast_ref::<Int64Array>().unwrap().values();
        assert!(input.contains(&(values.as_ptr() as usize)));
        assert_eq!(values.as_ref(), &[42]);
        assert_eq!(input_memory.used_bytes(), 4096);
        assert!(decode_memory.used_bytes() > 0);
        drop(array);
        assert_eq!(input_memory.used_bytes(), 0);
        assert_eq!(decode_memory.used_bytes(), 0);
    }
}
