use std::{
    collections::VecDeque,
    mem::size_of,
    sync::{Arc, Mutex},
};

use arrow_array::{
    Array, ArrayRef, RecordBatch,
    cast::AsArray,
    types::{
        ArrowPrimitiveType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type,
        UInt32Type, UInt64Type,
    },
};
use arrow_buffer::{Buffer, ScalarBuffer};
use arrow_flight::FlightData;
use arrow_schema::{DataType, Field, Schema};
use prost::Message;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_node::{Database, PreparedSql, ResultSignaturePolicy};
use rtdb_query::{QueryContext, QueryCursor};
use rtdb_types::{LogicalType, Timestamp, Value};
use sha2::{Digest, Sha256};
use tonic::{Response, Status};

use crate::{
    service::{Call, Output, Service},
    wire,
};

use super::{
    MAX_PAGE_ROWS, MAX_WAIT, error, head,
    proto::{self, at::Selection},
    tokens::{self, Continuation, cursor_expired},
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
            let minimum = tokens::read_basis(scope, basis)?;
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
                        let cut = tokens::read_basis(scope, basis)?;
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
            .map(|bytes| {
                Parameters::read(Buffer::from(call.retain_request_bytes(bytes)), &context.memory)
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
        let cursor = open_query(prepared, context, parameters.as_ref()).await?;
        let basis = tokens::basis(scope, page.cut);
        let reply = proto::QueryTrailer {
            basis: Some(basis.into()),
            system_time_micros: page.cut.0,
            claim: request.track.then(|| tokens.claim(basis)).transpose()?,
            next: None,
        };
        Ok((cursor, database, page, limit, reply))
    }
}

struct Parameters {
    batch: OwnedBatch,
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
        let types =
            wire::ParameterTypes::from_batch(batch.batch(), memory).map_err(error::map_status)?;
        Ok(Self {
            batch: batch.clone(),
            types,
        })
    }

    fn binds_arrow_without_row_admission(&self) -> bool {
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
                                | DataType::Float32
                                | DataType::Float64
                        )
                        && column.is_valid(0)
                })
    }

    fn can_bind_arrow(&self) -> bool {
        let batch = self.batch.batch();
        batch.num_columns() != 0
            && batch
                .schema_ref()
                .fields()
                .iter()
                .zip(batch.columns())
                .zip(self.types.as_slice())
                .all(|((field, array), logical)| match field.data_type() {
                    DataType::Union(..) | DataType::RunEndEncoded(..) => false,
                    DataType::List(child) => {
                        plain_float_list(field, child)
                            && array.is_valid(0)
                            && array.as_list::<i32>().value_length(0) > 0
                    }
                    DataType::FixedSizeList(child, width) => {
                        plain_float_list(field, child) && array.is_valid(0) && *width > 0
                    }
                    _ => match logical {
                        LogicalType::Scalar(_) | LogicalType::Null => true,
                        LogicalType::Maybe(inner) => {
                            matches!(inner.as_ref(), LogicalType::Scalar(_))
                        }
                        _ => false,
                    },
                })
    }

    fn reserve_scalar_row_bound(
        &self,
        memory: &Arc<MemoryBudget>,
    ) -> Result<Arc<MemoryReservation>, Status> {
        let batch = self.batch.batch();
        let base = wire::add(
            1024,
            wire::mul(batch.num_columns(), 3 * size_of::<Value>())?,
        )?;
        let bound = batch
            .schema_ref()
            .fields()
            .iter()
            .zip(batch.columns())
            .try_fold(base, |bytes, (field, array)| {
                let read = rtdb_columnar::estimate_read_value_bytes(field, array.as_ref(), 0)
                    .map_err(error::anomaly)?;
                let copy = rtdb_columnar::estimate_read_cell_bytes(field, array.as_ref(), 0)
                    .map_err(error::anomaly)?;
                wire::add(bytes, wire::add(read, copy)?)
            })?;
        memory.reserve(bound).map_err(error::anomaly)
    }

    async fn try_open_arrow(
        &self,
        prepared: &PreparedSql,
        context: &Arc<QueryContext>,
    ) -> Result<Option<QueryCursor>, Status> {
        let types = self.types.as_slice();
        let skip_row_admission = self.binds_arrow_without_row_admission();
        // Use the scalar path to report its original error if its eager row admission may fail.
        let _scalar_row = if skip_row_admission {
            None
        } else {
            if prepared.parameter_count() != types.len() || !self.can_bind_arrow() {
                return Ok(None);
            }
            let Ok(row) = self.reserve_scalar_row_bound(&context.memory) else {
                return Ok(None);
            };
            Some(row)
        };
        let result = if skip_row_admission {
            prepared
                .try_open_query_arrow(Arc::clone(context), &self.batch, types)
                .await
        } else {
            let batch = self.batch.batch();
            let signed = if batch
                .schema_ref()
                .fields()
                .iter()
                .any(|field| field.data_type().is_unsigned_integer())
            {
                let Ok(metadata) = wire::schema_bound(batch.schema_ref())
                    .and_then(|bytes| context.memory.reserve(bytes).map_err(error::anomaly))
                else {
                    return Ok(None);
                };
                let Some(signed) = self.signed_batch() else {
                    return Ok(None);
                };
                Some(signed.with_schema_reservation(metadata))
            } else {
                None
            };
            // The prepared plan keeps the declared types. Where it has no declaration, the
            // cursor falls back to the same observed type it would get from a decoded value.
            let observed = types
                .iter()
                .zip(batch.columns())
                .map(|(logical, array)| {
                    if array.is_null(0) {
                        LogicalType::Null
                    } else {
                        match logical {
                            LogicalType::Maybe(inner) => rtdb_columnar::execution_type(inner),
                            other => rtdb_columnar::execution_type(other),
                        }
                    }
                })
                .collect::<Vec<_>>();
            prepared
                .try_open_query_arrow(
                    Arc::clone(context),
                    signed.as_ref().unwrap_or(&self.batch),
                    &observed,
                )
                .await
        };
        match result {
            Err(error)
                if error.code == "rtdb.query/column-plan"
                    || (!skip_row_admission && error.code == "rtdb/memory-budget-exceeded") =>
            {
                Ok(None)
            }
            result => result.map_err(error::anomaly),
        }
    }

    fn signed_batch(&self) -> Option<OwnedBatch> {
        let batch = self.batch.batch();
        let mut fields = batch.schema_ref().fields().to_vec();
        let mut arrays = batch.columns().to_vec();
        for (field, array) in fields.iter_mut().zip(&mut arrays) {
            let signed: ArrayRef = match field.data_type() {
                DataType::UInt8 => signed_view::<UInt8Type, Int8Type>(array),
                DataType::UInt16 => signed_view::<UInt16Type, Int16Type>(array),
                DataType::UInt32 => signed_view::<UInt32Type, Int32Type>(array),
                DataType::UInt64 => signed_view::<UInt64Type, Int64Type>(array),
                _ => continue,
            };
            *field = Arc::new(
                field
                    .as_ref()
                    .clone()
                    .with_data_type(signed.data_type().clone()),
            );
            *array = signed;
        }
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            batch.schema_ref().metadata().clone(),
        ));
        let signed = RecordBatch::try_new(schema, arrays).ok()?;
        Some(self.batch.with_retained_buffers(signed))
    }
}

fn signed_view<Unsigned: ArrowPrimitiveType, Signed: ArrowPrimitiveType>(
    array: &ArrayRef,
) -> ArrayRef {
    let array = array.as_primitive::<Unsigned>();
    let values = ScalarBuffer::from(array.values().inner().clone());
    Arc::new(arrow_array::PrimitiveArray::<Signed>::new(
        values,
        array.nulls().cloned(),
    ))
}

fn plain_float_list(field: &Field, child: &Field) -> bool {
    field.metadata().is_empty()
        && child.metadata().is_empty()
        && child.name() == "$data$"
        && !child.is_nullable()
        && matches!(child.data_type(), DataType::Float32 | DataType::Float64)
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
    if let Some(cursor) = parameters.try_open_arrow(&prepared, &context).await? {
        return Ok(cursor);
    }
    let row = arguments
        .rows()
        .next()
        .ok_or_else(|| error::incorrect("Parameters require one row"))?
        .map_err(error::anomaly)?;
    let size = row
        .iter()
        .try_fold(1024usize, |bytes, value| {
            wire::add(
                wire::add(bytes, size_of::<Value>())?,
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        BinaryArray, BooleanArray, FixedSizeListArray, Float32Array, Float64Array, Int64Array,
        ListArray, NullArray, StringArray, UInt64Array,
    };
    use arrow_buffer::OffsetBuffer;

    #[test]
    fn arrow_binding_accepts_scalar_and_type_preserving_float_list_profiles() {
        let memory = MemoryBudget::new(1 << 20);
        let signed = Arc::new(Int64Array::from(vec![Some(42)])) as ArrayRef;
        let item = Arc::new(Field::new("$data$", DataType::Float32, false));
        let list = |child, components: Vec<f32>| -> ArrayRef {
            let end = i32::try_from(components.len()).unwrap();
            Arc::new(ListArray::new(
                child,
                OffsetBuffer::new(ScalarBuffer::from(vec![0, end])),
                Arc::new(Float32Array::from(components)),
                None,
            ))
        };
        let fixed = Arc::new(FixedSizeListArray::new(
            Arc::clone(&item),
            2,
            Arc::new(Float32Array::from(vec![-0.0, 1.0])),
            None,
        )) as ArrayRef;
        for (other, annotated, expected) in [
            (
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
                false,
                true,
            ),
            (
                Arc::new(Float32Array::from(vec![-0.0])) as ArrayRef,
                false,
                true,
            ),
            (
                Arc::new(Float64Array::from(vec![f64::NAN])) as ArrayRef,
                false,
                true,
            ),
            (
                Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
                true,
                true,
            ),
            (
                Arc::new(Float32Array::from(vec![None])) as ArrayRef,
                false,
                true,
            ),
            (
                Arc::new(Int64Array::from(vec![None])) as ArrayRef,
                false,
                true,
            ),
            (Arc::new(NullArray::new(1)) as ArrayRef, false, true),
            (
                Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef,
                false,
                true,
            ),
            (
                Arc::new(StringArray::from(vec!["text"])) as ArrayRef,
                false,
                true,
            ),
            (
                Arc::new(BinaryArray::from(vec![&b"\0\xff"[..]])) as ArrayRef,
                false,
                true,
            ),
            (list(Arc::clone(&item), vec![-0.0, 1.0]), false, true),
            (fixed, false, true),
            (list(Arc::clone(&item), vec![]), false, false),
            (
                list(
                    Arc::new(item.as_ref().clone().with_nullable(true)),
                    vec![1.0],
                ),
                false,
                false,
            ),
            (list(item, vec![1.0]), true, false),
        ] {
            let mut field = Field::new("b", other.data_type().clone(), true);
            if annotated {
                field = field.with_metadata([("tag".into(), "parameter".into())].into());
            }
            let schema = Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                field,
            ]));
            let batch = RecordBatch::try_new(schema, vec![Arc::clone(&signed), other]).unwrap();
            let ipc =
                rtdb_format::ipc::write_stream(batch.schema_ref(), std::slice::from_ref(&batch))
                    .unwrap();
            let parameters = Parameters::read(Buffer::from_vec(ipc), &memory).unwrap();
            assert_eq!(parameters.can_bind_arrow(), expected);
        }
    }

    #[test]
    fn unsigned_views_keep_the_ipc_values_and_validity() {
        let memory = MemoryBudget::new(1 << 20);
        for value in [Some(u64::MAX), None] {
            let source = Arc::new(UInt64Array::from(vec![value])) as ArrayRef;
            let batch = RecordBatch::try_from_iter([("u", source)]).unwrap();
            let bytes =
                rtdb_format::ipc::write_stream(batch.schema_ref(), std::slice::from_ref(&batch))
                    .unwrap();
            let parameters = Parameters::read(Buffer::from_vec(bytes), &memory).unwrap();
            let signed = parameters.signed_batch().unwrap();
            let source = parameters
                .batch
                .batch()
                .column(0)
                .as_primitive::<UInt64Type>();
            let array = signed.batch().column(0).as_primitive::<Int64Type>();
            assert_eq!(
                source.values().as_ptr().cast::<i64>(),
                array.values().as_ptr()
            );
            assert_eq!(array.iter().next(), Some(value.map(|value| value as i64)));
            assert_eq!(
                parameters.batch.batch().schema_ref().field(0).data_type(),
                &DataType::UInt64
            );
        }
    }

    #[test]
    fn direct_binding_checks_eager_payload_bounds_without_retaining_row_memory() {
        let memory = MemoryBudget::new(1 << 22);
        let text = "x".repeat(1 << 14);
        let batch = RecordBatch::try_from_iter([(
            "p",
            Arc::new(StringArray::from(vec![text.as_str()])) as ArrayRef,
        )])
        .unwrap();
        let ipc = rtdb_format::ipc::write_stream(batch.schema_ref(), std::slice::from_ref(&batch))
            .unwrap();
        let parameters = Parameters::read(Buffer::from_vec(ipc), &memory).unwrap();
        let small = MemoryBudget::new(1 << 14);
        assert!(parameters.reserve_scalar_row_bound(&small).is_err());
        assert_eq!(small.used_bytes(), 0);
        let before = memory.used_bytes();
        let reserved = parameters.reserve_scalar_row_bound(&memory).unwrap();
        assert!(reserved.bytes() > text.len());
        drop(reserved);
        assert_eq!(memory.used_bytes(), before);
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
