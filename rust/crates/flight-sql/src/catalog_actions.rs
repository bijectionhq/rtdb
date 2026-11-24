use std::{fmt::Write, sync::Arc, time::Duration};

use arrow_array::{
    Array, BinaryArray, DictionaryArray, Int32Array, Int64Array, ListArray, RecordBatch,
    StringArray, types::Int32Type,
};
use arrow_buffer::Buffer;
use arrow_flight::Result as ActionResult;
use arrow_ipc::writer::StreamWriter;
use bytes::Bytes;
use prost::Message;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_format::ipc::OwnedStream;
use rtdb_tx::{SqlArguments, TxOp};
use rtdb_types::{LogicalType, ScalarType, TableRef, Value};
use tonic::{Response, Status};

use crate::{
    error::{arrow, cancelled, invalid, status, unsupported},
    service::{Call, Output, Service},
    wire,
};

pub(crate) const PUBLISH_CATALOG: &str = "rtdb.owned.PublishCatalog.v1";
pub(crate) const SCAN_CATALOG: &str = "rtdb.owned.ScanCatalog.v1";
pub(crate) const WATCH_RELATION: &str = "rtdb.owned.WatchRelation.v1";
const MAX_BODY: usize = 32 << 20;
const INSERT_ROWS: usize = 128;
const WATCH_SPAN: Duration = Duration::from_secs(20);

#[derive(Message)]
struct Publication {
    #[prost(message, repeated, tag = "1")]
    before: Vec<Statement>,
    #[prost(bytes = "bytes", tag = "2")]
    rows: Bytes,
    #[prost(message, optional, tag = "3")]
    marker: Option<Statement>,
}

#[derive(Message)]
struct Statement {
    #[prost(string, tag = "1")]
    sql: String,
    #[prost(bytes = "bytes", tag = "2")]
    args: Bytes,
}

#[derive(Message)]
struct Receipt {
    #[prost(int64, tag = "1")]
    tx_id: i64,
    #[prost(int64, tag = "2")]
    system_micros: i64,
    #[prost(string, tag = "3")]
    await_token: String,
    #[prost(bool, tag = "4")]
    committed: bool,
    #[prost(string, tag = "5")]
    error_code: String,
    #[prost(string, tag = "6")]
    error_message: String,
}

#[derive(Message)]
struct Watch {
    #[prost(string, tag = "1")]
    relation: String,
    #[prost(int64, optional, tag = "2")]
    after: Option<i64>,
}

impl Service {
    pub(crate) async fn publish_catalog_action(
        &self,
        call: &Call,
        body: Bytes,
    ) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions {
            return Err(unsupported());
        }
        if body.len() > MAX_BODY {
            return Err(Status::resource_exhausted(
                "Catalog publication exceeds its Flight request limit",
            ));
        }
        let _request = call.reserve(wire::add(wire::mul(body.len(), 2)?, 4096)?)?;
        let request = Publication::decode(body).map_err(arrow)?;
        let connection = self.connection(call, &call.database).await?;
        let mut native = connection.borrow().await?;
        let context = native
            .prepare_context(&call.cancellation)
            .await
            .map_err(status)?;
        let options = crate::execute::options(&native);
        let prepare = |statement: Statement| -> Result<TxOp, Status> {
            let args = if statement.args.is_empty() {
                None
            } else {
                let stream = OwnedStream::read_buffer(Buffer::from(statement.args), call.memory())
                    .map_err(status)?;
                let [batch] = stream.pages() else {
                    return Err(invalid("Expected one SQL argument batch"));
                };
                if batch.batch().num_rows() != 1 {
                    return Err(invalid("Expected one SQL argument row"));
                }
                Some(
                    SqlArguments::from_arrow_with_canonical_source(batch.clone(), call.memory())
                        .map_err(status)?,
                )
            };
            let statement = rtdb_node::PreparedSql::prepare(
                &statement.sql,
                &context,
                options.clone(),
                rtdb_node::ResultSignaturePolicy::Dynamic,
            )
            .map_err(status)?;
            statement.dml_operation(args).map_err(status)
        };
        let row_operations = if request.rows.is_empty() {
            None
        } else {
            let stream = OwnedStream::read_buffer(Buffer::from(request.rows), call.memory())
                .map_err(status)?;
            let [batch] = stream.pages() else {
                return Err(invalid("Expected one catalog write batch"));
            };
            Some(catalog_inserts(batch.batch(), call.memory())?)
        };
        let mut operations = request
            .before
            .into_iter()
            .map(&prepare)
            .collect::<Result<Vec<_>, _>>()?;
        let _write_sql = row_operations.map(|(writes, charge)| {
            operations.extend(writes);
            charge
        });
        operations.extend(request.marker.map(prepare).transpose()?);
        let result = native
            .state_mut()
            .execute_tx(operations, Default::default())
            .await
            .map_err(status)?;
        let key = result.tx_key();
        let error = result.error();
        let receipt = Receipt {
            tx_id: key.tx_id.0,
            system_micros: key.system_time.0,
            await_token: native
                .state()
                .last_submitted_await_token()
                .ok_or_else(|| Status::internal("Missing catalog await token"))?
                .to_owned(),
            committed: result.committed(),
            error_code: error.map_or_else(String::new, |error| error.code.clone()),
            error_message: error
                .and_then(|error| error.message.clone())
                .unwrap_or_default(),
        };
        let charge = call.reserve(wire::add(receipt.encoded_len(), 256)?)?;
        Ok(ActionResult {
            body: wire::retain(receipt.encode_to_vec(), charge),
        })
    }

    pub(crate) async fn scan_catalog_action(
        &self,
        call: Call,
        body: Bytes,
    ) -> Result<Response<Output<ActionResult>>, Status> {
        let open = async {
            if body.len() > MAX_BODY {
                return Err(Status::resource_exhausted("Catalog scan exceeds its request limit"));
            }
            let _request = call.reserve(wire::add(wire::mul(body.len(), 2)?, 4096)?)?;
            let Statement { sql, args } = Statement::decode(body).map_err(arrow)?;
            let stream = OwnedStream::read_buffer(Buffer::from(args), call.memory()).map_err(status)?;
            let [batch] = stream.pages() else {
                return Err(invalid("Expected one catalog scan argument batch"));
            };
            if batch.batch().num_rows() != 1
                || batch.batch().columns().first()
                    .and_then(|arg| arg.as_any().downcast_ref::<StringArray>())
                    .is_none_or(|tokens| tokens.is_null(0) || tokens.value(0).is_empty())
            {
                return Err(invalid("A catalog scan requires the original SQL snapshot token"));
            }
            let arguments = SqlArguments::from_arrow(batch.clone(), call.memory()).map_err(status)?;
            let connection = self.connection(&call, &call.database).await?;
            let mut native = connection.borrow().await?;
            let context = native.query_context_with(&call.cancellation, true).await.map_err(status)?;
            let source = format!("SETTING SNAPSHOT_TOKEN = $1 {sql}");
            let prepared = rtdb_node::PreparedSql::prepare(
                &source, &context, crate::execute::options(&native),
                rtdb_node::ResultSignaturePolicy::Dynamic,
            ).map_err(status)?;
            if prepared.is_dml() || prepared.parameter_count() != batch.batch().num_columns() {
                return Err(invalid("Expected a bound catalog read"));
            }
            if let Some(cursor) = prepared.try_open_query_arrow(
                Arc::clone(&context), batch, arguments.types(),
            ).await.map_err(status)? {
                return Ok(cursor);
            }
            let row = arguments.rows().next().ok_or_else(|| invalid("Missing catalog scan operands"))?.map_err(status)?;
            let _values = row.iter().try_fold(4096, |size, value| {
                wire::add(size, value.retained_bytes().map_err(status)?)
            }).and_then(|size| call.reserve(size))?;
            prepared.open_query_columns_typed(context, row.to_vec(), arguments.types()).await.map_err(status)
        };
        let mut cursor = call.cancellation.run_until_cancelled(open)
            .await.ok_or_else(cancelled).and_then(|result| result).map_err(|error| call.error(error))?;
        let memory = Arc::clone(call.memory());
        let schema = wire::plan_schema(cursor.runtime_schema(), &memory)?;
        let schema_bound = wire::schema_bound(&schema)?;
        let schema_charge = call.reserve(schema_bound)?;
        let mut writer = StreamWriter::try_new(Vec::new(), &schema).map_err(arrow)?;
        let first = cursor.delivery_grant().await.map_err(status)?;
        let (mut headers, cutoff) = call.delivery_headers(&first, true)?;
        headers.insert(crate::actions::OWNED_IPC_HEADER, tonic::metadata::MetadataValue::from_static("stream-v1"));
        let max_message = self.0.config.maximum_message_bytes;
        let output: Output<ActionResult> = Box::pin(async_stream::try_stream! {
            let mut left = MAX_BODY;
            let schema_bytes = std::mem::take(writer.get_mut());
            left = left.checked_sub(schema_bytes.len())
                .ok_or_else(|| Status::resource_exhausted("Catalog scan schema exceeds its reply bound"))?;
            let schema_frame = ActionResult { body: wire::retain(schema_bytes, schema_charge) };
            if schema_frame.encoded_len() > max_message {
                Err(Status::resource_exhausted("Catalog scan schema exceeds the Flight message limit"))?;
            }
            call.handoff_action(first, cutoff)?;
            yield schema_frame;
            loop {
                let next = cursor.encode_next_batch(|batch| {
                    let batch = rtdb_columnar::conform_batch(batch.clone(), &schema, &memory).map_err(status)?;
                    let bytes = wire::mul(batch.batch().get_array_memory_size(), 6)?;
                    let charge = call.reserve(wire::add(bytes, schema_bound)?)?;
                    writer.write(batch.batch()).map_err(arrow)?;
                    Ok::<_, Status>(wire::retain(std::mem::take(writer.get_mut()), charge))
                });
                let Some(encoded) = call.cancellation.run_until_cancelled(next)
                    .await.ok_or_else(cancelled)?.map_err(status)? else { break };
                let encoded = encoded.inspect_err(|_| cursor.record_consumer_error())?;
                left = left.checked_sub(encoded.len())
                    .ok_or_else(|| Status::resource_exhausted("Catalog scan exceeds its reply bound"))?;
                for frame in crate::actions::IpcFrames(encoded) {
                    let frame = frame?;
                    if frame.encoded_len() > max_message {
                        Err(Status::resource_exhausted("Catalog scan frame exceeds the Flight message limit"))?;
                    }
                    call.handoff_action(cursor.delivery_grant().await.map_err(status)?, cutoff)?;
                    yield frame;
                }
            }
            writer.finish().map_err(arrow)?;
            if writer.get_ref().len() > left {
                Err(Status::resource_exhausted("Catalog scan exceeds its reply bound"))?;
            }
            let charge = call.reserve(256)?;
            call.handoff_action(cursor.delivery_grant().await.map_err(status)?, cutoff)?;
            yield ActionResult { body: wire::retain(std::mem::take(writer.get_mut()), charge) };
        });
        let mut response = Response::new(output);
        *response.metadata_mut() = headers;
        Ok(response)
    }

    pub(crate) async fn watch_relation_action(
        &self,
        call: &Call,
        body: Bytes,
    ) -> Result<ActionResult, Status> {
        if !self.0.config.owned_native_actions {
            return Err(unsupported());
        }
        if body.len() > 256 {
            return Err(invalid("Invalid relation watch request size"));
        }
        let request = Watch::decode(body).map_err(arrow)?;
        let table = TableRef::parse(&request.relation).map_err(status)?;
        if table.schema != "public" || request.after.is_some_and(|after| after < 0) {
            return Err(invalid("Invalid public relation watch"));
        }
        let (database, attachment) = call
            .session
            .resolve_database(Some(&call.database), &self.0.node)?;
        let database = match attachment {
            Some(attachment) => attachment,
            None => self.0.node.catalog().database(&database).map_err(status)?,
        };
        let native = call.session.open_native(&call.database, &self.0.node)?;
        native.state().revalidate().await.map_err(status)?;
        let count = call
            .cancellation
            .run_until_cancelled(database.wait_for_relation_change(
                &table,
                request.after,
                WATCH_SPAN,
            ))
            .await
            .ok_or_else(cancelled)?
            .map_err(status)?;
        native.state().revalidate().await.map_err(status)?;
        call.session
            .resolve_database(Some(&call.database), &self.0.node)?;
        let charge = call.reserve(256)?;
        Ok(ActionResult {
            body: wire::retain(count.to_be_bytes().to_vec(), charge),
        })
    }
}

fn catalog_inserts(
    batch: &RecordBatch,
    memory: &Arc<MemoryBudget>,
) -> Result<(Vec<TxOp>, Arc<MemoryReservation>), Status> {
    if !batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .eq([
            "_id",
            "world",
            "key",
            "idx",
            "op",
            "value",
            "number",
            "label",
            "units",
            "stamp_offsets",
        ])
    {
        return Err(invalid("Unexpected catalog write columns"));
    }
    let ids = column::<StringArray>(batch, 0)?;
    let worlds = column::<Int64Array>(batch, 1)?;
    let keys = column::<BinaryArray>(batch, 2)?;
    let ordinals = column::<Int64Array>(batch, 3)?;
    let ops = column::<DictionaryArray<Int32Type>>(batch, 4)?;
    let values = column::<BinaryArray>(batch, 5)?;
    let numbers = column::<Int64Array>(batch, 6)?;
    let labels = column::<DictionaryArray<Int32Type>>(batch, 7)?;
    let units = column::<ListArray>(batch, 8)?;
    let stamps = column::<ListArray>(batch, 9)?;
    let op_values = ops
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid("Invalid catalog operation dictionary"))?;
    let label_values = labels
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid("Invalid catalog label dictionary"))?;
    let unit_values = units
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid("Invalid catalog unit list"))?;
    let stamp_values = stamps
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| invalid("Invalid catalog stamp list"))?;
    if [
        ids.null_count(),
        worlds.null_count(),
        keys.null_count(),
        ordinals.null_count(),
        ops.null_count(),
        op_values.null_count(),
        labels.null_count(),
        label_values.null_count(),
        units.null_count(),
        unit_values.null_count(),
        stamps.null_count(),
        stamp_values.null_count(),
    ]
    .iter()
    .any(|count| *count != 0)
    {
        return Err(invalid("Null catalog write coordinate"));
    }

    let sql_bytes = wire::add(
        wire::mul(batch.num_rows(), 256)?,
        wire::mul(stamp_values.len(), 128)?,
    )?;
    let sql_charge = memory
        .reserve(wire::add(sql_bytes, 4096)?)
        .map_err(status)?;
    let text = LogicalType::Scalar(ScalarType::Utf8);
    let integer = LogicalType::Scalar(ScalarType::I64);
    let types = [
        text.clone(),
        integer.clone(),
        text.clone(),
        integer.clone(),
        text.clone(),
        LogicalType::Scalar(ScalarType::Binary).maybe(),
        integer.maybe(),
        text.clone(),
        LogicalType::list(text),
    ];
    let mut operations = Vec::new();
    for start in (0..batch.num_rows()).step_by(INSERT_ROWS) {
        let mut sql = String::from(
            "INSERT INTO bj_kernel_writes (_id, world, key, idx, op, value, number, label, units, _valid_from) VALUES ",
        );
        let end = (start + INSERT_ROWS).min(batch.num_rows());
        let bytes = (start..end).try_fold(4096, |total, row| {
            let strings = [
                ids.value(row).len(),
                wire::mul(keys.value(row).len(), 2)?,
                op_values.value(ops.keys().value(row) as usize).len(),
                label_values.value(labels.keys().value(row) as usize).len(),
                if values.is_null(row) {
                    0
                } else {
                    values.value(row).len()
                },
            ];
            let bytes = strings
                .into_iter()
                .try_fold(wire::add(total, 2048)?, wire::add)?;
            list_range(units, row).try_fold(bytes, |bytes, index| {
                wire::add(bytes, wire::add(unit_values.value(index).len(), 128)?)
            })
        })?;
        let _args = memory.reserve(bytes).map_err(status)?;
        let mut args = Vec::with_capacity((end - start) * types.len());
        for row in start..end {
            let first = args.len() + 1;
            if row != start {
                sql.push_str(", ");
            }
            let op = op_values.value(ops.keys().value(row) as usize);
            let label = label_values.value(labels.keys().value(row) as usize);
            let mut key = String::with_capacity(keys.value(row).len() * 2);
            for byte in keys.value(row) {
                write!(key, "{byte:02x}").expect("String writes do not fail");
            }
            let units = list_range(units, row)
                .map(|i| Value::Utf8(unit_values.value(i).to_owned()))
                .collect();
            args.extend([
                Value::Utf8(ids.value(row).to_owned()),
                Value::I64(worlds.value(row)),
                Value::Utf8(key),
                Value::I64(ordinals.value(row)),
                Value::Utf8(op.to_owned()),
                if values.is_null(row) {
                    Value::Null
                } else {
                    Value::Binary(values.value(row).to_vec())
                },
                if numbers.is_null(row) {
                    Value::Null
                } else {
                    Value::I64(numbers.value(row))
                },
                Value::Utf8(label.to_owned()),
                Value::list(units),
            ]);

            write!(
                sql,
                "(${first}, ${}, ${}, ${}, ${}, ",
                first + 1,
                first + 2,
                first + 3,
                first + 4
            )
            .expect("String writes do not fail");
            let offsets = list_range(stamps, row);
            for _ in offsets.clone() {
                sql.push_str("OVERLAY(");
            }
            write!(sql, "${}", first + 5).expect("String writes do not fail");
            let mut previous = None;
            for stamp in offsets.map(|i| stamp_values.value(i)) {
                if values.is_null(row) || stamp <= 0 {
                    return Err(invalid("Invalid ordered catalog stamp"));
                }
                let offset = stamp as usize - 1;
                let end = offset + 8;
                if end > values.value(row).len() || previous.is_some_and(|previous| previous < end)
                {
                    return Err(invalid("Invalid ordered catalog stamp"));
                }
                previous = Some(offset);
                write!(
                    sql,
                    " PLACING EPOCH_NANOS_BE(CURRENT_TIMESTAMP) FROM {stamp} FOR 8)"
                )
                .expect("String writes do not fail");
            }
            write!(
                sql,
                ", ${}, ${}, ${}, TIMESTAMP WITH TIME ZONE '1970-01-01 00:00:00+00:00')",
                first + 6,
                first + 7,
                first + 8
            )
            .expect("String writes do not fail");
        }
        let types = (start..end).flat_map(|_| types.iter().cloned()).collect();
        let args = SqlArguments::new(types, vec![args], memory).map_err(status)?;
        operations.push(TxOp::Sql {
            sql,
            args: Some(args),
        });
    }
    Ok((operations, sql_charge))
}

fn list_range(array: &ListArray, row: usize) -> std::ops::Range<usize> {
    let offsets = array.value_offsets();
    offsets[row] as usize..offsets[row + 1] as usize
}

fn column<T: 'static>(batch: &RecordBatch, index: usize) -> Result<&T, Status> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| invalid(format!("Invalid catalog column type at {index}")))
}
