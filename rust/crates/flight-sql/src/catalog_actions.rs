use std::{fmt::Write, sync::Arc, time::Duration};

use arrow_array::{
    Array, ArrayRef, BinaryArray, DictionaryArray, Int32Array, Int64Array, ListArray, RecordBatch,
    StringArray, types::Int32Type,
};
use arrow_buffer::Buffer;
use arrow_flight::Result as ActionResult;
use arrow_ipc::{
    MetadataVersion,
    writer::{CompressionContext, DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteOptions, write_message},
};
use arrow_schema::{DataType, Schema};
use bytes::Bytes;
use prost::Message;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch, to_field};
use rtdb_format::ipc::OwnedStream;
use rtdb_tx::{SqlArguments, TxOp};
use rtdb_types::{LogicalType, ScalarType, TableRef};
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
const IPC_ALIGNMENT: usize = 64;
const IPC_EOS: &[u8] = &[255, 255, 255, 255, 0, 0, 0, 0];

struct CatalogIpcEncoder {
    options: IpcWriteOptions,
    dictionaries: DictionaryTracker,
    compression: CompressionContext,
}

impl CatalogIpcEncoder {
    fn new(schema: &Schema, charge: Arc<MemoryReservation>) -> Result<(Self, ActionResult), Status> {
        let options = IpcWriteOptions::try_new(IPC_ALIGNMENT, false, MetadataVersion::V5).map_err(arrow)?;
        let mut dictionaries = DictionaryTracker::new(false);
        let message = IpcDataGenerator::default()
            .schema_to_bytes_with_dictionary_tracker(schema, &mut dictionaries, &options);
        let frame = catalog_ipc_frame(message, &options, charge)?;
        Ok((Self { options, dictionaries, compression: CompressionContext::default() }, frame))
    }

    fn page(&mut self, batch: &RecordBatch, charge: Arc<MemoryReservation>) -> Result<Vec<ActionResult>, Status> {
        let (dictionaries, record) = IpcDataGenerator::default()
            .encode(batch, &mut self.dictionaries, &self.options, &mut self.compression)
            .map_err(arrow)?;
        dictionaries.into_iter().chain([record]).map(|message| {
            catalog_ipc_frame(message, &self.options, Arc::clone(&charge))
        }).collect()
    }
}

fn catalog_ipc_frame(
    message: EncodedData, options: &IpcWriteOptions, charge: Arc<MemoryReservation>,
) -> Result<ActionResult, Status> {
    let metadata = wire::add(IPC_EOS.len(), message.ipc_message.len())?
        .checked_next_multiple_of(IPC_ALIGNMENT)
        .ok_or_else(|| invalid("Flight allocation size overflow"))?;
    let mut frame = Vec::with_capacity(wire::add(metadata, message.arrow_data.len())?);
    write_message(&mut frame, message, options).map_err(arrow)?;
    Ok(ActionResult { body: wire::retain(frame, charge) })
}

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
            Some(catalog_inserts(batch, call.memory())?)
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
        let schema = wire::plan_schema(cursor.runtime_schema(), &memory)?.into_declared();
        let schema_bound = wire::schema_bound(schema.schema())?;
        let schema_charge = call.reserve(schema_bound)?;
        let (mut encoder, schema_frame) = CatalogIpcEncoder::new(schema.schema(), schema_charge)?;
        let first = cursor.delivery_grant().await.map_err(status)?;
        let (mut headers, cutoff) = call.delivery_headers(&first, true)?;
        headers.insert(crate::actions::OWNED_IPC_HEADER, tonic::metadata::MetadataValue::from_static("stream-v1"));
        let max_message = self.0.config.maximum_message_bytes;
        let output: Output<ActionResult> = Box::pin(async_stream::try_stream! {
            let mut left = MAX_BODY;
            left = left.checked_sub(schema_frame.body.len())
                .ok_or_else(|| Status::resource_exhausted("Catalog scan schema exceeds its reply bound"))?;
            if schema_frame.encoded_len() > max_message {
                Err(Status::resource_exhausted("Catalog scan schema exceeds the Flight message limit"))?;
            }
            call.handoff_action(first, cutoff)?;
            yield schema_frame;
            loop {
                let next = cursor.encode_next_batch(|batch| {
                    let batch = schema.conform(batch.clone(), &memory).map_err(status)?;
                    let bytes = wire::mul(batch.batch().get_array_memory_size(), 6)?;
                    let charge = call.reserve(wire::add(bytes, schema_bound)?)?;
                    encoder.page(batch.batch(), charge)
                });
                let Some(encoded) = call.cancellation.run_until_cancelled(next)
                    .await.ok_or_else(cancelled)?.map_err(status)? else { break };
                let encoded = encoded.inspect_err(|_| cursor.record_consumer_error())?;
                let bytes = encoded.iter().try_fold(0, |bytes, frame| wire::add(bytes, frame.body.len()))?;
                left = left.checked_sub(bytes)
                    .ok_or_else(|| Status::resource_exhausted("Catalog scan exceeds its reply bound"))?;
                for frame in encoded {
                    if frame.encoded_len() > max_message {
                        Err(Status::resource_exhausted("Catalog scan frame exceeds the Flight message limit"))?;
                    }
                    call.handoff_action(cursor.delivery_grant().await.map_err(status)?, cutoff)?;
                    yield frame;
                }
            }
            if IPC_EOS.len() > left {
                Err(Status::resource_exhausted("Catalog scan exceeds its reply bound"))?;
            }
            let charge = call.reserve(256)?;
            call.handoff_action(cursor.delivery_grant().await.map_err(status)?, cutoff)?;
            yield ActionResult { body: wire::retain(Bytes::from_static(IPC_EOS), charge) };
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
    owner: &OwnedBatch,
    memory: &Arc<MemoryBudget>,
) -> Result<(Vec<TxOp>, Arc<MemoryReservation>), Status> {
    let batch = owner.batch();
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
        wire::mul(batch.num_rows(), 4096)?,
        wire::mul(stamp_values.len(), 128)?,
    )?;
    let sql_charge = memory
        .reserve(wire::add(sql_bytes, 4096)?)
        .map_err(status)?;
    let text = LogicalType::Scalar(ScalarType::Utf8);
    let integer = LogicalType::Scalar(ScalarType::I64);
    let binary = LogicalType::Scalar(ScalarType::Binary);
    let types = [
        text.clone(),
        integer.clone(),
        binary.clone(),
        integer.clone(),
        text.clone(),
        binary.maybe(),
        integer.maybe(),
        text.clone(),
        LogicalType::list(text),
    ];
    let unit_type = to_field("units", &types[8]).map_err(status)?;
    let DataType::List(unit_field) = unit_type.data_type() else {
        return Err(invalid("Invalid catalog unit type"));
    };
    let units = ListArray::try_new(
        Arc::clone(unit_field),
        units.offsets().clone(),
        Arc::clone(units.values()),
        None,
    )
    .map_err(arrow)?;
    let mut operations = Vec::new();
    for start in (0..batch.num_rows()).step_by(INSERT_ROWS) {
        let mut sql = String::from(
            "INSERT INTO bj_kernel_writes (_id, world, key, idx, op, value, number, label, units, _valid_from) VALUES ",
        );
        let end = (start + INSERT_ROWS).min(batch.num_rows());
        let mut columns: Vec<ArrayRef> = Vec::with_capacity((end - start) * types.len());
        for row in start..end {
            let first = columns.len() + 1;
            if row != start {
                sql.push_str(", ");
            }
            columns.extend([
                Arc::new(ids.slice(row, 1)) as ArrayRef,
                Arc::new(worlds.slice(row, 1)),
                Arc::new(keys.slice(row, 1)),
                Arc::new(ordinals.slice(row, 1)),
                Arc::new(op_values.slice(ops.keys().value(row) as usize, 1)),
                Arc::new(values.slice(row, 1)),
                Arc::new(numbers.slice(row, 1)),
                Arc::new(label_values.slice(labels.keys().value(row) as usize, 1)),
                Arc::new(units.slice(row, 1)),
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
        let fields = types
            .iter()
            .cycle()
            .take(columns.len())
            .enumerate()
            .map(|(index, ty)| to_field(format!("?_{index}"), ty).map_err(status))
            .collect::<Result<Vec<_>, _>>()?;
        let arguments = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(arrow)?;
        let args = SqlArguments::from_arrow_with_canonical_source(
            owner.with_retained_buffers(arguments),
            memory,
        )
        .map_err(status)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::StringDictionaryBuilder;
    use arrow_buffer::OffsetBuffer;
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::Field;
    use rtdb_tx::prepare_submission;
    use rtdb_types::Value;

    #[test]
    fn catalog_ipc_frames_match_stream_writer_with_dictionary_replacements() {
        let batch = |values: Vec<&str>| {
            let keys = Int32Array::from(vec![Some(0), None, Some(1)]);
            let dictionary = DictionaryArray::try_new(keys, Arc::new(StringArray::from(values))).unwrap();
            RecordBatch::try_from_iter([("label", Arc::new(dictionary) as ArrayRef)]).unwrap()
        };
        let batches = [
            batch(vec!["alpha", "beta"]),
            batch(vec!["alpha", "beta"]),
            batch(vec!["alpha", "beta", "gamma"]),
            batch(vec!["beta", "alpha"]),
        ];
        let schema = batches[0].schema_ref();
        let memory = MemoryBudget::new(1 << 20);
        let charge = memory.reserve(65_536).unwrap();
        let (mut encoder, initial) = CatalogIpcEncoder::new(schema, Arc::clone(&charge)).unwrap();
        let mut reference = StreamWriter::try_new(Vec::new(), schema).unwrap();
        assert_eq!(initial.body.as_ref(), std::mem::take(reference.get_mut()).as_slice());
        let mut frames = vec![initial];
        for (batch, expected_frames) in batches.iter().zip([2, 1, 2, 2]) {
            reference.write(batch).unwrap();
            let expected = std::mem::take(reference.get_mut());
            let page = encoder.page(batch, Arc::clone(&charge)).unwrap();
            assert_eq!(page.len(), expected_frames);
            let mut remaining = expected.as_slice();
            for frame in &page {
                let metadata = u32::from_le_bytes(frame.body[4..8].try_into().unwrap()) as usize;
                let header = arrow_ipc::root_as_message(&frame.body[8..8 + metadata]).unwrap();
                assert_eq!(frame.body.len(), 8 + metadata + header.bodyLength() as usize);
                if let Some(dictionary) = header.header_as_dictionary_batch() {
                    assert!(!dictionary.isDelta());
                }
                let (prefix, rest) = remaining.split_at(frame.body.len());
                assert_eq!(frame.body.as_ref(), prefix);
                remaining = rest;
            }
            assert!(remaining.is_empty());
            frames.extend(page);
        }
        reference.finish().unwrap();
        assert_eq!(reference.get_ref().as_slice(), IPC_EOS);
        drop(charge);
        assert_eq!(memory.used_bytes(), 65_536);
        drop(frames);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn catalog_conformance_reuses_exact_schema_with_no_scratch_and_funds_adaptation() {
        let schema = Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]);
        let memory = MemoryBudget::new(1 << 20);
        let key = Arc::new(Int64Array::from(vec![1]));
        let exact = RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![key.clone(), Arc::new(StringArray::from(vec!["original"]))],
        ).unwrap();
        let exact = OwnedBatch::new(exact, &memory).unwrap();
        let narrow = RecordBatch::try_from_iter([("key", key as ArrayRef)]).unwrap();
        let narrow = OwnedBatch::new(narrow, &memory).unwrap();
        let declared = wire::OwnedSchema::build(8192, &memory, || Ok(schema)).unwrap().into_declared();

        let full = memory.reserve(memory.available_bytes()).unwrap();
        let page = declared.conform(exact, &memory).unwrap();
        assert!(Arc::ptr_eq(page.declared_schema(), declared.schema()));
        assert_eq!(page.batch().column(1).as_any().downcast_ref::<StringArray>().unwrap().value(0), "original");
        assert!(declared.conform(narrow.clone(), &memory).is_err());
        drop(full);

        let adapted = declared.conform(narrow, &memory).unwrap();
        assert!(Arc::ptr_eq(adapted.declared_schema(), declared.schema()));
        assert_eq!(adapted.batch().schema_ref(), declared.schema());
        assert!(adapted.batch().column(1).is_null(0));
    }

    #[test]
    fn catalog_arrow_bindings_preserve_order_and_the_sql_source() {
        let memory = MemoryBudget::new(32 << 20);
        let dictionary = |entries: [&str; 2]| {
            let mut values = StringDictionaryBuilder::<Int32Type>::new();
            for entry in entries {
                values.append(entry).unwrap();
            }
            Arc::new(values.finish()) as ArrayRef
        };
        let columns = [
            ("_id", Arc::new(StringArray::from(vec!["z", "a"])) as ArrayRef),
            ("world", Arc::new(Int64Array::from(vec![1, 2]))),
            ("key", Arc::new(BinaryArray::from_vec(vec![&[0xff], &[0]]))),
            ("idx", Arc::new(Int64Array::from(vec![3, 4]))),
            ("op", dictionary(["put", "del"])),
            ("value", Arc::new(BinaryArray::from_opt_vec(vec![Some(&[0; 8][..]), None]))),
            ("number", Arc::new(Int64Array::from(vec![None, Some(5)]))),
            ("label", dictionary(["owner", "reader"])),
            ("units", Arc::new(ListArray::new(
                Arc::new(Field::new("item", DataType::Utf8, true)),
                OffsetBuffer::new(vec![0, 2, 2].into()),
                Arc::new(StringArray::from(vec!["a", "b"])),
                None,
            ))),
            ("stamp_offsets", Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>([
                Some(vec![Some(1)]),
                Some(vec![]),
            ]))),
        ];
        let batch = OwnedBatch::new(RecordBatch::try_from_iter(columns).unwrap(), &memory).unwrap();
        let (writes, _charge) = catalog_inserts(&batch, &memory).unwrap();
        let [TxOp::Sql { sql, args: Some(args) }] = writes.as_slice() else {
            panic!("expected one catalog write");
        };
        assert!(args.single_arrow_page().is_some());
        assert!(sql.contains("OVERLAY($6 PLACING EPOCH_NANOS_BE(CURRENT_TIMESTAMP) FROM 1 FOR 8)"));
        assert!(sql.contains("($10, $11, $12, $13, $14, $15, $16, $17, $18,"));
        let expected = vec![
            Value::Utf8("z".into()), Value::I64(1), Value::Binary(vec![0xff]), Value::I64(3),
            Value::Utf8("put".into()), Value::Binary(vec![0; 8]), Value::Null,
            Value::Utf8("owner".into()),
            Value::list(vec![Value::Utf8("a".into()), Value::Utf8("b".into())]),
            Value::Utf8("a".into()), Value::I64(2), Value::Binary(vec![0]), Value::I64(4),
            Value::Utf8("del".into()), Value::Null, Value::I64(5),
            Value::Utf8("reader".into()), Value::list(vec![]),
        ];
        assert_eq!(&*args.rows().next().unwrap().unwrap(), expected.as_slice());
        let row = SqlArguments::new(args.types().to_vec(), vec![expected], &memory).unwrap();
        let source = |operations| {
            prepare_submission(operations, Default::default(), "xtdb", &memory)
                .unwrap().encode().unwrap()
        };
        let row = source(vec![TxOp::Sql { sql: sql.clone(), args: Some(row) }]);
        assert_eq!(source(writes).as_slice(), row.as_slice());
    }
}
