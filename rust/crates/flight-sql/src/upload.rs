use rtdb_types::StructFields;
use std::sync::Arc;
use arrow_array::{Array, DictionaryArray, RecordBatch, StringArray, types::Int8Type};
use arrow_schema::{DataType, Field};
use arrow_flight::{FlightData, PutResult, sql::{Command, CommandStatementIngest, CommandStatementUpdate, DoPutUpdateResult, TableExistsOption, TableNotExistOption}};
use prost::Message;
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation, OwnedBatch, estimate_read_value_bytes};
use rtdb_tx::{ArrowDocuments, SqlArguments, TxOp};
use rtdb_types::{TableRef, Value};
use tonic::{Response, Status, Streaming};
use crate::{error::{cancelled, invalid, status, unsupported}, service::{Call, Output, Service, command, result_stream}, wire};

struct UploadFrame {
    data: FlightData,
    _transport: Arc<MemoryReservation>,
}

impl UploadFrame {
    fn admit(data: FlightData, memory: &Arc<MemoryBudget>) -> Result<Self, Status> {
        let transport = memory.reserve(wire::add(data.encoded_len(), 8192)?).map_err(status)?;
        Ok(Self { data, _transport: transport })
    }

    fn command_bytes(&self) -> Result<&[u8], Status> {
        let descriptor = self.data.flight_descriptor.as_ref().ok_or_else(|| invalid("Flight upload has no descriptor"))?;
        Ok(&descriptor.cmd)
    }
}

fn decode_upload_command(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<(Arc<MemoryReservation>, Command), Status> {
    let charge = memory.reserve(wire::add(wire::mul(bytes.len(), 32)?, 8192)?).map_err(status)?;
    Ok((charge, command(bytes)?))
}

struct Upload { input: Streaming<FlightData>, first: Option<UploadFrame>, decoder: wire::Decoder, service: Service }
impl Upload {
    async fn next(&mut self) -> Result<Option<OwnedBatch>, Status> {
        loop {
            let UploadFrame { data, _transport } = match self.first.take() {
                Some(frame) => frame,
                None => {
                    let Some(data) = self.input.message().await? else { return Ok(None); };
                    UploadFrame::admit(data, self.service.memory())?
                }
            };
            if let Some(batch) = self.decoder.decode(data)? { return Ok(Some(batch)); }
        }
    }
    async fn all_arguments(&mut self) -> Result<Option<SqlArguments>, Status> {
        let mut arguments: Option<SqlArguments> = None;
        while let Some(batch) = self.next().await? {
            if batch.batch().num_columns() == 0 { return Ok(None); }
            let next = SqlArguments::from_arrow(batch, self.service.memory()).map_err(status)?;
            match &mut arguments { Some(arguments) => arguments.append(next).map_err(status)?, None => arguments = Some(next) }
        }
        if arguments.is_none() {
            if let Some(batch) = self.decoder.empty_batch()? {
                arguments = Some(SqlArguments::from_arrow(batch, self.service.memory()).map_err(status)?);
            }
        }
        Ok(arguments)
    }
}
impl Service {
    pub async fn upload(&self, call: Call, mut input: Streaming<FlightData>) -> Result<Response<Output<PutResult>>, Status> {
        let work = async {
            let first = input.message().await?.ok_or_else(|| invalid("Flight upload has no command"))?;
            let first = UploadFrame::admit(first, self.memory())?;
            let bytes = first.command_bytes()?;
            let command_bytes = bytes.len();
            let (mut charge, command) = decode_upload_command(bytes, call.memory())?;
            let mut upload = Upload { input, first: Some(first), decoder: wire::Decoder::new(Arc::clone(self.memory())), service: self.clone() };
            match command {
                Command::CommandStatementUpdate(CommandStatementUpdate { query, transaction_id }) => {
                    drop(upload.first.take());
                    let retained = wire::add(128, query.capacity())?;
                    let grant = Arc::get_mut(&mut charge).expect("upload command owns its grant");
                    // A decoded transaction ID can share the protobuf Any payload's backing.
                    grant.shrink_to(wire::add(retained, transaction_id.as_ref().map_or(0, |_| command_bytes))?).map_err(status)?;
                    let connection = self.tx_or_session(&call, transaction_id.as_ref()).await?;
                    drop(transaction_id);
                    grant.shrink_to(retained).map_err(status)?;
                    self.execute_update(&connection, &query, None).await?; Ok(update_ack())
                }
                Command::CommandPreparedStatementQuery(command) => {
                    let prepared = self.prepared(&call, &command.prepared_statement_handle)?;
                    drop(command);
                    drop(charge);
                    let params = match upload.next().await? {
                        Some(batch) if batch.batch().num_columns() != 0 => Some(wire::BoundParameters::new(batch, self.memory())?),
                        Some(_) => None,
                        None => upload.decoder.empty_batch()?
                            .map(|batch| wire::BoundParameters::new(batch, self.memory())).transpose()?,
                    };
                    *prepared.params.lock().expect("prepared parameters lock") = params; Ok(Vec::new())
                }
                Command::CommandPreparedStatementUpdate(command) => {
                    let prepared = self.prepared(&call, &command.prepared_statement_handle)?;
                    drop(command);
                    drop(charge);
                    let arguments = upload.all_arguments().await?;
                    let connection = self.scoped(&call, &prepared.scope).await?;
                    self.execute_update(&connection, &prepared.sql, arguments).await?; Ok(update_ack())
                }
                Command::CommandStatementIngest(mut command) => {
                    validate_ingest(&command, &call.database)?;
                    let table = TableRef::new(command.schema.take().unwrap_or_else(|| "public".to_owned()), std::mem::take(&mut command.table));
                    let transaction_id = command.transaction_id.take();
                    drop(command);
                    let retained = wire::add(128, wire::mul(wire::add(table.schema.capacity(), table.table.capacity())?, 2)?)?;
                    let grant = Arc::get_mut(&mut charge).expect("upload command owns its grant");
                    grant.shrink_to(wire::add(retained, transaction_id.as_ref().map_or(0, |_| command_bytes))?).map_err(status)?;
                    let connection = self.tx_or_session(&call, transaction_id.as_ref()).await?;
                    drop(transaction_id);
                    grant.shrink_to(retained).map_err(status)?;
                    let mut native = connection.borrow().await?;
                    while let Some(batch) = upload.next().await? {
                        let documents = match ArrowDocuments::from_batch(&batch, self.memory()).map_err(status)? {
                            Some(documents) => documents,
                            None => {
                                let Some(documents) = ArrowDocuments::from_ingest_batch(&batch, self.memory()).map_err(status)? else { continue; };
                                documents
                            }
                        };
                        let operation = TxOp::PutArrowDocs { table: table.clone(), documents };
                        if let Some(result) = native.state_mut().execute_operation(operation).await.map_err(status)? { if let Some(error) = result.error() { return Err(status(error.clone())); } }
                    }
                    Ok(update_ack())
                }
                _ => Err(unsupported()),
            }
        };
        let result = call.cancellation.run_until_cancelled(work).await.ok_or_else(cancelled).and_then(|result| result).map_err(|error| call.error(error))?;
        result_stream(call, result)
    }
}

#[test]
fn a_large_upload_header_uses_transport_bytes_without_inflating_the_command_grant() {
    use arrow_flight::{FlightDescriptor, sql::{CommandPreparedStatementQuery, ProstMessageExt}};
    use bytes::Bytes;

    let handle = Bytes::from_static(b"prepared");
    let bytes = CommandPreparedStatementQuery { prepared_statement_handle: handle.clone() }.as_any().encode_to_vec();
    let command_bound = 8192 + 32 * bytes.len();
    let data = FlightData {
        flight_descriptor: Some(FlightDescriptor::new_cmd(bytes)),
        data_header: vec![0; 256 << 10].into(),
        ..Default::default()
    };
    let frame_bound = 8192 + data.encoded_len();
    let memory = MemoryBudget::new(frame_bound + command_bound);
    let frame = UploadFrame::admit(data, &memory).unwrap();
    let (charge, command) = decode_upload_command(frame.command_bytes().unwrap(), &memory).unwrap();
    let Command::CommandPreparedStatementQuery(command) = command else { panic!("expected prepared query") };
    assert_eq!(command.prepared_statement_handle, handle);
    assert_eq!(memory.used_bytes(), frame_bound + command_bound);

    drop(command);
    drop(charge);
    assert_eq!(memory.used_bytes(), frame_bound);
    drop(frame);
    assert_eq!(memory.used_bytes(), 0);
}

fn update_ack() -> Vec<Result<PutResult, Status>> { vec![Ok(PutResult { app_metadata: DoPutUpdateResult { record_count: -1 }.encode_to_vec().into() })] }
fn validate_ingest(command: &CommandStatementIngest, database: &str) -> Result<(), Status> {
    if let Some(options) = command.table_definition_options {
        if options.if_exists != TableExistsOption::Unspecified as i32 && options.if_exists != TableExistsOption::Append as i32 { return Err(invalid("Bulk ingest only supports append-on-exists")); }
        if options.if_not_exist != TableNotExistOption::Unspecified as i32 && options.if_not_exist != TableNotExistOption::Create as i32 { return Err(invalid("Bulk ingest auto-creates tables and cannot fail-if-not-exist")); }
    }
    if command.catalog.as_deref().is_some_and(|catalog| !catalog.is_empty() && catalog != database) { return Err(invalid("Bulk ingest catalog must match the connection catalog")); }
    if command.table.contains('.') { return Err(invalid("Bulk ingest table name must not contain '.'; use the schema field")); }
    Ok(())
}
pub(crate) fn documents(batch: &RecordBatch, service: &Service) -> Result<(Vec<StructFields<Value>>, Arc<MemoryReservation>), Status> {
    decode_documents(batch, service.memory(), None, rtdb_columnar::read_execution_value_as)
}

pub(crate) fn domain_documents(batch: &RecordBatch, service: &Service) -> Result<(Vec<StructFields<Value>>, Arc<MemoryReservation>), Status> {
    decode_documents(batch, service.memory(), None, rtdb_columnar::read_value_as)
}

pub(crate) fn operation_dictionary(field: &Field) -> bool {
    field.name() == "_op" && field.metadata().is_empty()
        && matches!(field.data_type(), DataType::Dictionary(key, value)
            if key.as_ref() == &DataType::Int8 && value.as_ref() == &DataType::Utf8)
}

pub(crate) fn owned_operation(cell: ArrowCell<'_>) -> Option<&str> {
    if !operation_dictionary(cell.field) { return cell.utf8(); }
    let dictionary = cell.array.as_any().downcast_ref::<DictionaryArray<Int8Type>>()?;
    let key = dictionary.key(cell.row)?;
    let values = dictionary.values().as_any().downcast_ref::<StringArray>()?;
    (key < values.len() && values.is_valid(key)).then(|| values.value(key))
}

pub(crate) fn owned_documents(batch: &RecordBatch, memory: &Arc<MemoryBudget>) -> Result<(Vec<StructFields<Value>>, Arc<MemoryReservation>), Status> {
    let dictionary = batch.schema_ref().index_of("_op").ok()
        .filter(|&column| operation_dictionary(batch.schema_ref().field(column)));
    decode_documents(batch, memory, dictionary, |ty, field, column, row| {
        match ty {
            rtdb_types::LogicalType::Nothing => Ok(Value::Undefined),
            _ => rtdb_columnar::read_value_as(ty, field, column, row),
        }
    })
}

pub(crate) fn owned_payload(batch: &OwnedBatch, memory: &Arc<MemoryBudget>) -> Result<Option<ArrowDocuments>, Status> {
    if let Some(documents) = ArrowDocuments::from_owned_payload(batch, memory).map_err(status)? {
        return Ok(Some(documents));
    }
    ArrowDocuments::from_legacy_owned_payload(batch, memory).map_err(status)
}

#[test]
fn owned_payload_retains_a_vector_and_struct_with_legacy_upload_metadata() {
    use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, ListArray, StructArray};
    use arrow_schema::{DataType, Field, Schema};

    let memory = MemoryBudget::new(1 << 20);
    let vector = ListArray::from_iter_primitive::<arrow_array::types::Float64Type, _, _>([
        Some(vec![Some(-0.0), None]), None,
    ]);
    let companion = StructArray::new(vec![Field::new("key", DataType::Int64, false)].into(),
        vec![Arc::new(Int64Array::from(vec![9, 10]))], None);
    let schema = Arc::new(Schema::new(vec![Field::new("_id", DataType::Int64, false),
        Field::new("v", vector.data_type().clone(), true).with_metadata([("description".into(), "vector".into())].into()),
        Field::new("detail", companion.data_type().clone(), false)]));
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef, Arc::new(vector), Arc::new(companion),
    ]).unwrap();
    let batch = OwnedBatch::new(batch, &memory).unwrap();
    let assert_components = |array: &dyn Array| {
        let vector = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert!(vector.is_null(1));
        let values = vector.values().as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(values.value(0).to_bits(), (-0.0f64).to_bits());
        assert!(values.is_null(1));
    };
    assert_components(batch.batch().column(1).as_ref());
    assert!(ArrowDocuments::from_owned_payload(&batch, &memory).unwrap().is_none());
    assert_eq!(owned_payload(&batch, &memory).unwrap().unwrap().row_count(), 2);
}

fn decode_documents(
    batch: &RecordBatch, memory: &Arc<MemoryBudget>, dictionary: Option<usize>,
    read: impl Fn(&rtdb_types::LogicalType, &arrow_schema::Field, &dyn arrow_array::Array, usize) -> rtdb_types::Result<Value>,
) -> Result<(Vec<StructFields<Value>>, Arc<MemoryReservation>), Status> {
    let schema = batch.schema();
    let scratch = reserve_decode(batch, memory, dictionary)?;
    let types = if batch.num_rows() == 0 { Vec::new() } else {
        schema.fields().iter().enumerate().map(|(column, field)| {
            if Some(column) == dictionary { Ok(None) } else { rtdb_columnar::from_field(field).map(Some).map_err(status) }
        }).collect::<Result<Vec<_>, _>>()?
    };
    let rows = (0..batch.num_rows()).map(|row| {
        schema.fields().iter().zip(&types).enumerate().map(|(column, (field, ty))| {
            let array = batch.column(column).as_ref();
            let value = match ty {
                Some(ty) => read(ty, field, array, row).map_err(status)?,
                None => owned_operation(ArrowCell { field, array, row })
                    .map_or(Value::Null, |operation| Value::Utf8(operation.to_owned())),
            };
            Ok((field.name().clone(), value))
        }).collect::<Result<StructFields<_>, Status>>()
    }).collect::<Result<Vec<_>, _>>()?;
    Ok((rows, scratch))
}

pub(crate) fn reserve_document_decode(
    batch: &RecordBatch, memory: &Arc<MemoryBudget>,
) -> Result<Arc<MemoryReservation>, Status> {
    reserve_decode(batch, memory, None)
}

fn reserve_decode(
    batch: &RecordBatch, memory: &Arc<MemoryBudget>, dictionary: Option<usize>,
) -> Result<Arc<MemoryReservation>, Status> {
    let mut bound = 8192;
    for row in 0..batch.num_rows() {
        for (column, field) in batch.schema_ref().fields().iter().enumerate() {
            let array = batch.column(column).as_ref();
            let bytes = if Some(column) == dictionary {
                let length = owned_operation(ArrowCell { field, array, row }).map_or(0, str::len);
                wire::add(2048, wire::mul(length, 2)?)?
            } else {
                estimate_read_value_bytes(field, array, row).map_err(status)?
            };
            bound = wire::add(bound, wire::add(field.name().len(), bytes)?)?;
        }
    }
    memory.reserve(bound).map_err(status)
}

#[test]
fn owned_operation_dictionaries_match_text_in_arrow_and_materialized_reads() {
    use arrow_array::{ArrayRef, Int8Array};

    let operations = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(1), None, Some(0), Some(1), Some(2)]),
        Arc::new(StringArray::from(vec![Some("check"), Some("create"), None])),
    ).unwrap();
    let packed = RecordBatch::try_from_iter([("_op", Arc::new(operations) as ArrayRef)]).unwrap();
    let plain = RecordBatch::try_from_iter([("_op", Arc::new(StringArray::from(
        vec![Some("create"), None, Some("check"), Some("create"), None],
    )) as ArrayRef)]).unwrap();
    let memory = MemoryBudget::new(1 << 20);
    let mut ipc = Vec::new();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut ipc, packed.schema_ref()).unwrap();
    writer.write(&packed).unwrap();
    writer.finish().unwrap();
    drop(writer);
    let decoded = rtdb_format::ipc::OwnedStream::read_buffer(ipc.into(), &memory).unwrap();
    let packed = decoded.pages()[0].batch();
    let (actual, _actual_charge) = owned_documents(packed, &memory).unwrap();
    let (expected, _expected_charge) = owned_documents(&plain, &memory).unwrap();
    assert_eq!(actual, expected);
    for row in 0..packed.num_rows() {
        assert_eq!(
            ArrowCell::named(packed, "_op", row).and_then(owned_operation),
            ArrowCell::named(&plain, "_op", row).and_then(ArrowCell::utf8),
        );
    }
    let payload = RecordBatch::try_from_iter([("label", Arc::clone(packed.column(0)))]).unwrap();
    assert!(owned_documents(&payload, &memory).is_err());
}
