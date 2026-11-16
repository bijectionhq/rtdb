use std::{collections::BTreeMap, sync::Arc};
use arrow_array::RecordBatch;
use arrow_flight::{FlightData, PutResult, sql::{Command, CommandStatementIngest, DoPutUpdateResult, TableExistsOption, TableNotExistOption}};
use prost::Message;
use rtdb_columnar::{MemoryReservation, OwnedBatch, estimate_read_value_bytes, read_value};
use rtdb_tx::{Documents, SqlArguments, TxOp};
use rtdb_types::{TableRef, Value};
use tonic::{Response, Status, Streaming};
use crate::{error::{cancelled, invalid, status, unsupported}, service::{Call, Output, Service, command, result_stream}, wire};

struct Upload { input: Streaming<FlightData>, first: Option<FlightData>, decoder: wire::Decoder, service: Service }
impl Upload {
    async fn next(&mut self) -> Result<Option<OwnedBatch>, Status> {
        loop {
            let data = match self.first.take() { Some(data) => Some(data), None => self.input.message().await? };
            let Some(data) = data else { return Ok(None); };
            let _transport = self.service.memory().reserve(wire::add(data.encoded_len(), 8192)?).map_err(status)?;
            if let Some(batch) = self.decoder.decode(data)? { return Ok(Some(batch)); }
        }
    }
    async fn all_arguments(&mut self) -> Result<Option<SqlArguments>, Status> {
        let mut arguments: Option<SqlArguments> = None;
        while let Some(batch) = self.next().await? {
            if batch.batch().num_columns() == 0 { return Ok(None); }
            let next = wire::arguments(batch.batch(), self.service.memory())?;
            match &mut arguments { Some(arguments) => arguments.append(next).map_err(status)?, None => arguments = Some(next) }
        }
        if arguments.is_none() {
            if let Some(schema) = self.decoder.schema() { if !schema.fields().is_empty() { arguments = Some(wire::arguments(&RecordBatch::new_empty(Arc::clone(schema)), self.service.memory())?); } }
        }
        Ok(arguments)
    }
}
impl Service {
    pub async fn upload(&self, call: Call, mut input: Streaming<FlightData>) -> Result<Response<Output<PutResult>>, Status> {
        let work = async {
            let first = input.message().await?.ok_or_else(|| invalid("Flight upload has no command"))?;
            let _command_charge = call.reserve(wire::add(wire::mul(first.encoded_len(), 32)?, 8192)?)?;
            let descriptor = first.flight_descriptor.as_ref().ok_or_else(|| invalid("Flight upload has no descriptor"))?;
            let command = command(&descriptor.cmd)?;
            let mut upload = Upload { input, first: Some(first), decoder: wire::Decoder::new(Arc::clone(self.memory())), service: self.clone() };
            match command {
                Command::CommandStatementUpdate(command) => {
                    let connection = self.tx_or_session(&call, command.transaction_id.as_ref()).await?;
                    self.execute_update(&connection, &command.query, None).await?; Ok(update_ack())
                }
                Command::CommandPreparedStatementQuery(command) => {
                    let prepared = self.prepared(&call, &command.prepared_statement_handle)?;
                    let batch = upload.next().await?;
                    let params = match batch { Some(batch) => wire::parameter_ipc(batch.batch(), self.memory())?, None => match upload.decoder.schema() { Some(schema) => wire::parameter_ipc(&RecordBatch::new_empty(Arc::clone(schema)), self.memory())?, None => None } };
                    *prepared.params.lock().expect("prepared parameters lock") = params; Ok(Vec::new())
                }
                Command::CommandPreparedStatementUpdate(command) => {
                    let prepared = self.prepared(&call, &command.prepared_statement_handle)?;
                    let arguments = upload.all_arguments().await?;
                    let connection = self.scoped(&call, &prepared.scope).await?;
                    self.execute_update(&connection, &prepared.sql, arguments).await?; Ok(update_ack())
                }
                Command::CommandStatementIngest(command) => {
                    validate_ingest(&command, &call.database)?;
                    let table = TableRef::new(command.schema.as_deref().unwrap_or("public"), &command.table);
                    let connection = self.tx_or_session(&call, command.transaction_id.as_ref()).await?;
                    let mut native = connection.borrow().await?;
                    while let Some(batch) = upload.next().await? {
                        let (rows, _scratch) = documents(batch.batch(), self)?;
                        let operation = TxOp::PutDocs { table: table.clone(), documents: Documents::new(rows, self.memory()).map_err(status)?, valid_from: None, valid_to: None };
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
fn documents(batch: &RecordBatch, service: &Service) -> Result<(Vec<BTreeMap<String, Value>>, Arc<MemoryReservation>), Status> {
    let schema = batch.schema(); let mut bound = 8192;
    for row in 0..batch.num_rows() { for (column, field) in schema.fields().iter().enumerate() { bound = wire::add(bound, wire::add(field.name().len(), estimate_read_value_bytes(field, batch.column(column).as_ref(), row).map_err(status)?)?)?; } }
    let scratch = service.memory().reserve(bound).map_err(status)?;
    let rows = (0..batch.num_rows()).map(|row| schema.fields().iter().enumerate().map(|(column, field)| Ok((field.name().clone(), read_value(field, batch.column(column).as_ref(), row).map_err(status)?))).collect::<Result<BTreeMap<_, _>, Status>>()).collect::<Result<Vec<_>, _>>()?;
    Ok((rows, scratch))
}
