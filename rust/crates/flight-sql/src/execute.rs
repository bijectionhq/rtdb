use std::sync::Arc;
use arrow_flight::FlightData;
use arrow_schema::Schema;
use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use rtdb_node::{ControlExecutionOptions, ControlFrontend, ControlResult, NativeSession, PreparedSql, ResultSignaturePolicy, control_schema};
use rtdb_plan::ControlStatement;
use rtdb_query::QueryContext;
use rtdb_tx::SqlArguments;
use tokio_util::sync::CancellationToken;
use tonic::{Response, Status};
use crate::{error::{cancelled, initial_prepare_status, invalid, status}, service::{Call, Output, Service, query_ticket}, state::ConnectionResource, ticket::Ticket, wire};

pub(crate) struct Shape { pub schema: Option<wire::OwnedSchema>, pub parameters: usize }
impl Service {
    pub async fn prepare_shape(&self, connection: Arc<ConnectionResource>, source: &str, params: Option<&Bytes>) -> Result<Shape, Status> {
        let mut native = connection.borrow().await?;
        let token = native.state().cancellation().child_token(); let _cancel = token.clone().drop_guard();
        let options = options(&native);
        if let Some(control) = rtdb_sql::parse_admitted_control_in(source, &options, self.memory()).map_err(status)? {
            native.prepare_context(&token).await.map_err(status)?;
            return Ok(Shape { schema: control_schema(control.statement()).map_err(status)?.as_ref().map(|schema| wire::plan_schema(schema, self.memory())).transpose()?, parameters: 0 });
        }
        let context = native.prepare_context(&token).await.map_err(status)?;
        let prepared = PreparedSql::prepare(source, &context, options, ResultSignaturePolicy::Dynamic)
            .map_err(|error| initial_prepare_status(source, error, &context.memory))?;
        let schema = if prepared.is_dml() { None } else {
            Some(self.parameterized_schema(&prepared, &context, params)?)
        };
        Ok(Shape { schema, parameters: prepared.parameter_count() })
    }
    fn parameterized_schema(
        &self, prepared: &PreparedSql, context: &Arc<QueryContext>, params: Option<&Bytes>,
    ) -> Result<wire::OwnedSchema, Status> {
        let schema = if let Some(params) = params {
            let types = wire::parameter_types(params, self.memory())?;
            prepared.result_schema(context, types.as_slice()).map_err(status)?
        } else {
            prepared.result_schema(context, prepared.parameter_types()).map_err(status)?
        };
        wire::plan_schema(schema.schema(), self.memory())
    }

    pub async fn describe(&self, connection: Arc<ConnectionResource>, source: &str, params: Option<&Bytes>) -> Result<(wire::OwnedSchema, usize), Status> {
        let shape = self.prepare_shape(connection, source, params).await?;
        Ok((shape.schema.ok_or_else(|| invalid("executeSchema only supports queries (DML and SET return no result set)"))?, shape.parameters))
    }
    pub async fn execute_update(&self, connection: &Arc<ConnectionResource>, source: &str, arguments: Option<SqlArguments>) -> Result<(), Status> {
        let mut native = connection.borrow().await?;
        self.execute_update_in(&mut native, source, arguments).await
    }
    async fn execute_update_in(&self, native: &mut NativeSession, source: &str, arguments: Option<SqlArguments>) -> Result<(), Status> {
        let token = native.state().cancellation().child_token(); let _cancel = token.clone().drop_guard();
        let options = options(native);
        if let Some(control) = rtdb_sql::parse_admitted_control_in(source, &options, self.memory()).map_err(status)? {
            if control_schema(control.statement()).map_err(status)?.is_some() { return Err(invalid("executeUpdate does not support queries")); }
            return execute_control_update(native, control.statement(), arguments.as_ref(), &token).await;
        }
        let context = native.prepare_context(&token).await.map_err(status)?;
        let prepared = PreparedSql::prepare(source, &context, options, ResultSignaturePolicy::Dynamic).map_err(status)?;
        execute_prepared_update(native, &prepared, arguments).await
    }

    fn empty_update_info(&self) -> Result<(wire::OwnedSchema, Bytes), Status> {
        Ok((wire::OwnedSchema::empty(self.memory())?, query_ticket(Ticket::Empty {}.encode(self.memory())?, self.memory())?))
    }

    pub async fn query_info(&self, connection: Arc<ConnectionResource>, source: &str, params: Option<Bytes>) -> Result<(wire::OwnedSchema, Bytes), Status> {
        let mut native = connection.borrow().await?;
        let token = native.state().cancellation().child_token(); let _cancel = token.clone().drop_guard();
        let options = options(&native);
        let decode_arguments = || params.as_ref().map(|params| {
            let batch = wire::parameter_batch(params, self.memory())?;
            wire::arguments(batch.batch(), self.memory())
        }).transpose();
        let (basis, schema) = if let Some(control) = rtdb_sql::parse_admitted_control_in(source, &options, self.memory()).map_err(status)? {
            let Some(schema) = control_schema(control.statement()).map_err(status)? else {
                let arguments = decode_arguments()?;
                execute_control_update(&mut native, control.statement(), arguments.as_ref(), &token).await?;
                drop(control);
                return self.empty_update_info();
            };
            let basis = native.state_mut().unchecked_read_basis().await.map_err(status)?;
            (basis, wire::plan_schema(&schema, self.memory())?)
        } else {
            let prepared = {
                let context = native.prepare_context(&token).await.map_err(status)?;
                PreparedSql::prepare(source, &context, options.clone(), ResultSignaturePolicy::Dynamic)
                    .map_err(|error| initial_prepare_status(source, error, &context.memory))?
            };
            if prepared.is_dml() {
                let arguments = decode_arguments()?;
                native.prepare_context(&token).await.map_err(status)?;
                execute_prepared_update(&mut native, &prepared, arguments).await?;
                return self.empty_update_info();
            }
            drop(prepared);
            let basis = native.state_mut().read_basis().await.map_err(status)?;
            let context = native.query_context_at(&basis, &token).await.map_err(status)?;
            let prepared = PreparedSql::prepare(source, &context, options, ResultSignaturePolicy::Dynamic).map_err(status)?;
            let schema = self.parameterized_schema(&prepared, &context, params.as_ref())?;
            (basis, schema)
        };
        let basis_bytes = basis.snapshot.databases.iter().try_fold(8192usize, |size, (database, times)| wire::add(size, wire::add(wire::mul(database.len(), 8)?, wire::mul(times.len(), 128)?)?))?;
        let _ticket = self.memory().reserve(wire::add(basis_bytes, wire::mul(wire::add(source.len(), params.as_ref().map_or(0, Bytes::len))?, 8)?)?).map_err(status)?;
        let ticket = Ticket::run(native.state().database().into(), source.into(), params.as_ref(), &basis)?.encode(self.memory())?;
        Ok((schema, query_ticket(ticket, self.memory())?))
    }
    pub async fn query_stream(&self, call: Call, encoded: Bytes) -> Result<Response<Output<FlightData>>, Status> {
        let ticket = Ticket::decode(&encoded).map_err(|error| call.error(error))?;
        let Ticket::Run { database, sql, params, basis } = ticket else { return crate::service::result_stream(call, vec![Ok(wire::schema_data(&Schema::empty(), self.memory())?)]); };
        let work = async {
            let connection = self.connection(&call, &database).await?;
            let mut native = connection.borrow().await?;
            let token = native.state().cancellation().child_token();
            let token_guard = token.clone().drop_guard();
            let options = options(&native);
            let params = params.as_ref().map(|value| STANDARD.decode(value).map(Bytes::from).map_err(|_| invalid("invalid ticket parameters"))).transpose()?;
            let arguments = params.as_ref().map(|bytes| wire::parameter_batch(bytes, self.memory()).and_then(|batch| wire::arguments(batch.batch(), self.memory()))).transpose()?;
            let cursor = if let Some(control) = rtdb_sql::parse_admitted_control_in(&sql, &options, self.memory()).map_err(status)? {
                if control_schema(control.statement()).map_err(status)?.is_none() { return Err(invalid("ticket query cannot execute an update")); }
                match native.execute_control_with(control.statement(), &[], &token, ControlExecutionOptions { frontend: ControlFrontend::FlightSql, implicit_transaction: false }).await.map_err(status)? {
                    ControlResult::Rows(cursor) => cursor, _ => return Err(invalid("ticket query has no result set")),
                }
            } else {
                let context = native.query_context_at(&basis.read()?, &token).await.map_err(status)?;
                let prepared = PreparedSql::prepare(&sql, &context, options, ResultSignaturePolicy::Dynamic).map_err(status)?;
                if prepared.is_dml() { return Err(invalid("ticket query cannot execute an update")); }
                let row = match &arguments { None => &[][..], Some(arguments) => arguments.rows().next().ok_or_else(|| invalid("Query parameter batch has no rows"))? };
                let bound = row.iter().try_fold(wire::add(1024, wire::mul(row.len(), std::mem::size_of::<rtdb_types::Value>())?)?, |bytes, value| wire::add(bytes, value.retained_bytes().map_err(status)?))?;
                let _values = self.memory().reserve(bound).map_err(status)?;
                let values = row.to_vec();
                prepared.open_query_typed(context, values, arguments.as_ref().map(SqlArguments::types).unwrap_or_default()).await.map_err(status)?
            };
            drop(native); Ok::<_, Status>((cursor, connection, token_guard))
        };
        let (mut cursor, connection, token_guard) = call.cancellation.run_until_cancelled(work).await.ok_or_else(cancelled).and_then(|result| result).map_err(|error| call.error(error))?;
        let schema = wire::plan_schema(cursor.schema(), self.memory()).map_err(|error| call.error(error))?;
        let headers = call.response(())?.metadata().clone(); let service = self.clone();
        let stream: Output<FlightData> = Box::pin(async_stream::try_stream! {
            let _connection = connection; let _token = token_guard; let _call = call;
            yield wire::schema_data(&schema, service.memory())?;
            let mut encoder = match wire::Encoder::new(&schema, service.memory()) { Ok(encoder) => encoder, Err(error) => { cursor.record_consumer_error(); Err(error)?; unreachable!() } };
            loop {
                let batch = _call.cancellation.run_until_cancelled(cursor.next_batch()).await.ok_or_else(cancelled)?.map_err(status)?;
                let Some(batch) = batch else { break; };
                let batch = match rtdb_columnar::conform_batch(batch, &schema, service.memory()).map_err(status) { Ok(batch) => batch, Err(error) => { cursor.record_consumer_error(); Err(error)?; unreachable!() } };
                let encoded = match encoder.declared_batch(&batch, service.memory()) { Ok(encoded) => encoded, Err(error) => { cursor.record_consumer_error(); Err(error)?; unreachable!() } };
                for data in encoded { yield data; }
            }
        });
        let mut response = Response::new(stream); *response.metadata_mut() = headers; Ok(response)
    }
}
async fn execute_control_update(native: &mut NativeSession, control: &ControlStatement, arguments: Option<&SqlArguments>, token: &CancellationToken) -> Result<(), Status> {
    let params = arguments.and_then(|arguments| arguments.rows().next()).unwrap_or(&[]);
    match native.execute_control_with(control, params, token, ControlExecutionOptions { frontend: ControlFrontend::FlightSql, implicit_transaction: false }).await.map_err(status)? {
        ControlResult::Command { .. } => Ok(()),
        _ => Err(invalid("Statement is not supported by Flight SQL executeUpdate")),
    }
}

async fn execute_prepared_update(native: &mut NativeSession, prepared: &PreparedSql, arguments: Option<SqlArguments>) -> Result<(), Status> {
    let operation = prepared.dml_operation(arguments).map_err(status)?;
    if let Some(result) = native.state_mut().execute_operation(operation).await.map_err(status)? {
        if let Some(error) = result.error() { return Err(status(error.clone())); }
    }
    Ok(())
}

pub(crate) fn options(native: &NativeSession) -> rtdb_sql::PrepareOptions { rtdb_sql::PrepareOptions { search_path: native.state().parameter("search_path").unwrap_or("public").split(',').map(|name| name.trim().trim_matches('"').to_owned()).collect(), ..Default::default() } }
