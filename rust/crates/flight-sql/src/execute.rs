use std::{sync::{Arc, Mutex}, time::Duration};
use arrow_flight::FlightData;
use arrow_schema::Schema;
use bytes::Bytes;
use rtdb_node::{ControlExecutionOptions, ControlFrontend, ControlResult, NativeSession, PreparedSql, ResultSignaturePolicy, control_schema};
use rtdb_plan::ControlStatement;
use rtdb_query::{QueryContext, QueryCursor};
use rtdb_tx::SqlArguments;
use tokio::sync::OwnedSemaphorePermit;
use tokio_util::sync::CancellationToken;
use tonic::{Response, Status};
use uuid::Uuid;
use crate::{error::{cancelled, invalid, status}, lease::Leased, service::{Call, Output, Service, found, query_ticket}, state::{ConnectionResource, ResourceOwner}, ticket::Ticket, wire};

pub(crate) struct Shape { pub schema: Option<wire::OwnedSchema>, pub parameters: usize }
const QUERY_TICKET_LIFETIME: Duration = Duration::from_secs(60);

pub(crate) struct QueryTicket {
    owner: ResourceOwner,
    connection: Arc<ConnectionResource>,
    context: Arc<QueryContext>,
    input: QueryInput,
    deadline: tokio::time::Instant,
    _slot: OwnedSemaphorePermit,
}

enum QueryInput {
    Statement { prepared: PreparedSql, params: Option<Arc<wire::BoundParameters>> },
    Inspection(Mutex<Option<QueryCursor>>),
}

impl Leased for QueryTicket {
    fn close(&self) {
        self.context.cancellation.cancel();
    }

    fn hard_deadline(&self) -> Option<tokio::time::Instant> {
        Some(self.deadline)
    }
}

impl QueryTicket {
    async fn admit(&self, service: &Service, call: &Call) -> Result<(), Status> {
        if tokio::time::Instant::now() >= self.deadline {
            return Err(Status::not_found("query ticket expired"));
        }
        self.context.check_cancelled().map_err(status)?;
        service.0.node.admit_retained_catalog(
            &self.context.catalog, call.session.caller.principal.as_ref(), &call.cancellation,
        ).await.map_err(status)
    }

    async fn open(&self, memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<QueryCursor, Status> {
        let (prepared, params) = match &self.input {
            QueryInput::Statement { prepared, params } => (prepared, params),
            QueryInput::Inspection(cursor) => return cursor.lock().expect("query ticket lock").take()
                .ok_or_else(|| Status::not_found("query inspection was already retrieved")),
        };
        let arguments = params.as_ref().map(|params| params.arguments(memory)).transpose()?;
        let types = arguments.as_ref().map(SqlArguments::types).unwrap_or_default();
        let context = Arc::clone(&self.context);
        if let Some(params) = params
            && let Some(cursor) = prepared.try_open_query_arrow(Arc::clone(&context), params.owned_batch(), types).await.map_err(status)?
        {
            return Ok(cursor);
        }
        let row = arguments.as_ref().map(|arguments| {
            arguments.rows().next().ok_or_else(|| invalid("Query parameter batch has no rows"))?.map_err(status)
        }).transpose()?;
        let row = row.as_deref().unwrap_or_default();
        let bound = row.iter().try_fold(wire::add(1024, wire::mul(row.len(), std::mem::size_of::<rtdb_types::Value>())?)?, |bytes, value| wire::add(bytes, value.retained_bytes().map_err(status)?))?;
        let _values = memory.reserve(bound).map_err(status)?;
        prepared.open_query_columns_typed(context, row.to_vec(), types).await.map_err(status)
    }
}

impl Service {
    pub async fn prepare_shape(&self, connection: Arc<ConnectionResource>, source: &str) -> Result<Shape, Status> {
        let mut native = connection.borrow().await?;
        let token = native.state().cancellation().child_token(); let _cancel = token.clone().drop_guard();
        let options = options(&native);
        if let Some(control) = rtdb_sql::parse_admitted_control_in(source, &options, self.memory()).map_err(status)? {
            native.prepare_context(&token).await.map_err(status)?;
            return Ok(Shape { schema: control_schema(control.statement()).map_err(status)?.as_ref().map(|schema| wire::plan_schema(schema, self.memory())).transpose()?, parameters: 0 });
        }
        let context = native.prepare_context(&token).await.map_err(status)?;
        let prepared = PreparedSql::prepare(source, &context, options, ResultSignaturePolicy::Dynamic)
            .map_err(status)?;
        let schema = if prepared.is_dml() { None } else {
            Some(self.parameterized_schema(&prepared, &context, None)?)
        };
        Ok(Shape { schema, parameters: prepared.parameter_count() })
    }
    fn parameterized_schema(
        &self, prepared: &PreparedSql, context: &Arc<QueryContext>, params: Option<&wire::BoundParameters>,
    ) -> Result<wire::OwnedSchema, Status> {
        let schema = if let Some(params) = params {
            let types = wire::ParameterTypes::from_batch(params.batch(), self.memory())?;
            prepared.result_schema(context, types.as_slice()).map_err(status)?
        } else {
            prepared.result_schema(context, prepared.parameter_types()).map_err(status)?
        };
        wire::plan_schema(schema.schema(), self.memory())
    }

    pub async fn describe(&self, connection: Arc<ConnectionResource>, source: &str) -> Result<(wire::OwnedSchema, usize), Status> {
        let shape = self.prepare_shape(connection, source).await?;
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
        let prepared = PreparedSql::prepare(source, &context, options, ResultSignaturePolicy::Dynamic)
            .map_err(status)?;
        execute_prepared_update(native, &prepared, arguments).await
    }

    fn empty_update_info(&self) -> Result<(wire::OwnedSchema, Bytes), Status> {
        Ok((wire::OwnedSchema::empty(self.memory())?, query_ticket(Ticket::Empty.encode(self.memory())?, self.memory())?))
    }

    pub async fn query_info(&self, call: &Call, connection: Arc<ConnectionResource>, source: &str, params: Option<Arc<wire::BoundParameters>>) -> Result<(wire::OwnedSchema, Bytes), Status> {
        let mut native = connection.borrow().await?;
        let token = native.state().cancellation().child_token();
        let options = options(&native);
        let (context, schema, input) = if let Some(control) = rtdb_sql::parse_admitted_control_in(source, &options, self.memory()).map_err(status)? {
            let Some(schema) = control_schema(control.statement()).map_err(status)? else {
                let arguments = params.as_ref().map(|params| params.arguments(self.memory())).transpose()?;
                execute_control_update(&mut native, control.statement(), arguments.as_ref(), &token).await?;
                return self.empty_update_info();
            };
            let output = native.execute_control_with(control.statement(), &[], &token, ControlExecutionOptions {
                frontend: ControlFrontend::FlightSql, implicit_transaction: false,
            }).await.map_err(status)?;
            let ControlResult::Rows(cursor) = output else { return Err(invalid("query has no result set")); };
            (Arc::clone(cursor.context()), wire::plan_schema(&schema, self.memory())?, QueryInput::Inspection(Mutex::new(Some(cursor))))
        } else {
            let prepared = {
                let context = native.prepare_context(&token).await.map_err(status)?;
                PreparedSql::prepare(source, &context, options.clone(), ResultSignaturePolicy::Dynamic).map_err(status)?
            };
            if prepared.is_dml() {
                let arguments = params.as_ref().map(|params| params.arguments(self.memory())).transpose()?;
                native.prepare_context(&token).await.map_err(status)?;
                execute_prepared_update(&mut native, &prepared, arguments).await?;
                return self.empty_update_info();
            }
            let context = native.query_context_with(&token, true).await.map_err(status)?;
            let prepared = PreparedSql::prepare(source, &context, options, ResultSignaturePolicy::Dynamic).map_err(status)?;
            let schema = self.parameterized_schema(&prepared, &context, params.as_deref())?;
            let params = params.filter(|params| params.batch().num_columns() != 0);
            (context, schema, QueryInput::Statement { prepared, params })
        };
        drop(native);
        context.reading(async { Ok(()) }).await.map_err(status)?;
        let id = Uuid::new_v4();
        let ticket = query_ticket(Ticket::Read(id).encode(self.memory())?, self.memory())?;
        let slot = Arc::clone(&self.0.query_ticket_slots).try_acquire_owned()
            .map_err(|_| Status::resource_exhausted("The node has too many retained query tickets"))?;
        let deadline = tokio::time::Instant::now() + QUERY_TICKET_LIFETIME;
        self.0.query_tickets.insert(id, Arc::new(QueryTicket {
            owner: call.session.owner(), connection, context, input, deadline, _slot: slot,
        }), QUERY_TICKET_LIFETIME, QUERY_TICKET_LIFETIME, false, 16).map_err(status)?;
        Ok((schema, ticket))
    }

    pub async fn query_stream(&self, call: Call, encoded: Bytes) -> Result<Response<Output<FlightData>>, Status> {
        let ticket = Ticket::decode(&encoded).map_err(|error| call.error(error))?;
        let Ticket::Read(id) = ticket else { return crate::service::result_stream(call, vec![Ok(wire::schema_data(&Schema::empty(), self.memory())?)]); };
        let work = async {
            let read = found(self.0.query_tickets.lookup(&id, false), "query ticket")?;
            if !call.session.owns(&read.owner) {
                return Err(Status::not_found("unknown query ticket"));
            }
            if read.connection.cancellation.is_cancelled() {
                return Err(Status::not_found("query connection closed"));
            }
            read.admit(self, &call).await?;
            let cursor = tokio::time::timeout_at(read.deadline, read.open(self.memory()))
                .await.map_err(|_| Status::not_found("query ticket expired"))??;
            Ok::<_, Status>((cursor, read))
        };
        let (mut cursor, read) = call.cancellation.run_until_cancelled(work).await.ok_or_else(cancelled).and_then(|result| result).map_err(|error| call.error(error))?;
        let schema = wire::plan_schema(cursor.schema(), self.memory()).map_err(|error| call.error(error))?;
        let headers = call.response(())?.metadata().clone(); let service = self.clone();
        let stream: Output<FlightData> = Box::pin(async_stream::try_stream! {
            let _call = call;
            yield wire::schema_data(&schema, service.memory())?;
            let mut encoder = match wire::Encoder::new(&schema, service.memory()) { Ok(encoder) => encoder, Err(error) => { cursor.record_consumer_error(); Err(error)?; unreachable!() } };
            loop {
                read.admit(&service, &_call).await?;
                let batch = cursor.encode_next_batch(|batch| {
                    let batch = rtdb_columnar::conform_batch(batch.clone(), &schema, service.memory()).map_err(status)?;
                    encoder.declared_batch(&batch, service.memory())
                });
                let encoded = _call.cancellation.run_until_cancelled(tokio::time::timeout_at(read.deadline, batch))
                    .await.ok_or_else(cancelled)?
                    .map_err(|_| Status::not_found("query ticket expired"))?
                    .map_err(status)?;
                let encoded = match encoded.transpose() {
                    Ok(Some(encoded)) => encoded,
                    Ok(None) => break,
                    Err(error) => { cursor.record_consumer_error(); Err(error)?; unreachable!() },
                };
                read.admit(&service, &_call).await?;
                for data in encoded {
                    {
                        let grant = cursor.delivery_grant().await.map_err(status)?;
                        grant.check().map_err(status)?;
                        grant.observe_answer();
                    }
                    yield data;
                }
            }
        });
        let mut response = Response::new(stream); *response.metadata_mut() = headers; Ok(response)
    }
}
async fn execute_control_update(native: &mut NativeSession, control: &ControlStatement, arguments: Option<&SqlArguments>, token: &CancellationToken) -> Result<(), Status> {
    let params = arguments.and_then(|arguments| arguments.rows().next()).transpose().map_err(status)?;
    let params = params.as_deref().unwrap_or_default();
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
