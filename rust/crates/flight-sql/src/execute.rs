use std::{collections::BTreeMap, sync::Arc};
use arrow_flight::FlightData;
use arrow_schema::Schema;
use bytes::Bytes;
use rtdb_columnar::MemoryBudget;
use rtdb_node::{ControlExecutionOptions, ControlFrontend, ControlResult, NativeSession, PreparedSql, ResultSignaturePolicy, control_schema};
use rtdb_plan::ControlStatement;
use rtdb_query::{BoundParameters, QueryContext, QueryCursor};
use rtdb_tx::SqlArguments;
use rtdb_types::{LogicalType, Timestamp};
use tokio_util::sync::{CancellationToken, DropGuard};
use tonic::{Response, Status};
use crate::{error::{cancelled, invalid, status}, service::{Call, Output, Service, query_ticket}, state::ConnectionResource, ticket::{Cut, Inspection, QueryRun, Ticket, TicketBasis}, wire};

pub(crate) struct Shape { pub schema: Option<wire::OwnedSchema>, pub parameters: usize, pub plan: Option<PreparedSql> }

struct Redemption {
    _connection: Arc<ConnectionResource>,
    _cancel: DropGuard,
    context: Arc<QueryContext>,
    deadline: Option<tokio::time::Instant>,
}

impl Redemption {
    async fn admit(&self, service: &Service, call: &Call) -> Result<(), Status> {
        if self.deadline.is_some_and(|deadline| tokio::time::Instant::now() >= deadline) {
            return Err(Status::not_found("query catalog lease expired"));
        }
        self.context.check_cancelled().map_err(status)?;
        service.0.node.admit_retained_catalog(
            &self.context.catalog, call.session.caller.principal.as_ref(), &call.cancellation,
        ).await.map_err(status)
    }

}

async fn within<T>(deadline: Option<tokio::time::Instant>, work: impl std::future::Future<Output = Result<T, Status>>) -> Result<T, Status> {
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, work).await.map_err(|_| Status::not_found("query catalog lease expired"))?,
        None => work.await,
    }
}

async fn open_query(prepared: &PreparedSql, context: Arc<QueryContext>, params: Option<&wire::BoundParameters>, memory: &Arc<MemoryBudget>) -> Result<QueryCursor, Status> {
    if let Some(params) = params {
        let types = params.validated_query_types(memory).await?;
        if let Some(cursor) = prepared.try_open_query_arrow(Arc::clone(&context), params.owned_batch(), types).await.map_err(status)? {
            return Ok(cursor);
        }
        let (row, _values) = params.first_query_row(types, memory)?;
        return prepared.open_query_columns_typed(context, row, types).await.map_err(status);
    }
    let _values = memory.reserve(1024).map_err(status)?;
    prepared.open_query_columns_typed(context, Vec::new(), &[]).await.map_err(status)
}

/// Each database's last visible system time in a captured catalog, so that a later read can be capped to the same cut.
fn visible_cut(context: &QueryContext) -> Result<BTreeMap<String, Option<Timestamp>>, Status> {
    context.catalog.database_names().map(|name| {
        let database = context.catalog.database(name).map_err(status)?;
        let visible = database.partitions.first().and_then(|snapshot| snapshot.tx_basis().map(|head| {
            snapshot.system_time_cap().map_or(head.system_time, |cap| cap.min(head.system_time))
        }));
        Ok((name.to_owned(), visible))
    }).collect()
}

impl Service {
    pub async fn prepare_shape(&self, connection: Arc<ConnectionResource>, source: &str) -> Result<Shape, Status> {
        let mut native = connection.borrow().await?;
        let token = native.state().cancellation().child_token(); let _cancel = token.clone().drop_guard();
        let options = options(&native);
        if let Some(control) = rtdb_sql::parse_admitted_control_in(source, &options, self.memory()).map_err(status)? {
            native.prepare_context(&token).await.map_err(status)?;
            return Ok(Shape { schema: control_schema(control.statement()).map_err(status)?.as_ref().map(|schema| wire::plan_schema(schema, self.memory())).transpose()?, parameters: 0, plan: None });
        }
        let context = native.prepare_context(&token).await.map_err(status)?;
        let prepared = PreparedSql::prepare(source, &context, options, ResultSignaturePolicy::Dynamic)
            .map_err(status)?;
        let schema = if prepared.is_dml() { None } else {
            Some(self.parameterized_schema(&prepared, &context, prepared.parameter_types())?)
        };
        Ok(Shape { schema, parameters: prepared.parameter_count(), plan: Some(prepared) })
    }
    fn parameterized_schema(
        &self, prepared: &PreparedSql, context: &Arc<QueryContext>, types: &[LogicalType],
    ) -> Result<wire::OwnedSchema, Status> {
        let schema = prepared.result_schema(context, types).map_err(status)?;
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
            return execute_control_update(native, control.statement(), arguments.as_ref(), &token, self.memory()).await;
        }
        let context = native.prepare_context(&token).await.map_err(status)?;
        let prepared = PreparedSql::prepare(source, &context, options, ResultSignaturePolicy::Dynamic)
            .map_err(status)?;
        execute_prepared_update(native, &prepared, arguments).await
    }

    fn empty_update_info(&self) -> Result<(wire::OwnedSchema, Bytes), Status> {
        Ok((wire::OwnedSchema::empty(self.memory())?, query_ticket(Ticket::empty(self.memory())?, self.memory())?))
    }

    pub async fn query_info(
        &self, call: &Call, connection: Arc<ConnectionResource>, transaction: Option<&Bytes>, source: &str, previous: Option<&PreparedSql>,
        params: Option<Arc<wire::BoundParameters>>,
    ) -> Result<(wire::OwnedSchema, Bytes), Status> {
        let mut native = connection.borrow().await?;
        let token = native.state().cancellation().child_token(); let _cancel = token.clone().drop_guard();
        let options = options(&native);
        let database = native.state().database().to_owned();
        let (schema, ticket) = if let Some(control) = rtdb_sql::parse_admitted_control_in(source, &options, self.memory()).map_err(status)? {
            let Some(schema) = control_schema(control.statement()).map_err(status)? else {
                let arguments = params.as_ref().map(|params| params.arguments(self.memory())).transpose()?;
                execute_control_update(&mut native, control.statement(), arguments.as_ref(), &token, self.memory()).await?;
                drop(control);
                return self.empty_update_info();
            };
            (wire::plan_schema(&schema, self.memory())?, Ticket::Inspect(Inspection { database, transaction: transaction.cloned(), sql: source.to_owned(), search_path: options.search_path }))
        } else {
            let prepared = {
                let context = native.prepare_context(&token).await.map_err(status)?;
                match previous.filter(|plan| plan.search_path() == options.search_path.as_slice()) {
                    Some(plan) => plan.clone(),
                    None => PreparedSql::prepare(source, &context, options.clone(), ResultSignaturePolicy::Dynamic).map_err(status)?,
                }
            };
            if prepared.is_dml() {
                let arguments = params.as_ref().map(|params| params.arguments(self.memory())).transpose()?;
                native.prepare_context(&token).await.map_err(status)?;
                execute_prepared_update(&mut native, &prepared, arguments).await?;
                return self.empty_update_info();
            }
            let (basis, context) = native.read_context(&token).await.map_err(status)?;
            let types = match &params {
                Some(params) => params.query_types(self.memory()).await?,
                None => prepared.parameter_types(),
            };
            let schema = self.parameterized_schema(&prepared, &context, types)?;
            drop(native);
            context.reading(async { Ok(()) }).await.map_err(status)?;
            let cut = match &basis.resume_token { Some(token) => Cut::Snapshot(token.clone()), None => Cut::Visible(visible_cut(&context)?) };
            let params = params.filter(|params| params.batch().num_columns() != 0)
                .map(|params| wire::parameter_ipc(params.batch(), self.memory())).transpose()?.flatten();
            let basis = TicketBasis {
                current_time: basis.current_time, timezone: basis.timezone, snapshot_time: basis.snapshot_time,
                has_await_floor: basis.has_await_floor, cut,
            };
            (schema, Ticket::Run(QueryRun { database, sql: source.to_owned(), search_path: options.search_path, basis, params }))
        };
        let ticket = ticket.encode(&self.ticket_binding(call), self.memory())?;
        if ticket.len() > self.0.config.maximum_message_bytes {
            return Err(Status::resource_exhausted("The query ticket exceeds the maximum message size"));
        }
        Ok((schema, query_ticket(ticket, self.memory())?))
    }

    async fn redeem(&self, call: &Call, ticket: Ticket) -> Result<Option<(QueryCursor, Redemption)>, Status> {
        let (sql, search_path, connection) = match &ticket {
            Ticket::Empty => return Ok(None),
            Ticket::Run(run) => (&run.sql, &run.search_path, self.connection(call, &run.database).await?),
            Ticket::Inspect(inspection) => (&inspection.sql, &inspection.search_path, match &inspection.transaction {
                Some(handle) => Arc::clone(&self.transaction(call, handle)?.connection),
                None => self.connection(call, &inspection.database).await?,
            }),
        };
        let mut native = connection.borrow().await?;
        let token = native.state().cancellation().child_token();
        let cancel = token.clone().drop_guard();
        let options = rtdb_sql::PrepareOptions { search_path: search_path.clone(), ..Default::default() };
        let control = rtdb_sql::parse_admitted_control_in(sql, &options, self.memory()).map_err(status)?;
        let cursor = match (&ticket, control) {
            (Ticket::Inspect(_), Some(control)) if control_schema(control.statement()).map_err(status)?.is_some() => {
                let output = native.execute_control_with(control.statement(), &[], &token, ControlExecutionOptions {
                    frontend: ControlFrontend::FlightSql, implicit_transaction: false,
                }).await.map_err(status)?;
                drop(native);
                let ControlResult::Rows(cursor) = output else { return Err(invalid("query has no result set")); };
                cursor
            }
            (Ticket::Run(run), None) => {
                let databases = self.0.node.catalog().database_names();
                let context = native.query_context_at(&run.basis.query_basis(databases), &token).await.map_err(status)?;
                drop(native);
                let prepared = PreparedSql::prepare(sql, &context, options, ResultSignaturePolicy::Dynamic).map_err(status)?;
                if prepared.is_dml() { return Err(invalid("invalid ticket")); }
                let params = run.params.as_ref()
                    .map(|bytes| wire::BoundParameters::new(wire::parameter_batch(bytes, self.memory())?, self.memory())).transpose()?;
                let deadline = context.retention_deadline().map(tokio::time::Instant::from);
                within(deadline, open_query(&prepared, context, params.as_deref(), self.memory())).await?
            }
            _ => return Err(invalid("invalid ticket")),
        };
        let context = Arc::clone(cursor.context());
        let deadline = context.retention_deadline().map(tokio::time::Instant::from);
        Ok(Some((cursor, Redemption { _connection: connection, _cancel: cancel, context, deadline })))
    }

    pub async fn query_stream(&self, call: Call, encoded: Bytes) -> Result<Response<Output<FlightData>>, Status> {
        let decoded = self.memory().reserve(wire::add(wire::mul(encoded.len(), 2)?, 1024)?).map_err(status)?;
        let ticket = Ticket::decode(&encoded, &self.ticket_binding(&call)).map_err(|error| call.error(error))?;
        let work = async {
            let _decoded = decoded;
            let redeemed = self.redeem(&call, ticket).await?;
            if let Some((_, read)) = &redeemed { read.admit(self, &call).await?; }
            Ok::<_, Status>(redeemed)
        };
        let redeemed = call.cancellation.run_until_cancelled(work).await.ok_or_else(cancelled).and_then(|result| result).map_err(|error| call.error(error))?;
        let Some((mut cursor, read)) = redeemed else {
            return crate::service::result_stream(call, vec![Ok(wire::schema_data(&Schema::empty(), self.memory())?)]);
        };
        let schema = wire::plan_schema(cursor.schema(), self.memory()).map_err(|error| call.error(error))?;
        let initial = cursor.delivery_grant().await.map_err(status)?;
        let (headers, cutoff) = call.delivery_headers(&initial, true)?;
        let service = self.clone();
        let stream: Output<FlightData> = Box::pin(async_stream::try_stream! {
            let _call = call;
            let schema_data = wire::schema_data(&schema, service.memory())?;
            _call.handoff(initial, cutoff)?;
            yield schema_data;
            let mut encoder = match wire::Encoder::new(&schema, service.memory()) { Ok(encoder) => encoder, Err(error) => { cursor.record_consumer_error(); Err(error)?; unreachable!() } };
            loop {
                read.admit(&service, &_call).await?;
                let batch = cursor.encode_next_batch(|batch| {
                    let batch = rtdb_columnar::conform_batch(batch.clone(), &schema, service.memory()).map_err(status)?;
                    encoder.declared_batch(&batch, service.memory())
                });
                let encoded = _call.cancellation.run_until_cancelled(within(read.deadline, async { batch.await.map_err(status) }))
                    .await.ok_or_else(cancelled)??;
                let encoded = match encoded.transpose() {
                    Ok(Some(encoded)) => encoded,
                    Ok(None) => break,
                    Err(error) => { cursor.record_consumer_error(); Err(error)?; unreachable!() },
                };
                read.admit(&service, &_call).await?;
                for data in encoded {
                    {
                        let grant = cursor.delivery_grant().await.map_err(status)?;
                        _call.handoff(grant, cutoff)?;
                    }
                    yield data;
                }
            }
        });
        let mut response = Response::new(stream); *response.metadata_mut() = headers; Ok(response)
    }
}
async fn execute_control_update(native: &mut NativeSession, control: &ControlStatement, arguments: Option<&SqlArguments>, token: &CancellationToken, memory: &Arc<rtdb_columnar::MemoryBudget>) -> Result<(), Status> {
    let options = ControlExecutionOptions { frontend: ControlFrontend::FlightSql, implicit_transaction: false };
    let result = if let Some(arguments) = arguments && let Some(page) = arguments.first_arrow_page() {
        let params = BoundParameters::from_arrow(page.clone(), 0, arguments.execution_types().collect(), memory).map_err(status)?;
        native.execute_control_arrow_with(control, Some(&params), token, options).await
    } else {
        let params = arguments.and_then(|arguments| arguments.rows().next()).transpose().map_err(status)?;
        native.execute_control_with(control, params.as_deref().unwrap_or_default(), token, options).await
    };
    match result.map_err(status)? {
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
