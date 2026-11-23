use std::{borrow::Cow, collections::BTreeMap};
use std::sync::Arc;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch};
use rtdb_node::{CommitResult, ControlExecutionOptions, ControlFrontend, ControlResult, NativeSession, Node, PreparedSql, ReplaySubmissionPermit, TransactionStatus, control_schema};
use rtdb_pg_codec::{Format, SessionEnvironment};
use rtdb_plan::{ControlStatement, DiscardTarget};
use rtdb_query::{BoundParameters, QueryCursor};
use rtdb_tx::SqlArguments;
use rtdb_sql::{AdmittedControl, BatchExecution, ContextualStatementDiagnostic, ExecutableSyntax, ExtendedStatement, SqlPrepareChild, SqlPrepareSyntax};
use rtdb_types::{Anomaly, PgType, Result, Value};
use tokio::io::AsyncWrite;
use tokio_util::sync::CancellationToken;
use crate::{GATEWAY_KEY_CONFIRM_KIND, GATEWAY_KEY_KIND, GATEWAY_NAMED_PARSE_KIND, GATEWAY_REPLAY_PERMIT_KIND, GATEWAY_SESSION_STATE_TAG, GATEWAY_SETTINGS_KIND, GATEWAY_SQL_PREPARE_KIND, GatewayReplayMode, GatewayReplayPermit, ServerConfig, connection::{Startup, apply_startup, display_parameter}, error::{cancelled, protocol}, server::CancellationSlot, wire::{Column, Frame, Input, Writer}};

fn bind_control_arguments(arguments: Option<&SqlArguments>, memory: &Arc<MemoryBudget>) -> Result<Option<BoundParameters>> {
    arguments.map(|args| {
        let batch = args.single_arrow_page().expect("portal owns one Arrow argument page");
        BoundParameters::from_arrow(batch.clone(), 0, args.execution_types().collect(), memory)
    }).transpose()
}

fn replay_requires_autocommit() -> Anomaly {
    Anomaly::unsupported("rtdb.pgwire/replay-command", "Automatic replay requires a single autocommit statement without transaction controls")
}

enum PreparedKind { Empty, Query(PreparedSql), CopyOut(PreparedSql, rtdb_plan::CopyOutFormat), Dml(PreparedSql), Control(PreparedControl), Keywords }
enum PreparedControl { Batch(ControlStatement), Extended(AdmittedControl) }
impl PreparedControl {
    fn statement(&self) -> &ControlStatement {
        match self {
            Self::Batch(statement) => statement,
            Self::Extended(control) => control.statement(),
        }
    }
}
struct PreparedShape {
    kind: PreparedKind,
    columns: Option<Vec<Column>>,
    parameter_count: usize,
}
enum PreparedBody {
    Ordinary { source: String, kind: PreparedKind },
    SqlPrepare(SqlPrepareSyntax),
}
struct Prepared { body: PreparedBody, oids: Vec<i32>, columns: Option<Vec<Column>>, _reservation: Arc<MemoryReservation> }
impl Prepared {
    fn ordinary_kind(&self) -> Option<&PreparedKind> {
        match &self.body {
            PreparedBody::Ordinary { kind, .. } => Some(kind),
            PreparedBody::SqlPrepare(_) => None,
        }
    }
    fn control(&self) -> Option<&ControlStatement> {
        match self.ordinary_kind() {
            Some(PreparedKind::Control(control)) => Some(control.statement()),
            _ => None,
        }
    }
}
struct RegisteredStatement { prepared: Arc<Prepared>, _reservation: Arc<MemoryReservation> }
struct QueryPortal { cursor: QueryCursor, batch: Option<OwnedBatch>, types: Vec<rtdb_types::LogicalType>, row: usize, sent: u64 }
enum PortalState { Query(QueryPortal), Ready }
struct Portal {
    prepared: Arc<Prepared>,
    arguments: Option<SqlArguments>,
    columns: Option<Vec<Column>>,
    state: PortalState,
    cancellation: CancellationToken,
    _charges: Vec<Arc<MemoryReservation>>,
    _bind_storage: Option<Arc<MemoryReservation>>,
}

impl Portal {
    fn retention_deadline(&self) -> Option<std::time::Instant> {
        match &self.state {
            PortalState::Query(query) => query.cursor.context().retention_deadline(),
            PortalState::Ready => None,
        }
    }
}

#[derive(Clone, Copy)]
struct PreparationSettings<'a> { search_path: &'a str, timezone: &'a str }

fn search_path(value: &str) -> Vec<String> {
    value.split(',').map(|name| name.trim().trim_matches('"').to_owned()).collect()
}

fn parse_frontend_definition(bytes: &[u8]) -> Result<(&str, &str, Vec<i32>)> {
    let mut input = Input::new(bytes);
    let name = input.string()?;
    let sql = input.string()?;
    let count = usize::from(input.short()?);
    let oids = (0..count).map(|_| input.integer()).collect::<Result<_>>()?;
    input.end()?;
    Ok((name, sql, oids))
}

pub(crate) struct ProtocolSession {
    session: NativeSession,
    node: Node,
    config: Arc<ServerConfig>,
    startup: Startup,
    cancellations: Arc<CancellationSlot>,
    extended: bool,
    skip_until_sync: bool,
    implicit: bool,
    transaction_generation: u64,
    reported_await_token: u64,
    reported_settings: u64,
    statements: BTreeMap<String, RegisteredStatement>,
    portals: BTreeMap<String, Portal>,
    copy: Option<crate::copy::CopyInput>,
    replay_permit: Option<GatewayReplayPermit>,
}
impl ProtocolSession {
    pub fn new(session: NativeSession, node: Node, config: Arc<ServerConfig>, startup: Startup, cancellations: Arc<CancellationSlot>) -> Self {
        let reported_settings = session.state().settings_revision();
        Self { session, node, config, startup, cancellations, extended: false, skip_until_sync: false, implicit: false, transaction_generation: 0,
            reported_await_token: 0, reported_settings, statements: BTreeMap::new(), portals: BTreeMap::new(), copy: None, replay_permit: None }
    }
    pub fn extended(&self) -> bool { self.extended }
    pub fn portal_deadline(&self) -> Option<std::time::Instant> {
        self.portals.values().filter_map(Portal::retention_deadline).min()
    }
    pub fn expire_portals(&mut self, now: std::time::Instant) {
        self.portals.retain(|_, portal| portal.retention_deadline().is_none_or(|deadline| deadline > now));
    }
    pub fn close(&mut self) { self.copy = None; self.portals.clear(); self.statements.clear(); self.session.state_mut().close(); }
    fn environment(&self) -> Result<SessionEnvironment> { SessionEnvironment::new(self.session.state().parameter_entries(), self.session.state().effective_timezone(), self.node.memory()) }
    fn token(&self) -> CancellationToken { self.session.state().cancellation().child_token() }
    fn register_statement(&mut self, name: Cow<'_, str>, prepared: Arc<Prepared>) -> Result<()> {
        let capacity = match &name { Cow::Borrowed(name) => name.len(), Cow::Owned(name) => name.capacity() };
        let reservation = self.node.memory().reserve(capacity.checked_add(512).ok_or_else(|| protocol("Statement name is too large"))?)?;
        self.statements.insert(name.into_owned(), RegisteredStatement { prepared, _reservation: reservation });
        Ok(())
    }
    fn ready(&self) -> u8 { match self.session.state().status() { TransactionStatus::Idle | TransactionStatus::Closed => b'I', TransactionStatus::Failed => b'E', _ => b'T' } }
    async fn report_ready<W: AsyncWrite + Unpin>(&mut self, writer: &mut Writer<W>) -> Result<()> {
        let status = self.ready();
        if status == b'I' && self.startup.gateway_session_state {
            let (revision, token) = self.session.state().connection_await_token();
            if revision != self.reported_await_token {
                writer.gateway_await_token(token).await?;
                self.reported_await_token = revision;
            }
            let revision = self.session.state().settings_revision();
            if self.startup.gateway_settings && revision != self.reported_settings {
                let (encoded, _reservation) = self.session.state().encode_gateway_settings()?;
                writer.gateway_settings(&encoded).await?;
                self.reported_settings = revision;
            }
        }
        writer.ready(status).await
    }
    fn options(&self) -> rtdb_sql::PrepareOptions { rtdb_sql::PrepareOptions { search_path: search_path(self.session.state().parameter("search_path").unwrap_or("public")), ..Default::default() } }
    fn begin_implicit(&mut self, read_only: Option<bool>) -> Result<()> { if self.session.state().status() != TransactionStatus::Idle { return Ok(()); } let mut settings = rtdb_node::TransactionSettings::default(); settings.access = read_only.map(|read_only| if read_only { rtdb_node::AccessSettings::ReadOnly(Default::default()) } else { rtdb_node::AccessSettings::ReadWrite(Default::default()) }); self.session.state_mut().begin_with(settings)?; self.implicit = true; Ok(()) }
    fn end_transaction(&mut self) { self.portals.clear(); self.implicit = false; self.transaction_generation = self.transaction_generation.wrapping_add(1); }
    async fn commit(&mut self, permit: Option<ReplaySubmissionPermit>) -> Result<()> {
        let _sample = rtdb_plan::ExecutionSample::new(self.node.execution_observer(), rtdb_plan::ExecutionTimer::PgwireTransaction);
        let state = self.session.state_mut();
        let result = match permit {
            Some(permit) => state.commit_tx_with_replay_permit(permit).await,
            None => state.commit_tx(None).await,
        };
        self.end_transaction();
        if let CommitResult::Executed(tx) = result? && let Some(error) = tx.error() { return Err(error.clone()); }
        Ok(())
    }
    fn rollback(&mut self) -> Result<()> { self.session.state_mut().rollback_tx()?; self.end_transaction(); Ok(()) }

    pub async fn handle<W: AsyncWrite + Unpin>(&mut self, frame: Frame, writer: &mut Writer<W>) -> Result<()> {
        if self.replay_permit.is_some() && frame.tag != b'Q' {
            return Err(protocol("A gateway replay permit must be followed by a simple query"));
        }
        if frame.tag == GATEWAY_SESSION_STATE_TAG && frame.body.first() == Some(&GATEWAY_REPLAY_PERMIT_KIND) {
            if !self.startup.gateway_session_state || self.extended || self.copy.is_some() || self.ready() != b'I' {
                return Err(protocol("A gateway replay permit requires an idle gateway session"));
            }
            let permit = GatewayReplayPermit::decode(&frame.body).ok_or_else(|| protocol("Invalid gateway replay permit"))?;
            self.session.state_mut().set_gateway_key(permit.nonce.simple().to_string())?;
            self.replay_permit = Some(permit);
            if permit.mode != GatewayReplayMode::Dispatch { writer.ready(b'I').await?; }
            return Ok(());
        }
        if frame.tag == GATEWAY_SESSION_STATE_TAG && let Some((&kind @ (GATEWAY_KEY_KIND | GATEWAY_KEY_CONFIRM_KIND), key)) = frame.body.split_first() {
            // Failure here ends the connection before it can execute the following command.
            if !self.startup.gateway_session_state || self.extended || self.copy.is_some() || self.ready() != b'I' {
                return Err(protocol("A gateway key requires an idle gateway session"));
            }
            let key = std::str::from_utf8(key).ok().filter(|key| key.len() == 32 && key.bytes().all(|byte| byte.is_ascii_hexdigit()))
                .ok_or_else(|| protocol("Invalid gateway key"))?;
            self.session.state_mut().set_gateway_key(key.to_owned())?;
            if kind == GATEWAY_KEY_CONFIRM_KIND { writer.ready(b'I').await?; }
            return Ok(());
        }
        if self.skip_until_sync && frame.tag != b'S' { return Ok(()); }
        if self.copy.is_some() && matches!(frame.tag, b'S' | b'H') { return Ok(()); }
        let result = self.message(&frame, writer).await;
        if let Err(error) = result {
            if error.code == "xtdb.pgwire/connection-error" { return Err(error); }
            if self.extended { self.skip_until_sync = true; }
            self.session.state_mut().fail_transaction();
            writer.error(&error, self.environment()?.fallback(), false).await?;
        }
        Ok(())
    }

    async fn message<W: AsyncWrite + Unpin>(&mut self, frame: &Frame, writer: &mut Writer<W>) -> Result<()> {
        let mut input = Input::new(&frame.body);
        match frame.tag {
            b'Q' => { let sql = input.string()?; input.end()?; self.simple(sql, writer).await?; }
            b'P' => {
                self.extended = true;
                let (name, sql, oids) = parse_frontend_definition(&frame.body)?;
                let prepared = self.prepare_client(sql, oids, None).await.inspect_err(|_| { if self.session.state().is_read_write() { self.record_transaction_error(); } })?;
                Self::planning_warnings(&prepared, writer).await?;
                self.register_statement(Cow::Borrowed(name), prepared)?;
                writer.empty(b'1').await?;
                if self.startup.gateway_settings && !name.is_empty() {
                    writer.gateway_named_parse(name, self.session.state().parameter("search_path").unwrap_or("public"),
                        self.session.state().effective_timezone(), &frame.body).await?;
                }
            }
            b'B' => {
                self.extended = true;
                let name = input.string()?; let statement = input.string()?;
                let prepared = Arc::clone(&self.statements.get(statement).ok_or_else(|| protocol("no prepared statement"))?.prepared);
                let count = usize::from(input.short()?); let formats = (0..count).map(|_| Format::from_code(input.short()? as i16)).collect::<Result<Vec<_>>>()?;
                let count = usize::from(input.short()?);
                if matches!(prepared.ordinary_kind(), Some(PreparedKind::Dml(_))) && count != prepared.oids.len() { self.record_transaction_error(); return Err(Anomaly::incorrect("xtdb.pgwire/missing-arg-types", "Missing types for args - client must specify types for all non-null params in DML statements")); }
                let charge_slots = count + 2;
                let slot_bytes = std::mem::size_of::<arrow_schema::Field>() + std::mem::size_of::<arrow_array::ArrayRef>() + 32;
                let bytes = count.checked_mul(slot_bytes)
                    .and_then(|bytes| bytes.checked_add(charge_slots.checked_mul(std::mem::size_of::<Arc<MemoryReservation>>())?))
                    .and_then(|bytes| bytes.checked_add(name.len()))
                    .and_then(|bytes| bytes.checked_add(512)).ok_or_else(|| protocol("Portal argument slots are too large"))?;
                let storage = self.node.memory().reserve(bytes)?;
                let mut fields = Vec::<arrow_schema::Field>::new();
                let mut columns = Vec::<arrow_array::ArrayRef>::new();
                let mut charges = Vec::new();
                for reserve in [fields.try_reserve_exact(count), columns.try_reserve_exact(count), charges.try_reserve_exact(charge_slots)] {
                    reserve.map_err(|cause| Anomaly::incorrect("rtdb/allocation-failed", cause.to_string()))?;
                }
                for index in 0..count {
                    let length = input.integer()?;
                    let name = format!("?_{index}");
                    if length == -1 {
                        let ty = prepared.oids.get(index).and_then(|oid| PgType::from_oid(*oid))
                            .filter(|ty| matches!(ty, PgType::Float4s | PgType::Float8s))
                            .and_then(PgType::logical_type).map(rtdb_types::LogicalType::maybe).unwrap_or(rtdb_types::LogicalType::Null);
                        let field = rtdb_columnar::to_field(&name, &ty)?;
                        columns.push(arrow_array::new_null_array(field.data_type(), 1));
                        fields.push(field);
                        continue;
                    }
                    let bytes = input.take(usize::try_from(length).map_err(|_| protocol("Negative argument length"))?)?;
                    let oid = *prepared.oids.get(index).ok_or_else(|| protocol("Argument count differs from parameter OIDs"))?;
                    if oid == 0 && matches!(prepared.ordinary_kind(), Some(PreparedKind::Dml(_))) { self.record_transaction_error(); return Err(Anomaly::incorrect("xtdb.pgwire/missing-arg-types", "Missing types for args - client must specify types for all non-null params in DML statements")); }
                    let ty = PgType::from_oid(oid).ok_or_else(|| Anomaly::unsupported("xtdb.pgwire/unsupported-param-type", format!("Unsupported param type OID {oid}")))?;
                    let format = item_format(&formats, count, index)?;
                    let (field, column, charge) = rtdb_pg_codec::decode_argument(&name, ty, format, bytes, self.node.memory())
                        .map_err(|error| invalid_argument(error, index, format))?;
                    fields.push(field);
                    columns.push(column);
                    charges.push(charge);
                }
                let requires_exact_arity = matches!(prepared.ordinary_kind(), Some(PreparedKind::Query(_) | PreparedKind::CopyOut(..)))
                    || matches!(prepared.control(), Some(ControlStatement::Execute { .. }));
                if requires_exact_arity && count != prepared.oids.len() { return Err(protocol("Argument count differs from prepared statement")); }
                let count = usize::from(input.short()?); let result_formats = (0..count).map(|_| Format::from_code(input.short()? as i16)).collect::<Result<Vec<_>>>()?; input.end()?;
                if name.is_empty() { self.portals.remove(name); } else if self.portals.contains_key(name) { return Err(protocol("Named portals must be explicit closed before they can be redefined")); }
                let arguments = if columns.is_empty() { None } else {
                    let batch = arrow_array::RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns)
                        .map_err(|error| protocol(error.to_string()))?;
                    Some(OwnedBatch::new(batch, self.node.memory())?)
                };
                drop(charges);
                let arguments = arguments.map(|batch| SqlArguments::from_canonical_arrow_row(batch, self.node.memory())).transpose()?;
                let mut portal = self.bind(prepared, arguments, vec![Arc::clone(&frame.charge)], &result_formats).await?;
                portal._bind_storage = Some(storage);
                self.portals.insert(name.to_owned(), portal); writer.empty(b'2').await?;
            }
            b'D' => {
                let kind = input.byte()?; let name = input.string()?; input.end()?;
                match kind {
                    b'S' => { if let Some(statement) = self.statements.get(name) { let statement = &statement.prepared; if !matches!(statement.ordinary_kind(), Some(PreparedKind::Empty | PreparedKind::Keywords)) { writer.parameters(&statement.oids).await?; } Self::description(statement.columns.as_deref(), self.environment()?.fallback(), writer).await?; } else { writer.empty(b'n').await?; } },
                    b'P' => Self::description(self.portals.get(name).and_then(|portal| portal.columns.as_deref()), self.environment()?.fallback(), writer).await?,
                    _ => return Err(protocol("Invalid describe target")),
                }
            }
            b'E' => {
                let name = input.string()?.to_owned(); let limit = input.integer()?; input.end()?; if limit < 0 { return Err(protocol("Execute row limit cannot be negative")); }
                let generation = self.transaction_generation;
                let mut portal = self.portals.remove(&name).ok_or_else(|| protocol("no such portal"))?;
                let result = self.execute(&mut portal, limit as usize, false, None, writer).await;
                if generation == self.transaction_generation { self.portals.insert(name, portal); }
                result?;
            }
            b'C' => { let kind = input.byte()?; let name = input.string()?; input.end()?; match kind { b'P' => { self.portals.remove(name); }, b'S' => { if let Some(statement) = self.statements.remove(name) { self.portals.retain(|_, portal| !Arc::ptr_eq(&portal.prepared, &statement.prepared)); } }, _ => return Err(protocol("Invalid close target")) } writer.empty(b'3').await?; }
            b'S' => { input.end()?; if self.implicit { let result = if self.session.state().status() == TransactionStatus::Failed { self.rollback() } else { self.commit(None).await }; if let Err(error) = result { writer.error(&error, self.environment()?.fallback(), false).await?; } } self.skip_until_sync = false; self.extended = false; self.report_ready(writer).await?; }
            b'H' => { input.end()?; writer.flush().await?; }
            b'd' => { self.copy.as_mut().ok_or_else(|| protocol("COPY IN not in progress, cannot write data"))?.append(input.remaining()).await?; }
            b'c' => {
                input.end()?;
                let copy = self.copy.take().ok_or_else(|| protocol("COPY IN not in progress"))?;
                let token = self.token();
                let result = copy.finish(&mut self.session, &self.node, &token).await;
                if self.session.state().status() == TransactionStatus::Idle { self.end_transaction(); }
                match result {
                    Ok(rows) => writer.string(b'C', &format!("COPY {rows}")).await?,
                    Err(error) if self.extended => return Err(error),
                    Err(error) => writer.error(&error, self.environment()?.fallback(), false).await?,
                }
                if !self.extended { self.report_ready(writer).await?; }
            }
            b'f' => {
                self.copy = None;
                if self.extended { return Err(protocol("COPY input was cancelled")); }
                self.report_ready(writer).await?;
            }
            GATEWAY_SESSION_STATE_TAG if self.startup.gateway_session_state => {
                if self.copy.is_some() || self.ready() != b'I' {
                    return Err(protocol("A gateway can restore state only on an idle session"));
                }
                match input.byte()? {
                    0 => {
                        input.end()?;
                        self.session.state_mut().set_await_token(None)?;
                        self.reported_await_token = self.session.state().connection_await_token().0;
                    }
                    1 => {
                        let token = std::str::from_utf8(input.remaining()).map_err(|_| protocol("The gateway await token is not UTF-8"))?.to_owned();
                        self.session.state_mut().set_await_token(Some(token))?;
                        self.reported_await_token = self.session.state().connection_await_token().0;
                    }
                    GATEWAY_SETTINGS_KIND if self.startup.gateway_settings => {
                        self.session.state_mut().restore_gateway_settings(input.remaining())?;
                        self.reported_settings = self.session.state().settings_revision();
                    }
                    GATEWAY_SQL_PREPARE_KIND if self.startup.gateway_settings => {
                        let name = input.string()?;
                        let source = input.string()?;
                        let settings = PreparationSettings { search_path: input.string()?, timezone: input.string()? };
                        input.end()?;
                        self.restore_sql_prepare(name, source, settings).await?;
                    }
                    GATEWAY_NAMED_PARSE_KIND if self.startup.gateway_settings => {
                        let expected = input.string()?;
                        let settings = PreparationSettings { search_path: input.string()?, timezone: input.string()? };
                        let (name, sql, oids) = parse_frontend_definition(input.remaining())?;
                        if name != expected || name.is_empty() { return Err(protocol("The gateway's Parse name does not match its definition")); }
                        let prepared = self.prepare_client(sql, oids, Some(settings)).await?;
                        self.register_statement(Cow::Borrowed(name), prepared)?;
                    }
                    _ => return Err(protocol("Invalid gateway session state")),
                }
            }
            b'p' => (),
            _ => return Err(Anomaly::unsupported("xtdb.pgwire/unknown-client-msg", format!("unknown client message: {}", frame.tag))),
        }
        Ok(())
    }

    async fn prepare_client(&mut self, original: &str, oids: Vec<i32>, settings: Option<PreparationSettings<'_>>) -> Result<Arc<Prepared>> {
        let input = rtdb_sql::parse_extended_statement(original, self.node.memory())?;
        let source = match input.statement {
            ExtendedStatement::Empty => "",
            ExtendedStatement::Multiple => return Err(protocol("A prepared statement must contain exactly one statement")),
            ExtendedStatement::Ordinary(source) => source,
            ExtendedStatement::Prepare(syntax) => return self.retain_sql_prepare(syntax, oids),
        };
        let source = replace_namespace_query(source, (source, ""));
        let mut options = self.options();
        if let Some(settings) = settings { options.search_path = search_path(settings.search_path); }
        let shape = if source.is_empty() {
            PreparedShape { kind: PreparedKind::Empty, columns: None, parameter_count: 0 }
        } else if legacy_keyword_probe(source) {
            keyword_shape()
        } else if let Some(control) = rtdb_sql::parse_admitted_control_in(source, &options, self.node.memory())
            .map_err(|error| extended_syntax_error(source, original, error, self.node.memory()))? {
            match control.statement() {
                ControlStatement::CopyOut { query, format } => self.prepare_copy_out(query, *format, options, &oids, settings).await?,
                _ => self.execute_columns(control_shape(PreparedControl::Extended(control), oids.len())?),
            }
        } else {
            let context = self.session.prepare_context_with_timezone(settings.map(|settings| settings.timezone), &self.token()).await?;
            let _rewrite_syntax = admit_rewrite_syntax(original.len(), source.len(), &context.memory)?;
            let syntax = rtdb_sql::parse_syntax_in(source, &context.memory)
                .map_err(|error| extended_syntax_error(source, original, error, &context.memory))?;
            sql_shape(PreparedSql::prepare_postgres_syntax(syntax, &context, options.search_path, &oids)?, oids.len())
        };
        self.retain_prepared(source, oids, shape)
    }

    async fn prepare_batch_command(&mut self, syntax: ExecutableSyntax<'_>, allocation: Arc<MemoryReservation>, settings: Option<PreparationSettings<'_>>) -> Result<Arc<Prepared>> {
        let source = syntax.source();
        let mut options = self.options();
        if let Some(settings) = settings { options.search_path = search_path(settings.search_path); }
        let shape = match syntax {
            ExecutableSyntax::FrontendResponse(_) => keyword_shape(),
            ExecutableSyntax::Control(control) => match control.bind(&options)? {
                ControlStatement::CopyOut { query, format } => self.prepare_copy_out(&query, format, options, &[], settings).await?,
                control => self.execute_columns(control_shape(PreparedControl::Batch(control), 0)?),
            },
            ExecutableSyntax::Sql(syntax) => {
                let context = self.session.prepare_context_with_timezone(settings.map(|settings| settings.timezone), &self.token()).await?;
                sql_shape(PreparedSql::prepare_postgres_statement(syntax, &context, options.search_path, &[])?, 0)
            }
        };
        drop(allocation);
        self.retain_prepared(source, Vec::new(), shape)
    }

    async fn prepare_copy_out(
        &mut self, query: &str, format: rtdb_plan::CopyOutFormat, mut options: rtdb_sql::PrepareOptions, oids: &[i32], settings: Option<PreparationSettings<'_>>,
    ) -> Result<PreparedShape> {
        let context = self.session.prepare_context_with_timezone(settings.map(|settings| settings.timezone), &self.token()).await?;
        for (index, &oid) in oids.iter().enumerate() {
            if let Some(ty) = PgType::from_oid(oid).and_then(PgType::logical_type) {
                options.parameter_types.insert(index, ty.maybe());
            }
        }
        let query = PreparedSql::prepare(query, &context, options, rtdb_node::ResultSignaturePolicy::Dynamic)?;
        if query.is_dml() { return Err(protocol("COPY TO requires a read query")); }
        let parameter_count = query.parameter_count();
        if oids.len() > parameter_count { return Err(protocol("Too many COPY parameter types")); }
        Ok(PreparedShape { kind: PreparedKind::CopyOut(query, format), columns: None, parameter_count })
    }

    fn retain_prepared(&self, source: &str, mut oids: Vec<i32>, shape: PreparedShape) -> Result<Arc<Prepared>> {
        let PreparedShape { kind, columns, parameter_count } = shape;
        let oid_capacity = oids.capacity().max(parameter_count);
        let column_bytes = columns.as_deref().unwrap_or_default().iter().try_fold(0usize, |bytes, column| bytes.checked_add(column.name.capacity()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(|| protocol("Column descriptors are too large")))?;
        let reservation = self.node.memory().reserve(source.len().checked_mul(128).and_then(|n| n.checked_add(parameter_count.checked_mul(256)?)).and_then(|n| n.checked_add(oid_capacity.checked_mul(std::mem::size_of::<i32>())?)).and_then(|n| n.checked_add(column_bytes)).and_then(|n| n.checked_add(8192)).ok_or_else(|| protocol("Prepared statement is too large"))?)?;
        oids.try_reserve_exact(parameter_count.saturating_sub(oids.len()))
            .map_err(|cause| Anomaly::incorrect("rtdb/allocation-failed", cause.to_string()))?;
        oids.resize(parameter_count, 0);
        Ok(Arc::new(Prepared { body: PreparedBody::Ordinary { source: source.into(), kind }, oids, columns, _reservation: reservation }))
    }

    fn retain_sql_prepare(&self, syntax: SqlPrepareSyntax, oids: Vec<i32>) -> Result<Arc<Prepared>> {
        let bound = oids.capacity().checked_mul(std::mem::size_of::<i32>())
            .and_then(|bytes| bytes.checked_add(8192)).ok_or_else(|| protocol("Prepared statement is too large"))?;
        let reservation = self.node.memory().reserve(bound)?;
        Ok(Arc::new(Prepared { body: PreparedBody::SqlPrepare(syntax), oids, columns: None, _reservation: reservation }))
    }

    async fn install_sql_prepare<W: AsyncWrite + Unpin>(&mut self, name: Cow<'_, str>, source: &str, prepared: Arc<Prepared>, writer: &mut Writer<W>) -> Result<()> {
        Self::planning_warnings(&prepared, writer).await?;
        if self.startup.gateway_settings && !name.is_empty() {
            self.register_statement(Cow::Borrowed(&name), prepared)?;
            writer.gateway_sql_prepare(&name, source,
                self.session.state().parameter("search_path").unwrap_or("public"), self.session.state().effective_timezone()).await?;
        } else {
            self.register_statement(name, prepared)?;
        }
        writer.string(b'C', "PREPARE").await
    }

    async fn restore_sql_prepare(&mut self, name: &str, source: &str, settings: PreparationSettings<'_>) -> Result<()> {
        let memory = Arc::clone(self.node.memory());
        let parsed = rtdb_sql::parse_extended_statement(source, &memory)?;
        let ExtendedStatement::Prepare(syntax) = parsed.statement else { return Err(protocol("The gateway's statement is not a PREPARE")); };
        if syntax.name() != name { return Err(protocol("The gateway's PREPARE name does not match its definition")); }
        let _admission = self.session.admit_frontend_prepare(&[], &self.token()).await?;
        let prepared = self.prepare_sql_child(syntax.child(&memory)?, Some(settings)).await?;
        self.register_statement(Cow::Borrowed(name), prepared)
    }

    async fn planning_warnings<W: AsyncWrite + Unpin>(prepared: &Prepared, writer: &mut Writer<W>) -> Result<()> {
        if let Some(PreparedKind::Query(statement) | PreparedKind::CopyOut(statement, _) | PreparedKind::Dml(statement)) = prepared.ordinary_kind() {
            for notice in statement.notices() { writer.planning_warning(&notice.message).await?; }
        }
        Ok(())
    }

    async fn bind(&mut self, prepared: Arc<Prepared>, arguments: Option<SqlArguments>, mut charges: Vec<Arc<MemoryReservation>>, formats: &[Format]) -> Result<Portal> {
        let token = self.token();
        let bound = prepared.columns.as_deref().unwrap_or_default().iter().try_fold(8192usize, |bytes, column| bytes.checked_add(column.name.len()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(|| protocol("Portal descriptors are too large")))?;
        charges.push(self.node.memory().reserve(bound)?);
        let columns = prepared.columns.as_ref().map(|columns| columns.iter().enumerate().map(|(index, column)| Ok(Column { name: column.name.clone(), ty: column.ty, format: item_format(formats, columns.len(), index)? })).collect::<Result<Vec<_>>>()).transpose()?;
        let mut portal = Portal { prepared: Arc::clone(&prepared), arguments, columns, state: PortalState::Ready, cancellation: token.clone(), _charges: charges, _bind_storage: None };
        let control_options = self.control_options();
        let cursor = match (prepared.ordinary_kind(), prepared.control()) {
            (Some(PreparedKind::CopyOut(query, _)), _) => {
                if self.session.state().is_read_write() { return Err(protocol("COPY TO requires a read transaction")); }
                let _active = self.cancellations.enter(&token);
                let context = self.session.query_context_with(&token, true).await?;
                let arguments = portal.arguments.take();
                Some(token.run_until_cancelled(async {
                    if let Some(args) = arguments {
                        let page = args.single_arrow_page().expect("portal owns one Arrow argument page");
                        query.try_open_query_arrow(context, page, args.types()).await?.ok_or_else(|| protocol("Portal arguments have no rows"))
                    } else {
                        query.open_query_columns_typed(context, Vec::new(), &[]).await
                    }
                }).await.ok_or_else(cancelled)??)
            },
            (Some(PreparedKind::Query(query)), _) => {
                let _active = self.cancellations.enter(&token);
                let checked = !self.session.state().is_read_write();
                let context = self.session.query_context_with(&token, checked).await?;
                let arguments = portal.arguments.take();
                Some(token.run_until_cancelled(async {
                    if let Some(args) = arguments {
                        let page = args.single_arrow_page().expect("portal owns one Arrow argument page");
                        query.open_query_arrow_rows(context, page, args.types()).await
                    } else {
                        query.open_query(context, Vec::new()).await
                    }
                }).await.ok_or_else(cancelled)?.map_err(|error| if token.is_cancelled() { cancelled() } else { error })?)
            },
            (_, Some(statement @ ControlStatement::Show { .. })) => {
                let args = bind_control_arguments(portal.arguments.as_ref(), self.node.memory())?;
                match self.session.execute_control_arrow_with(statement, args.as_ref(), &token, control_options).await? {
                    ControlResult::Rows(cursor) => Some(cursor),
                    _ => return Err(protocol("SHOW did not produce rows")),
                }
            },
            (_, Some(statement @ ControlStatement::Execute { .. })) => {
                let args = bind_control_arguments(portal.arguments.as_ref(), self.node.memory())?;
                let ControlResult::ExecuteArrow { name, arguments, allocation: _allocation } =
                    self.session.execute_control_arrow_with(statement, args.as_ref(), &token, control_options).await?
                else { return Err(protocol("EXECUTE did not produce arguments")); };
                let inner = Arc::clone(&self.statements.get(&name).ok_or_else(|| protocol("no prepared statement"))?.prepared);
                if !matches!(inner.ordinary_kind(), Some(PreparedKind::Query(_) | PreparedKind::Dml(_))) { return Err(Anomaly::unsupported("xtdb.pgwire/unsupported-execute", "EXECUTE only supports a prepared query or DML statement")); }
                if matches!(inner.ordinary_kind(), Some(PreparedKind::Query(_))) && arguments.batch().num_columns() != inner.oids.len() { return Err(protocol("EXECUTE argument count differs from prepared statement")); }
                let arguments = if arguments.batch().num_columns() == 0 { None } else {
                    Some(SqlArguments::from_canonical_arrow_row(arguments, self.node.memory())?)
                };
                return Box::pin(self.bind(inner, arguments, Vec::new(), formats)).await;
            }
            _ => None,
        };
        if let Some(cursor) = cursor {
            if let (Some(PreparedKind::Control(_)), Some(columns)) = (prepared.ordinary_kind(), &portal.columns) {
                let actual = cursor.runtime_schema();
                if columns.len() != actual.len() || columns.iter().zip(actual.fields()).any(|(column, field)| !column.ty.accepts_result(&field.data_type).unwrap_or(false)) { return Err(Anomaly::new(rtdb_types::AnomalyCategory::Conflict, "xtdb/prepared-query-out-of-date", "cached plan must not change result type")); }
            }
            portal.state = PortalState::Query(QueryPortal { cursor, batch: None, types: Vec::new(), row: 0, sent: 0 });
        }
        Ok(portal)
    }
    /// An EXECUTE describes the rows of the statement it names, as that statement is prepared when the EXECUTE is.
    fn execute_columns(&self, mut shape: PreparedShape) -> PreparedShape {
        if let PreparedKind::Control(control) = &shape.kind && let ControlStatement::Execute { name, .. } = control.statement() {
            shape.columns = self.statements.get(name).and_then(|statement| statement.prepared.columns.clone());
        }
        shape
    }
    fn control_options(&self) -> ControlExecutionOptions { ControlExecutionOptions { frontend: ControlFrontend::PostgreSql, implicit_transaction: self.implicit } }
    fn record_transaction_error(&self) { if let Some(observer) = self.node.execution_observer() { observer.record_error(rtdb_plan::ExecutionError::Transaction); } }
    async fn description<W: AsyncWrite + Unpin>(columns: Option<&[Column]>, fallback: PgType, writer: &mut Writer<W>) -> Result<()> { match columns { Some(columns) => writer.columns(columns, fallback).await, None => writer.empty(b'n').await } }

    async fn execute<W: AsyncWrite + Unpin>(&mut self, portal: &mut Portal, limit: usize, commit_dml_before_reply: bool, permit: Option<ReplaySubmissionPermit>, writer: &mut Writer<W>) -> Result<()> {
        let prepared = Arc::clone(&portal.prepared);
        let failed = self.session.state().status() == TransactionStatus::Failed;
        let control = prepared.control();
        if failed && !matches!(control, Some(ControlStatement::Rollback)) { return Err(protocol("current transaction is aborted, commands ignored until ROLLBACK is received")); }
        let (kind, source) = match &prepared.body {
            PreparedBody::Ordinary { kind, source } => (kind, source.as_str()),
            PreparedBody::SqlPrepare(syntax) => return self.execute_sql_prepare(syntax, portal, writer).await,
        };
        match (kind, control) {
            (PreparedKind::Empty, _) => writer.empty(b'I').await,
            (PreparedKind::Keywords, _) => { let env = self.environment()?; let value = rtdb_pg_codec::encode(PgType::VarChar, Format::Text, &Value::Utf8("xtdb".into()), &env, self.node.memory())?; writer.row(&[value]).await?; writer.string(b'C', "SELECT 1").await },
            (PreparedKind::Query(_), _) | (PreparedKind::Control(_), Some(ControlStatement::Show { .. })) => {
                if matches!(kind, PreparedKind::Query(_)) {
                    if self.session.state().is_read_write() && !is_type_probe(source) { return Err(Anomaly::incorrect("xtdb/queries-in-read-write-tx", "Queries are unsupported in a DML transaction").with_context("query", source.into())); }
                    self.begin_implicit(Some(true))?;
                }
                let token = portal.cancellation.clone(); let _active = self.cancellations.enter(&token);
                token.run_until_cancelled(self.stream(portal, source, limit, writer)).await.ok_or_else(cancelled)?
                    .map_err(|error| if token.is_cancelled() { cancelled() } else { error })
            }
            (PreparedKind::CopyOut(_, format), _) => {
                if limit != 0 { return Err(protocol("COPY TO cannot suspend a portal")); }
                self.begin_implicit(Some(true))?;
                let PortalState::Query(query) = std::mem::replace(&mut portal.state, PortalState::Ready) else {
                    return Err(protocol("COPY TO portal has no cursor"));
                };
                let memory = self.node.query_memory();
                let token = portal.cancellation.clone();
                let _active = self.cancellations.enter(&token);
                token.run_until_cancelled(crate::copy::write_arrow(query.cursor, *format, &memory, writer))
                    .await.ok_or_else(cancelled)?
            }
            (PreparedKind::Dml(statement), _) => {
                if self.config.read_only { return Err(Anomaly::incorrect("xtdb/dml-in-read-only-server", "DML is not allowed on the READ ONLY server")); }
                if self.session.state().is_read_only() { return Err(Anomaly::incorrect("xtdb/dml-in-read-only-tx", "DML is not allowed in a READ ONLY transaction").with_context("query", source.into())); }
                self.begin_implicit(Some(false))?;
                self.session.state_mut().execute_operation(statement.dml_operation(portal.arguments.clone())?).await?;
                if commit_dml_before_reply { self.commit(permit).await?; }
                match statement.ordered_index_command_tag() {
                    Some(tag) => writer.string(b'C', tag).await,
                    None => writer.string(b'C', &dml_tag(source)).await,
                }
            }
            (PreparedKind::Control(control), _) => {
                let statement = control.statement();
                let _sample = matches!(statement, ControlStatement::Commit(_)).then(|| rtdb_plan::ExecutionSample::new(self.node.execution_observer(), rtdb_plan::ExecutionTimer::PgwireTransaction));
                let options = self.control_options();
                let args = bind_control_arguments(portal.arguments.as_ref(), self.node.memory())?;
                let result = self.session.execute_control_arrow_with(statement, args.as_ref(), &portal.cancellation, options).await;
                if matches!(statement, ControlStatement::Commit(_) | ControlStatement::Rollback) && self.session.state().status() == TransactionStatus::Idle { self.end_transaction(); }
                match result? {
                    ControlResult::Command { tag, parameter_status, allocation: _allocation, .. } => {
                        if matches!(statement, ControlStatement::Begin(_)) { self.implicit = false; }
                        if let Some((name, value)) = parameter_status { writer.parameter(display_parameter(&name), if name == "standard_conforming_strings" { "on" } else { &value }).await?; }
                        if matches!(statement, ControlStatement::Discard(DiscardTarget::All)) { self.end_transaction(); self.statements.clear(); apply_startup(&mut self.session, &self.startup.parameters, writer).await?; }
                        if matches!(statement, ControlStatement::SetRole) { return Ok(()); }
                        writer.string(b'C', &tag).await
                    }
                    ControlResult::CopyIn { table, format, allocation: _allocation } => {
                        if self.config.read_only { return Err(Anomaly::incorrect("xtdb/dml-in-read-only-server", "COPY is not allowed on the READ ONLY server")); }
                        let copy = crate::copy::CopyInput::new(table, format, self.node.memory(), self.config.maximum_copy_bytes)?;
                        let format = copy.wire_format().code() as u8;
                        self.copy = Some(copy);
                        writer.send(b'G', &[format, 0, 1, 0, format]).await?;
                        writer.flush().await
                    }
                    _ => Err(protocol("Unexpected control execution result")),
                }
            }
        }
    }

    async fn execute_sql_prepare<W: AsyncWrite + Unpin>(
        &mut self, syntax: &SqlPrepareSyntax, portal: &mut Portal, writer: &mut Writer<W>,
    ) -> Result<()> {
        let args = bind_control_arguments(portal.arguments.as_ref(), self.node.memory())?;
        let _allocation = self.session.admit_frontend_prepare_arrow(args.as_ref(), &portal.cancellation).await?;
        let memory = Arc::clone(self.node.memory());
        let prepared = self.prepare_sql_child(syntax.child(&memory)?, None).await?;
        self.install_sql_prepare(Cow::Borrowed(syntax.name()), syntax.source(), prepared, writer).await
    }

    async fn prepare_sql_child(&mut self, child: SqlPrepareChild<'_>, settings: Option<PreparationSettings<'_>>) -> Result<Arc<Prepared>> {
        match child {
            SqlPrepareChild::Prepare(child) => self.retain_sql_prepare(child, Vec::new()),
            SqlPrepareChild::Command { syntax, allocation } => self.prepare_batch_command(syntax, allocation, settings).await,
        }
    }

    async fn stream<W: AsyncWrite + Unpin>(&mut self, portal: &mut Portal, source: &str, limit: usize, writer: &mut Writer<W>) -> Result<()> {
        let columns = portal.columns.as_deref().ok_or_else(|| protocol("Query portal has no column descriptors"))?;
        let PortalState::Query(query) = &mut portal.state else { return Err(protocol("Query portal has no cursor")); };
        let environment = self.environment()?; let mut emitted = 0;
        let memory = self.node.query_memory();
        while limit == 0 || emitted < limit {
            if query.batch.as_ref().is_none_or(|batch| query.row == batch.batch().num_rows()) {
                loop { query.batch = query.cursor.next_batch().await?; query.row = 0; if query.batch.as_ref().is_none_or(|batch| batch.batch().num_rows() != 0) { break; } }
                query.types = query.batch.as_ref().map(|batch| batch.batch().schema().fields().iter().map(|field| rtdb_columnar::from_field(field)).collect::<Result<Vec<_>>>()).transpose()?.unwrap_or_default();
            }
            let Some(batch) = &query.batch else { break; };
            let schema = batch.batch().schema();
            let row = query.row;
            let delivery = query.cursor.delivery_grant();
            let consumption: Result<()> = async {
                let _scratch = memory.reserve(4096)?;
                let mut body = DataRow::new(&memory)?;
                let count = u16::try_from(columns.len()).map_err(|_| protocol("Too many columns"))?;
                body.reserve(2)?;
                body.bytes.extend_from_slice(&count.to_be_bytes());
                for (index, column) in columns.iter().enumerate() {
                    let array = batch.batch().column(index).as_ref();
                    if direct_cell(column.ty, column.format, schema.field(index), array, row, &environment, &mut body)? { continue; }
                    let cell = rtdb_columnar::ArrowCell { field: schema.field(index), array, row };
                    match rtdb_pg_codec::encode_arrow(column.ty, column.format, &query.types[index], cell, &environment, &memory)? {
                        None => body.null_cell()?,
                        Some(encoded) => body.cell(encoded.as_slice())?,
                    }
                }
                let grant = delivery.await?;
                grant.deliver(async {
                    writer.row_body(&body.bytes).await?;
                    if grant.is_bounded() { writer.flush().await?; }
                    Ok(())
                }).await.map_err(crate::wire::delivery_error)
            }.await;
            if let Err(error) = consumption { query.cursor.record_consumer_error(); return Err(error); }
            query.row += 1; query.sent = query.sent.checked_add(1).ok_or_else(|| protocol("Result row count overflow"))?; emitted += 1;
        }
        let grant = query.cursor.delivery_grant().await?;
        grant.deliver(async {
            if limit != 0 && emitted == limit {
                writer.empty(b's').await?;
                writer.flush().await
            } else {
                writer.string(b'C', &format!("{} {}", head(source), query.sent)).await?;
                if grant.is_bounded() { writer.flush().await?; }
                Ok(())
            }
        }).await.map_err(crate::wire::delivery_error)?;
        grant.observe_answer();
        Ok(())
    }

    async fn simple<W: AsyncWrite + Unpin>(&mut self, source: &str, writer: &mut Writer<W>) -> Result<()> {
        self.extended = false; self.portals.remove("");
        let permit = self.replay_permit.take();
        let result = self.simple_statements(source, permit, writer).await;
        if let Err(error) = result {
            if error.anomaly().code == "xtdb.pgwire/connection-error" { return Err(error.anomaly().clone()); }
            writer.error(error.anomaly(), self.environment()?.fallback(), false).await?;
        }
        if self.copy.is_none() { self.report_ready(writer).await?; }
        Ok(())
    }
    async fn simple_statements<W: AsyncWrite + Unpin>(
        &mut self, original: &str, mut permit: Option<GatewayReplayPermit>, writer: &mut Writer<W>,
    ) -> std::result::Result<(), SimpleQueryFailure> {
        let normalized = normalized_simple_text(original);
        let keyword = keyword_prefix(normalized);
        let marker = simple_query_marker(normalized)
            .or((keyword == KeywordPrefix::MatchesEveryLocale).then_some(SimpleQueryMarker::Keywords));
        if let Some(marker) = marker {
            self.begin_implicit(None)?;
            let result: Result<()> = async {
                match marker {
                    SimpleQueryMarker::Empty => writer.empty(b'I').await,
                    SimpleQueryMarker::Semicolon => {
                        writer.empty(b'n').await?;
                        writer.string(b'C', " 0").await
                    }
                    SimpleQueryMarker::Keywords => {
                        writer.empty(b'n').await?;
                        writer.send(b'D', KEYWORDS_DATA_ROW).await?;
                        writer.string(b'C', "SELECT 1").await
                    }
                }
            }.await;
            self.simple_statement_result(result, writer).await?;
            return self.settle_simple_transaction().await.map_err(Into::into);
        }

        let memory = Arc::clone(self.node.memory());
        let rewritten = replace_namespace_query(original, normalized);
        let (source, _normalized_allocation) = if !std::ptr::eq(rewritten, original) {
            (Cow::Borrowed(rewritten), None)
        } else if normalized.1.is_empty() {
            (Cow::Borrowed(normalized.0), None)
        } else {
            let allocation = memory.reserve(normalized.0.len() + normalized.1.len())?;
            (Cow::Owned([normalized.0, normalized.1].concat()), Some(allocation))
        };
        let parsed = if keyword == KeywordPrefix::LocaleDependent {
            rtdb_sql::parse_statement_batch_with_frontend_response(&source, &memory, legacy_keyword_probe)
        } else {
            rtdb_sql::parse_statement_batch(&source, &memory)
        };
        let mut batch = parsed.map_err(|error| SimpleQueryFailure::from_batch(error, &source, original, &memory))?;
        let single_autocommit = batch.len() == 1 && self.session.state().status() == TransactionStatus::Idle;
        if !single_autocommit && permit.is_some_and(|permit| permit.mode == GatewayReplayMode::Replay) {
            return Err(replay_requires_autocommit().into());
        }
        while let Some(statement) = batch.next_for_execution() {
            let start_transaction = match &statement {
                BatchExecution::Prepare(_) => true,
                BatchExecution::Command { syntax, .. } => match syntax {
                    ExecutableSyntax::Sql(_) | ExecutableSyntax::FrontendResponse(_) => true,
                    ExecutableSyntax::Control(control) => control.starts_implicit_transaction(),
                },
            };
            if start_transaction { self.begin_implicit(None)?; }
            let permit = if single_autocommit { permit.take() } else { None };
            let result = self.run_simple_statement(statement, single_autocommit, permit, writer).await;
            self.simple_statement_result(result, writer).await?;
        }
        drop(batch);
        self.settle_simple_transaction().await.map_err(Into::into)
    }

    async fn run_simple_statement<W: AsyncWrite + Unpin>(&mut self, statement: BatchExecution<'_, '_>, commit_dml_before_reply: bool, permit: Option<GatewayReplayPermit>, writer: &mut Writer<W>) -> Result<()> {
        let replay = permit.is_some_and(|permit| permit.mode == GatewayReplayMode::Replay);
        let (syntax, allocation) = match statement {
            BatchExecution::Command { syntax, allocation } => (syntax, allocation),
            BatchExecution::Prepare(statement) => {
                if replay { return Err(replay_requires_autocommit()); }
                if self.session.state().status() == TransactionStatus::Failed {
                    return Err(protocol("current transaction is aborted, commands ignored until ROLLBACK is received"));
                }
                let token = self.token();
                let _allocation = self.session.admit_frontend_prepare(&[], &token).await?;
                let source = statement.source();
                let (name, child) = statement.into_child()?;
                let prepared = self.prepare_sql_child(child, None).await?;
                return self.install_sql_prepare(name.into(), source, prepared, writer).await;
            }
        };
        let prepared = self.prepare_batch_command(syntax, allocation, None).await?;
        let permit = match prepared.ordinary_kind() {
            Some(PreparedKind::Dml(_)) => permit.map(|permit| ReplaySubmissionPermit {
                key: format!("{}/0", permit.nonce.simple()), not_after: permit.not_after,
            }),
            Some(PreparedKind::Query(_) | PreparedKind::Empty | PreparedKind::Keywords) => None,
            _ if replay => return Err(replay_requires_autocommit()),
            _ => None,
        };
        Self::planning_warnings(&prepared, writer).await?;
        if !prepared.oids.is_empty() { return Err(protocol("Parameters not allowed in simple queries")); }
        let mut portal = self.bind(prepared, None, Vec::new(), &[]).await?;
        if portal.columns.is_some() { Self::description(portal.columns.as_deref(), self.environment()?.fallback(), writer).await?; }
        self.execute(&mut portal, 0, commit_dml_before_reply, permit, writer).await
    }

    async fn simple_statement_result<W: AsyncWrite + Unpin>(&mut self, result: Result<()>, writer: &mut Writer<W>) -> Result<()> {
        if let Err(error) = result {
            if self.implicit {
                self.rollback()?;
            } else {
                self.session.state_mut().fail_transaction();
            }
            writer.error(&error, self.environment()?.fallback(), false).await?;
        }
        Ok(())
    }

    async fn settle_simple_transaction(&mut self) -> Result<()> {
        if self.implicit {
            if self.session.state().status() == TransactionStatus::Failed {
                self.rollback()?;
            } else {
                self.commit(None).await?;
            }
        }
        Ok(())
    }
}

fn legacy_keyword_probe(source: &str) -> bool {
    source.as_bytes().get(..KEYWORDS_PREFIX.len()).is_some_and(|prefix| prefix.eq_ignore_ascii_case(KEYWORDS_PREFIX.as_bytes()))
}

fn keyword_shape() -> PreparedShape {
    PreparedShape {
        kind: PreparedKind::Keywords,
        columns: Some(vec![Column { name: "col1".into(), ty: PgType::VarChar, format: Format::Text }]),
        parameter_count: 0,
    }
}

fn control_shape(control: PreparedControl, declared_parameters: usize) -> Result<PreparedShape> {
    let statement = control.statement();
    let parameter_count = match statement {
        ControlStatement::Execute { .. } => statement.parameter_count(),
        ControlStatement::Show { .. } => 0,
        _ => declared_parameters,
    };
    let columns = control_schema(statement)?.map(|schema| schema.fields().iter().map(|field| {
        Ok(Column { name: field.name.clone(), ty: PgType::from_logical(&field.data_type)?, format: Format::Text })
    }).collect::<Result<_>>()).transpose()?;
    Ok(PreparedShape { kind: PreparedKind::Control(control), columns, parameter_count })
}

fn sql_shape(prepared: PreparedSql, declared_parameters: usize) -> PreparedShape {
    if prepared.is_dml() {
        PreparedShape { kind: PreparedKind::Dml(prepared), columns: None, parameter_count: declared_parameters }
    } else {
        let columns = prepared.prepared_schema().zip(prepared.postgres_types()).map(|(schema, types)| {
            schema.fields().iter().zip(types).map(|(field, ty)| Column { name: field.name.clone(), ty: *ty, format: Format::Text }).collect()
        });
        let parameter_count = prepared.parameter_count();
        PreparedShape { kind: PreparedKind::Query(prepared), columns, parameter_count }
    }
}

/// Extended Parse reaches the reference's multi-statement grammar, as Simple Query does.
fn extended_syntax_error(source: &str, original: &str, error: Anomaly, memory: &Arc<MemoryBudget>) -> Anomaly {
    match rtdb_sql::batch_syntax_diagnostic(source, &error, memory).and_then(|report| report.map(|report| report.with_sql_context(original)).transpose()) {
        Ok(Some(report)) => report.anomaly().clone(),
        Ok(None) => error,
        Err(admission) => admission,
    }
}

enum SimpleQueryFailure {
    Ordinary(Anomaly),
    Statement(ContextualStatementDiagnostic),
}

impl From<Anomaly> for SimpleQueryFailure {
    fn from(error: Anomaly) -> Self { Self::Ordinary(error) }
}

impl SimpleQueryFailure {
    fn from_batch(legacy: Anomaly, source: &str, original: &str, memory: &Arc<MemoryBudget>) -> Self {
        let diagnostic = rtdb_sql::batch_syntax_diagnostic(source, &legacy, memory)
            .and_then(|report| report.map(|report| report.with_sql_context(original)).transpose());
        match diagnostic {
            Ok(Some(report)) => Self::Statement(report),
            Ok(None) => Self::Ordinary(legacy),
            Err(error) => Self::Ordinary(error),
        }
    }

    fn anomaly(&self) -> &Anomaly {
        match self {
            Self::Ordinary(error) => error,
            Self::Statement(report) => report.anomaly(),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum SimpleQueryMarker {
    Empty,
    Semicolon,
    Keywords,
}

const KEYWORDS_PREFIX: &str = "select string_agg(word, ',') from pg_catalog.pg_get_keywords()";
const KEYWORDS_DATA_ROW: &[u8] = b"\0\x01\0\0\0\x04xtdb";

#[derive(Debug, PartialEq, Eq)]
enum KeywordPrefix {
    MatchesEveryLocale,
    LocaleDependent,
    DoesNotMatch,
}

fn keyword_prefix((body, suffix): (&str, &str)) -> KeywordPrefix {
    let mut characters = body.chars().chain(suffix.chars()).peekable();
    let mut disposition = KeywordPrefix::MatchesEveryLocale;
    for expected in KEYWORDS_PREFIX.chars() {
        let Some(character) = characters.next() else { return KeywordPrefix::DoesNotMatch; };
        match (expected, character) {
            ('i', 'I') => {
                let _ = characters.next_if_eq(&'\u{307}');
                disposition = KeywordPrefix::LocaleDependent;
            }
            ('i', '\u{130}') => disposition = KeywordPrefix::LocaleDependent,
            ('k', '\u{212a}') => (),
            _ if character.eq_ignore_ascii_case(&expected) => (),
            _ => return KeywordPrefix::DoesNotMatch,
        }
    }
    disposition
}

fn simple_query_marker((body, suffix): (&str, &str)) -> Option<SimpleQueryMarker> {
    if body.starts_with(';') {
        return Some(SimpleQueryMarker::Semicolon);
    }
    let mut chars = body.chars().chain(suffix.chars());
    while let Some(character) = chars.next() {
        if java_whitespace(character) {
            continue;
        }
        if character != '-' || chars.next() != Some('-') {
            return None;
        }
        let _ = chars.by_ref().find(|&character| matches!(character, '\r' | '\n'));
    }
    Some(SimpleQueryMarker::Empty)
}

fn normalized_simple_text(source: &str) -> (&str, &str) {
    let source = source.trim_start_matches(java_whitespace);
    if let Some(body) = source.trim_end_matches(java_regex_whitespace).strip_suffix(';') {
        return (body, "");
    }
    if let Some(last @ ('\u{85}' | '\u{2028}' | '\u{2029}')) = source.chars().next_back() {
        let (head, suffix) = source.split_at(source.len() - last.len_utf8());
        if let Some(body) = head.trim_end_matches(java_regex_whitespace).strip_suffix(';') {
            return (body, suffix);
        }
    }
    (source, "")
}

fn java_whitespace(character: char) -> bool {
    matches!(character, '\u{9}'..='\u{d}' | '\u{1c}'..=' '
        | '\u{1680}' | '\u{2000}'..='\u{2006}' | '\u{2008}'..='\u{200a}'
        | '\u{2028}' | '\u{2029}' | '\u{205f}' | '\u{3000}')
}

fn java_regex_whitespace(character: char) -> bool {
    matches!(character, '\u{9}'..='\u{d}' | ' ')
}

fn invalid_argument(error: Anomaly, index: usize, format: Format) -> Anomaly {
    Anomaly::new_nullable(rtdb_types::AnomalyCategory::Incorrect, "xtdb.pgwire/invalid-arg-representation", error.message)
        .with_context("arg-idx", index.into()).with_context("arg-format", if format == Format::Binary { "binary" } else { "text" }.into())
}
fn item_format(formats: &[Format], count: usize, index: usize) -> Result<Format> { match formats.len() { 0 => Ok(Format::Text), 1 => Ok(formats[0]), length if length == count => Ok(formats[index]), _ => Err(protocol("Invalid format code count")) } }
fn head(source: &str) -> String { source.split_whitespace().next().unwrap_or_default().to_ascii_uppercase() }
fn dml_tag(source: &str) -> String { match head(source).as_str() { "INSERT" => "INSERT 0 0".into(), "UPDATE" => "UPDATE 0".into(), "DELETE" => "DELETE 0".into(), "PATCH" => "PATCH 0".into(), "ERASE" => "ERASE 0".into(), "CREATE" => "CREATE TABLE".into(), value => value.into() } }
fn is_type_probe(source: &str) -> bool { let mut prior_space = false; source.chars().filter(|character| { let repeated = *character == ' ' && prior_space; prior_space = *character == ' '; !repeated }).collect::<String>() == "SELECT pg_type.oid, typname FROM pg_catalog.pg_type LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r from pg_namespace as ns join ( select s.r, (current_schemas(false))[s.r] as nspname from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r using ( nspname ) ) as sp ON sp.nspoid = typnamespace WHERE typname = $1 ORDER BY sp.r, pg_type.oid DESC LIMIT 1" }
const DBEAVER_NAMESPACE_QUERY: &str = "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass ORDER BY nspname";
const DBEAVER_NAMESPACE_REPLACEMENT: &str = "SELECT n.oid,n.nspname,n.nspowner,n.nspacl,d.description FROM pg_catalog.pg_namespace n LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass ORDER BY nspname";

fn replace_namespace_query<'source>(source: &'source str, (body, suffix): (&str, &str)) -> &'source str {
    if body == DBEAVER_NAMESPACE_QUERY && suffix.is_empty() {
        DBEAVER_NAMESPACE_REPLACEMENT
    } else {
        source
    }
}

fn admit_rewrite_syntax(original_bytes: usize, rewritten_bytes: usize, memory: &Arc<MemoryBudget>) -> Result<Option<Arc<MemoryReservation>>> {
    if rewritten_bytes <= original_bytes { return Ok(None); }
    let too_large = || protocol("Prepared statement is too large");
    let original = rtdb_sql::statement_syntax_estimate(original_bytes).ok_or_else(too_large)?;
    let rewritten = rtdb_sql::statement_syntax_estimate(rewritten_bytes).ok_or_else(too_large)?;
    Ok(Some(memory.reserve(rewritten - original)?))
}

/// Unsupported layouts leave the row intact for the value codec.
fn direct_cell(ty: PgType, format: Format, field: &arrow_schema::Field, array: &dyn arrow_array::Array, row: usize, environment: &SessionEnvironment, out: &mut DataRow) -> Result<bool> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
        TimestampSecondType, TimestampMillisecondType, TimestampMicrosecondType, TimestampNanosecondType};
    use arrow_schema::{DataType, TimeUnit as ArrowUnit, UnionMode};
    use rtdb_types::TimeUnit;

    let data_type = array.data_type();
    let is_uuid = data_type == &DataType::FixedSizeBinary(16) && field.extension_type_name() == Some("uuid");
    if !field.metadata().is_empty() && !is_uuid { return Ok(false); }
    if let DataType::Union(fields, UnionMode::Dense) = data_type {
        let union = array.as_any().downcast_ref::<arrow_array::UnionArray>().ok_or_else(|| protocol("Invalid result union"))?;
        let tag = union.type_id(row);
        let child = fields.iter().find_map(|(id, field)| (id == tag).then_some(field)).ok_or_else(|| protocol("Result union has no active field"))?;
        return direct_cell(ty, format, child, union.child(tag).as_ref(), union.value_offset(row), environment, out);
    }
    let binary = format == Format::Binary;
    let fits = match (data_type, ty) {
        (DataType::Null, _) => field.is_nullable(),
        (DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64, PgType::Int2 | PgType::Int4 | PgType::Int8) => true,
        (DataType::Float32 | DataType::Float64, PgType::Float4 | PgType::Float8)
        | (DataType::Boolean, PgType::Bool) | (DataType::Utf8, PgType::Text | PgType::VarChar | PgType::Keyword)
        | (DataType::Timestamp(..), PgType::Timestamp | PgType::TimestampTz)
        | (DataType::Binary, PgType::Bytes) => true,
        (DataType::FixedSizeBinary(_), PgType::Bytes) => !is_uuid,
        (DataType::FixedSizeBinary(16), PgType::Uuid) => is_uuid,
        _ => false,
    };
    if !fits { return Ok(false); }
    if data_type == &DataType::Null || array.is_null(row) {
        out.null_cell()?;
        return Ok(true);
    }
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let value = match data_type {
                DataType::Int8 => i64::from(array.as_primitive::<Int8Type>().value(row)),
                DataType::Int16 => i64::from(array.as_primitive::<Int16Type>().value(row)),
                DataType::Int32 => i64::from(array.as_primitive::<Int32Type>().value(row)),
                _ => array.as_primitive::<Int64Type>().value(row),
            };
            if binary {
                match ty {
                    PgType::Int2 => out.cell(&(value as i16).to_be_bytes())?,
                    PgType::Int4 => out.cell(&(value as i32).to_be_bytes())?,
                    _ => out.cell(&value.to_be_bytes())?,
                }
            } else {
                out.cell(value.to_string().as_bytes())?;
            }
        }
        DataType::Float32 | DataType::Float64 => {
            let value = if data_type == &DataType::Float32 {
                f64::from(array.as_primitive::<Float32Type>().value(row))
            } else {
                array.as_primitive::<Float64Type>().value(row)
            };
            match (ty, binary) {
                (PgType::Float4, true) => out.cell(&(value as f32).to_be_bytes())?,
                (PgType::Float8, true) => out.cell(&value.to_be_bytes())?,
                (PgType::Float4, false) => out.cell(rtdb_types::java_f32_text(value as f32).as_bytes())?,
                _ => out.cell(rtdb_types::java_f64_text(value).as_bytes())?,
            }
        }
        DataType::Boolean => {
            let value = array.as_boolean().value(row);
            if binary { out.cell(&[u8::from(value)])?; }
            else { out.cell(if value { b"t" } else { b"f" })?; }
        }
        DataType::Utf8 => out.cell(array.as_string::<i32>().value(row).as_bytes())?,
        DataType::Binary | DataType::FixedSizeBinary(_) if ty == PgType::Bytes => {
            let bytes = if data_type == &DataType::Binary {
                array.as_binary::<i32>().value(row)
            } else {
                array.as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                    .ok_or_else(|| protocol("Invalid result binary array"))?.value(row)
            };
            if binary {
                out.cell(bytes)?;
            } else {
                let len = bytes.len().checked_mul(2).and_then(|len| len.checked_add(2)).ok_or_else(|| protocol("Value too large"))?;
                let length = i32::try_from(len).map_err(|_| protocol("Value too large"))?;
                out.reserve(len.checked_add(4).ok_or_else(|| protocol("Value too large"))?)?;
                out.bytes.extend_from_slice(&length.to_be_bytes());
                out.bytes.extend_from_slice(b"\\x");
                for &byte in bytes {
                    let hex = b"0123456789abcdef";
                    out.bytes.extend_from_slice(&[hex[(byte >> 4) as usize], hex[(byte & 0xf) as usize]]);
                }
            }
        }
        DataType::FixedSizeBinary(16) => {
            let bytes = array.as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>()
                .ok_or_else(|| protocol("Invalid result UUID array"))?.value(row);
            if binary { out.cell(bytes)?; }
            else {
                let uuid = uuid::Uuid::from_slice(bytes).map_err(|_| protocol("Invalid result UUID"))?;
                out.cell(uuid.hyphenated().encode_lower(&mut uuid::Uuid::encode_buffer()).as_bytes())?;
            }
        }
        DataType::Timestamp(unit, timezone) => {
            let (value, unit) = match unit {
                ArrowUnit::Second => (array.as_primitive::<TimestampSecondType>().value(row), TimeUnit::Second),
                ArrowUnit::Millisecond => (array.as_primitive::<TimestampMillisecondType>().value(row), TimeUnit::Millisecond),
                ArrowUnit::Microsecond => (array.as_primitive::<TimestampMicrosecondType>().value(row), TimeUnit::Microsecond),
                ArrowUnit::Nanosecond => (array.as_primitive::<TimestampNanosecondType>().value(row), TimeUnit::Nanosecond),
            };
            let bytes = rtdb_pg_codec::encode_timestamp(format, value, unit, timezone.as_deref(), ty == PgType::TimestampTz, environment)?;
            out.cell(&bytes)?;
        }
        _ => unreachable!("The result layout was checked"),
    }
    Ok(true)
}

struct DataRow {
    bytes: Vec<u8>,
    reservation: Arc<MemoryReservation>,
}

impl DataRow {
    fn new(memory: &Arc<MemoryBudget>) -> Result<Self> {
        Ok(Self { bytes: Vec::new(), reservation: memory.reserve(256)? })
    }

    fn reserve(&mut self, additional: usize) -> Result<()> {
        let overflow = || protocol("Result row allocation overflow");
        let needed = self.bytes.len().checked_add(additional).ok_or_else(overflow)?;
        let current = self.bytes.capacity();
        if current >= needed { return Ok(()); }
        let maximum = current.checked_mul(2).ok_or_else(overflow)?.max(needed).max(32);
        let lease = Arc::get_mut(&mut self.reservation).expect("A DataRow owns its allocation");
        lease.grow_to(maximum.checked_add(current).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?)?;
        self.bytes.try_reserve_exact(maximum - self.bytes.len()).map_err(|_| overflow())?;
        lease.shrink_to(self.bytes.capacity().checked_add(256).ok_or_else(overflow)?)
    }

    fn null_cell(&mut self) -> Result<()> {
        self.reserve(4)?;
        self.bytes.extend_from_slice(&(-1i32).to_be_bytes());
        Ok(())
    }

    fn cell(&mut self, bytes: &[u8]) -> Result<()> {
        let length = i32::try_from(bytes.len()).map_err(|_| protocol("Value too large"))?;
        self.reserve(bytes.len().checked_add(4).ok_or_else(|| protocol("Value too large"))?)?;
        self.bytes.extend_from_slice(&length.to_be_bytes());
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rtdb_sql::BatchStatement;
    use super::*;

    #[test]
    fn postgres_arrow_cells_match_the_value_codec_in_both_formats() {
        use rtdb_types::{LogicalType, ScalarType, TimeUnit};

        let environment = SessionEnvironment::default();
        let memory = MemoryBudget::new(1 << 20);
        let instant = Value::Timestamp {
            value: -1001, unit: TimeUnit::Nanosecond, timezone: Some("UTC".into()),
        };
        let cases = [
            (PgType::Bytes, Value::Binary(vec![0, 9, 127, 255])),
            (PgType::Bytes, Value::FixedSizeBinary(vec![1, 2, 3])),
            (PgType::Uuid, Value::Uuid([3; 16])),
            (PgType::Float4, Value::F32(-0.0)),
            (PgType::Float8, Value::F32(1.0 / 3.0)),
            (PgType::Timestamp, instant.clone()),
            (PgType::TimestampTz, instant),
        ];
        for (ty, value) in cases {
            let logical = value.logical_type().maybe();
            let field = rtdb_columnar::to_field("result", &logical).unwrap();
            let array = rtdb_columnar::copy_array(&logical, &[Value::Null, value.clone()]).unwrap();
            for format in [Format::Text, Format::Binary] {
                let mut actual = DataRow::new(&memory).unwrap();
                assert!(direct_cell(ty, format, &field, array.as_ref(), 0, &environment, &mut actual).unwrap());
                assert_eq!(actual.bytes, (-1i32).to_be_bytes());
                actual.bytes.clear();
                assert!(direct_cell(ty, format, &field, array.as_ref(), 1, &environment, &mut actual).unwrap());
                let payload = rtdb_pg_codec::encode_value(ty, format, &value, &environment).unwrap();
                assert_eq!(&actual.bytes[..4], &(payload.len() as i32).to_be_bytes());
                assert_eq!(&actual.bytes[4..], payload.as_slice());
            }
        }
        let logical = LogicalType::Scalar(ScalarType::I64).join(&LogicalType::Scalar(ScalarType::I32));
        let field = rtdb_columnar::to_field("result", &logical).unwrap();
        let array = rtdb_columnar::copy_array(&logical, &[Value::I64(7), Value::I32(-5)]).unwrap();
        let mut output = DataRow::new(&memory).unwrap();
        for row in 0..array.len() {
            assert!(direct_cell(PgType::Int8, Format::Text, &field, array.as_ref(), row, &environment, &mut output).unwrap());
        }
        assert_eq!(&output.bytes, b"\0\0\0\x017\0\0\0\x02-5");
    }

    #[test]
    fn postgres_text_bytea_charges_its_wire_allocation_until_drop() {
        let memory = MemoryBudget::new(1 << 20);
        let input = vec![0xc0; 20 * 1024];
        let values = arrow_array::BinaryArray::from(vec![input.as_slice()]);
        let field = arrow_schema::Field::new("b", arrow_schema::DataType::Binary, false);
        let mut output = DataRow::new(&memory).unwrap();
        assert!(direct_cell(PgType::Bytes, Format::Text, &field, &values, 0, &SessionEnvironment::default(), &mut output).unwrap());
        assert_eq!(output.bytes.len(), 6 + input.len() * 2);
        assert!(memory.used_bytes() >= output.bytes.capacity());
        drop(output);
        assert_eq!(memory.used_bytes(), 0);
    }

    fn simple_query_marker(source: &str) -> Option<SimpleQueryMarker> {
        super::simple_query_marker(normalized_simple_text(source))
    }

    #[test]
    fn whole_simple_markers_use_java_blankness_comments_and_regex_end_anchors() {
        for source in [
            "", ";", ";\r\n", ";\u{b}\u{2028}", ";\u{2029}", "-- end;\u{85}",
            "\u{2003}-- first\r -- second\r\n\t-- last", "\u{1c}\u{3000}",
        ] {
            assert_eq!(simple_query_marker(source), Some(SimpleQueryMarker::Empty), "{source:?}");
        }
        for source in [
            ";;", ";\u{2028}\n", "\u{1c}\t\u{2003};; ", ";\u{a0}", ";-- comment", ";;\u{85}",
        ] {
            assert_eq!(simple_query_marker(source), Some(SimpleQueryMarker::Semicolon), "{source:?}");
        }
        for source in [
            ";\u{85}", "\u{85};@", "\u{a0};@", "/* only a block comment */", "-- a\n/* block */", "SELECT 1;",
        ] {
            assert_eq!(simple_query_marker(source), None, "{source:?}");
        }
    }

    #[test]
    fn argument_reclassification_preserves_nullable_messages_and_parameter_location() {
        for message in [None, Some(String::new()), Some("invalid input".into())] {
            for format in [Format::Text, Format::Binary] {
                let error = invalid_argument(Anomaly::new_nullable(rtdb_types::AnomalyCategory::Fault, "test/decode", message.clone()), 2, format);
                assert_eq!(error.message, message);
                assert_eq!(error.category, rtdb_types::AnomalyCategory::Incorrect);
                assert_eq!(error.code, "xtdb.pgwire/invalid-arg-representation");
                assert_eq!(error.context["arg-idx"], 2);
                assert_eq!(error.context["arg-format"], if format == Format::Binary { "binary" } else { "text" });
            }
        }
    }

    #[test]
    fn namespace_rewrite_matches_only_the_complete_normalized_simple_query() {
        let query = DBEAVER_NAMESPACE_QUERY;
        let normalized = format!("\u{1c}\u{2003}{query};\t\r\n");
        for source in [query, normalized.as_str()] {
            let rewritten = replace_namespace_query(source, normalized_simple_text(source));
            assert_eq!(rewritten, DBEAVER_NAMESPACE_REPLACEMENT);
        }
        for source in [
            query.to_ascii_lowercase(),
            format!("SELECT 1; {query}"),
            format!("{query} "),
            format!("{query};\u{2028}"),
            format!("\u{a0}{query};"),
        ] {
            let rewritten = replace_namespace_query(&source, normalized_simple_text(&source));
            assert!(std::ptr::eq(rewritten, source.as_str()));
        }
    }

    #[test]
    fn extended_namespace_rewrite_admits_only_syntax_growth_over_the_full_input() {
        let original = DBEAVER_NAMESPACE_QUERY.len();
        let rewritten = DBEAVER_NAMESPACE_REPLACEMENT.len();
        let initial = rtdb_sql::statement_syntax_estimate(original).unwrap();
        let growth = 3328;
        let short = MemoryBudget::new(initial + growth - 1);
        let classification = short.reserve(initial).unwrap();
        let error = admit_rewrite_syntax(original, rewritten, &short).unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert_eq!(short.peak_bytes(), initial);
        assert_eq!(short.used_bytes(), initial);
        drop(classification);
        assert_eq!(short.used_bytes(), 0);

        let exact = MemoryBudget::new(initial + growth);
        let classification = exact.reserve(initial).unwrap();
        let extra = admit_rewrite_syntax(original, rewritten, &exact).unwrap().unwrap();
        assert_eq!(extra.bytes(), growth);
        assert_eq!(exact.used_bytes(), rtdb_sql::statement_syntax_estimate(rewritten).unwrap());
        for padded in [rewritten, rewritten + 1] {
            assert!(admit_rewrite_syntax(padded, rewritten, &exact).unwrap().is_none());
        }
        drop(extra);
        assert_eq!(exact.used_bytes(), initial);
        drop(classification);
        assert_eq!(exact.used_bytes(), 0);
    }

    #[test]
    fn keyword_prefix_distinguishes_openjdk_locale_independent_and_dependent_spellings() {
        for (spelling, expected) in [
            ("StriNG", KeywordPrefix::MatchesEveryLocale),
            ("STRING", KeywordPrefix::LocaleDependent),
            ("STR\u{130}NG", KeywordPrefix::LocaleDependent),
            ("STRI\u{307}NG", KeywordPrefix::LocaleDependent),
            ("STRi\u{307}NG", KeywordPrefix::DoesNotMatch),
            ("STR\u{130}\u{307}NG", KeywordPrefix::DoesNotMatch),
            ("STRI\u{323}\u{307}NG", KeywordPrefix::DoesNotMatch),
            ("STRI\u{307}\u{307}NG", KeywordPrefix::DoesNotMatch),
            ("STR\u{131}NG", KeywordPrefix::DoesNotMatch),
            ("STRINGER", KeywordPrefix::DoesNotMatch),
        ] {
            let source = format!("SELECT {spelling}_AGG(word, ',') FROM pg_catalog.pg_get_\u{212a}EYWORDS();\u{2028}");
            assert_eq!(keyword_prefix(normalized_simple_text(&source)), expected, "{source:?}");
        }
        let lower = KEYWORDS_PREFIX;
        assert_eq!(keyword_prefix(normalized_simple_text(&format!("\u{1c}{lower}unexpected @"))), KeywordPrefix::MatchesEveryLocale);
        for source in [
            format!("SELECT 42; {lower}"),
            lower.replacen('s', "\u{17f}", 1),
            lower.trim_end_matches(')').to_owned(),
        ] {
            assert_eq!(keyword_prefix(normalized_simple_text(&source)), KeywordPrefix::DoesNotMatch, "{source:?}");
        }
    }

    async fn test_frontend(replication_enabled: bool) -> (ProtocolSession, rtdb_node::NodeDriver) {
        use rtdb_columnar::MemoryBudget;

        struct Services;
        #[async_trait::async_trait]
        impl rtdb_auth::HttpTransport for Services {
            async fn request(&self, _: rtdb_auth::HttpRequest) -> Result<rtdb_auth::HttpResponse> {
                panic!("static authentication does not use HTTP");
            }
        }
        impl rtdb_maintenance::CollectionObserver for Services {
            fn table_failed(&self, _: Option<&rtdb_types::TableRef>, error: &Anomaly) { panic!("{error}"); }
        }
        impl rtdb_plan::CatalogMetrics for Services {
            fn metrics(&self) -> Result<rtdb_plan::CatalogRows<rtdb_plan::CatalogMetric>> {
                Ok(rtdb_plan::CatalogRows::new(Vec::new(), Arc::new(())))
            }
        }

        let fixtures = Arc::new(Services);
        let auth = rtdb_auth::Authenticator::open(
            rtdb_auth::AuthConfig::SingleRootUser { password: None },
            MemoryBudget::new(1 << 20), Arc::new(rtdb_node::SystemClock), fixtures.clone(),
        ).await.unwrap();
        let identity = auth.authenticate_trusted("xtdb").unwrap();
        let services = rtdb_node::NodeServices::local(auth.clone(), fixtures.clone(), auth, fixtures);
        let mut options = rtdb_node::NodeOptions { automatic_compaction: false, automatic_gc: false, ..Default::default() };
        options.replication.enabled = replication_enabled;
        let (node, driver) = Node::open(options, services).await.unwrap();
        let session = node.session("xtdb", identity).unwrap();
        let startup = Startup { parameters: BTreeMap::new(), gateway_session_state: false, gateway_settings: false, replica_read: false, _reservation: node.memory().reserve(0).unwrap() };
        let frontend = ProtocolSession::new(session, node, Arc::new(ServerConfig::default()), startup, Arc::new(CancellationSlot::default()));
        (frontend, driver)
    }

    #[tokio::test]
    async fn a_gateway_key_is_installed_without_a_reply_and_rejection_ends_the_session() {
        let (mut frontend, driver) = test_frontend(false).await;
        let memory = Arc::clone(frontend.node.memory());
        let key = "00000000000000000000000000000001";
        let frame = |key: &str| Frame {
            tag: GATEWAY_SESSION_STATE_TAG,
            body: [&[GATEWAY_KEY_KIND], key.as_bytes()].concat(),
            charge: memory.reserve(0).unwrap(),
        };
        let mut writer = Writer::new(Vec::new(), memory.clone());
        assert!(frontend.handle(frame(key), &mut writer).await.is_err());

        frontend.startup.gateway_session_state = true;
        frontend.handle(frame(key), &mut writer).await.unwrap();
        assert!(frontend.handle(frame("invalid"), &mut writer).await.is_err());
        assert!(writer.into_inner().is_empty());
        drop(frontend);
        driver.close_without_run().await.unwrap();
    }

    #[tokio::test]
    async fn retried_writes_stay_keyed_through_session_reset_and_malformed_metadata() {
        let (mut frontend, driver) = test_frontend(true).await;
        frontend.startup.gateway_session_state = true;
        let node = frontend.node.clone();
        let driver = tokio::spawn(driver.run());
        let memory = Arc::clone(node.memory());
        let mut writer = Writer::new(Vec::new(), Arc::clone(&memory));
        frontend.simple("INSERT INTO counters (_id, n) VALUES (1, 0), (2, 0), (3, 0)", &mut writer).await.unwrap();

        for (keys, command) in [
            ([1, 1], "DISCARD ALL; UPDATE counters SET n = n + 1 WHERE _id = 1"),
            ([2, 2], "BEGIN READ WRITE WITH (METADATA = {idempotency_key: 123}); UPDATE counters SET n = n + 1 WHERE _id = 2; COMMIT"),
            ([3, 4], "BEGIN READ WRITE WITH (METADATA = {idempotency_key: 'application-key'}); UPDATE counters SET n = n + 1 WHERE _id = 3; COMMIT"),
        ] {
            for id in keys {
                let key = format!("{id:032x}");
                let prelude = Frame { tag: GATEWAY_SESSION_STATE_TAG, body: [&[GATEWAY_KEY_CONFIRM_KIND], key.as_bytes()].concat(), charge: memory.reserve(0).unwrap() };
                frontend.handle(prelude, &mut writer).await.unwrap();
                frontend.simple(command, &mut writer).await.unwrap();
            }
        }
        frontend.simple("SELECT n FROM counters ORDER BY _id", &mut writer).await.unwrap();
        let bytes = writer.into_inner();
        let mut reader = crate::wire::Reader::new(bytes.as_slice(), memory);
        let mut counters = Vec::new();
        while let Some(frame) = reader.read(1 << 20).await.unwrap() {
            assert_ne!(frame.tag, b'E', "{}", String::from_utf8_lossy(&frame.body));
            if frame.tag == b'D' { counters.push(frame.body[6..].to_vec()); }
        }
        assert_eq!(counters, [b"1".to_vec(), b"1".to_vec(), b"1".to_vec()]);
        drop(frontend);
        node.shutdown();
        driver.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn replay_permits_bind_one_autocommit_write_and_refuse_batches_and_expired_submissions() {
        let (mut frontend, driver) = test_frontend(true).await;
        frontend.startup.gateway_session_state = true;
        let node = frontend.node.clone();
        let driver = tokio::spawn(driver.run());
        let memory = Arc::clone(node.memory());
        let future_cutoff = rtdb_types::Timestamp(node.now().0 + 60_000_000);
        let expired_cutoff = rtdb_types::Timestamp(node.now().0 - 1);
        let mut writer = Writer::new(Vec::new(), Arc::clone(&memory));
        frontend.simple("INSERT INTO counters (_id, n) VALUES (1, 0), (2, 0), (3, 0)", &mut writer).await.unwrap();

        for (id, mode, not_after, sql) in [
            (1, GatewayReplayMode::Dispatch, future_cutoff, "UPDATE counters SET n = n + 1 WHERE _id = 1"),
            (1, GatewayReplayMode::Replay, future_cutoff, "UPDATE counters SET n = n + 1 WHERE _id = 1"),
            (2, GatewayReplayMode::Dispatch, future_cutoff, "BEGIN READ WRITE WITH (METADATA = {idempotency_key: 'app-key'}); UPDATE counters SET n = n + 1 WHERE _id = 2; COMMIT"),
            (2, GatewayReplayMode::Replay, future_cutoff, "BEGIN READ WRITE WITH (METADATA = {idempotency_key: 'app-key'}); UPDATE counters SET n = n + 1 WHERE _id = 2; COMMIT"),
            (3, GatewayReplayMode::Dispatch, expired_cutoff, "UPDATE counters SET n = n + 1 WHERE _id = 3"),
            (3, GatewayReplayMode::Replay, expired_cutoff, "UPDATE counters SET n = n + 1 WHERE _id = 3"),
        ] {
            let permit = GatewayReplayPermit { nonce: uuid::Uuid::from_u128(id), not_after, mode };
            let frame = Frame { tag: GATEWAY_SESSION_STATE_TAG, body: permit.encode().to_vec(), charge: memory.reserve(0).unwrap() };
            frontend.handle(frame, &mut writer).await.unwrap();
            frontend.simple(sql, &mut writer).await.unwrap();
        }
        frontend.simple("SELECT n FROM counters ORDER BY _id", &mut writer).await.unwrap();
        let bytes = writer.into_inner();
        let mut reader = crate::wire::Reader::new(bytes.as_slice(), memory);
        let mut counters = Vec::new();
        let mut errors = Vec::new();
        while let Some(frame) = reader.read(1 << 20).await.unwrap() {
            if frame.tag == b'D' { counters.push(frame.body[6..].to_vec()); }
            if frame.tag == b'E' { errors.push(String::from_utf8_lossy(&frame.body).into_owned()); }
        }
        assert_eq!(counters, [b"1".to_vec(), b"1".to_vec(), b"0".to_vec()]);
        assert_eq!(errors.len(), 3, "{errors:?}");
        assert!(errors[0].contains("Automatic replay requires"), "{errors:?}");
        drop(frontend);
        node.shutdown();
        driver.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn a_single_autocommit_write_reports_success_only_after_commit() {
        let (mut frontend, driver) = test_frontend(true).await;
        let node = frontend.node.clone();
        let memory = Arc::clone(node.memory());
        let driver = tokio::spawn(driver.run());
        for (sql, expected) in [
            ("INSERT INTO docs (_id, value) VALUES (NULL, 'invalid')", b'E'),
            ("INSERT INTO docs (_id, value) VALUES (1, 'valid')", b'C'),
        ] {
            let mut writer = Writer::new(Vec::new(), Arc::clone(&memory));
            frontend.simple(sql, &mut writer).await.unwrap();
            let bytes = writer.into_inner();
            let mut reader = crate::wire::Reader::new(bytes.as_slice(), Arc::clone(&memory));
            assert_eq!(reader.read(1 << 20).await.unwrap().unwrap().tag, expected);
            assert_eq!(reader.read(1 << 20).await.unwrap().unwrap().tag, b'Z');
            assert!(reader.read(1 << 20).await.unwrap().is_none());
        }
        drop(frontend);
        node.shutdown();
        driver.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn initial_simple_query_error_is_written_once_on_partial_connection_failure() {
        use std::{io, pin::Pin, task::{Context, Poll}};

        #[derive(Default)]
        struct PartialWriteFailure {
            writes: usize,
            bytes: Vec<u8>,
        }

        impl AsyncWrite for PartialWriteFailure {
            fn poll_write(mut self: Pin<&mut Self>, _: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> {
                self.writes += 1;
                if self.writes == 2 { return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into())); }
                self.bytes.extend_from_slice(bytes);
                Poll::Ready(Ok(bytes.len()))
            }

            fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
            fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> { Poll::Ready(Ok(())) }
        }

        let (mut frontend, driver) = test_frontend(false).await;
        let memory = Arc::clone(frontend.node.memory());
        let baseline = memory.used_bytes();
        let input = b"Q\0\0\0\x0cSELEC 1\0";
        let mut reader = crate::wire::Reader::new(input.as_slice(), Arc::clone(&memory));
        let frame = reader.read(1024).await.unwrap().unwrap();
        let mut writer = Writer::new(PartialWriteFailure::default(), Arc::clone(&memory));
        let error = frontend.handle(frame, &mut writer).await.unwrap_err();
        assert_eq!(error.code, "xtdb.pgwire/connection-error");
        let output = writer.into_inner();
        assert_eq!(output.writes, 2);
        assert_eq!(output.bytes, b"E");
        assert_eq!(memory.used_bytes(), baseline);
        drop(frontend);
        driver.close_without_run().await.unwrap();
    }

    #[tokio::test]
    async fn bind_argument_slots_are_admitted_before_publishing_a_portal() {
        let (mut frontend, driver) = test_frontend(false).await;
        let memory = Arc::clone(frontend.node.memory());
        let prepared = frontend.prepare_client("SET TIME ZONE 'UTC'", Vec::new(), None).await.unwrap();
        frontend.register_statement(Cow::Borrowed("zone"), prepared).unwrap();

        let count = 1024u16;
        let mut body = Vec::new();
        crate::wire::cstring(&mut body, "candidate").unwrap();
        crate::wire::cstring(&mut body, "zone").unwrap();
        body.extend_from_slice(&0u16.to_be_bytes());
        body.extend_from_slice(&count.to_be_bytes());
        for _ in 0..count { body.extend_from_slice(&(-1i32).to_be_bytes()); }
        body.extend_from_slice(&0u16.to_be_bytes());
        let charge = memory.reserve(body.len() * 16 + 8192).unwrap();
        let frame = Frame { tag: b'B', body, charge };
        let before = memory.used_bytes();
        let blocked = memory.reserve(memory.limit_bytes() - before - 1).unwrap();
        let mut writer = Writer::new(tokio::io::sink(), Arc::clone(&memory));
        let error = frontend.message(&frame, &mut writer).await.unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        assert!(!frontend.portals.contains_key("candidate"));
        assert!(frontend.statements.contains_key("zone"));
        assert_eq!(memory.used_bytes(), before + blocked.bytes());
        drop(blocked);

        frontend.message(&frame, &mut writer).await.unwrap();
        let portal = frontend.portals.get("candidate").unwrap();
        let arguments = portal.arguments.as_ref().unwrap().single_arrow_page().unwrap().batch();
        assert_eq!(arguments.num_columns(), usize::from(count));
        assert!(arguments.columns().iter().all(|argument| argument.data_type() == &arrow_schema::DataType::Null));
        assert!(memory.used_bytes() >= before + usize::from(count) * std::mem::size_of::<Value>());
        frontend.portals.clear();
        assert_eq!(memory.used_bytes(), before);
        drop((frame, writer, frontend));
        driver.close_without_run().await.unwrap();
    }

    #[tokio::test]
    async fn extended_namespace_rewrite_holds_syntax_growth_through_plan_admission() {
        let (mut frontend, driver) = test_frontend(false).await;
        let memory = Arc::clone(frontend.node.memory());
        let baseline = memory.used_bytes();
        let original = DBEAVER_NAMESPACE_QUERY;
        let rewritten = DBEAVER_NAMESPACE_REPLACEMENT;
        let padded = format!("{original}{}", " ".repeat(rewritten.len() - original.len()));
        let syntax_bytes = rtdb_sql::statement_syntax_estimate(rewritten.len()).unwrap();

        let scratch = MemoryBudget::new(32 << 20);
        let mut options = frontend.options();
        assert!(rtdb_sql::parse_admitted_control_in(rewritten, &options, &scratch).unwrap().is_none());
        let control_peak = scratch.peak_bytes();
        let token = frontend.token();
        let context = frontend.session.prepare_context(&token).await.unwrap();
        let context_bytes = memory.used_bytes() - baseline;
        let probe = context.memory.reserve(1).unwrap();
        assert_eq!(memory.used_bytes(), baseline + context_bytes + 1);
        drop(probe);
        options.default_timezone = context.timezone.clone();
        options.generate_series_end_exclusive = context.options.generate_series_end_exclusive;
        let raw = rtdb_sql::prepare_in(rewritten, &context.catalog, &options, &scratch).unwrap();
        assert_eq!(scratch.used_bytes(), 0);
        let plan = rtdb_query::PreparedQuery::from_prepared(raw, &scratch).unwrap();
        let plan_bytes = scratch.used_bytes();
        drop((plan, context));
        assert_eq!(scratch.used_bytes(), 0);
        assert_eq!(memory.used_bytes(), baseline);

        let before_plan = syntax_bytes + context_bytes + 2 * 1024 + rewritten.len() + 256;
        let plan_boundary = before_plan + plan_bytes;
        assert!(plan_boundary - 1 >= syntax_bytes + control_peak);

        let limit = memory.limit_bytes();
        let previous_peak = memory.peak_bytes();
        let calibration = memory.reserve(limit / 2 - baseline).unwrap();
        let calibration_floor = memory.used_bytes();
        assert!(calibration_floor > previous_peak);
        let prepared = frontend.prepare_client(&padded, Vec::new(), None).await.unwrap();
        let successful_headroom = memory.peak_bytes() - calibration_floor;
        let retained = memory.used_bytes() - calibration_floor;
        assert!(successful_headroom >= plan_boundary);
        assert!(matches!(prepared.ordinary_kind(), Some(PreparedKind::Query(query)) if query.source() == rewritten));
        drop((prepared, calibration));
        assert_eq!(memory.used_bytes(), baseline);

        let short = memory.reserve(limit - baseline - plan_boundary + 1).unwrap();
        let retained_at_plan = memory.used_bytes() + before_plan;
        let expected = format!("Cannot reserve {plan_bytes} bytes from budget {limit} with {retained_at_plan} retained");
        for source in [padded.as_str(), original] {
            let error = frontend.prepare_client(source, Vec::new(), None).await.err().unwrap();
            assert_eq!(error.code, "rtdb/memory-budget-exceeded");
            assert_eq!(error.message.as_deref(), Some(expected.as_str()));
            assert_eq!(memory.used_bytes(), baseline + short.bytes());
        }
        drop(short);

        let exact = memory.reserve(limit - baseline - successful_headroom).unwrap();
        for source in [padded.as_str(), original] {
            let prepared = frontend.prepare_client(source, Vec::new(), None).await.unwrap();
            assert!(matches!(prepared.ordinary_kind(), Some(PreparedKind::Query(query)) if query.source() == rewritten));
            assert_eq!(memory.used_bytes(), baseline + exact.bytes() + retained);
            drop(prepared);
            assert_eq!(memory.used_bytes(), baseline + exact.bytes());
        }
        assert_eq!(memory.peak_bytes(), limit);
        drop(exact);
        assert_eq!(memory.used_bytes(), baseline);
        drop(frontend);
        driver.close_without_run().await.unwrap();
    }

    #[tokio::test]
    async fn simple_control_reaches_execution_at_a_budget_for_one_lowered_command() {
        use rtdb_columnar::MemoryBudget;

        let sql = format!("SET TIME ZONE /*{}*/ COALESCE($1, REGEXP_REPLACE('UTC', 'U', 'U'))", "x".repeat(512));
        let source = format!("-- début\r\n{sql} /* ignored */;  ");
        let parse_probe = MemoryBudget::new(1 << 20);
        let mut batch = rtdb_sql::parse_statement_batch(normalized_simple_text(&source).0, &parse_probe).unwrap();
        let BatchStatement { command, allocation, preparations } = batch.next().unwrap();
        assert!(batch.next().is_none());
        assert!(preparations.is_empty());
        assert_eq!(command.source(), sql);
        let syntax_bytes = parse_probe.used_bytes();
        let batch_bytes = syntax_bytes - allocation.bytes();
        let ExecutableSyntax::Control(syntax) = command else { panic!("control syntax"); };
        let control = syntax.bind(&Default::default()).unwrap();
        assert!(matches!(control, ControlStatement::SetTimeZone(_)));
        assert_eq!(control.parameter_count(), 1);
        let control_bytes = parse_probe.used_bytes() - syntax_bytes;
        assert!(control_bytes > 0);
        drop(control);
        drop(allocation);
        drop(batch);
        assert_eq!(parse_probe.used_bytes(), 0);

        let missing = Anomaly::incorrect("xtdb/missing-parameter", "Control statement has fewer parameters than its expressions");
        let writer_probe = MemoryBudget::new(1 << 20);
        let environment = SessionEnvironment::new(std::iter::empty(), "UTC", &writer_probe).unwrap();
        let mut expected = Writer::new(Vec::new(), Arc::clone(&writer_probe));
        expected.error(&missing, environment.fallback(), false).await.unwrap();
        expected.ready(b'I').await.unwrap();
        drop(environment);
        assert_eq!(writer_probe.used_bytes(), 0);

        let statement_bytes = sql.len() * 128 + 8192;
        let portal_peak = batch_bytes + control_bytes + statement_bytes + 8192;
        assert!(parse_probe.peak_bytes() < portal_peak);
        assert!(batch_bytes + writer_probe.peak_bytes() < portal_peak);

        let (mut frontend, driver) = test_frontend(false).await;
        let memory = Arc::clone(frontend.node.memory());

        let initial_timezone = frontend.session.state().effective_timezone().to_owned();
        let frame_bytes = (source.len() + 1) * 16 + 8192;
        let baseline = memory.used_bytes();
        let held = memory.reserve(memory.limit_bytes() - baseline - frame_bytes - portal_peak).unwrap();
        let mut input = vec![b'Q'];
        input.extend_from_slice(&i32::try_from(source.len() + 5).unwrap().to_be_bytes());
        input.extend_from_slice(source.as_bytes());
        input.push(0);
        let mut reader = crate::wire::Reader::new(input.as_slice(), Arc::clone(&memory));
        let frame = reader.read(source.len() + 1).await.unwrap().unwrap();
        assert_eq!(frame.charge.bytes(), frame_bytes);
        let mut writer = Writer::new(Vec::new(), Arc::clone(&memory));
        frontend.handle(frame, &mut writer).await.unwrap();
        assert_eq!(writer.into_inner(), expected.into_inner());
        assert_eq!(frontend.session.state().status(), TransactionStatus::Idle);
        assert_eq!(frontend.session.state().effective_timezone(), initial_timezone);
        assert_eq!(memory.peak_bytes(), memory.limit_bytes());
        assert_eq!(memory.used_bytes(), baseline + held.bytes());
        drop(held);
        assert_eq!(memory.used_bytes(), baseline);
        drop(frontend);
        driver.close_without_run().await.unwrap();
    }

    #[tokio::test]
    async fn extended_control_parse_refusal_preserves_the_registration_and_releases_the_probe() {
        use rtdb_columnar::MemoryBudget;

        let replacement = "SET TIME ZONE 'UTC'";
        let calibration = MemoryBudget::new(1 << 20);
        let classified = rtdb_sql::parse_extended_statement(replacement, &calibration).unwrap();
        assert!(matches!(&classified.statement, ExtendedStatement::Ordinary(source) if *source == replacement));
        let classification_bytes = calibration.used_bytes();
        drop(classified);
        assert_eq!(calibration.used_bytes(), 0);

        let control_budget = MemoryBudget::new(1 << 20);
        let control = rtdb_sql::parse_admitted_control_in(replacement, &Default::default(), &control_budget).unwrap().unwrap();
        let control_bytes = control_budget.used_bytes();
        assert!(control_bytes > 0);
        let short_bytes = control_budget.peak_bytes() - 1;
        drop(control);
        assert_eq!(control_budget.used_bytes(), 0);
        let short = MemoryBudget::new(short_bytes);
        let expected = rtdb_sql::parse_admitted_control_in(replacement, &Default::default(), &short).unwrap_err();
        assert_eq!(expected.code, "rtdb/memory-budget-exceeded");
        let (requested, _) = expected.message.as_deref().unwrap().split_once(" from budget ").unwrap();
        assert_eq!(short.used_bytes(), 0);

        let (mut frontend, driver) = test_frontend(false).await;
        let memory = Arc::clone(frontend.node.memory());
        let baseline = memory.used_bytes();
        let old = frontend.prepare_client("SET TIME ZONE 'Europe/Paris'", Vec::new(), None).await.unwrap();
        let old_statement = Arc::downgrade(&old);
        frontend.register_statement(Cow::Borrowed("zone"), Arc::clone(&old)).unwrap();
        let portal = frontend.bind(old, None, Vec::new(), &[]).await.unwrap();
        let old_name_bytes = frontend.statements["zone"]._reservation.bytes();
        let retained = memory.used_bytes();

        let parse_frame = || {
            let mut body = Vec::new();
            crate::wire::cstring(&mut body, "zone").unwrap();
            crate::wire::cstring(&mut body, replacement).unwrap();
            body.extend_from_slice(&0u16.to_be_bytes());
            let charge = memory.reserve(body.len() * 16 + 8192).unwrap();
            Frame { tag: b'P', body, charge }
        };
        let frame = parse_frame();
        let held = memory.reserve(memory.limit_bytes() - memory.used_bytes() - classification_bytes - short_bytes).unwrap();
        let mut writer = Writer::new(Vec::new(), Arc::clone(&memory));
        let error = frontend.message(&frame, &mut writer).await.unwrap_err();
        assert_eq!(error.code, expected.code);
        assert_eq!(error.message.as_deref().unwrap().split_once(" from budget ").unwrap().0, requested);
        assert!(writer.into_inner().is_empty());
        assert_eq!(Arc::as_ptr(&frontend.statements["zone"].prepared), old_statement.as_ptr());
        drop(frame);
        assert_eq!(memory.used_bytes(), retained + held.bytes());
        drop(held);
        assert_eq!(memory.used_bytes(), retained);

        let frame = parse_frame();
        let frame_charge = Arc::downgrade(&frame.charge);
        let mut writer = Writer::new(Vec::new(), Arc::clone(&memory));
        frontend.handle(frame, &mut writer).await.unwrap();
        assert_eq!(writer.into_inner(), b"1\0\0\0\x04");
        assert!(frame_charge.upgrade().is_none());
        let installed = &frontend.statements["zone"];
        assert_ne!(Arc::as_ptr(&installed.prepared), old_statement.as_ptr());
        assert_eq!(Arc::as_ptr(&portal.prepared), old_statement.as_ptr());
        assert_eq!(memory.used_bytes(), retained - old_name_bytes + installed._reservation.bytes() + installed.prepared._reservation.bytes() + control_bytes);
        drop(portal);
        assert!(old_statement.upgrade().is_none());
        frontend.close();
        assert_eq!(memory.used_bytes(), baseline);
        drop(frontend);
        driver.close_without_run().await.unwrap();
    }

    #[tokio::test]
    async fn retained_prepare_outlives_parse_frame_and_retries_after_leaf_admission_failure() {
        let (mut frontend, driver) = test_frontend(false).await;
        let memory = Arc::clone(frontend.node.memory());
        let baseline = memory.used_bytes();
        let leaf = "SET TIME ZONE 'UTC'";
        let old = frontend.prepare_client("SET TIME ZONE 'Europe/Paris'", Vec::new(), None).await.unwrap();
        let old_statement = Arc::downgrade(&old);
        frontend.register_statement(Cow::Borrowed("target"), old).unwrap();

        let mut body = Vec::new();
        crate::wire::cstring(&mut body, "wire").unwrap();
        crate::wire::cstring(&mut body, &format!("PREPARE target AS {leaf}")).unwrap();
        body.extend_from_slice(&0u16.to_be_bytes());
        let charge = memory.reserve(body.len() * 16 + 8192).unwrap();
        let frame_charge = Arc::downgrade(&charge);
        let mut writer = Writer::new(Vec::new(), Arc::clone(&memory));
        frontend.handle(Frame { tag: b'P', body, charge }, &mut writer).await.unwrap();
        assert_eq!(writer.into_inner(), b"1\0\0\0\x04");
        assert!(frame_charge.upgrade().is_none());

        let prepared = Arc::clone(&frontend.statements["wire"].prepared);
        let wrapper = Arc::downgrade(&prepared);
        let mut portal = frontend.bind(prepared, None, Vec::new(), &[]).await.unwrap();
        let retained = memory.used_bytes();
        let argument_scope = frontend.session.admit_frontend_prepare(&[], &portal.cancellation).await.unwrap();
        let arguments = memory.used_bytes() - retained;
        drop(argument_scope);
        assert_eq!(memory.used_bytes(), retained);

        let leaf_bytes = leaf.len() * 128 + 8192;
        let held = memory.reserve(memory.limit_bytes() - retained - arguments - leaf_bytes + 1).unwrap();
        let mut writer = Writer::new(Vec::new(), Arc::clone(&memory));
        let error = frontend.execute(&mut portal, 0, false, None, &mut writer).await.unwrap_err();
        assert_eq!(error.code, "rtdb/memory-budget-exceeded");
        let request = format!("Cannot reserve {leaf_bytes} bytes");
        assert!(error.message.as_deref().unwrap().starts_with(request.as_str()));
        assert_eq!(Arc::as_ptr(&frontend.statements["target"].prepared), old_statement.as_ptr());
        drop(held);
        assert_eq!(memory.used_bytes(), retained);

        frontend.execute(&mut portal, 0, false, None, &mut writer).await.unwrap();
        assert!(old_statement.upgrade().is_none());
        frontend.portals.insert("portal".into(), portal);
        let mut body = vec![b'S'];
        crate::wire::cstring(&mut body, "wire").unwrap();
        let charge = memory.reserve(body.len() * 16 + 8192).unwrap();
        frontend.handle(Frame { tag: b'C', body, charge }, &mut writer).await.unwrap();
        assert!(!frontend.portals.contains_key("portal"));
        assert!(!frontend.statements.contains_key("wire"));
        assert!(wrapper.upgrade().is_none());
        assert_eq!(writer.into_inner(), b"C\0\0\0\x0cPREPARE\0\x33\0\0\0\x04");
        frontend.close();
        assert_eq!(memory.used_bytes(), baseline);
        drop(frontend);
        driver.close_without_run().await.unwrap();
    }
}

#[cfg(test)]
mod argument_tests {
    use std::sync::Arc;
    use rtdb_columnar::{MemoryBudget, OwnedBatch};
    use rtdb_pg_codec::Format;
    use rtdb_tx::{SourceTransaction, SqlArguments, SubmitOptions, TxOp};
    use rtdb_types::PgType;

    #[test]
    fn canonical_pgwire_arguments_keep_legacy_sql_log_bytes() {
        let memory = MemoryBudget::new(16 << 20);
        let nan = 0x7fa0_0001u32.to_be_bytes();
        let inputs = [
            (PgType::Int8, Format::Text, b"42".as_slice()),
            (PgType::Float4, Format::Binary, nan.as_slice()),
            (PgType::Int4s, Format::Text, b"{4,NULL,8}".as_slice()),
            (PgType::Numeric, Format::Text, b"1.25".as_slice()),
            (PgType::Json, Format::Text, br#"{"value":[1.5,null,2.5]}"#.as_slice()),
        ];
        let mut values = Vec::new();
        let mut fields = Vec::new();
        let mut arrays = Vec::new();
        for (index, (ty, format, payload)) in inputs.into_iter().enumerate() {
            let decoded = rtdb_pg_codec::decode(ty, format, payload, &memory).unwrap();
            values.push(decoded.into_parts().0);
            let (field, array, _) = rtdb_pg_codec::decode_argument(&format!("?_{index}"), ty, format, payload, &memory).unwrap();
            fields.push(field);
            arrays.push(array);
        }
        let batch = arrow_array::RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), arrays).unwrap();
        let typed = SqlArguments::from_canonical_arrow_row(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();
        let rows = SqlArguments::from_rows(vec![values], &memory).unwrap();
        let encode = |sql: &str, args| {
            SourceTransaction::new(vec![TxOp::Sql { sql: sql.into(), args: Some(args) }], SubmitOptions::default(), &memory)
                .unwrap().encode().unwrap()
        };
        let sql = "UPDATE docs SET v = $2 WHERE _id = $1";
        assert_eq!(encode(sql, typed.clone()).as_slice(), encode(sql, rows.clone()).as_slice());
        let (mut typed_pair, mut row_pair) = (typed.clone(), rows.clone());
        typed_pair.append(typed.clone()).unwrap();
        row_pair.append(rows.clone()).unwrap();
        assert_eq!(encode(sql, typed_pair).as_slice(), encode(sql, row_pair).as_slice());
        let submission = |args| {
            let sql = "INSERT INTO docs (_id, vector) VALUES ($1, $3)";
            rtdb_tx::prepare_submission(vec![TxOp::Sql { sql: sql.into(), args: Some(args) }],
                SubmitOptions::default(), "xtdb", &memory).unwrap().encode().unwrap()
        };
        assert_eq!(submission(typed).as_slice(), submission(rows).as_slice());
    }
}
