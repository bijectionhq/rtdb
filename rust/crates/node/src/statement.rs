use std::sync::{Arc, OnceLock, Weak};

use rtdb_columnar::{MemoryReservation, OwnedBatch};
use rtdb_plan::{Catalog, CatalogLookups, ColumnId, Field, PreparedStatement, RecordingCatalog, Schema, SqlNotice, Statement, TraceValue};
use rtdb_query::{PreparedQuery, QueryContext, QueryCursor, RuntimeSchema, SharedPlan, SharedPlanKey};
use rtdb_sql::{DeferredStatementSyntax, ParsedStatement, PrepareOptions, StatementKind, StatementSyntaxBatch};
use rtdb_tx::{PreparedDml, ScheduledOperation, SqlArguments, TxOp};
use rtdb_types::{Anomaly, AnomalyCategory, AnomalyKind, LogicalType, MemoryBudget, PgType, Result, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResultSignaturePolicy { Dynamic, Exact, PostgreSql }

struct PreparedData {
    source: String,
    options: PrepareOptions,
    parameters: Vec<LogicalType>,
    notices: Vec<SqlNotice>,
    ordered_index_command_tag: Option<&'static str>,
    snapshot_selector: rtdb_plan::QuerySettings,
    result: PreparedResult,
    policy: ResultSignaturePolicy,
    sharable: bool,
    _reservation: Arc<MemoryReservation>,
    plan: parking_lot::Mutex<Option<CachedPlan>>,
}
enum PreparedResult {
    Query { schema: Schema, pg_types: Vec<PgType>, parameter_projection: bool },
    Dml { static_source: Option<OnceLock<PreparedDml>> },
}

/// An immutable SQL preparation; each query execution binds against its supplied snapshot.
/// Session access checks and read-basis capture precede this API and remain the caller's obligation.
#[derive(Clone)]
pub struct PreparedSql(Arc<PreparedData>);

fn deep<T>(source: &str, prepare: impl FnOnce() -> T) -> T {
    const DEEP_SOURCE_BYTES: usize = 4096;
    if source.len() >= DEEP_SOURCE_BYTES {
        stacker::grow(64 << 20, prepare)
    } else {
        stacker::maybe_grow(512 << 10, 8 << 20, prepare)
    }
}

#[cfg(test)]
mod stack_tests {
    #[test]
    fn short_sql_preparation_checks_the_callers_remaining_stack() {
        stacker::grow(128 << 10, || {
            super::deep("SELECT 1", || assert!(stacker::remaining_stack().unwrap() > 512 << 10));
        });
    }
}

impl PreparedSql {
    pub fn prepare(source: &str, context: &Arc<QueryContext>, mut options: PrepareOptions, policy: ResultSignaturePolicy) -> Result<Self> {
        let source_reservation = admit_source(source, context, &mut options)?;
        let mut syntax = Some(admit_sql_syntax(source, &context.memory)?);
        with_read_syntax(source, context, || one_statement(source, &context.memory), |parsed| {
            match parsed {
                Ok(parsed) => deep(source, || Self::prepare_parsed(parsed, context, options, &[], policy, false, false, source_reservation)),
                Err(error) => {
                    drop((syntax.take(), source_reservation));
                    Err(statement_syntax_error(source, error, &context.memory))
                }
            }
        })
    }

    pub fn prepare_postgres(source: &str, context: &Arc<QueryContext>, search_path: Vec<String>, parameter_oids: &[i32]) -> Result<Self> {
        Self::prepare_postgres_with_syntax_error(source, context, search_path, parameter_oids, |error| error)
    }

    /// Applies the frontend diagnostic mapper to failures from the surrounding SQL parser.
    /// Operand resolution and binding retain their own diagnostic paths.
    pub fn prepare_postgres_with_syntax_error(
        source: &str, context: &Arc<QueryContext>, search_path: Vec<String>, parameter_oids: &[i32],
        syntax_error: impl FnOnce(Anomaly) -> Anomaly,
    ) -> Result<Self> {
        let parse = || {
            let syntax = rtdb_sql::parse_syntax_in(source, &context.memory).map_err(syntax_error)?;
            exactly_one_statement(syntax.resolve_in(&context.memory)?)
        };
        with_read_syntax(source, context, parse, |parsed| Self::prepare_postgres_parsed(parsed?, context, search_path, parameter_oids))
    }

    /// Requires syntax parsed with the same budget as the context.
    /// Returns the first source-order resolution error before checking that the input contains exactly one statement.
    /// Query parameter OIDs are positional. Missing OIDs or OIDs without a logical type leave query typing dynamic.
    pub fn prepare_postgres_syntax(
        syntax: StatementSyntaxBatch<'_>, context: &Arc<QueryContext>, search_path: Vec<String>, parameter_oids: &[i32],
    ) -> Result<Self> {
        let parsed = exactly_one_statement(syntax.resolve_in(&context.memory)?)?;
        Self::prepare_postgres_parsed(parsed, context, search_path, parameter_oids)
    }

    /// Resolves and prepares the selected statement in the reached query context.
    pub fn prepare_postgres_statement(syntax: DeferredStatementSyntax<'_>, context: &Arc<QueryContext>, search_path: Vec<String>, parameter_oids: &[i32]) -> Result<Self> {
        let parsed = syntax.resolve_in(&context.memory)?;
        Self::prepare_postgres_parsed(parsed, context, search_path, parameter_oids)
    }

    fn prepare_postgres_parsed(parsed: ParsedStatement<'_>, context: &Arc<QueryContext>, search_path: Vec<String>, parameter_oids: &[i32]) -> Result<Self> {
        let source = parsed.source();
        deep(source, || Self::prepare_postgres_parsed_at(parsed, context, search_path, parameter_oids))
    }

    fn prepare_postgres_parsed_at(parsed: ParsedStatement<'_>, context: &Arc<QueryContext>, search_path: Vec<String>, parameter_oids: &[i32]) -> Result<Self> {
        let source = parsed.source();
        if parsed.kind() == StatementKind::Dml {
            let mut options = PrepareOptions { search_path, ..Default::default() };
            let source_reservation = admit_source(source, context, &mut options)?;
            return Self::prepare_parsed(parsed, context, options, &[], ResultSignaturePolicy::PostgreSql, false, parameter_oids.is_empty(), source_reservation);
        }
        let count = parsed.parameter_count();
        let _parameters = context.memory.reserve(count.checked_mul(1024).and_then(|n| n.checked_add(1024)).ok_or_else(size_error)?)?;
        let mut options = PrepareOptions { search_path, ..Default::default() };
        let mut dynamic = false;
        let mut untyped = Vec::new();
        for index in 0..count {
            let oid = parameter_oids.get(index).copied().unwrap_or(0);
            match PgType::from_oid(oid).and_then(PgType::logical_type) {
                Some(ty) => {
                    options.parameter_types.insert(index, ty.maybe());
                    if oid == 0 { untyped.push(index); }
                }
                None => { dynamic = true; }
            }
        }
        let source_reservation = admit_source(source, context, &mut options)?;
        Self::prepare_parsed(parsed, context, options, &untyped, ResultSignaturePolicy::PostgreSql, dynamic, parameter_oids.is_empty(), source_reservation)
    }

    /// `untyped` parameters are planned and described with their type in `options`, as PostgreSQL describes an
    /// untyped parameter as text, but declare nothing: each execution binds them with its argument's type.
    fn prepare_parsed(
        parsed: ParsedStatement<'_>, context: &Arc<QueryContext>, mut options: PrepareOptions, untyped: &[usize], policy: ResultSignaturePolicy,
        dynamic_pg: bool, dml_notices: bool, source_reservation: Arc<MemoryReservation>,
    ) -> Result<Self> {
        let source = parsed.source();
        let count = parsed.parameter_count();
        let ordered_index_command_tag = parsed.ordered_index_command_tag();
        let sharable = !parsed.is_explain();
        let snapshot_selector = parsed.snapshot_selector()?;
        if options.parameter_types.keys().any(|index| *index >= count) { return Err(parameter_error("Declared parameter lies outside the statement")); }
        let parameter_bytes = options.parameter_types.values().try_fold(count.checked_mul(1024).and_then(|n| n.checked_add(1024)).ok_or_else(size_error)?, |bytes, ty| bytes.checked_add(ty.retained_bytes()?).ok_or_else(size_error))?;
        let _parameters = context.memory.reserve(parameter_bytes)?;
        let mut parameters = (0..count).map(|index| options.parameter_types.get(&index).cloned().unwrap_or(LogicalType::Null)).collect::<Vec<_>>();
        let mut notices = Vec::new();
        let mut cached = None;
        let result = match parsed.kind() {
            StatementKind::Dml => {
                let static_source = parsed.has_static_document_source().then(OnceLock::new);
                if dml_notices { notices = parsed.bind(&context.catalog, &options).map(|prepared| prepared.notices).unwrap_or_default(); }
                PreparedResult::Dml { static_source }
            }
            StatementKind::Query => {
                let shared = if sharable {
                    if let Some(plans) = context.shared_plans() { plans.record_read_preparation(); }
                    shared_plan(source, context, &options, |catalog, budget| bind_read(context, || parsed.bind_in(catalog, &options, budget)))?
                } else { None };
                let (query, inferred, kept) = match shared {
                    Some(shared) => {
                        notices.clone_from(&shared.notices);
                        (shared.query.clone(), shared.parameter_types.clone(), Some(KeptPlan::Shared(Arc::downgrade(&shared))))
                    }
                    None => {
                        let recording = RecordingCatalog::new(&context.catalog);
                        let prepared = bind_read(context, || parsed.bind(&recording, &options))?;
                        let inferred = prepared.parameter_types.clone();
                        notices = prepared.notices.clone();
                        let query = optimize_read(prepared, context, &context.memory)?;
                        let kept = recording.finish().map(|lookups| KeptPlan::Own { lookups: Arc::new(lookups), query: query.clone() });
                        (query, inferred, kept)
                    }
                };
                for (index, ty) in &inferred {
                    if !options.parameter_types.contains_key(index) { parameters[*index] = ty.clone(); }
                }
                let schema = if policy == ResultSignaturePolicy::PostgreSql && !dynamic_pg {
                    query.result_schema(context)?.schema().clone()
                } else {
                    query.schema().clone()
                };
                let pg_types = schema.fields().iter().map(|field| if dynamic_pg || policy != ResultSignaturePolicy::PostgreSql { Ok(PgType::Default) } else { PgType::from_logical(&field.data_type) }).collect::<Result<_>>()?;
                let result = PreparedResult::Query { schema, pg_types, parameter_projection: query.is_parameter_projection() };
                cached = kept.map(|plan| CachedPlan { options: options.clone(), plan });
                result
            }
            StatementKind::Session => return Err(session_statement(source, &options, &context.memory)),
        };
        for index in untyped { options.parameter_types.remove(index); }
        let source = source.to_owned();
        let bytes = retained_bytes(&source, &options, &parameters, &notices, &result)?;
        let bytes = bytes.checked_add(snapshot_selector.retained_bytes()?).ok_or_else(size_error)?;
        let reservation = context.memory.reserve(bytes)?;
        drop(source_reservation);
        Ok(Self(Arc::new(PreparedData { source, options, parameters, notices, ordered_index_command_tag, snapshot_selector, result, policy, sharable, _reservation: reservation, plan: parking_lot::Mutex::new(cached) })))
    }

    pub fn search_path(&self) -> &[String] { &self.0.options.search_path }
    pub fn source(&self) -> &str { &self.0.source }
    pub fn parameter_count(&self) -> usize { self.0.parameters.len() }
    pub fn parameter_types(&self) -> &[LogicalType] { &self.0.parameters }
    /// Planning warnings in source traversal order; repeated occurrences are retained.
    /// Execution-time rebinding does not append warnings to this immutable preparation.
    pub fn notices(&self) -> &[SqlNotice] { &self.0.notices }
    pub fn ordered_index_command_tag(&self) -> Option<&'static str> { self.0.ordered_index_command_tag }
    pub fn selects_read_settings(&self) -> bool { self.0.snapshot_selector != Default::default() }
    pub fn is_dml(&self) -> bool { matches!(&self.0.result, PreparedResult::Dml { .. }) }
    pub fn prepared_schema(&self) -> Option<&Schema> { match &self.0.result { PreparedResult::Query { schema, .. } => Some(schema), PreparedResult::Dml { .. } => None } }
    pub fn postgres_types(&self) -> Option<&[PgType]> { match &self.0.result { PreparedResult::Query { pg_types, .. } => Some(pg_types), PreparedResult::Dml { .. } => None } }

    /// ADBC parameters are positional, have empty field names, and publish unknown nullable types.
    pub fn parameter_schema(&self) -> Result<Schema> {
        Schema::new((0..self.parameter_count()).map(|index| Ok(Field { id: ColumnId(u32::try_from(index).map_err(|_| size_error())?), name: String::new(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Null })).collect::<Result<_>>()?)
    }

    /// Returns the current Arrow signature from supplied physical parameter fields without opening vectors.
    pub fn result_schema(&self, context: &Arc<QueryContext>, argument_types: &[LogicalType]) -> Result<Arc<RuntimeSchema>> {
        deep(self.source(), || self.bind(context, argument_types, false))?.result_schema(context)
    }

    pub async fn open_query(&self, context: Arc<QueryContext>, arguments: Vec<Value>) -> Result<QueryCursor> {
        let bound = arguments.iter().try_fold(1024usize, |bytes, value| bytes.checked_add(value.retained_bytes()?.checked_mul(8).ok_or_else(size_error)?).ok_or_else(size_error))?;
        let _types = context.memory.reserve(bound)?;
        let types = arguments.iter().map(Value::logical_type).collect::<Vec<_>>();
        self.open_query_typed(context, arguments, &types).await
    }

    /// Opens one argument row using its vector field types, preserving typed nulls and empty containers.
    /// Unsigned storage types and values enter execution at the corresponding signed width.
    pub async fn open_query_typed(&self, context: Arc<QueryContext>, arguments: Vec<Value>, types: &[LogicalType]) -> Result<QueryCursor> {
        self.open_query_batches(context, arguments, types, false, false).await.map(|(cursor, _)| cursor)
    }

    /// Opens Arrow batches directly for a client that consumes Arrow.
    pub async fn open_query_columns_typed(&self, context: Arc<QueryContext>, arguments: Vec<Value>, types: &[LogicalType]) -> Result<QueryCursor> {
        self.open_query_batches(context, arguments, types, true, false).await.map(|(cursor, _)| cursor)
    }

    pub async fn open_tracked_query_columns(&self, context: Arc<QueryContext>, arguments: Vec<Value>, types: &[LogicalType])
        -> Result<(QueryCursor, rtdb_types::rtdb_v1::ClaimFootprint)> {
        self.open_query_batches(context, arguments, types, true, true).await
    }

    pub async fn try_open_query_arrow(&self, context: Arc<QueryContext>, arguments: &OwnedBatch, types: &[LogicalType]) -> Result<Option<QueryCursor>> {
        if arguments.batch().num_rows() == 0 { return Ok(None); }
        if arguments.batch().num_columns() != self.parameter_count() {
            return Err(parameter_error("Argument count differs from the prepared statement"));
        }
        if let Some(cursor) = self.try_open_parameter_projection(Arc::clone(&context), arguments, types).await? {
            return Ok(Some(cursor));
        }
        self.open_arrow(context, arguments, types, true).await.map(Some)
    }

    pub async fn open_query_arrow_rows(&self, context: Arc<QueryContext>, arguments: &OwnedBatch, types: &[LogicalType]) -> Result<QueryCursor> {
        self.open_arrow(context, arguments, types, false).await
    }

    async fn open_arrow(&self, context: Arc<QueryContext>, arguments: &OwnedBatch, types: &[LogicalType], columnar: bool) -> Result<QueryCursor> {
        if arguments.batch().num_columns() != self.parameter_count() {
            return Err(parameter_error("Argument count differs from the prepared statement"));
        }
        let arguments = if arguments.batch().num_rows() == 1 {
            arguments.clone()
        } else {
            arguments.slice(0, 1)?
        };
        let selector = &self.0.snapshot_selector;
        let context = if let Some(rtdb_plan::SettingValue::Parameter { index }) = &selector.snapshot_token {
            let batch = arguments.batch();
            let width = index.checked_add(1).ok_or_else(size_error)?;
            let scratch = width.checked_mul(std::mem::size_of::<Value>()).ok_or_else(size_error)?;
            let cell = rtdb_columnar::estimate_read_cell_bytes(batch.schema_ref().field(*index), batch.column(*index).as_ref(), 0)?;
            let _charge = context.memory.reserve(scratch.checked_add(cell).ok_or_else(size_error)?)?;
            let mut values = vec![Value::Null; width];
            values[*index] = rtdb_columnar::read_execution_value(batch.schema_ref().field(*index), batch.column(*index).as_ref(), 0)?;
            context.reopen_sql_snapshot(selector, rtdb_expr::Parameters::Values(&values)).await?
        } else {
            context.reopen_sql_snapshot(selector, rtdb_expr::Parameters::Values(&[])).await?
        };
        let arguments = self.cast_arrow_arguments(arguments, types, &context)?;
        let query = deep(self.source(), || self.bind(&context, types, true))?;
        if self.0.policy == ResultSignaturePolicy::Exact { self.check_signature(query.schema())?; }
        let cursor = if columnar {
            query.execute_arrow_columns(context, arguments, 0, types, Some(self.source())).await?
        } else {
            query.execute_arrow_column_rows(context, arguments, 0, types, Some(self.source())).await?
        };
        if self.0.policy == ResultSignaturePolicy::PostgreSql { self.check_signature(cursor.runtime_schema())?; }
        Ok(cursor)
    }

    fn cast_arrow_arguments(&self, arguments: OwnedBatch, types: &[LogicalType], context: &QueryContext) -> Result<OwnedBatch> {
        use arrow_array::{ArrayRef, ListArray, RecordBatch};
        use arrow_buffer::OffsetBuffer;
        use arrow_schema::DataType;
        use rtdb_columnar::{ArrowCell, ResultArrayFunding, retain_result_array};

        let batch = arguments.batch();
        let backing = arguments.buffer_backing();
        let mut replacement = None;
        for (&index, declared) in &self.0.options.parameter_types {
            let source = types.get(index).ok_or_else(|| parameter_error("Parameter field count differs from the prepared statement"))?;
            let target = rtdb_columnar::execution_type(declared);
            if rtdb_columnar::execution_type(source).maybe() == target.clone().maybe() { continue; }
            let array = batch.column(index);
            let field = batch.schema_ref().field(index);
            let cell = ArrowCell { field, array: array.as_ref(), row: 0 };
            if cell.is_null()? { continue; }
            let (fields, pending, setup) = match &mut replacement {
                Some(replacement) => replacement,
                None => {
                    let slots = self.0.options.parameter_types.len().checked_mul(std::mem::size_of::<(usize, ArrayRef, Arc<MemoryReservation>, bool)>())
                        .and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
                    let bound = rtdb_format::ipc::schema_translation_bound(batch.schema_ref())?
                        .checked_add(slots).ok_or_else(size_error)?;
                    let setup = rtdb_expr::EvaluationScope::new(&context.memory)?;
                    setup.reserve(bound)?;
                    replacement.insert((batch.schema_ref().fields().to_vec(), Vec::with_capacity(self.0.options.parameter_types.len()), setup))
                }
            };
            let allocation = setup.scratch();
            let (field, array, shared): (_, ArrayRef, _) = match rtdb_expr::cast_bound_float_list(cell, &target, &allocation, backing)? {
                Some((element, child, shared)) => {
                    let length = i32::try_from(child.len()).map_err(|_| {
                        Anomaly::incorrect("rtdb/list-offset-overflow", "List children exceed i32 offsets").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded)
                    })?;
                    allocation.reserve(field.name().len().checked_add(1024).ok_or_else(size_error)?)?;
                    let field = rtdb_columnar::to_field(field.name(), &LogicalType::list(element))?;
                    let DataType::List(item) = field.data_type() else { unreachable!("float list binding") };
                    let array = ListArray::try_new(Arc::clone(item), OffsetBuffer::new(vec![0, length].into()), child, None)
                        .map_err(rtdb_columnar::arrow_error)?;
                    (field, Arc::new(array), shared)
                }
                None => {
                    allocation.reserve(rtdb_columnar::estimate_read_cell_bytes(field, array.as_ref(), 0)?)?;
                    let value = rtdb_columnar::read_execution_value_as(source, field, array.as_ref(), 0)?;
                    let value = rtdb_expr::cast(&value, &target, &context.timezone)?;
                    allocation.reserve(value.retained_bytes()?)?;
                    let logical = value.logical_type();
                    let values = std::slice::from_ref(&value);
                    allocation.reserve(rtdb_columnar::estimate_builder_bytes(&logical, values)?)?;
                    let array = rtdb_columnar::copy_array(&logical, values)?;
                    (rtdb_columnar::to_field(field.name(), &logical)?, array, false)
                }
            };
            fields[index] = Arc::new(field);
            let grant = allocation.into_reservation()?.expect("Argument cast has a construction grant");
            pending.push((index, array, grant, shared));
        }
        let Some((fields, pending, _setup)) = replacement else { return Ok(arguments); };
        let schema_bytes = fields.iter().try_fold(256usize, |bytes, field| {
            field.size().checked_add(32).and_then(|size| bytes.checked_add(size)).ok_or_else(size_error)
        })?;
        let schema_charge = context.memory.reserve(schema_bytes)?;
        let page_bytes = batch.num_columns().checked_mul(std::mem::size_of::<ArrayRef>())
            .and_then(|bytes| bytes.checked_add(512)).ok_or_else(size_error)?;
        let page_bytes = batch.columns().iter().enumerate().try_fold(page_bytes, |bytes, (index, array)| {
            if pending.binary_search_by_key(&index, |(position, ..)| *position).is_ok() {
                return Ok(bytes);
            }
            array.get_array_memory_size().checked_sub(array.get_buffer_memory_size())
                .and_then(|size| bytes.checked_add(size)).ok_or_else(size_error)
        })?;
        let mut reservation = context.memory.reserve(page_bytes)?;
        let page = Arc::get_mut(&mut reservation).expect("Argument batch owns its descriptor charge");
        let mut arrays = batch.columns().to_vec();
        for (index, array, grant, shared) in pending {
            let funding = if shared {
                ResultArrayFunding::Relayout(batch.column(index), backing.expect("Shared cast has a source lease"))
            } else { ResultArrayFunding::Copy };
            arrays[index] = retain_result_array(array, grant, funding, page, &schema_charge)?;
        }
        let batch = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), arrays)
            .map_err(|error| parameter_error(&error.to_string()))?;
        Ok(OwnedBatch::with_funded_columns(batch, reservation).with_schema_reservation(schema_charge))
    }

    /// Uses the first Arrow parameter row directly when the query only projects parameters.
    pub async fn try_open_parameter_projection(&self, context: Arc<QueryContext>, arguments: &OwnedBatch, types: &[LogicalType]) -> Result<Option<QueryCursor>> {
        if !matches!(&self.0.result, PreparedResult::Query { parameter_projection: true, .. })
            || self.0.policy != ResultSignaturePolicy::Dynamic || !self.0.options.parameter_types.is_empty()
            || arguments.batch().num_rows() == 0 || types.len() != self.parameter_count() {
            return Ok(None);
        }
        let query = deep(self.source(), || self.bind(&context, types, true))?;
        query.execute_parameter_projection(context, arguments, types, Some(self.source())).await
    }

    async fn open_query_batches(&self, context: Arc<QueryContext>, mut arguments: Vec<Value>, types: &[LogicalType], columnar: bool, track: bool)
        -> Result<(QueryCursor, rtdb_types::rtdb_v1::ClaimFootprint)> {
        if arguments.len() != self.parameter_count() { return Err(parameter_error("Argument count differs from the prepared statement")); }
        arguments.iter_mut().try_for_each(rtdb_columnar::admit_execution_value)?;
        for (index, ty) in &self.0.options.parameter_types { arguments[*index] = rtdb_expr::cast(&arguments[*index], &rtdb_columnar::execution_type(ty), &context.timezone)?; }
        let context = context.reopen_sql_snapshot(&self.0.snapshot_selector, rtdb_expr::Parameters::Values(&arguments)).await?;
        let query = deep(self.source(), || self.bind(&context, types, true))?;
        if !self.notices().is_empty() && let Some(observer) = context.execution_observer() { observer.record_query_warning(); }
        let footprint = if track { query.claim_footprint(&context, &arguments) } else { Default::default() };
        if self.0.policy == ResultSignaturePolicy::Exact { self.check_signature(query.schema())?; }
        let cursor = if columnar {
            query.execute_columns_with_text(context, arguments, Some(self.source())).await?
        } else {
            query.execute_column_rows(context, arguments, Some(self.source())).await?
        };
        if self.0.policy == ResultSignaturePolicy::PostgreSql { self.check_signature(cursor.runtime_schema())?; }
        Ok((cursor, footprint))
    }

    /// Preserves absent arguments and an explicitly empty argument batch as distinct source operations.
    /// SQL argument-width validation belongs to submission and transaction resolution.
    /// Table lookup occurs during transaction resolution.
    pub fn dml_operation(&self, arguments: Option<SqlArguments>) -> Result<TxOp> {
        if !self.is_dml() { return Err(kind_error()); }
        Ok(TxOp::Sql { sql: self.0.source.clone(), args: arguments })
    }

    /// Schedules a write with access to the statement-local static plan.
    pub fn schedule_dml(&self, arguments: Option<SqlArguments>) -> Result<ScheduledOperation> {
        let PreparedResult::Dml { static_source } = &self.0.result else { return Err(kind_error()); };
        if let Some(slot) = static_source {
            let prepared = slot.get().or_else(|| {
                let prepared = PreparedDml::new(self.source(), self.0._reservation.budget())?;
                Some(slot.get_or_init(|| prepared))
            });
            if let Some(prepared) = prepared { return Ok(prepared.schedule(arguments)); }
        }
        Ok(self.dml_operation(arguments)?.into())
    }

    pub(crate) fn bind(&self, context: &Arc<QueryContext>, argument_types: &[LogicalType], execution: bool) -> Result<PreparedQuery> {
        context.check_cancelled()?;
        if self.is_dml() { return Err(kind_error()); }
        if argument_types.len() != self.parameter_count() { return Err(parameter_error("Parameter field count differs from the prepared statement")); }
        let mut span = context.start_preparation_span("xtdb.query.prepare.rebind");
        if let Some(span) = span.as_mut() { span.attribute("rebind.execution", TraceValue::Boolean(execution)); }
        let cached = self.0.plan.lock().as_ref()
            .filter(|plan| self.binds_to(&plan.options, &context.timezone, argument_types, execution))
            .and_then(|plan| plan.plan.acquire());
        if let Some(cached) = cached && let Some(query) = cached.reuse(context)? {
            if let Some(span) = span.as_mut() { span.attribute("rebind.path", TraceValue::Text("statement")); }
            return Ok(query);
        }
        let options = self.bound_options(&context.timezone, argument_types, execution);
        let source = self.source();
        let syntax = std::cell::OnceCell::new();
        let prepare = |catalog: &dyn Catalog, memory: &Arc<MemoryBudget>| {
            let cached = syntax.get_or_init(|| context.shared_plans().filter(|_| self.0.sharable).and_then(|plans| plans.read_syntax(source)));
            let parsed = match cached {
                Some(syntax) => syntax.parsed_in(memory),
                None => parse_read(context, || one_statement(source, memory))?,
            };
            bind_read(context, || parsed.bind(catalog, &options))
        };
        let mut replanned = false;
        let shared = if self.0.sharable {
            shared_plan(source, context, &options, |catalog, memory| {
                replanned = true;
                prepare(catalog, memory)
            })?
        } else { None };
        if let Some(shared) = shared {
            if let Some(span) = span.as_mut() {
                let path = if replanned { "shared_replan" } else { "shared_hit" };
                span.attribute("rebind.path", TraceValue::Text(path));
            }
            let query = shared.query.recharged(&context.memory)?;
            *self.0.plan.lock() = Some(CachedPlan { options, plan: KeptPlan::Shared(Arc::downgrade(&shared)) });
            return Ok(query);
        }
        if let Some(span) = span.as_mut() { span.attribute("rebind.path", TraceValue::Text("local")); }
        let _syntax = admit_sql_syntax(&self.0.source, &context.memory)?;
        let budget = self.0._reservation.budget();
        let plan_in = |memory: &Arc<MemoryBudget>| {
            let recording = RecordingCatalog::new(&context.catalog);
            let prepared = prepare(&recording, memory)?;
            let query = optimize_read(prepared, context, memory)?;
            Ok::<_, Anomaly>((query, recording.finish()))
        };
        let (kept, lookups) = match plan_in(budget) {
            Err(error) if error.is(AnomalyKind::ResourceExhausted) && !Arc::ptr_eq(budget, &context.memory) => {
                return Ok(plan_in(&context.memory)?.0);
            }
            result => result?,
        };
        let query = kept.recharged(&context.memory)?;
        if let Some(lookups) = lookups {
            *self.0.plan.lock() = Some(CachedPlan { options, plan: KeptPlan::Own { lookups: Arc::new(lookups), query: kept } });
        }
        Ok(query)
    }

    /// The options a query binds with: the prepared ones in `timezone`, each argument's type where none is declared,
    /// and every parameter type in its execution form for an execution.
    fn bound_options(&self, timezone: &str, argument_types: &[LogicalType], execution: bool) -> PrepareOptions {
        let mut options = self.0.options.clone();
        timezone.clone_into(&mut options.default_timezone);
        if execution { for ty in options.parameter_types.values_mut() { *ty = rtdb_columnar::execution_type(ty); } }
        for (index, ty) in argument_types.iter().enumerate() { options.parameter_types.entry(index).or_insert_with(|| if execution { rtdb_columnar::execution_type(ty) } else { ty.clone() }); }
        options
    }

    /// Whether `bound` equals `bound_options(timezone, argument_types, execution)`, without building those.
    fn binds_to(&self, bound: &PrepareOptions, timezone: &str, argument_types: &[LogicalType], execution: bool) -> bool {
        let PrepareOptions { search_path, parameter_types, default_timezone, generate_series_end_exclusive } = bound;
        let prepared = &self.0.options;
        let bind = |ty: &LogicalType| if execution { rtdb_columnar::execution_type(ty) } else { ty.clone() };
        let undeclared = (0..argument_types.len()).filter(|index| !prepared.parameter_types.contains_key(index)).count();
        *search_path == prepared.search_path && default_timezone == timezone
            && *generate_series_end_exclusive == prepared.generate_series_end_exclusive
            && prepared.parameter_types.len().checked_add(undeclared) == Some(parameter_types.len())
            && parameter_types.iter().all(|(index, bound)| match prepared.parameter_types.get(index) {
                Some(declared) => bind(declared) == *bound,
                None => argument_types.get(*index).is_some_and(|argument| bind(argument) == *bound),
            })
    }

    fn check_signature(&self, actual: &Schema) -> Result<()> {
        let PreparedResult::Query { schema, pg_types, .. } = &self.0.result else { return Err(kind_error()); };
        let compatible = match self.0.policy {
            ResultSignaturePolicy::Dynamic => true,
            ResultSignaturePolicy::Exact => schema.fields().iter().map(|field| (&field.name, &field.data_type)).eq(actual.fields().iter().map(|field| (&field.name, &field.data_type))),
            ResultSignaturePolicy::PostgreSql => {
                schema.len() == actual.len() && pg_types.iter().zip(actual.fields()).map(|(prepared, field)| prepared.accepts_result(&field.data_type)).collect::<Result<Vec<_>>>()?.into_iter().all(|compatible| compatible)
            }
        };
        if compatible { Ok(()) } else { Err(Anomaly::new(AnomalyCategory::Conflict, "xtdb/prepared-query-out-of-date", "cached plan must not change result type")) }
    }
}

/// The last plan a statement bound, and what its planning read from the catalog: a snapshot that answers those
/// lookups the same plans the statement the same way, so it reuses the plan.
struct CachedPlan { options: PrepareOptions, plan: KeptPlan<Weak<SharedPlan>> }

enum KeptPlan<Shared> {
    /// Planned by the statement and charged to it.
    Own { lookups: Arc<CatalogLookups>, query: PreparedQuery },
    /// The statement holds a weak reference, while a rebind keeps the shared entry and its charge alive.
    Shared(Shared),
}

impl KeptPlan<Weak<SharedPlan>> {
    fn acquire(&self) -> Option<KeptPlan<Arc<SharedPlan>>> {
        match self {
            Self::Own { lookups, query } => Some(KeptPlan::Own { lookups: Arc::clone(lookups), query: query.clone() }),
            Self::Shared(plan) => plan.upgrade().map(KeptPlan::Shared),
        }
    }
}

impl KeptPlan<Arc<SharedPlan>> {
    fn reuse(&self, context: &QueryContext) -> Result<Option<PreparedQuery>> {
        let (lookups, query) = match self {
            Self::Own { lookups, query } => (lookups, query),
            Self::Shared(plan) => (&plan.lookups, &plan.query),
        };
        if !lookups.replay(&context.catalog)? || !query.join_statistics_match(&context.catalog) { return Ok(None); }
        query.recharged(&context.memory).map(Some)
    }
}

fn with_read_syntax<'input, T>(
    source: &'input str, context: &QueryContext,
    parse: impl FnOnce() -> Result<ParsedStatement<'input>>,
    prepare: impl for<'a> FnOnce(Result<ParsedStatement<'a>>) -> Result<T>,
) -> Result<T> {
    let parse = || parse_read(context, parse);
    let Some(plans) = context.shared_plans() else { return prepare(parse()); };
    if let Some(syntax) = plans.read_syntax(source) {
        return prepare(Ok(syntax.parsed_in(&context.memory)));
    }
    let parsed = parse();
    let candidate = parsed.as_ref().ok().filter(|parsed| plans.can_keep_read_syntax(parsed)).cloned();
    let prepared = prepare(parsed)?;
    if let Some(candidate) = candidate { plans.remember_read_syntax(&candidate); }
    Ok(prepared)
}

fn parse_read<T>(context: &QueryContext, parse: impl FnOnce() -> Result<T>) -> Result<T> {
    let _span = context.start_preparation_span("xtdb.query.prepare.parse");
    parse()
}

fn bind_read(context: &QueryContext, bind: impl FnOnce() -> Result<PreparedStatement>) -> Result<PreparedStatement> {
    let _span = context.start_preparation_span("xtdb.query.prepare.bind");
    bind()
}

fn optimize_read(prepared: PreparedStatement, context: &QueryContext, memory: &Arc<MemoryBudget>) -> Result<PreparedQuery> {
    let _span = context.start_preparation_span("xtdb.query.prepare.plan");
    PreparedQuery::from_prepared_in(prepared, &context.catalog, memory)
}

/// The node's shared plan for `source`, planning it into the node's cache on a miss.
/// Returns `Ok(None)` when no cache is present, the statement is not a sharable query, or the plan exceeds the cache's
/// per-entry budget; every other planning error propagates directly so the caller does not plan a failing query twice.
fn shared_plan(
    source: &str, context: &QueryContext, options: &PrepareOptions,
    prepare: impl FnOnce(&dyn Catalog, &Arc<MemoryBudget>) -> Result<PreparedStatement>,
) -> Result<Option<Arc<SharedPlan>>> {
    let Some(plans) = context.shared_plans() else { return Ok(None); };
    context.check_cancelled()?;
    let PrepareOptions { search_path, parameter_types, default_timezone, generate_series_end_exclusive } = options;
    let key = SharedPlanKey {
        source: source.to_owned(), search_path: search_path.clone(), parameter_types: parameter_types.clone(),
        timezone: default_timezone.clone(), generate_series_end_exclusive: *generate_series_end_exclusive,
    };
    if let Some(shared) = plans.get(&key, &context.catalog)? { return Ok(Some(shared)); }
    let (lookups, query, inferred, notices) = match plan_for_sharing(source, context, &plans.entry_budget(), prepare) {
        Ok(Some(plan)) => plan,
        Ok(None) => return Ok(None),
        Err(error) if error.is(AnomalyKind::ResourceExhausted) => return Ok(None),
        Err(error) => return Err(error),
    };
    plans.insert(key, lookups, query, inferred, notices)
}

type SharablePlan = (Arc<CatalogLookups>, PreparedQuery, std::collections::BTreeMap<usize, LogicalType>, Vec<SqlNotice>);

fn plan_for_sharing(
    source: &str, context: &QueryContext, budget: &Arc<MemoryBudget>,
    prepare: impl FnOnce(&dyn Catalog, &Arc<MemoryBudget>) -> Result<PreparedStatement>,
) -> Result<Option<SharablePlan>> {
    let _planning = admit_sql_syntax(source, budget)?;
    let recording = RecordingCatalog::new(&context.catalog);
    let mut prepared = prepare(&recording, budget)?;
    if !matches!(&prepared.statement, Statement::Query(_)) { return Ok(None); }
    let inferred = prepared.parameter_types.clone();
    let notices = std::mem::take(&mut prepared.notices);
    let query = optimize_read(prepared, context, budget)?;
    Ok(recording.finish().map(|lookups| (Arc::new(lookups), query, inferred, notices)))
}

fn admit_source(source: &str, context: &QueryContext, options: &mut PrepareOptions) -> Result<Arc<MemoryReservation>> {
    context.check_cancelled()?;
    options.default_timezone = context.timezone.clone();
    options.generate_series_end_exclusive = context.options.generate_series_end_exclusive;
    context.memory.reserve(source.len().checked_add(256).ok_or_else(size_error)?)
}

pub(crate) async fn prepare_query_at_snapshot(
    source: &str, context: Arc<QueryContext>, options: &PrepareOptions, arguments: &[Value],
) -> Result<(PreparedQuery, Arc<QueryContext>)> {
    let _syntax = admit_sql_syntax(source, &context.memory)?;
    let memory = Arc::clone(&context.memory);
    let parsed = one_statement(source, &memory)?;
    let context = context.reopen_sql_snapshot(&parsed.snapshot_selector()?, rtdb_expr::Parameters::Values(arguments)).await?;
    let prepared = parsed.bind(&context.catalog, options)?;
    Ok((PreparedQuery::from_prepared_in(prepared, &context.catalog, &memory)?, context))
}

pub(crate) fn prepare_query(source: &str, context: &QueryContext, options: &PrepareOptions) -> Result<PreparedQuery> {
    let _syntax = admit_sql_syntax(source, &context.memory)?;
    let prepared = rtdb_sql::prepare_in(source, &context.catalog, options, &context.memory)?;
    PreparedQuery::from_prepared_in(prepared, &context.catalog, &context.memory)
}

fn admit_sql_syntax(source: &str, memory: &Arc<MemoryBudget>) -> Result<Arc<MemoryReservation>> {
    memory.reserve(rtdb_sql::statement_syntax_estimate(source.len()).ok_or_else(size_error)?)
}

fn one_statement<'input>(source: &'input str, memory: &'input Arc<MemoryBudget>) -> Result<ParsedStatement<'input>> {
    exactly_one_statement(rtdb_sql::parse_in(source, memory)?)
}

/// Statements outside the reference grammar fail there as parse errors rather than as unsupported features.
fn session_statement(source: &str, options: &PrepareOptions, memory: &Arc<MemoryBudget>) -> Anomaly {
    match rtdb_sql::parse_admitted_control_in(source, options, memory) {
        Ok(Some(_)) => Anomaly::unsupported("rtdb.node/prepared-session-statement", "Session-control statements execute through the session API"),
        Ok(None) => Anomaly::incorrect("xtdb/sql-error", "Unsupported SQL statement"),
        Err(error) => error,
    }
}

fn statement_syntax_error(source: &str, error: Anomaly, memory: &Arc<MemoryBudget>) -> Anomaly {
    match rtdb_sql::statement_syntax_diagnostic(source, &error, memory) {
        Ok(Some(diagnostic)) => diagnostic.anomaly().clone(),
        Ok(None) => error,
        Err(admission) => admission,
    }
}

fn exactly_one_statement(mut parsed: Vec<ParsedStatement<'_>>) -> Result<ParsedStatement<'_>> {
    if parsed.len() != 1 { return Err(Anomaly::incorrect("xtdb/sql-error", "A prepared statement must contain exactly one SQL statement")); }
    Ok(parsed.remove(0))
}
fn parameter_error(message: &str) -> Anomaly { Anomaly::incorrect("xtdb/parameter-count-mismatch", message) }
fn kind_error() -> Anomaly { Anomaly::incorrect("rtdb.node/statement-kind", "Prepared statement has the wrong execution kind") }
fn size_error() -> Anomaly { Anomaly::incorrect("rtdb.node/preparation-size", "Prepared statement retained size exceeds usize").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded) }
fn retained_bytes(source: &String, options: &PrepareOptions, parameters: &Vec<LogicalType>, notices: &Vec<SqlNotice>, result: &PreparedResult) -> Result<usize> {
    let mut bytes = 1024usize.checked_add(source.capacity()).and_then(|bytes| bytes.checked_add(options.default_timezone.capacity())).ok_or_else(size_error)?;
    for path in &options.search_path { bytes = bytes.checked_add(path.capacity()).and_then(|n| n.checked_add(64)).ok_or_else(size_error)?; }
    for ty in options.parameter_types.values().chain(parameters) { bytes = bytes.checked_add(ty.retained_bytes()?).and_then(|n| n.checked_add(256)).ok_or_else(size_error)?; }
    bytes = bytes.checked_add(options.search_path.capacity().checked_mul(std::mem::size_of::<String>()).ok_or_else(size_error)?).and_then(|n| n.checked_add(parameters.capacity().checked_mul(std::mem::size_of::<LogicalType>())?)).ok_or_else(size_error)?;
    bytes = bytes.checked_add(notices.capacity().checked_mul(std::mem::size_of::<SqlNotice>()).ok_or_else(size_error)?).ok_or_else(size_error)?;
    for notice in notices { bytes = bytes.checked_add(notice.message.capacity()).ok_or_else(size_error)?; }
    if let PreparedResult::Query { schema, pg_types, .. } = result {
        bytes = bytes.checked_add(pg_types.capacity().checked_mul(std::mem::size_of::<PgType>()).ok_or_else(size_error)?).ok_or_else(size_error)?;
        for field in schema.fields() { bytes = bytes.checked_add(field.data_type.retained_bytes()?).and_then(|n| n.checked_add(field.name.capacity())).and_then(|n| n.checked_add(256)).ok_or_else(size_error)?; }
    }
    Ok(bytes)
}

#[test]
fn arrow_surface_preparation_reuses_the_first_plan_across_query_budgets() {
    use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryOptions};
    use rtdb_storage::{MemoryStore, cache::BufferPool};

    let catalog = CatalogSnapshot::new("xtdb", [("xtdb".into(), DatabaseSnapshot {
        partitions: vec![],
        storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)),
    })].into_iter().collect(), false).unwrap();
    let cancellation = tokio_util::sync::CancellationToken::new();
    let statement_budget = MemoryBudget::new(1 << 20);
    let context = QueryContext::new(catalog, Arc::clone(&statement_budget), rtdb_types::Timestamp(0), "UTC", QueryOptions::default(), &cancellation).unwrap();
    let prepared = PreparedSql::prepare("SELECT 1", &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let original = prepared.0.plan.lock().as_ref().unwrap().plan.lookups_and_query().unwrap().1.relation().clone();
    let query_budget = MemoryBudget::new(1 << 20);
    let mut query_context = context;
    Arc::make_mut(&mut query_context).memory = Arc::clone(&query_budget);

    let bound = prepared.bind(&query_context, &[], true).unwrap();
    assert!(Arc::ptr_eq(bound.relation(), &original));
    assert!(query_budget.used_bytes() > 0);
    drop(bound);
    assert_eq!(query_budget.used_bytes(), 0);
    assert!(statement_budget.used_bytes() > 0);
}

#[test]
fn a_bound_plan_matches_exactly_the_options_its_arguments_bind_with() {
    use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryOptions};
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::ScalarType;

    let catalog = CatalogSnapshot::new("xtdb", [("xtdb".into(), DatabaseSnapshot {
        partitions: vec![],
        storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)),
    })].into_iter().collect(), false).unwrap();
    let cancellation = tokio_util::sync::CancellationToken::new();
    let context = QueryContext::new(catalog, MemoryBudget::new(1 << 22), rtdb_types::Timestamp(0), "UTC", QueryOptions::default(), &cancellation).unwrap();
    let (unsigned, signed, text) = (LogicalType::Scalar(ScalarType::U32), LogicalType::Scalar(ScalarType::I64), LogicalType::Scalar(ScalarType::Utf8));
    let options = PrepareOptions { parameter_types: [(1, unsigned.clone())].into_iter().collect(), ..Default::default() };
    let prepared = PreparedSql::prepare("SELECT $1 AS a, $2 AS b, $3 AS c", &context, options, ResultSignaturePolicy::Dynamic).unwrap();
    let arguments = [vec![signed.clone(), unsigned.clone(), text.clone()], vec![unsigned.clone(), signed.clone(), text.clone()],
        vec![text.clone(), unsigned.clone(), signed.clone()]];
    let mut candidates = Vec::new();
    for types in &arguments {
        for execution in [false, true] {
            for timezone in ["UTC", "Europe/Paris"] { candidates.push(prepared.bound_options(timezone, types, execution)); }
        }
    }
    let mut searching = prepared.bound_options("UTC", &arguments[0], true);
    searching.search_path.push("other".into());
    candidates.push(searching);
    let mut series = prepared.bound_options("UTC", &arguments[0], true);
    series.generate_series_end_exclusive = !series.generate_series_end_exclusive;
    candidates.push(series);
    let mut extra = prepared.bound_options("UTC", &arguments[0], true);
    extra.parameter_types.insert(3, text.clone());
    candidates.push(extra);
    for types in &arguments {
        for execution in [false, true] {
            for timezone in ["UTC", "Europe/Paris"] {
                let expected = prepared.bound_options(timezone, types, execution);
                for candidate in &candidates {
                    assert_eq!(prepared.binds_to(candidate, timezone, types, execution), *candidate == expected);
                }
            }
        }
    }
}

#[test]
fn statements_prepared_in_different_sessions_share_the_nodes_plan() {
    use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryOptions, SharedPlans};
    use rtdb_storage::{MemoryStore, cache::BufferPool};

    let node_budget = MemoryBudget::new(1 << 24);
    let plans = Arc::new(SharedPlans::new(&node_budget));
    let cancellation = tokio_util::sync::CancellationToken::new();
    let session = || {
        let catalog = CatalogSnapshot::new("xtdb", [("xtdb".into(), DatabaseSnapshot {
            partitions: vec![],
            storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)),
        })].into_iter().collect(), false).unwrap();
        QueryContext::new(catalog, MemoryBudget::new(1 << 20), rtdb_types::Timestamp(0), "UTC", QueryOptions::default(), &cancellation).unwrap()
            .with_shared_plans(Arc::clone(&plans))
    };
    let sql = "SELECT 1 AS one";
    let (first, second) = (session(), session());
    let a = PreparedSql::prepare(sql, &first, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let b = PreparedSql::prepare(sql, &second, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
    let (a, b) = (a.bind(&first, &[], true).unwrap(), b.bind(&second, &[], true).unwrap());
    assert!(Arc::ptr_eq(a.relation(), b.relation()));
    drop((a, b));
    assert!(node_budget.used_bytes() > 0);
}

#[cfg(test)]
mod shared_plan_tests {
    use super::*;
    use rtdb_columnar::read_value;
    use rtdb_index::{PartitionState, TransactionBuffer};
    use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryOptions, SharedPlans};
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::{Interval, StructFields, TableRef, Timestamp, TransactionKey, TxId};
    use std::collections::BTreeMap;

    fn put(state: &PartitionState, tx: i64, schema: &str, value: Value, memory: &Arc<MemoryBudget>) {
        let mut buffer = TransactionBuffer::new(state.value().snapshot(), TransactionKey { tx_id: TxId(tx), system_time: Timestamp(tx) }, Arc::clone(memory)).unwrap();
        buffer.put(TableRef::new(schema, "docs"), StructFields::from([("_id".into(), Value::I64(1)), ("v".into(), value)]), Interval::from_micros(0, i64::MAX).unwrap()).unwrap();
        state.apply(&buffer.stage().unwrap()).unwrap();
    }

    fn session(databases: &[(&str, &PartitionState)], default: &str, timezone: &str, plans: &Arc<SharedPlans>) -> Arc<QueryContext> {
        let snapshots = databases.iter().map(|(name, state)| ((*name).to_owned(), DatabaseSnapshot {
            partitions: vec![state.value().snapshot()],
            storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)),
        })).collect::<BTreeMap<_, _>>();
        let catalog = CatalogSnapshot::new(default, snapshots, false).unwrap();
        QueryContext::new(catalog, MemoryBudget::new(1 << 26), Timestamp(100), timezone, QueryOptions::default(), &tokio_util::sync::CancellationToken::new()).unwrap()
            .with_shared_plans(Arc::clone(plans))
    }

    async fn first_value(statement: &PreparedSql, context: &Arc<QueryContext>) -> Value {
        let mut cursor = statement.open_query(Arc::clone(context), vec![]).await.unwrap();
        let page = cursor.next_batch().await.unwrap().unwrap();
        read_value(page.batch().schema().field(0), page.batch().column(0).as_ref(), 0).unwrap()
    }

    async fn value_of(sql: &str, context: &Arc<QueryContext>, search_path: &[&str]) -> Value {
        let options = PrepareOptions { search_path: search_path.iter().map(|schema| (*schema).to_owned()).collect(), ..Default::default() };
        first_value(&PreparedSql::prepare(sql, context, options, ResultSignaturePolicy::Dynamic).unwrap(), context).await
    }

    fn plan_of(sql: &str, context: &Arc<QueryContext>) -> Arc<rtdb_plan::Relation> {
        let statement = PreparedSql::prepare(sql, context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
        Arc::clone(statement.bind(context, &[], true).unwrap().relation())
    }

    type Stages = Arc<parking_lot::Mutex<Vec<(String, Option<String>)>>>;

    struct PreparationTracer(Stages);
    struct PreparationSpan { stages: Stages, name: String, path: Option<String> }

    impl rtdb_plan::Tracer for PreparationTracer {
        fn query_enabled(&self) -> bool { true }
        fn transaction_enabled(&self) -> bool { false }
        fn start(&self, _: Option<rtdb_plan::SpanContext>, name: &str, _: &[(&str, TraceValue<'_>)]) -> Option<Box<dyn rtdb_plan::TraceSpan>> {
            if !name.starts_with("xtdb.query.prepare.") && name != "xtdb.query" { return None; }
            Some(Box::new(PreparationSpan { stages: Arc::clone(&self.0), name: name.into(), path: None }))
        }
    }

    impl rtdb_plan::TraceSpan for PreparationSpan {
        fn context(&self) -> rtdb_plan::SpanContext { rtdb_plan::SpanContext { trace_id: [1; 16], span_id: [1; 8] } }
        fn attribute(&mut self, name: &str, value: TraceValue<'_>) {
            if name == "rebind.path" && let TraceValue::Text(path) = value { self.path = Some(path.into()); }
        }
        fn end_at(&mut self, _: std::time::SystemTime) {}
    }

    impl Drop for PreparationSpan {
        fn drop(&mut self) {
            self.stages.lock().push((std::mem::take(&mut self.name), self.path.take()));
        }
    }

    #[tokio::test]
    async fn preparation_trace_separates_cold_parse_warm_rebind_and_execution() {
        let node = MemoryBudget::new(1 << 30);
        let plans = Arc::new(SharedPlans::new(&node));
        let state = PartitionState::empty(Default::default()).unwrap();
        put(&state, 1, "public", Value::I64(11), &node);
        let stages: Stages = Arc::default();
        let at = |timezone| session(&[("xtdb", &state)], "xtdb", timezone, &plans)
            .with_tracer(Some(Arc::new(PreparationTracer(Arc::clone(&stages)))), None);
        let utc = at("UTC");
        let prepare = || PreparedSql::prepare_postgres("SELECT v FROM docs", &utc, vec!["public".into()], &[]).unwrap();
        let take = || std::mem::take(&mut *stages.lock());
        let stage = |name: &str, path: Option<&str>| (format!("xtdb.query.prepare.{name}"), path.map(str::to_owned));

        let first = prepare();
        assert_eq!(take(), [stage("parse", None), stage("bind", None), stage("plan", None)]);
        let second = prepare();
        assert!(take().is_empty());
        assert_eq!(first_value(&first, &utc).await, Value::I64(11));
        assert_eq!(take(), [stage("rebind", Some("statement")), ("xtdb.query".into(), None)]);

        let paris = at("Europe/Paris");
        assert_eq!(first_value(&first, &paris).await, Value::I64(11));
        assert_eq!(take(), [
            stage("bind", None), stage("plan", None), stage("rebind", Some("shared_replan")), ("xtdb.query".into(), None),
        ]);
        assert_eq!(first_value(&second, &paris).await, Value::I64(11));
        assert_eq!(take(), [stage("rebind", Some("shared_hit")), ("xtdb.query".into(), None)]);
    }

    #[test]
    fn an_inflight_rebind_keeps_evicted_plan_metadata_charged() {
        let node = MemoryBudget::new(1 << 30);
        let plans = Arc::new(SharedPlans::new(&node));
        let state = PartitionState::empty(Default::default()).unwrap();
        let context = session(&[("xtdb", &state)], "xtdb", "UTC", &plans);
        let statement = PreparedSql::prepare("SELECT 1 AS one", &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
        let reader = statement.0.plan.lock().as_ref().unwrap().plan.acquire().unwrap();
        assert!(matches!(&reader, KeptPlan::Shared(_)));
        let charge = plans.stats().plan_charged_bytes;
        assert!(charge > 0);
        drop((context, plans));
        assert_eq!(node.used_bytes(), charge);

        drop(reader);
        assert_eq!(node.used_bytes(), 0);
        assert!(statement.0.plan.lock().as_ref().unwrap().plan.acquire().is_none());
    }

    #[test]
    fn repeated_postgres_prepare_reuses_syntax_and_only_matching_plan_options() {
        let node = MemoryBudget::new(1 << 26);
        let plans = Arc::new(SharedPlans::new(&node));
        let state = PartitionState::empty(Default::default()).unwrap();
        let source = "/* é */\nSELECT ? AS value; ";

        let prepare = |oid| {
            let context = session(&[("xtdb", &state)], "xtdb", "UTC", &plans);
            PreparedSql::prepare_postgres(source, &context, vec!["public".into()], &[oid]).unwrap()
        };
        let first = prepare(23);
        let cold = plans.stats();
        assert_eq!((cold.read_preparations, cold.plan_misses, cold.syntax_hits, cold.syntax_misses), (1, 1, 0, 1));
        assert!(cold.plan_charged_bytes > 0);
        assert!(cold.syntax_retained_bytes > 0);
        assert_eq!(node.used_bytes(), cold.plan_charged_bytes + cold.syntax_retained_bytes);

        let repeated = prepare(23);
        assert_eq!(repeated.source(), source);
        assert_eq!(repeated.postgres_types().unwrap(), &[PgType::Int4]);
        let warm = plans.stats();
        assert_eq!((warm.read_preparations, warm.plan_misses, warm.syntax_hits, warm.syntax_misses), (2, 1, 1, 1));
        assert_eq!(warm.plan_charged_bytes, cold.plan_charged_bytes);
        assert_eq!(warm.syntax_retained_bytes, cold.syntax_retained_bytes);

        let bigint = prepare(20);
        assert_eq!(bigint.postgres_types().unwrap(), &[PgType::Int8]);
        assert_eq!(first.postgres_types().unwrap(), &[PgType::Int4]);
        let changed = plans.stats();
        assert_eq!((changed.read_preparations, changed.plan_misses, changed.syntax_hits, changed.syntax_misses), (3, 2, 2, 1));
        assert_eq!(changed.syntax_retained_bytes, cold.syntax_retained_bytes);
    }

    #[test]
    fn syntax_hit_preserves_the_catalog_diagnostic_and_original_source() {
        let memory = MemoryBudget::new(1 << 26);
        let plans = Arc::new(SharedPlans::new(&memory));
        let state = PartitionState::empty(Default::default()).unwrap();
        put(&state, 1, "public", Value::I64(1), &memory);
        let initial = session(&[("xtdb", &state), ("other", &state)], "xtdb", "UTC", &plans);
        let source = "/* é */\nSELECT v FROM other.public.docs; ";
        let prepare = |context| PreparedSql::prepare(source, context, Default::default(), ResultSignaturePolicy::Dynamic);
        assert_eq!(prepare(&initial).unwrap().source(), source);

        let missing = session(&[("xtdb", &state)], "xtdb", "UTC", &plans);
        let error = prepare(&missing).err().expect("a disappeared catalog must invalidate the plan");
        let uncached = session(&[("xtdb", &state)], "xtdb", "UTC", &Arc::new(SharedPlans::new(&MemoryBudget::new(0))));
        assert_eq!(prepare(&uncached).err().unwrap(), error);
        assert_eq!(plans.stats().syntax_hits, 1);
        assert_eq!(plans.stats().plan_misses, 2);
    }

    #[test]
    fn evicted_read_syntax_stays_charged_until_its_last_reader_releases_it() {
        let node = MemoryBudget::new(1 << 26);
        let plans = Arc::new(SharedPlans::new(&node));
        let state = PartitionState::empty(Default::default()).unwrap();
        let context = session(&[("xtdb", &state)], "xtdb", "UTC", &plans);
        let source = "SELECT 1 AS one";
        plan_of(source, &context);
        let reader = plans.read_syntax(source).unwrap();
        let charge = plans.stats().syntax_retained_bytes;
        for n in 0..32 {
            plan_of(&format!("SELECT {n} AS other"), &context);
            assert!(plans.stats().syntax_retained_bytes <= (1 << 17));
        }
        assert!(plans.read_syntax(source).is_none());
        drop((context, plans));

        assert_eq!(reader.source(), source);
        assert_eq!(node.used_bytes(), charge);
        drop(reader);
        assert_eq!(node.used_bytes(), 0);
    }

    #[tokio::test]
    async fn a_session_on_another_default_database_reads_its_own_table() {
        let memory = MemoryBudget::new(1 << 26);
        let plans = Arc::new(SharedPlans::new(&MemoryBudget::new(1 << 30)));
        let (a, b) = (PartitionState::empty(Default::default()).unwrap(), PartitionState::empty(Default::default()).unwrap());
        put(&a, 1, "public", Value::I64(1), &memory);
        put(&b, 1, "public", Value::I64(2), &memory);
        let databases = [("a", &a), ("b", &b)];
        let sql = "SELECT v FROM docs";
        assert_eq!(value_of(sql, &session(&databases, "a", "UTC", &plans), &["public"]).await, Value::I64(1));
        assert_eq!(value_of(sql, &session(&databases, "b", "UTC", &plans), &["public"]).await, Value::I64(2));
        let (first, second) = (session(&databases, "a", "UTC", &plans), session(&databases, "a", "UTC", &plans));
        assert!(Arc::ptr_eq(&plan_of(sql, &first), &plan_of(sql, &second)), "sessions with the same inputs share the plan");
    }

    #[tokio::test]
    async fn a_session_with_another_search_path_reads_its_own_schema() {
        let memory = MemoryBudget::new(1 << 26);
        let plans = Arc::new(SharedPlans::new(&MemoryBudget::new(1 << 30)));
        let state = PartitionState::empty(Default::default()).unwrap();
        put(&state, 1, "public", Value::I64(1), &memory);
        put(&state, 2, "alt", Value::I64(2), &memory);
        let databases = [("xtdb", &state)];
        let sql = "SELECT v FROM docs";
        assert_eq!(value_of(sql, &session(&databases, "xtdb", "UTC", &plans), &["public"]).await, Value::I64(1));
        assert_eq!(value_of(sql, &session(&databases, "xtdb", "UTC", &plans), &["alt"]).await, Value::I64(2));
    }

    #[tokio::test]
    async fn a_session_in_another_timezone_does_not_reuse_the_plan() {
        let plans = Arc::new(SharedPlans::new(&MemoryBudget::new(1 << 30)));
        let state = PartitionState::empty(Default::default()).unwrap();
        let sql = "SELECT CAST(TIMESTAMP '2020-01-01 00:00:00' AS TIMESTAMP WITH TIME ZONE) AS t";
        let utc = value_of(sql, &session(&[("xtdb", &state)], "xtdb", "UTC", &plans), &[]).await;
        let paris = value_of(sql, &session(&[("xtdb", &state)], "xtdb", "Europe/Paris", &plans), &[]).await;
        assert_ne!(utc, paris);
    }

    #[tokio::test]
    async fn sessions_binding_other_parameter_types_get_their_own_signature() {
        let plans = Arc::new(SharedPlans::new(&MemoryBudget::new(1 << 30)));
        let state = PartitionState::empty(Default::default()).unwrap();
        let typed = |ty| PrepareOptions { parameter_types: BTreeMap::from([(0, ty)]), ..Default::default() };
        let sql = "SELECT $1 AS x";
        let int = PreparedSql::prepare(sql, &session(&[("xtdb", &state)], "xtdb", "UTC", &plans), typed(LogicalType::Scalar(rtdb_types::ScalarType::I32)), ResultSignaturePolicy::Exact).unwrap();
        let text = PreparedSql::prepare(sql, &session(&[("xtdb", &state)], "xtdb", "UTC", &plans), typed(LogicalType::Scalar(rtdb_types::ScalarType::Utf8)), ResultSignaturePolicy::Exact).unwrap();
        assert_ne!(int.prepared_schema().unwrap().fields()[0].data_type, text.prepared_schema().unwrap().fields()[0].data_type);
    }

    #[tokio::test]
    async fn a_column_type_change_replans_for_a_later_session() {
        let memory = MemoryBudget::new(1 << 26);
        let plans = Arc::new(SharedPlans::new(&MemoryBudget::new(1 << 30)));
        let state = PartitionState::empty(Default::default()).unwrap();
        put(&state, 1, "public", Value::I32(1), &memory);
        let sql = "SELECT v FROM docs";
        let before = PreparedSql::prepare(sql, &session(&[("xtdb", &state)], "xtdb", "UTC", &plans), Default::default(), ResultSignaturePolicy::Exact).unwrap();
        put(&state, 2, "public", Value::Utf8("two".into()), &memory);
        let after = session(&[("xtdb", &state)], "xtdb", "UTC", &plans);
        let replanned = PreparedSql::prepare(sql, &after, Default::default(), ResultSignaturePolicy::Exact).unwrap();
        assert_ne!(before.prepared_schema(), replanned.prepared_schema());
        assert_eq!(first_value(&replanned, &after).await, Value::Utf8("two".into()));
    }

    #[test]
    fn prepared_and_shared_plans_refresh_join_order_when_estimates_change() {
        fn first_scan(relation: &rtdb_plan::Relation) -> Option<&str> {
            if let rtdb_plan::RelationNode::Scan(scan) = relation.node() {
                return Some(&scan.table.name.table.schema);
            }
            relation.children().into_iter().find_map(|child| first_scan(child))
        }

        let memory = MemoryBudget::new(1 << 26);
        let plans = Arc::new(SharedPlans::new(&MemoryBudget::new(1 << 30)));
        let state = PartitionState::empty(Default::default()).unwrap();
        put(&state, 1, "public", Value::I64(1), &memory);
        put(&state, 2, "alt", Value::I64(1), &memory);
        put(&state, 3, "alt", Value::I64(1), &memory);
        let databases = [("xtdb", &state)];
        let before = session(&databases, "xtdb", "UTC", &plans);
        let sql = "SELECT a.v FROM public.docs a JOIN alt.docs b ON a.v = b.v";
        let explain = format!("EXPLAIN {sql}");
        let prepare = |sql: &str, context: &Arc<QueryContext>| {
            PreparedSql::prepare(sql, context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap()
        };
        let query = prepare(sql, &before);
        let explained = prepare(&explain, &before);
        for prepared in [&query, &explained] {
            let original = prepared.bind(&before, &[], true).unwrap();
            assert_eq!(first_scan(original.relation()), Some("public"));
        }

        put(&state, 4, "public", Value::I64(1), &memory);
        put(&state, 5, "public", Value::I64(1), &memory);
        let after = session(&databases, "xtdb", "UTC", &plans);
        let before_rebind = plans.stats();
        let reused = query.bind(&after, &[], true).unwrap();
        let after_rebind = plans.stats();
        assert_eq!(after_rebind.plan_misses, before_rebind.plan_misses + 1);
        assert_eq!(after_rebind.syntax_hits, before_rebind.syntax_hits + 1);
        assert_eq!(after_rebind.read_preparations, before_rebind.read_preparations);
        let new_session = prepare(sql, &after).bind(&after, &[], true).unwrap();
        assert_eq!(first_scan(new_session.relation()), Some("alt"));
        assert!(Arc::ptr_eq(reused.relation(), new_session.relation()));
        let rebound = explained.bind(&after, &[], true).unwrap();
        assert_eq!(rebound.relation(), reused.relation());

        put(&state, 6, "unrelated", Value::I64(1), &memory);
        let unrelated = session(&databases, "xtdb", "UTC", &plans);
        let unchanged = query.bind(&unrelated, &[], true).unwrap();
        assert!(Arc::ptr_eq(unchanged.relation(), reused.relation()));
    }

    #[tokio::test]
    async fn a_table_created_earlier_on_the_search_path_replans_for_a_later_session() {
        let memory = MemoryBudget::new(1 << 26);
        let plans = Arc::new(SharedPlans::new(&MemoryBudget::new(1 << 30)));
        let state = PartitionState::empty(Default::default()).unwrap();
        put(&state, 1, "public", Value::I64(1), &memory);
        let sql = "SELECT v FROM docs";
        assert_eq!(value_of(sql, &session(&[("xtdb", &state)], "xtdb", "UTC", &plans), &["alt", "public"]).await, Value::I64(1));
        put(&state, 2, "alt", Value::I64(2), &memory);
        assert_eq!(value_of(sql, &session(&[("xtdb", &state)], "xtdb", "UTC", &plans), &["alt", "public"]).await, Value::I64(2));
    }

    #[test]
    fn explain_and_dml_never_plan_into_the_shared_cache() {
        let node = MemoryBudget::new(1 << 30);
        let plans = Arc::new(SharedPlans::new(&node));
        let state = PartitionState::empty(Default::default()).unwrap();
        let context = session(&[("xtdb", &state)], "xtdb", "UTC", &plans);
        let explain = PreparedSql::prepare("EXPLAIN SELECT 1 AS one", &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
        explain.bind(&context, &[], true).unwrap();
        let dml = PreparedSql::prepare("DELETE FROM docs WHERE _id = 1", &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
        assert!(dml.bind(&context, &[], true).is_err());
        assert_eq!(node.peak_bytes(), 0);
        PreparedSql::prepare("SELECT 1 AS one", &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
        assert!(node.used_bytes() > 0, "a plain query is shared");
    }

    #[test]
    fn inline_xtql_and_over_budget_syntax_can_prepare_without_syntax_retention() {
        let state = PartitionState::empty(Default::default()).unwrap();
        let plans = Arc::new(SharedPlans::new(&MemoryBudget::new(1 << 30)));
        let context = session(&[("xtdb", &state)], "xtdb", "UTC", &plans);
        let large = format!("SELECT 1 AS n /* {} */", "x".repeat(1024));
        for source in [r#"XTQL $$(rel [{:n 1}] [n])$$"#, &large] {
            let prepared = PreparedSql::prepare(source, &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
            assert_eq!(prepared.prepared_schema().unwrap().fields()[0].name, "n");
            assert_eq!(plans.stats().syntax_retained_bytes, 0);
        }
    }


    #[test]
    fn a_full_cache_budget_evicts_old_plans_for_new_ones() {
        let node = MemoryBudget::new(1 << 30);
        let plans = Arc::new(SharedPlans::new(&node));
        let state = PartitionState::empty(Default::default()).unwrap();
        let wide = |n: usize| format!("SELECT {} AS c{n}", (0..20).map(|i| format!("{}", i + n)).collect::<Vec<_>>().join(" + "));
        for n in 0..100 {
            drop(PreparedSql::prepare(&wide(n), &session(&[("xtdb", &state)], "xtdb", "UTC", &plans), Default::default(), ResultSignaturePolicy::Dynamic).unwrap());
            assert!(node.used_bytes() <= 16 << 20);
        }
        let latest = wide(1000);
        let (first, second) = (session(&[("xtdb", &state)], "xtdb", "UTC", &plans), session(&[("xtdb", &state)], "xtdb", "UTC", &plans));
        assert!(Arc::ptr_eq(&plan_of(&latest, &first), &plan_of(&latest, &second)));
        drop((first, second));
        drop(plans);
        assert_eq!(node.used_bytes(), 0);
    }

    #[test]
    fn an_evicted_plan_stops_charging_the_cache_while_its_statement_lives_on() {
        let state = PartitionState::empty(Default::default()).unwrap();
        let sql = "SELECT 1 AS one";
        let after_eviction = |keep: bool| {
            let node = MemoryBudget::new(1 << 30);
            let plans = Arc::new(SharedPlans::new(&node));
            let context = session(&[("xtdb", &state)], "xtdb", "UTC", &plans);
            let statement = PreparedSql::prepare(sql, &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap();
            drop(statement.bind(&context, &[], true).unwrap());
            let kept = keep.then_some(statement);
            for n in 0..200 {
                drop(PreparedSql::prepare(&format!("SELECT {n} AS c"), &context, Default::default(), ResultSignaturePolicy::Dynamic).unwrap());
            }
            let used = node.used_bytes();
            if let Some(statement) = &kept {
                drop(statement.bind(&context, &[], true).unwrap());
                assert!(node.used_bytes() > used, "rebinding after eviction shares the plan again");
            }
            used
        };
        assert_eq!(after_eviction(true), after_eviction(false));
    }

    #[test]
    fn shared_preparation_preserves_a_prepare_childs_batch_offsets() {
        use rtdb_sql::{BatchExecution, ExecutableSyntax, SqlPrepareChild, parse_statement_batch};

        let state = PartitionState::empty(Default::default()).unwrap();
        let cache_memory = MemoryBudget::new(1 << 30);
        let plans = Arc::new(SharedPlans::new(&cache_memory));
        let context = session(&[("xtdb", &state)], "xtdb", "UTC", &plans);
        let source = "SELECT 'é';\nPREPARE bad AS XTQL $$(from {:table :docs :db :absent} [v])$$;";
        let mut batch = parse_statement_batch(source, &context.memory).unwrap();
        drop(batch.next_for_execution().unwrap());
        let BatchExecution::Prepare(prepare) = batch.next_for_execution().unwrap() else { panic!("expected PREPARE"); };
        let (_, SqlPrepareChild::Command { syntax: ExecutableSyntax::Sql(syntax), allocation }) =
            prepare.into_child().unwrap() else { panic!("expected a query child"); };
        let start = source.find("XTQL").unwrap();
        let end = start + syntax.source().len();

        let Err(error) = PreparedSql::prepare_postgres_statement(syntax, &context, vec!["public".into()], &[])
            else { panic!("expected database resolution to fail"); };
        assert_eq!(error.code, "xtdb/unknown-db");
        assert_eq!(error.context["position"], serde_json::json!(start + 1));
        assert_eq!(error.context["span"], serde_json::json!({"start": start, "end": end}));
        assert!(cache_memory.peak_bytes() >= rtdb_sql::statement_syntax_estimate(end - start).unwrap());
        assert_eq!(cache_memory.used_bytes(), 0);
        drop((allocation, batch));
        assert_eq!(context.memory.used_bytes(), 0);
    }

    #[test]
    fn shared_plan_propagates_planning_errors_and_declines_only_on_cache_budget() {
        let state = PartitionState::empty(Default::default()).unwrap();
        let plans = Arc::new(SharedPlans::new(&MemoryBudget::new(1 << 30)));
        let context = session(&[("xtdb", &state)], "xtdb", "UTC", &plans);
        let source = "SELECT * FROM (SELECT 1) AS a CROSS JOIN (SELECT 2) AS a";
        let options = PrepareOptions::default();
        let error = super::shared_plan(source, &context, &options, |catalog, budget| rtdb_sql::prepare_in(source, catalog, &options, budget))
            .err().expect("a binder error must propagate instead of being swallowed and re-planned");
        assert!(!error.is(AnomalyKind::ResourceExhausted));

        let tiny_node = MemoryBudget::new(1);
        let tiny_plans = Arc::new(SharedPlans::new(&tiny_node));
        let tiny_context = session(&[("xtdb", &state)], "xtdb", "UTC", &tiny_plans);
        let local = PreparedSql::prepare("SELECT regexp_replace('abc', 'a', 'x') AS v", &tiny_context, options, ResultSignaturePolicy::Dynamic).unwrap();
        assert!(matches!(&local.0.plan.lock().as_ref().unwrap().plan, KeptPlan::Own { .. }));
        assert_eq!(tiny_node.used_bytes(), 0);
        assert_eq!(tiny_plans.stats().syntax_retained_bytes, 0);
        drop(local);
        assert_eq!(tiny_context.memory.used_bytes(), 0);
    }
}

#[cfg(test)]
mod argument_cast_tests {
    use super::*;
    use arrow_array::{Array, FixedSizeListArray, Float64Array, RecordBatch, StringArray, cast::AsArray, types::{Float32Type, Int32Type}};
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use rtdb_query::{CatalogSnapshot, DatabaseSnapshot, QueryOptions};
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::{ListKind, ScalarType, Timestamp};
    use tokio_util::sync::CancellationToken;

    #[test]
    fn prepared_cast_retains_unchanged_owners_and_funds_the_one_row_replacements() {
        let memory = MemoryBudget::new(1 << 20);
        let source_memory = MemoryBudget::new(1 << 24);
        let database = DatabaseSnapshot {
            partitions: Vec::new(),
            storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)),
        };
        let catalog = CatalogSnapshot::new("xtdb", [("xtdb".into(), database)].into(), false).unwrap();
        let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(0), "UTC", QueryOptions::default(), &CancellationToken::new()).unwrap();
        let options = PrepareOptions {
            parameter_types: [
                (0, LogicalType::list(LogicalType::Scalar(ScalarType::F32))),
                (2, LogicalType::Scalar(ScalarType::I32)),
            ].into(),
            ..Default::default()
        };
        let prepared = PreparedSql::prepare(
            "SELECT $1 AS xs, $2 AS kept, $3 AS converted", &context, options, ResultSignaturePolicy::Dynamic,
        ).unwrap();
        let source_element = Arc::new(ArrowField::new("client-child", DataType::Float64, true));
        let vectors = FixedSizeListArray::try_new(source_element, 3,
            Arc::new(Float64Array::from(vec![Some(-0.0), None, Some(8.5)])), None).unwrap();
        let text = "x".repeat(2 << 20);
        let kept_field = ArrowField::new("client-text", DataType::Utf8, false)
            .with_metadata([("label".into(), "original".into())].into());
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("client-vector", vectors.data_type().clone(), false),
            kept_field.clone(),
            ArrowField::new("client-number", DataType::Utf8, false),
        ]));
        let input = RecordBatch::try_new(schema, vec![
            Arc::new(vectors),
            Arc::new(StringArray::from(vec![text.as_str()])),
            Arc::new(StringArray::from(vec!["17"])),
        ]).unwrap();
        let input = OwnedBatch::new(input, &source_memory).unwrap();
        let kept_ptr = input.batch().column(1).as_string::<i32>().value(0).as_ptr();
        let types = [
            LogicalType::List { kind: ListKind::FixedSizeList(3), element: Box::new(LogicalType::Scalar(ScalarType::F64).maybe()) },
            LogicalType::Scalar(ScalarType::Utf8),
            LogicalType::Scalar(ScalarType::Utf8),
        ];

        let before = memory.used_bytes();
        let output = prepared.cast_arrow_arguments(input, &types, &context).unwrap();
        let record = output.batch();
        assert_eq!(record.num_rows(), 1);
        assert_eq!(record.schema_ref().field(1), &kept_field);
        assert_eq!(record.schema_ref().field(0).name(), "client-vector");
        assert!(!record.schema_ref().field(0).is_nullable());
        let DataType::List(item) = record.schema_ref().field(0).data_type() else { panic!("expected variable list"); };
        assert_eq!(item.as_ref(), &ArrowField::new("$data$", DataType::Float32, true));
        let list = record.column(0).as_list::<i32>();
        assert_eq!(list.value_offsets(), &[0, 3]);
        assert_eq!(list.values().as_primitive::<Float32Type>().iter()
            .map(|cell| cell.map(f32::to_bits)).collect::<Vec<_>>(),
            [Some((-0.0f32).to_bits()), None, Some(8.5f32.to_bits())]);
        assert_eq!(record.column(2).as_primitive::<Int32Type>().value(0), 17);

        let kept = Arc::clone(record.column(1));
        let cast = Arc::clone(record.column(0));
        drop(output);
        assert_eq!(kept.as_string::<i32>().value(0).as_ptr(), kept_ptr);
        assert_eq!(kept.as_string::<i32>().value(0).len(), text.len());
        assert_eq!(cast.as_list::<i32>().value_length(0), 3);
        assert!(memory.used_bytes() > before);
        assert!(source_memory.used_bytes() > 0);
        drop(kept);
        assert_eq!(source_memory.used_bytes(), 0);
        drop(cast);
        assert_eq!(memory.used_bytes(), before);
    }
}
