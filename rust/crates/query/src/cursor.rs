use std::sync::Arc;

use arrow_array::{Array, ArrayRef, NullArray, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef};
use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch, ResultArrayFunding, copy_array_refs, copy_nullable_cells, estimate_builder_bytes_refs, estimate_copy_cell_bytes, retain_result_array, to_field};
use rtdb_plan::{BinaryOp, Catalog, ExecutionError, ExecutionSample, ExecutionTimer, Expr, Relation, RelationNode, Schema};
use rtdb_types::{ListKind, LogicalType, Result, ScalarType, Value};

use crate::{QueryContext, context::{Execution, cancelled}, invalid};
use crate::parameters::BoundParameters;
use crate::runtime_schema::{EmissionMode, EmittedRelation, RuntimeSchema, arrow_parameter_types, parameter_types};

/// A row and the lease charging it; a lease may be shared by the rows of one page, so the row records its own size.
pub(crate) struct OwnedRow { pub values: Vec<Value>, _reservation: Arc<MemoryReservation>, _bytes: usize }
impl OwnedRow {
    pub fn new(values: Vec<Value>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bytes = row_bytes(&values, values.capacity())?;
        let reservation = memory.reserve(bytes)?;
        Ok(Self { values, _reservation: reservation, _bytes: bytes })
    }
    pub fn with_reservation(values: Vec<Value>, reservation: Arc<MemoryReservation>) -> Result<Self> {
        let bytes = row_bytes(&values, values.capacity())?;
        if bytes > reservation.bytes() { return Err(invalid("row-reservation", "Decoded row exceeds its memory reservation")); }
        Ok(Self { values, _reservation: reservation, _bytes: bytes })
    }
    #[cfg(test)]
    pub fn bytes(&self) -> usize { self._bytes }
    pub fn copy_parts(parts: &[&[Value]], prefix_nulls: usize, suffix_nulls: usize, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let nulls = prefix_nulls.checked_add(suffix_nulls).ok_or_else(|| invalid("row-size", "Row copy width overflow"))?;
        let width = parts.iter().try_fold(nulls, |width, part| width.checked_add(part.len()).ok_or_else(|| invalid("row-size", "Row copy width overflow")))?;
        let base = nulls.checked_mul(std::mem::size_of::<Value>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(|| invalid("row-size", "Row copy allocation overflow"))?;
        let bound = parts.iter().flat_map(|part| part.iter()).try_fold(base, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(|| invalid("row-size", "Row copy allocation overflow")))?;
        let reservation = memory.reserve(bound)?;
        let mut values = Vec::with_capacity(width);
        values.extend((0..prefix_nulls).map(|_| Value::Null));
        for part in parts { values.extend(part.iter().cloned()); }
        values.extend((0..suffix_nulls).map(|_| Value::Null));
        Self::with_reservation(values, reservation)
    }
}
fn row_bytes(values: &[Value], capacity: usize) -> Result<usize> {
    let spare = capacity.saturating_sub(values.len()).checked_mul(std::mem::size_of::<Value>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(|| invalid("row-size", "Retained row capacity overflow"))?;
    values.iter().try_fold(spare, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(|| invalid("row-size", "Retained row size overflow")))
}

#[async_trait]
pub(crate) trait RowCursor: Send {
    async fn next(&mut self) -> Result<Option<OwnedRow>>;
    fn trace_attributes(&self, _span: &mut dyn rtdb_plan::TraceSpan) {}
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { None }
}
pub(crate) type BoxCursor = Box<dyn RowCursor>;

#[derive(Clone)]
pub struct PreparedQuery { relation: Arc<Relation>, settings: Arc<rtdb_plan::QuerySettings>, output: QueryOutput, required_parameter_count: usize, _plan_reservation: Option<Arc<MemoryReservation>> }
#[derive(Clone)]
enum QueryOutput { Rows, Plan(Arc<crate::explain::ExplainPlan>), Analyze(Schema) }

impl PreparedQuery {
    /// The same plan, its retained bytes charged to `memory` instead: a plan kept beyond one query moves its charge to its keeper.
    pub fn recharged(&self, memory: &Arc<MemoryBudget>) -> Result<Self> {
        if self._plan_reservation.as_ref().is_some_and(|reservation| Arc::ptr_eq(reservation.budget(), memory)) {
            return Ok(self.clone());
        }
        let _plan_reservation = self._plan_reservation.as_ref().map(|reservation| memory.reserve(reservation.bytes())).transpose()?;
        let output = match &self.output {
            QueryOutput::Plan(plan) => QueryOutput::Plan(plan.recharged(memory)?),
            output => output.clone(),
        };
        Ok(Self { output, _plan_reservation, ..self.clone() })
    }
    pub fn new(relation: Arc<Relation>) -> Result<Self> { Self::with_settings(relation, rtdb_plan::QuerySettings::default()) }
    /// Retains a validated relation with its existing operator shape.
    /// The supplied budget holds the plan charge until the last prepared query or cursor drops.
    /// Invalid relations, plan size overflow and budget exhaustion return an error.
    pub fn new_in(relation: Arc<Relation>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let mut query = Self::new(relation)?;
        query._plan_reservation = Some(reserve_plan(&query.relation, &query.settings, memory)?);
        Ok(query)
    }

    pub fn with_settings(relation: Arc<Relation>, settings: rtdb_plan::QuerySettings) -> Result<Self> {
        relation.validate()?;
        let relation = crate::optimize::prepare(relation)?;
        let required_parameter_count = relation.parameter_count().max(settings.parameter_count());
        Ok(Self { relation, settings: Arc::new(settings), output: QueryOutput::Rows, required_parameter_count, _plan_reservation: None })
    }
    /// Retains query metadata as well as the relation; SQL callers should use this constructor.
    pub fn from_prepared(prepared: rtdb_plan::PreparedStatement, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let lexical_parameters = prepared.parameter_types.keys().next_back().map(|index| index.checked_add(1).ok_or_else(|| invalid("parameter-count", "Prepared parameter count overflow"))).transpose()?.unwrap_or(0);
        let (relation, mode) = match prepared.statement {
            rtdb_plan::Statement::Query(relation) => (relation, None),
            rtdb_plan::Statement::Explain { relation, mode } => (relation, Some(mode)),
            _ => return Err(invalid("not-a-query", "Statement does not produce a query result")),
        };
        let plan_reservation = reserve_plan(&relation, &prepared.settings, memory)?;
        let relation = crate::optimize::prepare(relation)?;
        let output = match mode {
            None => QueryOutput::Rows,
            Some(rtdb_plan::ExplainMode::Plan) => QueryOutput::Plan(crate::explain::ExplainPlan::new(&relation, memory)?),
            Some(rtdb_plan::ExplainMode::Analyze) => QueryOutput::Analyze(rtdb_plan::explain_analyze_schema()?),
        };
        relation.validate()?;
        let required_parameter_count = lexical_parameters.max(relation.parameter_count()).max(prepared.settings.parameter_count());
        Ok(Self { relation, settings: Arc::new(prepared.settings), output, required_parameter_count, _plan_reservation: Some(plan_reservation) })
    }
    pub fn schema(&self) -> &Schema { match &self.output { QueryOutput::Rows => self.relation.schema(), QueryOutput::Plan(plan) => &plan.schema, QueryOutput::Analyze(schema) => schema } }
    pub fn relation(&self) -> &Arc<Relation> { &self.relation }
    /// Emits result fields against the pinned catalog using the parameter types in the plan.
    pub fn result_schema(&self, context: &QueryContext) -> Result<Arc<RuntimeSchema>> {
        self.emit_result_schema(context, &[])
    }
    fn emit_result_schema(&self, context: &QueryContext, parameter_fields: &[LogicalType]) -> Result<Arc<RuntimeSchema>> {
        context.check_cancelled()?;
        let emitted = EmittedRelation::emit(Arc::clone(&self.relation), &context.catalog, parameter_fields, &context.memory, EmissionMode::ArrowFields)?;
        let schema = match &self.output {
            QueryOutput::Rows => emitted.schema(),
            QueryOutput::Plan(plan) => &plan.schema,
            QueryOutput::Analyze(schema) => schema,
        };
        RuntimeSchema::fixed(schema, &context.memory)
    }
    /// Opens referenced materializations before returning; ANALYZE consumes and closes the query first.
    /// Dropping this future releases partially opened cursors and their reservations.
    pub async fn execute(&self, context: Arc<QueryContext>, parameters: Vec<Value>) -> Result<QueryCursor> {
        self.execute_with_text(context, parameters, None).await
    }
    pub async fn execute_with_text(&self, context: Arc<QueryContext>, parameters: Vec<Value>, text: Option<&str>) -> Result<QueryCursor> {
        self.open(context, parameters, text, Batching::Rows).await
    }
    /// Opens the query with one output batch for each page delivered by its root operator.
    pub async fn execute_pages(&self, context: Arc<QueryContext>, parameters: Vec<Value>) -> Result<QueryCursor> {
        self.open(context, parameters, None, Batching::Pages).await
    }
    /// Opens the query with one output batch for each column batch where the whole plan runs a column at a time,
    /// keeping each column already in the result's layout rather than copying its cells; otherwise as `execute`.
    pub async fn execute_columns(&self, context: Arc<QueryContext>, parameters: Vec<Value>) -> Result<QueryCursor> {
        self.execute_columns_with_text(context, parameters, None).await
    }

    /// Preserves the row client's batch boundaries while using Arrow operators for supported plans.
    pub async fn execute_column_rows(&self, context: Arc<QueryContext>, parameters: Vec<Value>, text: Option<&str>) -> Result<QueryCursor> {
        self.open(context, parameters, text, Batching::ColumnRows).await
    }

    pub async fn execute_columns_with_text(&self, context: Arc<QueryContext>, parameters: Vec<Value>, text: Option<&str>) -> Result<QueryCursor> {
        self.open(context, parameters, text, Batching::Columns).await
    }

    /// Opens the query with one row of a bound Arrow batch.
    pub async fn execute_arrow_columns(&self, context: Arc<QueryContext>, batch: OwnedBatch, row: usize, types: &[LogicalType], text: Option<&str>) -> Result<QueryCursor> {
        let context = context.with_plaintext();
        let retention = Arc::clone(&context);
        let open = stacker::maybe_grow(512 * 1024, 2 * 1024 * 1024, || {
            Box::pin(self.open_arrow(context, batch, row, types, text, Batching::Columns))
        });
        measured_open(retention, open).await
    }

    pub async fn execute_arrow_column_rows(&self, context: Arc<QueryContext>, batch: OwnedBatch, row: usize, types: &[LogicalType], text: Option<&str>) -> Result<QueryCursor> {
        let context = context.with_plaintext();
        let retention = Arc::clone(&context);
        let open = stacker::maybe_grow(512 * 1024, 2 * 1024 * 1024, || {
            Box::pin(self.open_arrow(context, batch, row, types, text, Batching::ColumnRows))
        });
        measured_open(retention, open).await
    }

    pub async fn execute_arrow_pages(&self, context: Arc<QueryContext>, batch: OwnedBatch, types: &[LogicalType]) -> Result<QueryCursor> {
        let context = context.with_plaintext();
        let retention = Arc::clone(&context);
        let open = stacker::maybe_grow(512 * 1024, 2 * 1024 * 1024, || {
            Box::pin(self.open_arrow(context, batch, 0, types, None, Batching::Pages))
        });
        measured_open(retention, open).await
    }

    pub fn is_parameter_projection(&self) -> bool {
        if !matches!(self.output, QueryOutput::Rows) || self.settings.as_ref() != &rtdb_plan::QuerySettings::default()
            || self.required_parameter_count == 0 { return false; }
        let mut relation = self.relation.as_ref();
        loop {
            match relation.node() {
                RelationNode::Values { rows } => return matches!(rows.as_slice(), [row] if row.iter().all(parameter_projection_expression)),
                RelationNode::Project { input, expressions } | RelationNode::Rename { input, expressions, .. }
                    if expressions.iter().all(|expression| parameter_projection_expression(&expression.expr)) => relation = input,
                _ => return false,
            }
        }
    }

    /// Opens a pure parameter projection using the bound arrays when their fields have the emitted layout.
    pub async fn execute_parameter_projection(
        &self, context: Arc<QueryContext>, arguments: &OwnedBatch, types: &[LogicalType], text: Option<&str>,
    ) -> Result<Option<QueryCursor>> {
        let context = context.with_plaintext();
        measured_open(Arc::clone(&context), Box::pin(async { self.open_parameter_projection(context, arguments, types, text) })).await
    }

    fn open_parameter_projection(
        &self, context: Arc<QueryContext>, arguments: &OwnedBatch, types: &[LogicalType], text: Option<&str>,
    ) -> Result<Option<QueryCursor>> {
        let batch = arguments.batch();
        if batch.num_rows() == 0 || batch.num_columns() != types.len() || self.required_parameter_count != types.len()
            || !self.is_parameter_projection() || !types.iter().all(direct_parameter_type) {
            return Ok(None);
        }
        let context = context.apply_settings(&self.settings, rtdb_expr::Parameters::Values(&[]))?;
        let operator = if context.tracer.as_ref().is_some_and(|tracer| tracer.query_enabled()) {
            context.operator_scope()
        } else { Arc::clone(&context) };
        let schema = self.emit_result_schema(&operator, types)?;
        let runtime_schema = EmittedRelation::derive(Arc::clone(&self.relation), &operator.catalog, types, &operator.memory)?.output();
        let mut metadata = operator.memory.reserve(crate::runtime_schema::allocation_bound(&self.relation)?)?;
        let mut fields = Vec::with_capacity(runtime_schema.schema().len());
        let mut arrays = Vec::with_capacity(runtime_schema.schema().len());
        let computed = if projection_has_computation(&self.relation) {
            let _parameter_fields = operator.memory.reserve(rtdb_format::ipc::schema_translation_bound(batch.schema_ref())?)?;
            let Some(columns) = project_bound_columns(&self.relation, arguments, &operator.memory)? else { return Ok(None); };
            Some(columns)
        } else { None };
        let mut copy_cells = batch.num_rows() > 1 || computed.is_some();
        for (position, result) in runtime_schema.schema().fields().iter().enumerate() {
            let field = to_field(&result.name, &result.data_type)?;
            let (array, source) = if let Some(columns) = &computed {
                let Some(crate::columnar::Column::Arrow { array, field, .. }) = columns.get(position) else { return Ok(None); };
                (array, field.as_ref())
            } else {
                let Some(index) = projected_parameter(&self.relation, position) else { return Ok(None); };
                let Some(array) = batch.columns().get(index) else { return Ok(None); };
                (array, batch.schema_ref().field(index))
            };
            if field.data_type() != array.data_type() || field.metadata() != source.metadata() {
                return Ok(None);
            }
            copy_cells |= array.nulls().is_some() || matches!(array.data_type(),
                DataType::Boolean | DataType::Struct(_) | DataType::List(_) | DataType::FixedSizeList(..) | DataType::Union(..));
            fields.push(field);
            arrays.push(Arc::clone(array));
        }
        let reservation = if copy_cells {
            let bound = fields.iter().zip(&arrays).try_fold(metadata.bytes(), |bytes, (field, array)| {
                bytes.checked_add(estimate_copy_cell_bytes(field, array.as_ref(), 0)?)
                    .ok_or_else(|| invalid("parameter-size", "Projected parameters exceed addressable memory"))
            })?;
            Arc::get_mut(&mut metadata).expect("projection owns its reservation").grow_to(bound)?;
            for array in &mut arrays {
                *array = copy_nullable_cells(&[array.as_ref()], &[Some((0, 0))])?;
            }
            metadata
        } else {
            for array in &mut arrays {
                *array = rtdb_columnar::retain_array(Arc::clone(array), &metadata)?;
            }
            Arc::clone(arguments.reservation())
        };
        let options = RecordBatchOptions::new().with_row_count(Some(1));
        let projected = RecordBatch::try_new_with_options(Arc::new(ArrowSchema::new(fields)), arrays, &options).map_err(|error| invalid("output-arrow", error))?;
        let projected = match OwnedBatch::with_reservation(projected, reservation) {
            Ok(batch) => batch,
            Err(error) if error.code == "rtdb/undersized-reservation" => return Ok(None),
            Err(error) => return Err(error),
        };
        let query_span = crate::trace::QueryTrace::start(&context, text);
        let parent = query_span.as_ref().map(|span| span.context());
        let operator = operator.with_tracer(context.tracer.clone(), parent);
        let operator_trace = crate::trace::operator_trace(&self.relation, &operator);
        let context = Arc::clone(&context).with_tracer(context.tracer.clone(), parent);
        let trace = crate::trace::CursorTrace::new(&context, "output", "query.cursor.output", true);
        let sample = Some(ExecutionSample::new(context.execution_observer.clone(), ExecutionTimer::Query));
        Ok(Some(QueryCursor { schema, runtime_schema, result_arrow: None, input: Some(QueryInput::Projection { batch: Some(projected), trace: operator_trace }), context,
            sample, trace, query_span, plan_reservation: self._plan_reservation.clone() }))
    }

    async fn open(&self, context: Arc<QueryContext>, parameters: Vec<Value>, text: Option<&str>, batching: Batching) -> Result<QueryCursor> {
        let context = context.with_plaintext();
        let retention = Arc::clone(&context);
        let open = stacker::maybe_grow(512 * 1024, 2 * 1024 * 1024, || {
            Box::pin(self.open_retained(context, parameters, text, batching))
        });
        measured_open(retention, open).await
    }

    async fn open_retained(&self, context: Arc<QueryContext>, mut parameters: Vec<Value>, text: Option<&str>, batching: Batching) -> Result<QueryCursor> {
        if parameters.len() < self.required_parameter_count { return Err(invalid("missing-parameter", "Query has fewer parameters than its prepared plan")); }
        let context = context.reopen_sql_snapshot(&self.settings, rtdb_expr::Parameters::Values(&parameters)).await?;
        self.check_dependencies(&context)?;
        let bound = parameters.iter().try_fold(crate::runtime_schema::allocation_bound(&self.relation)?, |bytes, value| {
            bytes.checked_add(value.retained_bytes()?.checked_mul(16).ok_or_else(|| invalid("parameter-size", "Parameter type allocation overflow"))?).ok_or_else(|| invalid("parameter-size", "Parameter type allocation overflow"))
        })?;
        let temporary = context.memory.reserve(bound)?;
        let schema = self.emit_result_schema(&context, &parameter_types(&self.relation, &parameters, EmissionMode::ArrowFields))?;
        parameters.iter_mut().try_for_each(rtdb_columnar::admit_execution_value)?;
        let context = self.expose_snapshot_token(context.apply_settings(&self.settings, rtdb_expr::Parameters::Values(&parameters))?)?;
        self.open_bound(context, schema, text, batching, move |memory| {
            let types = parameter_types(&self.relation, &parameters, EmissionMode::Execution);
            let result = BoundParameters::new(parameters, types, memory);
            drop(temporary);
            result
        }).await
    }

    async fn open_arrow(&self, context: Arc<QueryContext>, batch: OwnedBatch, row: usize, types: &[LogicalType], text: Option<&str>, batching: Batching) -> Result<QueryCursor> {
        if types.len() < self.required_parameter_count { return Err(invalid("missing-parameter", "Query has fewer parameters than its prepared plan")); }
        let bound = types.iter().try_fold(crate::runtime_schema::allocation_bound(&self.relation)?, |bytes, ty| {
            ty.retained_bytes()?.checked_mul(8).and_then(|size| bytes.checked_add(size))
                .ok_or_else(|| invalid("parameter-size", "Parameter type allocation overflow"))
        })?;
        let temporary = context.memory.reserve(bound)?;
        let execution_types = arrow_parameter_types(&self.relation, types, EmissionMode::Execution);
        let mut parameters = BoundParameters::from_arrow(batch, row, execution_types, &context.memory)?;
        let context = context.reopen_sql_snapshot(&self.settings, parameters.evaluator()).await?;
        self.check_dependencies(&context)?;
        let schema = self.emit_result_schema(&context, &arrow_parameter_types(&self.relation, types, EmissionMode::ArrowFields))?;
        drop(temporary);
        let context = self.expose_snapshot_token(context.apply_settings(&self.settings, parameters.evaluator())?)?;
        parameters.use_execution_budget(&context.memory);
        self.open_bound(context, schema, text, batching, |_| Ok(parameters)).await
    }

    fn expose_snapshot_token(&self, context: Arc<QueryContext>) -> Result<Arc<QueryContext>> {
        fn requests_token(relation: &Relation) -> bool {
            relation.expressions().into_iter().any(|expression| {
                let mut found = false;
                expression.visit(&mut |expression| {
                    found |= matches!(expression, Expr::Call { name, .. } if name == "snapshot_token");
                });
                found
            }) || relation.children().into_iter().any(|child| requests_token(child))
        }
        if context.snapshot_token().is_none() && context.snapshot_tokens.is_some() && requests_token(&self.relation)
            && let Some(token) = context.issue_snapshot_token()?
        {
            return context.with_exact_snapshot_token(token);
        }
        Ok(context)
    }

    fn check_dependencies(&self, context: &QueryContext) -> Result<()> {
        for dependency in self.relation.dependencies() {
            let Some(current) = context.catalog.table(&dependency.name)? else {
                if dependency.oid.is_none() && dependency.columns.is_empty() { continue; }
                return Err(invalid("stale-plan", "Prepared table is no longer present"));
            };
            if current.oid != dependency.oid || current.columns != dependency.columns {
                return Err(invalid("stale-plan", "Prepared table OID or column types changed; prepare against the execution snapshot"));
            }
        }
        Ok(())
    }

    async fn open_bound(&self, context: Arc<QueryContext>, schema: Arc<RuntimeSchema>, text: Option<&str>, batching: Batching,
        bind: impl FnOnce(&Arc<MemoryBudget>) -> Result<BoundParameters>) -> Result<QueryCursor> {
        let query_span = crate::trace::QueryTrace::start(&context, text);
        let context = Arc::clone(&context).with_tracer(context.tracer.clone(), query_span.as_ref().map(|span| span.context()));
        if let QueryOutput::Plan(plan) = &self.output {
            let reservation = context.memory.reserve(plan.copy_bound()?)?;
            let mut cursor = QueryCursor::from_report_rows(&plan.schema, ReportRows::Plan { plan: Arc::clone(plan), _reservation: reservation }, context, None)?;
            cursor.query_span = query_span;
            cursor.plan_reservation = self._plan_reservation.clone();
            return Ok(cursor);
        }
        let parameters = Arc::new(bind(&context.memory)?);
        let runtime_schema = EmittedRelation::derive(Arc::clone(&self.relation), &context.catalog, parameters.types(), &context.memory)?;
        let analysis = if matches!(&self.output, QueryOutput::Analyze(_)) { Some(crate::analysis::AnalysisNode::new(&self.relation, &context)?) } else { None };
        let execution = Execution { context: Arc::clone(&context), parameters, outer: crate::context::OuterRows::empty(&context.memory)?, bindings: None, analysis: analysis.clone() };
        let cancellation = context.cancellation.clone();
        let owner = context.owner_cancellation.clone();
        let operators = Arc::clone(&runtime_schema);
        let input = owner.run_until_cancelled(cancellation.run_until_cancelled(async {
            if !context.accelerators.is_empty() && !matches!(batching, Batching::Pages)
                && let Some(batches) = crate::operators::build_ordered_columns(
                    Arc::clone(&operators), execution.clone(), (0..operators.schema().len()).collect(),
                ).await?
            {
                return Ok(QueryInput::Ordered(batches));
            }
            match batching {
                Batching::Pages => {
                    if let Some(batches) = crate::operators::build_column_pages(Arc::clone(&operators), execution.clone()).await? {
                        return Ok(QueryInput::ColumnPages(batches));
                    }
                    Err(invalid("column-plan", "Semantic-page query could not open as Arrow"))
                }
                Batching::Rows | Batching::Columns | Batching::ColumnRows => match crate::operators::build_columns(Arc::clone(&operators), execution.clone()).await? {
                    Some(input) => {
                        if matches!(batching, Batching::Rows | Batching::ColumnRows) || fills_row_batches(operators.relation()) {
                            let input = Box::new(RowBatchedColumns { input, pending: None, context: Arc::clone(&context) });
                            return Ok(QueryInput::Columns(ColumnOutput { input, limit: usize::MAX, pending: None }));
                        }
                        let limit = if uses_row_batch_limit(operators.relation()) {
                            context.options.batch_rows
                        } else {
                            usize::MAX
                        };
                        Ok(QueryInput::Columns(ColumnOutput { input, limit, pending: None }))
                    }
                    None => Err(invalid("column-plan", "Query could not open as Arrow")),
                },
            }
        })).await.flatten().ok_or_else(cancelled)??;
        let sample = Some(ExecutionSample::new(context.execution_observer.clone(), ExecutionTimer::Query));
        if let (QueryOutput::Analyze(schema), Some(analysis)) = (&self.output, analysis) {
            match input {
                QueryInput::Columns(ColumnOutput { mut input, .. }) | QueryInput::ColumnPages(mut input) | QueryInput::Ordered(mut input) => {
                    while owner.run_until_cancelled(cancellation.run_until_cancelled(input.next_batch())).await.flatten().ok_or_else(cancelled)??.is_some() {
                        context.check_cancelled()?;
                    }
                }
                QueryInput::Rows { .. } | QueryInput::Projection { .. } => return Err(invalid("analysis-input", "Query analysis requires an execution cursor")),
            }
            let peak = context.memory.peak_bytes();
            let reservation = context.memory.reserve(analysis.output_bound()?)?;
            let rows = analysis.rows(peak)?;
            let mut cursor = QueryCursor::from_rows_with_sample(schema.clone(), rows, context, reservation, sample)?;
            cursor.query_span = query_span;
            cursor.plan_reservation = self._plan_reservation.clone();
            return Ok(cursor);
        }
        let trace = crate::trace::CursorTrace::new(&context, "output", "query.cursor.output", true);
        Ok(QueryCursor { schema, runtime_schema: runtime_schema.output(), result_arrow: None, input: Some(input), context, sample, trace, query_span, plan_reservation: self._plan_reservation.clone() })
    }
}

async fn measured_open<T>(context: Arc<QueryContext>, mut open: std::pin::Pin<Box<impl std::future::Future<Output = Result<T>>>>) -> Result<T> {
    let counter = context.query_cpu.clone();
    let mut task = std::pin::pin!(context.reading(async {
        match counter {
            Some(counter) => crate::query_cpu::measure(counter, open.as_mut()).await,
            None => open.await,
        }
    }));
    std::future::poll_fn(|context| {
        stacker::maybe_grow(512 * 1024, 2 * 1024 * 1024, || task.as_mut().poll(context))
    }).await
}

fn projected_parameter(mut relation: &Relation, mut position: usize) -> Option<usize> {
    loop {
        match relation.node() {
            RelationNode::Values { rows } => return match rows.first()?.get(position)? {
                Expr::Parameter { index, .. } => Some(*index),
                _ => None,
            },
            RelationNode::Project { input, expressions } | RelationNode::Rename { input, expressions, .. } => {
                match &expressions.get(position)?.expr {
                    Expr::Parameter { index, .. } => return Some(*index),
                    Expr::Column { id, .. } => position = input.schema().position(*id)?,
                    _ => return None,
                }
                relation = input;
            }
            _ => return None,
        }
    }
}

fn parameter_projection_expression(expression: &Expr) -> bool {
    stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || match expression {
        Expr::Parameter { .. } | Expr::Column { .. } => true,
        Expr::Binary { op: BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply, left, right } => {
            let operand = |expr: &Expr| matches!(expr, Expr::Literal(Value::Null | Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_) | Value::F64(_)))
                || parameter_projection_expression(expr);
            operand(left) && operand(right)
        }
        _ => crate::vector::bound_lowerable(expression),
    })
}

fn projection_has_computation(mut relation: &Relation) -> bool {
    loop {
        if relation.expressions().iter().any(|expr| !matches!(expr, Expr::Column { .. } | Expr::Parameter { .. })) { return true; }
        match relation.node() {
            RelationNode::Project { input, .. } | RelationNode::Rename { input, .. } => relation = input,
            _ => return false,
        }
    }
}

fn project_bound_columns(
    relation: &Relation, arguments: &OwnedBatch, memory: &Arc<MemoryBudget>,
) -> Result<Option<Vec<crate::columnar::Column>>> {
    use crate::columnar::{Column, ColumnBatch};

    let parameters = ColumnBatch::from_owned(arguments.slice(0, 1)?)?;
    let bound_column = |expr: &Expr, schema: &Schema, input: &[Column]| {
        if let Some(column) = crate::columnar::projection::bound_column(expr, schema, input, &parameters.columns)? {
            return Ok(Some(column));
        }
        crate::vector::bound_column(expr, schema, input, &parameters.columns, memory)
    };
    let mut layers = Vec::new();
    let mut relation = relation;
    let empty = Schema::default();
    let mut columns = Vec::new();
    let mut schema = loop {
        match relation.node() {
            RelationNode::Project { input, expressions } | RelationNode::Rename { input, expressions, .. } => {
                layers.push((relation.schema(), expressions));
                relation = input;
            }
            RelationNode::Values { rows } => {
                let [row] = rows.as_slice() else { return Ok(None); };
                for expression in row {
                    let Some(column) = bound_column(expression, &empty, &[])? else { return Ok(None); };
                    columns.push(column);
                }
                break relation.schema();
            }
            _ => return Ok(None),
        }
    };
    for (output, expressions) in layers.into_iter().rev() {
        let mut projected = Vec::with_capacity(expressions.len());
        for expression in expressions {
            let Some(column) = bound_column(&expression.expr, schema, &columns)? else { return Ok(None); };
            projected.push(column);
        }
        (schema, columns) = (output, projected);
    }
    Ok(Some(columns))
}

fn direct_parameter_type(ty: &LogicalType) -> bool {
    stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || match ty {
        LogicalType::Maybe(ty) => direct_parameter_type(ty),
        LogicalType::Struct(fields) => fields.values().all(direct_parameter_type),
        LogicalType::List { kind: ListKind::List | ListKind::FixedSizeList(_), element } => direct_parameter_type(element),
        LogicalType::Union(legs) => legs.iter().all(direct_parameter_type),
        LogicalType::Null | LogicalType::Scalar(ScalarType::Bool | ScalarType::I8 | ScalarType::I16 | ScalarType::I32 | ScalarType::I64 |
            ScalarType::F32 | ScalarType::F64 | ScalarType::Utf8 | ScalarType::Binary | ScalarType::FixedSizeBinary(_) | ScalarType::Uuid) => true,
        _ => false,
    })
}

fn reserve_plan(relation: &Relation, settings: &rtdb_plan::QuerySettings, memory: &Arc<MemoryBudget>) -> Result<Arc<MemoryReservation>> {
    let settings_bytes = settings.retained_bytes()?;
    let bytes = crate::runtime_schema::allocation_bound(relation)?.checked_mul(2).and_then(|bytes| bytes.checked_add(settings_bytes))
        .ok_or_else(|| invalid("plan-size", "Prepared plan allocation overflow"))?;
    memory.reserve(bytes)
}

enum QueryInput {
    Rows { input: ReportRows, offset: usize },
    Columns(ColumnOutput),
    ColumnPages(crate::columnar::BoxBatchCursor),
    Ordered(crate::columnar::BoxBatchCursor),
    Projection { batch: Option<OwnedBatch>, trace: Option<crate::trace::CursorTrace> },
}

fn uses_row_batch_limit(relation: &Relation) -> bool {
    use rtdb_plan::{Expr, JoinKind, RelationNode};
    stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || {
        let row_batched = match relation.node() {
            RelationNode::ColumnTable { expr: Expr::Parameter { .. }, .. }
            | RelationNode::Join { kind: JoinKind::Left | JoinKind::Right | JoinKind::Full | JoinKind::Single | JoinKind::Semi | JoinKind::Anti, .. } => true,
            RelationNode::Aggregate { groups, .. } => !groups.is_empty(),
            RelationNode::Sort { order_by, .. } | RelationNode::SortLimit { order_by, .. } => {
                order_by.len() > 1 && order_by.iter().any(|key| !matches!(key.expr, Expr::Column { .. }))
            }
            RelationNode::Project { expressions, .. } | RelationNode::Rename { expressions, .. }
            | RelationNode::Extend { expressions, .. } => expressions.iter().any(|named| {
                !crate::columnar::projection::shaped(&named.expr)
            }),
            _ => false,
        };
        row_batched || relation.children().into_iter().any(|child| uses_row_batch_limit(child))
    })
}

fn fills_row_batches(relation: &Relation) -> bool {
    use rtdb_plan::{BinaryOp, JoinKind, RelationNode};

    fn direct_join(left: &Relation, right: &Relation, expr: &Expr) -> bool {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || match expr {
            Expr::Binary { op: BinaryOp::And, left: first, right: second } =>
                direct_join(left, right, first) && direct_join(left, right, second),
            Expr::Binary { op: BinaryOp::Eq, left: first, right: second } => {
                let (Expr::Column { id: a, data_type: a_type }, Expr::Column { id: b, data_type: b_type }) =
                    (first.as_ref(), second.as_ref()) else { return false; };
                rtdb_plan::validate_equality_types(a_type, b_type).is_ok()
                    && ((left.schema().get(*a).is_some() && right.schema().get(*b).is_some())
                        || (right.schema().get(*a).is_some() && left.schema().get(*b).is_some()))
            }
            _ => false,
        })
    }

    fn uses_rows(relation: &Relation, nested: bool) -> bool {
        stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || {
            let row_batched = match relation.node() {
                RelationNode::Values { .. } | RelationNode::Apply { .. } | RelationNode::Mark { .. } | RelationNode::Aggregate { .. } => true,
                RelationNode::Let { .. } => true,
                RelationNode::Join { left, right, kind, condition: Some(condition) } if *kind != JoinKind::Cross => nested
                    || !matches!(left.node(), RelationNode::Scan(_)) || !matches!(right.node(), RelationNode::Scan(_))
                    || !direct_join(left, right, condition),
                RelationNode::Project { input, expressions } | RelationNode::Rename { input, expressions, .. }
                | RelationNode::Extend { input, expressions } => !matches!(input.node(), RelationNode::Scan(_))
                    && expressions.iter().any(|named| !crate::columnar::projection::shaped(&named.expr)),
                _ => false,
            };
            row_batched || relation.children().into_iter().any(|child| uses_rows(child, true))
        })
    }

    uses_rows(relation, false)
}

struct RowBatchedColumns {
    input: crate::columnar::BoxBatchCursor,
    pending: Option<(Arc<crate::columnar::ColumnBatch>, usize)>,
    context: Arc<QueryContext>,
}

#[async_trait::async_trait]
impl crate::columnar::BatchCursor for RowBatchedColumns {
    async fn next_batch(&mut self) -> Result<Option<crate::columnar::ColumnBatch>> {
        use crate::columnar::ColumnBatch;
        let limit = self.context.options.batch_rows;
        let memory = &self.context.memory;
        let mut sources = Vec::new();
        let mut spans = Vec::new();
        let mut rows = 0;
        let mut reservation = memory.reserve(256)?;
        while rows < limit {
            self.context.check_cancelled()?;
            let (batch, offset) = match self.pending.take() {
                Some(pending) => pending,
                None => {
                    let Some(batch) = self.input.next_batch().await? else { break; };
                    if batch.len == 0 { continue; }
                    batch.validate_readable(&self.context)?;
                    if rows == 0 && batch.len == limit { return Ok(Some(batch)); }
                    (Arc::new(batch), 0)
                }
            };
            let count = (batch.len - offset).min(limit - rows);
            if offset + count < batch.len { self.pending = Some((Arc::clone(&batch), offset + count)); }
            let bytes = (sources.len() + 1).checked_mul(256).and_then(|bytes| bytes.checked_add(256))
                .ok_or_else(|| invalid("batch-size", "Output batch inventory exceeds addressable memory"))?;
            Arc::get_mut(&mut reservation).expect("The output owns its page inventory").grow_to(bytes)?;
            sources.push(batch);
            spans.push(offset..offset + count);
            rows += count;
        }
        ColumnBatch::take_ranges(sources, &spans, rows, memory)
    }
    fn trace_attributes(&self, span: &mut dyn rtdb_plan::TraceSpan) { self.input.trace_attributes(span); }
    fn scan_counts(&self) -> Option<crate::analysis::ScanCounts> { self.input.scan_counts() }
}

struct ColumnOutput {
    input: crate::columnar::BoxBatchCursor,
    limit: usize,
    pending: Option<(OwnedBatch, usize)>,
}

type EmitColumns<'a> = dyn FnMut(&Schema, &ResultArrowSchema, &Arc<MemoryBudget>, crate::columnar::ColumnBatch) -> Result<OwnedBatch> + Send + 'a;

impl ColumnOutput {
    async fn next(&mut self, schema: &Schema, result_arrow: &mut Option<ResultArrowSchema>, memory: &Arc<MemoryBudget>, emit: &mut EmitColumns<'_>) -> Result<Option<OwnedBatch>> {
        let (batch, offset) = match self.pending.take() {
            Some(pending) => pending,
            None => loop {
                let Some(batch) = self.input.next_batch().await? else { return Ok(None); };
                if batch.len != 0 {
                    check_output_width(schema, &batch)?;
                    let arrow = ResultArrowSchema::cached(result_arrow, schema, memory)?;
                    break (emit(schema, arrow, memory, batch)?, 0);
                }
            },
        };
        let count = (batch.batch().num_rows() - offset).min(self.limit);
        if count == batch.batch().num_rows() { return Ok(Some(batch)); }
        let first = u32::try_from(offset).map_err(|_| invalid("batch-size", "Arrow transport row offset exceeds 32 bits"))?;
        let end = u32::try_from(offset + count).map_err(|_| invalid("batch-size", "Arrow transport row range exceeds 32 bits"))?;
        let _indices = memory.reserve(count.checked_mul(std::mem::size_of::<u32>())
            .ok_or_else(|| invalid("batch-size", "Arrow transport selection exceeds addressable memory"))?)?;
        let selection = rtdb_columnar::Selection::Indices((first..end).collect());
        let output = batch.select(&selection)?;
        if offset + count < batch.batch().num_rows() { self.pending = Some((batch, offset + count)); }
        Ok(Some(output))
    }
}

tokio::task_local! { static ENCODING_ORDERED_BATCH: (); }

/// An ordered scan can borrow its hosted page buffers during synchronous wire encoding.
pub fn encoding_ordered_batch() -> bool { ENCODING_ORDERED_BATCH.try_with(|_| ()).is_ok() }
#[derive(Clone, Copy)]
enum Batching { Rows, Pages, Columns, ColumnRows }

pub struct QueryCursor { schema: Arc<RuntimeSchema>, runtime_schema: Arc<RuntimeSchema>, result_arrow: Option<ResultArrowSchema>, input: Option<QueryInput>, context: Arc<QueryContext>, sample: Option<ExecutionSample>, trace: Option<crate::trace::CursorTrace>, query_span: Option<crate::trace::QueryTrace>, plan_reservation: Option<Arc<MemoryReservation>> }
impl QueryCursor {
    /// The caller reserves before allocating rows; the reservation must cover their complete retained capacities.
    /// The returned cursor owns the rows and a child cancellation scope; its parent context stays usable after cancellation.
    pub fn from_rows(schema: Schema, rows: Vec<Vec<Value>>, context: Arc<QueryContext>, reservation: Arc<MemoryReservation>) -> Result<Self> {
        Self::from_rows_with_sample(schema, rows, context, reservation, None)
    }
    fn from_rows_with_sample(schema: Schema, rows: Vec<Vec<Value>>, context: Arc<QueryContext>, reservation: Arc<MemoryReservation>, sample: Option<ExecutionSample>) -> Result<Self> {
        let retained = rows.iter().try_fold(rows.capacity().checked_mul(std::mem::size_of::<Vec<Value>>()).ok_or_else(|| invalid("row-size", "Row container size overflow"))?, |bytes, row| {
            if row.len() != schema.len() { return Err(invalid("row-width", "Owned row differs from declared schema")); }
            bytes.checked_add(row_bytes(row, row.capacity())?).ok_or_else(|| invalid("row-size", "Owned row size overflow"))
        })?;
        if retained > reservation.bytes() { return Err(invalid("row-reservation", "Owned rows exceed their memory reservation")); }
        Self::from_report_rows(&schema, ReportRows::Owned { rows: rows.into_iter(), _reservation: reservation }, context, sample)
    }
    fn from_report_rows(schema: &Schema, input: ReportRows, context: Arc<QueryContext>, sample: Option<ExecutionSample>) -> Result<Self> {
        let runtime_schema = RuntimeSchema::fixed(schema, &context.memory)?;
        let sample = sample.or_else(|| Some(ExecutionSample::new(context.execution_observer.clone(), ExecutionTimer::Query)));
        Ok(Self { schema: Arc::clone(&runtime_schema), runtime_schema, result_arrow: None, input: Some(QueryInput::Rows { input, offset: 0 }), context: context.execution_child().with_plaintext(), sample, trace: None, query_span: None, plan_reservation: None })
    }
    pub fn schema(&self) -> &Schema { self.schema.schema() }
    pub fn context(&self) -> &Arc<QueryContext> { &self.context }
    /// Batch fields derived from this execution's pinned catalog, before any rows are read.
    /// The advertised result fields remain available through `schema`.
    pub fn runtime_schema(&self) -> &Schema { self.runtime_schema.schema() }
    pub(crate) fn retained_runtime_schema(&self) -> Arc<RuntimeSchema> { Arc::clone(&self.runtime_schema) }
    pub fn cancel(&mut self) { self.context.cancellation.cancel(); self.finish_trace(); self.sample = None; }
    /// Encodes one batch before returning to the transport. The encoder must
    /// return detached wire data and must not retain the borrowed Arrow buffers.
    pub async fn encode_next_batch<T>(&mut self, encode: impl FnOnce(&OwnedBatch) -> T) -> Result<Option<T>> {
        let answer = if matches!(self.input, Some(QueryInput::Ordered(_))) {
            ENCODING_ORDERED_BATCH.scope((), async {
                self.next_batch().await.map(|batch| batch.as_ref().map(encode))
            }).await?
        } else {
            self.next_batch().await?.as_ref().map(encode)
        };
        self.delivery_grant().await?;
        Ok(answer)
    }

    /// Rechecks the units behind batches retained by a caller after a pull or encoding.
    pub fn delivery_grant(&self) -> impl Future<Output = Result<rtdb_storage::sealing::PlaintextDelivery>> + Send + use<> {
        let context = Arc::clone(&self.context);
        async move { context.delivery_grant().await }
    }

    pub async fn next_batch(&mut self) -> Result<Option<OwnedBatch>> {
        self.next_batch_with(&mut cached_column_batch).await
    }

    #[doc(hidden)]
    pub async fn next_spatial_probe_batch<P: Send>(
        &mut self,
        mut plan: impl FnMut(&ArrowField, SpatialProbeCells<'_>, &Arc<MemoryBudget>) -> Option<P> + Send,
    ) -> Result<Option<(OwnedBatch, Option<SpatialProbeFallback<P>>)>> {
        if !matches!(&self.input, Some(QueryInput::Columns(output)) if output.limit == usize::MAX && output.pending.is_none()) {
            return Ok(self.next_batch().await?.map(|batch| (batch, None)));
        }
        let runtime = Arc::clone(&self.runtime_schema);
        let mut fallback = None;
        let batch = self.next_batch_with(&mut |schema, arrow, memory, input| {
            let bytes = schema.len().checked_mul(std::mem::size_of::<(usize, P)>()).and_then(|bytes| bytes.checked_add(512));
            let Some(charge) = bytes.and_then(|bytes| memory.reserve(bytes).ok()) else {
                return cached_column_batch(schema, arrow, memory, input);
            };
            let mut replacements = Vec::with_capacity(schema.len());
            for (index, field) in arrow.schema.fields().iter().enumerate() {
                let cells = SpatialProbeCells { column: &input.columns[index], logical: &schema.fields()[index].data_type };
                if let Some(plan) = plan(field, cells, memory) {
                    replacements.push((index, plan));
                }
            }
            if replacements.is_empty() {
                drop(charge);
                return cached_column_batch(schema, arrow, memory, input);
            }
            let skip = |index| replacements.binary_search_by_key(&index, |(index, _)| *index).is_ok();
            let output = match (|| {
                let deferred = ResultArrowSchema::new(schema, 0..schema.len(), &skip, memory)?;
                emit_columns(schema, &deferred, memory, &input, (0..schema.len()).map(|index| (index, index)), &skip, false)
            })() {
                Err(error) if error.code == "rtdb/memory-budget-exceeded" => {
                    drop(replacements);
                    drop(charge);
                    return cached_column_batch(schema, arrow, memory, input);
                }
                result => result?,
            };
            fallback = Some(SpatialProbeFallback {
                input, runtime: Arc::clone(&runtime), plans: replacements, _charge: charge,
            });
            Ok(output)
        }).await?;
        Ok(batch.map(|batch| (batch, fallback)))
    }

    async fn next_batch_with(&mut self, emit: &mut EmitColumns<'_>) -> Result<Option<OwnedBatch>> {
        if let Err(error) = self.context.check_cancelled() { self.release_input(); self.record_error(); return Err(error); }
        let token = self.context.cancellation.clone();
        let owner = self.context.owner_cancellation.clone();
        if let Some(trace) = &mut self.trace { trace.start(); }
        let started = std::time::Instant::now();
        let retention = self.context.clone();
        let counter = self.context.query_cpu.clone();
        let pull = async {
            let mut pull = std::pin::pin!(self.pull_batch(emit));
            match counter {
                Some(counter) => owner.run_until_cancelled(token.run_until_cancelled(crate::query_cpu::measure(counter, pull.as_mut()))).await,
                None => owner.run_until_cancelled(token.run_until_cancelled(pull)).await,
            }
        };
        let result = retention.reading(async { pull.await.flatten().ok_or_else(cancelled)? }).await;
        let result = result.and_then(|batch| {
            if let Some(id) = self.context.catalog.owned_identity() {
                if let Some(page) = &batch {
                    for row in 0..page.batch().num_rows() {
                        crate::owned_row_revision(page.batch(), row, id, &self.context.memory)?;
                    }
                }
            }
            self.context.catalog.check_view_lease()?;
            self.context.check_cancelled()?;
            Ok(batch)
        });
        match result {
            Ok(batch) => { if let Some(trace) = &mut self.trace { trace.completed(started.elapsed(), batch.as_ref().map(|batch| batch.batch().num_rows())); } if batch.is_none() { self.release_input(); } Ok(batch) }
            Err(error) => { self.release_input(); self.record_error(); Err(error) }
        }
    }
    fn record_error(&self) { if let Some(observer) = &self.context.execution_observer { observer.record_error(ExecutionError::Query); } }
    /// Records an error while consuming a returned batch, such as row encoding or transport writing.
    /// Do not call this for `next_batch` failures, which the cursor already records.
    pub fn record_consumer_error(&self) { self.record_error(); }
    fn release_input(&mut self) {
        self.input = None;
        self.result_arrow = None;
    }
    fn finish_trace(&mut self) {
        self.release_input();
        self.trace = None;
        self.query_span = None;
    }
    async fn pull_batch(&mut self, emit: &mut EmitColumns<'_>) -> Result<Option<OwnedBatch>> {
        let Some(input) = &mut self.input else { return Ok(None); };
        let rows = match input {
            QueryInput::Projection { batch, trace } => {
                let started = std::time::Instant::now();
                if let Some(trace) = trace { trace.start(); }
                let batch = batch.take();
                if let Some(trace) = trace { trace.completed(started.elapsed(), batch.as_ref().map(|batch| batch.batch().num_rows())); }
                return Ok(batch);
            }
            QueryInput::ColumnPages(input) => {
                let Some(batch) = input.next_batch().await? else { return Ok(None); };
                batch.validate_readable(&self.context)?;
                let schema = self.runtime_schema.schema();
                check_output_width(schema, &batch)?;
                let arrow = ResultArrowSchema::cached(&mut self.result_arrow, schema, &self.context.memory)?;
                return cached_column_batch(schema, arrow, &self.context.memory, batch).map(Some);
            }
            QueryInput::Columns(output) => {
                return output.next(self.runtime_schema.schema(), &mut self.result_arrow, &self.context.memory, emit).await;
            }
            QueryInput::Ordered(input) => loop {
                let Some(batch) = input.next_batch().await? else { return Ok(None); };
                if batch.len != 0 {
                    let schema = self.runtime_schema.schema();
                    check_output_width(schema, &batch)?;
                    let arrow = ResultArrowSchema::cached(&mut self.result_arrow, schema, &self.context.memory)?;
                    return cached_column_batch(schema, arrow, &self.context.memory, batch).map(Some);
                }
            },
            QueryInput::Rows { input, offset } => {
                let limit = self.context.options.batch_rows;
                match input {
                    ReportRows::Owned { rows, .. } => &rows.as_slice()[..rows.len().min(limit)],
                    ReportRows::Plan { plan, .. } => {
                        let rows = plan.rows();
                        let end = offset.saturating_add(limit).min(rows.len());
                        let page = &rows[*offset..end];
                        *offset = end;
                        page
                    }
                }
            }
        };
        if rows.is_empty() { return Ok(None); }
        let schema = self.runtime_schema.schema();
        let mut bound = 4096usize;
        for (column, field) in schema.fields().iter().enumerate() {
            bound = bound.checked_add(estimate_builder_bytes_refs(&field.data_type, rows.iter().map(|row| &row[column]))?).ok_or_else(|| invalid("batch-size", "Output batch allocation bound overflow"))?;
        }
        let reservation = self.context.memory.reserve(bound)?;
        let arrow = ResultArrowSchema::cached(&mut self.result_arrow, schema, &self.context.memory)?;
        let arrays: Vec<ArrayRef> = schema.fields().iter().enumerate().map(|(column, field)| {
            copy_array_refs(&field.data_type, &rows.iter().map(|row| &row[column]).collect::<Vec<_>>())
        }).collect::<Result<_>>()?;
        let count = rows.len();
        if let QueryInput::Rows { input: ReportRows::Owned { rows, .. }, .. } = input {
            rows.by_ref().take(count).for_each(drop);
        }
        let options = RecordBatchOptions::new().with_row_count(Some(count));
        let batch = RecordBatch::try_new_with_options(Arc::clone(&arrow.schema), arrays, &options).map_err(|error| invalid("output-arrow", error))?;
        Ok(Some(OwnedBatch::with_reservation(batch, reservation)?.with_schema_reservation(Arc::clone(&arrow.reservation))))
    }
}

struct ResultArrowSchema {
    schema: SchemaRef,
    reservation: Arc<MemoryReservation>,
}

impl ResultArrowSchema {
    fn new(
        logical: &Schema, indices: impl Iterator<Item = usize> + Clone, defer: &impl Fn(usize) -> bool,
        memory: &Arc<MemoryBudget>,
    ) -> Result<Self> {
        let overflow = || invalid("batch-size", "Output schema allocation bound overflow");
        let bound = indices.clone().try_fold(256usize, |bytes, index| {
            let field = &logical.fields()[index];
            let ty = if defer(index) { 0 } else { field.data_type.retained_bytes()? };
            ty.checked_add(field.name.len()).and_then(|size| size.checked_mul(8))
                .and_then(|size| size.checked_add(1024))
                .and_then(|size| size.checked_add(bytes)).ok_or_else(overflow)
        })?;
        let mut reservation = memory.reserve(bound)?;
        let fields = indices.map(|index| {
            let field = &logical.fields()[index];
            if defer(index) { Ok(ArrowField::new(&field.name, DataType::Null, true)) }
            else { to_field(&field.name, &field.data_type) }
        }).collect::<Result<Vec<_>>>()?;
        let schema = Arc::new(ArrowSchema::new(fields));
        let bytes = schema.fields().iter().try_fold(256usize, |bytes, field| {
            field.size().checked_add(32).and_then(|size| size.checked_add(bytes)).ok_or_else(overflow)
        })?;
        Arc::get_mut(&mut reservation).expect("Result schema construction owns its reservation").shrink_to(bytes)?;
        Ok(Self { schema, reservation })
    }

    fn cached<'a>(
        cache: &'a mut Option<Self>, logical: &Schema, memory: &Arc<MemoryBudget>,
    ) -> Result<&'a Self> {
        if cache.is_none() {
            *cache = Some(Self::new(logical, 0..logical.len(), &|_| false, memory)?);
        }
        Ok(cache.as_ref().expect("Result schema is initialized"))
    }
}

fn check_output_width(schema: &Schema, batch: &crate::columnar::ColumnBatch) -> Result<()> {
    if batch.columns.len() != schema.len() { return Err(invalid("row-width", "Operator output differs from its declared schema")); }
    Ok(())
}

pub(crate) fn column_batch(schema: &Schema, memory: &Arc<MemoryBudget>, batch: crate::columnar::ColumnBatch) -> Result<OwnedBatch> {
    check_output_width(schema, &batch)?;
    let arrow = ResultArrowSchema::new(schema, 0..schema.len(), &|_| false, memory)?;
    cached_column_batch(schema, &arrow, memory, batch)
}

fn cached_column_batch(
    schema: &Schema, arrow: &ResultArrowSchema, memory: &Arc<MemoryBudget>, batch: crate::columnar::ColumnBatch,
) -> Result<OwnedBatch> {
    emit_columns(schema, arrow, memory, &batch, (0..schema.len()).map(|index| (index, index)), &|_| false, false)
}

fn emit_columns(
    schema: &Schema, arrow: &ResultArrowSchema, memory: &Arc<MemoryBudget>, batch: &crate::columnar::ColumnBatch,
    indices: impl Iterator<Item = (usize, usize)>, defer: &impl Fn(usize) -> bool, selected_probe: bool,
) -> Result<OwnedBatch> {
    use crate::columnar::Column;

    let overflow = || invalid("batch-size", "Output batch allocation bound overflow");
    let fields = arrow.schema.fields();
    let bytes = fields.len().checked_mul(std::mem::size_of::<ArrayRef>())
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<OwnedBatch>() + 256)).ok_or_else(overflow)?;
    let mut reservation = memory.reserve(bytes)?;
    let page = Arc::get_mut(&mut reservation).expect("Output batch construction owns its reservation");
    let mut arrays = Vec::with_capacity(fields.len());
    for ((index, source), field) in indices.zip(fields) {
        let column = &batch.columns[source];
        let logical = &schema.fields()[index].data_type;
        let layout = field.as_ref();
        let (array, scratch, funding) = if defer(index) {
            let scratch = memory.reserve(256)?;
            (Arc::new(NullArray::new(batch.len)) as ArrayRef, scratch, ResultArrayFunding::Copy)
        } else {
            if let Column::Arrow { logical: Some(_), .. } = column {
                let bound = layout.size().checked_mul(4).and_then(|bytes| bytes.checked_add(512)).ok_or_else(overflow)?;
                let scratch = memory.reserve(bound)?;
                if let Some(array) = crate::columnar::conform(column, logical, layout) {
                    let funding = ResultArrayFunding::Shared(batch.column_reservation(source));
                    arrays.push(retain_result_array(array, scratch, funding, page, &arrow.reservation)?);
                    continue;
                }
            }
            let relayout = if selected_probe {
                crate::columnar::relayout_spatial_probe_output(column, logical, layout, memory)
            } else {
                crate::columnar::relayout_output(column, logical, layout, memory)
            };
            if let Some((array, scratch)) = relayout? {
                let funding = match column {
                    Column::Arrow { array, .. } => ResultArrayFunding::Relayout(array, batch.column_reservation(source)),
                    _ => ResultArrayFunding::Copy,
                };
                (array, scratch, funding)
            } else if let Some((array, scratch)) = crate::columnar::float_vector::relayout(column, batch.len, layout, memory)? {
                (array, scratch, ResultArrayFunding::Copy)
            } else {
                let (array, scratch) = rtdb_columnar::copy_evaluated_cells(logical, batch.len, memory,
                    |row| column.bound(row), |row| column.value(row))?;
                (array, scratch, ResultArrayFunding::Copy)
            }
        };
        arrays.push(retain_result_array(array, scratch, funding, page, &arrow.reservation)?);
    }
    let options = RecordBatchOptions::new().with_row_count(Some(batch.len));
    let output = RecordBatch::try_new_with_options(Arc::clone(&arrow.schema), arrays, &options).map_err(|error| invalid("output-arrow", error))?;
    Ok(OwnedBatch::with_funded_columns(output, reservation).with_schema_reservation(Arc::clone(&arrow.reservation)))
}

#[doc(hidden)]
pub struct SpatialProbeCells<'a> {
    column: &'a crate::columnar::Column,
    logical: &'a LogicalType,
}

impl<'a> SpatialProbeCells<'a> {
    pub fn cell(&self, row: usize) -> Option<Option<(rtdb_columnar::ArrowCell<'a>, &'a LogicalType)>> {
        match self.column.arrow_cell(row)? {
            Some((array, field, logical, row)) => Some(Some((rtdb_columnar::ArrowCell { field, array, row }, logical?))),
            None => Some(None),
        }
    }

    pub fn layouts(&self) -> Option<impl Iterator<Item = (&'a dyn Array, &'a ArrowField, Option<&'a LogicalType>)>> {
        use crate::columnar::Column;
        let (single, selected) = match self.column {
            Column::Arrow { array, field, logical } if self.logical.nullable() || !array.is_nullable() =>
                (Some((array.as_ref(), field.as_ref(), logical.as_ref())), None),
            Column::Selected(selected) if self.logical.nullable() => (None, Some(selected)),
            _ => return None,
        };
        Some(single.into_iter().chain(selected.into_iter().flat_map(|selected| selected.source_layouts())))
    }

    pub fn preserves_layout(&self, array: &dyn Array, field: &ArrowField, logical: &LogicalType, target: &ArrowField) -> bool {
        *logical != LogicalType::Nothing && rtdb_columnar::execution_type(logical) == *logical
            && rtdb_columnar::from_field(field).is_ok_and(|actual| actual == *logical)
            && array.data_type() == field.data_type()
            && ((field.data_type() == target.data_type() && field.metadata() == target.metadata()
                && (!field.is_nullable() || target.is_nullable())
                && (logical == self.logical || matches!(self.logical, LogicalType::Maybe(inner) if logical == inner.as_ref())))
                || matching_spatial_union_leg(target, self.logical, field, logical))
    }
}

fn matching_spatial_union_leg(target: &ArrowField, logical: &LogicalType, source: &ArrowField, source_type: &LogicalType) -> bool {
    let (DataType::Union(fields, arrow_schema::UnionMode::Dense), LogicalType::Union(legs)) = (target.data_type(), logical)
    else { return false; };
    let source_type = match source_type { LogicalType::Maybe(inner) => inner.as_ref(), other => other };
    if !legs.contains(source_type) || source.is_nullable() { return false; }
    let Ok(name) = rtdb_columnar::leg_name(source_type) else { return false; };
    fields.iter().any(|(_, field)| {
        *field.name() == name && field.data_type() == source.data_type() && field.metadata() == source.metadata()
    })
}

#[doc(hidden)]
pub struct SpatialProbeFallback<P> {
    input: crate::columnar::ColumnBatch,
    runtime: Arc<RuntimeSchema>,
    plans: Vec<(usize, P)>,
    _charge: Arc<MemoryReservation>,
}

impl<P> SpatialProbeFallback<P> {
    pub fn plan(&self, index: usize) -> Option<(&P, SpatialProbeCells<'_>)> {
        let (_, plan) = &self.plans[self.plans.binary_search_by_key(&index, |(index, _)| *index).ok()?];
        let logical = &self.runtime.schema().fields()[index].data_type;
        Some((plan, SpatialProbeCells { column: &self.input.columns[index], logical }))
    }

    /// The row positions address the returned page directly when the second value is true.
    pub fn project_rows(&self, indices: &[usize], rows: &[usize], memory: &Arc<MemoryBudget>) -> Result<(OwnedBatch, bool)> {
        let schema = self.runtime.schema();
        check_output_width(schema, &self.input)?;
        let arrow = ResultArrowSchema::new(schema, indices.iter().copied(), &|_| false, memory)?;
        let original = indices.iter().copied().map(|index| (index, index));
        if rows.iter().copied().eq(0..self.input.len) {
            return emit_columns(schema, &arrow, memory, &self.input, original, &|_| false, false).map(|page| (page, true));
        }
        let can_select = indices.iter().zip(arrow.schema.fields()).all(|(&index, field)| {
            if self.plan(index).is_some() {
                return true;
            }
            let declared = &schema.fields()[index].data_type;
            crate::columnar::conform(&self.input.columns[index], declared, field).is_some()
                || SpatialProbeCells { column: &self.input.columns[index], logical: declared }
                    .layouts().is_some()
        });
        if !can_select {
            return emit_columns(schema, &arrow, memory, &self.input, original, &|_| false, false).map(|page| (page, false));
        }
        let _positions = memory.reserve(size_of_val(rows) * 2)?;
        let picks = rows.iter().map(|&row| (0, row)).collect::<Vec<_>>();
        let selected = crate::columnar::ColumnBatch::take_rows_with_custody(&[&self.input], &picks, indices, true, memory)?;
        let selected_indices = indices.iter().copied().zip(0..indices.len());
        emit_columns(schema, &arrow, memory, &selected, selected_indices, &|_| false, true).map(|page| (page, true))
    }
}
enum ReportRows {
    Owned { rows: std::vec::IntoIter<Vec<Value>>, _reservation: Arc<MemoryReservation> },
    Plan { plan: Arc<crate::explain::ExplainPlan>, _reservation: Arc<MemoryReservation> },
}
impl Drop for QueryCursor { fn drop(&mut self) { self.context.cancellation.cancel(); self.finish_trace(); } }

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_columnar::copy_array;

    #[test]
    fn result_arrays_keep_their_projected_source_grants() -> Result<()> {
        use arrow_array::{Int64Array, ListArray};
        use arrow_buffer::OffsetBuffer;
        use crate::columnar::{Column, ColumnBatch};

        let memory = MemoryBudget::new(220 * 1024);
        let rows = 8192;
        let integer = LogicalType::Scalar(ScalarType::I64);
        let list = LogicalType::list(integer.clone());
        let scalar_field = Arc::new(to_field("scalar", &integer)?);
        let list_field = Arc::new(to_field("list", &list)?);
        let DataType::List(item) = list_field.data_type() else { unreachable!(); };
        let scalar = Int64Array::from(vec![11; rows]);
        let child = Int64Array::from(vec![23; rows]);
        let scalar_address = scalar.values().as_ptr();
        let child_address = child.values().as_ptr();
        let nested = ListArray::try_new(Arc::clone(item), OffsetBuffer::from_lengths(std::iter::repeat_n(1, rows)),
            Arc::new(child), None).unwrap();
        let scalar = Column::Arrow { array: Arc::new(scalar), field: scalar_field, logical: Some(integer.clone()) };
        let mut input = ColumnBatch::settled(vec![scalar], rows, memory.reserve(80 * 1024)?)?;
        let nested = Column::Arrow { array: Arc::new(nested), field: list_field, logical: Some(list.clone()) };
        input.append_column(nested, memory.reserve(112 * 1024)?, &memory)?;
        let scalar_charge = Arc::downgrade(input.column_reservation(0));
        let list_charge = Arc::downgrade(input.column_reservation(1));
        let projected = input.duplicate_for_replay(&memory)?;
        drop(input);

        let schema = Schema::new([("scalar", integer), ("list", list)].into_iter().enumerate().map(|(index, (name, data_type))| {
            rtdb_plan::Field { id: rtdb_plan::ColumnId(index as u32), name: name.into(), qualifier: None,
                qualified_only: false, unnamed: false, data_type }
        }).collect())?;
        let output = column_batch(&schema, &memory, projected)?;
        let scalar = Arc::clone(output.batch().column(0));
        let list = output.batch().column(1).as_any().downcast_ref::<ListArray>().unwrap();
        let child = Arc::clone(list.values());
        drop(output);
        assert!(scalar_charge.upgrade().is_some());
        assert!(list_charge.upgrade().is_some());
        let values = |array: &ArrayRef| array.as_any().downcast_ref::<Int64Array>().unwrap().values().as_ptr();
        assert_eq!(values(&scalar), scalar_address);
        assert_eq!(values(&child), child_address);
        drop(scalar);
        assert!(scalar_charge.upgrade().is_none());
        assert!(list_charge.upgrade().is_some());
        drop(child);
        assert!(list_charge.upgrade().is_none());
        assert_eq!(memory.used_bytes(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn columns_and_ordered_pages_share_a_funded_zero_field_schema() -> Result<()> {
        use crate::columnar::{BatchCursor, ColumnBatch};
        use std::collections::BTreeMap;
        use rtdb_types::Timestamp;

        struct Pages(std::vec::IntoIter<ColumnBatch>);
        #[async_trait]
        impl BatchCursor for Pages {
            async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> { Ok(self.0.next()) }
        }

        let memory = MemoryBudget::new(1 << 20);
        let database = crate::DatabaseSnapshot {
            partitions: Vec::new(),
            storage: Arc::new(rtdb_storage::cache::BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
        };
        let catalog = crate::CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false)?;
        let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(10), "UTC", crate::QueryOptions::default(),
            &tokio_util::sync::CancellationToken::new())?;
        for ordered in [false, true] {
            let runtime = RuntimeSchema::fixed(&Schema::new(vec![])?, &memory)?;
            let pages = [2, 3].into_iter().map(|rows| ColumnBatch::settled(vec![], rows, memory.reserve(256)?))
                .collect::<Result<Vec<_>>>()?;
            let pages = Box::new(Pages(pages.into_iter()));
            let input = if ordered { QueryInput::Ordered(pages) }
                else { QueryInput::Columns(ColumnOutput { input: pages, limit: usize::MAX, pending: None }) };
            let mut cursor = QueryCursor {
                schema: Arc::clone(&runtime), runtime_schema: runtime, result_arrow: None, input: Some(input),
                context: context.execution_child(), sample: None, trace: None, query_span: None, plan_reservation: None,
            };
            let first = cursor.next_batch().await?.unwrap();
            let second = cursor.next_batch().await?.unwrap();
            assert_eq!(first.batch().num_rows(), 2);
            assert_eq!(second.batch().num_rows(), 3);
            assert_eq!(first.batch().num_columns(), 0);
            assert!(Arc::ptr_eq(first.batch().schema_ref(), second.batch().schema_ref()));
            let charge = Arc::downgrade(&cursor.result_arrow.as_ref().unwrap().reservation);
            let sliced = second.slice(1, 1)?;
            drop((cursor, first, second));
            assert!(charge.upgrade().is_some());
            assert!(sliced.owns_schema());
            drop(sliced);
            assert!(charge.upgrade().is_none());
        }
        Ok(())
    }

    #[tokio::test]
    async fn spatial_probe_defers_covered_columns_and_keeps_uncovered_read_errors() -> Result<()> {
        use std::collections::BTreeMap;
        use rtdb_types::{StructFields, Timestamp};

        let memory = MemoryBudget::new(1 << 20);
        let database = crate::DatabaseSnapshot {
            partitions: Vec::new(),
            storage: Arc::new(rtdb_storage::cache::BufferPool::new(Arc::new(rtdb_storage::MemoryStore::default()), 1 << 20)),
        };
        let catalog = crate::CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false)?;
        let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(10), "UTC", crate::QueryOptions::default(),
            &tokio_util::sync::CancellationToken::new())?;
        let boolean = LogicalType::Scalar(ScalarType::Bool);
        let wrap = |element| LogicalType::Struct(StructFields::from([("v2_value".into(), LogicalType::list(element))]));
        let covered = wrap(boolean);
        let bottom = wrap(LogicalType::Nothing);
        let value = |item| Value::Struct(StructFields::from([("v2_value".into(), Value::list(vec![item]))]));
        let expected = value(Value::Bool(true));
        let schema = Schema::new(vec![rtdb_plan::Field {
            id: rtdb_plan::ColumnId(0), name: "c_fact".into(), qualifier: None,
            qualified_only: false, unnamed: false, data_type: covered.clone().maybe(),
        }])?;
        let runtime = RuntimeSchema::fixed(&schema, &memory)?;
        let open = |logical, cell| -> Result<QueryCursor> {
            let field = to_field("c_fact", &logical)?;
            let array = rtdb_columnar::build_array(&logical, &[cell])?;
            let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(vec![field])), vec![array]).unwrap();
            let batch = crate::columnar::ColumnBatch::from_owned(OwnedBatch::new(batch, &memory)?)?;
            let input = crate::columnar::single_batch(Some(batch), Arc::clone(&context));
            Ok(QueryCursor {
                schema: Arc::clone(&runtime), runtime_schema: Arc::clone(&runtime), result_arrow: None,
                input: Some(QueryInput::Columns(ColumnOutput { input, limit: usize::MAX, pending: None })),
                context: context.execution_child(), sample: None, trace: None, query_span: None, plan_reservation: None,
            })
        };
        let plan = |target: &ArrowField, cells: SpatialProbeCells<'_>, _: &Arc<MemoryBudget>| {
            cells.layouts()?.all(|(array, field, ty)| ty.is_some_and(|ty| cells.preserves_layout(array, field, ty, target)))
                .then_some(())
        };

        let (page, deferred) = open(covered, expected.clone())?.next_spatial_probe_batch(plan).await?.unwrap();
        assert_eq!(page.batch().column(0).data_type(), &DataType::Null);
        let deferred = deferred.unwrap();
        let (_, cells) = deferred.plan(0).unwrap();
        let (cell, _) = cells.cell(0).unwrap().unwrap();
        assert_eq!(rtdb_columnar::read_value(cell.field, cell.array, cell.row)?, expected);
        let (restored, compact) = deferred.project_rows(&[0], &[0, 0], &memory)?;
        assert!(compact);
        assert_eq!(restored.batch().num_rows(), 2);
        assert_eq!(rtdb_columnar::read_value(restored.batch().schema_ref().field(0), restored.batch().column(0).as_ref(), 0)?, expected);
        assert_eq!(rtdb_columnar::read_value(restored.batch().schema_ref().field(0), restored.batch().column(0).as_ref(), 1)?, expected);

        let value = value(Value::Undefined);
        let error = open(bottom.clone(), value.clone())?.next_batch().await.unwrap_err();
        assert_eq!(error.code, "xtdb/zero-leg-read");
        let error = open(bottom, value)?.next_spatial_probe_batch(plan).await.err().unwrap();
        assert_eq!(error.code, "xtdb/zero-leg-read");
        Ok(())
    }

    #[test]
    fn query_output_relayouts_nested_union_children_without_copying_values() {
        use arrow_array::{Array, Int64Array, StructArray, UnionArray};
        use arrow_schema::DataType;
        use rtdb_types::{ScalarType, StructFields};

        let memory = MemoryBudget::new(1 << 22);
        let integer = LogicalType::Scalar(ScalarType::I64);
        let source = LogicalType::Struct(StructFields::from([("a".into(), integer.clone())]));
        let target = LogicalType::from_legs([
            LogicalType::list(integer.clone()),
            LogicalType::Struct(StructFields::from([("a".into(), integer.clone()), ("b".into(), integer.maybe())])),
            LogicalType::Null,
        ]);
        let values = [Value::Struct(StructFields::from([("a".into(), Value::I64(5))]))];
        let column = rtdb_columnar::copy_array(&source, &values).unwrap();
        let array = column.as_any().downcast_ref::<StructArray>().unwrap();
        let original = array.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().as_ptr();
        let field = Arc::new(to_field("v", &source).unwrap());
        let input = crate::columnar::ColumnBatch::settled(vec![crate::columnar::Column::Arrow {
            array: column, field, logical: Some(source),
        }], 1, memory.reserve(4096).unwrap()).unwrap();
        let source_charge = Arc::downgrade(input.column_reservation(0));
        let schema = Schema::new(vec![rtdb_plan::Field {
            id: rtdb_plan::ColumnId(0), name: "v".into(), qualifier: None,
            qualified_only: false, unnamed: false, data_type: target,
        }]).unwrap();

        let output = column_batch(&schema, &memory, input).unwrap();

        let retained = Arc::clone(output.batch().column(0));
        drop(output);
        let array = retained.as_any().downcast_ref::<UnionArray>().unwrap();
        let DataType::Union(fields, _) = array.data_type() else { panic!("Expected the declared union"); };
        let (id, _) = fields.iter().find(|(_, field)| field.name() == "struct").unwrap();
        assert_eq!(array.type_id(0), id);
        let child = array.child(id).as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(child.column(0).as_any().downcast_ref::<Int64Array>().unwrap().values().as_ptr(), original);
        assert!(child.column(1).is_null(0));
        assert!(source_charge.upgrade().is_some());
        let nulls = Arc::clone(child.column(1));
        drop(retained);
        assert!(source_charge.upgrade().is_none());
        assert!(memory.used_bytes() > 0);
        assert!(nulls.is_null(0));
        drop(nulls);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn spatial_probe_relayouts_only_selected_union_rows_under_a_small_grant() -> Result<()> {
        let source_memory = MemoryBudget::new(4 << 20);
        let selected_memory = MemoryBudget::new(64 << 10);
        let text = LogicalType::Scalar(ScalarType::Utf8);
        let target = LogicalType::from_legs([text.clone(), LogicalType::Scalar(ScalarType::I64)]);
        let small = Value::Utf8("selected".into());
        let values = [Value::Utf8("x".repeat(256 << 10)), small.clone()];
        let array = copy_array(&text, &values)?;
        let batch = RecordBatch::try_new(Arc::new(ArrowSchema::new(vec![to_field("v", &text)?])), vec![array]).unwrap();
        let source = crate::columnar::ColumnBatch::from_owned(OwnedBatch::new(batch, &source_memory)?)?;
        let picked = crate::columnar::ColumnBatch::take_rows_with_custody(
            &[&source], &[(0, 1), (0, 1)], &[0], true, &selected_memory,
        )?;
        let field = to_field("v", &target)?;
        let (array, _reservation) = crate::columnar::relayout_spatial_probe_output(
            &picked.columns[0], &target, &field, &selected_memory,
        )?.expect("selected rows fit under the narrow grant");
        assert_eq!(array.len(), 2);
        for row in 0..2 {
            assert_eq!(rtdb_columnar::read_value(&field, array.as_ref(), row)?, small);
        }
        Ok(())
    }

    #[test]
    fn query_output_gathers_only_selected_float_union_components_into_plain_lists() {
        use arrow_array::{Array, FixedSizeListArray, Float32Array, ListArray};
        use rtdb_types::{ListKind, ScalarType, Value};

        for fixed in [false, true] {
            let memory = MemoryBudget::new(1 << 22);
            let f32_ty = LogicalType::Scalar(ScalarType::F32);
            let source_element = LogicalType::from_legs([f32_ty.clone(), LogicalType::Scalar(ScalarType::F64), LogicalType::Null]);
            let kind = if fixed { ListKind::FixedSizeList(3) } else { ListKind::List };
            let source = LogicalType::List { kind: kind.clone(), element: Box::new(source_element) }.maybe();
            let target = LogicalType::List { kind, element: Box::new(f32_ty.maybe()) }.maybe();
            let nan = f32::from_bits(0x7fc0_1234);
            let list = |values: Vec<Value>| if fixed { Value::FixedSizeList(values.into()) } else { Value::list(values) };
            let values = [list(vec![Value::F64(123.0); 3]), Value::Null, list(vec![Value::F32(-0.0), Value::Null, Value::F32(nan)])];
            let array = copy_array(&source, &values).unwrap();
            let input = RecordBatch::try_new(Arc::new(ArrowSchema::new(vec![to_field("v", &source).unwrap()])), vec![array]).unwrap();
            let input = crate::columnar::ColumnBatch::from_owned(OwnedBatch::new(input, &memory).unwrap()).unwrap();
            let picked = crate::columnar::ColumnBatch::take_rows_with_custody(&[&input], &[(0, 2), (0, 1)], &[0], true, &memory).unwrap();
            let field = to_field("v", &target).unwrap();
            let typed = crate::columnar::float_vector::relayout(&picked.columns[0], picked.len, &field, &memory).unwrap();
            assert!(typed.is_some());
            drop(typed);
            let schema = Schema::new(vec![rtdb_plan::Field {
                id: rtdb_plan::ColumnId(0), name: "v".into(), qualifier: None,
                qualified_only: false, unnamed: false, data_type: target,
            }]).unwrap();

            let output = column_batch(&schema, &memory, picked).unwrap();
            let array = output.batch().column(0);
            assert_eq!(array.data_type(), field.data_type());
            assert!(array.is_null(1));
            let components = if fixed {
                array.as_any().downcast_ref::<FixedSizeListArray>().unwrap().values()
            } else {
                array.as_any().downcast_ref::<ListArray>().unwrap().values()
            }.as_any().downcast_ref::<Float32Array>().unwrap();
            assert_eq!(components.value(0).to_bits(), (-0.0f32).to_bits());
            assert!(components.is_null(1));
            assert_eq!(components.value(2).to_bits(), nan.to_bits());
        }
    }


    #[test]
    fn mixed_result_union_gathers_vectors_and_keeps_declared_scalar_legs() {
        use arrow_array::{Array, UnionArray};
        use rtdb_types::{ScalarType, Value};

        let memory = MemoryBudget::new(1 << 20);
        let float = LogicalType::Scalar(ScalarType::F32);
        let source = LogicalType::from_legs([
            LogicalType::list(LogicalType::from_legs([float.clone(), LogicalType::Scalar(ScalarType::F64)])),
            LogicalType::Scalar(ScalarType::Utf8),
            LogicalType::Null,
        ]);
        let target = LogicalType::from_legs([
            LogicalType::Null,
            LogicalType::Scalar(ScalarType::Utf8),
            LogicalType::list(float),
        ]);
        let rows = [Value::Utf8("s".into()), Value::list(vec![Value::F32(-0.0f32)]), Value::Null];
        let source_field = Arc::new(to_field("v", &source).unwrap());
        let array = copy_array(&source, &rows).unwrap();
        let column = crate::columnar::Column::Arrow { array, field: source_field, logical: Some(source) };
        let field = to_field("v", &target).unwrap();
        let (typed, _scratch) = crate::columnar::float_vector::relayout(&column, rows.len(), &field, &memory).unwrap().unwrap();
        let typed = typed.as_any().downcast_ref::<UnionArray>().unwrap();
        assert_eq!(typed.data_type(), field.data_type());
        assert_eq!(typed.type_ids().as_ref(), &[1, 2, 0]);
        assert_eq!(typed.to_data(), copy_array(&target, &rows).unwrap().to_data());
    }

    #[test]
    fn document_result_gathers_vectors_beside_a_selected_keyword_alias() {
        use arrow_array::{Array, StringArray, UnionArray};
        use arrow_schema::UnionFields;

        let memory = MemoryBudget::new(1 << 20);
        let keyword = LogicalType::Scalar(ScalarType::Keyword);
        let float = LogicalType::Scalar(ScalarType::F32);
        let list = LogicalType::list(LogicalType::from_legs([float.clone(), LogicalType::Scalar(ScalarType::F64)]));
        let target = LogicalType::from_legs([keyword.clone(), LogicalType::list(float)]);
        let vector = Value::list(vec![Value::F32(-0.0), Value::F32(f32::from_bits(0x7fc0_1234))]);
        let rows = [vector, Value::Keyword(rtdb_types::Keyword::from_name("doc/tag"))];
        let fields = UnionFields::try_new([0, 1, 2], [
            to_field("list", &list).unwrap(),
            arrow_schema::Field::new("keyword", DataType::Utf8, false),
            to_field("unused", &keyword).unwrap(),
        ]).unwrap();
        let array = Arc::new(UnionArray::try_new(fields, vec![0_i8, 1].into(), Some(vec![0_i32, 1].into()), vec![
            copy_array(&list, &rows[..1]).unwrap(),
            Arc::new(StringArray::from(vec!["unselected", "doc/tag"])),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ]).unwrap());
        let field = Arc::new(arrow_schema::Field::new("v", array.data_type().clone(), false));
        let logical = rtdb_columnar::from_field(&field).unwrap();
        let array_bytes = array.get_array_memory_size();
        let column = crate::columnar::Column::Arrow { array, field, logical: Some(logical) };
        let declared = to_field("v", &target).unwrap();
        let expected = copy_array(&target, &rows).unwrap();
        let (typed, custody) = crate::columnar::float_vector::relayout(&column, rows.len(), &declared, &memory).unwrap().unwrap();
        assert_eq!(typed.to_data(), expected.to_data());
        assert_eq!(custody.bytes(), typed.get_array_memory_size());
        drop((typed, custody));

        let input = crate::columnar::ColumnBatch::settled(vec![column], 2, memory.reserve(array_bytes + 4096).unwrap()).unwrap();
        let plain = crate::columnar::ColumnBatch::settled(vec![crate::columnar::Column::Arrow {
            array: copy_array(&keyword, &rows[1..]).unwrap(),
            field: Arc::new(to_field("v", &keyword).unwrap()),
            logical: Some(keyword),
        }], 1, memory.reserve(4096).unwrap()).unwrap();
        let picked = crate::columnar::ColumnBatch::take_rows_with_custody(&[&input, &plain], &[(0, 0), (1, 0)], &[0], true, &memory).unwrap();
        let schema = Schema::new(vec![rtdb_plan::Field {
            id: rtdb_plan::ColumnId(0), name: "v".into(), qualifier: None,
            qualified_only: false, unnamed: false, data_type: target,
        }]).unwrap();
        assert!(picked.physical_record(&schema, &memory).unwrap().is_none());
        let output = column_batch(&schema, &memory, picked).unwrap();
        assert_eq!(output.batch().column(0).to_data(), expected.to_data());
        drop((input, plain, output));
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn result_conformance_expands_only_selected_run_end_float_components() {
        use arrow_array::{Array, RunArray, make_array, new_empty_array, types::Int16Type};
        use arrow_buffer::ScalarBuffer;

        let memory = MemoryBudget::new(1 << 20);
        let float = LogicalType::Scalar(ScalarType::F32);
        let run_type = LogicalType::RunEndEncoded { width: rtdb_types::RunEndWidth::I16, value: Box::new(float.clone().maybe()) };
        let runs = [Value::F32(-0.0), Value::F32(-0.0), Value::Null, Value::F32(f32::from_bits(0x7fc0_9552))];
        let elements = copy_array(&run_type, &runs).unwrap();
        assert_eq!(elements.as_any().downcast_ref::<RunArray<Int16Type>>().unwrap().values().len(), 3);
        let items = Arc::new(to_field("item", &run_type).unwrap());
        let array = make_array(new_empty_array(&DataType::List(items)).to_data().into_builder().len(2)
            .buffers(vec![ScalarBuffer::from(vec![0_i32, 1, 4]).into_inner()])
            .child_data(vec![elements.to_data()]).build().unwrap());
        let field = Arc::new(arrow_schema::Field::new("v", array.data_type().clone(), false));
        let column = crate::columnar::Column::Arrow { array, logical: Some(rtdb_columnar::from_field(&field).unwrap()), field };
        let target = LogicalType::list(float.maybe());
        let field = to_field("v", &target).unwrap();
        let (output, _custody) = crate::columnar::float_vector::relayout(&column, 2, &field, &memory).unwrap().unwrap();
        let expected = copy_array(&target, &[Value::list(runs[..1].to_vec()), Value::list(runs[1..].to_vec())]).unwrap();
        assert_eq!(output.to_data(), expected.to_data());
    }

    #[test]
    fn mixed_result_gathers_a_named_set_leg_and_float_components_without_scalar_rows() {
        use arrow_array::{Array, UnionArray};
        use arrow_schema::UnionFields;

        let memory = MemoryBudget::new(1 << 20);
        let text = LogicalType::Scalar(ScalarType::Utf8);
        let list = LogicalType::list(text.clone());
        let set = LogicalType::List { kind: ListKind::Set, element: Box::new(text) };
        let f32_list = LogicalType::list(LogicalType::Scalar(ScalarType::F32));
        let fields = UnionFields::try_new([0, 1, 2], [
            to_field("set", &list).unwrap(),
            to_field("unused", &set).unwrap(),
            to_field("floats", &f32_list).unwrap(),
        ]).unwrap();
        let elements = vec![Value::Utf8("zed".into()), Value::Utf8("a".into()), Value::Utf8("zed".into())];
        let vector = Value::list(vec![Value::F32(-0.0)]);
        let array = Arc::new(UnionArray::try_new(fields, vec![0_i8, 2].into(), Some(vec![0_i32, 0].into()), vec![
            copy_array(&list, &[Value::list(elements.clone())]).unwrap(), copy_array(&set, &[]).unwrap(),
            copy_array(&f32_list, std::slice::from_ref(&vector)).unwrap(),
        ]).unwrap());
        let field = Arc::new(arrow_schema::Field::new("v", array.data_type().clone(), false));
        let column = crate::columnar::Column::Arrow { array, logical: Some(rtdb_columnar::from_field(&field).unwrap()), field };
        let target = LogicalType::from_legs([f32_list, set]);
        let field = to_field("v", &target).unwrap();
        let (output, _custody) = crate::columnar::float_vector::relayout(&column, 2, &field, &memory).unwrap().unwrap();
        assert_eq!(output.to_data(), copy_array(&target, &[Value::Set(elements), vector]).unwrap().to_data());
    }

    #[test]
    fn incompatible_result_component_width_keeps_the_column_type_error() {
        let input_type = LogicalType::list(LogicalType::Scalar(ScalarType::F64));
        let result_type = LogicalType::list(LogicalType::Scalar(ScalarType::F32));
        let memory = MemoryBudget::new(1 << 20);
        let input = crate::columnar::ColumnBatch::settled(vec![crate::columnar::Column::Arrow {
            array: copy_array(&input_type, &[Value::list(vec![Value::F64(3.25)])]).unwrap(),
            field: Arc::new(to_field("v", &input_type).unwrap()),
            logical: Some(input_type),
        }], 1, memory.reserve(4096).unwrap()).unwrap();
        let schema = Schema::new(vec![rtdb_plan::Field {
            id: rtdb_plan::ColumnId(0), name: "v".into(), qualifier: None,
            qualified_only: false, unnamed: false, data_type: result_type,
        }]).unwrap();
        let Err(error) = column_batch(&schema, &memory, input) else { panic!("Conformance coerced the active width"); };
        assert_eq!(error.code, "rtdb/column-type-mismatch");
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn typed_result_writes_a_packed_component_null_under_its_numeric_tag() {
        use arrow_array::{Array, Float32Array, Float64Array, NullArray, UnionArray, make_array, new_empty_array};
        use arrow_array::cast::AsArray;
        use arrow_buffer::{NullBuffer, ScalarBuffer};

        let float = LogicalType::Scalar(ScalarType::F32);
        let double = LogicalType::Scalar(ScalarType::F64);
        let ty = LogicalType::list(LogicalType::from_legs([float, double, LogicalType::Null]));
        let field = Arc::new(to_field("v", &ty).unwrap());
        let DataType::List(item) = field.data_type() else { unreachable!(); };
        let DataType::Union(fields, _) = item.data_type() else { unreachable!(); };
        let bits = 0x8000_0000;
        let values = UnionArray::try_new(fields.clone(), vec![0_i8, 1].into(), Some(vec![0_i32, 0].into()), vec![
            Arc::new(Float32Array::new(vec![f32::from_bits(bits)].into(), Some(NullBuffer::from(vec![false])))),
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(NullArray::new(0)),
        ]).unwrap();
        let array = make_array(new_empty_array(field.data_type()).to_data().into_builder().len(1)
            .buffers(vec![ScalarBuffer::from(vec![0_i32, 2]).into_inner()])
            .child_data(vec![values.to_data()]).build().unwrap());
        let expected = copy_array(&ty, &[rtdb_columnar::read_value(&field, array.as_ref(), 0).unwrap()]).unwrap();
        let column = crate::columnar::Column::Arrow { array, field: Arc::clone(&field), logical: Some(ty) };
        let memory = MemoryBudget::new(1 << 20);
        let (output, _custody) = crate::columnar::float_vector::relayout(&column, 1, &field, &memory).unwrap().unwrap();
        let output = output.as_list::<i32>().values().as_union();
        let expected = expected.as_list::<i32>().values().as_union();
        assert_eq!(output.type_ids(), expected.type_ids());
        assert_eq!(output.type_id(0), 0);
        let values = output.child(0).as_primitive::<arrow_array::types::Float32Type>();
        assert!(values.is_null(0));
        assert_eq!(values.value(0).to_bits(), expected.child(0).as_primitive::<arrow_array::types::Float32Type>().value(0).to_bits());
    }

}
