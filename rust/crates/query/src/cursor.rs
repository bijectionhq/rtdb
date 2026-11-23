use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch, copy_array, estimate_builder_bytes_refs, to_field};
use rtdb_plan::{Catalog, ExecutionError, ExecutionSample, ExecutionTimer, Relation, Schema};
use rtdb_types::{LogicalType, Result, Value};

use crate::{QueryContext, context::{Execution, cancelled}, invalid};
use crate::runtime_schema::{EmissionMode, EmittedRelation, RuntimeSchema, parameter_types};

/// A row and the lease charging it; a lease may be shared by the rows of one page, so the row records its own size.
pub(crate) struct OwnedRow { pub values: Vec<Value>, reservation: Arc<MemoryReservation>, bytes: usize }
impl OwnedRow {
    pub fn new(values: Vec<Value>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bytes = row_bytes(&values, values.capacity())?;
        let reservation = memory.reserve(bytes)?;
        Ok(Self { values, reservation, bytes })
    }
    pub fn with_reservation(values: Vec<Value>, reservation: Arc<MemoryReservation>) -> Result<Self> {
        let bytes = row_bytes(&values, values.capacity())?;
        if bytes > reservation.bytes() { return Err(invalid("row-reservation", "Decoded row exceeds its memory reservation")); }
        Ok(Self { values, reservation, bytes })
    }
    pub fn bytes(&self) -> usize { self.bytes }
    pub fn replace_value(&mut self, index: usize, value: Value, memory: &Arc<MemoryBudget>) -> Result<()> {
        let previous = self.values.get(index).ok_or_else(|| invalid("row-index", "Output column is outside its row"))?;
        let value_bytes = value.retained_bytes()?;
        let bytes = self.bytes().checked_sub(previous.retained_bytes()?).and_then(|bytes| bytes.checked_add(value_bytes))
            .ok_or_else(|| invalid("row-size", "Replacement row exceeds addressable memory"))?;
        let reservation = memory.reserve(bytes)?;
        self.values[index] = value;
        self.reservation = reservation;
        self.bytes = bytes;
        Ok(())
    }
    /// As `copy_parts`, charged from a slab.
    pub fn copy_parts_in(parts: &[&[Value]], prefix_nulls: usize, suffix_nulls: usize, slab: &mut RowSlab) -> Result<Self> {
        let nulls = prefix_nulls.checked_add(suffix_nulls).ok_or_else(|| invalid("row-size", "Row copy width overflow"))?;
        let width = parts.iter().try_fold(nulls, |width, part| width.checked_add(part.len()).ok_or_else(|| invalid("row-size", "Row copy width overflow")))?;
        let base = nulls.checked_mul(std::mem::size_of::<Value>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(|| invalid("row-size", "Row copy allocation overflow"))?;
        let bound = parts.iter().flat_map(|part| part.iter()).try_fold(base, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(|| invalid("row-size", "Row copy allocation overflow")))?;
        let reservation = slab.charge(bound)?;
        let mut values = Vec::with_capacity(width);
        values.extend((0..prefix_nulls).map(|_| Value::Null));
        for part in parts { values.extend(part.iter().cloned()); }
        values.extend((0..suffix_nulls).map(|_| Value::Null));
        let bytes = row_bytes(&values, values.capacity())?;
        if bytes > bound { return Err(invalid("row-reservation", "Copied row exceeds its memory reservation")); }
        Ok(Self { values, reservation, bytes })
    }
    /// A row of owned values, charged from a slab for the size `row_bytes` reports, as `replace_value` charges a computed cell.
    pub fn in_slab(values: Vec<Value>, slab: &mut RowSlab) -> Result<Self> {
        let bytes = row_bytes(&values, values.capacity())?;
        let reservation = slab.charge(bytes)?;
        Ok(Self { values, reservation, bytes })
    }
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
/// Charges rows from chunks of the budget, so building many rows admits memory once per chunk rather than once per row.
/// Each row keeps the chunk it was charged from; a chunk is released when its last row drops.
/// A row the budget can hold is admitted even when a whole chunk is not, by charging it alone.
pub(crate) struct RowSlab { memory: Arc<MemoryBudget>, current: Option<Arc<MemoryReservation>>, remaining: usize }
impl RowSlab {
    const CHUNK: usize = 32 * 1024;
    pub fn new(memory: &Arc<MemoryBudget>) -> Self { Self { memory: Arc::clone(memory), current: None, remaining: 0 } }
    pub fn charge(&mut self, bytes: usize) -> Result<Arc<MemoryReservation>> {
        match &self.current {
            Some(current) if bytes <= self.remaining => { self.remaining -= bytes; Ok(Arc::clone(current)) }
            _ => {
                let (reservation, chunk) = match self.memory.reserve(bytes.max(Self::CHUNK)) {
                    Ok(reservation) => (reservation, bytes.max(Self::CHUNK)),
                    Err(_) => (self.memory.reserve(bytes)?, bytes),
                };
                self.remaining = chunk - bytes;
                self.current = Some(Arc::clone(&reservation));
                Ok(reservation)
            }
        }
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

fn uses_search_score(relation: &Relation) -> bool {
    for expression in relation.expressions() {
        let mut found = false;
        expression.visit(&mut |expr| {
            found |= matches!(expr, rtdb_plan::Expr::Call { name, .. } if name.eq_ignore_ascii_case("search_score"));
        });
        if found { return true; }
    }
    relation.children().into_iter().any(|child| uses_search_score(child))
}

impl PreparedQuery {
    /// The same plan, its retained bytes charged to `memory` instead: a plan kept beyond one query moves its charge to its keeper.
    pub fn recharged(&self, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let _plan_reservation = self._plan_reservation.as_ref().map(|reservation| memory.reserve(reservation.bytes())).transpose()?;
        Ok(Self { _plan_reservation, ..self.clone() })
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
        self.open(context, parameters, None, Batching::Columns).await
    }

    pub async fn execute_arrow(&self, context: Arc<QueryContext>, batch: OwnedBatch, text: Option<&str>, casts: &std::collections::BTreeMap<usize, LogicalType>) -> Result<QueryCursor> {
        let mut mask = if self.settings.parameter_count() == 0 && context.tracer.is_none() && matches!(self.output, QueryOutput::Rows) {
            crate::cosine::packed_parameters(&self.relation, batch.batch().num_columns())
        } else { vec![false; batch.batch().num_columns()] };
        for (index, (keep, array)) in mask.iter_mut().zip(batch.batch().columns()).enumerate() {
            *keep &= !casts.contains_key(&index) && crate::cosine::fixed_float(array.as_ref()) && array.null_count() == 0;
        }
        let (mut parameters, _scratch) = crate::parameters::decode_row(&batch, &mask, &context.memory)?;
        parameters.iter_mut().for_each(rtdb_columnar::admit_execution_value);
        for (index, ty) in casts {
            let parameter = parameters.get_mut(*index).ok_or_else(|| invalid("parameter-width", "Arrow parameter cast is outside the bound row"))?;
            *parameter = rtdb_expr::cast(parameter, &rtdb_columnar::execution_type(ty), &context.timezone)?;
        }
        let arrow = mask.iter().any(|keep| *keep).then_some(crate::parameters::PackedVectors { batch, mask });
        rtdb_storage::IoRegion::retaining(context.clone(), self.open_retained(context, parameters, text, Batching::Columns, arrow)).await
    }

    async fn open(&self, context: Arc<QueryContext>, parameters: Vec<Value>, text: Option<&str>, batching: Batching) -> Result<QueryCursor> {
        rtdb_storage::IoRegion::retaining(context.clone(), self.open_retained(context, parameters, text, batching, None)).await
    }

    async fn open_retained(&self, context: Arc<QueryContext>, mut parameters: Vec<Value>, text: Option<&str>, batching: Batching, arrow: Option<crate::parameters::PackedVectors>) -> Result<QueryCursor> {
        if parameters.len() < self.required_parameter_count { return Err(invalid("missing-parameter", "Query has fewer parameters than its prepared plan")); }
        for dependency in self.relation.dependencies() {
            let Some(current) = context.catalog.table(&dependency.name)? else {
                if dependency.oid.is_none() && dependency.columns.is_empty() { continue; }
                return Err(invalid("stale-plan", "Prepared table is no longer present"));
            };
            if current.oid != dependency.oid || current.columns != dependency.columns {
                return Err(invalid("stale-plan", "Prepared table OID or column types changed; prepare against the execution snapshot"));
            }
        }
        let bound = parameters.iter().try_fold(crate::runtime_schema::allocation_bound(&self.relation)?, |bytes, value| {
            bytes.checked_add(value.retained_bytes()?.checked_mul(16).ok_or_else(|| invalid("parameter-size", "Parameter type allocation overflow"))?).ok_or_else(|| invalid("parameter-size", "Parameter type allocation overflow"))
        })?;
        let temporary = context.memory.reserve(bound)?;
        let apply_types = |mode, values: &[Value]| -> Result<Vec<LogicalType>> {
            let mut types = parameter_types(&self.relation, values, mode);
            if let Some(arrow) = &arrow {
                for (index, &packed) in arrow.mask.iter().enumerate() {
                    if packed { types[index] = mode.field_type(&rtdb_columnar::from_field(arrow.batch.batch().schema_ref().field(index))?); }
                }
            }
            Ok(types)
        };
        let result_types = apply_types(EmissionMode::ArrowFields, &parameters)?;
        let schema = self.emit_result_schema(&context, &result_types)?;
        parameters.iter_mut().for_each(rtdb_columnar::admit_execution_value);
        let context = context.apply_settings(&self.settings, &parameters)?;
        let types = apply_types(EmissionMode::Execution, &parameters)?;
        let query_span = crate::trace::QueryTrace::start(&context, text);
        let context = Arc::clone(&context).with_tracer(context.tracer.clone(), query_span.as_ref().map(|span| span.context()));
        if let QueryOutput::Plan(plan) = &self.output {
            let reservation = context.memory.reserve(plan.copy_bound()?)?;
            let mut cursor = QueryCursor::from_rows(plan.schema.clone(), plan.rows(), context, reservation)?;
            cursor.query_span = query_span;
            cursor.plan_reservation = self._plan_reservation.clone();
            return Ok(cursor);
        }
        let scores_requested = !context.accelerators.is_empty() && uses_search_score(&self.relation);
        let context = context.with_score_corpus(scores_requested);
        let parameters = Arc::new(crate::parameters::BoundParameters::new(parameters, types, &context.memory)?.with_arrow(arrow));
        drop(temporary);
        let runtime_schema = EmittedRelation::derive(Arc::clone(&self.relation), &context.catalog, parameters.types(), &context.memory)?;
        let analysis = if matches!(&self.output, QueryOutput::Analyze(_)) { Some(crate::analysis::AnalysisNode::new(&self.relation, &context)?) } else { None };
        let execution = Execution { context: Arc::clone(&context), parameters, outer: crate::context::OuterRows::empty(&context.memory)?, bindings: None, analysis: analysis.clone() };
        let cancellation = context.cancellation.clone();
        let owner = context.owner_cancellation.clone();
        let operators = Arc::clone(&runtime_schema);
        let vector_projection = crate::cosine::in_projection(&self.relation)
            || runtime_schema.schema().fields().iter().any(|field| crate::columnar::has_float_vector(&field.data_type));
        let input = owner.run_until_cancelled(cancellation.run_until_cancelled(async {
            match batching {
                Batching::Pages if analysis.is_none() => crate::operators::build_pages(operators, execution).await.map(QueryInput::Pages),
                mode if analysis.is_none() && (matches!(mode, Batching::Columns) || vector_projection) => match crate::operators::build_columns(Arc::clone(&operators), execution.clone()).await? {
                    Some(batches) => Ok(QueryInput::Columns(batches)),
                    None => crate::operators::build(operators, execution.with_scalar_parameters()?).await.map(QueryInput::Rows),
                },
                _ => crate::operators::build(operators, execution).await.map(QueryInput::Rows),
            }
        })).await.flatten().ok_or_else(cancelled)??;
        let sample = Some(ExecutionSample::new(context.execution_observer.clone(), ExecutionTimer::Query));
        if let (QueryOutput::Analyze(schema), Some(analysis)) = (&self.output, analysis) {
            let QueryInput::Rows(mut input) = input else { return Err(invalid("analysis-input", "Query analysis requires a row cursor")); };
            while let Some(row) = owner.run_until_cancelled(cancellation.run_until_cancelled(input.next())).await.flatten().ok_or_else(cancelled)?? { context.check_cancelled()?; drop(row); }
            drop(input);
            let peak = context.memory.peak_bytes();
            let reservation = context.memory.reserve(analysis.output_bound()?)?;
            let rows = analysis.rows(peak)?;
            let mut cursor = QueryCursor::from_rows_with_sample(schema.clone(), rows, context, reservation, sample)?;
            cursor.query_span = query_span;
            cursor.plan_reservation = self._plan_reservation.clone();
            return Ok(cursor);
        }
        let trace = crate::trace::CursorTrace::new(&context, "output", "query.cursor.output", true);
        Ok(QueryCursor { schema, runtime_schema: runtime_schema.output(), input: Some(input), context, sample, trace, query_span, plan_reservation: self._plan_reservation.clone() })
    }
}

fn reserve_plan(relation: &Relation, settings: &rtdb_plan::QuerySettings, memory: &Arc<MemoryBudget>) -> Result<Arc<MemoryReservation>> {
    let settings_bytes = settings.retained_bytes()?;
    let bytes = crate::runtime_schema::allocation_bound(relation)?.checked_mul(2).and_then(|bytes| bytes.checked_add(settings_bytes))
        .ok_or_else(|| invalid("plan-size", "Prepared plan allocation overflow"))?;
    memory.reserve(bytes)
}

enum QueryInput { Rows(BoxCursor), Pages(crate::pages::BoxPageCursor), Columns(crate::columnar::BoxBatchCursor) }
#[derive(Clone, Copy)]
enum Batching { Rows, Pages, Columns }

pub struct QueryCursor { schema: Arc<RuntimeSchema>, runtime_schema: Arc<RuntimeSchema>, input: Option<QueryInput>, context: Arc<QueryContext>, sample: Option<ExecutionSample>, trace: Option<crate::trace::CursorTrace>, query_span: Option<crate::trace::QueryTrace>, plan_reservation: Option<Arc<MemoryReservation>> }
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
        let runtime_schema = RuntimeSchema::fixed(&schema, &context.memory)?;
        let sample = sample.or_else(|| Some(ExecutionSample::new(context.execution_observer.clone(), ExecutionTimer::Query)));
        Ok(Self { schema: Arc::clone(&runtime_schema), runtime_schema, input: Some(QueryInput::Rows(Box::new(OwnedRows { rows: rows.into_iter(), reservation }))), context: context.execution_child(), sample, trace: None, query_span: None, plan_reservation: None })
    }
    pub fn schema(&self) -> &Schema { self.schema.schema() }
    /// Batch fields derived from this execution's pinned catalog, before any rows are read.
    /// The advertised result fields remain available through `schema`.
    pub fn runtime_schema(&self) -> &Schema { self.runtime_schema.schema() }
    pub fn cancel(&mut self) { self.context.cancellation.cancel(); self.finish_trace(); self.sample = None; }
    pub async fn next_batch(&mut self) -> Result<Option<OwnedBatch>> {
        if let Err(error) = self.context.check_cancelled() { self.input = None; self.record_error(); return Err(error); }
        let token = self.context.cancellation.clone();
        let owner = self.context.owner_cancellation.clone();
        if let Some(trace) = &mut self.trace { trace.start(); }
        let started = std::time::Instant::now();
        let retention = self.context.clone();
        let pull = owner.run_until_cancelled(token.run_until_cancelled(self.pull_batch()));
        let result = rtdb_storage::IoRegion::retaining(retention, pull).await.flatten().ok_or_else(cancelled);
        match result {
            Ok(Ok(batch)) => { if let Some(trace) = &mut self.trace { trace.completed(started.elapsed(), batch.as_ref().map(|batch| batch.batch().num_rows())); } if batch.is_none() { self.input = None; } Ok(batch) }
            Ok(Err(error)) | Err(error) => { self.input = None; self.record_error(); Err(error) }
        }
    }
    fn record_error(&self) { if let Some(observer) = &self.context.execution_observer { observer.record_error(ExecutionError::Query); } }
    /// Records an error while consuming a returned batch, such as row encoding or transport writing.
    /// Do not call this for `next_batch` failures, which the cursor already records.
    pub fn record_consumer_error(&self) { self.record_error(); }
    fn finish_trace(&mut self) {
        self.input = None;
        self.trace = None;
        self.query_span = None;
    }
    async fn pull_batch(&mut self) -> Result<Option<OwnedBatch>> {
        let Some(input) = &mut self.input else { return Ok(None); };
        let page = match input {
            QueryInput::Columns(input) => loop {
                let Some(batch) = input.next_batch().await? else { return Ok(None); };
                if batch.len != 0 { return column_batch(self.runtime_schema.schema(), &self.context.memory, batch).map(Some); }
            },
            QueryInput::Pages(input) => {
                let Some(page) = input.next_page().await? else { return Ok(None); };
                page
            }
            QueryInput::Rows(input) => {
                let mut rows = crate::pages::PageBuilder::new(self.context.options.batch_rows, &self.context.memory)?;
                while rows.len() < self.context.options.batch_rows {
                    let Some(row) = input.next().await? else { break; };
                    if row.values.len() != self.schema.schema().len() { return Err(invalid("row-width", "Operator output differs from its declared schema")); }
                    rows.push(row)?;
                }
                if rows.len() == 0 { return Ok(None); }
                rows.finish()
            }
        };
        let rows = page.rows();
        if rows.iter().any(|row| row.values.len() != self.schema.schema().len()) {
            return Err(invalid("row-width", "Operator output differs from its declared schema"));
        }
        let schema = self.runtime_schema.schema();
        let mut bound = 4096usize;
        for (column, field) in schema.fields().iter().enumerate() {
            bound = bound.checked_add(estimate_builder_bytes_refs(&field.data_type, rows.iter().map(|row| &row.values[column]))?).ok_or_else(|| invalid("batch-size", "Output batch allocation bound overflow"))?;
        }
        let reservation = self.context.memory.reserve(bound)?;
        let fields = schema.fields().iter().map(|field| to_field(&field.name, &field.data_type)).collect::<Result<Vec<_>>>()?;
        let arrays: Vec<ArrayRef> = schema.fields().iter().enumerate().map(|(column, field)| {
            copy_array(&field.data_type, &rows.iter().map(|row| row.values[column].clone()).collect::<Vec<_>>())
        }).collect::<Result<_>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        let batch = RecordBatch::try_new_with_options(Arc::new(ArrowSchema::new(fields)), arrays, &options).map_err(|error| invalid("output-arrow", error))?;
        OwnedBatch::with_reservation(batch, reservation).map(Some)
    }
}

/// The batch a page of the same rows would encode: a column already in the result's layout is kept, any other is
/// copied from the cells a row would hold.
fn column_batch(schema: &Schema, memory: &Arc<MemoryBudget>, batch: crate::columnar::ColumnBatch) -> Result<OwnedBatch> {
    let overflow = || invalid("batch-size", "Output batch allocation bound overflow");
    if batch.columns.len() != schema.len() { return Err(invalid("row-width", "Operator output differs from its declared schema")); }
    let fields = schema.fields().iter().map(|field| to_field(&field.name, &field.data_type)).collect::<Result<Vec<_>>>()?;
    let kept: Vec<Option<ArrayRef>> = batch.columns.iter().zip(schema.fields()).zip(&fields).map(|((column, field), arrow)| crate::columnar::conform(column, &field.data_type, arrow)).collect();
    let kept_bytes = kept.iter().flatten().try_fold(4096usize, |bytes, array| bytes.checked_add(array.get_array_memory_size()).ok_or_else(overflow))?;
    let mut reservation = memory.reserve(kept_bytes)?;
    let mut arrays = Vec::with_capacity(kept.len());
    for ((kept, column), field) in kept.into_iter().zip(&batch.columns).zip(schema.fields()) {
        if let Some(array) = kept { arrays.push(array); continue; }
        let mut cells = batch.len.checked_mul(std::mem::size_of::<Value>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?;
        for row in 0..batch.len { cells = cells.checked_add(column.bound(row)?).ok_or_else(overflow)?; }
        let _cells = memory.reserve(cells)?;
        let values = (0..batch.len).map(|row| column.value(row)).collect::<Result<Vec<_>>>()?;
        let bytes = estimate_builder_bytes_refs(&field.data_type, values.iter())?;
        let lease = Arc::get_mut(&mut reservation).ok_or_else(|| invalid("batch-size", "An output batch's charge is shared"))?;
        lease.grow_to(lease.bytes().checked_add(bytes).ok_or_else(overflow)?)?;
        arrays.push(copy_array(&field.data_type, &values)?);
    }
    let options = RecordBatchOptions::new().with_row_count(Some(batch.len));
    let output = RecordBatch::try_new_with_options(Arc::new(ArrowSchema::new(fields)), arrays, &options).map_err(|error| invalid("output-arrow", error))?;
    drop(batch);
    OwnedBatch::with_reservation(output, reservation)
}
struct OwnedRows { rows: std::vec::IntoIter<Vec<Value>>, reservation: Arc<MemoryReservation> }
#[async_trait]
impl RowCursor for OwnedRows { async fn next(&mut self) -> Result<Option<OwnedRow>> { Ok(self.rows.next().map(|values| OwnedRow { values, bytes: self.reservation.bytes(), reservation: Arc::clone(&self.reservation) })) } }
impl Drop for QueryCursor { fn drop(&mut self) { self.context.cancellation.cancel(); self.finish_trace(); } }

#[cfg(test)]
mod slab_tests {
    use super::*;

    #[test]
    fn a_slab_admits_a_row_the_budget_holds_when_a_whole_chunk_does_not_fit() {
        let memory = MemoryBudget::new(RowSlab::CHUNK / 2);
        let mut slab = RowSlab::new(&memory);
        let row = OwnedRow::in_slab(vec![Value::I64(1), Value::I64(2)], &mut slab).unwrap();
        assert_eq!(memory.used_bytes(), row.bytes());
        drop((row, slab));
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[cfg(test)]
mod vector_path_tests {
    use super::*;
    use std::collections::BTreeMap;
    use arrow_array::{Array, FixedSizeListArray, Float32Array};
    use arrow_schema::{DataType, Field};
    use rtdb_index::{PartitionState, TransactionBuffer};
    use rtdb_plan::{Binder, Expr, NamedExpr, QualifiedTable, RelationNode, Scan, SortExpr, TemporalFilter};
    use rtdb_storage::{MemoryStore, cache::BufferPool};
    use rtdb_types::{Interval, ScalarType, StructFields, TableRef, Timestamp, TransactionKey, TxId};
    use tokio_util::sync::CancellationToken;

    #[tokio::test]
    async fn ranked_vector_query_opens_as_columns_from_arrow_arguments() {
        let memory = MemoryBudget::new(1 << 25);
        let table = TableRef::new("public", "vectors");
        let state = PartitionState::empty(rtdb_trie::TrieConfig::default()).unwrap();
        let key = TransactionKey { tx_id: TxId(0), system_time: Timestamp(1) };
        let mut tx = TransactionBuffer::new(state.value().snapshot(), key, Arc::clone(&memory)).unwrap();
        for (id, values) in [(1, [0.0, 1.0]), (2, [1.0, 0.0])] {
            let doc = StructFields::from([("_id".into(), Value::I64(id)),
                ("embedding".into(), Value::FixedSizeList(values.map(Value::F32).to_vec().into()))]);
            tx.put(table.clone(), doc, Interval::ALL).unwrap();
        }
        state.apply(&tx.stage().unwrap()).unwrap();
        let snapshot = state.value().snapshot();
        let vector_type = snapshot.column_types(&table)["embedding"].clone();
        let database = crate::DatabaseSnapshot { partitions: vec![snapshot],
            storage: Arc::new(BufferPool::new(Arc::new(MemoryStore::default()), 1 << 20)) };
        let catalog = crate::CatalogSnapshot::new("xtdb", BTreeMap::from([("xtdb".into(), database)]), false).unwrap();
        let context = QueryContext::new(catalog, Arc::clone(&memory), Timestamp(1), "UTC", crate::QueryOptions {
            batch_rows: 1, ..Default::default()
        }, &CancellationToken::new()).unwrap();
        let table = context.catalog.table(&QualifiedTable { database: "xtdb".into(), table }).unwrap().unwrap();
        let mut binder = Binder::default();
        let vector = binder.field("embedding", None, vector_type.clone()).unwrap();
        let scan = Arc::new(Relation::scan(Scan {
            table, columns: vec![vector.clone()], valid_time: TemporalFilter::All, system_time: TemporalFilter::All,
            clamp_valid_time: false, iids: None, predicates: vec![], top_k: None, ordered_top_k: None, matches: vec![],
        }).unwrap());
        let distance_type = LogicalType::Scalar(ScalarType::F64).maybe();
        let distance = binder.field("distance", None, distance_type.clone()).unwrap();
        let input = Arc::new(scan.project(vec![
            NamedExpr { field: vector.clone(), expr: vector.expression() },
            NamedExpr { field: distance.clone(), expr: Expr::Call { name: "cosine_distance".into(),
                args: vec![vector.expression(), Expr::Parameter { index: 0, data_type: vector_type }], return_type: distance_type } },
        ]).unwrap());
        let plan = Arc::new(Relation::new(input.schema().clone(), RelationNode::SortLimit {
            input, order_by: vec![SortExpr { expr: distance.expression(), descending: false, nulls_first: false }],
            offset: None, fetch: Some(Expr::Literal(Value::I64(1))),
        }).unwrap());
        let element = Arc::new(Field::new("item", DataType::Float32, false));
        let vector = FixedSizeListArray::try_new(element, 2, Arc::new(Float32Array::from(vec![1.0, 0.0])), None).unwrap();
        let schema = ArrowSchema::new(vec![Field::new("", vector.data_type().clone(), false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(vector)]).unwrap();
        let args = OwnedBatch::new(batch, &memory).unwrap();
        let mut cursor = PreparedQuery::new(plan).unwrap().execute_arrow(context, args, None, &BTreeMap::new()).await.unwrap();
        assert!(matches!(cursor.input, Some(QueryInput::Columns(_))));
        let result = cursor.next_batch().await.unwrap().unwrap();
        let vector = result.batch().column(0).as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        let values = vector.values().as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(values.values().as_ref(), &[1.0, 0.0]);
        assert!(cursor.next_batch().await.unwrap().is_none());
    }
}
