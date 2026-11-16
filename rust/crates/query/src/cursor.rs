use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch, copy_array, estimate_builder_bytes_refs, to_field};
use rtdb_plan::{Catalog, ExecutionError, ExecutionSample, ExecutionTimer, Relation, Schema};
use rtdb_types::{Result, Value};

use crate::{QueryContext, context::{Execution, cancelled}, invalid};

pub(crate) struct OwnedRow { pub values: Vec<Value>, reservation: Arc<MemoryReservation> }
impl OwnedRow {
    pub fn new(values: Vec<Value>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(row_bytes(&values, values.capacity())?)?;
        Ok(Self { values, reservation })
    }
    pub fn with_reservation(values: Vec<Value>, reservation: Arc<MemoryReservation>) -> Result<Self> {
        if row_bytes(&values, values.capacity())? > reservation.bytes() { return Err(invalid("row-reservation", "Decoded row exceeds its memory reservation")); }
        Ok(Self { values, reservation })
    }
    pub fn bytes(&self) -> usize { self.reservation.bytes() }
}
fn row_bytes(values: &[Value], capacity: usize) -> Result<usize> {
    let spare = capacity.saturating_sub(values.len()).checked_mul(std::mem::size_of::<Value>()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(|| invalid("row-size", "Retained row capacity overflow"))?;
    values.iter().try_fold(spare, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(|| invalid("row-size", "Retained row size overflow")))
}

#[async_trait]
pub(crate) trait RowCursor: Send {
    async fn next(&mut self) -> Result<Option<OwnedRow>>;
    fn trace_attributes(&self, _span: &mut dyn rtdb_plan::TraceSpan) {}
}
pub(crate) type BoxCursor = Box<dyn RowCursor>;

#[derive(Clone)]
pub struct PreparedQuery { relation: Arc<Relation> }
impl PreparedQuery {
    pub fn new(relation: Arc<Relation>) -> Result<Self> { relation.validate()?; Ok(Self { relation }) }
    pub fn schema(&self) -> &Schema { self.relation.schema() }
    pub fn relation(&self) -> &Arc<Relation> { &self.relation }
    pub fn execute(&self, context: Arc<QueryContext>, parameters: Vec<Value>) -> Result<QueryCursor> {
        self.execute_with_text(context, parameters, None)
    }
    pub fn execute_with_text(&self, context: Arc<QueryContext>, parameters: Vec<Value>, text: Option<&str>) -> Result<QueryCursor> {
        if parameters.len() < self.relation.parameter_count() { return Err(invalid("missing-parameter", "Query has fewer parameters than its prepared plan")); }
        for dependency in self.relation.dependencies() {
            let current = context.catalog.table(&dependency.name)?.ok_or_else(|| invalid("stale-plan", "Prepared table is no longer present"))?;
            if current.oid != dependency.oid || current.columns != dependency.columns {
                return Err(invalid("stale-plan", "Prepared table OID or column types changed; prepare against the execution snapshot"));
            }
        }
        let context = context.execution_child();
        let query_span = crate::trace::QueryTrace::start(&context, text);
        let context = Arc::clone(&context).with_tracer(context.tracer.clone(), query_span.as_ref().map(|span| span.context()));
        let parameters = Arc::new(OwnedRow::new(parameters, &context.memory)?);
        let execution = Execution { context: Arc::clone(&context), parameters, outer: Vec::new() };
        let input = crate::operators::build(Arc::clone(&self.relation), execution)?;
        let sample = Some(ExecutionSample::new(context.execution_observer.clone(), ExecutionTimer::Query));
        let trace = crate::trace::CursorTrace::new(&context, "output", "query.cursor.output", true);
        Ok(QueryCursor { schema: self.relation.schema().clone(), input: Some(input), context, sample, trace, query_span })
    }
}

pub struct QueryCursor { schema: Schema, input: Option<BoxCursor>, context: Arc<QueryContext>, sample: Option<ExecutionSample>, trace: Option<crate::trace::CursorTrace>, query_span: Option<crate::trace::QueryTrace> }
impl QueryCursor {
    /// The caller reserves before allocating rows; the reservation must cover their complete retained capacities.
    /// The returned cursor owns the rows and a child cancellation scope; its parent context stays usable after cancellation.
    pub fn from_rows(schema: Schema, rows: Vec<Vec<Value>>, context: Arc<QueryContext>, reservation: Arc<MemoryReservation>) -> Result<Self> {
        let retained = rows.iter().try_fold(rows.capacity().checked_mul(std::mem::size_of::<Vec<Value>>()).ok_or_else(|| invalid("row-size", "Row container size overflow"))?, |bytes, row| {
            if row.len() != schema.len() { return Err(invalid("row-width", "Owned row differs from declared schema")); }
            bytes.checked_add(row_bytes(row, row.capacity())?).ok_or_else(|| invalid("row-size", "Owned row size overflow"))
        })?;
        if retained > reservation.bytes() { return Err(invalid("row-reservation", "Owned rows exceed their memory reservation")); }
        let sample = Some(ExecutionSample::new(context.execution_observer.clone(), ExecutionTimer::Query));
        Ok(Self { schema, input: Some(Box::new(OwnedRows { rows: rows.into_iter(), reservation })), context: context.execution_child(), sample, trace: None, query_span: None })
    }
    pub fn schema(&self) -> &Schema { &self.schema }
    pub fn cancel(&mut self) { self.context.cancellation.cancel(); self.finish_trace(); self.sample = None; }
    pub async fn next_batch(&mut self) -> Result<Option<OwnedBatch>> {
        if let Err(error) = self.context.check_cancelled() { self.input = None; self.record_error(); return Err(error); }
        let token = self.context.cancellation.clone();
        let owner = self.context.owner_cancellation.clone();
        if let Some(trace) = &mut self.trace { trace.start(); }
        let started = std::time::Instant::now();
        let result = owner.run_until_cancelled(token.run_until_cancelled(self.pull_batch())).await.flatten().ok_or_else(cancelled);
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
        let mut rows = Vec::new();
        while rows.len() < self.context.options.batch_rows {
            let Some(row) = input.next().await? else { break; };
            if row.values.len() != self.schema.len() { return Err(invalid("row-width", "Operator output differs from its declared schema")); }
            rows.push(row);
        }
        if rows.is_empty() { return Ok(None); }
        let mut bound = 4096usize;
        for (column, field) in self.schema.fields().iter().enumerate() {
            bound = bound.checked_add(estimate_builder_bytes_refs(&field.data_type, rows.iter().map(|row| &row.values[column]))?).ok_or_else(|| invalid("batch-size", "Output batch allocation bound overflow"))?;
        }
        let reservation = self.context.memory.reserve(bound)?;
        let fields = self.schema.fields().iter().map(|field| to_field(&field.name, &field.data_type)).collect::<Result<Vec<_>>>()?;
        let arrays: Vec<ArrayRef> = self.schema.fields().iter().enumerate().map(|(column, field)| {
            copy_array(&field.data_type, &rows.iter().map(|row| row.values[column].clone()).collect::<Vec<_>>())
        }).collect::<Result<_>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        let batch = RecordBatch::try_new_with_options(Arc::new(ArrowSchema::new(fields)), arrays, &options).map_err(|error| invalid("output-arrow", error))?;
        OwnedBatch::with_reservation(batch, reservation).map(Some)
    }
}
struct OwnedRows { rows: std::vec::IntoIter<Vec<Value>>, reservation: Arc<MemoryReservation> }
#[async_trait]
impl RowCursor for OwnedRows { async fn next(&mut self) -> Result<Option<OwnedRow>> { Ok(self.rows.next().map(|values| OwnedRow { values, reservation: Arc::clone(&self.reservation) })) } }
impl Drop for QueryCursor { fn drop(&mut self) { self.context.cancellation.cancel(); self.finish_trace(); } }
