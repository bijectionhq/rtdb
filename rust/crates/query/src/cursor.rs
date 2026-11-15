use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use rtdb_columnar::{MemoryBudget, MemoryReservation, OwnedBatch, build_array, estimate_builder_bytes_refs, to_field};
use rtdb_plan::{Catalog, Relation, Schema};
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
pub(crate) trait RowCursor: Send { async fn next(&mut self) -> Result<Option<OwnedRow>>; }
pub(crate) type BoxCursor = Box<dyn RowCursor>;

#[derive(Clone)]
pub struct PreparedQuery { relation: Arc<Relation> }
impl PreparedQuery {
    pub fn new(relation: Arc<Relation>) -> Result<Self> { relation.validate()?; Ok(Self { relation }) }
    pub fn schema(&self) -> &Schema { self.relation.schema() }
    pub fn relation(&self) -> &Arc<Relation> { &self.relation }
    pub fn execute(&self, context: Arc<QueryContext>, parameters: Vec<Value>) -> Result<QueryCursor> {
        if parameters.len() < self.relation.parameter_count() { return Err(invalid("missing-parameter", "Query has fewer parameters than its prepared plan")); }
        for dependency in self.relation.dependencies() {
            let current = context.catalog.table(&dependency.name)?.ok_or_else(|| invalid("stale-plan", "Prepared table is no longer present"))?;
            if current.oid != dependency.oid || current.columns != dependency.columns {
                return Err(invalid("stale-plan", "Prepared table OID or column types changed; prepare against the execution snapshot"));
            }
        }
        let context = context.execution_child();
        let parameters = Arc::new(OwnedRow::new(parameters, &context.memory)?);
        let execution = Execution { context: Arc::clone(&context), parameters, outer: Vec::new() };
        let input = crate::operators::build(Arc::clone(&self.relation), execution)?;
        Ok(QueryCursor { schema: self.relation.schema().clone(), input: Some(input), context })
    }
}

pub struct QueryCursor { schema: Schema, input: Option<BoxCursor>, context: Arc<QueryContext> }
impl QueryCursor {
    pub fn schema(&self) -> &Schema { &self.schema }
    pub fn cancel(&mut self) { self.context.cancellation.cancel(); self.input = None; }
    pub async fn next_batch(&mut self) -> Result<Option<OwnedBatch>> {
        if let Err(error) = self.context.check_cancelled() { self.input = None; return Err(error); }
        let token = self.context.cancellation.clone();
        let owner = self.context.owner_cancellation.clone();
        let result = owner.run_until_cancelled(token.run_until_cancelled(self.pull_batch())).await.flatten().ok_or_else(cancelled);
        match result {
            Ok(Ok(batch)) => { if batch.is_none() { self.input = None; } Ok(batch) }
            Ok(Err(error)) | Err(error) => { self.input = None; Err(error) }
        }
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
            build_array(&field.data_type, &rows.iter().map(|row| row.values[column].clone()).collect::<Vec<_>>())
        }).collect::<Result<_>>()?;
        let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
        let batch = RecordBatch::try_new_with_options(Arc::new(ArrowSchema::new(fields)), arrays, &options).map_err(|error| invalid("output-arrow", error))?;
        OwnedBatch::with_reservation(batch, reservation).map(Some)
    }
}
impl Drop for QueryCursor { fn drop(&mut self) { self.context.cancellation.cancel(); } }
