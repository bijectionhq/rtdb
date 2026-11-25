use async_trait::async_trait;
use std::sync::Arc;
use rtdb_plan::{ColumnId, Expr, PatchEndpoint, Schema};
use rtdb_format::transit::{TransitValue, to_json_value};
use rtdb_types::{Anomaly, Iid, Result, Timestamp, Value};

use crate::{columnar::{BatchCursor, BoxBatchCursor, Column, ColumnBatch, gather_nullable_column_iter, times},
    context::Execution, invalid};

pub(crate) struct BatchPatchGaps {
    input: BoxBatchCursor,
    layout: PatchLayout,
}

struct PatchLayout {
    columns: [usize; 4],
    width: usize,
    from: Timestamp,
    to: Timestamp,
    execution: Execution,
}

fn read_iid(value: &Value) -> Result<Iid> {
    match value {
        Value::Binary(bytes) | Value::FixedSizeBinary(bytes) => Iid::from_slice(bytes),
        Value::Uuid(bytes) => Ok(Iid(*bytes)),
        _ => Err(invalid("patch-iid", "Patch input IID is not sixteen bytes")),
    }
}

impl PatchLayout {
    fn new(schema: &Schema, columns: [ColumnId; 4], from: &Expr, to: &Expr, execution: Execution) -> Result<Self> {
        let scope = execution.scope()?;
        fn bound(expr: &Expr, lower: bool, execution: &Execution, scope: &rtdb_expr::EvaluationScope) -> Result<Timestamp> {
            let value = match PatchEndpoint::from_expr(expr)? {
                PatchEndpoint::Default if !lower => return Ok(Timestamp::MAX),
                PatchEndpoint::Default | PatchEndpoint::CurrentTime => return execution.context.current_time.as_micros(),
                PatchEndpoint::DateTime(value) => value,
                PatchEndpoint::Parameter(index) => execution.parameters.get(index)?.ok_or_else(|| invalid("patch-parameter", "Patch endpoint parameter is absent"))?,
            };
            scope.reserve(value.retained_bytes()?.checked_add(execution.context.timezone.len()).and_then(|bytes|bytes.checked_mul(8)).and_then(|bytes|bytes.checked_add(16_384)).ok_or_else(||invalid("patch-endpoint-size", "Patch endpoint conversion exceeds addressable memory"))?)?;
            rtdb_expr::query_clock_instant(value, &execution.context.timezone)?.as_micros()
        }
        let from = bound(from, true, &execution, &scope)?;
        let to = bound(to, false, &execution, &scope)?;
        if from > to {
            scope.reserve(4096)?;
            return Err(Anomaly::incorrect("xtdb.indexer/invalid-valid-times", "Invalid valid times")
                .with_context("valid-from", to_json_value(&TransitValue::instant(from.into())?)?)
                .with_context("valid-to", to_json_value(&TransitValue::instant(to.into())?)?));
        }
        let columns = columns.map(|id| schema.position(id).ok_or_else(|| invalid("patch-column", "Patch column is absent")));
        let [iid, vf, vt, doc] = columns;
        Ok(Self { columns: [iid?, vf?, vt?, doc?], width: schema.len(), from, to, execution })
    }

}

#[derive(Clone, Copy)]
struct PatchSpan {
    source: usize,
    from: Timestamp,
    to: Timestamp,
    copy_document: bool,
}

struct PatchTimeline {
    from: Timestamp,
    to: Timestamp,
    current: Option<(usize, Iid, Timestamp)>,
}

impl PatchTimeline {
    fn advance(&mut self, source: usize, iid: Iid, from: Timestamp, to: Timestamp) -> [Option<PatchSpan>; 3] {
        let mut cursor = self.from;
        let mut tail = None;
        if let Some((previous_row, previous, previous_to)) = self.current {
            if iid == previous { cursor = previous_to; }
            else if previous_to < self.to { tail = Some(Self::gap(previous_row, previous_to, self.to)); }
        }
        self.current = Some((source, iid, to));
        [
            tail,
            (from > cursor).then(|| Self::gap(source, cursor, from.min(self.to))),
            Some(PatchSpan { source, from: from.max(self.from), to: to.min(self.to), copy_document: true }),
        ]
    }

    fn finish(self) -> Option<PatchSpan> {
        let (source, _, from) = self.current?;
        (from < self.to).then(|| Self::gap(source, from, self.to))
    }

    fn gap(source: usize, from: Timestamp, to: Timestamp) -> PatchSpan {
        PatchSpan { source, from, to, copy_document: false }
    }
}

impl PatchLayout {
    fn timeline(&self) -> PatchTimeline { PatchTimeline { from: self.from, to: self.to, current: None } }

    fn read_time(&self, value: &Value, upper: bool) -> Result<Timestamp> {
        if upper && matches!(value, Value::Null | Value::Undefined) { return Ok(Timestamp::MAX); }
        rtdb_expr::timestamp_micros(value, &self.execution.context.timezone)
    }
}

impl BatchPatchGaps {
    pub fn new(input: BoxBatchCursor, schema: &Schema, columns: [ColumnId; 4], from: &Expr, to: &Expr, execution: Execution) -> Result<Self> {
        Ok(Self { input, layout: PatchLayout::new(schema, columns, from, to, execution)? })
    }
}

#[async_trait]
impl BatchCursor for BatchPatchGaps {
    async fn next_batch(&mut self) -> Result<Option<ColumnBatch>> {
        use arrow_array::TimestampMicrosecondArray;
        use rtdb_types::{LogicalType, ScalarType};

        let layout = &self.layout;
        let context = &layout.execution.context;
        context.check_cancelled()?;
        let Some(batch) = crate::columnar::poll_nested_batch(self.input.as_mut()).await? else { return Ok(None); };
        let [iid, valid_from, valid_to, document] = layout.columns;
        let capacity = batch.len.checked_mul(3).ok_or_else(size_error)?;
        let span_bytes = capacity.checked_mul(size_of::<PatchSpan>()).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
        let _spans = context.memory.reserve(span_bytes)?;
        let mut spans = Vec::with_capacity(capacity);
        let mut timeline = layout.timeline();
        let read_time = |position, row, upper| -> Result<Timestamp> {
            let column: &Column = &batch.columns[position];
            if let Some(Some((array, _, _, offset))) = column.arrow_cell(row)
                && let Some(times) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                use arrow_array::Array;
                if times.is_valid(offset) && times.timezone().is_some() { return Ok(Timestamp(times.value(offset))); }
            }
            layout.read_time(&column.value(row)?, upper)
        };
        let read_iid = |row| {
            if let Some(Some((array, _, Some(LogicalType::Scalar(ScalarType::FixedSizeBinary(16))), offset))) = batch.columns[iid].arrow_cell(row)
                && let Some(ids) = array.as_any().downcast_ref::<arrow_array::FixedSizeBinaryArray>() {
                use arrow_array::Array;
                if ids.is_valid(offset) { return Iid::from_slice(ids.value(offset)); }
            }
            read_iid(&batch.columns[iid].value(row)?)
        };
        for row in 0..batch.len {
            context.check_cancelled_at(row)?;
            let id = read_iid(row)?;
            let from = read_time(valid_from, row, false)?;
            let to = read_time(valid_to, row, true)?;
            spans.extend(timeline.advance(row, id, from, to).into_iter().flatten());
        }
        spans.extend(timeline.finish());
        let timestamp_bytes = spans.len().checked_mul(4 * size_of::<i64>()).ok_or_else(size_error)?;
        let output_bound = layout.width.checked_mul(size_of::<Column>() + 512)
            .and_then(|bytes| bytes.checked_add(timestamp_bytes))
            .and_then(|bytes| bytes.checked_add(1024)).ok_or_else(size_error)?;
        let mut reservation = context.memory.reserve(output_bound)?;
        let mut output = Vec::with_capacity(layout.width);
        for position in 0..layout.width {
            context.check_cancelled()?;
            let column = match position {
                index if index == iid || index == document => {
                    let rows = spans.iter().map(|span| {
                        (index == iid || span.copy_document).then_some((0, span.source))
                    });
                    let (column, mut charge) = gather_nullable_column_iter(|_| &batch, rows, position, &context.memory)?;
                    let lease = Arc::get_mut(&mut reservation).expect("Patch output owns its reservation");
                    lease.absorb(Arc::get_mut(&mut charge).expect("A gap gather owns its reservation"))?;
                    column
                }
                index if index == valid_from => times("_valid_from", spans.iter().map(|span| span.from), false),
                index if index == valid_to => times("_valid_to", spans.iter().map(|span| span.to), true),
                _ => Column::Arrow { array: Arc::new(arrow_array::NullArray::new(spans.len())),
                    field: Arc::new(arrow_schema::Field::new("null", arrow_schema::DataType::Null, true)),
                    logical: Some(LogicalType::Null) },
            };
            output.push(column);
        }
        context.check_cancelled()?;
        ColumnBatch::settled(output, spans.len(), reservation).map(Some)
    }
}

fn size_error() -> Anomaly { invalid("patch-size", "Patch page allocation exceeds addressable memory") }

#[cfg(test)]
#[path = "patch_tests.rs"]
mod tests;
