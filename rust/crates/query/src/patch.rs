use async_trait::async_trait;
use rtdb_plan::{ColumnId, Expr, PatchEndpoint, Schema};
use rtdb_types::{Anomaly, Iid, Result, TimeUnit, Timestamp, Value};

use crate::{context::Execution, cursor::OwnedRow, pages::{BoxPageCursor, OwnedPage, PageBuilder, PageCursor}, invalid};

pub(crate) struct PatchGaps {
    input: BoxPageCursor,
    columns: [usize; 4],
    width: usize,
    from: Timestamp,
    to: Timestamp,
    execution: Execution,
}

impl PatchGaps {
    pub async fn new(input: BoxPageCursor, schema: &Schema, columns: [ColumnId; 4], from: &Expr, to: &Expr, execution: Execution) -> Result<Self> {
        let scope = execution.scope()?;
        fn bound(expr: &Expr, lower: bool, execution: &Execution, scope: &rtdb_expr::EvaluationScope) -> Result<Timestamp> {
            let value = match PatchEndpoint::from_expr(expr)? {
                PatchEndpoint::Default if !lower => return Ok(Timestamp::MAX),
                PatchEndpoint::Default | PatchEndpoint::CurrentTime => return execution.context.current_time.as_micros(),
                PatchEndpoint::DateTime(value) => value,
                PatchEndpoint::Parameter(index) => execution.parameters.values().get(index).ok_or_else(|| invalid("patch-parameter", "Patch endpoint parameter is absent"))?,
            };
            scope.reserve(value.retained_bytes()?.checked_add(execution.context.timezone.len()).and_then(|bytes|bytes.checked_mul(8)).and_then(|bytes|bytes.checked_add(16_384)).ok_or_else(||invalid("patch-endpoint-size", "Patch endpoint conversion exceeds addressable memory"))?)?;
            rtdb_expr::query_clock_instant(value, &execution.context.timezone)?.as_micros()
        }
        let from = bound(from, true, &execution, &scope)?;
        let to = bound(to, false, &execution, &scope)?;
        if from > to {
            scope.reserve(4096)?;
            let instant = |value: Timestamp| {
                use rtdb_format::transit::{TransitValue, to_json_value};
                to_json_value(&TransitValue::Tagged { tag: "time/instant".into(), representation: Box::new(TransitValue::String(rtdb_types::Instant::from(value).to_iso_string()?)) })
            };
            return Err(Anomaly::incorrect("xtdb.indexer/invalid-valid-times", "Invalid valid times")
                .with_context("valid-from", instant(from)?).with_context("valid-to", instant(to)?));
        }
        let columns = columns.map(|id| schema.position(id).ok_or_else(|| invalid("patch-column", "Patch column is absent")));
        let [iid, vf, vt, doc] = columns;
        Ok(Self { input, columns: [iid?, vf?, vt?, doc?], width: schema.len(), from, to, execution })
    }
    fn gap(&self, iid: Iid, from: Timestamp, to: Timestamp) -> Result<OwnedRow> {
        let bytes = self.width.checked_mul(std::mem::size_of::<Value>()).and_then(|bytes| bytes.checked_add(512)).ok_or_else(|| invalid("patch-size", "Patch row allocation exceeds addressable memory"))?;
        let reservation = self.execution.context.memory.reserve(bytes)?;
        let mut values = vec![Value::Null; self.width];
        values[self.columns[0]] = Value::FixedSizeBinary(iid.0.to_vec());
        values[self.columns[1]] = timestamp(from, false);
        values[self.columns[2]] = timestamp(to, true);
        OwnedRow::with_reservation(values, reservation)
    }
}

#[async_trait]
impl PageCursor for PatchGaps {
    async fn next_page(&mut self) -> Result<Option<OwnedPage>> {
        self.execution.context.check_cancelled()?;
        let Some(mut page) = self.input.next_page().await? else { return Ok(None); };
        let capacity = page.len().checked_mul(3).ok_or_else(|| invalid("patch-size", "Patch page row count exceeds addressable memory"))?;
        let mut output = PageBuilder::new(capacity, &self.execution.context.memory)?;
        let mut current: Option<(Iid, Timestamp)> = None;
        while let Some(mut row) = page.next() {
            self.execution.context.check_cancelled()?;
            let iid = match &row.values[self.columns[0]] {
                Value::FixedSizeBinary(bytes) | Value::Binary(bytes) => Iid::from_slice(bytes)?,
                Value::Uuid(bytes) => Iid(*bytes), _ => return Err(invalid("patch-iid", "Patch input IID is not sixteen bytes")),
            };
            let from = rtdb_expr::timestamp_micros(&row.values[self.columns[1]], &self.execution.context.timezone)?;
            let to = match &row.values[self.columns[2]] {
                Value::Null | Value::Undefined => Timestamp::MAX,
                value => rtdb_expr::timestamp_micros(value, &self.execution.context.timezone)?,
            };
            let mut cursor = self.from;
            if let Some((previous, previous_to)) = current {
                if iid != previous {
                    if previous_to < self.to { output.push(self.gap(previous, previous_to, self.to)?)?; }
                } else { cursor = previous_to; }
            }
            if from > cursor { output.push(self.gap(iid, cursor, from.min(self.to))?)?; }
            let reservation = self.execution.context.memory.reserve(row.bytes().checked_add(64).ok_or_else(|| invalid("patch-size", "Patch row allocation exceeds addressable memory"))?)?;
            for (index, value) in row.values.iter_mut().enumerate() { if !self.columns.contains(&index) { *value = Value::Null; } }
            row.values[self.columns[1]] = timestamp(from.max(self.from), false);
            row.values[self.columns[2]] = timestamp(to.min(self.to), true);
            output.push(OwnedRow::with_reservation(std::mem::take(&mut row.values), reservation)?)?;
            current = Some((iid, to));
        }
        if let Some((iid, cursor)) = current { if cursor < self.to { output.push(self.gap(iid, cursor, self.to)?)?; } }
        Ok(Some(output.finish()))
    }
}

fn timestamp(value: Timestamp, upper: bool) -> Value {
    if upper && value == Timestamp::MAX { Value::Null }
    else { Value::Timestamp { value: value.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) } }
}

#[cfg(test)]
#[path = "patch_tests.rs"]
mod tests;
