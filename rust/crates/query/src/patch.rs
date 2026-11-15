use async_trait::async_trait;
use rtdb_plan::{ColumnId, Expr, Schema};
use rtdb_types::{Iid, Result, TimeUnit, Timestamp, Value};

use crate::{context::Execution, cursor::{BoxCursor, OwnedRow, RowCursor}, invalid};

pub(crate) struct PatchGaps {
    input: BoxCursor,
    columns: [usize; 4],
    width: usize,
    from: Timestamp,
    to: Timestamp,
    execution: Execution,
    pending: Option<OwnedRow>,
    current: Option<(Iid, Timestamp)>,
    previous_end: Option<Timestamp>,
    ended: bool,
}

impl PatchGaps {
    pub fn new(input: BoxCursor, schema: &Schema, columns: [ColumnId; 4], from: &Expr, to: &Expr, execution: Execution) -> Result<Self> {
        let bound = |expr: &Expr, fallback| -> Result<Timestamp> {
            let value = execution.evaluate(expr, &Schema::default(), &[])?;
            if matches!(value, Value::Null | Value::Undefined) { Ok(fallback) } else { rtdb_expr::timestamp_micros(&value, &execution.context.timezone) }
        };
        let from = bound(from, Timestamp::MIN)?;
        let to = bound(to, Timestamp::MAX)?;
        if from >= to { return Err(invalid("patch-valid-time", "PATCH requires lower valid time before upper valid time")); }
        let columns = columns.map(|id| schema.position(id).ok_or_else(|| invalid("patch-column", "Patch column is absent")));
        let [iid, vf, vt, doc] = columns;
        Ok(Self { input, columns: [iid?, vf?, vt?, doc?], width: schema.len(), from, to, execution, pending: None, current: None, previous_end: None, ended: false })
    }
    fn gap(&self, iid: Iid, from: Timestamp, to: Timestamp) -> Result<OwnedRow> {
        let mut values = vec![Value::Null; self.width];
        values[self.columns[0]] = Value::FixedSizeBinary(iid.0.to_vec());
        values[self.columns[1]] = timestamp(from, false);
        values[self.columns[2]] = timestamp(to, true);
        OwnedRow::new(values, &self.execution.context.memory)
    }
}

#[async_trait]
impl RowCursor for PatchGaps {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        loop {
            self.execution.context.check_cancelled()?;
            if self.pending.is_none() && !self.ended {
                self.pending = self.input.next().await?;
                self.ended = self.pending.is_none();
            }
            let Some(row) = self.pending.as_ref() else {
                if let Some((iid, cursor)) = self.current.take() {
                    if cursor < self.to { return self.gap(iid, cursor, self.to).map(Some); }
                }
                return Ok(None);
            };
            let iid = match &row.values[self.columns[0]] {
                Value::FixedSizeBinary(bytes) | Value::Binary(bytes) => Iid::from_slice(bytes)?,
                Value::Uuid(bytes) => Iid(*bytes), _ => return Err(invalid("patch-iid", "Patch input IID is not sixteen bytes")),
            };
            let from = rtdb_expr::timestamp_micros(&row.values[self.columns[1]], &self.execution.context.timezone)?;
            let to = match &row.values[self.columns[2]] {
                Value::Null | Value::Undefined => Timestamp::MAX,
                value => rtdb_expr::timestamp_micros(value, &self.execution.context.timezone)?,
            };
            if from >= to { return Err(invalid("patch-order", "Patch input contains an empty or inverted interval")); }
            if let Some((previous, cursor)) = self.current {
                if iid < previous { return Err(invalid("patch-order", "Patch input IIDs are not sorted")); }
                if iid != previous {
                    self.current = Some((iid, self.from));
                    self.previous_end = None;
                    if cursor < self.to { return self.gap(previous, cursor, self.to).map(Some); }
                    continue;
                }
            } else { self.current = Some((iid, self.from)); }
            if self.previous_end.is_some_and(|end| from < end) { return Err(invalid("patch-order", "Patch input intervals overlap or are not sorted")); }
            let lower = from.max(self.from);
            let upper = to.min(self.to);
            let cursor = self.current.map(|(_, cursor)| cursor).unwrap_or(self.from);
            if cursor < self.to && lower > cursor {
                let end = lower.min(self.to);
                self.current = Some((iid, end));
                return self.gap(iid, cursor, end).map(Some);
            }
            let mut row = self.pending.take().ok_or_else(|| invalid("patch-state", "Pending patch row disappeared"))?;
            self.previous_end = Some(to);
            self.current = Some((iid, cursor.max(upper)));
            if lower >= upper { continue; }
            row.values[self.columns[1]] = timestamp(lower, false);
            row.values[self.columns[2]] = timestamp(upper, true);
            return Ok(Some(row));
        }
    }
}

fn timestamp(value: Timestamp, upper: bool) -> Value {
    if upper && value == Timestamp::MAX { Value::Null }
    else { Value::Timestamp { value: value.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) } }
}
