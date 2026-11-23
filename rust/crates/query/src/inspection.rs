use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_plan::{BinaryOp, CatalogInspection, ColumnId, Expr, InspectionRequest, Scan, Schema};
use rtdb_types::{Anomaly, Result, TableRef, Value};

use crate::{context::Execution, cursor::{OwnedRow, RowCursor}, invalid};

#[derive(Clone, Default)]
pub(crate) struct Constraints { bounds: BTreeMap<ColumnId, Vec<(BinaryOp, Expr)>> }

impl Constraints {
    pub fn add(&mut self, predicate: &Expr) {
        let Expr::Binary { op, left, right } = predicate else { return; };
        if *op == BinaryOp::And { self.add(left); self.add(right); return; }
        if !matches!(op, BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq) { return; }
        match (left.as_ref(), right.as_ref()) {
            (Expr::Column { id, .. }, constant @ (Expr::Literal(_) | Expr::Parameter { .. })) => self.bounds.entry(*id).or_default().push((*op, constant.clone())),
            (constant @ (Expr::Literal(_) | Expr::Parameter { .. }), Expr::Column { id, .. }) => self.bounds.entry(*id).or_default().push((flip(*op), constant.clone())),
            _ => (),
        }
    }
    async fn values(&self, scan: &Scan, name: &str, execution: &Execution, scope: &rtdb_expr::EvaluationScope) -> Result<Vec<(BinaryOp, Value)>> {
        let Some(column) = scan.columns.iter().find(|field| field.name == name) else { return Ok(Vec::new()); };
        let mut values = Vec::new();
        for (op, expression) in self.bounds.get(&column.id).into_iter().flatten() {
            values.push((*op, execution.evaluate(scope, expression, &Schema::default(), &[]).await?));
        }
        Ok(values)
    }
}

pub(crate) fn is_inspection(table: &TableRef) -> bool {
    table.schema == "xt" && ["source_log_msgs", "replica_log_msgs", "block_files", "table_block_files", "table_block_file_tries"].contains(&table.table.as_str())
}

pub(crate) struct InspectionCursor {
    scan: Scan,
    execution: Execution,
    request: Option<InspectionRequest>,
    input: Option<Box<dyn CatalogInspection>>,
    rows: VecDeque<OwnedRow>,
    done: bool,
    _request_reservation: Arc<rtdb_columnar::MemoryReservation>,
}

impl InspectionCursor {
    pub async fn new(scan: Scan, execution: Execution, constraints: &Constraints) -> Result<Self> {
        let scope = execution.scope()?;
        let mut request_reservation = execution.context.memory.reserve(256)?;
        let request = match scan.table.name.table.table.as_str() {
            "source_log_msgs" | "replica_log_msgs" => {
                let (from, to) = range(&constraints.values(&scan, "msg_id", &execution, &scope).await?, integer)?
                    .ok_or_else(|| Anomaly::incorrect("xtdb/missing-log-bounds", "Queries on log tables require msg_id bounds (e.g. WHERE msg_id = N or WHERE msg_id BETWEEN x AND y)"))?;
                if scan.table.name.table.table == "source_log_msgs" { InspectionRequest::SourceLog { from, to, tx_ops: scan.columns.iter().any(|field| field.name == "tx_ops") } }
                else { InspectionRequest::ReplicaLog { from, to } }
            }
            "block_files" => {
                let (from, to) = range(&constraints.values(&scan, "block_idx", &execution, &scope).await?, block_index)?
                    .ok_or_else(|| Anomaly::incorrect("xtdb/missing-block-idx-bounds", "Queries on xt.block_files require block_idx bounds (e.g. WHERE block_idx = '...' or WHERE block_idx BETWEEN '...' AND '...')"))?;
                InspectionRequest::BlockFiles { from, to }
            }
            name => {
                let table_values = constraints.values(&scan, "table_name", &execution, &scope).await?;
                let block_values = constraints.values(&scan, "block_idx", &execution, &scope).await?;
                let table = equality(&table_values);
                let block = equality(&block_values);
                let (Some(Value::Utf8(table)), Some(block)) = (table, block) else {
                    return Err(Anomaly::incorrect("xtdb/missing-table-block-predicates", format!("Queries on xt.{name} require table_name = '...' AND block_idx = '...'")));
                };
                request_reservation = execution.context.memory.reserve(table.len().checked_add(256).ok_or_else(|| invalid("inspection-size", "Inspection request exceeds addressable memory"))?)?;
                InspectionRequest::TableBlock { table: table.clone(), block: block_index(block)?, tries: name == "table_block_file_tries" }
            }
        };
        Ok(Self { scan, execution, request: Some(request), _request_reservation: request_reservation, input: None, rows: VecDeque::new(), done: false })
    }
}

#[async_trait]
impl RowCursor for InspectionCursor {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        loop {
            self.execution.context.check_cancelled()?;
            if let Some(row) = self.rows.pop_front() { return Ok(Some(row)); }
            if self.done { return Ok(None); }
            if let Some(request) = self.request.take() {
                let runtime = self.execution.context.catalog.runtime.as_ref().ok_or_else(|| invalid("catalog-runtime-unavailable", "Runtime catalog provider was not supplied"))?;
                self.input = Some(runtime.open_inspection(&self.scan.table.name.database, request).await?);
            }
            let Some(input) = &mut self.input else { return Err(invalid("inspection-state", "Inspection cursor has no input")); };
            let Some(page) = input.next().await? else { self.done = true; self.input = None; return Ok(None); };
            let bound = page.rows().iter().try_fold(4096usize, |bytes, row| {
                self.scan.columns.iter().try_fold(bytes.checked_add(1024).ok_or_else(|| invalid("inspection-size", "Inspection row count overflow"))?, |bytes, field| {
                    bytes.checked_add(row.get(&field.name).map_or(Ok(128), Value::retained_bytes)?).ok_or_else(|| invalid("inspection-size", "Inspection value size overflow"))
                })
            })?;
            let reservation = self.execution.context.memory.reserve(bound)?;
            self.rows = page.rows().iter().map(|row| OwnedRow::with_reservation(self.scan.columns.iter().map(|field| row.get(&field.name).cloned().unwrap_or(Value::Null)).collect(), Arc::clone(&reservation))).collect::<Result<_>>()?;
        }
    }
}

fn flip(op: BinaryOp) -> BinaryOp { match op { BinaryOp::Lt => BinaryOp::Gt, BinaryOp::LtEq => BinaryOp::GtEq, BinaryOp::Gt => BinaryOp::Lt, BinaryOp::GtEq => BinaryOp::LtEq, op => op } }
fn equality(values: &[(BinaryOp, Value)]) -> Option<&Value> { values.iter().find(|(op, value)| *op == BinaryOp::Eq && !matches!(value, Value::Null | Value::Undefined)).map(|(_, value)| value) }
fn integer(value: &Value) -> Result<i64> { rtdb_expr::integer(value).and_then(|value| i64::try_from(value).ok()).ok_or_else(|| invalid("inspection-bound", "Log bounds must be integers")) }
fn block_index(value: &Value) -> Result<i64> {
    let Value::Utf8(value) = value else { return Err(invalid("inspection-bound", "Block bounds must be lexicographic hexadecimal strings")); };
    let body = value.get(1..).ok_or_else(|| invalid("inspection-bound", "Block bound has no hexadecimal body"))?;
    i64::from_str_radix(body, 16).map_err(|_| invalid("inspection-bound", "Block bound has an invalid signed hexadecimal body"))
}
fn range(values: &[(BinaryOp, Value)], parse: impl Fn(&Value) -> Result<i64>) -> Result<Option<(i64, i64)>> {
    let mut lower = None;
    let mut upper = None;
    for (op, value) in values {
        if matches!(value, Value::Null | Value::Undefined) { continue; }
        let value = parse(value)?;
        let next = || value.checked_add(1).ok_or_else(|| invalid("inspection-bound", "Inclusive bound exceeds signed position range"));
        let (lo, hi) = match op { BinaryOp::Eq => (Some(value), Some(next()?)), BinaryOp::Gt => (Some(next()?), None), BinaryOp::GtEq => (Some(value), None), BinaryOp::Lt => (None, Some(value)), BinaryOp::LtEq => (None, Some(next()?)), _ => (None, None) };
        if let Some(lo) = lo { lower = Some(lower.map_or(lo, |old: i64| old.max(lo))); }
        if let Some(hi) = hi { upper = Some(upper.map_or(hi, |old: i64| old.min(hi))); }
    }
    Ok(lower.zip(upper))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn block_tables_test_bounds_accept_reference_noncanonical_prefix_and_exclusive_comparisons() {
        assert_eq!(range(&[(BinaryOp::GtEq, Value::Utf8("00".into())), (BinaryOp::LtEq, Value::Utf8("264".into()))], block_index).unwrap(), Some((0, 101)));
        assert_eq!(range(&[(BinaryOp::Gt, Value::I64(10)), (BinaryOp::Lt, Value::I64(20))], integer).unwrap(), Some((11, 20)));
        assert_eq!(range(&[(BinaryOp::Gt, Value::I64(10))], integer).unwrap(), None);
        assert_eq!(range(&[(BinaryOp::Eq, Value::Null)], integer).unwrap(), None);
    }
}
