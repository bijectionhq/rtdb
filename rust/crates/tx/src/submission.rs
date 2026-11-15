use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext, evaluate};
use rtdb_plan::{Catalog, QualifiedTable, Relation, RelationNode, Schema, TableBinding, TemporalFilter};
use rtdb_types::{Anomaly, Result, Timestamp, Value};

use crate::{Documents, SourceTransaction, SqlArguments, SubmitOptions, TxOp};

struct SubmissionCatalog<'a>(&'a str);
impl Catalog for SubmissionCatalog<'_> {
    fn default_database(&self) -> &str { self.0 }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from([self.0.to_owned()]) }
    fn table(&self, _: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
    fn transaction_scoped(&self) -> bool { true }
}

pub fn prepare_submission(operations: Vec<TxOp>, options: SubmitOptions, database: &str, memory: &Arc<MemoryBudget>) -> Result<SourceTransaction> {
    let mut output = Vec::new();
    for operation in operations {
        let expanded = match &operation {
            TxOp::Sql { sql, args } => expand(sql, args.as_ref(), &options, database, memory)?,
            TxOp::SqlBytes { sql, arg_bytes } => {
                let args = if arg_bytes.as_slice().is_empty() { None } else { Some(crate::source::decode_arguments(arg_bytes.as_slice(), memory)?) };
                expand(sql, args.as_ref(), &options, database, memory)?
            }
            _ => None,
        };
        match expanded { Some(operations) => output.extend(operations), None => output.push(operation) }
    }
    SourceTransaction::new(output, options, memory)
}

fn expand(sql: &str, args: Option<&SqlArguments>, options: &SubmitOptions, database: &str, memory: &Arc<MemoryBudget>) -> Result<Option<Vec<TxOp>>> {
    let prepare = rtdb_sql::PrepareOptions { parameter_types: args.map(|args| args.types().iter().cloned().enumerate().collect()).unwrap_or_default(), ..Default::default() };
    let Some(plan) = rtdb_sql::prepare_static(sql, &SubmissionCatalog(database), &prepare)? else { return Ok(None); };
    if args.is_some_and(|args| args.row_count() == 0) { return Ok(None); }
    let count = plan.input.parameter_count().max(plan.valid_time.expressions().iter().map(|expr| {
        let mut count = 0; expr.visit(&mut |expr| { if let rtdb_plan::Expr::Parameter { index, .. } = expr { count = count.max(index + 1); } }); count
    }).max().unwrap_or(0));
    if args.is_none() && count > 0 { return Err(Anomaly::incorrect("xtdb/missing-arg", "Static SQL requires arguments")); }
    if args.is_some_and(|args| args.types().len() != count) { return Err(Anomaly::incorrect("xtdb.indexer/incorrect-sql-arg-count", "Static SQL argument count differs")); }
    let parameter_rows: Box<dyn Iterator<Item = &[Value]> + '_> = match args { Some(args) => Box::new(args.rows()), None => Box::new(std::iter::once(&[][..])) };
    let mut groups: Vec<((Option<Timestamp>, Option<Timestamp>), Vec<BTreeMap<String, Value>>)> = Vec::new();
    let mut reservations = Vec::new();
    for parameters in parameter_rows {
        let parameter_bytes = parameters.iter().try_fold(1024usize, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(overflow))?;
        let mut expression_count = 0usize;
        fn count(input: &Relation, total: &mut usize) { for expr in input.expressions() { expr.visit(&mut |_| *total += 1); } for child in input.children() { count(child, total); } }
        count(&plan.input, &mut expression_count);
        let bound = parameter_bytes.checked_add(sql.len().checked_mul(128).ok_or_else(overflow)?).and_then(|bytes| bytes.checked_mul(expression_count.checked_add(8)?)).ok_or_else(overflow)?;
        reservations.push(memory.reserve(bound)?);
        let context = EvalContext { parameters, current_time: Timestamp(0), timezone: &options.default_tz, outer_rows: &[] };
        let temporal = match &plan.valid_time {
            TemporalFilter::Default => (None, None),
            TemporalFilter::All => (Some(Timestamp::MIN), Some(Timestamp::MAX)),
            TemporalFilter::FromTo { from, to } => (Some(bound_value(&evaluate(from, &Schema::default(), &[], &context)?, Timestamp::MIN, &context)?), Some(bound_value(&evaluate(to, &Schema::default(), &[], &context)?, Timestamp::MAX, &context)?)),
            _ => return Err(Anomaly::incorrect("xtdb/sql-error", "Static PATCH requires ALL or FROM/TO valid time")),
        };
        for row in materialize(&plan.input, &context)? {
            let mut document: BTreeMap<_, _> = plan.input.schema().fields().iter().zip(row).map(|(field, value)| (field.name.clone(), value)).collect();
            let bounds = if plan.patch { temporal } else {
                let from = document.remove("_valid_from").filter(|value| !matches!(value, Value::Null | Value::Undefined)).map(|value| rtdb_expr::timestamp_micros(&value, &options.default_tz)).transpose()?;
                let to = document.remove("_valid_to").filter(|value| !matches!(value, Value::Null | Value::Undefined)).map(|value| rtdb_expr::timestamp_micros(&value, &options.default_tz)).transpose()?;
                (from, to)
            };
            document.get("_id").ok_or_else(|| Anomaly::incorrect("xtdb.tx/missing-id", "missing _id"))?.entity_id()?;
            if let Some((_, rows)) = groups.iter_mut().find(|(key, _)| *key == bounds) { rows.push(document); }
            else { groups.push((bounds, vec![document])); }
        }
    }
    let operations = groups.into_iter().map(|((valid_from, valid_to), documents)| {
        let documents = Documents::new(documents, memory)?;
        let table = plan.table.table.clone();
        Ok(if plan.patch { TxOp::PatchDocs { table, documents, valid_from, valid_to } } else { TxOp::PutDocs { table, documents, valid_from, valid_to } })
    }).collect::<Result<Vec<_>>>()?;
    Ok((!operations.is_empty()).then_some(operations))
}

fn materialize(input: &Relation, context: &EvalContext<'_>) -> Result<Vec<Vec<Value>>> {
    match input.node() {
        RelationNode::Values { rows } => rows.iter().map(|row| row.iter().map(|expr| evaluate(expr, &Schema::default(), &[], context)).collect()).collect(),
        RelationNode::Project { input, expressions } => materialize(input, context)?.iter().map(|row| expressions.iter().map(|expr| evaluate(&expr.expr, input.schema(), row, context)).collect()).collect(),
        _ => Err(Anomaly::fault("rtdb.tx/static-plan", "Static SQL source contains a data-dependent operator")),
    }
}
fn bound_value(value: &Value, fallback: Timestamp, context: &EvalContext<'_>) -> Result<Timestamp> {
    if matches!(value, Value::Null | Value::Undefined) { Ok(fallback) } else { rtdb_expr::timestamp_micros(value, context.timezone) }
}
fn overflow() -> Anomaly { Anomaly::incorrect("rtdb.tx/allocation-overflow", "Static transaction allocation exceeds address space") }
