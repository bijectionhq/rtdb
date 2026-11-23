use rtdb_types::StructFields;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::DataType;
use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext, evaluate};
use rtdb_plan::{Catalog, Expr, QualifiedTable, Relation, RelationNode, Schema, TableBinding, TemporalFilter};
use rtdb_types::{Anomaly, Result, Timestamp, Value};
use rtdb_sql::StaticMutationKind;

use crate::{ArrowDocuments, Documents, SourceTransaction, SqlArguments, SubmitOptions, TxOp};
use crate::source::Operation;
use crate::values::{BoundArgumentRow, canonical_argument_column, plain_scalar_field};

struct SubmissionCatalog<'a>(&'a str);
impl Catalog for SubmissionCatalog<'_> {
    fn default_database(&self) -> &str { self.0 }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from([self.0.to_owned()]) }
    fn table(&self, _: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
    fn transaction_scoped(&self) -> bool { true }
}

#[cfg(test)]
#[test]
fn hosted_capacity_sql_remains_an_ordered_operation() -> Result<()> {
    let sql = r#"INSERT INTO xt.hosted_capacity_declarations
        (_id, relation, "column", cap, predicate, writer_micros)
        VALUES ('partition', 'facts', 'partition', 3, '', 100)"#;
    let memory = MemoryBudget::new(1 << 24);
    let request = prepare_submission(
        vec![TxOp::Sql { sql: sql.into(), args: None }],
        SubmitOptions::default(), "application", &memory,
    )?;
    assert!(matches!(&request.operations[..], [Operation::Public(TxOp::Sql { .. })]));
    Ok(())
}

pub fn prepare_submission(operations: Vec<TxOp>, options: SubmitOptions, database: &str, memory: &Arc<MemoryBudget>) -> Result<SourceTransaction> {
    prepare_submission_with_sql_options(operations,options,database,memory,&rtdb_sql::PrepareOptions::default())
}

/// Expands static SQL using the submitting node's captured preparation settings.
/// Argument types and timezone are taken from each operation and its transaction options; no SQL setting is added to the durable envelope.
pub fn prepare_submission_with_sql_options(operations: Vec<TxOp>, options: SubmitOptions, database: &str, memory: &Arc<MemoryBudget>, sql_options: &rtdb_sql::PrepareOptions) -> Result<SourceTransaction> {
    let mut output = Vec::new();
    for operation in operations {
        let expanded = match &operation {
            TxOp::Sql { sql, args } => expand(sql, args.as_ref(), &options, database, memory, sql_options)?,
            TxOp::SqlBytes { sql, arg_bytes } => {
                let args = if arg_bytes.as_slice().is_empty() { None } else { Some(crate::source::decode_arguments(arg_bytes.as_slice(), memory)?) };
                expand(sql, args.as_ref(), &options, database, memory, sql_options)?
            }
            _ => None,
        };
        match expanded { Some(operations) => output.extend(operations), None => output.push(Operation::Public(operation)) }
    }
    SourceTransaction::from_operations(output, options, memory)?.with_bounded_retry()
}

fn expand(sql: &str, args: Option<&SqlArguments>, options: &SubmitOptions, database: &str, memory: &Arc<MemoryBudget>, sql_options: &rtdb_sql::PrepareOptions) -> Result<Option<Vec<Operation>>> {
    let scope = rtdb_expr::EvaluationScope::new(memory)?;
    let _syntax = memory.reserve(rtdb_sql::statement_syntax_estimate(sql.len()).ok_or_else(overflow)?)?;
    let prepare = rtdb_sql::PrepareOptions { parameter_types: args.map(|args| args.execution_types().enumerate().collect()).unwrap_or_default(), default_timezone: options.default_tz.clone(), ..sql_options.clone() };
    let Some(plan) = rtdb_sql::prepare_static_in(sql, &SubmissionCatalog(database), &prepare, scope.memory_budget())? else { return Ok(None); };
    if plan.table.table.schema == "xt" && plan.table.table.table == crate::hosted_control::HOSTED_CAPACITY_DECLARATIONS {
        return Ok(None);
    }
    fn resolve_only(expr: &rtdb_plan::Expr) -> bool {
        let mut requires_clock = false;
        expr.visit(&mut |expr| if let rtdb_plan::Expr::Cast { expr, target } = expr { requires_clock |= target.requires_current_time(&expr.data_type()); });
        requires_clock || expr.is_volatile() || rtdb_expr::requires_pg_catalog(expr)
    }
    fn needs_resolver(input: &Relation) -> bool { input.expressions().iter().any(|expr| resolve_only(expr)) || input.children().iter().any(|input| needs_resolver(input)) }
    let valid_time = plan.valid_time();
    if needs_resolver(&plan.input) || valid_time.expressions().iter().any(|expr| resolve_only(expr)) { return Ok(None); }
    if args.is_some_and(|args| args.row_count() == 0) { return Ok(None); }
    let parameter_count = plan.input.parameter_count().max(valid_time.expressions().iter().map(|expr| {
        let mut count = 0; expr.visit(&mut |expr| { if let rtdb_plan::Expr::Parameter { index, .. } = expr { count = count.max(index + 1); } }); count
    }).max().unwrap_or(0));
    if args.map_or(0, |args| args.types().len()) < parameter_count { return Ok(None); }
    if matches!(plan.kind, StaticMutationKind::Put) && let Some(pages) = args.and_then(SqlArguments::uniform_arrow_pages) {
        let projected = if plan.input.schema().fields().iter().any(|field| valid_time_column(&field.name)) {
            projected_timed_puts(&plan.input, &pages, &plan.table.table, &options.default_tz, &scope, memory)
        } else {
            projected_arrow_documents(&plan.input, &pages, memory).map(|documents| documents.map(|documents| {
                vec![Operation::Public(TxOp::PutArrowDocs { table: plan.table.table.clone(), documents })]
            }))
        };
        match projected {
            Ok(Some(operations)) => return Ok(Some(operations)),
            Ok(None) => {}
            Err(error) if error.code == "rtdb/memory-budget-exceeded" => {}
            Err(error) => return Err(error),
        }
    }
    let parameter_rows: Box<dyn Iterator<Item = Result<BoundArgumentRow<'_>>> + '_> = match args {
        Some(args) => Box::new(args.indexed_bound_rows().map(|row| row.map(|(_, row)| row))),
        None => Box::new(std::iter::once(Ok(BoundArgumentRow::Values { values: &[], types: None }))),
    };
    let mut groups: Vec<((Option<Timestamp>, Option<Timestamp>), Vec<ArrowDocuments>)> = Vec::new();
    let mut group_of: BTreeMap<(Option<Timestamp>, Option<Timestamp>), usize> = Default::default();
    let mut inventory = memory.reserve(4096)?;
    let columns = plan.input.schema().fields().iter().map(|field| field.name.as_str())
        .filter(|name| matches!(&plan.kind, StaticMutationKind::Patch(_)) || !valid_time_column(name))
        .map(str::to_owned).collect::<Vec<_>>();
    let mut pending_bounds = (None, None);
    let mut pending = Vec::with_capacity(128);
    let mut pending_charge = memory.reserve(4096)?;
    let mut flush = |bounds, pending: &mut Vec<_>, pending_charge: &mut Arc<rtdb_columnar::MemoryReservation>| -> Result<()> {
        if pending.is_empty() { return Ok(()); }
        let rows = std::mem::replace(pending, Vec::with_capacity(128));
        let documents = Documents::with_column_order(rows, columns.clone(), memory)?;
        Arc::get_mut(pending_charge).expect("Static document buffer owns its reservation").shrink_to(4096)?;
        let page = crate::source::owned_row_documents(&documents, memory)?;
        let lease = Arc::get_mut(&mut inventory).expect("Static mutation owns its document groups");
        lease.grow_to(lease.bytes().checked_add(512).ok_or_else(overflow)?)?;
        match group_of.entry(bounds) {
            std::collections::btree_map::Entry::Occupied(group) => groups[*group.get()].1.push(page),
            std::collections::btree_map::Entry::Vacant(group) => { group.insert(groups.len()); groups.push((bounds, vec![page])); }
        }
        Ok(())
    };
    for parameters in parameter_rows {
        let row_scope = scope.scratch();
        let arrow;
        let values;
        let parameters = match parameters? {
            BoundArgumentRow::Arrow { batch, row, types } => {
                arrow = rtdb_query::BoundParameters::from_arrow(batch.clone(), row, types.to_vec(), memory)?;
                arrow.evaluator()
            }
            BoundArgumentRow::Values { values: input, .. } => {
                let input = &input[..parameter_count];
                let bytes = input.iter().try_fold(1024usize, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(overflow))?;
                row_scope.reserve(bytes)?;
                values = input.iter().cloned().map(|mut value| {
                    rtdb_columnar::admit_execution_value(&mut value);
                    value
                }).collect::<Vec<_>>();
                rtdb_expr::Parameters::Values(&values)
            }
        };
        let context = EvalContext { parameters, parameter_types: None, current_time: Timestamp(0).into(), timezone: &options.default_tz, outer_rows: &[], snapshot_token: None, allocation: Some(&row_scope), catalog: None };
        let temporal = match valid_time {
            TemporalFilter::Default => (None, None),
            TemporalFilter::All => (Some(Timestamp::MIN), Some(Timestamp::MAX)),
            TemporalFilter::FromTo { from, to } => (patch_bound_value(from, Timestamp::MIN, &context)?, patch_bound_value(to, Timestamp::MAX, &context)?),
            _ => return Err(Anomaly::incorrect("xtdb/sql-error", "Static PATCH requires ALL or FROM/TO valid time")),
        };
        materialize(&plan.input, &context, &mut |row| {
            let mut document: StructFields<_> = plan.input.schema().fields().iter().zip(row).map(|(field, value)| (field.name.clone(), value)).collect();
            let bounds = if matches!(&plan.kind, StaticMutationKind::Patch(_)) { temporal } else {
                let from = insert_bound(&document.remove("_valid_from").unwrap_or(Value::Null), &options.default_tz)?;
                let to = insert_bound(&document.remove("_valid_to").unwrap_or(Value::Null), &options.default_tz)?;
                (from, to)
            };
            for value in document.values_mut() { infer_document_decimals(value)?; }
            if pending.len() == 128 || pending_bounds != bounds {
                flush(pending_bounds, &mut pending, &mut pending_charge)?;
                pending_bounds = bounds;
            }
            let lease = Arc::get_mut(&mut pending_charge).expect("Static document buffer owns its reservation");
            lease.grow_to(lease.bytes().checked_add(Value::document_retained_bytes(&document)?).ok_or_else(overflow)?)?;
            pending.push(document);
            Ok(())
        })?;
    }
    flush(pending_bounds, &mut pending, &mut pending_charge)?;
    let operations = groups.into_iter().map(|((valid_from, valid_to), pages)| {
        let documents = crate::source::coalesce_arrow_documents(pages, memory)?;
        let table = plan.table.table.clone();
        Ok(match &plan.kind {
            StaticMutationKind::Patch(_) => {
                let reader = documents.id_reader()?;
                let iids = (0..documents.array().len()).map(|row| documents.iid(row, reader.as_ref(), memory)).collect::<Result<_>>()?;
                Operation::PatchArrowIids { table, documents, iids, valid_from, valid_to }
            }
            StaticMutationKind::Put => Operation::PutReplayArrow { table, documents, iids: None, valid_from, valid_to },
        })
    }).collect::<Result<Vec<_>>>()?;
    Ok((!operations.is_empty()).then_some(operations))
}

fn valid_time_column(name: &str) -> bool { matches!(name, "_valid_from" | "_valid_to") }

fn projected_timed_puts(
    input: &Relation, pages: &[&RecordBatch], table: &rtdb_types::TableRef, timezone: &str,
    scope: &rtdb_expr::EvaluationScope, memory: &Arc<MemoryBudget>,
) -> Result<Option<Vec<Operation>>> {
    let Some(first) = pages.first() else { return Ok(None); };
    let RelationNode::Rename { input: values, expressions, .. } = input.node() else { return Ok(None); };
    let RelationNode::Values { rows } = values.node() else { return Ok(None); };
    let [row] = rows.as_slice() else { return Ok(None); };
    let schema = first.schema_ref();
    if !schema.fields().iter().all(|field| plain_scalar_field(field) || field.metadata().is_empty()
        && matches!(field.data_type(), DataType::Date32 | DataType::Date64 | DataType::Timestamp(..))) {
        return Ok(None);
    }

    let row_count = pages.iter().try_fold(0usize, |len, page| len.checked_add(page.num_rows()).ok_or_else(overflow))?;
    let input_fields = rtdb_format::ipc::schema_translation_bound(schema)?;
    let output_fields = input.schema().fields().iter().try_fold(4096usize, |bytes, field| {
        bytes.checked_add(field.name.len().checked_mul(4).and_then(|name| name.checked_add(2048)).ok_or_else(overflow)?).ok_or_else(overflow)
    })?;
    let mut group_reservation = memory.reserve(row_count.checked_mul(512)
        .and_then(|bytes| bytes.checked_add(input_fields))
        .and_then(|bytes| bytes.checked_add(output_fields)).ok_or_else(overflow)?)?;
    let mut data = Vec::new();
    let mut times = Vec::with_capacity(2);
    let mut required = BTreeSet::new();
    for expression in expressions {
        let Expr::Column { id, .. } = expression.expr else { return Ok(None); };
        let Some(expr) = values.schema().position(id).and_then(|position| row.get(position)) else { return Ok(None); };
        let name = expression.field.name.as_str();
        if valid_time_column(name) {
            times.push((usize::from(name == "_valid_to"), expr));
            expr.visit(&mut |expr| if let Expr::Parameter { index, .. } = expr { required.insert(*index); });
        } else {
            let Expr::Parameter { index, .. } = expr else { return Ok(None); };
            let Some(field) = schema.fields().get(*index).filter(|field| plain_scalar_field(field)) else { return Ok(None); };
            data.push((*index, field.as_ref().clone().with_name(name)));
        }
    }
    let Some((id, _)) = data.iter().find(|(_, field)| field.name() == "_id") else { return Ok(None); };
    if pages.iter().any(|page| page.column(*id).logical_null_count() != 0) { return Ok(None); }
    let readers = required.into_iter().map(|index| {
        Ok((index, rtdb_columnar::ColumnReader::new(schema.field(index))?))
    }).collect::<Result<Vec<_>>>()?;

    let mut groups: Vec<(_, Vec<_>)> = Vec::new();
    let mut group_of = BTreeMap::new();
    for (page_index, page) in pages.iter().enumerate() {
        for row in 0..page.num_rows() {
            let row_scope = scope.scratch();
            let mut parameters = vec![Value::Null; schema.fields().len()];
            for (index, reader) in &readers {
                let array = page.column(*index).as_ref();
                row_scope.reserve(rtdb_columnar::estimate_read_cell_bytes(schema.field(*index), array, row)?)?;
                let mut value = reader.read(array, row)?;
                rtdb_columnar::admit_execution_value(&mut value);
                parameters[*index] = value;
            }
            let context = EvalContext { parameters: rtdb_expr::Parameters::Values(&parameters), parameter_types: None, current_time: Timestamp(0).into(),
                timezone, outer_rows: &[], snapshot_token: None, allocation: Some(&row_scope), catalog: None };
            let evaluate_bounds = || -> Result<_> {
                let mut endpoints = [Value::Null, Value::Null];
                for &(endpoint, expression) in &times {
                    endpoints[endpoint] = evaluate(expression, &Schema::default(), &[], &context)?;
                }
                Ok((insert_bound(&endpoints[0], timezone)?, insert_bound(&endpoints[1], timezone)?))
            };
            // The row path orders evaluation errors against admission of earlier documents.
            let Ok(bounds) = evaluate_bounds() else { return Ok(None); };
            let group = *group_of.entry(bounds).or_insert_with(|| {
                groups.push((bounds, Vec::new()));
                groups.len() - 1
            });
            groups[group].1.push(Some((page_index, row)));
        }
    }

    let table_bytes = table.schema.len().checked_add(table.table.len()).and_then(|bytes| bytes.checked_mul(groups.len())).ok_or_else(overflow)?;
    let bound = group_reservation.bytes().checked_add(table_bytes).ok_or_else(overflow)?;
    Arc::get_mut(&mut group_reservation).expect("timed INSERT owns its group reservation").grow_to(bound)?;
    let schema = Arc::new(arrow_schema::Schema::new(data.iter().map(|(_, field)| field.clone()).collect::<Vec<_>>()));
    let mut operations = Vec::with_capacity(groups.len());
    for ((valid_from, valid_to), cells) in groups {
        // The row codec can place an initial null before this scalar when a subsequent put adds another type.
        let (page, row) = cells[0].expect("a valid-time group has a first selected cell");
        if data.iter().any(|(index, _)| {
            let array = pages[page].column(*index);
            array.data_type() == &DataType::Null || array.is_null(row)
        }) {
            return Ok(None);
        }
        let columns = data.iter().map(|(index, field)| selected_argument_column(pages, *index, field, &cells, memory)).collect::<Result<Vec<_>>>()?;
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).map_err(|error| Anomaly::fault("rtdb.tx/static-plan", error.to_string()))?;
        let Some(documents) = ArrowDocuments::from_record_batch(&batch, memory)? else { return Ok(None); };
        let reader = documents.id_reader()?;
        let iids = (0..batch.num_rows()).map(|row| documents.iid(row, reader.as_ref(), memory)).collect::<Result<Vec<_>>>();
        let Ok(iids) = iids else { return Ok(None); };
        operations.push(Operation::PutReplayArrow { table: table.clone(), documents, iids: Some(iids), valid_from, valid_to });
    }
    Ok(Some(operations))
}

fn selected_argument_column(
    pages: &[&RecordBatch], index: usize, field: &arrow_schema::Field, cells: &[Option<(usize, usize)>], memory: &Arc<MemoryBudget>,
) -> Result<ArrayRef> {
    let base = pages.len().checked_mul(128).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
    let bound = cells.iter().flatten().try_fold(base, |bytes, &(page, row)| {
        bytes.checked_add(rtdb_columnar::estimate_copy_cell_bytes(field, pages[page].column(index).as_ref(), row)?)
            .ok_or_else(overflow)
    })?;
    let mut reservation = memory.reserve(bound)?;
    let sources = pages.iter().map(|page| page.column(index).as_ref()).collect::<Vec<_>>();
    let column = rtdb_columnar::copy_nullable_cells(&sources, cells)?;
    drop(sources);
    Arc::get_mut(&mut reservation).expect("argument selection owns its reservation").shrink_to(column.get_array_memory_size())?;
    rtdb_columnar::retain_array(column, &reservation)
}

fn projected_arrow_documents(input: &Relation, pages: &[&RecordBatch], memory: &Arc<MemoryBudget>) -> Result<Option<ArrowDocuments>> {
    let Some(first) = pages.first() else { return Ok(None); };
    let RelationNode::Rename { input: values, expressions, .. } = input.node() else { return Ok(None); };
    let RelationNode::Values { rows } = values.node() else { return Ok(None); };
    let [row] = rows.as_slice() else { return Ok(None); };
    if expressions.iter().any(|expr| matches!(expr.field.name.as_str(), "_valid_from" | "_valid_to")) {
        return Ok(None);
    }

    let schema = first.schema_ref();
    if !schema.fields().iter().all(|field| plain_scalar_field(field)) {
        return Ok(None);
    }

    let projection = || expressions.iter().map(|expr| {
        let Expr::Column { id, .. } = expr.expr else { return None; };
        let Expr::Parameter { index, .. } = row.get(values.schema().position(id)?)? else { return None; };
        let field = schema.fields().get(*index)?;
        Some((expr.field.name.as_str(), field, *index))
    });
    let mut bound = 4096usize;
    let mut has_id = false;
    for column in projection() {
        let Some((name, field, index)) = column else { return Ok(None); };
        if name == "_id" {
            if pages.iter().any(|page| page.column(index).logical_null_count() != 0) { return Ok(None); }
            has_id = true;
        }
        bound = field.size().checked_add(name.len()).and_then(|bytes| bytes.checked_add(256))
            .and_then(|bytes| bytes.checked_mul(4)).and_then(|bytes| bytes.checked_add(bound)).ok_or_else(overflow)?;
    }
    if !has_id { return Ok(None); }
    let _schema = memory.reserve(bound)?;
    let mut fields = Vec::with_capacity(expressions.len());
    let mut columns = Vec::with_capacity(expressions.len());
    for (name, field, index) in projection().flatten() {
        fields.push(field.as_ref().clone().with_name(name));
        columns.push(canonical_argument_column(pages, index, memory)?);
    }
    let projection = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns)
        .map_err(|error| Anomaly::fault("rtdb.tx/static-plan", error.to_string()))?;
    ArrowDocuments::from_record_batch(&projection, memory)
}

fn infer_document_decimals(value: &mut Value) -> Result<()> {
    match value {
        Value::Decimal(decimal) => *decimal = rtdb_expr::infer_decimal(decimal)?,
        Value::List(values) | Value::Set(values) | Value::FixedSizeList(values) => for value in values { infer_document_decimals(value)?; },
        Value::Struct(fields) => for value in fields.values_mut() { infer_document_decimals(value)?; },
        Value::Map { entries, .. } => for (key, value) in entries { infer_document_decimals(key)?; infer_document_decimals(value)?; },
        _ => {}
    }
    Ok(())
}

fn materialize(input: &Relation, context: &EvalContext<'_>, emit: &mut dyn FnMut(Vec<Value>) -> Result<()>) -> Result<()> {
    match input.node() {
        RelationNode::Values { rows } => {
            for row in rows {
                let values = row.iter().map(|expr| evaluate(expr, &Schema::default(), &[], context)).collect::<Result<_>>()?;
                emit(values)?;
            }
            Ok(())
        }
        RelationNode::Project { input, expressions } | RelationNode::Rename { input, expressions, .. } => {
            materialize(input, context, &mut |row| {
                let values = expressions.iter().map(|expr| evaluate(&expr.expr, input.schema(), &row, context)).collect::<Result<_>>()?;
                emit(values)
            })
        }
        _ => Err(Anomaly::fault("rtdb.tx/static-plan", "Static SQL source contains a data-dependent operator")),
    }
}
fn insert_bound(value: &Value, timezone: &str) -> Result<Option<Timestamp>> {
    if matches!(value, Value::Null | Value::Undefined) { Ok(None) }
    else { rtdb_expr::timestamp_micros(value, timezone).map(Some) }
}
fn patch_bound_value(expression: &rtdb_plan::Expr, fallback: Timestamp, context: &EvalContext<'_>) -> Result<Option<Timestamp>> {
    if matches!(expression,rtdb_plan::Expr::Literal(Value::Null | Value::Bool(false))) { return Ok(Some(fallback)); }
    let value = evaluate(expression, &Schema::default(), &[], context)?;
    if matches!(value, Value::Null | Value::Undefined) { Ok(None) }
    else { rtdb_expr::query_clock_instant(&value, context.timezone)?.as_micros().map(Some) }
}
fn overflow() -> Anomaly { Anomaly::incorrect("rtdb.tx/allocation-overflow", "Static transaction allocation exceeds address space") }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_insert_keeps_payload_columns_in_arrow_when_row_time_expressions_differ() {
        use arrow_array::{Int64Array, StringArray};
        use arrow_schema::Field;
        use rtdb_columnar::OwnedBatch;

        let memory = MemoryBudget::new(1 << 24);
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("data", DataType::Utf8, true),
            Field::new("start", DataType::Utf8, true),
            Field::new("end", DataType::Utf8, true),
        ]));
        let page = |ids: Vec<i64>, data: Vec<Option<&str>>, from: Vec<Option<&str>>, to: Vec<Option<&str>>| {
            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(data)),
                Arc::new(StringArray::from(from)),
                Arc::new(StringArray::from(to)),
            ]).unwrap();
            SqlArguments::from_arrow(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap()
        };
        let start = "2020-01-01T00:00:00Z";
        let end = "2021-01-01T00:00:00Z";
        let mut arguments = page(vec![1, 2], vec![Some("one"), Some("two")], vec![Some(start), None], vec![Some(end), None]);
        arguments.append(page(vec![3, 4], vec![None, None], vec![Some(start), None], vec![Some(end), None])).unwrap();
        let rows = arguments.rows().map(|row| row.unwrap().to_vec()).collect();
        let submit = |arguments| prepare_submission(vec![TxOp::Sql {
            sql: "INSERT INTO docs (_id, data, _valid_from, _valid_to) VALUES ($1, $2, CAST($3 AS TIMESTAMPTZ), CAST($4 AS TIMESTAMPTZ))".into(),
            args: Some(arguments),
        }], SubmitOptions::default(), "xtdb", &memory).unwrap();
        let ordinary = submit(SqlArguments::new(arguments.types().to_vec(), rows, &memory).unwrap());
        let projected = submit(arguments);

        let [Operation::PutReplayArrow { documents: bounded, valid_from: Some(_), valid_to: Some(_), .. },
            Operation::PutReplayArrow { documents: defaulted, valid_from: None, valid_to: None, .. }] = projected.operations.as_slice()
            else { panic!("Expected two Arrow writes in first-seen valid-time order"); };
        assert_eq!((bounded.array().len(), defaulted.array().len()), (2, 2));
        assert_eq!(projected.encode().unwrap().as_slice(), ordinary.encode().unwrap().as_slice());

        let null_first = submit(page(vec![5, 6], vec![None, Some("six")], vec![Some(start); 2], vec![Some(end); 2]));
        assert!(matches!(null_first.operations.as_slice(), [Operation::PutReplayArrow { .. }]));
    }

    #[test]
    fn static_insert_projects_arrow_arguments_with_the_same_source_encoding_as_rows() {
        use arrow_array::{StringArray, UInt64Array};
        use arrow_schema::Field;
        use rtdb_columnar::OwnedBatch;

        let memory = MemoryBudget::new(1 << 24);
        let labels = Arc::new(StringArray::from(vec!["label", "other"]));
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("bound_label", DataType::Utf8, true),
            Field::new("bound_id", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![labels, Arc::new(UInt64Array::from(vec![1, u64::MAX]))]).unwrap();
        let args = SqlArguments::from_arrow(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();
        let submit = |arguments| prepare_submission(vec![TxOp::Sql {
            sql: "INSERT INTO docs (_id, label) VALUES ($2, $1)".into(), args: Some(arguments),
        }], SubmitOptions::default(), "xtdb", &memory).unwrap();
        let rows = args.rows().map(|row| row.unwrap().to_vec()).collect();
        let boxed = submit(SqlArguments::new(args.types().to_vec(), rows, &memory).unwrap());
        let shared = args.uniform_arrow_pages().unwrap()[0].column(0).clone();

        let projected = submit(args);

        let crate::source::Operation::Public(TxOp::PutArrowDocs { documents, .. }) = &projected.operations[0]
            else { panic!("Expected Arrow documents") };
        assert!(Arc::ptr_eq(documents.array().column(1), &shared));
        assert_eq!(projected.encode().unwrap().as_slice(), boxed.encode().unwrap().as_slice());
    }

    #[test]
    fn static_insert_projects_all_uniform_arrow_argument_pages_in_order() {
        use arrow_array::{Int64Array, StringArray};
        use arrow_schema::Field;
        use rtdb_columnar::OwnedBatch;

        let memory = MemoryBudget::new(1 << 24);
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));
        let upload = |ids: Vec<i64>, labels: Vec<Option<&str>>| {
            let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(ids)), Arc::new(StringArray::from(labels))];
            let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
            SqlArguments::from_arrow(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap()
        };
        let mut args = upload(vec![1, 2], vec![Some("first"), None]);
        args.append(upload(vec![], vec![])).unwrap();
        args.append(upload(vec![3], vec![Some("last")])).unwrap();
        let rows = args.rows().map(|row| row.unwrap().to_vec()).collect();
        let submit = |arguments| prepare_submission(vec![TxOp::Sql {
            sql: "INSERT INTO docs (_id, label) VALUES (?, ?)".into(), args: Some(arguments),
        }], SubmitOptions::default(), "xtdb", &memory).unwrap();
        let boxed = submit(SqlArguments::new(args.types().to_vec(), rows, &memory).unwrap());

        let projected = submit(args);

        let [crate::source::Operation::Public(TxOp::PutArrowDocs { .. })] = projected.operations.as_slice()
            else { panic!("Expected one Arrow write in upload order") };
        assert_eq!(projected.encode().unwrap().as_slice(), boxed.encode().unwrap().as_slice());
    }

    #[test]
    fn static_insert_canonicalizes_arrow_padding_and_preserves_missing_id_errors() {
        use arrow_array::{BooleanArray, Int64Array};
        use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};
        use arrow_schema::Field;
        use rtdb_columnar::OwnedBatch;

        let memory = MemoryBudget::new(1 << 24);
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("flag", DataType::Boolean, false),
            Field::new("number", DataType::Int64, true),
        ]));
        let flags = Arc::new(BooleanArray::new(BooleanBuffer::new(Buffer::from_vec(vec![0xff_u8]), 0, 2), None));
        let validity = NullBuffer::new(BooleanBuffer::new(Buffer::from_vec(vec![0xfd_u8]), 0, 2));
        let numbers = Arc::new(Int64Array::new(ScalarBuffer::from(vec![17, 999]), Some(validity)));
        let submit = |id: Int64Array| {
            let columns: Vec<ArrayRef> = vec![Arc::new(id), flags.clone(), numbers.clone()];
            let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
            let args = SqlArguments::from_arrow(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();
            let rows = args.rows().map(|row| row.unwrap().to_vec()).collect();
            let submit = |arguments| prepare_submission(vec![TxOp::Sql {
                sql: "INSERT INTO docs (_id, flag, n) VALUES (?, ?, ?)".into(), args: Some(arguments),
            }], SubmitOptions::default(), "xtdb", &memory).unwrap();
            let boxed = submit(SqlArguments::new(args.types().to_vec(), rows, &memory).unwrap());
            (submit(args), boxed)
        };
        let (projected, boxed) = submit(Int64Array::from(vec![1, 2]));
        assert!(matches!(projected.operations[0], crate::source::Operation::Public(TxOp::PutArrowDocs { .. })));
        assert_eq!(projected.encode().unwrap().as_slice(), boxed.encode().unwrap().as_slice());

        let (projected, boxed) = submit(Int64Array::from(vec![None::<i64>, None]));
        let error = projected.encode().unwrap_err();
        let expected = boxed.encode().unwrap_err();
        assert_eq!(error.code, "missing-id");
        assert_eq!(error.context, expected.context);
    }

    fn decimal(coefficient: i128, precision: i32, scale: i32) -> Value {
        Value::Decimal(rtdb_types::DecimalValue::new(coefficient.to_be_bytes().to_vec(), precision, scale, 128).unwrap())
    }

    fn expanded(value: Value, memory: &Arc<MemoryBudget>) -> SourceTransaction {
        let args = SqlArguments::new(vec![value.logical_type()], vec![vec![value]], memory).unwrap();
        prepare_submission(vec![TxOp::Sql { sql: "INSERT INTO docs (_id, v) VALUES (1, ?)".into(), args: Some(args) }], SubmitOptions::default(), "xtdb", memory).unwrap()
    }

    fn document(source: &SourceTransaction, memory: &Arc<MemoryBudget>) -> StructFields<Value> {
        let Operation::PutReplayArrow { documents, .. } = &source.operations[0] else { panic!("Expected static Arrow documents") };
        documents.to_rows(false, memory).unwrap().rows()[0].clone()
    }

    #[test]
    fn static_unsigned_arguments_become_signed_documents_before_source_encoding() {
        let memory = MemoryBudget::new(1 << 24);
        let args = SqlArguments::new(vec![rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::U64)], vec![vec![Value::U64(u64::MAX)]], &memory).unwrap();
        let source = prepare_submission(vec![TxOp::Sql { sql: "INSERT INTO docs (_id, v) VALUES (1, ?)".into(), args: Some(args.clone()) }], SubmitOptions::default(), "xtdb", &memory).unwrap();
        assert_eq!(document(&source, &memory)["v"], Value::I64(-1));
        assert_eq!(&*args.rows().next().unwrap().unwrap(), &[Value::U64(u64::MAX)]);
        assert_eq!(args.types(), &[rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::U64)]);
        let encoded = source.encode().unwrap();
        let message = rtdb_format::envelope::SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
        let replay = SourceTransaction::decode(&message, &memory).unwrap();
        let crate::source::Operation::PutReplayArrow { documents, .. } = &replay.operations[0] else { panic!("Expected decoded static documents") };
        assert_eq!(documents.to_rows(true, &memory).unwrap().rows()[0]["v"], Value::I64(-1));
    }

    #[test]
    fn static_parameter_documents_infer_decimal_metadata_from_coefficients() {
        let memory = MemoryBudget::new(1 << 24);
        for precision in [i32::MIN, -1, 0, 1, 256, i32::MAX] {
            let source = expanded(decimal(18, precision, i32::MAX), &memory);
            assert_eq!(document(&source, &memory)["v"], decimal(18, 32, i32::MAX));
            let encoded = source.encode().unwrap();
            let message = rtdb_format::envelope::SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
            let replay = SourceTransaction::decode(&message, &memory).unwrap();
            let crate::source::Operation::PutReplayArrow { documents, .. } = &replay.operations[0] else { panic!("Expected Arrow replay of static documents") };
            assert_eq!(documents.to_rows(true, &memory).unwrap().rows()[0]["v"], decimal(18, 32, i32::MAX));
        }
        let source = expanded(decimal(i128::MAX, -1, i32::MIN), &memory);
        let Value::Decimal(value) = &document(&source, &memory)["v"] else { panic!("Expected decimal") };
        assert_eq!((value.precision, value.scale, value.bit_width), (64, i32::MIN, 256));
        assert_eq!(&value.unscaled[16..], &i128::MAX.to_be_bytes());
        drop(source);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn static_document_inference_visits_every_nested_decimal_cell() {
        let memory = MemoryBudget::new(1 << 24);
        let nested = |value: Value| Value::Struct(StructFields::from([
            ("list".into(), Value::List(vec![value.clone(), Value::Null])),
            ("set".into(), Value::Set(vec![value.clone()])),
            ("fixed".into(), Value::FixedSizeList(vec![value.clone()])),
            ("map".into(), Value::Map { entries: vec![(value.clone(), value.clone())], sorted: false }),
            ("struct".into(), Value::Struct(StructFields::from([("child".into(), value)]))),
        ]));
        let source = expanded(nested(decimal(18, -1, 129)), &memory);
        assert_eq!(document(&source, &memory)["v"], nested(decimal(18, 32, 129)));
    }

    #[test]
    fn static_document_inference_rejects_coefficients_above_sixty_four_digits_without_retaining_scratch() {
        let memory = MemoryBudget::new(1 << 24);
        let value = Value::Decimal(rtdb_types::DecimalValue::new(vec![127; 32], i32::MAX, i32::MIN, 256).unwrap());
        let args = SqlArguments::new(vec![value.logical_type()], vec![vec![value]], &memory).unwrap();
        let error = prepare_submission(vec![TxOp::Sql { sql: "INSERT INTO docs (_id, v) VALUES (1, ?)".into(), args: Some(args) }], SubmitOptions::default(), "xtdb", &memory).err().unwrap();
        assert_eq!(error.category, rtdb_types::AnomalyCategory::Unsupported);
        assert_eq!(error.code, "");
        assert_eq!(error.message.as_deref(), Some("Unsupported precision: 77"));
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[cfg(test)]
mod argument_tests {
    use super::*;
    use crate::source::Operation;

    #[test]
    fn static_expansion_requires_referenced_positions_and_ignores_unused_arguments() {
        let memory = MemoryBudget::new(1 << 25);
        let sql = "INSERT INTO docs (_id, v) VALUES (?, ?)";
        let batch = |row: Vec<Value>| SqlArguments::new(row.iter().map(Value::logical_type).collect(), vec![row], &memory).unwrap();
        let submit = |args| prepare_submission(vec![TxOp::Sql { sql: sql.into(), args }], SubmitOptions::default(), "xtdb", &memory).unwrap();

        for args in [None, Some(batch(vec![Value::I64(3)]))] {
            let source = submit(args);
            assert!(matches!(&source.operations[..], [Operation::Public(TxOp::Sql { .. })]));
        }
        let source = submit(Some(batch(vec![Value::I64(3), Value::I64(4), Value::I64(5)])));
        let [Operation::PutReplayArrow { documents, .. }] = &source.operations[..] else { panic!("expanded Arrow documents") };
        assert_eq!(documents.to_rows(false, &memory).unwrap().rows(), &[StructFields::from([("_id".into(), Value::I64(3)), ("v".into(), Value::I64(4))])]);
    }
}

#[cfg(test)]
mod series_tests {
    use super::*;

    #[test]
    fn computed_series_sql_remains_on_the_resolution_path_for_each_endpoint_policy() {
        for end_exclusive in [false,true] {
            let memory = MemoryBudget::new(1 << 26);
            let options = rtdb_sql::PrepareOptions { generate_series_end_exclusive: end_exclusive, ..Default::default() };
            let request = prepare_submission_with_sql_options(vec![TxOp::Sql { sql: "INSERT INTO docs (_id,v) VALUES (1,GENERATE_SERIES(1,3))".into(),args: None }],SubmitOptions::default(),"xtdb",&memory,&options).unwrap();
            let [crate::source::Operation::Public(TxOp::Sql { sql, args })] = request.operations.as_slice() else { panic!("SQL source") };
            assert_eq!(sql,"INSERT INTO docs (_id,v) VALUES (1,GENERATE_SERIES(1,3))");
            assert!(args.is_none());
        }
    }
}

#[cfg(test)]
mod patch_endpoint_tests {
    use super::*;

    fn bounds(source: &SourceTransaction) -> (Option<Timestamp>,Option<Timestamp>) {
        let [Operation::PatchArrowIids { valid_from,valid_to,.. }] = source.operations.as_slice() else { panic!("static patch") };
        (*valid_from,*valid_to)
    }

    #[test]
    fn static_patch_distinguishes_literal_null_sentinels_from_null_parameter_defaults() {
        let memory = MemoryBudget::new(1 << 26);
        let source = prepare_submission(vec![TxOp::Sql { sql: "PATCH INTO docs FOR VALID_TIME FROM NULL TO NULL RECORDS {_id: 1}".into(),args: None }],SubmitOptions::default(),"xtdb",&memory).unwrap();
        assert_eq!(bounds(&source),(Some(Timestamp::MIN),Some(Timestamp::MAX)));
        drop(source);
        let args = SqlArguments::new(vec![rtdb_types::LogicalType::Null,rtdb_types::LogicalType::Null],vec![vec![Value::Null,Value::Null]],&memory).unwrap();
        let source = prepare_submission(vec![TxOp::Sql { sql: "PATCH INTO docs FOR VALID_TIME FROM $1 TO $2 RECORDS {_id: 1}".into(),args: Some(args) }],SubmitOptions::default(),"xtdb",&memory).unwrap();
        assert_eq!(bounds(&source),(None,None));
        drop(source);assert_eq!(memory.used_bytes(),0);
    }

    #[test]
    fn static_patch_false_literals_are_sentinels_but_false_parameters_are_not() {
        let memory = MemoryBudget::new(1 << 26);
        for clause in ["FROM FALSE", "FROM FALSE TO FALSE", "FROM NULL TO FALSE"] {
            let source = prepare_submission(vec![TxOp::Sql { sql: format!("PATCH INTO docs FOR VALID_TIME {clause} RECORDS {{_id: 1}}"),args: None }],SubmitOptions::default(),"xtdb",&memory).unwrap();
            assert_eq!(bounds(&source),(Some(Timestamp::MIN),Some(Timestamp::MAX)));
            drop(source);
        }
        for clause in ["FROM $1", "FROM NULL TO $1"] {
            let args = SqlArguments::new(vec![rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::Bool)],vec![vec![Value::Bool(false)]],&memory).unwrap();
            assert!(prepare_submission(vec![TxOp::Sql { sql: format!("PATCH INTO docs FOR VALID_TIME {clause} RECORDS {{_id: 1}}"),args: Some(args) }],SubmitOptions::default(),"xtdb",&memory).is_err());
        }
        assert_eq!(memory.used_bytes(),0);
    }

    #[test]
    fn static_patch_string_values_use_time_conversions_date_fallback() {
        let memory = MemoryBudget::new(1 << 26);
        let source = prepare_submission(vec![TxOp::Sql { sql: "PATCH INTO docs FOR VALID_TIME FROM '1970-01-01T12:00:00' TO '1970-01-02T18:00:00' RECORDS {_id: 1}".into(),args: None }],SubmitOptions::default(),"xtdb",&memory).unwrap();
        assert_eq!(bounds(&source),(Some(Timestamp(0)),Some(Timestamp(86_400_000_000))));
        drop(source);assert_eq!(memory.used_bytes(),0);
    }
}
