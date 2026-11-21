use rtdb_types::StructFields;
use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::DataType;
use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext, evaluate};
use rtdb_plan::{Catalog, Expr, QualifiedTable, Relation, RelationNode, Schema, TableBinding, TemporalFilter};
use rtdb_types::{Anomaly, Result, Timestamp, Value};
use rtdb_sql::StaticMutationKind;

use crate::{ArrowDocuments, Documents, SourceTransaction, SqlArguments, SubmitOptions, TxOp};
use crate::values::plain_scalar_field;

struct SubmissionCatalog<'a>(&'a str);
impl Catalog for SubmissionCatalog<'_> {
    fn default_database(&self) -> &str { self.0 }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from([self.0.to_owned()]) }
    fn table(&self, _: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
    fn transaction_scoped(&self) -> bool { true }
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
        match expanded { Some(operations) => output.extend(operations), None => output.push(operation) }
    }
    SourceTransaction::new(output, options, memory)
}

fn expand(sql: &str, args: Option<&SqlArguments>, options: &SubmitOptions, database: &str, memory: &Arc<MemoryBudget>, sql_options: &rtdb_sql::PrepareOptions) -> Result<Option<Vec<TxOp>>> {
    let scope = rtdb_expr::EvaluationScope::new(memory)?;
    let _syntax = memory.reserve(rtdb_sql::statement_syntax_estimate(sql.len()).ok_or_else(overflow)?)?;
    let prepare = rtdb_sql::PrepareOptions { parameter_types: args.map(|args| args.execution_types().enumerate().collect()).unwrap_or_default(), default_timezone: options.default_tz.clone(), ..sql_options.clone() };
    let Some(plan) = rtdb_sql::prepare_static_in(sql, &SubmissionCatalog(database), &prepare, scope.memory_budget())? else { return Ok(None); };
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
        match projected_arrow_documents(&plan.input, &pages, memory) {
            Ok(Some(documents)) => return Ok(Some(vec![TxOp::PutArrowDocs { table: plan.table.table, documents }])),
            Ok(None) => {}
            Err(error) if error.code == "rtdb/memory-budget-exceeded" => {}
            Err(error) => return Err(error),
        }
    }
    let parameter_rows: Box<dyn Iterator<Item = Result<crate::values::ArgumentRow<'_>>> + '_> = match args {
        Some(args) => Box::new(args.rows()),
        None => Box::new(std::iter::once(Ok(crate::values::ArgumentRow::borrowed(&[])))),
    };
    let mut groups: Vec<((Option<Timestamp>, Option<Timestamp>), Vec<StructFields<Value>>)> = Vec::new();
    // Where each valid time's group is, so that rows each carrying their own find theirs without searching them all.
    let mut group_of: std::collections::BTreeMap<(Option<Timestamp>, Option<Timestamp>), usize> = Default::default();
    let mut document_charges = Vec::new();
    let mut expression_count = 0usize;
    fn count(input: &Relation, total: &mut usize) { for expr in input.expressions() { expr.visit(&mut |_| *total += 1); } for child in input.children() { count(child, total); } }
    count(&plan.input, &mut expression_count);
    for parameters in parameter_rows {
        let row_scope = scope.scratch();
        let parameters = parameters?;
        let parameters = &parameters[..parameter_count];
        let parameter_bytes = parameters.iter().try_fold(1024usize, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(overflow))?;
        // Literal values come from the statement text, so their copies scale with its length; only parameters can be copied per expression.
        let bound = parameter_bytes.checked_mul(expression_count.checked_add(8).ok_or_else(overflow)?).and_then(|bytes| bytes.checked_add(sql.len().checked_mul(128)?)).ok_or_else(overflow)?;
        let _evaluation = memory.reserve(bound)?;
        let mut parameters = parameters.to_vec();
        parameters.iter_mut().for_each(rtdb_columnar::admit_execution_value);
        let context = EvalContext { parameters: &parameters, parameter_types: None, current_time: Timestamp(0).into(), timezone: &options.default_tz, outer_rows: &[], snapshot_token: None, allocation: Some(&row_scope), catalog: None, corpora: None };
        let temporal = match valid_time {
            TemporalFilter::Default => (None, None),
            TemporalFilter::All => (Some(Timestamp::MIN), Some(Timestamp::MAX)),
            TemporalFilter::FromTo { from, to } => (patch_bound_value(from, Timestamp::MIN, &context)?, patch_bound_value(to, Timestamp::MAX, &context)?),
            _ => return Err(Anomaly::incorrect("xtdb/sql-error", "Static PATCH requires ALL or FROM/TO valid time")),
        };
        for row in materialize(&plan.input, &context)? {
            let mut document: StructFields<_> = plan.input.schema().fields().iter().zip(row).map(|(field, value)| (field.name.clone(), value)).collect();
            let bounds = if matches!(&plan.kind, StaticMutationKind::Patch(_)) { temporal } else {
                let from = document.remove("_valid_from").filter(|value| !matches!(value, Value::Null | Value::Undefined)).map(|value| rtdb_expr::timestamp_micros(&value, &options.default_tz)).transpose()?;
                let to = document.remove("_valid_to").filter(|value| !matches!(value, Value::Null | Value::Undefined)).map(|value| rtdb_expr::timestamp_micros(&value, &options.default_tz)).transpose()?;
                (from, to)
            };
            let document_bytes = document.iter().try_fold(256usize, |bytes, (name, value)| bytes.checked_add(name.capacity())?.checked_add(std::mem::size_of::<(String, Value)>())?.checked_add(value.retained_bytes().ok()?)).ok_or_else(overflow)?;
            document_charges.push(memory.reserve(document_bytes)?);
            match group_of.entry(bounds) {
                std::collections::btree_map::Entry::Occupied(group) => groups[*group.get()].1.push(document),
                std::collections::btree_map::Entry::Vacant(group) => { group.insert(groups.len()); groups.push((bounds, vec![document])); }
            }
        }
    }
    let operations = groups.into_iter().map(|((valid_from, valid_to), documents)| {
        let mut documents = documents;
        for document in &mut documents { for value in document.values_mut() { infer_document_decimals(value)?; } }
        let columns = plan.input.schema().fields().iter().map(|field| field.name.as_str())
            .filter(|name| matches!(&plan.kind, StaticMutationKind::Patch(_)) || !matches!(*name, "_valid_from" | "_valid_to"))
            .map(str::to_owned).collect();
        let documents = Documents::with_column_order(documents, columns, memory)?;
        let table = plan.table.table.clone();
        Ok(match &plan.kind {
            StaticMutationKind::Patch(_) => TxOp::PatchDocs { table, documents, valid_from, valid_to },
            StaticMutationKind::Put => TxOp::PutDocs { table, documents, valid_from, valid_to },
        })
    }).collect::<Result<Vec<_>>>()?;
    Ok((!operations.is_empty()).then_some(operations))
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

fn canonical_argument_column(pages: &[&RecordBatch], index: usize, memory: &Arc<MemoryBudget>) -> Result<ArrayRef> {
    if let [page] = pages {
        let array = page.column(index);
        let nulls = array.logical_null_count();
        if nulls == array.len() || (nulls == 0 && array.data_type() != &DataType::Boolean) {
            return Ok(Arc::clone(array));
        }
    }

    // The source body digest includes the payload beneath null slots and the padding on a Boolean vector.
    let rows = pages.iter().try_fold(0usize, |rows, page| rows.checked_add(page.num_rows()).ok_or_else(overflow))?;
    let base = rows.checked_mul(64).and_then(|bytes| bytes.checked_add(pages.len().checked_mul(64)?))
        .and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
    let bound = pages.iter().try_fold(base, |bytes, page| {
        bytes.checked_add(page.column(index).get_array_memory_size().checked_mul(4).ok_or_else(overflow)?).ok_or_else(overflow)
    })?;
    let mut reservation = memory.reserve(bound)?;
    let mut cells = Vec::with_capacity(rows);
    let sources = pages.iter().enumerate().map(|(source, page)| {
        cells.extend((0..page.num_rows()).map(|row| Some((source, row))));
        page.column(index).as_ref()
    }).collect::<Vec<_>>();
    let column = rtdb_columnar::copy_nullable_cells(&sources, &cells)?;
    drop(sources);
    drop(cells);
    Arc::get_mut(&mut reservation).expect("argument selection owns its reservation").shrink_to(column.get_array_memory_size())?;
    rtdb_columnar::retain_array(column, &reservation)
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

fn materialize(input: &Relation, context: &EvalContext<'_>) -> Result<Vec<Vec<Value>>> {
    match input.node() {
        RelationNode::Values { rows } => rows.iter().map(|row| row.iter().map(|expr| evaluate(expr, &Schema::default(), &[], context)).collect()).collect(),
        RelationNode::Project { input, expressions } | RelationNode::Rename { input, expressions, .. } => materialize(input, context)?.iter().map(|row| expressions.iter().map(|expr| evaluate(&expr.expr, input.schema(), row, context)).collect()).collect(),
        _ => Err(Anomaly::fault("rtdb.tx/static-plan", "Static SQL source contains a data-dependent operator")),
    }
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

    fn document(source: &SourceTransaction) -> &StructFields<Value> {
        let crate::source::Operation::Public(TxOp::PutDocs { documents, .. }) = &source.operations[0] else { panic!("Expected static documents") };
        &documents.rows()[0]
    }

    #[test]
    fn static_unsigned_arguments_become_signed_documents_before_source_encoding() {
        let memory = MemoryBudget::new(1 << 24);
        let args = SqlArguments::new(vec![rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::U64)], vec![vec![Value::U64(u64::MAX)]], &memory).unwrap();
        let source = prepare_submission(vec![TxOp::Sql { sql: "INSERT INTO docs (_id, v) VALUES (1, ?)".into(), args: Some(args.clone()) }], SubmitOptions::default(), "xtdb", &memory).unwrap();
        assert_eq!(document(&source)["v"], Value::I64(-1));
        assert_eq!(&*args.rows().next().unwrap().unwrap(), &[Value::U64(u64::MAX)]);
        assert_eq!(args.types(), &[rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::U64)]);
        let encoded = source.encode().unwrap();
        let message = rtdb_format::envelope::SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
        let replay = SourceTransaction::decode(&message, &memory).unwrap();
        let crate::source::Operation::PutArrowIids { documents, .. } = &replay.operations[0] else { panic!("Expected decoded static documents") };
        assert_eq!(documents.to_rows(true, &memory).unwrap().rows()[0]["v"], Value::I64(-1));
    }

    #[test]
    fn static_parameter_documents_infer_decimal_metadata_from_coefficients() {
        let memory = MemoryBudget::new(1 << 24);
        for precision in [i32::MIN, -1, 0, 1, 256, i32::MAX] {
            let source = expanded(decimal(18, precision, i32::MAX), &memory);
            assert_eq!(document(&source)["v"], decimal(18, 32, i32::MAX));
            let encoded = source.encode().unwrap();
            let message = rtdb_format::envelope::SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
            let replay = SourceTransaction::decode(&message, &memory).unwrap();
            let crate::source::Operation::PutIids { documents, .. } = &replay.operations[0] else { panic!("Expected decoded static documents") };
            assert_eq!(documents.rows()[0]["v"], decimal(18, 32, i32::MAX));
        }
        let source = expanded(decimal(i128::MAX, -1, i32::MIN), &memory);
        let Value::Decimal(value) = &document(&source)["v"] else { panic!("Expected decimal") };
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
        assert_eq!(document(&source)["v"], nested(decimal(18, 32, 129)));
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
        let [Operation::Public(TxOp::PutDocs { documents, .. })] = &source.operations[..] else { panic!("expanded documents") };
        assert_eq!(documents.rows(), &[StructFields::from([("_id".into(), Value::I64(3)), ("v".into(), Value::I64(4))])]);
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
        let [crate::source::Operation::Public(TxOp::PatchDocs { valid_from,valid_to,.. })] = source.operations.as_slice() else { panic!("static patch") };
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
