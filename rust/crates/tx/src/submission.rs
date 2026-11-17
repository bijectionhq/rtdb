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
    let prepare = rtdb_sql::PrepareOptions { parameter_types: args.map(|args| args.execution_types().enumerate().collect()).unwrap_or_default(), default_timezone: options.default_tz.clone(), ..sql_options.clone() };
    let Some(plan) = rtdb_sql::prepare_static(sql, &SubmissionCatalog(database), &prepare)? else { return Ok(None); };
    fn resolve_only(expr: &rtdb_plan::Expr) -> bool {
        let mut requires_clock = false;
        expr.visit(&mut |expr| if let rtdb_plan::Expr::Cast { expr, target } = expr { requires_clock |= target.requires_current_time(&expr.data_type()); });
        requires_clock || expr.is_volatile() || rtdb_expr::requires_pg_catalog(expr)
    }
    fn needs_resolver(input: &Relation) -> bool { input.expressions().iter().any(|expr| resolve_only(expr)) || input.children().iter().any(|input| needs_resolver(input)) }
    if needs_resolver(&plan.input) || plan.valid_time.expressions().iter().any(|expr| resolve_only(expr)) { return Ok(None); }
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
        let mut parameters = parameters.to_vec();
        parameters.iter_mut().for_each(rtdb_columnar::admit_execution_value);
        let context = EvalContext { parameters: &parameters, parameter_types: None, current_time: Timestamp(0).into(), timezone: &options.default_tz, outer_rows: &[], snapshot_token: None, allocation: Some(&scope), catalog: None };
        let temporal = match &plan.valid_time {
            TemporalFilter::Default => (None, None),
            TemporalFilter::All => (Some(Timestamp::MIN), Some(Timestamp::MAX)),
            TemporalFilter::FromTo { from, to } => (patch_bound_value(from, Timestamp::MIN, &context)?, patch_bound_value(to, Timestamp::MAX, &context)?),
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
        let mut documents = documents;
        for document in &mut documents { for value in document.values_mut() { infer_document_decimals(value)?; } }
        let documents = Documents::new(documents, memory)?;
        let table = plan.table.table.clone();
        Ok(if plan.patch { TxOp::PatchDocs { table, documents, valid_from, valid_to } } else { TxOp::PutDocs { table, documents, valid_from, valid_to } })
    }).collect::<Result<Vec<_>>>()?;
    Ok((!operations.is_empty()).then_some(operations))
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

    fn decimal(coefficient: i128, precision: i32, scale: i32) -> Value {
        Value::Decimal(rtdb_types::DecimalValue::new(coefficient.to_be_bytes().to_vec(), precision, scale, 128).unwrap())
    }

    fn expanded(value: Value, memory: &Arc<MemoryBudget>) -> SourceTransaction {
        let args = SqlArguments::new(vec![value.logical_type()], vec![vec![value]], memory).unwrap();
        prepare_submission(vec![TxOp::Sql { sql: "INSERT INTO docs (_id, v) VALUES (1, ?)".into(), args: Some(args) }], SubmitOptions::default(), "xtdb", memory).unwrap()
    }

    fn document(source: &SourceTransaction) -> &BTreeMap<String, Value> {
        let crate::source::Operation::Public(TxOp::PutDocs { documents, .. }) = &source.operations[0] else { panic!("Expected static documents") };
        &documents.rows()[0]
    }

    #[test]
    fn static_unsigned_arguments_become_signed_documents_before_source_encoding() {
        let memory = MemoryBudget::new(1 << 24);
        let args = SqlArguments::new(vec![rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::U64)], vec![vec![Value::U64(u64::MAX)]], &memory).unwrap();
        let source = prepare_submission(vec![TxOp::Sql { sql: "INSERT INTO docs (_id, v) VALUES (1, ?)".into(), args: Some(args.clone()) }], SubmitOptions::default(), "xtdb", &memory).unwrap();
        assert_eq!(document(&source)["v"], Value::I64(-1));
        assert_eq!(args.rows().next().unwrap(), &[Value::U64(u64::MAX)]);
        assert_eq!(args.types(), &[rtdb_types::LogicalType::Scalar(rtdb_types::ScalarType::U64)]);
        let encoded = source.encode().unwrap();
        let message = rtdb_format::envelope::SourceMessage::decode(encoded.as_slice()).unwrap().unwrap();
        let replay = SourceTransaction::decode(&message, &memory).unwrap();
        let crate::source::Operation::PutIids { documents, .. } = &replay.operations[0] else { panic!("Expected decoded static documents") };
        assert_eq!(documents.rows()[0]["v"], Value::I64(-1));
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
        let nested = |value: Value| Value::Struct(BTreeMap::from([
            ("list".into(), Value::List(vec![value.clone(), Value::Null])),
            ("set".into(), Value::Set(vec![value.clone()])),
            ("fixed".into(), Value::FixedSizeList(vec![value.clone()])),
            ("map".into(), Value::Map { entries: vec![(value.clone(), value.clone())], sorted: false }),
            ("struct".into(), Value::Struct(BTreeMap::from([("child".into(), value)]))),
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
mod series_tests {
    use super::*;

    #[test]
    fn static_series_expansion_captures_the_submitting_nodes_endpoint_policy() {
        for end_exclusive in [false,true] {
            let memory = MemoryBudget::new(1 << 26);
            let options = rtdb_sql::PrepareOptions { generate_series_end_exclusive: end_exclusive, ..Default::default() };
            let request = prepare_submission_with_sql_options(vec![TxOp::Sql { sql: "INSERT INTO docs (_id,v) VALUES (1,GENERATE_SERIES(1,3))".into(),args: None }],SubmitOptions::default(),"xtdb",&memory,&options).unwrap();
            let [crate::source::Operation::Public(TxOp::PutDocs { documents,.. })] = request.operations.as_slice() else { panic!("static documents") };
            assert_eq!(documents.rows()[0]["v"],Value::List((1..if end_exclusive {3} else {4}).map(Value::I64).collect()));
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
