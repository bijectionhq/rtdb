use rtdb_types::StructFields;
use std::borrow::Cow;
use std::collections::{BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_expr::{EvalContext, evaluate, evaluate_static_cell};
use rtdb_plan::{Catalog, QualifiedTable, Relation, RelationNode, Schema, TableBinding, TemporalFilter};
use rtdb_types::{Anomaly, Result, Timestamp, Value};
use rtdb_sql::{PrepareOptions, StaticMutation, StaticMutationKind};

use crate::{Documents, SourceTransaction, SqlArguments, SubmitOptions, TxOp};

struct SubmissionCatalog<'a>(&'a str);
impl Catalog for SubmissionCatalog<'_> {
    fn default_database(&self) -> &str { self.0 }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from([self.0.to_owned()]) }
    fn table(&self, _: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
    fn transaction_scoped(&self) -> bool { true }
}

/// Node-scoped cache for the plans of successfully expanded static SQL mutations.
pub struct StaticMutationCache {
    memory: Arc<MemoryBudget>,
    entries: Mutex<VecDeque<Arc<CachedMutation>>>,
}

struct CachedMutation {
    sql: String,
    database: String,
    options: PrepareOptions,
    plan: StaticMutation,
    _reservation: Arc<MemoryReservation>,
}

impl CachedMutation {
    fn matches(&self, sql: &str, database: &str, options: &PrepareOptions) -> bool {
        self.sql == sql && self.database == database && self.options == *options
    }
}

impl StaticMutationCache {
    const ENTRIES: usize = 128;
    const ENTRY_BYTES: usize = 512 << 10;

    /// Uses a bounded child of the node budget to retain plans and their cache keys.
    pub fn new(memory: &Arc<MemoryBudget>) -> Self {
        Self { memory: memory.scope_with_limit((memory.limit_bytes() / 128).min(8 << 20)), entries: Mutex::new(VecDeque::new()) }
    }

    fn lookup(&self, sql: &str, database: &str, options: &PrepareOptions) -> Option<Arc<CachedMutation>> {
        let mut entries = self.entries.lock().ok()?;
        let index = entries.iter().position(|entry| entry.matches(sql, database, options))?;
        let entry = entries.remove(index)?;
        entries.push_back(Arc::clone(&entry));
        Some(entry)
    }

    fn reserve(&self, bytes: usize) -> Option<Arc<MemoryReservation>> {
        if bytes > Self::ENTRY_BYTES || bytes > self.memory.limit_bytes() { return None; }
        let mut entries = self.entries.lock().ok()?;
        loop {
            if let Ok(reservation) = self.memory.reserve(bytes) { return Some(reservation); }
            entries.pop_front()?;
        }
    }

    fn remember(&self, sql: &str, database: &str, options: PrepareOptions, plan: &StaticMutation) {
        let Ok(bytes) = mutation_cache_bytes(sql, database, &options, plan) else { return; };
        let Some(reservation) = self.reserve(bytes) else { return; };
        // The cache reparses under its own budget because raw literals can retain their preparation budget.
        let Ok(Some(plan)) = rtdb_sql::prepare_static_in(sql, &SubmissionCatalog(database), &options, &self.memory) else { return; };
        let Ok(mut entries) = self.entries.lock() else { return; };
        if entries.iter().any(|entry| entry.matches(sql, database, &options)) { return; }
        if entries.len() == Self::ENTRIES { entries.pop_front(); }
        entries.push_back(Arc::new(CachedMutation { sql: sql.to_owned(), database: database.to_owned(), options, plan, _reservation: reservation }));
    }
}

fn mutation_cache_bytes(sql: &str, database: &str, options: &PrepareOptions, plan: &StaticMutation) -> Result<usize> {
    let mut key = [sql, database, options.default_timezone.as_str(), plan.table.database.as_str(), plan.table.table.schema.as_str(), plan.table.table.table.as_str()]
        .into_iter().try_fold(1024usize, |bytes, name| bytes.checked_add(name.len()).ok_or_else(overflow))?;
    for path in &options.search_path { key = key.checked_add(path.len()).and_then(|n| n.checked_add(64)).ok_or_else(overflow)?; }
    for ty in options.parameter_types.values() { key = key.checked_add(ty.retained_bytes()?).and_then(|n| n.checked_add(256)).ok_or_else(overflow)?; }
    let retained = rtdb_query::plan_allocation_bound(&plan.input, &plan.valid_time().expressions())?.checked_mul(2).ok_or_else(overflow)?;
    let syntax = rtdb_sql::statement_syntax_estimate(sql.len()).ok_or_else(overflow)?;
    key.checked_add(retained.max(syntax)).ok_or_else(overflow)
}

pub fn prepare_submission(operations: Vec<TxOp>, options: SubmitOptions, database: &str, memory: &Arc<MemoryBudget>) -> Result<SourceTransaction> {
    prepare_submission_with_sql_options(operations,options,database,memory,&rtdb_sql::PrepareOptions::default())
}

/// Expands static SQL using the submitting node's captured preparation settings.
/// Argument types and timezone are taken from each operation and its transaction options; no SQL setting is added to the durable envelope.
pub fn prepare_submission_with_sql_options(operations: Vec<TxOp>, options: SubmitOptions, database: &str, memory: &Arc<MemoryBudget>, sql_options: &rtdb_sql::PrepareOptions) -> Result<SourceTransaction> {
    prepare_submission_inner(operations, options, database, memory, sql_options, None)
}

/// Expands static SQL with a shared plan cache, evaluating each transaction's arguments and options on every call.
pub fn prepare_submission_with_cache(operations: Vec<TxOp>, options: SubmitOptions, database: &str, memory: &Arc<MemoryBudget>, sql_options: &PrepareOptions, cache: &StaticMutationCache) -> Result<SourceTransaction> {
    prepare_submission_inner(operations, options, database, memory, sql_options, Some(cache))
}

fn prepare_submission_inner(operations: Vec<TxOp>, options: SubmitOptions, database: &str, memory: &Arc<MemoryBudget>, sql_options: &PrepareOptions, cache: Option<&StaticMutationCache>) -> Result<SourceTransaction> {
    let mut output = Vec::new();
    for operation in operations {
        let expanded = match &operation {
            TxOp::Sql { sql, args } => expand(sql, args.as_ref(), &options, database, memory, sql_options, cache)?,
            TxOp::SqlBytes { sql, arg_bytes } => {
                let args = if arg_bytes.as_slice().is_empty() { None } else { Some(crate::source::decode_arguments(arg_bytes.as_slice(), memory)?) };
                expand(sql, args.as_ref(), &options, database, memory, sql_options, cache)?
            }
            _ => None,
        };
        match expanded { Some(operations) => output.extend(operations), None => output.push(operation) }
    }
    SourceTransaction::new(output, options, memory)
}

fn expand(sql: &str, args: Option<&SqlArguments>, options: &SubmitOptions, database: &str, memory: &Arc<MemoryBudget>, sql_options: &PrepareOptions, cache: Option<&StaticMutationCache>) -> Result<Option<Vec<TxOp>>> {
    let scope = rtdb_expr::EvaluationScope::new(memory)?;
    let prepare = PrepareOptions { parameter_types: args.map(|args| args.execution_types().enumerate().collect()).unwrap_or_default(), default_timezone: options.default_tz.clone(), ..sql_options.clone() };
    let cached = cache.and_then(|cache| cache.lookup(sql, database, &prepare));
    let (_syntax, built);
    let plan = if let Some(entry) = &cached {
        &entry.plan
    } else {
        _syntax = memory.reserve(rtdb_sql::statement_syntax_estimate(sql.len()).ok_or_else(overflow)?)?;
        let Some(plan) = rtdb_sql::prepare_static_in(sql, &SubmissionCatalog(database), &prepare, scope.memory_budget())? else { return Ok(None); };
        built = plan;
        &built
    };
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
    let parameter_rows: Box<dyn Iterator<Item = &[Value]> + '_> = match args { Some(args) => Box::new(args.rows()), None => Box::new(std::iter::once(&[][..])) };
    let mut groups: Vec<((Option<Timestamp>, Option<Timestamp>), Vec<StructFields<Value>>)> = Vec::new();
    let mut document_charges = Vec::new();
    let mut expression_count = 0usize;
    fn count(input: &Relation, total: &mut usize) { for expr in input.expressions() { expr.visit(&mut |_| *total += 1); } for child in input.children() { count(child, total); } }
    count(&plan.input, &mut expression_count);
    for parameters in parameter_rows {
        let row_scope = scope.scratch();
        let parameters = &parameters[..parameter_count];
        let parameter_bytes = parameters.iter().try_fold(1024usize, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(overflow))?;
        // Literal values come from the statement text, so their copies scale with its length; only parameters can be copied per expression.
        let bound = parameter_bytes.checked_mul(expression_count.checked_add(8).ok_or_else(overflow)?).and_then(|bytes| bytes.checked_add(sql.len().checked_mul(128)?)).ok_or_else(overflow)?;
        let _evaluation = memory.reserve(bound)?;
        let parameters = if parameters.iter().all(|value| matches!(value,
            Value::Null | Value::Undefined | Value::Bool(_) |
            Value::I8(_) | Value::I16(_) | Value::I32(_) | Value::I64(_) |
            Value::F16(_) | Value::F32(_) | Value::F64(_) | Value::Utf8(_)
        )) {
            Cow::Borrowed(parameters)
        } else {
            let mut owned = parameters.to_vec();
            owned.iter_mut().for_each(rtdb_columnar::admit_execution_value);
            Cow::Owned(owned)
        };
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
            if let Some((_, rows)) = groups.iter_mut().find(|(key, _)| *key == bounds) { rows.push(document); }
            else { groups.push((bounds, vec![document])); }
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
    if !operations.is_empty() && cached.is_none() && let Some(cache) = cache {
        cache.remember(sql, database, prepare, plan);
    }
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
        RelationNode::Values { rows } => rows.iter().map(|row| row.iter().map(|expr| evaluate_static_cell(expr, &Schema::default(), &[], context)).collect()).collect(),
        RelationNode::Project { input, expressions } | RelationNode::Rename { input, expressions, .. } => materialize(input, context)?.iter().map(|row| expressions.iter().map(|expr| evaluate_static_cell(&expr.expr, input.schema(), row, context)).collect()).collect(),
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

    fn document(source: &SourceTransaction) -> &StructFields<Value> {
        let crate::source::Operation::Public(TxOp::PutDocs { documents, .. }) = &source.operations[0] else { panic!("Expected static documents") };
        &documents.rows()[0]
    }

    #[test]
    fn static_mutation_cache_reuses_plans_for_new_arguments_and_keys_by_database_options_and_types() {
        let memory = MemoryBudget::new(1 << 28);
        let cache = StaticMutationCache::new(&memory);
        let sql = "INSERT INTO docs (_id, v) VALUES (1, ?)";
        let submit = |database, timezone: &str, value: Value| {
            let args = SqlArguments::new(vec![value.logical_type()], vec![vec![value]], &memory).unwrap();
            let options = SubmitOptions { default_tz: timezone.into(), ..SubmitOptions::default() };
            prepare_submission_with_cache(vec![TxOp::Sql { sql: sql.into(), args: Some(args) }], options, database, &memory, &PrepareOptions::default(), &cache).unwrap()
        };
        let mut key = PrepareOptions::default();
        key.parameter_types.insert(0, Value::I64(1).logical_type());
        assert_eq!(document(&submit("xtdb", "UTC", Value::I64(1)))["v"], Value::I64(1));
        let first = cache.lookup(sql, "xtdb", &key).unwrap();
        assert_eq!(document(&submit("xtdb", "UTC", Value::I64(2)))["v"], Value::I64(2));
        assert!(Arc::ptr_eq(&first, &cache.lookup(sql, "xtdb", &key).unwrap()));

        let _ = submit("other", "UTC", Value::I64(3));
        let other = cache.lookup(sql, "other", &key).unwrap();
        assert_eq!(other.plan.table.database, "other");
        assert!(!Arc::ptr_eq(&first, &other));

        let _ = submit("xtdb", "America/New_York", Value::I64(4));
        key.default_timezone = "America/New_York".into();
        assert!(!Arc::ptr_eq(&first, &cache.lookup(sql, "xtdb", &key).unwrap()));

        let text = Value::Utf8("text".into());
        assert_eq!(document(&submit("xtdb", "America/New_York", text.clone()))["v"], text);
        key.parameter_types.insert(0, text.logical_type());
        assert!(cache.lookup(sql, "xtdb", &key).is_some());
        assert_eq!(cache.entries.lock().unwrap().len(), 4);
    }

    #[test]
    fn static_mutation_cache_leaves_fallbacks_errors_and_unadmitted_plans_uncached() {
        let memory = MemoryBudget::new(1 << 28);
        let cache = StaticMutationCache::new(&memory);
        let submit = |sql: &str, cache| prepare_submission_with_cache(
            vec![TxOp::Sql { sql: sql.into(), args: None }], SubmitOptions::default(), "xtdb", &memory, &PrepareOptions::default(), cache,
        );
        let dynamic = "INSERT INTO docs (_id) SELECT _id FROM other_docs";
        let source = submit(dynamic, &cache).unwrap();
        assert!(matches!(&source.operations[0], crate::source::Operation::Public(TxOp::Sql { sql, .. }) if sql == dynamic));
        assert!(submit("INSERT INTO", &cache).is_err());
        assert!(cache.entries.lock().unwrap().is_empty());

        let disabled = StaticMutationCache::new(&memory.scope_with_limit(0));
        let source = submit("INSERT INTO docs (_id, v) VALUES (1, 42)", &disabled).unwrap();
        assert_eq!(document(&source)["v"], Value::I64(42));
        assert!(disabled.entries.lock().unwrap().is_empty());
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
    fn static_arguments_preserve_plain_text_and_admit_nested_unsigned_values() {
        let memory = MemoryBudget::new(1 << 24);
        let source = expanded(Value::Utf8("value".into()), &memory);
        assert_eq!(document(&source)["v"], Value::Utf8("value".into()));
        let source = expanded(Value::List(vec![Value::U64(u64::MAX)]), &memory);
        assert_eq!(document(&source)["v"], Value::List(vec![Value::I64(-1)]));
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
