use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::DataType;
use rtdb_columnar::{ArrowCell, MemoryBudget, MemoryReservation};
use rtdb_expr::{EvalContext, evaluate};
use rtdb_plan::{Catalog, Expr, QualifiedTable, Relation, RelationNode, Schema, TableBinding, TemporalFilter};
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, TimeUnit, Timestamp, Value};
#[cfg(test)]
use rtdb_types::StructFields;
use rtdb_sql::{PrepareOptions, StaticMutation, StaticMutationKind};

use crate::{ArrowDocuments, Documents, SourceTransaction, SqlArguments, SubmitOptions, TxOp};
use crate::source::{DocumentCell, Operation, owned_projected_documents};
use crate::values::{BoundArgumentRow, canonical_argument_column, plain_scalar_field};
use crate::values::arguments::{canonical_float_field, compatible_float_cell};

// Static planning sees the database name and a fixed empty catalog, even for PATCH.
// The resolver uses the ordered transaction snapshot when it applies the patch.
struct SubmissionCatalog<'a>(&'a str);
impl Catalog for SubmissionCatalog<'_> {
    fn default_database(&self) -> &str { self.0 }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from([self.0.to_owned()]) }
    fn table(&self, _: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
    fn transaction_scoped(&self) -> bool { true }
}

/// One prepared statement's source and most recently used static submission plan.
#[derive(Clone)]
pub struct PreparedDml(Arc<PreparedDmlData>);

struct PreparedDmlData {
    source: String,
    plan: Mutex<Option<Arc<CachedStaticMutation>>>,
    reservation: Arc<MemoryReservation>,
}

struct CachedStaticMutation {
    options: PrepareOptions,
    mutation: StaticMutation,
    _reservation: Arc<MemoryReservation>,
}

impl PreparedDml {
    pub fn new(source: &str, memory: &Arc<MemoryBudget>) -> Option<Self> {
        let reservation = memory.try_reserve(source.len().checked_add(512)?)?;
        Some(Self(Arc::new(PreparedDmlData {
            source: source.to_owned(),
            plan: Mutex::new(None),
            reservation,
        })))
    }

    pub fn schedule(&self, args: Option<SqlArguments>) -> ScheduledOperation {
        ScheduledOperation {
            operation: TxOp::Sql { sql: self.0.source.clone(), args },
            prepared: Some(self.clone()),
        }
    }

    fn cached(
        &self, sql_options: &PrepareOptions, timezone: &str, args: Option<&SqlArguments>, database: &str,
    ) -> Option<Arc<CachedStaticMutation>> {
        let cached = self.0.plan.lock().expect("prepared DML plan lock poisoned").clone()?;
        let PrepareOptions { search_path, parameter_types, default_timezone, generate_series_end_exclusive } = &cached.options;
        let matches = cached.mutation.table.database == database
            && search_path == &sql_options.search_path
            && default_timezone == timezone
            && *generate_series_end_exclusive == sql_options.generate_series_end_exclusive
            && parameter_types.len() == args.map_or(0, |args| args.types().len())
            && parameter_types.iter().zip(args.into_iter().flat_map(|args| args.execution_types()).enumerate())
                .all(|((&index, ty), (position, actual))| index == position && *ty == actual);
        matches.then_some(cached)
    }

    fn remember(&self, options: PrepareOptions, mutation: StaticMutation) {
        let Ok(bytes) = static_plan_bytes(&options, &mutation) else { return; };
        let Some(reservation) = self.0.reservation.budget().try_reserve(bytes) else { return; };
        let entry = Arc::new(CachedStaticMutation { options, mutation, _reservation: reservation });
        *self.0.plan.lock().expect("prepared DML plan lock poisoned") = Some(entry);
    }
}

fn static_plan_bytes(options: &PrepareOptions, mutation: &StaticMutation) -> Result<usize> {
    let mut bytes = rtdb_query::relation_allocation_bound(&mutation.input)?
        .checked_add(options.default_timezone.capacity()).and_then(|bytes| bytes.checked_add(1024))
        .ok_or_else(overflow)?;
    let table = &mutation.table;
    for name in options.search_path.iter().chain([&table.database, &table.table.schema, &table.table.table]) {
        bytes = bytes.checked_add(name.capacity()).and_then(|bytes| bytes.checked_add(64)).ok_or_else(overflow)?;
    }
    for ty in options.parameter_types.values() {
        bytes = bytes.checked_add(ty.retained_bytes()?).and_then(|bytes| bytes.checked_add(256)).ok_or_else(overflow)?;
    }
    for expr in mutation.valid_time().expressions() {
        bytes = bytes.checked_add(rtdb_query::expression_allocation_bound(expr)?).ok_or_else(overflow)?;
    }
    Ok(bytes)
}

/// A pending operation and its optional in-process submission plan owner.
pub struct ScheduledOperation {
    operation: TxOp,
    prepared: Option<PreparedDml>,
}

impl From<TxOp> for ScheduledOperation {
    fn from(operation: TxOp) -> Self { Self { operation, prepared: None } }
}

impl ScheduledOperation {
    pub fn operation(&self) -> &TxOp { &self.operation }
    pub fn into_operation(self) -> TxOp { self.operation }

    /// Coalesces consecutive equal SQL texts while preserving positional argument order.
    pub fn append_sql(&mut self, next: Self) -> Result<Option<Self>> {
        match (&mut self.operation, next.operation) {
            (TxOp::Sql { sql, args: Some(target) }, TxOp::Sql { sql: next_sql, args: Some(args) }) if *sql == next_sql => {
                target.append(args)?;
                if self.prepared.is_none() { self.prepared = next.prepared; }
                Ok(None)
            }
            (_, operation) => Ok(Some(Self { operation, prepared: next.prepared })),
        }
    }
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

#[cfg(test)]
mod prepared_tests {
    use super::{MemoryBudget, Operation, PrepareOptions, PreparedDml, SqlArguments, SubmitOptions, TxOp,
        prepare_scheduled_submission, prepare_submission, prepare_submission_with_sql_options};
    use rtdb_types::{Anomaly, AnomalyKind, Result, Timestamp, Value};
    use std::sync::Arc;

    #[test]
    fn repeated_insert_keeps_source_bytes_and_the_retry_digest_for_each_argument_batch() -> Result<()> {
        let memory = MemoryBudget::new(1 << 24);
        let sql = "INSERT INTO docs (_id, label) VALUES ($2, $1)";
        let statement = PreparedDml::new(sql, &memory).unwrap();
        let options = PrepareOptions::default();
        let submit = |args: SqlArguments| {
            let expected = prepare_submission(vec![TxOp::Sql { sql: sql.into(), args: Some(args.clone()) }], SubmitOptions::default(), "xtdb", &memory)?;
            let actual = prepare_scheduled_submission([statement.schedule(Some(args))], SubmitOptions::default(), "xtdb", &memory, &options)?;
            assert_eq!(actual.encode()?.as_slice(), expected.encode()?.as_slice());
            assert_eq!(actual.body_digest()?, expected.body_digest()?);
            Ok::<_, Anomaly>(actual)
        };
        let args = |values: &[(&str, i64)]| SqlArguments::from_rows(values.iter().map(|(label, id)| {
            vec![Value::Utf8((*label).into()), Value::I64(*id)]
        }).collect(), &memory);
        let first_args = args(&[("first", 1)])?;
        let first = submit(first_args.clone())?;
        let kept = statement.cached(&options, "UTC", Some(&first_args), "xtdb").unwrap();

        let second_args = args(&[("second", 2), ("third", 3)])?;
        let second = submit(second_args.clone())?;
        assert_ne!(first.body_digest()?, second.body_digest()?);
        assert!(Arc::ptr_eq(&kept, &statement.cached(&options, "UTC", Some(&second_args), "xtdb").unwrap()));

        let empty = SqlArguments::new(second_args.types().to_vec(), vec![], &memory)?;
        let empty = submit(empty)?;
        assert!(matches!(empty.operations.as_slice(), [Operation::Public(TxOp::Sql { sql: retained, args: Some(args) })]
            if retained == sql && args.row_count() == 0));
        Ok(())
    }

    #[test]
    fn warm_static_insert_does_not_require_a_syntax_reservation() -> Result<()> {
        let owner = MemoryBudget::new(1 << 24);
        let constrained = MemoryBudget::new(1 << 20);
        let sql = format!("INSERT /*{}*/ INTO docs (_id) VALUES (1)", " ".repeat(8192));
        let statement = PreparedDml::new(&sql, &owner).unwrap();
        let submit = |memory| prepare_scheduled_submission([statement.schedule(None)], SubmitOptions::default(), "xtdb", memory, &PrepareOptions::default());

        assert!(submit(&constrained).err().unwrap().is(AnomalyKind::ResourceExhausted));
        let expected = submit(&owner)?;
        let actual = submit(&constrained)?;
        assert_eq!(actual.encode()?.as_slice(), expected.encode()?.as_slice());
        drop(statement);
        drop(expected);
        assert_eq!(owner.used_bytes(), 0);
        Ok(())
    }

    #[test]
    fn prepared_patch_binds_current_options_and_coalesced_positional_types() -> Result<()> {
        let memory = MemoryBudget::new(1 << 24);
        let sql = "PATCH INTO docs FOR VALID_TIME FROM $1 RECORDS {_id: $2, label: $3}";
        let statement = PreparedDml::new(sql, &memory).unwrap();
        let use_args = |id, label| SqlArguments::from_rows(vec![vec![Value::Utf8("1970-01-01T00:00:00".into()), Value::I64(id), label]], &memory);
        let first = use_args(1, Value::I64(10))?;
        let second = use_args(2, Value::Utf8("two".into()))?;
        let mut joined = first.clone();
        joined.append(second.clone())?;
        let after_coalescing = || {
            let mut operation = statement.schedule(Some(first.clone()));
            assert!(operation.append_sql(statement.schedule(Some(second.clone())))?.is_none());
            Ok::<_, Anomaly>(operation)
        };
        let submit = |operation, args, database: &str, timezone: &str, sql_options: &PrepareOptions| {
            let submit = SubmitOptions { default_tz: timezone.into(), ..Default::default() };
            let expected = prepare_submission_with_sql_options(vec![TxOp::Sql { sql: sql.into(), args: Some(args) }], submit.clone(), database, &memory, sql_options)?;
            let actual = prepare_scheduled_submission([operation], submit, database, &memory, sql_options)?;
            assert_eq!(actual.encode()?.as_slice(), expected.encode()?.as_slice());
            assert_eq!(actual.body_digest()?, expected.body_digest()?);
            Ok::<_, Anomaly>(actual)
        };
        let options = PrepareOptions::default();
        submit(statement.schedule(Some(first.clone())), first.clone(), "xtdb", "UTC", &options)?;
        let initial = statement.cached(&options, "UTC", Some(&first), "xtdb").unwrap();
        assert!(statement.cached(&options, "UTC", Some(&joined), "xtdb").is_none());
        let result = submit(after_coalescing()?, joined.clone(), "xtdb", "UTC", &options)?;
        let rebound = statement.cached(&options, "UTC", Some(&joined), "xtdb").unwrap();
        assert!(!Arc::ptr_eq(&initial, &rebound));
        assert!(matches!(result.operations.as_slice(),
            [Operation::PatchArrowIids { valid_from: Some(Timestamp(0)), .. }]));

        assert!(statement.cached(&options, "+02:00", Some(&joined), "xtdb").is_none());
        assert!(statement.cached(&options, "UTC", Some(&joined), "other").is_none());
        let other = PrepareOptions { search_path: vec!["app".into()], generate_series_end_exclusive: true, ..options };
        assert!(statement.cached(&other, "UTC", Some(&joined), "xtdb").is_none());
        let result = submit(after_coalescing()?, joined.clone(), "other", "+02:00", &other)?;
        assert!(matches!(result.operations.as_slice(),
            [Operation::PatchArrowIids { table, valid_from: Some(Timestamp(-7_200_000_000)), .. }] if table.schema == "app"));
        assert!(statement.cached(&other, "+02:00", Some(&joined), "other").is_some());
        Ok(())
    }

    #[test]
    fn prepared_submission_preserves_dynamic_source_and_reached_errors() -> Result<()> {
        let memory = MemoryBudget::new(1 << 24);
        let options = PrepareOptions::default();
        for sql in [
            "INSERT INTO docs (_id, v) VALUES (1, CURRENT_TIMESTAMP)",
            "INSERT INTO docs (_id, v) VALUES (1, RANDOM())",
            "INSERT INTO docs (_id, v) VALUES (1)",
            "PATCH INTO docs FOR VALID_TIME FROM CURRENT_TIMESTAMP RECORDS {_id: 1}",
        ] {
            let statement = PreparedDml::new(sql, &memory).unwrap();
            for _ in 0..2 {
                let actual = prepare_scheduled_submission([statement.schedule(None)], SubmitOptions::default(), "xtdb", &memory, &options)?;
                assert!(matches!(actual.operations.as_slice(), [Operation::Public(TxOp::Sql { sql: retained, args: None })] if retained == sql));
            }
            assert!(statement.cached(&options, "UTC", None, "xtdb").is_none());
        }

        let sql = "PATCH INTO docs FOR VALID_TIME FROM $1 RECORDS {_id: $2}";
        let statement = PreparedDml::new(sql, &memory).unwrap();
        let args = |bound: &str| SqlArguments::from_rows(vec![
            vec![Value::Utf8("1970-01-02".into()), Value::I64(1)],
            vec![Value::Utf8(bound.into()), Value::I64(2)],
        ], &memory);
        prepare_scheduled_submission([statement.schedule(Some(args("1970-01-01")?))], SubmitOptions::default(), "xtdb", &memory, &options)?;
        let invalid = args("not a date")?;
        assert!(statement.cached(&options, "UTC", Some(&invalid), "xtdb").is_some());
        let actual = prepare_scheduled_submission([statement.schedule(Some(invalid.clone()))], SubmitOptions::default(), "xtdb", &memory, &options).err().unwrap();
        let expected = prepare_submission(vec![TxOp::Sql { sql: sql.into(), args: Some(invalid) }], SubmitOptions::default(), "xtdb", &memory).err().unwrap();
        assert_eq!(actual, expected);
        Ok(())
    }
}

pub fn prepare_submission(operations: Vec<TxOp>, options: SubmitOptions, database: &str, memory: &Arc<MemoryBudget>) -> Result<SourceTransaction> {
    prepare_submission_with_sql_options(operations,options,database,memory,&rtdb_sql::PrepareOptions::default())
}

/// Expands static SQL using the submitting node's captured preparation settings.
/// Argument types and timezone are taken from each operation and its transaction options; no SQL setting is added to the durable envelope.
pub fn prepare_submission_with_sql_options(operations: Vec<TxOp>, options: SubmitOptions, database: &str, memory: &Arc<MemoryBudget>, sql_options: &rtdb_sql::PrepareOptions) -> Result<SourceTransaction> {
    prepare_scheduled_submission(operations.into_iter().map(ScheduledOperation::from), options, database, memory, sql_options)
}

/// Prepares the original source operations, reusing matching plans from the submitting statements.
pub fn prepare_scheduled_submission(
    operations: impl IntoIterator<Item = ScheduledOperation>, options: SubmitOptions,
    database: &str, memory: &Arc<MemoryBudget>, sql_options: &PrepareOptions,
) -> Result<SourceTransaction> {
    let mut output = Vec::new();
    for ScheduledOperation { operation, prepared } in operations {
        let expanded = match &operation {
            TxOp::Sql { sql, args } => expand(sql, args.as_ref(), &options, database, memory, sql_options, prepared.as_ref())?,
            TxOp::SqlBytes { sql, arg_bytes } => {
                let args = if arg_bytes.as_slice().is_empty() { None } else { Some(crate::source::decode_arguments(arg_bytes.clone().into_bytes(), memory)?) };
                expand(sql, args.as_ref(), &options, database, memory, sql_options, None)?
            }
            _ => None,
        };
        match expanded { Some(operations) => output.extend(operations), None => output.push(Operation::Public(operation)) }
    }
    SourceTransaction::from_operations(output, options, memory)?.with_bounded_retry()
}

fn expand(
    sql: &str, args: Option<&SqlArguments>, options: &SubmitOptions, database: &str,
    memory: &Arc<MemoryBudget>, sql_options: &PrepareOptions, prepared: Option<&PreparedDml>,
) -> Result<Option<Vec<Operation>>> {
    let scope = rtdb_expr::EvaluationScope::new(memory)?;
    if let Some(cached) = prepared.and_then(|prepared| prepared.cached(sql_options, &options.default_tz, args, database)) {
        return expand_plan(&cached.mutation, args, options, memory, &scope);
    }
    let syntax = memory.reserve(rtdb_sql::statement_syntax_estimate(sql.len()).ok_or_else(overflow)?)?;
    let prepare = PrepareOptions {
        parameter_types: args.map(|args| args.execution_types().enumerate().collect()).unwrap_or_default(),
        default_timezone: options.default_tz.clone(),
        ..sql_options.clone()
    };
    let Some(plan) = rtdb_sql::prepare_static_in(sql, &SubmissionCatalog(database), &prepare, scope.memory_budget())? else { return Ok(None); };
    if plan.table.table.schema == "xt" && plan.table.table.table == crate::hosted_control::HOSTED_CAPACITY_DECLARATIONS {
        return Ok(None);
    }
    fn resolve_only(expr: &rtdb_plan::Expr) -> bool {
        expr.requires_current_time() || expr.is_volatile() || rtdb_expr::requires_pg_catalog(expr)
    }
    fn needs_resolver(input: &Relation) -> bool { input.expressions().iter().any(|expr| resolve_only(expr)) || input.children().iter().any(|input| needs_resolver(input)) }
    if needs_resolver(&plan.input) || plan.valid_time().expressions().iter().any(|expr| resolve_only(expr)) { return Ok(None); }
    let expanded = expand_plan(&plan, args, options, memory, &scope)?;
    if expanded.is_some() && let Some(prepared) = prepared {
        drop(syntax);
        prepared.remember(prepare, plan);
    }
    Ok(expanded)
}

fn expand_plan(plan: &StaticMutation, args: Option<&SqlArguments>, options: &SubmitOptions, memory: &Arc<MemoryBudget>, scope: &rtdb_expr::EvaluationScope) -> Result<Option<Vec<Operation>>> {
    let valid_time = plan.valid_time();
    if args.is_some_and(|args| args.row_count() == 0) { return Ok(None); }
    let parameter_count = plan.input.parameter_count().max(valid_time.expressions().iter().map(|expr| {
        let mut count = 0; expr.visit(&mut |expr| { if let rtdb_plan::Expr::Parameter { index, .. } = expr { count = count.max(index + 1); } }); count
    }).max().unwrap_or(0));
    if args.map_or(0, |args| args.types().len()) < parameter_count { return Ok(None); }
    if matches!(plan.kind, StaticMutationKind::Put) && let Some(args) = args && !args.built_from_rows() && let Some(pages) = args.uniform_arrow_pages() {
        let canonical_row = args.has_single_canonical_arrow_row();
        let projected = if plan.input.schema().fields().iter().any(|field| valid_time_column(&field.name)) {
            projected_timed_puts(&plan.input, &pages, canonical_row, &plan.table.table, &options.default_tz, scope, memory)
        } else {
            projected_arrow_documents(&plan.input, &pages, canonical_row, memory).map(|documents| documents.map(|documents| {
                let table = plan.table.table.clone();
                if canonical_row {
                    vec![Operation::PutReplayArrow { table, documents, iids: None, valid_from: None, valid_to: None }]
                } else {
                    vec![Operation::Public(TxOp::PutArrowDocs { table, documents })]
                }
            }))
        };
        if let Some(operations) = projected? { return Ok(Some(operations)); }
    }
    let parameter_rows = args.into_iter().flat_map(SqlArguments::indexed_bound_rows_with_source_types)
        .chain(args.is_none().then_some(Ok((0, BoundArgumentRow::Values { values: &[], types: None }, None))));
    let mut groups: Vec<((Option<Timestamp>, Option<Timestamp>), Vec<ArrowDocuments>)> = Vec::new();
    let mut group_of: BTreeMap<(Option<Timestamp>, Option<Timestamp>), usize> = Default::default();
    let mut inventory = memory.reserve(4096)?;
    let columns = plan.input.schema().fields().iter().map(|field| field.name.as_str())
        .map(str::to_owned).collect::<Vec<_>>();
    let mut pending_bounds = (None, None);
    let mut pending = Vec::with_capacity(128);
    let mut pending_charge = memory.reserve(4096)?;
    let mut flush = |bounds, pending: &mut Vec<Vec<DocumentCell<'_>>>, pending_charge: &mut Arc<rtdb_columnar::MemoryReservation>| -> Result<()> {
        if pending.is_empty() { return Ok(()); }
        let mut page_columns = columns.clone();
        if matches!(&plan.kind, StaticMutationKind::Put) {
            for (index, name) in columns.iter().enumerate().rev() {
                if valid_time_column(name) && pending.iter().all(|row|
                    matches!(&row[index], DocumentCell::Value(value) if matches!(value.as_ref(), Value::Null)))
                {
                    page_columns.remove(index);
                    for row in pending.iter_mut() { row.remove(index); }
                }
            }
        }
        let page = if pending.iter().flat_map(|row| row.iter()).any(|cell| matches!(cell, DocumentCell::Arrow { .. })) {
            for cell in pending.iter_mut().flat_map(|row| row.iter_mut()) {
                if let DocumentCell::Value(value) = cell { value.to_mut().admit_object_dates()?; }
            }
            let page = owned_projected_documents(&page_columns, pending, memory)?;
            pending.clear();
            Arc::get_mut(pending_charge).expect("Static document buffer owns its reservation").shrink_to(4096)?;
            page
        } else {
            let rows = pending.drain(..).map(|row| page_columns.iter().cloned().zip(row).map(|(name, cell)| {
                let DocumentCell::Value(value) = cell else { unreachable!() };
                (name, value.into_owned())
            }).collect()).collect();
            let documents = Documents::with_column_order(rows, page_columns, memory)?;
            Arc::get_mut(pending_charge).expect("Static document buffer owns its reservation").shrink_to(4096)?;
            crate::source::owned_row_documents(&documents, memory)?
        };
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
        let (_, binding, source_types) = parameters?;
        let input = match binding {
            BoundArgumentRow::Arrow { batch, row, .. } => Some(StaticArrowRow {
                batch: batch.batch(), row, types: source_types.expect("An Arrow row has source types"),
            }),
            BoundArgumentRow::Values { .. } => None,
        };
        let parameters = match binding {
            BoundArgumentRow::Arrow { batch, row, types } => {
                arrow = rtdb_query::BoundParameters::from_arrow(batch.clone(), row, types.to_vec(), memory)?;
                arrow.evaluator()
            }
            BoundArgumentRow::Values { values: input, .. } => {
                let input = &input[..parameter_count];
                let bytes = input.iter().try_fold(1024usize, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(overflow))?;
                row_scope.reserve(bytes)?;
                values = input.iter().cloned().map(|mut value| {
                    rtdb_columnar::admit_execution_value(&mut value)?;
                    Ok(value)
                }).collect::<Result<Vec<_>>>()?;
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
        materialize(&plan.input, input, &context, &mut |mut document| {
            let bounds = if matches!(&plan.kind, StaticMutationKind::Patch(_)) { temporal } else {
                for name in ["_valid_from", "_valid_to"] {
                    if let Some(index) = columns.iter().position(|column| column == name) {
                        let cell = &mut document[index];
                        let value = insert_bound(cell.value()?.as_ref(), &options.default_tz)?.map_or(Value::Null, |time| Value::Timestamp {
                            value: time.0, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()),
                        });
                        *cell = DocumentCell::Value(std::borrow::Cow::Owned(value));
                    }
                }
                (None, None)
            };
            for cell in &mut document {
                if let DocumentCell::Value(value) = cell {
                    infer_document_decimals(value.to_mut())?;
                }
            }
            if pending.len() == 128 || pending_bounds != bounds {
                flush(pending_bounds, &mut pending, &mut pending_charge)?;
                pending_bounds = bounds;
            }
            let lease = Arc::get_mut(&mut pending_charge).expect("Static document buffer owns its reservation");
            let bound = document.iter().try_fold(document.capacity().checked_mul(std::mem::size_of::<DocumentCell<'_>>()).ok_or_else(overflow)?, |bytes, cell| {
                let value = match cell { DocumentCell::Value(value) => value.retained_bytes()?, DocumentCell::Arrow { .. } => 0 };
                bytes.checked_add(value).ok_or_else(overflow)
            })?;
            lease.grow_to(lease.bytes().checked_add(bound).ok_or_else(overflow)?)?;
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
    input: &Relation, pages: &[&RecordBatch], canonical_row: bool, table: &rtdb_types::TableRef, timezone: &str,
    scope: &rtdb_expr::EvaluationScope, memory: &Arc<MemoryBudget>,
) -> Result<Option<Vec<Operation>>> {
    let Some(first) = pages.first() else { return Ok(None); };
    let RelationNode::Rename { input: values, expressions, .. } = input.node() else { return Ok(None); };
    let RelationNode::Values { rows } = values.node() else { return Ok(None); };
    let [row] = rows.as_slice() else { return Ok(None); };
    let schema = first.schema_ref();
    if !schema.fields().iter().all(|field| static_payload_field(field) || field.metadata().is_empty()
        && matches!(field.data_type(), DataType::Date32 | DataType::Date64 | DataType::Timestamp(..))) {
        return Ok(None);
    }

    let row_count = pages.iter().try_fold(0usize, |len, page| len.checked_add(page.num_rows()).ok_or_else(overflow))?;
    let input_fields = rtdb_format::ipc::schema_translation_bound(schema)?;
    let output_fields = input.schema().fields().iter().try_fold(4096usize, |bytes, field| {
        bytes.checked_add(field.name.len().checked_mul(4).and_then(|name| name.checked_add(2048)).ok_or_else(overflow)?).ok_or_else(overflow)
    })?;
    let _projection = memory.reserve(row_count.checked_mul(512)
        .and_then(|bytes| bytes.checked_add(input_fields))
        .and_then(|bytes| bytes.checked_add(output_fields)).ok_or_else(overflow)?)?;
    let mut data = BTreeMap::new();
    let mut times = Vec::with_capacity(2);
    let mut required = BTreeSet::new();
    for (position, expression) in expressions.iter().enumerate() {
        let Expr::Column { id, .. } = expression.expr else { return Ok(None); };
        let Some(expr) = values.schema().position(id).and_then(|position| row.get(position)) else { return Ok(None); };
        let name = expression.field.name.as_str();
        if valid_time_column(name) {
            times.push((position, expr));
            expr.visit(&mut |expr| if let Expr::Parameter { index, .. } = expr { required.insert(*index); });
        } else {
            let Expr::Parameter { index, .. } = expr else { return Ok(None); };
            let Some(field) = schema.fields().get(*index).filter(|field| static_payload_field(field)) else { return Ok(None); };
            data.insert(position, (*index, field.as_ref().clone().with_name(name)));
        }
    }
    let Some((id, _)) = data.values().find(|(_, field)| field.name() == "_id") else { return Ok(None); };
    if pages.iter().any(|page| page.column(*id).logical_null_count() != 0) { return Ok(None); }
    let readers = required.into_iter().map(|index| {
        Ok((index, rtdb_columnar::ColumnReader::new(schema.field(index))?))
    }).collect::<Result<Vec<_>>>()?;

    let mut cells = Vec::with_capacity(row_count);
    let mut bounds = times.iter().map(|(position, _)| (*position, Vec::with_capacity(row_count))).collect::<BTreeMap<_, _>>();
    for (page_index, page) in pages.iter().enumerate() {
        for row in 0..page.num_rows() {
            let row_scope = scope.scratch();
            let mut parameters = vec![Value::Null; schema.fields().len()];
            for (index, reader) in &readers {
                let array = page.column(*index).as_ref();
                row_scope.reserve(rtdb_columnar::estimate_read_cell_bytes(schema.field(*index), array, row)?)?;
                let mut value = reader.read(array, row)?;
                rtdb_columnar::admit_execution_value(&mut value)?;
                parameters[*index] = value;
            }
            let context = EvalContext { parameters: rtdb_expr::Parameters::Values(&parameters), parameter_types: None, current_time: Timestamp(0).into(),
                timezone, outer_rows: &[], snapshot_token: None, allocation: Some(&row_scope), catalog: None };
            for &(position, expression) in &times {
                let evaluated = evaluate(expression, &Schema::default(), &[], &context);
                // The row path orders evaluation errors against admission of earlier documents.
                let Ok(time) = evaluated.and_then(|value| insert_bound(&value, timezone)) else { return Ok(None); };
                bounds.get_mut(&position).expect("each bound has a column").push(time.map(|time| time.0));
            }
            cells.push(Some((page_index, row)));
        }
    }

    let Some((first_page, first_row)) = cells.first().copied().flatten() else { return Ok(None); };
    if data.values().any(|(index, _)| pages[first_page].column(*index).is_null(first_row)) {
        return Ok(None);
    }
    let mut fields = Vec::with_capacity(expressions.len());
    let mut columns = Vec::with_capacity(expressions.len());
    for (position, expression) in expressions.iter().enumerate() {
        if let Some((index, field)) = data.remove(&position) {
            columns.push(if canonical_row { Arc::clone(first.column(index)) }
                else { selected_argument_column(pages, index, &field, &cells, memory)? });
            fields.push(field);
        } else {
            let times = bounds.remove(&position).expect("each bound has a column");
            if times.iter().all(Option::is_none) { continue; }
            let column = arrow_array::TimestampMicrosecondArray::from(times).with_timezone("UTC");
            fields.push(arrow_schema::Field::new(&expression.field.name, column.data_type().clone(), true));
            columns.push(Arc::new(column));
        }
    }
    let batch = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns)
        .map_err(|error| Anomaly::fault("rtdb.tx/static-plan", error.to_string()))?;
    let Some(documents) = ArrowDocuments::from_record_batch(&batch, memory)? else { return Ok(None); };
    let reader = documents.id_reader()?;
    let iids = (0..batch.num_rows()).map(|row| documents.iid(row, reader.as_ref(), memory)).collect::<Result<Vec<_>>>();
    let Ok(iids) = iids else { return Ok(None); };
    Ok(Some(vec![Operation::PutReplayArrow { table: table.clone(), documents, iids: (!canonical_row).then_some(iids), valid_from: None, valid_to: None }]))
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

fn projected_arrow_documents(input: &Relation, pages: &[&RecordBatch], canonical_row: bool, memory: &Arc<MemoryBudget>) -> Result<Option<ArrowDocuments>> {
    let Some(first) = pages.first() else { return Ok(None); };
    let RelationNode::Rename { input: values, expressions, .. } = input.node() else { return Ok(None); };
    let RelationNode::Values { rows } = values.node() else { return Ok(None); };
    let [row] = rows.as_slice() else { return Ok(None); };
    if expressions.iter().any(|expr| matches!(expr.field.name.as_str(), "_valid_from" | "_valid_to")) {
        return Ok(None);
    }

    let schema = first.schema_ref();
    let projection = || expressions.iter().map(|expr| {
        let Expr::Column { id, .. } = expr.expr else { return None; };
        let Expr::Parameter { index, .. } = row.get(values.schema().position(id)?)? else { return None; };
        let field = schema.fields().get(*index).filter(|field| static_payload_field(field))?;
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
        columns.push(if canonical_row { Arc::clone(first.column(index)) }
            else { canonical_argument_column(pages, index, memory)? });
    }
    let projection = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns)
        .map_err(|error| Anomaly::fault("rtdb.tx/static-plan", error.to_string()))?;
    ArrowDocuments::from_record_batch(&projection, memory)
}

fn static_payload_field(field: &arrow_schema::Field) -> bool {
    if !field.metadata().is_empty() { return false; }
    match field.data_type() {
        DataType::List(child) | DataType::FixedSizeList(child, _) => static_payload_field(child),
        DataType::Struct(children) => children.iter().all(|child| static_payload_field(child)),
        _ => plain_scalar_field(field),
    }
}

fn infer_document_decimals(value: &mut Value) -> Result<()> {
    match value {
        Value::Decimal(decimal) => *decimal = rtdb_expr::infer_decimal(decimal)?,
        Value::Set(values) => for value in values { infer_document_decimals(value)?; },
        Value::List(values) | Value::FixedSizeList(values) => {
            if let Some(items) = values.values_mut() {
                for value in items { infer_document_decimals(value)?; }
            } else if values.element_type().legs().iter().any(|ty| matches!(ty, LogicalType::Scalar(ScalarType::Decimal { .. }))) {
                let mut inferred = rtdb_types::ListBuilder::with_capacity(values.len());
                for value in values.iter() {
                    let mut value = value.into_owned();
                    infer_document_decimals(&mut value)?;
                    inferred.push(value);
                }
                *values = inferred.finish();
            }
        }
        Value::Struct(fields) => for value in fields.values_mut() { infer_document_decimals(value)?; },
        Value::Map { entries, .. } => for (key, value) in entries { infer_document_decimals(key)?; infer_document_decimals(value)?; },
        _ => {}
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct StaticArrowRow<'a> { batch: &'a RecordBatch, row: usize, types: &'a [LogicalType] }

impl<'a> StaticArrowRow<'a> {
    fn argument(self, index: usize) -> Option<(ArrowCell<'a>, &'a LogicalType)> {
        let field = self.batch.schema_ref().fields().get(index)?;
        let array = self.batch.columns().get(index)?.as_ref();
        Some((ArrowCell { field, array, row: self.row }, self.types.get(index)?))
    }

    fn position(self, expr: &Expr) -> Option<(ArrowCell<'a>, &'a LogicalType)> {
        match expr {
            Expr::Parameter { index, .. } => self.argument(*index),
            Expr::Field { expr, name, .. } => {
                let (record, declared) = self.position(expr)?;
                if !record_field(record.field) { return None; }
                let declared = match declared { LogicalType::Maybe(inner) => inner.as_ref(), ty => ty };
                let LogicalType::Struct(fields) = declared else { return None; };
                let cell = record.structure()?.filter(|child| child.field.name() == name).last()?;
                Some((cell, fields.get(name)?))
            }
            _ => None,
        }
    }
}

fn record_field(field: &arrow_schema::Field) -> bool {
    plain_scalar_field(field) || canonical_float_field(field)
        || rtdb_columnar::decimal_field_parameters(field).is_ok_and(|decimal| decimal.is_some())
        || matches!(field.data_type(), DataType::Struct(fields) if field.metadata().is_empty() && fields.iter().all(|field| record_field(field)))
}

fn materialize<'a>(
    input: &Relation, binding: Option<StaticArrowRow<'a>>, context: &EvalContext<'_>,
    emit: &mut dyn FnMut(Vec<DocumentCell<'a>>) -> Result<()>,
) -> Result<()> {
    match input.node() {
        RelationNode::Values { rows } => {
            let schema = Schema::default();
            for row in rows {
                let mut cells = Vec::with_capacity(row.len());
                for expr in row { cells.push(project_static_cell(expr, binding, &schema, context)?); }
                emit(cells)?;
            }
            Ok(())
        }
        RelationNode::Project { input, expressions } | RelationNode::Rename { input, expressions, .. } => {
            materialize(input, binding, context, &mut |row| {
                let mut computed_inputs = BTreeSet::new();
                for expression in expressions.iter().filter(|expression| !matches!(expression.expr, Expr::Column { .. })) {
                    expression.expr.visit(&mut |expr| {
                        if let Expr::Column { id, .. } = expr && let Some(position) = input.schema().position(*id) {
                            computed_inputs.insert(position);
                        }
                    });
                }
                let values = row.iter().enumerate().map(|(position, cell)| {
                    if !computed_inputs.contains(&position) { return Ok(Value::Null); }
                    if let Some(scope) = context.allocation { scope.reserve(cell.bound()?)?; }
                    cell.value().map(Cow::into_owned)
                }).collect::<Result<Vec<_>>>()?;
                let projected = expressions.iter().map(|expression| {
                    if let Expr::Column { id, .. } = expression.expr {
                        let cell = &row[input.schema().position(id).expect("A static projection refers to its input column")];
                        return Ok(match cell {
                            DocumentCell::Arrow { cell, declared } => DocumentCell::Arrow { cell: *cell, declared: *declared },
                            DocumentCell::Value(value) => {
                                if let Some(scope) = context.allocation { scope.reserve(value.retained_bytes()?)?; }
                                DocumentCell::Value(Cow::Owned(value.as_ref().clone()))
                            }
                        });
                    }
                    evaluate(&expression.expr, input.schema(), &values, context).map(|value| DocumentCell::Value(Cow::Owned(value)))
                }).collect::<Result<_>>()?;
                emit(projected)
            })
        }
        _ => Err(Anomaly::fault("rtdb.tx/static-plan", "Static SQL source contains a data-dependent operator")),
    }
}

fn project_static_cell<'a>(
    expr: &Expr, binding: Option<StaticArrowRow<'a>>, schema: &Schema, context: &EvalContext<'_>,
) -> Result<DocumentCell<'a>> {
    let Some(binding) = binding else {
        return evaluate(expr, schema, &[], context).map(|value| DocumentCell::Value(Cow::Owned(value)));
    };
    let expr = if let Expr::Field { expr: record, name, .. } = expr
        && let Expr::Struct(fields) = record.as_ref()
        && fields.iter().all(|(_, expr)| match expr {
            Expr::Parameter { index, .. } => binding.argument(*index).is_some_and(|(cell, _)| {
                plain_scalar_field(cell.field) || canonical_float_field(cell.field)
                    || rtdb_columnar::decimal_field_parameters(cell.field).is_ok_and(|decimal| decimal.is_some())
            }),
            Expr::Literal(value) => matches!(value, Value::Null | Value::Bool(_) | Value::I64(_) | Value::I32(_) | Value::Utf8(_) | Value::Decimal(_)),
            _ => false,
        }) {
        let Some((_, expr)) = fields.iter().rev().find(|(field, _)| field == name) else {
            return Ok(DocumentCell::Value(Cow::Owned(Value::Null)));
        };
        expr
    } else { expr };
    if let Some((cell, declared)) = binding.position(expr) {
        if plain_scalar_field(cell.field) && !cell.field.data_type().is_unsigned_integer() {
            return Ok(DocumentCell::Arrow { cell, declared: Some(declared) });
        }
        if compatible_float_cell(cell, None)
            && matches!(cell.physical()?.field.data_type(), DataType::List(_) | DataType::FixedSizeList(..) | DataType::Null) {
            return Ok(DocumentCell::Arrow { cell, declared: None });
        }
    }
    evaluate(expr, schema, &[], context).map(|value| DocumentCell::Value(Cow::Owned(value)))
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
fn overflow() -> Anomaly { Anomaly::incorrect("rtdb.tx/allocation-overflow", "Static transaction allocation exceeds address space").with_kind(rtdb_types::AnomalyKind::ProgramLimitExceeded) }

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

        for transaction in [ordinary, projected] {
            let source = transaction.encode().unwrap();
            let source = rtdb_format::envelope::SourceMessage::decode(source.as_slice()).unwrap().unwrap();
            let replay = SourceTransaction::decode(&source, &memory).unwrap();
            let [Operation::PutReplayArrow { documents, iids: Some(iids), valid_from: None, valid_to: None, .. }] = replay.operations.as_slice()
                else { panic!("Expected one Arrow write with document validity"); };
            assert_eq!(documents.array().len(), 4);
            assert_eq!(*iids, (1..=4).map(|id| rtdb_types::EntityId::Integer(id).iid()).collect::<Vec<_>>());
            for name in ["_valid_from", "_valid_to"] {
                let column = documents.array().column_by_name(name).unwrap();
                assert_eq!((0..4).map(|row| column.is_null(row)).collect::<Vec<_>>(), [false, true, false, true]);
            }
        }

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
    fn static_insert_under_memory_pressure_refuses_instead_of_expanding_rows() {
        use arrow_array::{StringArray, UInt64Array};
        use arrow_schema::Field;
        use rtdb_columnar::OwnedBatch;

        let ample = MemoryBudget::new(1 << 24);
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("bound_label", DataType::Utf8, true),
            Field::new("bound_id", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["label", "other"])), Arc::new(UInt64Array::from(vec![1, 2]))]).unwrap();
        let args = SqlArguments::from_arrow(OwnedBatch::new(batch, &ample).unwrap(), &ample).unwrap();
        let labels = args.uniform_arrow_pages().unwrap()[0].column(0).clone();
        let mut refused = false;
        for limit in (1 << 10..=128 << 10).step_by(1 << 10) {
            let memory = MemoryBudget::new(limit);
            let submitted = prepare_submission(vec![TxOp::Sql {
                sql: "INSERT INTO docs (_id, label) VALUES ($2, $1)".into(), args: Some(args.clone()),
            }], SubmitOptions::default(), "xtdb", &memory);
            match submitted {
                Ok(submission) => {
                    let [Operation::Public(TxOp::PutArrowDocs { documents, .. })] = submission.operations.as_slice()
                        else { panic!("a {limit}-byte budget staged another representation") };
                    assert!(Arc::ptr_eq(documents.array().column(1), &labels), "a {limit}-byte budget expanded the rows");
                }
                Err(error) => { assert_eq!(error.code, "rtdb/memory-budget-exceeded"); refused = true; }
            }
        }
        assert!(refused, "the smallest budgets refuse the statement");
    }

    #[test]
    fn prepared_static_insert_projects_packed_embeddings_into_source_documents() {
        use arrow_array::{FixedSizeListArray, Float32Array, Int64Array};
        use arrow_schema::Field;
        use rtdb_columnar::OwnedBatch;

        let memory = MemoryBudget::new(8 << 20);
        let element = Arc::new(Field::new("$data$", DataType::Float32, true));
        let children = Arc::new(Float32Array::from(vec![Some(1.25), None, Some(-0.0), Some(2.0)]));
        let embedding = FixedSizeListArray::try_new(element, 2, children, None).unwrap();
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("bound", embedding.data_type().clone(), false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![2, 1])), Arc::new(embedding)]).unwrap();
        let args = SqlArguments::from_arrow(OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();
        let submission = prepare_submission(vec![TxOp::Sql {
            sql: "INSERT INTO docs (_id, embedding) VALUES ($1, $2)".into(), args: Some(args),
        }], SubmitOptions::default(), "xtdb", &memory).unwrap();
        let [Operation::Public(TxOp::PutArrowDocs { documents, .. })] = submission.operations.as_slice() else {
            panic!("expected Arrow document projection");
        };
        let embedding = documents.array().column_by_name("embedding").unwrap();
        let embedding = embedding.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        let values = embedding.values().as_any().downcast_ref::<Float32Array>().unwrap();
        assert_eq!(values.iter().map(|value| value.map(f32::to_bits)).collect::<Vec<_>>(),
            [Some(1.25_f32.to_bits()), None, Some((-0.0_f32).to_bits()), Some(2.0_f32.to_bits())]);
        assert!(!submission.encode().unwrap().as_slice().is_empty());
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
            ("list".into(), Value::list(vec![value.clone(), Value::Null])),
            ("set".into(), Value::Set(vec![value.clone()])),
            ("fixed".into(), Value::FixedSizeList(vec![value.clone()].into())),
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

#[cfg(test)]
#[test]
fn static_patch_and_timed_put_encode_mixed_float_bindings_as_rows_would() {
    let memory = MemoryBudget::new(1 << 25);
    let first = Value::FixedSizeList(vec![Value::F32(-0.0), Value::Null, Value::F32(f32::from_bits(0x7fc00001))].into());
    let last = Value::list(vec![Value::F64(2.0), Value::Null]);
    let start = Value::Utf8("2020-01-01T00:00:00Z".into());
    let decimal = |coefficient: i32| Value::Decimal(rtdb_types::DecimalValue::new(coefficient.to_be_bytes().to_vec(), 9, 2, 32).unwrap());
    let rows = vec![vec![Value::I64(1), first, start.clone(), decimal(123)],
        vec![Value::I64(2), Value::Null, start.clone(), decimal(456)],
        vec![Value::I64(3), last, start, decimal(789)]];
    let page = |rows: &[Vec<Value>]| {
        let types = SqlArguments::from_rows(rows.to_vec(), &memory).unwrap();
        let fields = types.types().iter().enumerate().map(|(index, ty)| rtdb_columnar::to_field(&index.to_string(), ty).unwrap()).collect::<Vec<_>>();
        let columns = types.types().iter().enumerate().map(|(index, ty)| {
            let values = rows.iter().map(|row| row[index].clone()).collect::<Vec<_>>();
            rtdb_columnar::copy_array(ty, &values).unwrap()
        }).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns).unwrap();
        SqlArguments::from_arrow(rtdb_columnar::OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap()
    };
    let mut mixed = page(&rows[..2]);
    mixed.append(SqlArguments::from_rows(rows[2..].to_vec(), &memory).unwrap()).unwrap();
    let boxed = SqlArguments::from_rows(rows.clone(), &memory).unwrap();
    for sql in [
        "PATCH INTO docs FOR VALID_TIME FROM $3 RECORDS {_id: $1, embedding: $2, amount: $4}",
        "INSERT INTO docs (_id, embedding, _valid_from, amount) VALUES ($1, $2, $3, $4)",
    ] {
        let submit = |args| prepare_submission(vec![TxOp::Sql { sql: sql.into(), args: Some(args) }],
            SubmitOptions::default(), "xtdb", &memory).unwrap();
        let columnar = submit(mixed.clone());
        let row = submit(boxed.clone());
        assert_eq!(columnar.encode().unwrap().as_slice(), row.encode().unwrap().as_slice());
        assert_eq!(columnar.body_digest().unwrap(), row.body_digest().unwrap());
        let ([Operation::PutReplayArrow { documents, .. }] | [Operation::PatchArrowIids { documents, .. }]) =
            columnar.operations.as_slice() else { panic!("Expected static Arrow documents"); };
        assert_eq!(documents.column_type("amount"), Some(&LogicalType::Scalar(ScalarType::Decimal {
            precision: 32, scale: 2, bit_width: 128,
        })));
        let plan = rtdb_sql::prepare_static(sql, &SubmissionCatalog("xtdb"),
            &rtdb_sql::PrepareOptions { parameter_types: mixed.execution_types().enumerate().collect(), ..Default::default() }).unwrap().unwrap();
        let (index, _) = plan.input.schema().fields().iter().enumerate().find(|(_, field)| field.name == "embedding").unwrap();
        let (_, binding, source_types) = mixed.indexed_bound_rows_with_source_types().next().unwrap().unwrap();
        let BoundArgumentRow::Arrow { batch, row, types } = binding else { panic!("Arrow arguments"); };
        let args = rtdb_query::BoundParameters::from_arrow(batch.clone(), row, types.to_vec(), &memory).unwrap();
        let context = EvalContext { parameters: args.evaluator(), ..EvalContext::default() };
        materialize(&plan.input, Some(StaticArrowRow { batch: batch.batch(), row, types: source_types.unwrap() }), &context, &mut |projected| {
            let DocumentCell::Arrow { cell, .. } = &projected[index] else { panic!("Expected a borrowed vector"); };
            assert!(std::ptr::eq(cell.array, batch.batch().column(1).as_ref()));
            Ok(())
        }).unwrap();
    }
}

#[test]
fn static_patch_records_selects_the_vector_child_of_a_bound_arrow_record() {
    let memory = MemoryBudget::new(1 << 25);
    let value = Value::Struct(StructFields::from([
        ("_id".into(), Value::I64(1)),
        ("embedding".into(), Value::FixedSizeList(vec![Value::F32(-0.0), Value::Null].into())),
    ]));
    let ty = value.logical_type();
    let field = rtdb_columnar::to_field("record", &ty).unwrap();
    let array = rtdb_columnar::copy_array(&ty, std::slice::from_ref(&value)).unwrap();
    let batch = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(vec![field])), vec![array.clone()]).unwrap();
    let arguments = SqlArguments::from_arrow(rtdb_columnar::OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();
    let sql = "PATCH INTO docs RECORDS $1";
    let plan = rtdb_sql::prepare_static(sql, &SubmissionCatalog("xtdb"),
        &rtdb_sql::PrepareOptions { parameter_types: arguments.execution_types().enumerate().collect(), ..Default::default() }).unwrap().unwrap();
    let (_, binding, source_types) = arguments.indexed_bound_rows_with_source_types().next().unwrap().unwrap();
    let BoundArgumentRow::Arrow { batch, row, types } = binding else { panic!("Expected Arrow"); };
    let args = rtdb_query::BoundParameters::from_arrow(batch.clone(), row, types.to_vec(), &memory).unwrap();
    let context = EvalContext { parameters: args.evaluator(), ..EvalContext::default() };
    let record = batch.batch().column(0).as_any().downcast_ref::<arrow_array::StructArray>().unwrap();
    materialize(&plan.input, Some(StaticArrowRow { batch: batch.batch(), row, types: source_types.unwrap() }), &context, &mut |cells| {
        for (field, selected) in plan.input.schema().fields().iter().zip(&cells) {
            let DocumentCell::Arrow { cell, .. } = selected else { panic!("Expected an Arrow child"); };
            assert!(std::ptr::eq(cell.array, record.column_by_name(&field.name).unwrap().as_ref()));
        }
        Ok(())
    }).unwrap();
    let submit = |args| prepare_submission(vec![TxOp::Sql { sql: sql.into(), args: Some(args) }],
        SubmitOptions::default(), "xtdb", &memory).unwrap();
    let row = submit(SqlArguments::from_rows(vec![vec![value]], &memory).unwrap());
    let arrow = submit(arguments);
    assert_eq!(arrow.encode().unwrap().as_slice(), row.encode().unwrap().as_slice());
    assert_eq!(arrow.body_digest().unwrap(), row.body_digest().unwrap());
}

#[test]
fn static_patch_mixed_primitive_bindings_keep_bounds_presence_and_receipt_bytes() {
    let memory = MemoryBudget::new(1 << 24);
    let bound = |value| Value::Timestamp { value, unit: TimeUnit::Nanosecond, timezone: Some("America/New_York".into()) };
    let rows = vec![
        vec![Value::I64(1), Value::Utf8("selected".into()), bound(-1999), bound(9001)],
        vec![Value::I32(2), Value::Null, bound(-1999), bound(9001)],
        vec![Value::I64(3), Value::I32(17), bound(9001), Value::Null],
    ];
    let page = |row: &Vec<Value>| {
        let fields = row.iter().enumerate().map(|(index, value)| rtdb_columnar::to_field(&index.to_string(), &value.logical_type()).unwrap()).collect::<Vec<_>>();
        let columns = row.iter().map(|value| rtdb_columnar::copy_array(&value.logical_type(), std::slice::from_ref(value)).unwrap()).collect();
        let batch = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns).unwrap();
        SqlArguments::from_arrow(rtdb_columnar::OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap()
    };
    let mut arguments = page(&rows[0]);
    for row in &rows[1..] { arguments.append(page(row)).unwrap(); }
    let sql = "PATCH INTO docs FOR VALID_TIME FROM $3 TO $4 RECORDS {_id: $1, payload: $2}";
    let submit = |args| prepare_submission(vec![TxOp::Sql { sql: sql.into(), args: Some(args) }],
        SubmitOptions::default(), "xtdb", &memory).unwrap();
    let arrow = submit(arguments);
    let row = submit(SqlArguments::from_rows(rows, &memory).unwrap());
    let [
        Operation::PatchArrowIids { documents, iids, valid_from: Some(from), valid_to: Some(to), .. },
        Operation::PatchArrowIids { valid_from: Some(next), valid_to: None, .. },
    ] = arrow.operations.as_slice() else { panic!("Expected two ordered PATCH groups"); };
    assert_eq!((*from, *to, *next), (Timestamp(-2), Timestamp(9), Timestamp(9)));
    assert_eq!(iids.as_slice(), &[rtdb_types::EntityId::Integer(1).iid(), rtdb_types::EntityId::Integer(2).iid()]);
    assert!(documents.column_type("payload").unwrap().nullable());
    assert_eq!(arrow.encode().unwrap().as_slice(), row.encode().unwrap().as_slice());
    assert_eq!(arrow.body_digest().unwrap(), row.body_digest().unwrap());
}

#[test]
fn canonical_arrow_row_keeps_legacy_sql_receipt_digests() {
    let memory = MemoryBudget::new(16 << 20);
    let values = vec![Value::I64(42), Value::list(vec![Value::I32(4), Value::Null, Value::I32(8)])];
    let fields = values.iter().enumerate().map(|(index, value)| {
        rtdb_columnar::to_field(&format!("?_{index}"), &value.logical_type()).unwrap()
    }).collect::<Vec<_>>();
    let columns = values.iter().map(|value| {
        rtdb_columnar::copy_array(&value.logical_type(), std::slice::from_ref(value)).unwrap()
    }).collect::<Vec<_>>();
    let batch = RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(fields)), columns).unwrap();
    let arrow = SqlArguments::from_canonical_arrow_row(rtdb_columnar::OwnedBatch::new(batch, &memory).unwrap(), &memory).unwrap();
    let rows = SqlArguments::from_rows(vec![values], &memory).unwrap();
    for sql in ["UPDATE docs SET vector = $2 WHERE _id = $1", "INSERT INTO docs (_id, vector) VALUES ($1, $2)"] {
        let prepare = |args| prepare_submission(
            vec![TxOp::Sql { sql: sql.into(), args: Some(args) }],
            SubmitOptions::default(), "xtdb", &memory,
        ).unwrap();
        let actual = prepare(arrow.clone());
        let expected = prepare(rows.clone());
        assert_eq!(actual.body_digest().unwrap(), expected.body_digest().unwrap(), "{sql}");
    }
}
