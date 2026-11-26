use std::collections::BTreeMap;
use std::sync::Arc;

use rtdb_plan::*;
use rtdb_types::{Anomaly, LogicalType, MemoryBudget, Result, ScalarType, TimeUnit, Value};

#[derive(Clone, Debug)]
pub enum Mutation {
    Put { table: QualifiedTable, input: Arc<Relation> },
    Delete { table: QualifiedTable, input: Arc<Relation> },
    Erase { table: QualifiedTable, input: Arc<Relation> },
    Assert { input: Arc<Relation>, message: Option<String> },
    CreateTable { table: QualifiedTable, columns: Vec<(String, LogicalType)> },
    GrantRole { user: String, role: String, revoke: bool },
    CreateSearchIndex { name: String, table: QualifiedTable, columns: Vec<String>, analyzer: rtdb_plan::Analyzer, history: bool, if_not_exists: bool },
    DropSearchIndex { name: String },
    CreateOrderedIndex { name: String, table: QualifiedTable, columns: Vec<String>, if_not_exists: bool },
    DropOrderedIndex { name: String },
    CreateVectorIndex { name: String, table: QualifiedTable, column: String, partition: Vec<String>, dimensions: i64, method: String, recall: f64, if_not_exists: bool },
    DropVectorIndex { name: String },
}

#[derive(Clone, Debug)]
pub struct StaticMutation {
    pub table: QualifiedTable,
    pub input: Arc<Relation>,
    pub kind: StaticMutationKind,
}

#[derive(Clone, Debug)]
pub enum StaticMutationKind {
    Put,
    Patch(TemporalFilter),
}

impl StaticMutation {
    pub fn valid_time(&self) -> &TemporalFilter {
        match &self.kind {
            StaticMutationKind::Put => &TemporalFilter::Default,
            StaticMutationKind::Patch(valid_time) => valid_time,
        }
    }
}

/// Returns an expression-only source for literal/parameter INSERT and PATCH statements.
/// A syntax error remains an error; a data-dependent statement returns `None`.
pub fn prepare_static(sql: &str, catalog: &dyn Catalog, options: &crate::PrepareOptions) -> Result<Option<StaticMutation>> {
    prepare_static_with(sql, catalog, options, crate::bind::FormAllocation::Untracked)
}

/// Preserves static eligibility and error order while admitting reached raw owners in the supplied budget.
pub fn prepare_static_in(sql: &str, catalog: &dyn Catalog, options: &crate::PrepareOptions, memory: &Arc<MemoryBudget>) -> Result<Option<StaticMutation>> {
    prepare_static_with(sql, catalog, options, crate::bind::FormAllocation::Tracked(memory))
}

fn prepare_static_with(sql: &str, catalog: &dyn Catalog, options: &crate::PrepareOptions, forms: crate::bind::FormAllocation<'_>) -> Result<Option<StaticMutation>> {
    let mut parsed = crate::parse::parse_with(sql, forms)?;
    if parsed.len() != 1 { return Err(invalid("A transaction operation must contain one statement")); }
    let statement = parsed.remove(0);
    Ok(crate::bind::bind_static_document_source(statement, catalog, options)?.filter(static_expressions))
}

fn static_expressions(output: &StaticMutation) -> bool {
    fn decimal_target(target: &CastTarget) -> bool {
        match target {
            CastTarget::DecimalDefault | CastTarget::Fixed(LogicalType::Scalar(ScalarType::Decimal { .. })) => true,
            CastTarget::List(element) | CastTarget::Checked { target: element, .. } => decimal_target(element),
            _ => false,
        }
    }
    fn expression(expr: &Expr) -> bool {
        match expr {
            Expr::Literal(_) | Expr::Parameter { .. } | Expr::Column { .. } => true,
            Expr::Cast { expr, target } => !decimal_target(target) && expression(expr),
            Expr::Field { expr, .. } | Expr::Unary { op: UnaryOp::Negate, expr } => expression(expr),
            Expr::List(values) => values.iter().all(expression),
            Expr::Struct(fields) => fields.iter().all(|(_, expr)| expression(expr)),
            _ => false,
        }
    }
    fn relation(input: &Relation) -> bool {
        matches!(input.node(), RelationNode::Values { .. } | RelationNode::Project { .. } | RelationNode::Rename { .. }) && input.expressions().into_iter().all(expression) && input.children().into_iter().all(|child| relation(child))
    }
    relation(&output.input) && output.valid_time().expressions().into_iter().all(expression)
}

pub fn prepare_tx(sql: &str, catalog: &dyn Catalog, options: &crate::PrepareOptions) -> Result<Mutation> {
    prepare_tx_with(sql, catalog, options, crate::bind::FormAllocation::Untracked)
}

/// Lowers a transaction source with independently retained raw owners charged to the existing caller budget.
pub fn prepare_tx_in(sql: &str, catalog: &dyn Catalog, options: &crate::PrepareOptions, memory: &Arc<MemoryBudget>) -> Result<Mutation> {
    prepare_tx_with(sql, catalog, options, crate::bind::FormAllocation::Tracked(memory))
}

fn prepare_tx_with(sql: &str, catalog: &dyn Catalog, options: &crate::PrepareOptions, forms: crate::bind::FormAllocation<'_>) -> Result<Mutation> {
    let prepared = crate::prepare_with(sql, catalog, options, forms)?;
    let Statement::Dml(dml) = prepared.statement else { return Err(Anomaly::incorrect("xtdb.query/invalid-sql-tx-op", "Invalid SQL query sent as transaction operation")); };
    lower(dml, catalog)
}

pub(crate) fn instant_type() -> LogicalType { LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }) }
pub(crate) fn timestamp_literal(value: i64) -> Expr { Expr::Literal(Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }) }

fn lower(dml: Dml, catalog: &dyn Catalog) -> Result<Mutation> {
    Ok(match dml {
        Dml::Insert { table, input } => Mutation::Put { table, input: canonical_put(input, None)? },
        Dml::Update { table, input, target_columns, assignments, .. } => {
            let mut allocator = allocator(&input)?;
            let old: BTreeMap<_, _> = input.schema().fields().iter().filter(|field| target_columns.contains(&field.id)).map(|field| (field.name.clone(), field.expression())).collect();
            let mut expressions = Vec::new();
            for field in input.schema().fields().iter().filter(|field| target_columns.contains(&field.id)) {
                if !assignments.iter().any(|(name, _)| name == &field.name) { expressions.push(NamedExpr { field: field.clone(), expr: field.expression() }); }
            }
            let mut differences = Vec::new();
            for (name, expression) in assignments {
                differences.push(Expr::unary(UnaryOp::Not, Expr::Call {
                    name: "===".into(),
                    args: vec![expression.clone(), old.get(&name).cloned().unwrap_or(Expr::Literal(Value::Null))],
                    return_type: LogicalType::Scalar(ScalarType::Bool),
                }));
                expressions.push(NamedExpr { field: allocator.field(name, None, expression.data_type())?, expr: expression });
            }
            let differs = differences.into_iter().reduce(|left, right| Expr::binary(BinaryOp::Or, left, right)).unwrap_or(Expr::Literal(Value::Bool(false)));
            let changed = Arc::new(input.filter(differs)?);
            let updated = Arc::new(changed.project(expressions)?);
            Mutation::Put { table, input: canonical_put(updated, Some("_iid"))? }
        }
        Dml::Delete { table, input, .. } => Mutation::Delete { table, input: select(input, &["_iid", "_valid_from", "_valid_to"])? },
        Dml::Erase { table, input } => {
            let input = select(input, &["_iid"])?;
            Mutation::Erase { table, input: Arc::new(Relation::new(input.schema().clone(), RelationNode::Distinct { input })?) }
        }
        Dml::Assert { input, message } => Mutation::Assert { input, message },
        Dml::Patch { table, input, valid_time } => {
            let mut binder = allocator(&input)?;
            let id = input.schema().fields().iter().find(|field| field.name == "_id").ok_or_else(|| invalid("PATCH requires _id"))?.expression();
            let iid = Expr::Call { name: "_iid".into(), args: vec![id], return_type: LogicalType::Scalar(ScalarType::FixedSizeBinary(16)) };
            let doc = Expr::Struct(input.schema().fields().iter().map(|field| (field.name.clone(), field.expression())).collect());
            let input = Arc::new(input.project(vec![NamedExpr { field: binder.field("_iid", None, iid.data_type())?, expr: iid }, NamedExpr { field: binder.field("doc", None, doc.data_type())?, expr: doc }])?);
            prepare_patch(table, input, valid_time, catalog)?
        }
        Dml::CreateTable { table, columns } => Mutation::CreateTable { table, columns },
        Dml::GrantRole { user, role, revoke } => Mutation::GrantRole { user, role, revoke },
        Dml::CreateSearchIndex { name, table, columns, analyzer, history, if_not_exists } => Mutation::CreateSearchIndex { name, table, columns, analyzer, history, if_not_exists },
        Dml::DropSearchIndex { name } => Mutation::DropSearchIndex { name },
        Dml::CreateOrderedIndex { name, table, columns, if_not_exists } => Mutation::CreateOrderedIndex { name, table, columns, if_not_exists },
        Dml::DropOrderedIndex { name } => Mutation::DropOrderedIndex { name },
        Dml::CreateVectorIndex { name, table, column, partition, dimensions, method, recall, if_not_exists } =>
            Mutation::CreateVectorIndex { name, table, column, partition, dimensions, method, recall, if_not_exists },
        Dml::DropVectorIndex { name } => Mutation::DropVectorIndex { name },
    })
}

/// The input exposes `_iid` and a non-null `doc` struct.
/// Endpoints must be datetime literals, direct parameters or a zero-argument current timestamp; unsupported expressions return a planning anomaly.
/// Literal NULL clause endpoints become start/end-of-time, and omitted valid time starts at the captured query clock.
pub fn prepare_patch(table: QualifiedTable, input: Arc<Relation>, valid_time: TemporalFilter, catalog: &dyn Catalog) -> Result<Mutation> {
    let now = Expr::Call { name: "current_timestamp".into(), args: vec![], return_type: instant_type() };
    let (from, to) = match valid_time {
        TemporalFilter::Default => (now, timestamp_literal(i64::MAX)),
        TemporalFilter::All => (timestamp_literal(i64::MIN), timestamp_literal(i64::MAX)),
        TemporalFilter::FromTo { from, to } => (patch_bound(from, i64::MIN), patch_bound(to, i64::MAX)),
        _ => return Err(invalid("PATCH requires ALL or FROM/TO valid time")),
    };
    PatchEndpoint::from_expr(&from)?;
    PatchEndpoint::from_expr(&to)?;
    let new_iid = input.schema().fields().iter().find(|field| field.name == "_iid").ok_or_else(|| invalid("PATCH input has no IID"))?.expression();
    let new_doc = input.schema().fields().iter().find(|field| field.name == "doc").ok_or_else(|| invalid("PATCH input has no document"))?.expression();
    let mut binder = allocator(&input)?;
    let binding = catalog.table(&table)?;
    let mut columns = binding.as_ref().map(|binding| binding.columns.clone()).unwrap_or_default();
    let content: Vec<_> = columns.keys().filter(|name| !["_iid", "_valid_from", "_valid_to", "_system_from", "_system_to"].contains(&name.as_str())).cloned().collect();
    columns.insert("_iid".into(), LogicalType::Scalar(ScalarType::FixedSizeBinary(16)));
    columns.insert("_valid_from".into(), instant_type());
    columns.insert("_valid_to".into(), instant_type().maybe());
    let fields = columns.into_iter().map(|(name, ty)| binder.field(name, None, ty)).collect::<Result<Vec<_>>>()?;
    let old = match binding {
        Some(table) => Arc::new(Relation::scan(Scan { table, columns: fields, valid_time: TemporalFilter::FromTo { from: from.clone(), to: to.clone() }, system_time: TemporalFilter::Default, clamp_valid_time: true, iids: None, predicates: Vec::new(), top_k: None, ordered_top_k: None, matches: Vec::new() })?),
        None => Arc::new(Relation::empty(Schema::new(fields)?)),
    };
    let find = |name: &str| old.schema().fields().iter().find(|field| field.name == name).map(Field::expression).ok_or_else(|| invalid("PATCH old column is absent"));
    let old_doc = Expr::Struct(content.into_iter().map(|name| Ok((name.clone(), find(&name)?))).collect::<Result<_>>()?);
    let old = Arc::new(old.clone().project([("_iid", find("_iid")?), ("_valid_from", find("_valid_from")?), ("_valid_to", find("_valid_to")?), ("doc", old_doc)].into_iter().map(|(name, expr)| Ok(NamedExpr { field: binder.field(name, None, expr.data_type())?, expr })).collect::<Result<_>>()?)?);
    let [old_iid, old_from, old_to, old_doc]: [Field; 4] = old.schema().fields().to_vec().try_into().map_err(|_| invalid("PATCH old shape"))?;
    let old = Arc::new(old.sort(vec![SortExpr { expr: old_iid.expression(), descending: false, nulls_first: false }, SortExpr { expr: old_from.expression(), descending: false, nulls_first: false }])?);
    let schema = Schema::new(old.schema().fields().iter().cloned().map(|mut field| { if field.id == old_doc.id { field.data_type = field.data_type.maybe(); } field }).collect())?;
    let old = Arc::new(Relation::new(schema, RelationNode::PatchGaps { input: old, iid: old_iid.id, valid_from: old_from.id, valid_to: old_to.id, document: old_doc.id, from: from.clone(), to: to.clone() })?);
    let schema = input.schema().concat(&old.schema().nullable()?)?;
    let joined = Arc::new(Relation::new(schema, RelationNode::Join { left: input, right: old, kind: JoinKind::Left, condition: Some(Expr::binary(BinaryOp::Eq, new_iid.clone(), old_iid.expression())) })?);
    let doc_args = vec![Expr::column(old_doc.id, old_doc.data_type.maybe()), new_doc];
    let doc = Expr::Call { name: "_patch".into(), return_type: function_type("_patch", &doc_args.iter().map(Expr::data_type).collect::<Vec<_>>())?, args: doc_args };
    let expressions = [("_iid", new_iid), ("_valid_from", instant_cast(Expr::Coalesce(vec![old_from.expression(), from]))), ("_valid_to", instant_cast(Expr::Coalesce(vec![old_to.expression(), to]))), ("doc", doc)].into_iter().map(|(name, expr)| Ok(NamedExpr { field: binder.field(name, None, expr.data_type())?, expr })).collect::<Result<_>>()?;
    Ok(Mutation::Put { table, input: Arc::new(joined.project(expressions)?) })
}

fn patch_bound(expression: Expr, fallback: i64) -> Expr {
    match expression {
        Expr::Literal(Value::Null | Value::Bool(false)) => timestamp_literal(fallback),
        Expr::Cast { expr, target: CastTarget::Temporal(rtdb_plan::TemporalCast { kind: rtdb_plan::TemporalKind::TimestampZone(_), precision: None, .. }) }
            if matches!(expr.as_ref(), Expr::Call { name, args, .. } if matches!(name.as_str(), "current_timestamp" | "now") && args.is_empty()) => patch_bound(*expr, fallback),
        Expr::Call { name,args,return_type } if name == "now" && args.is_empty() => Expr::Call { name: "current_timestamp".into(),args,return_type },
        expression => expression,
    }
}

fn canonical_put(input: Arc<Relation>, iid_name: Option<&str>) -> Result<Arc<Relation>> {
    let mut allocator = allocator(&input)?;
    let find = |name: &str| input.schema().fields().iter().find(|field| field.name == name).map(Field::expression);
    let iid = match iid_name {
        Some(name) => find(name).ok_or_else(|| invalid("Mutation input has no IID"))?,
        None => {
            let id = find("_id").ok_or_else(|| invalid("INSERT requires an _id column"))?;
            Expr::Call { name: "_iid".into(), args: vec![id], return_type: LogicalType::Scalar(ScalarType::FixedSizeBinary(16)) }
        }
    };
    let now = Expr::Call { name: "current_timestamp".into(), args: vec![], return_type: instant_type() };
    let from = instant_cast(Expr::Coalesce(vec![find("_valid_from").unwrap_or(Expr::Literal(Value::Null)), now]));
    let to = instant_cast(Expr::Coalesce(vec![find("_valid_to").unwrap_or(Expr::Literal(Value::Null)), timestamp_literal(i64::MAX)]));
    let fields = input.schema().fields().iter().filter(|field| !["_iid", "_valid_from", "_valid_to", "_system_from", "_system_to"].contains(&field.name.as_str())).cloned().collect();
    let expressions = [("_iid", iid), ("_valid_from", from), ("_valid_to", to), ("doc", Expr::RowCopy(fields))].into_iter()
        .map(|(name, expr)| Ok(NamedExpr { field: allocator.field(name, None, expr.data_type())?, expr })).collect::<Result<_>>()?;
    Ok(Arc::new(input.project(expressions)?))
}

fn instant_cast(expr: Expr) -> Expr { Expr::Cast { expr: Box::new(expr), target: rtdb_plan::CastTarget::fixed(instant_type()) } }
fn select(input: Arc<Relation>, names: &[&str]) -> Result<Arc<Relation>> {
    let expressions = names.iter().map(|name| {
        let field = input.schema().fields().iter().find(|field| field.name == *name).ok_or_else(|| invalid(format!("Mutation input has no {name}")))?;
        Ok(NamedExpr { field: field.clone().unqualified(), expr: field.expression() })
    }).collect::<Result<_>>()?;
    Ok(Arc::new(input.project(expressions)?))
}
fn allocator(input: &Relation) -> Result<Binder> {
    Binder::after(input.schema())
}
fn invalid(message: impl Into<String>) -> Anomaly { Anomaly::incorrect("xtdb/sql-error", message) }
