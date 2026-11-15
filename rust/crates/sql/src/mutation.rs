use std::collections::BTreeMap;
use std::sync::Arc;

use rtdb_plan::*;
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, TimeUnit, Value};

#[derive(Clone, Debug)]
pub enum Mutation {
    Put { table: QualifiedTable, input: Arc<Relation> },
    Delete { table: QualifiedTable, input: Arc<Relation> },
    Erase { table: QualifiedTable, input: Arc<Relation> },
    Assert { input: Arc<Relation>, message: Option<String> },
    CreateTable { table: QualifiedTable, columns: Vec<String> },
    GrantRole { user: String, role: String, revoke: bool },
}

#[derive(Clone, Debug)]
pub struct StaticMutation {
    pub table: QualifiedTable,
    pub input: Arc<Relation>,
    pub patch: bool,
    pub valid_time: TemporalFilter,
}

/// Returns an expression-only source for literal/parameter INSERT and PATCH statements.
/// A syntax error remains an error; a data-dependent statement returns `None`.
pub fn prepare_static(sql: &str, catalog: &dyn Catalog, options: &crate::PrepareOptions) -> Result<Option<StaticMutation>> {
    let mut parsed = crate::parse(sql)?;
    if parsed.len() != 1 { return Err(invalid("A transaction operation must contain one statement")); }
    let statement = parsed.remove(0);
    let eligible = match &statement.statement {
        crate::parse::Syntax::Records { .. } => true,
        crate::parse::Syntax::Standard(sqlparser::ast::Statement::Insert(insert)) => insert.source.as_ref().is_some_and(|query| matches!(query.body.as_ref(), sqlparser::ast::SetExpr::Values(_)) && query.with.is_none() && query.order_by.is_none() && query.limit_clause.is_none()),
        _ => false,
    };
    if !eligible { return Ok(None); }
    let prepared = crate::bind::bind(statement, sql, catalog, options)?;
    let output = match prepared.statement {
        Statement::Dml(Dml::Insert { table, input }) => StaticMutation { table, input, patch: false, valid_time: TemporalFilter::Default },
        Statement::Dml(Dml::Patch { table, input, valid_time }) => StaticMutation { table, input, patch: true, valid_time },
        _ => return Ok(None),
    };
    fn expression(expr: &Expr) -> bool {
        match expr {
            Expr::Literal(_) | Expr::Parameter { .. } | Expr::Column { .. } => true,
            Expr::Cast { expr, .. } | Expr::Field { expr, .. } | Expr::Unary { op: UnaryOp::Negate, expr } => expression(expr),
            Expr::List(values) => values.iter().all(expression),
            Expr::Struct(fields) => fields.iter().all(|(_, expr)| expression(expr)),
            _ => false,
        }
    }
    fn relation(input: &Relation) -> bool {
        matches!(input.node(), RelationNode::Values { .. } | RelationNode::Project { .. }) && input.expressions().into_iter().all(expression) && input.children().into_iter().all(|child| relation(child))
    }
    Ok((relation(&output.input) && output.valid_time.expressions().into_iter().all(expression)).then_some(output))
}

pub fn prepare_tx(sql: &str, catalog: &dyn Catalog, options: &crate::PrepareOptions) -> Result<Mutation> {
    let prepared = crate::prepare(sql, catalog, options)?;
    let Statement::Dml(dml) = prepared.statement else { return Err(Anomaly::incorrect("xtdb.query/invalid-sql-tx-op", "Invalid SQL query sent as transaction operation")); };
    lower(dml, catalog)
}

pub(crate) fn instant_type() -> LogicalType { LogicalType::Scalar(ScalarType::Timestamp { unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }) }
pub(crate) fn timestamp_literal(value: i64) -> Expr { Expr::Literal(Value::Timestamp { value, unit: TimeUnit::Microsecond, timezone: Some("UTC".into()) }) }

fn lower(dml: Dml, catalog: &dyn Catalog) -> Result<Mutation> {
    Ok(match dml {
        Dml::Insert { table, input } => Mutation::Put { table, input: canonical_put(input, None)? },
        Dml::Update { table, input, assignments, .. } => {
            let mut allocator = allocator(&input)?;
            let old: BTreeMap<_, _> = input.schema().fields().iter().map(|field| (field.name.clone(), field.expression())).collect();
            let mut expressions = Vec::new();
            for field in input.schema().fields() {
                if !assignments.iter().any(|(name, _)| name == &field.name) { expressions.push(NamedExpr { field: field.clone(), expr: field.expression() }); }
            }
            let mut differences = Vec::new();
            for (name, expression) in assignments {
                differences.push(Expr::unary(UnaryOp::Not, Expr::Call { name: "null_eq".into(), args: vec![expression.clone(), old.get(&name).cloned().unwrap_or(Expr::Literal(Value::Null))], return_type: LogicalType::Scalar(ScalarType::Bool) }));
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
    })
}

/// The input exposes `_iid` and a non-null `doc` struct; bounds are evaluated once per execution.
pub fn prepare_patch(table: QualifiedTable, input: Arc<Relation>, valid_time: TemporalFilter, catalog: &dyn Catalog) -> Result<Mutation> {
    let now = Expr::Call { name: "current_timestamp".into(), args: vec![], return_type: instant_type() };
    let (from, to) = match valid_time {
        TemporalFilter::Default => (now, timestamp_literal(i64::MAX)),
        TemporalFilter::All => (timestamp_literal(i64::MIN), timestamp_literal(i64::MAX)),
        TemporalFilter::FromTo { from, to } => (
            instant_cast(Expr::Coalesce(vec![from, timestamp_literal(i64::MIN)])),
            instant_cast(Expr::Coalesce(vec![to, timestamp_literal(i64::MAX)]))),
        _ => return Err(invalid("PATCH requires ALL or FROM/TO valid time")),
    };
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
        Some(table) => Arc::new(Relation::scan(Scan { table, columns: fields, valid_time: TemporalFilter::FromTo { from: from.clone(), to: to.clone() }, system_time: TemporalFilter::Default, clamp_valid_time: true, iids: None })?),
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
    let fields = input.schema().fields().iter().filter(|field| !["_iid", "_valid_from", "_valid_to", "_system_from", "_system_to"].contains(&field.name.as_str())).map(|field| (field.name.clone(), field.expression())).collect();
    let expressions = [("_iid", iid), ("_valid_from", from), ("_valid_to", to), ("doc", Expr::Struct(fields))].into_iter()
        .map(|(name, expr)| Ok(NamedExpr { field: allocator.field(name, None, expr.data_type())?, expr })).collect::<Result<_>>()?;
    Ok(Arc::new(input.project(expressions)?))
}

fn instant_cast(expr: Expr) -> Expr { Expr::Cast { expr: Box::new(expr), data_type: instant_type() } }
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
