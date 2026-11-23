use rtdb_plan::{CastTarget, Expr};
use rtdb_types::{Anomaly, LogicalType, Result, ScalarType, TableRef, Value};
use crate::{EvalContext, EvaluationScope};

/// Visits relation identities from the captured evaluation catalog.
/// Reserve enumeration scratch in `allocation` before creating it; borrowed names last only through `visit`.
pub trait PgCatalogLookup: Send + Sync {
    fn relations(&self, allocation: &EvaluationScope, visit: &mut dyn FnMut(i32, &TableRef) -> Result<()>) -> Result<()>;
}

pub(crate) fn relation_text(oid: i32, context: &EvalContext<'_>) -> Result<String> {
    let Some(catalog) = context.catalog else { return Ok(oid.to_string()); };
    let allocation = context.allocation.ok_or_else(|| Anomaly::fault("rtdb.expr/catalog-allocation", "Catalog lookup requires an allocation scope"))?;
    let mut name = None;
    catalog.relations(allocation, &mut |candidate, table| {
        if candidate == oid {
            allocation.reserve(table.schema.len().checked_add(table.table.len()).and_then(|bytes| bytes.checked_add(256)).ok_or_else(crate::numeric::overflow)?)?;
            name = Some(if matches!(table.schema.as_str(), "public" | "pg_catalog") { table.table.clone() } else { format!("{}.{}", table.schema, table.table) });
        }
        Ok(())
    })?;
    Ok(name.unwrap_or_else(|| oid.to_string()))
}

pub(crate) fn relation_oid(name: &str, context: &EvalContext<'_>) -> Result<i32> {
    let Some(catalog) = context.catalog else { return Err(unknown_relation(name)); };
    let allocation = context.allocation.ok_or_else(|| Anomaly::fault("rtdb.expr/catalog-allocation", "Catalog lookup requires an allocation scope"))?;
    let (schema, table) = name.rsplit_once('.').map_or((None, name), |(schema, table)| (Some(schema), table));
    let mut explicit = None; let mut public = None; let mut system = None;
    catalog.relations(allocation, &mut |oid, relation| {
        if relation.table == table { match schema {
            Some(schema) if schema == relation.schema => explicit = Some(oid),
            None if relation.schema == "pg_catalog" => system = Some(oid),
            None if relation.schema == "public" => public = Some(oid),
            _ => (),
        } }
        Ok(())
    })?;
    explicit.or(system).or(public).ok_or_else(|| unknown_relation(name))
}
fn unknown_relation(name: &str) -> Anomaly { Anomaly::incorrect("xtdb.expression.pg/unknown-relation", format!("Relation {name} does not exist")) }
pub(crate) fn proc_text(oid: i32) -> String { if oid == rtdb_types::pg_name_oid("array_in") { "array_in".into() } else { oid.to_string() } }
pub(crate) fn proc_oid(name: &str) -> Result<i32> {
    if matches!(name, "array_in" | "pg_catalog.array_in") { Ok(rtdb_types::pg_name_oid("array_in")) }
    else { Err(Anomaly::incorrect("xtdb.expression.pg/unknown-proc", format!("Procedure {name} does not exist"))) }
}

/// Reports whether evaluation needs relation names from its captured catalog.
/// Parameter types must be bound; callers with untyped parameters must also inspect their supplied values.
pub fn requires_pg_catalog(expression: &Expr) -> bool {
    fn contains(ty: &LogicalType) -> bool { match ty {
        LogicalType::Scalar(ScalarType::RegClass) => true,
        LogicalType::Maybe(inner) | LogicalType::RunEndEncoded { value: inner, .. } | LogicalType::List { element: inner, .. } => contains(inner),
        LogicalType::Union(legs) => legs.iter().any(contains), _ => false,
    } }
    fn requires_target(target: &CastTarget) -> bool { match target { CastTarget::Fixed(ty) => contains(ty), CastTarget::List(inner) | CastTarget::Checked { target: inner, .. } => requires_target(inner), _ => false } }
    let mut required = false;
    expression.visit(&mut |expression| match expression {
        Expr::Cast { expr, target: destination } => required |= requires_target(destination) || contains(&expr.data_type()),
        Expr::Call { name, args, .. } if name.eq_ignore_ascii_case("str") => required |= args.iter().any(|expr| contains(&expr.data_type())),
        _ => (),
    });
    required
}

pub(crate) fn cast(value: &Value, target: &CastTarget, context: &EvalContext<'_>) -> Result<Value> {
    if matches!(value, Value::Null | Value::Undefined) { return Ok(Value::Null); }
    target.validate_source(&value.logical_type())?;
    match target {
        CastTarget::Checked { target: inner, .. } => return cast(value, inner, context),
        CastTarget::Temporal(target) => return crate::temporal_cast::with_context(value,target,context),
        CastTarget::Deferred(target) => return match target.resolve_value(&value.logical_type())? {
            CastTarget::Temporal(target) => crate::temporal_cast::with_context_resolved(value,&target,context),
            target => cast(value,&target,context),
        },
        CastTarget::Fixed(LogicalType::Scalar(ScalarType::Utf8)) => match value {
            Value::RegClass(oid) => return Ok(Value::Utf8(relation_text(*oid, context)?)),
            Value::RegProc(oid) => return Ok(Value::Utf8(proc_text(*oid))), _ => (),
        },
        CastTarget::Fixed(LogicalType::Scalar(ScalarType::RegClass)) => if let Value::Utf8(name) = value { return Ok(Value::RegClass(relation_oid(name, context)?)); },
        CastTarget::Fixed(LogicalType::Scalar(ScalarType::RegProc)) => if let Value::Utf8(name) = value { return Ok(Value::RegProc(proc_oid(name)?)); },
        CastTarget::List(element) if element.union_target().is_none() => {
            if let Value::List(list) | Value::FixedSizeList(list) = value
                && let Some((field, values)) = crate::float_list_cast::packed(list)
                && let Some(list) = crate::float_list_cast::cast(&field, values.as_ref(), element, |bytes| context.reserve(bytes))?
            {
                return Ok(Value::List(list));
            }
            if let Value::List(list) | Value::FixedSizeList(list) = value
                && let Some(list) = crate::list_cast::numeric(
                    crate::list_cast::Input::Stored(list.view()), element, false, |bytes| context.reserve(bytes),
                )?
            {
                return Ok(Value::List(list));
            }
            let parsed;
            let values = match value {
                Value::List(_) | Value::FixedSizeList(_) => value.list_view().unwrap(),
                Value::Utf8(text) => { parsed = crate::cast::pg_array_text(text)?; rtdb_types::ListView::Values(&parsed) },
                _ => return crate::cast::cast_target(value, target, context.timezone),
            };
            if let Some(list) = crate::scalar_list_cast::cast_list(
                crate::scalar_list_cast::Source::Stored(values), element, context.timezone, Some(context.current_time), false, |bytes| context.reserve(bytes),
            )? {
                return Ok(Value::List(list));
            }
            return Ok(Value::list(values.iter().map(|value| cast(value.as_ref(), element, context)).collect::<Result<_>>()?));
        }
        _ => (),
    }
    crate::cast::cast_target(value, target, context.timezone)
}
