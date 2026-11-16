use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::{AggregateFunction, BinaryOp, ColumnId, Expr, Field, JoinKind, Relation, RelationNode, Schema, UnaryOp};
use rtdb_types::{Keyword, LogicalType, Result, ScalarType, Value};
use crate::invalid;

pub(crate) struct ExplainPlan { pub schema: Schema, rows: Vec<Vec<Value>>, retained_bytes: usize, _reservation: Arc<MemoryReservation> }
impl ExplainPlan {
    pub fn new(relation: &Relation, memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let reservation = memory.reserve(crate::runtime_schema::allocation_bound(relation)?.checked_mul(8).and_then(|bytes| bytes.checked_add(depth_bound(relation, 0).ok()?)).ok_or_else(size_error)?)?;
        let mut rows = Vec::new();
        let descriptor = describe(relation)?;
        descriptor.rows(0, &mut rows);
        let explanation = LogicalType::join_all(&rows.iter().map(|row| row[2].logical_type()).collect::<Vec<_>>());
        let schema = Schema::new(vec![("depth", LogicalType::Scalar(ScalarType::Utf8)), ("op", LogicalType::Scalar(ScalarType::Keyword)), ("explain", explanation)]
            .into_iter().enumerate().map(|(index, (name, data_type))| Field { id: ColumnId(index as u32), name: name.into(), data_type, qualifier: None, qualified_only: false, unnamed: false }).collect())?;
        let retained_bytes = rows.iter().try_fold(4096usize, |bytes, row| row.iter().try_fold(bytes, |bytes, value| bytes.checked_add(value.retained_bytes()?).ok_or_else(size_error)))?;
        if retained_bytes > reservation.bytes() { return Err(invalid("explain-size", "Plan description exceeds its allocation bound")); }
        Ok(Arc::new(Self { schema, rows, retained_bytes, _reservation: reservation }))
    }
    pub fn copy_bound(&self) -> Result<usize> { self.retained_bytes.checked_mul(2).and_then(|n| n.checked_add(self.rows.len().checked_mul(512)?)).ok_or_else(size_error) }
    pub fn rows(&self) -> Vec<Vec<Value>> { self.rows.clone() }
}
fn size_error() -> rtdb_types::Anomaly { invalid("explain-size", "Plan description exceeds addressable memory") }
fn depth_bound(relation: &Relation, depth: usize) -> Result<usize> {
    let own = depth.checked_mul(4).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(size_error)?;
    relation.children().iter().try_fold(own, |bytes, child| bytes.checked_add(depth_bound(child, depth.checked_add(1).ok_or_else(size_error)?)?).ok_or_else(size_error))
}

struct Descriptor { op: &'static str, explanation: Value, children: Vec<Descriptor>, columns: BTreeMap<ColumnId, String> }
impl Descriptor {
    fn rows(self, depth: usize, rows: &mut Vec<Vec<Value>>) {
        rows.push(vec![Value::Utf8(format!("{}->", "  ".repeat(depth))), Value::Keyword(Keyword::from_name(self.op)), self.explanation]);
        for child in self.children { child.rows(depth + 1, rows); }
    }
}
fn describe(relation: &Relation) -> Result<Descriptor> {
    let children = relation.children().iter().map(|child| describe(child)).collect::<Result<Vec<_>>>()?;
    let mut input_columns = BTreeMap::new();
    for child in &children { input_columns.extend(child.columns.iter().map(|(id, name)| (*id, name.clone()))); }
    let symbol = |id: ColumnId| input_columns.get(&id).cloned().unwrap_or_else(|| format!("_column_{}", id.0));
    let expression = |expr| form(expr, &input_columns);
    let text = |text: String| Value::Utf8(text);
    let strings = |values: Vec<String>| Value::List(values.into_iter().map(Value::Utf8).collect());
    let mut op = crate::analysis::operator(relation);
    let explanation = match relation.node() {
        RelationNode::Project { expressions, .. } => map([
            ("append?", Value::Bool(false)),
            ("project", text(format!("[{}]", expressions.iter().map(|named| Ok(format!("{{{} {}}}", named.field.name, expression(&named.expr)?))).collect::<Result<Vec<_>>>()?.join(" ")))),
        ]),
        RelationNode::Rename { prefix, expressions, .. } => {
            let mut fields = BTreeMap::from([("prefix".into(), text(prefix.clone()))]);
            if expressions.iter().any(|named| match &named.expr { Expr::Column { id, .. } => input_columns.get(id).is_some_and(|old| old.rsplit('/').next() != Some(named.field.name.as_str())), _ => true }) {
                fields.insert("columns".into(), text(format!("{{{}}}", expressions.iter().map(|named| Ok(format!("{} {}", named.field.name, expression(&named.expr)?))).collect::<Result<Vec<_>>>()?.join(", "))));
            }
            Value::Struct(fields)
        }
        RelationNode::Scan(scan) => {
            let columns = scan.columns.iter().map(|field| (field.id, field.name.clone())).collect();
            map([
                ("table", text(format!("{}.{}.{}", scan.table.name.database, scan.table.name.table.schema, scan.table.name.table.table))),
                ("columns", strings(scan.columns.iter().map(|field| field.name.clone()).collect())),
                ("predicates", strings(scan.predicates.iter().map(|predicate| form(predicate, &columns)).collect::<Result<_>>()?)),
            ])
        }
        RelationNode::Filter { predicate, .. } => map([("predicate", text(expression(predicate)?))]),
        RelationNode::SortLimit { order_by, offset, fetch, .. } => {
            let mut fields = BTreeMap::new();
            if !order_by.is_empty() { fields.insert("order-specs".into(), text(format!("[{}]", order_by.iter().map(|order| Ok(format!("[{} {{:direction :{}, :null-ordering :{}}}]", expression(&order.expr)?, if order.descending { "desc" } else { "asc" }, if order.nulls_first { "nulls-first" } else { "nulls-last" }))).collect::<Result<Vec<_>>>()?.join(" ")))); }
            if let Some(offset) = offset { fields.insert("skip".into(), text(expression(offset)?)); }
            if let Some(fetch) = fetch { fields.insert("limit".into(), text(expression(fetch)?)); }
            Value::Struct(fields)
        }
        RelationNode::PatchGaps { from, to, .. } => map([("valid-from", text(patch_endpoint(from,expression(from)?)?)), ("valid-to", text(patch_endpoint(to,expression(to)?)?))]),
        RelationNode::Join { kind, condition, .. } => {
            op = match kind { JoinKind::Inner => "inner-join", JoinKind::Cross => "cross-join", JoinKind::Left | JoinKind::Right => "left-join", JoinKind::Full => "full-join", JoinKind::Semi => "semi-join", JoinKind::Anti => "anti-join" };
            map([("condition", text(format!("[{}]", condition.as_ref().map(expression).transpose()?.unwrap_or_default())))])
        }
        RelationNode::Aggregate { groups, aggregates, .. } => map([
            ("group-by", strings(groups.iter().map(|group| expression(&group.expr)).collect::<Result<_>>()?)),
            ("aggregates", Value::List(aggregates.iter().map(|aggregate| Ok(Value::List(vec![text(aggregate.field.name.clone()), text(aggregate_form(aggregate, &input_columns)?)]))).collect::<Result<_>>()?)),
        ]),
        RelationNode::Window { partition_by, order_by, expressions, .. } => map([
            ("partition-by", strings(partition_by.iter().map(|id| symbol(*id)).collect())),
            ("order-by", text(format!("[{}]", order_by.iter().map(|order| Ok(format!("[{} {{:direction :{}, :null-ordering :{}}}]", expression(&order.expr)?, if order.descending { "desc" } else { "asc" }, if order.nulls_first { "nulls-first" } else { "nulls-last" }))).collect::<Result<Vec<_>>>()?.join(" ")))),
            ("window-functions", Value::List(expressions.iter().map(|window| {
                let function = match window.function { rtdb_plan::WindowFunction::RowNumber => "(row-number)".into(), rtdb_plan::WindowFunction::Lead { column, offset } => format!("(lead {} {offset})", symbol(column)), rtdb_plan::WindowFunction::Lag { column, offset } => format!("(lag {} {offset})", symbol(column)) };
                Value::List(vec![text(window.field.name.clone()), text(function)])
            }).collect())),
        ]),
        RelationNode::Unnest { expr, element, ordinality, .. } => {
            let mut fields = BTreeMap::from([("from".into(), text(expression(expr)?)), ("to".into(), text(element.name.clone()))]);
            if let Some(field) = ordinality { fields.insert("ordinality".into(), text(field.name.clone())); }
            Value::Struct(fields)
        }
        RelationNode::Mark { arguments, comparison, output, .. } => map([
            ("columns", text(format!("{{{}}}", arguments.iter().map(|(index, expr)| Ok(format!("{} ?_{}", expression(expr)?, index))).collect::<Result<Vec<_>>>()?.join(", ")))),
            ("mark-spec", text(format!("{{{} [{}]}}", output.name, expression(comparison)?))),
        ]),
        RelationNode::Apply { arguments, .. } => map([("columns", text(format!("{{{}}}", arguments.iter().map(|(index, expr)| Ok(format!("{} ?_{}", expression(expr)?, index))).collect::<Result<Vec<_>>>()?.join(", "))))]),
        RelationNode::Set { operator, .. } => { op = match operator { rtdb_plan::SetOperator::Union => "union-all", rtdb_plan::SetOperator::Intersect => "intersect", rtdb_plan::SetOperator::Except => "difference" }; Value::Null }
        _ => Value::Null,
    };
    let columns = relation.schema().fields().iter().map(|field| {
        let name = match relation.node() {
            RelationNode::Rename { prefix, .. } => format!("{prefix}/{}", field.name),
            RelationNode::Scan(_) | RelationNode::Values { .. } | RelationNode::Project { .. } | RelationNode::Aggregate { .. } | RelationNode::Reference { .. } => field.name.clone(),
            _ => input_columns.get(&field.id).cloned().unwrap_or_else(|| field.name.clone()),
        };
        (field.id, name)
    }).collect();
    Ok(Descriptor { op, explanation, children, columns })
}
fn map<const N: usize>(entries: [(&str, Value); N]) -> Value { Value::Struct(entries.into_iter().map(|(name, value)| (name.into(), value)).collect()) }
fn aggregate_form(aggregate: &rtdb_plan::AggregateExpr, columns: &BTreeMap<ColumnId, String>) -> Result<String> {
    let name = aggregate.function.name();
    let argument = aggregate.argument.as_ref().map(|arg| form(arg, columns)).transpose()?;
    let name = if aggregate.distinct && aggregate.function.quantifier() != Some(true) { format!("{name}_distinct") } else { name.into() };
    let mut parts = vec![name];
    match (&aggregate.function, argument) {
        (AggregateFunction::Percentile { fraction, descending, .. }, Some(argument)) => {
            parts.push(match fraction { rtdb_plan::PercentileFraction::Integer(value) => value.to_string(), rtdb_plan::PercentileFraction::Literal(value) => rtdb_types::java_f64_text(*value), rtdb_plan::PercentileFraction::Parameter { index } => format!("?_{index}") });
            parts.push(format!("[{argument} {{:direction :{}}}]", if *descending { "desc" } else { "asc" }));
        }
        (_, Some(argument)) => parts.push(argument),
        (_, None) => {}
    }
    let mut options = Vec::new();
    if let Some(filter) = &aggregate.filter { options.push(format!(":filter {}", form(filter, columns)?)); }
    if !aggregate.order_by.is_empty() {
        options.push(format!(":order-specs [{}]", aggregate.order_by.iter().map(|order| Ok(format!("[{} {{:direction :{}, :null-ordering :{}}}]", form(&order.expr, columns)?, if order.descending { "desc" } else { "asc" }, if order.nulls_first { "nulls-first" } else { "nulls-last" }))).collect::<Result<Vec<_>>>()?.join(" ")));
    }
    if !options.is_empty() { parts.push(format!("{{{}}}", options.join(", "))); }
    Ok(format!("({})", parts.join(" ")))
}
fn patch_endpoint(expr: &Expr, rendered: String) -> Result<String> {
    Ok(match rtdb_plan::PatchEndpoint::from_expr(expr)? {
        rtdb_plan::PatchEndpoint::Default => "nil".into(),
        rtdb_plan::PatchEndpoint::CurrentTime => format!("[:now {}]",if matches!(expr,Expr::Call {..}) { "(current-timestamp)" } else { &rendered }),
        rtdb_plan::PatchEndpoint::DateTime(_) => format!("[:literal {rendered}]"),
        rtdb_plan::PatchEndpoint::Parameter(_) => format!("[:param {rendered}]"),
    })
}
fn form(expr: &Expr, columns: &BTreeMap<ColumnId, String>) -> Result<String> {
    let child = |expr| form(expr, columns);
    let call = |name: &str, args: Vec<String>| if args.is_empty() { format!("({name})") } else { format!("({name} {})", args.join(" ")) };
    Ok(match expr {
        Expr::Literal(value) => literal(value)?,
        Expr::Column { id, .. } | Expr::Correlated { id, .. } => columns.get(id).cloned().unwrap_or_else(|| format!("_column_{}", id.0)),
        Expr::Parameter { index, .. } => format!("?_{}", index),
        Expr::Unary { op, expr } => call(match op { UnaryOp::Negate => "-", UnaryOp::Not => "not", UnaryOp::BitNot => "bit-not", UnaryOp::IsNull | UnaryOp::IsUnknown => "nil?", UnaryOp::IsNotNull => "some?", UnaryOp::IsTrue => "true?", UnaryOp::IsFalse => "false?" }, vec![child(expr)?]),
        Expr::Binary { op, left, right } => call(match op { BinaryOp::Add => "+", BinaryOp::Subtract => "-", BinaryOp::Multiply => "*", BinaryOp::Divide => "/", BinaryOp::Modulo => "mod", BinaryOp::Eq => "==", BinaryOp::NotEq => "<>", BinaryOp::Lt => "<", BinaryOp::LtEq => "<=", BinaryOp::Gt => ">", BinaryOp::GtEq => ">=", BinaryOp::IsDistinctFrom => "distinct-from", BinaryOp::IsNotDistinctFrom => "not-distinct-from", BinaryOp::And => "and", BinaryOp::Or => "or", BinaryOp::Concat => "concat", BinaryOp::BitAnd => "bit-and", BinaryOp::BitOr => "bit-or", BinaryOp::BitXor => "bit-xor", BinaryOp::ShiftLeft => "bit-shift-left", BinaryOp::ShiftRight => "bit-shift-right" }, vec![child(left)?, child(right)?]),
        Expr::Call { name, args, .. } => call(name, args.iter().map(child).collect::<Result<_>>()?),
        Expr::RegexReplace { source,recipe,.. } => call("regexp-replace",vec![child(source)?,regex_form(&recipe.pattern)?,regex_form(&recipe.replacement)?,regex_form(&recipe.start)?,regex_form(&recipe.occurrence)?,regex_form(&recipe.flags)?]),
        Expr::Cast { expr, target } => {
            let mut args = vec![child(expr)?, match target { rtdb_plan::CastTarget::Checked { requested, .. } => requested.to_edn()?, _ => target.result_type(&expr.data_type()).to_edn()? }];
            if let Some(options) = cast_options(target) { args.push(options); }
            call("cast",args)
        }
        Expr::Coalesce(args) => call("coalesce", args.iter().map(child).collect::<Result<_>>()?),
        Expr::Between { operand,low,high,symmetric } => call(if *symmetric { "between-symmetric" } else { "between" },vec![child(operand)?,child(low)?,child(high)?]),
        Expr::SimpleCase { operand, branches, otherwise } => { let mut args = vec![child(operand)?]; for (candidate, value) in branches { args.push(child(candidate)?); args.push(child(value)?); } args.push(child(otherwise)?); call("case", args) }
        Expr::Case { branches, otherwise } => { let mut args = Vec::new(); for (test, value) in branches { args.push(child(test)?); args.push(child(value)?); } args.push(child(otherwise)?); call("cond", args) }
        Expr::Field { expr, name, .. } => call(".", vec![child(expr)?, name.clone()]),
        Expr::Subscript { expr, index, .. } => call("nth", vec![child(expr)?, child(index)?]),
        Expr::List(values) => format!("[{}]", values.iter().map(child).collect::<Result<Vec<_>>>()?.join(" ")),
        Expr::Struct(fields) => format!("{{{}}}", fields.iter().map(|(name, value)| Ok(format!(":{name} {}", child(value)?))).collect::<Result<Vec<_>>>()?.join(", ")),
        Expr::RowCopy(fields) => format!("{{{}}}", fields.iter().map(|field| Ok(format!(":{} {}", field.name, child(&field.expression())?))).collect::<Result<Vec<_>>>()?.join(", ")),
        Expr::In { expr, values, negated } => { let value = call("in", vec![child(expr)?, format!("[{}]", values.iter().map(child).collect::<Result<Vec<_>>>()?.join(" "))]); if *negated { call("not", vec![value]) } else { value } }
        Expr::Like { expr, pattern, escape, case_insensitive, negated } => { let mut args = vec![child(expr)?, child(pattern)?]; if let Some(escape) = escape { args.push(quote(&escape.to_string())?); } let value = call(if *case_insensitive { "ilike" } else { "like" }, args); if *negated { call("not", vec![value]) } else { value } }
    })
}
fn quote(text: &str) -> Result<String> { serde_json::to_string(text).map_err(|error| invalid("explain-string", error)) }
fn regex_form(form: &rtdb_plan::RegexForm) -> Result<String> {
    use rtdb_plan::RegexForm as F;
    let sequence = |open: &str,values: &[F],close: &str| Ok::<_,rtdb_types::Anomaly>(format!("{open}{}{close}",values.iter().map(regex_form).collect::<Result<Vec<_>>>()?.join(" ")));
    Ok(match form {
        F::Literal(value) => literal(value)?,
        F::Character(ch) => if u32::from(*ch) <= 0xffff { format!("\\u{:04x}",u32::from(*ch)) } else { format!("\\{ch}") },
        F::Symbol(value) => value.clone(),F::BigInteger(value) => format!("{value}N"),F::Decimal(value) => format!("{value}M"),
        F::List(values) => sequence("(",values,")")?,F::Vector(values) => sequence("[",values,"]")?,F::Set(values) => sequence("#{",values,"}")?,
        F::Map(values) => format!("{{{}}}",values.iter().map(|(key,value)|Ok(format!("{} {}",regex_form(key)?,regex_form(value)?))).collect::<Result<Vec<_>>>()?.join(", ")),
        F::Tagged { tag,value } => format!("#{tag} {}",regex_form(value)?),F::AnonymousFunction(value) => format!("#{}",regex_form(value)?),
    })
}
fn cast_options(target: &rtdb_plan::CastTarget) -> Option<String> {
    match target {
        rtdb_plan::CastTarget::Deferred(target) => match target {
            rtdb_plan::DeferredCast::Temporal { precision, .. } => (!matches!(precision,rtdb_plan::CastOption::Null)).then(|| format!("{{:precision {}}}",precision.to_edn())),
            rtdb_plan::DeferredCast::Interval { start, end, leading, fractional } => Some(format!("{{{}}}",
                [("start-field",start),("end-field",end),("leading-precision",leading),("fractional-precision",fractional)].into_iter()
                    .filter(|(_,value)| !matches!(value,rtdb_plan::CastOption::Null)).map(|(name,value)| format!(":{name} {}",value.to_edn())).collect::<Vec<_>>().join(", "))),
        },
        rtdb_plan::CastTarget::Temporal(target) => target.precision.map(|precision| format!("{{:precision {precision}}}")),
        rtdb_plan::CastTarget::Interval(target) => {
            let mut options = vec![format!(":start-field \"{}\"",target.start.name())];
            if let Some(end) = target.end { options.push(format!(":end-field \"{}\"",end.name())); }
            if let Some(leading) = target.leading_precision { options.push(format!(":leading-precision {leading}")); }
            if let Some(fractional) = target.fractional_precision { options.push(format!(":fractional-precision {fractional}")); }
            Some(format!("{{{}}}",options.join(", ")))
        }
        rtdb_plan::CastTarget::IntervalEmptyOptions => Some("{}".into()),
        rtdb_plan::CastTarget::Checked { target, .. } | rtdb_plan::CastTarget::List(target) => cast_options(target),
        _ => None,
    }
}
fn literal(value: &Value) -> Result<String> {
    Ok(match value {
        Value::Null | Value::Undefined => "nil".into(), Value::Bool(value) => value.to_string(),
        Value::I8(value) => value.to_string(), Value::I16(value) => value.to_string(), Value::I32(value) | Value::Oid(value) | Value::RegClass(value) | Value::RegProc(value) => value.to_string(), Value::I64(value) => value.to_string(),
        Value::U8(value) => value.to_string(), Value::U16(value) => value.to_string(), Value::U32(value) => value.to_string(), Value::U64(value) => value.to_string(),
        Value::F32(value) => rtdb_types::java_f32_text(*value), Value::F64(value) => rtdb_types::java_f64_text(*value), Value::F16(value) => value.to_string(),
        Value::Decimal(value) => format!("{}M", rtdb_expr::decimal_text(value)), Value::Utf8(value) => quote(value)?, Value::Keyword(value) => value.to_string(),
        Value::Uri(value) => format!("#xt/uri {}", quote(&rtdb_types::UriParts::parse(value)?.to_ascii_string())?),
        Value::Uuid(value) => format!("#uuid \"{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}\"", value[0],value[1],value[2],value[3],value[4],value[5],value[6],value[7],value[8],value[9],value[10],value[11],value[12],value[13],value[14],value[15]),
        Value::Binary(bytes) | Value::FixedSizeBinary(bytes) | Value::Transit(bytes) => format!("#bytes \"{}\"", bytes.iter().map(|byte| format!("{byte:02x}")).collect::<String>()),
        Value::List(values) | Value::FixedSizeList(values) => format!("[{}]", values.iter().map(literal).collect::<Result<Vec<_>>>()?.join(" ")),
        Value::Set(values) => format!("#{{{}}}", values.iter().map(literal).collect::<Result<Vec<_>>>()?.join(" ")),
        Value::Struct(fields) => format!("{{{}}}", fields.iter().map(|(name, value)| Ok(format!(":{name} {}", literal(value)?))).collect::<Result<Vec<_>>>()?.join(", ")),
        Value::Map { entries, .. } => format!("{{{}}}", entries.iter().map(|(key, value)| Ok(format!("{} {}", literal(key)?, literal(value)?))).collect::<Result<Vec<_>>>()?.join(", ")),
        other => {
            let Value::Utf8(text) = rtdb_expr::cast(other, &LogicalType::Scalar(ScalarType::Utf8), "UTC")? else { return Err(invalid("explain-literal", "Temporal literal did not render as text")); };
            let tag = match other { Value::Date(_) | Value::DateDay(_) | Value::DateMilli(_) => "time/date", Value::Time { .. } => "time/time", Value::Timestamp { timezone: None, .. } => "time/date-time", Value::Timestamp { .. } => "time/zoned-date-time", Value::Duration { .. } => "time/duration", Value::TimestampTzRange { .. } => "xt/tstz-range", _ => "xt/interval" };
            format!("#{tag} {}", quote(&text)?)
        }
    })
}

#[cfg(test)]
mod patch_endpoint_tests {
    use super::*;
    #[test]
    fn patch_explanation_retains_conformed_endpoint_tags() {
        let clock = Expr::Call { name: "current_timestamp".into(),args: vec![],return_type: LogicalType::Nothing };
        assert_eq!(patch_endpoint(&clock,"(current_timestamp)".into()).unwrap(),"[:now (current-timestamp)]");
        assert_eq!(patch_endpoint(&Expr::Literal(Value::Null),"nil".into()).unwrap(),"nil");
        let marker = Expr::Literal(Value::Keyword(Keyword::from_name("now")));
        assert_eq!(patch_endpoint(&marker,":now".into()).unwrap(),"[:now :now]");
        let parameter = Expr::Parameter { index: 0,data_type: LogicalType::Nothing };
        assert_eq!(patch_endpoint(&parameter,"?_0".into()).unwrap(),"[:param ?_0]");
    }
}
