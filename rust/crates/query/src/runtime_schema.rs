use std::{collections::BTreeMap, sync::Arc};

use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::{BinaryOp, ColumnId, Expr, Field, JoinKind, Relation, RelationNode, Schema, UnaryOp};
use rtdb_types::{LogicalType as L, Result, ScalarType};

use crate::{CatalogSnapshot, invalid};

pub(crate) fn parameter_types(relation: &Relation, values: &[rtdb_types::Value], mode: EmissionMode) -> Vec<L> {
    declared_parameter_types(relation, values.len(), mode).into_iter().zip(values).map(|(declared, value)| {
        declared.filter(|ty| !matches!(value, rtdb_types::Value::Null) || ty.nullable()).unwrap_or_else(|| value.logical_type())
    }).collect()
}

pub(crate) fn arrow_parameter_types(relation: &Relation, fields: &[L], mode: EmissionMode) -> Vec<L> {
    declared_parameter_types(relation, fields.len(), mode).into_iter().zip(fields).map(|(declared, field)| {
        declared.unwrap_or_else(|| mode.field_type(field))
    }).collect()
}

fn declared_parameter_types(relation: &Relation, width: usize, mode: EmissionMode) -> Vec<Option<L>> {
    fn collect(relation: &Relation, declarations: &mut [Option<L>], local: &mut Vec<usize>, mode: EmissionMode) {
        for expression in relation.expressions() {
            expression.visit(&mut |expression| {
                if let Expr::Parameter { index, data_type } = expression {
                    if local.contains(index) || matches!(data_type, L::Nothing | L::Null) { return; }
                    if let Some(declared) = declarations.get_mut(*index) {
                        let ty = mode.field_type(data_type);
                        *declared = Some(match declared.take() { Some(previous) => L::join_all(&[previous, ty]), None => ty });
                    }
                }
            });
        }
        if let RelationNode::Apply { input, dependent, arguments, .. } | RelationNode::Mark { input, dependent, arguments, .. } = relation.node() {
            collect(input, declarations, local, mode);
            let outer = local.len();
            local.extend(arguments.iter().map(|(index, _)| *index));
            collect(dependent, declarations, local, mode);
            local.truncate(outer);
        } else {
            for child in relation.children() { collect(child, declarations, local, mode); }
        }
    }
    let mut declarations = vec![None; width];
    collect(relation, &mut declarations, &mut Vec::new(), mode);
    declarations
}

/// Result fields emitted from the plan and a catalog snapshot, with their memory reservation.
pub struct RuntimeSchema { schema: Schema, _reservation: Arc<MemoryReservation> }

#[derive(Clone, Copy)]
pub(crate) enum EmissionMode { ArrowFields, Execution }

impl EmissionMode {
    pub(crate) fn field_type(self, ty: &L) -> L {
        match self { Self::ArrowFields => ty.clone(), Self::Execution => rtdb_columnar::execution_type(ty) }
    }
}

#[derive(Clone, Copy)]
struct Emitter<'a> {
    catalog: &'a CatalogSnapshot,
    mode: EmissionMode,
    parameters: &'a BTreeMap<usize, L>,
    outer: &'a [Schema],
    reservation: &'a Arc<MemoryReservation>,
}

pub(crate) struct EmittedRelation {
    relation: Arc<Relation>,
    output: Arc<RuntimeSchema>,
    node: EmittedNode,
}

enum EmittedNode {
    Leaf,
    Unary(Arc<EmittedRelation>),
    Binary(Arc<EmittedRelation>, Arc<EmittedRelation>),
    Aggregate { input: Arc<EmittedRelation>, types: Vec<crate::aggregate::AggregateTypes> },
}

impl EmittedRelation {
    pub fn schema(&self) -> &Schema { self.output.schema() }
    pub fn output(&self) -> Arc<RuntimeSchema> { Arc::clone(&self.output) }
    pub fn relation(&self) -> &Arc<Relation> { &self.relation }
    pub fn child(&self, index: usize) -> Result<Arc<Self>> {
        match (&self.node, index) {
            (EmittedNode::Unary(input) | EmittedNode::Aggregate { input, .. } | EmittedNode::Binary(input, _), 0) => Ok(Arc::clone(input)),
            (EmittedNode::Binary(_, right), 1) => Ok(Arc::clone(right)),
            _ => Err(invalid("runtime-plan", "Runtime relation child is absent")),
        }
    }
    pub fn aggregates(&self) -> Result<&[crate::aggregate::AggregateTypes]> {
        match &self.node { EmittedNode::Aggregate { types, .. } => Ok(types), _ => Err(invalid("runtime-plan", "Runtime relation is not an aggregate")) }
    }
}

impl EmittedRelation {
    pub fn derive(relation: Arc<Relation>, catalog: &CatalogSnapshot, parameters: &[L], memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        Self::emit(relation, catalog, parameters, memory, EmissionMode::Execution)
    }

    pub fn emit(relation: Arc<Relation>, catalog: &CatalogSnapshot, parameters: &[L], memory: &Arc<MemoryBudget>, mode: EmissionMode) -> Result<Arc<Self>> {
        let bound = parameters.iter().try_fold(allocation_bound(&relation)?, |bytes, ty| add(bytes, add(multiply(ty.retained_bytes()?, 4)?, 512)?))?;
        let reservation = memory.reserve(bound)?;
        let parameters = parameters.iter().enumerate().map(|(index, ty)| (index, ty.clone())).collect();
        relation_types(&relation, &Emitter { catalog, mode, parameters: &parameters, outer: &[], reservation: &reservation })
    }
}

impl RuntimeSchema {
    pub(crate) fn fixed(schema: &Schema, memory: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        let reservation = memory.reserve(schema_bound(schema)?)?;
        Ok(Arc::new(Self { schema: schema.clone(), _reservation: reservation }))
    }
    pub fn schema(&self) -> &Schema { &self.schema }
}

fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(|| invalid("runtime-schema-size", "Runtime schema allocation exceeds addressable memory")) }
fn multiply(value: usize, factor: usize) -> Result<usize> { value.checked_mul(factor).ok_or_else(|| invalid("runtime-schema-size", "Runtime schema allocation exceeds addressable memory")) }
fn field_bound(field: &Field) -> Result<usize> {
    let qualifier = field.qualifier.as_ref().map(|qualifier| qualifier.retained_bytes()).transpose()?.unwrap_or(0);
    add(multiply(field.data_type.retained_bytes()?, 4)?, add(multiply(add(field.name.len(), qualifier)?, 8)?, 2048)?)
}
pub(crate) fn schema_bound(schema: &Schema) -> Result<usize> {
    schema.fields().iter().try_fold(4096, |bytes, field| add(bytes, field_bound(field)?))
}
pub(crate) fn expression_bound(expr: &Expr) -> Result<usize> {
    stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || expression_bound_at(expr))
}
fn expression_bound_at(expr: &Expr) -> Result<usize> {
    let mut bytes = 2048;
    match expr {
        Expr::Literal(value) => bytes = add(bytes, multiply(value.retained_bytes()?, 4)?)?,
        Expr::DurationLiteral(_) => {},
        Expr::Column { data_type, .. } | Expr::Parameter { data_type, .. } | Expr::Correlated { data_type, .. } => bytes = add(bytes, multiply(data_type.retained_bytes()?, 4)?)?,
        Expr::Unary { expr, .. } => bytes = add(bytes, expression_bound(expr)?)?,
        Expr::Cast { expr, target } => { bytes = add(bytes, expression_bound(expr)?)?; bytes = add(bytes, cast_bound(target)?)?; }
        Expr::Field { expr, name, data_type } => { bytes = add(bytes, expression_bound(expr)?)?; bytes = add(bytes, add(multiply(name.len(), 8)?, multiply(data_type.retained_bytes()?, 4)?)?)?; }
        Expr::Binary { left, right, .. } => { bytes = add(bytes, expression_bound(left)?)?; bytes = add(bytes, expression_bound(right)?)?; }
        Expr::Call { name, args, return_type } => { bytes = add(bytes, add(multiply(name.len(), 8)?, multiply(return_type.retained_bytes()?, 4)?)?)?; for arg in args { bytes = add(bytes, expression_bound(arg)?)?; } }
        Expr::RegexReplace { source,recipe,return_type } => { bytes = add(bytes,add(multiply(recipe.work_bound()?,8)?,multiply(return_type.retained_bytes()?,32)?)?)?;bytes = add(bytes,expression_bound(source)?)?; }
        Expr::Coalesce(args) | Expr::List(args) => for arg in args { bytes = add(bytes, expression_bound(arg)?)?; },
        Expr::Between { operand,low,high,.. } => { bytes = add(bytes,add(expression_bound(operand)?,add(expression_bound(low)?,expression_bound(high)?)?)?)?; }
        Expr::SimpleCase { operand, branches, otherwise } => { bytes = add(bytes, expression_bound(operand)?)?; for (candidate, value) in branches { bytes = add(bytes, add(expression_bound(candidate)?, expression_bound(value)?)?)?; } bytes = add(bytes, expression_bound(otherwise)?)?; }
        Expr::Case { branches, otherwise } => { for (condition, value) in branches { bytes = add(bytes, add(expression_bound(condition)?, expression_bound(value)?)?)?; } bytes = add(bytes, expression_bound(otherwise)?)?; }
        Expr::Subscript { expr, index, data_type } => { bytes = add(bytes, expression_bound(expr)?)?; bytes = add(bytes, expression_bound(index)?)?; bytes = add(bytes, multiply(data_type.retained_bytes()?, 4)?)?; }
        Expr::Struct(fields) => for (name, expr) in fields { bytes = add(bytes, add(multiply(name.len(), 8)?, expression_bound(expr)?)?)?; },
        Expr::RowCopy(fields) => for field in fields { bytes = add(bytes, field_bound(field)?)?; },
        Expr::In { expr, values, .. } => { bytes = add(bytes, expression_bound(expr)?)?; for value in values { bytes = add(bytes, expression_bound(value)?)?; } }
        Expr::Like { expr, pattern, .. } => { bytes = add(bytes, expression_bound(expr)?)?; bytes = add(bytes, expression_bound(pattern)?)?; }
    }
    Ok(bytes)
}
fn cast_bound(target: &rtdb_plan::CastTarget) -> Result<usize> {
    Ok(match target {
        rtdb_plan::CastTarget::Fixed(ty) => multiply(ty.retained_bytes()?, 4)?,
        rtdb_plan::CastTarget::List(element) => add(2048, cast_bound(element)?)?,
        rtdb_plan::CastTarget::Checked { requested, target } => add(multiply(requested.retained_bytes()?, 4)?, cast_bound(target)?)?,
        rtdb_plan::CastTarget::Temporal(target) => add(2048, match &target.kind { rtdb_plan::TemporalKind::TimestampZone(rtdb_plan::CastTimezone::Explicit(zone)) => multiply(zone.capacity(), 32)?, _ => 0 })?,
        rtdb_plan::CastTarget::Deferred(target) => add(2048, multiply(target.retained_bytes()?, 4)?)?,
        _ => 2048,
    })
}
pub(crate) fn local_allocation_bound(relation: &Relation) -> Result<usize> {
    let mut bytes = schema_bound(relation.schema())?;
    let mut expression = |expr: &Expr| { bytes = add(bytes, expression_bound(expr)?)?; Ok::<_, rtdb_types::Anomaly>(()) };
    match relation.node() {
        RelationNode::Values { rows } => for row in rows { for expr in row { expression(expr)?; } },
        RelationNode::LiteralPages { pages } => {
            for expr in pages.iter().flatten().flatten() { expression(expr)?; }
            bytes = add(bytes, multiply(pages.capacity(), std::mem::size_of::<Vec<Vec<Expr>>>())?)?;
            for page in pages { bytes = add(bytes, multiply(page.capacity(), std::mem::size_of::<Vec<Expr>>())?)?; }
        }
        RelationNode::Scan(scan) => {
            for filter in [&scan.valid_time, &scan.system_time] {
                match filter {
                    rtdb_plan::TemporalFilter::At(at) => expression(at)?,
                    rtdb_plan::TemporalFilter::FromTo { from, to } => { expression(from)?; expression(to)?; }
                    rtdb_plan::TemporalFilter::Between { from, to } => { expression(from)?; expression(to)?; }
                    rtdb_plan::TemporalFilter::Default | rtdb_plan::TemporalFilter::All => {}
                }
            }
            for predicate in &scan.predicates { expression(predicate)?; }
            for name in [&scan.table.name.database, &scan.table.name.table.schema, &scan.table.name.table.table] { bytes = add(bytes, multiply(name.len(), 8)?)?; }
            for (name, ty) in &scan.table.columns { bytes = add(bytes, add(add(1024, multiply(name.len(), 8)?)?, multiply(ty.retained_bytes()?, 4)?)?)?; }
            if let Some(iids) = &scan.iids { bytes = add(bytes, multiply(iids.len(), 64)?)?; }
        }
        RelationNode::Project { expressions, .. } | RelationNode::Extend { expressions, .. } | RelationNode::Rename { expressions, .. } => for named in expressions { expression(&named.expr)?; },
        RelationNode::Aggregate { groups, aggregates, .. } => {
            for group in groups { expression(&group.expr)?; }
            for aggregate in aggregates {
                if let Some(argument) = &aggregate.argument { expression(argument)?; }
                if let Some(filter) = &aggregate.filter { expression(filter)?; }
                for order in &aggregate.order_by { expression(&order.expr)?; }
            }
        }
        RelationNode::Unnest { expr, .. } | RelationNode::List { expr, .. } | RelationNode::ColumnTable { expr, .. } => expression(expr)?,
        RelationNode::Join { condition, .. } => if let Some(condition) = condition { expression(condition)?; },
        RelationNode::Apply { arguments, .. } => for (_, expr) in arguments { expression(expr)?; },
        RelationNode::Mark { arguments, condition, comparison, .. } => { for (_, expr) in arguments { expression(expr)?; } for expr in condition.iter().chain([comparison]) { expression(expr)?; } },
        RelationNode::Filter { predicate, .. } => expression(predicate)?,
        RelationNode::Sort { order_by, .. } | RelationNode::Window { order_by, .. } => for order in order_by { expression(&order.expr)?; },
        RelationNode::SortLimit { order_by, offset, fetch, .. } => {
            for order in order_by { expression(&order.expr)?; }
            for bound in [offset, fetch].into_iter().flatten() { expression(bound)?; }
        }
        RelationNode::Limit { offset, fetch, .. } => for bound in [offset, fetch].into_iter().flatten() { expression(bound)?; },
        RelationNode::PatchGaps { from, to, .. } => { expression(from)?; expression(to)?; }
        RelationNode::Let { .. } | RelationNode::Reference { .. } | RelationNode::Set { .. } | RelationNode::Distinct { .. } | RelationNode::Ordinality { .. } => {}
    }
    match relation.node() {
        RelationNode::Aggregate { aggregates, .. } => for aggregate in aggregates { bytes = add(bytes, multiply(aggregate.function.retained_bytes()?, 8)?)?; },
        RelationNode::Rename { prefix, .. } => bytes = add(bytes, multiply(prefix.len(), 8)?)?,
        RelationNode::Let { binding, .. } | RelationNode::Reference { binding } => bytes = add(bytes, multiply(binding.name.len(), 8)?)?,
        _ => {}
    }
    Ok(bytes)
}
pub(crate) fn allocation_bound(relation: &Relation) -> Result<usize> {
    let mut bytes = local_allocation_bound(relation)?;
    for child in relation.children() { bytes = add(bytes, allocation_bound(child)?)?; }
    if let RelationNode::Reference { binding } = relation.node() { bytes = add(bytes, allocation_bound(&binding.relation)?)?; }
    Ok(bytes)
}

fn field_type(field: &Field, data_type: L) -> Field { let mut field = field.clone(); field.data_type = data_type; field }
fn relation_types(relation: &Arc<Relation>, emitter: &Emitter<'_>) -> Result<Arc<EmittedRelation>> {
    stacker::maybe_grow(256 * 1024, 2 * 1024 * 1024, || relation_types_at(relation, emitter))
}

fn relation_types_at(relation: &Arc<Relation>, emitter: &Emitter<'_>) -> Result<Arc<EmittedRelation>> {
    let type_of = |expr: &Expr, input: &Schema| expression_type(expr, input, emitter);
    let mut children = Vec::new();
    let mut aggregates = Vec::new();
    let mut child = |input: &Arc<Relation>| {
        let runtime = relation_types(input, emitter)?;
        let schema = runtime.schema().clone();
        children.push(runtime);
        Ok::<_, rtdb_types::Anomaly>(schema)
    };
    let schema = match relation.node() {
        RelationNode::Values { .. } | RelationNode::LiteralPages { .. } => {
            let pages = match relation.node() {
                RelationNode::Values { rows } => std::slice::from_ref(rows),
                RelationNode::LiteralPages { pages } => pages.as_slice(),
                _ => unreachable!(),
            };
            Schema::new(relation.schema().fields().iter().enumerate().map(|(index, field)| {
                let types = pages.iter().flatten().map(|row| type_of(&row[index], &Schema::default())).collect::<Result<Vec<_>>>()?;
                Ok(if pages.iter().flatten().map(|row| row[index].data_type()).eq(types.iter().cloned()) { field.clone() } else { field_type(field, L::join_all(&types)) })
            }).collect::<Result<_>>()?)?
        }
        RelationNode::Scan(scan) => {
            let schema = Schema::new(scan.columns.iter().map(|field| match emitter.mode {
                EmissionMode::ArrowFields => field.clone(),
                EmissionMode::Execution => field_type(field, emitter.catalog.runtime_scan_type(&scan.table, field)),
            }).collect())?;
            for filter in [&scan.valid_time, &scan.system_time] {
                match filter {
                    rtdb_plan::TemporalFilter::At(at) => { type_of(at, &Schema::default())?; }
                    rtdb_plan::TemporalFilter::FromTo { from, to } | rtdb_plan::TemporalFilter::Between { from, to } => {
                        type_of(from, &Schema::default())?;
                        type_of(to, &Schema::default())?;
                    }
                    rtdb_plan::TemporalFilter::Default | rtdb_plan::TemporalFilter::All => {}
                }
            }
            for predicate in &scan.predicates { type_of(predicate, &schema)?; }
            schema
        }
        RelationNode::Project { input, expressions } | RelationNode::Extend { input, expressions } | RelationNode::Rename { input, expressions, .. } => {
            let input = child(input)?;
            let fields = expressions.iter().map(|expr| {
                let ty = type_of(&expr.expr, &input)?;
                Ok(if ty == expr.expr.data_type() { expr.field.clone() } else { field_type(&expr.field, ty) })
            }).collect::<Result<Vec<_>>>()?;
            if matches!(relation.node(), RelationNode::Extend { .. }) {
                Schema::new(input.fields().iter().cloned().chain(fields).collect())?
            } else { Schema::new(fields)? }
        }
        RelationNode::Let { binding, body } => { child(&binding.relation)?; child(body)? }
        RelationNode::Reference { binding } => relation_types(&binding.relation, emitter)?.schema().clone(),
        RelationNode::Filter { input, predicate } => {
            let input = child(input)?;
            type_of(predicate, &input)?;
            input
        }
        RelationNode::Sort { input, order_by } | RelationNode::SortLimit { input, order_by, .. } => {
            let input = child(input)?;
            for order in order_by { type_of(&order.expr, &input)?; }
            input
        }
        RelationNode::Limit { input, .. } | RelationNode::Distinct { input } => child(input)?,
        RelationNode::PatchGaps { input, document, .. } => {
            let input = child(input)?;
            Schema::new(input.fields().iter().map(|field| {
                if field.id == *document { field_type(field, field.data_type.clone().maybe()) } else { field.clone() }
            }).collect())?
        }
        RelationNode::Join { left, right, kind, condition } => {
            let (left, right) = (child(left)?, child(right)?);
            if let Some(condition) = condition { type_of(condition, &left.concat(&right)?)?; }
            match kind {
                JoinKind::Semi | JoinKind::Anti => left,
                JoinKind::Left | JoinKind::Single => left.concat(&right.nullable()?)?,
                JoinKind::Right => left.nullable()?.concat(&right)?,
                JoinKind::Full => left.nullable()?.concat(&right.nullable()?)?,
                _ => left.concat(&right)?,
            }
        }
        RelationNode::Set { left, right, .. } => {
            let (left, right) = (child(left)?, child(right)?);
            Schema::new(relation.schema().fields().iter().zip(left.fields().iter().zip(right.fields())).map(|(field, (left, right))| field_type(field, left.data_type.join(&right.data_type))).collect())?
        }
        RelationNode::Aggregate { input, groups, aggregates: expressions } => {
            let input = child(input)?;
            let mut fields = groups.iter().map(|group| Ok(field_type(&group.field, type_of(&group.expr, &input)?))).collect::<Result<Vec<_>>>()?;
            for expression in expressions {
                let input_type = expression.argument.as_ref().map(|argument| type_of(argument, &input)).transpose()?.unwrap_or(L::Nothing);
                if let Some(filter) = &expression.filter { type_of(filter, &input)?; }
                for order in &expression.order_by { type_of(&order.expr, &input)?; }
                let types = crate::aggregate::AggregateTypes::derive(expression, input_type, groups.is_empty(), emitter.mode)?;
                fields.push(field_type(&expression.field, types.output_type().clone()));
                aggregates.push(types);
            }
            Schema::new(fields)?
        }
        RelationNode::Window { input, expressions, order_by, .. } => {
            let input = child(input)?;
            for order in order_by { type_of(&order.expr, &input)?; }
            let mut fields = input.fields().to_vec();
            for expr in expressions { fields.push(field_type(&expr.field, expr.function.result_type(&input)?)); }
            Schema::new(fields)?
        }
        RelationNode::List { expr, element, .. } | RelationNode::ColumnTable { expr, element } => {
            let types = type_of(expr, &Schema::default())?.legs().into_iter().map(|ty| match ty.unencoded() {
                L::List { kind: rtdb_types::ListKind::List | rtdb_types::ListKind::Set | rtdb_types::ListKind::FixedSizeList(_), element } => *element,
                _ => L::Null,
            }).collect::<Vec<_>>();
            Schema::new(vec![field_type(element, L::join_all(&types))])?
        }
        RelationNode::Ordinality { input, field, .. } => {
            let input = child(input)?;
            Schema::new(input.fields().iter().cloned().chain([field.clone()]).collect())?
        }
        RelationNode::Unnest { input, expr, element, ordinality, outer: nullable } => {
            let input = child(input)?;
            let ty = rtdb_plan::unnest_element_type(&type_of(expr, &input)?);
            let mut fields = input.fields().to_vec();
            fields.push(field_type(element, if *nullable { ty.maybe() } else { ty }));
            fields.extend(ordinality.iter().cloned());
            Schema::new(fields)?
        }
        RelationNode::Mark { input, dependent, arguments, condition, comparison, output } => {
            let input = child(input)?;
            let mut bound = emitter.parameters.clone();
            for (index, expr) in arguments { bound.insert(*index, type_of(expr, &input)?); }
            let mut scopes = emitter.outer.to_vec(); scopes.push(input.clone());
            let dependent_types = relation_types(dependent, &Emitter { parameters: &bound, outer: &scopes, ..*emitter })?;
            let comparison_schema = input.concat(dependent_types.schema())?;
            for predicate in condition.iter().chain([comparison]) { type_of(predicate, &comparison_schema)?; }
            children.push(dependent_types);
            input.concat(&Schema::new(vec![field_type(output, L::Scalar(ScalarType::Bool).maybe())])?)?
        }
        RelationNode::Apply { input, dependent, arguments, kind, output } => {
            let input = child(input)?;
            let mut bound = emitter.parameters.clone();
            for (index, expr) in arguments { bound.insert(*index, type_of(expr, &input)?); }
            let mut scopes = emitter.outer.to_vec(); scopes.push(input.clone());
            let dependent_types = relation_types(dependent, &Emitter { parameters: &bound, outer: &scopes, ..*emitter })?;
            let dependent = dependent_types.schema().clone();
            children.push(dependent_types);
            kind.result_schema(&input,&dependent,output.as_ref())?
        }
    };
    let node = match (relation.node(), children.as_slice()) {
        (RelationNode::Values { .. } | RelationNode::LiteralPages { .. } | RelationNode::List { .. } | RelationNode::ColumnTable { .. } | RelationNode::Scan(_) | RelationNode::Reference { .. }, []) => EmittedNode::Leaf,
        (RelationNode::Aggregate { .. }, [input]) => EmittedNode::Aggregate { input: Arc::clone(input), types: aggregates },
        (RelationNode::Join { .. } | RelationNode::Set { .. } | RelationNode::Apply { .. } | RelationNode::Mark { .. } | RelationNode::Let { .. }, [left, right]) => EmittedNode::Binary(Arc::clone(left), Arc::clone(right)),
        (RelationNode::Project { .. } | RelationNode::Extend { .. } | RelationNode::Rename { .. } | RelationNode::Filter { .. } | RelationNode::Sort { .. } | RelationNode::SortLimit { .. } | RelationNode::Limit { .. }
            | RelationNode::Distinct { .. } | RelationNode::Window { .. } | RelationNode::Unnest { .. } | RelationNode::Ordinality { .. } | RelationNode::PatchGaps { .. }, [input]) => EmittedNode::Unary(Arc::clone(input)),
        _ => return Err(invalid("runtime-plan", "Runtime relation children disagree with its plan")),
    };
    Ok(Arc::new(EmittedRelation { relation: Arc::clone(relation), output: Arc::new(RuntimeSchema { schema, _reservation: Arc::clone(emitter.reservation) }), node }))
}

fn coalesce_type(args: &[Expr], input: &Schema, emitter: &Emitter<'_>) -> Result<L> {
    let mut result = L::Nothing;
    for (index,arg) in args.iter().enumerate() {
        let ty = expression_type(arg, input, emitter)?;
        for leg in ty.legs() { if leg != L::Null || index + 1 == args.len() { result = result.join(&leg); } }
        if !ty.nullable() { break; }
    }
    Ok(if result == L::Nothing { L::Null } else { result })
}

fn typed(data_type: L) -> Expr { Expr::column(ColumnId(0), data_type) }
fn expression_type(expr: &Expr, input: &Schema, emitter: &Emitter<'_>) -> Result<L> {
    stacker::maybe_grow(128 * 1024, 2 * 1024 * 1024, || expression_type_at(expr, input, emitter))
}
fn expression_type_at(expr: &Expr, input: &Schema, emitter: &Emitter<'_>) -> Result<L> {
    let child = |expr: &Expr| expression_type(expr, input, emitter);
    Ok(match expr {
        Expr::Literal(value) => value.logical_type(),
        Expr::DurationLiteral(_) => expr.data_type(),
        Expr::RegexReplace { source,recipe,.. } => recipe.output_type(&child(source)?)?,
        Expr::Column { id, .. } => input.get(*id).ok_or_else(|| invalid("runtime-column", "Runtime input column is absent"))?.data_type.clone(),
        Expr::Parameter { index, data_type } => emitter.parameters.get(index).cloned().unwrap_or_else(|| if *data_type == L::Nothing { L::Null } else { emitter.mode.field_type(data_type) }),
        Expr::Correlated { depth, id, data_type } => emitter.outer.iter().rev().nth(*depth).and_then(|schema| schema.get(*id)).map_or_else(|| data_type.clone(), |field| field.data_type.clone()),
        Expr::Cast { expr, target } => {
            let source = child(expr)?;
            target.validate_source(&source)?;
            target.result_type(&source)
        }
        Expr::Unary { op, expr } => {
            let ty = child(expr)?;
            if *op == UnaryOp::BitNot { return rtdb_plan::function_type("bit_not", &[rtdb_columnar::execution_type(&ty)]); }
            if ty == expr.data_type() { return Ok(Expr::unary(*op, typed(ty)).data_type()); }
            if ty == L::Null && matches!(op, UnaryOp::Negate | UnaryOp::BitNot | UnaryOp::Not) { L::Null }
            else { Expr::unary(*op, typed(ty)).data_type() }
        }
        Expr::Binary { op, left, right } => {
            let (left_type, right_type) = (child(left)?, child(right)?);
            if matches!(op,BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq) { rtdb_plan::validate_ordered_predicate_types(&left_type,&right_type)?; }
            if matches!(op, BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::IsDistinctFrom | BinaryOp::IsNotDistinctFrom) { rtdb_plan::validate_equality_types(&left_type, &right_type)?; }
            if matches!(op, BinaryOp::BitAnd | BinaryOp::BitOr | BinaryOp::BitXor | BinaryOp::ShiftLeft | BinaryOp::ShiftRight) {
                let left = Expr::Parameter { index: 0, data_type: rtdb_columnar::execution_type(&left_type) };
                let right = Expr::Parameter { index: 1, data_type: rtdb_columnar::execution_type(&right_type) };
                let expression = Expr::binary(*op, left, right);
                expression.validate_columns(&Default::default())?;
                return Ok(expression.data_type());
            }
            if left_type == left.data_type() && right_type == right.data_type() { return Ok(expr.data_type()); }
            if (left_type == L::Null || right_type == L::Null) && !matches!(op, BinaryOp::And | BinaryOp::Or | BinaryOp::IsDistinctFrom | BinaryOp::IsNotDistinctFrom) { L::Null }
            else { Expr::binary(*op, typed(left_type), typed(right_type)).data_type() }
        }
        Expr::Call { name, args, return_type } => {
            if name.eq_ignore_ascii_case("coalesce") { return coalesce_type(args, input, emitter); }
            let types = args.iter().map(child).collect::<Result<Vec<_>>>()?;
            if ["null_eq", "null-eq", "distinct_from", "distinct-from", "nullif"].iter().any(|candidate| name.eq_ignore_ascii_case(candidate)) && types.len() == 2 { rtdb_plan::validate_equality_types(&types[0], &types[1])?; }
            if args.iter().map(Expr::data_type).eq(types.iter().cloned()) { return Ok(return_type.clone()); }
            let normalized = name.to_lowercase().replace('-', "_");
            let nullable_call = !matches!(normalized.as_str(), "_patch" | "coalesce" | "nullif" | "period" | "set" | "str" | "string_to_array" | "format" | "format_type" | "nil?" | "is_null" | "true?" | "false?" | "is_true" | "is_false" | "distinct_from" | "compare_nulls_first" | "compare_nulls_last" | "boolean" | "null_eq" | "===" | "boolean?" | "number?" | "integer?" | "float?" | "string?" | "keyword?" | "uuid?" | "list?" | "set?" | "map?");
            if nullable_call && types.contains(&L::Null) { L::Null }
            else if normalized == "nullif" { types[0].clone().maybe() }
            else {
                let args = args.iter().zip(types).map(|(arg, ty)| if ty == arg.data_type() { arg.clone() } else { typed(ty) }).collect::<Vec<_>>();
                rtdb_plan::function_type_with_args(name, &args)?
            }
        }
        Expr::Between { operand,low,high,symmetric } => {
            let (operand,low,high) = (child(operand)?,child(low)?,child(high)?);
            rtdb_plan::validate_ordered_predicate_types(&operand,&low)?;
            rtdb_plan::validate_ordered_predicate_types(&operand,&high)?;
            Expr::Between { operand: Box::new(typed(operand)),low: Box::new(typed(low)),high: Box::new(typed(high)),symmetric: *symmetric }.data_type()
        },
        Expr::SimpleCase { operand, branches, otherwise } => {
            let operand = child(operand)?;
            let mut types = Vec::with_capacity(branches.len() + 1);
            for (candidate,value) in branches { rtdb_plan::validate_equality_types(&operand,&child(candidate)?)?;types.push(child(value)?); }
            types.push(child(otherwise)?);
            L::join_all(&types)
        }
        Expr::Case { branches, otherwise } => {
            let mut types = branches.iter().map(|(condition, value)| { child(condition)?; child(value) }).collect::<Result<Vec<_>>>()?; types.push(child(otherwise)?); L::join_all(&types)
        }
        Expr::Coalesce(args) => coalesce_type(args, input, emitter)?,
        Expr::List(args) => L::list(L::join_all(&args.iter().map(child).collect::<Result<Vec<_>>>()?)),
        Expr::Struct(fields) => L::Struct(fields.iter().map(|(name, expr)| Ok((name.clone(), child(expr)?))).collect::<Result<_>>()?),
        Expr::RowCopy(fields) => L::Struct(fields.iter().map(|field| {
            let actual = input.get(field.id).ok_or_else(|| invalid("runtime-column", "Raw record input column is absent"))?;
            Ok((field.name.clone(), actual.data_type.clone()))
        }).collect::<Result<_>>()?),
        Expr::Field { expr, name, data_type } => {
            let ty = child(expr)?;
            if ty == expr.data_type() { return Ok(data_type.clone()); }
            rtdb_plan::field_type(&ty, name)
        }
        Expr::Subscript { expr, index, .. } => rtdb_plan::function_type("nth", &[child(expr)?, child(index)?])?,
        Expr::In { expr, values, negated } => {
            let expression = Expr::In { expr: Box::new(typed(child(expr)?)), values: values.iter().map(|value| child(value).map(typed)).collect::<Result<_>>()?, negated: *negated };
            rtdb_plan::validate_equality_expression(&expression)?;
            expression.data_type()
        }
        Expr::Like { expr: value, pattern, case_insensitive, .. } => {
            let (value_type, pattern_type) = (child(value)?, child(pattern)?);
            rtdb_plan::function_type(if *case_insensitive { "ilike" } else { "like" }, &[value_type, pattern_type])?
        }
    })
}

#[cfg(test)]
mod parameter_tests {
    use super::*;
    use rtdb_plan::ApplyKind;
    use rtdb_types::Value;

    fn field(id: u32, ty: L) -> Field { Field { id: ColumnId(id), name: format!("c{id}"), qualifier: None, qualified_only: false, unnamed: false, data_type: ty } }
    fn parameter(id: u32, ty: L) -> Arc<Relation> {
        Arc::new(Relation::new(Schema::new(vec![field(id, ty.clone())]).unwrap(), RelationNode::Values { rows: vec![vec![Expr::Parameter { index: 0, data_type: ty }]] }).unwrap())
    }

    #[test]
    fn unknown_parameter_annotations_infer_the_admitted_cell_type() {
        let input = parameter(0, L::Null);
        assert_eq!(parameter_types(&input, &[Value::I64(7)], EmissionMode::Execution), [L::Scalar(ScalarType::I64)]);
    }

    #[test]
    fn raw_null_parameters_infer_null_when_the_annotation_excludes_it() {
        for ty in [L::Scalar(ScalarType::I64), L::Scalar(ScalarType::I64).maybe()] {
            let input = parameter(0, ty.clone());
            let expected = if ty.nullable() { ty } else { L::Null };
            assert_eq!(parameter_types(&input, &[Value::Null], EmissionMode::Execution), [expected]);
        }
    }

    #[test]
    fn apply_and_mark_local_parameters_do_not_replace_external_declarations() {
        let input = parameter(0, L::Scalar(ScalarType::U64).maybe());
        let dependent = parameter(1, L::Scalar(ScalarType::Utf8));
        let arguments = vec![(0, input.schema().fields()[0].expression())];
        let output = field(2, L::Scalar(ScalarType::Utf8).maybe());
        let schema = input.schema().concat(&Schema::new(vec![output.clone()]).unwrap()).unwrap();
        let apply = Relation::new(schema, RelationNode::Apply { input: Arc::clone(&input), dependent: Arc::clone(&dependent), arguments: arguments.clone(), kind: ApplyKind::Scalar, output: Some(output) }).unwrap();
        let output = field(2, L::Scalar(ScalarType::Bool).maybe());
        let schema = input.schema().concat(&Schema::new(vec![output.clone()]).unwrap()).unwrap();
        let mark = Relation::new(schema, RelationNode::Mark { input, dependent, arguments, condition: None, comparison: Expr::Literal(Value::Bool(true)), output }).unwrap();
        for relation in [&apply, &mark] {
            assert_eq!(parameter_types(relation, &[Value::I64(-1)], EmissionMode::Execution), [L::Scalar(ScalarType::I64).maybe()]);
        }
    }
}
