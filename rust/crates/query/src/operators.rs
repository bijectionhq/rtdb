use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use rtdb_expr::{integer, is_true, null_eq};
use rtdb_plan::{ApplyKind, Expr, NamedExpr, Relation, RelationNode, Schema, SetOperator};
use rtdb_types::{Result, Value};

use crate::context::Execution;
use crate::cursor::{BoxCursor, OwnedRow, RowCursor};
use crate::spill::{SpillReader, SpillWriter};
use crate::{invalid, QueryContext};

pub(crate) fn build(relation: Arc<Relation>, execution: Execution) -> Result<BoxCursor> {
    Ok(match relation.node() {
        RelationNode::Values { .. } => Box::new(Values { relation, execution, row: 0 }),
        RelationNode::Scan(scan) => if crate::system_schema::definition(&scan.table.name.table).is_some() {
            Box::new(crate::system::SystemCursor::new(scan.clone(), execution))
        } else { Box::new(crate::scan::ScanCursor::new(scan.clone(), execution)?) },
        RelationNode::Project { input, expressions } => Box::new(Project { input: build(Arc::clone(input), execution.clone())?, schema: input.schema().clone(), expressions: expressions.clone(), execution }),
        RelationNode::Filter { input, predicate } => Box::new(Filter { input: build(Arc::clone(input), execution.clone())?, schema: input.schema().clone(), predicate: predicate.clone(), execution }),
        RelationNode::Limit { input, offset, fetch } => {
            let offset = row_limit(offset.as_ref(), &execution)?.unwrap_or(0);
            let remaining = row_limit(fetch.as_ref(), &execution)?;
            Box::new(Limit { input: Some(build(Arc::clone(input), execution.clone())?), offset, remaining, execution })
        }
        RelationNode::Distinct { input } => Box::new(Distinct { input: build(Arc::clone(input), execution.clone())?, seen: SeenRows::new(&execution.context)?, execution }),
        RelationNode::Sort { input, order_by } => Box::new(crate::sort::SortCursor::new(build(Arc::clone(input), execution.clone())?, input.schema().clone(), order_by.clone(), execution)),
        RelationNode::Join { left, right, kind, condition } => Box::new(crate::join::JoinCursor::new(build(Arc::clone(left), execution.clone())?, build(Arc::clone(right), execution.clone())?, left.schema().clone(), right.schema().clone(), *kind, condition.clone(), execution)?),
        RelationNode::Aggregate { input, groups, aggregates } => Box::new(crate::aggregate::AggregateCursor::new(build(Arc::clone(input), execution.clone())?, input.schema().clone(), groups.clone(), aggregates.clone(), execution)),
        RelationNode::Set { left, right, operator, all } => {
            let left = build(Arc::clone(left), execution.clone())?;
            let right = build(Arc::clone(right), execution.clone())?;
            if *operator == SetOperator::Union {
                let input: BoxCursor = Box::new(Union { left: Some(left), right });
                if *all { input } else { Box::new(Distinct { input, seen: SeenRows::new(&execution.context)?, execution }) }
            } else { Box::new(SetCursor::new(left, right, *operator, *all, execution)?) }
        }
        RelationNode::Unnest { input, expr, ordinality, outer, .. } => Box::new(Unnest { input: build(Arc::clone(input), execution.clone())?, schema: input.schema().clone(), expr: expr.clone(), ordinality: ordinality.is_some(), outer: *outer, current: None, elements: None, index: 0, execution }),
        RelationNode::Apply { input, dependent, arguments, kind, .. } => Box::new(Apply { input: build(Arc::clone(input), execution.clone())?, schema: input.schema().clone(), dependent: Arc::clone(dependent), arguments: arguments.clone(), kind: *kind, execution, current: None, cursor: None, matched: false }),
        RelationNode::PatchGaps { input, iid, valid_from, valid_to, document, from, to } => Box::new(crate::patch::PatchGaps::new(build(Arc::clone(input), execution.clone())?, input.schema(), [*iid, *valid_from, *valid_to, *document], from, to, execution)?),
    })
}

fn row_limit(expr: Option<&Expr>, execution: &Execution) -> Result<Option<u64>> {
    expr.map(|expr| {
        let value = execution.evaluate(expr, &Schema::default(), &[])?;
        integer(&value).and_then(|value| u64::try_from(value).ok()).ok_or_else(|| invalid("invalid-limit", "LIMIT and OFFSET require a non-negative integer"))
    }).transpose()
}

struct Values { relation: Arc<Relation>, execution: Execution, row: usize }
#[async_trait]
impl RowCursor for Values {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        self.execution.context.check_cancelled()?;
        let RelationNode::Values { rows } = self.relation.node() else { unreachable!() };
        let Some(expressions) = rows.get(self.row) else { return Ok(None); };
        let values = expressions.iter().map(|expr| self.execution.evaluate(expr, &Schema::default(), &[])).collect::<Result<_>>()?;
        self.row += 1;
        OwnedRow::new(values, &self.execution.context.memory).map(Some)
    }
}
struct Project { input: BoxCursor, schema: Schema, expressions: Vec<NamedExpr>, execution: Execution }
#[async_trait]
impl RowCursor for Project {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        let Some(row) = self.input.next().await? else { return Ok(None); };
        let values = self.expressions.iter().map(|expr| self.execution.evaluate(&expr.expr, &self.schema, &row.values)).collect::<Result<_>>()?;
        OwnedRow::new(values, &self.execution.context.memory).map(Some)
    }
}
struct Filter { input: BoxCursor, schema: Schema, predicate: Expr, execution: Execution }
#[async_trait]
impl RowCursor for Filter {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        while let Some(row) = self.input.next().await? {
            if is_true(&self.execution.evaluate(&self.predicate, &self.schema, &row.values)?)? { return Ok(Some(row)); }
        }
        Ok(None)
    }
}
struct Limit { input: Option<BoxCursor>, offset: u64, remaining: Option<u64>, execution: Execution }
#[async_trait]
impl RowCursor for Limit {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        self.execution.context.check_cancelled()?;
        if self.remaining == Some(0) { self.input = None; return Ok(None); }
        let Some(input) = &mut self.input else { return Ok(None); };
        while self.offset > 0 {
            self.execution.context.check_cancelled()?;
            if input.next().await?.is_none() { self.input = None; return Ok(None); }
            self.offset -= 1;
        }
        let row = input.next().await?;
        if row.is_none() { self.input = None; }
        else if let Some(remaining) = &mut self.remaining { *remaining -= 1; }
        Ok(row)
    }
}
struct Union { left: Option<BoxCursor>, right: BoxCursor }
#[async_trait]
impl RowCursor for Union {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        if let Some(left) = &mut self.left { if let Some(row) = left.next().await? { return Ok(Some(row)); } }
        self.left = None;
        self.right.next().await
    }
}

pub(crate) fn rows_equal(left: &[Value], right: &[Value]) -> Result<bool> {
    if left.len() != right.len() { return Ok(false); }
    for (left, right) in left.iter().zip(right) { if !null_eq(left, right)? { return Ok(false); } }
    Ok(true)
}
pub(crate) struct SeenRows { writer: SpillWriter, context: Arc<QueryContext> }
impl SeenRows {
    pub fn new(context: &Arc<QueryContext>) -> Result<Self> { Ok(Self { writer: context.spill.create()?, context: Arc::clone(context) }) }
    pub fn insert(&mut self, values: &[Value]) -> Result<bool> {
        let mut reader = self.writer.reader(Arc::clone(&self.context.memory))?;
        while let Some(row) = reader.next()? {
            self.context.check_cancelled()?;
            if rows_equal(values, &row.values)? { return Ok(false); }
        }
        self.writer.write(values, &self.context.memory)?;
        Ok(true)
    }
}
pub(crate) struct OrdinalSet { writer: SpillWriter, context: Arc<QueryContext> }
impl OrdinalSet {
    pub fn new(context: &Arc<QueryContext>) -> Result<Self> { Ok(Self { writer: context.spill.create()?, context: Arc::clone(context) }) }
    pub fn contains(&mut self, ordinal: u64) -> Result<bool> {
        let mut reader = self.writer.reader(Arc::clone(&self.context.memory))?;
        while let Some(row) = reader.next()? {
            self.context.check_cancelled()?;
            if row.values.first() == Some(&Value::U64(ordinal)) { return Ok(true); }
        }
        Ok(false)
    }
    pub fn insert(&mut self, ordinal: u64) -> Result<()> { self.writer.write(&[Value::U64(ordinal)], &self.context.memory) }
}
struct Distinct { input: BoxCursor, seen: SeenRows, execution: Execution }
#[async_trait]
impl RowCursor for Distinct {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        while let Some(row) = self.input.next().await? {
            self.execution.context.check_cancelled()?;
            if self.seen.insert(&row.values)? { return Ok(Some(row)); }
        }
        Ok(None)
    }
}

struct SetCursor { left: BoxCursor, right: Option<BoxCursor>, reader: Option<SpillReader>, operator: SetOperator, all: bool, consumed: OrdinalSet, seen: SeenRows, execution: Execution }
impl SetCursor {
    fn new(left: BoxCursor, right: BoxCursor, operator: SetOperator, all: bool, execution: Execution) -> Result<Self> {
        Ok(Self { left, right: Some(right), reader: None, operator, all, consumed: OrdinalSet::new(&execution.context)?, seen: SeenRows::new(&execution.context)?, execution })
    }
}
#[async_trait]
impl RowCursor for SetCursor {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        if let Some(mut right) = self.right.take() {
            let mut writer = self.execution.context.spill.create()?;
            while let Some(row) = right.next().await? { self.execution.context.check_cancelled()?; writer.write(&row.values, &self.execution.context.memory)?; }
            self.reader = Some(writer.finish()?.reader(Arc::clone(&self.execution.context.memory))?);
        }
        while let Some(left) = self.left.next().await? {
            self.execution.context.check_cancelled()?;
            if !self.all && !self.seen.insert(&left.values)? { continue; }
            let reader = self.reader.as_mut().ok_or_else(|| invalid("set-state", "Set input was not spooled"))?;
            reader.rewind()?;
            let mut ordinal = 0u64;
            let mut matched = false;
            while let Some(right) = reader.next()? {
                self.execution.context.check_cancelled()?;
                if rows_equal(&left.values, &right.values)? && (!self.all || !self.consumed.contains(ordinal)?) {
                    if self.all { self.consumed.insert(ordinal)?; }
                    matched = true;
                    break;
                }
                ordinal = ordinal.checked_add(1).ok_or_else(|| invalid("row-count", "Set row count overflow"))?;
            }
            if matched == (self.operator == SetOperator::Intersect) { return Ok(Some(left)); }
        }
        Ok(None)
    }
}

struct Unnest { input: BoxCursor, schema: Schema, expr: Expr, ordinality: bool, outer: bool, current: Option<OwnedRow>, elements: Option<OwnedRow>, index: usize, execution: Execution }
#[async_trait]
impl RowCursor for Unnest {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        loop {
            self.execution.context.check_cancelled()?;
            if let (Some(row), Some(elements)) = (&self.current, &self.elements) {
                if self.index < elements.values.len() {
                    let mut values = row.values.clone();
                    values.push(elements.values[self.index].clone());
                    self.index += 1;
                    if self.ordinality { values.push(Value::I32(i32::try_from(self.index).map_err(|error| invalid("ordinality-overflow", error))?)); }
                    return OwnedRow::new(values, &self.execution.context.memory).map(Some);
                }
                if self.outer && elements.values.is_empty() && self.index == 0 {
                    self.index = 1;
                    let mut values = row.values.clone();
                    values.push(Value::Null);
                    if self.ordinality { values.push(Value::Null); }
                    return OwnedRow::new(values, &self.execution.context.memory).map(Some);
                }
            }
            self.current = None;
            self.elements = None;
            let Some(row) = self.input.next().await? else { return Ok(None); };
            let elements = match self.execution.evaluate(&self.expr, &self.schema, &row.values)? {
                Value::List(values) | Value::Set(values) => values,
                _ => Vec::new(),
            };
            self.elements = Some(OwnedRow::new(elements, &self.execution.context.memory)?);
            self.current = Some(row);
            self.index = 0;
        }
    }
}

struct Apply { input: BoxCursor, schema: Schema, dependent: Arc<Relation>, arguments: Vec<(usize, Expr)>, kind: ApplyKind, execution: Execution, current: Option<Arc<OwnedRow>>, cursor: Option<BoxCursor>, matched: bool }
impl Apply {
    fn dependent_execution(&self, row: Arc<OwnedRow>) -> Result<Execution> {
        let mut parameters = self.execution.parameters.values.clone();
        for (index, expression) in &self.arguments {
            let count = index.checked_add(1).ok_or_else(|| invalid("parameter-count", "Apply parameter index overflow"))?;
            if parameters.len() < count { parameters.resize(count, Value::Null); }
            parameters[*index] = self.execution.evaluate(expression, &self.schema, &row.values)?;
        }
        let mut outer = self.execution.outer.clone();
        outer.insert(0, (self.schema.clone(), row));
        Ok(Execution { context: Arc::clone(&self.execution.context), parameters: Arc::new(OwnedRow::new(parameters, &self.execution.context.memory)?), outer })
    }
    fn document(&self, row: &OwnedRow) -> Value {
        Value::Struct(self.dependent.schema().fields().iter().zip(&row.values).map(|(field, value)| (field.name.clone(), value.clone())).collect::<BTreeMap<_, _>>())
    }
}
#[async_trait]
impl RowCursor for Apply {
    async fn next(&mut self) -> Result<Option<OwnedRow>> {
        loop {
            self.execution.context.check_cancelled()?;
            if self.current.is_none() {
                let Some(row) = self.input.next().await? else { return Ok(None); };
                let row = Arc::new(row);
                self.cursor = Some(build(Arc::clone(&self.dependent), self.dependent_execution(Arc::clone(&row))?)?);
                self.current = Some(row);
                self.matched = false;
            }
            let row = Arc::clone(self.current.as_ref().ok_or_else(|| invalid("apply-state", "Apply input is absent"))?);
            let cursor = self.cursor.as_mut().ok_or_else(|| invalid("apply-state", "Apply dependent cursor is absent"))?;
            if matches!(self.kind, ApplyKind::Inner | ApplyKind::Left) {
                if let Some(dependent) = cursor.next().await? {
                    self.matched = true;
                    let values = row.values.iter().chain(&dependent.values).cloned().collect();
                    return OwnedRow::new(values, &self.execution.context.memory).map(Some);
                }
                let emit_outer = self.kind == ApplyKind::Left && !self.matched;
                self.current = None;
                self.cursor = None;
                if emit_outer {
                    let mut values = row.values.clone();
                    values.extend((0..self.dependent.schema().len()).map(|_| Value::Null));
                    return OwnedRow::new(values, &self.execution.context.memory).map(Some);
                }
                continue;
            }
            let first = cursor.next().await?;
            let result = match self.kind {
                ApplyKind::Exists => Value::Bool(first.is_some()),
                ApplyKind::Scalar | ApplyKind::Pull => {
                    if cursor.next().await?.is_some() { return Err(invalid("cardinality-violation", "Scalar subquery returned more than one row")); }
                    first.as_ref().map_or(Value::Null, |row| if self.kind == ApplyKind::Scalar { row.values[0].clone() } else { self.document(row) })
                }
                ApplyKind::PullMany => {
                    let mut documents = Vec::new();
                    let mut reservations = Vec::new();
                    if let Some(row) = first { let document = self.document(&row); reservations.push(self.execution.context.memory.reserve(document.retained_bytes()?)?); documents.push(document); }
                    while let Some(row) = self.cursor.as_mut().ok_or_else(|| invalid("apply-state", "Apply dependent cursor is absent"))?.next().await? {
                        self.execution.context.check_cancelled()?;
                        let document = self.document(&row);
                        reservations.push(self.execution.context.memory.reserve(document.retained_bytes()?)?);
                        documents.push(document);
                    }
                    let value = if documents.is_empty() { Value::Null } else { Value::List(documents) };
                    let mut values = row.values.clone();
                    values.push(value);
                    let output = OwnedRow::new(values, &self.execution.context.memory)?;
                    self.current = None;
                    self.cursor = None;
                    return Ok(Some(output));
                }
                _ => unreachable!(),
            };
            let mut values = row.values.clone();
            values.push(result);
            self.current = None;
            self.cursor = None;
            return OwnedRow::new(values, &self.execution.context.memory).map(Some);
        }
    }
}
