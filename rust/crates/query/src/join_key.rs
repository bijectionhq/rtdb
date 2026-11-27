use std::sync::Arc;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_plan::{BinaryOp, ColumnId, Expr, Field, Relation, RelationNode, Schema};
use rtdb_types::Result;
use crate::{
    QueryContext, columnar::ColumnBatch, context::Execution, invalid,
    runtime_schema::{expression_bound, schema_bound}, set_keys::{JoinHash, Key},
};

pub(crate) fn hash_join_row(
    batch: &ColumnBatch, row: usize, positions: &[usize], projected: bool, hashing: JoinHash, context: &QueryContext,
) -> Result<u32> {
    Key::for_columns(batch, row, projected, positions, context)?.join_hash(hashing)
}

pub(crate) struct JoinInput {
    pub original: Schema,
    pub schema: Schema,
    expressions: Vec<Expr>,
    key_positions: Vec<usize>,
}

pub(crate) struct JoinLayout {
    pub left: JoinInput,
    pub right: JoinInput,
    pub schema: Schema,
    pub theta_schema: Schema,
    pub condition: Option<Expr>,
    pub comparisons: Vec<Expr>,
    _reservation: Arc<MemoryReservation>,
}

struct InputBuilder<'a> { original: &'a Schema, fields: Vec<Field>, expressions: Vec<Expr>, key_positions: Vec<usize> }

impl<'a> InputBuilder<'a> {
    fn new(schema: &'a Schema) -> Self { Self { original: schema, fields: schema.fields().to_vec(), expressions: Vec::new(), key_positions: Vec::new() } }
    fn key(&mut self, expression: &mut Expr, next_id: &mut Option<u32>) -> Result<()> {
        self.project(expression, next_id)?;
        let Expr::Column { id, .. } = expression else { return Err(invalid("join-key", "Join key is not a projected column")); };
        self.key_positions.push(self.fields.iter().position(|field| field.id == *id).ok_or_else(|| invalid("join-key", "Join key column is absent"))?);
        Ok(())
    }
    fn project(&mut self, expression: &mut Expr, next_id: &mut Option<u32>) -> Result<()> {
        if matches!(expression, Expr::Column { .. }) { return Ok(()); }
        let id = next_id.ok_or_else(|| invalid("column-id", "Join key column identifiers are exhausted"))?;
        *next_id = id.checked_add(1);
        let field = Field { id: ColumnId(id), name: format!("?join-expr-{id}"), qualifier: None, qualified_only: false, unnamed: false, data_type: expression.data_type() };
        self.expressions.push(std::mem::replace(expression, field.expression()));
        self.fields.push(field);
        Ok(())
    }
    fn finish(self) -> Result<JoinInput> { Ok(JoinInput { original: self.original.clone(), schema: Schema::new(self.fields)?, expressions: self.expressions, key_positions: self.key_positions }) }
}

impl JoinLayout {
    pub fn new(left: &Relation, right: &Relation, condition: Option<&Expr>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bound = schema_bound(left.schema())?.checked_add(schema_bound(right.schema())?)
            .and_then(|bytes| bytes.checked_mul(4)).and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(size_error)?;
        let bound = bound.checked_add(condition.map(expression_bound).transpose()?.unwrap_or(0).checked_mul(8).ok_or_else(size_error)?).ok_or_else(size_error)?;
        let reservation = memory.reserve(bound)?;
        let mut next_id = maximum_id(left).max(maximum_id(right)).map_or(Some(0), |id| id.checked_add(1));
        let mut left = InputBuilder::new(left.schema());
        let mut right = InputBuilder::new(right.schema());
        let comparisons = condition.cloned().map(|condition| project_keys(condition, &mut left, &mut right, &mut next_id)).transpose()?.unwrap_or_default();
        let condition = comparisons.iter().cloned().reduce(|first,second|Expr::binary(BinaryOp::And,first,second));
        let left = left.finish()?; let right = right.finish()?;
        let schema = left.schema.concat(&right.schema)?;
        let theta_schema = schema.clone();
        Ok(Self { left, right, schema, theta_schema, condition, comparisons, _reservation: reservation })
    }
    pub fn resolve_types(&mut self,left: &Schema,right: &Schema,execution: &Execution) -> Result<()> {
        let scope = execution.scope()?;
        let bound = schema_bound(left)?.checked_add(schema_bound(right)?).and_then(|bytes|bytes.checked_mul(4)).and_then(|bytes|bytes.checked_add(self._reservation.bytes())).ok_or_else(size_error)?;
        scope.reserve(bound)?;
        self.left.resolve_types(left,execution,&scope)?;
        self.right.resolve_types(right,execution,&scope)?;
        let bound = schema_bound(&self.left.original)?.checked_add(schema_bound(&self.right.original)?).and_then(|bytes|bytes.checked_add(schema_bound(&self.left.schema).ok()?)).and_then(|bytes|bytes.checked_add(schema_bound(&self.right.schema).ok()?)).and_then(|bytes|bytes.checked_mul(4)).and_then(|bytes|bytes.checked_add(self._reservation.bytes())).ok_or_else(size_error)?;
        let reservation = execution.context.memory.reserve(bound)?;
        self.schema = self.left.schema.concat(&self.right.schema)?;
        self.theta_schema = self.schema.clone();
        self._reservation = reservation;
        Ok(())
    }
    pub fn into_reservation(self) -> Arc<MemoryReservation> { self._reservation }
}

impl JoinInput {
    fn resolve_types(&mut self,input: &Schema,execution: &Execution,scope: &rtdb_expr::EvaluationScope) -> Result<()> {
        if self.original.fields().iter().map(|field|field.id).ne(input.fields().iter().map(|field|field.id)) { return Err(invalid("join-schema","Runtime join columns disagree with the planned layout")); }
        let mut fields = input.fields().to_vec();
        for (field,expression) in self.schema.fields()[self.original.len()..].iter().zip(&self.expressions) {
            let data_type = execution.expression_type(scope,expression,input)?;
            let mut field = field.clone();field.data_type = data_type;fields.push(field);
        }
        self.original = input.clone();
        self.schema = Schema::new(fields)?;
        Ok(())
    }
    pub fn project_batches(&self, input: crate::columnar::BoxBatchCursor, execution: &Execution) -> crate::columnar::BoxBatchCursor {
        if self.expressions.is_empty() { return input; }
        let projections = self.original.fields().iter().map(Field::expression).chain(self.expressions.iter().cloned()).collect();
        Box::new(crate::columnar::BatchExpressions::new(input, self.original.clone(), projections, execution.clone()).validating_input(true))
    }

    pub fn key_count(&self) -> usize { self.key_positions.len() }
    pub fn key_positions(&self) -> &[usize] { &self.key_positions }
}

fn project_keys(condition: Expr, left: &mut InputBuilder<'_>, right: &mut InputBuilder<'_>, next_id: &mut Option<u32>) -> Result<Vec<Expr>> {
    fn collect(condition: Expr, left: &mut InputBuilder<'_>, right: &mut InputBuilder<'_>, next_id: &mut Option<u32>, keys: &mut Vec<Expr>, predicates: &mut Vec<Expr>) -> Result<()> {
        match condition {
            Expr::Binary { op: BinaryOp::And, left: first, right: second } => { collect(*first, left, right, next_id, keys, predicates)?; collect(*second, left, right, next_id, keys, predicates)?; }
            mut condition => {
                let mut key = false;
                if let Expr::Binary { op: BinaryOp::Eq, left: first, right: second } = &mut condition {
                    if belongs_to(first, left.original) && belongs_to(second, right.original) {
                        left.key(first, next_id)?; right.key(second, next_id)?; key = true;
                    } else if belongs_to(first, right.original) && belongs_to(second, left.original) {
                        right.key(first, next_id)?; left.key(second, next_id)?; key = true;
                    }
                }
                if key { keys.push(condition); } else { predicates.push(condition); }
            }
        }
        Ok(())
    }
    let mut keys = Vec::new(); let mut predicates = Vec::new();
    collect(condition, left, right, next_id, &mut keys, &mut predicates)?;
    if let Some(theta) = predicates.into_iter().reduce(|first,second|Expr::binary(BinaryOp::And,first,second)) { keys.push(theta); }
    Ok(keys)
}

fn belongs_to(expression: &Expr, schema: &Schema) -> bool {
    let columns = expression.referenced_columns();
    !columns.is_empty() && columns.iter().all(|id| schema.get(*id).is_some())
}

fn maximum_id(relation: &Relation) -> Option<u32> {
    let here = relation.schema().fields().iter().map(|field| field.id.0).max();
    let children = match relation.node() {
        RelationNode::Values { .. } | RelationNode::LiteralPages { .. } | RelationNode::ArrowLiteralPages { .. } | RelationNode::List { .. } | RelationNode::ColumnTable { .. } | RelationNode::Scan(_) | RelationNode::Reference { .. } => None,
        RelationNode::Project { input, .. } | RelationNode::Extend { input, .. } | RelationNode::Rename { input, .. } | RelationNode::Filter { input, .. }
        | RelationNode::Aggregate { input, .. } | RelationNode::Window { input, .. } | RelationNode::Sort { input, .. }
        | RelationNode::SortLimit { input, .. } | RelationNode::Limit { input, .. } | RelationNode::Distinct { input }
        | RelationNode::Unnest { input, .. } | RelationNode::Ordinality { input, .. } | RelationNode::PatchGaps { input, .. } => maximum_id(input),
        RelationNode::Join { left, right, .. } | RelationNode::Set { left, right, .. } => maximum_id(left).max(maximum_id(right)),
        RelationNode::Apply { input, dependent, .. } | RelationNode::Mark { input, dependent, .. } => maximum_id(input).max(maximum_id(dependent)),
        RelationNode::Let { binding, body } => maximum_id(&binding.relation).max(maximum_id(body)),
    };
    here.max(children)
}

fn size_error() -> rtdb_types::Anomaly { invalid("join-layout-size", "Join key layout exceeds addressable memory") }

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::{LogicalType, ScalarType, Value};

    fn input(id: u32) -> Relation {
        Relation::empty(Schema::new(vec![Field { id: ColumnId(id), name: "v".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) }]).unwrap())
    }

    #[test]
    fn computed_equi_operands_are_cached_per_occurrence_and_replaced_with_columns() {
        let left = input(0); let right = input(1);
        let random = Expr::Call { name: "random".into(), args: Vec::new(), return_type: LogicalType::Scalar(ScalarType::F64) };
        let computed = Expr::binary(BinaryOp::Add, left.schema().fields()[0].expression(), random);
        let equality = Expr::binary(BinaryOp::Eq, computed.clone(), right.schema().fields()[0].expression());
        let memory = MemoryBudget::new(1 << 24);
        let layout = JoinLayout::new(&left, &right, Some(&Expr::binary(BinaryOp::And, equality.clone(), equality)), &memory).unwrap();
        assert_eq!(layout.left.expressions, [computed.clone(), computed]);
        assert!(layout.right.expressions.is_empty());
        assert_eq!(layout.left.schema.len(), 3);
        let Some(Expr::Binary { op: BinaryOp::And, left, right }) = &layout.condition else { panic!("Expected conjunction") };
        for (expression, expected) in [(left, ColumnId(2)), (right, ColumnId(3))] {
            assert!(matches!(expression.as_ref(), Expr::Binary { op: BinaryOp::Eq, left, right } if matches!(left.as_ref(), Expr::Column { id, .. } if *id == expected) && matches!(right.as_ref(), Expr::Column { id: ColumnId(1), .. })));
        }
        drop(layout); assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn constants_and_cross_side_operands_remain_in_the_predicate() {
        let left = input(0); let right = input(1);
        let combined = Expr::binary(BinaryOp::Add, left.schema().fields()[0].expression(), right.schema().fields()[0].expression());
        let condition = Expr::binary(BinaryOp::Eq, combined, Expr::Literal(Value::I64(2)));
        let memory = MemoryBudget::new(1 << 24);
        let layout = JoinLayout::new(&left, &right, Some(&condition), &memory).unwrap();
        assert_eq!(layout.condition, Some(condition));
        assert!(layout.left.expressions.is_empty()); assert!(layout.right.expressions.is_empty());
        drop(layout); assert_eq!(memory.used_bytes(), 0);
    }
}
