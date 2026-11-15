use crate::{ColumnId, Expr, Field, Schema, TableBinding};
use rtdb_types::{Anomaly, Iid, LogicalType, Result, ScalarType, Value};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum TemporalFilter {
    Default,
    All,
    At(Expr),
    FromTo { from: Expr, to: Expr },
    Between { from: Expr, to: Expr },
}

impl TemporalFilter {
    pub fn expressions(&self) -> Vec<&Expr> {
        match self { Self::At(at) => vec![at], Self::FromTo { from, to } | Self::Between { from, to } => vec![from, to], _ => vec![] }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Scan {
    pub table: TableBinding,
    pub columns: Vec<Field>,
    pub valid_time: TemporalFilter,
    pub system_time: TemporalFilter,
    pub clamp_valid_time: bool,
    pub iids: Option<BTreeSet<Iid>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NamedExpr { pub field: Field, pub expr: Expr }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinKind { Inner, Left, Right, Full, Semi, Anti, Cross }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
/// Evaluates the dependent relation for each input row with apply-local argument bindings.
/// Scalar and Pull reject multiple dependent rows; Scalar, Pull, and PullMany yield null on no rows.
pub enum ApplyKind { Inner, Left, Scalar, Exists, Pull, PullMany }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SetOperator { Union, Intersect, Except }

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SortExpr { pub expr: Expr, pub descending: bool, pub nulls_first: bool }

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateFunction {
    Count, CountStar, Sum, Average, Min, Max, ArrayAgg, Every, Any,
    VariancePopulation, VarianceSample, StddevPopulation, StddevSample,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AggregateExpr {
    pub field: Field,
    pub function: AggregateFunction,
    pub argument: Option<Expr>,
    pub distinct: bool,
    pub filter: Option<Expr>,
    pub order_by: Vec<SortExpr>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RelationNode {
    Values { rows: Vec<Vec<Expr>> },
    Scan(Scan),
    Project { input: Arc<Relation>, expressions: Vec<NamedExpr> },
    Filter { input: Arc<Relation>, predicate: Expr },
    Join { left: Arc<Relation>, right: Arc<Relation>, kind: JoinKind, condition: Option<Expr> },
    Apply { input: Arc<Relation>, dependent: Arc<Relation>, arguments: Vec<(usize, Expr)>, kind: ApplyKind, output: Option<Field> },
    Aggregate { input: Arc<Relation>, groups: Vec<NamedExpr>, aggregates: Vec<AggregateExpr> },
    Sort { input: Arc<Relation>, order_by: Vec<SortExpr> },
    Limit { input: Arc<Relation>, offset: Option<Expr>, fetch: Option<Expr> },
    Distinct { input: Arc<Relation> },
    Set { left: Arc<Relation>, right: Arc<Relation>, operator: SetOperator, all: bool },
    Unnest { input: Arc<Relation>, expr: Expr, element: Field, ordinality: Option<Field>, outer: bool },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RelationParts", into = "RelationParts")]
pub struct Relation { schema: Schema, node: RelationNode }

#[derive(Serialize, Deserialize)]
struct RelationParts { schema: Schema, node: RelationNode }

impl Relation {
    pub fn new(schema: Schema, node: RelationNode) -> Result<Self> {
        let result = Self { schema, node };
        result.validate()?;
        Ok(result)
    }
    pub fn schema(&self) -> &Schema { &self.schema }
    pub fn node(&self) -> &RelationNode { &self.node }
    pub fn empty(schema: Schema) -> Self { Self { schema, node: RelationNode::Values { rows: vec![] } } }
    pub fn unit() -> Self { Self { schema: Schema::default(), node: RelationNode::Values { rows: vec![vec![]] } } }

    pub fn scan(scan: Scan) -> Result<Self> { Self::new(Schema::new(scan.columns.clone())?, RelationNode::Scan(scan)) }
    pub fn project(self: Arc<Self>, expressions: Vec<NamedExpr>) -> Result<Self> {
        Self::new(Schema::new(expressions.iter().map(|expr| expr.field.clone()).collect())?, RelationNode::Project { input: self, expressions })
    }
    pub fn filter(self: Arc<Self>, predicate: Expr) -> Result<Self> {
        Self::new(self.schema.clone(), RelationNode::Filter { input: self, predicate })
    }
    pub fn sort(self: Arc<Self>, order_by: Vec<SortExpr>) -> Result<Self> {
        Self::new(self.schema.clone(), RelationNode::Sort { input: self, order_by })
    }
    pub fn limit(self: Arc<Self>, offset: Option<Expr>, fetch: Option<Expr>) -> Result<Self> {
        Self::new(self.schema.clone(), RelationNode::Limit { input: self, offset, fetch })
    }

    pub fn children(&self) -> Vec<&Arc<Self>> {
        match &self.node {
            RelationNode::Values { .. } | RelationNode::Scan(_) => vec![],
            RelationNode::Project { input, .. } | RelationNode::Filter { input, .. }
            | RelationNode::Aggregate { input, .. } | RelationNode::Sort { input, .. }
            | RelationNode::Limit { input, .. } | RelationNode::Distinct { input }
            | RelationNode::Unnest { input, .. } => vec![input],
            RelationNode::Join { left, right, .. } | RelationNode::Set { left, right, .. } => vec![left, right],
            RelationNode::Apply { input, dependent, .. } => vec![input, dependent],
        }
    }

    pub fn expressions(&self) -> Vec<&Expr> {
        match &self.node {
            RelationNode::Values { rows } => rows.iter().flatten().collect(),
            RelationNode::Scan(scan) => scan.valid_time.expressions().into_iter().chain(scan.system_time.expressions()).collect(),
            RelationNode::Project { expressions, .. } => expressions.iter().map(|expr| &expr.expr).collect(),
            RelationNode::Filter { predicate, .. } => vec![predicate],
            RelationNode::Join { condition, .. } => condition.iter().collect(),
            RelationNode::Apply { arguments, .. } => arguments.iter().map(|(_, expr)| expr).collect(),
            RelationNode::Aggregate { groups, aggregates, .. } => groups.iter().map(|expr| &expr.expr).chain(aggregates.iter().flat_map(|agg| {
                agg.argument.iter().chain(agg.filter.iter()).chain(agg.order_by.iter().map(|order| &order.expr))
            })).collect(),
            RelationNode::Sort { order_by, .. } => order_by.iter().map(|order| &order.expr).collect(),
            RelationNode::Limit { offset, fetch, .. } => offset.iter().chain(fetch.iter()).collect(),
            RelationNode::Unnest { expr, .. } => vec![expr],
            RelationNode::Distinct { .. } | RelationNode::Set { .. } => vec![],
        }
    }

    pub fn parameter_count(&self) -> usize {
        self.parameter_indices().last().map_or(0, |index| index.saturating_add(1))
    }

    pub fn parameter_indices(&self) -> BTreeSet<usize> {
        let mut indices = BTreeSet::new();
        for expr in self.expressions() {
            expr.visit(&mut |expr| { if let Expr::Parameter { index, .. } = expr { indices.insert(*index); } });
        }
        if let RelationNode::Apply { input, dependent, arguments, .. } = &self.node {
            indices.extend(input.parameter_indices());
            let bound: BTreeSet<_> = arguments.iter().map(|(index, _)| *index).collect();
            indices.extend(dependent.parameter_indices().difference(&bound));
        } else {
            for child in self.children() { indices.extend(child.parameter_indices()); }
        }
        indices
    }

    pub fn dependencies(&self) -> Vec<TableBinding> {
        let mut result = if let RelationNode::Scan(scan) = &self.node { vec![scan.table.clone()] } else { vec![] };
        for child in self.children() {
            for dependency in child.dependencies() { if !result.contains(&dependency) { result.push(dependency); } }
        }
        result
    }

    pub fn validate(&self) -> Result<()> {
        let inputs: BTreeSet<ColumnId> = self.children().iter().flat_map(|child| child.schema.ids()).collect();
        for expr in self.expressions() { expr.validate_columns(&inputs)?; }
        let invalid = |message| Err(Anomaly::fault("rtdb.plan/invalid-relation", message));
        match &self.node {
            RelationNode::Values { rows } => {
                if rows.iter().any(|row| row.len() != self.schema.len()) { return invalid("VALUES row width differs from schema"); }
            }
            RelationNode::Scan(scan) => {
                if self.schema.fields() != scan.columns { return invalid("Scan columns differ from output schema"); }
            }
            RelationNode::Project { expressions, .. } => {
                if !self.schema.fields().iter().eq(expressions.iter().map(|expr| &expr.field)) { return invalid("Projection fields differ from output schema"); }
            }
            RelationNode::Filter { input, .. } | RelationNode::Sort { input, .. }
            | RelationNode::Limit { input, .. } | RelationNode::Distinct { input } => {
                if self.schema != input.schema { return invalid("Pass-through operator changes its schema"); }
            }
            RelationNode::Join { left, right, kind, .. } => {
                let expected = match kind {
                    JoinKind::Semi | JoinKind::Anti => left.schema.clone(),
                    JoinKind::Left => left.schema.concat(&right.schema.nullable()?)?,
                    JoinKind::Right => left.schema.nullable()?.concat(&right.schema)?,
                    JoinKind::Full => left.schema.nullable()?.concat(&right.schema.nullable()?)?,
                    JoinKind::Inner | JoinKind::Cross => left.schema.concat(&right.schema)?,
                };
                if self.schema != expected { return invalid("Join schema does not describe its output"); }
            }
            RelationNode::Apply { input, dependent, arguments, kind, output } => {
                let mut parameters = BTreeSet::new();
                for (index, expr) in arguments {
                    if !parameters.insert(index) { return invalid("Apply parameter is bound more than once"); }
                    expr.validate_columns(&input.schema.ids())?;
                }
                let expected = match kind {
                    ApplyKind::Inner | ApplyKind::Left => {
                        if output.is_some() { return invalid("Relational apply cannot declare a scalar output"); }
                        if *kind == ApplyKind::Left { input.schema.concat(&dependent.schema.nullable()?)? }
                        else { input.schema.concat(&dependent.schema)? }
                    }
                    _ => {
                        let Some(output) = output else { return invalid("Scalar apply requires an output field"); };
                        let document = LogicalType::Struct(dependent.schema.fields().iter().map(|field| (field.name.clone(), field.data_type.clone())).collect());
                        let data_type = match kind {
                            ApplyKind::Scalar => {
                                if dependent.schema.len() != 1 { return invalid("Scalar apply requires a single dependent column"); }
                                dependent.schema.fields()[0].data_type.clone().maybe()
                            }
                            ApplyKind::Exists => LogicalType::Scalar(ScalarType::Bool),
                            ApplyKind::Pull => document.maybe(),
                            ApplyKind::PullMany => LogicalType::list(document).maybe(),
                            ApplyKind::Inner | ApplyKind::Left => return invalid("Relational apply cannot declare a scalar result type"),
                        };
                        if output.data_type != data_type { return invalid("Scalar apply output type differs from its result"); }
                        input.schema.concat(&Schema::new(vec![output.clone()])?)?
                    }
                };
                if self.schema != expected { return invalid("Apply schema does not describe its output"); }
            }
            RelationNode::Aggregate { groups, aggregates, .. } => {
                if !self.schema.fields().iter().eq(groups.iter().map(|group| &group.field).chain(aggregates.iter().map(|agg| &agg.field))) {
                    return invalid("Aggregate schema does not describe its output");
                }
                for aggregate in aggregates {
                    if (aggregate.function == AggregateFunction::CountStar) != aggregate.argument.is_none() {
                        return invalid("Only COUNT(*) omits its input expression");
                    }
                }
            }
            RelationNode::Set { left, right, .. } => {
                if self.schema.len() != left.schema.len() || self.schema.len() != right.schema.len() { return invalid("Set inputs have different widths"); }
                for ((output, left), right) in self.schema.fields().iter().zip(left.schema.fields()).zip(right.schema.fields()) {
                    if output.data_type != left.data_type.join(&right.data_type) { return invalid("Set output type is not the join of its inputs"); }
                }
            }
            RelationNode::Unnest { input, element, ordinality, .. } => {
                if !self.schema.fields().iter().eq(input.schema.fields().iter().chain([element]).chain(ordinality.iter())) {
                    return invalid("Unnest fields differ from output schema");
                }
            }
        }
        Ok(())
    }

    pub fn explain_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self).map_err(|error| Anomaly::fault("rtdb.plan/serialization", error.to_string()))
    }
}

impl TryFrom<RelationParts> for Relation {
    type Error = Anomaly;
    fn try_from(parts: RelationParts) -> Result<Self> { Self::new(parts.schema, parts.node) }
}
impl From<Relation> for RelationParts { fn from(relation: Relation) -> Self { Self { schema: relation.schema, node: relation.node } } }

/// Removes literal identity filters without moving a value predicate below temporal resolution.
pub fn simplify(relation: Arc<Relation>) -> Result<Arc<Relation>> {
    if let RelationNode::Filter { input, predicate: Expr::Literal(value) } = relation.node() {
        match value {
            Value::Bool(true) => return simplify(input.clone()),
            Value::Bool(false) | Value::Null => return Ok(Arc::new(Relation::empty(relation.schema.clone()))),
            _ => (),
        }
    }
    Ok(relation)
}
