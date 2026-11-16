use std::sync::Arc;
use rtdb_plan::{AggregateAlias, AggregateExpr, AggregateFunction, AggregateName, ColumnId, Expr, Field, PercentileFraction, Relation, RelationNode, Schema};
use rtdb_types::{LogicalType, ScalarType, Value};

fn field() -> Field { Field { id: ColumnId(0), name: "sum".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64).maybe() } }

#[test]
fn aggregate_plan_requires_computed_inputs_to_be_projected_before_filtering() {
    let output = field();
    let aggregate = AggregateExpr { field: output.clone(), function: AggregateFunction::Sum, argument: Some(Expr::Literal(Value::I64(1))), distinct: false, filter: Some(Expr::Literal(Value::Bool(false))), order_by: Vec::new() };
    assert!(Relation::new(Schema::new(vec![output]).unwrap(), RelationNode::Aggregate { input: Arc::new(Relation::unit()), groups: Vec::new(), aggregates: vec![aggregate] }).is_err());
}

#[test]
fn percentile_integer_fraction_roundtrips_without_float_coercion() {
    for value in [i64::MIN, -1, 0, 1, i64::MAX] {
        let fraction = PercentileFraction::Integer(value);
        let encoded = serde_json::to_vec(&fraction).unwrap();
        assert_eq!(serde_json::from_slice::<PercentileFraction>(&encoded).unwrap(), fraction);
    }
}

#[test]
fn factory_spelling_retains_quantifier_and_canonical_operator() {
    assert_eq!(AggregateAlias::BoolAnd.name(), "bool_and");
    assert_eq!(AggregateAlias::BoolAnd.canonical(), AggregateFunction::Every);
    assert_eq!(AggregateAlias::SumAllHyphen.name(), "sum-all");
    assert_eq!(AggregateAlias::SumAllHyphen.distinct(), Some(false));
    assert_eq!(AggregateAlias::SumDistinctHyphen.distinct(), Some(true));
    assert_eq!(AggregateAlias::SumDistinctHyphen.canonical(), AggregateFunction::Sum);
}

#[test]
fn named_factories_roundtrip_original_case_and_validate_quantifier_identity() {
    for (spelling, canonical, distinct) in [("SUM_ALL", AggregateFunction::Sum, false), ("sUm-DiStInCt", AggregateFunction::Sum, true), ("ArRaY_aGg-DISTINCT", AggregateFunction::ArrayAgg, true), ("RoW-CoUnT", AggregateFunction::CountStar, false)] {
        let function = AggregateFunction::Named(AggregateName::new(spelling).unwrap());
        assert_eq!(function.name(), spelling); assert_eq!(function.canonical(), canonical); assert_eq!(function.quantifier(), Some(distinct));
        assert_eq!(serde_json::from_slice::<AggregateFunction>(&serde_json::to_vec(&function).unwrap()).unwrap(), function);
    }
    for invalid in ["average", "vec_agg_DISTINCT", "bool-and-all", "namespace/sum", "xt/sum"] {
        assert!(AggregateName::new(invalid).is_err());
        assert!(serde_json::from_value::<AggregateName>(serde_json::Value::String(invalid.into())).is_err());
    }
    let input_field = Field { id: ColumnId(1), name: "v".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: LogicalType::Scalar(ScalarType::I64) };
    let input = Arc::new(Relation::empty(Schema::new(vec![input_field.clone()]).unwrap()));
    for distinct in [false, true] {
        let aggregate = AggregateExpr { field: field(), function: AggregateFunction::Named(AggregateName::new("SUM-DISTINCT").unwrap()), argument: Some(input_field.expression()), distinct, filter: None, order_by: Vec::new() };
        assert_eq!(Relation::new(Schema::new(vec![field()]).unwrap(), RelationNode::Aggregate { input: Arc::clone(&input), groups: Vec::new(), aggregates: vec![aggregate] }).is_ok(), distinct);
    }
}
