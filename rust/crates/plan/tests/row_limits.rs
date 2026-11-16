use std::sync::Arc;
use rtdb_plan::{BinaryOp,Expr,Relation,RelationNode,Schema};
use rtdb_types::{LogicalType,ScalarType,Value};

#[test]
fn row_limit_plans_admit_only_nonnegative_signed_literals_and_parameters() {
    let input = Arc::new(Relation::new(Schema::default(),RelationNode::Values { rows: vec![vec![]] }).unwrap());
    for expression in [Expr::Literal(Value::I8(0)),Expr::Literal(Value::I64(i64::MAX)),Expr::Parameter { index: 0,data_type: LogicalType::Scalar(ScalarType::I64) }] {
        assert!(Arc::clone(&input).limit(None,Some(expression.clone())).is_ok());
        assert!(Relation::new(Schema::default(),RelationNode::SortLimit { input: Arc::clone(&input),order_by: vec![],offset: Some(expression),fetch: None }).is_ok());
    }
    for expression in [Expr::Literal(Value::Null),Expr::Literal(Value::I64(-1)),Expr::Literal(Value::F64(1.0)),Expr::binary(BinaryOp::Add,Expr::Literal(Value::I64(1)),Expr::Literal(Value::I64(1)))] {
        assert!(Arc::clone(&input).limit(None,Some(expression.clone())).is_err());
        assert!(Relation::new(Schema::default(),RelationNode::SortLimit { input: Arc::clone(&input),order_by: vec![],offset: Some(expression),fetch: None }).is_err());
    }
}
