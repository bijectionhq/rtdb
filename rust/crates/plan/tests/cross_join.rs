use std::sync::Arc;
use rtdb_plan::{Expr,JoinKind,Relation,RelationNode,Schema};
use rtdb_types::Value;

#[test]
fn cross_join_accepts_only_the_conditionless_operator_form() {
    let left = Arc::new(Relation::empty(Schema::default()));let right = Arc::clone(&left);
    for condition in [Some(Expr::Literal(Value::Bool(true))),Some(Expr::Literal(Value::Null))] {
        assert!(Relation::new(Schema::default(),RelationNode::Join { left: Arc::clone(&left),right: Arc::clone(&right),kind: JoinKind::Cross,condition }).is_err());
    }
    assert!(Relation::new(Schema::default(),RelationNode::Join { left,right,kind: JoinKind::Cross,condition: None }).is_ok());
}
