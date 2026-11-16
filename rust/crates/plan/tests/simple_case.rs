use rtdb_plan::{Expr,function_type_with_args};
use rtdb_types::{ListKind,LogicalType as L,ScalarType as S,TimeUnit,Value as V};

#[test]
fn simple_case_traverses_the_operand_and_every_candidate_and_result() {
    let args = vec![Expr::Literal(V::Duration { value: 1,unit: TimeUnit::Millisecond })];
    let sleep = Expr::Call { name: "sleep".into(),return_type: function_type_with_args("sleep",&args).unwrap(),args };
    let expression = Expr::SimpleCase {
        operand: Box::new(Expr::Parameter { index: 0,data_type: L::Scalar(S::I64) }),
        branches: vec![(Expr::Parameter { index: 2,data_type: L::Scalar(S::I64) },sleep)],
        otherwise: Box::new(Expr::Parameter { index: 4,data_type: L::Scalar(S::I64) }),
    };
    assert_eq!(expression.parameter_count(),5);
    assert!(expression.is_volatile());
    assert!(expression.requires_suspension());
    assert_eq!(expression.data_type(),L::Scalar(S::I64).maybe());
    let decoded: Expr = serde_json::from_slice(&serde_json::to_vec(&expression).unwrap()).unwrap();
    assert_eq!(decoded,expression);
}

#[test]
fn simple_case_admits_every_declared_equality_pair_before_selection() {
    let set = L::List { kind: ListKind::Set,element: Box::new(L::Nothing) };
    let expression = Expr::SimpleCase {
        operand: Box::new(Expr::Parameter { index: 0,data_type: set.clone() }),
        branches: vec![(Expr::Literal(V::I64(1)),Expr::Literal(V::I64(1))),(Expr::Parameter { index: 1,data_type: set },Expr::Literal(V::I64(2)))],
        otherwise: Box::new(Expr::Literal(V::I64(3))),
    };
    assert_eq!(expression.validate_columns(&Default::default()).unwrap_err().code,"xtdb.expression/set-equality");
}
