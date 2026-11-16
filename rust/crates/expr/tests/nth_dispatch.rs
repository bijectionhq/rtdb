use rtdb_expr::{EvalContext,evaluate};
use rtdb_plan::{BinaryOp,Expr,Schema,function_type_with_args};
use rtdb_types::Value as V;

fn int(value: i64) -> Expr { Expr::Literal(V::I64(value)) }
fn divide() -> Expr { Expr::binary(BinaryOp::Divide,int(1),int(0)) }
fn nth(input: Expr,index: Expr,call: bool) -> Expr {
    let data_type = function_type_with_args("nth",&[input.clone(),index.clone()]).unwrap();
    if call { Expr::Call { name: "nth".into(),args: vec![input,index],return_type: data_type } }
    else { Expr::Subscript { expr: Box::new(input),index: Box::new(index),data_type } }
}
fn run(expression: &Expr) -> rtdb_types::Result<V> { evaluate(expression,&Schema::default(),&[],&EvalContext::default()) }

#[test]
fn nth_constant_null_dispatch_does_not_consume_monomorphic_payloads() {
    for call in [false,true] {
        assert_eq!(run(&nth(divide(),int(0),call)).unwrap(),V::Null);
        assert_eq!(run(&nth(Expr::List(vec![divide()]),Expr::Literal(V::Null),call)).unwrap(),V::Null);
        assert_eq!(run(&nth(Expr::Literal(V::Null),divide(),call)).unwrap(),V::Null);
        assert_eq!(run(&nth(Expr::List(vec![divide()]),int(0),call)).unwrap_err().code,"xtdb.expression/division-by-zero");
    }
}

#[test]
fn nth_keeps_control_and_polymorphic_box_evaluation_before_constant_outcomes() {
    for call in [false,true] {
        let condition = Expr::binary(BinaryOp::Eq,divide(),int(0));
        let controlled = Expr::Case { branches: vec![(condition,int(1))],otherwise: Box::new(int(2)) };
        assert_eq!(run(&nth(controlled,int(0),call)).unwrap_err().code,"xtdb.expression/division-by-zero");
        let boxed = Expr::Case { branches: vec![(Expr::Literal(V::Bool(true)),divide())],otherwise: Box::new(Expr::Literal(V::Null)) };
        assert_eq!(run(&nth(boxed,int(0),call)).unwrap_err().code,"xtdb.expression/division-by-zero");
    }
}

#[test]
fn nth_rejects_noninteger_indexes_before_discarding_scalar_payloads() {
    for call in [false,true] {
        let input = divide();
        let index = Expr::Literal(V::F64(0.0));
        let expression = if call { Expr::Call { name: "nth".into(),args: vec![input,index],return_type: rtdb_types::LogicalType::Null } }
            else { Expr::Subscript { expr: Box::new(input),index: Box::new(index),data_type: rtdb_types::LogicalType::Null } };
        assert_eq!(run(&expression).unwrap_err().code,"xtdb.expression/function-type-mismatch");
    }
    assert_eq!(run(&nth(Expr::Literal(V::Set(vec![V::I64(1)])),divide(),true)).unwrap(),V::Null);
    assert_eq!(run(&nth(Expr::Literal(V::Null),Expr::Literal(V::F64(0.0)),true)).unwrap(),V::Null);
}
