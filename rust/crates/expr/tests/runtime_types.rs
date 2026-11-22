use rtdb_expr::{EvalContext,OuterRow,evaluate,expression_type};
use rtdb_plan::{BinaryOp as B,ColumnId,Expr,Field,Schema,UnaryOp as U,function_type_with_args};
use rtdb_types::{LogicalType as L,ScalarType as S,Value as V};

fn int(value: i64) -> Expr { Expr::Literal(V::I64(value)) }
fn parameter(index: usize) -> Expr { Expr::Parameter { index,data_type: L::Nothing } }
fn divide() -> Expr { Expr::binary(B::Divide,int(1),int(0)) }
fn branch(otherwise: Expr) -> Expr { Expr::Case { branches: vec![(Expr::Literal(V::Bool(true)),divide())],otherwise: Box::new(otherwise) } }
fn discarded(expr: Expr) -> Expr { Expr::binary(B::Add,expr,Expr::Literal(V::Null)) }
fn schema(ty: L) -> Schema { Schema::new(vec![Field { id: ColumnId(0),name: "v".into(),qualifier: None,qualified_only: false,unnamed: false,data_type: ty }]).unwrap() }
fn call(name: &str,args: Vec<Expr>) -> Expr { let return_type = function_type_with_args(name,&args).unwrap();Expr::Call { name: name.into(),args,return_type } }

#[test]
fn parameter_alternatives_control_case_boxing_before_strict_null_discard() {
    let expr = discarded(branch(parameter(0)));
    let context = EvalContext { parameters: rtdb_expr::Parameters::Values(&[V::Null]),..Default::default() };
    assert_eq!(evaluate(&expr,&Schema::default(),&[],&context).unwrap_err().code,"xtdb.expression/division-by-zero");
    let context = EvalContext { parameters: rtdb_expr::Parameters::Values(&[V::I64(7)]),..Default::default() };
    assert_eq!(evaluate(&expr,&Schema::default(),&[],&context).unwrap(),V::Null);
}

#[test]
fn column_and_correlated_types_retain_unselected_union_legs() {
    let schema = schema(L::Scalar(S::I64).maybe());
    let column = Expr::column(ColumnId(0),L::Nothing);
    assert_eq!(evaluate(&discarded(branch(column)),&schema,&[V::I64(1)],&EvalContext::default()).unwrap_err().code,"xtdb.expression/division-by-zero");
    let rows = [OuterRow { schema: &schema,values: &[V::I64(1)] }];
    let context = EvalContext { outer_rows: &rows,..Default::default() };
    let column = Expr::Correlated { depth: 0,id: ColumnId(0),data_type: L::Nothing };
    assert_eq!(evaluate(&discarded(branch(column)),&Schema::default(),&[],&context).unwrap_err().code,"xtdb.expression/division-by-zero");
}

#[test]
fn nullable_parameter_boolean_controls_run_before_null_inspection() {
    for op in [B::And,B::Or] {
        let expr = Expr::unary(U::IsNull,Expr::binary(op,Expr::binary(B::Eq,divide(),int(0)),parameter(0)));
        let context = EvalContext { parameters: rtdb_expr::Parameters::Values(&[V::Null]),..Default::default() };
        assert_eq!(evaluate(&expr,&Schema::default(),&[],&context).unwrap_err().code,"xtdb.expression/division-by-zero");
        let context = EvalContext { parameters: rtdb_expr::Parameters::Values(&[V::Bool(true)]),..Default::default() };
        assert_eq!(evaluate(&expr,&Schema::default(),&[],&context).unwrap(),V::Bool(false));
    }
}

#[test]
fn nested_list_metadata_and_hidden_equality_use_runtime_parameters() {
    let context = EvalContext { parameters: rtdb_expr::Parameters::Values(&[V::Null]),..Default::default() };
    assert_eq!(expression_type(&Expr::List(vec![branch(parameter(0))]),&Schema::default(),&context).unwrap(),L::list(L::Scalar(S::I64).maybe()));
    assert_eq!(evaluate(&call("cardinality",vec![Expr::List(vec![branch(parameter(0))])]),&Schema::default(),&[],&context).unwrap(),V::I32(1));
    let context = EvalContext { parameters: rtdb_expr::Parameters::Values(&[V::Set(vec![]),V::Set(vec![])]),..Default::default() };
    let expr = Expr::Case { branches: vec![(Expr::Literal(V::Bool(true)),int(1))],otherwise: Box::new(Expr::binary(B::Eq,parameter(0),parameter(1))) };
    assert_eq!(evaluate(&expr,&Schema::default(),&[],&context).unwrap_err().code,"xtdb.expression/set-equality");
}

#[test]
fn declared_parameter_vectors_override_stale_annotations_and_selected_cells() {
    let types = [L::Scalar(S::I64).maybe()];
    let context = EvalContext { parameters: rtdb_expr::Parameters::Values(&[V::I64(7)]),parameter_types: Some(&types),..Default::default() };
    for annotation in [L::Nothing,L::Scalar(S::I64)] {
        let captured = Expr::Parameter { index: 0,data_type: annotation };
        assert_eq!(expression_type(&captured,&Schema::default(),&context).unwrap(),types[0]);
        assert_eq!(evaluate(&discarded(branch(captured)),&Schema::default(),&[],&context).unwrap_err().code,"xtdb.expression/division-by-zero");
    }
}

#[test]
fn malformed_declared_parameter_width_fails_before_payload_evaluation() {
    let context = EvalContext { parameters: rtdb_expr::Parameters::Values(&[V::I64(7)]),parameter_types: Some(&[]),..Default::default() };
    assert_eq!(evaluate(&divide(),&Schema::default(),&[],&context).unwrap_err().code,"rtdb.expr/parameter-schema");
}
