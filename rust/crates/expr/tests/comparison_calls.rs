use rtdb_expr::{EvalContext,evaluate,evaluate_async,scalar_call};
use rtdb_plan::{BinaryOp as B,Expr,Schema,function_type};
use rtdb_types::{LogicalType as L,ScalarType as S,TimeUnit as U,Value as V};

fn call(name: &str,args: Vec<Expr>) -> Expr {
    Expr::Call { name: name.into(),return_type: function_type(name,&args.iter().map(Expr::data_type).collect::<Vec<_>>()).unwrap(),args }
}

#[test]
fn comparison_calls_keep_byte_pair_magnitudes_and_numeric_ordering() {
    for (left,right,expected) in [
        (V::Utf8("ab".into()),V::Utf8("cb".into()),-512),
        (V::Utf8("ab".into()),V::Utf8("ad".into()),-2),
        (V::Utf8("abc".into()),V::Utf8("abe".into()),-2),
        (V::Utf8("a".into()),V::Utf8("abc".into()),-2),
        (V::Binary(vec![0,255]),V::FixedSizeBinary(vec![1,0]),-1),
        (V::FixedSizeBinary(vec![255]),V::Binary(vec![0]),255),
        (V::I64(9_007_199_254_740_993),V::I64(9_007_199_254_740_992),1),
        (V::I64(9_007_199_254_740_993),V::F64(9_007_199_254_740_992.0),0),
        (V::F64(-0.0),V::F64(0.0),-1),
        (V::F64(f64::NAN),V::F64(f64::INFINITY),1),
        (V::F64(f64::NAN),V::F64(f64::from_bits(0xfff8_0000_0000_0001)),0),
        (V::Bool(false),V::Bool(true),-1),
        (V::Timestamp { value: 1,unit: U::Second,timezone: Some("UTC".into()) },V::Timestamp { value: 1000,unit: U::Millisecond,timezone: Some("UTC".into()) },0),
        (V::DateMilli(86_400_001),V::DateDay(1),0),
    ] {
        for name in ["compare","compare_nulls_first","compare-nulls-last"] {
            assert_eq!(scalar_call(name,&[left.clone(),right.clone()],&EvalContext::default()).unwrap(),V::I32(expected),"{name}: {left:?}, {right:?}");
        }
    }
    assert!(scalar_call("compare",&[V::Uri("a".into()),V::Uri("b".into())],&EvalContext::default()).is_err());
    for types in [vec![L::Scalar(S::Utf8),L::Scalar(S::Binary)],vec![L::list(L::Null),L::list(L::Null)],vec![L::Scalar(S::F16),L::Scalar(S::I64)],vec![L::Null]] {
        assert!(function_type("compare",&types).is_err());
    }
    assert_eq!(function_type("compare",&[L::Scalar(S::I64).maybe(),L::Scalar(S::I32)]).unwrap(),L::Scalar(S::I32).maybe());
    assert_eq!(function_type("compare_nulls_first",&[L::Scalar(S::I64).maybe(),L::Scalar(S::I32)]).unwrap(),L::Scalar(S::I32));
}

#[tokio::test]
async fn comparison_nulls_discard_payloads_after_controls_and_polymorphic_boxes() {
    let divide = Expr::binary(B::Divide,Expr::Literal(V::I64(1)),Expr::Literal(V::I64(0)));
    for (name,left_null,right_null,expected) in [
        ("compare",false,true,V::Null),("compare",true,false,V::Null),
        ("compare_nulls_first",true,true,V::I32(0)),("compare_nulls_first",true,false,V::I32(-1)),("compare_nulls_first",false,true,V::I32(1)),
        ("compare_nulls_last",true,true,V::I32(0)),("compare_nulls_last",true,false,V::I32(1)),("compare_nulls_last",false,true,V::I32(-1)),
    ] {
        let expression = call(name,vec![if left_null { Expr::Literal(V::Null) } else { divide.clone() },if right_null { Expr::Literal(V::Null) } else { divide.clone() }]);
        assert_eq!(evaluate(&expression,&Schema::default(),&[],&EvalContext::default()).unwrap(),expected);
        assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&EvalContext::default()).await.unwrap(),expected);
    }
    let boxed = Expr::Case { branches: vec![(Expr::Literal(V::Bool(true)),divide.clone())],otherwise: Box::new(Expr::Literal(V::Null)) };
    let controlled = Expr::Case { branches: vec![(Expr::binary(B::Eq,divide,Expr::Literal(V::I64(0))),Expr::Literal(V::I64(1)))],otherwise: Box::new(Expr::Literal(V::I64(2))) };
    for input in [boxed,controlled] {
        let expression = call("compare_nulls_first",vec![input,Expr::Literal(V::Null)]);
        assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&EvalContext::default()).await.unwrap_err().code,"xtdb.expression/division-by-zero");
    }
}
