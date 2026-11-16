use rtdb_expr::{EvalContext, arithmetic, evaluate, evaluate_async, scalar_call};
use rtdb_plan::{BinaryOp as B, Expr, Schema, function_type};
use rtdb_types::Value as V;

#[test]
fn bitwise_calls_and_operators_keep_widths_and_narrow_shift_counts_before_masking() {
    let context = EvalContext::default();
    for (name,args,expected) in [
        ("bit_not",vec![V::I8(0)],V::I8(-1)),
        ("BIT-NOT",vec![V::I16(i16::MIN)],V::I16(i16::MAX)),
        ("bit_and",vec![V::I8(-1),V::I32(258)],V::I32(258)),
        ("bit_or",vec![V::I8(1),V::I16(256)],V::I16(257)),
        ("bit_xor",vec![V::I8(-1),V::I64(1)],V::I64(-2)),
        ("bit_shift_left",vec![V::I8(127),V::I64(1)],V::I8(-2)),
        ("bit_shift_right",vec![V::I16(-4),V::I8(1)],V::I16(-2)),
        ("bit_shift_left",vec![V::I32(3),V::I64(64)],V::I32(3)),
        ("bit_shift_left",vec![V::I64(1),V::I64(-1)],V::I64(i64::MIN)),
        ("bit_shift_left",vec![V::I64(3),V::I64(i64::MAX)],V::I64(i64::MIN)),
        ("bit_and",vec![V::Null,V::Bool(true)],V::Null),
    ] { assert_eq!(scalar_call(name,&args,&context).unwrap(),expected,"{name}"); }
    for op in [B::ShiftLeft,B::ShiftRight] {
        for count in [i64::from(i32::MAX) + 1,i64::from(i32::MIN) - 1] {
            for left in [V::I8(1),V::I16(1),V::I32(1)] {
                assert_eq!(arithmetic(op,&left,&V::I64(count)).unwrap_err().code,"xtdb.expression/overflow-error");
            }
        }
    }
    assert!(arithmetic(B::BitAnd,&V::U8(1),&V::I64(1)).is_err());
}

#[tokio::test]
async fn bitwise_continuations_discard_null_payloads_and_keep_polymorphic_boxing() {
    let divide = Expr::binary(B::Divide,Expr::Literal(V::I64(1)),Expr::Literal(V::I64(0)));
    let call = |left: Expr| {
        let args = vec![left,Expr::Literal(V::Null)];
        Expr::Call { name: "bit-and".into(),return_type: function_type("bit-and",&args.iter().map(Expr::data_type).collect::<Vec<_>>()).unwrap(),args }
    };
    for expression in [call(divide.clone()),Expr::binary(B::BitAnd,divide.clone(),Expr::Literal(V::Null))] {
        assert_eq!(evaluate(&expression,&Schema::default(),&[],&EvalContext::default()).unwrap(),V::Null);
        assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&EvalContext::default()).await.unwrap(),V::Null);
    }
    let expression = call(Expr::Case { branches: vec![(Expr::Literal(V::Bool(true)),divide)],otherwise: Box::new(Expr::Literal(V::Null)) });
    assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&EvalContext::default()).await.unwrap_err().code,"xtdb.expression/division-by-zero");
}
