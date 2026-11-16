use rtdb_expr::{EvalContext,evaluate,scalar_call};
use rtdb_plan::{BinaryOp,Expr,Schema,function_type_with_args};
use rtdb_types::{IntervalUnit,LogicalType as L,ScalarType as S,Value as V};

fn lit(value: V) -> Expr { Expr::Literal(value) }
fn text(value: &str) -> Expr { lit(V::Utf8(value.into())) }
fn int(value: i64) -> Expr { lit(V::I64(value)) }
fn division() -> Expr { Expr::binary(BinaryOp::Divide,int(1),int(0)) }
fn call(name: &str,args: Vec<Expr>) -> Expr { Expr::Call { return_type: function_type_with_args(name,&args).unwrap(),name: name.into(),args } }
fn run(expression: &Expr) -> rtdb_types::Result<V> { evaluate(expression,&Schema::default(),&[],&EvalContext::default()) }
fn single(value: Expr,unit: &str,fraction: Expr) -> Expr { call("single_field_interval",vec![value,text(unit),int(2),fraction]) }

#[test]
fn constructor_integer_and_text_fields_reuse_physical_interval_values() {
    for (unit,expected) in [
        ("YEAR",V::IntervalYearMonth(24)),("MONTH",V::IntervalYearMonth(2)),
        ("DAY",V::IntervalMonthDayMicro { months: 0,days: 2,nanoseconds: 0 }),
        ("HOUR",V::IntervalMonthDayMicro { months: 0,days: 0,nanoseconds: 7_200_000_000_000 }),
        ("MINUTE",V::IntervalMonthDayMicro { months: 0,days: 0,nanoseconds: 120_000_000_000 }),
        ("SECOND",V::IntervalMonthDayMicro { months: 0,days: 0,nanoseconds: 2_000_000_000 }),
    ] {
        assert_eq!(run(&single(int(2),unit,int(6))).unwrap(),expected);
        assert_eq!(run(&single(text("2"),unit,int(6))).unwrap(),expected);
    }
    let second = single(text("-1.123456789"),"SECOND",int(2));
    assert_eq!(second.data_type(),L::Scalar(S::IntervalMonthDayMicro));
    assert_eq!(run(&second).unwrap(),V::IntervalMonthDayMicro { months: 0,days: 0,nanoseconds: -1_123_456_789 });
}

#[test]
fn constructor_metadata_is_validated_even_for_integer_input_and_before_payload() {
    for (args,code) in [
        (vec![division(),text("DAY"),int(0),int(6)],"xtdb.expression/invalid-interval-precision"),
        (vec![int(1),text("SECOND"),int(2),int(10)],"xtdb.expression/invalid-interval-fractional-precision"),
        (vec![int(1),text("day"),int(2),int(6)],"xtdb.expression/invalid-interval-units"),
        (vec![int(1),text("DAY"),division(),int(6)],"xtdb.expression/non-literal-interval-metadata"),
    ] { assert_eq!(function_type_with_args("single_field_interval",&args).unwrap_err().code,code); }
    assert_eq!(function_type_with_args("multi_field_interval",&[text("1-2"),text("MONTH"),int(0),text("YEAR"),int(6)]).unwrap_err().code,"xtdb.expression/invalid-interval-units");
    assert_eq!(function_type_with_args("multi_field_interval",&[text("1-2"),text("MONTH"),int(0),text("YEAR"),division()]).unwrap_err().code,"xtdb.expression/non-literal-interval-metadata");
    assert!(function_type_with_args("single_field_interval",&[lit(V::F64(1.0)),text("DAY"),int(2),int(6)]).is_err());
    assert!(function_type_with_args("multi_field_interval",&[int(1),text("DAY"),int(2),text("SECOND"),int(6)]).is_err());
}

#[test]
fn unused_fraction_payload_is_skipped_but_controls_and_polymorphic_boxes_are_eager() {
    assert_eq!(run(&single(int(2),"YEAR",division())).unwrap(),V::IntervalYearMonth(24));
    let branch = Expr::Case { branches: vec![(lit(V::Bool(true)),division())],otherwise: Box::new(int(2)) };
    assert_eq!(run(&single(int(2),"YEAR",branch)).unwrap(),V::IntervalYearMonth(24));
    let condition = Expr::binary(BinaryOp::Eq,division(),int(1));
    let branch = Expr::Case { branches: vec![(condition,int(1))],otherwise: Box::new(int(2)) };
    assert_eq!(run(&single(int(2),"YEAR",branch)).unwrap_err().code,"xtdb.expression/division-by-zero");
    let boxed = Expr::Case { branches: vec![(lit(V::Bool(true)),division())],otherwise: Box::new(lit(V::Null)) };
    assert_eq!(run(&single(int(2),"YEAR",boxed)).unwrap_err().code,"xtdb.expression/division-by-zero");
}

#[test]
fn strict_null_dispatch_skips_unconsumed_payloads_and_literal_metadata_checks() {
    assert_eq!(run(&single(division(),"DAY",lit(V::Null))).unwrap(),V::Null);
    let condition = Expr::binary(BinaryOp::Eq,division(),int(1));
    let first = Expr::Case { branches: vec![(condition,int(1))],otherwise: Box::new(int(2)) };
    assert_eq!(run(&single(first,"DAY",lit(V::Null))).unwrap_err().code,"xtdb.expression/division-by-zero");
    let args = vec![lit(V::Null),text("not-a-unit"),int(0),division()];
    assert_eq!(run(&call("single_field_interval",args)).unwrap(),V::Null);
    let args = vec![division(),text("not-a-unit"),int(0),lit(V::Null)];
    assert_eq!(run(&call("single_field_interval",args)).unwrap(),V::Null);
}

#[test]
fn multi_field_metadata_and_arithmetic_keep_reference_precision_and_overflow_order() {
    let hour = call("multi_field_interval",vec![text("1:02"),text("HOUR"),int(2),text("MINUTE"),int(9)]);
    assert_eq!(hour.data_type(),L::Scalar(S::Interval(IntervalUnit::MonthDayNano)));
    assert_eq!(run(&hour).unwrap(),V::IntervalMonthDayNano { months: 0,days: 0,nanoseconds: 3_720_000_000_000 });
    let year = call("multi-field-interval",vec![text("1-2suffix"),text("YEAR"),int(2),text("MONTH"),int(99)]);
    assert_eq!(run(&year).unwrap(),V::IntervalYearMonth(14));
    let negative_min = call("multi_field_interval",vec![text("-2562047:47:16.854775808"),text("HOUR"),int(8),text("SECOND"),int(9)]);
    assert_eq!(run(&negative_min).unwrap_err().code,"xtdb.expression/overflow-error");
    assert_eq!(run(&single(int(i64::from(i32::MAX)+1),"DAY",int(6))).unwrap_err().code,"xtdb.expression/overflow-error");
    assert_eq!(run(&single(text("1.5"),"DAY",int(6))).unwrap_err().code,"xtdb.expression/invalid-interval");
}

#[test]
fn direct_scalar_consumption_validates_arity_types_and_metadata() {
    assert_eq!(scalar_call("single_field_interval",&[V::I64(1),V::Utf8("DAY".into()),V::I64(0),V::I64(6)],&EvalContext::default()).unwrap_err().code,"xtdb.expression/invalid-interval-precision");
    assert!(scalar_call("multi_field_interval",&[],&EvalContext::default()).is_err());
    assert_eq!(scalar_call("single-field-interval",&[V::I64(1),V::Utf8("DAY".into()),V::I64(2),V::I64(99)],&EvalContext::default()).unwrap(),V::IntervalMonthDayMicro { months: 0,days: 1,nanoseconds: 0 });
}
