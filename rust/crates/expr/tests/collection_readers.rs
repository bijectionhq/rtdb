use rtdb_types::StructFields;
use std::time::Duration;
use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext,EvaluationScope,evaluate,evaluate_async,function_type};
use rtdb_plan::{BinaryOp,CastTarget,Expr,Schema,UnaryOp};
use rtdb_types::{CivilDate,DateUnit,LogicalType,ScalarType,TimeUnit,Value};

fn call(name: &str,args: Vec<Expr>) -> Expr {
    let return_type = function_type(name,&args.iter().map(Expr::data_type).collect::<Vec<_>>()).unwrap();
    Expr::Call { name: name.into(),args,return_type }
}
fn literal(value: Value) -> Expr { Expr::Literal(value) }
fn sleep(millis: i64) -> Expr { call("sleep",vec![literal(Value::Duration { value: millis,unit: TimeUnit::Millisecond })]) }
fn wide(unit: DateUnit) -> Expr { literal(Value::calendar_date(CivilDate::MAX,unit)) }
fn text(expr: Expr) -> Expr { Expr::Cast { expr: Box::new(expr),target: CastTarget::Fixed(LogicalType::Scalar(ScalarType::Utf8)) } }
fn field(expr: Expr,name: &str) -> Expr { let data_type = match expr.data_type() { LogicalType::Struct(fields) => fields[name].clone(),_ => panic!() };Expr::Field { expr: Box::new(expr),name: name.into(),data_type } }
fn nth(expr: Expr,index: i64) -> Expr { call("nth",vec![expr,literal(Value::I64(index))]) }
fn run(expr: &Expr) -> rtdb_types::Result<Value> { evaluate(expr,&Schema::default(),&[],&EvalContext::default()) }

#[test]
fn struct_fields_are_boxed_before_field_selection_and_outer_text_cast() {
    assert_eq!(run(&text(wide(DateUnit::Day))).unwrap(),Value::Utf8(CivilDate::MAX.to_string()));
    for selected in ["wide","other"] {
        let record = Expr::Struct(vec![("wide".into(),wide(DateUnit::Day)),("other".into(),literal(Value::I64(7)))]);
        assert_eq!(run(&text(field(record,selected))).unwrap_err().code,"xtdb.expression/overflow-error");
    }
}

#[test]
fn list_size_type_and_null_observers_do_not_read_cells() {
    let list = Expr::List(vec![wide(DateUnit::Day),sleep(-1)]);
    for (expr,expected) in [
        (call("cardinality",vec![list.clone()]),Value::I32(2)),
        (call("length",vec![list.clone()]),Value::I32(2)),
        (call("boolean",vec![list.clone()]),Value::Bool(true)),
        (call("nil?",vec![list.clone()]),Value::Bool(false)),
        (Expr::Unary { op: UnaryOp::IsNotNull,expr: Box::new(list.clone()) },Value::Bool(true)),
        (nth(list.clone(),-1),Value::Null),(nth(list,2),Value::Null),
    ] { assert_eq!(run(&expr).unwrap(),expected); }
}

#[test]
fn nth_boxes_only_its_selected_cell_and_nested_struct_fields() {
    let list = Expr::List(vec![literal(Value::I64(7)),wide(DateUnit::Day),Expr::Struct(vec![("d".into(),wide(DateUnit::Day))])]);
    assert_eq!(run(&nth(list.clone(),0)).unwrap(),Value::I64(7));
    assert!(run(&text(nth(list.clone(),1))).is_err());
    assert!(run(&nth(list,2)).is_err());
    let nested = Expr::List(vec![Expr::List(vec![wide(DateUnit::Day)])]);
    assert_eq!(run(&call("cardinality",vec![nth(nested,0)])).unwrap(),Value::I32(1));
}

#[test]
fn list_comparison_checks_sizes_then_short_circuits_definite_inequality() {
    let bad = || Expr::List(vec![wide(DateUnit::Day)]);
    assert_eq!(run(&Expr::binary(BinaryOp::Eq,bad(),Expr::List(vec![]))).unwrap(),Value::Bool(false));
    let left = Expr::List(vec![literal(Value::I64(1)),wide(DateUnit::Day)]);
    let right = Expr::List(vec![literal(Value::I64(2)),wide(DateUnit::Day)]);
    assert_eq!(run(&Expr::binary(BinaryOp::Eq,left.clone(),right.clone())).unwrap(),Value::Bool(false));
    assert_eq!(run(&call("===",vec![left,right])).unwrap(),Value::Bool(false));
    assert!(run(&Expr::binary(BinaryOp::Eq,bad(),bad())).is_err());
}

#[test]
fn list_cast_keeps_unread_cells_deferred() {
    let cast = Expr::Cast { expr: Box::new(Expr::List(vec![literal(Value::Utf8("7".into())),literal(Value::Utf8("bad".into()))])),target: CastTarget::List(Box::new(CastTarget::Fixed(LogicalType::Scalar(ScalarType::I64)))) };
    assert_eq!(run(&call("cardinality",vec![cast.clone()])).unwrap(),Value::I32(2));
    assert_eq!(run(&nth(cast.clone(),0)).unwrap(),Value::I64(7));
    assert!(run(&nth(cast,1)).is_err());
}

#[test]
fn list_union_cast_retains_unread_nested_lists_and_remaps_selected_scalars() {
    let integer = LogicalType::Scalar(ScalarType::I64);
    let date = LogicalType::Scalar(ScalarType::DateDay);
    let element = LogicalType::join_all(&[integer,LogicalType::list(date)]);
    let value = Expr::List(vec![literal(Value::I64(4)),Expr::List(vec![wide(DateUnit::Day)])]);
    let cast = Expr::Cast { expr: Box::new(value),target: CastTarget::List(Box::new(CastTarget::Fixed(element))) };
    assert_eq!(run(&nth(cast.clone(),0)).unwrap(),Value::I64(4));
    assert_eq!(run(&call("cardinality",vec![nth(cast,1)])).unwrap(),Value::I32(1));
}

#[test]
fn set_size_observation_preserves_distinct_forms_that_evaluate_to_equal_values() {
    let set = call("set",vec![literal(Value::I64(1)),Expr::binary(BinaryOp::Add,literal(Value::I64(0)),literal(Value::I64(1))),wide(DateUnit::Day)]);
    assert_eq!(run(&call("length",vec![set])).unwrap(),Value::I32(3));
}

#[test]
fn millisecond_date_boxes_preserve_full_days_until_vector_output() {
    let record = Expr::Struct(vec![("date".into(),wide(DateUnit::Millisecond))]);
    assert_eq!(run(&text(field(record.clone(),"date"))).unwrap(),Value::Utf8(CivilDate::MAX.to_string()));
    assert_eq!(run(&text(nth(Expr::List(vec![wide(DateUnit::Millisecond)]),0))).unwrap(),Value::Utf8(CivilDate::MAX.to_string()));
    assert_eq!(run(&record).unwrap(),Value::Struct(StructFields::from([("date".into(),Value::DateMilli(CivilDate::MAX.epoch_day().wrapping_mul(86_400_000)))])));
}

#[test]
fn struct_identity_cast_and_patch_keep_nested_lists_unread() {
    let record = Expr::Struct(vec![("list".into(),Expr::List(vec![wide(DateUnit::Day)]))]);
    let target = CastTarget::Fixed(record.data_type());
    let cast = Expr::Cast { expr: Box::new(record.clone()),target };
    assert_eq!(run(&call("cardinality",vec![field(cast,"list")])).unwrap(),Value::I32(1));
    let patch = call("_patch",vec![record,Expr::Struct(vec![("other".into(),literal(Value::I64(5)))])]);
    assert_eq!(run(&call("cardinality",vec![field(patch,"list")])).unwrap(),Value::I32(1));
}

#[tokio::test(start_paused = true)]
async fn list_nth_repeats_suspension_after_comparison_without_memoizing_cells() {
    let list = Expr::List(vec![sleep(5),literal(Value::I64(1))]);
    let other = Expr::List(vec![literal(Value::Null),literal(Value::I64(2))]);
    let expression = call("nullif",vec![list,other]);
    let start = tokio::time::Instant::now();
    assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&EvalContext::default()).await.unwrap(),Value::list(vec![Value::Null,Value::I64(1)]));
    assert_eq!(tokio::time::Instant::now() - start,Duration::from_millis(10));
}

#[tokio::test(start_paused = true)]
async fn struct_scalar_cells_are_eager_and_not_reevaluated_after_comparison() {
    let record = Expr::Struct(vec![("sleep".into(),sleep(5)),("value".into(),literal(Value::I64(1)))]);
    let other = Expr::Struct(vec![("sleep".into(),literal(Value::Null)),("value".into(),literal(Value::I64(2)))]);
    let expression = call("nullif",vec![record,other]);
    let start = tokio::time::Instant::now();
    assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&EvalContext::default()).await.unwrap(),Value::Struct(StructFields::from([("sleep".into(),Value::Null),("value".into(),Value::I64(1))])));
    assert_eq!(tokio::time::Instant::now() - start,Duration::from_millis(5));
}

#[tokio::test(start_paused = true)]
async fn async_size_and_unselected_cells_never_start_sleep() {
    let list = Expr::List(vec![sleep(i64::MAX),literal(Value::I64(4))]);
    let start = tokio::time::Instant::now();
    assert_eq!(evaluate_async(&call("cardinality",vec![list.clone()]),&Schema::default(),&[],&EvalContext::default()).await.unwrap(),Value::I32(2));
    assert_eq!(evaluate_async(&nth(list,1),&Schema::default(),&[],&EvalContext::default()).await.unwrap(),Value::I64(4));
    assert_eq!(tokio::time::Instant::now(),start);
}

#[tokio::test(start_paused = true)]
async fn cancelled_selected_list_cell_releases_the_invocation_scope() {
    let memory = MemoryBudget::new(1 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let expr = nth(Expr::List(vec![sleep(i64::MAX)]),0);
        assert!(tokio::time::timeout(Duration::from_millis(5),evaluate_async(&expr,&Schema::default(),&[],&context)).await.is_err());
        assert!(memory.used_bytes() > 0);
    }
    assert_eq!(memory.used_bytes(),0);
}
