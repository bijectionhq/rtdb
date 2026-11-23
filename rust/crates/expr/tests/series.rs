use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext, EvaluationScope, evaluate, evaluate_async, function_type, scalar_call};
use rtdb_plan::{Expr, Schema};
use rtdb_types::{CivilDate, DateUnit, IntervalUnit, LogicalType as L, ScalarType as S, TimeUnit as U, Value as V};

fn lit(value: V) -> Expr { Expr::Literal(value) }
fn int(value: i64) -> Expr { lit(V::I64(value)) }
fn call(name: &str,args: Vec<Expr>) -> Expr {
    let return_type = function_type(name,&args.iter().map(Expr::data_type).collect::<Vec<_>>()).unwrap();
    Expr::Call { name: name.into(),args,return_type }
}
fn ints(start: i64,end: i64,step: i64,inclusive: bool) -> Expr { call("generate_series",vec![int(start),int(end),int(step),lit(V::Bool(inclusive))]) }
fn run(expr: &Expr) -> rtdb_types::Result<V> { evaluate(expr,&Schema::default(),&[],&EvalContext::default()) }
fn list(values: &[i64]) -> V { V::list(values.iter().map(|value|V::I64(*value)).collect()) }
fn day(year: i32,month: u32,day: u32) -> V { V::calendar_date(CivilDate::from_ymd(year,month,day).unwrap(),DateUnit::Day) }
fn timestamp(second: i64,unit: U,zone: Option<&str>) -> V { V::Timestamp { value: second * unit.units_per_second(),unit,timezone: zone.map(str::to_owned) } }

#[test]
fn signed_integer_series_keep_default_exclusion_negative_steps_and_wrapped_endpoints() {
    assert_eq!(run(&call("generate-series",vec![int(1),int(5),int(2)])).unwrap(),list(&[1,3]));
    assert_eq!(run(&ints(1,5,2,true)).unwrap(),list(&[1,3,5]));
    assert_eq!(run(&ints(5,1,-2,false)).unwrap(),list(&[5,3]));
    assert_eq!(run(&ints(5,1,-2,true)).unwrap(),list(&[5,3,1]));
    assert_eq!(run(&ints(5,1,2,true)).unwrap(),list(&[]));
    assert_eq!(run(&ints(0,i64::MIN,-1,false)).unwrap(),list(&[]));
    assert_eq!(run(&ints(i64::MAX,i64::MAX,1,true)).unwrap_err().code,"xtdb.expression/overflow-error");
    assert_eq!(run(&ints(0,1,0,false)).unwrap_err().code,"xtdb.expression/division-by-zero");
    assert_eq!(run(&ints(i64::MIN,i64::MAX,1,false)).unwrap_err().code,"xtdb.expression/overflow-error");
    assert_eq!(run(&ints(0,2_147_483_648,1,false)).unwrap_err().code,"xtdb.expression/overflow-error");
}

#[test]
fn integer_series_size_and_nth_are_lazy_and_trim_extension_delegates_to_the_view() {
    let memory = MemoryBudget::new(8 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let series = ints(0,i64::from(i32::MAX),1,false);
        assert_eq!(evaluate(&call("array_length",vec![series.clone(),int(1)]),&Schema::default(),&[],&context).unwrap(),V::I32(i32::MAX));
        assert_eq!(evaluate(&call("nth",vec![series,int(3)]),&Schema::default(),&[],&context).unwrap(),V::I64(3));
        let extended = call("trim_array",vec![ints(0,2,1,false),int(-2)]);
        assert_eq!(evaluate(&call("nth",vec![extended,int(3)]),&Schema::default(),&[],&context).unwrap(),V::I64(3));
    }
    assert_eq!(memory.used_bytes(),0);
    assert_eq!(run(&call("array_lower",vec![ints(0,1,0,false),int(1)])).unwrap(),V::I32(1));
}

#[test]
fn calendar_series_clamp_each_month_and_preserve_temporal_output_units() {
    let values = [day(2024,1,31),day(2024,5,1),V::IntervalYearMonth(1)];
    assert_eq!(scalar_call("generate_series",&values,&EvalContext::default()).unwrap(),V::list(vec![day(2024,1,31),day(2024,2,29),day(2024,3,29),day(2024,4,29)]));
    let values = [day(1970,1,1),day(1970,1,3),V::IntervalMonthDayMicro { months: 0,days: 1,nanoseconds: 0 },V::Bool(true)];
    assert_eq!(scalar_call("generate_series",&values,&EvalContext::default()).unwrap(),V::list(vec![timestamp(0,U::Microsecond,None),timestamp(86_400,U::Microsecond,None),timestamp(172_800,U::Microsecond,None)]));
    let values = [V::Timestamp { value: -1,unit: U::Microsecond,timezone: None },V::Timestamp { value: 1,unit: U::Microsecond,timezone: None },V::IntervalMonthDayNano { months: 0,days: 0,nanoseconds: 500 }];
    let expected = [-1000,-500,0,500].into_iter().map(|value|V::Timestamp { value,unit: U::Nanosecond,timezone: None }).collect();
    assert_eq!(scalar_call("generate_series",&values,&EvalContext::default()).unwrap(),V::list(expected));
}

#[test]
fn date_series_cells_wrap_at_the_day_box_width_while_iteration_keeps_wide_calendar_days() {
    let start = CivilDate::from_epoch_day(i64::from(i32::MAX)).unwrap();
    let end = start.plus_months(1).unwrap();
    let expression = call("generate_series",vec![lit(V::DateDay(i32::MAX)),lit(V::calendar_date(end,DateUnit::Day)),lit(V::IntervalYearMonth(1)),lit(V::Bool(true))]);
    assert_eq!(run(&expression).unwrap(),V::list(vec![V::DateDay(i32::MAX),V::DateDay(end.epoch_day() as i32)]));
}

#[test]
fn zoned_calendar_days_cross_daylight_changes_and_differing_zones_select_utc() {
    let noon = CivilDate::from_ymd(2024,3,30).unwrap().epoch_day()*86_400+11*3600;
    let end = CivilDate::from_ymd(2024,4,1).unwrap().epoch_day()*86_400+10*3600;
    let stride = V::IntervalMonthDayMicro { months: 0,days: 1,nanoseconds: 0 };
    let values = [timestamp(noon,U::Second,Some("Europe/Paris")),timestamp(end,U::Second,Some("Europe/Paris")),stride.clone(),V::Bool(true)];
    assert_eq!(scalar_call("generate_series",&values,&EvalContext::default()).unwrap(),V::list(vec![timestamp(noon,U::Microsecond,Some("Europe/Paris")),timestamp(noon+23*3600,U::Microsecond,Some("Europe/Paris")),timestamp(end,U::Microsecond,Some("Europe/Paris"))]));
    let values = [timestamp(noon,U::Second,Some("Europe/Paris")),timestamp(end,U::Second,Some("UTC")),stride,V::Bool(true)];
    assert_eq!(scalar_call("generate_series",&values,&EvalContext::default()).unwrap(),V::list(vec![timestamp(noon,U::Microsecond,Some("UTC")),timestamp(noon+24*3600,U::Microsecond,Some("UTC"))]));
}

#[test]
fn series_type_admission_matches_integer_date_and_timestamp_families() {
    for ty in [S::I8,S::I16,S::I32,S::I64] {
        assert_eq!(function_type("generate_series",&[L::Scalar(ty.clone()),L::Scalar(ty.clone()),L::Scalar(ty)]).unwrap(),L::list(L::Scalar(S::I64)));
    }
    for ty in [S::U8,S::U64,S::F64,S::Utf8] {
        assert!(function_type("generate_series",&[L::Scalar(ty.clone()),L::Scalar(ty.clone()),L::Scalar(ty)]).is_err());
    }
    assert!(function_type("generate_series",&vec![L::Scalar(S::I64);2]).is_err());
    assert!(function_type("generate_series",&[L::Scalar(S::DateMilli),L::Scalar(S::DateDay),L::Scalar(S::Interval(IntervalUnit::YearMonth))]).is_err());
    for (start,end,message) in [(S::DateMilli,S::DateMilli,"generate_series with date-from of type date-milli"),(S::DateDay,S::DateMilli,"generate_series with date-to of type date-milli")] {
        assert_eq!(function_type("generate_series",&[L::Scalar(start),L::Scalar(end),L::Scalar(S::Interval(IntervalUnit::YearMonth))]).unwrap_err().message.as_deref(),Some(message));
    }
    let division = Expr::binary(rtdb_plan::BinaryOp::Divide,int(1),int(0));
    assert_eq!(run(&call("generate_series",vec![lit(V::Null),division,int(1)])).unwrap(),V::Null);
}

#[tokio::test]
async fn eager_temporal_series_yield_and_release_allocations_when_the_caller_cancels() {
    let memory = MemoryBudget::new(8 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let expression = call("generate_series",vec![lit(day(2024,1,1)),lit(day(2025,1,1)),lit(V::IntervalYearMonth(0))]);
        let schema = Schema::default();
        let mut evaluation = Box::pin(evaluate_async(&expression,&schema,&[],&context));
        assert!(matches!(futures::poll!(&mut evaluation),std::task::Poll::Pending));
        assert!(memory.used_bytes()>0);
        drop(evaluation);
    }
    assert_eq!(memory.used_bytes(),0);
    let memory = MemoryBudget::new(1 << 20);
    let scope = EvaluationScope::new(&memory).unwrap();
    let context = EvalContext { allocation: Some(&scope),..Default::default() };
    assert!(scalar_call("generate_series",&[day(2024,1,1),day(2025,1,1),V::IntervalYearMonth(0)],&context).is_err());
}

#[tokio::test]
async fn table_list_results_leave_maps_empty_and_keep_integer_views_owned() {
    let memory = MemoryBudget::new(8 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let parameters = [V::Map { entries: vec![(V::Utf8("key".into()),V::I64(1))], sorted: false }];
        let parameter = Expr::Parameter { index: 0,data_type: parameters[0].logical_type() };
        let context = EvalContext { parameters: &parameters,allocation: Some(&scope),..Default::default() };
        let schema = Schema::default();
        assert!(rtdb_expr::evaluate_list_async(&parameter,&schema,&[],&context).await.unwrap().is_empty());
        let series = ints(0,i64::from(i32::MAX),1,false);
        let result = rtdb_expr::evaluate_list_async(&series,&schema,&[],&context).await.unwrap();
        assert_eq!(result.len(),i32::MAX as usize);
        assert_eq!(result.get(3).unwrap().as_ref(),&V::I64(3));
        assert!(result.get(i32::MAX as usize).is_none());
        assert!(result.retained_bytes().unwrap() < 256);
    }
    assert_eq!(memory.used_bytes(),0);
}
