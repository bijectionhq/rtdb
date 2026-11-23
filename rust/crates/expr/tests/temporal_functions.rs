use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext, EvaluationScope, evaluate, evaluate_async, scalar_call};
use rtdb_plan::{Expr, Schema, function_type_with_args};
use rtdb_types::{CivilDate, DateUnit, IntervalUnit, LogicalType as L, ScalarType as S, TimeUnit as U, Timestamp, Value as V};

fn lit(value: V) -> Expr { Expr::Literal(value) }
fn text(value: &str) -> Expr { lit(V::Utf8(value.into())) }
fn call(name: &str,args: Vec<Expr>) -> Expr { Expr::Call { return_type: function_type_with_args(name,&args).unwrap(),name: name.into(),args } }
fn run(expression: &Expr) -> rtdb_types::Result<V> { evaluate(expression,&Schema::default(),&[],&EvalContext::default()) }
fn ts(value: i64,unit: U,zone: Option<&str>) -> V { V::Timestamp { value,unit,timezone: zone.map(str::to_owned) } }
fn date(year: i32,month: u32,day: u32) -> V { V::calendar_date(CivilDate::from_ymd(year,month,day).unwrap(),DateUnit::Day) }
fn local(year: i32,month: u32,day: u32,hour: i64,minute: i64) -> V { ts((CivilDate::from_ymd(year,month,day).unwrap().epoch_day()*86_400+hour*3600+minute*60)*1_000_000,U::Microsecond,None) }
fn interval(months: i32,days: i32,nanoseconds: i64) -> V { V::IntervalMonthDayMicro { months,days,nanoseconds } }
fn bins(from: i64,to: i64,stride: i64) -> Expr { call("range_bins",vec![lit(interval(0,0,stride*1000)),lit(ts(from,U::Microsecond,Some("UTC"))),lit(ts(to,U::Microsecond,Some("UTC")))]) }

#[test]
fn extract_uses_iso_weeks_source_zone_offsets_and_signed_interval_components() {
    for (field,expected) in [("YEAR",2021),("QUARTER",1),("MONTH",1),("DAY",1),("WEEK",53),("DOW",5),("ISODOW",5),("DOY",1)] {
        assert_eq!(run(&call("extract",vec![text(field),lit(date(2021,1,1))])).unwrap(),V::I32(expected));
    }
    for (field,expected) in [("TIMEZONE_HOUR",5),("TIMEZONE_MINUTE",45),("EPOCH",0),("HOUR",5),("MINUTE",45)] {
        assert_eq!(run(&call("extract",vec![text(field),lit(ts(0,U::Microsecond,Some("+05:45")))])).unwrap(),V::I32(expected));
    }
    for (field,expected) in [("YEAR",-1),("MONTH",-1),("DAY",-2),("HOUR",0),("MINUTE",0),("SECOND",-1)] {
        assert_eq!(run(&call("extract",vec![text(field),lit(interval(-13,-2,-1))])).unwrap(),V::I32(expected));
    }
    assert_eq!(run(&call("extract",vec![text("EPOCH"),lit(ts(i64::from(i32::MAX)+1,U::Second,Some("UTC")))])).unwrap_err().code,"xtdb.expression/overflow-error");
    assert!(function_type_with_args("extract",&[text("EPOCH"),lit(ts(0,U::Second,None))]).is_err());
    assert!(function_type_with_args("extract",&[text("year"),lit(date(2024,1,1))]).is_err());
    assert!(function_type_with_args("extract",&[Expr::Parameter { index: 0,data_type: L::Scalar(S::Utf8) },lit(date(2024,1,1))]).is_err());
}

#[test]
fn truncation_preserves_units_and_uses_toward_zero_year_and_interval_multiples() {
    assert_eq!(run(&call("date_trunc",vec![text("MILLENNIUM"),lit(date(-1234,7,8))])).unwrap(),date(-1000,1,1));
    assert_eq!(run(&call("date_trunc",vec![text("WEEK"),lit(date(2021,1,1))])).unwrap(),date(2020,12,28));
    assert_eq!(run(&call("date_trunc",vec![text("MONTH"),lit(V::DateMilli(86_400_000))])).unwrap(),V::DateMilli(0));
    assert_eq!(run(&call("extract",vec![text("DAY"),lit(V::DateMilli(86_400_000))])).unwrap(),V::I32(2));
    assert_eq!(run(&call("date_trunc",vec![text("HOUR"),lit(ts(-1,U::Nanosecond,None))])).unwrap(),ts(-3_600_000_000_000,U::Nanosecond,None));
    assert_eq!(run(&call("date_trunc",vec![text("MICROSECOND"),lit(ts(-1,U::Nanosecond,Some("UTC")))])).unwrap(),ts(-1000,U::Nanosecond,Some("UTC")));
    assert_eq!(run(&call("date_trunc",vec![text("YEAR"),lit(interval(-13,5,1))])).unwrap(),interval(-12,0,0));
    assert_eq!(run(&call("date_trunc",vec![text("WEEK"),lit(interval(13,-9,1))])).unwrap(),interval(13,-7,0));
    assert_eq!(run(&call("date_trunc",vec![text("SECOND"),lit(interval(13,-9,-1))])).unwrap(),interval(13,-9,0));
    let day = CivilDate::from_ymd(2024,10,27).unwrap().epoch_day()*86_400;
    for seconds in [day+1800,day+5400] {
        assert_eq!(run(&call("date_trunc",vec![text("HOUR"),lit(ts(seconds*1_000_000,U::Microsecond,Some("Europe/Paris")))])).unwrap(),ts((seconds-1800)*1_000_000,U::Microsecond,Some("Europe/Paris")));
    }
    assert_eq!(run(&call("date_trunc",vec![text("DAY"),lit(ts(0,U::Microsecond,Some("UTC"))),text("+05:45")])).unwrap(),ts(-20_700_000_000,U::Microsecond,Some("UTC")));
    assert!(function_type_with_args("date_trunc",&[text("DAY"),lit(ts(0,U::Microsecond,None)),text("UTC")]).is_err());
}

#[test]
fn age_preserves_period_between_adjustments_and_subday_remainders() {
    for (end,start,expected) in [
        (local(2022,5,2,1,0),local(2022,5,1,23,0),interval(0,0,7_200_000_000_000)),
        (local(2023,2,1,12,0),local(2023,1,2,0,0),interval(0,30,43_200_000_000_000)),
        (local(2023,2,28,23,0),local(2023,1,30,0,0),interval(0,29,82_800_000_000_000)),
        (local(2023,1,1,1,0),local(2023,1,2,0,0),interval(0,0,-82_800_000_000_000)),
        (date(2024,1,1),date(2023,1,1),interval(12,0,0)),
    ] { assert_eq!(run(&call("age",vec![lit(end),lit(start)])).unwrap(),expected); }
    assert_eq!(run(&call("age",vec![lit(ts(-1,U::Microsecond,None)),lit(ts(0,U::Microsecond,None))])).unwrap(),interval(0,0,-1000));
    assert_eq!(run(&call("age",vec![lit(ts(-1,U::Nanosecond,None)),lit(ts(0,U::Microsecond,None))])).unwrap(),V::IntervalMonthDayNano { months: 0,days: 0,nanoseconds: -1 });
    assert_eq!(run(&call("age",vec![lit(ts(-86_399_999_999_999,U::Nanosecond,None)),lit(ts(0,U::Nanosecond,None))])).unwrap(),V::IntervalMonthDayNano { months: 0,days: 0,nanoseconds: 1 });
    let context = EvalContext { timezone: "Europe/Paris",..Default::default() };
    let day = CivilDate::from_ymd(2024,3,31).unwrap().epoch_day()*86_400;
    assert_eq!(scalar_call("age",&[ts((day+3600)*1_000_000,U::Microsecond,Some("UTC")),ts(day*1_000_000,U::Microsecond,Some("UTC"))],&context).unwrap(),interval(0,0,7_200_000_000_000));
}

#[test]
fn clock_aliases_distinguish_utc_and_local_fields_and_bind_timestamp_metadata() {
    let context = EvalContext { current_time: Timestamp(-1).into(),timezone: "+05:45",..Default::default() };
    for name in ["current_date","local_date","localdate"] {
        assert_eq!(scalar_call(name,&[],&context).unwrap(),if name == "current_date" { date(1969,12,31) } else { date(1970,1,1) });
    }
    assert_eq!(scalar_call("current_time",&[],&context).unwrap(),V::Time { value: 86_399_999_999,unit: U::Microsecond,bit_width: 64 });
    assert_eq!(scalar_call("local-time",&[V::I64(0)],&context).unwrap(),V::Time { value: 20_699,unit: U::Second,bit_width: 32 });
    assert_eq!(scalar_call("current_timezone",&[],&context).unwrap(),V::Utf8("+05:45".into()));
    for (precision,unit,value) in [(-1,U::Second,-1),(1,U::Millisecond,0),(4,U::Microsecond,0),(6,U::Microsecond,-1),(9,U::Nanosecond,-1000),(10,U::Nanosecond,-1000)] {
        let expression = rtdb_plan::bind_clock_timezone(call("current_timestamp",vec![lit(V::I64(precision))]),Some("+05:45")).unwrap();
        assert_eq!(expression.data_type(),L::Scalar(S::Timestamp { unit,timezone: Some("+05:45".into()) }));
        assert_eq!(evaluate(&expression,&Schema::default(),&[],&context).unwrap(),ts(value,unit,Some("+05:45")));
    }
    let retained = rtdb_plan::bind_clock_timezone(call("now",vec![]),None).unwrap();
    assert_eq!(evaluate(&retained,&Schema::default(),&[],&context).unwrap(),ts(-1,U::Microsecond,Some("+05:45")));
    assert!(function_type_with_args("current_timestamp",&[Expr::Parameter { index: 0,data_type: L::Scalar(S::I64) }]).is_err());
    assert!(function_type_with_args("current_date",&[lit(V::I64(1))]).is_err());
}

#[test]
fn date_bin_keeps_macro_truncation_and_evaluates_bound_origin_before_source() {
    let stride = interval(0,0,900_000_000_000);
    for (source,result) in [(1_210_000_000,900_000_000),(-600_000_000,0)] {
        assert_eq!(run(&call("date_bin",vec![lit(stride.clone()),lit(ts(source,U::Microsecond,Some("UTC")))])).unwrap(),ts(result,U::Microsecond,Some("UTC")));
    }
    let source = Expr::binary(rtdb_plan::BinaryOp::Divide,lit(V::I64(1)),lit(V::I64(0)));
    assert_eq!(run(&call("date_bin",vec![lit(V::I64(1)),source,lit(V::Null)])).unwrap(),V::Null);
    let origin = Expr::binary(rtdb_plan::BinaryOp::Divide,lit(V::I64(1)),lit(V::I64(0)));
    assert_eq!(run(&call("date_bin",vec![lit(V::Null),lit(V::I64(3)),origin])).unwrap_err().code,"xtdb.expression/division-by-zero");
    assert!(function_type_with_args("date_bin",&[lit(V::IntervalYearMonth(1)),lit(ts(0,U::Microsecond,Some("UTC")))]).is_err());
}

#[test]
fn range_bins_keep_weighted_intersections_lazy_size_and_signed_reader_size() {
    let V::List(values) = run(&bins(13*60_000_000,38*60_000_000,15*60_000_000)).unwrap() else { panic!() };
    for (value,(from,to,weight)) in values.iter().zip([(0,15,0.08),(15,30,0.6),(30,45,0.32)]) {
        let V::Struct(fields) = value.as_ref() else { panic!() };
        assert_eq!(fields["_from"],ts(from*60_000_000,U::Microsecond,Some("UTC")));
        assert_eq!(fields["_to"],ts(to*60_000_000,U::Microsecond,Some("UTC")));
        assert_eq!(fields["_weight"],V::F64(weight));
    }
    assert_eq!(run(&call("cardinality",vec![bins(30,0,15)])).unwrap(),V::I32(-1));
    assert_eq!(run(&call("cardinality",vec![bins(0,i64::from(i32::MAX)+1,1)])).unwrap_err().code,"xtdb.expression/overflow-error");
    assert_eq!(run(&call("array_lower",vec![bins(0,i64::from(i32::MAX)+1,1),lit(V::I64(1))])).unwrap(),V::I32(1));
    assert_eq!(run(&bins(0,10,0)).unwrap_err().code,"xtdb.expression/division-by-zero");
    assert!(rtdb_plan::function_type("range_bins",&[L::Scalar(S::Interval(IntervalUnit::DayTime)),L::Scalar(S::DateDay),L::Scalar(S::DateDay)]).is_err());
    assert!(rtdb_plan::function_type("range_bins",&[interval(0,0,1000).logical_type(),ts(0,U::Nanosecond,Some("UTC")).logical_type(),ts(1000,U::Nanosecond,Some("UTC")).logical_type()]).is_err());
    let memory = MemoryBudget::new(8 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let expression = bins(0,i64::from(i32::MAX),1);
        assert_eq!(evaluate(&call("cardinality",vec![expression.clone()]),&Schema::default(),&[],&context).unwrap(),V::I32(i32::MAX));
        let V::Struct(cell) = evaluate(&call("nth",vec![expression,lit(V::I64(3))]),&Schema::default(),&[],&context).unwrap() else { panic!() };
        assert_eq!(cell["_from"],ts(3,U::Microsecond,Some("UTC")));
    }
    assert_eq!(memory.used_bytes(),0);
}

#[tokio::test]
async fn range_bin_table_views_reserve_one_generated_cell_and_materialization_is_cancelable() {
    let memory = MemoryBudget::new(1 << 30);
    {
        let scope = EvaluationScope::new(&memory).unwrap();let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let expression = bins(0,i64::from(i32::MAX),1);let schema = Schema::default();
        let view = rtdb_expr::evaluate_list_async(&expression,&schema,&[],&context).await.unwrap();
        assert_eq!(view.len(),i32::MAX as usize);
        let _cell = memory.reserve(view.cell_allocation_bound()).unwrap();
        assert!(matches!(view.get(0).unwrap().as_ref(),V::Struct(_)));
        assert!(view.retained_bytes().unwrap()<256);
        let expression = bins(0,100_000,1);
        let mut pending = Box::pin(evaluate_async(&expression,&schema,&[],&context));
        assert!(matches!(futures::poll!(&mut pending),std::task::Poll::Pending));
        drop(pending);
    }
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn date_trunc_reports_the_same_invalid_timezone_in_planning_and_evaluation() {
    let timezone = "NotRealRegion";
    let value = ts(0, U::Microsecond, Some("UTC"));
    let prepared = function_type_with_args("date_trunc", &[text("DAY"), lit(value.clone()), text(timezone)]).unwrap_err();
    let evaluated = scalar_call("date_trunc", &[V::Utf8("DAY".into()), value, V::Utf8(timezone.into())], &EvalContext::default()).unwrap_err();
    assert_eq!(prepared, evaluated);
    assert_eq!(prepared.code, "xtdb.expression/invalid-timezone");
    assert_eq!(prepared.message.as_deref(), Some("Unknown time-zone ID: NotRealRegion"));
    assert_eq!(prepared.context["timezone"], timezone);
}

#[test]
fn extract_rejections_preserve_field_and_source_type_data() {
    use rtdb_format::transit::{TransitValue as T, decode_anomaly, encode_anomaly, from_json_value};

    for (field, value, description, source, field_data) in [
        ("TIMEZONE_HOUR", ts(0, U::Microsecond, None), "timestamp without timezone", "timestamp-local", T::Keyword("timezone-hour".into())),
        ("TIMEZONE_HOUR", V::DateDay(0), "date", "date", T::String("TIMEZONE_HOUR".into())),
        ("YEAR", V::Time { value: 0, unit: U::Nanosecond, bit_width: 64 }, "time without timezone", "time-local", T::String("YEAR".into())),
        ("WEEK", interval(0, 3, 0), "interval", "interval", T::String("WEEK".into())),
    ] {
        let planned = function_type_with_args("extract", &[text(field), lit(value.clone())]).unwrap_err();
        let evaluated = scalar_call("extract", &[V::Utf8(field.into()), value], &EvalContext::default()).unwrap_err();
        assert_eq!(planned, evaluated);
        let error = decode_anomaly(&encode_anomaly(&planned).unwrap()).unwrap();
        assert_eq!(error.code, "xtdb.expression/extract-not-supported");
        assert_eq!(error.message.as_deref(), Some(format!("Extract \"{field}\" not supported for type {description}").as_str()));
        assert_eq!(from_json_value(&error.context["field"]).unwrap(), field_data);
        assert_eq!(from_json_value(&error.context["source-type"]).unwrap(), T::Keyword(source.into()));
    }
}

#[test]
fn direct_temporal_calls_validate_fields_and_map_delegated_checked_overflow() {
    for field in ["DOW","ISODOW","DOY","EPOCH","TIMEZONE_HOUR","TIMEZONE_MINUTE"] {
        assert_eq!(scalar_call("date_trunc",&[V::Utf8(field.into()),V::DateDay(0)],&EvalContext::default()).unwrap_err().code,"xtdb.expression/extract-not-supported");
    }
    assert_eq!(scalar_call("age",&[V::DateDay(i32::MAX),V::DateDay(0)],&EvalContext::default()).unwrap_err().code,"xtdb.expression/overflow-error");
    assert_eq!(scalar_call("date_bin",&[interval(0,0,1_000_000_000),V::DateDay(i32::MAX)],&EvalContext::default()).unwrap_err().code,"xtdb.expression/overflow-error");
}

#[tokio::test(start_paused = true)]
async fn temporal_strict_calls_discard_inline_null_payloads_but_force_control_and_boxing() {
    let divide = Expr::binary(rtdb_plan::BinaryOp::Divide,lit(V::I64(1)),lit(V::I64(0)));
    let source = Expr::Cast { expr: Box::new(divide.clone()),target: rtdb_plan::CastTarget::fixed(L::Scalar(S::Timestamp { unit: U::Microsecond,timezone: None })) };
    for arguments in [vec![source.clone(),lit(V::Null)],vec![lit(V::Null),source.clone()]] {
        assert_eq!(run(&call("age",arguments)).unwrap(),V::Null);
    }
    let condition = Expr::binary(rtdb_plan::BinaryOp::Eq,divide,lit(V::I64(0)));
    let forced_condition = Expr::Case { branches: vec![(condition,lit(ts(0,U::Microsecond,None)))],otherwise: Box::new(lit(ts(1,U::Microsecond,None))) };
    let forced_box = Expr::Case { branches: vec![(lit(V::Bool(true)),source)],otherwise: Box::new(lit(V::Null)) };
    for forced in [forced_condition,forced_box] {
        assert_eq!(run(&call("age",vec![forced,lit(V::Null)])).unwrap_err().code,"xtdb.expression/division-by-zero");
    }
    let start = tokio::time::Instant::now();
    let sleep = call("sleep",vec![lit(V::Duration { value: 1000,unit: U::Millisecond })]);
    for name in ["extract","date_trunc"] {
        let expression = call(name,vec![text("YEAR"),sleep.clone()]);
        assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&EvalContext::default()).await.unwrap(),V::Null);
        assert_eq!(tokio::time::Instant::now(),start);
    }
    let forced_sleep = Expr::Case { branches: vec![(lit(V::Bool(true)),sleep)],otherwise: Box::new(lit(ts(0,U::Microsecond,None))) };
    assert_eq!(evaluate_async(&call("extract",vec![text("YEAR"),forced_sleep]),&Schema::default(),&[],&EvalContext::default()).await.unwrap(),V::Null);
    assert_eq!(tokio::time::Instant::now()-start,std::time::Duration::from_secs(1));
}
