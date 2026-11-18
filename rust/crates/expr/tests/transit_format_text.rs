use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext,EvaluationScope,evaluate,evaluate_async,scalar_call};
use rtdb_format::transit::{TransitValue as T,encode_json,encode_msgpack};
use rtdb_plan::{Expr,Schema,function_type_with_args};
use rtdb_types::Value as V;

fn text(value: &str) -> V { V::Utf8(value.into()) }
fn tag(name: &str,representation: T) -> T { T::Tagged { tag: name.into(),representation: Box::new(representation) } }
fn info(message: T,data: T) -> T { tag("xtdb/exception-info",T::Array(vec![message,data])) }
fn stored(value: &T) -> V { V::Transit(encode_msgpack(value).unwrap()) }

#[test]
fn format_resolves_bounded_exception_info_messages_with_tracked_and_standalone_contexts() {
    let memory = MemoryBudget::new(8 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        for (message,expected) in [
            (T::Null,"clojure.lang.ExceptionInfo: null {}"),
            (T::String(String::new()),"clojure.lang.ExceptionInfo:  {}"),
            (T::String("quote ' \"\nλ😀".into()),"clojure.lang.ExceptionInfo: quote ' \"\nλ😀 {}"),
        ] {
            for data in [T::Null,T::Map(vec![])] {
                let args = [text("%s"),stored(&info(message.clone(),data))];
                for context in [EvalContext::default(),EvalContext { allocation: Some(&scope),..Default::default() }] {
                    assert_eq!(scalar_call("format",&args,&context).unwrap(),text(expected));
                }
            }
        }
        assert!(memory.used_bytes() > 4096);
    }
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn semantic_exception_info_arguments_keep_format_quoting_width_and_position() {
    let value = stored(&info(T::String("x".into()),T::Map(vec![])));
    let literal = stored(&info(T::String("O'Reilly".into()),T::Null));
    let identifier = stored(&info(T::String("said \"hi\"".into()),T::Map(vec![])));
    for (args,expected) in [
        (vec![text("%L"),literal],"'clojure.lang.ExceptionInfo: O''Reilly {}'"),
        (vec![text("%I"),identifier],"\"clojure.lang.ExceptionInfo: said \"\"hi\"\" {}\""),
        (vec![text("|%40s|"),value.clone()],"|        clojure.lang.ExceptionInfo: x {}|"),
        (vec![text("|%*s|"),V::I64(-36),value.clone()],"|clojure.lang.ExceptionInfo: x {}    |"),
        (vec![text("%2$s/%1$s"),value,text("tail")],"tail/clojure.lang.ExceptionInfo: x {}"),
    ] {
        assert_eq!(scalar_call("format",&args,&EvalContext::default()).unwrap(),text(expected));
    }
}

#[tokio::test]
async fn sync_and_async_expressions_reach_the_same_semantic_format_argument() {
    let args = vec![Expr::Literal(text("%s")),Expr::Literal(stored(&info(T::String("evaluated".into()),T::Null)))];
    let return_type = function_type_with_args("format",&args).unwrap();
    let expression = Expr::Call { name: "format".into(),args,return_type };
    let memory = MemoryBudget::new(8 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        for context in [EvalContext::default(),EvalContext { allocation: Some(&scope),..Default::default() }] {
            assert_eq!(evaluate(&expression,&Schema::default(),&[],&context).unwrap(),text("clojure.lang.ExceptionInfo: evaluated {}"));
            assert_eq!(evaluate_async(&expression,&Schema::default(),&[],&context).await.unwrap(),text("clojure.lang.ExceptionInfo: evaluated {}"));
        }
    }
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn format_keeps_first_argument_checks_and_resolves_unused_arguments_before_directives() {
    let invalid = V::Transit(vec![0xc1]);
    let error = scalar_call("format",&[V::Null,invalid.clone()],&EvalContext::default()).unwrap_err();
    assert_eq!(error.code,"xtdb.error/unknown");
    assert_eq!(error.message.as_deref(),Some("Parameter specified as non-null is null: method xtdb.expression.PgFormat.pgFormat, parameter formatStr"));
    let error = scalar_call("format",&[V::I64(1),invalid.clone()],&EvalContext::default()).unwrap_err();
    assert_eq!(error.code,"xtdb.expression/type-error");
    assert_eq!(error.message.as_deref(),Some("First argument to `format` must be a string"));
    let selected = stored(&info(T::String("first".into()),T::Null));
    for source in ["literal","%0$s"] {
        let error = scalar_call("format",&[text(source),selected.clone(),invalid.clone()],&EvalContext::default()).unwrap_err();
        assert_eq!(error.code,"rtdb.format/invalid-transit");
        assert_eq!(error.message.as_deref(),Some("MessagePack tag 0xc1 is not a Transit value"));
    }
    let json = V::Transit(encode_json(&info(T::Null,T::Null)).unwrap());
    let error = scalar_call("format",&[text("%s"),json],&EvalContext::default()).unwrap_err();
    assert_eq!(error.code,"rtdb.format/invalid-transit");
    assert_eq!(error.message.as_deref(),Some("Trailing bytes after Transit value"));
}

#[test]
fn unselected_registered_shapes_format_with_str_or_refuse_stack_trace_text() {
    let date = tag("m",T::String("0".into()));
    let nested = info(T::String("nested".into()),T::Map(vec![]));
    for (value,printable) in [(date,true),(info(T::Null,T::Map(vec![(T::String("nested".into()),nested)])),false)] {
        let args = [text("%s"),stored(&value)];
        let memory = MemoryBudget::new(8 << 20);
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            for context in [EvalContext::default(),EvalContext { allocation: Some(&scope),..Default::default() }] {
                match scalar_call("format",&args,&context) {
                    Ok(value) => assert!(printable && matches!(value,V::Utf8(_))),
                    Err(error) => assert!(!printable && error.code == "xtdb.expression/transit-throwable-text","{error:?}"),
                }
            }
        }
        assert_eq!(memory.used_bytes(),0);
    }
}

#[test]
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn legacy_date_cast_str_and_format_use_their_source_text_policies() {
    use rtdb_types::{LogicalType, ScalarType};

    let dates = [
        stored(&tag("m", T::Integer(-1))),
        stored(&tag("m", T::String("-0x1".into()))),
        stored(&tag("t", T::String("1969-12-31T23:59:59.999Z".into()))),
    ];
    let memory = MemoryBudget::new(8 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope), timezone: "Asia/Tokyo", ..Default::default() };
        let first = scalar_call("str", &dates[..1], &context).unwrap();
        let V::Utf8(readable) = &first else { panic!("expected readable Date text"); };
        assert!(readable.starts_with("#inst \""));
        assert!(readable.ends_with("-00:00\""));
        let plain = scalar_call("format", &[text("%s"), dates[0].clone()], &context).unwrap();
        let V::Utf8(display) = &plain else { panic!("expected scalar Date text"); };
        assert!(matches!(display.split_whitespace().nth(1), Some("Dec" | "Jan")));
        for date in dates {
            assert_eq!(rtdb_expr::cast(&date, &LogicalType::Scalar(ScalarType::Utf8), "Pacific/Honolulu").unwrap(), first);
            assert_eq!(scalar_call("str", std::slice::from_ref(&date), &context).unwrap(), first);
            assert_eq!(scalar_call("format", &[text("%s"), date], &context).unwrap(), plain);
        }
        assert!(memory.used_bytes() > 4096);
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn registered_temporal_strings_use_object_text_and_readable_tags() {
    let context = EvalContext::default();
    for (tag_name, source, scalar, readable) in [
        ("time/period", "P12M", "P12M", "period"),
        ("time/instant", "2024-01-02T03:04+02", "2024-01-02T01:04:00Z", "instant"),
        ("time/date-time", "2024-01-02T03:04:00.120000", "2024-01-02T03:04:00.120", "ldt"),
        ("time/offset-time", "08:09:00+02:00", "08:09+02:00", "offset-time"),
        ("time/offset-date-time", "2024-01-02T03:04:00+02", "2024-01-02T03:04+02:00", "odt"),
        ("time/zoned-date-time", "2024-01-02T03:04:00+01[Europe/Paris]", "2024-01-02T03:04+01:00[Europe/Paris]", "zdt"),
        ("time/year", "2024", "2024", "year"),
        ("time/year-month", "2024-02", "2024-02", "year-month"),
        ("time/month-day", "--02-29", "--02-29", "month-day"),
        ("xtdb/interval", "P1Y1M2DT3S", "P1Y1M2DT3S", "interval"),
    ] {
        let value = stored(&tag(tag_name, T::String(source.into())));
        assert_eq!(scalar_call("str", std::slice::from_ref(&value), &context).unwrap(), text(&format!("#xt/{readable} \"{scalar}\"")));
        assert_eq!(scalar_call("format", &[text("%s"), value], &context).unwrap(), text(scalar));
    }
    let wire_null = stored(&T::Null);
    let absent_interval = stored(&tag("xtdb/interval", T::String("unrecognized".into())));
    for value in [wire_null, absent_interval] {
        assert_eq!(scalar_call("str", std::slice::from_ref(&value), &context).unwrap(), text("nil"));
        assert_eq!(scalar_call("format", &[text("%s/%L"), value.clone(), value], &context).unwrap(), text("/NULL"));
    }
}

#[test]
fn registered_ranges_normalize_their_endpoints_before_formatting() {
    let context = EvalContext::default();
    let cases = [
        (T::Null, "ZonedDateTimeRange(from=null, to=null)", "#xt/tstz-range [nil nil]"),
        (
            T::Array(vec![tag("time/instant", T::String("2024-01-02T03:04+02".into()))]),
            "ZonedDateTimeRange(from=2024-01-02T01:04Z[UTC], to=null)",
            "#xt/tstz-range [#xt/zdt \"2024-01-02T01:04Z[UTC]\" nil]",
        ),
        (
            tag("list", T::Array(vec![tag("m", T::Integer(-1)), T::String("2024-01-02T03:04+01[Europe/Paris]".into())])),
            "ZonedDateTimeRange(from=1969-12-31T23:59:59.999Z[UTC], to=2024-01-02T03:04+01:00[Europe/Paris])",
            "#xt/tstz-range [#xt/zdt \"1969-12-31T23:59:59.999Z[UTC]\" #xt/zdt \"2024-01-02T03:04+01:00[Europe/Paris]\"]",
        ),
    ];
    for (representation, scalar, readable) in cases {
        let value = stored(&tag("xtdb/tstz-range", representation));
        assert_eq!(scalar_call("str", std::slice::from_ref(&value), &context).unwrap(), text(readable));
        assert_eq!(scalar_call("format", &[text("%s"), value], &context).unwrap(), text(scalar));
    }
    let unsupported_endpoint = tag("time/offset-date-time", T::String("2024-01-02T03:04Z".into()));
    let value = stored(&tag("xtdb/tstz-range", T::Array(vec![unsupported_endpoint])));
    assert!(scalar_call("str", std::slice::from_ref(&value), &context).is_err());
    assert!(scalar_call("format", &[text("%s"), value], &context).is_err());
}

#[test]
fn public_format_denies_constructor_charge_after_the_existing_transit_prefix() {
    let args = [text("%s"),stored(&info(T::String("message".into()),T::Map(vec![])))];
    let retained = args.iter().map(|value|value.retained_bytes().unwrap()).sum::<usize>();
    let V::Transit(bytes) = &args[1] else { panic!("Transit") };
    let generic = 16 * (256 + retained);
    let string = 16 * (1024 + retained);
    let raw = 64 * args[1].retained_bytes().unwrap() + 4096 + 16 * bytes.len() * bytes.len();
    let prefix = 4096 + generic + 256 + string + 256 + raw + 256;
    let memory = MemoryBudget::new(prefix);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        assert_eq!(scalar_call("format",&args,&context).unwrap_err().code,"rtdb/memory-budget-exceeded");
        assert_eq!(memory.used_bytes(),prefix);assert_eq!(memory.peak_bytes(),prefix);
    }
    assert_eq!(memory.used_bytes(),0);
    let memory = MemoryBudget::new(8 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        let output = scalar_call("format",&args,&context).unwrap();
        assert_eq!(output,text("clojure.lang.ExceptionInfo: message {}"));
        assert!(memory.used_bytes() > prefix);
        let retained = memory.used_bytes();drop(output);
        assert_eq!(memory.used_bytes(),retained);
    }
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn format_renders_stored_local_dates_and_times_as_scalars() {
    let memory = MemoryBudget::new(8 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        for context in [EvalContext::default(),EvalContext { allocation: Some(&scope),..Default::default() }] {
            for (tag_name,source,expected) in [
                ("time/date","0000-02-29","0000-02-29"),
                ("time/date","-00001-01-02","-0001-01-02"),
                ("time/date","+010000-12-31","+10000-12-31"),
                ("time/time","08:09:00.000000000","08:09"),
                ("time/time","08:09:10.","08:09:10"),
                ("time/time","08:09:10.12","08:09:10.120"),
                ("time/time","08:09:10.00012","08:09:10.000120"),
                ("time/time","08:09:10.000000001","08:09:10.000000001"),
            ] {
                let args = [text("%s"),stored(&tag(tag_name,T::String(source.into())))];
                assert_eq!(scalar_call("format",&args,&context).unwrap(),text(expected));
            }
            let date = stored(&tag("time/date",T::String("2024-02-29".into())));
            let time = stored(&tag("time/time",T::String("08:09:00".into())));
            let args = [text("%2$L/%1$I"),date,time];
            assert_eq!(scalar_call("format",&args,&context).unwrap(),text("'08:09'/\"2024-02-29\""));
        }
    }
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn invalid_stored_local_dates_and_times_fail_before_format_directives() {
    for (tag_name,source,code) in [
        ("time/date","2023-02-29","xtdb/invalid-date"),
        ("time/date","-0000-01-01","xtdb/invalid-date"),
        ("time/time","24:00","xtdb/invalid-time"),
        ("time/time","08:09:10.0000000001","xtdb/invalid-time"),
    ] {
        let args = [text("%0$s"),stored(&tag(tag_name,T::String(source.into())))];
        assert_eq!(scalar_call("format",&args,&EvalContext::default()).unwrap_err().code,code);
    }
}

#[test]
fn format_renders_stored_durations_and_enum_names_as_scalars() {
    let memory = MemoryBudget::new(8 << 20);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        for context in [EvalContext::default(), EvalContext { allocation: Some(&scope), ..Default::default() }] {
            for (tag_name, source, expected) in [
                ("time/duration", "P1DT2H", "PT26H"),
                ("time/duration", "P1Dt", "PT24H"),
                ("time/duration", "-PT-6H+3M", "PT5H57M"),
                ("time/duration", "PT-0,100000000S", "PT-0.1S"),
                ("time/duration", "PT1H-60M", "PT0S"),
                ("time/duration", "PT9223372036854775807.999999999S", "PT2562047788015215H30M7.999999999S"),
                ("time/duration", "PT-9223372036854775808S", "PT-2562047788015215H-30M-8S"),
                ("time/day-of-week", "MONDAY", "MONDAY"),
                ("time/day-of-week", "SUNDAY", "SUNDAY"),
                ("time/month", "JANUARY", "JANUARY"),
                ("time/month", "DECEMBER", "DECEMBER"),
            ] {
                let args = [text("%s"), stored(&tag(tag_name, T::String(source.into())))];
                assert_eq!(scalar_call("format", &args, &context).unwrap(), text(expected));
            }

            let duration = stored(&tag("time/duration", T::String("PT-0.10S".into())));
            let day = stored(&tag("time/day-of-week", T::String("MONDAY".into())));
            let month = stored(&tag("time/month", T::String("MARCH".into())));
            let args = [text("%3$L/%2$I/%1$s/%3$s"), duration, day, month];
            assert_eq!(scalar_call("format", &args, &context).unwrap(), text("'MARCH'/\"MONDAY\"/PT-0.1S/MARCH"));
        }
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn invalid_stored_durations_and_enum_names_fail_before_format_directives() {
    for (tag_name, source, code) in [
        ("time/duration", "P1DT", "xtdb.expression/invalid-temporal-string"),
        ("time/duration", "PT2562047788015216H", "xtdb.expression/invalid-temporal-string"),
        ("time/duration", "PT1M9223372036854775807S", "xtdb.expression/invalid-temporal-string"),
        ("time/duration", "PT-9223372036854775808.1S", "xtdb.expression/invalid-temporal-string"),
        ("time/duration", "-PT-9223372036854775808S", "xtdb.expression/invalid-temporal-string"),
        ("time/day-of-week", "monday", "xtdb.expression/invalid-transit-object"),
        ("time/day-of-week", "MONDAY ", "xtdb.expression/invalid-transit-object"),
        ("time/month", "January", "xtdb.expression/invalid-transit-object"),
        ("time/month", "MONDAY", "xtdb.expression/invalid-transit-object"),
    ] {
        let value = stored(&tag(tag_name, T::String(source.into())));
        for directive in ["literal", "%0$s"] {
            let args = [text(directive), value.clone()];
            assert_eq!(scalar_call("format", &args, &EvalContext::default()).unwrap_err().code, code);
        }
    }
}
