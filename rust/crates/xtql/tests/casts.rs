use rtdb_types::StructFields;
use std::collections::BTreeSet;
use rtdb_expr::{EvalContext, evaluate};
use rtdb_plan::{Catalog, QualifiedTable, RelationNode, Statement, TableBinding};
use rtdb_types::{DecimalValue, LogicalType as T, Result, ScalarType as S, Value as V};
use rtdb_xtql::{CompileOptions, compile};

struct EmptyCatalog;
impl Catalog for EmptyCatalog {
    fn default_database(&self) -> &str { "xtdb" }
    fn databases(&self) -> BTreeSet<String> { BTreeSet::from(["xtdb".into()]) }
    fn table(&self, _: &QualifiedTable) -> Result<Option<TableBinding>> { Ok(None) }
    fn tables(&self, _: &str) -> Result<Vec<TableBinding>> { Ok(Vec::new()) }
}

fn project(expression: &str) -> (T, V) {
    let source = format!("(-> (rel [{{}}] []) (return {{:v {expression}}}))");
    let Statement::Query(relation) = compile(&source, &EmptyCatalog, &CompileOptions::default()).unwrap().statement else { panic!("Expected query") };
    let RelationNode::Project { input, expressions } = relation.node() else { panic!("Expected projection") };
    let expression = &expressions[0].expr;
    (expression.data_type(), evaluate(expression, input.schema(), &[], &EvalContext::default()).unwrap())
}

#[test]
fn tagged_timestamp_readers_use_sql_fields_and_full_microsecond_calendar_range() {
    use rtdb_types::{CivilDate,TimeUnit as U};
    let date = CivilDate::from_ymd(280_000,7,1).unwrap();
    let micros = date.epoch_day() * 86_400_000_000;
    assert_eq!(project("#xt/ldt \"280000-07-01T00:00\"").1,V::Timestamp { value: micros,unit: U::Microsecond,timezone: None });
    assert_eq!(project("#xt/zdt \"280000-07-01T00:00[Europe/Paris]\"").1,V::Timestamp { value: micros - 7_200_000_000,unit: U::Microsecond,timezone: Some("Europe/Paris".into()) });
    assert_eq!(project("#xt/odt \"280000-07-01T00:00[Europe/Paris]\"").1,V::Timestamp { value: micros - 7_200_000_000,unit: U::Microsecond,timezone: Some("+02:00".into()) });
    assert_eq!(project("#xt/instant \"280000-07-01T00:00[Europe/Paris]\"").1,V::Timestamp { value: micros - 7_200_000_000,unit: U::Microsecond,timezone: Some("UTC".into()) });
    assert_eq!(project("#xt/ldt \"2021-03-28T02:30[Europe/Paris]\"").1,project("#xt/ldt \"2021-03-28T02:30\"").1);
    assert_eq!(project("#xt/ldt \"2020-02-31T24:00\"").1,project("#xt/ldt \"2020-03-01T00:00\"").1);
    assert_eq!(project("#xt/time \"12:34\"").1,V::Time { value: 45_240_000_000_000,unit: U::Nanosecond,bit_width: 64 });
    for literal in ["#xt/ldt \"2020-01-01\"","#xt/instant \"2020-01-01T12:30\"","#xt/instant \"300000-01-01Z\""] {
        assert!(compile(&format!("(-> (rel [{{}}] []) (return {{:v {literal}}}))"),&EmptyCatalog,&CompileOptions::default()).is_err(),"{literal}");
    }
}

#[test]
fn legacy_inst_reader_keeps_partial_fields_millisecond_truncation_and_calendar_cutover() {
    for (literal,normalized) in [
        ("1970","1970-01-01T00:00:00.000Z"), ("1970-01-01T00","1970-01-01T00:00:00.000Z"),
        ("1970-01-01T00:00:00.123456789123Z","1970-01-01T00:00:00.123Z"),
        ("1970-01-01T00:59:60Z","1970-01-01T01:00:00Z"),
        ("1970-01:00","1970-01-01T01:00:00Z"),
        ("1582-10-05","1582-10-15T00:00:00Z"),
    ] { assert_eq!(project(&format!("#inst {literal:?}")).1,project(&format!("#xt/instant {normalized:?}")).1); }
}

#[test]
fn temporal_and_qualified_interval_options_survive_recursive_xtql_targets() {
    use rtdb_types::TimeUnit as U;
    assert_eq!(project("(cast [\"05:34:56.123456789\" nil] #xt/type [:list [:time-local :nano]] {:precision 7})").1,
        V::list(vec![V::Time { value: 20_096_123_456_700, unit: U::Nanosecond, bit_width: 64 },V::Null]));
    assert_eq!(project("(cast \"1 10:10:10.111111111\" #xt/type [:ree [:interval :year-month]] {:start-field \"DAY\" :end-field \"SECOND\" :leading-precision 2 :fractional-precision 7})").1,
        V::IntervalMonthDayNano { months: 0, days: 1, nanoseconds: 36_610_111_111_100 });
    assert_eq!(project("(cast 3 #xt/type [:interval :year-month] {:start-field \"DAY\"})").1,
        V::IntervalMonthDayMicro { months: 0, days: 3, nanoseconds: 0 });
    assert_eq!(project("(cast (cast \"PT1.123456789S\" #xt/type [:duration :nano]) #xt/type [:duration :nano] {:precision 99})").1,
        V::Duration { value: 1_123_456_789, unit: U::Nanosecond });
    assert_eq!(project("(cast nil #xt/type [:time-local :nano] {:precision -1})"),(T::Null,V::Null));
    assert_eq!(project("(cast \"05:34:56.123456789\" #xt/type [:time-local :nano] {:precision false})").1,V::Time { value: 20_096_123_456_789,unit: U::Nanosecond,bit_width: 64 });
    assert_eq!(project("(cast-interval 10 {:start-field \"HOUR\"})").1,V::IntervalMonthDayMicro { months: 0,days: 0,nanoseconds: 36_000_000_000_000 });
    assert_eq!(project("(cast_interval 10 {:start-field \"HOUR\"})"),project("(cast-interval 10 {:start-field \"HOUR\"})"));
    assert_eq!(project("(cast-tstz \"1970-01-01T00:00:00.123456789Z\" {:unit :nano :precision 7})").1,V::Timestamp { value: 123_456_700,unit: U::Nanosecond,timezone: Some("UTC".into()) });
}

#[test]
fn session_timestamp_alias_binds_xtql_schema_to_compile_options() {
    let options = CompileOptions { default_timezone: "Europe/Paris".into(),..Default::default() };
    let Statement::Query(relation) = compile("(-> (rel [{}] []) (return {:v (cast-tstz \"1970-01-01Z\")}))",&EmptyCatalog,&options).unwrap().statement else { panic!() };
    let RelationNode::Project { input,expressions } = relation.node() else { panic!() };
    let expected = V::Timestamp { value: 0,unit: rtdb_types::TimeUnit::Microsecond,timezone: Some("Europe/Paris".into()) };
    assert_eq!(expressions[0].expr.data_type(),expected.logical_type());
    assert_eq!(evaluate(&expressions[0].expr,input.schema(),&[],&EvalContext { timezone: "Europe/Paris",..Default::default() }).unwrap(),expected);
}

#[test]
fn ignored_temporal_and_integer_interval_options_retain_malformed_values() {
    use rtdb_types::TimeUnit as U;
    for option in ["\"unused\"", ":unused", "true", "[1 2]", "{:value false}", "9223372036854775808N", "1E9999M"] {
        assert_eq!(project(&format!("(cast (cast \"1970-01-01T00:00:00.123456789Z\" #xt/type [:timestamp-tz :nano \"UTC\"]) #xt/type [:timestamp-tz :nano \"UTC\"] {{:precision {option}}})")).1,
            V::Timestamp { value: 123_456_789,unit: U::Nanosecond,timezone: Some("UTC".into()) });
        assert_eq!(project(&format!("(cast 3 #xt/type [:interval :year-month] {{:start-field \"DAY\" :leading-precision {option} :fractional-precision {option}}})")).1,
            V::IntervalMonthDayMicro { months: 0,days: 3,nanoseconds: 0 });
        assert_eq!(project(&format!("(cast [nil] #xt/type [:list [:time-local :nano]] {{:precision {option}}})")).1,V::list(vec![V::Null]));
    }
    assert_eq!(project("(cast nil #xt/type [:interval :year-month] {:start-field [:invalid] :end-field :invalid :leading-precision :invalid})"),(T::Null,V::Null));
    assert_eq!(project("(cast \"3\" #xt/type [:interval :year-month] {:start-field \"DAY\" :leading-precision 2 :fractional-precision :unused})").1,
        V::IntervalMonthDayMicro { months: 0,days: 3,nanoseconds: 0 });
}

#[test]
fn consumed_numeric_options_follow_checked_primitive_long_coercion() {
    use rtdb_types::TimeUnit as U;
    for precision in ["2", "2.9", "2.9M", "2N", "0x2N"] {
        assert_eq!(project(&format!("(cast \"PT1.123456789S\" #xt/type [:duration :nano] {{:precision {precision}}})")).1,
            V::Duration { value: 1_120_000_000,unit: U::Nanosecond });
        assert_eq!(project(&format!("(cast \"1 00:00:00.123456789\" #xt/type [:interval :month-day-nano] {{:start-field \"DAY\" :end-field \"SECOND\" :leading-precision 2.9M :fractional-precision {precision}}})")).1,
            V::IntervalMonthDayMicro { months: 0,days: 1,nanoseconds: 120_000_000 });
    }
    for precision in ["\"2\"", ":invalid", "[2]", "true", "9223372036854775808N", "##Inf"] {
        let source = format!("(-> (rel [{{}}] []) (return {{:v (cast \"PT1S\" #xt/type [:duration :nano] {{:precision {precision}}})}}))");
        assert!(compile(&source,&EmptyCatalog,&CompileOptions::default()).is_err(),"{precision}");
    }
    assert_eq!(project("(cast \"PT1.123S\" #xt/type [:duration :nano] {:precision ##NaN})").1,V::Duration { value: 1_000_000_000,unit: U::Nanosecond });
}

#[test]
fn temporal_codegen_and_generated_calls_preserve_distinct_numeric_coercions() {
    use rtdb_types::TimeUnit as U;
    for precision in ["18446744073709551618N","18446744073709551618M"] {
        let source = format!("(-> (rel [{{}}] []) (return {{:v (cast \"PT1.123S\" #xt/type [:duration :nano] {{:precision {precision}}})}}))");
        let Statement::Query(relation) = compile(&source,&EmptyCatalog,&CompileOptions::default()).unwrap().statement else { panic!() };
        let RelationNode::Project { input,expressions } = relation.node() else { panic!() };
        assert_eq!(expressions[0].expr.data_type(),T::Scalar(S::Duration(U::Nanosecond)));
        assert!(evaluate(&expressions[0].expr,input.schema(),&[],&EvalContext::default()).is_err());
        assert_eq!(project(&format!("(cast \"3\" #xt/type [:interval :year-month] {{:start-field \"DAY\" :leading-precision {precision}}})")).1,
            V::IntervalMonthDayMicro { months: 0,days: 3,nanoseconds: 0 });
    }
    for (precision,expected) in [("2.999999999999999999999M",1_123_000_000),("9.999999999999999999999M",1_123_456_789),("-0.999999999999999999999M",1_000_000_000)] {
        assert_eq!(project(&format!("(cast \"PT1.123456789S\" #xt/type [:duration :nano] {{:precision {precision}}})")).1,
            V::Duration { value: expected,unit: U::Nanosecond });
    }
    assert_eq!(project("(cast-interval 3 {:start-field \"DAY\" :fractional-precision \\2})").1,V::IntervalMonthDayMicro { months: 0,days: 3,nanoseconds: 0 });
    assert!(compile("(-> (rel [{}] []) (return {:v (cast \"PT1S\" #xt/type [:duration :nano] {:precision \\2})}))",&EmptyCatalog,&CompileOptions::default()).is_err());
    for precision in ["18446744073709551618N","18446744073709551618M"] {
        let source = format!("(-> (rel [{{}}] []) (return {{:v (cast-interval 3 {{:start-field \"DAY\" :fractional-precision {precision}}})}}))");
        assert!(compile(&source,&EmptyCatalog,&CompileOptions::default()).is_err());
    }
}

#[test]
fn cast_interval_alias_keeps_eager_fractional_type_selection() {
    assert_eq!(project("(cast-interval 3 {:start-field \"YEAR\" :leading-precision :unused :fractional-precision :unused})").1,V::IntervalYearMonth(36));
    assert!(compile("(-> (rel [{}] []) (return {:v (cast-interval 3 {:start-field \"DAY\" :fractional-precision :invalid})}))",&EmptyCatalog,&CompileOptions::default()).is_err());
    assert_eq!(project("(cast-interval 3 {:start-field \"DAY\" :fractional-precision false})").1,V::IntervalMonthDayMicro { months: 0,days: 3,nanoseconds: 0 });
}

#[test]
fn untyped_parameters_defer_option_checks_until_runtime_source_selection() {
    use rtdb_types::TimeUnit as U;
    let Statement::Query(relation) = compile("(fn [x] (-> (rel [{}] []) (return {:v (cast x #xt/type [:duration :nano] {:precision :ignored})})))",&EmptyCatalog,&CompileOptions::default()).unwrap().statement else { panic!() };
    let RelationNode::Project { input,expressions } = relation.node() else { panic!() };
    let expression = &expressions[0].expr;
    assert_eq!(expression.data_type(),T::Null);
    for value in [V::Null,V::Duration { value: 123,unit: U::Nanosecond }] {
        assert_eq!(evaluate(expression,input.schema(),&[],&EvalContext { parameters: &[value.clone()],..Default::default() }).unwrap(),value);
    }
    assert!(evaluate(expression,input.schema(),&[],&EvalContext { parameters: &[V::Utf8("PT1S".into())],..Default::default() }).is_err());
}

#[test]
fn source_family_and_precision_errors_follow_codegen_consumption_order() {
    use rtdb_types::{AnomalyCategory as C, TimeUnit as U};
    for (value, expression, category, code) in [
        (V::IntervalYearMonth(1), "(cast x #xt/type [:duration :nano] {:precision :invalid})", C::Incorrect, "xtdb.expression/invalid-cast"),
        (V::IntervalDayTime { days: 1, milliseconds: 0 }, "(cast x #xt/type [:duration :nano] {:precision :invalid})", C::Incorrect, "xtdb.expression/invalid-cast"),
        (V::Duration { value: 1, unit: U::Second }, "(cast x #xt/type [:interval :year-month] {:start-field \"YEAR\" :end-field :invalid :leading-precision :invalid :fractional-precision :invalid})", C::Incorrect, "xtdb.expression/unsupported-cast"),
        (V::Duration { value: 1, unit: U::Second }, "(cast x #xt/type [:interval :year-month] {:start-field \"MONTH\" :leading-precision :invalid :fractional-precision :invalid})", C::Incorrect, "xtdb.expression/unsupported-cast"),
        (V::IntervalYearMonth(1), "(cast x #xt/type [:interval :year-month] {:start-field \"DAY\" :leading-precision 2 :fractional-precision :invalid})", C::Unsupported, "xtdb.expression/unsupported-cast"),
        (V::IntervalYearMonth(1), "(cast x #xt/type [:interval :year-month] {:start-field \"DAY\" :end-field \"HOUR\" :leading-precision 2 :fractional-precision :invalid})", C::Unsupported, "xtdb.expression/unsupported-cast"),
        (V::IntervalYearMonth(1), "(cast x #xt/type [:interval :year-month] {:start-field \"DAY\" :end-field \"SECOND\" :leading-precision 2 :fractional-precision :invalid})", C::Incorrect, "xtql/malformed-cast"),
        (V::IntervalYearMonth(1), "(cast x #xt/type [:interval :year-month] {:start-field \"DAY\" :end-field \"SECOND\" :leading-precision 0 :fractional-precision :invalid})", C::Incorrect, "xtdb.expression/invalid-interval-precision"),
    ] {
        let source = format!("(fn [x] (-> (rel [{{}}] []) (return {{:v {expression}}})))");
        let options = CompileOptions { parameter_types: vec![value.logical_type()], ..Default::default() };
        let planned = compile(&source, &EmptyCatalog, &options).unwrap_err();
        assert_eq!((planned.category, planned.code.as_str()), (category, code), "{expression}");
        let Statement::Query(relation) = compile(&source, &EmptyCatalog, &CompileOptions::default()).unwrap().statement else { panic!() };
        let RelationNode::Project { input, expressions } = relation.node() else { panic!() };
        let evaluated = evaluate(&expressions[0].expr, input.schema(), &[], &EvalContext { parameters: &[value], ..Default::default() }).unwrap_err();
        assert_eq!((evaluated.category, evaluated.code.as_str()), (category, code), "{expression}");
    }
}

fn decimal(coefficient: i128, precision: i32, scale: i32) -> V {
    V::Decimal(DecimalValue::new(coefficient.to_be_bytes().to_vec(), precision, scale, 128).unwrap())
}

#[test]
fn expression_test_cast_list_elementwise_and_pg_array_text() {
    for (expression, expected) in [
        ("(cast [1 nil 3] #xt/type [:list :i64])", V::list(vec![V::I64(1), V::Null, V::I64(3)])),
        ("(cast [1 2] #xt/type [:list :utf8])", V::list(vec![V::Utf8("1".into()), V::Utf8("2".into())])),
        ("(cast [[1.0 2.0] [3.0]] #xt/type [:list [:list :i64]])", V::list(vec![V::list(vec![V::I64(1), V::I64(2)]), V::list(vec![V::I64(3)])])),
        ("(cast \"{1,NULL}\" #xt/type [:list :i64])", V::list(vec![V::I64(1), V::Null])),
        ("(cast [] #xt/type [:list :i64])", V::list(vec![])),
        ("(cast nil #xt/type [:list :i64])", V::Null),
    ] { assert_eq!(project(expression).1, expected, "{expression}"); }
}

#[test]
fn composite_type_casts_accept_nested_tags_and_preserve_identical_values() {
    assert_eq!(project("(cast {:x 1} #xt/type [:struct {\"x\" #xt/type :i64}])"),
        (T::Struct(StructFields::from([("x".into(), T::Scalar(S::I64))])), V::Struct(StructFields::from([("x".into(), V::I64(1))]))));
    assert_eq!(project("(cast #xt/tstz-range [nil nil] #xt/type :tstz-range)").1,
        V::TimestampTzRange { lower: i64::MIN, upper: i64::MAX });
    assert_eq!(project("(cast [1 2] #xt/type [:list #xt/type :i64])").1,
        V::list(vec![V::I64(1), V::I64(2)]));
}

#[test]
fn encoded_decimal_targets_dispatch_options_and_defaults_before_value_conversion() {
    for (encoded, plain) in [
        ("(cast 1 #xt/type [:ree [:decimal 32 2 128]])", "(cast 1 #xt/type [:decimal 32 2 128])"),
        ("(cast 1.25M #xt/type [:ree-i16 [:decimal 64 9 256]])", "1.25M"),
        ("(cast 1.25M #xt/type [:ree-i64 [:decimal 64 9 256]] {:precision 4 :scale 1})", "(cast 1.25M #xt/type [:decimal 64 9 256] {:precision 4 :scale 1})"),
        ("(cast [1.25M nil] #xt/type [:ree [:list [:ree [:decimal 64 9 256]]]] {:precision 4 :scale 1})", "(cast [1.25M nil] #xt/type [:list [:decimal 64 9 256]] {:precision 4 :scale 1})"),
    ] { assert_eq!(project(encoded), project(plain), "{encoded}"); }
    assert_eq!(project("(cast 1 #xt/type [:ree [:decimal 32 2 128]])").0, T::Scalar(S::Decimal { precision: 64, scale: 9, bit_width: 256 }));
}

#[test]
fn nullable_and_encoded_identity_targets_fail_during_known_type_binding() {
    for expression in [
        "(cast 1 #xt/type [:? :i64])",
        "(cast 1 #xt/type [:ree [:? :i64]])",
        "(cast {:x 1} #xt/type [:ree [:struct {\"x\" :i64}]])",
        "(cast {:x 1} #xt/type [:struct {\"x\" [:ree :i64]}])",
        "(cast true #xt/type [:ree :bool])",
        "(cast #{1 2} #xt/type [:ree [:set :i64]])",
        "(cast #xt/tstz-range [nil nil] #xt/type [:ree :tstz-range])",
        "(cast [1] #xt/type [:? :list :i64])",
        "(cast \"{1,NULL}\" #xt/type [:list [:? :i64]])",
    ] {
        let source = format!("(-> (rel [{{}}] []) (return {{:v {expression}}}))");
        assert_eq!(compile(&source, &EmptyCatalog, &CompileOptions::default()).unwrap_err().code, "xtdb.expression/cast-error", "{expression}");
    }
    assert_eq!(project("(cast nil #xt/type [:? :i64])"), (T::Null, V::Null));
    assert_eq!(project("(cast nil #xt/type [:ree [:struct {\"x\" :i64}]])"), (T::Null, V::Null));
    assert_eq!(project("(cast 1 #xt/type [:ree :bool])"), (T::Scalar(S::Bool), V::Bool(true)));
    assert_eq!(project("(cast 1 #xt/type [:ree :f64])"), (T::Scalar(S::F64), V::F64(1.0)));
}

#[test]
fn nullable_list_elements_remap_without_nullable_scalar_dispatch() {
    for expression in [
        "(cast [1 nil] #xt/type [:list [:? :i64]])",
        "(cast [1 nil] #xt/type [:ree [:list [:ree-i16 [:? :i64]]]])",
    ] {
        assert_eq!(project(expression), (T::list(T::Scalar(S::I64).maybe()), V::list(vec![V::I64(1), V::Null])), "{expression}");
    }
}

#[test]
fn unchecked_parameter_types_validate_encoded_identity_targets_at_evaluation() {
    let source = "(fn [x] (-> (rel [{}] []) (return {:v (cast x #xt/type [:ree :bool])})))";
    let Statement::Query(relation) = compile(source, &EmptyCatalog, &CompileOptions::default()).unwrap().statement else { panic!("Expected query") };
    let RelationNode::Project { input, expressions } = relation.node() else { panic!("Expected projection") };
    let expression = &expressions[0].expr;
    for (value, expected) in [(V::I64(1), Some(V::Bool(true))), (V::Null, Some(V::Null)), (V::Bool(true), None)] {
        let result = evaluate(expression, input.schema(), &[], &EvalContext { parameters: &[value], ..Default::default() });
        match expected { Some(expected) => assert_eq!(result.unwrap(), expected), None => assert_eq!(result.unwrap_err().code, "xtdb.expression/cast-error") }
    }
}

#[test]
fn expression_test_cast_list_applies_element_cast_opts() {
    assert_eq!(project("(cast (cast [1.5] #xt/type [:list [:decimal 64 9 256]]) #xt/type [:list [:decimal 64 9 256]] {:precision 10 :scale 2})"),
        (T::list(T::Scalar(S::Decimal { precision: 10, scale: 2, bit_width: 128 })), V::list(vec![decimal(150, 10, 2)])));
    assert_eq!(project("(cast [[1.245M nil] []] #xt/type [:list [:list [:decimal 64 9 256]]] {:precision 5 :scale 2})").1,
        V::list(vec![V::list(vec![decimal(124, 5, 2), V::Null]), V::list(vec![])]));
}

#[test]
fn unspecified_decimal_cast_preserves_literal_scale_and_scale_only_options() {
    for expression in ["(cast 1.25M #xt/type [:decimal 64 9 256])", "(cast 1.25M #xt/type [:decimal 64 9 256] {:scale 0})", "(cast 1.25M #xt/type [:decimal 64 9 256] {:scale :ignored})"] {
        assert_eq!(project(expression), (T::Scalar(S::Decimal { precision: 32, scale: 2, bit_width: 128 }), decimal(125, 32, 2)));
    }
    assert_eq!(project("(cast 125 #xt/type [:decimal 64 9 256] {:precision 3 :scale -1})").1, decimal(12, 3, -1));
}

#[test]
fn casts_retain_unification_dependencies_and_aggregate_rewriting() {
    for source in [
        "(unify (with {v (cast x #xt/type [:decimal 64 9 256])}) (rel [{:x 1.25M}] [x]))",
        "(-> (rel [{:x 1} {:x 2}] [x]) (aggregate {:total (cast (sum x) #xt/type [:decimal 64 9 256] {:precision 5 :scale 2})}))",
    ] {
        compile(source, &EmptyCatalog, &CompileOptions::default()).unwrap_or_else(|error| panic!("{source}: {error:?}"));
    }
}

#[test]
fn decimal_literals_casts_and_round_keep_signed32_scale() {
    for (expression, coefficient, scale) in [
        ("1E-2147483647M", 1, i32::MAX),
        ("1E+2147483648M", 1, i32::MIN),
        ("(cast 1E-1000M #xt/type [:decimal 64 9 256] {:precision 32 :scale 1000})", 1, 1000),
        ("(cast 0 #xt/type [:decimal 32 -2147483648 128] {:precision 32 :scale -2147483648})", 0, i32::MIN),
        ("(round 1.25E-1000M 1001)", 13, 1001),
    ] {
        assert_eq!(project(expression), (T::Scalar(S::Decimal { precision: 32, scale, bit_width: 128 }), decimal(coefficient, 32, scale)), "{expression}");
    }
    assert_eq!(project("(cast [1E-1000M 2E-1000M] #xt/type [:list [:decimal 64 9 256]] {:precision 32 :scale 1000})").1,
        V::list(vec![decimal(1, 32, 1000), decimal(2, 32, 1000)]));
}

#[test]
fn java_unicode_decimal_string_casts_keep_clojure_ascii_literal_grammar() {
    assert_eq!(project("(cast \"١२.３४\" #xt/type [:decimal 32 2 128] {:precision 32 :scale 2})").1, decimal(1234, 32, 2));
    assert_eq!(project("(cast \"١E+٢١٤٧٤٨٣٦٤٨\" #xt/type [:decimal 32 -2147483648 128] {:precision 32 :scale -2147483648})").1, decimal(1, 32, i32::MIN));
    for source in ["١M", "+१M", "1.٢M", "1e२M", "1M.0", "1eM", "1_0M"] {
        assert!(rtdb_xtql::read(source).is_err(), "{source}");
    }
    assert!(matches!(rtdb_xtql::read("𝟙M").unwrap().kind, rtdb_xtql::EdnKind::Symbol(_)));
    for source in ["1M", "+1.M", "1.e2M", "1E+2147483648M"] {
        assert!(matches!(rtdb_xtql::read(source).unwrap().kind, rtdb_xtql::EdnKind::Decimal(_)), "{source}");
    }
}

#[test]
fn decimal_inference_keeps_32_and_64_digit_classes_and_signed_cast_metadata() {
    for (digits, precision, bit_width) in [(32, 32, 128), (33, 64, 256), (64, 64, 256)] {
        let expression = format!("{}M", "9".repeat(digits));
        let (logical, value) = project(&expression);
        assert_eq!(logical, T::Scalar(S::Decimal { precision, scale: 0, bit_width }));
        let V::Decimal(value) = value else { panic!("Expected decimal") };
        assert_eq!((value.precision, value.scale, value.bit_width), (precision, 0, bit_width));
    }
    let source = format!("(-> (rel [{{}}] []) (return {{:v {}M}}))", "9".repeat(65));
    assert!(compile(&source, &EmptyCatalog, &CompileOptions::default()).is_err());
    for precision in [i32::MIN, -1, 0] {
        let source = format!("(-> (rel [{{}}] []) (return {{:v (cast 0 #xt/type [:decimal 2147483647 -2147483648 128] {{:precision {precision} :scale 0}})}}))");
        let Statement::Query(relation) = compile(&source, &EmptyCatalog, &CompileOptions::default()).unwrap().statement else { panic!("Expected query") };
        let RelationNode::Project { input, expressions } = relation.node() else { panic!("Expected projection") };
        assert_eq!(expressions[0].expr.data_type(), T::Scalar(S::Decimal { precision, scale: 0, bit_width: 128 }));
        assert!(evaluate(&expressions[0].expr, input.schema(), &[], &EvalContext::default()).is_err());
        let null_source = source.replace("(cast 0 ", "(cast nil ");
        let Statement::Query(relation) = compile(&null_source, &EmptyCatalog, &CompileOptions::default()).unwrap().statement else { panic!("Expected query") };
        let RelationNode::Project { input, expressions } = relation.node() else { panic!("Expected projection") };
        assert_eq!(evaluate(&expressions[0].expr, input.schema(), &[], &EvalContext::default()).unwrap(), V::Null);
    }
}
