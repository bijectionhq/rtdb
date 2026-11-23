use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext, EvaluationScope, evaluate, function_type, scalar_call};
use rtdb_plan::{BinaryOp, Expr, Schema};
use rtdb_types::{LogicalType as L, ScalarType as S, Value as V};

fn text(value: &str) -> V { V::Utf8(value.into()) }
fn call(name: &str, values: &[V]) -> V { scalar_call(name, values, &EvalContext::default()).unwrap() }
fn list(values: &[&str]) -> V { V::list(values.iter().map(|value| text(value)).collect()) }

#[test]
fn batch_execution_compiles_opened_fields_and_admits_unsigned_cells_and_parameters() {
    use std::sync::Arc;
    use rtdb_expr::{CompiledExpr, evaluate_batch};
    let physical = arrow_schema::Field::new("v", arrow_schema::DataType::UInt64, false);
    let logical = rtdb_columnar::execution_field_type(&physical).unwrap();
    let field = rtdb_plan::Field { id: rtdb_plan::ColumnId(0), name: "v".into(), qualifier: None, qualified_only: false, unnamed: false, data_type: logical.clone() };
    let schema = Schema::new(vec![field.clone()]).unwrap();
    let expression = Expr::binary(BinaryOp::Add, field.expression(), Expr::Parameter { index: 0, data_type: logical });
    let compiled = CompiledExpr::compile(&expression, &schema).unwrap();
    let input = arrow_array::RecordBatch::try_new(Arc::new(arrow_schema::Schema::new(vec![physical])), vec![Arc::new(arrow_array::UInt64Array::from(vec![u64::MAX]))]).unwrap();
    let memory = MemoryBudget::new(1 << 20);
    let output = evaluate_batch(&compiled, &input, &EvalContext { parameters: rtdb_expr::Parameters::Values(&[V::U64(u64::MAX)]), ..Default::default() }, &memory).unwrap();
    assert_eq!(output.batch().schema().field(0).data_type(), &arrow_schema::DataType::Int64);
    assert_eq!(rtdb_columnar::read_value(output.batch().schema().field(0), output.batch().column(0).as_ref(), 0).unwrap(), V::I64(-2));
    assert_eq!(rtdb_columnar::read_value(input.schema().field(0), input.column(0).as_ref(), 0).unwrap(), V::U64(u64::MAX));
    drop(output);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn test_str_retains_original_variadic_null_numeric_and_keyword_assertions() {
    assert_eq!(call("str", &[]), text(""));
    assert_eq!(call("str", &[text("hello")]), text("hello"));
    assert_eq!(call("str", &[V::I64(42)]), text("42"));
    assert_eq!(call("str", &[text("hello"), text(" "), text("world")]), text("hello world"));
    assert_eq!(call("str", &[text("hello"), V::Null, text("world")]), text("helloworld"));
    assert_eq!(call("str", &[text("number: "), V::F64(42.0)]), text("number: 42.0"));
    assert_eq!(call("str", &[text("keyword: "), V::Keyword(rtdb_types::Keyword::from_name("foo/bar"))]), text("keyword: :foo/bar"));
    assert_eq!(call("str", &[V::Null]), text(""));
    assert_eq!(function_type("str", &[L::Null]).unwrap(), L::Scalar(S::Utf8));
}

#[test]
fn keyword_conversion_uses_the_spelling_rules_for_each_operation() {
    let keyword = |name| V::Keyword(rtdb_types::Keyword::from_name(name));
    let target = L::Scalar(S::Keyword);
    for (input, name) in [("foo/bar", "foo/bar"), (":foo/bar", "foo/bar"), ("::foo/bar", ":foo/bar"), (":", ""), ("", "")] {
        let value = text(input);
        assert_eq!(rtdb_expr::cast(&value, &target, "UTC").unwrap(), keyword(name));
        assert_eq!(call("keyword", &[value]), keyword(input));
    }
    let prefixed = keyword(":foo/bar");
    assert_eq!(rtdb_expr::cast(&prefixed, &target, "UTC").unwrap(), prefixed);
    assert_eq!(rtdb_expr::cast(&V::Null, &target, "UTC").unwrap(), V::Null);
    assert_eq!(call("keyword", &[V::Null]), V::Null);
}

#[test]
fn test_cast_any_to_text_retains_original_collections_binary_and_uri_assertions() {
    let object = |fields: &[(&str, V)]| V::Struct(fields.iter().map(|(key, value)| ((*key).into(), value.clone())).collect());
    for (value, expected) in [
        (V::I64(42), "42"), (V::F64(3.14), "3.14"), (V::Bool(true), "true"), (text("hello"), "hello"),
        (V::list(vec![V::I64(1), V::I64(2), V::I64(3)]), "[1 2 3]"),
        (list(&["a", "b"]), "[\"a\" \"b\"]"),
        (object(&[("a", V::I64(1))]), "{\"a\" 1}"),
        (object(&[("x", text("hello")), ("y", V::I64(42))]), "{\"x\" \"hello\", \"y\" 42}"),
        (object(&[("arr", V::list(vec![V::I64(1), V::I64(2)])), ("obj", object(&[("z", V::I64(3))]))]), "{\"arr\" [1 2], \"obj\" {\"z\" 3}}"),
        (V::Set(vec![V::I64(1), V::I64(2), V::I64(3)]), "[1 2 3]"),
        (V::Uri("https://example.org/".into()), "https://example.org/"),
        (V::Binary(vec![0xde, 0xad, 0xbe, 0xef]), "\\xdeadbeef"),
    ] {
        assert_eq!(rtdb_expr::cast(&value, &L::Scalar(S::Utf8), "UTC").unwrap(), text(expected));
        assert_eq!(call("str", &[value]), text(expected));
    }
}

#[test]
fn reader_text_keeps_nested_extension_and_transit_forms() {
    let decimal = V::Decimal(rtdb_types::DecimalValue::new(123i128.to_be_bytes().to_vec(), 3, 2, 128).unwrap());
    assert_eq!(call("str", &[decimal.clone()]), text("1.23"));
    assert_eq!(call("str", &[V::List(vec![decimal, V::Keyword(rtdb_types::Keyword::from_name("foo/bar")), V::Uuid([0; 16]), V::Uri("https://example.org/é".into())])]),
        text("[1.23M :foo/bar #uuid \"00000000-0000-0000-0000-000000000000\" #xt/uri \"https://example.org/%C3%A9\"]"));
    use rtdb_format::transit::{TransitValue as T, encode_msgpack};
    let value = V::Transit(encode_msgpack(&T::Array(vec![T::Keyword("x".into()), T::String("hello".into())])).unwrap());
    assert_eq!(call("str", &[value.clone()]), text("[:x \"hello\"]"));
    assert_eq!(call("format", &[text("%s"), value]), text("[:x \"hello\"]"));
    assert_eq!(call("str", &[V::Transit(encode_msgpack(&T::String("hello".into())).unwrap())]), text("hello"));
}

#[test]
fn arrow_list_text_uses_the_elements_logical_objects() {
    let keyword = |name| V::Keyword(rtdb_types::Keyword::from_name(name));
    let record = |name: &str, number| V::Struct([(name.to_owned(), V::I64(number))].into_iter().collect());
    for (values, expected) in [
        (vec![keyword("foo"), keyword("bar")], "[:foo :bar]"),
        (vec![V::Uuid(uuid::uuid!("123e4567-e89b-12d3-a456-426614174000").into_bytes())], "[#uuid \"123e4567-e89b-12d3-a456-426614174000\"]"),
        (vec![V::Set(vec![V::I64(1)])], "[#{1}]"),
        (vec![record("a", 1), record("b", 2)], "[{\"a\" 1} {\"b\" 2}]"),
        (vec![V::TimestampTzRange { lower: i64::MIN, upper: i64::MAX }], "[#xt/tstz-range [nil nil]]"),
    ] {
        let source = V::list(values);
        let ty = source.logical_type();
        let field = rtdb_columnar::to_field("x", &ty).unwrap();
        let array = rtdb_columnar::build_array(&ty, &[source]).unwrap();
        let value = rtdb_columnar::read_value(&field, array.as_ref(), 0).unwrap();
        assert_eq!(rtdb_expr::cast(&value, &L::Scalar(S::Utf8), "UTC").unwrap(), text(expected));
    }
}

#[test]
fn temporal_text_casts_keep_java_spelling_and_format_reads_primitive_units() {
    use rtdb_types::TimeUnit as U;
    assert_eq!(call("str", &[V::DateMilli(-1)]), text("1969-12-31"));
    assert_eq!(call("str", &[V::DateMilli(-86_400_001)]), text("1969-12-30"));
    assert_eq!(call("str", &[V::List(vec![V::DateMilli(-1)])]), text("[#time/date \"1969-12-31\"]"));
    assert_eq!(call("str", &[V::Timestamp { value: 1, unit: U::Nanosecond, timezone: None }]), text("1970-01-01T00:00:00.000000001"));
    assert_eq!(call("str", &[V::Timestamp { value: 0, unit: U::Second, timezone: Some("UTC".into()) }]), text("1970-01-01T00:00Z[UTC]"));
    assert_eq!(call("str", &[V::Timestamp { value: 0, unit: U::Second, timezone: Some("+02:30".into()) }]), text("1970-01-01T02:30+02:30"));
    assert_eq!(call("str", &[V::Time { value: 1, unit: U::Millisecond, bit_width: 32 }]), text("00:00:00.001"));
    assert_eq!(call("format", &[text("%s/%s"), V::DateMilli(-86_400_001), V::Timestamp { value: 42, unit: U::Microsecond, timezone: Some("UTC".into()) }]), text("-2/42"));
    for (value, expected) in [(V::F64(1e7), "1.0E7"), (V::F64(1e-4), "1.0E-4"), (V::F64(-0.0), "-0.0"), (V::F64(f64::INFINITY), "Infinity")] { assert_eq!(call("str", &[value]), text(expected)); }
}

#[test]
fn test_like_regex_retains_original_search_flags_and_ignored_flag_assertions() {
    for (source, pattern, flags, expected) in [
        ("", "", "", true), ("a", "", "", true), ("foo", "oo", "", true), ("foo", "^fo$", "", false),
        ("A", "a", "", false), ("A", "a", "i", true), ("a\nB\nc", "^B$", "", false),
        ("a\nB\nc", "^B$", "m", true), ("a\nB\nc", "^b$", "i  zz\nm", true), ("a\nB\nc", "a.b.c", "is", true),
        ("a b", "a b", "x", true),
    ] { assert_eq!(call("like_regex", &[text(source), text(pattern), text(flags)]), V::Bool(expected)); }
    assert_eq!(call("like_regex", &[V::Null, text("."), text("")]), V::Null);
}

#[test]
fn regexp_replace_supports_postgres_and_java_backreferences_without_expanding_before_reserve() {
    assert_eq!(call("regexp_replace", &[text("ab ab"), text("(a)(b)"), text(r"\2\1")]), text("ba ba"));
    assert_eq!(call("regexp_replace", &[text("ab"), text("(a)(b)"), text("$2$1")]), text("ba"));
    assert_eq!(call("regexp_replace", &[text("ab"), text("(a)(b)"), text(r"\\1")]), text(r"\1"));
    assert_eq!(call("regexp_replace", &[text("A A"), text("a"), text("x"), text("i")]), text("x x"));
    assert_eq!(call("regexp_replace", &[text("x"), text("z"), text("$")]), text("x"));
    assert!(scalar_call("regexp_replace", &[text("x"), text("x"), text("$")], &EvalContext::default()).is_err());
}

#[test]
fn concat_requires_two_homogeneous_values_and_stops_at_null() {
    for values in [vec![], vec![text("one")]] {
        let error = scalar_call("concat", &values, &EvalContext::default()).unwrap_err();
        assert_eq!(error.code, "xtdb.expression/arity-error");
        assert_eq!(error.message.as_deref(), Some("Arity error, concat requires at least two arguments"));
    }
    assert_eq!(call("concat", &[V::Binary(vec![0, 255]), V::Binary(vec![1])]), V::Binary(vec![0, 255, 1]));
    assert_eq!(call("concat", &[text("a"), V::Null]), V::Null);
    assert_eq!(function_type("concat", &[L::Null, L::Null]).unwrap(), L::Null);
    assert_eq!(scalar_call("concat", &[text("a"), V::Binary(vec![])], &EvalContext::default()).unwrap_err().message.as_deref(), Some("All arguments to `concat` must be of the same type."));
    let failure = Expr::Cast { expr: Box::new(Expr::binary(BinaryOp::Divide, Expr::Literal(V::I64(1)), Expr::Literal(V::I64(0)))), target: rtdb_plan::CastTarget::Fixed(L::Scalar(S::Utf8)) };
    let expression = Expr::Call { name: "concat".into(), args: vec![Expr::Literal(V::Null), failure], return_type: L::Scalar(S::Utf8).maybe() };
    assert_eq!(evaluate(&expression, &Schema::default(), &[], &EvalContext::default()).unwrap(), V::Null);
}

#[test]
fn test_trim_and_test_binary_trim_keep_primary_examples() {
    for (input, leading, trailing, both) in [("", "", "", ""), (" ", " ", " ", " "), ("a", "a", "a", "a"), ("$a$", "a$", "$a", "a"), ("$$$", "", "", ""), ("$a$a$", "a$a$", "$a$a", "a$a")] {
        for (name, expected) in [("trim_leading", leading), ("trim_trailing", trailing), ("trim", both)] {
            assert_eq!(call(name, &[text(input), text("$")]), text(expected));
        }
    }
    for name in ["trim", "trim_leading", "trim_trailing"] {
        assert_eq!(call(name, &[text("a"), V::Null]), V::Null);
        assert_eq!(call(name, &[V::Null, text("a")]), V::Null);
    }
    for (name, expected) in [("trim", vec![42]), ("trim_leading", vec![42, 0]), ("trim_trailing", vec![0, 42])] {
        for octet in [V::Binary(vec![0, 99]), V::I8(0), V::I16(0), V::I32(0), V::I64(0), V::F32(0.0), V::F64(0.0)] {
            assert_eq!(call(name, &[V::Binary(vec![0, 42, 0]), octet]), V::Binary(expected.clone()));
        }
    }
    assert_eq!(call("trim", &[text("😎a😎"), text("😎")]), text("a😎"));
    assert_eq!(call("trim_trailing", &[text("a😎"), text("😎")]), text("a😎"));
    assert_eq!(scalar_call("trim", &[text("a"), text("")], &EvalContext::default()).unwrap_err().code, "xtdb.expression/data-exception");
    assert!(scalar_call("trim", &[V::Binary(vec![255]), V::I64(255)], &EvalContext::default()).is_err());
}

#[test]
fn stringutil_substring_overlay_positions_and_i32_wrapping() {
    let source = text("a😀éz");
    assert_eq!(call("substring", &[source.clone(), V::I64(2), V::I64(2)]), text("😀é"));
    assert_eq!(call("substring", &[source.clone(), V::I64(0), V::I64(2)]), text("a"));
    assert_eq!(call("substring", &[source.clone(), V::I64(4_294_967_298)]), text("😀éz"));
    assert_eq!(call("substring", &[source.clone(), V::I64(i64::from(i32::MIN))]), text(""));
    assert_eq!(call("substring", &[V::Binary(vec![10, 20, 30]), V::I64(2), V::I64(1)]), V::Binary(vec![20]));
    assert_eq!(call("position", &[text("é"), source.clone()]), V::I32(3));
    assert_eq!(call("octet_position", &[text("é"), source.clone()]), V::I32(6));
    assert_eq!(call("octet_position", &[V::Binary(vec![]), V::Binary(vec![])]), V::I32(1));
    assert_eq!(call("octet_length", &[source.clone()]), V::I32(8));
    assert_eq!(call("octet_length", &[V::Binary(vec![0, 255])]), V::I32(2));
    assert_eq!(call("overlay", &[source, text("XY"), V::I64(2), V::I64(1)]), text("aXYéz"));
    assert_eq!(call("overlay", &[V::Binary(vec![1, 2, 3]), V::Binary(vec![9]), V::I64(2), V::I64(1)]), V::Binary(vec![1, 9, 3]));
    let error = scalar_call("substring", &[text("abc"), V::I64(1), V::I64(-1)], &EvalContext::default()).unwrap_err();
    assert_eq!(error.code, "xtdb.expression/illegal-argument-exception");
    assert_eq!(error.message.as_deref(), Some("Negative substring length"));
}

#[test]
fn string_to_array_parse_ident_quote_ident_and_utf16_edges() {
    assert_eq!(call("string_to_array", &[text("a,,b,"), text(",")]), list(&["a", "", "b", ""]));
    assert_eq!(call("string_to_array", &[text(""), V::Null]), list(&[]));
    assert_eq!(call("string_to_array", &[text("ab"), text("")]), list(&["ab"]));
    assert_eq!(call("string_to_array", &[text("a😀"), V::Null]), list(&["a", "?", "?"]));
    assert_eq!(call("string_to_array", &[V::Null, text(",")]), V::Null);
    assert_eq!(call("parse_ident", &[text("Foo.\"Bar\".\"a\"\"b\"")]), list(&["foo", "Bar", "a\"b"]));
    assert_eq!(call("parse_ident", &[text(" Unchecked Name .x")]), list(&[" unchecked name ", "x"]));
    for (input, code) in [("", "zero-length-identifier"), ("a.", "zero-length-identifier"), ("\"\"", "empty-quoted-identifier"), ("\"open", "unterminated-quoted-identifier"), ("\"a\"x", "unexpected-character-in-identifier")] {
        let error = scalar_call("parse_ident", &[text(input)], &EvalContext::default()).unwrap_err();
        assert_eq!(error.code, format!("xtdb.expression/{code}"));
        assert_eq!(error.context["input"], input);
    }
    for (input, expected) in [("normal", "normal"), ("_name", "_name"), ("select", "\"select\""), ("analyse", "\"analyse\""), ("local_date", "\"local_date\""), ("Foo", "\"Foo\""), ("a\"b", "\"a\"\"b\"")] { assert_eq!(call("quote_ident", &[text(input)]), text(expected)); }
    assert_eq!(call("reverse", &[text("a😀b")]), text("b😀a"));
    assert_eq!(call("replace", &[text("😀"), text(""), text("-")]), text("-?-?-"));
}

#[test]
fn pgformat_test_original_width_position_quoting_and_null_cases() {
    for (format, arguments, expected) in [
        ("Hello %s", vec![text("World")], "Hello World"),
        ("Testing %s, %s, %s, %%", vec![text("one"), text("two"), text("three")], "Testing one, two, three, %"),
        ("INSERT INTO %I VALUES(%L)", vec![text("Foo bar"), text("O'Reilly")], "INSERT INTO \"Foo bar\" VALUES('O''Reilly')"),
        ("|%10s|", vec![text("foo")], "|       foo|"),
        ("|%-10s|", vec![text("foo")], "|foo       |"),
        ("|%*s|", vec![V::I64(-10), text("foo")], "|foo       |"),
        ("|%*2$s|", vec![text("foo"), V::I64(10), text("bar")], "|       bar|"),
        ("|%1$*2$s|", vec![text("foo"), V::I64(10), text("bar")], "|       foo|"),
        ("Testing %3$s, %2$s, %s", vec![text("one"), text("two"), text("three")], "Testing three, two, three"),
        ("value: %s", vec![V::Null], "value: "), ("value: %L", vec![V::Null], "value: NULL"),
        ("%I", vec![text("select")], "select"), ("%4s", vec![text("😀")], "  😀"),
        ("%z %", vec![], "%z %"),
    ] {
        let mut values = vec![text(format)]; values.extend(arguments);
        assert_eq!(call("format", &values), text(expected));
    }
    assert_eq!(scalar_call("format", &[text("%I"), V::Null], &EvalContext::default()).unwrap_err().message.as_deref(), Some("null values cannot be formatted as an SQL identifier"));
}

#[test]
fn format_reserves_width_before_padding_and_retains_output_until_transfer() {
    let memory = MemoryBudget::new(1_000_000);
    let scope = EvaluationScope::new(&memory).unwrap();
    let context = EvalContext { allocation: Some(&scope), ..Default::default() };
    let value = scalar_call("format", &[text("%100000s"), text("x")], &context).unwrap();
    assert!(memory.used_bytes() >= 100_000);
    assert_eq!(scalar_call("format", &[text("%2147483647s"), text("x")], &context).unwrap_err().code, "rtdb/memory-budget-exceeded");
    drop(value); drop(scope);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn list_text_outputs_keep_vector_capacity_charged_until_scope_transfer() {
    let memory = MemoryBudget::new(1_000_000);
    let scope = EvaluationScope::new(&memory).unwrap();
    let context = EvalContext { allocation: Some(&scope), ..Default::default() };
    let characters = scalar_call("string_to_array", &[text(&"a".repeat(1000)), V::Null], &context).unwrap();
    let identifiers = scalar_call("parse_ident", &[text(&vec!["a"; 500].join("."))], &context).unwrap();
    assert!(memory.used_bytes() >= characters.retained_bytes().unwrap() + identifiers.retained_bytes().unwrap());
    drop(characters); drop(identifiers); drop(scope);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn invocation_admission_precedes_argument_evaluation_and_null_short_circuit() {
    for name in ["concat", "str"] {
        let memory = MemoryBudget::new(8192);
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope), ..Default::default() };
        let mut args = vec![Expr::Literal(V::Null); 1024];
        args[1] = Expr::Parameter { index: 0, data_type: L::Scalar(S::Utf8) };
        let expression = Expr::Call { name: name.into(), args, return_type: L::Scalar(S::Utf8) };
        assert_eq!(evaluate(&expression, &Schema::default(), &[], &context).unwrap_err().code, "rtdb/memory-budget-exceeded");
        assert_eq!(memory.peak_bytes(), 8192);
        drop(scope);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn batch_output_container_budget_is_checked_before_any_row_is_evaluated() {
    use std::sync::Arc;
    use arrow_array::{RecordBatch, RecordBatchOptions};
    use rtdb_expr::{CompiledExpr, evaluate_batch};
    let expression = CompiledExpr::compile(&Expr::Literal(text("x")), &Schema::default()).unwrap();
    let options = RecordBatchOptions::new().with_row_count(Some(1024));
    let input = RecordBatch::try_new_with_options(Arc::new(arrow_schema::Schema::empty()), vec![], &options).unwrap();
    let memory = MemoryBudget::new(4096);
    assert_eq!(evaluate_batch(&expression, &input, &EvalContext::default(), &memory).unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.peak_bytes(), 0);
    assert_eq!(memory.used_bytes(), 0);
}
