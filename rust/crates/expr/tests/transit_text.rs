use rtdb_expr::{EvalContext,cast,scalar_call};
use rtdb_format::transit::{TransitValue as T,encode_msgpack};
use rtdb_types::{LogicalType,ScalarType,Value as V};

fn tag(name: &str,representation: T) -> T { T::Tagged { tag: name.into(),representation: Box::new(representation) } }
fn stored(value: &T) -> V { V::Transit(encode_msgpack(value).unwrap()) }
fn int(value: i64) -> T { T::Integer(value) }
fn keyword(value: &str) -> T { T::Keyword(value.into()) }
fn big(value: &str) -> T { tag("n",T::String(value.into())) }

fn cast_text(value: &T) -> String {
    let V::Utf8(text) = cast(&stored(value),&LogicalType::Scalar(ScalarType::Utf8),"UTC").unwrap() else { panic!("text") };
    let V::Utf8(str_text) = scalar_call("str",&[stored(value)],&EvalContext::default()).unwrap() else { panic!("text") };
    assert_eq!(text,str_text);
    text
}
fn format_text(value: &T) -> V { scalar_call("format",&[V::Utf8("%s".into()),stored(value)],&EvalContext::default()).unwrap() }

fn hash_map_of_one_to_nine() -> T { T::Map((1..=9).map(|value|(int(value),int(value))).collect()) }
fn set_of_one_to_three() -> T { tag("set",T::Array(vec![int(1),int(2),int(3)])) }
fn uuid_words() -> T { tag("u",T::Array(vec![int(1),int(2)])) }

#[test]
fn varchar_casts_and_str_print_stored_values_with_pr_str() {
    for (value,expected) in [
        (T::String("plain".into()),"plain"),
        (T::Array(vec![T::String("a".into()),T::Null]),r#"["a" nil]"#),
        (T::Map(vec![(T::Array(vec![int(1),int(2)]),T::String("a".into()))]),r#"{[1 2] "a"}"#),
        (uuid_words(),r#"#uuid "00000000-0000-0001-0000-000000000002""#),
        // PersistentHashMap/Set iteration order: (set (range 1 10)) prints #{7 1 4 6 3 2 9 5 8}.
        (hash_map_of_one_to_nine(),"{7 7, 1 1, 4 4, 6 6, 3 3, 2 2, 9 9, 5 5, 8 8}"),
        (set_of_one_to_three(),"#{1 3 2}"),
        (tag("c",T::String("a".into())),r"\a"),
        (tag("c",T::String("\n".into())),r"\newline"),
        (tag("ratio",T::Array(vec![big("1"),big("3")])),"1/3"),
        (big("1"),"1N"),
        (tag("with-meta",T::Array(vec![T::Array(vec![int(1),int(2)]),T::Map(vec![(keyword("a"),int(1))])])),"[1 2]"),
        (tag("xtdb/key-fn",keyword("snake-case-string")),"#xt/key-fn :snake-case-string"),
        (tag("xtdb/clj-form",tag("list",T::Array(vec![T::Symbol("+".into()),int(1),int(2)]))),"#xt/clj-form (+ 1 2)"),
        (tag("xt/table",T::Map(vec![(keyword("schema-name"),T::String("public".into())),(keyword("table-name"),T::String("foo".into()))])),"#xt/table foo"),
        (tag("xt/table",T::Map(vec![(keyword("schema-name"),T::String("s".into())),(keyword("table-name"),T::String("foo".into()))])),"#xt/table s/foo"),
    ] {
        assert_eq!(cast_text(&value),expected,"{value:?}");
    }
}

#[test]
fn varchar_casts_of_lists_print_nested_stored_strings_quoted() {
    let list = V::list(vec![stored(&T::String("a".into())),stored(&big("2"))]);
    assert_eq!(cast(&list,&LogicalType::Scalar(ScalarType::Utf8),"UTC").unwrap(),V::Utf8(r#"["a" 2N]"#.into()));
}

#[test]
fn format_arguments_use_str_of_stored_values() {
    for (value,expected) in [
        (big("1"),"1"),
        (tag("c",T::String("a".into())),"a"),
        (tag("ratio",T::Array(vec![big("1"),big("3")])),"1/3"),
        (keyword("k"),":k"),
        (T::Array(vec![T::String("a".into()),int(1)]),r#"["a" 1]"#),
        (T::Map(vec![(T::Array(vec![int(1),int(2)]),T::String("a".into()))]),r#"{[1 2] "a"}"#),
        (set_of_one_to_three(),"#{1 3 2}"),
        (uuid_words(),"00000000-0000-0001-0000-000000000002"),
        (tag("xtdb/key-fn",keyword("snake-case-string")),"SNAKE_CASE_STRING"),
        (tag("xt/table",T::Map(vec![(keyword("schema-name"),T::String("public".into())),(keyword("table-name"),T::String("foo".into()))])),"TableRef(schemaName=public, tableName=foo)"),
    ] {
        assert_eq!(format_text(&value),V::Utf8(expected.into()),"{value:?}");
    }
}
