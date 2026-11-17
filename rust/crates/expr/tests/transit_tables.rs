use rtdb_columnar::MemoryBudget;
use rtdb_expr::{EvalContext,EvaluationScope,evaluate};
use rtdb_format::transit::{TransitValue as T,decode_msgpack,encode_json,encode_msgpack};
use rtdb_plan::{Expr,Schema,field_type};
use rtdb_types::{Result,Value as V};

fn tag(name: &str,value: T) -> T { T::Tagged { tag: name.into(),representation: Box::new(value) } }
fn text(value: &str) -> T { T::String(value.into()) }
fn keyword(value: &str) -> T { T::Keyword(value.into()) }
fn table(schema: T,table: T) -> T { tag("xt/table",T::Map(vec![(keyword("schema-name"),schema),(keyword("table-name"),table)])) }
fn field(object: T,json: bool,context: &EvalContext<'_>) -> Result<T> {
    let bytes = if json { encode_json(&object)? } else { encode_msgpack(&object)? };
    let source = Expr::Literal(V::Transit(bytes));
    let expr = Expr::Field { data_type: field_type(&source.data_type(),"chosen"),expr: Box::new(source),name: "chosen".into() };
    let V::Transit(bytes) = evaluate(&expr,&Schema::default(),&[],context)? else { panic!("Transit") };decode_msgpack(&bytes)
}
fn selected(value: T,json: bool) -> Result<T> { field(T::Map(vec![(keyword("chosen"),value)]),json,&EvalContext::default()) }

#[test]
fn table_handlers_apply_null_defaults_and_unrestricted_object_names() {
    for json in [false,true] {
        for (schema,name,expected_schema,expected_name) in [
            (T::Null,T::Null,"public",""),(T::Bool(false),T::Bool(false),"false","false"),
            (text("a/b.c"),text(""),"a/b.c",""),(keyword("some/schema"),T::Symbol("table/name".into()),":some/schema","table/name"),
            (tag("n",text("001")),tag("f",text("1.00")),"1","1.00"),
            (T::Float(f64::INFINITY),tag("f32",T::Float(1.25)),"Infinity","1.25"),
        ] {
            assert_eq!(selected(table(schema,name),json).unwrap(),table(text(expected_schema),text(expected_name)));
        }
        for representation in [T::Null,T::Bool(false),text("name"),T::Array(vec![T::Map(vec![(keyword("table-name"),text("ignored"))])])] {
            assert_eq!(selected(tag("xt/table",representation),json).unwrap(),table(text("public"),text("")));
        }
    }
}

#[test]
fn table_map_destructuring_distinguishes_lists_vectors_and_sets() {
    for json in [false,true] {
        let entries = vec![(keyword("schema-name"),text("s")),(keyword("table-name"),text("t"))];
        for representation in [
            tag("list",T::Array(vec![T::Map(entries.clone())])),
            tag("list",T::Array(vec![keyword("schema-name"),text("s"),keyword("table-name"),text("t")])),
            tag("list",T::Array(vec![keyword("schema-name"),text("old"),keyword("schema-name"),text("s"),T::Map(vec![(keyword("table-name"),text("t"))])])),
            tag("list",T::Array(vec![keyword("schema-name"),text("s"),T::Array(vec![keyword("table-name"),text("t")])])),
        ] { assert_eq!(selected(tag("xt/table",representation),json).unwrap(),table(text("s"),text("t"))); }
        assert_eq!(selected(tag("xt/table",tag("set",T::Array(vec![keyword("schema-name"),keyword("table-name")]))),json).unwrap(),table(text(":schema-name"),text(":table-name")));
        assert_eq!(selected(tag("xt/table",tag("list",T::Array(vec![]))),json).unwrap(),table(text("public"),text("")));
        assert_eq!(selected(tag("xt/table",tag("list",T::Array(vec![keyword("table-name"),text("t"),T::Null]))),json).unwrap(),table(text("public"),text("t")));
        for trailing in [T::Bool(false),T::Array(vec![]),tag("list",T::Array(vec![T::Array(vec![keyword("table-name"),text("t")])]))] {
            assert!(selected(tag("xt/table",tag("list",T::Array(vec![keyword("schema-name"),text("s"),trailing]))),json).is_err());
        }
    }
}

#[test]
fn flat_table_destructuring_keeps_array_keys_without_hashing_at_nine_entries() {
    let mut entries = vec![keyword("schema-name"),text("s"),keyword("table-name"),text("t"),tag("unknown",T::Null),T::Null];
    for index in 0..6 { entries.extend([T::Integer(index),T::Null]); }
    for json in [false,true] {
        assert_eq!(selected(tag("xt/table",tag("list",T::Array(entries.clone()))),json).unwrap(),table(text("s"),text("t")));
    }
}

#[test]
fn flat_table_duplicate_compaction_retains_self_identity_for_unused_null_tagged_keys() {
    let entries = vec![
        tag("unknown",T::Null),T::Integer(1),
        keyword("schema-name"),text("old"),keyword("schema-name"),text("new"),
        keyword("table-name"),text("t"),
    ];
    for json in [false,true] {
        assert_eq!(selected(tag("xt/table",tag("list",T::Array(entries.clone()))),json).unwrap(),table(text("new"),text("t")));
    }
}

#[test]
fn table_text_distinguishes_scalar_to_string_from_readable_collection_elements() {
    for json in [false,true] {
        let value = T::Array(vec![T::Null,text("a\n\"b"),tag("n",text("1")),tag("f",text("1.00")),T::Float(f64::NAN),tag("c",text("\n"))]);
        assert_eq!(selected(table(T::Null,value),json).unwrap(),table(text("public"),text("[nil \"a\\n\\\"b\" 1N 1.00M ##NaN \\newline]")));
        let inner = table(text("s"),text("t"));
        assert_eq!(selected(table(inner.clone(),T::Array(vec![inner])),json).unwrap(),table(text("TableRef(schemaName=s, tableName=t)"),text("[#xt/table s/t]")));
        let form = tag("xtdb/clj-form",T::Array(vec![tag("n",text("2"))]));
        assert_eq!(selected(table(form.clone(),T::Array(vec![form])),json).unwrap(),table(text("ClojureForm(form=[2N])"),text("[#xt/clj-form [2N]]")));
    }
}

#[test]
fn table_names_use_registered_objects_custom_string_and_print_methods() {
    for json in [false,true] {
        for (value,scalar,readable) in [
            (tag("r",text("https://example.com/é")),"https://example.com/é","#xt/uri \"https://example.com/%C3%A9\""),
            (tag("u",text("00000000-0000-0000-0000-000000000001")),"00000000-0000-0000-0000-000000000001","#uuid \"00000000-0000-0000-0000-000000000001\""),
            (tag("time/date-time",text("2020-01-01T00:00:00")),"2020-01-01T00:00","#xt/ldt \"2020-01-01T00:00\""),
            (tag("xtdb/tstz-range",T::Null),"ZonedDateTimeRange(from=null, to=null)","#xt/tstz-range [nil nil]"),
            (tag("xtdb/path",text("a//b/")),"a/b","#xt/path \"a/b\""),
            (tag("xtdb/key-fn",keyword("kebab-case-keyword")),"KEBAB_CASE_KEYWORD","#xt/key-fn :kebab-case-keyword"),
            (tag("xtdb/tx-key",T::Null),"#xt/tx-key {:tx-id nil, :system-time nil}","#xt/tx-key {:tx-id nil, :system-time nil}"),
        ] {
            assert_eq!(selected(table(value.clone(),T::Array(vec![value])),json).unwrap(),table(text(scalar),text(&format!("[{readable}]"))));
        }
        let buffer = tag("xtdb/byte-array",text("0x0102"));
        assert_eq!(selected(table(T::Null,buffer),json).unwrap(),table(text("public"),text("java.nio.HeapByteBuffer[pos=0 lim=2 cap=2]")));
        let unknown = tag("x",T::Integer(1));
        assert_eq!(selected(table(T::Null,unknown),json).unwrap(),table(text("public"),text("com.cognitect.transit.impl.TaggedValueImpl@1de9f8")));
    }
}

#[test]
fn table_collection_names_keep_bytes_lists_maps_and_anomaly_print_forms() {
    for json in [false,true] {
        for (value,expected) in [
            (T::Array(vec![T::Bytes(vec![1,2])]),"[#bytes \"0102\"]"),
            (tag("list",T::Array(vec![T::Integer(1),text("x")])),"(1 \"x\")"),
            (T::Map(vec![(keyword("x"),text("y"))]),"{:x \"y\"}"),
            (tag("set",T::Array(vec![T::Integer(1)])),"#{1}"),
            (T::Array(vec![tag("xt/error",T::Array(vec![keyword("incorrect"),T::Null,text("message"),T::Map(vec![])]))]),"[ #xt/error [:incorrect nil \"message\" {}]]"),
        ] { assert_eq!(selected(table(T::Null,value),json).unwrap(),table(text("public"),text(expected))); }
        let anomaly = tag("xt/error",T::Array(vec![keyword("not-found"),T::Null,text("message"),T::Map(vec![])]));
        assert_eq!(selected(table(T::Null,anomaly),json).unwrap(),table(text("public"),text("xtdb.api.error.NotFound: message")));
        let info = tag("xtdb/exception-info",T::Array(vec![T::Null,T::Map(vec![])]));
        assert_eq!(selected(table(T::Null,info),json).unwrap(),table(text("public"),text("clojure.lang.ExceptionInfo: null {}")));
    }
}

#[test]
fn table_keys_use_completed_schema_and_table_strings_for_equality_and_hashing() {
    for json in [false,true] {
        let defaulted = table(T::Null,T::Integer(7));let canonical = table(text("public"),text("7"));
        assert_eq!(selected(T::Map(vec![(defaulted.clone(),T::Integer(1)),(canonical.clone(),T::Integer(2))]),json).unwrap(),T::Map(vec![(canonical.clone(),T::Integer(2))]));
        assert_eq!(selected(tag("set",T::Array(vec![defaulted,canonical.clone()])),json).unwrap(),tag("set",T::Array(vec![canonical])));
    }
}

#[test]
fn table_decode_and_name_errors_release_the_callers_admitted_memory() {
    let memory = MemoryBudget::new(1 << 20);
    for malformed in [
        table(T::Null,tag("x",T::Null)),
        tag("xt/table",tag("list",T::Array(vec![keyword("table-name"),text("t"),T::Bool(false)]))),
        table(T::Null,tag("r",text("invalid uri"))),
    ] {
        {
            let scope = EvaluationScope::new(&memory).unwrap();
            let context = EvalContext { allocation: Some(&scope),..Default::default() };
            let object = T::Map(vec![(keyword("chosen"),T::Integer(1)),(keyword("ignored"),malformed)]);
            assert!(field(object,false,&context).is_err());
        }
        assert_eq!(memory.used_bytes(),0);
    }
    let memory = MemoryBudget::new(4096);
    {
        let scope = EvaluationScope::new(&memory).unwrap();
        let context = EvalContext { allocation: Some(&scope),..Default::default() };
        assert!(field(T::Map(vec![(keyword("chosen"),table(T::Null,T::Array(vec![text(&"x".repeat(1024))])))]),false,&context).is_err());
    }
    assert_eq!(memory.used_bytes(),0);
}
