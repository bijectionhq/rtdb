use rtdb_expr::{EvalContext, EvaluationScope, cast, scalar_call};
use rtdb_columnar::MemoryBudget;
use rtdb_types::{LogicalType as L, ScalarType as S, Value as V};

fn part(name: &str, text: &str) -> V { scalar_call(name,&[V::Uri(text.into())],&EvalContext::default()).unwrap() }
fn text(value: &str) -> V { V::Utf8(value.into()) }

#[test]
fn uri_cast_validates_and_preserves_the_original_spelling() {
    let source = "HtTp://EXAMPLE.com:80/a/../é?x=%41#";
    let uri = cast(&text(source),&L::Scalar(S::Uri),"UTC").unwrap();
    assert_eq!(uri,V::Uri(source.into()));
    assert_eq!(cast(&uri,&L::Scalar(S::Utf8),"UTC").unwrap(),text(source));
    for source in ["bad space","http://[bad]","http://host/%0x", "s:"] {
        assert!(cast(&text(source),&L::Scalar(S::Uri),"UTC").is_err(),"{source}");
        assert!(scalar_call("uri_path",&[V::Uri(source.into())],&EvalContext::default()).is_err());
    }
}

#[test]
fn uri_extractors_match_java_component_getters_and_nulls() {
    let uri = "https://user%3Apass@EXAMPLE.com:80/path%20name?q=%ED%A0%80+#fragment";
    for (name,expected) in [("uri_scheme",text("https")),("uri_user_info",text("user:pass")),("uri_host",text("EXAMPLE.com")),("uri_port",V::I32(80)),("uri_path",text("/path name")),("uri_query",text("q=�+")),("uri_fragment",text("fragment"))] {
        assert_eq!(part(name,uri),expected);
        assert_eq!(scalar_call(name,&[V::Null],&EvalContext::default()).unwrap(),V::Null);
        assert!(scalar_call(name,&[text(uri)],&EvalContext::default()).is_err());
    }
    assert_eq!(part("uri_path","http://host"),text(""));
    assert_eq!(part("uri_path","mailto:x?y"),V::Null);
    assert_eq!(part("uri_query","mailto:x?y"),V::Null);
    assert_eq!(part("uri_query","http://host?"),text(""));
    assert_eq!(part("uri_host","http://u@bad_host:80"),V::Null);
    assert_eq!(part("uri_user_info","http://u@bad_host:80"),V::Null);
    assert_eq!(part("uri_port","http://host:2147483648"),V::Null);
    assert_eq!(part("uri_host","http://[fe80::1%eth0]/"),text("[fe80::1%eth0]"));
}

#[test]
fn uri_extraction_admits_output_before_allocation() {
    let memory = MemoryBudget::new(4096);
    let scope = EvaluationScope::new(&memory).unwrap();
    let context = EvalContext { allocation: Some(&scope),..Default::default() };
    assert!(scalar_call("uri_path",&[V::Uri("/a%20b".into())],&context).is_err());
    drop(scope);
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn uri_expression_equality_uses_objects_and_ordering_reaches_the_byte_buffer_error() {
    use rtdb_expr::{compare, ordered_compare_in_timezone, sql_eq, sql_not_eq_in_timezone, strict_eq};
    let left = V::Uri("HTTP://EXAMPLE.com/%ab".into());
    let right = V::Uri("http://example.COM/%AB".into());
    assert_eq!(sql_eq(&left,&right, &equality_scope()).unwrap(),Some(true));
    assert_eq!(sql_not_eq_in_timezone(&left,&right,"UTC", &equality_scope()).unwrap(),Some(false));
    assert!(strict_eq(&left,&right, &equality_scope()).unwrap());
    assert_eq!(sql_eq(&V::list(vec![left.clone()]),&V::list(vec![right.clone()]), &equality_scope()).unwrap(),Some(true));
    assert_eq!(sql_eq(&left,&text("HTTP://EXAMPLE.com/%ab"), &equality_scope()).unwrap(),Some(false));
    assert!(compare(&left,&right,false).is_err());
    assert!(ordered_compare_in_timezone(&left,&right,false,"UTC").is_err());
    assert!(compare(&left,&V::Null,false).is_ok());
    for name in ["least","greatest"] {
        assert!(scalar_call(name,&[left.clone(),right.clone()],&EvalContext::default()).is_err());
    }
}

#[test]
fn nested_uri_ascii_rendering_uses_nfc_and_keeps_direct_text_and_escape_spelling() {
    use rtdb_format::transit::{TransitValue as T, encode_msgpack};
    for (original,ascii) in [
        ("HTTP://host/e\u{301}","HTTP://host/%C3%A9"),
        ("/\u{1100}\u{1161}","/%EA%B0%80"),
        ("/ﬃ","/%EF%AC%83"),
        ("/%65%CC%81/e\u{301}","/%65%CC%81/%C3%A9"),
        ("HTTP://HOST/%ab","HTTP://HOST/%ab"),
    ] {
        let uri = V::Uri(original.into());
        assert_eq!(scalar_call("str",std::slice::from_ref(&uri),&EvalContext::default()).unwrap(),text(original));
        let expected = text(&format!("[#xt/uri {ascii:?}]"));
        assert_eq!(scalar_call("str",&[V::list(vec![uri])],&EvalContext::default()).unwrap(),expected);
        let transit = V::Transit(encode_msgpack(&T::Array(vec![T::Tagged { tag: "r".into(),representation: Box::new(T::String(original.into())) }])).unwrap());
        assert_eq!(scalar_call("str",&[transit],&EvalContext::default()).unwrap(),expected);
    }
}

#[test]
fn uri_nfc_scratch_is_admitted_before_nested_rendering() {
    let uri = V::Uri(format!("/a{}","\u{301}".repeat(8192)));
    let value = V::list(vec![uri]);
    let memory = MemoryBudget::new(1 << 20);
    let scope = EvaluationScope::new(&memory).unwrap();
    let context = EvalContext { allocation: Some(&scope),..Default::default() };
    assert!(scalar_call("str",&[value],&context).is_err());
    drop(scope);
    assert_eq!(memory.used_bytes(),0);
}

#[test]
fn transit_uri_rendering_rejects_invalid_object_construction() {
    use rtdb_format::transit::{TransitValue as T, encode_msgpack};
    for representation in [T::String("bad space".into()),T::Integer(1)] {
        let value = V::Transit(encode_msgpack(&T::Tagged { tag: "r".into(),representation: Box::new(representation) }).unwrap());
        assert!(scalar_call("str",&[value],&EvalContext::default()).is_err());
    }
}

fn equality_scope() -> rtdb_expr::EvaluationScope {
    rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap()
}
