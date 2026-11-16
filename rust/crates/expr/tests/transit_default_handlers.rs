use rtdb_expr::{EvalContext,evaluate};
use rtdb_format::transit::{TransitValue as T,decode_msgpack,encode_msgpack};
use rtdb_plan::{Expr,Schema,field_type};
use rtdb_types::{Result,Value as V};

fn tag(name: &str,representation: T) -> T { T::Tagged { tag: name.into(),representation: Box::new(representation) } }
fn text(value: &str) -> T { T::String(value.into()) }
fn evaluate_bytes(bytes: Vec<u8>) -> Result<V> {
    let value = Expr::Literal(V::Transit(bytes));
    evaluate(&Expr::Field { data_type: field_type(&value.data_type(),"chosen"),expr: Box::new(value),name: "chosen".into() },&Schema::default(),&[],&EvalContext::default())
}
fn selected(value: T) -> Result<T> {
    let value = T::Map(vec![(T::Keyword("chosen".into()),value)]);
    let V::Transit(bytes) = evaluate_bytes(encode_msgpack(&value)?)? else { panic!("Transit") };decode_msgpack(&bytes)
}

#[test]
fn default_primitive_handlers_validate_the_representation_and_use_java_string_rules() {
    let value = T::Array(vec![tag("_",T::Integer(7)),tag("?",text("anything")),tag("?",text("t")),tag(":",T::Integer(7)),tag(":",T::Symbol("a/b".into())),tag("$",T::Keyword("a/b".into())),tag("i",text("+００７"))]);
    assert_eq!(selected(value).unwrap(),T::Array(vec![T::Null,T::Bool(false),T::Bool(true),T::Null,T::Keyword("a/b".into()),T::Symbol("a/b".into()),T::Integer(7)]));
    for value in [tag("?",T::Bool(true)),tag("$",T::Null),tag("i",T::Integer(1)),tag("i",text("9223372036854775808")),tag("d",T::Float(1.0)),tag("z",T::Integer(1))] {
        assert!(selected(value).is_err());
    }
    for (input,expected) in [("-9223372036854775808",i64::MIN),("+9223372036854775807",i64::MAX),("0000000000000000000000000000000000000000000000000000000000000000007",7)] {
        assert_eq!(selected(tag("i",text(input))).unwrap(),T::Integer(expected));
    }
    for input in ["-9223372036854775809","9999999999999999999999999999999999999999999999999999999999999999999"] { assert!(selected(tag("i",text(input))).is_err()); }
}

#[test]
fn double_handlers_accept_java_decimal_and_hexadecimal_spelling_with_exact_rounding() {
    for (input,expected) in [
        ("\u{1} +1.5D \t",1.5),("0x1.8p1f",3.0),("0x1.00000000000008p0",1.0),
        ("0x1.00000000000008001p0",f64::from_bits(1.0f64.to_bits() + 1)),
        ("0x0.0000000000001p-1022",f64::from_bits(1)),("0x1p-1075",0.0),
        ("0x1.0000000000001p-1075",f64::from_bits(1)),("-0x1p-1075",-0.0),
        ("0x1.fffffffffffff8p1023",f64::INFINITY),("1e999999999999999999999",f64::INFINITY),
        ("-0x0p99999999999999999999",-0.0),("+Infinity",f64::INFINITY),
    ] {
        let T::Float(value) = selected(tag("d",text(input))).unwrap() else { panic!("double") };
        assert_eq!(value.to_bits(),expected.to_bits(),"{input}");
    }
    let T::Float(value) = selected(tag("d",text("-NaN"))).unwrap() else { panic!("double") };assert!(value.is_nan());
    for input in ["nan","inf","InfinityD","1_0","１.０","1e","0x1","0xp1","0x1p1p1","1.2.3","\u{a0}1"] { assert!(selected(tag("d",text(input))).is_err(),"{input}"); }
}

#[test]
fn binary_handlers_accept_omitted_padding_and_unused_bits_without_accepting_partial_padding() {
    for text in ["AQ","AQ==","Af","Af=="] { assert_eq!(selected(tag("b",T::String(text.into()))).unwrap(),T::Bytes(vec![1])); }
    for text in ["A","AQ=","AQ===","AQ==x","AQ==\n","A_","=","===="] { assert!(selected(tag("b",T::String(text.into()))).is_err(),"{text}"); }
    let V::Transit(bytes) = evaluate_bytes(br#"["^ ","~:chosen","~bAQ"]"#.to_vec()).unwrap() else { panic!("Transit") };
    assert_eq!(decode_msgpack(&bytes).unwrap(),T::Bytes(vec![1]));
}

#[test]
fn metadata_is_validated_and_omitted_by_the_default_writer() {
    for value in [T::Array(vec![T::Integer(1)]),tag("list",T::Array(vec![T::Integer(1)])),tag("set",T::Array(vec![T::Integer(1)])),T::Map(vec![]),T::Symbol("s".into())] {
        assert_eq!(selected(tag("with-meta",T::Array(vec![value.clone(),T::Map(vec![(T::Keyword("x".into()),T::Integer(2))])]))).unwrap(),value);
    }
    for value in [tag("with-meta",T::Null),tag("with-meta",T::Array(vec![])),tag("with-meta",T::Array(vec![T::Null,T::Null])),tag("with-meta",T::Array(vec![T::Integer(1),T::Null])),tag("with-meta",T::Array(vec![T::Array(vec![]),T::Bool(false)]))] { assert!(selected(value).is_err()); }
}

#[test]
fn links_accept_maps_without_field_validation_and_keep_reference_identity() {
    let link = tag("link",T::Map(vec![(T::Integer(1),T::Bool(false))]));
    assert_eq!(selected(link.clone()).unwrap(),link);
    assert!(selected(tag("link",T::Null)).is_err());
    let pairs = T::Array(vec![T::Array(vec![link.clone(),T::Integer(1)]),T::Array(vec![link,T::Integer(2)])]);
    let T::Tagged { representation,.. } = selected(tag("xtdb/tx-key",pairs)).unwrap() else { panic!("record") };
    let T::Map(entries) = *representation else { panic!("map") };assert_eq!(entries.len(),4);
    let T::Tagged { representation,.. } = selected(tag("link",tag("xtdb/tx-key",T::Map(vec![])))).unwrap() else { panic!("link") };
    assert!(matches!(*representation,T::Map(_)));
}

#[test]
fn invalid_nested_registered_children_are_read_before_discarding_metadata_or_null_payloads() {
    for value in [tag("_",tag("$",T::Null)),tag("with-meta",T::Array(vec![T::Symbol("s".into()),T::Null,tag("d",text("bad"))]))] { assert!(selected(value).is_err()); }
}
