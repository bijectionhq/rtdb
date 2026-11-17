use rtdb_expr::{EvalContext,evaluate};
use rtdb_format::transit::{TransitValue as T,decode_msgpack,encode_json,encode_msgpack};
use rtdb_plan::{Expr,Schema,field_type};
use rtdb_types::{Result,Value as V};

fn tag(name: &str,value: T) -> T { T::Tagged { tag: name.into(),representation: Box::new(value) } }
fn path(text: &str) -> T { tag("xtdb/path",T::String(text.into())) }
fn field(object: T,json: bool) -> Result<V> {
    let default_allocation = rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap();
    let bytes = if json { encode_json(&object)? } else { encode_msgpack(&object)? };
    let source = Expr::Literal(V::Transit(bytes));
    evaluate(&Expr::Field { data_type: field_type(&source.data_type(),"chosen"),expr: Box::new(source),name: "chosen".into() },&Schema::default(),&[],&EvalContext { allocation: Some(&default_allocation),..Default::default() })
}
fn selected(value: T,json: bool) -> Result<T> {
    let V::Transit(bytes) = field(T::Map(vec![(T::Keyword("chosen".into()),value)]),json)? else { panic!("Transit") };
    decode_msgpack(&bytes)
}

#[test]
fn selected_paths_apply_provider_construction_before_canonical_writing() {
    for json in [false,true] {
        for (input,output) in [("a//b/","a/b"),("file:/a%20b","/a b"),("file:///a%2F","/a/"),("file:///%ed%a0%80","/\u{fffd}"),("jrt:/java.base","/modules/java.base")] {
            assert_eq!(selected(path(input),json).unwrap(),path(output));
        }
        let object = T::Map(vec![(T::Keyword("chosen".into()),T::Integer(1)),(T::Keyword("ignored".into()),path("file:///é"))]);
        assert!(field(object,json).is_err());
    }
}

#[test]
fn path_keys_keep_raw_byte_and_provider_identity_after_text_becomes_equal() {
    for (left,right) in [("file:///%ff","file:/%ff"),("jrt:/java.base","/modules/java.base")] {
        let value = tag("set",T::Array(vec![path(left),path(right)]));
        let T::Tagged { representation,.. } = selected(value,false).unwrap() else { panic!("set") };
        let T::Array(values) = *representation else { panic!("members") };
        assert_eq!(values.len(),2);assert_eq!(values[0],values[1]);
    }
    let value = T::Map(vec![(path("file:/a//b/"),T::Integer(1)),(path("/a/b"),T::Integer(2))]);
    assert_eq!(selected(value,false).unwrap(),T::Map(vec![(path("/a/b"),T::Integer(2))]));
}
