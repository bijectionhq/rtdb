use rtdb_expr::{EvalContext,evaluate};
use rtdb_format::transit::{TransitValue as T,decode_msgpack,encode_msgpack};
use rtdb_plan::{Expr,Schema,field_type};
use rtdb_types::{Result,Value as V};

fn tag(name: &str,value: T) -> T { T::Tagged { tag: name.into(),representation: Box::new(value) } }
fn text(value: &str) -> T { T::String(value.into()) }
fn field(value: T) -> Result<V> {
    field_bytes(encode_msgpack(&value)?)
}
fn field_bytes(bytes: Vec<u8>) -> Result<V> {
    let default_allocation = rtdb_expr::EvaluationScope::new(&rtdb_columnar::MemoryBudget::new(64 * 1024 * 1024)).unwrap();
    let value = Expr::Literal(V::Transit(bytes));
    evaluate(&Expr::Field { data_type: field_type(&value.data_type(),"chosen"),expr: Box::new(value),name: "chosen".into() },&Schema::default(),&[],&EvalContext { allocation: Some(&default_allocation),..Default::default() })
}
fn selected(value: T) -> Result<T> {
    let V::Transit(bytes) = field(T::Map(vec![(T::Keyword("chosen".into()),value)]))? else { panic!("Transit") };decode_msgpack(&bytes)
}
fn set_values(value: T) -> Vec<T> { let T::Tagged { tag,representation } = value else { panic!("tag") };assert_eq!(tag,"set");let T::Array(values) = *representation else { panic!("array") };values }

#[test]
fn array_handlers_can_be_bypassed_by_a_nested_tag_or_map_marker() {
    let vector = T::Array(vec![T::Integer(1),T::Integer(1)]);
    for name in ["set","list","cmap"] { assert_eq!(selected(tag(name,tag("'",vector.clone()))).unwrap(),vector); }
    assert_eq!(selected(T::Array(vec![tag("cmap",T::Integer(1)),tag("set",T::Integer(2)),tag("list",T::Integer(3))])).unwrap(),T::Array(vec![T::Null,T::Integer(2),T::Integer(3)]));
    assert_eq!(selected(tag("cmap",T::Array(vec![text("x"),T::Integer(1),tag("unregistered",T::Null)]))).unwrap(),T::Map(vec![(text("x"),T::Integer(1))]));
    let V::Transit(bytes) = field_bytes(br#"["^ ","~:chosen",["~#set",["^ ","x",1]]]"#.to_vec()).unwrap() else { panic!("Transit") };
    assert_eq!(decode_msgpack(&bytes).unwrap(),T::Map(vec![(text("x"),T::Integer(1))]));
}

#[test]
fn array_map_keys_merge_before_hash_transition_and_hash_sets_keep_distinct_float_classes() {
    let single = tag("f32",T::Integer(1));let double = T::Float(1.0);
    let T::Map(entries) = selected(T::Map(vec![(single.clone(),T::Integer(1)),(double.clone(),T::Integer(2))])).unwrap() else { panic!("map") };
    assert_eq!(entries,vec![(T::Float(1.0),T::Integer(2))]);
    assert_eq!(set_values(selected(tag("set",T::Array(vec![single.clone(),double.clone()]))).unwrap()).len(),2);
    let mut entries = vec![(single,T::Integer(1))];
    entries.extend((2..10).map(|value|(T::Integer(value),T::Integer(value))));
    entries.push((double,T::Integer(2)));
    let T::Map(entries) = selected(T::Map(entries)).unwrap() else { panic!("map") };assert_eq!(entries.len(),10);
}

#[test]
fn sets_normalize_java_object_and_clojure_collection_keys() {
    for keys in [
        vec![tag("r",text("HTTP://Example.COM/%af")),tag("r",text("http://example.com/%AF"))],
        vec![tag("f",text("1.0")),tag("f",text("1.00"))],
        vec![T::Array(vec![tag("i8",T::Integer(1))]),tag("list",T::Array(vec![T::Integer(1)]))],
        vec![tag("unknown",tag("set",T::Array(vec![tag("i8",T::Integer(1))]))),tag("unknown",tag("set",T::Array(vec![T::Integer(1)])))],
    ] { assert_eq!(set_values(selected(tag("set",T::Array(keys))).unwrap()).len(),1); }
    for keys in [
        vec![T::Bytes(vec![1]),T::Bytes(vec![1])],
        vec![tag("unknown",T::Array(vec![tag("i8",T::Integer(1))])),tag("unknown",T::Array(vec![T::Integer(1)]))],
    ] { assert_eq!(set_values(selected(tag("set",T::Array(keys))).unwrap()).len(),2); }
}

#[test]
fn hash_and_equality_failures_occur_while_constructing_unselected_collections() {
    let bad = tag("unknown",T::Null);
    let object = |ignored|T::Map(vec![(T::Keyword("chosen".into()),T::Integer(7)),(T::Keyword("ignored".into()),ignored)]);
    assert_eq!(field(object(T::Map(vec![(bad.clone(),T::Integer(1))]))).unwrap(),V::Transit(encode_msgpack(&T::Integer(7)).unwrap()));
    assert!(field(object(T::Map(vec![(bad.clone(),T::Integer(1)),(bad.clone(),T::Integer(2))]))).is_err());
    assert!(field(object(tag("set",T::Array(vec![bad])))).is_err());
    assert!(field(object(tag("set",T::Array(vec![tag("f",text("10e2147483648"))])))).is_err());
}

#[test]
fn record_fields_precede_extension_entries_and_java_record_equality_accepts_maps() {
    let record = tag("xtdb/tx-key",T::Map(vec![(text("extra"),T::Integer(3))]));
    let T::Tagged { representation,.. } = selected(record.clone()).unwrap() else { panic!("record") };
    assert_eq!(*representation,T::Map(vec![(T::Keyword("tx-id".into()),T::Null),(T::Keyword("system-time".into()),T::Null),(text("extra"),T::Integer(3))]));
    let map = T::Map(vec![(T::Keyword("tx-id".into()),T::Null),(T::Keyword("system-time".into()),T::Null),(text("extra"),T::Integer(3))]);
    assert_eq!(set_values(selected(tag("set",T::Array(vec![tag("unknown",record.clone()),tag("unknown",map.clone())]))).unwrap()).len(),1);
    assert_eq!(set_values(selected(tag("set",T::Array(vec![record,map]))).unwrap()).len(),2);
}

#[test]
fn record_fixed_field_equality_precedes_extension_count_mismatch() {
    let record = |extra: bool| {
        let mut fields = vec![(T::Keyword("tx-id".into()),tag("unknown",T::Null))];
        if extra { fields.push((text("extra"),T::Integer(1))); }
        tag("xtdb/tx-key",T::Map(fields))
    };
    assert!(selected(T::Map(vec![(record(false),T::Integer(1)),(record(true),T::Integer(2))])).is_err());
}

#[test]
fn tagged_value_hashes_select_bucket_order_and_preserve_zero_hash_collisions() {
    let first = tag("x",T::Integer(1));let second = tag("x",T::Integer(2));
    assert_eq!(set_values(selected(tag("set",T::Array(vec![first.clone(),second.clone()]))).unwrap()),vec![second,first]);
    let first = tag("a",T::Integer(0));let second = tag("b",T::Integer(0));
    assert_eq!(set_values(selected(tag("set",T::Array(vec![first.clone(),second.clone()]))).unwrap()),vec![first,second]);
}
