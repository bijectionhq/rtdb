use std::sync::Arc;
use rtdb_format::transit::{TransitReadBuilder,TransitValue as T,decode_with,encode_msgpack};
use rtdb_types::{Anomaly,Result};

#[derive(Debug)]
enum Value { Scalar(T),Array(Vec<Arc<Value>>),Map(Vec<(Arc<Value>,Arc<Value>)>) }
struct Builder;
impl TransitReadBuilder for Builder {
    type Value = Arc<Value>;
    type Array = Vec<Self::Value>;
    type Map = Vec<(Self::Value,Self::Value)>;
    fn null(&mut self) -> Result<Self::Value> { Ok(Arc::new(Value::Scalar(T::Null))) }
    fn boolean(&mut self,value: bool) -> Result<Self::Value> { Ok(Arc::new(Value::Scalar(T::Bool(value)))) }
    fn integer(&mut self,value: i64) -> Result<Self::Value> { Ok(Arc::new(Value::Scalar(T::Integer(value)))) }
    fn float(&mut self,value: f64) -> Result<Self::Value> { Ok(Arc::new(Value::Scalar(T::Float(value)))) }
    fn string(&mut self,value: String) -> Result<Self::Value> { Ok(Arc::new(Value::Scalar(T::String(value)))) }
    fn keyword(&mut self,value: String) -> Result<Self::Value> { Ok(Arc::new(Value::Scalar(T::Keyword(value)))) }
    fn symbol(&mut self,value: String) -> Result<Self::Value> { Ok(Arc::new(Value::Scalar(T::Symbol(value)))) }
    fn bytes(&mut self,value: Vec<u8>) -> Result<Self::Value> { Ok(Arc::new(Value::Scalar(T::Bytes(value)))) }
    fn start_array(&mut self,_tag: Option<&str>,size: usize) -> Result<Self::Array> { Ok(Vec::with_capacity(size)) }
    fn array_item(&mut self,array: &mut Self::Array,value: Self::Value) -> Result<()> { array.push(value);Ok(()) }
    fn finish_array(&mut self,array: Self::Array) -> Result<Self::Value> { Ok(Arc::new(Value::Array(array))) }
    fn start_map(&mut self,size: usize) -> Result<Self::Map> { Ok(Vec::with_capacity(size)) }
    fn map_entry(&mut self,map: &mut Self::Map,key: Self::Value,value: Self::Value) -> Result<()> {
        if matches!(key.as_ref(),Value::Scalar(T::Keyword(key)) if key == "reject") { return Err(Anomaly::incorrect("test/map-insertion","Rejected map key")); }
        map.push((key,value));Ok(())
    }
    fn finish_map(&mut self,map: Self::Map) -> Result<Self::Value> { Ok(Arc::new(Value::Map(map))) }
    fn tagged(&mut self,tag: String,representation: Self::Value) -> Result<Self::Value> {
        if tag == "'" { Ok(representation) } else { Err(Anomaly::incorrect("test/rejected-tag",tag)) }
    }
}

#[test]
fn cache_references_clone_constructed_values_within_one_decode() {
    let wire = T::Array(vec![T::Map(vec![(T::Bytes(vec![1]),T::Integer(1))]),T::Map(vec![(T::Bytes(vec![1]),T::Integer(2))])]);
    for bytes in [encode_msgpack(&wire).unwrap(),br#"[{"~bAQ==":1},{"^0":2}]"#.to_vec()] {
        let first = decode_with(&bytes,&mut Builder).unwrap();
        let Value::Array(values) = first.as_ref() else { panic!("array") };
        let (Value::Map(left),Value::Map(right)) = (values[0].as_ref(),values[1].as_ref()) else { panic!("maps") };
        assert!(Arc::ptr_eq(&left[0].0,&right[0].0));
        assert!(matches!(left[0].0.as_ref(),Value::Scalar(T::Bytes(value)) if value == &[1]));
        let second = decode_with(&bytes,&mut Builder).unwrap();
        let Value::Array(values) = second.as_ref() else { panic!("array") };
        let Value::Map(other) = values[0].as_ref() else { panic!("map") };
        assert!(!Arc::ptr_eq(&left[0].0,&other[0].0));
    }
}

#[test]
fn semantic_builder_failure_is_preserved_after_syntax_selection() {
    let tagged = T::Tagged { tag: "rejected".into(),representation: Box::new(T::Integer(1)) };
    for bytes in [encode_msgpack(&tagged).unwrap(),br#"["~#rejected",1]"#.to_vec()] {
        assert_eq!(decode_with(&bytes,&mut Builder).unwrap_err().code,"test/rejected-tag");
    }
}

#[test]
fn map_insertion_failure_precedes_decoding_later_values() {
    let input = br#"["^ ","~:reject",1,"~:later",["~#rejected",2]]"#;
    assert_eq!(decode_with(input,&mut Builder).unwrap_err().code,"test/map-insertion");
}

#[test]
fn cache_rollover_reuses_slots_without_erasing_unwritten_old_slots() {
    let mut values = (0..44 * 44).map(|index|serde_json::Value::String(format!("~:key{index:04}"))).collect::<Vec<_>>();
    values.push(serde_json::Value::String("~:replacement".into()));
    values.push(serde_json::Value::String("^1".into()));
    let input = serde_json::to_vec(&values).unwrap();
    let object = decode_with(&input,&mut Builder).unwrap();
    let Value::Array(values) = object.as_ref() else { panic!("array") };
    assert!(Arc::ptr_eq(&values[1],values.last().unwrap()));
    assert!(matches!(values[44 * 44].as_ref(),Value::Scalar(T::Keyword(value)) if value == "replacement"));
    assert!(decode_with(br#"["^1"]"#,&mut Builder).is_err());
}
