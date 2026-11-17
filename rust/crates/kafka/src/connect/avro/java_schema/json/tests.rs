use super::*;
use super::super::test_support::*;

#[test]
fn numeric_nodes_preserve_concrete_classes_signed_zero_and_canonical_nan() {
    let mut context=context();let int=node(JsonValue::I32(1),&mut context);let long=node(JsonValue::I64(1),&mut context);let float=node(JsonValue::F32(1.0),&mut context);let double=node(JsonValue::F64(1.0),&mut context);assert!(!int.equal(&long,&mut context,0).unwrap());assert!(!float.equal(&double,&mut context,0).unwrap());let positive=node(JsonValue::F64(0.0),&mut context);let negative=node(JsonValue::F64(-0.0),&mut context);assert!(!positive.equal(&negative,&mut context,0).unwrap());assert_eq!(positive.hash(&mut context,0).unwrap(),0);assert_eq!(negative.hash(&mut context,0).unwrap(),i32::MIN);let first=node(JsonValue::F64(f64::from_bits(0x7ff0000000000001)),&mut context);let second=node(JsonValue::F64(f64::from_bits(0xfff8000000000001)),&mut context);assert!(first.equal(&second,&mut context,0).unwrap());assert_eq!(first.hash(&mut context,0).unwrap(),0x7ff80000);
}

#[test]
fn arbitrary_integer_hash_uses_unsigned_magnitude_words_and_sign() {
    let mut context=context();let positive=JsonNode::integer_decimal(JavaText::from_utf8("4294967296"),&mut context).unwrap();let negative=JsonNode::integer_decimal(JavaText::from_utf8("-4294967296"),&mut context).unwrap();let zero=JsonNode::integer_decimal(JavaText::from_utf8("-000"),&mut context).unwrap();assert_eq!(positive.hash(&mut context,0).unwrap(),31);assert_eq!(negative.hash(&mut context,0).unwrap(),-31);assert_eq!(zero.hash(&mut context,0).unwrap(),0);let same=JsonNode::integer_decimal(JavaText::from_utf8("+0004294967296"),&mut context).unwrap();assert!(positive.equal(&same,&mut context,0).unwrap());assert!(!positive.equal(&negative,&mut context,0).unwrap());let ordinary=node(JsonValue::I64(4294967296),&mut context);assert!(!positive.equal(&ordinary,&mut context,0).unwrap());
}

#[test]
fn decimal_nodes_compare_numeric_value_and_hash_the_demanded_double_projection() {
    let mut context=context();let first=JsonNode::decimal("1.00",&mut context).unwrap();let second=JsonNode::decimal("100e-2",&mut context).unwrap();assert!(first.equal(&second,&mut context,0).unwrap());assert_eq!(first.hash(&mut context,0).unwrap(),1072693248);assert_eq!(first.hash(&mut context,0).unwrap(),second.hash(&mut context,0).unwrap());assert!(!first.is_nan_double(&mut context).unwrap());let mut denied=ConversionContext::with_limit(rtdb_columnar::MemoryBudget::new(usize::MAX),0);assert!(first.hash(&mut denied,0).is_err());assert!(first.is_nan_double(&mut denied).is_err());assert!(first.equal(&second,&mut denied,0).unwrap());
}

#[test]
fn null_boolean_and_isolated_utf16_text_keep_jackson_hashes() {
    let mut context=context();let null=node(JsonValue::Null,&mut context);let yes=node(JsonValue::Bool(true),&mut context);let no=node(JsonValue::Bool(false),&mut context);assert_eq!(null.hash(&mut context,0).unwrap(),4);assert_eq!(yes.hash(&mut context,0).unwrap(),3);assert_eq!(no.hash(&mut context,0).unwrap(),1);let units=[0xd800,65,0xdc00];let value=Text::copy_units(&units,&mut context).unwrap();let json=node(JsonValue::Text(value),&mut context);assert_eq!(json.as_text().unwrap().view().units().collect::<Vec<_>>(),units);assert_eq!(json.hash(&mut context,0).unwrap(),JavaText::from_utf16(&units).java_hash_code());assert!(!json.is_nan_double(&mut context).unwrap());
}

#[test]
fn duplicate_object_names_keep_first_key_position_and_last_value() {
    let mut context=context();let first=text("a",&mut context);let last=text("a",&mut context);let original=first.clone();let one=node(JsonValue::I32(1),&mut context);let two=node(JsonValue::I32(2),&mut context);let three=node(JsonValue::I32(3),&mut context);let other=text("b",&mut context);let members=Members::with(4096,&mut context,||Ok(vec![(first,one),(other,two),(last,three)])).unwrap();assert_eq!(members.len(),2);assert!(members.entry(0).0.same_storage(&original));assert_eq!(members.entry(0).1.hash(&mut context,0).unwrap(),3);assert_eq!(members.entry(1).0.view().units().collect::<Vec<_>>(),vec![98]);let lookup=text("a",&mut context);assert_eq!(members.get(&lookup).unwrap().hash(&mut context,0).unwrap(),3);
}

#[test]
fn array_list_size_checks_precede_child_walk_while_object_maps_ignore_insertion_order() {
    fn nested(context:&mut ConversionContext)->JsonNode {let mut value=node(JsonValue::I32(1),context);for _ in 0..130 {value=node(JsonValue::Array(vec![value]),context);}value}
    let mut context=context();let first=nested(&mut context);let second=nested(&mut context);let left=node(JsonValue::Array(vec![first]),&mut context);let right=node(JsonValue::Array(vec![second,node(JsonValue::Null,&mut context)]),&mut context);assert!(!left.equal(&right,&mut context,0).unwrap());let a=text("a",&mut context);let b=text("b",&mut context);let one=node(JsonValue::I32(1),&mut context);let two=node(JsonValue::I32(2),&mut context);let first=Members::with(4096,&mut context,||Ok(vec![(a.clone(),one.clone()),(b.clone(),two.clone())])).unwrap();let second=Members::with(4096,&mut context,||Ok(vec![(b,two),(a,one)])).unwrap();let first=node(JsonValue::Object(first),&mut context);let second=node(JsonValue::Object(second),&mut context);assert!(first.equal(&second,&mut context,0).unwrap());assert_eq!(first.hash(&mut context,0).unwrap(),second.hash(&mut context,0).unwrap());
}

#[test]
fn failed_admission_prevents_node_payload_construction() {
    let mut denied=ConversionContext::with_limit(rtdb_columnar::MemoryBudget::new(usize::MAX),0);let mut constructed=false;assert!(JsonNode::with(4096,&mut denied,|_| {constructed=true;Ok(JsonValue::Null)}).is_err());assert!(!constructed);assert!(JsonNode::integer_decimal(JavaText::from_utf8("1x"),&mut denied).err().unwrap().code.ends_with("avro-java-schema"));
}

#[test]
fn native_json_hash_equality_and_retention_expose_the_same_demand_depth_boundary() {
    fn arrays(count: usize, context: &mut ConversionContext) -> JsonNode {
        let mut value = node(JsonValue::Null, context);
        for _ in 0..count { value = JsonNode::with(4096, context, |_| Ok(JsonValue::Array(vec![value]))).unwrap(); }
        value
    }
    let mut context = context();
    let edge = arrays(NATIVE_MAX_TRAVERSAL_DEPTH, &mut context);
    let edge_copy = arrays(NATIVE_MAX_TRAVERSAL_DEPTH, &mut context);
    let over = arrays(NATIVE_MAX_TRAVERSAL_DEPTH + 1, &mut context);
    let over_copy = arrays(NATIVE_MAX_TRAVERSAL_DEPTH + 1, &mut context);
    assert!(edge.hash(&mut context, 0).is_ok());
    assert!(edge.equal(&edge_copy, &mut context, 0).unwrap());
    assert!(edge.retain(&mut context, &mut JsonSeen::default(), 0).is_ok());
    assert!(over.hash(&mut context, 0).unwrap_err().code.ends_with("avro-native-traversal-limit"));
    assert!(over.equal(&over_copy, &mut context, 0).unwrap_err().code.ends_with("avro-native-traversal-limit"));
    assert!(over.retain(&mut context, &mut JsonSeen::default(), 0).unwrap_err().code.ends_with("avro-native-traversal-limit"));
    assert!(over.equal(&over, &mut context, 0).unwrap());
}
