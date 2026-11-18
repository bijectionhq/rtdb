use super::*;
use super::super::test_support::*;

#[test]
fn duplicate_object_names_keep_first_key_position_and_last_value() {
    let mut context=context();let first=text("a",&mut context);let last=text("a",&mut context);let original=first.clone();let one=node(JsonValue::I32(1),&mut context);let two=node(JsonValue::I32(2),&mut context);let three=node(JsonValue::I32(3),&mut context);let other=text("b",&mut context);let members=Members::with(4096,&mut context,||Ok(vec![(first,one),(other,two),(last,three)])).unwrap();assert_eq!(members.len(),2);assert!(members.entry(0).0.same_storage(&original));assert_eq!(members.entry(0).1.integral_long(),Some(3));assert_eq!(members.entry(1).0.view().units().collect::<Vec<_>>(),vec![98]);let lookup=text("a",&mut context);assert_eq!(members.get(&lookup).unwrap().integral_long(),Some(3));
}

#[test]
fn failed_admission_prevents_node_payload_construction() {
    let mut denied=ConversionContext::with_limit(rtdb_columnar::MemoryBudget::new(usize::MAX),0);let mut constructed=false;assert!(JsonNode::with(4096,&mut denied,|_| {constructed=true;Ok(JsonValue::Null)}).is_err());assert!(!constructed);assert!(JsonNode::integer_decimal(JavaText::from_utf8("1x"),&mut denied).err().unwrap().code.ends_with("avro-java-schema"));
}
