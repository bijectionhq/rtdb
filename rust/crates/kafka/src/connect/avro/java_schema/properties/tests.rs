use super::*;
use super::super::test_support::*;

fn nested(context:&mut ConversionContext)->JsonNode {let mut value=node(JsonValue::Null,context);for _ in 0..130 {value=node(JsonValue::Array(vec![value]),context);}value}

#[test]
fn property_equality_walks_chm_table_order_before_overridden_entry_set_order() {
    let mut context=context();let left_deep=nested(&mut context);let right_deep=nested(&mut context);let left_one=node(JsonValue::I32(1),&mut context);let right_two=node(JsonValue::I32(2),&mut context);let left=properties(vec![("b",left_deep),("a",left_one)],&mut context);let right=properties(vec![("b",right_deep),("a",right_two)],&mut context);assert_eq!(left.order,vec![1,0]);assert!(!left.equal(&right,&mut context,0).unwrap());
}

#[test]
fn property_equality_has_no_upfront_map_size_shortcut() {
    let mut context=context();let left_deep=nested(&mut context);let right_deep=nested(&mut context);let extra=node(JsonValue::Null,&mut context);let left=properties(vec![("a",left_deep)],&mut context);let right=properties(vec![("a",right_deep),("b",extra)],&mut context);assert!(left.equal(&right,&mut context,0).is_err());let empty=Properties::empty();assert!(!empty.equal(&right,&mut context,0).unwrap());
}

#[test]
fn property_hash_demands_values_in_positive_chm_spread_order() {
    let mut context=context();let decimal=JsonNode::decimal("1.0",&mut context).unwrap();let value=properties(vec![("b",decimal),("a",node(JsonValue::I32(1),&mut context))],&mut context);assert_eq!(value.order,vec![1,0]);let mut denied=ConversionContext::with_limit(rtdb_columnar::MemoryBudget::new(usize::MAX),0);assert!(value.hash(&mut denied,0).is_err());assert_eq!(value.hash(&mut context,0).unwrap(),(97i32^1).wrapping_add(98^1072693248));
}
