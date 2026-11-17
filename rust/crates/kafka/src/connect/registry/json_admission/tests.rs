use super::*;
use std::sync::Arc;
use rtdb_columnar::MemoryBudget;

fn run(bytes: &[u8], limit: usize, inspect: impl FnOnce(Result<Json>)) {
    let budget = MemoryBudget::new(limit);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    inspect(parse(bytes, &mut context));
    drop(context);
    assert_eq!(budget.used_bytes(), 0);
}
fn failure(bytes: &[u8], limit: usize) -> rtdb_types::Anomaly {
    let mut cause = None;
    run(bytes, limit, |value| cause = Some(value.unwrap_err()));
    cause.unwrap()
}

#[test]
fn a_scalar_tree_is_admitted_at_the_exact_boundary_and_rejected_one_byte_below() {
    let bytes = b"null";
    let required = scratch(bytes.len()).unwrap() + node_bytes().unwrap();
    run(bytes, required, |value| assert_eq!(value.unwrap(), Json::Null));
    assert!(failure(bytes, required - 1).code.ends_with("/batch-too-large"));
}

#[test]
fn dense_arrays_admit_each_value_slot_before_materializing_the_tree() {
    let bytes = b"[null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null]";
    let required = scratch(bytes.len()).unwrap() + 18 * node_bytes().unwrap();
    run(bytes, required, |value| assert_eq!(value.unwrap().as_array().unwrap().len(), 17));
    assert!(required > bytes.len() * 4);
    failure(bytes, required - 1);
}

#[test]
fn replaced_object_members_still_admit_their_source_values_and_keys() {
    let bytes = br#"{"k":[null,null],"k":false}"#;
    let required = scratch(bytes.len()).unwrap() + 5 * node_bytes().unwrap() + 2 * (key_bytes().unwrap() + text_bytes(1).unwrap());
    run(bytes, required, |value| assert_eq!(value.unwrap(), serde_json::json!({"k":false})));
    failure(bytes, required - 1);
}

#[test]
fn escaped_strings_charge_decoded_utf8_and_preserve_scalar_and_surrogate_pair_decoding() {
    let bytes = br#""a\n\u20ac\uD83D\uDE00""#;
    let required = scratch(bytes.len()).unwrap() + node_bytes().unwrap() + text_bytes(9).unwrap();
    run(bytes, required, |value| assert_eq!(value.unwrap(), Json::String("a\n€😀".into())));
    failure(bytes, required - 1);
}

#[test]
fn the_raw_value_first_key_keeps_its_nested_parser_and_owned_string_overlap_admitted() {
    let bytes = br#"{"$serde_json::private::RawValue":"[null,null]"}"#;
    let required = scratch(bytes.len()).unwrap() + 4 * node_bytes().unwrap() + key_bytes().unwrap() + text_bytes(RAW_VALUE.len()).unwrap() + text_bytes(11).unwrap() + scratch(11).unwrap();
    run(bytes, required, |value| assert_eq!(value.unwrap(), serde_json::json!([null,null])));
    failure(bytes, required - 1);
}

#[test]
fn later_raw_value_named_members_remain_ordinary_objects() {
    let bytes = br#"{"first":0,"$serde_json::private::RawValue":"[null,null]"}"#;
    run(bytes, 1 << 20, |value| {
        let value = value.unwrap();
        assert_eq!(value["first"], 0);
        assert_eq!(value[RAW_VALUE], "[null,null]");
        assert_eq!(value.as_object().unwrap().keys().map(String::as_str).collect::<Vec<_>>(), ["first",RAW_VALUE]);
    });
}

#[test]
fn nested_raw_value_parses_and_failure_order_follow_the_selected_json_value_deserializer() {
    let nested = serde_json::to_string(&serde_json::json!({RAW_VALUE:"[1,{\"k\":true}]"})).unwrap();
    let bytes = serde_json::to_vec(&serde_json::json!({RAW_VALUE:nested})).unwrap();
    run(&bytes, 1 << 20, |value| assert_eq!(value.unwrap(), serde_json::json!([1,{"k":true}])));
    for bytes in [br#"{"$serde_json::private::RawValue":false}"#.as_slice(), br#"{"$serde_json::private::RawValue":"?","later":false}"#] {
        let expected = serde_json::from_slice::<Json>(bytes).unwrap_err();
        assert_eq!(failure(bytes, 1 << 20).message.as_deref(), Some(expected.to_string().as_str()));
    }
}

#[test]
fn malformed_trailing_invalid_utf8_and_out_of_range_numbers_remain_schema_errors() {
    for bytes in [b"[1,]".as_slice(), b"{}x", b"1e999999", b"\"\xff\"", br#""\uD800""#] {
        let expected = serde_json::from_slice::<Json>(bytes).unwrap_err();
        let cause = failure(bytes, 1 << 20);
        assert!(cause.code.ends_with("/schema-registry"));
        assert_eq!(cause.message.as_deref(), Some(expected.to_string().as_str()));
    }
}

#[test]
fn the_same_json_depth_guard_is_used_by_measurement_and_materialization() {
    let bytes = format!("{}null{}", "[".repeat(130), "]".repeat(130)).into_bytes();
    let expected = serde_json::from_slice::<Json>(&bytes).unwrap_err();
    assert_eq!(failure(&bytes, 1 << 20).message.as_deref(), Some(expected.to_string().as_str()));
}

#[test]
fn scratch_and_tree_resource_failures_retain_their_resource_error_identity() {
    assert!(failure(b"?", 0).code.ends_with("/batch-too-large"));
    let bytes = b"null";
    let required = scratch(bytes.len()).unwrap() + node_bytes().unwrap();
    let budget = MemoryBudget::new(required - 1);
    let mut context = ConversionContext::with_limit(Arc::clone(&budget), required);
    let cause = parse(bytes, &mut context).unwrap_err();
    assert!(!cause.code.ends_with("/schema-registry"));
    drop(context);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn all_admission_arithmetic_is_checked_before_materialization() {
    assert!(scratch(usize::MAX).unwrap_err().code.ends_with("/allocation-overflow"));
    assert!(text_bytes(usize::MAX).is_err());
    assert!(add(usize::MAX, 1).is_err());
    assert!(mul(usize::MAX, 2).is_err());
}
