use super::*;
use super::super::test_support::*;

#[test]
fn property_lookup_matches_borrowed_utf16_keys() {
    let mut context = context();
    let expected = node(JsonValue::I32(7), &mut context);
    let value = properties(vec![("unit𐐷", expected.clone()), ("scale", node(JsonValue::Null, &mut context))], &mut context);
    assert!(value.get("unit𐐷").unwrap().equal(&expected, &mut context, 0).unwrap());
    assert!(value.get("unit").is_none());
}
