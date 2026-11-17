use super::*;
use rtdb_types::Value;

fn strict(schema: &str) -> JsonPlan {
    JsonPlan::parse(schema, &BTreeMap::new(), &BTreeMap::from([("json.fail.invalid.schema".into(), "true".into())])).unwrap()
}
fn accepts(plan: &JsonPlan, value: &str) -> bool {
    plan.decode(value.as_bytes(), &mut ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 26))).is_ok()
}

#[test]
fn integer_bounds_and_divisibility_keep_adjacent_values_beyond_u64() {
    let bounded = strict(r#"{"type":"integer","minimum":18446744073709551617,"maximum":18446744073709551617}"#);
    assert!(accepts(&bounded, "18446744073709551617"));
    assert!(!accepts(&bounded, "18446744073709551616"));
    assert!(!accepts(&bounded, "18446744073709551618"));
    let exclusive = strict(r#"{"type":"integer","exclusiveMinimum":18446744073709551616,"exclusiveMaximum":18446744073709551618}"#);
    assert!(accepts(&exclusive, "18446744073709551617"));
    assert!(!accepts(&exclusive, "18446744073709551616"));
    assert!(!accepts(&exclusive, "18446744073709551618"));
    let negative = strict(r#"{"type":"integer","maximum":-18446744073709551617}"#);
    assert!(accepts(&negative, "-18446744073709551617"));
    assert!(!accepts(&negative, "-18446744073709551616"));
    let multiple = strict(r#"{"type":"integer","multipleOf":3}"#);
    assert!(accepts(&multiple, "18446744073709551618"));
    assert!(accepts(&multiple, "-18446744073709551618"));
    assert!(!accepts(&multiple, "18446744073709551617"));
    let value = bounded.decode(b"18446744073709551617", &mut ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24))).unwrap().value;
    assert_eq!(value, Value::I64(1));
}

#[test]
fn decimal_constraints_remain_exact_before_connect_double_conversion() {
    let bounded = strict(r#"{"type":"number","minimum":18446744073709551617.01,"maximum":18446744073709551617.01}"#);
    assert!(accepts(&bounded, "18446744073709551617.01"));
    assert!(!accepts(&bounded, "18446744073709551617.00"));
    assert!(!accepts(&bounded, "18446744073709551617.02"));
    let multiple = strict(r#"{"type":"number","multipleOf":0.1}"#);
    assert!(accepts(&multiple, "0.3"));
    assert!(!accepts(&multiple, "0.30000000000000000000000000001"));
    let extreme = strict(r#"{"type":"number","minimum":1e2147483648}"#);
    assert!(!accepts(&extreme, "0"));
}

#[test]
fn numeric_const_enum_nested_equality_and_hashed_uniqueness_remain_exact() {
    let constant = strict(r#"{"type":"integer","const":18446744073709551617}"#);
    assert!(accepts(&constant, "18446744073709551617"));
    assert!(!accepts(&constant, "18446744073709551616"));
    let enumeration = strict(r#"{"type":"integer","enum":[18446744073709551617,18446744073709551619]}"#);
    assert!(accepts(&enumeration, "18446744073709551619"));
    assert!(!accepts(&enumeration, "18446744073709551618"));
    let nested = strict(r#"{"type":"object","properties":{"x":{"type":"integer"}},"const":{"x":18446744073709551617}}"#);
    assert!(accepts(&nested, r#"{"x":18446744073709551617}"#));
    assert!(!accepts(&nested, r#"{"x":18446744073709551616}"#));
    let unique = strict(r#"{"type":"array","items":{"type":"integer"},"uniqueItems":true}"#);
    let prefix = (0..16).map(|value| value.to_string()).collect::<Vec<_>>().join(",");
    assert!(accepts(&unique, &format!("[{prefix},18446744073709551616,18446744073709551617]")));
    assert!(!accepts(&unique, &format!("[{prefix},18446744073709551617,18446744073709551617]")));
}

#[test]
fn referenced_constraints_and_scratch_admission_cover_schema_operands() {
    let options = BTreeMap::from([("json.fail.invalid.schema".into(), "true".into())]);
    let references = BTreeMap::from([("number.json".into(), r#"{"type":"integer","minimum":18446744073709551617}"#.into())]);
    let plan = JsonPlan::parse(r#"{"type":"object","properties":{"x":{"$ref":"number.json"}}}"#, &references, &options).unwrap();
    assert!(accepts(&plan, r#"{"x":18446744073709551617}"#));
    assert!(!accepts(&plan, r#"{"x":18446744073709551616}"#));
    let schema = format!(r#"{{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"integer","multipleOf":{}}}"#, "3".repeat(900));
    let plan = strict(&schema);
    assert!(plan.validation_scratch >= 900 * 512);
    let memory = rtdb_columnar::MemoryBudget::new(1 << 22);
    let mut context = ConversionContext::with_limit(Arc::clone(&memory), 200_000);
    assert!(plan.decode(b"0", &mut context).is_err());
    drop(context);
    assert_eq!(memory.used_bytes(), 0);
}
