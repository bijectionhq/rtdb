use super::*;

const DIALECTS: &[(&str, bool)] = &[
    ("http://json-schema.org/draft-04/schema#", false),
    ("http://json-schema.org/draft-06/schema#", false),
    ("http://json-schema.org/draft-07/schema#", false),
    ("https://json-schema.org/draft/2019-09/schema", true),
    ("https://json-schema.org/draft/2020-12/schema", true),
];
fn compile(dialect: &str, body: &str) -> rtdb_types::Result<jsonschema::Validator> {
    let schema = format!(r#"{{"$schema":"{dialect}",{body}}}"#);
    let schema = validation::view(&Json::from_registry_str(&schema)?)?;
    jsonschema::options().with_jvm_registry_semantics().build(&schema).map_err(|cause| error("json-schema", cause))
}
fn valid(validator: &jsonschema::Validator, payload: &str) -> bool {
    validator.is_valid(&validation::view(&Json::from_registry_str(payload).unwrap()).unwrap())
}

#[test]
fn dialect_integer_types_preserve_java_node_classes_and_big_decimal_spelling() {
    for (dialect, modern) in DIALECTS {
        let integer = compile(dialect, r#""type":"integer""#).unwrap();
        let nullable = compile(dialect, r#""type":["integer","null"]"#).unwrap();
        for validator in [&integer, &nullable] {
            assert!(valid(validator, "18446744073709551617"));
            assert_eq!(valid(validator, "1.0"), *modern, "{dialect}");
            assert_eq!(valid(validator, "1.00"), *modern, "{dialect}");
            assert_eq!(valid(validator, "1e-7"), *modern, "{dialect}");
            assert!(!valid(validator, "0.000001"));
            assert!(!valid(validator, "1.0e7"));
        }
        assert!(valid(&nullable, "null"));
    }
}

#[test]
fn dialect_enum_and_const_keep_legacy_big_decimal_scale_and_cross_class_equality() {
    for (dialect, modern) in DIALECTS {
        let enumeration = compile(dialect, r#""enum":[1.0]"#).unwrap();
        assert_eq!(valid(&enumeration, "1.00"), *modern, "{dialect}");
        assert!(valid(&enumeration, "1"));
        assert!(!valid(&enumeration, "1.01"));
        if !dialect.contains("draft-04") {
            let constant = compile(dialect, r#""const":{"x":[1.0]}"#).unwrap();
            assert_eq!(valid(&constant, r#"{"x":[1.00]}"#), *modern, "{dialect}");
            assert!(valid(&constant, r#"{"x":[1]}"#));
        }
    }
}

#[test]
fn dialect_uniqueness_uses_java_hashes_and_indexed_legacy_compatibility() {
    for (dialect, modern) in DIALECTS {
        let unique = compile(dialect, r#""type":"array","uniqueItems":true"#).unwrap();
        assert!(valid(&unique, "[1.0,1.00]"));
        assert_eq!(valid(&unique, "[1,1.0]"), *modern, "{dialect}");
        assert!(!valid(&unique, "[1.0,1.0]"));
        assert_eq!(valid(&unique, "[[1],[1.0]]"), *modern, "{dialect}");
        assert!(valid(&unique, r#"["FB","Ea"]"#));
        assert!(!valid(&unique, r#"["FB","Ea","FB"]"#));
        assert!(valid(&unique, r#"[{"x":1.0,"y":2},{"x":1.00,"y":2.0}]"#));
        if !modern { assert!(!valid(&unique, r#"[{"x":1.0,"y":2},{"x":1.00,"y":2.0},{"x":1,"y":2.00}]"#)); }
        let prefix = (0..16).map(|value| value.to_string()).collect::<Vec<_>>().join(",");
        assert_eq!(valid(&unique, &format!("[{prefix},1.0]")), *modern, "{dialect}");
        assert!(valid(&unique, "[18446744073709551616,18446744073709551617]"));
        assert!(!valid(&unique, "[18446744073709551617,18446744073709551617]"));
    }
}

#[test]
fn count_loaders_keep_strict_legacy_int_nodes_and_modern_signed_narrowing() {
    for (dialect, modern) in DIALECTS {
        for annotation in ["1.5", "4294967297", "18446744073709551617"] {
            let result = compile(dialect, &format!(r#""type":"array","minItems":{annotation}"#));
            if *modern {
                let validator = result.unwrap();
                assert!(!valid(&validator, "[]"));
                assert!(valid(&validator, "[0]"));
            } else { assert!(result.is_err(), "{dialect}"); }
        }
        let negative_max = compile(dialect, r#""type":"array","maxItems":-1"#).unwrap();
        assert!(!valid(&negative_max, "[]"));
        let negative_min = compile(dialect, r#""type":"array","minItems":-1"#).unwrap();
        assert!(valid(&negative_min, "[]"));
        if *modern {
            let maximum = compile(dialect, r#""type":"array","maxItems":2147483648"#).unwrap();
            assert!(!valid(&maximum, "[]"));
            let length = compile(dialect, r#""type":"string","maxLength":4294967297"#).unwrap();
            assert!(valid(&length, r#""😀""#));
            assert!(!valid(&length, r#""😀x""#));
            let properties = compile(dialect, r#""type":"object","maxProperties":4294967296"#).unwrap();
            assert!(valid(&properties, "{}"));
            assert!(!valid(&properties, r#"{"x":0}"#));
        }
    }
}

#[test]
fn modern_contains_distinguishes_an_integer_zero_from_decimal_or_wrapped_zero() {
    for (dialect, modern) in DIALECTS {
        if !modern { continue; }
        let literal = compile(dialect, r#""contains":false,"minContains":0,"maxContains":-1"#).unwrap();
        assert!(valid(&literal, "[]"));
        assert!(!valid(&literal, "[0]"));
        for zero in ["0.0", "4294967296", "18446744073709551616"] {
            let validator = compile(dialect, &format!(r#""contains":false,"minContains":{zero}"#)).unwrap();
            assert!(!valid(&validator, "[]"));
            assert!(valid(&validator, "[0]"));
        }
        let ignored = compile(dialect, r#""contains":true,"minContains":"ignored","maxContains":null"#).unwrap();
        assert!(!valid(&ignored, "[]"));
        assert!(valid(&ignored, "[0]"));
    }
}

#[test]
fn multiple_of_loader_double_projection_is_separate_from_exact_runtime_arithmetic() {
    for (dialect, modern) in DIALECTS {
        let negative = compile(dialect, r#""multipleOf":-3"#).unwrap();
        assert!(valid(&negative, "18446744073709551618"));
        assert!(!valid(&negative, "18446744073709551617"));
        for number in ["1e-400", "1e400", "0"] {
            let result = compile(dialect, &format!(r#""multipleOf":{number}"#));
            if *modern { assert_eq!(valid(&result.unwrap(), "0"), number != "0"); }
            else { assert!(result.is_err(), "{dialect}"); }
        }
    }
}

#[test]
fn numeric_schema_accommodation_preserves_existing_structure_checks_and_data_literals() {
    for (dialect, modern) in DIALECTS {
        for body in [r#""required":"x""#, r#""required":["x","x"]"#, r#""type":["integer","integer"]"#, r#""type":"unsupported""#, r#""properties":[]"#, r#""minItems":"x""#] {
            assert!(compile(dialect, body).is_err(), "{dialect}: {body}");
        }
        assert_eq!(compile(dialect, r#""enum":[1,1]"#).is_ok(), *modern);
        let literals = compile(dialect, r#""enum":[{"minItems":1.5},{"minItems":2.5}],"default":{"minItems":1.5}"#).unwrap();
        assert!(valid(&literals, r#"{"minItems":1.5}"#));
        assert!(valid(&literals, r#"{"minItems":2.5}"#));
        assert!(!valid(&literals, r#"{"minItems":0}"#));
    }
    let nested = compile(DIALECTS[4].0, r#""type":"object","properties":{"x":{"type":"array","minItems":1.5}}"#).unwrap();
    assert!(!valid(&nested, r#"{"x":[]}"#));
    assert!(valid(&nested, r#"{"x":[0]}"#));
}

#[test]
fn named_resources_without_a_dialect_inherit_the_registry_root_numeric_policy() {
    for (dialect, modern) in DIALECTS {
        let root = format!(r#"{{"$schema":"{dialect}","type":"object","properties":{{"x":{{"$ref":"value.json"}}}}}}"#);
        let references = BTreeMap::from([("value.json".into(), r#"{"type":"number","enum":[1.0]}"#.into())]);
        let options = BTreeMap::from([("json.fail.invalid.schema".into(), "true".into())]);
        let plan = JsonPlan::parse(&root, &references, &options).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        assert_eq!(plan.decode(br#"{"x":1.00}"#, &mut context).is_ok(), *modern, "{dialect}");
    }
}
