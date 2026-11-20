use rtdb_types::StructFields;
use super::super::AvroPlan;
use rtdb_types::Value;

#[test]
fn record_default_source_members_require_raw_fields_before_destination_fallback() {
    for (default,expected) in [
        (r#"{}"#,Some("fallback")),
        (r#"{"actual":"given"}"#,Some("given")),
        (r#"{"alias":"ignored"}"#,None),
        (r#"{"unknown":"ignored"}"#,None),
    ] {
        let schema=format!(r#"{{"type":"record","name":"Outer","fields":[{{"name":"nested","type":{{"type":"record","name":"Inner","fields":[{{"name":"actual","aliases":["alias"],"type":"string","default":"fallback"}}]}},"default":{default}}}]}}"#);
        let plan=AvroPlan::parse(&schema,&[]).unwrap();
        let nested=plan.connect.field("nested").unwrap().schema;
        let expected=expected.map(|value| Value::Struct(StructFields::from([("actual".into(),Value::Utf8(value.into()))])));
        assert_eq!(nested.default,expected,"{default}");
    }
}

#[test]
fn an_alias_overwriting_the_native_index_does_not_hide_a_real_default_source_field() {
    let schema=r#"{"type":"record","name":"Outer","fields":[{"name":"nested","type":{"type":"record","name":"Inner","fields":[{"name":"actual","type":"string","default":"fallback"},{"name":"other","aliases":["actual"],"type":"string","default":"other fallback"}]},"default":{"actual":"given"}}]}"#;
    let plan=AvroPlan::parse(schema,&[]).unwrap();
    let nested=plan.connect.field("nested").unwrap().schema;
    assert_eq!(nested.default,Some(Value::Struct(StructFields::from([("actual".into(),Value::Utf8("given".into())),("other".into(),Value::Utf8("other fallback".into()))]))));
}
