use std::{collections::BTreeMap, sync::Arc};
use rtdb_types::Result;
use super::json_value::JsonValue as Json;
use super::{ConnectData, ConnectSchema, ConversionContext, error};

mod graph;
mod validation;
#[cfg(test)] mod validation_tests;
#[cfg(test)] mod dialect_tests;

pub(super) struct JsonPlan { schema: Arc<ConnectSchema>, validator: Option<jsonschema::Validator>, validation_scratch: usize }
pub(super) struct JsonDatum { pub value: Json, source_bytes: usize }
impl JsonDatum {
    pub fn decode(bytes: &[u8], context: &mut ConversionContext) -> Result<Self> {
        context.reserve(bytes.len().checked_add(1).and_then(|bytes| bytes.checked_mul(512)).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(|| error("allocation-overflow", "JSON datum allocation overflow"))?)?;
        let value = Json::from_registry_slice(bytes).map_err(|cause| error("conversion-error", cause))?;
        Ok(Self { value, source_bytes: bytes.len() })
    }
}
impl JsonPlan {
    #[cfg(test)]
    pub fn parse(text: &str, references: &BTreeMap<String, String>, options: &BTreeMap<String, String>) -> Result<Self> {
        Self::parse_version(text, references, options, None)
    }
    pub fn parse_version(text: &str, references: &BTreeMap<String, String>, options: &BTreeMap<String, String>, version: Option<i32>) -> Result<Self> {
        let root: Json = Json::from_registry_str(text).map_err(|cause| error("json-schema", cause))?;
        let references = references.iter().map(|(name, text)| Ok((name.clone(), Json::from_registry_str(text).map_err(|cause| error("json-schema", cause))?))).collect::<Result<BTreeMap<_, _>>>()?;
        let validator = if super::converter::option_bool(options, "json.fail.invalid.schema", false)? {
            let validation_root = validation::view(&root)?;
            let draft = jsonschema::Draft::Draft7.detect(&validation_root).map_err(|cause| error("json-schema", cause))?;
            let mut validation = jsonschema::options().with_jvm_registry_semantics().with_draft(draft).should_validate_formats(true);
            for (name, schema) in &references {
                let schema = validation::view(schema)?;
                let resource = draft.detect(&schema).map_err(|cause| error("json-schema", cause))?.create_resource(schema);
                validation = validation.with_resource(name.clone(), resource);
            }
            Some(validation.build(&validation_root).map_err(|cause| error("json-schema", cause))?)
        } else { None };
        let validation_scratch = if validator.is_some() {
            references.values().try_fold(root.encoded_bound()?, |size, value| size.checked_add(value.encoded_bound()?).ok_or_else(|| error("allocation-overflow", "JSON validation schema size overflow")))?.checked_mul(512).ok_or_else(|| error("allocation-overflow", "JSON validation scratch overflow"))?
        } else { 0 };
        let schema = graph::build(&root, &references, options, version)?;
        Ok(Self { schema, validator, validation_scratch })
    }
    #[cfg(test)]
    pub fn decode(&self, bytes: &[u8], context: &mut ConversionContext) -> Result<ConnectData> {
        self.convert(JsonDatum::decode(bytes, context)?, context)
    }
    pub fn convert(&self, datum: JsonDatum, context: &mut ConversionContext) -> Result<ConnectData> {
        let bound = datum.source_bytes.checked_add(1).and_then(|nodes| nodes.checked_mul(512usize.saturating_add(self.schema.view_allocation_bound()))).and_then(|bytes| bytes.checked_add(self.schema.retained_bytes().ok()?.checked_mul(4)?)).and_then(|bytes| bytes.checked_add(self.validation_scratch)).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(|| error("allocation-overflow", "JSON Schema decoded size overflow"))?;
        context.reserve(bound)?;
        if let Some(validator) = &self.validator { validator.validate(&validation::view(&datum.value)?).map_err(|cause| error("conversion-error", cause))?; }
        let value = super::converter::convert_registry(&self.schema, &datum.value, bound)?;
        Ok(ConnectData { schema: Some(Arc::clone(&self.schema)), value })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::{TimeUnit, Value};
    #[test]
    fn recursive_json_schema_values_keep_local_document_references() {
        let plan = JsonPlan::parse(r##"{"type":"object","title":"Node","properties":{"value":{"type":"integer"},"next":{"oneOf":[{"type":"null"},{"$ref":"#"}]}}}"##, &BTreeMap::new(), &BTreeMap::new()).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let Value::Struct(value) = plan.decode(br#"{"value":1,"next":{"value":2,"next":null}}"#, &mut context).unwrap().value else { panic!("expected Struct") };
        assert_eq!(value["next"], Value::Struct(BTreeMap::from([("value".into(), Value::I64(2)), ("next".into(), Value::Null)])));
        let next = plan.schema.field("next").unwrap().schema; drop(plan);
        assert!(next.optional); assert!(next.field("value").is_some());
        assert_eq!(next.parameters["io.confluent.connect.json.Id"], "#id1");
    }
    #[test]
    fn generalized_json_unions_skip_null_field_positions_and_keep_singletons() {
        let options = BTreeMap::from([("generalized.sum.type.support".into(), "true".into()), ("flatten.singleton.unions".into(), "false".into())]);
        let plan = JsonPlan::parse(r#"{"oneOf":[{"type":"null"},{"type":"integer"},{"type":"string"}]}"#, &BTreeMap::new(), &options).unwrap();
        assert_eq!(plan.schema.parameters["org.apache.kafka.connect.data.Union"], "connect_union_0");
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        assert_eq!(plan.decode(b"42", &mut context).unwrap().value, Value::Struct(BTreeMap::from([("connect_union_field_0".into(), Value::I64(42)), ("connect_union_field_1".into(), Value::Null)])));
        let singleton = JsonPlan::parse(r#"{"oneOf":[{"type":"string"}]}"#, &BTreeMap::new(), &options).unwrap();
        assert_eq!(singleton.decode(br#""text""#, &mut context).unwrap().value, Value::Struct(BTreeMap::from([("connect_union_field_0".into(), Value::Utf8("text".into()))])));
        let enumeration = JsonPlan::parse(r#"{"enum":["A","B",null]}"#, &BTreeMap::new(), &options).unwrap();
        assert_eq!(enumeration.schema.parameters["org.apache.kafka.connect.data.Enum.B"], "1");
        assert_eq!(enumeration.schema.parameters["org.apache.kafka.connect.data.Enum.<NULL>"], "2");
    }
    #[test]
    fn json_union_candidates_distinguish_integral_numbers_binary_nodes_and_map_arrays() {
        let options = BTreeMap::from([("generalized.sum.type.support".into(), "true".into())]);
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let numbers = JsonPlan::parse(r#"{"oneOf":[{"type":"integer"},{"type":"number"}]}"#, &BTreeMap::new(), &options).unwrap();
        assert_eq!(numbers.decode(b"1.5", &mut context).unwrap().value, Value::Struct(BTreeMap::from([("connect_union_field_0".into(), Value::Null), ("connect_union_field_1".into(), Value::F64(1.5))])));
        let text = JsonPlan::parse(r#"{"oneOf":[{"type":"string","connect.type":"bytes"},{"type":"string"}]}"#, &BTreeMap::new(), &options).unwrap();
        assert_eq!(text.decode(br#""YQ==""#, &mut context).unwrap().value, Value::Struct(BTreeMap::from([("connect_union_field_0".into(), Value::Null), ("connect_union_field_1".into(), Value::Utf8("YQ==".into()))])));
        let map = JsonPlan::parse(r#"{"oneOf":[{"type":"array","connect.type":"map","items":{"type":"object","properties":{"key":{"type":"integer"},"value":{"type":"string"}}}},{"type":"string"}]}"#, &BTreeMap::new(), &options).unwrap();
        assert_eq!(map.decode(br#"[{"key":1,"value":"x"}]"#, &mut context).unwrap().value, Value::Struct(BTreeMap::from([("connect_union_field_0".into(), Value::Map { entries: vec![(Value::I64(1), Value::Utf8("x".into()))], sorted: false }), ("connect_union_field_1".into(), Value::Null)])));
    }
    #[test]
    fn registry_big_integer_conversion_and_union_selection_retain_integral_nodes() {
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let integer = JsonPlan::parse(r#"{"type":"integer","default":18446744073709551617}"#, &BTreeMap::new(), &BTreeMap::new()).unwrap();
        assert_eq!(integer.decode(b"18446744073709551617", &mut context).unwrap().value, Value::I64(1));
        assert_eq!(integer.decode(b"null", &mut context).unwrap().value, Value::I64(1));
        let narrow = JsonPlan::parse(r#"{"type":"integer","connect.type":"int32"}"#, &BTreeMap::new(), &BTreeMap::new()).unwrap();
        assert_eq!(narrow.decode(b"4294967297", &mut context).unwrap().value, Value::I32(1));
        let union = JsonPlan::parse(r#"{"oneOf":[{"type":"integer"},{"type":"number"}]}"#, &BTreeMap::new(), &BTreeMap::from([("generalized.sum.type.support".into(), "true".into())])).unwrap();
        assert_eq!(union.decode(b"18446744073709551617", &mut context).unwrap().value, Value::Struct(BTreeMap::from([("connect_union_field_0".into(), Value::I64(1)), ("connect_union_field_1".into(), Value::Null)])));
        assert_eq!(union.decode(b"1.0", &mut context).unwrap().value, Value::Struct(BTreeMap::from([("connect_union_field_0".into(), Value::Null), ("connect_union_field_1".into(), Value::F64(1.0))])));
        for annotation in ["2147483648", "18446744073709551617", "1.0"] {
            assert!(JsonPlan::parse(&format!(r#"{{"type":"integer","connect.version":{annotation}}}"#), &BTreeMap::new(), &BTreeMap::new()).is_err());
            assert!(JsonPlan::parse(&format!(r#"{{"type":"object","properties":{{"x":{{"type":"integer","connect.index":{annotation}}}}}}}"#), &BTreeMap::new(), &BTreeMap::new()).is_err());
        }
    }
    #[test]
    fn registry_decimal_conversion_and_defaults_use_big_decimal_accessors() {
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let plan = JsonPlan::parse(r#"{"type":"integer","default":18446744073709551617.99}"#, &BTreeMap::new(), &BTreeMap::new()).unwrap();
        assert_eq!(plan.decode(b"18446744073709551617.99", &mut context).unwrap().value, Value::I64(1));
        assert_eq!(plan.decode(b"null", &mut context).unwrap().value, Value::I64(1));
        let float = JsonPlan::parse(r#"{"type":"number","connect.type":"float32"}"#, &BTreeMap::new(), &BTreeMap::new()).unwrap();
        assert_eq!(float.decode(b"18014399583223809.0", &mut context).unwrap().value, Value::F32(f32::from_bits(0x5a800001)));
    }
    #[test]
    fn lossless_json_admission_precedes_parsing_and_retains_the_record_charge() {
        let memory = rtdb_columnar::MemoryBudget::new(1 << 20);
        let mut limited = ConversionContext::with_limit(Arc::clone(&memory), 1);
        assert!(JsonDatum::decode(b"18446744073709551617", &mut limited).is_err());
        assert_eq!(memory.used_bytes(), 0);
        let mut context = ConversionContext::new(Arc::clone(&memory));
        let datum = JsonDatum::decode(b"18446744073709551617", &mut context).unwrap();
        assert_eq!(datum.value.long_value(), Some(1));
        assert!(memory.used_bytes() > 0);
        drop(datum); drop(context); assert_eq!(memory.used_bytes(), 0);
    }
    #[test]
    fn json_union_struct_matching_includes_nested_properties() {
        let schema = r#"{"oneOf":[{"type":"object","properties":{"child":{"type":"object","properties":{"a":{"type":"integer"}}}}},{"type":"object","properties":{"child":{"type":"object","properties":{"b":{"type":"integer"}}}}}]}"#;
        let plan = JsonPlan::parse(schema, &BTreeMap::new(), &BTreeMap::from([("generalized.sum.type.support".into(), "true".into())])).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let Value::Struct(value) = plan.decode(br#"{"child":{"b":2}}"#, &mut context).unwrap().value else { panic!("expected union Struct") };
        assert_eq!(value["connect_union_field_0"], Value::Null);
        assert_eq!(value["connect_union_field_1"], Value::Struct(BTreeMap::from([("child".into(), Value::Struct(BTreeMap::from([("b".into(), Value::I64(2))])))])));
    }
    #[test]
    fn json_parent_defaults_observe_completed_child_defaults() {
        let plan = JsonPlan::parse(r#"{"type":"object","default":{},"properties":{"child":{"type":"string","default":"x"}}}"#, &BTreeMap::new(), &BTreeMap::new()).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        assert_eq!(plan.decode(b"null", &mut context).unwrap().value, Value::Struct(BTreeMap::from([("child".into(), Value::Utf8("x".into()))])));
    }
    #[test]
    fn json_default_structs_and_arrays_reject_required_null_values() {
        assert!(JsonPlan::parse(r#"{"type":"object","default":{},"properties":{"child":{"type":"string"}}}"#, &BTreeMap::new(), &BTreeMap::new()).is_err());
        assert!(JsonPlan::parse(r#"{"type":"array","items":{"type":"string"},"default":[null]}"#, &BTreeMap::new(), &BTreeMap::new()).is_err());
        assert!(JsonPlan::parse(r#"{"type":"array","items":{"type":["null","string"]},"default":[null]}"#, &BTreeMap::new(), &BTreeMap::new()).is_ok());
    }
    #[test]
    fn json_schema_payload_validation_is_disabled_by_default() {
        let schema = r#"{"type":"integer","minimum":10}"#;
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let permissive = JsonPlan::parse(schema, &BTreeMap::new(), &BTreeMap::new()).unwrap();
        assert_eq!(permissive.decode(b"1", &mut context).unwrap().value, Value::I64(1));
        let strict = JsonPlan::parse(schema, &BTreeMap::new(), &BTreeMap::from([("json.fail.invalid.schema".into(), "true".into())])).unwrap();
        assert!(strict.decode(b"1", &mut context).is_err());
    }
    #[test]
    fn json_schema_titles_select_connect_logical_types_and_format_only_remains_text() {
        let schema = r#"{"type":"object","properties":{"when":{"type":"integer","title":"org.apache.kafka.connect.data.Timestamp"},"day":{"type":"string","format":"date"},"optional":{"type":["null","string"]}}}"#;
        let plan = JsonPlan::parse(schema, &BTreeMap::new(), &BTreeMap::from([("json.fail.invalid.schema".into(), "true".into())])).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let Value::Struct(values) = plan.decode(br#"{"when":1234,"day":"2024-06-08","optional":null}"#, &mut context).unwrap().value else { panic!("expected Struct") };
        assert_eq!(values["when"], Value::Timestamp { value: 1234, unit: TimeUnit::Millisecond, timezone: Some("UTC".into()) });
        assert_eq!(values["day"], Value::Utf8("2024-06-08".into())); assert_eq!(values["optional"], Value::Null);
        assert!(plan.decode(br#"{"when":"wrong","day":"not-a-date"}"#, &mut context).is_err());
    }
    #[test]
    fn absent_json_schema_fields_become_null_and_named_references_resolve() {
        let schema = r#"{"type":"object","properties":{"value":{"$ref":"nested.json"},"missing":{"type":"integer"}}}"#;
        let references = BTreeMap::from([("nested.json".into(), r#"{"type":"string"}"#.into())]);
        let plan = JsonPlan::parse(schema, &references, &BTreeMap::new()).unwrap();
        let mut context = ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 24));
        let Value::Struct(values) = plan.decode(br#"{"value":"text"}"#, &mut context).unwrap().value else { panic!("expected Struct") };
        assert_eq!(values["missing"], Value::Null); assert_eq!(values["value"], Value::Utf8("text".into()));
    }
}
