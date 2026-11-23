use rtdb_types::StructFields;
use std::{collections::BTreeMap, sync::Arc};
use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConnectConfig, ConnectData, ConnectField, ConnectSchema, ConversionContext, Converter, ConverterFactory, DateEnvironment, DateEnvironmentInput, PluginRegistry, PluginSpec, SchemaKind, SinkRecord, TransformChain};
use rtdb_types::{TimeUnit, Value};

fn chain(class: &str, options: &[(&str, &str)], memory: &Arc<MemoryBudget>) -> TransformChain {
    let mut properties = BTreeMap::from([
        ("key.converter".into(), "org.apache.kafka.connect.storage.StringConverter".into()),
        ("value.converter".into(), "org.apache.kafka.connect.json.JsonConverter".into()),
        ("transforms".into(), "convert".into()),
        ("transforms.convert.type".into(), format!("org.apache.kafka.connect.transforms.{class}$Value")),
    ]);
    properties.extend(options.iter().map(|(key, value)| (format!("transforms.convert.{key}"), (*value).into())));
    let environment = DateEnvironment::from_inputs(DateEnvironmentInput {
        language: "en".into(), script: String::new(), region: "US".into(), variant: String::new(), unicode: BTreeMap::new(), timezone: "UTC".into(),
        initialized_at: 1_600_000_000_000, observed_at: 1_600_000_000_000, constructed_at: 1_600_000_000_000,
    }, memory).unwrap();
    PluginRegistry::default().chain_with_environment(&ConnectConfig::parse(&properties).unwrap(), environment).unwrap()
}

fn record(schema: &Arc<ConnectSchema>, value: Value) -> SinkRecord {
    SinkRecord::new("events".into(), 0, 0, ConnectData::null(), ConnectData { schema: Some(Arc::clone(schema)), value })
}

fn date(millis: i64) -> Value { Value::Timestamp { value: millis, unit: TimeUnit::Millisecond, timezone: Some("UTC".into()) } }

fn logical_date(default: i64) -> Arc<ConnectSchema> {
    let mut schema = ConnectSchema::new(SchemaKind::I32);
    schema.name = Some("org.apache.kafka.connect.data.Date".into());
    schema.default = Some(date(default));
    Arc::new(schema)
}

fn date_field() -> Arc<ConnectSchema> {
    Arc::new(ConnectSchema::new(SchemaKind::Struct(vec![ConnectField { name: "when".into(), schema: logical_date(0) }])))
}

fn date_row(millis: i64) -> Value { Value::Struct(StructFields::from([("when".into(), date(millis))])) }

#[test]
fn timestamp_converter_rejects_non_struct_rows_before_parsing_schema_defaults() {
    let memory = MemoryBudget::new(256 << 20);
    let mut chain = chain("TimestampConverter", &[("field", "when"), ("target.type", "Timestamp"), ("format", "yyyy")], &memory);
    let mut schema = ConnectSchema::new(SchemaKind::Struct(vec![ConnectField { name: "when".into(), schema: Arc::new(ConnectSchema::new(SchemaKind::String)) }]));
    schema.default = Some(Value::Struct(StructFields::from([("when".into(), Value::Utf8("invalid".into()))])));
    let schema = Arc::new(schema);
    let baseline = memory.used_bytes();
    for value in [Value::I64(7), Value::Map { entries: vec![(Value::Utf8("when".into()), Value::Utf8("1970".into()))], sorted: false }] {
        let error = chain.apply(record(&schema, value)).err().unwrap();
        assert_eq!(error.message.as_deref(), Some("Field TimestampConverter requires a Struct or null"));
        assert_eq!(memory.used_bytes(), baseline);
    }
    let error = chain.apply(record(&schema, Value::Null)).err().unwrap();
    assert_ne!(error.message.as_deref(), Some("Field TimestampConverter requires a Struct or null"));
    assert_eq!(memory.used_bytes(), baseline);
    drop(chain);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn timestamp_converter_converts_schema_defaults_for_null_rows_and_reuses_them() {
    let memory = MemoryBudget::new(256 << 20);
    let mut chain = chain("TimestampConverter", &[("field", "when"), ("target.type", "Timestamp"), ("format", "yyyy")], &memory);
    let mut schema = ConnectSchema::new(SchemaKind::Struct(vec![ConnectField { name: "when".into(), schema: Arc::new(ConnectSchema::new(SchemaKind::String)) }]));
    schema.optional = true;
    schema.default = Some(Value::Struct(StructFields::from([("when".into(), Value::Utf8("1970".into()))])));
    let schema = Arc::new(schema);
    let first = chain.apply(record(&schema, Value::Null)).unwrap().unwrap();
    let retained = memory.used_bytes();
    let next = chain.apply(record(&schema, Value::Null)).unwrap().unwrap();
    assert_eq!(memory.used_bytes(), retained);
    assert_eq!(first.value.value, Value::Null);
    assert_eq!(first.value.schema.as_ref().unwrap().default, Some(Value::Struct(StructFields::from([("when".into(), date(0))]))));
    assert!(Arc::ptr_eq(first.value.schema.as_ref().unwrap(), next.value.schema.as_ref().unwrap()));
    drop(chain);
    assert!(memory.used_bytes() > 0);
    let retained = memory.used_bytes();
    drop(first);
    assert_eq!(memory.used_bytes(), retained);
    drop(next);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn cast_converts_all_schema_defaults_before_any_row_fields() {
    let memory = MemoryBudget::new(256 << 20);
    let mut chain = chain("Cast", &[("spec", "number:int64,when:int64")], &memory);
    let schema = Arc::new(ConnectSchema::new(SchemaKind::Struct(vec![
        ConnectField { name: "number".into(), schema: Arc::new(ConnectSchema::new(SchemaKind::String)) },
        ConnectField { name: "when".into(), schema: logical_date(1) },
    ])));
    let baseline = memory.used_bytes();
    let value = Value::Struct(StructFields::from([("number".into(), Value::Utf8("invalid".into())), ("when".into(), date(0))]));
    assert_eq!(chain.apply(record(&schema, value)).err().unwrap().message.as_deref(), Some("Connect Date contains nonzero time fields"));
    assert_eq!(memory.used_bytes(), baseline);
    drop(chain);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn cast_builds_whole_schema_defaults_before_values_and_keeps_null_records_unchanged() {
    let memory = MemoryBudget::new(256 << 20);
    let mut chain = chain("Cast", &[("spec", "int64")], &memory);
    let schema = logical_date(1);
    let baseline = memory.used_bytes();
    assert_eq!(chain.apply(record(&schema, Value::Utf8("invalid".into()))).err().unwrap().message.as_deref(), Some("Connect Date contains nonzero time fields"));
    let null = chain.apply(record(&schema, Value::Null)).unwrap().unwrap();
    assert!(Arc::ptr_eq(null.value.schema.as_ref().unwrap(), &schema));
    assert_eq!(memory.used_bytes(), baseline);
    drop(chain);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn cast_schema_cache_reuses_evicts_and_keeps_record_allocations_alive() {
    let memory = MemoryBudget::new(256 << 20);
    let mut chain = chain("Cast", &[("spec", "when:string")], &memory);
    let schema = date_field();
    let first = chain.apply(record(&schema, date_row(0))).unwrap().unwrap();
    let retained = memory.used_bytes();
    let next = chain.apply(record(&schema, date_row(0))).unwrap().unwrap();
    assert_eq!(memory.used_bytes(), retained);
    assert_eq!(first.value.value, Value::Struct(StructFields::from([("when".into(), Value::Utf8("00:00:00.000Z".into()))])));
    assert_eq!(first.value.schema.as_ref().unwrap().field("when").unwrap().schema.default, Some(Value::Utf8("00:00:00.000Z".into())));
    assert!(Arc::ptr_eq(first.value.schema.as_ref().unwrap(), next.value.schema.as_ref().unwrap()));
    drop(next);
    assert_eq!(memory.used_bytes(), retained);
    for index in 0..16 {
        let mut other = schema.as_ref().clone();
        other.doc = Some(index.to_string());
        chain.apply(record(&Arc::new(other), date_row(0))).unwrap();
    }
    let replaced = chain.apply(record(&schema, date_row(0))).unwrap().unwrap();
    assert!(!Arc::ptr_eq(first.value.schema.as_ref().unwrap(), replaced.value.schema.as_ref().unwrap()));
    drop((replaced, chain));
    assert!(memory.used_bytes() > 0);
    drop(first);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn cast_keeps_a_complete_cached_schema_after_row_failure() {
    let memory = MemoryBudget::new(256 << 20);
    let mut chain = chain("Cast", &[("spec", "when:int64")], &memory);
    let schema = date_field();
    let baseline = memory.used_bytes();
    assert!(chain.apply(record(&schema, date_row(1))).is_err());
    let retained = memory.used_bytes();
    assert!(retained > baseline);
    assert!(chain.apply(record(&schema, date_row(1))).is_err());
    assert_eq!(memory.used_bytes(), retained);
    let next = chain.apply(record(&schema, date_row(0))).unwrap().unwrap();
    assert_eq!(next.value.value, Value::Struct(StructFields::from([("when".into(), Value::I64(0))])));
    drop((next, chain));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn whole_cast_validates_converted_defaults_against_the_retained_logical_name() {
    let memory = MemoryBudget::new(256 << 20);
    for target in ["int64", "string"] {
        let mut chain = chain("Cast", &[("spec", target)], &memory);
        let baseline = memory.used_bytes();
        let error = chain.apply(record(&logical_date(0), date(0))).err().unwrap();
        assert_eq!(error.message.as_deref(), Some("Value does not match its Connect schema"));
        assert_eq!(memory.used_bytes(), baseline);
        drop(chain);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn date_field_transforms_validate_required_nulls_after_default_replacement_selection() {
    let memory = MemoryBudget::new(256 << 20);
    let mut timestamp_field = ConnectSchema::new(SchemaKind::String);
    timestamp_field.default = Some(Value::Utf8("1970".into()));
    let timestamp = Arc::new(ConnectSchema::new(SchemaKind::Struct(vec![ConnectField { name: "when".into(), schema: Arc::new(timestamp_field) }])));
    for replace in ["true", "false"] {
        for (class, schema, mut options) in [
            ("TimestampConverter", Arc::clone(&timestamp), vec![("field", "when"), ("target.type", "Timestamp"), ("format", "yyyy")]),
            ("Cast", date_field(), vec![("spec", "when:string")]),
        ] {
            options.push(("replace.null.with.default", replace));
            let mut chain = chain(class, &options, &memory);
            let result = chain.apply(record(&schema, Value::Struct(StructFields::new())));
            if replace == "true" { assert!(result.is_ok()); }
            else { assert_eq!(result.err().unwrap().message.as_deref(), Some("Value does not match its Connect schema")); }
        }
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn date_schema_cache_allocation_failure_releases_partial_state_and_allows_retry() {
    let memory = MemoryBudget::new(256 << 20);
    let timestamp = Arc::new(ConnectSchema::new(SchemaKind::Struct(vec![ConnectField { name: "when".into(), schema: Arc::new(ConnectSchema::new(SchemaKind::String)) }])));
    for (class, schema, value, options) in [
        ("TimestampConverter", timestamp, Value::Struct(StructFields::from([("when".into(), Value::Utf8("1970".into()))])), vec![("field", "when"), ("target.type", "Timestamp"), ("format", "yyyy")]),
        ("Cast", date_field(), date_row(0), vec![("spec", "when:string")]),
    ] {
        let mut chain = chain(class, &options, &memory);
        let blocker = memory.reserve(memory.limit_bytes() - memory.used_bytes() - 1024).unwrap();
        let blocked = memory.used_bytes();
        assert!(chain.apply(record(&schema, value.clone())).is_err());
        assert_eq!(memory.used_bytes(), blocked);
        drop(blocker);
        let output = chain.apply(record(&schema, value)).unwrap().unwrap();
        drop((output, chain));
        assert_eq!(memory.used_bytes(), 0);
    }
}

fn optional_schema_with_null_defaults(explicit: bool) -> Arc<ConnectSchema> {
    let optional = |kind| {
        let mut schema = ConnectSchema::new(kind);
        schema.optional = true;
        schema.default = explicit.then_some(Value::Null);
        Arc::new(schema)
    };
    let element = optional(SchemaKind::String);
    let array = optional(SchemaKind::Array(Arc::clone(&element)));
    let map = optional(SchemaKind::Map(element, array));
    let nested = optional(SchemaKind::Struct(vec![ConnectField { name: "items".into(), schema: map }]));
    optional(SchemaKind::Struct(vec![
        ConnectField { name: "when".into(), schema: optional(SchemaKind::String) },
        ConnectField { name: "nested".into(), schema: nested },
    ]))
}

#[test]
fn schema_caches_treat_absent_and_explicit_null_defaults_as_equal_recursively() {
    let absent = optional_schema_with_null_defaults(false);
    let explicit = optional_schema_with_null_defaults(true);
    assert_eq!(absent, explicit);
    for (class, options) in [
        ("TimestampConverter", vec![("field", "when"), ("target.type", "Timestamp"), ("format", "yyyy")]),
        ("Cast", vec![("spec", "when:string")]),
    ] {
        for (first_schema, next_schema) in [(&absent, &explicit), (&explicit, &absent)] {
            let memory = MemoryBudget::new(256 << 20);
            let mut chain = chain(class, &options, &memory);
            let value = || Value::Struct(StructFields::from([("when".into(), Value::Utf8("1970".into()))]));
            let first = chain.apply(record(first_schema, value())).unwrap().unwrap();
            let retained = memory.used_bytes();
            let next = chain.apply(record(next_schema, value())).unwrap().unwrap();
            assert_eq!(memory.used_bytes(), retained);
            assert!(Arc::ptr_eq(first.value.schema.as_ref().unwrap(), next.value.schema.as_ref().unwrap()));
            assert!(first.value.schema.as_ref().unwrap().default.is_none());
            assert!(first.value.schema.as_ref().unwrap().field("when").unwrap().schema.default.is_none());
            let mut changed = first_schema.as_ref().clone();
            let SchemaKind::Struct(fields) = &mut changed.kind else { unreachable!() };
            Arc::make_mut(&mut fields[0].schema).default = Some(Value::Utf8("2000".into()));
            let changed = Arc::new(changed);
            assert_ne!(&changed, first_schema);
            let other = chain.apply(record(&changed, value())).unwrap().unwrap();
            assert!(!Arc::ptr_eq(first.value.schema.as_ref().unwrap(), other.value.schema.as_ref().unwrap()));
            drop((other, next, chain));
            assert!(memory.used_bytes() > 0);
            drop(first);
            assert_eq!(memory.used_bytes(), 0);
        }
    }
}

#[tokio::test]
async fn json_null_defaults_are_absent_before_cast_and_timestamp_schema_caching() {
    let memory = MemoryBudget::new(256 << 20);
    let plugins = PluginRegistry::default();
    let mut converter = plugins.converter(&PluginSpec { class_name: "org.apache.kafka.connect.json.JsonConverter".into(), options: BTreeMap::from([("schemas.enable".into(), "true".into())]) }, false).unwrap();
    for (class, options) in [
        ("TimestampConverter", vec![("field", "when"), ("target.type", "Timestamp"), ("format", "yyyy")]),
        ("Cast", vec![("spec", "when:string")]),
    ] {
        let mut chain = chain(class, &options, &memory);
        let mut outputs = Vec::new();
        for explicit in [true, false] {
            let mut schema = serde_json::json!({"type":"struct","optional":true,"fields":[{"field":"when","type":"string","optional":true}]});
            if explicit { schema["default"] = serde_json::Value::Null; schema["fields"][0]["default"] = serde_json::Value::Null; }
            let bytes = serde_json::json!({"schema":schema,"payload":{"when":"1970"}}).to_string();
            let data = converter.decode("events", Some(bytes.as_bytes()), &mut ConversionContext::new(Arc::clone(&memory))).await.unwrap();
            assert!(data.schema.as_ref().unwrap().default.is_none());
            assert!(data.schema.as_ref().unwrap().field("when").unwrap().schema.default.is_none());
            outputs.push(chain.apply(SinkRecord::new("events".into(), 0, 0, ConnectData::null(), data)).unwrap().unwrap());
        }
        assert!(Arc::ptr_eq(outputs[0].value.schema.as_ref().unwrap(), outputs[1].value.schema.as_ref().unwrap()));
        assert!(outputs[0].value.schema.as_ref().unwrap().default.is_none());
        drop((outputs, chain));
        assert_eq!(memory.used_bytes(), 0);
    }
    let invalid = br#"{"schema":{"type":"string","default":null},"payload":"present"}"#;
    assert!(converter.decode("events", Some(invalid), &mut ConversionContext::new(Arc::clone(&memory))).await.is_err());
    converter.close().await.unwrap();
    drop(converter);
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn json_signed_zero_defaults_produce_distinct_cast_schema_defaults() {
    let memory = MemoryBudget::new(256 << 20);
    let mut converter = PluginRegistry::default().converter(&PluginSpec { class_name: "org.apache.kafka.connect.json.JsonConverter".into(), options: BTreeMap::from([("schemas.enable".into(), "true".into())]) }, false).unwrap();
    for kind in ["float", "double"] {
        let mut chain = chain("Cast", &[("spec", "string")], &memory);
        let mut outputs = Vec::new();
        let mut contexts = Vec::new();
        for zero in ["0.0", "-0.0"] {
            let bytes = format!(r#"{{"schema":{{"type":"{kind}","default":{zero}}},"payload":1}}"#);
            let mut context = ConversionContext::new(Arc::clone(&memory));
            let data = converter.decode("events", Some(bytes.as_bytes()), &mut context).await.unwrap();
            let output = chain.apply(SinkRecord::new("events".into(), 0, 0, ConnectData::null(), data)).unwrap().unwrap();
            assert_eq!(output.value.schema.as_ref().unwrap().default, Some(Value::Utf8(zero.into())));
            outputs.push(output);
            contexts.push(context);
        }
        assert!(!Arc::ptr_eq(outputs[0].value.schema.as_ref().unwrap(), outputs[1].value.schema.as_ref().unwrap()));
        drop((outputs, chain, contexts));
        assert_eq!(memory.used_bytes(), 0);
    }
    converter.close().await.unwrap();
}

struct NanDefaultsFactory;
struct NanDefaultsConverter;
impl ConverterFactory for NanDefaultsFactory {
    fn open(&self, _: &BTreeMap<String, String>, _: bool) -> rtdb_types::Result<Box<dyn Converter>> { Ok(Box::new(NanDefaultsConverter)) }
}
#[async_trait::async_trait]
impl Converter for NanDefaultsConverter {
    async fn decode(&mut self, _: &str, bytes: Option<&[u8]>, context: &mut ConversionContext) -> rtdb_types::Result<ConnectData> {
        context.reserve(128 << 10)?;
        let first = bytes == Some(&[0][..]);
        let single = Value::F32(f32::from_bits(if first { 0x7fc0_0001 } else { 0xffc0_0002 }));
        let double = Value::F64(f64::from_bits(if first { 0x7ff8_0000_0000_0001 } else { 0xfff8_0000_0000_0002 }));
        let scalar = |kind, default| {
            let mut schema = ConnectSchema::new(kind);
            schema.default = default;
            Arc::new(schema)
        };
        let f32_schema = scalar(SchemaKind::F32, None);
        let f64_schema = scalar(SchemaKind::F64, None);
        let mut entries = vec![(double.clone(), single.clone()), (Value::F64(1.0), Value::F32(2.0))];
        if !first { entries.reverse(); }
        let nested = scalar(SchemaKind::Struct(vec![ConnectField { name: "n".into(), schema: Arc::clone(&f64_schema) }]), Some(Value::Struct(StructFields::from([("n".into(), double.clone())]))));
        let schema = ConnectSchema::new(SchemaKind::Struct(vec![
            ConnectField { name: "when".into(), schema: scalar(SchemaKind::String, None) },
            ConnectField { name: "single".into(), schema: scalar(SchemaKind::F32, Some(single.clone())) },
            ConnectField { name: "double".into(), schema: scalar(SchemaKind::F64, Some(double.clone())) },
            ConnectField { name: "array".into(), schema: scalar(SchemaKind::Array(Arc::clone(&f64_schema)), Some(Value::list(vec![double.clone()]))) },
            ConnectField { name: "map".into(), schema: scalar(SchemaKind::Map(f64_schema, f32_schema), Some(Value::Map { entries, sorted: false })) },
            ConnectField { name: "nested".into(), schema: nested },
        ]));
        Ok(ConnectData { schema: Some(Arc::new(schema)), value: Value::Struct(StructFields::from([
            ("when".into(), Value::Utf8("1970".into())), ("single".into(), Value::F32(1.0)), ("double".into(), Value::F64(1.0)),
            ("array".into(), Value::list(vec![Value::F64(1.0)])), ("map".into(), Value::Map { entries: vec![(Value::F64(1.0), Value::F32(1.0))], sorted: false }),
            ("nested".into(), Value::Struct(StructFields::from([("n".into(), Value::F64(1.0))]))),
        ])) })
    }
}

#[tokio::test]
async fn custom_converter_nan_defaults_reuse_both_transform_caches_recursively() {
    let memory = MemoryBudget::new(256 << 20);
    let plugins = PluginRegistry::default().with_converter("test.NanDefaults".into(), Arc::new(NanDefaultsFactory)).unwrap();
    let mut converter = plugins.converter(&PluginSpec { class_name: "test.NanDefaults".into(), options: BTreeMap::new() }, false).unwrap();
    for (class, options) in [
        ("TimestampConverter", vec![("field", "when"), ("target.type", "Timestamp"), ("format", "yyyy")]),
        ("Cast", vec![("spec", "when:string")]),
    ] {
        let mut chain = chain(class, &options, &memory);
        let mut outputs = Vec::new();
        let mut contexts = Vec::new();
        for selector in [0, 1] {
            let mut context = ConversionContext::new(Arc::clone(&memory));
            let data = converter.decode("events", Some(&[selector]), &mut context).await.unwrap();
            let retained = memory.used_bytes();
            outputs.push(chain.apply(SinkRecord::new("events".into(), 0, 0, ConnectData::null(), data)).unwrap().unwrap());
            if selector == 1 { assert_eq!(memory.used_bytes(), retained); }
            contexts.push(context);
        }
        assert!(Arc::ptr_eq(outputs[0].value.schema.as_ref().unwrap(), outputs[1].value.schema.as_ref().unwrap()));
        drop((chain, contexts));
        let retained = memory.used_bytes();
        assert!(retained > 0);
        drop(outputs.pop());
        assert_eq!(memory.used_bytes(), retained);
        drop(outputs);
        assert_eq!(memory.used_bytes(), 0);
    }
    converter.close().await.unwrap();
}
