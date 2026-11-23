use rtdb_types::StructFields;
use std::collections::BTreeMap;
use prost_types::Any;
use rtdb_kafka::connect::{ConnectConfig, ConnectData, PluginRegistry, ResumeToken, SinkRecord, SourceConfig, SOURCE_TYPE};
use rtdb_types::Value;

fn environment() -> std::sync::Arc<rtdb_kafka::connect::DateEnvironment> {
    use rtdb_kafka::connect::{DateEnvironment,DateEnvironmentInput};
    DateEnvironment::from_inputs(DateEnvironmentInput {
        language: "en".into(),script: String::new(),region: "US".into(),variant: String::new(),unicode: BTreeMap::new(),timezone: "UTC".into(),
        initialized_at: 1_600_000_000_000,observed_at: 1_600_000_000_000,constructed_at: 1_600_000_000_000,
    },&rtdb_columnar::MemoryBudget::new(256 << 20)).unwrap()
}
fn properties() -> BTreeMap<String, String> { BTreeMap::from([
    ("key.converter".into(), "org.apache.kafka.connect.storage.StringConverter".into()),
    ("value.converter".into(), "org.apache.kafka.connect.json.JsonConverter".into()),
    ("value.converter.schemas.enable".into(), "false".into()),
]) }

fn transform_config(class: &str, options: &[(&str, &str)]) -> ConnectConfig {
    let mut properties = properties();
    properties.insert("transforms".into(), "step".into());
    properties.insert("transforms.step.type".into(), class.into());
    for &(key, value) in options {
        properties.insert(format!("transforms.step.{key}"), value.into());
    }
    ConnectConfig::parse(&properties).unwrap()
}

const STOCK_TRANSFORMS: &[(&[&str], &[(&str, &str)])] = &[
    (&["Cast$Key", "Cast$Value"], &[("spec", "int32")]),
    (&["DropHeaders"], &[("headers", "id")]),
    (&["ExtractField$Key", "ExtractField$Value"], &[("field", "id")]),
    (&["Filter"], &[]),
    (&["Flatten$Key", "Flatten$Value"], &[]),
    (&["HeaderFrom$Key", "HeaderFrom$Value"], &[("fields", "id"), ("headers", "id"), ("operation", "copy")]),
    (&["HoistField$Key", "HoistField$Value"], &[("field", "id")]),
    (&["InsertField$Key", "InsertField$Value"], &[("topic.field", "source")]),
    (&["InsertHeader"], &[("header", "id"), ("value.literal", "1")]),
    (&["MaskField$Key", "MaskField$Value"], &[("fields", "id")]),
    (&["RegexRouter"], &[("regex", "events"), ("replacement", "records")]),
    (&["ReplaceField$Key", "ReplaceField$Value"], &[]),
    (&["SetSchemaMetadata$Key", "SetSchemaMetadata$Value"], &[("schema.name", "Event")]),
    (&["TimestampConverter$Key", "TimestampConverter$Value"], &[("target.type", "unix")]),
    (&["TimestampRouter"], &[]),
    (&["ValueToKey"], &[("fields", "id")]),
];

#[test]
fn all_concrete_kafka_transformation_classes_open_by_their_binary_names() {
    let registry = PluginRegistry::default();
    let environment = environment();
    for &(names, options) in STOCK_TRANSFORMS {
        for name in names {
            let class = format!("org.apache.kafka.connect.transforms.{name}");
            registry.chain_with_environment(&transform_config(&class, options), environment.clone())
                .unwrap_or_else(|error| panic!("{class}: {error}"));
        }
    }
}

#[test]
fn whole_record_transform_suffixes_fail_class_lookup_before_option_validation() {
    let registry = PluginRegistry::default();
    let environment = environment();
    for name in ["DropHeaders", "Filter", "InsertHeader", "RegexRouter", "TimestampRouter", "ValueToKey"] {
        for suffix in ["$Key", "$Value"] {
            let class = format!("org.apache.kafka.connect.transforms.{name}{suffix}");
            let error = registry.chain_with_environment(&transform_config(&class, &[]), environment.clone())
                .err().unwrap_or_else(|| panic!("{class} opened"));
            assert_eq!(error.code, "xtdb.kafka-connect-source/plugin-class-not-found", "{class}");
            assert!(error.message.as_deref().is_some_and(|message| message.contains(class.as_str())), "{class}: {error}");
        }
    }
}

#[test]
fn abstract_transform_classes_require_a_concrete_key_or_value_class() {
    let registry = PluginRegistry::default();
    let environment = environment();
    for &(names, options) in STOCK_TRANSFORMS {
        if let Some(name) = names[0].strip_suffix("$Key") {
            let class = format!("org.apache.kafka.connect.transforms.{name}");
            assert!(registry.chain_with_environment(&transform_config(&class, options), environment.clone()).is_err(), "{class}");
        }
    }
}

#[test]
fn an_explicit_transform_factory_is_selected_before_stock_class_lookup() {
    use rtdb_kafka::connect::{Transform, TransformFactory};

    struct Factory;
    impl TransformFactory for Factory {
        fn open(&self, _: &BTreeMap<String, String>) -> rtdb_types::Result<Box<dyn Transform>> {
            Err(rtdb_types::Anomaly::incorrect("test/factory-option-error", "Factory requires options"))
        }
    }

    let class = "org.apache.kafka.connect.transforms.Filter$Key";
    let registry = PluginRegistry::default().with_transform(class.into(), std::sync::Arc::new(Factory)).unwrap();
    let error = registry.chain_with_environment(&transform_config(class, &[]), environment()).err().unwrap();
    assert_eq!(error.code, "test/factory-option-error");
}

#[test]
fn closed_config_namespace_preserves_plugin_options_and_validates_aliases() {
    let mut values = properties();
    values.insert("transforms".into(), "convert".into());
    values.insert("transforms.convert.type".into(), "org.apache.kafka.connect.transforms.TimestampConverter$Value".into());
    values.insert("transforms.convert.target.type".into(), "Timestamp".into());
    assert_eq!(ConnectConfig::parse(&values).unwrap().transforms[0].plugin.options["target.type"], "Timestamp");
    values.insert("max.poll.records".into(), "1".into());
    assert!(ConnectConfig::parse(&values).unwrap_err().code.ends_with("unknown-connect-config-key"));
    values.remove("max.poll.records"); values.insert("transforms".into(), "a,a".into());
    assert!(ConnectConfig::parse(&values).unwrap_err().code.ends_with("duplicate-alias"));
    values.insert("transforms".into(), "a,".into());
    assert!(ConnectConfig::parse(&values).unwrap_err().code.ends_with("empty-alias"));
}
#[test]
fn source_and_offset_protobuf_round_trip_without_validating_historic_plugin_configuration() {
    let source = SourceConfig { remote: "remote".into(), topic: "topic".into(), connect_config: BTreeMap::new(), indexer: Any { type_url: "custom/indexer".into(), value: vec![0, 1] } };
    assert_eq!(source.encode().type_url, SOURCE_TYPE);
    assert_eq!(SourceConfig::decode(&source.encode()).unwrap(), source);
    assert!(source.validate().is_err());
    assert_eq!(ResumeToken::decode(&ResumeToken(0).encode()).unwrap(), ResumeToken(0));
    assert_eq!(ResumeToken::decode(&ResumeToken(i64::MAX).encode()).unwrap(), ResumeToken(i64::MAX));
    assert!(ResumeToken::decode(&ResumeToken(-1).encode()).is_err());
}
#[tokio::test]
async fn json_converter_struct_defaults_logical_types_and_tombstones() {
    let mut context = context();
    let mut values = properties(); values.insert("value.converter.schemas.enable".into(), "true".into());
    let config = ConnectConfig::parse(&values).unwrap();
    let mut converter = PluginRegistry::default().converter(&config.value, false).unwrap();
    let value = converter.decode("topic", Some(br#"{"schema":{"type":"struct","fields":[{"field":"id","type":"int32"},{"field":"name","type":"string","default":"unknown"},{"field":"when","type":"int64","name":"org.apache.kafka.connect.data.Timestamp"}]},"payload":{"id":7,"when":1234}}"#), &mut context).await.unwrap();
    let Value::Struct(fields) = value.value else { panic!("expected Struct") };
    assert_eq!(fields["id"], Value::I32(7)); assert_eq!(fields["name"], Value::Utf8("unknown".into()));
    assert_eq!(fields["when"], Value::Timestamp { value: 1234, unit: rtdb_types::TimeUnit::Millisecond, timezone: Some("UTC".into()) });
    assert_eq!(converter.decode("topic", None, &mut context).await.unwrap(), ConnectData::null());
    assert!(converter.decode("topic", Some(b"not json"), &mut context).await.is_err());
}

#[tokio::test]
async fn json_decimal_converter_preserves_signed_full_scale_and_rejects_empty_coefficients() {
    let mut context = context();
    let mut values = properties(); values.insert("value.converter.schemas.enable".into(), "true".into());
    let config = ConnectConfig::parse(&values).unwrap();
    let mut converter = PluginRegistry::default().converter(&config.value, false).unwrap();
    for scale in [i32::MIN, -129, 129, i32::MAX] {
        let payload = serde_json::json!({"schema":{"type":"bytes","name":"org.apache.kafka.connect.data.Decimal","parameters":{"scale":scale.to_string()}},"payload":"AQ=="}).to_string();
        let value = converter.decode("topic", Some(payload.as_bytes()), &mut context).await.unwrap();
        let Value::Decimal(value) = value.value else { panic!("expected Decimal") };
        assert_eq!(value.scale, scale);
        assert_eq!(num_bigint::BigInt::from_signed_bytes_be(&value.unscaled), 1.into());
    }
    for (scale, payload) in [("2147483648", "AQ=="), ("0", "")] {
        let payload = serde_json::json!({"schema":{"type":"bytes","name":"org.apache.kafka.connect.data.Decimal","parameters":{"scale":scale}},"payload":payload}).to_string();
        assert!(converter.decode("topic", Some(payload.as_bytes()), &mut context).await.is_err());
    }
}
#[tokio::test]
async fn transforms_run_in_declared_order_and_predicate_negation_preserves_tombstones() {
    let mut context = context();
    let mut values = properties();
    values.extend([
        ("transforms".into(), "unwrap,rename".into()),
        ("transforms.unwrap.type".into(), "org.apache.kafka.connect.transforms.ExtractField$Value".into()),
        ("transforms.unwrap.field".into(), "payload".into()),
        ("transforms.rename.type".into(), "org.apache.kafka.connect.transforms.ReplaceField$Value".into()),
        ("transforms.rename.renames".into(), "old:new".into()),
        ("transforms.unwrap.predicate".into(), "deleted".into()), ("transforms.unwrap.negate".into(), " TRUE ".into()),
        ("predicates".into(), "deleted".into()), ("predicates.deleted.type".into(), "org.apache.kafka.connect.transforms.predicates.RecordIsTombstone".into()),
    ]);
    let config = ConnectConfig::parse(&values).unwrap(); let plugins = PluginRegistry::default();
    let mut converter = plugins.converter(&config.value, false).unwrap(); let mut chain = plugins.chain_with_environment(&config,environment()).unwrap();
    let data = converter.decode("events", Some(br#"{"payload":{"old":42}}"#), &mut context).await.unwrap();
    let record = SinkRecord::new("events".into(), 0, 7, ConnectData::schemaless(Value::Utf8("id".into())), data);
    let record = chain.apply(record).unwrap().unwrap();
    assert_eq!(record.value.value, Value::Map { entries: vec![(Value::Utf8("new".into()), Value::I64(42))], sorted: false });
    let tombstone = SinkRecord::new("events".into(), 0, 8, record.key, ConnectData::null());
    assert_eq!(chain.apply(tombstone).unwrap().unwrap().value, ConnectData::null());
}
#[tokio::test]
async fn scalar_converters_preserve_binary_width_and_string_encoding() {
    let mut context = context();
    let plugins = PluginRegistry::default();
    let mut converter = plugins.converter(&rtdb_kafka::connect::PluginSpec { class_name: "org.apache.kafka.connect.converters.LongConverter".into(), options: BTreeMap::new() }, true).unwrap();
    assert_eq!(converter.decode("t", Some(&42i64.to_be_bytes()), &mut context).await.unwrap().value, Value::I64(42));
    assert!(converter.decode("t", Some(&[1]), &mut context).await.is_err());
    let mut converter = plugins.converter(&rtdb_kafka::connect::PluginSpec { class_name: "org.apache.kafka.connect.storage.StringConverter".into(), options: BTreeMap::from([("converter.encoding".into(), "ISO-8859-1".into())]) }, false).unwrap();
    assert_eq!(converter.decode("t", Some(&[0xe9]), &mut context).await.unwrap().value, Value::Utf8("é".into()));
}

#[test]
fn regex_router_uses_full_match_and_java_numeric_replacement_groups() {
    let mut values = properties();
    values.extend([
        ("transforms".into(), "route".into()),
        ("transforms.route.type".into(), "org.apache.kafka.connect.transforms.RegexRouter".into()),
        ("transforms.route.regex".into(), "(a|ab)".into()),
        ("transforms.route.replacement".into(), "$1suffix".into()),
    ]);
    let mut chain = PluginRegistry::default().chain_with_environment(&ConnectConfig::parse(&values).unwrap(),environment()).unwrap();
    let record = SinkRecord::new("ab".into(), 0, 0, ConnectData::null(), ConnectData::null());
    assert_eq!(chain.apply(record).unwrap().unwrap().topic, "absuffix");
}

#[tokio::test]
async fn configured_schema_default_expansion_is_bounded_before_cloning() {
    let mut context = context();
    let mut values = properties(); values.insert("value.converter.schemas.enable".into(), "true".into());
    let schema = serde_json::json!({"type":"struct","fields":[{"field":"expanded","type":"string","default":"x".repeat(100_000)}]});
    values.insert("value.converter.schema.content".into(), schema.to_string());
    let config = ConnectConfig::parse(&values).unwrap();
    let mut converter = PluginRegistry::default().converter(&config.value, false).unwrap();
    assert!(converter.decode("topic", Some(b"{}"), &mut context).await.unwrap_err().code.ends_with("batch-too-large"));
}

#[test]
fn header_from_moves_fields_and_insert_field_preserves_source_metadata() {
    let mut values = properties();
    values.extend([
        ("transforms".into(), "insert,headers".into()),
        ("transforms.insert.type".into(), "org.apache.kafka.connect.transforms.InsertField$Value".into()),
        ("transforms.insert.offset.field".into(), "offset!".into()),
        ("transforms.insert.static.field".into(), "origin".into()), ("transforms.insert.static.value".into(), "source".into()),
        ("transforms.headers.type".into(), "org.apache.kafka.connect.transforms.HeaderFrom$Value".into()),
        ("transforms.headers.fields".into(), "origin".into()), ("transforms.headers.headers".into(), "x-origin".into()),
        ("transforms.headers.operation".into(), "move".into()),
    ]);
    let mut chain = PluginRegistry::default().chain_with_environment(&ConnectConfig::parse(&values).unwrap(),environment()).unwrap();
    let record = SinkRecord::new("events".into(), 2, 17, ConnectData::null(), ConnectData::schemaless(Value::Map { entries: Vec::new(), sorted: false }));
    let record = chain.apply(record).unwrap().unwrap();
    assert_eq!(record.headers[0].0, "x-origin"); assert_eq!(record.headers[0].1.value, Value::Utf8("source".into()));
    assert_eq!(record.value.value, Value::Map { entries: vec![(Value::Utf8("offset".into()), Value::I64(17))], sorted: false });
}

fn context() -> rtdb_kafka::connect::ConversionContext { rtdb_kafka::connect::ConversionContext::new(rtdb_columnar::MemoryBudget::new(1 << 30)) }

#[test]
fn converter_limit_counts_all_charges_and_shared_owners_once() {
    let memory = rtdb_columnar::MemoryBudget::new(4096); let mut context = rtdb_kafka::connect::ConversionContext::with_limit(memory, 1024);
    let charge = context.reserve(768).unwrap(); context.retain(charge).unwrap();
    assert!(context.reserve(257).unwrap_err().code.ends_with("batch-too-large"));
    context.reserve(256).unwrap();
}

#[test]
fn cast_uses_java_numeric_narrowing_and_string_range_checks() {
    let mut values = properties(); values.extend([("transforms".into(), "cast".into()), ("transforms.cast.type".into(), "org.apache.kafka.connect.transforms.Cast$Value".into()), ("transforms.cast.spec".into(), "int8".into())]);
    let mut chain = PluginRegistry::default().chain_with_environment(&ConnectConfig::parse(&values).unwrap(),environment()).unwrap();
    let record = |value| SinkRecord::new("events".into(), 0, 1, ConnectData::null(), ConnectData::schemaless(value));
    assert_eq!(chain.apply(record(Value::I64(257))).unwrap().unwrap().value.value, Value::I8(1));
    assert_eq!(chain.apply(record(Value::F64(f64::INFINITY))).unwrap().unwrap().value.value, Value::I8(-1));
    assert!(chain.apply(record(Value::Utf8("257".into()))).is_err());
    assert_eq!(chain.apply(record(Value::Null)).unwrap().unwrap().value.value, Value::Null);
}

#[tokio::test]
async fn flatten_propagates_ancestor_optional_defaults_and_preserves_arrays() {
    let mut context = context(); let mut values = properties(); values.insert("value.converter.schemas.enable".into(), "true".into());
    values.extend([("transforms".into(), "flatten".into()), ("transforms.flatten.type".into(), "org.apache.kafka.connect.transforms.Flatten$Value".into())]);
    let config = ConnectConfig::parse(&values).unwrap(); let mut converter = PluginRegistry::default().converter(&config.value, false).unwrap();
    let value = converter.decode("events", Some(br#"{"schema":{"type":"struct","fields":[{"field":"nested","type":"struct","optional":true,"fields":[{"field":"n","type":"int64","default":9},{"field":"xs","type":"array","items":{"type":"string"}}]}]},"payload":{"nested":{"n":null,"xs":["a","b"]}}}"#), &mut context).await.unwrap();
    let mut chain = PluginRegistry::default().chain_with_environment(&config,environment()).unwrap();
    let record = chain.apply(SinkRecord::new("events".into(), 0, 1, ConnectData::null(), value)).unwrap().unwrap();
    assert_eq!(record.value.value, Value::Struct(StructFields::from([("nested.n".into(), Value::I64(9)), ("nested.xs".into(), Value::list(vec![Value::Utf8("a".into()), Value::Utf8("b".into())]))])));
    let schema = record.value.schema.unwrap(); assert!(schema.field("nested.n").unwrap().schema.optional); assert_eq!(schema.field("nested.n").unwrap().schema.default, Some(Value::I64(9)));
}

#[test]
fn mask_field_adds_missing_schemaless_fields_as_null_and_rejects_binary_masks() {
    let mut values = properties(); values.extend([("transforms".into(), "mask".into()), ("transforms.mask.type".into(), "org.apache.kafka.connect.transforms.MaskField$Value".into()), ("transforms.mask.fields".into(), "secret,missing".into())]);
    let mut chain = PluginRegistry::default().chain_with_environment(&ConnectConfig::parse(&values).unwrap(),environment()).unwrap();
    let record = |value| SinkRecord::new("events".into(), 0, 1, ConnectData::null(), ConnectData::schemaless(Value::Map { entries: vec![(Value::Utf8("secret".into()), value)], sorted: false }));
    assert_eq!(chain.apply(record(Value::Utf8("sensitive".into()))).unwrap().unwrap().value.value, Value::Map { entries: vec![(Value::Utf8("missing".into()), Value::Null), (Value::Utf8("secret".into()), Value::Utf8(String::new()))], sorted: false });
    assert!(chain.apply(record(Value::Binary(vec![1, 2]))).is_err());
}

#[test]
fn timestamp_router_replaces_timestamp_placeholders_inserted_from_topic_and_header_infers_width() {
    let mut values = properties(); values.extend([
        ("transforms".into(), "header,route".into()), ("transforms.header.type".into(), "org.apache.kafka.connect.transforms.InsertHeader".into()),
        ("transforms.header.header".into(), "sequence".into()), ("transforms.header.value.literal".into(), "128".into()),
        ("transforms.route.type".into(), "org.apache.kafka.connect.transforms.TimestampRouter".into()),
    ]);
    let mut chain = PluginRegistry::default().chain_with_environment(&ConnectConfig::parse(&values).unwrap(),environment()).unwrap();
    let mut record = SinkRecord::new("events-${timestamp}".into(), 0, 1, ConnectData::null(), ConnectData::null()); record.timestamp_millis = Some(86_400_000);
    let record = chain.apply(record).unwrap().unwrap(); assert_eq!(record.topic, "events-19700102-19700102"); assert_eq!(record.headers[0].1.value, Value::I16(128));
}

#[tokio::test]
async fn confluent_avro_registry_lookup_is_cached_and_null_tombstones_do_not_fetch() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let schema = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}"#;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let body = serde_json::json!({"schema":schema}).to_string();
    let server = tokio::spawn(async move {
        let mut requests = Vec::new();
        for body in [body, r#"{"id":7,"version":3}"#.to_owned()] {
            let (mut stream, _) = listener.accept().await.unwrap(); let mut request = Vec::new();
            while !request.ends_with(b"\r\n\r\n") { let mut byte = [0]; stream.read_exact(&mut byte).await.unwrap(); request.push(byte[0]); assert!(request.len() < 8192); }
            let request = String::from_utf8(request).unwrap();
            let length = request.lines().find_map(|line| line.to_ascii_lowercase().strip_prefix("content-length:").map(|length| length.trim().parse::<usize>().unwrap())).unwrap_or(0);
            let mut payload = vec![0; length]; stream.read_exact(&mut payload).await.unwrap();
            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body);
            stream.write_all(response.as_bytes()).await.unwrap(); requests.push(request);
        }
        requests
    });
    let spec = rtdb_kafka::connect::PluginSpec { class_name: "io.confluent.connect.avro.AvroConverter".into(), options: BTreeMap::from([("schema.registry.url".into(), format!("http://{address}"))]) };
    let mut converter = PluginRegistry::default().converter(&spec, false).unwrap(); let mut context = context();
    assert_eq!(converter.decode("events", None, &mut context).await.unwrap(), ConnectData::null());
    let mut payload = vec![0, 0, 0, 0, 7]; payload.push(14);
    let first = converter.decode("events", Some(&payload), &mut context).await.unwrap();
    assert_eq!(first.value, Value::Struct(StructFields::from([("id".into(), Value::I64(7))])));
    assert_eq!(first.schema.as_ref().unwrap().version, Some(3));
    let requests = server.await.unwrap(); assert!(requests[0].starts_with("GET /schemas/ids/7 ")); assert!(requests[1].starts_with("POST /subjects/events-value?normalize=false&deleted=true "));
    assert_eq!(converter.decode("events", Some(&payload), &mut context).await.unwrap(), first);
}
