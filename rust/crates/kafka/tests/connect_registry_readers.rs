use rtdb_types::StructFields;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConversionContext, Converter, PluginRegistry, PluginSpec};
use rtdb_types::Value;
use serde_json::{Value as Json, json};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, task::JoinHandle};

struct Reply { method: &'static str, path: String, body: Json }
fn get(path: &str, body: Json) -> Reply { Reply { method: "GET", path: path.into(), body } }
fn lookup(version: i32) -> Reply { Reply { method: "POST", path: "/subjects/events-value?normalize=false&deleted=true".into(), body: json!({"id":7,"version":version}) } }
fn schema(format: &str, text: &str, version: i32, rules: Json) -> Json { json!({"id":7,"version":version,"schemaType":format,"schema":text,"ruleSet":rules}) }
fn framed(datum: &[u8]) -> Vec<u8> { let mut bytes = vec![0,0,0,0,7]; bytes.extend_from_slice(datum); bytes }
fn context(memory: &Arc<MemoryBudget>) -> ConversionContext { ConversionContext::new(Arc::clone(memory)) }

async fn registry(format: &str, replies: Vec<Reply>, mut options: BTreeMap<String,String>) -> (Box<dyn Converter>, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for reply in replies {
            let (mut stream,_) = tokio::time::timeout(Duration::from_secs(10),listener.accept()).await.unwrap().unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                let mut byte = [0]; stream.read_exact(&mut byte).await.unwrap(); header.push(byte[0]); assert!(header.len() < 16_384);
            }
            let header = String::from_utf8(header).unwrap();
            let line = header.lines().next().unwrap();
            assert_eq!(line,format!("{} {} HTTP/1.1",reply.method,reply.path));
            let length = header.lines().find_map(|line| line.to_ascii_lowercase().strip_prefix("content-length:").map(|value| value.trim().parse::<usize>().unwrap())).unwrap_or(0);
            let mut payload = vec![0;length]; stream.read_exact(&mut payload).await.unwrap();
            let body = reply.body.to_string();
            let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    options.insert("schema.registry.url".into(),format!("http://{address}"));
    let class = if format == "AVRO" { "io.confluent.connect.avro.AvroConverter" } else { "io.confluent.connect.json.JsonSchemaConverter" };
    let spec = PluginSpec { class_name: class.into(),options };
    (PluginRegistry::default().converter(&spec,false).unwrap(),server)
}
fn latest() -> BTreeMap<String,String> { BTreeMap::from([("use.latest.version".into(),"true".into())]) }

#[tokio::test]
async fn json_reader_uses_selected_version_and_fetches_intermediate_deleted_versions() {
    let writer = schema("JSON",r#"{"type":"integer","connect.type":"unsupported-writer-decoration"}"#,1,Json::Null);
    let reader = schema("JSON",r#"{"type":"integer"}"#,3,json!({"domainRules":[{"type":"unknown","onFailure":"NONE"}]}));
    let middle = schema("JSON",r#"{"type":"integer"}"#,2,json!({"migrationRules":[{"mode":"UPGRADE","type":"unknown","onFailure":"NONE"}]}));
    let (mut converter,server) = registry("JSON",vec![get("/schemas/ids/7",writer),lookup(1),get("/subjects/events-value/versions/latest",reader),get("/subjects/events-value/versions/2?deleted=true",middle)],latest()).await;
    let memory = MemoryBudget::new(1 << 27);
    let result = converter.decode("events",Some(&framed(b"42")),&mut context(&memory)).await.unwrap();
    assert_eq!(result.value,Value::I64(42)); assert_eq!(result.schema.as_ref().unwrap().version,Some(3));
    server.await.unwrap();
    assert_eq!(converter.decode("events",Some(&framed(b"42")),&mut context(&memory)).await.unwrap(),result);
    drop(converter); assert_eq!(memory.used_bytes(),0);
}

#[tokio::test]
async fn metadata_selection_encodes_pairs_and_empty_selector_suppresses_latest() {
    let writer = schema("JSON",r#"{"type":"integer"}"#,1,Json::Null);
    let reader = schema("JSON",r#"{"type":"integer"}"#,1,Json::Null);
    let options = BTreeMap::from([("use.latest.with.metadata".into(),"'tag=one,two',owner=a b".into())]);
    let (mut converter,server) = registry("JSON",vec![get("/schemas/ids/7",writer.clone()),lookup(1),get("/subjects/events-value/metadata?key=owner&value=a+b&key=tag&value=one%2Ctwo&deleted=true",reader)],options).await;
    let memory = MemoryBudget::new(1 << 27);
    assert_eq!(converter.decode("events",Some(&framed(b"1")),&mut context(&memory)).await.unwrap().value,Value::I64(1)); server.await.unwrap();
    let mut options = latest(); options.insert("use.latest.with.metadata".into(),String::new());
    let (mut converter,server) = registry("JSON",vec![get("/schemas/ids/7",writer),lookup(1)],options).await;
    assert_eq!(converter.decode("events",Some(&framed(b"1")),&mut context(&memory)).await.unwrap().value,Value::I64(1)); server.await.unwrap();
}

#[tokio::test]
async fn zero_latest_ttl_reselects_schema_without_reusing_an_obsolete_binding() {
    let writer = schema("JSON",r#"{"type":"integer"}"#,1,Json::Null);
    let first = schema("JSON",r#"{"type":"integer"}"#,1,Json::Null);
    let second = schema("JSON",r#"{"type":"integer","connect.type":"int32"}"#,2,Json::Null);
    let mut options = latest(); options.insert("latest.cache.ttl.sec".into(),"0".into());
    let (mut converter,server) = registry("JSON",vec![get("/schemas/ids/7",writer),lookup(1),get("/subjects/events-value/versions/latest",first),get("/subjects/events-value/versions/latest",second)],options).await;
    let memory = MemoryBudget::new(1 << 27);
    let first = converter.decode("events",Some(&framed(b"42")),&mut context(&memory)).await.unwrap();
    let second = converter.decode("events",Some(&framed(b"42")),&mut context(&memory)).await.unwrap();
    assert_eq!(first.value,Value::I64(42)); assert_eq!(first.schema.as_ref().unwrap().version,Some(1));
    assert_eq!(second.value,Value::I32(42)); assert_eq!(second.schema.as_ref().unwrap().version,Some(2));
    server.await.unwrap();
}

#[tokio::test]
async fn migration_direction_and_rule_order_choose_the_first_failure() {
    for (writer_version,reader_version,rules,expected) in [
        (1,3,json!({"migrationRules":[{"name":"first-upgrade","mode":"UPDOWN","type":"unknown","onFailure":"ERROR,NONE"},{"name":"second-upgrade","mode":"UPGRADE","type":"unknown"}]}),"first-upgrade"),
        (3,1,json!({"migrationRules":[{"name":"first-downgrade","mode":"DOWNGRADE","type":"unknown"},{"name":"second-downgrade","mode":"UPDOWN","type":"unknown","onFailure":"NONE,ERROR"}]}),"second-downgrade"),
    ] {
        let writer = schema("JSON",r#"{"type":"integer"}"#,writer_version,if writer_version == 3 { rules.clone() } else { Json::Null });
        let reader = schema("JSON",r#"{"type":"integer"}"#,reader_version,if reader_version == 3 { rules.clone() } else { Json::Null });
        let middle = schema("JSON",r#"{"type":"integer"}"#,2,json!({"migrationRules":[{"name":"middle","mode":"UPDOWN","type":"unknown","onFailure":"NONE,ERROR"}]}));
        let (mut converter,server) = registry("JSON",vec![get("/schemas/ids/7",writer),lookup(writer_version),get("/subjects/events-value/versions/latest",reader),get("/subjects/events-value/versions/2?deleted=true",middle)],latest()).await;
        let memory = MemoryBudget::new(1 << 27);
        let error = converter.decode("events",Some(&framed(b"null")),&mut context(&memory)).await.unwrap_err();
        assert!(error.to_string().contains(expected),"{error}"); server.await.unwrap();
    }
}

#[tokio::test]
async fn avro_reader_projects_defaults_aliases_and_keeps_writer_version() {
    let writer = schema("AVRO",r#"{"type":"record","name":"Old","fields":[{"name":"id","type":"int"},{"name":"discarded","type":"null"}]}"#,1,Json::Null);
    let reader = schema("AVRO",r#"{"type":"record","name":"New","aliases":["Old"],"fields":[{"name":"identifier","aliases":["id"],"type":"long"},{"name":"label","type":"string","default":"added"}]}"#,2,Json::Null);
    let (mut converter,server) = registry("AVRO",vec![get("/schemas/ids/7",writer),lookup(1),get("/subjects/events-value/versions/latest",reader)],latest()).await;
    let memory = MemoryBudget::new(1 << 27);
    let result = converter.decode("events",Some(&framed(&[14])),&mut context(&memory)).await.unwrap();
    assert_eq!(result.value,Value::Struct(StructFields::from([("identifier".into(),Value::I64(7)),("label".into(),Value::Utf8("added".into()))])));
    assert_eq!(result.schema.as_ref().unwrap().name.as_deref(),Some("New")); assert_eq!(result.schema.as_ref().unwrap().version,Some(1));
    server.await.unwrap();
    assert_eq!(converter.decode("events",Some(&framed(&[14])),&mut context(&memory)).await.unwrap(),result);
}

#[tokio::test]
async fn disabled_avro_migration_still_uses_json_reader_conversion() {
    let text = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}"#;
    let writer = schema("AVRO",text,1,Json::Null);
    let reader = schema("AVRO",r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"},{"name":"added","type":"string","default":"default"}]}"#,2,json!({"migrationRules":[{"name":"disabled","mode":"UPGRADE","type":"unknown","disabled":true}]}));
    let (mut converter,server) = registry("AVRO",vec![get("/schemas/ids/7",writer),lookup(1),get("/subjects/events-value/versions/latest",reader)],latest()).await;
    let memory = MemoryBudget::new(1 << 27);
    assert!(converter.decode("events",Some(&framed(&[14])),&mut context(&memory)).await.is_err());
    server.await.unwrap();
}

#[tokio::test]
async fn cached_reader_binding_reservations_are_checked_and_retained() {
    let text = r#"{"type":"record","name":"Event","fields":[{"name":"id","type":"long"}]}"#;
    let writer = schema("AVRO",text,1,Json::Null); let reader = schema("AVRO",text,1,Json::Null);
    let (mut converter,server) = registry("AVRO",vec![get("/schemas/ids/7",writer),lookup(1),get("/subjects/events-value/versions/latest",reader)],latest()).await;
    let memory = MemoryBudget::new(1 << 27);
    converter.decode("events",Some(&framed(&[14])),&mut context(&memory)).await.unwrap(); server.await.unwrap();
    let mut constrained = ConversionContext::with_limit(Arc::clone(&memory),1);
    assert_eq!(converter.decode("events",Some(&framed(&[14])),&mut constrained).await.unwrap_err().code,"xtdb.kafka-connect-source/batch-too-large"); drop(constrained);
    let mut retained = context(&memory); converter.decode("events",Some(&framed(&[14])),&mut retained).await.unwrap();
    drop(converter); assert!(memory.used_bytes() > 0); drop(retained); assert_eq!(memory.used_bytes(),0);
}

#[tokio::test]
async fn selected_json_read_rules_precede_connect_graph_conversion() {
    let text = r#"{"type":"integer","connect.type":"unknown"}"#;
    let writer = schema("JSON",r#"{"type":"integer"}"#,1,Json::Null);
    let reader = schema("JSON",text,1,json!({"domainRules":[{"name":"reader-guard","type":"unknown"}]}));
    let (mut converter,server) = registry("JSON",vec![get("/schemas/ids/7",writer),lookup(1),get("/subjects/events-value/versions/latest",reader)],latest()).await;
    let memory = MemoryBudget::new(1 << 27);
    let error = converter.decode("events",Some(&framed(b"42")),&mut context(&memory)).await.unwrap_err();
    assert!(error.to_string().contains("reader-guard")); server.await.unwrap();
}

#[tokio::test]
async fn framed_avro_null_fails_source_conversion_while_absent_payload_is_a_tombstone() {
    let writer = schema("AVRO",r#""null""#,1,json!({"domainRules":[{"name":"skipped-on-null","type":"unknown"}]}));
    let (mut converter,server) = registry("AVRO",vec![get("/schemas/ids/7",writer),lookup(1)],BTreeMap::new()).await;
    let memory = MemoryBudget::new(1 << 27);
    let error = converter.decode("events",Some(&framed(&[])),&mut context(&memory)).await.unwrap_err();
    assert!(error.to_string().contains("no schema and value")); assert!(!error.to_string().contains("skipped-on-null"));
    server.await.unwrap();
    assert_eq!(converter.decode("events",None,&mut context(&memory)).await.unwrap(),rtdb_kafka::connect::ConnectData::null());
}

#[tokio::test]
async fn avro_date_datums_use_final_connect_names_after_primitive_admission() {
    for (physical, name, millis) in [("int", "Date", 86_400_000), ("int", "Time", 1), ("long", "Timestamp", 1)] {
        let text = format!(r#"{{"type":"{physical}","connect.name":"org.apache.kafka.connect.data.{name}"}}"#);
        let writer = schema("AVRO", &text, 1, Json::Null);
        let (mut converter, server) = registry("AVRO", vec![get("/schemas/ids/7", writer), lookup(1)], BTreeMap::new()).await;
        let memory = MemoryBudget::new(1 << 27);
        let result = converter.decode("events", Some(&framed(&[2])), &mut context(&memory)).await.unwrap();
        assert_eq!(result.value, Value::Timestamp { value: millis, unit: rtdb_types::TimeUnit::Millisecond, timezone: Some("UTC".into()) });
        assert_eq!(result.schema.as_ref().unwrap().name.as_deref(), Some(format!("org.apache.kafka.connect.data.{name}").as_str()));
        server.await.unwrap();
        converter.close().await.unwrap();
        drop((result, converter));
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[tokio::test]
async fn avro_explicit_date_name_is_installed_after_default_conversion() {
    let text = r#"{"type":"record","name":"Dates","fields":[{"name":"explicit","type":{"type":"int","connect.name":"org.apache.kafka.connect.data.Date"},"default":1},{"name":"physical","type":{"type":"int","logicalType":"date"},"default":1},{"name":"configured","type":{"type":"int","connect.name":"org.apache.kafka.connect.data.Date","connect.default":2}}]}"#;
    let writer = schema("AVRO", text, 1, Json::Null);
    let (mut converter, server) = registry("AVRO", vec![get("/schemas/ids/7", writer), lookup(1)], BTreeMap::new()).await;
    let memory = MemoryBudget::new(1 << 27);
    let result = converter.decode("events", Some(&framed(&[2,2,2])), &mut context(&memory)).await.unwrap();
    let date = Value::Timestamp { value: 86_400_000, unit: rtdb_types::TimeUnit::Millisecond, timezone: Some("UTC".into()) };
    assert_eq!(result.value, Value::Struct(StructFields::from([("explicit".into(), date.clone()), ("physical".into(), date.clone()), ("configured".into(), date.clone())])));
    let schema = result.schema.as_ref().unwrap();
    assert_eq!(schema.field("explicit").unwrap().schema.default, Some(Value::I32(1)));
    assert_eq!(schema.field("physical").unwrap().schema.default, Some(date));
    assert_eq!(schema.field("configured").unwrap().schema.default, Some(Value::I32(2)));
    server.await.unwrap();
    converter.close().await.unwrap();
    drop((result, converter));
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn avro_explicit_decimal_name_converts_bytes_with_its_final_scale() {
    let text = r#"{"type":"bytes","connect.name":"org.apache.kafka.connect.data.Decimal","connect.parameters":{"scale":"-2"},"connect.default":"\u0001"}"#;
    let writer = schema("AVRO", text, 1, Json::Null);
    let (mut converter, server) = registry("AVRO", vec![get("/schemas/ids/7", writer), lookup(1)], BTreeMap::new()).await;
    let memory = MemoryBudget::new(1 << 27);
    let result = converter.decode("events", Some(&framed(&[1])), &mut context(&memory)).await.unwrap();
    let Value::Decimal(value) = &result.value else { panic!("expected Decimal") };
    assert_eq!(value.scale, -2);
    assert_eq!(num_bigint::BigInt::from_signed_bytes_be(&value.unscaled), 1.into());
    assert_eq!(result.schema.as_ref().unwrap().default, Some(Value::Binary(vec![1])));
    server.await.unwrap();
    converter.close().await.unwrap();
    drop((result, converter));
    assert_eq!(memory.used_bytes(), 0);
}

#[tokio::test]
async fn avro_decimal_conversion_failures_release_public_decoder_allocations() {
    let text = r#"{"type":"bytes","connect.name":"org.apache.kafka.connect.data.Decimal"}"#;
    let writer = schema("AVRO", text, 1, Json::Null);
    let (mut converter, server) = registry("AVRO", vec![get("/schemas/ids/7", writer), lookup(1)], BTreeMap::new()).await;
    let memory = MemoryBudget::new(1 << 27);
    assert!(converter.decode("events", Some(&framed(&[])), &mut context(&memory)).await.unwrap_err().message.as_deref().is_some_and(|message| message.contains("Decimal coefficient bytes are empty")));
    server.await.unwrap();
    assert!(converter.decode("events", Some(&framed(&[1])), &mut context(&memory)).await.unwrap_err().message.as_deref().is_some_and(|message| message.contains("Decimal schema has no scale")));
    converter.close().await.unwrap();
    drop(converter);
    assert_eq!(memory.used_bytes(), 0);
}
