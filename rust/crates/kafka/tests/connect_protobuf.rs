use std::{collections::BTreeMap, time::Duration};

use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{
    ConnectConfig, ConnectData, ConversionContext, Converter, PluginRegistry, SchemaKind,
};
use rtdb_types::{StructFields, Value};
use serde_json::{Value as Json, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinHandle,
};

const CLASS: &str = "io.confluent.connect.protobuf.ProtobufConverter";
const ROOT: &str = r#"
    syntax = "proto3";
    package example;
    import "detail.proto";
    message First { int32 id = 1; }
    enum Spare { UNUSED = 0; }
    message Envelope {
        map<string, int32> attributes = 10;
        message Ignored {}
        enum State { NEW = 0; DONE = 1; }
        message Event {
            ref.Detail detail = 1;
            optional int32 absent = 2;
            oneof identifier { string name = 3; int64 number = 4; }
            State state = 5;
            repeated sint32 deltas = 6;
        }
    }
"#;
const DETAIL: &str = r#"
    syntax = "proto3";
    package ref;
    import "code.proto";
    message Detail { Code wrapped = 1; }
"#;
const CODE: &str = r#"syntax = "proto3"; package ref; message Code { uint32 number = 1; }"#;

struct Reply {
    method: &'static str,
    path: String,
    response: Json,
    request: Option<Json>,
}

fn get(path: &str, response: Json) -> Reply {
    Reply {
        method: "GET",
        path: path.into(),
        response,
        request: None,
    }
}

fn lookup(subject: &str, version: i32, request: Json) -> Reply {
    Reply {
        method: "POST",
        path: format!("/subjects/{subject}?normalize=false&deleted=true"),
        response: json!({"id": 7, "version": version}),
        request: Some(request),
    }
}

fn registry_schema(text: &str) -> Json {
    json!({"schemaType": "PROTOBUF", "schema": text})
}

async fn registry(replies: Vec<Reply>, auth: bool) -> (String, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for reply in replies {
            let (mut stream, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut head = Vec::new();
            while !head.ends_with(b"\r\n\r\n") {
                let mut byte = [0];
                stream.read_exact(&mut byte).await.unwrap();
                head.push(byte[0]);
                assert!(head.len() <= 16_384);
            }
            let head = String::from_utf8(head).unwrap();
            assert_eq!(
                head.lines().next().unwrap(),
                format!("{} {} HTTP/1.1", reply.method, reply.path)
            );
            if auth {
                assert!(
                    head.lines()
                        .any(|line| line.eq_ignore_ascii_case("authorization: Basic dTpw"))
                );
            }
            let length = head
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap_or(0);
            let mut bytes = vec![0; length];
            stream.read_exact(&mut bytes).await.unwrap();
            if let Some(request) = reply.request {
                assert_eq!(serde_json::from_slice::<Json>(&bytes).unwrap(), request);
            }
            let body = reply.response.to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    (url, server)
}

fn converter(
    url: &str,
    is_key: bool,
    options: &[(&str, &str)],
) -> rtdb_types::Result<Box<dyn Converter>> {
    let role = if is_key { "key" } else { "value" };
    let mut properties = BTreeMap::from([
        (
            "key.converter".into(),
            "org.apache.kafka.connect.storage.StringConverter".into(),
        ),
        (
            "value.converter".into(),
            "org.apache.kafka.connect.json.JsonConverter".into(),
        ),
    ]);
    properties.insert(format!("{role}.converter"), CLASS.into());
    properties.insert(format!("{role}.converter.schema.registry.url"), url.into());
    for &(key, value) in options {
        properties.insert(format!("{role}.converter.{key}"), value.into());
    }
    let config = ConnectConfig::parse(&properties)?;
    PluginRegistry::default().converter(if is_key { &config.key } else { &config.value }, is_key)
}

fn framed(data: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0, 0, 0, 0, 7];
    bytes.extend_from_slice(data);
    bytes
}

fn context() -> ConversionContext {
    ConversionContext::new(MemoryBudget::new(128 << 20))
}

fn record(fields: impl Into<StructFields<Value>>) -> Value {
    Value::Struct(fields.into())
}

#[tokio::test]
async fn registry_reference_paths_and_nested_message_indexes_produce_connect_values() {
    let root = json!({"schemaType": "PROTOBUF", "schema": ROOT,
        "references": [{"name": "detail.proto", "subject": "detail/schema", "version": 2}]});
    let detail = json!({"schemaType": "PROTOBUF", "schema": DETAIL, "subject": "detail/schema", "version": 2,
        "references": [{"name": "code.proto", "subject": "code-schema", "version": 3}]});
    let (url, server) = registry(
        vec![
            get("/schemas/ids/7", root.clone()),
            get("/subjects/detail%2Fschema/versions/2?deleted=true", detail),
            get(
                "/subjects/code-schema/versions/3?deleted=true",
                registry_schema(CODE),
            ),
            lookup("events-value", 5, root),
        ],
        true,
    )
    .await;
    let mut converter = converter(
        &url,
        false,
        &[
            ("basic.auth.credentials.source", "USER_INFO"),
            ("basic.auth.user.info", "u:p"),
            ("enhanced.protobuf.schema.support", "true"),
        ],
    )
    .unwrap();

    let first = converter
        .decode("events", Some(&framed(&[0, 8, 7])), &mut context())
        .await
        .unwrap();
    assert_eq!(first.value, record([("id".into(), Value::I32(7))]));
    assert_eq!(first.schema.unwrap().name.as_deref(), Some("example.First"));

    server.await.unwrap();
    let event = converter
        .decode(
            "events",
            Some(&framed(&[
                4, 2, 2, 10, 4, 10, 2, 8, 42, 32, 7, 40, 1, 50, 2, 1, 4,
            ])),
            &mut context(),
        )
        .await
        .unwrap();
    let schema = event.schema.unwrap();
    assert_eq!(schema.name.as_deref(), Some("example.Envelope.Event"));
    assert_eq!(schema.version, Some(5));
    assert_eq!(
        schema.field("detail").unwrap().schema.name.as_deref(),
        Some("ref.Detail")
    );
    assert_eq!(
        schema.field("state").unwrap().schema.parameters["io.confluent.connect.protobuf.Enum.DONE"],
        "1"
    );
    assert_eq!(
        event.value,
        record([
            (
                "identifier_0".into(),
                record([
                    ("name".into(), Value::Null),
                    ("number".into(), Value::I64(7))
                ])
            ),
            (
                "detail".into(),
                record([(
                    "wrapped".into(),
                    record([("number".into(), Value::I64(42))])
                )])
            ),
            ("absent".into(), Value::Null),
            ("state".into(), Value::Utf8("DONE".into())),
            (
                "deltas".into(),
                Value::list(vec![Value::I32(-1), Value::I32(2)])
            ),
        ])
    );
}

#[tokio::test]
async fn record_subject_strategy_uses_the_indexed_full_name_and_key_role() {
    let root = registry_schema(
        r#"syntax = "proto3"; package example; message Key { string id = 1; } message Other { bool flag = 1; }"#,
    );
    let (url, server) = registry(
        vec![
            get("/schemas/ids/7", root.clone()),
            lookup("events-example.Other", 3, root.clone()),
            get("/schemas/ids/7", root.clone()),
            lookup("events-example.Key", 9, root),
        ],
        false,
    )
    .await;
    let mut converter = converter(
        &url,
        true,
        &[(
            "key.subject.name.strategy",
            "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy",
        )],
    )
    .unwrap();
    let other = converter
        .decode("events", Some(&framed(&[2, 2, 8, 1])), &mut context())
        .await
        .unwrap();
    assert_eq!(other.schema.unwrap().version, Some(3));
    assert_eq!(other.value, record([("flag".into(), Value::Bool(true))]));
    let key = converter
        .decode("events", Some(&framed(&[0, 10, 1, b'a'])), &mut context())
        .await
        .unwrap();
    assert_eq!(key.schema.unwrap().version, Some(9));
    assert_eq!(key.value, record([("id".into(), Value::Utf8("a".into()))]));
    server.await.unwrap();
}

#[tokio::test]
async fn tombstone_is_null_and_empty_message_keeps_scalar_default_and_absent_fields() {
    let root = registry_schema(
        r#"
        syntax = "proto3";
        import "google/protobuf/wrappers.proto";
        message Event {
            int32 id = 1;
            optional string optional_text = 2;
            google.protobuf.StringValue wrapper = 3;
        }
    "#,
    );
    let (url, server) = registry(
        vec![
            get("/schemas/ids/7", root.clone()),
            lookup("events-value", 1, root),
        ],
        false,
    )
    .await;
    let mut converter = converter(&url, false, &[("wrapper.for.nullables", "true")]).unwrap();
    assert_eq!(
        converter
            .decode("events", None, &mut context())
            .await
            .unwrap(),
        ConnectData::null()
    );
    let empty = converter
        .decode("events", Some(&framed(&[0])), &mut context())
        .await
        .unwrap();
    assert_eq!(
        empty.value,
        record([
            ("id".into(), Value::I32(0)),
            ("optional_text".into(), Value::Null),
            ("wrapper".into(), Value::Null)
        ])
    );
    let present = converter
        .decode("events", Some(&framed(&[0, 18, 0, 26, 0])), &mut context())
        .await
        .unwrap();
    assert_eq!(
        present.value,
        record([
            ("id".into(), Value::I32(0)),
            ("optional_text".into(), Value::Utf8(String::new())),
            ("wrapper".into(), Value::Utf8(String::new())),
        ])
    );
    let schema = present.schema.unwrap();
    assert!(!schema.field("id").unwrap().schema.optional);
    assert!(matches!(
        schema.field("wrapper").unwrap().schema.resolved().kind,
        SchemaKind::String
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn malformed_framing_payload_and_required_fields_fail() {
    let root = registry_schema(r#"syntax = "proto2"; message Event { required int32 id = 1; }"#);
    let (url, server) = registry(
        vec![
            get("/schemas/ids/7", root.clone()),
            lookup("events-value", 1, root),
        ],
        false,
    )
    .await;
    let mut converter = converter(&url, false, &[]).unwrap();
    let mut wrong_magic = framed(&[0, 8, 1]);
    wrong_magic[0] = 1;
    for bytes in [
        vec![],
        wrong_magic,
        framed(&[]),
        framed(&[1]),
        framed(&[2, 1]),
        framed(&[4, 0]),
        framed(&[0xff, 0xff, 0xff, 0xff, 0x10]),
    ] {
        let cause = converter
            .decode("events", Some(&bytes), &mut context())
            .await
            .unwrap_err();
        assert!(
            cause.code.ends_with("/conversion-error"),
            "{bytes:?} {cause}"
        );
    }

    let first = converter
        .decode("events", Some(&framed(&[0, 8, 1])), &mut context())
        .await
        .unwrap();
    assert_eq!(first.value, record([("id".into(), Value::I32(1))]));
    server.await.unwrap();

    for data in [&[2, 2][..], &[0, 8][..], &[0][..], &[0, 0][..]] {
        let cause = converter
            .decode("events", Some(&framed(data)), &mut context())
            .await
            .unwrap_err();
        assert!(
            cause.code.ends_with("/conversion-error"),
            "{data:?} {cause}"
        );
    }
}

#[tokio::test]
async fn unresolved_import_and_non_protobuf_registry_document_fail() {
    for root in [
        registry_schema(r#"syntax = "proto3"; import "missing.proto"; message Event {}"#),
        json!({"schemaType": "JSON", "schema": "{\"type\":\"string\"}"}),
    ] {
        let (url, server) = registry(vec![get("/schemas/ids/7", root)], false).await;
        let mut converter = converter(&url, false, &[]).unwrap();
        let cause = converter
            .decode("events", Some(&framed(&[0])), &mut context())
            .await
            .unwrap_err();
        assert!(cause.code.ends_with("/schema-registry"), "{cause}");
        server.await.unwrap();
    }
}

#[tokio::test]
async fn built_in_imports_keep_connect_metadata_logical_values_and_last_map_entry() {
    let root = registry_schema(
        r#"
        syntax = "proto3";
        import "confluent/meta.proto";
        import "google/type/date.proto";
        message Measurement {
            map<string, int32> counts = 1;
            int32 grade = 2 [(confluent.field_meta) = { params: { key: "connect.type", value: "int8" } }];
            google.type.Date day = 3;
        }
    "#,
    );
    let (url, server) = registry(
        vec![
            get("/schemas/ids/7", root.clone()),
            lookup("events-value", 1, root),
        ],
        false,
    )
    .await;
    let mut converter = converter(&url, false, &[]).unwrap();
    let datum = framed(&[
        0, 10, 5, 10, 1, b'a', 16, 1, 10, 5, 10, 1, b'a', 16, 2, 16, 130, 1, 26, 7, 8, 178, 15, 16,
        1, 24, 2,
    ]);
    let output = converter
        .decode("events", Some(&datum), &mut context())
        .await
        .unwrap();
    let schema = output.schema.unwrap();
    assert!(matches!(
        schema.field("grade").unwrap().schema.resolved().kind,
        SchemaKind::I8
    ));
    assert_eq!(
        schema.field("day").unwrap().schema.name.as_deref(),
        Some("org.apache.kafka.connect.data.Date")
    );
    assert_eq!(
        output.value,
        record([
            (
                "counts".into(),
                Value::Map {
                    entries: vec![(Value::Utf8("a".into()), Value::I32(2))],
                    sorted: false
                }
            ),
            ("grade".into(), Value::I8(-126)),
            (
                "day".into(),
                Value::Timestamp {
                    value: 86_400_000,
                    unit: rtdb_types::TimeUnit::Millisecond,
                    timezone: Some("UTC".into()),
                }
            ),
        ])
    );
    server.await.unwrap();
}

#[tokio::test]
async fn selected_reader_projects_its_schema_and_converter_options() {
    let root = registry_schema(
        r#"syntax = "proto3"; enum State { NEW = 0; DONE = 1; } message Event { State status = 1; }"#,
    );
    let reader = json!({"version": 3, "schemaType": "PROTOBUF",
        "schema": r#"syntax = "proto3"; enum State { NEW = 0; DONE = 1; } message Event { State state = 1; oneof extra { string name = 2; int32 code = 3; } }"#});
    let middle = json!({"version": 2, "schemaType": "PROTOBUF", "schema": root["schema"]});
    let (url, server) = registry(
        vec![
            get("/schemas/ids/7", root.clone()),
            lookup("events-value", 1, root),
            get("/subjects/events-value/versions/latest", reader),
            get("/subjects/events-value/versions/2?deleted=true", middle),
        ],
        false,
    )
    .await;
    let mut converter = converter(
        &url,
        false,
        &[
            ("use.latest.version", "true"),
            ("int.for.enums", "true"),
            ("flatten.unions", "true"),
        ],
    )
    .unwrap();
    let output = converter
        .decode("events", Some(&framed(&[0, 8, 1, 24, 7])), &mut context())
        .await
        .unwrap();
    assert_eq!(
        output.value,
        record([
            ("name".into(), Value::Null),
            ("code".into(), Value::I32(7)),
            ("state".into(), Value::I32(1))
        ])
    );
    assert_eq!(output.schema.unwrap().version, Some(3));
    server.await.unwrap();
}

#[test]
fn validates_protobuf_options_without_accessing_registry() {
    let url = "http://registry.invalid";
    assert!(converter(url, false, &[]).is_ok());
    for key in [
        "int.for.enums",
        "generate.index.for.unions",
        "optional.for.proto2",
        "wrapper.for.raw.primitives",
    ] {
        let result = converter(url, false, &[(key, "invalid")]);
        assert!(matches!(result, Err(cause) if cause.code.ends_with("/invalid-plugin-config")));
    }
    let generated = converter(url, false, &[("derive.type", "true")]);
    assert!(matches!(generated, Err(cause) if cause.code.ends_with("/invalid-plugin-config")));
}
