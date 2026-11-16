use std::collections::BTreeMap;
use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConversionContext, PluginRegistry, PluginSpec};
use rtdb_types::{TimeUnit, Value};

struct Decoded { value: Value, _context: ConversionContext }

async fn decode(payload: &str, schema: Option<&str>) -> rtdb_types::Result<Decoded> {
    let options = match schema {
        Some(schema) => BTreeMap::from([("schema.content".into(), schema.into())]),
        None => BTreeMap::from([("schemas.enable".into(), "false".into())]),
    };
    let spec = PluginSpec { class_name: "org.apache.kafka.connect.json.JsonConverter".into(), options };
    let mut converter = PluginRegistry::default().converter(&spec, false)?;
    let mut context = ConversionContext::new(MemoryBudget::new(1 << 24));
    let value = converter.decode("numbers", Some(payload.as_bytes()), &mut context).await?.value;
    Ok(Decoded { value, _context: context })
}

#[tokio::test]
async fn generic_json_narrows_big_integers_without_reclassifying_them_as_doubles() {
    assert_eq!(decode("18446744073709551617", None).await.unwrap().value, Value::I64(1));
    assert_eq!(decode("-18446744073709551617", Some(r#"{"type":"int64"}"#)).await.unwrap().value, Value::I64(-1));
    assert_eq!(decode("4294967297", Some(r#"{"type":"int32"}"#)).await.unwrap().value, Value::I32(1));
    assert_eq!(decode("1e100", Some(r#"{"type":"int32"}"#)).await.unwrap().value, Value::I32(i32::MAX));
    assert_eq!(decode("null", Some(r#"{"type":"int64","default":18446744073709551617}"#)).await.unwrap().value, Value::I64(1));
    assert_eq!(decode("18014399583223809", Some(r#"{"type":"float"}"#)).await.unwrap().value, Value::F32(f32::from_bits(0x5a800001)));
}

#[tokio::test]
async fn json_logical_dates_require_int_nodes_and_timestamps_wrap_integral_nodes() {
    let date = r#"{"type":"int32","name":"org.apache.kafka.connect.data.Date"}"#;
    let time = r#"{"type":"int32","name":"org.apache.kafka.connect.data.Time"}"#;
    let timestamp = r#"{"type":"int64","name":"org.apache.kafka.connect.data.Timestamp"}"#;
    let millis = |value| Value::Timestamp { value, unit: TimeUnit::Millisecond, timezone: Some("UTC".into()) };
    assert_eq!(decode("1", Some(date)).await.unwrap().value, millis(86_400_000));
    for invalid in ["1.0", "2147483648", "18446744073709551617"] { assert!(decode(invalid, Some(date)).await.is_err()); }
    assert_eq!(decode("86400000", Some(time)).await.unwrap().value, millis(86_400_000));
    assert!(decode("86400001", Some(time)).await.is_err());
    assert!(decode("-1", Some(time)).await.is_err());
    assert_eq!(decode("18446744073709551617", Some(timestamp)).await.unwrap().value, millis(1));
    assert!(decode("1.0", Some(timestamp)).await.is_err());
}
