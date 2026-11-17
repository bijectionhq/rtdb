use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConversionContext, Converter, PluginRegistry, PluginSpec, SchemaKind};
use rtdb_types::Value;

const PROBES: &str = include_str!("../src/connect/converter/charset/double_byte/jis_auto_detect/probes.tsv");

fn spec(options: &[(&str, &str)]) -> PluginSpec {
    PluginSpec {
        class_name: "org.apache.kafka.connect.storage.StringConverter".into(),
        options: options.iter().map(|(key, value)| ((*key).into(), (*value).into())).collect(),
    }
}

fn open(name: &str) -> Box<dyn Converter> {
    PluginRegistry::default().converter(&spec(&[("converter.encoding", name)]), false).unwrap()
}

async fn converted(converter: &mut dyn Converter, bytes: Option<&[u8]>) -> Value {
    let mut context = ConversionContext::new(MemoryBudget::new(1 << 20));
    let data = converter.decode("events", bytes, &mut context).await.unwrap();
    let schema = data.schema.unwrap();
    assert!(schema.optional);
    assert!(matches!(schema.kind, SchemaKind::String));
    data.value
}

async fn decode(converter: &mut dyn Converter, bytes: &[u8]) -> String {
    let Value::Utf8(text) = converted(converter, Some(bytes)).await else { panic!("expected string") };
    text
}

#[tokio::test]
async fn both_provider_names_and_extended_unicode_equivalents_decode_every_single_byte() {
    let expected: Vec<_> = PROBES.lines().skip(1).find(|line| line.starts_with("empty\t")).unwrap().split('\t').collect();
    for name in ["x-JISAutoDetect", "JISAutoDetect", "X-JISAUTODETECT", "jisautodetect", "x-JİSAutoDetect", "JıſAutoDetect", "x-JİſAutoDetect"] {
        let mut converter = open(name);
        let mut digest = openssl::sha::Sha256::new();
        for byte in 0u8..=255 {
            digest.update(decode(&mut *converter, &[byte]).await.as_bytes());
            digest.update(&[0]);
        }
        let hash: String = digest.finish().iter().map(|byte| format!("{byte:02x}")).collect();
        assert_eq!(hash, expected[3], "{name}");
        assert_eq!(decode(&mut *converter, b"\x81\x60").await, "〜");
    }
}

#[tokio::test]
async fn converter_selects_the_pinned_iso_euc_and_shift_jis_replacement_paths() {
    let mut converter = open("x-JISAutoDetect");
    for (bytes, text) in [
        (&b"\x1b$B$\""[..], "あ"),
        (&b"\x1b$B$"[..], "\u{fffd}"),
        (&b"\x1b$"[..], "\u{fffd}"),
        (&b"\x1b(J\x80A"[..], "\x1b(J\u{fffd}A"),
        (&b"\x80 "[..], "\u{fffd} "),
        (&b"\x8f\0"[..], "\u{fffd}"),
        (&b"\xa1\xe0"[..], "÷"),
        (&b"\xa1"[..], "｡"),
        (&b"\x8f"[..], "\u{fffd}"),
        (&b"\xa4\xa2\xa4\xa4"[..], "あい"),
        (&b"\xa4\xa2"[..], "､｢"),
        (&b"\x8e\xa6\x8e\xa7"[..], "ｦｧ"),
        (&b"\xa4\xa2\x8e\xa6"[..], "､｢示"),
        (&b"\x8e\xa1\x8e\xa4"[..], "治痔"),
        (&b"\xa5\xa2\xa5\xa4"[..], "･｢･､"),
    ] { assert_eq!(decode(&mut *converter, bytes).await, text, "{bytes:x?}"); }
}

#[tokio::test]
async fn ascii_prefix_shifts_and_decoder_selection_reset_at_each_record_boundary() {
    let mut converter = open("JISAutoDetect");
    assert_eq!(decode(&mut *converter, b"A\0\x7f\x0e\x0f").await, "A\0\x7f\x0e\x0f");
    assert_eq!(decode(&mut *converter, b"\x0e\x1b(J\\~").await, "\x0e¥‾");
    assert_eq!(decode(&mut *converter, b"\x1b$B$\"").await, "あ");
    assert_eq!(decode(&mut *converter, b"$\"").await, "$\"");
    assert_eq!(decode(&mut *converter, b"\xa4\xa2\xa4\xa4").await, "あい");
    assert_eq!(decode(&mut *converter, b"\xa4\xa2").await, "､｢");
}

#[tokio::test]
async fn null_and_empty_records_preserve_optional_string_schema_without_retaining_detection() {
    let mut converter = open("x-JISAutoDetect");
    assert_eq!(converted(&mut *converter, None).await, Value::Null);
    assert_eq!(converted(&mut *converter, Some(&[])).await, Value::Utf8(String::new()));
    assert_eq!(decode(&mut *converter, b"\x1b$B$\"").await, "あ");
    assert_eq!(converted(&mut *converter, None).await, Value::Null);
    assert_eq!(decode(&mut *converter, b"$\"").await, "$\"");
}

#[tokio::test]
async fn detector_configuration_keeps_provider_punctuation_trimming_and_key_value_precedence() {
    let mut trimmed = open("\0\tx-JİSAutoDetect\r\u{1f}");
    assert_eq!(decode(&mut *trimmed, b"\xa4\xa2\xa4\xa4").await, "あい");
    for invalid in ["JIS-AutoDetect", "JIS_AutoDetect", "x-JIS-AutoDetect", " x-JISAutoDetect ", "x-JISAutoDetect\0", "x-JI\u{307}SAutoDetect"] {
        assert!(PluginRegistry::default().converter(&spec(&[("value.deserializer.encoding", invalid)]), false).is_err(), "{invalid}");
    }
    let options = spec(&[
        ("converter.encoding", "ascii"),
        ("key.serializer.encoding", "JISAutoDetect"),
        ("key.deserializer.encoding", "x-JİSAutoDetect"),
        ("value.serializer.encoding", "x-JISAutoDetect"),
        ("value.deserializer.encoding", "UTF-8"),
    ]);
    let mut key = PluginRegistry::default().converter(&options, true).unwrap();
    let mut value = PluginRegistry::default().converter(&options, false).unwrap();
    assert_eq!(decode(&mut *key, b"\xa4\xa2\xa4\xa4").await, "あい");
    assert_eq!(decode(&mut *value, b"\xa4\xa2\xa4\xa4").await, "\u{fffd}\u{fffd}\u{fffd}\u{fffd}");
    for (is_key, serializer) in [(true, "key.serializer.encoding"), (false, "value.serializer.encoding")] {
        assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", "JISAutoDetect"), (serializer, "JIS-AutoDetect")]), is_key).is_err());
    }
}

#[tokio::test]
async fn probing_and_shared_output_obey_the_existing_scalar_admission_before_decode() {
    let mut converter = open("JISAutoDetect");
    for (bytes, expected) in [
        (b"\xa4\xa2".repeat(1024), "あ".repeat(1024)),
        (b"\x8e\xa6".repeat(1024), "ｦ".repeat(1024)),
        (vec![0x80; 1024], "\u{fffd}".repeat(1024)),
        (b"\x1b$B".repeat(1024), String::new()),
        (b"\x0e\x0f".repeat(1024), "\x0e\x0f".repeat(1024)),
        (vec![0x8f], "\u{fffd}".to_owned()),
        (vec![], String::new()),
    ] {
        let bound = 16_384 + bytes.len() * 4;
        let rejected_memory = MemoryBudget::new(bound - 1);
        let mut rejected = ConversionContext::new(rejected_memory);
        assert!(converter.decode("events", Some(&bytes), &mut rejected).await.is_err());
        let admitted_memory = MemoryBudget::new(bound);
        let mut admitted = ConversionContext::new(admitted_memory);
        assert_eq!(converter.decode("events", Some(&bytes), &mut admitted).await.unwrap().value, Value::Utf8(expected));
    }
}
