use std::collections::BTreeMap;
use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConnectConfig, ConversionContext, PluginRegistry, PluginSpec, SchemaKind};
use rtdb_types::Value;

fn spec(options: &[(&str, &str)]) -> PluginSpec {
    PluginSpec {
        class_name: "org.apache.kafka.connect.storage.StringConverter".into(),
        options: options.iter().map(|(key, value)| ((*key).into(), (*value).into())).collect(),
    }
}

async fn decode(encoding: &str, bytes: &[u8]) -> String {
    let mut converter = PluginRegistry::default().converter(&spec(&[("converter.encoding", encoding)]), false).unwrap();
    let mut context = ConversionContext::new(MemoryBudget::new(1 << 20));
    let data = converter.decode("events", Some(bytes), &mut context).await.unwrap();
    let schema = data.schema.unwrap();
    assert!(schema.optional);
    assert!(matches!(schema.kind, SchemaKind::String));
    let Value::Utf8(text) = data.value else { panic!("expected string") };
    text
}

#[tokio::test]
async fn string_encoding_selection_preserves_key_and_value_overrides() {
    let config = ConnectConfig::parse(&BTreeMap::from([
        ("key.converter".into(), "org.apache.kafka.connect.storage.StringConverter".into()),
        ("value.converter".into(), "org.apache.kafka.connect.storage.StringConverter".into()),
        ("key.converter.converter.encoding".into(), "UTF-8".into()),
        ("key.converter.key.deserializer.encoding".into(), "ISO-8859-1".into()),
        ("key.converter.value.deserializer.encoding".into(), "US-ASCII".into()),
        ("value.converter.converter.encoding".into(), "ISO-8859-1".into()),
        ("value.converter.key.deserializer.encoding".into(), "UTF-8".into()),
        ("value.converter.value.deserializer.encoding".into(), "US-ASCII".into()),
    ])).unwrap();
    let plugins = PluginRegistry::default();
    let mut key = plugins.converter(&config.key, true).unwrap();
    let mut value = plugins.converter(&config.value, false).unwrap();
    let mut context = ConversionContext::new(MemoryBudget::new(1 << 20));
    assert_eq!(key.decode("events", Some(&[0xe9]), &mut context).await.unwrap().value, Value::Utf8("é".into()));
    assert_eq!(value.decode("events", Some(&[0xe9]), &mut context).await.unwrap().value, Value::Utf8("\u{fffd}".into()));
}

#[tokio::test]
async fn converter_encoding_replaces_generic_serializer_options_and_external_type() {
    let plugins = PluginRegistry::default();
    let mut converter = plugins.converter(&spec(&[
        ("converter.type", "invalid"),
        ("converter.encoding", " ISO-8859-1 "),
        ("serializer.encoding", "invalid"),
        ("deserializer.encoding", "UTF-8"),
        ("key.serializer.encoding", "invalid"),
        ("key.deserializer.encoding", "invalid"),
    ]), false).unwrap();
    let mut context = ConversionContext::new(MemoryBudget::new(1 << 20));
    assert_eq!(converter.decode("events", Some(&[0xe9]), &mut context).await.unwrap().value, Value::Utf8("é".into()));
    assert_eq!(converter.decode("events", None, &mut context).await.unwrap().value, Value::Null);
    assert_eq!(converter.decode("events", Some(&[]), &mut context).await.unwrap().value, Value::Utf8(String::new()));
}

#[test]
fn selected_serializer_and_deserializer_encodings_are_both_validated() {
    let plugins = PluginRegistry::default();
    for is_key in [false, true] {
        for property in if is_key { ["key.serializer.encoding", "key.deserializer.encoding"] } else { ["value.serializer.encoding", "value.deserializer.encoding"] } {
            for invalid in ["no-such-encoding", " UTF-8 ", ""] {
                assert!(plugins.converter(&spec(&[(property, invalid)]), is_key).is_err(), "{property}={invalid:?}");
            }
        }
        assert!(plugins.converter(&spec(&[
            ("converter.encoding", "invalid"),
            ("key.serializer.encoding", "UTF-8"),
            ("key.deserializer.encoding", "UTF-8"),
            ("value.serializer.encoding", "UTF-8"),
            ("value.deserializer.encoding", "UTF-8"),
        ]), is_key).is_ok());
    }
}

#[tokio::test]
async fn encoding_config_trims_java_control_characters_but_not_unicode_spaces() {
    assert_eq!(decode("\0\tUTF-8\r\u{1f}", b"text").await, "text");
    for encoding in ["\u{a0}UTF-8", "UTF-8\u{2003}"] {
        assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", encoding)]), false).is_err());
    }
}

#[tokio::test]
async fn utf8_replacement_consumes_java_malformed_sequences() {
    for (bytes, expected) in [
        (&[0xed, 0xa0, 0x80][..], "\u{fffd}"),
        (&[0xed, 0xbf, 0xbf][..], "\u{fffd}"),
        (&[0xed, 0xa0, b'a'][..], "\u{fffd}a"),
        (&[0xf0, 0x80, 0x80, 0x80][..], "\u{fffd}\u{fffd}\u{fffd}\u{fffd}"),
        (&[0xe2, 0x82][..], "\u{fffd}"),
        (&[0xf0, 0x9f, 0x98, 0x80][..], "😀"),
        (&[0xef, 0xbb, 0xbf, b'x'][..], "\u{feff}x"),
    ] {
        assert_eq!(decode("UTF-8", bytes).await, expected, "{bytes:?}");
    }
}

#[tokio::test]
async fn utf16_replacement_consumes_malformed_pairs_and_truncated_tails() {
    for (units, odd, expected) in [
        (&[0xd800, 0x0061, 0x0062][..], false, "\u{fffd}b"),
        (&[0xd800, 0xd801, 0xdc00][..], false, "\u{fffd}\u{fffd}"),
        (&[0xdc00, 0x0061][..], false, "\u{fffd}a"),
        (&[0xd800][..], true, "\u{fffd}"),
        (&[0x0061][..], true, "a\u{fffd}"),
        (&[0xd83d, 0xde00][..], false, "😀"),
    ] {
        for little in [false, true] {
            let mut bytes = Vec::new();
            for unit in units { bytes.extend(if little { u16::to_le_bytes(*unit) } else { u16::to_be_bytes(*unit) }); }
            if odd { bytes.push(0); }
            let encoding = if little { "UTF-16LE" } else { "UTF-16BE" };
            assert_eq!(decode(encoding, &bytes).await, expected, "{encoding} {bytes:?}");
        }
    }
}

#[tokio::test]
async fn utf16_bom_and_fixed_order_decoders_have_distinct_mark_handling() {
    assert_eq!(decode("UTF-16", &[0xff, 0xfe, 0x61, 0]).await, "a");
    assert_eq!(decode("UTF-16", &[0xfe, 0xff, 0, 0x61]).await, "a");
    assert_eq!(decode("UTF-16", &[0, 0x61]).await, "a");
    assert_eq!(decode("UnicodeLittle", &[0x61, 0]).await, "a");
    assert_eq!(decode("UnicodeLittle", &[0xfe, 0xff, 0, 0x61]).await, "a");
    assert_eq!(decode("UnicodeBigUnmarked", &[0xfe, 0xff, 0, 0x61]).await, "\u{feff}a");
    assert_eq!(decode("UnicodeLittleUnmarked", &[0xff, 0xfe, 0x61, 0]).await, "\u{feff}a");
    assert_eq!(decode("UTF-16", &[0, 0x61, 0xfe, 0xff]).await, "a\u{feff}");
}

#[tokio::test]
async fn standard_java_charset_aliases_select_the_same_decoders() {
    for alias in ["UTF8", "unicode-1-1-utf-8"] { assert_eq!(decode(alias, "é".as_bytes()).await, "é"); }
    for alias in ["ISO_8859-1:1987", "cp819", "Latin1", "IBM-819"] { assert_eq!(decode(alias, &[0xe9]).await, "é"); }
    for alias in ["646", "ANSI_X3.4-1968", "iso_646.irv:1983", "ascii7"] { assert_eq!(decode(alias, &[0xe9, b'a']).await, "\u{fffd}a"); }
    for alias in ["utf16", "UTF_16", "UnicodeBig"] { assert_eq!(decode(alias, &[0, b'a']).await, "a"); }
}
