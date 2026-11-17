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

fn utf32_words(little: bool, words: &[u32]) -> Vec<u8> {
    words.iter().flat_map(|word| if little { word.to_le_bytes() } else { word.to_be_bytes() }).collect()
}

#[tokio::test]
async fn utf32_variants_and_all_java_aliases_decode_bmp_and_supplementary_values() {
    for (little, aliases) in [
        (false, &["UTF-32", "UTF_32", "UTF32", "UTF-32BE", "UTF_32BE", "X-UTF-32BE", "X-UTF-32BE-BOM", "UTF_32BE_BOM", "UTF-32BE-BOM"][..]),
        (true, &["UTF-32LE", "UTF_32LE", "X-UTF-32LE", "X-UTF-32LE-BOM", "UTF_32LE_BOM", "UTF-32LE-BOM"][..]),
    ] {
        let bytes = utf32_words(little, &[0, 0x61, 0xe9, 0x1f600, 0x10ffff, 0xffff]);
        for alias in aliases {
            assert_eq!(decode(alias, &bytes).await, "\0aé😀\u{10ffff}\u{ffff}", "{alias}");
            assert_eq!(decode(alias, &[]).await, "");
        }
    }
    assert_eq!(decode("uTf_32lE_bOm", &[0xe9, 0, 0, 0]).await, "é");
    for alias in ["UTF32BE", "UTF32LE", "UTF_32LE-BOM", "UTF-32-BOM"] {
        assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", alias)]), false).is_err());
    }
}

#[tokio::test]
async fn utf32_consumes_only_matching_initial_marks_and_auto_defaults_to_big_endian() {
    for (little, aliases) in [
        (false, &["UTF-32BE", "X-UTF-32BE-BOM"][..]),
        (true, &["UTF-32LE", "X-UTF-32LE-BOM"][..]),
    ] {
        let marked = utf32_words(little, &[0xfeff, 0x61, 0xfeff, 0xfffe]);
        assert_eq!(decode("UTF-32", &marked).await, "a\u{feff}\u{fffe}");
        for alias in aliases {
            assert_eq!(decode(alias, &marked).await, "a\u{feff}\u{fffe}");
            assert_eq!(decode(alias, &utf32_words(little, &[0xfeff])).await, "");
            assert_eq!(decode(alias, &utf32_words(little, &[0xfffe0000, 0x61])).await, "\u{fffd}a");
            assert_eq!(decode(alias, &utf32_words(little, &[0xfeff, 0xfffe0000, 0x61])).await, "\u{fffd}a");
        }
    }
    assert_eq!(decode("UTF-32", &[0, 0, 0, b'a']).await, "a");
    assert_eq!(decode("UTF-32", &[b'a', 0, 0, 0]).await, "\u{fffd}");
}

#[tokio::test]
async fn utf32_invalid_words_and_truncated_tails_have_separate_replacements() {
    for (little, aliases) in [
        (false, &["UTF-32", "UTF-32BE", "X-UTF-32BE-BOM"][..]),
        (true, &["UTF-32LE", "X-UTF-32LE-BOM"][..]),
    ] {
        for alias in aliases {
            let invalid = utf32_words(little, &[0x110000, 0x7fffffff, 0x80000000, 0xffffffff, 0x61]);
            assert_eq!(decode(alias, &invalid).await, "\u{fffd}\u{fffd}\u{fffd}\u{fffd}a");
            for tail in [&[0][..], &[0, 0][..], &[0, 0, 0][..]] {
                assert_eq!(decode(alias, tail).await, "\u{fffd}");
                let mut bytes = utf32_words(little, &[0xfeff, 0x61, 0xffffffff]);
                bytes.extend_from_slice(tail);
                assert_eq!(decode(alias, &bytes).await, "a\u{fffd}\u{fffd}");
            }
        }
    }
}

#[tokio::test]
async fn utf32_java_surrogate_units_pair_across_words_at_the_native_string_boundary() {
    for little in [false, true] {
        let encoding = if little { "UTF-32LE" } else { "UTF-32BE" };
        for (words, expected) in [
            (&[0xd83d, 0xde00][..], "😀"),
            (&[0xd800, 0xd801, 0xdc00][..], "\u{fffd}\u{10400}"),
            (&[0xd800, 0x61, 0xdc00][..], "\u{fffd}a\u{fffd}"),
            (&[0xd800, 0x1f600, 0xdc00][..], "\u{fffd}😀\u{fffd}"),
            (&[0xd800, 0xffffffff, 0xdc00][..], "\u{fffd}\u{fffd}\u{fffd}"),
        ] {
            assert_eq!(decode(encoding, &utf32_words(little, words)).await, expected);
        }
        let mut bytes = utf32_words(little, &[0xd800]);
        bytes.push(0);
        assert_eq!(decode(encoding, &bytes).await, "\u{fffd}\u{fffd}");
    }
}

#[tokio::test]
async fn utf32_decoder_byte_order_resets_for_each_record() {
    let mut converter = PluginRegistry::default().converter(&spec(&[("converter.encoding", "UTF-32")]), false).unwrap();
    let mut context = ConversionContext::new(MemoryBudget::new(1 << 20));
    for bytes in [utf32_words(true, &[0xfeff, 0x61]), utf32_words(false, &[0x61])] {
        assert_eq!(converter.decode("events", Some(&bytes), &mut context).await.unwrap().value, Value::Utf8("a".into()));
    }
    assert_eq!(converter.decode("events", None, &mut context).await.unwrap().value, Value::Null);
}

#[tokio::test]
async fn cesu8_aliases_decode_bmp_boundaries_and_preserve_marks() {
    let bytes = [0, 0x7f, 0xc2, 0x80, 0xdf, 0xbf, 0xe0, 0xa0, 0x80, 0xed, 0x9f, 0xbf, 0xee, 0x80, 0x80, 0xef, 0xbf, 0xbf];
    for alias in ["CESU-8", "CESU8", "csCESU-8", "cScEsU-8"] {
        assert_eq!(decode(alias, &bytes).await, "\0\u{7f}\u{80}\u{7ff}\u{800}\u{d7ff}\u{e000}\u{ffff}");
        assert_eq!(decode(alias, &[0xef, 0xbb, 0xbf, b'a', 0xef, 0xbb, 0xbf]).await, "\u{feff}a\u{feff}");
        assert_eq!(decode(alias, &[]).await, "");
    }
    for alias in ["CESU_8", "CESU", "csCESU8"] {
        assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", alias)]), false).is_err());
    }
}

#[tokio::test]
async fn cesu8_replacement_groups_follow_java_array_decoder_boundaries() {
    for (bytes, expected) in [
        (&[0xc0, 0x80, 0xc1, 0xbf][..], "\u{fffd}\u{fffd}\u{fffd}\u{fffd}"),
        (&[0xc2][..], "\u{fffd}"),
        (&[0xc2, b'a'][..], "\u{fffd}a"),
        (&[0xe0][..], "\u{fffd}"),
        (&[0xe0, 0x9f][..], "\u{fffd}\u{fffd}"),
        (&[0xe0, 0x9f, 0x80][..], "\u{fffd}\u{fffd}\u{fffd}"),
        (&[0xe0, 0xa0][..], "\u{fffd}"),
        (&[0xe0, 0xa0, b'a'][..], "\u{fffd}a"),
        (&[0xe1, b'a', 0x80][..], "\u{fffd}a\u{fffd}"),
        (&[0xe1, 0x80, b'a'][..], "\u{fffd}a"),
        (&[0xed, 0xa0][..], "\u{fffd}"),
        (&[0xed, 0xa0, b'a'][..], "\u{fffd}a"),
        (&[0xef, 0xbf][..], "\u{fffd}"),
        (&[0xe1, 0xc2, 0x80][..], "\u{fffd}\u{80}"),
    ] {
        assert_eq!(decode("CESU-8", bytes).await, expected, "{bytes:?}");
    }
}

#[tokio::test]
async fn cesu8_rejects_four_byte_utf8_sequences_one_byte_at_a_time() {
    for bytes in [
        &[0xf0, 0x90, 0x80, 0x80][..],
        &[0xf0, 0x9f, 0x98, 0x80][..],
        &[0xf4, 0x8f, 0xbf, 0xbf][..],
        &[0xf5, 0x80, 0x80, 0x80][..],
    ] {
        assert_eq!(decode("CESU-8", bytes).await, "\u{fffd}\u{fffd}\u{fffd}\u{fffd}");
    }
    assert_eq!(decode("CESU-8", &[0xf0, 0x90]).await, "\u{fffd}\u{fffd}");
    assert_eq!(decode("CESU-8", &[0xff, b'a', 0xfe]).await, "\u{fffd}a\u{fffd}");
}

#[tokio::test]
async fn cesu8_surrogate_units_pair_only_when_adjacent() {
    for (bytes, expected) in [
        (&[0xed, 0xa0, 0xbd, 0xed, 0xb8, 0x80][..], "😀"),
        (&[0xed, 0xa0, 0x80, 0xed, 0xa0, 0x81, 0xed, 0xb0, 0x80][..], "\u{fffd}\u{10400}"),
        (&[0xed, 0xa0, 0x80, b'a', 0xed, 0xb0, 0x80][..], "\u{fffd}a\u{fffd}"),
        (&[0xed, 0xa0, 0x80, 0xff, 0xed, 0xb0, 0x80][..], "\u{fffd}\u{fffd}\u{fffd}"),
        (&[0xed, 0xb0, 0x80, 0xed, 0xa0, 0x80][..], "\u{fffd}\u{fffd}"),
        (&[0xed, 0xa0, 0x80, 0xe1, 0x80][..], "\u{fffd}\u{fffd}"),
    ] {
        assert_eq!(decode("CESU-8", bytes).await, expected, "{bytes:?}");
    }
}

#[tokio::test]
async fn cesu8_decoder_resets_surrogates_and_partial_sequences_per_record() {
    let mut converter = PluginRegistry::default().converter(&spec(&[("converter.encoding", "CESU-8")]), false).unwrap();
    let mut context = ConversionContext::new(MemoryBudget::new(1 << 20));
    for bytes in [&[0xed, 0xa0, 0xbd][..], &[0xed, 0xb8, 0x80][..], &[0xe1, 0x80][..], &[0x80][..]] {
        assert_eq!(converter.decode("events", Some(bytes), &mut context).await.unwrap().value, Value::Utf8("\u{fffd}".into()));
    }
    assert_eq!(converter.decode("events", None, &mut context).await.unwrap().value, Value::Null);
}

#[tokio::test]
async fn cesu8_decoder_admits_replacement_storage_before_decoding() {
    let mut converter = PluginRegistry::default().converter(&spec(&[("converter.encoding", "CESU-8")]), false).unwrap();
    let bytes = [0xff; 1024];
    let mut rejected = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4 - 1));
    assert!(converter.decode("events", Some(&bytes), &mut rejected).await.is_err());
    let mut admitted = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4));
    assert_eq!(converter.decode("events", Some(&bytes), &mut admitted).await.unwrap().value, Value::Utf8("\u{fffd}".repeat(bytes.len())));
}
