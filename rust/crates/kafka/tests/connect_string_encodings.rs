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

const SINGLE_BYTE_MANIFEST: &str = include_str!("../src/connect/converter/charset/single_byte/charsets.tsv");

fn sha256_hex(bytes: &[u8]) -> String {
    openssl::sha::sha256(bytes).iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn single_byte_resources_retain_pinned_source_and_mapping_provenance() {
    let mappings = include_bytes!("../src/connect/converter/charset/single_byte/java25-single-byte.bin");
    assert_eq!(mappings.len(), 102 * 512);
    assert_eq!(sha256_hex(mappings), "29b4d331391a6e6505f5c10f8bfcd6516f4d5f09e22380502ee991d5dcdb195b");
    assert_eq!(sha256_hex(SINGLE_BYTE_MANIFEST.as_bytes()), "b676ed640f72bad381b6ab3e80b977da514b9ea727c8b488963bab275fec7b20");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/single_byte/aliases.rs")), "1640af5230a0f41278500e2e9c7c7d61b538d41d56900b73b017b6664a0a1a91");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/single_byte/sources.sha256")), "f3f9619fd9954c2257a494c34a0bd90a6d520549ab17916ca0a681a5bc3af5bb");
    assert_eq!(SINGLE_BYTE_MANIFEST.lines().skip(1).count(), 102);
    for (index, line) in SINGLE_BYTE_MANIFEST.lines().skip(1).enumerate() {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 9);
        assert_eq!(fields[3].len(), 64);
        assert!(fields[3].bytes().all(|byte| byte.is_ascii_hexdigit()));
        let table = &mappings[index * 512..(index + 1) * 512];
        assert_eq!(sha256_hex(table), fields[6], "{}", fields[0]);
        let units: Vec<_> = table.chunks_exact(2).map(|unit| u16::from_be_bytes([unit[0], unit[1]])).collect();
        assert!(units.iter().all(|unit| !(0xd800..=0xdfff).contains(unit)));
        if fields[4] == "true" { assert_eq!(&units[..128], &(0u16..128).collect::<Vec<_>>()); }
        if fields[5] == "true" { assert!(units.iter().all(|unit| *unit <= 255)); }
    }
}

#[tokio::test]
async fn single_byte_canonical_names_and_every_provider_alias_decode_all_byte_values() {
    let bytes: Vec<_> = (0u8..=255).collect();
    let mut names = std::collections::BTreeSet::new();
    for line in SINGLE_BYTE_MANIFEST.lines().skip(1) {
        let fields: Vec<_> = line.split('\t').collect();
        for name in std::iter::once(fields[0]).chain(fields[8].split('|').filter(|alias| !alias.is_empty())) {
            names.insert(name.to_ascii_lowercase());
            for selected in [name.to_owned(), name.to_ascii_uppercase()] {
                let text = decode(&selected, &bytes).await;
                assert_eq!(text.chars().count(), 256, "{selected}");
                assert_eq!(sha256_hex(text.as_bytes()), fields[7], "{selected}");
                assert_eq!(decode(&selected, &[]).await, "");
            }
        }
    }
    assert_eq!(names.len(), 589);
}

#[tokio::test]
async fn single_byte_families_retain_distinct_symbols_scripts_and_private_use_values() {
    for (encoding, bytes, expected) in [
        ("windows-1252", &[0x80, 0x91, 0x92, 0x93, 0x94][..], "€‘’“”"),
        ("ISO-8859-1", &[0x80, 0xa4][..], "\u{80}¤"),
        ("ISO-8859-15", &[0xa4, 0xbc, 0xbd, 0xbe][..], "€ŒœŸ"),
        ("ISO-8859-2", &[0xa1, 0xb1, 0xc8][..], "ĄąČ"),
        ("ISO-8859-7", &[0xa4, 0xa5, 0xc1][..], "€₯Α"),
        ("windows-1251", &[0xc0, 0xe0, 0x88][..], "Аа€"),
        ("KOI8-R", &[0xe1, 0xc1, 0xb3][..], "АаЁ"),
        ("KOI8-U", &[0xa4, 0xb4][..], "єЄ"),
        ("IBM437", &[0x80, 0xb3, 0xdb][..], "Ç│█"),
        ("IBM850", &[0xd5][..], "ı"),
        ("IBM00858", &[0xd5][..], "€"),
        ("IBM037", &[0x9f][..], "¤"),
        ("IBM01140", &[0x9f][..], "€"),
        ("JIS_X0201", &[0x5c, 0x7e, 0xa1, 0xdf][..], "\\~｡ﾟ"),
        ("TIS-620", &[0xa0, 0xa1, 0xdf][..], "\u{a0}ก฿"),
        ("x-windows-874", &[0x80, 0xa0, 0xa1, 0xdf][..], "€\u{a0}ก฿"),
        ("x-MacRoman", &[0x80, 0xdb, 0xf0][..], "Ä€\u{f8ff}"),
        ("x-MacSymbol", &[0x41, 0x57, 0x61, 0x77][..], "ΑΩαω"),
        ("x-MacDingbat", &[0x21, 0x22, 0x23][..], "✁✂✃"),
    ] { assert_eq!(decode(encoding, bytes).await, expected, "{encoding}"); }
}

#[tokio::test]
async fn single_byte_ebcdic_controls_and_encoder_only_corrections_remain_decodable() {
    assert_eq!(decode("IBM037", &[0x04, 0x05, 0x15, 0x25, 0x3f, 0x40, 0xc1, 0x81, 0xf0]).await, "\u{9c}\t\n\n\u{1a} Aa0");
    for name in ["IBM037", "IBM500", "IBM01140", "IBM01148"] {
        assert_eq!(decode(name, &[0x15, 0x25, 0x15, 0x25]).await, "\n\n\n\n", "{name}");
    }
    assert_eq!(decode("x-IBM874", &[0xa0, 0xdb, 0xdc, 0xdd, 0xde]).await, "\u{e48}\u{e49}\u{e4a}\u{e4b}\u{e4c}");
    assert_eq!(decode("IBM-Thai", &[0x25, 0x51, 0xca, 0xe1, 0xfd, 0xfe]).await, "\n\u{e48}\u{e49}\u{e4a}\u{e4b}\u{e4c}");
}

#[tokio::test]
async fn single_byte_unmappable_bytes_each_produce_one_replacement() {
    for (encoding, bytes, expected) in [
        ("windows-1252", &[0x81, 0x8d, b'a', 0x8f, 0x90, 0x9d][..], "\u{fffd}\u{fffd}a\u{fffd}\u{fffd}\u{fffd}"),
        ("ISO-8859-3", &[0xa5, 0xae, b'a'][..], "\u{fffd}\u{fffd}a"),
        ("JIS_X0201", &[0x80, 0x81, 0xa0, 0xa1, 0xe0, 0xff][..], "\u{fffd}\u{fffd}\u{fffd}｡\u{fffd}\u{fffd}"),
    ] { assert_eq!(decode(encoding, bytes).await, expected, "{encoding}"); }
}

#[tokio::test]
async fn single_byte_encodings_follow_key_and_value_precedence_and_serializer_validation() {
    let options = spec(&[
        ("converter.encoding", " no-such-encoding "),
        ("key.serializer.encoding", "MacRoman"),
        ("key.deserializer.encoding", "cp1251"),
        ("value.serializer.encoding", "cp037"),
        ("value.deserializer.encoding", "cp1252"),
    ]);
    let plugins = PluginRegistry::default();
    for (is_key, expected) in [(true, "Ђ"), (false, "€")] {
        let mut converter = plugins.converter(&options, is_key).unwrap();
        let mut context = ConversionContext::new(MemoryBudget::new(1 << 20));
        assert_eq!(converter.decode("events", Some(&[0x80]), &mut context).await.unwrap().value, Value::Utf8(expected.into()));
        assert_eq!(converter.decode("events", None, &mut context).await.unwrap().value, Value::Null);
        let serializer = if is_key { "key.serializer.encoding" } else { "value.serializer.encoding" };
        for invalid in ["cp-1252", "windows1252", " MacRoman ", "no-such-encoding"] {
            let mut invalid_options = options.clone();
            invalid_options.options.insert(serializer.into(), invalid.into());
            assert!(plugins.converter(&invalid_options, is_key).is_err(), "{serializer}={invalid}");
        }
    }
    assert_eq!(decode("\0\tCp1252\r\u{1f}", &[0x80]).await, "€");
}

#[test]
fn single_byte_lookup_rejects_unregistered_spellings() {
    for encoding in ["windows1252", "cp-1252", "latin_9", "Mac-Roman", "IBM01150"] {
        assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", encoding)]), false).is_err(), "{encoding}");
    }
}

#[tokio::test]
async fn single_byte_decoders_admit_worst_case_storage_before_decoding() {
    for (encoding, byte, expected) in [("windows-1252", 0x81, "\u{fffd}"), ("JIS_X0201", 0xa1, "｡"), ("x-MacRoman", 0xf0, "\u{f8ff}")] {
        let mut converter = PluginRegistry::default().converter(&spec(&[("converter.encoding", encoding)]), false).unwrap();
        let bytes = [byte; 1024];
        let mut rejected = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4 - 1));
        assert!(converter.decode("events", Some(&bytes), &mut rejected).await.is_err());
        let mut admitted = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4));
        assert_eq!(converter.decode("events", Some(&bytes), &mut admitted).await.unwrap().value, Value::Utf8(expected.repeat(bytes.len())));
    }
}

const DOUBLE_BYTE_MANIFEST: &str = include_str!("../src/connect/converter/charset/double_byte/charsets.tsv");

#[test]
fn double_byte_resources_retain_pinned_source_and_mapping_provenance() {
    let mappings: [&[u8]; 20] = [
        include_bytes!("../src/connect/converter/charset/double_byte/big5.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/euc-kr.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/gb2312.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/gbk.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/shift_jis.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/windows-31j.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/x-ibm1381.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/x-ibm942.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/x-ibm942c.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/x-ibm943.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/x-ibm943c.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/x-ibm948.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/x-ibm949.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/x-ibm949c.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/x-ibm950.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/x-johab.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/x-mswin-936.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/x-pck.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/x-windows-949.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/x-windows-950.bin"),
    ];
    assert_eq!(sha256_hex(DOUBLE_BYTE_MANIFEST.as_bytes()), "764aeaeda584221a3723d9ad51fed7b6ef023650261f9bd9fdd0bfb770c5e67d");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/aliases.rs")), "aecbe513193975e92ae16ff1be8e0b722117cbab990f081d892c7297c800a19b");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/tables.rs")), "2d65d771e3706002c5ca60f988cf75a9707033a446d1ed107c60af268fd983da");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/sources.sha256")), "7e4ddd5ae3690e645ba3fb2827bea0723e3afef51f9eab350a736ac9b347ce54");
    assert_eq!(DOUBLE_BYTE_MANIFEST.lines().skip(1).count(), 20);
    for (line, mapping) in DOUBLE_BYTE_MANIFEST.lines().skip(1).zip(mappings) {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 13);
        assert_eq!(fields[3].len(), 64);
        assert!(fields[3].bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(mapping.len(), fields[8].parse::<usize>().unwrap());
        assert_eq!(sha256_hex(mapping), fields[9]);
        assert_eq!(mapping[0], u8::from_str_radix(fields[5], 16).unwrap());
        assert_eq!(mapping[1], u8::from_str_radix(fields[6], 16).unwrap());
        let unit = |offset: usize| u16::from_be_bytes([mapping[offset], mapping[offset + 1]]);
        let rows = fields[7].parse::<usize>().unwrap();
        let width = usize::from(mapping[1] - mapping[0]) + 1;
        assert!(mapping[0] > 0);
        assert_eq!(unit(2), 0);
        assert_eq!(mapping.len(), 1026 + rows * width * 2);
        assert_eq!((0..256).filter(|index| unit(514 + index * 2) != 0xffff).count(), rows);
        for index in 0..256 {
            let row = unit(514 + index * 2);
            assert!(row == 0xffff || usize::from(row) < rows);
            assert!(!(0xd800..=0xdfff).contains(&unit(2 + index * 2)));
            if index < 128 {
                assert_ne!(unit(2 + index * 2), 0xfffd);
                if fields[4] == "true" { assert_eq!(unit(2 + index * 2), index as u16); }
            }
        }
        for pair in mapping[1026..].chunks_exact(2) {
            assert!(!(0xd800..=0xdfff).contains(&u16::from_be_bytes([pair[0], pair[1]])));
        }
    }
}

#[test]
fn shift_jis_source_mapping_and_probe_hashes_remain_pinned() {
    let mut manifest = format!("{}\n", DOUBLE_BYTE_MANIFEST.lines().next().unwrap());
    for line in DOUBLE_BYTE_MANIFEST.lines().filter(|line| line.starts_with("Shift_JIS\t") || line.starts_with("windows-31j\t")) {
        manifest.push_str(line);
        manifest.push('\n');
    }
    assert_eq!(sha256_hex(manifest.as_bytes()), "7a178b00038c0baa4de1bd3a099639a39f3b24a733527bd2fa9aedfc8a09595e");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/shift_jis.bin")), "315c3ae24f7fc41e8928dddec73ad8312a7f13e82216e16167aa936cb1a85d13");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/windows-31j.bin")), "f4a3961137be202c450776887c715e01cf44ca031d99493478991cca71e61f67");
}

#[tokio::test]
async fn double_byte_canonical_names_and_every_provider_alias_decode_single_bytes_and_pairs() {
    let mut names = std::collections::BTreeSet::new();
    for line in DOUBLE_BYTE_MANIFEST.lines().skip(1) {
        let fields: Vec<_> = line.split('\t').collect();
        for name in std::iter::once(fields[0]).chain(fields[12].split('|')) {
            names.insert(name.to_ascii_lowercase());
            for selected in [name.to_owned(), name.to_ascii_uppercase()] {
                let mut singles = String::new();
                for byte in 0u8..=255 { singles.push_str(&decode(&selected, &[byte]).await); }
                assert_eq!(sha256_hex(singles.as_bytes()), fields[10], "{selected}");
                let mut pairs = String::new();
                for first in 0u8..=255 {
                    let bytes: Vec<_> = (0u8..=255).flat_map(|second| [first, second, 0]).collect();
                    pairs.push_str(&decode(&selected, &bytes).await);
                }
                assert_eq!(sha256_hex(pairs.as_bytes()), fields[11], "{selected}");
                assert_eq!(decode(&selected, &[]).await, "");
            }
        }
    }
    assert_eq!(names.len(), 100);
}

#[tokio::test]
async fn shift_jis_decodes_japanese_ascii_controls_and_halfwidth_katakana() {
    for name in ["Shift_JIS", "windows-31j"] {
        assert_eq!(decode(name, &[0x93, 0xfa, 0x96, 0x7b, 0x8c, 0xea, 0x82, 0xa0, 0x82, 0xa2, 0x83, 0x41]).await, "日本語あいア");
        assert_eq!(decode(name, &[0, b'\t', b'\n', 0x5c, 0x7e, 0x7f, 0xa1, 0xdf]).await, "\0\t\n\\~\u{7f}｡ﾟ");
        assert_eq!(decode(name, &[0x81, 0x40, 0x81, 0x41, 0x81, 0x42, 0x88, 0x9f]).await, "\u{3000}、。亜");
    }
}

#[tokio::test]
async fn windows31j_retains_distinct_symbols_extensions_and_private_use_characters() {
    let symbols = [0x81, 0x5c, 0x81, 0x60, 0x81, 0x61, 0x81, 0x7c, 0x81, 0x91, 0x81, 0x92, 0x81, 0xca];
    assert_eq!(decode("Shift_JIS", &symbols).await, "—〜‖−¢£¬");
    assert_eq!(decode("windows-31j", &symbols).await, "―～∥－￠￡￢");
    assert_eq!(decode("windows-31j", &[0x87, 0x40, 0xed, 0x40, 0xfa, 0x40, 0xf0, 0x40, 0xf9, 0xfc]).await, "①纊ⅰ\u{e000}\u{e757}");
    for first in [0x87, 0xed, 0xfa, 0xf0, 0xf9] {
        assert_eq!(decode("Shift_JIS", &[first, 0x40]).await, "\u{fffd}@");
    }
}

#[tokio::test]
async fn shift_jis_replacement_replays_single_bytes_and_present_lead_rows() {
    for name in ["Shift_JIS", "windows-31j"] {
        for (bytes, expected) in [
            (&[0x81][..], "\u{fffd}"),
            (&[0x81, b' '][..], "\u{fffd} "),
            (&[0x81, 0x7f][..], "\u{fffd}\u{7f}"),
            (&[0x83, 0xdf][..], "\u{fffd}ﾟ"),
            (&[0x85, 0x81, 0x40][..], "\u{fffd}\u{3000}"),
            (&[0x88, 0x81, 0x40][..], "\u{fffd}\u{3000}"),
            (&[0x81, 0xfd, b'a'][..], "\u{fffd}a"),
            (&[0xfd, 0xfd, b'a'][..], "\u{fffd}\u{fffd}a"),
            (&[0x81, 0xff, 0x81][..], "\u{fffd}\u{fffd}"),
            (&[0x81, 0xfd, 0x82, 0xa0][..], "\u{fffd}あ"),
        ] { assert_eq!(decode(name, bytes).await, expected, "{name} {bytes:?}"); }
    }
    assert_eq!(decode("Shift_JIS", &[0x88, 0x87, 0x40]).await, "\u{fffd}@");
    assert_eq!(decode("windows-31j", &[0x88, 0x87, 0x40]).await, "\u{fffd}①");
    assert_eq!(decode("Shift_JIS", &[0x88, 0x87]).await, "\u{fffd}");
    assert_eq!(decode("windows-31j", &[0x88, 0x87]).await, "\u{fffd}\u{fffd}");
}

#[tokio::test]
async fn shift_jis_decoding_resets_partial_pairs_for_every_record() {
    for name in ["Shift_JIS", "windows-31j"] {
        let mut converter = PluginRegistry::default().converter(&spec(&[("converter.encoding", name)]), false).unwrap();
        let mut context = ConversionContext::new(MemoryBudget::new(1 << 20));
        for (bytes, expected) in [(&[0x81][..], "\u{fffd}"), (&[0x40][..], "@"), (&[0x81, 0x40][..], "\u{3000}"), (&[][..], "")] {
            assert_eq!(converter.decode("events", Some(bytes), &mut context).await.unwrap().value, Value::Utf8(expected.into()));
        }
        assert_eq!(converter.decode("events", None, &mut context).await.unwrap().value, Value::Null);
    }
}

#[tokio::test]
async fn shift_jis_encodings_follow_key_and_value_overrides_and_serializer_validation() {
    let options = spec(&[
        ("converter.encoding", "invalid"),
        ("key.serializer.encoding", "MS932"),
        ("key.deserializer.encoding", "SJIS"),
        ("value.serializer.encoding", "Shift_JIS"),
        ("value.deserializer.encoding", "windows-932"),
    ]);
    let plugins = PluginRegistry::default();
    for (is_key, expected) in [(true, "〜"), (false, "～")] {
        let mut converter = plugins.converter(&options, is_key).unwrap();
        let mut context = ConversionContext::new(MemoryBudget::new(1 << 20));
        assert_eq!(converter.decode("events", Some(&[0x81, 0x60]), &mut context).await.unwrap().value, Value::Utf8(expected.into()));
        let serializer = if is_key { "key.serializer.encoding" } else { "value.serializer.encoding" };
        for invalid in ["ShiftJIS", "windows_31j", "ms-932", " SJIS "] {
            let mut invalid_options = options.clone();
            invalid_options.options.insert(serializer.into(), invalid.into());
            assert!(plugins.converter(&invalid_options, is_key).is_err(), "{serializer}={invalid}");
        }
    }
    assert_eq!(decode("\0\tSJIS\r\u{1f}", &[0x81, 0x60]).await, "〜");
}

#[test]
fn shift_jis_lookup_rejects_unregistered_spellings() {
    for name in ["ShiftJIS", "windows_31j", "ms-932", "Shift_JIS-2004", "csWindows932"] {
        assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", name)]), false).is_err(), "{name}");
    }
}

#[tokio::test]
async fn shift_jis_decoders_admit_replacement_and_mapping_storage_before_decoding() {
    for name in ["Shift_JIS", "windows-31j"] {
        for (byte, expected) in [(0xff, "\u{fffd}"), (0xa1, "｡")] {
            let mut converter = PluginRegistry::default().converter(&spec(&[("converter.encoding", name)]), false).unwrap();
            let bytes = [byte; 1024];
            let mut rejected = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4 - 1));
            assert!(converter.decode("events", Some(&bytes), &mut rejected).await.is_err());
            let mut admitted = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4));
            assert_eq!(converter.decode("events", Some(&bytes), &mut admitted).await.unwrap().value, Value::Utf8(expected.repeat(bytes.len())));
        }
    }
}

#[tokio::test]
async fn double_byte_families_preserve_chinese_korean_and_vendor_mappings() {
    for (encoding, bytes, expected) in [
        ("Big5", &[0xa4, 0x40, 0xa4, 0xa4, 0xa4, 0xe5][..], "一中文"),
        ("GB2312", &[0xd6, 0xd0, 0xce, 0xc4, 0xa1, 0xa4, 0xa1, 0xaa][..], "中文・―"),
        ("GBK", &[0xd6, 0xd0, 0xce, 0xc4, 0xa1, 0xa4, 0xa1, 0xaa, 0x81, 0x40][..], "中文·—丂"),
        ("EUC-KR", &[0xb0, 0xa1, 0xc7, 0xd1, 0xb1, 0xb9][..], "가한국"),
        ("x-Johab", &[0x88, 0x61, 0xd0, 0x65, 0x8a, 0x82][..], "가한국"),
        ("x-IBM1381", &[0x80, 0xb0, 0xa1, 0xd6, 0xd0, 0x8c, 0xe0][..], "£啊中ⅰ"),
        ("x-IBM942", &[0x80, 0xff, 0x81, 0x5c, 0xfa, 0x40, 0xf0, 0x40][..], "¢~—ⅰ\u{e000}"),
        ("x-IBM942C", &[0x80, 0xff, 0x81, 0x5c, 0xfa, 0x40, 0xf0, 0x40][..], "¢~—ⅰ\u{e000}"),
        ("x-IBM943", &[0x81, 0x5c, 0xed, 0x40, 0xfa, 0x40, 0xf0, 0x40][..], "—纊ⅰ\u{e000}"),
        ("x-IBM943C", &[0x81, 0x5c, 0xed, 0x40, 0xfa, 0x40, 0xf0, 0x40][..], "—纊ⅰ\u{e000}"),
        ("x-IBM948", &[0x80, 0x8c, 0x40, 0xa4, 0x40, 0xfa, 0x40][..], "¢一氈\u{f6c4}"),
        ("x-IBM949", &[0x80, 0xb0, 0xa1, 0xc7, 0xd1, 0xb1, 0xb9, 0x8f, 0xa1][..], "¢가한국\u{e0bc}"),
        ("x-IBM949C", &[0x80, 0xb0, 0xa1, 0xc7, 0xd1, 0xb1, 0xb9, 0x8f, 0xa1][..], "¢가한국\u{e0bc}"),
        ("x-IBM950", &[0xa4, 0x40, 0xa4, 0xa4, 0xa4, 0xe5, 0xf9, 0xd6, 0xfa, 0x40][..], "一中文\u{f813}\u{e000}"),
        ("x-PCK", &[0x81, 0x5c, 0x81, 0x60, 0xed, 0x40, 0xfa, 0x40][..], "―〜纊ⅰ"),
        ("x-mswin-936", &[0x80, 0xd6, 0xd0, 0xce, 0xc4, 0x81, 0x40][..], "€中文丂"),
        ("x-windows-949", &[0xb0, 0xa1, 0xc7, 0xd1, 0xb1, 0xb9, 0x81, 0x41][..], "가한국갂"),
        ("x-windows-950", &[0xa4, 0x40, 0xa4, 0xa4, 0xa4, 0xe5, 0xa3, 0xe1, 0xf9, 0xd6][..], "一中文€碁"),
    ] { assert_eq!(decode(encoding, bytes).await, expected, "{encoding}"); }
}

#[tokio::test]
async fn ibm_double_byte_decoders_preserve_ascii_range_remappings_before_multibyte_suffixes() {
    let prefix = [0, 0x1a, 0x1c, 0x5c, 0x7e, 0x7f];
    for (name, expected) in [
        ("x-IBM942", "\0\u{1c}\u{7f}¥‾\u{1a}"),
        ("x-IBM943", "\0\u{1c}\u{7f}¥‾\u{1a}"),
        ("x-IBM942C", "\0\u{1a}\u{1c}\\~\u{7f}"),
        ("x-IBM943C", "\0\u{1a}\u{1c}\\~\u{7f}"),
    ] {
        assert_eq!(decode(name, &prefix).await, expected, "{name}");
        let mut bytes = prefix.to_vec();
        bytes.extend_from_slice(&[0x82, 0xa0]);
        assert_eq!(decode(name, &bytes).await, format!("{expected}あ"), "{name}");
    }
    for (name, expected) in [("x-IBM949", "\0\u{1a}\u{1c}₩~\u{7f}"), ("x-IBM949C", "\0\u{1a}\u{1c}\\~\u{7f}")] {
        assert_eq!(decode(name, &prefix).await, expected, "{name}");
        let mut bytes = prefix.to_vec();
        bytes.extend_from_slice(&[0xb0, 0xa1]);
        assert_eq!(decode(name, &bytes).await, format!("{expected}가"), "{name}");
    }
}

#[tokio::test]
async fn double_byte_aliases_keep_ibm_and_windows_code_pages_distinct() {
    for name in ["cp932", "ibm932", "ibm-932", "932", "x-ibm932", "cp942C"] {
        assert_eq!(decode(name, &[0x5c, 0x80, 0x81, 0x5c]).await, "\\¢—", "{name}");
    }
    assert_eq!(decode("windows-932", &[0x5c, 0x80, 0x81, 0x5c]).await, "\\\u{fffd}―");
    assert_eq!(decode("cp936", &[0x80]).await, "\u{fffd}");
    assert_eq!(decode("ms936", &[0x80]).await, "€");
    assert_eq!(decode("cp949", &[0x5c, 0x81, 0x41]).await, "₩\u{fffd}A");
    assert_eq!(decode("windows-949", &[0x5c, 0x81, 0x41]).await, "\\갂");
    assert_eq!(decode("cp950", &[0xf9, 0xd6]).await, "\u{f813}");
    assert_eq!(decode("windows-950", &[0xf9, 0xd6]).await, "碁");
}

#[tokio::test]
async fn double_byte_family_errors_follow_registered_lead_rows_and_trail_ranges() {
    for (name, bytes, expected) in [
        ("Big5", &[0xa1, 0x7f][..], "\u{fffd}\u{7f}"),
        ("Big5", &[0xa1, 0x80][..], "\u{fffd}"),
        ("Big5", &[0xf9, 0xd6][..], "\u{fffd}\u{fffd}"),
        ("EUC-KR", &[0xa1, b'A'][..], "\u{fffd}A"),
        ("EUC-KR", &[0xa1, 0xa0][..], "\u{fffd}"),
        ("EUC-KR", &[0x80, 0xa1, 0xa1][..], "\u{fffd}\u{3000}"),
        ("GB2312", &[0x81, 0x40][..], "\u{fffd}@"),
        ("GB2312", &[0x80, 0x80][..], "\u{fffd}\u{fffd}"),
        ("GBK", &[0x81, 0x7f][..], "\u{fffd}\u{7f}"),
        ("GBK", &[0x81, 0xff][..], "\u{fffd}"),
        ("x-Johab", &[0x84, 0x31][..], "\u{fffd}1"),
        ("x-Johab", &[0x84, 0xff][..], "\u{fffd}"),
        ("x-IBM942", &[0x81, 0xff][..], "\u{fffd}~"),
        ("x-IBM943", &[0x81, 0xff][..], "\u{fffd}"),
        ("x-PCK", &[0xf0, 0x40][..], "\u{fffd}@"),
    ] { assert_eq!(decode(name, bytes).await, expected, "{name} {bytes:?}"); }
}

#[tokio::test]
async fn double_byte_decoders_preserve_zero_separators_and_null_records() {
    for line in DOUBLE_BYTE_MANIFEST.lines().skip(1) {
        let name = line.split('\t').next().unwrap();
        let mut converter = PluginRegistry::default().converter(&spec(&[("converter.encoding", name)]), false).unwrap();
        let mut context = ConversionContext::new(MemoryBudget::new(1 << 20));
        assert_eq!(converter.decode("events", Some(&[0]), &mut context).await.unwrap().value, Value::Utf8("\0".into()), "{name}");
        assert_eq!(converter.decode("events", None, &mut context).await.unwrap().value, Value::Null, "{name}");
        for byte in 0u8..=255 {
            let standalone = decode(name, &[byte]).await;
            assert_eq!(decode(name, &[byte, 0]).await, format!("{standalone}\0"), "{name} {byte:02x}");
        }
    }
}

#[tokio::test]
async fn double_byte_legacy_symbols_obey_scalar_memory_admission() {
    for (name, byte, expected) in [("Big5", 0xff, "\u{fffd}"), ("x-IBM942", 0x7e, "‾"), ("x-IBM949", 0x5c, "₩"), ("x-mswin-936", 0x80, "€")] {
        let mut converter = PluginRegistry::default().converter(&spec(&[("converter.encoding", name)]), false).unwrap();
        let bytes = [byte; 1024];
        let mut rejected = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4 - 1));
        assert!(converter.decode("events", Some(&bytes), &mut rejected).await.is_err());
        let mut admitted = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4));
        assert_eq!(converter.decode("events", Some(&bytes), &mut admitted).await.unwrap().value, Value::Utf8(expected.repeat(bytes.len())));
    }
}
