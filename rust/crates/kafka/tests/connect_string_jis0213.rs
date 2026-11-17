use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConversionContext, Converter, PluginRegistry, PluginSpec, SchemaKind};
use rtdb_types::Value;

const MANIFEST: &str = include_str!("../src/connect/converter/charset/double_byte/jis0213/charsets.tsv");
const PROBES: &str = include_str!("../src/connect/converter/charset/double_byte/jis0213/probes.tsv");
const MAPPING: &[u8] = include_bytes!("../src/connect/converter/charset/double_byte/jis0213/sjis0213.dat");

fn spec(options: &[(&str, &str)]) -> PluginSpec {
    PluginSpec {
        class_name: "org.apache.kafka.connect.storage.StringConverter".into(),
        options: options.iter().map(|(key, value)| ((*key).into(), (*value).into())).collect(),
    }
}

fn open(encoding: &str) -> Box<dyn Converter> {
    PluginRegistry::default().converter(&spec(&[("converter.encoding", encoding)]), false).unwrap()
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

fn sha256_hex(bytes: &[u8]) -> String {
    openssl::sha::sha256(bytes).iter().map(|byte| format!("{byte:02x}")).collect()
}

fn unit(offset: usize) -> u16 { u16::from_be_bytes([MAPPING[offset], MAPPING[offset + 1]]) }

#[test]
fn jis0213_resources_pin_the_module_image_mapping_and_windows_source_tables() {
    assert_eq!(sha256_hex(MANIFEST.as_bytes()), "7d88be1af117460ecf815b7bc783178385ea7594557b81d1ba81836706d78d79");
    assert_eq!(sha256_hex(PROBES.as_bytes()), "e4aa3eb25f4562b64073f6a502b72da0d558a8a89819f22d8cf3af558cabda5e");
    assert_eq!(sha256_hex(MAPPING), "9a103ec5e953cb5f8dd205c928abde3b333086a1e784825c940a6acc2e1c0020");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/jis0213/sources.sha256")), "a76b288dc3fa8ad50c32a71a201d3f2e68eeee02ee1aa24fa4c233970f56757a");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/jis0213/aliases.rs")), "9e645158676ee82e0d80c6abefc750a4fcf54efe20e4f8e0ebddf2379407ade3");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/jis0213/tables.rs")), "0f4661048344dc5a9b58b83d69e0847351f7150cdfba4cbf16b15d7c2d9f6d7f");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/jis0213/mappings.tsv")), "db3b265d1ccb62bac4a1f4a2e5acf3980faf7d9430784ba3ea30cf97b854adf0");
    let windows = include_bytes!("../src/connect/converter/charset/double_byte/jis0213/windows-31j.bin");
    assert_eq!(windows, include_bytes!("../src/connect/converter/charset/double_byte/windows-31j.bin"));
    assert_eq!(windows.len(), 21_816);
    assert_eq!(MAPPING.len(), 26_566);
    assert_eq!(u32::from_be_bytes(MAPPING[..4].try_into().unwrap()) as usize, MAPPING.len() - 4);
    let mut position = 4;
    for (tag, offset, count) in [(8, 4, 256), (1, 520, 256), (2, 1036, 5921), (3, 12890, 5539), (5, 23980, 606), (6, 25196, 606), (7, 26412, 75)] {
        assert_eq!(position, offset);
        assert_eq!(unit(position), tag);
        position += 2;
        if tag == 2 || tag == 3 {
            assert_eq!(unit(position), if tag == 2 { 0x81 } else { 0xe0 });
            assert_eq!(unit(position + 2), if tag == 2 { 0x9f } else { 0xfc });
            assert_eq!(unit(position + 4), 0x40);
            assert_eq!(unit(position + 6), 0xfe);
            position += 8;
        }
        assert_eq!(usize::from(unit(position)), count);
        position += 2;
        if matches!(tag, 1..=3) {
            for index in 0..count { assert!(!(0xd800..=0xdfff).contains(&unit(position + index * 2))); }
        }
        if tag == 5 || tag == 6 {
            for index in 1..count / 2 { assert!(unit(position + (index - 1) * 2) < unit(position + index * 2)); }
        }
        if tag == 7 {
            for index in 0..count / 3 {
                if index > 0 { assert!(unit(position + (index - 1) * 6) < unit(position + index * 6)); }
                assert!(!(0xd800..=0xdfff).contains(&unit(position + index * 6 + 2)));
                assert!(!(0xd800..=0xdfff).contains(&unit(position + index * 6 + 4)));
            }
        }
        position += count * 2;
    }
    assert_eq!(position, MAPPING.len());
}

#[tokio::test]
async fn every_jis0213_provider_name_selects_its_decoder_case_insensitively() {
    let mut names = std::collections::BTreeSet::new();
    assert_eq!(MANIFEST.lines().count(), 3);
    assert_eq!(PROBES.lines().count(), 3);
    for (line, probe) in MANIFEST.lines().skip(1).zip(PROBES.lines().skip(1)) {
        let fields: Vec<_> = line.split('\t').collect();
        let expected: Vec<_> = probe.split('\t').collect();
        assert_eq!(fields.len(), 6);
        assert_eq!(expected.len(), 4);
        assert_eq!(fields[4], expected[0]);
        for name in std::iter::once(fields[0]).chain(fields[5].split('|')) {
            names.insert(name.to_ascii_lowercase());
            for selected in [name.to_owned(), name.to_ascii_uppercase()] {
                let mut converter = open(&selected);
                let mut singles = String::new();
                for byte in 0u8..=255 { singles.push_str(&decode(converter.as_mut(), &[byte]).await); }
                assert_eq!(sha256_hex(singles.as_bytes()), expected[1], "{selected}");
                assert_eq!(decode(converter.as_mut(), b"\x81\x60").await, if fields[4] == "Windows" { "～" } else { "〜" }, "{selected}");
                assert_eq!(decode(converter.as_mut(), b"\x82\xf5\x87\xa0").await, "か\u{309a}\u{2000b}", "{selected}");
            }
        }
    }
    assert_eq!(names.len(), 13);
}

#[tokio::test]
async fn jis0213_decoders_cover_every_pair_and_pair_followed_by_valid_recovery_input() {
    for (line, probe) in MANIFEST.lines().skip(1).zip(PROBES.lines().skip(1)) {
        let fields: Vec<_> = line.split('\t').collect();
        let expected: Vec<_> = probe.split('\t').collect();
        let mut converter = open(fields[0]);
        for (suffix, hash) in [(&b""[..], expected[2]), (&b"A\x82\xf5\x87\xa0"[..], expected[3])] {
            let mut record = vec![0, 0];
            record.extend_from_slice(suffix);
            let mut output = String::new();
            for first in 0u8..=255 {
                record[0] = first;
                for second in 0u8..=255 {
                    record[1] = second;
                    output.push_str(&decode(converter.as_mut(), &record).await);
                    output.push('\0');
                }
            }
            assert_eq!(sha256_hex(output.as_bytes()), hash, "{} {suffix:x?}", fields[0]);
        }
    }
}

#[tokio::test]
async fn jis0213_windows_bmp_precedence_retains_jis_supplementary_and_composite_fallback() {
    for (name, wave, parallel, extension) in [
        ("x-SJIS_0213", "〜", "‖", "\u{20089}"),
        ("x-MS932_0213", "～", "∥", "\u{e000}"),
    ] {
        let mut converter = open(name);
        for (bytes, expected) in [
            (&b"\x81\x60"[..], wave),
            (&b"\x81\x61"[..], parallel),
            (&b"\xf0\x40"[..], extension),
            (&b"\x82\x80"[..], "℧"),
            (&b"\x87\xa0"[..], "\u{2000b}"),
            (&b"\x82\xf5"[..], "か\u{309a}"),
            (&b"\x82\xf5\x87\xa0A"[..], "か\u{309a}\u{2000b}A"),
            (&b"\\~\xa1\xdf\0"[..], "\\~｡ﾟ\0"),
        ] { assert_eq!(decode(converter.as_mut(), bytes).await, expected, "{name} {bytes:x?}"); }
    }
}

#[tokio::test]
async fn jis0213_all_extended_entries_preserve_scalars_and_combining_sequences() {
    let mut standard = open("x-SJIS_0213");
    for index in 0..303 {
        let bytes = unit(23_984 + index * 2).to_be_bytes();
        let expected = char::from_u32(0x20000 + u32::from(unit(23_984 + (303 + index) * 2))).unwrap().to_string();
        assert_eq!(decode(standard.as_mut(), &bytes).await, expected, "{bytes:x?}");
    }
    for index in 0..25 {
        let offset = 26_416 + index * 6;
        let bytes = unit(offset).to_be_bytes();
        let expected: String = [unit(offset + 2), unit(offset + 4)].into_iter().map(|unit| char::from_u32(u32::from(unit)).unwrap()).collect();
        assert_eq!(decode(standard.as_mut(), &bytes).await, expected, "{bytes:x?}");
    }
}

#[tokio::test]
async fn jis0213_failed_pairs_consume_the_second_byte_only_when_it_is_not_a_single() {
    for name in ["x-SJIS_0213", "x-MS932_0213"] {
        let mut converter = open(name);
        for (bytes, expected) in [
            (&b"\x80"[..], "\u{fffd}"),
            (&b"\x80\x80"[..], "\u{fffd}"),
            (&b"\x80A"[..], "\u{fffd}A"),
            (&b"\x80\xa1"[..], "\u{fffd}｡"),
            (&b"\x80\x82A"[..], "\u{fffd}A"),
            (&b"\x80\x82\xa0"[..], "\u{fffd}\u{fffd}"),
            (&b"\x81 "[..], "\u{fffd} "),
            (&b"\x81\x7f"[..], "\u{fffd}\u{7f}"),
            (&b"\x81\xfdA"[..], "\u{fffd}A"),
            (&b"\x81\xfeA"[..], "\u{fffd}A"),
            (&b"\x81\xffA"[..], "\u{fffd}A"),
            (&b"\xff\x82 "[..], "\u{fffd} "),
            (&b"\x82\x80A"[..], "℧A"),
        ] { assert_eq!(decode(converter.as_mut(), bytes).await, expected, "{name} {bytes:x?}"); }
    }
}

#[tokio::test]
async fn jis0213_record_boundaries_finish_partial_pairs_and_keep_null_distinct_from_empty() {
    for name in ["x-SJIS_0213", "x-MS932_0213"] {
        let mut converter = open(name);
        for prefix in [0x81, 0x82, 0x87, 0xe0, 0xfc, 0xff] {
            assert_eq!(decode(converter.as_mut(), &[prefix]).await, "\u{fffd}");
            assert_eq!(converted(converter.as_mut(), None).await, Value::Null);
            assert_eq!(decode(converter.as_mut(), &[]).await, "");
            assert_eq!(decode(converter.as_mut(), b"A\\~").await, "A\\~");
        }
        let mut separated = decode(converter.as_mut(), &[0x82]).await;
        separated.push_str(&decode(converter.as_mut(), &[0xf5]).await);
        assert_eq!(separated, "\u{fffd}\u{fffd}");
        assert_eq!(decode(converter.as_mut(), b"\x82\xf5").await, "か\u{309a}");
    }
}

#[tokio::test]
async fn jis0213_configuration_preserves_provider_spelling_trimming_and_key_value_precedence() {
    let mut trimmed = open("\0\tSJIS:2004\r\u{1f}");
    assert_eq!(decode(trimmed.as_mut(), b"\x81\x60").await, "〜");
    for invalid in ["shift_jis_0213", "windows-932_0213", "x-SJIS-0213", "MS9320213", " x-MS932_0213 "] {
        assert!(PluginRegistry::default().converter(&spec(&[("value.deserializer.encoding", invalid)]), false).is_err(), "{invalid}");
    }
    let options = spec(&[
        ("converter.encoding", "x-SJIS_0213"),
        ("key.serializer.encoding", "MS932:2004"),
        ("key.deserializer.encoding", "shift_jis:2004"),
        ("value.serializer.encoding", "sjis_0213:2004"),
        ("value.deserializer.encoding", "windows-932:2004"),
    ]);
    let mut key = PluginRegistry::default().converter(&options, true).unwrap();
    let mut value = PluginRegistry::default().converter(&options, false).unwrap();
    assert_eq!(decode(key.as_mut(), b"\x81\x60").await, "〜");
    assert_eq!(decode(value.as_mut(), b"\x81\x60").await, "～");
    for (is_key, serializer) in [(true, "key.serializer.encoding"), (false, "value.serializer.encoding")] {
        assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", "x-SJIS_0213"), (serializer, "MS9320213")]), is_key).is_err());
    }
}

#[tokio::test]
async fn jis0213_bmp_supplementary_composite_and_replacement_output_obeys_scalar_memory_admission() {
    for name in ["x-SJIS_0213", "x-MS932_0213"] {
        let mut converter = open(name);
        for (bytes, expected) in [
            (b"\x82\x80".repeat(1024), "℧".repeat(1024)),
            (b"\x87\xa0".repeat(1024), "\u{2000b}".repeat(1024)),
            (b"\x82\xf5".repeat(1024), "か\u{309a}".repeat(1024)),
            (b"\x80\x80".repeat(1024), "\u{fffd}".repeat(1024)),
            (vec![0xa1; 1024], "｡".repeat(1024)),
            (vec![0x82], "\u{fffd}".to_owned()),
            (vec![], String::new()),
        ] {
            let mut rejected = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4 - 1));
            assert!(converter.decode("events", Some(&bytes), &mut rejected).await.is_err());
            let mut admitted = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4));
            assert_eq!(converter.decode("events", Some(&bytes), &mut admitted).await.unwrap().value, Value::Utf8(expected), "{name}");
        }
    }
}
