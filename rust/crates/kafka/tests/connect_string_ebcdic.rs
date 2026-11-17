use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConversionContext, Converter, PluginRegistry, PluginSpec, SchemaKind};
use rtdb_types::Value;

const MANIFEST: &str = include_str!("../src/connect/converter/charset/double_byte/ebcdic/charsets.tsv");

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

#[test]
fn ebcdic_resources_preserve_source_rows_provider_provenance_and_ibm933_mapping_bytes() {
    assert_eq!(sha256_hex(MANIFEST.as_bytes()), "59eb8f1cee32d26f7a34746f8a12fbae5d14d8d1a00eab0082d43845e17f8965");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/ebcdic/aliases.rs")), "cb1c22116143237080c9fa27a06a979c2c793aba55952ee3d880988b1388f216");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/ebcdic/tables.rs")), "fbba347b7cb613e8e9294c3b330d5081937dfa31e210bbe45b9d4afd528f0153");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/ebcdic/sources.sha256")), "7e4ddd5ae3690e645ba3fb2827bea0723e3afef51f9eab350a736ac9b347ce54");
    let mappings: [&[u8]; 6] = [
        include_bytes!("../src/connect/converter/charset/double_byte/ebcdic/x-ibm1364.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/ebcdic/x-ibm930.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/ebcdic/x-ibm933.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/ebcdic/x-ibm935.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/ebcdic/x-ibm937.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/ebcdic/x-ibm939.bin"),
    ];
    assert_eq!(MANIFEST.lines().count(), 7);
    assert_eq!(mappings[2], include_bytes!("../src/connect/converter/charset/double_byte/subclasses/x-ibm834.bin"));
    for (line, mapping) in MANIFEST.lines().skip(1).zip(mappings) {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 15);
        assert_eq!(&fields[4..7], &["false", "40", "fe"]);
        assert_eq!(&mapping[..2], &[0x40, 0xfe]);
        assert_eq!(mapping.len(), fields[8].parse::<usize>().unwrap());
        assert_eq!(sha256_hex(mapping), fields[9]);
        let rows: usize = fields[7].parse().unwrap();
        assert_eq!(mapping.len(), 1026 + rows * 191 * 2);
        let unit = |offset: usize| u16::from_be_bytes([mapping[offset], mapping[offset + 1]]);
        let mut present = 0;
        for byte in 0u8..=255 {
            let row = unit(514 + usize::from(byte) * 2);
            if row != 0xffff {
                assert_eq!(usize::from(row), present);
                present += 1;
            }
        }
        assert_eq!(present, rows);
        for offset in (2..514).step_by(2).chain((1026..mapping.len()).step_by(2)) {
            assert!(!(0xd800..=0xdfff).contains(&unit(offset)));
        }
        assert!((0u8..128).any(|byte| unit(2 + usize::from(byte) * 2) != u16::from(byte)));
        assert_eq!(unit(2 + 0x0e * 2), 0xfffd);
        assert_eq!(unit(2 + 0x0f * 2), 0xfffd);
    }
}

#[tokio::test]
async fn every_ebcdic_name_decodes_all_single_bytes_pairs_and_control_prefixed_triples() {
    let mut names = std::collections::BTreeSet::new();
    for line in MANIFEST.lines().skip(1) {
        let fields: Vec<_> = line.split('\t').collect();
        for name in std::iter::once(fields[0]).chain(fields[14].split('|')) {
            names.insert(name.to_ascii_lowercase());
            for selected in [name.to_owned(), name.to_ascii_uppercase()] {
                let mut converter = open(&selected);
                let mut singles = String::new();
                for byte in 0u8..=255 { singles.push_str(&decode(converter.as_mut(), &[byte]).await); }
                assert_eq!(sha256_hex(singles.as_bytes()), fields[10], "{selected}");
                for (prefix, expected) in [(None, fields[11]), (Some(0x0e), fields[12]), (Some(0x0f), fields[13])] {
                    let mut probe = String::new();
                    for first in 0u8..=255 {
                        for second in 0u8..=255 {
                            let bytes = [prefix.unwrap_or(0), first, second];
                            let record = if prefix.is_some() { &bytes[..] } else { &bytes[1..] };
                            probe.push_str(&decode(converter.as_mut(), record).await);
                            probe.push('\0');
                        }
                    }
                    assert_eq!(sha256_hex(probe.as_bytes()), expected, "{selected} {prefix:?}");
                }
                assert_eq!(decode(converter.as_mut(), &[]).await, "");
                assert_eq!(converted(converter.as_mut(), None).await, Value::Null);
            }
        }
    }
    assert_eq!(names.len(), 30);
}

#[tokio::test]
async fn ebcdic_variants_preserve_mixed_text_controls_symbols_and_private_use_entries() {
    for (name, pairs, expected, private_pair, private_scalar) in [
        ("x-IBM1364", [0xd0, 0x65, 0x8b, 0x69], "한글", [0x50, 0x5e], "\u{f843}"),
        ("x-IBM930", [0x44, 0x81, 0x44, 0x82], "あい", [0x69, 0x41], "\u{e000}"),
        ("x-IBM933", [0xd0, 0x65, 0x8b, 0x69], "한글", [0x50, 0x5e], "\u{f843}"),
        ("x-IBM935", [0x5b, 0xcf, 0x57, 0xc3], "中文", [0x43, 0x5b], "\u{f83d}"),
        ("x-IBM937", [0x4c, 0x84, 0x48, 0x83], "中文", [0x44, 0x5c], "\u{f83e}"),
        ("x-IBM939", [0x44, 0x81, 0x44, 0x82], "あい", [0x69, 0x41], "\u{e000}"),
    ] {
        let mut converter = open(name);
        let bytes = [0xc1, 0x40, 0x0e, pairs[0], pairs[1], pairs[2], pairs[3], 0x0f, 0xf0];
        assert_eq!(decode(converter.as_mut(), &bytes).await, format!("A {expected}0"), "{name}");
        assert_eq!(decode(converter.as_mut(), &[0x0e, private_pair[0], private_pair[1]]).await, private_scalar, "{name}");
        assert_eq!(decode(converter.as_mut(), &[0, 0x04, 0x05, 0x25, 0xff]).await, "\0\u{9c}\t\n\u{9f}", "{name}");
        assert_eq!(decode(converter.as_mut(), b"@").await, " ", "{name}");
    }
    for (name, expected) in [
        ("x-IBM1364", "\u{85}\u{fffd}\u{ffa0}$a~₩"),
        ("x-IBM930", "\n｡｢¥ｱ‾$"),
        ("x-IBM933", "\u{85}\u{fffd}\u{ffa0}$a~₩"),
        ("x-IBM935", "\n\u{fffd}\u{fffd}¥a‾$"),
        ("x-IBM937", "\n\u{fffd}\u{fffd}$a~\\"),
        ("x-IBM939", "\n\u{fffd}｡$a~\\"),
    ] {
        let mut converter = open(name);
        assert_eq!(decode(converter.as_mut(), &[0x15, 0x41, 0x42, 0x5b, 0x81, 0xa1, 0xe0]).await, expected, "{name}");
    }
    let mut extended = open("IBM1364");
    let mut original = open("IBM933");
    assert_eq!(decode(extended.as_mut(), &[0x0e, 0x49, 0x95, 0x49, 0x96, 0x4c, 0x41]).await, "€®\u{1100}");
    assert_eq!(decode(original.as_mut(), &[0x0e, 0x49, 0x95, 0x49, 0x96, 0x4c, 0x41]).await, "\u{fffd}\u{fffd}\u{fffd}");
}

#[tokio::test]
async fn ebcdic_shift_controls_replace_invalid_transitions_without_changing_state() {
    for line in MANIFEST.lines().skip(1) {
        let name = line.split('\t').next().unwrap();
        let mut converter = open(name);
        for (bytes, expected) in [
            (&[0x0e][..], ""),
            (&[0x0f][..], "\u{fffd}"),
            (&[0x0e, 0x0f][..], ""),
            (&[0x0e, 0x0e][..], "\u{fffd}"),
            (&[0x0f, 0x0f][..], "\u{fffd}\u{fffd}"),
            (&[0x0f, 0x0e][..], "\u{fffd}"),
            (&[0x0e, 0x0f, 0x0f, 0xc1][..], "\u{fffd}A"),
            (&[0x0e, 0x0e, 0x40, 0x40, 0x0f, 0xc1][..], "\u{fffd}\u{3000}A"),
            (&[0x0f, 0xc1, 0x0e, 0x40, 0x40, 0x0f][..], "\u{fffd}A\u{3000}"),
            (&[0xc1, 0x0e][..], "A"),
            (&[0x0e, 0x40][..], "\u{fffd}"),
        ] {
            assert_eq!(decode(converter.as_mut(), bytes).await, expected, "{name} {bytes:x?}");
        }
    }
}

#[tokio::test]
async fn ebcdic_pair_errors_consume_shift_control_trails_and_preserve_double_byte_mode() {
    for line in MANIFEST.lines().skip(1) {
        let name = line.split('\t').next().unwrap();
        let mut converter = open(name);
        for pair in [[0x40, 0x0e], [0x40, 0x0f], [0x01, 0xc1], [0, 0], [0xff, 0xff], [0x40, 0x3f]] {
            let bytes = [0x0e, pair[0], pair[1], 0x40, 0x40, 0x0f, 0xc1];
            assert_eq!(decode(converter.as_mut(), &bytes).await, "\u{fffd}\u{3000}A", "{name} {bytes:x?}");
        }
        assert_eq!(decode(converter.as_mut(), &[0x0e, 0x40, 0x0f, 0xc1]).await, "\u{fffd}\u{fffd}", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0x0e, 0x40, 0x40, 0]).await, "\u{3000}\u{fffd}", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0x0e, 0, 0x40, 0x40]).await, "\u{fffd}\u{fffd}", "{name}");
        let mut separated = decode(converter.as_mut(), &[0x0e, 0x40]).await;
        separated.push_str(&decode(converter.as_mut(), &[0]).await);
        assert_eq!(separated, "\u{fffd}\0", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0x0e, 0x40, 0]).await, "\u{fffd}", "{name}");
    }
}

#[tokio::test]
async fn ebcdic_records_reset_shift_state_and_keep_ibm834_raw_dbcs_behavior_separate() {
    for line in MANIFEST.lines().skip(1) {
        let name = line.split('\t').next().unwrap();
        let mut converter = open(name);
        assert_eq!(decode(converter.as_mut(), &[0x0e, 0x40, 0x40]).await, "\u{3000}", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0x40, 0x40]).await, "  ", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0x0e]).await, "", "{name}");
        assert_eq!(converted(converter.as_mut(), None).await, Value::Null, "{name}");
        assert_eq!(decode(converter.as_mut(), &[]).await, "", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0xc1]).await, "A", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0x0e, 0x40]).await, "\u{fffd}", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0xc1]).await, "A", "{name}");
    }
    let mut ibm834 = open("cp834");
    let mut ibm933 = open("cp933");
    assert_eq!(decode(ibm834.as_mut(), &[0x40, 0x40]).await, "\u{3000}");
    assert_eq!(decode(ibm933.as_mut(), &[0x40, 0x40]).await, "  ");
    assert_eq!(decode(ibm933.as_mut(), &[0x0e, 0x40, 0x40, 0x0f]).await, "\u{3000}");
    assert_eq!(decode(ibm834.as_mut(), &[0x0e, 0x40, 0x40, 0x0f]).await, "\u{fffd}\u{fffd}");
}

#[tokio::test]
async fn ebcdic_configuration_keeps_alias_punctuation_trimming_and_key_value_precedence() {
    let mut trimmed = open("\0\tCP1364\r\u{1f}");
    assert_eq!(decode(trimmed.as_mut(), &[0x0e, 0x49, 0x95]).await, "€");
    for invalid in ["IBM_933", "xIBM933", "CP-930", " IBM937 "] {
        assert!(PluginRegistry::default().converter(&spec(&[("value.deserializer.encoding", invalid)]), false).is_err(), "{invalid}");
    }
    let options = spec(&[
        ("converter.encoding", "ibm935"),
        ("key.serializer.encoding", "ibm-937"),
        ("key.deserializer.encoding", "cp1364"),
        ("value.serializer.encoding", "939"),
        ("value.deserializer.encoding", "ibm933"),
    ]);
    let mut key = PluginRegistry::default().converter(&options, true).unwrap();
    let mut value = PluginRegistry::default().converter(&options, false).unwrap();
    assert_eq!(decode(key.as_mut(), &[0x0e, 0x49, 0x95]).await, "€");
    assert_eq!(decode(value.as_mut(), &[0x0e, 0x49, 0x95]).await, "\u{fffd}");
    for (is_key, serializer) in [(true, "key.serializer.encoding"), (false, "value.serializer.encoding")] {
        assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", "cp933"), (serializer, "IBM_933")]), is_key).is_err());
    }
}

#[tokio::test]
async fn ebcdic_mappings_replacements_and_empty_shift_output_obey_scalar_memory_admission() {
    for line in MANIFEST.lines().skip(1) {
        let name = line.split('\t').next().unwrap();
        let mut converter = open(name);
        let mut shifted = vec![0x0e];
        shifted.extend([0x40, 0x40].repeat(1024));
        shifted.push(0x0f);
        for (bytes, expected) in [(shifted, "\u{3000}".repeat(1024)), (vec![0x0f; 1024], "\u{fffd}".repeat(1024)), (vec![0x0e], String::new())] {
            let mut rejected = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4 - 1));
            assert!(converter.decode("events", Some(&bytes), &mut rejected).await.is_err());
            let mut admitted = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4));
            assert_eq!(converter.decode("events", Some(&bytes), &mut admitted).await.unwrap().value, Value::Utf8(expected), "{name}");
        }
    }
}
