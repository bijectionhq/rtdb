use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConversionContext, Converter, PluginRegistry, PluginSpec, SchemaKind};
use rtdb_types::Value;

const MANIFEST: &str = include_str!("../src/connect/converter/charset/double_byte/subclasses/charsets.tsv");

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
fn subclass_resources_retain_source_tables_decoder_kinds_and_provider_provenance() {
    assert_eq!(sha256_hex(MANIFEST.as_bytes()), "59848c7998089d83738b2082ab7d4435ffdd0d663c85e4a06aae02f9bcae471a");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/subclasses/aliases.rs")), "2486f0e419eb906cdc22ed61edc9a06a57e0c936534761b1bd4585f52a52c71e");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/subclasses/tables.rs")), "3f1c99ae5a300586322d684e6a348b16f8dd2abeed24de3338c72bbd7e858f29");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/subclasses/sources.sha256")), "7e4ddd5ae3690e645ba3fb2827bea0723e3afef51f9eab350a736ac9b347ce54");
    let mappings: [&[u8]; 6] = [
        include_bytes!("../src/connect/converter/charset/double_byte/subclasses/jis_x0212-1990.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/subclasses/x-ibm1383.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/subclasses/x-ibm300.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/subclasses/x-ibm834.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/subclasses/x-ibm970.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/subclasses/x-jis0208.bin"),
    ];
    assert_eq!(MANIFEST.lines().count(), 7);
    for (line, mapping) in MANIFEST.lines().skip(1).zip(mappings) {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 16);
        assert_eq!(mapping.len(), fields[11].parse::<usize>().unwrap());
        assert_eq!(sha256_hex(mapping), fields[12]);
        assert_eq!(mapping[0], u8::from_str_radix(fields[8], 16).unwrap());
        assert_eq!(mapping[1], u8::from_str_radix(fields[9], 16).unwrap());
        let rows: usize = fields[10].parse().unwrap();
        let width = usize::from(mapping[1] - mapping[0]) + 1;
        assert_eq!(mapping.len(), 1026 + rows * width * 2);
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
        match fields[4] {
            "DbcsOnly" => assert_eq!(fields[7], "false"),
            "EucSimple" => {
                assert_eq!(fields[7], "true");
                for byte in 0u8..=255 {
                    assert_eq!(unit(2 + usize::from(byte) * 2), if byte < 0xa0 && !matches!(byte, 0x8e | 0x8f) { u16::from(byte) } else { 0xfffd });
                }
            }
            other => panic!("unexpected decoder {other}"),
        }
        if fields[0] == "x-IBM834" {
            assert_eq!(fields[1], "IBM834");
            assert_eq!(fields[5], "jdk.charsets/sun/nio/cs/ext/IBM933.java");
            assert_ne!(fields[3], fields[6]);
            assert_eq!(unit(2), 0);
        } else {
            assert_eq!(fields[2], fields[5]);
            assert_eq!(fields[3], fields[6]);
        }
    }
}

#[tokio::test]
async fn every_subclass_name_decodes_all_single_bytes_and_pairs_as_independent_records() {
    let mut names = std::collections::BTreeSet::new();
    for line in MANIFEST.lines().skip(1) {
        let fields: Vec<_> = line.split('\t').collect();
        for name in std::iter::once(fields[0]).chain(fields[15].split('|')) {
            names.insert(name.to_ascii_lowercase());
            for selected in [name.to_owned(), name.to_ascii_uppercase()] {
                let mut converter = open(&selected);
                let mut singles = String::new();
                for byte in 0u8..=255 { singles.push_str(&decode(converter.as_mut(), &[byte]).await); }
                assert_eq!(sha256_hex(singles.as_bytes()), fields[13], "{selected}");
                let mut pairs = String::new();
                for first in 0u8..=255 {
                    for second in 0u8..=255 {
                        pairs.push_str(&decode(converter.as_mut(), &[first, second]).await);
                        pairs.push('\0');
                    }
                }
                assert_eq!(sha256_hex(pairs.as_bytes()), fields[14], "{selected}");
                assert_eq!(decode(converter.as_mut(), &[]).await, "");
                assert_eq!(converted(converter.as_mut(), None).await, Value::Null);
            }
        }
    }
    assert_eq!(names.len(), 36);
}

#[tokio::test]
async fn dbcs_only_decoders_map_raw_jis_and_ebcdic_pairs_without_ascii_or_shift_modes() {
    for (name, pair, text) in [
        ("x-JIS0208", [0x24, 0x22], "あ"),
        ("JIS_X0212-1990", [0x30, 0x21], "丂"),
        ("x-IBM300", [0x44, 0x81], "あ"),
        ("x-IBM834", [0x88, 0x61], "가"),
    ] {
        let mut converter = open(name);
        assert_eq!(decode(converter.as_mut(), &pair).await, text, "{name}");
        for byte in pair { assert_eq!(decode(converter.as_mut(), &[byte]).await, "\u{fffd}", "{name}"); }
        for bytes in [&[0][..], b"A", b"\0A", &[0x0e, 0x0f]] {
            assert_eq!(decode(converter.as_mut(), bytes).await, "\u{fffd}", "{name} {bytes:x?}");
        }
        assert_eq!(decode(converter.as_mut(), &[0, b'A', pair[0], pair[1]]).await, format!("\u{fffd}{text}"), "{name}");
        assert_eq!(decode(converter.as_mut(), &[0x0e, 0x0f, pair[0], pair[1]]).await, format!("\u{fffd}{text}"), "{name}");
        assert_eq!(decode(converter.as_mut(), &[pair[0], pair[1], 0]).await, format!("{text}\u{fffd}"), "{name}");
        assert_eq!(decode(converter.as_mut(), &[0, pair[0], pair[1]]).await, "\u{fffd}\u{fffd}", "{name}");
    }
    let mut jis0208 = open("JIS_X0208-1983");
    assert_eq!(decode(jis0208.as_mut(), b"$\"$$%\"!!!AAB").await, "あいア\u{3000}〜疎");
    let mut jis0212 = open("iso-ir-159");
    assert_eq!(decode(jis0212.as_mut(), &[0x22, 0x2f, 0x22, 0x30, 0x30, 0x21, 0x30, 0x22, 0x41, 0x42]).await, "˘ˇ丂丄攢");
    let mut ibm300 = open("cp300");
    assert_eq!(decode(ibm300.as_mut(), &[0x40, 0x40, 0x41, 0x41, 0x41, 0x42, 0x69, 0x41]).await, "\u{3000}αβ\u{e000}");
}

#[tokio::test]
async fn ibm834_decoding_keeps_ibm933_values_for_non_round_trip_encoder_alternatives() {
    let mut converter = open("ibm-834");
    assert_eq!(decode(converter.as_mut(), &[0x41, 0x43, 0x41, 0x48, 0x41, 0x49, 0x42, 0xa1, 0x49, 0x54, 0x49, 0x6f]).await, "・‐—〜˜◉");
    assert_eq!(decode(converter.as_mut(), &[0x50, 0x5e, 0x88, 0x61, 0xd0, 0x61]).await, "\u{f843}가하");
}

#[tokio::test]
async fn simple_euc_decoders_preserve_text_controls_private_use_and_ss2_ss3_retry_boundaries() {
    for (name, text_bytes, text, pair, first_character, private_pair) in [
        ("x-IBM1383", [0xd6, 0xd0, 0xce, 0xc4], "中文", [0xd6, 0xd0], "中", [0xa2, 0xa1]),
        ("x-IBM970", [0xc7, 0xd1, 0xb1, 0xdb], "한글", [0xc7, 0xd1], "한", [0xc9, 0xa1]),
    ] {
        let mut converter = open(name);
        assert_eq!(decode(converter.as_mut(), &text_bytes).await, text, "{name}");
        assert_eq!(decode(converter.as_mut(), &private_pair).await, "\u{e000}", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0, b'A', 0x7f, 0x80, 0x8d, 0x90, 0x9f, pair[0], pair[1]]).await, format!("\0A\u{7f}\u{80}\u{8d}\u{90}\u{9f}{first_character}"), "{name}");
        for prefix in [0x8e, 0x8f] {
            assert_eq!(decode(converter.as_mut(), &[prefix, b'A']).await, "\u{fffd}A", "{name}");
            assert_eq!(decode(converter.as_mut(), &[prefix, 0]).await, "\u{fffd}\0", "{name}");
            assert_eq!(decode(converter.as_mut(), &[prefix, pair[0], pair[1]]).await, format!("\u{fffd}{first_character}"), "{name}");
            assert_eq!(decode(converter.as_mut(), &[prefix]).await, "\u{fffd}", "{name}");
            assert_eq!(decode(converter.as_mut(), &pair).await, first_character, "{name}");
        }
        assert_eq!(decode(converter.as_mut(), &[0x8e, 0x8f, b'A']).await, "\u{fffd}\u{fffd}A", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0xa0, 0x8e, b'A']).await, "\u{fffd}A", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0xa0, pair[0], pair[1]]).await, "\u{fffd}\u{fffd}", "{name}");
        for lead in [0xa0, 0xa1, 0xfe, 0xff] {
            assert_eq!(decode(converter.as_mut(), &[lead, b'A']).await, "\u{fffd}", "{name}");
            assert_eq!(decode(converter.as_mut(), &[lead, 0, b'B']).await, "\u{fffd}B", "{name}");
            assert_eq!(decode(converter.as_mut(), &[lead]).await, "\u{fffd}", "{name}");
        }
        assert_eq!(decode(converter.as_mut(), &[b'A', 0xa1, 0]).await, "A\u{fffd}", "{name}");
        let mut separated = decode(converter.as_mut(), &[b'A', 0xa1]).await;
        separated.push_str(&decode(converter.as_mut(), &[0]).await);
        assert_eq!(separated, "A\u{fffd}\0", "{name}");
    }
}

#[tokio::test]
async fn subclass_configuration_preserves_aliases_trimming_and_selected_serializer_validation() {
    let mut trimmed = open("\0\tJIS0208\r\u{1f}");
    assert_eq!(decode(trimmed.as_mut(), &[0x24, 0x22]).await, "あ");
    for invalid in ["JIS-X0208", "x-IBM-834", "IBM_EUCCN", " IBM970 "] {
        assert!(PluginRegistry::default().converter(&spec(&[("value.deserializer.encoding", invalid)]), false).is_err(), "{invalid}");
    }
    let options = spec(&[
        ("converter.encoding", "JIS0208"),
        ("key.serializer.encoding", "ibm-euccn"),
        ("key.deserializer.encoding", "cp300"),
        ("value.serializer.encoding", "cp834"),
        ("value.deserializer.encoding", "ibm-eucKR"),
    ]);
    let mut key = PluginRegistry::default().converter(&options, true).unwrap();
    let mut value = PluginRegistry::default().converter(&options, false).unwrap();
    assert_eq!(decode(key.as_mut(), &[0x44, 0x81]).await, "あ");
    assert_eq!(decode(value.as_mut(), &[0xc7, 0xd1]).await, "한");
    for (is_key, serializer) in [(true, "key.serializer.encoding"), (false, "value.serializer.encoding")] {
        assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", "cp1383"), (serializer, "IBM_EUCCN")]), is_key).is_err());
    }
}

#[tokio::test]
async fn subclass_mappings_and_replacements_obey_scalar_memory_admission() {
    for (name, pair, expected) in [
        ("x-JIS0208", [0x24, 0x22], "あ"),
        ("JIS_X0212-1990", [0x30, 0x21], "丂"),
        ("x-IBM300", [0x44, 0x81], "あ"),
        ("x-IBM834", [0x88, 0x61], "가"),
        ("x-IBM1383", [0xd6, 0xd0], "中"),
        ("x-IBM970", [0xc7, 0xd1], "한"),
        ("x-IBM834", [0, 0], "\u{fffd}"),
        ("x-IBM1383", [0x8e, 0x8f], "\u{fffd}\u{fffd}"),
    ] {
        let bytes = pair.repeat(1024);
        let mut converter = open(name);
        let mut rejected = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4 - 1));
        assert!(converter.decode("events", Some(&bytes), &mut rejected).await.is_err());
        let mut admitted = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4));
        assert_eq!(converter.decode("events", Some(&bytes), &mut admitted).await.unwrap().value, Value::Utf8(expected.repeat(1024)), "{name}");
    }
}
