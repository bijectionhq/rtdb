use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConversionContext, Converter, PluginRegistry, PluginSpec, SchemaKind};
use rtdb_types::Value;

const MANIFEST: &str = include_str!("../src/connect/converter/charset/double_byte/hkscs/charsets.tsv");
const OVERLAYS: &str = include_str!("../src/connect/converter/charset/double_byte/hkscs/overlays.tsv");

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
fn hkscs_overlays_and_provider_sources_retain_pinned_provenance() {
    assert_eq!(sha256_hex(MANIFEST.as_bytes()), "c9053f7bd4712874a6a933343a4f5d9bd1468a6e0f4ef04aec68f89ecd6c17cd");
    assert_eq!(sha256_hex(OVERLAYS.as_bytes()), "4594b763d6f4ffd21a9df0909f72520559b007c32c8204fd7b27331c0fb5e6a5");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/hkscs/aliases.rs")), "9fca29207527fdbfb24bd818ffbb8692e200963523a3fed7ec2472d36f780414");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/hkscs/tables.rs")), "d16d5307d9a059caa4568cd5f178f42fe38bea6dc4ab33db660e02d09c503240");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/hkscs/sources.sha256")), "4162674fda6e1edc8fa4cabdacaca25ff28399b2f2447e27150ca0608f846d4b");
    let blobs: [&[u8]; 3] = [
        include_bytes!("../src/connect/converter/charset/double_byte/hkscs/hkscsmapping.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/hkscs/hkscs2001mapping.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/hkscs/hkscs_xpmapping.bin"),
    ];
    assert_eq!(OVERLAYS.lines().count(), 4);
    assert_eq!(MANIFEST.lines().count(), 5);
    for (line, blob) in OVERLAYS.lines().skip(1).zip(blobs) {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 6);
        let rows: usize = fields[3].parse().unwrap();
        assert_eq!(blob.len(), fields[4].parse::<usize>().unwrap());
        assert_eq!(blob.len(), 512 + rows * 191 * 4);
        assert_eq!(sha256_hex(blob), fields[5]);
        let mut present = 0;
        for bytes in blob[..512].chunks_exact(2) {
            let row = u16::from_be_bytes([bytes[0], bytes[1]]);
            if row != 0xffff {
                assert_eq!(usize::from(row), present);
                present += 1;
            }
        }
        assert_eq!(present, rows);
        for bytes in blob[512..].chunks_exact(4) {
            assert!(char::from_u32(u32::from_be_bytes(bytes.try_into().unwrap())).is_some());
        }
    }
}

#[tokio::test]
async fn every_hkscs_name_decodes_all_single_bytes_and_pairs_as_independent_records() {
    let mut names = std::collections::BTreeSet::new();
    for line in MANIFEST.lines().skip(1) {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 9);
        for name in std::iter::once(fields[0]).chain(fields[8].split('|')) {
            names.insert(name.to_ascii_lowercase());
            for selected in [name.to_owned(), name.to_ascii_uppercase()] {
                let mut converter = open(&selected);
                let mut singles = String::new();
                for byte in 0u8..=255 { singles.push_str(&decode(converter.as_mut(), &[byte]).await); }
                assert_eq!(sha256_hex(singles.as_bytes()), fields[6], "{selected}");
                let mut pairs = String::new();
                for first in 0u8..=255 {
                    for second in 0u8..=255 {
                        pairs.push_str(&decode(converter.as_mut(), &[first, second]).await);
                        pairs.push('\0');
                    }
                }
                assert_eq!(sha256_hex(pairs.as_bytes()), fields[7], "{selected}");
                assert_eq!(decode(converter.as_mut(), &[]).await, "");
                assert_eq!(converted(converter.as_mut(), None).await, Value::Null);
            }
        }
    }
    assert_eq!(names.len(), 14);
}

#[tokio::test]
async fn hkscs_variants_preserve_overlay_priority_supplementary_rows_and_base_fallbacks() {
    let bytes = [0x87, 0x40, 0x87, 0x45, 0x88, 0x40, 0x88, 0x45, 0x8b, 0xc2, 0xfa, 0x40, 0xa4, 0xa4, 0xa3, 0xe1, 0x81, 0x40];
    for (name, expected) in [
        ("Big5-HKSCS", "\u{43f0}\u{27267}\u{31c0}\u{2010c}\u{21bc1}\u{20547}中\u{fffd}\u{fffd}"),
        ("x-Big5-HKSCS-2001", "\u{fffd}\u{fffd}\u{f303}\u{2010c}\u{21bc1}\u{20547}中\u{fffd}\u{fffd}"),
        ("x-MS950-HKSCS", "\u{43f0}\u{27267}\u{31c0}\u{2010c}\u{21bc1}\u{20547}中€\u{eeb8}"),
        ("x-MS950-HKSCS-XP", "\u{f266}\u{f26b}\u{f303}\u{f308}\u{f53a}\u{e000}中€\u{eeb8}"),
    ] {
        let mut converter = open(name);
        assert_eq!(decode(converter.as_mut(), &bytes).await, expected, "{name}");
        assert_eq!(decode(converter.as_mut(), &[0xf9, 0xd6, 0xf9, 0xd7, 0xf9, 0xd8, 0xf9, 0xd9, 0xf9, 0xda, 0xf9, 0xdb, 0xf9, 0xdc]).await, "碁銹裏墻恒粧嫺", "{name}");
        assert_eq!(decode(converter.as_mut(), b"\0\t\n\\~\x7f").await, "\0\t\n\\~\u{7f}", "{name}");
    }
}

#[tokio::test]
async fn hkscs_consumes_invalid_trails_and_keeps_truncation_local_to_each_record() {
    for line in MANIFEST.lines().skip(1) {
        let name = line.split('\t').next().unwrap();
        let mut converter = open(name);
        for pair in [[0xff, b'A'], [0xff, 0], [0x80, 0x80], [0xa4, 0x20], [0xa4, 0xff], [0xa4, 0x7f]] {
            assert_eq!(decode(converter.as_mut(), &pair).await, "\u{fffd}", "{name} {pair:x?}");
        }
        for byte in 0x80u8..=255 { assert_eq!(decode(converter.as_mut(), &[byte]).await, "\u{fffd}", "{name} {byte:02x}"); }
        assert_eq!(decode(converter.as_mut(), &[b'A', 0xa4]).await, "A\u{fffd}", "{name}");
        assert_eq!(decode(converter.as_mut(), &[b'A', 0xa4, 0]).await, "A\u{fffd}", "{name}");
        assert_eq!(decode(converter.as_mut(), &[b'A', 0xa4, 0, b'B']).await, "A\u{fffd}B", "{name}");
        let mut separated = decode(converter.as_mut(), &[b'A', 0xa4]).await;
        separated.push_str(&decode(converter.as_mut(), &[0]).await);
        assert_eq!(separated, "A\u{fffd}\0", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0xff, b'A', b'B', 0xa4, 0xa4, 0x80]).await, "\u{fffd}B中\u{fffd}", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0xa4]).await, "\u{fffd}", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0x40]).await, "@", "{name}");
    }
}

#[tokio::test]
async fn hkscs_configuration_preserves_alias_punctuation_trimming_and_key_value_precedence() {
    let mut trimmed = open("\0\tBig5_HKSCS\r\u{1f}");
    assert_eq!(decode(trimmed.as_mut(), &[0x87, 0x45]).await, "\u{27267}");
    let mut punctuated = open("big5-hkscs:unicode3.0");
    assert_eq!(decode(punctuated.as_mut(), &[0x88, 0x45]).await, "\u{2010c}");
    for invalid in ["Big5HKSCS2001", "Big5-HKSCS_2001", "x_Big5_HKSCS_2001", "MS950-HKSCS", "MS950-HKSCS-XP", " Big5_HKSCS "] {
        assert!(PluginRegistry::default().converter(&spec(&[("value.deserializer.encoding", invalid)]), false).is_err(), "{invalid}");
    }
    let options = spec(&[
        ("converter.encoding", "Big5-HKSCS"),
        ("key.serializer.encoding", "MS950_HKSCS_XP"),
        ("key.deserializer.encoding", "Big5_HKSCS_2001"),
        ("value.serializer.encoding", "big5hkscs"),
        ("value.deserializer.encoding", "MS950_HKSCS_XP"),
    ]);
    let mut key = PluginRegistry::default().converter(&options, true).unwrap();
    let mut value = PluginRegistry::default().converter(&options, false).unwrap();
    assert_eq!(decode(key.as_mut(), &[0x88, 0x45]).await, "\u{2010c}");
    assert_eq!(decode(value.as_mut(), &[0x88, 0x45]).await, "\u{f308}");
    for (is_key, serializer) in [(true, "key.serializer.encoding"), (false, "value.serializer.encoding")] {
        assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", "Big5-HKSCS"), (serializer, "MS950-HKSCS")]), is_key).is_err());
    }
}

#[tokio::test]
async fn hkscs_supplementary_and_replacement_output_obeys_scalar_memory_admission() {
    for (name, bytes, expected) in [
        ("Big5-HKSCS", [0x87, 0x45], "\u{27267}"),
        ("x-Big5-HKSCS-2001", [0x88, 0x45], "\u{2010c}"),
        ("x-MS950-HKSCS", [0x87, 0x45], "\u{27267}"),
        ("x-MS950-HKSCS-XP", [0x8b, 0xc2], "\u{f53a}"),
        ("Big5-HKSCS", [0xff, 0], "\u{fffd}"),
    ] {
        let bytes = bytes.repeat(1024);
        let mut converter = open(name);
        let mut rejected = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4 - 1));
        assert!(converter.decode("events", Some(&bytes), &mut rejected).await.is_err());
        let mut admitted = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4));
        assert_eq!(converter.decode("events", Some(&bytes), &mut admitted).await.unwrap().value, Value::Utf8(expected.repeat(1024)), "{name}");
    }
}
