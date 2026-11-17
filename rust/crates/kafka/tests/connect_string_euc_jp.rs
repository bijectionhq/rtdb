use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConversionContext, Converter, PluginRegistry, PluginSpec, SchemaKind};
use rtdb_types::Value;

const MANIFEST: &str = include_str!("../src/connect/converter/charset/double_byte/euc_jp/charsets.tsv");

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
fn euc_jp_resources_preserve_source_mappings_provider_provenance_and_existing_jis_tables() {
    const MAPPINGS: &str = include_str!("../src/connect/converter/charset/double_byte/euc_jp/mappings.tsv");
    assert_eq!(sha256_hex(MANIFEST.as_bytes()), "7021c062f8e4f324d1610e294660cfe7cee8b47ba115eb6b1a956110a0b21010");
    assert_eq!(sha256_hex(MAPPINGS.as_bytes()), "dc48bd50acbe1d006a238bc3c91435b3f6c651b2b0d68337c0cbeefff58ede87");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/euc_jp/aliases.rs")), "e8181fd14abf7fc5cccdb4ba1923a0a5d6384161819f923156a72981528e64f7");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/euc_jp/tables.rs")), "ca2d3f5ec3c26dd5b9eb603c191ef5961827e757afee035b1b0f2e4ba91065e3");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/euc_jp/sources.sha256")), "10127eb2251b9236e9c387c761b8ce11d2d227db075d250c90c610f70c2ec117");
    let mappings: [&[u8]; 5] = [
        include_bytes!("../src/connect/converter/charset/double_byte/euc_jp/jis_x_0201.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/euc_jp/jis_x_0208.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/euc_jp/jis_x_0212.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/euc_jp/jis_x_0208_solaris.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/euc_jp/jis_x_0212_solaris.bin"),
    ];
    assert_eq!(MAPPINGS.lines().count(), 6);
    assert_eq!(mappings.iter().map(|mapping| mapping.len()).sum::<usize>(), 47_104);
    assert_eq!(mappings[1], include_bytes!("../src/connect/converter/charset/double_byte/subclasses/x-jis0208.bin"));
    assert_eq!(mappings[2], include_bytes!("../src/connect/converter/charset/double_byte/subclasses/jis_x0212-1990.bin"));
    for (index, (line, mapping)) in MAPPINGS.lines().skip(1).zip(mappings).enumerate() {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 7);
        assert_eq!(mapping.len(), fields[4].parse::<usize>().unwrap());
        assert_eq!(sha256_hex(mapping), fields[5]);
        let unit = |offset: usize| u16::from_be_bytes([mapping[offset], mapping[offset + 1]]);
        if index == 0 {
            for byte in 0u8..=255 {
                let expected = match byte { 0..=127 => u16::from(byte), 0xa1..=0xdf => 0xff61 + u16::from(byte - 0xa1), _ => 0xfffd };
                assert_eq!(unit(usize::from(byte) * 2), expected);
            }
            continue;
        }
        assert_eq!(&mapping[..2], &[0x21, 0x7e]);
        let rows: usize = fields[6].parse().unwrap();
        assert_eq!(mapping.len(), 1026 + rows * 94 * 2);
        let mut present = 0;
        for byte in 0u8..=255 {
            assert_eq!(unit(2 + usize::from(byte) * 2), 0xfffd);
            let row = unit(514 + usize::from(byte) * 2);
            if row != 0xffff {
                assert_eq!(usize::from(row), present);
                present += 1;
            }
        }
        assert_eq!(present, rows);
        for offset in (1026..mapping.len()).step_by(2) { assert!(!(0xd800..=0xdfff).contains(&unit(offset))); }
    }
}

#[tokio::test]
async fn every_euc_jp_name_decodes_all_single_bytes_pairs_and_ss2_ss3_prefixed_triples() {
    let mut names = std::collections::BTreeSet::new();
    assert_eq!(MANIFEST.lines().count(), 4);
    for line in MANIFEST.lines().skip(1) {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 10);
        for name in std::iter::once(fields[0]).chain(fields[9].split('|')) {
            names.insert(name.to_ascii_lowercase());
            for selected in [name.to_owned(), name.to_ascii_uppercase()] {
                let mut converter = open(&selected);
                let mut singles = String::new();
                for byte in 0u8..=255 { singles.push_str(&decode(converter.as_mut(), &[byte]).await); }
                assert_eq!(sha256_hex(singles.as_bytes()), fields[5], "{selected}");
                for (prefix, expected) in [(None, fields[6]), (Some(0x8e), fields[7]), (Some(0x8f), fields[8])] {
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
    assert_eq!(names.len(), 14);
}

#[tokio::test]
async fn euc_jp_variants_preserve_ascii_halfwidth_and_jis0208_characters() {
    for name in ["EUC-JP", "x-euc-jp-linux", "x-eucJP-Open"] {
        let mut converter = open(name);
        assert_eq!(decode(converter.as_mut(), b"\\~\0\t\x1b").await, "\\~\0\t\u{1b}", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0xa4, 0xa2, 0xa4, 0xa4, 0xc6, 0xfc, 0xcb, 0xdc]).await, "あい日本", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0x8e, 0xa1, 0x8e, 0xb6, 0x8e, 0xde, 0x8e, 0xdf]).await, "｡ｶﾞﾟ", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0xa1, 0xc1, 0xa1, 0xc2, 0xa1, 0xef, 0xa1, 0xb1]).await, "〜‖￥￣", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0x8e, 0x5c, 0x8e, 0x7e]).await, "\u{fffd}\u{fffd}", "{name}");
    }
}

#[tokio::test]
async fn euc_jp_linux_rejects_jis0212_and_solaris_preserves_base_with_additional_mappings() {
    for (name, expected) in [
        ("EUC-JP", "あ？\u{fffd}˘\u{fffd}"),
        ("x-euc-jp-linux", "あ？\u{fffd}\u{fffd}\u{fffd}"),
        ("x-eucJP-Open", "あ？①˘ⅰ"),
    ] {
        let mut converter = open(name);
        assert_eq!(decode(converter.as_mut(), &[0xa4, 0xa2, 0xa1, 0xa9, 0xad, 0xa1, 0x8f, 0xa2, 0xaf, 0x8f, 0xf3, 0xf3]).await, expected, "{name}");
    }
    let mut standard = open("EUC-JP");
    let mut linux = open("x-euc-jp-linux");
    let mut solaris = open("EUC_JP_Solaris");
    assert_eq!(decode(standard.as_mut(), &[0xad, 0xa1]).await, "\u{fffd}");
    assert_eq!(decode(solaris.as_mut(), &[0xad, 0xa1]).await, "①");
    assert_eq!(decode(standard.as_mut(), &[0xad, 0xa1]).await, "\u{fffd}");
    assert_eq!(decode(linux.as_mut(), &[0x8f, 0xa2, 0xaf, b'A']).await, "\u{fffd}A");
}

#[tokio::test]
async fn euc_jp_malformed_pairs_triples_and_truncations_consume_java_error_lengths() {
    for name in ["EUC-JP", "x-euc-jp-linux", "x-eucJP-Open"] {
        let mut converter = open(name);
        for (bytes, expected) in [
            (&[0x80][..], "\u{fffd}"),
            (&[0x8e][..], "\u{fffd}"),
            (&[0x8f][..], "\u{fffd}"),
            (&[0x8f, b'A'][..], "\u{fffd}"),
            (&[0x8f, 0xa1][..], "\u{fffd}"),
            (&[0x80, b'A', b'B'][..], "\u{fffd}B"),
            (&[0xff, b'A', b'B'][..], "\u{fffd}B"),
            (&[0x8e, b'A', b'B'][..], "\u{fffd}B"),
            (&[0x8e, 0, b'A'][..], "\u{fffd}A"),
            (&[0x8e, 0x8e, 0xa1, 0xa1][..], "\u{fffd}\u{3000}"),
            (&[0x8f, b'A', b'B', b'C'][..], "\u{fffd}C"),
            (&[0x8f, 0xa1, b'A', b'C'][..], "\u{fffd}C"),
            (&[0x8f, 0x8f, 0xa1, b'C'][..], "\u{fffd}C"),
            (&[0x8f, 0xa1, 0xa1, b'A'][..], "\u{fffd}A"),
            (&[0xa1, 0x8f, b'A'][..], "\u{fffd}A"),
            (&[0xa1, 0, b'A'][..], "\u{fffd}A"),
            (&[0xa4, 0xa2, 0x8f, 0xa2][..], "あ\u{fffd}"),
            (&[0x8e, 0xa1, b'A'][..], "｡A"),
        ] {
            assert_eq!(decode(converter.as_mut(), bytes).await, expected, "{name} {bytes:x?}");
        }
    }
}

#[tokio::test]
async fn euc_jp_records_do_not_retain_truncated_prefixes_or_consume_decoded_probe_separators() {
    for name in ["EUC-JP", "x-eucJP-Open"] {
        let mut converter = open(name);
        assert_eq!(decode(converter.as_mut(), &[0x8f, 0xa2]).await, "\u{fffd}");
        assert_eq!(converted(converter.as_mut(), None).await, Value::Null);
        assert_eq!(decode(converter.as_mut(), &[]).await, "");
        assert_eq!(decode(converter.as_mut(), &[0xaf]).await, "\u{fffd}");
        assert_eq!(decode(converter.as_mut(), &[0x8f, 0xa2, 0xaf]).await, "˘");
        let mut separated = decode(converter.as_mut(), &[0xa1]).await;
        separated.push_str(&decode(converter.as_mut(), &[0]).await);
        assert_eq!(separated, "\u{fffd}\0");
        assert_eq!(decode(converter.as_mut(), &[0xa1, 0]).await, "\u{fffd}");
    }
}

#[tokio::test]
async fn euc_jp_configuration_keeps_alias_punctuation_trimming_and_key_value_precedence() {
    let mut trimmed = open("\0\tEUC_JP_SOLARIS\r\u{1f}");
    assert_eq!(decode(trimmed.as_mut(), &[0xad, 0xa1]).await, "①");
    for invalid in ["euc.jp", "x_euc_jp", "eucjp-linux", " EUC-JP "] {
        assert!(PluginRegistry::default().converter(&spec(&[("value.deserializer.encoding", invalid)]), false).is_err(), "{invalid}");
    }
    let options = spec(&[
        ("converter.encoding", "euc_jp"),
        ("key.serializer.encoding", "euc-jp-linux"),
        ("key.deserializer.encoding", "eucJP-open"),
        ("value.serializer.encoding", "x-eucjp"),
        ("value.deserializer.encoding", "x-euc-jp-linux"),
    ]);
    let mut key = PluginRegistry::default().converter(&options, true).unwrap();
    let mut value = PluginRegistry::default().converter(&options, false).unwrap();
    assert_eq!(decode(key.as_mut(), &[0xad, 0xa1]).await, "①");
    assert_eq!(decode(value.as_mut(), &[0xad, 0xa1]).await, "\u{fffd}");
    for (is_key, serializer) in [(true, "key.serializer.encoding"), (false, "value.serializer.encoding")] {
        assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", "EUC-JP"), (serializer, "euc.jp")]), is_key).is_err());
    }
}

#[tokio::test]
async fn euc_jp_mappings_replacements_and_truncated_groups_obey_scalar_memory_admission() {
    for name in ["EUC-JP", "x-euc-jp-linux", "x-eucJP-Open"] {
        let mut converter = open(name);
        for (bytes, expected) in [
            ([0xa4, 0xa2].repeat(1024), "あ".repeat(1024)),
            ([0x8e, 0xa1].repeat(1024), "｡".repeat(1024)),
            (vec![0x8f, 0xa2], "\u{fffd}".to_owned()),
        ] {
            let mut rejected = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4 - 1));
            assert!(converter.decode("events", Some(&bytes), &mut rejected).await.is_err());
            let mut admitted = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4));
            assert_eq!(converter.decode("events", Some(&bytes), &mut admitted).await.unwrap().value, Value::Utf8(expected), "{name}");
        }
    }
}
