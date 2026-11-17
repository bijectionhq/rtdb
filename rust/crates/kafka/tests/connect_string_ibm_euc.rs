use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConversionContext, Converter, PluginRegistry, PluginSpec, SchemaKind};
use rtdb_types::Value;

const MANIFEST: &str = include_str!("../src/connect/converter/charset/double_byte/ibm_euc/charsets.tsv");

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
fn ibm_euc_resources_preserve_provider_provenance_complete_groups_and_ibm943_availability() {
    assert_eq!(sha256_hex(MANIFEST.as_bytes()), "4c9ec754b37d52226463436a4f93b749e8b67587a32353338b2772fd27eca4c8");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/ibm_euc/aliases.rs")), "23803f3f59ad1cff6422a6e7b234dcf4382b114fbaa9a06ea0c67a6234ceae92");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/ibm_euc/tables.rs")), "b3f6428fd6394123f2d589e019847125f5690f0900a1561818b610ff57bb9d47");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/ibm_euc/sources.sha256")), "c65b277344f6d8037e7947406e99d1da05424911c96c761f24db3097877a3705");
    let mappings: [&[u8]; 2] = [
        include_bytes!("../src/connect/converter/charset/double_byte/ibm_euc/ibm29626c.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/ibm_euc/ibm33722.bin"),
    ];
    assert_eq!(MANIFEST.lines().count(), 3);
    assert_eq!(mappings.iter().map(|mapping| mapping.len()).sum::<usize>(), 71_704);
    for (index, (line, mapping)) in MANIFEST.lines().skip(1).zip(mappings).enumerate() {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 11);
        assert_eq!(mapping.len(), fields[4].parse::<usize>().unwrap());
        assert_eq!(sha256_hex(mapping), fields[5]);
        let units: Vec<_> = mapping.chunks_exact(2).map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]])).collect();
        assert_eq!(units.len(), 17_926);
        assert!(units.iter().all(|unit| !(0xd800..=0xdfff).contains(unit)));
        let counts = [(0, 160), (160, 8996), (8996, 9090), (9090, 17_926)].map(|(start, end)| units[start..end].iter().filter(|&&unit| unit != 0xfffd).count());
        assert_eq!(counts, if index == 0 { [158, 7902, 68, 1355] } else { [158, 7819, 68, 1326] });
    }
    let availability = include_bytes!("../src/connect/converter/charset/double_byte/ibm_euc/ibm943-can-encode.bin");
    assert_eq!(availability.len(), 8192);
    assert_eq!(sha256_hex(availability), "6ddbc70206b6bff90901f3b0b36ad0ce06fca75eef56e316046d0ec5e563d776");
    assert_eq!(availability.iter().map(|byte| byte.count_ones()).sum::<u32>(), 9442);
    for (unit, expected) in [(0x02d8usize, false), (0xff5e, true), (0x4fe0, true), (0xf86f, true), (0x2116, true), (0x2160, true), (0xfffd, false), (0x005c, false), (0x007e, false)] {
        assert_eq!(availability[unit / 8] & (0x80 >> (unit % 8)) != 0, expected, "{unit:04x}");
    }
}

#[tokio::test]
async fn every_ibm_euc_name_decodes_all_single_bytes_pairs_and_ss2_ss3_prefixed_triples() {
    let mut names = std::collections::BTreeSet::new();
    for line in MANIFEST.lines().skip(1) {
        let fields: Vec<_> = line.split('\t').collect();
        for name in std::iter::once(fields[0]).chain(fields[10].split('|')) {
            names.insert(name.to_ascii_lowercase());
            for selected in [name.to_owned(), name.to_ascii_uppercase()] {
                let mut converter = open(&selected);
                let mut singles = String::new();
                for byte in 0u8..=255 { singles.push_str(&decode(converter.as_mut(), &[byte]).await); }
                assert_eq!(sha256_hex(singles.as_bytes()), fields[6], "{selected}");
                for (prefix, expected) in [(None, fields[7]), (Some(0x8e), fields[8]), (Some(0x8f), fields[9])] {
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
    assert_eq!(names.len(), 13);
}

#[tokio::test]
async fn ibm_euc_variants_preserve_c1_halfwidth_extra_symbols_and_japanese_text() {
    for name in ["x-IBM29626C", "x-IBM33722"] {
        let mut converter = open(name);
        assert_eq!(decode(converter.as_mut(), &[0, 0x09, 0x1b, 0x80, 0x8d, 0x90, 0x9f]).await, "\0\t\u{1b}\u{80}\u{8d}\u{90}\u{9f}", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0xa4, 0xa2, 0xa4, 0xa4, 0xc6, 0xfc, 0xcb, 0xdc]).await, "あい日本", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0x8e, 0xa1, 0x8e, 0xb6, 0x8e, 0xde, 0x8e, 0xdf]).await, "｡ｶﾞﾟ", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0x8e, 0xe0, 0x8e, 0xe1, 0x8e, 0xe2, 0x8e, 0xe3, 0x8e, 0xe4]).await, "¢£¬\\~", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0xa1, 0xf1, 0xa1, 0xf2, 0xa2, 0xcc]).await, "￠￡￢", "{name}");
    }
    let mut ibm29626c = open("ibm-eucjp");
    let mut ibm33722 = open("ibm-5050");
    assert_eq!(decode(ibm29626c.as_mut(), b"\\~").await, "\\~");
    assert_eq!(decode(ibm33722.as_mut(), b"\\~").await, "¥‾");
    assert_eq!(decode(ibm29626c.as_mut(), &[0xad, 0xa1]).await, "①");
    assert_eq!(decode(ibm33722.as_mut(), &[0xad, 0xa1]).await, "\u{fffd}");
}

#[tokio::test]
async fn ibm29626c_jis0212_uses_ibm943_encoder_availability_and_first_matching_overlay() {
    let mut converter = open("x-IBM29626C");
    assert_eq!(decode(converter.as_mut(), &[0x8f, 0xa2, 0xaf]).await, "\u{fffd}");
    assert_eq!(decode(converter.as_mut(), &[0x8f, 0xa2, 0xb7, 0x8f, 0xb1, 0xc2]).await, "～俠");
    assert_eq!(decode(converter.as_mut(), &[0x8f, 0xa2, 0xf1]).await, "\u{fffd}");
    assert_eq!(decode(converter.as_mut(), &[0x8f, 0xf3, 0xb8]).await, "№");
    assert_eq!(decode(converter.as_mut(), &[0x8f, 0xb0, 0xa9]).await, "丨");
    let mut ibm33722 = open("cp33722");
    assert_eq!(decode(ibm33722.as_mut(), &[0x8f, 0xa2, 0xb7, 0x8f, 0xb1, 0xc2]).await, "\u{fffd}\u{fffd}");
    assert_eq!(decode(ibm33722.as_mut(), &[0x8f, 0xf3, 0xb8]).await, "№");
    let mut standard = open("EUC-JP");
    assert_eq!(decode(standard.as_mut(), &[0x8f, 0xa2, 0xaf]).await, "˘");
    assert_eq!(decode(standard.as_mut(), &[0x8f, 0xa2, 0xf1]).await, "№");
}

#[tokio::test]
async fn ibm_euc_private_use_groups_preserve_both_940_character_ranges() {
    for name in ["x-IBM29626C", "x-IBM33722"] {
        let mut converter = open(name);
        assert_eq!(decode(converter.as_mut(), &[0xf5, 0xa1, 0xfe, 0xfe, 0x8f, 0xf5, 0xa1, 0x8f, 0xfe, 0xfe]).await, "\u{e000}\u{e3ab}\u{e3ac}\u{e757}", "{name}");
        let mut expected = 0xe000;
        for first in 0xf5..=0xfe {
            for second in 0xa1..=0xfe {
                assert_eq!(decode(converter.as_mut(), &[first, second]).await, char::from_u32(expected).unwrap().to_string(), "{name}");
                assert_eq!(decode(converter.as_mut(), &[0x8f, first, second]).await, char::from_u32(expected + 940).unwrap().to_string(), "{name}");
                expected += 1;
            }
        }
        assert_eq!(expected, 0xe3ac);
    }
}

#[tokio::test]
async fn ibm_euc_invalid_leads_and_ss3_second_bytes_keep_variant_error_lengths() {
    for (name, invalid_pair, invalid_ss3) in [
        ("x-IBM29626C", "\u{fffd}B", "\u{fffd}C"),
        ("x-IBM33722", "\u{fffd}AB", "\u{fffd}BC"),
    ] {
        let mut converter = open(name);
        for first in [0xa0, 0xff] {
            assert_eq!(decode(converter.as_mut(), &[first, b'A', b'B']).await, invalid_pair, "{name}");
        }
        assert_eq!(decode(converter.as_mut(), &[0x8f, b'A', b'B', b'C']).await, invalid_ss3, "{name}");
        for (bytes, expected) in [
            (&[0x8e][..], "\u{fffd}"),
            (&[0x8f][..], "\u{fffd}"),
            (&[0x8f, b'A'][..], "\u{fffd}"),
            (&[0x8f, 0xa1][..], "\u{fffd}"),
            (&[0x8e, b'A', b'B'][..], "\u{fffd}B"),
            (&[0x8e, 0, b'A'][..], "\u{fffd}A"),
            (&[0x8f, 0xa1, b'A', b'C'][..], "\u{fffd}C"),
            (&[0x8f, 0xa1, 0xa1, b'A'][..], "\u{fffd}A"),
            (&[0xa1, 0x8f, b'A'][..], "\u{fffd}A"),
            (&[0xa1, 0, b'A'][..], "\u{fffd}A"),
            (&[0xa4, 0xa2, 0x8f, 0xa2][..], "あ\u{fffd}"),
        ] {
            assert_eq!(decode(converter.as_mut(), bytes).await, expected, "{name} {bytes:x?}");
        }
    }
    let mut ibm33722 = open("cp33722");
    assert_eq!(decode(ibm33722.as_mut(), &[0x8f, b'A', 0xa4, 0xa2]).await, "\u{fffd}あ");
    let mut ibm29626c = open("cp29626c");
    assert_eq!(decode(ibm29626c.as_mut(), &[0x8f, b'A', 0xa4, 0xa2]).await, "\u{fffd}\u{fffd}");
}

#[tokio::test]
async fn ibm_euc_record_boundaries_reset_incomplete_prefixes_and_preserve_result_separators() {
    for name in ["x-IBM29626C", "x-IBM33722"] {
        let mut converter = open(name);
        assert_eq!(decode(converter.as_mut(), &[0x8f, 0xf3]).await, "\u{fffd}");
        assert_eq!(converted(converter.as_mut(), None).await, Value::Null);
        assert_eq!(decode(converter.as_mut(), &[]).await, "");
        assert_eq!(decode(converter.as_mut(), &[0xb8]).await, "\u{fffd}");
        assert_eq!(decode(converter.as_mut(), &[0x8f, 0xf3, 0xb8]).await, "№");
        let mut separated = decode(converter.as_mut(), &[0xa1]).await;
        separated.push_str(&decode(converter.as_mut(), &[0]).await);
        assert_eq!(separated, "\u{fffd}\0");
        assert_eq!(decode(converter.as_mut(), &[0xa1, 0]).await, "\u{fffd}");
    }
}

#[tokio::test]
async fn ibm_euc_configuration_keeps_alias_punctuation_trimming_and_key_value_precedence() {
    let mut trimmed = open("\0\tIBM-EUCJP\r\u{1f}");
    assert_eq!(decode(trimmed.as_mut(), &[0xad, 0xa1]).await, "①");
    for invalid in ["IBM_29626C", "cp-33722", "xIBM33722", " IBM33722 "] {
        assert!(PluginRegistry::default().converter(&spec(&[("value.deserializer.encoding", invalid)]), false).is_err(), "{invalid}");
    }
    let options = spec(&[
        ("converter.encoding", "ibm-eucjp"),
        ("key.serializer.encoding", "ibm-5050"),
        ("key.deserializer.encoding", "cp29626c"),
        ("value.serializer.encoding", "29626c"),
        ("value.deserializer.encoding", "ibm-33722_vascii_vpua"),
    ]);
    let mut key = PluginRegistry::default().converter(&options, true).unwrap();
    let mut value = PluginRegistry::default().converter(&options, false).unwrap();
    assert_eq!(decode(key.as_mut(), b"\\~").await, "\\~");
    assert_eq!(decode(value.as_mut(), b"\\~").await, "¥‾");
    for (is_key, serializer) in [(true, "key.serializer.encoding"), (false, "value.serializer.encoding")] {
        assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", "ibm-eucjp"), (serializer, "IBM_29626C")]), is_key).is_err());
    }
}

#[tokio::test]
async fn ibm_euc_mappings_replacements_and_truncated_groups_obey_scalar_memory_admission() {
    for name in ["x-IBM29626C", "x-IBM33722"] {
        let mut converter = open(name);
        for (bytes, expected) in [
            ([0xa4, 0xa2].repeat(1024), "あ".repeat(1024)),
            ([0x8e, 0xe0].repeat(1024), "¢".repeat(1024)),
            ([0x8f, 0xf5, 0xa1].repeat(1024), "\u{e3ac}".repeat(1024)),
            (vec![0x8f, 0xa2], "\u{fffd}".to_owned()),
        ] {
            let mut rejected = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4 - 1));
            assert!(converter.decode("events", Some(&bytes), &mut rejected).await.is_err());
            let mut admitted = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4));
            assert_eq!(converter.decode("events", Some(&bytes), &mut admitted).await.unwrap().value, Value::Utf8(expected), "{name}");
        }
    }
}
