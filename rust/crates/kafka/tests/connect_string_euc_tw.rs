use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConversionContext, Converter, PluginRegistry, PluginSpec, SchemaKind};
use rtdb_types::Value;

const MANIFEST: &str = include_str!("../src/connect/converter/charset/double_byte/euc_tw/charsets.tsv");

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

const PLANES: &str = include_str!("../src/connect/converter/charset/double_byte/euc_tw/planes.tsv");

#[test]
fn euc_tw_resources_preserve_source_planes_supplementary_flags_and_ibm964_repertoire() {
    const MAPPINGS: &str = include_str!("../src/connect/converter/charset/double_byte/euc_tw/mappings.tsv");
    assert_eq!(sha256_hex(MANIFEST.as_bytes()), "26fe997f1ebdb2606015ce2cc8e68de1eebf1264b40e3bfa51bfc57053122fa1");
    assert_eq!(sha256_hex(MAPPINGS.as_bytes()), "876777893e26bd7c6ab493546d266290140b7cba4dea89ebc3799f5f63f1db46");
    assert_eq!(sha256_hex(PLANES.as_bytes()), "893498f607e79b1da8eda9b49423d2c5bdbd6b69195ffc4aa86f192658339b59");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/euc_tw/aliases.rs")), "01b21863799553c4b627e112d58495410d0ac2c0d99412d8b6651c74febcb28f");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/euc_tw/tables.rs")), "8f9238485ea47f90caa34c64185a21de85613121c6cb1604e2d09cbff21b269b");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/euc_tw/sources.sha256")), "5de165eafe0b85b9366a9c0070aba92be9515f90899ca3e13ef52722f36f8293");
    let standard = include_bytes!("../src/connect/converter/charset/double_byte/euc_tw/euc-tw-planes.bin");
    let flags = include_bytes!("../src/connect/converter/charset/double_byte/euc_tw/euc-tw-supp.bin");
    let ibm = include_bytes!("../src/connect/converter/charset/double_byte/euc_tw/ibm964.bin");
    let resources: [&[u8]; 3] = [standard, flags, ibm];
    assert_eq!(MAPPINGS.lines().count(), 4);
    assert_eq!(resources.iter().map(|resource| resource.len()).sum::<usize>(), 230_056);
    for (line, resource) in MAPPINGS.lines().skip(1).zip(resources) {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 5);
        assert_eq!(resource.len(), fields[3].parse::<usize>().unwrap());
        assert_eq!(sha256_hex(resource), fields[4]);
    }
    assert_eq!(flags.len(), 17_672);
    assert!(flags[8836..].iter().all(|&byte| byte == 0));
    assert_eq!(&standard[17_672..35_344], &ibm[(160 + 8836) * 2..(160 + 2 * 8836) * 2]);
    let expected_mapped = [5867, 7650, 6394, 7286, 8601, 6385, 6537, 6721];
    let expected_supplementary = [0, 0, 71, 3476, 8143, 6158, 6388, 6476];
    let mut total_supplementary = 0;
    for (plane, bytes) in standard.chunks_exact(17_672).enumerate() {
        let mut mapped = 0;
        let mut supplementary = 0;
        for (index, unit) in bytes.chunks_exact(2).enumerate() {
            let value = u16::from_be_bytes([unit[0], unit[1]]);
            if value == 0xfffd { continue; }
            mapped += 1;
            if flags[index] & (1 << plane) != 0 {
                supplementary += 1;
                assert!(char::from_u32(0x20000 + u32::from(value)).is_some());
            } else { assert!(!(0xd800..=0xdfff).contains(&value)); }
        }
        assert_eq!(mapped, expected_mapped[plane]);
        assert_eq!(supplementary, expected_supplementary[plane]);
        total_supplementary += supplementary;
    }
    assert_eq!(total_supplementary, 30_712);
    let ibm_units: Vec<_> = ibm.chunks_exact(2).map(|unit| u16::from_be_bytes([unit[0], unit[1]])).collect();
    assert!(ibm_units.iter().all(|unit| !(0xd800..=0xdfff).contains(unit)));
    for (plane, units) in ibm_units[160..].chunks_exact(8836).enumerate() {
        assert_eq!(units.iter().filter(|&&unit| unit != 0xfffd).count(), [6085, 7650, 6204, 325][plane]);
    }
}

#[tokio::test]
async fn every_euc_tw_name_decodes_all_bytes_pairs_plane_quartets_and_selector_cases() {
    let mut names = std::collections::BTreeSet::new();
    assert_eq!(MANIFEST.lines().count(), 3);
    assert_eq!(PLANES.lines().count(), 11);
    for line in MANIFEST.lines().skip(1) {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 9);
        let planes: Vec<_> = PLANES.lines().skip(1).map(|line| line.split('\t').collect::<Vec<_>>()).filter(|plane| plane[0] == fields[1]).collect();
        assert_eq!(planes.len(), if fields[1] == "EUC_TW" { 7 } else { 3 });
        for name in std::iter::once(fields[0]).chain(fields[8].split('|')) {
            names.insert(name.to_ascii_lowercase());
            for selected in [name.to_owned(), name.to_ascii_uppercase()] {
                let mut converter = open(&selected);
                let mut singles = String::new();
                for byte in 0u8..=255 { singles.push_str(&decode(converter.as_mut(), &[byte]).await); }
                assert_eq!(sha256_hex(singles.as_bytes()), fields[5], "{selected}");
                let mut pairs = String::new();
                for first in 0u8..=255 {
                    for second in 0u8..=255 {
                        pairs.push_str(&decode(converter.as_mut(), &[first, second]).await);
                        pairs.push('\0');
                    }
                }
                assert_eq!(sha256_hex(pairs.as_bytes()), fields[6], "{selected}");
                let mut selectors = String::new();
                for selector in 0u8..=255 {
                    for (first, second) in [(0xa1, 0xa1), (0, 0x41), (0xa1, 0), (0x8e, 0xa2), (0xff, 0xff)] {
                        selectors.push_str(&decode(converter.as_mut(), &[0x8e, selector, first, second]).await);
                        selectors.push('\0');
                    }
                }
                assert_eq!(sha256_hex(selectors.as_bytes()), fields[7], "{selected}");
                for plane in &planes {
                    assert_eq!(plane.len(), 6);
                    let selector = u8::from_str_radix(plane[1], 16).unwrap();
                    let mut quartets = String::new();
                    for first in 0u8..=255 {
                        for second in 0u8..=255 {
                            quartets.push_str(&decode(converter.as_mut(), &[0x8e, selector, first, second]).await);
                            quartets.push('\0');
                        }
                    }
                    assert_eq!(sha256_hex(quartets.as_bytes()), plane[5], "{selected} {selector:x}");
                }
                assert_eq!(decode(converter.as_mut(), &[]).await, "");
                assert_eq!(converted(converter.as_mut(), None).await, Value::Null);
            }
        }
    }
    assert_eq!(names.len(), 11);
}

#[tokio::test]
async fn euc_tw_variants_preserve_distinct_base_mappings_and_ibm_c1_singles() {
    for (name, text, symbols) in [
        ("EUC-TW", [0xc4, 0xe3, 0xc5, 0xc6], "・–\u{fffd}"),
        ("ibm-euctw", [0xc4, 0xe3, 0xa7, 0xe2], "‧﹘︳"),
    ] {
        let mut converter = open(name);
        assert_eq!(decode(converter.as_mut(), b"A\\~\0\t\x1b").await, "A\\~\0\t\u{1b}", "{name}");
        assert_eq!(decode(converter.as_mut(), &text).await, "中文", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0xa1, 0xa6, 0xa1, 0xb9, 0xa1, 0xba]).await, symbols, "{name}");
        assert_eq!(decode(converter.as_mut(), &[0x8e, 0xa2, 0xa1, 0xa1]).await, "乂", "{name}");
        assert_eq!(decode(converter.as_mut(), &[0xa3, 0xcf, b'A']).await, "\u{fffd}A", "{name}");
    }
    let mut standard = open("x-EUC-TW");
    let mut ibm = open("x-IBM964");
    assert_eq!(decode(standard.as_mut(), &[0x80, 0x8d, 0x90, 0x9f]).await, "\u{fffd}\u{fffd}\u{fffd}\u{fffd}");
    assert_eq!(decode(ibm.as_mut(), &[0x80, 0x8d, 0x90, 0x9f]).await, "\u{80}\u{8d}\u{90}\u{9f}");
}

#[tokio::test]
async fn euc_tw_supplementary_planes_emit_complete_scalars_and_reject_selected_plane_one() {
    let mut converter = open("cns11643");
    for (selector, first, second, expected) in [
        (0xa3, 0xa1, 0xa1, "丨"),
        (0xa3, 0xa1, 0xc4, "\u{2000b}"),
        (0xa4, 0xa1, 0xa1, "\u{20086}"),
        (0xa5, 0xa1, 0xa1, "\u{200d1}"),
        (0xa6, 0xa1, 0xa1, "\u{2f802}"),
        (0xa7, 0xa1, 0xa1, "\u{20055}"),
        (0xaf, 0xa1, 0xa1, "\u{20002}"),
    ] {
        assert_eq!(decode(converter.as_mut(), &[0x8e, selector, first, second]).await, expected);
    }
    for selector in [0xa1, 0xa8, 0xac, 0xad, 0xb0] {
        assert_eq!(decode(converter.as_mut(), &[0x8e, selector, 0xa1, 0xa1]).await, "\u{fffd}\u{3000}");
    }
    assert_eq!(decode(converter.as_mut(), &[0xa1, 0xa1]).await, "\u{3000}");
}

#[tokio::test]
async fn ibm964_ac_and_ad_planes_preserve_private_use_and_unsupported_selector_boundaries() {
    let mut converter = open("cp964");
    let mut expected = 0xe000;
    for first in 0xa1..=0xe2 {
        for second in 0xa1..=0xfe {
            assert_eq!(decode(converter.as_mut(), &[0x8e, 0xac, first, second]).await, char::from_u32(expected).unwrap().to_string());
            expected += 1;
        }
    }
    assert_eq!(expected, 0xf83c);
    assert_eq!(decode(converter.as_mut(), &[0x8e, 0xac, 0xe3, 0xa1]).await, "\u{fffd}");
    for (first, second, expected) in [(0xa1, 0xaa, '\u{f83e}'), (0xa4, 0xb9, '\u{f83f}'), (0xa4, 0xba, '\u{f840}'), (0xa4, 0xbb, '\u{f841}'), (0xa4, 0xbc, '\u{f842}')] {
        assert_eq!(decode(converter.as_mut(), &[0x8e, 0xad, first, second]).await, expected.to_string());
    }
    assert_eq!(decode(converter.as_mut(), &[0x8e, 0xad, 0xa1, 0xa1]).await, "゛");
    for selector in [0xa1, 0xa3, 0xa7, 0xaf] {
        assert_eq!(decode(converter.as_mut(), &[0x8e, selector, 0xa1, 0xa1]).await, "\u{fffd}\u{3000}");
    }
}

#[tokio::test]
async fn euc_tw_malformed_pair_and_quartet_lengths_follow_each_decoder() {
    for (name, pair_error, lead_error) in [
        ("EUC-TW", "\u{fffd}AB", "\u{fffd}B"),
        ("IBM964", "\u{fffd}B", "\u{fffd}AB"),
    ] {
        let mut converter = open(name);
        assert_eq!(decode(converter.as_mut(), &[0xa1, b'A', b'B']).await, pair_error, "{name}");
        assert_eq!(decode(converter.as_mut(), &[0x8e, 0xa2, 0, b'A', b'B']).await, lead_error, "{name}");
        for (bytes, expected) in [
            (&[0x8e][..], "\u{fffd}"),
            (&[0x8e, 0xa1][..], "\u{fffd}"),
            (&[0x8e, 0xa1, b'A'][..], "\u{fffd}"),
            (&[0x8e, 0xa2, b'A'][..], "\u{fffd}"),
            (&[0x8e, 0xa1, b'A', b'B'][..], "\u{fffd}AB"),
            (&[0x8e, 0xa2, 0xa1, b'A', b'B'][..], "\u{fffd}B"),
            (&[0xa0, b'A'][..], "\u{fffd}A"),
            (&[0xff, b'A'][..], "\u{fffd}A"),
            (&[0x8f, b'A'][..], "\u{fffd}A"),
            (&[0xa3, 0xcf, b'A'][..], "\u{fffd}A"),
        ] {
            assert_eq!(decode(converter.as_mut(), bytes).await, expected, "{name} {bytes:x?}");
        }
    }
    let mut standard = open("EUC-TW");
    assert_eq!(decode(standard.as_mut(), &[0xa1, 0x8e, 0xa2, 0xa1, 0xa1]).await, "\u{fffd}乂");
    let mut ibm = open("IBM964");
    assert_eq!(decode(ibm.as_mut(), &[0x8e, 0xa2, b'A', 0x8e, 0xa2, 0xa1, 0xa1]).await, "\u{fffd}乂");
}

#[tokio::test]
async fn euc_tw_record_boundaries_do_not_retain_partial_selectors_or_trails() {
    for name in ["EUC-TW", "IBM964"] {
        let mut converter = open(name);
        assert_eq!(decode(converter.as_mut(), &[0x8e, 0xa2, 0xa1]).await, "\u{fffd}");
        assert_eq!(converted(converter.as_mut(), None).await, Value::Null);
        assert_eq!(decode(converter.as_mut(), &[]).await, "");
        assert_eq!(decode(converter.as_mut(), &[0xa1]).await, "\u{fffd}");
        assert_eq!(decode(converter.as_mut(), &[0x8e, 0xa2, 0xa1, 0xa1]).await, "乂");
        let mut separated = decode(converter.as_mut(), &[0x8e, 0xa2, 0xa1]).await;
        separated.push_str(&decode(converter.as_mut(), &[0]).await);
        assert_eq!(separated, "\u{fffd}\0");
        assert_eq!(decode(converter.as_mut(), &[0x8e, 0xa2, 0xa1, 0]).await, "\u{fffd}");
    }
}

#[tokio::test]
async fn euc_tw_configuration_keeps_alias_punctuation_trimming_and_key_value_precedence() {
    let mut trimmed = open("\0\tCNS11643\r\u{1f}");
    assert_eq!(decode(trimmed.as_mut(), &[0x8e, 0xaf, 0xa1, 0xa1]).await, "\u{20002}");
    for invalid in ["euc.tw", "x_euc_tw", "ibm_euctw", " EUC-TW "] {
        assert!(PluginRegistry::default().converter(&spec(&[("value.deserializer.encoding", invalid)]), false).is_err(), "{invalid}");
    }
    let options = spec(&[
        ("converter.encoding", "EUC-TW"),
        ("key.serializer.encoding", "cp964"),
        ("key.deserializer.encoding", "cns11643"),
        ("value.serializer.encoding", "euc_tw"),
        ("value.deserializer.encoding", "ibm-euctw"),
    ]);
    let mut key = PluginRegistry::default().converter(&options, true).unwrap();
    let mut value = PluginRegistry::default().converter(&options, false).unwrap();
    assert_eq!(decode(key.as_mut(), &[0xa1, 0xa6]).await, "・");
    assert_eq!(decode(value.as_mut(), &[0xa1, 0xa6]).await, "‧");
    for (is_key, serializer) in [(true, "key.serializer.encoding"), (false, "value.serializer.encoding")] {
        assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", "EUC-TW"), (serializer, "euc.tw")]), is_key).is_err());
    }
}

#[tokio::test]
async fn euc_tw_supplementary_private_use_and_replacement_output_obey_scalar_memory_admission() {
    for (name, bytes, expected) in [
        ("EUC-TW", [0x8e, 0xaf, 0xa1, 0xa1].repeat(1024), "\u{20002}".repeat(1024)),
        ("IBM964", [0x8e, 0xac, 0xa1, 0xa1].repeat(1024), "\u{e000}".repeat(1024)),
        ("EUC-TW", vec![0x80; 1024], "\u{fffd}".repeat(1024)),
        ("EUC-TW", vec![0x8e, 0xa2, 0xa1], "\u{fffd}".to_owned()),
        ("IBM964", vec![0x8e, 0xa2, 0xa1], "\u{fffd}".to_owned()),
    ] {
        let mut converter = open(name);
        let mut rejected = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4 - 1));
        assert!(converter.decode("events", Some(&bytes), &mut rejected).await.is_err());
        let mut admitted = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4));
        assert_eq!(converter.decode("events", Some(&bytes), &mut admitted).await.unwrap().value, Value::Utf8(expected), "{name}");
    }
}
