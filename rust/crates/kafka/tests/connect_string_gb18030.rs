use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConversionContext, Converter, PluginRegistry, PluginSpec, SchemaKind};
use rtdb_types::Value;

const PROBES: &str = include_str!("../src/connect/converter/charset/double_byte/gb18030/probes.tsv");

fn spec(options: &[(&str, &str)]) -> PluginSpec {
    PluginSpec {
        class_name: "org.apache.kafka.connect.storage.StringConverter".into(),
        options: options.iter().map(|(key, value)| ((*key).into(), (*value).into())).collect(),
    }
}

fn open(name: &str) -> Box<dyn Converter> {
    PluginRegistry::default().converter(&spec(&[("converter.encoding", name)]), false).unwrap()
}

async fn decode(converter: &mut dyn Converter, bytes: &[u8]) -> String {
    let mut context = ConversionContext::new(MemoryBudget::new(1 << 20));
    let data = converter.decode("events", Some(bytes), &mut context).await.unwrap();
    let schema = data.schema.unwrap();
    assert!(schema.optional);
    assert!(matches!(schema.kind, SchemaKind::String));
    let Value::Utf8(text) = data.value else { panic!("expected string") };
    text
}

fn hex(bytes: &[u8]) -> String { bytes.iter().map(|byte| format!("{byte:02x}")).collect() }

fn expected(name: &str) -> (usize, &str) {
    let mut fields = PROBES.lines().skip(1).find(|line| line.starts_with(&format!("{name}\t"))).unwrap().split('\t');
    assert_eq!(fields.next(), Some(name));
    let count = fields.next().unwrap().parse().unwrap();
    let hash = fields.next().unwrap();
    assert!(fields.next().is_none());
    (count, hash)
}

async fn accumulate(converter: &mut dyn Converter, hash: &mut openssl::sha::Sha256, bytes: &[u8]) {
    hash.update(decode(converter, bytes).await.as_bytes());
    hash.update(&[0]);
}

#[test]
fn gb18030_resources_cover_the_complete_bmp_offsets_and_pair_rectangle() {
    let pairs = include_bytes!("../src/connect/converter/charset/double_byte/gb18030/pairs.bin");
    let bmp = include_bytes!("../src/connect/converter/charset/double_byte/gb18030/bmp.bin");
    assert_eq!(pairs.len(), 48_132);
    assert_eq!(bmp.len(), 78_840);
    assert_eq!(hex(&openssl::sha::sha256(pairs)), "17d8ca99b1a702c03da8204374bdc6c15df2222df836507ad0580875511178d9");
    assert_eq!(hex(&openssl::sha::sha256(bmp)), "92d029c9a66b5cddf30dd3cf5e3281178b58767b9490571dcdac49775ec03d18");
    for table in [pairs.as_slice(), bmp.as_slice()] {
        for bytes in table.chunks_exact(2) {
            assert!(!(0xd800..=0xdfff).contains(&u16::from_be_bytes([bytes[0], bytes[1]])));
        }
    }
    assert_eq!(u16::from_be_bytes([bmp[78_838], bmp[78_839]]), 0xffff);
    assert_eq!(PROBES.lines().count(), 6);
    assert_eq!(hex(&openssl::sha::sha256(PROBES.as_bytes())), "4c7bec99dfe13f444d743d1a737daa8493950faaad9debd14eb4f623a5aa2d77");
    assert_eq!(hex(&openssl::sha::sha256(include_bytes!("../src/connect/converter/charset/double_byte/gb18030/charsets.tsv"))), "1274ac16872b623147f5aa9bc7581aff1d65722f19934cd4642091c9dde9f7c1");
    assert_eq!(hex(&openssl::sha::sha256(include_bytes!("../src/connect/converter/charset/double_byte/gb18030/mappings.tsv"))), "9744aca2dbf2af59c37f2e5196d4dae5861b34b3bf0cb83ba18313407b1574c8");
    assert_eq!(hex(&openssl::sha::sha256(include_bytes!("../src/connect/converter/charset/double_byte/gb18030/sources.sha256"))), "f3b8b7fb7777641b82cac25a21de8e8492d0fe128abd6d4a169dd3debcbb0ef1");
}

#[tokio::test]
async fn gb18030_default_provider_names_select_the_2022_tables_and_all_single_bytes() {
    for name in ["GB18030", "gb18030", "gb18030-2022", "GB18030-2022"] {
        let mut converter = open(name);
        let mut digest = openssl::sha::Sha256::new();
        for byte in 0u8..=255 { accumulate(converter.as_mut(), &mut digest, &[byte]).await; }
        assert_eq!(expected("single"), (256, hex(&digest.finish()).as_str()));
        assert_eq!(decode(converter.as_mut(), b"\xa6\xd9\xa8\xbc\xfe\x59").await, "\u{fe10}\u{1e3f}\u{9fb4}");
        assert_eq!(decode(converter.as_mut(), b"\xd6\xd0\xb9\xfa").await, "中国");
    }
    for name in ["gb18030-2000", "GB18030-2000", "GB-18030", "gb18030-2005"] {
        assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", name)]), false).is_err(), "{name}");
    }
}

#[tokio::test]
async fn gb18030_complete_pair_space_matches_source_mapping_and_consumption() {
    let mut converter = open("GB18030");
    let mut digest = openssl::sha::Sha256::new();
    for first in 0u8..=255 {
        for second in 0u8..=255 { accumulate(converter.as_mut(), &mut digest, &[first, second]).await; }
    }
    assert_eq!(expected("pairs"), (65_536, hex(&digest.finish()).as_str()));
}

#[tokio::test]
async fn gb18030_every_structural_four_byte_sequence_matches_bmp_supplementary_and_gap_results() {
    let mut converter = open("GB18030");
    let mut digest = openssl::sha::Sha256::new();
    let mut count = 0;
    for first in 0x81..=0xfe {
        for second in 0x30..=0x39 {
            for third in 0x81..=0xfe {
                for fourth in 0x30..=0x39 {
                    accumulate(converter.as_mut(), &mut digest, &[first, second, third, fourth]).await;
                    count += 1;
                }
            }
        }
    }
    assert_eq!(expected("four-byte-space"), (count, hex(&digest.finish()).as_str()));
}

#[tokio::test]
async fn gb18030_complete_suffix_spaces_preserve_malformed_widths_and_terminal_prechecks() {
    let mut converter = open("GB18030");
    let mut digest = openssl::sha::Sha256::new();
    let mut count = 0;
    for first in [0x81, 0x84, 0x90, 0xe3, 0xfe] {
        for second in [0x30, 0x39] {
            for third in 0u8..=255 {
                for fourth in 0u8..=255 {
                    accumulate(converter.as_mut(), &mut digest, &[first, second, third, fourth]).await;
                    count += 1;
                }
            }
        }
    }
    assert_eq!(expected("four-byte-suffixes"), (count, hex(&digest.finish()).as_str()));
    let mut digest = openssl::sha::Sha256::new();
    let mut count = 0;
    for first in [0x81, 0xfe] {
        for second in 0u8..=255 {
            for third in 0u8..=255 {
                accumulate(converter.as_mut(), &mut digest, &[first, second, third]).await;
                count += 1;
            }
        }
    }
    assert_eq!(expected("three-byte-prefixes"), (count, hex(&digest.finish()).as_str()));
}

#[tokio::test]
async fn gb18030_bmp_transition_offsets_and_2022_relocations_retain_their_exact_values() {
    let mut converter = open("GB18030");
    for (bytes, scalar) in [
        ([0x81, 0x30, 0x81, 0x30], 0x0080),
        ([0x82, 0x35, 0x8f, 0x32], 0x4dff),
        ([0x82, 0x35, 0x8f, 0x33], 0x9fa6),
        ([0x82, 0x35, 0x90, 0x37], 0xe81e),
        ([0x82, 0x35, 0x91, 0x34], 0xe864),
        ([0x83, 0x36, 0xc7, 0x38], 0xd7ff),
        ([0x83, 0x36, 0xc7, 0x39], 0xe76c),
        ([0x83, 0x36, 0xcf, 0x39], 0xe863),
        ([0x83, 0x36, 0xd0, 0x30], 0xe865),
        ([0x84, 0x30, 0x81, 0x30], 0xf8ff),
        ([0x84, 0x30, 0x81, 0x31], 0xf900),
        ([0x84, 0x31, 0xa4, 0x39], 0xffff),
        ([0x84, 0x31, 0xa5, 0x30], 0xfffd),
        ([0x8f, 0x39, 0xfe, 0x39], 0xfffd),
        ([0x90, 0x30, 0x81, 0x30], 0x10000),
        ([0xe3, 0x32, 0x9a, 0x35], 0x10ffff),
        ([0xe3, 0x32, 0x9a, 0x36], 0xfffd),
        ([0xfe, 0x39, 0xfe, 0x39], 0xfffd),
    ] {
        assert_eq!(decode(converter.as_mut(), &bytes).await, char::from_u32(scalar).unwrap().to_string(), "{bytes:x?}");
    }
}

#[tokio::test]
async fn gb18030_error_lengths_reprocess_only_the_source_suffix() {
    let mut converter = open("GB18030");
    for (bytes, expected) in [
        (&b"\x80A\xff"[..], "\u{fffd}A\u{fffd}"),
        (&b"\x81\x2fA"[..], "\u{fffd}/A"),
        (&b"\x81\x3aA"[..], "\u{fffd}A"),
        (&b"\x81\x7fA"[..], "\u{fffd}A"),
        (&b"\x81\xffA"[..], "\u{fffd}A"),
        (&b"\x810A"[..], "\u{fffd}"),
        (&b"\x810AB"[..], "\u{fffd}B"),
        (&b"\x810\x81A!"[..], "\u{fffd}!"),
        (&b"\x810\xff\x81A"[..], "\u{fffd}丄"),
        (&b"A\x81"[..], "A\u{fffd}"),
        (&b"A\x810"[..], "A\u{fffd}"),
        (&b"A\x810\x81"[..], "A\u{fffd}"),
    ] {
        assert_eq!(decode(converter.as_mut(), bytes).await, expected, "{bytes:x?}");
    }
}

#[tokio::test]
async fn gb18030_partial_records_reset_across_null_and_empty_input() {
    let mut converter = open("gb18030-2022");
    for partial in [&b"\x81"[..], &b"\x810"[..], &b"\x810\x81"[..]] {
        assert_eq!(decode(converter.as_mut(), partial).await, "\u{fffd}");
        let mut context = ConversionContext::new(MemoryBudget::new(1 << 20));
        assert_eq!(converter.decode("events", None, &mut context).await.unwrap().value, Value::Null);
        assert_eq!(decode(converter.as_mut(), b"").await, "");
        assert_eq!(decode(converter.as_mut(), b"0A").await, "0A");
        assert_eq!(decode(converter.as_mut(), b"\x900\x810").await, "\u{10000}");
    }
}

#[tokio::test]
async fn gb18030_configuration_preserves_trimming_and_serializer_validation() {
    let mut converter = open("\0\tgb18030-2022\r\u{1f}");
    assert_eq!(decode(converter.as_mut(), b"\xd6\xd0").await, "中");
    for is_key in [false, true] {
        let (serializer, deserializer) = if is_key { ("key.serializer.encoding", "key.deserializer.encoding") } else { ("value.serializer.encoding", "value.deserializer.encoding") };
        let mut converter = PluginRegistry::default().converter(&spec(&[("converter.encoding", "UTF-8"), (serializer, "GB18030"), (deserializer, "gb18030-2022")]), is_key).unwrap();
        assert_eq!(decode(converter.as_mut(), b"\xd6\xd0").await, "中");
        for invalid in ["gb18030-2000", " GB18030 "] {
            assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", "GB18030"), (serializer, invalid)]), is_key).is_err());
            assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", "GB18030"), (deserializer, invalid)]), is_key).is_err());
        }
    }
}

#[tokio::test]
async fn gb18030_supplementary_and_replacement_outputs_require_scalar_admission_before_decoding() {
    let mut converter = open("GB18030");
    for (bytes, expected) in [
        (b"\x900\x810".repeat(1024), "\u{10000}".repeat(1024)),
        (vec![0xff; 1024], "\u{fffd}".repeat(1024)),
        (b"\xd6\xd0".repeat(1024), "中".repeat(1024)),
        (b"\x810".to_vec(), "\u{fffd}".to_owned()),
    ] {
        let mut rejected = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4 - 1));
        assert!(converter.decode("events", Some(&bytes), &mut rejected).await.is_err());
        let mut admitted = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4));
        assert_eq!(converter.decode("events", Some(&bytes), &mut admitted).await.unwrap().value, Value::Utf8(expected));
    }
}
