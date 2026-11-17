use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConversionContext, Converter, PluginRegistry, PluginSpec, SchemaKind};
use rtdb_types::Value;

const MANIFEST: &str = include_str!("../src/connect/converter/charset/double_byte/iso2022cnkr/charsets.tsv");

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

const MODES: &str = include_str!("../src/connect/converter/charset/double_byte/iso2022cnkr/modes.tsv");

fn prefix(hex: &str) -> Vec<u8> {
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

#[test]
fn iso2022cnkr_resources_pin_provider_sources_and_filter_only_supplementary_cns_entries() {
    const MAPPINGS: &str = include_str!("../src/connect/converter/charset/double_byte/iso2022cnkr/mappings.tsv");
    assert_eq!(sha256_hex(MANIFEST.as_bytes()), "44d37de2d3758770d179445ae875973f50a99e07e7069aafec5a0c1d3bac2c4f");
    assert_eq!(sha256_hex(MODES.as_bytes()), "a8102441eb900476fac275ce7b400575f64e1b3a1c67053cb9dcc396b21c9701");
    assert_eq!(sha256_hex(MAPPINGS.as_bytes()), "4ad22c69d94208b89b0c5f2cc154ef70c78044cdb46b64ccceef3dc6f7cdc5d5");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/iso2022cnkr/aliases.rs")), "9ce1c9d5aa5414398156eec6906fed035365126289920e8dc33f24681b02a896");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/iso2022cnkr/tables.rs")), "14538c31ecf34ba1f4fafa4a3324493e1bfd8cb085e1541a76e72cf0a2f4b1f8");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/iso2022cnkr/sources.sha256")), "a70c933fb53eaf7b697755868ef61dd37be6d9b3b41370d39983c9bff7471a3a");
    let mappings: [&[u8]; 5] = [
        include_bytes!("../src/connect/converter/charset/double_byte/iso2022cnkr/gb2312.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/iso2022cnkr/euc-kr.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/iso2022cnkr/cns1.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/iso2022cnkr/cns2.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/iso2022cnkr/cns3.bin"),
    ];
    assert_eq!(MAPPINGS.lines().count(), 6);
    assert_eq!(mappings.iter().map(|mapping| mapping.len()).sum::<usize>(), 87_028);
    assert_eq!(mappings[0], include_bytes!("../src/connect/converter/charset/double_byte/gb2312.bin"));
    assert_eq!(mappings[1], include_bytes!("../src/connect/converter/charset/double_byte/euc-kr.bin"));
    let original_planes = include_bytes!("../src/connect/converter/charset/double_byte/euc_tw/euc-tw-planes.bin");
    let supplementary_flags = include_bytes!("../src/connect/converter/charset/double_byte/euc_tw/euc-tw-supp.bin");
    for (index, (line, mapping)) in MAPPINGS.lines().skip(1).zip(mappings).enumerate() {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 8);
        assert_eq!(mapping.len(), fields[3].parse::<usize>().unwrap());
        assert_eq!(sha256_hex(mapping), fields[4]);
        let unit = |offset: usize| u16::from_be_bytes([mapping[offset], mapping[offset + 1]]);
        if index < 2 {
            assert_eq!(&mapping[..2], &[0xa1, 0xfe]);
            assert_eq!(fields[5], "double-byte");
            let rows: usize = fields[6].parse().unwrap();
            assert_eq!(mapping.len(), 1026 + rows * 94 * 2);
            let mut present = 0;
            for byte in 0u8..=255 {
                assert_eq!(unit(2 + usize::from(byte) * 2), if byte < 128 { u16::from(byte) } else { 0xfffd });
                let row = unit(514 + usize::from(byte) * 2);
                if row != 0xffff {
                    assert_eq!(usize::from(row), present);
                    present += 1;
                }
            }
            assert_eq!(present, rows);
            for offset in (1026..mapping.len()).step_by(2) { assert!(!(0xd800..=0xdfff).contains(&unit(offset))); }
        } else {
            let plane = index - 2;
            assert_eq!(fields[5], "cns-plane");
            assert_eq!(mapping.len(), 17_672);
            let mut mapped = 0;
            let mut replaced = 0;
            for position in 0..8836 {
                let offset = (plane * 8836 + position) * 2;
                let original = u16::from_be_bytes([original_planes[offset], original_planes[offset + 1]]);
                let supplementary = supplementary_flags[position] & (1 << plane) != 0;
                assert_eq!(unit(position * 2), if supplementary { 0xfffd } else { original });
                assert!(!(0xd800..=0xdfff).contains(&unit(position * 2)));
                mapped += usize::from(unit(position * 2) != 0xfffd);
                replaced += usize::from(supplementary && original != 0xfffd);
            }
            assert_eq!(mapped, fields[6].parse::<usize>().unwrap());
            assert_eq!(replaced, fields[7].parse::<usize>().unwrap());
            assert_eq!(replaced, [0, 0, 71][plane]);
        }
    }
}

#[tokio::test]
async fn every_iso2022cnkr_name_selects_its_source_decoder_case_insensitively() {
    let mut names = std::collections::BTreeSet::new();
    assert_eq!(MANIFEST.lines().count(), 5);
    assert_eq!(MODES.lines().count(), 7);
    for line in MANIFEST.lines().skip(1) {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 6);
        for name in std::iter::once(fields[0]).chain(fields[5].split('|')) {
            names.insert(name.to_ascii_lowercase());
            for selected in [name.to_owned(), name.to_ascii_uppercase()] {
                let mut converter = open(&selected);
                for mode in MODES.lines().skip(1).map(|line| line.split('\t').collect::<Vec<_>>()).filter(|mode| mode[0] == fields[4]) {
                    assert_eq!(mode.len(), 10);
                    let mut record = prefix(mode[2]);
                    record.push(0);
                    let mut probe = String::new();
                    for byte in 0u8..=255 {
                        *record.last_mut().unwrap() = byte;
                        probe.push_str(&decode(converter.as_mut(), &record).await);
                    }
                    assert_eq!(sha256_hex(probe.as_bytes()), mode[3], "{selected} {}", mode[1]);
                }
                assert_eq!(decode(converter.as_mut(), b"\x0e0!\x0f").await, if fields[4] == "Cn" { "啊" } else { "가" }, "{selected}");
                if fields[4] == "Cn" {
                    assert_eq!(decode(converter.as_mut(), b"\x1b$)G\x0e!\"\x0f\x1bN!!\x1bO!!").await, "，乂丨", "{selected}");
                }
                assert_eq!(decode(converter.as_mut(), &[]).await, "");
                assert_eq!(converted(converter.as_mut(), None).await, Value::Null);
            }
        }
    }
    assert_eq!(names.len(), 12);
}

#[tokio::test]
async fn iso2022cnkr_canonical_decoders_cover_all_pairs_escape_pairs_designations_and_single_shift_payloads() {
    for line in MANIFEST.lines().skip(1) {
        let fields: Vec<_> = line.split('\t').collect();
        let mut converter = open(fields[0]);
        for mode in MODES.lines().skip(1).map(|line| line.split('\t').collect::<Vec<_>>()).filter(|mode| mode[0] == fields[4]) {
            for (escape, expected) in [(&b""[..], mode[4]), (&b"\x1b"[..], mode[5]), (&b"\x1b$"[..], mode[6]), (&b"\x1bN"[..], mode[7]), (&b"\x1bO"[..], mode[8])] {
                let mut record = prefix(mode[2]);
                record.extend_from_slice(escape);
                let suffix = record.len();
                record.extend_from_slice(&[0, 0]);
                let mut probe = String::new();
                for first in 0u8..=255 {
                    record[suffix] = first;
                    for second in 0u8..=255 {
                        record[suffix + 1] = second;
                        probe.push_str(&decode(converter.as_mut(), &record).await);
                        probe.push('\0');
                    }
                }
                assert_eq!(sha256_hex(probe.as_bytes()), expected, "{} {} {escape:x?}", fields[0], mode[1]);
            }
            let mut record = prefix(mode[2]);
            let suffix = record.len();
            record.extend_from_slice(&[0x1b, 0, 0, 0]);
            let mut probe = String::new();
            for selector in 0u8..=255 {
                record[suffix + 1] = selector;
                for pair in [*b"!!", *b")C", *b"\0\xff", *b"\x0e\x0f", *b"\x1b\x1b"] {
                    record[suffix + 2..].copy_from_slice(&pair);
                    probe.push_str(&decode(converter.as_mut(), &record).await);
                    probe.push('\0');
                }
            }
            assert_eq!(sha256_hex(probe.as_bytes()), mode[9], "{} {}", fields[0], mode[1]);
        }
    }
}

#[tokio::test]
async fn iso2022cnkr_unshifted_high_bytes_and_shifted_trail_controls_follow_java_byte_interpretation() {
    for name in ["ISO-2022-CN", "x-ISO-2022-CN-CNS", "x-ISO-2022-CN-GB", "ISO-2022-KR"] {
        let mut converter = open(name);
        for (bytes, expected) in [
            (&b"A\0\t\x80\xa1\xff"[..], "A\0\t\u{80}¡ÿ"),
            (&b"\x0e!!\x0fA"[..], "\u{3000}A"),
            (&b"\x0e\xa1\xa1\x0fA"[..], "\u{3000}A"),
            (&b"\x0e!\xa1\xa1!\x0fA"[..], "\u{3000}\u{3000}A"),
            (&b"\x0e!\x1b!!\x0fA"[..], "\u{fffd}\u{3000}A"),
            (&b"\x0e!\x0f!!\x0fA"[..], "\u{fffd}\u{3000}A"),
            (&b"\x0e!\x0e!!\x0fA"[..], "\u{fffd}\u{3000}A"),
            (&b"\x0e\0!!!\x0fA"[..], "\u{fffd}\u{3000}A"),
            (&b"\x0e!\xff!!\x0fA"[..], "\u{fffd}\u{3000}A"),
            (&b"\x0e\x0e!!\x0f\x0fA"[..], "\u{3000}A"),
            (&b"\x0e\x9b!!!\x0fA"[..], "\u{fffd}\u{3000}A"),
        ] {
            assert_eq!(decode(converter.as_mut(), bytes).await, expected, "{name} {bytes:x?}");
        }
    }
}

#[tokio::test]
async fn iso2022cn_all_three_names_accept_gb_cns_and_undeclared_single_shifts_with_bmp_only_output() {
    for name in ["ISO-2022-CN", "x-ISO-2022-CN-CNS", "x-ISO-2022-CN-GB"] {
        let mut converter = open(name);
        for (bytes, expected) in [
            (&b"\x1b$)A\x0eVP9z\x0f!"[..], "中国!"),
            (&b"\x1b$A\x0eVP9z\x0f!"[..], "中国!"),
            (&b"\x0eVP9z\x0f!"[..], "中国!"),
            (&b"\x1b$)G\x0e!\"\x0f!\"\x0e!\""[..], "，!\"，"),
            (&b"\x1b$)G\x0e\x0e!\"\x0f\n\x0e!\""[..], "，\n，"),
            (&b"\x1bN!!\x1bO!!"[..], "乂丨"),
            (&b"\x1bN\xa1!\x1bO!\xa1"[..], "乂丨"),
            (&b"\x1b$*H\x1bN!!\x1b$+I\x1bO!!"[..], "乂丨"),
            (&b"\x1bO!D"[..], "\u{fffd}"),
            (&b"\x1bO\xa1\xc4"[..], "\u{fffd}"),
            (&b"\x1bN!\x0fA"[..], "\u{fffd}A"),
            (&b"\x1bO\0!A"[..], "\u{fffd}A"),
            (&b"\x1b$)G\x0e0!\x0f!"[..], "\u{fffd}!"),
        ] {
            assert_eq!(decode(converter.as_mut(), bytes).await, expected, "{name} {bytes:x?}");
        }
    }
}

#[tokio::test]
async fn iso2022cn_every_escape_resets_the_shifted_designation_even_when_it_replaces_or_single_shifts() {
    for name in ["ISO-2022-CN", "x-ISO-2022-CN-CNS", "x-ISO-2022-CN-GB"] {
        let mut converter = open(name);
        for (escape, expected) in [
            (&b"\x1b$A"[..], "，、"),
            (&b"\x1b$)A"[..], "，、"),
            (&b"\x1b$*H"[..], "，、"),
            (&b"\x1b$+I"[..], "，、"),
            (&b"\x1bX"[..], "，\u{fffd}、"),
            (&b"\x1b\xff"[..], "，\u{fffd}、"),
            (&b"\x1b$X"[..], "，\u{fffd}、"),
            (&b"\x1b$\xff"[..], "，\u{fffd}、"),
            (&b"\x1b$)X"[..], "，\u{fffd}、"),
            (&b"\x1b$)\xff"[..], "，\u{fffd}、"),
            (&b"\x1bN!!"[..], "，乂、"),
            (&b"\x1bO!!"[..], "，丨、"),
            (&b"\x1bO!D"[..], "，\u{fffd}、"),
        ] {
            let mut record = b"\x1b$)G\x0e!\"".to_vec();
            record.extend_from_slice(escape);
            record.extend_from_slice(b"!\"");
            assert_eq!(decode(converter.as_mut(), &record).await, expected, "{name} {escape:x?}");
        }
        assert_eq!(decode(converter.as_mut(), b"\x1b$)G\x1bN!!\x0e!\"").await, "乂、");
    }
}

#[tokio::test]
async fn iso2022kr_designations_preserve_shift_state_and_ss2_ss3_always_emit_replacement() {
    let mut converter = open("ISO-2022-KR");
    for (bytes, expected) in [
        (&b"\x1b$)C0!"[..], "0!"),
        (&b"\x0e0!\x1b$)C0!\x0f0!"[..], "가가0!"),
        (&b"\x0e0!\x1bX0!\x0f!"[..], "가\u{fffd}가!"),
        (&b"\x0e0!\x1bN0!0!\x0f!"[..], "가\u{fffd}가!"),
        (&b"\x0e0!\x1bO0!0!\x0f!"[..], "가\u{fffd}가!"),
        (&b"\x1bN\x0e\x0f0!"[..], "\u{fffd}0!"),
        (&b"\x1bO\0\xff0!"[..], "\u{fffd}0!"),
        (&b"\x1bN\x1b\x1b0!"[..], "\u{fffd}0!"),
        (&b"\x1b$)GAB"[..], "\u{fffd})GAB"),
        (&b"\x1b$AB"[..], "\u{fffd}AB"),
        (&b"\x0e\xb0\xa1\x0f!"[..], "가!"),
    ] {
        assert_eq!(decode(converter.as_mut(), bytes).await, expected, "{bytes:x?}");
    }
}

#[tokio::test]
async fn iso2022cnkr_malformed_lengths_and_terminal_truncations_keep_their_distinct_prechecks() {
    for name in ["ISO-2022-CN", "x-ISO-2022-CN-CNS", "x-ISO-2022-CN-GB", "ISO-2022-KR"] {
        let mut converter = open(name);
        let kr = name == "ISO-2022-KR";
        for bytes in [&b"\x1b"[..], &b"\x1b$"[..], &b"\x1b$)"[..], &b"\x1bN!"[..], &b"\x1bO!"[..], &b"\x0e!"[..]] {
            assert_eq!(decode(converter.as_mut(), bytes).await, "\u{fffd}", "{name} {bytes:x?}");
        }
        assert_eq!(decode(converter.as_mut(), b"\x1bXA").await, if kr { "\u{fffd}" } else { "\u{fffd}A" }, "{name}");
        assert_eq!(decode(converter.as_mut(), b"\x1bXAB").await, "\u{fffd}AB", "{name}");
        assert_eq!(decode(converter.as_mut(), b"\x1b\xffA").await, if kr { "\u{fffd}" } else { "\u{fffd}A" }, "{name}");
        assert_eq!(decode(converter.as_mut(), b"\x1b$A").await, if kr { "\u{fffd}" } else { "" }, "{name}");
        assert_eq!(decode(converter.as_mut(), b"\x1b$AB").await, if kr { "\u{fffd}AB" } else { "B" }, "{name}");
        assert_eq!(decode(converter.as_mut(), b"\x1b$XAB").await, if kr { "\u{fffd}XAB" } else { "\u{fffd}AB" }, "{name}");
        assert_eq!(decode(converter.as_mut(), b"\x1b$)XAB").await, if kr { "\u{fffd})XAB" } else { "\u{fffd}AB" }, "{name}");
        assert_eq!(decode(converter.as_mut(), b"\x0e!!\x1bN!").await, "\u{3000}\u{fffd}", "{name}");
        assert_eq!(decode(converter.as_mut(), b"\x0e\x0f").await, "", "{name}");
    }
}

#[tokio::test]
async fn iso2022cnkr_records_reset_shifting_designation_and_partial_escapes_across_null_and_empty_values() {
    for name in ["ISO-2022-CN", "x-ISO-2022-CN-CNS", "x-ISO-2022-CN-GB", "ISO-2022-KR"] {
        let mut converter = open(name);
        for bytes in [&b"\x0e"[..], &b"\x1b$)G\x0e"[..], &b"\x1b$)C\x0e"[..], &b"\x1bN!"[..], &b"\x0e!"[..]] {
            decode(converter.as_mut(), bytes).await;
            assert_eq!(converted(converter.as_mut(), None).await, Value::Null);
            assert_eq!(decode(converter.as_mut(), &[]).await, "");
            assert_eq!(decode(converter.as_mut(), b"!\"\xff").await, "!\"ÿ", "{name}");
            assert_eq!(decode(converter.as_mut(), b"\x0e!\"").await, "、", "{name}");
        }
        let mut separated = decode(converter.as_mut(), b"\x0e!").await;
        separated.push_str(&decode(converter.as_mut(), &[0]).await);
        assert_eq!(separated, "\u{fffd}\0");
        assert_eq!(decode(converter.as_mut(), b"\x0e!\0").await, "\u{fffd}");
    }
}

#[tokio::test]
async fn iso2022cnkr_configuration_preserves_provider_spelling_trimming_and_key_value_precedence() {
    let mut trimmed = open("\0\tISO2022CN_CNS\r\u{1f}");
    assert_eq!(decode(trimmed.as_mut(), b"\x1b$)G\x0e!\"").await, "，");
    for invalid in ["ISO2022_CN", "x-ISO2022CN", "ISO2022_CN_GB", " ISO-2022-KR "] {
        assert!(PluginRegistry::default().converter(&spec(&[("value.deserializer.encoding", invalid)]), false).is_err(), "{invalid}");
    }
    let options = spec(&[
        ("converter.encoding", "csISO2022CN"),
        ("key.serializer.encoding", "ISO-2022-CN-CNS"),
        ("key.deserializer.encoding", "ISO2022CN_GB"),
        ("value.serializer.encoding", "x-ISO-2022-CN-GB"),
        ("value.deserializer.encoding", "csISO2022KR"),
    ]);
    let mut key = PluginRegistry::default().converter(&options, true).unwrap();
    let mut value = PluginRegistry::default().converter(&options, false).unwrap();
    assert_eq!(decode(key.as_mut(), b"\x0e0!").await, "啊");
    assert_eq!(decode(value.as_mut(), b"\x0e0!").await, "가");
    for (is_key, serializer) in [(true, "key.serializer.encoding"), (false, "value.serializer.encoding")] {
        assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", "ISO-2022-CN"), (serializer, "ISO2022_CN")]), is_key).is_err());
    }
}

#[tokio::test]
async fn iso2022cnkr_mappings_replacements_and_output_free_designations_obey_scalar_memory_admission() {
    for name in ["ISO-2022-CN", "x-ISO-2022-CN-CNS", "x-ISO-2022-CN-GB", "ISO-2022-KR"] {
        let mut converter = open(name);
        let kr = name == "ISO-2022-KR";
        for (bytes, expected) in [
            (b"\x0e!!".repeat(1024), "\u{3000}".repeat(1024)),
            (vec![0xff; 1024], "ÿ".repeat(1024)),
            (b"\x0e!\0".repeat(1024), "\u{fffd}".repeat(1024)),
            (if kr { b"\x1b$)C".repeat(1024) } else { b"\x1b$)G".repeat(1024) }, String::new()),
            (b"\x1bN!".to_vec(), "\u{fffd}".to_owned()),
        ] {
            let mut rejected = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4 - 1));
            assert!(converter.decode("events", Some(&bytes), &mut rejected).await.is_err());
            let mut admitted = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4));
            assert_eq!(converter.decode("events", Some(&bytes), &mut admitted).await.unwrap().value, Value::Utf8(expected), "{name}");
        }
    }
}
