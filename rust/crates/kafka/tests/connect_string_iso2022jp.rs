use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConversionContext, Converter, PluginRegistry, PluginSpec, SchemaKind};
use rtdb_types::Value;

const MANIFEST: &str = include_str!("../src/connect/converter/charset/double_byte/iso2022jp/charsets.tsv");

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

const MODES: &str = include_str!("../src/connect/converter/charset/double_byte/iso2022jp/modes.tsv");

fn prefix(hex: &str) -> Vec<u8> {
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

#[test]
fn iso2022jp_resources_pin_provider_sources_and_five_independent_mapping_tables() {
    const MAPPINGS: &str = include_str!("../src/connect/converter/charset/double_byte/iso2022jp/mappings.tsv");
    assert_eq!(sha256_hex(MANIFEST.as_bytes()), "4954d93d8339ca19da2848e7c3cabe79e36eb5254def99c9dee7124c27ce2fae");
    assert_eq!(sha256_hex(MODES.as_bytes()), "ca06431e0a71b6d4f15d8e15abe8eb5238b17da13c3530e2f0cb9d1d431ff9ce");
    assert_eq!(sha256_hex(MAPPINGS.as_bytes()), "fdd9fe84e0aa40c589e820601ad534fb6b701a1cd527c3e1eb5ca5c14fde2da3");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/iso2022jp/aliases.rs")), "66dcdf5289b44f7d80adcb83c5a652eba5d209aca10e2e98a4cc7879c8f0066d");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/iso2022jp/tables.rs")), "b643c13c25e4d58c03c98e14ff452f950df444ef29adfe9876a6726830c97a57");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/iso2022jp/sources.sha256")), "f2337c81d82aee16df909872c6a87d968cd4e8130d529157806e94b3bac6a01b");
    let mappings: [&[u8]; 5] = [
        include_bytes!("../src/connect/converter/charset/double_byte/iso2022jp/jis_x_0208.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/iso2022jp/jis_x_0212.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/iso2022jp/jis_x_0208_ms5022x.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/iso2022jp/jis_x_0212_ms5022x.bin"),
        include_bytes!("../src/connect/converter/charset/double_byte/iso2022jp/jis_x_0208_ms932.bin"),
    ];
    assert_eq!(MAPPINGS.lines().count(), 6);
    assert_eq!(mappings.iter().map(|mapping| mapping.len()).sum::<usize>(), 79_390);
    assert_eq!(mappings[0], include_bytes!("../src/connect/converter/charset/double_byte/subclasses/x-jis0208.bin"));
    assert_eq!(mappings[1], include_bytes!("../src/connect/converter/charset/double_byte/subclasses/jis_x0212-1990.bin"));
    for (line, mapping) in MAPPINGS.lines().skip(1).zip(mappings) {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 7);
        assert_eq!(mapping.len(), fields[4].parse::<usize>().unwrap());
        assert_eq!(sha256_hex(mapping), fields[5]);
        let unit = |offset: usize| u16::from_be_bytes([mapping[offset], mapping[offset + 1]]);
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
async fn every_iso2022jp_name_selects_its_source_modes_and_mapping_variant_case_insensitively() {
    let mut names = std::collections::BTreeSet::new();
    assert_eq!(MANIFEST.lines().count(), 6);
    assert_eq!(MODES.lines().count(), 29);
    for line in MANIFEST.lines().skip(1) {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 6);
        for name in std::iter::once(fields[0]).chain(fields[5].split('|')) {
            names.insert(name.to_ascii_lowercase());
            for selected in [name.to_owned(), name.to_ascii_uppercase()] {
                let mut converter = open(&selected);
                for mode in MODES.lines().skip(1).map(|line| line.split('\t').collect::<Vec<_>>()).filter(|mode| mode[0] == fields[4]) {
                    assert_eq!(mode.len(), 7);
                    let mut record = prefix(mode[2]);
                    record.push(0);
                    let mut probe = String::new();
                    for byte in 0u8..=255 {
                        *record.last_mut().unwrap() = byte;
                        probe.push_str(&decode(converter.as_mut(), &record).await);
                    }
                    assert_eq!(sha256_hex(probe.as_bytes()), mode[3], "{selected} {}", mode[1]);
                }
                let expected = match fields[4] {
                    "Standard" | "Jp2" => "—〜‖\u{fffd}\u{fffd}",
                    "Ms5022x" => "―〜‖〞\u{e000}",
                    "MsIso" => "―～∥〝\u{fffd}",
                    variant => panic!("unknown variant {variant}"),
                };
                assert_eq!(decode(converter.as_mut(), b"\x1b$B!=!A!B-`u!").await, expected, "{selected}");
                assert_eq!(decode(converter.as_mut(), &[]).await, "");
                assert_eq!(converted(converter.as_mut(), None).await, Value::Null);
            }
        }
    }
    assert_eq!(names.len(), 17);
}

#[tokio::test]
async fn iso2022jp_canonical_decoders_cover_all_pairs_escape_pairs_and_jis0212_final_bytes_in_seven_modes() {
    for line in MANIFEST.lines().skip(1) {
        let fields: Vec<_> = line.split('\t').collect();
        let mut converter = open(fields[0]);
        for mode in MODES.lines().skip(1).map(|line| line.split('\t').collect::<Vec<_>>()).filter(|mode| mode[0] == fields[4]) {
            for (escape, expected) in [(false, mode[4]), (true, mode[5])] {
                let mut record = prefix(mode[2]);
                if escape { record.push(0x1b); }
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
                assert_eq!(sha256_hex(probe.as_bytes()), expected, "{} {} escape={escape}", fields[0], mode[1]);
            }
            let mut record = prefix(mode[2]);
            record.extend_from_slice(b"\x1b$(\0");
            let mut probe = String::new();
            for final_byte in 0u8..=255 {
                *record.last_mut().unwrap() = final_byte;
                probe.push_str(&decode(converter.as_mut(), &record).await);
                probe.push('\0');
            }
            assert_eq!(sha256_hex(probe.as_bytes()), mode[6], "{} {}", fields[0], mode[1]);
        }
    }
}

#[tokio::test]
async fn iso2022jp_designations_shifts_and_replacements_preserve_current_and_previous_state() {
    for name in ["ISO-2022-JP", "ISO-2022-JP-2", "x-windows-50220", "x-windows-50221", "x-windows-iso2022jp"] {
        let mut converter = open(name);
        for (bytes, expected) in [
            (&b"\\~\0\t"[..], "\\~\0\t"),
            (&b"\x1b(J\\~\x1b(B\\~"[..], "¥‾\\~"),
            (&b"\x1b(J\\~\x0f\\~"[..], "¥‾\\~"),
            (&b"\x1b$B$\"$$F|K\\\x1b(B!"[..], "あい日本!"),
            (&b"\x1b$@$\""[..], "あ"),
            (&b"\x1b(I!6^_\x1b(B!"[..], "｡ｶﾞﾟ!"),
            (&b"\x1b(I\0\n_`\x7f"[..], "\u{ff40}\u{ff4a}ﾟ\u{fffd}\u{fffd}"),
            (&b"\x1b$B\x0e!\x0f$\""[..], "｡あ"),
            (&b"\x1b$B\x0e\x0e\x0f$\""[..], "､｢"),
            (&b"\x1b$B\x0e\x1b(B\x0f$\""[..], "あ"),
            (&b"\x1b(J\x0e\xff!\x0f\\~"[..], "\u{fffd}｡¥‾"),
            (&b"\x1b$B\xff$\""[..], "\u{fffd}あ"),
            (&b"\x1b$B!\x1b$\""[..], "\u{fffd}あ"),
            (&b"\x1b$B!\x0e$\""[..], "\u{fffd}あ"),
            (&b"\x1b$B!\x0f$\""[..], "\u{fffd}あ"),
            (&b"\x1b$B!\xff$\""[..], "\u{fffd}あ"),
            (&b"\x1b$B\x1b(X$\""[..], "\u{fffd}あ"),
            (&b"\x1b$B\x1bX$\""[..], "\u{fffd}あ"),
        ] {
            assert_eq!(decode(converter.as_mut(), bytes).await, expected, "{name} {bytes:x?}");
        }
    }
}

#[tokio::test]
async fn iso2022jp_escape_errors_and_terminal_truncations_consume_java_error_lengths() {
    for name in ["ISO-2022-JP", "ISO-2022-JP-2", "x-windows-50220", "x-windows-50221", "x-windows-iso2022jp"] {
        let mut converter = open(name);
        for (bytes, expected) in [
            (&b"\x1b"[..], "\u{fffd}"),
            (&b"\x1b("[..], "\u{fffd}"),
            (&b"\x1b$"[..], "\u{fffd}"),
            (&b"\x1bX"[..], "\u{fffd}"),
            (&b"\x1b$("[..], "\u{fffd}"),
            (&b"\x1bXA"[..], "\u{fffd}A"),
            (&b"\x1b\xffA"[..], "\u{fffd}A"),
            (&b"\x1b(XA"[..], "\u{fffd}A"),
            (&b"\x1b$XA"[..], "\u{fffd}A"),
            (&b"\x1b(\xffA"[..], "\u{fffd}A"),
            (&b"\x1b$B$"[..], "\u{fffd}"),
            (&b"\x1b$B$\"\x1b("[..], "あ\u{fffd}"),
            (&b"\x1b$B"[..], ""),
            (&b"\x1b(J"[..], ""),
            (&b"\x0e"[..], ""),
        ] {
            assert_eq!(decode(converter.as_mut(), bytes).await, expected, "{name} {bytes:x?}");
        }
        let supports0212 = matches!(name, "ISO-2022-JP-2" | "x-windows-50220" | "x-windows-50221");
        assert_eq!(decode(converter.as_mut(), b"\x1b$(D").await, if supports0212 { "" } else { "\u{fffd}D" }, "{name}");
        assert_eq!(decode(converter.as_mut(), b"\x1b$(XA").await, if supports0212 { "\u{fffd}A" } else { "\u{fffd}XA" }, "{name}");
        assert_eq!(decode(converter.as_mut(), b"\x1b$(D\"7\x1b(B!").await, if supports0212 { "～!" } else { "\u{fffd}D\"7!" }, "{name}");
    }
}

#[tokio::test]
async fn iso2022jp_windows_variants_preserve_distinct_jis_and_private_use_mappings() {
    for name in ["x-windows-50220", "x-windows-50221"] {
        let mut converter = open(name);
        assert_eq!(decode(converter.as_mut(), b"\x1b$Bu!y!\x1b$(Du!").await, "\u{e000}\u{e178}\u{e3ac}", "{name}");
    }
    let mut msiso = open("windows-iso2022jp");
    assert_eq!(decode(msiso.as_mut(), b"\x1b$By!").await, "纊");
    let mut standard = open("ISO-2022-JP");
    assert_eq!(decode(standard.as_mut(), b"\x1b$Bu!y!").await, "\u{fffd}\u{fffd}");
}

#[tokio::test]
async fn iso2022jp_records_start_in_ascii_after_designations_shifts_truncation_null_and_empty_input() {
    for name in ["ISO-2022-JP", "ISO-2022-JP-2", "x-windows-50220", "x-windows-50221", "x-windows-iso2022jp"] {
        let mut converter = open(name);
        for bytes in [&b"\x1b$B"[..], &b"\x1b(J"[..], &b"\x0e"[..], &b"\x1b$("[..], &b"\x1b$B$"[..]] {
            decode(converter.as_mut(), bytes).await;
            assert_eq!(converted(converter.as_mut(), None).await, Value::Null);
            assert_eq!(decode(converter.as_mut(), &[]).await, "");
            assert_eq!(decode(converter.as_mut(), b"$\"\\~\x0f!").await, "$\"\\~!", "{name}");
        }
        let mut separated = decode(converter.as_mut(), b"\x1b$B$").await;
        separated.push_str(&decode(converter.as_mut(), &[0]).await);
        assert_eq!(separated, "\u{fffd}\0");
        assert_eq!(decode(converter.as_mut(), b"\x1b$B$\0").await, "\u{fffd}");
    }
}

#[tokio::test]
async fn iso2022jp_configuration_preserves_alias_spelling_trimming_and_key_value_precedence() {
    let mut trimmed = open("\0\tCSISO2022JP2\r\u{1f}");
    assert_eq!(decode(trimmed.as_mut(), b"\x1b$(D\"7").await, "～");
    for invalid in ["iso_2022_jp", "ISO-2022-JP2", "windows-50220", " ISO-2022-JP "] {
        assert!(PluginRegistry::default().converter(&spec(&[("value.deserializer.encoding", invalid)]), false).is_err(), "{invalid}");
    }
    let options = spec(&[
        ("converter.encoding", "jis"),
        ("key.serializer.encoding", "csISO2022JP2"),
        ("key.deserializer.encoding", "cp50220"),
        ("value.serializer.encoding", "ms50221"),
        ("value.deserializer.encoding", "windows-iso2022jp"),
    ]);
    let mut key = PluginRegistry::default().converter(&options, true).unwrap();
    let mut value = PluginRegistry::default().converter(&options, false).unwrap();
    assert_eq!(decode(key.as_mut(), b"\x1b$Bu!").await, "\u{e000}");
    assert_eq!(decode(value.as_mut(), b"\x1b$Bu!").await, "\u{fffd}");
    for (is_key, serializer) in [(true, "key.serializer.encoding"), (false, "value.serializer.encoding")] {
        assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", "jis"), (serializer, "iso_2022_jp")]), is_key).is_err());
    }
}

#[tokio::test]
async fn iso2022jp_mappings_replacements_and_output_free_designations_obey_scalar_memory_admission() {
    for name in ["ISO-2022-JP", "ISO-2022-JP-2", "x-windows-50220", "x-windows-50221", "x-windows-iso2022jp"] {
        let mut converter = open(name);
        for (bytes, expected) in [
            (b"\x1b$B$\"".repeat(1024), "あ".repeat(1024)),
            (b"\x1b(I\0".repeat(1024), "\u{ff40}".repeat(1024)),
            (vec![0xff; 1024], "\u{fffd}".repeat(1024)),
            (b"\x1b$B".repeat(1024), String::new()),
            (b"\x1b$(".to_vec(), "\u{fffd}".to_owned()),
        ] {
            let mut rejected = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4 - 1));
            assert!(converter.decode("events", Some(&bytes), &mut rejected).await.is_err());
            let mut admitted = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4));
            assert_eq!(converter.decode("events", Some(&bytes), &mut admitted).await.unwrap().value, Value::Utf8(expected), "{name}");
        }
    }
}
