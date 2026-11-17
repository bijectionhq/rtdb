use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConversionContext, Converter, PluginRegistry, PluginSpec, SchemaKind};
use rtdb_types::Value;

const MANIFEST: &str = include_str!("../src/connect/converter/charset/iscii91/charsets.tsv");

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

const CONTEXTS: &str = include_str!("../src/connect/converter/charset/iscii91/contexts.tsv");

fn prefix(hex: &str) -> Vec<u8> {
    hex.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

#[test]
fn iscii91_resources_preserve_all_255_source_units_and_provider_provenance() {
    const MAPPINGS: &str = include_str!("../src/connect/converter/charset/iscii91/mappings.tsv");
    const TABLE: &[u8; 510] = include_bytes!("../src/connect/converter/charset/iscii91/direct-map.bin");
    assert_eq!(sha256_hex(MANIFEST.as_bytes()), "629a022b08d7383739bb9c071d6dc94999ceffd289453377da21834518522fe9");
    assert_eq!(sha256_hex(CONTEXTS.as_bytes()), "23993847f8d8efcd4e9f66ca178af37c91afa549e803b46a9876228bd93d4b3a");
    assert_eq!(sha256_hex(MAPPINGS.as_bytes()), "0496a78187816101b0b9a0daab43dfcb834613427de2e9c37da1b9f419ebf2d3");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/iscii91/aliases.rs")), "c5fe3693f7b6012b986d057220bc22efa1f02960e6db8f430f98fc10008852c5");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/iscii91/sources.sha256")), "8252e054390fb4c6237bddeac12a89d860e97b84ad3e4a17219d796281a50e08");
    assert_eq!(MAPPINGS.lines().count(), 2);
    let fields: Vec<_> = MAPPINGS.lines().nth(1).unwrap().split('\t').collect();
    assert_eq!(fields.len(), 6);
    assert_eq!(fields[3], "510");
    assert_eq!(fields[5], "255");
    assert_eq!(sha256_hex(TABLE), fields[4]);
    let units: Vec<_> = TABLE.chunks_exact(2).map(|unit| u16::from_be_bytes([unit[0], unit[1]])).collect();
    assert_eq!(&units[..128], &(0u16..128).collect::<Vec<_>>());
    assert!(units.iter().all(|unit| !(0xd800..=0xdfff).contains(unit)));
    assert_eq!(units[0xa0], 0x0901);
    assert_eq!(units[0xe7], 0x094d);
    assert_eq!(units[0xe8], 0x093c);
    assert_eq!(units[0xee], 0xfffd);
    assert_eq!(units[0xef], 0xfffd);
    assert_eq!(units[0xfe], 0xffff);
}

#[tokio::test]
async fn every_iscii91_name_decodes_all_single_bytes_and_pairs_case_insensitively() {
    assert_eq!(MANIFEST.lines().count(), 2);
    assert_eq!(CONTEXTS.lines().count(), 21);
    let fields: Vec<_> = MANIFEST.lines().nth(1).unwrap().split('\t').collect();
    assert_eq!(fields.len(), 5);
    let empty: Vec<_> = CONTEXTS.lines().nth(1).unwrap().split('\t').collect();
    assert_eq!(empty[0], "empty");
    assert_eq!(empty[1], "");
    let mut names = std::collections::BTreeSet::new();
    for name in std::iter::once(fields[0]).chain(fields[4].split('|')) {
        names.insert(name.to_ascii_lowercase());
        for selected in [name.to_owned(), name.to_ascii_uppercase()] {
            let mut converter = open(&selected);
            let mut singles = String::new();
            for byte in 0u8..=255 { singles.push_str(&decode(converter.as_mut(), &[byte]).await); }
            assert_eq!(sha256_hex(singles.as_bytes()), empty[2], "{selected}");
            let mut pairs = String::new();
            for first in 0u8..=255 {
                for second in 0u8..=255 {
                    pairs.push_str(&decode(converter.as_mut(), &[first, second]).await);
                    pairs.push('\0');
                }
            }
            assert_eq!(sha256_hex(pairs.as_bytes()), empty[3], "{selected}");
            assert_eq!(decode(converter.as_mut(), &[]).await, "");
            assert_eq!(converted(converter.as_mut(), None).await, Value::Null);
        }
    }
    assert_eq!(names.len(), 6);
}

#[tokio::test]
async fn iscii91_all_byte_and_pair_suffixes_preserve_twenty_pending_emitted_and_error_contexts() {
    let mut converter = open("x-ISCII91");
    for line in CONTEXTS.lines().skip(1) {
        let fields: Vec<_> = line.split('\t').collect();
        assert_eq!(fields.len(), 4);
        let prefix = prefix(fields[1]);
        let mut single = prefix.clone();
        single.push(0);
        let mut singles = String::new();
        for byte in 0u8..=255 {
            *single.last_mut().unwrap() = byte;
            singles.push_str(&decode(converter.as_mut(), &single).await);
        }
        assert_eq!(sha256_hex(singles.as_bytes()), fields[2], "{}", fields[0]);
        let mut record = prefix;
        let suffix = record.len();
        record.extend_from_slice(&[0, 0]);
        let mut pairs = String::new();
        for first in 0u8..=255 {
            record[suffix] = first;
            for second in 0u8..=255 {
                record[suffix + 1] = second;
                pairs.push_str(&decode(converter.as_mut(), &record).await);
                pairs.push('\0');
            }
        }
        assert_eq!(sha256_hex(pairs.as_bytes()), fields[3], "{}", fields[0]);
    }
}

#[tokio::test]
async fn iscii91_signed_byte_indexing_and_all_eight_nukta_compositions_match_java() {
    let mut converter = open("iscii");
    assert_eq!(decode(converter.as_mut(), b"A\0\x7f\x80\x81\xff").await, "A\0\u{7f}\u{7f}\u{fffd}\u{fffd}");
    for (byte, base, composed) in [
        (0xa1, '\u{0901}', '\u{0950}'),
        (0xa6, '\u{0907}', '\u{090c}'),
        (0xa7, '\u{0908}', '\u{0961}'),
        (0xaa, '\u{090b}', '\u{0960}'),
        (0xdb, '\u{093f}', '\u{0962}'),
        (0xdc, '\u{0940}', '\u{0963}'),
        (0xdf, '\u{0943}', '\u{0944}'),
        (0xea, '\u{0964}', '\u{093d}'),
    ] {
        assert_eq!(decode(converter.as_mut(), &[byte]).await, base.to_string());
        assert_eq!(decode(converter.as_mut(), &[byte, 0xe9]).await, composed.to_string());
        assert_eq!(decode(converter.as_mut(), &[byte, b'A']).await, format!("{base}A"));
        assert_eq!(decode(converter.as_mut(), &[byte, byte, 0xe9]).await, format!("{base}{composed}"));
        assert_eq!(decode(converter.as_mut(), &[byte, 0xe9, 0xe9]).await, format!("{composed}\u{093c}"));
    }
    assert_eq!(decode(converter.as_mut(), &[0xb3, 0xe9]).await, "\u{0915}\u{093c}");
    assert_eq!(decode(converter.as_mut(), &[0xf1, 0xfa]).await, "\u{0966}\u{096f}");
}

#[tokio::test]
async fn iscii91_halant_context_survives_unmappable_bytes_and_pending_units_keep_their_flush_order() {
    let mut converter = open("ISCII91");
    for (bytes, expected) in [
        (&[0xe8, 0xe8][..], "\u{094d}\u{200c}"),
        (&[0xe8, 0xe9][..], "\u{094d}\u{200d}"),
        (&[0xe8, 0xe8, 0xe9][..], "\u{094d}\u{200c}\u{200d}"),
        (&[0xe8, 0x81, 0xe8][..], "\u{094d}\u{fffd}\u{200c}"),
        (&[0xe8, 0xff, 0xe9][..], "\u{094d}\u{fffd}\u{200d}"),
        (&[0xe8, b'A', 0xe9][..], "\u{094d}A\u{093c}"),
        (&[0xa1, 0xe8, 0xe9][..], "\u{0901}\u{094d}\u{093c}"),
        (&[0xa1, 0xe8, 0xe8][..], "\u{0901}\u{094d}\u{094d}"),
        (&[0xa1, 0x81][..], "\u{0901}\u{ffff}"),
        (&[0xa1, 0x81, 0xe9][..], "\u{0901}\u{ffff}\u{093c}"),
        (&[0xa1, 0x81, b'A'][..], "\u{0901}\u{ffff}A"),
        (&[0xa1, b'A', 0x81][..], "\u{0901}A\u{ffff}"),
        (&[0xa1, 0x81, 0xa6, 0xe9][..], "\u{0901}\u{ffff}\u{090c}"),
    ] {
        assert_eq!(decode(converter.as_mut(), bytes).await, expected, "{bytes:x?}");
    }
}

#[tokio::test]
async fn iscii91_attribute_and_extension_bytes_consume_the_following_byte_in_both_context_states() {
    let mut converter = open("x-ISCII91");
    for marker in [0xef, 0xf0] {
        assert_eq!(decode(converter.as_mut(), &[marker]).await, "\u{fffd}");
        assert_eq!(decode(converter.as_mut(), &[0xa1, marker]).await, "\u{0901}\u{fffd}");
        for next in [0, b'A', 0x81, 0xa1, 0xe8, 0xe9, 0xef, 0xf0, 0xff] {
            assert_eq!(decode(converter.as_mut(), &[marker, next, b'B']).await, "\u{fffd}\u{fffd}B");
            assert_eq!(decode(converter.as_mut(), &[0xa1, marker, next, b'B']).await, "\u{0901}\u{fffd}B");
        }
    }
}

#[tokio::test]
async fn iscii91_records_flush_pending_units_and_reset_context_across_null_and_empty_values() {
    let mut converter = open("iscii");
    for bytes in [&[0xa1][..], &[0xe8][..], &[0xef][..], &[0xa1, 0x81][..], &[0xa1, 0xf0][..]] {
        decode(converter.as_mut(), bytes).await;
        assert_eq!(converted(converter.as_mut(), None).await, Value::Null);
        assert_eq!(decode(converter.as_mut(), &[]).await, "");
        assert_eq!(decode(converter.as_mut(), &[0xe9]).await, "\u{093c}");
        assert_eq!(decode(converter.as_mut(), &[0xe8]).await, "\u{094d}");
        assert_eq!(decode(converter.as_mut(), b"A").await, "A");
    }
    let mut separated = decode(converter.as_mut(), &[0xa1]).await;
    separated.push_str(&decode(converter.as_mut(), &[0xe9]).await);
    assert_eq!(separated, "\u{0901}\u{093c}");
    assert_eq!(decode(converter.as_mut(), &[0xa1, 0xe9]).await, "\u{0950}");
}

#[tokio::test]
async fn iscii91_configuration_preserves_exact_aliases_trimming_and_key_value_precedence() {
    let mut trimmed = open("\0\tST_SEV_358-88\r\u{1f}");
    assert_eq!(decode(trimmed.as_mut(), &[0xa1, 0xe9]).await, "\u{0950}");
    for invalid in ["x_iscii91", "ISCII-91", "st-sev-358-88", " iscii "] {
        assert!(PluginRegistry::default().converter(&spec(&[("value.deserializer.encoding", invalid)]), false).is_err(), "{invalid}");
    }
    let options = spec(&[
        ("converter.encoding", "ISCII91"),
        ("key.serializer.encoding", "iso-ir-153"),
        ("key.deserializer.encoding", "csISO153GOST1976874"),
        ("value.serializer.encoding", "iscii"),
        ("value.deserializer.encoding", "ISO-8859-1"),
    ]);
    let mut key = PluginRegistry::default().converter(&options, true).unwrap();
    let mut value = PluginRegistry::default().converter(&options, false).unwrap();
    assert_eq!(decode(key.as_mut(), &[0xa1, 0xe9]).await, "\u{0950}");
    assert_eq!(decode(value.as_mut(), &[0xa1, 0xe9]).await, "¡é");
    for (is_key, serializer) in [(true, "key.serializer.encoding"), (false, "value.serializer.encoding")] {
        assert!(PluginRegistry::default().converter(&spec(&[("converter.encoding", "iscii"), (serializer, "ISCII-91")]), is_key).is_err());
    }
}

#[tokio::test]
async fn iscii91_compositions_pending_flushes_and_replacements_obey_scalar_memory_admission() {
    let mut converter = open("iscii");
    for (bytes, expected) in [
        (vec![0xa1; 1024], "\u{0901}".repeat(1024)),
        ([0xa1, 0xe9].repeat(1024), "\u{0950}".repeat(1024)),
        (vec![0x81; 1024], "\u{fffd}".repeat(1024)),
        (vec![0xa1, 0x81], "\u{0901}\u{ffff}".to_owned()),
        (vec![0xef, b'A'], "\u{fffd}\u{fffd}".to_owned()),
    ] {
        let mut rejected = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4 - 1));
        assert!(converter.decode("events", Some(&bytes), &mut rejected).await.is_err());
        let mut admitted = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4));
        assert_eq!(converter.decode("events", Some(&bytes), &mut admitted).await.unwrap().value, Value::Utf8(expected));
    }
}
