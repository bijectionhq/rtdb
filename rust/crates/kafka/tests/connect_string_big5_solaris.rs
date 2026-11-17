use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConversionContext, Converter, PluginRegistry, PluginSpec, SchemaKind};
use rtdb_types::Value;

const MANIFEST: &str = include_str!("../src/connect/converter/charset/double_byte/big5-solaris.tsv");

fn spec(options: &[(&str, &str)]) -> PluginSpec {
    PluginSpec {
        class_name: "org.apache.kafka.connect.storage.StringConverter".into(),
        options: options.iter().map(|(key, value)| ((*key).into(), (*value).into())).collect(),
    }
}

fn open(encoding: &str) -> Box<dyn Converter> {
    PluginRegistry::default().converter(&spec(&[("converter.encoding", encoding)]), false).unwrap()
}

async fn decode(converter: &mut dyn Converter, bytes: Option<&[u8]>) -> Value {
    let mut context = ConversionContext::new(MemoryBudget::new(1 << 20));
    let data = converter.decode("events", bytes, &mut context).await.unwrap();
    let schema = data.schema.unwrap();
    assert!(schema.optional);
    assert!(matches!(schema.kind, SchemaKind::String));
    data.value
}

fn sha256_hex(bytes: &[u8]) -> String {
    openssl::sha::sha256(bytes).iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn solaris_patch_and_provider_sources_retain_pinned_provenance() {
    assert_eq!(sha256_hex(MANIFEST.as_bytes()), "ff6566e677ad19bd24ee807c0135d0d65a5e89dba9ae7df7420864330d89f9d1");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/big5/patches.rs")), "43b7372024f17dd8de0e4fa2ac23b84cc2e8a5fb1ac3d54cb32baece56580238");
    assert_eq!(sha256_hex(include_bytes!("../src/connect/converter/charset/double_byte/big5-solaris.sources.sha256")), "fa01a990f1537afa1d914a004b96e50468afa943cd23b2a51db7459f6a21299c");
    let fields: Vec<_> = MANIFEST.lines().nth(1).unwrap().split('\t').collect();
    assert_eq!(fields.len(), 13);
    assert_eq!(&fields[..2], &["x-Big5-Solaris", "Big5_Solaris"]);
    assert_eq!(&fields[6..12], &["f9", "d6", "dc", "fffd", "7881|92b9|88cf|58bb|6052|7ca7|5afa", "Big5_Solaris"]);
}

#[tokio::test]
async fn first_nonempty_solaris_record_patches_existing_and_future_big5_converters_process_wide() {
    let mut existing = open("Big5");
    let hkscs_names = ["Big5-HKSCS", "x-Big5-HKSCS-2001", "x-MS950-HKSCS", "x-MS950-HKSCS-XP"];
    let mut hkscs: Vec<_> = hkscs_names.iter().map(|name| open(name)).collect();
    let mut alias = open("csBig5");
    let mut solaris = open("x-Big5-Solaris");
    let mut solaris_alias = open("bIg5_sOlArIs");
    let mut serializer_only = PluginRegistry::default().converter(&spec(&[
        ("converter.encoding", "Big5"),
        ("value.serializer.encoding", "Big5_Solaris"),
    ]), false).unwrap();
    for converter in &mut hkscs {
        assert_eq!(decode(converter.as_mut(), None).await, Value::Null);
        assert_eq!(decode(converter.as_mut(), Some(&[])).await, Value::Utf8(String::new()));
        assert_eq!(decode(converter.as_mut(), Some(b"a")).await, Value::Utf8("a".into()));
        for (second, scalar) in (0xd6..=0xdc).zip("碁銹裏墻恒粧嫺".chars()) {
            assert_eq!(decode(converter.as_mut(), Some(&[0xf9, second])).await, Value::Utf8(scalar.to_string()));
        }
    }
    for second in 0xd6..=0xdc {
        assert_eq!(decode(existing.as_mut(), Some(&[0xf9, second])).await, Value::Utf8("\u{fffd}\u{fffd}".into()));
        assert_eq!(decode(serializer_only.as_mut(), Some(&[0xf9, second])).await, Value::Utf8("\u{fffd}\u{fffd}".into()));
    }
    assert_eq!(decode(solaris.as_mut(), None).await, Value::Null);
    assert_eq!(decode(solaris_alias.as_mut(), Some(&[])).await, Value::Utf8(String::new()));
    assert_eq!(decode(alias.as_mut(), Some(&[0xf9, 0xd6])).await, Value::Utf8("\u{fffd}\u{fffd}".into()));
    let mut rejected = ConversionContext::new(MemoryBudget::new(16_384 + 4 - 1));
    assert!(solaris.decode("events", Some(b"a"), &mut rejected).await.is_err());
    assert_eq!(decode(existing.as_mut(), Some(&[0xf9, 0xd6])).await, Value::Utf8("\u{fffd}\u{fffd}".into()));
    assert_eq!(decode(solaris.as_mut(), Some(b"a")).await, Value::Utf8("a".into()));
    for (name, converter) in hkscs_names.into_iter().zip(&mut hkscs) {
        let mut future = open(name);
        for current in [converter, &mut future] {
            for (second, scalar) in (0xd6..=0xdc).zip("碁銹裏墻恒粧嫺".chars()) {
                assert_eq!(decode(current.as_mut(), Some(&[0xf9, second])).await, Value::Utf8(scalar.to_string()));
            }
            assert_eq!(decode(current.as_mut(), Some(&[0xa4, 0xa4])).await, Value::Utf8("中".into()));
        }
    }
    for converter in [&mut existing, &mut alias, &mut serializer_only, &mut solaris, &mut solaris_alias] {
        for (second, scalar) in (0xd6..=0xdc).zip("碁銹裏墻恒粧嫺".chars()) {
            assert_eq!(decode(converter.as_mut(), Some(&[0xf9, second])).await, Value::Utf8(scalar.to_string()));
        }
        assert_eq!(decode(converter.as_mut(), Some(&[0xa4, 0xa4, 0xa4, 0xe5])).await, Value::Utf8("中文".into()));
        assert_eq!(decode(converter.as_mut(), None).await, Value::Null);
        assert_eq!(decode(converter.as_mut(), Some(&[])).await, Value::Utf8(String::new()));
    }
    let fields: Vec<_> = MANIFEST.lines().nth(1).unwrap().split('\t').collect();
    for name in ["Big5", "csBig5", fields[0], fields[11]] {
        for selected in [name.to_owned(), name.to_ascii_uppercase()] {
            let mut converter = open(&selected);
            let mut text = String::new();
            for first in 0u8..=255 {
                let bytes: Vec<_> = (0u8..=255).flat_map(|second| [first, second, 0]).collect();
                let Value::Utf8(decoded) = decode(converter.as_mut(), Some(&bytes)).await else { panic!("expected string") };
                text.push_str(&decoded);
            }
            assert_eq!(sha256_hex(text.as_bytes()), fields[12], "{selected}");
        }
    }
    let mut trimmed = open("\0\tBig5_Solaris\r\u{1f}");
    assert_eq!(decode(trimmed.as_mut(), Some(&[0xf9, 0xdc])).await, Value::Utf8("嫺".into()));
    for invalid in ["Big5-Solaris", "Big5Solaris", "x_Big5_Solaris", " Big5_Solaris "] {
        assert!(PluginRegistry::default().converter(&spec(&[("value.deserializer.encoding", invalid)]), false).is_err(), "{invalid}");
    }
    let bytes: Vec<_> = (0..1024).flat_map(|_| [0xf9, 0xd6]).collect();
    let mut rejected = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4 - 1));
    assert!(solaris.decode("events", Some(&bytes), &mut rejected).await.is_err());
    let mut admitted = ConversionContext::new(MemoryBudget::new(16_384 + bytes.len() * 4));
    assert_eq!(solaris.decode("events", Some(&bytes), &mut admitted).await.unwrap().value, Value::Utf8("碁".repeat(1024)));
}
