use rtdb_columnar::MemoryBudget;
use rtdb_kafka::connect::{ConversionContext, Converter, PluginRegistry, PluginSpec};
use rtdb_types::Value;

fn spec(options: &[(&str, &str)]) -> PluginSpec {
    PluginSpec {
        class_name: "org.apache.kafka.connect.storage.StringConverter".into(),
        options: options.iter().map(|(key, value)| ((*key).into(), (*value).into())).collect(),
    }
}

fn open(name: &str) -> Box<dyn Converter> {
    PluginRegistry::default().converter(&spec(&[("converter.encoding", name)]), false).unwrap()
}

async fn converted(converter: &mut dyn Converter, bytes: Option<&[u8]>) -> Value {
    let mut context = ConversionContext::new(MemoryBudget::new(1 << 20));
    converter.decode("events", bytes, &mut context).await.unwrap().value
}

#[tokio::test]
async fn unicode_extension_names_select_the_same_decoder_and_keep_record_boundaries() {
    for (name, canonical) in [("x-ıSCII91", "x-iscii91"), ("x-ISCIİ91", "x-iscii91"),
        ("x-ſJIS_0213", "x-sjis_0213"), ("x-macuKraine", "x-macukraine"), ("x-İſCİİ91", "x-iscii91")] {
        let mut variant = open(name);
        let mut reference = open(canonical);
        for bytes in [&b"A\0"[..], &[0x81, 0xa1, 0x82, 0xa0], &[], &[0xff]] {
            assert_eq!(converted(&mut *variant, Some(bytes)).await, converted(&mut *reference, Some(bytes)).await, "{name}");
        }
        assert_eq!(converted(&mut *variant, None).await, Value::Null);
    }
}

#[test]
fn unicode_folding_does_not_extend_the_standard_provider_or_accept_extra_name_characters() {
    for name in ["ſhift_jis", "İSO-8859-1", "aſcii", "KOI8-R", "x-Ｉscii91", "x-i\u{307}scii91",
        " x-ıSCII91", "x-ıSCII91 ", "x-ıSCII91\0", "x-\u{10400}scii91"] {
        assert!(PluginRegistry::default().converter(&spec(&[("value.deserializer.encoding", name)]), false).is_err(), "{name}");
    }
}

#[tokio::test]
async fn serializer_and_deserializer_settings_apply_extension_lookup_at_their_existing_precedence() {
    let mut trimmed = open("\tx-ıSCII91 \n");
    assert_eq!(converted(&mut *trimmed, Some(b"A")).await, Value::Utf8("A".into()));
    let options = [("converter.encoding", "ascii"), ("value.serializer.encoding", "x-ſJIS_0213"), ("value.deserializer.encoding", "x-ıSCII91")];
    let mut converter = PluginRegistry::default().converter(&spec(&options), false).unwrap();
    assert_eq!(converted(&mut *converter, Some(b"A")).await, Value::Utf8("A".into()));
    let options = [("converter.encoding", "x-ıSCII91"), ("value.serializer.encoding", "ſhift_jis"), ("value.deserializer.encoding", "ascii")];
    assert!(PluginRegistry::default().converter(&spec(&options), false).is_err());
}
