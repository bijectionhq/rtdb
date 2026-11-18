use super::*;
use apache_avro::Schema;

fn record(text: &str) -> apache_avro::schema::RecordSchema {
    let schema = Schema::parse_str_with_list_with_name_policy(text, Vec::<&str>::new(), &JavaNames).unwrap().0;
    let Schema::Record(record) = schema else { panic!("expected record") };
    record
}

#[test]
fn packed_java_flags_match_every_independent_unicode_expectation_unit() {
    let mut next = 0u32;
    let mut first_count = 0;
    let mut later_count = 0;
    for line in include_str!("names/expected-ranges.tsv").lines().skip(1) {
        let mut fields = line.split('\t');
        let first = u32::from_str_radix(fields.next().unwrap(), 16).unwrap();
        let last = u32::from_str_radix(fields.next().unwrap(), 16).unwrap();
        let expected = fields.next().unwrap().parse::<u8>().unwrap();
        assert_eq!(fields.next(), None);
        assert_eq!(first, next);
        for unit in first..=last {
            assert_eq!(unit_flags(unit as u16), expected, "U+{unit:04X}");
            first_count += usize::from(expected & 1 != 0);
            later_count += usize::from(expected & 2 != 0);
        }
        next = last + 1;
    }
    assert_eq!(next, 65536);
    assert_eq!((first_count, later_count), (48974, 49344));
    for unit in 0xd800..=0xdfff { assert_eq!(unit_flags(unit), 0); }
}

#[test]
fn java_simple_names_use_bmp_letters_decimal_digits_and_underscore() {
    for name in ["A", "_", "Événement", "变量", "Ж9", "A\u{0661}", "A\u{ff10}", "\u{00aa}"] {
        assert!(JavaNames.record_field(name).is_ok(), "{name}");
        assert!(JavaNames.enum_symbol(name).is_ok(), "{name}");
        assert!(JavaNames.name(name, &None).is_ok(), "{name}");
    }
    for name in ["", "1A", "\u{0661}A", "A\u{0300}", "\u{2160}", "A\u{200c}", "\u{10400}", "A\u{10400}", "a.b", "has space", "a-b"] {
        assert!(JavaNames.record_field(name).is_err(), "{name}");
        assert!(JavaNames.enum_symbol(name).is_err(), "{name}");
    }
    assert!(NameParts::parse("qualified.\u{10400}", None).is_err());
}

#[test]
fn canonical_names_validate_only_suffix_and_preserve_namespace_override_and_empty_context() {
    for (input, namespace, expected_namespace, simple, fullname) in [
        ("A", None, "", "A", "A"),
        ("A", Some("parent"), "parent", "A", "parent.A"),
        ("A", Some(""), "", "A", "A"),
        (".A", Some("parent"), "", "A", "A"),
        ("one.two.A", Some("ignored"), "one.two", "A", "one.two.A"),
        ("A", Some("arbitrary space$.*\u{10400}"), "arbitrary space$.*\u{10400}", "A", "arbitrary space$.*\u{10400}.A"),
        ("space$.*.É", Some("ignored"), "space$.*", "É", "space$.*.É"),
    ] {
        let parts = NameParts::parse(input, namespace).unwrap();
        assert_eq!((parts.namespace, parts.simple), (expected_namespace, simple));
        assert_eq!(parts.fullname().unwrap(), fullname);
        assert_eq!(parts.fullname_bytes().unwrap(), fullname.len());
        assert_eq!(parts.owned_text_bytes().unwrap(), expected_namespace.len() + simple.len());
        assert_eq!(parts.owned().namespace.as_deref(), Some(expected_namespace));
    }
    assert_eq!(NameParts::parse(".A", None).unwrap(), NameParts::parse("A", Some("")).unwrap());
    for input in ["bad.", ".", "qualified.1A"] { assert!(NameParts::parse(input, Some("parent")).is_err()); }
}

#[test]
fn java_policy_isolated_calls_preserve_global_validation_and_raw_field_aliases() {
    assert!(Name::new("Événement").is_err());
    let record = record(r#"{"type":"record","name":"Événement","aliases":["AncienÉvénement"],"fields":[
        {"name":"donnée","aliases":["field alias with spaces"],"type":{"type":"enum","name":"État","symbols":["Été","Hiver"]}},
        {"name":"clé","type":{"type":"fixed","name":"Clé","size":1}},
        {"name":"next","type":["null","Événement"]}]}"#);
    assert_eq!(record.name.fullname(None), "Événement");
    assert_eq!(record.aliases.as_ref().unwrap()[0].fullname(None), "AncienÉvénement");
    assert_eq!(record.fields[0].aliases.as_ref().unwrap(), &["field alias with spaces"]);
    assert!(Name::new("Événement").is_err());
    let global = JavaNames.reference_fallback("A", &Some("parent".into())).unwrap().unwrap();
    assert_eq!(global.namespace.as_deref(), Some(""));
    assert_eq!(global.name, "A");
    assert!(JavaNames.reference_fallback("qualified.A", &None).unwrap().is_none());
}
