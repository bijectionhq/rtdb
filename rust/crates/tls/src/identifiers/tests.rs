use super::*;

fn der(tag: u8, content: &[u8]) -> Vec<u8> { encoded(tag, content) }
fn extension(oid: u8, critical: bool, value: &[u8]) -> Vec<u8> {
    der(0x30, &[der(6, &[0x55, 0x1d, oid]), der(1, &[u8::from(critical)]), der(4, value)].concat())
}
fn certificate(serial: &[u8], extensions: &[Vec<u8>]) -> Vec<u8> {
    let mut tbs = vec![der(0xa0, &der(2, &[2])), der(2, serial)];
    tbs.extend((0..5).map(|_| der(0x30, &[])));
    if !extensions.is_empty() { tbs.push(der(0xa3, &der(0x30, &extensions.concat()))); }
    der(0x30, &[der(0x30, &tbs.concat()), der(0x30, &[]), der(3, &[0])].concat())
}
fn decode(extensions: &[Vec<u8>]) -> CertificateIdentifiers {
    CertificateIdentifiers::decode(&certificate(&[1], extensions), &MemoryBudget::new(16 << 20)).unwrap()
}
fn aki(fields: &[Vec<u8>]) -> Vec<u8> { der(0x30, &fields.concat()) }
fn with_names(name: &[u8]) -> Vec<u8> { aki(&[der(0x80, b"key"), der(0xa1, name), der(0x82, &[7])]) }
fn directory() -> Vec<u8> { der(0xa4, &der(0x30, &der(0x31, &der(0x30, &[der(6, &[0x55, 4, 3]), der(12, b"CA")].concat())))) }

#[test]
fn original_skid_and_canonical_aki_have_distinct_encodings() {
    let original = [4, 0x81, 3, b'k', b'e', b'y'];
    let value = decode(&[extension(14, false, &original), extension(35, false, &aki(&[vec![0x80, 0x81, 3, b'k', b'e', b'y']]))]);
    assert_eq!(value.subject_key_identifier(), Some(original.as_slice()));
    assert_eq!(value.authority_key_identifier(), Some(der(4, b"key").as_slice()));
    assert_ne!(value.subject_key_identifier(), value.authority_key_identifier());
    assert_eq!(value.serial_number(), &[1]);
}

#[test]
fn signed_serials_preserve_sign_and_arbitrary_precision() {
    for (encoded, expected) in [(vec![0, 0, 127], vec![127]), (vec![0, 0x80], vec![0, 0x80]), (vec![0xff, 0xff, 0x80], vec![0x80]), (vec![0xff, 0x7f], vec![0xff, 0x7f]), (vec![0; 7], vec![0]), (vec![0x80; 80], vec![0x80; 80])] {
        let bytes = certificate(&encoded, &[extension(35, false, &aki(&[der(0x82, &encoded)]))]);
        let value = CertificateIdentifiers::decode(&bytes, &MemoryBudget::new(1 << 20)).unwrap();
        assert_eq!(value.serial_number(), expected);
        assert_eq!(value.authority_serial_number(), Some(expected.as_slice()));
        assert_eq!(value.authority_key_identifier(), None);
    }
    assert!(CertificateIdentifiers::decode(&certificate(&[], &[]), &MemoryBudget::new(1 << 20)).is_err());
}

#[test]
fn malformed_late_aki_fields_discard_all_partial_results() {
    for field in [der(0x82, &[]), der(0xa1, &[]), der(0x83, b"ignored"), der(0xa0, &der(4, b"key")), der(0xa1, &der(0x87, &[1, 2, 3]))] {
        let malformed = aki(&[der(0x80, b"key"), field]);
        let value = decode(&[extension(35, false, &malformed)]);
        assert_eq!(value.authority_key_identifier(), None);
        assert_eq!(value.authority_serial_number(), None);
        assert!(CertificateIdentifiers::decode(&certificate(&[1], &[extension(35, true, &malformed)]), &MemoryBudget::new(1 << 20)).is_err());
    }
}

#[test]
fn duplicate_aki_fields_are_invalid_even_when_empty() {
    for field in [der(0x80, &[]), der(0x82, &[0]), der(0xa1, &der(0x82, &[]))] {
        let value = decode(&[extension(35, false, &aki(&[field.clone(), field]))]);
        assert_eq!(value.authority_key_identifier(), None);
        assert_eq!(value.authority_serial_number(), None);
    }
    assert!(decode(&[extension(35, true, &aki(&[]))]).authority.is_some());
}

#[test]
fn raw_skid_fallback_keeps_last_unparseable_copy_and_prefers_parsed_copy() {
    let bad1 = der(5, &[]);
    let bad2 = der(2, &[9]);
    let good = der(4, b"key");
    let value = decode(&[extension(14, false, &bad1), extension(14, false, &bad2)]);
    assert_eq!(value.subject_key_identifier(), Some(bad2.as_slice()));
    for values in [vec![extension(14, false, &bad1), extension(14, false, &good), extension(14, false, &bad2)], vec![extension(14, false, &good), extension(14, false, &bad1)]] {
        assert_eq!(decode(&values).subject_key_identifier(), Some(good.as_slice()));
    }
    assert!(CertificateIdentifiers::decode(&certificate(&[1], &[extension(14, true, &bad1)]), &MemoryBudget::new(1 << 20)).is_err());
}

#[test]
fn two_parsed_identifier_extensions_fail_while_unparseable_copies_do_not_replace_them() {
    for (oid, good) in [(14, der(4, b"key")), (35, aki(&[der(0x80, b"key")]))] {
        let duplicate = certificate(&[1], &[extension(oid, false, &good), extension(oid, false, &good)]);
        assert!(CertificateIdentifiers::decode(&duplicate, &MemoryBudget::new(1 << 20)).is_err());
        let value = decode(&[extension(oid, false, &good), extension(oid, false, &[5, 0])]);
        assert_eq!(if oid == 14 { value.subject_key_identifier() } else { value.authority_key_identifier() }, Some(der(4, b"key").as_slice()));
    }
}

#[test]
fn general_names_cover_all_choices_with_java_constructor_validation() {
    let choices = [
        der(0xa0, &[der(6, &[0x2a, 3]), der(0xa0, &[0xff])].concat()),
        der(0x81, b"a@b@c"), der(0x82, b""), der(0xa3, &[0xff]), directory(),
        der(0xa5, &der(0x81, &der(12, b"Party"))), der(0x86, b"urn:example:ca"),
        der(0x87, &[0; 32]), der(0x88, &[0x2a, 3]),
    ];
    for choice in choices {
        let value = decode(&[extension(35, false, &with_names(&choice))]);
        assert_eq!(value.authority_key_identifier(), Some(der(4, b"key").as_slice()), "{choice:?}");
        assert_eq!(value.authority_serial_number(), Some([7].as_slice()));
    }
    let value = decode(&[extension(35, false, &with_names(&[der(0x82, &[0xff]), der(0x81, &[0xff])].concat()))]);
    assert!(value.authority_key_identifier().is_some());
}

#[test]
fn malformed_general_names_invalidate_noncritical_aki() {
    let choices = [
        der(0x80, &[]), der(0xa1, &[]), der(0xa2, &[]), der(0x83, &[]), der(0x84, &[]),
        der(0x85, &[]), der(0xa6, &[]), der(0xa7, &[0; 4]), der(0xa8, &[0x2a]), der(0x89, &[]),
        der(0x81, b""), der(0x81, b"a@"), der(0x81, b"."), der(0x81, b"a@."),
        der(0x86, b"relative/path"), der(0x86, b"https://host/%Q0"), der(0x87, &[0; 15]),
        der(0x88, &[0x80, 0]), der(0x88, &[0x81]), der(0x88, &[]),
        der(0xa5, &der(0xa1, &der(12, b"Party"))), der(0xa5, &der(0x81, &der(5, &[]))),
        der(0xa5, &[der(0x81, &der(12, b"p")), der(0x81, &der(12, b"q"))].concat()),
    ];
    for choice in choices {
        assert_eq!(decode(&[extension(35, false, &with_names(&choice))]).authority_key_identifier(), None, "{choice:?}");
    }
}

#[test]
fn other_name_validates_builtin_oid_casts_and_explicit_wrapper_only() {
    for oid in [&[0x55, 0x1d, 14][..], &[0x2b, 6, 1, 5, 5, 7, 1, 1], &[0x2b, 6, 1, 5, 5, 7, 0x30, 1, 5], &[0x60, 0x86, 0x48, 1, 0x86, 0xf8, 0x42, 1, 1]] {
        let name = der(0xa0, &[der(6, oid), der(0xa0, &[])].concat());
        assert_eq!(decode(&[extension(35, false, &with_names(&name))]).authority_key_identifier(), None);
    }
    let name = der(0xa0, &[der(6, &[0x2a, 3]), der(0xa0, &[]), vec![0xff]].concat());
    assert!(decode(&[extension(35, false, &with_names(&name))]).authority_key_identifier().is_some());
    let name = der(0xa0, &[der(6, &[0x2a, 3]), der(0x80, &[])].concat());
    assert_eq!(decode(&[extension(35, false, &with_names(&name))]).authority_key_identifier(), None);
}

#[test]
fn uri_hosts_distinguish_java_syntax_numeric_scope_and_environmental_resolution() {
    for name in ["https://ca.example/path", "urn:example:ca", "https://under_score/path", "https://[::1]/", "https://[fe80::1%2147483647]/", "https://[::ffff:1%1]/"] {
        assert!(decode(&[extension(35, false, &with_names(&der(0x86, name.as_bytes())))]).authority_key_identifier().is_some(), "{name}");
    }
    for name in ["https://[::ffff:192.0.2.1%1]/", "https://[0:0:0:0:0:ffff:c000:201%1]/"] {
        assert!(decode(&[extension(35, false, &with_names(&der(0x86, name.as_bytes())))]).authority_key_identifier().is_none(), "{name}");
    }
    for name in ["https://[fe80::1%eth0]/", "https://[fe80::1%2147483648]/", "https://ca.example./"] {
        let bytes = certificate(&[1], &[extension(35, false, &with_names(&der(0x86, name.as_bytes())))]);
        let error = CertificateIdentifiers::decode(&bytes, &MemoryBudget::new(1 << 20)).unwrap_err();
        assert_eq!(error.code(), "rtdb.tls/unsupported-identifier", "{name}");
    }
}

#[test]
fn constructed_subject_octets_keep_original_encoding_and_java_class_tolerance() {
    for tag in [0x24, 0x64, 0xa4, 0xe4] {
        let original = der(tag, &[der(4, b"k"), der(0x24, &der(4, b"ey"))].concat());
        assert_eq!(decode(&[extension(14, true, &original)]).subject_key_identifier(), Some(original.as_slice()));
    }
    let inner = der(4, b"key");
    let outer = der(0x30, &[der(6, &[0x55, 0x1d, 14]), der(0x24, &[der(4, &inner[..2]), der(4, &inner[2..])].concat())].concat());
    assert_eq!(decode(&[outer]).subject_key_identifier(), Some(inner.as_slice()));
}

#[test]
fn java_ber_forms_are_normalized_before_identifier_parsing() {
    let indefinite = [vec![0x30, 0x80], der(0x80, b"key"), vec![0, 0]].concat();
    assert!(decode(&[extension(35, true, &indefinite)]).authority_key_identifier().is_some());
    let padded = [vec![0x30, 0x80, 0x80, 0x81, 3], b"key".to_vec(), vec![0, 0]].concat();
    assert!(decode(&[extension(35, false, &padded)]).authority_key_identifier().is_none());
    let trailing = [indefinite, vec![0]].concat();
    assert!(decode(&[extension(35, false, &trailing)]).authority_key_identifier().is_none());
    let original = [vec![0x24, 0x80], der(4, b"key"), vec![0, 0]].concat();
    assert_eq!(decode(&[extension(14, true, &original)]).subject_key_identifier(), Some(original.as_slice()));
}

#[test]
fn deep_constructed_octets_use_input_bounded_iteration() {
    let mut original = [0x24, 0x80].repeat(2048);
    original.extend_from_slice(&der(4, b"key"));
    original.extend_from_slice(&[0, 0].repeat(2048));
    let bytes = certificate(&[1], &[extension(14, true, &original)]);
    let memory = MemoryBudget::new(16 << 20);
    let value = CertificateIdentifiers::decode(&bytes, &memory).unwrap();
    assert_eq!(value.subject_key_identifier(), Some(original.as_slice()));
    assert_eq!(memory.used_bytes(), bytes.len() * 512 + 65_536);
    drop(value);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn identifier_ownership_and_error_paths_release_independent_reservations() {
    let memory = MemoryBudget::new(1 << 20);
    let bytes = certificate(&[0xff, 0x80], &[extension(35, false, &aki(&[der(0x80, b"key")]))]);
    let value = CertificateIdentifiers::decode(&bytes, &memory).unwrap();
    assert!(memory.used_bytes() > 0);
    drop(bytes);
    assert_eq!(value.serial_number(), &[0x80]);
    assert_eq!(value.authority_key_identifier(), Some(der(4, b"key").as_slice()));
    drop(value);
    assert_eq!(memory.used_bytes(), 0);
    let tiny = MemoryBudget::new(1);
    assert_eq!(CertificateIdentifiers::decode(&[], &tiny).unwrap_err().code(), "rtdb/memory-budget-exceeded");
    assert!(CertificateIdentifiers::decode(&[], &memory).is_err());
    assert_eq!(memory.used_bytes(), 0);
    assert_eq!(tiny.used_bytes(), 0);
}

#[test]
fn nested_directory_name_admission_errors_are_not_noncritical_parse_failures() {
    let bytes = certificate(&[1], &[extension(35, false, &with_names(&directory()))]);
    let memory = MemoryBudget::new(bytes.len() * 512 + 65_536);
    assert_eq!(CertificateIdentifiers::decode(&bytes, &memory).unwrap_err().code(), "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), 0);
}
