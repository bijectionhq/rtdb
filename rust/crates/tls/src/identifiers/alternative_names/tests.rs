use super::*;

fn extension(critical: bool, value: &[u8]) -> Vec<u8> {
    encoded(0x30, &[encoded(6, &[0x55, 0x1d, 17]), encoded(1, &[u8::from(critical)]), encoded(4, value)].concat())
}
fn certificate(extensions: &[Vec<u8>]) -> Vec<u8> {
    let mut tbs = vec![encoded(0xa0, &encoded(2, &[2])), encoded(2, &[1])];
    tbs.extend((0..5).map(|_| encoded(0x30, &[])));
    tbs.push(encoded(0xa3, &encoded(0x30, &extensions.concat())));
    encoded(0x30, &[encoded(0x30, &tbs.concat()), encoded(0x30, &[]), encoded(3, &[0])].concat())
}
fn decode(extensions: &[Vec<u8>]) -> CertificateAlternativeNames {
    CertificateAlternativeNames::decode(&certificate(extensions), &MemoryBudget::new(16 << 20)).unwrap()
}
fn names(values: &[&[u8]]) -> Vec<u8> {
    encoded(0x30, &values.iter().flat_map(|value| encoded(0x82, value)).collect::<Vec<_>>())
}

#[test]
fn san_repetition_uses_original_parsed_extension_bytes() {
    let first = names(&[b"A.example", b"b.example"]);
    let alternatives = [names(&[b"a.example", b"b.example"]), names(&[b"b.example", b"A.example"]),
        [vec![0x30, 0x81, first[1]], first[2..].to_vec()].concat()];
    let original = decode(&[extension(false, &first)]);
    assert_eq!(original.extension_value(), Some(first.as_slice()));
    assert_eq!(original.extension_value(), decode(&[extension(true, &first)]).extension_value());
    for alternate in alternatives {
        assert_eq!(decode(&[extension(false, &alternate)]).extension_value(), Some(alternate.as_slice()));
        assert_ne!(original.extension_value(), decode(&[extension(false, &alternate)]).extension_value());
    }
}

#[test]
fn malformed_noncritical_san_is_absent_and_critical_san_fails() {
    assert_eq!(decode(&[]).extension_value(), None);
    for value in [vec![5, 0], vec![0x30, 0], vec![0x30, 3, 0x87, 1, 0], vec![0x30, 2, 0x82], [names(&[b"x"]), vec![0]].concat()] {
        assert_eq!(decode(&[extension(false, &value)]).extension_value(), None);
        assert!(CertificateAlternativeNames::decode(&certificate(&[extension(true, &value)]), &MemoryBudget::new(1 << 20)).is_err());
    }
}

#[test]
fn parsed_san_survives_unparseable_duplicates_but_two_parsed_copies_fail() {
    let good = names(&[b"x"]);
    for values in [vec![extension(false, &[5, 0]), extension(false, &good)], vec![extension(false, &good), extension(false, &[5, 0])]] {
        assert_eq!(decode(&values).extension_value(), Some(good.as_slice()));
    }
    assert_eq!(decode(&[extension(false, &[5, 0]), extension(false, &[0x30, 0])]).extension_value(), None);
    assert!(CertificateAlternativeNames::decode(&certificate(&[extension(false, &good), extension(true, &good)]), &MemoryBudget::new(1 << 20)).is_err());
}

#[test]
fn san_general_names_retain_java_constructor_behavior() {
    for name in [encoded(0x82, &[]), encoded(0x81, b"a@b@c"), encoded(0xa3, &[0xff]), encoded(0x87, &[0; 32]),
        encoded(0xa0, &[encoded(6, &[0x2a, 3]), encoded(0xa0, &[0xff])].concat())] {
        let value = encoded(0x30, &name);
        assert_eq!(decode(&[extension(false, &value)]).extension_value(), Some(value.as_slice()));
    }
    let registered_other_name = encoded(0x30, &encoded(0xa0, &[encoded(6, &[0x55, 0x1d, 17]), encoded(0xa0, &[0xff])].concat()));
    assert_eq!(decode(&[extension(false, &registered_other_name)]).extension_value(), None);
}

#[test]
fn san_indefinite_encoding_is_validated_without_rewriting_the_retained_bytes() {
    let value = [vec![0x30, 0x80], encoded(0x82, b"x"), vec![0, 0]].concat();
    assert_eq!(decode(&[extension(false, &value)]).extension_value(), Some(value.as_slice()));
    assert_ne!(decode(&[extension(false, &value)]).extension_value(), decode(&[extension(false, &names(&[b"x"]))]).extension_value());
}

#[test]
fn san_environmental_uri_resolution_errors_are_not_malformed_absence() {
    for uri in ["https://[fe80::1%interface]/", "https://host.example./"] {
        let value = encoded(0x30, &encoded(0x86, uri.as_bytes()));
        for critical in [false, true] {
            let result = CertificateAlternativeNames::decode(&certificate(&[extension(critical, &value)]), &MemoryBudget::new(1 << 20));
            assert_eq!(result.unwrap_err().code(), "rtdb.tls/unsupported-identifier");
        }
    }
}

#[test]
fn san_reservation_covers_success_absence_and_error_lifetimes() {
    let memory = MemoryBudget::new(1 << 20);
    let good = certificate(&[extension(false, &names(&[b"x"]))]);
    let first = CertificateAlternativeNames::decode(&good, &memory).unwrap();
    let retained = memory.used_bytes(); assert!(retained > good.len());
    let absent = CertificateAlternativeNames::decode(&certificate(&[]), &memory).unwrap();
    assert!(memory.used_bytes() > retained);
    drop(absent); assert_eq!(memory.used_bytes(), retained);
    assert!(CertificateAlternativeNames::decode(&certificate(&[extension(true, &[5, 0])]), &memory).is_err());
    assert_eq!(memory.used_bytes(), retained);
    drop(first); assert_eq!(memory.used_bytes(), 0);
    assert_eq!(CertificateAlternativeNames::decode(&good, &MemoryBudget::new(1)).unwrap_err().code(), "rtdb/memory-budget-exceeded");
}
