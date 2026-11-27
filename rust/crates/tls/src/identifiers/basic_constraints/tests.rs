use super::*;

fn extension(critical: bool, value: &[u8]) -> Vec<u8> {
    encoded(0x30, &[encoded(6, &[0x55, 0x1d, 19]), encoded(1, &[u8::from(critical)]), encoded(4, value)].concat())
}
fn certificate(extensions: &[Vec<u8>]) -> Vec<u8> {
    let mut tbs = vec![encoded(0xa0, &encoded(2, &[2])), encoded(2, &[1])];
    tbs.extend((0..5).map(|_| encoded(0x30, &[])));
    tbs.push(encoded(0xa3, &encoded(0x30, &extensions.concat())));
    encoded(0x30, &[encoded(0x30, &tbs.concat()), encoded(0x30, &[]), encoded(3, &[0])].concat())
}
fn decode(extensions: &[Vec<u8>]) -> CertificateBasicConstraints {
    CertificateBasicConstraints::decode(&certificate(extensions), &MemoryBudget::new(16 << 20)).unwrap()
}
fn constraints(ca: u8, path_length: Option<&[u8]>) -> Vec<u8> {
    let mut fields = encoded(1, &[ca]);
    if let Some(path_length) = path_length { fields.extend(encoded(2, path_length)); }
    encoded(0x30, &fields)
}

#[test]
fn absent_empty_and_non_boolean_first_fields_are_non_ca() {
    assert_eq!(decode(&[]).path_length(), -1);
    for fields in [vec![], encoded(2, &[5]), [encoded(5, &[]), vec![0xff]].concat()] {
        assert_eq!(decode(&[extension(true, &encoded(0x30, &fields))]).path_length(), -1);
    }
}

#[test]
fn java_ca_flag_accepts_any_nonzero_boolean_and_defaults_to_unlimited() {
    for ca in [1, 2, 0x7f, 0x80, 0xff] {
        assert_eq!(decode(&[extension(false, &constraints(ca, None))]).path_length(), i32::MAX);
        assert_eq!(decode(&[extension(true, &constraints(ca, Some(&[0])))]).path_length(), 0);
    }
    for length in [None, Some([9].as_slice()), Some([0xff].as_slice())] {
        assert_eq!(decode(&[extension(false, &constraints(0, length))]).path_length(), -1);
    }
}

#[test]
fn signed_java_path_lengths_preserve_ber_padding_and_int_boundaries() {
    for (bytes, expected) in [
        (vec![0xff], -1), (vec![0xfe], -2), (vec![0x80], -128),
        (vec![0xff, 0xff, 0x80, 0, 0, 0], i32::MIN),
        (vec![0, 0, 0x7f, 0xff, 0xff, 0xff], i32::MAX),
        (vec![0; 8], 0), (vec![0xff; 8], -1), (vec![0, 0x80], 128),
    ] {
        assert_eq!(decode(&[extension(true, &constraints(1, Some(&bytes)))]).path_length(), expected);
    }
}

#[test]
fn basic_constraints_ignore_fields_after_the_parsed_integer() {
    for tail in [encoded(2, &[99]), vec![0xff], vec![0x30, 0x80]] {
        let content = [encoded(1, &[1]), encoded(2, &[3]), tail].concat();
        assert_eq!(decode(&[extension(true, &encoded(0x30, &content))]).path_length(), 3);
    }
}

#[test]
fn malformed_basic_constraints_obey_criticality_even_for_non_ca() {
    let values = [vec![], encoded(5, &[]), [constraints(1, None), vec![0]].concat(),
        encoded(0x30, &encoded(1, &[])), encoded(0x30, &encoded(1, &[0, 1])),
        encoded(0x30, &[encoded(1, &[0]), encoded(5, &[])].concat()),
        constraints(0, Some(&[])), constraints(1, Some(&[0, 0x80, 0, 0, 0])),
        constraints(1, Some(&[0xff, 0x7f, 0xff, 0xff, 0xff]))];
    for value in values {
        assert_eq!(decode(&[extension(false, &value)]).path_length(), -1);
        assert_eq!(CertificateBasicConstraints::decode(&certificate(&[extension(true, &value)]), &MemoryBudget::new(1 << 20)).unwrap_err().code(), "rtdb.tls/invalid-keystore");
    }
}

#[test]
fn duplicate_parsed_non_ca_constraints_fail_but_malformed_copies_do_not_replace_them() {
    let good = constraints(1, Some(&[4]));
    let bad = encoded(5, &[]);
    for values in [vec![extension(false, &bad), extension(false, &good)], vec![extension(false, &good), extension(false, &bad)]] {
        assert_eq!(decode(&values).path_length(), 4);
    }
    for good in [good, encoded(0x30, &[]), constraints(0, None), constraints(1, Some(&[0xff]))] {
        assert!(CertificateBasicConstraints::decode(&certificate(&[extension(false, &good), extension(false, &good)]), &MemoryBudget::new(1 << 20)).is_err());
    }
}

#[test]
fn basic_constraints_ber_framing_and_reservation_lifetimes_follow_the_shared_scanner() {
    let value = [vec![0x30, 0x80], encoded(1, &[1]), vec![2, 0x81, 1, 7], vec![0, 0]].concat();
    let bytes = certificate(&[extension(true, &value)]);
    let memory = MemoryBudget::new(1 << 20);
    let parsed = CertificateBasicConstraints::decode(&bytes, &memory).unwrap();
    assert_eq!(parsed.path_length(), 7);
    let retained = memory.used_bytes(); assert!(retained > bytes.len());
    let absent = CertificateBasicConstraints::decode(&certificate(&[]), &memory).unwrap();
    assert!(memory.used_bytes() > retained);
    drop(absent); assert_eq!(memory.used_bytes(), retained);
    assert!(CertificateBasicConstraints::decode(&certificate(&[extension(true, &[5, 0])]), &memory).is_err());
    assert_eq!(memory.used_bytes(), retained);
    drop(parsed); assert_eq!(memory.used_bytes(), 0);
    assert_eq!(CertificateBasicConstraints::decode(&[0xff], &MemoryBudget::new(0)).unwrap_err().code(), "rtdb/memory-budget-exceeded");
}
