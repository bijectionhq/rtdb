use super::*;

const PKCS3: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 3, 1];
const X942: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3e, 2, 1];
const UNKNOWN: &[u8] = &[0x2a, 3, 4, 100];
fn parameter(p: &[u8], g: &[u8], length: Option<&[u8]>) -> Vec<u8> {
    let mut fields = [encoded(2, p), encoded(2, g)].concat();
    if let Some(length) = length { fields.extend(encoded(2, length)); }
    encoded(0x30, &fields)
}
fn certificate(oid: &[u8], parameters: &[u8], bits: &[u8]) -> Vec<u8> {
    let key = encoded(0x30, &[encoded(0x30, &[encoded(6, oid), parameters.to_vec()].concat()), encoded(3, bits)].concat());
    let mut fields = encoded(2, &[1]);
    for _ in 0..4 { fields.extend(encoded(0x30, &[])); }
    fields.extend(key);
    encoded(0x30, &[encoded(0x30, &fields), encoded(0x30, &[]), encoded(3, &[0])].concat())
}
fn read(oid: &[u8], parameters: &[u8], bits: &[u8]) -> CertificatePublicKeyEncoding {
    CertificatePublicKeyEncoding::decode(&certificate(oid, parameters, bits), &MemoryBudget::new(32 << 20)).unwrap()
}
fn key(oid: &[u8], p: &[u8], g: &[u8], y: &[u8], length: Option<&[u8]>) -> CertificatePublicKeyEncoding {
    read(oid, &parameter(p, g, length), &[vec![0], encoded(2, y)].concat())
}

#[test]
fn dh_numeric_equality_crosses_registered_names_and_ignores_private_value_length() {
    let first = key(PKCS3, &[23], &[5], &[8], None);
    for oid in [PKCS3, X942] {
        for length in [None, Some(&[0][..]), Some(&[7][..]), Some(&[0xff][..]), Some(&[0, 0, 9][..])] {
            let second = key(oid, &[0, 23], &[0, 5], &[0, 8], length);
            assert_ne!(first, second);
            assert!(first.same_public_key(&second).unwrap());
            assert!(second.same_public_key(&first).unwrap());
        }
    }
}

#[test]
fn signed_numeric_values_keep_zero_negative_and_large_integer_distinctions() {
    for (minimal, padded) in [(vec![0], vec![0, 0, 0]), (vec![0xff], vec![0xff, 0xff, 0xff]),
        (vec![0x80], vec![0xff, 0xff, 0x80]), (vec![0, 0x80], vec![0, 0, 0, 0x80]),
        ([vec![0], vec![0xff; 257]].concat(), [vec![0; 4], vec![0xff; 257]].concat()),
        ([vec![0x80], vec![0; 257]].concat(), [vec![0xff; 4], vec![0x80], vec![0; 257]].concat())] {
        let first = key(PKCS3, &minimal, &minimal, &minimal, None);
        assert!(first.same_public_key(&key(X942, &padded, &padded, &padded, None)).unwrap());
        for (p, g, y) in [(&[1][..], minimal.as_slice(), minimal.as_slice()),
            (minimal.as_slice(), &[1][..], minimal.as_slice()), (minimal.as_slice(), minimal.as_slice(), &[1][..])] {
            assert!(!first.same_public_key(&key(PKCS3, p, g, y, None)).unwrap());
        }
    }
    assert!(!key(PKCS3, &[0x80], &[5], &[8], None).same_public_key(&key(PKCS3, &[0, 0x80], &[5], &[8], None)).unwrap());
}

#[test]
fn private_value_length_accepts_the_signed_int_range_after_ber_sign_padding() {
    let first = key(PKCS3, &[23], &[5], &[8], None);
    for length in [vec![0x80, 0, 0, 0], vec![0x7f, 0xff, 0xff, 0xff], vec![0xff, 0xff, 0x80, 0, 0, 0],
        vec![0, 0, 0x7f, 0xff, 0xff, 0xff], vec![0; 32], vec![0xff; 32]] {
        assert!(first.same_public_key(&key(X942, &[23], &[5], &[8], Some(&length))).unwrap());
    }
    for length in [vec![], vec![0, 0x80, 0, 0, 0], vec![0xff, 0x7f, 0xff, 0xff, 0xff], vec![1; 8]] {
        let bad = key(PKCS3, &[23], &[5], &[8], Some(&length));
        assert!(first.same_public_key(&bad).is_err());
        assert!(bad.same_public_key(&first).is_err());
        assert!(bad.same_public_key(&bad).is_err());
    }
}

#[test]
fn dh_big_integers_accept_ber_lengths_and_ignore_bytes_after_the_first_y_integer() {
    let first = key(PKCS3, &[23], &[5], &[8], None);
    let parameters = encoded(0x30, &[2, 0x81, 2, 0, 23, 2, 0x82, 0, 2, 0, 5]);
    for tail in [vec![], vec![0xff], encoded(2, &[99]), vec![0x30, 0x80]] {
        let bits = [vec![0, 2, 0x81, 2, 0, 8], tail].concat();
        assert!(first.same_public_key(&read(X942, &parameters, &bits)).unwrap());
    }
    let zero = key(PKCS3, &[23], &[5], &[0], None);
    assert!(zero.same_public_key(&read(PKCS3, &parameter(&[23], &[5], None), &[7, 2, 1, 0x7f])).unwrap());
    assert!(first.same_public_key(&read(PKCS3, &parameter(&[23], &[5], None), &[7, 2, 1, 8, 0xff])).unwrap());
}

#[test]
fn dh_parameters_require_p_g_and_at_most_one_signed_int_without_extra_fields() {
    let good = key(PKCS3, &[23], &[5], &[8], None);
    for parameters in [vec![], vec![5, 0], encoded(4, &[]), encoded(0x30, &[]), encoded(0x30, &encoded(2, &[23])),
        parameter(&[], &[5], None), parameter(&[23], &[], None),
        encoded(0x30, &[encoded(2, &[23]), encoded(2, &[5]), encoded(5, &[])].concat()),
        encoded(0x30, &[encoded(2, &[23]), encoded(2, &[5]), encoded(2, &[8]), encoded(2, &[9])].concat()),
        encoded(0x30, &[vec![0x30, 0x80, 0, 0], encoded(2, &[5])].concat())] {
        for oid in [PKCS3, X942] {
            let bad = read(oid, &parameters, &[0, 2, 1, 8]);
            assert!(bad.same_public_key(&bad).is_err(), "{parameters:?}");
            assert!(good.same_public_key(&bad).is_err());
            assert!(bad.same_public_key(&good).is_err());
        }
    }
}

#[test]
fn dh_y_must_start_with_a_nonempty_integer_without_provider_fallback() {
    let unknown = key(UNKNOWN, &[23], &[5], &[8], None);
    for bits in [vec![0], vec![0, 2, 0], vec![0, 4, 1, 8], vec![0, 2, 2, 8], vec![0, 2, 0x80, 0, 0], vec![0, 0x30, 0x80, 0, 0]] {
        let bad = read(PKCS3, &parameter(&[23], &[5], None), &bits);
        assert_eq!(bad, bad);
        assert_eq!(bad.same_public_key(&bad).unwrap_err().code(), "rtdb.tls/invalid-keystore");
        assert!(bad.same_public_key(&unknown).is_err());
        assert!(unknown.same_public_key(&bad).is_err());
    }
}

#[test]
fn non_dh_keys_preserve_encoded_equality_and_remain_distinct_from_numeric_dh() {
    let first = key(UNKNOWN, &[23], &[5], &[8], None);
    let second = key(UNKNOWN, &[0, 23], &[5], &[8], None);
    assert_ne!(first, second);
    assert!(!first.same_public_key(&second).unwrap());
    assert!(first.same_public_key(&first).unwrap());
    let dh = key(PKCS3, &[23], &[5], &[8], None);
    assert!(!first.same_public_key(&dh).unwrap());
    assert!(!dh.same_public_key(&first).unwrap());
}

#[test]
fn numeric_comparison_borrows_its_owners_without_new_admission_or_retained_bytes() {
    let memory = MemoryBudget::new(1 << 20);
    let left = CertificatePublicKeyEncoding::decode(&certificate(PKCS3, &parameter(&[23], &[5], None), &[0, 2, 1, 8]), &memory).unwrap();
    let right = CertificatePublicKeyEncoding::decode(&certificate(X942, &parameter(&[0, 23], &[5], Some(&[7])), &[0, 2, 2, 0, 8]), &memory).unwrap();
    let bad = CertificatePublicKeyEncoding::decode(&certificate(PKCS3, &parameter(&[23], &[5], None), &[0]), &memory).unwrap();
    let retained = memory.used_bytes(); let peak = memory.peak_bytes();
    for _ in 0..16 {
        assert!(left.same_public_key(&right).unwrap());
        assert!(left.same_public_key(&bad).is_err());
        assert_eq!(memory.used_bytes(), retained);
        assert_eq!(memory.peak_bytes(), peak);
    }
    drop(left); drop(right); drop(bad); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn java_big_integer_magnitude_limit_handles_virtual_signed_boundary_values() {
    assert!(canonical_magnitude_in_range(268_435_455, 0x80, || panic!("shorter magnitude needs no tail scan")));
    assert!(canonical_magnitude_in_range(268_435_456, 0x7f, || panic!("positive boundary needs no tail scan")));
    assert!(!canonical_magnitude_in_range(268_435_457, 0, || panic!("positive sign byte already exceeds range")));
    assert!(!canonical_magnitude_in_range(268_435_456, 0x80, || true));
    assert!(canonical_magnitude_in_range(268_435_456, 0x80, || false));
    assert!(canonical_magnitude_in_range(268_435_456, 0x81, || panic!("smaller negative magnitude needs no tail scan")));
    assert!(!canonical_magnitude_in_range(268_435_457, 0xff, || panic!("negative boundary already exceeds range")));
    assert!(!canonical_magnitude_in_range(268_435_458, 0x80, || panic!("oversized magnitude needs no tail scan")));
}
