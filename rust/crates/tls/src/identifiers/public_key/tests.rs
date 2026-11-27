use super::*;

const UNKNOWN: &[u8] = &[0x2a, 3, 4, 100];
const RSA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 1];
const PSS: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 10];
fn spki(oid: &[u8], parameters: &[u8], bits: &[u8]) -> Vec<u8> {
    encoded(0x30, &[encoded(0x30, &[encoded(6, oid), parameters.to_vec()].concat()), encoded(3, bits)].concat())
}
fn certificate(key: &[u8]) -> Vec<u8> {
    let mut signed = encoded(0xa0, &encoded(2, &[2]));
    signed.extend(encoded(2, &[1]));
    for _ in 0..4 { signed.extend(encoded(0x30, &[])); }
    signed.extend_from_slice(key);
    encoded(0x30, &[encoded(0x30, &signed), encoded(0x30, &[]), encoded(3, &[0])].concat())
}
fn read(key: &[u8]) -> TlsResult<CertificatePublicKeyEncoding> {
    CertificatePublicKeyEncoding::decode(&certificate(key), &MemoryBudget::new(16 << 20))
}
fn decode(oid: &[u8], parameters: &[u8], bits: &[u8]) -> CertificatePublicKeyEncoding {
    read(&spki(oid, parameters, bits)).unwrap()
}

#[test]
fn nonnull_parameter_outer_lengths_normalize_while_nested_definite_bytes_remain_opaque() {
    let canonical = decode(UNKNOWN, &[0x30, 3, 2, 1, 1], &[0, 42]);
    assert_eq!(canonical, decode(UNKNOWN, &[0x30, 0x81, 3, 2, 1, 1], &[0, 42]));
    assert_eq!(canonical, decode(UNKNOWN, &[0x30, 0x84, 0, 0, 0, 3, 2, 1, 1], &[0, 42]));
    assert_eq!(canonical, decode(UNKNOWN, &[0x30, 0x80, 2, 1, 1, 0, 0], &[0, 42]));
    assert_ne!(canonical, decode(UNKNOWN, &[0x30, 4, 2, 0x81, 1, 1], &[0, 42]));
    assert_eq!(decode(UNKNOWN, &[0x30, 4, 0x30, 0x80, 0, 0], &[0]).encoded(), spki(UNKNOWN, &[0x30, 4, 0x30, 0x80, 0, 0], &[0]));
    assert_eq!(decode(UNKNOWN, &[0x30, 0x80, 0x30, 0x80, 0, 0, 0, 0], &[0]).encoded(), spki(UNKNOWN, &[0x30, 2, 0x30, 0], &[0]));
}

#[test]
fn indefinite_parameter_conversion_keeps_java_padding_and_scanned_primitive_sections() {
    let padding = decode(UNKNOWN, &[0x30, 0x80, 0x30, 0x81, 3, 2, 1, 1, 0, 0], &[0]);
    assert_eq!(padding.encoded(), spki(UNKNOWN, &[0x30, 6, 0x30, 3, 2, 1, 1, 0], &[0]));
    let scanned = decode(UNKNOWN, &[0x30, 0x80, 4, 0x80, 0, 0, 0, 0], &[0]);
    assert_eq!(scanned.encoded(), spki(UNKNOWN, &[0x30, 2, 4, 0], &[0]));
    assert!(read(&spki(UNKNOWN, &[4, 0x80, 0, 0], &[0])).is_err());
}

#[test]
fn parameters_require_exactly_one_value_and_null_requires_empty_content() {
    for parameters in [vec![5, 1, 0], vec![5, 0, 5, 0], vec![2, 1, 1, 5, 0], vec![0x1f, 0], vec![0x30],
        vec![0x30, 0x80, 2, 1, 1], vec![0x30, 0x85, 0, 0, 0, 0, 0]] {
        assert!(read(&spki(UNKNOWN, &parameters, &[0])).is_err(), "{parameters:?}");
    }
    assert_ne!(decode(UNKNOWN, &[0x25, 0], &[0]), decode(UNKNOWN, &[], &[0]));
    assert_ne!(decode(PSS, &[0x30, 0], &[0]), decode(PSS, &[], &[0]));
}

#[test]
fn algorithm_id_reemits_null_only_for_the_pinned_oid_set() {
    let mut null_oids = vec![RSA.to_vec(), vec![0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 2, 2],
        vec![0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 2, 5], vec![0x2b, 0x0e, 3, 2, 0x1a]];
    for id in [2, 4, 5, 11, 12, 13, 14, 15, 16] { null_oids.push(vec![0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, id]); }
    for id in 1..=10 { null_oids.push(vec![0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, id]); }
    for id in 13..=16 { null_oids.push(vec![0x60, 0x86, 0x48, 1, 0x65, 3, 4, 3, id]); }
    assert_eq!(null_oids.len(), 27);
    for oid in null_oids {
        let absent = decode(&oid, &[], &[0, 42]);
        assert_eq!(absent.encoded(), spki(&oid, &[5, 0], &[0, 42]));
        for count in 0..=4 {
            let null = if count == 0 { vec![5, 0] } else { [vec![5, 0x80 | count], vec![0; usize::from(count)]].concat() };
            assert_eq!(absent, decode(&oid, &null, &[0, 42]));
        }
    }
    for oid in [UNKNOWN.to_vec(), PSS.to_vec(), vec![0x2b, 0x0e, 3, 2, 0x1d], vec![0x55, 8, 1, 1], vec![0x2b, 0x65, 0x70],
        vec![0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, 11], vec![0x60, 0x86, 0x48, 1, 0x65, 3, 4, 3, 12]] {
        assert_eq!(decode(&oid, &[5, 0], &[0]).encoded(), spki(&oid, &[], &[0]));
    }
}

#[test]
fn object_identifier_validation_retains_valid_base128_content_and_size_limit() {
    for oid in [vec![], vec![0x80, 1], vec![0x2a, 0x80, 0], vec![0x2a, 0x81], vec![0; 4097]] {
        assert!(read(&spki(&oid, &[], &[0])).is_err(), "{}", oid.len());
    }
    for oid in [vec![0], vec![0x2a, 0x81, 0x80, 0], vec![0xff, 0xff, 0x7f], vec![0; 4096]] {
        assert_eq!(decode(&oid, &[], &[0]).encoded(), spki(&oid, &[], &[0]));
    }
}

#[test]
fn oid_and_algorithm_framing_accept_ber_lengths_and_reject_wrong_tags_or_extra_values() {
    let algorithm = [vec![6, 0x81, UNKNOWN.len() as u8], UNKNOWN.to_vec(), vec![5, 0x81, 0]].concat();
    let key = encoded(0x30, &[vec![0x30, 0x81, algorithm.len() as u8], algorithm, encoded(3, &[0, 42])].concat());
    assert_eq!(read(&key).unwrap(), decode(UNKNOWN, &[], &[0, 42]));
    for algorithm in [encoded(0x31, &encoded(6, UNKNOWN)), encoded(0x30, &encoded(4, UNKNOWN)),
        encoded(0x30, &[encoded(6, UNKNOWN), encoded(5, &[]), encoded(2, &[1])].concat())] {
        assert!(read(&encoded(0x30, &[algorithm, encoded(3, &[0])].concat())).is_err());
    }
}

#[test]
fn unaligned_bit_strings_clear_padding_and_preserve_the_declared_bit_count() {
    for padding in 0..=7 {
        let value = decode(UNKNOWN, &[], &[padding, 0xaa, 0xff]);
        let expected = spki(UNKNOWN, &[], &[padding, 0xaa, 0xff << padding]);
        assert_eq!(value.encoded(), expected);
        assert_eq!(value, read(&expected).unwrap());
        if padding != 0 { assert_ne!(value, decode(UNKNOWN, &[], &[0, 0xaa, 0xff << padding])); }
    }
    assert_eq!(decode(UNKNOWN, &[], &[0]).encoded(), spki(UNKNOWN, &[], &[0]));
    for bits in [vec![], vec![1], vec![7], vec![8, 0], vec![0xff, 0]] {
        assert!(read(&spki(UNKNOWN, &[], &bits)).is_err(), "{bits:?}");
    }
}

#[test]
fn bit_array_integer_wrapping_and_backing_tail_checks_match_java_before_allocation() {
    assert_eq!(bit_layout(0, 268_435_455, 268_435_455).unwrap(), (268_435_455, 0));
    assert!(bit_layout(0, 268_435_456, 268_435_456).is_err());
    assert!(bit_layout(1, 268_435_456, 268_435_456).is_err());
    assert_eq!(bit_layout(0, 536_870_912, 536_870_912).unwrap(), (0, 0));
    assert!(bit_layout(1, 536_870_912, 536_870_912).is_err());
    assert_eq!(bit_layout(7, 536_870_913, 536_870_913).unwrap(), (1, 7));
    assert!(bit_layout(0, 1, 268_435_456).is_err());
    assert!(bit_layout(0, 1, 536_870_912).is_err());
    assert_eq!(bit_layout(0, 1, 536_870_913).unwrap(), (1, 0));
    assert!(bit_layout(0, 0, 268_435_456).is_err());
}

#[test]
fn key_framing_rejects_constructed_bit_strings_and_trailing_spki_fields() {
    let algorithm = encoded(0x30, &encoded(6, UNKNOWN));
    for bits in [encoded(0x23, &encoded(3, &[0])), vec![3, 0x80, 0, 0], encoded(4, &[0]),
        [encoded(3, &[0]), vec![5, 0]].concat()] {
        assert!(read(&encoded(0x30, &[algorithm.clone(), bits].concat())).is_err());
    }
}

#[test]
fn indefinite_spki_algorithm_and_certificate_containers_use_the_shared_java_converter() {
    let algorithm = [vec![0x30, 0x80], encoded(6, UNKNOWN), encoded(5, &[]), vec![0, 0]].concat();
    let key = [vec![0x30, 0x80], algorithm, encoded(3, &[3, 0xff]), vec![0, 0]].concat();
    let expected = decode(UNKNOWN, &[], &[3, 0xf8]);
    assert_eq!(read(&key).unwrap(), expected);
    let mut fields = encoded(2, &[1]);
    for _ in 0..4 { fields.extend(encoded(0x30, &[])); }
    fields.extend(key);
    let signed = [vec![0x30, 0x80], fields, vec![0, 0]].concat();
    let certificate = [vec![0x30, 0x80], signed, encoded(0x30, &[]), encoded(3, &[0]), vec![0, 0]].concat();
    assert_eq!(CertificatePublicKeyEncoding::decode(&certificate, &MemoryBudget::new(16 << 20)).unwrap(), expected);
}

#[test]
fn deeply_nested_indefinite_parameters_use_bounded_iterative_scratch() {
    let mut parameter = Vec::new();
    for _ in 0..1024 { parameter.extend_from_slice(&[0x30, 0x80]); }
    for _ in 0..1024 { parameter.extend_from_slice(&[0, 0]); }
    let memory = MemoryBudget::new(4 << 20);
    let parsed = CertificatePublicKeyEncoding::decode(&certificate(&spki(UNKNOWN, &parameter, &[0])), &memory).unwrap();
    assert!(parsed.encoded().len() < parameter.len());
    assert!(memory.used_bytes() < 16_384);
    drop(parsed); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn public_key_encoding_owns_retained_bytes_and_releases_scratch_and_failures() {
    let memory = MemoryBudget::new(1 << 20);
    let bytes = certificate(&spki(UNKNOWN, &[0x30, 0x80, 2, 1, 1, 0, 0], &[3, 0xff]));
    let first = CertificatePublicKeyEncoding::decode(&bytes, &memory).unwrap();
    let retained = memory.used_bytes();
    assert!(retained >= first.encoded().len());
    assert!(retained < 1024);
    let second = CertificatePublicKeyEncoding::decode(&bytes, &memory).unwrap();
    drop(bytes);
    assert_eq!(first, second);
    assert_eq!(memory.used_bytes(), 2 * retained);
    drop(second); assert_eq!(memory.used_bytes(), retained);
    assert!(CertificatePublicKeyEncoding::decode(&certificate(&spki(UNKNOWN, &[5, 1, 0], &[0])), &memory).is_err());
    assert_eq!(memory.used_bytes(), retained);
    drop(first); assert_eq!(memory.used_bytes(), 0);
    assert_eq!(CertificatePublicKeyEncoding::decode(&[0xff], &MemoryBudget::new(0)).unwrap_err().code(), "rtdb/memory-budget-exceeded");
}
