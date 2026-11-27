use super::*;

fn extension(critical: bool, value: &[u8]) -> Vec<u8> {
    encoded(0x30, &[encoded(6, &[0x55, 0x1d, 15]), encoded(1, &[u8::from(critical)]), encoded(4, value)].concat())
}
fn certificate(extensions: &[Vec<u8>]) -> Vec<u8> {
    let mut tbs = vec![encoded(0xa0, &encoded(2, &[2])), encoded(2, &[1])];
    tbs.extend((0..5).map(|_| encoded(0x30, &[])));
    tbs.push(encoded(0xa3, &encoded(0x30, &extensions.concat())));
    encoded(0x30, &[encoded(0x30, &tbs.concat()), encoded(0x30, &[]), encoded(3, &[0])].concat())
}
fn decode(extensions: &[Vec<u8>]) -> CertificateKeyUsage {
    CertificateKeyUsage::decode(&certificate(extensions), &MemoryBudget::new(16 << 20)).unwrap()
}

#[test]
fn ca_key_usage_accepts_absence_and_requires_bit_five_when_parsed() {
    assert!(decode(&[]).permits_certificate_signing());
    for (bits, expected) in [(vec![0], false), (vec![0, 0x80], false), (vec![0, 4], true), (vec![0, 0, 4], false), (vec![0, 0xff], true)] {
        for critical in [false, true] {
            assert_eq!(decode(&[extension(critical, &encoded(3, &bits))]).permits_certificate_signing(), expected);
        }
    }
}

#[test]
fn java_key_usage_ignores_nonzero_padding_and_pads_short_arrays_with_false() {
    for padding in 0..=7 {
        let value = encoded(3, &[padding, 0xff]);
        assert_eq!(decode(&[extension(true, &value)]).permits_certificate_signing(), padding <= 2);
    }
    assert!(!decode(&[extension(true, &encoded(3, &[7, 0x04]))]).permits_certificate_signing());
}

#[test]
fn key_usage_accepts_exactly_one_primitive_octet_wrapper() {
    let bits = encoded(3, &[2, 4]);
    assert!(decode(&[extension(true, &encoded(4, &bits))]).permits_certificate_signing());
    let wrapped = [vec![4, 0x81, bits.len() as u8], bits.clone()].concat();
    assert!(decode(&[extension(true, &wrapped)]).permits_certificate_signing());
    for value in [encoded(4, &encoded(4, &bits)), encoded(0x24, &encoded(4, &bits)), [encoded(4, &bits), vec![0]].concat()] {
        assert!(decode(&[extension(false, &value)]).permits_certificate_signing());
        assert!(CertificateKeyUsage::decode(&certificate(&[extension(true, &value)]), &MemoryBudget::new(1 << 20)).is_err());
    }
}

#[test]
fn malformed_key_usage_is_absent_only_when_noncritical() {
    for value in [vec![], encoded(3, &[]), encoded(3, &[1]), encoded(3, &[8, 0]), encoded(3, &[0xff, 4]),
        encoded(5, &[]), [encoded(3, &[0, 4]), vec![0]].concat(), vec![3, 0x80, 0, 4, 0, 0]] {
        assert!(decode(&[extension(false, &value)]).permits_certificate_signing());
        assert_eq!(CertificateKeyUsage::decode(&certificate(&[extension(true, &value)]), &MemoryBudget::new(1 << 20)).unwrap_err().code(), "rtdb.tls/invalid-keystore");
    }
}

#[test]
fn duplicate_key_usage_counts_parsed_false_values_and_preserves_good_copies() {
    let bad = encoded(5, &[]);
    for good in [encoded(3, &[0]), encoded(3, &[0, 4])] {
        let expected = good.len() > 3;
        for values in [vec![extension(false, &bad), extension(false, &good)], vec![extension(false, &good), extension(false, &bad)]] {
            assert_eq!(decode(&values).permits_certificate_signing(), expected);
        }
        assert!(CertificateKeyUsage::decode(&certificate(&[extension(false, &good), extension(false, &good)]), &MemoryBudget::new(1 << 20)).is_err());
    }
}

#[test]
fn key_usage_reservations_cover_success_absence_and_error_until_drop() {
    let memory = MemoryBudget::new(1 << 20);
    let bytes = certificate(&[extension(true, &[3, 0x81, 2, 2, 4])]);
    let parsed = CertificateKeyUsage::decode(&bytes, &memory).unwrap();
    assert!(parsed.permits_certificate_signing());
    let retained = memory.used_bytes(); assert!(retained > bytes.len());
    let absent = CertificateKeyUsage::decode(&certificate(&[]), &memory).unwrap();
    assert!(memory.used_bytes() > retained);
    drop(absent); assert_eq!(memory.used_bytes(), retained);
    assert!(CertificateKeyUsage::decode(&certificate(&[extension(true, &[5, 0])]), &memory).is_err());
    assert_eq!(memory.used_bytes(), retained);
    drop(parsed); assert_eq!(memory.used_bytes(), 0);
    assert_eq!(CertificateKeyUsage::decode(&[0xff], &MemoryBudget::new(0)).unwrap_err().code(), "rtdb/memory-budget-exceeded");
}

#[test]
fn java_usage_decisions_mask_every_bit_beyond_the_declared_length() {
    for byte in 0..=255u8 {
        for padding in 0..=7u8 {
            let usage = decode(&[extension(true, &encoded(3, &[padding, byte]))]);
            assert_eq!(usage.permitted_bits(), byte & (0xff << padding));
            assert_eq!(usage.permits_digital_signatures(), byte & 0x80 != 0);
            assert_eq!(usage.permits_certificate_signing(), padding <= 2 && byte & 4 != 0);
            let primitive = (0..7u8).any(|bit| bit < 8 - padding && byte & (0x80 >> bit) != 0);
            assert_eq!(usage.permits_cryptographic_primitive(), primitive);
        }
    }
}

#[test]
fn algorithm_primitives_exclude_isolated_encipher_only_and_decipher_only() {
    for (bits, signature, ca, primitive) in [
        (vec![0], false, false, false),
        (vec![0, 0x80], true, false, true),
        (vec![0, 0x40], false, false, true),
        (vec![0, 0x20], false, false, true),
        (vec![0, 0x10], false, false, true),
        (vec![0, 0x08], false, false, true),
        (vec![0, 0x04], false, true, true),
        (vec![0, 0x02], false, false, true),
        (vec![0, 0x01], false, false, false),
        (vec![0, 0, 0x80], false, false, false),
        (vec![0, 0x09, 0x80], false, false, true),
    ] {
        let usage = decode(&[extension(true, &encoded(3, &bits))]);
        assert_eq!(usage.permits_digital_signatures(), signature);
        assert_eq!(usage.permits_certificate_signing(), ca);
        assert_eq!(usage.permits_cryptographic_primitive(), primitive);
    }
    for extensions in [vec![], vec![extension(false, &[5, 0])]] {
        let usage = decode(&extensions);
        assert!(usage.permits_digital_signatures());
        assert!(usage.permits_certificate_signing());
        assert!(usage.permits_cryptographic_primitive());
    }
}

#[test]
fn usage_decisions_keep_wrapper_and_duplicate_semantics() {
    let wrapped = encoded(4, &[3, 0x81, 2, 6, 0xff]);
    for extensions in [
        vec![extension(true, &wrapped)],
        vec![extension(false, &[5, 0]), extension(false, &wrapped)],
        vec![extension(false, &wrapped), extension(false, &[5, 0])],
    ] {
        let usage = decode(&extensions);
        assert!(usage.permits_digital_signatures());
        assert!(!usage.permits_certificate_signing());
        assert!(usage.permits_cryptographic_primitive());
    }
}

#[test]
fn standalone_key_usage_array_capacity_uses_java_signed_wrapping() {
    assert_eq!(declared_bit_length(0, 0).unwrap(), 0);
    assert!(declared_bit_length(1, 0).is_err());
    for padding in 0..=7 {
        assert_eq!(declared_bit_length(padding, 268_435_455).unwrap(), 2_147_483_640 - usize::from(padding));
        assert!(declared_bit_length(padding, 268_435_456).is_err());
        assert!(declared_bit_length(padding, 536_870_911).is_err());
        assert_eq!(declared_bit_length(padding, 536_870_913).unwrap(), 8 - usize::from(padding));
    }
    assert_eq!(declared_bit_length(0, 536_870_912).unwrap(), 0);
    assert!(declared_bit_length(1, 536_870_912).is_err());
    assert!(declared_bit_length(8, 1).is_err());
    assert!(declared_bit_length(0, i32::MAX as usize + 1).is_err());
}

#[test]
fn permitted_bits_distinguish_absent_usage_from_an_empty_parsed_value() {
    assert_eq!(decode(&[]).permitted_bits(), 0xff);
    assert_eq!(decode(&[extension(false, &[5, 0])]).permitted_bits(), 0xff);
    assert_eq!(decode(&[extension(false, &encoded(3, &[0]))]).permitted_bits(), 0);
}
