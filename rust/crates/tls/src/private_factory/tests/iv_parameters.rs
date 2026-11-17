use super::*;

const AES: &[u8] = &[0x60, 0x86, 0x48, 1, 0x65, 3, 4, 1];
const DESEDE: &[u8] = &[0x2b, 0x0e, 3, 2, 17];
const BLOWFISH: &[u8] = &[0x2b, 6, 1, 4, 1, 0x97, 0x55, 1, 1, 2];
const CHACHA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 9, 16, 3, 18];
const SERVICES: &[(&[u8], usize)] = &[(AES, 16), (DESEDE, 8), (BLOWFISH, 8), (CHACHA, 12)];

fn nested_key(oid: &[u8], parameter: &[u8]) -> Vec<u8> {
    key(PSS, &pss(&[der(0xa0, &algorithm(oid, parameter))]), &rsa_material(&components()), &[0], &[])
}

#[test]
fn pss_nested_iv_services_admit_primitive_parameters_and_retain_crt_encodings() {
    for &(oid, length) in SERVICES {
        let parameter = der(4, &vec![0x5a; length]);
        let input = nested_key(oid, &parameter);
        assert_eq!(decoded(&input), input);
        let parameter = [vec![4, 0x81, length as u8], vec![0x5a; length]].concat();
        let input = nested_key(oid, &parameter);
        assert_eq!(decoded(&input), input);
        for parameter in [vec![], der(5, &[])] {
            let input = nested_key(oid, &parameter);
            assert_eq!(decoded(&input), input);
        }
    }
}

#[test]
fn pss_nested_block_iv_services_reject_constructed_forms_and_alternate_sizes() {
    for &(oid, length) in &SERVICES[..3] {
        for tag in [0x24, 0xa4, 0xe4] {
            assert!(decode(&nested_key(oid, &der(tag, &der(4, &vec![0; length])))).is_err());
        }
        let indefinite = [vec![0x24, 0x80], der(4, &vec![0; length]), vec![0, 0]].concat();
        assert!(decode(&nested_key(oid, &indefinite)).is_err());
        for size in [0, 4, 8, 12, 15, 16, 17] {
            assert_eq!(decode(&nested_key(oid, &der(4, &vec![0; size]))).is_ok(), size == length);
        }
        assert!(decode(&nested_key(oid, &der(0x84, &vec![0; length]))).is_err());
    }
}

#[test]
fn pss_nested_chacha_nonce_flattens_constructed_octets_and_rejects_invalid_components() {
    for tag in [0x24, 0xa4, 0xe4] {
        let pieces = [der(4, &[1; 5]), der(0x24, &der(4, &[2; 7]))].concat();
        for parameter in [der(tag, &pieces), [vec![tag, 0x80], pieces, vec![0, 0]].concat()] {
            let input = nested_key(CHACHA, &parameter);
            assert!(decode(&input).is_ok());
        }
    }
    for parameter in [der(4, &[0; 11]), der(4, &[0; 13]), der(0x24, &der(2, &[0; 12])),
        der(0x24, &der(0x84, &[0; 12])), der(0x24, &[der(4, &[0; 12]), vec![0]].concat()), der(0x24, &[])] {
        assert!(decode(&nested_key(CHACHA, &parameter)).is_err());
    }
}

#[test]
fn pss_nested_iv_services_reject_extra_parameter_values_and_non_octet_encodings() {
    for &(oid, length) in SERVICES {
        for parameter in [[der(4, &vec![0; length]), der(5, &[])].concat(), der(0x30, &der(4, &vec![0; length])),
            der(2, &vec![0; length]), der(5, &[0])] {
            assert!(decode(&nested_key(oid, &parameter)).is_err());
        }
    }
}

#[test]
fn pss_noncrt_regeneration_discards_nested_iv_parameters_after_validation() {
    for &(oid, length) in SERVICES {
        let mut values = components(); values[3] = vec![0];
        let parameter = pss(&[der(0xa0, &algorithm(oid, &der(4, &vec![42; length])))]);
        let input = key(PSS, &parameter, &rsa_material(&values), &[0], &[]);
        values[0].insert(0, 0);
        for index in [1, 3, 4, 5, 6, 7] { values[index] = vec![0]; }
        let expected = key(PSS, &pss(&[der(0xa0, &algorithm(oid, &[]))]), &rsa_material(&values), &[0], &[]);
        assert_eq!(decoded(&input), expected);
    }
}

#[test]
fn cipher_specific_oids_without_parameter_services_keep_opaque_hash_parameters() {
    for oid in [&[0x2b, 0x0e, 3, 2, 7][..], &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 3, 2], &[0x60, 0x86, 0x48, 1, 0x65, 3, 4, 1, 6]] {
        let input = nested_key(oid, &der(2, &[1]));
        assert_eq!(decoded(&input), input);
    }
}

#[test]
fn constructed_nonce_validation_uses_the_factory_reservation_and_releases_failures() {
    let bytes = nested_key(CHACHA, &der(0x24, &der(4, &[0; 12])));
    let insufficient = MemoryBudget::new(allocation_bound(bytes.len()).unwrap() - 1);
    assert_eq!(PrivateKey::decode(&bytes, &insufficient).err().unwrap().code, "rtdb/memory-budget-exceeded");
    assert_eq!(insufficient.used_bytes(), 0);
    let memory = MemoryBudget::new(1 << 20);
    let key = PrivateKey::decode(&bytes, &memory).unwrap();
    assert!(memory.used_bytes() > 0); assert_eq!(key.encoded_pkcs8(), bytes);
    drop(key); assert_eq!(memory.used_bytes(), 0);
    assert!(PrivateKey::decode(&nested_key(CHACHA, &der(0x24, &der(2, &[0; 12]))), &memory).is_err());
    assert_eq!(memory.used_bytes(), 0);
}
