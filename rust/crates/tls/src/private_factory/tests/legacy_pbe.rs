use super::*;

const SERVICES: &[&[u8]] = &[
    &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 3],
    &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 12, 1, 1],
    &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 12, 1, 2],
    &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 12, 1, 3],
    &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 12, 1, 5],
    &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 12, 1, 6],
];

fn parameters(salt: &[u8], count: &[u8]) -> Vec<u8> { der(0x30, &[der(4, salt), der(2, count)].concat()) }
fn nested_key(oid: &[u8], parameter: &[u8]) -> Vec<u8> {
    key(PSS, &pss(&[der(0xa0, &algorithm(oid, parameter))]), &rsa_material(&components()), &[0], &[])
}

#[test]
fn pss_nested_legacy_pbe_accepts_arbitrary_salt_lengths_and_retains_crt_parameters() {
    for &oid in SERVICES {
        for size in [0, 1, 7, 8, 9, 32, 256] {
            let input = nested_key(oid, &parameters(&vec![0x5a; size], &[1]));
            assert_eq!(decoded(&input), input);
        }
    }
}

#[test]
fn pss_nested_legacy_pbe_accepts_signed_counts_and_redundant_sign_padding() {
    for &oid in SERVICES {
        for count in [
            vec![0], vec![1], vec![0xff], vec![0x7f, 0xff, 0xff, 0xff], vec![0x80, 0, 0, 0],
            vec![0, 0, 0, 0, 0, 1], vec![0xff, 0xff, 0xff, 0xff, 0xff, 0x80],
            vec![0, 0, 0x7f, 0xff, 0xff, 0xff], vec![0xff, 0xff, 0x80, 0, 0, 0],
        ] {
            let input = nested_key(oid, &parameters(&[], &count));
            assert_eq!(decoded(&input), input);
        }
        for count in [vec![], vec![0, 0x80, 0, 0, 0], vec![0xff, 0x7f, 0xff, 0xff, 0xff], vec![1, 0, 0, 0, 0]] {
            assert!(decode(&nested_key(oid, &parameters(&[], &count))).is_err());
        }
        for tag in [4, 10, 0x22, 0x82] {
            let parameter = der(0x30, &[der(4, &[]), der(tag, &[1])].concat());
            assert!(decode(&nested_key(oid, &parameter)).is_err());
        }
    }
}

#[test]
fn pss_nested_legacy_pbe_rejects_constructed_and_non_octet_salts() {
    for &oid in SERVICES {
        for tag in [0x24, 0xa4, 0xe4] {
            for salt in [der(tag, &der(4, &[0; 8])), [vec![tag, 0x80], der(4, &[0; 8]), vec![0, 0]].concat()] {
                let parameter = der(0x30, &[salt, der(2, &[1])].concat());
                assert!(decode(&nested_key(oid, &parameter)).is_err());
            }
        }
        for salt in [der(0x84, &[0; 8]), der(2, &[0; 8]), der(5, &[]), der(0x30, &der(4, &[0; 8]))] {
            let parameter = der(0x30, &[salt, der(2, &[1])].concat());
            assert!(decode(&nested_key(oid, &parameter)).is_err());
        }
    }
}

#[test]
fn pss_nested_legacy_pbe_accepts_ber_frames_and_requires_exact_sequence_fields() {
    for &oid in SERVICES {
        let fields = [der(4, &[42; 8]), der(2, &[0, 0, 1])].concat();
        let padded_fields = [vec![4, 0x81, 8], vec![42; 8], vec![2, 0x82, 0, 2, 0, 1]].concat();
        for parameter in [
            der(0x30, &fields), [vec![0x30, 0x80], fields.clone(), vec![0, 0]].concat(),
            [vec![0x30, 0x81, fields.len() as u8], fields.clone()].concat(), der(0x30, &padded_fields),
        ] {
            let input = nested_key(oid, &parameter);
            assert_eq!(decoded(&input), input);
        }
        for parameter in [
            der(0x30, &[]), der(0x30, &der(4, &[0; 8])), der(0x30, &der(2, &[1])),
            der(0x30, &[fields.clone(), der(5, &[])].concat()), der(0x30, &[fields.clone(), vec![0]].concat()),
            [der(0x30, &fields), der(5, &[])].concat(), der(0x31, &fields), der(4, &fields),
            [vec![0x30, 0x80], fields.clone()].concat(),
        ] {
            assert!(decode(&nested_key(oid, &parameter)).is_err());
        }
    }
}

#[test]
fn pss_nested_legacy_pbe_skips_absent_and_null_parameters() {
    for &oid in SERVICES {
        for parameter in [vec![], der(5, &[]), vec![5, 0x81, 0]] {
            let input = nested_key(oid, &parameter);
            assert_eq!(decoded(&input), input);
        }
        assert!(decode(&nested_key(oid, &der(5, &[0]))).is_err());
    }
}

#[test]
fn pss_noncrt_regeneration_validates_legacy_pbe_then_retains_only_its_algorithm_name() {
    for &oid in SERVICES {
        let mut values = components(); values[3] = vec![0];
        let parameter = pss(&[der(0xa0, &algorithm(oid, &parameters(&[], &[0xff])))]);
        let input = key(PSS, &parameter, &rsa_material(&values), &[0], &[]);
        let malformed = pss(&[der(0xa0, &algorithm(oid, &parameters(&[], &[0, 0x80, 0, 0, 0])))]);
        assert!(decode(&key(PSS, &malformed, &rsa_material(&values), &[0], &[])).is_err());
        values[0].insert(0, 0);
        for index in [1, 3, 4, 5, 6, 7] { values[index] = vec![0]; }
        let expected = key(PSS, &pss(&[der(0xa0, &algorithm(oid, &[]))]), &rsa_material(&values), &[0], &[]);
        assert_eq!(decoded(&input), expected);
    }
}

#[test]
fn nearby_pbe_oids_without_parameter_services_retain_opaque_values() {
    for oid in [
        &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 4][..],
        &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 6],
        &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 10],
        &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 11],
        &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 12, 1, 0],
        &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 12, 1, 4],
        &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 12, 1, 7],
    ] {
        for parameter in [der(2, &[1]), parameters(&[], &[0, 0x80, 0, 0, 0])] {
            let input = nested_key(oid, &parameter);
            assert_eq!(decoded(&input), input);
        }
    }
}

#[test]
fn legacy_pbe_validation_uses_factory_admission_and_releases_success_and_failure() {
    for &oid in SERVICES {
        let parameter = [vec![0x30, 0x80], der(4, &[42; 8]), der(2, &[0xff]), vec![0, 0]].concat();
        let bytes = nested_key(oid, &parameter);
        let insufficient = MemoryBudget::new(allocation_bound(bytes.len()).unwrap() - 1);
        assert_eq!(PrivateKey::decode(&bytes, &insufficient).err().unwrap().code, "rtdb/memory-budget-exceeded");
        assert_eq!(insufficient.used_bytes(), 0);
        let memory = MemoryBudget::new(1 << 20);
        let key = PrivateKey::decode(&bytes, &memory).unwrap();
        assert!(memory.used_bytes() > 0); assert_eq!(key.encoded_pkcs8(), bytes);
        drop(key); assert_eq!(memory.used_bytes(), 0);
        let malformed = [vec![0x30, 0x80], der(4, &[42; 8]), der(2, &[0, 0x80, 0, 0, 0]), vec![0, 0]].concat();
        assert!(PrivateKey::decode(&nested_key(oid, &malformed), &memory).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
}
