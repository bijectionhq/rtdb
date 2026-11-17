use super::*;

const HSS: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 9, 16, 3, 17];
const PBES2: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 13];
const SPECIFIED: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 4, 3];

fn jks(bytes: &[u8]) -> Result<Vec<u8>> {
    let memory = MemoryBudget::new(32 << 20);
    let recovered = PrivateKey::parse_jks(bytes, &memory)?;
    let result = recovered.encoded_pkcs8().to_vec();
    drop(recovered);
    assert_eq!(memory.used_bytes(), 0);
    Ok(result)
}
fn pbes2() -> Vec<u8> {
    let kdf = algorithm(&[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 12], &pss(&[der(4, &[]), der(2, &[1])]));
    pss(&[kdf, algorithm(&[0x60, 0x86, 0x48, 1, 0x65, 3, 4, 1, 2], &der(4, &[]))])
}

#[test]
fn jks_unknown_and_hss_factories_return_normalized_raw_keys_with_opaque_material() {
    for oid in [UNKNOWN, HSS] {
        let input = key(oid, &der(4, &[9]), b"opaque private material", &[0, 0], &der(0x80, &[0xff]));
        let expected = key(oid, &der(4, &[9]), b"opaque private material", &[0], &[]);
        assert_eq!(jks(&input).unwrap(), expected);
        assert!(decode(&input).is_err());
        let input = key(oid, &[], b"opaque", &[1], &[der(0x80, &[0xff]), der(0xa1, &[3, 0xff])].concat());
        assert_eq!(jks(&input).unwrap(), key(oid, &[], b"opaque", &[1], &[der(0xa0, &[0xff]), der(0x81, &[3, 0xf8])].concat()));
    }
}

#[test]
fn jks_missing_ec_dh_parameters_fallback_but_malformed_present_parameters_fail_raw_admission() {
    for oid in [EC, DH] {
        for parameters in [vec![], der(5, &[])] {
            let input = key(oid, &parameters, b"opaque", &[0], &[]);
            assert_eq!(jks(&input).unwrap(), key(oid, &[], b"opaque", &[0], &[]));
            assert!(decode(&input).is_err());
        }
        for parameter in [der(4, &[]), der(0x30, &[]), der(6, UNKNOWN)] {
            assert!(jks(&key(oid, &parameter, b"opaque", &[0], &[])).is_err());
        }
    }
}

#[test]
fn jks_registered_nonfactory_services_validate_before_raw_fallback() {
    let aes = &[0x60, 0x86, 0x48, 1, 0x65, 3, 4, 1];
    let input = key(aes, &der(4, &[7; 16]), b"opaque", &[0], &[]);
    assert_eq!(jks(&input).unwrap(), input);
    assert!(decode(&input).is_err());
    for parameters in [der(4, &[]), der(0x30, &[]), der(0x24, &der(4, &[7; 16]))] {
        assert!(jks(&key(aes, &parameters, b"opaque", &[0], &[])).is_err());
    }
    for oid in [UNKNOWN, HSS] {
        let input = key(oid, &der(0x30, &[0xff]), b"opaque", &[0], &[]);
        assert_eq!(jks(&input).unwrap(), input);
    }
}

#[test]
fn jks_derived_pbes2_and_specified_names_select_raw_fallback_without_losing_parameters() {
    for (oid, parameter) in [(PBES2, pbes2()), (SPECIFIED, algorithm(SHA1, &[])),
        (SPECIFIED, algorithm(UNKNOWN, &[])), (SPECIFIED, der(0x30, &[]))] {
        let input = key(oid, &parameter, b"opaque", &[0], &[]);
        assert_eq!(jks(&input).unwrap(), input);
    }
    assert!(jks(&key(PBES2, &der(0x30, &[]), b"opaque", &[0], &[])).is_err());
    for oid in [PBES2, SPECIFIED] {
        for parameter in [vec![], der(5, &[])] {
            assert_eq!(jks(&key(oid, &parameter, b"opaque", &[0], &[])).unwrap(), key(oid, &[], b"opaque", &[0], &[]));
        }
    }
}

#[test]
fn jks_checked_provider_rejections_preserve_raw_private_material_and_optional_fields() {
    let curve = der(6, curves::CURVES[0].1);
    let named = &[0x60, 0x86, 0x48, 1, 0x65, 3, 4, 3, 17];
    for (oid, parameter, material) in [(RSA, der(5, &[]), b"not PKCS1".to_vec()),
        (DSA, vec![], der(4, &[9])), (EC, curve, ec_material(&der(0x82, &[]))),
        (&[0x2b, 0x65, 112][..], vec![], der(4, &[7; 31])), (named, der(4, &[9]), der(4, &[]))] {
        let input = key(oid, &parameter, &material, &[1], &[der(0x80, &[0xff]), der(0xa1, &[1, 0xff])].concat());
        let expected = key(oid, &parameter, &material, &[1], &[der(0xa0, &[0xff]), der(0x81, &[1, 0xfe])].concat());
        assert_eq!(jks(&input).unwrap(), expected);
        assert!(decode(&input).is_err());
    }
}

#[test]
fn jks_available_factories_apply_rsa_noncrt_and_ec_public_key_reencoding() {
    let mut values = components(); values[3] = vec![0];
    let input = key(RSA, &[], &rsa_material(&values), &[1], &der(0x80, &[0xff]));
    values[0].insert(0, 0);
    for index in [1, 3, 4, 5, 6, 7] { values[index] = vec![0]; }
    assert_eq!(jks(&input).unwrap(), key(RSA, &der(5, &[]), &rsa_material(&values), &[0], &[]));
    let curve = der(6, curves::CURVES[0].1);
    let inner = ec_material(&der(0xa1, &der(3, &[2, 0xff])));
    let input = key(EC, &curve, &inner, &[1], &der(0x81, &[0, 0]));
    assert_eq!(jks(&input).unwrap(), key(EC, &curve, &inner, &[1], &der(0x81, &[2, 0xfc])));
    let input = key(DSA, &[], &der(2, &[0xff]), &[0], &[]);
    assert_eq!(jks(&input).unwrap(), input);
}

#[test]
fn jks_dh_factory_retains_normalized_input_while_strict_pkcs12_keeps_original_input() {
    let parameters = pss(&[der(2, &[1]), der(2, &[2])]);
    let content = [der(2, &[0, 0]), algorithm(DH, &parameters), der(4, &der(2, &[0, 0xff]))].concat();
    let input = [vec![0x30, 0x80], content, vec![0, 0]].concat();
    let expected = key(DH, &parameters, &der(2, &[0, 0xff]), &[0], &[]);
    assert_eq!(jks(&input).unwrap(), expected);
    assert_eq!(decoded(&input), input);
    let input = key(DH, &parameters, &der(2, &[1]), &[1], &der(0x80, &[0xff]));
    assert_eq!(jks(&input).unwrap(), key(DH, &parameters, &der(2, &[1]), &[1], &der(0xa0, &[0xff])));
    assert!(decode(&input).is_err());
}

#[test]
fn jks_noncrt_pss_unchecked_name_encoding_failure_never_selects_raw_fallback() {
    for (digest, parameter) in [(PBES2, pbes2()), (SPECIFIED, algorithm(UNKNOWN, &[]))] {
        let parameters = pss(&[der(0xa0, &algorithm(digest, &parameter))]);
        let crt = key(PSS, &parameters, &rsa_material(&components()), &[0], &[]);
        assert_eq!(jks(&crt).unwrap(), crt);
        let mut values = components(); values[3] = vec![0];
        let input = key(PSS, &parameters, &rsa_material(&values), &[0], &[]);
        assert_eq!(jks(&input).err().unwrap(), malformed());
        assert_eq!(decode(&input).err().unwrap(), malformed());
    }
}

#[test]
fn jks_raw_framing_failures_precede_unknown_factory_fallback() {
    for input in [vec![], der(4, &[]), key(UNKNOWN, &[], b"opaque", &[2], &[]),
        key(UNKNOWN, &der(5, &[0]), b"opaque", &[0], &[]),
        key(UNKNOWN, &[], b"opaque", &[0], &der(0x81, &[0])),
        key(UNKNOWN, &[], b"opaque", &[1], &der(0x81, &[8, 0])),
        key(UNKNOWN, &[], b"opaque", &[1], &[der(0x81, &[0]), der(5, &[])].concat()),
        der(0x30, &[der(2, &[0]), algorithm(UNKNOWN, &[]), der(0x24, &der(4, &[1]))].concat())] {
        let memory = MemoryBudget::new(4 << 20);
        assert!(PrivateKey::parse_jks(&input, &memory).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn jks_missing_factory_needs_only_raw_admission_and_retains_one_key_lease() {
    let input = key(UNKNOWN, &[], b"opaque", &[0], &[]);
    let bound = allocation_bound(input.len()).unwrap();
    let insufficient = MemoryBudget::new(bound - 1);
    assert_eq!(PrivateKey::parse_jks(&input, &insufficient).err().unwrap().code, "rtdb/memory-budget-exceeded");
    assert_eq!(insufficient.used_bytes(), 0);
    let memory = MemoryBudget::new(bound);
    let recovered = PrivateKey::parse_jks(&input, &memory).unwrap();
    assert_eq!(recovered.encoded_pkcs8(), input);
    assert_eq!(memory.used_bytes(), recovered.retained_bytes());
    assert!(recovered.retained_bytes() < bound);
    drop(recovered);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn jks_factory_admission_uses_actual_normalized_size_and_never_falls_back_on_budget_failure() {
    let material = rsa_material(&components());
    let input = key(RSA, &[], &material, &[0], &[]);
    let normalized = key(RSA, &der(5, &[]), &material, &[0], &[]);
    assert!(normalized.len() > input.len());
    let raw_bound = allocation_bound(input.len()).unwrap();
    let factory_bound = allocation_bound(normalized.len()).unwrap();
    let insufficient = MemoryBudget::new(raw_bound + factory_bound - 1);
    assert_eq!(PrivateKey::parse_jks(&input, &insufficient).err().unwrap().code, "rtdb/memory-budget-exceeded");
    assert_eq!(insufficient.used_bytes(), 0);
    let memory = MemoryBudget::new(raw_bound + factory_bound);
    let recovered = PrivateKey::parse_jks(&input, &memory).unwrap();
    assert_eq!(recovered.encoded_pkcs8(), normalized);
    assert_eq!(memory.used_bytes(), recovered.retained_bytes());
    drop(recovered);
    assert_eq!(memory.used_bytes(), 0);
    let rejected = key(RSA, &[], b"not PKCS1", &[0], &[]);
    let retained = PrivateKey::parse_jks(&rejected, &memory).unwrap();
    assert_eq!(retained.encoded_pkcs8(), key(RSA, &der(5, &[]), b"not PKCS1", &[0], &[]));
    assert_eq!(memory.used_bytes(), retained.retained_bytes());
    drop(retained);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn jks_generalized_initialization_handles_deep_specified_names_without_retained_state() {
    let mut parameter = algorithm(SHA1, &[]);
    for _ in 0..1024 { parameter = algorithm(SPECIFIED, &parameter); }
    let input = key(SPECIFIED, &parameter, b"opaque", &[0], &[]);
    assert_eq!(jks(&input).unwrap(), input);
    let input = key(SPECIFIED, &der(0x30, &[]), b"opaque", &[0], &[]);
    assert_eq!(jks(&input).unwrap(), input);
    let input = key(UNKNOWN, &[], b"opaque", &[0], &[]);
    assert_eq!(jks(&input).unwrap(), input);
}
