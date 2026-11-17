use super::*;

const SPECIFIED: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 4, 3];
const OAEP: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 7];
const PBES2: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 13];
const SHA256: &[u8] = &[0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, 1];
const ECDSA_SHA1: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 4, 1];

fn hash(oid: &[u8], parameter: &[u8]) -> Vec<u8> { der(0xa0, &algorithm(oid, parameter)) }
fn factory_key(parameter: &[u8], crt: bool) -> Vec<u8> {
    let mut values = components();
    if !crt { values[3] = vec![0]; }
    key(PSS, parameter, &rsa_material(&values), &[0], &[])
}
fn nested_key(parameter: &[u8], crt: bool) -> Vec<u8> { factory_key(&pss(&[hash(SPECIFIED, parameter)]), crt) }
fn noncrt_expected(oid: &[u8]) -> Vec<u8> {
    let mut values = components();
    values[0].insert(0, 0);
    for index in [1, 3, 4, 5, 6, 7] { values[index] = vec![0]; }
    key(PSS, &pss(&[hash(oid, &[])]), &rsa_material(&values), &[0], &[])
}
fn pbes2(prf: u8, cipher: u8) -> Vec<u8> {
    let prf = algorithm(&[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 2, prf], &[]);
    let kdf = algorithm(&[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 12], &pss(&[der(4, &[]), der(2, &[1]), prf]));
    let cipher = algorithm(&[0x60, 0x86, 0x48, 1, 0x65, 3, 4, 1, cipher], &der(4, &[]));
    pss(&[kdf, cipher])
}

#[test]
fn specified_sha2_reencodes_each_registered_derived_signature_oid() {
    for (digest, signature) in [
        (SHA1.to_vec(), ECDSA_SHA1.to_vec()),
        (SHA256.to_vec(), vec![0x2a, 0x86, 0x48, 0xce, 0x3d, 4, 3, 2]),
        (vec![0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, 2], vec![0x2a, 0x86, 0x48, 0xce, 0x3d, 4, 3, 3]),
        (vec![0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, 3], vec![0x2a, 0x86, 0x48, 0xce, 0x3d, 4, 3, 4]),
        (vec![0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, 4], vec![0x2a, 0x86, 0x48, 0xce, 0x3d, 4, 3, 1]),
    ] {
        for parameter in [algorithm(&digest, &[]), algorithm(&digest, &der(5, &[])), algorithm(&digest, &der(4, &[0xff]))] {
            let input = nested_key(&parameter, true);
            assert_eq!(decoded(&input), input);
            assert_eq!(decoded(&nested_key(&parameter, false)), noncrt_expected(&signature));
        }
    }
}

#[test]
fn specified_sha2_unknown_truncated_sha3_and_fixed_service_names_have_no_reverse_oid() {
    let mut oids = vec![UNKNOWN.to_vec(), RSA.to_vec(), PSS.to_vec(), OAEP.to_vec(), SPECIFIED.to_vec(), ECDSA_SHA1.to_vec()];
    for suffix in 5..=18 { oids.push(vec![0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, suffix]); }
    for oid in oids {
        let parameter = algorithm(&oid, &[]);
        let input = nested_key(&parameter, true);
        assert_eq!(decoded(&input), input);
        assert!(decode(&nested_key(&parameter, false)).is_err());
    }
}

#[test]
fn specified_sha2_all_initialized_pbes2_names_remain_unmapped_after_suffixing() {
    for prf in 7..=13 {
        for cipher in [2, 42] {
            let parameter = algorithm(PBES2, &pbes2(prf, cipher));
            let input = nested_key(&parameter, true);
            assert_eq!(decoded(&input), input);
            assert!(decode(&nested_key(&parameter, false)).is_err());
        }
    }
}

#[test]
fn specified_sha2_absent_null_and_malformed_child_values_use_the_base_oid() {
    for parameter in [vec![], der(5, &[]), der(0x30, &[]), der(0x31, &der(6, SHA1)),
        der(0x30, &der(2, SHA1)), der(0x30, &der(6, &[])), der(0x30, &der(6, &[0x80])),
        der(0x30, &[der(6, SHA1), der(5, &[]), der(5, &[])].concat()), algorithm(SHA1, &der(5, &[0])), der(4, &[0xff])] {
        let input = nested_key(&parameter, true);
        assert_eq!(decoded(&input), input);
        assert_eq!(decoded(&nested_key(&parameter, false)), noncrt_expected(SPECIFIED));
    }
}

#[test]
fn specified_sha2_catches_registered_child_service_decoding_failures() {
    let mut parameters = vec![(PSS.to_vec(), der(0x30, &[0xff])), (OAEP.to_vec(), der(0x30, &[0xff])),
        (DSA.to_vec(), der(0x30, &[])), (DH.to_vec(), der(0x30, &[])), (EC.to_vec(), der(6, UNKNOWN)),
        (vec![0x60, 0x86, 0x48, 1, 0x65, 3, 4, 1], der(4, &[])),
        (vec![0x2b, 0x0e, 3, 2, 17], der(4, &[])), (vec![0x2b, 6, 1, 4, 1, 0x97, 0x55, 1, 1, 2], der(4, &[])),
        (vec![0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 9, 16, 3, 18], der(4, &[])),
        (PBES2.to_vec(), der(0x30, &[])), (vec![0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 3], der(0x30, &[]))];
    for suffix in [1, 2, 3, 5, 6] { parameters.push((vec![0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 12, 1, suffix], der(0x30, &[]))); }
    for (oid, invalid) in parameters {
        let parameter = algorithm(&oid, &invalid);
        let input = nested_key(&parameter, true);
        assert_eq!(decoded(&input), input);
        assert_eq!(decoded(&nested_key(&parameter, false)), noncrt_expected(SPECIFIED));
    }
}

#[test]
fn specified_sha2_catches_signed32_errors_from_pss_pbe_pbkdf2_and_dh() {
    let oversized = der(2, &[0, 0x80, 0, 0, 0]);
    for (oid, invalid) in [(PSS.to_vec(), pss(&[der(0xa2, &oversized)])),
        (vec![0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 3], pss(&[der(4, &[]), oversized.clone()])),
        (DH.to_vec(), pss(&[der(2, &[1]), der(2, &[2]), oversized.clone()]))] {
        assert_eq!(decoded(&nested_key(&algorithm(&oid, &invalid), false)), noncrt_expected(SPECIFIED));
    }
    let kdf = algorithm(&[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 12], &pss(&[der(4, &[]), oversized]));
    let invalid = pss(&[kdf, algorithm(&[0x60, 0x86, 0x48, 1, 0x65, 3, 4, 1, 2], &der(4, &[]))]);
    assert_eq!(decoded(&nested_key(&algorithm(PBES2, &invalid), false)), noncrt_expected(SPECIFIED));
}

#[test]
fn specified_sha2_does_not_catch_its_own_malformed_outer_identifier() {
    for outer in [der(0x31, &der(6, SPECIFIED)), der(0x30, &[der(6, SPECIFIED), der(5, &[0])].concat()),
        der(0x30, &[der(6, SPECIFIED), algorithm(SHA1, &[]), der(5, &[])].concat()), der(0x30, &der(6, &[0x80]))] {
        assert!(decode(&factory_key(&pss(&[der(0xa0, &outer)]), true)).is_err());
        assert_eq!(decoded(&nested_key(&outer, false)), noncrt_expected(SPECIFIED));
    }
}

#[test]
fn nested_specified_sha2_distinguishes_inner_fallback_from_outer_success() {
    for inner in [algorithm(SHA1, &[]), der(0x30, &[])] {
        let parameter = algorithm(SPECIFIED, &inner);
        let input = nested_key(&parameter, true);
        assert_eq!(decoded(&input), input);
        assert!(decode(&nested_key(&parameter, false)).is_err());
    }
    let invalid_pss = pss(&[hash(SHA1, &[]), der(0xa2, &der(2, &[0xff]))]);
    assert_eq!(decoded(&nested_key(&algorithm(PSS, &invalid_pss), false)), noncrt_expected(SPECIFIED));
}

#[test]
fn fixed_service_names_validate_specified_descendants_before_noncrt_reencoding() {
    for outer in [PSS, OAEP] {
        for inner in [algorithm(SHA1, &[]), der(0x30, &[]), algorithm(UNKNOWN, &[])] {
            let parameter = pss(&[hash(SPECIFIED, &inner)]);
            let encoded = factory_key(&pss(&[hash(outer, &parameter)]), false);
            assert_eq!(decoded(&encoded), noncrt_expected(outer));
        }
    }
}

#[test]
fn specified_names_never_satisfy_mgf_allowlists_or_wrapper_oid_requirements() {
    for service in [PSS, OAEP] {
        for child in [algorithm(SHA1, &[]), der(0x30, &[]), algorithm(UNKNOWN, &[])] {
            let mgf = der(0xa1, &algorithm(MGF1, &algorithm(SPECIFIED, &child)));
            assert!(decode(&factory_key(&pss(&[hash(service, &pss(&[mgf]))]), true)).is_err());
            for tag in [0xa1, 0xa2] {
                let wrong = der(tag, &algorithm(SPECIFIED, &child));
                assert!(decode(&factory_key(&pss(&[hash(OAEP, &pss(&[wrong]))]), true)).is_err());
            }
        }
    }
}

#[test]
fn duplicate_root_hashes_apply_each_specified_derivation_before_last_name_wins() {
    let unmapped = hash(SPECIFIED, &algorithm(UNKNOWN, &[]));
    let derived = hash(SPECIFIED, &algorithm(SHA1, &[]));
    assert_eq!(decoded(&factory_key(&pss(&[unmapped.clone(), derived.clone()]), false)), noncrt_expected(ECDSA_SHA1));
    assert!(decode(&factory_key(&pss(&[derived, unmapped]), false)).is_err());
    let malformed = der(0xa0, &algorithm(SPECIFIED, &der(5, &[0])));
    assert!(decode(&factory_key(&pss(&[malformed, hash(SHA1, &[])]), true)).is_err());
}

#[test]
fn specified_child_ber_validation_preserves_containing_crt_bytes_and_reencodes_derived_name() {
    let child = [vec![0x30, 0x80], der(6, SHA256), der(5, &[]), vec![0, 0]].concat();
    let parameter = [vec![0x30, 0x80], der(6, SPECIFIED), child, vec![0, 0]].concat();
    for tag in [0x80, 0xa0] {
        let pss = pss(&[der(tag, &parameter)]);
        let input = factory_key(&pss, true);
        assert_eq!(decoded(&input), input);
        assert_eq!(decoded(&factory_key(&pss, false)), noncrt_expected(&[0x2a, 0x86, 0x48, 0xce, 0x3d, 4, 3, 2]));
    }
}

#[test]
fn specified_context_tails_follow_pss_and_oaep_cardinality_rules() {
    let context = der(0xa0, &[algorithm(SPECIFIED, &algorithm(SHA1, &[])), der(5, &[])].concat());
    let input = factory_key(&pss(&[context.clone()]), true);
    assert_eq!(decoded(&input), input);
    assert!(decode(&factory_key(&pss(&[hash(OAEP, &pss(&[context]))]), true)).is_err());
}

#[test]
fn deep_specified_and_mixed_service_work_is_iterative_and_keeps_key_local_state() {
    let mut child = algorithm(SHA1, &[]);
    for _ in 0..1024 { child = algorithm(SPECIFIED, &child); }
    let input = factory_key(&pss(&[der(0xa0, &child)]), true);
    assert_eq!(decoded(&input), input);
    assert!(decode(&factory_key(&pss(&[der(0xa0, &child)]), false)).is_err());
    let mut child = algorithm(SHA1, &[]);
    for index in 0..1023 {
        child = match index % 3 {
            0 => algorithm(SPECIFIED, &child),
            1 => algorithm(PSS, &pss(&[der(0xa0, &child)])),
            _ => algorithm(OAEP, &pss(&[der(0xa0, &child)])),
        };
    }
    assert_eq!(decoded(&factory_key(&pss(&[der(0xa0, &child)]), false)), noncrt_expected(OAEP));
    assert_eq!(decoded(&nested_key(&der(0x30, &[]), false)), noncrt_expected(SPECIFIED));
    assert_eq!(decoded(&nested_key(&algorithm(SHA1, &[]), false)), noncrt_expected(ECDSA_SHA1));
}

#[test]
fn specified_validation_admission_and_output_lifetimes_release_every_reservation() {
    let input = nested_key(&algorithm(SHA256, &[]), true);
    let bound = allocation_bound(input.len()).unwrap();
    let insufficient = MemoryBudget::new(bound - 1);
    assert_eq!(PrivateKey::decode(&input, &insufficient).err().unwrap().code, "rtdb/memory-budget-exceeded");
    assert_eq!(insufficient.used_bytes(), 0);
    let memory = MemoryBudget::new(bound);
    let recovered = PrivateKey::decode(&input, &memory).unwrap();
    assert_eq!(recovered.encoded_pkcs8(), input);
    assert!(memory.used_bytes() > 0 && memory.used_bytes() < bound);
    drop(recovered);
    assert_eq!(memory.used_bytes(), 0);
    let invalid = nested_key(&algorithm(UNKNOWN, &[]), false);
    assert!(PrivateKey::decode(&invalid, &memory).is_err());
    assert_eq!(memory.used_bytes(), 0);
    let malformed = factory_key(&pss(&[der(0xa0, &der(0x30, &der(6, &[])))]), true);
    assert!(PrivateKey::decode(&malformed, &memory).is_err());
    assert_eq!(memory.used_bytes(), 0);
}
