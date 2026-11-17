use super::*;

const PBES2: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 13];
const PBKDF2: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 12];
const OAEP: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 7];

fn aes(suffix: u8) -> Vec<u8> { vec![0x60, 0x86, 0x48, 1, 0x65, 3, 4, 1, suffix] }
fn prf(suffix: u8, parameters: &[u8]) -> Vec<u8> { algorithm(&[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 2, suffix], parameters) }
fn hash(oid: &[u8], parameter: &[u8]) -> Vec<u8> { der(0xa0, &algorithm(oid, parameter)) }
fn nested_key(parameter: &[u8]) -> Vec<u8> { key(PSS, &pss(&[hash(PBES2, parameter)]), &rsa_material(&components()), &[0], &[]) }
fn params(salt: &[u8], count: &[u8], optional: &[u8], cipher: u8, iv: &[u8]) -> Vec<u8> {
    pss(&[algorithm(PBKDF2, &pss(&[salt.to_vec(), count.to_vec(), optional.to_vec()])), algorithm(&aes(cipher), iv)])
}
fn basic(prf_suffix: u8, cipher: u8) -> Vec<u8> { params(&der(4, &[]), &der(2, &[1]), &prf(prf_suffix, &[5, 0]), cipher, &der(4, &[])) }

#[test]
fn all_fourteen_initialized_pbes2_names_admit_crt_keys_and_fail_noncrt_name_reencoding() {
    for prf in 7..=13 {
        for cipher in [2, 42] {
            let parameter = basic(prf, cipher);
            let input = nested_key(&parameter);
            assert_eq!(decoded(&input), input);
            let mut values = components(); values[3] = vec![0];
            let noncrt = key(PSS, &pss(&[hash(PBES2, &parameter)]), &rsa_material(&values), &[0], &[]);
            assert!(decode(&noncrt).is_err());
        }
    }
}

#[test]
fn absent_and_null_pbes2_parameters_retain_the_base_name_for_noncrt_reencoding() {
    for parameter in [vec![], der(5, &[])] {
        let input = nested_key(&parameter);
        assert_eq!(decoded(&input), input);
        let mut values = components(); values[3] = vec![0];
        let input = key(PSS, &pss(&[hash(PBES2, &parameter)]), &rsa_material(&values), &[0], &[]);
        values[0].insert(0, 0);
        for index in [1, 3, 4, 5, 6, 7] { values[index] = vec![0]; }
        assert_eq!(decoded(&input), key(PSS, &pss(&[hash(PBES2, &[])]), &rsa_material(&values), &[0], &[]));
    }
}

#[test]
fn pbes2_parameter_admission_accepts_arbitrary_primitive_salt_and_iv_lengths() {
    for salt_size in [0, 1, 7, 8, 9, 256] {
        for iv_size in [0, 1, 8, 15, 16, 17, 32] {
            let parameter = params(&der(4, &vec![0x41; salt_size]), &der(2, &[1]), &[], 2, &der(4, &vec![0x42; iv_size]));
            let input = nested_key(&parameter);
            assert_eq!(decoded(&input), input);
        }
    }
    for value in [der(0x24, &der(4, &[0; 16])), der(0x84, &[0; 16]), der(5, &[]), der(2, &[0])] {
        assert!(decode(&nested_key(&params(&value, &der(2, &[1]), &[], 2, &der(4, &[])))).is_err());
        assert!(decode(&nested_key(&params(&der(4, &[]), &der(2, &[1]), &[], 2, &value))).is_err());
    }
}

#[test]
fn pbes2_iterations_and_optional_key_lengths_accept_the_full_signed_int_range() {
    let counts = [vec![0], vec![0xff], vec![0x80, 0, 0, 0], vec![0x7f, 0xff, 0xff, 0xff], vec![0, 0, 1], vec![0xff, 0xff, 0xff]];
    for count in &counts {
        for length in &counts {
            let optional = [der(2, length), prf(9, &[])].concat();
            let input = nested_key(&params(&der(4, &[]), &der(2, count), &optional, 42, &der(4, &[])));
            assert_eq!(decoded(&input), input);
        }
    }
    for invalid in [vec![], vec![0, 0x80, 0, 0, 0], vec![0xff, 0x7f, 0xff, 0xff, 0xff]] {
        assert!(decode(&nested_key(&params(&der(4, &[]), &der(2, &invalid), &[], 2, &der(4, &[])))).is_err());
        assert!(decode(&nested_key(&params(&der(4, &[]), &der(2, &[1]), &der(2, &invalid), 2, &der(4, &[])))).is_err());
    }
    assert!(decode(&nested_key(&params(&der(4, &[]), &der(0x22, &[1]), &[], 2, &der(4, &[])))).is_err());
}

#[test]
fn pbes2_prf_parameters_allow_absence_or_one_null_value_without_a_null_length_check() {
    for suffix in 7..=13 {
        for null in [vec![], der(5, &[]), der(5, &[0]), der(5, &[0xff; 32])] {
            let input = nested_key(&params(&der(4, &[]), &der(2, &[1]), &prf(suffix, &null), 2, &der(4, &[])));
            assert_eq!(decoded(&input), input);
        }
        for invalid in [der(4, &[]), der(0x25, &[]), vec![5], [der(5, &[]), der(5, &[])].concat(), [der(5, &[]), vec![0xff]].concat()] {
            assert!(decode(&nested_key(&params(&der(4, &[]), &der(2, &[1]), &prf(suffix, &invalid), 2, &der(4, &[])))).is_err());
        }
    }
}

#[test]
fn pbes2_optional_fields_are_tag_selected_and_unconsumed_pbkdf2_tails_are_ignored() {
    for optional in [
        vec![], vec![0xff], der(5, &[]), der(0x22, &[1]), der(0x31, &der(6, UNKNOWN)),
        [der(5, &[]), prf(255, &[])].concat(),
        [der(2, &[0xff]), vec![0xff]].concat(),
        [der(2, &[1]), der(2, &[0xff])].concat(),
        [prf(9, &[]), vec![0xff]].concat(),
        [prf(9, &[]), prf(255, &[])].concat(),
    ] {
        let input = nested_key(&params(&der(4, &[]), &der(2, &[1]), &optional, 2, &der(4, &[])));
        assert_eq!(decoded(&input), input);
    }
    for optional in [vec![2], vec![0x30], der(0x30, &[])] {
        assert!(decode(&nested_key(&params(&der(4, &[]), &der(2, &[1]), &optional, 2, &der(4, &[])))).is_err());
    }
}

#[test]
fn pbes2_rejects_wrong_kdf_prf_aes192_and_malformed_required_identifiers() {
    let kdf_parameters = pss(&[der(4, &[]), der(2, &[1])]);
    let encryption = algorithm(&aes(2), &der(4, &[]));
    for kdf in [algorithm(UNKNOWN, &kdf_parameters), algorithm(PBKDF2, &der(0x31, &[])),
        der(0x31, &[der(6, PBKDF2), kdf_parameters.clone()].concat()),
        der(0x30, &[der(6, &[]), kdf_parameters.clone()].concat()),
        algorithm(PBKDF2, &pss(&[der(4, &[])]))] {
        assert!(decode(&nested_key(&pss(&[kdf, encryption.clone()]))).is_err());
    }
    for suffix in [0, 1, 6, 14, 255] {
        assert!(decode(&nested_key(&params(&der(4, &[]), &der(2, &[1]), &prf(suffix, &[]), 2, &der(4, &[])))).is_err());
    }
    for suffix in [0, 1, 6, 22, 26, 41, 46] {
        assert!(decode(&nested_key(&basic(7, suffix))).is_err());
    }
    for invalid in [der(0x30, &[]), der(0x31, &[]), der(4, &[]), der(5, &[0])] { assert!(decode(&nested_key(&invalid)).is_err()); }
}

#[test]
fn pbes2_encryption_outer_tag_and_kdf_encryption_outer_tails_follow_parameter_parser() {
    let pbkdf2 = pss(&[der(4, &[]), der(2, &[1]), prf(9, &[])]);
    let kdf = der(0x30, &[der(6, PBKDF2), pbkdf2, vec![0xff]].concat());
    for tag in [0x30, 0x31, 2, 4, 5, 6, 0x80, 0xa0, 0xe1] {
        let encryption = der(tag, &[der(6, &aes(42)), der(4, &[]), vec![0xff]].concat());
        let parameter = der(0x30, &[kdf.clone(), encryption, vec![0xff]].concat());
        let input = nested_key(&parameter);
        assert_eq!(decoded(&input), input);
    }
    let parameter = [basic(7, 2), der(5, &[])].concat();
    assert!(decode(&nested_key(&parameter)).is_err());
}

#[test]
fn pbes2_historical_wrapper_skips_any_first_oid_content_and_ignores_its_tail() {
    let real = basic(7, 2);
    for oid in [PBES2.to_vec(), UNKNOWN.to_vec(), vec![], vec![0x80]] {
        let parameter = der(0x30, &[der(6, &oid), real.clone(), vec![0xff]].concat());
        let input = nested_key(&parameter);
        assert_eq!(decoded(&input), input);
    }
    for bad in [
        pss(&[der(6, PBES2)]), pss(&[der(6, PBES2), der(0x31, &[])]),
        pss(&[der(6, PBES2), der(4, &[algorithm(PBKDF2, &pss(&[der(4, &[]), der(2, &[1])])), algorithm(&aes(2), &der(4, &[]))].concat())]),
        pss(&[der(6, PBES2), pss(&[der(6, PBES2), real])]),
    ] { assert!(decode(&nested_key(&bad)).is_err()); }
}

#[test]
fn pbes2_ber_validation_preserves_containing_crt_bytes() {
    let kdf = [vec![0x30, 0x80], der(6, PBKDF2), [vec![0x30, 0x80], der(4, &[]), der(2, &[0xff]), prf(13, &der(5, &[1])), vec![0, 0]].concat(), vec![0, 0]].concat();
    let es = [vec![0xa0, 0x80], der(6, &aes(42)), der(4, &[1; 3]), vec![0, 0]].concat();
    for parameter in [pss(&[kdf.clone(), es.clone()]), [vec![0x30, 0x80], kdf, es, vec![0, 0]].concat()] {
        let input = nested_key(&parameter);
        assert_eq!(decoded(&input), input);
    }
    let definite = basic(7, 2);
    let padded = [vec![0x30, 0x84, 0, 0, 0, definite[1]], definite[2..].to_vec()].concat();
    let input = nested_key(&padded);
    assert_eq!(decoded(&input), input);
}

#[test]
fn recursive_pss_and_oaep_validate_pbes2_children_without_changing_their_own_names() {
    for outer in [PSS, OAEP] {
        let nested = pss(&[hash(PBES2, &basic(9, 42))]);
        let input = key(PSS, &pss(&[hash(outer, &nested)]), &rsa_material(&components()), &[0], &[]);
        assert_eq!(decoded(&input), input);
        let mut values = components(); values[3] = vec![0];
        let input = key(PSS, &pss(&[hash(outer, &nested)]), &rsa_material(&values), &[0], &[]);
        values[0].insert(0, 0);
        for index in [1, 3, 4, 5, 6, 7] { values[index] = vec![0]; }
        assert_eq!(decoded(&input), key(PSS, &pss(&[hash(outer, &[])]), &rsa_material(&values), &[0], &[]));
        assert!(decode(&key(PSS, &pss(&[hash(outer, &pss(&[hash(PBES2, &der(0x30, &[]))]))]), &rsa_material(&components()), &[0], &[])).is_err());
    }
}

#[test]
fn pbes2_names_are_not_mgf_digests_and_duplicate_root_hashes_use_the_last_valid_name() {
    for outer in [PSS, OAEP] {
        let mgf = der(0xa1, &algorithm(MGF1, &algorithm(PBES2, &basic(7, 2))));
        assert!(decode(&key(PSS, &pss(&[hash(outer, &pss(&[mgf]))]), &rsa_material(&components()), &[0], &[])).is_err());
    }
    let mut values = components(); values[3] = vec![0];
    let accepted = pss(&[hash(PBES2, &basic(7, 2)), hash(SHA1, &[])]);
    assert!(decode(&key(PSS, &accepted, &rsa_material(&values), &[0], &[])).is_ok());
    let rejected = pss(&[hash(SHA1, &[]), hash(PBES2, &basic(7, 2))]);
    assert!(decode(&key(PSS, &rejected, &rsa_material(&values), &[0], &[])).is_err());
    let malformed_first = pss(&[hash(PBES2, &der(0x30, &[])), hash(SHA1, &[])]);
    assert!(decode(&key(PSS, &malformed_first, &rsa_material(&components()), &[0], &[])).is_err());
}

#[test]
fn pbes2_noncrt_name_failure_waits_for_all_later_nested_parameter_validation() {
    let specified_sha2 = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 4, 3];
    let mut values = components(); values[3] = vec![0];
    let parameter = pss(&[hash(PBES2, &basic(7, 2)), hash(specified_sha2, &algorithm(SHA1, &[]))]);
    let error = decode(&key(PSS, &parameter, &rsa_material(&values), &[0], &[])).err().unwrap();
    assert!(error.to_string().contains("Unsupported nested Java private-key AlgorithmParameters"));
    let mgf = der(0xa1, &algorithm(MGF1, &algorithm(PBES2, &basic(7, 2))));
    let parameter = pss(&[mgf, hash(specified_sha2, &algorithm(SHA1, &[]))]);
    let error = decode(&key(PSS, &parameter, &rsa_material(&values), &[0], &[])).err().unwrap();
    assert!(!error.to_string().contains("Unsupported nested Java private-key AlgorithmParameters"));
}

#[test]
fn pbes2_validation_and_noncrt_name_failure_release_factory_reservations() {
    let input = nested_key(&basic(13, 42));
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
    assert!(PrivateKey::decode(&nested_key(&der(0x30, &[])), &memory).is_err());
    assert_eq!(memory.used_bytes(), 0);
    let mut values = components(); values[3] = vec![0];
    let noncrt = key(PSS, &pss(&[hash(PBES2, &basic(13, 42))]), &rsa_material(&values), &[0], &[]);
    assert!(PrivateKey::decode(&noncrt, &memory).is_err());
    assert_eq!(memory.used_bytes(), 0);
}
