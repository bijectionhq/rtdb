use super::*;
use crate::java_der::encoded as der;

const UNKNOWN: &[u8] = &[0x2a, 3, 4];
const SHA1: &[u8] = &[0x2b, 0x0e, 3, 2, 26];
const MGF1: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 8];

fn algorithm(oid: &[u8], parameter: &[u8]) -> Vec<u8> { der(0x30, &[der(6, oid), parameter.to_vec()].concat()) }
fn key(oid: &[u8], parameter: &[u8], private: &[u8], version: &[u8], optional: &[u8]) -> Vec<u8> {
    der(0x30, &[der(2, version), algorithm(oid, parameter), der(4, private), optional.to_vec()].concat())
}
fn decoded(bytes: &[u8]) -> Vec<u8> { decode(bytes).unwrap().to_vec() }
fn components() -> Vec<Vec<u8>> { vec![vec![0x80; 64], vec![3], vec![7], vec![1], vec![2], vec![3], vec![4], vec![5]] }
fn rsa_material(components: &[Vec<u8>]) -> Vec<u8> {
    der(0x30, &[der(2, &[0]), components.iter().flat_map(|value| der(2, value)).collect()].concat())
}
fn rsa_key(components: &[Vec<u8>]) -> Vec<u8> { key(RSA, &[], &rsa_material(components), &[0], &[]) }
fn pss(fields: &[Vec<u8>]) -> Vec<u8> { der(0x30, &fields.concat()) }
fn ec_material(optional: &[u8]) -> Vec<u8> { der(0x30, &[der(2, &[1]), der(4, &[]), optional.to_vec()].concat()) }
fn named(oid: &[u8], length: usize) -> Vec<u8> { key(oid, &[], &der(4, &vec![42; length]), &[0], &[]) }

#[test]
fn pkcs8_versions_signed_padding_and_optional_fields_follow_java_reencoding() {
    let private = der(2, &[0xff]);
    let attrs = der(0x80, &[0x30, 0x80]);
    let public = der(0xa1, &[3, 0xff]);
    let optional = [attrs, public].concat();
    let normalized = [der(0xa0, &[0x30, 0x80]), der(0x81, &[3, 0xf8])].concat();
    assert_eq!(decoded(&key(DSA, &[], &private, &[0, 0, 1], &optional)), key(DSA, &[], &private, &[1], &normalized));
    assert_eq!(decoded(&key(DSA, &[5, 0], &private, &[0, 0], &der(0x80, &[0xff]))), key(DSA, &[], &private, &[0], &[]));
    for version in [vec![], vec![2], vec![0xff], vec![0, 0x80, 0, 0, 0]] {
        assert!(decode(&key(DSA, &[], &private, &version, &[])).is_err());
    }
    for (version, optional) in [
        (0, der(0x81, &[0])), (1, der(0x82, &[])), (1, der(0x81, &[])), (1, der(0x81, &[8])),
        (1, [der(0x81, &[0]), der(0x80, &[])].concat()), (1, [der(0x80, &[]), der(0x80, &[])].concat()),
    ] { assert!(decode(&key(DSA, &[], &private, &[version], &optional)).is_err()); }
}

#[test]
fn pkcs12_algorithm_preparse_cardinality_and_primitive_key_octets_are_enforced() {
    for parameter in [vec![5, 1, 0], vec![5, 0, 5, 0], vec![0x1f, 0]] {
        assert!(decode(&key(DSA, &parameter, &der(2, &[1]), &[0], &[])).is_err());
    }
    let content = [der(2, &[0]), algorithm(DSA, &[]), der(0x24, &der(4, &der(2, &[1])))].concat();
    assert!(decode(&der(0x30, &content)).is_err());
    assert!(decode(&der(0x31, &[der(2, &[0]), algorithm(DSA, &[]), der(4, &der(2, &[1]))].concat())).is_err());
    assert!(decode(&key(UNKNOWN, &[], &der(2, &[1]), &[0], &[])).is_err());
    assert!(decode(&key(&[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 5], &[], &rsa_material(&components()), &[0], &[])).is_err());
}

#[test]
fn common_factory_ber_frames_normalize_without_rewriting_private_octet_contents() {
    let private = [der(2, &[0, 0, 1]), vec![0xff]].concat();
    let fields = [der(2, &[0, 0]), algorithm(DSA, &[5, 0]), der(4, &private)].concat();
    let expected = key(DSA, &[], &private, &[0], &[]);
    let indefinite = [vec![0x30, 0x80], fields.clone(), vec![0, 0]].concat();
    assert_eq!(decoded(&indefinite), expected);
    let padded_length = [vec![0x30, 0x84, 0, 0, 0, fields.len() as u8], fields].concat();
    assert_eq!(decoded(&padded_length), expected);
    assert!(decode(&[expected, vec![0]].concat()).is_err());
}

#[test]
fn rsa_factory_accepts_negative_wire_integers_without_native_key_validation() {
    let material = rsa_material(&components());
    let input = key(RSA, &[], &material, &[0], &[]);
    assert_eq!(decoded(&input), key(RSA, &[5, 0], &material, &[0], &[]));
    for oid in [RSA, &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1], &[0x55, 8, 1, 1]] {
        assert!(decode(&key(oid, &der(2, &[7]), &material, &[0], &[])).is_ok());
    }
    for index in 0..8 {
        let mut values = components(); values[index].clear();
        assert!(decode(&rsa_key(&values)).is_err());
    }
    let missing = der(0x30, &[der(2, &[0]), der(2, &[1])].concat());
    assert!(decode(&key(RSA, &[], &missing, &[0], &[])).is_err());
    let extra = der(0x30, &[der(2, &[0]), components().iter().flat_map(|value| der(2, value)).collect(), der(2, &[1])].concat());
    assert!(decode(&key(RSA, &[], &extra, &[0], &[])).is_err());
}

#[test]
fn rsa_modulus_bounds_round_to_bytes_and_default_exponent_restriction_precedes_fallback() {
    for (bytes, first, valid) in [(63, 0x80, false), (64, 1, true), (2048, 0x80, true), (2049, 1, false)] {
        let mut values = components(); values[0] = vec![0; bytes]; values[0][0] = first;
        assert_eq!(decode(&rsa_key(&values)).is_ok(), valid);
    }
    for (bytes, exponent, valid) in [(384, 9, true), (385, 9, false), (385, 8, true)] {
        let mut values = components(); values[0] = vec![0x80; bytes]; values[1] = vec![1; exponent]; values[3] = vec![0];
        assert_eq!(decode(&rsa_key(&values)).is_ok(), valid);
    }
    let mut values = components(); values[0] = vec![0];
    assert!(decode(&rsa_key(&values)).is_err());
}

#[test]
fn any_zero_crt_component_rebuilds_v0_key_with_only_modulus_and_private_exponent() {
    for missing in [1, 3, 4, 5, 6, 7] {
        let mut values = components(); values[missing] = vec![0, 0]; values[2] = vec![0xff];
        let optional = [der(0xa0, &[1]), der(0x81, &[0, 42])].concat();
        let input = key(RSA, &der(2, &[7]), &rsa_material(&values), &[1], &optional);
        let mut expected = values;
        expected[0].insert(0, 0); expected[2].insert(0, 0);
        for index in [1, 3, 4, 5, 6, 7] { expected[index] = vec![0]; }
        assert_eq!(decoded(&input), key(RSA, &[5, 0], &rsa_material(&expected), &[0], &[]));
    }
    let mut values = components(); values[2] = vec![0];
    assert_eq!(decoded(&rsa_key(&values)), key(RSA, &[5, 0], &rsa_material(&values), &[0], &[]));
}

#[test]
fn rsa_crt_key_retains_parameters_that_have_no_java_parameter_service() {
    let material = rsa_material(&components());
    for parameter in [der(2, &[1]), der(0x30, &[0xff]), der(0x25, &[])] {
        let input = key(RSA, &parameter, &material, &[0], &[]);
        assert_eq!(decoded(&input), input);
    }
}

#[test]
fn pss_defaults_duplicate_context_fields_and_fallback_parameter_canonicalization() {
    let defaults = pss(&[der(0x82, &der(2, &[32])), der(0x80, &algorithm(SHA1, &[5, 0])), der(0xa2, &der(2, &[20])), der(0xa3, &der(2, &[1]))]);
    let input = key(PSS, &defaults, &rsa_material(&components()), &[0], &[]);
    assert_eq!(decoded(&input), input);
    let mut values = components(); values[3] = vec![0];
    let output = decoded(&key(PSS, &defaults, &rsa_material(&values), &[1], &der(0x80, &[1])));
    values[0].insert(0, 0);
    for index in [1, 3, 4, 5, 6, 7] { values[index] = vec![0]; }
    assert_eq!(output, key(PSS, &der(0x30, &[]), &rsa_material(&values), &[0], &[]));
    for parameter in [vec![], vec![5, 0]] {
        assert_eq!(decoded(&key(PSS, &parameter, &rsa_material(&components()), &[0], &[])), key(PSS, &[], &rsa_material(&components()), &[0], &[]));
    }
}

#[test]
fn pss_mgf_digest_allowlist_and_integer_constraints_follow_parameter_provider() {
    let material = rsa_material(&components());
    let mut digests = vec![SHA1.to_vec()];
    for suffix in 1..=10 { digests.push(vec![0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, suffix]); }
    for digest in digests {
        let mgf = der(0xa1, &algorithm(MGF1, &algorithm(&digest, &der(2, &[1]))));
        assert!(decode(&key(PSS, &pss(&[mgf]), &material, &[0], &[])).is_ok());
    }
    for field in [
        der(0xa4, &[]), der(0xa1, &algorithm(UNKNOWN, &algorithm(SHA1, &[]))),
        der(0xa1, &algorithm(MGF1, &[])), der(0xa1, &algorithm(MGF1, &algorithm(UNKNOWN, &[]))),
        der(0xa2, &der(2, &[0xff])), der(0xa2, &der(2, &[0, 0x80, 0, 0, 0])), der(0xa3, &der(2, &[2])),
    ] { assert!(decode(&key(PSS, &pss(&[field]), &material, &[0], &[])).is_err()); }
    let maximum_salt = der(0xa2, &der(2, &[0, 0x7f, 0xff, 0xff, 0xff]));
    assert!(decode(&key(PSS, &pss(&[maximum_salt]), &material, &[0], &[])).is_ok());
}

#[test]
fn pss_hash_names_are_not_restricted_to_mgf_digests_and_context_tails_are_ignored() {
    let material = rsa_material(&components());
    let hash = der(0x80, &[algorithm(UNKNOWN, &der(2, &[1])), vec![0xff]].concat());
    let salt = der(0x82, &[der(2, &[0]), vec![0xff]].concat());
    assert!(decode(&key(PSS, &pss(&[hash, salt]), &material, &[0], &[])).is_ok());
    let hash = der(0xa0, &algorithm(&[0x2b, 0x0e, 3, 2, 7], &der(2, &[1])));
    assert!(decode(&key(PSS, &pss(&[hash]), &material, &[0], &[])).is_ok());
}

#[test]
fn pss_noncrt_rebuild_canonicalizes_digest_aliases_and_nondefault_mask_and_salt() {
    let sha256 = [0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, 1];
    let original = pss(&[
        der(0x82, &der(2, &[0, 32])),
        der(0x80, &algorithm(&[0x2b, 0x0e, 3, 2, 12], &[])),
        der(0x81, &algorithm(MGF1, &algorithm(&sha256, &der(2, &[99])))),
    ]);
    let mut values = components(); values[3] = vec![0];
    let output = decoded(&key(PSS, &original, &rsa_material(&values), &[0], &[]));
    values[0].insert(0, 0);
    for index in [1, 3, 4, 5, 6, 7] { values[index] = vec![0]; }
    let expected = pss(&[
        der(0xa0, &algorithm(DSA, &[])),
        der(0xa1, &algorithm(MGF1, &algorithm(&sha256, &[5, 0]))),
        der(0xa2, &der(2, &[32])),
    ]);
    assert_eq!(output, key(PSS, &expected, &rsa_material(&values), &[0], &[]));
}

#[test]
fn pss_uncovered_nested_parameter_services_return_explicit_errors() {
    for (oid, parameter) in [(&[0x2a, 0x86, 0x48, 0xce, 0x3d, 4, 3][..], der(0x30, &[])),
        (&[0x2a, 0x86, 0x48, 0xce, 0x3d, 4, 3][..], algorithm(SHA1, &[]))] {
        let input = key(PSS, &pss(&[der(0xa0, &algorithm(oid, &parameter))]), &rsa_material(&components()), &[0], &[]);
        assert!(decode(&input).err().unwrap().to_string().contains("Unsupported nested Java private-key AlgorithmParameters"));
    }
}

mod iv_parameters;
mod legacy_pbe;
mod recursive_parameters;
mod pbes2_parameters;

#[test]
fn dsa_accepts_signed_scalars_and_ignored_scalar_tails_with_optional_parameters() {
    for oid in [DSA, &[0x2b, 0x0e, 3, 2, 12]] {
        for scalar in [vec![0], vec![0xff], vec![0, 0, 1], vec![0xff, 0xff, 0x80]] {
            let private = [der(2, &scalar), vec![0xff]].concat();
            for parameter in [vec![], vec![5, 0], der(0x30, &[der(2, &[0xff]), der(2, &[0]), der(2, &[1])].concat())] {
                assert!(decode(&key(oid, &parameter, &private, &[0], &[])).is_ok());
            }
        }
    }
    for parameter in [der(2, &[1]), der(0x30, &[]), der(0x30, &[der(2, &[1]), der(2, &[1]), der(2, &[])].concat()),
        der(0x30, &der(2, &[1]).repeat(4))] {
        assert!(decode(&key(DSA, &parameter, &der(2, &[1]), &[0], &[])).is_err());
    }
}

#[test]
fn dh_retains_original_ber_bytes_and_accepts_signed_parameter_and_scalar_values() {
    let parameter = der(0x30, &[der(2, &[0xff]), der(2, &[0]), der(2, &[0x80, 0, 0, 0])].concat());
    for oid in [DH, &[0x2a, 0x86, 0x48, 0xce, 0x3e, 2, 1]] {
        let input = key(oid, &parameter, &[der(2, &[0xff, 0xff]), vec![0xff]].concat(), &[0, 0], &[]);
        assert_eq!(decoded(&input), input);
        let mut fields = Der(&input); let content = expect(&mut fields, 0x30).unwrap();
        let indefinite = [vec![0x30, 0x80], content.to_vec(), vec![0, 0]].concat();
        assert_eq!(decoded(&indefinite), indefinite);
    }
}

#[test]
fn dh_factory_rejects_pkcs8_v1_optional_fields_and_parameter_overflow() {
    let parameter = der(0x30, &[der(2, &[1]), der(2, &[1])].concat());
    assert!(decode(&key(DH, &parameter, &der(2, &[1]), &[1], &[])).is_err());
    assert!(decode(&key(DH, &parameter, &der(2, &[1]), &[0], &der(0xa0, &[]))).is_err());
    for parameter in [vec![], vec![5, 0], der(0x30, &[der(2, &[1]), der(2, &[1]), der(2, &[0, 0x80, 0, 0, 0])].concat()),
        der(0x30, &der(2, &[1]).repeat(4))] {
        assert!(decode(&key(DH, &parameter, &der(2, &[1]), &[0], &[])).is_err());
    }
    let extra_algorithm = [parameter, der(5, &[])].concat();
    assert!(decode(&key(DH, &extra_algorithm, &der(2, &[1]), &[0], &[])).is_err());
}

#[test]
fn ec_accepts_all_registered_curve_parameters_without_scalar_size_validation() {
    assert_eq!(curves::CURVES.len(), 53);
    for &(name, oid) in curves::CURVES {
        let private = [ec_material(&[]), vec![0xff]].concat();
        assert!(decode(&key(EC, &der(6, oid), &private, &[0], &[])).is_ok(), "{name}");
    }
    for parameter in [vec![], vec![5, 0], der(0x30, &[]), der(6, UNKNOWN), der(6, &[0x2a, 0x86, 0x48, 0xce, 0x3d, 3, 0, 1])] {
        assert!(decode(&key(EC, &parameter, &ec_material(&[]), &[0], &[])).is_err());
    }
}

#[test]
fn ec_optional_public_bits_are_nested_and_override_pkcs8_v1_public_bits() {
    let parameter = der(6, curves::CURVES[0].1);
    let optional = [der(0x80, &[0xff]), der(0xa1, &[der(3, &[3, 0xff]), vec![0xff]].concat()), vec![0xff]].concat();
    let private = ec_material(&optional);
    let input = key(EC, &parameter, &private, &[1], &der(0x81, &[0, 42]));
    assert_eq!(decoded(&input), key(EC, &parameter, &private, &[1], &der(0x81, &[3, 0xf8])));
    assert_eq!(decoded(&key(EC, &parameter, &private, &[0], &[])), key(EC, &parameter, &private, &[0], &[]));
    for optional in [der(0x81, &[0, 42]), der(0xa1, &der(4, &[0, 42])), der(0xa1, &der(3, &[8, 0])), der(0x82, &[])] {
        assert!(decode(&key(EC, &parameter, &ec_material(&optional), &[0], &[])).is_err());
    }
    assert!(decode(&key(EC, &parameter, &ec_material(&der(0x80, &[0xff])), &[0], &[])).is_ok());
}

#[test]
fn ec_ber_conversion_retains_local_backing_tail_for_java_bit_array_limits() {
    let borrowed = SecretBytes::Borrowed(&[]);
    let owned = SecretBytes::Owned(Zeroizing::new(Vec::new()));
    assert_eq!(backing_tail(&borrowed, 7, 11), 18);
    assert_eq!(backing_tail(&owned, 7, 11), 7);
    let local = (1 << 28) - 1;
    assert!(bits(&[0, 0], backing_tail(&owned, local, 0)).is_err());
    assert!(bits(&[0, 0], backing_tail(&borrowed, 0, local)).is_err());
    assert_eq!(bits(&[0, 0], backing_tail(&owned, 0, local)).unwrap().as_slice(), &[0, 0]);
    assert_eq!(bits(&[0, 0], backing_tail(&owned, local - 1, 0)).unwrap().as_slice(), &[0, 0]);
}

#[test]
fn xdh_and_eddsa_factories_require_inner_octet_lengths_and_keep_unregistered_parameters() {
    for (last, length) in [(110, 32), (111, 56), (112, 32), (113, 57)] {
        let oid = [0x2b, 0x65, last];
        for parameter in [vec![], der(2, &[1]), der(0x30, &[0xff])] {
            let private = [der(4, &vec![42; length]), vec![0xff]].concat();
            let input = key(&oid, &parameter, &private, &[0], &[]);
            assert_eq!(decoded(&input), input);
        }
        for wrong in [0, length - 1, length + 1] { assert!(decode(&named(&oid, wrong)).is_err()); }
        let constructed = der(0x24, &der(4, &vec![42; length]));
        assert!(decode(&key(&oid, &[], &constructed, &[0], &[])).is_err());
    }
}

#[test]
fn named_post_quantum_factories_accept_inner_octets_without_cryptographic_length_checks() {
    for (group, suffix) in [(3, 17), (3, 18), (3, 19), (4, 1), (4, 2), (4, 3)] {
        let oid = [0x60, 0x86, 0x48, 1, 0x65, 3, 4, group, suffix];
        for length in [0, 1, 100] { assert_eq!(decoded(&named(&oid, length)), named(&oid, length)); }
        let input = key(&oid, &[5, 0], &[der(4, &[]), vec![0xff]].concat(), &[0], &[]);
        assert_eq!(decoded(&input), key(&oid, &[], &[der(4, &[]), vec![0xff]].concat(), &[0], &[]));
        assert!(decode(&key(&oid, &der(2, &[1]), &der(4, &[]), &[0], &[])).is_err());
        assert!(decode(&key(&oid, &[], &der(0x24, &der(4, &[])), &[0], &[])).is_err());
    }
    let hss = [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 9, 16, 3, 17];
    assert!(decode(&named(&hss, 32)).err().unwrap().to_string().contains("HSS/LMS does not generate private keys"));
}

#[test]
fn provider_decode_admits_before_parsing_and_releases_failed_and_retained_allocations() {
    let bytes = named(&[0x2b, 0x65, 112], 32);
    let bound = allocation_bound(bytes.len()).unwrap();
    let rejected = MemoryBudget::new(bound - 1);
    assert_eq!(PrivateKey::decode(&bytes, &rejected).err().unwrap().code, "rtdb/memory-budget-exceeded");
    assert_eq!(rejected.used_bytes(), 0);
    let memory = MemoryBudget::new(bound);
    let key = PrivateKey::decode(&bytes, &memory).unwrap();
    assert_eq!(key.encoded_pkcs8(), bytes);
    assert!(memory.used_bytes() >= bytes.len());
    assert!(memory.used_bytes() < bound);
    drop(key); assert_eq!(memory.used_bytes(), 0);
    for malformed in [key_bytes_with_wrong_length(), vec![0x30, 0x80, 0x30, 0x80], vec![0x30, 0]] {
        assert!(PrivateKey::decode(&malformed, &memory).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
    let none = MemoryBudget::new(0);
    assert_eq!(PrivateKey::decode(&[], &none).err().unwrap().code, "rtdb/memory-budget-exceeded");
    assert!(allocation_bound(usize::MAX).is_err());
}

fn key_bytes_with_wrong_length() -> Vec<u8> { named(&[0x2b, 0x65, 112], 31) }
