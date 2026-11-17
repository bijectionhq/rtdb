use super::*;

const OAEP: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 7];
const P_SPECIFIED: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 9];
const SPECIFIED_SHA2: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 4, 3];
const SHA256: &[u8] = &[0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, 1];

fn nested_key(oid: &[u8], parameter: &[u8]) -> Vec<u8> {
    key(PSS, &pss(&[der(0xa0, &algorithm(oid, parameter))]), &rsa_material(&components()), &[0], &[])
}

fn hash(oid: &[u8], parameter: &[u8]) -> Vec<u8> { der(0xa0, &algorithm(oid, parameter)) }
fn mgf(oid: &[u8], parameter: &[u8]) -> Vec<u8> { der(0xa1, &algorithm(MGF1, &algorithm(oid, parameter))) }
fn label(parameter: &[u8]) -> Vec<u8> { der(0xa2, &algorithm(P_SPECIFIED, parameter)) }

#[test]
fn nested_pss_and_oaep_admit_defaults_absence_and_null_while_crt_retains_parameters() {
    for oid in [PSS, OAEP] {
        for parameter in [vec![], der(5, &[]), der(0x30, &[])] {
            let input = nested_key(oid, &parameter);
            assert_eq!(decoded(&input), input);
        }
        for parameter in [der(5, &[0]), vec![0x30], vec![0x30, 1], [der(0x30, &[]), der(5, &[])].concat()] {
            assert!(decode(&nested_key(oid, &parameter)).is_err());
        }
    }
}

#[test]
fn nested_oaep_accepts_any_outer_value_tag_but_nested_pss_requires_sequence() {
    for tag in [2, 4, 0x31, 0x80, 0xa0, 0xe1] {
        for content in [vec![], hash(UNKNOWN, &der(2, &[7]))] {
            let parameter = der(tag, &content);
            let input = nested_key(OAEP, &parameter);
            assert_eq!(decoded(&input), input);
            assert!(decode(&nested_key(PSS, &parameter)).is_err());
        }
    }
}

#[test]
fn nested_oaep_accepts_ordered_optional_primitive_or_constructed_context_fields() {
    for constructed in [false, true] {
        let mut fields = [hash(UNKNOWN, &der(2, &[7])), mgf(SHA256, &der(2, &[9])), label(&der(4, b"label"))];
        if !constructed { for field in &mut fields { field[0] &= !0x20; } }
        for mask in 0usize..8 {
            let chosen = fields.iter().enumerate().filter(|(index, _)| mask & (1usize << *index) != 0).map(|(_, field)| field.clone()).collect::<Vec<_>>();
            let input = nested_key(OAEP, &pss(&chosen));
            assert_eq!(decoded(&input), input);
        }
    }
}

#[test]
fn nested_oaep_rejects_duplicate_reordered_unknown_and_nonexact_context_fields() {
    let hash = hash(SHA1, &[]);
    let mask = mgf(SHA1, &[]);
    let source = label(&der(4, &[]));
    for fields in [
        vec![hash.clone(), hash.clone()], vec![mask.clone(), mask.clone()], vec![source.clone(), source.clone()],
        vec![mask.clone(), hash.clone()], vec![source.clone(), mask.clone()], vec![source, hash],
        vec![der(0xa3, &der(2, &[1]))], vec![der(0x60, &algorithm(SHA1, &[]))],
        vec![der(0xa0, &[])], vec![der(0xa0, &[algorithm(SHA1, &[]), der(5, &[])].concat())],
        vec![der(0xa1, &[algorithm(MGF1, &algorithm(SHA1, &[])), vec![0xff]].concat())],
        vec![der(0xa2, &[algorithm(P_SPECIFIED, &der(4, &[])), der(5, &[])].concat())],
    ] { assert!(decode(&nested_key(OAEP, &pss(&fields))).is_err()); }
}

#[test]
fn nested_pss_keeps_duplicate_last_values_context_tails_and_unordered_fields() {
    let parameter = pss(&[
        der(0x83, &[der(2, &[1]), vec![0xff]].concat()),
        der(0xa2, &der(2, &[0])),
        der(0x80, &[algorithm(OAEP, &der(0x31, &[])), vec![0xff]].concat()),
        der(0x81, &[algorithm(MGF1, &algorithm(SHA256, &[])), vec![0xff]].concat()),
        hash(PSS, &der(0x30, &[])),
        der(0x82, &[der(2, &[0, 32]), vec![0xff]].concat()),
        mgf(SHA1, &[]),
    ]);
    let input = nested_key(PSS, &parameter);
    assert_eq!(decoded(&input), input);
    for bad in [der(0xa2, &der(2, &[0xff])), der(0xa3, &der(2, &[2])), der(0xa2, &der(2, &[0, 0x80, 0, 0, 0]))] {
        assert!(decode(&nested_key(PSS, &pss(&[bad, der(0xa2, &der(2, &[20])), der(0xa3, &der(2, &[1]))]))).is_err());
    }
}

#[test]
fn nested_pss_and_oaep_mgf_allowlists_differ_for_sha3() {
    let mut digests = vec![SHA1.to_vec()];
    digests.extend((1..=10).map(|suffix| vec![0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, suffix]));
    for (index, digest) in digests.iter().enumerate() {
        let parameter = pss(&[mgf(digest, &der(2, &[99]))]);
        assert!(decode(&nested_key(PSS, &parameter)).is_ok());
        assert_eq!(decode(&nested_key(OAEP, &parameter)).is_ok(), index <= 6);
    }
    for oid in [PSS, OAEP] {
        for field in [
            der(0xa1, &algorithm(MGF1, &[])), der(0xa1, &algorithm(MGF1, &der(5, &[]))),
            der(0xa1, &algorithm(UNKNOWN, &algorithm(SHA1, &[]))), mgf(UNKNOWN, &[]),
            mgf(PSS, &der(0x30, &[])), mgf(OAEP, &der(0x30, &[])),
        ] { assert!(decode(&nested_key(oid, &pss(&[field]))).is_err()); }
    }
}

#[test]
fn nested_oaep_pspecified_labels_flatten_constructed_octets_and_require_present_parameters() {
    for label_value in [
        der(4, &[]), der(4, b"label"), der(0x24, &[]),
        der(0xa4, &[der(4, b"a"), der(0x24, &der(4, b"bc"))].concat()),
        [vec![0xe4, 0x80], der(4, b"abc"), vec![0, 0]].concat(),
    ] {
        let input = nested_key(OAEP, &pss(&[label(&label_value)]));
        assert_eq!(decoded(&input), input);
    }
    for field in [
        label(&[]), label(&der(5, &[])), label(&der(2, &[1])), label(&der(0x24, &der(2, &[1]))),
        label(&[der(4, &[]), der(4, &[])].concat()), der(0xa2, &algorithm(UNKNOWN, &der(4, &[]))),
    ] { assert!(decode(&nested_key(OAEP, &pss(&[field]))).is_err()); }
}

#[test]
fn recursive_pss_oaep_validate_inner_identifier_cardinality_and_all_fixed_services() {
    let dsa = pss(&[der(2, &[0xff]), der(2, &[0]), der(2, &[1])]);
    let dh = pss(&[der(2, &[1]), der(2, &[2]), der(2, &[0])]);
    let curve = der(6, &[0x2a, 0x86, 0x48, 0xce, 0x3d, 3, 1, 7]);
    for (oid, parameter) in [(DSA, dsa), (DH, dh), (EC, curve),
        (&[0x60, 0x86, 0x48, 1, 0x65, 3, 4, 1][..], der(4, &[0; 16])),
        (&[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 3][..], pss(&[der(4, &[]), der(2, &[0xff])]))] {
        for outer in [PSS, OAEP] {
            let input = nested_key(outer, &pss(&[hash(oid, &parameter)]));
            assert_eq!(decoded(&input), input);
            assert!(decode(&nested_key(outer, &pss(&[hash(oid, &der(2, &[0]))]))).is_err());
        }
    }
    for inner in [der(0x31, &der(6, SHA1)), der(0x30, &[]), der(0x30, &der(2, SHA1)),
        der(0x30, &[der(6, SHA1), der(5, &[]), der(5, &[])].concat()), algorithm(SHA1, &der(5, &[0]))] {
        for outer in [PSS, OAEP] { assert!(decode(&nested_key(outer, &pss(&[der(0xa0, &inner)]))).is_err()); }
    }
}

#[test]
fn recursive_parameter_initialization_precedes_mgf_and_psource_oid_rejection() {
    for field in [mgf(SPECIFIED_SHA2, &der(0x30, &[])), der(0xa1, &algorithm(SPECIFIED_SHA2, &der(0x30, &[]))),
        der(0xa2, &algorithm(SPECIFIED_SHA2, &der(0x30, &[])))] {
        let error = decode(&nested_key(OAEP, &pss(&[field]))).err().unwrap();
        assert!(error.to_string().contains("Malformed Java private key"));
    }
}

#[test]
fn pss_preframes_all_fields_but_oaep_initializes_each_field_before_reading_the_next() {
    let content = [hash(SPECIFIED_SHA2, &der(0x30, &[])), vec![0xff]].concat();
    let pss_error = decode(&nested_key(PSS, &der(0x30, &content))).err().unwrap();
    assert!(pss_error.to_string().contains("Malformed certificate identifier"));
    let oaep_error = decode(&nested_key(OAEP, &der(0x30, &content))).err().unwrap();
    assert!(oaep_error.to_string().contains("Malformed certificate identifier"));
    let nonexact = der(0xa0, &[algorithm(SPECIFIED_SHA2, &der(0x30, &[])), der(5, &[])].concat());
    assert!(decode(&nested_key(OAEP, &pss(&[nonexact]))).err().unwrap().to_string().contains("Malformed certificate identifier"));
    let content = [hash(DSA, &der(4, &[])), vec![0xff]].concat();
    let pss_error = decode(&nested_key(PSS, &der(0x30, &content))).err().unwrap();
    assert!(pss_error.to_string().contains("Malformed certificate identifier"));
    let oaep_error = decode(&nested_key(OAEP, &der(0x30, &content))).err().unwrap();
    assert!(oaep_error.to_string().contains("Malformed Java private key"));
}

#[test]
fn recursive_parameter_ber_frames_retain_original_crt_bytes_and_normalize_only_validation_views() {
    for outer in [PSS, OAEP] {
        let nested = [vec![0x30, 0x80], hash(SHA256, &[5, 0]), vec![0, 0]].concat();
        let alg = [vec![0x30, 0x80], der(6, outer), nested, vec![0, 0]].concat();
        let context = [vec![0xa0, 0x80], alg, vec![0, 0]].concat();
        let input = nested_key(PSS, &pss(&[context]));
        assert_eq!(decoded(&input), input);
        let padded = vec![0x30, 0x84, 0, 0, 0, 0];
        assert!(decode(&nested_key(outer, &padded)).is_ok());
    }
}

#[test]
fn noncrt_reconstruction_retains_only_the_outer_nested_service_name_after_recursive_validation() {
    for oid in [PSS, OAEP] {
        let mut values = components(); values[3] = vec![0];
        let nested = pss(&[hash(OAEP, &pss(&[hash(PSS, &der(0x30, &[]))]))]);
        let original = pss(&[hash(oid, &nested)]);
        let input = key(PSS, &original, &rsa_material(&values), &[0], &[]);
        values[0].insert(0, 0);
        for index in [1, 3, 4, 5, 6, 7] { values[index] = vec![0]; }
        let expected = key(PSS, &pss(&[hash(oid, &[])]), &rsa_material(&values), &[0], &[]);
        assert_eq!(decoded(&input), expected);
    }
}

#[test]
fn deep_alternating_parameter_nesting_uses_admitted_iterative_frames_and_releases_failures() {
    for depth in [1, 16, 128, 1024] {
        let mut parameter = der(0x30, &[]);
        for level in 0..depth { parameter = pss(&[hash(if level % 2 == 0 { OAEP } else { PSS }, &parameter)]); }
        let input = nested_key(PSS, &parameter);
        let bound = allocation_bound(input.len()).unwrap();
        assert_eq!(bound, 384 * input.len() + 65_536);
        let insufficient = MemoryBudget::new(bound - 1);
        assert_eq!(PrivateKey::decode(&input, &insufficient).err().unwrap().code, "rtdb/memory-budget-exceeded");
        assert_eq!(insufficient.used_bytes(), 0);
        let memory = MemoryBudget::new(bound);
        let key = PrivateKey::decode(&input, &memory).unwrap();
        assert_eq!(key.encoded_pkcs8(), input);
        assert!(memory.used_bytes() > 0 && memory.used_bytes() < bound);
        drop(key);
        assert_eq!(memory.used_bytes(), 0);
        let mut malformed = der(0x30, &[0xff]);
        for level in 0..depth { malformed = pss(&[hash(if level % 2 == 0 { OAEP } else { PSS }, &malformed)]); }
        let malformed = nested_key(PSS, &malformed);
        let memory = MemoryBudget::new(allocation_bound(malformed.len()).unwrap());
        assert!(PrivateKey::decode(&malformed, &memory).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
}
