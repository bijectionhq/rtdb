use super::*;

#[derive(Clone, Copy, Debug)]
enum Expected { Missing, Hss, Retained, Preparation, Provider }

const CASES: &[(&str, &str, Expected)] = &[
    ("set_unknown_without_private", "310a020100300506032a0304", Expected::Missing),
    ("unknown_nonempty_null", "300d020100300806032a0304050100", Expected::Missing),
    ("unknown_nonempty_null_invalid_version_and_tail", "300e0201ff300806032a0304050100ff", Expected::Missing),
    ("hss_nonempty_null_no_private", "30150201003010060b2a864886f70d0109100311050100", Expected::Hss),
    ("set_hss_invalid_version_no_private", "3112020102300d060b2a864886f70d0109100311", Expected::Hss),
    ("hss_invalid_version_unread_tail", "3013020102300d060b2a864886f70d0109100311ff", Expected::Hss),
    ("unknown_negative_version_no_private", "300a0201ff300506032a0304", Expected::Missing),
    ("unknown_version_above_i32", "300e02050080000000300506032a0304", Expected::Preparation),
    ("unknown_empty_version", "30090200300506032a0304", Expected::Preparation),
    ("unknown_redundant_version_padding", "300c0203000000300506032a0304", Expected::Missing),
    ("outer_bytes_after_value", "300a020100300506032a030400", Expected::Preparation),
    ("indefinite_outer_bytes_after_value", "3080020100300506032a03040000ff", Expected::Preparation),
    ("primitive_outer_unknown", "040a020100300506032a0304", Expected::Preparation),
    ("empty_algorithm_sequence", "30050201003000", Expected::Preparation),
    ("three_algorithm_children_before_invalid_oid", "300b0201003006060005000500", Expected::Preparation),
    ("malformed_second_frame_before_invalid_oid", "30080201003003060005", Expected::Preparation),
    ("unknown_opaque_inner_bad_frame", "300d020100300806032a03043001ff", Expected::Missing),
    ("specified_uninitialized_opaque_parameter", "3011020100300c06072a8648ce3d04033001ff", Expected::Missing),
    ("pbes2_uninitialized_opaque_parameter", "3013020100300e06092a864886f70d01050d3001ff", Expected::Missing),
    ("oaep_uninitialized_opaque_parameter", "3013020100300e06092a864886f70d0101073001ff", Expected::Missing),
    ("specified_child_sha256_does_not_rename", "301b020100301606072a8648ce3d0403300b0609608648016503040201", Expected::Missing),
    ("known_rsa_set_outer_reaches_provider", "3110020100300b06092a864886f70d010101", Expected::Provider),
    ("dh_original_ber_retained", "308002020000301306092a864886f70d0103013006020102020103040502020001ff0000", Expected::Retained),
    ("dh_x942_selects_diffiehellman", "301d020100301106072a8648ce3e02013006020102020103040502020001ff", Expected::Retained),
    ("dh_original_v0_attributes_rejected", "3021020100301306092a864886f70d0103013006020102020103040502020001ff8000", Expected::Provider),
    ("dh_original_v1_rejected", "301f020101301306092a864886f70d0103013006020102020103040502020001ff", Expected::Provider),
    ("rsa_bad_version_precedes_bad_null_in_provider", "3013020102300e06092a864886f70d010101050100", Expected::Provider),
];

fn bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes().chunks_exact(2).map(|pair| {
        let digit = |byte: u8| if byte <= b'9' { byte - b'0' } else { byte - b'a' + 10 };
        digit(pair[0]) * 16 + digit(pair[1])
    }).collect()
}

#[test]
fn strict_pkcs12_source_fixtures_preserve_all_preparse_lookup_and_provider_stages() {
    assert_eq!(CASES.len(), 27);
    for &(name, hex, expected) in CASES {
        let input = bytes(hex);
        let prepared = pkcs12::prepare(&input);
        match expected {
            Expected::Preparation => assert!(prepared.is_err(), "{name}"),
            Expected::Missing => {
                let prepared = prepared.unwrap();
                assert_eq!(prepared.target, FactoryTarget::NoSuchAlgorithm, "{name}");
                let error = prepared.generate().err().unwrap();
                assert!(matches!(&error, FactoryFailure::NoSuchAlgorithm(_)), "{name}");
                assert_eq!(error.into_anomaly(), invalid("Unsupported Java private-key factory"), "{name}");
            }
            Expected::Hss => {
                let prepared = prepared.unwrap();
                assert_eq!(prepared.target, FactoryTarget::InvalidKeySpec, "{name}");
                let error = prepared.generate().err().unwrap();
                assert!(matches!(&error, FactoryFailure::InvalidKeySpec(_)), "{name}");
                assert_eq!(error.into_anomaly(), invalid("Java HSS/LMS does not generate private keys"), "{name}");
            }
            Expected::Retained => assert_eq!(prepared.unwrap().generate().unwrap().as_slice(), input, "{name}"),
            Expected::Provider => {
                let prepared = prepared.unwrap();
                assert!(matches!(prepared.target, FactoryTarget::Available(_)), "{name}");
                assert!(matches!(prepared.generate(), Err(FactoryFailure::InvalidKeySpec(_))), "{name}");
            }
        }
    }
}

#[test]
fn strict_pkcs12_reads_all_identifier_frames_and_cardinality_before_interpreting_the_oid() {
    for children in [vec![der(2, &[1]), vec![5]], vec![der(2, &[1]), der(5, &[]), der(5, &[])]] {
        let input = der(0x30, &[der(2, &[0]), der(0x30, &children.concat())].concat());
        let error = pkcs12::prepare(&input).err().unwrap();
        assert!(matches!(&error, pkcs12::PreparationFailure::Der(_)));
        assert_eq!(error.into_pkcs12(), crate::java_der::malformed());
    }
    let input = der(0x30, &[der(2, &[0]), der(0x30, &der(2, &[1]))].concat());
    assert!(matches!(pkcs12::prepare(&input), Err(pkcs12::PreparationFailure::Invalid)));
}

#[test]
fn strict_pkcs12_integer_consumption_accepts_every_i32_before_lookup_and_rejects_overflow() {
    for version in [vec![0xff], vec![2], vec![0x80, 0, 0, 0], vec![0x7f, 0xff, 0xff, 0xff], vec![0, 0, 0], vec![0xff, 0xff]] {
        let input = key(UNKNOWN, &[], &[], &version, &[]);
        assert_eq!(pkcs12::prepare(&input).unwrap().target, FactoryTarget::NoSuchAlgorithm);
    }
    for version in [vec![], vec![0, 0x80, 0, 0, 0], vec![0xff, 0x7f, 0xff, 0xff, 0xff]] {
        let input = key(UNKNOWN, &[], &[], &version, &[]);
        assert!(matches!(pkcs12::prepare(&input), Err(pkcs12::PreparationFailure::Invalid)));
    }
    let input = der(0x30, &[der(4, &[0]), algorithm(UNKNOWN, &[])].concat());
    assert!(matches!(pkcs12::prepare(&input), Err(pkcs12::PreparationFailure::Invalid)));
}

#[test]
fn strict_pkcs12_factory_selection_keeps_registered_aliases_and_original_dh_bytes() {
    for oid in [RSA, &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1], &[0x55, 8, 1, 1]] {
        let input = key(oid, &[], &rsa_material(&components()), &[0], &[]);
        assert_eq!(pkcs12::prepare(&input).unwrap().target, FactoryTarget::Available(Family::Rsa));
        assert!(attempt(&input).is_ok());
    }
    let parameters = pss(&[der(2, &[1]), der(2, &[2])]);
    let private = der(2, &[1]);
    let input = key(DH, &parameters, &private, &[0], &der(0x80, &[]));
    assert!(decode(&input).is_err());
    let memory = MemoryBudget::new(4 << 20);
    let key = PrivateKey::parse_jks(&input, &memory).unwrap();
    assert_eq!(key.encoded_pkcs8(), super::key(DH, &parameters, &private, &[0], &[]));
    drop(key); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn strict_pkcs12_preparation_and_factory_admission_preserve_their_separate_caller_order() {
    let none = MemoryBudget::new(0);
    assert!(FactoryInput::pkcs12(&[]).is_err());
    assert_eq!(PrivateKey::decode(&[], &none).err().unwrap().code, "rtdb/memory-budget-exceeded");
    let input = key(UNKNOWN, &[], &[], &[0], &[]);
    let prepared = FactoryInput::pkcs12(&input).unwrap();
    assert_eq!(PrivateKey::from_pkcs12(prepared, &none).err().unwrap().code(), "rtdb/memory-budget-exceeded");
    assert_eq!(none.used_bytes(), 0);
    let bound = allocation_bound(input.len()).unwrap();
    let memory = MemoryBudget::new(bound);
    let prepared = FactoryInput::pkcs12(&input).unwrap();
    assert_eq!(PrivateKey::from_pkcs12(prepared, &memory).err().unwrap().into_anomaly(), invalid("Unsupported Java private-key factory"));
    assert_eq!(memory.used_bytes(), 0);
    let input = named(&[0x2b, 0x65, 112], 32);
    let memory = MemoryBudget::new(allocation_bound(input.len()).unwrap());
    let prepared = FactoryInput::pkcs12(&input).unwrap();
    let key = PrivateKey::from_pkcs12(prepared, &memory).unwrap();
    assert_eq!(key.encoded_pkcs8(), input);
    assert_eq!(memory.used_bytes(), key.retained_bytes());
    drop(key); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn strict_preparation_failure_adapters_keep_der_shape_and_unchecked_categories() {
    assert_eq!(pkcs12::PreparationFailure::Der(crate::java_der::malformed()).into_pkcs12(), crate::java_der::malformed());
    assert_eq!(pkcs12::PreparationFailure::Invalid.into_pkcs12(), invalid("Malformed SunX509 PKCS12 material"));
    assert!(matches!(pkcs12::PreparationFailure::Invalid.into_factory(), FactoryFailure::InvalidKeySpec(_)));
    assert!(matches!(pkcs12::PreparationFailure::Unchecked.into_factory(), FactoryFailure::Fatal(_)));
}

#[test]
fn strict_pkcs12_known_pss_version_rejection_precedes_registered_parameter_initialization() {
    let parameter = der(0x30, &[0xa0]);
    for (version, expected) in [(0, crate::java_der::malformed()), (2, malformed())] {
        let input = key(PSS, &parameter, &rsa_material(&components()), &[version], &[]);
        let prepared = FactoryInput::pkcs12(&input).unwrap();
        assert_eq!(prepared.target, FactoryTarget::Available(Family::Pss));
        let error = prepared.generate().err().unwrap();
        assert!(matches!(&error, FactoryFailure::InvalidKeySpec(_)));
        assert_eq!(error.into_anomaly(), expected);
    }
}
