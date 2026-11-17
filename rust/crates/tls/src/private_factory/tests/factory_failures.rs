use super::*;

const HSS: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 9, 16, 3, 17];
const SPECIFIED: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 4, 3];
const PBES2: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 13];

fn pbes2() -> Vec<u8> {
    let kdf = algorithm(&[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 12], &pss(&[der(4, &[]), der(2, &[1])]));
    let cipher = algorithm(&[0x60, 0x86, 0x48, 1, 0x65, 3, 4, 1, 2], &der(4, &[]));
    pss(&[kdf, cipher])
}

#[test]
fn missing_factory_and_known_hss_private_rejection_have_distinct_failure_classes() {
    let input = key(UNKNOWN, &[], &[], &[0], &[]);
    let error = attempt(&input).err().unwrap();
    assert!(matches!(&error, FactoryFailure::NoSuchAlgorithm(_)));
    assert_eq!(error.into_anomaly(), invalid("Unsupported Java private-key factory"));
    assert_eq!(decode(&input).err().unwrap(), invalid("Unsupported Java private-key factory"));
    let input = key(HSS, &[], &[], &[0], &[]);
    let error = attempt(&input).err().unwrap();
    assert!(matches!(&error, FactoryFailure::InvalidKeySpec(_)));
    assert_eq!(error.into_anomaly(), invalid("Java HSS/LMS does not generate private keys"));
    assert_eq!(decode(&input).err().unwrap(), invalid("Java HSS/LMS does not generate private keys"));
}

#[test]
fn typed_factory_lookup_keeps_the_existing_strict_framing_and_version_order() {
    for version in [&[2][..], &[0xff], &[0, 0x80, 0, 0, 0]] {
        let input = key(UNKNOWN, &[], &[], version, &[]);
        let error = attempt(&input).err().unwrap();
        if version.len() > 4 {
            assert!(matches!(&error, FactoryFailure::InvalidKeySpec(_)));
            assert_eq!(error.into_anomaly(), malformed());
        } else {
            assert!(matches!(&error, FactoryFailure::NoSuchAlgorithm(_)));
            assert_eq!(error.into_anomaly(), invalid("Unsupported Java private-key factory"));
        }
    }
    let invalid = der(0x30, &[der(2, &[0]), der(0x30, &der(6, &[])), der(4, &[])].concat());
    let error = attempt(&invalid).err().unwrap();
    assert!(matches!(&error, FactoryFailure::InvalidKeySpec(_)));
    assert_eq!(error.into_anomaly(), crate::java_der::malformed());
    let input = key(RSA, &[], &rsa_material(&components()), &[2], &[]);
    let error = attempt(&input).err().unwrap();
    assert!(matches!(&error, FactoryFailure::InvalidKeySpec(_)));
    assert_eq!(error.into_anomaly(), malformed());
}

#[test]
fn provider_pkcs8_parameter_and_material_errors_are_invalid_key_spec() {
    let curve = der(6, curves::CURVES[0].1);
    let short_rsa = vec![vec![1]; 8];
    for input in [
        vec![], vec![0x30], der(4, &[]), rsa_key(&short_rsa),
        key(RSA, &[], &der(2, &[0]), &[0], &[]),
        key(PSS, &der(4, &[]), &rsa_material(&components()), &[0], &[]),
        key(DSA, &[], &der(4, &[1]), &[0], &[]),
        key(DH, &[], &der(2, &[1]), &[0], &[]),
        key(EC, &[], &ec_material(&[]), &[0], &[]),
        key(EC, &curve, &der(0x30, &[der(2, &[2]), der(4, &[])].concat()), &[0], &[]),
        key(&[0x2b, 0x65, 112], &[], &der(4, &[0; 31]), &[0], &[]),
        key(&[0x60, 0x86, 0x48, 1, 0x65, 3, 4, 3, 17], &der(4, &[]), &der(4, &[]), &[0], &[]),
    ] {
        let expected = decode(&input).err().unwrap();
        let error = attempt(&input).err().unwrap();
        assert!(matches!(&error, FactoryFailure::InvalidKeySpec(_)));
        assert_eq!(error.into_anomaly(), expected);
    }
}

#[test]
fn noncrt_pss_reverse_name_encoding_is_fatal_while_crt_bytes_remain_admitted() {
    for (digest, parameter) in [(SPECIFIED, algorithm(UNKNOWN, &[])), (PBES2, pbes2()),
        (&[0x2b, 6, 1, 5, 5, 7, 3, 8][..], vec![])] {
        let parameter = pss(&[der(0xa0, &algorithm(digest, &parameter))]);
        let crt = key(PSS, &parameter, &rsa_material(&components()), &[0], &[]);
        assert_eq!(attempt(&crt).unwrap().as_slice(), crt);
        let mut values = components(); values[3] = vec![0];
        let input = key(PSS, &parameter, &rsa_material(&values), &[0], &[]);
        let error = attempt(&input).err().unwrap();
        assert!(matches!(&error, FactoryFailure::Fatal(_)));
        assert_eq!(error.into_anomaly(), malformed());
        assert_eq!(decode(&input).err().unwrap(), malformed());
    }
}
