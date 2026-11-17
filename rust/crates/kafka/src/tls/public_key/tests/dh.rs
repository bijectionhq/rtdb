use super::*;
use openssl::{asn1::Asn1Time, x509::X509};
use crate::tls::validation::SimpleTrust;

const PKCS3: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 3, 1];
const X942: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3e, 2, 1];
fn parameters(length: Option<&[u8]>) -> Vec<u8> {
    let mut fields = [der(2, &[23]), der(2, &[5])].concat();
    if let Some(length) = length { fields.extend(der(2, length)); }
    der(0x30, &fields)
}
fn certificate(issuer: &Credential, oid: &[u8], parameters: &[u8], y: &[u8]) -> X509 {
    let signature_algorithm = der(0x30, &[der(6, &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 11]), vec![5, 0]].concat());
    let key = der(0x30, &[der(0x30, &[der(6, oid), parameters.to_vec()].concat()), der(3, &[vec![0], der(2, y)].concat())].concat());
    let name = issuer.certificate.subject_name().to_der().unwrap();
    let signed = der(0x30, &[der(0xa0, &der(2, &[2])), der(2, &[1]), signature_algorithm.clone(), name.clone(),
        der(0x30, &[der(0x17, b"200101000000Z"), der(0x17, b"331231235959Z")].concat()), name, key].concat());
    let mut signer = openssl::sign::Signer::new(MessageDigest::sha256(), &issuer.key).unwrap();
    signer.update(&signed).unwrap();
    let encoded = der(0x30, &[signed, signature_algorithm, der(3, &[vec![0], signer.sign_to_vec().unwrap()].concat())].concat());
    let certificate = X509::from_der(&encoded).unwrap();
    assert_eq!(certificate.to_der().unwrap(), encoded);
    assert!(certificate.verify(&issuer.key).unwrap());
    certificate
}

#[test]
fn native_certificates_compare_cross_oid_dh_numbers_without_using_native_public_key_equality() {
    let issuer = Credential::new("issuer", false, None);
    let first = certificate(&issuer, PKCS3, &parameters(None), &[8]);
    let second = certificate(&issuer, X942, &parameters(Some(&[7])), &[0, 8]);
    let different = certificate(&issuer, PKCS3, &parameters(None), &[9]);
    let memory = MemoryBudget::new(16 << 20);
    assert_ne!(decode(&first, &memory).unwrap(), decode(&second, &memory).unwrap());
    assert!(same(&first, &second, &memory).unwrap());
    assert!(same(&second, &first, &memory).unwrap());
    assert!(!same(&first, &different, &memory).unwrap());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn simple_trust_replaces_a_reissued_dh_anchor_by_numeric_key_equality() {
    let issuer = Credential::new("issuer", false, None);
    let peer = [certificate(&issuer, PKCS3, &parameters(None), &[8])];
    let anchor = certificate(&issuer, X942, &parameters(Some(&[7])), &[0, 8]);
    let expected = anchor.to_der().unwrap();
    let memory = MemoryBudget::new(16 << 20);
    let trust = SimpleTrust::new(vec![anchor], memory.reserve(1024).unwrap()).unwrap();
    let selected = trust.verify(&peer, &Asn1Time::from_unix(1_800_000_000).unwrap(), |_| Ok(())).unwrap();
    assert_eq!(selected.len(), 1);
    assert_eq!(selected[0].to_der().unwrap(), expected);
    drop(selected); drop(trust); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn malformed_dh_provider_fields_map_to_invalid_tls_config_even_for_native_self_comparison() {
    let issuer = Credential::new("issuer", false, None);
    let good = certificate(&issuer, PKCS3, &parameters(None), &[8]);
    let memory = MemoryBudget::new(16 << 20);
    for parameters in [parameters(Some(&[0, 0x80, 0, 0, 0])), der(0x30, &der(2, &[23]))] {
        let bad = certificate(&issuer, X942, &parameters, &[8]);
        for (left, right) in [(&bad, &bad), (&good, &bad), (&bad, &good)] {
            assert_eq!(same(left, right, &memory).unwrap_err().code, "xtdb.kafka/invalid-tls-config");
            assert_eq!(memory.used_bytes(), 0);
        }
    }
}
