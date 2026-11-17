use super::*;
use openssl::{asn1::{Asn1Object, Asn1OctetString, Asn1Time}, bn::BigNum, hash::MessageDigest, pkey::PKey, rsa::Rsa, x509::{X509, X509Extension, X509NameBuilder}};

fn usage(critical: bool, value: &[u8]) -> X509Extension {
    X509Extension::new_from_der(&Asn1Object::from_str("2.5.29.15").unwrap(), critical, &Asn1OctetString::new_from_bytes(value).unwrap()).unwrap()
}

fn certificate(extensions: Vec<X509Extension>) -> X509 {
    let key = PKey::from_rsa(Rsa::generate(1024).unwrap()).unwrap();
    let mut name = X509NameBuilder::new().unwrap(); name.append_entry_by_text("CN", "client usage").unwrap(); let name = name.build();
    let mut certificate = X509::builder().unwrap(); certificate.set_version(2).unwrap();
    certificate.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap()).unwrap();
    certificate.set_subject_name(&name).unwrap(); certificate.set_issuer_name(&name).unwrap(); certificate.set_pubkey(&key).unwrap();
    certificate.set_not_before(&Asn1Time::from_unix(1_600_000_000).unwrap()).unwrap(); certificate.set_not_after(&Asn1Time::from_unix(1_800_000_000).unwrap()).unwrap();
    for extension in extensions { certificate.append_extension(extension).unwrap(); }
    certificate.sign(&key, MessageDigest::sha256()).unwrap(); certificate.build()
}

#[test]
fn every_client_chain_position_uses_java_key_usage_instead_of_native_extension_caches() {
    let memory = MemoryBudget::new(4 << 20);
    let plain = certificate(vec![]);
    for malformed_first in [false, true] {
        let mut extensions = vec![usage(false, &[3, 2, 0, 0x80]), usage(false, &[5, 0])];
        if malformed_first { extensions.reverse(); }
        let parsed = certificate(extensions);
        let malformed = certificate(vec![usage(false, &[5, 0])]);
        for changed in [&parsed, &malformed] {
            for chain in [[changed.as_ref(), plain.as_ref()], [plain.as_ref(), changed.as_ref()]] {
                assert!(java_client_chain_eligible(&chain, &memory).unwrap());
                assert!(super::super::verify_client_chain(&chain).is_err());
                assert_eq!(memory.used_bytes(), 0);
            }
        }
    }
}

#[test]
fn client_chain_primitive_gate_masks_declared_bits_and_accepts_java_octet_wrapping() {
    let memory = MemoryBudget::new(4 << 20);
    for (value, valid) in [
        (vec![3, 2, 0, 1], false), (vec![3, 2, 0, 8], true), (vec![3, 2, 0, 4], true),
        (vec![3, 2, 7, 4], false), (vec![3, 2, 7, 0x84], true),
        (vec![4, 4, 3, 2, 0, 4], true), (vec![3, 0x81, 2, 0, 4], true),
    ] {
        let certificate = certificate(vec![usage(false, &value)]);
        assert_eq!(java_client_chain_eligible(&[&certificate], &memory).unwrap(), valid, "{value:?}");
        assert_eq!(memory.used_bytes(), 0);
    }
    for extensions in [vec![usage(true, &[5, 0])], vec![usage(false, &[3, 2, 0, 0x80]), usage(false, &[3, 2, 0, 0x80])]] {
        assert!(!java_client_chain_eligible(&[&certificate(extensions)], &memory).unwrap());
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn client_chain_admission_failure_is_an_error_and_native_policy_rejection_is_ineligible() {
    let certificate = certificate(vec![]);
    let bound = certificate.to_der().unwrap().len() + 64;
    for (limit, peak) in [(bound - 1, 0), (bound, bound)] {
        let memory = MemoryBudget::new(limit);
        assert_eq!(java_client_chain_eligible(&[&certificate], &memory).unwrap_err().code, "rtdb/memory-budget-exceeded");
        assert_eq!(memory.used_bytes(), 0);
        assert_eq!(memory.peak_bytes(), peak);
    }
    let memory = MemoryBudget::new(4 << 20);
    let weak = super::super::tests::certificate(512, MessageDigest::sha256(), 1_700_000_000, None);
    let digest = super::super::tests::certificate(1024, MessageDigest::md5(), 1_700_000_000, None);
    assert!(!java_client_chain_eligible(&[&weak], &memory).unwrap());
    assert!(!java_client_chain_eligible(&[&digest], &memory).unwrap());
    assert_eq!(memory.used_bytes(), 0);
}
