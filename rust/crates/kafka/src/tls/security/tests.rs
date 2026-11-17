use super::*;
use openssl::{bn::BigNum, ec::{EcGroup, EcKey}, pkey::PKey, rsa::Rsa, x509::{X509, X509NameBuilder, extension::KeyUsage}};
use rtdb_columnar::MemoryBudget;

fn fingerprint(hex: &str) -> Fingerprint {
    let mut bytes = [0; 32];
    for (index, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap();
    }
    Fingerprint(bytes)
}

fn certificate(bits: u32, digest: MessageDigest, issued: i64, usage: Option<KeyUsage>) -> X509 {
    let key = PKey::from_rsa(Rsa::generate(bits).unwrap()).unwrap();
    let mut name = X509NameBuilder::new().unwrap(); name.append_entry_by_text("CN", "policy fixture").unwrap(); let name = name.build();
    let mut certificate = X509::builder().unwrap(); certificate.set_version(2).unwrap();
    certificate.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap()).unwrap();
    certificate.set_subject_name(&name).unwrap(); certificate.set_issuer_name(&name).unwrap(); certificate.set_pubkey(&key).unwrap();
    certificate.set_not_before(&Asn1Time::from_unix(issued).unwrap()).unwrap();
    certificate.set_not_after(&Asn1Time::from_unix(2_000_000_000).unwrap()).unwrap();
    if let Some(usage) = usage { certificate.append_extension(usage.build().unwrap()).unwrap(); }
    certificate.sign(&key, digest).unwrap(); certificate.build()
}

#[test]
fn immutable_resources_pin_runtime_blocklist_and_jdk_anchor_fingerprints() {
    let digest = |bytes: &[u8]| openssl::sha::sha256(bytes).iter().map(|byte| format!("{byte:02x}")).collect::<String>();
    assert_eq!(digest(BLOCKED.as_bytes()), "96572f243f31c2ef81a6e627542e596f6a9295cff3c7ae095c1b595cb1457ded");
    assert_eq!(digest(JDK_ANCHORS.as_bytes()), "cc9fd59d407106b5c5f00a19069276a673aab4b526ca0e2a0535d9314dd16226");
    assert_eq!(BLOCKED.lines().filter(|line| line.len() == 64).count(), 38);
    assert_eq!(JDK_ANCHORS.lines().count(), 146);
    for line in BLOCKED.lines().filter(|line| line.len() == 64) { assert!(blocked(fingerprint(line)).is_err()); }
    assert!(blocked(Fingerprint([0; 32])).is_ok());
    let memory = MemoryBudget::new(32 << 20);
    let charge = memory.reserve(crate::tls::trust::allocation_bound().unwrap()).unwrap();
    let store = crate::tls::trust::decode(charge).unwrap();
    for certificate in store.trusted_certificates() {
        assert!(Fingerprint::of(&X509::from_der(certificate).unwrap()).unwrap().listed(JDK_ANCHORS));
    }
    drop(store); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn direct_trust_skips_algorithm_checks_but_selection_checks_every_certificate() {
    let weak = certificate(512, MessageDigest::md5(), 1_700_000_000, None);
    assert!(verify_server_chain(&[&weak]).is_ok());
    assert!(verify_client_chain(&[&weak]).is_err());
    let root = certificate(1024, MessageDigest::sha256(), 1_600_000_000, None);
    assert!(verify_server_chain(&[&weak, &root]).is_err());
    let leaf = certificate(1024, MessageDigest::sha256(), 1_700_000_000, None);
    assert!(verify_server_chain(&[&leaf, &root]).is_ok());
    assert!(verify_server_chain(&[&leaf, &weak]).is_err());
    assert!(verify_server_chain(&[]).is_err());
}

#[test]
fn sha1_is_restricted_only_for_server_chains_to_the_pinned_jdk_anchors() {
    let leaf = certificate(1024, MessageDigest::sha1(), 1_800_000_000, None);
    let private_root = certificate(1024, MessageDigest::sha256(), 1_600_000_000, None);
    assert!(verify_server_chain(&[&leaf, &private_root]).is_ok());
    let memory = MemoryBudget::new(32 << 20);
    let charge = memory.reserve(crate::tls::trust::allocation_bound().unwrap()).unwrap();
    let store = crate::tls::trust::decode(charge).unwrap();
    let jdk_root = X509::from_der(&store.trusted_certificates()[0]).unwrap();
    assert!(verify_server_chain(&[&leaf, &jdk_root]).is_err());
    assert!(verify_client_chain(&[&leaf, &jdk_root]).is_ok());
    assert!(digest(Nid::MD2, false).is_err());
    assert!(digest(Nid::MD5, false).is_err());
    assert!(digest(Nid::SHA256, true).is_ok());
}

#[test]
fn exact_key_size_rules_keep_rsa_pss_and_unknown_dsa_parameters_distinct() {
    for algorithm in [Id::RSA, Id::DSA, Id::DH, Id::DHX] {
        assert!(Key { algorithm, bits: Some(1023) }.check().is_err());
        assert!(Key { algorithm, bits: Some(1024) }.check().is_ok());
    }
    assert!(Key { algorithm: Id::RSA_PSS, bits: Some(768) }.check().is_ok());
    assert!(Key { algorithm: Id::EC, bits: Some(223) }.check().is_err());
    assert!(Key { algorithm: Id::EC, bits: Some(224) }.check().is_ok());
    let missing = Key { algorithm: Id::DSA, bits: None };
    assert!(missing.check().is_ok());
    assert!(missing.inherit(Some(Key { algorithm: Id::RSA, bits: Some(1024) })).is_err());
    assert!(missing.inherit(Some(missing)).is_err());
    assert_eq!(missing.inherit(Some(Key { algorithm: Id::DSA, bits: Some(2048) })).unwrap().bits, Some(2048));
    assert!(missing.inherit(None).unwrap().bits.is_none());
    for (curve, valid) in [(Nid::X9_62_PRIME192V1, false), (Nid::SECP224R1, true)] {
        let group = EcGroup::from_curve_name(curve).unwrap();
        let private = PKey::from_ec_key(EcKey::generate(&group).unwrap()).unwrap();
        let public = PKey::public_key_from_der(&private.public_key_to_der().unwrap()).unwrap();
        assert_eq!(Key::of(&public).check().is_ok(), valid);
    }
}

#[test]
fn key_usage_needs_a_cryptographic_primitive_for_chain_or_key_selection() {
    let mut only_encipher = KeyUsage::new(); only_encipher.encipher_only();
    let invalid = certificate(1024, MessageDigest::sha256(), 1_700_000_000, Some(only_encipher));
    let root = certificate(1024, MessageDigest::sha256(), 1_600_000_000, None);
    assert!(verify_server_chain(&[&invalid, &root]).is_err());
    assert!(verify_client_chain(&[&invalid]).is_err());
    assert!(verify_server_chain(&[&invalid]).is_ok());
    let mut signature = KeyUsage::new(); signature.digital_signature();
    let mut agreement = KeyUsage::new(); agreement.key_agreement().encipher_only();
    for usage in [signature, agreement] {
        let valid = certificate(1024, MessageDigest::sha256(), 1_700_000_000, Some(usage));
        assert!(verify_client_chain(&[&valid]).is_ok());
    }
}

#[test]
fn distrust_cutoffs_keep_all_three_policies_and_symantec_subca_exceptions() {
    let first = |list: &str| fingerprint(list.lines().next().unwrap());
    assert_eq!(distrust_cutoff(first(SYMANTEC), None), Some(1_555_459_200));
    assert_eq!(distrust_cutoff(first(SYMANTEC), Some(first(SYMANTEC_EXEMPT))), Some(1_577_836_800));
    assert_eq!(distrust_cutoff(first(ENTRUST), Some(first(SYMANTEC_EXEMPT))), Some(1_731_369_600));
    assert_eq!(distrust_cutoff(first(CAMERFIRMA), None), Some(1_744_761_600));
    assert_eq!(distrust_cutoff(Fingerprint([0; 32]), None), None);
    for cutoff in [1_555_459_200, 1_577_836_800, 1_731_369_600, 1_744_761_600] {
        let before = certificate(1024, MessageDigest::sha256(), cutoff - 1, None);
        let at = certificate(1024, MessageDigest::sha256(), cutoff, None);
        assert!(issued_before(&before, cutoff).is_ok());
        assert!(issued_before(&at, cutoff).is_err());
    }
}

#[test]
fn real_entrust_anchor_uses_leaf_issuance_date_even_from_an_explicit_store() {
    let memory = MemoryBudget::new(32 << 20);
    let charge = memory.reserve(crate::tls::trust::allocation_bound().unwrap()).unwrap();
    let store = crate::tls::trust::decode(charge).unwrap();
    let root = store.trusted_certificates().iter().map(|bytes| X509::from_der(bytes).unwrap())
        .find(|certificate| Fingerprint::of(certificate).unwrap().listed(ENTRUST)).unwrap();
    let before = certificate(1024, MessageDigest::sha256(), 1_731_369_599, None);
    let after = certificate(1024, MessageDigest::sha256(), 1_731_369_600, None);
    assert!(verify_server_chain(&[&before, &root]).is_ok());
    assert!(verify_server_chain(&[&after, &root]).is_err());
    assert!(verify_client_chain(&[&after, &root]).is_ok());
}

fn der(tag: u8, content: &[u8]) -> Vec<u8> { assert!(content.len() < 128); [vec![tag, content.len() as u8], content.to_vec()].concat() }
fn hash_algorithm(oid: &[u8]) -> Vec<u8> { der(0x30, &[der(6, oid), vec![5, 0]].concat()) }

#[test]
fn pss_parameters_check_both_digests_and_default_sha1_independently() {
    let md5 = hash_algorithm(&[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 2, 5]);
    let sha256 = hash_algorithm(&[0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, 1]);
    assert_eq!(parameters::pss(&[0x30, 0]).unwrap(), (Nid::SHA1, Nid::SHA1));
    let mgf = der(0x30, &[der(6, &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 8]), md5].concat());
    let encoded = der(0x30, &[der(0xa0, &sha256), der(0xa1, &mgf)].concat());
    let (hash, mask) = parameters::pss(&encoded).unwrap();
    assert!(digest(hash, true).is_ok()); assert!(digest(mask, true).is_err());
    let hash_only = der(0x30, &der(0xa0, &sha256));
    let (hash, mask) = parameters::pss(&hash_only).unwrap();
    assert!(digest(hash, true).is_ok()); assert!(digest(mask, true).is_err());
    assert!(digest(mask, false).is_ok());
    for malformed in [vec![], vec![0x30, 0x80, 0, 0], vec![0x30, 0x81, 0], der(0x30, &[der(0xa0, &sha256), der(0xa0, &sha256)].concat())] {
        assert!(parameters::pss(&malformed).is_err());
    }
}
