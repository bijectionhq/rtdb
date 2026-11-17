use super::*;
use crate::ProtectedKeyStore;
use openssl::{asn1::{Asn1Object, Asn1OctetString, Asn1Time}, bn::BigNum, hash::MessageDigest, pkey::Private, x509::{X509Extension, X509NameBuilder}};

fn load_protected(bytes: &[u8], password: Option<&str>, memory: &Arc<MemoryBudget>) -> Result<ProtectedKeyStore> {
    ProtectedKeyStore::decode(bytes, password, memory.reserve(ProtectedKeyStore::allocation_bound(bytes.len(), password)?)?)
}
fn pfx(bags: &[Vec<u8>]) -> Vec<u8> {
    let authenticated = sequence(&[sequence(&[der(6, DATA), der(0xa0, &der(4, &sequence(bags)))])]);
    sequence(&[der(2, &[3]), sequence(&[der(6, DATA), der(0xa0, &der(4, &authenticated))])])
}
fn certificate_bag(certificate: &X509, id: u8, alias: Option<&str>) -> Vec<u8> {
    let value = sequence(&[der(6, b"\x2a\x86\x48\x86\xf7\x0d\x01\x09\x16\x01"), der(0xa0, &der(4, &certificate.to_der().unwrap()))]);
    safe_bag(3, value, id, alias)
}
fn fixture() -> (PKey<Private>, X509) {
    (PKey::private_key_from_pem(include_bytes!("../../../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.key")).unwrap(),
        X509::from_pem(include_bytes!("../../../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.crt")).unwrap())
}
fn certificate(key: &PKey<Private>, subject: &str, issuer: &str, serial: u32, extensions: Vec<X509Extension>) -> X509 {
    let name = |value: &str| { let mut name = X509NameBuilder::new().unwrap(); name.append_entry_by_text("CN", value).unwrap(); name.build() };
    let mut certificate = X509::builder().unwrap(); certificate.set_version(2).unwrap();
    certificate.set_serial_number(&BigNum::from_u32(serial).unwrap().to_asn1_integer().unwrap()).unwrap();
    certificate.set_subject_name(&name(subject)).unwrap(); certificate.set_issuer_name(&name(issuer)).unwrap(); certificate.set_pubkey(key).unwrap();
    certificate.set_not_before(&Asn1Time::from_unix(1_600_000_000).unwrap()).unwrap(); certificate.set_not_after(&Asn1Time::from_unix(1_800_000_000).unwrap()).unwrap();
    for extension in extensions { certificate.append_extension(extension).unwrap(); }
    certificate.sign(key, MessageDigest::sha256()).unwrap(); certificate.build()
}
fn extension(oid: &str, bytes: &[u8]) -> X509Extension {
    X509Extension::new_from_der(&Asn1Object::from_str(oid).unwrap(), false, &Asn1OctetString::new_from_bytes(bytes).unwrap()).unwrap()
}

#[test]
fn protected_pkcs12_defers_bad_keys_but_checks_the_store_mac() {
    let memory = MemoryBudget::new(32 << 20);
    let bytes = store(false);
    assert!(load_protected(&bytes, Some("wrong"), &memory).is_err());
    let loaded = load_protected(&bytes, Some("store-password"), &memory).unwrap();
    assert_eq!(loaded.identities().len(), 2);
    for identity in loaded.identities() {
        assert!(identity.recover(Some("wrong"), &memory).is_err());
        assert!(identity.recover(Some("key-password"), &memory).is_ok());
    }
    let (key, certificate) = fixture();
    let good = key.private_key_to_pkcs8_passphrase(Cipher::aes_256_cbc(), b"key-password").unwrap();
    let bytes = pfx(&[safe_bag(2, der(5, &[]), 1, Some("bad")), safe_bag(2, good, 2, Some("good")), certificate_bag(&certificate, 1, None), certificate_bag(&certificate, 2, None)]);
    let loaded = load_protected(&bytes, None, &memory).unwrap();
    assert_eq!(loaded.identities().len(), 2);
    for identity in loaded.identities() { assert_eq!(identity.recover(Some("key-password"), &memory).is_ok(), identity.alias() == "good".encode_utf16().collect::<Vec<_>>()); }
    assert!(decode(&bytes, None, Some("key-password"), true).is_err());
}

#[test]
fn protected_pkcs12_normalizes_aliases_and_preserves_replacement_position() {
    let (key, certificate) = fixture();
    let encrypted = key.private_key_to_pkcs8_passphrase(Cipher::aes_256_cbc(), b"key-password").unwrap();
    let bytes = pfx(&[
        safe_bag(2, der(5, &[]), 1, Some("İ")), safe_bag(2, encrypted.clone(), 2, Some("second")), safe_bag(2, encrypted, 3, Some("i\u{307}")),
        certificate_bag(&certificate, 1, None), certificate_bag(&certificate, 2, None), certificate_bag(&certificate, 3, None),
    ]);
    let memory = MemoryBudget::new(32 << 20);
    let loaded = load_protected(&bytes, None, &memory).unwrap();
    assert_eq!(loaded.identities().iter().map(|identity| String::from_utf16(identity.alias()).unwrap()).collect::<Vec<_>>(), ["i\u{307}", "second"]);
    for identity in loaded.identities() { assert!(identity.recover(Some("key-password"), &memory).is_ok()); }
}

#[test]
fn protected_pkcs12_friendly_name_matching_uses_java_simple_case() {
    let (key, certificate) = fixture();
    let encrypted = key.private_key_to_pkcs8_passphrase(Cipher::aes_256_cbc(), b"key-password").unwrap();
    let bytes = pfx(&[safe_bag(2, encrypted, 1, Some("İ")), certificate_bag(&certificate, 2, Some("i"))]);
    let memory = MemoryBudget::new(32 << 20);
    let loaded = load_protected(&bytes, None, &memory).unwrap();
    assert_eq!(loaded.identities().len(), 1);
    assert!(loaded.identities()[0].recover(Some("key-password"), &memory).is_ok());
    assert!(decode(&bytes, None, Some("key-password"), true).unwrap().identities.is_empty());
}

#[test]
fn protected_pkcs12_secret_entries_can_replace_a_private_key_alias() {
    let (key, certificate) = fixture();
    let encrypted = key.private_key_to_pkcs8_passphrase(Cipher::aes_256_cbc(), b"key-password").unwrap();
    let secret = safe_bag(4, sequence(&[der(6, PBES2), der(0xa0, &der(4, b"protected secret"))]), 2, Some("SHARED"));
    let bytes = pfx(&[safe_bag(2, encrypted.clone(), 1, Some("shared")), secret.clone(), certificate_bag(&certificate, 1, None)]);
    let memory = MemoryBudget::new(32 << 20);
    assert!(load_protected(&bytes, None, &memory).unwrap().identities().is_empty());
    let bytes = pfx(&[secret, safe_bag(2, encrypted, 1, Some("shared")), certificate_bag(&certificate, 1, None)]);
    assert_eq!(load_protected(&bytes, None, &memory).unwrap().identities().len(), 1);
}

#[test]
fn protected_pkcs12_uses_java_names_and_first_issuer_match_without_aki() {
    let (key, _) = fixture();
    let encrypted = key.private_key_to_pkcs8_passphrase(Cipher::aes_256_cbc(), b"key-password").unwrap();
    let leaf = certificate(&key, "leaf", "ÉCOLE", 1, vec![]);
    let first = certificate(&key, "école", "école", 2, vec![]);
    let second = certificate(&key, "ÉCOLE", "ÉCOLE", 3, vec![]);
    let bytes = pfx(&[safe_bag(2, encrypted, 1, Some("key")), certificate_bag(&leaf, 1, None), certificate_bag(&first, 2, None), certificate_bag(&second, 3, None)]);
    let memory = MemoryBudget::new(32 << 20);
    let loaded = load_protected(&bytes, None, &memory).unwrap();
    assert_eq!(loaded.identities()[0].certificates(), [leaf.to_der().unwrap(), first.to_der().unwrap()]);
    assert_eq!(decode(&bytes, None, Some("key-password"), true).unwrap().identities[0].certificates()[1], second.to_der().unwrap());
}

#[test]
fn protected_pkcs12_compares_encoded_identifier_bytes_and_uses_last_missing_skid_fallback() {
    let (key, _) = fixture();
    let encrypted = key.private_key_to_pkcs8_passphrase(Cipher::aes_256_cbc(), b"key-password").unwrap();
    let leaf = certificate(&key, "leaf", "issuer", 1, vec![extension("2.5.29.35", &[0x30, 3, 0x80, 1, 9])]);
    let first = certificate(&key, "issuer", "issuer", 2, vec![extension("2.5.29.14", &[4, 0x81, 1, 9])]);
    let last = certificate(&key, "issuer", "issuer", 3, vec![]);
    let exact = certificate(&key, "issuer", "issuer", 4, vec![extension("2.5.29.14", &[4, 1, 9])]);
    let missing = certificate(&key, "issuer", "issuer", 5, vec![]);
    let memory = MemoryBudget::new(32 << 20);
    for include_exact in [false, true] {
        let mut bags = vec![safe_bag(2, encrypted.clone(), 1, Some("key")), certificate_bag(&leaf, 1, None), certificate_bag(&first, 2, None), certificate_bag(&missing, 5, None), certificate_bag(&last, 3, None)];
        if include_exact { bags.push(certificate_bag(&exact, 4, None)); }
        let bytes = pfx(&bags);
        let loaded = load_protected(&bytes, None, &memory).unwrap();
        assert_eq!(loaded.identities()[0].certificates()[1], if include_exact { exact.to_der().unwrap() } else { last.to_der().unwrap() });
    }
}

#[test]
fn protected_pkcs12_expanded_chains_keep_independent_reservations() {
    let (key, _) = fixture();
    let encrypted = key.private_key_to_pkcs8_passphrase(Cipher::aes_256_cbc(), b"key-password").unwrap();
    let mut bags = (0..20).map(|index| safe_bag(2, encrypted.clone(), 1, Some(&format!("key{index}")))).collect::<Vec<_>>();
    for index in 0..20 {
        let certificate = certificate(&key, &format!("certificate{index}"), &format!("certificate{}", (index + 1).min(19)), index + 1, vec![]);
        bags.push(certificate_bag(&certificate, if index == 0 { 1 } else { index as u8 + 1 }, None));
    }
    let bytes = pfx(&bags);
    let memory = MemoryBudget::new(64 << 20);
    let initial = ProtectedKeyStore::allocation_bound(bytes.len(), None).unwrap();
    let loaded = load_protected(&bytes, None, &memory).unwrap();
    assert_eq!(loaded.identities().len(), 20);
    for identity in loaded.identities() { assert_eq!(identity.certificates().len(), 20); }
    assert!(memory.used_bytes() > initial);
    assert!(decode(&bytes, None, Some("key-password"), true).is_err());
    drop(loaded); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn java_bmp_alias_decoder_matches_malformed_sequence_consumption() {
    assert_eq!(java_bmp_units(&[0xd8, 0, 0xd8, 0, 0xdc, 0]), [0xfffd, 0xfffd]);
    assert_eq!(java_bmp_units(&[0xd8, 0, 0]), [0xfffd]);
    assert_eq!(java_bmp_units(&[0, b'A', 0]), [b'A' as u16, 0xfffd]);
    assert_eq!(java_bmp_units(&[0xd8, 0, 0xdc, 0]), [0xd800, 0xdc00]);
    assert_eq!(java_bmp_units(&[0xfe, 0xff, 0, b'A']), [0xfeff, b'A' as u16]);
}
