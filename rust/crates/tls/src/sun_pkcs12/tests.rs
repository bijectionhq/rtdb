use super::*;
use openssl::{asn1::{Asn1Object, Asn1OctetString, Asn1Time}, bn::BigNum, hash::MessageDigest, pkey::{PKey, Private}, symm::Cipher, x509::{X509, X509Extension, X509NameBuilder}};
use crate::{StoreFormat, StoreUse, java_der::{Der, encoded}, secret_der::next};

const DATA: &[u8] = b"\x2a\x86\x48\x86\xf7\x0d\x01\x07\x01";
const PBES2: &[u8] = b"\x2a\x86\x48\x86\xf7\x0d\x01\x05\x0d";
fn seq(parts: &[Vec<u8>]) -> Vec<u8> { encoded(0x30, &parts.concat()) }
fn fixture() -> (PKey<Private>, X509) {
    (PKey::private_key_from_pem(include_bytes!("../../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.key")).unwrap(),
        X509::from_pem(include_bytes!("../../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.crt")).unwrap())
}
fn attributes(id: Option<u8>, alias: Option<&str>, trusted: bool) -> Vec<u8> {
    let mut values = Vec::new();
    if let Some(id) = id { values.push(seq(&[encoded(6, b"\x2a\x86\x48\x86\xf7\x0d\x01\x09\x15"), encoded(0x31, &encoded(4, &[id]))])); }
    if let Some(alias) = alias { values.push(seq(&[encoded(6, b"\x2a\x86\x48\x86\xf7\x0d\x01\x09\x14"), encoded(0x31, &encoded(0x1e, &alias.encode_utf16().flat_map(u16::to_be_bytes).collect::<Vec<_>>()))])); }
    if trusted { values.push(seq(&[encoded(6, b"\x60\x86\x48\x01\x86\xf9\x66\xad\xca\x7b\x01\x01"), encoded(0x31, &[])])); }
    encoded(0x31, &values.concat())
}
fn bag(kind: u8, value: &[u8], id: Option<u8>, alias: Option<&str>, trusted: bool) -> Vec<u8> {
    let mut oid = b"\x2a\x86\x48\x86\xf7\x0d\x01\x0c\x0a\x01".to_vec(); oid.push(kind);
    seq(&[encoded(6, &oid), encoded(0xa0, value), attributes(id, alias, trusted)])
}
fn key_bag(protected: &[u8], id: Option<u8>, alias: Option<&str>) -> Vec<u8> { bag(2, protected, id, alias, false) }
fn certificate_bag(certificate: &X509, id: Option<u8>, alias: Option<&str>, trusted: bool) -> Vec<u8> {
    bag(3, &seq(&[encoded(6, b"\x2a\x86\x48\x86\xf7\x0d\x01\x09\x16\x01"), encoded(0xa0, &encoded(4, &certificate.to_der().unwrap()))]), id, alias, trusted)
}
pub(super) fn secret_bag(protected: &[u8], alias: &str) -> Vec<u8> {
    bag(5, &seq(&[encoded(6, PBES2), encoded(0xa0, &encoded(4, protected))]), None, Some(alias), false)
}
fn section(bytes: &[u8]) -> Vec<u8> { seq(&[encoded(6, DATA), encoded(0xa0, &encoded(4, bytes))]) }
pub(super) fn pfx(bags: &[Vec<u8>]) -> Vec<u8> { seq(&[encoded(2, &[3]), section(&seq(&[section(&seq(bags))]))]) }
pub(super) fn load(bytes: &[u8], password: Option<&str>, memory: &Arc<MemoryBudget>) -> Result<SunX509Pkcs12Store> {
    SunX509Pkcs12Store::decode(bytes, None, password, memory.reserve(SunX509Pkcs12Store::allocation_bound(bytes.len(), None, password)?)?)
}
fn protect(plaintext: &[u8], password: &str) -> Vec<u8> {
    let salt = b"eightbyt"; let iv = [0x43; 16]; let mut key = [0; 16];
    openssl::pkcs5::pbkdf2_hmac(password.as_bytes(), salt, 2, MessageDigest::sha256(), &mut key).unwrap();
    let parameters = seq(&[seq(&[encoded(6, b"\x2a\x86\x48\x86\xf7\x0d\x01\x05\x0c"), seq(&[encoded(4, salt), encoded(2, &[2]), seq(&[encoded(6, b"\x2a\x86\x48\x86\xf7\x0d\x02\x09"), encoded(5, &[])])])]), seq(&[encoded(6, b"\x60\x86\x48\x01\x65\x03\x04\x01\x02"), encoded(4, &iv)])]);
    seq(&[seq(&[encoded(6, PBES2), parameters]), encoded(4, &openssl::symm::encrypt(Cipher::aes_128_cbc(), &key, Some(&iv), plaintext).unwrap())])
}
fn secret(version: &[u8], oid: &[u8], key: &[u8]) -> Vec<u8> { seq(&[encoded(2, version), seq(&[encoded(6, oid)]), encoded(4, key)]) }
fn certificate(key: &PKey<Private>, subject: &str, issuer: &str, serial: u32, extensions: Vec<X509Extension>) -> X509 {
    let name = |value: &str| { let mut name = X509NameBuilder::new().unwrap(); name.append_entry_by_text("CN", value).unwrap(); name.build() };
    let mut certificate = X509::builder().unwrap(); certificate.set_version(2).unwrap();
    certificate.set_serial_number(&BigNum::from_u32(serial).unwrap().to_asn1_integer().unwrap()).unwrap();
    certificate.set_subject_name(&name(subject)).unwrap(); certificate.set_issuer_name(&name(issuer)).unwrap(); certificate.set_pubkey(key).unwrap();
    certificate.set_not_before(&Asn1Time::from_unix(1_600_000_000).unwrap()).unwrap(); certificate.set_not_after(&Asn1Time::from_unix(1_800_000_000).unwrap()).unwrap();
    for extension in extensions { certificate.append_extension(extension).unwrap(); }
    certificate.sign(key, MessageDigest::sha256()).unwrap(); certificate.build()
}
fn extension(oid: &str, bytes: &[u8]) -> X509Extension { X509Extension::new_from_der(&Asn1Object::from_str(oid).unwrap(), false, &Asn1OctetString::new_from_bytes(bytes).unwrap()).unwrap() }

#[test]
fn sun_pkcs12_recovers_unchained_private_keys_before_filtering_and_rejects_unknown_factories() {
    let (key, _) = fixture(); let memory = MemoryBudget::new(32 << 20);
    let bytes = pfx(&[key_bag(&protect(&key.private_key_to_pkcs8().unwrap(), "key"), Some(1), Some("key"))]);
    assert!(load(&bytes, Some("key"), &memory).unwrap().identities().is_empty());
    assert!(load(&bytes, Some("wrong"), &memory).is_err()); assert!(load(&bytes, None, &memory).is_err());
    let generic = KeyStore::decode(&bytes, StoreFormat::Java, StoreUse::OptionalIdentity, None, Some("wrong"), memory.reserve(KeyStore::allocation_bound_with_passwords(bytes.len(), None, Some("wrong")).unwrap()).unwrap()).unwrap();
    assert!(generic.identities().is_empty()); drop(generic);
    let unknown = secret(&[0], &[0x2a, 3, 4, 100], &[1, 2, 3]);
    assert!(load(&pfx(&[key_bag(&protect(&unknown, "key"), Some(1), Some("unknown"))]), Some("key"), &memory).is_err());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sun_pkcs12_eager_secret_recovery_validates_before_private_key_filtering() {
    let memory = MemoryBudget::new(32 << 20);
    for version in [vec![0], vec![1], vec![0xff], vec![0x7f, 0xff, 0xff, 0xff]] {
        let bytes = pfx(&[secret_bag(&protect(&secret(&version, &[0x2a, 3, 4, 100], &[7]), "key"), "secret")]);
        assert!(load(&bytes, Some("key"), &memory).unwrap().identities().is_empty());
        assert!(load(&bytes, Some("wrong"), &memory).is_err()); assert!(load(&bytes, None, &memory).is_err());
    }
    for malformed in [secret(&[0], &[0x2a, 3], &[]), secret(&[0], b"\x2a\x86\x48\x86\xf7\x0d\x01\x05\x0d", &[7]),
        seq(&[encoded(2, &[0]), seq(&[encoded(6, &[0x2a, 3])]), encoded(0x24, &encoded(4, &[7]))])] {
        assert!(load(&pfx(&[secret_bag(&protect(&malformed, "key"), "secret")]), Some("key"), &memory).is_err());
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sun_pkcs12_ignores_crl_bags_without_recovering_or_replacing_aliases() {
    let (key, cert) = fixture(); let protected = protect(&key.private_key_to_pkcs8().unwrap(), "key");
    let bytes = pfx(&[key_bag(&protected, Some(1), Some("client")), bag(4, &encoded(5, &[]), None, Some("CLIENT"), false), certificate_bag(&cert, Some(1), None, false)]);
    let memory = MemoryBudget::new(32 << 20); let loaded = load(&bytes, Some("key"), &memory).unwrap();
    assert_eq!(loaded.identities().len(), 1); assert_eq!(loaded.identities()[0].certificates()[0], cert.to_der().unwrap());
    drop(loaded); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sun_pkcs12_alias_replacements_keep_first_position_and_recover_only_final_bindings() {
    let (key, cert) = fixture(); let memory = MemoryBudget::new(32 << 20);
    let protected = protect(&key.private_key_to_pkcs8().unwrap(), "key");
    let bytes = pfx(&[key_bag(&encoded(5, &[]), Some(1), Some("İ")), key_bag(&protected, Some(2), Some("second")),
        key_bag(&protected, Some(3), Some("i\u{307}")), certificate_bag(&cert, Some(2), None, false), certificate_bag(&cert, Some(3), None, false)]);
    let loaded = load(&bytes, Some("key"), &memory).unwrap();
    assert_eq!(loaded.identities().iter().map(|identity| String::from_utf16(identity.alias()).unwrap()).collect::<Vec<_>>(), ["i\u{307}", "second"]); drop(loaded);
    let secret = secret_bag(&protect(&secret(&[0], &[0x2a, 3], &[7]), "key"), "SHARED");
    for entries in [vec![key_bag(&encoded(5, &[]), Some(1), Some("shared")), secret],
        vec![secret_bag(b"invalid protected secret", "shared"), certificate_bag(&cert, None, Some("SHARED"), true)]] {
        assert!(load(&pfx(&entries), Some("key"), &memory).unwrap().identities().is_empty());
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sun_pkcs12_java_friendly_name_matching_and_generated_names_survive_missing_ids() {
    let (key, cert) = fixture(); let memory = MemoryBudget::new(32 << 20); let protected = protect(&key.private_key_to_pkcs8().unwrap(), "key");
    let bytes = pfx(&[key_bag(&protected, Some(1), Some("İ")), certificate_bag(&cert, Some(2), Some("i"), false)]);
    let loaded = load(&bytes, Some("key"), &memory).unwrap(); assert_eq!(loaded.identities().len(), 1); drop(loaded);
    let bytes = pfx(&[key_bag(&protected, None, None), key_bag(b"not recovered", None, None), certificate_bag(&cert, Some(1), Some("1"), false)]);
    let loaded = load(&bytes, Some("key"), &memory).unwrap(); assert_eq!(loaded.identities()[0].alias(), &[b'1' as u16]); drop(loaded);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sun_pkcs12_missing_id_certificate_fallback_uses_index_zero_of_each_safe_contents() {
    let (key, cert) = fixture(); let protected = protect(&key.private_key_to_pkcs8().unwrap(), "key"); let memory = MemoryBudget::new(32 << 20);
    let keys = section(&seq(&[key_bag(&protected, None, None)]));
    let certificate = certificate_bag(&cert, None, None, false);
    for (certificate_bags, expected) in [(vec![certificate.clone()], 1), (vec![bag(4, &encoded(5, &[]), None, None, false), certificate], 0)] {
        let bytes = seq(&[encoded(2, &[3]), section(&seq(&[keys.clone(), section(&seq(&certificate_bags))]))]);
        let loaded = load(&bytes, Some("key"), &memory).unwrap(); assert_eq!(loaded.identities().len(), expected);
        if expected == 1 { assert_eq!(loaded.identities()[0].certificates()[0], cert.to_der().unwrap()); }
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sun_pkcs12_builds_overwritten_private_chains_before_filtering_final_trusted_bindings() {
    let (_, cert) = fixture(); let bytes = pfx(&[key_bag(b"not recovered", Some(1), Some("same")), certificate_bag(&cert, Some(1), None, false), certificate_bag(&cert, None, Some("SAME"), true)]);
    let memory = MemoryBudget::new(32 << 20);
    let charge = memory.reserve(SunX509Pkcs12Store::allocation_bound(bytes.len(), None, None).unwrap() + 192 * bytes.len()).unwrap();
    let loaded = loader::load(&bytes, None, &memory).unwrap();
    let remaining = memory.reserve((32 << 20) - memory.used_bytes()).unwrap();
    assert_eq!(loaded.recover(None, &memory).err().unwrap().code, "rtdb/memory-budget-exceeded");
    drop(remaining); drop(charge);
    assert!(load(&bytes, None, &memory).unwrap().identities().is_empty()); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sun_pkcs12_openssl_stores_load_encrypted_certificates_and_verify_mac_with_unicode_passwords() {
    let (key, cert) = fixture(); let memory = MemoryBudget::new(32 << 20);
    for password in ["store", "é", ""] {
        let bytes = openssl::pkcs12::Pkcs12::builder().name("CLIENT").pkey(&key).cert(&cert).build2(password).unwrap().to_der().unwrap();
        let charge = memory.reserve(SunX509Pkcs12Store::allocation_bound(bytes.len(), Some(password), Some(password)).unwrap()).unwrap();
        let loaded = SunX509Pkcs12Store::decode(&bytes, Some(password), Some(password), charge).unwrap();
        assert_eq!(loaded.identities().len(), 1); assert_eq!(loaded.identities()[0].alias(), "client".encode_utf16().collect::<Vec<_>>());
        assert_eq!(loaded.identities()[0].certificates()[0], cert.to_der().unwrap()); drop(loaded);
        let charge = memory.reserve(SunX509Pkcs12Store::allocation_bound(bytes.len(), Some("wrong"), Some(password)).unwrap()).unwrap();
        assert!(SunX509Pkcs12Store::decode(&bytes, Some("wrong"), Some(password), charge).is_err());
        assert!(load(&bytes, Some(password), &memory).unwrap().identities().is_empty());
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sun_pkcs12_metadata_accepts_mismatched_key_values_and_generic_identity_remains_strict() {
    let (key, cert) = fixture(); let other = PKey::from_rsa(openssl::rsa::Rsa::generate(1024).unwrap()).unwrap();
    assert!(!other.public_eq(&key));
    let bytes = pfx(&[key_bag(&protect(&other.private_key_to_pkcs8().unwrap(), "key"), Some(1), Some("key")), certificate_bag(&cert, Some(1), None, false)]);
    let memory = MemoryBudget::new(32 << 20); let loaded = load(&bytes, Some("key"), &memory).unwrap();
    assert!(PKey::private_key_from_pkcs8(loaded.identities()[0].private_key_pkcs8()).unwrap().public_eq(&other));
    assert_eq!(loaded.identities()[0].certificates()[0], cert.to_der().unwrap()); drop(loaded);
    assert!(KeyStore::decode(&bytes, StoreFormat::Java, StoreUse::Identity, None, Some("key"), memory.reserve(KeyStore::allocation_bound_with_passwords(bytes.len(), None, Some("key")).unwrap()).unwrap()).is_err());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sun_pkcs12_chain_requires_issuer_name_and_uses_exact_skid_then_last_absent_skid() {
    let (key, _) = fixture(); let protected = protect(&key.private_key_to_pkcs8().unwrap(), "key"); let memory = MemoryBudget::new(32 << 20);
    let leaf = certificate(&key, "leaf", "issuer", 1, vec![extension("2.5.29.35", &[0x30, 3, 0x80, 1, 9])]);
    let wrong_name = certificate(&key, "other", "other", 2, vec![extension("2.5.29.14", &[4, 1, 9])]);
    let wrong_id = certificate(&key, "issuer", "issuer", 3, vec![extension("2.5.29.14", &[4, 0x81, 1, 9])]);
    let first = certificate(&key, "issuer", "issuer", 4, vec![]); let last = certificate(&key, "issuer", "issuer", 5, vec![]);
    let exact = certificate(&key, "issuer", "issuer", 6, vec![extension("2.5.29.14", &[4, 1, 9])]);
    for include_exact in [false, true] {
        let mut bags = vec![key_bag(&protected, Some(1), Some("key")), certificate_bag(&leaf, Some(1), None, false)];
        for (id, cert) in [(2, &wrong_name), (3, &wrong_id), (4, &first), (5, &last)] { bags.push(certificate_bag(cert, Some(id), None, false)); }
        if include_exact { bags.push(certificate_bag(&exact, Some(6), None, false)); }
        let loaded = load(&pfx(&bags), Some("key"), &memory).unwrap();
        assert_eq!(loaded.identities()[0].certificates(), [leaf.to_der().unwrap(), if include_exact { exact.to_der().unwrap() } else { last.to_der().unwrap() }]);
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sun_pkcs12_self_issued_leaf_requires_a_valid_self_signature_to_stop_chain_building() {
    let (key, _) = fixture(); let other = PKey::from_rsa(openssl::rsa::Rsa::generate(1024).unwrap()).unwrap();
    let mut leaf = certificate(&key, "issuer", "issuer", 1, vec![]).to_der().unwrap(); let last = leaf.len() - 1; leaf[last] ^= 1;
    let leaf = X509::from_der(&leaf).unwrap(); assert!(!leaf.verify(&key).unwrap());
    let issuer = certificate(&other, "issuer", "issuer", 2, vec![]);
    let protected = protect(&key.private_key_to_pkcs8().unwrap(), "key");
    let bytes = pfx(&[key_bag(&protected, Some(1), Some("key")), certificate_bag(&issuer, Some(2), None, false), certificate_bag(&leaf, Some(1), None, false)]);
    let memory = MemoryBudget::new(32 << 20); let loaded = load(&bytes, Some("key"), &memory).unwrap();
    assert_eq!(loaded.identities()[0].certificates(), [leaf.to_der().unwrap(), issuer.to_der().unwrap()]);
}

#[test]
fn sun_pkcs12_pfx_stream_tail_set_outer_and_content_wrapper_follow_java_framing() {
    let (key, cert) = fixture(); let protected = protect(&key.private_key_to_pkcs8().unwrap(), "key");
    let mut input = Der(&protected); let outer = next(&mut input).unwrap(); let mut fields = Der(&outer.content);
    let algorithm = next(&mut fields).unwrap(); let ciphertext = next(&mut fields).unwrap();
    let protected = encoded(0x31, &[encoded(algorithm.tag, &algorithm.content), encoded(0x24, &encoded(4, &ciphertext.content))].concat());
    let contents = seq(&[key_bag(&protected, Some(1), Some("key")), certificate_bag(&cert, Some(1), None, false)]);
    let section = seq(&[encoded(6, DATA), encoded(4, &encoded(0x24, &encoded(4, &contents)))]);
    let mut bytes = encoded(0x31, &[encoded(2, &[3]), seq(&[encoded(6, DATA), encoded(0xa3, &encoded(4, &seq(&[section])))])].concat()); bytes.extend_from_slice(b"ignored stream tail");
    let memory = MemoryBudget::new(32 << 20); assert_eq!(load(&bytes, Some("key"), &memory).unwrap().identities().len(), 1);
}

#[test]
fn sun_pkcs12_section_errors_precede_mac_and_missing_store_password_skips_encrypted_sections() {
    let (key, _) = fixture(); let protected = protect(&key.private_key_to_pkcs8().unwrap(), "key");
    let encrypted = seq(&[encoded(6, b"\x2a\x86\x48\x86\xf7\x0d\x01\x07\x06"), encoded(0xa0, &encoded(5, &[]))]);
    let authenticated = seq(&[section(&seq(&[key_bag(&protected, Some(1), Some("key"))])), encrypted]);
    let bytes = seq(&[encoded(2, &[3]), section(&authenticated), encoded(5, b"invalid MAC")]);
    let memory = MemoryBudget::new(32 << 20); assert!(load(&bytes, Some("key"), &memory).unwrap().identities().is_empty());
    let charge = memory.reserve(SunX509Pkcs12Store::allocation_bound(bytes.len(), Some("store"), Some("key")).unwrap()).unwrap();
    let error = SunX509Pkcs12Store::decode(&bytes, Some("store"), Some("key"), charge).err().unwrap();
    assert_eq!(error.message.as_deref(), Some("Malformed SunX509 PKCS12 material")); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sun_pkcs12_initial_transient_and_retained_material_have_separate_admission() {
    let (key, cert) = fixture(); let protected = protect(&key.private_key_to_pkcs8().unwrap(), "key");
    let bytes = pfx(&[key_bag(&protected, Some(1), Some("key")), certificate_bag(&cert, Some(1), None, false)]);
    let bound = SunX509Pkcs12Store::allocation_bound(bytes.len(), None, Some("key")).unwrap();
    let tight = MemoryBudget::new(bound); let charge = tight.reserve(bound).unwrap();
    assert_eq!(SunX509Pkcs12Store::decode(&bytes, None, Some("key"), charge).err().unwrap().code, "rtdb/memory-budget-exceeded"); assert_eq!(tight.used_bytes(), 0);
    let memory = MemoryBudget::new(32 << 20); assert!(SunX509Pkcs12Store::decode(&bytes, None, Some("key"), memory.reserve(0).unwrap()).is_err());
    let loaded = load(&bytes, Some("key"), &memory).unwrap(); assert!(memory.used_bytes() > bound);
    assert!(memory.used_bytes() < bound + 192 * bytes.len()); drop(loaded); assert_eq!(memory.used_bytes(), 0);
}

mod ordering;
