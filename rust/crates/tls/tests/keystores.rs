use rtdb_columnar::MemoryBudget;
use rtdb_tls::{KeyStore, ProtectedKeyStore, StoreFormat, StoreUse};
use sha1::{Digest, Sha1};

fn decode(bytes: &[u8], store: Option<&str>, key: Option<&str>, usage: StoreUse) -> rtdb_types::Result<KeyStore> {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    KeyStore::decode(bytes, StoreFormat::Java, usage, store, key, memory.reserve(KeyStore::allocation_bound_with_passwords(bytes.len(),store,key)?)?)
}
#[test]
fn independently_generated_jks_vectors_decrypt_unicode_and_distinct_key_passwords() {
    for (bytes, password, key_password) in [
        (include_bytes!("fixtures/rsa1024.jks").as_slice(), "12345678", None),
        (include_bytes!("fixtures/distinct-passwords.jks").as_slice(), "store_password", Some("private_password")),
        (include_bytes!("fixtures/non-ascii-password.jks").as_slice(), "\u{10da}(\u{0ca0}\u{76ca}\u{0ca0}\u{10da})", None),
    ] {
        let store = decode(bytes, Some(password), key_password, StoreUse::Identity).unwrap();
        assert_eq!(store.identities().len(), 1);
        assert!(!store.identities()[0].certificates().is_empty());
        assert!(!store.identities()[0].private_key_pkcs8().is_empty());
        assert!(decode(bytes, Some("wrong"), key_password, StoreUse::Identity).is_err());
    }
}
#[test]
fn jks_tampering_and_private_key_password_failure_are_detected() {
    let bytes = include_bytes!("fixtures/rsa1024.jks");
    for index in [12, bytes.len() / 2, bytes.len() - 1] {
        let mut damaged = bytes.to_vec(); damaged[index] ^= 1;
        assert!(decode(&damaged, Some("12345678"), None, StoreUse::Identity).is_err());
    }
    let bytes = include_bytes!("fixtures/distinct-passwords.jks");
    assert!(decode(bytes, Some("store_password"), Some("wrong"), StoreUse::Identity).is_err());
    let trust = decode(bytes, Some("store_password"), None, StoreUse::Trust).unwrap();
    assert!(trust.identities().is_empty()); assert!(!trust.trusted_certificates().is_empty());
}
#[test]
fn pem_copies_own_independent_reservations_after_store_release() {
    let bytes = include_bytes!("../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.p12");
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let store = KeyStore::decode(bytes, StoreFormat::Java, StoreUse::Identity, Some("password123"), None, memory.reserve(KeyStore::allocation_bound_with_passwords(bytes.len(),Some("password123"),None).unwrap()).unwrap()).unwrap();
    let original = memory.used_bytes();
    let pem = store.identities()[0].to_pem(&memory).unwrap();
    let first = memory.used_bytes() - original;
    let second = store.identities()[0].to_pem(&memory).unwrap();
    assert_eq!(memory.used_bytes(), original + 2 * first);
    assert_eq!(pem.bytes(), second.bytes());
    drop(store); assert_eq!(memory.used_bytes(), 2 * first);
    let pem_store = KeyStore::decode(pem.bytes(), StoreFormat::Pem, StoreUse::Identity, None, None, memory.reserve(KeyStore::allocation_bound(pem.bytes().len()).unwrap()).unwrap()).unwrap();
    assert_eq!(pem_store.identities().len(), 1); drop(pem_store); drop(pem);
    assert_eq!(memory.used_bytes(), first); drop(second); assert_eq!(memory.used_bytes(), 0);
}
#[test]
fn issuer_names_own_independent_reservations_and_fail_before_copying() {
    let bytes = include_bytes!("fixtures/rsa1024.jks");
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let store = KeyStore::decode(bytes, StoreFormat::Java, StoreUse::Identity, Some("12345678"), None, memory.reserve(KeyStore::allocation_bound_with_passwords(bytes.len(),Some("12345678"),None).unwrap()).unwrap()).unwrap();
    let original = memory.used_bytes();
    let first = store.identities()[0].issuer_names(&memory).unwrap();
    let charge = memory.used_bytes() - original;
    let second = store.identities()[0].issuer_names(&memory).unwrap();
    assert_eq!(first.as_slice(), second.as_slice());
    assert!(!first.as_slice().is_empty());
    assert_eq!(memory.used_bytes(), original + 2 * charge);
    let empty = MemoryBudget::new(0);
    assert!(store.identities()[0].issuer_names(&empty).is_err());
    assert!(store.identities()[0].to_pem(&empty).is_err());
    assert_eq!(empty.used_bytes(), 0);
    drop(store); assert_eq!(memory.used_bytes(), 2 * charge);
    drop(first); assert_eq!(memory.used_bytes(), charge);
    drop(second); assert_eq!(memory.used_bytes(), 0);
}
#[test]
fn encrypted_pkcs8_pem_rejects_the_wrong_password() {
    let key = openssl::pkey::PKey::private_key_from_pem(include_bytes!("../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.key")).unwrap();
    let mut pem = key.private_key_to_pem_pkcs8_passphrase(openssl::symm::Cipher::aes_256_cbc(), b"key-password").unwrap();
    pem.extend_from_slice(include_bytes!("../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.crt"));
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let charge = || memory.reserve(KeyStore::allocation_bound_with_passwords(pem.len(),None,Some("key-password")).unwrap()).unwrap();
    assert!(KeyStore::decode(&pem, StoreFormat::Pem, StoreUse::Identity, None, Some("key-password"), charge()).is_ok());
    assert!(KeyStore::decode(&pem, StoreFormat::Pem, StoreUse::Identity, None, Some("wrong"), charge()).is_err());
}

fn take<'a>(bytes: &mut &'a [u8], count: usize) -> &'a [u8] {
    let (value, rest) = bytes.split_at(count); *bytes = rest; value
}
fn long<'a>(bytes: &mut &'a [u8]) -> &'a [u8] {
    let count = u32::from_be_bytes(take(bytes, 4).try_into().unwrap()) as usize;
    take(bytes, count)
}
fn short<'a>(bytes: &mut &'a [u8]) -> &'a [u8] {
    let count = u16::from_be_bytes(take(bytes, 2).try_into().unwrap()) as usize;
    take(bytes, count)
}
fn fixture_key() -> (Vec<u8>, Vec<Vec<u8>>) {
    let mut bytes = include_bytes!("fixtures/rsa1024.jks").as_slice();
    assert_eq!(take(&mut bytes, 8), &[0xfe, 0xed, 0xfe, 0xed, 0, 0, 0, 2]);
    assert_eq!(take(&mut bytes, 4), 1u32.to_be_bytes());
    assert_eq!(take(&mut bytes, 4), 1u32.to_be_bytes());
    short(&mut bytes); take(&mut bytes, 8);
    let protected = long(&mut bytes).to_vec();
    let count = u32::from_be_bytes(take(&mut bytes, 4).try_into().unwrap());
    let certificates = (0..count).map(|_| { short(&mut bytes); long(&mut bytes).to_vec() }).collect();
    assert_eq!(bytes.len(), 20);
    (protected, certificates)
}
fn entry_prefix(tag: u32, alias: &[u8]) -> Vec<u8> {
    let mut bytes = tag.to_be_bytes().to_vec();
    bytes.extend_from_slice(&u16::try_from(alias.len()).unwrap().to_be_bytes());
    bytes.extend_from_slice(alias); bytes.extend_from_slice(&0i64.to_be_bytes()); bytes
}
fn append_certificate(bytes: &mut Vec<u8>, certificate: &[u8]) {
    bytes.extend_from_slice(&5u16.to_be_bytes()); bytes.extend_from_slice(b"X.509");
    bytes.extend_from_slice(&u32::try_from(certificate.len()).unwrap().to_be_bytes()); bytes.extend_from_slice(certificate);
}
fn key_entry(alias: &[u8], protected: &[u8], certificates: &[Vec<u8>]) -> Vec<u8> {
    let mut bytes = entry_prefix(1, alias);
    bytes.extend_from_slice(&u32::try_from(protected.len()).unwrap().to_be_bytes()); bytes.extend_from_slice(protected);
    bytes.extend_from_slice(&u32::try_from(certificates.len()).unwrap().to_be_bytes());
    for certificate in certificates { append_certificate(&mut bytes, certificate); }
    bytes
}
fn certificate_entry(alias: &[u8], certificate: &[u8]) -> Vec<u8> {
    let mut bytes = entry_prefix(2, alias); append_certificate(&mut bytes, certificate); bytes
}
fn jks_store(entries: &[Vec<u8>]) -> Vec<u8> {
    let mut bytes = 0xfeedfeedu32.to_be_bytes().to_vec(); bytes.extend_from_slice(&2u32.to_be_bytes());
    bytes.extend_from_slice(&u32::try_from(entries.len()).unwrap().to_be_bytes());
    for entry in entries { bytes.extend_from_slice(entry); }
    let mut hash = Sha1::new(); for unit in "12345678".encode_utf16() { hash.update(unit.to_be_bytes()); }
    hash.update(b"Mighty Aphrodite"); hash.update(&bytes); bytes.extend_from_slice(&hash.finalize()); bytes
}
#[test]
fn empty_java_stores_keep_client_and_server_identity_admission_separate() {
    let bytes = jks_store(&[]);
    for usage in [StoreUse::Trust, StoreUse::OptionalIdentity] {
        let store = decode(&bytes, Some("12345678"), None, usage).unwrap();
        assert!(store.identities().is_empty());
        assert!(store.trusted_certificates().is_empty());
    }
    assert!(decode(&bytes, Some("12345678"), None, StoreUse::Identity).is_err());
    let (_, chain) = fixture_key();
    let bytes = jks_store(&[certificate_entry(b"trusted", &chain[0])]);
    let store = decode(&bytes, Some("12345678"), None, StoreUse::OptionalIdentity).unwrap();
    assert!(store.identities().is_empty());
    assert_eq!(store.trusted_certificates().len(), 1);
    assert!(decode(&bytes, Some("12345678"), None, StoreUse::Identity).is_err());
}
#[test]
fn jks_duplicate_aliases_discard_overwritten_encrypted_keys_before_decryption() {
    let (key, chain) = fixture_key();
    for (first, second) in [(b"a".as_slice(), b"a".as_slice()), (b"\xc1\xa1", b"a"), (b"\x00", b"\xc0\x80"), (b"\xed\xa0\x80", b"\xed\xa0\x80")] {
        let bytes = jks_store(&[key_entry(first, b"invalid overwritten key", &chain), key_entry(second, &key, &chain)]);
        let store = decode(&bytes, Some("12345678"), None, StoreUse::Identity).unwrap();
        assert_eq!(store.identities().len(), 1); assert_eq!(store.trusted_certificates().len(), 1);
    }
}
#[test]
fn jks_last_alias_entry_replaces_across_key_and_certificate_types() {
    let (key, chain) = fixture_key();
    let other = openssl::x509::X509::from_pem(include_bytes!("../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.crt")).unwrap().to_der().unwrap();
    let bytes = jks_store(&[key_entry(b"same", b"invalid overwritten key", &chain), certificate_entry(b"same", &other)]);
    let trust = decode(&bytes, Some("12345678"), None, StoreUse::Trust).unwrap();
    assert_eq!(trust.trusted_certificates(), &[other.clone()]);
    let bytes = jks_store(&[certificate_entry(b"same", &other), key_entry(b"same", &key, &chain)]);
    let store = decode(&bytes, Some("12345678"), None, StoreUse::Identity).unwrap();
    assert_eq!(store.identities().len(), 1); assert_eq!(store.trusted_certificates(), &chain[..1]);
}
#[test]
fn jks_load_compares_stored_alias_case_exactly_and_validates_overwritten_certificates() {
    let (_, chain) = fixture_key();
    let bytes = jks_store(&[certificate_entry(b"Name", &chain[0]), certificate_entry(b"name", &chain[0])]);
    assert_eq!(decode(&bytes, Some("12345678"), None, StoreUse::Trust).unwrap().trusted_certificates().len(), 2);
    let bytes = jks_store(&[certificate_entry(b"same", b"invalid certificate"), certificate_entry(b"same", &chain[0])]);
    assert!(decode(&bytes, Some("12345678"), None, StoreUse::Trust).is_err());
    for alias in [b"\xc0".as_slice(), b"\x80", b"\xe0\x80", b"\xc0\x00", b"\xf0\x90\x80\x80"] {
        assert!(decode(&jks_store(&[certificate_entry(alias, &chain[0])]), Some("12345678"), None, StoreUse::Trust).is_err());
    }
}

#[test]
fn jks_identity_aliases_retain_hashtable_enumeration_before_and_after_resize() {
    let (key, chain) = fixture_key();
    for (names, expected) in [
        (vec!["a", "b", "c", "d", "e", "f", "g", "h"], vec!["b", "a", "h", "g", "f", "e", "d", "c"]),
        (vec!["a", "b", "c", "d", "e", "f", "g", "h", "i"], vec!["i", "h", "g", "f", "e", "d", "c", "b", "a"]),
        (vec!["a", "l", "a"], vec!["l", "a"]),
        (vec!["a", "x", "b", "c", "d", "e", "f", "g", "h"], vec!["h", "g", "f", "e", "d", "c", "b", "a", "x"]),
    ] {
        let entries = names.iter().map(|name| key_entry(name.as_bytes(), &key, &chain)).collect::<Vec<_>>();
        let store = decode(&jks_store(&entries), Some("12345678"), None, StoreUse::Identity).unwrap();
        let aliases = store.identities().iter().map(|identity| String::from_utf16(identity.alias().unwrap()).unwrap()).collect::<Vec<_>>();
        assert_eq!(aliases, expected);
    }
}

#[test]
fn jks_identity_aliases_keep_isolated_utf16_code_units() {
    let (key, chain) = fixture_key();
    let store = decode(&jks_store(&[key_entry(b"\xed\xa0\x80", &key, &chain)]), Some("12345678"), None, StoreUse::Identity).unwrap();
    assert_eq!(store.identities()[0].alias(), Some([0xd800u16].as_slice()));
}

#[test]
fn pem_identity_has_no_java_store_alias() {
    let mut pem = include_bytes!("../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.key").to_vec();
    pem.extend_from_slice(include_bytes!("../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.crt"));
    let memory = MemoryBudget::new(1 << 20);
    let charge = memory.reserve(KeyStore::allocation_bound(pem.len()).unwrap()).unwrap();
    let store = KeyStore::decode(&pem, StoreFormat::Pem, StoreUse::Identity, None, None, charge).unwrap();
    assert_eq!(store.identities()[0].alias(), None);
}

fn protected(bytes: &[u8], password: Option<&str>, memory: &std::sync::Arc<MemoryBudget>) -> rtdb_types::Result<ProtectedKeyStore> {
    ProtectedKeyStore::decode(bytes, password, memory.reserve(ProtectedKeyStore::allocation_bound(bytes.len(), password)?)?)
}

#[test]
fn protected_jks_loads_metadata_before_recovering_only_the_selected_key() {
    let (key, chain) = fixture_key();
    let bytes = jks_store(&[key_entry(b"bad", b"invalid protected key", &chain), key_entry(b"good", &key, &chain)]);
    let memory = MemoryBudget::new(16 << 20);
    let store = protected(&bytes, Some("12345678"), &memory).unwrap();
    assert_eq!(store.identities().len(), 2);
    for identity in store.identities() {
        assert_eq!(identity.certificates(), chain);
        let good = identity.alias() == b"good".iter().map(|byte| u16::from(*byte)).collect::<Vec<_>>();
        assert_eq!(identity.recover(Some("12345678"), &memory).is_ok(), good);
        assert!(identity.recover(Some("wrong"), &memory).is_err());
        assert!(identity.recover(None, &memory).is_err());
    }
    assert!(decode(&bytes, Some("12345678"), None, StoreUse::Identity).is_err());
}

#[test]
fn protected_jks_resolves_raw_enumeration_aliases_through_java_lowercase_lookup() {
    let (key, chain) = fixture_key();
    let memory = MemoryBudget::new(16 << 20);
    let upper_only = jks_store(&[key_entry(b"UPPER", &key, &chain)]);
    assert!(protected(&upper_only, Some("12345678"), &memory).unwrap().identities().is_empty());
    assert_eq!(decode(&upper_only, Some("12345678"), None, StoreUse::Identity).unwrap().identities().len(), 1);
    for upper in [key_entry(b"UPPER", b"unrecoverable upper key", &chain), certificate_entry(b"UPPER", &chain[0])] {
        let bytes = jks_store(&[upper, key_entry(b"upper", &key, &chain)]);
        let store = protected(&bytes, Some("12345678"), &memory).unwrap();
        assert_eq!(store.identities().len(), 2);
        for identity in store.identities() {
            assert!(identity.alias() == "UPPER".encode_utf16().collect::<Vec<_>>() || identity.alias() == "upper".encode_utf16().collect::<Vec<_>>());
            assert!(identity.recover(Some("12345678"), &memory).is_ok());
        }
    }
    let bytes = jks_store(&[key_entry(b"UPPER", &key, &chain), certificate_entry(b"upper", &chain[0])]);
    assert!(protected(&bytes, Some("12345678"), &memory).unwrap().identities().is_empty());
}

#[test]
fn protected_jks_uses_pinned_lowercase_expansion_and_keeps_isolated_surrogates() {
    let (key, chain) = fixture_key();
    let bytes = jks_store(&[
        key_entry("İ".as_bytes(), b"unrecoverable uppercase", &chain),
        key_entry("i\u{307}".as_bytes(), &key, &chain),
        key_entry(b"\xed\xa0\x80", &key, &chain),
    ]);
    let memory = MemoryBudget::new(16 << 20);
    let store = protected(&bytes, Some("12345678"), &memory).unwrap();
    assert_eq!(store.identities().len(), 3);
    assert!(store.identities().iter().any(|identity| identity.alias() == [0xd800]));
    for identity in store.identities() { assert!(identity.recover(Some("12345678"), &memory).is_ok()); }
}

#[test]
fn recovered_private_keys_own_charges_after_the_protected_store_drops() {
    let bytes = include_bytes!("fixtures/rsa1024.jks");
    let memory = MemoryBudget::new(16 << 20);
    let store = protected(bytes, Some("12345678"), &memory).unwrap();
    let original = memory.used_bytes();
    let first = store.identities()[0].recover(Some("12345678"), &memory).unwrap();
    let charge = memory.used_bytes() - original;
    let second = store.identities()[0].recover(Some("12345678"), &memory).unwrap();
    assert_eq!(first.private_key_pkcs8(), second.private_key_pkcs8());
    assert_eq!(memory.used_bytes(), original + charge * 2);
    let empty = MemoryBudget::new(0);
    let error = store.identities()[0].recover(Some("12345678"), &empty).err().unwrap();
    assert_eq!(error.code(), "rtdb/memory-budget-exceeded");
    drop(store); assert_eq!(memory.used_bytes(), charge * 2);
    assert!(openssl::pkey::PKey::private_key_from_pkcs8(first.private_key_pkcs8()).is_ok());
    let retained = first.reservation(); drop(first); assert_eq!(memory.used_bytes(), charge * 2);
    drop(retained); assert_eq!(memory.used_bytes(), charge);
    drop(second); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn protected_jks_integrity_and_certificate_errors_remain_load_time_failures() {
    let (key, chain) = fixture_key();
    let memory = MemoryBudget::new(16 << 20);
    assert!(protected(include_bytes!("fixtures/rsa1024.jks"), Some("wrong"), &memory).is_err());
    assert!(protected(&jks_store(&[key_entry(b"bad", &key, &[b"invalid certificate".to_vec()])]), Some("12345678"), &memory).is_err());
    let bytes = jks_store(&[key_entry(b"empty", b"invalid key", &[]), certificate_entry(b"trusted", &chain[0])]);
    assert!(protected(&bytes, Some("12345678"), &memory).unwrap().identities().is_empty());
    assert!(protected(include_bytes!("fixtures/empty.jks"), Some("12345678"), &memory).unwrap().identities().is_empty());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn protected_recovery_preserves_java_algorithm_only_validation_and_eager_key_equality() {
    let (key, _) = fixture_key();
    let other = openssl::x509::X509::from_pem(include_bytes!("../../../../src/test/resources/xtdb/pgwire/xtdb.example.com.crt")).unwrap();
    let bytes = jks_store(&[key_entry(b"different", &key, &[other.to_der().unwrap()])]);
    let memory = MemoryBudget::new(16 << 20);
    let store = protected(&bytes, Some("12345678"), &memory).unwrap();
    let recovered = store.identities()[0].recover(Some("12345678"), &memory).unwrap();
    let recovered = openssl::pkey::PKey::private_key_from_pkcs8(recovered.private_key_pkcs8()).unwrap();
    assert!(!other.public_key().unwrap().public_eq(&recovered));
    assert!(decode(&bytes, Some("12345678"), None, StoreUse::Identity).is_err());
}

#[test]
fn protected_recovery_rejects_a_private_key_entry_with_a_different_algorithm() {
    let (key, chain) = fixture_key();
    let source = openssl::x509::X509::from_der(&chain[0]).unwrap();
    let group = openssl::ec::EcGroup::from_curve_name(openssl::nid::Nid::X9_62_PRIME256V1).unwrap();
    let ec = openssl::pkey::PKey::from_ec_key(openssl::ec::EcKey::generate(&group).unwrap()).unwrap();
    let mut certificate = openssl::x509::X509::builder().unwrap(); certificate.set_version(2).unwrap();
    certificate.set_serial_number(source.serial_number()).unwrap();
    certificate.set_subject_name(source.subject_name()).unwrap(); certificate.set_issuer_name(source.subject_name()).unwrap();
    certificate.set_not_before(source.not_before()).unwrap(); certificate.set_not_after(source.not_after()).unwrap();
    certificate.set_pubkey(&ec).unwrap(); certificate.sign(&ec, openssl::hash::MessageDigest::sha256()).unwrap();
    let bytes = jks_store(&[key_entry(b"different algorithm", &key, &[certificate.build().to_der().unwrap()])]);
    let memory = MemoryBudget::new(16 << 20);
    let store = protected(&bytes, Some("12345678"), &memory).unwrap();
    assert_eq!(store.identities().len(), 1);
    assert!(store.identities()[0].recover(Some("12345678"), &memory).is_err());
}
