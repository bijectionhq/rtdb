use super::*;
use crate::{StoreFormat, StoreUse, java_der::{Der, encoded}};
use sha1::{Digest, Sha1};

fn alias(text: &str) -> Vec<u16> { text.encode_utf16().collect() }
fn password(text: &str) -> Vec<u8> { text.encode_utf16().flat_map(u16::to_be_bytes).collect() }
fn fixture() -> (Vec<u8>, Vec<Vec<u8>>) {
    crate::jks_entries(include_bytes!("../../tests/fixtures/rsa1024.jks"), Some("12345678"), |_| Ok(())).unwrap().into_enumeration().find_map(|(_, entry)| {
        if let JksEntry::Key { protected, certificates } = entry { Some((protected.to_vec(), certificates.into_iter().map(<[u8]>::to_vec).collect())) } else { None }
    }).unwrap()
}
fn prefix(tag: u32, alias: &[u16]) -> Vec<u8> {
    let mut text = Vec::new();
    for &unit in alias {
        match unit {
            1..=0x7f => text.push(unit as u8),
            0..=0x7ff => text.extend_from_slice(&[0xc0 | (unit >> 6) as u8, 0x80 | (unit & 63) as u8]),
            _ => text.extend_from_slice(&[0xe0 | (unit >> 12) as u8, 0x80 | ((unit >> 6) & 63) as u8, 0x80 | (unit & 63) as u8]),
        }
    }
    let mut result = tag.to_be_bytes().to_vec();
    result.extend_from_slice(&(text.len() as u16).to_be_bytes()); result.extend(text); result.extend_from_slice(&[0; 8]); result
}
fn certificate(output: &mut Vec<u8>, bytes: &[u8]) {
    output.extend_from_slice(&[0, 5]); output.extend_from_slice(b"X.509");
    output.extend_from_slice(&(bytes.len() as u32).to_be_bytes()); output.extend_from_slice(bytes);
}
fn key(alias: &[u16], protected: &[u8], chain: &[Vec<u8>]) -> Vec<u8> {
    let mut result = prefix(1, alias);
    result.extend_from_slice(&(protected.len() as u32).to_be_bytes()); result.extend_from_slice(protected);
    result.extend_from_slice(&(chain.len() as u32).to_be_bytes());
    for bytes in chain { certificate(&mut result, bytes); }
    result
}
fn trusted(alias: &[u16], bytes: &[u8]) -> Vec<u8> { let mut result = prefix(2, alias); certificate(&mut result, bytes); result }
fn store(entries: &[Vec<u8>]) -> Vec<u8> {
    let mut result = vec![0xfe, 0xed, 0xfe, 0xed, 0, 0, 0, 2];
    result.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for entry in entries { result.extend_from_slice(entry); }
    let mut hash = Sha1::new(); hash.update(password("store")); hash.update(b"Mighty Aphrodite"); hash.update(&result);
    result.extend_from_slice(&hash.finalize()); result
}
fn protect(plaintext: &[u8], key_password: &str) -> Vec<u8> {
    let password = password(key_password);
    let mut protected = vec![0x53; 20]; let mut previous = [0x53; 20];
    for block in plaintext.chunks(20) {
        let mut hash = Sha1::new(); hash.update(&password); hash.update(previous); previous = hash.finalize().into();
        protected.extend(block.iter().zip(previous).map(|(byte, mask)| byte ^ mask));
    }
    let mut hash = Sha1::new(); hash.update(password); hash.update(plaintext); protected.extend_from_slice(&hash.finalize());
    encoded(0x30, &[encoded(0x30, &encoded(6, &[0x2b, 6, 1, 4, 1, 0x2a, 2, 0x11, 1, 1])), encoded(4, &protected)].concat())
}
fn load(bytes: &[u8], key_password: Option<&str>, memory: &Arc<MemoryBudget>) -> Result<SunX509JksStore> {
    let charge = memory.reserve(SunX509JksStore::allocation_bound(bytes.len(), Some("store"), key_password)?)?;
    SunX509JksStore::decode(bytes, Some("store"), key_password, charge)
}
fn unknown_key(version: &[u8], optional: &[u8]) -> Vec<u8> {
    encoded(0x30, &[encoded(2, version), encoded(0x30, &encoded(6, &[0x2a, 3, 4, 100])), encoded(4, &[1, 2, 3]), optional.to_vec()].concat())
}

mod factory;

#[test]
fn raw_jks_aliases_resolve_before_eager_recovery_and_share_the_lowercase_entry() {
    let (protected, chain) = fixture(); let memory = MemoryBudget::new(32 << 20);
    for entries in [vec![key(&alias("UPPER"), b"broken", &chain)],
        vec![key(&alias("UPPER"), b"broken", &chain), trusted(&alias("upper"), &chain[0])]] {
        assert!(load(&store(&entries), None, &memory).unwrap().identities().is_empty());
    }
    let bytes = store(&[key(&alias("UPPER"), b"broken", &chain), key(&alias("upper"), &protected, &chain)]);
    let loaded = load(&bytes, Some("12345678"), &memory).unwrap();
    assert_eq!(loaded.identities().len(), 2);
    assert!(Arc::ptr_eq(&loaded.identities[0].entry, &loaded.identities[1].entry));
    let names = loaded.identities().iter().map(|identity| identity.alias().to_vec()).collect::<Vec<_>>();
    assert!(names.contains(&alias("UPPER"))); assert!(names.contains(&alias("upper")));
    drop(loaded); assert_eq!(memory.used_bytes(), 0);
    let bytes = store(&[trusted(&alias("UPPER"), &chain[0]), key(&alias("upper"), &protected, &chain)]);
    let loaded = load(&bytes, Some("12345678"), &memory).unwrap();
    assert_eq!(loaded.identities().len(), 2);
    assert!(Arc::ptr_eq(&loaded.identities[0].entry, &loaded.identities[1].entry));
    drop(loaded); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn eager_alias_lookup_preserves_unicode_expansion_surrogates_and_duplicate_replacement() {
    let (protected, chain) = fixture(); let memory = MemoryBudget::new(32 << 20);
    let surrogate = vec![0xd800];
    let entries = vec![key(&alias("\u{130}"), b"broken", &chain), key(&alias("i\u{307}"), &protected, &chain),
        key(&surrogate, b"overwritten", &chain), key(&surrogate, &protected, &chain)];
    let loaded = load(&store(&entries), Some("12345678"), &memory).unwrap();
    assert_eq!(loaded.identities().len(), 3);
    for name in [alias("\u{130}"), alias("i\u{307}"), surrogate] { assert!(loaded.identities().iter().any(|identity| identity.alias() == name)); }
    drop(loaded); assert_eq!(memory.used_bytes(), 0);
    let malformed = store(&[trusted(&alias("same"), b"bad certificate"), key(&alias("same"), &protected, &chain)]);
    assert!(load(&malformed, Some("12345678"), &memory).is_err()); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn eager_credentials_keep_hashtable_alias_order_across_collision_and_resize() {
    let (protected, chain) = fixture(); let memory = MemoryBudget::new(32 << 20);
    for (names, expected) in [
        (vec!["a", "b", "c", "d", "e", "f", "g", "h"], vec!["b", "a", "h", "g", "f", "e", "d", "c"]),
        (vec!["a", "b", "c", "d", "e", "f", "g", "h", "i"], vec!["i", "h", "g", "f", "e", "d", "c", "b", "a"]),
        (vec!["a", "l", "a"], vec!["l", "a"]),
    ] {
        let entries = names.iter().map(|name| key(&alias(name), &protected, &chain)).collect::<Vec<_>>();
        let loaded = load(&store(&entries), Some("12345678"), &memory).unwrap();
        assert_eq!(loaded.identities().iter().map(|identity| String::from_utf16(identity.alias()).unwrap()).collect::<Vec<_>>(), expected);
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn empty_chains_still_recover_and_parse_keys_without_native_provider_acceptance() {
    let (protected, _) = fixture(); let memory = MemoryBudget::new(32 << 20);
    let valid = store(&[key(&alias("key"), &protected, &[])]);
    assert!(load(&valid, Some("12345678"), &memory).unwrap().identities().is_empty());
    for password in [None, Some("wrong"), Some("")] { assert!(load(&valid, password, &memory).is_err()); }
    for bytes in [b"bad wrapper".to_vec(), protect(b"bad PKCS8", "key"), protect(&unknown_key(&[2], &[]), "key")] {
        assert!(load(&store(&[key(&alias("key"), &bytes, &[])]), Some("key"), &memory).is_err());
    }
    for oid in [&[0x2a, 3, 4, 100][..], &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 1][..]] {
        let plaintext = encoded(0x30, &[encoded(2, &[0]), encoded(0x30, &encoded(6, oid)), encoded(4, b"not a native private key")].concat());
        let bytes = store(&[key(&alias("key"), &protect(&plaintext, ""), &[])]);
        assert!(load(&bytes, Some(""), &memory).unwrap().identities().is_empty());
        assert!(load(&bytes, None, &memory).is_err());
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sun_recovery_accepts_java_protection_envelopes_without_changing_generic_decoder() {
    let (protected, chain) = fixture(); let memory = MemoryBudget::new(32 << 20);
    let outer = crate::java_der::single(&protected, 0x30).unwrap(); let mut fields = Der(&outer);
    let algorithm = fields.expect(0x30).unwrap(); let encrypted = fields.expect(4).unwrap();
    let parameters = encoded(1, &[0xff]);
    let mut oid = Der(&algorithm); let oid = oid.expect(6).unwrap();
    let algorithm = [vec![0x30, 0x80], encoded(6, &oid), parameters, vec![0, 0]].concat();
    let encrypted = [vec![0x24, 0x80], encoded(4, &encrypted[..20]), encoded(4, &encrypted[20..]), vec![0, 0]].concat();
    for tag in [0x30, 0x31, 4] {
        let protected = encoded(tag, &[algorithm.clone(), encrypted.clone()].concat());
        let bytes = store(&[key(&alias("key"), &protected, &chain)]);
        assert_eq!(load(&bytes, Some("12345678"), &memory).unwrap().identities().len(), 1);
        assert!(crate::decrypt_jks_key(&protected, &password("12345678")).is_err());
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn pkcs8_versions_attributes_and_optional_public_bits_follow_raw_key_encoding() {
    let (_, chain) = fixture(); let memory = MemoryBudget::new(32 << 20);
    for version in [0, 1] {
        for attribute_tag in [0x80, 0xa0] {
            let optional = encoded(attribute_tag, &[0xff, 0]);
            let plaintext = unknown_key(&[0, version], &optional);
            let loaded = load(&store(&[key(&alias("key"), &protect(&plaintext, "key"), &chain)]), Some("key"), &memory).unwrap();
            assert_eq!(loaded.identities()[0].private_key_pkcs8(), unknown_key(&[version], &if version == 0 { vec![] } else { encoded(0xa0, &[0xff, 0]) }));
        }
    }
    for tag in [0x81, 0xa1] {
        let plaintext = unknown_key(&[1], &encoded(tag, &[3, 0xff]));
        let loaded = load(&store(&[key(&alias("key"), &protect(&plaintext, "key"), &chain)]), Some("key"), &memory).unwrap();
        assert_eq!(loaded.identities()[0].private_key_pkcs8(), unknown_key(&[1], &encoded(0x81, &[3, 0xf8])));
    }
    for (version, optional) in [(0, encoded(0x81, &[0])), (1, encoded(0x82, &[])), (1, encoded(0x81, &[8, 0])),
        (1, [encoded(0x80, &[]), encoded(0x80, &[])].concat()), (1, [encoded(0x81, &[0]), encoded(5, &[])].concat())] {
        assert!(load(&store(&[key(&alias("key"), &protect(&unknown_key(&[version], &optional), "key"), &[])]), Some("key"), &memory).is_err());
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn pkcs8_ber_containers_accept_primitive_private_keys_and_reject_constructed_values() {
    let (_, chain) = fixture(); let memory = MemoryBudget::new(32 << 20);
    let algorithm = encoded(0x30, &[encoded(6, &[0x2a, 3, 4, 100]), vec![5, 0x81, 0]].concat());
    for plaintext in [
        [vec![0x30, 0x80], encoded(2, &[0]), algorithm.clone(), encoded(4, &[1, 2, 3]), vec![0, 0]].concat(),
        encoded(0x30, &[encoded(2, &[0]), algorithm.clone(), vec![4, 0x81, 3, 1, 2, 3]].concat()),
    ] {
        let loaded = load(&store(&[key(&alias("key"), &protect(&plaintext, "key"), &chain)]), Some("key"), &memory).unwrap();
        assert_eq!(loaded.identities()[0].private_key_pkcs8(), unknown_key(&[0], &[]));
        drop(loaded); assert_eq!(memory.used_bytes(), 0);
    }
    for content in [encoded(0x24, &encoded(4, &[1, 2, 3])), vec![0x24, 0x80, 4, 3, 1, 2, 3, 0, 0],
        vec![0x24, 0x80, 4, 1, 7, 2, 1, 9, 0, 0], vec![0x30, 0x80, 4, 1, 7, 0, 0]] {
        let plaintext = encoded(0x30, &[encoded(2, &[0]), algorithm.clone(), content].concat());
        for certificates in [chain.as_slice(), &[]] {
            assert!(load(&store(&[key(&alias("key"), &protect(&plaintext, "key"), certificates)]), Some("key"), &memory).is_err());
            assert_eq!(memory.used_bytes(), 0);
        }
    }
}

#[test]
fn eager_entry_sharing_retains_one_key_and_chain_and_rejects_unadmitted_work() {
    let (protected, chain) = fixture(); let memory = MemoryBudget::new(32 << 20);
    let bytes = store(&[key(&alias("LOWER"), b"unused", &chain), key(&alias("Lower"), b"unused", &chain), key(&alias("lower"), &protected, &chain)]);
    let loaded = load(&bytes, Some("12345678"), &memory).unwrap();
    assert_eq!(loaded.identities().len(), 3);
    let entry = Arc::clone(&loaded.identities[0].entry);
    for identity in loaded.identities() { assert!(Arc::ptr_eq(&entry, &identity.entry)); }
    drop(loaded); assert_eq!(memory.used_bytes(), entry._reservation.bytes() + entry.private_key.retained_bytes());
    assert!(!entry.private_key.encoded_pkcs8().is_empty()); assert_eq!(entry.certificates, chain);
    drop(entry); assert_eq!(memory.used_bytes(), 0);
    assert!(SunX509JksStore::decode(&bytes, Some("store"), Some("12345678"), memory.reserve(0).unwrap()).is_err());
    let bound = SunX509JksStore::allocation_bound(bytes.len(), Some("store"), Some("12345678")).unwrap();
    let limited = MemoryBudget::new(bound);
    assert_eq!(load(&bytes, Some("12345678"), &limited).err().unwrap().code, "rtdb/memory-budget-exceeded");
    assert_eq!(limited.used_bytes(), 0);
}

#[test]
fn eager_sun_material_accepts_mismatch_while_generic_identity_stays_strict() {
    let (_, chain) = fixture(); let memory = MemoryBudget::new(32 << 20);
    let key_bytes = openssl::pkey::PKey::from_rsa(openssl::rsa::Rsa::generate(1024).unwrap()).unwrap().private_key_to_pkcs8().unwrap();
    let bytes = store(&[key(&alias("key"), &protect(&key_bytes, "key"), &chain)]);
    let loaded = load(&bytes, Some("key"), &memory).unwrap();
    assert_eq!(loaded.identities()[0].private_key_pkcs8(), key_bytes);
    let charge = memory.reserve(KeyStore::allocation_bound_with_passwords(bytes.len(), Some("store"), Some("key")).unwrap()).unwrap();
    assert!(KeyStore::decode(&bytes, StoreFormat::Java, StoreUse::OptionalIdentity, Some("store"), Some("key"), charge).is_err());
    drop(loaded); assert_eq!(memory.used_bytes(), 0);
}
