use super::*;

const RSA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 1];
const PSS: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 10];
const PBES2: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 13];
const SPECIFIED: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 4, 3];
const DH: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 3, 1];

fn algorithm(oid: &[u8], parameter: &[u8]) -> Vec<u8> { encoded(0x30, &[encoded(6, oid), parameter.to_vec()].concat()) }
fn pkcs8(oid: &[u8], parameter: &[u8], private: &[u8]) -> Vec<u8> {
    encoded(0x30, &[encoded(2, &[0]), algorithm(oid, parameter), encoded(4, private)].concat())
}
fn material(crt: bool) -> Vec<u8> {
    let values = [vec![0x80; 64], vec![3], vec![7], vec![if crt { 1 } else { 0 }], vec![2], vec![3], vec![4], vec![5]];
    encoded(0x30, &[encoded(2, &[0]), values.iter().flat_map(|value| encoded(2, value)).collect()].concat())
}
fn pbes2() -> Vec<u8> {
    let kdf = algorithm(&[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 12], &encoded(0x30, &[encoded(4, &[]), encoded(2, &[1])].concat()));
    encoded(0x30, &[kdf, algorithm(&[0x60, 0x86, 0x48, 1, 0x65, 3, 4, 1, 2], &encoded(4, &[]))].concat())
}
fn unmapped_pss(crt: bool) -> Vec<u8> {
    let parameter = encoded(0x30, &encoded(0xa0, &algorithm(SPECIFIED, &algorithm(&[0x2a, 3, 4], &[]))));
    pkcs8(PSS, &parameter, &material(crt))
}

#[test]
fn eager_jks_passes_normalized_raw_bytes_to_the_dh_factory() {
    let (_, chain) = fixture(); let memory = MemoryBudget::new(32 << 20);
    let parameters = encoded(0x30, &[encoded(2, &[1]), encoded(2, &[2])].concat());
    let private = encoded(2, &[0, 0xff]);
    let plaintext = [vec![0x30, 0x80], encoded(2, &[0, 0]), algorithm(DH, &parameters), encoded(4, &private), vec![0, 0]].concat();
    let bytes = store(&[key(&alias("key"), &protect(&plaintext, "key"), &chain)]);
    let loaded = load(&bytes, Some("key"), &memory).unwrap();
    assert_eq!(loaded.identities()[0].private_key_pkcs8(), pkcs8(DH, &parameters, &private));
    drop(loaded); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn eager_jks_checked_factory_rejections_return_raw_keys_before_chain_filtering() {
    let (_, chain) = fixture(); let memory = MemoryBudget::new(32 << 20);
    for oid in [RSA, DH, &[0x2a, 0x86, 0x48, 0xce, 0x3d, 2, 1][..]] {
        let plaintext = pkcs8(oid, &[], b"opaque private material");
        for certificates in [chain.as_slice(), &[]] {
            let bytes = store(&[key(&alias("key"), &protect(&plaintext, "key"), certificates)]);
            let loaded = load(&bytes, Some("key"), &memory).unwrap();
            if certificates.is_empty() { assert!(loaded.identities().is_empty()); }
            else {
                let parameter = if oid == RSA { encoded(5, &[]) } else { vec![] };
                assert_eq!(loaded.identities()[0].private_key_pkcs8(), pkcs8(oid, &parameter, b"opaque private material"));
            }
            drop(loaded); assert_eq!(memory.used_bytes(), 0);
        }
    }
}

#[test]
fn eager_jks_registered_parameter_errors_abort_even_empty_chains() {
    let (_, chain) = fixture(); let memory = MemoryBudget::new(32 << 20);
    for oid in [DH, PBES2, &[0x2a, 0x86, 0x48, 0xce, 0x3d, 2, 1][..], &[0x60, 0x86, 0x48, 1, 0x65, 3, 4, 1][..]] {
        let plaintext = pkcs8(oid, &encoded(0x30, &[]), b"opaque");
        for certificates in [chain.as_slice(), &[]] {
            let bytes = store(&[key(&alias("key"), &protect(&plaintext, "key"), certificates)]);
            assert!(load(&bytes, Some("key"), &memory).is_err());
            assert_eq!(memory.used_bytes(), 0);
        }
    }
}

#[test]
fn eager_jks_top_level_derived_names_retain_their_raw_parameter_encoding() {
    let (_, chain) = fixture(); let memory = MemoryBudget::new(32 << 20);
    for (oid, parameter) in [(PBES2, pbes2()), (SPECIFIED, algorithm(&[0x2b, 0x0e, 3, 2, 26], &[])),
        (SPECIFIED, encoded(0x30, &[]))] {
        let plaintext = pkcs8(oid, &parameter, b"opaque");
        let bytes = store(&[key(&alias("key"), &protect(&plaintext, "key"), &chain)]);
        let loaded = load(&bytes, Some("key"), &memory).unwrap();
        assert_eq!(loaded.identities()[0].private_key_pkcs8(), plaintext);
        drop(loaded); assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn eager_jks_crt_pss_accepts_unmapped_hash_names_but_noncrt_encoding_is_fatal() {
    let (_, chain) = fixture(); let memory = MemoryBudget::new(32 << 20);
    for certificates in [chain.as_slice(), &[]] {
        let crt = unmapped_pss(true);
        let bytes = store(&[key(&alias("key"), &protect(&crt, "key"), certificates)]);
        let loaded = load(&bytes, Some("key"), &memory).unwrap();
        if certificates.is_empty() { assert!(loaded.identities().is_empty()); }
        else { assert_eq!(loaded.identities()[0].private_key_pkcs8(), crt); }
        drop(loaded); assert_eq!(memory.used_bytes(), 0);
        let bytes = store(&[key(&alias("key"), &protect(&unmapped_pss(false), "key"), certificates)]);
        assert!(load(&bytes, Some("key"), &memory).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn repeated_jks_alias_recovery_repeats_factory_admission_before_reusing_the_entry() {
    let (_, chain) = fixture(); let memory = MemoryBudget::new(32 << 20);
    let plaintext = pkcs8(RSA, &[], &material(true));
    let normalized = pkcs8(RSA, &encoded(5, &[]), &material(true));
    let protected = protect(&plaintext, "key");
    let mut entry = Entry::Pending(JksEntry::Key { protected: &protected, certificates: chain.iter().map(Vec::as_slice).collect() });
    let recovered = entry.recover(Some(&password("key")), &memory).unwrap().unwrap();
    let recovery_bound = protected.len() * 512 + 65_536;
    let raw_bound = plaintext.len() * 384 + 65_536;
    let factory_bound = normalized.len() * 384 + 65_536;
    let insufficient = MemoryBudget::new(recovery_bound + raw_bound + factory_bound - 1);
    assert_eq!(entry.recover(Some(&password("key")), &insufficient).err().unwrap().code, "rtdb/memory-budget-exceeded");
    assert_eq!(insufficient.used_bytes(), 0);
    let exact = MemoryBudget::new(recovery_bound + raw_bound + factory_bound);
    let shared = entry.recover(Some(&password("key")), &exact).unwrap().unwrap();
    assert!(Arc::ptr_eq(&recovered, &shared));
    assert_eq!(exact.used_bytes(), 0);
    drop(shared); drop(entry); drop(recovered);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn eager_entry_and_key_owners_retain_each_inline_size_and_buffer_once() {
    let (protected, chain) = fixture(); let memory = MemoryBudget::new(32 << 20);
    let bytes = store(&[key(&alias("key"), &protected, &chain)]);
    let loaded = load(&bytes, Some("12345678"), &memory).unwrap();
    let entry = Arc::clone(&loaded.identities[0].entry);
    let entry_bound = std::mem::size_of::<RecoveredEntry>() - std::mem::size_of::<PrivateKey>() + 128
        + entry.certificates.capacity() * std::mem::size_of::<Vec<u8>>()
        + entry.certificates.iter().map(Vec::capacity).sum::<usize>();
    assert_eq!(entry._reservation.bytes(), entry_bound);
    let key_bound = entry.private_key.retained_bytes();
    drop(loaded);
    assert_eq!(memory.used_bytes(), entry_bound + key_bound);
    assert!(!entry.private_key.encoded_pkcs8().is_empty());
    drop(entry);
    assert_eq!(memory.used_bytes(), 0);
}
