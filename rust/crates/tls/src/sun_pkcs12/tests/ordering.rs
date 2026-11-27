use super::*;
use crate::private_factory::{FactoryInput, PrivateKey};

const HSS: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 9, 16, 3, 17];
const DH: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 3, 1];

fn input(tag: u8, oid: &[u8], parameter: &[u8], version: u8) -> Vec<u8> {
    encoded(tag, &[encoded(2, &[version]), seq(&[encoded(6, oid), parameter.to_vec()])].concat())
}
fn recover(plaintext: &[u8], memory: &Arc<MemoryBudget>) -> crate::TlsResult<PrivateKey> {
    PrivateKey::from_pkcs12(FactoryInput::pkcs12(plaintext).map_err(crate::TlsError::from_invalid_anomaly)?, memory)
}

#[test]
fn sun_pkcs12_opaque_parameters_and_set_outer_reach_factories_before_empty_chain_filtering() {
    let memory = MemoryBudget::new(32 << 20);
    for (plaintext, expected) in [
        (input(0x31, &[0x2a, 3, 4], &[], 0), "Unsupported Java private-key factory"),
        (input(0x30, &[0x2a, 3, 4], &[5, 1, 0], 0), "Unsupported Java private-key factory"),
        (input(0x30, HSS, &[5, 1, 0], 0), "Java HSS/LMS does not generate private keys"),
        (input(0x31, HSS, &[], 2), "Java HSS/LMS does not generate private keys"),
    ] {
        let bytes = pfx(&[key_bag(&protect(&plaintext, "key"), Some(1), Some("key"))]);
        assert_eq!(load(&bytes, Some("key"), &memory).err().unwrap(), invalid(expected));
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn sun_pkcs12_dh_identity_keeps_original_ber_and_rejects_original_version_zero_attributes() {
    let (_, certificate) = fixture(); let memory = MemoryBudget::new(32 << 20);
    let parameters = seq(&[encoded(2, &[1]), encoded(2, &[2])]);
    let fields = [encoded(2, &[0, 0]), seq(&[encoded(6, DH), parameters]), encoded(4, &encoded(2, &[0, 0xff]))].concat();
    let original = [vec![0x30, 0x80], fields.clone(), vec![0, 0]].concat();
    let bytes = pfx(&[key_bag(&protect(&original, "key"), Some(1), Some("key")), certificate_bag(&certificate, Some(1), Some("key"), false)]);
    let loaded = load(&bytes, Some("key"), &memory).unwrap();
    assert_eq!(loaded.identities()[0].private_key_pkcs8(), original);
    drop(loaded); assert_eq!(memory.used_bytes(), 0);
    let original = encoded(0x30, &[fields, encoded(0x80, &[])].concat());
    let bytes = pfx(&[key_bag(&protect(&original, "key"), Some(1), Some("key"))]);
    assert!(load(&bytes, Some("key"), &memory).is_err());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sun_pkcs12_empty_password_retries_preparse_and_factory_errors_and_returns_the_last_failure() {
    let memory = MemoryBudget::new(4 << 20); let mut attempts = 0;
    let first = input(4, &[0x2a, 3, 4], &[], 0);
    let second = input(0x31, HSS, &[5, 1, 0], 2);
    let error = protection::retry(Some(""), |password| {
        attempts += 1;
        if password.is_empty() { recover(&first, &memory) }
        else { assert_eq!(password, "\0"); recover(&second, &memory) }
    }).err().unwrap();
    assert_eq!(attempts, 2);
    assert_eq!(error.into_anomaly(), invalid("Java HSS/LMS does not generate private keys"));
    assert_eq!(memory.used_bytes(), 0);
    let none = MemoryBudget::new(0); let mut attempts = 0;
    let error = protection::retry(Some(""), |_| { attempts += 1; recover(&second, &none) }).err().unwrap();
    assert_eq!(attempts, 1);
    assert_eq!(error.code(), "rtdb/memory-budget-exceeded");
    assert_eq!(none.used_bytes(), 0);
}
