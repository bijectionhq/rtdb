use super::*;
use crate::java_der::encoded;

fn seq(parts: &[Vec<u8>]) -> Vec<u8> { encoded(0x30, &parts.concat()) }
fn bag(kind: u8, attributes: &[u8]) -> Vec<u8> {
    let mut oid = BAG_PREFIX.to_vec(); oid.push(kind);
    seq(&[encoded(6, &oid), encoded(0xa0, &encoded(5, &[])), attributes.to_vec()])
}

#[test]
fn empty_password_retry_preserves_entries_and_counters_from_partially_loaded_safe_contents() {
    let bad_attribute = seq(&[encoded(6, &[]), encoded(0x31, &[])]);
    let first = seq(&[bag(2, &[]), bag(4, &encoded(0x31, &bad_attribute))]); let second = seq(&[]);
    let memory = MemoryBudget::new(32 << 20); let charge = memory.reserve(256 * first.len() + 65_536).unwrap();
    let mut loaded = LoadedPkcs12::default(); let mut attempts = 0;
    protection::retry(Some(""), |password| {
        attempts += 1;
        if password.is_empty() { loaded.read(&first, &memory) }
        else { assert_eq!(password, "\0"); loaded.read(&second, &memory) }
    }).unwrap();
    assert_eq!(attempts, 2); assert_eq!(loaded.private_keys, 1); assert_eq!(loaded.unnamed, 1);
    assert_eq!(loaded.entries.len(), 1); assert_eq!(loaded.keys.len(), 1);
    assert_eq!(loaded.entries[0].alias, [b'1' as u16]); assert!(matches!(loaded.entries[0].entry, Entry::Private(0)));
    assert!(loaded.recover(Some(""), &memory).is_err()); drop(charge); assert_eq!(memory.used_bytes(), 0);
}
