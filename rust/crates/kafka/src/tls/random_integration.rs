use super::*;
use rtdb_columnar::MemoryBudget;

#[test]
fn kafka_random_contexts_have_independent_lifetimes_and_signature_defaults() {
    let memory = MemoryBudget::new(32 << 20);
    let options = BTreeMap::from([("security.protocol".into(), "SSL".into()), ("ssl.secure.random.implementation".into(), " SHA1PRNG ".into())]);
    let owner = NativeTls::from_java(&options, &mut |bytes| memory.reserve(bytes)).unwrap().unwrap();
    assert_ne!(owner.random.as_ptr(), owner.signature_random.as_ptr());
    assert_eq!(unsafe { rdkafka::config::SslContextConfigurer::library_context(owner.as_ref()) }, owner.random.as_ptr().cast());
    assert!(memory.used_bytes() > owner.reservations().iter().map(|charge| charge.bytes()).sum::<usize>());
    let retained = Arc::clone(&owner); drop(owner);
    assert!(memory.used_bytes() > 0);
    drop(retained); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn kafka_random_admission_preserves_java_string_trimming_and_releases_failures() {
    let memory = MemoryBudget::new(32 << 20);
    for algorithm in ["", "custom.Random", "\u{a0}SHA1PRNG\u{a0}"] {
        let options = BTreeMap::from([("security.protocol".into(), "SSL".into()), ("ssl.secure.random.implementation".into(), algorithm.into())]);
        assert!(NativeTls::from_java(&options, &mut |bytes| memory.reserve(bytes)).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
}
