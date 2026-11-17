use super::*;

fn hex(value: &str) -> Vec<u8> {
    value.as_bytes().chunks_exact(2).map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap()).collect()
}

#[test]
fn algorithm_lookup_is_case_insensitive_but_preserves_empty_and_whitespace() {
    for (name, expected) in [("sha1prng", Algorithm::Sha1), ("DRBG", Algorithm::Drbg), ("NativePRNG", Algorithm::Native), ("nativeprngblocking", Algorithm::Blocking), ("NativePRNGNonBlocking", Algorithm::NonBlocking)] {
        assert_eq!(Algorithm::parse(name).unwrap(), expected);
    }
    for name in ["", " SHA1PRNG", "DRBG ", "SHA-1", "DEFAULT", "UnknownRandom"] { assert!(Algorithm::parse(name).is_err()); }
    assert_eq!(Algorithm::parse("Windows-PRNG").is_ok(), cfg!(windows));
}

#[cfg(not(unix))]
#[test]
fn native_device_algorithms_are_unavailable_without_unix_provider_registration() {
    let budget = MemoryBudget::new(1 << 24);
    for name in ["NativePRNG", "NativePRNGBlocking", "NativePRNGNonBlocking"] {
        assert!(Context::new(Some(name), &budget).is_err());
        assert_eq!(budget.used_bytes(), 0);
    }
}

#[cfg(windows)]
#[test]
fn windows_prng_context_generates_and_releases_its_admission() {
    let budget = MemoryBudget::new(1 << 24);
    let context = Context::new(Some("Windows-PRNG"), &budget).unwrap();
    let mut output = [0; 32];
    context.fill(&mut output).unwrap();
    assert_eq!(budget.used_bytes(), 262_144);
    drop(context);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn hash_drbg_matches_nist_sha256_vector_after_two_requests() {
    let entropy = hex("a65ad0f345db4e0effe875c3a2e71f42c7129d620ff5c119a9ef55f05185e0fb");
    let nonce = hex("8581f9317517276e06e9607ddbcbcc2e");
    let expected = hex("d3e160c35b99f340b2628264d1751060e0045da383ff57a57d73a673d2b8d80daaf6a6c35a91bb4579d73fd0c8fed111b0391306828adfed528f018121b3febdc343e797b87dbb63db1333ded9d1ece177cfa6b71fe8ab1da46624ed6415e51ccde2c7ca86e283990eeaeb91120415528b2295910281b02dd431f4c9f70427df");
    let mut random = HashDrbg::new(&entropy, &nonce);
    let mut actual = vec![0; expected.len()];
    random.fill(&mut actual);
    random.fill(&mut actual);
    assert_eq!(actual, expected);
    assert_eq!(random.counter, 3);
}

#[test]
fn default_drbg_requests_128_bits_of_entropy_and_increments_a_128_bit_nonce() {
    let nonces = Mutex::new([0; 16]);
    let mut requests = Vec::new();
    let mut source = |bytes: &mut [u8]| { requests.push(bytes.len()); bytes.fill(7); Ok(()) };
    let first = HashDrbg::from_default_entropy(&mut source, &nonces).unwrap();
    let second = HashDrbg::from_default_entropy(&mut source, &nonces).unwrap();
    assert_eq!(requests, vec![16, 16]);
    let mut nonce = [0; 16];
    nonce[15] = 1;
    assert_eq!(first.value, HashDrbg::new(&[7; 16], &nonce).value);
    nonce[15] = 2;
    assert_eq!(second.value, HashDrbg::new(&[7; 16], &nonce).value);
}

#[test]
fn sha1prng_retains_unused_bytes_across_requests() {
    let mut complete = Sha1::new(b"fixture seed");
    let mut split = Sha1::new(b"fixture seed");
    let mut expected = [0; 101];
    let mut actual = [0; 101];
    complete.fill(&mut expected);
    for chunk in actual.chunks_mut(7) { split.fill(chunk); }
    assert_eq!(actual, expected);
    assert_eq!(split.state, complete.state);
    assert_eq!(split.remainder, complete.remainder);
    assert_eq!(split.position, complete.position);
    assert!(split.remainder[..split.position].iter().all(|byte| *byte == 0));
}

#[test]
fn modular_addition_carries_across_the_full_drbg_state() {
    let mut value = [255; 55];
    add(&mut value, &[1]);
    assert_eq!(value, [0; 55]);
    add(&mut value, &[255, 255]);
    add(&mut value, &[1]);
    assert_eq!(&value[52..], &[1, 0, 0]);
}

#[cfg(unix)]
#[test]
fn native_random_buffer_expires_and_reports_entropy_eof() {
    use std::io::Write;
    let mut fixture = tempfile::NamedTempFile::new().unwrap();
    fixture.write_all(&[7; 512]).unwrap();
    let source = File::open(fixture.path()).unwrap();
    let mut random = Native { _seed: source.try_clone().unwrap(), next: source, mixer: Sha1::new(b"native fixture"), buffer: Zeroizing::new(vec![0; 65_536].into_boxed_slice()), size: 256, position: 256, changes: 0, refreshed: SystemTime::UNIX_EPOCH, _charge: MemoryBudget::new(1 << 20).reserve(73_728).unwrap() };
    let now = SystemTime::UNIX_EPOCH + Duration::from_secs(1);
    random.refresh(now).unwrap();
    random.position = 1;
    random.refresh(now + Duration::from_millis(99)).unwrap();
    assert_eq!(random.position, 1);
    random.refresh(now + Duration::from_millis(100)).unwrap();
    assert_eq!(random.position, 0);
    random.position = random.size;
    assert!(random.refresh(now + Duration::from_millis(101)).is_err());
    assert_eq!(random.position, random.size);
}

#[test]
fn provider_contexts_keep_private_and_public_draws_in_the_same_independent_stream() {
    native::test_independent_streams();
}

#[test]
fn librdkafka_creates_tls_with_the_retained_selected_random_context() {
    native::test_librdkafka_context();
}

#[test]
fn certificate_signatures_use_the_separate_context_for_native_rsa_and_ec_keys() {
    native::test_signature_contexts();
}

#[test]
fn random_context_admission_rolls_back_and_survives_cloned_owners() {
    let tiny = MemoryBudget::new(262_143);
    assert!(Context::new(Some("SHA1PRNG"), &tiny).is_err());
    assert_eq!(tiny.used_bytes(), 0);
    let budget = MemoryBudget::new(1 << 24);
    assert!(Context::new(Some(""), &budget).is_err());
    assert_eq!(budget.used_bytes(), 0);
    let context = Context::new(Some("DRBG"), &budget).unwrap();
    let provider = Arc::clone(&context);
    drop(context);
    assert_eq!(budget.used_bytes(), 262_144);
    drop(provider);
    assert_eq!(budget.used_bytes(), 0);
}

#[cfg(unix)]
#[test]
fn native_contexts_share_their_live_device_cache_and_reservation() {
    let budget = MemoryBudget::new(1 << 24);
    let a = SharedNative::new(Algorithm::Native, &budget).unwrap();
    let b = SharedNative::new(Algorithm::Native, &budget).unwrap();
    assert!(Arc::ptr_eq(&a, &b));
    let registration = a.registration;
    let charge = Arc::downgrade(&a.random.lock().unwrap()._charge);
    assert_eq!(budget.used_bytes(), NATIVE_ALLOCATION_BOUND);
    drop(a);
    assert_eq!(charge.upgrade().unwrap().bytes(), NATIVE_ALLOCATION_BOUND);
    drop(b);
    assert!(charge.upgrade().is_none());
    assert_eq!(budget.used_bytes(), 0);
    assert!(!NATIVE_SOURCES.lock().unwrap().contains_key(&registration));
}

#[cfg(unix)]
#[test]
fn native_sources_release_independent_budgets_in_either_drop_order() {
    for first_drops_early in [false, true] {
        let first = MemoryBudget::new(1 << 24);
        let second = MemoryBudget::new(1 << 24);
        let a = SharedNative::new(Algorithm::Native, &first).unwrap();
        let b = SharedNative::new(Algorithm::Native, &second).unwrap();
        assert!(!Arc::ptr_eq(&a, &b));
        assert_eq!(first.used_bytes(), NATIVE_ALLOCATION_BOUND);
        assert_eq!(second.used_bytes(), NATIVE_ALLOCATION_BOUND);
        if first_drops_early {
            drop(a);
            assert_eq!(first.used_bytes(), 0);
            assert_eq!(second.used_bytes(), NATIVE_ALLOCATION_BOUND);
            drop(b);
        } else {
            drop(b);
            assert_eq!(second.used_bytes(), 0);
            assert_eq!(first.used_bytes(), NATIVE_ALLOCATION_BOUND);
            drop(a);
        }
        assert_eq!(first.used_bytes(), 0);
        assert_eq!(second.used_bytes(), 0);
    }
}

#[cfg(unix)]
#[test]
fn native_algorithms_are_independent_and_released_sources_can_be_recreated() {
    let budget = MemoryBudget::new(1 << 24);
    let first = SharedNative::new(Algorithm::Native, &budget).unwrap();
    let nonblocking = SharedNative::new(Algorithm::NonBlocking, &budget).unwrap();
    assert!(!Arc::ptr_eq(&first, &nonblocking));
    assert_eq!(budget.used_bytes(), 2 * NATIVE_ALLOCATION_BOUND);
    let weak = Arc::downgrade(&first);
    let registration = first.registration;
    drop(first);
    assert!(weak.upgrade().is_none());
    assert!(!NATIVE_SOURCES.lock().unwrap().contains_key(&registration));
    let replacement = SharedNative::new(Algorithm::Native, &budget).unwrap();
    assert!(!std::ptr::eq(weak.as_ptr(), Arc::as_ptr(&replacement)));
    assert_eq!(budget.used_bytes(), 2 * NATIVE_ALLOCATION_BOUND);
    drop(replacement);
    drop(nonblocking);
    assert_eq!(budget.used_bytes(), 0);
}

#[cfg(unix)]
#[test]
fn native_source_admission_failure_leaves_no_registry_entry_or_context_charge() {
    let budget = MemoryBudget::new(NATIVE_ALLOCATION_BOUND - 1);
    assert!(SharedNative::new(Algorithm::Native, &budget).is_err());
    assert_eq!(budget.used_bytes(), 0);
    let registration = (Arc::as_ptr(&budget) as usize, Algorithm::Native);
    assert!(!NATIVE_SOURCES.lock().unwrap().contains_key(&registration));
    let budget = MemoryBudget::new(262_144 + NATIVE_ALLOCATION_BOUND - 1);
    assert!(Context::new(Some("NativePRNG"), &budget).is_err());
    assert_eq!(budget.used_bytes(), 0);
    let registration = (Arc::as_ptr(&budget) as usize, Algorithm::Native);
    assert!(!NATIVE_SOURCES.lock().unwrap().contains_key(&registration));
}

#[cfg(unix)]
#[test]
fn native_source_cleanup_preserves_a_replacement_registration() {
    let budget = MemoryBudget::new(1 << 24);
    let original = SharedNative::new(Algorithm::Native, &budget).unwrap();
    let registration = original.registration;
    let replacement = Arc::new(SharedNative {
        random: Mutex::new(Native::new(Algorithm::Native, budget.reserve(NATIVE_ALLOCATION_BOUND).unwrap()).unwrap()),
        registration,
    });
    NATIVE_SOURCES.lock().unwrap().insert(registration, Arc::downgrade(&replacement));
    drop(original);
    assert_eq!(budget.used_bytes(), NATIVE_ALLOCATION_BOUND);
    let discovered = SharedNative::new(Algorithm::Native, &budget).unwrap();
    assert!(Arc::ptr_eq(&replacement, &discovered));
    drop(discovered);
    drop(replacement);
    assert_eq!(budget.used_bytes(), 0);
    assert!(!NATIVE_SOURCES.lock().unwrap().contains_key(&registration));
}

#[cfg(unix)]
#[test]
fn concurrent_native_source_recreation_releases_every_registration_and_charge() {
    let budget = MemoryBudget::new(1 << 24);
    let barrier = std::sync::Barrier::new(4);
    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                barrier.wait();
                for _ in 0..32 {
                    let source = SharedNative::new(Algorithm::NonBlocking, &budget).unwrap();
                    drop(source);
                }
            });
        }
    });
    assert_eq!(budget.used_bytes(), 0);
    let registration = (Arc::as_ptr(&budget) as usize, Algorithm::NonBlocking);
    assert!(!NATIVE_SOURCES.lock().unwrap().contains_key(&registration));
}
