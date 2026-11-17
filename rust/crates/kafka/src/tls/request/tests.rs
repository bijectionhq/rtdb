use super::*;
use openssl::ssl::SslMethod;
use rtdb_columnar::MemoryBudget;

fn message(payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![13, 0, (payload.len() >> 8) as u8, payload.len() as u8];
    bytes.extend_from_slice(payload);
    bytes
}

#[test]
fn tls12_keeps_certificate_type_and_peer_signature_preference_order() {
    let memory = MemoryBudget::new(1 << 20);
    let charge = memory.reserve(65_536).unwrap();
    let bytes = message(&[3, 64, 1, 255, 0, 8, 8, 7, 4, 1, 8, 4, 8, 7, 0, 0]);
    let request = Request::parse(&bytes, 0x0303, charge).unwrap();
    assert_eq!(request.signatures, [0x0807, 0x0401, 0x0804, 0x0807]);
    assert_eq!(request.certificate_types.as_deref(), Some(&[64, 1, 255][..]));
    assert!(request.certificate_signatures.is_none());
    drop(request);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn tls13_separates_certificate_preferences_and_retains_fallback_input() {
    let memory = MemoryBudget::new(1 << 20);
    let bytes = message(&[0, 0, 18, 0, 13, 0, 6, 0, 4, 8, 4, 8, 7, 0, 50, 0, 4, 0, 2, 8, 7]);
    let request = Request::parse(&bytes, 0x0304, memory.reserve(65_536).unwrap()).unwrap();
    assert_eq!(request.signatures, [0x0804, 0x0807]);
    assert_eq!(request.certificate_signatures.as_deref(), Some(&[0x0807][..]));
    assert!(request.certificate_types.is_none());
    let fallback = message(&[0, 0, 10, 0, 13, 0, 6, 0, 4, 4, 1, 8, 7]);
    let fallback = Request::parse(&fallback, 0x0304, memory.reserve(65_536).unwrap()).unwrap();
    assert_eq!(fallback.signatures, [0x0401, 0x0807]);
    assert!(fallback.certificate_signatures.is_none());
    drop(request); drop(fallback);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn request_types_follow_certificate_preference_and_require_a_handshake_signature() {
    assert_eq!(selection_types([0x0807, 0x0401], [0x0804, 0x0807], None), [KeyType::EdDsa, KeyType::Rsa]);
    assert_eq!(selection_types([0x0403, 0x0401], [0x0804], None), [KeyType::Rsa]);
    assert_eq!(selection_types([0x0807, 0x0809, 0x0401], [0x0804, 0x0807, 0x0809], Some(&[1])), [KeyType::RsaPss, KeyType::Rsa]);
    assert!(selection_types([0x0807], [0x0807], Some(&[])).is_empty());
    assert!(selection_types([0x0402, 0x0301], [0x0402, 0x0804], None).is_empty());
    assert_eq!(selection_types([0x0201], [0x0804], None), [KeyType::Rsa]);
}

#[test]
fn certificate_request_rejects_truncation_duplicate_and_empty_lists() {
    let memory = MemoryBudget::new(1 << 20);
    let valid = message(&[0, 0, 8, 0, 13, 0, 4, 0, 2, 8, 4]);
    for length in 0..valid.len() { assert!(Request::parse(&valid[..length], 0x0304, memory.reserve(65_536).unwrap()).is_err()); }
    for payload in [
        &[0, 0, 0][..],
        &[0, 0, 6, 0, 13, 0, 2, 0, 0],
        &[0, 0, 16, 0, 13, 0, 4, 0, 2, 8, 4, 0, 13, 0, 4, 0, 2, 8, 7],
        &[0, 0, 14, 0, 13, 0, 4, 0, 2, 8, 4, 0, 50, 0, 2, 0, 0],
    ] { assert!(Request::parse(&message(payload), 0x0304, memory.reserve(65_536).unwrap()).is_err()); }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn captured_request_is_charged_until_connection_release_and_budget_failure_rejects() {
    for limit in [1024, 1 << 20] {
        let memory = MemoryBudget::new(limit);
        let charge = memory.reserve(128).unwrap();
        let mut context = SslContextBuilder::new(SslMethod::tls_client()).unwrap();
        unsafe { configure(&mut context, &charge).unwrap(); }
        let context = context.build();
        let ssl = Ssl::new(&context).unwrap();
        let bytes = message(&[0, 0, 8, 0, 13, 0, 4, 0, 2, 8, 4]);
        unsafe { capture(0, 0x0304, 22, bytes.as_ptr().cast(), bytes.len(), ssl.as_ptr(), Arc::as_ptr(&charge).cast_mut().cast()); }
        assert_eq!(matches!(ssl.ex_data(index().unwrap()), Some(Captured::Ready(_))), limit > 1024);
        assert_eq!(memory.used_bytes() > 128, limit > 1024);
        drop(ssl);
        assert_eq!(memory.used_bytes(), 128);
        drop(context); drop(charge);
        assert_eq!(memory.used_bytes(), 0);
    }
}
