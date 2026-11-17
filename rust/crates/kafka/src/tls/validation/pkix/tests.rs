use super::*;
use openssl::{asn1::{Asn1Object, Asn1OctetString}, bn::BigNum, hash::MessageDigest, pkey::{PKey, Private}, rsa::Rsa, x509::{X509Extension, X509NameBuilder, store::X509StoreBuilder, extension::{BasicConstraints, ExtendedKeyUsage, KeyUsage}}};
use rtdb_columnar::MemoryBudget;
mod policy_integration;

const VALID: (i64, i64) = (1_600_000_000, 1_800_000_000);
fn now() -> Asn1Time { Asn1Time::from_unix(1_700_000_000).unwrap() }
fn key(bits: u32) -> PKey<Private> { PKey::from_rsa(Rsa::generate(bits).unwrap()).unwrap() }
fn certificate(name: &str, key: &PKey<Private>, issuer: Option<(&X509, &PKey<Private>)>, ca: bool, validity: (i64, i64), digest: MessageDigest, extra: Vec<X509Extension>) -> X509 {
    let mut subject = X509NameBuilder::new().unwrap(); subject.append_entry_by_text("CN", name).unwrap(); let subject = subject.build();
    let mut certificate = X509::builder().unwrap(); certificate.set_version(2).unwrap();
    certificate.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap()).unwrap();
    certificate.set_subject_name(&subject).unwrap(); certificate.set_issuer_name(issuer.map_or(subject.as_ref(), |(issuer, _)| issuer.subject_name())).unwrap();
    certificate.set_pubkey(key).unwrap();
    certificate.set_not_before(&Asn1Time::from_unix(validity.0).unwrap()).unwrap(); certificate.set_not_after(&Asn1Time::from_unix(validity.1).unwrap()).unwrap();
    if ca {
        certificate.append_extension(BasicConstraints::new().critical().ca().build().unwrap()).unwrap();
        certificate.append_extension(KeyUsage::new().key_cert_sign().build().unwrap()).unwrap();
    } else { certificate.append_extension(KeyUsage::new().digital_signature().build().unwrap()).unwrap(); }
    for extension in extra { certificate.append_extension(extension).unwrap(); }
    certificate.sign(issuer.map_or(key, |(_, key)| key), digest).unwrap(); certificate.build()
}
fn store(anchors: &[X509]) -> openssl::x509::store::X509Store {
    let mut store = X509StoreBuilder::new().unwrap();
    for anchor in anchors { store.add_cert(anchor.clone()).unwrap(); }
    store.build()
}

#[test]
fn pkix_builds_unordered_chains_and_retains_selected_path_charge() {
    let root_key = key(1024); let root = certificate("root", &root_key, None, true, VALID, MessageDigest::sha256(), vec![]);
    let issuer_key = key(1024); let issuer = certificate("issuer", &issuer_key, Some((&root, &root_key)), true, VALID, MessageDigest::sha256(), vec![]);
    let leaf = certificate("server", &key(1024), Some((&issuer, &issuer_key)), false, VALID, MessageDigest::sha256(), vec![]);
    let store = store(&[root.clone()]);
    let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1024).unwrap();
    let verified = verify_store(&store, &[leaf, root.clone(), issuer], &now(), &charge).unwrap();
    assert_eq!(verified.certificates.len(), 3);
    assert_eq!(verified.certificates[2].to_der().unwrap(), root.to_der().unwrap());
    assert!(memory.used_bytes() > 1024);
    drop(verified); assert_eq!(memory.used_bytes(), 1024);
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn pkix_checks_nonanchor_dates_but_accepts_expired_anchor_and_exact_trusted_leaf() {
    let root_key = key(1024); let root = certificate("root", &root_key, None, true, (1_400_000_000, 1_500_000_000), MessageDigest::sha256(), vec![]);
    let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1024).unwrap();
    for (validity, valid) in [(VALID, true), ((1_400_000_000, 1_500_000_000), false), ((1_750_000_000, 1_800_000_000), false)] {
        let leaf = certificate("server", &key(1024), Some((&root, &root_key)), false, validity, MessageDigest::sha256(), vec![]);
        assert_eq!(verify_store(&store(&[root.clone()]), &[leaf], &now(), &charge).is_ok(), valid);
    }
    let leaf = certificate("direct trust", &key(512), None, false, (1_400_000_000, 1_500_000_000), MessageDigest::md5(), vec![ExtendedKeyUsage::new().client_auth().build().unwrap()]);
    let verified = verify_store(&store(&[leaf.clone()]), &[leaf.clone()], &now(), &charge).unwrap();
    assert_eq!(verified.certificates.len(), 1);
    assert_eq!(verified.certificates[0].to_der().unwrap(), leaf.to_der().unwrap());
    drop(verified); drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn pkix_applies_java_chain_policy_and_preserves_private_ca_sha1() {
    let root_key = key(1024); let root = certificate("root", &root_key, None, true, VALID, MessageDigest::sha256(), vec![]);
    let store = store(&[root.clone()]);
    let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1024).unwrap();
    for (bits, digest, valid) in [(1024, MessageDigest::sha1(), true), (1024, MessageDigest::sha256(), true), (512, MessageDigest::sha256(), false), (1024, MessageDigest::md5(), false)] {
        let leaf = certificate("server", &key(bits), Some((&root, &root_key)), false, VALID, digest, vec![]);
        assert_eq!(verify_store(&store, &[leaf], &now(), &charge).is_ok(), valid);
    }
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn pkix_ignores_intermediate_eku_and_consumes_critical_certificate_policies() {
    let root_key = key(1024); let root = certificate("root", &root_key, None, true, VALID, MessageDigest::sha256(), vec![]);
    let issuer_key = key(1024);
    let issuer = certificate("issuer", &issuer_key, Some((&root, &root_key)), true, VALID, MessageDigest::sha256(), vec![ExtendedKeyUsage::new().critical().client_auth().build().unwrap()]);
    let policies = X509Extension::new_from_der(&Asn1Object::from_str("2.5.29.32").unwrap(), true, &Asn1OctetString::new_from_bytes(&[0x30, 8, 0x30, 6, 6, 4, 0x55, 0x1d, 0x20, 0]).unwrap()).unwrap();
    let leaf = certificate("server", &key(1024), Some((&issuer, &issuer_key)), false, VALID, MessageDigest::sha256(), vec![policies, ExtendedKeyUsage::new().server_auth().build().unwrap()]);
    let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1024).unwrap();
    assert!(verify_store(&store(&[root]), &[leaf, issuer], &now(), &charge).is_ok());
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn pkix_rejects_empty_trust_and_unadmitted_chain_allocations() {
    let root_key = key(1024); let root = certificate("root", &root_key, None, true, VALID, MessageDigest::sha256(), vec![]);
    let leaf = certificate("server", &key(1024), Some((&root, &root_key)), false, VALID, MessageDigest::sha256(), vec![]);
    let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1024).unwrap();
    assert!(verify_store(&store(&[]), &[leaf.clone()], &now(), &charge).is_err());
    let low = MemoryBudget::new(1024); let low_charge = low.reserve(1024).unwrap();
    assert!(verify_store(&store(&[root]), &[leaf], &now(), &low_charge).is_err());
    assert_eq!(low.used_bytes(), 1024);
    drop(low_charge); drop(charge); assert_eq!(low.used_bytes(), 0); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn pkix_path_search_prefers_a_valid_intermediate_to_an_expired_alternative() {
    let root_key = key(1024); let root = certificate("root", &root_key, None, true, VALID, MessageDigest::sha256(), vec![]);
    let issuer_key = key(1024);
    let expired = certificate("issuer", &issuer_key, Some((&root, &root_key)), true, (1_400_000_000, 1_500_000_000), MessageDigest::sha256(), vec![]);
    let issuer = certificate("issuer", &issuer_key, Some((&root, &root_key)), true, VALID, MessageDigest::sha256(), vec![]);
    let leaf = certificate("server", &key(1024), Some((&issuer, &issuer_key)), false, VALID, MessageDigest::sha256(), vec![]);
    let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1024).unwrap();
    let verified = verify_store(&store(&[root]), &[leaf, expired, issuer.clone()], &now(), &charge).unwrap();
    assert_eq!(verified.certificates[1].to_der().unwrap(), issuer.to_der().unwrap());
    drop(verified); drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn pkix_date_adapter_preserves_previous_callback_and_restores_borrowed_state() {
    unsafe extern "C" fn reject(_: i32, _: *mut openssl_sys::X509_STORE_CTX) -> i32 { 0 }
    let root_key = key(1024); let root = certificate("root", &root_key, None, true, VALID, MessageDigest::sha256(), vec![]);
    let leaf = certificate("server", &key(1024), Some((&root, &root_key)), false, VALID, MessageDigest::sha256(), vec![]);
    let mut context = X509StoreContext::new().unwrap();
    let untrusted = Stack::new().unwrap();
    let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1024).unwrap();
    context.init(&store(&[root]), &leaf, &untrusted, |context| {
        unsafe { X509_STORE_CTX_set_verify_cb(context.as_ptr(), Some(reject)); }
        let index = date_index().unwrap();
        assert!(unsafe { openssl_sys::X509_STORE_CTX_get_ex_data(context.as_ptr(), index) }.is_null());
        assert!(verify_context(context, &now(), &charge).is_err());
        assert!(unsafe { openssl_sys::X509_STORE_CTX_get_ex_data(context.as_ptr(), index) }.is_null());
        assert_eq!(unsafe { X509_STORE_CTX_get_verify_cb(context.as_ptr()) }.unwrap() as usize, reject as usize);
        Ok(())
    }).unwrap();
}

#[test]
fn pkix_anchor_extensions_do_not_constrain_the_certificate_path() {
    let root_key = key(1024);
    let unknown = || X509Extension::new_from_der(&Asn1Object::from_str("1.2.3.4.5").unwrap(), true, &Asn1OctetString::new_from_bytes(&[5, 0]).unwrap()).unwrap();
    let constraints = || X509Extension::new_from_der(&Asn1Object::from_str("2.5.29.30").unwrap(), true, &Asn1OctetString::new_from_bytes(&[0x30, 0x0f, 0xa0, 0x0d, 0x30, 0x0b, 0x82, 9, b'.', b'e', b'x', b'c', b'l', b'u', b'd', b'e', b'd']).unwrap()).unwrap();
    let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1024).unwrap();
    let mut roots = Vec::new();
    for extra in [unknown(), constraints()] {
        roots.push(certificate("root", &root_key, None, true, VALID, MessageDigest::sha256(), vec![extra]));
    }
    let mut name = X509NameBuilder::new().unwrap(); name.append_entry_by_text("CN", "root").unwrap(); let name = name.build();
    let mut limited = X509::builder().unwrap(); limited.set_version(2).unwrap(); limited.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap()).unwrap();
    limited.set_subject_name(&name).unwrap(); limited.set_issuer_name(&name).unwrap(); limited.set_pubkey(&root_key).unwrap();
    limited.set_not_before(&Asn1Time::from_unix(VALID.0).unwrap()).unwrap(); limited.set_not_after(&Asn1Time::from_unix(VALID.1).unwrap()).unwrap();
    limited.append_extension(BasicConstraints::new().critical().ca().pathlen(0).build().unwrap()).unwrap(); limited.append_extension(KeyUsage::new().key_cert_sign().build().unwrap()).unwrap();
    limited.sign(&root_key, MessageDigest::sha256()).unwrap(); roots.push(limited.build());
    for root in roots {
        let issuer_key = key(1024); let issuer = certificate("issuer", &issuer_key, Some((&root, &root_key)), true, VALID, MessageDigest::sha256(), vec![]);
        let mut san = vec![0x30, 17, 0x82, 15]; san.extend_from_slice(b"outside.example");
        let san = X509Extension::new_from_der(&Asn1Object::from_str("2.5.29.17").unwrap(), false, &Asn1OctetString::new_from_bytes(&san).unwrap()).unwrap();
        let leaf = certificate("server", &key(1024), Some((&issuer, &issuer_key)), false, VALID, MessageDigest::sha256(), vec![san]);
        let verified = verify_store(&store(&[root.clone()]), &[leaf, issuer], &now(), &charge).unwrap();
        assert_eq!(verified.certificates.last().unwrap().to_der().unwrap(), root.to_der().unwrap());
    }
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn pkix_accepts_nonselfissued_v1_and_v2_trust_anchors() {
    let signer = key(1024); let parent = certificate("untrusted root issuer", &signer, None, true, VALID, MessageDigest::sha256(), vec![]);
    let root_key = key(1024);
    let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1024).unwrap();
    for version in [0, 1] {
        let mut name = X509NameBuilder::new().unwrap(); name.append_entry_by_text("CN", "root").unwrap(); let name = name.build();
        let mut root = X509::builder().unwrap(); root.set_version(version).unwrap(); root.set_serial_number(&BigNum::from_u32(7).unwrap().to_asn1_integer().unwrap()).unwrap();
        root.set_subject_name(&name).unwrap(); root.set_issuer_name(parent.subject_name()).unwrap(); root.set_pubkey(&root_key).unwrap();
        root.set_not_before(&Asn1Time::from_unix(VALID.0).unwrap()).unwrap(); root.set_not_after(&Asn1Time::from_unix(VALID.1).unwrap()).unwrap();
        root.sign(&signer, MessageDigest::sha256()).unwrap(); let root = root.build();
        let leaf = certificate("server", &key(1024), Some((&root, &root_key)), false, VALID, MessageDigest::sha256(), vec![]);
        let verified = verify_store(&store(&[root.clone()]), &[leaf], &now(), &charge).unwrap();
        assert_eq!(verified.certificates[1].to_der().unwrap(), root.to_der().unwrap());
    }
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn pkix_builder_retries_policy_rejected_issuers_but_ordered_validation_does_not() {
    let root_key = key(1024); let root = certificate("root", &root_key, None, true, VALID, MessageDigest::sha256(), vec![]);
    let issuer_key = key(1024);
    let disabled = certificate("issuer", &issuer_key, Some((&root, &root_key)), true, VALID, MessageDigest::md5(), vec![]);
    let issuer = certificate("issuer", &issuer_key, Some((&root, &root_key)), true, VALID, MessageDigest::sha256(), vec![]);
    let leaf = certificate("server", &key(1024), Some((&issuer, &issuer_key)), false, VALID, MessageDigest::sha256(), vec![]);
    let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1024).unwrap();
    let store = store(&[root.clone()]);
    let verified = verify_store(&store, &[leaf.clone(), disabled.clone(), issuer.clone()], &now(), &charge).unwrap();
    assert_eq!(verified.certificates[1].to_der().unwrap(), issuer.to_der().unwrap());
    assert!(verify_store(&store, &[leaf, disabled, root, issuer], &now(), &charge).is_err());
    drop(verified); drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn pkix_builder_default_depth_limit_does_not_limit_ordered_validation() {
    let key = key(1024); let root = certificate("root", &key, None, true, VALID, MessageDigest::sha256(), vec![]);
    let mut issuers = Vec::new();
    for index in 0..6 {
        let parent = issuers.last().unwrap_or(&root);
        issuers.push(certificate(&format!("issuer {index}"), &key, Some((parent, &key)), true, VALID, MessageDigest::sha256(), vec![]));
    }
    let leaf = certificate("server", &key, Some((issuers.last().unwrap(), &key)), false, VALID, MessageDigest::sha256(), vec![]);
    issuers.reverse();
    let mut ordered = vec![leaf]; ordered.extend(issuers);
    let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1024).unwrap();
    assert!(verify_store(&store(&[root.clone()]), &ordered, &now(), &charge).is_ok());
    let issuer = ordered.remove(1); ordered.push(issuer);
    assert!(verify_store(&store(&[root]), &ordered, &now(), &charge).is_err());
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn pkix_candidate_admission_errors_survive_path_retry() {
    let root_key = key(1024); let root = certificate("root", &root_key, None, true, VALID, MessageDigest::sha256(), vec![]);
    let leaf = certificate("server", &key(1024), Some((&root, &root_key)), false, VALID, MessageDigest::sha256(), vec![]);
    let memory = MemoryBudget::new(1024 + 65_536 + leaf.to_der().unwrap().len() * 32 + 512);
    let charge = memory.reserve(1024).unwrap();
    let error = verify_store(&store(&[root]), &[leaf], &now(), &charge).err().unwrap();
    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), 1024);
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn pkix_native_verification_rejects_critical_qualifiers_only_while_policy_tree_is_live() {
    let root_key = key(1024); let root = certificate("root", &root_key, None, true, VALID, MessageDigest::sha256(), vec![]);
    let qualified = || X509Extension::new_from_der(&Asn1Object::from_str("2.5.29.32").unwrap(), true, &Asn1OctetString::new_from_bytes(&[0x30, 24, 0x30, 22, 6, 3, 0x2a, 3, 4, 0x30, 15, 0x30, 13, 6, 8, 0x2b, 6, 1, 5, 5, 7, 2, 1, 0x16, 1, b'x']).unwrap()).unwrap();
    let leaf = certificate("server", &key(1024), Some((&root, &root_key)), false, VALID, MessageDigest::sha256(), vec![qualified()]);
    let memory = MemoryBudget::new(1 << 24); let charge = memory.reserve(1024).unwrap();
    assert!(verify_store(&store(&[root.clone()]), &[leaf], &now(), &charge).is_err());
    let issuer_key = key(1024); let issuer = certificate("issuer", &issuer_key, Some((&root, &root_key)), true, VALID, MessageDigest::sha256(), vec![]);
    let leaf = certificate("server", &key(1024), Some((&issuer, &issuer_key)), false, VALID, MessageDigest::sha256(), vec![qualified()]);
    assert!(verify_store(&store(&[root]), &[leaf, issuer], &now(), &charge).is_ok());
    drop(charge); assert_eq!(memory.used_bytes(), 0);
}
