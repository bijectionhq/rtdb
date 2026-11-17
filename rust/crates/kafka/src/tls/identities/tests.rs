use super::*;
use openssl::ssl::{SslContextBuilder, SslMethod};
use rtdb_columnar::MemoryBudget;
use crate::tls::{keys::Status, tests::support::{self, Credential}};

fn material(bytes: &[u8], password: Option<&str>, algorithm: Algorithm, memory: &Arc<MemoryBudget>) -> Material {
    let charge = memory.reserve(Material::allocation_bound(bytes.len(), StoreFormat::Java, StoreUse::OptionalIdentity, algorithm, Some("store"), password).unwrap()).unwrap();
    Material::decode(bytes, StoreFormat::Java, StoreUse::OptionalIdentity, algorithm, Some("store"), password, charge).unwrap()
}

#[test]
fn deferred_keys_preserve_selected_alias_failure_without_trying_another_entry() {
    let first = Credential::new("first", false, None);
    let second = Credential::new("second", false, None);
    let bytes = support::jks_with_passwords(&[("a", &first, "key"), ("b", &second, "other")], "store");
    let memory = MemoryBudget::new(32 << 20);
    let material = material(&bytes, Some("key"), Algorithm::NewSunX509, &memory);
    let charge = memory.reserve(65_536).unwrap();
    let random = random::Context::new(None, &memory).unwrap();
    let signatures = random::Context::new(None, &memory).unwrap();
    let (manager, identities, native_charge) = material.native(Algorithm::NewSunX509, &random, &charge).unwrap();
    let selected = manager.choose(&[KeyType::Rsa], |_, _| true, |_| Status::Valid).unwrap();
    assert_eq!(identities[selected].certificate.to_der().unwrap(), second.certificate.to_der().unwrap());
    let context = SslContextBuilder::new(SslMethod::tls_client()).unwrap().build();
    let mut ssl = Ssl::new(&context).unwrap();
    let before = memory.used_bytes();
    assert!(!identities[selected].install(&mut ssl, &random, &signatures).unwrap());
    assert!(ssl.certificate().is_none());
    assert!(ssl.ex_data(selected_index().unwrap()).is_none());
    assert_eq!(memory.used_bytes(), before);
    let selected = manager.choose(&[KeyType::Rsa], |index, _| identities[index].certificate.to_der().unwrap() == first.certificate.to_der().unwrap(), |_| Status::Valid).unwrap();
    assert!(identities[selected].install(&mut ssl, &random, &signatures).unwrap());
    assert_eq!(ssl.certificate().unwrap().to_der().unwrap(), first.certificate.to_der().unwrap());
    assert_eq!(ssl.ex_data(selected_index().unwrap()).unwrap().len(), 1);
    drop(ssl); drop(context); drop(identities); drop(material); drop(native_charge); drop(charge); drop(random); drop(signatures);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn recovered_ssl_key_lease_retains_contexts_and_charge_through_native_ssl_cleanup() {
    let identity = Credential::new("client", false, None);
    let bytes = support::jks(&[("client", &identity)], "store", "store");
    let memory = MemoryBudget::new(32 << 20);
    let material = material(&bytes, None, Algorithm::NewSunX509, &memory);
    let charge = memory.reserve(65_536).unwrap();
    let random = random::Context::new(None, &memory).unwrap();
    let signatures = random::Context::new(None, &memory).unwrap();
    let weak_random = Arc::downgrade(&random); let weak_signatures = Arc::downgrade(&signatures);
    let (_, identities, native_charge) = material.native(Algorithm::NewSunX509, &random, &charge).unwrap();
    let context = SslContextBuilder::new(SslMethod::tls_client()).unwrap().build();
    let mut ssl = Ssl::new(&context).unwrap();
    assert!(identities[0].install(&mut ssl, &random, &signatures).unwrap());
    assert!(identities[0].install(&mut ssl, &random, &signatures).unwrap());
    assert_eq!(ssl.ex_data(selected_index().unwrap()).unwrap().len(), 2);
    drop(identities); drop(material); drop(charge); drop(random); drop(signatures);
    assert!(weak_random.upgrade().is_some()); assert!(weak_signatures.upgrade().is_some());
    assert!(ssl.private_key().unwrap().public_eq(&identity.key));
    assert!(memory.used_bytes() > 0);
    drop(ssl); drop(context); drop(native_charge);
    assert!(weak_random.upgrade().is_none()); assert!(weak_signatures.upgrade().is_none());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn selected_key_memory_failure_is_not_an_absent_java_key_entry() {
    let identity = Credential::new("client", false, None);
    let bytes = support::jks(&[("client", &identity)], "store", "key");
    let memory = MemoryBudget::new(32 << 20);
    let material = material(&bytes, Some("key"), Algorithm::NewSunX509, &memory);
    let charge = memory.reserve(65_536).unwrap();
    let random = random::Context::new(None, &memory).unwrap();
    let signatures = random::Context::new(None, &memory).unwrap();
    let (_, identities, native_charge) = material.native(Algorithm::NewSunX509, &random, &charge).unwrap();
    let context = SslContextBuilder::new(SslMethod::tls_client()).unwrap().build();
    let mut ssl = Ssl::new(&context).unwrap();
    let remaining = memory.reserve((32 << 20) - memory.used_bytes()).unwrap();
    assert_eq!(identities[0].install(&mut ssl, &random, &signatures).unwrap_err().code, "rtdb/memory-budget-exceeded");
    assert!(ssl.certificate().is_none());
    drop(remaining);
    assert!(identities[0].install(&mut ssl, &random, &signatures).unwrap());
    drop(ssl); drop(context); drop(identities); drop(material); drop(native_charge); drop(charge); drop(random); drop(signatures);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn java_new_sunx509_defers_password_errors_but_pem_and_sunx509_remain_eager() {
    let identity = Credential::new("client", false, None);
    let bytes = support::jks(&[("client", &identity)], "store", "key");
    let pkcs12 = openssl::pkcs12::Pkcs12::builder().name("client").pkey(&identity.key).cert(&identity.certificate).build2("store").unwrap().to_der().unwrap();
    let mut pem = identity.key.private_key_to_pem_pkcs8_passphrase(openssl::symm::Cipher::aes_256_cbc(), b"key").unwrap();
    pem.extend(identity.certificate.to_pem().unwrap());
    let memory = MemoryBudget::new(32 << 20);
    for algorithm in [Algorithm::SunX509, Algorithm::NewSunX509] {
        for bytes in [&bytes, &pkcs12] {
            let charge = memory.reserve(1 << 20).unwrap();
            let loaded = Material::decode(bytes, StoreFormat::Java, StoreUse::OptionalIdentity, algorithm, Some("store"), Some("wrong"), charge);
            assert_eq!(loaded.is_ok(), algorithm == Algorithm::NewSunX509);
            drop(loaded);
        }
        let charge = memory.reserve(1 << 20).unwrap();
        assert!(Material::decode(&pem, StoreFormat::Pem, StoreUse::OptionalIdentity, algorithm, None, Some("wrong"), charge).is_err());
    }
    assert_eq!(memory.used_bytes(), 0);
}
