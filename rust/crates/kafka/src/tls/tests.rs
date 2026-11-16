use super::*;
use rtdb_columnar::MemoryBudget;

fn options() -> BTreeMap<String, String> { BTreeMap::from([("security.protocol".into(), "SSL".into())]) }

#[test]
fn java_tls_material_is_admitted_before_opening_files_and_ignores_plaintext_paths() {
    let memory = MemoryBudget::new(0); let mut options = options();
    options.insert("ssl.keystore.location".into(), "/missing/keystore".into());
    assert_eq!(NativeTls::from_java(&options, &mut |bytes| memory.reserve(bytes)).err().unwrap().code, "rtdb/memory-budget-exceeded");
    options.insert("security.protocol".into(), "PLAINTEXT".into());
    assert!(NativeTls::from_java(&options, &mut |bytes| memory.reserve(bytes)).unwrap().is_none());
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn jks_distinct_store_and_key_passwords_keep_material_charged_until_the_last_owner() {
    let memory = MemoryBudget::new(1 << 25); let mut options = options();
    options.insert("ssl.keystore.location".into(), format!("{}/../tls/tests/fixtures/distinct-passwords.jks", env!("CARGO_MANIFEST_DIR")));
    options.insert("ssl.keystore.password".into(), "store_password".into()); options.insert("ssl.key.password".into(), "private_password".into());
    let material = NativeTls::from_java(&options, &mut |bytes| memory.reserve(bytes)).unwrap().unwrap();
    assert_eq!(material.identities.len(), 1); assert!(memory.used_bytes() > 0);
    let copy = Arc::clone(&material); drop(material); assert!(memory.used_bytes() > 0); drop(copy); assert_eq!(memory.used_bytes(), 0);
    options.insert("ssl.key.password".into(), "wrong".into());
    assert!(NativeTls::from_java(&options, &mut |bytes| memory.reserve(bytes)).is_err()); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn every_java_cipher_name_is_validated_even_after_a_valid_entry() {
    let memory = MemoryBudget::new(1 << 25);
    for ciphers in ["TLS_AES_128_GCM_SHA256,TLS_NOT_A_CIPHER", "ECDHE-RSA-AES128-GCM-SHA256", "DEFAULT", "TLS_AES_128_GCM_SHA256,"] {
        let mut options = options(); options.insert("ssl.cipher.suites".into(), ciphers.into());
        assert!(NativeTls::from_java(&options, &mut |bytes| memory.reserve(bytes)).is_err()); assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn empty_java_client_stores_survive_native_client_creation_and_release_on_close() {
    let memory = MemoryBudget::new(1 << 25);
    let mut options = options();
    let path = format!("{}/../tls/tests/fixtures/empty.jks", env!("CARGO_MANIFEST_DIR"));
    for store in ["keystore", "truststore"] {
        options.insert(format!("ssl.{store}.location"), path.clone());
        options.insert(format!("ssl.{store}.password"), "12345678".into());
    }
    let material = NativeTls::from_java(&options, &mut |bytes| memory.reserve(bytes)).unwrap().unwrap();
    assert!(material.identities.is_empty());
    assert!(material.trust.as_ref().is_some_and(Vec::is_empty));
    let mut configuration = rdkafka::ClientConfig::new();
    configuration.set("security.protocol", "SSL").set_ssl_context_configurer(material);
    let producer: rdkafka::producer::BaseProducer = configuration.create().unwrap();
    drop(configuration);
    assert!(memory.used_bytes() > 0);
    drop(producer);
    assert_eq!(memory.used_bytes(), 0);
}

