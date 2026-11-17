use super::*;
use rtdb_columnar::MemoryBudget;
#[path = "../../tests/support/kafka_tls.rs"]
pub(super) mod support;

fn options() -> BTreeMap<String, String> { BTreeMap::from([("security.protocol".into(), "SSL".into())]) }

#[test]
fn producer_sunjsse_provider_uses_java_string_trim_and_exact_provider_case() {
    let memory = MemoryBudget::new(1 << 25);
    for provider in ["SunJSSE", "\u{1} SunJSSE \u{20}"] {
        let mut options = options(); options.insert("ssl.provider".into(), provider.into());
        let material = NativeTls::from_java(&options, &mut |bytes| memory.reserve(bytes)).unwrap().unwrap();
        assert_eq!(material.trust.as_ref().unwrap().len(), 146);
        drop(material); assert_eq!(memory.used_bytes(), 0);
    }
    for provider in ["", "sunjsse", "\u{a0}SunJSSE\u{a0}", "SUN", "custom.Provider"] {
        let mut options = options(); options.insert("ssl.provider".into(), provider.into());
        assert!(NativeTls::from_java(&options, &mut |bytes| memory.reserve(bytes)).is_err());
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn new_sunx509_rejects_weak_keys_in_the_complete_client_chain() {
    use openssl::{bn::BigNum, hash::MessageDigest, rsa::Rsa, x509::{X509NameBuilder, extension::{BasicConstraints, KeyUsage}}};
    let key = PKey::from_rsa(Rsa::generate(512).unwrap()).unwrap();
    let mut name = X509NameBuilder::new().unwrap(); name.append_entry_by_text("CN", "weak issuer").unwrap(); let name = name.build();
    let mut root = X509::builder().unwrap(); root.set_version(2).unwrap();
    root.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap()).unwrap();
    root.set_subject_name(&name).unwrap(); root.set_issuer_name(&name).unwrap(); root.set_pubkey(&key).unwrap();
    root.set_not_before(&Asn1Time::days_from_now(0).unwrap()).unwrap(); root.set_not_after(&Asn1Time::days_from_now(1).unwrap()).unwrap();
    root.append_extension(BasicConstraints::new().ca().build().unwrap()).unwrap(); root.append_extension(KeyUsage::new().key_cert_sign().build().unwrap()).unwrap();
    root.sign(&key, MessageDigest::sha256()).unwrap();
    let root = support::Credential { key, certificate: root.build(), chain: vec![] };
    let client = support::Credential::new("client", false, Some(&root));
    let directory = tempfile::tempdir().unwrap(); let file = directory.path().join("client.jks");
    std::fs::write(&file, support::jks(&[("client", &client)], "store", "key")).unwrap();
    for (algorithm, eligible) in [("SunX509", true), ("NewSunX509", false), ("PKIX", false)] {
        let mut options = options(); options.insert("ssl.keymanager.algorithm".into(), algorithm.into());
        options.insert("ssl.keystore.location".into(), file.to_str().unwrap().into());
        options.insert("ssl.keystore.password".into(), "store".into()); options.insert("ssl.key.password".into(), "key".into());
        options.insert("ssl.ca.pem".into(), "native-owned".into());
        let memory = MemoryBudget::new(1 << 24);
        let material = NativeTls::from_java(&options, &mut |bytes| memory.reserve(bytes)).unwrap().unwrap();
        assert_eq!(material.identities.len(), 1); assert_eq!(material.identities[0].eligible, eligible);
        drop(material); assert_eq!(memory.used_bytes(), 0);
    }
}

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

#[test]
fn java_default_roots_replace_host_trust_unless_native_roots_are_explicit() {
    let memory = MemoryBudget::new(32 << 20);
    let defaults = NativeTls::from_java(&options(), &mut |bytes| memory.reserve(bytes)).unwrap().unwrap();
    assert_eq!(defaults.trust.as_ref().unwrap().len(), 146);
    drop(defaults);
    assert_eq!(memory.used_bytes(), 0);
    for name in ["ssl.ca.location", "ssl.ca.pem"] {
        let mut options = options();
        options.insert(name.into(), "native-owned".into());
        let explicit = NativeTls::from_java(&options, &mut |bytes| memory.reserve(bytes)).unwrap().unwrap();
        assert!(explicit.trust.is_none());
        drop(explicit);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn native_builtin_managers_select_java_alias_order_and_verify_trust_for_both_protocols() {
    use std::net::TcpStream;
    use std::time::Duration;
    use openssl::ssl::{Ssl, SslMethod, SslStream, SslVerifyMode};
    let root = support::Credential::new("issuer", false, None);
    let server = support::Credential::new("localhost", false, Some(&root));
    let first = support::Credential::new("first client", false, Some(&root));
    let second = support::Credential::new("second client", false, Some(&root));
    let directory = tempfile::tempdir().unwrap(); let file = directory.path().join("clients.jks");
    std::fs::write(&file, support::jks(&[("a", &first), ("b", &second)], "store-password", "key-password")).unwrap();
    for version in [SslVersion::TLS1_2, SslVersion::TLS1_3] {
        for (algorithm, expected) in [("SunX509", &first), ("NewSunX509", &second), ("PKIX", &second)] {
            let (address, broker) = support::broker(&server, &root, version, Some("rsa_pss_rsae_sha256"));
            let mut options = options();
            options.insert("ssl.keymanager.algorithm".into(), algorithm.into());
            options.insert("ssl.trustmanager.algorithm".into(), if algorithm == "PKIX" { "SunX509" } else { "PKIX" }.into());
            options.insert("ssl.truststore.type".into(), "PEM".into());
            options.insert("ssl.truststore.certificates".into(), String::from_utf8(root.certificate.to_pem().unwrap()).unwrap());
            options.insert("ssl.keystore.location".into(), file.to_str().unwrap().into());
            options.insert("ssl.keystore.password".into(), "store-password".into());
            options.insert("ssl.key.password".into(), "key-password".into());
            let memory = MemoryBudget::new(1 << 25);
            let material = NativeTls::from_java(&options, &mut |bytes| memory.reserve(bytes)).unwrap().unwrap();
            let mut context = SslContextBuilder::new(SslMethod::tls_client()).unwrap();
            context.set_verify(SslVerifyMode::PEER);
            unsafe { material.configure_context(&mut context).unwrap(); }
            let context = context.build();
            let socket = TcpStream::connect(address).unwrap();
            socket.set_read_timeout(Some(Duration::from_secs(3))).unwrap(); socket.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
            let mut ssl = Ssl::new(&context).unwrap(); ssl.param_mut().set_host("localhost").unwrap();
            let mut stream = SslStream::new(ssl, socket).unwrap();
            stream.connect().unwrap();
            let observed = broker.join().unwrap().unwrap();
            assert_eq!(observed.certificate, expected.certificate.to_der().unwrap());
            assert_eq!(observed.protocol, if version == SslVersion::TLS1_2 { "TLSv1.2" } else { "TLSv1.3" });
            assert!(!observed.cipher.is_empty());
            drop(stream); drop(context); drop(material); assert_eq!(memory.used_bytes(), 0);
        }
    }
}

