use super::*;
use rtdb_tls::KeyStore;
use openssl::ssl::Ssl;
use support::Credential;

#[test]
fn mismatched_sun_jks_credentials_survive_producer_and_registry_native_construction() {
    let leaf = Credential::new("client leaf", false, None);
    for ec_key in [false, true] {
        let other = Credential::new("other key", ec_key, None);
        let mismatch = Credential { key: other.key, certificate: leaf.certificate.clone(), chain: vec![] };
        let bytes = support::jks(&[("client", &mismatch)], "store", "key");
        let directory = tempfile::tempdir().unwrap(); let file = directory.path().join("client.jks");
        std::fs::write(&file, &bytes).unwrap();
        let empty_bytes = include_bytes!("../../../../tls/tests/fixtures/empty.jks");
        let trust_file = directory.path().join("trust.jks"); std::fs::write(&trust_file, empty_bytes).unwrap();
        let mut options = options();
        options.insert("ssl.keystore.location".into(), file.to_str().unwrap().into());
        options.insert("ssl.keystore.password".into(), "store".into()); options.insert("ssl.key.password".into(), "key".into());
        options.insert("ssl.truststore.location".into(), trust_file.to_str().unwrap().into()); options.insert("ssl.truststore.password".into(), "12345678".into());
        let memory = MemoryBudget::new(32 << 20);
        let producer_owner = NativeTls::from_java(&options, &mut |bytes| memory.reserve(bytes)).unwrap().unwrap();
        let identity = &producer_owner.identities.as_ref().unwrap().entries()[0];
        assert_eq!(identity.certificate.to_der().unwrap(), leaf.certificate.to_der().unwrap());
        assert_eq!(identity.kind, KeyType::Rsa); assert!(identity.eligible);
        let mut configuration = rdkafka::ClientConfig::new();
        configuration.set("security.protocol", "SSL").set_ssl_context_configurer(producer_owner.clone());
        let producer: rdkafka::producer::BaseProducer = configuration.create().unwrap();
        drop(configuration); drop(producer); drop(producer_owner);
        assert_eq!(memory.used_bytes(), 0);

        let charge = memory.reserve(Material::allocation_bound(bytes.len(), StoreFormat::Java, StoreUse::OptionalIdentity, Algorithm::SunX509, Some("store"), Some("key")).unwrap()).unwrap();
        let material = Material::decode(&bytes, StoreFormat::Java, StoreUse::OptionalIdentity, Algorithm::SunX509, Some("store"), Some("key"), Arc::clone(&charge)).unwrap();
        let trust = KeyStore::decode(empty_bytes, StoreFormat::Java, StoreUse::Trust, Some("12345678"), None, Arc::clone(&charge)).unwrap();
        let registry_owner = NativeTls::from_registry(&BTreeMap::new(), Some(&trust), Some(&material), Arc::clone(&charge)).unwrap();
        let context = unsafe { registry_owner.registry_context().unwrap() };
        let ssl = Ssl::new(&context).unwrap();
        assert!(ssl.certificate().is_none());
        assert_eq!(registry_owner.identities.as_ref().unwrap().entries()[0].kind, KeyType::Rsa);
        drop(ssl); drop(context); drop(registry_owner); drop(trust); drop(material); drop(charge);
        assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn mismatched_sun_private_values_fail_when_the_peer_requests_client_authentication() {
    use std::{net::TcpStream, time::Duration};
    use openssl::ssl::{SslStream, SslVersion};
    let root = Credential::new("issuer", false, None);
    let server = Credential::new("localhost", false, Some(&root));
    let leaf = Credential::new("client", false, Some(&root));
    let other = Credential::new("other private key", false, None);
    let mismatch = Credential { key: other.key, certificate: leaf.certificate, chain: leaf.chain };
    let bytes = support::jks(&[("client", &mismatch)], "store", "key");
    let memory = MemoryBudget::new(32 << 20); let charge = memory.reserve(1 << 20).unwrap();
    let trust = KeyStore::decode(&root.certificate.to_pem().unwrap(), StoreFormat::Pem, StoreUse::Trust, None, None, Arc::clone(&charge)).unwrap();
    let material = Material::decode(&bytes, StoreFormat::Java, StoreUse::OptionalIdentity, Algorithm::SunX509, Some("store"), Some("key"), Arc::clone(&charge)).unwrap();
    for version in [SslVersion::TLS1_2, SslVersion::TLS1_3] {
        let (address, broker) = support::broker(&server, &root, version, Some("rsa_pss_rsae_sha256:rsa_pkcs1_sha256"));
        let owner = NativeTls::from_registry(&BTreeMap::new(), Some(&trust), Some(&material), Arc::clone(&charge)).unwrap();
        let context = unsafe { owner.registry_context().unwrap() };
        let socket = TcpStream::connect(address).unwrap(); socket.set_read_timeout(Some(Duration::from_secs(3))).unwrap(); socket.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
        let mut stream = SslStream::new(Ssl::new(&context).unwrap(), socket).unwrap();
        assert!(stream.connect().is_err()); assert!(broker.join().unwrap().is_err());
        drop(stream); drop(context); drop(owner);
    }
    drop(material); drop(trust); drop(charge); assert_eq!(memory.used_bytes(), 0);
}
