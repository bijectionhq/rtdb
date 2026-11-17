use super::*;
use rtdb_tls::KeyStore;
use openssl::{ssl::Ssl, symm::Cipher};
use support::Credential;

fn der(tag: u8, bytes: &[u8]) -> Vec<u8> {
    let mut output = vec![tag];
    if bytes.len() < 128 { output.push(bytes.len() as u8); }
    else { let length = bytes.len().to_be_bytes(); let start = length.iter().position(|byte| *byte != 0).unwrap(); output.push(0x80 | (length.len() - start) as u8); output.extend_from_slice(&length[start..]); }
    output.extend_from_slice(bytes); output
}
fn seq(parts: &[Vec<u8>]) -> Vec<u8> { der(0x30, &parts.concat()) }
fn bag(kind: u8, value: &[u8], id: u8, alias: Option<&str>) -> Vec<u8> {
    let mut oid = b"\x2a\x86\x48\x86\xf7\x0d\x01\x0c\x0a\x01".to_vec(); oid.push(kind);
    let mut attributes = vec![seq(&[der(6, b"\x2a\x86\x48\x86\xf7\x0d\x01\x09\x15"), der(0x31, &der(4, &[id]))])];
    if let Some(alias) = alias { attributes.push(seq(&[der(6, b"\x2a\x86\x48\x86\xf7\x0d\x01\x09\x14"), der(0x31, &der(0x1e, &alias.encode_utf16().flat_map(u16::to_be_bytes).collect::<Vec<_>>()))])); }
    seq(&[der(6, &oid), der(0xa0, value), der(0x31, &attributes.concat())])
}
fn pfx(bags: &[Vec<u8>]) -> Vec<u8> {
    let section = |bytes: &[u8]| seq(&[der(6, b"\x2a\x86\x48\x86\xf7\x0d\x01\x07\x01"), der(0xa0, &der(4, bytes))]);
    seq(&[der(2, &[3]), section(&seq(&[section(&seq(bags))]))])
}
fn store(identity: &Credential, password: &str, include_chain: bool) -> Vec<u8> {
    let protected = identity.key.private_key_to_pkcs8_passphrase(Cipher::aes_256_cbc(), password.as_bytes()).unwrap();
    let mut bags = vec![bag(2, &protected, 1, Some("CLIENT"))];
    if include_chain {
        for (index, certificate) in std::iter::once(&identity.certificate).chain(identity.chain.iter()).enumerate() {
            let value = seq(&[der(6, b"\x2a\x86\x48\x86\xf7\x0d\x01\x09\x16\x01"), der(0xa0, &der(4, &certificate.to_der().unwrap()))]);
            bags.push(bag(3, &value, index as u8 + 1, None));
        }
    }
    pfx(&bags)
}
fn material(bytes: &[u8], password: Option<&str>, memory: &Arc<MemoryBudget>) -> Result<Material> {
    let charge = memory.reserve(Material::allocation_bound(bytes.len(), StoreFormat::Java, StoreUse::OptionalIdentity, Algorithm::SunX509, Some("store"), password)?)?;
    Material::decode(bytes, StoreFormat::Java, StoreUse::OptionalIdentity, Algorithm::SunX509, Some("store"), password, charge)
}

#[test]
fn sun_pkcs12_mismatched_credentials_survive_producer_and_registry_construction() {
    let leaf = Credential::new("client leaf", false, None);
    for ec_key in [false, true] {
        let other = Credential::new("other key", ec_key, None);
        let mismatch = Credential { key: other.key, certificate: leaf.certificate.clone(), chain: vec![] };
        let bytes = store(&mismatch, "key", true);
        let directory = tempfile::tempdir().unwrap(); let file = directory.path().join("client.p12"); std::fs::write(&file, &bytes).unwrap();
        let empty_bytes = include_bytes!("../../../../tls/tests/fixtures/empty.jks"); let trust_file = directory.path().join("trust.jks"); std::fs::write(&trust_file, empty_bytes).unwrap();
        let mut options = options(); options.insert("ssl.keystore.location".into(), file.to_str().unwrap().into()); options.insert("ssl.keystore.type".into(), "PKCS12".into());
        options.insert("ssl.keystore.password".into(), "store".into()); options.insert("ssl.key.password".into(), "key".into());
        options.insert("ssl.truststore.location".into(), trust_file.to_str().unwrap().into()); options.insert("ssl.truststore.password".into(), "12345678".into());
        let memory = MemoryBudget::new(32 << 20);
        let producer_owner = NativeTls::from_java(&options, &mut |bytes| memory.reserve(bytes)).unwrap().unwrap();
        assert_eq!(producer_owner.identities.as_ref().unwrap().entries()[0].certificate.to_der().unwrap(), leaf.certificate.to_der().unwrap());
        let mut configuration = rdkafka::ClientConfig::new(); configuration.set("security.protocol", "SSL").set_ssl_context_configurer(producer_owner.clone());
        let producer: rdkafka::producer::BaseProducer = configuration.create().unwrap(); drop(configuration); drop(producer); drop(producer_owner); assert_eq!(memory.used_bytes(), 0);

        let material = material(&bytes, Some("key"), &memory).unwrap(); assert!(matches!(material, Material::SunPkcs12(_)));
        let charge = memory.reserve(1 << 20).unwrap(); let trust = KeyStore::decode(empty_bytes, StoreFormat::Java, StoreUse::Trust, Some("12345678"), None, Arc::clone(&charge)).unwrap();
        let registry_owner = NativeTls::from_registry(&BTreeMap::new(), Some(&trust), Some(&material), Arc::clone(&charge)).unwrap();
        let context = unsafe { registry_owner.registry_context().unwrap() }; let ssl = Ssl::new(&context).unwrap(); assert!(ssl.certificate().is_none());
        assert_eq!(registry_owner.identities.as_ref().unwrap().entries()[0].kind, KeyType::Rsa);
        drop(ssl); drop(context); drop(registry_owner); drop(trust); drop(material); drop(charge); assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn sun_pkcs12_material_recovers_unchained_keys_and_secrets_before_filtering() {
    let identity = Credential::new("client", false, None); let bytes = store(&identity, "key", false); let memory = MemoryBudget::new(32 << 20);
    assert!(material(&bytes, Some("wrong"), &memory).is_err());
    let loaded = material(&bytes, Some("key"), &memory).unwrap(); let Material::SunPkcs12(store) = loaded else { panic!("expected Sun PKCS12"); };
    assert!(store.identities().is_empty()); drop(store);
    let secret = bag(5, &seq(&[der(6, &[0x2a, 3]), der(0xa0, &der(4, b"invalid protected secret"))]), 1, Some("secret"));
    let bytes = pfx(&[secret]); assert!(material(&bytes, Some("key"), &memory).is_err());
    let charge = memory.reserve(Material::allocation_bound(bytes.len(), StoreFormat::Java, StoreUse::OptionalIdentity, Algorithm::NewSunX509, None, None).unwrap()).unwrap();
    let deferred = Material::decode(&bytes, StoreFormat::Java, StoreUse::OptionalIdentity, Algorithm::NewSunX509, None, None, charge).unwrap();
    assert!(matches!(deferred, Material::Deferred(_))); drop(deferred); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sun_pkcs12_native_expansion_is_admitted_and_effective_password_falls_back() {
    let identity = Credential::new("client", false, None); let bytes = store(&identity, "store", true); let memory = MemoryBudget::new(32 << 20);
    let material = material(&bytes, None, &memory).unwrap(); let charge = memory.reserve(65_536).unwrap(); let random = random::Context::new(None, &memory).unwrap();
    assert!(material.native(Algorithm::NewSunX509, &random, &charge).is_err());
    let remaining = memory.reserve((32 << 20) - memory.used_bytes()).unwrap();
    assert_eq!(material.native(Algorithm::SunX509, &random, &charge).err().unwrap().code, "rtdb/memory-budget-exceeded"); drop(remaining);
    let native = material.native(Algorithm::SunX509, &random, &charge).unwrap(); let native_charge = native.reservation(); drop(material);
    assert_eq!(native.entries()[0].certificate.to_der().unwrap(), identity.certificate.to_der().unwrap());
    drop(native); drop(native_charge); drop(random); drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn sun_pkcs12_client_authentication_accepts_matching_keys_and_rejects_mismatched_values() {
    use std::{net::TcpStream, time::Duration};
    use openssl::ssl::{SslStream, SslVersion};
    let root = Credential::new("issuer", false, None); let server = Credential::new("localhost", false, Some(&root)); let client = Credential::new("client", false, Some(&root));
    let other = Credential::new("other key", false, None); let mismatch = Credential { key: other.key, certificate: client.certificate.clone(), chain: client.chain.clone() };
    let memory = MemoryBudget::new(32 << 20); let charge = memory.reserve(1 << 20).unwrap();
    let trust = KeyStore::decode(&root.certificate.to_pem().unwrap(), StoreFormat::Pem, StoreUse::Trust, None, None, Arc::clone(&charge)).unwrap();
    for (identity, succeeds) in [(&client, true), (&mismatch, false)] {
        let material = material(&store(identity, "key", true), Some("key"), &memory).unwrap();
        for version in [SslVersion::TLS1_2, SslVersion::TLS1_3] {
            let (address, broker) = support::broker(&server, &root, version, Some("rsa_pss_rsae_sha256:rsa_pkcs1_sha256"));
            let owner = NativeTls::from_registry(&BTreeMap::new(), Some(&trust), Some(&material), Arc::clone(&charge)).unwrap(); let context = unsafe { owner.registry_context().unwrap() };
            let socket = TcpStream::connect(address).unwrap(); socket.set_read_timeout(Some(Duration::from_secs(3))).unwrap(); socket.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
            let mut stream = SslStream::new(Ssl::new(&context).unwrap(), socket).unwrap();
            assert_eq!(stream.connect().is_ok(), succeeds); drop(stream);
            assert_eq!(broker.join().unwrap().is_ok(), succeeds); drop(context); drop(owner);
        }
    }
    drop(trust); drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn new_sun_pkcs12_secret_alias_positions_reach_native_selection_and_crls_stay_ignored() {
    let first = Credential::new("first", false, None); let second = Credential::new("second", false, None);
    let first_key = first.key.private_key_to_pkcs8_passphrase(Cipher::aes_256_cbc(), b"key").unwrap();
    let second_key = second.key.private_key_to_pkcs8_passphrase(Cipher::aes_256_cbc(), b"key").unwrap();
    let certificate_bag = |identity: &Credential, id| {
        bag(3, &seq(&[der(6, b"\x2a\x86\x48\x86\xf7\x0d\x01\x09\x16\x01"), der(0xa0, &der(4, &identity.certificate.to_der().unwrap()))]), id, None)
    };
    let secret = seq(&[der(6, &[0x2a, 3]), der(0xa0, &der(4, b"not recovered"))]);
    for (kind, expected) in [(4, ["second", "first"]), (5, ["first", "second"])] {
        let bytes = pfx(&[bag(kind, &secret, 1, Some("FIRST")), bag(2, &second_key, 2, Some("second")), bag(2, &first_key, 3, Some("first")),
            certificate_bag(&second, 2), certificate_bag(&first, 3)]);
        let memory = MemoryBudget::new(32 << 20);
        let initial = memory.reserve(Material::allocation_bound(bytes.len(), StoreFormat::Java, StoreUse::OptionalIdentity, Algorithm::NewSunX509, None, Some("key")).unwrap()).unwrap();
        let material = Material::decode(&bytes, StoreFormat::Java, StoreUse::OptionalIdentity, Algorithm::NewSunX509, None, Some("key"), initial).unwrap();
        assert!(matches!(material, Material::Deferred(_)));
        let charge = memory.reserve(65_536).unwrap(); let random = random::Context::new(None, &memory).unwrap();
        let native = material.native(Algorithm::NewSunX509, &random, &charge).unwrap();
        assert_eq!(native.manager().choose(&[KeyType::Rsa], |_, _| true, |_| crate::tls::keys::Status::Valid), Some(0));
        let expected = expected.map(|alias| if alias == "first" { first.certificate.to_der().unwrap() } else { second.certificate.to_der().unwrap() });
        assert_eq!(native.entries().iter().map(|identity| identity.certificate.to_der().unwrap()).collect::<Vec<_>>(), expected);
        drop(material); drop(native); drop(random); drop(charge); assert_eq!(memory.used_bytes(), 0);
    }
}
