use super::*;
use std::{net::TcpStream, time::Duration};
use openssl::ssl::{Ssl, SslStream};
use rtdb_tls::KeyStore;

#[test]
fn native_new_sunx509_recovers_only_the_metadata_selected_entry_in_both_transports() {
    let root = support::Credential::new("accepted issuer", false, None);
    let other = support::Credential::new("other issuer", false, None);
    let server = support::Credential::new("localhost", false, Some(&root));
    let good = support::Credential::new("good client", false, Some(&root));
    let rejected = support::Credential::new("rejected issuer", false, Some(&other));
    let selected_bad = support::Credential::new("selected bad key", false, Some(&root));
    let trust_pem = root.certificate.to_pem().unwrap();
    let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("client.jks");
    for (bad, accepts) in [(&rejected, true), (&selected_bad, false)] {
        let bytes = support::jks_with_passwords(&[("a", &good, "key"), ("b", bad, "unavailable")], "store");
        std::fs::write(&path, &bytes).unwrap();
        for version in [SslVersion::TLS1_2, SslVersion::TLS1_3] {
            for registry in [false, true] {
                let memory = MemoryBudget::new(32 << 20);
                let owner = if registry {
                    let charge = memory.reserve(1 << 20).unwrap();
                    let trust = KeyStore::decode(&trust_pem, StoreFormat::Pem, StoreUse::Trust, None, None, Arc::clone(&charge)).unwrap();
                    let identities = Material::decode(&bytes, StoreFormat::Java, StoreUse::OptionalIdentity, Algorithm::NewSunX509, Some("store"), Some("key"), Arc::clone(&charge)).unwrap();
                    NativeTls::from_registry(&BTreeMap::from([("keymanager.algorithm", "NewSunX509")]), Some(&trust), Some(&identities), charge).unwrap()
                } else {
                    let mut options = options();
                    options.insert("ssl.keymanager.algorithm".into(), "NewSunX509".into());
                    options.insert("ssl.keystore.location".into(), path.to_str().unwrap().into());
                    options.insert("ssl.keystore.password".into(), "store".into());
                    options.insert("ssl.key.password".into(), "key".into());
                    options.insert("ssl.truststore.type".into(), "PEM".into());
                    options.insert("ssl.truststore.certificates".into(), String::from_utf8(trust_pem.clone()).unwrap());
                    NativeTls::from_java(&options, &mut |bytes| memory.reserve(bytes)).unwrap().unwrap()
                };
                let (address, broker) = support::broker(&server, &root, version, Some("rsa_pss_rsae_sha256:rsa_pkcs1_sha256"));
                let context = unsafe { owner.registry_context().unwrap() };
                let socket = TcpStream::connect(address).unwrap(); socket.set_read_timeout(Some(Duration::from_secs(3))).unwrap(); socket.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
                let mut ssl = Ssl::new(&context).unwrap(); ssl.param_mut().set_host("localhost").unwrap();
                let mut stream = SslStream::new(ssl, socket).unwrap();
                let connected = stream.connect();
                if accepts { connected.unwrap(); }
                let observed = broker.join().unwrap();
                if accepts { assert_eq!(observed.unwrap().certificate, good.certificate.to_der().unwrap()); }
                else { assert!(observed.is_err()); assert!(stream.ssl().certificate().is_none()); }
                drop(stream); drop(context); drop(owner);
                assert_eq!(memory.used_bytes(), 0);
            }
        }
    }
}
