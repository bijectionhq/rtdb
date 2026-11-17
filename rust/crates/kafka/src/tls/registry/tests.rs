use super::*;
use openssl::ssl::{Ssl, SslMethod, SslOptions, SslStream, SslVersion};
use rtdb_columnar::MemoryBudget;
use rtdb_tls::{StoreFormat, StoreUse};
use crate::tls::tests::support::Credential;

fn empty(charge: Arc<MemoryReservation>) -> KeyStore {
    KeyStore::decode(include_bytes!("../../../../tls/tests/fixtures/empty.jks"), StoreFormat::Java, StoreUse::Trust, Some("12345678"), None, charge).unwrap()
}

#[test]
fn registry_constructor_preserves_raw_names_and_ignores_unused_socket_options() {
    let memory = MemoryBudget::new(1 << 25); let charge = memory.reserve(1 << 20).unwrap(); let empty = empty(Arc::clone(&charge));
    let options = BTreeMap::from([("protocol", "TLSv1.2"), ("cipher.suites", "NOT_A_CIPHER"), ("enabled.protocols", "NOT_A_PROTOCOL"), ("secure.random.implementation", "SHA1PRNG")]);
    let owner = NativeTls::from_registry(&options, Some(&empty), None, Arc::clone(&charge)).unwrap();
    let context = unsafe { owner.registry_context().unwrap() };
    let options = unsafe { openssl_sys::SSL_CTX_get_options(context.as_ptr()) } as u64;
    assert_ne!(options & SslOptions::NO_TLSV1_3.bits() as u64, 0);
    assert_eq!(options & SslOptions::NO_TLSV1_2.bits() as u64, 0);
    assert_eq!(context.verify_mode(), SslVerifyMode::PEER);
    drop(context); drop(owner);
    for (name, value) in [("protocol", " TLSv1.2 "), ("protocol", ""), ("provider", " SunJSSE"), ("keymanager.algorithm", " SunX509"), ("trustmanager.algorithm", "PKIX "), ("secure.random.implementation", ""), ("secure.random.implementation", " SHA1PRNG ")] {
        assert!(NativeTls::from_registry(&BTreeMap::from([(name, value)]), Some(&empty), None, Arc::clone(&charge)).is_err());
    }
}

#[test]
fn registry_default_context_negotiates_java_tls12_cbc() {
    use std::{io::{self, Read, Write}, net::{TcpListener, TcpStream}, thread, time::{Duration, Instant}};
    let root = Credential::new("issuer", false, None);
    let server = Credential::new("localhost", false, Some(&root));
    let mut server_context = SslContextBuilder::new(SslMethod::tls_server()).unwrap();
    server_context.set_min_proto_version(Some(SslVersion::TLS1_2)).unwrap(); server_context.set_max_proto_version(Some(SslVersion::TLS1_2)).unwrap();
    server_context.set_cipher_list("ECDHE-RSA-AES128-SHA").unwrap();
    server_context.set_certificate(&server.certificate).unwrap(); server_context.set_private_key(&server.key).unwrap();
    let server_context = server_context.build();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap(); let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let server = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        let socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock && Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
                Err(error) => panic!("Registry fixture accept failed: {error}"),
            }
        };
        socket.set_nonblocking(false).unwrap();
        socket.set_read_timeout(Some(Duration::from_secs(3))).unwrap(); socket.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
        let mut stream = SslStream::new(Ssl::new(&server_context).unwrap(), socket).unwrap(); stream.accept().unwrap();
        let mut byte = [0]; stream.read_exact(&mut byte).unwrap(); assert_eq!(byte, [7]);
        stream.ssl().current_cipher().unwrap().standard_name().unwrap().to_owned()
    });
    let memory = MemoryBudget::new(1 << 25); let charge = memory.reserve(1 << 20).unwrap();
    let trust = KeyStore::decode(&root.certificate.to_pem().unwrap(), StoreFormat::Pem, StoreUse::Trust, None, None, Arc::clone(&charge)).unwrap();
    let owner = NativeTls::from_registry(&BTreeMap::from([("protocol", "TLSv1.2"), ("secure.random.implementation", "SHA1PRNG")]), Some(&trust), None, charge).unwrap();
    let context = unsafe { owner.registry_context().unwrap() };
    let mut ssl = Ssl::new(&context).unwrap(); ssl.param_mut().set_host("localhost").unwrap(); ssl.set_hostname("localhost").unwrap();
    let socket = TcpStream::connect(address).unwrap();
    socket.set_read_timeout(Some(Duration::from_secs(3))).unwrap(); socket.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut stream = SslStream::new(ssl, socket).unwrap(); stream.connect().unwrap(); stream.write_all(&[7]).unwrap();
    assert_eq!(server.join().unwrap(), "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA");
    drop(stream); drop(context); drop(owner); drop(trust);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn registry_native_default_roots_and_empty_explicit_store_keep_their_owners() {
    let memory = MemoryBudget::new(32 << 20);
    let charge = memory.reserve(crate::tls::trust::allocation_bound().unwrap()).unwrap();
    let defaults = NativeTls::from_registry(&BTreeMap::new(), None, None, Arc::clone(&charge)).unwrap();
    assert_eq!(defaults.trust.as_ref().unwrap().len(), 146);
    let empty = empty(Arc::clone(&charge));
    let explicit = NativeTls::from_registry(&BTreeMap::new(), Some(&empty), None, Arc::clone(&charge)).unwrap();
    assert!(explicit.trust.as_ref().unwrap().is_empty());
    drop(defaults); drop(explicit); drop(empty); drop(charge);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn registry_native_managers_select_java_alias_order_for_tls12_and_tls13() {
    use crate::tls::tests::support;
    use std::{net::TcpStream, time::Duration};
    let root = Credential::new("issuer", false, None);
    let server = Credential::new("localhost", false, Some(&root));
    let first = Credential::new("first", false, Some(&root));
    let second = Credential::new("second", false, Some(&root));
    let bytes = support::jks(&[("a", &first), ("b", &second)], "store", "key");
    let memory = MemoryBudget::new(32 << 20); let charge = memory.reserve(1 << 20).unwrap();
    let trust = KeyStore::decode(&root.certificate.to_pem().unwrap(), StoreFormat::Pem, StoreUse::Trust, None, None, Arc::clone(&charge)).unwrap();
    for version in [SslVersion::TLS1_2, SslVersion::TLS1_3] {
        for (algorithm, expected) in [("SunX509", &first), ("NewSunX509", &second), ("PKIX", &second)] {
            let identities = Material::decode(&bytes, StoreFormat::Java, StoreUse::OptionalIdentity, Algorithm::parse(Some(algorithm)).unwrap(), Some("store"), Some("key"), Arc::clone(&charge)).unwrap();
            let (address, broker) = support::broker(&server, &root, version, Some("rsa_pss_rsae_sha256:rsa_pkcs1_sha256"));
            let owner = NativeTls::from_registry(&BTreeMap::from([("keymanager.algorithm", algorithm)]), Some(&trust), Some(&identities), Arc::clone(&charge)).unwrap();
            let context = unsafe { owner.registry_context().unwrap() };
            let socket = TcpStream::connect(address).unwrap(); socket.set_read_timeout(Some(Duration::from_secs(3))).unwrap(); socket.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
            let mut ssl = Ssl::new(&context).unwrap(); ssl.param_mut().set_host("localhost").unwrap();
            let mut stream = SslStream::new(ssl, socket).unwrap(); stream.connect().unwrap();
            let observed = broker.join().unwrap().unwrap();
            assert_eq!(observed.certificate, expected.certificate.to_der().unwrap());
            assert_eq!(observed.protocol, if version == SslVersion::TLS1_2 { "TLSv1.2" } else { "TLSv1.3" });
            drop(stream); drop(context); drop(owner);
        }
    }
    drop(trust); drop(charge); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn registry_native_selection_honors_peer_issuer_hints_and_ec_signatures() {
    use crate::tls::tests::support;
    use std::{net::TcpStream, time::Duration};
    let root = Credential::new("accepted issuer", false, None);
    let other = Credential::new("other issuer", false, None);
    let server = Credential::new("localhost", true, Some(&root));
    let rsa = Credential::new("rsa client", false, Some(&root));
    let ec = Credential::new("ec client", true, Some(&root));
    let bytes = support::jks(&[("rsa", &rsa), ("ec", &ec)], "store", "key");
    let memory = MemoryBudget::new(32 << 20); let charge = memory.reserve(1 << 20).unwrap();
    let trust = KeyStore::decode(&root.certificate.to_pem().unwrap(), StoreFormat::Pem, StoreUse::Trust, None, None, Arc::clone(&charge)).unwrap();
    for algorithm in ["SunX509", "NewSunX509"] {
        let identities = Material::decode(&bytes, StoreFormat::Java, StoreUse::OptionalIdentity, Algorithm::parse(Some(algorithm)).unwrap(), Some("store"), Some("key"), Arc::clone(&charge)).unwrap();
        for (issuer, accepted) in [(&root, true), (&other, false)] {
            let (address, broker) = support::broker(&server, issuer, SslVersion::TLS1_2, Some("ecdsa_secp256r1_sha256:rsa_pkcs1_sha256"));
            let owner = NativeTls::from_registry(&BTreeMap::from([("keymanager.algorithm", algorithm)]), Some(&trust), Some(&identities), Arc::clone(&charge)).unwrap();
            let context = unsafe { owner.registry_context().unwrap() };
            let socket = TcpStream::connect(address).unwrap(); socket.set_read_timeout(Some(Duration::from_secs(3))).unwrap(); socket.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
            let mut stream = SslStream::new(Ssl::new(&context).unwrap(), socket).unwrap();
            assert_eq!(stream.connect().is_ok(), accepted);
            let observed = broker.join().unwrap();
            if accepted { assert_eq!(observed.unwrap().certificate, ec.certificate.to_der().unwrap()); } else { assert!(observed.is_err()); }
            drop(stream); drop(context); drop(owner);
        }
    }
    drop(trust); drop(charge); assert_eq!(memory.used_bytes(), 0);
}
