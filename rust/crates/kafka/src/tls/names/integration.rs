use super::*;
use crate::tls::{NativeTls, tests::support::{self, Credential}};
use foreign_types::ForeignType;
use openssl::{ssl::{Ssl, SslContextBuilder, SslMethod, SslStream, SslVerifyMode, SslVersion}, x509::X509NameBuilder};
use std::{collections::BTreeMap, io, net::{TcpListener, TcpStream}, thread, time::{Duration, Instant}};

fn broker(server: &Credential, root: &Credential, authority: &Credential, version: SslVersion) -> (String, thread::JoinHandle<std::result::Result<Vec<u8>, String>>) {
    let mut context = SslContextBuilder::new(SslMethod::tls_server()).unwrap();
    context.set_min_proto_version(Some(version)).unwrap(); context.set_max_proto_version(Some(version)).unwrap();
    context.set_certificate(&server.certificate).unwrap(); context.set_private_key(&server.key).unwrap();
    context.cert_store_mut().add_cert(root.certificate.clone()).unwrap();
    context.add_client_ca(&authority.certificate).unwrap();
    context.set_sigalgs_list("rsa_pss_rsae_sha256:rsa_pkcs1_sha256").unwrap();
    context.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
    let context = context.build();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap(); listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let task = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        let socket = loop {
            match listener.accept() {
                Ok((socket, _)) => break socket,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock && Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
                Err(error) => return Err(error.to_string()),
            }
        };
        socket.set_read_timeout(Some(Duration::from_secs(3))).unwrap(); socket.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
        let mut stream = SslStream::new(Ssl::new(&context).unwrap(), socket).unwrap();
        stream.accept().map_err(|error| error.to_string())?;
        stream.ssl().peer_certificate().ok_or_else(|| "Client certificate is absent".to_owned())?.to_der().map_err(|error| error.to_string())
    });
    (address, task)
}

#[test]
fn native_client_managers_match_java_authority_names_for_tls12_and_tls13() {
    let root = Credential::new("ÉCOLE", false, None);
    let server = Credential::new("localhost", false, Some(&root));
    let client = Credential::new("client", false, Some(&root));
    let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("client.jks");
    std::fs::write(&path, support::jks(&[("client", &client)], "store", "key")).unwrap();
    let memory = MemoryBudget::new(32 << 20);
    for version in [SslVersion::TLS1_2, SslVersion::TLS1_3] {
        for algorithm in ["SunX509", "NewSunX509"] {
            for (authority, accepted) in [("e\u{301}cole", true), ("ecole", false)] {
                let authority = Credential::new(authority, false, None);
                let (address, broker) = broker(&server, &root, &authority, version);
                let options = BTreeMap::from([
                    ("security.protocol".into(), "SSL".into()),
                    ("ssl.keymanager.algorithm".into(), algorithm.into()),
                    ("ssl.trustmanager.algorithm".into(), "SunX509".into()),
                    ("ssl.truststore.type".into(), "PEM".into()),
                    ("ssl.truststore.certificates".into(), String::from_utf8(root.certificate.to_pem().unwrap()).unwrap()),
                    ("ssl.keystore.location".into(), path.to_str().unwrap().into()),
                    ("ssl.keystore.password".into(), "store".into()),
                    ("ssl.key.password".into(), "key".into()),
                ]);
                let owner = NativeTls::from_java(&options, &mut |bytes| memory.reserve(bytes)).unwrap().unwrap();
                let context = unsafe { SSL_CTX_new_ex(owner.random.as_ptr(), ptr::null(), openssl_sys::TLS_client_method()) };
                assert!(!context.is_null());
                let mut context = unsafe { SslContextBuilder::from_ptr(context) };
                context.set_verify(SslVerifyMode::PEER);
                unsafe { owner.configure_context(&mut context).unwrap(); }
                let context = context.build();
                let socket = TcpStream::connect(address).unwrap();
                socket.set_read_timeout(Some(Duration::from_secs(3))).unwrap(); socket.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
                let mut ssl = Ssl::new(&context).unwrap(); ssl.param_mut().set_host("localhost").unwrap();
                let mut stream = SslStream::new(ssl, socket).unwrap();
                let result = stream.connect();
                let observed = broker.join().unwrap();
                if accepted { result.unwrap(); assert_eq!(observed.unwrap(), client.certificate.to_der().unwrap()); }
                else { assert!(observed.is_err()); }
                drop(stream); drop(context); drop(owner);
                assert_eq!(memory.used_bytes(), 0);
            }
        }
    }
}

#[test]
fn native_authority_storage_and_matching_release_independent_charges() {
    let memory = MemoryBudget::new(1 << 20);
    let credential = Credential::new("ÉCOLE", false, None);
    let mut name = X509NameBuilder::new().unwrap(); name.append_entry_by_text("CN", "e\u{301}cole").unwrap();
    let mut names = openssl::stack::Stack::new().unwrap(); names.push(name.build()).unwrap();
    let authorities = Authorities::decode(Some(&names), &memory).unwrap();
    let retained = memory.used_bytes(); assert!(retained > 0);
    assert!(matches_issuer([credential.certificate.as_ref()], &authorities, &memory).unwrap());
    assert_eq!(memory.used_bytes(), retained);
    let occupied = memory.reserve(memory.limit_bytes() - retained).unwrap();
    assert_eq!(matches_issuer([credential.certificate.as_ref()], &authorities, &memory).err().unwrap().code, "rtdb/memory-budget-exceeded");
    drop(occupied); drop(authorities); assert_eq!(memory.used_bytes(), 0);
    let empty = Authorities::decode(None, &memory).unwrap();
    assert!(matches_issuer(std::iter::empty::<&openssl::x509::X509Ref>(), &empty, &memory).unwrap());
    drop(empty); assert_eq!(memory.used_bytes(), 0);
}

unsafe extern "C" {
    fn SSL_CTX_new_ex(context: *mut openssl_sys::OSSL_LIB_CTX, properties: *const std::ffi::c_char, method: *const openssl_sys::SSL_METHOD) -> *mut openssl_sys::SSL_CTX;
}
