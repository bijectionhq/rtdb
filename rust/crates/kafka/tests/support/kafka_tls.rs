use std::{io, net::TcpListener, thread, time::{Duration, Instant}};
use openssl::{asn1::Asn1Time, bn::BigNum, ec::{EcGroup, EcKey}, hash::{MessageDigest, hash}, nid::Nid, pkey::{PKey, Private}, rsa::Rsa, ssl::{Ssl, SslContextBuilder, SslMethod, SslStream, SslVerifyMode, SslVersion}, x509::{X509, X509NameBuilder, extension::{BasicConstraints, ExtendedKeyUsage, KeyUsage, SubjectAlternativeName}}};

pub struct Credential { pub key: PKey<Private>, pub certificate: X509, pub chain: Vec<X509> }
impl Credential {
    pub fn new(name: &str, ec: bool, issuer: Option<&Self>) -> Self {
        let key = if ec {
            PKey::from_ec_key(EcKey::generate(&EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap()).unwrap()).unwrap()
        } else { PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap() };
        let mut subject = X509NameBuilder::new().unwrap(); subject.append_entry_by_text("CN", name).unwrap(); let subject = subject.build();
        let mut certificate = X509::builder().unwrap();
        certificate.set_version(2).unwrap();
        let serial = BigNum::from_u32(if ec { 2 } else { 1 }).unwrap().to_asn1_integer().unwrap(); certificate.set_serial_number(&serial).unwrap();
        certificate.set_subject_name(&subject).unwrap();
        certificate.set_issuer_name(issuer.map_or(subject.as_ref(), |issuer| issuer.certificate.subject_name())).unwrap();
        certificate.set_pubkey(&key).unwrap();
        certificate.set_not_before(&Asn1Time::days_from_now(0).unwrap()).unwrap(); certificate.set_not_after(&Asn1Time::days_from_now(1).unwrap()).unwrap();
        if issuer.is_none() {
            certificate.append_extension(BasicConstraints::new().critical().ca().build().unwrap()).unwrap();
            certificate.append_extension(KeyUsage::new().key_cert_sign().crl_sign().digital_signature().build().unwrap()).unwrap();
        } else {
            certificate.append_extension(BasicConstraints::new().critical().build().unwrap()).unwrap();
            certificate.append_extension(KeyUsage::new().digital_signature().build().unwrap()).unwrap();
            certificate.append_extension(ExtendedKeyUsage::new().client_auth().server_auth().build().unwrap()).unwrap();
        }
        let names = SubjectAlternativeName::new().dns("localhost").ip("127.0.0.1").build(&certificate.x509v3_context(issuer.map(|issuer| issuer.certificate.as_ref()), None)).unwrap();
        certificate.append_extension(names).unwrap();
        certificate.sign(issuer.map_or(&key, |issuer| &issuer.key), MessageDigest::sha256()).unwrap();
        let chain = issuer.map(|issuer| std::iter::once(issuer.certificate.clone()).chain(issuer.chain.iter().cloned()).collect()).unwrap_or_default();
        Self { key, certificate: certificate.build(), chain }
    }
}

fn der(tag: u8, bytes: &[u8]) -> Vec<u8> {
    let mut result = vec![tag];
    if bytes.len() < 128 { result.push(bytes.len() as u8); }
    else {
        let length = bytes.len().to_be_bytes(); let first = length.iter().position(|byte| *byte != 0).unwrap();
        result.push(0x80 | (length.len() - first) as u8); result.extend_from_slice(&length[first..]);
    }
    result.extend_from_slice(bytes); result
}
fn sha1(bytes: &[u8]) -> Vec<u8> { hash(MessageDigest::sha1(), bytes).unwrap().to_vec() }
fn password(value: &str) -> Vec<u8> { value.encode_utf16().flat_map(u16::to_be_bytes).collect() }
fn short(output: &mut Vec<u8>, value: &[u8]) { output.extend_from_slice(&u16::try_from(value.len()).unwrap().to_be_bytes()); output.extend_from_slice(value); }
fn long(output: &mut Vec<u8>, value: &[u8]) { output.extend_from_slice(&u32::try_from(value.len()).unwrap().to_be_bytes()); output.extend_from_slice(value); }

pub fn jks(identities: &[(&str, &Credential)], store_password: &str, key_password: &str) -> Vec<u8> {
    jks_with_passwords(&identities.iter().map(|(alias, identity)| (*alias, *identity, key_password)).collect::<Vec<_>>(), store_password)
}

pub fn jks_with_passwords(identities: &[(&str, &Credential, &str)], store_password: &str) -> Vec<u8> {
    let mut store = 0xfeedfeedu32.to_be_bytes().to_vec(); store.extend_from_slice(&2u32.to_be_bytes()); store.extend_from_slice(&(identities.len() as u32).to_be_bytes());
    for (alias, identity, key_password) in identities {
        let key = identity.key.private_key_to_pkcs8().unwrap();
        let key_password = password(key_password); let mut previous = vec![7; 20]; let mut protected = previous.clone();
        for block in key.chunks(20) { previous = sha1(&[key_password.as_slice(), &previous].concat()); protected.extend(block.iter().zip(&previous).map(|(byte, mask)| byte ^ mask)); }
        protected.extend_from_slice(&sha1(&[key_password.as_slice(), &key].concat()));
        let algorithm = der(0x30, &[der(6, &[0x2b, 6, 1, 4, 1, 0x2a, 2, 0x11, 1, 1]), der(5, &[])].concat());
        let protected = der(0x30, &[algorithm, der(4, &protected)].concat());
        store.extend_from_slice(&1u32.to_be_bytes()); short(&mut store, alias.as_bytes()); store.extend_from_slice(&0i64.to_be_bytes()); long(&mut store, &protected);
        store.extend_from_slice(&u32::try_from(1 + identity.chain.len()).unwrap().to_be_bytes());
        for certificate in std::iter::once(&identity.certificate).chain(&identity.chain) { short(&mut store, b"X.509"); long(&mut store, &certificate.to_der().unwrap()); }
    }
    let checksum = sha1(&[password(store_password), b"Mighty Aphrodite".to_vec(), store.clone()].concat()); store.extend_from_slice(&checksum); store
}

pub struct Handshake { pub certificate: Vec<u8>, pub protocol: String, pub cipher: String }

pub fn broker(server: &Credential, accepted_issuer: &Credential, version: SslVersion, signatures: Option<&str>) -> (String, thread::JoinHandle<Result<Handshake, String>>) {
    let mut context = SslContextBuilder::new(SslMethod::tls_server()).unwrap();
    context.set_min_proto_version(Some(version)).unwrap(); context.set_max_proto_version(Some(version)).unwrap();
    context.set_certificate(&server.certificate).unwrap(); context.set_private_key(&server.key).unwrap();
    for certificate in &server.chain { context.add_extra_chain_cert(certificate.clone()).unwrap(); }
    context.cert_store_mut().add_cert(accepted_issuer.certificate.clone()).unwrap(); context.add_client_ca(&accepted_issuer.certificate).unwrap();
    context.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
    if let Some(signatures) = signatures { context.set_sigalgs_list(signatures).unwrap(); }
    let context = context.build();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap(); listener.set_nonblocking(true).unwrap(); let address = listener.local_addr().unwrap().to_string();
    let server = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(10);
        let stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == io::ErrorKind::WouldBlock && Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
                Err(error) => return Err(format!("accept: {error}")),
            }
        };
        stream.set_nonblocking(false).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(3))).unwrap(); stream.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
        let mut stream = SslStream::new(Ssl::new(&context).unwrap(), stream).unwrap(); stream.accept().map_err(|error| format!("handshake: {error}"))?;
        Ok(Handshake { certificate: stream.ssl().peer_certificate().unwrap().to_der().unwrap(), protocol: stream.ssl().version_str().into(), cipher: stream.ssl().current_cipher().unwrap().standard_name().unwrap().into() })
    });
    (address, server)
}
