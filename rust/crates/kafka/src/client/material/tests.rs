use super::*;
use std::{ffi::CString, ptr};
use foreign_types::ForeignTypeRef;
use openssl::{asn1::{Asn1Object, Asn1OctetString, Asn1Time}, hash::MessageDigest, pkcs12::Pkcs12, ssl::{SslContextBuilder, SslContextRef, SslMethod}, symm::Cipher,
    x509::{X509CrlBuilder, X509Extension}};
use rdkafka::{ClientConfig, client::{Client, DefaultClientContext}, producer::BaseProducer, types::RDKafkaType};
use rtdb_columnar::MemoryBudget;
#[path = "../../../tests/support/kafka_tls.rs"]
pub(super) mod support;

pub(super) fn owner(memory: &Arc<MemoryBudget>) -> Arc<NativeTls> {
    let options = BTreeMap::from([("security.protocol".into(), "SSL".into()), ("ssl.ca.pem".into(), "native trust".into())]);
    NativeTls::from_java(&options, &mut |bytes| memory.reserve(bytes)).unwrap().unwrap()
}

fn properties(root: &support::Credential) -> BTreeMap<String, String> {
    BTreeMap::from([("security.protocol".into(), "SSL".into()), ("ssl.ca.pem".into(), String::from_utf8(root.certificate.to_pem().unwrap()).unwrap())])
}

fn client(properties: &BTreeMap<String, String>, memory: &Arc<MemoryBudget>) -> ClientConfig {
    let snapshot = Configuration::capture(owner(memory), properties, &mut |bytes| memory.reserve(bytes)).unwrap();
    let mut client = crate::client_config(properties);
    client.set_ssl_context_configurer(snapshot);
    client
}

fn crl(root: &support::Credential) -> Vec<u8> {
    let mut crl = X509CrlBuilder::new().unwrap();
    crl.set_issuer_name(root.certificate.subject_name()).unwrap();
    crl.set_last_update(&Asn1Time::days_from_now(0).unwrap()).unwrap();
    crl.set_next_update(&Asn1Time::days_from_now(1).unwrap()).unwrap();
    for (oid, der) in [("2.5.29.35", &[0x30, 0][..]), ("2.5.29.20", &[2, 1, 1][..])] {
        crl.append_extension(X509Extension::new_from_der(&Asn1Object::from_str(oid).unwrap(), false, &Asn1OctetString::new_from_bytes(der).unwrap()).unwrap()).unwrap();
    }
    crl.sign(&root.key, MessageDigest::sha256()).unwrap();
    crl.build().unwrap().to_pem().unwrap()
}

#[test]
fn an_unrepresentable_snapshot_is_resource_exhaustion() {
    assert!(overflow().is(rtdb_types::AnomalyKind::ResourceExhausted));
}

#[test]
fn snapshots_keep_each_native_filename_and_bytes_after_unlinking() {
    let directory = tempfile::tempdir().unwrap();
    let memory = MemoryBudget::new(32 << 20);
    let mut properties = BTreeMap::new();
    for (index, (_, name)) in MATERIALS.iter().enumerate() {
        let path = directory.path().join(index.to_string());
        std::fs::write(&path, [index as u8, 0, 255]).unwrap();
        properties.insert((*name).into(), path.to_str().unwrap().into());
    }
    let configuration = Configuration::capture(owner(&memory), &properties, &mut |bytes| memory.reserve(bytes)).unwrap();
    let retained = memory.used_bytes(); assert!(retained > 0);
    for (index, (kind, name)) in MATERIALS.iter().enumerate() {
        let path = CString::new(properties[*name].as_str()).unwrap();
        std::fs::remove_file(&properties[*name]).unwrap();
        let SslMaterial::Bytes(bytes) = configuration.material(*kind, &path) else { panic!("missing snapshot"); };
        assert_eq!(bytes, &[index as u8, 0, 255]);
        assert!(matches!(configuration.material(*kind, c"another-file"), SslMaterial::Unavailable(libc::EINVAL)));
    }
    let copy = Arc::clone(&configuration); drop(configuration);
    assert_eq!(memory.used_bytes(), retained); drop(copy); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn file_read_failure_is_deferred_and_not_retried_after_the_path_is_repaired() {
    let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("missing");
    let memory = MemoryBudget::new(32 << 20);
    let properties = BTreeMap::from([("ssl.key.location".into(), path.to_str().unwrap().into())]);
    let configuration = Configuration::capture(owner(&memory), &properties, &mut |bytes| memory.reserve(bytes)).unwrap();
    std::fs::write(&path, b"created later").unwrap();
    assert!(matches!(configuration.material(SslMaterialKind::PrivateKey, &CString::new(path.to_str().unwrap()).unwrap()), SslMaterial::Unavailable(_)));
    drop(configuration); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn raw_file_admission_failure_rolls_back_previously_captured_files() {
    let directory = tempfile::tempdir().unwrap(); let first = directory.path().join("ca"); let second = directory.path().join("key");
    std::fs::write(&first, [1; 64]).unwrap(); std::fs::write(&second, [2; 4096]).unwrap();
    let owner_memory = MemoryBudget::new(32 << 20); let memory = MemoryBudget::new(2048);
    let properties = BTreeMap::from([("ssl.ca.location".into(), first.to_str().unwrap().into()), ("ssl.key.location".into(), second.to_str().unwrap().into())]);
    let error = Configuration::capture(owner(&owner_memory), &properties, &mut |bytes| memory.reserve(bytes)).err().unwrap();
    assert_eq!(error.code, "rtdb/memory-budget-exceeded");
    assert_eq!(memory.used_bytes(), 0); assert_eq!(owner_memory.used_bytes(), 0);
}

#[test]
fn native_directories_keep_the_original_lazy_filesystem_lookup() {
    let directory = tempfile::tempdir().unwrap(); let memory = MemoryBudget::new(32 << 20);
    let properties = BTreeMap::from([("ssl.ca.location".into(), directory.path().to_str().unwrap().into())]);
    let configuration = Configuration::capture(owner(&memory), &properties, &mut |bytes| memory.reserve(bytes)).unwrap();
    assert!(matches!(configuration.material(SslMaterialKind::CertificateAuthorities, &CString::new(directory.path().to_str().unwrap()).unwrap()), SslMaterial::Unmanaged));
    drop(configuration); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_files_are_validated_in_order_before_later_inline_overrides() {
    let root = support::Credential::new("root", false, None);
    let directory = tempfile::tempdir().unwrap(); let missing = directory.path().join("missing");
    let invalid = directory.path().join("invalid"); std::fs::write(&invalid, b"invalid PEM").unwrap();
    let memory = MemoryBudget::new(128 << 20);
    for (name, inline, value) in [
        ("ssl.ca.location", "ssl.ca.pem", root.certificate.to_pem().unwrap()),
        ("ssl.certificate.location", "ssl.certificate.pem", root.certificate.to_pem().unwrap()),
        ("ssl.key.location", "ssl.key.pem", root.key.private_key_to_pem_pkcs8().unwrap()),
    ] {
        let mut properties = properties(&root);
        properties.insert(name.into(), invalid.to_str().unwrap().into());
        properties.insert(inline.into(), String::from_utf8(value).unwrap());
        properties.insert("ssl.key.password".into(), "password".into());
        let configuration = client(&properties, &memory);
        let error = configuration.create::<BaseProducer>().err().unwrap().to_string();
        assert!(error.contains(&format!("{name} failed")), "{error}");
        drop(configuration); assert_eq!(memory.used_bytes(), 0);
    }
    let mut properties = properties(&root);
    properties.insert("ssl.ca.pem".into(), "invalid earlier inline CA".into());
    properties.insert("ssl.key.location".into(), missing.to_str().unwrap().into());
    let configuration = client(&properties, &memory);
    let error = configuration.create::<BaseProducer>().err().unwrap().to_string();
    assert!(error.contains("ssl.ca.pem"), "{error}");
    assert!(!error.contains("ssl.key.location failed"), "{error}");
    drop(configuration); assert_eq!(memory.used_bytes(), 0);
}

struct Inspect { snapshot: Arc<Configuration>, certificate: Vec<u8>, chain_length: usize }
impl SslContextConfigurer for Inspect {
    fn material_source(&self) -> Option<&dyn SslMaterialSource> { self.snapshot.material_source() }
    fn replaces_default_trust(&self) -> bool { self.snapshot.replaces_default_trust() }
    unsafe fn library_context(&self) -> *mut c_void { unsafe { self.snapshot.library_context() } }
    unsafe fn configure(&self, context: *mut c_void) -> std::result::Result<(), String> {
        let context_ref = unsafe { SslContextRef::from_ptr(context.cast()) };
        assert_eq!(context_ref.certificate().unwrap().to_der().unwrap(), self.certificate);
        assert!(context_ref.private_key().unwrap().public_eq(&context_ref.certificate().unwrap().public_key().unwrap()));
        assert_eq!(chain_length(context.cast()), self.chain_length);
        unsafe { self.snapshot.configure(context) }
    }
}

#[test]
fn native_ca_crl_certificate_and_key_snapshots_survive_configuration_copies() {
    let root = support::Credential::new("root", false, None); let credential = support::Credential::new("client", false, Some(&root));
    let directory = tempfile::tempdir().unwrap(); let memory = MemoryBudget::new(128 << 20);
    let mut properties = properties(&root);
    properties.insert("ssl.key.password".into(), "password".into());
    for (name, bytes) in [
        ("ssl.ca.location", root.certificate.to_pem().unwrap()),
        ("ssl.crl.location", crl(&root)),
        ("ssl.certificate.location", [credential.certificate.to_pem().unwrap(), root.certificate.to_pem().unwrap()].concat()),
        ("ssl.key.location", credential.key.private_key_to_pem_pkcs8_passphrase(Cipher::aes_256_cbc(), b"password").unwrap()),
    ] {
        let path = directory.path().join(name); std::fs::write(&path, bytes).unwrap(); properties.insert(name.into(), path.to_str().unwrap().into());
    }
    let snapshot = Configuration::capture(owner(&memory), &properties, &mut |bytes| memory.reserve(bytes)).unwrap();
    let mut configuration = crate::client_config(&properties);
    configuration.set_ssl_context_configurer(Arc::new(Inspect { snapshot, certificate: credential.certificate.to_der().unwrap(), chain_length: 1 }));
    let native = configuration.create_native_config().unwrap(); let duplicate = native.clone();
    for (kind, name) in MATERIALS { if kind != SslMaterialKind::KeyStore { std::fs::remove_file(&properties[name]).unwrap(); } }
    drop(configuration);
    let retained = memory.used_bytes();
    let first = Client::new(&ClientConfig::new(), native, RDKafkaType::RD_KAFKA_PRODUCER, DefaultClientContext).unwrap();
    let per_client = memory.used_bytes() - retained; assert!(per_client > 0);
    let second = Client::new(&ClientConfig::new(), duplicate, RDKafkaType::RD_KAFKA_PRODUCER, DefaultClientContext).unwrap();
    assert_eq!(memory.used_bytes(), retained + 2 * per_client);
    drop(first); assert_eq!(memory.used_bytes(), retained + per_client);
    drop(second); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_pkcs12_snapshot_loads_after_unlinking_and_releases_after_failure() {
    let root = support::Credential::new("client", false, None);
    let directory = tempfile::tempdir().unwrap(); let memory = MemoryBudget::new(128 << 20);
    let mut builder = Pkcs12::builder(); builder.name("client").pkey(&root.key).cert(&root.certificate);
    let bytes = builder.build2("password").unwrap().to_der().unwrap();
    for password in ["password", "wrong"] {
        let path = directory.path().join("client.p12"); std::fs::write(&path, &bytes).unwrap();
        let mut properties = properties(&root);
        properties.insert("ssl.keystore.location".into(), path.to_str().unwrap().into()); properties.insert("ssl.keystore.password".into(), password.into());
        let configuration = client(&properties, &memory); std::fs::remove_file(path).unwrap();
        let result = configuration.create::<BaseProducer>();
        assert_eq!(result.is_ok(), password == "password");
        drop(configuration); drop(result); assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn ca_and_crl_location_accept_the_same_mixed_certificate_crl_pem() {
    let root = support::Credential::new("root", false, None); let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("mixed.pem"); std::fs::write(&path, [root.certificate.to_pem().unwrap(), crl(&root)].concat()).unwrap();
    let memory = MemoryBudget::new(32 << 20);
    for name in ["ssl.ca.location", "ssl.crl.location"] {
        let mut properties = properties(&root); properties.insert(name.into(), path.to_str().unwrap().into());
        let configuration = client(&properties, &memory); let producer: BaseProducer = configuration.create().unwrap();
        drop(configuration); drop(producer); assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn main_client_assembly_captures_native_paths_before_the_first_native_client() {
    let root = support::Credential::new("root", false, None); let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("roots.pem"); std::fs::write(&path, root.certificate.to_pem().unwrap()).unwrap();
    let properties = BTreeMap::from([("security.protocol".into(), "SSL".into()), ("ssl.ca.location".into(), path.to_str().unwrap().into())]);
    let memory = MemoryBudget::new(32 << 20);
    let configuration = crate::client::prepare(&properties, &memory).unwrap(); std::fs::remove_file(path).unwrap();
    let producer: BaseProducer = configuration.create().unwrap();
    drop(configuration); assert!(memory.used_bytes() > 0); drop(producer); assert_eq!(memory.used_bytes(), 0);
}

fn chain_length(context: *mut openssl_sys::SSL_CTX) -> usize {
    let mut chain: *mut openssl_sys::stack_st_X509 = ptr::null_mut();
    const SSL_CTRL_GET_CHAIN_CERTS: libc::c_int = 115;
    unsafe { openssl_sys::SSL_CTX_ctrl(context, SSL_CTRL_GET_CHAIN_CERTS, 0, (&mut chain as *mut *mut openssl_sys::stack_st_X509).cast()); }
    if chain.is_null() { 0 } else { unsafe { openssl::stack::StackRef::<openssl::x509::X509>::from_ptr(chain).len() } }
}

#[test]
fn borrowed_certificate_chain_loader_clears_a_preexisting_extra_chain() {
    let root = support::Credential::new("root", false, None); let credential = support::Credential::new("client", false, Some(&root));
    let context = SslContextBuilder::new(SslMethod::tls_client()).unwrap();
    for (bytes, expected) in [
        ([credential.certificate.to_pem().unwrap(), root.certificate.to_pem().unwrap()].concat(), 1),
        (credential.certificate.to_pem().unwrap(), 0),
    ] {
        let bio = unsafe { openssl_sys::BIO_new_mem_buf(bytes.as_ptr().cast(), i32::try_from(bytes.len()).unwrap()) };
        assert!(!bio.is_null());
        let result = unsafe { SSL_CTX_use_certificate_chain_bio(context.as_ptr(), bio) };
        unsafe { openssl_sys::BIO_free_all(bio); }
        assert_eq!(result, 1); assert_eq!(chain_length(context.as_ptr()), expected);
    }
}

struct PanickingSource;
impl SslMaterialSource for PanickingSource {
    fn material(&self, _: SslMaterialKind, _: &CStr) -> SslMaterial<'_> { panic!("snapshot failure") }
}
impl SslContextConfigurer for PanickingSource {
    fn material_source(&self) -> Option<&dyn SslMaterialSource> { Some(self) }
    unsafe fn configure(&self, _: *mut c_void) -> std::result::Result<(), String> { panic!("material failure must precede configuration") }
}

#[test]
fn material_callback_panics_are_caught_and_never_fall_back_to_a_valid_file() {
    let root = support::Credential::new("root", false, None); let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("roots.pem"); std::fs::write(&path, root.certificate.to_pem().unwrap()).unwrap();
    let mut configuration = ClientConfig::new();
    configuration.set("security.protocol", "SSL").set("ssl.ca.location", path.to_str().unwrap()).set_ssl_context_configurer(Arc::new(PanickingSource));
    let error = configuration.create::<BaseProducer>().err().unwrap().to_string();
    assert!(error.contains("ssl.ca.location failed"), "{error}");
}

#[test]
fn empty_ca_snapshot_is_validated_before_a_later_inline_root() {
    let root = support::Credential::new("root", false, None); let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("empty.pem"); std::fs::write(&path, []).unwrap();
    let mut properties = properties(&root); properties.insert("ssl.ca.location".into(), path.to_str().unwrap().into());
    let memory = MemoryBudget::new(32 << 20); let configuration = client(&properties, &memory);
    std::fs::write(&path, root.certificate.to_pem().unwrap()).unwrap();
    let error = configuration.create::<BaseProducer>().err().unwrap().to_string();
    assert!(error.contains("ssl.ca.location failed"), "{error}");
    drop(configuration); assert_eq!(memory.used_bytes(), 0);
}

unsafe extern "C" { fn SSL_CTX_use_certificate_chain_bio(context: *mut openssl_sys::SSL_CTX, input: *mut openssl_sys::BIO) -> libc::c_int; }

mod admission;
