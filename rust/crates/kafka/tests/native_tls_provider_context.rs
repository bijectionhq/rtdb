use std::{ffi::{c_char, c_int, c_void}, ptr, sync::{Arc, atomic::{AtomicUsize, Ordering}}};
use foreign_types::{ForeignType, ForeignTypeRef};
use openssl::{lib_ctx::LibCtx, pkcs12::Pkcs12, provider::Provider, ssl::SslContextRef, symm::Cipher};
use rdkafka::{ClientConfig, config::SslContextConfigurer, producer::BaseProducer};
#[path = "support/kafka_tls.rs"]
mod support;

#[repr(C)]
struct Dispatch { id: c_int, function: Option<unsafe extern "C" fn()> }
static DISPATCH: [Dispatch; 1] = [Dispatch { id: 0, function: None }];
unsafe extern "C" fn initialize(_: *const c_void, _: *const Dispatch, output: *mut *const Dispatch, context: *mut *mut c_void) -> c_int {
    unsafe { *output = DISPATCH.as_ptr(); *context = ptr::null_mut(); }
    1
}

struct Configuration { calls: AtomicUsize, context: LibCtx }
impl Configuration {
    fn new() -> Arc<Self> {
        let context = LibCtx::new().unwrap();
        assert_eq!(unsafe { OSSL_PROVIDER_add_builtin(context.as_ptr(), b"rtdb-native-context-test\0".as_ptr().cast(), Some(initialize)) }, 1);
        Arc::new(Self { calls: AtomicUsize::new(0), context })
    }
}
impl SslContextConfigurer for Configuration {
    unsafe fn library_context(&self) -> *mut c_void { self.context.as_ptr().cast() }
    unsafe fn configure(&self, context: *mut c_void) -> Result<(), String> {
        let context = unsafe { SslContextRef::from_ptr(context.cast()) };
        let key = context.private_key().ok_or("Native private key was not loaded")?;
        let provider = Provider::load(Some(&self.context), "default").map_err(|error| error.to_string())?;
        assert_eq!(unsafe { EVP_PKEY_get0_provider(key.as_ptr()) }, provider.as_ptr().cast_const());
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[test]
fn native_providers_and_pem_or_pkcs12_keys_share_the_selected_library_context() {
    let credential = support::Credential::new("client", false, None);
    let directory = tempfile::tempdir().unwrap();
    let pem_file = directory.path().join("client.pem");
    let pem = credential.key.private_key_to_pem_pkcs8_passphrase(Cipher::aes_256_cbc(), b"key-password").unwrap();
    std::fs::write(&pem_file, &pem).unwrap();
    let store_file = directory.path().join("client.p12");
    let mut store = Pkcs12::builder(); store.name("client").pkey(&credential.key).cert(&credential.certificate);
    std::fs::write(&store_file, store.build2("store-password").unwrap().to_der().unwrap()).unwrap();
    for source in ["inline", "file", "pkcs12"] {
        let owner = Configuration::new();
        let mut configuration = ClientConfig::new();
        configuration.set("security.protocol", "SSL").set("ssl.providers", "default,rtdb-native-context-test").set_ssl_context_configurer(owner.clone());
        if source == "pkcs12" {
            configuration.set("ssl.keystore.location", store_file.to_str().unwrap()).set("ssl.keystore.password", "store-password");
        } else {
            configuration.set("ssl.certificate.pem", String::from_utf8(credential.certificate.to_pem().unwrap()).unwrap()).set("ssl.key.password", "key-password");
            if source == "inline" { configuration.set("ssl.key.pem", std::str::from_utf8(&pem).unwrap()); }
            else { configuration.set("ssl.key.location", pem_file.to_str().unwrap()); }
        }
        let producer: BaseProducer = configuration.create().unwrap();
        assert_eq!(owner.calls.load(Ordering::SeqCst), 1);
        let weak = Arc::downgrade(&owner); drop(owner); drop(configuration);
        assert!(weak.upgrade().is_some());
        drop(producer); assert!(weak.upgrade().is_none());
    }
}

#[test]
fn failed_native_pkcs12_decoding_releases_its_selected_context_owner() {
    let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("invalid.p12");
    for bytes in [&[][..], &[0x30, 0x20, 2, 1, 3][..]] {
        std::fs::write(&path, bytes).unwrap();
        let owner = Configuration::new(); let weak = Arc::downgrade(&owner);
        let mut configuration = ClientConfig::new();
        configuration.set("security.protocol", "SSL").set("ssl.providers", "default,rtdb-native-context-test")
            .set("ssl.keystore.location", path.to_str().unwrap()).set("ssl.keystore.password", "password")
            .set_ssl_context_configurer(owner);
        assert!(configuration.create::<BaseProducer>().is_err());
        drop(configuration); assert!(weak.upgrade().is_none());
    }
}

unsafe extern "C" {
    fn OSSL_PROVIDER_add_builtin(context: *mut openssl_sys::OSSL_LIB_CTX, name: *const c_char, initialize: Option<unsafe extern "C" fn(*const c_void, *const Dispatch, *mut *const Dispatch, *mut *mut c_void) -> c_int>) -> c_int;
    fn EVP_PKEY_get0_provider(key: *const openssl_sys::EVP_PKEY) -> *const openssl_sys::OSSL_PROVIDER;
}
