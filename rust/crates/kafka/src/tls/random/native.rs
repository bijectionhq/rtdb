use std::{ffi::{c_char, c_int, c_long, c_uint, c_void}, ptr, sync::{Arc, Mutex}};

use foreign_types::ForeignType;
use openssl::{lib_ctx::LibCtx, pkey::{PKey, Private}, provider::Provider, ssl::SslContextBuilder};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::Result;
use zeroize::Zeroizing;

use super::{Algorithm, Random};

#[cfg(test)]
#[path = "../../../tests/support/kafka_tls.rs"]
mod tls_test_support;

pub(crate) struct Context {
    _random_provider: Provider,
    _default_provider: Provider,
    context: LibCtx,
    _charge: Arc<MemoryReservation>,
}

impl std::fmt::Debug for Context {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.write_str("SecureRandomContext") }
}

impl Context {
    pub(crate) fn new(name: Option<&str>, budget: &Arc<MemoryBudget>) -> Result<Arc<Self>> {
        Self::new_typed(name, budget).map_err(super::super::TlsConfigError::into_anomaly)
    }

    pub(crate) fn new_typed(name: Option<&str>, budget: &Arc<MemoryBudget>) -> super::super::TlsConfigResult<Arc<Self>> {
        let algorithm = match name {
            Some(name) => Algorithm::parse(name).map_err(super::super::TlsConfigError::from_invalid_anomaly)?,
            None if cfg!(unix) && std::fs::File::open("/dev/random").is_ok() && std::fs::File::open("/dev/urandom").is_ok() => Algorithm::Native,
            None => Algorithm::Drbg,
        };
        let charge = budget.reserve(262_144).map_err(super::super::TlsConfigError::Other)?;
        let mut random = Random::new(algorithm, budget)?;
        random.fill(Zeroizing::new([0; 4]).as_mut()).map_err(super::super::TlsConfigError::from_invalid_anomaly)?;
        let context = LibCtx::new().map_err(super::super::TlsConfigError::invalid)?;
        let default_provider = Provider::load(Some(&context), "default").map_err(super::super::TlsConfigError::invalid)?;
        if unsafe { OSSL_PROVIDER_add_builtin(context.as_ptr(), c"rtdb-secure-random".as_ptr(), Some(initialize)) } != 1 {
            return Err(super::super::TlsConfigError::invalid("Unable to register SecureRandom provider"));
        }
        let random_provider = Provider::load(Some(&context), "rtdb-secure-random").map_err(|_| super::super::TlsConfigError::invalid("Unable to initialize SecureRandom algorithm"))?;
        let state = unsafe { OSSL_PROVIDER_get0_provider_ctx(random_provider.as_ptr()) };
        if state.is_null() { return Err(super::super::TlsConfigError::invalid("Unable to initialize SecureRandom provider state")); }
        let state = unsafe { &*state.cast::<Mutex<Option<Random>>>() };
        *state.lock().map_err(|_| super::super::TlsConfigError::invalid("SecureRandom state is unavailable"))? = Some(random);
        if unsafe { RAND_set1_random_provider(context.as_ptr(), random_provider.as_ptr()) } != 1 {
            return Err(super::super::TlsConfigError::invalid("Unable to select SecureRandom provider"));
        }
        Ok(Arc::new(Self { _random_provider: random_provider, _default_provider: default_provider, context, _charge: charge }))
    }

    pub(crate) fn as_ptr(&self) -> *mut openssl_sys::OSSL_LIB_CTX { self.context.as_ptr() }

    // This owner must outlive the SSL context, its connections and their cached native keys.
    pub(crate) unsafe fn configure_signatures(&self, context: &mut SslContextBuilder) {
        unsafe { SSL_CTX_set_signature_libctx(context.as_ptr(), self.as_ptr()); }
    }

    #[cfg(test)]
    pub(super) fn fill(&self, output: &mut [u8]) -> Result<()> {
        if unsafe { RAND_bytes_ex(self.as_ptr(), output.as_mut_ptr(), output.len(), 0) } == 1 { Ok(()) }
        else { Err(super::super::invalid("SecureRandom generation failed")) }
    }

    // Destroy the returned key and every native key copy before this context; their operations borrow its library context.
    pub(crate) unsafe fn private_key_from_pkcs8(&self, bytes: &[u8]) -> Result<PKey<Private>> {
        let length = c_long::try_from(bytes.len()).map_err(super::super::invalid)?;
        unsafe {
            let mut position = bytes.as_ptr();
            let info = openssl_sys::d2i_PKCS8_PRIV_KEY_INFO(ptr::null_mut(), &mut position, length);
            if info.is_null() { return Err(super::super::invalid("Invalid PKCS8 private key")); }
            let key = EVP_PKCS82PKEY_ex(info, self.as_ptr(), ptr::null());
            openssl_sys::PKCS8_PRIV_KEY_INFO_free(info);
            if key.is_null() { return Err(super::super::invalid("Unable to load private key into SecureRandom context")); }
            Ok(PKey::from_ptr(key))
        }
    }
}

impl Drop for Context {
    fn drop(&mut self) { unsafe { RAND_set1_random_provider(self.as_ptr(), ptr::null_mut()); } }
}

#[repr(C)]
struct Dispatch { id: c_int, function: Option<unsafe extern "C" fn()> }

type ProviderInit = unsafe extern "C" fn(*const c_void, *const Dispatch, *mut *const Dispatch, *mut *mut c_void) -> c_int;

static DISPATCH: [Dispatch; 3] = [
    Dispatch { id: 1024, function: Some(unsafe { std::mem::transmute::<unsafe extern "C" fn(*mut c_void), unsafe extern "C" fn()>(teardown) }) },
    Dispatch { id: 1032, function: Some(unsafe { std::mem::transmute::<unsafe extern "C" fn(*mut c_void, c_int, *mut c_void, usize, c_uint) -> c_int, unsafe extern "C" fn()>(generate) }) },
    Dispatch { id: 0, function: None },
];

unsafe extern "C" fn initialize(_: *const c_void, _: *const Dispatch, functions: *mut *const Dispatch, provider: *mut *mut c_void) -> c_int {
    match std::panic::catch_unwind(|| {
        Box::new(Mutex::new(None::<Random>))
    }) {
        Ok(random) => {
            unsafe { *provider = Box::into_raw(random).cast(); *functions = DISPATCH.as_ptr(); }
            1
        }
        _ => 0,
    }
}

unsafe extern "C" fn teardown(provider: *mut c_void) {
    if !provider.is_null() { unsafe { drop(Box::from_raw(provider.cast::<Mutex<Option<Random>>>())); } }
}

unsafe extern "C" fn generate(provider: *mut c_void, _: c_int, output: *mut c_void, length: usize, _: c_uint) -> c_int {
    if length > isize::MAX as usize || provider.is_null() || (length > 0 && output.is_null()) { return 0; }
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let random = unsafe { &*provider.cast::<Mutex<Option<Random>>>() };
        let output = if length == 0 { &mut [] } else { unsafe { std::slice::from_raw_parts_mut(output.cast::<u8>(), length) } };
        random.lock().map_err(|_| super::super::invalid("SecureRandom state is unavailable"))?.as_mut().ok_or_else(|| super::super::invalid("SecureRandom provider is not configured"))?.fill(output)
    })) {
        Ok(Ok(())) => 1,
        _ => 0,
    }
}

unsafe extern "C" {
    fn OSSL_PROVIDER_add_builtin(context: *mut openssl_sys::OSSL_LIB_CTX, name: *const c_char, initialize: Option<ProviderInit>) -> c_int;
    fn RAND_set1_random_provider(context: *mut openssl_sys::OSSL_LIB_CTX, provider: *mut openssl_sys::OSSL_PROVIDER) -> c_int;
    #[cfg(test)]
    fn RAND_bytes_ex(context: *mut openssl_sys::OSSL_LIB_CTX, output: *mut u8, length: usize, strength: c_uint) -> c_int;
    fn EVP_PKCS82PKEY_ex(info: *const openssl_sys::PKCS8_PRIV_KEY_INFO, context: *mut openssl_sys::OSSL_LIB_CTX, properties: *const c_char) -> *mut openssl_sys::EVP_PKEY;
    fn OSSL_PROVIDER_get0_provider_ctx(provider: *const openssl_sys::OSSL_PROVIDER) -> *mut c_void;
    fn SSL_CTX_set_signature_libctx(context: *mut openssl_sys::SSL_CTX, library: *mut openssl_sys::OSSL_LIB_CTX);
}

#[cfg(test)]
pub(super) fn test_independent_streams() {
    unsafe extern "C" {
        fn OSSL_PROVIDER_get0_provider_ctx(provider: *const openssl_sys::OSSL_PROVIDER) -> *mut c_void;
        fn RAND_priv_bytes_ex(context: *mut openssl_sys::OSSL_LIB_CTX, output: *mut u8, length: usize, strength: c_uint) -> c_int;
    }
    let first = Context::new(Some("SHA1PRNG"), &rtdb_columnar::MemoryBudget::new(1 << 24)).unwrap();
    let second = Context::new(Some("SHA1PRNG"), &rtdb_columnar::MemoryBudget::new(1 << 24)).unwrap();
    for context in [&first, &second] {
        let provider = unsafe { OSSL_PROVIDER_get0_provider_ctx(context._random_provider.as_ptr()) };
        let random = unsafe { &*provider.cast::<Mutex<Option<Random>>>() };
        *random.lock().unwrap() = Some(Random::Sha1(super::Sha1::new(b"provider fixture")));
    }
    let mut expected = [0; 64];
    let mut actual = [0; 64];
    second.fill(&mut expected).unwrap();
    first.fill(&mut actual[..17]).unwrap();
    assert_eq!(unsafe { RAND_priv_bytes_ex(first.as_ptr(), actual[17..].as_mut_ptr(), actual.len() - 17, 0) }, 1);
    assert_eq!(actual, expected);
    assert_ne!(first.as_ptr(), second.as_ptr());
}

#[cfg(test)]
pub(super) fn test_librdkafka_context() {
    use rdkafka::{ClientConfig, client::{Client, DefaultClientContext}, config::SslContextConfigurer, types::RDKafkaType};
    unsafe extern "C" { fn OSSL_PROVIDER_get0_provider_ctx(provider: *const openssl_sys::OSSL_PROVIDER) -> *mut c_void; }
    struct Configuration(Arc<Context>);
    impl SslContextConfigurer for Configuration {
        unsafe fn library_context(&self) -> *mut c_void { self.0.as_ptr().cast() }
        unsafe fn configure(&self, _: *mut c_void) -> std::result::Result<(), String> { Ok(()) }
    }
    let context = Context::new(Some("SHA1PRNG"), &rtdb_columnar::MemoryBudget::new(1 << 24)).unwrap();
    let provider = unsafe { OSSL_PROVIDER_get0_provider_ctx(context._random_provider.as_ptr()) };
    let random = unsafe { &*provider.cast::<Mutex<Option<Random>>>() };
    let before = match &*random.lock().unwrap() { Some(Random::Sha1(state)) => state.state, _ => unreachable!() };
    let weak = Arc::downgrade(&context);
    let mut configuration = ClientConfig::new();
    configuration.set("security.protocol", "SSL");
    configuration.set_ssl_context_configurer(Arc::new(Configuration(context)));
    let native = configuration.create_native_config().unwrap();
    let duplicate = native.clone();
    drop(native);
    drop(configuration);
    let client = Client::new(&ClientConfig::new(), duplicate, RDKafkaType::RD_KAFKA_PRODUCER, DefaultClientContext).unwrap();
    let after = match &*random.lock().unwrap() { Some(Random::Sha1(state)) => state.state, _ => unreachable!() };
    assert_ne!(before, after);
    assert!(weak.upgrade().is_some());
    drop(client);
    assert!(weak.upgrade().is_none());
}

#[cfg(test)]
pub(super) fn test_sha1_state(context: &Context) -> [u8; 20] {
    unsafe extern "C" { fn OSSL_PROVIDER_get0_provider_ctx(provider: *const openssl_sys::OSSL_PROVIDER) -> *mut c_void; }
    let provider = unsafe { OSSL_PROVIDER_get0_provider_ctx(context._random_provider.as_ptr()) };
    let random = unsafe { &*provider.cast::<Mutex<Option<Random>>>() };
    match &*random.lock().unwrap() { Some(Random::Sha1(random)) => random.state, _ => unreachable!() }
}

#[cfg(test)]
pub(super) fn test_signature_contexts() {
    use std::{net::TcpStream, time::Duration};
    use openssl::ssl::{Ssl, SslContext, SslStream, SslVersion};
    unsafe extern "C" {
        fn SSL_CTX_new_ex(library: *mut openssl_sys::OSSL_LIB_CTX, properties: *const c_char, method: *const openssl_sys::SSL_METHOD) -> *mut openssl_sys::SSL_CTX;
        fn SSL_CTX_get0_signature_libctx(context: *const openssl_sys::SSL_CTX) -> *mut openssl_sys::OSSL_LIB_CTX;
    }
    struct Owners { tls: SslContext, _key: PKey<Private>, selected: Arc<Context>, signature: Arc<Context> }
    struct Connection { stream: SslStream<TcpStream>, _owners: Arc<Owners> }
    let authority = tls_test_support::Credential::new("random root", false, None);
    for version in [SslVersion::TLS1_2, SslVersion::TLS1_3] {
        for ec in [false, true] {
            let server = tls_test_support::Credential::new("localhost", ec, Some(&authority));
            let client = tls_test_support::Credential::new("client", ec, Some(&authority));
            let budget = MemoryBudget::new(1 << 24);
            let selected = Context::new(Some("SHA1PRNG"), &budget).unwrap();
            let signature = Context::new(Some("SHA1PRNG"), &budget).unwrap();
            let pointer = unsafe { SSL_CTX_new_ex(selected.as_ptr(), ptr::null(), openssl_sys::TLS_client_method()) };
            assert!(!pointer.is_null());
            let mut tls = unsafe { SslContextBuilder::from_ptr(pointer) };
            assert_eq!(unsafe { SSL_CTX_get0_signature_libctx(tls.as_ptr()) }, selected.as_ptr());
            unsafe { signature.configure_signatures(&mut tls); }
            assert_eq!(unsafe { SSL_CTX_get0_signature_libctx(tls.as_ptr()) }, signature.as_ptr());
            tls.set_min_proto_version(Some(version)).unwrap();
            tls.set_max_proto_version(Some(version)).unwrap();
            tls.set_certificate(&client.certificate).unwrap();
            tls.set_private_key(&client.key).unwrap();
            tls.cert_store_mut().add_cert(authority.certificate.clone()).unwrap();
            let before = test_sha1_state(&signature);
            let weak = Arc::downgrade(&signature);
            let owners = Arc::new(Owners { tls: tls.build(), _key: client.key, selected, signature });
            let signatures = if ec { "ecdsa_secp256r1_sha256" } else { "rsa_pss_rsae_sha256" };
            let (address, server) = tls_test_support::broker(&server, &authority, version, Some(signatures));
            let socket = TcpStream::connect(address).unwrap();
            socket.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            socket.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
            let stream = SslStream::new(Ssl::new(&owners.tls).unwrap(), socket).unwrap();
            let mut connection = Connection { stream, _owners: Arc::clone(&owners) };
            connection.stream.connect().unwrap();
            assert_ne!(before, test_sha1_state(&owners.signature));
            assert_ne!(owners.selected.as_ptr(), owners.signature.as_ptr());
            assert_eq!(server.join().unwrap().unwrap().certificate, client.certificate.to_der().unwrap());
            drop(owners);
            assert!(weak.upgrade().is_some());
            drop(connection);
            assert!(weak.upgrade().is_none());
            assert_eq!(budget.used_bytes(), 0);
        }
    }
}
