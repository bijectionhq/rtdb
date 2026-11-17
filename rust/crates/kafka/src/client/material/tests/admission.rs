use super::*;
use std::sync::{Mutex, atomic::{AtomicUsize, Ordering}};
use openssl::ssl::SslContext;

#[derive(Clone, Copy)]
enum Outcome { Success, ConfigureError, LoadedPanic }

struct Observations { events: Mutex<Vec<&'static str>>, material_calls: AtomicUsize, memory: Arc<MemoryBudget>, baseline: usize }
struct ObservedSource { snapshot: Arc<Configuration>, observations: Arc<Observations>, outcome: Outcome }
struct ObservedLease { inner: Box<dyn SslMaterialLease>, observations: Arc<Observations>, outcome: Outcome }
struct NativeDrop(Arc<Observations>);

impl SslMaterialLease for ObservedLease {
    fn loaded(&mut self) {
        self.observations.events.lock().unwrap().push("loaded");
        if matches!(self.outcome, Outcome::LoadedPanic) { panic!("loaded callback failure"); }
        self.inner.loaded();
    }
}
impl Drop for ObservedLease {
    fn drop(&mut self) { self.observations.events.lock().unwrap().push("lease-drop"); }
}
impl Drop for NativeDrop {
    fn drop(&mut self) {
        let event = if self.0.memory.used_bytes() > self.0.baseline { "native-drop" } else { "native-drop-uncharged" };
        if let Ok(mut events) = self.0.events.lock() { events.push(event); }
    }
}
impl SslMaterialSource for ObservedSource {
    fn admit_context(&self) -> std::result::Result<Option<Box<dyn SslMaterialLease>>, String> {
        let inner = self.snapshot.admit_context()?.expect("regular-file decode lease");
        self.observations.events.lock().unwrap().push("admit");
        Ok(Some(Box::new(ObservedLease { inner, observations: Arc::clone(&self.observations), outcome: self.outcome })))
    }
    fn material(&self, kind: SslMaterialKind, path: &CStr) -> SslMaterial<'_> {
        self.observations.material_calls.fetch_add(1, Ordering::SeqCst);
        self.snapshot.material(kind, path)
    }
}
impl SslContextConfigurer for ObservedSource {
    fn material_source(&self) -> Option<&dyn SslMaterialSource> { Some(self) }
    fn replaces_default_trust(&self) -> bool { self.snapshot.replaces_default_trust() }
    unsafe fn library_context(&self) -> *mut c_void { unsafe { self.snapshot.library_context() } }
    unsafe fn configure(&self, context: *mut c_void) -> std::result::Result<(), String> {
        self.observations.events.lock().unwrap().push("configure");
        let context = context.cast::<openssl_sys::SSL_CTX>();
        assert_eq!(unsafe { openssl_sys::SSL_CTX_up_ref(context) }, 1);
        let mut borrowed = unsafe { SslContextBuilder::from_ptr(context) };
        borrowed.set_ex_data(SslContext::new_ex_index::<NativeDrop>().unwrap(), NativeDrop(Arc::clone(&self.observations)));
        if matches!(self.outcome, Outcome::ConfigureError) { return Err("configured failure".into()); }
        unsafe { self.snapshot.configure(context.cast()) }
    }
}

fn observed(properties: &BTreeMap<String, String>, memory: &Arc<MemoryBudget>, outcome: Outcome) -> (ClientConfig, Arc<Observations>) {
    let snapshot = Configuration::capture(owner(memory), properties, &mut |bytes| memory.reserve(bytes)).unwrap();
    let observations = Arc::new(Observations { events: Mutex::new(Vec::new()), material_calls: AtomicUsize::new(0), memory: Arc::clone(memory), baseline: memory.used_bytes() });
    let mut client = crate::client_config(properties);
    client.set_ssl_context_configurer(Arc::new(ObservedSource { snapshot, observations: Arc::clone(&observations), outcome }));
    (client, observations)
}

#[test]
fn admission_denial_precedes_native_file_parsing_and_configuration() {
    let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("invalid.key");
    std::fs::write(&path, b"invalid PEM").unwrap();
    let properties = BTreeMap::from([("security.protocol".into(), "SSL".into()), ("ssl.key.location".into(), path.to_str().unwrap().into())]);
    let memory = MemoryBudget::new(4 << 20);
    let (configuration, observations) = observed(&properties, &memory, Outcome::Success);
    let error = configuration.create::<BaseProducer>().err().unwrap().to_string();
    assert!(error.contains("rtdb/memory-budget-exceeded"), "{error}");
    assert_eq!(observations.material_calls.load(Ordering::SeqCst), 0);
    assert!(observations.events.lock().unwrap().is_empty());
    assert_eq!(memory.used_bytes(), observations.baseline);
    drop(configuration); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn built_in_key_decode_scratch_is_released_after_native_creation() {
    let root = support::Credential::new("root", false, None);
    let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("private.pem");
    std::fs::write(&path, root.key.private_key_to_pem_pkcs8_passphrase(Cipher::aes_256_cbc(), b"password").unwrap()).unwrap();
    let mut properties = properties(&root);
    properties.insert("ssl.key.location".into(), path.to_str().unwrap().into());
    properties.insert("ssl.key.password".into(), "password".into());
    properties.insert("ssl.certificate.pem".into(), String::from_utf8(root.certificate.to_pem().unwrap()).unwrap());
    let memory = MemoryBudget::new(128 << 20);
    let (configuration, observations) = observed(&properties, &memory, Outcome::Success);
    let producer: BaseProducer = configuration.create().unwrap();
    let retained = memory.used_bytes();
    assert!(retained > observations.baseline);
    assert_eq!(memory.peak_bytes() - retained, PBE_SCRATCH_BYTES);
    assert_eq!(*observations.events.lock().unwrap(), ["admit", "configure", "loaded"]);
    drop(producer);
    assert_eq!(*observations.events.lock().unwrap(), ["admit", "configure", "loaded", "native-drop", "lease-drop"]);
    assert_eq!(memory.used_bytes(), observations.baseline);
    drop(configuration); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn failed_native_creation_releases_admission_before_or_after_tls_setup() {
    let root = support::Credential::new("root", false, None);
    let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("roots.pem");
    std::fs::write(&path, root.certificate.to_pem().unwrap()).unwrap();
    let mut properties = properties(&root); properties.insert("ssl.ca.location".into(), path.to_str().unwrap().into());
    for outcome in [Outcome::Success, Outcome::ConfigureError] {
        let memory = MemoryBudget::new(128 << 20);
        let (mut configuration, observations) = observed(&properties, &memory, outcome);
        if matches!(outcome, Outcome::Success) { configuration.set("enable.idempotence", "true").set("acks", "0"); }
        assert!(configuration.create::<BaseProducer>().is_err());
        let expected: &[&str] = if matches!(outcome, Outcome::Success) { &["admit", "lease-drop"] } else { &["admit", "configure", "native-drop", "lease-drop"] };
        assert_eq!(observations.events.lock().unwrap().as_slice(), expected);
        assert_eq!(memory.used_bytes(), observations.baseline);
        drop(configuration); assert_eq!(memory.used_bytes(), 0);
    }
}

#[test]
fn loaded_callback_panic_destroys_native_tls_before_dropping_admission() {
    let root = support::Credential::new("root", false, None);
    let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("roots.pem");
    std::fs::write(&path, root.certificate.to_pem().unwrap()).unwrap();
    let mut properties = properties(&root); properties.insert("ssl.ca.location".into(), path.to_str().unwrap().into());
    let memory = MemoryBudget::new(128 << 20);
    let (configuration, observations) = observed(&properties, &memory, Outcome::LoadedPanic);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| configuration.create::<BaseProducer>()));
    assert!(result.is_err());
    assert_eq!(*observations.events.lock().unwrap(), ["admit", "configure", "loaded", "native-drop", "lease-drop"]);
    assert_eq!(memory.used_bytes(), observations.baseline);
    drop(configuration); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn each_native_configuration_copy_admits_its_own_decoded_material() {
    let root = support::Credential::new("root", false, None);
    let directory = tempfile::tempdir().unwrap(); let path = directory.path().join("roots.pem");
    std::fs::write(&path, root.certificate.to_pem().unwrap()).unwrap();
    let mut properties = properties(&root); properties.insert("ssl.ca.location".into(), path.to_str().unwrap().into());
    let memory = MemoryBudget::new(128 << 20);
    let (configuration, observations) = observed(&properties, &memory, Outcome::Success);
    let native = configuration.create_native_config().unwrap(); let copy = native.clone();
    let first = Client::new(&ClientConfig::new(), native, RDKafkaType::RD_KAFKA_PRODUCER, DefaultClientContext).unwrap();
    let per_client = memory.used_bytes() - observations.baseline;
    let second = Client::new(&ClientConfig::new(), copy, RDKafkaType::RD_KAFKA_PRODUCER, DefaultClientContext).unwrap();
    assert_eq!(memory.used_bytes(), observations.baseline + 2 * per_client);
    assert_eq!(observations.events.lock().unwrap().iter().filter(|event| **event == "admit").count(), 2);
    drop(first); assert_eq!(memory.used_bytes(), observations.baseline + per_client);
    drop(second); assert_eq!(memory.used_bytes(), observations.baseline);
    drop(configuration); assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn omitted_material_has_no_lease_and_directories_release_the_constructor_lease() {
    let directory = tempfile::tempdir().unwrap();
    for properties in [BTreeMap::new(), BTreeMap::from([("ssl.ca.location".into(), directory.path().to_str().unwrap().into())])] {
        let memory = MemoryBudget::new(128 << 20);
        let snapshot = Configuration::capture(owner(&memory), &properties, &mut |bytes| memory.reserve(bytes)).unwrap();
        let baseline = memory.used_bytes();
        let lease = snapshot.admit_context().unwrap();
        if properties.is_empty() { assert!(lease.is_none()); }
        else {
            let mut lease = lease.unwrap(); assert!(memory.used_bytes() > baseline);
            lease.loaded(); assert_eq!(memory.used_bytes(), baseline);
        }
        drop(snapshot); assert_eq!(memory.used_bytes(), 0);
    }
}
