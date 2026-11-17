use std::{ffi::c_void, sync::{Arc, atomic::{AtomicUsize, Ordering}}};
use foreign_types::ForeignTypeRef;
use openssl::x509::store::X509StoreRef;
use rdkafka::{ClientConfig, client::{Client, DefaultClientContext}, config::SslContextConfigurer, types::RDKafkaType};

struct DefaultTrust;
impl SslContextConfigurer for DefaultTrust {
    unsafe fn configure(&self, _: *mut c_void) -> Result<(), String> { Ok(()) }
}

struct ReplacingTrust { calls: Arc<AtomicUsize> }
impl SslContextConfigurer for ReplacingTrust {
    fn replaces_default_trust(&self) -> bool { true }
    unsafe fn configure(&self, context: *mut c_void) -> Result<(), String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let store = unsafe { openssl_sys::SSL_CTX_get_cert_store(context.cast()) };
        if store.is_null() { return Err("TLS context has no certificate store".into()); }
        let store = unsafe { X509StoreRef::from_ptr(store) };
        if store.all_certificates().is_empty() { Ok(()) } else { Err("Automatic roots loaded before trust replacement".into()) }
    }
}

fn replacing_config(calls: &Arc<AtomicUsize>) -> ClientConfig {
    let mut configuration = ClientConfig::new();
    configuration.set("security.protocol", "SSL").set("ssl.ca.location", "probe");
    configuration.set_ssl_context_configurer(Arc::new(ReplacingTrust { calls: Arc::clone(calls) }));
    configuration
}

#[test]
fn automatic_trust_loading_is_enabled_when_the_capability_keeps_its_default() {
    let mut configuration = ClientConfig::new();
    configuration.set_ssl_context_configurer(Arc::new(DefaultTrust));
    let native = configuration.create_native_config().unwrap();
    assert_eq!(native.get("ssl.ctx.replaces.default.trust").unwrap(), "false");
    assert_eq!(native.clone().get("ssl.ctx.replaces.default.trust").unwrap(), "false");
}

#[test]
fn replacement_trust_skips_root_probing_after_the_source_configuration_is_dropped() {
    let calls = Arc::new(AtomicUsize::new(0));
    let configuration = replacing_config(&calls);
    let native = configuration.create_native_config().unwrap();
    let copy = native.clone();
    drop(native); drop(configuration);
    assert_eq!(copy.get("ssl.ctx.replaces.default.trust").unwrap(), "true");
    let client = Client::new(&ClientConfig::new(), copy, RDKafkaType::RD_KAFKA_PRODUCER, DefaultClientContext).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    drop(client);
}

#[test]
fn replacement_trust_preserves_explicit_native_ca_validation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut configuration = replacing_config(&calls);
    configuration.set("ssl.ca.pem", "invalid certificate");
    let native = configuration.create_native_config().unwrap();
    let client = Client::new(&ClientConfig::new(), native, RDKafkaType::RD_KAFKA_PRODUCER, DefaultClientContext);
    assert!(client.is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
