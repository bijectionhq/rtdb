use std::{ffi::c_void, sync::{Arc, atomic::{AtomicUsize, Ordering}}};
use rdkafka::{ClientConfig, admin::AdminClient, client::{Client, DefaultClientContext}, config::SslContextConfigurer, consumer::BaseConsumer, producer::BaseProducer, types::RDKafkaType};

enum Outcome { Success, Error, Panic }
struct Configuration { calls: Arc<AtomicUsize>, drops: Arc<AtomicUsize>, outcome: Outcome }
impl Drop for Configuration { fn drop(&mut self) { self.drops.fetch_add(1, Ordering::SeqCst); } }
impl SslContextConfigurer for Configuration {
    unsafe fn configure(&self, _: *mut c_void) -> Result<(), String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        match self.outcome { Outcome::Success => Ok(()), Outcome::Error => Err("configured TLS failure".into()), Outcome::Panic => panic!("configured TLS panic") }
    }
}
fn config(outcome: Outcome) -> (ClientConfig, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0)); let drops = Arc::new(AtomicUsize::new(0));
    let mut config = ClientConfig::new();
    config.set("security.protocol", "SSL").set("group.id", "tls-ownership");
    config.set_ssl_context_configurer(Arc::new(Configuration { calls: Arc::clone(&calls), drops: Arc::clone(&drops), outcome }));
    (config, calls, drops)
}

#[test]
fn native_ssl_configuration_copies_survive_the_source_config_and_release_after_the_client() {
    let (configuration, calls, drops) = config(Outcome::Success);
    let native = configuration.create_native_config().unwrap();
    let duplicate = native.clone(); drop(native); drop(configuration);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let client = Client::new(&ClientConfig::new(), duplicate, RDKafkaType::RD_KAFKA_PRODUCER, DefaultClientContext).unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1); assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(client); assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn producers_consumers_and_admin_clients_retain_the_same_tls_capability_until_destroyed() {
    let (configuration, calls, drops) = config(Outcome::Success);
    let copy = configuration.clone();
    let producer: BaseProducer = configuration.create().unwrap();
    let consumer: BaseConsumer = copy.create().unwrap();
    let admin: AdminClient<DefaultClientContext> = copy.create().unwrap();
    drop(configuration); drop(copy);
    assert_eq!(calls.load(Ordering::SeqCst), 3); assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(producer); drop(consumer); assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(admin); assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn tls_hook_errors_and_panics_release_the_failed_native_configuration() {
    for outcome in [Outcome::Error, Outcome::Panic] {
        let (configuration, calls, drops) = config(outcome);
        let native = configuration.create_native_config().unwrap(); drop(configuration);
        let client = Client::new(&ClientConfig::new(), native, RDKafkaType::RD_KAFKA_PRODUCER, DefaultClientContext);
        assert!(client.is_err()); assert_eq!(calls.load(Ordering::SeqCst), 1); assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn failures_before_tls_setup_release_the_callback_without_invoking_it() {
    let (mut configuration, calls, drops) = config(Outcome::Success);
    configuration.set("unknown.configuration", "invalid");
    assert!(configuration.create_native_config().is_err()); drop(configuration);
    assert_eq!(calls.load(Ordering::SeqCst), 0); assert_eq!(drops.load(Ordering::SeqCst), 1);
    let (mut configuration, calls, drops) = config(Outcome::Success);
    configuration.set("enable.idempotence", "true").set("acks", "0");
    let native = configuration.create_native_config().unwrap(); drop(configuration);
    assert!(Client::new(&ClientConfig::new(), native, RDKafkaType::RD_KAFKA_PRODUCER, DefaultClientContext).is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0); assert_eq!(drops.load(Ordering::SeqCst), 1);
}
