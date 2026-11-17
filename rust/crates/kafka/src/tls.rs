use std::{collections::BTreeMap, ffi::c_void, io::Read, sync::Arc};
use foreign_types::{ForeignType, ForeignTypeRef};
use openssl::{asn1::Asn1Time, pkey::{PKey, Private}, ssl::{SslContextBuilder, SslOptions, SslRef, SslVersion}, stack::Stack, x509::{X509, store::X509StoreBuilder}};
use rtdb_columnar::MemoryReservation;
use rtdb_tls::{KeyStore, StoreFormat, StoreUse};
use rtdb_types::{Anomaly, Result};
use zeroize::Zeroizing;

mod policy;
use policy::Policy;
pub(crate) mod keys;
use keys::{Algorithm, KeyManager, KeyType};
pub(crate) mod trust;
pub(crate) mod validation;
pub(crate) mod security;
pub(crate) mod request;
mod public_key;
pub(crate) mod random;

pub(crate) struct NativeTls {
    identities: Vec<ClientIdentity>,
    random: Arc<random::Context>,
    signature_random: Arc<random::Context>,
    key_manager: KeyManager,
    trust: Option<Vec<X509>>,
    simple_trust: Option<validation::SimpleTrust>,
    policy: Policy,
    reservations: Vec<Arc<MemoryReservation>>,
}
struct ClientIdentity { certificate: X509, key: PKey<Private>, chain: Stack<X509>, kind: KeyType, eligible: bool }

impl NativeTls {
    pub(crate) fn from_java(options: &BTreeMap<String, String>, reserve: &mut impl FnMut(usize) -> Result<Arc<MemoryReservation>>) -> Result<Option<Arc<Self>>> {
        if !options.get("security.protocol").is_some_and(|protocol| protocol.eq_ignore_ascii_case("SSL") || protocol.eq_ignore_ascii_case("SASL_SSL")) { return Ok(None); }
        let bytes = options.iter().try_fold(65_536usize, |bytes, (name, value)| bytes.checked_add(name.len()).and_then(|bytes| bytes.checked_add(value.len())).ok_or_else(overflow))?;
        let mut reservations = vec![reserve(bytes.checked_mul(16).ok_or_else(overflow)?)?];
        validate_provider(options.get("ssl.provider").map(|name| name.trim_matches(|character| character <= '\u{20}')))?;
        let policy = Policy::from_java(options)?;
        let algorithm = Algorithm::parse(options.get("ssl.keymanager.algorithm").map(|name| name.trim_matches(|character| character <= '\u{20}')))?;
        let trust_algorithm = validation::Algorithm::parse(options.get("ssl.trustmanager.algorithm").map(|name| name.trim_matches(|character| character <= '\u{20}')))?;
        let random = random::Context::new(options.get("ssl.secure.random.implementation").map(|name| name.trim_matches(|character| character <= '\u{20}')), reservations[0].budget())?;
        let signature_random = random::Context::new(None, reservations[0].budget())?;
        let mut key_manager = KeyManager::new(algorithm, &[]);
        let mut identities = Vec::new();
        let mut trust = None;
        for (store, usage) in [("truststore", StoreUse::Trust), ("keystore", StoreUse::OptionalIdentity)] {
            let material = material(options, store, usage, reserve)?;
            let material = if usage == StoreUse::Trust && material.is_none() && !trust::native_override(options) {
                Some(trust::defaults(reserve)?)
            } else { material };
            if let Some(material) = material {
                reservations.push(material.reservation());
                match usage {
                    StoreUse::Trust => {
                        let certificates = material.trusted_certificates().iter().map(|certificate| X509::from_der(certificate).map_err(invalid)).collect::<Result<Vec<_>>>()?;
                        trust = Some(certificates);
                    }
                    StoreUse::Identity | StoreUse::OptionalIdentity => {
                        key_manager = KeyManager::new(algorithm, material.identities());
                        for identity in material.identities() {
                            let certificate = X509::from_der(&identity.certificates()[0]).map_err(invalid)?;
                            let kind = KeyType::of(&certificate)?;
                            let key = unsafe { random.private_key_from_pkcs8(identity.private_key_pkcs8())? };
                            let mut chain = Stack::new().map_err(invalid)?;
                            for certificate in &identity.certificates()[1..] { chain.push(X509::from_der(certificate).map_err(invalid)?).map_err(invalid)?; }
                            let eligible = algorithm == Algorithm::SunX509 || {
                                let certificates = std::iter::once(certificate.as_ref()).chain(chain.iter()).collect::<Vec<_>>();
                                security::verify_client_chain(&certificates).is_ok()
                            };
                            identities.push(ClientIdentity { certificate, key, chain, kind, eligible });
                        }
                    }
                }
            }
        }
        let simple_trust = if trust_algorithm == validation::Algorithm::SunX509 {
            let anchors = trust.as_ref().ok_or_else(|| invalid("SunX509 trust management requires Java-style trust material"))?.clone();
            Some(validation::SimpleTrust::new(anchors, Arc::clone(&reservations[0]))?)
        } else { None };
        Ok(Some(Arc::new(Self { identities, random, signature_random, key_manager, trust, simple_trust, policy, reservations })))
    }

    pub(crate) fn reservations(&self) -> &[Arc<MemoryReservation>] { &self.reservations }

    // The installed certificate callback borrows self until this context and its SSL objects are destroyed.
    unsafe fn configure_context(&self, context: &mut SslContextBuilder) -> Result<()> {
        unsafe { self.signature_random.configure_signatures(context); }
        context.set_min_proto_version(Some(SslVersion::TLS1_2)).map_err(invalid)?;
        context.set_max_proto_version(Some(SslVersion::TLS1_3)).map_err(invalid)?;
        context.clear_options(SslOptions::NO_TLSV1_2 | SslOptions::NO_TLSV1_3);
        self.policy.apply(context)?;
        if let Some(certificates) = &self.trust {
            let mut store = X509StoreBuilder::new().map_err(invalid)?;
            for certificate in certificates { store.add_cert(certificate.clone()).map_err(invalid)?; }
            context.set_cert_store(store.build());
        }
        if !self.identities.is_empty() {
            unsafe { request::configure(context, &self.reservations[0])?; }
            unsafe { SSL_CTX_set_cert_cb(context.as_ptr(), Some(select_identity), (self as *const Self).cast_mut().cast()); }
        }
        if let Some(trust) = &self.simple_trust { unsafe { trust.configure_native(context)?; } }
        Ok(())
    }

    fn select(&self, ssl: &mut SslRef) -> Result<()> {
        unsafe { SSL_certs_clear(ssl.as_ptr()); }
        let request = request::Request::of(ssl)?;
        let types = request.requested_types(ssl);
        let now = Asn1Time::days_from_now(0).map_err(invalid)?;
        let required = CERT_PKEY_VALID | CERT_PKEY_ISSUER_NAME | if ssl.version2() == Some(SslVersion::TLS1_2) { CERT_PKEY_CERT_TYPE } else { 0 };
        let selected = self.key_manager.choose(&types, |index, kind| {
            let identity = &self.identities[index];
            identity.eligible && identity.kind == kind
                && (!self.key_manager.constrains_certificates() || std::iter::once(identity.certificate.as_ref()).chain(identity.chain.iter()).all(|certificate| request.permits(certificate)))
                && unsafe { SSL_check_chain(ssl.as_ptr(), identity.certificate.as_ptr(), identity.key.as_ptr(), identity.chain.as_ptr()) } & required == required
        }, |index| keys::client_status(&self.identities[index].certificate, &now));
        if let Some(index) = selected {
            let identity = &self.identities[index];
            ssl.set_certificate(&identity.certificate).map_err(invalid)?;
            ssl.set_private_key(&identity.key).map_err(invalid)?;
            for certificate in &identity.chain { ssl.add_chain_cert(certificate.to_owned()).map_err(invalid)?; }
        }
        Ok(())
    }
}

impl rdkafka::config::SslContextConfigurer for NativeTls {
    fn replaces_default_trust(&self) -> bool { self.trust.is_some() }
    unsafe fn library_context(&self) -> *mut c_void { self.random.as_ptr().cast() }
    unsafe fn configure(&self, context: *mut c_void) -> std::result::Result<(), String> {
        let context = context.cast::<openssl_sys::SSL_CTX>();
        if unsafe { openssl_sys::SSL_CTX_up_ref(context) } != 1 { return Err("Unable to retain TLS context".into()); }
        let mut context = unsafe { SslContextBuilder::from_ptr(context) };
        unsafe { self.configure_context(&mut context) }.map_err(|_| "Invalid Kafka TLS context configuration".into())
    }
}

unsafe extern "C" fn select_identity(ssl: *mut openssl_sys::SSL, opaque: *mut c_void) -> i32 {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let configuration = unsafe { &*opaque.cast::<NativeTls>() };
        let ssl = unsafe { SslRef::from_ptr_mut(ssl) };
        configuration.select(ssl)
    })) {
        Ok(Ok(())) => 1,
        _ => 0,
    }
}

fn material(options: &BTreeMap<String, String>, store: &str, usage: StoreUse, reserve: &mut impl FnMut(usize) -> Result<Arc<MemoryReservation>>) -> Result<Option<KeyStore>> {
    let get = |suffix: &str| options.get(&format!("ssl.{store}.{suffix}")).map(String::as_str);
    let location = get("location");
    let inline = get(if usage == StoreUse::Trust { "certificates" } else { "key" });
    let chain = if usage != StoreUse::Trust { get("certificate.chain") } else { None };
    let password = get("password");
    let key_password = options.get("ssl.key.password").map(String::as_str);
    if location.is_none() && inline.is_none() {
        if password.is_some() || chain.is_some() { return Err(invalid("Producer store password or chain requires a store")); }
        return Ok(None);
    }
    let format = match get("type").unwrap_or("JKS") {
        "PEM" => StoreFormat::Pem,
        java if java.eq_ignore_ascii_case("JKS") || java.eq_ignore_ascii_case("PKCS12") => StoreFormat::Java,
        _ => return Err(invalid("Unknown producer keystore format")),
    };
    if inline.is_some() && format != StoreFormat::Pem { return Err(invalid("Inline producer keys and certificates require PEM format")); }
    if location.is_some() && (inline.is_some() || chain.is_some()) { return Err(invalid("Producer keystore file and inline material are mutually exclusive")); }
    if format == StoreFormat::Java && usage != StoreUse::Trust && password.is_none() { return Err(invalid("Producer private-key stores require a store password")); }
    let length = match location {
        Some(path) => usize::try_from(std::fs::metadata(path).map_err(invalid)?.len()).map_err(|_| overflow())?,
        None => inline.map_or(0, str::len).checked_add(chain.map_or(0, str::len)).and_then(|bytes| bytes.checked_add(1)).ok_or_else(overflow)?,
    };
    if length > 4 * 1024 * 1024 { return Err(invalid("Producer keystore exceeds its byte limit")); }
    let bound = KeyStore::allocation_bound_with_passwords(length, password, key_password)?.checked_add(length.checked_mul(32).ok_or_else(overflow)?).ok_or_else(overflow)?;
    let charge = reserve(bound)?;
    let mut bytes = Zeroizing::new(Vec::with_capacity(length.checked_add(1).ok_or_else(overflow)?));
    if let Some(path) = location {
        std::fs::File::open(path).map_err(invalid)?.take(length as u64 + 1).read_to_end(&mut bytes).map_err(invalid)?;
        if bytes.len() != length { return Err(invalid("Producer keystore changed while opening")); }
    } else {
        bytes.extend_from_slice(inline.expect("one material source exists").as_bytes());
        if usage != StoreUse::Trust {
            let chain = chain.ok_or_else(|| invalid("Producer private key requires a certificate chain"))?;
            bytes.push(b'\n'); bytes.extend_from_slice(chain.as_bytes());
        }
    }
    KeyStore::decode(&bytes, format, usage, password, key_password, charge).map(Some)
}

const CERT_PKEY_VALID: i32 = 0x1;
const CERT_PKEY_ISSUER_NAME: i32 = 0x200;
const CERT_PKEY_CERT_TYPE: i32 = 0x400;

unsafe extern "C" {
    fn SSL_CTX_set_cert_cb(context: *mut openssl_sys::SSL_CTX, callback: Option<unsafe extern "C" fn(*mut openssl_sys::SSL, *mut c_void) -> i32>, opaque: *mut c_void);
    fn SSL_check_chain(ssl: *mut openssl_sys::SSL, certificate: *mut openssl_sys::X509, key: *mut openssl_sys::EVP_PKEY, chain: *mut openssl_sys::stack_st_X509) -> i32;
    fn SSL_certs_clear(ssl: *mut openssl_sys::SSL);
}
fn invalid(cause: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("xtdb.kafka/invalid-tls-config", cause.to_string()) }
fn overflow() -> Anomaly { invalid("Kafka TLS material exceeds allocation range") }

pub(crate) fn validate_provider(name: Option<&str>) -> Result<()> {
    match name {
        None | Some("SunJSSE") => Ok(()),
        _ => Err(invalid("TLS provider requires a native context adapter")),
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod random_integration;
