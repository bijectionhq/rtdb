use std::{collections::BTreeMap, ffi::c_void, io::Read, sync::Arc};
use foreign_types::{ForeignType, ForeignTypeRef};
use openssl::{pkey::{PKey, Private}, ssl::{SslContextBuilder, SslOptions, SslRef, SslVersion}, stack::Stack, x509::{X509, store::X509StoreBuilder}};
use rtdb_columnar::MemoryReservation;
use rtdb_tls::{KeyStore, StoreFormat, StoreUse};
use rtdb_types::{Anomaly, Result};
use zeroize::Zeroizing;

mod policy;
use policy::Policy;
pub(crate) mod trust;

pub(crate) struct NativeTls {
    identities: Vec<ClientIdentity>,
    trust: Option<Vec<X509>>,
    policy: Policy,
    reservations: Vec<Arc<MemoryReservation>>,
}
struct ClientIdentity { certificate: X509, key: PKey<Private>, chain: Stack<X509> }

impl NativeTls {
    pub(crate) fn from_java(options: &BTreeMap<String, String>, reserve: &mut impl FnMut(usize) -> Result<Arc<MemoryReservation>>) -> Result<Option<Arc<Self>>> {
        if !options.get("security.protocol").is_some_and(|protocol| protocol.eq_ignore_ascii_case("SSL") || protocol.eq_ignore_ascii_case("SASL_SSL")) { return Ok(None); }
        let bytes = options.iter().try_fold(65_536usize, |bytes, (name, value)| bytes.checked_add(name.len()).and_then(|bytes| bytes.checked_add(value.len())).ok_or_else(overflow))?;
        let mut reservations = vec![reserve(bytes.checked_mul(16).ok_or_else(overflow)?)?];
        let policy = Policy::from_java(options)?;
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
                        for identity in material.identities() {
                            let certificate = X509::from_der(&identity.certificates()[0]).map_err(invalid)?;
                            let key = PKey::private_key_from_pkcs8(identity.private_key_pkcs8()).map_err(invalid)?;
                            let mut chain = Stack::new().map_err(invalid)?;
                            for certificate in &identity.certificates()[1..] { chain.push(X509::from_der(certificate).map_err(invalid)?).map_err(invalid)?; }
                            identities.push(ClientIdentity { certificate, key, chain });
                        }
                    }
                }
            }
        }
        Ok(Some(Arc::new(Self { identities, trust, policy, reservations })))
    }

    pub(crate) fn reservations(&self) -> &[Arc<MemoryReservation>] { &self.reservations }

    // The installed certificate callback borrows self until this context and its SSL objects are destroyed.
    unsafe fn configure_context(&self, context: &mut SslContextBuilder) -> Result<()> {
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
            unsafe { SSL_CTX_set_cert_cb(context.as_ptr(), Some(select_identity), (self as *const Self).cast_mut().cast()); }
        }
        Ok(())
    }

    fn select(&self, ssl: &mut SslRef) -> Result<()> {
        unsafe { SSL_certs_clear(ssl.as_ptr()); }
        for identity in &self.identities {
            // SSL_check_chain checks peer signature, curve, certificate-type and issuer constraints.
            // https://docs.openssl.org/3.0/man3/SSL_check_chain/
            if unsafe { SSL_check_chain(ssl.as_ptr(), identity.certificate.as_ptr(), identity.key.as_ptr(), identity.chain.as_ptr()) } & CERT_PKEY_VALID == 0 { continue; }
            ssl.set_certificate(&identity.certificate).map_err(invalid)?;
            ssl.set_private_key(&identity.key).map_err(invalid)?;
            for certificate in &identity.chain { ssl.add_chain_cert(certificate.to_owned()).map_err(invalid)?; }
            break;
        }
        Ok(())
    }
}

impl rdkafka::config::SslContextConfigurer for NativeTls {
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
unsafe extern "C" {
    fn SSL_CTX_set_cert_cb(context: *mut openssl_sys::SSL_CTX, callback: Option<unsafe extern "C" fn(*mut openssl_sys::SSL, *mut c_void) -> i32>, opaque: *mut c_void);
    fn SSL_check_chain(ssl: *mut openssl_sys::SSL, certificate: *mut openssl_sys::X509, key: *mut openssl_sys::EVP_PKEY, chain: *mut openssl_sys::stack_st_X509) -> i32;
    fn SSL_certs_clear(ssl: *mut openssl_sys::SSL);
}
fn invalid(cause: impl std::fmt::Display) -> Anomaly { Anomaly::incorrect("xtdb.kafka/invalid-tls-config", cause.to_string()) }
fn overflow() -> Anomaly { invalid("Kafka TLS material exceeds allocation range") }

#[cfg(test)]
mod tests;
