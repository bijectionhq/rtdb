use std::{collections::BTreeMap, ffi::c_void, io::Read, sync::Arc};
use foreign_types::ForeignTypeRef;
use openssl::{asn1::Asn1Time, ssl::{SslContextBuilder, SslOptions, SslRef, SslVersion}, x509::{X509, store::X509StoreBuilder}};
use rtdb_columnar::MemoryReservation;
use rtdb_tls::{StoreFormat, StoreUse};
use rtdb_types::{Anomaly, Result};
use zeroize::Zeroizing;

mod policy;
use policy::Policy;
pub(crate) mod keys;
use keys::{Algorithm, KeyManager, KeyType};
pub(crate) mod identities;
use identities::{Material, NativeIdentities};
pub(crate) mod trust;
pub(crate) mod validation;
pub(crate) mod security;
pub(crate) mod request;
mod public_key;
mod names;
pub(crate) mod random;
mod registry;

pub(crate) struct NativeTls {
    identities: Option<NativeIdentities>,
    random: Arc<random::Context>,
    signature_random: Arc<random::Context>,
    trust: Option<Vec<X509>>,
    simple_trust: Option<validation::SimpleTrust>,
    policy: Policy,
    reservations: Vec<Arc<MemoryReservation>>,
}

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
        let mut identities = None;
        let mut trust = None;
        for (store, usage) in [("truststore", StoreUse::Trust), ("keystore", StoreUse::OptionalIdentity)] {
            let material = material(options, store, usage, algorithm, reserve)?;
            let material = if usage == StoreUse::Trust && material.is_none() && !trust::native_override(options) {
                Some(Material::Eager(Arc::new(trust::defaults(reserve)?)))
            } else { material };
            if let Some(material) = material {
                match usage {
                    StoreUse::Trust => {
                        let Material::Eager(material) = material else { unreachable!("trust material is eager"); };
                        reservations.push(material.reservation());
                        let certificates = material.trusted_certificates().iter().map(|certificate| X509::from_der(certificate).map_err(invalid)).collect::<Result<Vec<_>>>()?;
                        trust = Some(certificates);
                    }
                    StoreUse::Identity | StoreUse::OptionalIdentity => {
                        let loaded = material.native(algorithm, &random, &reservations[0])?;
                        reservations.push(loaded.reservation());
                        identities = Some(loaded);
                    }
                }
            }
        }
        let simple_trust = if trust_algorithm == validation::Algorithm::SunX509 {
            let anchors = trust.as_ref().ok_or_else(|| invalid("SunX509 trust management requires Java-style trust material"))?.clone();
            Some(validation::SimpleTrust::new(anchors, Arc::clone(&reservations[0]))?)
        } else { None };
        Ok(Some(Arc::new(Self { identities, random, signature_random, trust, simple_trust, policy, reservations })))
    }

    pub(crate) fn reservations(&self) -> &[Arc<MemoryReservation>] { &self.reservations }

    // The installed certificate callback borrows self until this context and its SSL objects are destroyed.
    unsafe fn configure_context(&self, context: &mut SslContextBuilder) -> Result<()> {
        unsafe { self.signature_random.configure_signatures(context); }
        security::configure_native(context)?;
        context.set_min_proto_version(Some(SslVersion::TLS1_2)).map_err(invalid)?;
        context.set_max_proto_version(Some(SslVersion::TLS1_3)).map_err(invalid)?;
        context.clear_options(SslOptions::NO_TLSV1_2 | SslOptions::NO_TLSV1_3);
        self.policy.apply(context)?;
        if let Some(certificates) = &self.trust {
            let mut store = X509StoreBuilder::new().map_err(invalid)?;
            for certificate in certificates { store.add_cert(certificate.clone()).map_err(invalid)?; }
            context.set_cert_store(store.build());
        }
        if self.identities.as_ref().is_some_and(|identities| !identities.entries().is_empty()) {
            unsafe { request::configure(context, &self.reservations[0])?; }
            unsafe { SSL_CTX_set_cert_cb(context.as_ptr(), Some(select_identity), (self as *const Self).cast_mut().cast()); }
        }
        if let Some(trust) = &self.simple_trust { unsafe { trust.configure_native(context)?; } }
        else { unsafe { validation::pkix::configure_native(context, &self.reservations[0])?; } }
        Ok(())
    }

    fn select(&self, ssl: &mut SslRef) -> Result<()> {
        unsafe { SSL_certs_clear(ssl.as_ptr()); }
        let Some(material) = &self.identities else { return Ok(()); };
        let identities = material.entries();
        let key_manager = material.manager();
        let request = request::Request::of(ssl)?;
        let types = request.requested_types(ssl);
        let now = Asn1Time::days_from_now(0).map_err(invalid)?;
        let memory = self.reservations[0].budget();
        let authorities = names::peer_authorities(ssl, memory)?;
        let _matches_charge = memory.reserve(identities.len().checked_add(64).ok_or_else(overflow)?)?;
        let mut issuer_matches = Vec::with_capacity(identities.len());
        for identity in identities {
            issuer_matches.push(names::matches_issuer(std::iter::once(identity.certificate.as_ref()).chain(identity.chain.iter()), &authorities, memory)?);
        }
        let required = CERT_PKEY_VALID | if ssl.version2() == Some(SslVersion::TLS1_2) { CERT_PKEY_CERT_TYPE } else { 0 };
        let selected = key_manager.try_choose(&types, |index, kind| {
            let identity = &identities[index];
            identity.eligible && identity.kind == kind && issuer_matches[index]
                && (!key_manager.constrains_certificates() || std::iter::once(identity.certificate.as_ref()).chain(identity.chain.iter()).all(|certificate| request.permits(certificate)))
                && (key_manager.constrains_certificates() || identity.eager_compatible(ssl, required))
        }, |index| keys::client_status(&identities[index].certificate, &now, memory))?;
        if let Some(index) = selected {
            identities[index].install(ssl, &self.random, &self.signature_random)?;
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

fn material(options: &BTreeMap<String, String>, store: &str, usage: StoreUse, algorithm: Algorithm, reserve: &mut impl FnMut(usize) -> Result<Arc<MemoryReservation>>) -> Result<Option<Material>> {
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
    let bound = Material::allocation_bound(length, format, usage, algorithm, password, key_password)?.checked_add(length.checked_mul(32).ok_or_else(overflow)?).ok_or_else(overflow)?;
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
    Material::decode(&bytes, format, usage, algorithm, password, key_password, charge).map(Some)
}

const CERT_PKEY_VALID: i32 = 0x1;
const CERT_PKEY_CERT_TYPE: i32 = 0x400;

unsafe extern "C" {
    fn SSL_CTX_set_cert_cb(context: *mut openssl_sys::SSL_CTX, callback: Option<unsafe extern "C" fn(*mut openssl_sys::SSL, *mut c_void) -> i32>, opaque: *mut c_void);
    fn SSL_check_chain(ssl: *mut openssl_sys::SSL, certificate: *mut openssl_sys::X509, key: *mut openssl_sys::EVP_PKEY, chain: *mut openssl_sys::stack_st_X509) -> i32;
    fn SSL_certs_clear(ssl: *mut openssl_sys::SSL);
}
pub(crate) type TlsConfigResult<T> = std::result::Result<T, TlsConfigError>;

/// Typed failure for Kafka TLS configuration and PKIX path validation, keeping wire codes unchanged.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TlsConfigError {
    Invalid(Option<String>),
    Other(Anomaly),
}

impl TlsConfigError {
    pub(crate) fn invalid(cause: impl std::fmt::Display) -> Self {
        Self::Invalid(Some(cause.to_string()))
    }

    pub(crate) fn from_tls(error: rtdb_tls::TlsError) -> Self {
        match error {
            rtdb_tls::TlsError::InvalidKeystore(message) => Self::Invalid(Some(message)),
            other => Self::Other(other.into_anomaly()),
        }
    }

    pub(crate) fn from_invalid_anomaly(error: Anomaly) -> Self {
        Self::Invalid(error.message)
    }

    pub(crate) fn into_anomaly(self) -> Anomaly {
        self.into()
    }
}

impl From<TlsConfigError> for Anomaly {
    fn from(error: TlsConfigError) -> Self {
        match error {
            TlsConfigError::Invalid(message) => Anomaly::new_nullable(rtdb_types::AnomalyCategory::Incorrect, "xtdb.kafka/invalid-tls-config", message),
            TlsConfigError::Other(anomaly) => anomaly,
        }
    }
}

pub(crate) fn material_error(error: rtdb_tls::TlsError) -> Anomaly {
    TlsConfigError::from_tls(error).into_anomaly()
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
pub(crate) mod tests;
#[cfg(test)]
mod random_integration;
