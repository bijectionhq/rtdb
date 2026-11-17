use std::{collections::BTreeMap, ptr, sync::Arc};
use foreign_types::ForeignType;
use openssl::{ssl::{SslContext, SslContextBuilder, SslMode, SslVerifyMode}, stack::Stack, x509::X509};
use rtdb_columnar::MemoryReservation;
use rtdb_tls::KeyStore;
use rtdb_types::Result;
use super::{Algorithm, ClientIdentity, KeyManager, KeyType, NativeTls, Policy, invalid, random, security, trust, validation};

impl NativeTls {
    pub(crate) fn from_registry(options: &BTreeMap<&str, &str>, trust: Option<&KeyStore>, identities: Option<&KeyStore>, charge: Arc<MemoryReservation>) -> Result<Arc<Self>> {
        super::validate_provider(options.get("provider").copied().filter(|name| !name.is_empty()))?;
        let algorithm = Algorithm::parse(options.get("keymanager.algorithm").copied().filter(|name| !name.is_empty()))?;
        let trust_algorithm = validation::Algorithm::parse(options.get("trustmanager.algorithm").copied().filter(|name| !name.is_empty()))?;
        let protocol = options.get("protocol").copied().unwrap_or("TLSv1.3");
        if !matches!(protocol.to_ascii_uppercase().as_str(), "TLS" | "SSL" | "TLSV1.3" | "TLSV1.2" | "TLSV1.1" | "TLSV1" | "SSLV3") { return Err(invalid("Unknown Registry TLS context protocol")); }
        let policy = Policy::from_java(&BTreeMap::from([("ssl.protocol".into(), protocol.into()), ("ssl.enabled.protocols".into(), String::new())]))?;
        let random = random::Context::new(options.get("secure.random.implementation").copied(), charge.budget())?;
        let signature_random = random::Context::new(None, charge.budget())?;
        let mut bound = 65_536usize;
        for certificate in trust.into_iter().flat_map(KeyStore::trusted_certificates) { bound = native_bound(bound, certificate.len())?; }
        for identity in identities.into_iter().flat_map(KeyStore::identities) {
            bound = native_bound(bound, identity.private_key_pkcs8().len())?;
            for certificate in identity.certificates() { bound = native_bound(bound, certificate.len())?; }
        }
        let reservations = vec![Arc::clone(&charge), charge.budget().reserve(bound)?];
        let default_trust = if trust.is_none() { Some(trust::decode(Arc::clone(&charge))?) } else { None };
        let trust = trust.or(default_trust.as_ref()).expect("explicit or default Registry trust");
        let trust = trust.trusted_certificates().iter().map(|certificate| X509::from_der(certificate).map_err(invalid)).collect::<Result<Vec<_>>>()?;
        let key_manager = KeyManager::new(algorithm, identities.map(KeyStore::identities).unwrap_or(&[]));
        let identities = identities.into_iter().flat_map(KeyStore::identities).map(|identity| {
            let certificate = X509::from_der(&identity.certificates()[0]).map_err(invalid)?;
            let kind = KeyType::of(&certificate)?;
            let key = unsafe { random.private_key_from_pkcs8(identity.private_key_pkcs8())? };
            let mut chain = Stack::new().map_err(invalid)?;
            for certificate in &identity.certificates()[1..] { chain.push(X509::from_der(certificate).map_err(invalid)?).map_err(invalid)?; }
            let eligible = algorithm == Algorithm::SunX509 || security::verify_client_chain(&std::iter::once(certificate.as_ref()).chain(chain.iter()).collect::<Vec<_>>()).is_ok();
            Ok(ClientIdentity { certificate, key, chain, kind, eligible })
        }).collect::<Result<Vec<_>>>()?;
        let simple_trust = if trust_algorithm == validation::Algorithm::SunX509 { Some(validation::SimpleTrust::new(trust.clone(), Arc::clone(&charge))?) } else { None };
        Ok(Arc::new(Self { identities, key_manager, trust: Some(trust), simple_trust, policy, reservations, random, signature_random }))
    }

    // This owner must outlive the returned context and every SSL connection and native key created from it.
    pub(crate) unsafe fn registry_context(&self) -> Result<SslContext> {
        let context = unsafe { SSL_CTX_new_ex(self.random.as_ptr(), ptr::null(), openssl_sys::TLS_client_method()) };
        if context.is_null() { return Err(invalid(openssl::error::ErrorStack::get())); }
        let mut context = unsafe { SslContextBuilder::from_ptr(context) };
        context.set_verify(SslVerifyMode::PEER);
        context.set_mode(SslMode::AUTO_RETRY | SslMode::ACCEPT_MOVING_WRITE_BUFFER | SslMode::ENABLE_PARTIAL_WRITE | SslMode::RELEASE_BUFFERS);
        unsafe { self.configure_context(&mut context)?; }
        Ok(context.build())
    }
}

fn native_bound(bytes: usize, length: usize) -> Result<usize> {
    length.checked_mul(32).and_then(|length| bytes.checked_add(length)).ok_or_else(|| invalid("Registry TLS material exceeds allocation range"))
}

unsafe extern "C" {
    fn SSL_CTX_new_ex(context: *mut openssl_sys::OSSL_LIB_CTX, properties: *const std::ffi::c_char, method: *const openssl_sys::SSL_METHOD) -> *mut openssl_sys::SSL_CTX;
}

#[cfg(test)]
mod tests;
