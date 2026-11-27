use std::{collections::BTreeMap, ptr, sync::Arc};
use openssl::{ssl::{SslContext, SslContextBuilder, SslMode, SslVerifyMode}, x509::X509};
use rtdb_columnar::MemoryReservation;
use rtdb_tls::KeyStore;
use super::{Algorithm, Material, NativeTls, Policy, TlsConfigError, TlsConfigResult, random, trust, validation};

impl NativeTls {
    pub(crate) fn from_registry(options: &BTreeMap<&str, &str>, trust: Option<&KeyStore>, identities: Option<&Material>, charge: Arc<MemoryReservation>) -> TlsConfigResult<Arc<Self>> {
        super::validate_provider(options.get("provider").copied().filter(|name| !name.is_empty())).map_err(TlsConfigError::from_invalid_anomaly)?;
        let algorithm = Algorithm::parse(options.get("keymanager.algorithm").copied().filter(|name| !name.is_empty())).map_err(TlsConfigError::from_invalid_anomaly)?;
        let trust_algorithm = validation::Algorithm::parse(options.get("trustmanager.algorithm").copied().filter(|name| !name.is_empty())).map_err(TlsConfigError::from_invalid_anomaly)?;
        let protocol = options.get("protocol").copied().unwrap_or("TLSv1.3");
        if !matches!(protocol.to_ascii_uppercase().as_str(), "TLS" | "SSL" | "TLSV1.3" | "TLSV1.2" | "TLSV1.1" | "TLSV1" | "SSLV3") { return Err(TlsConfigError::invalid("Unknown Registry TLS context protocol")); }
        let policy = Policy::from_java(&BTreeMap::from([("ssl.protocol".into(), protocol.into()), ("ssl.enabled.protocols".into(), String::new())])).map_err(TlsConfigError::from_invalid_anomaly)?;
        let random = random::Context::new_typed(options.get("secure.random.implementation").copied(), charge.budget())?;
        let signature_random = random::Context::new_typed(None, charge.budget())?;
        let mut bound = 65_536usize;
        for certificate in trust.into_iter().flat_map(KeyStore::trusted_certificates) { bound = native_bound(bound, certificate.len())?; }
        let mut reservations = vec![Arc::clone(&charge), charge.budget().reserve(bound).map_err(TlsConfigError::Other)?];
        let default_trust = if trust.is_none() { Some(trust::decode_typed(Arc::clone(&charge))?) } else { None };
        let trust = trust.or(default_trust.as_ref()).expect("explicit or default Registry trust");
        let trust = trust.trusted_certificates().iter().map(|certificate| X509::from_der(certificate).map_err(TlsConfigError::invalid)).collect::<TlsConfigResult<Vec<_>>>()?;
        let identities = if let Some(material) = identities {
            let identities = material.native_typed(algorithm, &random, &charge)?;
            reservations.push(identities.reservation());
            Some(identities)
        } else { None };
        let simple_trust = if trust_algorithm == validation::Algorithm::SunX509 {
            Some(validation::SimpleTrust::new(trust.clone(), Arc::clone(&charge)).map_err(TlsConfigError::from_invalid_anomaly)?)
        } else { None };
        Ok(Arc::new(Self { identities, trust: Some(trust), simple_trust, policy, reservations, random, signature_random }))
    }

    // This owner must outlive the returned context and every SSL connection and native key created from it.
    pub(crate) unsafe fn registry_context(&self) -> TlsConfigResult<SslContext> {
        let context = unsafe { SSL_CTX_new_ex(self.random.as_ptr(), ptr::null(), openssl_sys::TLS_client_method()) };
        if context.is_null() { return Err(TlsConfigError::invalid(openssl::error::ErrorStack::get())); }
        let mut context = unsafe { SslContextBuilder::from_ptr(context) };
        context.set_verify(SslVerifyMode::PEER);
        context.set_mode(SslMode::AUTO_RETRY | SslMode::ACCEPT_MOVING_WRITE_BUFFER | SslMode::ENABLE_PARTIAL_WRITE | SslMode::RELEASE_BUFFERS);
        unsafe { self.configure_context(&mut context).map_err(TlsConfigError::from_invalid_anomaly)?; }
        Ok(context.build())
    }
}

fn native_bound(bytes: usize, length: usize) -> TlsConfigResult<usize> {
    length.checked_mul(32).and_then(|length| bytes.checked_add(length)).ok_or_else(|| TlsConfigError::invalid("Registry TLS material exceeds allocation range"))
}

unsafe extern "C" {
    fn SSL_CTX_new_ex(context: *mut openssl_sys::OSSL_LIB_CTX, properties: *const std::ffi::c_char, method: *const openssl_sys::SSL_METHOD) -> *mut openssl_sys::SSL_CTX;
}

#[cfg(test)]
mod tests;
