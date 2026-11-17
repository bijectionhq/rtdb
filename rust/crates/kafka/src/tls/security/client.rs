use std::{ptr, sync::Arc};
use foreign_types::ForeignTypeRef;
use openssl::x509::X509Ref;
use rtdb_columnar::MemoryBudget;
use rtdb_tls::CertificateKeyUsage;
use rtdb_types::Result;
use super::super::invalid;

pub(crate) fn java_client_chain_eligible(chain: &[&X509Ref], memory: &Arc<MemoryBudget>) -> Result<bool> {
    match super::verify_client_chain_with_usage(chain, |certificate| key_usage(certificate, memory)) {
        Ok(()) => Ok(true),
        Err(error) if matches!(error.code.as_str(), "xtdb.kafka/invalid-tls-config" | "rtdb.tls/invalid-keystore") => Ok(false),
        Err(error) => Err(error),
    }
}

fn key_usage(certificate: &X509Ref, memory: &Arc<MemoryBudget>) -> Result<()> {
    let length = unsafe { openssl_sys::i2d_X509(certificate.as_ptr(), ptr::null_mut()) };
    let bound = usize::try_from(length).ok().filter(|length| *length > 0).and_then(|length| length.checked_add(64))
        .ok_or_else(|| invalid("TLS client certificate encoding exceeds allocation range"))?;
    let _encoding = memory.reserve(bound)?;
    let encoded = certificate.to_der().map_err(invalid)?;
    if !CertificateKeyUsage::decode(&encoded, memory)?.permits_cryptographic_primitive() {
        return Err(invalid("TLS certificate key usage permits no cryptographic primitive"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
