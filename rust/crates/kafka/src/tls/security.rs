use std::{cmp::Ordering, ptr};
use foreign_types::{ForeignType, ForeignTypeRef};
use openssl::{asn1::{Asn1BitString, Asn1Time}, hash::MessageDigest, nid::Nid, pkey::{Id, PKeyRef, Public}, x509::X509Ref};
use rtdb_types::Result;
use super::invalid;

mod parameters;
mod client;
pub(crate) use client::java_client_chain_eligible;
mod peer;
pub(crate) use peer::permits_client_certificate;
mod pkix;
pub(crate) use pkix::{JavaCertificateFacts, PathMetadata, verify_pkix_anchor, verify_pkix_server_usage};
mod native;
pub(crate) use native::configure as configure_native;

const BLOCKED: &str = include_str!("security/temurin-25.0.1.blocked.certs");
const JDK_ANCHORS: &str = include_str!("security/temurin-25.0.1.jdk-anchors.sha256");
const SYMANTEC: &str = include_str!("security/symantec.sha256");
const SYMANTEC_EXEMPT: &str = include_str!("security/symantec-exempt.sha256");
const ENTRUST: &str = include_str!("security/entrust.sha256");
const CAMERFIRMA: &str = include_str!("security/camerfirma.sha256");

#[derive(Clone, Copy, PartialEq, Eq)]
struct Fingerprint([u8; 32]);
impl Fingerprint {
    fn of(certificate: &X509Ref) -> Result<Self> {
        let digest = certificate.digest(MessageDigest::sha256()).map_err(invalid)?;
        Ok(Self(digest.as_ref().try_into().map_err(|_| invalid("TLS certificate fingerprint has an invalid length"))?))
    }
    fn listed(self, list: &str) -> bool {
        const HEX: &[u8] = b"0123456789ABCDEF";
        list.lines().any(|line| line.len() == 64 && self.0.iter().enumerate().all(|(index, byte)| {
            line.as_bytes()[2 * index].to_ascii_uppercase() == HEX[usize::from(byte >> 4)]
                && line.as_bytes()[2 * index + 1].to_ascii_uppercase() == HEX[usize::from(byte & 15)]
        }))
    }
}

#[derive(Clone, Copy)]
struct Key { algorithm: Id, bits: Option<u32> }
impl Key {
    fn of(key: &PKeyRef<Public>) -> Self {
        let algorithm = key.id();
        let missing = algorithm == Id::DSA && unsafe { EVP_PKEY_missing_parameters(key.as_ptr()) } != 0;
        Self { algorithm, bits: if missing { None } else { Some(key.bits()) } }
    }
    fn check(self) -> Result<()> {
        let minimum = match self.algorithm { Id::RSA | Id::DSA | Id::DH | Id::DHX => 1024, Id::EC => 224, _ => return Ok(()) };
        if self.bits.is_some_and(|bits| bits < minimum) { return Err(invalid("TLS certificate public key is disabled by the pinned Java security policy")); }
        Ok(())
    }
    fn inherit(self, issuer: Option<Self>) -> Result<Self> {
        if self.algorithm == Id::DSA && self.bits.is_none() {
            if let Some(issuer) = issuer {
                if issuer.algorithm != Id::DSA || issuer.bits.is_none() { return Err(invalid("TLS DSA certificate cannot inherit issuer parameters")); }
                return Ok(Self { bits: issuer.bits, ..self });
            }
        }
        Ok(self)
    }
}

pub(crate) fn verify_public_key(certificate: &X509Ref) -> Result<()> {
    Key::of(certificate.public_key().map_err(invalid)?.as_ref()).check()
}

pub(crate) fn verify_untrusted_certificate(certificate: &X509Ref) -> Result<()> {
    blocked(Fingerprint::of(certificate)?)
}

// OpenJDK 25.0.1 SimpleValidator/AlgorithmChecker and EndEntityChecker security policy.
pub(crate) fn verify_server_chain(chain: &[&X509Ref]) -> Result<()> {
    verify_server_path(chain)?;
    verify_server_distrust(chain)
}

pub(crate) fn verify_server_path(chain: &[&X509Ref]) -> Result<()> {
    verify_server_path_with_usage(chain, |_, certificate| key_usage(certificate))
}

fn verify_server_path_with_usage(chain: &[&X509Ref], usage: impl Fn(usize, &X509Ref) -> Result<()>) -> Result<()> {
    let anchor = *chain.last().ok_or_else(|| invalid("TLS security policy requires a nonempty chain"))?;
    let anchor_fingerprint = Fingerprint::of(anchor)?;
    blocked(anchor_fingerprint)?;
    if chain.len() == 1 { return Ok(()); }
    let jdk_ca = anchor_fingerprint.listed(JDK_ANCHORS);
    let mut issuer = Key::of(anchor.public_key().map_err(invalid)?.as_ref());
    for (index, certificate) in chain[..chain.len() - 1].iter().enumerate().rev() {
        blocked(Fingerprint::of(certificate)?)?;
        usage(index, certificate)?;
        issuer.check()?;
        let key = Key::of(certificate.public_key().map_err(invalid)?.as_ref());
        signature(certificate, jdk_ca, true)?;
        key.check()?;
        issuer = key.inherit(Some(issuer))?;
    }
    Ok(())
}

pub(crate) fn verify_server_distrust(chain: &[&X509Ref]) -> Result<()> {
    if chain.len() < 2 { return Ok(()); }
    let anchor = Fingerprint::of(chain[chain.len() - 1])?;
    let sub_ca = if chain.len() > 2 { Some(Fingerprint::of(chain[chain.len() - 2])?) } else { None };
    if let Some(cutoff) = distrust_cutoff(anchor, sub_ca) { issued_before(chain[0], cutoff)?; }
    Ok(())
}

#[cfg(test)]
pub(crate) fn verify_client_chain(chain: &[&X509Ref]) -> Result<()> {
    verify_client_chain_with_usage(chain, |certificate| key_usage(certificate).map_err(super::TlsConfigError::from_invalid_anomaly))
        .map_err(super::TlsConfigError::into_anomaly)
}

fn verify_client_chain_with_usage(chain: &[&X509Ref], usage: impl Fn(&X509Ref) -> super::TlsConfigResult<()>) -> super::TlsConfigResult<()> {
    let mut issuer: Option<Key> = None;
    for certificate in chain.iter().rev() {
        usage(certificate)?;
        signature(certificate, false, false).map_err(super::TlsConfigError::from_invalid_anomaly)?;
        let key = Key::of(certificate.public_key().map_err(super::TlsConfigError::invalid)?.as_ref());
        key.check().map_err(super::TlsConfigError::from_invalid_anomaly)?;
        if let Some(issuer) = issuer { issuer.check().map_err(super::TlsConfigError::from_invalid_anomaly)?; }
        issuer = Some(key.inherit(issuer).map_err(super::TlsConfigError::from_invalid_anomaly)?);
    }
    Ok(())
}

fn blocked(fingerprint: Fingerprint) -> Result<()> {
    if fingerprint.listed(BLOCKED) { return Err(invalid("TLS certificate is blocked by the pinned Java runtime")); }
    Ok(())
}

fn key_usage(certificate: &X509Ref) -> Result<()> {
    let mut critical = -1;
    let usage = unsafe { openssl_sys::X509_get_ext_d2i(certificate.as_ptr(), Nid::KEY_USAGE.as_raw(), &mut critical, ptr::null_mut()) };
    if usage.is_null() {
        return if critical == -1 { Ok(()) } else { Err(invalid("TLS certificate has an invalid key-usage extension")) };
    }
    let usage = unsafe { Asn1BitString::from_ptr(usage.cast()) };
    if usage.as_slice().first().is_none_or(|bits| bits & 0xfe == 0) { return Err(invalid("TLS certificate key usage permits no cryptographic primitive")); }
    Ok(())
}

fn signature(certificate: &X509Ref, jdk_ca: bool, inspect_parameters: bool) -> Result<()> {
    let algorithm = certificate.signature_algorithm().object().nid();
    if let Some(algorithms) = algorithm.signature_algorithms() { digest(algorithms.digest, jdk_ca)?; }
    if inspect_parameters && algorithm == Nid::RSASSAPSS {
        if let Some(encoded) = parameters::of(certificate)? {
            let (hash, mask_hash) = parameters::pss(encoded)?;
            digest(hash, jdk_ca)?;
            digest(mask_hash, jdk_ca)?;
        }
    }
    Ok(())
}

fn digest(digest: Nid, jdk_ca: bool) -> Result<()> {
    if matches!(digest, Nid::MD2 | Nid::MD5) || digest == Nid::SHA1 && jdk_ca {
        return Err(invalid("TLS certificate digest is disabled by the pinned Java security policy"));
    }
    Ok(())
}

fn distrust_cutoff(anchor: Fingerprint, sub_ca: Option<Fingerprint>) -> Option<i64> {
    if anchor.listed(SYMANTEC) {
        Some(if sub_ca.is_some_and(|sub_ca| sub_ca.listed(SYMANTEC_EXEMPT)) { 1_577_836_800 } else { 1_555_459_200 })
    } else if anchor.listed(ENTRUST) { Some(1_731_369_600) }
    else if anchor.listed(CAMERFIRMA) { Some(1_744_761_600) }
    else { None }
}

fn issued_before(certificate: &X509Ref, first_distrusted_second: i64) -> Result<()> {
    let cutoff = Asn1Time::from_unix(first_distrusted_second).map_err(invalid)?;
    if certificate.not_before().compare(&cutoff).map_err(invalid)? != Ordering::Less {
        return Err(invalid("TLS server certificate was issued after its root CA's Java distrust cutoff"));
    }
    Ok(())
}

unsafe extern "C" { fn EVP_PKEY_missing_parameters(key: *const openssl_sys::EVP_PKEY) -> i32; }

#[cfg(test)]
mod tests;
