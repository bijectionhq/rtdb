use std::{ptr, sync::Arc};
use foreign_types::ForeignTypeRef;
use openssl::x509::X509Ref;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_tls::{CertificateBasicConstraints, CertificateExtendedKeyUsage, CertificateKeyUsage};
use rtdb_types::Result;
use super::super::{TlsConfigError, TlsConfigResult, invalid};

#[repr(C)]
pub(crate) struct JavaCertificateFacts {
    basic_constraints: i32,
    self_issued: u8,
    key_usage: u8,
}

pub(crate) struct PathMetadata<'a> {
    chain: &'a [&'a X509Ref],
    facts: Vec<JavaCertificateFacts>,
    _reservation: Arc<MemoryReservation>,
}

impl<'a> PathMetadata<'a> {
    pub(crate) fn decode(chain: &'a [&'a X509Ref], memory: &Arc<MemoryBudget>) -> Result<Self> {
        Self::decode_typed(chain, memory).map_err(TlsConfigError::into_anomaly)
    }

    pub(crate) fn decode_typed(chain: &'a [&'a X509Ref], memory: &Arc<MemoryBudget>) -> TlsConfigResult<Self> {
        let count = chain.len().checked_sub(1).filter(|count| *count > 0 && *count < i32::MAX as usize)
            .ok_or_else(|| TlsConfigError::invalid("TLS selected metadata requires a nontrivial certificate path"))?;
        let bound = count.checked_mul(std::mem::size_of::<JavaCertificateFacts>()).and_then(|bytes| bytes.checked_add(128))
            .ok_or_else(|| TlsConfigError::invalid("TLS selected-chain metadata exceeds allocation range"))?;
        let bound = chain[..count].iter().try_fold(bound, |bound, certificate| {
            encoded_length(certificate).map_err(TlsConfigError::from_invalid_anomaly)?.checked_mul(64).and_then(|length| length.checked_add(65_536))
                .and_then(|length| bound.checked_add(length)).ok_or_else(|| TlsConfigError::invalid("TLS selected-chain metadata exceeds allocation range"))
        })?;
        let reservation = memory.reserve(bound).map_err(TlsConfigError::Other)?;
        let mut facts = Vec::with_capacity(count);
        for certificate in &chain[..count] {
            let _encoding = reserve_encoding_typed(certificate, memory)?;
            let encoded = certificate.to_der().map_err(TlsConfigError::invalid)?;
            let basic_constraints = CertificateBasicConstraints::decode(&encoded, memory).map_err(TlsConfigError::from_tls)?.path_length();
            let key_usage = CertificateKeyUsage::decode(&encoded, memory).map_err(TlsConfigError::from_tls)?.permitted_bits();
            drop(CertificateExtendedKeyUsage::decode(&encoded, memory).map_err(TlsConfigError::from_tls)?);
            let self_issued = u8::from(super::super::names::same_typed(certificate.subject_name(), certificate.issuer_name(), memory)?);
            facts.push(JavaCertificateFacts { basic_constraints, self_issued, key_usage });
        }
        Ok(Self { chain, facts, _reservation: reservation })
    }

    pub(crate) fn facts(&self) -> &[JavaCertificateFacts] { &self.facts }

    pub(crate) fn verify_ssl_algorithms(&self) -> Result<()> {
        super::verify_server_path_with_usage(self.chain, |index, _| {
            if self.facts[index].key_usage & 0xfe == 0 {
                Err(invalid("TLS certificate key usage permits no cryptographic primitive"))
            } else { Ok(()) }
        })
    }
}

fn encoded_length(certificate: &X509Ref) -> Result<usize> {
    let length = unsafe { openssl_sys::i2d_X509(certificate.as_ptr(), ptr::null_mut()) };
    usize::try_from(length).map_err(|_| invalid("TLS certificate encoding exceeds allocation range"))
}

fn reserve_encoding(certificate: &X509Ref, memory: &Arc<MemoryBudget>) -> Result<Arc<MemoryReservation>> {
    reserve_encoding_typed(certificate, memory).map_err(TlsConfigError::into_anomaly)
}

fn reserve_encoding_typed(certificate: &X509Ref, memory: &Arc<MemoryBudget>) -> TlsConfigResult<Arc<MemoryReservation>> {
    let bound = encoded_length(certificate).map_err(TlsConfigError::from_invalid_anomaly)?.checked_add(64)
        .ok_or_else(|| TlsConfigError::invalid("TLS certificate encoding exceeds allocation range"))?;
    memory.reserve(bound).map_err(TlsConfigError::Other)
}

fn material_error(error: rtdb_tls::TlsError) -> rtdb_types::Anomaly {
    super::super::material_error(error)
}

pub(crate) fn verify_pkix_server_usage(certificate: &X509Ref, memory: &Arc<MemoryBudget>) -> Result<()> {
    let _encoding = reserve_encoding(certificate, memory)?;
    let encoded = certificate.to_der().map_err(invalid)?;
    if !CertificateKeyUsage::decode(&encoded, memory).map_err(material_error)?.permits_digital_signatures() {
        return Err(invalid("TLS server key usage excludes digital signatures"));
    }
    if !CertificateExtendedKeyUsage::decode(&encoded, memory).map_err(material_error)?.permits_server_authentication().map_err(material_error)? {
        return Err(invalid("TLS server extended key usage excludes server authentication"));
    }
    Ok(())
}

pub(crate) fn verify_pkix_anchor(anchor: &X509Ref, memory: &Arc<MemoryBudget>) -> Result<()> {
    if anchor.version() < 2 { return Ok(()); }
    let length = unsafe { openssl_sys::i2d_X509(anchor.as_ptr(), ptr::null_mut()) };
    let bound = usize::try_from(length).ok().and_then(|length| length.checked_add(64))
        .ok_or_else(|| invalid("TLS anchor encoding exceeds allocation range"))?;
    let _encoding = memory.reserve(bound)?;
    let encoded = anchor.to_der().map_err(invalid)?;
    if CertificateBasicConstraints::decode(&encoded, memory).map_err(material_error)?.path_length() == -1 {
        return Err(invalid("TLS PKIX trust anchor is not a certificate authority"));
    }
    if !CertificateKeyUsage::decode(&encoded, memory).map_err(material_error)?.permits_certificate_signing() {
        return Err(invalid("TLS PKIX trust anchor excludes certificate signing"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use openssl::{asn1::Asn1Time, bn::BigNum, hash::MessageDigest, pkey::PKey, rsa::Rsa, x509::{X509, X509NameBuilder, extension::{BasicConstraints, KeyUsage}}};

    fn verify_pkix_server_chain(chain: &[&X509Ref]) -> Result<()> {
        let anchor = chain.last().ok_or_else(|| invalid("TLS PKIX policy requires a nonempty chain"))?;
        if chain.len() == 1 { return Ok(()); }
        super::super::verify_server_chain(chain)?;
        verify_pkix_anchor(anchor, &MemoryBudget::new(16 << 20))
    }

    fn anchor(version: i32, ca: bool, sign: Option<bool>) -> X509 {
        let key = PKey::from_rsa(Rsa::generate(1024).unwrap()).unwrap();
        let mut name = X509NameBuilder::new().unwrap(); name.append_entry_by_text("CN", "PKIX anchor").unwrap(); let name = name.build();
        let mut certificate = X509::builder().unwrap(); certificate.set_version(version).unwrap();
        certificate.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap()).unwrap();
        certificate.set_subject_name(&name).unwrap(); certificate.set_issuer_name(&name).unwrap(); certificate.set_pubkey(&key).unwrap();
        certificate.set_not_before(&Asn1Time::from_unix(1_600_000_000).unwrap()).unwrap();
        certificate.set_not_after(&Asn1Time::from_unix(2_000_000_000).unwrap()).unwrap();
        if version == 2 {
            let mut constraints = BasicConstraints::new(); if ca { constraints.ca(); }
            certificate.append_extension(constraints.build().unwrap()).unwrap();
        }
        if let Some(sign) = sign {
            let mut usage = KeyUsage::new();
            if sign { usage.key_cert_sign(); } else { usage.digital_signature(); }
            certificate.append_extension(usage.build().unwrap()).unwrap();
        }
        certificate.sign(&key, MessageDigest::sha256()).unwrap(); certificate.build()
    }

    #[test]
    fn pkix_exact_trusted_leaf_skips_policy_while_nontrivial_paths_require_ca_anchors() {
        let leaf = super::super::tests::certificate(1024, MessageDigest::sha256(), 1_700_000_000, None);
        for (version, ca, usage, valid) in [(0, false, None, true), (1, false, None, true), (2, false, None, false), (2, true, None, true), (2, true, Some(false), false), (2, true, Some(true), true)] {
            let anchor = anchor(version, ca, usage);
            assert_eq!(verify_pkix_server_chain(&[&leaf, &anchor]).is_ok(), valid);
            assert!(verify_pkix_server_chain(&[&anchor]).is_ok());
        }
        let weak = super::super::tests::certificate(512, MessageDigest::md5(), 1_700_000_000, None);
        assert!(verify_pkix_server_chain(&[&weak]).is_ok());
        assert!(verify_pkix_server_chain(&[]).is_err());
    }

    #[test]
    fn path_metadata_admits_facts_and_native_copies_before_encoding_and_releases_scratch() {
        let leaf = super::super::tests::certificate(1024, MessageDigest::sha256(), 1_700_000_000, None);
        let root = anchor(2, true, Some(true));
        let chain = [leaf.as_ref(), root.as_ref()];
        let bound = 128 + std::mem::size_of::<JavaCertificateFacts>() + 65_536 + 64 * encoded_length(&leaf).unwrap();
        let denied = MemoryBudget::new(bound - 1);
        assert_eq!(PathMetadata::decode(&chain, &denied).err().unwrap().code, "rtdb/memory-budget-exceeded");
        assert_eq!(denied.used_bytes(), 0); assert_eq!(denied.peak_bytes(), 0);
        let scratch_denied = MemoryBudget::new(bound);
        assert_eq!(PathMetadata::decode(&chain, &scratch_denied).err().unwrap().code, "rtdb/memory-budget-exceeded");
        assert_eq!(scratch_denied.used_bytes(), 0); assert_eq!(scratch_denied.peak_bytes(), bound);
        let memory = MemoryBudget::new(16 << 20);
        let metadata = PathMetadata::decode(&chain, &memory).unwrap();
        assert_eq!(metadata.facts.len(), 1);
        assert_eq!(metadata.chain[0].as_ptr(), leaf.as_ptr());
        assert_eq!(memory.used_bytes(), bound);
        assert!(memory.peak_bytes() > bound);
        drop(metadata); assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn path_metadata_rejects_trivial_paths_and_releases_admission_after_critical_parse_failure() {
        let root = anchor(2, true, Some(true)); let memory = MemoryBudget::new(16 << 20);
        assert!(PathMetadata::decode(&[], &memory).is_err());
        assert!(PathMetadata::decode(&[root.as_ref()], &memory).is_err());
        assert_eq!(memory.peak_bytes(), 0);
        let key = PKey::from_rsa(Rsa::generate(1024).unwrap()).unwrap();
        let mut certificate = X509::builder().unwrap(); certificate.set_version(2).unwrap();
        certificate.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap()).unwrap();
        certificate.set_subject_name(root.subject_name()).unwrap(); certificate.set_issuer_name(root.subject_name()).unwrap(); certificate.set_pubkey(&key).unwrap();
        certificate.set_not_before(root.not_before()).unwrap(); certificate.set_not_after(root.not_after()).unwrap();
        let oid = openssl::asn1::Asn1Object::from_str("2.5.29.37").unwrap();
        let bytes = openssl::asn1::Asn1OctetString::new_from_bytes(&[5, 0]).unwrap();
        certificate.append_extension(openssl::x509::X509Extension::new_from_der(&oid, true, &bytes).unwrap()).unwrap();
        certificate.sign(&key, MessageDigest::sha256()).unwrap(); let certificate = certificate.build();
        assert_eq!(PathMetadata::decode(&[certificate.as_ref(), root.as_ref()], &memory).err().unwrap().code, "xtdb.kafka/invalid-tls-config");
        assert!(memory.peak_bytes() > 0); assert_eq!(memory.used_bytes(), 0);
    }

}
