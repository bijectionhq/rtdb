use std::{ptr, sync::Arc};
use foreign_types::ForeignTypeRef;
use openssl::x509::X509Ref;
use rtdb_columnar::MemoryBudget;
use rtdb_tls::{CertificateBasicConstraints, CertificateKeyUsage};
use rtdb_types::Result;
use super::super::invalid;

pub(crate) fn verify_pkix_anchor(anchor: &X509Ref, memory: &Arc<MemoryBudget>) -> Result<()> {
    if anchor.version() < 2 { return Ok(()); }
    let length = unsafe { openssl_sys::i2d_X509(anchor.as_ptr(), ptr::null_mut()) };
    let bound = usize::try_from(length).ok().and_then(|length| length.checked_add(64))
        .ok_or_else(|| invalid("TLS anchor encoding exceeds allocation range"))?;
    let _encoding = memory.reserve(bound)?;
    let encoded = anchor.to_der().map_err(invalid)?;
    let material_error = |error: rtdb_types::Anomaly| if error.code == "rtdb.tls/invalid-keystore" { invalid(error) } else { error };
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
}
