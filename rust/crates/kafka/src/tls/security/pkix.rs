use std::ptr;
use foreign_types::{ForeignType, ForeignTypeRef};
use openssl::{asn1::Asn1BitString, nid::Nid, x509::X509Ref};
use rtdb_types::Result;
use super::super::invalid;

// A one-certificate chain is admitted here only after exact certificate membership in the trust store.
pub(crate) fn verify_pkix_server_chain(chain: &[&X509Ref]) -> Result<()> {
    let anchor = chain.last().ok_or_else(|| invalid("TLS PKIX policy requires a nonempty chain"))?;
    if chain.len() == 1 { return Ok(()); }
    super::verify_server_chain(chain)?;
    if anchor.version() < 2 { return Ok(()); }
    let flags = unsafe { openssl_sys::X509_get_extension_flags(anchor.as_ptr()) };
    if flags & (openssl_sys::EXFLAG_BCONS | openssl_sys::EXFLAG_CA) != (openssl_sys::EXFLAG_BCONS | openssl_sys::EXFLAG_CA) {
        return Err(invalid("TLS PKIX trust anchor is not a certificate authority"));
    }
    let mut critical = -1;
    let usage = unsafe { openssl_sys::X509_get_ext_d2i(anchor.as_ptr(), Nid::KEY_USAGE.as_raw(), &mut critical, ptr::null_mut()) };
    if usage.is_null() {
        if critical != -1 { return Err(invalid("TLS PKIX trust anchor has invalid key usage")); }
    } else {
        let usage = unsafe { Asn1BitString::from_ptr(usage.cast()) };
        if usage.as_slice().first().is_none_or(|bits| bits & 0x04 == 0) { return Err(invalid("TLS PKIX trust anchor excludes certificate signing")); }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use openssl::{asn1::Asn1Time, bn::BigNum, hash::MessageDigest, pkey::PKey, rsa::Rsa, x509::{X509, X509NameBuilder, extension::{BasicConstraints, KeyUsage}}};

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
