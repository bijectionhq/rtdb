use std::sync::Arc;
use foreign_types::ForeignTypeRef;
use openssl::x509::X509Ref;
use rtdb_columnar::MemoryBudget;
use rtdb_tls::CertificatePublicKeyEncoding;
use rtdb_types::Result;
use super::{TlsConfigError, TlsConfigResult};

pub(super) fn same(left: &X509Ref, right: &X509Ref, memory: &Arc<MemoryBudget>) -> Result<bool> {
    same_typed(left, right, memory).map_err(TlsConfigError::into_anomaly)
}

pub(super) fn same_typed(left: &X509Ref, right: &X509Ref, memory: &Arc<MemoryBudget>) -> TlsConfigResult<bool> {
    let left = decode(left, memory)?;
    let right = decode(right, memory)?;
    left.same_public_key(&right).map_err(TlsConfigError::from_tls)
}

fn decode(certificate: &X509Ref, memory: &Arc<MemoryBudget>) -> TlsConfigResult<CertificatePublicKeyEncoding> {
    let length = unsafe { openssl_sys::i2d_X509(certificate.as_ptr(), std::ptr::null_mut()) };
    let bound = usize::try_from(length).ok().and_then(|length| length.checked_add(128))
        .ok_or_else(|| TlsConfigError::invalid("TLS public-key certificate encoding exceeds allocation range"))?;
    let _encoding = memory.reserve(bound).map_err(TlsConfigError::Other)?;
    let encoded = certificate.to_der().map_err(TlsConfigError::invalid)?;
    CertificatePublicKeyEncoding::decode(&encoded, memory).map_err(TlsConfigError::from_tls)
}

#[cfg(test)]
mod tests {
    use super::*;
    use openssl::{hash::MessageDigest, pkey::PKey, rsa::Rsa};
    use crate::tls::tests::support::Credential;
    mod dh;

    fn der(tag: u8, content: &[u8]) -> Vec<u8> {
        let mut bytes = vec![tag];
        if content.len() < 128 { bytes.push(content.len() as u8); }
        else { bytes.extend_from_slice(&[0x82, (content.len() >> 8) as u8, content.len() as u8]); }
        bytes.extend_from_slice(content);
        bytes
    }
    fn pss(key: &[u8], parameters: &[u8]) -> Vec<u8> {
        let mut algorithm = der(6, &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 10]);
        algorithm.extend_from_slice(parameters);
        let mut content = der(0x30, &algorithm);
        let mut bits = vec![0]; bits.extend_from_slice(key);
        content.extend_from_slice(&der(3, &bits));
        der(0x30, &content)
    }
    fn parse(key: &[u8]) -> rtdb_tls::TlsResult<CertificatePublicKeyEncoding> {
        let mut signed = der(2, &[1]);
        for _ in 0..4 { signed.extend(der(0x30, &[])); }
        signed.extend_from_slice(key);
        let certificate = der(0x30, &[der(0x30, &signed), der(0x30, &[]), der(3, &[0])].concat());
        CertificatePublicKeyEncoding::decode(&certificate, &MemoryBudget::new(16 << 20))
    }
    fn parsed(key: &[u8]) -> CertificatePublicKeyEncoding { parse(key).unwrap() }

    #[test]
    fn pss_key_equality_preserves_restrictions_ignored_by_native_public_equality() {
        let rsa = Rsa::generate(2048).unwrap();
        let encoded = rsa.public_key_to_der_pkcs1().unwrap();
        let first = pss(&encoded, &[0x30, 5, 0xa2, 3, 2, 1, 20]);
        let second = pss(&encoded, &[0x30, 5, 0xa2, 3, 2, 1, 32]);
        let first_key = PKey::public_key_from_der(&first).unwrap();
        let second_key = PKey::public_key_from_der(&second).unwrap();
        assert!(first_key.public_eq(&second_key));
        assert!(parsed(&first) != parsed(&second));
        let issuer = Credential::new("issuer", false, None);
        let certificate = |key: &PKey<openssl::pkey::Public>| {
            let mut certificate = openssl::x509::X509::builder().unwrap();
            certificate.set_version(2).unwrap();
            certificate.set_serial_number(issuer.certificate.serial_number()).unwrap();
            certificate.set_subject_name(issuer.certificate.subject_name()).unwrap();
            certificate.set_issuer_name(issuer.certificate.subject_name()).unwrap();
            certificate.set_pubkey(key).unwrap();
            certificate.set_not_before(issuer.certificate.not_before()).unwrap();
            certificate.set_not_after(issuer.certificate.not_after()).unwrap();
            certificate.sign(&issuer.key, MessageDigest::sha256()).unwrap();
            certificate.build()
        };
        let first = certificate(&first_key); let second = certificate(&second_key);
        let memory = MemoryBudget::new(16 << 20);
        assert!(!same(&first, &second, &memory).unwrap());
        assert!(same(&first, &first, &memory).unwrap());
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn public_key_comparison_releases_encoding_admission_when_parser_admission_fails() {
        let credential = Credential::new("issuer", false, None);
        for limit in [0, credential.certificate.to_der().unwrap().len() + 128] {
            let memory = MemoryBudget::new(limit);
            assert_eq!(same(&credential.certificate, &credential.certificate, &memory).unwrap_err().code, "rtdb/memory-budget-exceeded");
            assert_eq!(memory.used_bytes(), 0);
        }
    }

    #[test]
    fn java_public_key_equality_normalizes_null_but_keeps_explicit_pss_defaults() {
        let absent = pss(&[0x30, 0], &[]);
        let null = pss(&[0x30, 0], &[5, 0]);
        let defaults = pss(&[0x30, 0], &[0x30, 0]);
        assert!(parsed(&absent) == parsed(&null));
        assert!(parsed(&absent) != parsed(&defaults));
        for count in 1..=4 {
            let mut parameters = vec![5, 0x80 | count];
            parameters.resize(usize::from(count) + 2, 0);
            assert!(parsed(&absent) == parsed(&pss(&[0x30, 0], &parameters)));
        }
    }

    #[test]
    fn java_public_key_null_parameters_reject_content_extra_values_and_invalid_lengths() {
        for parameters in [vec![5, 1, 0], vec![5, 0, 5, 0], vec![5, 0x81, 0, 5, 0],
            vec![5, 0x80, 0, 0], vec![5, 0x82, 0], vec![5, 0x85, 0, 0, 0, 0, 0]] {
            let key = pss(&[0x30, 0], &parameters);
            assert!(parse(&key).is_err(), "{parameters:?}");
        }
    }
}
