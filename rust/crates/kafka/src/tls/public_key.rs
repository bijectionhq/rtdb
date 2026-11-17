use openssl::x509::X509Ref;
use rtdb_types::Result;
use super::invalid;

pub(super) fn same(left: &X509Ref, right: &X509Ref) -> Result<bool> {
    let left = left.to_der().map_err(invalid)?;
    let right = right.to_der().map_err(invalid)?;
    Ok(Key::from_certificate(&left)? == Key::from_certificate(&right)?)
}

// OpenJDK 25.0.1 X509Key.equals and AlgorithmId.parse/encode compare encoded keys after NULL parameter normalization.
#[derive(PartialEq, Eq)]
struct Key<'a> { algorithm: &'a [u8], parameters: &'a [u8], bits: &'a [u8] }
impl<'a> Key<'a> {
    fn from_certificate(certificate: &'a [u8]) -> Result<Self> {
        let mut certificate = Der(certificate);
        let mut certificate = Der(certificate.value(0x30)?);
        let mut signed = Der(certificate.value(0x30)?);
        if signed.0.first() == Some(&0xa0) { signed.value(0xa0)?; }
        signed.value(2)?;
        for _ in 0..4 { signed.value(0x30)?; }
        Self::read(signed.value(0x30)?)
    }

    fn read(contents: &'a [u8]) -> Result<Self> {
        let mut input = Der(contents);
        let mut algorithm = Der(input.value(0x30)?);
        let identifier = algorithm.value(6)?;
        let parameters = match algorithm.0 { [] | [5, 0] => &[][..], other => other };
        let bits = input.value(3)?;
        if !input.0.is_empty() || bits.first() != Some(&0) { return Err(invalid("TLS public key has an invalid bit-string encoding")); }
        Ok(Self { algorithm: identifier, parameters, bits })
    }
}

struct Der<'a>(&'a [u8]);
impl<'a> Der<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let (head, tail) = self.0.split_at_checked(length).ok_or_else(|| invalid("TLS public-key encoding is truncated"))?;
        self.0 = tail;
        Ok(head)
    }
    fn value(&mut self, tag: u8) -> Result<&'a [u8]> {
        if self.take(1)?[0] != tag { return Err(invalid("TLS public-key encoding has an unexpected tag")); }
        let length = self.take(1)?[0];
        let length = if length < 128 { usize::from(length) } else {
            let count = usize::from(length & 127);
            if count == 0 || count > std::mem::size_of::<usize>() { return Err(invalid("TLS public-key encoding has an invalid length")); }
            self.take(count)?.iter().try_fold(0usize, |value, byte| value.checked_mul(256).and_then(|value| value.checked_add(usize::from(*byte))).ok_or_else(|| invalid("TLS public-key length exceeds allocation range")))?
        };
        self.take(length)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openssl::{hash::MessageDigest, pkey::PKey, rsa::Rsa};
    use crate::tls::tests::support::Credential;

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
    fn parsed(key: &[u8]) -> Key<'_> { Key::read(Der(key).value(0x30).unwrap()).unwrap() }

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
        assert!(!same(&first, &second).unwrap());
        assert!(same(&first, &first).unwrap());
    }

    #[test]
    fn java_public_key_equality_normalizes_null_but_keeps_explicit_pss_defaults() {
        let absent = pss(&[0x30, 0], &[]);
        let null = pss(&[0x30, 0], &[5, 0]);
        let defaults = pss(&[0x30, 0], &[0x30, 0]);
        assert!(parsed(&absent) == parsed(&null));
        assert!(parsed(&absent) != parsed(&defaults));
    }
}
