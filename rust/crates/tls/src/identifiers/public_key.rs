use super::*;
use crate::java_der::bit_layout;
mod comparison;

/// Immutable `X509Key` encoded-key normalization with independent allocation ownership.
/// `PartialEq` compares normalized encodings; `same_public_key` additionally applies the supported provider rules.
#[derive(Debug)]
pub struct CertificatePublicKeyEncoding { encoded: Vec<u8>, _reservation: Arc<MemoryReservation> }

impl CertificatePublicKeyEncoding {
    /// Reads SubjectPublicKeyInfo from an already decoded X.509 certificate's encoding.
    ///
    /// - Follows OpenJDK 25.0.1 `AlgorithmId`, `ObjectIdentifier`, `DerValue` and `BitArray` framing and encoding rules.
    /// - Provider parameter validation, key acceptance and certificate verification are outside decoding.
    /// - Invalid framing, OIDs and bit lengths return a TLS-material error.
    /// - Reserves before parsing and releases scratch admission before returning independently owned encoding bytes.
    /// - The borrowed input has its own owner, and budget errors propagate unchanged.
    pub fn decode(certificate_der: &[u8], memory: &Arc<MemoryBudget>) -> TlsResult<Self> {
        let bound = certificate_der.len().checked_mul(512).and_then(|bytes| bytes.checked_add(65_536))
            .ok_or_else(|| TlsError::invalid_keystore("Certificate public-key encoding exceeds allocation range"))?;
        let mut reservation = memory.reserve(bound).map_err(TlsError::Other)?;
        let encoded = decode_certificate(certificate_der).map_err(der_err)?;
        let retained = encoded.capacity().checked_add(std::mem::size_of::<Self>()).and_then(|bytes| bytes.checked_add(128))
            .ok_or_else(|| TlsError::invalid_keystore("Certificate public-key encoding exceeds allocation range"))?;
        Arc::get_mut(&mut reservation).expect("unpublished public-key reservation").shrink_to(retained).map_err(TlsError::Other)?;
        Ok(Self { encoded, _reservation: reservation })
    }

    /// Borrows the normalized SubjectPublicKeyInfo encoding through this value's reservation.
    pub fn encoded(&self) -> &[u8] { &self.encoded }

    /// Compares the pinned built-in key equality rules implemented for these encodings.
    ///
    /// - The PKCS#3 and X9.42 DH OIDs use SunJCE numeric `y`, `p` and `g` equality after validating both keys' DH fields.
    /// - Other OIDs use normalized `X509Key` encoding equality.
    /// - Invalid DH fields return a TLS-material error, including when an encoding is compared with itself.
    /// - Comparison borrows both values' admitted bytes without retaining additional state.
    /// - Other provider acceptance checks, custom provider equality and certificate admission remain outside this method.
    pub fn same_public_key(&self, other: &Self) -> TlsResult<bool> {
        let left = comparison::Key::read(self).map_err(der_err)?;
        let right = comparison::Key::read(other).map_err(der_err)?;
        Ok(left == right)
    }
}

impl PartialEq for CertificatePublicKeyEncoding { fn eq(&self, other: &Self) -> bool { self.encoded == other.encoded } }
impl Eq for CertificatePublicKeyEncoding {}

fn decode_certificate(bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.len() > i32::MAX as usize { return Err(malformed()); }
    let certificate = single(bytes, 0x30)?;
    let mut certificate = Der(&certificate);
    let signed = certificate.expect(0x30)?;
    let signed_tail = certificate.0.len();
    let mut signed = Der(&signed);
    if signed.0.first().is_some_and(|tag| tag & 0xdf == 0x80) { signed.next()?; }
    signed.expect(2)?;
    for _ in 0..4 { signed.expect(0x30)?; }
    let public_key = signed.expect(0x30)?;
    let public_key_tail = signed.0.len() + if matches!(public_key, Cow::Borrowed(_)) { signed_tail } else { 0 };
    normalize_key(&public_key, public_key_tail)
}

fn normalize_key(content: &[u8], backing_tail: usize) -> Result<Vec<u8>> {
    let mut input = Der(content);
    let algorithm = input.expect(0x30)?;
    let mut algorithm = Der(&algorithm);
    let oid = algorithm.expect(6)?;
    valid_oid(&oid)?;
    let mut normalized_algorithm = encoded(6, &oid);
    let parameter = if algorithm.0.is_empty() { None } else {
        let (tag, content) = algorithm.next()?;
        if tag == 5 {
            if !content.is_empty() { return Err(malformed()); }
            None
        } else { Some(encoded(tag, &content)) }
    };
    algorithm.end()?;
    if let Some(parameter) = parameter { normalized_algorithm.extend(parameter); }
    else if emits_null(&oid) { normalized_algorithm.extend_from_slice(&[5, 0]); }
    let bits = input.expect(3)?;
    input.end()?;
    let (&padding, bytes) = bits.split_first().ok_or_else(malformed)?;
    let (length, padding) = bit_layout(padding, bytes.len(), bytes.len() + backing_tail)?;
    let mut normalized_bits = Vec::with_capacity(length + 1);
    normalized_bits.push(padding);
    normalized_bits.extend_from_slice(&bytes[..length]);
    if padding != 0 { *normalized_bits.last_mut().expect("nonempty padded bit string") &= 0xff << padding; }
    let algorithm = encoded(0x30, &normalized_algorithm);
    let bits = encoded(3, &normalized_bits);
    Ok(encoded(0x30, &[algorithm, bits].concat()))
}

fn emits_null(oid: &[u8]) -> bool {
    matches!(oid,
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 1 | 2 | 4 | 5 | 11..=16]
        | [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 2, 2 | 5]
        | [0x2b, 0x0e, 3, 2, 0x1a]
        | [0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, 1..=10]
        | [0x60, 0x86, 0x48, 1, 0x65, 3, 4, 3, 13..=16])
}

#[cfg(test)]
mod tests;
