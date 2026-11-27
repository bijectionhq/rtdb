use super::*;

/// Immutable Java KeyUsage decisions with independent allocation ownership.
#[derive(Debug)]
pub struct CertificateKeyUsage { permissions: u8, _reservation: Arc<MemoryReservation> }

impl CertificateKeyUsage {
    /// Reads KeyUsage from an already decoded X.509 certificate's encoding.
    ///
    /// - Malformed noncritical values are absent.
    /// - Malformed critical values and duplicate parsed copies fail.
    /// - Signatures, validity, other extension contents and trust are not verified.
    /// - Parsing scratch owns a reservation until this value drops.
    /// - Budget errors propagate unchanged.
    pub fn decode(certificate_der: &[u8], memory: &Arc<MemoryBudget>) -> TlsResult<Self> {
        let bound = certificate_der.len().checked_mul(512).and_then(|bytes| bytes.checked_add(65_536))
            .ok_or_else(|| TlsError::invalid_keystore("Certificate key usage exceeds allocation range"))?;
        let reservation = memory.reserve(bound).map_err(TlsError::Other)?;
        let mut permits = None;
        scan_certificate(certificate_der, |oid, critical, encoded| {
            if oid != [0x55, 0x1d, 0x0f] { return Ok(()); }
            match decode_permissions(&encoded).map_err(der_err) {
                Ok(value) => {
                    if permits.is_some() { return Err(TlsError::invalid_keystore("Certificate contains duplicate parsed key-usage extensions")); }
                    permits = Some(value);
                }
                Err(TlsError::InvalidKeystore(_)) if !critical => {},
                Err(error) => return Err(error),
            }
            Ok(())
        })?;
        Ok(Self { permissions: permits.unwrap_or(0xff), _reservation: reservation })
    }

    /// Returns permissions for usage bits zero through seven in high-bit-first order.
    /// Missing or malformed noncritical usage returns 0xff; parsed bits outside the declared length are zero.
    pub fn permitted_bits(&self) -> u8 { self.permissions }

    /// Accepts absence or a parsed keyCertSign bit; missing bits in a parsed value are false.
    pub fn permits_certificate_signing(&self) -> bool { self.permissions & 4 != 0 }

    /// Accepts absence or a parsed digitalSignature bit; missing bits in a parsed value are false.
    pub fn permits_digital_signatures(&self) -> bool { self.permissions & 0x80 != 0 }

    /// Accepts absence or any parsed usage in bits zero through six, following Java AlgorithmChecker.
    /// encipherOnly and decipherOnly without another primitive do not grant permission.
    pub fn permits_cryptographic_primitive(&self) -> bool { self.permissions & 0xfe != 0 }
}

fn decode_permissions(encoded: &[u8]) -> Result<u8> {
    let unwrapped = if encoded.first() == Some(&4) { single(encoded, 4)? } else { Cow::Borrowed(encoded) };
    let bits = single(&unwrapped, 3)?;
    let (&padding, bytes) = bits.split_first().ok_or_else(malformed)?;
    let length = declared_bit_length(padding, bytes.len())?;
    let mask = if length == 0 { 0 } else { 0xff << (8 - length.min(8)) };
    Ok(bytes.first().copied().unwrap_or(0) & mask)
}

// KeyUsageExtension gives DerValue a standalone value array; BitArray's remaining backing length equals this BIT STRING's payload length.
fn declared_bit_length(padding: u8, bytes: usize) -> Result<usize> {
    if padding > 7 || bytes > i32::MAX as usize { return Err(malformed()); }
    let capacity = (bytes as i32).wrapping_mul(8);
    let length = capacity.wrapping_sub(i32::from(padding));
    if length < 0 || capacity < length { return Err(malformed()); }
    Ok(length as usize)
}

#[cfg(test)]
mod tests;
