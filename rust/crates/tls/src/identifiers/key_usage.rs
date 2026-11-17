use super::*;

/// Immutable Java CA KeyUsage decision with independent allocation ownership.
#[derive(Debug)]
pub struct CertificateKeyUsage { permits_certificate_signing: bool, _reservation: Arc<MemoryReservation> }

impl CertificateKeyUsage {
    /// Reads KeyUsage from an already decoded X.509 certificate's encoding.
    ///
    /// - Malformed noncritical values are absent.
    /// - Malformed critical values and duplicate parsed copies fail.
    /// - Signatures, validity, other extension contents and trust are not verified.
    /// - Parsing scratch owns a reservation until this value drops.
    /// - Budget errors propagate unchanged.
    pub fn decode(certificate_der: &[u8], memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bound = certificate_der.len().checked_mul(512).and_then(|bytes| bytes.checked_add(65_536))
            .ok_or_else(|| invalid("Certificate key usage exceeds allocation range"))?;
        let reservation = memory.reserve(bound)?;
        let mut permits = None;
        scan_certificate(certificate_der, |oid, critical, encoded| {
            if oid != [0x55, 0x1d, 0x0f] { return Ok(()); }
            match decode_certificate_signing(&encoded) {
                Ok(value) => {
                    if permits.is_some() { return Err(invalid("Certificate contains duplicate parsed key-usage extensions")); }
                    permits = Some(value);
                }
                Err(error) if !critical && error.code == "rtdb.tls/invalid-keystore" => {},
                Err(error) => return Err(error),
            }
            Ok(())
        })?;
        Ok(Self { permits_certificate_signing: permits.unwrap_or(true), _reservation: reservation })
    }

    /// Accepts absence or a parsed keyCertSign bit; missing bits in a parsed value are false.
    pub fn permits_certificate_signing(&self) -> bool { self.permits_certificate_signing }
}

fn decode_certificate_signing(encoded: &[u8]) -> Result<bool> {
    let unwrapped = if encoded.first() == Some(&4) { single(encoded, 4)? } else { Cow::Borrowed(encoded) };
    let bits = single(&unwrapped, 3)?;
    let (&padding, bytes) = bits.split_first().ok_or_else(malformed)?;
    if padding > 7 { return Err(malformed()); }
    let length = (bytes.len() as i32).wrapping_shl(3).wrapping_sub(i32::from(padding));
    if length < 0 { return Err(malformed()); }
    Ok(length > 5 && bytes[0] & 4 != 0)
}

#[cfg(test)]
mod tests;
