use super::*;

/// Immutable parsed Java subject-alternative-name extension value with independent allocation ownership.
/// The bytes retain their original inner encoding; they are not the normalized GeneralNames list.
#[derive(Debug)]
pub struct CertificateAlternativeNames { value: Option<Vec<u8>>, _reservation: Arc<MemoryReservation> }

impl CertificateAlternativeNames {
    /// Reads SAN from an already decoded X.509 certificate's encoding.
    ///
    /// - Malformed noncritical SAN is absent.
    /// - Malformed critical SAN and two parsed copies fail.
    /// - Signatures, validity, other extension contents and trust are not verified.
    /// - Parsing scratch and retained bytes own a reservation until this value drops.
    /// - Budget and environmental URI-resolution errors propagate unchanged.
    pub fn decode(certificate_der: &[u8], memory: &Arc<MemoryBudget>) -> TlsResult<Self> {
        let bound = certificate_der.len().checked_mul(512).and_then(|bytes| bytes.checked_add(65_536))
            .ok_or_else(|| TlsError::invalid_keystore("Certificate alternative names exceed allocation range"))?;
        let reservation = memory.reserve(bound).map_err(TlsError::Other)?;
        let mut value = None;
        scan_certificate(certificate_der, |oid, critical, encoded| {
            if oid != [0x55, 0x1d, 0x11] { return Ok(()); }
            match single(&encoded, 0x30).map_err(der_err).and_then(|names| general_names(&names, memory)) {
                Ok(()) => {
                    if value.is_some() { return Err(TlsError::invalid_keystore("Certificate contains duplicate parsed subject-alternative-name extensions")); }
                    value = Some(encoded);
                }
                Err(TlsError::InvalidKeystore(_)) if !critical => {},
                Err(error) => return Err(error),
            }
            Ok(())
        })?;
        Ok(Self { value, _reservation: reservation })
    }

    /// Borrows the original extension bytes when Java's SAN parser accepted a value; absence includes an unparseable noncritical extension.
    pub fn extension_value(&self) -> Option<&[u8]> { self.value.as_deref() }
}

#[cfg(test)]
mod tests;
