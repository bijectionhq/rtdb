use super::*;

/// Immutable Java BasicConstraints result with independent allocation ownership.
#[derive(Debug)]
pub struct CertificateBasicConstraints { path_length: i32, _reservation: Arc<MemoryReservation> }

impl CertificateBasicConstraints {
    /// Reads BasicConstraints from an already decoded X.509 certificate's encoding.
    ///
    /// - Malformed noncritical values are absent.
    /// - Malformed critical values and duplicate parsed copies fail.
    /// - Signatures, validity, other extension contents and trust are not verified.
    /// - Parsing scratch owns a reservation until this value drops.
    /// - Budget errors propagate unchanged.
    pub fn decode(certificate_der: &[u8], memory: &Arc<MemoryBudget>) -> TlsResult<Self> {
        let bound = certificate_der.len().checked_mul(512).and_then(|bytes| bytes.checked_add(65_536))
            .ok_or_else(|| TlsError::invalid_keystore("Certificate basic constraints exceed allocation range"))?;
        let reservation = memory.reserve(bound).map_err(TlsError::Other)?;
        let mut path_length = None;
        scan_certificate(certificate_der, |oid, critical, encoded| {
            if oid != [0x55, 0x1d, 0x13] { return Ok(()); }
            match decode_path_length(&encoded).map_err(der_err) {
                Ok(value) => {
                    if path_length.is_some() { return Err(TlsError::invalid_keystore("Certificate contains duplicate parsed basic-constraints extensions")); }
                    path_length = Some(value);
                }
                Err(TlsError::InvalidKeystore(_)) if !critical => {},
                Err(error) => return Err(error),
            }
            Ok(())
        })?;
        Ok(Self { path_length: path_length.unwrap_or(-1), _reservation: reservation })
    }

    /// Java's signed path-length result: `-1` for absent or non-CA constraints, `i32::MAX` for a CA without a limit.
    /// Explicit negative path lengths remain negative.
    pub fn path_length(&self) -> i32 { self.path_length }
}

fn decode_path_length(encoded: &[u8]) -> Result<i32> {
    let sequence = single(encoded, 0x30)?;
    let mut fields = Der(&sequence);
    if fields.0.is_empty() { return Ok(-1); }
    let (tag, value) = fields.next()?;
    if tag != 1 { return Ok(-1); }
    if value.len() != 1 { return Err(malformed()); }
    let ca = value[0] != 0;
    let path_length = if fields.0.is_empty() { i32::MAX } else {
        let integer = fields.expect(2)?;
        let integer = signed(&integer)?;
        if integer.len() > 4 { return Err(malformed()); }
        let mut value = if integer[0] & 0x80 == 0 { [0; 4] } else { [0xff; 4] };
        value[4 - integer.len()..].copy_from_slice(&integer);
        i32::from_be_bytes(value)
    };
    Ok(if ca { path_length } else { -1 })
}

#[cfg(test)]
mod tests;
