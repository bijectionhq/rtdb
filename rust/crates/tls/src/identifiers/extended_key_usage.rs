use super::*;

/// Immutable Java extended-key-usage state with independent allocation ownership.
#[derive(Debug)]
pub struct CertificateExtendedKeyUsage { usage: Usage, _reservation: Arc<MemoryReservation> }
#[derive(Clone, Copy, Debug)]
enum Usage { Absent, Parsed { server_authentication: bool, client_authentication: bool }, Unparseable }

impl CertificateExtendedKeyUsage {
    /// Reads ExtendedKeyUsage from an already decoded X.509 certificate's encoding.
    ///
    /// - Malformed noncritical values remain unparseable unless another copy parses.
    /// - Malformed critical values and duplicate parsed copies fail.
    /// - Every parsed usage OID is validated, including unrecognized purposes.
    /// - Signatures, validity, other extension contents and trust are not verified.
    /// - Parsing scratch owns a reservation until this value drops, and budget errors propagate unchanged.
    pub fn decode(certificate_der: &[u8], memory: &Arc<MemoryBudget>) -> TlsResult<Self> {
        let bound = certificate_der.len().checked_mul(512).and_then(|bytes| bytes.checked_add(65_536))
            .ok_or_else(|| TlsError::invalid_keystore("Certificate extended key usage exceeds allocation range"))?;
        let reservation = memory.reserve(bound).map_err(TlsError::Other)?;
        let mut usage = Usage::Absent;
        scan_certificate(certificate_der, |oid, critical, encoded| {
            if oid != [0x55, 0x1d, 0x25] { return Ok(()); }
            match decode_authentication(&encoded).map_err(der_err) {
                Ok((server_authentication, client_authentication)) => {
                    if matches!(usage, Usage::Parsed { .. }) { return Err(TlsError::invalid_keystore("Certificate contains duplicate parsed extended-key-usage extensions")); }
                    usage = Usage::Parsed { server_authentication, client_authentication };
                }
                Err(TlsError::InvalidKeystore(_)) if !critical => {
                    if !matches!(usage, Usage::Parsed { .. }) { usage = Usage::Unparseable; }
                }
                Err(error) => return Err(error),
            }
            Ok(())
        })?;
        Ok(Self { usage, _reservation: reservation })
    }

    /// Applies Java EndEntityChecker's TLS server extended-usage decision.
    ///
    /// - Absence, anyExtendedKeyUsage, serverAuth and the Microsoft/Netscape SGC purposes permit server authentication.
    /// - Other parsed lists, including an empty list, deny permission.
    /// - Unparseable-only noncritical copies return a TLS-material error, following X509CertImpl.getExtendedKeyUsage.
    pub fn permits_server_authentication(&self) -> TlsResult<bool> {
        match self.usage {
            Usage::Absent => Ok(true),
            Usage::Parsed { server_authentication, .. } => Ok(server_authentication),
            Usage::Unparseable => Err(TlsError::invalid_keystore("Certificate extended key usage is unparseable")),
        }
    }

    /// Applies Java X509KeyManagerImpl's TLS client extended-usage decision.
    ///
    /// - Absence, anyExtendedKeyUsage and clientAuth permit client authentication.
    /// - Other parsed lists, including server-only and SGC purposes, deny permission.
    /// - Unparseable-only noncritical copies return a TLS-material error, following X509CertImpl.getExtendedKeyUsage.
    pub fn permits_client_authentication(&self) -> TlsResult<bool> {
        match self.usage {
            Usage::Absent => Ok(true),
            Usage::Parsed { client_authentication, .. } => Ok(client_authentication),
            Usage::Unparseable => Err(TlsError::invalid_keystore("Certificate extended key usage is unparseable")),
        }
    }
}

fn decode_authentication(encoded: &[u8]) -> Result<(bool, bool)> {
    let sequence = single(encoded, 0x30)?;
    let mut usages = Der(&sequence);
    let mut server = false;
    let mut client = false;
    while !usages.0.is_empty() {
        let oid = usages.expect(6)?;
        valid_oid(&oid)?;
        server |= matches!(oid.as_ref(),
            [0x55, 0x1d, 0x25, 0]
            | [0x2b, 6, 1, 5, 5, 7, 3, 1]
            | [0x2b, 6, 1, 4, 1, 0x82, 0x37, 0x0a, 3, 3]
            | [0x60, 0x86, 0x48, 1, 0x86, 0xf8, 0x42, 4, 1]);
        client |= matches!(oid.as_ref(), [0x55, 0x1d, 0x25, 0] | [0x2b, 6, 1, 5, 5, 7, 3, 2]);
    }
    Ok((server, client))
}

#[cfg(test)]
mod tests;
