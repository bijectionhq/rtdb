use std::{borrow::Cow, sync::Arc};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, Result, UriParts};
use crate::{DistinguishedName, TlsError, TlsResult, invalid};
use crate::java_der::{Der, encoded, malformed, octets, single, valid_oid};

mod alternative_names;
pub use alternative_names::CertificateAlternativeNames;
mod basic_constraints;
pub use basic_constraints::CertificateBasicConstraints;
mod key_usage;
pub use key_usage::CertificateKeyUsage;
mod extended_key_usage;
pub use extended_key_usage::CertificateExtendedKeyUsage;
mod public_key;
pub use public_key::CertificatePublicKeyEncoding;

/// Immutable Java certificate identifiers, independently charged through their last borrowed use.
/// Serial numbers use minimal signed two's-complement bytes, including a sign byte when required.
#[derive(Debug)]
pub struct CertificateIdentifiers {
    subject: Option<Vec<u8>>,
    authority: Option<Authority>,
    serial: Vec<u8>,
    _reservation: Arc<MemoryReservation>,
}
#[derive(Debug)]
struct Authority { key: Option<Vec<u8>>, serial: Option<Vec<u8>> }

impl CertificateIdentifiers {
    /// Reads identifier extensions from an already decoded X.509 certificate's encoding.
    /// This does not verify its signature, validity, other extension contents or trust.
    /// Malformed noncritical identifiers follow Java's absence/fallback rules; malformed critical identifiers and duplicate parsed identifiers fail.
    /// Parsing and retained bytes own an independent reservation; budget errors remain budget errors.
    /// URI authority names requiring interface lookup or DNS return `rtdb.tls/unsupported-identifier`.
    pub fn decode(certificate_der: &[u8], memory: &Arc<MemoryBudget>) -> TlsResult<Self> {
        let bound = certificate_der.len().checked_mul(512).and_then(|bytes| bytes.checked_add(65_536))
            .ok_or_else(|| TlsError::invalid_keystore("Certificate identifiers exceed allocation range"))?;
        let reservation = memory.reserve(bound).map_err(TlsError::Other)?;
        let mut subject = None;
        let mut unparseable_subject = None;
        let mut authority = None;
        let serial = scan_certificate(certificate_der, |oid, critical, value| {
            match oid {
                [0x55, 0x1d, 0x0e] => match subject_identifier(&value) {
                    Ok(()) => {
                        if subject.is_some() { return Err(duplicate()); }
                        subject = Some(value);
                    }
                    Err(TlsError::InvalidKeystore(_)) if !critical => unparseable_subject = Some(value),
                    Err(error) => return Err(error),
                },
                [0x55, 0x1d, 0x23] => match authority_identifier(&value, memory) {
                    Ok(value) => {
                        if authority.is_some() { return Err(duplicate()); }
                        authority = Some(value);
                    }
                    Err(TlsError::InvalidKeystore(_)) if !critical => {},
                    Err(error) => return Err(error),
                },
                _ => {},
            }
            Ok(())
        })?;
        Ok(Self { subject: subject.or(unparseable_subject), authority, serial, _reservation: reservation })
    }

    /// Original inner SKID bytes; a parsed extension takes precedence over malformed noncritical copies.
    /// If none parses, returns the last malformed noncritical copy, as Java `getExtensionValue` does.
    pub fn subject_key_identifier(&self) -> Option<&[u8]> { self.subject.as_deref() }
    /// Canonical DER OCTET STRING from a completely parsed AKI; malformed noncritical AKI is absent.
    pub fn authority_key_identifier(&self) -> Option<&[u8]> { self.authority.as_ref().and_then(|authority| authority.key.as_deref()) }
    /// Signed AKI serial from the same completely parsed extension as `authority_key_identifier`.
    pub fn authority_serial_number(&self) -> Option<&[u8]> { self.authority.as_ref().and_then(|authority| authority.serial.as_deref()) }
    pub fn serial_number(&self) -> &[u8] { &self.serial }
}

fn der_err(error: Anomaly) -> TlsError { TlsError::from_invalid_anomaly(error) }

fn scan_certificate(certificate_der: &[u8], mut visit: impl FnMut(&[u8], bool, Vec<u8>) -> TlsResult<()>) -> TlsResult<Vec<u8>> {
    let certificate = single(certificate_der, 0x30).map_err(der_err)?;
    let mut certificate = Der(&certificate);
    let tbs = certificate.expect(0x30).map_err(der_err)?;
    certificate.expect(0x30).map_err(der_err)?;
    certificate.expect(3).map_err(der_err)?;
    certificate.end().map_err(der_err)?;
    let mut tbs = Der(&tbs);
    if tbs.0.first().is_some_and(|tag| tag & 0xdf == 0x80) { tbs.next().map_err(der_err)?; }
    let serial = signed(&tbs.expect(2).map_err(der_err)?).map_err(der_err)?;
    for _ in 0..5 { tbs.expect(0x30).map_err(der_err)?; }
    for tag in [1, 2] {
        if tbs.0.first().is_some_and(|value| value & 0xdf == (0x80 | tag)) { tbs.next().map_err(der_err)?; }
    }
    if !tbs.0.is_empty() {
        let (tag, extensions) = tbs.next().map_err(der_err)?;
        if tag == 0xa3 {
            let extensions = Der(&extensions).expect(0x30).map_err(der_err)?;
            let mut extensions = Der(&extensions);
            while !extensions.0.is_empty() {
                let extension = extensions.next().map_err(der_err)?;
                if !matches!(extension.0, 0x30 | 0x31) { return Err(der_err(malformed())); }
                let mut extension = Der(&extension.1);
                let oid = extension.expect(6).map_err(der_err)?;
                valid_oid(&oid).map_err(der_err)?;
                let (tag, content) = extension.next().map_err(der_err)?;
                let (critical, value) = if tag == 1 {
                    if content.len() != 1 { return Err(der_err(malformed())); }
                    (content[0] != 0, octets(extension.next().map_err(der_err)?).map_err(der_err)?)
                } else { (false, octets((tag, content)).map_err(der_err)?) };
                visit(&oid, critical, value)?;
            }
        }
    }
    Ok(serial)
}

fn signed(mut bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.is_empty() { return Err(malformed()); }
    while bytes.len() > 1 && ((bytes[0] == 0 && bytes[1] & 0x80 == 0) || (bytes[0] == 0xff && bytes[1] & 0x80 != 0)) { bytes = &bytes[1..]; }
    Ok(bytes.to_vec())
}

fn subject_identifier(bytes: &[u8]) -> TlsResult<()> {
    let mut encoded = Der(bytes);
    let value = encoded.next().map_err(der_err)?;
    encoded.end().map_err(der_err)?;
    octets(value).map_err(der_err)?;
    Ok(())
}

fn authority_identifier(bytes: &[u8], memory: &Arc<MemoryBudget>) -> TlsResult<Authority> {
    let sequence = single(bytes, 0x30).map_err(der_err)?;
    let mut fields = Der(&sequence);
    let mut result = Authority { key: None, serial: None };
    let mut names = false;
    while !fields.0.is_empty() {
        let (tag, content) = fields.next().map_err(der_err)?;
        match tag {
            0x80 if result.key.is_none() => result.key = Some(encoded(4, &content)),
            0xa1 if !names => { general_names(&content, memory)?; names = true; },
            0x82 if result.serial.is_none() => result.serial = Some(signed(&content).map_err(der_err)?),
            _ => return Err(der_err(malformed())),
        }
    }
    Ok(result)
}

fn general_names(bytes: &[u8], memory: &Arc<MemoryBudget>) -> TlsResult<()> {
    if bytes.is_empty() { return Err(der_err(malformed())); }
    let mut names = Der(bytes);
    while !names.0.is_empty() {
        let (tag, content) = names.next().map_err(der_err)?;
        match tag {
            0xa0 => {
                let mut name = Der(&content);
                let oid = name.expect(6).map_err(der_err)?;
                valid_oid(&oid).map_err(der_err)?;
                name.expect(0xa0).map_err(der_err)?;
                if builtin_extension_oid(&oid) { return Err(der_err(malformed())); }
            },
            0x81 => {
                let name = ascii(&content);
                let domain = name.find('@').map_or(name.as_str(), |index| &name[index + 1..]);
                if domain.is_empty() || domain == "." { return Err(der_err(malformed())); }
            },
            0x82 | 0xa3 => {},
            0xa4 => { DistinguishedName::decode(&content, memory)?; },
            0xa5 => edi_name(&content).map_err(der_err)?,
            0x86 => uri_name(&content)?,
            0x87 if matches!(content.len(), 4 | 8 | 16 | 32) => {},
            0x88 => valid_oid(&content).map_err(der_err)?,
            _ => return Err(der_err(malformed())),
        }
    }
    Ok(())
}

fn edi_name(bytes: &[u8]) -> Result<()> {
    let mut fields = Der(bytes);
    let mut count = 0;
    let mut assigner = false;
    let mut party = false;
    while !fields.0.is_empty() {
        count += 1;
        if count > 2 { return Err(malformed()); }
        let (tag, content) = fields.next()?;
        let target = match tag { 0x80 => &mut assigner, 0x81 => &mut party, _ => continue };
        if *target { return Err(malformed()); }
        let (tag, _) = Der(&content).next()?;
        *target = matches!(tag, 12 | 19 | 20 | 22 | 27 | 28 | 30);
    }
    if party { Ok(()) } else { Err(malformed()) }
}

fn uri_name(bytes: &[u8]) -> TlsResult<()> {
    let text = ascii(bytes);
    let uri = UriParts::parse(&text).map_err(TlsError::invalid_keystore)?;
    if uri.scheme().is_none() { return Err(der_err(malformed())); }
    if let Some(host) = uri.host() {
        if let Some(address) = host.strip_prefix('[').and_then(|host| host.strip_suffix(']')) {
            if let Some((address, scope)) = address.split_once('%') {
                let mapped = ipv4_mapped(address).map_err(der_err)?;
                if mapped { return Err(der_err(malformed())); }
                if scope.bytes().try_fold(0i32, |zone, byte| {
                    if !byte.is_ascii_digit() { None } else { zone.checked_mul(10)?.checked_add(i32::from(byte - b'0')) }
                }).is_none() {
                    return Err(TlsError::unsupported_identifier("Certificate authority URI requires IPv6 interface-scope resolution"));
                }
            }
        } else if host.ends_with('.') {
            return Err(TlsError::unsupported_identifier("Certificate authority URI requires Java host-resolution fallback"));
        }
    }
    Ok(())
}

fn ipv4_mapped(address: &str) -> Result<bool> {
    let normalized;
    let address = if let Some((prefix, tail)) = address.rsplit_once(':').filter(|(_, tail)| tail.contains('.')) {
        let mut parts = tail.split('.');
        let mut bytes = [0u8; 4];
        for byte in &mut bytes {
            *byte = parts.next().ok_or_else(malformed)?.parse::<u8>().map_err(invalid)?;
        }
        if parts.next().is_some() { return Err(malformed()); }
        normalized = format!("{prefix}:{:x}:{:x}", u16::from_be_bytes([bytes[0], bytes[1]]), u16::from_be_bytes([bytes[2], bytes[3]]));
        normalized.as_str()
    } else { address };
    let address = address.parse::<std::net::Ipv6Addr>().map_err(invalid)?;
    Ok(address.to_ipv4_mapped().is_some())
}

fn ascii(bytes: &[u8]) -> String { bytes.iter().map(|byte| if byte.is_ascii() { char::from(*byte) } else { '\u{fffd}' }).collect() }

// OpenJDK 25.0.1 OIDMap classes are Extensions, so OtherName's GeneralNameInterface cast fails for these OIDs.
fn builtin_extension_oid(oid: &[u8]) -> bool {
    match oid {
        [0x55, 0x1d, 14 | 15 | 16 | 17 | 18 | 19 | 20 | 21 | 27 | 28 | 29 | 30 | 31 | 32 | 33 | 35 | 36 | 37 | 46 | 54] => true,
        [0x2b, 6, 1, 5, 5, 7, 1, 1 | 11] => true,
        [0x2b, 6, 1, 5, 5, 7, 0x30, 1, 5] => true,
        [0x60, 0x86, 0x48, 1, 0x86, 0xf8, 0x42, 1, 1] => true,
        _ => false,
    }
}

fn duplicate() -> TlsError { TlsError::invalid_keystore("Certificate contains duplicate parsed identifier extensions") }

#[cfg(test)]
mod tests;
