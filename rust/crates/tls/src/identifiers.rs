use std::{borrow::Cow, sync::Arc};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, Result, UriParts};
use crate::{DistinguishedName, invalid};

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
    pub fn decode(certificate_der: &[u8], memory: &Arc<MemoryBudget>) -> Result<Self> {
        let bound = certificate_der.len().checked_mul(512).and_then(|bytes| bytes.checked_add(65_536))
            .ok_or_else(|| invalid("Certificate identifiers exceed allocation range"))?;
        let reservation = memory.reserve(bound)?;
        let certificate = single(certificate_der, 0x30)?;
        let mut certificate = Der(&certificate);
        let tbs = certificate.expect(0x30)?;
        certificate.expect(0x30)?;
        certificate.expect(3)?;
        certificate.end()?;
        let mut tbs = Der(&tbs);
        if tbs.0.first().is_some_and(|tag| tag & 0xdf == 0x80) { tbs.next()?; }
        let serial = signed(&tbs.expect(2)?)?;
        for _ in 0..5 { tbs.expect(0x30)?; }
        let mut subject = None;
        let mut unparseable_subject = None;
        let mut authority = None;
        for tag in [1, 2] {
            if tbs.0.first().is_some_and(|value| value & 0xdf == (0x80 | tag)) { tbs.next()?; }
        }
        if !tbs.0.is_empty() {
            let (tag, extensions) = tbs.next()?;
            if tag == 0xa3 {
                let extensions = Der(&extensions).expect(0x30)?;
                let mut extensions = Der(&extensions);
                while !extensions.0.is_empty() {
                    let extension = extensions.next()?;
                    if !matches!(extension.0, 0x30 | 0x31) { return Err(malformed()); }
                    let mut extension = Der(&extension.1);
                    let oid = extension.expect(6)?;
                    valid_oid(&oid)?;
                    let (tag, content) = extension.next()?;
                    let (critical, value) = if tag == 1 {
                        if content.len() != 1 { return Err(malformed()); }
                        (content[0] != 0, octets(extension.next()?)?)
                    } else { (false, octets((tag, content))?) };
                    match oid.as_ref() {
                        [0x55, 0x1d, 0x0e] => match subject_identifier(&value) {
                            Ok(()) => {
                                if subject.is_some() { return Err(duplicate()); }
                                subject = Some(value);
                            }
                            Err(error) if !critical && error.code == "rtdb.tls/invalid-keystore" => unparseable_subject = Some(value),
                            Err(error) => return Err(error),
                        },
                        [0x55, 0x1d, 0x23] => match authority_identifier(&value, memory) {
                            Ok(value) => {
                                if authority.is_some() { return Err(duplicate()); }
                                authority = Some(value);
                            }
                            Err(error) if !critical && error.code == "rtdb.tls/invalid-keystore" => {},
                            Err(error) => return Err(error),
                        },
                        _ => {},
                    }
                }
            }
        }
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

fn signed(mut bytes: &[u8]) -> Result<Vec<u8>> {
    if bytes.is_empty() { return Err(malformed()); }
    while bytes.len() > 1 && ((bytes[0] == 0 && bytes[1] & 0x80 == 0) || (bytes[0] == 0xff && bytes[1] & 0x80 != 0)) { bytes = &bytes[1..]; }
    Ok(bytes.to_vec())
}

fn subject_identifier(bytes: &[u8]) -> Result<()> {
    let mut encoded = Der(bytes);
    let value = encoded.next()?;
    encoded.end()?;
    octets(value)?;
    Ok(())
}

fn authority_identifier(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<Authority> {
    let sequence = single(bytes, 0x30)?;
    let mut fields = Der(&sequence);
    let mut result = Authority { key: None, serial: None };
    let mut names = false;
    while !fields.0.is_empty() {
        let (tag, content) = fields.next()?;
        match tag {
            0x80 if result.key.is_none() => result.key = Some(encoded(4, &content)),
            0xa1 if !names => { general_names(&content, memory)?; names = true; },
            0x82 if result.serial.is_none() => result.serial = Some(signed(&content)?),
            _ => return Err(malformed()),
        }
    }
    Ok(result)
}

fn general_names(bytes: &[u8], memory: &Arc<MemoryBudget>) -> Result<()> {
    if bytes.is_empty() { return Err(malformed()); }
    let mut names = Der(bytes);
    while !names.0.is_empty() {
        let (tag, content) = names.next()?;
        match tag {
            0xa0 => {
                let mut name = Der(&content);
                let oid = name.expect(6)?;
                valid_oid(&oid)?;
                name.expect(0xa0)?;
                if builtin_extension_oid(&oid) { return Err(malformed()); }
            },
            0x81 => {
                let name = ascii(&content);
                let domain = name.find('@').map_or(name.as_str(), |index| &name[index + 1..]);
                if domain.is_empty() || domain == "." { return Err(malformed()); }
            },
            0x82 | 0xa3 => {},
            0xa4 => { DistinguishedName::decode(&content, memory)?; },
            0xa5 => edi_name(&content)?,
            0x86 => uri_name(&content)?,
            0x87 if matches!(content.len(), 4 | 8 | 16 | 32) => {},
            0x88 => valid_oid(&content)?,
            _ => return Err(malformed()),
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

fn uri_name(bytes: &[u8]) -> Result<()> {
    let text = ascii(bytes);
    let uri = UriParts::parse(&text).map_err(invalid)?;
    if uri.scheme().is_none() { return Err(malformed()); }
    if let Some(host) = uri.host() {
        if let Some(address) = host.strip_prefix('[').and_then(|host| host.strip_suffix(']')) {
            if let Some((address, scope)) = address.split_once('%') {
                let mapped = ipv4_mapped(address)?;
                if mapped { return Err(malformed()); }
                if scope.bytes().try_fold(0i32, |zone, byte| {
                    if !byte.is_ascii_digit() { None } else { zone.checked_mul(10)?.checked_add(i32::from(byte - b'0')) }
                }).is_none() {
                    return Err(Anomaly::unsupported("rtdb.tls/unsupported-identifier", "Certificate authority URI requires IPv6 interface-scope resolution"));
                }
            }
        } else if host.ends_with('.') {
            return Err(Anomaly::unsupported("rtdb.tls/unsupported-identifier", "Certificate authority URI requires Java host-resolution fallback"));
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

fn valid_oid(bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() || bytes.len() > 4096 || bytes.last().is_some_and(|byte| byte & 0x80 != 0)
        || bytes.iter().enumerate().any(|(index, byte)| *byte == 0x80 && (index == 0 || bytes[index - 1] & 0x80 == 0)) { return Err(malformed()); }
    Ok(())
}

fn octets(value: (u8, Cow<'_, [u8]>)) -> Result<Vec<u8>> {
    let mut pending = vec![(value.0, value.1.into_owned())];
    let mut output = Vec::new();
    while let Some((tag, content)) = pending.pop() {
        if tag == 4 { output.extend_from_slice(&content); }
        else if tag & 0x3f == 0x24 {
            let start = pending.len();
            let mut parts = Der(&content);
            while !parts.0.is_empty() { let (tag, content) = parts.next()?; pending.push((tag, content.into_owned())); }
            pending[start..].reverse();
        } else { return Err(malformed()); }
    }
    Ok(output)
}

fn single(bytes: &[u8], tag: u8) -> Result<Cow<'_, [u8]>> {
    let mut encoded = Der(bytes);
    let content = encoded.expect(tag)?;
    encoded.end()?;
    Ok(content)
}

fn encoded(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(content.len() + 6);
    encoded.push(tag); write_length(&mut encoded, content.len()); encoded.extend_from_slice(content); encoded
}

struct Der<'a>(&'a [u8]);
impl<'a> Der<'a> {
    fn next(&mut self) -> Result<(u8, Cow<'a, [u8]>)> {
        let bytes = self.0;
        if bytes.len() < 2 || bytes[0] & 31 == 31 { return Err(malformed()); }
        let tag = bytes[0];
        let (content, remaining) = if bytes[1] == 0x80 {
            if tag & 0x20 == 0 { return Err(malformed()); }
            let (mut converted, consumed) = convert_indefinite(bytes)?;
            let mut header = 1;
            let length = definite_length(&converted, &mut header)?;
            converted.copy_within(header..header + length, 0);
            converted.truncate(length);
            (Cow::Owned(converted), &bytes[consumed..])
        } else {
            let mut header = 1;
            let length = definite_length(bytes, &mut header)?;
            let end = header.checked_add(length).filter(|end| *end <= bytes.len()).ok_or_else(malformed)?;
            (Cow::Borrowed(&bytes[header..end]), &bytes[end..])
        };
        self.0 = remaining;
        Ok((tag, content))
    }
    fn expect(&mut self, expected: u8) -> Result<Cow<'a, [u8]>> {
        let (tag, content) = self.next()?;
        if tag == expected { Ok(content) } else { Err(malformed()) }
    }
    fn end(self) -> Result<()> { if self.0.is_empty() { Ok(()) } else { Err(malformed()) } }
}

fn definite_length(bytes: &[u8], offset: &mut usize) -> Result<usize> {
    let first = *bytes.get(*offset).ok_or_else(malformed)?;
    *offset += 1;
    if first < 0x80 { return Ok(usize::from(first)); }
    let count = usize::from(first & 0x7f);
    if count == 0 || count > 4 || bytes.len() - *offset < count { return Err(malformed()); }
    let length = bytes[*offset..*offset + count].iter().fold(0usize, |length, byte| length * 256 + usize::from(*byte));
    if length > i32::MAX as usize { return Err(malformed()); }
    *offset += count;
    Ok(length)
}

#[derive(Clone, Copy)]
enum Section { Open { parent: Option<usize>, content_start: usize, previous_change: isize }, Closed(usize) }

// OpenJDK 25.0.1 DerIndefLenConverter skips definite bodies and retains padding when their length headers shrink.
fn convert_indefinite(bytes: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut sections = Vec::<Section>::new();
    let mut current: Option<usize> = None;
    let mut change = 0isize;
    let mut offset = 0;
    loop {
        if bytes.len() - offset < 2 { return Err(malformed()); }
        if bytes[offset..].starts_with(&[0, 0]) {
            let index = current.ok_or_else(malformed)?;
            let Section::Open { parent, content_start, previous_change } = sections[index] else { return Err(malformed()); };
            let length = (offset - content_start).checked_add_signed(change - previous_change)
                .filter(|length| *length <= i32::MAX as usize).ok_or_else(malformed)?;
            sections[index] = Section::Closed(length);
            change = change.checked_add(length_octets(length) as isize - 3).ok_or_else(malformed)?;
            current = parent;
            offset += 2;
            if current.is_none() { break; }
        } else {
            offset += 1;
            if bytes[offset] == 0x80 {
                offset += 1;
                sections.push(Section::Open { parent: current, content_start: offset, previous_change: change });
                current = Some(sections.len() - 1);
            } else {
                let length = definite_length(bytes, &mut offset)?;
                offset = offset.checked_add(length).filter(|offset| *offset <= bytes.len()).ok_or_else(malformed)?;
            }
        }
    }
    let consumed = offset;
    let size = consumed.checked_add_signed(change).ok_or_else(malformed)?;
    let mut converted = Vec::with_capacity(size);
    let mut section = 0;
    offset = 0;
    while offset < consumed {
        if bytes[offset..].starts_with(&[0, 0]) { offset += 2; continue; }
        converted.push(bytes[offset]); offset += 1;
        if bytes[offset] == 0x80 {
            let Section::Closed(length) = sections[section] else { return Err(malformed()); };
            section += 1; offset += 1;
            write_length(&mut converted, length);
        } else {
            let length = definite_length(bytes, &mut offset)?;
            write_length(&mut converted, length);
            converted.extend_from_slice(&bytes[offset..offset + length]); offset += length;
        }
    }
    converted.resize(size, 0);
    Ok((converted, consumed))
}

fn length_octets(length: usize) -> usize { if length < 128 { 1 } else { 1 + (usize::BITS - length.leading_zeros()).div_ceil(8) as usize } }
fn write_length(bytes: &mut Vec<u8>, length: usize) {
    let count = length_octets(length);
    if count == 1 { bytes.push(length as u8); }
    else {
        bytes.push(0x80 | (count - 1) as u8);
        bytes.extend_from_slice(&length.to_be_bytes()[std::mem::size_of::<usize>() - (count - 1)..]);
    }
}

fn malformed() -> Anomaly { invalid("Malformed certificate identifier") }
fn duplicate() -> Anomaly { invalid("Certificate contains duplicate parsed identifier extensions") }

#[cfg(test)]
mod tests;
