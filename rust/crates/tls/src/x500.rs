use std::{borrow::Cow, fmt::Write, sync::Arc};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::Result;
use unicode_normalization::UnicodeNormalization;
use crate::{TlsError, TlsResult, invalid};

mod case;
pub(crate) use case::{equals_ignore_case_utf16, lowercase_utf16};

/// Immutable Java X500Principal canonical name decoded from an ASN.1 name.
/// Its allocation reservation covers parsing scratch and remains owned until this value drops.
pub struct DistinguishedName { canonical: String, rdns: Vec<CanonicalRdn>, _reservation: Arc<MemoryReservation> }
struct CanonicalRdn { range: std::ops::Range<usize>, assertions: usize, geographical: bool }

impl DistinguishedName {
    /// Decodes the name under an independent reservation; the borrowed input has its own owner.
    /// Attribute value types, RDN order and repeated assertions remain significant.
    /// Accepts the outer sequence or unwrapped RDN sets, including Java's BER length forms; bytes after the outer sequence are ignored.
    /// Invalid framing, OIDs or decoded BMP supplementary characters return a TLS-material error; insufficient memory returns the budget error before parsing.
    pub fn decode(encoded: &[u8], memory: &Arc<MemoryBudget>) -> TlsResult<Self> {
        let bound = encoded.len().checked_mul(512).and_then(|length| length.checked_add(65_536))
            .ok_or_else(|| TlsError::invalid_keystore("Distinguished name exceeds allocation range"))?;
        let reservation = memory.reserve(bound).map_err(TlsError::Other)?;
        let (canonical, rdns) = decode_canonical(encoded).map_err(TlsError::from_invalid_anomaly)?;
        Ok(Self { canonical, rdns, _reservation: reservation })
    }

    /// Borrows the RFC2253 canonical spelling used for Java principal equality and hashing.
    pub fn canonical(&self) -> &str { &self.canonical }

    /// Returns the issuer's RDN distance from its closest common ancestor with this name.
    /// Returns `None` when there is no common RDN or the last common RDN contains country, state, locality or street-address attributes.
    /// Uses OpenJDK 25.0.1 ForwardBuilder's structural RDN comparison; canonical-string delimiters do not define RDN boundaries.
    pub fn distance_to_common_non_geographical_ancestor(&self, issuer: &Self) -> Option<usize> {
        let common = self.rdns.iter().zip(&issuer.rdns).take_while(|(left, right)| {
            left.assertions == right.assertions && self.canonical[left.range.clone()] == issuer.canonical[right.range.clone()]
        }).count();
        if common == 0 || self.rdns[common - 1].geographical { None }
        else { Some(issuer.rdns.len() - common) }
    }
}

impl PartialEq for DistinguishedName { fn eq(&self, other: &Self) -> bool { self.canonical == other.canonical } }
impl Eq for DistinguishedName {}
impl std::fmt::Debug for DistinguishedName {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { formatter.debug_tuple("DistinguishedName").field(&self.canonical).finish() }
}

fn decode_canonical(encoded: &[u8]) -> Result<(String, Vec<CanonicalRdn>)> {
    let mut outer = Der(encoded);
    let content = if encoded.first() == Some(&0x30) { outer.expect(0x30)?.content } else { Cow::Borrowed(encoded) };
    let mut rdns = Der(&content);
    let mut canonical = Vec::new();
    while !rdns.0.is_empty() {
        let rdn = rdns.expect(0x31)?;
        let mut assertions = Der(&rdn.content);
        let mut values = Vec::new();
        let mut geographical = false;
        while !assertions.0.is_empty() {
            let sequence = assertions.expect(0x30)?;
            let mut assertion = Der(&sequence.content);
            let oid = assertion.expect(6)?.content;
            geographical |= matches!(oid.as_ref(), [0x55, 4, 6 | 7 | 8 | 9]);
            let value = assertion.next()?;
            assertion.end()?;
            values.push(canonical_assertion(&oid, value)?);
        }
        values.sort_by(|left, right| right.0.cmp(&left.0).then_with(|| left.1.encode_utf16().cmp(right.1.encode_utf16())));
        let count = values.len();
        canonical.push((values.into_iter().map(|(_, value)| value).collect::<Vec<_>>().join("+"), count, geographical));
    }
    let mut text = String::new();
    let mut rdns = Vec::with_capacity(canonical.len());
    for (value, assertions, geographical) in canonical.into_iter().rev() {
        if !rdns.is_empty() { text.push(','); }
        let start = text.len();
        text.push_str(&value);
        rdns.push(CanonicalRdn { range: start..text.len(), assertions, geographical });
    }
    rdns.reverse();
    Ok((text, rdns))
}

fn canonical_assertion(oid: &[u8], value: Value<'_>) -> Result<(bool, String)> {
    let keyword = match oid {
        [0x55, 4, 3] => Some("CN"), [0x55, 4, 6] => Some("C"), [0x55, 4, 7] => Some("L"),
        [0x55, 4, 8] => Some("ST"), [0x55, 4, 10] => Some("O"), [0x55, 4, 11] => Some("OU"),
        [0x55, 4, 9] => Some("STREET"),
        [9, 0x92, 0x26, 0x89, 0x93, 0xf2, 0x2c, 0x64, 1, 0x19] => Some("DC"),
        [9, 0x92, 0x26, 0x89, 0x93, 0xf2, 0x2c, 0x64, 1, 1] => Some("UID"),
        _ => None,
    };
    let mut text = keyword.map(str::to_owned).map(Ok).unwrap_or_else(|| dotted_oid(oid))?;
    text.push('=');
    if value.tag == 0x1e { validate_bmp(&value.content)?; }
    if keyword.is_some() && matches!(value.tag, 0x0c | 0x13) {
        let decoded = if value.tag == 0x0c { rtdb_types::decode_java_utf8(&value.content) }
            else { value.content.iter().map(|byte| char::from(*byte)).collect() };
        let mut escaped = String::new();
        let mut previous_space = false;
        for (index, character) in decoded.chars().enumerate() {
            if ",+<>;\"\\".contains(character) || index == 0 && character == '#' { escaped.push('\\'); }
            if character == ' ' {
                if !previous_space { escaped.push(character); }
                previous_space = true;
            } else { escaped.push(character); previous_space = false; }
        }
        text.push_str(escaped.trim_matches(|character| character <= '\u{20}'));
    } else {
        text.push('#');
        let length = value.content.len();
        write!(text, "{:02x}", value.tag).expect("writing to a String");
        if length < 128 { write!(text, "{length:02x}").expect("writing to a String"); }
        else {
            let bytes = length.to_be_bytes();
            let first = bytes.iter().position(|byte| *byte != 0).expect("nonzero length");
            write!(text, "{:02x}", 0x80 | (bytes.len() - first)).expect("writing to a String");
            for byte in &bytes[first..] { write!(text, "{byte:02x}").expect("writing to a String"); }
        }
        for byte in value.content.iter() { write!(text, "{byte:02x}").expect("writing to a String"); }
    }
    Ok((keyword.is_some(), case::fold(&text).nfkd().collect()))
}

fn validate_bmp(bytes: &[u8]) -> Result<()> {
    let mut units = bytes.chunks_exact(2).map(|bytes| u16::from_be_bytes([bytes[0], bytes[1]]));
    while let Some(unit) = units.next() {
        if (0xd800..=0xdbff).contains(&unit) && units.next().is_some_and(|next| (0xdc00..=0xdfff).contains(&next)) {
            return Err(invalid("Distinguished-name BMPString decodes to a supplementary character"));
        }
    }
    Ok(())
}

fn dotted_oid(bytes: &[u8]) -> Result<String> {
    if bytes.is_empty() || bytes.len() > 4096 { return Err(invalid("Distinguished-name object identifier exceeds its Java size limit")); }
    let mut components = Vec::new();
    let mut rest = bytes;
    while !rest.is_empty() {
        if rest[0] == 0x80 { return Err(invalid("Distinguished-name object identifier has a redundant group")); }
        let end = rest.iter().position(|byte| byte & 0x80 == 0).ok_or_else(|| invalid("Truncated distinguished-name object identifier"))?;
        let mut decimal = vec![0u8];
        for byte in &rest[..=end] {
            let mut carry = u16::from(byte & 0x7f);
            for digit in &mut decimal {
                carry += u16::from(*digit) * 128;
                *digit = (carry % 10) as u8; carry /= 10;
            }
            while carry != 0 { decimal.push((carry % 10) as u8); carry /= 10; }
        }
        if components.is_empty() {
            let small = (end == 0).then_some(rest[0]);
            let first = match small { Some(0..=39) => 0, Some(40..=79) => 1, _ => 2 };
            components.push(first.to_string());
            let mut subtract = first * 40;
            for digit in &mut decimal {
                let part = subtract % 10; subtract /= 10;
                if *digit < part { *digit += 10 - part; subtract += 1; } else { *digit -= part; }
            }
            while decimal.len() > 1 && decimal.last() == Some(&0) { decimal.pop(); }
        }
        components.push(decimal.into_iter().rev().map(|digit| char::from(b'0' + digit)).collect::<String>());
        rest = &rest[end + 1..];
    }
    Ok(components.join("."))
}

struct Value<'a> { tag: u8, content: Cow<'a, [u8]> }
struct Der<'a>(&'a [u8]);
impl<'a> Der<'a> {
    fn next(&mut self) -> Result<Value<'a>> {
        let (value, rest) = parse(self.0)?; self.0 = rest; Ok(value)
    }
    fn expect(&mut self, tag: u8) -> Result<Value<'a>> {
        let value = self.next()?;
        if value.tag != tag { return Err(invalid("Unexpected distinguished-name ASN.1 tag")); }
        Ok(value)
    }
    fn end(self) -> Result<()> { if self.0.is_empty() { Ok(()) } else { Err(invalid("Trailing distinguished-name assertion content")) } }
}

fn parse(bytes: &[u8]) -> Result<(Value<'_>, &[u8])> {
    if bytes.len() < 2 || bytes[0] & 31 == 31 { return Err(invalid("Malformed distinguished-name ASN.1 framing")); }
    let tag = bytes[0];
    if bytes[1] == 0x80 {
        if tag & 0x20 == 0 { return Err(invalid("Primitive distinguished-name value has indefinite length")); }
        let (mut converted, consumed) = convert_indefinite(bytes)?;
        let mut header = 1;
        let length = definite_length(&converted, &mut header)?;
        converted.copy_within(header..header + length, 0);
        converted.truncate(length);
        return Ok((Value { tag, content: Cow::Owned(converted) }, &bytes[consumed..]));
    }
    let mut header = 1;
    let length = definite_length(bytes, &mut header)?;
    let end = header.checked_add(length).ok_or_else(|| invalid("Distinguished-name length exceeds range"))?;
    if end > bytes.len() { return Err(invalid("Truncated distinguished-name value")); }
    Ok((Value { tag, content: Cow::Borrowed(&bytes[header..end]) }, &bytes[end..]))
}

fn definite_length(bytes: &[u8], offset: &mut usize) -> Result<usize> {
    let first = *bytes.get(*offset).ok_or_else(|| invalid("Truncated distinguished-name length"))?;
    *offset += 1;
    if first < 0x80 { Ok(usize::from(first)) }
    else {
        let count = usize::from(first & 0x7f);
        if count == 0 || count > 4 || bytes.len() - *offset < count { return Err(invalid("Distinguished-name length exceeds Java range")); }
        let length = bytes[*offset..*offset + count].iter().fold(0usize, |length, byte| length * 256 + usize::from(*byte));
        if length > i32::MAX as usize { return Err(invalid("Distinguished-name length exceeds Java range")); }
        *offset += count; Ok(length)
    }
}

#[derive(Clone, Copy)]
enum Section { Open { parent: Option<usize>, content_start: usize, previous_change: isize }, Closed(usize) }

// OpenJDK 25.0.1 DerIndefLenConverter: scanned definite bodies remain opaque, including nested BER.
fn convert_indefinite(bytes: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut sections = Vec::<Section>::new();
    let mut current: Option<usize> = None;
    let mut change = 0isize;
    let mut offset = 0;
    loop {
        if bytes.len() - offset < 2 { return Err(invalid("Unterminated distinguished-name indefinite value")); }
        if bytes[offset..].starts_with(&[0, 0]) {
            let index = current.ok_or_else(|| invalid("Unmatched distinguished-name end-of-contents"))?;
            let Section::Open { parent, content_start, previous_change } = sections[index] else { return Err(invalid("Invalid distinguished-name BER section")); };
            let length = (offset - content_start).checked_add_signed(change - previous_change)
                .filter(|length| *length <= i32::MAX as usize).ok_or_else(|| invalid("Distinguished-name BER length exceeds Java range"))?;
            sections[index] = Section::Closed(length);
            change = change.checked_add(length_octets(length) as isize - 3).ok_or_else(|| invalid("Distinguished-name BER length exceeds range"))?;
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
                offset = offset.checked_add(length).filter(|offset| *offset <= bytes.len()).ok_or_else(|| invalid("Truncated distinguished-name BER value"))?;
            }
        }
    }
    let consumed = offset;
    let size = consumed.checked_add_signed(change).ok_or_else(|| invalid("Distinguished-name BER output exceeds range"))?;
    let mut converted = Vec::with_capacity(size);
    let mut section = 0;
    offset = 0;
    while offset < consumed {
        if bytes[offset..].starts_with(&[0, 0]) { offset += 2; continue; }
        converted.push(bytes[offset]); offset += 1;
        if bytes[offset] == 0x80 {
            let Section::Closed(length) = sections[section] else { return Err(invalid("Unterminated distinguished-name BER section")); };
            section += 1; offset += 1;
            write_length(&mut converted, length);
        } else {
            let length = definite_length(bytes, &mut offset)?;
            write_length(&mut converted, length);
            converted.extend_from_slice(&bytes[offset..offset + length]); offset += length;
        }
    }
    // Java retains zero padding when scanned definite-length headers shrink.
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

#[cfg(test)]
mod tests;
