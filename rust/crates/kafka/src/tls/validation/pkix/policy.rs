use std::{borrow::Cow, collections::{BTreeMap, BTreeSet}, ptr, sync::Arc};
use foreign_types::ForeignTypeRef;
use openssl::{nid::Nid, x509::X509Ref};
use rtdb_columnar::MemoryReservation;
use rtdb_types::Result;
use crate::tls::{TlsConfigError, TlsConfigResult};
use super::invalid;

const ANY_POLICY: &[u8] = &[0x55, 0x1d, 0x20, 0];
type Oid = Arc<[u8]>;
type Frontier = BTreeMap<Oid, Expected>;
struct Expected { policies: BTreeSet<Oid>, original: bool }
impl Expected {
    fn new(policy: Oid) -> Self { Self { policies: BTreeSet::from([policy]), original: true } }
    fn mapped(&mut self, policy: Oid) {
        if self.original { self.policies.clear(); self.original = false; }
        self.policies.insert(policy);
    }
}
struct Policy { oid: Oid, qualified: bool }
struct Constraints { require: i32, inhibit: i32 }

// OpenJDK 25.0.1 PolicyChecker, with the default initial anyPolicy set; only the current depth affects policy-tree survival.
pub(super) fn verify(leaf_first_with_anchor: &[&X509Ref], charge: &Arc<MemoryReservation>) -> Result<()> {
    verify_typed(leaf_first_with_anchor, charge).map_err(TlsConfigError::into_anomaly)
}

pub(super) fn verify_typed(leaf_first_with_anchor: &[&X509Ref], charge: &Arc<MemoryReservation>) -> TlsConfigResult<()> {
    let Some((_, nonanchors)) = leaf_first_with_anchor.split_last() else { return Ok(()); };
    if nonanchors.is_empty() { return Ok(()); }
    let bound = nonanchors.iter().try_fold(65_536usize, |bytes, certificate| {
        let length = unsafe { openssl_sys::i2d_X509(certificate.as_ptr(), ptr::null_mut()) };
        usize::try_from(length).ok().and_then(|length| length.checked_mul(512)).and_then(|length| bytes.checked_add(length))
            .ok_or_else(|| TlsConfigError::invalid("TLS certificate policy state exceeds allocation range"))
    })?;
    let _reservation = charge.budget().reserve(bound).map_err(TlsConfigError::Other)?;
    let mut frontier = Frontier::from([(ANY_POLICY.into(), Expected::new(ANY_POLICY.into()))]);
    let mut mapping = i64::try_from(leaf_first_with_anchor.len()).map_err(|_| TlsConfigError::invalid("TLS certificate path exceeds policy counter range"))?;
    let mut any = mapping;
    let mut explicit = mapping;
    for (index, certificate) in nonanchors.iter().enumerate().rev() {
        let policies = parsed(certificate, Nid::CERTIFICATE_POLICIES, policies).map_err(TlsConfigError::from_invalid_anomaly)?;
        let mappings = parsed(certificate, Nid::POLICY_MAPPINGS, mappings).map_err(TlsConfigError::from_invalid_anomaly)?;
        let constraints = parsed(certificate, Nid::POLICY_CONSTRAINTS, constraints).map_err(TlsConfigError::from_invalid_anomaly)?.map(|(_, value)| value)
            .unwrap_or(Constraints { require: -1, inhibit: -1 });
        let inhibit_any = parsed(certificate, Nid::INHIBIT_ANY_POLICY, inhibit_any).map_err(TlsConfigError::from_invalid_anomaly)?.map(|(_, value)| value);
        let final_certificate = index == 0;
        let self_issued = self_issued(certificate, charge)?;
        if !frontier.is_empty() {
            if let Some((critical, policies)) = policies {
                frontier = advance(&frontier, &policies, critical, any > 0 || (!final_certificate && self_issued)).map_err(TlsConfigError::from_invalid_anomaly)?;
            } else { frontier.clear(); }
            if !frontier.is_empty() && !final_certificate {
                if let Some((_, mappings)) = mappings { apply_mappings(&mut frontier, &mappings, mapping).map_err(TlsConfigError::from_invalid_anomaly)?; }
            }
        }
        if final_certificate {
            if explicit > 0 && !self_issued { explicit -= 1; }
            if constraints.require == 0 { explicit = 0; }
        }
        if explicit == 0 && frontier.is_empty() { return Err(TlsConfigError::invalid("TLS certificate path requires a nonempty policy tree")); }
        if !final_certificate {
            if !self_issued {
                if explicit > 0 { explicit -= 1; }
                if mapping > 0 { mapping -= 1; }
                if any > 0 { any -= 1; }
            }
            merge_constraint(&mut explicit, constraints.require);
            merge_constraint(&mut mapping, constraints.inhibit);
            if let Some(inhibit) = inhibit_any { any = any.min(i64::from(inhibit)); }
        }
    }
    Ok(())
}

fn merge_constraint(counter: &mut i64, constraint: i32) {
    if constraint != -1 && (*counter == -1 || i64::from(constraint) < *counter) { *counter = i64::from(constraint); }
}

fn advance(previous: &Frontier, policies: &[Policy], critical: bool, allow_any: bool) -> Result<Frontier> {
    let mut next = Frontier::new();
    let mut any = false;
    for policy in policies {
        if policy.oid.as_ref() == ANY_POLICY { any = true; continue; }
        if critical && policy.qualified { return Err(invalid("TLS certificate contains critical policy qualifiers")); }
        if previous.values().any(|expected| expected.policies.contains(policy.oid.as_ref()) || expected.policies.contains(ANY_POLICY)) {
            next.entry(Arc::clone(&policy.oid)).or_insert_with(|| Expected::new(Arc::clone(&policy.oid)));
        }
    }
    if any && allow_any {
        for expected in previous.values().flat_map(|expected| &expected.policies) {
            next.entry(Arc::clone(expected)).or_insert_with(|| Expected::new(Arc::clone(expected)));
        }
    }
    Ok(next)
}

fn apply_mappings(frontier: &mut Frontier, mappings: &[(Oid, Oid)], permitted: i64) -> Result<()> {
    for (issuer, subject) in mappings {
        if issuer.as_ref() == ANY_POLICY || subject.as_ref() == ANY_POLICY { return Err(invalid("TLS certificate policy mapping uses anyPolicy")); }
        if permitted > 0 || permitted == -1 {
            if let Some(expected) = frontier.get_mut(issuer.as_ref()) { expected.mapped(Arc::clone(subject)); }
            else if frontier.contains_key(ANY_POLICY) {
                let mut expected = Expected::new(Arc::clone(subject)); expected.original = false;
                frontier.insert(Arc::clone(issuer), expected);
            }
        } else if permitted == 0 { frontier.remove(issuer.as_ref()); }
    }
    Ok(())
}

fn self_issued(certificate: &X509Ref, charge: &MemoryReservation) -> TlsConfigResult<bool> {
    let subject = certificate.subject_name().to_der().map_err(TlsConfigError::invalid)?;
    let issuer = certificate.issuer_name().to_der().map_err(TlsConfigError::invalid)?;
    let decode = |encoded: &[u8]| rtdb_tls::DistinguishedName::decode(encoded, charge.budget()).map_err(TlsConfigError::from_tls);
    let subject = decode(&subject)?;
    let issuer = decode(&issuer)?;
    Ok(subject == issuer)
}

fn parsed<T>(certificate: &X509Ref, nid: Nid, parse: impl Fn(&[u8], bool) -> Result<T>) -> Result<Option<(bool, T)>> {
    let mut index = -1;
    let mut result = None;
    loop {
        index = unsafe { openssl_sys::X509_get_ext_by_NID(certificate.as_ptr(), nid.as_raw(), index) };
        if index < 0 { return Ok(result); }
        let (critical, bytes) = extension(certificate, index)?;
        match parse(bytes, critical) {
            Ok(value) => {
                if result.is_some() { return Err(invalid("TLS certificate contains duplicate policy extensions")); }
                result = Some((critical, value));
            }
            Err(error) if critical => return Err(error),
            Err(_) => {}
        }
    }
}

fn extension(certificate: &X509Ref, index: i32) -> Result<(bool, &[u8])> {
    let extension = unsafe { openssl_sys::X509_get_ext(certificate.as_ptr(), index) };
    if extension.is_null() { return Err(malformed()); }
    let critical = unsafe { openssl_sys::X509_EXTENSION_get_critical(extension) } != 0;
    let data = unsafe { openssl_sys::X509_EXTENSION_get_data(extension) };
    if data.is_null() { return Err(malformed()); }
    let length = usize::try_from(unsafe { openssl_sys::ASN1_STRING_length(data.cast()) }).map_err(|_| malformed())?;
    let bytes = unsafe { openssl_sys::ASN1_STRING_get0_data(data.cast()) };
    let bytes = if length == 0 { &[] } else {
        if bytes.is_null() { return Err(malformed()); }
        unsafe { std::slice::from_raw_parts(bytes, length) }
    };
    Ok((critical, bytes))
}

fn policies(bytes: &[u8], _: bool) -> Result<Vec<Policy>> {
    let sequence = sequence(bytes)?;
    let mut entries = Der(&sequence);
    let mut policies = Vec::new();
    while !entries.0.is_empty() {
        let value = entries.expect(0x30)?;
        let mut policy = Der(&value);
        let oid = policy.oid()?;
        let qualified = !policy.0.is_empty();
        if qualified {
            let value = policy.expect(0x30)?;
            let mut qualifiers = Der(&value);
            if qualifiers.0.is_empty() { return Err(malformed()); }
            while !qualifiers.0.is_empty() { Der(&qualifiers.expect(0x30)?).oid()?; }
        }
        policies.push(Policy { oid, qualified });
    }
    Ok(policies)
}

fn mappings(bytes: &[u8], _: bool) -> Result<Vec<(Oid, Oid)>> {
    let sequence = sequence(bytes)?;
    let mut entries = Der(&sequence);
    let mut mappings = Vec::new();
    while !entries.0.is_empty() {
        let value = entries.expect(0x30)?;
        let mut mapping = Der(&value);
        mappings.push((mapping.oid()?, mapping.oid()?));
    }
    Ok(mappings)
}

fn constraints(bytes: &[u8], _: bool) -> Result<Constraints> {
    let sequence = sequence(bytes)?;
    let mut entries = Der(&sequence);
    let (mut require, mut inhibit) = (-1, -1);
    while !entries.0.is_empty() {
        let (tag, bytes) = entries.next()?;
        let target = match tag { 0x80 => &mut require, 0x81 => &mut inhibit, _ => return Err(malformed()) };
        if *target != -1 { return Err(malformed()); }
        *target = integer(&bytes)?;
    }
    Ok(Constraints { require, inhibit })
}

fn inhibit_any(bytes: &[u8], critical: bool) -> Result<i32> {
    if !critical { return Err(malformed()); }
    let mut encoded = Der(bytes);
    let count = integer(&encoded.expect(2)?)?;
    encoded.end()?;
    match count { -1 => Ok(i32::MAX), 0.. => Ok(count), _ => Err(malformed()) }
}

fn integer(mut bytes: &[u8]) -> Result<i32> {
    if bytes.is_empty() { return Err(malformed()); }
    while bytes.len() > 1 && ((bytes[0] == 0 && bytes[1] & 0x80 == 0) || (bytes[0] == 0xff && bytes[1] & 0x80 != 0)) { bytes = &bytes[1..]; }
    if bytes.len() > 4 { return Err(malformed()); }
    Ok(bytes.iter().fold(if bytes[0] & 0x80 == 0 { 0i32 } else { -1i32 }, |value, byte| (value << 8) | i32::from(*byte)))
}

fn sequence(bytes: &[u8]) -> Result<Cow<'_, [u8]>> {
    let mut encoded = Der(bytes);
    let sequence = encoded.expect(0x30)?;
    encoded.end()?;
    Ok(sequence)
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
    fn oid(&mut self) -> Result<Oid> {
        let bytes = self.expect(6)?;
        if bytes.is_empty() || bytes.len() > 4096 || bytes.last().is_some_and(|byte| byte & 0x80 != 0)
            || bytes.iter().enumerate().any(|(index, byte)| *byte == 0x80 && (index == 0 || bytes[index - 1] & 0x80 == 0)) { return Err(malformed()); }
        Ok(Arc::from(bytes.as_ref()))
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

fn malformed() -> rtdb_types::Anomaly { invalid("TLS certificate policy extension is malformed") }

#[cfg(test)]
mod tests;
