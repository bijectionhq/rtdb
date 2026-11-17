use std::borrow::Cow;
use rtdb_types::{Anomaly, Result};
use crate::invalid;

pub(crate) fn valid_oid(bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() || bytes.len() > 4096 || bytes.last().is_some_and(|byte| byte & 0x80 != 0)
        || bytes.iter().enumerate().any(|(index, byte)| *byte == 0x80 && (index == 0 || bytes[index - 1] & 0x80 == 0)) { return Err(malformed()); }
    Ok(())
}

pub(crate) fn octets(value: (u8, Cow<'_, [u8]>)) -> Result<Vec<u8>> {
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

pub(crate) fn single(bytes: &[u8], tag: u8) -> Result<Cow<'_, [u8]>> {
    let mut encoded = Der(bytes);
    let content = encoded.expect(tag)?;
    encoded.end()?;
    Ok(content)
}

pub(crate) fn encoded(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(content.len() + 6);
    encoded.push(tag); write_length(&mut encoded, content.len()); encoded.extend_from_slice(content); encoded
}

pub(crate) struct Der<'a>(pub(crate) &'a [u8]);
impl<'a> Der<'a> {
    pub(crate) fn next(&mut self) -> Result<(u8, Cow<'a, [u8]>)> {
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
    pub(crate) fn expect(&mut self, expected: u8) -> Result<Cow<'a, [u8]>> {
        let (tag, content) = self.next()?;
        if tag == expected { Ok(content) } else { Err(malformed()) }
    }
    pub(crate) fn end(self) -> Result<()> { if self.0.is_empty() { Ok(()) } else { Err(malformed()) } }
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

pub(crate) fn malformed() -> Anomaly { invalid("Malformed certificate identifier") }

// OpenJDK 25.0.1 DerValue.checkPaddedBits and BitArray(int, byte[], int) use signed wrapping arithmetic and the backing array's remaining length.
pub(crate) fn bit_layout(padding: u8, bytes: usize, backing: usize) -> Result<(usize, u8)> {
    if padding > 7 { return Err(malformed()); }
    let bits = (bytes as i32).wrapping_mul(8).wrapping_sub(i32::from(padding));
    if bits < 0 || (backing as i32).wrapping_mul(8) < bits { return Err(malformed()); }
    let length = usize::try_from(bits.wrapping_add(7) / 8).map_err(|_| malformed())?;
    if length > bytes { return Err(malformed()); }
    let padding = ((length as i32).wrapping_mul(8) - bits) as u8;
    Ok((length, padding))
}
