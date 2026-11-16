use rtdb_types::Result;
use zeroize::Zeroizing;
use crate::invalid;
#[derive(Clone, Copy)]
pub(super) struct Value<'a> { pub tag: u8, pub content: &'a [u8], pub encoded: &'a [u8] }
pub(super) struct Reader<'a>(pub &'a [u8]);
impl<'a> Reader<'a> {
    pub fn next(&mut self) -> Result<Value<'a>> { let (value, rest) = parse(self.0, 0)?; self.0 = rest; Ok(value) }
    pub fn next_and_end(&mut self) -> Result<Value<'a>> { let value = self.next()?; self.end()?; Ok(value) }
    pub fn expect(&mut self, tag: u8) -> Result<Value<'a>> { let value = self.next()?; if value.tag != tag { return Err(invalid("Unexpected PKCS12 BER tag")); } Ok(value) }
    pub fn integer(&mut self) -> Result<usize> {
        let value = self.expect(2)?.content;
        if value.is_empty() || value[0] & 0x80 != 0 { return Err(invalid("Invalid nonnegative PKCS12 integer")); }
        value.iter().try_fold(0usize, |n, byte| n.checked_mul(256).and_then(|n| n.checked_add(usize::from(*byte))).ok_or_else(|| invalid("PKCS12 integer exceeds range")))
    }
    pub fn is_empty(&self) -> bool { self.0.is_empty() }
    pub fn peek(&self) -> Option<u8> { self.0.first().copied() }
    pub fn end(&self) -> Result<()> { if self.is_empty() { Ok(()) } else { Err(invalid("Trailing PKCS12 BER content")) } }
    pub fn optional_null(&mut self) -> Result<()> { if !self.is_empty() && !self.expect(5)?.content.is_empty() { return Err(invalid("Invalid PKCS12 algorithm parameters")); } self.end() }
}
impl<'a> Value<'a> {
    pub fn reader(self) -> Reader<'a> { Reader(self.content) }
    pub fn sequence(self) -> Result<Reader<'a>> { if self.tag != 0x30 { return Err(invalid("Expected PKCS12 sequence")); } Ok(self.reader()) }
    pub fn octets(self, implicit: bool) -> Result<Zeroizing<Vec<u8>>> {
        let mut bytes = Zeroizing::new(Vec::with_capacity(self.content.len())); self.append_octets(implicit, &mut bytes, 0)?; Ok(bytes)
    }
    fn append_octets(self, implicit: bool, output: &mut Vec<u8>, depth: usize) -> Result<()> {
        if depth > 64 { return Err(invalid("PKCS12 octet nesting exceeds limit")); }
        let primitive = if implicit { 0x80 } else { 4 };
        if self.tag == primitive { output.extend_from_slice(self.content); }
        else if self.tag == primitive | 0x20 { let mut reader = self.reader(); while !reader.is_empty() { reader.next()?.append_octets(false, output, depth + 1)?; } }
        else { return Err(invalid("Expected PKCS12 octet string")); }
        Ok(())
    }
}
fn parse(bytes: &[u8], depth: usize) -> Result<(Value<'_>, &[u8])> {
    if depth > 64 || bytes.len() < 2 { return Err(invalid("Truncated or deeply nested PKCS12 BER")); }
    let tag = bytes[0]; if tag == 0 || tag & 31 == 31 { return Err(invalid("Unsupported PKCS12 BER tag")); }
    let first = bytes[1]; let mut header = 2usize;
    let (length, suffix) = if first == 0x80 {
        if tag & 0x20 == 0 { return Err(invalid("Primitive PKCS12 BER has indefinite length")); }
        let mut rest = &bytes[header..];
        while !rest.starts_with(&[0,0]) { let (_, tail) = parse(rest, depth + 1)?; rest = tail; }
        (bytes.len() - rest.len() - header, 2)
    } else if first & 0x80 == 0 { (usize::from(first), 0) }
    else {
        let count = usize::from(first & 0x7f);
        if count > std::mem::size_of::<usize>() || bytes.len() < header + count { return Err(invalid("PKCS12 BER length exceeds range")); }
        let length = bytes[header..header + count].iter().try_fold(0usize, |n, byte| n.checked_mul(256).and_then(|n| n.checked_add(usize::from(*byte))).ok_or_else(|| invalid("PKCS12 BER length exceeds range")))?;
        header += count; (length, 0)
    };
    let end = header.checked_add(length).ok_or_else(|| invalid("PKCS12 BER length exceeds range"))?;
    let total = end.checked_add(suffix).ok_or_else(|| invalid("PKCS12 BER length exceeds range"))?;
    if total > bytes.len() { return Err(invalid("Truncated PKCS12 BER value")); }
    Ok((Value { tag, content: &bytes[header..end], encoded: &bytes[..total] }, &bytes[total..]))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn constructed_octets_and_indefinite_lengths_keep_exact_content_boundaries() {
        let bytes = [0x24,0x80,4,2,1,2,0x24,3,4,1,3,0,0];
        assert_eq!(&*Reader(&bytes).next_and_end().unwrap().octets(false).unwrap(), &[1,2,3]);
        for bytes in [&[4,0x80][..], &[0x30,0x80,4,0][..], &[4,0x88,0xff,0xff,0xff,0xff,0xff,0xff,0xff,0xff][..]] { assert!(Reader(bytes).next().is_err()); }
    }
}
