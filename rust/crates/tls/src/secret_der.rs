use std::{borrow::Cow, ops::Deref};
use rtdb_types::{Anomaly, Result};
use zeroize::Zeroizing;
use crate::java_der::{Der, encoded, valid_oid};

pub(crate) enum SecretBytes<'a> { Borrowed(&'a [u8]), Owned(Zeroizing<Vec<u8>>) }
pub(crate) struct Value<'a> { pub(crate) tag: u8, pub(crate) content: SecretBytes<'a> }
pub(crate) struct Algorithm { pub(crate) oid: Zeroizing<Vec<u8>>, pub(crate) encoded: Zeroizing<Vec<u8>> }

impl Deref for SecretBytes<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] { match self { Self::Borrowed(bytes) => bytes, Self::Owned(bytes) => bytes } }
}
impl SecretBytes<'_> {
    pub(crate) fn into_owned(self) -> Zeroizing<Vec<u8>> { match self { Self::Borrowed(bytes) => Zeroizing::new(bytes.to_vec()), Self::Owned(bytes) => bytes } }
}

pub(crate) fn next<'a>(input: &mut Der<'a>) -> Result<Value<'a>> {
    let (tag, content) = input.next()?;
    let content = match content { Cow::Borrowed(bytes) => SecretBytes::Borrowed(bytes), Cow::Owned(bytes) => SecretBytes::Owned(Zeroizing::new(bytes)) };
    Ok(Value { tag, content })
}

pub(crate) fn expect<'a>(input: &mut Der<'a>, tag: u8, malformed: fn() -> Anomaly) -> Result<SecretBytes<'a>> {
    let value = next(input)?;
    if value.tag != tag { return Err(malformed()); }
    Ok(value.content)
}

pub(crate) fn octets(value: Value<'_>, malformed: fn() -> Anomaly) -> Result<Zeroizing<Vec<u8>>> {
    let capacity = value.content.len();
    let mut pending = vec![(value.tag, value.content.into_owned())];
    let mut output = Zeroizing::new(Vec::with_capacity(capacity));
    while let Some((tag, content)) = pending.pop() {
        if tag == 4 { output.extend_from_slice(&content); }
        else if tag & 0x3f == 0x24 {
            let start = pending.len();
            let mut parts = Der(&content);
            while !parts.0.is_empty() { let value = next(&mut parts)?; pending.push((value.tag, value.content.into_owned())); }
            pending[start..].reverse();
        } else { return Err(malformed()); }
    }
    Ok(output)
}

pub(crate) fn encode(tag: u8, content: &[u8]) -> Zeroizing<Vec<u8>> { Zeroizing::new(encoded(tag, content)) }

pub(crate) fn concatenate(parts: &[Zeroizing<Vec<u8>>], malformed: fn() -> Anomaly) -> Result<Zeroizing<Vec<u8>>> {
    let capacity = parts.iter().try_fold(0usize, |bytes, part| bytes.checked_add(part.len()).ok_or_else(malformed))?;
    let mut output = Zeroizing::new(Vec::with_capacity(capacity));
    for part in parts { output.extend_from_slice(part); }
    Ok(output)
}

pub(crate) fn algorithm(input: &mut Der<'_>, malformed: fn() -> Anomaly) -> Result<Algorithm> {
    let content = expect(input, 0x30, malformed)?;
    let mut fields = Der(&content);
    let oid = expect(&mut fields, 6, malformed)?.into_owned();
    valid_oid(&oid)?;
    let parameter = if fields.0.is_empty() { None } else {
        let value = next(&mut fields)?;
        if value.tag == 5 {
            if !value.content.is_empty() { return Err(malformed()); }
            None
        } else { Some(encode(value.tag, &value.content)) }
    };
    fields.end()?;
    let parameter = parameter.unwrap_or_else(|| if emits_null(&oid) { encode(5, &[]) } else { Zeroizing::new(Vec::new()) });
    let content = concatenate(&[encode(6, &oid), parameter], malformed)?;
    Ok(Algorithm { oid, encoded: encode(0x30, &content) })
}

// OpenJDK 25.0.1 AlgorithmId.encode emits NULL only for its pinned RSA, digest and signature OIDs.
fn emits_null(oid: &[u8]) -> bool {
    matches!(oid,
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 1 | 2 | 4 | 5 | 11..=16]
        | [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 2, 2 | 5]
        | [0x2b, 0x0e, 3, 2, 0x1a]
        | [0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, 1..=10]
        | [0x60, 0x86, 0x48, 1, 0x65, 3, 4, 3, 13..=16])
}
