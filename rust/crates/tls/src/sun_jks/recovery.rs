use std::{borrow::Cow, ops::Deref};
use sha1::{Digest, Sha1};
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;
use rtdb_types::Result;
use crate::{invalid, java_der::{Der, bit_layout, encoded, valid_oid}};

enum SecretBytes<'a> { Borrowed(&'a [u8]), Owned(Zeroizing<Vec<u8>>) }
struct Value<'a> { tag: u8, content: SecretBytes<'a> }
struct Algorithm { oid: Zeroizing<Vec<u8>>, encoded: Zeroizing<Vec<u8>> }

impl Deref for SecretBytes<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] { match self { Self::Borrowed(bytes) => bytes, Self::Owned(bytes) => bytes } }
}
impl SecretBytes<'_> {
    fn into_owned(self) -> Zeroizing<Vec<u8>> { match self { Self::Borrowed(bytes) => Zeroizing::new(bytes.to_vec()), Self::Owned(bytes) => bytes } }
}

fn next<'a>(input: &mut Der<'a>) -> Result<Value<'a>> {
    let (tag, content) = input.next()?;
    let content = match content { Cow::Borrowed(bytes) => SecretBytes::Borrowed(bytes), Cow::Owned(bytes) => SecretBytes::Owned(Zeroizing::new(bytes)) };
    Ok(Value { tag, content })
}

fn expect<'a>(input: &mut Der<'a>, tag: u8) -> Result<SecretBytes<'a>> {
    let value = next(input)?;
    if value.tag != tag { return Err(malformed()); }
    Ok(value.content)
}

fn octets(value: Value<'_>) -> Result<Zeroizing<Vec<u8>>> {
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

fn encode(tag: u8, content: &[u8]) -> Zeroizing<Vec<u8>> { Zeroizing::new(encoded(tag, content)) }

fn concatenate(parts: &[Zeroizing<Vec<u8>>]) -> Result<Zeroizing<Vec<u8>>> {
    let capacity = parts.iter().try_fold(0usize, |bytes, part| bytes.checked_add(part.len()).ok_or_else(malformed))?;
    let mut output = Zeroizing::new(Vec::with_capacity(capacity));
    for part in parts { output.extend_from_slice(part); }
    Ok(output)
}

fn algorithm(input: &mut Der<'_>) -> Result<Algorithm> {
    let content = expect(input, 0x30)?;
    let mut fields = Der(&content);
    let oid = expect(&mut fields, 6)?.into_owned();
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
    let content = concatenate(&[encode(6, &oid), parameter])?;
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

pub(super) fn recover(protected: &[u8], password: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    if protected.len() > i32::MAX as usize { return Err(malformed()); }
    let mut input = Der(protected);
    let outer = next(&mut input)?;
    input.end()?;
    let mut fields = Der(&outer.content);
    let algorithm = algorithm(&mut fields)?;
    let encrypted = octets(next(&mut fields)?)?;
    fields.end()?;
    if algorithm.oid.as_slice() != [0x2b, 6, 1, 4, 1, 0x2a, 2, 0x11, 1, 1] { return Err(invalid("Unsupported JKS private key protection algorithm")); }
    if encrypted.len() < 40 { return Err(malformed()); }
    let (salt, rest) = encrypted.split_at(20);
    let (encrypted, expected) = rest.split_at(rest.len() - 20);
    let mut previous = Zeroizing::new(<[u8; 20]>::try_from(salt).map_err(invalid)?);
    let mut plaintext = Zeroizing::new(Vec::with_capacity(encrypted.len()));
    for block in encrypted.chunks(20) {
        let mut hash = Sha1::new(); hash.update(password); hash.update(previous.as_slice());
        *previous = hash.finalize().into();
        plaintext.extend(block.iter().zip(previous.iter()).map(|(byte, mask)| byte ^ mask));
    }
    let mut verifier = Sha1::new(); verifier.update(password); verifier.update(plaintext.as_slice());
    if !bool::from(verifier.finalize().as_slice().ct_eq(expected)) { return Err(invalid("JKS private key password is incorrect")); }
    normalize(&plaintext)
}

fn normalize(bytes: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    let mut input = Der(bytes);
    let outer = expect(&mut input, 0x30)?;
    input.end()?;
    let mut fields = Der(&outer);
    let version = expect(&mut fields, 2)?;
    let mut version: &[u8] = &version;
    while version.len() > 1 && version[0] == 0 { version = &version[1..]; }
    let version = match version { [0] => 0, [1] => 1, _ => return Err(malformed()) };
    let algorithm = algorithm(&mut fields)?;
    let private_key = expect(&mut fields, 4)?;
    let mut optional = if fields.0.is_empty() { None } else { Some(next(&mut fields)?) };
    let attributes = if optional.as_ref().is_some_and(|value| value.tag & 0xdf == 0x80) {
        let attributes = optional.take().expect("present PKCS8 attributes").content.into_owned();
        optional = if fields.0.is_empty() { None } else { Some(next(&mut fields)?) };
        Some(attributes)
    } else { None };
    let public_key = if let Some(value) = optional {
        if version != 1 || value.tag & 0xdf != 0x81 { return Err(malformed()); }
        let (&padding, bytes) = value.content.split_first().ok_or_else(malformed)?;
        let (length, padding) = bit_layout(padding, bytes.len(), bytes.len() + fields.0.len())?;
        let mut bits = Zeroizing::new(Vec::with_capacity(length + 1));
        bits.push(padding); bits.extend_from_slice(&bytes[..length]);
        if padding != 0 { *bits.last_mut().expect("nonempty padded public key") &= 0xff << padding; }
        Some(bits)
    } else { None };
    fields.end()?;
    let mut parts = vec![encode(2, &[version]), algorithm.encoded, encode(4, &private_key)];
    if version == 1 {
        if let Some(attributes) = attributes { parts.push(encode(0xa0, &attributes)); }
        if let Some(public_key) = public_key { parts.push(encode(0x81, &public_key)); }
    }
    Ok(encode(0x30, &concatenate(&parts)?))
}

fn malformed() -> rtdb_types::Anomaly { invalid("Malformed SunX509 JKS private key") }
