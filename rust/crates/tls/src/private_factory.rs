use std::sync::Arc;
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::{Anomaly, Result};
use zeroize::Zeroizing;
use crate::{invalid, java_der::{Der, bit_layout}, secret_der::{self, Algorithm, SecretBytes, concatenate, encode, next}};
mod curves;
mod integer;
mod parameters;

pub(crate) struct PrivateKey { encoded: Zeroizing<Vec<u8>>, _reservation: Arc<MemoryReservation> }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Family { Rsa, Pss, Dsa, Dh, Ec, Xdh(usize), EdDsa(usize), Named }

const RSA: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 1];
const PSS: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 10];
const DSA: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x38, 4, 1];
const DH: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 3, 1];
const EC: &[u8] = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 2, 1];

impl PrivateKey {
    pub(crate) fn decode(encoded: &[u8], memory: &Arc<MemoryBudget>) -> Result<Self> {
        let mut reservation = memory.reserve(allocation_bound(encoded.len())?)?;
        let encoded = decode(encoded)?;
        let retained = encoded.capacity().checked_add(std::mem::size_of::<Self>()).and_then(|bytes| bytes.checked_add(128)).ok_or_else(overflow)?;
        Arc::get_mut(&mut reservation).expect("unpublished private key reservation").shrink_to(retained)?;
        Ok(Self { encoded, _reservation: reservation })
    }

    pub(crate) fn encoded_pkcs8(&self) -> &[u8] { &self.encoded }
}

fn allocation_bound(bytes: usize) -> Result<usize> { bytes.checked_mul(384).and_then(|bytes| bytes.checked_add(65_536)).ok_or_else(overflow) }

fn family(oid: &[u8]) -> Result<Family> {
    Ok(match oid {
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1] | [0x55, 8, 1, 1] => Family::Rsa,
        RSA => Family::Rsa,
        PSS => Family::Pss,
        DSA | [0x2b, 0x0e, 3, 2, 12] => Family::Dsa,
        DH | [0x2a, 0x86, 0x48, 0xce, 0x3e, 2, 1] => Family::Dh,
        EC => Family::Ec,
        [0x2b, 0x65, 110] => Family::Xdh(32),
        [0x2b, 0x65, 111] => Family::Xdh(56),
        [0x2b, 0x65, 112] => Family::EdDsa(32),
        [0x2b, 0x65, 113] => Family::EdDsa(57),
        [0x60, 0x86, 0x48, 1, 0x65, 3, 4, 3, 17..=19] | [0x60, 0x86, 0x48, 1, 0x65, 3, 4, 4, 1..=3] => Family::Named,
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 9, 16, 3, 17] => return Err(invalid("Java HSS/LMS does not generate private keys")),
        _ => return Err(invalid("Unsupported Java private-key factory")),
    })
}

fn decode(encoded: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    if encoded.len() > i32::MAX as usize { return Err(malformed()); }
    let mut input = Der(encoded);
    let outer = expect(&mut input, 0x30)?;
    input.end()?;
    let mut fields = Der(&outer);
    let version = integer::int(&mut fields)?;
    let mut algorithm = secret_der::algorithm(&mut fields, malformed)?;
    let family = family(&algorithm.oid)?;
    if !matches!(version, 0 | 1) || family == Family::Dh && version != 0 { return Err(malformed()); }
    let canonical_parameters = parameters::validate(&algorithm, family)?;
    let private_key = expect(&mut fields, 4)?;
    if family == Family::Dh {
        fields.end()?;
        integer::signed(&expect(&mut Der(&private_key), 2)?)?;
        return Ok(Zeroizing::new(encoded.to_vec()));
    }
    let mut optional = if fields.0.is_empty() { None } else { Some(next(&mut fields)?) };
    let attributes = if optional.as_ref().is_some_and(|value| value.tag & 0xdf == 0x80) {
        let attributes = optional.take().expect("present PKCS8 attributes").content;
        optional = if fields.0.is_empty() { None } else { Some(next(&mut fields)?) };
        Some(attributes)
    } else { None };
    let mut public_key = if let Some(value) = optional {
        if version != 1 || value.tag & 0xdf != 0x81 { return Err(malformed()); }
        Some(bits(&value.content, fields.0.len())?)
    } else { None };
    fields.end()?;

    let replacement = match family {
        Family::Rsa | Family::Pss => rsa(&private_key)?,
        Family::Dsa => { integer::signed(&expect(&mut Der(&private_key), 2)?)?; None }
        Family::Ec => { if let Some(bits) = ec(&private_key)? { public_key = Some(bits); } None }
        Family::Xdh(length) | Family::EdDsa(length) => {
            if expect(&mut Der(&private_key), 4)?.len() != length { return Err(malformed()); }
            None
        }
        Family::Named => { expect(&mut Der(&private_key), 4)?; None }
        Family::Dh => unreachable!("DH retains its input encoding"),
    };
    let mut parts = Vec::with_capacity(5);
    if let Some(private_key) = replacement {
        let canonical_parameters = canonical_parameters.map(|parameters| parameters.encode()).transpose()?;
        algorithm = make_algorithm(if family == Family::Pss { PSS } else { RSA }, canonical_parameters.as_deref().map(Vec::as_slice))?;
        parts.push(encode(2, &[0]));
        parts.push(algorithm.encoded);
        parts.push(encode(4, &private_key));
    } else {
        parts.push(encode(2, &[version as u8]));
        parts.push(algorithm.encoded);
        parts.push(encode(4, &private_key));
        if version == 1 {
            if let Some(attributes) = attributes { parts.push(encode(0xa0, &attributes)); }
            if let Some(public_key) = public_key { parts.push(encode(0x81, &public_key)); }
        }
    }
    Ok(encode(0x30, &concatenate(&parts, malformed)?))
}

fn rsa(bytes: &[u8]) -> Result<Option<Zeroizing<Vec<u8>>>> {
    let mut input = Der(bytes);
    let content = expect(&mut input, 0x30)?;
    input.end()?;
    let mut fields = Der(&content);
    if integer::int(&mut fields)? != 0 { return Err(malformed()); }
    let components = (0..8).map(|_| expect(&mut fields, 2)).collect::<Result<Vec<_>>>()?;
    fields.end()?;
    let components = components.iter().map(|value| integer::unsigned(value)).collect::<Result<Vec<_>>>()?;
    let modulus_bits = integer::bit_length(components[0])?.div_ceil(8) * 8;
    let exponent_bits = integer::bit_length(components[1])?;
    if !(512..=16_384).contains(&modulus_bits) || modulus_bits > 3072 && exponent_bits > 64 { return Err(malformed()); }
    if [1, 3, 4, 5, 6, 7].iter().all(|index| components[*index] != [0]) { return Ok(None); }
    let mut parts = Vec::with_capacity(9);
    parts.push(encode(2, &[0]));
    for (index, component) in components.iter().enumerate() {
        parts.push(if index == 0 || index == 2 { integer::encode_unsigned(component)? } else { encode(2, &[0]) });
    }
    Ok(Some(encode(0x30, &concatenate(&parts, malformed)?)))
}

fn ec(bytes: &[u8]) -> Result<Option<Zeroizing<Vec<u8>>>> {
    let mut input = Der(bytes);
    let content = expect(&mut input, 0x30)?;
    let sequence_tail = backing_tail(&content, input.0.len(), 0);
    let mut fields = Der(&content);
    if integer::int(&mut fields)? != 1 { return Err(malformed()); }
    expect(&mut fields, 4)?;
    if fields.0.is_empty() { return Ok(None); }
    let mut value = next(&mut fields)?;
    if value.tag & 0xdf == 0x80 {
        if fields.0.is_empty() { return Ok(None); }
        value = next(&mut fields)?;
    }
    if value.tag & 0xdf != 0x81 { return Err(malformed()); }
    let context_tail = backing_tail(&value.content, fields.0.len(), sequence_tail);
    let mut explicit = Der(&value.content);
    let value = expect(&mut explicit, 3)?;
    let tail = backing_tail(&value, explicit.0.len(), context_tail);
    Ok(Some(bits(&value, tail)?))
}

fn backing_tail(value: &SecretBytes<'_>, local: usize, enclosing: usize) -> usize {
    local + match value { SecretBytes::Borrowed(_) => enclosing, SecretBytes::Owned(_) => 0 }
}

fn bits(content: &[u8], tail: usize) -> Result<Zeroizing<Vec<u8>>> {
    let (&padding, bytes) = content.split_first().ok_or_else(malformed)?;
    let (length, padding) = bit_layout(padding, bytes.len(), bytes.len().checked_add(tail).ok_or_else(overflow)?)?;
    let mut bits = Zeroizing::new(Vec::with_capacity(length + 1));
    bits.push(padding); bits.extend_from_slice(&bytes[..length]);
    if padding != 0 { *bits.last_mut().expect("nonempty padded public key") &= 0xff << padding; }
    Ok(bits)
}

fn make_algorithm(oid: &[u8], parameter: Option<&[u8]>) -> Result<Algorithm> {
    let content = concatenate(&[encode(6, oid), Zeroizing::new(parameter.unwrap_or_default().to_vec())], malformed)?;
    secret_der::algorithm(&mut Der(&encode(0x30, &content)), malformed)
}

fn parameter(algorithm: &Algorithm) -> Result<Option<&[u8]>> {
    let mut input = Der(&algorithm.encoded);
    let content = normalized(&mut input, 0x30)?;
    let mut fields = Der(content);
    normalized(&mut fields, 6)?;
    if fields.0.is_empty() || fields.0 == [5, 0] { Ok(None) } else { Ok(Some(fields.0)) }
}

fn normalized<'a>(input: &mut Der<'a>, tag: u8) -> Result<&'a [u8]> {
    match expect(input, tag)? { SecretBytes::Borrowed(bytes) => Ok(bytes), SecretBytes::Owned(_) => Err(malformed()) }
}

fn expect<'a>(input: &mut Der<'a>, tag: u8) -> Result<SecretBytes<'a>> { secret_der::expect(input, tag, malformed) }
fn malformed() -> Anomaly { invalid("Malformed Java private key") }
fn overflow() -> Anomaly { invalid("Java private-key allocation exceeds range") }

#[cfg(test)]
mod tests;
