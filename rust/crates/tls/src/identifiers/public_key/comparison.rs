use super::*;

#[derive(PartialEq, Eq)]
pub(super) enum Key<'a> { Encoding(&'a [u8]), DiffieHellman { p: &'a [u8], g: &'a [u8], y: &'a [u8] } }

impl<'a> Key<'a> {
    pub(super) fn read(key: &'a CertificatePublicKeyEncoding) -> Result<Self> {
        let mut outer = Der(key.encoded());
        let mut key_fields = Der(normalized_value(&mut outer, 0x30)?);
        let mut algorithm = Der(normalized_value(&mut key_fields, 0x30)?);
        let oid = normalized_value(&mut algorithm, 6)?;
        if !matches!(oid, [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 3, 1] | [0x2a, 0x86, 0x48, 0xce, 0x3e, 2, 1]) {
            return Ok(Self::Encoding(key.encoded()));
        }
        let mut parameters = Der(normalized_value(&mut algorithm, 0x30)?);
        let p = integer(&mut parameters)?;
        let g = integer(&mut parameters)?;
        if !parameters.0.is_empty() && integer(&mut parameters)?.len() > 4 { return Err(malformed()); }
        parameters.end()?;
        let bits = normalized_value(&mut key_fields, 3)?;
        let mut value = Der(&bits[1..]);
        let y = integer(&mut value)?;
        Ok(Self::DiffieHellman { p, g, y })
    }
}

fn normalized_value<'a>(reader: &mut Der<'a>, tag: u8) -> Result<&'a [u8]> {
    if reader.0.first() != Some(&tag) || reader.0.get(1) == Some(&0x80) { return Err(malformed()); }
    match reader.expect(tag)? { Cow::Borrowed(value) => Ok(value), Cow::Owned(_) => Err(malformed()) }
}

fn integer<'a>(reader: &mut Der<'a>) -> Result<&'a [u8]> {
    let mut value = normalized_value(reader, 2)?;
    if value.is_empty() { return Err(malformed()); }
    while value.len() > 1 && (value[0] == 0 && value[1] & 0x80 == 0 || value[0] == 0xff && value[1] & 0x80 != 0) { value = &value[1..]; }
    if !canonical_magnitude_in_range(value.len(), value[0], || value[1..].iter().all(|byte| *byte == 0)) { return Err(malformed()); }
    Ok(value)
}

// OpenJDK 25.0.1 BigInteger.checkRange rejects absolute magnitudes at or above 2^Integer.MAX_VALUE.
fn canonical_magnitude_in_range(length: usize, first: u8, zero_tail: impl FnOnce() -> bool) -> bool {
    const MAX_BYTES: usize = 1 << 28;
    length < MAX_BYTES || length == MAX_BYTES && (first != 0x80 || !zero_tail())
}

#[cfg(test)]
mod tests;
