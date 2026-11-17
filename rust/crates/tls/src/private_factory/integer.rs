use super::*;

pub(super) fn signed(mut bytes: &[u8]) -> Result<&[u8]> {
    if bytes.is_empty() { return Err(malformed()); }
    while bytes.len() > 1 && (bytes[0] == 0 && bytes[1] & 0x80 == 0 || bytes[0] == 0xff && bytes[1] & 0x80 != 0) { bytes = &bytes[1..]; }
    if !signed_range(bytes.len(), bytes[0], || bytes[1..].iter().all(|byte| *byte == 0)) { return Err(malformed()); }
    Ok(bytes)
}

pub(super) fn unsigned(mut bytes: &[u8]) -> Result<&[u8]> {
    if bytes.is_empty() { return Err(malformed()); }
    while bytes.len() > 1 && bytes[0] == 0 { bytes = &bytes[1..]; }
    if !unsigned_range(bytes.len(), bytes[0]) { return Err(malformed()); }
    Ok(bytes)
}

fn signed_range(length: usize, first: u8, zero_tail: impl FnOnce() -> bool) -> bool {
    const MAX_BYTES: usize = 1 << 28;
    length < MAX_BYTES || length == MAX_BYTES && (first != 0x80 || !zero_tail())
}

fn unsigned_range(length: usize, first: u8) -> bool { length < 1 << 28 || length == 1 << 28 && first < 0x80 }

pub(super) fn int(input: &mut Der<'_>) -> Result<i32> {
    let value = expect(input, 2)?;
    let value = signed(&value)?;
    if value.len() > 4 { return Err(malformed()); }
    let initial = if value[0] & 0x80 == 0 { 0i32 } else { -1i32 };
    Ok(value.iter().fold(initial, |result, byte| result.wrapping_shl(8) | i32::from(*byte)))
}

pub(super) fn bit_length(value: &[u8]) -> Result<usize> {
    value.len().checked_sub(1).and_then(|bytes| bytes.checked_mul(8)).and_then(|bits| bits.checked_add(8 - value[0].leading_zeros() as usize)).ok_or_else(overflow)
}

pub(super) fn encode_unsigned(value: &[u8]) -> Result<Zeroizing<Vec<u8>>> {
    let first = value.first().ok_or_else(malformed)?;
    let mut content = Zeroizing::new(Vec::with_capacity(value.len() + 1));
    if first & 0x80 != 0 { content.push(0); }
    content.extend_from_slice(value);
    Ok(encode(2, &content))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_and_unsigned_magnitude_ranges_preserve_java_boundaries() {
        assert!(signed_range((1 << 28) - 1, 0x80, || true));
        assert!(!signed_range(1 << 28, 0x80, || true));
        assert!(signed_range(1 << 28, 0x80, || false));
        assert!(signed_range(1 << 28, 0x7f, || panic!("positive boundary has no tail check")));
        assert!(!signed_range((1 << 28) + 1, 0x7f, || false));
        assert!(unsigned_range(1 << 28, 0x7f));
        assert!(!unsigned_range(1 << 28, 0x80));
        assert!(!unsigned_range((1 << 28) + 1, 1));
    }

    #[test]
    fn unsigned_integer_encoding_rejects_empty_input_and_preserves_positive_signs() {
        assert!(encode_unsigned(&[]).is_err());
        assert_eq!(encode_unsigned(&[0]).unwrap().as_slice(), [2, 1, 0]);
        assert_eq!(encode_unsigned(&[0x7f]).unwrap().as_slice(), [2, 1, 0x7f]);
        assert_eq!(encode_unsigned(&[0x80]).unwrap().as_slice(), [2, 2, 0, 0x80]);
    }
}
