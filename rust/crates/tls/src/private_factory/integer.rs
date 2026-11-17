use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum IntegerError { Malformed, MagnitudeOverflow }

pub(super) fn signed(bytes: &[u8]) -> Result<&[u8]> { signed_checked(bytes).map_err(|_| malformed()) }

pub(super) fn signed_checked(mut bytes: &[u8]) -> std::result::Result<&[u8], IntegerError> {
    if bytes.is_empty() { return Err(IntegerError::Malformed); }
    while bytes.len() > 1 && (bytes[0] == 0 && bytes[1] & 0x80 == 0 || bytes[0] == 0xff && bytes[1] & 0x80 != 0) { bytes = &bytes[1..]; }
    check_signed_range(bytes.len(), bytes[0], || bytes[1..].iter().all(|byte| *byte == 0))?;
    Ok(bytes)
}

#[cfg(test)]
pub(super) fn unsigned(bytes: &[u8]) -> Result<&[u8]> { unsigned_checked(bytes).map_err(|_| malformed()) }

pub(super) fn unsigned_checked(mut bytes: &[u8]) -> std::result::Result<&[u8], IntegerError> {
    if bytes.is_empty() { return Err(IntegerError::Malformed); }
    while bytes.len() > 1 && bytes[0] == 0 { bytes = &bytes[1..]; }
    check_unsigned_range(bytes.len(), bytes[0])?;
    Ok(bytes)
}

fn signed_range(length: usize, first: u8, zero_tail: impl FnOnce() -> bool) -> bool {
    const MAX_BYTES: usize = 1 << 28;
    length < MAX_BYTES || length == MAX_BYTES && (first != 0x80 || !zero_tail())
}

fn check_signed_range(length: usize, first: u8, zero_tail: impl FnOnce() -> bool) -> std::result::Result<(), IntegerError> {
    if signed_range(length, first, zero_tail) { Ok(()) } else { Err(IntegerError::MagnitudeOverflow) }
}

fn unsigned_range(length: usize, first: u8) -> bool { length < 1 << 28 || length == 1 << 28 && first < 0x80 }

fn check_unsigned_range(length: usize, first: u8) -> std::result::Result<(), IntegerError> {
    if unsigned_range(length, first) { Ok(()) } else { Err(IntegerError::MagnitudeOverflow) }
}

#[cfg(test)]
pub(super) fn int(input: &mut Der<'_>) -> Result<i32> {
    let value = expect(input, 2)?;
    int_content(&value)
}

#[cfg(test)]
pub(super) fn int_content(bytes: &[u8]) -> Result<i32> { int_content_checked(bytes).map_err(|_| malformed()) }

pub(super) fn int_content_checked(bytes: &[u8]) -> std::result::Result<i32, IntegerError> {
    let value = signed_checked(bytes)?;
    if value.len() > 4 { return Err(IntegerError::Malformed); }
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

    #[test]
    fn typed_signed_integer_results_preserve_sign_padding_and_java_int_boundaries() {
        for (bytes, expected) in [(&[0, 0, 1][..], &[1][..]), (&[0xff, 0xff, 0x80], &[0x80]), (&[0, 0x80], &[0, 0x80])] {
            assert_eq!(signed_checked(bytes).unwrap(), expected);
            assert_eq!(signed(bytes).unwrap(), expected);
        }
        for (bytes, expected) in [(&[0][..], 0), (&[0xff], -1), (&[0x80, 0, 0, 0], i32::MIN), (&[0x7f, 0xff, 0xff, 0xff], i32::MAX)] {
            assert_eq!(int_content_checked(bytes).unwrap(), expected);
            assert_eq!(int_content(bytes).unwrap(), expected);
        }
        for bytes in [&[][..], &[0, 0x80, 0, 0, 0], &[0xff, 0x7f, 0xff, 0xff, 0xff]] {
            assert_eq!(int_content_checked(bytes), Err(IntegerError::Malformed));
            assert!(int_content(bytes).is_err());
        }
        assert_eq!(signed_checked(&[]), Err(IntegerError::Malformed));
    }

    #[test]
    fn typed_magnitude_overflow_is_distinct_from_java_int_range_failure() {
        assert_eq!(check_signed_range((1 << 28) - 1, 0x80, || true), Ok(()));
        assert_eq!(check_signed_range(1 << 28, 0x80, || true), Err(IntegerError::MagnitudeOverflow));
        assert_eq!(check_signed_range(1 << 28, 0x80, || false), Ok(()));
        assert_eq!(check_signed_range(1 << 28, 0x7f, || panic!("positive boundary has no tail check")), Ok(()));
        assert_eq!(check_signed_range((1 << 28) + 1, 1, || false), Err(IntegerError::MagnitudeOverflow));
        assert_eq!(int_content_checked(&[0, 0x80, 0, 0, 0]), Err(IntegerError::Malformed));
    }

    #[test]
    fn typed_unsigned_integers_keep_positive_interpretation_and_empty_rejection() {
        for (bytes, expected) in [(&[0, 0, 0x80][..], &[0x80][..]), (&[0xff], &[0xff]), (&[0, 0], &[0])] {
            assert_eq!(unsigned_checked(bytes).unwrap(), expected);
            assert_eq!(unsigned(bytes).unwrap(), expected);
        }
        assert_eq!(unsigned_checked(&[]), Err(IntegerError::Malformed));
        assert!(unsigned(&[]).is_err());
        assert_eq!(int(&mut Der(&[2, 1, 1])).unwrap(), 1);
    }

    #[test]
    fn typed_unsigned_magnitude_boundaries_are_unchecked_big_integer_failures() {
        assert_eq!(check_unsigned_range((1 << 28) - 1, 0xff), Ok(()));
        assert_eq!(check_unsigned_range(1 << 28, 0x7f), Ok(()));
        assert_eq!(check_unsigned_range(1 << 28, 0x80), Err(IntegerError::MagnitudeOverflow));
        assert_eq!(check_unsigned_range((1 << 28) + 1, 1), Err(IntegerError::MagnitudeOverflow));
    }
}
