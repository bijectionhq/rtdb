use rtdb_types::Result;
use crate::{java_der::{Der, valid_oid}, secret_der::{SecretBytes, Value, next}};
use super::der::{expect, malformed, octets};

#[derive(Default)]
pub(super) struct Attributes {
    pub(super) alias: Option<Vec<u16>>,
    pub(super) key_id: Option<Vec<u8>>,
    pub(super) trusted: bool,
}

const FRIENDLY_NAME: &[u8] = b"\x2a\x86\x48\x86\xf7\x0d\x01\x09\x14";
const LOCAL_KEY_ID: &[u8] = b"\x2a\x86\x48\x86\xf7\x0d\x01\x09\x15";
const TRUSTED_KEY_USAGE: &[u8] = b"\x60\x86\x48\x01\x86\xf9\x66\xad\xca\x7b\x01\x01";

pub(super) fn parse(input: &mut Der<'_>) -> Result<Attributes> {
    let Ok(content) = checked_set(input) else { return Ok(Attributes::default()); };
    let mut input = Der(&content);
    let mut attributes = Attributes::default();
    while !input.0.is_empty() {
        let content = expect(&mut input, 0x30)?;
        let mut fields = Der(&content);
        let oid = expect(&mut fields, 6)?;
        valid_oid(&oid)?;
        let values = checked_set(&mut fields)?;
        fields.end()?;
        let mut values = Der(&values);
        match &*oid {
            FRIENDLY_NAME => attributes.alias = Some(bmp_units(&expect(&mut values, 0x1e)?)),
            LOCAL_KEY_ID => attributes.key_id = Some(octets(next(&mut values)?)?.to_vec()),
            TRUSTED_KEY_USAGE => {
                while !values.0.is_empty() { valid_oid(&expect(&mut values, 6)?)?; }
                attributes.trusted = true;
            }
            _ => while !values.0.is_empty() { validate_value(&next(&mut values)?)?; },
        }
    }
    Ok(attributes)
}

fn checked_set<'a>(input: &mut Der<'a>) -> Result<SecretBytes<'a>> {
    let content = expect(input, 0x31)?;
    let mut check = Der(&content);
    while !check.0.is_empty() { next(&mut check)?; }
    Ok(content)
}

fn bmp_units(bytes: &[u8]) -> Vec<u16> {
    let mut result = Vec::with_capacity(bytes.len().div_ceil(2));
    let mut offset = 0;
    while offset + 1 < bytes.len() {
        let unit = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]);
        offset += 2;
        if (0xd800..=0xdbff).contains(&unit) {
            if offset + 1 >= bytes.len() { result.push(0xfffd); return result; }
            let next = u16::from_be_bytes([bytes[offset], bytes[offset + 1]]);
            offset += 2;
            if (0xdc00..=0xdfff).contains(&next) { result.extend([unit, next]); }
            else { result.push(0xfffd); }
        } else if (0xdc00..=0xdfff).contains(&unit) { result.push(0xfffd); }
        else { result.push(unit); }
    }
    if offset < bytes.len() { result.push(0xfffd); }
    result
}

fn validate_value(value: &Value<'_>) -> Result<()> {
    match value.tag {
        1 if value.content.len() != 1 => Err(malformed()),
        2 => integer(&value.content),
        6 => valid_oid(&value.content),
        0x17 => time(&value.content, false),
        0x18 => time(&value.content, true),
        _ => Ok(()),
    }
}

fn integer(mut bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() { return Err(malformed()); }
    while bytes.len() > 1 && (bytes[0] == 0 && bytes[1] & 0x80 == 0 || bytes[0] == 0xff && bytes[1] & 0x80 != 0) { bytes = &bytes[1..]; }
    if !integer_range(bytes.len(), bytes[0], || bytes[1..].iter().all(|byte| *byte == 0)) { return Err(malformed()); }
    Ok(())
}

fn integer_range(length: usize, first: u8, zero_tail: impl FnOnce() -> bool) -> bool {
    const MAX_BYTES: usize = 1 << 28;
    length < MAX_BYTES || length == MAX_BYTES && (first != 0x80 || !zero_tail())
}

fn time(mut bytes: &[u8], generalized: bool) -> Result<()> {
    if generalized && bytes.len() < 13 || !generalized && !(11..=17).contains(&bytes.len()) { return Err(malformed()); }
    digits(&mut bytes, if generalized { 4 } else { 2 })?;
    let month = digits(&mut bytes, 2)?;
    let day = digits(&mut bytes, 2)?;
    let hour = digits(&mut bytes, 2)?;
    let minute = digits(&mut bytes, 2)?;
    let second = if bytes.len() > 2 {
        let second = digits(&mut bytes, 2)?;
        if generalized && matches!(bytes.first(), Some(b'.' | b',')) {
            bytes = &bytes[1..];
            let mut count = 0;
            while !matches!(bytes.first(), Some(b'Z' | b'+' | b'-')) {
                digits(&mut bytes, 1)?;
                count += 1;
            }
            if count == 0 { return Err(malformed()); }
        }
        second
    } else { 0 };
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || hour >= 24 || minute >= 60 || second >= 60 { return Err(malformed()); }
    match bytes {
        b"Z" => Ok(()),
        [b'+' | b'-', ..] if bytes.len() == 5 => {
            bytes = &bytes[1..];
            if digits(&mut bytes, 2)? >= 24 || digits(&mut bytes, 2)? >= 60 { return Err(malformed()); }
            Ok(())
        }
        _ => Err(malformed()),
    }
}

fn digits(input: &mut &[u8], count: usize) -> Result<u16> {
    if input.len() < count { return Err(malformed()); }
    let (number, tail) = input.split_at(count);
    let value = number.iter().try_fold(0u16, |value, byte| if byte.is_ascii_digit() { Ok(value * 10 + u16::from(*byte - b'0')) } else { Err(malformed()) })?;
    *input = tail;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::java_der::encoded;

    fn attribute(oid: &[u8], values: &[u8]) -> Vec<u8> {
        encoded(0x30, &[encoded(6, oid), encoded(0x31, values)].concat())
    }

    fn parsed(attributes: &[u8]) -> Result<Attributes> { parse(&mut Der(&encoded(0x31, attributes))) }
    fn valid(tag: u8, content: &[u8]) -> bool { validate_value(&Value { tag, content: SecretBytes::Borrowed(content) }).is_ok() }

    #[test]
    fn malformed_outer_set_discards_all_attributes_but_semantic_errors_fail() {
        let good = attribute(FRIENDLY_NAME, &encoded(0x1e, &[0, b'A']));
        let mut truncated = good.clone();
        truncated.push(0x30);
        let result = parsed(&truncated).unwrap();
        assert!(result.alias.is_none() && result.key_id.is_none() && !result.trusted);
        assert!(parse(&mut Der(&[0x30, 0])).unwrap().alias.is_none());
        assert!(parsed(&[encoded(4, &[]), good].concat()).is_err());
        assert!(parsed(&attribute(FRIENDLY_NAME, &encoded(4, b"A"))).is_err());
        assert!(parsed(&attribute(FRIENDLY_NAME, &[])).is_err());
    }

    #[test]
    fn known_attributes_take_first_values_and_last_duplicate_attributes() {
        let first = attribute(FRIENDLY_NAME, &[encoded(0x1e, &[0, b'A']), encoded(2, &[])].concat());
        let last = attribute(FRIENDLY_NAME, &encoded(0x1e, &[0, b'B']));
        let id = attribute(LOCAL_KEY_ID, &encoded(0x24, &[encoded(4, b"x"), encoded(4, b"y")].concat()));
        let result = parsed(&[first, last, id, attribute(TRUSTED_KEY_USAGE, &[])].concat()).unwrap();
        assert_eq!(result.alias, Some(vec![u16::from(b'B')]));
        assert_eq!(result.key_id.as_deref(), Some(b"xy".as_slice()));
        assert!(result.trusted);
        assert!(parsed(&attribute(TRUSTED_KEY_USAGE, &encoded(6, &[0x80]))).is_err());
        assert!(parsed(&attribute(TRUSTED_KEY_USAGE, &encoded(4, &[1]))).is_err());
        let mut malformed_later_value = encoded(0x1e, &[0, b'A']);
        malformed_later_value.push(2);
        assert!(parsed(&attribute(FRIENDLY_NAME, &malformed_later_value)).is_err());
    }

    #[test]
    fn bmp_aliases_keep_java_utf16_replacement_consumption_and_bom() {
        assert_eq!(bmp_units(&[0xfe, 0xff, 0, b'A']), [0xfeff, 65]);
        assert_eq!(bmp_units(&[0xd8, 0, 0xdc, 0]), [0xd800, 0xdc00]);
        assert_eq!(bmp_units(&[0xd8, 0, 0, b'A', 0, b'B']), [0xfffd, 66]);
        assert_eq!(bmp_units(&[0xd8, 0, 0]), [0xfffd]);
        assert_eq!(bmp_units(&[0xdc, 0, 0]), [0xfffd, 0xfffd]);
        assert_eq!(bmp_units(&[0, b'A', 0]), [65, 0xfffd]);
    }

    #[test]
    fn unknown_attribute_values_validate_java_typed_values_and_accept_other_payloads() {
        assert!(!valid(1, &[]));
        assert!(!valid(1, &[0, 0]));
        for byte in 0..=255 { assert!(valid(1, &[byte])); }
        assert!(!valid(2, &[]));
        assert!(valid(2, &[0, 0, 0x7f]));
        assert!(valid(2, &[0xff, 0xff, 0x80]));
        assert!(!valid(6, &[0x80]));
        assert!(valid(6, &[0x2a, 3]));
        for tag in [4, 0x0c, 0x13, 0x14, 0x16, 0x1b, 0x1c, 0x1e, 0x24, 0xa0] { assert!(valid(tag, &[0xff])); }
        assert!(parsed(&attribute(&[0x2a, 3], &encoded(2, &[]))).is_err());
        assert!(parsed(&attribute(&[0x2a, 3], &[])).is_ok());
    }

    #[test]
    fn java_time_values_allow_missing_seconds_only_with_z_and_normalize_calendar_days() {
        for value in [b"2502311234Z".as_slice(), b"250231123459Z", b"250101000000+2359", b"250101000000-2359"] { assert!(valid(0x17, value), "{value:?}"); }
        for value in [b"000002310000Z".as_slice(), b"20250231000000Z", b"20250101000000+0100", b"20250101000000-0100"] { assert!(valid(0x18, value), "{value:?}"); }
        for (tag, value) in [(0x17, b"2501010000+0100".as_slice()), (0x18, b"202501010000+0100".as_slice())] { assert!(!valid(tag, value)); }
    }

    #[test]
    fn generalized_fraction_requires_digits_and_accepts_unbounded_precision() {
        for value in [b"20250101000000.1Z".as_slice(), b"20250101000000,123456789Z", b"20250101000000.123456789+2359"] { assert!(valid(0x18, value), "{value:?}"); }
        for value in [b"20250101000000.Z".as_slice(), b"20250101000000,Z", b"20250101000000.1", b"20250101000000.aZ", b"20250101000000.Zjunk"] { assert!(!valid(0x18, value), "{value:?}"); }
        let long = [b"20250101000000.".as_slice(), &vec![b'1'; 1024], b"Z"].concat();
        assert!(valid(0x18, &long));
        assert!(!valid(0x17, b"250101000000.1Z"));
    }

    #[test]
    fn invalid_time_ranges_digits_and_offsets_fail_unknown_attribute_loading() {
        for value in [b"20250001000000Z".as_slice(), b"20251301000000Z", b"20250100000000Z", b"20250132000000Z", b"20250101240000Z", b"20250101006000Z", b"20250101000060Z", b"20250101000000+2400", b"20250101000000-0060", b"20250101000000z", b"202x0101000000Z", b"20250101000000Z0", b"20250101000000"] {
            assert!(!valid(0x18, value), "{value:?}");
            assert!(parsed(&attribute(&[0x2a, 3], &encoded(0x18, value))).is_err());
        }
        for value in [b"250101000Z".as_slice(), b"250101000000+00000"] { assert!(!valid(0x17, value)); }
    }

    #[test]
    fn signed_integer_magnitude_limits_match_big_integer_without_boundary_allocations() {
        assert!(integer_range((1 << 28) - 1, 0x80, || true));
        assert!(integer_range(1 << 28, 0x7f, || panic!("positive boundary")));
        assert!(integer_range(1 << 28, 0x80, || false));
        assert!(!integer_range(1 << 28, 0x80, || true));
        assert!(!integer_range((1 << 28) + 1, 0x7f, || false));
    }
}
