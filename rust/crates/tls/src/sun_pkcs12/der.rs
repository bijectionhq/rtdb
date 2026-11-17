use rtdb_types::{Anomaly, Result};
use zeroize::Zeroizing;
use crate::{invalid, java_der::{Der, valid_oid}, secret_der::{self, SecretBytes, Value, next}};

pub(super) fn malformed() -> Anomaly { invalid("Malformed SunX509 PKCS12 material") }
pub(super) fn expect<'a>(input: &mut Der<'a>, tag: u8) -> Result<SecretBytes<'a>> { secret_der::expect(input, tag, malformed) }
pub(super) fn octets(value: Value<'_>) -> Result<Zeroizing<Vec<u8>>> { secret_der::octets(value, malformed) }
pub(super) fn values(bytes: &[u8]) -> Result<Vec<Value<'_>>> {
    let mut input = Der(bytes); let mut values = Vec::new();
    while !input.0.is_empty() { values.push(next(&mut input)?); }
    Ok(values)
}
pub(super) fn fields<'a>(value: &'a Value<'_>) -> Result<Der<'a>> {
    if !matches!(value.tag, 0x30 | 0x31) { return Err(malformed()); }
    Ok(Der(&value.content))
}
pub(super) fn oid(input: &mut Der<'_>) -> Result<Zeroizing<Vec<u8>>> {
    let value = expect(input, 6)?; valid_oid(&value)?; Ok(value.into_owned())
}
pub(super) fn integer(input: &mut Der<'_>) -> Result<i32> {
    let value = expect(input, 2)?;
    let mut bytes: &[u8] = &value;
    if bytes.is_empty() { return Err(malformed()); }
    while bytes.len() > 1 && ((bytes[0] == 0 && bytes[1] & 0x80 == 0) || (bytes[0] == 0xff && bytes[1] & 0x80 != 0)) { bytes = &bytes[1..]; }
    if bytes.len() > 4 { return Err(malformed()); }
    Ok(bytes.iter().fold(if bytes[0] & 0x80 == 0 { 0 } else { -1i32 }, |number, byte| number.wrapping_shl(8) | i32::from(*byte)))
}
