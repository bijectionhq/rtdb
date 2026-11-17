use super::super::{integer, malformed};
use crate::{java_der::{self, Der}, secret_der::{self, SecretBytes, Value}};
use rtdb_types::Anomaly;
use zeroize::Zeroizing;

pub(super) type DecodeResult<T> = std::result::Result<T, Failure>;

#[derive(Debug)]
pub(super) enum Failure { JavaIo(Anomaly), Propagate(Anomaly) }

impl Failure {
    pub(super) fn into_anomaly(self) -> Anomaly {
        match self { Self::JavaIo(error) | Self::Propagate(error) => error }
    }
}

pub(super) fn malformed_parameter() -> Failure { Failure::JavaIo(malformed()) }

pub(super) fn next<'a>(input: &mut Der<'a>) -> DecodeResult<Value<'a>> { secret_der::next(input).map_err(Failure::JavaIo) }

pub(super) fn expect<'a>(input: &mut Der<'a>, tag: u8) -> DecodeResult<SecretBytes<'a>> {
    let value = next(input)?;
    if value.tag != tag { return Err(malformed_parameter()); }
    Ok(value.content)
}

pub(super) fn end(input: Der<'_>) -> DecodeResult<()> { input.end().map_err(Failure::JavaIo) }

pub(super) fn valid_oid(oid: &[u8]) -> DecodeResult<()> { java_der::valid_oid(oid).map_err(Failure::JavaIo) }

pub(super) fn octets(value: Value<'_>) -> DecodeResult<Zeroizing<Vec<u8>>> { secret_der::octets(value, malformed).map_err(Failure::JavaIo) }

fn integer_failure(error: integer::IntegerError) -> Failure {
    match error {
        integer::IntegerError::Malformed => malformed_parameter(),
        integer::IntegerError::MagnitudeOverflow => Failure::Propagate(malformed()),
    }
}

pub(super) fn signed(bytes: &[u8]) -> DecodeResult<&[u8]> { integer::signed_checked(bytes).map_err(integer_failure) }

pub(super) fn int(input: &mut Der<'_>) -> DecodeResult<i32> { int_content(&expect(input, 2)?) }

pub(super) fn int_content(bytes: &[u8]) -> DecodeResult<i32> { integer::int_content_checked(bytes).map_err(integer_failure) }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn der_tag_oid_cardinality_and_integer_failures_are_java_io() {
        assert!(matches!(next(&mut Der(&[0x30])), Err(Failure::JavaIo(_))));
        assert!(matches!(expect(&mut Der(&[4, 0]), 2), Err(Failure::JavaIo(_))));
        assert!(matches!(valid_oid(&[0x80]), Err(Failure::JavaIo(_))));
        assert!(matches!(end(Der(&[5, 0])), Err(Failure::JavaIo(_))));
        assert!(matches!(signed(&[]), Err(Failure::JavaIo(_))));
        assert!(matches!(int_content(&[0, 0x80, 0, 0, 0]), Err(Failure::JavaIo(_))));
        let value = next(&mut Der(&[2, 0])).unwrap();
        assert!(matches!(octets(value), Err(Failure::JavaIo(_))));
    }

    #[test]
    fn magnitude_overflow_preserves_propagation_and_specified_child_io_is_caught() {
        let error = integer_failure(integer::IntegerError::MagnitudeOverflow);
        assert!(matches!(&error, Failure::Propagate(_)));
        assert_eq!(error.into_anomaly(), malformed());
        let oid = &[0x2a, 0x86, 0x48, 0xce, 0x3d, 4, 3];
        let name = super::super::recursive::initialized_name(oid, &[0x30, 0]).unwrap();
        assert!(matches!(name, super::super::DigestName::Oid(value) if value.as_slice() == oid));
    }

    #[test]
    fn outward_conversion_retains_the_original_diagnostic_for_each_policy() {
        let expected = crate::java_der::malformed();
        let error = next(&mut Der(&[0x30])).err().unwrap().into_anomaly();
        assert_eq!(error, expected);
        let expected = malformed();
        let error = expect(&mut Der(&[4, 0]), 2).err().unwrap().into_anomaly();
        assert_eq!(error, expected);
    }
}
