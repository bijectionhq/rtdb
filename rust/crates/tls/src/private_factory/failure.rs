use super::integer;
use crate::{java_der::{self, Der, BitLayoutError}, secret_der::{self, Algorithm, SecretBytes, Value}};
use rtdb_types::Anomaly;
use zeroize::Zeroizing;

pub(super) type FactoryResult<T> = std::result::Result<T, FactoryFailure>;

#[derive(Debug)]
pub(super) enum FactoryFailure { NoSuchAlgorithm(Anomaly), InvalidKeySpec(Anomaly), Fatal(Anomaly) }

impl FactoryFailure {
    pub(super) fn into_anomaly(self) -> Anomaly {
        match self { Self::NoSuchAlgorithm(error) | Self::InvalidKeySpec(error) | Self::Fatal(error) => error }
    }
}

pub(super) fn malformed() -> FactoryFailure { FactoryFailure::InvalidKeySpec(super::malformed()) }

pub(super) fn next<'a>(input: &mut Der<'a>) -> FactoryResult<Value<'a>> { secret_der::next(input).map_err(FactoryFailure::InvalidKeySpec) }

pub(super) fn expect<'a>(input: &mut Der<'a>, tag: u8) -> FactoryResult<SecretBytes<'a>> {
    let value = next(input)?;
    if value.tag != tag { return Err(malformed()); }
    Ok(value.content)
}

pub(super) fn end(input: Der<'_>) -> FactoryResult<()> { input.end().map_err(FactoryFailure::InvalidKeySpec) }

pub(super) fn algorithm(input: &mut Der<'_>) -> FactoryResult<Algorithm> {
    secret_der::algorithm(input, super::malformed).map_err(FactoryFailure::InvalidKeySpec)
}

pub(super) fn concatenate(parts: &[Zeroizing<Vec<u8>>]) -> FactoryResult<Zeroizing<Vec<u8>>> {
    secret_der::concatenate(parts, super::malformed).map_err(FactoryFailure::Fatal)
}

fn integer_failure(error: integer::IntegerError) -> FactoryFailure {
    match error {
        integer::IntegerError::Malformed => malformed(),
        integer::IntegerError::MagnitudeOverflow => FactoryFailure::Fatal(super::malformed()),
    }
}

pub(super) fn signed(bytes: &[u8]) -> FactoryResult<&[u8]> { integer::signed_checked(bytes).map_err(integer_failure) }

pub(super) fn unsigned(bytes: &[u8]) -> FactoryResult<&[u8]> { integer::unsigned_checked(bytes).map_err(integer_failure) }

pub(super) fn int(input: &mut Der<'_>) -> FactoryResult<i32> { integer::int_content_checked(&expect(input, 2)?).map_err(integer_failure) }

pub(super) fn bit_layout(padding: u8, bytes: usize, backing: usize) -> FactoryResult<(usize, u8)> {
    java_der::bit_layout_checked(padding, bytes, backing).map_err(|error| match error {
        BitLayoutError::JavaIo => FactoryFailure::InvalidKeySpec(java_der::malformed()),
        BitLayoutError::Unchecked => FactoryFailure::Fatal(java_der::malformed()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_integer_io_and_unchecked_failures_keep_their_classes() {
        assert!(matches!(signed(&[]), Err(FactoryFailure::InvalidKeySpec(_))));
        assert!(matches!(unsigned(&[]), Err(FactoryFailure::InvalidKeySpec(_))));
        assert!(matches!(int(&mut Der(&[2, 5, 0, 0x80, 0, 0, 0])), Err(FactoryFailure::InvalidKeySpec(_))));
        let error = integer_failure(integer::IntegerError::MagnitudeOverflow);
        assert!(matches!(&error, FactoryFailure::Fatal(_)));
        assert_eq!(error.into_anomaly(), super::super::malformed());
    }

    #[test]
    fn bit_array_backing_failures_propagate_while_der_padding_failures_reject_the_key() {
        for (padding, bytes, backing) in [(8, 1, 1), (0, 268_435_456, 268_435_456), (1, 536_870_912, 536_870_912)] {
            let error = bit_layout(padding, bytes, backing).unwrap_err();
            assert!(matches!(&error, FactoryFailure::InvalidKeySpec(_)));
            assert_eq!(error.into_anomaly(), java_der::malformed());
        }
        for (padding, bytes, backing) in [(1, 268_435_456, 268_435_456), (0, 1, 268_435_456), (0, 1, 536_870_912), (0, 0, 268_435_456)] {
            let error = bit_layout(padding, bytes, backing).unwrap_err();
            assert!(matches!(&error, FactoryFailure::Fatal(_)));
            assert_eq!(error.into_anomaly(), java_der::malformed());
        }
        assert_eq!(bit_layout(0, 536_870_912, 536_870_912).unwrap(), (0, 0));
        assert_eq!(bit_layout(7, 536_870_913, 536_870_913).unwrap(), (1, 7));
    }
}
