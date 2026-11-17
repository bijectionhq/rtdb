use super::*;

#[derive(Debug)]
pub(super) enum PreparationFailure { Der(Anomaly), Invalid, Unchecked }

impl PreparationFailure {
    pub(super) fn into_pkcs12(self) -> Anomaly {
        match self {
            Self::Der(error) => error,
            Self::Invalid | Self::Unchecked => invalid("Malformed SunX509 PKCS12 material"),
        }
    }

    #[cfg(test)]
    pub(super) fn into_factory(self) -> FactoryFailure {
        match self {
            Self::Der(error) => FactoryFailure::InvalidKeySpec(error),
            Self::Invalid => failure::malformed(),
            Self::Unchecked => FactoryFailure::Fatal(malformed()),
        }
    }
}

pub(super) fn prepare(encoded: &[u8]) -> std::result::Result<FactoryInput<'_>, PreparationFailure> {
    if encoded.len() > i32::MAX as usize { return Err(PreparationFailure::Unchecked); }
    let mut input = Der(encoded);
    let outer = next(&mut input)?;
    input.end().map_err(PreparationFailure::Der)?;
    if !matches!(outer.tag, 0x30 | 0x31) { return Err(PreparationFailure::Invalid); }
    let mut fields = Der(&outer.content);
    let version = next(&mut fields)?;
    if version.tag != 2 { return Err(PreparationFailure::Invalid); }
    integer::int_content_checked(&version.content).map_err(|error| match error {
        integer::IntegerError::Malformed => PreparationFailure::Invalid,
        integer::IntegerError::MagnitudeOverflow => PreparationFailure::Unchecked,
    })?;
    let algorithm = next(&mut fields)?;
    if algorithm.tag != 0x30 { return Err(PreparationFailure::Invalid); }
    let mut children = Der(&algorithm.content);
    let mut first = None;
    let mut count = 0usize;
    while !children.0.is_empty() {
        let child = next(&mut children)?;
        if first.is_none() { first = Some(child); }
        count = (count + 1).min(3);
    }
    if !(1..=2).contains(&count) { return Err(PreparationFailure::Der(crate::java_der::malformed())); }
    let oid = first.expect("nonempty AlgorithmIdentifier");
    if oid.tag != 6 { return Err(PreparationFailure::Invalid); }
    crate::java_der::valid_oid(&oid.content).map_err(PreparationFailure::Der)?;
    Ok(FactoryInput { encoded, target: FactoryTarget::for_oid(&oid.content) })
}

fn next<'a>(input: &mut Der<'a>) -> std::result::Result<secret_der::Value<'a>, PreparationFailure> {
    secret_der::next(input).map_err(PreparationFailure::Der)
}
