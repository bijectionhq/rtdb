use super::*;

struct RawPrivateKey { encoded: Zeroizing<Vec<u8>>, target: FactoryTarget, reservation: Arc<MemoryReservation> }

pub(super) fn parse_key(encoded: &[u8], memory: &Arc<MemoryBudget>) -> Result<PrivateKey> {
    let raw = RawPrivateKey::parse(encoded, memory)?;
    match raw.target {
        FactoryTarget::NoSuchAlgorithm | FactoryTarget::InvalidKeySpec => raw.into_private_key(),
        FactoryTarget::Available(_) => {
            let reservation = memory.reserve(allocation_bound(raw.encoded.len())?)?;
            match (FactoryInput { encoded: &raw.encoded, target: raw.target }).generate() {
                Ok(encoded) => {
                    drop(raw);
                    PrivateKey::from_admitted(encoded, reservation).map_err(Anomaly::from)
                }
                Err(FactoryFailure::NoSuchAlgorithm(_) | FactoryFailure::InvalidKeySpec(_)) => {
                    drop(reservation);
                    raw.into_private_key()
                }
                Err(FactoryFailure::Fatal(error)) => Err(error),
            }
        }
    }
}

impl RawPrivateKey {
    fn parse(encoded: &[u8], memory: &Arc<MemoryBudget>) -> Result<Self> {
        let reservation = memory.reserve(allocation_bound(encoded.len())?)?;
        let (encoded, target) = normalize(encoded)?;
        Ok(Self { encoded, target, reservation })
    }

    fn into_private_key(self) -> Result<PrivateKey> {
        let Self { encoded, target: _, reservation } = self;
        PrivateKey::from_admitted(encoded, reservation).map_err(Anomaly::from)
    }
}

fn normalize(encoded: &[u8]) -> Result<(Zeroizing<Vec<u8>>, FactoryTarget)> {
    if encoded.len() > i32::MAX as usize { return Err(malformed()); }
    let mut input = Der(encoded);
    let outer = expect(&mut input, 0x30)?;
    input.end()?;
    let mut fields = Der(&outer);
    let version = expect(&mut fields, 2)?;
    let version = integer::int_content_checked(&version).map_err(|error| match error {
        integer::IntegerError::Malformed | integer::IntegerError::MagnitudeOverflow => malformed(),
    })?;
    if !matches!(version, 0 | 1) { return Err(malformed()); }
    let algorithm = secret_der::algorithm(&mut fields, malformed)?;
    let target = parameters::factory_target(&algorithm)?;
    let private_key = expect(&mut fields, 4)?;
    let mut optional = if fields.0.is_empty() { None } else { Some(secret_der::next(&mut fields)?) };
    let attributes = if optional.as_ref().is_some_and(|value| value.tag & 0xdf == 0x80) {
        let attributes = optional.take().expect("present raw PKCS8 attributes").content;
        optional = if fields.0.is_empty() { None } else { Some(secret_der::next(&mut fields)?) };
        Some(attributes)
    } else { None };
    let public_key = if let Some(value) = optional {
        if version != 1 || value.tag & 0xdf != 0x81 { return Err(malformed()); }
        let (&padding, bytes) = value.content.split_first().ok_or_else(malformed)?;
        let backing = bytes.len().checked_add(fields.0.len()).ok_or_else(overflow)?;
        let (length, padding) = crate::java_der::bit_layout_checked(padding, bytes.len(), backing).map_err(|error| match error {
            crate::java_der::BitLayoutError::JavaIo | crate::java_der::BitLayoutError::Unchecked => crate::java_der::malformed(),
        })?;
        let mut bits = Zeroizing::new(Vec::with_capacity(length + 1));
        bits.push(padding); bits.extend_from_slice(&bytes[..length]);
        if padding != 0 { *bits.last_mut().expect("nonempty padded raw public key") &= 0xff << padding; }
        Some(bits)
    } else { None };
    fields.end()?;
    let mut parts = vec![encode(2, &[version as u8]), algorithm.encoded, encode(4, &private_key)];
    if version == 1 {
        if let Some(attributes) = attributes { parts.push(encode(0xa0, &attributes)); }
        if let Some(public_key) = public_key { parts.push(encode(0x81, &public_key)); }
    }
    Ok((encode(0x30, &concatenate(&parts, malformed)?), target))
}

fn expect<'a>(input: &mut Der<'a>, tag: u8) -> Result<SecretBytes<'a>> { secret_der::expect(input, tag, malformed) }
fn malformed() -> Anomaly { invalid("Malformed SunX509 JKS private key") }
