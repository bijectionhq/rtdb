use super::*;
use admission::{DecodeResult, Failure};

const SHA1: &[u8] = &[0x2b, 0x0e, 3, 2, 26];
const MGF1: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 8];

enum DigestName { Oid(Zeroizing<Vec<u8>>), Pbes2(pbes2::Name) }

pub(super) struct Pss { digest: DigestName, mgf_digest: Zeroizing<Vec<u8>>, salt: i32 }

pub(super) fn validate(algorithm: &Algorithm, family: Family) -> Result<Option<Pss>> {
    let parameter = parameter(algorithm)?;
    match family {
        Family::Pss => return parameter.map(pss).transpose(),
        Family::Dsa => if let Some(parameter) = parameter { integers(parameter, 3, false).map_err(Failure::into_anomaly)?; },
        Family::Dh => integers(parameter.ok_or_else(malformed)?, 2, true).map_err(Failure::into_anomaly)?,
        Family::Ec => curve(parameter.ok_or_else(malformed)?).map_err(Failure::into_anomaly)?,
        Family::Named if parameter.is_some() => return Err(malformed()),
        _ => {},
    }
    Ok(None)
}

fn integers(bytes: &[u8], required: usize, optional_int: bool) -> DecodeResult<()> {
    let mut input = Der(bytes);
    let sequence = admission::expect(&mut input, 0x30)?;
    admission::end(input)?;
    let mut fields = Der(&sequence);
    for _ in 0..required { admission::signed(&admission::expect(&mut fields, 2)?)?; }
    if optional_int && !fields.0.is_empty() { admission::int(&mut fields)?; }
    admission::end(fields)
}

fn curve(bytes: &[u8]) -> DecodeResult<()> {
    let mut input = Der(bytes);
    let oid = admission::expect(&mut input, 6)?;
    admission::valid_oid(&oid)?;
    admission::end(input)?;
    if curves::CURVES.iter().any(|(_, candidate)| *candidate == &*oid) { Ok(()) } else { Err(admission::malformed_parameter()) }
}

mod admission;
mod pbes2;
mod recursive;

fn pss(bytes: &[u8]) -> Result<Pss> { recursive::pss(bytes).map_err(Failure::into_anomaly) }

fn fixed_service(oid: &[u8], parameter: &[u8]) -> DecodeResult<()> {
    match oid {
        DSA | [0x2b, 0x0e, 3, 2, 12] => integers(parameter, 3, false)?,
        DH | [0x2a, 0x86, 0x48, 0xce, 0x3e, 2, 1] => integers(parameter, 2, true)?,
        EC => curve(parameter)?,
        [0x60, 0x86, 0x48, 1, 0x65, 3, 4, 1] => primitive_iv(parameter, 16)?,
        [0x2b, 0x0e, 3, 2, 17] | [0x2b, 6, 1, 4, 1, 0x97, 0x55, 1, 1, 2] => primitive_iv(parameter, 8)?,
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 9, 16, 3, 18] => chacha_nonce(parameter)?,
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 3]
            | [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 12, 1, 1 | 2 | 3 | 5 | 6] => legacy_pbe(parameter)?,
        [0x2a, 0x86, 0x48, 0xce, 0x3d, 4, 3] => return Err(Failure::Propagate(unsupported())),
        _ => {},
    }
    Ok(())
}

fn primitive_iv(bytes: &[u8], length: usize) -> DecodeResult<()> {
    let mut input = Der(bytes);
    if admission::expect(&mut input, 4)?.len() != length { return Err(admission::malformed_parameter()); }
    admission::end(input)
}

fn chacha_nonce(bytes: &[u8]) -> DecodeResult<()> {
    let mut input = Der(bytes);
    let nonce = admission::next(&mut input)?;
    admission::end(input)?;
    if admission::octets(nonce)?.len() != 12 { return Err(admission::malformed_parameter()); }
    Ok(())
}

fn legacy_pbe(bytes: &[u8]) -> DecodeResult<()> {
    let mut input = Der(bytes);
    let sequence = admission::expect(&mut input, 0x30)?;
    admission::end(input)?;
    let mut fields = Der(&sequence);
    admission::expect(&mut fields, 4)?;
    admission::int(&mut fields)?;
    admission::end(fields)
}

fn is_mgf_digest(oid: &[u8]) -> bool { matches!(oid, SHA1 | [0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, 1..=10]) }

impl Pss {
    pub(super) fn encode(&self) -> Result<Zeroizing<Vec<u8>>> {
        let mut fields = Vec::with_capacity(3);
        let digest = match &self.digest { DigestName::Oid(oid) => canonical_oid(oid)?, DigestName::Pbes2(name) => name.reencoding_oid()? };
        if digest != SHA1 { fields.push(encode(0xa0, &make_algorithm(digest, None)?.encoded)); }
        if self.mgf_digest.as_slice() != SHA1 {
            let digest = make_algorithm(&self.mgf_digest, None)?;
            fields.push(encode(0xa1, &make_algorithm(MGF1, Some(&digest.encoded))?.encoded));
        }
        if self.salt != 20 {
            let bytes = self.salt.to_be_bytes();
            fields.push(encode(0xa2, &encode(2, integer::signed(&bytes)?)));
        }
        Ok(encode(0x30, &concatenate(&fields, malformed)?))
    }
}

fn canonical_oid(oid: &[u8]) -> Result<&[u8]> {
    Ok(match oid {
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1] | [0x55, 8, 1, 1] => RSA,
        [0x2b, 0x0e, 3, 2, 12] => DSA,
        [0x2a, 0x86, 0x48, 0xce, 0x3e, 2, 1] => DH,
        [0x2b, 0x0e, 3, 2, 13 | 27] => &[0x2a, 0x86, 0x48, 0xce, 0x38, 4, 3],
        [0x2b, 0x0e, 3, 2, 15 | 29] => &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 5],
        [0x2b, 6, 1, 5, 5, 7, 3, 8] | [0x2b, 6, 1, 5, 5, 7, 48, 3] => return Err(malformed()),
        _ => oid,
    })
}

fn unsupported() -> Anomaly { invalid("Unsupported nested Java private-key AlgorithmParameters") }
