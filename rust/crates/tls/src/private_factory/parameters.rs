use super::*;

const SHA1: &[u8] = &[0x2b, 0x0e, 3, 2, 26];
const MGF1: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 8];

pub(super) struct Pss { digest: Zeroizing<Vec<u8>>, mgf_digest: Zeroizing<Vec<u8>>, salt: i32 }

pub(super) fn validate(algorithm: &Algorithm, family: Family) -> Result<Option<Pss>> {
    let parameter = parameter(algorithm)?;
    match family {
        Family::Pss => return parameter.map(pss).transpose(),
        Family::Dsa => if let Some(parameter) = parameter { integers(parameter, 3, false)?; },
        Family::Dh => integers(parameter.ok_or_else(malformed)?, 2, true)?,
        Family::Ec => curve(parameter.ok_or_else(malformed)?)?,
        Family::Named if parameter.is_some() => return Err(malformed()),
        _ => {},
    }
    Ok(None)
}

fn integers(bytes: &[u8], required: usize, optional_int: bool) -> Result<()> {
    let mut input = Der(bytes);
    let sequence = expect(&mut input, 0x30)?;
    input.end()?;
    let mut fields = Der(&sequence);
    for _ in 0..required { integer::signed(&expect(&mut fields, 2)?)?; }
    if optional_int && !fields.0.is_empty() { integer::int(&mut fields)?; }
    fields.end()
}

fn curve(bytes: &[u8]) -> Result<()> {
    let mut input = Der(bytes);
    let oid = expect(&mut input, 6)?;
    crate::java_der::valid_oid(&oid)?;
    input.end()?;
    if curves::CURVES.iter().any(|(_, candidate)| *candidate == &*oid) { Ok(()) } else { Err(malformed()) }
}

fn pss(bytes: &[u8]) -> Result<Pss> {
    let mut input = Der(bytes);
    let sequence = expect(&mut input, 0x30)?;
    let mut fields = Der(&sequence);
    let mut result = Pss { digest: Zeroizing::new(SHA1.to_vec()), mgf_digest: Zeroizing::new(SHA1.to_vec()), salt: 20 };
    while !fields.0.is_empty() {
        let value = next(&mut fields)?;
        let mut inner = Der(&value.content);
        match value.tag & 0xdf {
            0x80 => { result.digest = digest(&mut inner)?.oid; }
            0x81 => {
                let algorithm = secret_der::algorithm(&mut inner, malformed)?;
                if algorithm.oid.as_slice() != MGF1 { return Err(malformed()); }
                let mut encoded = Der(parameter(&algorithm)?.ok_or_else(malformed)?);
                let digest = digest(&mut encoded)?;
                encoded.end()?;
                if !is_mgf_digest(&digest.oid) { return Err(malformed()); }
                result.mgf_digest = digest.oid;
            }
            0x82 => { result.salt = integer::int(&mut inner)?; if result.salt < 0 { return Err(malformed()); } }
            0x83 => { if integer::int(&mut inner)? != 1 { return Err(malformed()); } }
            _ => return Err(malformed()),
        }
    }
    Ok(result)
}

fn digest(input: &mut Der<'_>) -> Result<Algorithm> {
    let algorithm = secret_der::algorithm(input, malformed)?;
    if let Some(parameter) = parameter(&algorithm)? {
        match algorithm.oid.as_slice() {
            DSA | [0x2b, 0x0e, 3, 2, 12] => integers(parameter, 3, false)?,
            DH | [0x2a, 0x86, 0x48, 0xce, 0x3e, 2, 1] => integers(parameter, 2, true)?,
            EC => curve(parameter)?,
            [0x60, 0x86, 0x48, 1, 0x65, 3, 4, 1] => primitive_iv(parameter, 16)?,
            [0x2b, 0x0e, 3, 2, 17] | [0x2b, 6, 1, 4, 1, 0x97, 0x55, 1, 1, 2] => primitive_iv(parameter, 8)?,
            [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 9, 16, 3, 18] => chacha_nonce(parameter)?,
            [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 3]
                | [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 12, 1, 1 | 2 | 3 | 5 | 6] => legacy_pbe(parameter)?,
            PSS | [0x2a, 0x86, 0x48, 0xce, 0x3d, 4, 3] => return Err(unsupported()),
            oid if registered_parameter_service(oid) => return Err(unsupported()),
            _ => {},
        }
    }
    Ok(algorithm)
}

fn primitive_iv(bytes: &[u8], length: usize) -> Result<()> {
    let mut input = Der(bytes);
    if expect(&mut input, 4)?.len() != length { return Err(malformed()); }
    input.end()
}

fn chacha_nonce(bytes: &[u8]) -> Result<()> {
    let mut input = Der(bytes);
    let nonce = next(&mut input)?;
    input.end()?;
    if secret_der::octets(nonce, malformed)?.len() != 12 { return Err(malformed()); }
    Ok(())
}

fn legacy_pbe(bytes: &[u8]) -> Result<()> {
    let mut input = Der(bytes);
    let sequence = expect(&mut input, 0x30)?;
    input.end()?;
    let mut fields = Der(&sequence);
    expect(&mut fields, 4)?;
    integer::int(&mut fields)?;
    fields.end()
}

fn is_mgf_digest(oid: &[u8]) -> bool { matches!(oid, SHA1 | [0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, 1..=10]) }

fn registered_parameter_service(oid: &[u8]) -> bool {
    matches!(oid,
        [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 7]
        | [0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 5, 13])
}

impl Pss {
    pub(super) fn encode(&self) -> Result<Zeroizing<Vec<u8>>> {
        let mut fields = Vec::with_capacity(3);
        let digest = canonical_oid(&self.digest)?;
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
