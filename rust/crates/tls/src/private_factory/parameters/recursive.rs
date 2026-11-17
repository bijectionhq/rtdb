use super::*;

const OAEP: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 7];
const P_SPECIFIED: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 9];

#[derive(Clone, Copy)]
enum Fields { Pss, Oaep { next: u8 } }

enum Frame {
    Fields { remaining: Zeroizing<Vec<u8>>, fields: Fields, root: bool },
    CheckMgf { oid: Zeroizing<Vec<u8>>, sha3: bool },
    Reject,
}

const _: () = assert!(std::mem::size_of::<Frame>() <= 64);

struct Identifier { oid: Zeroizing<Vec<u8>>, parameter: Option<Zeroizing<Vec<u8>>> }

pub(super) fn pss(bytes: &[u8]) -> Result<Pss> {
    let mut result = Pss { digest: DigestName::Oid(Zeroizing::new(SHA1.to_vec())), mgf_digest: Zeroizing::new(SHA1.to_vec()), salt: 20 };
    let mut frames = vec![fields(bytes, Fields::Pss, true)?];
    while let Some(frame) = frames.pop() {
        match frame {
            Frame::Reject => return Err(malformed()),
            Frame::CheckMgf { oid, sha3 } => {
                if !is_mgf_digest(&oid) || !sha3 && matches!(oid.as_slice(), [0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, 7..=10]) { return Err(malformed()); }
            }
            Frame::Fields { remaining, fields: format, root } => {
                if remaining.is_empty() { continue; }
                let mut input = Der(&remaining);
                let context = next(&mut input)?;
                let tag = context.tag & 0xdf;
                let (following, exact) = match format {
                    Fields::Pss => (Fields::Pss, false),
                    Fields::Oaep { next } => {
                        if !(0x80 + next..=0x82).contains(&tag) { return Err(malformed()); }
                        (Fields::Oaep { next: tag - 0x80 + 1 }, true)
                    }
                };
                frames.push(Frame::Fields { remaining: Zeroizing::new(input.0.to_vec()), fields: following, root });
                let mut inner = Der(&context.content);
                let value = next(&mut inner)?;
                if exact { inner.end()?; }
                match tag {
                    0x80 => {
                        let algorithm = identifier(value)?;
                        let name = schedule(algorithm, &mut frames)?;
                        if root { result.digest = name; }
                    }
                    0x81 => {
                        let algorithm = identifier(value)?;
                        if algorithm.oid.as_slice() != MGF1 {
                            frames.push(Frame::Reject);
                            schedule(algorithm, &mut frames)?;
                            continue;
                        }
                        let parameter = algorithm.parameter.ok_or_else(malformed)?;
                        let mut encoded = Der(&parameter);
                        let value = next(&mut encoded)?;
                        encoded.end()?;
                        let digest = identifier(value)?;
                        if root { result.mgf_digest = Zeroizing::new(digest.oid.to_vec()); }
                        frames.push(Frame::CheckMgf { oid: Zeroizing::new(digest.oid.to_vec()), sha3: matches!(format, Fields::Pss) });
                        schedule(digest, &mut frames)?;
                    }
                    0x82 if matches!(format, Fields::Oaep { .. }) => {
                        let algorithm = identifier(value)?;
                        if algorithm.oid.as_slice() != P_SPECIFIED {
                            frames.push(Frame::Reject);
                            schedule(algorithm, &mut frames)?;
                            continue;
                        }
                        let parameter = algorithm.parameter.ok_or_else(malformed)?;
                        let mut encoded = Der(&parameter);
                        let value = next(&mut encoded)?;
                        encoded.end()?;
                        secret_der::octets(value, malformed)?;
                    }
                    0x82 => {
                        if value.tag != 2 { return Err(malformed()); }
                        let salt = integer::int_content(&value.content)?;
                        if salt < 0 { return Err(malformed()); }
                        if root { result.salt = salt; }
                    }
                    0x83 => {
                        if value.tag != 2 || integer::int_content(&value.content)? != 1 { return Err(malformed()); }
                    }
                    _ => return Err(malformed()),
                }
            }
        }
    }
    Ok(result)
}

fn fields(bytes: &[u8], format: Fields, root: bool) -> Result<Frame> {
    let mut input = Der(bytes);
    let outer = next(&mut input)?;
    if matches!(format, Fields::Pss) {
        if outer.tag != 0x30 { return Err(malformed()); }
        let mut framing = Der(&outer.content);
        while !framing.0.is_empty() { next(&mut framing)?; }
    } else { input.end()?; }
    Ok(Frame::Fields { remaining: outer.content.into_owned(), fields: format, root })
}

fn identifier(value: secret_der::Value<'_>) -> Result<Identifier> {
    if value.tag != 0x30 { return Err(malformed()); }
    let mut fields = Der(&value.content);
    let oid = expect(&mut fields, 6)?.into_owned();
    crate::java_der::valid_oid(&oid)?;
    let parameter = if fields.0.is_empty() { None } else {
        let value = next(&mut fields)?;
        if value.tag == 5 {
            if !value.content.is_empty() { return Err(malformed()); }
            None
        } else { Some(encode(value.tag, &value.content)) }
    };
    fields.end()?;
    Ok(Identifier { oid, parameter })
}

fn schedule(algorithm: Identifier, frames: &mut Vec<Frame>) -> Result<DigestName> {
    if let Some(parameter) = algorithm.parameter {
        match algorithm.oid.as_slice() {
            PSS => frames.push(fields(&parameter, Fields::Pss, false)?),
            OAEP => frames.push(fields(&parameter, Fields::Oaep { next: 0 }, false)?),
            pbes2::OID => return pbes2::decode(&parameter).map(DigestName::Pbes2),
            oid => fixed_service(oid, &parameter)?,
        }
    }
    Ok(DigestName::Oid(algorithm.oid))
}
