use super::*;
use admission::{expect, next, malformed_parameter as malformed};

const OAEP: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 7];
const P_SPECIFIED: &[u8] = &[0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 1, 1, 9];

#[derive(Clone, Copy)]
enum Fields { Pss, Oaep { next: u8 } }

enum Service { Pss, Oaep }

enum Frame {
    Initialize(Identifier),
    Fields { remaining: Zeroizing<Vec<u8>>, fields: Fields, root: bool },
    Finish(Service),
    SetDigest,
    CheckMgf { sha3: bool, root: bool },
    Reject,
    Specified,
}

const _: () = assert!(std::mem::size_of::<Frame>() <= 64);

struct Identifier { oid: Zeroizing<Vec<u8>>, parameter: Option<Zeroizing<Vec<u8>>> }

struct Evaluator { frames: Vec<Frame>, name: Option<DigestName>, result: Pss }

pub(super) fn pss(bytes: &[u8]) -> DecodeResult<Pss> {
    let mut evaluator = Evaluator::new(vec![fields(bytes, Fields::Pss, true)?]);
    evaluator.run()?;
    Ok(evaluator.result)
}

pub(super) fn initialize(algorithm: &Algorithm) -> DecodeResult<DigestName> {
    let parameter = parameter(algorithm).map_err(Failure::JavaIo)?.map(|bytes| Zeroizing::new(bytes.to_vec()));
    initialize_name(Identifier { oid: Zeroizing::new(algorithm.oid.to_vec()), parameter })
}

fn initialize_name(algorithm: Identifier) -> DecodeResult<DigestName> {
    let mut evaluator = Evaluator::new(vec![Frame::Initialize(algorithm)]);
    evaluator.run()?;
    Ok(evaluator.completed_name())
}

impl Evaluator {
    fn new(frames: Vec<Frame>) -> Self {
        let result = Pss { digest: DigestName::Oid(Zeroizing::new(SHA1.to_vec())), mgf_digest: Zeroizing::new(SHA1.to_vec()), salt: 20 };
        Self { frames, name: None, result }
    }

    fn run(&mut self) -> DecodeResult<()> {
        while let Some(frame) = self.frames.pop() {
            if let Err(error) = self.step(frame) { self.failure(error)?; }
        }
        Ok(())
    }

    fn failure(&mut self, error: Failure) -> DecodeResult<()> {
        let catch = if matches!(&error, Failure::JavaIo(_)) { self.frames.iter().rposition(|frame| matches!(frame, Frame::Specified)) } else { None };
        if let Some(catch) = catch {
            self.frames.truncate(catch);
            self.name = Some(DigestName::Oid(Zeroizing::new(specified::OID.to_vec())));
            Ok(())
        } else { Err(error) }
    }

    fn completed_name(&mut self) -> DigestName { self.name.take().expect("completed parameter child name") }

    fn step(&mut self, frame: Frame) -> DecodeResult<()> {
        match frame {
            Frame::Initialize(algorithm) => self.initialize(algorithm)?,
            Frame::Finish(service) => {
                let oid = match service { Service::Pss => PSS, Service::Oaep => OAEP };
                self.name = Some(DigestName::Oid(Zeroizing::new(oid.to_vec())));
            }
            Frame::SetDigest => {
                let name = self.completed_name();
                self.result.digest = name;
            }
            Frame::Specified => {
                let name = self.completed_name();
                self.name = Some(specified::name(name));
            }
            Frame::Reject => return Err(malformed()),
            Frame::CheckMgf { sha3, root } => {
                let DigestName::Oid(oid) = self.completed_name() else { return Err(malformed()); };
                if !is_mgf_digest(&oid) || !sha3 && matches!(oid.as_slice(), [0x60, 0x86, 0x48, 1, 0x65, 3, 4, 2, 7..=10]) { return Err(malformed()); }
                if root { self.result.mgf_digest = oid; }
            }
            Frame::Fields { remaining, fields: format, root } => {
                if remaining.is_empty() { return Ok(()); }
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
                self.frames.push(Frame::Fields { remaining: Zeroizing::new(input.0.to_vec()), fields: following, root });
                let mut inner = Der(&context.content);
                let value = next(&mut inner)?;
                if exact { admission::end(inner)?; }
                match tag {
                    0x80 => {
                        let algorithm = identifier(value)?;
                        if root { self.frames.push(Frame::SetDigest); }
                        self.frames.push(Frame::Initialize(algorithm));
                    }
                    0x81 => {
                        let algorithm = identifier(value)?;
                        if algorithm.oid.as_slice() != MGF1 {
                            self.frames.push(Frame::Reject);
                            self.frames.push(Frame::Initialize(algorithm));
                            return Ok(());
                        }
                        let parameter = algorithm.parameter.ok_or_else(malformed)?;
                        let digest = encoded_identifier(&parameter)?;
                        self.frames.push(Frame::CheckMgf { sha3: matches!(format, Fields::Pss), root });
                        self.frames.push(Frame::Initialize(digest));
                    }
                    0x82 if matches!(format, Fields::Oaep { .. }) => {
                        let algorithm = identifier(value)?;
                        if algorithm.oid.as_slice() != P_SPECIFIED {
                            self.frames.push(Frame::Reject);
                            self.frames.push(Frame::Initialize(algorithm));
                            return Ok(());
                        }
                        let parameter = algorithm.parameter.ok_or_else(malformed)?;
                        let mut encoded = Der(&parameter);
                        let value = next(&mut encoded)?;
                        admission::end(encoded)?;
                        admission::octets(value)?;
                    }
                    0x82 => {
                        if value.tag != 2 { return Err(malformed()); }
                        let salt = admission::int_content(&value.content)?;
                        if salt < 0 { return Err(malformed()); }
                        if root { self.result.salt = salt; }
                    }
                    0x83 => {
                        if value.tag != 2 || admission::int_content(&value.content)? != 1 { return Err(malformed()); }
                    }
                    _ => return Err(malformed()),
                }
            }
        }
        Ok(())
    }

    fn initialize(&mut self, algorithm: Identifier) -> DecodeResult<()> {
        self.name = None;
        if let Some(parameter) = algorithm.parameter {
            match algorithm.oid.as_slice() {
                PSS => {
                    self.frames.push(Frame::Finish(Service::Pss));
                    self.frames.push(fields(&parameter, Fields::Pss, false)?);
                    return Ok(());
                }
                OAEP => {
                    self.frames.push(Frame::Finish(Service::Oaep));
                    self.frames.push(fields(&parameter, Fields::Oaep { next: 0 }, false)?);
                    return Ok(());
                }
                specified::OID => {
                    self.frames.push(Frame::Specified);
                    let child = encoded_identifier(&parameter)?;
                    self.frames.push(Frame::Initialize(child));
                    return Ok(());
                }
                pbes2::OID => {
                    self.name = Some(DigestName::Pbes2(pbes2::decode(&parameter)?));
                    return Ok(());
                }
                oid => fixed_service(oid, &parameter)?,
            }
        }
        self.name = Some(DigestName::Oid(algorithm.oid));
        Ok(())
    }
}

fn fields(bytes: &[u8], format: Fields, root: bool) -> DecodeResult<Frame> {
    let mut input = Der(bytes);
    let outer = next(&mut input)?;
    if matches!(format, Fields::Pss) {
        if outer.tag != 0x30 { return Err(malformed()); }
        let mut framing = Der(&outer.content);
        while !framing.0.is_empty() { next(&mut framing)?; }
    } else { admission::end(input)?; }
    Ok(Frame::Fields { remaining: outer.content.into_owned(), fields: format, root })
}

fn encoded_identifier(bytes: &[u8]) -> DecodeResult<Identifier> {
    let mut input = Der(bytes);
    let value = next(&mut input)?;
    admission::end(input)?;
    identifier(value)
}

fn identifier(value: secret_der::Value<'_>) -> DecodeResult<Identifier> {
    if value.tag != 0x30 { return Err(malformed()); }
    let mut fields = Der(&value.content);
    let oid = expect(&mut fields, 6)?.into_owned();
    admission::valid_oid(&oid)?;
    let parameter = if fields.0.is_empty() { None } else {
        let value = next(&mut fields)?;
        if value.tag == 5 {
            if !value.content.is_empty() { return Err(malformed()); }
            None
        } else { Some(encode(value.tag, &value.content)) }
    };
    admission::end(fields)?;
    Ok(Identifier { oid, parameter })
}

#[cfg(test)]
pub(super) fn initialized_name(oid: &[u8], parameter: &[u8]) -> DecodeResult<DigestName> {
    let algorithm = Identifier { oid: Zeroizing::new(oid.to_vec()), parameter: Some(Zeroizing::new(parameter.to_vec())) };
    initialize_name(algorithm)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn java_io_uses_only_the_nearest_specified_catch_and_preserves_outer_work() {
        let mut evaluator = Evaluator::new(vec![Frame::SetDigest, Frame::Specified, Frame::Reject, Frame::Specified, Frame::Reject]);
        evaluator.failure(malformed()).unwrap();
        assert_eq!(evaluator.frames.len(), 3);
        assert!(matches!(&evaluator.name, Some(DigestName::Oid(oid)) if oid.as_slice() == specified::OID));
        evaluator.run().unwrap();
        assert!(matches!(&evaluator.result.digest, DigestName::Oid(oid) if oid.as_slice() == specified::OID));
    }

    #[test]
    fn propagated_failures_bypass_specified_catches_and_retain_the_diagnostic() {
        let mut evaluator = Evaluator::new(vec![Frame::Specified, Frame::Reject]);
        let expected = crate::invalid("propagated parameter failure");
        let error = evaluator.failure(Failure::Propagate(expected.clone())).unwrap_err();
        assert!(matches!(&error, Failure::Propagate(_)));
        assert_eq!(error.into_anomaly(), expected);
        assert_eq!(evaluator.frames.len(), 2);
        assert!(evaluator.name.is_none());
    }

    #[test]
    fn java_io_without_a_specified_catch_propagates_unchanged() {
        let mut evaluator = Evaluator::new(vec![Frame::SetDigest]);
        let expected = crate::java_der::malformed();
        let error = evaluator.failure(Failure::JavaIo(expected.clone())).unwrap_err();
        assert!(matches!(&error, Failure::JavaIo(_)));
        assert_eq!(error.into_anomaly(), expected);
        assert_eq!(evaluator.frames.len(), 1);
    }
}
