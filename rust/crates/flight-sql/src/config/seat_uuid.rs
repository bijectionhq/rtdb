pub(super) use uuid::Uuid;

pub(super) fn parse_attempt(bytes: &[u8], start: usize) -> Option<(Uuid, [u8; 36])> {
    let text = std::str::from_utf8(bytes.get(start..)?).ok()?;
    let attempt = Uuid::parse_str(text).ok().filter(|attempt| !attempt.is_nil())?;
    let mut canonical = [0; 36];
    attempt.hyphenated().encode_lower(&mut canonical);
    Some((attempt, canonical))
}
