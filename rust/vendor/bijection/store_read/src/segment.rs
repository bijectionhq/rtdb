//! The kernel's key segments: each segment's bytes escaped (`0x00` becomes
//! `0x00 0xFF`) and terminated by `0x00 0x01`, so byte order of keys is
//! segment-wise order and a key of fewer complete segments is a prefix of
//! every key extending it.
// TWIN (interim, not landable): of the Verus-verified `kernel_storage::key::encoding`; the prover lane hoists the shared segment spec layer into store_read and deletes the twin.

/// `bytes` as one escaped, terminated segment.
pub fn segment(bytes: &[u8]) -> Vec<u8> {
    let mut out = escaped(bytes);
    out.extend_from_slice(&[0, 1]);
    out
}

/// `bytes` escaped as inside a segment, without its terminator: the lower
/// bound of every segment whose value is at least `bytes`.
pub fn escaped(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len());
    for &byte in bytes {
        out.push(byte);
        if byte == 0 {
            out.push(0xff);
        }
    }
    out
}

/// The values of the first `segments` segments of `bytes` after its family
/// byte. `None` unless `bytes` is exactly that many segments.
pub fn decode(bytes: &[u8], segments: usize) -> Option<Vec<Vec<u8>>> {
    let (values, remaining) = decode_prefix(bytes, segments)?;
    remaining.is_empty().then_some(values)
}

/// Decode complete leading segments after the family byte and leave the
/// remaining bytes untouched.
pub fn decode_prefix(bytes: &[u8], segments: usize) -> Option<(Vec<Vec<u8>>, &[u8])> {
    let mut out = Vec::with_capacity(segments);
    let mut at = 1;
    for _ in 0..segments {
        let mut value = Vec::new();
        loop {
            let byte = *bytes.get(at)?;
            if byte != 0x00 {
                value.push(byte);
                at += 1;
                continue;
            }
            let next = *bytes.get(at + 1)?;
            at += 2;
            if next == 0x01 {
                break;
            }
            // The escaped zero: `0x00 0xFF`.
            if next != 0xFF {
                return None;
            }
            value.push(0);
        }
        out.push(value);
    }
    Some((out, bytes.get(at..)?))
}
