//! The prefix laws a store grant's scope is admitted by: the one definition,
//! with no imports outside `core` and `alloc`.
//!
//! A prefix `p` covers exactly the keys that start with it, which is the
//! half-open range `[p, successor(p))` in byte order (`successor: None` is
//! unbounded: `p` is all `0xff`).

/// The first key after every key that starts with `prefix`: `prefix` without
/// its trailing `0xff` bytes, its last byte raised by one. `None` when every
/// byte is `0xff` (every key at or above `prefix` starts with it).
pub(crate) fn successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let last = prefix.iter().rposition(|byte| *byte != 0xff)?;
    let mut next = prefix[..=last].to_vec();
    next[last] += 1;
    Some(next)
}

/// Whether every key of `[start, end)` starts with `prefix`: `start` does,
/// and `end` lies at or below the prefix's successor. An unbounded `end`
/// stays under `prefix` only when it has no successor.
pub(crate) fn range_within(prefix: &[u8], start: &[u8], end: Option<&[u8]>) -> bool {
    if !start.starts_with(prefix) {
        return false;
    }
    match (successor(prefix), end) {
        (None, _) => true,
        (Some(_), None) => false,
        (Some(next), Some(end)) => end <= next.as_slice(),
    }
}

/// Whether a write of `key` lies under `prefix` once the store fills its
/// stamp: the key starts with `prefix` and the 16-byte placeholder at
/// `stamp` begins at or past the prefix's end, so the filled bytes never
/// touch it.
pub(crate) fn write_within(prefix: &[u8], key: &[u8], stamp: Option<u32>) -> bool {
    key.starts_with(prefix) && stamp.is_none_or(|offset| offset as usize >= prefix.len())
}

/// Whether `prefix` starts with one whole partition prefix: a big-endian
/// `u16` length, then that many bytes.
pub(crate) fn names_partition(prefix: &[u8]) -> bool {
    match prefix {
        [high, low, rest @ ..] => rest.len() >= usize::from(u16::from_be_bytes([*high, *low])),
        _ => false,
    }
}
