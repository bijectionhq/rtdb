//! The binary encoding of the branch transaction log records: the source
//! admission and the replica transaction decision (`dev/doc/db.allium`).
//!
//! The encoding is positional: a value's bytes carry no field names, so every
//! field is always written, and a type it reaches cannot need a self-describing
//! format to decode (no `untagged`, internally tagged or `flatten` shapes, no
//! `skip_serializing_if`). Integers are varints, sequences carry their length,
//! and trailing bytes are refused.

use bincode::Options;
use serde::{Deserialize, Serialize};

fn options() -> impl Options {
    bincode::DefaultOptions::new()
}

pub fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, bincode::Error> {
    options().serialize(value)
}

/// Encodes `value` after one leading `version` byte, in a single allocation.
pub fn encode_versioned<T: Serialize + ?Sized>(version: u8, value: &T) -> Result<Vec<u8>, bincode::Error> {
    let size = usize::try_from(options().serialized_size(value)?).map_err(|_| bincode::ErrorKind::SizeLimit)?;
    let mut bytes = Vec::with_capacity(size + 1);
    bytes.push(version);
    options().serialize_into(&mut bytes, value)?;
    Ok(bytes)
}

pub fn decode<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T, bincode::Error> {
    options().deserialize(bytes)
}

/// Decodes the leading fields of a value's encoding, ignoring the bytes after them.
pub(super) fn decode_prefix<'a, T: Deserialize<'a>>(bytes: &'a [u8]) -> Result<T, bincode::Error> {
    options().allow_trailing_bytes().deserialize(bytes)
}
