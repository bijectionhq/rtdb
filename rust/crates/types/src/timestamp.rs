#![allow(unexpected_cfgs)]

/// Signed microseconds since the Unix epoch, with the extrema reserved as unbounded endpoints.
#[derive(Clone, Copy)]
#[cfg_attr(not(verus_keep_ghost), derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize))]
pub struct Timestamp(pub i64);
