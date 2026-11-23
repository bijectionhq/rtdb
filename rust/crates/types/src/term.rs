#![allow(unexpected_cfgs)]

use crate::{Anomaly, Result};

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg(not(verus_keep_ghost))]
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy)]
#[cfg_attr(not(verus_keep_ghost), derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize))]
pub struct Term(pub i64);

#[derive(Clone, Copy)]
#[cfg_attr(not(verus_keep_ghost), derive(Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize))]
pub struct TermSequence(pub i64);

#[cfg(not(verus_keep_ghost))]
impl std::fmt::Display for Term {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.0.fmt(f) }
}

#[cfg(not(verus_keep_ghost))]
impl std::fmt::Display for TermSequence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { self.0.fmt(f) }
}

impl TermSequence {
    pub const CLAIM: Self = Self(0);
    #[cfg_attr(verus_keep_ghost, verus_spec(out =>
        ensures match out {
            Ok(next) => next.0 == self.0 as int + 1,
            Err(_) => self.0 == i64::MAX,
        },
    ))]
    pub fn next(self) -> Result<Self> {
        match self.0.checked_add(1) {
            Some(next) => Ok(Self(next)),
            None => Err(Anomaly::fault("xtdb/term-sequence-overflow", "Term sequence exhausted")),
        }
    }
}
