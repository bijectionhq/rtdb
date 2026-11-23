#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

pub(crate) enum InvalidStageJournal {
    Incomplete,
    DigestLength,
}

#[cfg_attr(verus_keep_ghost, verus_spec(out =>
    ensures out == match (before, certificate) {
        (None, None) => Ok(None),
        (Some(before), Some(certificate)) if before >= stage_cutoff =>
            if certificate@.len() == 32 { Ok(Some((before, certificate))) }
            else { Err(InvalidStageJournal::DigestLength) },
        _ => Err(InvalidStageJournal::Incomplete),
    },
))]
pub(crate) fn checked_journal_fields(
    stage_cutoff: i64, before: Option<i64>, certificate: Option<&[u8]>,
) -> Result<Option<(i64, &[u8])>, InvalidStageJournal> {
    match (before, certificate) {
        (None, None) => Ok(None),
        (Some(before), Some(certificate)) if before >= stage_cutoff => {
            if certificate.len() != 32 {
                return Err(InvalidStageJournal::DigestLength);
            }
            Ok(Some((before, certificate)))
        }
        _ => Err(InvalidStageJournal::Incomplete),
    }
}
