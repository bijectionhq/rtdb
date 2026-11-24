//! Field decisions for the hosted Flight control and world publication wire.
#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

pub(crate) const ID: &str = "_id";
pub(crate) const EXPECTED: &str = "_expected_micros";
pub(crate) const CHECK_ONLY: &str = "_check_only";
pub(crate) const REVISION: &str = "rtdb.owned.revision_micros";

#[cfg_attr(verus_keep_ghost, verus_spec(names => ensures
    names.0@ == "_id"@,
    names.1@ == "_expected_micros"@,
    names.2@ == "_check_only"@,
))]
pub(crate) fn hosted_check_columns() -> (&'static str, &'static str, &'static str) {
    (ID, EXPECTED, CHECK_ONLY)
}

#[cfg_attr(verus_keep_ghost, verus_spec(allowed => ensures
    allowed == (name@.len() == 0 || name@[0] != b'_'),
))]
pub(crate) fn absence_payload_column(name: &[u8]) -> bool {
    name.is_empty() || name[0] != b'_'
}

pub(crate) enum ControlCommand {
    Check(Option<i64>),
    Replace(Option<i64>),
    Refuse,
}

#[cfg_attr(verus_keep_ghost, verus_spec(command => ensures
    command == if check_only {
        ControlCommand::Check(expected)
    } else if require_checks {
        ControlCommand::Refuse
    } else {
        ControlCommand::Replace(expected)
    },
))]
pub(crate) fn hosted_control_command(
    expected: Option<i64>, check_only: bool, require_checks: bool,
) -> ControlCommand {
    if check_only {
        ControlCommand::Check(expected)
    } else if require_checks {
        ControlCommand::Refuse
    } else {
        ControlCommand::Replace(expected)
    }
}

#[cfg_attr(verus_keep_ghost, verus_spec(n => ensures
    n == ((bytes@[0] as u16) << 8) | bytes@[1] as u16,
))]
pub(crate) fn group_u16(bytes: &[u8; 2]) -> u16 {
    ((bytes[0] as u16) << 8) | bytes[1] as u16
}

#[cfg_attr(verus_keep_ghost, verus_spec(n => ensures
    n == ((bytes@[0] as u32) << 24) | ((bytes@[1] as u32) << 16)
        | ((bytes@[2] as u32) << 8) | bytes@[3] as u32,
))]
pub(crate) fn group_u32(bytes: &[u8; 4]) -> u32 {
    ((bytes[0] as u32) << 24) | ((bytes[1] as u32) << 16)
        | ((bytes[2] as u32) << 8) | bytes[3] as u32
}

#[cfg_attr(verus_keep_ghost, verus_spec(out => ensures
    out is Some <==> body@.len() >= 32,
    out matches Some((fingerprint, rest)) ==> fingerprint@ == body@.take(32)
        && rest@ == body@.skip(32),
))]
pub(crate) fn world_fingerprint(body: &[u8]) -> Option<([u8; 32], &[u8])> {
    let (prefix, rest) = body.split_at_checked(32)?;
    let mut fingerprint = [0; 32];
    let mut i = 0;
    #[cfg_attr(verus_keep_ghost, verus_spec(invariant
        i <= 32,
        prefix@.len() == 32,
        forall|j: int| 0 <= j < i ==> fingerprint@[j] == prefix@[j],
        decreases 32 - i,
    ))]
    while i < 32 {
        fingerprint[i] = prefix[i];
        i += 1;
    }
    #[cfg(verus_keep_ghost)]
    proof! {
        assert(fingerprint@ =~= prefix@);
    }
    Some((fingerprint, rest))
}

#[cfg_attr(verus_keep_ghost, verus_spec(out => ensures
    out matches Some((key, at)) ==> key@ == "rtdb.owned.revision_micros"@ && revision == Some(at),
    out is None <==> revision is None,
))]
pub(crate) fn point_revision_metadata(revision: Option<i64>) -> Option<(&'static str, i64)> {
    match revision {
        Some(at) => Some((REVISION, at)),
        None => None,
    }
}
