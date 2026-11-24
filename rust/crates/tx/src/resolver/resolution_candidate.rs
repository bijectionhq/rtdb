#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(out => ensures
    out == match outcome {
        Ok(()) => Ok((candidate, prepared_writes)),
        Err(error) => Err(error),
    },
))]
pub(super) fn resolve_candidate<T, W, E>(
    candidate: T,
    prepared_writes: Option<W>,
    outcome: Result<(), E>,
) -> Result<(T, Option<W>), E> {
    match outcome {
        Ok(()) => Ok((candidate, prepared_writes)),
        Err(error) => Err(error),
    }
}
