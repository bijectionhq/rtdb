#![allow(unexpected_cfgs)]

#[cfg(verus_keep_ghost)]
use vstd::prelude::*;

#[cfg_attr(verus_keep_ghost, verus_spec(source => ensures
    source == match recorded {
        Some(position) => position,
        None => if external_source { previous } else { transaction },
    },
))]
pub(super) fn replayed_source(
    recorded: Option<i64>, external_source: bool, previous: i64, transaction: i64,
) -> i64 {
    match recorded {
        Some(position) => position,
        None if external_source => previous,
        None => transaction,
    }
}

pub(super) struct AppliedUpdate<Receipt> {
    pub(super) source: i64,
    pub(super) completed: Option<(i64, Receipt)>,
}

pub(super) enum Regression {
    Source,
    Transaction,
}

#[cfg_attr(verus_keep_ghost, verus_spec(update => ensures
    match update {
        Ok(applied) => !(offered_source matches Some(source) && source < previous_source)
            && !(completed matches Some((tx, _)) && tx <= previous_tx)
            && applied.source == offered_source.unwrap_or(previous_source)
            && applied.completed == completed,
        Err(Regression::Source) => offered_source matches Some(source) && source < previous_source,
        Err(Regression::Transaction) => !(offered_source matches Some(source) && source < previous_source)
            && (completed matches Some((tx, _)) && tx <= previous_tx),
    },
))]
pub(super) fn applied_update<Receipt>(
    previous_source: i64,
    previous_tx: i64,
    offered_source: Option<i64>,
    completed: Option<(i64, Receipt)>,
) -> Result<AppliedUpdate<Receipt>, Regression> {
    let source = offered_source.unwrap_or(previous_source);
    if source < previous_source {
        return Err(Regression::Source);
    }
    if let Some((tx, _)) = &completed {
        if *tx <= previous_tx {
            return Err(Regression::Transaction);
        }
    }
    Ok(AppliedUpdate { source, completed })
}
